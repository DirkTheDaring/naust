//! Test-only contained filesystem manifest read integration seam for `registry-rust`.
//!
//! # Architectural Ownership Boundaries
//! - `storage-core`: Defines domain-neutral contracts ([`storage_core::ObjectPayloadReader`],
//!   [`storage_core::ObjectPayload`], [`storage_core::ObjectStream`], [`storage_core::ReadError`],
//!   [`storage_core::ObjectKey`]).
//! - `storage-fs`: Implements Linux descriptor-relative containment (`openat2` + `O_PATH` with
//!   `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`), regular file validation (`S_IFREG`),
//!   and Phase 2 readable reopening via `/proc/self/fd/N`.
//! - `registry-rust`: Owns manifest key construction (`repos/<repository>/manifests/<digest.hex()>`),
//!   pre-composition path safety checks, full payload buffering, media-type detection,
//!   and mapping to [`StorageError`].
//!
//! # Contract Differences: `ObjectKey` Path Safety vs `CanonicalRepoName`
//! - `CanonicalRepoName` validates OCI distribution specification repository grammar (non-empty segments,
//!   lowercase ASCII alphanumerics, allowed separators `.` `_` `-` `__` `---`, component segment bounds,
//!   maximum total length of 255).
//! - `ObjectKey` enforces relative path safety for containment (rejecting leading/trailing slashes,
//!   empty segments, backslashes, control characters, and `..` or `.` segments).
//! - Pre-composition validation in [`manifest_key`] enforces these path safety constraints before key creation,
//!   without normalizing or mutating unsafe inputs. Validating `ObjectKey` safety does not imply canonical
//!   repository validity, and canonical repository validation does not replace containment checks.
//!
//! # Observation Semantics, Buffering, and Lack of Snapshot Guarantees
//! - **Full Read Buffering**: Both [`head_manifest_seam`] and [`get_manifest_seam`] read the full manifest payload
//!   into memory because `mediaType` is dynamically parsed from the JSON body (`detect_manifest_media_type`).
//! - **Unbounded Resource Concern**: Manifests are currently buffered into memory without an enforcement limit.
//!   This is recorded as an unresolved resource concern for future production cutover.
//! - **No Snapshot Isolation**: Reading metadata followed by reading payload does not guarantee snapshot consistency
//!   under concurrent mutations. Files are not immutable merely because their filename is a digest.
//! - **No Digest Verification**: Existing production behavior does not compute a hash over read bytes to verify
//!   against the requested digest; this seam strictly preserves that behavior.

use crate::registry::digest::Digest;
use crate::storage::{ManifestMeta, StorageError};
use storage_core::{ObjectKey, ObjectPayloadReader};
use tokio::io::AsyncReadExt;

/// Constructs the relative [`ObjectKey`] for a repository manifest:
/// `repos/<repository>/manifests/<digest.hex()>`.
///
/// # Validation Checks
/// Explicitly validates `repo` before composing the key:
/// - Rejects empty repository strings.
/// - Rejects leading or trailing `/` characters.
/// - Rejects backslashes (`\`), NUL bytes, and ASCII control characters.
/// - Rejects empty segments (consecutive slashes `//`).
/// - Rejects `.` (current directory) and `..` (parent directory) segments.
///
/// Unsafe inputs are rejected with [`StorageError::InvalidRepoName`] without silent normalization.
pub(crate) fn manifest_key(repo: &str, digest: &Digest) -> Result<ObjectKey, StorageError> {
    if repo.is_empty() {
        return Err(StorageError::InvalidRepoName(
            "repository name cannot be empty".to_string(),
        ));
    }
    if repo.starts_with('/') || repo.ends_with('/') {
        return Err(StorageError::InvalidRepoName(
            "repository name cannot have leading or trailing slashes".to_string(),
        ));
    }
    if repo.contains('\\') {
        return Err(StorageError::InvalidRepoName(
            "repository name cannot contain backslashes".to_string(),
        ));
    }
    if repo.contains(|c: char| c == '\0' || c.is_ascii_control()) {
        return Err(StorageError::InvalidRepoName(
            "repository name cannot contain NUL bytes or control characters".to_string(),
        ));
    }

    for segment in repo.split('/') {
        if segment.is_empty() {
            return Err(StorageError::InvalidRepoName(
                "repository name cannot contain empty segments (repeated slashes)".to_string(),
            ));
        }
        if segment == "." {
            return Err(StorageError::InvalidRepoName(
                "repository name cannot contain '.' segments".to_string(),
            ));
        }
        if segment == ".." {
            return Err(StorageError::InvalidRepoName(
                "repository name cannot contain '..' segments (path traversal attempt)".to_string(),
            ));
        }
    }

    let key_str = format!("repos/{repo}/manifests/{}", digest.hex());
    ObjectKey::parse(&key_str).map_err(|e| StorageError::InvalidRepoName(e.to_string()))
}

/// Detects the media type of a manifest payload using existing registry conventions.
///
/// NOTE: This function duplicates [`super::FsStorage::detect_manifest_media_type`].
/// It is retained as a test-only copy because [`super::FsStorage::detect_manifest_media_type`] is
/// an instance method requiring an allocated `&FsStorage` instance, and modifying production
/// `FsStorage` methods or extracting shared helpers is outside the authorized scope of this
/// test-only slice. Direct parity is verified against `FsStorage::detect_manifest_media_type`
/// in `test_parity_with_fs_storage_detect_manifest_media_type`.
///
/// Parses JSON looking for a top-level string `"mediaType"`.
/// - If present and string: returns the specified media type.
/// - If missing, non-string, or the JSON is a scalar: defaults to `"application/vnd.oci.image.manifest.v1+json"`.
/// - If the payload is empty (0 bytes) or malformed non-JSON: returns [`crate::storage::StorageErrorKind::CorruptData`].
pub(crate) fn detect_manifest_media_type(bytes: &[u8]) -> Result<String, StorageError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|err| StorageError::corrupt_data(err.to_string()))?;
    let media_type = value
        .get("mediaType")
        .and_then(|v| v.as_str())
        .unwrap_or("application/vnd.oci.image.manifest.v1+json");
    Ok(media_type.to_string())
}

/// Reads the complete manifest payload and derives metadata via [`storage_core::ObjectPayloadReader`].
///
/// - Opens the payload once via descriptor-relative containment.
/// - Drains the stream to completion.
/// - Derives `ManifestMeta.size` from the bytes actually read, not acquisition metadata.
/// - Returns [`ManifestMeta`] and the complete payload [`bytes::Bytes`].
pub(crate) async fn get_manifest_seam(
    reader: &(impl ObjectPayloadReader + ?Sized),
    repo: &str,
    digest: &Digest,
) -> Result<(ManifestMeta, bytes::Bytes), StorageError> {
    let key = manifest_key(repo, digest)?;

    let payload = reader
        .open_payload(&key)
        .await
        .map_err(super::read_adapter::translate_payload_read_error)?;

    let (_meta, mut stream) = payload.into_parts();
    let mut bytes = Vec::new();
    stream
        .read_to_end(&mut bytes)
        .await
        .map_err(|e| StorageError::io(format!("failed to read manifest payload: {e}")))?;

    let media_type = detect_manifest_media_type(&bytes)?;
    let size = bytes.len() as u64;

    let meta = ManifestMeta { size, media_type };
    Ok((meta, bytes::Bytes::from(bytes)))
}

/// Reads the complete manifest payload and derives metadata via [`storage_core::ObjectPayloadReader`].
///
/// - Opens the payload once via descriptor-relative containment.
/// - Drains the stream to completion to parse media type and validate JSON structure.
/// - Derives `ManifestMeta.size` from the bytes actually read.
/// - Discards the payload bytes and returns [`ManifestMeta`].
pub(crate) async fn head_manifest_seam(
    reader: &(impl ObjectPayloadReader + ?Sized),
    repo: &str,
    digest: &Digest,
) -> Result<ManifestMeta, StorageError> {
    let (meta, _bytes) = get_manifest_seam(reader, repo, digest).await?;
    Ok(meta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::fs::FsStorage;
    use async_trait::async_trait;
    use std::collections::{HashMap, VecDeque};
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use storage_core::{ObjectMetadata, ObjectPayload, ObjectStream, ReadError};
    use tokio::io::AsyncRead;

    struct RecordingFakePayloadReader {
        calls: Arc<Mutex<Vec<ObjectKey>>>,
        responses: Arc<Mutex<HashMap<ObjectKey, VecDeque<Result<ObjectPayload, ReadError>>>>>,
    }

    impl RecordingFakePayloadReader {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                responses: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn script(&self, key: ObjectKey, response: Result<ObjectPayload, ReadError>) {
            self.responses
                .lock()
                .unwrap()
                .entry(key)
                .or_default()
                .push_back(response);
        }

        fn calls(&self) -> Vec<ObjectKey> {
            self.calls.lock().unwrap().clone()
        }

        fn recorded_calls(&self) -> Vec<ObjectKey> {
            self.calls()
        }
    }

    #[async_trait]
    impl ObjectPayloadReader for RecordingFakePayloadReader {
        async fn open_payload(&self, key: &ObjectKey) -> Result<ObjectPayload, ReadError> {
            self.calls.lock().unwrap().push(key.clone());
            let mut responses = self.responses.lock().unwrap();
            let queue = responses.get_mut(key).unwrap_or_else(|| {
                panic!("unexpected call to ObjectPayloadReader with key: {key}")
            });
            queue
                .pop_front()
                .unwrap_or_else(|| panic!("no more scripted responses for key: {key}"))
        }
    }

    fn test_digest(hex: &str) -> Digest {
        Digest::parse(&format!("sha256:{hex}")).expect("valid sha256 digest")
    }

    fn mock_payload(bytes: Vec<u8>) -> ObjectPayload {
        let meta = ObjectMetadata::new(bytes.len() as u64);
        let stream: ObjectStream = Box::pin(std::io::Cursor::new(bytes));
        ObjectPayload::new(meta, stream)
    }

    fn mock_payload_with_meta_size(meta_size: u64, bytes: Vec<u8>) -> ObjectPayload {
        let meta = ObjectMetadata::new(meta_size);
        let stream: ObjectStream = Box::pin(std::io::Cursor::new(bytes));
        ObjectPayload::new(meta, stream)
    }

    struct FailingStream {
        head_bytes: Vec<u8>,
        cursor: usize,
        error_kind: std::io::ErrorKind,
        message: &'static str,
    }

    impl FailingStream {
        fn new(head_bytes: Vec<u8>, error_kind: std::io::ErrorKind, message: &'static str) -> Self {
            Self {
                head_bytes,
                cursor: 0,
                error_kind,
                message,
            }
        }
    }

    impl AsyncRead for FailingStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if self.cursor < self.head_bytes.len() {
                let to_write = std::cmp::min(buf.remaining(), self.head_bytes.len() - self.cursor);
                buf.put_slice(&self.head_bytes[self.cursor..self.cursor + to_write]);
                self.cursor += to_write;
                std::task::Poll::Ready(Ok(()))
            } else {
                std::task::Poll::Ready(Err(std::io::Error::new(self.error_kind, self.message)))
            }
        }
    }

    // ========================================================================
    // Category A: Key Construction and Unsafe Input Rejection Tests
    // ========================================================================

    #[test]
    fn test_manifest_key_valid_single_and_multisegment() {
        let d = test_digest("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");

        let k1 = manifest_key("testrepo", &d).expect("valid single segment");
        assert_eq!(
            k1.as_str(),
            "repos/testrepo/manifests/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );

        let k2 = manifest_key("library/ubuntu", &d).expect("valid multi segment");
        assert_eq!(
            k2.as_str(),
            "repos/library/ubuntu/manifests/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );

        let k3 = manifest_key("org/team/sub/app", &d).expect("valid deeply nested segment");
        assert_eq!(
            k3.as_str(),
            "repos/org/team/sub/app/manifests/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
    }

    #[test]
    fn test_manifest_key_rejects_unsafe_inputs() {
        let d = test_digest("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");

        let unsafe_repos = [
            ("", "empty"),
            ("/absolute", "leading slash"),
            ("trailing/", "trailing slash"),
            ("a/../b", "parent directory segment"),
            ("..", "dot-dot parent"),
            ("../escape", "leading dot-dot"),
            ("escape/..", "trailing dot-dot"),
            ("a/./b", "dot segment"),
            (".", "dot segment"),
            ("a//b", "repeated slash / empty segment"),
            ("a\\b", "backslash"),
            ("a\0b", "embedded NUL"),
            ("a\x01b", "control character"),
        ];

        for (input, label) in unsafe_repos {
            let res = manifest_key(input, &d);
            assert!(
                matches!(res, Err(StorageError::InvalidRepoName(_))),
                "expected InvalidRepoName for {label} ('{input}'), got: {res:?}"
            );
        }
    }

    // ========================================================================
    // Category B: Recording Fake Tests (Calls, Draining, Size, Media Type)
    // ========================================================================

    #[tokio::test]
    async fn test_recording_fake_exact_key_and_single_open_call() {
        let reader = RecordingFakePayloadReader::new();
        let digest =
            test_digest("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let key = manifest_key("library/busybox", &digest).unwrap();

        let manifest_bytes =
            br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}"#
                .to_vec();
        reader.script(key.clone(), Ok(mock_payload(manifest_bytes.clone())));
        reader.script(key.clone(), Ok(mock_payload(manifest_bytes)));

        // 1. HEAD invocation
        let head_res = head_manifest_seam(&reader, "library/busybox", &digest)
            .await
            .expect("head succeeds");
        assert_eq!(reader.calls(), vec![key.clone()]);
        assert_eq!(
            head_res.media_type,
            "application/vnd.oci.image.manifest.v1+json"
        );

        // 2. GET invocation
        let (get_res, payload) = get_manifest_seam(&reader, "library/busybox", &digest)
            .await
            .expect("get succeeds");
        assert_eq!(reader.calls(), vec![key.clone(), key]);
        assert_eq!(get_res, head_res);
        assert_eq!(payload.len() as u64, get_res.size);
    }

    #[tokio::test]
    async fn test_recording_fake_size_derived_from_consumed_bytes_not_metadata() {
        let reader = RecordingFakePayloadReader::new();
        let digest =
            test_digest("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let key = manifest_key("myrepo", &digest).unwrap();

        let manifest_bytes = br#"{"schemaVersion":2}"#.to_vec();
        let actual_size = manifest_bytes.len() as u64;
        let bogus_meta_size = 999_999u64;

        reader.script(
            key.clone(),
            Ok(mock_payload_with_meta_size(
                bogus_meta_size,
                manifest_bytes.clone(),
            )),
        );
        reader.script(
            key.clone(),
            Ok(mock_payload_with_meta_size(bogus_meta_size, manifest_bytes)),
        );

        // HEAD must derive size from consumed bytes, ignoring bogus metadata size
        let head_meta = head_manifest_seam(&reader, "myrepo", &digest)
            .await
            .unwrap();
        assert_eq!(
            head_meta.size, actual_size,
            "HEAD size must match actual stream bytes, not bogus metadata"
        );

        // GET must derive size from consumed bytes, ignoring bogus metadata size
        let (get_meta, payload) = get_manifest_seam(&reader, "myrepo", &digest).await.unwrap();
        assert_eq!(
            get_meta.size, actual_size,
            "GET size must match actual stream bytes, not bogus metadata"
        );
        assert_eq!(payload.len() as u64, actual_size);
    }

    #[tokio::test]
    async fn test_recording_fake_media_type_variants_and_corrupt_data() {
        let reader = RecordingFakePayloadReader::new();
        let digest =
            test_digest("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");
        let key = manifest_key("repo", &digest).unwrap();

        // 1. Explicit custom media type
        reader.script(
            key.clone(),
            Ok(mock_payload(
                br#"{"mediaType":"application/vnd.custom.manifest.v1+json"}"#.to_vec(),
            )),
        );
        let m1 = head_manifest_seam(&reader, "repo", &digest).await.unwrap();
        assert_eq!(m1.media_type, "application/vnd.custom.manifest.v1+json");

        // 2. Missing mediaType -> OCI default fallback
        reader.script(
            key.clone(),
            Ok(mock_payload(br#"{"schemaVersion":2}"#.to_vec())),
        );
        let m2 = head_manifest_seam(&reader, "repo", &digest).await.unwrap();
        assert_eq!(m2.media_type, "application/vnd.oci.image.manifest.v1+json");

        // 3. Non-string mediaType (integer 42) -> OCI default fallback
        reader.script(
            key.clone(),
            Ok(mock_payload(br#"{"mediaType":42}"#.to_vec())),
        );
        let m3 = head_manifest_seam(&reader, "repo", &digest).await.unwrap();
        assert_eq!(m3.media_type, "application/vnd.oci.image.manifest.v1+json");

        // 4. Scalar JSON -> OCI default fallback
        reader.script(
            key.clone(),
            Ok(mock_payload(br#""a bare json string""#.to_vec())),
        );
        let m4 = head_manifest_seam(&reader, "repo", &digest).await.unwrap();
        assert_eq!(m4.media_type, "application/vnd.oci.image.manifest.v1+json");

        // 5. Empty payload (0 bytes) -> CorruptData
        reader.script(key.clone(), Ok(mock_payload(Vec::new())));
        let err_empty = head_manifest_seam(&reader, "repo", &digest)
            .await
            .unwrap_err();
        match err_empty {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, crate::storage::StorageErrorKind::CorruptData)
            }
            other => panic!("expected CorruptData, got {other:?}"),
        }

        // 6. Malformed non-JSON -> CorruptData
        reader.script(
            key.clone(),
            Ok(mock_payload(b"this is definitely not json".to_vec())),
        );
        let err_malformed = head_manifest_seam(&reader, "repo", &digest)
            .await
            .unwrap_err();
        match err_malformed {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, crate::storage::StorageErrorKind::CorruptData)
            }
            other => panic!("expected CorruptData, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_recording_fake_acquisition_failures_suppress_stream_and_fallback() {
        let reader = RecordingFakePayloadReader::new();
        let digest =
            test_digest("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
        let key = manifest_key("repo", &digest).unwrap();

        // 1. NotFound (verify for HEAD and GET)
        reader.script(key.clone(), Err(ReadError::not_found(key.clone())));
        let calls_before = reader.recorded_calls().len();
        let err_nf = head_manifest_seam(&reader, "repo", &digest)
            .await
            .unwrap_err();
        assert!(matches!(err_nf, StorageError::NotFound));
        assert_eq!(
            reader.recorded_calls().len(),
            calls_before + 1,
            "exactly one reader open call for HEAD NotFound"
        );

        reader.script(key.clone(), Err(ReadError::not_found(key.clone())));
        let calls_before = reader.recorded_calls().len();
        let err_nf_get = get_manifest_seam(&reader, "repo", &digest)
            .await
            .unwrap_err();
        assert!(matches!(err_nf_get, StorageError::NotFound));
        assert_eq!(
            reader.recorded_calls().len(),
            calls_before + 1,
            "exactly one reader open call for GET NotFound"
        );

        // 2. PermissionDenied -> StorageErrorKind::Io (verify for HEAD and GET)
        reader.script(key.clone(), Err(ReadError::permission_denied(key.clone())));
        let calls_before = reader.recorded_calls().len();
        let err_perm = head_manifest_seam(&reader, "repo", &digest)
            .await
            .unwrap_err();
        match err_perm {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, crate::storage::StorageErrorKind::Io)
            }
            other => panic!("expected StorageErrorKind::Io, got {other:?}"),
        }
        assert_eq!(
            reader.recorded_calls().len(),
            calls_before + 1,
            "exactly one reader open call for HEAD PermissionDenied"
        );

        reader.script(key.clone(), Err(ReadError::permission_denied(key.clone())));
        let calls_before = reader.recorded_calls().len();
        let err_perm_get = get_manifest_seam(&reader, "repo", &digest)
            .await
            .unwrap_err();
        match err_perm_get {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, crate::storage::StorageErrorKind::Io)
            }
            other => panic!("expected StorageErrorKind::Io, got {other:?}"),
        }
        assert_eq!(
            reader.recorded_calls().len(),
            calls_before + 1,
            "exactly one reader open call for GET PermissionDenied"
        );

        // 3. Backend ResolutionRejected -> StorageErrorKind::Io
        let res_rejected = storage_fs::FsMetadataError::ResolutionRejected {
            raw_os_error: libc::ELOOP,
            source: std::io::Error::from_raw_os_error(libc::ELOOP),
        };
        reader.script(
            key.clone(),
            Err(ReadError::backend_with_source(
                "resolution rejected",
                Box::new(res_rejected),
            )),
        );
        let calls_before = reader.recorded_calls().len();
        let err_res = head_manifest_seam(&reader, "repo", &digest)
            .await
            .unwrap_err();
        match err_res {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, crate::storage::StorageErrorKind::Io)
            }
            other => panic!("expected StorageErrorKind::Io, got {other:?}"),
        }
        assert_eq!(
            reader.recorded_calls().len(),
            calls_before + 1,
            "exactly one reader open call for ResolutionRejected"
        );
    }

    #[tokio::test]
    async fn test_parity_with_fs_storage_detect_manifest_media_type() {
        let temp = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(temp.path().to_path_buf(), 1024 * 1024);

        let cases: &[(&[u8], &str)] = &[
            (
                br#"{"schemaVersion":2,"mediaType":"application/vnd.custom.v1+json"}"#,
                "custom mediaType",
            ),
            (
                br#"{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json"}"#,
                "docker schema2 mediaType",
            ),
            (
                br#"{"schemaVersion":2}"#,
                "missing mediaType (OCI default)",
            ),
            (
                br#"{"mediaType":42}"#,
                "non-string mediaType (OCI default)",
            ),
            (
                br#""a scalar json string""#,
                "scalar string JSON (OCI default)",
            ),
            (
                br#"12345"#,
                "scalar number JSON (OCI default)",
            ),
            (
                b"",
                "empty 0-byte payload",
            ),
            (
                b"not valid json at all",
                "malformed non-JSON payload",
            ),
            (
                b"{incomplete json",
                "truncated JSON payload",
            ),
        ];

        for (bytes, label) in cases {
            let seam_res = detect_manifest_media_type(bytes);
            let prod_res = storage.detect_manifest_media_type(bytes).await;

            match (seam_res, prod_res) {
                (Ok(seam_val), Ok(prod_val)) => {
                    assert_eq!(
                        seam_val, prod_val,
                        "parity mismatch for success case: {label}"
                    );
                }
                (Err(seam_err), Err(prod_err)) => match (seam_err, prod_err) {
                    (
                        StorageError::Internal {
                            kind: seam_kind,
                            message: seam_msg,
                        },
                        StorageError::Internal {
                            kind: prod_kind,
                            message: prod_msg,
                        },
                    ) => {
                        assert_eq!(
                            seam_kind, prod_kind,
                            "parity mismatch for error kind: {label}"
                        );
                        assert_eq!(
                            seam_msg, prod_msg,
                            "parity mismatch for error diagnostic message: {label}"
                        );
                    }
                    (seam_other, prod_other) => {
                        panic!(
                            "unexpected error shape mismatch for {label}: seam={seam_other:?}, prod={prod_other:?}"
                        );
                    }
                },
                (seam, prod) => {
                    panic!("outcome mismatch for {label}: seam={seam:?}, prod={prod:?}");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_recording_fake_mid_stream_io_failure() {
        let reader = RecordingFakePayloadReader::new();
        let digest =
            test_digest("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        let key = manifest_key("repo", &digest).unwrap();

        // Complete valid JSON manifest prefix followed by an injected stream error.
        // Proves that the seam must consume through completion rather than accept an early valid prefix.
        let valid_json_prefix =
            br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}"#;
        let injected_diagnostic = "injected mid-stream connection reset error";

        // 1. Verify GET mid-stream failure
        let failing_stream_get = Box::pin(FailingStream::new(
            valid_json_prefix.to_vec(),
            std::io::ErrorKind::ConnectionReset,
            injected_diagnostic,
        ));
        let payload_get = ObjectPayload::new(ObjectMetadata::new(100), failing_stream_get);
        reader.script(key.clone(), Ok(payload_get));

        let calls_before = reader.recorded_calls().len();
        let err_get = get_manifest_seam(&reader, "repo", &digest)
            .await
            .unwrap_err();
        assert_eq!(
            reader.recorded_calls().len(),
            calls_before + 1,
            "exactly one reader open call for GET"
        );
        match err_get {
            StorageError::Internal { kind, message } => {
                assert_eq!(kind, crate::storage::StorageErrorKind::Io);
                assert!(
                    message.contains(injected_diagnostic),
                    "injected diagnostic must survive error translation, got: {message}"
                );
            }
            other => panic!("expected StorageErrorKind::Io, got {other:?}"),
        }

        // 2. Verify HEAD mid-stream failure
        let failing_stream_head = Box::pin(FailingStream::new(
            valid_json_prefix.to_vec(),
            std::io::ErrorKind::ConnectionReset,
            injected_diagnostic,
        ));
        let payload_head = ObjectPayload::new(ObjectMetadata::new(100), failing_stream_head);
        reader.script(key.clone(), Ok(payload_head));

        let calls_before = reader.recorded_calls().len();
        let err_head = head_manifest_seam(&reader, "repo", &digest)
            .await
            .unwrap_err();
        assert_eq!(
            reader.recorded_calls().len(),
            calls_before + 1,
            "exactly one reader open call for HEAD"
        );
        match err_head {
            StorageError::Internal { kind, message } => {
                assert_eq!(kind, crate::storage::StorageErrorKind::Io);
                assert!(
                    message.contains(injected_diagnostic),
                    "injected diagnostic must survive error translation, got: {message}"
                );
            }
            other => panic!("expected StorageErrorKind::Io, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_recording_fake_typed_runtime_and_task_failures() {
        let reader = RecordingFakePayloadReader::new();
        let digest =
            test_digest("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        let key = manifest_key("repo", &digest).unwrap();

        // 1. Genuine RuntimeMissing -> StorageErrorKind::Backend
        let try_current_err = std::thread::spawn(|| {
            tokio::runtime::Handle::try_current()
                .expect_err("clean OS thread must not have an entered Tokio runtime")
        })
        .join()
        .expect("join thread");
        let rt_missing = storage_fs::FsMetadataError::RuntimeMissing(try_current_err);
        reader.script(
            key.clone(),
            Err(ReadError::backend_with_source(
                "runtime missing",
                Box::new(rt_missing),
            )),
        );
        let err_rt = head_manifest_seam(&reader, "repo", &digest)
            .await
            .unwrap_err();
        match err_rt {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, crate::storage::StorageErrorKind::Backend)
            }
            other => panic!("expected StorageErrorKind::Backend, got {other:?}"),
        }

        // 2. Genuine TaskJoinFailed -> StorageErrorKind::Backend
        let task = tokio::task::spawn_blocking(|| {
            panic!("deliberate worker panic to construct genuine JoinError fixture");
        });
        let join_err = task
            .await
            .expect_err("task deliberate panic must yield JoinError");
        assert!(join_err.is_panic());
        let task_join_err = storage_fs::FsMetadataError::TaskJoinFailed(join_err);
        reader.script(
            key.clone(),
            Err(ReadError::backend_with_source(
                "task join failed",
                Box::new(task_join_err),
            )),
        );
        let err_join = head_manifest_seam(&reader, "repo", &digest)
            .await
            .unwrap_err();
        match err_join {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, crate::storage::StorageErrorKind::Backend)
            }
            other => panic!("expected StorageErrorKind::Backend, got {other:?}"),
        }

        // 3. SyscallUnsupported (syscall failure) -> StorageErrorKind::Configuration
        let syscall_err = storage_fs::FsMetadataError::SyscallUnsupported(
            std::io::Error::from_raw_os_error(libc::ENOSYS),
        );
        reader.script(
            key.clone(),
            Err(ReadError::backend_with_source(
                "openat2 unavailable",
                Box::new(syscall_err),
            )),
        );
        let err_syscall = head_manifest_seam(&reader, "repo", &digest)
            .await
            .unwrap_err();
        match err_syscall {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, crate::storage::StorageErrorKind::Configuration)
            }
            other => panic!("expected StorageErrorKind::Configuration, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_recording_fake_unsafe_input_suppresses_reader_invocation() {
        let reader = RecordingFakePayloadReader::new();
        let digest =
            test_digest("1111111111111111111111111111111111111111111111111111111111111111");

        let res = get_manifest_seam(&reader, "../../escape", &digest).await;
        assert!(matches!(res, Err(StorageError::InvalidRepoName(_))));
        assert!(
            reader.calls().is_empty(),
            "unsafe repo input must reject before open_payload is called"
        );
    }

    // ========================================================================
    // Category C: Real Filesystem Tests with FsMetadataReader
    // ========================================================================

    #[cfg(all(test, target_os = "linux"))]
    mod real_fs_tests {
        use super::*;
        use std::path::{Path, PathBuf};

        fn create_test_root() -> (tempfile::TempDir, PathBuf) {
            let fixture = tempfile::tempdir().expect("create tempdir");
            let root = fixture.path().join("storage-root");
            std::fs::create_dir_all(&root).expect("create storage root");
            (fixture, root)
        }

        fn write_manifest_file(root: &Path, repo: &str, hex: &str, content: &[u8]) {
            let dir = root.join("repos").join(repo).join("manifests");
            std::fs::create_dir_all(&dir).expect("create manifests dir");
            std::fs::write(dir.join(hex), content).expect("write manifest");
        }

        #[tokio::test]
        async fn test_real_fs_representative_valid_manifests_and_nested_repos() {
            let (_fixture, root) = create_test_root();
            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let manifest_bytes = br#"{
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "config": {
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    "size": 0
                },
                "layers": []
            }"#;

            // 1. Single-segment repo
            let hex1 = "1212121212121212121212121212121212121212121212121212121212121212";
            let d1 = test_digest(hex1);
            write_manifest_file(&root, "single_repo", hex1, manifest_bytes);

            let (get_meta1, payload1) = get_manifest_seam(&reader, "single_repo", &d1)
                .await
                .expect("get succeeds");
            assert_eq!(get_meta1.size, manifest_bytes.len() as u64);
            assert_eq!(
                get_meta1.media_type,
                "application/vnd.oci.image.manifest.v1+json"
            );
            assert_eq!(payload1.as_ref(), manifest_bytes);

            let head_meta1 = head_manifest_seam(&reader, "single_repo", &d1)
                .await
                .expect("head succeeds");
            assert_eq!(head_meta1, get_meta1);

            // 2. Nested multi-segment repo
            let hex2 = "2323232323232323232323232323232323232323232323232323232323232323";
            let d2 = test_digest(hex2);
            write_manifest_file(&root, "org/team/sub/app", hex2, manifest_bytes);

            let (get_meta2, payload2) = get_manifest_seam(&reader, "org/team/sub/app", &d2)
                .await
                .expect("nested repo get succeeds");
            assert_eq!(get_meta2.size, manifest_bytes.len() as u64);
            assert_eq!(payload2.as_ref(), manifest_bytes);

            let head_meta2 = head_manifest_seam(&reader, "org/team/sub/app", &d2)
                .await
                .expect("nested repo head succeeds");
            assert_eq!(head_meta2, get_meta2);
        }

        #[tokio::test]
        async fn test_real_fs_supported_digest_algorithms() {
            let (_fixture, root) = create_test_root();
            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");
            let repo = "algo_repo";
            let manifest_bytes =
                br#"{"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json"}"#;

            // SHA-256 (64 hex)
            let hex256 = "2562562562562562562562562562562562562562562562562562562562562562";
            let d256 = test_digest(hex256);
            write_manifest_file(&root, repo, hex256, manifest_bytes);

            let (meta256, p256) = get_manifest_seam(&reader, repo, &d256).await.unwrap();
            assert_eq!(meta256.size, manifest_bytes.len() as u64);
            assert_eq!(p256.as_ref(), manifest_bytes);

            // SHA-512 (128 hex)
            let hex512 = "51251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251";
            let d512 = Digest::parse(&format!("sha512:{hex512}")).expect("valid sha512");
            write_manifest_file(&root, repo, hex512, manifest_bytes);

            let (meta512, p512) = get_manifest_seam(&reader, repo, &d512).await.unwrap();
            assert_eq!(meta512.size, manifest_bytes.len() as u64);
            assert_eq!(p512.as_ref(), manifest_bytes);
        }

        #[tokio::test]
        async fn test_real_fs_missing_manifest_and_missing_repo() {
            let (_fixture, root) = create_test_root();
            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");
            let d = test_digest("3434343434343434343434343434343434343434343434343434343434343434");

            // 1. Missing repository directory
            let err_repo = get_manifest_seam(&reader, "missing_repo", &d)
                .await
                .unwrap_err();
            assert!(matches!(err_repo, StorageError::NotFound));

            // 2. Missing manifest file in existing repo
            std::fs::create_dir_all(root.join("repos/existing_repo/manifests")).unwrap();
            let err_file = get_manifest_seam(&reader, "existing_repo", &d)
                .await
                .unwrap_err();
            assert!(matches!(err_file, StorageError::NotFound));
        }

        #[tokio::test]
        async fn test_real_fs_symlinks_rejected_without_reading_outside_content() {
            use std::os::unix::fs::symlink;

            let fixture = tempfile::tempdir().expect("create tempdir");
            let root = fixture.path().join("storage-root");
            let outside = fixture.path().join("outside");
            std::fs::create_dir_all(&root).unwrap();
            std::fs::create_dir_all(&outside).unwrap();

            let hex = "4545454545454545454545454545454545454545454545454545454545454545";
            let digest = test_digest(hex);

            let secret_content = b"SECRET_OUTSIDE_MANIFEST_CONTENT_DO_NOT_READ";
            let outside_file = outside.join("secret_manifest.json");
            std::fs::write(&outside_file, secret_content).unwrap();

            let manifests_dir = root.join("repos/sym_repo/manifests");
            std::fs::create_dir_all(&manifests_dir).unwrap();

            // Scenario 1: Final component symlink to outside file
            let symlink_file = manifests_dir.join(hex);
            symlink(&outside_file, &symlink_file).unwrap();

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");
            let err_sym = get_manifest_seam(&reader, "sym_repo", &digest)
                .await
                .unwrap_err();
            match err_sym {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, crate::storage::StorageErrorKind::Io);
                    assert!(
                        message.contains("Too many levels of symbolic links")
                            || message.contains("kernel containment policy rejected resolution"),
                        "symlink must be rejected by containment policy: {message}"
                    );
                }
                other => panic!("expected StorageErrorKind::Io, got {other:?}"),
            }

            // Scenario 2: Ancestor directory symlink
            std::fs::remove_file(&symlink_file).unwrap();
            let repo_dir = root.join("repos/dir_sym_repo");
            std::fs::create_dir_all(&repo_dir).unwrap();
            let outside_manifests = outside.join("manifests_store");
            std::fs::create_dir_all(&outside_manifests).unwrap();
            std::fs::write(outside_manifests.join(hex), secret_content).unwrap();
            symlink(&outside_manifests, repo_dir.join("manifests")).unwrap();

            let err_dir_sym = get_manifest_seam(&reader, "dir_sym_repo", &digest)
                .await
                .unwrap_err();
            match err_dir_sym {
                StorageError::Internal { kind, .. } => {
                    assert_eq!(kind, crate::storage::StorageErrorKind::Io);
                }
                other => panic!("expected StorageErrorKind::Io, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_fs_directory_substituted_for_manifest_rejected() {
            let (_fixture, root) = create_test_root();
            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");
            let hex = "5656565656565656565656565656565656565656565656565656565656565656";
            let digest = test_digest(hex);

            // Create directory instead of file
            let dir_as_manifest = root.join("repos/dir_repo/manifests").join(hex);
            std::fs::create_dir_all(&dir_as_manifest).unwrap();

            let err = get_manifest_seam(&reader, "dir_repo", &digest)
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, crate::storage::StorageErrorKind::Io);
                    assert!(message.contains("unsupported object type"));
                }
                other => panic!("expected unsupported object type Io, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_fs_pinned_root_across_rename() {
            let (fixture, root) = create_test_root();
            let hex = "6767676767676767676767676767676767676767676767676767676767676767";
            let digest = test_digest(hex);

            let content_a = br#"{"schemaVersion":2,"mediaType":"application/vnd.manifest.a+json"}"#;
            write_manifest_file(&root, "pinned_repo", hex, content_a);

            // Open reader against initial root
            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            // Rename root and create fresh directory with different content
            let renamed_root = fixture.path().join("storage-root-renamed");
            std::fs::rename(&root, &renamed_root).expect("rename root");

            std::fs::create_dir_all(&root).expect("recreate empty root");
            let content_b = br#"{"schemaVersion":2,"mediaType":"application/vnd.manifest.b+json"}"#;
            write_manifest_file(&root, "pinned_repo", hex, content_b);

            // Seam read through pinned reader must observe content A from renamed directory!
            let (meta, payload) = get_manifest_seam(&reader, "pinned_repo", &digest)
                .await
                .expect("seam read succeeds via pinned root");
            assert_eq!(meta.media_type, "application/vnd.manifest.a+json");
            assert_eq!(payload.as_ref(), content_a);
        }

        #[tokio::test]
        #[ignore = "requires unprivileged user environment where chmod 0o000 denies filesystem access"]
        async fn test_real_fs_permission_denied_ignored() {
            use std::os::unix::fs::PermissionsExt;

            let (_fixture, root) = create_test_root();
            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");
            let hex = "7878787878787878787878787878787878787878787878787878787878787878";
            let digest = test_digest(hex);
            let manifest_bytes = br#"{"schemaVersion": 2}"#;
            write_manifest_file(&root, "perm_repo", hex, manifest_bytes);

            let manifest_path = root.join("repos/perm_repo/manifests").join(hex);
            let orig_perms = std::fs::metadata(&manifest_path).unwrap().permissions();

            struct ScopedPermReset<'a> {
                path: &'a Path,
                original_permissions: std::fs::Permissions,
            }

            impl<'a> Drop for ScopedPermReset<'a> {
                fn drop(&mut self) {
                    if let Err(err) =
                        std::fs::set_permissions(self.path, self.original_permissions.clone())
                    {
                        if std::thread::panicking() {
                            eprintln!(
                                "ScopedPermReset: failed to restore permissions on {:?} during unwinding: {err}",
                                self.path
                            );
                        } else {
                            panic!(
                                "ScopedPermReset: failed to restore permissions on {:?}: {err}",
                                self.path
                            );
                        }
                    }
                }
            }

            {
                let _guard = ScopedPermReset {
                    path: &manifest_path,
                    original_permissions: orig_perms.clone(),
                };
                std::fs::set_permissions(&manifest_path, std::fs::Permissions::from_mode(0o000))
                    .expect("set mode 0o000");

                if std::fs::read(&manifest_path).is_ok() {
                    panic!("ineffective permissions: read succeeded under mode 0o000");
                }

                let err = get_manifest_seam(&reader, "perm_repo", &digest)
                    .await
                    .unwrap_err();
                match err {
                    StorageError::Internal { kind, .. } => {
                        assert_eq!(kind, crate::storage::StorageErrorKind::Io);
                    }
                    StorageError::NotFound => panic!("Permission denied must not map to NotFound"),
                    other => panic!("expected StorageErrorKind::Io, got: {other:?}"),
                }
            }

            let restored_perms = std::fs::metadata(&manifest_path).unwrap().permissions();
            assert_eq!(restored_perms.mode(), orig_perms.mode());
        }
    }
}
