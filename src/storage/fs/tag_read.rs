//! Contained filesystem tag read implementation for `registry-rust`.
//!
//! # Architectural Ownership Boundaries
//! - `storage-core`: Defines domain-neutral contracts ([`storage_core::ObjectPayloadReader`],
//!   [`storage_core::ObjectPayload`], [`storage_core::ObjectStream`], [`storage_core::ReadError`],
//!   [`storage_core::ObjectKey`]).
//! - `storage-fs`: Implements Linux descriptor-relative containment (`openat2` + `O_PATH` with
//!   `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`), regular file validation (`S_IFREG`),
//!   and Phase 2 readable reopening via `/proc/self/fd/N`.
//! - `registry-rust`: Owns tag key construction (`repos/<repository>/tags/<tag>`),
//!   pre-composition path safety checks, bounded stream draining, separate caller parsing contracts,
//!   raw-byte version hashing, and mapping to [`StorageError`].
//!
//! # Separate Caller Contracts
//! - `resolve_tag`: Validates UTF-8 via [`std::str::from_utf8`] (invalid UTF-8 maps to
//!   [`StorageErrorKind::Io`]), trims whitespace, and parses digest (missing file, empty content,
//!   and malformed digest text map to [`StorageError::NotFound`]).
//! - `get_tag_with_version`: Decodes via [`String::from_utf8_lossy`], trims whitespace,
//!   and parses digest (missing file maps to `Ok(None)`, while empty content, malformed text,
//!   and invalid UTF-8 replacement characters map to [`StorageErrorKind::CorruptData`]).
//!   Hashes the **unmodified original raw bytes** via SHA-256 for optimistic-concurrency versions.
//!
//! # Bounded Stream Draining
//! - `TagReadLimits::max_payload_bytes = None` explicitly means **no seam-imposed ceiling**,
//!   preserving current production `FsStorage` unbounded behavior.
//! - `Some(limit)` enforces an exact byte ceiling: consumes at most $N + 1$ bytes for overflow
//!   detection and accepts at most $N$ bytes.
//! - Checked arithmetic prevents integer overflow (e.g. `u64::MAX`).
//! - Metadata size is treated as an acquisition-time observation and is not used for early rejection,
//!   preventing false rejections if a file shrinks before stream reading.
//!
//! # Concurrency, Containment, and Coherence Demarcation
//! - In production, tag reads resolve through the shared pinned root descriptor (`openat2` on `root_fd`).
//!   They no longer dynamically resolve from `self.root`.
//! - Renaming/replacing the configured root pathname leaves the existing root descriptor referring to
//!   the originally opened directory.
//! - Descendant paths are resolved afresh beneath that descriptor for each payload acquisition.
//! - Replacing a descendant directory or file can therefore affect subsequent acquisitions.
//! - Once acquired, a file descriptor refers to that opened object, but concurrent modification of its
//!   contents can still affect reading.
//! - Root pinning provides neither a namespace snapshot nor read/write coherence.
//! - Tag mutations (`set_tag`, `mutate_tag`, `delete_tag`, `delete_tag_conditional`) resolve through
//!   the same pinned `repos` authority as tag reads (O-04 write-containment cutover), so they observe
//!   the same pinned root: a version token read from a tag drives a matching conditional delete on the
//!   same leaf even across a whole-root rename/replace. Tag read and write are therefore coherent.
//! - Other mutating operations (e.g. `put_manifest`) still resolve ambient pathnames starting from
//!   `self.root`; their pathname-mutation divergence is a residual limitation addressed by separate
//!   O-04 write-containment slices, not this tag slice.
//! - Pathname mutation divergence (for the still-ambient operations above) means that, if the root
//!   path is replaced concurrently, pinned reads continue to resolve beneath the originally opened
//!   root while those pathname mutations operate on the replacement tree.
//! - Read operations do not serialize with advisory locks (`.lock.{tag}`).
//! - Payload reads do not provide snapshot isolation: concurrent writes or file truncation during stream
//!   draining may return changed or partial bytes; detection is not guaranteed.
//! - Version hashing describes the raw bytes actually read and does not bind a version to a root directory or inode.
//! - Identical byte content produces identical version hashes across different roots or files.
//! - Linux descriptor-relative containment (`openat2`) requires a genuine, accessible, stable procfs mount
//!   for Phase 2 readable reopenings via `/proc/self/fd/N`.
//! - Kernel containment does not guarantee mount or hard-link isolation.
//! - Non-Linux verification remains unperformed.

use crate::registry::digest::Digest;
use crate::storage::StorageError;
use sha2::Digest as Sha2Digest;
use storage_core::{ObjectKey, ObjectPayload, ObjectPayloadReader, ReadError};
use tokio::io::AsyncReadExt;

/// Caller-supplied limits for tag payload reading.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) struct TagReadLimits {
    /// Maximum bytes to read from a tag payload stream.
    /// If None, reads without a seam-imposed ceiling (preserving existing unbounded behavior).
    /// If Some(limit), enforces an exact ceiling against stream bytes.
    pub max_payload_bytes: Option<u64>,
}

/// Constructs the relative [`ObjectKey`] for a repository tag:
/// `repos/<repository>/tags/<tag>`.
///
/// # Validation Checks
/// Explicitly validates `repo` and `tag` before composing the key:
/// - Rejects empty strings.
/// - Rejects leading or trailing `/` characters.
/// - Rejects backslashes (`\`), NUL bytes, and ASCII control characters.
/// - Rejects empty segments (consecutive slashes `//`).
/// - Rejects `.` (current directory) and `..` (parent directory) segments.
///
/// Unsafe inputs are rejected with [`StorageError::InvalidRepoName`] without silent normalization.
/// Note: Nested valid path components (e.g. `sub/nested_tag`) remain permitted under structural
/// safety checks; strict OCI tag grammar is deferred.
pub(crate) fn tag_key(repo: &str, tag: &str) -> Result<ObjectKey, StorageError> {
    validate_path_component(repo, "repository name")?;
    validate_path_component(tag, "tag name")?;

    let key_str = format!("repos/{repo}/tags/{tag}");
    ObjectKey::parse(&key_str).map_err(|e| StorageError::InvalidRepoName(e.to_string()))
}

pub(crate) fn validate_path_component(
    component: &str,
    field_name: &str,
) -> Result<(), StorageError> {
    if component.is_empty() {
        return Err(StorageError::InvalidRepoName(format!(
            "{field_name} cannot be empty"
        )));
    }
    if component.starts_with('/') || component.ends_with('/') {
        return Err(StorageError::InvalidRepoName(format!(
            "{field_name} cannot have leading or trailing slashes"
        )));
    }
    if component.contains('\\') {
        return Err(StorageError::InvalidRepoName(format!(
            "{field_name} cannot contain backslashes"
        )));
    }
    if component.contains(|c: char| c == '\0' || c.is_ascii_control()) {
        return Err(StorageError::InvalidRepoName(format!(
            "{field_name} cannot contain NUL bytes or control characters"
        )));
    }

    for segment in component.split('/') {
        if segment.is_empty() {
            return Err(StorageError::InvalidRepoName(format!(
                "{field_name} cannot contain empty segments (repeated slashes)"
            )));
        }
        if segment == "." {
            return Err(StorageError::InvalidRepoName(format!(
                "{field_name} cannot contain '.' segments"
            )));
        }
        if segment == ".." {
            return Err(StorageError::InvalidRepoName(format!(
                "{field_name} cannot contain '..' segments (path traversal attempt)"
            )));
        }
    }
    Ok(())
}

/// Drains a tag payload stream in accordance with caller-supplied [`TagReadLimits`].
///
/// - If `limits.max_payload_bytes == None`: drains without a seam-imposed ceiling.
/// - If `limits.max_payload_bytes == Some(limit)`: consumes at most `limit + 1` bytes
///   using `stream.take(...)` to detect stream overflow safely.
/// - Rejects overflow beyond `limit` with [`StorageErrorKind::CorruptData`].
/// - If `limit == 0`: accepts empty streams (0 bytes) and rejects non-empty streams.
pub(crate) async fn drain_tag_stream(
    payload: ObjectPayload,
    limits: &TagReadLimits,
) -> Result<Vec<u8>, StorageError> {
    let (_metadata, stream) = payload.into_parts();

    match limits.max_payload_bytes {
        None => {
            let mut buffer = Vec::new();
            let mut pinned_stream = stream;
            pinned_stream
                .read_to_end(&mut buffer)
                .await
                .map_err(|e| StorageError::io(format!("failed to read tag payload stream: {e}")))?;
            Ok(buffer)
        }
        Some(limit) => {
            let take_limit = limit.checked_add(1).ok_or_else(|| {
                StorageError::corrupt_data(format!(
                    "tag payload limit {limit} cannot be represented for bounded draining"
                ))
            })?;

            let mut limited_stream = stream.take(take_limit);
            let mut buffer = Vec::new();
            limited_stream
                .read_to_end(&mut buffer)
                .await
                .map_err(|e| StorageError::io(format!("failed to read tag payload stream: {e}")))?;

            if buffer.len() as u64 > limit {
                return Err(StorageError::corrupt_data(format!(
                    "tag payload stream length exceeds limit of {limit} bytes"
                )));
            }

            Ok(buffer)
        }
    }
}

/// Resolves a tag to its target [`Digest`] using descriptor-relative containment.
///
/// Preserves legacy `resolve_tag` contract:
/// - Missing file -> [`StorageError::NotFound`].
/// - Stream read error -> [`StorageErrorKind::Io`].
/// - Invalid UTF-8 bytes -> [`StorageErrorKind::Io`].
/// - Empty content / malformed digest text -> [`StorageError::NotFound`].
/// - Whitespace trimmed before parsing.
pub(crate) async fn resolve_tag(
    reader: &(impl ObjectPayloadReader + ?Sized),
    repo: &str,
    tag: &str,
    limits: &TagReadLimits,
) -> Result<Digest, StorageError> {
    let key = tag_key(repo, tag)?;

    let payload = match reader.open_payload(&key).await {
        Ok(p) => p,
        Err(ReadError::NotFound { .. }) => return Err(StorageError::NotFound),
        Err(other) => return Err(super::read_adapter::translate_payload_read_error(other)),
    };

    let bytes = drain_tag_stream(payload, limits).await?;

    let s = std::str::from_utf8(&bytes)
        .map_err(|e| StorageError::io(format!("invalid utf-8 sequence in tag file {key}: {e}")))?;

    let reference = s.trim();
    Digest::parse(reference).map_err(|_| StorageError::NotFound)
}

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use resolve_tag as resolve_tag_seam;

/// Retrieves a tag target [`Digest`] and optimistic-concurrency version token.
///
/// Preserves legacy `get_tag_with_version` contract:
/// - Missing file -> `Ok(None)`.
/// - Stream read error -> [`StorageErrorKind::Io`].
/// - Empty content / malformed text / invalid UTF-8 replacement chars -> [`StorageErrorKind::CorruptData`].
/// - Version is computed strictly as the SHA-256 hex digest of the raw unmodified bytes (`&bytes`).
pub(crate) async fn get_tag_with_version(
    reader: &(impl ObjectPayloadReader + ?Sized),
    repo: &str,
    tag: &str,
    limits: &TagReadLimits,
) -> Result<Option<(Digest, String)>, StorageError> {
    let key = tag_key(repo, tag)?;

    let payload = match reader.open_payload(&key).await {
        Ok(p) => p,
        Err(ReadError::NotFound { .. }) => return Ok(None),
        Err(other) => return Err(super::read_adapter::translate_payload_read_error(other)),
    };

    let bytes = drain_tag_stream(payload, limits).await?;

    let s = String::from_utf8_lossy(&bytes);
    let digest = Digest::parse(s.trim())
        .map_err(|e| StorageError::corrupt_data(format!("corrupt tag {tag}: {e}")))?;

    let mut hasher = sha2::Sha256::new();
    hasher.update(&bytes);
    let version = hex::encode(hasher.finalize());

    Ok(Some((digest, version)))
}

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use get_tag_with_version as get_tag_with_version_seam;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageErrorKind;
    use async_trait::async_trait;
    use std::collections::{HashMap, VecDeque};
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use storage_core::{ObjectMetadata, ObjectStream};
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
    // Category A: Key Construction and Traversal Rejection Tests
    // ========================================================================

    #[test]
    fn test_tag_key_valid_single_and_nested() {
        let key1 = tag_key("myrepo", "latest").expect("valid key");
        assert_eq!(key1.as_str(), "repos/myrepo/tags/latest");

        let key2 = tag_key("org/team/app", "v1.0.0").expect("valid nested key");
        assert_eq!(key2.as_str(), "repos/org/team/app/tags/v1.0.0");

        let key3 = tag_key("repo", "sub/nested_tag").expect("valid nested tag key");
        assert_eq!(key3.as_str(), "repos/repo/tags/sub/nested_tag");
    }

    #[test]
    fn test_tag_key_structural_rejections() {
        assert!(matches!(
            tag_key("", "latest"),
            Err(StorageError::InvalidRepoName(_))
        ));
        assert!(matches!(
            tag_key("repo", ""),
            Err(StorageError::InvalidRepoName(_))
        ));
        assert!(matches!(
            tag_key("/repo", "latest"),
            Err(StorageError::InvalidRepoName(_))
        ));
        assert!(matches!(
            tag_key("repo/", "latest"),
            Err(StorageError::InvalidRepoName(_))
        ));
        assert!(matches!(
            tag_key("repo", "/latest"),
            Err(StorageError::InvalidRepoName(_))
        ));
        assert!(matches!(
            tag_key("repo", "latest/"),
            Err(StorageError::InvalidRepoName(_))
        ));
        assert!(matches!(
            tag_key("repo//sub", "latest"),
            Err(StorageError::InvalidRepoName(_))
        ));
        assert!(matches!(
            tag_key("repo", "tag//sub"),
            Err(StorageError::InvalidRepoName(_))
        ));
        assert!(matches!(
            tag_key("repo/../sibling", "latest"),
            Err(StorageError::InvalidRepoName(_))
        ));
        assert!(matches!(
            tag_key("repo", "../outside"),
            Err(StorageError::InvalidRepoName(_))
        ));
        assert!(matches!(
            tag_key("repo/./current", "latest"),
            Err(StorageError::InvalidRepoName(_))
        ));
        assert!(matches!(
            tag_key("repo", "./tag"),
            Err(StorageError::InvalidRepoName(_))
        ));
        assert!(matches!(
            tag_key("repo\\bad", "latest"),
            Err(StorageError::InvalidRepoName(_))
        ));
        assert!(matches!(
            tag_key("repo", "tag\\bad"),
            Err(StorageError::InvalidRepoName(_))
        ));
        assert!(matches!(
            tag_key("repo\0null", "latest"),
            Err(StorageError::InvalidRepoName(_))
        ));
        assert!(matches!(
            tag_key("repo", "tag\x01ctrl"),
            Err(StorageError::InvalidRepoName(_))
        ));
    }

    #[tokio::test]
    async fn test_seam_key_validation_zero_reader_calls() {
        let fake = RecordingFakePayloadReader::new();
        let limits = TagReadLimits::default();

        let err_resolve = resolve_tag_seam(&fake, "../escaperepo", "latest", &limits)
            .await
            .unwrap_err();
        assert!(matches!(err_resolve, StorageError::InvalidRepoName(_)));

        let err_version = get_tag_with_version_seam(&fake, "myrepo", "../escapetag", &limits)
            .await
            .unwrap_err();
        assert!(matches!(err_version, StorageError::InvalidRepoName(_)));

        assert_eq!(
            fake.calls().len(),
            0,
            "reader must not be called on invalid keys"
        );
    }

    // ========================================================================
    // Category B: Separate Parsing and Digest Algorithm Tests
    // ========================================================================

    #[tokio::test]
    async fn test_seam_resolve_and_get_tag_valid_sha256() {
        let fake = RecordingFakePayloadReader::new();
        let key = ObjectKey::parse("repos/myrepo/tags/latest").unwrap();
        let hex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let content = format!("sha256:{hex}\n");

        fake.script(key.clone(), Ok(mock_payload(content.as_bytes().to_vec())));
        fake.script(key.clone(), Ok(mock_payload(content.as_bytes().to_vec())));

        let limits = TagReadLimits::default();
        let digest_resolve = resolve_tag_seam(&fake, "myrepo", "latest", &limits)
            .await
            .expect("resolve succeeds");
        assert_eq!(digest_resolve.hex(), hex);

        let opt_version = get_tag_with_version_seam(&fake, "myrepo", "latest", &limits)
            .await
            .expect("get_tag succeeds");
        let (digest_get, version) = opt_version.expect("tag found");
        assert_eq!(digest_get.hex(), hex);

        let mut hasher = sha2::Sha256::new();
        hasher.update(content.as_bytes());
        let expected_version = hex::encode(hasher.finalize());
        assert_eq!(version, expected_version);
    }

    #[tokio::test]
    async fn test_seam_resolve_and_get_tag_valid_sha512() {
        let fake = RecordingFakePayloadReader::new();
        let key = ObjectKey::parse("repos/myrepo/tags/v512").unwrap();
        let hex = "51251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251251";
        let content = format!("sha512:{hex}\n");

        fake.script(key.clone(), Ok(mock_payload(content.as_bytes().to_vec())));
        fake.script(key.clone(), Ok(mock_payload(content.as_bytes().to_vec())));

        let limits = TagReadLimits::default();
        let digest_resolve = resolve_tag_seam(&fake, "myrepo", "v512", &limits)
            .await
            .expect("resolve sha512 succeeds");
        assert_eq!(digest_resolve.hex(), hex);
        assert_eq!(digest_resolve.algorithm(), "sha512");

        let (digest_get, version) = get_tag_with_version_seam(&fake, "myrepo", "v512", &limits)
            .await
            .expect("get sha512 succeeds")
            .expect("tag found");
        assert_eq!(digest_get.hex(), hex);

        let mut hasher = sha2::Sha256::new();
        hasher.update(content.as_bytes());
        assert_eq!(version, hex::encode(hasher.finalize()));
    }

    #[tokio::test]
    async fn test_seam_resolve_and_get_tag_padded_whitespace() {
        let fake = RecordingFakePayloadReader::new();
        let key = ObjectKey::parse("repos/myrepo/tags/padded").unwrap();
        let hex = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let padded = format!("   \t\r\n   sha256:{hex}   \r\n\t  \n");

        fake.script(key.clone(), Ok(mock_payload(padded.as_bytes().to_vec())));
        fake.script(key.clone(), Ok(mock_payload(padded.as_bytes().to_vec())));

        let limits = TagReadLimits::default();
        let d = resolve_tag_seam(&fake, "myrepo", "padded", &limits)
            .await
            .expect("resolve padded tag succeeds");
        assert_eq!(d.hex(), hex);

        let (d_get, version) = get_tag_with_version_seam(&fake, "myrepo", "padded", &limits)
            .await
            .expect("get padded tag succeeds")
            .expect("tag found");
        assert_eq!(d_get.hex(), hex);

        // Version reflects padded bytes exactly
        let mut hasher = sha2::Sha256::new();
        hasher.update(padded.as_bytes());
        assert_eq!(version, hex::encode(hasher.finalize()));
    }

    #[tokio::test]
    async fn test_seam_raw_byte_version_hash_whitespace_sensitivity() {
        let fake = RecordingFakePayloadReader::new();
        let key = ObjectKey::parse("repos/myrepo/tags/vtest").unwrap();
        let hex = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

        let c1 = format!("sha256:{hex}");
        let c2 = format!("sha256:{hex}\n");
        let c3 = format!("sha256:{hex}\r\n");
        let c4 = format!("  sha256:{hex}\n");

        fake.script(key.clone(), Ok(mock_payload(c1.as_bytes().to_vec())));
        fake.script(key.clone(), Ok(mock_payload(c2.as_bytes().to_vec())));
        fake.script(key.clone(), Ok(mock_payload(c3.as_bytes().to_vec())));
        fake.script(key.clone(), Ok(mock_payload(c4.as_bytes().to_vec())));

        let limits = TagReadLimits::default();
        let (_, v1) = get_tag_with_version_seam(&fake, "myrepo", "vtest", &limits)
            .await
            .unwrap()
            .unwrap();
        let (_, v2) = get_tag_with_version_seam(&fake, "myrepo", "vtest", &limits)
            .await
            .unwrap()
            .unwrap();
        let (_, v3) = get_tag_with_version_seam(&fake, "myrepo", "vtest", &limits)
            .await
            .unwrap()
            .unwrap();
        let (_, v4) = get_tag_with_version_seam(&fake, "myrepo", "vtest", &limits)
            .await
            .unwrap()
            .unwrap();

        assert_ne!(v1, v2);
        assert_ne!(v1, v3);
        assert_ne!(v2, v3);
        assert_ne!(v2, v4);
    }

    // ========================================================================
    // Category C: Error Taxonomy Divergence (Empty, Corrupt, Invalid UTF-8, Missing)
    // ========================================================================

    #[tokio::test]
    async fn test_seam_missing_tag_taxonomy() {
        let fake = RecordingFakePayloadReader::new();
        let key = ObjectKey::parse("repos/myrepo/tags/missing").unwrap();

        fake.script(key.clone(), Err(ReadError::not_found(key.clone())));
        fake.script(key.clone(), Err(ReadError::not_found(key.clone())));

        let limits = TagReadLimits::default();
        let err_resolve = resolve_tag_seam(&fake, "myrepo", "missing", &limits)
            .await
            .unwrap_err();
        assert!(matches!(err_resolve, StorageError::NotFound));

        let opt_get = get_tag_with_version_seam(&fake, "myrepo", "missing", &limits)
            .await
            .expect("get_tag missing returns Ok(None)");
        assert!(opt_get.is_none());
    }

    #[tokio::test]
    async fn test_seam_empty_file_taxonomy() {
        let fake = RecordingFakePayloadReader::new();
        let key = ObjectKey::parse("repos/myrepo/tags/empty").unwrap();

        fake.script(key.clone(), Ok(mock_payload(vec![])));
        fake.script(key.clone(), Ok(mock_payload(vec![])));

        let limits = TagReadLimits::default();
        // resolve_tag maps empty parse failure to NotFound
        let err_resolve = resolve_tag_seam(&fake, "myrepo", "empty", &limits)
            .await
            .unwrap_err();
        assert!(matches!(err_resolve, StorageError::NotFound));

        // get_tag_with_version maps empty parse failure to CorruptData
        let err_get = get_tag_with_version_seam(&fake, "myrepo", "empty", &limits)
            .await
            .unwrap_err();
        match err_get {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::CorruptData),
            other => panic!("expected CorruptData for empty tag, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_seam_malformed_text_taxonomy() {
        let fake = RecordingFakePayloadReader::new();
        let key = ObjectKey::parse("repos/myrepo/tags/badtext").unwrap();
        let bad_content = b"not-a-valid-digest\n".to_vec();

        fake.script(key.clone(), Ok(mock_payload(bad_content.clone())));
        fake.script(key.clone(), Ok(mock_payload(bad_content)));

        let limits = TagReadLimits::default();
        // resolve_tag maps malformed text to NotFound
        let err_resolve = resolve_tag_seam(&fake, "myrepo", "badtext", &limits)
            .await
            .unwrap_err();
        assert!(matches!(err_resolve, StorageError::NotFound));

        // get_tag_with_version maps malformed text to CorruptData
        let err_get = get_tag_with_version_seam(&fake, "myrepo", "badtext", &limits)
            .await
            .unwrap_err();
        match err_get {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::CorruptData),
            other => panic!("expected CorruptData for malformed text, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_seam_invalid_utf8_taxonomy() {
        let fake = RecordingFakePayloadReader::new();
        let key = ObjectKey::parse("repos/myrepo/tags/badutf8").unwrap();
        let invalid_utf8 = vec![0xff, 0xfe, 0xfd];

        fake.script(key.clone(), Ok(mock_payload(invalid_utf8.clone())));
        fake.script(key.clone(), Ok(mock_payload(invalid_utf8)));

        let limits = TagReadLimits::default();
        // resolve_tag fails UTF-8 validation and maps to StorageErrorKind::Io
        let err_resolve = resolve_tag_seam(&fake, "myrepo", "badutf8", &limits)
            .await
            .unwrap_err();
        match err_resolve {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
            other => panic!("expected Io for invalid UTF-8 in resolve_tag, got {other:?}"),
        }

        // get_tag_with_version uses lossy UTF-8 (yielding \u{FFFD}), which fails Digest::parse -> CorruptData
        let err_get = get_tag_with_version_seam(&fake, "myrepo", "badutf8", &limits)
            .await
            .unwrap_err();
        match err_get {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::CorruptData),
            other => panic!("expected CorruptData for invalid UTF-8 in get_tag, got {other:?}"),
        }
    }

    // ========================================================================
    // Category D: Limits, Overflow, and Precision Draining Tests
    // ========================================================================

    #[tokio::test]
    async fn test_seam_unbounded_large_padded_payload() {
        let fake = RecordingFakePayloadReader::new();
        let key = ObjectKey::parse("repos/myrepo/tags/huge").unwrap();
        let hex = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

        // 70 KiB of whitespace padding + valid digest
        let mut huge_content = vec![b' '; 70 * 1024];
        huge_content.extend_from_slice(format!("sha256:{hex}\n").as_bytes());

        fake.script(key.clone(), Ok(mock_payload(huge_content.clone())));
        fake.script(key.clone(), Ok(mock_payload(huge_content)));

        let limits = TagReadLimits::default(); // max_payload_bytes: None
        let d = resolve_tag_seam(&fake, "myrepo", "huge", &limits)
            .await
            .expect("unbounded read succeeds for >64 KiB");
        assert_eq!(d.hex(), hex);

        let (d_get, _) = get_tag_with_version_seam(&fake, "myrepo", "huge", &limits)
            .await
            .expect("unbounded get_tag succeeds for >64 KiB")
            .expect("tag found");
        assert_eq!(d_get.hex(), hex);
    }

    #[tokio::test]
    async fn test_seam_limit_zero_behavior() {
        let fake = RecordingFakePayloadReader::new();
        let key_empty = ObjectKey::parse("repos/myrepo/tags/empty").unwrap();
        let key_nonempty = ObjectKey::parse("repos/myrepo/tags/nonempty").unwrap();

        fake.script(key_empty.clone(), Ok(mock_payload(vec![])));
        fake.script(key_empty.clone(), Ok(mock_payload(vec![])));
        fake.script(key_nonempty.clone(), Ok(mock_payload(vec![b'a'])));
        fake.script(key_nonempty.clone(), Ok(mock_payload(vec![b'a'])));

        let limits = TagReadLimits {
            max_payload_bytes: Some(0),
        };

        // Empty file: drain accepts empty bytes; resolve_tag returns NotFound, get_tag returns CorruptData
        let err_res_empty = resolve_tag_seam(&fake, "myrepo", "empty", &limits)
            .await
            .unwrap_err();
        assert!(matches!(err_res_empty, StorageError::NotFound));

        let err_get_empty = get_tag_with_version_seam(&fake, "myrepo", "empty", &limits)
            .await
            .unwrap_err();
        match err_get_empty {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::CorruptData),
            other => panic!("expected CorruptData, got {other:?}"),
        }

        // Non-empty file (1 byte): drain layer rejects with CorruptData
        let err_res_nonempty = resolve_tag_seam(&fake, "myrepo", "nonempty", &limits)
            .await
            .unwrap_err();
        match err_res_nonempty {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::CorruptData),
            other => panic!("expected CorruptData, got {other:?}"),
        }

        let err_get_nonempty = get_tag_with_version_seam(&fake, "myrepo", "nonempty", &limits)
            .await
            .unwrap_err();
        match err_get_nonempty {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::CorruptData),
            other => panic!("expected CorruptData, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_seam_limit_exact_and_one_over() {
        let fake = RecordingFakePayloadReader::new();
        let key_exact = ObjectKey::parse("repos/myrepo/tags/exact").unwrap();
        let key_over = ObjectKey::parse("repos/myrepo/tags/over").unwrap();
        let hex = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
        let content = format!("sha256:{hex}\n"); // 72 bytes
        let len = content.len() as u64;

        fake.script(
            key_exact.clone(),
            Ok(mock_payload(content.as_bytes().to_vec())),
        );
        fake.script(
            key_over.clone(),
            Ok(mock_payload(content.as_bytes().to_vec())),
        );

        // Exact limit: len == 72
        let limits_exact = TagReadLimits {
            max_payload_bytes: Some(len),
        };
        let d = resolve_tag_seam(&fake, "myrepo", "exact", &limits_exact)
            .await
            .expect("exact limit succeeds");
        assert_eq!(d.hex(), hex);

        // One-byte-under limit: len - 1 == 71 (content is 72) -> rejected
        let limits_under = TagReadLimits {
            max_payload_bytes: Some(len - 1),
        };
        let err_over = resolve_tag_seam(&fake, "myrepo", "over", &limits_under)
            .await
            .unwrap_err();
        match err_over {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::CorruptData),
            other => panic!("expected CorruptData on one-over, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_seam_limit_u64_max_overflow_rejection() {
        let fake = RecordingFakePayloadReader::new();
        let key = ObjectKey::parse("repos/myrepo/tags/overflow").unwrap();
        fake.script(key.clone(), Ok(mock_payload(b"content".to_vec())));

        let limits = TagReadLimits {
            max_payload_bytes: Some(u64::MAX),
        };
        let err = resolve_tag_seam(&fake, "myrepo", "overflow", &limits)
            .await
            .unwrap_err();
        match err {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::CorruptData),
            other => panic!("expected CorruptData on limit overflow, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_seam_understated_metadata_valid_digest() {
        let fake = RecordingFakePayloadReader::new();
        let key = ObjectKey::parse("repos/myrepo/tags/understated").unwrap();
        let hex = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let content = format!("sha256:{hex}\n"); // 72 bytes

        // Metadata understated as 10 bytes; stream produces valid 72-byte digest within limit 100
        fake.script(
            key.clone(),
            Ok(mock_payload_with_meta_size(10, content.as_bytes().to_vec())),
        );

        let limits = TagReadLimits {
            max_payload_bytes: Some(100),
        };
        let d = resolve_tag_seam(&fake, "myrepo", "understated", &limits)
            .await
            .expect("understated metadata does not block stream reading");
        assert_eq!(d.hex(), hex);
    }

    #[tokio::test]
    async fn test_seam_drain_understated_metadata_helper() {
        // Direct test of drain helper with understated metadata producing 50 bytes within limit 100
        let payload = mock_payload_with_meta_size(10, vec![b'z'; 50]);
        let limits = TagReadLimits {
            max_payload_bytes: Some(100),
        };
        let bytes = drain_tag_stream(payload, &limits)
            .await
            .expect("drain helper succeeds");
        assert_eq!(bytes.len(), 50);
    }

    #[tokio::test]
    async fn test_seam_overstated_metadata_valid_digest() {
        let fake = RecordingFakePayloadReader::new();
        let key = ObjectKey::parse("repos/myrepo/tags/overstated").unwrap();
        let hex = "0000000000000000000000000000000000000000000000000000000000000000";
        let content = format!("sha256:{hex}\n"); // 72 bytes

        // Metadata overstated as 1000 bytes; stream produces valid 72-byte digest within limit 100
        fake.script(
            key.clone(),
            Ok(mock_payload_with_meta_size(
                1000,
                content.as_bytes().to_vec(),
            )),
        );

        let limits = TagReadLimits {
            max_payload_bytes: Some(100),
        };
        let d = resolve_tag_seam(&fake, "myrepo", "overstated", &limits)
            .await
            .expect("overstated metadata is ignored for early rejection");
        assert_eq!(d.hex(), hex);
    }

    #[tokio::test]
    async fn test_seam_stream_io_failure_partial() {
        let fake = RecordingFakePayloadReader::new();
        let key = ObjectKey::parse("repos/myrepo/tags/failing").unwrap();
        let limits = TagReadLimits::default();

        // 1. Exercise resolve_tag_seam with stream that fails mid-stream
        let failing_stream1 = FailingStream::new(
            vec![b'x'; 20],
            std::io::ErrorKind::ConnectionReset,
            "stream broken",
        );
        let payload1 = ObjectPayload::new(ObjectMetadata::new(50), Box::pin(failing_stream1));
        fake.script(key.clone(), Ok(payload1));

        let err1 = resolve_tag_seam(&fake, "myrepo", "failing", &limits)
            .await
            .unwrap_err();
        match err1 {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
            other => panic!("expected Io error on mid-stream failure, got {other:?}"),
        }

        // 2. Exercise get_tag_with_version_seam with stream that fails mid-stream
        let failing_stream2 = FailingStream::new(
            vec![b'x'; 20],
            std::io::ErrorKind::ConnectionReset,
            "stream broken",
        );
        let payload2 = ObjectPayload::new(ObjectMetadata::new(50), Box::pin(failing_stream2));
        fake.script(key.clone(), Ok(payload2));

        let err2 = get_tag_with_version_seam(&fake, "myrepo", "failing", &limits)
            .await
            .unwrap_err();
        match err2 {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
            other => panic!("expected Io error on mid-stream failure, got {other:?}"),
        }
    }

    // ========================================================================
    // Category E: Typed Filesystem Error Translation Tests
    // ========================================================================

    #[derive(Debug)]
    struct CustomBackendError;

    impl std::fmt::Display for CustomBackendError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "custom backend error neither FsMetadataError nor io::Error"
            )
        }
    }

    impl std::error::Error for CustomBackendError {}

    #[tokio::test]
    async fn test_seam_typed_fs_error_mappings() {
        let fake = RecordingFakePayloadReader::new();
        let key = ObjectKey::parse("repos/myrepo/tags/err").unwrap();
        let limits = TagReadLimits::default();

        async fn assert_both_seams_error<F>(
            fake: &RecordingFakePayloadReader,
            key: &ObjectKey,
            make_err: F,
            expected_kind: StorageErrorKind,
            limits: &TagReadLimits,
        ) where
            F: Fn() -> ReadError,
        {
            fake.script(key.clone(), Err(make_err()));
            fake.script(key.clone(), Err(make_err()));

            let err_res = resolve_tag_seam(fake, "myrepo", "err", limits)
                .await
                .unwrap_err();
            match err_res {
                StorageError::Internal { kind, .. } => assert_eq!(kind, expected_kind),
                other => panic!("expected {expected_kind:?} on resolve_tag_seam, got {other:?}"),
            }

            let err_get = get_tag_with_version_seam(fake, "myrepo", "err", limits)
                .await
                .unwrap_err();
            match err_get {
                StorageError::Internal { kind, .. } => assert_eq!(kind, expected_kind),
                other => {
                    panic!("expected {expected_kind:?} on get_tag_with_version_seam, got {other:?}")
                }
            }
        }

        // 1. PermissionDenied -> Io on both APIs
        assert_both_seams_error(
            &fake,
            &key,
            || ReadError::permission_denied(key.clone()),
            StorageErrorKind::Io,
            &limits,
        )
        .await;

        // 2. ResolutionRejected (symlink rejected) -> Io on both APIs
        assert_both_seams_error(
            &fake,
            &key,
            || {
                ReadError::backend_with_source(
                    "resolution rejected",
                    Box::new(storage_fs::FsMetadataError::ResolutionRejected {
                        raw_os_error: libc::ELOOP,
                        source: std::io::Error::from_raw_os_error(libc::ELOOP),
                    }),
                )
            },
            StorageErrorKind::Io,
            &limits,
        )
        .await;

        // 3. UnsupportedObjectType (directory) -> Io on both APIs
        assert_both_seams_error(
            &fake,
            &key,
            || {
                ReadError::backend_with_source(
                    "unsupported object type",
                    Box::new(storage_fs::FsMetadataError::UnsupportedObjectType {
                        mode: libc::S_IFDIR,
                    }),
                )
            },
            StorageErrorKind::Io,
            &limits,
        )
        .await;

        // 4. SyscallUnsupported (openat2 missing) -> Configuration on both APIs
        assert_both_seams_error(
            &fake,
            &key,
            || {
                ReadError::backend_with_source(
                    "syscall unsupported",
                    Box::new(storage_fs::FsMetadataError::SyscallUnsupported(
                        std::io::Error::from_raw_os_error(libc::ENOSYS),
                    )),
                )
            },
            StorageErrorKind::Configuration,
            &limits,
        )
        .await;

        // 5. StatFailed -> Io on both APIs
        assert_both_seams_error(
            &fake,
            &key,
            || {
                ReadError::backend_with_source(
                    "stat failed",
                    Box::new(storage_fs::FsMetadataError::StatFailed {
                        stage: "Phase 1",
                        source: std::io::Error::from_raw_os_error(libc::EACCES),
                    }),
                )
            },
            StorageErrorKind::Io,
            &limits,
        )
        .await;

        // 6. ProcfsReopenFailed -> Io on both APIs
        assert_both_seams_error(
            &fake,
            &key,
            || {
                ReadError::backend_with_source(
                    "procfs reopen failed",
                    Box::new(storage_fs::FsMetadataError::ProcfsReopenFailed {
                        source: std::io::Error::from_raw_os_error(libc::ENOENT),
                    }),
                )
            },
            StorageErrorKind::Io,
            &limits,
        )
        .await;

        // 7. IdentityMismatch -> Io on both APIs
        assert_both_seams_error(
            &fake,
            &key,
            || {
                ReadError::backend_with_source(
                    "identity mismatch",
                    Box::new(storage_fs::FsMetadataError::IdentityMismatch {
                        expected_dev: 1,
                        expected_ino: 2,
                        actual_dev: 1,
                        actual_ino: 3,
                    }),
                )
            },
            StorageErrorKind::Io,
            &limits,
        )
        .await;

        // 8. InvalidMetadata -> Io on both APIs
        assert_both_seams_error(
            &fake,
            &key,
            || {
                ReadError::backend_with_source(
                    "invalid metadata",
                    Box::new(storage_fs::FsMetadataError::InvalidMetadata {
                        message: "negative size",
                    }),
                )
            },
            StorageErrorKind::Io,
            &limits,
        )
        .await;

        // 9. PlatformUnsupported -> Io on both APIs
        assert_both_seams_error(
            &fake,
            &key,
            || {
                ReadError::backend_with_source(
                    "platform unsupported",
                    Box::new(storage_fs::FsMetadataError::PlatformUnsupported),
                )
            },
            StorageErrorKind::Io,
            &limits,
        )
        .await;

        // 10. RuntimeMissing -> Backend on both APIs
        assert_both_seams_error(
            &fake,
            &key,
            || {
                let rt_err =
                    std::thread::spawn(|| tokio::runtime::Handle::try_current().unwrap_err())
                        .join()
                        .unwrap();
                ReadError::backend_with_source(
                    "runtime missing",
                    Box::new(storage_fs::FsMetadataError::RuntimeMissing(rt_err)),
                )
            },
            StorageErrorKind::Backend,
            &limits,
        )
        .await;

        // 11. TaskJoinFailed -> Backend on both APIs
        let join_err1 = tokio::spawn(async { panic!("forced panic for join error 1") })
            .await
            .unwrap_err();
        let join_err2 = tokio::spawn(async { panic!("forced panic for join error 2") })
            .await
            .unwrap_err();
        fake.script(
            key.clone(),
            Err(ReadError::backend_with_source(
                "task join failed",
                Box::new(storage_fs::FsMetadataError::TaskJoinFailed(join_err1)),
            )),
        );
        fake.script(
            key.clone(),
            Err(ReadError::backend_with_source(
                "task join failed",
                Box::new(storage_fs::FsMetadataError::TaskJoinFailed(join_err2)),
            )),
        );
        let err_join_res = resolve_tag_seam(&fake, "myrepo", "err", &limits)
            .await
            .unwrap_err();
        assert!(matches!(
            err_join_res,
            StorageError::Internal {
                kind: StorageErrorKind::Backend,
                ..
            }
        ));
        let err_join_get = get_tag_with_version_seam(&fake, "myrepo", "err", &limits)
            .await
            .unwrap_err();
        assert!(matches!(
            err_join_get,
            StorageError::Internal {
                kind: StorageErrorKind::Backend,
                ..
            }
        ));

        // 12. Backend source that is neither FsMetadataError nor std::io::Error -> Io on both APIs
        assert_both_seams_error(
            &fake,
            &key,
            || {
                ReadError::backend_with_source(
                    "custom backend failure",
                    Box::new(CustomBackendError),
                )
            },
            StorageErrorKind::Io,
            &limits,
        )
        .await;

        // 13. Generic Backend with std::io::Error source -> Io on both APIs
        assert_both_seams_error(
            &fake,
            &key,
            || {
                ReadError::backend_with_source(
                    "io backend failure",
                    Box::new(std::io::Error::new(std::io::ErrorKind::Other, "custom io")),
                )
            },
            StorageErrorKind::Io,
            &limits,
        )
        .await;

        // 14. Generic Backend without source -> Io on both APIs
        assert_both_seams_error(
            &fake,
            &key,
            || ReadError::backend("generic backend failure"),
            StorageErrorKind::Io,
            &limits,
        )
        .await;
    }

    // ========================================================================
    // Category F: Real Filesystem Linux-Gated Tests
    // ========================================================================

    #[cfg(target_os = "linux")]
    mod real_fs_tests {
        use super::*;
        use crate::storage::Storage;
        use crate::storage::fs::FsStorage;
        use std::path::{Path, PathBuf};

        fn create_test_root() -> (tempfile::TempDir, PathBuf) {
            let fixture = tempfile::tempdir().expect("create tempdir");
            let root = fixture.path().join("storage-root");
            std::fs::create_dir_all(&root).expect("create storage root");
            (fixture, root)
        }

        fn write_tag_file(root: &Path, repo: &str, tag: &str, content: &[u8]) {
            let dir = root.join("repos").join(repo).join("tags");
            std::fs::create_dir_all(&dir).expect("create tags dir");
            std::fs::write(dir.join(tag), content).expect("write tag");
        }

        #[tokio::test]
        async fn test_seam_real_shared_reader_identity() {
            let (_fixture, root) = create_test_root();
            let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");

            // Verify reader instance identity against the existing production read adapter
            let seam_reader_ptr = Arc::as_ptr(&storage.reader);
            let adapter_reader_ptr = Arc::as_ptr(storage.read_adapter.reader());
            assert_eq!(
                seam_reader_ptr, adapter_reader_ptr,
                "shared reader instance is identical between tag seam caller and production read adapter"
            );

            // Execute both seam calls using the actual shared reader to prove operational validity
            let hex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
            write_tag_file(
                &root,
                "myrepo",
                "shared_tag",
                format!("sha256:{hex}\n").as_bytes(),
            );

            let limits = TagReadLimits::default();
            let d = resolve_tag_seam(&*storage.reader, "myrepo", "shared_tag", &limits)
                .await
                .expect("contained resolve succeeds with shared reader");
            assert_eq!(d.hex(), hex);

            let (d_get, _) =
                get_tag_with_version_seam(&*storage.reader, "myrepo", "shared_tag", &limits)
                    .await
                    .expect("contained get succeeds with shared reader")
                    .expect("tag found");
            assert_eq!(d_get.hex(), hex);
        }

        #[tokio::test]
        async fn test_seam_real_contained_read_success() {
            let (_fixture, root) = create_test_root();
            let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");
            let hex = "1111111111111111111111111111111111111111111111111111111111111111";
            let content = format!("sha256:{hex}\n");
            write_tag_file(&root, "myrepo", "latest", content.as_bytes());

            let limits = TagReadLimits::default();
            let d = resolve_tag_seam(&*storage.reader, "myrepo", "latest", &limits)
                .await
                .expect("contained resolve succeeds");
            assert_eq!(d.hex(), hex);

            let (d_get, v) =
                get_tag_with_version_seam(&*storage.reader, "myrepo", "latest", &limits)
                    .await
                    .expect("contained get succeeds")
                    .expect("tag found");
            assert_eq!(d_get.hex(), hex);

            let mut hasher = sha2::Sha256::new();
            hasher.update(content.as_bytes());
            assert_eq!(v, hex::encode(hasher.finalize()));
        }

        #[tokio::test]
        async fn test_seam_real_final_symlink_rejected() {
            let (fixture, root) = create_test_root();
            let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");

            // Create target file outside storage root
            let outside_file = fixture.path().join("outside_tag.txt");
            std::fs::write(
                &outside_file,
                b"sha256:2222222222222222222222222222222222222222222222222222222222222222\n",
            )
            .unwrap();

            // Create symlink in tags pointing outside
            let tags_dir = root.join("repos").join("myrepo").join("tags");
            std::fs::create_dir_all(&tags_dir).unwrap();
            std::os::unix::fs::symlink(&outside_file, tags_dir.join("symlink_tag")).unwrap();

            let limits = TagReadLimits::default();
            let err_res = resolve_tag_seam(&*storage.reader, "myrepo", "symlink_tag", &limits)
                .await
                .unwrap_err();
            match err_res {
                StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
                other => panic!("expected Io on final symlink rejection, got {other:?}"),
            }

            let err_get =
                get_tag_with_version_seam(&*storage.reader, "myrepo", "symlink_tag", &limits)
                    .await
                    .unwrap_err();
            match err_get {
                StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
                other => panic!("expected Io on final symlink rejection, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_seam_real_ancestor_symlink_rejected() {
            let (fixture, root) = create_test_root();
            let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");

            // Create outside directory with a tag file
            let outside_tags = fixture.path().join("outside_tags");
            std::fs::create_dir_all(&outside_tags).unwrap();
            std::fs::write(
                outside_tags.join("tag1"),
                b"sha256:3333333333333333333333333333333333333333333333333333333333333333\n",
            )
            .unwrap();

            // Symlink the entire repos/myrepo/tags directory to outside_tags
            let repo_dir = root.join("repos").join("myrepo");
            std::fs::create_dir_all(&repo_dir).unwrap();
            std::os::unix::fs::symlink(&outside_tags, repo_dir.join("tags")).unwrap();

            let limits = TagReadLimits::default();
            let err = resolve_tag_seam(&*storage.reader, "myrepo", "tag1", &limits)
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
                other => panic!("expected Io on ancestor symlink rejection, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_seam_real_dangling_symlink_rejected() {
            let (_fixture, root) = create_test_root();
            let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");

            let tags_dir = root.join("repos").join("myrepo").join("tags");
            std::fs::create_dir_all(&tags_dir).unwrap();
            std::os::unix::fs::symlink(root.join("nonexistent_target"), tags_dir.join("dangling"))
                .unwrap();

            let limits = TagReadLimits::default();
            let err = resolve_tag_seam(&*storage.reader, "myrepo", "dangling", &limits)
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
                other => panic!("expected Io on dangling symlink rejection, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_seam_real_non_regular_rejected() {
            let (_fixture, root) = create_test_root();
            let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");

            // Create directory where tag file is expected
            let dir_tag = root
                .join("repos")
                .join("myrepo")
                .join("tags")
                .join("dir_tag");
            std::fs::create_dir_all(&dir_tag).unwrap();

            let limits = TagReadLimits::default();
            let err = resolve_tag_seam(&*storage.reader, "myrepo", "dir_tag", &limits)
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
                other => panic!("expected Io on non-regular directory rejection, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_seam_real_path_traversal_rejected() {
            let (_fixture, root) = create_test_root();
            let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");
            let limits = TagReadLimits::default();

            let err1 = resolve_tag_seam(&*storage.reader, "../sibling", "latest", &limits)
                .await
                .unwrap_err();
            assert!(matches!(err1, StorageError::InvalidRepoName(_)));

            let err2 = get_tag_with_version_seam(&*storage.reader, "myrepo", "../outside", &limits)
                .await
                .unwrap_err();
            assert!(matches!(err2, StorageError::InvalidRepoName(_)));
        }

        // After the O-04 tag-mutation write-containment cutover, tag mutations
        // resolve through the same pinned `repos` authority (rooted at the same
        // pinned root fd) that the read seam uses. This test — formerly a
        // divergence demonstration where the ambient write path saw a replaced
        // root while the pinned reader did not — now proves read/write
        // COHERENCE across a whole-root rename+recreate: both the reader and the
        // contained conditional-delete observe the pinned OLD root, so a version
        // token obtained from the read path drives a matching (Deleted)
        // conditional delete on the very same leaf.
        #[tokio::test]
        async fn test_seam_real_root_replacement_read_write_coherent() {
            let (fixture, root) = create_test_root();
            let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");

            let hex_old = "4444444444444444444444444444444444444444444444444444444444444444";
            write_tag_file(
                &root,
                "myrepo",
                "target",
                format!("sha256:{hex_old}\n").as_bytes(),
            );

            // Verify initial read
            let limits = TagReadLimits::default();
            let d_initial = resolve_tag_seam(&*storage.reader, "myrepo", "target", &limits)
                .await
                .unwrap();
            assert_eq!(d_initial.hex(), hex_old);

            // Rename root directory and recreate a fresh tree at the identical pathname with different content
            let root_old = fixture.path().join("storage-root-old");
            std::fs::rename(&root, &root_old).unwrap();
            std::fs::create_dir_all(&root).unwrap();

            let hex_new = "5555555555555555555555555555555555555555555555555555555555555555";
            write_tag_file(
                &root,
                "myrepo",
                "target",
                format!("sha256:{hex_new}\n").as_bytes(),
            );

            // 1. Contained seam and production entry points continue to observe the OLD pinned root!
            let d_contained = resolve_tag_seam(&*storage.reader, "myrepo", "target", &limits)
                .await
                .unwrap();
            assert_eq!(
                d_contained.hex(),
                hex_old,
                "contained seam observes pinned old root"
            );

            let (d_contained_get, v_contained) =
                get_tag_with_version_seam(&*storage.reader, "myrepo", "target", &limits)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(d_contained_get.hex(), hex_old);

            // 2. Production entry points route through the contained reader and observe the pinned OLD root
            let d_prod = storage.resolve_tag("myrepo", "target").await.unwrap();
            assert_eq!(
                d_prod.hex(),
                hex_old,
                "production resolve_tag observes pinned old root"
            );

            let (d_prod_get, v_prod) = storage
                .get_tag_with_version("myrepo", "target")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(d_prod_get.hex(), hex_old);
            assert_eq!(v_prod, v_contained);

            // 3. Contained mutations also resolve through the pinned root, so a
            //    conditional delete carrying the token read from the pinned old
            //    root MATCHES that same leaf and deletes it — read and write are
            //    coherent (no ambient reconstruction onto the replacement tree).
            let del_res = storage
                .delete_tag_conditional("myrepo", "target", Some(&v_contained))
                .await
                .unwrap();
            assert!(
                matches!(del_res, crate::storage::ConditionalDeleteResult::Deleted),
                "contained mutation observes the same pinned old root as the read seam: \
                 the old-root version token matches and the leaf is deleted"
            );

            // The delete landed on the pinned old root, NOT on the ambient
            // replacement tree at `self.root`: the recreated tree's leaf (with
            // hex_new) is untouched, confirming no ambient path reconstruction.
            let replacement_leaf = root
                .join("repos")
                .join("myrepo")
                .join("tags")
                .join("target");
            assert_eq!(
                std::fs::read(&replacement_leaf).unwrap(),
                format!("sha256:{hex_new}\n").as_bytes(),
                "the replacement tree leaf is untouched; the contained delete hit the pinned old root"
            );
        }

        #[tokio::test]
        #[cfg(unix)]
        #[ignore = "requires unprivileged user environment where chmod 0o000 denies filesystem access"]
        async fn test_seam_real_permission_denied() {
            use std::os::unix::fs::PermissionsExt;

            let (_fixture, root) = create_test_root();
            let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");
            let tag_path = root
                .join("repos")
                .join("myrepo")
                .join("tags")
                .join("perm_tag");
            write_tag_file(
                &root,
                "myrepo",
                "perm_tag",
                b"sha256:6666666666666666666666666666666666666666666666666666666666666666\n",
            );

            let orig_perms = std::fs::metadata(&tag_path).unwrap().permissions();

            struct ScopedPermReset<'a> {
                path: &'a Path,
                original_permissions: std::fs::Permissions,
            }
            impl<'a> Drop for ScopedPermReset<'a> {
                fn drop(&mut self) {
                    let _ = std::fs::set_permissions(self.path, self.original_permissions.clone());
                }
            }

            {
                let _guard = ScopedPermReset {
                    path: &tag_path,
                    original_permissions: orig_perms.clone(),
                };
                std::fs::set_permissions(&tag_path, std::fs::Permissions::from_mode(0o000))
                    .unwrap();

                match std::fs::read(&tag_path) {
                    Ok(_) => panic!("ineffective permissions: read succeeded under mode 0o000"),
                    Err(err) => assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied),
                }

                let limits = TagReadLimits::default();
                let err_res = resolve_tag_seam(&*storage.reader, "myrepo", "perm_tag", &limits)
                    .await
                    .unwrap_err();
                match err_res {
                    StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
                    other => panic!("expected Io for permission denial, got {other:?}"),
                }

                let err_get =
                    get_tag_with_version_seam(&*storage.reader, "myrepo", "perm_tag", &limits)
                        .await
                        .unwrap_err();
                match err_get {
                    StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
                    other => panic!("expected Io for permission denial, got {other:?}"),
                }
            }
        }
    }
}
