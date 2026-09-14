//! Contained filesystem referrers read implementation for `registry-rust`.
//!
//! # Architectural Ownership Boundaries
//! - `storage-core`: Defines domain-neutral contracts ([`storage_core::ObjectPayloadReader`],
//!   [`storage_core::ObjectPayload`], [`storage_core::ReadError`], [`storage_core::ObjectKey`]).
//! - `storage-fs`: Implements Linux descriptor-relative containment (`openat2` + `O_PATH` with
//!   `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`), regular file validation
//!   (`S_IFREG`), and Phase 2 readable reopening via `/proc/self/fd/N`.
//! - `registry-rust`: Owns referrers key construction (`repos/<repository>/referrers/<hex>.json`),
//!   pre-composition structural path safety checks, bounded stream draining, JSON deserialization,
//!   descriptor count checking, and mapping to [`StorageError`].
//!
//! # Preserved Legacy Contracts
//! - Missing file (any missing path component) returns `Ok(Vec::new())` without repository probing.
//! - JSON deserialization failures map to legacy [`StorageErrorKind::Io`] via
//!   [`StorageError::io`], preserving the characterized error taxonomy of the ambient
//!   implementation (including serde messages such as "EOF while parsing a value").
//! - The physical descriptor order of the stored JSON array is preserved without sorting,
//!   deduplication, or normalization.
//!
//! # Resource Limit Contracts
//! - `ReferrersReadLimits::default()` is `{ max_payload_bytes: None, max_descriptors: None }`,
//!   which preserves the exact unbounded characterization baseline.
//! - `max_payload_bytes = Some(limit)` bounds the number of bytes *collected* from the payload
//!   stream: at most `limit + 1` bytes are drained so overflow is detectable, and at most `limit`
//!   bytes are accepted. This is a collected-byte ceiling, not a precise peak-memory or
//!   peak-allocation bound: `Vec` capacity growth, allocator overhead, and the simultaneous
//!   retention of the raw byte buffer plus the parsed descriptor structures (strings, maps)
//!   all add memory costs beyond the collected byte count.
//! - `max_descriptors = Some(n)` is checked **after** full deserialization. It limits accepted
//!   results; it does not limit peak allocation during serde parsing.
//! - `max_payload_bytes = Some(u64::MAX)` cannot be represented for limit-plus-one draining and
//!   is rejected with [`StorageErrorKind::CorruptData`]. Because draining follows acquisition,
//!   acquisition outcomes take precedence: a missing file still returns `Ok(Vec::new())` and an
//!   acquisition error still surfaces before the representation error is evaluated.
//! - No global memory or concurrency budget follows from these per-read limits.
//! - There is no decompression stage in this JSON pipeline.
//!
//! # Concurrency, Containment, and Coherence Demarcation
//! - Contained reads resolve beneath the pinned root descriptor; they do not provide snapshot
//!   isolation. Concurrent writers can produce truncated bytes (failing parsing) or changed but
//!   syntactically valid JSON; successful parsing does not establish snapshot consistency.
//! - Kernel containment does not guarantee mount or hard-link isolation.
//! - Non-Linux verification remains unperformed.
//!
//! # Mutation-Caller Read Semantics
//! Ordinary reads (`FsStorage::list_referrers`) independently resolve through the contained
//! object reader in this module. Mutations (`add_referrer`, `remove_referrer`, and
//! `delete_manifest` via `remove_referrer`) do NOT re-resolve for their inspection: each
//! mutation resolves ONE contained `repos/<repo>/referrers` authority (validation via
//! [`referrers_key`], then `ensure_subdir` beneath the pinned `repos` root) and retains it
//! across its entire inspection/action sequence, reading the index leaf through that same
//! authority (deserialized by the shared [`parse_referrers_bytes`], preserving the legacy
//! missing→empty / corrupt→Io / order-preserving contract):
//! - Structurally invalid repository names are rejected without any directory creation, and
//!   directory creation itself is contained (no ambient `create_dir_all`).
//! - Read rejections (symlink rejection, non-regular leaves, parse failures) abort mutations
//!   fail-closed via `?` before serialization or writeback.
//! - Retaining one authority prevents a mutation from inspecting one replaceable lower-level
//!   tree and acting on another after a repository/`referrers` namespace replacement. It does
//!   NOT provide snapshot isolation or cross-process serialization; the in-process shard lock
//!   only serializes same-instance mutations.
//! - `delete_manifest` ignores `remove_referrer` failures; a rejected referrers read during
//!   cleanup does not resurrect the already-removed manifest.
//! - Remaining durability questions are governed by Quality Gate O-04.

use crate::registry::digest::Digest;
use crate::storage::{ReferrerDescriptor, StorageError};
use storage_core::{ObjectKey, ObjectPayload, ObjectPayloadReader, ReadError};
use tokio::io::AsyncReadExt;

/// Caller-supplied resource limits for referrers payload reading.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) struct ReferrersReadLimits {
    /// Maximum bytes to collect from the referrers payload stream.
    /// If None, reads without a seam-imposed ceiling (preserving existing unbounded behavior).
    /// If Some(limit), collects at most `limit + 1` bytes and accepts at most `limit` bytes.
    pub max_payload_bytes: Option<u64>,

    /// Maximum number of parsed descriptors permitted in the referrers array,
    /// checked after deserialization. If None, permits any array length that
    /// the payload byte limit admits.
    pub max_descriptors: Option<usize>,
}

/// Constructs the relative [`ObjectKey`] for a repository referrers index file:
/// `repos/<repository>/referrers/<subject.hex()>.json`.
///
/// Applies the shared structural path safety checks from
/// [`super::tag_read::validate_path_component`] to the repository name before composition:
/// rejects empty names, leading/trailing slashes, backslashes, NUL/control bytes, empty
/// segments, and `.`/`..` segments with [`StorageError::InvalidRepoName`]. Structural
/// validation is not `CanonicalRepoName` grammar; multi-segment and uppercase names remain
/// permitted at this boundary. The subject filename is derived from the strongly typed
/// [`Digest`] hex encoding and supports both SHA-256 and SHA-512 digests.
pub(crate) fn referrers_key(repo: &str, subject: &Digest) -> Result<ObjectKey, StorageError> {
    crate::storage::tag_domain::validate_path_component(repo, "repository name")?;

    let key_str = format!("repos/{repo}/referrers/{}.json", subject.hex());
    ObjectKey::parse(&key_str).map_err(|e| StorageError::InvalidRepoName(e.to_string()))
}

/// Drains a referrers payload stream in accordance with caller-supplied [`ReferrersReadLimits`].
///
/// - If `limits.max_payload_bytes == None`: drains without a seam-imposed ceiling.
/// - If `limits.max_payload_bytes == Some(limit)`: consumes at most `limit + 1` bytes using
///   `stream.take(...)` to detect overflow safely, rejecting overflow with
///   [`StorageErrorKind::CorruptData`].
/// - If `limit == 0`: accepts empty streams (0 bytes) and rejects non-empty streams before parsing.
/// - `Some(u64::MAX)` fails checked limit-plus-one representation with
///   [`StorageErrorKind::CorruptData`].
async fn drain_referrers_stream(
    payload: ObjectPayload,
    limits: &ReferrersReadLimits,
) -> Result<Vec<u8>, StorageError> {
    let (_metadata, stream) = payload.into_parts();

    match limits.max_payload_bytes {
        None => {
            let mut buffer = Vec::new();
            let mut pinned_stream = stream;
            pinned_stream.read_to_end(&mut buffer).await.map_err(|e| {
                StorageError::io(format!("failed to read referrers payload stream: {e}"))
            })?;
            Ok(buffer)
        }
        Some(limit) => {
            let take_limit = limit.checked_add(1).ok_or_else(|| {
                StorageError::corrupt_data(format!(
                    "referrers payload limit {limit} cannot be represented for bounded draining"
                ))
            })?;

            let mut limited_stream = stream.take(take_limit);
            let mut buffer = Vec::new();
            limited_stream.read_to_end(&mut buffer).await.map_err(|e| {
                StorageError::io(format!("failed to read referrers payload stream: {e}"))
            })?;

            if buffer.len() as u64 > limit {
                return Err(StorageError::corrupt_data(format!(
                    "referrers payload stream length exceeds limit of {limit} bytes"
                )));
            }

            Ok(buffer)
        }
    }
}

/// Deserializes referrers index bytes with the legacy contract shared by the
/// contained reader seam and the mutation-side authority read:
/// corrupted JSON / invalid UTF-8 map to the legacy [`StorageErrorKind::Io`]
/// taxonomy (carrying the serde message), the physical descriptor order of the
/// stored array is preserved, and no normalization or deduplication is applied.
///
/// [`StorageErrorKind::Io`]: crate::storage::StorageErrorKind::Io
pub(crate) fn parse_referrers_bytes(bytes: &[u8]) -> Result<Vec<ReferrerDescriptor>, StorageError> {
    serde_json::from_slice(bytes).map_err(|err| StorageError::io(err.to_string()))
}

/// Reads and deserializes a referrers index file using descriptor-relative containment.
///
/// Contract:
/// - Structurally invalid repository names -> [`StorageError::InvalidRepoName`] with zero
///   reader calls.
/// - Missing file (any missing path component) -> `Ok(Vec::new())` without repository probing.
/// - Symlinks, uncontained paths, and non-regular objects -> [`StorageErrorKind::Io`] via
///   [`super::read_adapter::translate_payload_read_error`].
/// - Corrupted JSON / invalid UTF-8 -> legacy [`StorageErrorKind::Io`] carrying the serde message.
/// - Exceeding `max_payload_bytes` (checked before parsing) or `max_descriptors`
///   (checked after parsing) -> [`StorageErrorKind::CorruptData`].
/// - Preserves the physical descriptor order of the stored JSON array.
pub(crate) async fn read_referrers_contained(
    reader: &(impl ObjectPayloadReader + ?Sized),
    repo: &str,
    subject: &Digest,
    limits: &ReferrersReadLimits,
) -> Result<Vec<ReferrerDescriptor>, StorageError> {
    let key = referrers_key(repo, subject)?;

    let payload = match reader.open_payload(&key).await {
        Ok(p) => p,
        Err(ReadError::NotFound { .. }) => return Ok(Vec::new()),
        Err(other) => return Err(super::read_adapter::translate_payload_read_error(other)),
    };

    let bytes = drain_referrers_stream(payload, limits).await?;

    let descriptors = parse_referrers_bytes(&bytes)?;

    if let Some(max_descriptors) = limits.max_descriptors {
        if descriptors.len() > max_descriptors {
            return Err(StorageError::corrupt_data(format!(
                "referrers descriptor count {} exceeds configured limit of {max_descriptors}",
                descriptors.len()
            )));
        }
    }

    Ok(descriptors)
}

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

    struct FailingStream {
        head_bytes: Vec<u8>,
        cursor: usize,
        error_kind: std::io::ErrorKind,
        message: &'static str,
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

    fn subject_sha256() -> Digest {
        Digest::parse("sha256:1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap()
    }

    fn subject_key(repo: &str, subject: &Digest) -> ObjectKey {
        ObjectKey::parse(&format!("repos/{repo}/referrers/{}.json", subject.hex())).unwrap()
    }

    fn make_descriptor(digest: &str, size: u64, artifact_type: Option<&str>) -> ReferrerDescriptor {
        ReferrerDescriptor {
            media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
            digest: digest.to_string(),
            size,
            artifact_type: artifact_type.map(|s| s.to_string()),
            annotations: None,
        }
    }

    // ========================================================================
    // Category A: Key Construction and Traversal Rejection
    // ========================================================================

    #[test]
    fn test_referrers_key_construction_valid() {
        let subject = subject_sha256();
        let key = referrers_key("testrepo", &subject).expect("valid key");
        assert_eq!(
            key.as_str(),
            format!("repos/testrepo/referrers/{}.json", subject.hex())
        );

        let nested = referrers_key("org/team/app", &subject).expect("valid nested key");
        assert_eq!(
            nested.as_str(),
            format!("repos/org/team/app/referrers/{}.json", subject.hex())
        );

        let sha512_hex = "5125125125125125125125125125125125125125125125125125125125125125\
                          5125125125125125125125125125125125125125125125125125125125125125";
        let subject512 = Digest::parse(&format!("sha512:{sha512_hex}")).unwrap();
        let key512 = referrers_key("repo512", &subject512).expect("valid sha512 key");
        assert_eq!(
            key512.as_str(),
            format!("repos/repo512/referrers/{sha512_hex}.json")
        );
    }

    #[test]
    fn test_referrers_key_construction_traversal_rejection() {
        let subject = subject_sha256();
        for bad_repo in [
            "",
            "/repo",
            "repo/",
            "repo//sub",
            "../escaped",
            "repo/../sibling",
            "repo/./current",
            "repo\\bad",
            "repo\0null",
            "repo\x01ctrl",
        ] {
            assert!(
                matches!(
                    referrers_key(bad_repo, &subject),
                    Err(StorageError::InvalidRepoName(_))
                ),
                "expected InvalidRepoName for repository name {bad_repo:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_contained_read_invalid_repo_zero_reader_calls() {
        let fake = RecordingFakePayloadReader::new();
        let limits = ReferrersReadLimits::default();

        let err = read_referrers_contained(&fake, "../escaped", &subject_sha256(), &limits)
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::InvalidRepoName(_)));
        assert_eq!(
            fake.calls().len(),
            0,
            "reader must not be called for invalid repository names"
        );
    }

    // ========================================================================
    // Category B: Missing Files, Ordering, and Field Fidelity
    // ========================================================================

    #[tokio::test]
    async fn test_contained_read_missing_file_empty_success() {
        let fake = RecordingFakePayloadReader::new();
        let subject = subject_sha256();
        let key = subject_key("myrepo", &subject);
        fake.script(key.clone(), Err(ReadError::not_found(key.clone())));

        let limits = ReferrersReadLimits::default();
        let result = read_referrers_contained(&fake, "myrepo", &subject, &limits)
            .await
            .expect("missing file yields empty success");
        assert!(result.is_empty());
        assert_eq!(
            fake.calls(),
            vec![key],
            "exactly one acquisition call and no repository probes"
        );
    }

    #[tokio::test]
    async fn test_contained_read_preserves_stored_order_and_fields() {
        let fake = RecordingFakePayloadReader::new();
        let subject = subject_sha256();
        let key = subject_key("myrepo", &subject);

        let mut annotations = HashMap::new();
        annotations.insert("vnd.custom.field".to_string(), "custom_value".to_string());
        let desc_c = ReferrerDescriptor {
            media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
            digest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                .to_string(),
            size: 300,
            artifact_type: Some("application/vnd.example.sbom.v1".to_string()),
            annotations: Some(annotations),
        };
        let desc_a = make_descriptor(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            100,
            None,
        );
        let desc_b = make_descriptor(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            200,
            Some("application/vnd.example.signature.v1"),
        );

        let stored = vec![desc_c.clone(), desc_a.clone(), desc_b.clone()];
        fake.script(
            key.clone(),
            Ok(mock_payload(serde_json::to_vec(&stored).unwrap())),
        );

        let limits = ReferrersReadLimits::default();
        let result = read_referrers_contained(&fake, "myrepo", &subject, &limits)
            .await
            .expect("valid multi-descriptor read succeeds");
        assert_eq!(
            result,
            vec![desc_c, desc_a, desc_b],
            "stored physical order [C, A, B] must be preserved without sorting"
        );
    }

    // ========================================================================
    // Category C: Payload Byte Limits and Edge Cases
    // ========================================================================

    #[tokio::test]
    async fn test_contained_read_empty_array_with_zero_descriptor_limit() {
        let fake = RecordingFakePayloadReader::new();
        let subject = subject_sha256();
        let key = subject_key("myrepo", &subject);
        fake.script(key.clone(), Ok(mock_payload(b"[]".to_vec())));

        // [] occupies two bytes; max_descriptors Some(0) permits a valid empty array.
        let limits = ReferrersReadLimits {
            max_payload_bytes: Some(2),
            max_descriptors: Some(0),
        };
        let result = read_referrers_contained(&fake, "myrepo", &subject, &limits)
            .await
            .expect("empty array admitted under Some(0) descriptor limit");
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_contained_read_zero_byte_file_with_limit_zero() {
        let fake = RecordingFakePayloadReader::new();
        let subject = subject_sha256();
        let key = subject_key("myrepo", &subject);
        fake.script(key.clone(), Ok(mock_payload(vec![])));

        // Existing zero-byte file with Some(0): byte check passes (0 <= 0),
        // then JSON parsing fails with the legacy Io mapping.
        let limits = ReferrersReadLimits {
            max_payload_bytes: Some(0),
            max_descriptors: None,
        };
        let err = read_referrers_contained(&fake, "myrepo", &subject, &limits)
            .await
            .unwrap_err();
        match err {
            StorageError::Internal {
                kind, ref message, ..
            } => {
                assert_eq!(kind, StorageErrorKind::Io);
                assert!(
                    message.contains("EOF while parsing a value"),
                    "expected serde EOF message, got: {message}"
                );
            }
            other => panic!("expected Internal(Io) for zero-byte file parse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_contained_read_nonempty_file_with_limit_zero() {
        let fake = RecordingFakePayloadReader::new();
        let subject = subject_sha256();
        let key = subject_key("myrepo", &subject);
        fake.script(key.clone(), Ok(mock_payload(b"[]".to_vec())));

        // Existing nonempty file with Some(0): overflow detected before parsing.
        let limits = ReferrersReadLimits {
            max_payload_bytes: Some(0),
            max_descriptors: None,
        };
        let err = read_referrers_contained(&fake, "myrepo", &subject, &limits)
            .await
            .unwrap_err();
        match err {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::CorruptData),
            other => panic!("expected CorruptData overflow before parsing, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_contained_read_payload_limit_exact_and_one_over() {
        let fake = RecordingFakePayloadReader::new();
        let subject = subject_sha256();
        let key = subject_key("myrepo", &subject);

        let desc = make_descriptor(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            100,
            None,
        );
        let bytes = serde_json::to_vec(&vec![desc.clone()]).unwrap();
        let len = bytes.len() as u64;
        fake.script(key.clone(), Ok(mock_payload(bytes.clone())));
        fake.script(key.clone(), Ok(mock_payload(bytes)));

        // Exact limit passes.
        let limits_exact = ReferrersReadLimits {
            max_payload_bytes: Some(len),
            max_descriptors: None,
        };
        let result = read_referrers_contained(&fake, "myrepo", &subject, &limits_exact)
            .await
            .expect("exact byte limit succeeds");
        assert_eq!(result, vec![desc]);

        // One byte under the payload size rejects before parsing.
        let limits_under = ReferrersReadLimits {
            max_payload_bytes: Some(len - 1),
            max_descriptors: None,
        };
        let err = read_referrers_contained(&fake, "myrepo", &subject, &limits_under)
            .await
            .unwrap_err();
        match err {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::CorruptData),
            other => panic!("expected CorruptData on over-limit payload, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_contained_read_u64_max_limit_and_acquisition_precedence() {
        let fake = RecordingFakePayloadReader::new();
        let subject = subject_sha256();
        let key = subject_key("myrepo", &subject);
        let limits = ReferrersReadLimits {
            max_payload_bytes: Some(u64::MAX),
            max_descriptors: None,
        };

        // Successful acquisition: representation failure of limit-plus-one draining.
        fake.script(key.clone(), Ok(mock_payload(b"[]".to_vec())));
        let err = read_referrers_contained(&fake, "myrepo", &subject, &limits)
            .await
            .unwrap_err();
        match err {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::CorruptData),
            other => panic!("expected CorruptData for u64::MAX limit, got {other:?}"),
        }

        // Acquisition NotFound takes precedence over the representation error:
        // draining is never reached and the missing-file contract holds.
        fake.script(key.clone(), Err(ReadError::not_found(key.clone())));
        let result = read_referrers_contained(&fake, "myrepo", &subject, &limits)
            .await
            .expect("missing file with u64::MAX limit still yields empty success");
        assert!(result.is_empty());

        // Acquisition permission error also takes precedence over the representation error.
        fake.script(key.clone(), Err(ReadError::permission_denied(key.clone())));
        let err = read_referrers_contained(&fake, "myrepo", &subject, &limits)
            .await
            .unwrap_err();
        match err {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
            other => panic!("expected Io acquisition precedence, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_contained_read_descriptor_count_ceiling() {
        let fake = RecordingFakePayloadReader::new();
        let subject = subject_sha256();
        let key = subject_key("myrepo", &subject);

        let descriptors: Vec<ReferrerDescriptor> = (0..11)
            .map(|i| make_descriptor(&format!("sha256:{:064x}", i), 100 + i as u64, None))
            .collect();
        let bytes = serde_json::to_vec(&descriptors).unwrap();
        fake.script(key.clone(), Ok(mock_payload(bytes.clone())));
        fake.script(key.clone(), Ok(mock_payload(bytes)));

        // 11 descriptors with limit 10 is rejected after parsing.
        let limits_reject = ReferrersReadLimits {
            max_payload_bytes: None,
            max_descriptors: Some(10),
        };
        let err = read_referrers_contained(&fake, "myrepo", &subject, &limits_reject)
            .await
            .unwrap_err();
        match err {
            StorageError::Internal {
                kind, ref message, ..
            } => {
                assert_eq!(kind, StorageErrorKind::CorruptData);
                assert!(
                    message.contains("11") && message.contains("10"),
                    "expected count diagnostic, got: {message}"
                );
            }
            other => panic!("expected CorruptData descriptor count rejection, got {other:?}"),
        }

        // Exactly 11 descriptors with limit 11 is accepted.
        let limits_accept = ReferrersReadLimits {
            max_payload_bytes: None,
            max_descriptors: Some(11),
        };
        let result = read_referrers_contained(&fake, "myrepo", &subject, &limits_accept)
            .await
            .expect("exact descriptor count admitted");
        assert_eq!(result.len(), 11);
    }

    // ========================================================================
    // Category D: Error Taxonomy
    // ========================================================================

    #[tokio::test]
    async fn test_contained_read_corrupted_json_legacy_io_taxonomy() {
        let fake = RecordingFakePayloadReader::new();
        let subject = subject_sha256();
        let key = subject_key("myrepo", &subject);
        let limits = ReferrersReadLimits::default();

        for corrupt in [
            b"".to_vec(),
            b"{not-valid-json}".to_vec(),
            b"[{\"mediaType\": \"application/vnd.oci.image.manifest.v1+json\"".to_vec(),
            b"{\"mediaType\": \"application/vnd.oci.image.manifest.v1+json\"}".to_vec(),
            b"[\xFF\xFE\xFD]".to_vec(),
        ] {
            fake.script(key.clone(), Ok(mock_payload(corrupt.clone())));
            let err = read_referrers_contained(&fake, "myrepo", &subject, &limits)
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, .. } => assert_eq!(
                    kind,
                    StorageErrorKind::Io,
                    "corrupt payload {corrupt:?} must keep the legacy Io mapping"
                ),
                other => panic!("expected Internal(Io) for corrupt payload, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn test_contained_read_stream_failure_maps_to_io() {
        let fake = RecordingFakePayloadReader::new();
        let subject = subject_sha256();
        let key = subject_key("myrepo", &subject);

        let failing = FailingStream {
            head_bytes: b"[".to_vec(),
            cursor: 0,
            error_kind: std::io::ErrorKind::ConnectionReset,
            message: "stream broken",
        };
        let payload = ObjectPayload::new(ObjectMetadata::new(50), Box::pin(failing));
        fake.script(key.clone(), Ok(payload));

        let limits = ReferrersReadLimits::default();
        let err = read_referrers_contained(&fake, "myrepo", &subject, &limits)
            .await
            .unwrap_err();
        match err {
            StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
            other => panic!("expected Io on mid-stream failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_contained_read_acquisition_error_translation() {
        let fake = RecordingFakePayloadReader::new();
        let subject = subject_sha256();
        let key = subject_key("myrepo", &subject);
        let limits = ReferrersReadLimits::default();

        // Symlink resolution rejection -> Io.
        fake.script(
            key.clone(),
            Err(ReadError::backend_with_source(
                "resolution rejected",
                Box::new(storage_fs::FsMetadataError::ResolutionRejected {
                    raw_os_error: libc::ELOOP,
                    source: std::io::Error::from_raw_os_error(libc::ELOOP),
                }),
            )),
        );
        let err = read_referrers_contained(&fake, "myrepo", &subject, &limits)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            StorageError::Internal {
                kind: StorageErrorKind::Io,
                ..
            }
        ));

        // Non-regular object rejection -> Io.
        fake.script(
            key.clone(),
            Err(ReadError::backend_with_source(
                "unsupported object type",
                Box::new(storage_fs::FsMetadataError::UnsupportedObjectType {
                    mode: libc::S_IFDIR,
                }),
            )),
        );
        let err = read_referrers_contained(&fake, "myrepo", &subject, &limits)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            StorageError::Internal {
                kind: StorageErrorKind::Io,
                ..
            }
        ));

        // openat2 unavailable -> Configuration.
        fake.script(
            key.clone(),
            Err(ReadError::backend_with_source(
                "syscall unsupported",
                Box::new(storage_fs::FsMetadataError::SyscallUnsupported(
                    std::io::Error::from_raw_os_error(libc::ENOSYS),
                )),
            )),
        );
        let err = read_referrers_contained(&fake, "myrepo", &subject, &limits)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            StorageError::Internal {
                kind: StorageErrorKind::Configuration,
                ..
            }
        ));
    }

    // ========================================================================
    // Category E: Real Filesystem Linux-Gated Tests
    // ========================================================================

    #[cfg(target_os = "linux")]
    mod real_fs_tests {
        use super::*;
        use crate::storage::fs::FsStorage;
        use std::path::{Path, PathBuf};

        fn create_test_root() -> (tempfile::TempDir, PathBuf) {
            let fixture = tempfile::tempdir().expect("create tempdir");
            let root = fixture.path().join("storage-root");
            std::fs::create_dir_all(&root).expect("create storage root");
            (fixture, root)
        }

        fn write_referrers_file(
            root: &Path,
            repo: &str,
            subject: &Digest,
            bytes: &[u8],
        ) -> PathBuf {
            let dir = root.join("repos").join(repo).join("referrers");
            std::fs::create_dir_all(&dir).expect("create referrers dir");
            let path = dir.join(format!("{}.json", subject.hex()));
            std::fs::write(&path, bytes).expect("write referrers file");
            path
        }

        #[tokio::test]
        async fn test_real_shared_reader_contained_read() {
            let (_fixture, root) = create_test_root();
            let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");
            let subject = subject_sha256();

            let desc = make_descriptor(
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                100,
                None,
            );
            write_referrers_file(
                &root,
                "myrepo",
                &subject,
                &serde_json::to_vec(&vec![desc.clone()]).unwrap(),
            );

            let limits = ReferrersReadLimits::default();
            let result = read_referrers_contained(&*storage.reader, "myrepo", &subject, &limits)
                .await
                .expect("contained read succeeds with shared reader");
            assert_eq!(result, vec![desc]);
        }

        #[tokio::test]
        async fn test_real_final_symlink_rejected() {
            let (fixture, root) = create_test_root();
            let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");
            let subject = subject_sha256();

            let outside_file = fixture.path().join("outside_referrers.json");
            std::fs::write(&outside_file, b"[]").unwrap();

            let referrers_dir = root.join("repos").join("myrepo").join("referrers");
            std::fs::create_dir_all(&referrers_dir).unwrap();
            std::os::unix::fs::symlink(
                &outside_file,
                referrers_dir.join(format!("{}.json", subject.hex())),
            )
            .unwrap();

            let limits = ReferrersReadLimits::default();
            let err = read_referrers_contained(&*storage.reader, "myrepo", &subject, &limits)
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
                other => panic!("expected Io on final symlink rejection, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_directory_symlink_rejected() {
            let (fixture, root) = create_test_root();
            let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");
            let subject = subject_sha256();

            let outside_dir = fixture.path().join("outside_referrers");
            std::fs::create_dir_all(&outside_dir).unwrap();
            std::fs::write(outside_dir.join(format!("{}.json", subject.hex())), b"[]").unwrap();

            let repo_dir = root.join("repos").join("myrepo");
            std::fs::create_dir_all(&repo_dir).unwrap();
            std::os::unix::fs::symlink(&outside_dir, repo_dir.join("referrers")).unwrap();

            let limits = ReferrersReadLimits::default();
            let err = read_referrers_contained(&*storage.reader, "myrepo", &subject, &limits)
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
                other => panic!("expected Io on directory symlink rejection, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_directory_in_place_of_file_rejected() {
            let (_fixture, root) = create_test_root();
            let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");
            let subject = subject_sha256();

            let dir_path = root
                .join("repos")
                .join("myrepo")
                .join("referrers")
                .join(format!("{}.json", subject.hex()));
            std::fs::create_dir_all(&dir_path).unwrap();

            let limits = ReferrersReadLimits::default();
            let err = read_referrers_contained(&*storage.reader, "myrepo", &subject, &limits)
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
                other => panic!("expected Io on non-regular object rejection, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_missing_paths_empty_success() {
            let (_fixture, root) = create_test_root();
            let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");
            let subject = subject_sha256();
            let limits = ReferrersReadLimits::default();

            // Missing repository directory entirely.
            let result =
                read_referrers_contained(&*storage.reader, "missing_repo", &subject, &limits)
                    .await
                    .expect("missing repo yields empty success");
            assert!(result.is_empty());

            // Repository exists without referrers directory.
            std::fs::create_dir_all(root.join("repos").join("norefs")).unwrap();
            let result = read_referrers_contained(&*storage.reader, "norefs", &subject, &limits)
                .await
                .expect("missing referrers dir yields empty success");
            assert!(result.is_empty());

            // Referrers directory exists without the subject file.
            std::fs::create_dir_all(root.join("repos").join("nosubject").join("referrers"))
                .unwrap();
            let result = read_referrers_contained(&*storage.reader, "nosubject", &subject, &limits)
                .await
                .expect("missing subject file yields empty success");
            assert!(result.is_empty());
        }

        #[tokio::test]
        #[cfg(unix)]
        #[ignore = "requires unprivileged user environment where chmod 0o000 denies filesystem access"]
        async fn test_real_permission_denied() {
            use std::os::unix::fs::PermissionsExt;

            let (_fixture, root) = create_test_root();
            let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");
            let subject = subject_sha256();
            let path = write_referrers_file(&root, "myrepo", &subject, b"[]");

            let orig_perms = std::fs::metadata(&path).unwrap().permissions();

            struct ScopedPermReset<'a> {
                path: &'a Path,
                original_permissions: std::fs::Permissions,
            }
            impl<'a> Drop for ScopedPermReset<'a> {
                fn drop(&mut self) {
                    let _ = std::fs::set_permissions(self.path, self.original_permissions.clone());
                }
            }

            let _guard = ScopedPermReset {
                path: &path,
                original_permissions: orig_perms,
            };
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();

            match std::fs::read(&path) {
                Ok(_) => panic!("ineffective permissions: read succeeded under mode 0o000"),
                Err(err) => assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied),
            }

            let limits = ReferrersReadLimits::default();
            let err = read_referrers_contained(&*storage.reader, "myrepo", &subject, &limits)
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
                other => panic!("expected Io for permission denial, got {other:?}"),
            }
        }
    }
}
