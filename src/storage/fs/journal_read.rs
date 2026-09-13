//! Contained filesystem lifecycle-journal reads for `registry-rust` (gap item R-12).
//!
//! # Architecture and Scope
//!
//! Implements the production read path behind `FsStorage::read_lifecycle_journal`
//! over the shared pinned root descriptor via [`storage_core::ObjectPayloadReader`]
//! (`storage_fs::FsMetadataReader`: `openat2` with
//! `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`, two-phase
//! `O_PATH` + `S_IFREG` validation + procfs reopen; blocking work on the
//! dependency's `spawn_blocking` offload). There is no separate journal
//! discovery method: GC pre-delete validation iterates repositories via the
//! already-contained catalog discovery and point-reads each repository's
//! journal through this path.
//!
//! # Preserved Journal Contracts
//!
//! - Key/path: `repos/<repo>/meta/lifecycle_journal.json`, with the strict
//!   `CanonicalRepoName` grammar enforced before key composition (unchanged).
//! - The storage layer returns the raw journal bytes; deserialization and
//!   schema handling stay with the callers (`ManifestLifecycleManager`
//!   parses `LifecycleJournalRecord` and maps malformed payloads to
//!   `CorruptJournal`; GC validation parses with its own typed error), so all
//!   supported operation kinds, phases, and any tolerated older record shapes
//!   are unchanged by this cutover.
//! - Genuine absence (missing repository, `meta/`, or journal file) ->
//!   `Ok(None)` — the only condition under which callers may treat the
//!   repository as having no pending operation.
//! - Existing empty or malformed journal payloads still reach the callers as
//!   bytes and keep failing at their parse step (empty file is NOT absence).
//! - Read failures keep the legacy `Io` taxonomy.
//!
//! # Intentional Containment Changes (test-frozen)
//!
//! - Pinned-root resolution: replacing the root pathname no longer redirects
//!   journal reads to the replacement tree.
//! - Symlinked path components (repository dir, `meta/`, or the journal file)
//!   are rejected (`Io`) instead of silently followed; non-regular objects at
//!   the journal path are rejected by `S_IFREG` validation without a
//!   potentially blocking open.
//! - Containment resolution rejection (`ELOOP`/`EXDEV`) is never treated as
//!   absence — it can involve an ancestor and always propagates. No failure
//!   class is converted into "no pending operation".
//! - Unsupported execution environments surface as `Configuration` (via the
//!   shared read adapter) rather than pathname fallback; there is no ambient
//!   fallback after a contained failure.
//!
//! # Associated Write/Delete Audit (NOT contained by this batch — O-04)
//!
//! `write_lifecycle_journal`: ambient `ensure_dir(meta)`;
//! `tokio::fs::write` of a `.tmp.journal.<uuid>` file **without an fsync of
//! the file data before rename**; ambient `rename` onto the journal path; a
//! best-effort directory fsync whose result is **ignored** (`let _ =`). File
//! durability before the rename is therefore assumed, not established, and
//! directory-entry durability is best-effort only.
//! `delete_lifecycle_journal`: ambient `remove_file` (missing -> `Ok`), then
//! an ignored best-effort directory fsync.
//! Both resolve **ambient pathnames** while this read resolves the **pinned
//! root**: if the root pathname or journal ancestors are replaced at runtime,
//! writes/deletes operate on the replacement tree while reads keep observing
//! the originally opened root — the pre-existing read/write divergence shared
//! by every contained read path. No ambient read fallback compensates for
//! this; closing it requires write containment (Gate O-04), which this batch
//! deliberately does not attempt. Routing the read through containment does
//! not change any journal write/delete body; the indirect effect is on the
//! recovery boundary only: outer mutations (`publish_manifest`, manifest/tag
//! deletion, proxy eviction paths) call `recover_and_ensure_index_healthy`,
//! whose journal-read failure now aborts the mutation before recovery — after
//! repository lease acquisition, which is legitimate earlier work released by
//! the coordination guard, and without deleting or overwriting the unreadable
//! journal.
//!
//! # Resource Costs
//!
//! No numeric ceiling is imposed (no approved journal limit exists; the
//! ambient baseline buffered the whole file). One journal read buffers the
//! full payload once here and its parsed record once at the caller; these are
//! per-call costs with no global memory or concurrency budget, and payload
//! buffering bounds are not peak-memory bounds. Descriptor containment does
//! not provide snapshot isolation: the journal can change between read and
//! recovery.

use bytes::Bytes;
use storage_core::{ObjectKey, ObjectPayloadReader, ReadError};
use tokio::io::AsyncReadExt;

use crate::registry::canonical_name::CanonicalRepoName;
use crate::storage::StorageError;

/// Contained lifecycle-journal read returning the raw journal bytes.
pub(crate) async fn read_lifecycle_journal_impl(
    reader: &(impl ObjectPayloadReader + ?Sized),
    repo: &str,
) -> Result<Option<Bytes>, StorageError> {
    let canonical =
        CanonicalRepoName::parse(repo).map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
    let key_str = format!("repos/{}/meta/lifecycle_journal.json", canonical.as_str());
    let key = ObjectKey::parse(&key_str).map_err(|e| {
        StorageError::internal_invariant(format!("invalid journal key {key_str:?}: {e}"))
    })?;

    let payload = match reader.open_payload(&key).await {
        Ok(p) => p,
        Err(ReadError::NotFound { .. }) => return Ok(None),
        Err(other) => return Err(super::read_adapter::translate_payload_read_error(other)),
    };

    let (_metadata, mut stream) = payload.into_parts();
    let mut buffer = Vec::new();
    stream.read_to_end(&mut buffer).await.map_err(|e| {
        StorageError::io(format!("failed to read lifecycle journal {key_str}: {e}"))
    })?;
    Ok(Some(Bytes::from(buffer)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageErrorKind;
    use async_trait::async_trait;
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};
    use storage_core::{ObjectMetadata, ObjectPayload, ObjectStream};

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
            let queue = responses
                .get_mut(key)
                .unwrap_or_else(|| panic!("unexpected open_payload call: {key}"));
            queue
                .pop_front()
                .unwrap_or_else(|| panic!("no more scripted responses for: {key}"))
        }
    }

    fn payload_of(bytes: Vec<u8>) -> ObjectPayload {
        let meta = ObjectMetadata::new(bytes.len() as u64);
        let stream: ObjectStream = Box::pin(std::io::Cursor::new(bytes));
        ObjectPayload::new(meta, stream)
    }

    fn journal_key(repo: &str) -> ObjectKey {
        ObjectKey::parse(&format!("repos/{repo}/meta/lifecycle_journal.json")).unwrap()
    }

    fn rejection(code: i32) -> ReadError {
        ReadError::backend_with_source(
            "resolution rejected",
            Box::new(storage_fs::FsMetadataError::ResolutionRejected {
                raw_os_error: code,
                source: std::io::Error::from_raw_os_error(code),
            }),
        )
    }

    #[tokio::test]
    async fn test_fake_journal_roundtrip_raw_bytes_and_absence() {
        let k = journal_key("myrepo");

        // Bytes are returned verbatim — even payloads that are not valid
        // records (parsing stays with the callers), including empty files
        // (which are NOT absence).
        for body in [
            br#"{"op_id":"op-1"}"#.to_vec(),
            b"{broken".to_vec(),
            b"".to_vec(),
        ] {
            let fake = RecordingFakePayloadReader::new();
            fake.script(k.clone(), Ok(payload_of(body.clone())));
            let out = read_lifecycle_journal_impl(&fake, "myrepo")
                .await
                .unwrap()
                .expect("existing journal returns Some");
            assert_eq!(out.as_ref(), body.as_slice());
        }

        // Genuine absence -> None.
        let fake = RecordingFakePayloadReader::new();
        fake.script(k.clone(), Err(ReadError::not_found(k.clone())));
        assert!(
            read_lifecycle_journal_impl(&fake, "myrepo")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_fake_failures_never_become_absence() {
        let k = journal_key("myrepo");

        // Containment rejection (incl. ancestor rejection evidence) -> Io.
        for code in [libc::ELOOP, libc::EXDEV] {
            let fake = RecordingFakePayloadReader::new();
            fake.script(k.clone(), Err(rejection(code)));
            let err = read_lifecycle_journal_impl(&fake, "myrepo")
                .await
                .expect_err("resolution rejection must never be treated as absence");
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
        }

        // Permission denial -> Io.
        let fake = RecordingFakePayloadReader::new();
        fake.script(k.clone(), Err(ReadError::permission_denied(k.clone())));
        let err = read_lifecycle_journal_impl(&fake, "myrepo")
            .await
            .unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));

        // Non-regular object -> Io (rejected by fstat evidence, no blocking open).
        let fake = RecordingFakePayloadReader::new();
        fake.script(
            k.clone(),
            Err(ReadError::backend_with_source(
                "unsupported object type",
                Box::new(storage_fs::FsMetadataError::UnsupportedObjectType {
                    mode: libc::S_IFIFO,
                }),
            )),
        );
        let err = read_lifecycle_journal_impl(&fake, "myrepo")
            .await
            .unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));

        // Unsupported execution environment -> Configuration.
        let fake = RecordingFakePayloadReader::new();
        fake.script(
            k.clone(),
            Err(ReadError::backend_with_source(
                "syscall unsupported",
                Box::new(storage_fs::FsMetadataError::SyscallUnsupported(
                    std::io::Error::from_raw_os_error(libc::ENOSYS),
                )),
            )),
        );
        let err = read_lifecycle_journal_impl(&fake, "myrepo")
            .await
            .unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Configuration));

        // Mid-stream failure -> Io.
        struct FailingStream;
        impl tokio::io::AsyncRead for FailingStream {
            fn poll_read(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                _buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Err(std::io::Error::other("stream broken")))
            }
        }
        let fake = RecordingFakePayloadReader::new();
        fake.script(
            k.clone(),
            Ok(ObjectPayload::new(
                ObjectMetadata::new(10),
                Box::pin(FailingStream),
            )),
        );
        let err = read_lifecycle_journal_impl(&fake, "myrepo")
            .await
            .unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
    }

    #[tokio::test]
    async fn test_fake_invalid_repo_names_rejected_zero_reader_calls() {
        let fake = RecordingFakePayloadReader::new();
        for bad in ["../escape", "Bad_Repo!", "a//b", ""] {
            let err = read_lifecycle_journal_impl(&fake, bad).await.unwrap_err();
            assert!(
                matches!(err, StorageError::InvalidRepoName(_)),
                "expected InvalidRepoName for {bad:?}, got {err:?}"
            );
        }
        assert_eq!(fake.calls().len(), 0);
    }

    // ========================================================================
    // Linux-gated real filesystem and actual-caller tests
    // ========================================================================

    #[cfg(target_os = "linux")]
    mod real_fs_tests {
        use super::*;
        use crate::consistency::ConsistencyCoordinator;
        use crate::manifest_lifecycle::{
            LifecycleJournalRecord, LifecycleOpKind, LifecyclePhase, ManifestLifecycleError,
            ManifestLifecycleService,
        };
        use crate::registry::digest::Digest;
        use crate::storage::Storage;
        use crate::storage::fs::FsStorage;

        fn fixture_root() -> (tempfile::TempDir, std::path::PathBuf) {
            let fixture = tempfile::tempdir().expect("create tempdir");
            let root = fixture.path().join("storage_root");
            std::fs::create_dir_all(&root).expect("create storage root");
            (fixture, root)
        }

        fn journal_path(root: &std::path::Path, repo: &str) -> std::path::PathBuf {
            root.join("repos")
                .join(repo)
                .join("meta")
                .join("lifecycle_journal.json")
        }

        fn journal_record(repo: &str, target: &Digest) -> LifecycleJournalRecord {
            LifecycleJournalRecord {
                op_id: "op-test".to_string(),
                repo: crate::registry::canonical_name::CanonicalRepoName::parse(repo).unwrap(),
                op_kind: LifecycleOpKind::Publish,
                target_digest: target.clone(),
                target_reference: None,
                phase: LifecyclePhase::ManifestStored,
                owner_id: "owner-test".to_string(),
                lease_expiry_unix_secs: 9_999_999_999,
                started_unix_secs: 100,
                updated_unix_secs: 100,
                relevant_tags: Vec::new(),
                subject_digest: None,
                artifact_type: None,
                annotations: None,
                media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
                manifest_size: Some(100),
            }
        }

        async fn seed_manifest_and_tag(
            storage: &FsStorage,
            repo: &str,
            tag: &str,
        ) -> (Digest, Vec<u8>) {
            let manifest = serde_json::json!({
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "config": {
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "size": 2,
                    "digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                },
                "layers": []
            });
            let bytes = serde_json::to_vec(&manifest).unwrap();
            let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
            sha2::Digest::update(&mut hasher, &bytes);
            let digest = Digest::parse(&format!(
                "sha256:{}",
                hex::encode(sha2::Digest::finalize(hasher))
            ))
            .unwrap();
            storage
                .put_manifest(repo, &digest, bytes.clone().into())
                .await
                .expect("put manifest");
            storage.set_tag(repo, tag, &digest).await.expect("set tag");
            (digest, bytes)
        }

        #[tokio::test]
        async fn test_real_journal_roundtrip_absence_and_structural_validation() {
            let (_fixture, root) = fixture_root();
            let storage = FsStorage::new(root.clone(), 1024 * 1024);
            let d = Digest::parse(
                "sha256:2222222222222222222222222222222222222222222222222222222222222222",
            )
            .unwrap();

            // Genuine absence.
            assert!(
                storage
                    .read_lifecycle_journal("myrepo")
                    .await
                    .unwrap()
                    .is_none()
            );

            // Production write -> contained read round-trip of a real record.
            let rec = journal_record("myrepo", &d);
            let bytes = bytes::Bytes::from(serde_json::to_vec(&rec).unwrap());
            storage
                .write_lifecycle_journal("myrepo", bytes.clone())
                .await
                .unwrap();
            let read = storage
                .read_lifecycle_journal("myrepo")
                .await
                .unwrap()
                .expect("journal present");
            assert_eq!(read, bytes);
            let parsed: LifecycleJournalRecord = serde_json::from_slice(&read).unwrap();
            assert_eq!(parsed.op_id, "op-test");

            // Existing empty journal file is Some(empty), not absence.
            std::fs::write(journal_path(&root, "myrepo"), b"").unwrap();
            let read = storage
                .read_lifecycle_journal("myrepo")
                .await
                .unwrap()
                .expect("empty journal file is not absence");
            assert!(read.is_empty());

            // Structural/grammar validation preserved.
            let err = storage
                .read_lifecycle_journal("../escape")
                .await
                .unwrap_err();
            assert!(matches!(err, StorageError::InvalidRepoName(_)));

            // Production delete round-trip (audited ambient path, unchanged).
            storage.delete_lifecycle_journal("myrepo").await.unwrap();
            assert!(
                storage
                    .read_lifecycle_journal("myrepo")
                    .await
                    .unwrap()
                    .is_none()
            );
        }

        #[tokio::test]
        async fn test_real_symlinks_nonregular_and_pinned_root() {
            let (fixture, root) = fixture_root();
            let storage = FsStorage::new(root.clone(), 1024 * 1024);

            // Symlinked journal file -> Io, never absence.
            let outside = fixture.path().join("outside_journal.json");
            std::fs::write(&outside, b"{}").unwrap();
            let jp = journal_path(&root, "linkrepo");
            std::fs::create_dir_all(jp.parent().unwrap()).unwrap();
            std::os::unix::fs::symlink(&outside, &jp).unwrap();
            let err = storage
                .read_lifecycle_journal("linkrepo")
                .await
                .unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));

            // Symlinked meta/ ancestor -> Io.
            let outside_meta = fixture.path().join("outside_meta");
            std::fs::create_dir_all(&outside_meta).unwrap();
            std::fs::write(outside_meta.join("lifecycle_journal.json"), b"{}").unwrap();
            let repo_dir = root.join("repos").join("ancrepo");
            std::fs::create_dir_all(&repo_dir).unwrap();
            std::os::unix::fs::symlink(&outside_meta, repo_dir.join("meta")).unwrap();
            let err = storage.read_lifecycle_journal("ancrepo").await.unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));

            // Directory at the journal path -> Io (S_IFREG rejection).
            let dirjp = journal_path(&root, "dirrepo");
            std::fs::create_dir_all(&dirjp).unwrap();
            let err = storage.read_lifecycle_journal("dirrepo").await.unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));

            // Pinned root: replacing the root pathname does not redirect reads.
            let (fixture2, root2) = fixture_root();
            let storage2 = FsStorage::new(root2.clone(), 1024 * 1024);
            storage2
                .write_lifecycle_journal("pinrepo", bytes::Bytes::from_static(b"{\"v\":1}"))
                .await
                .unwrap();
            let renamed = fixture2.path().join("storage_root_old");
            std::fs::rename(&root2, &renamed).unwrap();
            std::fs::create_dir_all(journal_path(&root2, "pinrepo").parent().unwrap()).unwrap();
            std::fs::write(journal_path(&root2, "pinrepo"), b"{\"v\":2}").unwrap();
            let read = storage2
                .read_lifecycle_journal("pinrepo")
                .await
                .unwrap()
                .expect("journal from pinned original root");
            assert_eq!(read.as_ref(), b"{\"v\":1}");
        }

        #[tokio::test]
        async fn test_real_outer_mutation_aborts_on_unreadable_journal_then_recovers() {
            let (fixture, root) = fixture_root();
            let storage = std::sync::Arc::new(FsStorage::new(root.clone(), 50 * 1024 * 1024));
            let service =
                ManifestLifecycleService::new(storage.clone(), None, ConsistencyCoordinator::new());

            let (digest, _bytes) = seed_manifest_and_tag(&storage, "reporec", "v1").await;

            // Fault: the journal path is a symlink to an outside target.
            let outside = fixture.path().join("outside_journal.json");
            let planted = serde_json::to_vec(&journal_record("reporec", &digest)).unwrap();
            std::fs::write(&outside, &planted).unwrap();
            let jp = journal_path(&root, "reporec");
            std::fs::create_dir_all(jp.parent().unwrap()).unwrap();
            std::os::unix::fs::symlink(&outside, &jp).unwrap();

            // The actual outer mutation aborts at the recovery boundary.
            // Lease acquisition legitimately precedes the failure (released by
            // the coordination guard); no rollback of that earlier work is
            // claimed. The unreadable journal is neither deleted nor
            // overwritten, no conflicting operation starts, and the tag
            // survives.
            let err = service
                .delete_tag("reporec", "v1")
                .await
                .expect_err("outer mutation must abort when the journal is unreadable");
            assert!(
                matches!(err, ManifestLifecycleError::Storage(_)),
                "expected propagated storage error, got {err:?}"
            );
            assert!(
                std::fs::symlink_metadata(&jp)
                    .unwrap()
                    .file_type()
                    .is_symlink(),
                "unreadable journal must not be deleted or overwritten"
            );
            assert_eq!(
                std::fs::read(&outside).unwrap(),
                planted,
                "symlink target must remain unmodified"
            );
            assert!(
                storage.resolve_tag("reporec", "v1").await.is_ok(),
                "tag must survive the aborted mutation"
            );

            // Clearing the deterministic fault allows the same mutation to
            // succeed (retry contract preserved; the journal written and
            // deleted by the successful flow uses the audited ambient write
            // path, unchanged).
            std::fs::remove_file(&jp).unwrap();
            let result = service
                .delete_tag("reporec", "v1")
                .await
                .expect("mutation succeeds after the fault is cleared");
            let _ = result;
            assert!(
                storage.resolve_tag("reporec", "v1").await.is_err(),
                "tag deleted after successful retry"
            );
            assert!(
                storage
                    .read_lifecycle_journal("reporec")
                    .await
                    .unwrap()
                    .is_none(),
                "successful flow completes and clears its journal"
            );
        }

        #[tokio::test]
        async fn test_real_corrupt_journal_aborts_outer_mutation_and_is_preserved() {
            let (_fixture, root) = fixture_root();
            let storage = std::sync::Arc::new(FsStorage::new(root.clone(), 50 * 1024 * 1024));
            let service =
                ManifestLifecycleService::new(storage.clone(), None, ConsistencyCoordinator::new());
            seed_manifest_and_tag(&storage, "reporec", "v1").await;

            storage
                .write_lifecycle_journal("reporec", bytes::Bytes::from_static(b"{broken"))
                .await
                .unwrap();

            let err = service
                .delete_tag("reporec", "v1")
                .await
                .expect_err("corrupt journal must abort the outer mutation");
            assert!(
                matches!(err, ManifestLifecycleError::CorruptJournal(_)),
                "expected CorruptJournal, got {err:?}"
            );
            // Corrupt journal preserved byte-for-byte; tag intact.
            assert_eq!(
                std::fs::read(journal_path(&root, "reporec")).unwrap(),
                b"{broken"
            );
            assert!(storage.resolve_tag("reporec", "v1").await.is_ok());
        }

        #[tokio::test]
        async fn test_real_repo_identity_mismatch_fails_closed() {
            let (_fixture, root) = fixture_root();
            let storage = std::sync::Arc::new(FsStorage::new(root.clone(), 50 * 1024 * 1024));
            let service =
                ManifestLifecycleService::new(storage.clone(), None, ConsistencyCoordinator::new());
            let (digest, _bytes) = seed_manifest_and_tag(&storage, "reporec", "v1").await;

            // A journal recording a DIFFERENT repository placed at this
            // repository's journal path: recovery would apply its digests and
            // tags to the location repository, so the mismatch fails closed.
            let foreign = journal_record("otherrepo", &digest);
            let foreign_bytes = serde_json::to_vec(&foreign).unwrap();
            storage
                .write_lifecycle_journal("reporec", bytes::Bytes::from(foreign_bytes.clone()))
                .await
                .unwrap();

            let err = service
                .delete_tag("reporec", "v1")
                .await
                .expect_err("repository identity mismatch must abort recovery");
            match err {
                ManifestLifecycleError::Storage(StorageError::Internal { kind, .. }) => {
                    assert_eq!(kind, StorageErrorKind::CorruptData);
                }
                other => panic!("expected Storage(CorruptData), got {other:?}"),
            }
            // Mismatched journal preserved; tag intact.
            assert_eq!(
                std::fs::read(journal_path(&root, "reporec")).unwrap(),
                foreign_bytes
            );
            assert!(storage.resolve_tag("reporec", "v1").await.is_ok());
        }

        #[tokio::test]
        async fn test_real_is_lifecycle_active_contract() {
            let (fixture, root) = fixture_root();
            let storage = std::sync::Arc::new(FsStorage::new(root.clone(), 1024 * 1024));
            let service =
                ManifestLifecycleService::new(storage.clone(), None, ConsistencyCoordinator::new());
            let d = Digest::parse(
                "sha256:3333333333333333333333333333333333333333333333333333333333333333",
            )
            .unwrap();

            // Genuine absence -> Ok(false).
            assert!(!service.is_lifecycle_active("myrepo").await.unwrap());

            // Unexpired journal -> Ok(true).
            let mut rec = journal_record("myrepo", &d);
            storage
                .write_lifecycle_journal(
                    "myrepo",
                    bytes::Bytes::from(serde_json::to_vec(&rec).unwrap()),
                )
                .await
                .unwrap();
            assert!(service.is_lifecycle_active("myrepo").await.unwrap());

            // Expired lease -> Ok(false).
            rec.lease_expiry_unix_secs = 1;
            storage
                .write_lifecycle_journal(
                    "myrepo",
                    bytes::Bytes::from(serde_json::to_vec(&rec).unwrap()),
                )
                .await
                .unwrap();
            assert!(!service.is_lifecycle_active("myrepo").await.unwrap());

            // Read failures propagate instead of presenting as inactive
            // (previously `.ok().flatten()` reported false).
            let outside = fixture.path().join("outside_journal.json");
            std::fs::write(&outside, b"{}").unwrap();
            storage.delete_lifecycle_journal("myrepo").await.unwrap();
            std::os::unix::fs::symlink(&outside, journal_path(&root, "myrepo")).unwrap();
            assert!(service.is_lifecycle_active("myrepo").await.is_err());
        }
    }
}
