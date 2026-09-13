//! Contained filesystem repository timestamps and storage-emptiness inspection
//! for `registry-rust`.
//!
//! # Architecture and Scope
//!
//! Implements the production paths behind `FsStorage::repo_timestamps`
//! (formerly the ambient `max_mtime_in_dir` scans) and the subtree probes of
//! `FsStorage::is_storage_empty` (formerly the ambient recursive
//! `fs_dir_has_any_entry`). All filesystem observation resolves beneath the
//! shared pinned root descriptor via [`storage_fs::FsMetadataReader`]:
//! directory enumeration through `enumerate_dir` (`openat2` with
//! `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`) and
//! per-file attribute inspection through `inspect_file_metadata` (`O_PATH`
//! acquisition, `S_IFREG` enforcement, `fstat` with checked nanosecond
//! timestamp conversion). Blocking work executes off async executor threads
//! inside the dependency's `spawn_blocking` offload.
//!
//! # Preserved Semantics
//!
//! Repository timestamps (`repo_timestamps_impl`):
//! - Missing repository directory -> [`StorageError::NotFound`].
//! - Repository present with missing `tags/` or `manifests/` directory -> the
//!   corresponding timestamp is `None` (also when the directory vanishes
//!   between the repository probe and its enumeration).
//! - Only the direct children of `tags/` and `manifests/` are inspected; no
//!   recursion.
//! - Only regular files contribute; directories and other object types do not.
//! - Hidden (dot-prefixed) regular files still contribute, as before
//!   (e.g. persisted `.lock.*` files).
//! - The maximum modification time per directory is selected; an empty
//!   directory yields `None`. Timestamp precision is whatever the filesystem
//!   reports (nanosecond-capable, converted with checked arithmetic; pre-epoch
//!   timestamps are representable and compare correctly through
//!   [`std::time::SystemTime`] ordering).
//! - A `tags`/`manifests` path that is a regular file instead of a directory
//!   maps to the legacy [`StorageErrorKind::Io`] taxonomy, as does an
//!   unreadable directory.
//!
//! Storage emptiness (`contained_subtree_has_any_entry`):
//! - A missing subtree root is empty (`false` contribution), matching the
//!   legacy `NotFound -> Ok(false)`.
//! - Any non-directory entry (regular file, symlink, FIFO, socket, device)
//!   anywhere in the subtree makes storage non-empty; this conclusion is
//!   returned as soon as one qualifying entry is observed, without exhaustive
//!   enumeration — matching the legacy short-circuit.
//! - Directory entries are descended; a subtree containing only empty
//!   directories is still empty.
//! - A directory that vanishes between observation and its own enumeration is
//!   treated as an empty subtree (legacy recursion returned `false` for it).
//! - A subtree path that is not a directory, an unreadable directory, and
//!   mid-iteration failures map to the legacy [`StorageErrorKind::Io`]
//!   taxonomy and fail the check closed.
//!
//! # Intentional Containment Changes (relative to the ambient implementation)
//!
//! - Repository names are structurally validated before key composition
//!   (shared `tag_read::validate_path_component`); traversal names such as
//!   `../x` now fail with `InvalidRepoName` instead of resolving ambiently
//!   (they previously produced `NotFound` in practice).
//! - Symlinked path components (a symlinked repository directory, `tags/`,
//!   `manifests/`, or emptiness subtree root such as `blobs/`) are rejected
//!   with [`StorageErrorKind::Io`] instead of being silently followed outside
//!   the pinned root.
//! - Symlink entries inside `tags/`/`manifests/` no longer contribute
//!   timestamps (the legacy scan followed them with `stat` and counted the
//!   target's mtime when it was a regular file). The dirent-type policy used
//!   by every other contained walk applies.
//! - Mid-iteration enumeration and per-entry inspection failures previously
//!   truncated the timestamp scan silently (`while let Ok(Some(..))`, per-entry
//!   `Err(_) => continue`), producing a maximum over an incomplete relevant
//!   set. Genuine inspection failures (permission denial, stat failure,
//!   invalid metadata, containment resolution rejection, unsupported
//!   environment) now propagate as errors — a containment resolution
//!   rejection (`ELOOP`/`EXDEV`) does not establish the leaf's object type
//!   and can stem from an ancestor substitution, so it is never suppressed
//!   and no successful partial timestamp result is returned after it. Entries
//!   that are genuinely absent (`NotFound`) or confirmed non-regular objects
//!   at inspection time (`UnsupportedObjectType` from `fstat` on the acquired
//!   leaf) are excluded as non-qualifying, preserving the legacy filtering
//!   while distinguishing absence from failure. Observed symlink dirents are
//!   filtered during enumeration, which is distinct from a failure resolving
//!   a previously observed regular entry.
//! - Entry names that cannot form a contained [`ObjectKey`] (non-UTF-8,
//!   backslashes, control characters) fail closed with
//!   [`StorageErrorKind::CorruptData`]: for timestamps they would otherwise be
//!   silently excluded from the relevant set (the legacy scan included them,
//!   as it never needed the name); for emptiness a non-descendable directory
//!   could hide entries and produce a false empty-storage success. Non-directory
//!   entries in the emptiness walk never need a name and conclude "not empty"
//!   regardless of their name bytes.
//! - Reads resolve beneath the pinned root descriptor: replacing the root
//!   pathname no longer redirects timestamps or emptiness to the replacement
//!   tree.
//!
//! # Resource Costs
//!
//! No numeric ceilings are imposed: per-call `DirEnumerationLimits` are
//! effectively unbounded, matching the ambient baseline, and no approved
//! production limit's scope covers these operations. Actual costs:
//! `repo_timestamps` performs one repository-probe enumeration plus one
//! enumeration per present `tags`/`manifests` directory and one
//! `inspect_file_metadata` call per regular entry (each an `openat2` + `fstat`
//! pair on a blocking thread); memory is one entry batch per enumerated
//! directory. The emptiness walk enumerates directories breadth-first until
//! the first non-directory entry, retaining a pending-key queue and one entry
//! batch at a time; a pathological all-directory tree is walked completely.
//! These are per-call costs with no global memory or concurrency budget.
//!
//! # Coherence Demarcation
//!
//! Descriptor containment provides none of: snapshot isolation, hard-link
//! isolation, mount isolation, global memory protection, or hardware
//! durability. Entries may appear, vanish, or change between the enumeration
//! and inspection steps of one call; timestamps and emptiness describe
//! point-in-time observations only.

use std::collections::VecDeque;
use std::time::SystemTime;

use async_trait::async_trait;
use storage_core::{ObjectKey, ReadError};
use storage_fs::{
    DirEntry, DirEntryType, DirEnumerationLimits, FsDirError, FsFileMetadata, FsMetadataReader,
};

use super::catalog_discovery::map_contained_dir_error;
use crate::storage::{RepoTimestamps, StorageError};

/// Unbounded per-call enumeration limits preserving the ambient baseline.
fn unbounded_dir_limits() -> DirEnumerationLimits {
    DirEnumerationLimits::new(usize::MAX, usize::MAX)
}

/// Narrow seam over the pinned reader operations used by timestamp and
/// emptiness inspection, enabling deterministic fault-injection fakes for
/// failures real filesystems cannot reproduce reliably.
#[async_trait]
pub(crate) trait RepoMetaInspector: Send + Sync {
    /// Enumerates entries of one directory relative to the pinned storage root.
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError>;

    /// Inspects size and modification time of one regular file relative to the
    /// pinned storage root.
    async fn inspect_file_metadata(&self, key: &ObjectKey) -> Result<FsFileMetadata, ReadError>;
}

#[async_trait]
impl RepoMetaInspector for FsMetadataReader {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError> {
        FsMetadataReader::enumerate_dir(self, target, limits).await
    }

    async fn inspect_file_metadata(&self, key: &ObjectKey) -> Result<FsFileMetadata, ReadError> {
        FsMetadataReader::inspect_file_metadata(self, key).await
    }
}

/// Classifies a per-entry inspection failure: an entry that is genuinely
/// absent ([`ReadError::NotFound`]) or confirmed to be a non-regular object at
/// inspection time ([`storage_fs::FsMetadataError::UnsupportedObjectType`],
/// established by `fstat` on the acquired leaf descriptor) is excluded from
/// the timestamp set, matching the legacy non-file filtering. Every other
/// failure propagates.
///
/// Containment resolution rejection
/// ([`storage_fs::FsMetadataError::ResolutionRejected`], e.g. `ELOOP`/`EXDEV`)
/// is deliberately NOT treated as evidence about the leaf's object type: the
/// kernel rejects the whole path resolution, which can equally be caused by an
/// ancestor path component being substituted (for example the `tags/`
/// directory replaced by a symlink after enumeration). Suppressing it could
/// return `Ok(None)` or a maximum computed over only the earlier-inspected
/// files. It therefore propagates as an inspection failure through
/// [`super::read_adapter::translate_metadata_read_error`] (legacy `Io`
/// taxonomy), and no successful partial timestamp result is returned after it.
fn inspection_says_not_a_regular_file(err: &ReadError) -> bool {
    match err {
        ReadError::NotFound { .. } => true,
        ReadError::Backend {
            source: Some(source),
            ..
        } => matches!(
            source.downcast_ref::<storage_fs::FsMetadataError>(),
            Some(storage_fs::FsMetadataError::UnsupportedObjectType { .. })
        ),
        _ => false,
    }
}

/// Computes the maximum modification time across the regular-file children of
/// one directory beneath the pinned root.
///
/// - Missing directory (including one that vanished after being observed in
///   the repository probe) -> `Ok(None)`.
/// - Empty directory or no qualifying regular files -> `Ok(None)`.
/// - Non-regular dirents (directories, symlinks, others) are skipped.
/// - Names that cannot form a contained key fail closed with `CorruptData`.
/// - Inspection failures propagate unless the entry is absent or confirmed to
///   be a non-regular object (see [`inspection_says_not_a_regular_file`];
///   containment resolution rejection propagates).
async fn contained_max_mtime(
    ops: &(impl RepoMetaInspector + ?Sized),
    dir_key_str: &str,
) -> Result<Option<SystemTime>, StorageError> {
    let dir_key = ObjectKey::parse(dir_key_str).map_err(|e| {
        StorageError::internal_invariant(format!("invalid directory key {dir_key_str:?}: {e}"))
    })?;

    let entries = match ops
        .enumerate_dir(Some(&dir_key), unbounded_dir_limits())
        .await
    {
        Ok(entries) => entries,
        Err(FsDirError::NotFound { .. }) => return Ok(None),
        Err(other) => return Err(map_contained_dir_error(other, dir_key_str)),
    };

    let mut max_time: Option<SystemTime> = None;
    for entry in entries {
        if entry.file_type() != DirEntryType::Regular {
            continue;
        }

        let Some(name) = entry.name().to_str() else {
            return Err(StorageError::corrupt_data(format!(
                "non-UTF-8 file name in {dir_key_str} prevents contained timestamp inspection: {:?}",
                entry.name()
            )));
        };

        let child_str = format!("{dir_key_str}/{name}");
        let child_key = ObjectKey::parse(&child_str).map_err(|err| {
            StorageError::corrupt_data(format!(
                "file name in {dir_key_str} cannot form a contained object key: {name:?}: {err}"
            ))
        })?;

        match ops.inspect_file_metadata(&child_key).await {
            Ok(meta) => {
                if let Some(modified) = meta.modified() {
                    max_time = Some(match max_time {
                        Some(current) if current >= modified => current,
                        _ => modified,
                    });
                }
            }
            Err(err) if inspection_says_not_a_regular_file(&err) => continue,
            Err(err) => return Err(super::read_adapter::translate_metadata_read_error(err)),
        }
    }
    Ok(max_time)
}

/// Computes repository tag/manifest timestamps beneath the pinned root,
/// preserving the legacy `repo_timestamps` contract (see module docs).
pub(crate) async fn repo_timestamps_impl(
    ops: &(impl RepoMetaInspector + ?Sized),
    repo: &str,
) -> Result<RepoTimestamps, StorageError> {
    super::tag_read::validate_path_component(repo, "repository name")?;

    let repo_key_str = format!("repos/{repo}");
    let repo_key = ObjectKey::parse(&repo_key_str)
        .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;

    // Existence probe: distinguishes a missing repository (legacy NotFound)
    // from a present repository with missing tags/manifests directories
    // (legacy Ok with None timestamps). Entries are not otherwise consumed.
    match ops
        .enumerate_dir(Some(&repo_key), unbounded_dir_limits())
        .await
    {
        Ok(_) => {}
        Err(FsDirError::NotFound { .. }) => return Err(StorageError::NotFound),
        Err(other) => return Err(map_contained_dir_error(other, &repo_key_str)),
    }

    let last_tag_update = contained_max_mtime(ops, &format!("{repo_key_str}/tags")).await?;
    let last_manifest_update =
        contained_max_mtime(ops, &format!("{repo_key_str}/manifests")).await?;

    Ok(RepoTimestamps {
        last_tag_update,
        last_manifest_update,
    })
}

/// Determines whether a storage subtree beneath the pinned root contains any
/// non-directory entry, preserving the legacy `fs_dir_has_any_entry` contract
/// (see module docs). Returns `true` as soon as one qualifying entry is
/// observed; an unreadable or non-descendable area fails closed instead of
/// contributing a false empty result.
pub(crate) async fn contained_subtree_has_any_entry(
    ops: &(impl RepoMetaInspector + ?Sized),
    subtree: &str,
) -> Result<bool, StorageError> {
    let root_key = ObjectKey::parse(subtree).map_err(|e| {
        StorageError::internal_invariant(format!("invalid subtree key {subtree:?}: {e}"))
    })?;

    let mut queue: VecDeque<ObjectKey> = VecDeque::new();
    queue.push_back(root_key);

    while let Some(current_key) = queue.pop_front() {
        let entries = match ops
            .enumerate_dir(Some(&current_key), unbounded_dir_limits())
            .await
        {
            Ok(entries) => entries,
            Err(FsDirError::NotFound { .. }) => {
                // Missing subtree root or a directory removed after being
                // observed: both count as empty contributions (legacy).
                continue;
            }
            Err(other) => return Err(map_contained_dir_error(other, current_key.as_str())),
        };

        for entry in entries {
            if entry.file_type() != DirEntryType::Directory {
                // Any non-directory entry makes storage non-empty; no name
                // decoding is required for this conclusion.
                return Ok(true);
            }

            let Some(name) = entry.name().to_str() else {
                return Err(StorageError::corrupt_data(format!(
                    "non-UTF-8 directory name in {} prevents contained emptiness inspection: {:?}",
                    current_key.as_str(),
                    entry.name()
                )));
            };
            let child_str = format!("{}/{name}", current_key.as_str());
            let child_key = ObjectKey::parse(&child_str).map_err(|err| {
                StorageError::corrupt_data(format!(
                    "directory name in {} cannot form a contained object key: {name:?}: {err}",
                    current_key.as_str()
                ))
            })?;
            queue.push_back(child_key);
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageErrorKind;
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, UNIX_EPOCH};

    struct RecordingFakeInspector {
        dir_calls: Arc<Mutex<Vec<Option<ObjectKey>>>>,
        inspect_calls: Arc<Mutex<Vec<ObjectKey>>>,
        dir_responses:
            Arc<Mutex<HashMap<Option<ObjectKey>, VecDeque<Result<Vec<DirEntry>, FsDirError>>>>>,
        inspect_responses:
            Arc<Mutex<HashMap<ObjectKey, VecDeque<Result<FsFileMetadata, ReadError>>>>>,
    }

    impl RecordingFakeInspector {
        fn new() -> Self {
            Self {
                dir_calls: Arc::new(Mutex::new(Vec::new())),
                inspect_calls: Arc::new(Mutex::new(Vec::new())),
                dir_responses: Arc::new(Mutex::new(HashMap::new())),
                inspect_responses: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn script_dir(
            &self,
            target: Option<ObjectKey>,
            response: Result<Vec<DirEntry>, FsDirError>,
        ) {
            self.dir_responses
                .lock()
                .unwrap()
                .entry(target)
                .or_default()
                .push_back(response);
        }

        fn script_inspect(&self, key: ObjectKey, response: Result<FsFileMetadata, ReadError>) {
            self.inspect_responses
                .lock()
                .unwrap()
                .entry(key)
                .or_default()
                .push_back(response);
        }

        fn dir_calls(&self) -> Vec<Option<ObjectKey>> {
            self.dir_calls.lock().unwrap().clone()
        }

        fn inspect_calls(&self) -> Vec<ObjectKey> {
            self.inspect_calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl RepoMetaInspector for RecordingFakeInspector {
        async fn enumerate_dir(
            &self,
            target: Option<&ObjectKey>,
            _limits: DirEnumerationLimits,
        ) -> Result<Vec<DirEntry>, FsDirError> {
            self.dir_calls.lock().unwrap().push(target.cloned());
            let mut responses = self.dir_responses.lock().unwrap();
            let queue = responses
                .get_mut(&target.cloned())
                .unwrap_or_else(|| panic!("unexpected enumerate_dir call with target: {target:?}"));
            queue
                .pop_front()
                .unwrap_or_else(|| panic!("no more scripted dir responses for: {target:?}"))
        }

        async fn inspect_file_metadata(
            &self,
            key: &ObjectKey,
        ) -> Result<FsFileMetadata, ReadError> {
            self.inspect_calls.lock().unwrap().push(key.clone());
            let mut responses = self.inspect_responses.lock().unwrap();
            let queue = responses
                .get_mut(key)
                .unwrap_or_else(|| panic!("unexpected inspect call with key: {key}"));
            queue
                .pop_front()
                .unwrap_or_else(|| panic!("no more scripted inspect responses for: {key}"))
        }
    }

    fn dir_entry(name: &str, file_type: DirEntryType) -> DirEntry {
        DirEntry::new(OsString::from(name), file_type)
    }

    fn key(s: &str) -> ObjectKey {
        ObjectKey::parse(s).unwrap()
    }

    fn meta_at(secs: u64) -> FsFileMetadata {
        FsFileMetadata::new(1, Some(UNIX_EPOCH + Duration::from_secs(secs)))
    }

    // ========================================================================
    // Timestamps: fake-driven contract tests
    // ========================================================================

    #[tokio::test]
    async fn test_fake_missing_repo_not_found_single_probe() {
        let fake = RecordingFakeInspector::new();
        fake.script_dir(
            Some(key("repos/missing")),
            Err(FsDirError::NotFound {
                path: Some("repos/missing".to_string()),
            }),
        );

        let err = repo_timestamps_impl(&fake, "missing").await.unwrap_err();
        assert!(matches!(err, StorageError::NotFound));
        assert_eq!(fake.dir_calls().len(), 1);
        assert_eq!(fake.inspect_calls().len(), 0);
    }

    #[tokio::test]
    async fn test_fake_traversal_repo_name_rejected_zero_calls() {
        let fake = RecordingFakeInspector::new();
        for bad in ["../escape", "a/../b", "bad\\name", ""] {
            let err = repo_timestamps_impl(&fake, bad).await.unwrap_err();
            assert!(
                matches!(err, StorageError::InvalidRepoName(_)),
                "expected InvalidRepoName for {bad:?}, got {err:?}"
            );
        }
        assert_eq!(fake.dir_calls().len(), 0);
    }

    #[tokio::test]
    async fn test_fake_missing_and_vanished_subdirs_yield_none() {
        let fake = RecordingFakeInspector::new();
        fake.script_dir(Some(key("repos/r")), Ok(vec![]));
        fake.script_dir(
            Some(key("repos/r/tags")),
            Err(FsDirError::NotFound {
                path: Some("repos/r/tags".to_string()),
            }),
        );
        fake.script_dir(
            Some(key("repos/r/manifests")),
            Err(FsDirError::NotFound {
                path: Some("repos/r/manifests".to_string()),
            }),
        );

        let ts = repo_timestamps_impl(&fake, "r").await.unwrap();
        assert_eq!(ts.last_tag_update, None);
        assert_eq!(ts.last_manifest_update, None);
    }

    #[tokio::test]
    async fn test_fake_max_selection_and_nonqualifying_entry_filtering() {
        let fake = RecordingFakeInspector::new();
        fake.script_dir(Some(key("repos/r")), Ok(vec![]));
        // tags: two regular files (older + newer), plus a hidden regular file
        // (contributes, legacy), plus skipped symlink/dir/other entries.
        fake.script_dir(
            Some(key("repos/r/tags")),
            Ok(vec![
                dir_entry("older", DirEntryType::Regular),
                dir_entry("newer", DirEntryType::Regular),
                dir_entry(".lock.newer", DirEntryType::Regular),
                dir_entry("sym", DirEntryType::Symlink),
                dir_entry("subdir", DirEntryType::Directory),
                dir_entry("fifo", DirEntryType::Other),
            ]),
        );
        fake.script_inspect(key("repos/r/tags/older"), Ok(meta_at(100)));
        fake.script_inspect(key("repos/r/tags/newer"), Ok(meta_at(300)));
        fake.script_inspect(key("repos/r/tags/.lock.newer"), Ok(meta_at(200)));
        fake.script_dir(
            Some(key("repos/r/manifests")),
            Ok(vec![dir_entry("m1", DirEntryType::Regular)]),
        );
        fake.script_inspect(key("repos/r/manifests/m1"), Ok(meta_at(50)));

        let ts = repo_timestamps_impl(&fake, "r").await.unwrap();
        assert_eq!(
            ts.last_tag_update,
            Some(UNIX_EPOCH + Duration::from_secs(300)),
            "maximum across qualifying regular files including hidden entries"
        );
        assert_eq!(
            ts.last_manifest_update,
            Some(UNIX_EPOCH + Duration::from_secs(50))
        );
        // Only the four regular files were inspected.
        assert_eq!(fake.inspect_calls().len(), 4);
    }

    #[tokio::test]
    async fn test_fake_vanished_and_type_changed_entries_excluded_not_failed() {
        let fake = RecordingFakeInspector::new();
        fake.script_dir(Some(key("repos/r")), Ok(vec![]));
        fake.script_dir(
            Some(key("repos/r/tags")),
            Ok(vec![
                dir_entry("vanished", DirEntryType::Regular),
                dir_entry("became_dir", DirEntryType::Regular),
                dir_entry("stable", DirEntryType::Regular),
            ]),
        );
        let k_vanished = key("repos/r/tags/vanished");
        fake.script_inspect(k_vanished.clone(), Err(ReadError::not_found(k_vanished)));
        // A confirmed non-regular object: fstat on the acquired leaf descriptor
        // established the type, so exclusion is safe (legacy non-file filtering).
        fake.script_inspect(
            key("repos/r/tags/became_dir"),
            Err(ReadError::backend_with_source(
                "unsupported object type",
                Box::new(storage_fs::FsMetadataError::UnsupportedObjectType {
                    mode: libc::S_IFDIR,
                }),
            )),
        );
        fake.script_inspect(key("repos/r/tags/stable"), Ok(meta_at(700)));
        fake.script_dir(
            Some(key("repos/r/manifests")),
            Err(FsDirError::NotFound {
                path: Some("repos/r/manifests".to_string()),
            }),
        );

        let ts = repo_timestamps_impl(&fake, "r").await.unwrap();
        assert_eq!(
            ts.last_tag_update,
            Some(UNIX_EPOCH + Duration::from_secs(700)),
            "absent / confirmed non-regular entries are excluded, not failures"
        );
    }

    #[tokio::test]
    async fn test_fake_resolution_rejection_propagates_never_suppressed() {
        // Injected evidence via the recording fake, deterministically modeling
        // what an ancestor substitution (e.g. tags/ replaced by a symlink after
        // enumeration) produces at the seam: openat2 rejects the whole path
        // resolution. This is scripted, not a real concurrent filesystem race.
        fn rejection(code: i32) -> ReadError {
            ReadError::backend_with_source(
                "resolution rejected",
                Box::new(storage_fs::FsMetadataError::ResolutionRejected {
                    raw_os_error: code,
                    source: std::io::Error::from_raw_os_error(code),
                }),
            )
        }

        for code in [libc::ELOOP, libc::EXDEV] {
            // 1. Rejection on the FIRST candidate: an error, not Ok(None).
            let fake = RecordingFakeInspector::new();
            fake.script_dir(Some(key("repos/r")), Ok(vec![]));
            fake.script_dir(
                Some(key("repos/r/tags")),
                Ok(vec![dir_entry("first", DirEntryType::Regular)]),
            );
            fake.script_inspect(key("repos/r/tags/first"), Err(rejection(code)));

            let err = repo_timestamps_impl(&fake, "r")
                .await
                .expect_err("resolution rejection on the first candidate must be an error");
            assert_eq!(
                err.internal_kind(),
                Some(StorageErrorKind::Io),
                "existing translate_metadata_read_error mapping preserved for errno {code}"
            );

            // 2. Rejection AFTER a successful inspection: an error, not a
            //    partial maximum; 3. no subsequent inspection is attempted
            //    (the fake would panic on an unscripted call, and the recorded
            //    call count proves the scan stopped).
            let fake = RecordingFakeInspector::new();
            fake.script_dir(Some(key("repos/r")), Ok(vec![]));
            fake.script_dir(
                Some(key("repos/r/tags")),
                Ok(vec![
                    dir_entry("earlier_ok", DirEntryType::Regular),
                    dir_entry("rejected", DirEntryType::Regular),
                    dir_entry("never_inspected", DirEntryType::Regular),
                ]),
            );
            fake.script_inspect(key("repos/r/tags/earlier_ok"), Ok(meta_at(100)));
            fake.script_inspect(key("repos/r/tags/rejected"), Err(rejection(code)));
            // Intentionally NO scripted response for "never_inspected".

            let err = repo_timestamps_impl(&fake, "r")
                .await
                .expect_err("no partial maximum after a resolution rejection");
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
            assert_eq!(
                fake.inspect_calls().len(),
                2,
                "no further inspections after the failure (errno {code})"
            );
        }
    }

    #[tokio::test]
    async fn test_fake_inspection_failure_no_partial_maximum() {
        let fake = RecordingFakeInspector::new();
        fake.script_dir(Some(key("repos/r")), Ok(vec![]));
        fake.script_dir(
            Some(key("repos/r/tags")),
            Ok(vec![
                dir_entry("ok_first", DirEntryType::Regular),
                dir_entry("denied", DirEntryType::Regular),
            ]),
        );
        fake.script_inspect(key("repos/r/tags/ok_first"), Ok(meta_at(100)));
        let k_denied = key("repos/r/tags/denied");
        fake.script_inspect(
            k_denied.clone(),
            Err(ReadError::permission_denied(k_denied)),
        );

        let err = repo_timestamps_impl(&fake, "r")
            .await
            .expect_err("a genuine inspection failure must not yield a partial maximum");
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));

        // Stat failure after acquisition likewise propagates.
        let fake2 = RecordingFakeInspector::new();
        fake2.script_dir(Some(key("repos/r")), Ok(vec![]));
        fake2.script_dir(
            Some(key("repos/r/tags")),
            Ok(vec![dir_entry("statfail", DirEntryType::Regular)]),
        );
        fake2.script_inspect(
            key("repos/r/tags/statfail"),
            Err(ReadError::backend_with_source(
                "failed to stat inspected descriptor",
                Box::new(storage_fs::FsMetadataError::StatFailed {
                    stage: "file inspection",
                    source: std::io::Error::from_raw_os_error(libc::EIO),
                }),
            )),
        );
        let err = repo_timestamps_impl(&fake2, "r").await.unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
    }

    #[tokio::test]
    async fn test_fake_unaddressable_and_non_utf8_names_fail_closed() {
        // Unaddressable UTF-8 name.
        let fake = RecordingFakeInspector::new();
        fake.script_dir(Some(key("repos/r")), Ok(vec![]));
        fake.script_dir(
            Some(key("repos/r/tags")),
            Ok(vec![dir_entry("bad\\name", DirEntryType::Regular)]),
        );
        let err = repo_timestamps_impl(&fake, "r").await.unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::CorruptData));

        // Non-UTF-8 name.
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let fake = RecordingFakeInspector::new();
            fake.script_dir(Some(key("repos/r")), Ok(vec![]));
            let non_utf8 = std::ffi::OsStr::from_bytes(b"bad_\xff").to_os_string();
            fake.script_dir(
                Some(key("repos/r/tags")),
                Ok(vec![DirEntry::new(non_utf8, DirEntryType::Regular)]),
            );
            let err = repo_timestamps_impl(&fake, "r").await.unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::CorruptData));
        }
    }

    #[tokio::test]
    async fn test_fake_subdir_wrong_type_and_resolution_errors_are_io() {
        // tags path is not a directory (legacy Io).
        let fake = RecordingFakeInspector::new();
        fake.script_dir(Some(key("repos/r")), Ok(vec![]));
        fake.script_dir(
            Some(key("repos/r/tags")),
            Err(FsDirError::NotADirectory {
                path: Some("repos/r/tags".to_string()),
            }),
        );
        let err = repo_timestamps_impl(&fake, "r").await.unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));

        // Symlinked tags directory rejected by contained resolution.
        let fake = RecordingFakeInspector::new();
        fake.script_dir(Some(key("repos/r")), Ok(vec![]));
        fake.script_dir(
            Some(key("repos/r/tags")),
            Err(FsDirError::ResolutionRejected {
                raw_os_error: libc::ELOOP,
                source: std::io::Error::from_raw_os_error(libc::ELOOP),
            }),
        );
        let err = repo_timestamps_impl(&fake, "r").await.unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));

        // Repository probe permission denial keeps the legacy Io taxonomy.
        let fake = RecordingFakeInspector::new();
        fake.script_dir(
            Some(key("repos/r")),
            Err(FsDirError::PermissionDenied {
                path: Some("repos/r".to_string()),
                source: std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
            }),
        );
        let err = repo_timestamps_impl(&fake, "r").await.unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
    }

    #[tokio::test]
    async fn test_fake_metadata_without_timestamp_contributes_nothing() {
        let fake = RecordingFakeInspector::new();
        fake.script_dir(Some(key("repos/r")), Ok(vec![]));
        fake.script_dir(
            Some(key("repos/r/tags")),
            Ok(vec![dir_entry("no_ts", DirEntryType::Regular)]),
        );
        fake.script_inspect(key("repos/r/tags/no_ts"), Ok(FsFileMetadata::new(1, None)));
        fake.script_dir(
            Some(key("repos/r/manifests")),
            Err(FsDirError::NotFound {
                path: Some("repos/r/manifests".to_string()),
            }),
        );

        let ts = repo_timestamps_impl(&fake, "r").await.unwrap();
        assert_eq!(ts.last_tag_update, None);
    }

    #[tokio::test]
    async fn test_fake_pre_epoch_timestamps_compare_correctly() {
        let fake = RecordingFakeInspector::new();
        fake.script_dir(Some(key("repos/r")), Ok(vec![]));
        fake.script_dir(
            Some(key("repos/r/tags")),
            Ok(vec![
                dir_entry("pre_epoch", DirEntryType::Regular),
                dir_entry("post_epoch", DirEntryType::Regular),
            ]),
        );
        let pre = UNIX_EPOCH - Duration::from_secs(1000);
        fake.script_inspect(
            key("repos/r/tags/pre_epoch"),
            Ok(FsFileMetadata::new(1, Some(pre))),
        );
        fake.script_inspect(key("repos/r/tags/post_epoch"), Ok(meta_at(10)));
        fake.script_dir(
            Some(key("repos/r/manifests")),
            Err(FsDirError::NotFound {
                path: Some("repos/r/manifests".to_string()),
            }),
        );

        let ts = repo_timestamps_impl(&fake, "r").await.unwrap();
        assert_eq!(
            ts.last_tag_update,
            Some(UNIX_EPOCH + Duration::from_secs(10))
        );
    }

    // ========================================================================
    // Emptiness: fake-driven contract tests
    // ========================================================================

    #[tokio::test]
    async fn test_fake_emptiness_missing_root_and_empty_nested_dirs() {
        let fake = RecordingFakeInspector::new();
        fake.script_dir(
            Some(key("blobs")),
            Err(FsDirError::NotFound {
                path: Some("blobs".to_string()),
            }),
        );
        assert!(
            !contained_subtree_has_any_entry(&fake, "blobs")
                .await
                .unwrap()
        );

        // Nested empty directories only -> still empty, fully descended.
        let fake = RecordingFakeInspector::new();
        fake.script_dir(
            Some(key("uploads")),
            Ok(vec![dir_entry("a", DirEntryType::Directory)]),
        );
        fake.script_dir(
            Some(key("uploads/a")),
            Ok(vec![dir_entry("b", DirEntryType::Directory)]),
        );
        fake.script_dir(Some(key("uploads/a/b")), Ok(vec![]));
        assert!(
            !contained_subtree_has_any_entry(&fake, "uploads")
                .await
                .unwrap()
        );
        assert_eq!(fake.dir_calls().len(), 3);
    }

    #[tokio::test]
    async fn test_fake_emptiness_early_not_empty_on_any_nondir_entry() {
        for ft in [
            DirEntryType::Regular,
            DirEntryType::Symlink,
            DirEntryType::Other,
        ] {
            let fake = RecordingFakeInspector::new();
            fake.script_dir(
                Some(key("quarantine")),
                Ok(vec![
                    dir_entry("hit", ft),
                    dir_entry("never_descended", DirEntryType::Directory),
                ]),
            );
            assert!(
                contained_subtree_has_any_entry(&fake, "quarantine")
                    .await
                    .unwrap(),
                "non-directory entry type {ft:?} must conclude non-empty"
            );
            assert_eq!(
                fake.dir_calls().len(),
                1,
                "conclusion is reached without further enumerations for {ft:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_fake_emptiness_nondir_entry_name_never_decoded() {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let fake = RecordingFakeInspector::new();
            let non_utf8 = std::ffi::OsStr::from_bytes(b"weird_\xff\xfe").to_os_string();
            fake.script_dir(
                Some(key("journals")),
                Ok(vec![DirEntry::new(non_utf8, DirEntryType::Regular)]),
            );
            assert!(
                contained_subtree_has_any_entry(&fake, "journals")
                    .await
                    .unwrap(),
                "a non-directory entry concludes non-empty regardless of its name bytes"
            );
        }
    }

    #[tokio::test]
    async fn test_fake_emptiness_vanished_child_dir_is_empty_contribution() {
        let fake = RecordingFakeInspector::new();
        fake.script_dir(
            Some(key("repo-blobs")),
            Ok(vec![dir_entry("ghost", DirEntryType::Directory)]),
        );
        fake.script_dir(
            Some(key("repo-blobs/ghost")),
            Err(FsDirError::NotFound {
                path: Some("repo-blobs/ghost".to_string()),
            }),
        );
        assert!(
            !contained_subtree_has_any_entry(&fake, "repo-blobs")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_fake_emptiness_uninspectable_areas_fail_closed_no_false_empty() {
        // Unreadable subtree root.
        let fake = RecordingFakeInspector::new();
        fake.script_dir(
            Some(key("blobs")),
            Err(FsDirError::PermissionDenied {
                path: Some("blobs".to_string()),
                source: std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
            }),
        );
        let err = contained_subtree_has_any_entry(&fake, "blobs")
            .await
            .unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));

        // Symlinked subtree root rejected by contained resolution.
        let fake = RecordingFakeInspector::new();
        fake.script_dir(
            Some(key("blobs")),
            Err(FsDirError::ResolutionRejected {
                raw_os_error: libc::ELOOP,
                source: std::io::Error::from_raw_os_error(libc::ELOOP),
            }),
        );
        let err = contained_subtree_has_any_entry(&fake, "blobs")
            .await
            .unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));

        // Wrong-type subtree root (legacy Io).
        let fake = RecordingFakeInspector::new();
        fake.script_dir(
            Some(key("uploads")),
            Err(FsDirError::NotADirectory {
                path: Some("uploads".to_string()),
            }),
        );
        let err = contained_subtree_has_any_entry(&fake, "uploads")
            .await
            .unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));

        // Mid-walk I/O failure after an earlier empty observation.
        let fake = RecordingFakeInspector::new();
        fake.script_dir(
            Some(key("repos")),
            Ok(vec![
                dir_entry("empty_ok", DirEntryType::Directory),
                dir_entry("broken", DirEntryType::Directory),
            ]),
        );
        fake.script_dir(Some(key("repos/empty_ok")), Ok(vec![]));
        fake.script_dir(
            Some(key("repos/broken")),
            Err(FsDirError::Io {
                source: std::io::Error::other("disk error"),
            }),
        );
        let err = contained_subtree_has_any_entry(&fake, "repos")
            .await
            .unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));

        // Non-descendable directory names fail closed.
        let fake = RecordingFakeInspector::new();
        fake.script_dir(
            Some(key("meta")),
            Ok(vec![dir_entry("bad\\dir", DirEntryType::Directory)]),
        );
        let err = contained_subtree_has_any_entry(&fake, "meta")
            .await
            .unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::CorruptData));

        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let fake = RecordingFakeInspector::new();
            let non_utf8 = std::ffi::OsStr::from_bytes(b"dir_\xff").to_os_string();
            fake.script_dir(
                Some(key("meta")),
                Ok(vec![DirEntry::new(non_utf8, DirEntryType::Directory)]),
            );
            let err = contained_subtree_has_any_entry(&fake, "meta")
                .await
                .unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::CorruptData));
        }
    }

    // ========================================================================
    // Linux-gated real filesystem tests (production entry points)
    // ========================================================================

    #[cfg(target_os = "linux")]
    mod real_fs_tests {
        use super::*;
        use crate::storage::Storage;
        use crate::storage::fs::FsStorage;

        fn fixture_root() -> (tempfile::TempDir, std::path::PathBuf) {
            let fixture = tempfile::tempdir().expect("create tempdir");
            let root = fixture.path().join("storage_root");
            std::fs::create_dir_all(&root).expect("create storage root");
            (fixture, root)
        }

        fn write_with_mtime(path: &std::path::Path, secs: u64) {
            std::fs::write(path, b"x").unwrap();
            let file = std::fs::File::options().write(true).open(path).unwrap();
            let ts = UNIX_EPOCH + Duration::from_secs(secs);
            file.set_times(std::fs::FileTimes::new().set_modified(ts))
                .unwrap();
        }

        #[tokio::test]
        async fn test_real_repo_timestamps_max_selection_and_missing_cases() {
            let (_fixture, root) = fixture_root();
            let repo = root.join("repos").join("r1");
            std::fs::create_dir_all(repo.join("tags")).unwrap();
            std::fs::create_dir_all(repo.join("manifests")).unwrap();
            write_with_mtime(&repo.join("tags").join("older"), 1_600_000_000);
            write_with_mtime(&repo.join("tags").join("newer"), 1_700_000_000);
            write_with_mtime(&repo.join("manifests").join("m1"), 1_650_000_000);
            // Subdirectory inside tags does not contribute.
            std::fs::create_dir_all(repo.join("tags").join("subdir")).unwrap();

            let storage = FsStorage::new(root.clone(), 1024 * 1024);
            let ts = storage.repo_timestamps("r1").await.unwrap();
            assert_eq!(
                ts.last_tag_update,
                Some(UNIX_EPOCH + Duration::from_secs(1_700_000_000))
            );
            assert_eq!(
                ts.last_manifest_update,
                Some(UNIX_EPOCH + Duration::from_secs(1_650_000_000))
            );

            // Missing repo -> NotFound.
            let err = storage.repo_timestamps("absent").await.unwrap_err();
            assert!(matches!(err, StorageError::NotFound));

            // Repo without tags/manifests dirs -> None timestamps.
            std::fs::create_dir_all(root.join("repos").join("bare")).unwrap();
            let ts = storage.repo_timestamps("bare").await.unwrap();
            assert_eq!(ts.last_tag_update, None);
            assert_eq!(ts.last_manifest_update, None);

            // Empty tags dir -> None.
            std::fs::create_dir_all(root.join("repos").join("emptydirs").join("tags")).unwrap();
            let ts = storage.repo_timestamps("emptydirs").await.unwrap();
            assert_eq!(ts.last_tag_update, None);
        }

        #[tokio::test]
        async fn test_real_repo_timestamps_symlink_policy() {
            let (fixture, root) = fixture_root();
            let repo = root.join("repos").join("r1");
            std::fs::create_dir_all(repo.join("tags")).unwrap();
            write_with_mtime(&repo.join("tags").join("real_tag"), 1_600_000_000);

            // Symlinked file entry with a NEWER outside target: previously the
            // followed stat contributed it; contained inspection excludes it.
            let outside_file = fixture.path().join("outside_tag");
            write_with_mtime(&outside_file, 1_900_000_000);
            std::os::unix::fs::symlink(&outside_file, repo.join("tags").join("sym_tag")).unwrap();

            let storage = FsStorage::new(root.clone(), 1024 * 1024);
            let ts = storage.repo_timestamps("r1").await.unwrap();
            assert_eq!(
                ts.last_tag_update,
                Some(UNIX_EPOCH + Duration::from_secs(1_600_000_000)),
                "symlinked entries no longer contribute timestamps"
            );

            // Symlinked manifests directory is rejected instead of followed.
            let outside_dir = fixture.path().join("outside_manifests");
            std::fs::create_dir_all(&outside_dir).unwrap();
            std::os::unix::fs::symlink(&outside_dir, repo.join("manifests")).unwrap();
            let err = storage.repo_timestamps("r1").await.unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));

            // Symlinked repository directory is rejected instead of followed.
            let outside_repo = fixture.path().join("outside_repo");
            std::fs::create_dir_all(outside_repo.join("tags")).unwrap();
            std::os::unix::fs::symlink(&outside_repo, root.join("repos").join("symrepo")).unwrap();
            let err = storage.repo_timestamps("symrepo").await.unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
        }

        #[tokio::test]
        async fn test_real_repo_timestamps_pinned_root_replacement() {
            let (fixture, root) = fixture_root();
            let repo = root.join("repos").join("r1");
            std::fs::create_dir_all(repo.join("tags")).unwrap();
            write_with_mtime(&repo.join("tags").join("t"), 1_600_000_000);

            let storage = FsStorage::new(root.clone(), 1024 * 1024);
            let initial = storage.repo_timestamps("r1").await.unwrap();
            assert_eq!(
                initial.last_tag_update,
                Some(UNIX_EPOCH + Duration::from_secs(1_600_000_000))
            );

            // Replace the root pathname with a tree carrying a different mtime.
            let renamed = fixture.path().join("storage_root_old");
            std::fs::rename(&root, &renamed).unwrap();
            let new_repo = root.join("repos").join("r1");
            std::fs::create_dir_all(new_repo.join("tags")).unwrap();
            write_with_mtime(&new_repo.join("tags").join("t"), 1_800_000_000);

            let pinned = storage.repo_timestamps("r1").await.unwrap();
            assert_eq!(
                pinned.last_tag_update,
                Some(UNIX_EPOCH + Duration::from_secs(1_600_000_000)),
                "timestamps remain tied to the pinned original root"
            );
        }

        #[tokio::test]
        async fn test_real_repo_timestamps_wrong_type_tags_is_io() {
            let (_fixture, root) = fixture_root();
            let repo = root.join("repos").join("r1");
            std::fs::create_dir_all(&repo).unwrap();
            std::fs::write(repo.join("tags"), b"file, not dir").unwrap();

            let storage = FsStorage::new(root.clone(), 1024 * 1024);
            let err = storage.repo_timestamps("r1").await.unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
        }

        #[tokio::test]
        async fn test_real_is_storage_empty_areas_and_short_circuit() {
            let (_fixture, root) = fixture_root();
            let storage = FsStorage::new(root.clone(), 1024 * 1024);

            // Fully missing tree -> empty.
            assert!(storage.is_storage_empty().await.unwrap());

            // Nested empty directories only -> still empty.
            std::fs::create_dir_all(root.join("uploads").join("a").join("b")).unwrap();
            std::fs::create_dir_all(root.join("blobs").join("sha256")).unwrap();
            assert!(storage.is_storage_empty().await.unwrap());

            // One file in each area (checked one at a time) -> not empty.
            for area in [
                "blobs",
                "uploads",
                "quarantine",
                "repo-blobs",
                "repo-memberships",
                "repos",
                "journals",
            ] {
                let (_f2, root2) = fixture_root();
                let storage2 = FsStorage::new(root2.clone(), 1024 * 1024);
                let dir = root2.join(area).join("nested");
                std::fs::create_dir_all(&dir).unwrap();
                std::fs::write(dir.join("payload"), b"x").unwrap();
                assert!(
                    !storage2.is_storage_empty().await.unwrap(),
                    "a file under {area}/ must make storage non-empty"
                );
            }
        }

        #[tokio::test]
        async fn test_real_is_storage_empty_symlinked_area_fails_closed() {
            let (fixture, root) = fixture_root();
            let storage = FsStorage::new(root.clone(), 1024 * 1024);

            // blobs is a symlink to an outside directory holding data: the
            // ambient walk followed it; contained resolution rejects it, so an
            // uninspectable area can never produce a false empty result.
            let outside = fixture.path().join("outside_blobs");
            std::fs::create_dir_all(&outside).unwrap();
            std::fs::write(outside.join("data"), b"x").unwrap();
            std::os::unix::fs::symlink(&outside, root.join("blobs")).unwrap();

            let err = storage.is_storage_empty().await.unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));

            // The readiness-inspector port (consumed by runtime startup before
            // mark_membership_ready) sees the same fail-closed error.
            let wiring = crate::storage::ports::StorageWiring::from_backend(std::sync::Arc::new(
                FsStorage::new(root.clone(), 1024 * 1024),
            ));
            let err = wiring
                .readiness_inspector()
                .is_storage_empty()
                .await
                .unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
        }

        #[tokio::test]
        async fn test_real_is_storage_empty_symlink_entry_counts_as_entry() {
            let (fixture, root) = fixture_root();
            let storage = FsStorage::new(root.clone(), 1024 * 1024);

            // A symlink ENTRY inside an area is a non-directory dirent: it
            // counts as an entry (non-empty), exactly as before.
            std::fs::create_dir_all(root.join("journals")).unwrap();
            let target = fixture.path().join("target");
            std::fs::write(&target, b"x").unwrap();
            std::os::unix::fs::symlink(&target, root.join("journals").join("link")).unwrap();

            assert!(!storage.is_storage_empty().await.unwrap());
        }

        #[tokio::test]
        async fn test_real_is_storage_empty_pinned_root_replacement() {
            let (fixture, root) = fixture_root();
            let storage = FsStorage::new(root.clone(), 1024 * 1024);
            assert!(storage.is_storage_empty().await.unwrap());

            // Replace the root pathname with a populated tree: the pinned
            // reader still observes the original (empty) root.
            let renamed = fixture.path().join("storage_root_old");
            std::fs::rename(&root, &renamed).unwrap();
            std::fs::create_dir_all(root.join("blobs")).unwrap();
            std::fs::write(root.join("blobs").join("data"), b"x").unwrap();

            assert!(
                storage.is_storage_empty().await.unwrap(),
                "emptiness remains tied to the pinned original root"
            );
        }
    }
}
