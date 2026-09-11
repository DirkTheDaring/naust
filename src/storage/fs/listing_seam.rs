//! Test-only registry CAS listing integration seam using the committed
//! `storage-fs` directory enumeration (`FsMetadataReader::enumerate_dir`) and
//! contained file metadata inspection (`FsMetadataReader::inspect_file_metadata`).
//!
//! # Architectural Ownership Boundaries
//! - `storage-core`: Neutral storage contracts, [`storage_core::ObjectKey`],
//!   [`storage_core::ReadError`].
//! - `storage-fs`: Pinned root descriptor ownership, Linux `openat2` containment flags
//!   (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`), domain-free
//!   single-directory enumeration ([`storage_fs::DirEntry`], [`storage_fs::DirEntryType`],
//!   [`storage_fs::DirEnumerationLimits`], [`storage_fs::FsDirError`]), and single-file
//!   metadata inspection ([`storage_fs::FsFileMetadata`]).
//! - `registry-rust`: CAS namespace layout (`blobs/sha256/<2-char-prefix>/<64-char-hex>`),
//!   digest validation and normalization, lexical cursor comparisons, page-limit clamping [1, 1000],
//!   exact-full-page next-cursor calculation, error taxonomy translation ([`StorageError`]),
//!   and candidate metadata/version conversion ([`GcBlobCandidate`]).
//!
//! # Candidate Metadata & Version Completion
//! In legacy production listing (`FsStorage::list_cas_blobs_page`), candidate size and modification
//! time (`mtime`) were obtained by calling uncontained `tokio::fs::metadata(&path)` on reconstructed
//! pathnames, from which version was computed as `BlobObjectVersion(format!("{mtime_secs}:{size}"))`.
//! In that legacy path, `modified()` failure fell back to `std::time::UNIX_EPOCH`, and pre-epoch
//! duration conversion defaulted to zero for the version's seconds component.
//!
//! The completed seam integrates the extracted [`storage_fs::FsMetadataReader::inspect_file_metadata`]
//! API beneath the pinned root descriptor to obtain exact byte size and modification time for each
//! selected candidate surviving lexical cursor filtering. Registry policy then applies the legacy
//! conversion rules to construct full [`GcBlobCandidate`] instances:
//!
//! ```text
//! last_modified = inspected.modified().unwrap_or(std::time::UNIX_EPOCH)
//! size = inspected.size()
//! version_seconds = last_modified.duration_since(std::time::UNIX_EPOCH)
//!                                .unwrap_or_default()
//!                                .as_secs()
//! version = BlobObjectVersion(format!("{version_seconds}:{size}"))
//! ```
//!
//! # Containment & Concurrency Boundaries
//! - **Separate Observations**: Directory enumeration and subsequent metadata inspection are separate
//!   observations. Inspection resolves the current object at the selected key beneath the pinned root;
//!   it does not prove identity with an earlier directory entry or establish transactional snapshot isolation.
//! - **No Atomic Snapshot Under Mutation**: A single `fstat` result does not establish an atomic snapshot
//!   of all attributes under concurrent mutation, nor does it guarantee snapshot isolation across multiple operations.
//! - **Substituted Symlinks & Non-Regular Objects**: If a candidate is replaced with a symlink before inspection,
//!   contained `openat2` resolution rejects it with [`storage_fs::FsMetadataError::ResolutionRejected`]
//!   (mapped to [`StorageErrorKind::Io`]). If replaced with a non-regular object (e.g. directory or FIFO),
//!   inspection rejects it with [`storage_fs::FsMetadataError::UnsupportedObjectType`] (mapped to
//!   [`StorageErrorKind::CorruptData`]).
//! - **Regular-File Replacement**: If an enumerated blob file is unlinked and replaced with a different regular
//!   file of the same name before inspection, inspection succeeds and reports the replacement file's attributes
//!   at resolution time. This is a point-in-time observation, not an identity guarantee.
//! - **Disappeared Candidate Handling**: If a selected candidate disappears between enumeration and inspection,
//!   the seam fails the entire page with [`StorageErrorKind::Io`], preserving legacy per-candidate failure
//!   semantics without returning a partial successful page, silently skipping the entry, or falling back
//!   to uncontained pathname resolution.
//! - **Quality Gates**: All established quality gates (O-03, O-04, O-05, O-06, O-13, O-15, O-16, D-06)
//!   remain OPEN.
//!
//! # Execution Constraints
//! This module is strictly test-only (`#[cfg(test)]`). No production caller may invoke this seam.

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use storage_core::{ObjectKey, ReadError};
use storage_fs::{DirEntry, DirEntryType, DirEnumerationLimits, FsDirError, FsFileMetadata};

use crate::registry::digest::Digest;
use crate::storage::{
    BlobObjectVersion, GcBlobCandidate, GcBlobPage, GcCursor, StorageError, StorageErrorKind,
};

/// Narrow registry-owned test abstraction for descriptor-relative directory enumeration.
///
/// Enables exercising both the concrete [`storage_fs::FsMetadataReader`] and deterministic
/// recording fakes for failure injection.
#[async_trait]
pub trait CasDirEnumerator: Send + Sync {
    /// Enumerates a directory relative to the storage root descriptor.
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError>;
}

/// Narrow registry-owned test abstraction for descriptor-relative file metadata inspection.
///
/// Enables exercising both the concrete [`storage_fs::FsMetadataReader`] and deterministic
/// recording fakes for metadata inspection and failure injection.
#[async_trait]
pub trait CasMetadataInspector: Send + Sync {
    /// Inspects metadata of a regular file beneath the storage root descriptor.
    async fn inspect_file_metadata(&self, key: &ObjectKey) -> Result<FsFileMetadata, ReadError>;
}

/// Combined trait for sources providing both directory enumeration and metadata inspection.
pub trait CasListingSource: CasDirEnumerator + CasMetadataInspector {}
impl<T: CasDirEnumerator + CasMetadataInspector + ?Sized> CasListingSource for T {}

#[async_trait]
impl CasDirEnumerator for storage_fs::FsMetadataReader {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError> {
        self.enumerate_dir(target, limits).await
    }
}

#[async_trait]
impl CasMetadataInspector for storage_fs::FsMetadataReader {
    async fn inspect_file_metadata(&self, key: &ObjectKey) -> Result<FsFileMetadata, ReadError> {
        self.inspect_file_metadata(key).await
    }
}

#[async_trait]
impl<T: CasDirEnumerator + ?Sized> CasDirEnumerator for &T {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError> {
        (**self).enumerate_dir(target, limits).await
    }
}

#[async_trait]
impl<T: CasMetadataInspector + ?Sized> CasMetadataInspector for &T {
    async fn inspect_file_metadata(&self, key: &ObjectKey) -> Result<FsFileMetadata, ReadError> {
        (**self).inspect_file_metadata(key).await
    }
}

#[async_trait]
impl<T: CasDirEnumerator + ?Sized> CasDirEnumerator for Arc<T> {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError> {
        (**self).enumerate_dir(target, limits).await
    }
}

#[async_trait]
impl<T: CasMetadataInspector + ?Sized> CasMetadataInspector for Arc<T> {
    async fn inspect_file_metadata(&self, key: &ObjectKey) -> Result<FsFileMetadata, ReadError> {
        (**self).inspect_file_metadata(key).await
    }
}

/// Translates strongly typed [`FsDirError`] outcomes into the registry [`StorageError`] taxonomy.
pub(crate) fn translate_dir_error(err: FsDirError) -> StorageError {
    match err {
        FsDirError::NotFound { .. } => StorageError::NotFound,
        FsDirError::NotADirectory { ref path } => {
            StorageError::corrupt_data(format!("target path is not a directory: {path:?}"))
        }
        FsDirError::PermissionDenied { ref source, .. } => {
            StorageError::permission_denied(source.to_string())
        }
        FsDirError::ResolutionRejected { ref source, .. } => StorageError::io(source.to_string()),
        FsDirError::SyscallUnsupported(ref source) => StorageError::configuration(format!(
            "openat2 is unavailable in this execution environment: {source}"
        )),
        FsDirError::PlatformUnsupported => StorageError::configuration(
            "platform unsupported: descriptor-relative containment requires Linux openat2",
        ),
        FsDirError::LimitExceeded { reason } => {
            StorageError::backend(format!("enumeration resource limit exceeded: {reason:?}"))
        }
        FsDirError::EntryDisappeared { ref name } => StorageError::io(format!(
            "directory entry disappeared during type inspection: {name:?}"
        )),
        FsDirError::Io { ref source } => StorageError::io(source.to_string()),
        FsDirError::RuntimeMissing(ref err) => {
            StorageError::backend(format!("tokio runtime missing: {err}"))
        }
        FsDirError::TaskJoinFailed(ref err) => {
            StorageError::backend(format!("blocking enumeration task join failed: {err}"))
        }
        other => StorageError::backend(format!("unexpected directory enumeration error: {other}")),
    }
}

/// Translates strongly typed [`storage_core::ReadError`] inspection outcomes into [`StorageError`].
///
/// Error classification is driven strictly by typed error variants and typed source downcasts;
/// error message text is never parsed to determine the category.
pub(crate) fn translate_inspect_error(err: ReadError) -> StorageError {
    match err {
        ReadError::NotFound { key, .. } => {
            // A candidate entry discovered during directory enumeration that has disappeared
            // before metadata inspection fails the page with StorageErrorKind::Io, matching
            // legacy FsStorage::list_cas_blobs_page per-candidate metadata failure semantics.
            StorageError::io(format!(
                "candidate blob disappeared before metadata inspection: {key}"
            ))
        }
        ReadError::PermissionDenied { key, source, .. } => {
            if let Some(src) = source {
                if let Some(io_err) = src.downcast_ref::<std::io::Error>() {
                    return StorageError::io(io_err.to_string());
                }
                StorageError::io(src.to_string())
            } else {
                StorageError::io(format!(
                    "permission denied inspecting candidate blob: {key}"
                ))
            }
        }
        ReadError::Backend {
            message, source, ..
        } => {
            if let Some(src) = source {
                if let Some(fs_err) = src.downcast_ref::<storage_fs::FsMetadataError>() {
                    match fs_err {
                        storage_fs::FsMetadataError::ResolutionRejected { source, .. } => {
                            StorageError::io(source.to_string())
                        }
                        storage_fs::FsMetadataError::UnsupportedObjectType { mode } => {
                            StorageError::corrupt_data(format!(
                                "unsupported object type (mode: {mode:#o})"
                            ))
                        }
                        storage_fs::FsMetadataError::SyscallUnsupported(io_err) => {
                            StorageError::configuration(format!(
                                "openat2 is unavailable in this execution environment: {io_err}"
                            ))
                        }
                        storage_fs::FsMetadataError::PlatformUnsupported => {
                            StorageError::configuration(
                                "platform unsupported: descriptor-relative containment requires Linux openat2",
                            )
                        }
                        storage_fs::FsMetadataError::StatFailed { stage, source } => {
                            StorageError::io(format!("failed to stat {stage} descriptor: {source}"))
                        }
                        storage_fs::FsMetadataError::InvalidMetadata { message } => {
                            StorageError::corrupt_data(format!("invalid metadata: {message}"))
                        }
                        storage_fs::FsMetadataError::RuntimeMissing(err) => {
                            StorageError::backend(format!("tokio runtime missing: {err}"))
                        }
                        storage_fs::FsMetadataError::TaskJoinFailed(err) => StorageError::backend(
                            format!("blocking metadata task join failed: {err}"),
                        ),
                        other => StorageError::io(other.to_string()),
                    }
                } else if let Some(io_err) = src.downcast_ref::<std::io::Error>() {
                    if io_err.raw_os_error() == Some(libc::ENOTDIR) {
                        StorageError::corrupt_data(io_err.to_string())
                    } else {
                        StorageError::io(io_err.to_string())
                    }
                } else {
                    StorageError::io(src.to_string())
                }
            } else {
                StorageError::io(message)
            }
        }
        _ => StorageError::io("unknown storage metadata read failure"),
    }
}

/// Applies registry-owned metadata and version conversion rules to produce a [`GcBlobCandidate`].
///
/// # Conversion Rules
/// - `size`: Preserves exact inspected `u64` file size.
/// - `last_modified`: Preserves inspected `SystemTime` if present; falls back to [`std::time::UNIX_EPOCH`]
///   if `inspected.modified()` is `None`. Pre-epoch `SystemTime` values are preserved byte-for-byte.
/// - `version_seconds`: Duration since `UNIX_EPOCH` in whole seconds, defaulting to 0 for pre-epoch timestamps.
/// - `version`: Formatted as `BlobObjectVersion(format!("{version_seconds}:{size}"))`.
pub(crate) fn convert_candidate(digest: Digest, inspected: &FsFileMetadata) -> GcBlobCandidate {
    let last_modified = inspected.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    let size = inspected.size();
    let version_seconds = last_modified
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let version = BlobObjectVersion(format!("{version_seconds}:{size}"));

    GcBlobCandidate {
        digest,
        size,
        last_modified,
        version,
    }
}

/// Registry-owned resource limits for CAS listing enumeration.
///
/// Encapsulates separate operational bounds for the CAS root directory (`blobs/sha256`)
/// and individual shard directories (`blobs/sha256/<p2>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsListingBudgets {
    /// Budget applied when enumerating the CAS root directory (`blobs/sha256`).
    pub root: DirEnumerationLimits,
    /// Budget applied when enumerating individual shard directories (`blobs/sha256/<p2>`).
    pub shard: DirEnumerationLimits,
}

impl FsListingBudgets {
    /// Default maximum entries permitted in the CAS root directory (`blobs/sha256`).
    pub const DEFAULT_ROOT_MAX_ENTRIES: usize = 512;
    /// Default maximum total raw filename bytes permitted in the CAS root directory.
    pub const DEFAULT_ROOT_MAX_NAME_BYTES: usize = 16 * 1024; // 16,384

    /// Default maximum entries permitted in a single CAS shard directory (`blobs/sha256/<p2>`).
    pub const DEFAULT_SHARD_MAX_ENTRIES: usize = 100_000;
    /// Default maximum total raw filename bytes permitted in a single CAS shard directory.
    pub const DEFAULT_SHARD_MAX_NAME_BYTES: usize = 8 * 1024 * 1024; // 8,388,608

    /// Constructs a new budget pair with distinct root and shard enumeration limits.
    pub fn new(root: DirEnumerationLimits, shard: DirEnumerationLimits) -> Self {
        Self { root, shard }
    }
}

impl Default for FsListingBudgets {
    fn default() -> Self {
        Self {
            root: DirEnumerationLimits::new(
                Self::DEFAULT_ROOT_MAX_ENTRIES,
                Self::DEFAULT_ROOT_MAX_NAME_BYTES,
            ),
            shard: DirEnumerationLimits::new(
                Self::DEFAULT_SHARD_MAX_ENTRIES,
                Self::DEFAULT_SHARD_MAX_NAME_BYTES,
            ),
        }
    }
}

/// Executes paginated CAS blob listing through a directory enumerator and metadata inspector seam.
///
/// # Arguments
/// - `source`: Combined directory enumerator and metadata inspector (e.g. `FsMetadataReader` or fake).
/// - `cursor`: Optional continuation cursor from a preceding page.
/// - `limit`: Requested candidate limit, clamped to `[1, 1000]`.
/// - `budgets`: Registry-owned resource limits containing distinct root and shard enumeration bounds.
///
/// # Invariants Enforced
/// - Target directory is `blobs/sha256` relative to the root descriptor.
/// - If `blobs/sha256` is `NotFound`, returns `Ok(empty page)` (absent CAS repository state).
/// - Non-NotFound errors on `blobs/sha256` (e.g. `NotADirectory`, `PermissionDenied`) fail closed.
/// - Prefix entries in `blobs/sha256` must be directories with 2-char hex names; otherwise `CorruptData`.
/// - Entries inside shard directories must be regular files with 64-char hex names matching the shard prefix;
///   otherwise `CorruptData`.
/// - Shards and shard entries are sorted ASCII-lexicographically.
/// - Cursors are evaluated via lexical comparison (`digest_str <= cursor`).
/// - Candidates excluded by the cursor or beyond the page limit are NEVER inspected.
/// - Metadata inspection uses descriptor-relative `inspect_file_metadata` beneath the pinned root.
/// - Disappeared candidates fail the page immediately with `StorageErrorKind::Io` without partial results.
/// - Substituted symlinks or non-regular objects fail closed with typed errors.
/// - Exact-full-page returns `Some(next_cursor)`; subsequent terminal page returns `None`.
/// - Budget exhaustion fails closed immediately without partial results.
pub async fn list_cas_blobs_page_seam(
    source: &(impl CasListingSource + ?Sized),
    cursor: Option<&GcCursor>,
    limit: usize,
    budgets: FsListingBudgets,
) -> Result<GcBlobPage, StorageError> {
    let max_limit = 1000;
    let limit = limit.min(max_limit).max(1);
    let cursor_str = cursor.map(|c| c.0.as_str());

    let cas_root_key = ObjectKey::parse("blobs/sha256")
        .map_err(|e| StorageError::internal(StorageErrorKind::InternalInvariant, e.to_string()))?;

    let root_entries = match source
        .enumerate_dir(Some(&cas_root_key), budgets.root)
        .await
    {
        Ok(entries) => entries,
        Err(FsDirError::NotFound { .. }) => {
            return Ok(GcBlobPage {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        Err(err) => return Err(translate_dir_error(err)),
    };

    let mut prefix_dirs = Vec::new();
    for ent in root_entries {
        let name_str = ent.name().to_str().ok_or_else(|| {
            StorageError::corrupt_data(format!(
                "malformed non-utf8 entry in CAS root: {:?}",
                ent.name()
            ))
        })?;

        if ent.file_type() != DirEntryType::Directory {
            return Err(StorageError::corrupt_data(format!(
                "malformed non-directory entry in CAS prefix directory root: {name_str}"
            )));
        }

        if name_str.len() != 2 || !name_str.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(StorageError::corrupt_data(format!(
                "malformed 2-char prefix directory name in CAS root: {name_str}"
            )));
        }
        prefix_dirs.push(name_str.to_ascii_lowercase());
    }
    prefix_dirs.sort();

    let mut candidates = Vec::new();
    let mut next_cursor = None;

    'outer: for p2 in prefix_dirs {
        let shard_key_str = format!("blobs/sha256/{p2}");
        let shard_key = ObjectKey::parse(&shard_key_str).map_err(|e| {
            StorageError::internal(StorageErrorKind::InternalInvariant, e.to_string())
        })?;

        let shard_entries = source
            .enumerate_dir(Some(&shard_key), budgets.shard)
            .await
            .map_err(translate_dir_error)?;

        let mut file_names = Vec::new();
        for ent in shard_entries {
            let name_str = ent.name().to_str().ok_or_else(|| {
                StorageError::corrupt_data(format!(
                    "malformed non-utf8 blob filename in CAS shard directory {p2}: {:?}",
                    ent.name()
                ))
            })?;

            if ent.file_type() != DirEntryType::Regular {
                return Err(StorageError::corrupt_data(format!(
                    "malformed non-file entry in CAS shard directory {p2}: {name_str}"
                )));
            }

            if name_str.len() != 64
                || !name_str.to_ascii_lowercase().starts_with(&p2)
                || !name_str.chars().all(|c| c.is_ascii_hexdigit())
            {
                return Err(StorageError::corrupt_data(format!(
                    "malformed blob file name in CAS shard {p2}: {name_str}"
                )));
            }
            file_names.push(name_str.to_ascii_lowercase());
        }
        file_names.sort();

        for hex in file_names {
            let digest_str = format!("sha256:{hex}");
            if let Some(c) = cursor_str {
                if digest_str.as_str() <= c {
                    continue;
                }
            }

            let digest = Digest::parse(&digest_str).map_err(|e| {
                StorageError::internal_invariant(format!(
                    "failed to parse digest from hex {hex}: {e}"
                ))
            })?;

            let blob_key_str = format!("blobs/sha256/{p2}/{hex}");
            let blob_key = ObjectKey::parse(&blob_key_str).map_err(|e| {
                StorageError::internal(StorageErrorKind::InternalInvariant, e.to_string())
            })?;

            let inspected = source
                .inspect_file_metadata(&blob_key)
                .await
                .map_err(translate_inspect_error)?;

            let candidate = convert_candidate(digest, &inspected);
            candidates.push(candidate);

            if candidates.len() >= limit {
                next_cursor = Some(GcCursor(digest_str));
                break 'outer;
            }
        }
    }

    Ok(GcBlobPage {
        items: candidates,
        next_cursor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;
    use std::time::Duration;

    use crate::blob_gc::policy::{AgeEligibility, check_candidate_age};
    use crate::blob_gc::traverser::{CasBlobTraverser, GcPaginationError};

    /// Recording fake enumerator and metadata inspector for deterministic call order,
    /// inspection verification, and failure injection.
    struct RecordingFakeDirEnumerator {
        calls: Mutex<Vec<(Option<ObjectKey>, DirEnumerationLimits)>>,
        responses: Mutex<HashMap<Option<ObjectKey>, VecDeque<Result<Vec<DirEntry>, FsDirError>>>>,
        inspect_calls: Mutex<Vec<ObjectKey>>,
        inspect_responses: Mutex<HashMap<ObjectKey, VecDeque<Result<FsFileMetadata, ReadError>>>>,
        default_metadata: Mutex<Option<FsFileMetadata>>,
    }

    impl RecordingFakeDirEnumerator {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                responses: Mutex::new(HashMap::new()),
                inspect_calls: Mutex::new(Vec::new()),
                inspect_responses: Mutex::new(HashMap::new()),
                default_metadata: Mutex::new(None),
            }
        }

        fn script(&self, target: Option<ObjectKey>, response: Result<Vec<DirEntry>, FsDirError>) {
            self.responses
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

        fn with_default_metadata(&self, metadata: FsFileMetadata) {
            *self.default_metadata.lock().unwrap() = Some(metadata);
        }

        fn calls(&self) -> Vec<(Option<ObjectKey>, DirEnumerationLimits)> {
            self.calls.lock().unwrap().clone()
        }

        fn called_targets(&self) -> Vec<Option<ObjectKey>> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .map(|(t, _)| t.clone())
                .collect()
        }

        fn inspect_calls(&self) -> Vec<ObjectKey> {
            self.inspect_calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl CasDirEnumerator for RecordingFakeDirEnumerator {
        async fn enumerate_dir(
            &self,
            target: Option<&ObjectKey>,
            limits: DirEnumerationLimits,
        ) -> Result<Vec<DirEntry>, FsDirError> {
            self.calls.lock().unwrap().push((target.cloned(), limits));
            let mut responses = self.responses.lock().unwrap();
            let queue = responses.get_mut(&target.cloned()).unwrap_or_else(|| {
                panic!("unexpected call to RecordingFakeDirEnumerator::enumerate_dir with target: {target:?}")
            });
            queue
                .pop_front()
                .unwrap_or_else(|| panic!("no more scripted dir responses for target: {target:?}"))
        }
    }

    #[async_trait]
    impl CasMetadataInspector for RecordingFakeDirEnumerator {
        async fn inspect_file_metadata(
            &self,
            key: &ObjectKey,
        ) -> Result<FsFileMetadata, ReadError> {
            self.inspect_calls.lock().unwrap().push(key.clone());
            let mut responses = self.inspect_responses.lock().unwrap();
            if let Some(queue) = responses.get_mut(key) {
                if let Some(resp) = queue.pop_front() {
                    return resp;
                }
            }
            if let Some(m) = *self.default_metadata.lock().unwrap() {
                return Ok(m);
            }
            panic!(
                "unexpected call to RecordingFakeDirEnumerator::inspect_file_metadata with key: {key}"
            );
        }
    }

    fn default_test_budget() -> FsListingBudgets {
        FsListingBudgets::default()
    }

    #[test]
    fn test_default_listing_budget_values() {
        let defaults = FsListingBudgets::default();
        assert_eq!(defaults.root.max_entries(), 512);
        assert_eq!(defaults.root.max_total_name_bytes(), 16_384);
        assert_eq!(defaults.shard.max_entries(), 100_000);
        assert_eq!(defaults.shard.max_total_name_bytes(), 8_388_608);
        assert_eq!(
            defaults.root.max_entries(),
            FsListingBudgets::DEFAULT_ROOT_MAX_ENTRIES
        );
        assert_eq!(
            defaults.root.max_total_name_bytes(),
            FsListingBudgets::DEFAULT_ROOT_MAX_NAME_BYTES
        );
        assert_eq!(
            defaults.shard.max_entries(),
            FsListingBudgets::DEFAULT_SHARD_MAX_ENTRIES
        );
        assert_eq!(
            defaults.shard.max_total_name_bytes(),
            FsListingBudgets::DEFAULT_SHARD_MAX_NAME_BYTES
        );
    }

    fn cas_key(path: &str) -> ObjectKey {
        ObjectKey::parse(path).expect("valid object key")
    }

    // ========================================================================
    // Category A: Recording Fake Tests (Call Sequences, Failures & Suppression)
    // ========================================================================

    #[tokio::test]
    async fn test_fake_empty_root_not_found_returns_empty_page() {
        let fake = RecordingFakeDirEnumerator::new();
        let root_key = cas_key("blobs/sha256");
        fake.script(
            Some(root_key.clone()),
            Err(FsDirError::NotFound {
                path: Some("blobs/sha256".into()),
            }),
        );

        let page = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
            .await
            .expect("NotFound on CAS root must return empty page");

        assert!(page.items.is_empty());
        assert_eq!(page.next_cursor, None);
        assert_eq!(fake.called_targets(), vec![Some(root_key)]);
        assert!(fake.inspect_calls().is_empty());
    }

    #[tokio::test]
    async fn test_fake_empty_root_empty_entries_returns_empty_page() {
        let fake = RecordingFakeDirEnumerator::new();
        let root_key = cas_key("blobs/sha256");
        fake.script(Some(root_key.clone()), Ok(vec![]));

        let page = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
            .await
            .expect("empty CAS root returns empty page");

        assert!(page.items.is_empty());
        assert_eq!(page.next_cursor, None);
        assert_eq!(fake.called_targets(), vec![Some(root_key)]);
        assert!(fake.inspect_calls().is_empty());
    }

    #[tokio::test]
    async fn test_fake_suppression_on_root_failure() {
        let fake = RecordingFakeDirEnumerator::new();
        let root_key = cas_key("blobs/sha256");
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");

        fake.script(
            Some(root_key.clone()),
            Err(FsDirError::PermissionDenied {
                path: Some("blobs/sha256".into()),
                source: io_err,
            }),
        );

        let err = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
            .await
            .expect_err("root permission denied must fail closed");

        match err {
            StorageError::Internal { kind, message } => {
                assert_eq!(kind, StorageErrorKind::PermissionDenied);
                assert!(message.contains("access denied"));
            }
            other => panic!("expected StorageError::Internal(PermissionDenied), got {other:?}"),
        }
        assert_eq!(fake.called_targets(), vec![Some(root_key)]);
        assert!(fake.inspect_calls().is_empty());
    }

    #[tokio::test]
    async fn test_fake_suppression_on_shard_failure() {
        let fake = RecordingFakeDirEnumerator::new();
        let root_key = cas_key("blobs/sha256");
        let shard_0a = cas_key("blobs/sha256/0a");

        fake.script(
            Some(root_key.clone()),
            Ok(vec![
                DirEntry::new("0a".into(), DirEntryType::Directory),
                DirEntry::new("0b".into(), DirEntryType::Directory),
            ]),
        );

        let io_err = std::io::Error::new(std::io::ErrorKind::Other, "disk read fault");
        fake.script(
            Some(shard_0a.clone()),
            Err(FsDirError::Io { source: io_err }),
        );

        let err = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
            .await
            .expect_err("shard read fault must fail closed");

        match err {
            StorageError::Internal { kind, message } => {
                assert_eq!(kind, StorageErrorKind::Io);
                assert!(message.contains("disk read fault"));
            }
            other => panic!("expected StorageError::Internal(Io), got {other:?}"),
        }
        assert_eq!(fake.called_targets(), vec![Some(root_key), Some(shard_0a)]);
        assert!(fake.inspect_calls().is_empty());
    }

    #[tokio::test]
    async fn test_fake_typed_dir_error_mappings() {
        // 1. NotADirectory -> CorruptData
        {
            let fake = RecordingFakeDirEnumerator::new();
            let root_key = cas_key("blobs/sha256");
            fake.script(
                Some(root_key.clone()),
                Err(FsDirError::NotADirectory {
                    path: Some("blobs/sha256".into()),
                }),
            );
            let err = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::CorruptData);
                    assert!(message.contains("not a directory"));
                }
                other => panic!("expected CorruptData, got {other:?}"),
            }
        }

        // 2. ResolutionRejected -> Io
        {
            let fake = RecordingFakeDirEnumerator::new();
            let root_key = cas_key("blobs/sha256");
            fake.script(
                Some(root_key.clone()),
                Err(FsDirError::ResolutionRejected {
                    raw_os_error: libc::ELOOP,
                    source: std::io::Error::from_raw_os_error(libc::ELOOP),
                }),
            );
            let err = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Io);
                    assert!(
                        message
                            .contains(&std::io::Error::from_raw_os_error(libc::ELOOP).to_string())
                    );
                }
                other => panic!("expected Io, got {other:?}"),
            }
        }

        // 3. LimitExceeded -> Backend
        {
            let fake = RecordingFakeDirEnumerator::new();
            let root_key = cas_key("blobs/sha256");
            fake.script(
                Some(root_key.clone()),
                Err(FsDirError::LimitExceeded {
                    reason: storage_fs::LimitExceededReason::MaxEntries(5),
                }),
            );
            let err = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Backend);
                    assert!(message.contains("enumeration resource limit exceeded"));
                }
                other => panic!("expected Backend, got {other:?}"),
            }
        }

        // 4. EntryDisappeared -> Io
        {
            let fake = RecordingFakeDirEnumerator::new();
            let root_key = cas_key("blobs/sha256");
            fake.script(
                Some(root_key.clone()),
                Err(FsDirError::EntryDisappeared {
                    name: "disappeared_entry".into(),
                }),
            );
            let err = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Io);
                    assert!(message.contains("directory entry disappeared"));
                }
                other => panic!("expected Io, got {other:?}"),
            }
        }

        // 5. SyscallUnsupported -> Configuration
        {
            let fake = RecordingFakeDirEnumerator::new();
            let root_key = cas_key("blobs/sha256");
            fake.script(
                Some(root_key.clone()),
                Err(FsDirError::SyscallUnsupported(
                    std::io::Error::from_raw_os_error(libc::ENOSYS),
                )),
            );
            let err = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Configuration);
                    assert!(message.contains("openat2 is unavailable"));
                }
                other => panic!("expected Configuration, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn test_distinct_root_and_shard_budgets_forwarded_to_respective_calls() {
        let fake = RecordingFakeDirEnumerator::new();
        let root_key = cas_key("blobs/sha256");
        let shard_key = cas_key("blobs/sha256/0a");

        fake.script(
            Some(root_key.clone()),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(Some(shard_key.clone()), Ok(vec![]));

        // Use deliberately distinct entry and byte limits so accidentally reusing
        // either budget fails the test.
        let root_limits = DirEnumerationLimits::new(111, 222);
        let shard_limits = DirEnumerationLimits::new(333, 444);
        let budgets = FsListingBudgets::new(root_limits, shard_limits);

        let _ = list_cas_blobs_page_seam(&fake, None, 10, budgets)
            .await
            .unwrap();

        let calls = fake.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], (Some(root_key), root_limits));
        assert_eq!(calls[1], (Some(shard_key), shard_limits));
    }

    #[tokio::test]
    async fn test_root_limit_failure_prevents_shard_enumeration_and_candidate_inspection() {
        let fake = RecordingFakeDirEnumerator::new();
        let root_key = cas_key("blobs/sha256");

        fake.script(
            Some(root_key),
            Err(FsDirError::LimitExceeded {
                reason: storage_fs::LimitExceededReason::MaxEntries(512),
            }),
        );
        // Do NOT script any shard or inspect responses: if called, fake will panic.

        let res = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget()).await;
        assert!(
            res.is_err(),
            "root limit failure must fail closed immediately"
        );
        let err = res.unwrap_err();
        match err {
            StorageError::Internal { kind, message } => {
                assert_eq!(kind, StorageErrorKind::Backend);
                assert!(message.contains("enumeration resource limit exceeded"));
            }
            other => panic!("expected Backend error, got {other:?}"),
        }

        // Verify root call occurred, but 0 shard calls and 0 candidate inspections occurred
        let calls = fake.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(fake.inspect_calls().len(), 0);
    }

    #[tokio::test]
    async fn test_shard_limit_failure_preserves_typed_error_and_whole_page_failure() {
        let fake = RecordingFakeDirEnumerator::new();
        let root_key = cas_key("blobs/sha256");
        let shard_key = cas_key("blobs/sha256/0a");

        fake.script(
            Some(root_key),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(
            Some(shard_key),
            Err(FsDirError::LimitExceeded {
                reason: storage_fs::LimitExceededReason::MaxTotalNameBytes(8_388_608),
            }),
        );
        // Do NOT script inspect responses: if called, fake will panic.

        let res = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget()).await;
        assert!(
            res.is_err(),
            "shard limit failure must fail the entire page"
        );
        let err = res.unwrap_err();
        match err {
            StorageError::Internal { kind, message } => {
                assert_eq!(kind, StorageErrorKind::Backend);
                assert!(message.contains("enumeration resource limit exceeded"));
            }
            other => panic!("expected Backend error, got {other:?}"),
        }

        assert_eq!(fake.calls().len(), 2);
        assert_eq!(fake.inspect_calls().len(), 0);
    }

    #[tokio::test]
    async fn test_success_when_root_and_shard_require_different_budgets() {
        let fake = RecordingFakeDirEnumerator::new();
        let root_key = cas_key("blobs/sha256");
        let shard_key = cas_key("blobs/sha256/0a");

        fake.script(
            Some(root_key),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );

        // Shard contains 10 entries.
        let mut shard_entries = Vec::new();
        for i in 0..10 {
            let hex = format!("0a{:062x}", i);
            shard_entries.push(DirEntry::new(hex.into(), DirEntryType::Regular));
        }
        fake.script(Some(shard_key), Ok(shard_entries));
        fake.with_default_metadata(FsFileMetadata::new(128, Some(SystemTime::UNIX_EPOCH)));

        // Configure asymmetric budgets:
        // Root has max_entries: 5 (less than shard's 10 entries)
        // Shard has max_entries: 20 (enough for shard's 10 entries)
        let root_limits = DirEnumerationLimits::new(5, 500);
        let shard_limits = DirEnumerationLimits::new(20, 2000);
        let asymmetric_budgets = FsListingBudgets::new(root_limits, shard_limits);

        let page = list_cas_blobs_page_seam(&fake, None, 10, asymmetric_budgets)
            .await
            .expect("page succeeds when root and shard require different budgets");

        assert_eq!(page.items.len(), 10);
        assert_eq!(fake.calls().len(), 2);
        assert_eq!(fake.calls()[0].1, root_limits);
        assert_eq!(fake.calls()[1].1, shard_limits);
        assert_eq!(fake.inspect_calls().len(), 10);
    }

    #[tokio::test]
    async fn test_fake_corrupt_entry_types_and_names() {
        // Regular file inside CAS root
        {
            let fake = RecordingFakeDirEnumerator::new();
            let root_key = cas_key("blobs/sha256");
            fake.script(
                Some(root_key.clone()),
                Ok(vec![DirEntry::new("0a".into(), DirEntryType::Regular)]),
            );
            let err = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::CorruptData);
                    assert!(
                        message
                            .contains("malformed non-directory entry in CAS prefix directory root")
                    );
                }
                other => panic!("expected CorruptData, got {other:?}"),
            }
        }

        // Malformed shard name (3 characters)
        {
            let fake = RecordingFakeDirEnumerator::new();
            let root_key = cas_key("blobs/sha256");
            fake.script(
                Some(root_key.clone()),
                Ok(vec![DirEntry::new("0aa".into(), DirEntryType::Directory)]),
            );
            let err = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::CorruptData);
                    assert!(message.contains("malformed 2-char prefix directory name"));
                }
                other => panic!("expected CorruptData, got {other:?}"),
            }
        }

        // Non-regular file inside shard
        {
            let fake = RecordingFakeDirEnumerator::new();
            let root_key = cas_key("blobs/sha256");
            let shard_key = cas_key("blobs/sha256/0a");
            fake.script(
                Some(root_key),
                Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
            );
            fake.script(
                Some(shard_key),
                Ok(vec![DirEntry::new(
                    "nested_dir".into(),
                    DirEntryType::Directory,
                )]),
            );
            let err = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::CorruptData);
                    assert!(message.contains("malformed non-file entry in CAS shard directory"));
                }
                other => panic!("expected CorruptData, got {other:?}"),
            }
        }

        // Malformed blob file name inside shard (not starting with shard prefix)
        {
            let fake = RecordingFakeDirEnumerator::new();
            let root_key = cas_key("blobs/sha256");
            let shard_key = cas_key("blobs/sha256/0a");
            fake.script(
                Some(root_key),
                Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
            );
            fake.script(
                Some(shard_key),
                Ok(vec![DirEntry::new(
                    "1b00000000000000000000000000000000000000000000000000000000000001".into(),
                    DirEntryType::Regular,
                )]),
            );
            let err = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
                .await
                .unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::CorruptData);
                    assert!(message.contains("malformed blob file name in CAS shard"));
                }
                other => panic!("expected CorruptData, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn test_fake_ordered_pagination_and_boundaries() {
        let fake = RecordingFakeDirEnumerator::new();
        fake.with_default_metadata(FsFileMetadata::new(
            128,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(50)),
        ));

        let root_key = cas_key("blobs/sha256");
        let shard_0a = cas_key("blobs/sha256/0a");
        let shard_1b = cas_key("blobs/sha256/1b");

        let hex_0a1 = "0a00000000000000000000000000000000000000000000000000000000000001";
        let hex_0a2 = "0a00000000000000000000000000000000000000000000000000000000000002";
        let hex_1b1 = "1b00000000000000000000000000000000000000000000000000000000000001";

        // Script Page 1 (limit 2): returns 0a1 and 0a2
        fake.script(
            Some(root_key.clone()),
            Ok(vec![
                DirEntry::new("1b".into(), DirEntryType::Directory),
                DirEntry::new("0a".into(), DirEntryType::Directory),
            ]),
        );
        fake.script(
            Some(shard_0a.clone()),
            Ok(vec![
                DirEntry::new(hex_0a2.into(), DirEntryType::Regular),
                DirEntry::new(hex_0a1.into(), DirEntryType::Regular),
            ]),
        );

        let page1 = list_cas_blobs_page_seam(&fake, None, 2, default_test_budget())
            .await
            .unwrap();
        assert_eq!(page1.items.len(), 2);
        assert_eq!(page1.items[0].digest.hex(), hex_0a1);
        assert_eq!(page1.items[0].size, 128);
        assert_eq!(page1.items[0].version.0, "50:128");
        assert_eq!(page1.items[1].digest.hex(), hex_0a2);
        assert_eq!(page1.items[1].size, 128);
        assert_eq!(page1.items[1].version.0, "50:128");
        let c1 = format!("sha256:{hex_0a2}");
        assert_eq!(
            page1.next_cursor.as_ref().map(|c| c.0.as_str()),
            Some(c1.as_str())
        );

        // Script Page 2 (cursor c1, limit 2): returns 1b1
        fake.script(
            Some(root_key.clone()),
            Ok(vec![
                DirEntry::new("0a".into(), DirEntryType::Directory),
                DirEntry::new("1b".into(), DirEntryType::Directory),
            ]),
        );
        fake.script(
            Some(shard_0a.clone()),
            Ok(vec![
                DirEntry::new(hex_0a1.into(), DirEntryType::Regular),
                DirEntry::new(hex_0a2.into(), DirEntryType::Regular),
            ]),
        );
        fake.script(
            Some(shard_1b.clone()),
            Ok(vec![DirEntry::new(hex_1b1.into(), DirEntryType::Regular)]),
        );

        let page2 =
            list_cas_blobs_page_seam(&fake, page1.next_cursor.as_ref(), 2, default_test_budget())
                .await
                .unwrap();
        assert_eq!(page2.items.len(), 1);
        assert_eq!(page2.items[0].digest.hex(), hex_1b1);
        assert_eq!(page2.items[0].size, 128);
        assert_eq!(page2.items[0].version.0, "50:128");
        assert_eq!(page2.next_cursor, None);
    }

    #[tokio::test]
    async fn test_fake_limit_clamping_upper_bound_with_1001_candidates() {
        let fake = RecordingFakeDirEnumerator::new();
        fake.with_default_metadata(FsFileMetadata::new(
            64,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1)),
        ));

        let root_key = cas_key("blobs/sha256");
        let shard_key = cas_key("blobs/sha256/0a");

        let mut entries = Vec::with_capacity(1001);
        let mut hexes = Vec::with_capacity(1001);
        for i in 0..1001 {
            let hex = format!("0a{:062x}", i);
            hexes.push(hex.clone());
            entries.push(DirEntry::new(hex.into(), DirEntryType::Regular));
        }

        // Script page 1
        fake.script(
            Some(root_key.clone()),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(Some(shard_key.clone()), Ok(entries.clone()));

        let budgets = FsListingBudgets::new(
            DirEnumerationLimits::new(512, 16_384),
            DirEnumerationLimits::new(2000, 200_000),
        );

        let page1 = list_cas_blobs_page_seam(&fake, None, 50_000, budgets)
            .await
            .expect("page 1 succeeds");

        assert_eq!(page1.items.len(), 1000);
        assert_eq!(page1.items[0].digest.hex(), hexes[0]);
        assert_eq!(page1.items[0].size, 64);
        assert_eq!(page1.items[0].version.0, "1:64");
        assert_eq!(page1.items[999].digest.hex(), hexes[999]);
        assert_eq!(page1.items[999].size, 64);
        assert_eq!(page1.items[999].version.0, "1:64");

        let exp_c1 = format!("sha256:{}", hexes[999]);
        assert_eq!(
            page1.next_cursor.as_ref().map(|c| c.0.as_str()),
            Some(exp_c1.as_str())
        );

        // Script page 2
        fake.script(
            Some(root_key.clone()),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(Some(shard_key.clone()), Ok(entries.clone()));

        let page2 = list_cas_blobs_page_seam(&fake, page1.next_cursor.as_ref(), 50_000, budgets)
            .await
            .expect("page 2 succeeds");

        assert_eq!(page2.items.len(), 1);
        assert_eq!(page2.items[0].digest.hex(), hexes[1000]);
        assert_eq!(page2.items[0].size, 64);
        assert_eq!(page2.items[0].version.0, "1:64");
        assert_eq!(page2.next_cursor, None);

        // Script page 3
        fake.script(
            Some(root_key.clone()),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(Some(shard_key.clone()), Ok(entries));

        let cursor_terminal = GcCursor(format!("sha256:{}", hexes[1000]));
        let page3 = list_cas_blobs_page_seam(&fake, Some(&cursor_terminal), 50_000, budgets)
            .await
            .expect("page 3 succeeds");

        assert!(page3.items.is_empty());
        assert_eq!(page3.next_cursor, None);
    }

    #[tokio::test]
    async fn test_fake_direct_seam_multi_page_progression_and_terminal_behavior() {
        let fake = RecordingFakeDirEnumerator::new();
        fake.with_default_metadata(FsFileMetadata::new(
            256,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(10)),
        ));

        let root_key = cas_key("blobs/sha256");
        let shard_key = cas_key("blobs/sha256/0a");
        let hex1 = "0a00000000000000000000000000000000000000000000000000000000000001";
        let hex2 = "0a00000000000000000000000000000000000000000000000000000000000002";

        // Script Page 1
        fake.script(
            Some(root_key.clone()),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(
            Some(shard_key.clone()),
            Ok(vec![
                DirEntry::new(hex1.into(), DirEntryType::Regular),
                DirEntry::new(hex2.into(), DirEntryType::Regular),
            ]),
        );

        let page1 = list_cas_blobs_page_seam(&fake, None, 2, default_test_budget())
            .await
            .expect("page 1 succeeds");
        assert_eq!(page1.items.len(), 2);
        assert_eq!(page1.items[0].digest.hex(), hex1);
        assert_eq!(page1.items[0].size, 256);
        assert_eq!(page1.items[0].version.0, "10:256");
        assert_eq!(page1.items[1].digest.hex(), hex2);
        assert_eq!(page1.items[1].size, 256);
        assert_eq!(page1.items[1].version.0, "10:256");
        assert_eq!(
            page1.next_cursor.as_ref().map(|c| c.0.as_str()),
            Some(format!("sha256:{hex2}").as_str())
        );

        // Script Page 2
        fake.script(
            Some(root_key.clone()),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(
            Some(shard_key.clone()),
            Ok(vec![
                DirEntry::new(hex1.into(), DirEntryType::Regular),
                DirEntry::new(hex2.into(), DirEntryType::Regular),
            ]),
        );

        let page2 =
            list_cas_blobs_page_seam(&fake, page1.next_cursor.as_ref(), 2, default_test_budget())
                .await
                .expect("page 2 succeeds");
        assert!(page2.items.is_empty());
        assert_eq!(page2.next_cursor, None);
    }

    // ========================================================================
    // Category B: Recording Fake Tests (Metadata Inspection Invariants)
    // ========================================================================

    #[tokio::test]
    async fn test_fake_inspect_selected_keys_order_and_count() {
        let fake = RecordingFakeDirEnumerator::new();
        fake.with_default_metadata(FsFileMetadata::new(100, Some(SystemTime::UNIX_EPOCH)));

        let root_key = cas_key("blobs/sha256");
        let shard_key = cas_key("blobs/sha256/0a");

        let hex1 = "0a00000000000000000000000000000000000000000000000000000000000001";
        let hex2 = "0a00000000000000000000000000000000000000000000000000000000000002";
        let hex3 = "0a00000000000000000000000000000000000000000000000000000000000003";
        let hex4 = "0a00000000000000000000000000000000000000000000000000000000000004";

        fake.script(
            Some(root_key),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(
            Some(shard_key),
            Ok(vec![
                DirEntry::new(hex4.into(), DirEntryType::Regular),
                DirEntry::new(hex1.into(), DirEntryType::Regular),
                DirEntry::new(hex3.into(), DirEntryType::Regular),
                DirEntry::new(hex2.into(), DirEntryType::Regular),
            ]),
        );

        // Cursor excludes hex1. Limit is 2, so only hex2 and hex3 should be returned.
        let cursor = GcCursor(format!("sha256:{hex1}"));
        let page = list_cas_blobs_page_seam(&fake, Some(&cursor), 2, default_test_budget())
            .await
            .expect("listing succeeds");

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].digest.hex(), hex2);
        assert_eq!(page.items[1].digest.hex(), hex3);

        // Assert exact metadata inspection calls:
        // hex1 is skipped by cursor -> NOT inspected.
        // hex2 and hex3 are selected -> inspected in exact order.
        // hex4 is beyond limit -> NOT inspected.
        let inspect_calls = fake.inspect_calls();
        assert_eq!(
            inspect_calls,
            vec![
                cas_key(&format!("blobs/sha256/0a/{hex2}")),
                cas_key(&format!("blobs/sha256/0a/{hex3}")),
            ]
        );
    }

    #[tokio::test]
    async fn test_fake_candidate_size_above_u32_max() {
        let fake = RecordingFakeDirEnumerator::new();
        let large_size = 5_000_000_000_u64;
        let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
        let blob_key = cas_key(&format!("blobs/sha256/0a/{hex}"));

        fake.script(
            Some(cas_key("blobs/sha256")),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(
            Some(cas_key("blobs/sha256/0a")),
            Ok(vec![DirEntry::new(hex.into(), DirEntryType::Regular)]),
        );
        fake.script_inspect(
            blob_key,
            Ok(FsFileMetadata::new(
                large_size,
                Some(SystemTime::UNIX_EPOCH + Duration::from_secs(42)),
            )),
        );

        let page = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
            .await
            .expect("succeeds");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].size, large_size);
        assert_eq!(page.items[0].version.0, format!("42:{large_size}"));
    }

    #[tokio::test]
    async fn test_fake_fractional_timestamp_preservation() {
        let fake = RecordingFakeDirEnumerator::new();
        let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
        let blob_key = cas_key(&format!("blobs/sha256/0a/{hex}"));

        let fractional_time = SystemTime::UNIX_EPOCH + Duration::new(12345, 987_654_321);
        fake.script(
            Some(cas_key("blobs/sha256")),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(
            Some(cas_key("blobs/sha256/0a")),
            Ok(vec![DirEntry::new(hex.into(), DirEntryType::Regular)]),
        );
        fake.script_inspect(
            blob_key,
            Ok(FsFileMetadata::new(200, Some(fractional_time))),
        );

        let page = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
            .await
            .expect("succeeds");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].last_modified, fractional_time);
        // Whole seconds in listing version
        assert_eq!(page.items[0].version.0, "12345:200");
    }

    #[tokio::test]
    async fn test_fake_genuine_epoch_handling() {
        let fake = RecordingFakeDirEnumerator::new();
        let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
        let blob_key = cas_key(&format!("blobs/sha256/0a/{hex}"));

        fake.script(
            Some(cas_key("blobs/sha256")),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(
            Some(cas_key("blobs/sha256/0a")),
            Ok(vec![DirEntry::new(hex.into(), DirEntryType::Regular)]),
        );
        fake.script_inspect(
            blob_key,
            Ok(FsFileMetadata::new(100, Some(SystemTime::UNIX_EPOCH))),
        );

        let page = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
            .await
            .expect("succeeds");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].last_modified, SystemTime::UNIX_EPOCH);
        assert_eq!(page.items[0].version.0, "0:100");

        // Evaluated as MissingTimestamp by registry age policy
        let eligibility = check_candidate_age(
            page.items[0].last_modified,
            SystemTime::now(),
            Duration::from_secs(3600),
        );
        assert_eq!(eligibility, AgeEligibility::MissingTimestamp);
    }

    #[tokio::test]
    async fn test_fake_none_timestamp_fallback() {
        let fake = RecordingFakeDirEnumerator::new();
        let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
        let blob_key = cas_key(&format!("blobs/sha256/0a/{hex}"));

        fake.script(
            Some(cas_key("blobs/sha256")),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(
            Some(cas_key("blobs/sha256/0a")),
            Ok(vec![DirEntry::new(hex.into(), DirEntryType::Regular)]),
        );
        fake.script_inspect(blob_key, Ok(FsFileMetadata::new(100, None)));

        let page = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
            .await
            .expect("succeeds");

        assert_eq!(page.items.len(), 1);
        // None falls back to UNIX_EPOCH
        assert_eq!(page.items[0].last_modified, SystemTime::UNIX_EPOCH);
        assert_eq!(page.items[0].version.0, "0:100");

        let eligibility = check_candidate_age(
            page.items[0].last_modified,
            SystemTime::now(),
            Duration::from_secs(3600),
        );
        assert_eq!(eligibility, AgeEligibility::MissingTimestamp);
    }

    #[tokio::test]
    async fn test_fake_pre_epoch_timestamp_retention_and_zero_version_seconds() {
        let fake = RecordingFakeDirEnumerator::new();
        let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
        let blob_key = cas_key(&format!("blobs/sha256/0a/{hex}"));

        let pre_epoch = SystemTime::UNIX_EPOCH - Duration::new(500, 250_000_000);
        fake.script(
            Some(cas_key("blobs/sha256")),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(
            Some(cas_key("blobs/sha256/0a")),
            Ok(vec![DirEntry::new(hex.into(), DirEntryType::Regular)]),
        );
        fake.script_inspect(blob_key, Ok(FsFileMetadata::new(300, Some(pre_epoch))));

        let page = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
            .await
            .expect("succeeds");

        assert_eq!(page.items.len(), 1);
        // Genuine pre-epoch SystemTime is retained
        assert_eq!(page.items[0].last_modified, pre_epoch);
        // Version seconds calculation defaults to 0
        assert_eq!(page.items[0].version.0, "0:300");

        // Evaluated as Eligible (>50 years old relative to now)
        let eligibility = check_candidate_age(
            page.items[0].last_modified,
            SystemTime::now(),
            Duration::from_secs(3600),
        );
        assert_eq!(eligibility, AgeEligibility::Eligible);
    }

    #[tokio::test]
    async fn test_fake_future_timestamp_retention() {
        let fake = RecordingFakeDirEnumerator::new();
        let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
        let blob_key = cas_key(&format!("blobs/sha256/0a/{hex}"));

        let now = SystemTime::now();
        let future_time = now + Duration::from_secs(7200);
        fake.script(
            Some(cas_key("blobs/sha256")),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(
            Some(cas_key("blobs/sha256/0a")),
            Ok(vec![DirEntry::new(hex.into(), DirEntryType::Regular)]),
        );
        fake.script_inspect(blob_key, Ok(FsFileMetadata::new(400, Some(future_time))));

        let page = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
            .await
            .expect("succeeds");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].last_modified, future_time);

        let eligibility =
            check_candidate_age(page.items[0].last_modified, now, Duration::from_secs(3600));
        assert_eq!(eligibility, AgeEligibility::FutureTimestamp);
    }

    #[tokio::test]
    async fn test_fake_missing_selected_entry_fails_whole_page_io() {
        let fake = RecordingFakeDirEnumerator::new();
        let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
        let blob_key = cas_key(&format!("blobs/sha256/0a/{hex}"));

        fake.script(
            Some(cas_key("blobs/sha256")),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(
            Some(cas_key("blobs/sha256/0a")),
            Ok(vec![DirEntry::new(hex.into(), DirEntryType::Regular)]),
        );
        fake.script_inspect(blob_key.clone(), Err(ReadError::not_found(blob_key)));

        let err = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget())
            .await
            .expect_err("disappeared candidate must fail page closed");

        match err {
            StorageError::Internal { kind, message } => {
                assert_eq!(kind, StorageErrorKind::Io);
                assert!(message.contains("candidate blob disappeared before metadata inspection"));
            }
            other => panic!("expected StorageErrorKind::Io, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_fake_later_inspection_failure_does_not_yield_partial_page() {
        let fake = RecordingFakeDirEnumerator::new();
        let hex1 = "0a00000000000000000000000000000000000000000000000000000000000001";
        let hex2 = "0a00000000000000000000000000000000000000000000000000000000000002";
        let key1 = cas_key(&format!("blobs/sha256/0a/{hex1}"));
        let key2 = cas_key(&format!("blobs/sha256/0a/{hex2}"));

        fake.script(
            Some(cas_key("blobs/sha256")),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(
            Some(cas_key("blobs/sha256/0a")),
            Ok(vec![
                DirEntry::new(hex1.into(), DirEntryType::Regular),
                DirEntry::new(hex2.into(), DirEntryType::Regular),
            ]),
        );

        // Candidate 1 succeeds
        fake.script_inspect(
            key1,
            Ok(FsFileMetadata::new(100, Some(SystemTime::UNIX_EPOCH))),
        );
        // Candidate 2 fails
        fake.script_inspect(key2.clone(), Err(ReadError::not_found(key2)));

        let res = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget()).await;
        assert!(
            res.is_err(),
            "later inspection failure must fail entire page"
        );
        match res.unwrap_err() {
            StorageError::Internal { kind, message } => {
                assert_eq!(kind, StorageErrorKind::Io);
                assert!(message.contains("candidate blob disappeared"));
            }
            other => panic!("expected StorageErrorKind::Io, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_fake_typed_error_mappings_unaffected_by_diagnostic_words() {
        // 1. ReadError::Backend with misleading message containing "corrupt" and "not a directory"
        // but source is std::io::Error(PermissionDenied) -> maps to Io
        {
            let err = ReadError::backend_with_source(
                "corrupt data syntax fatal not a directory",
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "permission denied",
                )),
            );
            let mapped = translate_inspect_error(err);
            match mapped {
                StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
                other => panic!("expected Io, got {other:?}"),
            }
        }

        // 2. ReadError::Backend with misleading message "io error missing"
        // but source is UnsupportedObjectType -> maps to CorruptData
        {
            let err = ReadError::backend_with_source(
                "io error missing not found",
                Box::new(storage_fs::FsMetadataError::UnsupportedObjectType {
                    mode: libc::S_IFDIR,
                }),
            );
            let mapped = translate_inspect_error(err);
            match mapped {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::CorruptData);
                    assert!(message.contains("unsupported object type"));
                }
                other => panic!("expected CorruptData, got {other:?}"),
            }
        }

        // 3. StatFailed with misleading "corrupt" message -> maps to Io
        {
            let err = ReadError::backend_with_source(
                "corrupt invalid data",
                Box::new(storage_fs::FsMetadataError::StatFailed {
                    stage: "file inspection",
                    source: std::io::Error::from_raw_os_error(libc::EIO),
                }),
            );
            let mapped = translate_inspect_error(err);
            match mapped {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Io);
                    assert!(message.contains("failed to stat file inspection descriptor"));
                }
                other => panic!("expected Io, got {other:?}"),
            }
        }

        // 4. InvalidMetadata -> maps to CorruptData
        {
            let err = ReadError::backend_with_source(
                "io read failure",
                Box::new(storage_fs::FsMetadataError::InvalidMetadata {
                    message: "negative file size",
                }),
            );
            let mapped = translate_inspect_error(err);
            match mapped {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::CorruptData);
                    assert!(message.contains("negative file size"));
                }
                other => panic!("expected CorruptData, got {other:?}"),
            }
        }

        // 5. SyscallUnsupported -> maps to Configuration
        {
            let err = ReadError::backend_with_source(
                "io error",
                Box::new(storage_fs::FsMetadataError::SyscallUnsupported(
                    std::io::Error::from_raw_os_error(libc::ENOSYS),
                )),
            );
            let mapped = translate_inspect_error(err);
            match mapped {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Configuration);
                    assert!(message.contains("openat2 is unavailable"));
                }
                other => panic!("expected Configuration, got {other:?}"),
            }
        }

        // 6. PlatformUnsupported -> maps to Configuration
        {
            let err = ReadError::backend_with_source(
                "io error",
                Box::new(storage_fs::FsMetadataError::PlatformUnsupported),
            );
            let mapped = translate_inspect_error(err);
            match mapped {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Configuration);
                    assert!(message.contains("platform unsupported"));
                }
                other => panic!("expected Configuration, got {other:?}"),
            }
        }

        // 7. ResolutionRejected -> maps to Io
        {
            let err = ReadError::backend_with_source(
                "corrupt link",
                Box::new(storage_fs::FsMetadataError::ResolutionRejected {
                    raw_os_error: libc::ELOOP,
                    source: std::io::Error::from_raw_os_error(libc::ELOOP),
                }),
            );
            let mapped = translate_inspect_error(err);
            match mapped {
                StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
                other => panic!("expected Io, got {other:?}"),
            }
        }

        // 8. Plain message with no source -> maps to Io
        {
            let err = ReadError::backend("misleading corrupt not a directory");
            let mapped = translate_inspect_error(err);
            match mapped {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Io);
                    assert_eq!(message, "misleading corrupt not a directory");
                }
                other => panic!("expected Io, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn test_fake_budget_failure_prevents_partial_page() {
        let fake = RecordingFakeDirEnumerator::new();
        fake.with_default_metadata(FsFileMetadata::new(100, Some(SystemTime::UNIX_EPOCH)));

        let root_key = cas_key("blobs/sha256");
        let shard_0a = cas_key("blobs/sha256/0a");
        let shard_0b = cas_key("blobs/sha256/0b");

        let hex_0a = "0a00000000000000000000000000000000000000000000000000000000000001";

        fake.script(
            Some(root_key),
            Ok(vec![
                DirEntry::new("0a".into(), DirEntryType::Directory),
                DirEntry::new("0b".into(), DirEntryType::Directory),
            ]),
        );
        fake.script(
            Some(shard_0a),
            Ok(vec![DirEntry::new(hex_0a.into(), DirEntryType::Regular)]),
        );
        fake.script(
            Some(shard_0b),
            Err(FsDirError::LimitExceeded {
                reason: storage_fs::LimitExceededReason::MaxEntries(10),
            }),
        );

        let res = list_cas_blobs_page_seam(&fake, None, 10, default_test_budget()).await;
        assert!(res.is_err(), "budget failure must not return partial page");
        match res.unwrap_err() {
            StorageError::Internal { kind, message } => {
                assert_eq!(kind, StorageErrorKind::Backend);
                assert!(message.contains("enumeration resource limit exceeded"));
            }
            other => panic!("expected Backend, got {other:?}"),
        }
    }

    // ========================================================================
    // Category C: Real Linux storage-fs Filesystem Tests
    // ========================================================================

    #[cfg(target_os = "linux")]
    mod linux_fs_tests {
        use super::*;
        use std::fs::{File, FileTimes};
        use std::os::unix::fs::MetadataExt;
        use std::path::{Path, PathBuf};

        fn create_test_root() -> (tempfile::TempDir, PathBuf) {
            let fixture = tempfile::tempdir().expect("create tempdir");
            let root = fixture.path().join("storage_root");
            std::fs::create_dir_all(&root).expect("create storage root");
            (fixture, root)
        }

        fn put_blob(root: &Path, hex: &str, content: &[u8]) {
            let p2 = &hex[..2];
            let dir = root.join("blobs").join("sha256").join(p2);
            std::fs::create_dir_all(&dir).expect("create shard dir");
            std::fs::write(dir.join(hex), content).expect("write blob file");
        }

        /// Narrow wrapper intercepting metadata inspection to inject deterministic filesystem mutations.
        struct InterceptingListingWrapper<'a, T: CasListingSource + ?Sized> {
            inner: &'a T,
            on_before_inspect: Mutex<Option<Box<dyn FnMut(&ObjectKey) + Send + Sync + 'a>>>,
        }

        impl<'a, T: CasListingSource + ?Sized> InterceptingListingWrapper<'a, T> {
            fn new(inner: &'a T, hook: impl FnMut(&ObjectKey) + Send + Sync + 'a) -> Self {
                Self {
                    inner,
                    on_before_inspect: Mutex::new(Some(Box::new(hook))),
                }
            }
        }

        #[async_trait]
        impl<'a, T: CasListingSource + ?Sized> CasDirEnumerator for InterceptingListingWrapper<'a, T> {
            async fn enumerate_dir(
                &self,
                target: Option<&ObjectKey>,
                limits: DirEnumerationLimits,
            ) -> Result<Vec<DirEntry>, FsDirError> {
                self.inner.enumerate_dir(target, limits).await
            }
        }

        #[async_trait]
        impl<'a, T: CasListingSource + ?Sized> CasMetadataInspector for InterceptingListingWrapper<'a, T> {
            async fn inspect_file_metadata(
                &self,
                key: &ObjectKey,
            ) -> Result<FsFileMetadata, ReadError> {
                if let Some(ref mut hook) = *self.on_before_inspect.lock().unwrap() {
                    hook(key);
                }
                self.inner.inspect_file_metadata(key).await
            }
        }

        #[tokio::test]
        async fn test_real_fs_absent_cas_root_returns_empty_page() {
            let (_fixture, root) = create_test_root();
            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let page = list_cas_blobs_page_seam(&reader, None, 100, default_test_budget())
                .await
                .expect("absent CAS directory must return empty page");
            assert!(page.items.is_empty());
            assert_eq!(page.next_cursor, None);
        }

        #[tokio::test]
        async fn test_real_fs_empty_cas_root_returns_empty_page() {
            let (_fixture, root) = create_test_root();
            std::fs::create_dir_all(root.join("blobs").join("sha256")).unwrap();
            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let page = list_cas_blobs_page_seam(&reader, None, 100, default_test_budget())
                .await
                .expect("empty CAS directory must return empty page");
            assert!(page.items.is_empty());
            assert_eq!(page.next_cursor, None);
        }

        #[tokio::test]
        async fn test_real_fs_empty_shard_returns_empty_page() {
            let (_fixture, root) = create_test_root();
            std::fs::create_dir_all(root.join("blobs").join("sha256").join("0a")).unwrap();
            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let page = list_cas_blobs_page_seam(&reader, None, 100, default_test_budget())
                .await
                .expect("empty shard returns empty page");
            assert!(page.items.is_empty());
            assert_eq!(page.next_cursor, None);
        }

        #[tokio::test]
        async fn test_real_fs_ordering_and_multi_page_pagination() {
            let (_fixture, root) = create_test_root();
            let hexes = [
                "0a00000000000000000000000000000000000000000000000000000000000001",
                "0a00000000000000000000000000000000000000000000000000000000000002",
                "1b00000000000000000000000000000000000000000000000000000000000001",
                "ff00000000000000000000000000000000000000000000000000000000000001",
                "ff00000000000000000000000000000000000000000000000000000000000002",
            ];
            for hex in &hexes {
                put_blob(&root, hex, b"payload");
            }

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            // Page 1 (limit 2): hexes[0], hexes[1]
            let page1 = list_cas_blobs_page_seam(&reader, None, 2, default_test_budget())
                .await
                .expect("page 1");
            assert_eq!(page1.items.len(), 2);
            assert_eq!(page1.items[0].digest.hex(), hexes[0]);
            assert_eq!(page1.items[0].size, 7);
            assert!(page1.items[0].version.0.ends_with(":7"));
            assert_eq!(page1.items[1].digest.hex(), hexes[1]);
            assert_eq!(page1.items[1].size, 7);
            assert!(page1.items[1].version.0.ends_with(":7"));
            let exp_c1 = format!("sha256:{}", hexes[1]);
            assert_eq!(
                page1.next_cursor.as_ref().map(|c| c.0.as_str()),
                Some(exp_c1.as_str())
            );

            // Page 2 (limit 2, cursor page 1): hexes[2], hexes[3]
            let page2 = list_cas_blobs_page_seam(
                &reader,
                page1.next_cursor.as_ref(),
                2,
                default_test_budget(),
            )
            .await
            .expect("page 2");
            assert_eq!(page2.items.len(), 2);
            assert_eq!(page2.items[0].digest.hex(), hexes[2]);
            assert_eq!(page2.items[0].size, 7);
            assert_eq!(page2.items[1].digest.hex(), hexes[3]);
            assert_eq!(page2.items[1].size, 7);
            let exp_c2 = format!("sha256:{}", hexes[3]);
            assert_eq!(
                page2.next_cursor.as_ref().map(|c| c.0.as_str()),
                Some(exp_c2.as_str())
            );

            // Page 3 (limit 2, cursor page 2): hexes[4]
            let page3 = list_cas_blobs_page_seam(
                &reader,
                page2.next_cursor.as_ref(),
                2,
                default_test_budget(),
            )
            .await
            .expect("page 3");
            assert_eq!(page3.items.len(), 1);
            assert_eq!(page3.items[0].digest.hex(), hexes[4]);
            assert_eq!(page3.items[0].size, 7);
            assert_eq!(page3.next_cursor, None);

            // Resuming with final cursor returns empty page
            let cursor_final = GcCursor(format!("sha256:{}", hexes[4]));
            let page_empty =
                list_cas_blobs_page_seam(&reader, Some(&cursor_final), 2, default_test_budget())
                    .await
                    .expect("page empty");
            assert!(page_empty.items.is_empty());
            assert_eq!(page_empty.next_cursor, None);
        }

        #[tokio::test]
        async fn test_real_fs_exact_full_final_page() {
            let (_fixture, root) = create_test_root();
            let hexes = [
                "0a00000000000000000000000000000000000000000000000000000000000001",
                "0a00000000000000000000000000000000000000000000000000000000000002",
            ];
            for hex in &hexes {
                put_blob(&root, hex, b"payload");
            }

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let page1 = list_cas_blobs_page_seam(&reader, None, 2, default_test_budget())
                .await
                .expect("page 1");
            assert_eq!(page1.items.len(), 2);
            assert_eq!(page1.items[0].size, 7);
            assert_eq!(page1.items[1].size, 7);
            let expected_cursor = format!("sha256:{}", hexes[1]);
            assert_eq!(
                page1.next_cursor.as_ref().map(|c| c.0.as_str()),
                Some(expected_cursor.as_str())
            );

            let page2 = list_cas_blobs_page_seam(
                &reader,
                page1.next_cursor.as_ref(),
                2,
                default_test_budget(),
            )
            .await
            .expect("terminal page");
            assert!(page2.items.is_empty());
            assert_eq!(page2.next_cursor, None);
        }

        #[tokio::test]
        async fn test_real_fs_limit_clamping_zero_to_one() {
            let (_fixture, root) = create_test_root();
            let hex1 = "0a00000000000000000000000000000000000000000000000000000000000001";
            let hex2 = "0a00000000000000000000000000000000000000000000000000000000000002";
            put_blob(&root, hex1, b"p1");
            put_blob(&root, hex2, b"p2");

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let page_zero = list_cas_blobs_page_seam(&reader, None, 0, default_test_budget())
                .await
                .expect("limit zero clamped to 1");
            assert_eq!(page_zero.items.len(), 1);
            assert_eq!(page_zero.items[0].digest.hex(), hex1);
            assert_eq!(page_zero.items[0].size, 2);
        }

        #[tokio::test]
        async fn test_real_fs_cursor_lexical_filtering_and_malformed() {
            let (_fixture, root) = create_test_root();
            let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
            put_blob(&root, hex, b"p1");

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let cursor_zzz = GcCursor("zzz".into());
            let page_zzz =
                list_cas_blobs_page_seam(&reader, Some(&cursor_zzz), 10, default_test_budget())
                    .await
                    .expect("zzz cursor");
            assert!(page_zzz.items.is_empty());
            assert_eq!(page_zzz.next_cursor, None);

            let cursor_aaa = GcCursor("aaa".into());
            let page_aaa =
                list_cas_blobs_page_seam(&reader, Some(&cursor_aaa), 10, default_test_budget())
                    .await
                    .expect("aaa cursor");
            assert_eq!(page_aaa.items.len(), 1);
            assert_eq!(page_aaa.items[0].digest.hex(), hex);
            assert_eq!(page_aaa.items[0].size, 2);
        }

        #[tokio::test]
        async fn test_real_fs_fails_closed_on_symlinked_shard() {
            let (fixture, root) = create_test_root();
            let cas_root = root.join("blobs").join("sha256");
            std::fs::create_dir_all(&cas_root).unwrap();

            let target_shard = fixture.path().join("external_shard");
            std::fs::create_dir_all(&target_shard).unwrap();
            let link_shard = cas_root.join("2b");
            std::os::unix::fs::symlink(&target_shard, &link_shard).unwrap();

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let res = list_cas_blobs_page_seam(&reader, None, 10, default_test_budget()).await;
            assert!(res.is_err(), "symlinked shard directory must fail closed");
            let err = res.unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::CorruptData);
                    assert!(
                        message
                            .contains("malformed non-directory entry in CAS prefix directory root")
                    );
                }
                other => panic!("expected CorruptData, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_fs_fails_closed_on_symlinked_blob() {
            let (fixture, root) = create_test_root();
            let shard = root.join("blobs").join("sha256").join("0a");
            std::fs::create_dir_all(&shard).unwrap();

            let target_file = fixture.path().join("target_blob.bin");
            std::fs::write(&target_file, b"symlinked blob data").unwrap();
            let valid_hex_name = "0a00000000000000000000000000000000000000000000000000000000000001";
            let link_file = shard.join(valid_hex_name);
            std::os::unix::fs::symlink(&target_file, &link_file).unwrap();

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let res = list_cas_blobs_page_seam(&reader, None, 10, default_test_budget()).await;
            assert!(res.is_err(), "symlinked blob file must fail closed");
            let err = res.unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::CorruptData);
                    assert!(message.contains("malformed non-file entry in CAS shard directory"));
                }
                other => panic!("expected CorruptData, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_fs_fails_closed_on_nested_subdirectories_in_shard() {
            let (_fixture, root) = create_test_root();
            let shard = root.join("blobs").join("sha256").join("0a");
            let nested = shard.join("nested_subdir");
            std::fs::create_dir_all(&nested).unwrap();

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let res = list_cas_blobs_page_seam(&reader, None, 10, default_test_budget()).await;
            assert!(res.is_err(), "nested directory in shard must fail closed");
            let err = res.unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::CorruptData);
                    assert!(message.contains("malformed non-file entry in CAS shard directory"));
                }
                other => panic!("expected CorruptData, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_fs_ancestor_symlink_rejection() {
            let (fixture, root) = create_test_root();
            let target_blobs = fixture.path().join("target_blobs");
            let target_cas = target_blobs.join("sha256").join("0a");
            std::fs::create_dir_all(&target_cas).unwrap();
            let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
            std::fs::write(target_cas.join(hex), b"payload").unwrap();

            std::os::unix::fs::symlink(&target_blobs, root.join("blobs")).unwrap();

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let res = list_cas_blobs_page_seam(&reader, None, 10, default_test_budget()).await;
            assert!(
                res.is_err(),
                "listing through ancestor symlink must fail closed under openat2 containment"
            );
            let err = res.unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Io);
                    assert!(
                        message
                            .contains(&std::io::Error::from_raw_os_error(libc::ELOOP).to_string()),
                        "error must reflect kernel ELOOP containment rejection: {message}"
                    );
                }
                other => panic!("expected StorageError::Internal(Io), got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_fs_initial_not_a_directory_error_not_suppressed() {
            let (_fixture, root) = create_test_root();
            std::fs::write(root.join("blobs"), b"regular file not a directory").unwrap();

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let res = list_cas_blobs_page_seam(&reader, None, 10, default_test_budget()).await;
            assert!(
                res.is_err(),
                "NotADirectory on CAS root must fail closed, NOT be suppressed as empty results"
            );
            let err = res.unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::CorruptData);
                    assert!(message.contains("not a directory"));
                }
                other => panic!("expected CorruptData, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_fs_budget_limits_entry_count() {
            let (_fixture, root) = create_test_root();
            for p2 in ["0a", "0b", "0c"] {
                std::fs::create_dir_all(root.join("blobs").join("sha256").join(p2)).unwrap();
            }

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let tight_budget = FsListingBudgets::new(
                DirEnumerationLimits::new(1, 100_000),
                DirEnumerationLimits::new(100_000, 8 * 1024 * 1024),
            );
            let res = list_cas_blobs_page_seam(&reader, None, 10, tight_budget).await;
            assert!(
                res.is_err(),
                "enumeration exceeding root max_entries budget must fail closed"
            );
            let err = res.unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Backend);
                    assert!(message.contains("enumeration resource limit exceeded"));
                }
                other => panic!("expected Backend, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_fs_budget_limits_name_bytes() {
            let (_fixture, root) = create_test_root();
            for p2 in ["0a", "0b"] {
                std::fs::create_dir_all(root.join("blobs").join("sha256").join(p2)).unwrap();
            }

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let tight_budget = FsListingBudgets::new(
                DirEnumerationLimits::new(100, 2),
                DirEnumerationLimits::new(100_000, 8 * 1024 * 1024),
            );
            let res = list_cas_blobs_page_seam(&reader, None, 10, tight_budget).await;
            assert!(
                res.is_err(),
                "enumeration exceeding root max_total_name_bytes must fail closed"
            );
            let err = res.unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Backend);
                    assert!(message.contains("enumeration resource limit exceeded"));
                }
                other => panic!("expected Backend, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_fs_shard_budget_limits_entry_count() {
            let (_fixture, root) = create_test_root();
            let shard_dir = root.join("blobs").join("sha256").join("0a");
            std::fs::create_dir_all(&shard_dir).unwrap();
            let hex1 = "0a00000000000000000000000000000000000000000000000000000000000001";
            let hex2 = "0a00000000000000000000000000000000000000000000000000000000000002";
            std::fs::write(shard_dir.join(hex1), b"data1").unwrap();
            std::fs::write(shard_dir.join(hex2), b"data2").unwrap();

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            // Ample root budget (512, 16 KiB), tight shard budget (1 entry)
            let tight_shard_budget = FsListingBudgets::new(
                DirEnumerationLimits::new(512, 16 * 1024),
                DirEnumerationLimits::new(1, 100_000),
            );
            let res = list_cas_blobs_page_seam(&reader, None, 10, tight_shard_budget).await;
            assert!(
                res.is_err(),
                "enumeration exceeding shard max_entries budget must fail closed"
            );
            let err = res.unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Backend);
                    assert!(message.contains("enumeration resource limit exceeded"));
                }
                other => panic!("expected Backend, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_fs_shard_budget_limits_name_bytes() {
            let (_fixture, root) = create_test_root();
            let shard_dir = root.join("blobs").join("sha256").join("0a");
            std::fs::create_dir_all(&shard_dir).unwrap();
            let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
            std::fs::write(shard_dir.join(hex), b"data").unwrap();

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            // Ample root budget (512, 16 KiB), tight shard byte budget (10 bytes < 64 byte filename)
            let tight_shard_budget = FsListingBudgets::new(
                DirEnumerationLimits::new(512, 16 * 1024),
                DirEnumerationLimits::new(100, 10),
            );
            let res = list_cas_blobs_page_seam(&reader, None, 10, tight_shard_budget).await;
            assert!(
                res.is_err(),
                "enumeration exceeding shard max_total_name_bytes must fail closed"
            );
            let err = res.unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Backend);
                    assert!(message.contains("enumeration resource limit exceeded"));
                }
                other => panic!("expected Backend, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_fs_success_when_root_and_shard_require_different_budgets() {
            let (_fixture, root) = create_test_root();
            let shard_dir = root.join("blobs").join("sha256").join("0a");
            std::fs::create_dir_all(&shard_dir).unwrap();
            for i in 1..=3 {
                let hex = format!("0a{:062x}", i);
                std::fs::write(shard_dir.join(hex), b"blob").unwrap();
            }

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            // Root has max_entries: 2 (enough for 1 prefix dir "0a", but LESS than 3 shard blobs)
            // Shard has max_entries: 5 (enough for 3 shard blobs)
            let asymmetric_budgets = FsListingBudgets::new(
                DirEnumerationLimits::new(2, 500),
                DirEnumerationLimits::new(5, 2000),
            );
            let page = list_cas_blobs_page_seam(&reader, None, 10, asymmetric_budgets)
                .await
                .expect("page succeeds when root and shard require different budgets");
            assert_eq!(page.items.len(), 3);
        }

        #[tokio::test]
        async fn test_real_fs_zero_limit_empty_vs_non_empty() {
            let (_fixture, root) = create_test_root();
            let cas_root = root.join("blobs").join("sha256");
            std::fs::create_dir_all(&cas_root).unwrap();

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");
            let zero_budget = FsListingBudgets::new(
                DirEnumerationLimits::new(0, 0),
                DirEnumerationLimits::new(0, 0),
            );

            let page_empty = list_cas_blobs_page_seam(&reader, None, 10, zero_budget)
                .await
                .expect("empty directory succeeds with zero limits");
            assert!(page_empty.items.is_empty());

            std::fs::create_dir_all(cas_root.join("0a")).unwrap();
            let res = list_cas_blobs_page_seam(&reader, None, 10, zero_budget).await;
            assert!(
                res.is_err(),
                "non-empty directory must fail closed under zero limit"
            );
            let err = res.unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Backend);
                    assert!(message.contains("enumeration resource limit exceeded"));
                }
                other => panic!("expected Backend, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_fs_deterministic_inter_page_mutation_no_snapshot() {
            let (_fixture, root) = create_test_root();
            let hex_0a = "0a00000000000000000000000000000000000000000000000000000000000002";
            let hex_1b = "1b00000000000000000000000000000000000000000000000000000000000001";
            put_blob(&root, hex_0a, b"p_0a");
            put_blob(&root, hex_1b, b"p_1b");

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let page1 = list_cas_blobs_page_seam(&reader, None, 1, default_test_budget())
                .await
                .expect("page 1");
            assert_eq!(page1.items.len(), 1);
            assert_eq!(page1.items[0].digest.hex(), hex_0a);
            let c1 = page1.next_cursor.expect("cursor present on full page");

            let hex_behind = "0a00000000000000000000000000000000000000000000000000000000000001";
            put_blob(&root, hex_behind, b"behind");
            let hex_ahead = "1b00000000000000000000000000000000000000000000000000000000000002";
            put_blob(&root, hex_ahead, b"ahead");

            let page2 = list_cas_blobs_page_seam(&reader, Some(&c1), 10, default_test_budget())
                .await
                .expect("page 2");

            let returned_hexes: Vec<String> = page2
                .items
                .iter()
                .map(|i| i.digest.hex().to_string())
                .collect();

            assert!(
                !returned_hexes.contains(&hex_behind.to_string()),
                "blob inserted behind cursor must be missed in current pagination cycle"
            );
            assert!(
                returned_hexes.contains(&hex_1b.to_string()),
                "pre-existing blob ahead of cursor must be observed"
            );
            assert!(
                returned_hexes.contains(&hex_ahead.to_string()),
                "blob inserted ahead of cursor must be observed"
            );
        }

        #[tokio::test]
        async fn test_real_fs_candidate_metadata_accuracy_and_formatting() {
            let (_fixture, root) = create_test_root();
            let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
            let payload = b"deterministic payload with 33 b";
            put_blob(&root, hex, payload);

            let blob_disk_path = root.join("blobs").join("sha256").join("0a").join(hex);

            // Set deliberate modification timestamp and obtain filesystem readback
            let deliberate_mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(1_720_000_000);
            let blob_file = File::open(&blob_disk_path).expect("open blob file to set mtime");
            let mut times = FileTimes::new();
            times = times.set_modified(deliberate_mtime);
            blob_file
                .set_times(times)
                .expect("set deliberate blob modification time");
            drop(blob_file);

            let fs_meta = std::fs::metadata(&blob_disk_path).expect("readback blob fs metadata");
            let fs_mtime = fs_meta.modified().expect("readback blob fs mtime");

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");
            let page = list_cas_blobs_page_seam(&reader, None, 10, default_test_budget())
                .await
                .expect("listing succeeds");

            assert_eq!(page.items.len(), 1);
            let cand = &page.items[0];
            assert_eq!(cand.digest.hex(), hex);
            assert_eq!(cand.size, payload.len() as u64);

            // Modification timestamp equals actual filesystem readback observation
            assert_eq!(cand.last_modified, fs_mtime);

            // Version format matches {mtime_secs}:{size}
            let exp_secs = fs_mtime
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let exp_version = format!("{exp_secs}:{}", payload.len());
            assert_eq!(cand.version.0, exp_version);

            // Derive age-check reference time deterministically from observed timestamp
            // rather than SystemTime::now(), eliminating wall-clock dependencies
            let check_now = fs_mtime + Duration::from_secs(3600);
            let min_age = Duration::from_secs(1800);
            let age_eligibility = check_candidate_age(cand.last_modified, check_now, min_age);
            assert_eq!(age_eligibility, AgeEligibility::Eligible);

            let check_young = fs_mtime + Duration::from_secs(600);
            let age_young = check_candidate_age(cand.last_modified, check_young, min_age);
            assert_eq!(age_young, AgeEligibility::IneligibleAge);
        }

        #[tokio::test]
        async fn test_real_fs_disappeared_blob_between_enumeration_and_inspection_fails_closed_io()
        {
            let (_fixture, root) = create_test_root();
            let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
            put_blob(&root, hex, b"data to disappear");

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let blob_disk_path = root.join("blobs").join("sha256").join("0a").join(hex);
            let wrapper = InterceptingListingWrapper::new(&reader, move |_key| {
                // Delete file right before metadata inspection with checked failure handling
                std::fs::remove_file(&blob_disk_path)
                    .expect("fixture failure: must remove blob file before metadata inspection");
            });

            let res = list_cas_blobs_page_seam(&wrapper, None, 10, default_test_budget()).await;
            assert!(
                res.is_err(),
                "candidate disappearing before inspection must fail closed"
            );
            let err = res.unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Io);
                    assert!(
                        message.contains("candidate blob disappeared before metadata inspection")
                    );
                }
                other => panic!("expected StorageErrorKind::Io, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_fs_symlink_substituted_blob_fails_closed_io() {
            let (fixture, root) = create_test_root();
            let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
            put_blob(&root, hex, b"initial regular file");

            let external_target = fixture.path().join("external_target.bin");
            std::fs::write(&external_target, b"external content").unwrap();

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let blob_disk_path = root.join("blobs").join("sha256").join("0a").join(hex);
            let wrapper = InterceptingListingWrapper::new(&reader, move |_key| {
                // Replace regular file with symlink pointing outside root
                std::fs::remove_file(&blob_disk_path).unwrap();
                std::os::unix::fs::symlink(&external_target, &blob_disk_path).unwrap();
            });

            let res = list_cas_blobs_page_seam(&wrapper, None, 10, default_test_budget()).await;
            assert!(
                res.is_err(),
                "symlink-substituted candidate must fail closed under openat2 containment"
            );
            let err = res.unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::Io);
                    assert!(
                        message
                            .contains(&std::io::Error::from_raw_os_error(libc::ELOOP).to_string()),
                        "must reflect kernel ELOOP containment rejection: {message}"
                    );
                }
                other => panic!("expected StorageErrorKind::Io, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_fs_directory_substituted_blob_fails_closed_corrupt_data() {
            let (_fixture, root) = create_test_root();
            let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
            put_blob(&root, hex, b"initial regular file");

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let blob_disk_path = root.join("blobs").join("sha256").join("0a").join(hex);
            let wrapper = InterceptingListingWrapper::new(&reader, move |_key| {
                // Replace regular file with a directory
                std::fs::remove_file(&blob_disk_path).unwrap();
                std::fs::create_dir(&blob_disk_path).unwrap();
            });

            let res = list_cas_blobs_page_seam(&wrapper, None, 10, default_test_budget()).await;
            assert!(
                res.is_err(),
                "directory-substituted candidate must fail closed with CorruptData"
            );
            let err = res.unwrap_err();
            match err {
                StorageError::Internal { kind, message } => {
                    assert_eq!(kind, StorageErrorKind::CorruptData);
                    assert!(message.contains("unsupported object type"));
                }
                other => panic!("expected StorageErrorKind::CorruptData, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_real_fs_regular_file_replacement_observed_at_resolution_time() {
            let (fixture, root) = create_test_root();
            let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
            let orig_payload = b"original 10 bytes!";
            put_blob(&root, hex, orig_payload);

            let blob_disk_path = root.join("blobs").join("sha256").join("0a").join(hex);

            // Keep an open handle to the original file throughout the identity assertions,
            // preventing inode reuse from undermining the identity distinction evidence.
            let original_held_file =
                File::open(&blob_disk_path).expect("open original blob to retain handle and inode");
            let orig_meta = original_held_file
                .metadata()
                .expect("original held file metadata");
            let (orig_dev, orig_ino) = (orig_meta.dev(), orig_meta.ino());

            // Create a separate regular file with different content and size on the same filesystem,
            // outside the enumerated shard directory.
            let replacement_path = fixture.path().join("replacement_object.bin");
            let replacement_payload = b"replacement payload with distinct size 37 b";
            std::fs::write(&replacement_path, replacement_payload)
                .expect("write replacement object");

            // Set deliberate modification timestamp on replacement and obtain its filesystem readback
            let replacement_deliberate_time =
                SystemTime::UNIX_EPOCH + Duration::from_secs(1_730_000_000);
            let replacement_file =
                File::open(&replacement_path).expect("open replacement file to set times");
            let mut repl_times = FileTimes::new();
            repl_times = repl_times.set_modified(replacement_deliberate_time);
            replacement_file
                .set_times(repl_times)
                .expect("set replacement deliberate modification time");
            drop(replacement_file);

            let repl_meta =
                std::fs::metadata(&replacement_path).expect("readback replacement metadata");
            let (repl_dev, repl_ino) = (repl_meta.dev(), repl_meta.ino());
            let repl_mtime = repl_meta.modified().expect("readback replacement mtime");

            // Verify on disk that held original file and replacement are distinct file objects
            assert_eq!(
                orig_dev, repl_dev,
                "both files must reside on the same filesystem"
            );
            assert_ne!(
                orig_ino, repl_ino,
                "held original file and replacement file must have distinct (dev, ino) identities"
            );

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            let replacement_path_clone = replacement_path.clone();
            let blob_disk_path_clone = blob_disk_path.clone();
            let wrapper = InterceptingListingWrapper::new(&reader, move |_key| {
                // In the before-inspection hook, atomically rename the separate replacement file
                // over the selected blob path.
                std::fs::rename(&replacement_path_clone, &blob_disk_path_clone)
                    .expect("fixture failure: rename replacement file over blob path");
            });

            let page = list_cas_blobs_page_seam(&wrapper, None, 10, default_test_budget())
                .await
                .expect("inspection succeeds on valid replacement file");

            assert_eq!(page.items.len(), 1);
            let cand = &page.items[0];

            // Verify that inspection observed the replacement file's exact attributes:
            // 1. Replacement's exact size
            assert_eq!(cand.size, replacement_payload.len() as u64);
            // 2. Replacement's filesystem-observed modification time
            assert_eq!(cand.last_modified, repl_mtime);
            // 3. Complete registry-formatted version matching replacement attributes
            let exp_version_secs = repl_mtime
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let exp_version = format!("{exp_version_secs}:{}", replacement_payload.len());
            assert_eq!(cand.version.0, exp_version);

            // Establish that the held original file still retains its distinct inode
            // and did not experience in-place modification
            let orig_after_meta = original_held_file
                .metadata()
                .expect("held original file metadata after rename");
            assert_eq!(
                orig_after_meta.ino(),
                orig_ino,
                "held original file inode remains unchanged"
            );
            assert_eq!(
                orig_after_meta.len(),
                orig_payload.len() as u64,
                "held original file size remains unchanged"
            );

            // Retain explicit boundary: enumeration and inspection are separate observations.
            // This replacement observation does not constitute an identity or snapshot guarantee.
            drop(original_held_file);
        }
    }

    // ========================================================================
    // Category D: CasBlobTraverser Integration Over Seam Bridge
    // ========================================================================

    /// Test-only read-only bridge connecting a [`CasListingSource`] to [`crate::storage::ports::GcStoragePort`]
    /// to exercise [`CasBlobTraverser`] over real seam pages.
    struct SeamGcStorageBridge<'a, S: CasListingSource + ?Sized> {
        source: &'a S,
        budgets: FsListingBudgets,
    }

    impl<'a, S: CasListingSource + ?Sized> SeamGcStorageBridge<'a, S> {
        fn new(source: &'a S, budgets: FsListingBudgets) -> Self {
            Self { source, budgets }
        }
    }

    #[async_trait]
    impl<'a, S: CasListingSource + ?Sized> crate::storage::ports::GcStoragePort
        for SeamGcStorageBridge<'a, S>
    {
        fn kind(&self) -> &'static str {
            "seam-gc-storage-bridge"
        }
        fn gc_strategy(&self) -> crate::storage::GcStorageStrategy {
            crate::storage::GcStorageStrategy::FilesystemQuarantine
        }
        async fn check_bucket_versioning_for_gc(&self) -> Result<(), StorageError> {
            Ok(())
        }
        async fn list_cas_blobs_page(
            &self,
            cursor: Option<&GcCursor>,
            limit: usize,
        ) -> Result<GcBlobPage, StorageError> {
            list_cas_blobs_page_seam(self.source, cursor, limit, self.budgets).await
        }
        async fn quarantine_blob(
            &self,
            _: &crate::storage::GcMutationPermit<'_>,
            _: &Digest,
            _: &BlobObjectVersion,
        ) -> Result<crate::storage::GcQuarantineResult, StorageError> {
            panic!("test-only read-only seam bridge: quarantine_blob must not be called")
        }
        async fn restore_quarantined_blob(
            &self,
            _: &crate::storage::GcMutationPermit<'_>,
            _: &Digest,
        ) -> Result<Option<u64>, StorageError> {
            panic!("test-only read-only seam bridge: restore_quarantined_blob must not be called")
        }
        async fn quarantined_blob_version(
            &self,
            _: &Digest,
        ) -> Result<Option<BlobObjectVersion>, StorageError> {
            panic!("test-only read-only seam bridge: quarantined_blob_version must not be called")
        }
        async fn delete_blob_conditional(
            &self,
            _: &crate::storage::GcMutationPermit<'_>,
            _: &Digest,
            _: Option<&BlobObjectVersion>,
        ) -> Result<crate::storage::GcDeleteResult, StorageError> {
            panic!("test-only read-only seam bridge: delete_blob_conditional must not be called")
        }
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn test_traverser_progression_over_seam_bridge() {
        let fixture = tempfile::tempdir().unwrap();
        let root = fixture.path().join("storage_root");
        std::fs::create_dir_all(&root).unwrap();

        let hex1 = "0a00000000000000000000000000000000000000000000000000000000000001";
        let hex2 = "0a00000000000000000000000000000000000000000000000000000000000002";
        let hex3 = "1b00000000000000000000000000000000000000000000000000000000000001";

        let blobs: [(&str, &[u8]); 3] = [(hex1, b"blob1"), (hex2, b"blob2_long"), (hex3, b"blob3")];
        for (hex, content) in blobs {
            let p2 = &hex[..2];
            let dir = root.join("blobs").join("sha256").join(p2);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(hex), content).unwrap();
        }

        let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");
        let bridge = SeamGcStorageBridge::new(&reader, default_test_budget());

        // Run CasBlobTraverser with batch size 2
        let mut traverser = CasBlobTraverser::new(&bridge, 2);

        // Batch 1
        let batch1 = traverser.next_batch().await.unwrap().expect("batch 1");
        assert_eq!(batch1.len(), 2);
        assert_eq!(batch1[0].digest.hex(), hex1);
        assert_eq!(batch1[0].size, 5);
        assert_eq!(batch1[1].digest.hex(), hex2);
        assert_eq!(batch1[1].size, 10);

        // Batch 2
        let batch2 = traverser.next_batch().await.unwrap().expect("batch 2");
        assert_eq!(batch2.len(), 1);
        assert_eq!(batch2[0].digest.hex(), hex3);
        assert_eq!(batch2[0].size, 5);

        // Batch 3: termination
        let batch3 = traverser.next_batch().await.unwrap();
        assert!(batch3.is_none());

        // Subsequent call remains None
        let batch4 = traverser.next_batch().await.unwrap();
        assert!(batch4.is_none());
    }

    struct ScriptedCursorAdapter {
        pages: Mutex<VecDeque<Result<GcBlobPage, StorageError>>>,
    }

    #[async_trait]
    impl crate::storage::ports::GcStoragePort for ScriptedCursorAdapter {
        fn kind(&self) -> &'static str {
            "scripted-cursor-adapter"
        }
        fn gc_strategy(&self) -> crate::storage::GcStorageStrategy {
            crate::storage::GcStorageStrategy::FilesystemQuarantine
        }
        async fn check_bucket_versioning_for_gc(&self) -> Result<(), StorageError> {
            Ok(())
        }
        async fn list_cas_blobs_page(
            &self,
            _: Option<&GcCursor>,
            _: usize,
        ) -> Result<GcBlobPage, StorageError> {
            self.pages.lock().unwrap().pop_front().unwrap()
        }
        async fn quarantine_blob(
            &self,
            _: &crate::storage::GcMutationPermit<'_>,
            _: &Digest,
            _: &BlobObjectVersion,
        ) -> Result<crate::storage::GcQuarantineResult, StorageError> {
            panic!("read-only")
        }
        async fn restore_quarantined_blob(
            &self,
            _: &crate::storage::GcMutationPermit<'_>,
            _: &Digest,
        ) -> Result<Option<u64>, StorageError> {
            panic!("read-only")
        }
        async fn quarantined_blob_version(
            &self,
            _: &Digest,
        ) -> Result<Option<BlobObjectVersion>, StorageError> {
            panic!("read-only")
        }
        async fn delete_blob_conditional(
            &self,
            _: &crate::storage::GcMutationPermit<'_>,
            _: &Digest,
            _: Option<&BlobObjectVersion>,
        ) -> Result<crate::storage::GcDeleteResult, StorageError> {
            panic!("read-only")
        }
    }

    fn dummy_candidate(hex: &str) -> GcBlobCandidate {
        GcBlobCandidate {
            digest: Digest::parse(&format!("sha256:{hex}")).unwrap(),
            size: 100,
            last_modified: SystemTime::UNIX_EPOCH,
            version: BlobObjectVersion("0:100".into()),
        }
    }

    #[tokio::test]
    async fn test_traverser_detects_cursor_cycle_and_repeated_cursor() {
        let hex1 = "0a00000000000000000000000000000000000000000000000000000000000001";
        let hex2 = "0a00000000000000000000000000000000000000000000000000000000000002";

        // Case 1: Repeated cursor
        {
            let adapter = ScriptedCursorAdapter {
                pages: Mutex::new(VecDeque::from([
                    Ok(GcBlobPage {
                        items: vec![dummy_candidate(hex1)],
                        next_cursor: Some(GcCursor(format!("sha256:{hex1}"))),
                    }),
                    Ok(GcBlobPage {
                        items: vec![dummy_candidate(hex1)],
                        next_cursor: Some(GcCursor(format!("sha256:{hex1}"))),
                    }),
                ])),
            };
            let mut traverser = CasBlobTraverser::new(&adapter, 1);
            let b1 = traverser.next_batch().await.unwrap();
            assert!(b1.is_some());
            let err = traverser.next_batch().await.unwrap_err();
            match err {
                GcPaginationError::RepeatedCursor(c) => assert_eq!(c, format!("sha256:{hex1}")),
                other => panic!("expected RepeatedCursor, got {other:?}"),
            }
        }

        // Case 2: Cursor cycle (A -> B -> A)
        {
            let adapter = ScriptedCursorAdapter {
                pages: Mutex::new(VecDeque::from([
                    Ok(GcBlobPage {
                        items: vec![dummy_candidate(hex1)],
                        next_cursor: Some(GcCursor(format!("sha256:{hex1}"))),
                    }),
                    Ok(GcBlobPage {
                        items: vec![dummy_candidate(hex2)],
                        next_cursor: Some(GcCursor(format!("sha256:{hex2}"))),
                    }),
                    Ok(GcBlobPage {
                        items: vec![dummy_candidate(hex1)],
                        next_cursor: Some(GcCursor(format!("sha256:{hex1}"))),
                    }),
                ])),
            };
            let mut traverser = CasBlobTraverser::new(&adapter, 1);
            let b1 = traverser.next_batch().await.unwrap();
            assert!(b1.is_some());
            let b2 = traverser.next_batch().await.unwrap();
            assert!(b2.is_some());
            let err = traverser.next_batch().await.unwrap_err();
            match err {
                GcPaginationError::CursorCycle(c) => assert_eq!(c, format!("sha256:{hex1}")),
                other => panic!("expected CursorCycle, got {other:?}"),
            }
        }
    }
}
