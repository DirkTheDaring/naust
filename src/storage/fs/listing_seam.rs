//! Test-only registry CAS listing integration seam using the committed
//! `storage-fs` directory enumeration API (`FsMetadataReader::enumerate_dir`).
//!
//! # Architectural Ownership Boundaries
//! - `storage-core`: Neutral storage contracts, [`storage_core::ObjectKey`],
//!   [`storage_core::ReadError`].
//! - `storage-fs`: Pinned root descriptor ownership, Linux `openat2` containment flags
//!   (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`), domain-free
//!   single-directory enumeration ([`storage_fs::DirEntry`], [`storage_fs::DirEntryType`],
//!   [`storage_fs::DirEnumerationLimits`], [`storage_fs::FsDirError`]).
//! - `registry-rust`: CAS namespace layout (`blobs/sha256/<2-char-prefix>/<64-char-hex>`),
//!   digest validation and normalization, lexical cursor comparisons, page-limit clamping [1, 1000],
//!   exact-full-page next-cursor calculation, error taxonomy translation ([`StorageError`]),
//!   and candidate translation.
//!
//! # Metadata & Version Compatibility Gap
//! In legacy production listing (`FsStorage::list_cas_blobs_page`), candidate size and modification
//! time (`mtime`) were obtained by calling uncontained `tokio::fs::metadata(&path)` on reconstructed
//! pathnames, from which version was computed as `BlobObjectVersion(format!("{mtime_secs}:{size}"))`.
//! In that legacy path, `modified()` failure fell back to `std::time::UNIX_EPOCH`, and pre-epoch
//! duration conversion defaulted to zero for the version's seconds component.
//!
//! The committed `storage-fs::enumerate_dir` API returns only [`storage_fs::DirEntry`], which exposes
//! raw directory entry names and point-in-time [`storage_fs::DirEntryType`]. It does **not** expose
//! file size, modification timestamp, or version identifiers.
//! Furthermore, [`storage_core::ObjectMetadata`] (from `FsMetadataReader::head`) exposes only `size: u64`,
//! not timestamps or version strings.
//!
//! In accordance with refactoring constraints:
//! 1. Metadata values (`size`, `mtime`, `version`) are **not** fabricated with synthetic or dummy values.
//! 2. Uncontained filesystem pathnames are **not** reopened or stat'd behind the reader's back.
//! 3. Neither `storage-core` nor `storage-fs` is extended in this slice.
//!
//! The seam therefore yields [`IncompleteGcCandidate`] items inside [`IncompleteGcBlobPage`], and
//! labels candidate translation explicitly via [`IncompleteCandidateTranslationGap`].
//!
//! # Containment & Behavioral Differences
//! - **Ancestor Symlink Rejection Beneath Storage Root**: Legacy listing traversed symlinks through
//!   any ancestor path components beneath the configured root (such as `blobs` or `blobs/sha256` pointing
//!   outside). The seam resolves paths beneath the already-pinned storage root descriptor with `openat2`
//!   containment flags (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`); symlinks
//!   encountered beneath the pinned descriptor are rejected with [`storage_fs::FsDirError::ResolutionRejected`]
//!   (translated to [`StorageErrorKind::Io`]). Note that opening the reader itself resolves the configured
//!   root path via standard OS resolution to obtain the initial `root_fd`.
//! - **Initial Directory Error Handling**: Legacy listing suppressed *all* initial directory metadata
//!   failures (including `ENOTDIR` and permission errors) into empty results. The seam distinguishes
//!   typed errors: only `NotFound` on `blobs/sha256` represents an absent CAS directory (returning empty
//!   results), while `NotADirectory` (mapped to [`StorageErrorKind::CorruptData`]), `PermissionDenied`,
//!   and other failures fail closed. All proposed error mappings from `FsDirError` to `StorageError`
//!   are test-seam proposals requiring a deliberate compatibility assessment before production cutover.
//! - **Enumeration Resource Limits**: Unlike public page limits which clamp to `[1, 1000]`, the seam
//!   requires explicit caller-supplied [`storage_fs::DirEnumerationLimits`]. These limits set per-enumeration
//!   entry count and total name-byte limits for each single `enumerate_dir` call, rather than a global
//!   bound on total seam memory or syscall duration. Shard directory naming does not impose an upper
//!   cardinality bound on valid CAS contents; production budget policy remains undecided. Budget exhaustion
//!   fails closed with [`storage_fs::FsDirError::LimitExceeded`] without partial results or synthetic
//!   continuation cursors.
//! - **Observations, Not Handles**: Directory entry types are point-in-time observations; separate
//!   enumeration and metadata calls do not establish snapshot isolation or guarantee identity across
//!   concurrent replacement. A concurrent replacement of a file or directory with another valid file
//!   or directory may be accessed by subsequent operations rather than detected.
//!
//! # Execution Constraints
//! This module is strictly test-only (`#[cfg(test)]`). No production caller may invoke this seam.

use std::sync::Arc;

use async_trait::async_trait;
use storage_core::ObjectKey;
use storage_fs::{DirEntry, DirEntryType, DirEnumerationLimits, FsDirError};

use crate::registry::digest::Digest;
use crate::storage::{GcBlobCandidate, GcCursor, StorageError, StorageErrorKind};

/// An enumerated CAS blob candidate before metadata inspection.
///
/// **Incomplete Candidate Translation**: `enumerate_dir` provides only the entry's raw name
/// and observed [`DirEntryType`]. It does not provide byte size, modification timestamp, or
/// object version required for a complete [`GcBlobCandidate`]. Values are not fabricated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IncompleteGcCandidate {
    pub digest: Digest,
    pub shard: String,
}

/// Typed indicator documenting the gap preventing full [`GcBlobCandidate`] translation.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "cannot translate candidate for {digest}: required size, mtime, and version are unavailable \
     from committed directory enumeration API without fabricating values or uncontained path reopening"
)]
pub struct IncompleteCandidateTranslationGap {
    pub digest: Digest,
}

impl IncompleteGcCandidate {
    /// Attempts conversion into legacy [`GcBlobCandidate`].
    ///
    /// Always fails with [`IncompleteCandidateTranslationGap`] because required metadata
    /// (size, mtime, version) is unavailable from directory enumeration alone.
    pub fn try_into_legacy_candidate(
        &self,
    ) -> Result<GcBlobCandidate, IncompleteCandidateTranslationGap> {
        Err(IncompleteCandidateTranslationGap {
            digest: self.digest.clone(),
        })
    }
}

/// A paginated page of incomplete CAS blob candidates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IncompleteGcBlobPage {
    pub items: Vec<IncompleteGcCandidate>,
    pub next_cursor: Option<GcCursor>,
}

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
impl<T: CasDirEnumerator + ?Sized> CasDirEnumerator for Arc<T> {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError> {
        (**self).enumerate_dir(target, limits).await
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

/// Executes paginated CAS blob listing through a directory enumerator seam.
///
/// # Arguments
/// - `enumerator`: Abstract directory enumerator (e.g. `FsMetadataReader` or fake).
/// - `cursor`: Optional continuation cursor from a preceding page.
/// - `limit`: Requested candidate limit, clamped to `[1, 1000]`.
/// - `budget`: Caller-supplied resource bounds for single-directory enumeration.
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
/// - Exact-full-page returns `Some(next_cursor)`; subsequent terminal page returns `None`.
/// - Budget exhaustion fails closed immediately without partial results.
pub async fn list_cas_blobs_page_seam(
    enumerator: &(impl CasDirEnumerator + ?Sized),
    cursor: Option<&GcCursor>,
    limit: usize,
    budget: DirEnumerationLimits,
) -> Result<IncompleteGcBlobPage, StorageError> {
    let max_limit = 1000;
    let limit = limit.min(max_limit).max(1);
    let cursor_str = cursor.map(|c| c.0.as_str());

    let cas_root_key = ObjectKey::parse("blobs/sha256")
        .map_err(|e| StorageError::internal(StorageErrorKind::InternalInvariant, e.to_string()))?;

    let root_entries = match enumerator.enumerate_dir(Some(&cas_root_key), budget).await {
        Ok(entries) => entries,
        Err(FsDirError::NotFound { .. }) => {
            return Ok(IncompleteGcBlobPage {
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

        let shard_entries = enumerator
            .enumerate_dir(Some(&shard_key), budget)
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

            candidates.push(IncompleteGcCandidate {
                digest,
                shard: p2.clone(),
            });

            if candidates.len() >= limit {
                next_cursor = Some(GcCursor(digest_str));
                break 'outer;
            }
        }
    }

    Ok(IncompleteGcBlobPage {
        items: candidates,
        next_cursor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;

    /// Recording fake enumerator for deterministic call order and failure injection.
    struct RecordingFakeDirEnumerator {
        calls: Mutex<Vec<(Option<ObjectKey>, DirEnumerationLimits)>>,
        responses: Mutex<HashMap<Option<ObjectKey>, VecDeque<Result<Vec<DirEntry>, FsDirError>>>>,
    }

    impl RecordingFakeDirEnumerator {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                responses: Mutex::new(HashMap::new()),
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
                panic!("unexpected call to RecordingFakeDirEnumerator with target: {target:?}")
            });
            queue
                .pop_front()
                .unwrap_or_else(|| panic!("no more scripted responses for target: {target:?}"))
        }
    }

    fn default_test_budget() -> DirEnumerationLimits {
        DirEnumerationLimits::new(1000, 100_000)
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
        // Subdirectory enumeration is suppressed
        assert_eq!(fake.called_targets(), vec![Some(root_key)]);
    }

    #[tokio::test]
    async fn test_fake_suppression_on_shard_failure() {
        let fake = RecordingFakeDirEnumerator::new();
        let root_key = cas_key("blobs/sha256");
        let shard_0a = cas_key("blobs/sha256/0a");
        let _shard_0b = cas_key("blobs/sha256/0b");

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
        // Shard 0b enumeration is suppressed after 0a failure
        assert_eq!(fake.called_targets(), vec![Some(root_key), Some(shard_0a)]);
    }

    #[tokio::test]
    async fn test_fake_typed_error_mappings() {
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
    async fn test_fake_budget_forwarded_to_enumerator() {
        let fake = RecordingFakeDirEnumerator::new();
        let root_key = cas_key("blobs/sha256");
        let shard_key = cas_key("blobs/sha256/0a");

        fake.script(
            Some(root_key.clone()),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(Some(shard_key.clone()), Ok(vec![]));

        let custom_budget = DirEnumerationLimits::new(42, 999);
        let _ = list_cas_blobs_page_seam(&fake, None, 10, custom_budget)
            .await
            .unwrap();

        let calls = fake.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], (Some(root_key), custom_budget));
        assert_eq!(calls[1], (Some(shard_key), custom_budget));
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
        assert_eq!(page1.items[1].digest.hex(), hex_0a2);
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
        assert_eq!(page2.next_cursor, None); // Under limit, next_cursor is None
    }

    #[tokio::test]
    async fn test_fake_limit_clamping_upper_bound_with_1001_candidates() {
        let fake = RecordingFakeDirEnumerator::new();
        let root_key = cas_key("blobs/sha256");
        let shard_key = cas_key("blobs/sha256/0a");

        // Generate 1,001 valid 64-hex blob entries for shard "0a"
        let mut entries = Vec::with_capacity(1001);
        let mut hexes = Vec::with_capacity(1001);
        for i in 0..1001 {
            let hex = format!("0a{:062x}", i);
            hexes.push(hex.clone());
            entries.push(DirEntry::new(hex.into(), DirEntryType::Regular));
        }

        // Script page 1: root directory returns "0a", shard returns all 1,001 entries
        fake.script(
            Some(root_key.clone()),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(Some(shard_key.clone()), Ok(entries.clone()));

        // Explicit budget large enough so it does not become the limiting factor
        let budget = DirEnumerationLimits::new(2000, 200_000);

        // Call with public limit = 50,000, which must clamp to 1,000
        let page1 = list_cas_blobs_page_seam(&fake, None, 50_000, budget)
            .await
            .expect("page 1 succeeds");

        // Page 1 contains exactly 1,000 ordered candidates
        assert_eq!(page1.items.len(), 1000);
        assert_eq!(page1.items[0].digest.hex(), hexes[0]);
        assert_eq!(page1.items[999].digest.hex(), hexes[999]);

        // Cursor identifies the last returned candidate (index 999)
        let exp_c1 = format!("sha256:{}", hexes[999]);
        assert_eq!(
            page1.next_cursor.as_ref().map(|c| c.0.as_str()),
            Some(exp_c1.as_str())
        );

        // Script page 2: same directory contents
        fake.script(
            Some(root_key.clone()),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(Some(shard_key.clone()), Ok(entries.clone()));

        // Resumption with cursor returns the remaining 1 candidate with correct termination (None)
        let page2 = list_cas_blobs_page_seam(&fake, page1.next_cursor.as_ref(), 50_000, budget)
            .await
            .expect("page 2 succeeds");

        assert_eq!(page2.items.len(), 1);
        assert_eq!(page2.items[0].digest.hex(), hexes[1000]);
        assert_eq!(page2.next_cursor, None);

        // Script page 3: resumption after termination returns empty page
        fake.script(
            Some(root_key.clone()),
            Ok(vec![DirEntry::new("0a".into(), DirEntryType::Directory)]),
        );
        fake.script(Some(shard_key.clone()), Ok(entries));

        let cursor_terminal = GcCursor(format!("sha256:{}", hexes[1000]));
        let page3 = list_cas_blobs_page_seam(&fake, Some(&cursor_terminal), 50_000, budget)
            .await
            .expect("page 3 succeeds");

        assert!(page3.items.is_empty());
        assert_eq!(page3.next_cursor, None);
    }

    #[tokio::test]
    async fn test_fake_direct_seam_multi_page_progression_and_terminal_behavior() {
        let fake = RecordingFakeDirEnumerator::new();
        let root_key = cas_key("blobs/sha256");
        let shard_key = cas_key("blobs/sha256/0a");
        let hex1 = "0a00000000000000000000000000000000000000000000000000000000000001";
        let hex2 = "0a00000000000000000000000000000000000000000000000000000000000002";

        // Script Page 1: 2 entries, next_cursor = sha256:hex2
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

        // Page 1: limit = 2
        let page1 = list_cas_blobs_page_seam(&fake, None, 2, default_test_budget())
            .await
            .expect("page 1 succeeds");
        assert_eq!(page1.items.len(), 2);
        assert_eq!(page1.items[0].digest.hex(), hex1);
        assert_eq!(page1.items[0].shard, "0a");
        assert_eq!(page1.items[1].digest.hex(), hex2);
        assert_eq!(page1.items[1].shard, "0a");
        assert_eq!(
            page1.next_cursor.as_ref().map(|c| c.0.as_str()),
            Some(format!("sha256:{hex2}").as_str())
        );

        // Verify that candidates remain incomplete and cannot produce legacy GcBlobCandidate
        assert!(page1.items[0].try_into_legacy_candidate().is_err());
        assert!(page1.items[1].try_into_legacy_candidate().is_err());

        // Script Page 2: with cursor hex2 -> returns empty items, next_cursor = None
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

        // Script Page 3: call after terminal returns empty
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

        let page3 =
            list_cas_blobs_page_seam(&fake, page1.next_cursor.as_ref(), 2, default_test_budget())
                .await
                .expect("page 3 succeeds");
        assert!(page3.items.is_empty());
        assert_eq!(page3.next_cursor, None);
    }

    // ========================================================================
    // Category B: Real Linux storage-fs Filesystem Tests
    // ========================================================================

    #[cfg(target_os = "linux")]
    mod linux_fs_tests {
        use super::*;
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
            assert_eq!(page1.items[1].digest.hex(), hexes[1]);
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
            assert_eq!(page2.items[1].digest.hex(), hexes[3]);
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

            // limit = 0 clamped to 1
            let page_zero = list_cas_blobs_page_seam(&reader, None, 0, default_test_budget())
                .await
                .expect("limit zero clamped to 1");
            assert_eq!(page_zero.items.len(), 1);
            assert_eq!(page_zero.items[0].digest.hex(), hex1);
        }

        #[tokio::test]
        async fn test_real_fs_cursor_lexical_filtering_and_malformed() {
            let (_fixture, root) = create_test_root();
            let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
            put_blob(&root, hex, b"p1");

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            // Cursor lexicographically greater than all items ("zzz") returns empty
            let cursor_zzz = GcCursor("zzz".into());
            let page_zzz =
                list_cas_blobs_page_seam(&reader, Some(&cursor_zzz), 10, default_test_budget())
                    .await
                    .expect("zzz cursor");
            assert!(page_zzz.items.is_empty());
            assert_eq!(page_zzz.next_cursor, None);

            // Cursor lexicographically less than all items ("aaa") returns all items
            let cursor_aaa = GcCursor("aaa".into());
            let page_aaa =
                list_cas_blobs_page_seam(&reader, Some(&cursor_aaa), 10, default_test_budget())
                    .await
                    .expect("aaa cursor");
            assert_eq!(page_aaa.items.len(), 1);
            assert_eq!(page_aaa.items[0].digest.hex(), hex);
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

            // In directory enumeration of blobs/sha256:
            // "2b" is observed as DirEntryType::Symlink by readdir/fstatat in enumerate_dir.
            // Registry type validation rejects it as CorruptData ("malformed non-directory entry in CAS prefix directory root").
            // It is not rejected by openat2 resolution as ResolutionRejected because enumerate_dir enumerates the root
            // directory rather than resolving paths through the symlinked shard.
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
            // Characterizes the core containment improvement over legacy listing:
            // Legacy listing resolved through ancestor symlinks via pathname resolution.
            // storage-fs openat2 containment strictly rejects ancestor symlinks!
            let (fixture, root) = create_test_root();
            let target_blobs = fixture.path().join("target_blobs");
            let target_cas = target_blobs.join("sha256").join("0a");
            std::fs::create_dir_all(&target_cas).unwrap();
            let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
            std::fs::write(target_cas.join(hex), b"payload").unwrap();

            // Make 'blobs' a symlink to target_blobs
            std::os::unix::fs::symlink(&target_blobs, root.join("blobs")).unwrap();

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            // In legacy listing, this succeeded. In storage-fs containment, it MUST fail closed.
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
            // Characterizes typed error propagation vs legacy silent suppression:
            // In legacy listing, if 'blobs' was a regular file, metadata("blobs/sha256")
            // returned an error which was silently converted to an empty page.
            // In the seam, NotADirectory fails closed as CorruptData!
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
            // Create 3 shards: 0a, 0b, 0c
            for p2 in ["0a", "0b", "0c"] {
                std::fs::create_dir_all(root.join("blobs").join("sha256").join(p2)).unwrap();
            }

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");

            // Budget allowing max 1 entry: enumeration of blobs/sha256 (3 shards) must fail closed
            let tight_budget = DirEnumerationLimits::new(1, 100_000);
            let res = list_cas_blobs_page_seam(&reader, None, 10, tight_budget).await;
            assert!(
                res.is_err(),
                "enumeration exceeding max_entries budget must fail closed"
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

            // Budget allowing max 2 name bytes: "0a" is 2 bytes, "0b" exceeds cumulative bytes
            let tight_budget = DirEnumerationLimits::new(100, 2);
            let res = list_cas_blobs_page_seam(&reader, None, 10, tight_budget).await;
            assert!(
                res.is_err(),
                "enumeration exceeding max_total_name_bytes must fail closed"
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
        async fn test_real_fs_zero_limit_empty_vs_non_empty() {
            let (_fixture, root) = create_test_root();
            let cas_root = root.join("blobs").join("sha256");
            std::fs::create_dir_all(&cas_root).unwrap();

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open root reader");
            let zero_budget = DirEnumerationLimits::new(0, 0);

            // Empty CAS root succeeds under zero limit
            let page_empty = list_cas_blobs_page_seam(&reader, None, 10, zero_budget)
                .await
                .expect("empty directory succeeds with zero limits");
            assert!(page_empty.items.is_empty());

            // Non-empty CAS root fails closed under zero limit
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

            // Page 1 with limit = 1 returns hex_0a
            let page1 = list_cas_blobs_page_seam(&reader, None, 1, default_test_budget())
                .await
                .expect("page 1");
            assert_eq!(page1.items.len(), 1);
            assert_eq!(page1.items[0].digest.hex(), hex_0a);
            let c1 = page1.next_cursor.expect("cursor present on full page");

            // Mutation between pages:
            // 1. Insert blob behind cursor ("0a...01" < "0a...02")
            let hex_behind = "0a00000000000000000000000000000000000000000000000000000000000001";
            put_blob(&root, hex_behind, b"behind");
            // 2. Insert blob ahead of cursor ("1b...02" > "0a...02")
            let hex_ahead = "1b00000000000000000000000000000000000000000000000000000000000002";
            put_blob(&root, hex_ahead, b"ahead");

            // Page 2 with cursor c1 and limit = 10
            let page2 = list_cas_blobs_page_seam(&reader, Some(&c1), 10, default_test_budget())
                .await
                .expect("page 2");

            let returned_hexes: Vec<String> = page2
                .items
                .iter()
                .map(|i| i.digest.hex().to_string())
                .collect();

            // Absence of snapshot isolation:
            // - The mutation behind the cursor was MISSED in this pagination traversal
            assert!(
                !returned_hexes.contains(&hex_behind.to_string()),
                "blob inserted behind cursor must be missed in current pagination cycle"
            );
            // - The mutations ahead of the cursor WERE OBSERVED
            assert!(
                returned_hexes.contains(&hex_1b.to_string()),
                "pre-existing blob ahead of cursor must be observed"
            );
            assert!(
                returned_hexes.contains(&hex_ahead.to_string()),
                "blob inserted ahead of cursor must be observed"
            );
        }
    }

    // ========================================================================
    // Category C: Metadata & Version Gap Evaluation Tests
    // ========================================================================

    #[tokio::test]
    async fn test_metadata_gap_incomplete_candidate_cannot_produce_gc_blob_candidate() {
        let digest = Digest::parse(
            "sha256:11223344556677889900aabbccddeeff11223344556677889900aabbccddeeff",
        )
        .unwrap();

        let candidate = IncompleteGcCandidate {
            digest: digest.clone(),
            shard: "11".into(),
        };

        // Attempting translation fails with typed error
        let err = candidate
            .try_into_legacy_candidate()
            .expect_err("candidate translation must fail because size/mtime/version are missing");

        assert_eq!(err, IncompleteCandidateTranslationGap { digest });
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn test_metadata_gap_head_lacks_mtime_and_version() {
        use storage_core::ObjectMetadataReader;

        let fixture = tempfile::tempdir().unwrap();
        let root = fixture.path().join("root");
        let hex = "11223344556677889900aabbccddeeff11223344556677889900aabbccddeeff";
        let blob_path = root.join("blobs").join("sha256").join("11").join(hex);
        std::fs::create_dir_all(blob_path.parent().unwrap()).unwrap();
        let payload = b"test blob payload with 31 bytes";
        std::fs::write(&blob_path, payload).unwrap();

        let reader = storage_fs::FsMetadataReader::open(&root).unwrap();
        let key = ObjectKey::parse(&format!("blobs/sha256/11/{hex}")).unwrap();

        // ObjectMetadataReader::head provides only size: u64
        let meta = reader.head(&key).await.expect("head succeeds");
        assert_eq!(meta.size(), payload.len() as u64);
        // There is NO timestamp or version method on ObjectMetadata!
        // Demonstrates the gap: even if head() is invoked per candidate,
        // mtime and version cannot be populated.
    }

    // ========================================================================
    // Category D: Independent CasBlobTraverser Tests (Scripted Fixtures Only)
    // ========================================================================
    // These tests verify the CasBlobTraverser batch progression, terminal behavior,
    // and cycle detection contracts in isolation using purely scripted GcStoragePort
    // mock fixtures. They do NOT wrap, invoke, or translate seam enumeration results,
    // preserving the boundary that seam candidates cannot produce GcBlobCandidate.

    struct ScriptedCursorAdapter {
        pages: Mutex<VecDeque<Result<crate::storage::GcBlobPage, StorageError>>>,
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
        ) -> Result<crate::storage::GcBlobPage, StorageError> {
            self.pages.lock().unwrap().pop_front().unwrap()
        }
        async fn quarantine_blob(
            &self,
            _: &crate::storage::GcMutationPermit<'_>,
            _: &Digest,
            _: &crate::storage::BlobObjectVersion,
        ) -> Result<crate::storage::GcQuarantineResult, StorageError> {
            Ok(crate::storage::GcQuarantineResult::Skipped)
        }
        async fn restore_quarantined_blob(
            &self,
            _: &crate::storage::GcMutationPermit<'_>,
            _: &Digest,
        ) -> Result<Option<u64>, StorageError> {
            Ok(None)
        }
        async fn quarantined_blob_version(
            &self,
            _: &Digest,
        ) -> Result<Option<crate::storage::BlobObjectVersion>, StorageError> {
            Ok(None)
        }
        async fn delete_blob_conditional(
            &self,
            _: &crate::storage::GcMutationPermit<'_>,
            _: &Digest,
            _: Option<&crate::storage::BlobObjectVersion>,
        ) -> Result<crate::storage::GcDeleteResult, StorageError> {
            Ok(crate::storage::GcDeleteResult::NotFound)
        }
    }

    fn dummy_candidate(hex: &str) -> crate::storage::GcBlobCandidate {
        crate::storage::GcBlobCandidate {
            digest: Digest::parse(&format!("sha256:{hex}")).unwrap(),
            size: 100,
            last_modified: std::time::UNIX_EPOCH,
            version: crate::storage::BlobObjectVersion("1:1".into()),
        }
    }

    #[tokio::test]
    async fn test_traverser_batch_progression_with_scripted_fixtures() {
        use crate::blob_gc::traverser::CasBlobTraverser;
        use crate::storage::GcBlobPage;

        let hex1 = "0a00000000000000000000000000000000000000000000000000000000000001";
        let hex2 = "0a00000000000000000000000000000000000000000000000000000000000002";

        let adapter = ScriptedCursorAdapter {
            pages: Mutex::new(VecDeque::from([
                Ok(GcBlobPage {
                    items: vec![dummy_candidate(hex1), dummy_candidate(hex2)],
                    next_cursor: Some(GcCursor(format!("sha256:{hex2}"))),
                }),
                Ok(GcBlobPage {
                    items: Vec::new(),
                    next_cursor: None,
                }),
            ])),
        };

        let mut traverser = CasBlobTraverser::new(&adapter, 2);

        // First batch
        let batch1 = traverser.next_batch().await.unwrap().expect("batch 1");
        assert_eq!(batch1.len(), 2);
        assert_eq!(batch1[0].digest.hex(), hex1);
        assert_eq!(batch1[1].digest.hex(), hex2);

        // Second batch: empty page terminates traverser
        let batch2 = traverser.next_batch().await.unwrap();
        assert!(batch2.is_none());

        // Subsequent call remains None
        let batch3 = traverser.next_batch().await.unwrap();
        assert!(batch3.is_none());
    }

    #[tokio::test]
    async fn test_traverser_detects_cursor_cycle_and_repeated_cursor() {
        use crate::blob_gc::traverser::{CasBlobTraverser, GcPaginationError};
        use crate::storage::GcBlobPage;

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
