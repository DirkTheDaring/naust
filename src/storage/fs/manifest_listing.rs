//! Contained filesystem manifest listing implementation for `registry-rust`.
//!
//! Provides descriptor-relative directory enumeration for `repos/<repo>/manifests`
//! beneath the pinned root directory descriptor using `storage_fs::FsMetadataReader`.
//!
//! # Architectural Boundaries
//! - **Test-Only Seam**: This module is scoped under `#[cfg(test)]` as an architectural
//!   seam. Production `FsStorage::list_manifest_digests_page` remains unmodified in this slice.
//! - **Descriptor Containment**: Uses Linux `openat2` on the pinned storage root with
//!   flags `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`.
//! - **Fail-Closed Repository Validation**: Enforces path-safety validation before key composition
//!   or filesystem interaction, even when `page_limit == 0`.
//! - **Syntactic Read Alignment**: Aligns listing output with the canonical read path convention
//!   (`repos/<repo>/manifests/<digest.hex()>`), without guaranteeing subsequent read success or
//!   snapshot consistency.
//!
//! # Operational Limitations & Environmental Dependencies
//! - **No Snapshot, Mount, or Hard-Link Isolation**: Listing observes directory entries at enumeration
//!   time; concurrent additions, removals, or hard-link replacements are not transactionally isolated.
//! - **Non-Guaranteed Subsequent Readability**: Canonical filename alignment does not guarantee that a
//!   listed digest remains readable or uncorrupted when subsequently read via `get_manifest`.
//! - **Root Replacement Divergence**: A pinned reader remains attached to the descriptor of the original
//!   storage root directory across external path renames or replacements; subsequent pathname-based mutations
//!   to the replacement directory tree are not observed by the pinned reader.
//! - **Procfs Dependency for Reads**: Reopening file descriptors from `O_PATH` handles (used during manifest
//!   reading by `FsMetadataReader`) requires a genuine, stable, accessible `/proc` filesystem.
//! - **System Call & Platform Boundaries**: Descriptor-relative directory containment requires the Linux
//!   `openat2` syscall with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`. Non-Linux
//!   platforms or execution environments returning `ENOSYS` fail closed with `Configuration` errors.
//!   External seccomp filter actions depend on policy configuration and are not assumed to map uniformly
//!   to a single errno.

#![cfg(test)]

use async_trait::async_trait;
use storage_core::ObjectKey;
use storage_fs::{DirEntry, DirEntryType, DirEnumerationLimits, FsDirError};

use crate::registry::digest::Digest;
use crate::storage::StorageError;

/// Explicitly provisional test fixture limits for manifest directory enumeration.
///
/// NOTE: These values are provisional test fixtures and do not represent approved
/// production defaults, measured capacity, or guaranteed scale.
pub const PROVISIONAL_TEST_MAX_MANIFEST_ENTRIES: usize = 10_000;
pub const PROVISIONAL_TEST_MAX_MANIFEST_NAME_BYTES: usize = 1_500_000;

/// Runtime helper to construct provisional test limits.
pub fn provisional_test_manifest_dir_limits() -> DirEnumerationLimits {
    DirEnumerationLimits::new(
        PROVISIONAL_TEST_MAX_MANIFEST_ENTRIES,
        PROVISIONAL_TEST_MAX_MANIFEST_NAME_BYTES,
    )
}

/// Narrow registry-owned test abstraction for descriptor-relative directory enumeration.
#[async_trait]
pub(crate) trait ManifestDirEnumerator: Send + Sync {
    /// Enumerates a directory relative to the storage root descriptor.
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError>;
}

#[async_trait]
impl ManifestDirEnumerator for storage_fs::FsMetadataReader {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError> {
        self.enumerate_dir(target, limits).await
    }
}

/// Validates repository name and composes the relative [`ObjectKey`] for `repos/<repo>/manifests`.
///
/// Enforces path safety constraints before key composition:
/// - Rejects empty repository strings.
/// - Rejects leading or trailing `/` characters.
/// - Rejects backslashes (`\\`), NUL bytes, and both ASCII and non-ASCII control characters.
/// - Rejects empty segments (consecutive slashes `//`).
/// - Rejects `.` and `..` segments.
///
/// Unsafe inputs fail closed with [`StorageError::InvalidRepoName`].
pub(crate) fn manifest_dir_key(repo: &str) -> Result<ObjectKey, StorageError> {
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
    if repo.contains(|c: char| c.is_control()) {
        return Err(StorageError::InvalidRepoName(
            "repository name cannot contain control characters".to_string(),
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

    let key_str = format!("repos/{repo}/manifests");
    ObjectKey::parse(&key_str).map_err(|e| StorageError::InvalidRepoName(e.to_string()))
}

/// Translates strongly typed [`FsDirError`] outcomes into [`StorageError`] aligned with CAS listing conventions.
pub(crate) fn translate_fs_dir_error(err: FsDirError) -> StorageError {
    match err {
        FsDirError::NotFound { .. } => StorageError::NotFound,
        FsDirError::NotADirectory { path } => {
            StorageError::corrupt_data(format!("target path is not a directory: {path:?}"))
        }
        FsDirError::PermissionDenied { source, .. } => {
            StorageError::permission_denied(source.to_string())
        }
        FsDirError::ResolutionRejected { source, .. } => StorageError::io(source.to_string()),
        FsDirError::SyscallUnsupported(source) => StorageError::configuration(format!(
            "openat2 is unavailable in this execution environment: {source}"
        )),
        FsDirError::PlatformUnsupported => StorageError::configuration(
            "platform unsupported: descriptor-relative containment requires Linux openat2",
        ),
        FsDirError::LimitExceeded { reason } => {
            StorageError::backend(format!("enumeration resource limit exceeded: {reason:?}"))
        }
        FsDirError::EntryDisappeared { name } => StorageError::io(format!(
            "directory entry disappeared during type inspection: {name:?}"
        )),
        FsDirError::Io { source } => StorageError::io(source.to_string()),
        FsDirError::RuntimeMissing(err) => {
            StorageError::backend(format!("tokio runtime missing: {err}"))
        }
        FsDirError::TaskJoinFailed(err) => {
            StorageError::backend(format!("blocking enumeration task join failed: {err}"))
        }
        other => StorageError::backend(format!("unexpected directory enumeration error: {other}")),
    }
}

/// Core implementation of contained manifest listing.
///
/// - Validates `repo` before checking `page_limit == 0`.
/// - For a valid `repo` with `page_limit == 0`, returns empty results without enumeration.
/// - Performs descriptor-relative enumeration of `repos/<repo>/manifests`.
/// - Filters for observed regular files matching canonical 64-hex SHA-256 or 128-hex SHA-512 filenames.
/// - Sorts in-place via derived `Ord` (`sort_unstable()`) and deduplicates.
/// - Evaluates continuation tokens using whole-string raw lexical comparison.
/// - Slices the page using saturating arithmetic.
pub(crate) async fn list_manifest_digests_page_impl(
    enumerator: &(impl ManifestDirEnumerator + ?Sized),
    repo: &str,
    continuation_token: Option<&str>,
    page_limit: usize,
    limits: DirEnumerationLimits,
) -> Result<(Vec<Digest>, Option<String>), StorageError> {
    let dir_key = manifest_dir_key(repo)?;
    if page_limit == 0 {
        return Ok((Vec::new(), None));
    }

    let entries = match enumerator.enumerate_dir(Some(&dir_key), limits).await {
        Ok(entries) => entries,
        Err(FsDirError::NotFound { .. }) => return Ok((Vec::new(), None)),
        Err(err) => return Err(translate_fs_dir_error(err)),
    };

    let mut all_digests: Vec<Digest> = Vec::new();
    for entry in entries {
        if entry.file_type() != DirEntryType::Regular {
            continue;
        }
        let Some(name) = entry.name().to_str() else {
            continue;
        };
        if name.starts_with(".tmp.") || name.starts_with(".lock.") {
            continue;
        }

        // Canonical hex validation: lowercase ascii hex only
        if !name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            continue;
        }

        if name.len() == 64 {
            if let Ok(digest) = Digest::parse(&format!("sha256:{name}")) {
                all_digests.push(digest);
            }
        } else if name.len() == 128 {
            if let Ok(digest) = Digest::parse(&format!("sha512:{name}")) {
                all_digests.push(digest);
            }
        }
    }

    all_digests.sort_unstable();
    all_digests.dedup();

    let start_idx = match continuation_token {
        None => 0,
        Some(token) => match all_digests.binary_search_by(|d| d.as_str().as_str().cmp(token)) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        },
    };

    let start = start_idx.min(all_digests.len());
    let end = start.saturating_add(page_limit).min(all_digests.len());
    let page = all_digests[start..end].to_vec();

    let next_token = if end < all_digests.len() {
        page.last().map(|d| d.as_str())
    } else {
        None
    };

    Ok((page, next_token))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageErrorKind;
    use async_trait::async_trait;
    use std::collections::{HashMap, VecDeque};
    use std::ffi::OsString;
    #[cfg(target_os = "linux")]
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use storage_fs::LimitExceededReason;

    struct RecordingFakeEnumerator {
        calls: Arc<Mutex<Vec<(Option<ObjectKey>, DirEnumerationLimits)>>>,
        responses:
            Arc<Mutex<HashMap<Option<ObjectKey>, VecDeque<Result<Vec<DirEntry>, FsDirError>>>>>,
    }

    impl RecordingFakeEnumerator {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                responses: Arc::new(Mutex::new(HashMap::new())),
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
    }

    #[async_trait]
    impl ManifestDirEnumerator for RecordingFakeEnumerator {
        async fn enumerate_dir(
            &self,
            target: Option<&ObjectKey>,
            limits: DirEnumerationLimits,
        ) -> Result<Vec<DirEntry>, FsDirError> {
            self.calls.lock().unwrap().push((target.cloned(), limits));
            let mut responses = self.responses.lock().unwrap();
            let queue = responses.get_mut(&target.cloned()).unwrap_or_else(|| {
                panic!("unexpected call to enumerate_dir with target: {target:?}")
            });
            queue
                .pop_front()
                .unwrap_or_else(|| panic!("no more scripted responses for target: {target:?}"))
        }
    }

    fn sha256_entry(hex: &str, file_type: DirEntryType) -> DirEntry {
        DirEntry::new(OsString::from(hex), file_type)
    }

    fn sha512_entry(hex: &str, file_type: DirEntryType) -> DirEntry {
        DirEntry::new(OsString::from(hex), file_type)
    }

    // --- Recording Fake Tests (Portable) ---

    #[tokio::test]
    async fn test_manifest_listing_fake_zero_limit_validation_order_and_no_io() {
        let fake = RecordingFakeEnumerator::new();
        let limits = DirEnumerationLimits::new(100, 1000);

        // 1. Invalid repo with zero limit: fails validation without calling enumerator
        let res = list_manifest_digests_page_impl(&fake, "../escape", None, 0, limits).await;
        assert!(matches!(res, Err(StorageError::InvalidRepoName(_))));
        assert_eq!(fake.calls().len(), 0);

        // 2. Empty repo with zero limit: fails validation
        let res = list_manifest_digests_page_impl(&fake, "", None, 0, limits).await;
        assert!(matches!(res, Err(StorageError::InvalidRepoName(_))));
        assert_eq!(fake.calls().len(), 0);

        // 3. Valid repo with zero limit: returns empty success with zero enumeration calls
        let res = list_manifest_digests_page_impl(&fake, "valid/repo", None, 0, limits).await;
        let (page, next_tok) = res.expect("valid repo zero limit succeeds");
        assert!(page.is_empty());
        assert!(next_tok.is_none());
        assert_eq!(fake.calls().len(), 0);
    }

    #[tokio::test]
    async fn test_manifest_listing_fake_repository_validation_edge_cases() {
        let fake = RecordingFakeEnumerator::new();
        let limits = DirEnumerationLimits::new(100, 1000);

        let invalid_names = [
            "",
            "/leading",
            "trailing/",
            "/both/",
            "back\\slash",
            "nul\0byte",
            "ctrl\x1fchar",
            "non_ascii_ctrl\u{0085}char",
            "consecutive//slashes",
            "dot/./segment",
            "dotdot/../segment",
            "..",
            ".",
        ];

        for name in invalid_names {
            let res = list_manifest_digests_page_impl(&fake, name, None, 10, limits).await;
            assert!(
                matches!(res, Err(StorageError::InvalidRepoName(_))),
                "expected InvalidRepoName for {name:?}, got {res:?}"
            );
        }
        assert_eq!(fake.calls().len(), 0);

        // 1. Nested repository is valid
        let target_key = ObjectKey::parse("repos/org/subteam/app/manifests").unwrap();
        fake.script(Some(target_key.clone()), Ok(vec![]));
        let res = list_manifest_digests_page_impl(&fake, "org/subteam/app", None, 10, limits).await;
        assert!(res.is_ok());
        let calls = fake.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, Some(target_key));
        assert_eq!(calls[0].1, limits);

        // 2. Linux C:/repo is valid and composes to repos/C:/repo/manifests
        let c_repo_key = ObjectKey::parse("repos/C:/repo/manifests").unwrap();
        fake.script(Some(c_repo_key.clone()), Ok(vec![]));
        let res = list_manifest_digests_page_impl(&fake, "C:/repo", None, 10, limits).await;
        assert!(res.is_ok());
        let calls = fake.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[1].0, Some(c_repo_key));
        assert_eq!(calls[1].1, limits);
    }

    #[tokio::test]
    async fn test_manifest_listing_fake_error_translations_aligned_with_cas_listing() {
        let fake = RecordingFakeEnumerator::new();
        let limits = DirEnumerationLimits::new(50, 500);
        let target_key = ObjectKey::parse("repos/testrepo/manifests").unwrap();

        // 1. NotFound -> returns empty page
        fake.script(
            Some(target_key.clone()),
            Err(FsDirError::NotFound {
                path: Some("repos/testrepo/manifests".to_string()),
            }),
        );
        let res = list_manifest_digests_page_impl(&fake, "testrepo", None, 10, limits).await;
        let (page, tok) = res.expect("NotFound maps to empty success");
        assert!(page.is_empty());
        assert!(tok.is_none());

        // 2. NotADirectory -> CorruptData
        fake.script(
            Some(target_key.clone()),
            Err(FsDirError::NotADirectory {
                path: Some("repos/testrepo/manifests".to_string()),
            }),
        );
        let res = list_manifest_digests_page_impl(&fake, "testrepo", None, 10, limits).await;
        match res.expect_err("NotADirectory must fail") {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, StorageErrorKind::CorruptData);
            }
            other => panic!("expected CorruptData, got {other:?}"),
        }

        // 3. PermissionDenied -> PermissionDenied
        fake.script(
            Some(target_key.clone()),
            Err(FsDirError::PermissionDenied {
                path: Some("repos/testrepo/manifests".to_string()),
                source: std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied"),
            }),
        );
        let res = list_manifest_digests_page_impl(&fake, "testrepo", None, 10, limits).await;
        match res.expect_err("PermissionDenied must fail") {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, StorageErrorKind::PermissionDenied);
            }
            other => panic!("expected PermissionDenied, got {other:?}"),
        }

        // 4. ResolutionRejected -> Io
        fake.script(
            Some(target_key.clone()),
            Err(FsDirError::ResolutionRejected {
                raw_os_error: libc::ELOOP,
                source: std::io::Error::from_raw_os_error(libc::ELOOP),
            }),
        );
        let res = list_manifest_digests_page_impl(&fake, "testrepo", None, 10, limits).await;
        match res.expect_err("ResolutionRejected must fail") {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, StorageErrorKind::Io);
            }
            other => panic!("expected Io, got {other:?}"),
        }

        // 5. SyscallUnsupported -> Configuration
        fake.script(
            Some(target_key.clone()),
            Err(FsDirError::SyscallUnsupported(
                std::io::Error::from_raw_os_error(libc::ENOSYS),
            )),
        );
        let res = list_manifest_digests_page_impl(&fake, "testrepo", None, 10, limits).await;
        match res.expect_err("SyscallUnsupported must fail") {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, StorageErrorKind::Configuration);
            }
            other => panic!("expected Configuration, got {other:?}"),
        }

        // 6. PlatformUnsupported -> Configuration
        fake.script(
            Some(target_key.clone()),
            Err(FsDirError::PlatformUnsupported),
        );
        let res = list_manifest_digests_page_impl(&fake, "testrepo", None, 10, limits).await;
        match res.expect_err("PlatformUnsupported must fail") {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, StorageErrorKind::Configuration);
            }
            other => panic!("expected Configuration, got {other:?}"),
        }

        // 7. LimitExceeded (MaxEntries) -> Backend
        fake.script(
            Some(target_key.clone()),
            Err(FsDirError::LimitExceeded {
                reason: LimitExceededReason::MaxEntries(50),
            }),
        );
        let res = list_manifest_digests_page_impl(&fake, "testrepo", None, 10, limits).await;
        match res.expect_err("LimitExceeded must fail") {
            StorageError::Internal { kind, message } => {
                assert_eq!(kind, StorageErrorKind::Backend);
                assert!(message.contains("MaxEntries"));
            }
            other => panic!("expected Backend, got {other:?}"),
        }

        // 8. LimitExceeded (MaxTotalNameBytes) -> Backend
        fake.script(
            Some(target_key.clone()),
            Err(FsDirError::LimitExceeded {
                reason: LimitExceededReason::MaxTotalNameBytes(500),
            }),
        );
        let res = list_manifest_digests_page_impl(&fake, "testrepo", None, 10, limits).await;
        match res.expect_err("LimitExceeded must fail") {
            StorageError::Internal { kind, message } => {
                assert_eq!(kind, StorageErrorKind::Backend);
                assert!(message.contains("MaxTotalNameBytes"));
            }
            other => panic!("expected Backend, got {other:?}"),
        }

        // 9. EntryDisappeared -> Io
        fake.script(
            Some(target_key.clone()),
            Err(FsDirError::EntryDisappeared {
                name: OsString::from("disappeared_file"),
            }),
        );
        let res = list_manifest_digests_page_impl(&fake, "testrepo", None, 10, limits).await;
        match res.expect_err("EntryDisappeared must fail") {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, StorageErrorKind::Io);
            }
            other => panic!("expected Io, got {other:?}"),
        }

        // 10. Io -> Io
        fake.script(
            Some(target_key.clone()),
            Err(FsDirError::Io {
                source: std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broken"),
            }),
        );
        let res = list_manifest_digests_page_impl(&fake, "testrepo", None, 10, limits).await;
        match res.expect_err("Io must fail") {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, StorageErrorKind::Io);
            }
            other => panic!("expected Io, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_manifest_listing_fake_genuine_runtime_and_join_error_fixtures() {
        let fake = RecordingFakeEnumerator::new();
        let limits = DirEnumerationLimits::new(10, 100);
        let target_key = ObjectKey::parse("repos/testrepo/manifests").unwrap();

        // 1. Genuine RuntimeMissing: captured on std::thread outside any tokio runtime
        let genuine_runtime_missing =
            std::thread::spawn(|| tokio::runtime::Handle::try_current().unwrap_err())
                .join()
                .expect("thread join");

        fake.script(
            Some(target_key.clone()),
            Err(FsDirError::RuntimeMissing(genuine_runtime_missing)),
        );
        let res = list_manifest_digests_page_impl(&fake, "testrepo", None, 10, limits).await;
        match res.expect_err("RuntimeMissing must fail") {
            StorageError::Internal { kind, message } => {
                assert_eq!(kind, StorageErrorKind::Backend);
                assert!(message.contains("tokio runtime missing"));
            }
            other => panic!("expected Backend, got {other:?}"),
        }

        // 2. Genuine TaskJoinFailed: captured by joining panicked task
        let genuine_join_error = tokio::task::spawn(async {
            panic!("simulated panic for genuine JoinError fixture");
        })
        .await
        .unwrap_err();

        fake.script(
            Some(target_key.clone()),
            Err(FsDirError::TaskJoinFailed(genuine_join_error)),
        );
        let res = list_manifest_digests_page_impl(&fake, "testrepo", None, 10, limits).await;
        match res.expect_err("TaskJoinFailed must fail") {
            StorageError::Internal { kind, message } => {
                assert_eq!(kind, StorageErrorKind::Backend);
                assert!(message.contains("blocking enumeration task join failed"));
            }
            other => panic!("expected Backend, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_manifest_listing_fake_filtering_parsing_and_deduplication() {
        let fake = RecordingFakeEnumerator::new();
        let limits = DirEnumerationLimits::new(100, 2000);
        let target_key = ObjectKey::parse("repos/testrepo/manifests").unwrap();

        let hex256_a = "1111111111111111111111111111111111111111111111111111111111111111";
        let hex256_b = "2222222222222222222222222222222222222222222222222222222222222222";
        let hex512_a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        let entries = vec![
            sha256_entry(hex256_a, DirEntryType::Regular),
            sha256_entry(hex256_a, DirEntryType::Regular), // duplicate
            sha256_entry(hex256_b, DirEntryType::Regular),
            sha512_entry(hex512_a, DirEntryType::Regular),
            sha512_entry(hex512_a, DirEntryType::Regular), // duplicate
            // Non-regular entry types (portable representation)
            sha256_entry(
                "3333333333333333333333333333333333333333333333333333333333333333",
                DirEntryType::Directory,
            ),
            sha256_entry(
                "4444444444444444444444444444444444444444444444444444444444444444",
                DirEntryType::Symlink,
            ),
            sha256_entry(
                "5555555555555555555555555555555555555555555555555555555555555555",
                DirEntryType::Other,
            ),
            // Non-canonical filenames: uppercase, prefixed, bad length, non-hex, temp/lock
            DirEntry::new(
                OsString::from("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                DirEntryType::Regular,
            ),
            DirEntry::new(
                OsString::from(
                    "sha256:1111111111111111111111111111111111111111111111111111111111111111",
                ),
                DirEntryType::Regular,
            ),
            DirEntry::new(OsString::from("1111"), DirEntryType::Regular),
            DirEntry::new(
                OsString::from("gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg"),
                DirEntryType::Regular,
            ),
            DirEntry::new(OsString::from(".tmp.upload_1234"), DirEntryType::Regular),
            DirEntry::new(OsString::from(".lock.manifest"), DirEntryType::Regular),
        ];

        fake.script(Some(target_key), Ok(entries));
        let (page, tok) = list_manifest_digests_page_impl(&fake, "testrepo", None, 10, limits)
            .await
            .expect("listing succeeds");

        // Exactly 3 distinct canonical digests should be returned:
        // sha256_a, sha256_b, sha512_a
        assert_eq!(page.len(), 3);
        assert_eq!(page[0].as_str(), format!("sha256:{hex256_a}"));
        assert_eq!(page[1].as_str(), format!("sha256:{hex256_b}"));
        assert_eq!(page[2].as_str(), format!("sha512:{hex512_a}"));
        assert!(tok.is_none());
    }

    #[tokio::test]
    async fn test_manifest_listing_fake_arbitrary_tokens_raw_string_comparison() {
        let fake = RecordingFakeEnumerator::new();
        let limits = DirEnumerationLimits::new(100, 2000);
        let target_key = ObjectKey::parse("repos/testrepo/manifests").unwrap();

        let hex_1 = "1111111111111111111111111111111111111111111111111111111111111111";
        let hex_5 = "5555555555555555555555555555555555555555555555555555555555555555";
        let hex_b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let hex_c = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

        let entries = vec![
            sha256_entry(hex_1, DirEntryType::Regular),
            sha256_entry(hex_5, DirEntryType::Regular),
            sha256_entry(hex_b, DirEntryType::Regular),
            sha256_entry(hex_c, DirEntryType::Regular),
        ];

        // 1. "sha2560:anything": In whole string comparison, '0' < ':', so "sha2560:anything" < "sha256:1111..."
        // Returns all 4 items starting at index 0
        fake.script(Some(target_key.clone()), Ok(entries.clone()));
        let (page, _) = list_manifest_digests_page_impl(
            &fake,
            "testrepo",
            Some("sha2560:anything"),
            10,
            limits,
        )
        .await
        .unwrap();
        assert_eq!(page.len(), 4);
        assert_eq!(page[0].hex(), hex_1);

        // 2. "sha256:anything": "sha256:5555..." < "sha256:anything" < "sha256:bbbb..." ('5' < 'a' < 'b')
        // Lands at insertion point index 2 (hex_b)
        fake.script(Some(target_key.clone()), Ok(entries.clone()));
        let (page, _) =
            list_manifest_digests_page_impl(&fake, "testrepo", Some("sha256:anything"), 10, limits)
                .await
                .unwrap();
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].hex(), hex_b);
        assert_eq!(page[1].hex(), hex_c);

        // 3. "sha256:abc:def": multiple colons token
        // "sha256:5555..." < "sha256:abc:def" < "sha256:bbbb..." ('5' < 'a' < 'b')
        // Lands at insertion point index 2 (hex_b)
        fake.script(Some(target_key.clone()), Ok(entries.clone()));
        let (page, _) =
            list_manifest_digests_page_impl(&fake, "testrepo", Some("sha256:abc:def"), 10, limits)
                .await
                .unwrap();
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].hex(), hex_b);
        assert_eq!(page[1].hex(), hex_c);

        // 4. Empty token "": sorts before all valid digests, returns all 4 items
        fake.script(Some(target_key.clone()), Ok(entries.clone()));
        let (page, _) = list_manifest_digests_page_impl(&fake, "testrepo", Some(""), 10, limits)
            .await
            .unwrap();
        assert_eq!(page.len(), 4);

        // 5. No-colon token "sha256": "sha256" < "sha256:", returns all 4 items
        fake.script(Some(target_key.clone()), Ok(entries.clone()));
        let (page, _) =
            list_manifest_digests_page_impl(&fake, "testrepo", Some("sha256"), 10, limits)
                .await
                .unwrap();
        assert_eq!(page.len(), 4);

        // 6. Uppercase token "SHA256:0000": 'S' < 's', returns all 4 items
        fake.script(Some(target_key.clone()), Ok(entries.clone()));
        let (page, _) =
            list_manifest_digests_page_impl(&fake, "testrepo", Some("SHA256:0000"), 10, limits)
                .await
                .unwrap();
        assert_eq!(page.len(), 4);

        // 7. Matching canonical token: resumes after match
        let token_match = format!("sha256:{hex_1}");
        fake.script(Some(target_key.clone()), Ok(entries.clone()));
        let (page, _) =
            list_manifest_digests_page_impl(&fake, "testrepo", Some(&token_match), 10, limits)
                .await
                .unwrap();
        assert_eq!(page.len(), 3);
        assert_eq!(page[0].hex(), hex_5);

        // 8. Previously issued canonical token (hex_5): resumes at hex_b
        let token_prev = format!("sha256:{hex_5}");
        fake.script(Some(target_key.clone()), Ok(entries.clone()));
        let (page, _) =
            list_manifest_digests_page_impl(&fake, "testrepo", Some(&token_prev), 10, limits)
                .await
                .unwrap();
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].hex(), hex_b);
        assert_eq!(page[1].hex(), hex_c);

        // 9. Token after all items: returns empty
        fake.script(Some(target_key), Ok(entries));
        let (page, tok) =
            list_manifest_digests_page_impl(&fake, "testrepo", Some("sha256:zzzz"), 10, limits)
                .await
                .unwrap();
        assert!(page.is_empty());
        assert!(tok.is_none());
    }

    #[tokio::test]
    async fn test_manifest_listing_fake_mixed_algorithm_full_traversal_progress() {
        let fake = RecordingFakeEnumerator::new();
        let limits = DirEnumerationLimits::new(100, 2000);
        let target_key = ObjectKey::parse("repos/testrepo/manifests").unwrap();

        let hex_512 = "11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111";
        let hex_256_a = "2222222222222222222222222222222222222222222222222222222222222222";
        let hex_256_b = "8888888888888888888888888888888888888888888888888888888888888888";

        let entries = vec![
            sha512_entry(hex_512, DirEntryType::Regular),
            sha256_entry(hex_256_a, DirEntryType::Regular),
            sha256_entry(hex_256_b, DirEntryType::Regular),
        ];

        // Derived Ord orders: "sha256:2222...", "sha256:8888...", "sha512:1111..."
        // Traverse with page_limit = 2
        fake.script(Some(target_key.clone()), Ok(entries.clone()));
        let (page1, tok1) = list_manifest_digests_page_impl(&fake, "testrepo", None, 2, limits)
            .await
            .unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].as_str(), format!("sha256:{hex_256_a}"));
        assert_eq!(page1[1].as_str(), format!("sha256:{hex_256_b}"));
        let token = tok1.expect("page 1 has next token");
        assert_eq!(token, format!("sha256:{hex_256_b}"));

        // Page 2
        fake.script(Some(target_key), Ok(entries));
        let (page2, tok2) =
            list_manifest_digests_page_impl(&fake, "testrepo", Some(&token), 2, limits)
                .await
                .unwrap();
        assert_eq!(page2.len(), 1);
        assert_eq!(page2[0].as_str(), format!("sha512:{hex_512}"));
        assert!(tok2.is_none());
    }

    #[tokio::test]
    async fn test_manifest_listing_fake_usize_max_saturating_addition() {
        let fake = RecordingFakeEnumerator::new();
        let limits = DirEnumerationLimits::new(100, 2000);
        let target_key = ObjectKey::parse("repos/testrepo/manifests").unwrap();

        let hex_1 = "1111111111111111111111111111111111111111111111111111111111111111";
        let hex_2 = "2222222222222222222222222222222222222222222222222222222222222222";
        let entries = vec![
            sha256_entry(hex_1, DirEntryType::Regular),
            sha256_entry(hex_2, DirEntryType::Regular),
        ];

        // With start_idx = 1 and page_limit = usize::MAX, start_idx.saturating_add(page_limit) does not panic
        let token = format!("sha256:{hex_1}");
        fake.script(Some(target_key), Ok(entries));
        let (page, tok) =
            list_manifest_digests_page_impl(&fake, "testrepo", Some(&token), usize::MAX, limits)
                .await
                .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].hex(), hex_2);
        assert!(tok.is_none());
    }

    // --- Real Filesystem Tests (Gated to Linux descriptor containment) ---

    #[cfg(target_os = "linux")]
    fn create_test_root() -> (tempfile::TempDir, PathBuf) {
        let fixture = tempfile::tempdir().expect("create tempdir");
        let root = fixture.path().join("storage_root");
        std::fs::create_dir_all(&root).expect("create storage root");
        (fixture, root)
    }

    #[cfg(target_os = "linux")]
    fn write_manifest_file(root: &Path, repo: &str, name: &str, content: &[u8]) {
        let dir = root.join("repos").join(repo).join("manifests");
        std::fs::create_dir_all(&dir).expect("create manifests dir");
        std::fs::write(dir.join(name), content).expect("write manifest file");
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn test_manifest_listing_real_fs_missing_and_empty_directories() {
        let (_fixture, root) = create_test_root();
        let reader = storage_fs::FsMetadataReader::open(&root).expect("open reader");
        let limits = provisional_test_manifest_dir_limits();

        // Missing repo directory -> empty success
        let (page, tok) =
            list_manifest_digests_page_impl(&reader, "missing_repo", None, 10, limits)
                .await
                .unwrap();
        assert!(page.is_empty());
        assert!(tok.is_none());

        // Repo exists, but manifests/ missing -> empty success
        std::fs::create_dir_all(root.join("repos").join("empty_repo")).unwrap();
        let (page, tok) = list_manifest_digests_page_impl(&reader, "empty_repo", None, 10, limits)
            .await
            .unwrap();
        assert!(page.is_empty());
        assert!(tok.is_none());

        // manifests/ exists but empty -> empty success
        std::fs::create_dir_all(root.join("repos").join("has_dir").join("manifests")).unwrap();
        let (page, tok) = list_manifest_digests_page_impl(&reader, "has_dir", None, 10, limits)
            .await
            .unwrap();
        assert!(page.is_empty());
        assert!(tok.is_none());
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn test_manifest_listing_real_fs_canonical_sha256_and_sha512_alignment() {
        let (_fixture, root) = create_test_root();
        let reader = storage_fs::FsMetadataReader::open(&root).expect("open reader");
        let limits = provisional_test_manifest_dir_limits();

        let hex256 = "1111111111111111111111111111111111111111111111111111111111111111";
        let hex512 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        let payload256 = br#"{"mediaType":"application/vnd.oci.image.manifest.v1+json"}"#;
        let payload512 = br#"{"mediaType":"application/vnd.oci.image.index.v1+json"}"#;

        write_manifest_file(&root, "myrepo", hex256, payload256);
        write_manifest_file(&root, "myrepo", hex512, payload512);

        let (page, tok) = list_manifest_digests_page_impl(&reader, "myrepo", None, 10, limits)
            .await
            .unwrap();
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].as_str(), format!("sha256:{hex256}"));
        assert_eq!(page[1].as_str(), format!("sha512:{hex512}"));
        assert!(tok.is_none());

        // Verify alignment with canonical get_manifest read path
        let (meta256, bytes256) =
            crate::storage::fs::manifest::get_manifest_impl(&reader, "myrepo", &page[0])
                .await
                .unwrap();
        assert_eq!(
            meta256.media_type,
            "application/vnd.oci.image.manifest.v1+json"
        );
        assert_eq!(&bytes256[..], payload256);

        let (meta512, bytes512) =
            crate::storage::fs::manifest::get_manifest_impl(&reader, "myrepo", &page[1])
                .await
                .unwrap();
        assert_eq!(
            meta512.media_type,
            "application/vnd.oci.image.index.v1+json"
        );
        assert_eq!(&bytes512[..], payload512);
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn test_manifest_listing_real_fs_filtering_non_regular_and_non_canonical() {
        let (_fixture, root) = create_test_root();
        let reader = storage_fs::FsMetadataReader::open(&root).expect("open reader");
        let limits = provisional_test_manifest_dir_limits();

        let hex256 = "2222222222222222222222222222222222222222222222222222222222222222";
        write_manifest_file(&root, "filter_repo", hex256, b"{}");

        let manifests_dir = root.join("repos").join("filter_repo").join("manifests");

        // Subdirectory inside manifests/
        std::fs::create_dir_all(
            manifests_dir.join("3333333333333333333333333333333333333333333333333333333333333333"),
        )
        .unwrap();

        // Symlink inside manifests/ (fully evaluated on Linux)
        std::os::unix::fs::symlink(
            manifests_dir.join(hex256),
            manifests_dir.join("4444444444444444444444444444444444444444444444444444444444444444"),
        )
        .unwrap();

        // Uppercase hex file
        std::fs::write(
            manifests_dir.join("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            b"{}",
        )
        .unwrap();

        // Non-hex chars
        std::fs::write(
            manifests_dir.join("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"),
            b"{}",
        )
        .unwrap();

        // Wrong length (63 bytes and 65 bytes)
        std::fs::write(
            manifests_dir.join("111111111111111111111111111111111111111111111111111111111111111"),
            b"{}",
        )
        .unwrap();
        std::fs::write(
            manifests_dir.join("11111111111111111111111111111111111111111111111111111111111111111"),
            b"{}",
        )
        .unwrap();

        // Temporary and lock files
        std::fs::write(manifests_dir.join(".tmp.upload_in_progress"), b"{}").unwrap();
        std::fs::write(manifests_dir.join(".lock.exclusive"), b"{}").unwrap();

        let (page, tok) = list_manifest_digests_page_impl(&reader, "filter_repo", None, 10, limits)
            .await
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].hex(), hex256);
        assert!(tok.is_none());
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn test_manifest_listing_real_fs_wrong_type_and_symlinked_components() {
        let (_fixture, root) = create_test_root();
        let reader = storage_fs::FsMetadataReader::open(&root).expect("open reader");
        let limits = provisional_test_manifest_dir_limits();

        // 1. manifests is a regular file, not a directory -> NotADirectory -> CorruptData
        let bad_repo = root.join("repos").join("bad_repo");
        std::fs::create_dir_all(&bad_repo).unwrap();
        std::fs::write(bad_repo.join("manifests"), b"not a dir").unwrap();

        let res = list_manifest_digests_page_impl(&reader, "bad_repo", None, 10, limits).await;
        match res.expect_err("regular file manifests must fail with CorruptData") {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, StorageErrorKind::CorruptData);
            }
            other => panic!("expected CorruptData, got {other:?}"),
        }

        // 2. manifests is a symlink to another directory -> openat2 rejects with ResolutionRejected -> Io
        let symlink_repo = root.join("repos").join("symlink_repo");
        std::fs::create_dir_all(&symlink_repo).unwrap();
        let external_dir = root.join("external_manifests");
        std::fs::create_dir_all(&external_dir).unwrap();
        std::os::unix::fs::symlink(&external_dir, symlink_repo.join("manifests")).unwrap();

        let res = list_manifest_digests_page_impl(&reader, "symlink_repo", None, 10, limits).await;
        match res.expect_err("symlink manifests must be rejected by containment") {
            StorageError::Internal { kind, .. } => {
                assert_eq!(kind, StorageErrorKind::Io);
            }
            other => panic!("expected Io for ResolutionRejected, got {other:?}"),
        }
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn test_manifest_listing_real_fs_budget_boundaries_and_ignored_names_consumption() {
        let (_fixture, root) = create_test_root();
        let reader = storage_fs::FsMetadataReader::open(&root).expect("open reader");

        let hex_a = "1111111111111111111111111111111111111111111111111111111111111111";
        let hex_b = "2222222222222222222222222222222222222222222222222222222222222222";
        write_manifest_file(&root, "budget_repo", hex_a, b"{}");
        write_manifest_file(&root, "budget_repo", hex_b, b"{}");

        // Two files in dir, total raw name bytes = 64 + 64 = 128 bytes.

        // 1. Entry-count boundary:
        // Below boundary: 3 entries > 2 -> succeeds
        let limit_below_count = DirEnumerationLimits::new(3, 1000);
        let (page, _) =
            list_manifest_digests_page_impl(&reader, "budget_repo", None, 10, limit_below_count)
                .await
                .unwrap();
        assert_eq!(page.len(), 2);

        // Exactly at boundary: 2 entries == 2 -> succeeds
        let limit_exact_count = DirEnumerationLimits::new(2, 1000);
        let (page, _) =
            list_manifest_digests_page_impl(&reader, "budget_repo", None, 10, limit_exact_count)
                .await
                .unwrap();
        assert_eq!(page.len(), 2);

        // Above boundary: 1 entry < 2 -> fails with LimitExceeded(MaxEntries) -> Backend
        let limit_above_count = DirEnumerationLimits::new(1, 1000);
        let res =
            list_manifest_digests_page_impl(&reader, "budget_repo", None, 10, limit_above_count)
                .await;
        match res.expect_err("1 entry limit must fail") {
            StorageError::Internal { kind, message } => {
                assert_eq!(kind, StorageErrorKind::Backend);
                assert!(message.contains("MaxEntries"));
            }
            other => panic!("expected Backend, got {other:?}"),
        }

        // 2. Raw-name-byte boundary:
        // Below boundary: 129 bytes > 128 bytes -> succeeds
        let limit_below_bytes = DirEnumerationLimits::new(10, 129);
        let (page, _) =
            list_manifest_digests_page_impl(&reader, "budget_repo", None, 10, limit_below_bytes)
                .await
                .unwrap();
        assert_eq!(page.len(), 2);

        // Exactly at boundary: 128 bytes == 128 bytes -> succeeds
        let limit_exact_bytes = DirEnumerationLimits::new(10, 128);
        let (page, _) =
            list_manifest_digests_page_impl(&reader, "budget_repo", None, 10, limit_exact_bytes)
                .await
                .unwrap();
        assert_eq!(page.len(), 2);

        // Above boundary: 127 bytes < 128 bytes -> fails with LimitExceeded(MaxTotalNameBytes) -> Backend
        let limit_above_bytes = DirEnumerationLimits::new(10, 127);
        let res =
            list_manifest_digests_page_impl(&reader, "budget_repo", None, 10, limit_above_bytes)
                .await;
        match res.expect_err("127 bytes limit must fail") {
            StorageError::Internal { kind, message } => {
                assert_eq!(kind, StorageErrorKind::Backend);
                assert!(message.contains("MaxTotalNameBytes"));
            }
            other => panic!("expected Backend, got {other:?}"),
        }

        // 3. Ignored entries consuming enumeration budgets:
        // Add a temporary file `.tmp.1` (name length 6 bytes). Total entries = 3, total name bytes = 134 bytes.
        let manifests_dir = root.join("repos").join("budget_repo").join("manifests");
        std::fs::write(manifests_dir.join(".tmp.1"), b"").unwrap();

        // Under limit_exact_count (max 2 entries), enumeration fails on the 3rd entry encountered,
        // proving that non-manifest entries consume entry count budget!
        let res =
            list_manifest_digests_page_impl(&reader, "budget_repo", None, 10, limit_exact_count)
                .await;
        assert!(matches!(
            res,
            Err(StorageError::Internal {
                kind: StorageErrorKind::Backend,
                ..
            })
        ));

        // Under limit_exact_bytes (max 128 bytes), enumeration fails when total name bytes exceed 128,
        // proving that non-manifest entries consume raw name bytes budget!
        let res =
            list_manifest_digests_page_impl(&reader, "budget_repo", None, 10, limit_exact_bytes)
                .await;
        assert!(matches!(
            res,
            Err(StorageError::Internal {
                kind: StorageErrorKind::Backend,
                ..
            })
        ));
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn test_manifest_listing_real_fs_shared_root_pinning_across_rename_and_replacement() {
        let (fixture, root) = create_test_root();
        let hex_orig = "1111111111111111111111111111111111111111111111111111111111111111";
        let hex_repl = "2222222222222222222222222222222222222222222222222222222222222222";

        write_manifest_file(&root, "pin_repo", hex_orig, b"{}");

        // Open reader pinned to original root descriptor
        let reader = storage_fs::FsMetadataReader::open(&root).expect("open reader");
        let limits = provisional_test_manifest_dir_limits();

        // Verify initial listing observes hex_orig
        let (page1, _) = list_manifest_digests_page_impl(&reader, "pin_repo", None, 10, limits)
            .await
            .unwrap();
        assert_eq!(page1.len(), 1);
        assert_eq!(page1[0].hex(), hex_orig);

        // Rename root to root.old, and create a fresh directory at root with hex_repl
        let renamed_root = fixture.path().join("storage_root_renamed");
        std::fs::rename(&root, &renamed_root).expect("rename root");
        std::fs::create_dir_all(&root).expect("create replacement root");
        write_manifest_file(&root, "pin_repo", hex_repl, b"{}");

        // The pinned reader continues resolving relative to the original pinned directory descriptor!
        // It observes hex_orig, proving root pinning across path replacement.
        let (page_pinned, _) =
            list_manifest_digests_page_impl(&reader, "pin_repo", None, 10, limits)
                .await
                .unwrap();
        assert_eq!(page_pinned.len(), 1);
        assert_eq!(page_pinned[0].hex(), hex_orig);
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn test_manifest_listing_real_fs_sequential_changes_between_listing_and_reading() {
        let (_fixture, root) = create_test_root();
        let reader = storage_fs::FsMetadataReader::open(&root).expect("open reader");
        let limits = provisional_test_manifest_dir_limits();

        let hex = "1111111111111111111111111111111111111111111111111111111111111111";
        write_manifest_file(&root, "seq_repo", hex, b"{}");

        // 1. List discovers hex
        let (page, _) = list_manifest_digests_page_impl(&reader, "seq_repo", None, 10, limits)
            .await
            .unwrap();
        assert_eq!(page.len(), 1);
        let digest = page[0].clone();

        // 2. Sequentially remove the manifest file
        let path = root
            .join("repos")
            .join("seq_repo")
            .join("manifests")
            .join(hex);
        std::fs::remove_file(path).unwrap();

        // 3. Attempting get_manifest fails with NotFound, proving lack of guaranteed subsequent readability
        let res =
            crate::storage::fs::manifest::get_manifest_impl(&reader, "seq_repo", &digest).await;
        assert!(matches!(res, Err(StorageError::NotFound)));
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    #[ignore = "requires unprivileged user environment where chmod 0o000 denies filesystem access"]
    async fn test_manifest_listing_real_fs_permission_denied_restoration_guard() {
        use std::os::unix::fs::PermissionsExt;

        if unsafe { libc::geteuid() } == 0 {
            panic!("ineffective permissions: running as root (UID 0) bypasses DAC");
        }

        let (_fixture, root) = create_test_root();
        let repo = "perm_repo";
        let manifests_dir = root.join("repos").join(repo).join("manifests");
        std::fs::create_dir_all(&manifests_dir).expect("create manifests dir");
        std::fs::write(
            manifests_dir.join("1111111111111111111111111111111111111111111111111111111111111111"),
            b"{}",
        )
        .expect("write manifest file");

        let orig_perms = std::fs::metadata(&manifests_dir).unwrap().permissions();

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
                path: &manifests_dir,
                original_permissions: orig_perms.clone(),
            };

            std::fs::set_permissions(&manifests_dir, std::fs::Permissions::from_mode(0o000))
                .expect("set mode 0o000");

            // Fail-fast assertion: verify permissions actually deny access with PermissionDenied
            match std::fs::read_dir(&manifests_dir) {
                Ok(_) => {
                    panic!("ineffective permissions: std::fs::read_dir succeeded under mode 0o000");
                }
                Err(err) => assert_eq!(
                    err.kind(),
                    std::io::ErrorKind::PermissionDenied,
                    "expected PermissionDenied error under mode 0o000, got: {err:?}"
                ),
            }

            let reader = storage_fs::FsMetadataReader::open(&root).expect("open reader");
            let limits = provisional_test_manifest_dir_limits();
            let res = list_manifest_digests_page_impl(&reader, repo, None, 10, limits).await;
            match res.expect_err("permission denied must fail closed") {
                StorageError::Internal { kind, .. } => {
                    assert_eq!(kind, StorageErrorKind::PermissionDenied);
                }
                other => panic!("expected PermissionDenied, got {other:?}"),
            }
        }

        // On normal execution, explicitly restore and verify original permissions before fixture cleanup
        let restored_perms = std::fs::metadata(&manifests_dir).unwrap().permissions();
        assert_eq!(restored_perms.mode(), orig_perms.mode());
    }
}
