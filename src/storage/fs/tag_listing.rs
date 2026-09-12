//! Contained filesystem tag listing implementation.
//!
//! Provides descriptor-relative contained tag listing operations (`contained_list_tags_seam`
//! and `contained_list_tags_page_seam`) routing production `FsStorage::list_tags` and
//! `FsStorage::list_tags_page` through `storage-fs` and `storage-core`.
//!
//! # Safety & Containment
//! Path traversal is prevented through upfront structural path validation and Linux
//! `openat2` resolution flags (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`)
//! anchored to the storage root descriptor.

use crate::registry::digest::Digest;
use crate::storage::StorageError;
use async_trait::async_trait;
use storage_core::{ObjectKey, ObjectPayloadReader, ReadError};
use storage_fs::{DirEntry, DirEntryType, DirEnumerationLimits, FsDirError};

/// Default maximum number of tag directory entries to enumerate during tag listing.
pub const DEFAULT_TAG_LISTING_MAX_ENTRIES: usize = 10_000;
/// Default maximum cumulative bytes of entry filenames during tag listing.
pub const DEFAULT_TAG_LISTING_MAX_NAME_BYTES: usize = 1_500_000;
/// Default maximum number of directory entries to inspect when probing repo existence on tags NotFound.
pub const DEFAULT_TAG_LISTING_REPO_PROBE_MAX_ENTRIES: usize = 64;
/// Default maximum cumulative filename bytes when probing repo existence.
pub const DEFAULT_TAG_LISTING_REPO_PROBE_MAX_NAME_BYTES: usize = 4_096;
/// Default maximum candidate payload size in bytes for paged tag listing.
pub const DEFAULT_TAG_LISTING_MAX_PAYLOAD_BYTES: u64 = 1_024;

/// Minimum allowable value for `tag_listing_max_entries`.
pub const MIN_TAG_LISTING_ENTRIES: usize = 1;
/// Minimum allowable value for `tag_listing_max_name_bytes` (accommodates a 128-byte tag name).
pub const MIN_TAG_LISTING_NAME_BYTES: usize = 128;
/// Minimum allowable value for `tag_listing_repo_probe_max_entries`.
pub const MIN_TAG_LISTING_REPO_PROBE_ENTRIES: usize = 1;
/// Minimum allowable value for `tag_listing_repo_probe_max_name_bytes`.
pub const MIN_TAG_LISTING_REPO_PROBE_NAME_BYTES: usize = 64;
/// Minimum allowable value for `tag_listing_max_payload_bytes` (accommodates SHA-512 text and whitespace).
pub const MIN_TAG_LISTING_PAYLOAD_BYTES: u64 = 256;

/// Configured resource limits for contained filesystem tag listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagListingLimits {
    pub repo_probe_limits: storage_fs::DirEnumerationLimits,
    pub tags_dir_limits: storage_fs::DirEnumerationLimits,
    pub payload_limits: super::tag_read::TagReadLimits,
}

impl Default for TagListingLimits {
    fn default() -> Self {
        Self {
            repo_probe_limits: storage_fs::DirEnumerationLimits::new(
                DEFAULT_TAG_LISTING_REPO_PROBE_MAX_ENTRIES,
                DEFAULT_TAG_LISTING_REPO_PROBE_MAX_NAME_BYTES,
            ),
            tags_dir_limits: storage_fs::DirEnumerationLimits::new(
                DEFAULT_TAG_LISTING_MAX_ENTRIES,
                DEFAULT_TAG_LISTING_MAX_NAME_BYTES,
            ),
            payload_limits: super::tag_read::TagReadLimits {
                max_payload_bytes: Some(DEFAULT_TAG_LISTING_MAX_PAYLOAD_BYTES),
            },
        }
    }
}

impl TagListingLimits {
    #[allow(dead_code)]
    pub fn new(
        repo_probe_limits: storage_fs::DirEnumerationLimits,
        tags_dir_limits: storage_fs::DirEnumerationLimits,
        payload_limits: super::tag_read::TagReadLimits,
    ) -> Self {
        Self {
            repo_probe_limits,
            tags_dir_limits,
            payload_limits,
        }
    }
}

/// Test-seam directory enumeration abstraction over contained storage readers.
#[async_trait]
pub(crate) trait TagDirEnumerator: Send + Sync {
    /// Enumerates a directory relative to the pinned storage root descriptor.
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError>;
}

#[async_trait]
impl TagDirEnumerator for storage_fs::FsMetadataReader {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError> {
        self.enumerate_dir(target, limits).await
    }
}

/// Test-local path component validator mirroring the rules of `tag_read::validate_path_component`.
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

/// Translates directory enumeration errors from [`FsDirError`] into [`StorageError`].
pub(crate) fn translate_tag_dir_error(err: FsDirError, dir_key: &str) -> StorageError {
    match err {
        FsDirError::NotFound { .. } => {
            StorageError::io(format!("directory vanished before enumeration: {dir_key}"))
        }
        FsDirError::NotADirectory { path } => {
            StorageError::corrupt_data(format!("path is not a directory ({path:?}): {dir_key}"))
        }
        FsDirError::PermissionDenied { source, .. } => StorageError::permission_denied(format!(
            "permission denied enumerating directory {dir_key}: {source}"
        )),
        FsDirError::ResolutionRejected { source, .. } => StorageError::io(format!(
            "path resolution rejected for directory {dir_key}: {source}"
        )),
        FsDirError::SyscallUnsupported(source) => StorageError::configuration(format!(
            "openat2 is unavailable in this execution environment for {dir_key}: {source}"
        )),
        FsDirError::PlatformUnsupported => StorageError::configuration(format!(
            "platform unsupported: descriptor-relative containment requires Linux openat2 for {dir_key}"
        )),
        FsDirError::LimitExceeded { reason } => StorageError::backend(format!(
            "directory enumeration resource limit exceeded for {dir_key}: {reason:?}"
        )),
        FsDirError::EntryDisappeared { name } => StorageError::io(format!(
            "directory entry disappeared during inspection in {dir_key}: {name:?}"
        )),
        FsDirError::Io { source } => StorageError::io(format!(
            "I/O error enumerating directory {dir_key}: {source}"
        )),
        FsDirError::RuntimeMissing(err) => StorageError::backend(format!(
            "tokio runtime missing during enumeration of {dir_key}: {err}"
        )),
        FsDirError::TaskJoinFailed(err) => StorageError::backend(format!(
            "blocking enumeration task join failed for {dir_key}: {err}"
        )),
        other => StorageError::backend(format!(
            "unexpected directory enumeration error for {dir_key}: {other}"
        )),
    }
}

fn tags_dir_key(repo: &str) -> Result<ObjectKey, StorageError> {
    validate_path_component(repo, "repository name")?;
    let key_str = format!("repos/{repo}/tags");
    ObjectKey::parse(&key_str).map_err(|e| StorageError::InvalidRepoName(e.to_string()))
}

fn repo_key(repo: &str) -> Result<ObjectKey, StorageError> {
    validate_path_component(repo, "repository name")?;
    let key_str = format!("repos/{repo}");
    ObjectKey::parse(&key_str).map_err(|e| StorageError::InvalidRepoName(e.to_string()))
}

/// Test seam for contained name-only tag listing (`list_tags`).
pub(crate) async fn contained_list_tags_seam<D>(
    dir_enumerator: &D,
    repo_name: &str,
    repo_probe_limits: DirEnumerationLimits,
    tags_dir_limits: DirEnumerationLimits,
) -> Result<Vec<String>, StorageError>
where
    D: TagDirEnumerator + ?Sized,
{
    // Step 1: Input validation before any filesystem access
    let tags_key = tags_dir_key(repo_name)?;
    let rep_key = repo_key(repo_name)?;

    // Step 2: Enumerate repos/<repo>/tags beneath pinned root
    let entries = match dir_enumerator
        .enumerate_dir(Some(&tags_key), tags_dir_limits)
        .await
    {
        Ok(entries) => entries,
        Err(FsDirError::NotFound { .. }) => {
            // Step 3: Probe repos/<repo> with repo_probe_limits
            match dir_enumerator
                .enumerate_dir(Some(&rep_key), repo_probe_limits)
                .await
            {
                Ok(_) => {
                    // Repository exists, but tags/ directory is missing
                    return Ok(Vec::new());
                }
                Err(FsDirError::NotFound { .. }) => {
                    // Repository does not exist
                    return Err(StorageError::NotFound);
                }
                Err(err) => {
                    return Err(translate_tag_dir_error(err, rep_key.as_str()));
                }
            }
        }
        Err(err) => {
            return Err(translate_tag_dir_error(err, tags_key.as_str()));
        }
    };

    // Step 4: Candidate filtering
    let mut tags: Vec<String> = Vec::new();
    for entry in entries {
        // Skip dotfiles (such as .tmp.*, .lock.*, .*)
        let Some(name) = entry.name().to_str() else {
            // Skip non-UTF-8 entries
            continue;
        };
        if name.starts_with('.') {
            continue;
        }
        // Retain only entries observed as Regular
        if entry.file_type() != DirEntryType::Regular {
            continue;
        }
        tags.push(name.to_string());
    }

    // Step 5: In-place lexical sort returning sorted tag names
    tags.sort_unstable();
    Ok(tags)
}

/// Test seam for contained digest-bearing paginated tag listing (`list_tags_page`).
pub(crate) async fn contained_list_tags_page_seam<D, P>(
    dir_enumerator: &D,
    payload_reader: &P,
    repo_name: &str,
    cursor: Option<&str>,
    page_limit: usize,
    repo_probe_limits: DirEnumerationLimits,
    tags_dir_limits: DirEnumerationLimits,
    payload_limits: super::tag_read::TagReadLimits,
) -> Result<(Vec<(String, Digest)>, Option<String>), StorageError>
where
    D: TagDirEnumerator + ?Sized,
    P: ObjectPayloadReader + ?Sized,
{
    // Step 1: Input validation before any filesystem access
    let tags_key = tags_dir_key(repo_name)?;
    let rep_key = repo_key(repo_name)?;

    // Step 2: Enumerate repos/<repo>/tags beneath pinned root (performed even if page_limit == 0)
    let entries = match dir_enumerator
        .enumerate_dir(Some(&tags_key), tags_dir_limits)
        .await
    {
        Ok(entries) => entries,
        Err(FsDirError::NotFound { .. }) => {
            // Step 3: Probe repos/<repo> with repo_probe_limits
            match dir_enumerator
                .enumerate_dir(Some(&rep_key), repo_probe_limits)
                .await
            {
                Ok(_) => {
                    // Repository exists, but tags/ directory is missing
                    return Ok((Vec::new(), None));
                }
                Err(FsDirError::NotFound { .. }) => {
                    // Repository does not exist: returns empty terminal page for list_tags_page
                    return Ok((Vec::new(), None));
                }
                Err(err) => {
                    return Err(translate_tag_dir_error(err, rep_key.as_str()));
                }
            }
        }
        Err(err) => {
            return Err(translate_tag_dir_error(err, tags_key.as_str()));
        }
    };

    // Step 4: If page_limit == 0, short-circuit after directory validation without opening payloads
    if page_limit == 0 {
        return Ok((Vec::new(), None));
    }

    // Step 5: Candidate filtering and payload acquisition
    let mut tags_with_digest: Vec<(String, Digest)> = Vec::new();
    for entry in entries {
        // Skip dotfiles and non-UTF-8 entries
        let Some(name_str) = entry.name().to_str() else {
            continue;
        };
        if name_str.starts_with('.') {
            continue;
        }
        // Retain only entries observed as Regular
        if entry.file_type() != DirEntryType::Regular {
            continue;
        }

        // Validate candidate key construction before payload access
        let candidate_key = super::tag_read::tag_key(repo_name, name_str)?;

        // Open candidate payload via contained reader
        let payload = match payload_reader.open_payload(&candidate_key).await {
            Ok(p) => p,
            Err(ReadError::NotFound { .. }) => {
                // Acquisition NotFound: candidate disappeared between readdir and open, omit
                continue;
            }
            Err(other) => {
                return Err(super::read_adapter::translate_payload_read_error(other));
            }
        };

        // Drain payload stream
        let raw_bytes = super::tag_read::drain_tag_stream(payload, &payload_limits).await?;

        // Decode UTF-8 (fail-closed with StorageErrorKind::Io under experimental seam policy)
        let content_str = match std::str::from_utf8(&raw_bytes) {
            Ok(s) => s,
            Err(err) => {
                return Err(StorageError::io(format!(
                    "invalid UTF-8 in tag payload {name_str}: {err}"
                )));
            }
        };

        // Parse valid digest after trimming whitespace; empty or malformed digest text is omitted
        if let Ok(digest) = Digest::parse(content_str.trim()) {
            tags_with_digest.push((name_str.to_string(), digest));
        }
    }

    // Step 6: Lexical sorting and pagination slicing
    tags_with_digest.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let start_idx = if let Some(token) = cursor {
        match tags_with_digest.binary_search_by(|(t, _)| t.as_str().cmp(token)) {
            Ok(idx) => idx.saturating_add(1),
            Err(idx) => idx,
        }
    } else {
        0
    };

    let start_idx = start_idx.min(tags_with_digest.len());
    let end_idx = start_idx
        .saturating_add(page_limit)
        .min(tags_with_digest.len());
    let page_slice = &tags_with_digest[start_idx..end_idx];

    let next_token = if end_idx < tags_with_digest.len() {
        page_slice.last().map(|(t, _)| t.clone())
    } else {
        None
    };

    Ok((page_slice.to_vec(), next_token))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageErrorKind;
    use std::collections::{HashMap, VecDeque};

    use std::ffi::{OsStr, OsString};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixListener;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use storage_core::{ObjectMetadata, ObjectPayload, ObjectStream};
    use storage_fs::{FsMetadataError, LimitExceededReason};
    use tokio::io::{AsyncRead, ReadBuf};

    // --- Mock Tag Directory Enumerator ---
    #[derive(Default)]
    pub(crate) struct MockTagDirEnumerator {
        pub recorded_invocations: Mutex<Vec<(Option<ObjectKey>, storage_fs::DirEnumerationLimits)>>,
        pub scripted_responses: Mutex<
            HashMap<
                Option<ObjectKey>,
                VecDeque<Result<Vec<storage_fs::DirEntry>, storage_fs::FsDirError>>,
            >,
        >,
    }

    impl MockTagDirEnumerator {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        pub(crate) fn script(
            &self,
            target: Option<&ObjectKey>,
            response: Result<Vec<storage_fs::DirEntry>, storage_fs::FsDirError>,
        ) {
            let key = target.cloned();
            self.scripted_responses
                .lock()
                .unwrap()
                .entry(key)
                .or_default()
                .push_back(response);
        }

        pub(crate) fn recorded_invocations(
            &self,
        ) -> Vec<(Option<ObjectKey>, storage_fs::DirEnumerationLimits)> {
            self.recorded_invocations.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl TagDirEnumerator for MockTagDirEnumerator {
        async fn enumerate_dir(
            &self,
            target: Option<&ObjectKey>,
            limits: storage_fs::DirEnumerationLimits,
        ) -> Result<Vec<storage_fs::DirEntry>, storage_fs::FsDirError> {
            self.recorded_invocations
                .lock()
                .unwrap()
                .push((target.cloned(), limits));

            let mut responses = self.scripted_responses.lock().unwrap();
            let queue = responses.get_mut(&target.cloned()).unwrap_or_else(|| {
                panic!("unexpected call to MockTagDirEnumerator with target: {target:?}")
            });
            queue
                .pop_front()
                .unwrap_or_else(|| panic!("no more scripted responses for target: {target:?}"))
        }
    }

    // --- Mock Payload Body & Stream Reader ---
    #[derive(Clone, Debug)]
    pub(crate) enum MockPayloadBody {
        Complete(Vec<u8>),
        PartialThenError {
            data: Vec<u8>,
            error_kind: std::io::ErrorKind,
            error_message: String,
        },
        ImmediateIoError {
            error_kind: std::io::ErrorKind,
            error_message: String,
        },
    }

    pub(crate) struct MockPayloadAsyncReader {
        data: Vec<u8>,
        cursor: usize,
        terminal_error: Option<std::io::Error>,
    }

    impl MockPayloadAsyncReader {
        pub(crate) fn new(body: MockPayloadBody) -> Self {
            match body {
                MockPayloadBody::Complete(data) => Self {
                    data,
                    cursor: 0,
                    terminal_error: None,
                },
                MockPayloadBody::PartialThenError {
                    data,
                    error_kind,
                    error_message,
                } => Self {
                    data,
                    cursor: 0,
                    terminal_error: Some(std::io::Error::new(error_kind, error_message)),
                },
                MockPayloadBody::ImmediateIoError {
                    error_kind,
                    error_message,
                } => Self {
                    data: Vec::new(),
                    cursor: 0,
                    terminal_error: Some(std::io::Error::new(error_kind, error_message)),
                },
            }
        }
    }

    impl AsyncRead for MockPayloadAsyncReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.cursor < self.data.len() {
                let available = &self.data[self.cursor..];
                let to_copy = std::cmp::min(available.len(), buf.remaining());
                buf.put_slice(&available[..to_copy]);
                self.cursor += to_copy;
                Poll::Ready(Ok(()))
            } else if let Some(err) = self.terminal_error.take() {
                Poll::Ready(Err(err))
            } else {
                Poll::Ready(Ok(()))
            }
        }
    }

    // --- Mock Tag Payload Reader ---
    #[derive(Default)]
    pub(crate) struct MockTagPayloadReader {
        pub inject_open_error: Mutex<HashMap<ObjectKey, ReadError>>,
        pub inject_payload_bodies: Mutex<HashMap<ObjectKey, (ObjectMetadata, MockPayloadBody)>>,
        pub recorded_opens: Mutex<Vec<ObjectKey>>,
    }

    impl MockTagPayloadReader {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        pub(crate) fn script_open_error(&self, key: ObjectKey, error: ReadError) {
            self.inject_open_error.lock().unwrap().insert(key, error);
        }

        pub(crate) fn script_payload(
            &self,
            key: ObjectKey,
            metadata: ObjectMetadata,
            body: MockPayloadBody,
        ) {
            self.inject_payload_bodies
                .lock()
                .unwrap()
                .insert(key, (metadata, body));
        }

        pub(crate) fn script_bytes(&self, key: ObjectKey, bytes: Vec<u8>) {
            let meta = ObjectMetadata::new(bytes.len() as u64);
            self.script_payload(key, meta, MockPayloadBody::Complete(bytes));
        }

        pub(crate) fn recorded_opens(&self) -> Vec<ObjectKey> {
            self.recorded_opens.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ObjectPayloadReader for MockTagPayloadReader {
        async fn open_payload(&self, key: &ObjectKey) -> Result<ObjectPayload, ReadError> {
            self.recorded_opens.lock().unwrap().push(key.clone());

            if let Some(err) = self.inject_open_error.lock().unwrap().remove(key) {
                return Err(err);
            }

            if let Some((meta, body)) = self.inject_payload_bodies.lock().unwrap().remove(key) {
                let reader = MockPayloadAsyncReader::new(body);
                let stream: ObjectStream = Box::pin(reader);
                return Ok(ObjectPayload::new(meta, stream));
            }

            panic!("unexpected call to MockTagPayloadReader::open_payload with key: {key}")
        }
    }

    // Helper RAII guard for permissions
    struct PermGuard(std::path::PathBuf);
    impl Drop for PermGuard {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
        }
    }

    // 1. Name-only listing makes zero payload opens
    #[tokio::test]
    async fn test_name_only_zero_payload_opens() {
        let enumerator = MockTagDirEnumerator::new();
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();
        enumerator.script(
            Some(&tags_key),
            Ok(vec![
                DirEntry::new(OsString::from("v1.0"), DirEntryType::Regular),
                DirEntry::new(OsString::from("v2.0"), DirEntryType::Regular),
                DirEntry::new(OsString::from("latest"), DirEntryType::Regular),
            ]),
        );

        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);

        let tags = contained_list_tags_seam(&enumerator, "myrepo", probe_limits, tags_limits)
            .await
            .expect("list_tags should succeed");

        assert_eq!(tags, vec!["latest", "v1.0", "v2.0"]);
        // Verify only tags directory was enumerated
        let calls = enumerator.recorded_invocations();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0.as_ref(), Some(&tags_key));
    }

    // 2. Valid SHA-256/SHA-512, whitespace padding, and sorted paged results
    #[tokio::test]
    async fn test_paged_valid_sha256_sha512_whitespace_padding() {
        let enumerator = MockTagDirEnumerator::new();
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();
        enumerator.script(
            Some(&tags_key),
            Ok(vec![
                DirEntry::new(OsString::from("tag-sha512"), DirEntryType::Regular),
                DirEntry::new(OsString::from("tag-sha256"), DirEntryType::Regular),
            ]),
        );

        let payload_reader = MockTagPayloadReader::new();
        let sha256_hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let sha512_hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

        // With surrounding whitespace and trailing \r\n
        let payload256 = format!("  sha256:{sha256_hex} \r\n");
        let payload512 = format!("\nsha512:{sha512_hex}\n");

        payload_reader.script_bytes(
            ObjectKey::parse("repos/myrepo/tags/tag-sha256").unwrap(),
            payload256.into_bytes(),
        );
        payload_reader.script_bytes(
            ObjectKey::parse("repos/myrepo/tags/tag-sha512").unwrap(),
            payload512.into_bytes(),
        );

        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);
        let payload_limits = super::super::tag_read::TagReadLimits {
            max_payload_bytes: None,
        };

        let (page, next) = contained_list_tags_page_seam(
            &enumerator,
            &payload_reader,
            "myrepo",
            None,
            1,
            probe_limits,
            tags_limits,
            payload_limits.clone(),
        )
        .await
        .expect("page 1 should succeed");

        assert_eq!(page.len(), 1);
        assert_eq!(page[0].0, "tag-sha256");
        assert_eq!(page[0].1.to_string(), format!("sha256:{sha256_hex}"));
        assert_eq!(next.as_deref(), Some("tag-sha256"));

        // Page 2 using cursor
        enumerator.script(
            Some(&tags_key),
            Ok(vec![
                DirEntry::new(OsString::from("tag-sha512"), DirEntryType::Regular),
                DirEntry::new(OsString::from("tag-sha256"), DirEntryType::Regular),
            ]),
        );
        payload_reader.script_bytes(
            ObjectKey::parse("repos/myrepo/tags/tag-sha256").unwrap(),
            format!("sha256:{sha256_hex}").into_bytes(),
        );
        payload_reader.script_bytes(
            ObjectKey::parse("repos/myrepo/tags/tag-sha512").unwrap(),
            format!("sha512:{sha512_hex}").into_bytes(),
        );

        let (page2, next2) = contained_list_tags_page_seam(
            &enumerator,
            &payload_reader,
            "myrepo",
            next.as_deref(),
            1,
            probe_limits,
            tags_limits,
            payload_limits,
        )
        .await
        .expect("page 2 should succeed");

        assert_eq!(page2.len(), 1);
        assert_eq!(page2[0].0, "tag-sha512");
        assert_eq!(page2[0].1.to_string(), format!("sha512:{sha512_hex}"));
        assert_eq!(next2, None);
    }

    // 3. Missing repository, missing tags/, and empty directories
    #[tokio::test]
    async fn test_missing_repo_vs_missing_tags_directory() {
        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);
        let payload_limits = super::super::tag_read::TagReadLimits::default();
        let payload_reader = MockTagPayloadReader::new();

        // Case A: Missing repository (both tags/ and repos/<repo> return NotFound)
        {
            let enumerator = MockTagDirEnumerator::new();
            let tags_key = ObjectKey::parse("repos/ghost/tags").unwrap();
            let repo_k = ObjectKey::parse("repos/ghost").unwrap();
            enumerator.script(
                Some(&tags_key),
                Err(FsDirError::NotFound {
                    path: Some("repos/ghost/tags".to_string()),
                }),
            );
            enumerator.script(
                Some(&repo_k),
                Err(FsDirError::NotFound {
                    path: Some("repos/ghost".to_string()),
                }),
            );

            // list_tags returns StorageError::NotFound
            let err = contained_list_tags_seam(&enumerator, "ghost", probe_limits, tags_limits)
                .await
                .unwrap_err();
            assert!(matches!(err, StorageError::NotFound));

            // list_tags_page returns Ok(([], None))
            enumerator.script(
                Some(&tags_key),
                Err(FsDirError::NotFound {
                    path: Some("repos/ghost/tags".to_string()),
                }),
            );
            enumerator.script(
                Some(&repo_k),
                Err(FsDirError::NotFound {
                    path: Some("repos/ghost".to_string()),
                }),
            );
            let (page, next) = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "ghost",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits.clone(),
            )
            .await
            .expect("missing repo on paged listing returns empty terminal page");
            assert!(page.is_empty());
            assert_eq!(next, None);
        }

        // Case B: Existing repository with missing tags/
        {
            let enumerator = MockTagDirEnumerator::new();
            let tags_key = ObjectKey::parse("repos/existing/tags").unwrap();
            let repo_k = ObjectKey::parse("repos/existing").unwrap();
            enumerator.script(
                Some(&tags_key),
                Err(FsDirError::NotFound {
                    path: Some("repos/existing/tags".to_string()),
                }),
            );
            enumerator.script(
                Some(&repo_k),
                Ok(vec![DirEntry::new(
                    OsString::from("manifests"),
                    DirEntryType::Directory,
                )]),
            );

            let tags = contained_list_tags_seam(&enumerator, "existing", probe_limits, tags_limits)
                .await
                .expect("existing repo missing tags/ should succeed empty");
            assert!(tags.is_empty());

            enumerator.script(
                Some(&tags_key),
                Err(FsDirError::NotFound {
                    path: Some("repos/existing/tags".to_string()),
                }),
            );
            enumerator.script(
                Some(&repo_k),
                Ok(vec![DirEntry::new(
                    OsString::from("manifests"),
                    DirEntryType::Directory,
                )]),
            );
            let (page, next) = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "existing",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits,
            )
            .await
            .expect("existing repo missing tags/ should succeed empty page");
            assert!(page.is_empty());
            assert_eq!(next, None);
        }
    }

    // 4. Injected mock independent probe and tags-directory budgets translation test
    #[tokio::test]
    async fn test_mock_independent_probe_and_tags_budgets_translation() {
        let enumerator = MockTagDirEnumerator::new();
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();
        let repo_k = ObjectKey::parse("repos/myrepo").unwrap();

        let probe_limits = DirEnumerationLimits::new(1, 64);
        let tags_limits = DirEnumerationLimits::new(500, 4096);

        // Simulate tags/ missing and probe exceeding its budget
        enumerator.script(
            Some(&tags_key),
            Err(FsDirError::NotFound {
                path: Some("repos/myrepo/tags".to_string()),
            }),
        );
        enumerator.script(
            Some(&repo_k),
            Err(FsDirError::LimitExceeded {
                reason: LimitExceededReason::MaxEntries(1),
            }),
        );

        let err = contained_list_tags_seam(&enumerator, "myrepo", probe_limits, tags_limits)
            .await
            .unwrap_err();

        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Backend));

        let calls = enumerator.recorded_invocations();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0.as_ref(), Some(&tags_key));
        assert_eq!(calls[0].1, tags_limits);
        assert_eq!(calls[1].0.as_ref(), Some(&repo_k));
        assert_eq!(calls[1].1, probe_limits);
    }

    // 4b. Actual repository-probe exhaustion and exact boundary test with real FsMetadataReader
    #[tokio::test]
    async fn test_real_repository_probe_exhaustion_and_exact_boundary() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let root_path = temp_dir.path().to_path_buf();

        // Create repos/probed_repo without tags/ directory, but with 3 child entries
        let repo_dir = root_path.join("repos/probed_repo");
        std::fs::create_dir_all(&repo_dir).expect("create repo dir");
        std::fs::create_dir(repo_dir.join("manifests")).expect("create manifests dir");
        std::fs::create_dir(repo_dir.join("revisions")).expect("create revisions dir");
        std::fs::write(repo_dir.join("metadata_file"), b"meta").expect("write metadata file");

        let reader = storage_fs::FsMetadataReader::open(&root_path).expect("open reader");

        let tags_dir_limits = DirEnumerationLimits::new(500, 4096);
        let payload_limits = super::super::tag_read::TagReadLimits::default();

        // Case A: Probe budget exhausted (max_entries = 2 < 3 actual entries in repo)
        let exhausted_probe_limits = DirEnumerationLimits::new(2, 4096);

        let err_name = contained_list_tags_seam(
            &reader,
            "probed_repo",
            exhausted_probe_limits,
            tags_dir_limits,
        )
        .await
        .unwrap_err();
        assert_eq!(err_name.internal_kind(), Some(StorageErrorKind::Backend));

        let err_page = contained_list_tags_page_seam(
            &reader,
            &reader,
            "probed_repo",
            None,
            10,
            exhausted_probe_limits,
            tags_dir_limits,
            payload_limits.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(err_page.internal_kind(), Some(StorageErrorKind::Backend));

        // Case B: Exact successful probe boundary (max_entries = 3 == 3 actual entries in repo)
        let exact_probe_limits = DirEnumerationLimits::new(3, 4096);

        let names =
            contained_list_tags_seam(&reader, "probed_repo", exact_probe_limits, tags_dir_limits)
                .await
                .expect("exact probe budget should succeed");
        assert!(names.is_empty());

        let (page, next) = contained_list_tags_page_seam(
            &reader,
            &reader,
            "probed_repo",
            None,
            10,
            exact_probe_limits,
            tags_dir_limits,
            payload_limits.clone(),
        )
        .await
        .expect("exact probe budget should succeed for paged listing");
        assert!(page.is_empty());
        assert_eq!(next, None);

        // Case C: Tags-first contract - when tags/ exists, probe limits are NOT exercised
        let tags_dir = repo_dir.join("tags");
        std::fs::create_dir_all(&tags_dir).expect("create tags dir");
        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        std::fs::write(
            tags_dir.join("v1"),
            format!(
                "sha256:{hex}
"
            ),
        )
        .expect("write v1 tag");

        // Intentionally zero/failing probe limits to prove probe is not invoked on tags/ hit
        let zero_probe_limits = DirEnumerationLimits::new(0, 0);
        let tags_hit_names =
            contained_list_tags_seam(&reader, "probed_repo", zero_probe_limits, tags_dir_limits)
                .await
                .expect("tags/ hit should not invoke probe");
        assert_eq!(tags_hit_names, vec!["v1"]);

        let (tags_hit_page, _) = contained_list_tags_page_seam(
            &reader,
            &reader,
            "probed_repo",
            None,
            10,
            zero_probe_limits,
            tags_dir_limits,
            payload_limits,
        )
        .await
        .expect("tags/ hit should not invoke probe for paged listing");
        assert_eq!(tags_hit_page.len(), 1);
        assert_eq!(tags_hit_page[0].0, "v1");
    }

    // 5. Actual directory-boundary tests and filtered entry budget consumption with real FsMetadataReader
    #[tokio::test]
    async fn test_real_directory_boundaries_and_filtered_consumption() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let root_path = temp_dir.path().to_path_buf();
        let tags_dir = root_path.join("repos/myrepo/tags");
        std::fs::create_dir_all(&tags_dir).expect("create tags dir");

        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        // Create 3 regular files: t1 (2 bytes), t2 (2 bytes), t3 (2 bytes)
        // Total entries = 3; total raw name bytes = 2 + 2 + 2 = 6 bytes
        std::fs::write(
            tags_dir.join("t1"),
            format!(
                "sha256:{hex}
"
            ),
        )
        .expect("write t1");
        std::fs::write(
            tags_dir.join("t2"),
            format!(
                "sha256:{hex}
"
            ),
        )
        .expect("write t2");
        std::fs::write(
            tags_dir.join("t3"),
            format!(
                "sha256:{hex}
"
            ),
        )
        .expect("write t3");

        let reader = storage_fs::FsMetadataReader::open(&root_path).expect("open reader");
        let probe_limits = DirEnumerationLimits::new(100, 10240);
        let payload_limits = super::super::tag_read::TagReadLimits::default();

        // --- Boundary 1: Entry count (N = 3 succeeds; N - 1 = 2 fails) ---
        let exact_count_limits = DirEnumerationLimits::new(3, 1024);
        let names = contained_list_tags_seam(&reader, "myrepo", probe_limits, exact_count_limits)
            .await
            .expect("exact 3 entries should succeed");
        assert_eq!(names, vec!["t1", "t2", "t3"]);

        let (page, _) = contained_list_tags_page_seam(
            &reader,
            &reader,
            "myrepo",
            None,
            10,
            probe_limits,
            exact_count_limits,
            payload_limits.clone(),
        )
        .await
        .expect("exact 3 entries should succeed for paged listing");
        assert_eq!(page.len(), 3);

        // Exhausted by 1 entry (limit 2 < 3 entries)
        let exhausted_count_limits = DirEnumerationLimits::new(2, 1024);
        let err_count_name =
            contained_list_tags_seam(&reader, "myrepo", probe_limits, exhausted_count_limits)
                .await
                .unwrap_err();
        assert_eq!(
            err_count_name.internal_kind(),
            Some(StorageErrorKind::Backend)
        );

        let err_count_page = contained_list_tags_page_seam(
            &reader,
            &reader,
            "myrepo",
            None,
            10,
            probe_limits,
            exhausted_count_limits,
            payload_limits.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(
            err_count_page.internal_kind(),
            Some(StorageErrorKind::Backend)
        );

        // --- Boundary 2: Raw filename bytes (B = 6 succeeds; B - 1 = 5 fails) ---
        let exact_bytes_limits = DirEnumerationLimits::new(10, 6);
        let names_bytes =
            contained_list_tags_seam(&reader, "myrepo", probe_limits, exact_bytes_limits)
                .await
                .expect("exact 6 bytes should succeed");
        assert_eq!(names_bytes, vec!["t1", "t2", "t3"]);

        let (page_bytes, _) = contained_list_tags_page_seam(
            &reader,
            &reader,
            "myrepo",
            None,
            10,
            probe_limits,
            exact_bytes_limits,
            payload_limits.clone(),
        )
        .await
        .expect("exact 6 bytes should succeed for paged listing");
        assert_eq!(page_bytes.len(), 3);

        // Exhausted by 1 byte (limit 5 < 6 total name bytes)
        let exhausted_bytes_limits = DirEnumerationLimits::new(10, 5);
        let err_bytes_name =
            contained_list_tags_seam(&reader, "myrepo", probe_limits, exhausted_bytes_limits)
                .await
                .unwrap_err();
        assert_eq!(
            err_bytes_name.internal_kind(),
            Some(StorageErrorKind::Backend)
        );

        let err_bytes_page = contained_list_tags_page_seam(
            &reader,
            &reader,
            "myrepo",
            None,
            10,
            probe_limits,
            exhausted_bytes_limits,
            payload_limits.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(
            err_bytes_page.internal_kind(),
            Some(StorageErrorKind::Backend)
        );

        // --- Boundary 3: Filtered entries still consume enumeration budgets ---
        // Add 3 non-regular entries: subdirectory, dotfile, and symlink
        std::fs::create_dir(tags_dir.join("sub_dir")).expect("create subdir");
        std::fs::write(tags_dir.join(".dotfile"), b"hidden").expect("write dotfile");
        std::os::unix::fs::symlink(tags_dir.join("t1"), tags_dir.join("symlink_tag"))
            .expect("create symlink");

        // Directory now contains 6 total entries (3 regular + 3 non-regular)
        // With max_entries = 5, enumeration must exhaust and fail with Backend,
        // even though only 3 regular tags would remain after filtering!
        let filtered_exhausted_limits = DirEnumerationLimits::new(5, 1024);
        let err_filt_name =
            contained_list_tags_seam(&reader, "myrepo", probe_limits, filtered_exhausted_limits)
                .await
                .unwrap_err();
        assert_eq!(
            err_filt_name.internal_kind(),
            Some(StorageErrorKind::Backend)
        );

        let err_filt_page = contained_list_tags_page_seam(
            &reader,
            &reader,
            "myrepo",
            None,
            10,
            probe_limits,
            filtered_exhausted_limits,
            payload_limits.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(
            err_filt_page.internal_kind(),
            Some(StorageErrorKind::Backend)
        );

        // With max_entries = 6, all entries are enumerated, non-regular are filtered out,
        // returning exactly the 3 regular tags
        let filtered_success_limits = DirEnumerationLimits::new(6, 1024);
        let filtered_success_names =
            contained_list_tags_seam(&reader, "myrepo", probe_limits, filtered_success_limits)
                .await
                .expect("6 entries should succeed and filter non-regular");
        assert_eq!(filtered_success_names, vec!["t1", "t2", "t3"]);
    }

    // 5b. Injected mock directory limit exceeded error translation test
    #[tokio::test]
    async fn test_mock_directory_limit_exceeded_error_translation() {
        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);

        // Entry count error translation
        {
            let enumerator = MockTagDirEnumerator::new();
            let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();
            enumerator.script(
                Some(&tags_key),
                Err(FsDirError::LimitExceeded {
                    reason: LimitExceededReason::MaxEntries(10),
                }),
            );

            let err = contained_list_tags_seam(&enumerator, "myrepo", probe_limits, tags_limits)
                .await
                .unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Backend));
        }

        // Total name bytes error translation
        {
            let enumerator = MockTagDirEnumerator::new();
            let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();
            enumerator.script(
                Some(&tags_key),
                Err(FsDirError::LimitExceeded {
                    reason: LimitExceededReason::MaxTotalNameBytes(1024),
                }),
            );

            let err = contained_list_tags_seam(&enumerator, "myrepo", probe_limits, tags_limits)
                .await
                .unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Backend));
        }
    }

    // 5c. Exact payload byte boundary test (71 bytes vs 70 bytes)
    #[tokio::test]
    async fn test_payload_byte_exact_boundary() {
        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);

        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let digest_str = format!("sha256:{hex}");
        assert_eq!(digest_str.len(), 71);

        let enumerator = MockTagDirEnumerator::new();
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();
        enumerator.script(
            Some(&tags_key),
            Ok(vec![DirEntry::new(
                OsString::from("t1"),
                DirEntryType::Regular,
            )]),
        );

        let payload_reader = MockTagPayloadReader::new();
        let tag_key = ObjectKey::parse("repos/myrepo/tags/t1").unwrap();
        payload_reader.script_bytes(tag_key.clone(), digest_str.clone().into_bytes());

        // Limit = 71 succeeds
        let (page, _) = contained_list_tags_page_seam(
            &enumerator,
            &payload_reader,
            "myrepo",
            None,
            10,
            probe_limits,
            tags_limits,
            super::super::tag_read::TagReadLimits {
                max_payload_bytes: Some(71),
            },
        )
        .await
        .expect("71 bytes limit should succeed");
        assert_eq!(page.len(), 1);

        // Limit = 70 fails with CorruptData
        enumerator.script(
            Some(&tags_key),
            Ok(vec![DirEntry::new(
                OsString::from("t1"),
                DirEntryType::Regular,
            )]),
        );
        payload_reader.script_bytes(tag_key, digest_str.into_bytes());
        let err = contained_list_tags_page_seam(
            &enumerator,
            &payload_reader,
            "myrepo",
            None,
            10,
            probe_limits,
            tags_limits,
            super::super::tag_read::TagReadLimits {
                max_payload_bytes: Some(70),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::CorruptData));
    }

    // 6. None payload limit and Some(u64::MAX) checked-overflow behavior
    #[tokio::test]
    async fn test_payload_limit_none_and_checked_overflow_u64_max() {
        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);
        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let digest_str = format!("sha256:{hex}");

        let enumerator = MockTagDirEnumerator::new();
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();
        enumerator.script(
            Some(&tags_key),
            Ok(vec![DirEntry::new(
                OsString::from("t1"),
                DirEntryType::Regular,
            )]),
        );

        let payload_reader = MockTagPayloadReader::new();
        let tag_key = ObjectKey::parse("repos/myrepo/tags/t1").unwrap();
        payload_reader.script_bytes(tag_key.clone(), digest_str.clone().into_bytes());

        // None limit drains unbounded cleanly
        let (page, _) = contained_list_tags_page_seam(
            &enumerator,
            &payload_reader,
            "myrepo",
            None,
            10,
            probe_limits,
            tags_limits,
            super::super::tag_read::TagReadLimits {
                max_payload_bytes: None,
            },
        )
        .await
        .expect("None limit succeeds");
        assert_eq!(page.len(), 1);

        // Some(u64::MAX) triggers checked_add(1) overflow -> CorruptData
        enumerator.script(
            Some(&tags_key),
            Ok(vec![DirEntry::new(
                OsString::from("t1"),
                DirEntryType::Regular,
            )]),
        );
        payload_reader.script_bytes(tag_key, digest_str.into_bytes());
        let err = contained_list_tags_page_seam(
            &enumerator,
            &payload_reader,
            "myrepo",
            None,
            10,
            probe_limits,
            tags_limits,
            super::super::tag_read::TagReadLimits {
                max_payload_bytes: Some(u64::MAX),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::CorruptData));
    }

    // 7. Invalid repository inputs cause zero reader calls
    #[tokio::test]
    async fn test_invalid_repository_input_causes_zero_reader_calls() {
        let invalid_repos = [
            "",
            "/leading",
            "trailing/",
            "back\\slash",
            "nul\0byte",
            "control\x07char",
            "repeated//slash",
            ".",
            "..",
            "sub/../repo",
        ];

        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);
        let payload_limits = super::super::tag_read::TagReadLimits::default();

        for repo in invalid_repos {
            let enumerator = MockTagDirEnumerator::new();
            let payload_reader = MockTagPayloadReader::new();

            let err_name = contained_list_tags_seam(&enumerator, repo, probe_limits, tags_limits)
                .await
                .unwrap_err();
            assert!(matches!(err_name, StorageError::InvalidRepoName(_)));

            let err_page = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                repo,
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits.clone(),
            )
            .await
            .unwrap_err();
            assert!(matches!(err_page, StorageError::InvalidRepoName(_)));

            // Verify ZERO enumerator or payload reader calls occurred
            assert_eq!(enumerator.recorded_invocations().len(), 0);
            assert_eq!(payload_reader.recorded_opens().len(), 0);
        }
    }

    // 8. Dotfiles, non-UTF-8 names, and observed non-regular entries
    #[tokio::test]
    async fn test_dotfiles_non_utf8_and_non_regular_entries_skipped() {
        let enumerator = MockTagDirEnumerator::new();
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();

        // Non-UTF-8 entry
        let non_utf8 = OsStr::from_bytes(b"invalid_\xff\xfe").to_os_string();

        enumerator.script(
            Some(&tags_key),
            Ok(vec![
                DirEntry::new(OsString::from(".tmp.upload_1"), DirEntryType::Regular),
                DirEntry::new(OsString::from(".lock"), DirEntryType::Regular),
                DirEntry::new(non_utf8, DirEntryType::Regular),
                DirEntry::new(OsString::from("sub_dir"), DirEntryType::Directory),
                DirEntry::new(OsString::from("symlink_tag"), DirEntryType::Symlink),
                DirEntry::new(OsString::from("fifo_tag"), DirEntryType::Other),
                DirEntry::new(OsString::from("valid_tag"), DirEntryType::Regular),
            ]),
        );

        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);

        let tags = contained_list_tags_seam(&enumerator, "myrepo", probe_limits, tags_limits)
            .await
            .expect("should succeed");

        // Only valid_tag is retained
        assert_eq!(tags, vec!["valid_tag"]);
    }

    // 9. Directory-path symlink rejection versus skipped child symlink entries
    #[tokio::test]
    async fn test_symlinks_path_resolution_rejected_vs_child_entries_skipped() {
        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);

        // Case A: Directory path symlink fails closed with ResolutionRejected -> Io
        {
            let enumerator = MockTagDirEnumerator::new();
            let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();
            enumerator.script(
                Some(&tags_key),
                Err(FsDirError::ResolutionRejected {
                    raw_os_error: libc::ELOOP,
                    source: std::io::Error::from_raw_os_error(libc::ELOOP),
                }),
            );

            let err = contained_list_tags_seam(&enumerator, "myrepo", probe_limits, tags_limits)
                .await
                .unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
        }

        // Case B: Child symlink entry in tags/ is skipped, allowing regular tags to be listed
        {
            let enumerator = MockTagDirEnumerator::new();
            let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![
                    DirEntry::new(OsString::from("symlink_entry"), DirEntryType::Symlink),
                    DirEntry::new(OsString::from("regular_tag"), DirEntryType::Regular),
                ]),
            );

            let tags = contained_list_tags_seam(&enumerator, "myrepo", probe_limits, tags_limits)
                .await
                .expect("should succeed");
            assert_eq!(tags, vec!["regular_tag"]);
        }
    }

    // 10. Candidate observed as Regular then replaced before acquisition
    #[tokio::test]
    async fn test_candidate_observed_regular_replaced_before_acquisition() {
        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);
        let payload_limits = super::super::tag_read::TagReadLimits::default();
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();

        // Subcase 1: Replaced with Directory -> UnsupportedObjectType -> Io
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![DirEntry::new(
                    OsString::from("t1"),
                    DirEntryType::Regular,
                )]),
            );
            let payload_reader = MockTagPayloadReader::new();
            payload_reader.script_open_error(
                ObjectKey::parse("repos/myrepo/tags/t1").unwrap(),
                ReadError::backend_with_source(
                    "unsupported object type",
                    Box::new(FsMetadataError::UnsupportedObjectType { mode: 0o040755 }),
                ),
            );

            let err = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits.clone(),
            )
            .await
            .unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
        }

        // Subcase 2: Replaced with Symlink -> ResolutionRejected -> Io
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![DirEntry::new(
                    OsString::from("t1"),
                    DirEntryType::Regular,
                )]),
            );
            let payload_reader = MockTagPayloadReader::new();
            payload_reader.script_open_error(
                ObjectKey::parse("repos/myrepo/tags/t1").unwrap(),
                ReadError::backend_with_source(
                    "resolution rejected",
                    Box::new(FsMetadataError::ResolutionRejected {
                        raw_os_error: libc::ELOOP,
                        source: std::io::Error::from_raw_os_error(libc::ELOOP),
                    }),
                ),
            );

            let err = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits.clone(),
            )
            .await
            .unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
        }

        // Subcase 3: Replaced permissions -> PermissionDenied -> Io
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![DirEntry::new(
                    OsString::from("t1"),
                    DirEntryType::Regular,
                )]),
            );
            let payload_reader = MockTagPayloadReader::new();
            payload_reader.script_open_error(
                ObjectKey::parse("repos/myrepo/tags/t1").unwrap(),
                ReadError::permission_denied(ObjectKey::parse("repos/myrepo/tags/t1").unwrap()),
            );

            let err = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits,
            )
            .await
            .unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
        }
    }

    // 11. Directory EntryDisappeared propagates; payload NotFound omits
    #[tokio::test]
    async fn test_dir_entry_disappeared_propagates_payload_not_found_omits() {
        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);
        let payload_limits = super::super::tag_read::TagReadLimits::default();
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();

        // Case A: Directory EntryDisappeared during inspection propagates directory error
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Err(FsDirError::EntryDisappeared {
                    name: OsString::from("vanishing_tag"),
                }),
            );

            let err = contained_list_tags_seam(&enumerator, "myrepo", probe_limits, tags_limits)
                .await
                .unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
        }

        // Case B: Payload NotFound after enumeration omits candidate safely
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![
                    DirEntry::new(OsString::from("vanished"), DirEntryType::Regular),
                    DirEntry::new(OsString::from("survived"), DirEntryType::Regular),
                ]),
            );

            let payload_reader = MockTagPayloadReader::new();
            payload_reader.script_open_error(
                ObjectKey::parse("repos/myrepo/tags/vanished").unwrap(),
                ReadError::not_found(ObjectKey::parse("repos/myrepo/tags/vanished").unwrap()),
            );

            let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
            payload_reader.script_bytes(
                ObjectKey::parse("repos/myrepo/tags/survived").unwrap(),
                format!("sha256:{hex}").into_bytes(),
            );

            let (page, _) = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits,
            )
            .await
            .expect("should succeed with vanished tag omitted");

            assert_eq!(page.len(), 1);
            assert_eq!(page[0].0, "survived");
        }
    }

    // 12. Permission errors and platform/runtime/backend error mappings
    #[tokio::test]
    async fn test_permission_denied_directory_and_payload_mappings() {
        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);
        let payload_limits = super::super::tag_read::TagReadLimits::default();
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();

        // Directory permission denied -> PermissionDenied
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Err(FsDirError::PermissionDenied {
                    path: Some("repos/myrepo/tags".to_string()),
                    source: std::io::Error::from_raw_os_error(libc::EACCES),
                }),
            );

            let err = contained_list_tags_seam(&enumerator, "myrepo", probe_limits, tags_limits)
                .await
                .unwrap_err();
            assert_eq!(
                err.internal_kind(),
                Some(StorageErrorKind::PermissionDenied)
            );
        }

        // Directory platform unsupported -> Configuration
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(Some(&tags_key), Err(FsDirError::PlatformUnsupported));

            let err = contained_list_tags_seam(&enumerator, "myrepo", probe_limits, tags_limits)
                .await
                .unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Configuration));
        }

        // Payload permission denied -> Io
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![DirEntry::new(
                    OsString::from("t1"),
                    DirEntryType::Regular,
                )]),
            );
            let payload_reader = MockTagPayloadReader::new();
            payload_reader.script_open_error(
                ObjectKey::parse("repos/myrepo/tags/t1").unwrap(),
                ReadError::permission_denied(ObjectKey::parse("repos/myrepo/tags/t1").unwrap()),
            );

            let err = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits,
            )
            .await
            .unwrap_err();
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
        }
    }

    // 13. Zero-page error precedence and zero payload opens
    #[tokio::test]
    async fn test_zero_page_error_precedence_and_zero_payload_opens() {
        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);
        let payload_limits = super::super::tag_read::TagReadLimits::default();
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();

        // 1. Invalid repo name fails closed even with page_limit == 0
        {
            let enumerator = MockTagDirEnumerator::new();
            let payload_reader = MockTagPayloadReader::new();
            let err = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "invalid/../repo",
                None,
                0,
                probe_limits,
                tags_limits,
                payload_limits.clone(),
            )
            .await
            .unwrap_err();
            assert!(matches!(err, StorageError::InvalidRepoName(_)));
        }

        // 2. Directory unreadable (PermissionDenied) fails closed even with page_limit == 0
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Err(FsDirError::PermissionDenied {
                    path: Some("repos/myrepo/tags".to_string()),
                    source: std::io::Error::from_raw_os_error(libc::EACCES),
                }),
            );
            let payload_reader = MockTagPayloadReader::new();
            let err = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                0,
                probe_limits,
                tags_limits,
                payload_limits.clone(),
            )
            .await
            .unwrap_err();
            assert_eq!(
                err.internal_kind(),
                Some(StorageErrorKind::PermissionDenied)
            );
        }

        // 3. Directory accessible with unreadable/corrupt tag files: page_limit == 0 performs ZERO payload opens
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![DirEntry::new(
                    OsString::from("t1"),
                    DirEntryType::Regular,
                )]),
            );
            let payload_reader = MockTagPayloadReader::new();
            // Zero payload opens configured; any call to open_payload will panic
            let (page, next) = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                0,
                probe_limits,
                tags_limits,
                payload_limits,
            )
            .await
            .expect("page_limit == 0 succeeds after directory enumeration");
            assert!(page.is_empty());
            assert_eq!(next, None);
            assert_eq!(payload_reader.recorded_opens().len(), 0);
        }
    }

    // 14. Raw unusual cursors, terminal/missing anchors, zero size, and usize::MAX
    #[tokio::test]
    async fn test_raw_unusual_cursors_terminal_missing_zero_size_usize_max() {
        let enumerator = MockTagDirEnumerator::new();
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();
        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);
        let payload_limits = super::super::tag_read::TagReadLimits::default();

        let long_cursor = "z".repeat(10 * 1024);
        let test_cursors = ["", "t0_before", "t2", "t2.5", "t3 🏷️", &long_cursor];

        for cursor in test_cursors {
            enumerator.script(
                Some(&tags_key),
                Ok(vec![
                    DirEntry::new(OsString::from("t1"), DirEntryType::Regular),
                    DirEntry::new(OsString::from("t2"), DirEntryType::Regular),
                    DirEntry::new(OsString::from("t3"), DirEntryType::Regular),
                ]),
            );
            let payload_reader = MockTagPayloadReader::new();
            payload_reader.script_bytes(
                ObjectKey::parse("repos/myrepo/tags/t1").unwrap(),
                format!("sha256:{hex}").into_bytes(),
            );
            payload_reader.script_bytes(
                ObjectKey::parse("repos/myrepo/tags/t2").unwrap(),
                format!("sha256:{hex}").into_bytes(),
            );
            payload_reader.script_bytes(
                ObjectKey::parse("repos/myrepo/tags/t3").unwrap(),
                format!("sha256:{hex}").into_bytes(),
            );

            // Using usize::MAX as page_limit does not panic or overflow
            let (page, next) = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                Some(cursor),
                usize::MAX,
                probe_limits,
                tags_limits,
                payload_limits.clone(),
            )
            .await
            .expect("should not overflow with usize::MAX");

            assert_eq!(next, None);
            if cursor == "t2" {
                assert_eq!(page.len(), 1);
                assert_eq!(page[0].0, "t3");
            } else if cursor == &long_cursor {
                assert_eq!(page.len(), 0);
            }
        }
    }

    // 15. Deterministic insertion, deletion, and target modification between pages
    #[tokio::test]
    async fn test_deterministic_inter_page_mutations_insertion_deletion_modification() {
        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);
        let payload_limits = super::super::tag_read::TagReadLimits::default();
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();
        let hex1 = "1111111111111111111111111111111111111111111111111111111111111111";
        let hex2 = "2222222222222222222222222222222222222222222222222222222222222222";
        let hex3 = "3333333333333333333333333333333333333333333333333333333333333333";
        let hex_updated = "9999999999999999999999999999999999999999999999999999999999999999";

        let enumerator = MockTagDirEnumerator::new();
        let payload_reader = MockTagPayloadReader::new();

        // Page 1: t1, t2
        enumerator.script(
            Some(&tags_key),
            Ok(vec![
                DirEntry::new(OsString::from("t1"), DirEntryType::Regular),
                DirEntry::new(OsString::from("t2"), DirEntryType::Regular),
                DirEntry::new(OsString::from("t3"), DirEntryType::Regular),
            ]),
        );
        payload_reader.script_bytes(
            ObjectKey::parse("repos/myrepo/tags/t1").unwrap(),
            format!("sha256:{hex1}").into_bytes(),
        );
        payload_reader.script_bytes(
            ObjectKey::parse("repos/myrepo/tags/t2").unwrap(),
            format!("sha256:{hex2}").into_bytes(),
        );
        payload_reader.script_bytes(
            ObjectKey::parse("repos/myrepo/tags/t3").unwrap(),
            format!("sha256:{hex3}").into_bytes(),
        );

        let (page1, next1) = contained_list_tags_page_seam(
            &enumerator,
            &payload_reader,
            "myrepo",
            None,
            2,
            probe_limits,
            tags_limits,
            payload_limits.clone(),
        )
        .await
        .expect("page 1 succeeds");
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].0, "t1");
        assert_eq!(page1[1].0, "t2");
        assert_eq!(next1.as_deref(), Some("t2"));

        // Inter-page mutations:
        // - Insertion: t2.5 inserted
        // - Modification: t3 modified to hex_updated
        enumerator.script(
            Some(&tags_key),
            Ok(vec![
                DirEntry::new(OsString::from("t1"), DirEntryType::Regular),
                DirEntry::new(OsString::from("t2"), DirEntryType::Regular),
                DirEntry::new(OsString::from("t2.5"), DirEntryType::Regular),
                DirEntry::new(OsString::from("t3"), DirEntryType::Regular),
            ]),
        );
        payload_reader.script_bytes(
            ObjectKey::parse("repos/myrepo/tags/t1").unwrap(),
            format!("sha256:{hex1}").into_bytes(),
        );
        payload_reader.script_bytes(
            ObjectKey::parse("repos/myrepo/tags/t2").unwrap(),
            format!("sha256:{hex2}").into_bytes(),
        );
        payload_reader.script_bytes(
            ObjectKey::parse("repos/myrepo/tags/t2.5").unwrap(),
            format!("sha256:{hex2}").into_bytes(),
        );
        payload_reader.script_bytes(
            ObjectKey::parse("repos/myrepo/tags/t3").unwrap(),
            format!("sha256:{hex_updated}").into_bytes(),
        );

        let (page2, next2) = contained_list_tags_page_seam(
            &enumerator,
            &payload_reader,
            "myrepo",
            next1.as_deref(),
            10,
            probe_limits,
            tags_limits,
            payload_limits,
        )
        .await
        .expect("page 2 succeeds");

        assert_eq!(page2.len(), 2);
        assert_eq!(page2[0].0, "t2.5");
        assert_eq!(page2[1].0, "t3");
        assert_eq!(page2[1].1.to_string(), format!("sha256:{hex_updated}"));
        assert_eq!(next2, None);
    }

    // 16. Stream tests: partial bytes followed by explicit I/O error vs normal EOF
    #[tokio::test]
    async fn test_stream_partial_io_error_vs_clean_eof_valid_digest_vs_malformed_omission() {
        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);
        let payload_limits = super::super::tag_read::TagReadLimits::default();
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();

        // Subcase A: Stream returns initial bytes then explicit std::io::Error -> fails closed with Io
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![DirEntry::new(
                    OsString::from("t1"),
                    DirEntryType::Regular,
                )]),
            );
            let payload_reader = MockTagPayloadReader::new();
            let key = ObjectKey::parse("repos/myrepo/tags/t1").unwrap();
            let meta = ObjectMetadata::new(71);
            payload_reader.script_payload(
                key,
                meta,
                MockPayloadBody::PartialThenError {
                    data: b"sha256:12345678".to_vec(),
                    error_kind: std::io::ErrorKind::UnexpectedEof,
                    error_message: "simulated network stream drop".into(),
                },
            );

            let err = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits.clone(),
            )
            .await
            .unwrap_err();

            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
        }

        // Subcase A2: Stream returns immediate std::io::Error -> fails closed with Io
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![DirEntry::new(
                    OsString::from("t1"),
                    DirEntryType::Regular,
                )]),
            );
            let payload_reader = MockTagPayloadReader::new();
            let key = ObjectKey::parse("repos/myrepo/tags/t1").unwrap();
            let meta = ObjectMetadata::new(71);
            payload_reader.script_payload(
                key,
                meta,
                MockPayloadBody::ImmediateIoError {
                    error_kind: std::io::ErrorKind::ConnectionReset,
                    error_message: "immediate connection reset".into(),
                },
            );

            let err = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits.clone(),
            )
            .await
            .unwrap_err();

            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
        }

        // Subcase B: Clean EOF containing malformed text -> omitted under proposed omission policy
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![
                    DirEntry::new(OsString::from("empty"), DirEntryType::Regular),
                    DirEntry::new(OsString::from("malformed"), DirEntryType::Regular),
                    DirEntry::new(OsString::from("valid"), DirEntryType::Regular),
                ]),
            );
            let payload_reader = MockTagPayloadReader::new();
            payload_reader.script_bytes(
                ObjectKey::parse("repos/myrepo/tags/empty").unwrap(),
                b"".to_vec(),
            );
            payload_reader.script_bytes(
                ObjectKey::parse("repos/myrepo/tags/malformed").unwrap(),
                b"not-a-digest".to_vec(),
            );
            let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
            payload_reader.script_bytes(
                ObjectKey::parse("repos/myrepo/tags/valid").unwrap(),
                format!("sha256:{hex}").into_bytes(),
            );

            let (page, _) = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits.clone(),
            )
            .await
            .expect("should succeed with corrupt tags omitted");

            assert_eq!(page.len(), 1);
            assert_eq!(page[0].0, "valid");
        }

        // Subcase C: Invalid UTF-8 bytes -> fails closed with Io
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![DirEntry::new(
                    OsString::from("bad_utf8"),
                    DirEntryType::Regular,
                )]),
            );
            let payload_reader = MockTagPayloadReader::new();
            payload_reader.script_bytes(
                ObjectKey::parse("repos/myrepo/tags/bad_utf8").unwrap(),
                vec![0xff, 0xfe, 0xfd],
            );

            let err = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits,
            )
            .await
            .unwrap_err();

            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
        }
    }

    // 17. Metadata length does not enforce expected stream length
    #[tokio::test]
    async fn test_metadata_length_not_enforced_as_stream_length() {
        let enumerator = MockTagDirEnumerator::new();
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();
        enumerator.script(
            Some(&tags_key),
            Ok(vec![DirEntry::new(
                OsString::from("t1"),
                DirEntryType::Regular,
            )]),
        );

        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let valid_digest_bytes = format!("sha256:{hex}").into_bytes();

        let payload_reader = MockTagPayloadReader::new();
        let meta = ObjectMetadata::new(5000);
        payload_reader.script_payload(
            ObjectKey::parse("repos/myrepo/tags/t1").unwrap(),
            meta,
            MockPayloadBody::Complete(valid_digest_bytes),
        );

        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);
        let payload_limits = super::super::tag_read::TagReadLimits::default();

        let (page, _) = contained_list_tags_page_seam(
            &enumerator,
            &payload_reader,
            "myrepo",
            None,
            10,
            probe_limits,
            tags_limits,
            payload_limits,
        )
        .await
        .expect("metadata length mismatch should not fail stream parsing");

        assert_eq!(page.len(), 1);
        assert_eq!(page[0].0, "t1");
    }

    // 18. Real filesystem tests using storage_fs::FsMetadataReader with root renaming and replacement
    #[tokio::test]
    async fn test_real_filesystem_contained_listing_and_root_pinning() {
        let parent_dir = tempfile::tempdir().expect("tempdir");
        let original_root = parent_dir.path().join("original_root");

        // Create repos/alpine/tags directory structure under original_root
        let tags_path = original_root.join("repos/alpine/tags");
        std::fs::create_dir_all(&tags_path).expect("create_dir_all original tags");

        let hex_orig1 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let hex_orig2 = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
        std::fs::write(
            tags_path.join("3.18"),
            format!(
                "sha256:{hex_orig1}
"
            ),
        )
        .expect("write 3.18");
        std::fs::write(
            tags_path.join("latest"),
            format!(
                "sha256:{hex_orig2}
"
            ),
        )
        .expect("write latest");
        // Dotfile
        std::fs::write(tags_path.join(".tmp.upload"), "dotfile").expect("write dotfile");
        // Nested directory
        std::fs::create_dir(tags_path.join("nested_dir")).expect("create nested_dir");

        let reader = Arc::new(
            storage_fs::FsMetadataReader::open(&original_root).expect("FsMetadataReader::open"),
        );

        let probe_limits = DirEnumerationLimits::new(100, 10240);
        let tags_limits = DirEnumerationLimits::new(100, 10240);
        let payload_limits = super::super::tag_read::TagReadLimits::default();

        // Real name-only listing on original root
        let names = contained_list_tags_seam(&*reader, "alpine", probe_limits, tags_limits)
            .await
            .expect("real contained_list_tags_seam");
        assert_eq!(names, vec!["3.18", "latest"]);

        // Real paged listing on original root with explicit shared reader identity
        let (page, next) = contained_list_tags_page_seam(
            &*reader,
            &*reader,
            "alpine",
            None,
            1,
            probe_limits,
            tags_limits,
            payload_limits.clone(),
        )
        .await
        .expect("real contained_list_tags_page_seam");
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].0, "3.18");
        assert_eq!(page[0].1.to_string(), format!("sha256:{hex_orig1}"));
        assert_eq!(next.as_deref(), Some("3.18"));

        // Rename original root to moved_root inside test-owned parent directory
        let moved_root = parent_dir.path().join("moved_root");
        std::fs::rename(&original_root, &moved_root).expect("rename original root");

        // Install a replacement tree at the original pathname with distinguishable names and digests
        let replacement_tags = original_root.join("repos/alpine/tags");
        std::fs::create_dir_all(&replacement_tags).expect("create replacement tags");
        let hex_rep = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        std::fs::write(
            replacement_tags.join("replacement_tag"),
            format!(
                "sha256:{hex_rep}
"
            ),
        )
        .expect("write replacement tag");

        // Verify BOTH name-only and paged payload reads continue using the pinned original tree
        let names_pinned = contained_list_tags_seam(&*reader, "alpine", probe_limits, tags_limits)
            .await
            .expect("reader should continue observing pinned tree descriptor");
        assert_eq!(names_pinned, vec!["3.18", "latest"]);

        let (page_pinned, next_pinned) = contained_list_tags_page_seam(
            &*reader,
            &*reader,
            "alpine",
            None,
            10,
            probe_limits,
            tags_limits,
            payload_limits,
        )
        .await
        .expect("paged read should continue observing pinned tree descriptor");
        assert_eq!(page_pinned.len(), 2);
        assert_eq!(page_pinned[0].0, "3.18");
        assert_eq!(page_pinned[0].1.to_string(), format!("sha256:{hex_orig1}"));
        assert_eq!(page_pinned[1].0, "latest");
        assert_eq!(page_pinned[1].1.to_string(), format!("sha256:{hex_orig2}"));
        assert_eq!(next_pinned, None);
    }

    // Helper wrapper for deterministic replacement hooks
    struct ReplacingDirEnumerator<'a> {
        inner: &'a storage_fs::FsMetadataReader,
        on_enumerate: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }

    #[async_trait]
    impl<'a> TagDirEnumerator for ReplacingDirEnumerator<'a> {
        async fn enumerate_dir(
            &self,
            target: Option<&ObjectKey>,
            limits: storage_fs::DirEnumerationLimits,
        ) -> Result<Vec<storage_fs::DirEntry>, storage_fs::FsDirError> {
            let res = self.inner.enumerate_dir(target, limits).await?;
            if let Some(hook) = self.on_enumerate.lock().unwrap().take() {
                hook();
            }
            Ok(res)
        }
    }

    // 19a. Real filesystem candidate replaced with symlink to controlled external fixture
    #[tokio::test]
    async fn test_real_filesystem_candidate_replaced_with_symlink() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let root_path = temp_dir.path().to_path_buf();
        let tags_path = root_path.join("repos/myrepo/tags");
        std::fs::create_dir_all(&tags_path).expect("create_dir_all");

        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let target_file = tags_path.join("t1");
        std::fs::write(
            &target_file,
            format!(
                "sha256:{hex}
"
            ),
        )
        .expect("write t1");

        // Controlled external fixture in test-owned temporary directory
        let fixture_dir = tempfile::tempdir().expect("fixture tempdir");
        let controlled_target = fixture_dir.path().join("external_fixture");
        std::fs::write(&controlled_target, b"external payload content").expect("write fixture");

        let reader = storage_fs::FsMetadataReader::open(&root_path).expect("open");

        let target_file_clone = target_file.clone();
        let replacing_enumerator = ReplacingDirEnumerator {
            inner: &reader,
            on_enumerate: Mutex::new(Some(Box::new(move || {
                std::fs::remove_file(&target_file_clone).expect("remove regular t1");
                std::os::unix::fs::symlink(&controlled_target, &target_file_clone)
                    .expect("symlink replace with controlled fixture");
            }))),
        };

        let probe_limits = DirEnumerationLimits::new(100, 10240);
        let tags_limits = DirEnumerationLimits::new(100, 10240);
        let payload_limits = super::super::tag_read::TagReadLimits::default();

        let err = contained_list_tags_page_seam(
            &replacing_enumerator,
            &reader,
            "myrepo",
            None,
            10,
            probe_limits,
            tags_limits,
            payload_limits,
        )
        .await
        .unwrap_err();

        // Must fail closed with Io (ResolutionRejected translated to Io)
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
    }

    // 19b. Real filesystem candidate replaced with directory
    #[tokio::test]
    async fn test_real_filesystem_candidate_replaced_with_directory() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let root_path = temp_dir.path().to_path_buf();
        let tags_path = root_path.join("repos/myrepo/tags");
        std::fs::create_dir_all(&tags_path).expect("create_dir_all");

        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let target_file = tags_path.join("t1");
        std::fs::write(
            &target_file,
            format!(
                "sha256:{hex}
"
            ),
        )
        .expect("write t1");

        let reader = storage_fs::FsMetadataReader::open(&root_path).expect("open");

        let target_file_clone = target_file.clone();
        let replacing_enumerator = ReplacingDirEnumerator {
            inner: &reader,
            on_enumerate: Mutex::new(Some(Box::new(move || {
                std::fs::remove_file(&target_file_clone).expect("remove regular t1");
                std::fs::create_dir(&target_file_clone).expect("create replacement directory");
            }))),
        };

        let probe_limits = DirEnumerationLimits::new(100, 10240);
        let tags_limits = DirEnumerationLimits::new(100, 10240);
        let payload_limits = super::super::tag_read::TagReadLimits::default();

        let err = contained_list_tags_page_seam(
            &replacing_enumerator,
            &reader,
            "myrepo",
            None,
            10,
            probe_limits,
            tags_limits,
            payload_limits,
        )
        .await
        .unwrap_err();

        // Must fail closed with Io (UnsupportedObjectType S_IFDIR translated to Io)
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
    }

    // 19c. Real filesystem candidate replaced with non-regular object (Unix domain socket)
    #[tokio::test]
    async fn test_real_filesystem_candidate_replaced_with_unix_socket() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let root_path = temp_dir.path().to_path_buf();
        let tags_path = root_path.join("repos/myrepo/tags");
        std::fs::create_dir_all(&tags_path).expect("create_dir_all");

        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let target_file = tags_path.join("t1");
        std::fs::write(
            &target_file,
            format!(
                "sha256:{hex}
"
            ),
        )
        .expect("write t1");

        let reader = storage_fs::FsMetadataReader::open(&root_path).expect("open");

        let target_file_clone = target_file.clone();
        let socket_holder = Arc::new(Mutex::new(None::<UnixListener>));
        let socket_holder_clone = socket_holder.clone();

        let replacing_enumerator = ReplacingDirEnumerator {
            inner: &reader,
            on_enumerate: Mutex::new(Some(Box::new(move || {
                std::fs::remove_file(&target_file_clone).expect("remove regular t1");
                let listener = UnixListener::bind(&target_file_clone).expect("bind unix socket");
                *socket_holder_clone.lock().unwrap() = Some(listener);
            }))),
        };

        let probe_limits = DirEnumerationLimits::new(100, 10240);
        let tags_limits = DirEnumerationLimits::new(100, 10240);
        let payload_limits = super::super::tag_read::TagReadLimits::default();

        let err = contained_list_tags_page_seam(
            &replacing_enumerator,
            &reader,
            "myrepo",
            None,
            10,
            probe_limits,
            tags_limits,
            payload_limits,
        )
        .await
        .unwrap_err();

        // Must fail closed with Io (UnsupportedObjectType S_IFSOCK translated to Io)
        assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
    }

    // 20. Complete directory error mappings including real Tokio errors
    #[tokio::test]
    async fn test_directory_error_mappings_complete() {
        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();

        // NotADirectory -> CorruptData
        {
            let err = translate_tag_dir_error(
                FsDirError::NotADirectory {
                    path: Some("repos/myrepo/tags".to_string()),
                },
                "repos/myrepo/tags",
            );
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::CorruptData));

            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Err(FsDirError::NotADirectory {
                    path: Some("repos/myrepo/tags".to_string()),
                }),
            );
            let seam_err =
                contained_list_tags_seam(&enumerator, "myrepo", probe_limits, tags_limits)
                    .await
                    .unwrap_err();
            assert_eq!(
                seam_err.internal_kind(),
                Some(StorageErrorKind::CorruptData)
            );
        }

        // SyscallUnsupported -> Configuration
        {
            let err = translate_tag_dir_error(
                FsDirError::SyscallUnsupported(std::io::Error::from_raw_os_error(libc::ENOSYS)),
                "repos/myrepo/tags",
            );
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Configuration));

            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Err(FsDirError::SyscallUnsupported(
                    std::io::Error::from_raw_os_error(libc::ENOSYS),
                )),
            );
            let seam_err =
                contained_list_tags_seam(&enumerator, "myrepo", probe_limits, tags_limits)
                    .await
                    .unwrap_err();
            assert_eq!(
                seam_err.internal_kind(),
                Some(StorageErrorKind::Configuration)
            );
        }

        // Generic Io -> Io
        {
            let err = translate_tag_dir_error(
                FsDirError::Io {
                    source: std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken pipe"),
                },
                "repos/myrepo/tags",
            );
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));

            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Err(FsDirError::Io {
                    source: std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken pipe"),
                }),
            );
            let seam_err =
                contained_list_tags_seam(&enumerator, "myrepo", probe_limits, tags_limits)
                    .await
                    .unwrap_err();
            assert_eq!(seam_err.internal_kind(), Some(StorageErrorKind::Io));
        }

        // RuntimeMissing with real TryCurrentError -> Backend
        {
            let rt_err = std::thread::spawn(|| tokio::runtime::Handle::try_current().unwrap_err())
                .join()
                .expect("join thread");
            let err =
                translate_tag_dir_error(FsDirError::RuntimeMissing(rt_err), "repos/myrepo/tags");
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Backend));

            let rt_err2 = std::thread::spawn(|| tokio::runtime::Handle::try_current().unwrap_err())
                .join()
                .expect("join thread");
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(Some(&tags_key), Err(FsDirError::RuntimeMissing(rt_err2)));
            let seam_err =
                contained_list_tags_seam(&enumerator, "myrepo", probe_limits, tags_limits)
                    .await
                    .unwrap_err();
            assert_eq!(seam_err.internal_kind(), Some(StorageErrorKind::Backend));
        }

        // TaskJoinFailed with real JoinError -> Backend
        {
            let handle = tokio::spawn(async { std::future::pending::<()>().await });
            handle.abort();
            let join_err = handle.await.unwrap_err();
            assert!(join_err.is_cancelled());

            let err =
                translate_tag_dir_error(FsDirError::TaskJoinFailed(join_err), "repos/myrepo/tags");
            assert_eq!(err.internal_kind(), Some(StorageErrorKind::Backend));

            let handle2 = tokio::spawn(async { std::future::pending::<()>().await });
            handle2.abort();
            let join_err2 = handle2.await.unwrap_err();
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(Some(&tags_key), Err(FsDirError::TaskJoinFailed(join_err2)));
            let seam_err =
                contained_list_tags_seam(&enumerator, "myrepo", probe_limits, tags_limits)
                    .await
                    .unwrap_err();
            assert_eq!(seam_err.internal_kind(), Some(StorageErrorKind::Backend));
        }
    }

    // 21. Complete payload acquisition error mappings distinct from directory mappings
    #[tokio::test]
    async fn test_payload_error_mappings_complete_and_distinct_from_directory() {
        let probe_limits = DirEnumerationLimits::new(10, 1024);
        let tags_limits = DirEnumerationLimits::new(10, 1024);
        let payload_limits = super::super::tag_read::TagReadLimits::default();
        let tags_key = ObjectKey::parse("repos/myrepo/tags").unwrap();
        let tag_key = ObjectKey::parse("repos/myrepo/tags/t1").unwrap();

        // 1. Payload PermissionDenied maps to StorageErrorKind::Io
        // Distinct from directory PermissionDenied which maps to StorageErrorKind::PermissionDenied
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![DirEntry::new(
                    OsString::from("t1"),
                    DirEntryType::Regular,
                )]),
            );
            let payload_reader = MockTagPayloadReader::new();
            payload_reader.script_open_error(
                tag_key.clone(),
                ReadError::permission_denied(tag_key.clone()),
            );

            let seam_err = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits.clone(),
            )
            .await
            .unwrap_err();

            assert_eq!(seam_err.internal_kind(), Some(StorageErrorKind::Io));

            let dir_err = translate_tag_dir_error(
                FsDirError::PermissionDenied {
                    path: Some("repos/myrepo/tags".to_string()),
                    source: std::io::Error::from_raw_os_error(libc::EACCES),
                },
                "repos/myrepo/tags",
            );
            assert_eq!(
                dir_err.internal_kind(),
                Some(StorageErrorKind::PermissionDenied)
            );
        }

        // 2. Payload Backend with RuntimeMissing -> StorageErrorKind::Backend
        {
            let rt_err = std::thread::spawn(|| tokio::runtime::Handle::try_current().unwrap_err())
                .join()
                .expect("join thread");
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![DirEntry::new(
                    OsString::from("t1"),
                    DirEntryType::Regular,
                )]),
            );
            let payload_reader = MockTagPayloadReader::new();
            payload_reader.script_open_error(
                tag_key.clone(),
                ReadError::backend_with_source(
                    "runtime missing",
                    Box::new(FsMetadataError::RuntimeMissing(rt_err)),
                ),
            );

            let seam_err = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits.clone(),
            )
            .await
            .unwrap_err();
            assert_eq!(seam_err.internal_kind(), Some(StorageErrorKind::Backend));
        }

        // 3. Payload Backend with TaskJoinFailed -> StorageErrorKind::Backend
        {
            let handle = tokio::spawn(async { std::future::pending::<()>().await });
            handle.abort();
            let join_err = handle.await.unwrap_err();

            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![DirEntry::new(
                    OsString::from("t1"),
                    DirEntryType::Regular,
                )]),
            );
            let payload_reader = MockTagPayloadReader::new();
            payload_reader.script_open_error(
                tag_key.clone(),
                ReadError::backend_with_source(
                    "join failed",
                    Box::new(FsMetadataError::TaskJoinFailed(join_err)),
                ),
            );

            let seam_err = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits.clone(),
            )
            .await
            .unwrap_err();
            assert_eq!(seam_err.internal_kind(), Some(StorageErrorKind::Backend));
        }

        // 4. Payload Backend with SyscallUnsupported -> StorageErrorKind::Configuration
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![DirEntry::new(
                    OsString::from("t1"),
                    DirEntryType::Regular,
                )]),
            );
            let payload_reader = MockTagPayloadReader::new();
            payload_reader.script_open_error(
                tag_key.clone(),
                ReadError::backend_with_source(
                    "syscall unsupported",
                    Box::new(FsMetadataError::SyscallUnsupported(
                        std::io::Error::from_raw_os_error(libc::ENOSYS),
                    )),
                ),
            );

            let seam_err = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits.clone(),
            )
            .await
            .unwrap_err();
            assert_eq!(
                seam_err.internal_kind(),
                Some(StorageErrorKind::Configuration)
            );
        }

        // 5. Payload Backend generic -> StorageErrorKind::Io
        {
            let enumerator = MockTagDirEnumerator::new();
            enumerator.script(
                Some(&tags_key),
                Ok(vec![DirEntry::new(
                    OsString::from("t1"),
                    DirEntryType::Regular,
                )]),
            );
            let payload_reader = MockTagPayloadReader::new();
            payload_reader.script_open_error(
                tag_key,
                ReadError::backend("unexpected generic read failure"),
            );

            let seam_err = contained_list_tags_page_seam(
                &enumerator,
                &payload_reader,
                "myrepo",
                None,
                10,
                probe_limits,
                tags_limits,
                payload_limits,
            )
            .await
            .unwrap_err();
            assert_eq!(seam_err.internal_kind(), Some(StorageErrorKind::Io));
        }
    }

    // 22. Real filesystem unprivileged permission test with RAII restore
    // Kept explicitly ignored so the default seam suite is portable across privilege levels (including root)
    #[tokio::test]
    #[ignore = "requires unprivileged user environment where chmod 0o000 denies filesystem access"]
    async fn test_real_filesystem_permission_denied_unprivileged() {
        assert_ne!(
            unsafe { libc::geteuid() },
            0,
            "permission test requires unprivileged execution"
        );

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let root_path = temp_dir.path().to_path_buf();
        let tags_path = root_path.join("repos/myrepo/tags");
        std::fs::create_dir_all(&tags_path).expect("create_dir_all");

        let reader = storage_fs::FsMetadataReader::open(&root_path).expect("open");

        let _guard = PermGuard(tags_path.clone());
        std::fs::set_permissions(&tags_path, std::fs::Permissions::from_mode(0o000))
            .expect("chmod 000");

        let probe_limits = DirEnumerationLimits::new(100, 10240);
        let tags_limits = DirEnumerationLimits::new(100, 10240);

        let err = contained_list_tags_seam(&reader, "myrepo", probe_limits, tags_limits)
            .await
            .unwrap_err();
        assert_eq!(
            err.internal_kind(),
            Some(StorageErrorKind::PermissionDenied)
        );
    }
}
