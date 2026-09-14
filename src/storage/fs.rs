use super::upload_session::*;
use super::{
    BlobMeta, BlobObjectVersion, GcBlobPage, GcCursor, GcDeleteResult, GcQuarantineResult,
    GcStorage, GcStorageStrategy, ManifestMeta, ReferrerDescriptor, RepoTimestamps, Storage,
    StorageError, ensure_dir,
};
use crate::registry::canonical_name::CanonicalRepoName;
use crate::registry::digest::Digest;
use crate::storage::repo_membership::RepositoryBlobMembershipStorage;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use std::path::Path;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use storage_fs::{BlockingDir, ContainedDir, FileName, FsMutateError, LeafWriteMode};
#[cfg(test)]
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::sync::{Mutex, OnceCell};

const UPLOAD_SHA256_STATE_MAGIC: &[u8; 8] = b"RRSHA256";
const UPLOAD_SHA256_STATE_VERSION: u8 = 1;

#[derive(Clone, Debug)]
struct SerializableSha256 {
    state: [u32; 8],
    buffer: [u8; 64],
    buffer_len: usize,
    total_len: u64,
}

impl SerializableSha256 {
    fn new() -> Self {
        // SHA-256 IV (FIPS 180-4)
        Self {
            state: [
                0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
                0x5be0cd19,
            ],
            buffer: [0u8; 64],
            buffer_len: 0,
            total_len: 0,
        }
    }

    fn update(&mut self, mut input: &[u8]) {
        if input.is_empty() {
            return;
        }

        self.total_len = self.total_len.saturating_add(input.len() as u64);

        // Fill existing buffer to a full block.
        if self.buffer_len > 0 {
            let need = 64 - self.buffer_len;
            let take = need.min(input.len());
            self.buffer[self.buffer_len..self.buffer_len + take].copy_from_slice(&input[..take]);
            self.buffer_len += take;
            input = &input[take..];

            if self.buffer_len == 64 {
                let block = self.buffer;
                self.compress_block(&block);
                self.buffer_len = 0;
            }
        }

        // Process full blocks directly from input.
        while input.len() >= 64 {
            let block: &[u8; 64] = input[..64].try_into().expect("slice length checked");
            self.compress_block(block);
            input = &input[64..];
        }

        // Store remaining tail.
        if !input.is_empty() {
            self.buffer[..input.len()].copy_from_slice(input);
            self.buffer_len = input.len();
        }
    }

    fn compress_block(&mut self, block: &[u8; 64]) {
        use sha2::digest::generic_array::GenericArray;
        use sha2::digest::typenum::U64;
        let mut ga = GenericArray::<u8, U64>::default();
        ga.copy_from_slice(block);
        sha2::compress256(&mut self.state, std::slice::from_ref(&ga));
    }

    fn finalize_hex(&self) -> String {
        let mut tmp = self.clone();
        let bit_len = tmp.total_len.saturating_mul(8);

        // Padding: 0x80, then 0x00 until length mod 64 == 56, then 64-bit big-endian length.
        let mut pad = [0u8; 128];
        pad[0] = 0x80;

        let rem = (tmp.total_len % 64) as usize;
        let pad_len = if rem < 56 { 56 - rem } else { 56 + 64 - rem };
        tmp.update(&pad[..pad_len]);

        let mut len_bytes = [0u8; 8];
        len_bytes.copy_from_slice(&bit_len.to_be_bytes());
        tmp.update(&len_bytes);

        // Output is state words in big-endian.
        let mut out = [0u8; 32];
        for (i, w) in tmp.state.iter().enumerate() {
            out[i * 4..i * 4 + 4].copy_from_slice(&w.to_be_bytes());
        }
        hex::encode(out)
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + 1 + 8 + 1 + 64 + 32);
        out.extend_from_slice(UPLOAD_SHA256_STATE_MAGIC);
        out.push(UPLOAD_SHA256_STATE_VERSION);
        out.extend_from_slice(&self.total_len.to_le_bytes());
        out.push(self.buffer_len.min(64) as u8);
        out.extend_from_slice(&self.buffer);
        for w in self.state {
            out.extend_from_slice(&w.to_le_bytes());
        }
        out
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let need = 8 + 1 + 8 + 1 + 64 + 32;
        if bytes.len() != need {
            return None;
        }
        if &bytes[..8] != UPLOAD_SHA256_STATE_MAGIC {
            return None;
        }
        if bytes[8] != UPLOAD_SHA256_STATE_VERSION {
            return None;
        }

        let total_len = u64::from_le_bytes(bytes[9..17].try_into().ok()?);
        let buffer_len = bytes[17] as usize;
        if buffer_len > 64 {
            return None;
        }
        let mut buffer = [0u8; 64];
        buffer.copy_from_slice(&bytes[18..82]);

        let mut state = [0u32; 8];
        let mut off = 82;
        for i in 0..8 {
            state[i] = u32::from_le_bytes(bytes[off..off + 4].try_into().ok()?);
            off += 4;
        }

        Some(Self {
            state,
            buffer,
            buffer_len,
            total_len,
        })
    }
}

/// Filesystem repository path codec: safely computes the directory path for a validated repository name,
/// ensuring strict containment under `<base_root>/repos/`.
pub(crate) fn fs_repo_dir(
    base_root: &Path,
    repo: &CanonicalRepoName,
) -> Result<PathBuf, StorageError> {
    let path = base_root.join("repos").join(repo.as_str());
    for component in path.components() {
        if let std::path::Component::ParentDir = component {
            return Err(StorageError::InvalidRepoName(
                "path traversal attempt detected in repository path".to_string(),
            ));
        }
    }
    Ok(path)
}

const HASH_SHARDS: usize = 64;
const REFERRER_SHARDS: usize = 64;

fn shard_index(key: &str, num_shards: usize) -> usize {
    let mut hasher = std::hash::DefaultHasher::new();
    std::hash::Hash::hash(key, &mut hasher);
    std::hash::Hasher::finish(&hasher) as usize % num_shards
}

// ============================================================================
// Contained upload-lifecycle authorities (Option A)
// ============================================================================
//
// Every upload-lifecycle participant (session create/status/append/finalize/
// commit/recover/abort, the expiry reaper, and the receipt/CAS/membership
// helpers they call) resolves its targets beneath ONE shared authority per
// subtree: the storage *root* is pinned once at construction, and each subtree
// (`uploads`, `uploads/.finalized`, `blobs`, `repo-memberships`) is pinned
// exactly once, lazily, and then shared for the process lifetime via
// `Arc<UploadAuthorities>` (see `UploadAuthorities` below for the once-init and
// guarantee-scope contract). Because a pinned descriptor follows the inode it
// was opened on, replacing the *pathname* of a pinned ancestor cannot redirect a
// later operation: inspection and the destructive actions it authorizes always
// resolve through the same (possibly detached) tree, so a detached tree's expiry
// can never drive deletion of a same-name record in a fresh replacement tree.
// That an out-of-band pathname replacement is *not* silently adopted mid-flight
// is the point — it is the invariant that keeps lock acquisition, reads, and
// mutations coherent for the life of an authority; adopting a replacement is a
// deliberate, explicit act (re-deriving the authorities, i.e. a restart), never
// an implicit reopen. See
// `docs/architecture/filesystem-upload-lifecycle-contained-cleanup.md`.

/// Bounded read budgets for the small contained session records. Session data
/// payloads are never slurped through these; they are streamed via owned
/// descriptors.
const SESSION_META_READ_LIMIT: u64 = 1 << 20;
const SESSION_HASH_READ_LIMIT: u64 = 4096;
const FINALIZED_RECEIPT_READ_LIMIT: u64 = 1 << 16;

/// The contained upload-lifecycle authority.
///
/// The storage *root* descriptor is pinned once at construction. Each lifecycle
/// subtree (`uploads`, `uploads/.finalized`, `blobs`, `repo-memberships`) is a
/// distinct pinned authority that is initialized **exactly once, lazily, and
/// shared** across every participant through a `tokio::sync::OnceCell`:
///
/// * The first operation that needs a subtree runs `ensure_subdir` beneath the
///   pinned root (or, for `.finalized`, beneath the shared `uploads` authority)
///   and installs the resulting pinned descriptor in the cell.
/// * Every later call — from any task, on any `FsStorage` clone that shares this
///   `Arc<UploadAuthorities>` — receives that *same cached descriptor*. Directory
///   and lock identity are therefore stable across calls: once `uploads` is
///   pinned, a later rename/replacement of the `uploads` *pathname* on disk
///   cannot redirect any operation, because no operation re-resolves the pathname
///   — every access, lock, read, write, recovery, abort, receipt, and
///   publication step flows through the one cached inode.
/// * `OnceCell::get_or_try_init` guarantees concurrent initializers cannot install
///   competing authorities: exactly one initialization runs to success and every
///   racing caller observes that one result. An initialization *failure* (a
///   symlink squatting the subtree name, a permission error, …) is surfaced to
///   the caller and leaves the cell empty, so a later attempt may retry;
///   construction stays total and no directory is materialized until a real
///   lifecycle operation demands it.
///
/// **Guarantee scope.** The sharing is *intra-instance*. All clones of one
/// `FsStorage` (held behind an `Arc`) share these cells, so every task in one
/// process operating through one `FsStorage` observes one coherent authority per
/// subtree. Two independent `FsStorage` instances — or two OS processes — each
/// pin their own root and their own cells and do **not** share cached
/// descriptors; an `Arc` within one instance implies nothing about another
/// instance. Cross-instance / cross-process mutual exclusion rests solely on the
/// stable on-disk `.lock.{uuid}` flock domain (a kernel lock keyed by inode),
/// not on any in-memory sharing. See
/// `docs/architecture/filesystem-upload-lifecycle-contained-cleanup.md`.
#[derive(Debug)]
struct UploadAuthorities {
    /// The pinned storage root. All lifecycle subtrees resolve beneath this fd.
    root: ContainedDir,
    /// `<root>/uploads`, pinned once and shared.
    uploads: OnceCell<ContainedDir>,
    /// `<root>/uploads/.finalized`, pinned once beneath the shared `uploads`.
    finalized: OnceCell<ContainedDir>,
    /// `<root>/blobs`, pinned once and shared.
    blobs: OnceCell<ContainedDir>,
    /// `<root>/repo-memberships`, pinned once and shared.
    memberships: OnceCell<ContainedDir>,
    /// `<root>/repos`, pinned once and shared. This is the fixed top-level
    /// repository namespace root; per-`<repository>` authorities are **not**
    /// cached — each tag mutation re-resolves `repos/<repo>/tags` freshly
    /// beneath this pinned dir (see the tag-mutation contained-integration
    /// design, Decision A). Caching stops at this never-deleted top level so a
    /// deleted/recreated repository inode is always observed on the next
    /// operation, keeping tag reads and writes coherent.
    repos: OnceCell<ContainedDir>,
    /// `<root>/quarantine`, pinned once and shared (GC quarantine payloads and
    /// timestamp metadata).
    quarantine: OnceCell<ContainedDir>,
}

impl UploadAuthorities {
    /// Pin the storage root beneath `reader`. Fully synchronous (no runtime
    /// required), so it is usable from the synchronous `FsStorage` constructors.
    /// No subtree directories are created here: each is pinned lazily-once by its
    /// accessor, resolving beneath the pinned root inode.
    fn capture(reader: &storage_fs::FsMetadataReader) -> Result<Self, StorageError> {
        let root = reader
            .open_contained_dir_sync("")
            .map_err(map_fs_mutate_startup_err)?;
        Ok(Self {
            root,
            uploads: OnceCell::new(),
            finalized: OnceCell::new(),
            blobs: OnceCell::new(),
            memberships: OnceCell::new(),
            repos: OnceCell::new(),
            quarantine: OnceCell::new(),
        })
    }

    /// The shared `<root>/uploads` authority (session data/meta/hash leaves and
    /// the lock domain). Pinned exactly once and shared thereafter.
    async fn uploads(&self) -> Result<ContainedDir, FsMutateError> {
        self.uploads
            .get_or_try_init(|| {
                let root = self.root.clone();
                async move { root.ensure_subdir(&FileName::new("uploads")?).await }
            })
            .await
            .map(ContainedDir::clone)
    }

    /// The shared `<root>/uploads/.finalized` authority (finalized receipts),
    /// pinned beneath the shared `uploads` authority.
    async fn finalized(&self) -> Result<ContainedDir, FsMutateError> {
        let uploads = self.uploads().await?;
        self.finalized
            .get_or_try_init(|| async move {
                uploads.ensure_subdir(&FileName::new(".finalized")?).await
            })
            .await
            .map(ContainedDir::clone)
    }

    /// The shared `<root>/blobs` CAS authority.
    async fn blobs(&self) -> Result<ContainedDir, FsMutateError> {
        self.blobs
            .get_or_try_init(|| {
                let root = self.root.clone();
                async move { root.ensure_subdir(&FileName::new("blobs")?).await }
            })
            .await
            .map(ContainedDir::clone)
    }

    /// The shared `<root>/repo-memberships` authority.
    async fn memberships(&self) -> Result<ContainedDir, FsMutateError> {
        self.memberships
            .get_or_try_init(|| {
                let root = self.root.clone();
                async move {
                    root.ensure_subdir(&FileName::new("repo-memberships")?)
                        .await
                }
            })
            .await
            .map(ContainedDir::clone)
    }

    /// The shared `<root>/repos` authority (fixed top-level repository namespace
    /// root). Pinned exactly once; per-repository `tags` authorities are resolved
    /// freshly beneath it per operation and never cached.
    async fn repos(&self) -> Result<ContainedDir, FsMutateError> {
        self.repos
            .get_or_try_init(|| {
                let root = self.root.clone();
                async move { root.ensure_subdir(&FileName::new("repos")?).await }
            })
            .await
            .map(ContainedDir::clone)
    }

    /// The shared `<root>/quarantine` authority (fixed top-level GC quarantine
    /// namespace: `quarantine/blobs/...` payloads and `quarantine/meta/...`
    /// timestamps). Pinned exactly once; per-digest shard directories are
    /// resolved freshly beneath it per operation and never cached.
    async fn quarantine(&self) -> Result<ContainedDir, FsMutateError> {
        self.quarantine
            .get_or_try_init(|| {
                let root = self.root.clone();
                async move { root.ensure_subdir(&FileName::new("quarantine")?).await }
            })
            .await
            .map(ContainedDir::clone)
    }
}

/// Validate a caller/record-derived single path component, mapping an invalid
/// name to `NotFound` (an escape attempt is treated as an absent target rather
/// than surfaced as an I/O error).
fn session_name(name: impl Into<String>) -> Result<FileName, FsMutateError> {
    FileName::new(name)
}

fn session_data_name(uuid: &str) -> Result<FileName, FsMutateError> {
    session_name(format!("{uuid}.data"))
}
fn session_meta_name(uuid: &str) -> Result<FileName, FsMutateError> {
    session_name(format!("{uuid}.meta.json"))
}
fn session_hash_name(uuid: &str, generation: u64) -> Result<FileName, FsMutateError> {
    session_name(format!("{uuid}.hash.{generation}"))
}
fn session_lock_name(uuid: &str) -> Result<FileName, FsMutateError> {
    session_name(format!(".lock.{uuid}"))
}
fn finalized_receipt_name(uuid: &str) -> Result<FileName, FsMutateError> {
    session_name(format!("{uuid}.json"))
}
/// Legacy streaming-upload hash-state cache leaf (`{uuid}.sha256state`).
fn upload_hash_state_name(uuid: &str) -> Result<FileName, FsMutateError> {
    session_name(format!("{uuid}.sha256state"))
}

/// Convert a caller-supplied tag into a single contained leaf name.
///
/// Applies the same structural validation as the contained tag-read seam
/// (`tag_read::validate_path_component`) and additionally requires the tag to be
/// a single path segment: a tag is the final filesystem leaf and must not span
/// directory components. Invalid tags map to `StorageError::InvalidRepoName`,
/// matching the read seam's taxonomy rather than introducing a second grammar.
fn tag_leaf_name(tag: &str) -> Result<FileName, StorageError> {
    tag_read::validate_path_component(tag, "tag name")?;
    FileName::new(tag).map_err(|_| {
        StorageError::InvalidRepoName("tag name cannot span path components".to_string())
    })
}

/// Map a mutation error surfaced during startup capture to a configuration/IO
/// startup error.
fn map_fs_mutate_startup_err(err: FsMutateError) -> StorageError {
    match err {
        FsMutateError::PlatformUnsupported => StorageError::configuration(
            "contained upload lifecycle requires Linux openat2 (platform unsupported)",
        ),
        other => StorageError::io(format!("capturing upload authorities: {other}")),
    }
}

/// Map a contained mutation error to a `StorageError`, preserving the disk-full
/// signal and unwrapping a cleanup-failure to its primary cause.
fn map_fs_mutate_err(err: FsMutateError) -> StorageError {
    match err {
        FsMutateError::Io(e) => map_fs_io_err(e),
        FsMutateError::SyscallUnsupported(e) => map_fs_io_err(e),
        FsMutateError::CleanupFailed { primary, .. } => map_fs_mutate_err(*primary),
        other => StorageError::io(other.to_string()),
    }
}

#[derive(Debug)]
pub struct FsStorage {
    root: PathBuf,
    max_upload_bytes: u64,
    upload_hashes: Vec<Mutex<std::collections::HashMap<String, SerializableSha256>>>,
    referrer_locks: Vec<Mutex<()>>,
    repo_locks: std::sync::Mutex<std::collections::HashMap<String, std::fs::File>>,
    reader: std::sync::Arc<storage_fs::FsMetadataReader>,
    read_adapter: std::sync::Arc<read_adapter::FsBlobCasReadAdapter<storage_fs::FsMetadataReader>>,
    /// Pinned contained directory authorities shared by every upload-lifecycle
    /// operation (Option A). Captured once at construction; see `UploadAuthorities`.
    upload_authorities: std::sync::Arc<UploadAuthorities>,
    manifest_listing_limits: storage_fs::DirEnumerationLimits,
    gc_discovery_limits: repo_discovery::DiscoveryLimits,
    gc_ref_limits: manifest_refs::ManifestReferenceLimits,
    pub(crate) tag_listing_limits: tag_listing::TagListingLimits,
    /// Test-only synchronization seam invoked inside the reaper's held-lock closure,
    /// at the boundary between a candidate's confirmed expiry decision and its
    /// destructive action, with the candidate uuid. Lets a regression prove that no
    /// cooperating update can slip in between the locked check and the action.
    #[cfg(test)]
    reaper_boundary_hook: ReaperBoundaryHookSlot,
    /// Test-only synchronization seam invoked inside the reaper's held-lock closure
    /// for the receipt-cleanup pass, right after the session lock is acquired and
    /// BEFORE the receipt is re-read, with the candidate uuid. Lets a regression
    /// mutate the on-disk receipt at that point (delete / republish / change identity)
    /// and prove the reaper acts on the current under-lock state, not a listing-time
    /// snapshot.
    #[cfg(test)]
    reaper_receipt_boundary_hook: ReaperBoundaryHookSlot,
}

/// See [`FsStorage::reaper_boundary_hook`]. The callback runs on the reaper's
/// `spawn_blocking` worker while the session lock is held.
#[cfg(test)]
type ReaperBoundaryHook = std::sync::Arc<dyn Fn(&str) + Send + Sync>;

/// A `Debug`-transparent slot holding the optional reaper boundary hook so
/// `FsStorage` can keep `#[derive(Debug)]` while the hook itself is not `Debug`.
#[cfg(test)]
#[derive(Default)]
struct ReaperBoundaryHookSlot(std::sync::Mutex<Option<ReaperBoundaryHook>>);

#[cfg(test)]
impl std::fmt::Debug for ReaperBoundaryHookSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReaperBoundaryHookSlot(..)")
    }
}

impl FsStorage {
    /// Crate-visible constructor enforcing complete limit validation including tag listing limits.
    pub(crate) fn try_new_with_all_limits(
        root: PathBuf,
        max_upload_bytes: u64,
        manifest_listing_limits: storage_fs::DirEnumerationLimits,
        gc_discovery_limits: repo_discovery::DiscoveryLimits,
        gc_ref_limits: manifest_refs::ManifestReferenceLimits,
        tag_listing_limits: tag_listing::TagListingLimits,
    ) -> Result<Self, StorageError> {
        // 1. Validate manifest listing limits
        if manifest_listing_limits.max_entries() < manifest_listing::MIN_MANIFEST_LISTING_ENTRIES {
            return Err(StorageError::configuration(
                "manifest_listing_max_entries must be at least 1",
            ));
        }
        if manifest_listing_limits.max_total_name_bytes()
            < manifest_listing::MIN_MANIFEST_LISTING_NAME_BYTES
        {
            return Err(StorageError::configuration(
                "manifest_listing_max_name_bytes must be at least 128",
            ));
        }

        // 2. Validate GC discovery limits
        if gc_discovery_limits.max_depth < 1 {
            return Err(StorageError::configuration(
                "gc discovery max_depth must be at least 1",
            ));
        }
        if gc_discovery_limits.max_dir_enumerations < 1 {
            return Err(StorageError::configuration(
                "gc discovery max_dir_enumerations must be at least 1",
            ));
        }
        if gc_discovery_limits.max_total_entries < 1 {
            return Err(StorageError::configuration(
                "gc discovery max_total_entries must be at least 1",
            ));
        }
        if gc_discovery_limits.max_manifest_dirs < 1 {
            return Err(StorageError::configuration(
                "gc discovery max_manifest_dirs must be at least 1",
            ));
        }
        if gc_discovery_limits.max_retained_path_bytes < 65_536 {
            return Err(StorageError::configuration(
                "gc discovery max_retained_path_bytes must be at least 65536",
            ));
        }
        if gc_discovery_limits.per_dir_limits.max_entries() < 1 {
            return Err(StorageError::configuration(
                "gc discovery intermediate_dir_max_entries must be at least 1",
            ));
        }
        if gc_discovery_limits.per_dir_limits.max_total_name_bytes() < 128 {
            return Err(StorageError::configuration(
                "gc discovery intermediate_dir_max_name_bytes must be at least 128",
            ));
        }

        // 3. Validate GC reference collection limits
        if gc_ref_limits.max_terminal_dir_enumerations < 1 {
            return Err(StorageError::configuration(
                "gc ref max_terminal_dir_enumerations must be at least 1",
            ));
        }
        if gc_ref_limits.per_dir_limits.max_entries() < 1 {
            return Err(StorageError::configuration(
                "gc ref terminal_dir_max_entries must be at least 1",
            ));
        }
        if gc_ref_limits.per_dir_limits.max_total_name_bytes() < 128 {
            return Err(StorageError::configuration(
                "gc ref terminal_dir_max_name_bytes must be at least 128",
            ));
        }
        if gc_ref_limits.max_total_manifest_entries < 1 {
            return Err(StorageError::configuration(
                "gc ref max_total_manifest_entries must be at least 1",
            ));
        }
        if gc_ref_limits.max_manifests_read < 1 {
            return Err(StorageError::configuration(
                "gc ref max_manifests_read must be at least 1",
            ));
        }
        if gc_ref_limits.max_total_references < 1 {
            return Err(StorageError::configuration(
                "gc ref max_total_references must be at least 1",
            ));
        }
        if gc_ref_limits.max_retained_logical_bytes < 131_072 {
            return Err(StorageError::configuration(
                "gc ref max_retained_logical_bytes must be at least 131072",
            ));
        }
        if let Some(ceiling) = gc_ref_limits.max_manifest_payload_bytes {
            if ceiling < 1024 || ceiling == u64::MAX {
                return Err(StorageError::configuration(
                    "gc ref max_manifest_payload_bytes must be >= 1024 and < u64::MAX",
                ));
            }
        }

        // 4. Validate tag listing limits
        if tag_listing_limits.tags_dir_limits.max_entries() < tag_listing::MIN_TAG_LISTING_ENTRIES {
            return Err(StorageError::configuration(
                "tag_listing_max_entries must be at least 1",
            ));
        }
        if tag_listing_limits.tags_dir_limits.max_total_name_bytes()
            < tag_listing::MIN_TAG_LISTING_NAME_BYTES
        {
            return Err(StorageError::configuration(
                "tag_listing_max_name_bytes must be at least 128",
            ));
        }
        if tag_listing_limits.repo_probe_limits.max_entries()
            < tag_listing::MIN_TAG_LISTING_REPO_PROBE_ENTRIES
        {
            return Err(StorageError::configuration(
                "tag_listing_repo_probe_max_entries must be at least 1",
            ));
        }
        if tag_listing_limits.repo_probe_limits.max_total_name_bytes()
            < tag_listing::MIN_TAG_LISTING_REPO_PROBE_NAME_BYTES
        {
            return Err(StorageError::configuration(
                "tag_listing_repo_probe_max_name_bytes must be at least 64",
            ));
        }
        match tag_listing_limits.payload_limits.max_payload_bytes {
            Some(ceiling) => {
                if ceiling < tag_listing::MIN_TAG_LISTING_PAYLOAD_BYTES || ceiling == u64::MAX {
                    return Err(StorageError::configuration(
                        "tag_listing_max_payload_bytes must be >= 256 and < u64::MAX",
                    ));
                }
            }
            None => {
                return Err(StorageError::configuration(
                    "tag_listing_max_payload_bytes must be >= 256 and < u64::MAX",
                ));
            }
        }

        ensure_dir(&root)?;
        let reader = storage_fs::FsMetadataReader::open(&root)
            .map_err(read_adapter::map_fs_startup_error)?;
        reader
            .probe_capability()
            .map_err(read_adapter::map_fs_startup_error)?;
        let reader = std::sync::Arc::new(reader);
        let read_adapter = std::sync::Arc::new(read_adapter::FsBlobCasReadAdapter::new(
            std::sync::Arc::clone(&reader),
        ));
        let upload_authorities = std::sync::Arc::new(UploadAuthorities::capture(&reader)?);

        let mut upload_hashes = Vec::with_capacity(HASH_SHARDS);
        for _ in 0..HASH_SHARDS {
            upload_hashes.push(Mutex::new(std::collections::HashMap::new()));
        }
        let mut referrer_locks = Vec::with_capacity(REFERRER_SHARDS);
        for _ in 0..REFERRER_SHARDS {
            referrer_locks.push(Mutex::new(()));
        }
        Ok(Self {
            root,
            max_upload_bytes,
            upload_hashes,
            referrer_locks,
            repo_locks: std::sync::Mutex::new(std::collections::HashMap::new()),
            reader,
            read_adapter,
            upload_authorities,
            manifest_listing_limits,
            gc_discovery_limits,
            gc_ref_limits,
            tag_listing_limits,
            #[cfg(test)]
            reaper_boundary_hook: ReaperBoundaryHookSlot::default(),
            #[cfg(test)]
            reaper_receipt_boundary_hook: ReaperBoundaryHookSlot::default(),
        })
    }

    /// Crate-visible constructor enforcing complete limit validation with default tag listing limits.
    pub(crate) fn try_new_with_gc_limits(
        root: PathBuf,
        max_upload_bytes: u64,
        manifest_listing_limits: storage_fs::DirEnumerationLimits,
        gc_discovery_limits: repo_discovery::DiscoveryLimits,
        gc_ref_limits: manifest_refs::ManifestReferenceLimits,
    ) -> Result<Self, StorageError> {
        Self::try_new_with_all_limits(
            root,
            max_upload_bytes,
            manifest_listing_limits,
            gc_discovery_limits,
            gc_ref_limits,
            tag_listing::TagListingLimits::default(),
        )
    }

    pub fn try_new_with_limits(
        root: PathBuf,
        max_upload_bytes: u64,
        limits: storage_fs::DirEnumerationLimits,
    ) -> Result<Self, StorageError> {
        Self::try_new_with_gc_limits(
            root,
            max_upload_bytes,
            limits,
            repo_discovery::DiscoveryLimits::default(),
            manifest_refs::ManifestReferenceLimits::default(),
        )
    }

    pub fn try_new(root: PathBuf, max_upload_bytes: u64) -> Result<Self, StorageError> {
        Self::try_new_with_limits(
            root,
            max_upload_bytes,
            manifest_listing::default_manifest_dir_limits(),
        )
    }

    pub fn new(root: PathBuf, max_upload_bytes: u64) -> Self {
        Self::try_new(root, max_upload_bytes).unwrap_or_else(|err| {
            panic!("failed to initialize FsStorage: {err}");
        })
    }

    /// Returns a reference to the underlying read adapter.
    #[cfg(test)]
    pub(crate) fn read_adapter(
        &self,
    ) -> &std::sync::Arc<read_adapter::FsBlobCasReadAdapter<storage_fs::FsMetadataReader>> {
        &self.read_adapter
    }

    /// Returns a reference to the shared root metadata reader.
    #[cfg(test)]
    pub(crate) fn reader(&self) -> &std::sync::Arc<storage_fs::FsMetadataReader> {
        &self.reader
    }

    /// Returns configured directory enumeration limits for manifest listing.
    #[cfg(test)]
    pub(crate) fn manifest_listing_limits(&self) -> storage_fs::DirEnumerationLimits {
        self.manifest_listing_limits
    }

    /// Returns configured limits for contained tag listing.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn tag_listing_limits(&self) -> &tag_listing::TagListingLimits {
        &self.tag_listing_limits
    }

    /// Internal test helper to list manifest digests with explicitly injected limits.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) async fn list_manifest_digests_page_with_limits(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
        limits: storage_fs::DirEnumerationLimits,
    ) -> Result<(Vec<Digest>, Option<String>), StorageError> {
        manifest_listing::list_manifest_digests_page_impl(
            self.reader.as_ref(),
            repo,
            continuation_token,
            page_limit,
            limits,
        )
        .await
    }

    /// Internal test helper to list CAS blobs with explicitly injected listing budgets.
    #[cfg(test)]
    pub(crate) async fn list_cas_blobs_page_with_budgets(
        &self,
        cursor: Option<&GcCursor>,
        limit: usize,
        budgets: listing::FsListingBudgets,
    ) -> Result<GcBlobPage, StorageError> {
        listing::list_cas_blobs_page_impl(self.reader.as_ref(), cursor, limit, budgets).await
    }

    /// Computes the repository root directory for a validated canonical repository identity.
    pub fn repo_dir(&self, repo: &CanonicalRepoName) -> Result<PathBuf, StorageError> {
        fs_repo_dir(&self.root, repo)
    }

    fn upload_hash_shard(
        &self,
        uuid: &str,
    ) -> &Mutex<std::collections::HashMap<String, SerializableSha256>> {
        let idx = shard_index(uuid, HASH_SHARDS);
        &self.upload_hashes[idx]
    }

    fn referrer_lock_shard(&self, name: &str, subject: &Digest) -> &Mutex<()> {
        let key = format!("{name}:{}", subject.hex());
        let idx = shard_index(&key, REFERRER_SHARDS);
        &self.referrer_locks[idx]
    }

    async fn load_upload_hash_state_from_disk(
        &self,
        uuid: &str,
        expected_len: u64,
    ) -> Option<SerializableSha256> {
        // Contained cache read beneath the pinned `uploads` authority; ANY
        // failure (absence, containment rejection, corrupt bytes) is a cache
        // miss, exactly as the prior ambient read's `.ok()?` behavior.
        let uploads = self.upload_authorities.uploads().await.ok()?;
        let leaf = upload_hash_state_name(uuid).ok()?;
        let bytes = uploads.read_leaf(&leaf, u64::MAX).await.ok()?;
        let st = SerializableSha256::from_bytes(&bytes)?;
        if st.total_len != expected_len {
            return None;
        }
        Some(st)
    }

    async fn persist_upload_hash_state(
        &self,
        uuid: &str,
        st: &SerializableSha256,
    ) -> Result<(), StorageError> {
        // Contained atomic replace beneath the pinned `uploads` authority
        // (non-durable: the prior ambient helper's directory fsync was
        // best-effort/swallowed; every call site treats persistence as
        // best-effort anyway).
        let uploads = self
            .upload_authorities
            .uploads()
            .await
            .map_err(map_fs_mutate_err)?;
        let leaf = upload_hash_state_name(uuid).map_err(map_fs_mutate_err)?;
        uploads
            .write_leaf_atomic(&leaf, st.to_bytes(), false)
            .await
            .map_err(map_fs_mutate_err)
    }

    async fn rebuild_upload_hash_state_from_data_file(
        &self,
        uuid: &str,
        observed_len: u64,
    ) -> Option<SerializableSha256> {
        // Contained open through the pinned `uploads` authority; the
        // stream-hash and the concurrent-growth re-check both run on the
        // OPENED descriptor (the prior ambient re-stat by pathname served the
        // same purpose against the same inode). Any failure -> None (cache
        // rebuild miss), as before.
        let uploads = self.upload_authorities.uploads().await.ok()?;
        let leaf = session_data_name(uuid).ok()?;
        let handle = uploads.open_leaf_read(&leaf).await.ok()?;

        let t = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            use std::io::Read as _;
            let mut file = handle.into_file();
            let mut hasher = SerializableSha256::new();
            let mut buf = vec![0u8; 1024 * 64];
            loop {
                let n = file.read(&mut buf).ok()?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            let file_len_now = file.metadata().ok()?.len();
            Some((hasher, file_len_now))
        })
        .await
        .ok()??;
        let (hasher, file_len_now) = result;

        let elapsed = t.elapsed();
        if file_len_now != observed_len {
            return None;
        }

        if tracing::enabled!(tracing::Level::DEBUG) {
            let mib = observed_len as f64 / (1024.0 * 1024.0);
            let mib_s = mib / elapsed.as_secs_f64().max(0.000_001);
            tracing::debug!(
                target: "registry_rust::storage::fs",
                event = "upload_hash_resume_rebuild",
                uuid,
                size_bytes = observed_len,
                elapsed_ms = elapsed.as_millis() as u64,
                read_hash_mib_s = mib_s,
            );
        }

        Some(hasher)
    }

    async fn ensure_upload_hash_state(
        &self,
        uuid: &str,
        current_len: u64,
    ) -> Option<SerializableSha256> {
        let shard = self.upload_hash_shard(uuid);
        // Fast path: in-memory.
        {
            let map = shard.lock().await;
            if let Some(st) = map.get(uuid) {
                if st.total_len == current_len {
                    return Some(st.clone());
                }
            }
        }

        // Next: on-disk state.
        if let Some(st) = self
            .load_upload_hash_state_from_disk(uuid, current_len)
            .await
        {
            let mut map = shard.lock().await;
            map.insert(uuid.to_string(), st.clone());
            return Some(st);
        }

        // If empty file, create fresh state and persist.
        if current_len == 0 {
            let st = SerializableSha256::new();
            let _ = self.persist_upload_hash_state(uuid, &st).await;
            let mut map = shard.lock().await;
            map.insert(uuid.to_string(), st.clone());
            return Some(st);
        }

        // Fallback: rebuild from partial file and persist.
        let st = self
            .rebuild_upload_hash_state_from_data_file(uuid, current_len)
            .await?;
        let _ = self.persist_upload_hash_state(uuid, &st).await;
        let mut map = shard.lock().await;
        map.insert(uuid.to_string(), st.clone());
        Some(st)
    }

    #[cfg(test)]
    fn blob_path(&self, digest: &Digest) -> PathBuf {
        // data/blobs/<algo>/ab/<hex>
        self.root
            .join("blobs")
            .join(digest.algorithm())
            .join(digest.prefix2())
            .join(digest.hex())
    }

    #[cfg(test)]
    pub(crate) fn quarantine_blob_path(&self, digest: &Digest) -> PathBuf {
        // data/quarantine/blobs/<algo>/ab/<hex>
        self.root
            .join("quarantine")
            .join("blobs")
            .join(digest.algorithm())
            .join(digest.prefix2())
            .join(digest.hex())
    }

    /// Resolve a fresh contained authority for `repos/<repo>/tags`, beneath the
    /// stable pinned `repos` authority. The repository (which may be a nested,
    /// slash-separated name) is re-resolved on every call: no per-repository
    /// authority is cached, so a repository directory that is removed and
    /// recreated at the same pathname is observed as its new inode on the next
    /// operation — keeping tag writes coherent with the (already contained) tag
    /// reads and preventing stale-inode writes into a detached tree.
    ///
    /// Repository names are validated with the same structural contract the
    /// contained tag-read seam applies (`tag_read::validate_path_component`), so
    /// reads and writes share one grammar and one error taxonomy
    /// (`StorageError::InvalidRepoName`). Every directory component is traversed
    /// through contained `ensure_subdir` primitives; the resulting authority (and
    /// the lock/inspection/mutation performed through it) never reconstructs an
    /// ambient path.
    async fn tags_authority(&self, repo: &str) -> Result<ContainedDir, StorageError> {
        tag_read::validate_path_component(repo, "repository name")?;
        let mut dir = self
            .upload_authorities
            .repos()
            .await
            .map_err(map_fs_mutate_err)?;
        for segment in repo.split('/') {
            let name = FileName::new(segment).map_err(map_fs_mutate_err)?;
            dir = dir.ensure_subdir(&name).await.map_err(map_fs_mutate_err)?;
        }
        let tags = FileName::new("tags").map_err(map_fs_mutate_err)?;
        dir.ensure_subdir(&tags).await.map_err(map_fs_mutate_err)
    }

    /// Non-creating variant of [`Self::tags_authority`]: resolves the contained
    /// authority for `repos/<repo>/tags` via `open_subdir` and returns
    /// `Ok(None)` when any component is absent. Used by the `delete_manifest`
    /// tag-cleanup scan, whose frozen contract on an absent tags directory is
    /// an empty scan with ZERO directory creation. Same pinned `repos` root,
    /// same structural validation, same fresh-per-operation resolution — not a
    /// second grammar or authority model.
    async fn open_tags_authority(&self, repo: &str) -> Result<Option<ContainedDir>, StorageError> {
        tag_read::validate_path_component(repo, "repository name")?;
        let mut dir = self
            .upload_authorities
            .repos()
            .await
            .map_err(map_fs_mutate_err)?;
        for segment in repo.split('/').chain(std::iter::once("tags")) {
            let name = FileName::new(segment).map_err(map_fs_mutate_err)?;
            dir = match dir.open_subdir(&name).await {
                Ok(d) => d,
                Err(FsMutateError::NotFound) => return Ok(None),
                Err(err) => return Err(map_fs_mutate_err(err)),
            };
        }
        Ok(Some(dir))
    }

    /// Inner `delete_manifest` tag-cleanup pass: enumerate, inspect, and delete
    /// matching tag leaves ALL through the ONE retained `tags` authority (no
    /// independent path re-resolution between enumeration, read, and unlink).
    /// This seam is also exercised directly by the same-authority replacement
    /// regression.
    ///
    /// Frozen scan contract (previously the ambient `list_tag_files` +
    /// `read_to_string` + ignored `remove_file` loop): dot-entries and
    /// non-UTF-8 entry names are skipped silently; a leaf that vanishes between
    /// enumeration and read is tolerated (`continue`); other read failures
    /// (including non-regular or — new, fail-closed — symlinked entries) and
    /// non-UTF-8 tag content propagate as Io; a tag whose trimmed content
    /// equals the deleted digest is unlinked best-effort (result ignored). The
    /// scan takes NO per-tag locks — the historical `delete_manifest` cleanup
    /// is deliberately lock-free and its read/match/unlink race profile against
    /// concurrent tag mutation is unchanged. Enumeration is unbounded
    /// (`usize::MAX` limits), matching the prior unbounded `read_dir`.
    async fn delete_manifest_tag_cleanup_in(
        tags: &ContainedDir,
        digest_str: &str,
    ) -> Result<(), StorageError> {
        let entries = tags
            .list(storage_fs::DirEnumerationLimits::new(
                usize::MAX,
                usize::MAX,
            ))
            .await
            .map_err(map_fs_mutate_err)?;
        for entry in entries {
            let Some(entry_name) = entry.name().to_str() else {
                continue;
            };
            if entry_name.starts_with('.') {
                continue;
            }
            let leaf = FileName::new(entry_name).map_err(map_fs_mutate_err)?;
            let bytes = match tags.read_leaf(&leaf, u64::MAX).await {
                Ok(b) => b,
                Err(FsMutateError::NotFound) => continue,
                Err(err) => return Err(map_fs_mutate_err(err)),
            };
            let content = match String::from_utf8(bytes) {
                Ok(s) => s,
                Err(err) => return Err(StorageError::io(err.to_string())),
            };
            if content.trim() == digest_str {
                let _ = tags.unlink(&leaf, true).await;
            }
        }
        Ok(())
    }

    /// Resolve a fresh contained authority for `repos/<repo>/manifests`, beneath
    /// the stable pinned `repos` authority — the manifest-namespace analogue of
    /// [`Self::tags_authority`]. The repository is re-resolved on every call (no
    /// per-repository authority is cached), so a repository directory removed and
    /// recreated at the same pathname is observed as its new inode on the next
    /// operation, keeping manifest writes coherent with the already-contained
    /// manifest reads and preventing stale-inode writes into a detached tree.
    ///
    /// Validation reuses the manifest read seam's own grammar
    /// ([`manifest::manifest_key`]) rather than the tag validator: the read path
    /// accepts colon-bearing segments such as `C:/repo`, so validating writes with
    /// the same function keeps manifest reads and writes on one grammar and one
    /// error taxonomy ([`StorageError::InvalidRepoName`]). Every directory
    /// component is traversed through contained `ensure_subdir` primitives; the
    /// resulting authority never reconstructs an ambient path.
    async fn manifests_authority(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<ContainedDir, StorageError> {
        // Reuse the manifest read grammar (rejects `..`/`.`/empty segments,
        // control characters, backslashes, leading/trailing slashes; accepts
        // nested and colon-bearing segments) so a write is accepted iff the
        // matching read is. The composed key is discarded; only its validation
        // side effect is required here.
        manifest::manifest_key(repo, digest)?;
        let mut dir = self
            .upload_authorities
            .repos()
            .await
            .map_err(map_fs_mutate_err)?;
        for segment in repo.split('/') {
            let name = FileName::new(segment).map_err(map_fs_mutate_err)?;
            dir = dir.ensure_subdir(&name).await.map_err(map_fs_mutate_err)?;
        }
        let manifests = FileName::new("manifests").map_err(map_fs_mutate_err)?;
        dir.ensure_subdir(&manifests)
            .await
            .map_err(map_fs_mutate_err)
    }

    /// Resolves the contained authority for `repos/<repo>/referrers`, creating
    /// missing directories beneath the pinned `repos` root.
    ///
    /// Reuses the referrers READ grammar (`referrers_read::referrers_key`:
    /// `tag_read::validate_path_component` + `ObjectKey` composition) so a
    /// mutation is accepted iff the matching contained read accepts the same
    /// repository name. The composed key is discarded; only its validation side
    /// effect is required here. Resolution is fresh per operation — no
    /// per-repository authority caching — so repository deletion/recreation is
    /// observed and read/write namespace resolution stays coherent.
    async fn referrers_authority(
        &self,
        repo: &str,
        subject: &Digest,
    ) -> Result<ContainedDir, StorageError> {
        referrers_read::referrers_key(repo, subject)?;
        let mut dir = self
            .upload_authorities
            .repos()
            .await
            .map_err(map_fs_mutate_err)?;
        for segment in repo.split('/') {
            let name = FileName::new(segment).map_err(map_fs_mutate_err)?;
            dir = dir.ensure_subdir(&name).await.map_err(map_fs_mutate_err)?;
        }
        let referrers = FileName::new("referrers").map_err(map_fs_mutate_err)?;
        dir.ensure_subdir(&referrers)
            .await
            .map_err(map_fs_mutate_err)
    }

    /// Reads and parses the referrers index leaf through an ALREADY-RESOLVED
    /// contained authority, so a mutation's inspection and action share one
    /// resolution (no independent path re-resolution between them).
    ///
    /// Preserves the mutation-read contract of the contained reader seam:
    /// missing leaf → empty vector; corrupt JSON / invalid UTF-8 → legacy Io
    /// taxonomy via the shared `referrers_read::parse_referrers_bytes`;
    /// physical descriptor order preserved; no normalization or deduplication.
    /// Symlinked or non-regular leaves fail closed at the contained primitive.
    async fn read_referrers_from_authority(
        referrers: &ContainedDir,
        leaf: &FileName,
    ) -> Result<Vec<ReferrerDescriptor>, StorageError> {
        match referrers.read_leaf(leaf, u64::MAX).await {
            Ok(bytes) => referrers_read::parse_referrers_bytes(&bytes),
            Err(FsMutateError::NotFound) => Ok(Vec::new()),
            Err(err) => Err(map_fs_mutate_err(err)),
        }
    }

    /// Inner `add_referrer` sequence, run under the caller-held shard lock:
    /// inspect, modify, and write all through the ONE retained `referrers`
    /// authority. This seam is also exercised directly by the same-authority
    /// replacement regressions.
    async fn add_referrer_locked(
        &self,
        referrers: &ContainedDir,
        leaf: &FileName,
        descriptor: ReferrerDescriptor,
    ) -> Result<(), StorageError> {
        let mut existing = Self::read_referrers_from_authority(referrers, leaf).await?;
        if !existing.iter().any(|d| d.digest == descriptor.digest) {
            existing.push(descriptor);
        }

        // A duplicate add still rewrites the (unchanged) array, as before.
        let bytes = serde_json::to_vec(&existing)
            .map_err(|err| StorageError::serialization(err.to_string()))?;
        referrers
            .write_leaf_atomic(leaf, bytes, true)
            .await
            .map_err(map_fs_mutate_err)?;
        Ok(())
    }

    /// Inner `remove_referrer` sequence, run under the caller-held shard lock:
    /// inspect, decide (no-change / rewrite / empty unlink), and act all
    /// through the ONE retained `referrers` authority.
    async fn remove_referrer_locked(
        &self,
        referrers: &ContainedDir,
        leaf: &FileName,
        referrer: &Digest,
    ) -> Result<(), StorageError> {
        let mut existing = Self::read_referrers_from_authority(referrers, leaf).await?;
        let orig_len = existing.len();
        let referrer_str = referrer.as_str();
        existing.retain(|d| d.digest != referrer_str);
        if existing.len() == orig_len {
            return Ok(());
        }

        if existing.is_empty() {
            // Best-effort removal of the now-empty index (result ignored, as
            // before) plus best-effort directory-entry durability.
            let _ = referrers.unlink(leaf, true).await;
            let _ = referrers.sync().await;
        } else {
            let bytes = serde_json::to_vec(&existing)
                .map_err(|err| StorageError::serialization(err.to_string()))?;
            referrers
                .write_leaf_atomic(leaf, bytes, true)
                .await
                .map_err(map_fs_mutate_err)?;
        }
        Ok(())
    }

    /// Resolves the contained authority for the membership record directory
    /// `repo-memberships/by-repo/<key>/<algo>` beneath the pinned memberships
    /// root WITHOUT creating missing components: returns `Ok(None)` when any
    /// component is absent, preserving the exact absent-record contract of the
    /// candidate/unlink mutations (`Ok(false)`) with zero directory-creation
    /// side effects.
    ///
    /// Repository grammar is the upstream-validated [`CanonicalRepoName`]
    /// (parsed by the callers); the encoded key is a single base64url
    /// component. This is the same layout `link_repo_blob` writes and the
    /// contained membership reads resolve. Resolution is fresh per operation.
    async fn membership_record_authority(
        &self,
        canonical: &CanonicalRepoName,
        digest: &Digest,
    ) -> Result<Option<ContainedDir>, StorageError> {
        let mut dir = self
            .upload_authorities
            .memberships()
            .await
            .map_err(map_fs_mutate_err)?;
        let key = crate::storage::repo_membership::encode_canonical_repo_key(canonical);
        for segment in ["by-repo", key.as_str(), digest.algorithm()] {
            let name = FileName::new(segment).map_err(map_fs_mutate_err)?;
            dir = match dir.open_subdir(&name).await {
                Ok(d) => d,
                Err(FsMutateError::NotFound) => return Ok(None),
                Err(err) => return Err(map_fs_mutate_err(err)),
            };
        }
        Ok(Some(dir))
    }

    /// Reads and parses the membership record leaf through an ALREADY-RESOLVED
    /// contained authority. Missing leaf → `Ok(None)`; corrupt JSON keeps the
    /// frozen `corrupt membership record: ...` `CorruptData` taxonomy.
    async fn read_membership_record_from_authority(
        dir: &ContainedDir,
        leaf: &FileName,
    ) -> Result<Option<crate::storage::repo_membership::RepoBlobMembershipRecord>, StorageError>
    {
        let bytes = match dir.read_leaf(leaf, u64::MAX).await {
            Ok(b) => b,
            Err(FsMutateError::NotFound) => return Ok(None),
            Err(err) => return Err(map_fs_mutate_err(err)),
        };
        let record = serde_json::from_slice::<
            crate::storage::repo_membership::RepoBlobMembershipRecord,
        >(&bytes)
        .map_err(|e| StorageError::corrupt_data(format!("corrupt membership record: {e}")))?;
        Ok(Some(record))
    }

    /// Inner `set_membership_candidate` transition: inspect, decide, and
    /// rewrite all through the ONE retained membership authority (there is no
    /// lock on candidate transitions — unchanged). This seam is also exercised
    /// directly by the same-authority replacement regression.
    async fn set_membership_candidate_in(
        dir: &ContainedDir,
        leaf: &FileName,
        since_unix_secs: u64,
    ) -> Result<bool, StorageError> {
        let Some(mut record) = Self::read_membership_record_from_authority(dir, leaf).await? else {
            return Ok(false);
        };
        if record.state == crate::storage::repo_membership::MembershipState::Candidate {
            return Ok(false);
        }
        record.state = crate::storage::repo_membership::MembershipState::Candidate;
        record.unreferenced_since_unix_secs = Some(since_unix_secs);
        let updated_bytes = serde_json::to_vec(&record)
            .map_err(|e| StorageError::serialization(format!("serialize membership: {e}")))?;
        dir.write_leaf_atomic(leaf, updated_bytes, true)
            .await
            .map_err(map_fs_mutate_err)?;
        Ok(true)
    }

    /// Inner `clear_membership_candidate` transition: same retained-authority
    /// inspect/decide/rewrite shape as the set transition.
    async fn clear_membership_candidate_in(
        dir: &ContainedDir,
        leaf: &FileName,
    ) -> Result<bool, StorageError> {
        let Some(mut record) = Self::read_membership_record_from_authority(dir, leaf).await? else {
            return Ok(false);
        };
        if record.state == crate::storage::repo_membership::MembershipState::Active
            && record.unreferenced_since_unix_secs.is_none()
        {
            return Ok(false);
        }
        record.state = crate::storage::repo_membership::MembershipState::Active;
        record.unreferenced_since_unix_secs = None;
        let updated_bytes = serde_json::to_vec(&record)
            .map_err(|e| StorageError::serialization(format!("serialize membership: {e}")))?;
        dir.write_leaf_atomic(leaf, updated_bytes, true)
            .await
            .map_err(map_fs_mutate_err)?;
        Ok(true)
    }

    /// Leaf name for a membership record: `<hex>.json`.
    fn membership_leaf_name(digest: &Digest) -> Result<FileName, StorageError> {
        FileName::new(format!("{}.json", digest.hex())).map_err(map_fs_mutate_err)
    }

    #[cfg(test)]
    fn uploads_dir(&self) -> PathBuf {
        self.root.join("uploads")
    }

    #[cfg(test)]
    fn session_data_path(&self, uuid: &str) -> PathBuf {
        self.uploads_dir().join(format!("{uuid}.data"))
    }

    #[cfg(test)]
    fn session_meta_path(&self, uuid: &str) -> PathBuf {
        self.uploads_dir().join(format!("{uuid}.meta.json"))
    }

    #[cfg(test)]
    fn session_hash_path(&self, uuid: &str, generation: u64) -> PathBuf {
        self.uploads_dir().join(format!("{uuid}.hash.{generation}"))
    }

    #[cfg(test)]
    fn session_lock_path(&self, uuid: &str) -> PathBuf {
        self.uploads_dir().join(format!(".lock.{uuid}"))
    }

    #[cfg(test)]
    fn finalized_dir(&self) -> PathBuf {
        self.uploads_dir().join(".finalized")
    }

    #[cfg(test)]
    fn finalized_receipt_path(&self, uuid: &str) -> PathBuf {
        self.finalized_dir().join(format!("{uuid}.json"))
    }

    /// Install the test-only reaper boundary hook (see `reaper_boundary_hook`).
    #[cfg(test)]
    fn set_reaper_boundary_hook(&self, hook: ReaperBoundaryHook) {
        *self.reaper_boundary_hook.0.lock().unwrap() = Some(hook);
    }

    /// Install the test-only reaper receipt-cleanup boundary hook
    /// (see `reaper_receipt_boundary_hook`).
    #[cfg(test)]
    fn set_reaper_receipt_boundary_hook(&self, hook: ReaperBoundaryHook) {
        *self.reaper_receipt_boundary_hook.0.lock().unwrap() = Some(hook);
    }

    async fn detect_manifest_media_type(&self, bytes: &[u8]) -> Result<String, StorageError> {
        manifest::detect_manifest_media_type(bytes)
    }

    async fn list_repo_names(&self) -> Result<Vec<String>, StorageError> {
        catalog_discovery::discover_catalog_repositories_impl(
            self.reader.as_ref(),
            &catalog_discovery::CatalogDiscoveryLimits::default(),
        )
        .await
    }
}

fn map_fs_io_err(err: std::io::Error) -> StorageError {
    // Prefer a clear signal for the common operational failure: disk full.
    if err.raw_os_error() == Some(libc::ENOSPC) || err.kind() == std::io::ErrorKind::StorageFull {
        return StorageError::InsufficientStorage;
    }
    StorageError::io(err.to_string())
}

fn map_blocking_join_error(err: tokio::task::JoinError) -> StorageError {
    StorageError::internal_invariant(err.to_string())
}

async fn fsync_dir(path: &Path) -> Result<(), StorageError> {
    // Best-effort durability: fsync the directory so rename/link updates survive power loss.
    // This is a blocking operation; run it off the async runtime.
    let dir = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let f = std::fs::File::open(&dir)?;
        f.sync_all()?;
        Ok::<(), std::io::Error>(())
    })
    .await
    .map_err(map_blocking_join_error)?
    .map_err(map_fs_io_err)
}

async fn atomic_write_file(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let Some(parent) = path.parent() else {
        return Err(StorageError::internal_invariant("invalid path"));
    };
    ensure_dir(&parent.to_path_buf())?;

    let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("file");
    let tmp_name = format!(".tmp.{file_name}.{}", uuid::Uuid::new_v4());
    let tmp_path = parent.join(tmp_name);

    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)
        .await
        .map_err(map_fs_io_err)?;

    file.write_all(bytes).await.map_err(map_fs_io_err)?;
    file.flush().await.map_err(map_fs_io_err)?;
    // Ensure file data+metadata is on stable storage before we make it visible.
    file.sync_all().await.map_err(map_fs_io_err)?;
    drop(file);

    if let Err(err) = tokio::fs::rename(&tmp_path, path).await {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        return Err(map_fs_io_err(err));
    }

    fsync_dir(parent).await?;
    Ok(())
}

/// Low-level filesystem metadata inquiry returning object byte size.
///
/// Retains the original [`std::io::Error`] on failure so its [`std::io::ErrorKind`]
/// remains available for typed inspection before outward conversion.
/// Preserved as a historical helper for low-level baseline characterization tests.
#[cfg(test)]
pub(crate) async fn fs_metadata_size(path: &Path) -> Result<u64, std::io::Error> {
    tokio::fs::metadata(path).await.map(|m| m.len())
}

#[async_trait]
impl Storage for FsStorage {
    fn kind(&self) -> &'static str {
        "fs"
    }

    async fn list_repositories(&self) -> Result<Vec<String>, StorageError> {
        self.list_repo_names().await
    }

    async fn repo_timestamps(&self, name: &str) -> Result<RepoTimestamps, StorageError> {
        timestamps_emptiness::repo_timestamps_impl(self.reader.as_ref(), name).await
    }

    async fn is_storage_empty(&self) -> Result<bool, StorageError> {
        let repos = self.list_repositories().await?;
        if !repos.is_empty() {
            return Ok(false);
        }
        let subdirs = [
            "blobs",
            "uploads",
            "quarantine",
            "repo-blobs",
            "repo-memberships",
            "repos",
            "journals",
        ];
        for sub in &subdirs {
            if timestamps_emptiness::contained_subtree_has_any_entry(self.reader.as_ref(), sub)
                .await?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError> {
        use crate::storage::ports::BlobCasReader;
        self.read_adapter.head_blob(digest).await
    }

    async fn open_blob(
        &self,
        digest: &Digest,
    ) -> Result<(BlobMeta, std::pin::Pin<Box<dyn AsyncRead + Send>>), StorageError> {
        use crate::storage::ports::BlobCasReader;
        self.read_adapter.open_blob(digest).await
    }

    async fn resolve_tag(&self, name: &str, tag: &str) -> Result<Digest, StorageError> {
        tag_read::resolve_tag(
            self.reader.as_ref(),
            name,
            tag,
            &tag_read::TagReadLimits::default(),
        )
        .await
    }

    async fn list_tags(&self, name: &str) -> Result<Vec<String>, StorageError> {
        tag_listing::contained_list_tags_seam(
            self.reader.as_ref(),
            name,
            self.tag_listing_limits.repo_probe_limits,
            self.tag_listing_limits.tags_dir_limits,
        )
        .await
    }

    async fn head_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<ManifestMeta, StorageError> {
        manifest::head_manifest_impl(self.reader.as_ref(), name, digest).await
    }

    async fn get_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<(ManifestMeta, bytes::Bytes), StorageError> {
        manifest::get_manifest_impl(self.reader.as_ref(), name, digest).await
    }

    async fn put_manifest(
        &self,
        name: &str,
        digest: &Digest,
        bytes: Bytes,
    ) -> Result<ManifestMeta, StorageError> {
        // Contained authority for `repos/<repo>/manifests` (fresh per op, no
        // per-repo cache), resolved beneath the pinned `repos` root — matching the
        // already-contained manifest reads and replacing the prior ambient
        // `self.root.join(...)` reconstruction. Resolving the authority also ensures
        // the `manifests` directory, mirroring the previous `ensure_dir`, so the
        // create/detect/write ordering is preserved.
        let manifests = self.manifests_authority(name, digest).await?;

        let media_type = self.detect_manifest_media_type(&bytes).await?;
        let size = bytes.len() as u64;
        let leaf = FileName::new(digest.hex()).map_err(map_fs_mutate_err)?;
        manifests
            .write_leaf_atomic(&leaf, bytes.to_vec(), true)
            .await
            .map_err(map_fs_mutate_err)?;

        Ok(ManifestMeta { size, media_type })
    }

    async fn set_tag(&self, name: &str, tag: &str, digest: &Digest) -> Result<(), StorageError> {
        self.mutate_tag(name, tag, digest, super::TagMutationPolicy::Replace)
            .await?;
        Ok(())
    }

    async fn mutate_tag(
        &self,
        name: &str,
        tag: &str,
        digest: &Digest,
        policy: super::TagMutationPolicy,
    ) -> Result<super::TagMutation, StorageError> {
        // Fresh contained authority for `repos/<repo>/tags` (no per-repo cache).
        let tags = self.tags_authority(name).await?;
        let leaf = tag_leaf_name(tag)?;
        let lock_name = FileName::new(format!(".lock.{tag}")).map_err(map_fs_mutate_err)?;
        let body = format!("{}\n", digest.as_str()).into_bytes();
        let digest_clone = digest.clone();

        // Acquire the retained `.lock.<tag>` exclusive lock, then perform the
        // inspection and mutation under that lock, on the same pinned authority.
        let guard = tags.lock(&lock_name).await.map_err(map_fs_mutate_err)?;
        tags.run_locked(guard, move |dir, _g| {
            // Read the existing leaf: absent → None; readable-but-unparseable
            // (corrupt) → None; any other error (incl. a non-regular/symlink
            // leaf rejected by containment) propagates.
            let existing_d = match dir.read_leaf(&leaf, u64::MAX) {
                Ok(existing_bytes) => {
                    let s = String::from_utf8_lossy(&existing_bytes);
                    Digest::parse(s.trim()).ok()
                }
                Err(FsMutateError::NotFound) => None,
                Err(e) => return Err(e),
            };

            // Same-digest short-circuit runs BEFORE mutation-policy rejection.
            if let Some(ref prev) = existing_d {
                if *prev == digest_clone {
                    return Ok(Ok(super::TagMutation::Unchanged));
                }
            }

            let outcome: Result<super::TagMutation, StorageError> = match policy {
                super::TagMutationPolicy::CreateOnly => {
                    if existing_d.is_some() {
                        return Ok(Err(StorageError::TagAlreadyExists));
                    }
                    dir.write_leaf_atomic(&leaf, &body, true)?;
                    Ok(super::TagMutation::Created)
                }
                super::TagMutationPolicy::Replace => {
                    dir.write_leaf_atomic(&leaf, &body, true)?;
                    match existing_d {
                        Some(prev) => Ok(super::TagMutation::Replaced { previous: prev }),
                        None => Ok(super::TagMutation::Created),
                    }
                }
            };
            Ok(outcome)
        })
        .await
        .map_err(map_fs_mutate_err)?
    }

    async fn delete_tag(&self, name: &str, tag: &str) -> Result<(), StorageError> {
        // Fresh contained authority for `repos/<repo>/tags` (no per-repo cache).
        // The reviewed design intentionally does not introduce locking here: the
        // prior ambient `delete_tag` performed an unserialized `remove_file`, so
        // we preserve that (no `.lock.<tag>`) and only contain the unlink+sync.
        let tags = self.tags_authority(name).await?;
        let leaf = tag_leaf_name(tag)?;
        let blocking = tags.blocking();
        tokio::task::spawn_blocking(move || match blocking.unlink(&leaf, false) {
            Ok(()) => {
                let _ = blocking.sync();
                Ok(())
            }
            Err(FsMutateError::NotFound) => Err(StorageError::NotFound),
            Err(e) => Err(map_fs_mutate_err(e)),
        })
        .await
        .map_err(map_blocking_join_error)?
    }

    async fn list_manifest_digests_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<Digest>, Option<String>), StorageError> {
        manifest_listing::list_manifest_digests_page_impl(
            self.reader.as_ref(),
            repo,
            continuation_token,
            page_limit,
            self.manifest_listing_limits,
        )
        .await
    }

    async fn list_tags_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<(String, Digest)>, Option<String>), StorageError> {
        tag_listing::contained_list_tags_page_seam(
            self.reader.as_ref(),
            self.reader.as_ref(),
            repo,
            continuation_token,
            page_limit,
            self.tag_listing_limits.repo_probe_limits,
            self.tag_listing_limits.tags_dir_limits,
            self.tag_listing_limits.payload_limits.clone(),
        )
        .await
    }

    async fn list_referrers_page(
        &self,
        repo: &str,
        subject: &Digest,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<ReferrerDescriptor>, Option<String>), StorageError> {
        let mut refs = self.list_referrers(repo, subject).await.unwrap_or_default();
        refs.sort_by(|a, b| a.digest.cmp(&b.digest));

        let start_idx = if let Some(token) = continuation_token {
            match refs.binary_search_by(|r| r.digest.as_str().cmp(token)) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            }
        } else {
            0
        };

        let end_idx = (start_idx + page_limit).min(refs.len());
        let page_slice = &refs[start_idx..end_idx];

        let next_token = if end_idx < refs.len() {
            page_slice.last().map(|r| r.digest.clone())
        } else {
            None
        };

        Ok((page_slice.to_vec(), next_token))
    }

    async fn get_tag_with_version(
        &self,
        repo: &str,
        tag: &str,
    ) -> Result<Option<(Digest, String)>, StorageError> {
        tag_read::get_tag_with_version(
            self.reader.as_ref(),
            repo,
            tag,
            &tag_read::TagReadLimits::default(),
        )
        .await
    }

    async fn delete_tag_conditional(
        &self,
        repo: &str,
        tag: &str,
        expected_version: Option<&str>,
    ) -> Result<super::ConditionalDeleteResult, StorageError> {
        // Fresh contained authority for `repos/<repo>/tags` (no per-repo cache).
        let tags = self.tags_authority(repo).await?;
        let leaf = tag_leaf_name(tag)?;
        let lock_name = FileName::new(format!(".lock.{tag}")).map_err(map_fs_mutate_err)?;
        let exp_v = expected_version.map(|s| s.to_string());

        // Acquire the retained `.lock.<tag>` and perform the whole
        // inspect/precondition/delete sequence under that lock on the same
        // pinned authority.
        let guard = tags.lock(&lock_name).await.map_err(map_fs_mutate_err)?;
        tags.run_locked(guard, move |dir, _g| {
            // Read the exact leaf bytes; absent → NotFound.
            let bytes = match dir.read_leaf(&leaf, u64::MAX) {
                Ok(b) => b,
                Err(FsMutateError::NotFound) => {
                    return Ok(super::ConditionalDeleteResult::NotFound);
                }
                Err(e) => return Err(e),
            };

            // Precondition: SHA-256 over the exact raw bytes read (never a
            // reparsed/reformatted digest), matching `get_tag_with_version`.
            if let Some(ref exp) = exp_v {
                let mut hasher = sha2::Sha256::new();
                hasher.update(&bytes);
                let current_version = hex::encode(hasher.finalize());
                if current_version != *exp {
                    return Ok(super::ConditionalDeleteResult::PreconditionFailed {
                        current_version: Some(current_version),
                    });
                }
            }

            dir.unlink(&leaf, false)?;
            let _ = dir.sync();
            Ok(super::ConditionalDeleteResult::Deleted)
        })
        .await
        .map_err(map_fs_mutate_err)
    }

    async fn read_lifecycle_journal(&self, repo: &str) -> Result<Option<Bytes>, StorageError> {
        journal_read::read_lifecycle_journal_impl(self.reader.as_ref(), repo).await
    }

    async fn write_lifecycle_journal(&self, repo: &str, data: Bytes) -> Result<(), StorageError> {
        let canonical = crate::registry::canonical_name::CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let meta_dir = fs_repo_dir(&self.root, &canonical)?.join("meta");
        ensure_dir(&meta_dir)?;
        let path = meta_dir.join("lifecycle_journal.json");
        let tmp_path = meta_dir.join(format!(".tmp.journal.{}", uuid::Uuid::new_v4()));
        tokio::fs::write(&tmp_path, &data)
            .await
            .map_err(|e| StorageError::io(e.to_string()))?;
        tokio::fs::rename(&tmp_path, &path)
            .await
            .map_err(|e| StorageError::io(e.to_string()))?;
        let _ = fsync_dir(&meta_dir).await;
        Ok(())
    }

    async fn delete_lifecycle_journal(&self, repo: &str) -> Result<(), StorageError> {
        let canonical = crate::registry::canonical_name::CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let meta_dir = fs_repo_dir(&self.root, &canonical)?.join("meta");
        let path = meta_dir.join("lifecycle_journal.json");
        match tokio::fs::remove_file(&path).await {
            Ok(_) => {
                let _ = fsync_dir(&meta_dir).await;
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::io(e.to_string())),
        }
    }

    async fn acquire_repo_lease(
        &self,
        repo: &str,
        owner_id: &str,
        lease_id: &str,
        _ttl_secs: u64,
    ) -> Result<bool, StorageError> {
        let canonical = crate::registry::canonical_name::CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let repo_dir = fs_repo_dir(&self.root, &canonical)?;
        ensure_dir(&repo_dir)?;
        let lock_path = repo_dir.join(".repo_lock");
        let key = format!("{}:{owner_id}:{lease_id}", canonical.as_str());

        let file = tokio::task::spawn_blocking(move || {
            use fs2::FileExt;
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path)
                .map_err(map_fs_io_err)?;

            file.lock_exclusive().map_err(map_fs_io_err)?;
            Ok::<_, StorageError>(file)
        })
        .await
        .map_err(map_blocking_join_error)??;

        self.repo_locks.lock().unwrap().insert(key, file);
        Ok(true)
    }

    async fn renew_repo_lease(
        &self,
        _repo: &str,
        _owner_id: &str,
        _lease_id: &str,
        _ttl_secs: u64,
    ) -> Result<bool, StorageError> {
        Ok(true)
    }

    async fn release_repo_lease(
        &self,
        repo: &str,
        owner_id: &str,
        lease_id: &str,
    ) -> Result<(), StorageError> {
        let key = format!("{repo}:{owner_id}:{lease_id}");
        let _ = self.repo_locks.lock().unwrap().remove(&key);
        Ok(())
    }

    async fn create_upload(&self) -> Result<super::UploadMeta, StorageError> {
        // Contained: the shared pinned `uploads` authority (ensured once, as
        // the prior ambient ensure_dir did) and a create-or-truncate open of
        // the data leaf through it (`O_CREAT|O_TRUNC`, mode 0o644 — the exact
        // `File::create` semantics, including truncation of an existing leaf).
        let uploads = self
            .upload_authorities
            .uploads()
            .await
            .map_err(map_fs_mutate_err)?;
        let uuid = uuid::Uuid::new_v4().to_string();
        let leaf = session_data_name(&uuid).map_err(map_fs_mutate_err)?;
        uploads
            .open_leaf_write(&leaf, LeafWriteMode::CreateOrTruncate)
            .await
            .map_err(map_fs_mutate_err)?;

        // Track + persist hash state from the beginning so resumes after restart are cheap.
        let st = SerializableSha256::new();
        let _ = self.persist_upload_hash_state(&uuid, &st).await;
        let shard = self.upload_hash_shard(&uuid);
        let mut map = shard.lock().await;
        map.insert(uuid.clone(), st);

        Ok(super::UploadMeta { uuid, offset: 0 })
    }

    async fn upload_status(&self, uuid: &str) -> Result<super::UploadMeta, StorageError> {
        let uploads = self
            .upload_authorities
            .uploads()
            .await
            .map_err(map_fs_mutate_err)?;
        // A caller-supplied identifier that cannot form a contained leaf is an
        // absent upload (never an ambient path).
        let Ok(leaf) = session_data_name(uuid) else {
            return Err(StorageError::NotFound);
        };
        match uploads.inspect(&leaf).await {
            Ok(Some(identity)) => Ok(super::UploadMeta {
                uuid: uuid.to_string(),
                offset: identity.size,
            }),
            Ok(None) => Err(StorageError::NotFound),
            Err(err) => Err(map_fs_mutate_err(err)),
        }
    }

    async fn append_upload(
        &self,
        uuid: &str,
        chunk: Bytes,
    ) -> Result<super::UploadMeta, StorageError> {
        let t_total = Instant::now();
        let uploads = self
            .upload_authorities
            .uploads()
            .await
            .map_err(map_fs_mutate_err)?;
        let Ok(leaf) = session_data_name(uuid) else {
            return Err(StorageError::NotFound);
        };

        // ONE contained `O_APPEND` open (kernel append semantics preserved);
        // the length inspection, size-limit check, write, and final offset all
        // operate on this securely opened file description (previously the
        // pre-write length came from a separate pathname stat). The blocking
        // closure OWNS the handle, the chunk, and the limit, so caller
        // cancellation cannot strand borrowed state.
        let handle = match uploads.open_leaf_write(&leaf, LeafWriteMode::Append).await {
            Ok(h) => h,
            Err(FsMutateError::NotFound) => return Err(StorageError::NotFound),
            Err(err) => return Err(map_fs_mutate_err(err)),
        };
        let max_upload_bytes = self.max_upload_bytes;
        let chunk_for_write = chunk.clone();
        let (current_len, new_len) =
            tokio::task::spawn_blocking(move || -> Result<(u64, u64), StorageError> {
                use std::io::Write as _;
                let mut file = handle.into_file();
                let current_len = file.metadata().map_err(map_fs_io_err)?.len();
                let next_len = current_len.saturating_add(chunk_for_write.len() as u64);
                if next_len > max_upload_bytes {
                    return Err(StorageError::TooLarge);
                }
                file.write_all(&chunk_for_write).map_err(map_fs_io_err)?;
                file.flush().map_err(map_fs_io_err)?;
                let new_len = file.metadata().map_err(map_fs_io_err)?.len();
                Ok((current_len, new_len))
            })
            .await
            .map_err(map_blocking_join_error)??;

        // Update hash state (persisted). If we can't keep it consistent, drop state and fall back.
        let shard = self.upload_hash_shard(uuid);
        if let Some(mut st) = self.ensure_upload_hash_state(uuid, current_len).await {
            if st.total_len == current_len {
                st.update(&chunk);
                let _ = self.persist_upload_hash_state(uuid, &st).await;
                let mut map = shard.lock().await;
                map.insert(uuid.to_string(), st);
            } else {
                let mut map = shard.lock().await;
                map.remove(uuid);
            }
        }

        // Debug-level timing to help diagnose slow pushes without spamming normal logs.
        // We only log when enabled AND the operation is "interesting" (big chunk or slow write).
        let elapsed = t_total.elapsed();
        if tracing::enabled!(tracing::Level::DEBUG)
            && (elapsed > Duration::from_millis(200) || chunk.len() >= 16 * 1024 * 1024)
        {
            let mib = chunk.len() as f64 / (1024.0 * 1024.0);
            let secs = elapsed.as_secs_f64().max(0.000_001);
            let mib_s = mib / secs;
            tracing::debug!(
                target: "registry_rust::storage::fs",
                event = "upload_append",
                uuid,
                chunk_bytes = chunk.len(),
                elapsed_ms = elapsed.as_millis() as u64,
                write_mib_s = mib_s,
            );
        }

        Ok(super::UploadMeta {
            uuid: uuid.to_string(),
            offset: new_len,
        })
    }

    async fn finalize_upload(&self, uuid: &str, digest: &Digest) -> Result<BlobMeta, StorageError> {
        // Contained finalize: ONE secure open of the upload leaf beneath the
        // pinned `uploads` authority; the size inspection, digest hashing (when
        // a reread is needed), and the pre-publish `sync_all` all operate on
        // that opened file description. The CAS publish is a contained
        // cross-authority rename (uploads -> blobs shard) followed by
        // propagated directory syncs of both pinned directories, exactly as
        // the prior ambient fsync_dir pair. Blocking segments own their file
        // handle (caller cancellation cannot strand borrowed state).
        let uploads = self
            .upload_authorities
            .uploads()
            .await
            .map_err(map_fs_mutate_err)?;
        let Ok(upload_leaf) = session_data_name(uuid) else {
            return Err(StorageError::NotFound);
        };

        let t_total = Instant::now();
        let handle = match uploads.open_leaf_read(&upload_leaf).await {
            Ok(h) => h,
            Err(FsMutateError::NotFound) => return Err(StorageError::NotFound),
            Err(err) => return Err(map_fs_mutate_err(err)),
        };
        let (file, upload_size_bytes) =
            tokio::task::spawn_blocking(move || -> Result<(std::fs::File, u64), StorageError> {
                let file = handle.into_file();
                let len = file
                    .metadata()
                    .map_err(|err| StorageError::io(err.to_string()))?
                    .len();
                Ok((file, len))
            })
            .await
            .map_err(map_blocking_join_error)??;

        let mut hash_source = "file_reread";
        let t_hash = Instant::now();

        // Prefer persisted state (and in-memory cache) to avoid a second full reread at finalize.
        let shard = self.upload_hash_shard(uuid);
        let state_from_mem = {
            let mut map = shard.lock().await;
            map.remove(uuid)
        };

        let state = match state_from_mem {
            Some(st) if st.total_len == upload_size_bytes => Some(st),
            _ => self.ensure_upload_hash_state(uuid, upload_size_bytes).await,
        };

        let use_sha512 = digest.algorithm() == "sha512";
        let (computed_hex, file) = if !use_sha512
            && let Some(st) = state.as_ref()
            && st.total_len == upload_size_bytes
        {
            hash_source = "saved_state";
            (st.finalize_hex(), file)
        } else {
            // Stream-hash from the already-opened descriptor (owned by the
            // blocking closure, returned for the subsequent sync).
            tokio::task::spawn_blocking(move || -> Result<(String, std::fs::File), StorageError> {
                use std::io::Read as _;
                let mut file = file;
                let mut buf = vec![0u8; 1024 * 64];
                let hex_out = if use_sha512 {
                    let mut hasher = sha2::Sha512::new();
                    loop {
                        let n = file
                            .read(&mut buf)
                            .map_err(|err| StorageError::io(err.to_string()))?;
                        if n == 0 {
                            break;
                        }
                        hasher.update(&buf[..n]);
                    }
                    hex::encode(hasher.finalize())
                } else {
                    let mut hasher = sha2::Sha256::new();
                    loop {
                        let n = file
                            .read(&mut buf)
                            .map_err(|err| StorageError::io(err.to_string()))?;
                        if n == 0 {
                            break;
                        }
                        hasher.update(&buf[..n]);
                    }
                    hex::encode(hasher.finalize())
                };
                Ok((hex_out, file))
            })
            .await
            .map_err(map_blocking_join_error)??
        };
        let hash_elapsed = t_hash.elapsed();

        if computed_hex != digest.hex() {
            return Err(StorageError::DigestMismatch);
        }

        // Ensure the uploaded data is durable before we make it visible in the blob store.
        let t_sync = Instant::now();
        tokio::task::spawn_blocking(move || file.sync_all().map_err(map_fs_io_err))
            .await
            .map_err(map_blocking_join_error)??;
        let sync_elapsed = t_sync.elapsed();

        // Move into blob store: contained cross-authority rename into the
        // ensured CAS shard.
        let dest = self
            .cas_blobs_shard(digest, true)
            .await?
            .expect("ensure-mode shard resolution always yields an authority");
        let blob_leaf = Self::blob_leaf(digest)?;
        let t_rename = Instant::now();
        uploads
            .rename_leaf(&upload_leaf, &dest, &blob_leaf)
            .await
            .map_err(map_fs_mutate_err)?;
        let rename_elapsed = t_rename.elapsed();

        // Make the rename durable (both directories are updated by rename).
        let t_fsync = Instant::now();
        uploads.sync().await.map_err(map_fs_mutate_err)?;
        dest.sync().await.map_err(map_fs_mutate_err)?;
        let fsync_elapsed = t_fsync.elapsed();

        // Info-level summary for operators: where did the time go?
        let total_elapsed = t_total.elapsed();
        let size_mib = upload_size_bytes as f64 / (1024.0 * 1024.0);
        let hash_ms_u64 = hash_elapsed.as_millis() as u64;
        let total_ms_u64 = total_elapsed.as_millis() as u64;
        let hash_mib_s = (hash_ms_u64 >= 1).then(|| size_mib / (hash_ms_u64 as f64 / 1000.0));
        let total_mib_s = (total_ms_u64 >= 1).then(|| size_mib / (total_ms_u64 as f64 / 1000.0));
        tracing::info!(
            target: "registry_rust::storage::fs",
            event = "upload_finalize",
            uuid,
            digest = %digest.as_str(),
            size_bytes = upload_size_bytes,
            hash_source,
            hash_ms = hash_ms_u64,
            sync_ms = sync_elapsed.as_millis() as u64,
            rename_ms = rename_elapsed.as_millis() as u64,
            fsync_ms = fsync_elapsed.as_millis() as u64,
            total_ms = total_ms_u64,
            hash_mib_s,
            total_mib_s,
        );

        // Best-effort cleanup: remove persisted hash state.
        if let Ok(state_leaf) = upload_hash_state_name(uuid) {
            let _ = uploads.unlink(&state_leaf, true).await;
        }

        let size = match dest.inspect(&blob_leaf).await {
            Ok(Some(identity)) => identity.size,
            Ok(None) => {
                return Err(StorageError::io(format!(
                    "finalized blob missing after rename: {}",
                    digest.as_str()
                )));
            }
            Err(err) => return Err(map_fs_mutate_err(err)),
        };
        Ok(BlobMeta { size })
    }

    async fn abort_upload(&self, uuid: &str) -> Result<(), StorageError> {
        // Best-effort cleanup: drop in-memory state.
        let shard = self.upload_hash_shard(uuid);
        {
            let mut map = shard.lock().await;
            map.remove(uuid);
        }

        let uploads = self
            .upload_authorities
            .uploads()
            .await
            .map_err(map_fs_mutate_err)?;

        // Best-effort cleanup: remove persisted hash state (result ignored, as
        // before).
        if let Ok(state_leaf) = upload_hash_state_name(uuid) {
            let _ = uploads.unlink(&state_leaf, true).await;
        }

        // An identifier that cannot form a contained leaf is an absent upload:
        // absent aborts are Ok.
        let Ok(leaf) = session_data_name(uuid) else {
            return Ok(());
        };
        match uploads.unlink(&leaf, false).await {
            Ok(()) => Ok(()),
            Err(FsMutateError::NotFound) => Ok(()),
            Err(err) => Err(map_fs_mutate_err(err)),
        }
    }

    async fn list_referrers(
        &self,
        name: &str,
        subject: &Digest,
    ) -> Result<Vec<ReferrerDescriptor>, StorageError> {
        referrers_read::read_referrers_contained(
            self.reader.as_ref(),
            name,
            subject,
            &referrers_read::ReferrersReadLimits::default(),
        )
        .await
    }

    async fn add_referrer(
        &self,
        name: &str,
        subject: &Digest,
        descriptor: ReferrerDescriptor,
    ) -> Result<(), StorageError> {
        // Retained in-process shard lock (same identity/scope as before) held
        // across the full read/modify/write sequence. ONE contained
        // `repos/<repo>/referrers` authority is resolved (validate-then-ensure
        // beneath the pinned `repos` root) and retained across inspection AND
        // write, so a repository/referrers namespace replacement mid-operation
        // cannot split the tree that is inspected from the tree that is
        // mutated. Ordinary reads still resolve independently through the
        // contained reader seam; no snapshot isolation or cross-process
        // serialization is claimed.
        let _lock = self.referrer_lock_shard(name, subject).lock().await;
        let referrers = self.referrers_authority(name, subject).await?;
        let leaf = FileName::new(format!("{}.json", subject.hex())).map_err(map_fs_mutate_err)?;
        self.add_referrer_locked(&referrers, &leaf, descriptor)
            .await
    }

    async fn remove_referrer(
        &self,
        name: &str,
        subject: &Digest,
        referrer: &Digest,
    ) -> Result<(), StorageError> {
        // Same retained shard lock and single retained contained authority as
        // `add_referrer`; read, decision, rewrite, and empty-index unlink all
        // act on that one resolution.
        let _lock = self.referrer_lock_shard(name, subject).lock().await;
        let referrers = self.referrers_authority(name, subject).await?;
        let leaf = FileName::new(format!("{}.json", subject.hex())).map_err(map_fs_mutate_err)?;
        self.remove_referrer_locked(&referrers, &leaf, referrer)
            .await
    }

    async fn delete_manifest(&self, name: &str, digest: &Digest) -> Result<(), StorageError> {
        // Contained authority for `repos/<repo>/manifests`; replaces the prior
        // ambient read+remove of the reconstructed manifest path. Both the subject
        // pre-read and the unlink resolve through this single pinned authority (no
        // ambient reconstruction; a symlinked or root-replaced manifest leaf fails
        // closed). The tag cleanup below runs on one retained contained tags
        // authority and the referrer cleanup is contained via the contained
        // `remove_referrer` seam — every namespace this operation touches is
        // contained. Ordering (manifest unlink -> tag scan -> referrer cleanup),
        // error propagation/suppression, and the possibility of partial cleanup
        // after the manifest unlink are unchanged; this is not a transaction.
        let manifests = self.manifests_authority(name, digest).await?;
        let leaf = FileName::new(digest.hex()).map_err(map_fs_mutate_err)?;

        // Pre-read manifest bytes to extract subject if present for referrers cleanup.
        let bytes = match manifests.read_leaf(&leaf, u64::MAX).await {
            Ok(b) => b,
            Err(FsMutateError::NotFound) => return Err(StorageError::NotFound),
            Err(err) => return Err(map_fs_mutate_err(err)),
        };

        let maybe_subject = crate::manifest_refs::extract_subject_digest(&bytes).map_err(|e| {
            StorageError::corrupt_data(format!(
                "cannot delete manifest with malformed structure: {e}"
            ))
        })?;

        match manifests.unlink(&leaf, false).await {
            Ok(()) => {}
            Err(FsMutateError::NotFound) => return Err(StorageError::NotFound),
            Err(err) => return Err(map_fs_mutate_err(err)),
        }
        // Best-effort durability of the directory-entry removal (result ignored, as
        // with the contained `delete_tag`); no success/error-path change.
        let _ = manifests.sync().await;

        // Remove any tags pointing to this digest — enumeration, inspection, and
        // deletion all through ONE retained contained tags authority, resolved
        // non-creating (an absent tags directory preserves the empty-scan
        // contract with zero directory creation).
        let digest_str = digest.as_str();
        if let Some(tags) = self.open_tags_authority(name).await? {
            Self::delete_manifest_tag_cleanup_in(&tags, &digest_str).await?;
        }

        // Clean up from referrers list if this manifest referenced a subject.
        if let Some(subject) = maybe_subject {
            let _ = self.remove_referrer(name, &subject, digest).await;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FsSessionMetaRecord {
    pub format_version: u32,
    pub repo: crate::registry::canonical_name::CanonicalRepoName,
    pub uuid: String,
    pub state: UploadSessionState,
    pub committed_offset: u64,
    pub hash_generation: u64,
    pub created_at_unix_secs: u64,
    pub last_active_at_unix_secs: u64,
    pub finalizing_info: Option<FsFinalizingInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FsFinalizingInfo {
    pub operation_id: String,
    pub expected_digest: String,
    pub size: u64,
    pub finalizing_at_unix_secs: u64,
}

/// Legacy ambient (fs2-based) session lock guard. Retained only for the regression
/// test that verifies the contained reaper honors an externally held advisory lock on
/// the same `.lock.{uuid}` inode (an fs2 exclusive flock conflicts with the authority's
/// contained flock across OFDs on the same inode).
#[cfg(test)]
struct FsSessionLockGuard {
    file: Option<std::fs::File>,
    _path: PathBuf,
}

#[cfg(test)]
impl Drop for FsSessionLockGuard {
    fn drop(&mut self) {
        if let Some(f) = self.file.take() {
            let _ = fs2::FileExt::unlock(&f);
            drop(f);
        }
    }
}

#[cfg(test)]
async fn acquire_fs_session_lock(lock_path: PathBuf) -> Result<FsSessionLockGuard, StorageError> {
    if let Some(parent) = lock_path.parent() {
        ensure_dir(parent)?;
    }
    tokio::task::spawn_blocking(move || {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| {
                StorageError::io(format!("failed to open lock file {lock_path:?}: {e}"))
            })?;
        fs2::FileExt::lock_exclusive(&f)
            .map_err(|e| StorageError::io(format!("failed to acquire lock {lock_path:?}: {e}")))?;
        Ok(FsSessionLockGuard {
            file: Some(f),
            _path: lock_path,
        })
    })
    .await
    .map_err(map_blocking_join_error)?
}

// ============================================================================
// Contained upload-lifecycle record helpers
// ============================================================================
//
// These operate on the pinned `UploadAuthorities` directory descriptors. The sync
// helpers (`*_sync`) run inside `run_locked` bodies on a `BlockingDir`; the async
// helper (`rebuild_hash_async`) runs in the streaming append/finalize paths on a
// `ContainedDir`. All resolution is descriptor-relative and beneath the pinned root.

const S_IFMT: u32 = 0o170000;
const S_IFREG: u32 = 0o100000;

fn is_regular(mode: u32) -> bool {
    (mode & S_IFMT) == S_IFREG
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Wrap a `Display` error as a generic contained I/O failure. Used for
/// serialize/logic failures surfaced from inside a `run_locked` body, whose error
/// type is fixed to `FsMutateError`.
fn body_io_err<E: std::fmt::Display>(e: E) -> FsMutateError {
    FsMutateError::Io(std::io::Error::other(e.to_string()))
}

fn json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, FsMutateError> {
    serde_json::to_vec(value).map_err(body_io_err)
}

/// Outcome of a synchronous session-meta read beneath the uploads authority.
enum SessionMetaOutcome {
    Present(FsSessionMetaRecord),
    /// Malformed record; carries the deserialization error message so callers can
    /// surface the same corrupt-data detail the ambient reads did.
    Corrupt(String),
    Absent,
}

fn read_session_meta_sync(
    uploads: &BlockingDir,
    uuid: &str,
) -> Result<SessionMetaOutcome, FsMutateError> {
    let name = session_meta_name(uuid)?;
    match uploads.read_leaf(&name, SESSION_META_READ_LIMIT) {
        Ok(bytes) => match serde_json::from_slice::<FsSessionMetaRecord>(&bytes) {
            Ok(meta) => Ok(SessionMetaOutcome::Present(meta)),
            Err(e) => Ok(SessionMetaOutcome::Corrupt(e.to_string())),
        },
        Err(FsMutateError::NotFound) => Ok(SessionMetaOutcome::Absent),
        Err(FsMutateError::InvalidName { .. }) => Ok(SessionMetaOutcome::Absent),
        Err(e) => Err(e),
    }
}

fn write_session_meta_sync(
    uploads: &BlockingDir,
    meta: &FsSessionMetaRecord,
) -> Result<(), FsMutateError> {
    let name = session_meta_name(&meta.uuid)?;
    let bytes = json_bytes(meta)?;
    uploads.write_leaf_atomic(&name, &bytes, true)
}

fn read_receipt_sync(
    finalized: &BlockingDir,
    uuid: &str,
) -> Result<Option<FinalizedReceipt>, FsMutateError> {
    let name = finalized_receipt_name(uuid)?;
    match finalized.read_leaf(&name, FINALIZED_RECEIPT_READ_LIMIT) {
        // A corrupt receipt is treated as absent, matching the lenient ambient reads.
        Ok(bytes) => Ok(serde_json::from_slice::<FinalizedReceipt>(&bytes).ok()),
        Err(FsMutateError::NotFound) => Ok(None),
        Err(FsMutateError::InvalidName { .. }) => Ok(None),
        Err(e) => Err(e),
    }
}

fn write_receipt_sync(
    finalized: &BlockingDir,
    receipt: &FinalizedReceipt,
) -> Result<(), FsMutateError> {
    let name = finalized_receipt_name(&receipt.uuid)?;
    let bytes = json_bytes(receipt)?;
    finalized.write_leaf_atomic(&name, &bytes, true)
}

/// Durably write a repository blob membership record beneath the memberships
/// authority, creating the `by-repo/{key}/{algo}` subtree as needed. Mirrors
/// `canonical_repo_membership_relpath`.
fn write_membership_sync(
    memberships: &BlockingDir,
    record: &crate::storage::repo_membership::RepoBlobMembershipRecord,
) -> Result<(), FsMutateError> {
    let key = crate::storage::repo_membership::encode_canonical_repo_key(&record.repo);
    let by_repo = memberships.ensure_subdir(&FileName::new("by-repo")?)?;
    let repo_dir = by_repo.ensure_subdir(&FileName::new(key)?)?;
    let algo_dir = repo_dir.ensure_subdir(&FileName::new(record.digest.algorithm())?)?;
    let leaf = FileName::new(format!("{}.json", record.digest.hex()))?;
    let bytes = json_bytes(record)?;
    algo_dir.write_leaf_atomic(&leaf, &bytes, true)
}

/// Inspect the CAS leaf for `digest` beneath the blobs authority
/// (`{algo}/{prefix2}/{hex}`), returning its identity if present.
fn cas_blob_present_sync(
    blobs: &BlockingDir,
    digest: &Digest,
) -> Result<Option<storage_fs::FsFileIdentity>, FsMutateError> {
    let algo = match blobs.open_subdir(&FileName::new(digest.algorithm())?) {
        Ok(d) => d,
        Err(FsMutateError::NotFound) => return Ok(None),
        Err(e) => return Err(e),
    };
    let shard = match algo.open_subdir(&FileName::new(digest.prefix2())?) {
        Ok(d) => d,
        Err(FsMutateError::NotFound) => return Ok(None),
        Err(e) => return Err(e),
    };
    shard.inspect(&FileName::new(digest.hex())?)
}

/// Rebuild a rolling SHA-256 state by reading up to `upto` bytes of `data_name`
/// beneath the uploads authority through a contained read descriptor.
fn rebuild_hash_sync(
    uploads: &BlockingDir,
    data_name: &FileName,
    upto: u64,
) -> Result<SerializableSha256, FsMutateError> {
    use std::io::Read as _;
    let mut file = uploads.open_leaf_read(data_name)?.into_file();
    let mut st = SerializableSha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    let mut read_total = 0u64;
    while read_total < upto {
        let to_read = ((upto - read_total).min(buf.len() as u64)) as usize;
        let n = file.read(&mut buf[..to_read]).map_err(FsMutateError::Io)?;
        if n == 0 {
            break;
        }
        st.update(&buf[..n]);
        read_total += n as u64;
    }
    Ok(st)
}

/// Per-candidate result of one reaper iteration, distinguishing confirmed cleanups
/// from the non-failure reasons a candidate is left alone. Genuine I/O failures are
/// surfaced as `Err` out of `run_locked` and never collapse into these variants.
enum ReapOutcome {
    /// A confirmed cleanup: an abort completed, a Finalizing session rolled forward,
    /// or a past-TTL receipt was unlinked. Counted.
    CleanedUp,
    /// Not yet past its TTL (session `last_active` / receipt `finalized_at`).
    NotExpired,
    /// An expired `Finalizing` session whose CAS blob is not yet published: recovery
    /// left it intact for later completion. Diagnosed, **not** counted, and never
    /// aborted — containment does not authorize destroying in-flight finalizations.
    PendingFinalization,
    /// The record vanished between enumeration and the locked re-read.
    Absent,
    /// The session meta was present but unparseable (diagnostic recorded).
    Corrupt(String),
    /// A receipt whose stored identity no longer matches its leaf name.
    Changed,
}

/// Outcome of a locked-inner recovery (`recover_session_locked`) run beneath an
/// already-held session lock.
enum RecoverLocked {
    /// A `Finalizing` session whose CAS blob is fully published was rolled *forward*:
    /// membership and receipt are now durably (re)written. This is a completed
    /// finalization, not merely an inspection.
    RolledForward {
        committed_offset: u64,
        created: u64,
        last_active: u64,
    },
    /// The meta record was present and coherent but there was nothing to finalize
    /// (an `Appending` tail was truncated to the committed offset, or a `Finalizing`
    /// session whose CAS blob is *not* yet published). The session still exists.
    Pending {
        state: UploadSessionState,
        committed_offset: u64,
        created: u64,
        last_active: u64,
    },
    /// Neither a meta record nor a finalized receipt exists: nothing to recover.
    NotFound,
    /// The meta record was present but could not be parsed.
    Corrupt(String),
}

/// Perform `Finalizing` roll-forward or `Appending` torn-tail recovery for one
/// session **beneath an already-held session lock**. Every directory operation
/// resolves through the caller-provided pinned authorities (`dir` = uploads,
/// plus the sibling `finalized` / `blobs` / `memberships` views); the caller
/// retains the matching [`ContainedLockGuard`] for the whole call. This function
/// never acquires or releases a lock, so inspection, decision, and mutation are one
/// continuously-locked operation.
///
/// Unlike a best-effort sweep, a membership or receipt write failure during
/// roll-forward is **propagated**, not swallowed: the caller must not treat a failed
/// roll-forward as a completed cleanup.
fn recover_session_locked(
    dir: &BlockingDir,
    finalized: &BlockingDir,
    blobs: &BlockingDir,
    memberships: &BlockingDir,
    repo: &CanonicalRepoName,
    uuid: &str,
) -> Result<RecoverLocked, FsMutateError> {
    let meta = match read_session_meta_sync(dir, uuid)? {
        SessionMetaOutcome::Present(m) => m,
        SessionMetaOutcome::Corrupt(msg) => return Ok(RecoverLocked::Corrupt(msg)),
        SessionMetaOutcome::Absent => {
            // A receipt without a meta record is an already-completed finalization.
            if let Some(receipt) = read_receipt_sync(finalized, uuid)? {
                return Ok(RecoverLocked::RolledForward {
                    committed_offset: receipt.size,
                    created: receipt.finalized_at_unix_secs,
                    last_active: receipt.finalized_at_unix_secs,
                });
            }
            return Ok(RecoverLocked::NotFound);
        }
    };

    if meta.state == UploadSessionState::Finalizing {
        if let Some(ref fin_info) = meta.finalizing_info
            && let Ok(digest) = Digest::parse(&fin_info.expected_digest)
            && let Some(id) = cas_blob_present_sync(blobs, &digest)?
            && id.size == fin_info.size
        {
            // Roll forward only when the CAS blob is fully published. Membership is
            // written BEFORE the receipt and both failures are propagated.
            let membership = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
                repo.clone(),
                digest.clone(),
                Some(uuid.to_string()),
            );
            write_membership_sync(memberships, &membership)?;
            let receipt = FinalizedReceipt {
                repo: repo.clone(),
                uuid: uuid.to_string(),
                digest: fin_info.expected_digest.clone(),
                size: fin_info.size,
                finalized_at_unix_secs: fin_info.finalizing_at_unix_secs,
                format_version: 1,
            };
            write_receipt_sync(finalized, &receipt)?;
            return Ok(RecoverLocked::RolledForward {
                committed_offset: fin_info.size,
                created: meta.created_at_unix_secs,
                last_active: fin_info.finalizing_at_unix_secs,
            });
        }
    } else {
        // Torn-tail recovery: truncate any bytes past the committed offset.
        let data_name = session_data_name(uuid)?;
        if let Some(id) = dir.inspect(&data_name)?
            && id.size > meta.committed_offset
        {
            dir.truncate(&data_name, meta.committed_offset)?;
        }
    }

    Ok(RecoverLocked::Pending {
        state: meta.state,
        committed_offset: meta.committed_offset,
        created: meta.created_at_unix_secs,
        last_active: meta.last_active_at_unix_secs,
    })
}

/// Abort one session **beneath an already-held session lock**: remove the staging
/// data leaf and every hash generation, then remove the meta record LAST so a crash
/// mid-abort leaves a still-recoverable session rather than orphaned staging data.
/// The stable `.lock.{uuid}` file is intentionally never unlinked. Resolves through
/// the caller-provided pinned uploads `dir`; never acquires or releases a lock.
///
/// Failure boundary (matches the abort guarantee):
/// * A genuinely missing data/hash/meta leaf is idempotent absence (`missing_ok`),
///   not a failure.
/// * Any other data or hash cleanup failure is **propagated before the meta is
///   removed**, so the meta record survives, the session stays recoverable, and the
///   caller never counts an incomplete abort as a completed cleanup.
/// * The final meta removal is propagated so the caller can confirm the effect.
///
/// Hash-generation discovery enumerates what is actually on disk rather than a window
/// derived from `meta.hash_generation`. That window is **not** a sound bound: the
/// generation-advancing paths (`append_if_offset`, the `begin_finalize` trailing
/// stream) write generation `G+1`, persist the meta at `G+1`, then clean up the old
/// `G` leaf with a *suppressed*, best-effort unlink (`let _ = dir.unlink(...)`). A
/// real, non-`ENOENT` unlink failure there leaves `G` behind while the recorded
/// generation advances past it, and subsequent appends can advance the recorded
/// generation arbitrarily far without ever revisiting that residual. Repeated
/// failures can leave arbitrarily older generations, so no `{G-1, G, G+1}` (or any
/// other fixed) window is complete. Completeness therefore requires enumerating this
/// session's own `{uuid}.hash.{n}` leaves directly (`session_hash_generations_by_scan`)
/// under the held `.lock.{uuid}` — no concurrent append can add a generation
/// mid-abort — and the `{uuid}.hash.` prefix keeps another session's files out of
/// scope. The same enumeration is used regardless of the meta state, so residual hash
/// leaves are cleaned even when the meta is corrupt or already absent.
fn abort_session_locked(dir: &BlockingDir, uuid: &str) -> Result<(), FsMutateError> {
    let data_name = session_data_name(uuid)?;
    let meta_name = session_meta_name(uuid)?;

    // Enumerate EVERY residual hash leaf for this session directly from disk. A listing
    // failure (an I/O error, or a directory too large to enumerate under the limits) is
    // a required cleanup failure: it is propagated here, before anything is removed, so
    // the meta survives and a later retry can complete the abort.
    let hash_generations = session_hash_generations_by_scan(dir, uuid)?;

    // Remove the staging data leaf. A genuine miss is idempotent; any other failure is
    // propagated here, before the meta is touched, so the session remains recoverable.
    dir.unlink(&data_name, true)?;

    // Remove every residual hash generation (ascending, for deterministic behaviour).
    // A genuine miss is idempotent; any other failure is propagated before the meta is
    // removed. Because the meta survives, a later retry re-scans and completes cleanup.
    for generation in hash_generations {
        let hash_name = session_hash_name(uuid, generation)?;
        dir.unlink(&hash_name, true)?;
    }

    // Remove the meta record LAST so a crash mid-abort leaves a still-recoverable
    // session rather than orphaned staging data.
    dir.unlink(&meta_name, true)?;
    Ok(())
}

/// Enumerate the hash generations that currently exist for `uuid` by scanning the
/// pinned uploads directory for this session's own `{uuid}.hash.{n}` leaves, returned
/// in ascending order. This is the authoritative residual set for abort cleanup: it
/// reflects whatever leaves are actually present, including generations orphaned by a
/// failed best-effort old-hash unlink in `append_if_offset` / `begin_finalize`. The
/// `{uuid}.hash.` prefix and the strict `u64` suffix parse keep the scan
/// session-specific, so no unrelated file (another session's leaf, a rename temp such
/// as `{uuid}.hash.{n}.<rand>`, or the `{uuid}.data` / `{uuid}.meta.json` leaves) is
/// ever considered. A directory too large to enumerate under the limits surfaces as an
/// error rather than a silently truncated (and therefore incomplete) listing.
fn session_hash_generations_by_scan(
    dir: &BlockingDir,
    uuid: &str,
) -> Result<Vec<u64>, FsMutateError> {
    let prefix = format!("{uuid}.hash.");
    let entries = dir.list(storage_fs::DirEnumerationLimits::new(
        1_048_576,
        256 * 1024 * 1024,
    ))?;
    let mut generations = Vec::new();
    for entry in entries {
        let name = entry.name().to_string_lossy();
        if let Some(rest) = name.strip_prefix(&prefix)
            && let Ok(generation) = rest.parse::<u64>()
        {
            generations.push(generation);
        }
    }
    generations.sort_unstable();
    Ok(generations)
}

// ============================================================================
// Supervised streaming owner
// ============================================================================
//
// The streaming append/finalize operations cannot move an async byte stream into
// a synchronous `run_locked` body directly, but they must still guarantee that
// every outstanding file mutation either completes while the session lock is held
// or is definitively stopped before that lock is released. They achieve this by
// splitting into two halves connected by a bounded channel:
//
// * An async **feeder** (part of the request future) pulls chunks from the byte
//   stream and forwards them to the worker, sending an explicit terminal message
//   (`Finish` on clean end, `StreamError` on producer error). Backpressure is
//   provided by the bounded channel.
// * A synchronous **worker** runs inside `ContainedDir::run_locked`, so it owns
//   the session lock on a `spawn_blocking` thread. It drains chunks with
//   `blocking_recv`, performs all writes synchronously on its own thread (no
//   detached Tokio blocking op can outlive it), and only then releases the lock
//   (the guard is dropped at the end of the `run_locked` body).
//
// Cancellation safety: if the request future is dropped, the feeder is dropped
// with it, closing the channel *without* a terminal message. `run_locked` awaits
// a `spawn_blocking` join handle, and dropping that await does **not** abort the
// blocking task — so the worker keeps running, observes the closed channel
// (`blocking_recv` -> `None`), rolls the data file back to the last committed
// offset, and releases the lock. No mutation from the cancelled operation can act
// after the lock is released, and the worker never waits forever on a cancelled
// producer (channel closure wakes `blocking_recv` immediately). A competing
// append/finalize/abort/reaper operation blocks on the same session lock until
// the worker's rollback (or commit) has completed.

/// A message from the async feeder to the synchronous append worker.
enum StreamMsg {
    /// A non-terminal payload chunk to append.
    Chunk(Bytes),
    /// The producer finished the byte stream cleanly; the worker should commit.
    Finish,
    /// The producer observed a stream error; the worker should roll back.
    StreamError,
}

/// The result of draining the chunk channel into an open append descriptor.
enum DrainOutcome {
    /// The producer finished cleanly; `written` bytes were appended and synced.
    Finished { written: u64 },
    /// The producer reported a stream error; the file was rolled back.
    StreamAborted,
    /// The caller was cancelled (channel closed without a terminal message); the
    /// file was rolled back to the committed offset.
    Cancelled,
    /// The upload exceeded the byte limit; the file was rolled back.
    TooLarge,
    /// A write/sync I/O error occurred; the file was rolled back.
    Io(std::io::Error),
}

/// Bounded chunk-channel capacity. Small enough to bound buffered memory, large
/// enough to keep the blocking worker fed without lock-step stalls.
const STREAM_CHANNEL_CAP: usize = 16;

/// Drain the chunk channel into `file` (opened for contained append), rolling the
/// SHA-256 state forward and enforcing `limit`. On any non-`Finished` outcome the
/// file is truncated back to `committed_offset` before returning, so the lock the
/// caller still holds is released over a file that is at the committed length.
/// Runs entirely on the calling (blocking) thread.
fn drain_append_blocking(
    file: &mut std::fs::File,
    rx: &mut tokio::sync::mpsc::Receiver<StreamMsg>,
    hash_st: &mut SerializableSha256,
    committed_offset: u64,
    limit: u64,
) -> DrainOutcome {
    use std::io::Write as _;
    let mut written = 0u64;
    loop {
        match rx.blocking_recv() {
            Some(StreamMsg::Chunk(chunk)) => {
                if chunk.is_empty() {
                    continue;
                }
                let next_total = committed_offset
                    .saturating_add(written)
                    .saturating_add(chunk.len() as u64);
                if limit > 0 && next_total > limit {
                    let _ = file.set_len(committed_offset);
                    let _ = file.sync_data();
                    return DrainOutcome::TooLarge;
                }
                if let Err(e) = file.write_all(&chunk) {
                    let _ = file.set_len(committed_offset);
                    let _ = file.sync_data();
                    return DrainOutcome::Io(e);
                }
                hash_st.update(&chunk);
                written = written.saturating_add(chunk.len() as u64);
            }
            Some(StreamMsg::Finish) => {
                if let Err(e) = file.sync_data() {
                    let _ = file.set_len(committed_offset);
                    return DrainOutcome::Io(e);
                }
                return DrainOutcome::Finished { written };
            }
            Some(StreamMsg::StreamError) => {
                let _ = file.set_len(committed_offset);
                let _ = file.sync_data();
                return DrainOutcome::StreamAborted;
            }
            None => {
                // Channel closed without a terminal message: the request future was
                // dropped (caller cancellation). Roll back and release the lock.
                let _ = file.set_len(committed_offset);
                let _ = file.sync_data();
                return DrainOutcome::Cancelled;
            }
        }
    }
}

/// Pump `stream` into `tx`, translating each item into a [`StreamMsg`] and sending
/// an explicit terminal message. Returns any producer stream error by value (it is
/// not `Clone`, so it cannot be carried through the worker). If the worker has gone
/// away (receiver dropped), the pump stops early. If this future is itself dropped
/// (caller cancellation), `tx` drops with it and the worker observes a closed
/// channel without a terminal message.
async fn feed_stream(
    mut stream: UploadByteStream,
    tx: tokio::sync::mpsc::Sender<StreamMsg>,
) -> Option<UploadStreamError> {
    while let Some(item) = stream.next().await {
        match item {
            Ok(chunk) => {
                if tx.send(StreamMsg::Chunk(chunk)).await.is_err() {
                    // Worker returned early and dropped the receiver; stop pumping.
                    return None;
                }
            }
            Err(e) => {
                let _ = tx.send(StreamMsg::StreamError).await;
                return Some(e);
            }
        }
    }
    let _ = tx.send(StreamMsg::Finish).await;
    None
}

async fn write_atomic_file(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("file");
    let tmp_path = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!(".tmp.{file_name}.{}", uuid::Uuid::new_v4()));

    tokio::fs::write(&tmp_path, bytes)
        .await
        .map_err(map_fs_io_err)?;
    if let Ok(f) = tokio::fs::File::open(&tmp_path).await {
        let _ = f.sync_all().await;
    }
    tokio::fs::rename(&tmp_path, path)
        .await
        .map_err(map_fs_io_err)?;
    if let Some(parent) = path.parent() {
        let _ = fsync_dir(parent).await;
    }
    Ok(())
}

#[async_trait]
impl UploadSessionStorage for FsStorage {
    async fn create_session(&self, repo: &str) -> Result<UploadSessionId, StorageError> {
        let canonical_repo = crate::registry::canonical_name::CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let uuid = uuid::Uuid::new_v4().to_string();
        let session = UploadSessionId::new(canonical_repo.clone(), &uuid);

        let uploads = self
            .upload_authorities
            .uploads()
            .await
            .map_err(map_fs_mutate_err)?;
        let lock_name = session_lock_name(&uuid).map_err(map_fs_mutate_err)?;
        let guard = uploads.lock(&lock_name).await.map_err(map_fs_mutate_err)?;

        let now = now_unix_secs();
        let meta = FsSessionMetaRecord {
            format_version: 1,
            repo: canonical_repo,
            uuid: uuid.clone(),
            state: UploadSessionState::Active,
            committed_offset: 0,
            hash_generation: 0,
            created_at_unix_secs: now,
            last_active_at_unix_secs: now,
            finalizing_info: None,
        };
        let meta_bytes = json_bytes(&meta).map_err(map_fs_mutate_err)?;
        let hash_bytes = SerializableSha256::new().to_bytes();
        let data_name = session_data_name(&uuid).map_err(map_fs_mutate_err)?;
        let hash_name = session_hash_name(&uuid, 0).map_err(map_fs_mutate_err)?;
        let meta_name = session_meta_name(&uuid).map_err(map_fs_mutate_err)?;

        uploads
            .run_locked(guard, move |dir, _g| {
                // Create the empty data leaf (exclusive; tolerate an existing empty leaf
                // from a crash-retry of the same uuid).
                match dir.open_leaf_write(&data_name, LeafWriteMode::CreateNew) {
                    Ok(_) | Err(FsMutateError::AlreadyExists) => {}
                    Err(e) => return Err(e),
                }
                dir.write_leaf_atomic(&hash_name, &hash_bytes, true)?;
                dir.write_leaf_atomic(&meta_name, &meta_bytes, true)?;
                Ok(())
            })
            .await
            .map_err(map_fs_mutate_err)?;

        Ok(session)
    }

    async fn session_status(
        &self,
        session: &UploadSessionId,
    ) -> Result<UploadSessionStatus, UploadTransitionError> {
        enum StatusOutcome {
            Status {
                state: UploadSessionState,
                committed_offset: u64,
                created: u64,
                last_active: u64,
            },
            NotFound,
            Corrupt(String),
        }

        let uploads = self
            .upload_authorities
            .uploads()
            .await
            .map_err(|e| UploadTransitionError::Storage(map_fs_mutate_err(e)))?;
        // Pre-initialize the shared `.finalized` authority in the async context and
        // move its blocking view into the owned-boundary body: the body operates on
        // the same cached descriptor rather than reopening a replacement subtree.
        let finalized = self
            .upload_authorities
            .finalized()
            .await
            .map_err(|e| UploadTransitionError::Storage(map_fs_mutate_err(e)))?
            .blocking();
        let repo = session.repo.clone();
        let uuid = session.uuid.clone();

        let lock_name = session_lock_name(&uuid)
            .map_err(|e| UploadTransitionError::Storage(map_fs_mutate_err(e)))?;
        let guard = uploads
            .lock(&lock_name)
            .await
            .map_err(|e| UploadTransitionError::Storage(map_fs_mutate_err(e)))?;

        let outcome = uploads
            .run_locked(guard, move |dir, _g| {
                match read_session_meta_sync(&dir, &uuid)? {
                    SessionMetaOutcome::Present(meta) => {
                        if meta.repo != repo || meta.uuid != uuid {
                            return Ok(StatusOutcome::NotFound);
                        }
                        Ok(StatusOutcome::Status {
                            state: meta.state,
                            committed_offset: meta.committed_offset,
                            created: meta.created_at_unix_secs,
                            last_active: meta.last_active_at_unix_secs,
                        })
                    }
                    SessionMetaOutcome::Corrupt(msg) => Ok(StatusOutcome::Corrupt(msg)),
                    SessionMetaOutcome::Absent => {
                        // Already finalized? Read the receipt through the pinned
                        // finalized authority.
                        if let Some(receipt) = read_receipt_sync(&finalized, &uuid)? {
                            return Ok(StatusOutcome::Status {
                                state: UploadSessionState::Finalizing,
                                committed_offset: receipt.size,
                                created: receipt.finalized_at_unix_secs,
                                last_active: receipt.finalized_at_unix_secs,
                            });
                        }

                        // Legacy first-access migration: a bare `{uuid}` data file and/or
                        // a `.sha256state` sidecar written by an older format.
                        let legacy_name = match FileName::new(uuid.clone()) {
                            Ok(n) => n,
                            Err(_) => return Ok(StatusOutcome::NotFound),
                        };
                        let data_name = session_data_name(&uuid)?;
                        let legacy_regular = dir
                            .inspect(&legacy_name)
                            .ok()
                            .flatten()
                            .map(|id| is_regular(id.mode))
                            .unwrap_or(false);
                        let mut data_regular = dir
                            .inspect(&data_name)
                            .ok()
                            .flatten()
                            .map(|id| is_regular(id.mode))
                            .unwrap_or(false);

                        if legacy_regular || data_regular {
                            if legacy_regular && !data_regular {
                                let _ = dir.rename_leaf(&legacy_name, &dir, &data_name);
                                data_regular = dir
                                    .inspect(&data_name)
                                    .ok()
                                    .flatten()
                                    .map(|id| is_regular(id.mode))
                                    .unwrap_or(false);
                            }

                            if data_regular && let Some(id) = dir.inspect(&data_name)? {
                                let size = id.size;
                                let now = now_unix_secs();

                                let sidecar_name =
                                    FileName::new(format!("{uuid}.sha256state")).ok();
                                let sidecar_state = sidecar_name.as_ref().and_then(|n| {
                                    dir.read_leaf(n, SESSION_HASH_READ_LIMIT)
                                        .ok()
                                        .and_then(|b| SerializableSha256::from_bytes(&b))
                                });
                                let hash_st = match sidecar_state {
                                    Some(st) if st.total_len == size => st,
                                    _ => rebuild_hash_sync(&dir, &data_name, size)?,
                                };

                                let hash_name = session_hash_name(&uuid, 0)?;
                                dir.write_leaf_atomic(&hash_name, &hash_st.to_bytes(), true)?;
                                if let Some(n) = sidecar_name.as_ref() {
                                    let _ = dir.unlink(n, true);
                                }

                                let migrated = FsSessionMetaRecord {
                                    format_version: 1,
                                    repo: repo.clone(),
                                    uuid: uuid.clone(),
                                    state: UploadSessionState::Active,
                                    committed_offset: size,
                                    hash_generation: 0,
                                    created_at_unix_secs: now,
                                    last_active_at_unix_secs: now,
                                    finalizing_info: None,
                                };
                                write_session_meta_sync(&dir, &migrated)?;

                                return Ok(StatusOutcome::Status {
                                    state: UploadSessionState::Active,
                                    committed_offset: size,
                                    created: now,
                                    last_active: now,
                                });
                            }
                        }

                        Ok(StatusOutcome::NotFound)
                    }
                }
            })
            .await
            .map_err(|e| UploadTransitionError::Storage(map_fs_mutate_err(e)))?;

        match outcome {
            StatusOutcome::Status {
                state,
                committed_offset,
                created,
                last_active,
            } => Ok(UploadSessionStatus {
                session: session.clone(),
                state,
                committed_offset,
                created_at: UNIX_EPOCH + Duration::from_secs(created),
                last_active_at: UNIX_EPOCH + Duration::from_secs(last_active),
            }),
            StatusOutcome::NotFound => Err(UploadTransitionError::NotFound),
            StatusOutcome::Corrupt(msg) => Err(UploadTransitionError::Storage(
                StorageError::corrupt_data(msg),
            )),
        }
    }

    async fn append_if_offset(
        &self,
        session: &UploadSessionId,
        expected_offset: UploadOffsetPrecondition,
        stream: UploadByteStream,
        max_upload_bytes: u64,
    ) -> Result<UploadAppendResult, UploadTransitionError> {
        let se = |e: FsMutateError| UploadTransitionError::Storage(map_fs_mutate_err(e));
        let uploads = self.upload_authorities.uploads().await.map_err(se)?;

        let lock_name = session_lock_name(&session.uuid).map_err(se)?;
        let guard = uploads.lock(&lock_name).await.map_err(se)?;

        // Supervised owner: the whole inspect -> recover -> append -> commit body runs
        // inside `run_locked` on a blocking thread that holds the session lock until
        // the mutation commits or is rolled back, even if this request future is
        // cancelled. See the "Supervised streaming owner" section above.
        let repo = session.repo.clone();
        let uuid = session.uuid.clone();
        let limit = if max_upload_bytes > 0 {
            max_upload_bytes
        } else {
            self.max_upload_bytes
        };

        enum AppendOutcome {
            Committed { new_offset: u64 },
            OffsetMismatch { current_offset: u64 },
            Conflict,
            NotFound,
            Corrupt(String),
            TooLarge,
            StreamAborted,
            Cancelled,
            Io(StorageError),
        }

        let (tx, rx) = tokio::sync::mpsc::channel::<StreamMsg>(STREAM_CHANNEL_CAP);
        let worker = uploads.run_locked(guard, move |dir, _g| {
            let mut rx = rx;
            let data_name = session_data_name(&uuid)?;

            let mut meta = match read_session_meta_sync(&dir, &uuid)? {
                SessionMetaOutcome::Present(m) => m,
                SessionMetaOutcome::Corrupt(msg) => return Ok(AppendOutcome::Corrupt(msg)),
                SessionMetaOutcome::Absent => return Ok(AppendOutcome::NotFound),
            };
            if meta.repo != repo || meta.uuid != uuid {
                return Ok(AppendOutcome::NotFound);
            }
            if meta.state != UploadSessionState::Active {
                return Ok(AppendOutcome::Conflict);
            }

            // Recovery-on-entry: drop any torn tail past the committed offset.
            if let Some(id) = dir.inspect(&data_name)?
                && id.size > meta.committed_offset
            {
                dir.truncate(&data_name, meta.committed_offset)?;
            }

            match expected_offset {
                UploadOffsetPrecondition::Exact(off) => {
                    if off != meta.committed_offset {
                        return Ok(AppendOutcome::OffsetMismatch {
                            current_offset: meta.committed_offset,
                        });
                    }
                }
                UploadOffsetPrecondition::CurrentForServerComposedMonolithicOperation => {}
            }

            // Load or rebuild the rolling hash for the committed prefix.
            let hash_name = session_hash_name(&uuid, meta.hash_generation)?;
            let loaded = match dir.read_leaf(&hash_name, SESSION_HASH_READ_LIMIT) {
                Ok(b) => SerializableSha256::from_bytes(&b),
                Err(_) => None,
            };
            let mut hash_st = match loaded {
                Some(st) if st.total_len == meta.committed_offset => st,
                _ => rebuild_hash_sync(&dir, &data_name, meta.committed_offset)?,
            };

            let mut file = dir
                .open_leaf_write(&data_name, LeafWriteMode::Append)?
                .into_file();
            let drain = drain_append_blocking(
                &mut file,
                &mut rx,
                &mut hash_st,
                meta.committed_offset,
                limit,
            );
            drop(file);

            let written = match drain {
                DrainOutcome::Finished { written } => written,
                DrainOutcome::TooLarge => return Ok(AppendOutcome::TooLarge),
                DrainOutcome::StreamAborted => return Ok(AppendOutcome::StreamAborted),
                DrainOutcome::Cancelled => return Ok(AppendOutcome::Cancelled),
                DrainOutcome::Io(e) => return Ok(AppendOutcome::Io(map_fs_io_err(e))),
            };

            // Commit: advance hash generation and meta.
            let next_gen = meta.hash_generation.saturating_add(1);
            let next_hash_name = session_hash_name(&uuid, next_gen)?;
            dir.write_leaf_atomic(&next_hash_name, &hash_st.to_bytes(), true)?;

            let now = now_unix_secs();
            meta.committed_offset = meta.committed_offset.saturating_add(written);
            meta.hash_generation = next_gen;
            meta.last_active_at_unix_secs = now;
            write_session_meta_sync(&dir, &meta)?;
            let _ = dir.unlink(&hash_name, true);

            Ok(AppendOutcome::Committed {
                new_offset: meta.committed_offset,
            })
        });

        let (feed_res, worker_res) = tokio::join!(feed_stream(stream, tx), worker);
        match worker_res.map_err(se)? {
            AppendOutcome::Committed { new_offset } => {
                Ok(UploadAppendResult::Committed { new_offset })
            }
            AppendOutcome::OffsetMismatch { current_offset } => {
                Ok(UploadAppendResult::OffsetMismatch { current_offset })
            }
            AppendOutcome::Conflict => Ok(UploadAppendResult::Conflict),
            AppendOutcome::NotFound => Err(UploadTransitionError::NotFound),
            AppendOutcome::Corrupt(msg) => Err(UploadTransitionError::Storage(
                StorageError::corrupt_data(msg),
            )),
            AppendOutcome::TooLarge => Err(UploadTransitionError::TooLarge),
            AppendOutcome::StreamAborted => Err(UploadTransitionError::Stream(
                feed_res.unwrap_or(UploadStreamError::IdleTimeout),
            )),
            AppendOutcome::Cancelled => Err(UploadTransitionError::Storage(StorageError::io(
                "append cancelled before completion",
            ))),
            AppendOutcome::Io(e) => Err(UploadTransitionError::Storage(e)),
        }
    }

    async fn begin_finalize(
        &self,
        session: &UploadSessionId,
        expected_offset: UploadOffsetPrecondition,
        trailing_stream: Option<UploadByteStream>,
        expected_digest: &Digest,
        max_upload_bytes: u64,
        abort_on_digest_mismatch: bool,
    ) -> Result<PreparedFinalize, UploadTransitionError> {
        let se = |e: FsMutateError| UploadTransitionError::Storage(map_fs_mutate_err(e));
        let uploads = self.upload_authorities.uploads().await.map_err(se)?;
        // Pre-initialize the shared `.finalized` authority and hand its blocking view
        // to the supervised worker, which owns the lock across the (optional) trailing
        // stream, the digest verification, and the Finalizing persist.
        let finalized = self
            .upload_authorities
            .finalized()
            .await
            .map_err(se)?
            .blocking();

        let lock_name = session_lock_name(&session.uuid).map_err(se)?;
        let guard = uploads.lock(&lock_name).await.map_err(se)?;

        let repo = session.repo.clone();
        let uuid = session.uuid.clone();
        let expected_digest_owned = expected_digest.clone();
        let limit = if max_upload_bytes > 0 {
            max_upload_bytes
        } else {
            self.max_upload_bytes
        };

        enum FinOutcome {
            Prepared {
                operation_id: String,
                committed_offset: u64,
            },
            AlreadyFinalizedReplay {
                size: u64,
            },
            NotFound,
            Conflict,
            Corrupt(String),
            OffsetMismatch {
                current: u64,
            },
            DigestMismatch {
                computed: String,
            },
            TooLarge,
            StreamAborted,
            Cancelled,
            Io(StorageError),
        }

        // A channel is created only when there is a trailing stream to drain.
        let (tx, rx_opt) = match &trailing_stream {
            Some(_) => {
                let (tx, rx) = tokio::sync::mpsc::channel::<StreamMsg>(STREAM_CHANNEL_CAP);
                (Some(tx), Some(rx))
            }
            None => (None, None),
        };

        let worker = uploads.run_locked(guard, move |dir, _g| {
            let data_name = session_data_name(&uuid)?;
            let meta_name = session_meta_name(&uuid)?;

            let mut meta = match read_session_meta_sync(&dir, &uuid)? {
                SessionMetaOutcome::Present(m) => m,
                SessionMetaOutcome::Corrupt(msg) => return Ok(FinOutcome::Corrupt(msg)),
                SessionMetaOutcome::Absent => {
                    // Meta absent: consult the finalized receipt for idempotent replay.
                    return match read_receipt_sync(&finalized, &uuid)? {
                        Some(receipt) => {
                            if receipt.digest == expected_digest_owned.as_str() {
                                Ok(FinOutcome::AlreadyFinalizedReplay { size: receipt.size })
                            } else {
                                Ok(FinOutcome::DigestMismatch {
                                    computed: receipt.digest.clone(),
                                })
                            }
                        }
                        None => Ok(FinOutcome::NotFound),
                    };
                }
            };
            if meta.repo != repo || meta.uuid != uuid {
                return Ok(FinOutcome::NotFound);
            }
            if meta.state != UploadSessionState::Active {
                return Ok(FinOutcome::Conflict);
            }

            // Recovery-on-entry: drop any torn tail past the committed offset.
            if let Some(id) = dir.inspect(&data_name)?
                && id.size > meta.committed_offset
            {
                dir.truncate(&data_name, meta.committed_offset)?;
            }

            // Optional trailing stream: append and roll the committed hash forward.
            if let Some(mut rx) = rx_opt {
                let hash_name = session_hash_name(&uuid, meta.hash_generation)?;
                let loaded = match dir.read_leaf(&hash_name, SESSION_HASH_READ_LIMIT) {
                    Ok(b) => SerializableSha256::from_bytes(&b),
                    Err(_) => None,
                };
                let mut hash_st = match loaded {
                    Some(st) if st.total_len == meta.committed_offset => st,
                    _ => rebuild_hash_sync(&dir, &data_name, meta.committed_offset)?,
                };
                let mut file = dir
                    .open_leaf_write(&data_name, LeafWriteMode::Append)?
                    .into_file();
                let drain = drain_append_blocking(
                    &mut file,
                    &mut rx,
                    &mut hash_st,
                    meta.committed_offset,
                    limit,
                );
                drop(file);
                let written = match drain {
                    DrainOutcome::Finished { written } => written,
                    DrainOutcome::TooLarge => return Ok(FinOutcome::TooLarge),
                    DrainOutcome::StreamAborted => return Ok(FinOutcome::StreamAborted),
                    DrainOutcome::Cancelled => return Ok(FinOutcome::Cancelled),
                    DrainOutcome::Io(e) => return Ok(FinOutcome::Io(map_fs_io_err(e))),
                };
                let next_gen = meta.hash_generation.saturating_add(1);
                let next_hash_name = session_hash_name(&uuid, next_gen)?;
                dir.write_leaf_atomic(&next_hash_name, &hash_st.to_bytes(), true)?;
                meta.committed_offset = meta.committed_offset.saturating_add(written);
                meta.hash_generation = next_gen;
                let _ = dir.unlink(&hash_name, true);
            }

            match expected_offset {
                UploadOffsetPrecondition::Exact(off) => {
                    if off != meta.committed_offset {
                        return Ok(FinOutcome::OffsetMismatch {
                            current: meta.committed_offset,
                        });
                    }
                }
                UploadOffsetPrecondition::CurrentForServerComposedMonolithicOperation => {}
            }

            // Verify the digest over the committed prefix.
            let computed_hex = if expected_digest_owned.algorithm() == "sha512" {
                use std::io::Read as _;
                let mut file = dir.open_leaf_read(&data_name)?.into_file();
                let mut hasher = sha2::Sha512::new();
                let mut buf = vec![0u8; 64 * 1024];
                let mut read_total = 0u64;
                while read_total < meta.committed_offset {
                    let to_read =
                        ((meta.committed_offset - read_total).min(buf.len() as u64)) as usize;
                    let n = file.read(&mut buf[..to_read]).map_err(FsMutateError::Io)?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                    read_total += n as u64;
                }
                hex::encode(hasher.finalize())
            } else {
                let hash_name = session_hash_name(&uuid, meta.hash_generation)?;
                let loaded = match dir.read_leaf(&hash_name, SESSION_HASH_READ_LIMIT) {
                    Ok(b) => SerializableSha256::from_bytes(&b),
                    Err(_) => None,
                };
                let st = match loaded {
                    Some(s) if s.total_len == meta.committed_offset => s,
                    _ => rebuild_hash_sync(&dir, &data_name, meta.committed_offset)?,
                };
                st.finalize_hex()
            };

            if computed_hex != expected_digest_owned.hex() {
                if abort_on_digest_mismatch {
                    let _ = dir.unlink(&data_name, true);
                    let _ = dir.unlink(&meta_name, true);
                    let hash_name = session_hash_name(&uuid, meta.hash_generation)?;
                    let _ = dir.unlink(&hash_name, true);
                }
                return Ok(FinOutcome::DigestMismatch {
                    computed: computed_hex,
                });
            }

            // Persist Finalizing state.
            let operation_id = uuid::Uuid::new_v4().to_string();
            let now = now_unix_secs();
            meta.state = UploadSessionState::Finalizing;
            meta.finalizing_info = Some(FsFinalizingInfo {
                operation_id: operation_id.clone(),
                expected_digest: expected_digest_owned.as_str().to_string(),
                size: meta.committed_offset,
                finalizing_at_unix_secs: now,
            });
            meta.last_active_at_unix_secs = now;
            write_session_meta_sync(&dir, &meta)?;

            Ok(FinOutcome::Prepared {
                operation_id,
                committed_offset: meta.committed_offset,
            })
        });

        let (feed_res, worker_res) = if let Some(stream) = trailing_stream {
            let tx = tx.expect("channel sender present when trailing stream present");
            tokio::join!(feed_stream(stream, tx), worker)
        } else {
            (None, worker.await)
        };

        match worker_res.map_err(se)? {
            FinOutcome::Prepared {
                operation_id,
                committed_offset,
            } => Ok(PreparedFinalize {
                session: session.clone(),
                operation_id,
                expected_digest: expected_digest.clone(),
                committed_offset,
                size: committed_offset,
            }),
            FinOutcome::AlreadyFinalizedReplay { size } => Ok(PreparedFinalize {
                session: session.clone(),
                operation_id: "already-finalized".to_string(),
                expected_digest: expected_digest.clone(),
                committed_offset: size,
                size,
            }),
            FinOutcome::NotFound => Err(UploadTransitionError::NotFound),
            FinOutcome::Conflict => Err(UploadTransitionError::Conflict),
            FinOutcome::Corrupt(msg) => Err(UploadTransitionError::Storage(
                StorageError::corrupt_data(msg),
            )),
            FinOutcome::OffsetMismatch { current } => Err(UploadTransitionError::OffsetMismatch {
                expected: expected_offset,
                current,
            }),
            FinOutcome::DigestMismatch { computed } => Err(UploadTransitionError::DigestMismatch {
                expected: expected_digest.clone(),
                computed,
            }),
            FinOutcome::TooLarge => Err(UploadTransitionError::TooLarge),
            FinOutcome::StreamAborted => Err(UploadTransitionError::Stream(
                feed_res.unwrap_or(UploadStreamError::IdleTimeout),
            )),
            FinOutcome::Cancelled => Err(UploadTransitionError::Storage(StorageError::io(
                "finalize cancelled before completion",
            ))),
            FinOutcome::Io(e) => Err(UploadTransitionError::Storage(e)),
        }
    }

    async fn commit_finalize(
        &self,
        prepared: &PreparedFinalize,
    ) -> Result<FinalizeOutcome, UploadTransitionError> {
        enum CommitOutcome {
            Published(u64),
            AlreadyFinalized(u64),
            NotFound,
            Invalid,
            Corrupt(String),
        }

        let prepared = prepared.clone();
        let se = |e: FsMutateError| UploadTransitionError::Storage(map_fs_mutate_err(e));
        let uploads = self.upload_authorities.uploads().await.map_err(se)?;
        // Pre-initialize the shared sibling authorities in the async context so the
        // owned-boundary body operates on the same cached descriptors; nothing in the
        // body reopens a (possibly replaced) subtree by pathname.
        let finalized = self
            .upload_authorities
            .finalized()
            .await
            .map_err(se)?
            .blocking();
        let blobs = self
            .upload_authorities
            .blobs()
            .await
            .map_err(se)?
            .blocking();
        let memberships = self
            .upload_authorities
            .memberships()
            .await
            .map_err(se)?
            .blocking();

        let lock_name = session_lock_name(&prepared.session.uuid).map_err(se)?;
        let guard = uploads.lock(&lock_name).await.map_err(se)?;

        let outcome = uploads
            .run_locked(guard, move |dir, _g| {
                let uuid = &prepared.session.uuid;
                let digest = &prepared.expected_digest;

                // 1. Idempotent replay: a matching receipt already exists.
                if let Some(receipt) = read_receipt_sync(&finalized, uuid)?
                    && receipt.digest == digest.as_str()
                {
                    return Ok(CommitOutcome::AlreadyFinalized(receipt.size));
                }

                // 2. Validate the session metadata.
                let data_name = session_data_name(uuid)?;
                let meta = match read_session_meta_sync(&dir, uuid)? {
                    SessionMetaOutcome::Present(m) => m,
                    SessionMetaOutcome::Corrupt(msg) => return Ok(CommitOutcome::Corrupt(msg)),
                    SessionMetaOutcome::Absent => {
                        // Meta gone: if the CAS blob is already published at the
                        // expected size, (re)assert membership + receipt and report
                        // an idempotent success rather than a spurious NotFound.
                        if let Some(id) = cas_blob_present_sync(&blobs, digest)?
                            && id.size == prepared.size
                        {
                            let membership =
                                crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
                                    prepared.session.repo.clone(),
                                    digest.clone(),
                                    Some(uuid.clone()),
                                );
                            write_membership_sync(&memberships, &membership)?;
                            let receipt = FinalizedReceipt {
                                repo: prepared.session.repo.clone(),
                                uuid: uuid.clone(),
                                digest: digest.as_str().to_string(),
                                size: prepared.size,
                                finalized_at_unix_secs: now_unix_secs(),
                                format_version: 1,
                            };
                            write_receipt_sync(&finalized, &receipt)?;
                            return Ok(CommitOutcome::AlreadyFinalized(prepared.size));
                        }
                        return Ok(CommitOutcome::NotFound);
                    }
                };

                if meta.state != UploadSessionState::Finalizing {
                    return Ok(CommitOutcome::Invalid);
                }
                let Some(ref fin_info) = meta.finalizing_info else {
                    return Ok(CommitOutcome::Invalid);
                };
                if fin_info.operation_id != prepared.operation_id
                    || fin_info.expected_digest != digest.as_str()
                {
                    return Ok(CommitOutcome::Invalid);
                }

                // 3. Publish to CAS: blobs/{algo}/{prefix2}/{hex} via contained rename.
                let algo_dir = blobs.ensure_subdir(&FileName::new(digest.algorithm())?)?;
                let shard_dir = algo_dir.ensure_subdir(&FileName::new(digest.prefix2())?)?;
                let hex_name = FileName::new(digest.hex())?;
                if let Err(err) = dir.rename_leaf(&data_name, &shard_dir, &hex_name) {
                    // Tolerate a prior partial publication only when the destination
                    // already holds a blob of the expected size.
                    match shard_dir.inspect(&hex_name)? {
                        Some(id) if id.size == prepared.size => {}
                        _ => return Err(err),
                    }
                }
                let _ = dir.sync();
                let _ = shard_dir.sync();

                // 4. Durable target-repository membership BEFORE the receipt.
                let membership =
                    crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
                        prepared.session.repo.clone(),
                        digest.clone(),
                        Some(uuid.clone()),
                    );
                write_membership_sync(&memberships, &membership)?;

                // 5. Finalized receipt.
                let receipt = FinalizedReceipt {
                    repo: prepared.session.repo.clone(),
                    uuid: uuid.clone(),
                    digest: digest.as_str().to_string(),
                    size: prepared.size,
                    finalized_at_unix_secs: now_unix_secs(),
                    format_version: 1,
                };
                write_receipt_sync(&finalized, &receipt)?;

                // 6. Remove the staging meta + hash LAST.
                let meta_name = session_meta_name(uuid)?;
                let _ = dir.unlink(&meta_name, true);
                let hash_name = session_hash_name(uuid, meta.hash_generation)?;
                let _ = dir.unlink(&hash_name, true);

                Ok(CommitOutcome::Published(prepared.size))
            })
            .await
            .map_err(se)?;

        match outcome {
            CommitOutcome::Published(size) => Ok(FinalizeOutcome::Published(BlobMeta { size })),
            CommitOutcome::AlreadyFinalized(size) => {
                Ok(FinalizeOutcome::AlreadyFinalized(BlobMeta { size }))
            }
            CommitOutcome::NotFound => Err(UploadTransitionError::NotFound),
            CommitOutcome::Invalid => Err(UploadTransitionError::InvalidPreparedHandle),
            CommitOutcome::Corrupt(msg) => Err(UploadTransitionError::Storage(
                StorageError::corrupt_data(msg),
            )),
        }
    }

    async fn abort_session(&self, session: &UploadSessionId) -> Result<(), StorageError> {
        let uploads = self
            .upload_authorities
            .uploads()
            .await
            .map_err(map_fs_mutate_err)?;
        let uuid = session.uuid.clone();

        let lock_name = session_lock_name(&uuid).map_err(map_fs_mutate_err)?;
        let guard = uploads.lock(&lock_name).await.map_err(map_fs_mutate_err)?;

        uploads
            .run_locked(guard, move |dir, _g| abort_session_locked(&dir, &uuid))
            .await
            .map_err(map_fs_mutate_err)?;

        Ok(())
    }

    async fn recover_session(
        &self,
        session: &UploadSessionId,
    ) -> Result<UploadSessionStatus, UploadTransitionError> {
        let repo = session.repo.clone();
        let uuid = session.uuid.clone();
        let se = |e: FsMutateError| UploadTransitionError::Storage(map_fs_mutate_err(e));
        let uploads = self.upload_authorities.uploads().await.map_err(se)?;
        // Pre-initialize the shared sibling authorities in the async context; the body
        // then operates on the same cached descriptors under the held lock.
        let finalized = self
            .upload_authorities
            .finalized()
            .await
            .map_err(se)?
            .blocking();
        let blobs = self
            .upload_authorities
            .blobs()
            .await
            .map_err(se)?
            .blocking();
        let memberships = self
            .upload_authorities
            .memberships()
            .await
            .map_err(se)?
            .blocking();

        let lock_name = session_lock_name(&uuid).map_err(se)?;
        let guard = uploads.lock(&lock_name).await.map_err(se)?;

        let outcome = uploads
            .run_locked(guard, move |dir, _g| {
                recover_session_locked(&dir, &finalized, &blobs, &memberships, &repo, &uuid)
            })
            .await
            .map_err(se)?;

        let (state, committed_offset, created, last_active) = match outcome {
            RecoverLocked::RolledForward {
                committed_offset,
                created,
                last_active,
            } => (
                UploadSessionState::Finalizing,
                committed_offset,
                created,
                last_active,
            ),
            RecoverLocked::Pending {
                state,
                committed_offset,
                created,
                last_active,
            } => (state, committed_offset, created, last_active),
            RecoverLocked::NotFound => return Err(UploadTransitionError::NotFound),
            RecoverLocked::Corrupt(msg) => {
                return Err(UploadTransitionError::Storage(StorageError::corrupt_data(
                    msg,
                )));
            }
        };

        Ok(UploadSessionStatus {
            session: session.clone(),
            state,
            committed_offset,
            created_at: UNIX_EPOCH + Duration::from_secs(created),
            last_active_at: UNIX_EPOCH + Duration::from_secs(last_active),
        })
    }

    async fn get_finalized_receipt(
        &self,
        session: &UploadSessionId,
    ) -> Result<Option<FinalizedReceipt>, StorageError> {
        // Route the public lookup through the SAME pinned finalized authority the
        // writers (`commit_finalize`, recovery roll-forward) publish through, rather
        // than re-resolving `uploads/.finalized` from the pinned root on each call.
        // This keeps reader and writer in agreement after a `.finalized` (or
        // `uploads`) pathname replacement. Repository/UUID validation, missing-file
        // behavior, and corrupt/error semantics are preserved.
        tag_read::validate_path_component(&session.uuid, "upload session id")?;
        let finalized = self
            .upload_authorities
            .finalized()
            .await
            .map_err(map_fs_mutate_err)?;
        let name = match finalized_receipt_name(&session.uuid) {
            Ok(n) => n,
            // A uuid that passed component validation but is not a single contained
            // leaf name has no receipt for THIS session.
            Err(_) => return Ok(None),
        };
        let bytes = match finalized
            .read_leaf(&name, FINALIZED_RECEIPT_READ_LIMIT)
            .await
        {
            Ok(b) => b,
            Err(FsMutateError::NotFound) => return Ok(None),
            Err(FsMutateError::InvalidName { .. }) => return Ok(None),
            Err(e) => return Err(map_fs_mutate_err(e)),
        };
        let receipt: FinalizedReceipt = serde_json::from_slice(&bytes)
            .map_err(|e| StorageError::corrupt_data(e.to_string()))?;
        // Preserved identity semantics: a receipt for another repository/session is
        // "no receipt for THIS session", never an accepted foreign receipt.
        if receipt.repo == session.repo && receipt.uuid == session.uuid {
            Ok(Some(receipt))
        } else {
            Ok(None)
        }
    }

    async fn reap_expired_sessions(
        &self,
        max_age_secs: u64,
        receipt_ttl_secs: u64,
    ) -> Result<usize, StorageError> {
        let now = now_unix_secs();
        let mut count = 0usize;

        let uploads = self
            .upload_authorities
            .uploads()
            .await
            .map_err(map_fs_mutate_err)?;
        let finalized = self
            .upload_authorities
            .finalized()
            .await
            .map_err(map_fs_mutate_err)?;
        let blobs = self
            .upload_authorities
            .blobs()
            .await
            .map_err(map_fs_mutate_err)?;
        let memberships = self
            .upload_authorities
            .memberships()
            .await
            .map_err(map_fs_mutate_err)?;

        #[cfg(test)]
        let boundary_hook = self.reaper_boundary_hook.0.lock().unwrap().clone();
        #[cfg(test)]
        let receipt_boundary_hook = self.reaper_receipt_boundary_hook.0.lock().unwrap().clone();

        // Sessions: enumerate `{uuid}.meta.json` under the pinned uploads authority.
        // Each candidate's fresh inspection, expiry decision, revalidation, and
        // destructive action run inside ONE `run_locked` body under a single
        // continuously-held `.lock.{uuid}` — the lock is acquired once via `try_lock`
        // and never dropped-and-reacquired, so no cooperating update can slip between
        // the locked check and the action. Inspection and every destructive action
        // resolve through the SAME pinned subtree, so a detached original tree's
        // expiry can never drive deletion of a same-UUID replacement in a fresh tree.
        // `count` tallies only CONFIRMED cleanups; busy / not-expired / absent /
        // corrupt / changed candidates are distinguished from failures, per-candidate
        // failures are logged and skipped, and a fatal listing failure is surfaced.
        let session_entries = uploads
            .list(storage_fs::DirEnumerationLimits::new(
                1_048_576,
                256 * 1024 * 1024,
            ))
            .await
            .map_err(map_fs_mutate_err)?;
        for entry in session_entries {
            let file_name = entry.name().to_string_lossy().into_owned();
            let Some(uuid) = file_name.strip_suffix(".meta.json") else {
                continue;
            };
            let uuid = uuid.to_string();

            let Ok(lock_name) = session_lock_name(&uuid) else {
                continue;
            };

            // Acquire the session lock ONCE and hold it across inspection, expiry
            // decision, revalidation, and action. A live participant holding the lock
            // (Busy / None) means the session is not ours to reap.
            let guard = match uploads.try_lock(&lock_name).await {
                Ok(Some(g)) => g,
                Ok(None) | Err(FsMutateError::Busy) => continue,
                Err(err) => {
                    tracing::warn!(uuid = %uuid, error = %err, "reaper: session lock probe failed");
                    continue;
                }
            };

            let finalized_view = finalized.blocking();
            let blobs_view = blobs.blocking();
            let memberships_view = memberships.blocking();
            let uuid_body = uuid.clone();
            #[cfg(test)]
            let hook = boundary_hook.clone();

            let outcome = uploads
                .run_locked(guard, move |dir, _g| {
                    let uuid = uuid_body;
                    // Fresh meta read UNDER the lock — never a stale pre-lock decision.
                    let meta = match read_session_meta_sync(&dir, &uuid)? {
                        SessionMetaOutcome::Present(m) => m,
                        SessionMetaOutcome::Corrupt(msg) => {
                            return Ok(ReapOutcome::Corrupt(msg));
                        }
                        SessionMetaOutcome::Absent => return Ok(ReapOutcome::Absent),
                    };
                    // Expiry decided against the freshly-read `last_active`.
                    if now.saturating_sub(meta.last_active_at_unix_secs) < max_age_secs {
                        return Ok(ReapOutcome::NotExpired);
                    }

                    // Boundary between the confirmed expiry decision and the
                    // destructive action. The lock is still held here; a cooperating
                    // update cannot proceed until this closure returns.
                    #[cfg(test)]
                    if let Some(hook) = &hook {
                        hook(&uuid);
                    }

                    match meta.state {
                        UploadSessionState::Finalizing => {
                            // Baseline policy: attempt recovery for an expired
                            // Finalizing session, but NEVER abort it. Containment did
                            // not authorize a new destructive expiry policy.
                            //   * A fully-published CAS blob rolls forward (a completed
                            //     finalization) and counts as a cleanup.
                            //   * A not-yet-published finalization stays intact and
                            //     available for later completion/recovery; it is not
                            //     counted and its staging data + meta survive.
                            //   * A corrupt/failed recovery is diagnosed and must not
                            //     authorize deletion.
                            match recover_session_locked(
                                &dir,
                                &finalized_view,
                                &blobs_view,
                                &memberships_view,
                                &meta.repo,
                                &uuid,
                            )? {
                                RecoverLocked::RolledForward { .. } => Ok(ReapOutcome::CleanedUp),
                                RecoverLocked::Pending { .. } => {
                                    Ok(ReapOutcome::PendingFinalization)
                                }
                                RecoverLocked::Corrupt(msg) => Ok(ReapOutcome::Corrupt(msg)),
                                RecoverLocked::NotFound => Ok(ReapOutcome::Absent),
                            }
                        }
                        // Any other expired state (Appending, …) is aborted directly.
                        // Abort removes the staging data regardless of a torn tail, so
                        // a preceding recovery would be redundant; the state machine
                        // for an expired non-finalizing session is simply "abort".
                        _ => {
                            abort_session_locked(&dir, &uuid)?;
                            Ok(ReapOutcome::CleanedUp)
                        }
                    }
                })
                .await;

            match outcome {
                Ok(ReapOutcome::CleanedUp) => count += 1,
                Ok(ReapOutcome::PendingFinalization) => {
                    tracing::debug!(
                        uuid = %uuid,
                        "reaper: expired finalizing session left intact for later completion"
                    );
                }
                Ok(ReapOutcome::Corrupt(msg)) => {
                    tracing::warn!(uuid = %uuid, detail = %msg, "reaper: skipped corrupt session meta");
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!(uuid = %uuid, error = %err, "reaper: session cleanup failed");
                }
            }
        }

        // Receipts: enumerate `{uuid}.json` under the pinned finalized authority.
        // Each receipt is unlinked only under the matching `.lock.{uuid}` session
        // lock, re-reading it under the lock so a concurrently (re)published or
        // identity-changed receipt, or a fresh same-UUID session, is respected.
        let receipt_entries = finalized
            .list(storage_fs::DirEnumerationLimits::new(
                1_048_576,
                256 * 1024 * 1024,
            ))
            .await
            .map_err(map_fs_mutate_err)?;
        for entry in receipt_entries {
            let file_name = entry.name().to_string_lossy().into_owned();
            let Some(uuid) = file_name.strip_suffix(".json") else {
                continue;
            };
            let uuid = uuid.to_string();

            let Ok(lock_name) = session_lock_name(&uuid) else {
                continue;
            };

            // Hold the session lock across the receipt re-read, TTL decision, and
            // unlink. A busy lock means a live same-UUID session owns it — leave its
            // receipt in place.
            let guard = match uploads.try_lock(&lock_name).await {
                Ok(Some(g)) => g,
                Ok(None) | Err(FsMutateError::Busy) => continue,
                Err(err) => {
                    tracing::warn!(uuid = %uuid, error = %err, "reaper: receipt lock probe failed");
                    continue;
                }
            };

            let finalized_view = finalized.blocking();
            let uuid_body = uuid.clone();
            #[cfg(test)]
            let hook = receipt_boundary_hook.clone();
            let outcome = uploads
                .run_locked(guard, move |_dir, _g| {
                    let uuid = uuid_body;
                    // Boundary under the held lock, before the receipt re-read. A
                    // regression may mutate the on-disk receipt here (delete /
                    // republish fresh / change identity) to prove the reaper acts on
                    // the CURRENT under-lock state, not a stale listing-time snapshot.
                    #[cfg(test)]
                    if let Some(hook) = &hook {
                        hook(&uuid);
                    }
                    // Re-read the CURRENT receipt under the lock.
                    let Some(receipt) = read_receipt_sync(&finalized_view, &uuid)? else {
                        return Ok(ReapOutcome::Absent);
                    };
                    // Identity revalidation: a receipt whose stored uuid no longer
                    // matches the leaf name has been replaced; do not delete it.
                    if receipt.uuid != uuid {
                        return Ok(ReapOutcome::Changed);
                    }
                    if now.saturating_sub(receipt.finalized_at_unix_secs) < receipt_ttl_secs {
                        return Ok(ReapOutcome::NotExpired);
                    }
                    let name = finalized_receipt_name(&uuid)?;
                    finalized_view.unlink(&name, true)?;
                    Ok(ReapOutcome::CleanedUp)
                })
                .await;

            match outcome {
                Ok(ReapOutcome::CleanedUp) => count += 1,
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!(uuid = %uuid, error = %err, "reaper: receipt cleanup failed");
                }
            }
        }

        Ok(count)
    }
}

#[async_trait]
impl RepositoryBlobMembershipStorage for FsStorage {
    async fn get_repo_blob_membership(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<Option<crate::storage::repo_membership::RepoBlobMembershipRecord>, StorageError>
    {
        membership_read::get_repo_blob_membership_impl(self.reader.as_ref(), repo, digest).await
    }

    async fn link_repo_blob(
        &self,
        record: &crate::storage::repo_membership::RepoBlobMembershipRecord,
    ) -> Result<(), StorageError> {
        // Route the durable membership write through the pinned memberships authority,
        // mirroring `canonical_repo_membership_relpath`
        // (`repo-memberships/by-repo/{key}/{algo}/{hex}.json`). The candidate
        // transitions and `unlink_repo_blob` resolve the same layout through the
        // non-creating `membership_record_authority`; reads resolve independently
        // through the contained reader seam.
        let memberships = self
            .upload_authorities
            .memberships()
            .await
            .map_err(map_fs_mutate_err)?;
        let key = crate::storage::repo_membership::encode_canonical_repo_key(&record.repo);
        let by_repo = memberships
            .ensure_subdir(&FileName::new("by-repo").map_err(map_fs_mutate_err)?)
            .await
            .map_err(map_fs_mutate_err)?;
        let repo_dir = by_repo
            .ensure_subdir(&FileName::new(key).map_err(map_fs_mutate_err)?)
            .await
            .map_err(map_fs_mutate_err)?;
        let algo_dir = repo_dir
            .ensure_subdir(&FileName::new(record.digest.algorithm()).map_err(map_fs_mutate_err)?)
            .await
            .map_err(map_fs_mutate_err)?;
        let leaf =
            FileName::new(format!("{}.json", record.digest.hex())).map_err(map_fs_mutate_err)?;
        let bytes = serde_json::to_vec(record)
            .map_err(|e| StorageError::serialization(format!("serialize membership: {e}")))?;
        algo_dir
            .write_leaf_atomic(&leaf, bytes, true)
            .await
            .map_err(map_fs_mutate_err)?;
        Ok(())
    }

    async fn set_membership_candidate(
        &self,
        repo: &str,
        digest: &Digest,
        since_unix_secs: u64,
    ) -> Result<bool, StorageError> {
        // ONE contained membership authority (resolved non-creating beneath the
        // pinned memberships root) retained across the inspect/rewrite
        // transition; absent components preserve the Ok(false) contract with
        // zero directory creation. No lock exists on candidate transitions —
        // unchanged; no cross-process serialization is claimed.
        let canonical = CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let Some(dir) = self.membership_record_authority(&canonical, digest).await? else {
            return Ok(false);
        };
        let leaf = Self::membership_leaf_name(digest)?;
        Self::set_membership_candidate_in(&dir, &leaf, since_unix_secs).await
    }

    async fn clear_membership_candidate(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<bool, StorageError> {
        // Same retained single-authority shape as `set_membership_candidate`.
        let canonical = CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let Some(dir) = self.membership_record_authority(&canonical, digest).await? else {
            return Ok(false);
        };
        let leaf = Self::membership_leaf_name(digest)?;
        Self::clear_membership_candidate_in(&dir, &leaf).await
    }

    async fn unlink_repo_blob(&self, repo: &str, digest: &Digest) -> Result<bool, StorageError> {
        // Single contained unlink through the non-creating membership
        // authority; absent components or leaf preserve the Ok(false) contract.
        let canonical = CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let Some(dir) = self.membership_record_authority(&canonical, digest).await? else {
            return Ok(false);
        };
        let leaf = Self::membership_leaf_name(digest)?;
        match dir.unlink(&leaf, false).await {
            Ok(()) => {
                // Best-effort directory-entry durability (result ignored), as
                // with the other contained deletes; no observable change.
                let _ = dir.sync().await;
                Ok(true)
            }
            Err(FsMutateError::NotFound) => Ok(false),
            Err(err) => Err(map_fs_mutate_err(err)),
        }
    }

    async fn list_repo_blob_memberships_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<
        (
            Vec<crate::storage::repo_membership::RepoBlobMembershipRecord>,
            Option<String>,
        ),
        StorageError,
    > {
        membership_read::list_repo_blob_memberships_page_impl(
            self.reader.as_ref(),
            repo,
            continuation_token,
            page_limit,
        )
        .await
    }

    async fn list_all_repo_blob_memberships_page(
        &self,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<
        (
            Vec<crate::storage::repo_membership::RepoBlobMembershipRecord>,
            Option<String>,
        ),
        StorageError,
    > {
        membership_read::list_all_repo_blob_memberships_page_impl(
            self.reader.as_ref(),
            continuation_token,
            page_limit,
        )
        .await
    }

    async fn count_repo_blob_memberships(&self, digest: &Digest) -> Result<usize, StorageError> {
        membership_read::count_repo_blob_memberships_impl(self.reader.as_ref(), digest).await
    }

    async fn is_membership_ready(&self) -> Result<bool, StorageError> {
        let checkpoint = self.get_migration_checkpoint().await?;
        let ready_marker_exists =
            membership_read::membership_ready_marker_present(self.reader.as_ref()).await?;
        match checkpoint {
            Some(cp) => Ok(
                cp.phase == crate::storage::repo_membership::MigrationPhase::Ready
                    && ready_marker_exists,
            ),
            None => Ok(ready_marker_exists),
        }
    }

    async fn mark_membership_ready(&self) -> Result<(), StorageError> {
        let dir = self.root.join("meta");
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| StorageError::io(e.to_string()))?;
        let marker = dir.join("membership_ready.json");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let payload = serde_json::json!({
            "version": 1,
            "ready_at_unix_secs": now
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        atomic_write_file(&marker, &bytes).await?;

        let cp = self.get_migration_checkpoint().await?;
        let ready_cp = match cp {
            Some(mut c) => {
                c.phase = crate::storage::repo_membership::MigrationPhase::Ready;
                c.verification_result = Some(true);
                c.last_updated_unix_secs = now;
                c
            }
            None => crate::storage::repo_membership::MigrationCheckpointRecord {
                schema_version: 1,
                phase: crate::storage::repo_membership::MigrationPhase::Ready,
                owner_id: None,
                lease_expiry_unix_secs: None,
                source_continuation_token: None,
                current_repository: None,
                current_cursor: None,
                stats: crate::storage::repo_membership::MigrationStats::default(),
                started_unix_secs: now,
                last_updated_unix_secs: now,
                failure_info: None,
                verification_result: Some(true),
            },
        };
        self.save_migration_checkpoint(&ready_cp).await?;
        Ok(())
    }

    async fn get_migration_checkpoint(
        &self,
    ) -> Result<Option<crate::storage::repo_membership::MigrationCheckpointRecord>, StorageError>
    {
        membership_read::get_migration_checkpoint_impl(self.reader.as_ref()).await
    }

    async fn save_migration_checkpoint(
        &self,
        checkpoint: &crate::storage::repo_membership::MigrationCheckpointRecord,
    ) -> Result<(), StorageError> {
        let dir = self.root.join("meta");
        ensure_dir(&dir)?;
        let path = dir.join("migration_checkpoint.json");
        let bytes = serde_json::to_vec(checkpoint).map_err(|e| {
            StorageError::serialization(format!("serialize migration checkpoint: {e}"))
        })?;
        write_atomic_file(&path, &bytes).await?;
        Ok(())
    }
}

/// Resolve a sharded GC leaf directory (`<base>/<segments...>`) beneath a
/// pinned fixed top-level authority. `create = false` opens without creating
/// (`Ok(None)` when any component is absent, preserving absent-state
/// contracts with zero directory creation); `create = true` ensures the shard
/// directories. Fresh resolution per operation; never cached.
async fn gc_shard_authority(
    base: &ContainedDir,
    segments: &[&str],
    create: bool,
) -> Result<Option<ContainedDir>, StorageError> {
    let mut dir = base.clone();
    for segment in segments {
        let name = FileName::new(*segment).map_err(map_fs_mutate_err)?;
        dir = if create {
            dir.ensure_subdir(&name).await.map_err(map_fs_mutate_err)?
        } else {
            match dir.open_subdir(&name).await {
                Ok(d) => d,
                Err(FsMutateError::NotFound) => return Ok(None),
                Err(err) => return Err(map_fs_mutate_err(err)),
            }
        };
    }
    Ok(Some(dir))
}

impl FsStorage {
    /// Contained shard authority for `blobs/<algo>/<prefix2>` (CAS side of the
    /// quarantine protocol).
    async fn cas_blobs_shard(
        &self,
        digest: &Digest,
        create: bool,
    ) -> Result<Option<ContainedDir>, StorageError> {
        let blobs = self
            .upload_authorities
            .blobs()
            .await
            .map_err(map_fs_mutate_err)?;
        gc_shard_authority(&blobs, &[digest.algorithm(), digest.prefix2()], create).await
    }

    /// Contained shard authority for `quarantine/blobs/<algo>/<prefix2>`.
    async fn quarantine_blobs_shard(
        &self,
        digest: &Digest,
        create: bool,
    ) -> Result<Option<ContainedDir>, StorageError> {
        let quarantine = self
            .upload_authorities
            .quarantine()
            .await
            .map_err(map_fs_mutate_err)?;
        gc_shard_authority(
            &quarantine,
            &["blobs", digest.algorithm(), digest.prefix2()],
            create,
        )
        .await
    }

    /// Contained shard authority for `quarantine/meta/<algo>/<prefix2>`.
    async fn quarantine_meta_shard(
        &self,
        digest: &Digest,
        create: bool,
    ) -> Result<Option<ContainedDir>, StorageError> {
        let quarantine = self
            .upload_authorities
            .quarantine()
            .await
            .map_err(map_fs_mutate_err)?;
        gc_shard_authority(
            &quarantine,
            &["meta", digest.algorithm(), digest.prefix2()],
            create,
        )
        .await
    }

    fn quarantine_ts_leaf(digest: &Digest) -> Result<FileName, StorageError> {
        FileName::new(format!("{}.ts", digest.hex())).map_err(map_fs_mutate_err)
    }

    fn blob_leaf(digest: &Digest) -> Result<FileName, StorageError> {
        FileName::new(digest.hex()).map_err(map_fs_mutate_err)
    }

    /// Inner conditional-delete sequence on an ALREADY-RESOLVED quarantine
    /// shard authority: open the leaf through that authority, recompute the
    /// version on the OPENED descriptor (fstat + streaming hash — revalidation
    /// refers to exactly the inode the unlink targets), compare, and unlink
    /// through the SAME authority. A namespace replacement after resolution
    /// cannot split the object that is revalidated from the leaf that is
    /// unlinked, and a symlinked leaf fails closed instead of being followed.
    /// This seam is also exercised directly by the same-authority replacement
    /// regression. Timestamp cleanup is handled by the caller.
    async fn delete_blob_conditional_in(
        shard: &ContainedDir,
        leaf: &FileName,
        expected_version: &BlobObjectVersion,
    ) -> Result<GcDeleteResult, StorageError> {
        let handle = match shard.open_leaf_read(leaf).await {
            Ok(h) => h,
            Err(FsMutateError::NotFound) => return Ok(GcDeleteResult::NotFound),
            Err(err) => return Err(map_fs_mutate_err(err)),
        };

        let current_version = tokio::task::spawn_blocking(move || {
            let mut file = handle.into_file();
            compute_blob_version_from_file(&mut file)
        })
        .await
        .map_err(map_blocking_join_error)??;
        if &current_version != expected_version {
            return Ok(GcDeleteResult::PreconditionFailed {
                current_version: Some(current_version),
            });
        }

        match shard.unlink(leaf, false).await {
            Ok(()) => Ok(GcDeleteResult::Deleted),
            Err(FsMutateError::NotFound) => Ok(GcDeleteResult::NotFound),
            Err(err) => Err(map_fs_mutate_err(err)),
        }
    }

    pub async fn write_quarantine_timestamp(
        &self,
        digest: &Digest,
        timestamp: SystemTime,
    ) -> Result<(), StorageError> {
        // Contained: ensure `quarantine/meta/<algo>/<p2>` beneath the pinned
        // quarantine authority (was: ambient create_dir_all + write_atomic_file).
        let meta = self
            .quarantine_meta_shard(digest, true)
            .await?
            .expect("ensure-mode shard resolution always yields an authority");
        let leaf = Self::quarantine_ts_leaf(digest)?;
        let secs = timestamp
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        meta.write_leaf_atomic(&leaf, format!("{secs}\n").into_bytes(), true)
            .await
            .map_err(map_fs_mutate_err)?;
        Ok(())
    }

    pub async fn read_quarantine_timestamp(
        &self,
        digest: &Digest,
    ) -> Result<Option<SystemTime>, StorageError> {
        upload_quarantine_read::read_quarantine_timestamp_impl(self.reader.as_ref(), digest).await
    }

    pub async fn remove_quarantine_timestamp(&self, digest: &Digest) -> Result<(), StorageError> {
        // Best-effort (result fully ignored, as before: the ambient path did
        // `let _ = remove_file(..)` and always returned Ok). Resolution is
        // contained and non-creating; on any resolution failure (including a
        // fail-closed symlink rejection) nothing is removed and Ok is returned.
        if let Ok(Some(meta)) = self.quarantine_meta_shard(digest, false).await
            && let Ok(leaf) = Self::quarantine_ts_leaf(digest)
        {
            let _ = meta.unlink(&leaf, true).await;
        }
        Ok(())
    }
}

/// Reference implementation of the conditional-delete version token from an
/// ambient path (metadata + streaming SHA-256). Retained ONLY for byte-identity
/// equivalence tests against the contained implementations; production paths
/// compute versions through contained authorities.
#[cfg(test)]
pub(crate) async fn compute_fs_blob_version(
    path: &Path,
) -> Result<BlobObjectVersion, StorageError> {
    let meta = tokio::fs::metadata(path).await.map_err(map_fs_io_err)?;
    let len = meta.len();
    let mtime = meta
        .modified()
        .map(|t| {
            t.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        })
        .unwrap_or(0);

    let mut file = tokio::fs::File::open(path).await.map_err(map_fs_io_err)?;
    let mut hasher = sha2::Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).await.map_err(map_fs_io_err)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let hash = hex::encode(hasher.finalize());
    Ok(BlobObjectVersion(format!("fs:{len}:{mtime}:{hash}")))
}

/// Compute the conditional-delete version token from an ALREADY-OPENED
/// contained leaf descriptor: `fstat` (length + mtime) and a streaming
/// SHA-256 read on the SAME fd, so revalidation refers to exactly the inode
/// the retained authority will unlink. Token bytes are identical to the
/// contained read seam and the legacy ambient helper:
/// `fs:{len}:{mtime_nanos}:{sha256hex}` (pre-epoch or unavailable mtime maps
/// to 0). Blocking I/O — call from `spawn_blocking`.
fn compute_blob_version_from_file(
    file: &mut std::fs::File,
) -> Result<BlobObjectVersion, StorageError> {
    use std::io::Read as _;
    let meta = file.metadata().map_err(map_fs_io_err)?;
    let len = meta.len();
    let mtime = meta
        .modified()
        .map(|t| {
            t.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        })
        .unwrap_or(0);
    let mut hasher = sha2::Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).map_err(map_fs_io_err)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let hash = hex::encode(hasher.finalize());
    Ok(BlobObjectVersion(format!("fs:{len}:{mtime}:{hash}")))
}

#[async_trait]
impl GcStorage for FsStorage {
    async fn list_cas_blobs_page(
        &self,
        cursor: Option<&GcCursor>,
        limit: usize,
    ) -> Result<GcBlobPage, StorageError> {
        listing::list_cas_blobs_page_impl(
            self.reader.as_ref(),
            cursor,
            limit,
            listing::FsListingBudgets::default(),
        )
        .await
    }

    async fn quarantine_blob(
        &self,
        permit: &crate::storage::mutation_authority::GcMutationPermit<'_>,
        digest: &Digest,
        _version: &BlobObjectVersion,
    ) -> Result<GcQuarantineResult, StorageError> {
        if !permit.is_valid() {
            return Err(StorageError::permission_denied(
                "invalid or inactive GC mutation permit",
            ));
        }

        // Contained CAS shard (non-creating: an absent shard means an absent
        // blob -> Skipped, exactly the prior ambient NotFound contract).
        let Some(src) = self.cas_blobs_shard(digest, false).await? else {
            return Ok(GcQuarantineResult::Skipped);
        };
        let leaf = Self::blob_leaf(digest)?;
        let size = match src.inspect(&leaf).await {
            Ok(Some(identity)) => identity.size,
            Ok(None) => return Ok(GcQuarantineResult::Skipped),
            Err(err) => return Err(map_fs_mutate_err(err)),
        };

        // Contained quarantine destination shard (created if missing, as the
        // prior create_dir_all did).
        let dest = self
            .quarantine_blobs_shard(digest, true)
            .await?
            .expect("ensure-mode shard resolution always yields an authority");
        match dest.inspect(&leaf).await {
            Ok(Some(_)) => return Ok(GcQuarantineResult::Skipped),
            Ok(None) => {}
            Err(err) => return Err(map_fs_mutate_err(err)),
        }

        // Contained cross-authority rename (renameat between the two pinned
        // shard fds; no ambient path reconstruction).
        match src.rename_leaf(&leaf, &dest, &leaf).await {
            Ok(()) => {
                let now = SystemTime::now();
                self.write_quarantine_timestamp(digest, now).await?;
                Ok(GcQuarantineResult::Quarantined { size })
            }
            Err(FsMutateError::NotFound) => Ok(GcQuarantineResult::Skipped),
            Err(err) => Err(map_fs_mutate_err(err)),
        }
    }

    async fn restore_quarantined_blob(
        &self,
        permit: &crate::storage::mutation_authority::GcMutationPermit<'_>,
        digest: &Digest,
    ) -> Result<Option<u64>, StorageError> {
        if !permit.is_valid() {
            return Err(StorageError::permission_denied(
                "invalid or inactive GC mutation permit",
            ));
        }

        let Some(src) = self.quarantine_blobs_shard(digest, false).await? else {
            return Ok(None);
        };
        let leaf = Self::blob_leaf(digest)?;
        let size = match src.inspect(&leaf).await {
            Ok(Some(identity)) => identity.size,
            Ok(None) => return Ok(None),
            Err(err) => return Err(map_fs_mutate_err(err)),
        };

        let dest = self
            .cas_blobs_shard(digest, true)
            .await?
            .expect("ensure-mode shard resolution always yields an authority");
        match dest.inspect(&leaf).await {
            Ok(Some(_)) => {
                // CAS copy already present: drop the quarantined duplicate and
                // its timestamp best-effort, as before.
                let _ = src.unlink(&leaf, true).await;
                let _ = self.remove_quarantine_timestamp(digest).await;
                return Ok(Some(size));
            }
            Ok(None) => {}
            Err(err) => return Err(map_fs_mutate_err(err)),
        }

        match src.rename_leaf(&leaf, &dest, &leaf).await {
            Ok(()) => {
                let _ = self.remove_quarantine_timestamp(digest).await;
                Ok(Some(size))
            }
            Err(FsMutateError::NotFound) => Ok(None),
            Err(err) => Err(map_fs_mutate_err(err)),
        }
    }

    async fn quarantined_blob_version(
        &self,
        digest: &Digest,
    ) -> Result<Option<BlobObjectVersion>, StorageError> {
        upload_quarantine_read::quarantined_blob_version_impl(self.reader.as_ref(), digest).await
    }

    async fn delete_blob_conditional(
        &self,
        permit: &crate::storage::mutation_authority::GcMutationPermit<'_>,
        digest: &Digest,
        version: Option<&BlobObjectVersion>,
    ) -> Result<GcDeleteResult, StorageError> {
        if !permit.is_valid() {
            return Err(StorageError::permission_denied(
                "invalid or inactive GC mutation permit",
            ));
        }

        let Some(expected_version) = version else {
            return Err(StorageError::conflict(
                "conditional delete on filesystem storage requires expected version",
            ));
        };

        // ONE retained contained quarantine shard authority across
        // revalidation and unlink (see `delete_blob_conditional_in`).
        let Some(shard) = self.quarantine_blobs_shard(digest, false).await? else {
            let _ = self.remove_quarantine_timestamp(digest).await;
            return Ok(GcDeleteResult::NotFound);
        };
        let leaf = Self::blob_leaf(digest)?;
        let result = Self::delete_blob_conditional_in(&shard, &leaf, expected_version).await?;
        match &result {
            GcDeleteResult::Deleted | GcDeleteResult::NotFound => {
                let _ = self.remove_quarantine_timestamp(digest).await;
            }
            GcDeleteResult::PreconditionFailed { .. } => {}
        }
        Ok(result)
    }

    fn gc_strategy(&self) -> GcStorageStrategy {
        GcStorageStrategy::FilesystemQuarantine
    }

    async fn discover_manifest_references(
        &self,
    ) -> Result<Option<std::collections::HashSet<Digest>>, StorageError> {
        let obs = manifest_refs::collect_manifest_references_end_to_end(
            self.reader.as_ref(),
            self.gc_discovery_limits,
            self.gc_ref_limits.clone(),
        )
        .await?;
        Ok(Some(obs.protected_digests))
    }
}

#[cfg(test)]
#[path = "fs/tests.rs"]
mod tests;

#[cfg(all(target_os = "linux", test))]
#[path = "fs/contained_metadata.rs"]
mod contained_metadata;

#[path = "fs/read_adapter.rs"]
pub(crate) mod read_adapter;

#[cfg(test)]
#[path = "fs/metadata_seam.rs"]
mod metadata_seam;

#[cfg(test)]
#[path = "fs/payload_seam.rs"]
mod payload_seam;

#[path = "fs/listing.rs"]
pub(crate) mod listing;

#[path = "fs/manifest.rs"]
pub(crate) mod manifest;

#[path = "fs/manifest_listing.rs"]
pub(crate) mod manifest_listing;

#[path = "fs/repo_discovery.rs"]
pub(crate) mod repo_discovery;

#[path = "fs/manifest_refs.rs"]
pub(crate) mod manifest_refs;

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use manifest_refs as manifest_refs_seam;

#[path = "fs/tag_read.rs"]
pub(crate) mod tag_read;

#[path = "fs/tag_listing.rs"]
pub(crate) mod tag_listing;

#[path = "fs/referrers_read.rs"]
pub(crate) mod referrers_read;

#[path = "fs/catalog_discovery.rs"]
pub(crate) mod catalog_discovery;

#[path = "fs/timestamps_emptiness.rs"]
pub(crate) mod timestamps_emptiness;

#[path = "fs/membership_read.rs"]
pub(crate) mod membership_read;

#[path = "fs/journal_read.rs"]
pub(crate) mod journal_read;

#[path = "fs/upload_quarantine_read.rs"]
pub(crate) mod upload_quarantine_read;
