use super::upload_session::*;
use super::{
    BlobMeta, BlobObjectVersion, GcBlobCandidate, GcBlobPage, GcCursor, GcDeleteResult,
    GcQuarantineResult, GcStorage, GcStorageStrategy, ManifestMeta, ReferrerDescriptor,
    RepoTimestamps, Storage, StorageError, ensure_dir,
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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

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

#[derive(Debug)]
pub struct FsStorage {
    root: PathBuf,
    max_upload_bytes: u64,
    upload_hashes: Vec<Mutex<std::collections::HashMap<String, SerializableSha256>>>,
    referrer_locks: Vec<Mutex<()>>,
    repo_locks: std::sync::Mutex<std::collections::HashMap<String, std::fs::File>>,
}

impl FsStorage {
    pub fn try_new(root: PathBuf, max_upload_bytes: u64) -> Result<Self, StorageError> {
        ensure_dir(&root)?;
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
        })
    }

    pub fn new(root: PathBuf, max_upload_bytes: u64) -> Self {
        Self::try_new(root, max_upload_bytes).unwrap_or_else(|err| {
            panic!("failed to initialize FsStorage: {err}");
        })
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

    fn upload_hash_path(&self, uuid: &str) -> PathBuf {
        self.uploads_dir().join(format!("{uuid}.sha256state"))
    }

    async fn load_upload_hash_state_from_disk(
        &self,
        uuid: &str,
        expected_len: u64,
    ) -> Option<SerializableSha256> {
        let path = self.upload_hash_path(uuid);
        let bytes = tokio::fs::read(&path).await.ok()?;
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
        let path = self.upload_hash_path(uuid);
        let bytes = st.to_bytes();
        atomic_write_file(&path, &bytes).await
    }

    async fn rebuild_upload_hash_state_from_data_file(
        &self,
        uuid: &str,
        observed_len: u64,
    ) -> Option<SerializableSha256> {
        let path = self.upload_path(uuid);
        let mut file = tokio::fs::File::open(&path).await.ok()?;

        let t = Instant::now();
        let mut hasher = SerializableSha256::new();
        let mut buf = vec![0u8; 1024 * 64];
        loop {
            let n = file.read(&mut buf).await.ok()?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }

        let elapsed = t.elapsed();
        let file_len_now = tokio::fs::metadata(&path).await.ok()?.len();
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

    fn blob_path(&self, digest: &Digest) -> PathBuf {
        // data/blobs/<algo>/ab/<hex>
        self.root
            .join("blobs")
            .join(digest.algorithm())
            .join(digest.prefix2())
            .join(digest.hex())
    }

    fn quarantine_blob_path(&self, digest: &Digest) -> PathBuf {
        // data/quarantine/blobs/<algo>/ab/<hex>
        self.root
            .join("quarantine")
            .join("blobs")
            .join(digest.algorithm())
            .join(digest.prefix2())
            .join(digest.hex())
    }

    fn manifest_path(&self, name: &str, digest: &Digest) -> PathBuf {
        // data/repos/<name>/manifests/<hex>
        self.root
            .join("repos")
            .join(name)
            .join("manifests")
            .join(digest.hex())
    }

    fn tag_path(&self, name: &str, tag: &str) -> PathBuf {
        // data/repos/<name>/tags/<tag>
        self.root.join("repos").join(name).join("tags").join(tag)
    }

    fn repo_blobs_dir(&self, repo: &CanonicalRepoName, algorithm: &str) -> PathBuf {
        self.root
            .join("repo-memberships")
            .join("by-repo")
            .join(crate::storage::repo_membership::encode_canonical_repo_key(
                repo,
            ))
            .join(algorithm)
    }

    fn repo_blob_path(&self, repo: &CanonicalRepoName, digest: &Digest) -> PathBuf {
        self.root
            .join(crate::storage::repo_membership::canonical_repo_membership_relpath(repo, digest))
    }

    fn referrers_path(&self, name: &str, subject: &Digest) -> PathBuf {
        // data/repos/<name>/referrers/<hex>.json
        self.root
            .join("repos")
            .join(name)
            .join("referrers")
            .join(format!("{}.json", subject.hex()))
    }

    fn uploads_dir(&self) -> PathBuf {
        self.root.join("uploads")
    }

    fn upload_path(&self, uuid: &str) -> PathBuf {
        self.uploads_dir().join(format!("{uuid}.data"))
    }

    fn session_data_path(&self, uuid: &str) -> PathBuf {
        self.uploads_dir().join(format!("{uuid}.data"))
    }

    fn session_meta_path(&self, uuid: &str) -> PathBuf {
        self.uploads_dir().join(format!("{uuid}.meta.json"))
    }

    fn session_hash_path(&self, uuid: &str, generation: u64) -> PathBuf {
        self.uploads_dir().join(format!("{uuid}.hash.{generation}"))
    }

    fn session_lock_path(&self, uuid: &str) -> PathBuf {
        self.uploads_dir().join(format!(".lock.{uuid}"))
    }

    fn finalized_dir(&self) -> PathBuf {
        self.uploads_dir().join(".finalized")
    }

    fn finalized_receipt_path(&self, uuid: &str) -> PathBuf {
        self.finalized_dir().join(format!("{uuid}.json"))
    }

    async fn detect_manifest_media_type(&self, bytes: &[u8]) -> Result<String, StorageError> {
        let value: serde_json::Value = serde_json::from_slice(bytes)
            .map_err(|err| StorageError::corrupt_data(err.to_string()))?;
        let media_type = value
            .get("mediaType")
            .and_then(|v| v.as_str())
            .unwrap_or("application/vnd.oci.image.manifest.v1+json");
        Ok(media_type.to_string())
    }

    async fn list_tag_files(&self, name: &str) -> Result<Vec<PathBuf>, StorageError> {
        let tags_dir = self.root.join("repos").join(name).join("tags");
        let mut dir = match tokio::fs::read_dir(&tags_dir).await {
            Ok(d) => d,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(StorageError::io(err.to_string())),
        };

        let mut files = Vec::new();
        loop {
            match dir.next_entry().await {
                Ok(Some(entry)) => {
                    if let Some(file_name) = entry.file_name().to_str() {
                        if !file_name.starts_with('.') {
                            files.push(entry.path());
                        }
                    }
                }
                Ok(None) => break,
                Err(err) => return Err(StorageError::io(err.to_string())),
            }
        }
        Ok(files)
    }

    async fn list_repo_names(&self) -> Result<Vec<String>, StorageError> {
        let repos_root = self.root.join("repos");
        let mut repos = Vec::new();

        let mut stack: Vec<(PathBuf, String)> = vec![(repos_root.clone(), String::new())];
        while let Some((dir_path, rel)) = stack.pop() {
            let mut dir = match tokio::fs::read_dir(&dir_path).await {
                Ok(d) => d,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(StorageError::io(err.to_string())),
            };

            while let Ok(Some(entry)) = dir.next_entry().await {
                let file_type = match entry.file_type().await {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                if !file_type.is_dir() {
                    continue;
                }

                let name = match entry.file_name().to_str() {
                    Some(s) => s.to_string(),
                    None => continue,
                };

                // Do not descend into internal leaf dirs.
                if name == "tags"
                    || name == "manifests"
                    || name == "referrers"
                    || name == "blobs"
                    || name == "meta"
                {
                    continue;
                }

                let child_path = entry.path();
                let child_rel = if rel.is_empty() {
                    name
                } else {
                    format!("{rel}/{name}")
                };

                // Consider this a repo if it has tags/, manifests/, blobs/, or meta/ directories.
                let tags_dir = child_path.join("tags");
                let manifests_dir = child_path.join("manifests");
                let blobs_dir = child_path.join("blobs");
                let meta_dir = child_path.join("meta");
                let has_tags = tokio::fs::metadata(&tags_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                let has_manifests = tokio::fs::metadata(&manifests_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                let has_blobs = tokio::fs::metadata(&blobs_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                let has_meta = tokio::fs::metadata(&meta_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                if has_tags || has_manifests || has_blobs || has_meta {
                    repos.push(child_rel.clone());
                }

                stack.push((child_path, child_rel));
            }
        }

        repos.sort();
        repos.dedup();
        Ok(repos)
    }

    async fn max_mtime_in_dir(&self, dir: &PathBuf) -> Result<Option<SystemTime>, StorageError> {
        let mut rd = match tokio::fs::read_dir(dir).await {
            Ok(d) => d,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(StorageError::io(err.to_string())),
        };

        let mut max_time: Option<SystemTime> = None;
        while let Ok(Some(entry)) = rd.next_entry().await {
            let meta = match entry.metadata().await {
                Ok(m) => m,
                Err(_) => continue,
            };
            if !meta.is_file() {
                continue;
            }
            let modified = match meta.modified() {
                Ok(t) => t,
                Err(_) => continue,
            };
            max_time = Some(match max_time {
                Some(cur) if cur >= modified => cur,
                _ => modified,
            });
        }
        Ok(max_time)
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

async fn fs_dir_has_any_entry(path: &Path) -> Result<bool, StorageError> {
    match tokio::fs::read_dir(path).await {
        Ok(mut read_dir) => {
            while let Some(entry) = read_dir
                .next_entry()
                .await
                .map_err(|e| StorageError::io(e.to_string()))?
            {
                let file_type = entry
                    .file_type()
                    .await
                    .map_err(|e| StorageError::io(e.to_string()))?;
                if file_type.is_dir() {
                    if Box::pin(fs_dir_has_any_entry(&entry.path())).await? {
                        return Ok(true);
                    }
                } else {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(StorageError::io(err.to_string())),
    }
}

/// Low-level filesystem metadata inquiry returning object byte size.
///
/// Retains the original [`std::io::Error`] on failure so its [`std::io::ErrorKind`]
/// remains available for typed inspection before outward conversion.
async fn fs_metadata_size(path: &Path) -> Result<u64, std::io::Error> {
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
        let repo_dir = self.root.join("repos").join(name);
        match tokio::fs::metadata(&repo_dir).await {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(err) => return Err(StorageError::io(err.to_string())),
        }

        let tags_dir = repo_dir.join("tags");
        let manifests_dir = repo_dir.join("manifests");

        let last_tag_update = self.max_mtime_in_dir(&tags_dir).await?;
        let last_manifest_update = self.max_mtime_in_dir(&manifests_dir).await?;

        Ok(RepoTimestamps {
            last_tag_update,
            last_manifest_update,
        })
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
            let p = self.root.join(sub);
            if fs_dir_has_any_entry(&p).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError> {
        let path = self.blob_path(digest);
        match fs_metadata_size(&path).await {
            Ok(size) => Ok(BlobMeta { size }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let qpath = self.quarantine_blob_path(digest);
                match fs_metadata_size(&qpath).await {
                    Ok(size) => Ok(BlobMeta { size }),
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        Err(StorageError::NotFound)
                    }
                    Err(err) => Err(StorageError::io(err.to_string())),
                }
            }
            Err(err) => Err(StorageError::io(err.to_string())),
        }
    }

    async fn open_blob(
        &self,
        digest: &Digest,
    ) -> Result<(BlobMeta, std::pin::Pin<Box<dyn AsyncRead + Send>>), StorageError> {
        let path = self.blob_path(digest);
        let file = match tokio::fs::File::open(&path).await {
            Ok(f) => f,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let qpath = self.quarantine_blob_path(digest);
                match tokio::fs::File::open(&qpath).await {
                    Ok(f) => f,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        return Err(StorageError::NotFound);
                    }
                    Err(err) => return Err(StorageError::io(err.to_string())),
                }
            }
            Err(err) => return Err(StorageError::io(err.to_string())),
        };
        let meta = match file.metadata().await {
            Ok(m) => m,
            Err(err) => return Err(StorageError::io(err.to_string())),
        };
        Ok((BlobMeta { size: meta.len() }, Box::pin(file)))
    }

    async fn resolve_tag(&self, name: &str, tag: &str) -> Result<Digest, StorageError> {
        let path = self.tag_path(name, tag);
        let content = match tokio::fs::read_to_string(&path).await {
            Ok(s) => s,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(err) => return Err(StorageError::io(err.to_string())),
        };
        let reference = content.trim();
        Digest::parse(reference).map_err(|_| StorageError::NotFound)
    }

    async fn list_tags(&self, name: &str) -> Result<Vec<String>, StorageError> {
        let repo_dir = self.root.join("repos").join(name);
        match tokio::fs::metadata(&repo_dir).await {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(err) => return Err(StorageError::io(err.to_string())),
        }

        let tags_dir = repo_dir.join("tags");
        let mut dir = match tokio::fs::read_dir(&tags_dir).await {
            Ok(d) => d,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(StorageError::io(err.to_string())),
        };

        let mut tags = Vec::new();
        loop {
            match dir.next_entry().await {
                Ok(Some(entry)) => {
                    if let Some(file_name) = entry.file_name().to_str() {
                        if !file_name.starts_with('.') {
                            tags.push(file_name.to_string());
                        }
                    }
                }
                Ok(None) => break,
                Err(err) => return Err(StorageError::io(err.to_string())),
            }
        }

        tags.sort();
        Ok(tags)
    }

    async fn head_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<ManifestMeta, StorageError> {
        let path = self.manifest_path(name, digest);
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(err) => return Err(StorageError::io(err.to_string())),
        };
        let media_type = self.detect_manifest_media_type(&bytes).await?;
        Ok(ManifestMeta {
            size: bytes.len() as u64,
            media_type,
        })
    }

    async fn get_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<(ManifestMeta, bytes::Bytes), StorageError> {
        let path = self.manifest_path(name, digest);
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(err) => return Err(StorageError::io(err.to_string())),
        };
        let media_type = self.detect_manifest_media_type(&bytes).await?;
        let meta = ManifestMeta {
            size: bytes.len() as u64,
            media_type,
        };
        Ok((meta, bytes::Bytes::from(bytes)))
    }

    async fn put_manifest(
        &self,
        name: &str,
        digest: &Digest,
        bytes: Bytes,
    ) -> Result<ManifestMeta, StorageError> {
        let dir = self.root.join("repos").join(name).join("manifests");
        ensure_dir(&dir)?;

        let media_type = self.detect_manifest_media_type(&bytes).await?;
        let path = dir.join(digest.hex());
        atomic_write_file(&path, &bytes).await?;

        Ok(ManifestMeta {
            size: bytes.len() as u64,
            media_type,
        })
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
        let dir = self.root.join("repos").join(name).join("tags");
        ensure_dir(&dir)?;
        let path = dir.join(tag);
        let lock_path = dir.join(format!(".lock.{tag}"));
        let body = format!("{}\n", digest.as_str());
        let tag_name = tag.to_string();
        let digest_clone = digest.clone();

        tokio::task::spawn_blocking(move || {
            use fs2::FileExt;
            use std::io::Write;

            let lock_file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path)
                .map_err(map_fs_io_err)?;

            lock_file.lock_exclusive().map_err(map_fs_io_err)?;

            let res: Result<super::TagMutation, StorageError> = (|| {
                let existing_d = match std::fs::read(&path) {
                    Ok(existing_bytes) => {
                        let s = String::from_utf8_lossy(&existing_bytes);
                        Digest::parse(s.trim()).ok()
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
                    Err(err) => return Err(map_fs_io_err(err)),
                };

                if let Some(ref prev) = existing_d {
                    if *prev == digest_clone {
                        return Ok(super::TagMutation::Unchanged);
                    }
                }

                match policy {
                    super::TagMutationPolicy::CreateOnly => {
                        if existing_d.is_some() {
                            return Err(StorageError::TagAlreadyExists);
                        }

                        let tmp_name = format!(".tmp.{tag_name}.{}", uuid::Uuid::new_v4());
                        let tmp_path = dir.join(tmp_name);

                        let mut file = std::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(&tmp_path)
                            .map_err(map_fs_io_err)?;
                        file.write_all(body.as_bytes()).map_err(map_fs_io_err)?;
                        file.flush().map_err(map_fs_io_err)?;
                        file.sync_all().map_err(map_fs_io_err)?;
                        drop(file);

                        if let Err(e) = std::fs::rename(&tmp_path, &path) {
                            let _ = std::fs::remove_file(&tmp_path);
                            return Err(map_fs_io_err(e));
                        }

                        if let Ok(dir_file) = std::fs::File::open(&dir) {
                            let _ = dir_file.sync_all();
                        }

                        Ok(super::TagMutation::Created)
                    }
                    super::TagMutationPolicy::Replace => {
                        let tmp_name = format!(".tmp.{tag_name}.{}", uuid::Uuid::new_v4());
                        let tmp_path = dir.join(tmp_name);

                        let mut file = std::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(&tmp_path)
                            .map_err(map_fs_io_err)?;
                        file.write_all(body.as_bytes()).map_err(map_fs_io_err)?;
                        file.flush().map_err(map_fs_io_err)?;
                        file.sync_all().map_err(map_fs_io_err)?;
                        drop(file);

                        if let Err(e) = std::fs::rename(&tmp_path, &path) {
                            let _ = std::fs::remove_file(&tmp_path);
                            return Err(map_fs_io_err(e));
                        }

                        if let Ok(dir_file) = std::fs::File::open(&dir) {
                            let _ = dir_file.sync_all();
                        }

                        match existing_d {
                            Some(prev) => Ok(super::TagMutation::Replaced { previous: prev }),
                            None => Ok(super::TagMutation::Created),
                        }
                    }
                }
            })();

            let _ = lock_file.unlock();
            res
        })
        .await
        .map_err(map_blocking_join_error)?
    }

    async fn delete_tag(&self, name: &str, tag: &str) -> Result<(), StorageError> {
        let tag_file = self.tag_path(name, tag);
        match tokio::fs::remove_file(&tag_file).await {
            Ok(_) => {
                let tags_dir = self.root.join("repos").join(name).join("tags");
                let _ = fsync_dir(tags_dir.as_path()).await;
                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(StorageError::NotFound),
            Err(err) => Err(StorageError::io(err.to_string())),
        }
    }

    async fn list_manifest_digests_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<Digest>, Option<String>), StorageError> {
        let manifests_dir = self.root.join("repos").join(repo).join("manifests");
        if !manifests_dir.exists() {
            return Ok((Vec::new(), None));
        }

        let mut all_digests: Vec<Digest> = Vec::new();
        if let Ok(mut entries) = tokio::fs::read_dir(&manifests_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let file_name = entry.file_name().to_string_lossy().to_string();
                if file_name.starts_with(".tmp.") || file_name.starts_with(".lock.") {
                    continue;
                }
                if let Ok(d) = Digest::parse(&format!("sha256:{file_name}")) {
                    all_digests.push(d);
                } else if let Ok(d) = Digest::parse(&file_name) {
                    all_digests.push(d);
                }
            }
        }
        all_digests.sort_by(|a, b| a.hex().cmp(b.hex()));

        let start_idx = if let Some(token) = continuation_token {
            match all_digests.binary_search_by(|d| d.as_str().as_str().cmp(token)) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            }
        } else {
            0
        };

        let end_idx = (start_idx + page_limit).min(all_digests.len());
        let page_slice = &all_digests[start_idx..end_idx];

        let next_token = if end_idx < all_digests.len() {
            page_slice.last().map(|d| d.as_str().to_string())
        } else {
            None
        };

        Ok((page_slice.to_vec(), next_token))
    }

    async fn list_tags_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<(String, Digest)>, Option<String>), StorageError> {
        let tag_files = self.list_tag_files(repo).await?;
        let tags_dir = self.root.join("repos").join(repo).join("tags");

        let mut tags_with_digest: Vec<(String, Digest)> = Vec::new();
        for path in tag_files {
            let rel = match path.strip_prefix(&tags_dir) {
                Ok(r) => r.to_string_lossy().to_string(),
                Err(_) => continue,
            };
            if rel.starts_with(".tmp.") || rel.starts_with(".lock.") {
                continue;
            }
            if let Ok(content) = tokio::fs::read_to_string(&path).await
                && let Ok(digest) = Digest::parse(content.trim())
            {
                tags_with_digest.push((rel, digest));
            }
        }
        tags_with_digest.sort_by(|a, b| a.0.cmp(&b.0));

        let start_idx = if let Some(token) = continuation_token {
            match tags_with_digest.binary_search_by(|(t, _)| t.as_str().cmp(token)) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            }
        } else {
            0
        };

        let end_idx = (start_idx + page_limit).min(tags_with_digest.len());
        let page_slice = &tags_with_digest[start_idx..end_idx];

        let next_token = if end_idx < tags_with_digest.len() {
            page_slice.last().map(|(t, _)| t.clone())
        } else {
            None
        };

        Ok((page_slice.to_vec(), next_token))
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
        let tag_path = self.tag_path(repo, tag);
        match tokio::fs::read(&tag_path).await {
            Ok(bytes) => {
                let s = String::from_utf8_lossy(&bytes);
                let digest = Digest::parse(s.trim())
                    .map_err(|e| StorageError::corrupt_data(format!("corrupt tag {tag}: {e}")))?;
                let mut hasher = sha2::Sha256::new();
                hasher.update(&bytes);
                let version = hex::encode(hasher.finalize());
                Ok(Some((digest, version)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::io(e.to_string())),
        }
    }

    async fn delete_tag_conditional(
        &self,
        repo: &str,
        tag: &str,
        expected_version: Option<&str>,
    ) -> Result<super::ConditionalDeleteResult, StorageError> {
        let dir = self.root.join("repos").join(repo).join("tags");
        let path = self.tag_path(repo, tag);
        let lock_path = dir.join(format!(".lock.{tag}"));
        let exp_v = expected_version.map(|s| s.to_string());

        tokio::task::spawn_blocking(move || {
            use fs2::FileExt;
            if let Some(parent) = lock_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let lock_file = match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path)
            {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(super::ConditionalDeleteResult::NotFound);
                }
                Err(e) => return Err(map_fs_io_err(e)),
            };

            lock_file.lock_exclusive().map_err(map_fs_io_err)?;

            let res = (|| {
                let bytes = match std::fs::read(&path) {
                    Ok(b) => b,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Ok(super::ConditionalDeleteResult::NotFound);
                    }
                    Err(e) => return Err(map_fs_io_err(e)),
                };

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

                std::fs::remove_file(&path).map_err(map_fs_io_err)?;
                if let Ok(dir_file) = std::fs::File::open(&dir) {
                    let _ = dir_file.sync_all();
                }
                Ok(super::ConditionalDeleteResult::Deleted)
            })();

            let _ = lock_file.unlock();
            res
        })
        .await
        .map_err(map_blocking_join_error)?
    }

    async fn read_lifecycle_journal(&self, repo: &str) -> Result<Option<Bytes>, StorageError> {
        let canonical = crate::registry::canonical_name::CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let path = fs_repo_dir(&self.root, &canonical)?
            .join("meta")
            .join("lifecycle_journal.json");
        match tokio::fs::read(&path).await {
            Ok(b) => Ok(Some(Bytes::from(b))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::io(e.to_string())),
        }
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
        let dir = self.uploads_dir();
        ensure_dir(&dir)?;

        let uuid = uuid::Uuid::new_v4().to_string();
        let path = self.upload_path(&uuid);

        tokio::fs::File::create(&path)
            .await
            .map_err(map_fs_io_err)?;

        // Track + persist hash state from the beginning so resumes after restart are cheap.
        let st = SerializableSha256::new();
        let _ = self.persist_upload_hash_state(&uuid, &st).await;
        let shard = self.upload_hash_shard(&uuid);
        let mut map = shard.lock().await;
        map.insert(uuid.clone(), st);

        Ok(super::UploadMeta { uuid, offset: 0 })
    }

    async fn upload_status(&self, uuid: &str) -> Result<super::UploadMeta, StorageError> {
        let path = self.upload_path(uuid);
        let meta = match tokio::fs::metadata(&path).await {
            Ok(m) => m,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(err) => return Err(StorageError::io(err.to_string())),
        };
        Ok(super::UploadMeta {
            uuid: uuid.to_string(),
            offset: meta.len(),
        })
    }

    async fn append_upload(
        &self,
        uuid: &str,
        chunk: Bytes,
    ) -> Result<super::UploadMeta, StorageError> {
        let t_total = Instant::now();
        let path = self.upload_path(uuid);

        let current_len = match tokio::fs::metadata(&path).await {
            Ok(m) => m.len(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(err) => return Err(StorageError::io(err.to_string())),
        };

        let next_len = current_len.saturating_add(chunk.len() as u64);
        if next_len > self.max_upload_bytes {
            return Err(StorageError::TooLarge);
        }

        let mut file = match tokio::fs::OpenOptions::new().append(true).open(&path).await {
            Ok(f) => f,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(err) => return Err(StorageError::io(err.to_string())),
        };
        file.write_all(&chunk).await.map_err(map_fs_io_err)?;
        file.flush().await.map_err(map_fs_io_err)?;

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

        let meta = file
            .metadata()
            .await
            .map_err(|err| StorageError::io(err.to_string()))?;
        Ok(super::UploadMeta {
            uuid: uuid.to_string(),
            offset: meta.len(),
        })
    }

    async fn finalize_upload(&self, uuid: &str, digest: &Digest) -> Result<BlobMeta, StorageError> {
        let upload_path = self.upload_path(uuid);
        let upload_hash_path = self.upload_hash_path(uuid);

        let t_total = Instant::now();
        let mut file = match tokio::fs::File::open(&upload_path).await {
            Ok(f) => f,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(err) => return Err(StorageError::io(err.to_string())),
        };

        let upload_size_bytes = file
            .metadata()
            .await
            .map_err(|err| StorageError::io(err.to_string()))?
            .len();

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

        let (computed_hex, hash_elapsed) = if digest.algorithm() == "sha512" {
            let mut hasher = sha2::Sha512::new();
            let mut buf = vec![0u8; 1024 * 64];
            loop {
                let n = file
                    .read(&mut buf)
                    .await
                    .map_err(|err| StorageError::io(err.to_string()))?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            (hex::encode(hasher.finalize()), t_hash.elapsed())
        } else if let Some(st) = state {
            if st.total_len == upload_size_bytes {
                hash_source = "saved_state";
                (st.finalize_hex(), t_hash.elapsed())
            } else {
                // Unexpected mismatch; fall back.
                let mut hasher = sha2::Sha256::new();
                let mut buf = vec![0u8; 1024 * 64];
                loop {
                    let n = file
                        .read(&mut buf)
                        .await
                        .map_err(|err| StorageError::io(err.to_string()))?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                }
                (hex::encode(hasher.finalize()), t_hash.elapsed())
            }
        } else {
            // No usable state: hash the file now.
            let mut hasher = sha2::Sha256::new();
            let mut buf = vec![0u8; 1024 * 64];
            loop {
                let n = file
                    .read(&mut buf)
                    .await
                    .map_err(|err| StorageError::io(err.to_string()))?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            (hex::encode(hasher.finalize()), t_hash.elapsed())
        };

        if computed_hex != digest.hex() {
            return Err(StorageError::DigestMismatch);
        }

        // Ensure the uploaded data is durable before we make it visible in the blob store.
        let t_sync = Instant::now();
        file.sync_all().await.map_err(map_fs_io_err)?;
        let sync_elapsed = t_sync.elapsed();
        drop(file);

        // Move into blob store.
        let dest_dir = self
            .root
            .join("blobs")
            .join(digest.algorithm())
            .join(digest.prefix2());
        ensure_dir(&dest_dir)?;

        let dest_path = dest_dir.join(digest.hex());
        let t_rename = Instant::now();
        tokio::fs::rename(&upload_path, &dest_path)
            .await
            .map_err(map_fs_io_err)?;
        let rename_elapsed = t_rename.elapsed();

        // Make the rename durable (both directories are updated by rename).
        let t_fsync = Instant::now();
        fsync_dir(self.uploads_dir().as_path()).await?;
        fsync_dir(dest_dir.as_path()).await?;
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
        let _ = tokio::fs::remove_file(&upload_hash_path).await;

        let meta = tokio::fs::metadata(&dest_path)
            .await
            .map_err(|err| StorageError::io(err.to_string()))?;
        Ok(BlobMeta { size: meta.len() })
    }

    async fn abort_upload(&self, uuid: &str) -> Result<(), StorageError> {
        // Best-effort cleanup: drop in-memory state.
        let shard = self.upload_hash_shard(uuid);
        {
            let mut map = shard.lock().await;
            map.remove(uuid);
        }

        // Best-effort cleanup: remove persisted hash state.
        let hash_path = self.upload_hash_path(uuid);
        let _ = tokio::fs::remove_file(&hash_path).await;

        let path = self.upload_path(uuid);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(StorageError::io(err.to_string())),
        }
    }

    async fn list_referrers(
        &self,
        name: &str,
        subject: &Digest,
    ) -> Result<Vec<ReferrerDescriptor>, StorageError> {
        let path = self.referrers_path(name, subject);
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(StorageError::io(err.to_string())),
        };
        serde_json::from_slice::<Vec<ReferrerDescriptor>>(&bytes)
            .map_err(|err| StorageError::io(err.to_string()))
    }

    async fn add_referrer(
        &self,
        name: &str,
        subject: &Digest,
        descriptor: ReferrerDescriptor,
    ) -> Result<(), StorageError> {
        let _lock = self.referrer_lock_shard(name, subject).lock().await;
        let dir = self.root.join("repos").join(name).join("referrers");
        ensure_dir(&dir)?;

        let path = self.referrers_path(name, subject);
        let mut existing = self.list_referrers(name, subject).await?;
        if !existing.iter().any(|d| d.digest == descriptor.digest) {
            existing.push(descriptor);
        }

        let bytes = serde_json::to_vec(&existing)
            .map_err(|err| StorageError::serialization(err.to_string()))?;
        atomic_write_file(&path, &bytes).await?;
        Ok(())
    }

    async fn remove_referrer(
        &self,
        name: &str,
        subject: &Digest,
        referrer: &Digest,
    ) -> Result<(), StorageError> {
        let _lock = self.referrer_lock_shard(name, subject).lock().await;
        let path = self.referrers_path(name, subject);
        let mut existing = self.list_referrers(name, subject).await?;
        let orig_len = existing.len();
        let referrer_str = referrer.as_str();
        existing.retain(|d| d.digest != referrer_str);
        if existing.len() == orig_len {
            return Ok(());
        }

        if existing.is_empty() {
            let _ = tokio::fs::remove_file(&path).await;
        } else {
            let bytes = serde_json::to_vec(&existing)
                .map_err(|err| StorageError::serialization(err.to_string()))?;
            atomic_write_file(&path, &bytes).await?;
        }
        Ok(())
    }

    async fn delete_manifest(&self, name: &str, digest: &Digest) -> Result<(), StorageError> {
        let manifest_path = self.manifest_path(name, digest);

        // Pre-read manifest bytes to extract subject if present for referrers cleanup.
        let bytes = match tokio::fs::read(&manifest_path).await {
            Ok(b) => b,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(err) => return Err(StorageError::io(err.to_string())),
        };

        let maybe_subject = crate::manifest_refs::extract_subject_digest(&bytes).map_err(|e| {
            StorageError::corrupt_data(format!(
                "cannot delete manifest with malformed structure: {e}"
            ))
        })?;

        match tokio::fs::remove_file(&manifest_path).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(err) => return Err(StorageError::io(err.to_string())),
        }

        // Remove any tags pointing to this digest.
        let digest_str = digest.as_str();
        for path in self.list_tag_files(name).await? {
            let content = match tokio::fs::read_to_string(&path).await {
                Ok(s) => s,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(StorageError::io(err.to_string())),
            };
            if content.trim() == digest_str {
                let _ = tokio::fs::remove_file(&path).await;
            }
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

struct FsSessionLockGuard {
    file: Option<std::fs::File>,
    _path: PathBuf,
}

impl Drop for FsSessionLockGuard {
    fn drop(&mut self) {
        if let Some(f) = self.file.take() {
            let _ = fs2::FileExt::unlock(&f);
            drop(f);
        }
    }
}

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

async fn try_acquire_fs_session_lock(
    lock_path: PathBuf,
) -> Result<Option<FsSessionLockGuard>, StorageError> {
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
        match fs2::FileExt::try_lock_exclusive(&f) {
            Ok(()) => Ok(Some(FsSessionLockGuard {
                file: Some(f),
                _path: lock_path,
            })),
            Err(_) => Ok(None),
        }
    })
    .await
    .map_err(map_blocking_join_error)?
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
        let dir = self.uploads_dir();
        ensure_dir(&dir)?;
        let uuid = uuid::Uuid::new_v4().to_string();
        let session = UploadSessionId::new(canonical_repo.clone(), &uuid);
        let lock_path = self.session_lock_path(&uuid);
        let _lock = acquire_fs_session_lock(lock_path).await?;

        let data_path = self.session_data_path(&uuid);
        tokio::fs::File::create(&data_path)
            .await
            .map_err(map_fs_io_err)?;

        let hash_st = SerializableSha256::new();
        let hash_path = self.session_hash_path(&uuid, 0);
        write_atomic_file(&hash_path, &hash_st.to_bytes()).await?;

        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

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
        let meta_json =
            serde_json::to_vec(&meta).map_err(|e| StorageError::serialization(e.to_string()))?;
        let meta_path = self.session_meta_path(&uuid);
        write_atomic_file(&meta_path, &meta_json).await?;

        Ok(session)
    }

    async fn session_status(
        &self,
        session: &UploadSessionId,
    ) -> Result<UploadSessionStatus, UploadTransitionError> {
        let lock_path = self.session_lock_path(&session.uuid);
        let _lock = acquire_fs_session_lock(lock_path).await?;

        let meta_path = self.session_meta_path(&session.uuid);
        let meta_bytes = match tokio::fs::read(&meta_path).await {
            Ok(b) => b,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                if let Some(receipt) = self
                    .get_finalized_receipt(session)
                    .await
                    .map_err(UploadTransitionError::Storage)?
                {
                    return Ok(UploadSessionStatus {
                        session: session.clone(),
                        state: UploadSessionState::Finalizing,
                        committed_offset: receipt.size,
                        created_at: std::time::UNIX_EPOCH
                            + Duration::from_secs(receipt.finalized_at_unix_secs),
                        last_active_at: std::time::UNIX_EPOCH
                            + Duration::from_secs(receipt.finalized_at_unix_secs),
                    });
                }

                // Check for legacy file migration on first access
                let legacy_path = self.uploads_dir().join(&session.uuid);
                let data_path = self.session_data_path(&session.uuid);

                let legacy_exists = match tokio::fs::metadata(&legacy_path).await {
                    Ok(m) if m.is_file() => true,
                    _ => false,
                };
                let data_exists = match tokio::fs::metadata(&data_path).await {
                    Ok(m) if m.is_file() => true,
                    _ => false,
                };

                if legacy_exists || data_exists {
                    if legacy_exists && !data_exists {
                        let _ = tokio::fs::rename(&legacy_path, &data_path).await;
                    }

                    if let Ok(data_meta) = tokio::fs::metadata(&data_path).await {
                        let size = data_meta.len();
                        let now = SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();

                        // Try loading legacy .sha256state or rebuild
                        let old_sidecar = self.upload_hash_path(&session.uuid);
                        let hash_st = match tokio::fs::read(&old_sidecar).await {
                            Ok(b) => SerializableSha256::from_bytes(&b),
                            Err(_) => None,
                        };
                        let hash_st = match hash_st {
                            Some(st) if st.total_len == size => st,
                            _ => {
                                let mut st = SerializableSha256::new();
                                if let Ok(mut f) = tokio::fs::File::open(&data_path).await {
                                    let mut buf = vec![0u8; 64 * 1024];
                                    let mut read_total = 0u64;
                                    while read_total < size {
                                        let to_read =
                                            (size - read_total).min(buf.len() as u64) as usize;
                                        if let Ok(n) = f.read(&mut buf[..to_read]).await {
                                            if n == 0 {
                                                break;
                                            }
                                            st.update(&buf[..n]);
                                            read_total += n as u64;
                                        } else {
                                            break;
                                        }
                                    }
                                }
                                st
                            }
                        };

                        let hash_path = self.session_hash_path(&session.uuid, 0);
                        let _ = write_atomic_file(&hash_path, &hash_st.to_bytes()).await;
                        let _ = tokio::fs::remove_file(&old_sidecar).await;

                        let migrated_meta = FsSessionMetaRecord {
                            format_version: 1,
                            repo: session.repo.clone(),
                            uuid: session.uuid.clone(),
                            state: UploadSessionState::Active,
                            committed_offset: size,
                            hash_generation: 0,
                            created_at_unix_secs: now,
                            last_active_at_unix_secs: now,
                            finalizing_info: None,
                        };
                        let _ = write_atomic_file(
                            &meta_path,
                            &serde_json::to_vec(&migrated_meta).unwrap(),
                        )
                        .await;

                        return Ok(UploadSessionStatus {
                            session: session.clone(),
                            state: UploadSessionState::Active,
                            committed_offset: size,
                            created_at: std::time::UNIX_EPOCH + Duration::from_secs(now),
                            last_active_at: std::time::UNIX_EPOCH + Duration::from_secs(now),
                        });
                    }
                }

                return Err(UploadTransitionError::NotFound);
            }
            Err(err) => {
                return Err(UploadTransitionError::Storage(StorageError::io(
                    err.to_string(),
                )));
            }
        };
        let meta: FsSessionMetaRecord = serde_json::from_slice(&meta_bytes).map_err(|e| {
            UploadTransitionError::Storage(StorageError::corrupt_data(e.to_string()))
        })?;
        if meta.repo != session.repo || meta.uuid != session.uuid {
            return Err(UploadTransitionError::NotFound);
        }

        Ok(UploadSessionStatus {
            session: session.clone(),
            state: meta.state,
            committed_offset: meta.committed_offset,
            created_at: std::time::UNIX_EPOCH + Duration::from_secs(meta.created_at_unix_secs),
            last_active_at: std::time::UNIX_EPOCH
                + Duration::from_secs(meta.last_active_at_unix_secs),
        })
    }

    async fn append_if_offset(
        &self,
        session: &UploadSessionId,
        expected_offset: UploadOffsetPrecondition,
        mut stream: UploadByteStream,
        max_upload_bytes: u64,
    ) -> Result<UploadAppendResult, UploadTransitionError> {
        let lock_path = self.session_lock_path(&session.uuid);
        let _lock = acquire_fs_session_lock(lock_path).await?;

        let meta_path = self.session_meta_path(&session.uuid);
        let meta_bytes = match tokio::fs::read(&meta_path).await {
            Ok(b) => b,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(UploadTransitionError::NotFound);
            }
            Err(err) => {
                return Err(UploadTransitionError::Storage(StorageError::io(
                    err.to_string(),
                )));
            }
        };
        let mut meta: FsSessionMetaRecord = serde_json::from_slice(&meta_bytes).map_err(|e| {
            UploadTransitionError::Storage(StorageError::corrupt_data(e.to_string()))
        })?;
        if meta.repo != session.repo || meta.uuid != session.uuid {
            return Err(UploadTransitionError::NotFound);
        }

        if meta.state != UploadSessionState::Active {
            return Ok(UploadAppendResult::Conflict);
        }

        let data_path = self.session_data_path(&session.uuid);

        // Crash recovery: recover physical file size if needed
        if let Ok(file_meta) = tokio::fs::metadata(&data_path).await {
            if file_meta.len() > meta.committed_offset {
                if let Ok(f) = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&data_path)
                    .await
                {
                    let _ = f.set_len(meta.committed_offset).await;
                    let _ = f.sync_data().await;
                }
            }
        }

        match expected_offset {
            UploadOffsetPrecondition::Exact(off) => {
                if off != meta.committed_offset {
                    return Ok(UploadAppendResult::OffsetMismatch {
                        current_offset: meta.committed_offset,
                    });
                }
            }
            UploadOffsetPrecondition::CurrentForServerComposedMonolithicOperation => {}
        }

        // Load or recover hash state
        let hash_path = self.session_hash_path(&session.uuid, meta.hash_generation);
        let hash_st = match tokio::fs::read(&hash_path).await {
            Ok(b) => SerializableSha256::from_bytes(&b),
            Err(_) => None,
        };
        let mut hash_st = match hash_st {
            Some(st) if st.total_len == meta.committed_offset => st,
            _ => {
                let mut st = SerializableSha256::new();
                if let Ok(mut f) = tokio::fs::File::open(&data_path).await {
                    let mut buf = vec![0u8; 64 * 1024];
                    let mut read_total = 0u64;
                    while read_total < meta.committed_offset {
                        let to_read =
                            (meta.committed_offset - read_total).min(buf.len() as u64) as usize;
                        let n = f.read(&mut buf[..to_read]).await.map_err(|e| {
                            UploadTransitionError::Storage(StorageError::io(e.to_string()))
                        })?;
                        if n == 0 {
                            break;
                        }
                        st.update(&buf[..n]);
                        read_total += n as u64;
                    }
                }
                st
            }
        };

        let mut file = match tokio::fs::OpenOptions::new()
            .append(true)
            .open(&data_path)
            .await
        {
            Ok(f) => f,
            Err(err) => return Err(UploadTransitionError::Storage(map_fs_io_err(err))),
        };

        let limit = if max_upload_bytes > 0 {
            max_upload_bytes
        } else {
            self.max_upload_bytes
        };
        let mut written = 0u64;

        while let Some(chunk_res) = stream.next().await {
            let chunk = match chunk_res {
                Ok(c) => c,
                Err(err) => {
                    let _ = file.set_len(meta.committed_offset).await;
                    let _ = file.sync_data().await;
                    return Err(UploadTransitionError::Stream(err));
                }
            };
            if chunk.is_empty() {
                continue;
            }
            let next_total = meta
                .committed_offset
                .saturating_add(written)
                .saturating_add(chunk.len() as u64);
            if limit > 0 && next_total > limit {
                let _ = file.set_len(meta.committed_offset).await;
                let _ = file.sync_data().await;
                return Err(UploadTransitionError::TooLarge);
            }
            if let Err(err) = file.write_all(&chunk).await {
                let _ = file.set_len(meta.committed_offset).await;
                let _ = file.sync_data().await;
                return Err(UploadTransitionError::Storage(map_fs_io_err(err)));
            }
            hash_st.update(&chunk);
            written = written.saturating_add(chunk.len() as u64);
        }

        if let Err(err) = file.sync_data().await {
            let _ = file.set_len(meta.committed_offset).await;
            return Err(UploadTransitionError::Storage(map_fs_io_err(err)));
        }
        drop(file);

        let next_gen = meta.hash_generation.saturating_add(1);
        let next_hash_path = self.session_hash_path(&session.uuid, next_gen);
        write_atomic_file(&next_hash_path, &hash_st.to_bytes())
            .await
            .map_err(UploadTransitionError::Storage)?;

        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        meta.committed_offset = meta.committed_offset.saturating_add(written);
        meta.hash_generation = next_gen;
        meta.last_active_at_unix_secs = now;

        let meta_json = serde_json::to_vec(&meta).map_err(|e| {
            UploadTransitionError::Storage(StorageError::serialization(e.to_string()))
        })?;
        write_atomic_file(&meta_path, &meta_json)
            .await
            .map_err(UploadTransitionError::Storage)?;

        let _ = tokio::fs::remove_file(&hash_path).await;

        Ok(UploadAppendResult::Committed {
            new_offset: meta.committed_offset,
        })
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
        let lock_path = self.session_lock_path(&session.uuid);
        let _lock = acquire_fs_session_lock(lock_path).await?;

        let meta_path = self.session_meta_path(&session.uuid);
        let meta_bytes = match tokio::fs::read(&meta_path).await {
            Ok(b) => b,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                if let Some(receipt) = self
                    .get_finalized_receipt(session)
                    .await
                    .map_err(UploadTransitionError::Storage)?
                {
                    if receipt.digest == expected_digest.as_str() {
                        return Ok(PreparedFinalize {
                            session: session.clone(),
                            operation_id: "already-finalized".to_string(),
                            expected_digest: expected_digest.clone(),
                            committed_offset: receipt.size,
                            size: receipt.size,
                        });
                    } else {
                        return Err(UploadTransitionError::DigestMismatch {
                            expected: expected_digest.clone(),
                            computed: receipt.digest.clone(),
                        });
                    }
                }
                return Err(UploadTransitionError::NotFound);
            }
            Err(err) => {
                return Err(UploadTransitionError::Storage(StorageError::io(
                    err.to_string(),
                )));
            }
        };
        let mut meta: FsSessionMetaRecord = serde_json::from_slice(&meta_bytes).map_err(|e| {
            UploadTransitionError::Storage(StorageError::corrupt_data(e.to_string()))
        })?;
        if meta.repo != session.repo || meta.uuid != session.uuid {
            return Err(UploadTransitionError::NotFound);
        }

        if meta.state != UploadSessionState::Active {
            return Err(UploadTransitionError::Conflict);
        }

        let data_path = self.session_data_path(&session.uuid);

        if let Some(mut stream) = trailing_stream {
            let mut file = tokio::fs::OpenOptions::new()
                .append(true)
                .open(&data_path)
                .await
                .map_err(|e| UploadTransitionError::Storage(map_fs_io_err(e)))?;
            let limit = if max_upload_bytes > 0 {
                max_upload_bytes
            } else {
                self.max_upload_bytes
            };
            let mut written = 0u64;

            let hash_path = self.session_hash_path(&session.uuid, meta.hash_generation);
            let mut hash_st = match tokio::fs::read(&hash_path).await {
                Ok(b) => SerializableSha256::from_bytes(&b),
                Err(_) => None,
            }
            .unwrap_or_else(SerializableSha256::new);

            while let Some(chunk_res) = stream.next().await {
                let chunk = match chunk_res {
                    Ok(c) => c,
                    Err(err) => {
                        let _ = file.set_len(meta.committed_offset).await;
                        let _ = file.sync_data().await;
                        return Err(UploadTransitionError::Stream(err));
                    }
                };
                if chunk.is_empty() {
                    continue;
                }
                let next_total = meta
                    .committed_offset
                    .saturating_add(written)
                    .saturating_add(chunk.len() as u64);
                if limit > 0 && next_total > limit {
                    let _ = file.set_len(meta.committed_offset).await;
                    let _ = file.sync_data().await;
                    return Err(UploadTransitionError::TooLarge);
                }
                if let Err(err) = file.write_all(&chunk).await {
                    let _ = file.set_len(meta.committed_offset).await;
                    let _ = file.sync_data().await;
                    return Err(UploadTransitionError::Storage(map_fs_io_err(err)));
                }
                hash_st.update(&chunk);
                written = written.saturating_add(chunk.len() as u64);
            }
            if let Err(err) = file.sync_data().await {
                let _ = file.set_len(meta.committed_offset).await;
                return Err(UploadTransitionError::Storage(map_fs_io_err(err)));
            }
            drop(file);

            let next_gen = meta.hash_generation.saturating_add(1);
            let next_hash_path = self.session_hash_path(&session.uuid, next_gen);
            write_atomic_file(&next_hash_path, &hash_st.to_bytes())
                .await
                .map_err(UploadTransitionError::Storage)?;

            meta.committed_offset = meta.committed_offset.saturating_add(written);
            meta.hash_generation = next_gen;
            let _ = tokio::fs::remove_file(&hash_path).await;
        }

        match expected_offset {
            UploadOffsetPrecondition::Exact(off) => {
                if off != meta.committed_offset {
                    return Err(UploadTransitionError::OffsetMismatch {
                        expected: expected_offset,
                        current: meta.committed_offset,
                    });
                }
            }
            UploadOffsetPrecondition::CurrentForServerComposedMonolithicOperation => {}
        }

        // Verify digest
        let computed_hex = if expected_digest.algorithm() == "sha512" {
            let mut hasher = sha2::Sha512::new();
            let mut file = tokio::fs::File::open(&data_path)
                .await
                .map_err(|e| UploadTransitionError::Storage(map_fs_io_err(e)))?;
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = file
                    .read(&mut buf)
                    .await
                    .map_err(|e| UploadTransitionError::Storage(map_fs_io_err(e)))?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            hex::encode(hasher.finalize())
        } else {
            let hash_path = self.session_hash_path(&session.uuid, meta.hash_generation);
            let hash_st = match tokio::fs::read(&hash_path).await {
                Ok(b) => SerializableSha256::from_bytes(&b),
                Err(_) => None,
            };
            let st = match hash_st {
                Some(s) if s.total_len == meta.committed_offset => s,
                _ => {
                    let mut s = SerializableSha256::new();
                    let mut file = tokio::fs::File::open(&data_path)
                        .await
                        .map_err(|e| UploadTransitionError::Storage(map_fs_io_err(e)))?;
                    let mut buf = vec![0u8; 64 * 1024];
                    loop {
                        let n = file
                            .read(&mut buf)
                            .await
                            .map_err(|e| UploadTransitionError::Storage(map_fs_io_err(e)))?;
                        if n == 0 {
                            break;
                        }
                        s.update(&buf[..n]);
                    }
                    s
                }
            };
            st.finalize_hex()
        };

        if computed_hex != expected_digest.hex() {
            if abort_on_digest_mismatch {
                let _ = tokio::fs::remove_file(&data_path).await;
                let _ = tokio::fs::remove_file(&meta_path).await;
                let hash_path = self.session_hash_path(&session.uuid, meta.hash_generation);
                let _ = tokio::fs::remove_file(&hash_path).await;
            }
            return Err(UploadTransitionError::DigestMismatch {
                expected: expected_digest.clone(),
                computed: computed_hex,
            });
        }

        // Persist Finalizing state
        let operation_id = uuid::Uuid::new_v4().to_string();
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        meta.state = UploadSessionState::Finalizing;
        meta.finalizing_info = Some(FsFinalizingInfo {
            operation_id: operation_id.clone(),
            expected_digest: expected_digest.as_str().to_string(),
            size: meta.committed_offset,
            finalizing_at_unix_secs: now,
        });

        let meta_json = serde_json::to_vec(&meta).map_err(|e| {
            UploadTransitionError::Storage(StorageError::serialization(e.to_string()))
        })?;
        write_atomic_file(&meta_path, &meta_json)
            .await
            .map_err(UploadTransitionError::Storage)?;

        Ok(PreparedFinalize {
            session: session.clone(),
            operation_id,
            expected_digest: expected_digest.clone(),
            committed_offset: meta.committed_offset,
            size: meta.committed_offset,
        })
    }

    async fn commit_finalize(
        &self,
        prepared: &PreparedFinalize,
    ) -> Result<FinalizeOutcome, UploadTransitionError> {
        let lock_path = self.session_lock_path(&prepared.session.uuid);
        let _lock = acquire_fs_session_lock(lock_path).await?;

        // 1. Check if receipt already exists (idempotent retry)
        let receipt_path = self.finalized_receipt_path(&prepared.session.uuid);
        if let Ok(receipt_bytes) = tokio::fs::read(&receipt_path).await {
            if let Ok(receipt) = serde_json::from_slice::<FinalizedReceipt>(&receipt_bytes) {
                if receipt.digest == prepared.expected_digest.as_str() {
                    return Ok(FinalizeOutcome::AlreadyFinalized(BlobMeta {
                        size: receipt.size,
                    }));
                }
            }
        }

        // 2. Validate session metadata
        let meta_path = self.session_meta_path(&prepared.session.uuid);
        let data_path = self.session_data_path(&prepared.session.uuid);

        let meta_bytes = match tokio::fs::read(&meta_path).await {
            Ok(b) => b,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let dest_path = self.blob_path(&prepared.expected_digest);
                if let Ok(cas_meta) = tokio::fs::metadata(&dest_path).await {
                    if cas_meta.len() == prepared.size {
                        let receipt = FinalizedReceipt {
                            repo: prepared.session.repo.clone(),
                            uuid: prepared.session.uuid.clone(),
                            digest: prepared.expected_digest.as_str().to_string(),
                            size: prepared.size,
                            finalized_at_unix_secs: SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            format_version: 1,
                        };
                        let _ = write_atomic_file(
                            &receipt_path,
                            &serde_json::to_vec(&receipt).unwrap(),
                        )
                        .await;
                        return Ok(FinalizeOutcome::AlreadyFinalized(BlobMeta {
                            size: prepared.size,
                        }));
                    }
                }
                return Err(UploadTransitionError::NotFound);
            }
            Err(err) => {
                return Err(UploadTransitionError::Storage(StorageError::io(
                    err.to_string(),
                )));
            }
        };

        let meta: FsSessionMetaRecord = serde_json::from_slice(&meta_bytes).map_err(|e| {
            UploadTransitionError::Storage(StorageError::corrupt_data(e.to_string()))
        })?;

        if meta.state != UploadSessionState::Finalizing {
            return Err(UploadTransitionError::InvalidPreparedHandle);
        }
        let Some(ref fin_info) = meta.finalizing_info else {
            return Err(UploadTransitionError::InvalidPreparedHandle);
        };
        if fin_info.operation_id != prepared.operation_id
            || fin_info.expected_digest != prepared.expected_digest.as_str()
        {
            return Err(UploadTransitionError::InvalidPreparedHandle);
        }

        // Publish to CAS blob store
        let dest_dir = self
            .root
            .join("blobs")
            .join(prepared.expected_digest.algorithm())
            .join(prepared.expected_digest.prefix2());
        ensure_dir(&dest_dir).map_err(UploadTransitionError::Storage)?;
        let dest_path = dest_dir.join(prepared.expected_digest.hex());

        if let Err(err) = tokio::fs::rename(&data_path, &dest_path).await {
            if tokio::fs::metadata(&dest_path).await.is_err() {
                return Err(UploadTransitionError::Storage(map_fs_io_err(err)));
            }
        }
        let _ = fsync_dir(self.uploads_dir().as_path()).await;
        let _ = fsync_dir(dest_dir.as_path()).await;

        // STEP 5: Durably create target repository membership BEFORE receipt
        let membership = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
            prepared.session.repo.clone(),
            prepared.expected_digest.clone(),
            Some(prepared.session.uuid.clone()),
        );
        self.link_repo_blob(&membership)
            .await
            .map_err(UploadTransitionError::Storage)?;

        // Write finalized receipt
        ensure_dir(&self.finalized_dir()).map_err(UploadTransitionError::Storage)?;
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let receipt = FinalizedReceipt {
            repo: prepared.session.repo.clone(),
            uuid: prepared.session.uuid.clone(),
            digest: prepared.expected_digest.as_str().to_string(),
            size: prepared.size,
            finalized_at_unix_secs: now,
            format_version: 1,
        };
        let receipt_json = serde_json::to_vec(&receipt).map_err(|e| {
            UploadTransitionError::Storage(StorageError::serialization(e.to_string()))
        })?;
        write_atomic_file(&receipt_path, &receipt_json)
            .await
            .map_err(UploadTransitionError::Storage)?;

        // Clean staging meta and hash files
        let _ = tokio::fs::remove_file(&meta_path).await;
        let hash_path = self.session_hash_path(&prepared.session.uuid, meta.hash_generation);
        let _ = tokio::fs::remove_file(&hash_path).await;

        Ok(FinalizeOutcome::Published(BlobMeta {
            size: prepared.size,
        }))
    }

    async fn abort_session(&self, session: &UploadSessionId) -> Result<(), StorageError> {
        let lock_path = self.session_lock_path(&session.uuid);
        let _lock = acquire_fs_session_lock(lock_path.clone()).await?;

        let data_path = self.session_data_path(&session.uuid);
        let meta_path = self.session_meta_path(&session.uuid);

        let _ = tokio::fs::remove_file(&data_path).await;
        let _ = tokio::fs::remove_file(&meta_path).await;

        // Best effort clean any hash gen files
        for generation in 0..100 {
            let hash_path = self.session_hash_path(&session.uuid, generation);
            if tokio::fs::remove_file(&hash_path).await.is_err() && generation > 10 {
                break;
            }
        }
        let _ = tokio::fs::remove_file(&lock_path).await;

        Ok(())
    }

    async fn recover_session(
        &self,
        session: &UploadSessionId,
    ) -> Result<UploadSessionStatus, UploadTransitionError> {
        let lock_path = self.session_lock_path(&session.uuid);
        let _lock = acquire_fs_session_lock(lock_path).await?;

        let meta_path = self.session_meta_path(&session.uuid);
        let meta_bytes = match tokio::fs::read(&meta_path).await {
            Ok(b) => b,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                // Check if already finalized
                if let Some(receipt) = self
                    .get_finalized_receipt(session)
                    .await
                    .map_err(UploadTransitionError::Storage)?
                {
                    return Ok(UploadSessionStatus {
                        session: session.clone(),
                        state: UploadSessionState::Finalizing,
                        committed_offset: receipt.size,
                        created_at: UNIX_EPOCH
                            + Duration::from_secs(receipt.finalized_at_unix_secs),
                        last_active_at: UNIX_EPOCH
                            + Duration::from_secs(receipt.finalized_at_unix_secs),
                    });
                }
                return Err(UploadTransitionError::NotFound);
            }
            Err(err) => {
                return Err(UploadTransitionError::Storage(StorageError::io(
                    err.to_string(),
                )));
            }
        };

        let meta: FsSessionMetaRecord = serde_json::from_slice(&meta_bytes).map_err(|e| {
            UploadTransitionError::Storage(StorageError::corrupt_data(e.to_string()))
        })?;

        if meta.state == UploadSessionState::Finalizing {
            if let Some(ref fin_info) = meta.finalizing_info
                && let Ok(digest) = Digest::parse(&fin_info.expected_digest)
            {
                let dest_path = self.blob_path(&digest);
                if let Ok(cas_meta) = tokio::fs::metadata(&dest_path).await
                    && cas_meta.len() == fin_info.size
                {
                    let membership =
                        crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
                            session.repo.clone(),
                            digest.clone(),
                            Some(session.uuid.clone()),
                        );
                    let _ = self.link_repo_blob(&membership).await;

                    let receipt_path = self.finalized_receipt_path(&session.uuid);
                    let receipt = FinalizedReceipt {
                        repo: session.repo.clone(),
                        uuid: session.uuid.clone(),
                        digest: fin_info.expected_digest.clone(),
                        size: fin_info.size,
                        finalized_at_unix_secs: fin_info.finalizing_at_unix_secs,
                        format_version: 1,
                    };
                    let _ =
                        write_atomic_file(&receipt_path, &serde_json::to_vec(&receipt).unwrap())
                            .await;
                    return Ok(UploadSessionStatus {
                        session: session.clone(),
                        state: UploadSessionState::Finalizing,
                        committed_offset: fin_info.size,
                        created_at: UNIX_EPOCH + Duration::from_secs(meta.created_at_unix_secs),
                        last_active_at: UNIX_EPOCH
                            + Duration::from_secs(fin_info.finalizing_at_unix_secs),
                    });
                }
            }
        } else {
            let data_path = self.session_data_path(&session.uuid);
            if let Ok(file_meta) = tokio::fs::metadata(&data_path).await
                && file_meta.len() > meta.committed_offset
                && let Ok(f) = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&data_path)
                    .await
            {
                let _ = f.set_len(meta.committed_offset).await;
                let _ = f.sync_data().await;
            }
        }

        Ok(UploadSessionStatus {
            session: session.clone(),
            state: meta.state,
            committed_offset: meta.committed_offset,
            created_at: UNIX_EPOCH + Duration::from_secs(meta.created_at_unix_secs),
            last_active_at: UNIX_EPOCH + Duration::from_secs(meta.last_active_at_unix_secs),
        })
    }

    async fn get_finalized_receipt(
        &self,
        session: &UploadSessionId,
    ) -> Result<Option<FinalizedReceipt>, StorageError> {
        let receipt_path = self.finalized_receipt_path(&session.uuid);
        match tokio::fs::read(&receipt_path).await {
            Ok(bytes) => {
                let receipt: FinalizedReceipt = serde_json::from_slice(&bytes)
                    .map_err(|e| StorageError::corrupt_data(e.to_string()))?;
                if receipt.repo == session.repo && receipt.uuid == session.uuid {
                    Ok(Some(receipt))
                } else {
                    Ok(None)
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(StorageError::io(err.to_string())),
        }
    }

    async fn reap_expired_sessions(
        &self,
        max_age_secs: u64,
        receipt_ttl_secs: u64,
    ) -> Result<usize, StorageError> {
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut count = 0;

        // 1. Scan sessions in uploads_dir
        let uploads_dir = self.uploads_dir();
        if let Ok(mut entries) = tokio::fs::read_dir(&uploads_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let file_name = entry.file_name().to_string_lossy().to_string();
                if file_name.ends_with(".meta.json") {
                    let uuid = file_name.trim_end_matches(".meta.json");
                    let lock_path = self.session_lock_path(uuid);
                    let lock_opt = match try_acquire_fs_session_lock(lock_path).await {
                        Ok(Some(g)) => Some(g),
                        _ => None,
                    };
                    if let Some(_guard) = lock_opt
                        && let Ok(bytes) = tokio::fs::read(entry.path()).await
                        && let Ok(meta) = serde_json::from_slice::<FsSessionMetaRecord>(&bytes)
                        && now.saturating_sub(meta.last_active_at_unix_secs) >= max_age_secs
                    {
                        let session = UploadSessionId::new(meta.repo.clone(), uuid);
                        drop(_guard);
                        if meta.state == UploadSessionState::Finalizing {
                            let _ = self.recover_session(&session).await;
                            count += 1;
                        } else if meta.state == UploadSessionState::Appending {
                            let _ = self.recover_session(&session).await;
                            let _ = self.abort_session(&session).await;
                            count += 1;
                        } else {
                            let _ = self.abort_session(&session).await;
                            count += 1;
                        }
                    }
                }
            }
        }

        // 2. Scan finalized receipts
        let finalized_dir = self.finalized_dir();
        if let Ok(mut entries) = tokio::fs::read_dir(&finalized_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let file_name = entry.file_name().to_string_lossy().to_string();
                if file_name.ends_with(".json")
                    && let Ok(receipt_bytes) = tokio::fs::read(entry.path()).await
                    && let Ok(receipt) = serde_json::from_slice::<FinalizedReceipt>(&receipt_bytes)
                {
                    let age = now.saturating_sub(receipt.finalized_at_unix_secs);
                    if age >= receipt_ttl_secs {
                        let _ = tokio::fs::remove_file(entry.path()).await;
                        count += 1;
                    }
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
        let canonical = CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let path = self.repo_blob_path(&canonical, digest);
        match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let record = serde_json::from_slice::<
                    crate::storage::repo_membership::RepoBlobMembershipRecord,
                >(&bytes)
                .map_err(|e| {
                    StorageError::corrupt_data(format!(
                        "corrupt membership record in {}: {e}",
                        path.display()
                    ))
                })?;
                Ok(Some(record))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(StorageError::io(err.to_string())),
        }
    }

    async fn link_repo_blob(
        &self,
        record: &crate::storage::repo_membership::RepoBlobMembershipRecord,
    ) -> Result<(), StorageError> {
        let dir = self.repo_blobs_dir(&record.repo, record.digest.algorithm());
        ensure_dir(&dir)?;
        let path = self.repo_blob_path(&record.repo, &record.digest);
        let bytes = serde_json::to_vec(record)
            .map_err(|e| StorageError::serialization(format!("serialize membership: {e}")))?;
        write_atomic_file(&path, &bytes).await?;
        Ok(())
    }

    async fn set_membership_candidate(
        &self,
        repo: &str,
        digest: &Digest,
        since_unix_secs: u64,
    ) -> Result<bool, StorageError> {
        let canonical = CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let path = self.repo_blob_path(&canonical, digest);
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(StorageError::io(err.to_string())),
        };
        let mut record = serde_json::from_slice::<
            crate::storage::repo_membership::RepoBlobMembershipRecord,
        >(&bytes)
        .map_err(|e| StorageError::corrupt_data(format!("corrupt membership record: {e}")))?;
        if record.state == crate::storage::repo_membership::MembershipState::Candidate {
            return Ok(false);
        }
        record.state = crate::storage::repo_membership::MembershipState::Candidate;
        record.unreferenced_since_unix_secs = Some(since_unix_secs);
        let updated_bytes = serde_json::to_vec(&record)
            .map_err(|e| StorageError::serialization(format!("serialize membership: {e}")))?;
        write_atomic_file(&path, &updated_bytes).await?;
        Ok(true)
    }

    async fn clear_membership_candidate(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<bool, StorageError> {
        let canonical = CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let path = self.repo_blob_path(&canonical, digest);
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(StorageError::io(err.to_string())),
        };
        let mut record = serde_json::from_slice::<
            crate::storage::repo_membership::RepoBlobMembershipRecord,
        >(&bytes)
        .map_err(|e| StorageError::corrupt_data(format!("corrupt membership record: {e}")))?;
        if record.state == crate::storage::repo_membership::MembershipState::Active
            && record.unreferenced_since_unix_secs.is_none()
        {
            return Ok(false);
        }
        record.state = crate::storage::repo_membership::MembershipState::Active;
        record.unreferenced_since_unix_secs = None;
        let updated_bytes = serde_json::to_vec(&record)
            .map_err(|e| StorageError::serialization(format!("serialize membership: {e}")))?;
        write_atomic_file(&path, &updated_bytes).await?;
        Ok(true)
    }

    async fn unlink_repo_blob(&self, repo: &str, digest: &Digest) -> Result<bool, StorageError> {
        let canonical = CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let path = self.repo_blob_path(&canonical, digest);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(StorageError::io(err.to_string())),
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
        let max_limit = 1000;
        let limit = page_limit.min(max_limit).max(1);

        let canonical = CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let encoded_repo = crate::storage::repo_membership::encode_canonical_repo_key(&canonical);
        let repo_dir = self
            .root
            .join("repo-memberships")
            .join("by-repo")
            .join(&encoded_repo);
        if tokio::fs::metadata(&repo_dir).await.is_err() {
            return Ok((Vec::new(), None));
        }

        use std::cmp::Ordering;
        use std::collections::BinaryHeap;

        #[derive(Eq, PartialEq)]
        struct Candidate {
            digest_str: String,
            path: PathBuf,
        }

        impl Ord for Candidate {
            fn cmp(&self, other: &Self) -> Ordering {
                self.digest_str.cmp(&other.digest_str)
            }
        }

        impl PartialOrd for Candidate {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        let mut heap: BinaryHeap<Candidate> = BinaryHeap::with_capacity(limit + 2);

        if let Ok(mut algo_entries) = tokio::fs::read_dir(&repo_dir).await {
            while let Ok(Some(algo_entry)) = algo_entries.next_entry().await {
                if algo_entry
                    .file_type()
                    .await
                    .map(|t| t.is_dir())
                    .unwrap_or(false)
                {
                    let algo_path = algo_entry.path();
                    let algo_str = algo_entry.file_name().to_string_lossy().to_string();
                    if let Ok(mut file_entries) = tokio::fs::read_dir(&algo_path).await {
                        while let Ok(Some(file_entry)) = file_entries.next_entry().await {
                            let file_name = file_entry.file_name().to_string_lossy().to_string();
                            if file_name.ends_with(".json") && !file_name.contains(".tmp.") {
                                let hex = file_name.trim_end_matches(".json");
                                let digest_str = format!("{algo_str}:{hex}");

                                if let Some(token) = continuation_token
                                    && digest_str.as_str() <= token
                                {
                                    continue;
                                }

                                let cand = Candidate {
                                    digest_str,
                                    path: file_entry.path(),
                                };

                                if heap.len() < limit + 1 {
                                    heap.push(cand);
                                } else if let Some(top) = heap.peek()
                                    && cand.digest_str < top.digest_str
                                {
                                    heap.pop();
                                    heap.push(cand);
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut sorted: Vec<Candidate> = heap.into_sorted_vec();
        let has_more = sorted.len() > limit;
        if has_more {
            sorted.truncate(limit);
        }

        let next_token = if has_more {
            sorted.last().map(|c| c.digest_str.clone())
        } else {
            None
        };

        let mut records = Vec::with_capacity(sorted.len());
        for cand in sorted {
            let bytes = tokio::fs::read(&cand.path).await.map_err(|e| {
                StorageError::io(format!(
                    "failed to read membership in {}: {e}",
                    cand.path.display()
                ))
            })?;
            let record = serde_json::from_slice::<
                crate::storage::repo_membership::RepoBlobMembershipRecord,
            >(&bytes)
            .map_err(|e| {
                StorageError::corrupt_data(format!(
                    "corrupt membership record in {}: {e}",
                    cand.path.display()
                ))
            })?;
            records.push(record);
        }

        Ok((records, next_token))
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
        let max_limit = 1000;
        let limit = page_limit.min(max_limit).max(1);

        let root_dir = self.root.join("repo-memberships").join("by-repo");
        if tokio::fs::metadata(&root_dir).await.is_err() {
            return Ok((Vec::new(), None));
        }

        use std::cmp::Ordering;
        use std::collections::BinaryHeap;

        #[derive(Eq, PartialEq)]
        struct Candidate {
            sort_key: String,
            repo: crate::registry::canonical_name::CanonicalRepoName,
            digest: Digest,
            path: PathBuf,
        }

        impl Ord for Candidate {
            fn cmp(&self, other: &Self) -> Ordering {
                self.sort_key.cmp(&other.sort_key)
            }
        }

        impl PartialOrd for Candidate {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        let mut heap: BinaryHeap<Candidate> = BinaryHeap::with_capacity(limit + 2);

        if let Ok(mut repo_entries) = tokio::fs::read_dir(&root_dir).await {
            while let Ok(Some(repo_entry)) = repo_entries.next_entry().await {
                if repo_entry
                    .file_type()
                    .await
                    .map(|t| t.is_dir())
                    .unwrap_or(false)
                {
                    let repo_encoded = repo_entry.file_name().to_string_lossy().to_string();
                    let repo =
                        crate::storage::repo_membership::decode_canonical_repo_key(&repo_encoded)
                            .map_err(|e| {
                            StorageError::corrupt_data(format!(
                                "corrupt repository membership directory '{repo_encoded}': {e}"
                            ))
                        })?;
                    let repo_path = repo_entry.path();
                    if let Ok(mut algo_entries) = tokio::fs::read_dir(&repo_path).await {
                        while let Ok(Some(algo_entry)) = algo_entries.next_entry().await {
                            if algo_entry
                                .file_type()
                                .await
                                .map(|t| t.is_dir())
                                .unwrap_or(false)
                            {
                                let algo_path = algo_entry.path();
                                let algo_str = algo_entry.file_name().to_string_lossy().to_string();
                                if let Ok(mut file_entries) = tokio::fs::read_dir(&algo_path).await
                                {
                                    while let Ok(Some(file_entry)) = file_entries.next_entry().await
                                    {
                                        let file_name =
                                            file_entry.file_name().to_string_lossy().to_string();
                                        if file_name.ends_with(".json")
                                            && !file_name.contains(".tmp.")
                                        {
                                            let hex = file_name.trim_end_matches(".json");
                                            if let Ok(digest) =
                                                Digest::parse(&format!("{algo_str}:{hex}"))
                                            {
                                                let sort_key =
                                                    format!("{repo_encoded}/{algo_str}/{hex}");

                                                if let Some(token) = continuation_token
                                                    && sort_key.as_str() <= token
                                                {
                                                    continue;
                                                }

                                                let cand = Candidate {
                                                    sort_key,
                                                    repo: repo.clone(),
                                                    digest,
                                                    path: file_entry.path(),
                                                };

                                                if heap.len() < limit + 1 {
                                                    heap.push(cand);
                                                } else if let Some(top) = heap.peek()
                                                    && cand.sort_key < top.sort_key
                                                {
                                                    heap.pop();
                                                    heap.push(cand);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut sorted: Vec<Candidate> = heap.into_sorted_vec();
        let has_more = sorted.len() > limit;
        if has_more {
            sorted.truncate(limit);
        }

        let next_token = if has_more {
            sorted.last().map(|c| c.sort_key.clone())
        } else {
            None
        };

        let mut records = Vec::with_capacity(sorted.len());
        for cand in sorted {
            let bytes = tokio::fs::read(&cand.path).await.map_err(|e| {
                StorageError::io(format!(
                    "failed to read membership in {}: {e}",
                    cand.path.display()
                ))
            })?;
            let record = serde_json::from_slice::<
                crate::storage::repo_membership::RepoBlobMembershipRecord,
            >(&bytes)
            .map_err(|e| {
                StorageError::corrupt_data(format!(
                    "corrupt membership record in {}: {e}",
                    cand.path.display()
                ))
            })?;
            records.push(record);
        }

        Ok((records, next_token))
    }

    async fn count_repo_blob_memberships(&self, digest: &Digest) -> Result<usize, StorageError> {
        let mut count = 0;
        let by_repo_root = self.root.join("repo-memberships").join("by-repo");
        if let Ok(mut repo_entries) = tokio::fs::read_dir(&by_repo_root).await {
            while let Ok(Some(repo_entry)) = repo_entries.next_entry().await {
                if let Ok(ft) = repo_entry.file_type().await
                    && ft.is_dir()
                {
                    let marker_path = repo_entry
                        .path()
                        .join(digest.algorithm())
                        .join(format!("{}.json", digest.hex()));
                    if tokio::fs::metadata(&marker_path).await.is_ok() {
                        count += 1;
                    }
                }
            }
        }
        Ok(count)
    }

    async fn is_membership_ready(&self) -> Result<bool, StorageError> {
        let checkpoint = self.get_migration_checkpoint().await?;
        let marker = self.root.join("meta").join("membership_ready.json");
        let ready_marker_exists = tokio::fs::metadata(&marker).await.is_ok();
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
        let path = self.root.join("meta").join("migration_checkpoint.json");
        match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let rec = serde_json::from_slice::<
                    crate::storage::repo_membership::MigrationCheckpointRecord,
                >(&bytes)
                .map_err(|e| {
                    StorageError::corrupt_data(format!("corrupt migration checkpoint: {e}"))
                })?;
                Ok(Some(rec))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::io(e.to_string())),
        }
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

impl FsStorage {
    pub async fn write_quarantine_timestamp(
        &self,
        digest: &Digest,
        timestamp: SystemTime,
    ) -> Result<(), StorageError> {
        let ts_dir = self
            .root
            .join("quarantine")
            .join("meta")
            .join(digest.algorithm())
            .join(digest.prefix2());
        tokio::fs::create_dir_all(&ts_dir)
            .await
            .map_err(|e| StorageError::io(format!("mkdir {}: {e}", ts_dir.display())))?;
        let ts_path = ts_dir.join(format!("{}.ts", digest.hex()));
        let secs = timestamp
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        write_atomic_file(&ts_path, format!("{secs}\n").as_bytes()).await?;
        Ok(())
    }

    pub async fn read_quarantine_timestamp(
        &self,
        digest: &Digest,
    ) -> Result<Option<SystemTime>, StorageError> {
        let ts_path = self
            .root
            .join("quarantine")
            .join("meta")
            .join(digest.algorithm())
            .join(digest.prefix2())
            .join(format!("{}.ts", digest.hex()));
        if let Ok(content) = tokio::fs::read_to_string(&ts_path).await {
            if let Ok(secs) = content.trim().parse::<u64>() {
                return Ok(Some(
                    std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs),
                ));
            }
        }

        Ok(None)
    }

    pub async fn remove_quarantine_timestamp(&self, digest: &Digest) -> Result<(), StorageError> {
        let ts_path = self
            .root
            .join("quarantine")
            .join("meta")
            .join(digest.algorithm())
            .join(digest.prefix2())
            .join(format!("{}.ts", digest.hex()));
        let _ = tokio::fs::remove_file(&ts_path).await;
        Ok(())
    }
}

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

#[async_trait]
impl GcStorage for FsStorage {
    async fn list_cas_blobs_page(
        &self,
        cursor: Option<&GcCursor>,
        limit: usize,
    ) -> Result<GcBlobPage, StorageError> {
        let max_limit = 1000;
        let limit = limit.min(max_limit).max(1);
        let root = self.root.join("blobs").join("sha256");

        if tokio::fs::metadata(&root).await.is_err() {
            return Ok(GcBlobPage {
                items: Vec::new(),
                next_cursor: None,
            });
        }

        let cursor_str = cursor.map(|c| c.0.as_str());

        let mut prefix_dirs = Vec::new();
        let mut rd = tokio::fs::read_dir(&root)
            .await
            .map_err(|e| StorageError::io(format!("read_dir {}: {e}", root.display())))?;
        while let Some(ent) = rd
            .next_entry()
            .await
            .map_err(|e| StorageError::io(format!("read_dir entry in {}: {e}", root.display())))?
        {
            let ft = ent.file_type().await.map_err(|e| {
                StorageError::io(format!("file_type for {}: {e}", ent.path().display()))
            })?;
            let fname = ent.file_name();
            let name = fname.to_str().ok_or_else(|| {
                StorageError::corrupt_data(format!(
                    "malformed non-utf8 entry in CAS root: {}",
                    ent.path().display()
                ))
            })?;

            if !ft.is_dir() {
                return Err(StorageError::corrupt_data(format!(
                    "malformed non-directory entry in CAS prefix directory root: {}",
                    ent.path().display()
                )));
            }

            if name.len() != 2 || !name.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(StorageError::corrupt_data(format!(
                    "malformed 2-char prefix directory name in CAS root: {name}"
                )));
            }
            prefix_dirs.push(name.to_ascii_lowercase());
        }
        prefix_dirs.sort();

        let mut candidates = Vec::new();
        let mut next_cursor = None;

        'outer: for p2 in prefix_dirs {
            let dir_path = root.join(&p2);
            let mut entries = Vec::new();
            let mut rd = tokio::fs::read_dir(&dir_path)
                .await
                .map_err(|e| StorageError::io(format!("read_dir {}: {e}", dir_path.display())))?;
            while let Some(ent) = rd.next_entry().await.map_err(|e| {
                StorageError::io(format!("read_dir entry in {}: {e}", dir_path.display()))
            })? {
                let ft = ent.file_type().await.map_err(|e| {
                    StorageError::io(format!("file_type for {}: {e}", ent.path().display()))
                })?;
                let fname = ent.file_name();
                let name = fname.to_str().ok_or_else(|| {
                    StorageError::corrupt_data(format!(
                        "malformed non-utf8 blob filename in {}: {}",
                        dir_path.display(),
                        ent.path().display()
                    ))
                })?;

                if !ft.is_file() {
                    return Err(StorageError::corrupt_data(format!(
                        "malformed non-file entry in CAS shard directory {}: {}",
                        dir_path.display(),
                        name
                    )));
                }

                if name.len() != 64
                    || !name.to_ascii_lowercase().starts_with(&p2)
                    || !name.chars().all(|c| c.is_ascii_hexdigit())
                {
                    return Err(StorageError::corrupt_data(format!(
                        "malformed blob file name in CAS shard {}: {}",
                        dir_path.display(),
                        name
                    )));
                }
                entries.push(name.to_ascii_lowercase());
            }
            entries.sort();

            for hex in entries {
                let digest_str = format!("sha256:{hex}");
                if let Some(c) = cursor_str {
                    if digest_str.as_str() <= c {
                        continue;
                    }
                }

                let path = dir_path.join(&hex);
                let meta = tokio::fs::metadata(&path)
                    .await
                    .map_err(|e| StorageError::io(format!("metadata {}: {e}", path.display())))?;
                let modified = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
                let size = meta.len();
                let mtime_secs = modified
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let version = BlobObjectVersion(format!("{mtime_secs}:{size}"));
                let digest = Digest::parse(&digest_str).map_err(|e| {
                    StorageError::internal_invariant(format!(
                        "failed to parse digest from hex {hex}: {e}"
                    ))
                })?;

                candidates.push(GcBlobCandidate {
                    digest,
                    size,
                    last_modified: modified,
                    version,
                });

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

        let src = self
            .root
            .join("blobs")
            .join(digest.algorithm())
            .join(digest.prefix2())
            .join(digest.hex());

        let meta = match tokio::fs::metadata(&src).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(GcQuarantineResult::Skipped);
            }
            Err(e) => {
                return Err(StorageError::io(format!("metadata {}: {e}", src.display())));
            }
        };

        let dest_dir = self
            .root
            .join("quarantine")
            .join("blobs")
            .join(digest.algorithm())
            .join(digest.prefix2());
        tokio::fs::create_dir_all(&dest_dir)
            .await
            .map_err(|e| StorageError::io(format!("mkdir {}: {e}", dest_dir.display())))?;

        let dest = dest_dir.join(digest.hex());
        if tokio::fs::metadata(&dest).await.is_ok() {
            return Ok(GcQuarantineResult::Skipped);
        }

        match tokio::fs::rename(&src, &dest).await {
            Ok(()) => {
                let now = SystemTime::now();
                self.write_quarantine_timestamp(digest, now).await?;
                Ok(GcQuarantineResult::Quarantined { size: meta.len() })
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(GcQuarantineResult::Skipped),
            Err(e) => Err(StorageError::io(format!(
                "rename {} -> {}: {e}",
                src.display(),
                dest.display()
            ))),
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

        let src = self
            .root
            .join("quarantine")
            .join("blobs")
            .join(digest.algorithm())
            .join(digest.prefix2())
            .join(digest.hex());

        let meta = match tokio::fs::metadata(&src).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(StorageError::io(format!("metadata {}: {e}", src.display())));
            }
        };

        let dest_dir = self
            .root
            .join("blobs")
            .join(digest.algorithm())
            .join(digest.prefix2());
        tokio::fs::create_dir_all(&dest_dir)
            .await
            .map_err(|e| StorageError::io(format!("mkdir {}: {e}", dest_dir.display())))?;
        let dest = dest_dir.join(digest.hex());

        if tokio::fs::metadata(&dest).await.is_ok() {
            let _ = tokio::fs::remove_file(&src).await;
            let _ = self.remove_quarantine_timestamp(digest).await;
            return Ok(Some(meta.len()));
        }

        match tokio::fs::rename(&src, &dest).await {
            Ok(()) => {
                let _ = self.remove_quarantine_timestamp(digest).await;
                Ok(Some(meta.len()))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::io(format!(
                "rename {} -> {}: {e}",
                src.display(),
                dest.display()
            ))),
        }
    }

    async fn quarantined_blob_version(
        &self,
        digest: &Digest,
    ) -> Result<Option<BlobObjectVersion>, StorageError> {
        let path = self
            .root
            .join("quarantine")
            .join("blobs")
            .join(digest.algorithm())
            .join(digest.prefix2())
            .join(digest.hex());
        if tokio::fs::metadata(&path).await.is_err() {
            return Ok(None);
        }
        compute_fs_blob_version(&path).await.map(Some)
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

        let path = self
            .root
            .join("quarantine")
            .join("blobs")
            .join(digest.algorithm())
            .join(digest.prefix2())
            .join(digest.hex());

        if tokio::fs::metadata(&path).await.is_err() {
            let _ = self.remove_quarantine_timestamp(digest).await;
            return Ok(GcDeleteResult::NotFound);
        }

        let current_version = compute_fs_blob_version(&path).await?;
        if &current_version != expected_version {
            return Ok(GcDeleteResult::PreconditionFailed {
                current_version: Some(current_version),
            });
        }

        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                let _ = self.remove_quarantine_timestamp(digest).await;
                Ok(GcDeleteResult::Deleted)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let _ = self.remove_quarantine_timestamp(digest).await;
                Ok(GcDeleteResult::NotFound)
            }
            Err(e) => Err(StorageError::io(format!(
                "remove_file {}: {e}",
                path.display()
            ))),
        }
    }

    fn gc_strategy(&self) -> GcStorageStrategy {
        GcStorageStrategy::FilesystemQuarantine
    }
}

#[cfg(test)]
#[path = "fs/tests.rs"]
mod tests;

#[cfg(all(target_os = "linux", test))]
#[path = "fs/contained_metadata.rs"]
mod contained_metadata;

#[cfg(test)]
#[path = "fs/metadata_seam.rs"]
mod metadata_seam;

#[cfg(test)]
#[path = "fs/payload_seam.rs"]
mod payload_seam;
