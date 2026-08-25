use super::{
    BlobMeta, ManifestMeta, ReferrerDescriptor, RepoTimestamps, Storage, StorageError, ensure_dir,
};
use crate::registry::digest::Digest;
use async_trait::async_trait;
use bytes::Bytes;
use sha2::Digest as _;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;
use std::time::{Duration, Instant};
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
}

impl FsStorage {
    pub fn new(root: PathBuf, max_upload_bytes: u64) -> Self {
        ensure_dir(&root);
        let mut upload_hashes = Vec::with_capacity(HASH_SHARDS);
        for _ in 0..HASH_SHARDS {
            upload_hashes.push(Mutex::new(std::collections::HashMap::new()));
        }
        let mut referrer_locks = Vec::with_capacity(REFERRER_SHARDS);
        for _ in 0..REFERRER_SHARDS {
            referrer_locks.push(Mutex::new(()));
        }
        Self {
            root,
            max_upload_bytes,
            upload_hashes,
            referrer_locks,
        }
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

    async fn detect_manifest_media_type(&self, bytes: &[u8]) -> Result<String, StorageError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|err| StorageError::Internal(err.to_string()))?;
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
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };

        let mut files = Vec::new();
        loop {
            match dir.next_entry().await {
                Ok(Some(entry)) => files.push(entry.path()),
                Ok(None) => break,
                Err(err) => return Err(StorageError::Internal(err.to_string())),
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
                Err(err) => return Err(StorageError::Internal(err.to_string())),
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
                if name == "tags" || name == "manifests" || name == "referrers" {
                    continue;
                }

                let child_path = entry.path();
                let child_rel = if rel.is_empty() {
                    name
                } else {
                    format!("{rel}/{name}")
                };

                // Consider this a repo if it has tags/ or manifests/ directories.
                let tags_dir = child_path.join("tags");
                let manifests_dir = child_path.join("manifests");
                let has_tags = tokio::fs::metadata(&tags_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                let has_manifests = tokio::fs::metadata(&manifests_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                if has_tags || has_manifests {
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
            Err(err) => return Err(StorageError::Internal(err.to_string())),
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
    if err.raw_os_error() == Some(libc::ENOSPC) {
        return StorageError::InsufficientStorage;
    }
    StorageError::Internal(err.to_string())
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
    .map_err(|e| StorageError::Internal(e.to_string()))?
    .map_err(map_fs_io_err)
}

async fn atomic_write_file(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let Some(parent) = path.parent() else {
        return Err(StorageError::Internal("invalid path".to_string()));
    };
    ensure_dir(&parent.to_path_buf());

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
            Err(err) => return Err(StorageError::Internal(err.to_string())),
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

    async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError> {
        let path = self.blob_path(digest);
        match tokio::fs::metadata(&path).await {
            Ok(meta) => Ok(BlobMeta { size: meta.len() }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let qpath = self.quarantine_blob_path(digest);
                match tokio::fs::metadata(&qpath).await {
                    Ok(meta) => Ok(BlobMeta { size: meta.len() }),
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        Err(StorageError::NotFound)
                    }
                    Err(err) => Err(StorageError::Internal(err.to_string())),
                }
            }
            Err(err) => Err(StorageError::Internal(err.to_string())),
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
                    Err(err) => return Err(StorageError::Internal(err.to_string())),
                }
            }
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };
        let meta = match file.metadata().await {
            Ok(m) => m,
            Err(err) => return Err(StorageError::Internal(err.to_string())),
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
            Err(err) => return Err(StorageError::Internal(err.to_string())),
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
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        }

        let tags_dir = repo_dir.join("tags");
        let mut dir = match tokio::fs::read_dir(&tags_dir).await {
            Ok(d) => d,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };

        let mut tags = Vec::new();
        loop {
            match dir.next_entry().await {
                Ok(Some(entry)) => {
                    if let Some(file_name) = entry.file_name().to_str() {
                        tags.push(file_name.to_string());
                    }
                }
                Ok(None) => break,
                Err(err) => return Err(StorageError::Internal(err.to_string())),
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
            Err(err) => return Err(StorageError::Internal(err.to_string())),
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
            Err(err) => return Err(StorageError::Internal(err.to_string())),
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
        ensure_dir(&dir);

        let media_type = self.detect_manifest_media_type(&bytes).await?;
        let path = dir.join(digest.hex());
        atomic_write_file(&path, &bytes).await?;

        Ok(ManifestMeta {
            size: bytes.len() as u64,
            media_type,
        })
    }

    async fn set_tag(&self, name: &str, tag: &str, digest: &Digest) -> Result<(), StorageError> {
        let dir = self.root.join("repos").join(name).join("tags");
        ensure_dir(&dir);
        let path = dir.join(tag);
        let body = format!("{}\n", digest.as_str());
        atomic_write_file(&path, body.as_bytes()).await?;
        Ok(())
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
            Err(err) => Err(StorageError::Internal(err.to_string())),
        }
    }

    async fn create_upload(&self) -> Result<super::UploadMeta, StorageError> {
        let dir = self.uploads_dir();
        ensure_dir(&dir);

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
            Err(err) => return Err(StorageError::Internal(err.to_string())),
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
            Err(err) => return Err(StorageError::Internal(err.to_string())),
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
            Err(err) => return Err(StorageError::Internal(err.to_string())),
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
            .map_err(|err| StorageError::Internal(err.to_string()))?;
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
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };

        let upload_size_bytes = file
            .metadata()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?
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
                    .map_err(|err| StorageError::Internal(err.to_string()))?;
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
                        .map_err(|err| StorageError::Internal(err.to_string()))?;
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
                    .map_err(|err| StorageError::Internal(err.to_string()))?;
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
        ensure_dir(&dest_dir);

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
            .map_err(|err| StorageError::Internal(err.to_string()))?;
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
            Err(err) => Err(StorageError::Internal(err.to_string())),
        }
    }

    async fn delete_blob(&self, digest: &Digest) -> Result<(), StorageError> {
        let path = self.blob_path(digest);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(StorageError::NotFound),
            Err(err) => Err(StorageError::Internal(err.to_string())),
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
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };
        serde_json::from_slice::<Vec<ReferrerDescriptor>>(&bytes)
            .map_err(|err| StorageError::Internal(err.to_string()))
    }

    async fn add_referrer(
        &self,
        name: &str,
        subject: &Digest,
        descriptor: ReferrerDescriptor,
    ) -> Result<(), StorageError> {
        let _lock = self.referrer_lock_shard(name, subject).lock().await;
        let dir = self.root.join("repos").join(name).join("referrers");
        ensure_dir(&dir);

        let path = self.referrers_path(name, subject);
        let mut existing = self.list_referrers(name, subject).await?;
        if !existing.iter().any(|d| d.digest == descriptor.digest) {
            existing.push(descriptor);
        }

        let bytes =
            serde_json::to_vec(&existing).map_err(|err| StorageError::Internal(err.to_string()))?;
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
                .map_err(|err| StorageError::Internal(err.to_string()))?;
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
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };

        let maybe_subject = crate::manifest_refs::extract_subject_digest(&bytes).map_err(|e| {
            StorageError::Internal(format!(
                "cannot delete manifest with malformed structure: {e}"
            ))
        })?;

        match tokio::fs::remove_file(&manifest_path).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        }

        // Remove any tags pointing to this digest.
        let digest_str = digest.as_str();
        for path in self.list_tag_files(name).await? {
            let content = match tokio::fs::read_to_string(&path).await {
                Ok(s) => s,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(StorageError::Internal(err.to_string())),
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn tmp_fs_root() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "registry-rust-fsstorage-test-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&p).expect("create temp fs_root");
        p
    }

    fn write_file(path: &Path, bytes: &[u8]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent dirs");
        }
        std::fs::write(path, bytes).expect("write file");
    }

    #[tokio::test]
    async fn referrers_add_list_remove_and_delete_manifest() {
        let root = tmp_fs_root();
        let storage = FsStorage::new(root.clone(), 1024 * 1024);

        let subject = Digest::parse(
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        )
        .unwrap();
        let ref1 = Digest::parse(
            "sha256:2222222222222222222222222222222222222222222222222222222222222222",
        )
        .unwrap();
        let ref2 = Digest::parse(
            "sha256:3333333333333333333333333333333333333333333333333333333333333333",
        )
        .unwrap();

        let desc1 = ReferrerDescriptor {
            media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
            digest: ref1.as_str().to_string(),
            size: 100,
            artifact_type: Some("application/vnd.example.sbom.v1".to_string()),
            annotations: None,
        };

        let desc2 = ReferrerDescriptor {
            media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
            digest: ref2.as_str().to_string(),
            size: 200,
            artifact_type: Some("application/vnd.example.sig.v1".to_string()),
            annotations: None,
        };

        // Add both referrers
        storage
            .add_referrer("testrepo", &subject, desc1)
            .await
            .expect("add ref1");
        storage
            .add_referrer("testrepo", &subject, desc2)
            .await
            .expect("add ref2");

        let list = storage
            .list_referrers("testrepo", &subject)
            .await
            .expect("list referrers");
        assert_eq!(list.len(), 2);

        // Remove ref1 directly
        storage
            .remove_referrer("testrepo", &subject, &ref1)
            .await
            .expect("remove ref1");
        let list = storage
            .list_referrers("testrepo", &subject)
            .await
            .expect("list referrers");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].digest, ref2.as_str());

        // Put a manifest for ref2 that declares subject
        let manifest_ref2 = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "subject": {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": subject.as_str(),
                "size": 500
            }
        });
        let bytes = serde_json::to_vec(&manifest_ref2).unwrap();
        storage
            .put_manifest("testrepo", &ref2, bytes.into())
            .await
            .expect("put manifest");

        // Delete ref2 manifest -> should remove from referrers
        storage
            .delete_manifest("testrepo", &ref2)
            .await
            .expect("delete manifest");
        let list = storage
            .list_referrers("testrepo", &subject)
            .await
            .expect("list referrers");
        assert_eq!(list.len(), 0);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn head_and_open_blob_fall_back_to_quarantine() {
        let root = tmp_fs_root();
        let storage = FsStorage::new(root.clone(), 1024 * 1024);

        let digest = Digest::parse(
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .expect("valid digest");

        let content = b"hello from quarantine";
        let qpath = storage.quarantine_blob_path(&digest);
        write_file(&qpath, content);

        let meta = storage.head_blob(&digest).await.expect("head_blob");
        assert_eq!(meta.size, content.len() as u64);

        let (meta, mut reader) = storage.open_blob(&digest).await.expect("open_blob");
        assert_eq!(meta.size, content.len() as u64);

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.expect("read blob");
        assert_eq!(buf, content);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn open_blob_prefers_live_over_quarantine() {
        let root = tmp_fs_root();
        let storage = FsStorage::new(root.clone(), 1024 * 1024);

        let digest = Digest::parse(
            "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        )
        .expect("valid digest");

        let live_content = b"live";
        let quarantine_content = b"quarantine";

        write_file(&storage.blob_path(&digest), live_content);
        write_file(&storage.quarantine_blob_path(&digest), quarantine_content);

        let (meta, mut reader) = storage.open_blob(&digest).await.expect("open_blob");
        assert_eq!(meta.size, live_content.len() as u64);

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.expect("read blob");
        assert_eq!(buf, live_content);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn delete_manifest_fails_safe_on_malformed_manifest() {
        let root = tmp_fs_root();
        let storage = FsStorage::new(root.clone(), 1024 * 1024);
        let repo = "library/delete-malformed";
        let digest = Digest::parse(
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        )
        .unwrap();

        // Write a malformed manifest on disk
        let malformed_manifest = serde_json::json!({
            "schemaVersion": 2,
            "subject": { "digest": "sha256:invalid-subject-hex" }
        });
        storage
            .put_manifest(
                repo,
                &digest,
                serde_json::to_vec(&malformed_manifest).unwrap().into(),
            )
            .await
            .unwrap();
        storage.set_tag(repo, "tag1", &digest).await.unwrap();

        // Attempt deletion
        let err = storage
            .delete_manifest(repo, &digest)
            .await
            .expect_err("should abort on malformed manifest");
        match err {
            StorageError::Internal(msg) => {
                assert!(msg.contains("malformed"));
            }
            other => panic!("expected StorageError::Internal, got {other:?}"),
        }

        // Verify manifest and tag are still present (not mutated)
        assert!(storage.get_manifest(repo, &digest).await.is_ok());
        assert_eq!(storage.resolve_tag(repo, "tag1").await.unwrap(), digest);

        let _ = std::fs::remove_dir_all(&root);
    }
}
