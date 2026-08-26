use crate::{
    manifest_refs::parse_manifest_refs,
    registry::digest::Digest,
    storage::{Storage, StorageError},
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const SCHEMA_VERSION: u32 = 1;

const META_SCHEMA_VERSION: &[u8] = b"schema_version";
const META_STATE: &[u8] = b"state";
const META_STATE_READY: &[u8] = b"ready";
const META_STATE_BUILDING: &[u8] = b"building";
const META_STATE_DIRTY: &[u8] = b"dirty";

#[derive(thiserror::Error, Debug)]
pub enum RefIndexError {
    #[error("sled error: {0}")]
    Sled(#[from] sled::Error),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("manifest parse error: {0}")]
    ManifestParse(#[from] crate::manifest_refs::ManifestParseError),

    #[error("ref-index corrupt: {0}")]
    Corrupt(String),
}

#[derive(Clone)]
pub struct BlobRefIndex {
    db: sled::Db,
    meta: sled::Tree,
    tag_to_root: sled::Tree,
    root_counts: sled::Tree,
    rev_edges: sled::Tree,
    pins: sled::Tree,
    repo_memberships: sled::Tree,
    fail_mark_dirty: Arc<std::sync::atomic::AtomicBool>,
    fail_mark_ready: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Clone, Debug, Default)]
pub struct TagRootedRefreshStats {
    pub repos_scanned: u64,
    pub tags_scanned: u64,
    pub roots_ingested: u64,
    pub tags_updated: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PinRecord {
    until_unix_secs: u64,
    reason: String,
}

fn system_time_to_unix_secs(t: SystemTime) -> Option<u64> {
    t.duration_since(UNIX_EPOCH).ok().map(|d| d.as_secs())
}

fn decode_pin_record(bytes: &[u8]) -> Option<PinRecord> {
    serde_json::from_slice(bytes).ok()
}

fn encode_pin_record(rec: &PinRecord) -> Result<Vec<u8>, RefIndexError> {
    serde_json::to_vec(rec).map_err(|e| RefIndexError::Corrupt(format!("invalid pin record: {e}")))
}

fn encode_repo_membership_key(digest: &Digest, repo: &str) -> Vec<u8> {
    let s = digest.as_str();
    let digest_bytes = s.as_bytes();
    let repo_bytes = repo.as_bytes();
    let mut key = Vec::with_capacity(2 + digest_bytes.len() + 4 + repo_bytes.len());
    key.extend_from_slice(&(digest_bytes.len() as u16).to_be_bytes());
    key.extend_from_slice(digest_bytes);
    key.extend_from_slice(&(repo_bytes.len() as u32).to_be_bytes());
    key.extend_from_slice(repo_bytes);
    key
}

fn encode_repo_membership_prefix(digest: &Digest) -> Vec<u8> {
    let s = digest.as_str();
    let digest_bytes = s.as_bytes();
    let mut prefix = Vec::with_capacity(2 + digest_bytes.len());
    prefix.extend_from_slice(&(digest_bytes.len() as u16).to_be_bytes());
    prefix.extend_from_slice(digest_bytes);
    prefix
}

impl BlobRefIndex {
    pub fn open(path: PathBuf) -> Result<Self, RefIndexError> {
        let db = sled::open(path)?;
        let meta = db.open_tree("meta")?;
        let tag_to_root = db.open_tree("tag_to_root")?;
        let root_counts = db.open_tree("root_counts")?;
        let rev_edges = db.open_tree("rev_edges")?;
        let pins = db.open_tree("pins")?;
        let repo_memberships = db.open_tree("repo_memberships")?;

        Ok(Self {
            db,
            meta,
            tag_to_root,
            root_counts,
            rev_edges,
            pins,
            repo_memberships,
            fail_mark_dirty: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            fail_mark_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    // Pin/lease store (for online blob GC safety): best-effort and conservative.
    // - The server owns the sled DB; external tools must not mutate it.
    // - Pinning should never break pushes; callers should treat errors as non-fatal.
    pub fn acquire_pin(
        &self,
        digest: &Digest,
        pin_id: &str,
        until: SystemTime,
        reason: &str,
    ) -> Result<(), RefIndexError> {
        let Some(until_secs) = system_time_to_unix_secs(until) else {
            // System time before UNIX_EPOCH (or otherwise invalid): skip pinning.
            return Ok(());
        };

        let key = format!("{}:{}", digest.as_str(), pin_id);

        if let Some(existing) = self.pins.get(key.as_bytes())? {
            if let Some(old) = decode_pin_record(&existing) {
                // Never shorten an existing pin.
                if old.until_unix_secs >= until_secs {
                    return Ok(());
                }
            }
        }

        let rec = PinRecord {
            until_unix_secs: until_secs,
            reason: reason.to_string(),
        };
        self.pins.insert(key.as_bytes(), encode_pin_record(&rec)?)?;
        self.db.flush()?;
        Ok(())
    }

    pub fn release_pin(&self, digest: &Digest, pin_id: &str) -> Result<bool, RefIndexError> {
        let key = format!("{}:{}", digest.as_str(), pin_id);
        let removed = self.pins.remove(key.as_bytes())?.is_some();
        if removed {
            self.db.flush()?;
        }
        Ok(removed)
    }

    pub fn pin_blob(
        &self,
        digest: &Digest,
        until: SystemTime,
        reason: &str,
    ) -> Result<(), RefIndexError> {
        self.acquire_pin(digest, "default", until, reason)
    }

    pub fn unpin_blob(&self, digest: &Digest, pin_id: &str) -> Result<bool, RefIndexError> {
        self.release_pin(digest, pin_id)
    }

    pub fn is_blob_pinned(&self, digest: &Digest, now: SystemTime) -> Result<bool, RefIndexError> {
        let Some(now_secs) = system_time_to_unix_secs(now) else {
            // Clock backwards / invalid: conservative.
            return Ok(true);
        };

        // Check exact key (backward compatibility with legacy single-key pins)
        let exact_key = digest.as_str();
        if let Some(v) = self.pins.get(exact_key.as_bytes())? {
            if let Some(rec) = decode_pin_record(&v) {
                if rec.until_unix_secs > now_secs {
                    return Ok(true);
                }
            }
        }

        // Check all scoped pin leases starting with "digest:"
        let prefix = format!("{}:", digest.as_str());
        for item in self.pins.scan_prefix(prefix.as_bytes()) {
            let (_k, v) = item?;
            if let Some(rec) = decode_pin_record(&v) {
                if rec.until_unix_secs > now_secs {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    pub fn purge_expired_pins(&self, now: SystemTime) -> Result<u64, RefIndexError> {
        let Some(now_secs) = system_time_to_unix_secs(now) else {
            // If time is invalid, do not purge.
            return Ok(0);
        };

        let mut removed = 0u64;
        for item in self.pins.iter() {
            let (k, v) = item?;
            let Some(rec) = decode_pin_record(&v) else {
                continue;
            };
            if rec.until_unix_secs <= now_secs {
                let _ = self.pins.remove(k);
                removed = removed.saturating_add(1);
            }
        }

        if removed > 0 {
            self.db.flush()?;
        }
        Ok(removed)
    }

    pub fn record_membership(&self, digest: &Digest, repo: &str) -> Result<(), RefIndexError> {
        let key = encode_repo_membership_key(digest, repo);
        self.repo_memberships.insert(&key, &[1u8])?;
        Ok(())
    }

    pub fn remove_membership(&self, digest: &Digest, repo: &str) -> Result<bool, RefIndexError> {
        let key = encode_repo_membership_key(digest, repo);
        let removed = self.repo_memberships.remove(&key)?.is_some();
        Ok(removed)
    }

    pub fn has_any_repo_membership(&self, digest: &Digest) -> Result<bool, RefIndexError> {
        self.check_health()?;
        let prefix = encode_repo_membership_prefix(digest);
        let mut iter = self.repo_memberships.scan_prefix(&prefix);
        Ok(iter.next().is_some())
    }

    pub fn get_membership_count(&self, digest: &Digest) -> Result<usize, RefIndexError> {
        self.check_health()?;
        let prefix = encode_repo_membership_prefix(digest);
        let count = self.repo_memberships.scan_prefix(&prefix).count();
        Ok(count)
    }

    pub fn check_health(&self) -> Result<(), RefIndexError> {
        let v = self
            .meta
            .get(META_SCHEMA_VERSION)?
            .ok_or_else(|| RefIndexError::Corrupt("missing schema_version".to_string()))?;
        let schema = decode_u32(&v)
            .ok_or_else(|| RefIndexError::Corrupt("invalid schema_version".to_string()))?;
        if schema != SCHEMA_VERSION {
            return Err(RefIndexError::Corrupt(format!(
                "unsupported schema_version={schema} (expected {SCHEMA_VERSION})"
            )));
        }

        let state = self
            .meta
            .get(META_STATE)?
            .ok_or_else(|| RefIndexError::Corrupt("missing state".to_string()))?;
        if state.as_ref() == META_STATE_DIRTY {
            return Err(RefIndexError::Corrupt(
                "index marked dirty (rebuild required)".to_string(),
            ));
        }
        if state.as_ref() != META_STATE_READY {
            return Err(RefIndexError::Corrupt(
                "index not ready (previous rebuild incomplete?)".to_string(),
            ));
        }

        // Light-weight sanity check: ensure at least one root_count entry (if any)
        // decodes as u64.
        if let Some(res) = self.root_counts.iter().next() {
            let (_k, v) = res?;
            if decode_u64(&v).is_none() {
                return Err(RefIndexError::Corrupt(
                    "invalid root_counts entry".to_string(),
                ));
            }
        }

        Ok(())
    }

    pub fn mark_dirty(&self) -> Result<(), RefIndexError> {
        if self
            .fail_mark_dirty
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(RefIndexError::Corrupt(
                "injected mark_dirty failure".to_string(),
            ));
        }
        self.meta.insert(META_STATE, META_STATE_DIRTY)?;
        self.db.flush()?;
        Ok(())
    }

    pub fn set_fail_mark_dirty(&self, fail: bool) {
        self.fail_mark_dirty
            .store(fail, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn set_fail_mark_ready(&self, fail: bool) {
        self.fail_mark_ready
            .store(fail, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn mark_ready(&self) -> Result<(), RefIndexError> {
        if self
            .fail_mark_ready
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(RefIndexError::Corrupt(
                "injected mark_ready failure".to_string(),
            ));
        }
        self.meta.insert(META_STATE, META_STATE_READY)?;
        self.db.flush()?;
        Ok(())
    }

    pub fn flush(&self) -> Result<(), RefIndexError> {
        self.db.flush()?;
        Ok(())
    }

    pub async fn ensure_healthy_or_rebuild(
        &self,
        storage: &Arc<dyn Storage>,
        auto_rebuild_on_corruption: bool,
        rebuild_on_start: bool,
    ) -> Result<(), RefIndexError> {
        if rebuild_on_start {
            return self.rebuild(storage).await;
        }

        match self.check_health() {
            Ok(()) => Ok(()),
            Err(RefIndexError::Corrupt(reason)) if auto_rebuild_on_corruption => {
                tracing::warn!(reason, "ref-index: corruption detected; rebuilding");
                self.rebuild(storage).await
            }
            Err(e) => Err(e),
        }
    }

    pub fn is_blob_referenced(&self, digest: &Digest) -> Result<bool, RefIndexError> {
        // If the index isn't healthy, treat it as corrupt.
        self.check_health()?;

        let mut queue: VecDeque<String> = VecDeque::new();
        let mut visited: HashSet<String> = HashSet::new();

        let start = digest.as_str().to_string();
        queue.push_back(start);

        while let Some(cur) = queue.pop_front() {
            if !visited.insert(cur.clone()) {
                continue;
            }

            if self.root_counts.contains_key(cur.as_bytes())? {
                return Ok(true);
            }

            let Some(v) = self.rev_edges.get(cur.as_bytes())? else {
                continue;
            };

            let parents = decode_parent_list(&v)
                .ok_or_else(|| RefIndexError::Corrupt("invalid rev_edges entry".to_string()))?;

            for p in parents {
                if !visited.contains(&p) {
                    if self.root_counts.contains_key(p.as_bytes())? {
                        return Ok(true);
                    }
                    queue.push_back(p);
                }
            }
        }

        Ok(false)
    }

    pub async fn on_tag_mutation(
        &self,
        storage: &Arc<dyn Storage>,
        repo: &str,
        tag: &str,
        new_root: &Digest,
        mutation: &crate::storage::TagMutation,
    ) -> Result<(), RefIndexError> {
        match mutation {
            crate::storage::TagMutation::Unchanged => {
                self.ingest_root(storage, repo, new_root).await?;
                Ok(())
            }
            crate::storage::TagMutation::Created => {
                self.ingest_root(storage, repo, new_root).await?;
                let key = tag_key(repo, tag);
                let new_val = new_root.as_str().as_bytes().to_vec();
                self.tag_to_root.insert(&key, new_val)?;
                self.inc_root_count(new_root.as_str().as_bytes())?;
                self.db.flush()?;
                Ok(())
            }
            crate::storage::TagMutation::Replaced { previous } => {
                self.ingest_root(storage, repo, new_root).await?;
                let key = tag_key(repo, tag);
                let new_val = new_root.as_str().as_bytes().to_vec();
                self.tag_to_root.insert(&key, new_val)?;
                self.dec_root_count(previous.as_str().as_bytes())?;
                self.inc_root_count(new_root.as_str().as_bytes())?;
                self.db.flush()?;
                Ok(())
            }
        }
    }

    pub async fn on_tag_set(
        &self,
        storage: &Arc<dyn Storage>,
        repo: &str,
        tag: &str,
        new_root: &Digest,
        old_root: Option<Digest>,
    ) -> Result<(), RefIndexError> {
        // Ensure the manifest graph is present.
        self.ingest_root(storage, repo, new_root).await?;

        // Update tag mapping + root refcounts.
        let key = tag_key(repo, tag);
        let new_val = new_root.as_str().as_bytes().to_vec();

        // If caller provided old_root, use it. Otherwise, derive from existing tag_to_root.
        let prev = match old_root {
            Some(d) => Some(d.as_str().to_string()),
            None => self
                .tag_to_root
                .get(&key)?
                .and_then(|v| String::from_utf8(v.to_vec()).ok()),
        };

        self.tag_to_root.insert(&key, new_val)?;

        let changed = match prev.as_deref() {
            Some(p) => p != new_root.as_str(),
            None => true,
        };
        if changed {
            if let Some(prev_root) = prev {
                self.dec_root_count(prev_root.as_bytes())?;
            }
            self.inc_root_count(new_root.as_str().as_bytes())?;
        }

        self.db.flush()?;
        Ok(())
    }

    pub async fn sync_repo_manifests_and_tags(
        &self,
        storage: &Arc<dyn Storage>,
        repo: &str,
    ) -> Result<(), RefIndexError> {
        // 1. Remove all existing tags for this repo from the index.
        let prefix = tag_prefix(repo);
        let existing_tags: Vec<Vec<u8>> = self
            .tag_to_root
            .scan_prefix(prefix)
            .filter_map(|r| r.ok())
            .map(|(k, _v)| k.to_vec())
            .collect();

        for k in existing_tags {
            let _ = self.tag_to_root.remove(k);
        }

        // 2. Bounded pagination of ALL stored manifests in this repo (stored manifests are roots)
        let mut manifest_token: Option<String> = None;
        loop {
            let (manifests, next_tok) = storage
                .list_manifest_digests_page(repo, manifest_token.as_deref(), 128)
                .await?;
            for digest in manifests {
                self.inc_root_count(digest.as_str().as_bytes())?;
                self.ingest_root(storage, repo, &digest).await?;
            }
            match next_tok {
                Some(tok) => manifest_token = Some(tok),
                None => break,
            }
        }

        // 3. Bounded pagination of tags in this repo (tags map alias -> digest)
        let mut tag_token: Option<String> = None;
        loop {
            let (tags, next_tok) = storage
                .list_tags_page(repo, tag_token.as_deref(), 128)
                .await?;
            for (tag, digest) in tags {
                self.tag_to_root
                    .insert(tag_key(repo, &tag), digest.as_str().as_bytes())?;
            }
            match next_tok {
                Some(tok) => tag_token = Some(tok),
                None => break,
            }
        }

        self.db.flush()?;
        Ok(())
    }

    pub async fn sync_repo_tags(
        &self,
        storage: &Arc<dyn Storage>,
        repo: &str,
    ) -> Result<(), RefIndexError> {
        self.sync_repo_manifests_and_tags(storage, repo).await
    }

    pub async fn on_manifest_published(
        &self,
        storage: &Arc<dyn Storage>,
        repo: &str,
        digest: &Digest,
        tag: Option<&str>,
    ) -> Result<(), RefIndexError> {
        self.inc_root_count(digest.as_str().as_bytes())?;
        self.ingest_root(storage, repo, digest).await?;
        if let Some(t) = tag {
            self.tag_to_root
                .insert(tag_key(repo, t), digest.as_str().as_bytes())?;
        }
        self.db.flush()?;
        Ok(())
    }

    pub fn on_tag_deleted(&self, repo: &str, tag: &str) -> Result<(), RefIndexError> {
        self.tag_to_root.remove(tag_key(repo, tag))?;
        self.db.flush()?;
        Ok(())
    }

    pub fn on_manifest_deleted(&self, repo: &str, digest: &Digest) -> Result<(), RefIndexError> {
        let digest_str = digest.as_str();
        self.root_counts.remove(digest_str.as_bytes())?;
        let prefix = tag_prefix(repo);
        let digest_bytes = digest_str.as_bytes();
        let tags_to_remove: Vec<Vec<u8>> = self
            .tag_to_root
            .scan_prefix(prefix)
            .filter_map(|r| r.ok())
            .filter(|(_k, v)| v.as_ref() == digest_bytes)
            .map(|(k, _v)| k.to_vec())
            .collect();
        for k in tags_to_remove {
            let _ = self.tag_to_root.remove(k);
        }
        self.db.flush()?;
        Ok(())
    }

    pub async fn rebuild(&self, storage: &Arc<dyn Storage>) -> Result<(), RefIndexError> {
        self.meta
            .insert(META_SCHEMA_VERSION, encode_u32(SCHEMA_VERSION))?;
        self.meta.insert(META_STATE, META_STATE_BUILDING)?;
        self.db.flush()?;

        self.tag_to_root.clear()?;
        self.root_counts.clear()?;
        self.rev_edges.clear()?;
        self.repo_memberships.clear()?;

        let repos = storage.list_repositories().await?;
        for repo in &repos {
            self.sync_repo_manifests_and_tags(storage, repo).await?;
        }

        // Global bounded pagination over ALL repository blob memberships
        // (Ensures membership-only repositories with zero tags/manifests are fully indexed)
        let mut token: Option<String> = None;
        loop {
            let (page, next_tok) = storage
                .list_all_repo_blob_memberships_page(token.as_deref(), 256)
                .await?;
            for rec in page {
                self.record_membership(&rec.digest, rec.repo.as_str())?;
            }
            match next_tok {
                Some(tok) => token = Some(tok),
                None => break,
            }
        }

        self.meta.insert(META_STATE, META_STATE_READY)?;
        self.db.flush()?;
        Ok(())
    }

    /// Best-effort, conservative refresh for tag-rooted GC.
    ///
    /// This ensures the current on-disk tag roots are represented in the index so
    /// `is_blob_referenced()` won't produce false negatives due to stale/missed updates.
    ///
    /// It is conservative under concurrent writes:
    /// - it ingests reachable manifests for current roots (adds edges)
    /// - it sets tag->root when it differs
    /// - it increments root refcounts for new roots
    /// - it never decrements counts or deletes old mappings (may over-retain, but is safe)
    pub async fn refresh_tag_rooted_conservative(
        &self,
        storage: &Arc<dyn Storage>,
    ) -> Result<TagRootedRefreshStats, RefIndexError> {
        self.check_health()?;

        let mut stats = TagRootedRefreshStats::default();
        let repos = storage.list_repositories().await?;
        for repo in repos {
            stats.repos_scanned += 1;
            let tags = match storage.list_tags(&repo).await {
                Ok(t) => t,
                Err(StorageError::NotFound) => continue,
                Err(e) => return Err(e.into()),
            };

            for tag in tags {
                stats.tags_scanned += 1;
                let root = match storage.resolve_tag(&repo, &tag).await {
                    Ok(d) => d,
                    Err(StorageError::NotFound) => continue,
                    Err(e) => return Err(e.into()),
                };

                self.ingest_root(storage, &repo, &root).await?;
                stats.roots_ingested += 1;

                let key = tag_key(&repo, &tag);
                let new_val = root.as_str().as_bytes().to_vec();

                let cur = self.tag_to_root.get(&key)?;
                let needs_update = cur.as_ref().map(|v| v.as_ref()) != Some(new_val.as_slice());
                if needs_update {
                    self.tag_to_root.insert(&key, new_val)?;
                    self.inc_root_count(root.as_str().as_bytes())?;
                    stats.tags_updated += 1;
                }
            }
        }

        self.db.flush()?;
        Ok(stats)
    }

    async fn ingest_root(
        &self,
        storage: &Arc<dyn Storage>,
        repo: &str,
        root: &Digest,
    ) -> Result<(), RefIndexError> {
        let mut queue: VecDeque<Digest> = VecDeque::new();
        queue.push_back(root.clone());

        let mut visited: HashSet<String> = HashSet::new();

        // Per-repo cache to avoid repeated manifest reads/parses.
        let mut refs_cache: HashMap<String, Option<crate::manifest_refs::ManifestRefs>> =
            HashMap::new();

        while let Some(digest) = queue.pop_front() {
            if !visited.insert(digest.hex().to_string()) {
                continue;
            }

            let digest_hex = digest.hex().to_string();
            let refs = if let Some(v) = refs_cache.get(&digest_hex) {
                match v.clone() {
                    Some(r) => r,
                    None => continue,
                }
            } else {
                let (_meta, bytes) = match storage.get_manifest(repo, &digest).await {
                    Ok(v) => v,
                    Err(StorageError::NotFound) => {
                        refs_cache.insert(digest_hex, None);
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                };

                let parsed = parse_manifest_refs(&bytes)?;
                refs_cache.insert(digest_hex, Some(parsed.clone()));
                parsed
            };

            // child blob -> parent manifest
            for child_blob in refs.blob_references() {
                self.add_parent(child_blob.as_str().as_bytes(), digest.as_str().as_bytes())?;
            }

            // child manifest -> parent manifest
            for child_manifest in refs.manifest_references() {
                self.add_parent(
                    child_manifest.as_str().as_bytes(),
                    digest.as_str().as_bytes(),
                )?;
                queue.push_back(child_manifest.clone());
            }
        }

        Ok(())
    }

    fn add_parent(&self, child_key: &[u8], parent: &[u8]) -> Result<(), RefIndexError> {
        self.rev_edges.update_and_fetch(child_key, |old| {
            let mut parents: Vec<Vec<u8>> = match old {
                Some(v) => decode_parent_list_bytes(v).unwrap_or_default(),
                None => Vec::new(),
            };

            if parents.iter().any(|p| p.as_slice() == parent) {
                return Some(encode_parent_list_bytes(&parents));
            }

            parents.push(parent.to_vec());
            Some(encode_parent_list_bytes(&parents))
        })?;
        Ok(())
    }

    fn inc_root_count(&self, root: &[u8]) -> Result<(), RefIndexError> {
        self.root_counts.update_and_fetch(root, |old| {
            let cur = old.and_then(|v| decode_u64(v)).unwrap_or(0);
            Some(encode_u64(cur.saturating_add(1)))
        })?;
        Ok(())
    }

    fn dec_root_count(&self, root: &[u8]) -> Result<(), RefIndexError> {
        let updated = self.root_counts.update_and_fetch(root, |old| {
            let cur = old.and_then(|v| decode_u64(v)).unwrap_or(0);
            let next = cur.saturating_sub(1);
            if next == 0 {
                None
            } else {
                Some(encode_u64(next))
            }
        })?;

        // If updated == None, key was removed.
        let _ = updated;
        Ok(())
    }
}

fn tag_key(repo: &str, tag: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(repo.len() + 1 + tag.len());
    k.extend_from_slice(repo.as_bytes());
    k.push(0);
    k.extend_from_slice(tag.as_bytes());
    k
}

fn tag_prefix(repo: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(repo.len() + 1);
    p.extend_from_slice(repo.as_bytes());
    p.push(0);
    p
}

fn encode_u64(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn decode_u64(v: &[u8]) -> Option<u64> {
    if v.len() != 8 {
        return None;
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(v);
    Some(u64::from_be_bytes(a))
}

fn encode_u32(v: u32) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn decode_u32(v: &[u8]) -> Option<u32> {
    if v.len() != 4 {
        return None;
    }
    let mut a = [0u8; 4];
    a.copy_from_slice(v);
    Some(u32::from_be_bytes(a))
}

fn encode_parent_list_bytes(parents: &[Vec<u8>]) -> Vec<u8> {
    // newline-separated utf8 digests (sha256:...)
    let mut out: Vec<u8> = Vec::new();
    for (i, p) in parents.iter().enumerate() {
        if i > 0 {
            out.push(b'\n');
        }
        out.extend_from_slice(p);
    }
    out
}

fn decode_parent_list_bytes(v: &[u8]) -> Option<Vec<Vec<u8>>> {
    if v.is_empty() {
        return Some(Vec::new());
    }
    Some(v.split(|b| *b == b'\n').map(|s| s.to_vec()).collect())
}

fn decode_parent_list(v: &[u8]) -> Option<Vec<String>> {
    if v.is_empty() {
        return Some(Vec::new());
    }
    let s = std::str::from_utf8(v).ok()?;
    Some(
        s.split('\n')
            .filter(|p| !p.is_empty())
            .map(|p| p.to_string())
            .collect(),
    )
}

// Keep clippy happy: used by signature clarity.
#[allow(dead_code)]
fn _path_exists(p: &Path) -> bool {
    p.exists()
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::collections::{HashMap, HashSet};
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::time::{Duration, UNIX_EPOCH};
    use tokio::io::AsyncRead;

    #[derive(Default)]
    struct MockStorage {
        repos: Mutex<HashSet<String>>,
        tags: Mutex<HashMap<String, HashMap<String, Digest>>>,
        manifests: Mutex<HashMap<(String, String), Bytes>>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self::default()
        }

        fn add_repo(&self, repo: &str) {
            self.repos.lock().unwrap().insert(repo.to_string());
        }

        fn set_tag(&self, repo: &str, tag: &str, digest: Digest) {
            self.add_repo(repo);
            self.tags
                .lock()
                .unwrap()
                .entry(repo.to_string())
                .or_default()
                .insert(tag.to_string(), digest);
        }

        fn remove_tag(&self, repo: &str, tag: &str) {
            if let Some(m) = self.tags.lock().unwrap().get_mut(repo) {
                m.remove(tag);
            }
        }

        fn remove_manifest(&self, repo: &str, digest: &Digest) {
            self.manifests
                .lock()
                .unwrap()
                .remove(&(repo.to_string(), digest.as_str().to_string()));
        }

        fn put_manifest_bytes(&self, repo: &str, digest: &Digest, bytes: Bytes) {
            self.add_repo(repo);
            self.manifests
                .lock()
                .unwrap()
                .insert((repo.to_string(), digest.as_str().to_string()), bytes);
        }
    }

    #[async_trait]
    impl crate::storage::UploadSessionStorage for MockStorage {}

    #[async_trait]
    impl crate::storage::repo_membership::RepositoryBlobMembershipStorage for MockStorage {}

    #[async_trait]
    impl Storage for MockStorage {
        fn kind(&self) -> &'static str {
            "mock"
        }

        async fn list_repositories(&self) -> Result<Vec<String>, StorageError> {
            let mut v: Vec<String> = self.repos.lock().unwrap().iter().cloned().collect();
            v.sort();
            Ok(v)
        }

        async fn repo_timestamps(
            &self,
            _name: &str,
        ) -> Result<crate::storage::RepoTimestamps, StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn head_blob(
            &self,
            _digest: &Digest,
        ) -> Result<crate::storage::BlobMeta, StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn open_blob(
            &self,
            _digest: &Digest,
        ) -> Result<(crate::storage::BlobMeta, Pin<Box<dyn AsyncRead + Send>>), StorageError>
        {
            Err(StorageError::Unsupported)
        }

        async fn resolve_tag(&self, name: &str, tag: &str) -> Result<Digest, StorageError> {
            self.tags
                .lock()
                .unwrap()
                .get(name)
                .and_then(|m| m.get(tag).cloned())
                .ok_or(StorageError::NotFound)
        }

        async fn list_tags(&self, name: &str) -> Result<Vec<String>, StorageError> {
            let mut v: Vec<String> = self
                .tags
                .lock()
                .unwrap()
                .get(name)
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default();
            v.sort();
            Ok(v)
        }

        async fn head_manifest(
            &self,
            _name: &str,
            _digest: &Digest,
        ) -> Result<crate::storage::ManifestMeta, StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn get_manifest(
            &self,
            name: &str,
            digest: &Digest,
        ) -> Result<(crate::storage::ManifestMeta, Bytes), StorageError> {
            let key = (name.to_string(), digest.as_str().to_string());
            let bytes = self
                .manifests
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .ok_or(StorageError::NotFound)?;
            let meta = crate::storage::ManifestMeta {
                size: bytes.len() as u64,
                media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
            };
            Ok((meta, bytes))
        }

        async fn put_manifest(
            &self,
            _name: &str,
            _digest: &Digest,
            _bytes: Bytes,
        ) -> Result<crate::storage::ManifestMeta, StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn set_tag(
            &self,
            _name: &str,
            _tag: &str,
            _digest: &Digest,
        ) -> Result<(), StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn mutate_tag(
            &self,
            _name: &str,
            _tag: &str,
            _digest: &Digest,
            _policy: crate::storage::TagMutationPolicy,
        ) -> Result<crate::storage::TagMutation, StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn delete_tag(&self, _name: &str, _tag: &str) -> Result<(), StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn list_manifest_digests_page(
            &self,
            repo: &str,
            _continuation_token: Option<&str>,
            _page_limit: usize,
        ) -> Result<(Vec<Digest>, Option<String>), StorageError> {
            let mut res = Vec::new();
            for (r, d_str) in self.manifests.lock().unwrap().keys() {
                if r == repo {
                    if let Ok(d) = Digest::parse(d_str) {
                        res.push(d);
                    }
                }
            }
            Ok((res, None))
        }

        async fn list_tags_page(
            &self,
            repo: &str,
            _continuation_token: Option<&str>,
            _page_limit: usize,
        ) -> Result<(Vec<(String, Digest)>, Option<String>), StorageError> {
            let mut res = Vec::new();
            if let Some(tags_map) = self.tags.lock().unwrap().get(repo) {
                for (t, d) in tags_map {
                    res.push((t.clone(), d.clone()));
                }
            }
            Ok((res, None))
        }

        async fn list_referrers_page(
            &self,
            _repo: &str,
            _subject: &Digest,
            _continuation_token: Option<&str>,
            _page_limit: usize,
        ) -> Result<(Vec<crate::storage::ReferrerDescriptor>, Option<String>), StorageError>
        {
            Ok((Vec::new(), None))
        }

        async fn get_tag_with_version(
            &self,
            repo: &str,
            tag: &str,
        ) -> Result<Option<(Digest, String)>, StorageError> {
            match self.resolve_tag(repo, tag).await {
                Ok(d) => Ok(Some((d, "v1".to_string()))),
                Err(StorageError::NotFound) => Ok(None),
                Err(e) => Err(e),
            }
        }

        async fn delete_tag_conditional(
            &self,
            repo: &str,
            tag: &str,
            _expected_version: Option<&str>,
        ) -> Result<crate::storage::ConditionalDeleteResult, StorageError> {
            match self.resolve_tag(repo, tag).await {
                Ok(_) => {
                    self.remove_tag(repo, tag);
                    Ok(crate::storage::ConditionalDeleteResult::Deleted)
                }
                Err(StorageError::NotFound) => {
                    Ok(crate::storage::ConditionalDeleteResult::NotFound)
                }
                Err(e) => Err(e),
            }
        }

        async fn read_lifecycle_journal(&self, _repo: &str) -> Result<Option<Bytes>, StorageError> {
            Ok(None)
        }

        async fn write_lifecycle_journal(
            &self,
            _repo: &str,
            _data: Bytes,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn delete_lifecycle_journal(&self, _repo: &str) -> Result<(), StorageError> {
            Ok(())
        }

        async fn acquire_repo_lease(
            &self,
            _repo: &str,
            _owner_id: &str,
            _lease_id: &str,
            _ttl_secs: u64,
        ) -> Result<bool, StorageError> {
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
            _repo: &str,
            _owner_id: &str,
            _lease_id: &str,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn create_upload(&self) -> Result<crate::storage::UploadMeta, StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn upload_status(
            &self,
            _uuid: &str,
        ) -> Result<crate::storage::UploadMeta, StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn append_upload(
            &self,
            _uuid: &str,
            _chunk: Bytes,
        ) -> Result<crate::storage::UploadMeta, StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn finalize_upload(
            &self,
            _uuid: &str,
            _digest: &Digest,
        ) -> Result<crate::storage::BlobMeta, StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn abort_upload(&self, _uuid: &str) -> Result<(), StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn delete_blob(&self, _digest: &Digest) -> Result<(), StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn list_referrers(
            &self,
            _name: &str,
            _subject: &Digest,
        ) -> Result<Vec<crate::storage::ReferrerDescriptor>, StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn add_referrer(
            &self,
            _name: &str,
            _subject: &Digest,
            _descriptor: crate::storage::ReferrerDescriptor,
        ) -> Result<(), StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn remove_referrer(
            &self,
            _name: &str,
            _subject: &Digest,
            _referrer: &Digest,
        ) -> Result<(), StorageError> {
            Err(StorageError::Unsupported)
        }

        async fn delete_manifest(&self, _name: &str, _digest: &Digest) -> Result<(), StorageError> {
            Err(StorageError::Unsupported)
        }
    }

    fn temp_index_path() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "registry-rust-ref-index-test-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&p).expect("create temp dir");
        p
    }

    fn d(ch: char) -> Digest {
        let hex: String = std::iter::repeat_n(ch, 64).collect();
        Digest::parse(&format!("sha256:{hex}")).expect("valid sha256")
    }

    fn bytes(s: String) -> Bytes {
        Bytes::from(s.into_bytes())
    }

    fn index_manifest(child: &Digest) -> Bytes {
        bytes(format!(
            "{{\"schemaVersion\":2,\"manifests\":[{{\"digest\":\"{}\"}}]}}",
            child.as_str()
        ))
    }

    fn image_manifest(config: &Digest, layer: &Digest) -> Bytes {
        bytes(format!(
            "{{\"schemaVersion\":2,\"config\":{{\"digest\":\"{}\"}},\"layers\":[{{\"digest\":\"{}\"}}]}}",
            config.as_str(),
            layer.as_str()
        ))
    }

    fn artifact_manifest(subject: &Digest, blob: &Digest) -> Bytes {
        bytes(format!(
            "{{\"schemaVersion\":2,\"subject\":{{\"digest\":\"{}\"}},\"blobs\":[{{\"digest\":\"{}\"}}]}}",
            subject.as_str(),
            blob.as_str()
        ))
    }

    #[tokio::test]
    async fn pins_are_conservative_and_purge_expired() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");

        let digest = d('e');

        let now = UNIX_EPOCH + Duration::from_secs(100);
        let until = UNIX_EPOCH + Duration::from_secs(110);

        idx.pin_blob(&digest, until, "finalize_upload")
            .expect("pin");

        assert!(idx.is_blob_pinned(&digest, now).expect("is_pinned"));
        assert!(
            !idx.is_blob_pinned(&digest, UNIX_EPOCH + Duration::from_secs(111))
                .expect("is_pinned")
        );

        let removed = idx
            .purge_expired_pins(UNIX_EPOCH + Duration::from_secs(111))
            .expect("purge");
        assert_eq!(removed, 1);

        assert!(
            !idx.is_blob_pinned(&digest, UNIX_EPOCH + Duration::from_secs(200))
                .expect("is_pinned")
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn pin_does_not_shorten_existing_pin() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");

        let digest = d('f');

        idx.pin_blob(&digest, UNIX_EPOCH + Duration::from_secs(200), "first")
            .expect("pin");

        // Attempt to shorten to 150; should be ignored.
        idx.pin_blob(&digest, UNIX_EPOCH + Duration::from_secs(150), "shorten")
            .expect("pin");

        assert!(
            idx.is_blob_pinned(&digest, UNIX_EPOCH + Duration::from_secs(160))
                .expect("is_pinned")
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn check_health_before_rebuild_is_corrupt() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");

        let err = idx.check_health().expect_err("should be corrupt");
        match err {
            RefIndexError::Corrupt(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn rebuild_then_check_health_ok_and_references_resolve() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");

        let mock = Arc::new(MockStorage::new());
        let storage: Arc<dyn Storage> = mock.clone();

        let repo = "org/repo";
        let root = d('a');
        let cfg = d('b');
        let layer = d('c');

        mock.set_tag(repo, "latest", root.clone());
        mock.put_manifest_bytes(repo, &root, image_manifest(&cfg, &layer));

        idx.rebuild(&storage).await.expect("rebuild");
        idx.check_health().expect("healthy");

        assert!(idx.is_blob_referenced(&layer).expect("lookup"));
        assert!(idx.is_blob_referenced(&cfg).expect("lookup"));
        assert!(!idx.is_blob_referenced(&d('d')).expect("lookup"));

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn index_manifest_to_child_manifest_traversal_works() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");

        let mock = Arc::new(MockStorage::new());
        let storage: Arc<dyn Storage> = mock.clone();

        let repo = "org/repo";
        let root_index = d('a');
        let child = d('b');
        let layer = d('c');

        mock.set_tag(repo, "v1", root_index.clone());
        mock.put_manifest_bytes(repo, &root_index, index_manifest(&child));
        mock.put_manifest_bytes(repo, &child, image_manifest(&d('d'), &layer));

        idx.rebuild(&storage).await.expect("rebuild");

        assert!(idx.is_blob_referenced(&layer).expect("lookup"));

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn artifact_subject_and_blobs_are_indexed() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let mock = Arc::new(MockStorage::new());
        let storage: Arc<dyn Storage> = mock.clone();

        let repo = "org/repo";
        let root = d('a');
        let subject = d('b');
        let blob = d('c');

        mock.set_tag(repo, "artifact", root.clone());
        mock.put_manifest_bytes(repo, &root, artifact_manifest(&subject, &blob));

        idx.rebuild(&storage).await.expect("rebuild");
        assert!(idx.is_blob_referenced(&blob).expect("lookup"));
        assert!(idx.is_blob_referenced(&subject).expect("lookup"));

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn on_tag_set_is_idempotent_for_same_digest() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let mock = Arc::new(MockStorage::new());
        let storage: Arc<dyn Storage> = mock.clone();

        // Bring index to a healthy state.
        idx.meta
            .insert(META_SCHEMA_VERSION, encode_u32(SCHEMA_VERSION))
            .expect("meta");
        idx.meta.insert(META_STATE, META_STATE_READY).expect("meta");

        let repo = "org/repo";
        let root = d('a');
        mock.put_manifest_bytes(repo, &root, image_manifest(&d('b'), &d('c')));

        idx.on_tag_set(&storage, repo, "latest", &root, None)
            .await
            .expect("set");
        idx.on_tag_set(&storage, repo, "latest", &root, None)
            .await
            .expect("set idempotent");

        let v = idx
            .root_counts
            .get(root.as_str().as_bytes())
            .expect("get")
            .expect("present");
        let n = decode_u64(&v).expect("decode");
        assert_eq!(n, 1, "refcount should not double on same tag->digest");

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn sync_repo_tags_updates_counts_and_removes_deleted_tags() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let mock = Arc::new(MockStorage::new());
        let storage: Arc<dyn Storage> = mock.clone();

        let repo = "org/repo";
        let r1 = d('a');
        let r2 = d('b');
        mock.set_tag(repo, "t1", r1.clone());
        mock.set_tag(repo, "t2", r2.clone());
        mock.put_manifest_bytes(repo, &r1, image_manifest(&d('c'), &d('d')));
        mock.put_manifest_bytes(repo, &r2, image_manifest(&d('e'), &d('f')));

        idx.rebuild(&storage).await.expect("rebuild");

        // 1. Removing tag t2 in storage removes tag alias from tag_to_root, but r2 is still a stored manifest and remains in root_counts.
        mock.remove_tag(repo, "t2");
        idx.sync_repo_tags(&storage, repo).await.expect("sync");

        assert!(
            idx.root_counts
                .contains_key(r1.as_str().as_bytes())
                .expect("contains")
        );
        assert!(
            idx.root_counts
                .contains_key(r2.as_str().as_bytes())
                .expect("contains")
        );
        assert!(
            idx.tag_to_root
                .get(tag_key(repo, "t2"))
                .expect("get")
                .is_none()
        );

        // 2. Removing manifest r2 from storage removes r2 from root_counts on rebuild/sync.
        mock.remove_manifest(repo, &r2);
        idx.rebuild(&storage).await.expect("rebuild");

        assert!(
            idx.root_counts
                .contains_key(r1.as_str().as_bytes())
                .expect("contains")
        );
        assert!(
            !idx.root_counts
                .contains_key(r2.as_str().as_bytes())
                .expect("contains")
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn corruption_detection_and_auto_rebuild() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let mock = Arc::new(MockStorage::new());
        let storage: Arc<dyn Storage> = mock.clone();

        let repo = "org/repo";
        let root = d('a');
        mock.set_tag(repo, "latest", root.clone());
        mock.put_manifest_bytes(repo, &root, image_manifest(&d('b'), &d('c')));

        // Simulate an incomplete prior rebuild.
        idx.meta
            .insert(META_SCHEMA_VERSION, encode_u32(SCHEMA_VERSION))
            .expect("meta");
        idx.meta
            .insert(META_STATE, META_STATE_BUILDING)
            .expect("meta");

        let err = idx
            .ensure_healthy_or_rebuild(&storage, false, false)
            .await
            .expect_err("should refuse when auto rebuild disabled");
        match err {
            RefIndexError::Corrupt(_) => {}
            other => panic!("unexpected: {other:?}"),
        }

        idx.ensure_healthy_or_rebuild(&storage, true, false)
            .await
            .expect("auto rebuild");
        idx.check_health().expect("healthy");

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn corrupted_rev_edges_entry_is_detected() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let mock = Arc::new(MockStorage::new());
        let storage: Arc<dyn Storage> = mock.clone();

        let repo = "org/repo";
        let root = d('a');
        let layer = d('b');
        mock.set_tag(repo, "latest", root.clone());
        mock.put_manifest_bytes(repo, &root, image_manifest(&d('c'), &layer));

        idx.rebuild(&storage).await.expect("rebuild");

        // Corrupt the rev_edges value for the layer.
        idx.rev_edges
            .insert(layer.as_str().as_bytes(), b"\xff\xff\xff")
            .expect("corrupt");

        let err = idx
            .is_blob_referenced(&layer)
            .expect_err("should be corrupt");
        match err {
            RefIndexError::Corrupt(_) => {}
            other => panic!("unexpected: {other:?}"),
        }

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn test_pin_lifecycle_success_removes_pin() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let digest = d('e');
        let op_id = "op-12345";
        let now = SystemTime::now();
        let until = now + Duration::from_secs(3600);

        // 1. Acquire pin before publication
        idx.acquire_pin(&digest, op_id, until, "upload_finalizing")
            .unwrap();
        assert!(idx.is_blob_pinned(&digest, now).unwrap());

        // 2. Publication completes -> release pin
        let released = idx.release_pin(&digest, op_id).unwrap();
        assert!(released);
        assert!(!idx.is_blob_pinned(&digest, now).unwrap());

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn test_pin_lifecycle_failed_commit_retains_protection() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let digest = d('e');
        let op_id = "op-crash-test";
        let now = SystemTime::now();
        let until = now + Duration::from_secs(3600);

        // Acquire pin
        idx.acquire_pin(&digest, op_id, until, "upload_finalizing")
            .unwrap();

        // Simulate crash during commit: pin remains active and protected
        assert!(idx.is_blob_pinned(&digest, now).unwrap());
        assert!(
            idx.is_blob_pinned(&digest, now + Duration::from_secs(1800))
                .unwrap()
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn test_pin_lifecycle_recovery_removes_pin() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let digest = d('e');
        let op_id = "op-recovered";
        let now = SystemTime::now();
        let until = now + Duration::from_secs(3600);

        idx.acquire_pin(&digest, op_id, until, "upload_finalizing")
            .unwrap();
        assert!(idx.is_blob_pinned(&digest, now).unwrap());

        // Recovery succeeds -> releases pin
        idx.release_pin(&digest, op_id).unwrap();
        assert!(!idx.is_blob_pinned(&digest, now).unwrap());

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn test_pin_lifecycle_expired_abandoned_pin_purged() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let digest = d('e');
        let op_id = "op-abandoned";
        let now = SystemTime::now();
        let until = now + Duration::from_secs(10);

        idx.acquire_pin(&digest, op_id, until, "upload_finalizing")
            .unwrap();
        assert!(idx.is_blob_pinned(&digest, now).unwrap());

        // Advance past expiration
        let future = now + Duration::from_secs(20);
        assert!(!idx.is_blob_pinned(&digest, future).unwrap());

        // Purge expired
        let purged = idx.purge_expired_pins(future).unwrap();
        assert_eq!(purged, 1);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn test_pin_lifecycle_idempotent_cleanup() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let digest = d('e');
        let op_id = "op-idempotent";
        let now = SystemTime::now();
        let until = now + Duration::from_secs(100);

        idx.acquire_pin(&digest, op_id, until, "upload_finalizing")
            .unwrap();

        // First release returns true
        assert!(idx.release_pin(&digest, op_id).unwrap());
        // Second release returns false without error
        assert!(!idx.release_pin(&digest, op_id).unwrap());

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn test_pin_lifecycle_unreferenced_blob_becomes_gc_eligible() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        idx.meta
            .insert(META_SCHEMA_VERSION, encode_u32(SCHEMA_VERSION))
            .expect("meta");
        idx.meta.insert(META_STATE, META_STATE_READY).expect("meta");

        let digest = d('e');
        let op_id = "op-gc-eligible";
        let now = SystemTime::now();
        let until = now + Duration::from_secs(10);

        idx.acquire_pin(&digest, op_id, until, "upload_finalizing")
            .unwrap();

        // Release pin upon commit
        idx.release_pin(&digest, op_id).unwrap();

        // Not referenced by any manifest and not pinned -> GC eligible
        assert!(!idx.is_blob_pinned(&digest, now).unwrap());
        assert!(!idx.is_blob_referenced(&digest).unwrap());

        let _ = std::fs::remove_dir_all(path);
    }
}
