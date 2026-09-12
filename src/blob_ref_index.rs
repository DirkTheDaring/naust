use crate::{manifest_refs::parse_manifest_refs, registry::digest::Digest, storage::StorageError};
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

    #[error("ref-index not found at {0}")]
    NotFound(PathBuf),
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

#[derive(Debug, Default)]
struct DiscoveredRepoData {
    roots: Vec<Digest>,
    edges: Vec<(Vec<u8>, Vec<u8>)>,
    tags: Vec<(Vec<u8>, Vec<u8>)>,
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

    /// Open an existing index if it exists on disk, returning NotFound without creating files if absent.
    pub fn open_existing(path: &Path) -> Result<Self, RefIndexError> {
        if !path.exists() {
            return Err(RefIndexError::NotFound(path.to_path_buf()));
        }
        Self::open(path.to_path_buf())
    }

    /// Check health of an index on disk without creating files if absent.
    pub fn check_path_health(path: &Path) -> Result<(), RefIndexError> {
        if !path.exists() {
            return Err(RefIndexError::NotFound(path.to_path_buf()));
        }
        let idx = Self::open(path.to_path_buf())?;
        idx.check_health()
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
        storage: &(impl crate::storage::BlobRefIndexStoragePort + ?Sized),
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
        storage: &(impl crate::storage::BlobRefIndexStoragePort + ?Sized),
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
        storage: &(impl crate::storage::BlobRefIndexStoragePort + ?Sized),
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
            Some(r) => Some(r),
            None => self
                .tag_to_root
                .get(&key)?
                .and_then(|v| std::str::from_utf8(&v).ok().map(|s| s.to_string()))
                .and_then(|s| Digest::parse(&s).ok()),
        };

        self.tag_to_root.insert(&key, new_val)?;

        if let Some(p) = prev {
            self.dec_root_count(p.as_str().as_bytes())?;
        }
        self.inc_root_count(new_root.as_str().as_bytes())?;

        self.db.flush()?;
        Ok(())
    }

    pub async fn sync_repo_manifests_and_tags(
        &self,
        storage: &(impl crate::storage::BlobRefIndexStoragePort + ?Sized),
        repo: &str,
    ) -> Result<(), RefIndexError> {
        // Phase 1: Read-only storage discovery (all-or-nothing; zero index mutations on error)
        let staged = Self::discover_repo_manifests_and_tags(storage, repo).await?;

        // Phase 2: Index application (performs sled operations, no backend I/O)
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

        // 2. Increment root counts and apply DAG edges for discovered manifests.
        // Root occurrence order and multiplicity are preserved; count inflation on repeated
        // successful syncs remains unresolved in this narrow slice and is explicitly documented.
        for digest in staged.roots {
            self.inc_root_count(digest.as_str().as_bytes())?;
        }
        for (child, parent) in staged.edges {
            self.add_parent(&child, &parent)?;
        }

        // 3. Insert discovered tags into tag_to_root
        for (tag_k, digest_bytes) in staged.tags {
            self.tag_to_root.insert(tag_k, digest_bytes.as_slice())?;
        }

        self.db.flush()?;
        Ok(())
    }

    async fn discover_repo_manifests_and_tags(
        storage: &(impl crate::storage::BlobRefIndexStoragePort + ?Sized),
        repo: &str,
    ) -> Result<DiscoveredRepoData, RefIndexError> {
        let mut roots: Vec<Digest> = Vec::new();
        let mut edges: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut tags: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        // 1. Enumerate all stored manifests in this repo with token cycle detection.
        // Memory growth across distinct continuation tokens and staged data remains unbounded
        // in this slice; arbitrary page caps are deliberately deferred.
        let mut manifest_token: Option<String> = None;
        let mut seen_manifest_tokens: HashSet<String> = HashSet::new();

        loop {
            let (manifests, next_tok) = storage
                .list_manifest_digests_page(repo, manifest_token.as_deref(), 128)
                .await?;

            for digest in manifests {
                roots.push(digest.clone());

                // Perform recursive DAG discovery for this root.
                // Traversal queue, visited set, and refs_cache are scoped per root, preserving
                // exact ingest_root semantics and key identity (digest.hex().to_string()).
                let mut queue: VecDeque<Digest> = VecDeque::new();
                queue.push_back(digest.clone());

                let mut visited: HashSet<String> = HashSet::new();
                let mut refs_cache: HashMap<String, Option<crate::manifest_refs::ManifestRefs>> =
                    HashMap::new();

                while let Some(cur_digest) = queue.pop_front() {
                    if !visited.insert(cur_digest.hex().to_string()) {
                        continue;
                    }

                    let digest_hex = cur_digest.hex().to_string();
                    let refs = if let Some(v) = refs_cache.get(&digest_hex) {
                        match v.clone() {
                            Some(r) => r,
                            None => continue,
                        }
                    } else {
                        let (_meta, bytes) = match storage.get_manifest(repo, &cur_digest).await {
                            Ok(v) => v,
                            // Tolerated NotFound: preserve existing semantics where missing roots or
                            // child manifests are cached as absent and skipped without failing discovery.
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
                        edges.push((
                            child_blob.as_str().as_bytes().to_vec(),
                            cur_digest.as_str().as_bytes().to_vec(),
                        ));
                    }

                    // child manifest -> parent manifest
                    // The parent edge to the child manifest is retained even if the child manifest
                    // is subsequently found to be missing from storage.
                    for child_manifest in refs.manifest_references() {
                        edges.push((
                            child_manifest.as_str().as_bytes().to_vec(),
                            cur_digest.as_str().as_bytes().to_vec(),
                        ));
                        queue.push_back(child_manifest.clone());
                    }
                }
            }

            match next_tok {
                Some(tok) => {
                    if !seen_manifest_tokens.insert(tok.clone()) {
                        return Err(StorageError::backend(format!(
                            "pagination cycle detected on continuation token '{tok}' in repository '{repo}'"
                        ))
                        .into());
                    }
                    manifest_token = Some(tok);
                }
                None => break,
            }
        }

        // 2. Enumerate tags in this repo with independent token cycle detection
        let mut tag_token: Option<String> = None;
        let mut seen_tag_tokens: HashSet<String> = HashSet::new();

        loop {
            let (page_tags, next_tok) = storage
                .list_tags_page(repo, tag_token.as_deref(), 128)
                .await?;

            for (tag, digest) in page_tags {
                tags.push((tag_key(repo, &tag), digest.as_str().as_bytes().to_vec()));
            }

            match next_tok {
                Some(tok) => {
                    if !seen_tag_tokens.insert(tok.clone()) {
                        return Err(StorageError::backend(format!(
                            "pagination cycle detected on continuation token '{tok}' in repository '{repo}'"
                        ))
                        .into());
                    }
                    tag_token = Some(tok);
                }
                None => break,
            }
        }

        Ok(DiscoveredRepoData { roots, edges, tags })
    }

    pub async fn sync_repo_tags(
        &self,
        storage: &(impl crate::storage::BlobRefIndexStoragePort + ?Sized),
        repo: &str,
    ) -> Result<(), RefIndexError> {
        self.sync_repo_manifests_and_tags(storage, repo).await
    }

    pub async fn on_manifest_published(
        &self,
        storage: &(impl crate::storage::BlobRefIndexStoragePort + ?Sized),
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

    pub async fn rebuild(
        &self,
        storage: &(impl crate::storage::BlobRefIndexStoragePort + ?Sized),
    ) -> Result<(), RefIndexError> {
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
        storage: &(impl crate::storage::BlobRefIndexStoragePort + ?Sized),
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
        storage: &(impl crate::storage::BlobRefIndexStoragePort + ?Sized),
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
    use crate::storage::Storage;
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
        manifest_pages:
            Mutex<HashMap<String, Vec<Result<(Vec<Digest>, Option<String>), StorageError>>>>,
        tag_pages: Mutex<
            HashMap<String, Vec<Result<(Vec<(String, Digest)>, Option<String>), StorageError>>>,
        >,
        manifest_faults: Mutex<HashMap<(String, String), StorageError>>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self::default()
        }

        fn add_repo(&self, repo: &str) {
            self.repos.lock().unwrap().insert(repo.to_string());
        }

        fn set_tag_sync(&self, repo: &str, tag: &str, digest: &Digest) {
            self.add_repo(repo);
            self.tags
                .lock()
                .unwrap()
                .entry(repo.to_string())
                .or_default()
                .insert(tag.to_string(), digest.clone());
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

        fn queue_manifest_page(
            &self,
            repo: &str,
            page: Result<(Vec<Digest>, Option<String>), StorageError>,
        ) {
            self.manifest_pages
                .lock()
                .unwrap()
                .entry(repo.to_string())
                .or_default()
                .push(page);
        }

        fn queue_tag_page(
            &self,
            repo: &str,
            page: Result<(Vec<(String, Digest)>, Option<String>), StorageError>,
        ) {
            self.tag_pages
                .lock()
                .unwrap()
                .entry(repo.to_string())
                .or_default()
                .push(page);
        }

        fn inject_manifest_fault(&self, repo: &str, digest: &Digest, err: StorageError) {
            self.manifest_faults
                .lock()
                .unwrap()
                .insert((repo.to_string(), digest.as_str().to_string()), err);
        }
    }

    #[async_trait]
    impl crate::storage::UploadSessionStorage for MockStorage {}

    #[async_trait]
    impl crate::storage::repo_membership::RepositoryBlobMembershipStorage for MockStorage {}

    #[async_trait]
    impl crate::storage::GcStorage for MockStorage {}

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

        async fn is_storage_empty(&self) -> Result<bool, StorageError> {
            Ok(self.repos.lock().unwrap().is_empty())
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
            if let Some(err) = self
                .manifest_faults
                .lock()
                .unwrap()
                .remove(&(name.to_string(), digest.as_str().to_string()))
            {
                return Err(err);
            }
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
            if let Some(queue) = self.manifest_pages.lock().unwrap().get_mut(repo) {
                if !queue.is_empty() {
                    return queue.remove(0);
                }
            }
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
            if let Some(queue) = self.tag_pages.lock().unwrap().get_mut(repo) {
                if !queue.is_empty() {
                    return queue.remove(0);
                }
            }
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

    crate::impl_storage_ports!(MockStorage);
    crate::impl_gc_storage_port!(MockStorage);

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
        let storage = mock.clone();

        let repo = "org/repo";
        let root = d('a');
        let cfg = d('b');
        let layer = d('c');

        mock.set_tag_sync(repo, "latest", &root);
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
        let storage = mock.clone();

        let repo = "org/repo";
        let root_index = d('a');
        let child = d('b');
        let layer = d('c');

        mock.set_tag_sync(repo, "v1", &root_index);
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
        let storage = mock.clone();

        let repo = "org/repo";
        let root = d('a');
        let subject = d('b');
        let blob = d('c');

        mock.set_tag_sync(repo, "artifact", &root);
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
        let storage = mock.clone();

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
        let storage = mock.clone();

        let repo = "org/repo";
        let r1 = d('a');
        let r2 = d('b');
        mock.set_tag_sync(repo, "t1", &r1);
        mock.set_tag_sync(repo, "t2", &r2);
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
        let storage = mock.clone();

        let repo = "org/repo";
        let root = d('a');
        mock.set_tag_sync(repo, "latest", &root);
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
        let storage = mock.clone();

        let repo = "org/repo";
        let root = d('a');
        let layer = d('b');
        mock.set_tag_sync(repo, "latest", &root);
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

    fn snapshot_tree(tree: &sled::Tree) -> Vec<(Vec<u8>, Vec<u8>)> {
        tree.iter()
            .map(|r| r.expect("iter"))
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect()
    }

    #[tokio::test]
    async fn test_sync_repo_first_page_manifest_listing_failure_preserves_populated_index() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let mock = Arc::new(MockStorage::new());
        let storage = mock.clone();

        let repo = "org/repo";
        let other_repo = "org/other";
        let r1 = d('1');
        let r_other = d('9');

        mock.set_tag_sync(repo, "t1", &r1);
        mock.put_manifest_bytes(repo, &r1, image_manifest(&d('2'), &d('3')));

        mock.set_tag_sync(other_repo, "t_other", &r_other);
        mock.put_manifest_bytes(other_repo, &r_other, image_manifest(&d('4'), &d('5')));

        // Populate initial index
        idx.sync_repo_manifests_and_tags(&storage, other_repo)
            .await
            .expect("sync other");
        idx.sync_repo_manifests_and_tags(&storage, repo)
            .await
            .expect("sync repo");

        let tags_before = snapshot_tree(&idx.tag_to_root);
        let roots_before = snapshot_tree(&idx.root_counts);
        let edges_before = snapshot_tree(&idx.rev_edges);
        let meta_before = snapshot_tree(&idx.meta);

        // Inject first-page manifest listing error
        mock.queue_manifest_page(
            repo,
            Err(StorageError::backend("injected manifest page 1 failure")),
        );

        let err = idx
            .sync_repo_manifests_and_tags(&storage, repo)
            .await
            .expect_err("should fail");
        match err {
            RefIndexError::Storage(err) => {
                assert!(err.to_string().contains("injected manifest page 1 failure"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        // Verify byte-for-byte preservation across all trees
        assert_eq!(snapshot_tree(&idx.tag_to_root), tags_before);
        assert_eq!(snapshot_tree(&idx.root_counts), roots_before);
        assert_eq!(snapshot_tree(&idx.rev_edges), edges_before);
        assert_eq!(snapshot_tree(&idx.meta), meta_before);

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn test_sync_repo_later_page_manifest_listing_failure_preserves_index() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let mock = Arc::new(MockStorage::new());
        let storage = mock.clone();

        let repo = "org/repo";
        let r1 = d('1');
        let r2 = d('2');
        mock.put_manifest_bytes(repo, &r1, image_manifest(&d('3'), &d('4')));
        mock.put_manifest_bytes(repo, &r2, image_manifest(&d('5'), &d('6')));

        mock.queue_manifest_page(repo, Ok((vec![r1.clone()], Some("page2".to_string()))));
        mock.queue_manifest_page(
            repo,
            Err(StorageError::backend("injected manifest page 2 failure")),
        );

        let err = idx
            .sync_repo_manifests_and_tags(&storage, repo)
            .await
            .expect_err("should fail on page 2");
        match err {
            RefIndexError::Storage(err) => {
                assert!(err.to_string().contains("injected manifest page 2 failure"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        // r1 was on page 1, but must NOT have been incremented in root_counts
        assert!(
            !idx.root_counts
                .contains_key(r1.as_str().as_bytes())
                .unwrap()
        );
        assert!(
            !idx.root_counts
                .contains_key(r2.as_str().as_bytes())
                .unwrap()
        );
        assert_eq!(snapshot_tree(&idx.rev_edges), Vec::new());

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn test_sync_repo_root_and_recursive_child_read_and_parse_failures() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let mock = Arc::new(MockStorage::new());
        let storage = mock.clone();

        // 1. Root read failure
        let repo1 = "org/repo1";
        let r1 = d('1');
        mock.put_manifest_bytes(repo1, &r1, image_manifest(&d('3'), &d('4')));
        mock.inject_manifest_fault(repo1, &r1, StorageError::backend("root read error"));
        let err = idx
            .sync_repo_manifests_and_tags(&storage, repo1)
            .await
            .expect_err("root read fail");
        match err {
            RefIndexError::Storage(err) => {
                assert!(err.to_string().contains("root read error"));
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert!(
            !idx.root_counts
                .contains_key(r1.as_str().as_bytes())
                .unwrap()
        );

        // 2. Root parse failure
        let repo2 = "org/repo2";
        let r2 = d('2');
        mock.put_manifest_bytes(repo2, &r2, bytes("invalid manifest json".to_string()));
        let err = idx
            .sync_repo_manifests_and_tags(&storage, repo2)
            .await
            .expect_err("root parse fail");
        match err {
            RefIndexError::ManifestParse(_) => {}
            other => panic!("unexpected: {other:?}"),
        }
        assert!(
            !idx.root_counts
                .contains_key(r2.as_str().as_bytes())
                .unwrap()
        );

        // 3. Recursive child read failure
        let repo3 = "org/repo3";
        let r3 = d('3');
        let child3 = d('c');
        mock.put_manifest_bytes(repo3, &r3, index_manifest(&child3));
        mock.inject_manifest_fault(repo3, &child3, StorageError::backend("child read error"));
        let err = idx
            .sync_repo_manifests_and_tags(&storage, repo3)
            .await
            .expect_err("child read fail");
        match err {
            RefIndexError::Storage(err) => {
                assert!(err.to_string().contains("child read error"));
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert!(
            !idx.root_counts
                .contains_key(r3.as_str().as_bytes())
                .unwrap()
        );
        assert_eq!(snapshot_tree(&idx.rev_edges), Vec::new());

        // 4. Recursive child parse failure
        let repo4 = "org/repo4";
        let r4 = d('4');
        let child4 = d('d');
        mock.put_manifest_bytes(repo4, &r4, index_manifest(&child4));
        mock.put_manifest_bytes(repo4, &child4, bytes("invalid child json".to_string()));
        let err = idx
            .sync_repo_manifests_and_tags(&storage, repo4)
            .await
            .expect_err("child parse fail");
        match err {
            RefIndexError::ManifestParse(_) => {}
            other => panic!("unexpected: {other:?}"),
        }
        assert!(
            !idx.root_counts
                .contains_key(r4.as_str().as_bytes())
                .unwrap()
        );
        assert_eq!(snapshot_tree(&idx.rev_edges), Vec::new());

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn test_sync_repo_first_and_later_tag_page_failures_preserve_index() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let mock = Arc::new(MockStorage::new());
        let storage = mock.clone();

        let repo = "org/repo";
        let r1 = d('1');
        mock.put_manifest_bytes(repo, &r1, image_manifest(&d('2'), &d('3')));

        // Case A: First tag page failure
        mock.queue_tag_page(repo, Err(StorageError::backend("tag page 1 failure")));
        let err = idx
            .sync_repo_manifests_and_tags(&storage, repo)
            .await
            .expect_err("tag page 1 fail");
        match err {
            RefIndexError::Storage(err) => {
                assert!(err.to_string().contains("tag page 1 failure"));
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert!(
            !idx.root_counts
                .contains_key(r1.as_str().as_bytes())
                .unwrap()
        );
        assert_eq!(snapshot_tree(&idx.tag_to_root), Vec::new());

        // Case B: Later tag page failure
        mock.queue_tag_page(
            repo,
            Ok((
                vec![("t1".to_string(), r1.clone())],
                Some("page2".to_string()),
            )),
        );
        mock.queue_tag_page(repo, Err(StorageError::backend("tag page 2 failure")));
        let err = idx
            .sync_repo_manifests_and_tags(&storage, repo)
            .await
            .expect_err("tag page 2 fail");
        match err {
            RefIndexError::Storage(err) => {
                assert!(err.to_string().contains("tag page 2 failure"));
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert!(
            !idx.root_counts
                .contains_key(r1.as_str().as_bytes())
                .unwrap()
        );
        assert_eq!(snapshot_tree(&idx.tag_to_root), Vec::new());

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn test_sync_repo_token_cycles_detected_in_both_pagination_streams() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let mock = Arc::new(MockStorage::new());
        let storage = mock.clone();

        let repo = "org/repo";
        let r1 = d('1');
        let r2 = d('2');
        let r3 = d('3');
        mock.put_manifest_bytes(repo, &r1, image_manifest(&d('a'), &d('b')));
        mock.put_manifest_bytes(repo, &r2, image_manifest(&d('c'), &d('d')));
        mock.put_manifest_bytes(repo, &r3, image_manifest(&d('e'), &d('f')));

        // 1. Manifest immediate cycle: tok_a -> tok_a
        mock.queue_manifest_page(repo, Ok((vec![r1.clone()], Some("tok_a".to_string()))));
        mock.queue_manifest_page(repo, Ok((vec![r2.clone()], Some("tok_a".to_string()))));
        let err = idx
            .sync_repo_manifests_and_tags(&storage, repo)
            .await
            .expect_err("immediate cycle");
        match err {
            RefIndexError::Storage(err) => {
                assert!(err.to_string().contains("pagination cycle detected on continuation token 'tok_a' in repository 'org/repo'"));
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(snapshot_tree(&idx.root_counts), Vec::new());

        // 2. Manifest multi-token cycle: tok_1 -> tok_2 -> tok_1
        mock.queue_manifest_page(repo, Ok((vec![r1.clone()], Some("tok_1".to_string()))));
        mock.queue_manifest_page(repo, Ok((vec![r2.clone()], Some("tok_2".to_string()))));
        mock.queue_manifest_page(repo, Ok((vec![r3.clone()], Some("tok_1".to_string()))));
        let err = idx
            .sync_repo_manifests_and_tags(&storage, repo)
            .await
            .expect_err("multi cycle");
        match err {
            RefIndexError::Storage(err) => {
                assert!(err.to_string().contains("pagination cycle detected on continuation token 'tok_1' in repository 'org/repo'"));
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(snapshot_tree(&idx.root_counts), Vec::new());

        // 3. Tag immediate cycle: tag_tok_a -> tag_tok_a
        mock.queue_tag_page(
            repo,
            Ok((
                vec![("t1".to_string(), r1.clone())],
                Some("tag_tok_a".to_string()),
            )),
        );
        mock.queue_tag_page(
            repo,
            Ok((
                vec![("t2".to_string(), r2.clone())],
                Some("tag_tok_a".to_string()),
            )),
        );
        let err = idx
            .sync_repo_manifests_and_tags(&storage, repo)
            .await
            .expect_err("tag immediate cycle");
        match err {
            RefIndexError::Storage(err) => {
                assert!(err.to_string().contains("pagination cycle detected on continuation token 'tag_tok_a' in repository 'org/repo'"));
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(snapshot_tree(&idx.tag_to_root), Vec::new());

        // 4. Tag multi-token cycle: tag_tok_1 -> tag_tok_2 -> tag_tok_1
        mock.queue_tag_page(
            repo,
            Ok((
                vec![("t1".to_string(), r1.clone())],
                Some("tag_tok_1".to_string()),
            )),
        );
        mock.queue_tag_page(
            repo,
            Ok((
                vec![("t2".to_string(), r2.clone())],
                Some("tag_tok_2".to_string()),
            )),
        );
        mock.queue_tag_page(
            repo,
            Ok((
                vec![("t3".to_string(), r3.clone())],
                Some("tag_tok_1".to_string()),
            )),
        );
        let err = idx
            .sync_repo_manifests_and_tags(&storage, repo)
            .await
            .expect_err("tag multi cycle");
        match err {
            RefIndexError::Storage(err) => {
                assert!(err.to_string().contains("pagination cycle detected on continuation token 'tag_tok_1' in repository 'org/repo'"));
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(snapshot_tree(&idx.tag_to_root), Vec::new());

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn test_sync_repo_tolerates_missing_root_and_missing_child_retaining_parent_edges() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let mock = Arc::new(MockStorage::new());
        let storage = mock.clone();

        let repo = "org/repo";
        let missing_root = d('1');
        let parent_root = d('2');
        let missing_child = d('3');

        // missing_root is in manifest listing but get_manifest returns NotFound
        mock.queue_manifest_page(
            repo,
            Ok((vec![missing_root.clone(), parent_root.clone()], None)),
        );

        // parent_root references missing_child
        mock.put_manifest_bytes(repo, &parent_root, index_manifest(&missing_child));
        // missing_child is NOT put into manifests -> get_manifest returns NotFound

        idx.sync_repo_manifests_and_tags(&storage, repo)
            .await
            .expect("should tolerate NotFound roots and children");

        // Tolerated NotFound root is counted in root_counts matching existing line 510 behavior
        assert!(
            idx.root_counts
                .contains_key(missing_root.as_str().as_bytes())
                .unwrap()
        );
        assert!(
            idx.root_counts
                .contains_key(parent_root.as_str().as_bytes())
                .unwrap()
        );

        // The parent edge to missing_child is retained even though missing_child was absent
        let parents_raw = idx
            .rev_edges
            .get(missing_child.as_str().as_bytes())
            .unwrap()
            .expect("parent edge retained");
        let parents = decode_parent_list(&parents_raw).expect("decode");
        assert_eq!(parents, vec![parent_root.as_str().to_string()]);

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn test_sync_repo_successful_recursive_dag_and_tag_sync() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        idx.meta
            .insert(META_SCHEMA_VERSION, encode_u32(SCHEMA_VERSION))
            .expect("meta");
        idx.meta.insert(META_STATE, META_STATE_READY).expect("meta");

        let mock = Arc::new(MockStorage::new());
        let storage = mock.clone();

        let repo = "org/repo";
        let root_index = d('a');
        let child_manifest = d('b');
        let config = d('c');
        let layer = d('d');

        mock.set_tag_sync(repo, "latest", &root_index);
        mock.put_manifest_bytes(repo, &root_index, index_manifest(&child_manifest));
        mock.put_manifest_bytes(repo, &child_manifest, image_manifest(&config, &layer));

        idx.sync_repo_manifests_and_tags(&storage, repo)
            .await
            .expect("successful sync");

        assert!(
            idx.root_counts
                .contains_key(root_index.as_str().as_bytes())
                .unwrap()
        );
        let tag_val = idx
            .tag_to_root
            .get(tag_key(repo, "latest"))
            .unwrap()
            .unwrap();
        assert_eq!(tag_val.as_ref(), root_index.as_str().as_bytes());

        // Reverse edges are navigable and blobs are reachable
        assert!(idx.is_blob_referenced(&layer).expect("layer reachable"));
        assert!(idx.is_blob_referenced(&config).expect("config reachable"));

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn test_sync_repo_repeated_success_count_behavior_documented() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let mock = Arc::new(MockStorage::new());
        let storage = mock.clone();

        let repo = "org/repo";
        let r1 = d('1');
        mock.put_manifest_bytes(repo, &r1, image_manifest(&d('2'), &d('3')));

        // Run 1: count becomes 1
        idx.sync_repo_manifests_and_tags(&storage, repo)
            .await
            .expect("run 1");
        let count1 = decode_u64(
            &idx.root_counts
                .get(r1.as_str().as_bytes())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(count1, 1);

        // Run 2: existing behavior increments count to 2
        idx.sync_repo_manifests_and_tags(&storage, repo)
            .await
            .expect("run 2");
        let count2 = decode_u64(
            &idx.root_counts
                .get(r1.as_str().as_bytes())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(count2, 2);

        // Run 3: count increments to 3
        idx.sync_repo_manifests_and_tags(&storage, repo)
            .await
            .expect("run 3");
        let count3 = decode_u64(
            &idx.root_counts
                .get(r1.as_str().as_bytes())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(count3, 3);

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn test_sync_repo_failure_followed_by_success_matches_single_success_from_identical_state()
     {
        let path_a = temp_index_path();
        let path_b = temp_index_path();
        let idx_a = BlobRefIndex::open(path_a.clone()).expect("open a");
        let idx_b = BlobRefIndex::open(path_b.clone()).expect("open b");

        let mock_a = Arc::new(MockStorage::new());
        let mock_b = Arc::new(MockStorage::new());

        let repo = "org/repo";
        let r1 = d('1');
        let r2 = d('2');
        mock_a.set_tag_sync(repo, "v1", &r1);
        mock_a.put_manifest_bytes(repo, &r1, image_manifest(&d('3'), &d('4')));
        mock_a.put_manifest_bytes(repo, &r2, image_manifest(&d('5'), &d('6')));

        mock_b.set_tag_sync(repo, "v1", &r1);
        mock_b.put_manifest_bytes(repo, &r1, image_manifest(&d('3'), &d('4')));
        mock_b.put_manifest_bytes(repo, &r2, image_manifest(&d('5'), &d('6')));

        // Path A: clean single success
        idx_a
            .sync_repo_manifests_and_tags(&mock_a, repo)
            .await
            .expect("sync a");

        // Path B: failure on page 2, followed by clean success
        mock_b.queue_manifest_page(repo, Ok((vec![r1.clone()], Some("page2".to_string()))));
        mock_b.queue_manifest_page(repo, Err(StorageError::backend("transient page 2 error")));
        idx_b
            .sync_repo_manifests_and_tags(&mock_b, repo)
            .await
            .expect_err("expected failure");

        // Now run successful sync on path B
        idx_b
            .sync_repo_manifests_and_tags(&mock_b, repo)
            .await
            .expect("retry b success");

        // Compare all trees: identical state
        assert_eq!(
            snapshot_tree(&idx_a.tag_to_root),
            snapshot_tree(&idx_b.tag_to_root)
        );
        assert_eq!(
            snapshot_tree(&idx_a.root_counts),
            snapshot_tree(&idx_b.root_counts)
        );
        assert_eq!(
            snapshot_tree(&idx_a.rev_edges),
            snapshot_tree(&idx_b.rev_edges)
        );

        let _ = std::fs::remove_dir_all(path_a);
        let _ = std::fs::remove_dir_all(path_b);
    }

    #[tokio::test]
    async fn test_sync_repo_rebuild_failure_retains_building_state() {
        let path = temp_index_path();
        let idx = BlobRefIndex::open(path.clone()).expect("open");
        let mock = Arc::new(MockStorage::new());
        let storage = mock.clone();

        let repo = "org/repo";
        let r1 = d('1');
        mock.set_tag_sync(repo, "latest", &r1);
        mock.put_manifest_bytes(repo, &r1, image_manifest(&d('2'), &d('3')));

        // Initial rebuild succeeds -> READY
        idx.rebuild(&storage).await.expect("initial rebuild");
        idx.check_health().expect("healthy");

        // Second rebuild encounters discovery failure
        mock.queue_manifest_page(repo, Err(StorageError::backend("storage unavailable")));
        idx.rebuild(&storage).await.expect_err("rebuild fails");

        // State remains BUILDING; health check fails
        let state = idx.meta.get(META_STATE).unwrap().unwrap();
        assert_eq!(state.as_ref(), META_STATE_BUILDING);

        let err = idx.check_health().expect_err("should be corrupt/building");
        match err {
            RefIndexError::Corrupt(msg) => {
                assert!(msg.contains("index not ready (previous rebuild incomplete?)"));
            }
            other => panic!("unexpected: {other:?}"),
        }

        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn test_sync_repo_real_fs_promoted_listing_failure_preserves_populated_index() {
        use crate::storage::fs::FsStorage;

        let index_dir = temp_index_path();
        let idx = BlobRefIndex::open(index_dir.clone()).expect("open index");
        idx.meta
            .insert(META_SCHEMA_VERSION, encode_u32(SCHEMA_VERSION))
            .expect("set schema");
        idx.meta
            .insert(META_STATE, META_STATE_READY)
            .expect("set state");

        let storage_dir = tempfile::tempdir().expect("create storage dir");
        let root = storage_dir.path().join("storage-root");
        // Distinctive limit of 1 entry to induce promoted-listing failure
        let storage = FsStorage::try_new_with_limits(
            root.clone(),
            1024 * 1024,
            storage_fs::DirEnumerationLimits::new(1, 100_000),
        )
        .expect("create fs storage");

        let unaffected_repo = "unaffected/repo";
        let target_repo = "target/repo";

        let r_unaff = d('a');
        let b_unaff = d('1');
        let m_unaff = image_manifest(&b_unaff, &d('2'));

        let r_target_1 = d('b');
        let b_target_1 = d('3');
        let m_target_1 = image_manifest(&b_target_1, &d('4'));

        // Write initial valid manifests and tags
        storage
            .put_manifest(unaffected_repo, &r_unaff, m_unaff)
            .await
            .expect("put unaffected manifest");
        storage
            .set_tag(unaffected_repo, "latest", &r_unaff)
            .await
            .expect("set unaffected tag");

        storage
            .put_manifest(target_repo, &r_target_1, m_target_1)
            .await
            .expect("put target manifest 1");
        storage
            .set_tag(target_repo, "v1", &r_target_1)
            .await
            .expect("set target tag 1");

        // Populate initial index for both repositories
        idx.sync_repo_manifests_and_tags(&storage, unaffected_repo)
            .await
            .expect("sync unaffected repo");
        idx.sync_repo_manifests_and_tags(&storage, target_repo)
            .await
            .expect("sync target repo");

        // Verify initial index is populated across roots, edges, and tags
        assert!(
            !snapshot_tree(&idx.tag_to_root).is_empty(),
            "tag_to_root must not be empty"
        );
        assert!(
            !snapshot_tree(&idx.root_counts).is_empty(),
            "root_counts must not be empty"
        );
        assert!(
            !snapshot_tree(&idx.rev_edges).is_empty(),
            "rev_edges must not be empty"
        );
        assert!(
            idx.is_blob_referenced(&b_unaff).expect("check b_unaff"),
            "unaffected blob must be referenced"
        );
        assert!(
            idx.is_blob_referenced(&b_target_1)
                .expect("check b_target_1"),
            "target blob must be referenced"
        );

        // Snapshot all sled trees before triggering failure
        let tags_before = snapshot_tree(&idx.tag_to_root);
        let roots_before = snapshot_tree(&idx.root_counts);
        let edges_before = snapshot_tree(&idx.rev_edges);
        let pins_before = snapshot_tree(&idx.pins);
        let memberships_before = snapshot_tree(&idx.repo_memberships);
        let meta_before = snapshot_tree(&idx.meta);

        // Add a second manifest to target_repo, exceeding the configured limit of 1 entry
        let r_target_2 = d('c');
        let b_target_2 = d('5');
        let m_target_2 = image_manifest(&b_target_2, &d('6'));
        storage
            .put_manifest(target_repo, &r_target_2, m_target_2)
            .await
            .expect("put target manifest 2");

        // Synchronizing target_repo must encounter promoted-listing budget exhaustion
        let err = idx
            .sync_repo_manifests_and_tags(&storage, target_repo)
            .await
            .expect_err("sync must fail due to limit exceeded in real FsStorage");

        match err {
            RefIndexError::Storage(storage_err) => {
                assert!(
                    storage_err
                        .to_string()
                        .contains("enumeration resource limit exceeded"),
                    "expected budget exhaustion error, got: {storage_err:?}"
                );
            }
            other => panic!("expected RefIndexError::Storage, got {other:?}"),
        }

        // Verify byte-for-byte preservation across all sled trees
        assert_eq!(
            snapshot_tree(&idx.tag_to_root),
            tags_before,
            "tag_to_root must be byte-for-byte preserved"
        );
        assert_eq!(
            snapshot_tree(&idx.root_counts),
            roots_before,
            "root_counts must be byte-for-byte preserved"
        );
        assert_eq!(
            snapshot_tree(&idx.rev_edges),
            edges_before,
            "rev_edges must be byte-for-byte preserved"
        );
        assert_eq!(
            snapshot_tree(&idx.pins),
            pins_before,
            "pins must be byte-for-byte preserved"
        );
        assert_eq!(
            snapshot_tree(&idx.repo_memberships),
            memberships_before,
            "repo_memberships must be byte-for-byte preserved"
        );
        assert_eq!(
            snapshot_tree(&idx.meta),
            meta_before,
            "meta must be byte-for-byte preserved"
        );

        // Verify unaffected repository data and target repository data remain referenced and index remains healthy
        assert!(idx.is_blob_referenced(&b_unaff).expect("check b_unaff"));
        assert!(
            idx.is_blob_referenced(&b_target_1)
                .expect("check b_target_1")
        );
        idx.check_health().expect("check_health must succeed");

        let _ = std::fs::remove_dir_all(index_dir);
    }
}
