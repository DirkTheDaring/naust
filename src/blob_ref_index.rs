use crate::{
    manifest_refs::parse_manifest_refs,
    registry::digest::Digest,
    storage::{Storage, StorageError},
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const SCHEMA_VERSION: u32 = 1;

const META_SCHEMA_VERSION: &[u8] = b"schema_version";
const META_STATE: &[u8] = b"state";
const META_STATE_READY: &[u8] = b"ready";
const META_STATE_BUILDING: &[u8] = b"building";

#[derive(thiserror::Error, Debug)]
pub enum RefIndexError {
    #[error("sled error: {0}")]
    Sled(#[from] sled::Error),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

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
}

impl BlobRefIndex {
    pub fn open(path: PathBuf) -> Result<Self, RefIndexError> {
        let db = sled::open(path)?;
        let meta = db.open_tree("meta")?;
        let tag_to_root = db.open_tree("tag_to_root")?;
        let root_counts = db.open_tree("root_counts")?;
        let rev_edges = db.open_tree("rev_edges")?;

        Ok(Self {
            db,
            meta,
            tag_to_root,
            root_counts,
            rev_edges,
        })
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

    pub async fn sync_repo_tags(
        &self,
        storage: &Arc<dyn Storage>,
        repo: &str,
    ) -> Result<(), RefIndexError> {
        // Remove all existing tags for this repo from the index.
        let prefix = tag_prefix(repo);
        let existing: Vec<(Vec<u8>, Vec<u8>)> = self
            .tag_to_root
            .scan_prefix(prefix)
            .filter_map(|r| r.ok())
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();

        for (k, v) in existing {
            if let Ok(root) = String::from_utf8(v) {
                let _ = self.dec_root_count(root.as_bytes());
            }
            let _ = self.tag_to_root.remove(k);
        }

        // Re-add current tags.
        let tags = match storage.list_tags(repo).await {
            Ok(t) => t,
            Err(StorageError::NotFound) => {
                self.db.flush()?;
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        // De-dup roots per repo to avoid repeated reads.
        let mut roots: HashSet<String> = HashSet::new();

        for tag in tags {
            let root = match storage.resolve_tag(repo, &tag).await {
                Ok(d) => d,
                Err(StorageError::NotFound) => continue,
                Err(e) => return Err(e.into()),
            };

            self.tag_to_root
                .insert(tag_key(repo, &tag), root.as_str().as_bytes())?;
            self.inc_root_count(root.as_str().as_bytes())?;
            roots.insert(root.as_str().to_string());
        }

        for root in roots {
            if let Ok(d) = Digest::parse(&root) {
                self.ingest_root(storage, repo, &d).await?;
            }
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

        let repos = storage.list_repositories().await?;
        for repo in repos {
            self.sync_repo_tags(storage, &repo).await?;
        }

        self.meta.insert(META_STATE, META_STATE_READY)?;
        self.db.flush()?;
        Ok(())
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

                let parsed = parse_manifest_refs(&bytes);
                refs_cache.insert(digest_hex, parsed.clone());
                let Some(r) = parsed else {
                    continue;
                };
                r
            };

            // child blob -> parent manifest
            for child_blob in &refs.blobs {
                self.add_parent(child_blob.as_bytes(), digest.as_str().as_bytes())?;
            }

            // child manifest -> parent manifest
            for child_manifest in &refs.manifests {
                self.add_parent(child_manifest.as_bytes(), digest.as_str().as_bytes())?;
                if let Ok(d) = Digest::parse(child_manifest) {
                    queue.push_back(d);
                }
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
            if next == 0 { None } else { Some(encode_u64(next)) }
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
    Some(s.split('\n').filter(|p| !p.is_empty()).map(|p| p.to_string()).collect())
}

// Keep clippy happy: used by signature clarity.
#[allow(dead_code)]
fn _path_exists(p: &Path) -> bool {
    p.exists()
}
