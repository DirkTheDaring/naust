use crate::blob_ref_index::BlobRefIndex;
use crate::manifest_refs::parse_manifest_refs;
use crate::registry::digest::Digest;
use crate::storage;
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum BlobGcPolicy {
    /// Consider a blob "in use" only if reachable from any tag root.
    TagRooted,
    /// Consider a blob "in use" if reachable from any stored manifest (tagged or untagged).
    ManifestRooted,
}

impl Default for BlobGcPolicy {
    fn default() -> Self {
        Self::ManifestRooted
    }
}

#[derive(Debug, Default, Clone)]
pub struct BlobGcStats {
    pub scanned_blobs: u64,
    pub scanned_bytes: u64,
    pub eligible_blobs: u64,
    pub eligible_bytes: u64,
    pub quarantined_blobs: u64,
    pub quarantined_bytes: u64,
    pub restored_blobs: u64,
    pub restored_bytes: u64,
    pub deleted_blobs: u64,
    pub deleted_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct BlobGcLimits {
    pub max_per_run: usize,
    pub max_bytes: u64,
    pub max_seconds: u64,
}

impl BlobGcLimits {
    pub fn unlimited(max_per_run: usize) -> Self {
        Self {
            max_per_run,
            max_bytes: u64::MAX,
            max_seconds: u64::MAX,
        }
    }
}

pub async fn blob_gc_plan(
    cfg: &crate::config::Config,
    storage: &Arc<dyn storage::Storage>,
    idx: &BlobRefIndex,
    policy: BlobGcPolicy,
    min_age: Duration,
    limits: BlobGcLimits,
) -> Result<BlobGcStats, String> {
    let mut policy_ctx = PolicyContext::build(cfg, storage, idx, policy).await?;
    let mut stats = BlobGcStats::default();

    let t0 = Instant::now();

    let root = cfg.fs_root.join("blobs").join("sha256");
    let now = SystemTime::now();
    let mut prefixes = match tokio::fs::read_dir(&root).await {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(stats),
        Err(e) => return Err(format!("read_dir {}: {e}", root.display())),
    };

    'prefixes: while let Ok(Some(prefix_ent)) = prefixes.next_entry().await {
        if t0.elapsed() > Duration::from_secs(limits.max_seconds) {
            break;
        }

        let ft = match prefix_ent.file_type().await {
            Ok(t) => t,
            Err(_) => continue,
        };
        if !ft.is_dir() {
            continue;
        }

        let mut dir = match tokio::fs::read_dir(prefix_ent.path()).await {
            Ok(d) => d,
            Err(_) => continue,
        };

        while let Ok(Some(ent)) = dir.next_entry().await {
            if t0.elapsed() > Duration::from_secs(limits.max_seconds) {
                break 'prefixes;
            }

            let ft = match ent.file_type().await {
                Ok(t) => t,
                Err(_) => continue,
            };
            if !ft.is_file() {
                continue;
            }

            let path = ent.path();
            let file_hex = match path.file_name().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            if file_hex.len() != 64 || !file_hex.chars().all(|c| c.is_ascii_hexdigit()) {
                continue;
            }

            let meta = match ent.metadata().await {
                Ok(m) => m,
                Err(_) => continue,
            };
            let modified = meta.modified().unwrap_or(UNIX_EPOCH);
            let age = now
                .duration_since(modified)
                .unwrap_or(Duration::from_secs(0));
            if age < min_age {
                continue;
            }

            let digest = match Digest::parse(&format!("sha256:{file_hex}")) {
                Ok(d) => d,
                Err(_) => continue,
            };

            if policy_ctx.is_pinned(&digest, now)? {
                continue;
            }

            stats.scanned_blobs += 1;
            stats.scanned_bytes = stats.scanned_bytes.saturating_add(meta.len());

            if stats.eligible_blobs as usize >= limits.max_per_run {
                continue;
            }
            if policy_ctx.is_referenced(&digest).await? {
                continue;
            }

            if stats.eligible_bytes >= limits.max_bytes {
                continue;
            }
            stats.eligible_blobs += 1;
            stats.eligible_bytes = stats.eligible_bytes.saturating_add(meta.len());
        }
    }

    Ok(stats)
}

pub async fn blob_gc_quarantine(
    cfg: &crate::config::Config,
    storage: &Arc<dyn storage::Storage>,
    idx: &BlobRefIndex,
    policy: BlobGcPolicy,
    min_age: Duration,
    limits: BlobGcLimits,
) -> Result<BlobGcStats, String> {
    let mut policy_ctx = PolicyContext::build(cfg, storage, idx, policy).await?;
    let mut stats = BlobGcStats::default();

    let t0 = Instant::now();

    let now = SystemTime::now();

    let root = cfg.fs_root.join("blobs").join("sha256");
    let now_for_age = SystemTime::now();
    let mut prefixes = match tokio::fs::read_dir(&root).await {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(stats),
        Err(e) => return Err(format!("read_dir {}: {e}", root.display())),
    };

    while let Ok(Some(prefix_ent)) = prefixes.next_entry().await {
        if t0.elapsed() > Duration::from_secs(limits.max_seconds) {
            break;
        }
        if stats.quarantined_blobs as usize >= limits.max_per_run {
            break;
        }
        if stats.quarantined_bytes >= limits.max_bytes {
            break;
        }

        let ft = match prefix_ent.file_type().await {
            Ok(t) => t,
            Err(_) => continue,
        };
        if !ft.is_dir() {
            continue;
        }

        let mut dir = match tokio::fs::read_dir(prefix_ent.path()).await {
            Ok(d) => d,
            Err(_) => continue,
        };

        while let Ok(Some(ent)) = dir.next_entry().await {
            if t0.elapsed() > Duration::from_secs(limits.max_seconds) {
                break;
            }
            if stats.quarantined_blobs as usize >= limits.max_per_run {
                break;
            }
            if stats.quarantined_bytes >= limits.max_bytes {
                break;
            }

            let ft = match ent.file_type().await {
                Ok(t) => t,
                Err(_) => continue,
            };
            if !ft.is_file() {
                continue;
            }

            let path = ent.path();
            let file_hex = match path.file_name().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            if file_hex.len() != 64 || !file_hex.chars().all(|c| c.is_ascii_hexdigit()) {
                continue;
            }

            let meta = match ent.metadata().await {
                Ok(m) => m,
                Err(_) => continue,
            };
            let modified = meta.modified().unwrap_or(UNIX_EPOCH);
            let age = now_for_age
                .duration_since(modified)
                .unwrap_or(Duration::from_secs(0));
            if age < min_age {
                continue;
            }

            let digest = match Digest::parse(&format!("sha256:{file_hex}")) {
                Ok(d) => d,
                Err(_) => continue,
            };

            if policy_ctx.is_pinned(&digest, now_for_age)? {
                continue;
            }

            stats.scanned_blobs += 1;
            stats.scanned_bytes = stats.scanned_bytes.saturating_add(meta.len());

            if policy_ctx.is_referenced(&digest).await? {
                continue;
            }

            if stats.quarantined_bytes >= limits.max_bytes {
                continue;
            }
            match quarantine_blob(cfg, &digest, now).await {
                Ok(QuarantineOutcome::Moved { size }) => {
                    stats.quarantined_blobs += 1;
                    stats.quarantined_bytes = stats.quarantined_bytes.saturating_add(size);
                }
                Ok(QuarantineOutcome::Skipped) => {}
                Err(e) => return Err(e),
            }
        }
    }

    Ok(stats)
}

pub async fn blob_gc_delete(
    cfg: &crate::config::Config,
    storage: &Arc<dyn storage::Storage>,
    idx: &BlobRefIndex,
    policy: BlobGcPolicy,
    quarantine_delay: Duration,
    limits: BlobGcLimits,
) -> Result<BlobGcStats, String> {
    let mut policy_ctx = PolicyContext::build(cfg, storage, idx, policy).await?;
    let mut stats = BlobGcStats::default();

    let t0 = Instant::now();

    let now = SystemTime::now();

    let root = cfg.fs_root.join("quarantine").join("blobs").join("sha256");

    let mut prefixes = match tokio::fs::read_dir(&root).await {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(stats),
        Err(e) => return Err(format!("read_dir {}: {e}", root.display())),
    };

    while let Ok(Some(prefix_ent)) = prefixes.next_entry().await {
        if t0.elapsed() > Duration::from_secs(limits.max_seconds) {
            break;
        }
        if stats.deleted_blobs as usize >= limits.max_per_run {
            break;
        }
        if stats.deleted_bytes >= limits.max_bytes {
            break;
        }

        let ft = match prefix_ent.file_type().await {
            Ok(t) => t,
            Err(_) => continue,
        };
        if !ft.is_dir() {
            continue;
        }

        let mut dir = match tokio::fs::read_dir(prefix_ent.path()).await {
            Ok(d) => d,
            Err(_) => continue,
        };

        while let Ok(Some(ent)) = dir.next_entry().await {
            if t0.elapsed() > Duration::from_secs(limits.max_seconds) {
                break;
            }
            if stats.deleted_blobs as usize >= limits.max_per_run {
                break;
            }
            if stats.deleted_bytes >= limits.max_bytes {
                break;
            }

            let ft = match ent.file_type().await {
                Ok(t) => t,
                Err(_) => continue,
            };
            if !ft.is_file() {
                continue;
            }

            let path = ent.path();
            let file_hex = match path.file_name().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            if file_hex.len() != 64 || !file_hex.chars().all(|c| c.is_ascii_hexdigit()) {
                continue;
            }

            let meta = match ent.metadata().await {
                Ok(m) => m,
                Err(_) => continue,
            };

            let digest = match Digest::parse(&format!("sha256:{file_hex}")) {
                Ok(d) => d,
                Err(_) => continue,
            };

            // Never delete (or churn) pinned blobs.
            if policy_ctx.is_pinned(&digest, now)? {
                continue;
            }

            // If a quarantined blob becomes referenced again, restore it regardless of its
            // quarantine timestamp metadata.
            if policy_ctx.is_referenced(&digest).await? {
                match restore_blob(cfg, &digest).await {
                    Ok(Some(size)) => {
                        stats.restored_blobs += 1;
                        stats.restored_bytes = stats.restored_bytes.saturating_add(size);
                    }
                    Ok(None) => {}
                    Err(e) => return Err(e),
                }
                continue;
            }

            // Crash/outage recovery: if the quarantine timestamp is missing or invalid,
            // self-heal it conservatively (treat as newly quarantined) so the blob can be
            // deleted in a later run without risking premature deletes.
            let q_at = match read_quarantine_time(cfg, &digest).await? {
                Some(t) => t,
                None => {
                    let _ = write_quarantine_time(cfg, &digest, now).await;
                    continue;
                }
            };

            let age = now.duration_since(q_at).unwrap_or(Duration::from_secs(0));
            if age < quarantine_delay {
                continue;
            }

            if stats.deleted_bytes >= limits.max_bytes {
                continue;
            }
            match delete_quarantined_blob(cfg, &digest).await {
                Ok(Some(size)) => {
                    stats.deleted_blobs += 1;
                    stats.deleted_bytes = stats.deleted_bytes.saturating_add(size);
                }
                Ok(None) => {}
                Err(e) => return Err(e),
            }

            // Keep stats usage of metadata len consistent with delete outcome.
            let _ = meta;
        }
    }

    Ok(stats)
}

struct PolicyContext {
    policy: BlobGcPolicy,
    idx: Arc<BlobRefIndex>,
    manifest_protected: Option<HashSet<String>>,
}

impl PolicyContext {
    async fn build(
        cfg: &crate::config::Config,
        _storage: &Arc<dyn storage::Storage>,
        idx: &BlobRefIndex,
        policy: BlobGcPolicy,
    ) -> Result<Self, String> {
        // We require index health for safe operation.
        idx.check_health().map_err(|e| format!("ref-index: {e}"))?;

        let idx = Arc::new(idx.clone());

        let manifest_protected = match policy {
            BlobGcPolicy::TagRooted => None,
            BlobGcPolicy::ManifestRooted => Some(build_manifest_protected_set(&cfg.fs_root).await?),
        };

        Ok(Self {
            policy,
            idx,
            manifest_protected,
        })
    }

    async fn is_referenced(&mut self, digest: &Digest) -> Result<bool, String> {
        // Always treat tag-rooted reachability as protected.
        let tag_reachable = self
            .idx
            .is_blob_referenced(digest)
            .map_err(|e| format!("ref-index: {e}"))?;
        if tag_reachable {
            return Ok(true);
        }

        if self.policy == BlobGcPolicy::ManifestRooted {
            let Some(set) = self.manifest_protected.as_ref() else {
                return Ok(false);
            };
            let key = digest.as_str();
            return Ok(set.contains(&key));
        }

        Ok(false)
    }

    fn is_pinned(&self, digest: &Digest, now: SystemTime) -> Result<bool, String> {
        self.idx
            .is_blob_pinned(digest, now)
            .map_err(|e| format!("ref-index: {e}"))
    }
}

enum QuarantineOutcome {
    Moved { size: u64 },
    Skipped,
}

async fn quarantine_blob(
    cfg: &crate::config::Config,
    digest: &Digest,
    now: SystemTime,
) -> Result<QuarantineOutcome, String> {
    let src = cfg
        .fs_root
        .join("blobs")
        .join("sha256")
        .join(digest.prefix2())
        .join(digest.hex());

    let meta = match tokio::fs::metadata(&src).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(QuarantineOutcome::Skipped);
        }
        Err(e) => return Err(format!("metadata {}: {e}", src.display())),
    };

    let dest_dir = cfg
        .fs_root
        .join("quarantine")
        .join("blobs")
        .join("sha256")
        .join(digest.prefix2());
    tokio::fs::create_dir_all(&dest_dir)
        .await
        .map_err(|e| format!("mkdir {}: {e}", dest_dir.display()))?;

    let dest = dest_dir.join(digest.hex());
    if tokio::fs::metadata(&dest).await.is_ok() {
        // Something odd: same digest exists in quarantine already; safest is to skip.
        return Ok(QuarantineOutcome::Skipped);
    }

    match tokio::fs::rename(&src, &dest).await {
        Ok(()) => {
            write_quarantine_time(cfg, digest, now).await?;
            Ok(QuarantineOutcome::Moved { size: meta.len() })
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(QuarantineOutcome::Skipped),
        Err(e) => Err(format!(
            "rename {} -> {}: {e}",
            src.display(),
            dest.display()
        )),
    }
}

async fn restore_blob(cfg: &crate::config::Config, digest: &Digest) -> Result<Option<u64>, String> {
    let src = cfg
        .fs_root
        .join("quarantine")
        .join("blobs")
        .join("sha256")
        .join(digest.prefix2())
        .join(digest.hex());

    let meta = match tokio::fs::metadata(&src).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("metadata {}: {e}", src.display())),
    };

    let dest_dir = cfg
        .fs_root
        .join("blobs")
        .join("sha256")
        .join(digest.prefix2());
    tokio::fs::create_dir_all(&dest_dir)
        .await
        .map_err(|e| format!("mkdir {}: {e}", dest_dir.display()))?;
    let dest = dest_dir.join(digest.hex());

    // If blob already exists in live store, we can drop the quarantined duplicate.
    if tokio::fs::metadata(&dest).await.is_ok() {
        let _ = tokio::fs::remove_file(&src).await;
        let _ = remove_quarantine_time(cfg, digest).await;
        return Ok(Some(meta.len()));
    }

    match tokio::fs::rename(&src, &dest).await {
        Ok(()) => {
            let _ = remove_quarantine_time(cfg, digest).await;
            Ok(Some(meta.len()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!(
            "rename {} -> {}: {e}",
            src.display(),
            dest.display()
        )),
    }
}

async fn delete_quarantined_blob(
    cfg: &crate::config::Config,
    digest: &Digest,
) -> Result<Option<u64>, String> {
    let path = cfg
        .fs_root
        .join("quarantine")
        .join("blobs")
        .join("sha256")
        .join(digest.prefix2())
        .join(digest.hex());

    let meta = match tokio::fs::metadata(&path).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("metadata {}: {e}", path.display())),
    };

    match tokio::fs::remove_file(&path).await {
        Ok(()) => {
            let _ = remove_quarantine_time(cfg, digest).await;
            Ok(Some(meta.len()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("remove {}: {e}", path.display())),
    }
}

fn quarantine_meta_path(cfg: &crate::config::Config, digest: &Digest) -> PathBuf {
    cfg.fs_root
        .join("quarantine")
        .join("meta")
        .join("sha256")
        .join(digest.prefix2())
        .join(format!("{}.ts", digest.hex()))
}

async fn write_quarantine_time(
    cfg: &crate::config::Config,
    digest: &Digest,
    at: SystemTime,
) -> Result<(), String> {
    let path = quarantine_meta_path(cfg, digest);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }

    let secs = at
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs();
    tokio::fs::write(&path, format!("{secs}\n"))
        .await
        .map_err(|e| format!("write {}: {e}", path.display()))
}

async fn read_quarantine_time(
    cfg: &crate::config::Config,
    digest: &Digest,
) -> Result<Option<SystemTime>, String> {
    let path = quarantine_meta_path(cfg, digest);
    let s = match tokio::fs::read_to_string(&path).await {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("read {}: {e}", path.display())),
    };
    let secs: u64 = match s.trim().parse() {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    Ok(Some(UNIX_EPOCH + Duration::from_secs(secs)))
}

async fn remove_quarantine_time(
    cfg: &crate::config::Config,
    digest: &Digest,
) -> Result<(), String> {
    let path = quarantine_meta_path(cfg, digest);
    match tokio::fs::remove_file(&path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("remove {}: {e}", path.display())),
    }
}

async fn build_manifest_protected_set(fs_root: &Path) -> Result<HashSet<String>, String> {
    let repos_root = fs_root.join("repos");
    let mut stack: Vec<PathBuf> = vec![repos_root];
    let mut protected: HashSet<String> = HashSet::new();

    while let Some(dir) = stack.pop() {
        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(format!("read_dir {}: {e}", dir.display())),
        };

        while let Ok(Some(ent)) = rd.next_entry().await {
            let ft = match ent.file_type().await {
                Ok(t) => t,
                Err(_) => continue,
            };
            let path = ent.path();

            if ft.is_dir() {
                let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if name == "manifests" {
                    // Consume manifests dir.
                    let mut md = match tokio::fs::read_dir(&path).await {
                        Ok(d) => d,
                        Err(_) => continue,
                    };
                    while let Ok(Some(m)) = md.next_entry().await {
                        let mft = match m.file_type().await {
                            Ok(t) => t,
                            Err(_) => continue,
                        };
                        if !mft.is_file() {
                            continue;
                        }
                        let mp = m.path();
                        let hex = match mp.file_name().and_then(|s| s.to_str()) {
                            Some(s) => s,
                            None => continue,
                        };
                        if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                            continue;
                        }
                        let digest = format!("sha256:{hex}");
                        protected.insert(digest.clone());

                        let bytes = match tokio::fs::read(&mp).await {
                            Ok(b) => b,
                            Err(e) => {
                                return Err(format!("read manifest {}: {e}", mp.display()));
                            }
                        };
                        let refs = parse_manifest_refs(&bytes)
                            .map_err(|e| format!("unparsable manifest {}: {e}", mp.display()))?;
                        for r in refs.all_references() {
                            protected.insert(r.as_str());
                        }
                    }
                } else {
                    stack.push(path);
                }
                continue;
            }
        }
    }

    Ok(protected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_build_manifest_protected_set_aborts_on_unparsable_manifest() {
        let temp = tempfile::TempDir::new().unwrap();
        let fs_root = temp.path();

        // Create a repository with a valid manifest and a malformed manifest
        let manifests_dir = fs_root
            .join("repos")
            .join("library")
            .join("test")
            .join("manifests");
        tokio::fs::create_dir_all(&manifests_dir).await.unwrap();

        // 1. Valid manifest
        let valid_hex = "1111111111111111111111111111111111111111111111111111111111111111";
        let valid_manifest = serde_json::json!({
            "schemaVersion": 2,
            "config": { "digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222" },
            "layers": [{ "digest": "sha256:3333333333333333333333333333333333333333333333333333333333333333" }]
        });
        tokio::fs::write(
            manifests_dir.join(valid_hex),
            serde_json::to_vec(&valid_manifest).unwrap(),
        )
        .await
        .unwrap();

        // Only valid manifest: should succeed and protect blobs
        let protected = build_manifest_protected_set(fs_root).await.unwrap();
        assert!(
            protected.contains(
                "sha256:2222222222222222222222222222222222222222222222222222222222222222"
            )
        );
        assert!(
            protected.contains(
                "sha256:3333333333333333333333333333333333333333333333333333333333333333"
            )
        );

        // 2. Add an unparsable / malformed manifest (e.g. invalid digest in layers)
        let malformed_hex = "4444444444444444444444444444444444444444444444444444444444444444";
        let malformed_manifest = serde_json::json!({
            "schemaVersion": 2,
            "config": { "digest": "sha256:5555555555555555555555555555555555555555555555555555555555555555" },
            "layers": [{ "digest": "sha256:invalid-hex" }]
        });
        tokio::fs::write(
            manifests_dir.join(malformed_hex),
            serde_json::to_vec(&malformed_manifest).unwrap(),
        )
        .await
        .unwrap();

        // Now scan must fail immediately rather than returning an incomplete protection set
        let err = build_manifest_protected_set(fs_root).await.unwrap_err();
        assert!(err.contains("unparsable manifest"));
    }
}
