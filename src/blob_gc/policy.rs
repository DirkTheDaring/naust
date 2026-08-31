use crate::blob_ref_index::BlobRefIndex;
use crate::manifest_refs::{ManifestParseError, parse_manifest_refs};
use crate::registry::digest::Digest;
use crate::storage;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgeEligibility {
    Eligible,
    IneligibleAge,
    FutureTimestamp,
    MissingTimestamp,
}

/// Authoritative age/grace eligibility evaluation for GC candidates across all storage backends.
///
/// Invariants:
/// - Missing or unparseable timestamps (`last_modified == UNIX_EPOCH`) fail closed as `MissingTimestamp`.
/// - Future timestamps (`last_modified > now`) fail closed as `FutureTimestamp`.
/// - Elapsed age < `min_age` returns `IneligibleAge`.
/// - Elapsed age >= `min_age` returns `Eligible`.
/// - Zero grace (`min_age == 0`) with valid past timestamp returns `Eligible`.
pub fn check_candidate_age(
    last_modified: SystemTime,
    now: SystemTime,
    min_age: Duration,
) -> AgeEligibility {
    if last_modified == UNIX_EPOCH {
        return AgeEligibility::MissingTimestamp;
    }
    match now.duration_since(last_modified) {
        Ok(age) => {
            if age >= min_age {
                AgeEligibility::Eligible
            } else {
                AgeEligibility::IneligibleAge
            }
        }
        Err(_) => AgeEligibility::FutureTimestamp,
    }
}

/// Typed error model for policy evaluation, reachability traversal, and root set construction.
#[derive(Debug, thiserror::Error)]
pub enum GcPolicyError {
    #[error("reference index health check failed: {0}")]
    IndexHealth(#[from] crate::blob_ref_index::RefIndexError),

    #[error("repository enumeration failed: {0}")]
    ListRepositories(#[source] crate::storage::StorageError),

    #[error("manifest listing failed for repository '{repository}': {source}")]
    ListManifests {
        repository: String,
        #[source]
        source: crate::storage::StorageError,
    },

    #[error("failed to read manifest '{digest}' in repository '{repository}': {source}")]
    ReadManifest {
        repository: String,
        digest: String,
        #[source]
        source: crate::storage::StorageError,
    },

    #[error(
        "failed to parse manifest references for '{digest}' in repository '{repository}': {source}"
    )]
    ParseManifest {
        repository: String,
        digest: String,
        #[source]
        source: ManifestParseError,
    },

    #[error("filesystem traversal failed for '{path}': {source}")]
    FsReadDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to read manifest file '{path}': {source}")]
    FsReadManifest {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

pub struct PolicyContext {
    pub(crate) policy: BlobGcPolicy,
    pub(crate) idx: Arc<BlobRefIndex>,
    pub(crate) manifest_protected: Option<HashSet<String>>,
}

impl PolicyContext {
    pub async fn build(
        cfg: &crate::config::Config,
        storage: &(impl storage::GcServiceStoragePort + ?Sized),
        idx: &BlobRefIndex,
        policy: BlobGcPolicy,
    ) -> Result<Self, GcPolicyError> {
        idx.check_health()?;

        let idx = Arc::new(idx.clone());

        let manifest_protected = match policy {
            BlobGcPolicy::TagRooted => None,
            BlobGcPolicy::ManifestRooted => Some(build_manifest_protected_set(cfg, storage).await?),
        };

        Ok(Self {
            policy,
            idx,
            manifest_protected,
        })
    }

    pub async fn is_referenced(
        &mut self,
        digest: &Digest,
    ) -> Result<bool, crate::blob_ref_index::RefIndexError> {
        let tag_reachable = self.idx.is_blob_referenced(digest)?;
        if tag_reachable {
            return Ok(true);
        }

        if self.policy == BlobGcPolicy::ManifestRooted {
            let Some(set) = self.manifest_protected.as_ref() else {
                return Ok(false);
            };
            return Ok(set.contains(&digest.as_str()));
        }

        Ok(false)
    }

    pub fn is_pinned(
        &self,
        digest: &Digest,
        now: SystemTime,
    ) -> Result<bool, crate::blob_ref_index::RefIndexError> {
        self.idx.is_blob_pinned(digest, now)
    }
}

pub async fn build_manifest_protected_set(
    cfg: &crate::config::Config,
    storage: &(impl storage::GcServiceStoragePort + ?Sized),
) -> Result<HashSet<String>, GcPolicyError> {
    if storage.kind() == "fs"
        && tokio::fs::metadata(&cfg.fs_root.join("repos"))
            .await
            .is_ok()
    {
        return build_manifest_protected_set_fs(&cfg.fs_root).await;
    }

    let repos = storage
        .list_repositories()
        .await
        .map_err(GcPolicyError::ListRepositories)?;

    let mut protected = HashSet::new();
    for repo in repos {
        let mut cursor = None;
        loop {
            let (digests, next_cursor) = storage
                .list_manifest_digests_page(&repo, cursor.as_deref(), 100)
                .await
                .map_err(|source| GcPolicyError::ListManifests {
                    repository: repo.clone(),
                    source,
                })?;

            for digest in digests {
                protected.insert(digest.as_str().to_string());
                let (_meta, bytes) =
                    storage
                        .get_manifest(&repo, &digest)
                        .await
                        .map_err(|source| GcPolicyError::ReadManifest {
                            repository: repo.clone(),
                            digest: digest.to_string(),
                            source,
                        })?;
                let refs =
                    parse_manifest_refs(&bytes).map_err(|source| GcPolicyError::ParseManifest {
                        repository: repo.clone(),
                        digest: digest.to_string(),
                        source,
                    })?;
                for r in refs.all_references() {
                    protected.insert(r.as_str().to_string());
                }
            }

            if next_cursor.is_none() {
                break;
            }
            cursor = next_cursor;
        }
    }

    Ok(protected)
}

pub async fn build_manifest_protected_set_fs(
    fs_root: &Path,
) -> Result<HashSet<String>, GcPolicyError> {
    let repos_root = fs_root.join("repos");
    let mut stack: Vec<PathBuf> = vec![repos_root];
    let mut protected: HashSet<String> = HashSet::new();

    while let Some(dir) = stack.pop() {
        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(GcPolicyError::FsReadDir { path: dir, source }),
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
                    let mut md = match tokio::fs::read_dir(&path).await {
                        Ok(d) => d,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(source) => {
                            return Err(GcPolicyError::FsReadDir { path, source });
                        }
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
                            Err(source) => {
                                return Err(GcPolicyError::FsReadManifest { path: mp, source });
                            }
                        };
                        let refs = parse_manifest_refs(&bytes).map_err(|source| {
                            GcPolicyError::ParseManifest {
                                repository: "fs".to_string(),
                                digest: digest.clone(),
                                source,
                            }
                        })?;
                        for r in refs.all_references() {
                            protected.insert(r.as_str().to_string());
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
