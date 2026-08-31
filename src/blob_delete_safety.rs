use crate::{manifest_refs::parse_manifest_refs, registry::digest::Digest, storage::StorageError};
use std::{
    collections::HashMap,
    collections::{HashSet, VecDeque},
    sync::Arc,
};

#[derive(Clone, Debug)]
pub struct BlobReference {
    pub repo: String,
    pub tag: Option<String>,
    pub manifest: String,
}

use crate::manifest_refs::ManifestRefs;
use crate::storage::BlobIndexStoragePort;

async fn scan_repo_for_blob(
    storage: &(impl BlobIndexStoragePort + ?Sized),
    repo: &str,
    root_manifest: Digest,
    root_tag: Option<String>,
    target: &Digest,
    refs_cache: &mut HashMap<Digest, Option<ManifestRefs>>,
) -> Result<Option<BlobReference>, StorageError> {
    let mut queue: VecDeque<Digest> = VecDeque::new();
    queue.push_back(root_manifest);

    let mut visited: HashSet<Digest> = HashSet::new();

    while let Some(digest) = queue.pop_front() {
        if !visited.insert(digest.clone()) {
            continue;
        }

        let refs = if let Some(v) = refs_cache.get(&digest) {
            match v.clone() {
                Some(r) => r,
                None => continue,
            }
        } else {
            let (_meta, bytes) = match storage.get_manifest(repo, &digest).await {
                Ok(v) => v,
                Err(StorageError::NotFound) => {
                    refs_cache.insert(digest.clone(), None);
                    continue;
                }
                Err(e) => return Err(e),
            };

            let parsed = match parse_manifest_refs(&bytes) {
                Ok(r) => r,
                Err(e) => {
                    return Err(StorageError::Internal(format!(
                        "unparsable manifest {}: {e}",
                        digest.as_str()
                    )));
                }
            };
            refs_cache.insert(digest.clone(), Some(parsed.clone()));
            parsed
        };

        if refs.blob_references().any(|d| d == target) {
            return Ok(Some(BlobReference {
                repo: repo.to_string(),
                tag: root_tag,
                manifest: digest.as_str(),
            }));
        }

        for child in refs.manifest_references() {
            queue.push_back(child.clone());
        }
    }

    Ok(None)
}

/// Returns the first known reference to `target` if found.
///
/// The algorithm is intentionally conservative:
/// - It walks all repositories and all tags.
/// - For each tag, it traverses the manifest graph (indexes -> manifests) and checks
///   config/layer/subject digests.
/// - If it cannot fetch a referenced manifest, it skips that node (treating the repo
///   as already inconsistent).
#[allow(dead_code)]
pub async fn find_blob_reference(
    storage: &(impl BlobIndexStoragePort + ?Sized),
    target: &Digest,
) -> Result<Option<BlobReference>, StorageError> {
    let repos = storage.list_repositories().await?;

    for repo in repos {
        let tags = match storage.list_tags(&repo).await {
            Ok(t) => t,
            Err(StorageError::NotFound) => continue,
            Err(e) => return Err(e),
        };

        // Many tags often point to the same digest. De-dup roots to avoid repeated manifest reads.
        let mut roots: HashMap<Digest, String> = HashMap::new();
        for tag in tags {
            let root = match storage.resolve_tag(&repo, &tag).await {
                Ok(d) => d,
                Err(StorageError::NotFound) => continue,
                Err(e) => return Err(e),
            };
            roots.entry(root).or_insert(tag);
        }

        let mut refs_cache: HashMap<Digest, Option<ManifestRefs>> = HashMap::new();
        for (root, tag) in roots {
            if let Some(r) =
                scan_repo_for_blob(storage, &repo, root, Some(tag), target, &mut refs_cache).await?
            {
                return Ok(Some(r));
            }
        }
    }

    Ok(None)
}

/// Returns the first known reference to `target` in the specified repository `repo`.
pub async fn find_repo_blob_reference(
    storage: &(impl BlobIndexStoragePort + ?Sized),
    repo: &str,
    target: &Digest,
) -> Result<Option<BlobReference>, StorageError> {
    let tags = match storage.list_tags(repo).await {
        Ok(t) => t,
        Err(StorageError::NotFound) => return Ok(None),
        Err(e) => return Err(e),
    };

    let mut roots: HashMap<Digest, String> = HashMap::new();
    for tag in tags {
        let root = match storage.resolve_tag(repo, &tag).await {
            Ok(d) => d,
            Err(StorageError::NotFound) => continue,
            Err(e) => return Err(e),
        };
        roots.entry(root).or_insert(tag);
    }

    let mut refs_cache: HashMap<Digest, Option<ManifestRefs>> = HashMap::new();
    for (root, tag) in roots {
        if let Some(r) =
            scan_repo_for_blob(storage, repo, root, Some(tag), target, &mut refs_cache).await?
        {
            return Ok(Some(r));
        }
    }

    Ok(None)
}

#[derive(Debug, PartialEq, Eq)]
pub enum BlobDeleteResult {
    Success,
    NotFound,
    InUse { message: String },
}

pub struct BlobDeleteService {
    ledger: crate::repository_membership_ledger::RepositoryMembershipLedger,
    index_storage: Arc<dyn BlobIndexStoragePort>,
}

impl BlobDeleteService {
    pub fn new(
        index_storage: Arc<dyn BlobIndexStoragePort>,
        ledger: crate::repository_membership_ledger::RepositoryMembershipLedger,
    ) -> Self {
        Self {
            ledger,
            index_storage,
        }
    }

    pub fn ledger(&self) -> &crate::repository_membership_ledger::RepositoryMembershipLedger {
        &self.ledger
    }

    /// Safely handles a repository-scoped blob deletion request:
    /// 1. Acquires mutation guard from coordinator.
    /// 2. Ensures ref-index is healthy / rebuilt if dirty.
    /// 3. Verifies membership in the requested repository (returns NotFound if absent).
    /// 4. Verifies whether any manifest in the requested repository still references the blob (returns InUse if so).
    /// 5. Unlinks only the requested repository's membership record through the ledger under guard.
    pub async fn delete_repo_blob(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<BlobDeleteResult, StorageError> {
        // 1. Acquire mutation guard for atomic reference validation and unlinking
        let guard = self.ledger.consistency().acquire_mutation().await;

        // 2. Verify membership exists in the requested repository
        let membership = self.ledger.get_membership(repo, digest).await?;
        if membership.is_none() {
            return Ok(BlobDeleteResult::NotFound);
        }

        // 3. Check if referenced by manifest in this repository
        if let Some(r) = find_repo_blob_reference(self.index_storage.as_ref(), repo, digest).await?
        {
            let mut msg = format!("blob is still referenced by manifest {}", r.manifest);
            if let Some(tag) = r.tag {
                msg = format!("{msg} (repo={}, tag={})", r.repo, tag);
            }
            return Ok(BlobDeleteResult::InUse { message: msg });
        }

        // 4. Unlink repository membership via ledger under guard
        match self.ledger.unlink_with_guard(&guard, repo, digest).await {
            Ok(true) => Ok(BlobDeleteResult::Success),
            Ok(false) => Ok(BlobDeleteResult::NotFound),
            Err(crate::repository_membership_ledger::LedgerError::Storage(e)) => Err(e),
            Err(e) => Err(StorageError::Internal(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use crate::storage::fs::FsStorage;
    use sha2::{Digest as _, Sha256, Sha512};
    use std::path::PathBuf;

    fn tmp_fs_root() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "registry-rust-delete-safety-test-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&p).expect("create temp fs_root");
        p
    }

    #[tokio::test]
    async fn test_find_blob_reference_with_sha512_manifest_root() {
        let root = tmp_fs_root();
        let storage = Arc::new(FsStorage::new(root.clone(), 1024 * 1024));

        let repo = "testrepo";
        let blob_bytes = b"sample layer data for deletion safety";
        let blob_hex = hex::encode(Sha256::digest(blob_bytes));
        let blob_digest = Digest::parse(&format!("sha256:{blob_hex}")).unwrap();

        // 1. Put blob into storage
        let upload = storage.create_upload().await.unwrap();
        storage
            .append_upload(&upload.uuid, bytes::Bytes::from_static(blob_bytes))
            .await
            .unwrap();
        storage
            .finalize_upload(&upload.uuid, &blob_digest)
            .await
            .unwrap();

        // 2. Create manifest referencing this blob
        let manifest_json = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.empty.v1+json",
                "digest": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                "size": 0
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar",
                    "digest": blob_digest.as_str(),
                    "size": blob_bytes.len()
                }
            ]
        });
        let manifest_bytes = serde_json::to_vec(&manifest_json).unwrap();
        let manifest_hex = hex::encode(Sha512::digest(&manifest_bytes));
        let manifest_digest = Digest::parse(&format!("sha512:{manifest_hex}")).unwrap();

        storage
            .put_manifest(repo, &manifest_digest, bytes::Bytes::from(manifest_bytes))
            .await
            .unwrap();
        storage
            .set_tag(repo, "v1.0", &manifest_digest)
            .await
            .unwrap();

        // 3. Find reference to target blob
        let found = find_blob_reference(&storage, &blob_digest)
            .await
            .unwrap()
            .expect("should find reference in sha512 manifest root");

        assert_eq!(found.repo, repo);
        assert_eq!(found.tag.as_deref(), Some("v1.0"));
        assert_eq!(found.manifest, manifest_digest.as_str());

        let _ = std::fs::remove_dir_all(&root);
    }
}
