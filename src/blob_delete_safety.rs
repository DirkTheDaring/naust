use crate::{
    manifest_refs::parse_manifest_refs,
    registry::digest::Digest,
    storage::{Storage, StorageError},
};
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

async fn scan_repo_for_blob(
    storage: &Arc<dyn Storage>,
    repo: &str,
    root_manifest: Digest,
    root_tag: Option<String>,
    target: &Digest,
    refs_cache: &mut HashMap<Digest, Option<ManifestRefs>>,
) -> Result<Option<BlobReference>, StorageError> {
    let target_str = target.as_str();

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

            let parsed = parse_manifest_refs(&bytes);
            refs_cache.insert(digest.clone(), parsed.clone());
            let Some(r) = parsed else {
                continue;
            };
            r
        };

        if refs.blobs.iter().any(|d| d.as_str() == target_str) {
            return Ok(Some(BlobReference {
                repo: repo.to_string(),
                tag: root_tag,
                manifest: digest.as_str(),
            }));
        }

        for child in refs.manifests {
            if let Ok(d) = Digest::parse(&child) {
                queue.push_back(d);
            }
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
pub async fn find_blob_reference(
    storage: &Arc<dyn Storage>,
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

#[cfg(test)]
mod tests {
    use super::*;
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
        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(root.clone(), 1024 * 1024));

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
