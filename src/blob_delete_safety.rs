use crate::{
    manifest_refs::parse_manifest_refs,
    registry::digest::Digest,
    storage::{Storage, StorageError},
};
use std::{
    collections::{HashSet, VecDeque},
    collections::HashMap,
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
    refs_cache: &mut HashMap<String, Option<ManifestRefs>>,
) -> Result<Option<BlobReference>, StorageError> {
    let target_str = target.as_str();

    let mut queue: VecDeque<Digest> = VecDeque::new();
    queue.push_back(root_manifest);

    let mut visited: HashSet<String> = HashSet::new();

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
                Err(e) => return Err(e),
            };

            let parsed = parse_manifest_refs(&bytes);
            refs_cache.insert(digest_hex, parsed.clone());
            let Some(r) = parsed else {
                continue;
            };
            r
        };

        if refs.blobs.iter().any(|d| d.as_str() == target_str) {
            return Ok(Some(BlobReference {
                repo: repo.to_string(),
                tag: root_tag,
                manifest: digest.as_str().to_string(),
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
        let mut roots: HashMap<String, String> = HashMap::new();
        for tag in tags {
            let root = match storage.resolve_tag(&repo, &tag).await {
                Ok(d) => d,
                Err(StorageError::NotFound) => continue,
                Err(e) => return Err(e),
            };
            roots.entry(root.hex().to_string()).or_insert(tag);
        }

        let mut refs_cache: HashMap<String, Option<ManifestRefs>> = HashMap::new();
        for (hex, tag) in roots {
            let Ok(root) = Digest::parse(&format!("sha256:{hex}")) else {
                continue;
            };
            if let Some(r) =
                scan_repo_for_blob(storage, &repo, root, Some(tag), target, &mut refs_cache)
                    .await?
            {
                return Ok(Some(r));
            }
        }
    }

    Ok(None)
}
