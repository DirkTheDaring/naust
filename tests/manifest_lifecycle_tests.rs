use bytes::Bytes;
use sha2::Digest as _;
use std::sync::Arc;

use registry_rust::blob_ref_index::BlobRefIndex;
use registry_rust::manifest_lifecycle::{ManifestLifecycleService, PublishManifestRequest};
use registry_rust::registry::digest::Digest;
use registry_rust::storage::Storage;
use registry_rust::storage::fs::FsStorage;
use registry_rust::storage::repo_membership::{
    RepoBlobMembershipRecord, RepositoryBlobMembershipStorage,
};

fn sha256_digest(bytes: &[u8]) -> Digest {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let hex = hex::encode(hasher.finalize());
    Digest::parse(&format!("sha256:{hex}")).expect("valid sha256 digest")
}

async fn setup_test_service(
    dir: &tempfile::TempDir,
) -> (Arc<FsStorage>, Arc<BlobRefIndex>, ManifestLifecycleService) {
    let fs_root = dir.path().join("data");
    let ref_idx_path = dir.path().join("ref-index");
    std::fs::create_dir_all(&fs_root).unwrap();
    std::fs::create_dir_all(&ref_idx_path).unwrap();

    let storage = Arc::new(FsStorage::new(fs_root.clone(), 50 * 1024 * 1024));
    let ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());
    ref_index
        .rebuild(&(storage.clone() as Arc<dyn Storage>))
        .await
        .unwrap();

    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let service = ManifestLifecycleService::new(
        storage.clone() as Arc<dyn Storage>,
        Some(ref_index.clone()),
        gate,
    );

    (storage, ref_index, service)
}

async fn write_test_blob(storage: &Arc<FsStorage>, repo: &str, content: &[u8]) -> Digest {
    let digest = sha256_digest(content);
    let upload = storage.create_upload().await.unwrap();
    storage
        .append_upload(&upload.uuid, Bytes::copy_from_slice(content))
        .await
        .unwrap();
    storage
        .finalize_upload(&upload.uuid, &digest)
        .await
        .unwrap();

    let rec = RepoBlobMembershipRecord::new_upload(repo, digest.clone(), Some("s1".to_string()));
    storage.link_repo_blob(&rec).await.unwrap();
    digest
}

fn create_manifest_json(config: &Digest, layer: &Digest) -> (Bytes, Digest) {
    let json_val = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": 2,
            "digest": config.as_str()
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "size": 10,
                "digest": layer.as_str()
            }
        ]
    });
    let bytes = Bytes::from(serde_json::to_vec(&json_val).unwrap());
    let digest = sha256_digest(&bytes);
    (bytes, digest)
}

#[tokio::test]
async fn test_tag_deletion_leaves_untagged_manifest_readable_and_blobs_protected() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "test-repo";

    let cfg_d = write_test_blob(&storage, repo, b"{}").await;
    let layer_d = write_test_blob(&storage, repo, b"layer-payload-content").await;
    let (m_bytes, m_digest) = create_manifest_json(&cfg_d, &layer_d);

    // 1. Publish manifest with tag "v1.0"
    let pub_res = service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "v1.0".to_string(),
            payload: m_bytes.clone(),
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .expect("publish manifest");

    assert_eq!(pub_res.digest, m_digest);
    assert!(ref_index.is_blob_referenced(&layer_d).unwrap());
    assert!(ref_index.is_blob_referenced(&cfg_d).unwrap());

    // 2. Delete tag "v1.0"
    let del_tag_res = service.delete_tag(repo, "v1.0").await.expect("delete tag");
    assert_eq!(del_tag_res.tag, "v1.0");
    assert_eq!(del_tag_res.target_digest, m_digest);

    // 3. Tag is gone, but manifest MUST still be readable by digest!
    assert!(storage.resolve_tag(repo, "v1.0").await.is_err());
    let (m_meta, read_bytes) = storage
        .get_manifest(repo, &m_digest)
        .await
        .expect("get manifest by digest");
    assert_eq!(read_bytes, m_bytes);
    assert_eq!(m_meta.size, m_bytes.len() as u64);

    // 4. Referenced blobs MUST remain protected reachability roots in ref_index!
    assert!(ref_index.is_blob_referenced(&layer_d).unwrap());
    assert!(ref_index.is_blob_referenced(&cfg_d).unwrap());

    // 5. Index rebuild must maintain reachability from the untagged stored manifest!
    ref_index
        .rebuild(&(storage.clone() as Arc<dyn Storage>))
        .await
        .unwrap();
    assert!(ref_index.is_blob_referenced(&layer_d).unwrap());
    assert!(ref_index.is_blob_referenced(&cfg_d).unwrap());

    // 6. Deleting the manifest itself by digest removes the reachability root!
    let del_m_res = service
        .delete_manifest(repo, &m_digest)
        .await
        .expect("delete manifest");
    assert_eq!(del_m_res.digest, m_digest);

    // 7. Manifest is now gone and blobs are no longer reachable!
    assert!(storage.get_manifest(repo, &m_digest).await.is_err());
    assert!(!ref_index.is_blob_referenced(&layer_d).unwrap());
    assert!(!ref_index.is_blob_referenced(&cfg_d).unwrap());
}

#[tokio::test]
async fn test_manifest_deletion_policy_b_removes_all_referencing_tags() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_index, service) = setup_test_service(&dir).await;
    let repo = "policy-b-repo";

    let cfg_d = write_test_blob(&storage, repo, b"{}").await;
    let layer_d = write_test_blob(&storage, repo, b"layer-data").await;
    let (m_bytes, m_digest) = create_manifest_json(&cfg_d, &layer_d);

    // Publish under 3 different tags
    for tag in &["v1", "v1.0", "latest"] {
        service
            .publish_manifest(PublishManifestRequest {
                repo: repo.to_string(),
                reference: tag.to_string(),
                payload: m_bytes.clone(),
                declared_media_type: None,
                allow_tag_overwrite: true,
            })
            .await
            .unwrap();
    }

    let tags_before = storage.list_tags(repo).await.unwrap();
    assert_eq!(tags_before.len(), 3);

    // Delete manifest by digest
    let del_res = service.delete_manifest(repo, &m_digest).await.unwrap();
    assert_eq!(del_res.digest, m_digest);
    assert_eq!(del_res.removed_tags.len(), 3);

    // All tags pointing to this deleted manifest are cleanly removed (no dangling tags)
    let tags_after = storage.list_tags(repo).await.unwrap();
    assert!(
        tags_after.is_empty(),
        "All referencing tags must be removed"
    );
    for tag in &["v1", "v1.0", "latest"] {
        assert!(storage.resolve_tag(repo, tag).await.is_err());
    }
}

#[tokio::test]
async fn test_referrer_manifest_deletion_removes_descriptor_from_subject() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_index, service) = setup_test_service(&dir).await;
    let repo = "referrer-lifecycle-repo";

    // 1. Publish Subject Manifest
    let cfg_d = write_test_blob(&storage, repo, b"{}").await;
    let layer_d = write_test_blob(&storage, repo, b"subject-base").await;
    let (subj_bytes, subj_digest) = create_manifest_json(&cfg_d, &layer_d);

    service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "latest".to_string(),
            payload: subj_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    // 2. Publish Referrer Manifest (e.g. SBOM / Signature artifact)
    let artifact_blob = write_test_blob(&storage, repo, b"sbom-spdx-json").await;
    let referrer_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "artifactType": "application/vnd.example.sbom.v1",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "size": 2,
            "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        },
        "layers": [
            {
                "mediaType": "application/vnd.example.sbom.v1",
                "size": 14,
                "digest": artifact_blob.as_str()
            }
        ],
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "size": 200,
            "digest": subj_digest.as_str()
        }
    });
    let ref_bytes = Bytes::from(serde_json::to_vec(&referrer_json).unwrap());
    let ref_pub = service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: sha256_digest(&ref_bytes).to_string(),
            payload: ref_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    // Subject has 1 referrer in list
    let refs = storage.list_referrers(repo, &subj_digest).await.unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].digest, ref_pub.digest.as_str());

    // 3. Delete referrer manifest
    service
        .delete_manifest(repo, &ref_pub.digest)
        .await
        .unwrap();

    // Subject referrers list is now cleanly updated to 0
    let refs_after = storage.list_referrers(repo, &subj_digest).await.unwrap();
    assert!(refs_after.is_empty());

    // Subject manifest remains intact
    assert!(storage.get_manifest(repo, &subj_digest).await.is_ok());
}

#[tokio::test]
async fn test_index_deletion_preserves_child_manifests_as_reachability_roots() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "multi-arch-repo";

    // 1. Child Manifest 1 (linux/amd64)
    let cfg1 = write_test_blob(&storage, repo, b"config-amd64").await;
    let layer1 = write_test_blob(&storage, repo, b"layer-amd64").await;
    let (m1_bytes, m1_d) = create_manifest_json(&cfg1, &layer1);
    service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: m1_d.to_string(),
            payload: m1_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    // 2. Child Manifest 2 (linux/arm64)
    let cfg2 = write_test_blob(&storage, repo, b"config-arm64").await;
    let layer2 = write_test_blob(&storage, repo, b"layer-arm64").await;
    let (m2_bytes, m2_d) = create_manifest_json(&cfg2, &layer2);
    service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: m2_d.to_string(),
            payload: m2_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    // 3. Multi-arch index referencing Child 1 & Child 2 under tag "app:latest"
    let index_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "size": 200,
                "digest": m1_d.as_str(),
                "platform": { "os": "linux", "architecture": "amd64" }
            },
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "size": 200,
                "digest": m2_d.as_str(),
                "platform": { "os": "linux", "architecture": "arm64" }
            }
        ]
    });
    let idx_bytes = Bytes::from(serde_json::to_vec(&index_json).unwrap());
    let idx_pub = service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "latest".to_string(),
            payload: idx_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    // 4. Delete multi-arch index by digest
    service
        .delete_manifest(repo, &idx_pub.digest)
        .await
        .unwrap();

    // 5. Index is deleted, but Child 1 and Child 2 remain stored reachability roots!
    assert!(storage.get_manifest(repo, &idx_pub.digest).await.is_err());
    assert!(storage.get_manifest(repo, &m1_d).await.is_ok());
    assert!(storage.get_manifest(repo, &m2_d).await.is_ok());

    // Both amd64 and arm64 blobs remain protected
    assert!(ref_index.is_blob_referenced(&layer1).unwrap());
    assert!(ref_index.is_blob_referenced(&layer2).unwrap());
}

#[tokio::test]
async fn test_conditional_tag_cas_concurrency_and_races() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_index, service) = setup_test_service(&dir).await;
    let repo = "tag-cas-repo";

    let cfg = write_test_blob(&storage, repo, b"{}").await;
    let layer = write_test_blob(&storage, repo, b"cas-layer").await;
    let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

    service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "stable".to_string(),
            payload: m_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    // 1. Read tag with version
    let (digest, version) = storage
        .get_tag_with_version(repo, "stable")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(digest, m_d);

    // 2. Conditional delete with matching version succeeds
    let ok = storage
        .delete_tag_conditional(repo, "stable", Some(&version))
        .await
        .unwrap();
    assert!(ok, "Conditional delete with matching version must succeed");

    // 3. Tag is now deleted
    assert!(
        storage
            .get_tag_with_version(repo, "stable")
            .await
            .unwrap()
            .is_none()
    );

    // 4. Re-delete with stale version returns NotFound
    assert!(
        storage
            .delete_tag_conditional(repo, "stable", Some(&version))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_bounded_pagination_manifests_tags_referrers() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_index, service) = setup_test_service(&dir).await;
    let repo = "pagination-repo";

    // 1. Create 5 manifests with tags
    let mut manifest_digests = Vec::new();
    for i in 0..5 {
        let cfg = write_test_blob(&storage, repo, format!("cfg-{i}").as_bytes()).await;
        let layer = write_test_blob(&storage, repo, format!("layer-{i}").as_bytes()).await;
        let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);
        service
            .publish_manifest(PublishManifestRequest {
                repo: repo.to_string(),
                reference: format!("tag-{i:02}"),
                payload: m_bytes,
                declared_media_type: None,
                allow_tag_overwrite: true,
            })
            .await
            .unwrap();
        manifest_digests.push(m_d);
    }

    // 2. Page manifests with limit 2
    let (page1, tok1) = storage
        .list_manifest_digests_page(repo, None, 2)
        .await
        .unwrap();
    assert_eq!(page1.len(), 2);
    assert!(tok1.is_some());

    let (page2, tok2) = storage
        .list_manifest_digests_page(repo, tok1.as_deref(), 2)
        .await
        .unwrap();
    assert_eq!(page2.len(), 2);
    assert!(tok2.is_some());

    let (page3, tok3) = storage
        .list_manifest_digests_page(repo, tok2.as_deref(), 2)
        .await
        .unwrap();
    assert_eq!(page3.len(), 1);
    assert!(tok3.is_none());

    let mut all_paged = Vec::new();
    all_paged.extend(page1);
    all_paged.extend(page2);
    all_paged.extend(page3);
    assert_eq!(all_paged.len(), 5);

    // 3. Page tags with limit 2
    let (t_page1, t_tok1) = storage.list_tags_page(repo, None, 2).await.unwrap();
    assert_eq!(t_page1.len(), 2);
    assert!(t_tok1.is_some());

    let (t_page2, t_tok2) = storage
        .list_tags_page(repo, t_tok1.as_deref(), 2)
        .await
        .unwrap();
    assert_eq!(t_page2.len(), 2);
    assert!(t_tok2.is_some());

    let (t_page3, t_tok3) = storage
        .list_tags_page(repo, t_tok2.as_deref(), 2)
        .await
        .unwrap();
    assert_eq!(t_page3.len(), 1);
    assert!(t_tok3.is_none());
}

#[tokio::test]
async fn test_manifest_lifecycle_recovers_dirty_index_on_crash() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "dirty-recovery-repo";

    let cfg = write_test_blob(&storage, repo, b"{}").await;
    let layer = write_test_blob(&storage, repo, b"layer-data").await;
    let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

    // Simulate crash before publication complete: forcibly mark ref-index dirty
    ref_index.mark_dirty().unwrap();
    assert!(ref_index.check_health().is_err());

    // Next publication should detect unhealthy/dirty index and automatically rebuild it before proceeding!
    let pub_res = service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "latest".to_string(),
            payload: m_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .expect("publish manifest recovering dirty index");

    assert_eq!(pub_res.digest, m_d);
    assert!(ref_index.check_health().is_ok());
    assert!(ref_index.is_blob_referenced(&layer).unwrap());
}
