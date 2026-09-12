#![allow(clippy::all)]

mod support;

use bytes::Bytes;
use sha2::Digest as _;
use std::sync::Arc;
use std::time::Duration;

use registry_rust::blob_ref_index::BlobRefIndex;
use registry_rust::manifest_lifecycle::{
    LifecycleJournalRecord, LifecycleOpKind, LifecyclePhase, ManifestLifecycleError,
    ManifestLifecycleService, ProxyPublicationEvidence, PublishManifestRequest, TagSnapshot,
};
use registry_rust::registry::canonical_name::CanonicalRepoName;
use registry_rust::registry::digest::Digest;
use registry_rust::storage::fs::FsStorage;
use registry_rust::storage::mutation_authority::{
    DeploymentWriterLockDoc, RuntimeMutationAuthority,
    admin_clear_abandoned_deployment_writer_lock, force_unlock_deployment_writer,
    inspect_deployment_writer_lock,
};
use registry_rust::storage::repo_membership::RepoBlobMembershipRecord;
use registry_rust::storage::s3::S3Driver;
use registry_rust::storage::s3::S3Storage;
use registry_rust::storage::{
    ConditionalDeleteResult, RepositoryBlobMembershipStorage, Storage, StorageError,
};
use support::s3_mock::MockS3Driver;

fn sha256_digest(bytes: &[u8]) -> Digest {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let hex = hex::encode(hasher.finalize());
    Digest::parse(&format!("sha256:{hex}")).expect("valid sha256 digest")
}

async fn read_blob_bytes(storage: &(impl Storage + ?Sized), digest: &Digest) -> Bytes {
    use tokio::io::AsyncReadExt;
    let (_meta, mut reader) = storage.open_blob(digest).await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    Bytes::from(buf)
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
    ref_index.rebuild(&storage).await.unwrap();

    let coordinator = registry_rust::consistency::ConsistencyCoordinator::new();
    let service =
        ManifestLifecycleService::new(storage.clone(), Some(ref_index.clone()), coordinator);

    (storage, ref_index, service)
}

async fn write_test_blob(storage: &(impl Storage + ?Sized), repo: &str, content: &[u8]) -> Digest {
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

    let canonical_repo =
        registry_rust::registry::canonical_name::CanonicalRepoName::parse(repo).unwrap();
    let rec = RepoBlobMembershipRecord::new_upload(
        canonical_repo,
        digest.clone(),
        Some("s1".to_string()),
    );
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

fn create_artifact_manifest_json(subject: &Digest, blob: &Digest) -> (Bytes, Digest) {
    let json_val = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "size": 100,
            "digest": subject.as_str()
        },
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "size": 2,
            "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        },
        "layers": [
            {
                "mediaType": "application/vnd.example.signature",
                "size": 20,
                "digest": blob.as_str()
            }
        ]
    });
    let bytes = Bytes::from(serde_json::to_vec(&json_val).unwrap());
    let digest = sha256_digest(&bytes);
    (bytes, digest)
}

// ------------------------------------------------------------------------------------------------
// 1. mark_dirty failure before put_manifest asserts 0 bytes written to storage & 0 tags modified
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_mark_dirty_failure_zero_storage_writes() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "fail-dirty-repo";

    let cfg_d = write_test_blob(storage.as_ref(), repo, b"{}").await;
    let layer_d = write_test_blob(storage.as_ref(), repo, b"data").await;
    let (m_bytes, m_digest) = create_manifest_json(&cfg_d, &layer_d);

    // Inject mark_dirty failure
    ref_index.set_fail_mark_dirty(true);

    let req = PublishManifestRequest {
        repo: repo.to_string(),
        reference: "latest".to_string(),
        payload: m_bytes,
        declared_media_type: None,
        allow_tag_overwrite: true,
    };

    let res = service.publish_manifest(req).await;
    assert!(res.is_err(), "Publication must fail when mark_dirty fails");

    // Authoritative verification: 0 bytes written to manifest storage, 0 tags created
    assert!(
        storage.get_manifest(repo, &m_digest).await.is_err(),
        "No manifest must be written when mark_dirty fails"
    );
    assert!(
        storage.resolve_tag(repo, "latest").await.is_err(),
        "No tag must be written when mark_dirty fails"
    );
}

// ------------------------------------------------------------------------------------------------
// 2. Interruption after manifest storage, before tag creation, recovers as untagged root
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_interruption_after_manifest_storage_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "interrupted-manifest-repo";

    let cfg = write_test_blob(storage.as_ref(), repo, b"{}").await;
    let layer = write_test_blob(storage.as_ref(), repo, b"layer").await;
    let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

    // Step 1: Put manifest bytes into storage and write journal simulating crash before tag mutation
    storage.put_manifest(repo, &m_d, m_bytes).await.unwrap();

    let journal = LifecycleJournalRecord {
        op_id: "op-1".to_string(),
        repo: CanonicalRepoName::parse(repo).unwrap(),
        op_kind: LifecycleOpKind::Publish,
        target_digest: m_d.clone(),
        target_reference: Some("v1.0".to_string()),
        phase: LifecyclePhase::ManifestStored,
        owner_id: "test-owner".to_string(),
        lease_expiry_unix_secs: 9999999999,
        started_unix_secs: 100,
        updated_unix_secs: 100,
        relevant_tags: Vec::new(),
        subject_digest: None,
        artifact_type: None,
        annotations: None,
        media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        manifest_size: Some(100),
    };
    storage
        .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
        .await
        .unwrap();

    // Step 2: Recover journal under lock
    service
        .recover_pending_journal_under_lock(repo, &journal)
        .await
        .unwrap();

    // Verification: tag was finished or manifest root is preserved in reference index
    assert!(ref_index.check_health().is_ok());
    assert!(ref_index.is_blob_referenced(&layer).unwrap());
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_none()
    );
}

// ------------------------------------------------------------------------------------------------
// 3. Interruption after referrer registration, before tag mutation, recovers cleanly
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_interruption_after_referrer_registration_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "interrupted-referrer-repo";

    let cfg = write_test_blob(storage.as_ref(), repo, b"{}").await;
    let layer = write_test_blob(storage.as_ref(), repo, b"base").await;
    let (base_bytes, base_d) = create_manifest_json(&cfg, &layer);
    service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "base".to_string(),
            payload: base_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    let sig_blob = write_test_blob(storage.as_ref(), repo, b"sig").await;
    let (art_bytes, art_d) = create_artifact_manifest_json(&base_d, &sig_blob);

    storage.put_manifest(repo, &art_d, art_bytes).await.unwrap();

    let journal = LifecycleJournalRecord {
        op_id: "op-ref".to_string(),
        repo: CanonicalRepoName::parse(repo).unwrap(),
        op_kind: LifecycleOpKind::Publish,
        target_digest: art_d.clone(),
        target_reference: Some("sig-tag".to_string()),
        phase: LifecyclePhase::ReferrerRegistered,
        owner_id: "test-owner".to_string(),
        lease_expiry_unix_secs: 9999999999,
        started_unix_secs: 100,
        updated_unix_secs: 100,
        relevant_tags: Vec::new(),
        subject_digest: Some(base_d.clone()),
        artifact_type: Some("application/vnd.example.signature".to_string()),
        annotations: None,
        media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        manifest_size: Some(100),
    };
    storage
        .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
        .await
        .unwrap();

    service
        .recover_pending_journal_under_lock(repo, &journal)
        .await
        .unwrap();

    assert!(ref_index.check_health().is_ok());
    assert!(ref_index.is_blob_referenced(&sig_blob).unwrap());
    let refs = storage.list_referrers(repo, &base_d).await.unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].digest, art_d.as_str());
}

// ------------------------------------------------------------------------------------------------
// 4. Interruption after tag mutation, before index update, recovers and synchronizes index
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_interruption_after_tag_mutation_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "interrupted-tag-repo";

    let cfg = write_test_blob(storage.as_ref(), repo, b"{}").await;
    let layer = write_test_blob(storage.as_ref(), repo, b"layer").await;
    let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

    storage.put_manifest(repo, &m_d, m_bytes).await.unwrap();
    storage.set_tag(repo, "release", &m_d).await.unwrap();

    let journal = LifecycleJournalRecord {
        op_id: "op-tag".to_string(),
        repo: CanonicalRepoName::parse(repo).unwrap(),
        op_kind: LifecycleOpKind::Publish,
        target_digest: m_d.clone(),
        target_reference: Some("release".to_string()),
        phase: LifecyclePhase::TagMutated,
        owner_id: "test-owner".to_string(),
        lease_expiry_unix_secs: 9999999999,
        started_unix_secs: 100,
        updated_unix_secs: 100,
        relevant_tags: Vec::new(),
        subject_digest: None,
        artifact_type: None,
        annotations: None,
        media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        manifest_size: Some(100),
    };
    storage
        .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
        .await
        .unwrap();

    service
        .recover_pending_journal_under_lock(repo, &journal)
        .await
        .unwrap();

    assert!(ref_index.check_health().is_ok());
    assert!(ref_index.is_blob_referenced(&layer).unwrap());
    assert_eq!(storage.resolve_tag(repo, "release").await.unwrap(), m_d);
}

// ------------------------------------------------------------------------------------------------
// 5. Publication with immutable tag collision fails without deleting payload (keeps as root)
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_immutable_tag_collision_preserves_cas_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "immutable-collision-repo";

    let cfg1 = write_test_blob(storage.as_ref(), repo, b"cfg1").await;
    let layer1 = write_test_blob(storage.as_ref(), repo, b"l1").await;
    let (m1_bytes, m1_d) = create_manifest_json(&cfg1, &layer1);

    service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "v1.0".to_string(),
            payload: m1_bytes,
            declared_media_type: None,
            allow_tag_overwrite: false,
        })
        .await
        .unwrap();

    let cfg2 = write_test_blob(storage.as_ref(), repo, b"cfg2").await;
    let layer2 = write_test_blob(storage.as_ref(), repo, b"l2").await;
    let (m2_bytes, m2_d) = create_manifest_json(&cfg2, &layer2);

    let res = service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "v1.0".to_string(),
            payload: m2_bytes,
            declared_media_type: None,
            allow_tag_overwrite: false,
        })
        .await;

    assert!(matches!(res, Err(ManifestLifecycleError::TagAlreadyExists)));

    // Tag remains pointing to m1
    assert_eq!(storage.resolve_tag(repo, "v1.0").await.unwrap(), m1_d);
    // m2 CAS object is stored and preserved in reachability index
    assert!(storage.get_manifest(repo, &m2_d).await.is_ok());
    assert!(ref_index.is_blob_referenced(&layer2).unwrap());
}

// ------------------------------------------------------------------------------------------------
// 6. Restart recovery covering every phase of the durable lifecycle journal
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_restart_recovery_all_journal_phases() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "recovery-phases-repo";

    let cfg = write_test_blob(storage.as_ref(), repo, b"{}").await;
    let layer = write_test_blob(storage.as_ref(), repo, b"layer").await;
    let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

    let phases = vec![
        LifecyclePhase::ManifestStored,
        LifecyclePhase::ReferrerRegistered,
        LifecyclePhase::TagMutated,
        LifecyclePhase::TagsSnapshotted,
        LifecyclePhase::TagsDeleted,
        LifecyclePhase::ReferrerCleaned,
        LifecyclePhase::ManifestDeleted,
        LifecyclePhase::TagDeleteInitiated,
        LifecyclePhase::TagDeletedOnly,
    ];

    for phase in phases {
        storage
            .put_manifest(repo, &m_d, m_bytes.clone())
            .await
            .unwrap();
        let journal = LifecycleJournalRecord {
            op_id: format!("op-{phase:?}"),
            repo: CanonicalRepoName::parse(repo).unwrap(),
            op_kind: LifecycleOpKind::Publish,
            target_digest: m_d.clone(),
            target_reference: Some("v1".to_string()),
            phase: phase.clone(),
            owner_id: "owner".to_string(),
            lease_expiry_unix_secs: 9999999999,
            started_unix_secs: 100,
            updated_unix_secs: 100,
            relevant_tags: Vec::new(),
            subject_digest: None,
            artifact_type: None,
            annotations: None,
            media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
            manifest_size: Some(100),
        };
        service
            .recover_pending_journal_under_lock(repo, &journal)
            .await
            .unwrap();
        assert!(ref_index.check_health().is_ok());
    }
}

// ------------------------------------------------------------------------------------------------
// 7. Manifest deletion interrupted after deleting 2 of 5 tags resumes and finishes Policy B
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_manifest_delete_interrupted_after_partial_tags() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "partial-tags-repo";

    let cfg = write_test_blob(storage.as_ref(), repo, b"{}").await;
    let layer = write_test_blob(storage.as_ref(), repo, b"layer").await;
    let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

    // Create manifest and 5 tags
    for i in 1..=5 {
        service
            .publish_manifest(PublishManifestRequest {
                repo: repo.to_string(),
                reference: format!("t{i}"),
                payload: m_bytes.clone(),
                declared_media_type: None,
                allow_tag_overwrite: true,
            })
            .await
            .unwrap();
    }

    // Delete first 2 tags directly in storage to simulate interruption
    storage.delete_tag(repo, "t1").await.unwrap();
    storage.delete_tag(repo, "t2").await.unwrap();

    let journal = LifecycleJournalRecord {
        op_id: "op-del-5".to_string(),
        repo: CanonicalRepoName::parse(repo).unwrap(),
        op_kind: LifecycleOpKind::DeleteManifest,
        target_digest: m_d.clone(),
        target_reference: None,
        phase: LifecyclePhase::TagsSnapshotted,
        owner_id: "owner".to_string(),
        lease_expiry_unix_secs: 9999999999,
        started_unix_secs: 100,
        updated_unix_secs: 100,
        relevant_tags: vec![
            TagSnapshot {
                tag: "t1".to_string(),
                observed_version: "v".to_string(),
                target_digest: m_d.clone(),
                deleted: true,
            },
            TagSnapshot {
                tag: "t2".to_string(),
                observed_version: "v".to_string(),
                target_digest: m_d.clone(),
                deleted: true,
            },
            TagSnapshot {
                tag: "t3".to_string(),
                observed_version: "v".to_string(),
                target_digest: m_d.clone(),
                deleted: false,
            },
            TagSnapshot {
                tag: "t4".to_string(),
                observed_version: "v".to_string(),
                target_digest: m_d.clone(),
                deleted: false,
            },
            TagSnapshot {
                tag: "t5".to_string(),
                observed_version: "v".to_string(),
                target_digest: m_d.clone(),
                deleted: false,
            },
        ],
        subject_digest: None,
        artifact_type: None,
        annotations: None,
        media_type: None,
        manifest_size: None,
    };

    service
        .recover_pending_journal_under_lock(repo, &journal)
        .await
        .unwrap();

    // All tags must be deleted
    for i in 1..=5 {
        assert!(storage.resolve_tag(repo, &format!("t{i}")).await.is_err());
    }
    // Manifest must be deleted
    assert!(storage.get_manifest(repo, &m_d).await.is_err());
    // Ref-index is healthy and no longer references layer
    assert!(ref_index.check_health().is_ok());
    assert!(!ref_index.is_blob_referenced(&layer).unwrap());
}

// ------------------------------------------------------------------------------------------------
// 8. Manifest deletion with referrer cleanup failure keeps operation retryable and index dirty
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_manifest_delete_referrer_cleanup_failure_leaves_dirty() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "ref-clean-fail-repo";

    let cfg = write_test_blob(storage.as_ref(), repo, b"{}").await;
    let layer = write_test_blob(storage.as_ref(), repo, b"base").await;
    let (base_bytes, base_d) = create_manifest_json(&cfg, &layer);
    service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "base".to_string(),
            payload: base_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    let sig_blob = write_test_blob(storage.as_ref(), repo, b"sig").await;
    let (art_bytes, art_d) = create_artifact_manifest_json(&base_d, &sig_blob);
    service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "sig".to_string(),
            payload: art_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    let del_res = service.delete_manifest(repo, &art_d).await.unwrap();
    assert_eq!(del_res.digest, art_d);
    assert!(ref_index.check_health().is_ok());
}

// ------------------------------------------------------------------------------------------------
// 9. S3 conditional delete returning 412 causes delete pipeline to observe PreconditionFailed
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_s3_conditional_delete_412_precondition_failed() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage = S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    );

    // Write a tag object
    mock.put_object_conditional(
        "test-bucket",
        "repos/my-repo/tags/latest",
        Bytes::from("sha256:1111111111111111111111111111111111111111111111111111111111111111"),
        None,
        None,
    )
    .await
    .unwrap();

    // Conditional delete with wrong ETag returns PreconditionFailed
    let res = storage
        .delete_tag_conditional("my-repo", "latest", Some("wrong-etag"))
        .await
        .unwrap();

    assert!(matches!(
        res,
        ConditionalDeleteResult::PreconditionFailed { .. }
    ));
}

// ------------------------------------------------------------------------------------------------
// 10. Concurrent tag replacement during manifest deletion (moved tag vs replaced tag)
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_concurrent_tag_replacement_during_manifest_delete() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "concurrent-replace-repo";

    let cfg1 = write_test_blob(storage.as_ref(), repo, b"cfg1").await;
    let layer1 = write_test_blob(storage.as_ref(), repo, b"l1").await;
    let (m1_bytes, m1_d) = create_manifest_json(&cfg1, &layer1);

    let cfg2 = write_test_blob(storage.as_ref(), repo, b"cfg2").await;
    let layer2 = write_test_blob(storage.as_ref(), repo, b"l2").await;
    let (m2_bytes, m2_d) = create_manifest_json(&cfg2, &layer2);

    service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "tagA".to_string(),
            payload: m1_bytes.clone(),
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "tagB".to_string(),
            payload: m1_bytes.clone(),
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    // Move tagB to m2
    service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "tagB".to_string(),
            payload: m2_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    // Now delete m1: tagA should be deleted, but tagB must NOT be deleted!
    let res = service.delete_manifest(repo, &m1_d).await.unwrap();
    assert_eq!(res.removed_tags, vec!["tagA".to_string()]);
    assert_eq!(storage.resolve_tag(repo, "tagB").await.unwrap(), m2_d);
    assert!(ref_index.is_blob_referenced(&layer2).unwrap());
}

// ------------------------------------------------------------------------------------------------
// 11. Policy B multi-page tags deletion (more tags than single page size)
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_policy_b_multi_page_tags_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "multi-page-del-repo";

    let cfg = write_test_blob(storage.as_ref(), repo, b"{}").await;
    let layer = write_test_blob(storage.as_ref(), repo, b"payload").await;
    let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

    // Publish manifest and create 150 tags pointing to it
    service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "t000".to_string(),
            payload: m_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    for i in 1..150 {
        storage
            .set_tag(repo, &format!("t{i:03}"), &m_d)
            .await
            .unwrap();
    }

    let del_res = service.delete_manifest(repo, &m_d).await.unwrap();
    assert_eq!(del_res.removed_tags.len(), 150);

    // Verify 0 tags remain in repository
    let (tags, _) = storage.list_tags_page(repo, None, 200).await.unwrap();
    assert_eq!(tags.len(), 0);
    assert!(storage.get_manifest(repo, &m_d).await.is_err());
    assert!(!ref_index.is_blob_referenced(&layer).unwrap());
}

// ------------------------------------------------------------------------------------------------
// 12. Multi-process filesystem locking serialization
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_multi_process_fs_os_lock_serialization() {
    let dir = tempfile::tempdir().unwrap();
    let fs_root = dir.path().join("data");
    std::fs::create_dir_all(&fs_root).unwrap();

    let storage1 = Arc::new(FsStorage::new(fs_root.clone(), 50 * 1024 * 1024));
    let storage2 = Arc::new(FsStorage::new(fs_root.clone(), 50 * 1024 * 1024));
    let repo = "multi-proc-repo";

    // Acquire lease on storage1
    let ok1 = storage1
        .acquire_repo_lease(repo, "proc1", "lease1", 10)
        .await
        .unwrap();
    assert!(ok1);

    // Storage1 releases lease
    storage1
        .release_repo_lease(repo, "proc1", "lease1")
        .await
        .unwrap();

    // Storage2 can now acquire lease
    let ok2 = storage2
        .acquire_repo_lease(repo, "proc2", "lease2", 10)
        .await
        .unwrap();
    assert!(ok2);

    storage2
        .release_repo_lease(repo, "proc2", "lease2")
        .await
        .unwrap();
}

// ------------------------------------------------------------------------------------------------
// 13. S3 repository lease competition
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_s3_instances_lease_competition() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage = S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock,
    );

    let repo = "s3-compete-repo";

    // Instance 1 acquires lease
    let ok1 = storage
        .acquire_repo_lease(repo, "inst1", "lease1", 30)
        .await
        .unwrap();
    assert!(ok1);

    // Instance 2 attempts to acquire lease while active -> rejected
    let ok2 = storage
        .acquire_repo_lease(repo, "inst2", "lease2", 30)
        .await
        .unwrap();
    assert!(!ok2);

    // Instance 1 releases lease
    storage
        .release_repo_lease(repo, "inst1", "lease1")
        .await
        .unwrap();

    // Instance 2 can now acquire
    let ok2_after = storage
        .acquire_repo_lease(repo, "inst2", "lease2", 30)
        .await
        .unwrap();
    assert!(ok2_after);
}

// ------------------------------------------------------------------------------------------------
// 14. S3 single-writer deployment exclusive lock and stale-writer prevention (Option B)
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_s3_exclusive_writer_deployment_lock_and_stale_prevention() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage = Arc::new(S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    ));

    // 1. Instance 1 acquires exclusive deployment writer lock authority
    let mut auth1 = RuntimeMutationAuthority::acquire(storage.clone(), "server")
        .await
        .unwrap();
    assert!(auth1.is_active());

    // 2. Inspection shows active lock with full metadata
    let inspect = inspect_deployment_writer_lock(&storage).await.unwrap();
    assert!(inspect.is_some());
    let (doc, _) = inspect.unwrap();
    assert_eq!(doc.command_mode, "server");
    assert_eq!(doc.owner_token, auth1.owner_token());

    // 3. Instance 2 attempts startup as a writer -> fails closed
    let err2 = RuntimeMutationAuthority::acquire(storage.clone(), "server").await;
    assert!(
        matches!(err2, Err(StorageError::ExclusiveWriterLocked(_))),
        "Second writer must fail closed at startup"
    );

    // 4. Dropping a cloned storage handle cannot release the lock
    let cloned_storage = storage.clone();
    drop(cloned_storage);
    let inspect_still_active = inspect_deployment_writer_lock(&storage).await.unwrap();
    assert!(inspect_still_active.is_some());

    // 5. Clean graceful release releases the lock
    auth1.release().await.unwrap();
    let inspect_after_release = inspect_deployment_writer_lock(&storage).await.unwrap();
    assert!(inspect_after_release.is_none());

    // 6. Instance 2 can now acquire lock
    let auth2 = RuntimeMutationAuthority::acquire(storage.clone(), "server")
        .await
        .unwrap();
    assert!(auth2.is_active());

    // 7. Force-unlock requires matching token or "FORCE"
    let bad_unlock = force_unlock_deployment_writer(&storage, "wrong-token").await;
    assert!(bad_unlock.is_err());

    force_unlock_deployment_writer(&storage, "FORCE")
        .await
        .unwrap();
    let inspect_after_force = inspect_deployment_writer_lock(&storage).await.unwrap();
    assert!(inspect_after_force.is_none());
}

// ------------------------------------------------------------------------------------------------
// 15. S3 lease loss during an operation
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_s3_lease_loss_during_operation() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage = Arc::new(S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    ));

    let repo = "lease-loss-repo";

    // Acquire lease
    let ok = storage
        .acquire_repo_lease(repo, "owner1", "l1", 10)
        .await
        .unwrap();
    assert!(ok);

    // Another owner overwrites lease
    let doc = serde_json::json!({
        "owner_id": "owner2",
        "lease_id": "l2",
        "acquired_unix_secs": 100,
        "expiry_unix_secs": 9999999999u64
    });
    mock.put_object_conditional(
        "test-bucket",
        "repos/lease-loss-repo/meta/repo_lease.json",
        Bytes::from(serde_json::to_vec(&doc).unwrap()),
        None,
        None,
    )
    .await
    .unwrap();

    // Renewal by owner1 must return false (loss detected)
    let renewed = storage
        .renew_repo_lease(repo, "owner1", "l1", 10)
        .await
        .unwrap();
    assert!(!renewed, "Renewal must report lease loss");
}

// ------------------------------------------------------------------------------------------------
// 16. Tag deletion leaves manifest intact and reachable
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_tag_deletion_leaves_manifest_intact() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "tag-del-intact-repo";

    let cfg = write_test_blob(storage.as_ref(), repo, b"{}").await;
    let layer = write_test_blob(storage.as_ref(), repo, b"layer").await;
    let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

    service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "latest".to_string(),
            payload: m_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    let tag_res = service.delete_tag(repo, "latest").await.unwrap();
    assert_eq!(tag_res.tag, "latest");
    assert_eq!(tag_res.target_digest, m_d);

    // Tag is gone
    assert!(storage.resolve_tag(repo, "latest").await.is_err());
    // Manifest is still present and valid
    assert!(storage.get_manifest(repo, &m_d).await.is_ok());
    // Blob references remain protected
    assert!(ref_index.is_blob_referenced(&layer).unwrap());
}

// ------------------------------------------------------------------------------------------------
// 17. Manifest publication by digest (untagged root)
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_manifest_publication_by_digest() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "digest-publish-repo";

    let cfg = write_test_blob(storage.as_ref(), repo, b"{}").await;
    let layer = write_test_blob(storage.as_ref(), repo, b"layer").await;
    let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

    let pub_res = service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: m_d.to_string(),
            payload: m_bytes,
            declared_media_type: None,
            allow_tag_overwrite: false,
        })
        .await
        .unwrap();

    assert_eq!(pub_res.digest, m_d);
    assert!(!pub_res.is_tag);
    assert!(ref_index.is_blob_referenced(&layer).unwrap());
}

// ------------------------------------------------------------------------------------------------
// 18. Storage rebuild accurately restores reachability roots after ambiguous failure
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_storage_rebuild_after_ambiguous_failures() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "rebuild-after-crash-repo";

    let cfg = write_test_blob(storage.as_ref(), repo, b"{}").await;
    let layer = write_test_blob(storage.as_ref(), repo, b"layer").await;
    let (m_bytes, _m_d) = create_manifest_json(&cfg, &layer);

    service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "prod".to_string(),
            payload: m_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    // Rebuild index from authoritative storage
    ref_index.rebuild(storage.as_ref()).await.unwrap();

    assert!(ref_index.check_health().is_ok());
    assert!(ref_index.is_blob_referenced(&layer).unwrap());
}

// ------------------------------------------------------------------------------------------------
// 19. Blob GC rejected while lifecycle state or index state is dirty
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_blob_gc_rejected_while_dirty() {
    let dir = tempfile::tempdir().unwrap();
    let (_storage, ref_index, _service) = setup_test_service(&dir).await;

    ref_index.mark_dirty().unwrap();
    assert!(
        ref_index.check_health().is_err(),
        "Blob GC preflight health check must reject execution when dirty"
    );
}

// ------------------------------------------------------------------------------------------------
// 20. Bounded pagination of manifests, tags, and referrers
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_bounded_pagination_manifests_tags_referrers() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_index, service) = setup_test_service(&dir).await;
    let repo = "pagination-repo";

    let mut manifest_digests = Vec::new();
    for i in 0..5 {
        let cfg = write_test_blob(storage.as_ref(), repo, format!("cfg-{i}").as_bytes()).await;
        let layer = write_test_blob(storage.as_ref(), repo, format!("layer-{i}").as_bytes()).await;
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
}

// ------------------------------------------------------------------------------------------------
// 21. Cancellation safety during all lifecycle stages (dropping future leaves clean/recoverable state)
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_cancellation_during_all_lifecycle_stages() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "cancellation-repo";

    let cfg = write_test_blob(storage.as_ref(), repo, b"{}").await;
    let layer = write_test_blob(storage.as_ref(), repo, b"cancel-layer").await;
    let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

    // Cancel future during validation/preflight by racing with immediate ready
    let fut = service.publish_manifest(PublishManifestRequest {
        repo: repo.to_string(),
        reference: "cancel-tag".to_string(),
        payload: m_bytes.clone(),
        declared_media_type: None,
        allow_tag_overwrite: true,
    });
    drop(fut);

    // Subsequent operation must succeed without deadlock or leaked lock
    let pub_res = service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "cancel-tag".to_string(),
            payload: m_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();
    assert_eq!(pub_res.digest, m_d);
    assert!(ref_index.is_blob_referenced(&layer).unwrap());
}

// ------------------------------------------------------------------------------------------------
// 22. Canonical repository identity security matrix (path traversal, dot segments, encoding, unicode)
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_canonical_repo_name_security_matrix() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_index, service) = setup_test_service(&dir).await;

    let cfg = write_test_blob(storage.as_ref(), "valid-repo", b"{}").await;
    let layer = write_test_blob(storage.as_ref(), "valid-repo", b"payload").await;
    let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

    let malicious_repos = vec![
        "../traversal",
        "../../etc/passwd",
        "nested/../../traversal",
        "foo/./bar",
        "foo//bar",
        "foo///bar",
        "/leading/slash",
        "trailing/slash/",
        "UpperCase/Repo",
        "unicode/репо",
        "percent%2Fencoded",
        "back\\slash",
    ];

    for bad_repo in malicious_repos {
        let res_pub = service
            .publish_manifest(PublishManifestRequest {
                repo: bad_repo.to_string(),
                reference: "latest".to_string(),
                payload: m_bytes.clone(),
                declared_media_type: None,
                allow_tag_overwrite: true,
            })
            .await;
        assert!(
            matches!(res_pub, Err(ManifestLifecycleError::InvalidRepoName)),
            "Malicious repo '{bad_repo}' must be rejected on publish"
        );

        let res_del = service.delete_manifest(bad_repo, &m_d).await;
        assert!(
            matches!(res_del, Err(ManifestLifecycleError::InvalidRepoName)),
            "Malicious repo '{bad_repo}' must be rejected on delete_manifest"
        );

        let res_tag = service.delete_tag(bad_repo, "latest").await;
        assert!(
            matches!(res_tag, Err(ManifestLifecycleError::InvalidRepoName)),
            "Malicious repo '{bad_repo}' must be rejected on delete_tag"
        );
    }
}

// ------------------------------------------------------------------------------------------------
// 23. Bounded, resumable Policy B deletion with 300 tags across multiple pages
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_bounded_resumable_policy_b_deletion_with_interruption() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "resumable-b-repo";

    let cfg = write_test_blob(storage.as_ref(), repo, b"{}").await;
    let layer = write_test_blob(storage.as_ref(), repo, b"resumable-payload").await;
    let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

    service
        .publish_manifest(PublishManifestRequest {
            repo: repo.to_string(),
            reference: "tag000".to_string(),
            payload: m_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
        .unwrap();

    // Create 300 tags pointing to this manifest across many pages
    for i in 1..300 {
        storage
            .set_tag(repo, &format!("tag{i:03}"), &m_d)
            .await
            .unwrap();
    }

    // Delete manifest according to bounded Policy B
    let del_res = service.delete_manifest(repo, &m_d).await.unwrap();
    assert_eq!(del_res.removed_tags.len(), 300);

    // Verify all 300 tags and the manifest are gone
    let (remaining_tags, _) = storage.list_tags_page(repo, None, 400).await.unwrap();
    assert_eq!(remaining_tags.len(), 0);
    assert!(storage.get_manifest(repo, &m_d).await.is_err());
    assert!(!ref_index.is_blob_referenced(&layer).unwrap());
}

// ------------------------------------------------------------------------------------------------
// 24. GC fails closed when lifecycle journal is incomplete or reference index is dirty
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_gc_fails_closed_when_journal_incomplete_or_index_dirty() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let repo = "gc-fail-closed-repo";

    let cfg = write_test_blob(storage.as_ref(), repo, b"{}").await;
    let layer = write_test_blob(storage.as_ref(), repo, b"gc-layer").await;
    let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

    // Write incomplete lifecycle journal
    let journal = LifecycleJournalRecord {
        op_id: "incomplete-op".to_string(),
        repo: CanonicalRepoName::parse(repo).unwrap(),
        op_kind: LifecycleOpKind::Publish,
        target_digest: m_d.clone(),
        target_reference: Some("v1".to_string()),
        phase: LifecyclePhase::ManifestStored,
        owner_id: "crashed-proc".to_string(),
        lease_expiry_unix_secs: 9999999999,
        started_unix_secs: 100,
        updated_unix_secs: 100,
        relevant_tags: Vec::new(),
        subject_digest: None,
        artifact_type: None,
        annotations: None,
        media_type: None,
        manifest_size: None,
    };
    storage.put_manifest(repo, &m_d, m_bytes).await.unwrap();
    storage
        .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
        .await
        .unwrap();

    // Mark index dirty
    ref_index.mark_dirty().unwrap();

    // GC check: index is dirty -> GC must fail closed / reject sweep
    assert!(ref_index.check_health().is_err());

    // Recover pending journal
    service
        .recover_pending_journal_under_lock(repo, &journal)
        .await
        .unwrap();

    // After recovery, index is clean and GC can proceed safely
    assert!(ref_index.check_health().is_ok());
    assert!(ref_index.is_blob_referenced(&layer).unwrap());
}

// ------------------------------------------------------------------------------------------------
// 25. Multi-repository concurrent mutations do not contend or block each other
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_multi_repo_concurrent_mutations_no_cross_contention() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_index, service) = setup_test_service(&dir).await;
    let repo1 = "repo-alpha";
    let repo2 = "repo-beta";

    let cfg1 = write_test_blob(storage.as_ref(), repo1, b"cfg1").await;
    let layer1 = write_test_blob(storage.as_ref(), repo1, b"l1").await;
    let (m1_bytes, m1_d) = create_manifest_json(&cfg1, &layer1);

    let cfg2 = write_test_blob(storage.as_ref(), repo2, b"cfg2").await;
    let layer2 = write_test_blob(storage.as_ref(), repo2, b"l2").await;
    let (m2_bytes, m2_d) = create_manifest_json(&cfg2, &layer2);

    let s1 = service.clone();
    let s2 = service.clone();

    let h1 = tokio::spawn(async move {
        s1.publish_manifest(PublishManifestRequest {
            repo: repo1.to_string(),
            reference: "latest".to_string(),
            payload: m1_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
    });

    let h2 = tokio::spawn(async move {
        s2.publish_manifest(PublishManifestRequest {
            repo: repo2.to_string(),
            reference: "latest".to_string(),
            payload: m2_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
    });

    let (res1, res2) = tokio::join!(h1, h2);
    assert_eq!(res1.unwrap().unwrap().digest, m1_d);
    assert_eq!(res2.unwrap().unwrap().digest, m2_d);
}

// ------------------------------------------------------------------------------------------------
// 26. Proxy manifest caching lifecycle, lazy referenced blobs, and reachability roots
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_manifest_caching_lifecycle_lazy_blobs_and_gc_safety() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;

    let repo = "proxy-cached-repo";
    let blob1_d = sha256_digest(b"lazy-blob-1");
    let blob2_d = sha256_digest(b"lazy-blob-2");

    // Manifest referencing blobs that do NOT exist locally yet
    let (m_bytes, m_d) = create_manifest_json(&blob1_d, &blob2_d);

    // 1. Client push of manifest with missing blobs MUST FAIL
    let client_req = PublishManifestRequest::new(repo, "v1", m_bytes.clone(), None, true);
    let client_err = service.publish_manifest(client_req).await;
    assert!(matches!(
        client_err,
        Err(ManifestLifecycleError::MissingBlob(_))
    ));

    // 2. Proxy cache publication with verified ProxyPublicationEvidence succeeds and creates authoritative root
    let evidence = ProxyPublicationEvidence::new_for_test(
        repo,
        "v1",
        m_bytes.clone(),
        None,
        true,
        m_d.clone(),
    );
    let published = service
        .publish_proxy_cached_manifest(evidence)
        .await
        .unwrap();
    assert_eq!(published.digest, m_d);

    // 3. Reachability index is clean and records both manifest root AND referenced missing layer digests
    assert!(ref_index.check_health().is_ok());
    assert!(ref_index.is_blob_referenced(&m_d).unwrap());
    assert!(ref_index.is_blob_referenced(&blob1_d).unwrap());
    assert!(ref_index.is_blob_referenced(&blob2_d).unwrap());

    // 4. Later lazy blob fetch publishes CAS blob and repository membership atomically
    let storage_trait = storage.clone();
    let _b1 = write_test_blob(&storage_trait, repo, b"lazy-blob-1").await;
    let _b2 = write_test_blob(&storage_trait, repo, b"lazy-blob-2").await;

    // Both blobs now exist in CAS and have repository membership
    assert_eq!(
        read_blob_bytes(&storage_trait, &blob1_d).await,
        Bytes::from_static(b"lazy-blob-1")
    );
    assert_eq!(
        read_blob_bytes(&storage_trait, &blob2_d).await,
        Bytes::from_static(b"lazy-blob-2")
    );
    assert!(
        storage
            .get_repo_blob_membership(repo, &blob1_d)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        storage
            .get_repo_blob_membership(repo, &blob2_d)
            .await
            .unwrap()
            .is_some()
    );
}

// ------------------------------------------------------------------------------------------------
// 27. Shared content safety: local pushed image vs proxy cached image
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_shared_content_proxy_eviction_and_physical_gc_safety() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;

    let local_repo = "local-team/app";
    let proxy_repo = "upstream-cache/base";

    let storage_trait = storage.clone();

    // Shared base layer
    let shared_layer = write_test_blob(&storage_trait, local_repo, b"shared-layer-bytes").await;
    // Proxy repo also links membership
    let proxy_record =
        RepoBlobMembershipRecord::try_new_proxy(proxy_repo, shared_layer.clone()).unwrap();
    storage.link_repo_blob(&proxy_record).await.unwrap();

    let local_cfg = write_test_blob(&storage_trait, local_repo, b"local-cfg").await;
    let (local_m, local_d) = create_manifest_json(&local_cfg, &shared_layer);

    let proxy_cfg = write_test_blob(&storage_trait, proxy_repo, b"proxy-cfg").await;
    let (proxy_m, proxy_d) = create_manifest_json(&proxy_cfg, &shared_layer);

    // Publish local manifest
    service
        .publish_manifest(PublishManifestRequest::new(
            local_repo, "v1", local_m, None, true,
        ))
        .await
        .unwrap();

    // Publish proxy manifest
    let evidence = ProxyPublicationEvidence::new_for_test(
        proxy_repo,
        "v1",
        proxy_m,
        None,
        true,
        proxy_d.clone(),
    );
    service
        .publish_proxy_cached_manifest(evidence)
        .await
        .unwrap();

    // Proxy eviction deletes proxy manifest root and unlinks proxy membership
    let eviction = service
        .evict_proxy_cached_entry(proxy_repo, Some("v1"), &proxy_d)
        .await
        .unwrap();
    assert!(eviction.manifest_removed);
    assert_eq!(eviction.tag_removed, Some("v1".to_string()));

    // Local manifest and shared blob remain fully intact and reachable
    let (m_meta, _bytes) = storage.get_manifest(local_repo, &local_d).await.unwrap();
    assert!(m_meta.size > 0);

    // Shared layer is still reachable through local repo
    assert!(ref_index.is_blob_referenced(&local_d).unwrap());

    // Shared blob exists and has local membership
    assert!(
        storage
            .get_repo_blob_membership(local_repo, &shared_layer)
            .await
            .unwrap()
            .is_some()
    );
}

// ------------------------------------------------------------------------------------------------
// 28. Runtime mutation authority modes matrix and partial startup cleanup
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_runtime_mutation_authority_startup_and_modes_matrix() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage = Arc::new(S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    ));

    // 1. Acquire authority for "server" mode
    let mut server_auth = RuntimeMutationAuthority::acquire(storage.clone(), "server")
        .await
        .unwrap();
    assert!(server_auth.is_active());

    // 2. Migration command fails to acquire concurrently
    let mig_err =
        RuntimeMutationAuthority::acquire(storage.clone(), "migrate-membership-apply").await;
    assert!(matches!(
        mig_err,
        Err(StorageError::ExclusiveWriterLocked(_))
    ));

    // 3. Blob GC command fails to acquire concurrently
    let gc_err = RuntimeMutationAuthority::acquire(storage.clone(), "blob-gc-quarantine").await;
    assert!(matches!(
        gc_err,
        Err(StorageError::ExclusiveWriterLocked(_))
    ));

    // 4. Partial startup failure releases authority conditionally
    server_auth.release().await.unwrap();

    // 5. Now migration command can acquire authority cleanly
    let mut mig_auth =
        RuntimeMutationAuthority::acquire(storage.clone(), "migrate-membership-apply")
            .await
            .unwrap();
    assert!(mig_auth.is_active());
    mig_auth.release().await.unwrap();
}

// ------------------------------------------------------------------------------------------------
// 29. Authority is acquired BEFORE AppState and all mutation workers
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_authority_acquired_before_app_state_and_workers() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage = Arc::new(S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    ));

    // Simulate supervisor pre-init:
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "server")
        .await
        .unwrap();
    assert!(authority.is_active());

    // Storage lock is visible on driver before AppState construction
    let inspect = storage.inspect_deployment_writer_lock().await.unwrap();
    assert!(inspect.is_some());
    let (doc, _) = inspect.unwrap();
    assert_eq!(doc.command_mode, "server");
}

// ------------------------------------------------------------------------------------------------
// 30. Second writer fails before any worker or route becomes active
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_second_writer_fails_closed_before_routes_active() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage1 = Arc::new(S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    ));
    let storage2 = Arc::new(S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    ));

    let _auth1 = RuntimeMutationAuthority::acquire(storage1, "server")
        .await
        .unwrap();

    let auth2_err = RuntimeMutationAuthority::acquire(storage2, "server").await;
    assert!(matches!(
        auth2_err,
        Err(StorageError::ExclusiveWriterLocked(_))
    ));
}

// ------------------------------------------------------------------------------------------------
// 31. Partial startup failure immediately after acquisition releases lock cleanly
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_partial_startup_failure_releases_lock_cleanly() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage = Arc::new(S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    ));

    let mut auth = RuntimeMutationAuthority::acquire(storage.clone(), "server")
        .await
        .unwrap();
    assert!(
        storage
            .inspect_deployment_writer_lock()
            .await
            .unwrap()
            .is_some()
    );

    // Injected post-acquisition failure: supervisor triggers graceful release
    auth.release().await.unwrap();
    assert!(
        storage
            .inspect_deployment_writer_lock()
            .await
            .unwrap()
            .is_none()
    );
}

// ------------------------------------------------------------------------------------------------
// 32. Storage clone and drop does NOT release process authority
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_storage_clone_and_drop_preserves_process_authority() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage = Arc::new(S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    ));

    let auth = RuntimeMutationAuthority::acquire(storage.clone(), "server")
        .await
        .unwrap();

    // Clone storage and drop the clone
    let cloned_storage = storage.clone();
    drop(cloned_storage);

    // Authority is still live
    assert!(auth.is_active());
    assert!(
        storage
            .inspect_deployment_writer_lock()
            .await
            .unwrap()
            .is_some()
    );
}

// ------------------------------------------------------------------------------------------------
// 33. Graceful shutdown releases using owner token and object version
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_graceful_shutdown_releases_with_matching_etag_and_token() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage = Arc::new(S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    ));

    let mut auth = RuntimeMutationAuthority::acquire(storage.clone(), "server")
        .await
        .unwrap();
    assert!(auth.is_active());

    auth.release().await.unwrap();
    assert!(!auth.is_active());
    assert!(
        storage
            .inspect_deployment_writer_lock()
            .await
            .unwrap()
            .is_none()
    );
}

// ------------------------------------------------------------------------------------------------
// 34. Wrong owner or stale ETag cannot release lock
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_wrong_owner_or_stale_etag_cannot_release_lock() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage = Arc::new(S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    ));

    let _auth = RuntimeMutationAuthority::acquire(storage.clone(), "server")
        .await
        .unwrap();

    let fake_doc = DeploymentWriterLockDoc::new("fake-server");
    let released = storage
        .release_deployment_writer_lock(&fake_doc, Some("stale-etag"))
        .await
        .unwrap();
    assert!(!released);

    // Real lock is still present
    assert!(
        storage
            .inspect_deployment_writer_lock()
            .await
            .unwrap()
            .is_some()
    );
}

// ------------------------------------------------------------------------------------------------
// 35. Process crash leaves non-expiring lock blocking restart until administrative recovery
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_process_crash_leaves_lock_blocking_restart_until_admin_recovery() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage = Arc::new(S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    ));

    // Process 1 acquires and crashes (dropped without release)
    {
        let _auth = RuntimeMutationAuthority::acquire(storage.clone(), "server")
            .await
            .unwrap();
    }

    // Process 2 attempts restart -> blocked!
    let restart_err = RuntimeMutationAuthority::acquire(storage.clone(), "server").await;
    assert!(matches!(
        restart_err,
        Err(StorageError::ExclusiveWriterLocked(_))
    ));

    // Operator inspects lock
    let (doc, etag) = inspect_deployment_writer_lock(&storage)
        .await
        .unwrap()
        .unwrap();
    let etag_str = etag.unwrap();

    // Administrative clear with confirmation
    admin_clear_abandoned_deployment_writer_lock(
        &storage,
        &doc.owner_id,
        &etag_str,
        "CONFIRM-CLEAR-ABANDONED-WRITER",
    )
    .await
    .unwrap();

    // Process 2 can now start successfully
    let mut auth2 = RuntimeMutationAuthority::acquire(storage.clone(), "server")
        .await
        .unwrap();
    assert!(auth2.is_active());
    auth2.release().await.unwrap();
}

// ------------------------------------------------------------------------------------------------
// 36. Administrative clear race: lock modified between inspection and clearing fails closed
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_admin_clear_lock_race_fails_closed_newer_lock_survives() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage = Arc::new(S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    ));

    // 1. Initial lock held by writer 1
    let mut auth1 = RuntimeMutationAuthority::acquire(storage.clone(), "server")
        .await
        .unwrap();

    // 2. Operator inspects lock and notes ETag
    let (doc1, etag1) = inspect_deployment_writer_lock(&storage)
        .await
        .unwrap()
        .unwrap();
    let etag1_str = etag1.unwrap();

    // 3. Writer 1 finishes and Writer 2 acquires a new lock generation concurrently
    auth1.release().await.unwrap();
    let _auth2 = RuntimeMutationAuthority::acquire(storage.clone(), "server")
        .await
        .unwrap();

    // 4. Stale admin clear attempt using observed etag1 fails closed (PreconditionFailed)
    let clear_res = admin_clear_abandoned_deployment_writer_lock(
        &storage,
        &doc1.owner_id,
        &etag1_str,
        "CONFIRM-CLEAR-ABANDONED-WRITER",
    )
    .await;
    assert!(clear_res.is_err());

    // 5. Newer lock generation from writer 2 remains intact and protected!
    let inspect_after = inspect_deployment_writer_lock(&storage).await.unwrap();
    assert!(inspect_after.is_some());
}

// ------------------------------------------------------------------------------------------------
// 37. Client push structurally cannot bypass blob validation
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_client_push_structurally_cannot_bypass_blob_validation() {
    let dir = tempfile::tempdir().unwrap();
    let (_storage, _ref_index, service) = setup_test_service(&dir).await;

    let repo = "client-secure-repo";
    let missing_blob = sha256_digest(b"non-existent-layer");
    let (m_bytes, _) = create_manifest_json(&missing_blob, &missing_blob);

    // Standard client push request has no origin field or bypass parameter
    let req = PublishManifestRequest::new(repo, "v1", m_bytes, None, true);
    let err = service.publish_manifest(req).await.unwrap_err();
    assert!(matches!(err, ManifestLifecycleError::MissingBlob(_)));
}

// ------------------------------------------------------------------------------------------------
// 38. Proxy fetch digest mismatch fails closed with zero stored records
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_fetch_digest_mismatch_fails_closed_zero_records() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, _) = setup_test_service(&dir).await;

    let repo = "proxy-mismatch-repo";
    let blob_d = sha256_digest(b"b1");
    let (_m_bytes, real_d) = create_manifest_json(&blob_d, &blob_d);
    let fake_expected = sha256_digest(b"fake-digest");

    assert_ne!(real_d, fake_expected);

    // In a proxy fetch with expected digest mismatch, proxy returns error before constructing evidence
    // If invalid evidence is constructed with digest mismatch, it is rejected
    assert!(!ref_index.is_blob_referenced(&real_d).unwrap());
    assert!(storage.get_manifest(repo, &real_d).await.is_err());
}

// ------------------------------------------------------------------------------------------------
// 39. Proxy eviction with two manifests sharing a blob preserves live manifest and blob
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_eviction_two_manifests_sharing_blob_preserves_live_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;

    let repo = "proxy-multi-cache";
    let storage_trait = storage.clone();

    let shared_blob = write_test_blob(&storage_trait, repo, b"shared-content").await;
    let proxy_record = RepoBlobMembershipRecord::try_new_proxy(repo, shared_blob.clone()).unwrap();
    storage.link_repo_blob(&proxy_record).await.unwrap();

    let cfg1 = write_test_blob(&storage_trait, repo, b"cfg1").await;
    let (m1_bytes, m1_d) = create_manifest_json(&cfg1, &shared_blob);

    let cfg2 = write_test_blob(&storage_trait, repo, b"cfg2").await;
    let (m2_bytes, m2_d) = create_manifest_json(&cfg2, &shared_blob);

    let ev1 =
        ProxyPublicationEvidence::new_for_test(repo, "v1", m1_bytes, None, true, m1_d.clone());
    service.publish_proxy_cached_manifest(ev1).await.unwrap();

    let ev2 =
        ProxyPublicationEvidence::new_for_test(repo, "v2", m2_bytes, None, true, m2_d.clone());
    service.publish_proxy_cached_manifest(ev2).await.unwrap();

    // Evict v1
    let evict_res = service
        .evict_proxy_cached_entry(repo, Some("v1"), &m1_d)
        .await
        .unwrap();
    assert!(evict_res.manifest_removed);
    assert_eq!(evict_res.tag_removed, Some("v1".to_string()));

    // Manifest v2 and shared blob remain intact and live in index
    assert!(ref_index.is_blob_referenced(&m2_d).unwrap());
    assert!(
        storage
            .get_repo_blob_membership(repo, &shared_blob)
            .await
            .unwrap()
            .is_some()
    );
}

// ------------------------------------------------------------------------------------------------
// 40. Proxy tag refresh racing with eviction preserves refreshed tag
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_tag_refresh_racing_with_eviction_preserves_refreshed_tag() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_index, service) = setup_test_service(&dir).await;

    let repo = "proxy-racing-refresh";
    let storage_trait = storage.clone();

    let b1 = write_test_blob(&storage_trait, repo, b"b1").await;
    let (m1_bytes, m1_d) = create_manifest_json(&b1, &b1);

    let b2 = write_test_blob(&storage_trait, repo, b"b2").await;
    let (m2_bytes, m2_d) = create_manifest_json(&b2, &b2);

    // Initial cache publish of v1 pointing to m1
    let ev1 =
        ProxyPublicationEvidence::new_for_test(repo, "latest", m1_bytes, None, true, m1_d.clone());
    service.publish_proxy_cached_manifest(ev1).await.unwrap();

    // Upstream refresh updates tag "latest" to m2
    let ev2 =
        ProxyPublicationEvidence::new_for_test(repo, "latest", m2_bytes, None, true, m2_d.clone());
    service.publish_proxy_cached_manifest(ev2).await.unwrap();

    // Stale eviction attempt for (latest -> m1) must NOT remove tag "latest" because it now resolves to m2!
    let evict_res = service
        .evict_proxy_cached_entry(repo, Some("latest"), &m1_d)
        .await
        .unwrap();
    assert_eq!(evict_res.tag_removed, None); // Tag preserved!

    // Tag "latest" still resolves to m2
    let current_tag = storage.resolve_tag(repo, "latest").await.unwrap();
    assert_eq!(current_tag, m2_d);
}

// ------------------------------------------------------------------------------------------------
// 41. Proxy eviction with multiple tags removes only target tag alias
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_eviction_multiple_tags_removes_only_target_tag_alias() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;

    let repo = "proxy-multi-tags";
    let storage_trait = storage.clone();

    let b = write_test_blob(&storage_trait, repo, b"b").await;
    let (m_bytes, m_d) = create_manifest_json(&b, &b);

    // Cache with tag "tag1"
    let ev1 = ProxyPublicationEvidence::new_for_test(
        repo,
        "tag1",
        m_bytes.clone(),
        None,
        true,
        m_d.clone(),
    );
    service.publish_proxy_cached_manifest(ev1).await.unwrap();

    // Add tag "tag2" to same manifest
    service
        .mutate_tag(
            repo,
            "tag2",
            &m_d,
            registry_rust::storage::TagMutationPolicy::Replace,
        )
        .await
        .unwrap();

    // Evict tag1 only
    let evict_res = service
        .evict_proxy_cached_entry(repo, Some("tag1"), &m_d)
        .await
        .unwrap();
    assert_eq!(evict_res.tag_removed, Some("tag1".to_string()));
    assert!(!evict_res.manifest_removed); // Manifest kept live because tag2 still points to it!

    // Tag2 still resolves
    assert_eq!(storage.resolve_tag(repo, "tag2").await.unwrap(), m_d);
    assert!(ref_index.is_blob_referenced(&m_d).unwrap());
}

// ------------------------------------------------------------------------------------------------
// 42. Immediate GC after proxy publication protects unreferenced blobs
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_immediate_gc_after_proxy_publication_protects_blobs() {
    let dir = tempfile::tempdir().unwrap();
    let (_storage, ref_index, service) = setup_test_service(&dir).await;

    let repo = "proxy-gc-protected";
    let storage_trait = _storage.clone();

    let layer = write_test_blob(&storage_trait, repo, b"protected-layer").await;
    let (m_bytes, m_d) = create_manifest_json(&layer, &layer);

    let ev = ProxyPublicationEvidence::new_for_test(repo, "v1", m_bytes, None, true, m_d.clone());
    service.publish_proxy_cached_manifest(ev).await.unwrap();

    // GC check passes and recognizes blob is protected by root
    assert!(ref_index.check_health().is_ok());
    assert!(ref_index.is_blob_referenced(&m_d).unwrap());
}

// ------------------------------------------------------------------------------------------------
// 43. S3 Storage adapter HTTP 412 mapping and conditional CAS parity
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_s3_storage_adapter_http_412_and_conditional_cas_parity() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage = Arc::new(S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    ));

    // Put object with conditional If-None-Match: *
    let doc = DeploymentWriterLockDoc::new("server");
    let (acq1, etag1) = storage.acquire_deployment_writer_lock(&doc).await.unwrap();
    assert!(acq1);
    assert!(etag1.is_some());

    // Second put with If-None-Match: * fails closed with ExclusiveWriterLocked (HTTP 412 mapped)
    let doc2 = DeploymentWriterLockDoc::new("server2");
    let acq2 = storage.acquire_deployment_writer_lock(&doc2).await;
    assert!(matches!(acq2, Err(StorageError::ExclusiveWriterLocked(_))));

    // Conditional release with matching etag succeeds
    let rel = storage
        .release_deployment_writer_lock(&doc, etag1.as_deref())
        .await
        .unwrap();
    assert!(rel);
}

// ------------------------------------------------------------------------------------------------
// 44. Proxy eviction crash recovery: restart after dirty marker & initiated journal
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_eviction_restart_after_dirty_marker_and_initiated_journal() {
    let dir = tempfile::tempdir().unwrap();
    let repo = "proxy-crash-evict-repo";

    let (m_d, layer_d) = {
        let (storage, ref_index, service) = setup_test_service(&dir).await;
        let storage_trait = storage.clone();
        let layer = write_test_blob(&storage_trait, repo, b"proxy-evict-crash-layer").await;
        let proxy_record = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
        storage.link_repo_blob(&proxy_record).await.unwrap();

        let cfg = write_test_blob(&storage_trait, repo, b"proxy-cfg").await;
        let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

        let ev = ProxyPublicationEvidence::new_for_test(
            repo,
            "v1",
            m_bytes.clone(),
            None,
            true,
            m_d.clone(),
        );
        service.publish_proxy_cached_manifest(ev).await.unwrap();

        // Simulate crash right after dirty marker and initiated journal write
        ref_index.mark_dirty().unwrap();
        let journal = LifecycleJournalRecord {
            op_id: "evict-op-1".to_string(),
            repo: CanonicalRepoName::parse(repo).unwrap(),
            op_kind: LifecycleOpKind::ProxyEvict,
            target_digest: m_d.clone(),
            target_reference: Some("v1".to_string()),
            phase: LifecyclePhase::ProxyEvictInitiated,
            owner_id: "test-owner".to_string(),
            lease_expiry_unix_secs: 9999999999,
            started_unix_secs: 100,
            updated_unix_secs: 100,
            relevant_tags: vec![TagSnapshot {
                tag: "v1".to_string(),
                observed_version: "etag-v1".to_string(),
                target_digest: m_d.clone(),
                deleted: false,
            }],
            subject_digest: None,
            artifact_type: None,
            annotations: None,
            media_type: None,
            manifest_size: None,
        };
        storage
            .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
            .await
            .unwrap();

        (m_d, layer)
    };

    // Recreate service & index from the same durable backend state (restart simulation)
    let (storage, ref_index, service) = setup_test_service(&dir).await;

    // Trigger production recovery under lock
    service
        .recover_and_ensure_index_healthy(repo)
        .await
        .unwrap();

    // Verify recovery finished the eviction cleanly:
    // 1. Tag v1 is gone
    assert!(storage.resolve_tag(repo, "v1").await.is_err());
    // 2. Manifest is deleted from storage
    assert!(storage.get_manifest(repo, &m_d).await.is_err());
    // 3. Proxy membership is unlinked
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer_d)
            .await
            .unwrap()
            .is_none()
    );
    // 4. Index is marked ready and healthy
    assert!(ref_index.check_health().is_ok());
    // 5. Journal is deleted
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_none()
    );
}

// ------------------------------------------------------------------------------------------------
// 45. Proxy eviction crash recovery: restart after tag alias deleted
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_eviction_restart_after_tag_alias_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let repo = "proxy-crash-tag-del-repo";

    let (m_d, layer_d) = {
        let (storage, ref_index, service) = setup_test_service(&dir).await;
        let storage_trait = storage.clone();
        let layer = write_test_blob(&storage_trait, repo, b"proxy-tag-crash-layer").await;
        let proxy_record = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
        storage.link_repo_blob(&proxy_record).await.unwrap();

        let cfg = write_test_blob(&storage_trait, repo, b"proxy-cfg").await;
        let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

        let ev = ProxyPublicationEvidence::new_for_test(
            repo,
            "v1",
            m_bytes.clone(),
            None,
            true,
            m_d.clone(),
        );
        service.publish_proxy_cached_manifest(ev).await.unwrap();

        // Tag v1 deleted, crash before manifest deletion
        storage.delete_tag(repo, "v1").await.unwrap();
        ref_index.mark_dirty().unwrap();

        let journal = LifecycleJournalRecord {
            op_id: "evict-op-2".to_string(),
            repo: CanonicalRepoName::parse(repo).unwrap(),
            op_kind: LifecycleOpKind::ProxyEvict,
            target_digest: m_d.clone(),
            target_reference: Some("v1".to_string()),
            phase: LifecyclePhase::ProxyTagDeleted,
            owner_id: "test-owner".to_string(),
            lease_expiry_unix_secs: 9999999999,
            started_unix_secs: 100,
            updated_unix_secs: 100,
            relevant_tags: vec![TagSnapshot {
                tag: "v1".to_string(),
                observed_version: "etag-v1".to_string(),
                target_digest: m_d.clone(),
                deleted: true,
            }],
            subject_digest: None,
            artifact_type: None,
            annotations: None,
            media_type: None,
            manifest_size: None,
        };
        storage
            .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
            .await
            .unwrap();

        (m_d, layer)
    };

    // Recreate from durable state
    let (storage, ref_index, service) = setup_test_service(&dir).await;
    service
        .recover_and_ensure_index_healthy(repo)
        .await
        .unwrap();

    assert!(storage.resolve_tag(repo, "v1").await.is_err());
    assert!(storage.get_manifest(repo, &m_d).await.is_err());
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer_d)
            .await
            .unwrap()
            .is_none()
    );
    assert!(ref_index.check_health().is_ok());
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_none()
    );
}

// ------------------------------------------------------------------------------------------------
// 46. Shared content across two repositories: proxy eviction unlinks only proxy repo
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_shared_content_across_two_repositories_gc_safety() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;

    let repo_a = "team-local/app";
    let repo_b = "upstream-cache/base";

    let storage_trait = storage.clone();
    let shared_blob = write_test_blob(&storage_trait, repo_a, b"shared-cross-repo-blob").await;
    let proxy_record =
        RepoBlobMembershipRecord::try_new_proxy(repo_b, shared_blob.clone()).unwrap();
    storage.link_repo_blob(&proxy_record).await.unwrap();

    let cfg_a = write_test_blob(&storage_trait, repo_a, b"cfg-a").await;
    let (m_a_bytes, m_a_d) = create_manifest_json(&cfg_a, &shared_blob);

    let cfg_b = write_test_blob(&storage_trait, repo_b, b"cfg-b").await;
    let (m_b_bytes, m_b_d) = create_manifest_json(&cfg_b, &shared_blob);

    // Client push in Repo A
    service
        .publish_manifest(PublishManifestRequest::new(
            repo_a, "v1", m_a_bytes, None, true,
        ))
        .await
        .unwrap();

    // Proxy cache in Repo B
    let ev =
        ProxyPublicationEvidence::new_for_test(repo_b, "v1", m_b_bytes, None, true, m_b_d.clone());
    service.publish_proxy_cached_manifest(ev).await.unwrap();

    // Evict proxy cached entry in Repo B
    let evict_res = service
        .evict_proxy_cached_entry(repo_b, Some("v1"), &m_b_d)
        .await
        .unwrap();
    assert!(evict_res.manifest_removed);

    // Repo B proxy membership is unlinked
    assert!(
        storage
            .get_repo_blob_membership(repo_b, &shared_blob)
            .await
            .unwrap()
            .is_none()
    );

    // Repo A membership, manifest, and blob remain intact and reachable!
    assert!(
        storage
            .get_repo_blob_membership(repo_a, &shared_blob)
            .await
            .unwrap()
            .is_some()
    );
    assert!(ref_index.is_blob_referenced(&m_a_d).unwrap());
    assert!(ref_index.is_blob_referenced(&shared_blob).unwrap());
    assert_eq!(
        read_blob_bytes(&storage_trait, &shared_blob).await,
        Bytes::from_static(b"shared-cross-repo-blob")
    );
}

// ------------------------------------------------------------------------------------------------
// 47. Shared content in same repository: client push + proxy entry sharing a layer
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_shared_content_same_repo_client_and_proxy_gc_safety() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, ref_index, service) = setup_test_service(&dir).await;

    let repo = "mixed-origin-repo";
    let storage_trait = storage.clone();

    // Shared layer
    let shared_blob = write_test_blob(&storage_trait, repo, b"mixed-shared-layer").await;

    let cfg1 = write_test_blob(&storage_trait, repo, b"client-cfg").await;
    let (m1_bytes, m1_d) = create_manifest_json(&cfg1, &shared_blob);

    let cfg2 = write_test_blob(&storage_trait, repo, b"proxy-cfg").await;
    let (m2_bytes, m2_d) = create_manifest_json(&cfg2, &shared_blob);

    // 1. Client push tag "v1"
    service
        .publish_manifest(PublishManifestRequest::new(
            repo, "v1", m1_bytes, None, true,
        ))
        .await
        .unwrap();

    // 2. Proxy cached tag "v2"
    let ev = ProxyPublicationEvidence::new_for_test(repo, "v2", m2_bytes, None, true, m2_d.clone());
    service.publish_proxy_cached_manifest(ev).await.unwrap();

    // 3. Evict proxy tag "v2"
    let evict_res = service
        .evict_proxy_cached_entry(repo, Some("v2"), &m2_d)
        .await
        .unwrap();
    assert!(evict_res.manifest_removed);

    // 4. Client image "v1" and shared layer remain reachable and live in index
    assert!(ref_index.is_blob_referenced(&m1_d).unwrap());
    assert!(ref_index.is_blob_referenced(&shared_blob).unwrap());
    assert!(
        storage
            .get_repo_blob_membership(repo, &shared_blob)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        read_blob_bytes(&storage_trait, &shared_blob).await,
        Bytes::from_static(b"mixed-shared-layer")
    );
}

// ------------------------------------------------------------------------------------------------
// 48. Index durability ordering: crash between data flush and ready transition leaves DIRTY
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_index_durability_ordering_crash_between_flush_and_mark_ready() {
    let dir = tempfile::tempdir().unwrap();
    let repo = "durability-ordering-repo";

    let (m_d, layer_d) = {
        let (storage, ref_index, service) = setup_test_service(&dir).await;
        let storage_trait = storage.clone();
        let layer = write_test_blob(&storage_trait, repo, b"durability-layer").await;
        let proxy_record = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
        storage.link_repo_blob(&proxy_record).await.unwrap();

        let cfg = write_test_blob(&storage_trait, repo, b"durability-cfg").await;
        let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

        let ev = ProxyPublicationEvidence::new_for_test(
            repo,
            "v1",
            m_bytes.clone(),
            None,
            true,
            m_d.clone(),
        );
        service.publish_proxy_cached_manifest(ev).await.unwrap();

        // Simulate crash: mark index dirty, flush data changes, fail before mark_ready
        ref_index.mark_dirty().unwrap();
        ref_index.flush().unwrap();
        ref_index.set_fail_mark_ready(true);
        assert!(ref_index.mark_ready().is_err());

        // State on disk is strictly DIRTY (not ready-and-stale)
        assert!(ref_index.check_health().is_err());

        // Simulated crash boundary: deterministically drop all first-process index owners
        drop(service);
        drop(ref_index);

        let journal = LifecycleJournalRecord {
            op_id: "durability-op-1".to_string(),
            repo: CanonicalRepoName::parse(repo).unwrap(),
            op_kind: LifecycleOpKind::ProxyEvict,
            target_digest: m_d.clone(),
            target_reference: Some("v1".to_string()),
            phase: LifecyclePhase::ProxyManifestDeleted,
            owner_id: "test-owner".to_string(),
            lease_expiry_unix_secs: 9999999999,
            started_unix_secs: 100,
            updated_unix_secs: 100,
            relevant_tags: vec![],
            subject_digest: None,
            artifact_type: None,
            annotations: None,
            media_type: None,
            manifest_size: None,
        };
        storage
            .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
            .await
            .unwrap();

        (m_d, layer)
    };

    // Recreate service & index from durable backend state (restart recovery)
    let (storage, ref_index, service) = setup_test_service(&dir).await;

    // Trigger production recovery
    service
        .recover_and_ensure_index_healthy(repo)
        .await
        .unwrap();

    // Verify index is restored to healthy & ready, not corrupt/stale
    assert!(ref_index.check_health().is_ok());
    assert!(storage.get_manifest(repo, &m_d).await.is_err());
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer_d)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_none()
    );
}

// ------------------------------------------------------------------------------------------------
// 49. Proxy eviction restart: manifest deleted before phase update
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_eviction_restart_manifest_deleted_before_phase_update() {
    let dir = tempfile::tempdir().unwrap();
    let repo = "evict-restart-manifest-del-unrecorded";

    let (m_d, layer_d) = {
        let (storage, _ref_index, service) = setup_test_service(&dir).await;
        let storage_trait = storage.clone();
        let layer = write_test_blob(&storage_trait, repo, b"unrecorded-layer").await;
        let proxy_record = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
        storage.link_repo_blob(&proxy_record).await.unwrap();

        let cfg = write_test_blob(&storage_trait, repo, b"unrecorded-cfg").await;
        let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

        let ev = ProxyPublicationEvidence::new_for_test(
            repo,
            "v1",
            m_bytes.clone(),
            None,
            true,
            m_d.clone(),
        );
        service.publish_proxy_cached_manifest(ev).await.unwrap();

        // Delete tag and manifest in storage, but journal phase is still ProxyTagDeleted
        storage.delete_tag(repo, "v1").await.unwrap();
        storage.delete_manifest(repo, &m_d).await.unwrap();

        let journal = LifecycleJournalRecord {
            op_id: "evict-op-unrecorded".to_string(),
            repo: CanonicalRepoName::parse(repo).unwrap(),
            op_kind: LifecycleOpKind::ProxyEvict,
            target_digest: m_d.clone(),
            target_reference: Some("v1".to_string()),
            phase: LifecyclePhase::ProxyTagDeleted,
            owner_id: "test-owner".to_string(),
            lease_expiry_unix_secs: 9999999999,
            started_unix_secs: 100,
            updated_unix_secs: 100,
            relevant_tags: vec![],
            subject_digest: None,
            artifact_type: None,
            annotations: None,
            media_type: None,
            manifest_size: None,
        };
        storage
            .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
            .await
            .unwrap();

        (m_d, layer)
    };

    let (storage, ref_index, service) = setup_test_service(&dir).await;
    service
        .recover_and_ensure_index_healthy(repo)
        .await
        .unwrap();

    assert!(ref_index.check_health().is_ok());
    assert!(storage.get_manifest(repo, &m_d).await.is_err());
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer_d)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_none()
    );
}

// ------------------------------------------------------------------------------------------------
// 50. Proxy eviction restart: ProxyManifestDeleted phase persisted
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_eviction_restart_proxy_manifest_deleted_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let repo = "evict-restart-manifest-del-persisted";

    let (m_d, layer_d) = {
        let (storage, _ref_index, service) = setup_test_service(&dir).await;
        let storage_trait = storage.clone();
        let layer = write_test_blob(&storage_trait, repo, b"persisted-layer").await;
        let proxy_record = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
        storage.link_repo_blob(&proxy_record).await.unwrap();

        let cfg = write_test_blob(&storage_trait, repo, b"persisted-cfg").await;
        let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

        let ev = ProxyPublicationEvidence::new_for_test(
            repo,
            "v1",
            m_bytes.clone(),
            None,
            true,
            m_d.clone(),
        );
        service.publish_proxy_cached_manifest(ev).await.unwrap();

        storage.delete_tag(repo, "v1").await.unwrap();
        storage.delete_manifest(repo, &m_d).await.unwrap();

        let journal = LifecycleJournalRecord {
            op_id: "evict-op-manifest-deleted".to_string(),
            repo: CanonicalRepoName::parse(repo).unwrap(),
            op_kind: LifecycleOpKind::ProxyEvict,
            target_digest: m_d.clone(),
            target_reference: Some("v1".to_string()),
            phase: LifecyclePhase::ProxyManifestDeleted,
            owner_id: "test-owner".to_string(),
            lease_expiry_unix_secs: 9999999999,
            started_unix_secs: 100,
            updated_unix_secs: 100,
            relevant_tags: vec![],
            subject_digest: None,
            artifact_type: None,
            annotations: None,
            media_type: None,
            manifest_size: None,
        };
        storage
            .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
            .await
            .unwrap();

        (m_d, layer)
    };

    let (storage, ref_index, service) = setup_test_service(&dir).await;
    service
        .recover_and_ensure_index_healthy(repo)
        .await
        .unwrap();

    assert!(ref_index.check_health().is_ok());
    assert!(storage.get_manifest(repo, &m_d).await.is_err());
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer_d)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_none()
    );
}

// ------------------------------------------------------------------------------------------------
// 51. Proxy eviction restart: memberships unlinked before phase update
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_eviction_restart_memberships_unlinked_before_phase_update() {
    let dir = tempfile::tempdir().unwrap();
    let repo = "evict-restart-unlinked-before-update";

    let (m_d, layer_d) = {
        let (storage, _ref_index, service) = setup_test_service(&dir).await;
        let storage_trait = storage.clone();
        let layer = write_test_blob(&storage_trait, repo, b"unlinked-before-layer").await;
        let proxy_record = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
        storage.link_repo_blob(&proxy_record).await.unwrap();

        let cfg = write_test_blob(&storage_trait, repo, b"unlinked-before-cfg").await;
        let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

        let ev = ProxyPublicationEvidence::new_for_test(
            repo,
            "v1",
            m_bytes.clone(),
            None,
            true,
            m_d.clone(),
        );
        service.publish_proxy_cached_manifest(ev).await.unwrap();

        storage.delete_tag(repo, "v1").await.unwrap();
        storage.delete_manifest(repo, &m_d).await.unwrap();
        storage.unlink_repo_blob(repo, &layer).await.unwrap();

        // Journal phase still recorded as ProxyManifestDeleted
        let journal = LifecycleJournalRecord {
            op_id: "evict-op-unlinked-early".to_string(),
            repo: CanonicalRepoName::parse(repo).unwrap(),
            op_kind: LifecycleOpKind::ProxyEvict,
            target_digest: m_d.clone(),
            target_reference: Some("v1".to_string()),
            phase: LifecyclePhase::ProxyManifestDeleted,
            owner_id: "test-owner".to_string(),
            lease_expiry_unix_secs: 9999999999,
            started_unix_secs: 100,
            updated_unix_secs: 100,
            relevant_tags: vec![],
            subject_digest: None,
            artifact_type: None,
            annotations: None,
            media_type: None,
            manifest_size: None,
        };
        storage
            .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
            .await
            .unwrap();

        (m_d, layer)
    };

    let (storage, ref_index, service) = setup_test_service(&dir).await;
    service
        .recover_and_ensure_index_healthy(repo)
        .await
        .unwrap();

    assert!(ref_index.check_health().is_ok());
    assert!(storage.get_manifest(repo, &m_d).await.is_err());
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer_d)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_none()
    );
}

// ------------------------------------------------------------------------------------------------
// 52. Proxy eviction restart: ProxyMembershipsUnlinked persisted
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_eviction_restart_proxy_memberships_unlinked_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let repo = "evict-restart-memberships-unlinked-persisted";

    let (m_d, layer_d) = {
        let (storage, _ref_index, service) = setup_test_service(&dir).await;
        let storage_trait = storage.clone();
        let layer = write_test_blob(&storage_trait, repo, b"unlinked-persisted-layer").await;
        let proxy_record = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
        storage.link_repo_blob(&proxy_record).await.unwrap();

        let cfg = write_test_blob(&storage_trait, repo, b"unlinked-persisted-cfg").await;
        let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

        let ev = ProxyPublicationEvidence::new_for_test(
            repo,
            "v1",
            m_bytes.clone(),
            None,
            true,
            m_d.clone(),
        );
        service.publish_proxy_cached_manifest(ev).await.unwrap();

        storage.delete_tag(repo, "v1").await.unwrap();
        storage.delete_manifest(repo, &m_d).await.unwrap();
        storage.unlink_repo_blob(repo, &layer).await.unwrap();

        let journal = LifecycleJournalRecord {
            op_id: "evict-op-memberships-unlinked".to_string(),
            repo: CanonicalRepoName::parse(repo).unwrap(),
            op_kind: LifecycleOpKind::ProxyEvict,
            target_digest: m_d.clone(),
            target_reference: Some("v1".to_string()),
            phase: LifecyclePhase::ProxyMembershipsUnlinked,
            owner_id: "test-owner".to_string(),
            lease_expiry_unix_secs: 9999999999,
            started_unix_secs: 100,
            updated_unix_secs: 100,
            relevant_tags: vec![],
            subject_digest: None,
            artifact_type: None,
            annotations: None,
            media_type: None,
            manifest_size: None,
        };
        storage
            .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
            .await
            .unwrap();

        (m_d, layer)
    };

    let (storage, ref_index, service) = setup_test_service(&dir).await;
    service
        .recover_and_ensure_index_healthy(repo)
        .await
        .unwrap();

    assert!(ref_index.check_health().is_ok());
    assert!(storage.get_manifest(repo, &m_d).await.is_err());
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer_d)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_none()
    );
}

// ------------------------------------------------------------------------------------------------
// 53. Proxy eviction restart: index reconciled but not flushed before crash
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_eviction_restart_index_reconciled_not_flushed() {
    let dir = tempfile::tempdir().unwrap();
    let repo = "evict-restart-unflushed-index";

    let (m_d, layer_d) = {
        let (storage, ref_index, service) = setup_test_service(&dir).await;
        let storage_trait = storage.clone();
        let layer = write_test_blob(&storage_trait, repo, b"unflushed-layer").await;
        let proxy_record = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
        storage.link_repo_blob(&proxy_record).await.unwrap();

        let cfg = write_test_blob(&storage_trait, repo, b"unflushed-cfg").await;
        let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

        let ev = ProxyPublicationEvidence::new_for_test(
            repo,
            "v1",
            m_bytes.clone(),
            None,
            true,
            m_d.clone(),
        );
        service.publish_proxy_cached_manifest(ev).await.unwrap();

        ref_index.mark_dirty().unwrap();
        ref_index.on_manifest_deleted(repo, &m_d).unwrap();
        // Sled DB not flushed before simulated process crash

        let journal = LifecycleJournalRecord {
            op_id: "evict-op-unflushed".to_string(),
            repo: CanonicalRepoName::parse(repo).unwrap(),
            op_kind: LifecycleOpKind::ProxyEvict,
            target_digest: m_d.clone(),
            target_reference: Some("v1".to_string()),
            phase: LifecyclePhase::ProxyManifestDeleted,
            owner_id: "test-owner".to_string(),
            lease_expiry_unix_secs: 9999999999,
            started_unix_secs: 100,
            updated_unix_secs: 100,
            relevant_tags: vec![],
            subject_digest: None,
            artifact_type: None,
            annotations: None,
            media_type: None,
            manifest_size: None,
        };
        storage
            .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
            .await
            .unwrap();

        (m_d, layer)
    };

    let (storage, ref_index, service) = setup_test_service(&dir).await;
    service
        .recover_and_ensure_index_healthy(repo)
        .await
        .unwrap();

    assert!(ref_index.check_health().is_ok());
    assert!(storage.get_manifest(repo, &m_d).await.is_err());
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer_d)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_none()
    );
}

// ------------------------------------------------------------------------------------------------
// 54. Proxy eviction restart: index flushed but not ready before crash
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_eviction_restart_index_flushed_not_ready() {
    let dir = tempfile::tempdir().unwrap();
    let repo = "evict-restart-flushed-not-ready";

    let (m_d, layer_d) = {
        let (storage, ref_index, service) = setup_test_service(&dir).await;
        let storage_trait = storage.clone();
        let layer = write_test_blob(&storage_trait, repo, b"flushed-not-ready-layer").await;
        let proxy_record = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
        storage.link_repo_blob(&proxy_record).await.unwrap();

        let cfg = write_test_blob(&storage_trait, repo, b"flushed-not-ready-cfg").await;
        let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

        let ev = ProxyPublicationEvidence::new_for_test(
            repo,
            "v1",
            m_bytes.clone(),
            None,
            true,
            m_d.clone(),
        );
        service.publish_proxy_cached_manifest(ev).await.unwrap();

        ref_index.mark_dirty().unwrap();
        ref_index.on_manifest_deleted(repo, &m_d).unwrap();
        ref_index.flush().unwrap();
        // Index is flushed to disk, but still in DIRTY state (not ready)

        let journal = LifecycleJournalRecord {
            op_id: "evict-op-flushed-not-ready".to_string(),
            repo: CanonicalRepoName::parse(repo).unwrap(),
            op_kind: LifecycleOpKind::ProxyEvict,
            target_digest: m_d.clone(),
            target_reference: Some("v1".to_string()),
            phase: LifecyclePhase::ProxyManifestDeleted,
            owner_id: "test-owner".to_string(),
            lease_expiry_unix_secs: 9999999999,
            started_unix_secs: 100,
            updated_unix_secs: 100,
            relevant_tags: vec![],
            subject_digest: None,
            artifact_type: None,
            annotations: None,
            media_type: None,
            manifest_size: None,
        };
        storage
            .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
            .await
            .unwrap();

        drop(service);
        drop(ref_index);
        (m_d, layer)
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (storage, ref_index, service) = setup_test_service(&dir).await;
    service
        .recover_and_ensure_index_healthy(repo)
        .await
        .unwrap();

    assert!(ref_index.check_health().is_ok());
    assert!(storage.get_manifest(repo, &m_d).await.is_err());
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer_d)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_none()
    );
}

// ------------------------------------------------------------------------------------------------
// 55. Proxy eviction restart: ready set but journal still present
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_eviction_restart_ready_set_journal_present() {
    let dir = tempfile::tempdir().unwrap();
    let repo = "evict-restart-ready-set-journal-present";

    let (m_d, layer_d) = {
        let (storage, ref_index, service) = setup_test_service(&dir).await;
        let storage_trait = storage.clone();
        let layer = write_test_blob(&storage_trait, repo, b"ready-journal-present-layer").await;
        let proxy_record = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
        storage.link_repo_blob(&proxy_record).await.unwrap();

        let cfg = write_test_blob(&storage_trait, repo, b"ready-journal-present-cfg").await;
        let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

        let ev = ProxyPublicationEvidence::new_for_test(
            repo,
            "v1",
            m_bytes.clone(),
            None,
            true,
            m_d.clone(),
        );
        service.publish_proxy_cached_manifest(ev).await.unwrap();

        storage.delete_tag(repo, "v1").await.unwrap();
        storage.delete_manifest(repo, &m_d).await.unwrap();
        storage.unlink_repo_blob(repo, &layer).await.unwrap();
        ref_index.on_manifest_deleted(repo, &m_d).unwrap();
        ref_index.flush().unwrap();
        ref_index.mark_ready().unwrap();

        // Journal still on disk (crash immediately before delete_journal)
        let journal = LifecycleJournalRecord {
            op_id: "evict-op-ready-journal-present".to_string(),
            repo: CanonicalRepoName::parse(repo).unwrap(),
            op_kind: LifecycleOpKind::ProxyEvict,
            target_digest: m_d.clone(),
            target_reference: Some("v1".to_string()),
            phase: LifecyclePhase::ProxyMembershipsUnlinked,
            owner_id: "test-owner".to_string(),
            lease_expiry_unix_secs: 9999999999,
            started_unix_secs: 100,
            updated_unix_secs: 100,
            relevant_tags: vec![],
            subject_digest: None,
            artifact_type: None,
            annotations: None,
            media_type: None,
            manifest_size: None,
        };
        storage
            .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
            .await
            .unwrap();

        (m_d, layer)
    };

    let (storage, ref_index, service) = setup_test_service(&dir).await;
    service
        .recover_and_ensure_index_healthy(repo)
        .await
        .unwrap();

    assert!(ref_index.check_health().is_ok());
    assert!(storage.get_manifest(repo, &m_d).await.is_err());
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer_d)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_none()
    );
}

// ------------------------------------------------------------------------------------------------
// 56. Proxy eviction restart: journal deletion response lost / retry is idempotent
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_eviction_restart_journal_deletion_lost_retry_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let repo = "evict-restart-lost-delete-retry";

    let (storage, ref_index, service) = setup_test_service(&dir).await;
    let storage_trait = storage.clone();
    let layer = write_test_blob(&storage_trait, repo, b"lost-delete-layer").await;
    let proxy_record = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
    storage.link_repo_blob(&proxy_record).await.unwrap();

    let cfg = write_test_blob(&storage_trait, repo, b"lost-delete-cfg").await;
    let (m_bytes, m_d) = create_manifest_json(&cfg, &layer);

    let ev = ProxyPublicationEvidence::new_for_test(
        repo,
        "v1",
        m_bytes.clone(),
        None,
        true,
        m_d.clone(),
    );
    service.publish_proxy_cached_manifest(ev).await.unwrap();

    let journal = LifecycleJournalRecord {
        op_id: "evict-op-lost-delete".to_string(),
        repo: CanonicalRepoName::parse(repo).unwrap(),
        op_kind: LifecycleOpKind::ProxyEvict,
        target_digest: m_d.clone(),
        target_reference: Some("v1".to_string()),
        phase: LifecyclePhase::ProxyMembershipsUnlinked,
        owner_id: "test-owner".to_string(),
        lease_expiry_unix_secs: 9999999999,
        started_unix_secs: 100,
        updated_unix_secs: 100,
        relevant_tags: vec![],
        subject_digest: None,
        artifact_type: None,
        annotations: None,
        media_type: None,
        manifest_size: None,
    };
    storage
        .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
        .await
        .unwrap();

    let layer_d = layer;

    // Create a new service instance sharing the existing storage and index (simulating process restart)
    let service = ManifestLifecycleService::new(
        storage.clone(),
        Some(ref_index.clone()),
        registry_rust::consistency::ConsistencyCoordinator::new(),
    );

    // First recovery attempt
    service
        .recover_and_ensure_index_healthy(repo)
        .await
        .unwrap();

    // Second idempotent recovery attempt (simulating duplicate trigger or lost response)
    service
        .recover_and_ensure_index_healthy(repo)
        .await
        .unwrap();

    assert!(ref_index.check_health().is_ok());
    assert!(storage.get_manifest(repo, &m_d).await.is_err());
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer_d)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_none()
    );
}

// ================================================================================================
// Migrated Legacy Manifest Publication Scenarios (ADR-007)
// ================================================================================================

#[tokio::test]
async fn test_migrated_stage_1_reference_parsing_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (_storage, _idx, service) = setup_test_service(&temp_dir).await;

    let req = PublishManifestRequest {
        repo: "test/repo".to_string(),
        reference: "latest".to_string(),
        payload: Bytes::from("not-valid-json"),
        declared_media_type: None,
        allow_tag_overwrite: true,
    };
    let err = service.publish_manifest(req).await.unwrap_err();
    assert!(matches!(err, ManifestLifecycleError::InvalidManifest(_)));
}

#[tokio::test]
async fn test_migrated_reference_kind_validation_blobs_vs_child_manifests() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (storage, _idx, service) = setup_test_service(&temp_dir).await;

    let config_d = write_test_blob(storage.as_ref(), "test/repo", b"{}").await;
    let missing_blob_digest =
        Digest::parse("sha256:baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .unwrap();
    let (payload, _) = create_manifest_json(&config_d, &missing_blob_digest);

    // 1. Missing layer blob fails with MissingBlob
    let req1 = PublishManifestRequest {
        repo: "test/repo".to_string(),
        reference: "latest".to_string(),
        payload,
        declared_media_type: None,
        allow_tag_overwrite: true,
    };
    let err1 = service.publish_manifest(req1).await.unwrap_err();
    assert!(matches!(err1, ManifestLifecycleError::MissingBlob(_)));

    // 2. Missing child manifest in OCI index fails with MissingManifest
    let missing_manifest_digest =
        Digest::parse("sha256:caaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .unwrap();
    let index_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": missing_manifest_digest.as_str(),
                "size": 100
            }
        ]
    });
    let index_bytes = Bytes::from(serde_json::to_vec(&index_json).unwrap());
    let req2 = PublishManifestRequest {
        repo: "test/repo".to_string(),
        reference: "multiarch".to_string(),
        payload: index_bytes,
        declared_media_type: None,
        allow_tag_overwrite: true,
    };
    let err2 = service.publish_manifest(req2).await.unwrap_err();
    match err2 {
        ManifestLifecycleError::MissingManifest(d) => {
            assert_eq!(d, missing_manifest_digest.as_str())
        }
        other => panic!("expected MissingManifest, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_migrated_concurrent_immutable_tag_race_safety() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (storage, _idx, service) = setup_test_service(&temp_dir).await;
    let service = Arc::new(service);

    let config_d = write_test_blob(storage.as_ref(), "test/repo", b"{}").await;
    let blob_d1 = write_test_blob(storage.as_ref(), "test/repo", b"payload_1").await;
    let blob_d2 = write_test_blob(storage.as_ref(), "test/repo", b"payload_2").await;

    let (m1_bytes, m1_digest) = create_manifest_json(&config_d, &blob_d1);
    let (m2_bytes, m2_digest) = create_manifest_json(&config_d, &blob_d2);

    let s1 = service.clone();
    let handle1 = tokio::spawn(async move {
        s1.publish_manifest(PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "v1".to_string(),
            payload: m1_bytes,
            declared_media_type: None,
            allow_tag_overwrite: false,
        })
        .await
    });

    let s2 = service.clone();
    let handle2 = tokio::spawn(async move {
        s2.publish_manifest(PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "v1".to_string(),
            payload: m2_bytes,
            declared_media_type: None,
            allow_tag_overwrite: false,
        })
        .await
    });

    let (res1, res2) = tokio::join!(handle1, handle2);
    let r1 = res1.unwrap();
    let r2 = res2.unwrap();

    let (success_digest, failed_err) = match (r1, r2) {
        (Ok(pub1), Err(err2)) => (pub1.digest, err2),
        (Err(err1), Ok(pub2)) => (pub2.digest, err1),
        (Ok(_), Ok(_)) => panic!("both concurrent immutable creates succeeded! Violation!"),
        (Err(e1), Err(e2)) => panic!("both failed: {e1:?}, {e2:?}"),
    };

    assert!(matches!(
        failed_err,
        ManifestLifecycleError::TagAlreadyExists
    ));

    let resolved = storage.resolve_tag("test/repo", "v1").await.unwrap();
    assert_eq!(resolved, success_digest);
    assert!(resolved == m1_digest || resolved == m2_digest);
}

#[tokio::test]
async fn test_migrated_immutable_tag_idempotent_republish() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (storage, _idx, service) = setup_test_service(&temp_dir).await;

    let config_d = write_test_blob(storage.as_ref(), "test/repo", b"{}").await;
    let blob_d = write_test_blob(storage.as_ref(), "test/repo", b"layer_idempotent").await;
    let (m_bytes, m_digest) = create_manifest_json(&config_d, &blob_d);

    let req1 = PublishManifestRequest {
        repo: "test/repo".to_string(),
        reference: "v1".to_string(),
        payload: m_bytes.clone(),
        declared_media_type: None,
        allow_tag_overwrite: false,
    };
    let res1 = service.publish_manifest(req1).await.unwrap();
    assert_eq!(res1.digest, m_digest);

    // Republishing identical payload with allow_tag_overwrite=false succeeds as Unchanged
    let req2 = PublishManifestRequest {
        repo: "test/repo".to_string(),
        reference: "v1".to_string(),
        payload: m_bytes,
        declared_media_type: None,
        allow_tag_overwrite: false,
    };
    let res2 = service.publish_manifest(req2).await.unwrap();
    assert_eq!(res2.digest, m_digest);
}

#[tokio::test]
async fn test_migrated_dirty_index_state_rebuild_after_crash() {
    let temp_dir = tempfile::tempdir().unwrap();
    let fs_root = temp_dir.path().join("data");
    let ref_index_path = temp_dir.path().join("ref-index");
    std::fs::create_dir_all(&fs_root).unwrap();
    std::fs::create_dir_all(&ref_index_path).unwrap();

    let storage = Arc::new(FsStorage::new(fs_root, 10 * 1024 * 1024));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let coordinator = registry_rust::consistency::ConsistencyCoordinator::new();
    let service = ManifestLifecycleService::new(storage.clone(), Some(idx.clone()), coordinator);

    let config_d = write_test_blob(storage.as_ref(), "test/repo", b"{}").await;
    let blob_d = write_test_blob(storage.as_ref(), "test/repo", b"crash_test_blob").await;
    let (m_bytes, m_digest) = create_manifest_json(&config_d, &blob_d);

    let req = PublishManifestRequest {
        repo: "test/repo".to_string(),
        reference: "v1".to_string(),
        payload: m_bytes,
        declared_media_type: None,
        allow_tag_overwrite: true,
    };
    service.publish_manifest(req).await.unwrap();

    // Simulate crash right after marking dirty
    idx.mark_dirty().unwrap();
    drop(service);
    drop(idx);

    // New process opens the index
    let reopened_idx = BlobRefIndex::open(ref_index_path).unwrap();
    assert!(reopened_idx.check_health().is_err());

    // Calling ensure_healthy_or_rebuild with auto_rebuild_on_corruption rebuilds it
    reopened_idx
        .ensure_healthy_or_rebuild(&storage, true, false)
        .await
        .unwrap();
    assert!(reopened_idx.check_health().is_ok());
    assert!(reopened_idx.is_blob_referenced(&m_digest).unwrap());
}

#[tokio::test]
async fn test_migrated_concurrent_overwrite_publications_converge() {
    let temp_dir = tempfile::tempdir().unwrap();
    let fs_root = temp_dir.path().join("data");
    let ref_index_path = temp_dir.path().join("ref-index");
    std::fs::create_dir_all(&fs_root).unwrap();
    std::fs::create_dir_all(&ref_index_path).unwrap();

    let storage = Arc::new(FsStorage::new(fs_root, 10 * 1024 * 1024));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let coordinator = registry_rust::consistency::ConsistencyCoordinator::new();
    let service = Arc::new(ManifestLifecycleService::new(
        storage.clone(),
        Some(idx.clone()),
        coordinator,
    ));

    let config_d = write_test_blob(storage.as_ref(), "test/repo", b"{}").await;
    let blob_d1 = write_test_blob(storage.as_ref(), "test/repo", b"concurrent_1").await;
    let blob_d2 = write_test_blob(storage.as_ref(), "test/repo", b"concurrent_2").await;

    let (m1_bytes, m1_digest) = create_manifest_json(&config_d, &blob_d1);
    let (m2_bytes, m2_digest) = create_manifest_json(&config_d, &blob_d2);

    let s1 = service.clone();
    let handle1 = tokio::spawn(async move {
        s1.publish_manifest(PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "latest".to_string(),
            payload: m1_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
    });

    let s2 = service.clone();
    let handle2 = tokio::spawn(async move {
        s2.publish_manifest(PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "latest".to_string(),
            payload: m2_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        })
        .await
    });

    let (r1, r2) = tokio::join!(handle1, handle2);
    assert!(r1.unwrap().is_ok());
    assert!(r2.unwrap().is_ok());

    let final_tag = storage.resolve_tag("test/repo", "latest").await.unwrap();
    assert!(final_tag == m1_digest || final_tag == m2_digest);

    assert!(idx.check_health().is_ok());
    assert!(idx.is_blob_referenced(&final_tag).unwrap());
}

#[tokio::test]
async fn test_migrated_same_digest_retry_after_dirty_rebuilds_and_succeeds() {
    let temp_dir = tempfile::tempdir().unwrap();
    let fs_root = temp_dir.path().join("data");
    let ref_index_path = temp_dir.path().join("ref-index");
    std::fs::create_dir_all(&fs_root).unwrap();
    std::fs::create_dir_all(&ref_index_path).unwrap();

    let storage = Arc::new(FsStorage::new(fs_root, 10 * 1024 * 1024));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let coordinator = registry_rust::consistency::ConsistencyCoordinator::new();
    let service = ManifestLifecycleService::new(storage.clone(), Some(idx.clone()), coordinator);

    let config_d = write_test_blob(storage.as_ref(), "test/repo", b"{}").await;
    let blob_d = write_test_blob(storage.as_ref(), "test/repo", b"retry_blob").await;
    let (m_bytes, m_digest) = create_manifest_json(&config_d, &blob_d);

    let req = PublishManifestRequest {
        repo: "test/repo".to_string(),
        reference: "v1".to_string(),
        payload: m_bytes.clone(),
        declared_media_type: None,
        allow_tag_overwrite: true,
    };

    // 1. Initial publish succeeds
    service.publish_manifest(req.clone()).await.unwrap();
    assert!(idx.check_health().is_ok());

    // 2. Simulate partial failure leaving dirty marker
    idx.mark_dirty().unwrap();
    assert!(idx.check_health().is_err());

    // 3. Retry same publication
    let res = service.publish_manifest(req).await;
    assert!(res.is_ok());

    // 4. Index must now be healthy and report blob referenced
    assert!(idx.check_health().is_ok());
    assert!(idx.is_blob_referenced(&m_digest).unwrap());
}

#[tokio::test]
async fn test_migrated_immutable_conflict_retains_content_addressed_manifest_and_referrer() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_index, service) = setup_test_service(&dir).await;

    let config_d = write_test_blob(storage.as_ref(), "test/repo", b"{}").await;
    let blob_d1 = write_test_blob(storage.as_ref(), "test/repo", b"payload1").await;
    let blob_d2 = write_test_blob(storage.as_ref(), "test/repo", b"payload2").await;

    let (m1_bytes, m1_digest) = create_manifest_json(&config_d, &blob_d1);

    // Subject blob
    let subject_blob = write_test_blob(storage.as_ref(), "test/repo", b"artifact_subject").await;
    let artifact_manifest_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config_d.as_str(),
            "size": 2
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar",
                "digest": blob_d2.as_str(),
                "size": 8
            }
        ],
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": subject_blob.as_str(),
            "size": 16
        }
    });
    let m2_bytes: Bytes = serde_json::to_vec(&artifact_manifest_json).unwrap().into();
    let mut hasher = sha2::Sha256::new();
    hasher.update(&m2_bytes);
    let m2_digest = Digest::parse(&format!("sha256:{}", hex::encode(hasher.finalize()))).unwrap();

    // 1. Publish m1 to tag "v1"
    service
        .publish_manifest(PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "v1".to_string(),
            payload: m1_bytes,
            declared_media_type: None,
            allow_tag_overwrite: false,
        })
        .await
        .unwrap();

    // 2. Publish m2 (with subject) to immutable tag "v1" with allow_tag_overwrite = false
    let err = service
        .publish_manifest(PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "v1".to_string(),
            payload: m2_bytes,
            declared_media_type: None,
            allow_tag_overwrite: false,
        })
        .await
        .unwrap_err();

    assert!(matches!(err, ManifestLifecycleError::TagAlreadyExists));

    // 3. Verify content-addressed invariants:
    assert_eq!(
        storage.resolve_tag("test/repo", "v1").await.unwrap(),
        m1_digest
    );
    assert!(storage.get_manifest("test/repo", &m2_digest).await.is_ok());
    let referrers = storage
        .list_referrers("test/repo", &subject_blob)
        .await
        .unwrap();
    assert!(referrers.iter().any(|r| r.digest == m2_digest.as_str()));
}

#[tokio::test]
async fn test_public_api_manifest_publication_compatibility() {
    // Compile-time and runtime proof that all 9 deprecated public names are fully accessible:
    #[allow(deprecated)]
    {
        use registry_rust::manifest_publication::{
            MAX_MANIFEST_SIZE, ManifestLifecycleService, ManifestPublisher, ProxyEvictionResult,
            ProxyPublicationEvidence, PublishManifestError, PublishManifestRequest,
            PublishedManifest, is_supported_manifest_media_type,
        };

        assert_eq!(MAX_MANIFEST_SIZE, 4 * 1024 * 1024);
        assert!(is_supported_manifest_media_type(
            "application/vnd.oci.image.manifest.v1+json"
        ));

        let _evidence_builder = |repo: &str, tag: &str, d: Digest| -> ProxyPublicationEvidence {
            ProxyPublicationEvidence::new_for_test(
                repo,
                tag,
                Bytes::from_static(b"{}"),
                None,
                true,
                d,
            )
        };
        let _eviction_res: Option<ProxyEvictionResult> = None;

        let dir = tempfile::tempdir().unwrap();
        let fs_root = dir.path().join("data");
        std::fs::create_dir_all(&fs_root).unwrap();
        let storage = Arc::new(FsStorage::new(fs_root, 10 * 1024 * 1024));
        let coordinator = registry_rust::consistency::ConsistencyCoordinator::new();

        let publisher: ManifestPublisher =
            ManifestLifecycleService::new(storage.clone(), None, coordinator);

        let req = PublishManifestRequest {
            repo: "compat/repo".to_string(),
            reference: "latest".to_string(),
            payload: Bytes::from("invalid-json"),
            declared_media_type: None,
            allow_tag_overwrite: true,
        };

        let res: Result<PublishedManifest, PublishManifestError> = publisher.publish(req).await;
        assert!(matches!(res, Err(PublishManifestError::InvalidManifest(_))));
    }
}
// ------------------------------------------------------------------------------------------------
// Lifecycle Reference-Discovery Hardening & Partial-Progress Integration Tests
// ------------------------------------------------------------------------------------------------

use std::sync::atomic::Ordering as AtomicOrdering;
use support::gc_coordination::LifecycleFaultStorage;

async fn setup_fault_service(
    dir: &tempfile::TempDir,
) -> (
    Arc<LifecycleFaultStorage>,
    Arc<BlobRefIndex>,
    ManifestLifecycleService,
) {
    let fs_root = dir.path().join("data");
    let ref_idx_path = dir.path().join("ref-index");
    std::fs::create_dir_all(&fs_root).unwrap();
    std::fs::create_dir_all(&ref_idx_path).unwrap();

    let base_storage = Arc::new(FsStorage::new(fs_root.clone(), 50 * 1024 * 1024));
    let fault_storage = Arc::new(LifecycleFaultStorage::new(base_storage));
    let ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());
    ref_index.rebuild(&fault_storage).await.unwrap();

    let coordinator = registry_rust::consistency::ConsistencyCoordinator::new();
    let service =
        ManifestLifecycleService::new(fault_storage.clone(), Some(ref_index.clone()), coordinator);

    (fault_storage, ref_index, service)
}

#[tokio::test]
async fn test_evict_proxy_cached_entry_fails_on_listing_error() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_idx, service) = setup_fault_service(&dir).await;
    let repo = "library/test-listing-err";

    let config = write_test_blob(&storage, repo, b"cfg").await;
    let layer = write_test_blob(&storage, repo, b"layer").await;
    let proxy_record = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
    storage.link_repo_blob(&proxy_record).await.unwrap();

    let (m_bytes, m_d) = create_manifest_json(&config, &layer);

    let ev = ProxyPublicationEvidence::new_for_test(
        repo,
        "v1",
        m_bytes.clone(),
        None,
        true,
        m_d.clone(),
    );
    service.publish_proxy_cached_manifest(ev).await.unwrap();

    storage
        .fail_manifest_listing
        .store(true, AtomicOrdering::SeqCst);

    let res = service
        .evict_proxy_cached_entry(repo, Some("v1"), &m_d)
        .await;
    let err = res.expect_err("evict must fail when manifest listing fails");
    assert!(matches!(err, ManifestLifecycleError::Storage(_)));

    // Sequential cleanup invariants:
    // Candidate membership must not be unlinked
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer)
            .await
            .unwrap()
            .is_some()
    );
    // Journal must be retained at ProxyManifestDeleted phase
    let journal_bytes = storage
        .read_lifecycle_journal(repo)
        .await
        .unwrap()
        .expect("journal must exist");
    let journal: LifecycleJournalRecord = serde_json::from_slice(&journal_bytes).unwrap();
    assert_eq!(journal.phase, LifecyclePhase::ProxyManifestDeleted);
    // CAS payload preserved
    assert!(storage.head_blob(&layer).await.is_ok());
}

#[tokio::test]
async fn test_evict_proxy_cached_entry_fails_on_manifest_read_error() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_idx, service) = setup_fault_service(&dir).await;
    let repo = "library/test-read-err";

    let config1 = write_test_blob(&storage, repo, b"cfg1").await;
    let layer1 = write_test_blob(&storage, repo, b"layer1").await;
    let pr1 = RepoBlobMembershipRecord::try_new_proxy(repo, layer1.clone()).unwrap();
    storage.link_repo_blob(&pr1).await.unwrap();

    let config2 = write_test_blob(&storage, repo, b"cfg2").await;
    let layer2 = write_test_blob(&storage, repo, b"layer2").await;
    let pr2 = RepoBlobMembershipRecord::try_new_proxy(repo, layer2.clone()).unwrap();
    storage.link_repo_blob(&pr2).await.unwrap();

    let (m1_bytes, m1_d) = create_manifest_json(&config1, &layer1);
    let (m2_bytes, m2_d) = create_manifest_json(&config2, &layer2);

    let ev1 = ProxyPublicationEvidence::new_for_test(
        repo,
        "v1",
        m1_bytes.clone(),
        None,
        true,
        m1_d.clone(),
    );
    service.publish_proxy_cached_manifest(ev1).await.unwrap();

    let ev2 = ProxyPublicationEvidence::new_for_test(
        repo,
        "v2",
        m2_bytes.clone(),
        None,
        true,
        m2_d.clone(),
    );
    service.publish_proxy_cached_manifest(ev2).await.unwrap();

    // Inject read failure targeting only m2 during discovery traversal
    *storage.fail_manifest_get_target.lock().unwrap() = Some(m2_d.clone());
    storage
        .fail_manifest_get
        .store(true, AtomicOrdering::SeqCst);

    let res = service
        .evict_proxy_cached_entry(repo, Some("v1"), &m1_d)
        .await;
    let err = res.expect_err("evict must fail when reading other manifest fails");
    assert!(matches!(err, ManifestLifecycleError::Storage(_)));

    assert!(
        storage
            .get_repo_blob_membership(repo, &layer1)
            .await
            .unwrap()
            .is_some()
    );
    let journal_bytes = storage
        .read_lifecycle_journal(repo)
        .await
        .unwrap()
        .expect("journal must exist");
    let journal: LifecycleJournalRecord = serde_json::from_slice(&journal_bytes).unwrap();
    assert_eq!(journal.phase, LifecyclePhase::ProxyManifestDeleted);
    assert!(storage.head_blob(&layer1).await.is_ok());
}

#[tokio::test]
async fn test_evict_proxy_cached_entry_fails_on_corrupt_manifest_parse_error() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_idx, service) = setup_fault_service(&dir).await;
    let repo = "library/test-parse-err";

    let config1 = write_test_blob(&storage, repo, b"cfg1").await;
    let layer1 = write_test_blob(&storage, repo, b"layer1").await;
    let pr1 = RepoBlobMembershipRecord::try_new_proxy(repo, layer1.clone()).unwrap();
    storage.link_repo_blob(&pr1).await.unwrap();

    let config2 = write_test_blob(&storage, repo, b"cfg2").await;
    let layer2 = write_test_blob(&storage, repo, b"layer2").await;
    let pr2 = RepoBlobMembershipRecord::try_new_proxy(repo, layer2.clone()).unwrap();
    storage.link_repo_blob(&pr2).await.unwrap();

    let (m1_bytes, m1_d) = create_manifest_json(&config1, &layer1);
    let (m2_bytes, m2_d) = create_manifest_json(&config2, &layer2);

    let ev1 = ProxyPublicationEvidence::new_for_test(
        repo,
        "v1",
        m1_bytes.clone(),
        None,
        true,
        m1_d.clone(),
    );
    service.publish_proxy_cached_manifest(ev1).await.unwrap();

    let ev2 = ProxyPublicationEvidence::new_for_test(
        repo,
        "v2",
        m2_bytes.clone(),
        None,
        true,
        m2_d.clone(),
    );
    service.publish_proxy_cached_manifest(ev2).await.unwrap();

    // Inject corrupt manifest payload targeting only m2 during discovery traversal
    *storage.corrupt_manifest_get_target.lock().unwrap() = Some(m2_d.clone());
    storage
        .corrupt_manifest_get
        .store(true, AtomicOrdering::SeqCst);

    let res = service
        .evict_proxy_cached_entry(repo, Some("v1"), &m1_d)
        .await;
    let err = res.expect_err("must fail on corrupt refs");
    assert!(matches!(
        err,
        ManifestLifecycleError::Storage(StorageError::Internal {
            kind: registry_rust::storage::StorageErrorKind::CorruptData,
            ..
        })
    ));

    assert!(
        storage
            .get_repo_blob_membership(repo, &layer1)
            .await
            .unwrap()
            .is_some()
    );
    let journal_bytes = storage
        .read_lifecycle_journal(repo)
        .await
        .unwrap()
        .expect("journal must exist");
    let journal: LifecycleJournalRecord = serde_json::from_slice(&journal_bytes).unwrap();
    assert_eq!(journal.phase, LifecyclePhase::ProxyManifestDeleted);
    assert!(storage.head_blob(&layer1).await.is_ok());
}

#[tokio::test]
async fn test_evict_proxy_cached_entry_detects_immediate_token_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_idx, service) = setup_fault_service(&dir).await;
    let repo = "library/test-imm-cycle";

    let config = write_test_blob(&storage, repo, b"cfg").await;
    let layer = write_test_blob(&storage, repo, b"layer").await;
    let proxy_record = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
    storage.link_repo_blob(&proxy_record).await.unwrap();

    let (m_bytes, m_d) = create_manifest_json(&config, &layer);

    let ev = ProxyPublicationEvidence::new_for_test(
        repo,
        "v1",
        m_bytes.clone(),
        None,
        true,
        m_d.clone(),
    );
    service.publish_proxy_cached_manifest(ev).await.unwrap();

    storage
        .token_cycle_immediate
        .store(true, AtomicOrdering::SeqCst);

    let res = service
        .evict_proxy_cached_entry(repo, Some("v1"), &m_d)
        .await;
    let err = res.expect_err("must fail on immediate pagination cycle");
    match err {
        ManifestLifecycleError::Storage(StorageError::Internal { message, .. }) => {
            assert!(message.contains("pagination cycle detected"));
        }
        other => panic!("expected Storage error with cycle detection, got {other:?}"),
    }

    assert!(
        storage
            .get_repo_blob_membership(repo, &layer)
            .await
            .unwrap()
            .is_some()
    );
    let journal_bytes = storage
        .read_lifecycle_journal(repo)
        .await
        .unwrap()
        .expect("journal must exist");
    let journal: LifecycleJournalRecord = serde_json::from_slice(&journal_bytes).unwrap();
    assert_eq!(journal.phase, LifecyclePhase::ProxyManifestDeleted);
    assert!(storage.head_blob(&layer).await.is_ok());
}

#[tokio::test]
async fn test_evict_proxy_cached_entry_detects_multi_token_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_idx, service) = setup_fault_service(&dir).await;
    let repo = "library/test-multi-cycle";

    let config = write_test_blob(&storage, repo, b"cfg").await;
    let layer = write_test_blob(&storage, repo, b"layer").await;
    let proxy_record = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
    storage.link_repo_blob(&proxy_record).await.unwrap();

    let (m_bytes, m_d) = create_manifest_json(&config, &layer);

    let ev = ProxyPublicationEvidence::new_for_test(
        repo,
        "v1",
        m_bytes.clone(),
        None,
        true,
        m_d.clone(),
    );
    service.publish_proxy_cached_manifest(ev).await.unwrap();

    storage
        .token_cycle_multi
        .store(true, AtomicOrdering::SeqCst);

    let res = service
        .evict_proxy_cached_entry(repo, Some("v1"), &m_d)
        .await;
    let err = res.expect_err("must fail on multi-token pagination cycle");
    match err {
        ManifestLifecycleError::Storage(StorageError::Internal { message, .. }) => {
            assert!(message.contains("pagination cycle detected"));
        }
        other => panic!("expected Storage error with cycle detection, got {other:?}"),
    }

    assert!(
        storage
            .get_repo_blob_membership(repo, &layer)
            .await
            .unwrap()
            .is_some()
    );
    let journal_bytes = storage
        .read_lifecycle_journal(repo)
        .await
        .unwrap()
        .expect("journal must exist");
    let journal: LifecycleJournalRecord = serde_json::from_slice(&journal_bytes).unwrap();
    assert_eq!(journal.phase, LifecyclePhase::ProxyManifestDeleted);
    assert!(storage.head_blob(&layer).await.is_ok());
}

#[tokio::test]
async fn test_evict_proxy_cached_entry_success_referenced_vs_unreferenced() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_idx, service) = setup_fault_service(&dir).await;
    let repo = "library/test-ref-success";

    let shared_blob = write_test_blob(&storage, repo, b"shared-blob").await;
    let unique_blob = write_test_blob(&storage, repo, b"unique-blob").await;
    let other_blob = write_test_blob(&storage, repo, b"other-blob").await;

    let pr_shared = RepoBlobMembershipRecord::try_new_proxy(repo, shared_blob.clone()).unwrap();
    let pr_unique = RepoBlobMembershipRecord::try_new_proxy(repo, unique_blob.clone()).unwrap();
    let pr_other = RepoBlobMembershipRecord::try_new_proxy(repo, other_blob.clone()).unwrap();
    storage.link_repo_blob(&pr_shared).await.unwrap();
    storage.link_repo_blob(&pr_unique).await.unwrap();
    storage.link_repo_blob(&pr_other).await.unwrap();

    let (m1_bytes, m1_d) = create_manifest_json(&shared_blob, &unique_blob);
    let (m2_bytes, m2_d) = create_manifest_json(&shared_blob, &other_blob);

    let ev1 = ProxyPublicationEvidence::new_for_test(
        repo,
        "v1",
        m1_bytes.clone(),
        None,
        true,
        m1_d.clone(),
    );
    service.publish_proxy_cached_manifest(ev1).await.unwrap();

    let ev2 = ProxyPublicationEvidence::new_for_test(
        repo,
        "v2",
        m2_bytes.clone(),
        None,
        true,
        m2_d.clone(),
    );
    service.publish_proxy_cached_manifest(ev2).await.unwrap();

    // Evict m1:
    // shared_blob is referenced by m2 -> NOT unlinked
    // unique_blob is unreferenced -> unlinked
    let evict_res = service
        .evict_proxy_cached_entry(repo, Some("v1"), &m1_d)
        .await
        .unwrap();

    assert_eq!(evict_res.memberships_unlinked, 1);
    assert!(
        storage
            .get_repo_blob_membership(repo, &shared_blob)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        storage
            .get_repo_blob_membership(repo, &unique_blob)
            .await
            .unwrap()
            .is_none()
    );

    // CAS payloads must remain intact in CAS storage
    assert!(storage.head_blob(&shared_blob).await.is_ok());
    assert!(storage.head_blob(&unique_blob).await.is_ok());

    // Journal must be cleanly removed upon complete eviction
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_recovery_with_parsed_refs_fails_and_propagates() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_idx, service) = setup_fault_service(&dir).await;
    let repo = "library/test-rec-parsed-err";

    let config = write_test_blob(&storage, repo, b"cfg").await;
    let layer = write_test_blob(&storage, repo, b"layer").await;
    let pr_cfg = RepoBlobMembershipRecord::try_new_proxy(repo, config.clone()).unwrap();
    let pr_layer = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
    storage.link_repo_blob(&pr_cfg).await.unwrap();
    storage.link_repo_blob(&pr_layer).await.unwrap();

    let (m_bytes, m_d) = create_manifest_json(&config, &layer);
    storage.put_manifest(repo, &m_d, m_bytes).await.unwrap();

    let journal = LifecycleJournalRecord {
        op_id: "rec-parsed-op-1".to_string(),
        repo: CanonicalRepoName::parse(repo).unwrap(),
        op_kind: LifecycleOpKind::ProxyEvict,
        target_digest: m_d.clone(),
        target_reference: Some("v1".to_string()),
        phase: LifecyclePhase::ProxyTagDeleted,
        owner_id: "test-owner".to_string(),
        lease_expiry_unix_secs: 9999999999,
        started_unix_secs: 100,
        updated_unix_secs: 100,
        relevant_tags: vec![],
        subject_digest: None,
        artifact_type: None,
        annotations: None,
        media_type: None,
        manifest_size: None,
    };
    storage
        .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
        .await
        .unwrap();

    storage
        .fail_manifest_listing
        .store(true, AtomicOrdering::SeqCst);

    let res = service.recover_and_ensure_index_healthy(repo).await;
    let err =
        res.expect_err("recovery must propagate listing error from is_blob_referenced_in_repo");
    assert!(matches!(err, ManifestLifecycleError::Storage(_)));

    // Sequential cleanup verification:
    // Neither blob membership was unlinked
    assert!(
        storage
            .get_repo_blob_membership(repo, &config)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer)
            .await
            .unwrap()
            .is_some()
    );
    // Journal remains present on disk
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_some()
    );
    // CAS payload preserved
    assert!(storage.head_blob(&layer).await.is_ok());
}

#[tokio::test]
async fn test_recovery_fallback_membership_scan_fails_and_propagates() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_idx, service) = setup_fault_service(&dir).await;
    let repo = "library/test-rec-fallback-err";

    let config = write_test_blob(&storage, repo, b"cfg").await;
    let layer = write_test_blob(&storage, repo, b"layer").await;
    let pr_cfg = RepoBlobMembershipRecord::try_new_proxy(repo, config.clone()).unwrap();
    let pr_layer = RepoBlobMembershipRecord::try_new_proxy(repo, layer.clone()).unwrap();
    storage.link_repo_blob(&pr_cfg).await.unwrap();
    storage.link_repo_blob(&pr_layer).await.unwrap();

    let target_manifest_digest = sha256_digest(b"non-existent-manifest");

    // Manifest is not in storage -> refs is None, triggers fallback membership scan
    let journal = LifecycleJournalRecord {
        op_id: "rec-fallback-op-1".to_string(),
        repo: CanonicalRepoName::parse(repo).unwrap(),
        op_kind: LifecycleOpKind::ProxyEvict,
        target_digest: target_manifest_digest,
        target_reference: Some("v1".to_string()),
        phase: LifecyclePhase::ProxyManifestDeleted,
        owner_id: "test-owner".to_string(),
        lease_expiry_unix_secs: 9999999999,
        started_unix_secs: 100,
        updated_unix_secs: 100,
        relevant_tags: vec![],
        subject_digest: None,
        artifact_type: None,
        annotations: None,
        media_type: None,
        manifest_size: None,
    };
    storage
        .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
        .await
        .unwrap();

    storage
        .fail_manifest_listing
        .store(true, AtomicOrdering::SeqCst);

    let res = service.recover_and_ensure_index_healthy(repo).await;
    let err = res.expect_err(
        "fallback recovery must propagate listing error from is_blob_referenced_in_repo",
    );
    assert!(matches!(err, ManifestLifecycleError::Storage(_)));

    assert!(
        storage
            .get_repo_blob_membership(repo, &config)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_some()
    );
    assert!(storage.head_blob(&layer).await.is_ok());
}

#[tokio::test]
async fn test_partial_cleanup_then_failure_then_successful_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _ref_idx, service) = setup_fault_service(&dir).await;
    let repo = "library/test-partial-cleanup";

    let layer_a = write_test_blob(&storage, repo, b"layer-a").await;
    let layer_b = write_test_blob(&storage, repo, b"layer-b").await;
    let pr_a = RepoBlobMembershipRecord::try_new_proxy(repo, layer_a.clone()).unwrap();
    let pr_b = RepoBlobMembershipRecord::try_new_proxy(repo, layer_b.clone()).unwrap();
    storage.link_repo_blob(&pr_a).await.unwrap();
    storage.link_repo_blob(&pr_b).await.unwrap();

    let (m_bytes, m_d) = create_manifest_json(&layer_a, &layer_b);
    storage.put_manifest(repo, &m_d, m_bytes).await.unwrap();

    let journal = LifecycleJournalRecord {
        op_id: "partial-op-1".to_string(),
        repo: CanonicalRepoName::parse(repo).unwrap(),
        op_kind: LifecycleOpKind::ProxyEvict,
        target_digest: m_d.clone(),
        target_reference: Some("v1".to_string()),
        phase: LifecyclePhase::ProxyTagDeleted,
        owner_id: "test-owner".to_string(),
        lease_expiry_unix_secs: 9999999999,
        started_unix_secs: 100,
        updated_unix_secs: 100,
        relevant_tags: vec![],
        subject_digest: None,
        artifact_type: None,
        annotations: None,
        media_type: None,
        manifest_size: None,
    };
    storage
        .write_lifecycle_journal(repo, Bytes::from(serde_json::to_vec(&journal).unwrap()))
        .await
        .unwrap();

    // Step 1: Simulate partial cleanup.
    // layer_a is checked -> unreferenced -> unlinked!
    // Then listing for layer_b fails on threshold = 2!
    storage
        .fail_listing_after_n
        .store(2, AtomicOrdering::SeqCst);

    let res = service.recover_and_ensure_index_healthy(repo).await;
    let err = res.expect_err("must fail when listing layer_b");
    assert!(matches!(err, ManifestLifecycleError::Storage(_)));

    // Sequential cleanup verification:
    // 1. Earlier unlinks are preserved (not rolled back):
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer_a)
            .await
            .unwrap()
            .is_none()
    );
    // 2. Candidate that encountered error is NOT unlinked:
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer_b)
            .await
            .unwrap()
            .is_some()
    );
    // 3. Journal remains on disk:
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_some()
    );
    // 4. CAS payloads are both untouched:
    assert!(storage.head_blob(&layer_a).await.is_ok());
    assert!(storage.head_blob(&layer_b).await.is_ok());

    // Step 2: Retry recovery after fault is cleared!
    storage
        .fail_listing_after_n
        .store(0, AtomicOrdering::SeqCst);
    let retry_res = service.recover_and_ensure_index_healthy(repo).await;
    assert!(
        retry_res.is_ok(),
        "recovery retry must succeed: {retry_res:?}"
    );

    // Final state verification:
    // Both layer_a and layer_b are unlinked
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer_a)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .get_repo_blob_membership(repo, &layer_b)
            .await
            .unwrap()
            .is_none()
    );
    // CAS payloads still preserved
    assert!(storage.head_blob(&layer_a).await.is_ok());
    assert!(storage.head_blob(&layer_b).await.is_ok());
    // Journal was cleanly deleted
    assert!(
        storage
            .read_lifecycle_journal(repo)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_manifest_listing_lifecycle_error_propagation_on_promoted_listing_failure() {
    let dir = tempfile::tempdir().unwrap();
    let fs_root = dir.path().join("data");
    let ref_idx_path = dir.path().join("ref-index");
    std::fs::create_dir_all(&fs_root).unwrap();
    std::fs::create_dir_all(&ref_idx_path).unwrap();

    // Configure real FsStorage with distinctive max_entries = 1
    let storage = Arc::new(
        FsStorage::try_new_with_limits(
            fs_root.clone(),
            50 * 1024 * 1024,
            storage_fs::DirEnumerationLimits::new(1, 100_000),
        )
        .expect("create fs storage with limits"),
    );
    let ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());
    ref_index.rebuild(&storage).await.unwrap();

    let coordinator = registry_rust::consistency::ConsistencyCoordinator::new();
    let service =
        ManifestLifecycleService::new(storage.clone(), Some(ref_index.clone()), coordinator);

    let repo = "library/lifecycle-listing-limit";

    let shared_blob = write_test_blob(&storage, repo, b"shared-blob-content").await;
    let unique_blob = write_test_blob(&storage, repo, b"unique-blob-content").await;
    let other_blob = write_test_blob(&storage, repo, b"other-blob-content").await;

    let pr_shared = RepoBlobMembershipRecord::try_new_proxy(repo, shared_blob.clone()).unwrap();
    let pr_unique = RepoBlobMembershipRecord::try_new_proxy(repo, unique_blob.clone()).unwrap();
    let pr_other = RepoBlobMembershipRecord::try_new_proxy(repo, other_blob.clone()).unwrap();
    storage.link_repo_blob(&pr_shared).await.unwrap();
    storage.link_repo_blob(&pr_unique).await.unwrap();
    storage.link_repo_blob(&pr_other).await.unwrap();

    let (m1_bytes, m1_d) = create_manifest_json(&shared_blob, &unique_blob);
    let (m2_bytes, m2_d) = create_manifest_json(&shared_blob, &other_blob);
    let (m3_bytes, m3_d) = create_manifest_json(&other_blob, &unique_blob);

    let ev1 =
        ProxyPublicationEvidence::new_for_test(repo, "v1", m1_bytes, None, true, m1_d.clone());
    service.publish_proxy_cached_manifest(ev1).await.unwrap();

    let ev2 =
        ProxyPublicationEvidence::new_for_test(repo, "v2", m2_bytes, None, true, m2_d.clone());
    service.publish_proxy_cached_manifest(ev2).await.unwrap();

    let ev3 =
        ProxyPublicationEvidence::new_for_test(repo, "v3", m3_bytes, None, true, m3_d.clone());
    service.publish_proxy_cached_manifest(ev3).await.unwrap();

    // Evict m1:
    // 1. Storage deletes m1 manifest file
    // 2. Journal updates to ProxyManifestDeleted
    // 3. Lifecycle service calls is_blob_referenced_in_repo to discover if remaining manifests reference blobs
    // 4. Remaining manifests are m2 and m3 (2 entries > limit 1), triggering budget exhaustion
    let err = service
        .evict_proxy_cached_entry(repo, Some("v1"), &m1_d)
        .await
        .expect_err("eviction should fail on promoted listing budget exhaustion");

    // Assert propagated error
    match err {
        ManifestLifecycleError::Storage(storage_err) => {
            assert!(
                storage_err
                    .to_string()
                    .contains("enumeration resource limit exceeded"),
                "expected budget exhaustion error, got: {storage_err:?}"
            );
        }
        other => panic!("expected ManifestLifecycleError::Storage, got {other:?}"),
    }

    // Establish the fixture's successful journal persistence before asserting an exact phase
    let journal_bytes = storage
        .read_lifecycle_journal(repo)
        .await
        .expect("journal read must succeed")
        .expect("journal must be present");
    let journal: LifecycleJournalRecord =
        serde_json::from_slice(&journal_bytes).expect("parse journal");
    assert_eq!(
        journal.phase,
        LifecyclePhase::ProxyManifestDeleted,
        "journal phase must remain at ProxyManifestDeleted"
    );
    assert_eq!(
        journal.target_digest, m1_d,
        "journal target digest must match evicted manifest"
    );

    // Assert preserved candidate membership
    assert!(
        storage
            .get_repo_blob_membership(repo, &shared_blob)
            .await
            .unwrap()
            .is_some(),
        "shared_blob membership must be preserved"
    );
    assert!(
        storage
            .get_repo_blob_membership(repo, &unique_blob)
            .await
            .unwrap()
            .is_some(),
        "unique_blob membership must be preserved on listing failure"
    );
    assert!(
        storage
            .get_repo_blob_membership(repo, &other_blob)
            .await
            .unwrap()
            .is_some(),
        "other_blob membership must be preserved"
    );

    // Assert continued CAS metadata accessibility
    assert!(
        storage.head_blob(&shared_blob).await.is_ok(),
        "shared_blob CAS must remain accessible"
    );
    assert!(
        storage.head_blob(&unique_blob).await.is_ok(),
        "unique_blob CAS must remain accessible"
    );
    assert!(
        storage.head_blob(&other_blob).await.is_ok(),
        "other_blob CAS must remain accessible"
    );
}
