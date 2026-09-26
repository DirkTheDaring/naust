use bytes::Bytes;
use sha2::Digest as _;
use std::sync::Arc;
use tempfile::TempDir;

use naust::application::{
    BlobMutationError, BlobMutationService, ManifestMutationError, ManifestMutationService,
};
use naust::consistency::ConsistencyCoordinator;
use naust::manifest_lifecycle::{ProxyPublicationEvidence, PublishManifestRequest};
use naust::registry::digest::Digest;
use naust::storage::fs::FsStorage;
use naust::upload_coordinator::BlobUploadCoordinatorConfig;

fn sha256_digest(bytes: &[u8]) -> Digest {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let hex = hex::encode(hasher.finalize());
    Digest::parse(&format!("sha256:{hex}")).unwrap()
}

async fn setup_services() -> (
    TempDir,
    Arc<BlobMutationService>,
    Arc<ManifestMutationService>,
) {
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(FsStorage::new(
        temp_dir.path().to_path_buf(),
        50 * 1024 * 1024,
    ));
    let consistency = ConsistencyCoordinator::new();

    let blob_cfg = BlobUploadCoordinatorConfig {
        signing_key: b"test-key-32-bytes-long-for-test!!".to_vec(),
        max_upload_bytes: 50 * 1024 * 1024,
        abort_on_digest_mismatch: true,
        disallow_monolithic_uploads: false,
        upload_chunk_min_bytes: None,
        gc_pin_duration_secs: 60,
        finalize_grace_secs: 0,
    };

    let blob_service = Arc::new(BlobMutationService::new(
        storage.clone(),
        None,
        consistency.clone(),
        blob_cfg,
    ));

    let manifest_service = Arc::new(ManifestMutationService::new(
        storage.clone(),
        None,
        consistency,
    ));

    (temp_dir, blob_service, manifest_service)
}

#[tokio::test]
async fn test_blob_mutation_service_chunked_upload_lifecycle() {
    let (_tmp, blob_service, _manifest_service) = setup_services().await;
    let repo = "test/app-chunked";

    // 1. Start upload session
    let start = blob_service.start_upload(repo).await.unwrap();
    let uuid = start.session.uuid.clone();

    // 2. Query status
    let status = blob_service
        .get_upload_status(repo, &uuid, Some(&start.state_token))
        .await
        .unwrap();
    assert_eq!(status.offset, 0);

    // 3. Append chunk 1: "hello "
    let chunk1 = Bytes::from_static(b"hello ");
    let stream1: naust::storage::upload_session::UploadByteStream =
        Box::pin(futures_util::stream::once(async move { Ok(chunk1) }));
    let append1 = blob_service
        .append_chunk(
            repo,
            &uuid,
            &start.state_token,
            Some((0, 5)),
            Some(6),
            stream1,
        )
        .await
        .unwrap();
    assert_eq!(append1.new_offset, 6);

    // 4. Append chunk 2: "world"
    let chunk2 = Bytes::from_static(b"world");
    let stream2: naust::storage::upload_session::UploadByteStream =
        Box::pin(futures_util::stream::once(async move { Ok(chunk2) }));
    let append2 = blob_service
        .append_chunk(
            repo,
            &uuid,
            &append1.state_token,
            Some((6, 10)),
            Some(5),
            stream2,
        )
        .await
        .unwrap();
    assert_eq!(append2.new_offset, 11);

    // 5. Finalize upload
    let expected_digest = sha256_digest(b"hello world");
    let finalized = blob_service
        .finalize_upload(
            repo,
            &uuid,
            Some(&append2.state_token),
            None,
            None,
            &expected_digest,
        )
        .await
        .unwrap();
    assert_eq!(finalized.digest, expected_digest);
    assert_eq!(finalized.size, 11);
}

#[tokio::test]
async fn test_blob_mutation_service_abort_upload() {
    let (_tmp, blob_service, _manifest_service) = setup_services().await;
    let repo = "test/app-abort";

    let start = blob_service.start_upload(repo).await.unwrap();
    let uuid = start.session.uuid.clone();

    // Abort session
    blob_service
        .abort_upload(repo, &uuid, Some(&start.state_token))
        .await
        .unwrap();

    // Status after abort should return SessionNotFound
    let res = blob_service
        .get_upload_status(repo, &uuid, Some(&start.state_token))
        .await;
    assert!(matches!(res, Err(BlobMutationError::SessionNotFound)));
}

#[tokio::test]
async fn test_blob_mutation_service_monolithic_upload() {
    let (_tmp, blob_service, _manifest_service) = setup_services().await;
    let repo = "test/app-monolithic";
    let data = Bytes::from_static(b"monolithic content payload");
    let digest = sha256_digest(&data);

    let stream: naust::storage::upload_session::UploadByteStream =
        Box::pin(futures_util::stream::once(async move { Ok(data) }));

    let res = blob_service
        .monolithic_upload(repo, &digest, Some(stream))
        .await
        .unwrap();

    match res {
        naust::upload_coordinator::MonolithicUploadResult::Created(fin) => {
            assert_eq!(fin.digest, digest);
            assert_eq!(fin.size, 26);
        }
        _ => panic!("expected Created result"),
    }
}

#[tokio::test]
async fn test_blob_mutation_service_cross_mount() {
    let (_tmp, blob_service, _manifest_service) = setup_services().await;
    let src_repo = "test/src-repo";
    let dst_repo = "test/dst-repo";

    // Upload blob in src_repo
    let data = Bytes::from_static(b"cross mountable blob");
    let digest = sha256_digest(&data);

    let stream: naust::storage::upload_session::UploadByteStream =
        Box::pin(futures_util::stream::once(async move { Ok(data) }));
    blob_service
        .monolithic_upload(src_repo, &digest, Some(stream))
        .await
        .unwrap();

    // Cross-mount into dst_repo
    let mount_res = blob_service
        .cross_mount(dst_repo, Some(src_repo), &digest)
        .await
        .unwrap();

    match mount_res {
        naust::upload_coordinator::CrossMountResult::Mounted(fin) => {
            assert_eq!(fin.digest, digest);
        }
        _ => panic!("expected Mounted result"),
    }
}

#[tokio::test]
async fn test_blob_mutation_service_delete_repo_blob() {
    let (_tmp, blob_service, _manifest_service) = setup_services().await;
    let repo = "test/app-delete-blob";
    let data = Bytes::from_static(b"delete me please");
    let digest = sha256_digest(&data);

    let stream: naust::storage::upload_session::UploadByteStream =
        Box::pin(futures_util::stream::once(async move { Ok(data) }));
    blob_service
        .monolithic_upload(repo, &digest, Some(stream))
        .await
        .unwrap();

    // Delete blob membership
    let del_res = blob_service.delete_repo_blob(repo, &digest).await.unwrap();
    assert_eq!(
        del_res,
        naust::blob_delete_safety::BlobDeleteResult::Success
    );

    // Subsequent delete returns NotFound
    let del_res2 = blob_service.delete_repo_blob(repo, &digest).await.unwrap();
    assert_eq!(
        del_res2,
        naust::blob_delete_safety::BlobDeleteResult::NotFound
    );
}

#[tokio::test]
async fn test_manifest_mutation_service_lifecycle() {
    let (_tmp, blob_service, manifest_service) = setup_services().await;
    let repo = "test/app-manifest";

    // 1. Upload config blob
    let config_bytes = Bytes::from_static(b"{}");
    let config_digest = sha256_digest(&config_bytes);
    let stream: naust::storage::upload_session::UploadByteStream =
        Box::pin(futures_util::stream::once(async move { Ok(config_bytes) }));
    blob_service
        .monolithic_upload(repo, &config_digest, Some(stream))
        .await
        .unwrap();

    // 2. Upload layer blob
    let layer_bytes = Bytes::from_static(b"layer-payload-content");
    let layer_digest = sha256_digest(&layer_bytes);
    let stream: naust::storage::upload_session::UploadByteStream =
        Box::pin(futures_util::stream::once(async move { Ok(layer_bytes) }));
    blob_service
        .monolithic_upload(repo, &layer_digest, Some(stream))
        .await
        .unwrap();

    // 3. Publish manifest with tag "v1.0"
    let manifest_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": 2,
            "digest": config_digest.as_str()
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar",
                "size": 21,
                "digest": layer_digest.as_str()
            }
        ]
    });
    let manifest_bytes = Bytes::from(serde_json::to_vec(&manifest_json).unwrap());
    let manifest_digest = sha256_digest(&manifest_bytes);

    let req = PublishManifestRequest::new(
        repo,
        "v1.0",
        manifest_bytes,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
    );

    let published = manifest_service.publish_manifest(req).await.unwrap();
    assert_eq!(published.digest, manifest_digest);
    assert!(published.is_tag);

    // 4. Attempt to delete tag when immutable (allow_tag_overwrite = false)
    let tag_del_err = manifest_service
        .delete_tag(repo, "v1.0", false)
        .await
        .unwrap_err();
    assert!(matches!(tag_del_err, ManifestMutationError::TagImmutable));

    // 5. Delete tag when mutable (allow_tag_overwrite = true)
    let tag_del = manifest_service
        .delete_tag(repo, "v1.0", true)
        .await
        .unwrap();
    assert_eq!(tag_del.tag, "v1.0");
    assert_eq!(tag_del.target_digest, manifest_digest);

    // 6. Delete manifest by digest
    let man_del = manifest_service
        .delete_manifest(repo, &manifest_digest)
        .await
        .unwrap();
    assert_eq!(man_del.digest, manifest_digest);
}

#[tokio::test]
async fn test_manifest_proxy_publication_and_eviction() {
    let (_tmp, _blob_service, manifest_service) = setup_services().await;
    let repo = "test/app-proxy-manifest";

    let manifest_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": 2,
            "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        },
        "layers": []
    });
    let manifest_bytes = Bytes::from(serde_json::to_vec(&manifest_json).unwrap());
    let manifest_digest = sha256_digest(&manifest_bytes);

    let evidence = ProxyPublicationEvidence::new(
        repo,
        "proxy-tag",
        manifest_bytes,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
        manifest_digest.clone(),
    );

    // Publish proxy cached manifest
    let published = manifest_service
        .publish_manifest_from_proxy(evidence)
        .await
        .unwrap();
    assert_eq!(published.digest, manifest_digest);

    // Evict proxy cached entry
    let evict = manifest_service
        .evict_proxy_manifest_and_memberships(repo, &manifest_digest, Some("proxy-tag"))
        .await
        .unwrap();
    assert_eq!(evict.target_digest, manifest_digest);
    assert_eq!(evict.tag_removed, Some("proxy-tag".to_string()));
}

#[tokio::test]
async fn test_blob_mutation_error_invalid_repo_name_preserves_typed_source() {
    let (_tmp, blob_service, _manifest_service) = setup_services().await;
    use std::error::Error;

    // Test invalid repo name with consecutive slashes
    let err = blob_service
        .start_upload("invalid//repo")
        .await
        .unwrap_err();
    match &err {
        BlobMutationError::InvalidRepoName { name, source } => {
            assert_eq!(name, "invalid//repo");
            assert_eq!(
                *source,
                naust::registry::canonical_name::RepoNameError::ConsecutiveSlashes
            );
            let src = err.source().expect("source error must exist");
            let downcasted = src
                .downcast_ref::<naust::registry::canonical_name::RepoNameError>()
                .expect("source error must downcast to RepoNameError");
            assert_eq!(
                *downcasted,
                naust::registry::canonical_name::RepoNameError::ConsecutiveSlashes
            );
        }
        _ => panic!("expected InvalidRepoName error with typed source"),
    }
}

#[tokio::test]
async fn test_manifest_mutation_error_typed_variants_and_sources() {
    let (_tmp, _blob_service, manifest_service) = setup_services().await;
    use std::error::Error;

    // 1. Invalid repo name
    let req_bad_repo =
        PublishManifestRequest::new("bad//repo", "v1", Bytes::from_static(b"{}"), None, true);
    let err = manifest_service
        .publish_manifest(req_bad_repo)
        .await
        .unwrap_err();
    match &err {
        ManifestMutationError::InvalidRepoName { name, source } => {
            assert_eq!(name, "bad//repo");
            assert_eq!(
                *source,
                naust::registry::canonical_name::RepoNameError::ConsecutiveSlashes
            );
            let src = err.source().expect("source error must exist");
            let downcasted = src
                .downcast_ref::<naust::registry::canonical_name::RepoNameError>()
                .expect("source error must downcast to RepoNameError");
            assert_eq!(
                *downcasted,
                naust::registry::canonical_name::RepoNameError::ConsecutiveSlashes
            );
        }
        _ => panic!("expected InvalidRepoName error with typed source"),
    }

    // 2. Empty payload
    let req_empty = PublishManifestRequest::new("valid/repo", "v1", Bytes::new(), None, true);
    let err_empty = manifest_service
        .publish_manifest(req_empty)
        .await
        .unwrap_err();
    assert!(matches!(err_empty, ManifestMutationError::EmptyPayload));

    // 3. Tag immutable
    let err_immutable = manifest_service
        .delete_tag("valid/repo", "v1", false)
        .await
        .unwrap_err();
    assert!(matches!(err_immutable, ManifestMutationError::TagImmutable));

    // 4. Malformed JSON manifest preserving serde_json::Error source
    let req_bad_json = PublishManifestRequest::new(
        "valid/repo",
        "v1",
        Bytes::from_static(b"not json at all"),
        None,
        true,
    );
    let err_bad_json = manifest_service
        .publish_manifest(req_bad_json)
        .await
        .unwrap_err();
    match &err_bad_json {
        ManifestMutationError::InvalidManifest(parse_err) => match parse_err {
            naust::manifest_refs::ManifestParseError::InvalidJson(json_err) => {
                let src = parse_err.source().expect("parse error must have source");
                let downcasted = src
                    .downcast_ref::<serde_json::Error>()
                    .expect("source must downcast to serde_json::Error");
                assert_eq!(format!("{json_err}"), format!("{downcasted}"));
            }
            _ => panic!("expected InvalidJson parse error variant"),
        },
        _ => panic!("expected InvalidManifest variant"),
    }

    // 5. Unverified manifest with signature field
    let req_unverified = PublishManifestRequest::new(
        "valid/repo",
        "v1",
        Bytes::from_static(br#"{"schemaVersion": 2, "signatures": []}"#),
        None,
        true,
    );
    let err_unverified = manifest_service
        .publish_manifest(req_unverified)
        .await
        .unwrap_err();
    match err_unverified {
        ManifestMutationError::Unverified(reason) => {
            assert_eq!(
                reason,
                naust::manifest_lifecycle::UnverifiedReason::SignatureVerificationFailed
            );
        }
        _ => panic!("expected Unverified variant"),
    }

    // 6. Direct Lifecycle error conversion preservation
    let lifecycle_err = naust::manifest_lifecycle::ManifestLifecycleError::CoordinationLeaseHeld;
    let converted: ManifestMutationError = lifecycle_err.into();
    match converted {
        ManifestMutationError::Lifecycle(
            naust::manifest_lifecycle::ManifestLifecycleError::CoordinationLeaseHeld,
        ) => {}
        _ => panic!("expected Lifecycle(CoordinationLeaseHeld)"),
    }
}
