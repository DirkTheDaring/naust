#[allow(unused_imports)]
use bytes::Bytes;
#[allow(unused_imports)]
use sha2::Digest as _;
#[allow(unused_imports)]
use std::sync::Arc;

#[allow(unused_imports)]
use crate::blob_ref_index::BlobRefIndex;
#[allow(unused_imports)]
use crate::manifest_refs::parse_manifest_refs;
#[allow(unused_imports)]
use crate::registry::digest::Digest;
#[allow(unused_imports)]
use crate::registry::validation::{is_valid_repo_name, is_valid_tag};
#[allow(unused_imports)]
use crate::storage::{ReferrerDescriptor, Storage, StorageError, TagMutationPolicy};

#[allow(unused_imports)]
pub use crate::manifest_lifecycle::{
    MAX_MANIFEST_SIZE, ManifestLifecycleError as PublishManifestError, ManifestLifecycleService,
    ProxyEvictionResult, ProxyPublicationEvidence, PublishManifestRequest, PublishedManifest,
    is_supported_manifest_media_type,
};

#[allow(dead_code)]
pub type ManifestPublisher = ManifestLifecycleService;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::fs::FsStorage;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FaultInjectableStorage {
        inner: Arc<FsStorage>,
        pub fail_put_manifest: AtomicBool,
        pub fail_mutate_tag: AtomicBool,
        pub fail_add_referrer: AtomicBool,
    }

    impl FaultInjectableStorage {
        fn new(fs_root: PathBuf) -> Self {
            let inner = Arc::new(FsStorage::new(fs_root, 50 * 1024 * 1024));
            Self {
                inner,
                fail_put_manifest: AtomicBool::new(false),
                fail_mutate_tag: AtomicBool::new(false),
                fail_add_referrer: AtomicBool::new(false),
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::storage::UploadSessionStorage for FaultInjectableStorage {}

    #[async_trait::async_trait]
    impl crate::storage::repo_membership::RepositoryBlobMembershipStorage for FaultInjectableStorage {
        async fn get_repo_blob_membership(
            &self,
            repo: &str,
            digest: &Digest,
        ) -> Result<Option<crate::storage::repo_membership::RepoBlobMembershipRecord>, StorageError>
        {
            self.inner.get_repo_blob_membership(repo, digest).await
        }

        async fn link_repo_blob(
            &self,
            record: &crate::storage::repo_membership::RepoBlobMembershipRecord,
        ) -> Result<(), StorageError> {
            self.inner.link_repo_blob(record).await
        }

        async fn unlink_repo_blob(
            &self,
            repo: &str,
            digest: &Digest,
        ) -> Result<bool, StorageError> {
            self.inner.unlink_repo_blob(repo, digest).await
        }

        async fn list_repo_blob_memberships_page(
            &self,
            repo: &str,
            continuation_token: Option<&str>,
            page_limit: usize,
        ) -> Result<
            (
                Vec<crate::storage::repo_membership::RepoBlobMembershipRecord>,
                Option<String>,
            ),
            StorageError,
        > {
            self.inner
                .list_repo_blob_memberships_page(repo, continuation_token, page_limit)
                .await
        }

        async fn count_repo_blob_memberships(
            &self,
            digest: &Digest,
        ) -> Result<usize, StorageError> {
            self.inner.count_repo_blob_memberships(digest).await
        }

        async fn is_membership_ready(&self) -> Result<bool, StorageError> {
            self.inner.is_membership_ready().await
        }

        async fn mark_membership_ready(&self) -> Result<(), StorageError> {
            self.inner.mark_membership_ready().await
        }
    }

    #[async_trait::async_trait]
    impl Storage for FaultInjectableStorage {
        fn kind(&self) -> &'static str {
            "fault-injectable"
        }

        async fn list_repositories(&self) -> Result<Vec<String>, StorageError> {
            self.inner.list_repositories().await
        }

        async fn repo_timestamps(
            &self,
            name: &str,
        ) -> Result<crate::storage::RepoTimestamps, StorageError> {
            self.inner.repo_timestamps(name).await
        }

        async fn head_blob(
            &self,
            digest: &Digest,
        ) -> Result<crate::storage::BlobMeta, StorageError> {
            self.inner.head_blob(digest).await
        }

        async fn open_blob(
            &self,
            digest: &Digest,
        ) -> Result<
            (
                crate::storage::BlobMeta,
                std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>,
            ),
            StorageError,
        > {
            self.inner.open_blob(digest).await
        }

        async fn get_manifest(
            &self,
            name: &str,
            digest: &Digest,
        ) -> Result<(crate::storage::ManifestMeta, Bytes), StorageError> {
            self.inner.get_manifest(name, digest).await
        }

        async fn head_manifest(
            &self,
            name: &str,
            digest: &Digest,
        ) -> Result<crate::storage::ManifestMeta, StorageError> {
            self.inner.head_manifest(name, digest).await
        }

        async fn put_manifest(
            &self,
            name: &str,
            digest: &Digest,
            bytes: Bytes,
        ) -> Result<crate::storage::ManifestMeta, StorageError> {
            if self.fail_put_manifest.load(Ordering::SeqCst) {
                return Err(StorageError::Internal("injected put_manifest error".into()));
            }
            self.inner.put_manifest(name, digest, bytes).await
        }

        async fn set_tag(
            &self,
            name: &str,
            tag: &str,
            digest: &Digest,
        ) -> Result<(), StorageError> {
            self.inner.set_tag(name, tag, digest).await
        }

        async fn mutate_tag(
            &self,
            name: &str,
            tag: &str,
            digest: &Digest,
            policy: TagMutationPolicy,
        ) -> Result<crate::storage::TagMutation, StorageError> {
            if self.fail_mutate_tag.load(Ordering::SeqCst) {
                return Err(StorageError::Internal("injected mutate_tag error".into()));
            }
            self.inner.mutate_tag(name, tag, digest, policy).await
        }

        async fn delete_tag(&self, name: &str, tag: &str) -> Result<(), StorageError> {
            self.inner.delete_tag(name, tag).await
        }

        async fn list_manifest_digests_page(
            &self,
            repo: &str,
            continuation_token: Option<&str>,
            page_limit: usize,
        ) -> Result<(Vec<Digest>, Option<String>), StorageError> {
            self.inner
                .list_manifest_digests_page(repo, continuation_token, page_limit)
                .await
        }

        async fn list_tags_page(
            &self,
            repo: &str,
            continuation_token: Option<&str>,
            page_limit: usize,
        ) -> Result<(Vec<(String, Digest)>, Option<String>), StorageError> {
            self.inner
                .list_tags_page(repo, continuation_token, page_limit)
                .await
        }

        async fn list_referrers_page(
            &self,
            repo: &str,
            subject: &Digest,
            continuation_token: Option<&str>,
            page_limit: usize,
        ) -> Result<(Vec<ReferrerDescriptor>, Option<String>), StorageError> {
            self.inner
                .list_referrers_page(repo, subject, continuation_token, page_limit)
                .await
        }

        async fn get_tag_with_version(
            &self,
            repo: &str,
            tag: &str,
        ) -> Result<Option<(Digest, String)>, StorageError> {
            self.inner.get_tag_with_version(repo, tag).await
        }

        async fn delete_tag_conditional(
            &self,
            repo: &str,
            tag: &str,
            expected_version: Option<&str>,
        ) -> Result<crate::storage::ConditionalDeleteResult, StorageError> {
            self.inner
                .delete_tag_conditional(repo, tag, expected_version)
                .await
        }

        async fn read_lifecycle_journal(&self, repo: &str) -> Result<Option<Bytes>, StorageError> {
            self.inner.read_lifecycle_journal(repo).await
        }

        async fn write_lifecycle_journal(
            &self,
            repo: &str,
            data: Bytes,
        ) -> Result<(), StorageError> {
            self.inner.write_lifecycle_journal(repo, data).await
        }

        async fn delete_lifecycle_journal(&self, repo: &str) -> Result<(), StorageError> {
            self.inner.delete_lifecycle_journal(repo).await
        }

        async fn acquire_repo_lease(
            &self,
            repo: &str,
            owner_id: &str,
            lease_id: &str,
            ttl_secs: u64,
        ) -> Result<bool, StorageError> {
            self.inner
                .acquire_repo_lease(repo, owner_id, lease_id, ttl_secs)
                .await
        }

        async fn renew_repo_lease(
            &self,
            repo: &str,
            owner_id: &str,
            lease_id: &str,
            ttl_secs: u64,
        ) -> Result<bool, StorageError> {
            self.inner
                .renew_repo_lease(repo, owner_id, lease_id, ttl_secs)
                .await
        }

        async fn release_repo_lease(
            &self,
            repo: &str,
            owner_id: &str,
            lease_id: &str,
        ) -> Result<(), StorageError> {
            self.inner
                .release_repo_lease(repo, owner_id, lease_id)
                .await
        }

        async fn create_upload(&self) -> Result<crate::storage::UploadMeta, StorageError> {
            self.inner.create_upload().await
        }

        async fn upload_status(
            &self,
            uuid: &str,
        ) -> Result<crate::storage::UploadMeta, StorageError> {
            self.inner.upload_status(uuid).await
        }

        async fn append_upload(
            &self,
            uuid: &str,
            chunk: Bytes,
        ) -> Result<crate::storage::UploadMeta, StorageError> {
            self.inner.append_upload(uuid, chunk).await
        }

        async fn finalize_upload(
            &self,
            uuid: &str,
            digest: &Digest,
        ) -> Result<crate::storage::BlobMeta, StorageError> {
            self.inner.finalize_upload(uuid, digest).await
        }

        async fn list_tags(&self, name: &str) -> Result<Vec<String>, StorageError> {
            self.inner.list_tags(name).await
        }

        async fn resolve_tag(&self, name: &str, tag: &str) -> Result<Digest, StorageError> {
            self.inner.resolve_tag(name, tag).await
        }

        async fn abort_upload(&self, uuid: &str) -> Result<(), StorageError> {
            self.inner.abort_upload(uuid).await
        }

        async fn delete_blob(&self, digest: &Digest) -> Result<(), StorageError> {
            self.inner.delete_blob(digest).await
        }

        async fn list_referrers(
            &self,
            name: &str,
            subject: &Digest,
        ) -> Result<Vec<ReferrerDescriptor>, StorageError> {
            self.inner.list_referrers(name, subject).await
        }

        async fn add_referrer(
            &self,
            name: &str,
            subject: &Digest,
            descriptor: ReferrerDescriptor,
        ) -> Result<(), StorageError> {
            if self.fail_add_referrer.load(Ordering::SeqCst) {
                return Err(StorageError::Internal("injected add_referrer error".into()));
            }
            self.inner.add_referrer(name, subject, descriptor).await
        }

        async fn remove_referrer(
            &self,
            name: &str,
            subject: &Digest,
            referrer: &Digest,
        ) -> Result<(), StorageError> {
            self.inner.remove_referrer(name, subject, referrer).await
        }

        async fn delete_manifest(&self, name: &str, digest: &Digest) -> Result<(), StorageError> {
            self.inner.delete_manifest(name, digest).await
        }
    }

    async fn write_test_blob(storage: &Arc<dyn Storage>, content: &[u8]) -> Digest {
        let mut hasher = sha2::Sha256::new();
        hasher.update(content);
        let digest = Digest::parse(&format!("sha256:{}", hex::encode(hasher.finalize()))).unwrap();
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
            crate::registry::canonical_name::CanonicalRepoName::parse("test/repo").unwrap();
        let membership = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
            canonical_repo,
            digest.clone(),
            Some(upload.uuid),
        );
        let _ = storage.link_repo_blob(&membership).await;
        digest
    }

    fn sample_valid_manifest(config_digest: &Digest, layer_digest: &Digest) -> (Bytes, Digest) {
        let json = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest.as_str(),
                "size": 2
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": layer_digest.as_str(),
                    "size": 12
                }
            ]
        });
        let bytes = Bytes::from(serde_json::to_vec(&json).unwrap());
        let mut hasher = sha2::Sha256::new();
        hasher.update(&bytes);
        let digest = Digest::parse(&format!("sha256:{}", hex::encode(hasher.finalize()))).unwrap();
        (bytes, digest)
    }

    #[tokio::test]
    async fn test_stage_1_reference_parsing_fails() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(
            temp_dir.path().to_path_buf(),
            10 * 1024 * 1024,
        ));
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage, None, gate);

        let req = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "latest".to_string(),
            payload: Bytes::from("not-valid-json"),
            declared_media_type: None,
            allow_tag_overwrite: true,
        };
        let err = publisher.publish(req).await.unwrap_err();
        assert!(matches!(err, PublishManifestError::InvalidManifest(_)));
    }

    #[tokio::test]
    async fn test_reference_kind_validation_blobs_vs_child_manifests() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(
            temp_dir.path().to_path_buf(),
            10 * 1024 * 1024,
        ));
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage.clone(), None, gate);

        let config_d = write_test_blob(&storage, b"{}").await;
        let missing_blob_digest = Digest::parse(
            "sha256:baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let (payload, _) = sample_valid_manifest(&config_d, &missing_blob_digest);

        // 1. Missing layer blob fails with MissingBlob
        let req1 = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "latest".to_string(),
            payload,
            declared_media_type: None,
            allow_tag_overwrite: true,
        };
        let err1 = publisher.publish(req1).await.unwrap_err();
        assert!(matches!(err1, PublishManifestError::MissingBlob(_)));

        // 2. Missing child manifest in OCI index fails with MissingManifest
        let missing_manifest_digest = Digest::parse(
            "sha256:caaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
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
        let err2 = publisher.publish(req2).await.unwrap_err();
        match err2 {
            PublishManifestError::MissingManifest(d) => {
                assert_eq!(d, missing_manifest_digest.as_str())
            }
            other => panic!("expected MissingManifest, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_real_fs_concurrent_immutable_tag_creates() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(
            temp_dir.path().to_path_buf(),
            10 * 1024 * 1024,
        ));
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = Arc::new(ManifestPublisher::new(storage.clone(), None, gate));

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d1 = write_test_blob(&storage, b"payload_1").await;
        let blob_d2 = write_test_blob(&storage, b"payload_2").await;

        let (m1_bytes, m1_digest) = sample_valid_manifest(&config_d, &blob_d1);
        let (m2_bytes, m2_digest) = sample_valid_manifest(&config_d, &blob_d2);

        let p1 = publisher.clone();
        let handle1 = tokio::spawn(async move {
            p1.publish(PublishManifestRequest {
                repo: "test/repo".to_string(),
                reference: "v1".to_string(),
                payload: m1_bytes,
                declared_media_type: None,
                allow_tag_overwrite: false,
            })
            .await
        });

        let p2 = publisher.clone();
        let handle2 = tokio::spawn(async move {
            p2.publish(PublishManifestRequest {
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

        // Exactly one create must succeed, and one must fail with TagAlreadyExists
        let (success_digest, failed_err) = match (r1, r2) {
            (Ok(pub1), Err(err2)) => (pub1.digest, err2),
            (Err(err1), Ok(pub2)) => (pub2.digest, err1),
            (Ok(_), Ok(_)) => panic!("both concurrent immutable creates succeeded! Violation!"),
            (Err(e1), Err(e2)) => panic!("both failed: {e1:?}, {e2:?}"),
        };

        assert!(matches!(failed_err, PublishManifestError::TagAlreadyExists));

        // The authoritative tag pointer must match the successful writer's digest
        let resolved = storage.resolve_tag("test/repo", "v1").await.unwrap();
        assert_eq!(resolved, success_digest);
        assert!(resolved == m1_digest || resolved == m2_digest);
    }

    #[tokio::test]
    async fn test_immutable_tag_idempotent_republish() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(
            temp_dir.path().to_path_buf(),
            10 * 1024 * 1024,
        ));
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage.clone(), None, gate);

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d = write_test_blob(&storage, b"layer_idempotent").await;
        let (m_bytes, m_digest) = sample_valid_manifest(&config_d, &blob_d);

        let req1 = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "v1".to_string(),
            payload: m_bytes.clone(),
            declared_media_type: None,
            allow_tag_overwrite: false,
        };
        let res1 = publisher.publish(req1).await.unwrap();
        assert_eq!(res1.digest, m_digest);

        // Republishing identical payload with allow_tag_overwrite=false succeeds as Unchanged
        let req2 = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "v1".to_string(),
            payload: m_bytes,
            declared_media_type: None,
            allow_tag_overwrite: false,
        };
        let res2 = publisher.publish(req2).await.unwrap();
        assert_eq!(res2.digest, m_digest);
    }

    #[tokio::test]
    async fn test_synchronous_referrer_registration_failure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fault_storage = Arc::new(FaultInjectableStorage::new(temp_dir.path().to_path_buf()));
        let storage: Arc<dyn Storage> = fault_storage.clone();
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage.clone(), None, gate);

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d = write_test_blob(&storage, b"base_layer").await;
        let (base_bytes, base_digest) = sample_valid_manifest(&config_d, &blob_d);

        let req_base = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "latest".to_string(),
            payload: base_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        };
        publisher.publish(req_base).await.unwrap();

        // Create artifact manifest referencing base
        let artifact_json = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.artifact.manifest.v1+json",
            "artifactType": "application/vnd.example.sbom.v1",
            "subject": {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": base_digest.as_str(),
                "size": 100
            },
            "blobs": []
        });
        let artifact_bytes = Bytes::from(serde_json::to_vec(&artifact_json).unwrap());

        // Inject add_referrer failure
        fault_storage
            .fail_add_referrer
            .store(true, Ordering::SeqCst);

        let req_artifact = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "sbom-tag".to_string(),
            payload: artifact_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        };

        // Must fail synchronously when referrer registration fails
        let err = publisher.publish(req_artifact).await.unwrap_err();
        assert!(matches!(err, PublishManifestError::Storage(_)));

        // Tag must NOT have been created
        assert!(storage.resolve_tag("test/repo", "sbom-tag").await.is_err());
    }

    #[tokio::test]
    async fn test_dirty_index_state_rebuild_after_crash() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs_root = temp_dir.path().join("data");
        let ref_index_path = temp_dir.path().join("ref-index");
        std::fs::create_dir_all(&fs_root).unwrap();
        std::fs::create_dir_all(&ref_index_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root, 10 * 1024 * 1024));
        let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).unwrap());
        idx.ensure_healthy_or_rebuild(&storage, true, true)
            .await
            .unwrap();

        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage.clone(), Some(idx.clone()), gate);

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d = write_test_blob(&storage, b"crash_test_blob").await;
        let (m_bytes, m_digest) = sample_valid_manifest(&config_d, &blob_d);

        let req = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "v1".to_string(),
            payload: m_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        };
        publisher.publish(req).await.unwrap();

        // Simulate crash right after marking dirty
        idx.mark_dirty().unwrap();
        drop(publisher);
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
    async fn test_concurrent_overwrite_publications_converge() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs_root = temp_dir.path().join("data");
        let ref_index_path = temp_dir.path().join("ref-index");
        std::fs::create_dir_all(&fs_root).unwrap();
        std::fs::create_dir_all(&ref_index_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root, 10 * 1024 * 1024));
        let idx = Arc::new(BlobRefIndex::open(ref_index_path).unwrap());
        idx.ensure_healthy_or_rebuild(&storage, true, true)
            .await
            .unwrap();

        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = Arc::new(ManifestPublisher::new(
            storage.clone(),
            Some(idx.clone()),
            gate,
        ));

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d1 = write_test_blob(&storage, b"concurrent_1").await;
        let blob_d2 = write_test_blob(&storage, b"concurrent_2").await;

        let (m1_bytes, m1_digest) = sample_valid_manifest(&config_d, &blob_d1);
        let (m2_bytes, m2_digest) = sample_valid_manifest(&config_d, &blob_d2);

        let p1 = publisher.clone();
        let handle1 = tokio::spawn(async move {
            p1.publish(PublishManifestRequest {
                repo: "test/repo".to_string(),
                reference: "latest".to_string(),
                payload: m1_bytes,
                declared_media_type: None,
                allow_tag_overwrite: true,
            })
            .await
        });

        let p2 = publisher.clone();
        let handle2 = tokio::spawn(async move {
            p2.publish(PublishManifestRequest {
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

        // Final tag must point to either m1 or m2
        let final_tag = storage.resolve_tag("test/repo", "latest").await.unwrap();
        assert!(final_tag == m1_digest || final_tag == m2_digest);

        // Reference index must be healthy and report the final tag's blob as referenced
        assert!(idx.check_health().is_ok());
        assert!(idx.is_blob_referenced(&final_tag).unwrap());
    }

    #[tokio::test]
    async fn test_failure_after_dirty_marker_before_tag_mutation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs_root = temp_dir.path().join("data");
        let ref_index_path = temp_dir.path().join("ref-index");
        std::fs::create_dir_all(&fs_root).unwrap();
        std::fs::create_dir_all(&ref_index_path).unwrap();

        let fault_storage = Arc::new(FaultInjectableStorage::new(fs_root));
        let storage: Arc<dyn Storage> = fault_storage.clone();
        let idx = Arc::new(BlobRefIndex::open(ref_index_path).unwrap());
        idx.ensure_healthy_or_rebuild(&storage, true, true)
            .await
            .unwrap();

        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage.clone(), Some(idx.clone()), gate);

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d = write_test_blob(&storage, b"mutate_fail_blob").await;
        let (m_bytes, _m_digest) = sample_valid_manifest(&config_d, &blob_d);

        // Inject mutate_tag failure
        fault_storage.fail_mutate_tag.store(true, Ordering::SeqCst);

        let req = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "v1".to_string(),
            payload: m_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        };
        let err = publisher.publish(req).await.unwrap_err();
        assert!(matches!(err, PublishManifestError::Storage(_)));

        // Tag was NOT updated
        assert!(storage.resolve_tag("test/repo", "v1").await.is_err());
    }

    #[tokio::test]
    async fn test_gc_exclusion_with_shared_coordinator() {
        let gate = Arc::new(tokio::sync::Mutex::new(()));

        // When publication holds the gate, trying to acquire the gate from GC is blocked
        let guard = gate.try_lock();
        assert!(guard.is_ok());

        // A second attempt to lock while held fails immediately with would-block (TryLockError)
        let second_attempt = gate.try_lock();
        assert!(second_attempt.is_err());

        drop(guard);
        // After publication completes, lock can be acquired
        assert!(gate.try_lock().is_ok());
    }

    #[tokio::test]
    async fn test_same_digest_retry_after_dirty_rebuilds_and_succeeds() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs_root = temp_dir.path().join("data");
        let ref_index_path = temp_dir.path().join("ref-index");
        std::fs::create_dir_all(&fs_root).unwrap();
        std::fs::create_dir_all(&ref_index_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root, 10 * 1024 * 1024));
        let idx = Arc::new(BlobRefIndex::open(ref_index_path).unwrap());
        idx.ensure_healthy_or_rebuild(&storage, true, true)
            .await
            .unwrap();

        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage.clone(), Some(idx.clone()), gate);

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d = write_test_blob(&storage, b"retry_blob").await;
        let (m_bytes, m_digest) = sample_valid_manifest(&config_d, &blob_d);

        let req = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "v1".to_string(),
            payload: m_bytes.clone(),
            declared_media_type: None,
            allow_tag_overwrite: true,
        };

        // 1. Initial publish succeeds
        publisher.publish(req.clone()).await.unwrap();
        assert!(idx.check_health().is_ok());

        // 2. Simulate partial failure leaving dirty marker
        idx.mark_dirty().unwrap();
        assert!(idx.check_health().is_err());

        // 3. Retry same publication (returns Unchanged from mutate_tag)
        // Must rebuild dirty index rather than blindly assuming clean
        let res = publisher.publish(req).await;
        assert!(res.is_ok());

        // 4. Index must now be healthy and report blob referenced
        assert!(idx.check_health().is_ok());
        assert!(idx.is_blob_referenced(&m_digest).unwrap());
    }

    #[tokio::test]
    async fn test_immutable_conflict_retains_content_addressed_manifest_and_referrer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs_root = temp_dir.path().join("data");
        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root, 10 * 1024 * 1024));
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage.clone(), None, gate);

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d1 = write_test_blob(&storage, b"payload1").await;
        let blob_d2 = write_test_blob(&storage, b"payload2").await;

        let (m1_bytes, m1_digest) = sample_valid_manifest(&config_d, &blob_d1);

        // Subject blob
        let subject_blob = write_test_blob(&storage, b"artifact_subject").await;
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
        let m2_digest =
            Digest::parse(&format!("sha256:{}", hex::encode(hasher.finalize()))).unwrap();

        // 1. Publish m1 to tag "v1"
        publisher
            .publish(PublishManifestRequest {
                repo: "test/repo".to_string(),
                reference: "v1".to_string(),
                payload: m1_bytes,
                declared_media_type: None,
                allow_tag_overwrite: false,
            })
            .await
            .unwrap();

        // 2. Publish m2 (with subject) to immutable tag "v1" with allow_tag_overwrite = false
        let err = publisher
            .publish(PublishManifestRequest {
                repo: "test/repo".to_string(),
                reference: "v1".to_string(),
                payload: m2_bytes,
                declared_media_type: None,
                allow_tag_overwrite: false,
            })
            .await
            .unwrap_err();

        assert!(matches!(err, PublishManifestError::TagAlreadyExists));

        // 3. Verify content-addressed invariants:
        // - Tag still points to m1
        assert_eq!(
            storage.resolve_tag("test/repo", "v1").await.unwrap(),
            m1_digest
        );
        // - Manifest m2 is addressable by content digest
        assert!(storage.get_manifest("test/repo", &m2_digest).await.is_ok());
        // - Referrer metadata for m2 is registered under subject
        let referrers = storage
            .list_referrers("test/repo", &subject_blob)
            .await
            .unwrap();
        assert!(referrers.iter().any(|r| r.digest == m2_digest.as_str()));
    }
}
