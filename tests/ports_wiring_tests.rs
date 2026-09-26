use bytes::Bytes;
use registry_rust::application::blob::BlobMutationService;
use registry_rust::application::manifest::ManifestMutationService;
use registry_rust::blob_delete_safety::BlobDeleteService;
use registry_rust::blob_gc::BlobGcPolicy;
use registry_rust::blob_gc::policy::PolicyContext;
use registry_rust::blob_ref_index::BlobRefIndex;
use registry_rust::config::Config;
use registry_rust::consistency::ConsistencyCoordinator;
use registry_rust::gc_service::{GcBudgets, GcService};
use registry_rust::registry::canonical_name::CanonicalRepoName;
use registry_rust::registry::digest::Digest;
use registry_rust::repository_membership_ledger::RepositoryMembershipLedger;
use registry_rust::storage::fs::FsStorage;
use registry_rust::storage::mutation_authority::{
    DeploymentWriterLockDoc, GcMutationPermit, RuntimeMutationAuthority,
};
use registry_rust::storage::ports::*;
use registry_rust::storage::repo_membership::{
    RepoBlobMembershipRecord, RepositoryBlobMembershipStorage,
};
use registry_rust::storage::s3::S3Storage;
use registry_rust::storage::upload_session::UploadSessionStorage;
use registry_rust::storage::{
    BlobMeta, BlobObjectVersion, ConditionalDeleteResult, GcBlobCandidate, GcBlobPage, GcCursor,
    GcDeleteResult, GcQuarantineResult, GcStorageStrategy, ManifestMeta, ReferrerDescriptor,
    RepoTimestamps, StorageError, TagMutation, TagMutationPolicy, UploadMeta,
};
use registry_rust::upload_coordinator::BlobUploadCoordinatorConfig;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio::io::AsyncRead;
use uuid::Uuid;

fn sha256_digest(bytes: &[u8]) -> Digest {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let hex = hex::encode(hasher.finalize());
    Digest::parse(&format!("sha256:{hex}")).expect("valid sha256 digest")
}

// =================================================================================================
// 1. Filesystem: Shared Backend State Across Segregated Port Views
// =================================================================================================
#[tokio::test]
async fn test_storage_wiring_shared_backend_state_across_port_views_fs() {
    let temp = TempDir::new().unwrap();
    let fs_root = temp.path().join("root");
    std::fs::create_dir_all(&fs_root).unwrap();

    let backend = Arc::new(FsStorage::new(fs_root, 10 * 1024 * 1024));
    let wiring = StorageWiring::from_backend(backend);

    assert_eq!(wiring.backend_kind(), "fs");

    let blob_mutation = wiring.blob_mutation();
    let blob_reader = wiring.blob_reader();
    let membership_reader = wiring.membership_reader();
    let tag_reader = wiring.tag_reader();

    // 1. Upload a blob via blob_mutation port view
    let payload = b"shared-storage-verification-payload-fs";
    let digest = sha256_digest(payload);
    let upload = blob_mutation.create_upload().await.expect("create upload");
    blob_mutation
        .append_upload(&upload.uuid, Bytes::from_static(payload))
        .await
        .expect("append");
    blob_mutation
        .finalize_upload(&upload.uuid, &digest)
        .await
        .expect("finalize");

    // 2. Link blob membership via blob_mutation port view
    let canonical = CanonicalRepoName::parse("library/test-app").unwrap();
    let rec = RepoBlobMembershipRecord::new_upload(canonical.clone(), digest.clone(), None);
    blob_mutation.link_repo_blob(&rec).await.expect("link");

    // 3. Immediately read the blob via the segregated blob_reader view (proves shared backend)
    let meta = blob_reader
        .head_blob(&digest)
        .await
        .expect("head_blob on blob_reader view");
    assert_eq!(meta.size, payload.len() as u64);

    // 4. Immediately check membership via membership_reader view (proves shared backend)
    let membership = membership_reader
        .get_repo_blob_membership("library/test-app", &digest)
        .await
        .expect("get membership");
    assert!(membership.is_some());
    assert_eq!(membership.unwrap().digest, digest);

    // 5. Mutate tag via manifest_lifecycle port view
    let manifest_lifecycle = wiring.manifest_lifecycle();
    manifest_lifecycle
        .set_tag("library/test-app", "v1.0.0", &digest)
        .await
        .expect("set tag");

    // 6. Read tag via tag_reader port view (proves shared backend)
    let resolved = tag_reader
        .resolve_tag("library/test-app", "v1.0.0")
        .await
        .expect("resolve tag");
    assert_eq!(resolved, digest);

    let tag_list = tag_reader
        .list_tags("library/test-app")
        .await
        .expect("list tags");
    assert_eq!(tag_list, vec!["v1.0.0".to_string()]);
}

// =================================================================================================
// 2. S3/MinIO: Shared Backend State Across Segregated Port Views
// =================================================================================================
#[tokio::test]
async fn test_storage_wiring_shared_backend_state_across_port_views_s3_minio() {
    let is_required = std::env::var("TEST_S3_REQUIRED").as_deref() == Ok("1");
    let endpoint = match std::env::var("TEST_S3_ENDPOINT") {
        Ok(ep) => ep,
        Err(_) => {
            if is_required {
                panic!(
                    "TEST_S3_REQUIRED=1 is enabled but TEST_S3_ENDPOINT is not set in environment"
                );
            }
            "http://127.0.0.1:9000".to_string()
        }
    };
    let bucket = match std::env::var("TEST_S3_BUCKET") {
        Ok(b) => b,
        Err(_) => {
            if is_required {
                panic!(
                    "TEST_S3_REQUIRED=1 is enabled but TEST_S3_BUCKET is not set in environment"
                );
            }
            "registry-live-test".to_string()
        }
    };
    let region = match std::env::var("TEST_S3_REGION") {
        Ok(r) => r,
        Err(_) => {
            if is_required {
                panic!(
                    "TEST_S3_REQUIRED=1 is enabled but TEST_S3_REGION is not set in environment"
                );
            }
            "us-east-1".to_string()
        }
    };

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let prefix = format!("live-test-ports-wiring-{}-{}/", Uuid::new_v4(), now_secs);

    // Ensure bucket exists on live MinIO
    let loader = aws_config::defaults(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region.clone()));
    let loader = if std::env::var("AWS_ACCESS_KEY_ID").is_err() {
        loader.credentials_provider(aws_sdk_s3::config::Credentials::new(
            "minioadmin",
            "minioadmin",
            None,
            None,
            "static",
        ))
    } else {
        loader
    };
    let shared = loader.load().await;
    let builder = aws_sdk_s3::config::Builder::from(&shared)
        .endpoint_url(&endpoint)
        .force_path_style(true);
    let s3_client = aws_sdk_s3::Client::from_conf(builder.build());

    // Verify S3 connectivity via endpoint-level probe (independent of test bucket existence)
    let probe = s3_client.list_buckets().send().await;
    if probe.is_err() && !is_required {
        println!("Skipping live S3 test: MinIO endpoint unreachable at {endpoint}");
        return;
    }
    probe.expect("MinIO live probe failed");

    // Create bucket if not present; MUST panic on connection failure
    let create_res = s3_client.create_bucket().bucket(&bucket).send().await;
    if let Err(e) = create_res {
        let err_str = e.to_string();
        if !err_str.contains("BucketAlreadyOwnedByYou") && !err_str.contains("BucketAlreadyExists")
        {
            s3_client
                .head_bucket()
                .bucket(&bucket)
                .send()
                .await
                .expect("MinIO live test endpoint must be reachable and bucket verified");
        }
    }

    // Construct single S3Storage backend instance
    let backend = Arc::new(S3Storage::new(
        Some(endpoint.clone()),
        Some(region.clone()),
        Some(bucket.clone()),
        prefix.clone(),
        50 * 1024 * 1024,
    ));

    // Construct StorageWiring from the single concrete S3Storage
    let wiring = StorageWiring::from_backend(backend);
    assert_eq!(wiring.backend_kind(), "s3");

    let blob_mutation = wiring.blob_mutation();
    let blob_reader = wiring.blob_reader();
    let membership_reader = wiring.membership_reader();
    let manifest_lifecycle = wiring.manifest_lifecycle();
    let tag_reader = wiring.tag_reader();

    // 1. Upload a blob via blob_mutation port view
    let payload = b"shared-storage-verification-payload-s3-minio";
    let digest = sha256_digest(payload);
    let upload = blob_mutation
        .create_upload()
        .await
        .expect("create upload on S3");
    blob_mutation
        .append_upload(&upload.uuid, Bytes::from_static(payload))
        .await
        .expect("append upload on S3");
    blob_mutation
        .finalize_upload(&upload.uuid, &digest)
        .await
        .expect("finalize upload on S3");

    // 2. Link blob membership via blob_mutation port view
    let canonical = CanonicalRepoName::parse("library/s3-test-app").unwrap();
    let rec = RepoBlobMembershipRecord::new_upload(canonical.clone(), digest.clone(), None);
    blob_mutation
        .link_repo_blob(&rec)
        .await
        .expect("link repo blob on S3");

    // 3. Immediately read blob via blob_reader view (proves shared backend)
    let meta = blob_reader
        .head_blob(&digest)
        .await
        .expect("head_blob on blob_reader view for S3");
    assert_eq!(meta.size, payload.len() as u64);

    // 4. Immediately check membership via membership_reader view (proves shared backend)
    let membership = membership_reader
        .get_repo_blob_membership("library/s3-test-app", &digest)
        .await
        .expect("get membership on S3");
    assert!(membership.is_some());
    assert_eq!(membership.unwrap().digest, digest);

    // 5. Mutate tag via manifest_lifecycle port view
    manifest_lifecycle
        .set_tag("library/s3-test-app", "v2.0.0", &digest)
        .await
        .expect("set tag on S3");

    // 6. Read tag via tag_reader port view (proves shared backend)
    let resolved = tag_reader
        .resolve_tag("library/s3-test-app", "v2.0.0")
        .await
        .expect("resolve tag on S3");
    assert_eq!(resolved, digest);

    let tag_list = tag_reader
        .list_tags("library/s3-test-app")
        .await
        .expect("list tags on S3");
    assert_eq!(tag_list, vec!["v2.0.0".to_string()]);

    // Cleanup S3 test prefix
    let list_res = s3_client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(&prefix)
        .send()
        .await
        .expect("list objects for cleanup");
    if let Some(contents) = list_res.contents {
        for obj in contents {
            if let Some(key) = obj.key {
                let _ = s3_client
                    .delete_object()
                    .bucket(&bucket)
                    .key(key)
                    .send()
                    .await;
            }
        }
    }

    // Assert post-cleanup absence
    let post_check = s3_client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(&prefix)
        .send()
        .await
        .expect("verify prefix is absent");
    assert_eq!(
        post_check.key_count().unwrap_or(0),
        0,
        "Expected 0 objects remaining under prefix {}",
        prefix
    );
    println!(
        "PORTS WIRING S3 CLEANUP PROOF: Verified 0 objects remaining under prefix '{}'",
        prefix
    );
}

// =================================================================================================
// 3. Minimal Fake Test: BlobUploadCoordinator & BlobMutationService
// =================================================================================================
#[derive(Clone, Default)]
struct FakeBlobUploadStorage {
    blobs: Arc<Mutex<HashMap<Digest, Bytes>>>,
    memberships: Arc<Mutex<HashMap<(String, Digest), RepoBlobMembershipRecord>>>,
    tags: Arc<Mutex<HashMap<(String, String), Digest>>>,
    uploads: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

#[async_trait::async_trait]
impl BlobCasReader for FakeBlobUploadStorage {
    async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError> {
        let guard = self.blobs.lock().unwrap();
        guard
            .get(digest)
            .map(|b| BlobMeta {
                size: b.len() as u64,
            })
            .ok_or(StorageError::NotFound)
    }

    async fn open_blob(
        &self,
        digest: &Digest,
    ) -> Result<(BlobMeta, Pin<Box<dyn AsyncRead + Send>>), StorageError> {
        let guard = self.blobs.lock().unwrap();
        let b = guard.get(digest).cloned().ok_or(StorageError::NotFound)?;
        let meta = BlobMeta {
            size: b.len() as u64,
        };
        let reader = Box::pin(std::io::Cursor::new(b));
        Ok((meta, reader))
    }
}

#[async_trait::async_trait]
impl BlobCasWriter for FakeBlobUploadStorage {
    async fn create_upload(&self) -> Result<UploadMeta, StorageError> {
        let uuid = Uuid::new_v4().to_string();
        self.uploads
            .lock()
            .unwrap()
            .insert(uuid.clone(), Vec::new());
        Ok(UploadMeta { uuid, offset: 0 })
    }

    async fn upload_status(&self, uuid: &str) -> Result<UploadMeta, StorageError> {
        let guard = self.uploads.lock().unwrap();
        let buf = guard.get(uuid).ok_or(StorageError::NotFound)?;
        Ok(UploadMeta {
            uuid: uuid.to_string(),
            offset: buf.len() as u64,
        })
    }

    async fn append_upload(&self, uuid: &str, chunk: Bytes) -> Result<UploadMeta, StorageError> {
        let mut guard = self.uploads.lock().unwrap();
        let buf = guard.get_mut(uuid).ok_or(StorageError::NotFound)?;
        buf.extend_from_slice(&chunk);
        Ok(UploadMeta {
            uuid: uuid.to_string(),
            offset: buf.len() as u64,
        })
    }

    async fn finalize_upload(&self, uuid: &str, digest: &Digest) -> Result<BlobMeta, StorageError> {
        let mut guard = self.uploads.lock().unwrap();
        let buf = guard.remove(uuid).ok_or(StorageError::NotFound)?;
        let size = buf.len() as u64;
        self.blobs
            .lock()
            .unwrap()
            .insert(digest.clone(), Bytes::from(buf));
        Ok(BlobMeta { size })
    }

    async fn abort_upload(&self, uuid: &str) -> Result<(), StorageError> {
        let mut guard = self.uploads.lock().unwrap();
        guard.remove(uuid).ok_or(StorageError::NotFound)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl RepositoryBlobMembershipStorage for FakeBlobUploadStorage {
    async fn link_repo_blob(&self, record: &RepoBlobMembershipRecord) -> Result<(), StorageError> {
        self.memberships.lock().unwrap().insert(
            (record.repo.as_str().to_string(), record.digest.clone()),
            record.clone(),
        );
        Ok(())
    }

    async fn unlink_repo_blob(&self, repo: &str, digest: &Digest) -> Result<bool, StorageError> {
        let mut guard = self.memberships.lock().unwrap();
        Ok(guard.remove(&(repo.to_string(), digest.clone())).is_some())
    }

    async fn get_repo_blob_membership(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<Option<RepoBlobMembershipRecord>, StorageError> {
        let guard = self.memberships.lock().unwrap();
        Ok(guard.get(&(repo.to_string(), digest.clone())).cloned())
    }

    async fn list_repo_blob_memberships_page(
        &self,
        repo: &str,
        _continuation_token: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<RepoBlobMembershipRecord>, Option<String>), StorageError> {
        let guard = self.memberships.lock().unwrap();
        let list: Vec<RepoBlobMembershipRecord> = guard
            .iter()
            .filter(|((r, _), _)| r == repo)
            .take(limit)
            .map(|(_, v)| v.clone())
            .collect();
        Ok((list, None))
    }
}

#[async_trait::async_trait]
impl RepositoryCatalogReader for FakeBlobUploadStorage {
    async fn list_repositories(&self) -> Result<Vec<String>, StorageError> {
        Ok(vec!["test-repo".to_string()])
    }
    async fn repo_timestamps(&self, _name: &str) -> Result<RepoTimestamps, StorageError> {
        Ok(RepoTimestamps {
            last_tag_update: Some(SystemTime::now()),
            last_manifest_update: Some(SystemTime::now()),
        })
    }
}

#[async_trait::async_trait]
impl ManifestReader for FakeBlobUploadStorage {
    async fn head_manifest(
        &self,
        _name: &str,
        _digest: &Digest,
    ) -> Result<ManifestMeta, StorageError> {
        Err(StorageError::NotFound)
    }
    async fn get_manifest(
        &self,
        _name: &str,
        _digest: &Digest,
    ) -> Result<(ManifestMeta, Bytes), StorageError> {
        Err(StorageError::NotFound)
    }
    async fn list_manifest_digests_page(
        &self,
        _repo: &str,
        _continuation_token: Option<&str>,
        _limit: usize,
    ) -> Result<(Vec<Digest>, Option<String>), StorageError> {
        Ok((Vec::new(), None))
    }
}

#[async_trait::async_trait]
impl TagReader for FakeBlobUploadStorage {
    async fn resolve_tag(&self, repo: &str, tag: &str) -> Result<Digest, StorageError> {
        let guard = self.tags.lock().unwrap();
        guard
            .get(&(repo.to_string(), tag.to_string()))
            .cloned()
            .ok_or(StorageError::NotFound)
    }
    async fn list_tags(&self, _name: &str) -> Result<Vec<String>, StorageError> {
        Ok(Vec::new())
    }
    async fn list_tags_page(
        &self,
        _repo: &str,
        _continuation_token: Option<&str>,
        _limit: usize,
    ) -> Result<(Vec<(String, Digest)>, Option<String>), StorageError> {
        Ok((Vec::new(), None))
    }
    async fn get_tag_with_version(
        &self,
        repo: &str,
        tag: &str,
    ) -> Result<Option<(Digest, String)>, StorageError> {
        match self.resolve_tag(repo, tag).await {
            Ok(d) => Ok(Some((d, "v1".to_string()))),
            Err(_) => Ok(None),
        }
    }
}

#[async_trait::async_trait]
impl UploadSessionStorage for FakeBlobUploadStorage {
    async fn create_session(
        &self,
        repo: &str,
    ) -> Result<registry_rust::storage::upload_session::UploadSessionId, StorageError> {
        let uuid = Uuid::new_v4().to_string();
        self.uploads
            .lock()
            .unwrap()
            .insert(uuid.clone(), Vec::new());
        let canonical = CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        Ok(registry_rust::storage::upload_session::UploadSessionId {
            repo: canonical,
            uuid,
        })
    }

    async fn session_status(
        &self,
        session: &registry_rust::storage::upload_session::UploadSessionId,
    ) -> Result<
        registry_rust::storage::upload_session::UploadSessionStatus,
        registry_rust::storage::upload_session::UploadTransitionError,
    > {
        let guard = self.uploads.lock().unwrap();
        if let Some(buf) = guard.get(&session.uuid) {
            Ok(
                registry_rust::storage::upload_session::UploadSessionStatus {
                    session: session.clone(),
                    state: registry_rust::storage::upload_session::UploadSessionState::Active,
                    committed_offset: buf.len() as u64,
                    created_at: SystemTime::now(),
                    last_active_at: SystemTime::now(),
                },
            )
        } else {
            Err(registry_rust::storage::upload_session::UploadTransitionError::NotFound)
        }
    }

    async fn abort_session(
        &self,
        session: &registry_rust::storage::upload_session::UploadSessionId,
    ) -> Result<(), StorageError> {
        let mut guard = self.uploads.lock().unwrap();
        guard.remove(&session.uuid);
        Ok(())
    }
}

#[tokio::test]
async fn test_minimal_fake_blob_upload_coordinator_and_mutation_service() {
    let fake_storage = Arc::new(FakeBlobUploadStorage::default());
    let coordinator = ConsistencyCoordinator::new();
    let config = BlobUploadCoordinatorConfig {
        signing_key: b"secret-key-1234567890123456".to_vec(),
        max_upload_bytes: 10 * 1024 * 1024,
        abort_on_digest_mismatch: false,
        disallow_monolithic_uploads: false,
        upload_chunk_min_bytes: Some(0),
        gc_pin_duration_secs: 60,
        finalize_grace_secs: 0,
    };

    // Construct BlobMutationService using ONLY BlobUploadCoordinatorStoragePort (no Storage)
    let service = BlobMutationService::new(fake_storage.clone(), None, coordinator, config);

    let start_res = service
        .start_upload("myorg/myrepo")
        .await
        .expect("start upload");
    assert!(!start_res.session.uuid.is_empty());
    assert!(!start_res.state_token.is_empty());

    let status = service
        .get_upload_status(
            "myorg/myrepo",
            &start_res.session.uuid,
            Some(&start_res.state_token),
        )
        .await
        .expect("get status");
    assert_eq!(status.offset, 0);

    service
        .abort_upload(
            "myorg/myrepo",
            &start_res.session.uuid,
            Some(&start_res.state_token),
        )
        .await
        .expect("abort upload");
}

// =================================================================================================
// 4. Minimal Fake Test: ManifestLifecycleService & ManifestMutationService
// =================================================================================================
#[derive(Clone, Default)]
#[allow(clippy::type_complexity)]
struct FakeManifestLifecycleStorage {
    manifests: Arc<Mutex<HashMap<(String, Digest), Bytes>>>,
    tags: Arc<Mutex<HashMap<(String, String), (Digest, String)>>>,
    memberships: Arc<Mutex<HashMap<(String, Digest), RepoBlobMembershipRecord>>>,
    journals: Arc<Mutex<HashMap<String, Bytes>>>,
    leases: Arc<Mutex<HashMap<String, String>>>,
    referrers: Arc<Mutex<HashMap<(String, Digest), Vec<ReferrerDescriptor>>>>,
}

#[async_trait::async_trait]
impl RepositoryCatalogReader for FakeManifestLifecycleStorage {
    async fn list_repositories(&self) -> Result<Vec<String>, StorageError> {
        Ok(vec!["myorg/myrepo".to_string()])
    }
    async fn repo_timestamps(&self, _name: &str) -> Result<RepoTimestamps, StorageError> {
        Ok(RepoTimestamps {
            last_tag_update: Some(SystemTime::now()),
            last_manifest_update: Some(SystemTime::now()),
        })
    }
}

#[async_trait::async_trait]
impl ManifestReader for FakeManifestLifecycleStorage {
    async fn head_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<ManifestMeta, StorageError> {
        let guard = self.manifests.lock().unwrap();
        guard
            .get(&(name.to_string(), digest.clone()))
            .map(|b| ManifestMeta {
                size: b.len() as u64,
                media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
            })
            .ok_or(StorageError::NotFound)
    }

    async fn get_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<(ManifestMeta, Bytes), StorageError> {
        let guard = self.manifests.lock().unwrap();
        let bytes = guard
            .get(&(name.to_string(), digest.clone()))
            .cloned()
            .ok_or(StorageError::NotFound)?;
        let meta = ManifestMeta {
            size: bytes.len() as u64,
            media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
        };
        Ok((meta, bytes))
    }

    async fn list_manifest_digests_page(
        &self,
        _repo: &str,
        _continuation_token: Option<&str>,
        _limit: usize,
    ) -> Result<(Vec<Digest>, Option<String>), StorageError> {
        Ok((Vec::new(), None))
    }
}

#[async_trait::async_trait]
impl ManifestStore for FakeManifestLifecycleStorage {
    async fn put_manifest(
        &self,
        name: &str,
        digest: &Digest,
        bytes: Bytes,
    ) -> Result<ManifestMeta, StorageError> {
        let size = bytes.len() as u64;
        self.manifests
            .lock()
            .unwrap()
            .insert((name.to_string(), digest.clone()), bytes);
        Ok(ManifestMeta {
            size,
            media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
        })
    }

    async fn delete_manifest(&self, name: &str, digest: &Digest) -> Result<(), StorageError> {
        self.manifests
            .lock()
            .unwrap()
            .remove(&(name.to_string(), digest.clone()))
            .ok_or(StorageError::NotFound)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl TagReader for FakeManifestLifecycleStorage {
    async fn resolve_tag(&self, repo: &str, tag: &str) -> Result<Digest, StorageError> {
        let guard = self.tags.lock().unwrap();
        guard
            .get(&(repo.to_string(), tag.to_string()))
            .map(|(d, _)| d.clone())
            .ok_or(StorageError::NotFound)
    }

    async fn list_tags(&self, repo: &str) -> Result<Vec<String>, StorageError> {
        let guard = self.tags.lock().unwrap();
        let mut list: Vec<String> = guard
            .keys()
            .filter(|(r, _)| r == repo)
            .map(|(_, t)| t.clone())
            .collect();
        list.sort();
        Ok(list)
    }

    async fn list_tags_page(
        &self,
        repo: &str,
        _continuation_token: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<(String, Digest)>, Option<String>), StorageError> {
        let guard = self.tags.lock().unwrap();
        let list: Vec<(String, Digest)> = guard
            .iter()
            .filter(|((r, _), _)| r == repo)
            .take(limit)
            .map(|((_, t), (d, _))| (t.clone(), d.clone()))
            .collect();
        Ok((list, None))
    }

    async fn get_tag_with_version(
        &self,
        repo: &str,
        tag: &str,
    ) -> Result<Option<(Digest, String)>, StorageError> {
        let guard = self.tags.lock().unwrap();
        Ok(guard.get(&(repo.to_string(), tag.to_string())).cloned())
    }
}

#[async_trait::async_trait]
impl TagStore for FakeManifestLifecycleStorage {
    async fn set_tag(&self, repo: &str, tag: &str, digest: &Digest) -> Result<(), StorageError> {
        self.tags.lock().unwrap().insert(
            (repo.to_string(), tag.to_string()),
            (digest.clone(), "v1".to_string()),
        );
        Ok(())
    }

    async fn mutate_tag(
        &self,
        repo: &str,
        tag: &str,
        digest: &Digest,
        _policy: TagMutationPolicy,
    ) -> Result<TagMutation, StorageError> {
        self.set_tag(repo, tag, digest).await?;
        Ok(TagMutation::Created)
    }

    async fn delete_tag(&self, repo: &str, tag: &str) -> Result<(), StorageError> {
        self.tags
            .lock()
            .unwrap()
            .remove(&(repo.to_string(), tag.to_string()))
            .ok_or(StorageError::NotFound)?;
        Ok(())
    }

    async fn delete_tag_conditional(
        &self,
        repo: &str,
        tag: &str,
        _expected_version: Option<&str>,
    ) -> Result<ConditionalDeleteResult, StorageError> {
        self.delete_tag(repo, tag).await?;
        Ok(ConditionalDeleteResult::Deleted)
    }
}

#[async_trait::async_trait]
impl RepositoryBlobMembershipStorage for FakeManifestLifecycleStorage {
    async fn link_repo_blob(&self, record: &RepoBlobMembershipRecord) -> Result<(), StorageError> {
        self.memberships.lock().unwrap().insert(
            (record.repo.as_str().to_string(), record.digest.clone()),
            record.clone(),
        );
        Ok(())
    }

    async fn unlink_repo_blob(&self, repo: &str, digest: &Digest) -> Result<bool, StorageError> {
        let mut guard = self.memberships.lock().unwrap();
        Ok(guard.remove(&(repo.to_string(), digest.clone())).is_some())
    }

    async fn get_repo_blob_membership(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<Option<RepoBlobMembershipRecord>, StorageError> {
        let guard = self.memberships.lock().unwrap();
        Ok(guard.get(&(repo.to_string(), digest.clone())).cloned())
    }

    async fn list_repo_blob_memberships_page(
        &self,
        repo: &str,
        _continuation_token: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<RepoBlobMembershipRecord>, Option<String>), StorageError> {
        let guard = self.memberships.lock().unwrap();
        let list: Vec<RepoBlobMembershipRecord> = guard
            .iter()
            .filter(|((r, _), _)| r == repo)
            .take(limit)
            .map(|(_, v)| v.clone())
            .collect();
        Ok((list, None))
    }
}

#[async_trait::async_trait]
impl ReferrersReader for FakeManifestLifecycleStorage {
    async fn list_referrers(
        &self,
        repo: &str,
        subject: &Digest,
    ) -> Result<Vec<ReferrerDescriptor>, StorageError> {
        let guard = self.referrers.lock().unwrap();
        Ok(guard
            .get(&(repo.to_string(), subject.clone()))
            .cloned()
            .unwrap_or_default())
    }

    async fn list_referrers_page(
        &self,
        repo: &str,
        subject: &Digest,
        _continuation_token: Option<&str>,
        _limit: usize,
    ) -> Result<(Vec<ReferrerDescriptor>, Option<String>), StorageError> {
        let list = self.list_referrers(repo, subject).await?;
        Ok((list, None))
    }
}

#[async_trait::async_trait]
impl ReferrersStore for FakeManifestLifecycleStorage {
    async fn add_referrer(
        &self,
        repo: &str,
        subject: &Digest,
        descriptor: ReferrerDescriptor,
    ) -> Result<(), StorageError> {
        let mut guard = self.referrers.lock().unwrap();
        guard
            .entry((repo.to_string(), subject.clone()))
            .or_default()
            .push(descriptor);
        Ok(())
    }

    async fn remove_referrer(
        &self,
        repo: &str,
        subject: &Digest,
        referrer: &Digest,
    ) -> Result<(), StorageError> {
        let mut guard = self.referrers.lock().unwrap();
        if let Some(list) = guard.get_mut(&(repo.to_string(), subject.clone())) {
            list.retain(|d| d.digest != referrer.as_str());
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl LifecycleJournalStore for FakeManifestLifecycleStorage {
    async fn read_lifecycle_journal(&self, repo: &str) -> Result<Option<Bytes>, StorageError> {
        let guard = self.journals.lock().unwrap();
        Ok(guard.get(repo).cloned())
    }

    async fn write_lifecycle_journal(&self, repo: &str, data: Bytes) -> Result<(), StorageError> {
        self.journals.lock().unwrap().insert(repo.to_string(), data);
        Ok(())
    }

    async fn delete_lifecycle_journal(&self, repo: &str) -> Result<(), StorageError> {
        self.journals.lock().unwrap().remove(repo);
        Ok(())
    }
}

#[async_trait::async_trait]
impl RepositoryLeaseStore for FakeManifestLifecycleStorage {
    async fn acquire_repo_lease(
        &self,
        repo: &str,
        owner_id: &str,
        _lease_id: &str,
        _ttl_secs: u64,
    ) -> Result<bool, StorageError> {
        let mut guard = self.leases.lock().unwrap();
        if guard.contains_key(repo) {
            Ok(false)
        } else {
            guard.insert(repo.to_string(), owner_id.to_string());
            Ok(true)
        }
    }

    async fn renew_repo_lease(
        &self,
        _repo: &str,
        _owner_id: &str,
        _lease_id: &str,
        _ttl_secs: u64,
    ) -> Result<bool, StorageError> {
        Ok(true)
    }

    async fn release_repo_lease(
        &self,
        repo: &str,
        _owner_id: &str,
        _lease_id: &str,
    ) -> Result<(), StorageError> {
        self.leases.lock().unwrap().remove(repo);
        Ok(())
    }
}

#[tokio::test]
async fn test_minimal_fake_manifest_lifecycle_and_mutation_service() {
    let fake_storage = Arc::new(FakeManifestLifecycleStorage::default());
    let coordinator = ConsistencyCoordinator::new();

    // Construct ManifestMutationService using ONLY ManifestLifecycleStoragePort (no Storage)
    let service = ManifestMutationService::new(fake_storage.clone(), None, coordinator);

    let digest = sha256_digest(b"manifest-bytes");
    fake_storage
        .set_tag("myorg/myimage", "v1.0.0", &digest)
        .await
        .unwrap();

    let del_res = service
        .delete_tag("myorg/myimage", "v1.0.0", true)
        .await
        .expect("delete tag");
    assert_eq!(del_res.tag, "v1.0.0");
    assert_eq!(del_res.target_digest, digest);
}

// =================================================================================================
// 5. Minimal Fake Test: ClusterLockStore & RuntimeMutationAuthority
// =================================================================================================
#[derive(Clone, Default)]
struct FakeClusterLockStorage {
    lock: Arc<Mutex<Option<(DeploymentWriterLockDoc, String)>>>,
    version_seq: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl ClusterLockStore for FakeClusterLockStorage {
    async fn acquire_deployment_writer_lock(
        &self,
        doc: &DeploymentWriterLockDoc,
    ) -> Result<(bool, Option<String>), StorageError> {
        let mut guard = self.lock.lock().unwrap();
        if let Some((existing, etag)) = guard.as_ref() {
            if existing.owner_id == doc.owner_id {
                let v = self.version_seq.fetch_add(1, Ordering::SeqCst);
                let new_etag = format!("etag-{v}");
                *guard = Some((doc.clone(), new_etag.clone()));
                Ok((true, Some(new_etag)))
            } else {
                Ok((false, Some(etag.clone())))
            }
        } else {
            let v = self.version_seq.fetch_add(1, Ordering::SeqCst);
            let new_etag = format!("etag-{v}");
            *guard = Some((doc.clone(), new_etag.clone()));
            Ok((true, Some(new_etag)))
        }
    }

    async fn release_deployment_writer_lock(
        &self,
        _doc: &DeploymentWriterLockDoc,
        _expected_etag: Option<&str>,
    ) -> Result<bool, StorageError> {
        let mut guard = self.lock.lock().unwrap();
        *guard = None;
        Ok(true)
    }

    async fn inspect_deployment_writer_lock(
        &self,
    ) -> Result<Option<(DeploymentWriterLockDoc, Option<String>)>, StorageError> {
        let guard = self.lock.lock().unwrap();
        Ok(guard.as_ref().map(|(d, e)| (d.clone(), Some(e.clone()))))
    }

    async fn admin_clear_deployment_writer_lock(
        &self,
        _expected_owner: &str,
        _expected_etag: &str,
    ) -> Result<(), StorageError> {
        let mut guard = self.lock.lock().unwrap();
        *guard = None;
        Ok(())
    }
}

#[tokio::test]
async fn test_minimal_fake_runtime_mutation_authority() {
    let fake_storage = Arc::new(FakeClusterLockStorage::default());

    // Acquire authority using ONLY ClusterLockStore (no Storage)
    let authority = RuntimeMutationAuthority::acquire(fake_storage.clone(), "test-node-1")
        .await
        .expect("acquire authority");
    assert!(authority.is_active());

    let permit: GcMutationPermit<'_> = authority.gc_mutation_permit();
    assert!(permit.owner_id().ends_with("test-node-1"));

    let inspect = fake_storage
        .inspect_deployment_writer_lock()
        .await
        .expect("inspect");
    assert!(inspect.is_some());
    assert!(inspect.unwrap().0.owner_id.ends_with("test-node-1"));
}

// =================================================================================================
// 6. Minimal Fake Test: GcService & PolicyContext
// =================================================================================================
#[derive(Clone, Default)]
struct FakeGcServiceStorage {
    blobs: Arc<Mutex<HashMap<Digest, Bytes>>>,
    quarantined: Arc<Mutex<HashMap<Digest, Bytes>>>,
    tags: Arc<Mutex<HashMap<(String, String), Digest>>>,
    memberships: Arc<Mutex<HashMap<(String, Digest), RepoBlobMembershipRecord>>>,
    journals: Arc<Mutex<HashMap<String, Bytes>>>,
}

#[async_trait::async_trait]
impl GcStoragePort for FakeGcServiceStorage {
    fn kind(&self) -> &'static str {
        "fake"
    }

    fn gc_strategy(&self) -> GcStorageStrategy {
        GcStorageStrategy::FilesystemQuarantine
    }

    async fn check_bucket_versioning_for_gc(&self) -> Result<(), StorageError> {
        Ok(())
    }

    async fn list_cas_blobs_page(
        &self,
        _cursor: Option<&GcCursor>,
        _limit: usize,
    ) -> Result<GcBlobPage, StorageError> {
        let guard = self.blobs.lock().unwrap();
        let items: Vec<GcBlobCandidate> = guard
            .iter()
            .map(|(d, b)| GcBlobCandidate {
                digest: d.clone(),
                size: b.len() as u64,
                last_modified: SystemTime::now(),
                version: BlobObjectVersion("v1".to_string()),
            })
            .collect();
        Ok(GcBlobPage {
            items,
            next_cursor: None,
        })
    }

    async fn quarantine_blob(
        &self,
        _permit: &GcMutationPermit<'_>,
        digest: &Digest,
        _version: &BlobObjectVersion,
    ) -> Result<GcQuarantineResult, StorageError> {
        let mut blobs = self.blobs.lock().unwrap();
        let bytes = blobs.remove(digest).ok_or(StorageError::NotFound)?;
        let size = bytes.len() as u64;
        self.quarantined
            .lock()
            .unwrap()
            .insert(digest.clone(), bytes);
        Ok(GcQuarantineResult::Quarantined { size })
    }

    async fn restore_quarantined_blob(
        &self,
        _permit: &GcMutationPermit<'_>,
        digest: &Digest,
    ) -> Result<Option<u64>, StorageError> {
        let mut q = self.quarantined.lock().unwrap();
        if let Some(bytes) = q.remove(digest) {
            let size = bytes.len() as u64;
            self.blobs.lock().unwrap().insert(digest.clone(), bytes);
            Ok(Some(size))
        } else {
            Ok(None)
        }
    }

    async fn quarantined_blob_version(
        &self,
        digest: &Digest,
    ) -> Result<Option<BlobObjectVersion>, StorageError> {
        let q = self.quarantined.lock().unwrap();
        if q.contains_key(digest) {
            Ok(Some(BlobObjectVersion("fs-v1".to_string())))
        } else {
            Ok(None)
        }
    }

    async fn delete_blob_conditional(
        &self,
        _permit: &GcMutationPermit<'_>,
        digest: &Digest,
        _version: Option<&BlobObjectVersion>,
    ) -> Result<GcDeleteResult, StorageError> {
        let mut q = self.quarantined.lock().unwrap();
        if q.remove(digest).is_some() {
            Ok(GcDeleteResult::Deleted)
        } else {
            Ok(GcDeleteResult::NotFound)
        }
    }

    async fn discover_manifest_references(
        &self,
    ) -> Result<Option<std::collections::HashSet<Digest>>, StorageError> {
        Ok(None)
    }
}

#[async_trait::async_trait]
impl RepositoryCatalogReader for FakeGcServiceStorage {
    async fn list_repositories(&self) -> Result<Vec<String>, StorageError> {
        Ok(Vec::new())
    }
    async fn repo_timestamps(&self, _name: &str) -> Result<RepoTimestamps, StorageError> {
        Ok(RepoTimestamps {
            last_tag_update: Some(SystemTime::now()),
            last_manifest_update: Some(SystemTime::now()),
        })
    }
}

#[async_trait::async_trait]
impl ManifestReader for FakeGcServiceStorage {
    async fn head_manifest(
        &self,
        _name: &str,
        _digest: &Digest,
    ) -> Result<ManifestMeta, StorageError> {
        Err(StorageError::NotFound)
    }
    async fn get_manifest(
        &self,
        _name: &str,
        _digest: &Digest,
    ) -> Result<(ManifestMeta, Bytes), StorageError> {
        Err(StorageError::NotFound)
    }
    async fn list_manifest_digests_page(
        &self,
        _repo: &str,
        _continuation_token: Option<&str>,
        _limit: usize,
    ) -> Result<(Vec<Digest>, Option<String>), StorageError> {
        Ok((Vec::new(), None))
    }
}

#[async_trait::async_trait]
impl TagReader for FakeGcServiceStorage {
    async fn resolve_tag(&self, repo: &str, tag: &str) -> Result<Digest, StorageError> {
        let guard = self.tags.lock().unwrap();
        guard
            .get(&(repo.to_string(), tag.to_string()))
            .cloned()
            .ok_or(StorageError::NotFound)
    }
    async fn list_tags(&self, _name: &str) -> Result<Vec<String>, StorageError> {
        Ok(Vec::new())
    }
    async fn list_tags_page(
        &self,
        _repo: &str,
        _continuation_token: Option<&str>,
        _limit: usize,
    ) -> Result<(Vec<(String, Digest)>, Option<String>), StorageError> {
        Ok((Vec::new(), None))
    }
    async fn get_tag_with_version(
        &self,
        repo: &str,
        tag: &str,
    ) -> Result<Option<(Digest, String)>, StorageError> {
        match self.resolve_tag(repo, tag).await {
            Ok(d) => Ok(Some((d, "v1".to_string()))),
            Err(_) => Ok(None),
        }
    }
}

#[async_trait::async_trait]
impl RepositoryBlobMembershipStorage for FakeGcServiceStorage {
    async fn link_repo_blob(&self, record: &RepoBlobMembershipRecord) -> Result<(), StorageError> {
        self.memberships.lock().unwrap().insert(
            (record.repo.as_str().to_string(), record.digest.clone()),
            record.clone(),
        );
        Ok(())
    }

    async fn unlink_repo_blob(&self, repo: &str, digest: &Digest) -> Result<bool, StorageError> {
        let mut guard = self.memberships.lock().unwrap();
        Ok(guard.remove(&(repo.to_string(), digest.clone())).is_some())
    }

    async fn get_repo_blob_membership(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<Option<RepoBlobMembershipRecord>, StorageError> {
        let guard = self.memberships.lock().unwrap();
        Ok(guard.get(&(repo.to_string(), digest.clone())).cloned())
    }

    async fn list_repo_blob_memberships_page(
        &self,
        repo: &str,
        _continuation_token: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<RepoBlobMembershipRecord>, Option<String>), StorageError> {
        let guard = self.memberships.lock().unwrap();
        let list: Vec<RepoBlobMembershipRecord> = guard
            .iter()
            .filter(|((r, _), _)| r == repo)
            .take(limit)
            .map(|(_, v)| v.clone())
            .collect();
        Ok((list, None))
    }
}

#[async_trait::async_trait]
impl LifecycleJournalStore for FakeGcServiceStorage {
    async fn read_lifecycle_journal(&self, repo: &str) -> Result<Option<Bytes>, StorageError> {
        let guard = self.journals.lock().unwrap();
        Ok(guard.get(repo).cloned())
    }

    async fn write_lifecycle_journal(&self, repo: &str, data: Bytes) -> Result<(), StorageError> {
        self.journals.lock().unwrap().insert(repo.to_string(), data);
        Ok(())
    }

    async fn delete_lifecycle_journal(&self, repo: &str) -> Result<(), StorageError> {
        self.journals.lock().unwrap().remove(repo);
        Ok(())
    }
}

#[tokio::test]
async fn test_minimal_fake_gc_service_and_policy_context() {
    let fake_gc_storage = Arc::new(FakeGcServiceStorage::default());
    let fake_upload_storage = Arc::new(FakeBlobUploadStorage::default());
    let fake_cluster_lock = Arc::new(FakeClusterLockStorage::default());

    let temp = TempDir::new().unwrap();
    let idx_path = temp.path().join("index.sled");
    let idx = Arc::new(BlobRefIndex::open(idx_path).expect("open index"));
    idx.ensure_healthy_or_rebuild(fake_upload_storage.as_ref(), true, true)
        .await
        .expect("rebuild index");

    let cfg = Config::from_env().unwrap();
    let authority = RuntimeMutationAuthority::acquire(fake_cluster_lock, "gc-test")
        .await
        .expect("acquire authority");
    let coordinator = ConsistencyCoordinator::new();

    // Construct GcService using ONLY GcServiceStoragePort (no Storage)
    let gc_service = GcService::with_coordinator_and_authority(
        Arc::new(registry_rust::policy::GcPolicy::from(&cfg)),
        fake_gc_storage.clone(),
        idx.clone(),
        coordinator,
        Arc::new(tokio::sync::Mutex::new(Some(authority))),
    );

    let plan_res = gc_service
        .plan(
            BlobGcPolicy::TagRooted,
            Duration::from_secs(0),
            GcBudgets {
                max_blobs: 100,
                max_bytes: 1024 * 1024,
                max_seconds: 10,
            },
        )
        .await
        .expect("gc plan");
    assert_eq!(plan_res.scanned_blobs, 0);

    // Build PolicyContext with GcServiceStoragePort
    let mut policy_ctx =
        PolicyContext::build(fake_gc_storage.as_ref(), &idx, BlobGcPolicy::TagRooted)
            .await
            .expect("build policy ctx");
    let unref = sha256_digest(b"unreferenced-blob");
    let is_ref = policy_ctx
        .is_referenced(&unref)
        .await
        .expect("is referenced");
    assert!(!is_ref);
}

// =================================================================================================
// 7. Minimal Fake Test: BlobRefIndex & RepositoryMembershipLedger
// =================================================================================================
#[tokio::test]
async fn test_minimal_fake_repository_membership_ledger_and_ref_index() {
    let fake_storage = Arc::new(FakeBlobUploadStorage::default());
    let coordinator = ConsistencyCoordinator::new();

    let temp = TempDir::new().unwrap();
    let idx_path = temp.path().join("index.sled");
    let idx = Arc::new(BlobRefIndex::open(idx_path).expect("open index"));
    idx.ensure_healthy_or_rebuild(fake_storage.as_ref(), true, true)
        .await
        .expect("rebuild index");

    // Construct RepositoryMembershipLedger using ONLY BlobRefIndexStoragePort (no Storage)
    let ledger =
        RepositoryMembershipLedger::new(fake_storage.clone(), Some(idx.clone()), coordinator);

    let digest = sha256_digest(b"ledger-blob-payload");
    let canonical = CanonicalRepoName::parse("myorg/ledger-repo").unwrap();
    let rec = RepoBlobMembershipRecord::new_upload(canonical, digest.clone(), None);

    ledger.link(&rec).await.expect("link blob");

    let has = ledger
        .has_any_membership(&digest)
        .await
        .expect("has any membership");
    assert!(has);
}

// =================================================================================================
// 8. Minimal Fake Test: BlobDeleteService
// =================================================================================================
#[tokio::test]
async fn test_minimal_fake_blob_delete_service() {
    let fake_storage = Arc::new(FakeBlobUploadStorage::default());
    let coordinator = ConsistencyCoordinator::new();

    let temp = TempDir::new().unwrap();
    let idx_path = temp.path().join("index.sled");
    let idx = Arc::new(BlobRefIndex::open(idx_path).expect("open index"));
    idx.ensure_healthy_or_rebuild(fake_storage.as_ref(), true, true)
        .await
        .expect("rebuild index");

    let ledger =
        RepositoryMembershipLedger::new(fake_storage.clone(), Some(idx.clone()), coordinator);

    let digest = sha256_digest(b"blob-to-delete");
    let canonical = CanonicalRepoName::parse("myorg/delete-repo").unwrap();
    let rec = RepoBlobMembershipRecord::new_upload(canonical, digest.clone(), None);
    ledger.link(&rec).await.expect("link blob");

    // Construct BlobDeleteService from ledger and minimal index storage (no Storage)
    let delete_service = BlobDeleteService::new(fake_storage.clone(), ledger);

    let del_res = delete_service
        .delete_repo_blob("myorg/delete-repo", &digest)
        .await
        .expect("delete repo blob");
    assert!(matches!(
        del_res,
        registry_rust::blob_delete_safety::BlobDeleteResult::Success
    ));
}

// =================================================================================================
// 9. Narrow Port Isolation: BlobCasReader & TagReader
// =================================================================================================
struct FakeBlobCasOnlyReader {
    blobs: HashMap<Digest, Bytes>,
}

#[async_trait::async_trait]
impl BlobCasReader for FakeBlobCasOnlyReader {
    async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError> {
        self.blobs
            .get(digest)
            .map(|b| BlobMeta {
                size: b.len() as u64,
            })
            .ok_or(StorageError::NotFound)
    }

    async fn open_blob(
        &self,
        digest: &Digest,
    ) -> Result<(BlobMeta, Pin<Box<dyn AsyncRead + Send>>), StorageError> {
        let b = self.blobs.get(digest).ok_or(StorageError::NotFound)?;
        let meta = BlobMeta {
            size: b.len() as u64,
        };
        let reader = Box::pin(std::io::Cursor::new(b.clone()));
        Ok((meta, reader))
    }
}

#[tokio::test]
async fn test_isolated_blob_cas_reader_consumer_without_omnibus_storage() {
    let payload = b"isolated-blob-content";
    let digest = sha256_digest(payload);
    let mut map = HashMap::new();
    map.insert(digest.clone(), Bytes::from_static(payload));

    let fake_reader = FakeBlobCasOnlyReader { blobs: map };

    let meta = fake_reader.head_blob(&digest).await.expect("head blob");
    assert_eq!(meta.size, payload.len() as u64);

    let not_found = sha256_digest(b"nonexistent");
    let err = fake_reader.head_blob(&not_found).await;
    assert!(matches!(err, Err(StorageError::NotFound)));
}

struct FakeTagOnlyReader {
    tags: HashMap<(String, String), Digest>,
}

#[async_trait::async_trait]
impl TagReader for FakeTagOnlyReader {
    async fn resolve_tag(&self, repo: &str, tag: &str) -> Result<Digest, StorageError> {
        self.tags
            .get(&(repo.to_string(), tag.to_string()))
            .cloned()
            .ok_or(StorageError::NotFound)
    }

    async fn list_tags(&self, repo: &str) -> Result<Vec<String>, StorageError> {
        let mut list = Vec::new();
        for (k_repo, k_tag) in self.tags.keys() {
            if k_repo == repo {
                list.push(k_tag.clone());
            }
        }
        list.sort();
        Ok(list)
    }

    async fn list_tags_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<(String, Digest)>, Option<String>), StorageError> {
        let mut all: Vec<(String, Digest)> = self
            .tags
            .iter()
            .filter(|((r, _), _)| r == repo)
            .map(|((_, t), d)| (t.clone(), d.clone()))
            .collect();
        all.sort_by(|a, b| a.0.cmp(&b.0));
        let start = match continuation_token {
            Some(tok) => all.iter().position(|(t, _)| t == tok).unwrap_or(0),
            None => 0,
        };
        let page = all.into_iter().skip(start).take(limit).collect();
        Ok((page, None))
    }

    async fn get_tag_with_version(
        &self,
        repo: &str,
        tag: &str,
    ) -> Result<Option<(Digest, String)>, StorageError> {
        match self.resolve_tag(repo, tag).await {
            Ok(d) => Ok(Some((d, "v1".to_string()))),
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[tokio::test]
async fn test_isolated_tag_reader_consumer_without_omnibus_storage() {
    let digest = sha256_digest(b"manifest-payload");
    let mut tags = HashMap::new();
    tags.insert(
        ("myorg/myimage".to_string(), "latest".to_string()),
        digest.clone(),
    );

    let fake_tag_reader = FakeTagOnlyReader { tags };

    let target = fake_tag_reader
        .resolve_tag("myorg/myimage", "latest")
        .await
        .expect("resolve tag");
    assert_eq!(target, digest);

    let not_found = fake_tag_reader
        .resolve_tag("myorg/myimage", "missing")
        .await;
    assert!(matches!(not_found, Err(StorageError::NotFound)));
}
