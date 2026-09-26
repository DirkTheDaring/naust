mod support;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use registry_rust::blob_gc::policy::{GcPolicyError, build_manifest_protected_set};
use registry_rust::blob_gc::{
    BlobGcError, BlobGcLimits, BlobGcPolicy, blob_gc_delete_with_authority, blob_gc_plan,
    blob_gc_quarantine_with_authority,
};
use registry_rust::blob_ref_index::BlobRefIndex;
use registry_rust::config::{Config, StorageBackend};
use registry_rust::consistency::ConsistencyCoordinator;
use registry_rust::registry::digest::Digest;
use registry_rust::storage::fs::FsStorage;
use registry_rust::storage::mutation_authority::{GcMutationPermit, RuntimeMutationAuthority};
use registry_rust::storage::ports::*;
use registry_rust::storage::repo_membership::RepositoryBlobMembershipStorage;
use registry_rust::storage::{
    BlobObjectVersion, GcBlobPage, GcCursor, GcDeleteResult, GcQuarantineResult, GcStorageStrategy,
    ManifestMeta, RepoBlobMembershipRecord, RepoTimestamps, StorageError, StorageErrorKind,
};
use support::gc_coordination::{
    HookedStorage, LifecycleFaultStorage, StorageHooks, write_live_blob,
};

fn sha256_digest(bytes: &[u8]) -> Digest {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let hex = hex::encode(hasher.finalize());
    Digest::parse(&format!("sha256:{hex}")).expect("valid sha256 digest")
}

fn test_manifest_json(config_digest: &str, layer_digest: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": 70,
            "digest": config_digest
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "size": 150,
                "digest": layer_digest
            }
        ]
    }))
    .unwrap()
}

/// Recording wrapper around `GcServiceStoragePort` that records calls to
/// `list_repositories`, `list_manifests`, `quarantine_blob`, `delete_blob_conditional`,
/// and `restore_quarantined_blob`. Optionally fails closed if catalog fallback is invoked.
#[derive(Clone)]
struct RecordingGcServiceStoragePort {
    inner: Arc<dyn GcServiceStoragePort>,
    list_repositories_called: Arc<AtomicBool>,
    list_manifests_called: Arc<AtomicBool>,
    quarantine_blob_called: Arc<AtomicBool>,
    delete_blob_conditional_called: Arc<AtomicBool>,
    restore_quarantined_blob_called: Arc<AtomicBool>,
    fail_on_catalog_listing: bool,
}

impl RecordingGcServiceStoragePort {
    fn new(inner: Arc<dyn GcServiceStoragePort>, fail_on_catalog_listing: bool) -> Self {
        Self {
            inner,
            list_repositories_called: Arc::new(AtomicBool::new(false)),
            list_manifests_called: Arc::new(AtomicBool::new(false)),
            quarantine_blob_called: Arc::new(AtomicBool::new(false)),
            delete_blob_conditional_called: Arc::new(AtomicBool::new(false)),
            restore_quarantined_blob_called: Arc::new(AtomicBool::new(false)),
            fail_on_catalog_listing,
        }
    }
}

#[async_trait::async_trait]
impl RepositoryCatalogReader for RecordingGcServiceStoragePort {
    async fn list_repositories(&self) -> Result<Vec<String>, StorageError> {
        self.list_repositories_called.store(true, Ordering::SeqCst);
        if self.fail_on_catalog_listing {
            return Err(StorageError::backend(
                "fail_on_catalog_listing: unexpected catalog fallback invoked",
            ));
        }
        self.inner.list_repositories().await
    }

    async fn repo_timestamps(&self, name: &str) -> Result<RepoTimestamps, StorageError> {
        self.inner.repo_timestamps(name).await
    }
}

#[async_trait::async_trait]
impl TagReader for RecordingGcServiceStoragePort {
    async fn resolve_tag(&self, name: &str, tag: &str) -> Result<Digest, StorageError> {
        self.inner.resolve_tag(name, tag).await
    }

    async fn list_tags(&self, name: &str) -> Result<Vec<String>, StorageError> {
        self.inner.list_tags(name).await
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

    async fn get_tag_with_version(
        &self,
        repo: &str,
        tag: &str,
    ) -> Result<Option<(Digest, String)>, StorageError> {
        self.inner.get_tag_with_version(repo, tag).await
    }
}

#[async_trait::async_trait]
impl ManifestReader for RecordingGcServiceStoragePort {
    async fn head_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<ManifestMeta, StorageError> {
        self.inner.head_manifest(name, digest).await
    }

    async fn get_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<(ManifestMeta, bytes::Bytes), StorageError> {
        self.inner.get_manifest(name, digest).await
    }

    async fn list_manifest_digests_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<Digest>, Option<String>), StorageError> {
        self.list_manifests_called.store(true, Ordering::SeqCst);
        if self.fail_on_catalog_listing {
            return Err(StorageError::backend(
                "fail_on_catalog_listing: unexpected manifest listing invoked",
            ));
        }
        self.inner
            .list_manifest_digests_page(repo, continuation_token, page_limit)
            .await
    }
}

#[async_trait::async_trait]
impl RepositoryBlobMembershipStorage for RecordingGcServiceStoragePort {
    async fn link_repo_blob(&self, record: &RepoBlobMembershipRecord) -> Result<(), StorageError> {
        self.inner.link_repo_blob(record).await
    }

    async fn unlink_repo_blob(&self, repo: &str, digest: &Digest) -> Result<bool, StorageError> {
        self.inner.unlink_repo_blob(repo, digest).await
    }

    async fn get_repo_blob_membership(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<Option<RepoBlobMembershipRecord>, StorageError> {
        self.inner.get_repo_blob_membership(repo, digest).await
    }

    async fn list_repo_blob_memberships_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<RepoBlobMembershipRecord>, Option<String>), StorageError> {
        self.inner
            .list_repo_blob_memberships_page(repo, continuation_token, limit)
            .await
    }
}

#[async_trait::async_trait]
impl LifecycleJournalStore for RecordingGcServiceStoragePort {
    async fn read_lifecycle_journal(
        &self,
        repo: &str,
    ) -> Result<Option<bytes::Bytes>, StorageError> {
        self.inner.read_lifecycle_journal(repo).await
    }

    async fn write_lifecycle_journal(
        &self,
        repo: &str,
        data: bytes::Bytes,
    ) -> Result<(), StorageError> {
        self.inner.write_lifecycle_journal(repo, data).await
    }

    async fn delete_lifecycle_journal(&self, repo: &str) -> Result<(), StorageError> {
        self.inner.delete_lifecycle_journal(repo).await
    }
}

#[async_trait::async_trait]
impl GcStoragePort for RecordingGcServiceStoragePort {
    fn kind(&self) -> &'static str {
        self.inner.kind()
    }

    fn gc_strategy(&self) -> GcStorageStrategy {
        self.inner.gc_strategy()
    }

    async fn check_bucket_versioning_for_gc(&self) -> Result<(), StorageError> {
        self.inner.check_bucket_versioning_for_gc().await
    }

    async fn list_cas_blobs_page(
        &self,
        cursor: Option<&GcCursor>,
        limit: usize,
    ) -> Result<GcBlobPage, StorageError> {
        self.inner.list_cas_blobs_page(cursor, limit).await
    }

    async fn quarantine_blob(
        &self,
        permit: &GcMutationPermit<'_>,
        digest: &Digest,
        version: &BlobObjectVersion,
    ) -> Result<GcQuarantineResult, StorageError> {
        self.quarantine_blob_called.store(true, Ordering::SeqCst);
        self.inner.quarantine_blob(permit, digest, version).await
    }

    async fn restore_quarantined_blob(
        &self,
        permit: &GcMutationPermit<'_>,
        digest: &Digest,
    ) -> Result<Option<u64>, StorageError> {
        self.restore_quarantined_blob_called
            .store(true, Ordering::SeqCst);
        self.inner.restore_quarantined_blob(permit, digest).await
    }

    async fn quarantined_blob_version(
        &self,
        digest: &Digest,
    ) -> Result<Option<BlobObjectVersion>, StorageError> {
        self.inner.quarantined_blob_version(digest).await
    }

    async fn delete_blob_conditional(
        &self,
        permit: &GcMutationPermit<'_>,
        digest: &Digest,
        version: Option<&BlobObjectVersion>,
    ) -> Result<GcDeleteResult, StorageError> {
        self.delete_blob_conditional_called
            .store(true, Ordering::SeqCst);
        self.inner
            .delete_blob_conditional(permit, digest, version)
            .await
    }

    async fn discover_manifest_references(&self) -> Result<Option<HashSet<Digest>>, StorageError> {
        self.inner.discover_manifest_references().await
    }
}

// =================================================================================================
// 1. Capability Forwarding Through Actual Production Wiring & Arc
// =================================================================================================

#[tokio::test]
#[cfg(target_os = "linux")]
async fn test_capability_forwarding_through_production_wiring_and_arc() {
    let temp = tempfile::tempdir().unwrap();
    let fs_root = temp.path().to_path_buf();
    let manifests_dir = fs_root.join("repos").join("my-app").join("manifests");
    tokio::fs::create_dir_all(&manifests_dir).await.unwrap();

    let cfg_d = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    let layer_d = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
    let manifest_bytes = test_manifest_json(cfg_d, layer_d);
    let manifest_d = sha256_digest(&manifest_bytes);
    let manifest_hex = manifest_d.hex();
    tokio::fs::write(manifests_dir.join(manifest_hex), &manifest_bytes)
        .await
        .unwrap();

    let mut cfg = Config::from_env_with_files(&[]).unwrap();
    cfg.storage_backend = StorageBackend::Filesystem;
    cfg.fs_root = fs_root.clone();

    // Production wiring construction
    let wiring = registry_rust::storage_wiring::storage_wiring_try_from_config(&cfg)
        .expect("storage wiring must construct from config");
    let storage = wiring.gc_service_port();

    // Forwarding through Arc<dyn GcServiceStoragePort> -> discover_manifest_references
    let discovered = storage
        .discover_manifest_references()
        .await
        .expect("discover_manifest_references must succeed through storage wiring")
        .expect("FsStorage must return Some(set)");

    assert!(discovered.contains(&manifest_d));
    assert!(discovered.contains(&Digest::parse(cfg_d).unwrap()));
    assert!(discovered.contains(&Digest::parse(layer_d).unwrap()));
    assert_eq!(discovered.len(), 3);
}

// =================================================================================================
// 2. Some(empty), None, and Err Routing Asserting Fallback Avoidance
// =================================================================================================

#[tokio::test]
#[cfg(target_os = "linux")]
async fn test_routing_some_empty_proves_catalog_fallback_avoided() {
    let temp = tempfile::tempdir().unwrap();
    let fs_root = temp.path().to_path_buf();
    tokio::fs::create_dir_all(fs_root.join("repos"))
        .await
        .unwrap();

    let base_storage: Arc<dyn GcServiceStoragePort> =
        Arc::new(FsStorage::new(fs_root.clone(), 50 * 1024 * 1024));
    let recording = RecordingGcServiceStoragePort::new(base_storage, true);
    let cfg = Config::from_env_with_files(&[]).unwrap();

    // Directly calling discover_manifest_references returns Some(empty)
    let refs = recording
        .discover_manifest_references()
        .await
        .unwrap()
        .expect("must be Some");
    assert!(refs.is_empty());

    // build_manifest_protected_set must return empty set without triggering fail_on_catalog_listing
    let protected = build_manifest_protected_set(&recording)
        .await
        .expect("must succeed without calling catalog fallback");
    assert!(protected.is_empty());
    assert!(
        !recording.list_repositories_called.load(Ordering::SeqCst),
        "catalog listing must NOT be invoked when contained discovery returns Some(empty)"
    );
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn test_routing_err_fails_closed_proving_catalog_fallback_avoided() {
    let temp = tempfile::tempdir().unwrap();
    let fs_root = temp.path().to_path_buf();
    let manifests_dir = fs_root.join("repos").join("corrupt-app").join("manifests");
    tokio::fs::create_dir_all(&manifests_dir).await.unwrap();

    let corrupt_hex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    tokio::fs::write(manifests_dir.join(corrupt_hex), b"corrupt json")
        .await
        .unwrap();

    let base_storage: Arc<dyn GcServiceStoragePort> =
        Arc::new(FsStorage::new(fs_root.clone(), 50 * 1024 * 1024));
    let recording = RecordingGcServiceStoragePort::new(base_storage, true);
    let cfg = Config::from_env_with_files(&[]).unwrap();

    let err = build_manifest_protected_set(&recording).await.unwrap_err();

    match err {
        GcPolicyError::ManifestDiscovery(storage_err) => {
            assert_eq!(
                storage_err.internal_kind(),
                Some(StorageErrorKind::CorruptData),
                "corrupt manifest must fail closed with CorruptData storage error"
            );
        }
        other => panic!("expected GcPolicyError::ManifestDiscovery, got: {other:?}"),
    }

    assert!(
        !recording.list_repositories_called.load(Ordering::SeqCst),
        "catalog listing must NOT be invoked when contained discovery returns Err"
    );
}

#[tokio::test]
async fn test_routing_none_invokes_generic_traversal() {
    let mut cfg = Config::from_env_with_files(&[]).unwrap();
    cfg.storage_backend = StorageBackend::S3;
    cfg.s3_bucket = Some("test-bucket".to_string());

    let wiring = registry_rust::storage_wiring::storage_wiring_try_from_config(&cfg)
        .expect("s3 wiring must construct");
    let base_storage = wiring.gc_service_port();

    let direct_res = base_storage.discover_manifest_references().await.unwrap();
    assert!(
        direct_res.is_none(),
        "S3Storage backend must return Ok(None) to preserve generic catalog traversal"
    );

    let recording = RecordingGcServiceStoragePort::new(base_storage, false);
    // Calling build_manifest_protected_set with None should trigger generic catalog traversal
    let _ = build_manifest_protected_set(&recording).await;
    assert!(
        recording.list_repositories_called.load(Ordering::SeqCst),
        "generic traversal must invoke catalog listing when discover_manifest_references returns None"
    );
}

// =================================================================================================
// 3. Wrapper Forwarding: HookedStorage and LifecycleFaultStorage
// =================================================================================================

#[tokio::test]
#[cfg(target_os = "linux")]
async fn test_wrapper_forwarding_hooked_storage_gc_port() {
    let temp = tempfile::tempdir().unwrap();
    let fs_root = temp.path().to_path_buf();
    let manifests_dir = fs_root.join("repos").join("hooked-app").join("manifests");
    tokio::fs::create_dir_all(&manifests_dir).await.unwrap();

    let cfg_d = "sha256:5555555555555555555555555555555555555555555555555555555555555555";
    let layer_d = "sha256:6666666666666666666666666666666666666666666666666666666666666666";
    let manifest_bytes = test_manifest_json(cfg_d, layer_d);
    let manifest_d = sha256_digest(&manifest_bytes);
    tokio::fs::write(manifests_dir.join(manifest_d.hex()), &manifest_bytes)
        .await
        .unwrap();

    let fs_storage = Arc::new(FsStorage::new(fs_root.clone(), 50 * 1024 * 1024));
    let hooked = Arc::new(HookedStorage::new(fs_storage, StorageHooks::default()));
    let port: Arc<dyn GcServiceStoragePort> = hooked;

    let discovered = port
        .discover_manifest_references()
        .await
        .expect("HookedStorage must forward discover_manifest_references")
        .expect("must return Some(set)");

    assert!(discovered.contains(&manifest_d));
    assert_eq!(discovered.len(), 3);
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn test_wrapper_forwarding_lifecycle_fault_storage_gc_port() {
    let temp = tempfile::tempdir().unwrap();
    let fs_root = temp.path().to_path_buf();
    let manifests_dir = fs_root.join("repos").join("fault-app").join("manifests");
    tokio::fs::create_dir_all(&manifests_dir).await.unwrap();

    let cfg_d = "sha256:7777777777777777777777777777777777777777777777777777777777777777";
    let layer_d = "sha256:8888888888888888888888888888888888888888888888888888888888888888";
    let manifest_bytes = test_manifest_json(cfg_d, layer_d);
    let manifest_d = sha256_digest(&manifest_bytes);
    tokio::fs::write(manifests_dir.join(manifest_d.hex()), &manifest_bytes)
        .await
        .unwrap();

    let fs_storage = Arc::new(FsStorage::new(fs_root.clone(), 50 * 1024 * 1024));
    let fault_storage = Arc::new(LifecycleFaultStorage::new(fs_storage));
    let port: Arc<dyn GcServiceStoragePort> = fault_storage;

    let discovered = port
        .discover_manifest_references()
        .await
        .expect("LifecycleFaultStorage must forward discover_manifest_references")
        .expect("must return Some(set)");

    assert!(discovered.contains(&manifest_d));
    assert_eq!(discovered.len(), 3);
}

// =================================================================================================
// 4. Constructor Validation via Storage Wiring
// =================================================================================================

#[tokio::test]
#[cfg(target_os = "linux")]
async fn test_storage_wiring_constructor_validation_rejects_invalid_limits() {
    let temp = tempfile::tempdir().unwrap();
    let fs_root = temp.path().to_path_buf();

    let mut cfg = Config::from_env_with_files(&[]).unwrap();
    cfg.storage_backend = StorageBackend::Filesystem;
    cfg.fs_root = fs_root.clone();

    // Invalid max_depth = 0
    cfg.fs_gc_discovery_max_depth = 0;
    let err = match registry_rust::storage_wiring::storage_wiring_try_from_config(&cfg) {
        Err(e) => e,
        Ok(_) => panic!("should have failed with invalid max_depth"),
    };
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::Configuration));
    assert!(err.to_string().contains("max_depth"));

    // Reset and test invalid name bytes < 128
    cfg.fs_gc_discovery_max_depth = 32;
    cfg.fs_gc_discovery_terminal_dir_max_name_bytes = 64;
    let err = match registry_rust::storage_wiring::storage_wiring_try_from_config(&cfg) {
        Err(e) => e,
        Ok(_) => panic!("should have failed with invalid name bytes"),
    };
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::Configuration));
    assert!(err.to_string().contains("max_name_bytes"));
}

// =================================================================================================
// 5. Configured Limits Reaching Discovery and Reference Collection Independently (D-12)
// =================================================================================================

#[tokio::test]
#[cfg(target_os = "linux")]
async fn test_configured_limits_reaching_discovery_and_refs_independently() {
    let temp = tempfile::tempdir().unwrap();
    let fs_root = temp.path().to_path_buf();

    for repo_idx in 0..3 {
        let repo_dir = fs_root
            .join("repos")
            .join(format!("repo-{repo_idx}"))
            .join("manifests");
        tokio::fs::create_dir_all(&repo_dir).await.unwrap();

        let cfg_d = format!("sha256:{repo_idx:064x}");
        let layer_d = format!("sha256:{:064x}", repo_idx + 100);
        let manifest_bytes = test_manifest_json(&cfg_d, &layer_d);
        let manifest_d = sha256_digest(&manifest_bytes);
        tokio::fs::write(repo_dir.join(manifest_d.hex()), &manifest_bytes)
            .await
            .unwrap();
    }

    // A. Discovery budget exceeded: max_manifest_dirs = 2 (when 3 exist)
    let mut cfg_disc = Config::from_env_with_files(&[]).unwrap();
    cfg_disc.storage_backend = StorageBackend::Filesystem;
    cfg_disc.fs_root = fs_root.clone();
    cfg_disc.fs_gc_discovery_max_manifest_dirs = 2;

    let wiring_disc =
        registry_rust::storage_wiring::storage_wiring_try_from_config(&cfg_disc).unwrap();
    let storage_disc = wiring_disc.gc_service_port();

    let err = storage_disc
        .discover_manifest_references()
        .await
        .unwrap_err();
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::Backend));
    assert!(
        err.to_string()
            .contains("manifest directories limit reached"),
        "error should indicate discovery max_manifest_dirs exceeded: {err}"
    );

    // B. Terminal enumeration budget exceeded: max_terminal_dir_enumerations = 1 (when 3 exist)
    let mut cfg_ref = Config::from_env_with_files(&[]).unwrap();
    cfg_ref.storage_backend = StorageBackend::Filesystem;
    cfg_ref.fs_root = fs_root.clone();
    cfg_ref.fs_gc_discovery_max_terminal_dir_enumerations = 1;

    let wiring_ref =
        registry_rust::storage_wiring::storage_wiring_try_from_config(&cfg_ref).unwrap();
    let storage_ref = wiring_ref.gc_service_port();

    let err = storage_ref
        .discover_manifest_references()
        .await
        .unwrap_err();
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::Backend));
    assert!(
        err.to_string()
            .contains("terminal directory enumerations limit"),
        "error should indicate ref collection terminal_dir_enumerations exceeded: {err}"
    );
}

// =================================================================================================
// 6. Actual GC Planning, Quarantine, and Deletion Integration Tests with Candidate Fixtures
// =================================================================================================

#[tokio::test]
#[cfg(target_os = "linux")]
async fn test_actual_gc_planning_fails_closed_on_discovery_error_and_preserves_candidate() {
    let temp = tempfile::tempdir().unwrap();
    let fs_root = temp.path().to_path_buf();
    let ref_index_path = temp.path().join("ref-index");

    let base_storage: Arc<dyn GcServiceStoragePort> =
        Arc::new(FsStorage::new(fs_root.clone(), 50 * 1024 * 1024));
    let recording = Arc::new(RecordingGcServiceStoragePort::new(base_storage, false));
    let recording_port: Arc<dyn GcServiceStoragePort> = recording.clone();

    let mut cfg = Config::from_env_with_files(&[]).unwrap();
    cfg.fs_root = fs_root.clone();

    // Initialize healthy index
    let idx = Arc::new(BlobRefIndex::open(ref_index_path).unwrap());
    idx.ensure_healthy_or_rebuild(recording_port.as_ref(), true, true)
        .await
        .unwrap();

    // Create an eligible live blob candidate in CAS storage
    let candidate = sha256_digest(b"eligible-plan-candidate-data");
    write_live_blob(&fs_root, &candidate, b"eligible-plan-candidate-data").await;
    let candidate_path = fs_root
        .join("blobs")
        .join("sha256")
        .join(candidate.prefix2())
        .join(candidate.hex());
    assert!(candidate_path.exists());

    // Inject contained discovery failure via corrupt manifest
    let manifests_dir = fs_root.join("repos").join("bad-repo").join("manifests");
    tokio::fs::create_dir_all(&manifests_dir).await.unwrap();
    tokio::fs::write(
        manifests_dir.join("1111111111111111111111111111111111111111111111111111111111111111"),
        b"malformed json payload",
    )
    .await
    .unwrap();

    // Execute actual blob_gc_plan entry point
    let plan_res = blob_gc_plan(
        &recording_port,
        &idx,
        BlobGcPolicy::ManifestRooted,
        Duration::from_secs(0),
        BlobGcLimits::unlimited(100),
    )
    .await;

    // Assert precise error reached caller
    match plan_res {
        Err(BlobGcError::Policy(GcPolicyError::ManifestDiscovery(storage_err))) => {
            assert_eq!(
                storage_err.internal_kind(),
                Some(StorageErrorKind::CorruptData)
            );
        }
        other => panic!("expected ManifestDiscovery(CorruptData), got: {other:?}"),
    }

    // Assert candidate data remains where expected
    assert!(
        candidate_path.exists(),
        "live candidate blob must remain intact on planning failure"
    );
    assert!(
        !recording.quarantine_blob_called.load(Ordering::SeqCst),
        "quarantine must not be invoked during planning"
    );
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn test_actual_gc_quarantine_fails_closed_on_discovery_error_and_preserves_candidate() {
    let temp = tempfile::tempdir().unwrap();
    let fs_root = temp.path().to_path_buf();
    let ref_index_path = temp.path().join("ref-index");

    let raw_fs = Arc::new(FsStorage::new(fs_root.clone(), 50 * 1024 * 1024));
    let base_storage: Arc<dyn GcServiceStoragePort> = raw_fs.clone();
    let recording = Arc::new(RecordingGcServiceStoragePort::new(base_storage, false));
    let recording_port: Arc<dyn GcServiceStoragePort> = recording.clone();

    let mut cfg = Config::from_env_with_files(&[]).unwrap();
    cfg.fs_root = fs_root.clone();

    let idx = Arc::new(BlobRefIndex::open(ref_index_path).unwrap());
    idx.ensure_healthy_or_rebuild(recording_port.as_ref(), true, true)
        .await
        .unwrap();

    let consistency = ConsistencyCoordinator::new();
    let authority = RuntimeMutationAuthority::acquire(raw_fs.clone(), "test-quarantine-auth")
        .await
        .expect("authority must be acquired");

    // Write eligible live candidate
    let candidate = sha256_digest(b"eligible-quarantine-candidate-data");
    write_live_blob(&fs_root, &candidate, b"eligible-quarantine-candidate-data").await;
    let candidate_path = fs_root
        .join("blobs")
        .join("sha256")
        .join(candidate.prefix2())
        .join(candidate.hex());
    assert!(candidate_path.exists());

    // Inject contained discovery failure
    let manifests_dir = fs_root.join("repos").join("bad-repo").join("manifests");
    tokio::fs::create_dir_all(&manifests_dir).await.unwrap();
    tokio::fs::write(
        manifests_dir.join("2222222222222222222222222222222222222222222222222222222222222222"),
        b"malformed json",
    )
    .await
    .unwrap();

    // Execute actual blob_gc_quarantine_with_authority entry point
    let q_res = blob_gc_quarantine_with_authority(
        &recording_port,
        &idx,
        &consistency,
        &authority,
        BlobGcPolicy::ManifestRooted,
        Duration::from_secs(0),
        BlobGcLimits::unlimited(100),
    )
    .await;

    match q_res {
        Err(BlobGcError::Policy(GcPolicyError::ManifestDiscovery(storage_err))) => {
            assert_eq!(
                storage_err.internal_kind(),
                Some(StorageErrorKind::CorruptData)
            );
        }
        other => panic!("expected ManifestDiscovery(CorruptData), got: {other:?}"),
    }

    // Assert subsequent quarantine storage mutation was NOT called
    assert!(
        !recording.quarantine_blob_called.load(Ordering::SeqCst),
        "quarantine_blob must NOT be called when discovery fails"
    );
    assert!(
        candidate_path.exists(),
        "candidate must remain in live storage"
    );

    let quarantined_path = fs_root
        .join("quarantine")
        .join("blobs")
        .join("sha256")
        .join(candidate.prefix2())
        .join(candidate.hex());
    assert!(
        !quarantined_path.exists(),
        "candidate must NOT be moved to quarantine"
    );
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn test_actual_gc_deletion_fails_closed_on_discovery_error_and_preserves_quarantined_candidate()
 {
    let temp = tempfile::tempdir().unwrap();
    let fs_root = temp.path().to_path_buf();
    let ref_index_path = temp.path().join("ref-index");

    let raw_fs = Arc::new(FsStorage::new(fs_root.clone(), 50 * 1024 * 1024));
    let base_storage: Arc<dyn GcServiceStoragePort> = raw_fs.clone();
    let recording = Arc::new(RecordingGcServiceStoragePort::new(base_storage, false));
    let recording_port: Arc<dyn GcServiceStoragePort> = recording.clone();

    let mut cfg = Config::from_env_with_files(&[]).unwrap();
    cfg.fs_root = fs_root.clone();

    let idx = Arc::new(BlobRefIndex::open(ref_index_path).unwrap());
    idx.ensure_healthy_or_rebuild(recording_port.as_ref(), true, true)
        .await
        .unwrap();

    let consistency = ConsistencyCoordinator::new();
    let authority = RuntimeMutationAuthority::acquire(raw_fs.clone(), "test-delete-auth")
        .await
        .expect("authority");

    // Put a candidate directly in quarantine with expired timestamp metadata (age > 10s)
    let candidate = sha256_digest(b"expired-quarantined-candidate");
    let q_blob_path = fs_root
        .join("quarantine")
        .join("blobs")
        .join("sha256")
        .join(candidate.prefix2())
        .join(candidate.hex());
    tokio::fs::create_dir_all(q_blob_path.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&q_blob_path, b"expired-quarantined-candidate")
        .await
        .unwrap();

    let q_meta_path = fs_root
        .join("quarantine")
        .join("meta")
        .join("sha256")
        .join(candidate.prefix2())
        .join(format!("{}.ts", candidate.hex()));
    tokio::fs::create_dir_all(q_meta_path.parent().unwrap())
        .await
        .unwrap();
    let expired_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .saturating_sub(200);
    tokio::fs::write(&q_meta_path, format!("{expired_secs}\n"))
        .await
        .unwrap();

    // Inject contained discovery failure
    let manifests_dir = fs_root.join("repos").join("bad-repo").join("manifests");
    tokio::fs::create_dir_all(&manifests_dir).await.unwrap();
    tokio::fs::write(
        manifests_dir.join("3333333333333333333333333333333333333333333333333333333333333333"),
        b"malformed json",
    )
    .await
    .unwrap();

    // Execute actual blob_gc_delete_with_authority entry point
    let del_res = blob_gc_delete_with_authority(
        &cfg.fs_root,
        &recording_port,
        &idx,
        &consistency,
        &authority,
        BlobGcPolicy::ManifestRooted,
        Duration::from_secs(10),
        BlobGcLimits::unlimited(100),
    )
    .await;

    match del_res {
        Err(BlobGcError::Policy(GcPolicyError::ManifestDiscovery(storage_err))) => {
            assert_eq!(
                storage_err.internal_kind(),
                Some(StorageErrorKind::CorruptData)
            );
        }
        other => panic!("expected ManifestDiscovery(CorruptData), got: {other:?}"),
    }

    // Assert delete_blob_conditional and restore_quarantined_blob were NOT invoked
    assert!(
        !recording
            .delete_blob_conditional_called
            .load(Ordering::SeqCst),
        "delete_blob_conditional must NOT be called when discovery fails"
    );
    assert!(
        !recording
            .restore_quarantined_blob_called
            .load(Ordering::SeqCst),
        "restore_quarantined_blob must NOT be called when discovery fails"
    );

    // Candidate data and metadata remain where expected
    assert!(q_blob_path.exists(), "quarantined blob must remain on disk");
    assert!(
        q_meta_path.exists(),
        "quarantine metadata must remain on disk"
    );
}

// =================================================================================================
// 7. Real Timestamp Initialization via Deletion Loop and Preservation Across Failure
// =================================================================================================

#[tokio::test]
#[cfg(target_os = "linux")]
async fn test_real_deletion_loop_initializes_quarantine_timestamp_and_preserves_across_failure() {
    let temp = tempfile::tempdir().unwrap();
    let fs_root = temp.path().to_path_buf();
    let ref_index_path = temp.path().join("ref-index");

    let raw_fs = Arc::new(FsStorage::new(fs_root.clone(), 50 * 1024 * 1024));
    let storage: Arc<dyn GcServiceStoragePort> = raw_fs.clone();
    let mut cfg = Config::from_env_with_files(&[]).unwrap();
    cfg.fs_root = fs_root.clone();

    let idx = Arc::new(BlobRefIndex::open(ref_index_path).unwrap());
    idx.ensure_healthy_or_rebuild(storage.as_ref(), true, true)
        .await
        .unwrap();

    let consistency = ConsistencyCoordinator::new();
    let authority = RuntimeMutationAuthority::acquire(raw_fs.clone(), "test-ts-init-auth")
        .await
        .expect("authority");

    // Place candidate in quarantine with NO timestamp metadata
    let candidate = sha256_digest(b"quarantined-without-timestamp");
    let q_blob_path = fs_root
        .join("quarantine")
        .join("blobs")
        .join("sha256")
        .join(candidate.prefix2())
        .join(candidate.hex());
    tokio::fs::create_dir_all(q_blob_path.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&q_blob_path, b"quarantined-without-timestamp")
        .await
        .unwrap();

    let q_meta_path = fs_root
        .join("quarantine")
        .join("meta")
        .join("sha256")
        .join(candidate.prefix2())
        .join(format!("{}.ts", candidate.hex()));
    assert!(
        !q_meta_path.exists(),
        "timestamp file must NOT exist before deletion loop runs"
    );

    // Initial deletion run: clean repository state
    // The deletion loop discovers the un-timestamped blob, initializes timestamp, and continues.
    let del_stats = blob_gc_delete_with_authority(
        &cfg.fs_root,
        &storage,
        &idx,
        &consistency,
        &authority,
        BlobGcPolicy::ManifestRooted,
        Duration::from_secs(3600),
        BlobGcLimits::unlimited(100),
    )
    .await
    .expect("initial deletion pass must succeed");

    assert_eq!(
        del_stats.deleted_blobs, 0,
        "freshly timestamped blob must NOT be deleted in initial pass"
    );
    assert!(
        q_meta_path.exists(),
        "deletion loop must have created the quarantine timestamp metadata file"
    );

    let ts_content = tokio::fs::read_to_string(&q_meta_path).await.unwrap();
    let parsed_ts: u64 = ts_content
        .trim()
        .parse()
        .expect("valid unix seconds timestamp");
    assert!(parsed_ts > 1_700_000_000);

    // Subsequent pass: inject contained discovery failure
    let manifests_dir = fs_root.join("repos").join("bad-repo").join("manifests");
    tokio::fs::create_dir_all(&manifests_dir).await.unwrap();
    tokio::fs::write(
        manifests_dir.join("4444444444444444444444444444444444444444444444444444444444444444"),
        b"malformed json",
    )
    .await
    .unwrap();

    let failure_res = blob_gc_delete_with_authority(
        &cfg.fs_root,
        &storage,
        &idx,
        &consistency,
        &authority,
        BlobGcPolicy::ManifestRooted,
        Duration::from_secs(0),
        BlobGcLimits::unlimited(100),
    )
    .await;

    assert!(failure_res.is_err(), "subsequent pass must fail closed");

    // The timestamp metadata initialized during the real deletion run remains intact and observable
    let ts_content_after = tokio::fs::read_to_string(&q_meta_path).await.unwrap();
    assert_eq!(
        ts_content.trim(),
        ts_content_after.trim(),
        "quarantine timestamp metadata initialized by deletion loop must be preserved across later discovery failure"
    );
    assert!(q_blob_path.exists(), "quarantined blob must remain on disk");
}

// =================================================================================================
// 8. Pinned Discovery Inode Preserved Across Root Replacement
// =================================================================================================

#[tokio::test]
#[cfg(target_os = "linux")]
async fn test_pinned_discovery_inode_preserved_across_root_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let root_path = temp.path().join("fs_store");
    let manifests_dir = root_path.join("repos").join("app").join("manifests");
    tokio::fs::create_dir_all(&manifests_dir).await.unwrap();

    let cfg_d = "sha256:3333333333333333333333333333333333333333333333333333333333333333";
    let layer_d = "sha256:4444444444444444444444444444444444444444444444444444444444444444";
    let manifest_bytes = test_manifest_json(cfg_d, layer_d);
    let manifest_d = sha256_digest(&manifest_bytes);
    tokio::fs::write(manifests_dir.join(manifest_d.hex()), &manifest_bytes)
        .await
        .unwrap();

    // Construct FsStorage, which pins open the directory descriptor to root_path inode
    let storage: Arc<dyn GcServiceStoragePort> =
        Arc::new(FsStorage::new(root_path.clone(), 50 * 1024 * 1024));

    // Rename root_path to root_path_old, and create a new empty directory at root_path
    let root_old = temp.path().join("fs_store_old");
    std::fs::rename(&root_path, &root_old).unwrap();
    std::fs::create_dir_all(&root_path).unwrap();

    // discover_manifest_references on storage continues inspecting the pinned inode (root_old)
    let discovered = storage
        .discover_manifest_references()
        .await
        .expect("must succeed on pinned inode")
        .expect("must be Some");

    assert!(
        discovered.contains(&manifest_d),
        "pinned reader must discover manifests from original pinned root descriptor"
    );
}
