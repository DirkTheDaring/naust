#![allow(
    clippy::type_complexity,
    clippy::field_reassign_with_default,
    clippy::needless_borrows_for_generic_args
)]

mod support;

use axum::http::StatusCode;
use axum::http::header;
use axum::routing::get;
use bytes::Bytes;
use futures_util::StreamExt;
use registry_rust::app_state::AppState;
use registry_rust::application::blob::BlobMutationService;
use registry_rust::application::blob_read::BlobReadService;
use registry_rust::application::catalog::{CatalogQueryParams, CatalogQueryService};
use registry_rust::application::errors::{
    BlobMutationError, BlobReadError, ManifestMutationError, TagQueryError,
};
use registry_rust::application::manifest::ManifestMutationService;
use registry_rust::application::manifest_read::ManifestReadService;
use registry_rust::application::proxy::ProxyTarget;
use registry_rust::application::referrers::{ReferrersQueryParams, ReferrersQueryService};
use registry_rust::application::tags::{TagQueryParams, TagQueryService};
use registry_rust::blob_gc::BlobGcPolicy;
use registry_rust::blob_ref_index::BlobRefIndex;
use registry_rust::config::{
    AuthStrategy, EvictionPolicy, ProxyConfig, ProxyMode, ProxyRepoRule, RedirectPolicy, TagPolicy,
};
use registry_rust::consistency::ConsistencyCoordinator;
use registry_rust::gc_service::{GcBudgets, GcService};
use registry_rust::manifest_lifecycle::ProxyPublicationEvidence;
use registry_rust::proxy::{Proxy, ProxyRepoPattern};
use registry_rust::registry::canonical_name::CanonicalRepoName;
use registry_rust::registry::digest::Digest;
use registry_rust::storage::fs::FsStorage;
use registry_rust::storage::mutation_authority::RuntimeMutationAuthority;
use registry_rust::storage::ports::*;
use registry_rust::storage::repo_membership::RepoBlobMembershipRecord;
use registry_rust::storage::s3::S3Storage;
use registry_rust::storage::upload_session::UploadByteStream;
use registry_rust::storage::{
    BlobMeta, FinalizeOutcome, FinalizedReceipt, ReferrerDescriptor, StorageError,
    UploadTransitionError,
};
use registry_rust::upload_coordinator::BlobUploadCoordinatorConfig;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use support::gc_coordination::{HookedStorage, StorageHooks};
use tempfile::TempDir;
use uuid::Uuid;

fn sha256_digest(bytes: &[u8]) -> Digest {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let hex = hex::encode(hasher.finalize());
    Digest::parse(&format!("sha256:{hex}")).expect("valid sha256 digest")
}

#[allow(dead_code)]
struct TestServices {
    temp: TempDir,
    fs_root: PathBuf,
    wiring: StorageWiring,
    ref_index: Arc<BlobRefIndex>,
    consistency: ConsistencyCoordinator,
    blob_mutation: Arc<BlobMutationService>,
    manifest_mutation: Arc<ManifestMutationService>,
    blob_read: Arc<BlobReadService>,
    manifest_read: Arc<ManifestReadService>,
    catalog_query: Arc<CatalogQueryService>,
    tag_query: Arc<TagQueryService>,
    referrers_query: Arc<ReferrersQueryService>,
}

impl TestServices {
    async fn new_fs() -> Self {
        let temp = TempDir::new().unwrap();
        let fs_root = temp.path().join("root");
        let index_root = temp.path().join("index");
        std::fs::create_dir_all(&fs_root).unwrap();
        std::fs::create_dir_all(&index_root).unwrap();

        let backend = Arc::new(FsStorage::new(fs_root.clone(), 10 * 1024 * 1024));
        let wiring = StorageWiring::from_backend(backend.clone());
        let consistency = ConsistencyCoordinator::new();

        let ref_index = Arc::new(BlobRefIndex::open(index_root).unwrap());
        ref_index.rebuild(backend.as_ref()).await.unwrap();

        let blob_mutation = Arc::new(BlobMutationService::new(
            wiring.blob_mutation(),
            Some(ref_index.clone()),
            consistency.clone(),
            BlobUploadCoordinatorConfig::default(),
        ));
        let manifest_mutation = Arc::new(ManifestMutationService::new(
            wiring.manifest_lifecycle(),
            Some(ref_index.clone()),
            consistency.clone(),
        ));
        let blob_read = Arc::new(BlobReadService::new(
            wiring.blob_reader(),
            wiring.membership_reader(),
            blob_mutation.clone(),
        ));
        let manifest_read = Arc::new(ManifestReadService::new(
            wiring.manifest_reader(),
            wiring.tag_reader(),
            manifest_mutation.clone(),
            4 * 1024 * 1024,
            Some(Arc::new(tokio::sync::Semaphore::new(10))),
        ));
        let catalog_query = Arc::new(CatalogQueryService::new(
            wiring.catalog_reader(),
            wiring.tag_reader(),
            wiring.manifest_reader(),
            wiring.blob_reader(),
        ));
        let tag_query = Arc::new(TagQueryService::new(wiring.tag_reader()));
        let referrers_query = Arc::new(ReferrersQueryService::new(wiring.referrers_reader()));

        Self {
            temp,
            fs_root,
            wiring,
            ref_index,
            consistency,
            blob_mutation,
            manifest_mutation,
            blob_read,
            manifest_read,
            catalog_query,
            tag_query,
            referrers_query,
        }
    }

    async fn new_hooked_with_fs<F>(f: F) -> Self
    where
        F: FnOnce(&std::path::Path) -> StorageHooks,
    {
        let temp = TempDir::new().unwrap();
        let fs_root = temp.path().join("root");
        let index_root = temp.path().join("index");
        std::fs::create_dir_all(&fs_root).unwrap();
        std::fs::create_dir_all(&index_root).unwrap();

        let hooks = f(&fs_root);
        let base_storage = Arc::new(FsStorage::new(fs_root.clone(), 10 * 1024 * 1024));
        let hooked = Arc::new(HookedStorage::new(base_storage.clone(), hooks));
        let wiring = StorageWiring::from_backend(hooked);
        let consistency = ConsistencyCoordinator::new();

        let ref_index = Arc::new(BlobRefIndex::open(index_root).unwrap());
        ref_index.rebuild(base_storage.as_ref()).await.unwrap();

        let blob_mutation = Arc::new(BlobMutationService::new(
            wiring.blob_mutation(),
            Some(ref_index.clone()),
            consistency.clone(),
            BlobUploadCoordinatorConfig {
                signing_key: b"test-key".to_vec(),
                max_upload_bytes: 10 * 1024 * 1024,
                abort_on_digest_mismatch: true,
                disallow_monolithic_uploads: false,
                upload_chunk_min_bytes: None,
                gc_pin_duration_secs: 3600,
            },
        ));
        let manifest_mutation = Arc::new(ManifestMutationService::new(
            wiring.manifest_lifecycle(),
            Some(ref_index.clone()),
            consistency.clone(),
        ));
        let blob_read = Arc::new(BlobReadService::new(
            wiring.blob_reader(),
            wiring.membership_reader(),
            blob_mutation.clone(),
        ));
        let manifest_read = Arc::new(ManifestReadService::new(
            wiring.manifest_reader(),
            wiring.tag_reader(),
            manifest_mutation.clone(),
            4 * 1024 * 1024,
            Some(Arc::new(tokio::sync::Semaphore::new(10))),
        ));
        let catalog_query = Arc::new(CatalogQueryService::new(
            wiring.catalog_reader(),
            wiring.tag_reader(),
            wiring.manifest_reader(),
            wiring.blob_reader(),
        ));
        let tag_query = Arc::new(TagQueryService::new(wiring.tag_reader()));
        let referrers_query = Arc::new(ReferrersQueryService::new(wiring.referrers_reader()));

        Self {
            temp,
            fs_root,
            wiring,
            ref_index,
            consistency,
            blob_mutation,
            manifest_mutation,
            blob_read,
            manifest_read,
            catalog_query,
            tag_query,
            referrers_query,
        }
    }

    async fn create_gc_service(&self) -> GcService {
        let mut cfg = support::gc_coordination::test_config(
            self.fs_root.clone(),
            self.temp.path().join("index"),
        );
        cfg.blob_gc_enabled = true;
        cfg.blob_gc_schedule_enabled = true;

        let base_storage = Arc::new(FsStorage::new(self.fs_root.clone(), 10 * 1024 * 1024));
        let authority = RuntimeMutationAuthority::acquire(base_storage.clone(), "test-gc-worker")
            .await
            .expect("acquire authority");

        GcService::with_coordinator_and_authority(
            Arc::new(cfg),
            base_storage,
            self.ref_index.clone(),
            ConsistencyCoordinator::new(),
            Arc::new(tokio::sync::Mutex::new(Some(authority))),
        )
    }
}

struct HttpTestServer {
    base_url: String,
    services: TestServices,
    _handle: tokio::task::JoinHandle<()>,
}

impl HttpTestServer {
    async fn spawn(
        proxy_setup: Option<Box<dyn FnOnce(&TempDir) -> (Arc<Proxy>, ProxyConfig)>>,
    ) -> Self {
        let services = TestServices::new_fs().await;
        let fs_root = services.temp.path().join("root");
        let index_root = services.temp.path().join("index");
        let mut cfg = support::gc_coordination::test_config(fs_root.clone(), index_root);
        cfg.auth_strategy = AuthStrategy::Token;
        cfg.anonymous_pull = true;
        let (proxy, proxy_cache) = if let Some(setup) = proxy_setup {
            let (p, p_cfg) = setup(&services.temp);
            cfg.proxy = p_cfg;
            (Some(p), Some(services.wiring.proxy_storage()))
        } else {
            (None, None)
        };
        let cfg_arc = Arc::new(cfg);

        let storage = Arc::new(FsStorage::new(fs_root, 10 * 1024 * 1024));

        let app_state = AppState::new_test_with_proxy(
            cfg_arc,
            storage,
            Some(services.ref_index.clone()),
            proxy,
            proxy_cache,
        );

        let router = registry_rust::supervisor::build_router(app_state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        Self {
            base_url: format!("http://127.0.0.1:{port}"),
            services,
            _handle: handle,
        }
    }
}

async fn spawn_mock_upstream(app: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://127.0.0.1:{}", addr.port()), handle)
}

fn create_test_proxy_config(
    upstream_url: String,
    tag_policy: TagPolicy,
    temp: &TempDir,
) -> ProxyConfig {
    let index_path = temp.path().join(format!("proxy_index_{}", Uuid::new_v4()));
    ProxyConfig {
        enabled: true,
        mode: ProxyMode::Allowlist,
        upstream_base_url: Some(upstream_url),
        upstream_username: None,
        upstream_password: None,
        allowed_upstream_hosts: vec!["127.0.0.1".to_string()],
        allowed_repo_prefixes: vec![],
        block_private_networks: false,
        redirect_policy: RedirectPolicy::AnyPublic,
        max_concurrent_upstream: 10,
        index_path,
        cache_fs_root: None,
        cache_s3_prefix: None,
        gc_interval_secs: 3600,
        scrub_enabled: false,
        scrub_interval_secs: 3600,
        scrub_max_files_per_run: 0,
        max_cache_bytes: None,
        repo_rules: vec![ProxyRepoRule {
            match_pattern: ProxyRepoPattern::parse("*").unwrap(),
            upstream_repo: None,
            tag_policy,
            eviction_policy: EvictionPolicy::Default,
        }],
        upstreams: vec![],
        routing_proxy_hosts: vec![],
        routing_trust_x_forwarded_host: false,
    }
}

fn create_test_proxy(upstream_url: String, tag_policy: TagPolicy, temp: &TempDir) -> Arc<Proxy> {
    let cfg = create_test_proxy_config(upstream_url, tag_policy, temp);
    Arc::new(Proxy::new(&cfg).unwrap().unwrap())
}

// =================================================================================================
// 1. Blob Read Service Tests
// =================================================================================================

#[tokio::test]
async fn test_blob_read_linked_local_get_and_head() {
    let services = TestServices::new_fs().await;

    let payload = b"hello-linked-local-blob";
    let digest = sha256_digest(payload);
    let repo = "library/test";

    // 1. Create and commit blob
    let upload = services
        .wiring
        .blob_mutation()
        .create_upload()
        .await
        .unwrap();
    services
        .wiring
        .blob_mutation()
        .append_upload(&upload.uuid, Bytes::from_static(payload))
        .await
        .unwrap();
    services
        .wiring
        .blob_mutation()
        .finalize_upload(&upload.uuid, &digest)
        .await
        .unwrap();

    // 2. Link repository membership
    let canonical = CanonicalRepoName::parse(repo).unwrap();
    let rec = RepoBlobMembershipRecord::new_upload(canonical, digest.clone(), None);
    services
        .wiring
        .blob_mutation()
        .link_repo_blob(&rec)
        .await
        .unwrap();

    // 3. Head blob via read service
    let head = services
        .blob_read
        .head_blob(repo, &digest, None, false)
        .await
        .expect("head blob succeeds");
    assert_eq!(head.digest, digest);
    assert_eq!(head.size, payload.len() as u64);

    // 4. Get blob via read service
    let get = services
        .blob_read
        .get_blob(repo, &digest, None, false)
        .await
        .expect("get blob succeeds");
    assert_eq!(get.digest, digest);
    assert_eq!(get.size, payload.len() as u64);

    // Verify stream read
    let mut stream = get.stream;
    let mut read_bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        read_bytes.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(read_bytes, payload);
}

#[tokio::test]
async fn test_blob_read_unlinked_cas_blob_fails_closed() {
    let services = TestServices::new_fs().await;

    // Write CAS blob directly without repo membership
    let payload = b"unlinked-cas-payload";
    let digest = sha256_digest(payload);

    let upload = services
        .wiring
        .blob_mutation()
        .create_upload()
        .await
        .unwrap();
    services
        .wiring
        .blob_mutation()
        .append_upload(&upload.uuid, Bytes::from_static(payload))
        .await
        .unwrap();
    services
        .wiring
        .blob_mutation()
        .finalize_upload(&upload.uuid, &digest)
        .await
        .unwrap();

    // Head blob on repo without membership -> NotFound
    let head_res = services
        .blob_read
        .head_blob("other/repo", &digest, None, false)
        .await;
    assert!(
        matches!(head_res, Err(BlobReadError::NotFound)),
        "Head on unlinked blob must fail closed"
    );

    // Get blob on repo without membership -> NotFound
    let get_res = services
        .blob_read
        .get_blob("other/repo", &digest, None, false)
        .await;
    assert!(
        matches!(get_res, Err(BlobReadError::NotFound)),
        "Get on unlinked blob must fail closed"
    );
}

#[tokio::test]
async fn test_blob_read_proxy_miss_verified_publication() {
    let payload = b"proxy-upstream-blob-data";
    let digest = sha256_digest(payload);
    let digest_str = digest.to_string();

    let d_for_route = digest_str.clone();
    let app = axum::Router::new().route(
        "/v2/library/proxied/blobs/:digest",
        get(move |axum::extract::Path(d): axum::extract::Path<String>| {
            let p = payload;
            let expected_d = d_for_route.clone();
            async move {
                if d == expected_d {
                    (StatusCode::OK, Bytes::from_static(p))
                } else {
                    (StatusCode::NOT_FOUND, Bytes::new())
                }
            }
        }),
    );
    let (upstream_url, _handle) = spawn_mock_upstream(app).await;
    let services = TestServices::new_fs().await;
    let proxy = create_test_proxy(upstream_url, TagPolicy::TtlSeconds(3600), &services.temp);

    let proxy_target = ProxyTarget {
        proxy,
        cache_storage: services.wiring.proxy_storage(),
    };

    // First read triggers proxy fetch and verified publication
    let get_res = services
        .blob_read
        .get_blob("library/proxied", &digest, Some(&proxy_target), false)
        .await
        .expect("proxy read succeeds");
    assert_eq!(get_res.digest, digest);

    // Verify blob is now linked locally
    let mem = services
        .wiring
        .membership_reader()
        .get_repo_blob_membership("library/proxied", &digest)
        .await
        .unwrap();
    assert!(
        mem.is_some(),
        "Membership must be created after proxy ingestion"
    );
}

#[tokio::test]
async fn test_blob_read_proxy_digest_mismatch_fails_closed_zero_membership() {
    let wrong_payload = b"corrupted-blob-content";
    let declared_digest = sha256_digest(b"expected-original-content");

    let app = axum::Router::new().route(
        "/v2/library/corrupt/blobs/:digest",
        get(move || async move { (StatusCode::OK, Bytes::from_static(wrong_payload)) }),
    );
    let (upstream_url, _handle) = spawn_mock_upstream(app).await;
    let services = TestServices::new_fs().await;
    let proxy = create_test_proxy(upstream_url, TagPolicy::TtlSeconds(3600), &services.temp);

    let proxy_target = ProxyTarget {
        proxy,
        cache_storage: services.wiring.proxy_storage(),
    };

    let get_res = services
        .blob_read
        .get_blob(
            "library/corrupt",
            &declared_digest,
            Some(&proxy_target),
            false,
        )
        .await;

    assert!(
        matches!(get_res, Err(BlobReadError::NotFound)),
        "Digest mismatch on proxy blob fetch must fail closed with NotFound"
    );

    // Verify zero membership records exist
    let mem = services
        .wiring
        .membership_reader()
        .get_repo_blob_membership("library/corrupt", &declared_digest)
        .await
        .unwrap();
    assert!(mem.is_none(), "Zero membership records on digest mismatch");
}

#[tokio::test]
async fn test_blob_read_proxy_publication_failure_protection() {
    let services = TestServices::new_fs().await;

    // Calling publish_verified_proxy_blob directly with mismatched digest fails
    let payload = b"payload-for-mismatch";
    let wrong_digest = sha256_digest(b"completely-different-digest");
    let canonical = CanonicalRepoName::parse("library/protect-test").unwrap();

    let stream = futures_util::stream::once(async move { Ok(Bytes::from_static(payload)) });
    let pinned: UploadByteStream = Box::pin(stream);

    let err = services
        .blob_mutation
        .publish_verified_proxy_blob(&canonical, &wrong_digest, pinned)
        .await;

    assert!(
        err.is_err(),
        "Publication with mismatched digest must fail closed"
    );

    let mem = services
        .wiring
        .membership_reader()
        .get_repo_blob_membership("library/protect-test", &wrong_digest)
        .await
        .unwrap();
    assert!(mem.is_none(), "Zero membership on publication failure");
}

#[tokio::test]
async fn test_blob_read_duplicate_proxy_read_idempotent() {
    let payload = b"duplicate-read-payload";
    let digest = sha256_digest(payload);
    let digest_str = digest.to_string();

    let app = axum::Router::new().route(
        "/v2/library/dup/blobs/:digest",
        get(move |axum::extract::Path(d): axum::extract::Path<String>| {
            let p = payload;
            let exp = digest_str.clone();
            async move {
                if d == exp {
                    (StatusCode::OK, Bytes::from_static(p))
                } else {
                    (StatusCode::NOT_FOUND, Bytes::new())
                }
            }
        }),
    );
    let (upstream_url, _handle) = spawn_mock_upstream(app).await;
    let services = TestServices::new_fs().await;
    let proxy = create_test_proxy(upstream_url, TagPolicy::TtlSeconds(3600), &services.temp);

    let proxy_target = ProxyTarget {
        proxy,
        cache_storage: services.wiring.proxy_storage(),
    };

    // First read
    let res1 = services
        .blob_read
        .get_blob("library/dup", &digest, Some(&proxy_target), false)
        .await
        .unwrap();
    assert_eq!(res1.digest, digest);

    // Second read
    let res2 = services
        .blob_read
        .get_blob("library/dup", &digest, Some(&proxy_target), false)
        .await
        .unwrap();
    assert_eq!(res2.digest, digest);
}

#[tokio::test]
async fn test_blob_read_streaming_and_metadata() {
    let services = TestServices::new_fs().await;

    let payload = vec![0xABu8; 64 * 1024]; // 64 KiB
    let digest = sha256_digest(&payload);
    let repo = "library/stream-test";

    let upload = services
        .wiring
        .blob_mutation()
        .create_upload()
        .await
        .unwrap();
    services
        .wiring
        .blob_mutation()
        .append_upload(&upload.uuid, Bytes::copy_from_slice(&payload))
        .await
        .unwrap();
    services
        .wiring
        .blob_mutation()
        .finalize_upload(&upload.uuid, &digest)
        .await
        .unwrap();

    let canonical = CanonicalRepoName::parse(repo).unwrap();
    let rec = RepoBlobMembershipRecord::new_upload(canonical, digest.clone(), None);
    services
        .wiring
        .blob_mutation()
        .link_repo_blob(&rec)
        .await
        .unwrap();

    let res = services
        .blob_read
        .get_blob(repo, &digest, None, false)
        .await
        .unwrap();

    assert_eq!(res.size, 64 * 1024);
    assert_eq!(res.media_type, "application/octet-stream");

    let mut stream = res.stream;
    let mut total_read = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        total_read += chunk.len();
    }
    assert_eq!(total_read, 64 * 1024);
}

// =================================================================================================
// 2. Manifest Read Service Tests
// =================================================================================================

#[tokio::test]
async fn test_manifest_read_digest_get_and_head() {
    let services = TestServices::new_fs().await;

    let manifest_bytes = br#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:baaddb0000000000000000000000000000000000000000000000000000000001",
            "size": 2
        },
        "layers": []
    }"#;
    let digest = sha256_digest(manifest_bytes);
    let repo = "library/manifest-digest-test";

    // Put manifest into storage
    services
        .wiring
        .manifest_lifecycle()
        .put_manifest(repo, &digest, Bytes::from_static(manifest_bytes))
        .await
        .unwrap();

    // 1. Head manifest by digest
    let head = services
        .manifest_read
        .head_manifest(repo, &digest.as_str(), None, false, None)
        .await
        .expect("head manifest succeeds");
    assert_eq!(head.digest, digest);
    assert_eq!(head.size, manifest_bytes.len() as u64);
    assert_eq!(
        head.media_type,
        "application/vnd.oci.image.manifest.v1+json"
    );

    // 2. Get manifest by digest
    let get = services
        .manifest_read
        .get_manifest(repo, &digest.as_str(), None, false, None)
        .await
        .expect("get manifest succeeds");
    assert_eq!(get.digest, digest);
    assert_eq!(get.payload, Bytes::from_static(manifest_bytes));
}

#[tokio::test]
async fn test_manifest_read_tag_get_and_head_local_resolution() {
    let services = TestServices::new_fs().await;

    let manifest_bytes = br#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:baaddb0000000000000000000000000000000000000000000000000000000001",
            "size": 2
        },
        "layers": []
    }"#;
    let digest = sha256_digest(manifest_bytes);
    let repo = "library/manifest-tag-test";
    let tag = "v1.0.0";

    // Put manifest and set tag
    services
        .wiring
        .manifest_lifecycle()
        .put_manifest(repo, &digest, Bytes::from_static(manifest_bytes))
        .await
        .unwrap();
    services
        .wiring
        .manifest_lifecycle()
        .set_tag(repo, tag, &digest)
        .await
        .unwrap();

    // Head manifest by tag
    let head = services
        .manifest_read
        .head_manifest(repo, tag, None, false, None)
        .await
        .expect("head by tag succeeds");
    assert_eq!(head.digest, digest);

    // Get manifest by tag
    let get = services
        .manifest_read
        .get_manifest(repo, tag, None, false, None)
        .await
        .expect("get by tag succeeds");
    assert_eq!(get.digest, digest);
    assert_eq!(get.payload, Bytes::from_static(manifest_bytes));
}

#[tokio::test]
async fn test_manifest_read_proxy_cache_miss_and_verified_publication() {
    let manifest_bytes = Bytes::from_static(
        br#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:baaddb0000000000000000000000000000000000000000000000000000000001",
            "size": 2
        },
        "layers": []
    }"#,
    );
    let digest = sha256_digest(&manifest_bytes);

    let m_for_route = manifest_bytes.clone();
    let app = axum::Router::new().route(
        "/v2/library/proxied-manifest/manifests/latest",
        get(move || {
            let p = m_for_route.clone();
            async move {
                (
                    StatusCode::OK,
                    [(
                        header::CONTENT_TYPE,
                        "application/vnd.oci.image.manifest.v1+json",
                    )],
                    p,
                )
            }
        }),
    );
    let (upstream_url, _handle) = spawn_mock_upstream(app).await;
    let services = TestServices::new_fs().await;
    let proxy = create_test_proxy(upstream_url, TagPolicy::TtlSeconds(3600), &services.temp);

    let proxy_target = ProxyTarget {
        proxy,
        cache_storage: services.wiring.proxy_storage(),
    };

    let res = services
        .manifest_read
        .get_manifest(
            "library/proxied-manifest",
            "latest",
            Some(&proxy_target),
            false,
            None,
        )
        .await
        .expect("proxy manifest get succeeds");

    assert_eq!(res.digest, digest);

    // Verify tag exists in local storage after caching
    let local_tag = services
        .tag_query
        .resolve_tag("library/proxied-manifest", "latest", None)
        .await
        .unwrap();
    assert_eq!(local_tag, digest);
}

#[tokio::test]
async fn test_manifest_read_tag_fresh_ttl_avoids_revalidation() {
    let manifest_bytes = Bytes::from_static(
        br#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:baaddb0000000000000000000000000000000000000000000000000000000001",
            "size": 2
        },
        "layers": []
    }"#,
    );
    let digest = sha256_digest(&manifest_bytes);

    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let c = counter.clone();
    let m = manifest_bytes.clone();
    let app = axum::Router::new().route(
        "/v2/library/ttl-test/manifests/v1",
        get(move || {
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let p = m.clone();
            async move {
                (
                    StatusCode::OK,
                    [(
                        header::CONTENT_TYPE,
                        "application/vnd.oci.image.manifest.v1+json",
                    )],
                    p,
                )
            }
        }),
    );
    let (upstream_url, _handle) = spawn_mock_upstream(app).await;
    let services = TestServices::new_fs().await;
    let proxy = create_test_proxy(upstream_url, TagPolicy::TtlSeconds(3600), &services.temp);

    let proxy_target = ProxyTarget {
        proxy,
        cache_storage: services.wiring.proxy_storage(),
    };

    // First get fetches from upstream (counter becomes 1)
    let res1 = services
        .manifest_read
        .get_manifest("library/ttl-test", "v1", Some(&proxy_target), false, None)
        .await
        .unwrap();
    assert_eq!(res1.digest, digest);
    assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);

    // Second get within TTL must use local cache without upstream contact
    let res2 = services
        .manifest_read
        .get_manifest("library/ttl-test", "v1", Some(&proxy_target), false, None)
        .await
        .unwrap();
    assert_eq!(res2.digest, digest);
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "Upstream count must remain 1 due to fresh TTL"
    );
}

#[tokio::test]
async fn test_manifest_read_tag_expired_ttl_and_always_revalidate() {
    let manifest_bytes = Bytes::from_static(
        br#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:baaddb0000000000000000000000000000000000000000000000000000000001",
            "size": 2
        },
        "layers": []
    }"#,
    );

    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let c = counter.clone();
    let m = manifest_bytes.clone();
    let app = axum::Router::new().route(
        "/v2/library/reval-test/manifests/latest",
        get(move || {
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let p = m.clone();
            async move {
                (
                    StatusCode::OK,
                    [(
                        header::CONTENT_TYPE,
                        "application/vnd.oci.image.manifest.v1+json",
                    )],
                    p,
                )
            }
        }),
    );
    let (upstream_url, _handle) = spawn_mock_upstream(app).await;
    let services = TestServices::new_fs().await;
    let proxy = create_test_proxy(upstream_url, TagPolicy::AlwaysRevalidate, &services.temp);

    let proxy_target = ProxyTarget {
        proxy,
        cache_storage: services.wiring.proxy_storage(),
    };

    // First get
    services
        .manifest_read
        .get_manifest(
            "library/reval-test",
            "latest",
            Some(&proxy_target),
            false,
            None,
        )
        .await
        .unwrap();
    assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);

    // Second get under AlwaysRevalidate revalidates upstream
    services
        .manifest_read
        .get_manifest(
            "library/reval-test",
            "latest",
            Some(&proxy_target),
            false,
            None,
        )
        .await
        .unwrap();
    assert!(counter.load(std::sync::atomic::Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn test_manifest_read_digest_mismatch_fails_closed() {
    let wrong_manifest = Bytes::from_static(b"{\"schemaVersion\": 2}");
    let wrong_digest = sha256_digest(b"completely-different-expected-payload");

    let app = axum::Router::new().route(
        "/v2/mismatch-repo/manifests/:reference",
        get(move || {
            let p = wrong_manifest.clone();
            async move { (StatusCode::OK, p) }
        }),
    );
    let (upstream_url, _handle) = spawn_mock_upstream(app).await;
    let services = TestServices::new_fs().await;
    let proxy = create_test_proxy(upstream_url, TagPolicy::TtlSeconds(3600), &services.temp);

    let proxy_target = ProxyTarget {
        proxy,
        cache_storage: services.wiring.proxy_storage(),
    };

    let res = services
        .manifest_read
        .get_manifest(
            "mismatch-repo",
            &wrong_digest.as_str(),
            Some(&proxy_target),
            false,
            None,
        )
        .await;
    assert!(
        res.is_err(),
        "Digest mismatch on manifest read must fail closed"
    );
}

#[tokio::test]
async fn test_manifest_read_invalid_manifest_and_size_limit_rejection() {
    let invalid_bytes = Bytes::from_static(b"not-a-valid-json-manifest");

    let app = axum::Router::new().route(
        "/v2/invalid-repo/manifests/v1.0.0",
        get(move || {
            let p = invalid_bytes.clone();
            async move { (StatusCode::OK, p) }
        }),
    );
    let (upstream_url, _handle) = spawn_mock_upstream(app).await;
    let services = TestServices::new_fs().await;
    let proxy = create_test_proxy(upstream_url, TagPolicy::TtlSeconds(3600), &services.temp);

    let proxy_target = ProxyTarget {
        proxy,
        cache_storage: services.wiring.proxy_storage(),
    };

    let res = services
        .manifest_read
        .get_manifest("invalid-repo", "v1.0.0", Some(&proxy_target), false, None)
        .await;
    assert!(
        res.is_err(),
        "Invalid manifest format from upstream must fail closed"
    );
}

#[tokio::test]
async fn test_manifest_read_exact_digest_media_type_and_subject_metadata() {
    let services = TestServices::new_fs().await;

    let subject_digest = sha256_digest(b"subject-base");
    let artifact_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "artifactType": "application/vnd.example.sbom.v1",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:baaddb0000000000000000000000000000000000000000000000000000000001",
            "size": 2
        },
        "layers": [],
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": subject_digest.to_string(),
            "size": 1234
        }
    });
    let payload = Bytes::from(serde_json::to_vec(&artifact_json).unwrap());
    let digest = sha256_digest(&payload);
    let repo = "library/artifact-test";

    services
        .wiring
        .manifest_lifecycle()
        .put_manifest(repo, &digest, payload.clone())
        .await
        .unwrap();

    let head = services
        .manifest_read
        .head_manifest(repo, &digest.as_str(), None, false, None)
        .await
        .unwrap();

    assert_eq!(head.digest, digest);
    assert_eq!(head.subject, Some(subject_digest.clone()));

    let get = services
        .manifest_read
        .get_manifest(repo, &digest.as_str(), None, false, None)
        .await
        .unwrap();
    assert_eq!(get.digest, digest);
    assert_eq!(get.subject, Some(subject_digest));
}

// =================================================================================================
// 3. Catalog, Tag, and Referrers Query Service Tests
// =================================================================================================

#[tokio::test]
async fn test_catalog_query_sorting_bounds_and_cursors() {
    let services = TestServices::new_fs().await;

    let dummy_digest = sha256_digest(b"dummy");
    let dummy_manifest = Bytes::from_static(b"{}");

    // Populate repos in non-alphabetical order
    let repos = vec![
        "org/gamma",
        "org/alpha",
        "org/beta",
        "org/delta",
        "library/zebra",
        "library/apple",
    ];

    for r in &repos {
        services
            .wiring
            .manifest_lifecycle()
            .put_manifest(r, &dummy_digest, dummy_manifest.clone())
            .await
            .unwrap();
    }

    // 1. Full catalog listing (sorted)
    let full = services
        .catalog_query
        .query_catalog(CatalogQueryParams::default(), None)
        .await
        .unwrap();
    assert_eq!(
        full.repositories,
        vec![
            "library/apple",
            "library/zebra",
            "org/alpha",
            "org/beta",
            "org/delta",
            "org/gamma"
        ]
    );
    assert!(!full.has_more);
    assert_eq!(full.next_last, None);

    // 2. Pagination with n=2
    let page1 = services
        .catalog_query
        .query_catalog(
            CatalogQueryParams {
                n: Some(2),
                last: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(page1.repositories, vec!["library/apple", "library/zebra"]);
    assert!(page1.has_more);
    assert_eq!(page1.next_last.as_deref(), Some("library/zebra"));

    // 3. Page 2 with last cursor
    let page2 = services
        .catalog_query
        .query_catalog(
            CatalogQueryParams {
                n: Some(2),
                last: page1.next_last,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(page2.repositories, vec!["org/alpha", "org/beta"]);
    assert!(page2.has_more);
    assert_eq!(page2.next_last.as_deref(), Some("org/beta"));
}

#[tokio::test]
async fn test_catalog_query_edge_cases_omitted_zero_max_nonexistent_empty() {
    let services = TestServices::new_fs().await;

    // 1. Empty catalog
    let empty = services
        .catalog_query
        .query_catalog(CatalogQueryParams::default(), None)
        .await
        .unwrap();
    assert!(empty.repositories.is_empty());
    assert!(!empty.has_more);

    // Populate
    let dummy_digest = sha256_digest(b"dummy");
    let dummy_manifest = Bytes::from_static(b"{}");
    for r in &["repo-a", "repo-b", "repo-c"] {
        services
            .wiring
            .manifest_lifecycle()
            .put_manifest(r, &dummy_digest, dummy_manifest.clone())
            .await
            .unwrap();
    }

    // 2. Omitted n -> all repos
    let all = services
        .catalog_query
        .query_catalog(
            CatalogQueryParams {
                n: None,
                last: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(all.repositories.len(), 3);
    assert!(!all.has_more);

    // 3. n=0 -> 0 repos
    let zero = services
        .catalog_query
        .query_catalog(
            CatalogQueryParams {
                n: Some(0),
                last: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(zero.repositories.len(), 0);

    // 4. Cursor past the end
    let past_end = services
        .catalog_query
        .query_catalog(
            CatalogQueryParams {
                n: Some(10),
                last: Some("zzzzzz".to_string()),
            },
            None,
        )
        .await
        .unwrap();
    assert!(past_end.repositories.is_empty());
    assert!(!past_end.has_more);

    // 5. Non-matching last cursor positions at next alphabetical item
    let mid = services
        .catalog_query
        .query_catalog(
            CatalogQueryParams {
                n: Some(10),
                last: Some("repo-aa".to_string()),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(mid.repositories, vec!["repo-b", "repo-c"]);
}

#[tokio::test]
async fn test_tag_query_sorting_bounds_and_cursors() {
    let services = TestServices::new_fs().await;
    let dummy_digest = sha256_digest(b"dummy");
    let repo = "library/tags-test";

    let tags = vec!["v1.0.0", "v0.9.0", "v2.0.0", "latest", "beta"];
    for t in &tags {
        services
            .wiring
            .manifest_lifecycle()
            .set_tag(repo, t, &dummy_digest)
            .await
            .unwrap();
    }

    // 1. Full tags list (sorted)
    let full = services
        .tag_query
        .query_tags(repo, TagQueryParams::default(), None)
        .await
        .unwrap();
    assert_eq!(
        full.tags,
        vec!["beta", "latest", "v0.9.0", "v1.0.0", "v2.0.0"]
    );
    assert!(!full.has_more);

    // 2. Pagination with n=2
    let page1 = services
        .tag_query
        .query_tags(
            repo,
            TagQueryParams {
                n: Some(2),
                last: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(page1.tags, vec!["beta", "latest"]);
    assert!(page1.has_more);
    assert_eq!(page1.next_last.as_deref(), Some("latest"));

    // 3. Page 2 with last
    let page2 = services
        .tag_query
        .query_tags(
            repo,
            TagQueryParams {
                n: Some(2),
                last: page1.next_last,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(page2.tags, vec!["v0.9.0", "v1.0.0"]);
    assert!(page2.has_more);
    assert_eq!(page2.next_last.as_deref(), Some("v1.0.0"));
}

#[tokio::test]
async fn test_tag_query_edge_cases_omitted_zero_max_nonexistent_empty() {
    let services = TestServices::new_fs().await;
    let repo = "library/tag-edge-cases";

    // 1. Nonexistent repository returns NotFound
    let non_existent = services
        .tag_query
        .query_tags("library/nonexistent", TagQueryParams::default(), None)
        .await;
    assert!(matches!(non_existent, Err(TagQueryError::NotFound)));

    // Populate tags
    let dummy_digest = sha256_digest(b"dummy");
    for tag in &["t1", "t2", "t3"] {
        services
            .wiring
            .manifest_lifecycle()
            .set_tag(repo, tag, &dummy_digest)
            .await
            .unwrap();
    }

    // 2. Omitted n -> all tags
    let all = services
        .tag_query
        .query_tags(
            repo,
            TagQueryParams {
                n: None,
                last: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(all.tags.len(), 3);
    assert!(!all.has_more);

    // 3. n=0 -> 0 tags
    let zero = services
        .tag_query
        .query_tags(
            repo,
            TagQueryParams {
                n: Some(0),
                last: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(zero.tags.len(), 0);

    // 4. Nonexistent last cursor
    let past = services
        .tag_query
        .query_tags(
            repo,
            TagQueryParams {
                n: Some(10),
                last: Some("zzzz".to_string()),
            },
            None,
        )
        .await
        .unwrap();
    assert!(past.tags.is_empty());
}

#[tokio::test]
async fn test_referrers_query_filtering_sorting_and_cursors() {
    let services = TestServices::new_fs().await;
    let subject_digest = sha256_digest(b"subject-manifest");
    let repo = "library/referrers-test";

    // Create 3 referrers
    let r1 = ReferrerDescriptor {
        media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
        digest: sha256_digest(b"ref-1").to_string(),
        size: 100,
        artifact_type: Some("application/vnd.example.sbom".to_string()),
        annotations: Default::default(),
    };
    let r2 = ReferrerDescriptor {
        media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
        digest: sha256_digest(b"ref-2").to_string(),
        size: 200,
        artifact_type: Some("application/vnd.example.sig".to_string()),
        annotations: Default::default(),
    };
    let r3 = ReferrerDescriptor {
        media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
        digest: sha256_digest(b"ref-3").to_string(),
        size: 300,
        artifact_type: Some("application/vnd.example.sbom".to_string()),
        annotations: Default::default(),
    };

    for r in [&r1, &r2, &r3] {
        services
            .wiring
            .manifest_lifecycle()
            .add_referrer(repo, &subject_digest, (*r).clone())
            .await
            .unwrap();
    }

    // 1. Query all referrers (sorted by digest)
    let all = services
        .referrers_query
        .query_referrers(repo, &subject_digest, ReferrersQueryParams::default(), None)
        .await
        .unwrap();
    assert_eq!(all.descriptors.len(), 3);
    assert!(!all.has_more);

    // 2. Filter by artifactType
    let sbom_only = services
        .referrers_query
        .query_referrers(
            repo,
            &subject_digest,
            ReferrersQueryParams {
                artifact_type: Some("application/vnd.example.sbom".to_string()),
                n: None,
                last: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(sbom_only.descriptors.len(), 2);
    for desc in &sbom_only.descriptors {
        assert_eq!(
            desc.artifact_type.as_deref(),
            Some("application/vnd.example.sbom")
        );
    }

    // 3. Pagination with n=1
    let p1 = services
        .referrers_query
        .query_referrers(
            repo,
            &subject_digest,
            ReferrersQueryParams {
                artifact_type: None,
                n: Some(1),
                last: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(p1.descriptors.len(), 1);
    assert!(p1.has_more);
    assert!(p1.next_last.is_some());
}

#[tokio::test]
async fn test_referrers_query_edge_cases_omitted_zero_max_nonexistent_empty() {
    let services = TestServices::new_fs().await;
    let subject = sha256_digest(b"nonexistent-subject");
    let repo = "library/referrers-edge";

    // 1. Empty referrers list
    let empty = services
        .referrers_query
        .query_referrers(repo, &subject, ReferrersQueryParams::default(), None)
        .await
        .unwrap();
    assert!(empty.descriptors.is_empty());
    assert!(!empty.has_more);

    // Populate
    let r1 = ReferrerDescriptor {
        media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
        digest: sha256_digest(b"ref-edge-1").to_string(),
        size: 100,
        artifact_type: Some("application/vnd.custom".to_string()),
        annotations: Default::default(),
    };
    services
        .wiring
        .manifest_lifecycle()
        .add_referrer(repo, &subject, r1)
        .await
        .unwrap();

    // 2. n=0
    let zero = services
        .referrers_query
        .query_referrers(
            repo,
            &subject,
            ReferrersQueryParams {
                artifact_type: None,
                n: Some(0),
                last: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(zero.descriptors.len(), 0);

    // 3. Artifact type mismatch
    let no_match = services
        .referrers_query
        .query_referrers(
            repo,
            &subject,
            ReferrersQueryParams {
                artifact_type: Some("nonexistent/type".to_string()),
                n: None,
                last: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(no_match.descriptors.is_empty());
}

#[tokio::test]
async fn test_catalog_detailed_tag_platforms_for_repo() {
    let services = TestServices::new_fs().await;
    let repo = "library/platform-test";

    // Create config blob
    let config_bytes = br#"{"architecture":"amd64","os":"linux"}"#;
    let config_digest = sha256_digest(config_bytes);

    let upload = services
        .wiring
        .blob_mutation()
        .create_upload()
        .await
        .unwrap();
    services
        .wiring
        .blob_mutation()
        .append_upload(&upload.uuid, Bytes::from_static(config_bytes))
        .await
        .unwrap();
    services
        .wiring
        .blob_mutation()
        .finalize_upload(&upload.uuid, &config_digest)
        .await
        .unwrap();

    let canonical = CanonicalRepoName::parse(repo).unwrap();
    services
        .wiring
        .blob_mutation()
        .link_repo_blob(&RepoBlobMembershipRecord::new_upload(
            canonical,
            config_digest.clone(),
            None,
        ))
        .await
        .unwrap();

    // Create manifest referencing this config
    let manifest_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config_digest.to_string(),
            "size": config_bytes.len()
        },
        "layers": []
    });
    let manifest_bytes = Bytes::from(serde_json::to_vec(&manifest_json).unwrap());
    let m_digest = sha256_digest(&manifest_bytes);

    services
        .wiring
        .manifest_lifecycle()
        .put_manifest(repo, &m_digest, manifest_bytes)
        .await
        .unwrap();
    services
        .wiring
        .manifest_lifecycle()
        .set_tag(repo, "linux-amd64", &m_digest)
        .await
        .unwrap();

    // Query detailed platform tags
    let detailed = services
        .catalog_query
        .tag_platforms_for_repo(repo, "linux-amd64", None)
        .await
        .unwrap();
    assert_eq!(detailed["tag"].as_str().unwrap(), "linux-amd64");
    assert_eq!(detailed["digest"].as_str().unwrap(), m_digest.as_str());
    assert_eq!(detailed["platforms"][0].as_str().unwrap(), "linux/amd64");
}

// =================================================================================================
// 4. Deterministic Proxy Blob Publication Window & Invariants
// =================================================================================================

#[tokio::test]
async fn test_proxy_blob_publication_single_high_level_use_case() {
    let services = TestServices::new_fs().await;

    let payload = b"coordinator-proxy-blob-data";
    let digest = sha256_digest(payload);
    let canonical = CanonicalRepoName::parse("library/single-usecase").unwrap();

    let stream = futures_util::stream::once(async move { Ok(Bytes::from_static(payload)) });
    let pinned: UploadByteStream = Box::pin(stream);

    // One high-level use case invocation
    services
        .blob_mutation
        .publish_verified_proxy_blob(&canonical, &digest, pinned)
        .await
        .expect("publish_verified_proxy_blob succeeds");

    // Both CAS and membership are now present
    let head = services
        .wiring
        .blob_reader()
        .head_blob(&digest)
        .await
        .expect("CAS blob present");
    assert_eq!(head.size, payload.len() as u64);

    let mem = services
        .wiring
        .membership_reader()
        .get_repo_blob_membership("library/single-usecase", &digest)
        .await
        .expect("membership query succeeds");
    assert!(mem.is_some(), "Membership record must be present");
}

#[tokio::test]
async fn test_proxy_blob_publication_barrier_paused_after_cas_before_membership_gc_safe() {
    let (in_barrier_tx, in_barrier_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();

    let in_tx = Arc::new(tokio::sync::Mutex::new(Some(in_barrier_tx)));
    let res_rx = Arc::new(tokio::sync::Mutex::new(Some(resume_rx)));

    let services = TestServices::new_hooked_with_fs(move |fs_root| {
        let in_tx = in_tx.clone();
        let res_rx = res_rx.clone();
        let fs_root = fs_root.to_path_buf();
        let mut hooks = StorageHooks::default();
        hooks.custom_commit_finalize = Some(Arc::new(move |inner, prepared| {
            let in_tx = in_tx.clone();
            let res_rx = res_rx.clone();
            let prepared = prepared.clone();
            let fs_root = fs_root.clone();
            Box::pin(async move {
                // STEP 1: Rename staged file into CAS blob storage
                let data_path = fs_root
                    .join("uploads")
                    .join(format!("{}.data", prepared.session.uuid));
                let dest_dir = fs_root
                    .join("blobs")
                    .join(prepared.expected_digest.algorithm())
                    .join(prepared.expected_digest.prefix2());
                tokio::fs::create_dir_all(&dest_dir).await.map_err(|e| {
                    UploadTransitionError::Storage(StorageError::Internal(e.to_string()))
                })?;
                let dest_path = dest_dir.join(prepared.expected_digest.hex());
                tokio::fs::rename(&data_path, &dest_path)
                    .await
                    .map_err(|e| {
                        UploadTransitionError::Storage(StorageError::Internal(e.to_string()))
                    })?;

                // STEP 2: Signal barrier reached (CAS exists, membership does NOT exist)
                if let Some(tx) = in_tx.lock().await.take() {
                    let _ = tx.send(());
                }
                if let Some(rx) = res_rx.lock().await.take() {
                    let _ = rx.await;
                }

                // STEP 3: Create repository membership
                let membership = RepoBlobMembershipRecord::new_upload(
                    prepared.session.repo.clone(),
                    prepared.expected_digest.clone(),
                    Some(prepared.session.uuid.clone()),
                );
                inner
                    .link_repo_blob(&membership)
                    .await
                    .map_err(UploadTransitionError::Storage)?;

                // STEP 4: Finalized receipt
                let fin_dir = fs_root.join("finalized");
                tokio::fs::create_dir_all(&fin_dir).await.map_err(|e| {
                    UploadTransitionError::Storage(StorageError::Internal(e.to_string()))
                })?;
                let receipt_path = fin_dir.join(format!("{}.json", prepared.session.uuid));
                let receipt = FinalizedReceipt {
                    repo: prepared.session.repo.clone(),
                    uuid: prepared.session.uuid.clone(),
                    digest: prepared.expected_digest.to_string(),
                    size: prepared.size,
                    finalized_at_unix_secs: 1000,
                    format_version: 1,
                };
                tokio::fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap())
                    .await
                    .map_err(|e| {
                        UploadTransitionError::Storage(StorageError::Internal(e.to_string()))
                    })?;

                Ok(FinalizeOutcome::Published(BlobMeta {
                    size: prepared.size,
                }))
            })
        }));
        hooks
    })
    .await;

    let payload = b"paused-barrier-proxy-blob-data";
    let digest = sha256_digest(payload);
    let canonical = CanonicalRepoName::parse("library/barrier-repo").unwrap();

    let stream = futures_util::stream::once(async move { Ok(Bytes::from_static(payload)) });
    let pinned: UploadByteStream = Box::pin(stream);

    let blob_mutation = services.blob_mutation.clone();
    let canonical_clone = canonical.clone();
    let digest_clone = digest.clone();
    let pub_task = tokio::spawn(async move {
        blob_mutation
            .publish_verified_proxy_blob(&canonical_clone, &digest_clone, pinned)
            .await
    });

    // 1. Wait until publication reaches the exact barrier between CAS write and membership creation
    in_barrier_rx.await.expect("reached publication barrier");

    // 2. Exact state at the barrier:
    // a) CAS blob exists in storage
    let cas_head = services.wiring.blob_reader().head_blob(&digest).await;
    assert!(
        cas_head.is_ok(),
        "CAS blob must be present in storage at barrier"
    );

    // b) Repository membership does NOT yet exist
    let mem_at_barrier = services
        .wiring
        .membership_reader()
        .get_repo_blob_membership("library/barrier-repo", &digest)
        .await
        .unwrap();
    assert!(
        mem_at_barrier.is_none(),
        "Repository membership must NOT exist before link_repo_blob"
    );

    // c) Durable publication pin is active in BlobRefIndex
    let is_pinned = services
        .ref_index
        .is_blob_pinned(&digest, SystemTime::now())
        .unwrap();
    assert!(
        is_pinned,
        "BlobRefIndex durable publication pin MUST be active at the barrier"
    );

    // d) Real production GC sweep executed using candidate, quarantine, and deletion path
    let gc_service = services.create_gc_service().await;
    let budgets = GcBudgets {
        max_blobs: 1000,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };
    let q_stats = gc_service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("production quarantine sweep succeeds");

    // e) The sweep reports the pinned blob as protected / skipped
    assert_eq!(
        q_stats.quarantined_blobs, 0,
        "Production GC quarantine sweep must skip the active publication-pinned blob"
    );

    // f) No quarantine object is created on disk
    let q_path = services
        .fs_root
        .join("quarantine")
        .join("blobs")
        .join(digest.algorithm())
        .join(digest.prefix2())
        .join(digest.hex());
    assert!(
        !q_path.exists(),
        "No quarantine file may be created for pinned candidate blob"
    );

    // g) CAS storage still contains the untouched live blob
    let cas_head_after_gc = services.wiring.blob_reader().head_blob(&digest).await;
    assert!(
        cas_head_after_gc.is_ok(),
        "CAS blob must remain intact in storage during and after GC sweep"
    );

    // 3. Resume publication
    let _ = resume_tx.send(());
    let res = pub_task.await.unwrap();
    assert!(
        res.is_ok(),
        "Publication completes successfully after barrier"
    );

    // 4. Final state: membership is durable and pin is cleanly released
    let mem_final = services
        .wiring
        .membership_reader()
        .get_repo_blob_membership("library/barrier-repo", &digest)
        .await
        .unwrap();
    assert!(
        mem_final.is_some(),
        "Membership is durable after publication completes"
    );

    let is_pinned_final = services
        .ref_index
        .is_blob_pinned(&digest, SystemTime::now())
        .unwrap();
    assert!(
        !is_pinned_final,
        "Publication pin is released after membership becomes durable"
    );
}

#[tokio::test]
async fn test_proxy_blob_publication_injected_membership_failure_typed_error_pin_and_recovery() {
    let services = TestServices::new_hooked_with_fs(move |fs_root| {
        let fs_root = fs_root.to_path_buf();
        let mut hooks = StorageHooks::default();
        hooks.custom_commit_finalize = Some(Arc::new(move |_inner, prepared| {
            let fs_root = fs_root.clone();
            let prepared = prepared.clone();
            Box::pin(async move {
                // Write CAS blob directly
                let data_path = fs_root
                    .join("uploads")
                    .join(format!("{}.data", prepared.session.uuid));
                let dest_dir = fs_root
                    .join("blobs")
                    .join(prepared.expected_digest.algorithm())
                    .join(prepared.expected_digest.prefix2());
                tokio::fs::create_dir_all(&dest_dir).await.map_err(|e| {
                    UploadTransitionError::Storage(StorageError::Internal(e.to_string()))
                })?;
                let dest_path = dest_dir.join(prepared.expected_digest.hex());
                tokio::fs::rename(&data_path, &dest_path)
                    .await
                    .map_err(|e| {
                        UploadTransitionError::Storage(StorageError::Internal(e.to_string()))
                    })?;

                // Inject typed error on membership linking
                Err(UploadTransitionError::Storage(StorageError::Internal(
                    "injected membership ledger failure".to_string(),
                )))
            })
        }));
        hooks
    })
    .await;

    let payload = b"failed-link-proxy-blob-data";
    let digest = sha256_digest(payload);
    let canonical = CanonicalRepoName::parse("library/injected-fail-repo").unwrap();

    let stream = futures_util::stream::once(async move { Ok(Bytes::from_static(payload)) });
    let pinned: UploadByteStream = Box::pin(stream);

    // 1. Invoking publication must return a typed Storage error
    let pub_res = services
        .blob_mutation
        .publish_verified_proxy_blob(&canonical, &digest, pinned)
        .await;

    assert!(
        matches!(pub_res, Err(BlobMutationError::Storage(_))),
        "Injected membership failure must return typed BlobMutationError::Storage: {:?}",
        pub_res
    );

    // 2. Blob was not published with membership
    let mem = services
        .wiring
        .membership_reader()
        .get_repo_blob_membership("library/injected-fail-repo", &digest)
        .await
        .unwrap();
    assert!(
        mem.is_none(),
        "Membership record must not exist on failed publication"
    );

    // 3. While pin is active: real GC quarantine execution protects the blob
    let gc_service = services.create_gc_service().await;
    let budgets = GcBudgets {
        max_blobs: 1000,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };
    let q_stats_pinned = gc_service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets.clone(),
        )
        .await
        .expect("quarantine run while pinned succeeds");
    assert_eq!(
        q_stats_pinned.quarantined_blobs, 0,
        "Active retained pin must protect failed publication from GC quarantine"
    );

    // 4. Simulate process restart and recovery: purge expired pins and rebuild index
    let future_time = SystemTime::now() + Duration::from_secs(7200);
    services.ref_index.purge_expired_pins(future_time).unwrap();
    let base_storage = Arc::new(FsStorage::new(services.fs_root.clone(), 10 * 1024 * 1024));
    services
        .ref_index
        .rebuild(base_storage.as_ref())
        .await
        .unwrap();

    let is_pinned_after_expiry = services
        .ref_index
        .is_blob_pinned(&digest, future_time)
        .unwrap();
    assert!(
        !is_pinned_after_expiry,
        "Pin expires naturally after recovery window"
    );

    // 5. After simulated restart and pin expiry: real GC quarantine actually quarantines the unreferenced blob
    let q_stats_expired = gc_service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets.clone(),
        )
        .await
        .expect("quarantine after pin expiry succeeds");
    assert_eq!(
        q_stats_expired.quarantined_blobs, 1,
        "Unreferenced failed publication blob is actually quarantined by production GC sweep"
    );

    // 6. Real GC delete actually collects/deletes the quarantined blob
    let d_stats = gc_service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("delete sweep succeeds");
    assert_eq!(
        d_stats.deleted_blobs, 1,
        "Quarantined unreferenced blob is physically deleted by production GC delete"
    );

    let head_deleted = services.wiring.blob_reader().head_blob(&digest).await;
    assert!(
        head_deleted.is_err(),
        "Blob must be physically deleted from CAS storage"
    );
}

#[tokio::test]
async fn test_proxy_blob_publication_successful_membership_durable_before_pin_release() {
    let services = TestServices::new_fs().await;

    let payload = b"pin-release-ordering-test";
    let digest = sha256_digest(payload);
    let canonical = CanonicalRepoName::parse("library/pin-release-repo").unwrap();

    let stream = futures_util::stream::once(async move { Ok(Bytes::from_static(payload)) });
    let pinned: UploadByteStream = Box::pin(stream);

    services
        .blob_mutation
        .publish_verified_proxy_blob(&canonical, &digest, pinned)
        .await
        .unwrap();

    // After publication finishes, membership must be durably recorded in reverse index and storage
    let has_membership = services.ref_index.has_any_repo_membership(&digest).unwrap();
    assert!(
        has_membership,
        "Reverse index must record membership upon publication"
    );
    let mem = services
        .wiring
        .membership_reader()
        .get_repo_blob_membership("library/pin-release-repo", &digest)
        .await
        .unwrap();
    assert!(
        mem.is_some(),
        "Membership record must be present in storage"
    );
}

#[tokio::test]
async fn test_proxy_blob_publication_crash_after_membership_before_pin_release_membership_protects()
{
    let (in_barrier_tx, in_barrier_rx) = tokio::sync::oneshot::channel::<()>();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel::<()>();

    let in_tx = Arc::new(tokio::sync::Mutex::new(Some(in_barrier_tx)));
    let res_rx = Arc::new(tokio::sync::Mutex::new(Some(resume_rx)));

    let services = TestServices::new_hooked_with_fs(move |fs_root| {
        let in_tx = in_tx.clone();
        let res_rx = res_rx.clone();
        let fs_root = fs_root.to_path_buf();
        let mut hooks = StorageHooks::default();
        hooks.custom_commit_finalize = Some(Arc::new(move |inner, prepared| {
            let in_tx = in_tx.clone();
            let res_rx = res_rx.clone();
            let prepared = prepared.clone();
            let fs_root = fs_root.clone();
            Box::pin(async move {
                // 1. Move/rename staged data file into CAS blob storage
                let data_path = fs_root
                    .join("uploads")
                    .join(format!("{}.data", prepared.session.uuid));
                let dest_dir = fs_root
                    .join("blobs")
                    .join(prepared.expected_digest.algorithm())
                    .join(prepared.expected_digest.prefix2());
                tokio::fs::create_dir_all(&dest_dir).await.map_err(|e| {
                    UploadTransitionError::Storage(StorageError::Internal(e.to_string()))
                })?;
                let dest_path = dest_dir.join(prepared.expected_digest.hex());
                tokio::fs::rename(&data_path, &dest_path)
                    .await
                    .map_err(|e| {
                        UploadTransitionError::Storage(StorageError::Internal(e.to_string()))
                    })?;

                // 2. Durably write repository membership record in authoritative storage
                let membership = RepoBlobMembershipRecord::new_upload(
                    prepared.session.repo.clone(),
                    prepared.expected_digest.clone(),
                    Some(prepared.session.uuid.clone()),
                );
                inner
                    .link_repo_blob(&membership)
                    .await
                    .map_err(UploadTransitionError::Storage)?;

                // 3. Durably write finalized receipt
                let fin_dir = fs_root.join("finalized");
                tokio::fs::create_dir_all(&fin_dir).await.map_err(|e| {
                    UploadTransitionError::Storage(StorageError::Internal(e.to_string()))
                })?;
                let receipt_path = fin_dir.join(format!("{}.json", prepared.session.uuid));
                let receipt = FinalizedReceipt {
                    repo: prepared.session.repo.clone(),
                    uuid: prepared.session.uuid.clone(),
                    digest: prepared.expected_digest.to_string(),
                    size: prepared.size,
                    finalized_at_unix_secs: 1000,
                    format_version: 1,
                };
                tokio::fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap())
                    .await
                    .map_err(|e| {
                        UploadTransitionError::Storage(StorageError::Internal(e.to_string()))
                    })?;

                // 4. Signal barrier reached: CAS exists, repo membership is durable, receipt is written.
                // Execution pauses here before commit_finalize returns to coordinator, holding the publication pin active!
                if let Some(tx) = in_tx.lock().await.take() {
                    let _ = tx.send(());
                }
                if let Some(rx) = res_rx.lock().await.take() {
                    let _ = rx.await;
                }

                Ok(FinalizeOutcome::Published(BlobMeta {
                    size: prepared.size,
                }))
            })
        }));
        hooks
    })
    .await;

    let payload = b"crash-after-membership-blob-data";
    let digest = sha256_digest(payload);
    let canonical = CanonicalRepoName::parse("library/crash-repo").unwrap();

    let stream = futures_util::stream::once(async move { Ok(Bytes::from_static(payload)) });
    let pinned: UploadByteStream = Box::pin(stream);

    let blob_mutation = services.blob_mutation.clone();
    let canonical_clone = canonical.clone();
    let digest_clone = digest.clone();

    // Spawn high-level publication future in a separate task
    let pub_task = tokio::spawn(async move {
        blob_mutation
            .publish_verified_proxy_blob(&canonical_clone, &digest_clone, pinned)
            .await
    });

    // 1. Wait for execution to reach the barrier (after CAS and repo membership write, before pin release)
    in_barrier_rx.await.expect("reached crash-window barrier");

    // 2. Assert state at the barrier:
    // a) CAS contains the blob
    let cas_head = services.wiring.blob_reader().head_blob(&digest).await;
    assert!(cas_head.is_ok(), "CAS must contain the blob at the barrier");

    // b) Repository membership exists in authoritative storage
    let mem = services
        .wiring
        .membership_reader()
        .get_repo_blob_membership("library/crash-repo", &digest)
        .await
        .unwrap();
    assert!(
        mem.is_some(),
        "Repository membership must exist in authoritative storage at barrier"
    );

    // c) The publication pin is still active in BlobRefIndex
    let is_pinned = services
        .ref_index
        .is_blob_pinned(&digest, SystemTime::now())
        .unwrap();
    assert!(is_pinned, "Publication pin must still be active at barrier");

    // d) The high-level publication future has NOT returned
    assert!(
        !pub_task.is_finished(),
        "Publication task must still be in-flight (has not returned)"
    );

    // 3. Simulate sudden process crash:
    // Abort the in-flight publication task. PinLeaseGuard::drop aborts renewal but retains the persisted pin.
    pub_task.abort();
    let _ = pub_task.await;
    drop(resume_tx);

    let fs_root = services.fs_root.clone();
    let index_path = services.temp.path().join("index");
    let _temp = services.temp;
    drop(services.blob_mutation);
    drop(services.manifest_mutation);
    drop(services.blob_read);
    drop(services.manifest_read);
    drop(services.catalog_query);
    drop(services.tag_query);
    drop(services.referrers_query);
    drop(services.wiring);
    drop(services.ref_index);
    drop(services.consistency);

    // 4. Reopen storage and BlobRefIndex as a fresh process would
    let reopened_storage = Arc::new(FsStorage::new(fs_root.clone(), 10 * 1024 * 1024));
    let reopened_wiring = StorageWiring::from_backend(reopened_storage.clone());
    let reopened_index = Arc::new(BlobRefIndex::open(index_path.clone()).unwrap());
    reopened_index
        .ensure_healthy_or_rebuild(reopened_storage.as_ref(), true, true)
        .await
        .unwrap();

    // 5. Prove that the durable pin survived the simulated crash in reopened index
    let is_pinned_after_crash = reopened_index
        .is_blob_pinned(&digest, SystemTime::now())
        .unwrap();
    assert!(
        is_pinned_after_crash,
        "Durable publication pin survived crash restart"
    );

    // 6. Prove durable repository membership survived the crash in storage and ref index
    let mem_reopened = reopened_wiring
        .membership_reader()
        .get_repo_blob_membership("library/crash-repo", &digest)
        .await
        .unwrap();
    assert!(
        mem_reopened.is_some(),
        "Durable repo membership survived crash in storage"
    );
    let has_mem_reopened = reopened_index.has_any_repo_membership(&digest).unwrap();
    assert!(
        has_mem_reopened,
        "Durable repo membership survived crash in ref index"
    );

    // 7. Advance test clock beyond pin TTL and perform expired pin recovery
    let future_time = SystemTime::now() + Duration::from_secs(7200);
    let purged = reopened_index.purge_expired_pins(future_time).unwrap();
    assert!(purged > 0, "Expired pin was purged during recovery");

    let is_pinned_expired = reopened_index.is_blob_pinned(&digest, future_time).unwrap();
    assert!(!is_pinned_expired, "Publication pin is absent after expiry");

    // 8. Run real production GC sweeps without publishing any manifest (isolating membership protection)
    let mut cfg = support::gc_coordination::test_config(fs_root.clone(), index_path.clone());
    cfg.blob_gc_enabled = true;
    cfg.blob_gc_schedule_enabled = true;

    let authority =
        RuntimeMutationAuthority::acquire(reopened_storage.clone(), "test-crash-recovery-gc")
            .await
            .expect("acquire authority");

    let gc_service = GcService::with_coordinator_and_authority(
        Arc::new(cfg),
        reopened_storage.clone(),
        reopened_index.clone(),
        ConsistencyCoordinator::new(),
        Arc::new(tokio::sync::Mutex::new(Some(authority))),
    );

    let budgets = GcBudgets {
        max_blobs: 1000,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };

    // First, verify membership sweep preserves durable repo membership within grace period
    let mem_sweep = gc_service
        .sweep_repository_memberships(Duration::from_secs(3600), 100)
        .await
        .expect("membership sweep succeeds");
    assert_eq!(
        mem_sweep.unlinked, 0,
        "Durable repository membership is not unlinked"
    );

    // Run real production quarantine sweep (min_age = 3600s grace period protects active repo memberships)
    let q_stats = gc_service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(3600),
            budgets.clone(),
        )
        .await
        .expect("quarantine sweep succeeds");

    // Assert: quarantined_blobs == 0
    assert_eq!(
        q_stats.quarantined_blobs, 0,
        "Durable repo membership protects the blob from GC quarantine even after pin expiry"
    );

    // Assert: no quarantine object exists on disk
    let q_path = fs_root
        .join("quarantine")
        .join("blobs")
        .join(digest.algorithm())
        .join(digest.prefix2())
        .join(digest.hex());
    assert!(
        !q_path.exists(),
        "No quarantine file must exist for protected blob"
    );

    // Run real production delete sweep
    let d_stats = gc_service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(3600),
            budgets,
        )
        .await
        .expect("delete sweep succeeds");

    // Assert: deleted_blobs == 0
    assert_eq!(
        d_stats.deleted_blobs, 0,
        "Zero blobs deleted by GC delete sweep"
    );

    // Assert: CAS blob still exists
    let cas_head_final = reopened_wiring.blob_reader().head_blob(&digest).await;
    assert!(cas_head_final.is_ok(), "CAS blob must still exist");

    // Assert: repository membership still exists
    let mem_final = reopened_wiring
        .membership_reader()
        .get_repo_blob_membership("library/crash-repo", &digest)
        .await
        .unwrap();
    assert!(
        mem_final.is_some(),
        "Repository membership must still exist in storage"
    );
}

#[tokio::test]
async fn test_proxy_blob_publication_duplicate_idempotent() {
    let services = TestServices::new_fs().await;

    let payload = b"idempotent-proxy-blob-data";
    let digest = sha256_digest(payload);
    let canonical = CanonicalRepoName::parse("library/idempotent-repo").unwrap();

    let stream1 = futures_util::stream::once(async move { Ok(Bytes::from_static(payload)) });
    services
        .blob_mutation
        .publish_verified_proxy_blob(&canonical, &digest, Box::pin(stream1))
        .await
        .expect("first publish succeeds");

    let stream2 = futures_util::stream::once(async move { Ok(Bytes::from_static(payload)) });
    let res2 = services
        .blob_mutation
        .publish_verified_proxy_blob(&canonical, &digest, Box::pin(stream2))
        .await;

    assert!(
        res2.is_ok(),
        "Duplicate proxy blob publication must be idempotent and succeed: {:?}",
        res2
    );

    // Real production GC membership sweep verifies published blob membership is protected
    let gc_service = services.create_gc_service().await;
    let mem_stats = gc_service
        .sweep_repository_memberships(Duration::from_secs(3600), 100)
        .await
        .expect("membership sweep succeeds");
    assert_eq!(
        mem_stats.unlinked, 0,
        "Idempotently published blob membership is protected from unlinking"
    );

    let has_membership = services.ref_index.has_any_repo_membership(&digest).unwrap();
    assert!(
        has_membership,
        "Durable membership exists for idempotently published blob"
    );
}

// =================================================================================================
// 5. Manifest Proxy Publication Focused Tests
// =================================================================================================

#[tokio::test]
async fn test_manifest_proxy_publication_by_digest() {
    let services = TestServices::new_fs().await;

    let manifest_bytes = Bytes::from_static(
        br#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:baaddb0000000000000000000000000000000000000000000000000000000001",
            "size": 2
        },
        "layers": []
    }"#,
    );
    let digest = sha256_digest(&manifest_bytes);
    let evidence = ProxyPublicationEvidence::new(
        "library/proxy-manifest-repo",
        &digest.as_str(),
        manifest_bytes.clone(),
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
        digest.clone(),
    );

    let published = services
        .manifest_mutation
        .publish_verified_proxy_manifest(evidence)
        .await
        .expect("publish proxy manifest succeeds");

    assert_eq!(published.digest, digest);
    assert_eq!(
        published.media_type,
        "application/vnd.oci.image.manifest.v1+json"
    );

    // Verify stored in CAS
    let stored = services
        .wiring
        .manifest_reader()
        .get_manifest("library/proxy-manifest-repo", &digest)
        .await
        .unwrap();
    assert_eq!(stored.1, manifest_bytes);
}

#[tokio::test]
async fn test_manifest_proxy_publication_by_tag() {
    let services = TestServices::new_fs().await;

    let manifest_bytes = Bytes::from_static(
        br#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:baaddb0000000000000000000000000000000000000000000000000000000001",
            "size": 2
        },
        "layers": []
    }"#,
    );
    let digest = sha256_digest(&manifest_bytes);
    let evidence = ProxyPublicationEvidence::new(
        "library/proxy-tag-repo",
        "v2.1.0",
        manifest_bytes.clone(),
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
        digest.clone(),
    );

    let published = services
        .manifest_mutation
        .publish_verified_proxy_manifest(evidence)
        .await
        .expect("publish proxy tag manifest succeeds");

    assert_eq!(published.digest, digest);

    // Verify tag exists in TagReader via TagQueryService
    let tag_digest = services
        .tag_query
        .resolve_tag("library/proxy-tag-repo", "v2.1.0", None)
        .await
        .unwrap();
    assert_eq!(tag_digest, digest);
}

#[tokio::test]
async fn test_manifest_proxy_publication_subject_and_referrers() {
    let services = TestServices::new_fs().await;
    let subject_digest = sha256_digest(b"base-image-subject");

    let artifact_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "artifactType": "application/vnd.example.attestation",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:baaddb0000000000000000000000000000000000000000000000000000000001",
            "size": 2
        },
        "layers": [],
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": subject_digest.to_string(),
            "size": 500
        }
    });
    let payload = Bytes::from(serde_json::to_vec(&artifact_json).unwrap());
    let digest = sha256_digest(&payload);

    let evidence = ProxyPublicationEvidence::new(
        "library/proxy-subject-repo",
        &digest.as_str(),
        payload,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
        digest.clone(),
    );

    services
        .manifest_mutation
        .publish_verified_proxy_manifest(evidence)
        .await
        .unwrap();

    // Verify referrer descriptor was indexed
    let referrers = services
        .wiring
        .referrers_reader()
        .list_referrers("library/proxy-subject-repo", &subject_digest)
        .await
        .unwrap();

    assert_eq!(referrers.len(), 1);
    assert_eq!(referrers[0].digest, digest.to_string());
    assert_eq!(
        referrers[0].artifact_type.as_deref(),
        Some("application/vnd.example.attestation")
    );
}

#[tokio::test]
async fn test_manifest_proxy_publication_invalid_manifest_rejection() {
    let services = TestServices::new_fs().await;
    let invalid_bytes = Bytes::from_static(b"{ not valid json }");
    let digest = sha256_digest(&invalid_bytes);

    let evidence = ProxyPublicationEvidence::new(
        "library/invalid-manifest-repo",
        "latest",
        invalid_bytes,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
        digest,
    );

    let res = services
        .manifest_mutation
        .publish_verified_proxy_manifest(evidence)
        .await;

    assert!(res.is_err(), "Invalid manifest json must be rejected");
}

#[tokio::test]
async fn test_manifest_proxy_publication_digest_mismatch_rejection() {
    let services = TestServices::new_fs().await;
    let payload = Bytes::from_static(b"{\"schemaVersion\": 2}");
    let wrong_digest = sha256_digest(b"completely-wrong-content");

    let evidence = ProxyPublicationEvidence::new(
        "library/mismatch-manifest-repo",
        &wrong_digest.as_str(),
        payload,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
        wrong_digest,
    );

    let res = services
        .manifest_mutation
        .publish_verified_proxy_manifest(evidence)
        .await;

    assert!(
        matches!(res, Err(ManifestMutationError::DigestMismatch { .. })),
        "Digest mismatch must be rejected with DigestMismatch: {:?}",
        res
    );
}

#[tokio::test]
async fn test_manifest_proxy_publication_size_limit_rejection() {
    let services = TestServices::new_fs().await;
    // 5 MiB payload exceeds 4 MiB limit
    let big_payload = Bytes::from(vec![0x20u8; 5 * 1024 * 1024]);
    let digest = sha256_digest(&big_payload);

    let evidence = ProxyPublicationEvidence::new(
        "library/oversized-manifest-repo",
        "latest",
        big_payload,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
        digest,
    );

    let res = services
        .manifest_mutation
        .publish_verified_proxy_manifest(evidence)
        .await;

    assert!(
        matches!(res, Err(ManifestMutationError::PayloadTooLarge)),
        "Payload exceeding MAX_MANIFEST_SIZE must be rejected with PayloadTooLarge"
    );
}

#[tokio::test]
async fn test_manifest_proxy_publication_failed_publication_leaves_no_partial_state() {
    let services = TestServices::new_fs().await;
    let invalid_bytes = Bytes::from_static(b"{ malformed }");
    let digest = sha256_digest(&invalid_bytes);

    let evidence = ProxyPublicationEvidence::new(
        "library/no-partial-state",
        "v1.0",
        invalid_bytes,
        None,
        true,
        digest.clone(),
    );

    let _ = services
        .manifest_mutation
        .publish_verified_proxy_manifest(evidence)
        .await;

    // Verify 0 tags and 0 CAS entries exist
    let tag = services
        .tag_query
        .resolve_tag("library/no-partial-state", "v1.0", None)
        .await;
    assert!(tag.is_err(), "No partial tag created on failure");

    let cas = services
        .wiring
        .manifest_reader()
        .head_manifest("library/no-partial-state", &digest)
        .await;
    assert!(cas.is_err(), "No CAS manifest created on failure");
}

#[tokio::test]
async fn test_manifest_proxy_publication_duplicate_idempotent() {
    let services = TestServices::new_fs().await;

    let manifest_bytes = Bytes::from_static(
        br#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:baaddb0000000000000000000000000000000000000000000000000000000001",
            "size": 2
        },
        "layers": []
    }"#,
    );
    let digest = sha256_digest(&manifest_bytes);
    let evidence1 = ProxyPublicationEvidence::new(
        "library/dup-manifest-repo",
        "v1",
        manifest_bytes.clone(),
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
        digest.clone(),
    );
    let evidence2 = ProxyPublicationEvidence::new(
        "library/dup-manifest-repo",
        "v1",
        manifest_bytes,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
        digest.clone(),
    );

    let p1 = services
        .manifest_mutation
        .publish_verified_proxy_manifest(evidence1)
        .await
        .unwrap();
    let p2 = services
        .manifest_mutation
        .publish_verified_proxy_manifest(evidence2)
        .await
        .unwrap();

    assert_eq!(p1.digest, digest);
    assert_eq!(p2.digest, digest);
}

// =================================================================================================
// 6. Complete HTTP Endpoint Protocol Behavior Parity Tests
// =================================================================================================

#[tokio::test]
async fn test_http_endpoint_blob_get_head_empty_body_and_exact_headers() {
    let server = HttpTestServer::spawn(None).await;
    let client = reqwest::Client::new();

    let payload = b"http-blob-payload-content";
    let digest = sha256_digest(payload);
    let canonical = CanonicalRepoName::parse("test/http-blob").unwrap();

    let stream = futures_util::stream::once(async move { Ok(Bytes::from_static(payload)) });
    server
        .services
        .blob_mutation
        .publish_verified_proxy_blob(&canonical, &digest, Box::pin(stream))
        .await
        .unwrap();

    // 1. GET blob
    let get_resp = client
        .get(format!(
            "{}/v2/test/http-blob/blobs/{}",
            server.base_url, digest
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(get_resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        get_resp
            .headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
        payload.len().to_string()
    );
    assert_eq!(
        get_resp
            .headers()
            .get("docker-content-digest")
            .unwrap()
            .to_str()
            .unwrap(),
        digest.to_string()
    );
    let body = get_resp.bytes().await.unwrap();
    assert_eq!(&body[..], payload);

    // 2. HEAD blob (must have empty body)
    let head_resp = client
        .head(format!(
            "{}/v2/test/http-blob/blobs/{}",
            server.base_url, digest
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(head_resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        head_resp
            .headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
        payload.len().to_string()
    );
    assert_eq!(
        head_resp
            .headers()
            .get("docker-content-digest")
            .unwrap()
            .to_str()
            .unwrap(),
        digest.to_string()
    );
    let head_body = head_resp.bytes().await.unwrap();
    assert_eq!(head_body.len(), 0, "HEAD response body must be empty");
}

#[tokio::test]
async fn test_http_endpoint_blob_valid_ranges_206_and_unsatisfiable_416() {
    let server = HttpTestServer::spawn(None).await;
    let client = reqwest::Client::new();

    let payload = b"0123456789abcdef"; // 16 bytes
    let digest = sha256_digest(payload);
    let canonical = CanonicalRepoName::parse("test/range-blob").unwrap();

    let stream = futures_util::stream::once(async move { Ok(Bytes::from_static(payload)) });
    server
        .services
        .blob_mutation
        .publish_verified_proxy_blob(&canonical, &digest, Box::pin(stream))
        .await
        .unwrap();

    // 1. Valid range bytes=0-3
    let range_resp = client
        .get(format!(
            "{}/v2/test/range-blob/blobs/{}",
            server.base_url, digest
        ))
        .header("Range", "bytes=0-3")
        .send()
        .await
        .unwrap();

    assert_eq!(range_resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        range_resp
            .headers()
            .get("content-range")
            .unwrap()
            .to_str()
            .unwrap(),
        "bytes 0-3/16"
    );
    assert_eq!(
        range_resp
            .headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
        "4"
    );
    let range_body = range_resp.bytes().await.unwrap();
    assert_eq!(&range_body[..], b"0123");

    // 2. Unsatisfiable range beyond EOF (bytes=100-200)
    let unsat_resp = client
        .get(format!(
            "{}/v2/test/range-blob/blobs/{}",
            server.base_url, digest
        ))
        .header("Range", "bytes=100-200")
        .send()
        .await
        .unwrap();

    assert_eq!(
        unsat_resp.status(),
        reqwest::StatusCode::RANGE_NOT_SATISFIABLE
    );
    assert_eq!(
        unsat_resp
            .headers()
            .get("content-range")
            .unwrap()
            .to_str()
            .unwrap(),
        "bytes */16"
    );
}

#[tokio::test]
async fn test_http_endpoint_manifest_get_head_empty_body_and_exact_headers() {
    let server = HttpTestServer::spawn(None).await;
    let client = reqwest::Client::new();

    let manifest_bytes = Bytes::from_static(
        br#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:baaddb0000000000000000000000000000000000000000000000000000000001",
            "size": 2
        },
        "layers": []
    }"#,
    );
    let digest = sha256_digest(&manifest_bytes);
    let repo = "test/http-manifest";

    let evidence = ProxyPublicationEvidence::new(
        repo,
        "v1.0",
        manifest_bytes.clone(),
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
        digest.clone(),
    );
    server
        .services
        .manifest_mutation
        .publish_verified_proxy_manifest(evidence)
        .await
        .unwrap();

    // 1. GET manifest by tag
    let get_resp = client
        .get(format!("{}/v2/{}/manifests/v1.0", server.base_url, repo))
        .send()
        .await
        .unwrap();

    assert_eq!(get_resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        get_resp
            .headers()
            .get("docker-content-digest")
            .unwrap()
            .to_str()
            .unwrap(),
        digest.to_string()
    );
    assert_eq!(
        get_resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/vnd.oci.image.manifest.v1+json"
    );

    // 2. HEAD manifest by digest (must have empty body)
    let head_resp = client
        .head(format!(
            "{}/v2/{}/manifests/{}",
            server.base_url, repo, digest
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(head_resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        head_resp
            .headers()
            .get("docker-content-digest")
            .unwrap()
            .to_str()
            .unwrap(),
        digest.to_string()
    );
    let head_body = head_resp.bytes().await.unwrap();
    assert_eq!(head_body.len(), 0, "HEAD manifest body must be empty");
}

#[tokio::test]
async fn test_http_endpoint_manifest_accept_negotiation_compatible_wildcard_incompatible() {
    let server = HttpTestServer::spawn(None).await;
    let client = reqwest::Client::new();

    let manifest_bytes = Bytes::from_static(
        br#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:baaddb0000000000000000000000000000000000000000000000000000000001",
            "size": 2
        },
        "layers": []
    }"#,
    );
    let digest = sha256_digest(&manifest_bytes);
    let repo = "test/accept-negotiation";

    let evidence = ProxyPublicationEvidence::new(
        repo,
        "latest",
        manifest_bytes,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
        digest.clone(),
    );
    server
        .services
        .manifest_mutation
        .publish_verified_proxy_manifest(evidence)
        .await
        .unwrap();

    // 1. Accept compatible OCI
    let resp_oci = client
        .get(format!("{}/v2/{}/manifests/latest", server.base_url, repo))
        .header("Accept", "application/vnd.oci.image.manifest.v1+json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp_oci.status(), reqwest::StatusCode::OK);

    // 2. Accept wildcard */*
    let resp_wildcard = client
        .get(format!("{}/v2/{}/manifests/latest", server.base_url, repo))
        .header("Accept", "*/*")
        .send()
        .await
        .unwrap();
    assert_eq!(resp_wildcard.status(), reqwest::StatusCode::OK);

    // 3. Accept incompatible type
    let resp_incompatible = client
        .get(format!("{}/v2/{}/manifests/latest", server.base_url, repo))
        .header("Accept", "application/xml")
        .send()
        .await
        .unwrap();
    assert!(
        resp_incompatible.status() == reqwest::StatusCode::NOT_ACCEPTABLE
            || resp_incompatible.status() == reqwest::StatusCode::OK,
        "Incompatible Accept header returns 406 or valid manifest"
    );
}

#[tokio::test]
async fn test_http_endpoint_manifest_conditional_if_none_match_304() {
    let server = HttpTestServer::spawn(None).await;
    let client = reqwest::Client::new();

    let manifest_bytes = Bytes::from_static(
        br#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:baaddb0000000000000000000000000000000000000000000000000000000001",
            "size": 2
        },
        "layers": []
    }"#,
    );
    let digest = sha256_digest(&manifest_bytes);
    let repo = "test/conditional-manifest";

    let evidence = ProxyPublicationEvidence::new(
        repo,
        "v1",
        manifest_bytes,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
        digest.clone(),
    );
    server
        .services
        .manifest_mutation
        .publish_verified_proxy_manifest(evidence)
        .await
        .unwrap();

    // GET with matching If-None-Match header
    let resp = client
        .get(format!("{}/v2/{}/manifests/v1", server.base_url, repo))
        .header("If-None-Match", format!("\"{}\"", digest))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::NOT_MODIFIED);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), 0, "304 response body must be empty");
}

#[tokio::test]
async fn test_http_endpoint_proxy_miss_and_proxy_only_routing() {
    let payload = b"http-proxy-miss-blob-data";
    let digest = sha256_digest(payload);
    let digest_str = digest.to_string();

    let app = axum::Router::new().route(
        "/v2/proxy/http-miss/blobs/:digest",
        get(move |axum::extract::Path(d): axum::extract::Path<String>| {
            let p = payload;
            let exp = digest_str.clone();
            async move {
                if d == exp {
                    (StatusCode::OK, Bytes::from_static(p))
                } else {
                    (StatusCode::NOT_FOUND, Bytes::new())
                }
            }
        }),
    );
    let (upstream_url, _handle) = spawn_mock_upstream(app).await;

    let upstream_url_clone = upstream_url.clone();
    let server = HttpTestServer::spawn(Some(Box::new(move |temp| {
        let proxy_cfg =
            create_test_proxy_config(upstream_url_clone, TagPolicy::TtlSeconds(3600), temp);
        let proxy = Arc::new(Proxy::new(&proxy_cfg).unwrap().unwrap());
        (proxy, proxy_cfg)
    })))
    .await;
    let client = reqwest::Client::new();

    // GET blob via proxy cache miss
    let resp = client
        .get(format!(
            "{}/v2/proxy/http-miss/blobs/{}",
            server.base_url, digest
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.bytes().await.unwrap();
    assert_eq!(&body[..], payload);
}

#[tokio::test]
async fn test_http_endpoint_catalog_tags_referrers_pagination_ordering_and_link_headers() {
    let server = HttpTestServer::spawn(None).await;
    let client = reqwest::Client::new();

    let dummy_digest = sha256_digest(b"dummy");
    let dummy_manifest = Bytes::from_static(b"{}");

    for r in &["repo-1", "repo-2", "repo-3"] {
        server
            .services
            .wiring
            .manifest_lifecycle()
            .put_manifest(r, &dummy_digest, dummy_manifest.clone())
            .await
            .unwrap();
        server
            .services
            .wiring
            .manifest_lifecycle()
            .set_tag(r, "tag-a", &dummy_digest)
            .await
            .unwrap();
        server
            .services
            .wiring
            .manifest_lifecycle()
            .set_tag(r, "tag-b", &dummy_digest)
            .await
            .unwrap();
    }

    // 1. Catalog pagination Link header
    let cat_resp = client
        .get(format!("{}/v2/_catalog?n=2", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(cat_resp.status(), reqwest::StatusCode::OK);
    let cat_link = cat_resp.headers().get("link").unwrap().to_str().unwrap();
    assert!(
        cat_link.contains("rel=\"next\"") && cat_link.contains("last=repo-2"),
        "Catalog Link header must contain next cursor: {}",
        cat_link
    );

    // 2. Tags pagination Link header
    let tag_resp = client
        .get(format!("{}/v2/repo-1/tags/list?n=1", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(tag_resp.status(), reqwest::StatusCode::OK);
    let tag_link = tag_resp.headers().get("link").unwrap().to_str().unwrap();
    assert!(
        tag_link.contains("rel=\"next\"") && tag_link.contains("last=tag-a"),
        "Tag Link header must contain next cursor: {}",
        tag_link
    );

    // 3. Referrers pagination Link header
    let r1 = ReferrerDescriptor {
        media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
        digest: sha256_digest(b"ref-link-1").to_string(),
        size: 100,
        artifact_type: Some("application/vnd.example.test".to_string()),
        annotations: Default::default(),
    };
    let r2 = ReferrerDescriptor {
        media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
        digest: sha256_digest(b"ref-link-2").to_string(),
        size: 200,
        artifact_type: Some("application/vnd.example.test".to_string()),
        annotations: Default::default(),
    };
    for r in [&r1, &r2] {
        server
            .services
            .wiring
            .manifest_lifecycle()
            .add_referrer("repo-1", &dummy_digest, (*r).clone())
            .await
            .unwrap();
    }

    let ref_resp = client
        .get(format!(
            "{}/v2/repo-1/referrers/{}?n=1",
            server.base_url, dummy_digest
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(ref_resp.status(), reqwest::StatusCode::OK);
    let ref_link = ref_resp.headers().get("link").unwrap().to_str().unwrap();
    assert!(
        ref_link.contains("rel=\"next\""),
        "Referrers Link header must contain next cursor: {}",
        ref_link
    );
}

#[tokio::test]
async fn test_http_endpoint_referrers_artifact_type_filtering_and_applied_headers() {
    let server = HttpTestServer::spawn(None).await;
    let client = reqwest::Client::new();

    let subject = sha256_digest(b"subject-filter-test");
    let repo = "filter/referrers";

    let r_sbom = ReferrerDescriptor {
        media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
        digest: sha256_digest(b"sbom-payload").to_string(),
        size: 100,
        artifact_type: Some("application/vnd.example.sbom".to_string()),
        annotations: Default::default(),
    };
    let r_sig = ReferrerDescriptor {
        media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
        digest: sha256_digest(b"sig-payload").to_string(),
        size: 200,
        artifact_type: Some("application/vnd.example.sig".to_string()),
        annotations: Default::default(),
    };

    for r in [&r_sbom, &r_sig] {
        server
            .services
            .wiring
            .manifest_lifecycle()
            .add_referrer(repo, &subject, (*r).clone())
            .await
            .unwrap();
    }

    let resp = client
        .get(format!(
            "{}/v2/{}/referrers/{}?artifactType=application/vnd.example.sbom",
            server.base_url, repo, subject
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("oci-filters-applied")
            .unwrap()
            .to_str()
            .unwrap(),
        "artifactType"
    );

    let body: serde_json::Value = resp.json().await.unwrap();
    let manifests = body["manifests"].as_array().unwrap();
    assert_eq!(manifests.len(), 1);
    assert_eq!(
        manifests[0]["artifactType"].as_str().unwrap(),
        "application/vnd.example.sbom"
    );
}

#[tokio::test]
async fn test_http_endpoint_invalid_repo_name_400_and_unknown_entities_404_error_codes() {
    let server = HttpTestServer::spawn(None).await;
    let client = reqwest::Client::new();

    let dummy_digest = sha256_digest(b"nonexistent");

    // 1. Invalid repo name uppercase -> 400 with NAME_INVALID
    let invalid_repo = client
        .get(format!(
            "{}/v2/INVALID_UPPERCASE/blobs/{}",
            server.base_url, dummy_digest
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_repo.status(), reqwest::StatusCode::BAD_REQUEST);
    let inv_body: serde_json::Value = invalid_repo.json().await.unwrap();
    assert_eq!(
        inv_body["errors"][0]["code"].as_str().unwrap(),
        "NAME_INVALID"
    );

    // 2. Unknown blob -> 404 with BLOB_UNKNOWN
    let unknown_blob = client
        .get(format!(
            "{}/v2/valid/repo/blobs/{}",
            server.base_url, dummy_digest
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown_blob.status(), reqwest::StatusCode::NOT_FOUND);
    let blob_body: serde_json::Value = unknown_blob.json().await.unwrap();
    assert_eq!(
        blob_body["errors"][0]["code"].as_str().unwrap(),
        "BLOB_UNKNOWN"
    );

    // 3. Unknown manifest -> 404 with MANIFEST_UNKNOWN
    let unknown_manifest = client
        .get(format!(
            "{}/v2/valid/repo/manifests/{}",
            server.base_url, dummy_digest
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown_manifest.status(), reqwest::StatusCode::NOT_FOUND);
    let man_body: serde_json::Value = unknown_manifest.json().await.unwrap();
    assert_eq!(
        man_body["errors"][0]["code"].as_str().unwrap(),
        "MANIFEST_UNKNOWN"
    );

    // 4. Unknown repo tags -> 404 with NAME_UNKNOWN
    let unknown_tags = client
        .get(format!("{}/v2/nonexistent/repo/tags/list", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown_tags.status(), reqwest::StatusCode::NOT_FOUND);
    let tags_body: serde_json::Value = unknown_tags.json().await.unwrap();
    assert_eq!(
        tags_body["errors"][0]["code"].as_str().unwrap(),
        "NAME_UNKNOWN"
    );
}

// =================================================================================================
// 7. S3 MinIO Application Read and Proxy Publication Tests
// =================================================================================================

#[tokio::test]
async fn test_application_read_and_proxy_publication_services_s3_minio() {
    let endpoint =
        std::env::var("TEST_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
    let region = std::env::var("TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let bucket =
        std::env::var("TEST_S3_BUCKET").unwrap_or_else(|_| "registry-live-test".to_string());

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let prefix = format!("live-test-app-read-{}-{}/", Uuid::new_v4(), now_secs);

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

    let backend = Arc::new(S3Storage::new(
        Some(endpoint.clone()),
        Some(region.clone()),
        Some(bucket.clone()),
        prefix.clone(),
        10 * 1024 * 1024,
    ));

    let wiring = StorageWiring::from_backend(backend);
    assert_eq!(wiring.backend_kind(), "s3");
    println!("LIVE S3 TEST: Selected backend={}", wiring.backend_kind());
    println!(
        "LIVE S3 TEST: Contacted MinIO at endpoint={}, region={}, bucket={}, prefix={}",
        endpoint, region, bucket, prefix
    );

    let consistency = ConsistencyCoordinator::new();
    let blob_mutation = Arc::new(BlobMutationService::new(
        wiring.blob_mutation(),
        None,
        consistency.clone(),
        BlobUploadCoordinatorConfig::default(),
    ));
    let manifest_mutation = Arc::new(ManifestMutationService::new(
        wiring.manifest_lifecycle(),
        None,
        consistency,
    ));
    let blob_read = Arc::new(BlobReadService::new(
        wiring.blob_reader(),
        wiring.membership_reader(),
        blob_mutation.clone(),
    ));
    let manifest_read = Arc::new(ManifestReadService::new(
        wiring.manifest_reader(),
        wiring.tag_reader(),
        manifest_mutation.clone(),
        4 * 1024 * 1024,
        Some(Arc::new(tokio::sync::Semaphore::new(10))),
    ));
    let catalog_query = Arc::new(CatalogQueryService::new(
        wiring.catalog_reader(),
        wiring.tag_reader(),
        wiring.manifest_reader(),
        wiring.blob_reader(),
    ));
    let tag_query = Arc::new(TagQueryService::new(wiring.tag_reader()));
    let referrers_query = Arc::new(ReferrersQueryService::new(wiring.referrers_reader()));

    // 1. Direct proxy publication use case invocation on S3
    let payload = b"s3-proxy-publication-service-verification";
    let digest = sha256_digest(payload);
    let canonical = CanonicalRepoName::parse("library/s3-proxy-app").unwrap();
    let stream = futures_util::stream::once(async move { Ok(Bytes::from_static(payload)) });
    let pinned: UploadByteStream = Box::pin(stream);

    blob_mutation
        .publish_verified_proxy_blob(&canonical, &digest, pinned)
        .await
        .expect("publish_verified_proxy_blob succeeds on S3 storage");
    println!(
        "LIVE S3 TEST: Successfully published proxy blob via BlobMutationService::publish_verified_proxy_blob: {}",
        digest
    );

    // 2. Read blob via BlobReadService
    let head = blob_read
        .head_blob("library/s3-proxy-app", &digest, None, false)
        .await
        .expect("head blob on S3");
    assert_eq!(head.digest, digest);
    assert_eq!(head.size, payload.len() as u64);

    let get = blob_read
        .get_blob("library/s3-proxy-app", &digest, None, false)
        .await
        .expect("get blob on S3");
    assert_eq!(get.digest, digest);

    // 3. Manifest Proxy Publication on S3
    let manifest_bytes = Bytes::from_static(
        br#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:baaddb0000000000000000000000000000000000000000000000000000000001",
            "size": 2
        },
        "layers": []
    }"#,
    );
    let m_digest = sha256_digest(&manifest_bytes);
    let evidence = ProxyPublicationEvidence::new(
        "library/s3-proxy-app",
        "v1.0.0",
        manifest_bytes.clone(),
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
        m_digest.clone(),
    );

    manifest_mutation
        .publish_verified_proxy_manifest(evidence)
        .await
        .expect("publish proxy manifest succeeds on S3");
    println!(
        "LIVE S3 TEST: Successfully published proxy manifest via ManifestMutationService::publish_verified_proxy_manifest: {}",
        m_digest
    );

    // 4. Read manifest via ManifestReadService
    let m_head = manifest_read
        .head_manifest("library/s3-proxy-app", "v1.0.0", None, false, None)
        .await
        .expect("head manifest on S3");
    assert_eq!(m_head.digest, m_digest);

    // 5. Query tags via TagQueryService
    let tag_page = tag_query
        .query_tags(
            "library/s3-proxy-app",
            TagQueryParams {
                n: Some(10),
                last: None,
            },
            None,
        )
        .await
        .expect("query tags on S3");
    assert_eq!(tag_page.tags, vec!["v1.0.0".to_string()]);

    // 6. Query catalog via CatalogQueryService
    let cat_page = catalog_query
        .query_catalog(
            CatalogQueryParams {
                n: Some(10),
                last: None,
            },
            None,
        )
        .await
        .expect("query catalog on S3");
    assert!(
        cat_page
            .repositories
            .contains(&"library/s3-proxy-app".to_string())
    );

    // 7. Query referrers via ReferrersQueryService
    let ref_page = referrers_query
        .query_referrers(
            "library/s3-proxy-app",
            &m_digest,
            ReferrersQueryParams::default(),
            None,
        )
        .await
        .expect("query referrers on S3");
    assert!(ref_page.descriptors.is_empty());

    // Cleanup S3 test prefix
    let list_res = s3_client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(&prefix)
        .send()
        .await
        .expect("list objects for cleanup");
    let mut deleted_count = 0;
    if let Some(contents) = list_res.contents {
        for obj in contents {
            if let Some(key) = obj.key {
                let _ = s3_client
                    .delete_object()
                    .bucket(&bucket)
                    .key(key)
                    .send()
                    .await;
                deleted_count += 1;
            }
        }
    }
    println!(
        "LIVE S3 TEST: Enumerated and purged {} objects under prefix '{}'",
        deleted_count, prefix
    );

    // Assert post-cleanup absence
    let post_check = s3_client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(&prefix)
        .send()
        .await
        .expect("verify prefix is absent");
    let remaining_count = post_check.key_count().unwrap_or(0);
    println!(
        "LIVE S3 TEST: Verified {} objects remaining under prefix '{}'",
        remaining_count, prefix
    );
    assert_eq!(
        remaining_count, 0,
        "S3 prefix must be completely empty after cleanup"
    );
}
