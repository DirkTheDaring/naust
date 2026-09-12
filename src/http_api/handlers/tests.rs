use crate::AppState;
use crate::http_api::admin::{
    AdminGcDeleteRequest, AdminGcPlanRequest, AdminGcQuarantineRequest, admin_gc_delete,
    admin_gc_plan, admin_gc_quarantine,
};
use crate::http_api::auth_token::{
    TokenRejection, decide_token_scopes_for_request, parse_scopes, sanitize_token_scopes,
    service_param_is_valid, token_scope_requests_repo_action, wants_push_from_token_scopes,
};
use crate::http_api::catalog::meta_catalog;
use crate::registry::validation::{is_valid_repo_name, is_valid_tag};
use crate::storage::Storage;
use axum::extract::{Json, State};
use axum::http::{HeaderMap, StatusCode};
use headers::{Authorization, HeaderMapExt};
use http_body_util::BodyExt;
use std::sync::Arc;

fn with_admin_creds(mut cfg: Config) -> Config {
    cfg.admin_api = crate::config::AdminApiConfig {
        enabled: true,
        username: Some("admin".to_string()),
        password: Some("secret".to_string()),
    };
    cfg
}

fn admin_headers_ok() -> HeaderMap {
    let mut headers = HeaderMap::new();
    let auth = Authorization::basic("admin", "secret");
    headers.typed_insert(auth);
    headers
}

fn test_app_state(
    cfg: Arc<crate::config::Config>,
    storage: Arc<crate::storage::fs::FsStorage>,
    gc_service: Option<Arc<crate::gc_service::GcService>>,
) -> AppState {
    AppState::new_test(cfg, storage, gc_service)
}

#[tokio::test]
async fn admin_gc_requires_auth() {
    let cfg = Arc::new(with_admin_creds(minimal_config_for_token_tests()));
    let storage = Arc::new(crate::storage::fs::FsStorage::new(
        cfg.fs_root.clone(),
        cfg.max_upload_bytes,
    ));
    let state = test_app_state(cfg, storage, None);

    let req = AdminGcPlanRequest::default();

    let resp = admin_gc_plan(State(state), HeaderMap::new(), Json(req)).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn admin_gc_returns_503_when_service_missing() {
    let cfg = Arc::new(with_admin_creds(minimal_config_for_token_tests()));
    let storage = Arc::new(crate::storage::fs::FsStorage::new(
        cfg.fs_root.clone(),
        cfg.max_upload_bytes,
    ));
    let state = test_app_state(cfg, storage, None);

    let req = AdminGcPlanRequest::default();

    let resp = admin_gc_plan(State(state), admin_headers_ok(), Json(req)).await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn admin_gc_maps_already_running_to_conflict() {
    let fs_root =
        std::env::temp_dir().join(format!("registry-rust-admin-gc-{}", uuid::Uuid::new_v4()));
    let _ = std::fs::create_dir_all(&fs_root);
    let ref_index_path = fs_root.join("ref-index");
    let _ = std::fs::create_dir_all(&ref_index_path);

    let mut cfg = minimal_config_for_token_tests();
    cfg.fs_root = fs_root.clone();
    cfg.ref_index.path = ref_index_path.clone();
    let cfg = Arc::new(with_admin_creds(cfg));

    let storage_raw = Arc::new(crate::storage::fs::FsStorage::new(
        cfg.fs_root.clone(),
        cfg.max_upload_bytes,
    ));
    let idx = Arc::new(crate::blob_ref_index::BlobRefIndex::open(ref_index_path).expect("idx"));
    idx.rebuild(&storage_raw).await.expect("rebuild");
    let service = Arc::new(crate::gc_service::GcService::new(
        cfg.clone(),
        storage_raw.clone(),
        idx,
        crate::consistency::ConsistencyCoordinator::new(),
    ));
    let service_for_state = service.clone();
    let held = service.test_try_lock().expect("lock");

    let state = test_app_state(cfg, storage_raw, Some(service_for_state));

    let req = AdminGcPlanRequest::default();

    let resp = admin_gc_plan(State(state), admin_headers_ok(), Json(req)).await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);

    drop(held);
    let _ = std::fs::remove_dir_all(&fs_root);
}

#[tokio::test]
async fn admin_gc_quarantine_blocked_when_kill_switch_off() {
    let fs_root =
        std::env::temp_dir().join(format!("registry-rust-admin-gc-{}", uuid::Uuid::new_v4()));
    let _ = std::fs::create_dir_all(&fs_root);
    let ref_index_path = fs_root.join("ref-index");
    let _ = std::fs::create_dir_all(&ref_index_path);

    let mut cfg = minimal_config_for_token_tests();
    cfg.fs_root = fs_root.clone();
    cfg.ref_index.path = ref_index_path.clone();
    cfg.blob_gc_enabled = false;
    let cfg = Arc::new(with_admin_creds(cfg));

    let storage_raw = Arc::new(crate::storage::fs::FsStorage::new(
        cfg.fs_root.clone(),
        cfg.max_upload_bytes,
    ));
    let idx = Arc::new(crate::blob_ref_index::BlobRefIndex::open(ref_index_path).expect("idx"));
    idx.rebuild(&storage_raw).await.expect("rebuild");
    let service = Arc::new(crate::gc_service::GcService::new(
        cfg.clone(),
        storage_raw.clone(),
        idx,
        crate::consistency::ConsistencyCoordinator::new(),
    ));

    let state = test_app_state(cfg, storage_raw, Some(service));

    let resp = admin_gc_quarantine(
        State(state),
        admin_headers_ok(),
        Json(AdminGcQuarantineRequest::default()),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    let _ = std::fs::remove_dir_all(&fs_root);
}

#[tokio::test]
async fn admin_gc_delete_blocked_when_delete_gate_off() {
    let fs_root =
        std::env::temp_dir().join(format!("registry-rust-admin-gc-{}", uuid::Uuid::new_v4()));
    let _ = std::fs::create_dir_all(&fs_root);
    let ref_index_path = fs_root.join("ref-index");
    let _ = std::fs::create_dir_all(&ref_index_path);

    let mut cfg = minimal_config_for_token_tests();
    cfg.fs_root = fs_root.clone();
    cfg.ref_index.path = ref_index_path.clone();
    cfg.blob_gc_enabled = true;
    cfg.blob_gc_enable_delete = false;
    let cfg = Arc::new(with_admin_creds(cfg));

    let storage_raw = Arc::new(crate::storage::fs::FsStorage::new(
        cfg.fs_root.clone(),
        cfg.max_upload_bytes,
    ));
    let idx = Arc::new(crate::blob_ref_index::BlobRefIndex::open(ref_index_path).expect("idx"));
    idx.rebuild(&storage_raw).await.expect("rebuild");
    let service = Arc::new(crate::gc_service::GcService::new(
        cfg.clone(),
        storage_raw.clone(),
        idx,
        crate::consistency::ConsistencyCoordinator::new(),
    ));

    let state = test_app_state(cfg, storage_raw, Some(service));

    let resp = admin_gc_delete(
        State(state),
        admin_headers_ok(),
        Json(AdminGcDeleteRequest::default()),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    let _ = std::fs::remove_dir_all(&fs_root);
}

#[tokio::test]
async fn meta_catalog_include_tags_adds_tags_and_tag_count() {
    let fs_root =
        std::env::temp_dir().join(format!("registry-rust-meta-tags-{}", uuid::Uuid::new_v4()));
    let _ = std::fs::create_dir_all(&fs_root);

    // Create repos + tags on disk.
    let repo_dir = fs_root
        .join("repos")
        .join("org1")
        .join("repoa")
        .join("tags");
    let _ = std::fs::create_dir_all(&repo_dir);
    std::fs::write(repo_dir.join("latest"), "sha256:deadbeef\n").unwrap();
    std::fs::write(repo_dir.join("v1"), "sha256:cafebabe\n").unwrap();

    let mut cfg = minimal_config_for_token_tests();
    cfg.fs_root = fs_root.clone();
    cfg.catalog_requires_auth = false;
    let cfg = Arc::new(cfg);

    let storage = Arc::new(crate::storage::fs::FsStorage::new(
        cfg.fs_root.clone(),
        cfg.max_upload_bytes,
    ));

    let state = test_app_state(cfg, storage, None);

    let mut q = std::collections::HashMap::new();
    q.insert("include_tags".to_string(), "1".to_string());

    let resp = meta_catalog(State(state), HeaderMap::new(), axum::extract::Query(q)).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let repos = v.get("repositories").and_then(|x| x.as_array()).unwrap();
    assert_eq!(repos.len(), 1);
    let repo0 = repos[0].as_object().unwrap();
    assert_eq!(
        repo0.get("name").and_then(|x| x.as_str()),
        Some("org1/repoa")
    );
    assert_eq!(repo0.get("tag_count").and_then(|x| x.as_u64()), Some(2));

    let tags = repo0.get("tags").and_then(|x| x.as_array()).unwrap();
    let tags: Vec<&str> = tags.iter().filter_map(|x| x.as_str()).collect();
    assert_eq!(tags, vec!["latest", "v1"]);

    let _ = std::fs::remove_dir_all(&fs_root);
}

#[tokio::test]
async fn meta_catalog_include_platforms_adds_tag_details_with_platforms() {
    let fs_root = std::env::temp_dir().join(format!(
        "registry-rust-meta-platforms-{}",
        uuid::Uuid::new_v4()
    ));
    let _ = std::fs::create_dir_all(&fs_root);

    let repo = "org1/repoa";

    let idx_digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let single_digest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let cfg_digest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    // Tags.
    let tags_dir = fs_root
        .join("repos")
        .join("org1")
        .join("repoa")
        .join("tags");
    let _ = std::fs::create_dir_all(&tags_dir);
    std::fs::write(tags_dir.join("multi"), format!("{idx_digest}\n")).unwrap();
    std::fs::write(tags_dir.join("single"), format!("{single_digest}\n")).unwrap();

    // Manifests.
    let manifests_dir = fs_root
        .join("repos")
        .join("org1")
        .join("repoa")
        .join("manifests");
    let _ = std::fs::create_dir_all(&manifests_dir);

    let idx_hex = idx_digest.split_once(':').unwrap().1;
    let single_hex = single_digest.split_once(':').unwrap().1;

    let idx_manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                "size": 1,
                "platform": {"os": "linux", "architecture": "amd64"}
            },
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                "size": 1,
                "platform": {"os": "linux", "architecture": "arm64"}
            }
        ]
    });
    std::fs::write(
        manifests_dir.join(idx_hex),
        serde_json::to_vec(&idx_manifest).unwrap(),
    )
    .unwrap();

    let single_manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": cfg_digest,
            "size": 123
        },
        "layers": []
    });
    std::fs::write(
        manifests_dir.join(single_hex),
        serde_json::to_vec(&single_manifest).unwrap(),
    )
    .unwrap();

    // Config blob for single-manifest platform inference.
    let cfg_hex = cfg_digest.split_once(':').unwrap().1;
    let cfg_prefix2 = &cfg_hex[..2];
    let cfg_blob_dir = fs_root.join("blobs").join("sha256").join(cfg_prefix2);
    let _ = std::fs::create_dir_all(&cfg_blob_dir);
    let cfg_json = serde_json::json!({
        "architecture": "amd64",
        "os": "linux"
    });
    std::fs::write(
        cfg_blob_dir.join(cfg_hex),
        serde_json::to_vec(&cfg_json).unwrap(),
    )
    .unwrap();

    let mut cfg = minimal_config_for_token_tests();
    cfg.fs_root = fs_root.clone();
    cfg.catalog_requires_auth = false;
    let cfg = Arc::new(cfg);

    let storage = Arc::new(crate::storage::fs::FsStorage::new(
        cfg.fs_root.clone(),
        cfg.max_upload_bytes,
    ));

    let state = test_app_state(cfg, storage, None);

    let mut q = std::collections::HashMap::new();
    q.insert("include_platforms".to_string(), "1".to_string());

    let resp = meta_catalog(State(state), HeaderMap::new(), axum::extract::Query(q)).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let repos = v.get("repositories").and_then(|x| x.as_array()).unwrap();
    assert_eq!(repos.len(), 1);
    let repo0 = repos[0].as_object().unwrap();
    assert_eq!(repo0.get("name").and_then(|x| x.as_str()), Some(repo));
    assert_eq!(repo0.get("tag_count").and_then(|x| x.as_u64()), Some(2));

    let tag_details = repo0.get("tag_details").and_then(|x| x.as_array()).unwrap();
    assert_eq!(tag_details.len(), 2);

    let mut by_tag: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for d in tag_details {
        let tag = d.get("tag").and_then(|x| x.as_str()).unwrap().to_string();
        let plats = d
            .get("platforms")
            .and_then(|x| x.as_array())
            .unwrap()
            .iter()
            .filter_map(|x| x.as_str().map(|s| s.to_string()))
            .collect::<Vec<_>>();
        by_tag.insert(tag, plats);
    }

    assert_eq!(
        by_tag.get("multi").cloned().unwrap(),
        vec!["linux/amd64".to_string(), "linux/arm64".to_string()]
    );
    assert_eq!(
        by_tag.get("single").cloned().unwrap(),
        vec!["linux/amd64".to_string()]
    );

    let _ = std::fs::remove_dir_all(&fs_root);
}
use crate::config::{Config, ProxyConfig, ProxyMode, RobotsConfig, UploadPolicyConfig};
use crate::rbac::Grant;
use crate::robot_secrets;
use crate::security;
use std::net::SocketAddr;
use std::path::PathBuf;

#[test]
fn repo_name_validation() {
    assert!(is_valid_repo_name("library/alpine"));
    assert!(is_valid_repo_name("org.name/repo_name-1"));
    assert!(!is_valid_repo_name(""));
    assert!(!is_valid_repo_name("/leading"));
    assert!(!is_valid_repo_name(".."));
    assert!(!is_valid_repo_name("a/../b"));
    assert!(!is_valid_repo_name("a b"));
    assert!(!is_valid_repo_name("INVALID/UPPERCASE"));
    assert!(!is_valid_repo_name("-invalid-leading-dash"));
    assert!(is_valid_repo_name("valid__double_underscore"));
    assert!(!is_valid_repo_name("invalid___triple_underscore"));
    assert!(!is_valid_repo_name("invalid..dots"));
}

#[test]
fn tag_validation() {
    assert!(is_valid_tag("latest"));
    assert!(is_valid_tag("v1.2.3"));
    assert!(is_valid_tag("_start_ok"));
    assert!(!is_valid_tag(""));
    assert!(!is_valid_tag("has space"));
    assert!(!is_valid_tag("has/slash"));
    assert!(!is_valid_tag("-badstart"));
}

#[test]
fn parse_scopes_normalizes_typ_and_actions() {
    let scopes = parse_scopes("RePoSiToRy:org/repo:PUSH,Pull");
    assert_eq!(scopes.len(), 1);
    assert_eq!(scopes[0].typ, "repository");
    assert_eq!(scopes[0].name, "org/repo");
    assert_eq!(
        scopes[0].actions,
        vec!["push".to_string(), "pull".to_string()]
    );
}

#[test]
fn parse_scopes_splits_by_whitespace_into_multiple_items() {
    let scopes = parse_scopes("repository:org/repo:pull  repository:org/repo2:push");
    assert_eq!(scopes.len(), 2);
    assert_eq!(scopes[0].typ, "repository");
    assert_eq!(scopes[0].name, "org/repo");
    assert_eq!(scopes[0].actions, vec!["pull".to_string()]);
    assert_eq!(scopes[1].name, "org/repo2");
    assert_eq!(scopes[1].actions, vec!["push".to_string()]);
}

#[test]
fn sanitize_token_scopes_preserves_catalog_and_repository_scopes() {
    let scopes = parse_scopes("registry:catalog:* repository:org/repo:pull");
    let token_scopes = sanitize_token_scopes(&scopes);
    assert_eq!(token_scopes.len(), 2);
    assert_eq!(token_scopes[0].typ, "registry");
    assert_eq!(token_scopes[0].name, "catalog");
    assert_eq!(token_scopes[1].typ, "repository");
    assert_eq!(token_scopes[1].name, "org/repo");
}

#[test]
fn service_param_validation_allows_missing_and_requires_exact_match() {
    assert!(service_param_is_valid(None, "registry"));
    assert!(service_param_is_valid(Some("registry"), "registry"));
    assert!(!service_param_is_valid(Some("other"), "registry"));
}

#[test]
fn parse_scopes_deduplicates_actions_preserving_order() {
    let scopes = parse_scopes("repository:org/repo:pull,pull,push,pull");
    assert_eq!(scopes.len(), 1);
    assert_eq!(
        scopes[0].actions,
        vec!["pull".to_string(), "push".to_string()]
    );
}

#[test]
fn scope_requests_repo_action_requires_repository_type() {
    let scopes = parse_scopes("registry:catalog:* repository:org/repo:pull");
    assert_eq!(scopes.len(), 2);

    let token_scopes = sanitize_token_scopes(&scopes);
    assert_eq!(token_scopes.len(), 2);

    assert!(!token_scope_requests_repo_action(
        &token_scopes[0],
        security::RepoAction::Push
    ));
}

#[test]
fn sanitize_token_scopes_drops_unknown_actions() {
    let scopes = parse_scopes("repository:org/repo:pull,unknown,push");
    let token_scopes = sanitize_token_scopes(&scopes);
    assert_eq!(token_scopes.len(), 1);
    assert_eq!(
        token_scopes[0].actions,
        vec!["pull".to_string(), "push".to_string()]
    );
}

#[test]
fn sanitize_token_scopes_preserves_delete_action() {
    let scopes = parse_scopes("repository:org/repo:pull,push,delete");
    let token_scopes = sanitize_token_scopes(&scopes);
    assert_eq!(token_scopes.len(), 1);
    assert_eq!(
        token_scopes[0].actions,
        vec!["pull".to_string(), "push".to_string(), "delete".to_string()]
    );
}

#[test]
fn sanitize_token_scopes_drops_empty_scopes() {
    let scopes = parse_scopes("repository:org/repo:unknown");
    let token_scopes = sanitize_token_scopes(&scopes);
    assert!(token_scopes.is_empty());
}

#[test]
fn sanitize_token_scopes_drops_empty_repo_names() {
    let scopes = parse_scopes("repository::pull");
    let token_scopes = sanitize_token_scopes(&scopes);
    assert!(token_scopes.is_empty());
}

#[test]
fn parse_scopes_ignores_whitespace_only() {
    let scopes = parse_scopes("   \t  ");
    assert!(scopes.is_empty());
}

#[test]
fn sanitize_token_scopes_drops_empty_action_lists() {
    let scopes = parse_scopes("repository:org/repo:");
    let token_scopes = sanitize_token_scopes(&scopes);
    assert!(token_scopes.is_empty());
}

#[test]
fn wants_push_ignores_unknown_only_actions() {
    let scopes = parse_scopes("repository:org/repo:delete");
    let token_scopes = sanitize_token_scopes(&scopes);
    assert!(!wants_push_from_token_scopes(&token_scopes));
}

#[test]
fn wants_push_true_only_when_repository_push_present() {
    let scopes = parse_scopes(
        "repository:org/repo:pull repository:org/repo2:pull,push registry:catalog:*:push",
    );
    let token_scopes = sanitize_token_scopes(&scopes);

    assert!(wants_push_from_token_scopes(&token_scopes));
}

#[test]
fn wants_push_false_for_repository_pull_only() {
    let scopes = parse_scopes("repository:org/repo:pull registry:catalog:*:push");
    let token_scopes = sanitize_token_scopes(&scopes);

    assert!(!wants_push_from_token_scopes(&token_scopes));
}

fn minimal_config_for_token_tests() -> Config {
    Config {
        listen_addr: SocketAddr::from(([127, 0, 0, 1], 5000)),
        tls_cert_path: None,
        tls_key_path: None,
        tls_acme: None,
        auth_strategy: crate::config::AuthStrategy::Token,
        anonymous_pull: true,
        push_username: None,
        push_password: None,
        push_allow_repos: Some(vec![crate::registry::RepositoryAccessPattern::All]),
        push_actions: vec!["pull".to_string(), "push".to_string()],
        push_implies_delete: false,
        storage_backend: crate::config::StorageBackend::Filesystem,
        fs_root: PathBuf::from("./data"),
        fs_manifest_listing_max_entries:
            crate::storage::fs::manifest_listing::DEFAULT_MANIFEST_LISTING_MAX_ENTRIES,
        fs_manifest_listing_max_name_bytes:
            crate::storage::fs::manifest_listing::DEFAULT_MANIFEST_LISTING_MAX_NAME_BYTES,
        s3_endpoint: None,
        s3_region: None,
        s3_bucket: None,
        s3_prefix: "registry".to_string(),
        s3_single_instance_mode: false,
        s3_lease_duration_secs: 60,
        s3_lease_renewal_interval_secs: 20,
        s3_max_retry_attempts: 3,
        s3_legacy_multipart_cleanup_policy: crate::config::LegacyMultipartCleanupPolicy::Disabled,
        upload_receipt_lifetime_secs: 86400,
        gc_pin_duration_secs: 1800,
        ref_index: crate::config::RefIndexConfig {
            enabled: true,
            path: PathBuf::from("./data/ref-index"),
            rebuild_on_start: false,
            auto_rebuild_on_corruption: true,
        },
        allow_tag_overwrite: true,
        automatic_crossmount: false,
        upload_gc_enabled: true,
        upload_gc_interval_secs: 3600,
        upload_gc_max_age_secs: 86400,
        blob_gc_finalize_grace_secs: 72 * 3600,
        blob_gc_enabled: true,
        blob_gc_enable_delete: true,
        blob_gc_default_min_age_secs: 7 * 24 * 3600,
        blob_gc_default_quarantine_delay_secs: 24 * 3600,
        blob_gc_default_max_blobs: 1000,
        blob_gc_default_max_bytes: u64::MAX,
        blob_gc_default_max_seconds: 60,
        blob_gc_schedule_enabled: false,
        blob_gc_schedule_interval_secs: 7 * 24 * 3600,
        admin_api: crate::config::AdminApiConfig {
            enabled: false,
            username: None,
            password: None,
        },
        max_upload_bytes: 5 * 1024 * 1024 * 1024,
        max_request_body_bytes: 32 * 1024 * 1024,
        upload_chunk_min_bytes: None,
        max_concurrent_buffered_requests: 8,
        max_concurrent_requests: 256,
        max_concurrent_upload_requests: 256,
        request_timeout_secs: 300,
        upload_request_timeout_secs: 3600,
        upload_chunk_idle_timeout_secs: 20,
        upload_rate_window_secs: 10,
        upload_rate_grace_period_secs: 15,
        min_upload_bytes_per_sec: 32768,
        header_read_timeout_secs: 10,
        slow_connection_policy: crate::config::SlowConnectionPolicy::Enforce,
        max_connections_per_ip: 50,
        trusted_bypass_cidrs: vec![],
        trusted_proxies: vec![],
        disallow_monolithic_uploads: false,
        upload_policy: UploadPolicyConfig {
            abort_on_error: false,
            abort_on_digest_mismatch: false,
            repo_rules: Vec::new(),
        },
        catalog_requires_auth: false,
        public_url: Some("http://127.0.0.1:5000".to_string()),
        token_service: "registry-rust".to_string(),
        token_signing_key: "test-key".to_string(),
        token_signing_keys: vec![security::TokenSigningKey {
            kid: "default".to_string(),
            key: "test-key".to_string(),
        }],
        token_ttl_secs: 600,
        robots: RobotsConfig::default(),
        users: crate::config::UsersConfig::default(),
        proxy: ProxyConfig {
            enabled: false,
            mode: ProxyMode::Allowlist,
            upstream_base_url: None,
            upstream_username: None,
            upstream_password: None,
            allowed_upstream_hosts: Vec::new(),
            allowed_repo_prefixes: Vec::new(),
            block_private_networks: true,
            redirect_policy: crate::config::RedirectPolicy::AnyPublic,
            max_concurrent_upstream: 16,
            index_path: PathBuf::from("./data/cache/proxy-index"),
            cache_fs_root: None,
            cache_s3_prefix: None,
            gc_interval_secs: 3600,
            scrub_enabled: false,
            scrub_interval_secs: 3600,
            scrub_max_files_per_run: 2000,
            max_cache_bytes: None,
            repo_rules: Vec::new(),
            upstreams: Vec::new(),
            routing_proxy_hosts: Vec::new(),
            routing_trust_x_forwarded_host: false,
        },
    }
}

#[test]
fn token_primary_signing_key_follows_key_order() {
    let mut cfg = minimal_config_for_token_tests();

    cfg.token_signing_keys = vec![
        security::TokenSigningKey {
            kid: "k_new".to_string(),
            key: "new-key".to_string(),
        },
        security::TokenSigningKey {
            kid: "k_old".to_string(),
            key: "old-key".to_string(),
        },
    ];
    assert_eq!(cfg.token_primary_signing_key().kid, "k_new");

    cfg.token_signing_keys.swap(0, 1);
    assert_eq!(cfg.token_primary_signing_key().kid, "k_old");
}

#[test]
fn robot_push_token_is_scoped_by_prefix_grants() {
    let mut cfg = minimal_config_for_token_tests();
    cfg.robots.enabled = true;

    let hash = robot_secrets::hash_robot_secret("s3cr3t").expect("hash");
    cfg.robots.accounts.push(crate::config::RobotAccountConfig {
        name: "ci".to_string(),
        secret_hash: hash,
        grants: vec![Grant::try_new("org/", vec!["pull".to_string(), "push".to_string()]).unwrap()],
        max_ttl_secs: Some(120),
    });

    let requested = vec![security::TokenScope {
        typ: "repository".to_string(),
        name: "org/repo".to_string(),
        actions: vec!["pull".to_string(), "push".to_string()],
    }];

    let decision = decide_token_scopes_for_request(
        &cfg,
        &requested,
        Some(("ci".to_string(), "s3cr3t".to_string())),
    )
    .expect("should authorize");

    assert_eq!(decision.subject.as_deref(), Some("robot:ci"));
    assert_eq!(decision.scopes, requested);
    assert_eq!(decision.ttl_secs, 120);
}

#[test]
fn robot_push_token_denied_when_repo_not_in_grants() {
    let mut cfg = minimal_config_for_token_tests();
    cfg.robots.enabled = true;

    let hash = robot_secrets::hash_robot_secret("s3cr3t").expect("hash");
    cfg.robots.accounts.push(crate::config::RobotAccountConfig {
        name: "ci".to_string(),
        secret_hash: hash,
        grants: vec![Grant::try_new("org/", vec!["pull".to_string(), "push".to_string()]).unwrap()],
        max_ttl_secs: None,
    });

    let requested = vec![security::TokenScope {
        typ: "repository".to_string(),
        name: "other/repo".to_string(),
        actions: vec!["pull".to_string(), "push".to_string()],
    }];

    let err = decide_token_scopes_for_request(
        &cfg,
        &requested,
        Some(("ci".to_string(), "s3cr3t".to_string())),
    )
    .expect_err("should deny");

    assert_eq!(
        err,
        TokenRejection::Denied("action not allowed by robot policy")
    );
}

#[test]
fn robot_auth_failure_does_not_allow_push_without_legacy_creds() {
    let mut cfg = minimal_config_for_token_tests();
    cfg.robots.enabled = true;

    let hash = robot_secrets::hash_robot_secret("s3cr3t").expect("hash");
    cfg.robots.accounts.push(crate::config::RobotAccountConfig {
        name: "ci".to_string(),
        secret_hash: hash,
        grants: vec![Grant::try_new("org/", vec!["pull".to_string(), "push".to_string()]).unwrap()],
        max_ttl_secs: None,
    });

    let requested = vec![security::TokenScope {
        typ: "repository".to_string(),
        name: "org/repo".to_string(),
        actions: vec!["pull".to_string(), "push".to_string()],
    }];

    let err = decide_token_scopes_for_request(
        &cfg,
        &requested,
        Some(("ci".to_string(), "wrong".to_string())),
    )
    .expect_err("should deny");

    assert_eq!(err, TokenRejection::Unauthorized);
}

#[test]
fn legacy_push_creds_work_even_when_robots_enabled() {
    let mut cfg = minimal_config_for_token_tests();
    cfg.robots.enabled = true;
    cfg.push_username = Some("admin".to_string());
    cfg.push_password = Some("pw".to_string());

    // Add a robot too; we should still allow legacy when legacy creds match.
    let hash = robot_secrets::hash_robot_secret("s3cr3t").expect("hash");
    cfg.robots.accounts.push(crate::config::RobotAccountConfig {
        name: "ci".to_string(),
        secret_hash: hash,
        grants: vec![Grant::try_new("org/", vec!["pull".to_string(), "push".to_string()]).unwrap()],
        max_ttl_secs: None,
    });

    let requested = vec![security::TokenScope {
        typ: "repository".to_string(),
        name: "org/repo".to_string(),
        actions: vec!["pull".to_string(), "push".to_string()],
    }];

    let decision = decide_token_scopes_for_request(
        &cfg,
        &requested,
        Some(("admin".to_string(), "pw".to_string())),
    )
    .expect("should authorize");

    assert_eq!(decision.subject.as_deref(), Some("admin"));
    assert_eq!(decision.scopes, requested);
}

#[test]
fn user_push_token_is_scoped_by_group_grants() {
    let mut cfg = minimal_config_for_token_tests();
    cfg.users.enabled = true;

    cfg.users.groups.push(crate::config::GroupConfig {
        name: "dev".to_string(),
        grants: vec![Grant::try_new("org/", vec!["pull".to_string(), "push".to_string()]).unwrap()],
    });

    let hash = robot_secrets::hash_robot_secret("pw").expect("hash");
    cfg.users.accounts.push(crate::config::UserAccountConfig {
        name: "alice".to_string(),
        secret_hash: hash,
        groups: vec!["dev".to_string()],
        max_ttl_secs: Some(120),
    });

    let requested = vec![security::TokenScope {
        typ: "repository".to_string(),
        name: "org/repo".to_string(),
        actions: vec!["pull".to_string(), "push".to_string()],
    }];

    let decision = decide_token_scopes_for_request(
        &cfg,
        &requested,
        Some(("alice".to_string(), "pw".to_string())),
    )
    .expect("should authorize");

    assert_eq!(decision.subject.as_deref(), Some("user:alice"));
    assert_eq!(decision.scopes, requested);
    assert_eq!(decision.ttl_secs, 120);
}

#[test]
fn user_push_token_denied_when_repo_not_in_group_grants() {
    let mut cfg = minimal_config_for_token_tests();
    cfg.users.enabled = true;

    cfg.users.groups.push(crate::config::GroupConfig {
        name: "dev".to_string(),
        grants: vec![
            Grant::try_new("other/", vec!["pull".to_string(), "push".to_string()]).unwrap(),
        ],
    });

    let hash = robot_secrets::hash_robot_secret("pw").expect("hash");
    cfg.users.accounts.push(crate::config::UserAccountConfig {
        name: "bob".to_string(),
        secret_hash: hash,
        groups: vec!["dev".to_string()],
        max_ttl_secs: None,
    });

    let requested = vec![security::TokenScope {
        typ: "repository".to_string(),
        name: "org/repo".to_string(),
        actions: vec!["pull".to_string(), "push".to_string()],
    }];

    let err = decide_token_scopes_for_request(
        &cfg,
        &requested,
        Some(("bob".to_string(), "pw".to_string())),
    )
    .expect_err("should deny");

    assert_eq!(
        err,
        TokenRejection::Denied("action not allowed by user policy")
    );
}

#[test]
fn robot_precedence_on_name_collision_denies_even_if_user_would_allow() {
    let mut cfg = minimal_config_for_token_tests();
    cfg.robots.enabled = true;
    cfg.users.enabled = true;

    // Robot has the colliding name and valid creds but does NOT allow this repo.
    let shared_hash = robot_secrets::hash_robot_secret("pw").expect("hash");
    cfg.robots.accounts.push(crate::config::RobotAccountConfig {
        name: "sam".to_string(),
        secret_hash: shared_hash.clone(),
        grants: vec![Grant::try_new("org/", vec!["pull".to_string(), "push".to_string()]).unwrap()],
        max_ttl_secs: None,
    });

    // User would allow it via group grants, but must not be reached.
    cfg.users.groups.push(crate::config::GroupConfig {
        name: "writers".to_string(),
        grants: vec![
            Grant::try_new("other/", vec!["pull".to_string(), "push".to_string()]).unwrap(),
        ],
    });
    cfg.users.accounts.push(crate::config::UserAccountConfig {
        name: "sam".to_string(),
        secret_hash: shared_hash,
        groups: vec!["writers".to_string()],
        max_ttl_secs: None,
    });

    let requested = vec![security::TokenScope {
        typ: "repository".to_string(),
        name: "other/repo".to_string(),
        actions: vec!["pull".to_string(), "push".to_string()],
    }];

    let err = decide_token_scopes_for_request(
        &cfg,
        &requested,
        Some(("sam".to_string(), "pw".to_string())),
    )
    .expect_err("should deny");

    assert_eq!(
        err,
        TokenRejection::Denied("action not allowed by robot policy")
    );
}

use sha2::Digest as _;

async fn setup_upload_test_env() -> (
    AppState,
    tempfile::TempDir,
    String,
    Arc<crate::storage::fs::FsStorage>,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cfg = minimal_config_for_token_tests();
    cfg.fs_root = temp_dir.path().to_path_buf();
    cfg.token_signing_keys = vec![crate::security::TokenSigningKey {
        kid: "default".to_string(),
        key: "test-signing-key".to_string(),
    }];
    let cfg = Arc::new(cfg);
    let storage = Arc::new(crate::storage::fs::FsStorage::new(
        cfg.fs_root.clone(),
        cfg.max_upload_bytes,
    ));
    let upload = storage.create_upload().await.unwrap();
    let state = test_app_state(cfg, storage.clone(), None);
    (state, temp_dir, upload.uuid, storage)
}

#[tokio::test]
async fn test_handler_put_finalize_with_valid_state_accepted() {
    let (state, _tmp, uuid, storage) = setup_upload_test_env().await;
    let repo = "library/test";
    let chunk = b"chunk of 1000 bytes";
    storage
        .append_upload(&uuid, bytes::Bytes::from_static(chunk))
        .await
        .unwrap();

    let key = b"test-signing-key";
    let state_token =
        crate::http_api::upload_state::UploadStateData::new(repo, &uuid, chunk.len() as u64)
            .encode_and_sign(key);

    let mut hasher = sha2::Sha256::new();
    sha2::Digest::update(&mut hasher, chunk);
    let digest = format!("sha256:{}", hex::encode(hasher.finalize()));

    let mut query = std::collections::HashMap::new();
    query.insert("digest".to_string(), digest);
    query.insert("_state".to_string(), state_token);

    let resp = super::upload_session(
        state,
        axum::http::Method::PUT,
        &HeaderMap::new(),
        repo,
        &uuid,
        query,
        axum::body::Body::empty(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_handler_put_finalize_with_stale_offset_rejected() {
    let (state, _tmp, uuid, storage) = setup_upload_test_env().await;
    let repo = "library/test";
    let chunk = b"chunk of 1000 bytes";
    storage
        .append_upload(&uuid, bytes::Bytes::from_static(chunk))
        .await
        .unwrap();

    let key = b"test-signing-key";
    // Stale offset 0 when stored offset is 20
    let state_token =
        crate::http_api::upload_state::UploadStateData::new(repo, &uuid, 0).encode_and_sign(key);

    let mut hasher = sha2::Sha256::new();
    sha2::Digest::update(&mut hasher, chunk);
    let digest = format!("sha256:{}", hex::encode(hasher.finalize()));

    let mut query = std::collections::HashMap::new();
    query.insert("digest".to_string(), digest);
    query.insert("_state".to_string(), state_token);

    let resp = super::upload_session(
        state,
        axum::http::Method::PUT,
        &HeaderMap::new(),
        repo,
        &uuid,
        query,
        axum::body::Body::empty(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
}

#[tokio::test]
async fn test_handler_put_finalize_with_wrong_uuid_rejected() {
    let (state, _tmp, uuid, storage) = setup_upload_test_env().await;
    let repo = "library/test";
    let chunk = b"chunk of 1000 bytes";
    storage
        .append_upload(&uuid, bytes::Bytes::from_static(chunk))
        .await
        .unwrap();

    let key = b"test-signing-key";
    let state_token = crate::http_api::upload_state::UploadStateData::new(
        repo,
        "00000000-0000-0000-0000-000000000000",
        chunk.len() as u64,
    )
    .encode_and_sign(key);

    let mut hasher = sha2::Sha256::new();
    sha2::Digest::update(&mut hasher, chunk);
    let digest = format!("sha256:{}", hex::encode(hasher.finalize()));

    let mut query = std::collections::HashMap::new();
    query.insert("digest".to_string(), digest);
    query.insert("_state".to_string(), state_token);

    let resp = super::upload_session(
        state,
        axum::http::Method::PUT,
        &HeaderMap::new(),
        repo,
        &uuid,
        query,
        axum::body::Body::empty(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_handler_put_finalize_with_wrong_repo_rejected() {
    let (state, _tmp, uuid, storage) = setup_upload_test_env().await;
    let repo = "library/test";
    let chunk = b"chunk of 1000 bytes";
    storage
        .append_upload(&uuid, bytes::Bytes::from_static(chunk))
        .await
        .unwrap();

    let key = b"test-signing-key";
    let state_token = crate::http_api::upload_state::UploadStateData::new(
        "library/other-repo",
        &uuid,
        chunk.len() as u64,
    )
    .encode_and_sign(key);

    let mut hasher = sha2::Sha256::new();
    sha2::Digest::update(&mut hasher, chunk);
    let digest = format!("sha256:{}", hex::encode(hasher.finalize()));

    let mut query = std::collections::HashMap::new();
    query.insert("digest".to_string(), digest);
    query.insert("_state".to_string(), state_token);

    let resp = super::upload_session(
        state,
        axum::http::Method::PUT,
        &HeaderMap::new(),
        repo,
        &uuid,
        query,
        axum::body::Body::empty(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_handler_put_finalize_with_tampered_sig_rejected() {
    let (state, _tmp, uuid, storage) = setup_upload_test_env().await;
    let repo = "library/test";
    let chunk = b"chunk of 1000 bytes";
    storage
        .append_upload(&uuid, bytes::Bytes::from_static(chunk))
        .await
        .unwrap();

    let mut hasher = sha2::Sha256::new();
    sha2::Digest::update(&mut hasher, chunk);
    let digest = format!("sha256:{}", hex::encode(hasher.finalize()));

    let mut query = std::collections::HashMap::new();
    query.insert("digest".to_string(), digest);
    query.insert("_state".to_string(), "invalid.signature_data".to_string());

    let resp = super::upload_session(
        state,
        axum::http::Method::PUT,
        &HeaderMap::new(),
        repo,
        &uuid,
        query,
        axum::body::Body::empty(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_handler_put_finalize_missing_state_accepted_for_monolithic() {
    let (state, _tmp, uuid, _storage) = setup_upload_test_env().await;
    let repo = "library/test";
    let chunk = b"monolithic upload bytes";

    let mut hasher = sha2::Sha256::new();
    sha2::Digest::update(&mut hasher, chunk);
    let digest = format!("sha256:{}", hex::encode(hasher.finalize()));

    let mut query = std::collections::HashMap::new();
    query.insert("digest".to_string(), digest);
    // No _state query param

    let resp = super::upload_session(
        state,
        axum::http::Method::PUT,
        &HeaderMap::new(),
        repo,
        &uuid,
        query,
        axum::body::Body::from(chunk.to_vec()),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_handler_put_finalize_with_final_body_pre_append_offset_validated() {
    let (state, _tmp, uuid, storage) = setup_upload_test_env().await;
    let repo = "library/test";
    let chunk1 = b"chunk1-bytes-";
    let chunk2 = b"chunk2-bytes-final";
    storage
        .append_upload(&uuid, bytes::Bytes::from_static(chunk1))
        .await
        .unwrap();

    let key = b"test-signing-key";
    // Pre-append offset is chunk1.len()
    let state_token =
        crate::http_api::upload_state::UploadStateData::new(repo, &uuid, chunk1.len() as u64)
            .encode_and_sign(key);

    let mut total_bytes = chunk1.to_vec();
    total_bytes.extend_from_slice(chunk2);
    let mut hasher = sha2::Sha256::new();
    sha2::Digest::update(&mut hasher, &total_bytes);
    let digest = format!("sha256:{}", hex::encode(hasher.finalize()));

    let mut query = std::collections::HashMap::new();
    query.insert("digest".to_string(), digest);
    query.insert("_state".to_string(), state_token);

    let resp = super::upload_session(
        state,
        axum::http::Method::PUT,
        &HeaderMap::new(),
        repo,
        &uuid,
        query,
        axum::body::Body::from(chunk2.to_vec()),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_handler_patch_missing_state_rejected() {
    let (state, _tmp, uuid, _storage) = setup_upload_test_env().await;
    let repo = "library/test";

    let resp = super::upload_session(
        state,
        axum::http::Method::PATCH,
        &HeaderMap::new(),
        repo,
        &uuid,
        std::collections::HashMap::new(),
        axum::body::Body::from("data"),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_handler_patch_stale_offset_rejected() {
    let (state, _tmp, uuid, storage) = setup_upload_test_env().await;
    let repo = "library/test";
    storage
        .append_upload(&uuid, bytes::Bytes::from_static(b"existing-1000"))
        .await
        .unwrap();

    let key = b"test-signing-key";
    let state_token =
        crate::http_api::upload_state::UploadStateData::new(repo, &uuid, 0).encode_and_sign(key);

    let mut query = std::collections::HashMap::new();
    query.insert("_state".to_string(), state_token);

    let resp = super::upload_session(
        state,
        axum::http::Method::PATCH,
        &HeaderMap::new(),
        repo,
        &uuid,
        query,
        axum::body::Body::from("next-chunk"),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
}

#[tokio::test]
async fn test_handler_get_session_with_invalid_state_rejected() {
    let (state, _tmp, uuid, _storage) = setup_upload_test_env().await;
    let repo = "library/test";

    let mut query = std::collections::HashMap::new();
    query.insert("_state".to_string(), "invalid-tampered-state".to_string());

    let resp = super::upload_session(
        state,
        axum::http::Method::GET,
        &HeaderMap::new(),
        repo,
        &uuid,
        query,
        axum::body::Body::empty(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_handler_delete_session_with_invalid_state_rejected() {
    let (state, _tmp, uuid, _storage) = setup_upload_test_env().await;
    let repo = "library/test";

    let mut query = std::collections::HashMap::new();
    query.insert("_state".to_string(), "invalid-tampered-state".to_string());

    let resp = super::upload_session(
        state,
        axum::http::Method::DELETE,
        &HeaderMap::new(),
        repo,
        &uuid,
        query,
        axum::body::Body::empty(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_manifest_put_rejects_malformed_layer_digest() {
    let fs_root = std::env::temp_dir().join(format!("registry-rust-test-{}", uuid::Uuid::new_v4()));
    let _ = std::fs::create_dir_all(&fs_root);
    let mut cfg = minimal_config_for_token_tests();
    cfg.fs_root = fs_root.clone();
    let cfg = Arc::new(cfg);
    let storage = Arc::new(crate::storage::fs::FsStorage::new(
        cfg.fs_root.clone(),
        cfg.max_upload_bytes,
    ));
    let state = test_app_state(cfg, storage, None);
    let repo = "library/malformed";

    let malformed_manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
            "size": 2
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": "sha256:not-valid-hex-digest",
                "size": 100
            }
        ]
    });
    let bytes = serde_json::to_vec(&malformed_manifest).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        "application/vnd.oci.image.manifest.v1+json"
            .parse()
            .unwrap(),
    );
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        bytes.len().to_string().parse().unwrap(),
    );

    let resp = super::manifest_put(
        state,
        &headers,
        repo,
        "latest",
        axum::body::Body::from(bytes),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body_bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let err_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(err_json["errors"][0]["code"], "MANIFEST_INVALID");

    let _ = std::fs::remove_dir_all(&fs_root);
}

#[tokio::test]
async fn test_manifest_put_rejects_malformed_config_structure() {
    let fs_root = std::env::temp_dir().join(format!("registry-rust-test-{}", uuid::Uuid::new_v4()));
    let _ = std::fs::create_dir_all(&fs_root);
    let mut cfg = minimal_config_for_token_tests();
    cfg.fs_root = fs_root.clone();
    let cfg = Arc::new(cfg);
    let storage = Arc::new(crate::storage::fs::FsStorage::new(
        cfg.fs_root.clone(),
        cfg.max_upload_bytes,
    ));
    let state = test_app_state(cfg, storage, None);
    let repo = "library/malformed-cfg";

    let malformed_manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": "not_an_object",
        "layers": []
    });
    let bytes = serde_json::to_vec(&malformed_manifest).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        "application/vnd.oci.image.manifest.v1+json"
            .parse()
            .unwrap(),
    );
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        bytes.len().to_string().parse().unwrap(),
    );

    let resp = super::manifest_put(
        state,
        &headers,
        repo,
        "latest",
        axum::body::Body::from(bytes),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body_bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let err_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(err_json["errors"][0]["code"], "MANIFEST_INVALID");

    let _ = std::fs::remove_dir_all(&fs_root);
}

#[tokio::test]
async fn test_repo_named_quota_or_limited_behaves_normally() {
    let fs_root = std::env::temp_dir().join(format!("registry-rust-test-{}", uuid::Uuid::new_v4()));
    let _ = std::fs::create_dir_all(&fs_root);
    let mut cfg = minimal_config_for_token_tests();
    cfg.fs_root = fs_root.clone();
    cfg.max_upload_bytes = 10_000_000; // 10 MB limit
    let cfg = Arc::new(cfg);
    let storage = Arc::new(crate::storage::fs::FsStorage::new(
        cfg.fs_root.clone(),
        cfg.max_upload_bytes,
    ));
    let state = test_app_state(cfg, storage, None);

    // 1. Repo containing "quota" with 2.5 MB (> old 1 MB test-hook) succeeds (202 Accepted)
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        "2500000".parse().unwrap(),
    );
    let query = std::collections::HashMap::new();

    let resp_quota = super::upload_create(
        state.clone(),
        &headers,
        axum::http::Method::POST,
        "quota-app",
        &query,
        axum::body::Body::empty(),
    )
    .await;
    assert_eq!(resp_quota.status(), StatusCode::ACCEPTED);

    // 2. Repo containing "limited" with 2.5 MB succeeds (202 Accepted)
    let resp_limited = super::upload_create(
        state.clone(),
        &headers,
        axum::http::Method::POST,
        "limited-repo",
        &query,
        axum::body::Body::empty(),
    )
    .await;
    assert_eq!(resp_limited.status(), StatusCode::ACCEPTED);

    // 3. Exceeding configured max_upload_bytes (e.g. 15 MB > 10 MB) fails with 413 Payload Too Large
    let mut headers_oversize = HeaderMap::new();
    headers_oversize.insert(
        axum::http::header::CONTENT_LENGTH,
        "15000000".parse().unwrap(),
    );
    let resp_oversize = super::upload_create(
        state,
        &headers_oversize,
        axum::http::Method::POST,
        "quota-app",
        &query,
        axum::body::Body::empty(),
    )
    .await;
    assert_eq!(resp_oversize.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let _ = std::fs::remove_dir_all(&fs_root);
}
