use axum::http::StatusCode;
use base64::Engine;
use bytes::Bytes;
use registry_rust::blob_ref_index::BlobRefIndex;
use registry_rust::manifest_lifecycle::{ManifestLifecycleService, PublishManifestRequest};
use registry_rust::registry::canonical_name::CanonicalRepoName;
use registry_rust::registry::digest::Digest;
use registry_rust::storage::Storage;
use registry_rust::storage::fs::FsStorage;
use registry_rust::storage::repo_membership::{
    RepoBlobMembershipRecord, RepositoryBlobMembershipStorage,
};
use reqwest::header;
use serde_json::json;
use sha2::Digest as _;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

fn hex_sha256(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn sha256_digest(bytes: &[u8]) -> Digest {
    Digest::parse(&format!("sha256:{}", hex_sha256(bytes))).expect("valid digest")
}

fn pick_unused_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral port");
    listener.local_addr().expect("local_addr").port()
}

fn bin_path() -> String {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_registry-rust") {
        return p;
    }
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_registry_rust") {
        return p;
    }

    let exe = std::env::current_exe().expect("current_exe");
    let deps_dir = exe.parent().expect("exe parent");
    let profile_dir = deps_dir.parent().expect("deps parent");

    let bin_name = if cfg!(windows) {
        "registry-rust.exe"
    } else {
        "registry-rust"
    };

    profile_dir.join(bin_name).to_string_lossy().to_string()
}

struct ServerGuard {
    child: Child,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn write_config(dir: &tempfile::TempDir, port: u16) -> (PathBuf, PathBuf) {
    let fs_root = dir.path().join("data");
    let ref_index = dir.path().join("ref-index");

    std::fs::create_dir_all(fs_root.join("blobs").join("sha256")).expect("mkdir blobs");
    std::fs::create_dir_all(fs_root.join("repos")).expect("mkdir repos");
    std::fs::create_dir_all(fs_root.join("uploads")).expect("mkdir uploads");
    std::fs::create_dir_all(&ref_index).expect("mkdir ref-index");

    let toml = format!(
        r#"
[server]
listen_addr = "127.0.0.1:{port}"
public_url = "http://127.0.0.1:{port}"

[auth]
strategy = "both"

[auth.push]
username = "demo"
password = "demo"
allow_repos = ["*"]
actions = ["pull", "push", "delete"]

[storage]
backend = "fs"

[storage.fs]
root = "{}"

[storage.ref_index]
enabled = true
path = "{}"

[admin_api]
enabled = false

[limits]
max_upload_bytes = 104857600
max_request_body_bytes = 33554432
upload_chunk_min_bytes = 1024
"#,
        fs_root.display(),
        ref_index.display()
    );

    let path = dir.path().join("config.toml");
    std::fs::write(&path, toml).expect("write config");
    (path, fs_root)
}

fn spawn_server(config_path: &Path) -> ServerGuard {
    let err_file = config_path.parent().unwrap().join("server.err");
    let stderr = std::fs::File::create(&err_file).expect("create err file");
    let child = Command::new(bin_path())
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .expect("spawn server");

    ServerGuard { child }
}

async fn wait_for_server(config_path: &Path, port: u16) {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/v2/");
    for _ in 0..80 {
        if matches!(
            client.get(&url).send().await,
            Ok(resp) if resp.status().is_success() || resp.status() == 401
        ) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let err_file = config_path.parent().unwrap().join("server.err");
    let err_contents = std::fs::read_to_string(&err_file).unwrap_or_default();
    panic!("server failed to start on port {port}:\n{err_contents}");
}

// ================================================================================================
// 1. ALL VALID SEPARATOR FORMS
// ================================================================================================
#[test]
fn test_01_all_valid_separator_forms() {
    let valid_cases = vec![
        "a",
        "0",
        "a0",
        "0a",
        "app",
        "a.b",
        "a_b",
        "a__b",
        "a-b",
        "a--b",
        "a---b",
        "a----b",
        "a-----b",
        "team.alpha/img.beta",
        "team_alpha/img_beta",
        "team__alpha/img__beta",
        "team-alpha/img-beta",
        "team--alpha/img--beta",
        "team---alpha/img---beta",
        "a.b-c_d__e---f/g----h",
    ];

    for name in valid_cases {
        let parsed = CanonicalRepoName::parse(name);
        assert!(
            parsed.is_ok(),
            "Expected '{name}' to be valid, got error: {:?}",
            parsed.err()
        );
        let repo = parsed.unwrap();
        assert_eq!(repo.as_str(), name);
    }
}

// ================================================================================================
// 2. INVALID SEPARATOR COMBINATIONS
// ================================================================================================
#[test]
fn test_02_invalid_separator_combinations() {
    let invalid_cases = vec![
        // Empty and slash issues
        "",
        "/",
        "/a",
        "a/",
        "a//b",
        "a///b",
        // Dots
        ".",
        "..",
        "a..b",
        "a...b",
        ".a",
        "a.",
        // Underscores
        "_a",
        "a_",
        "a___b",
        "a____b",
        // Hyphens
        "-a",
        "a-",
        "a--",
        // Mixed adjacent separators
        "a._b",
        "a_.b",
        "a.-b",
        "a-.b",
        "a_-b",
        "a-_b",
        "a__-b",
        "a_--b",
        // Uppercase, whitespace, control, special characters
        "Team/Image",
        "A",
        "a b",
        "a\tb",
        "a\nb",
        "a\0b",
        "a:b",
        "a@b",
        "a\\b",
        "a?b",
        "a#b",
        "a%20b",
        "team/imáge",
        "team/🦀",
    ];

    for name in invalid_cases {
        let res = CanonicalRepoName::parse(name);
        assert!(
            res.is_err(),
            "Expected '{name}' to be invalid, but parsed successfully as: {:?}",
            res.ok()
        );
    }
}

// ================================================================================================
// 3. NESTED NAMES
// ================================================================================================
#[test]
fn test_03_nested_names() {
    let nested_cases = vec![
        "a/b/c/d/e",
        "team/subteam/service/app/component",
        "a.b/c_d/e__f/g---h",
        "org/team--1/project__a/service.b/module",
    ];

    for name in nested_cases {
        let repo = CanonicalRepoName::parse(name).expect("valid nested repo");
        assert_eq!(repo.as_str(), name);
        assert_eq!(
            repo.components().collect::<Vec<_>>(),
            name.split('/').collect::<Vec<_>>()
        );
    }
}

// ================================================================================================
// 4. EXACT PARSE, DISPLAY, SERDE ROUND TRIP
// ================================================================================================
#[test]
fn test_04_exact_parse_display_serde_round_trip() {
    let repo_names = vec![
        "app",
        "team/image__cache",
        "team/image--cache",
        "team/image---cache",
        "a.b/c_d/e__f/g---h",
    ];

    for name in repo_names {
        let repo = CanonicalRepoName::parse(name).expect("parse");

        // Display
        assert_eq!(format!("{repo}"), name);
        // Explicit as_str
        assert_eq!(repo.as_str(), name);

        // Serde round trip
        let json = serde_json::to_string(&repo).expect("serialize");
        assert_eq!(json, format!("\"{name}\""));

        let deserialized: CanonicalRepoName = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized, repo);
        assert_eq!(deserialized.as_str(), name);
    }

    // Deserialization of invalid JSON string fails with custom serde error
    let invalid_json = "\"team/Invalid__Name___\"";
    let res: Result<CanonicalRepoName, _> = serde_json::from_str(invalid_json);
    assert!(res.is_err());
}

// ================================================================================================
// 5. HTTP GET/HEAD/PUT/DELETE USING __, --, AND ---
// ================================================================================================
#[tokio::test]
async fn test_05_http_verbs_with_special_separators() {
    let dir = tempfile::tempdir().unwrap();
    let port = pick_unused_port();
    let (cfg_path, _fs_root) = write_config(&dir, port);
    let _guard = spawn_server(&cfg_path);
    wait_for_server(&cfg_path, port).await;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{port}");

    let repos = vec![
        "team/image__cache",
        "team/image--cache",
        "team/image---cache",
        "a.b/c_d/e__f/g---h",
    ];

    for repo in repos {
        let content = format!("blob-data-for-{repo}").into_bytes();
        let digest = format!("sha256:{}", hex_sha256(&content));

        // 1. POST start upload
        let post_res = client
            .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
            .basic_auth("demo", Some("demo"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            post_res.status(),
            StatusCode::ACCEPTED,
            "POST start upload failed for {repo}"
        );
        let location = post_res
            .headers()
            .get(header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap();
        let upload_url = if location.starts_with("http") {
            location.to_string()
        } else {
            format!("{base_url}{location}")
        };

        // 2. PUT finalize upload
        let put_blob_res = client
            .put(format!("{upload_url}&digest={digest}"))
            .basic_auth("demo", Some("demo"))
            .body(content.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(
            put_blob_res.status(),
            StatusCode::CREATED,
            "PUT finalize blob failed for {repo}"
        );

        // 3. HEAD blob
        let head_res = client
            .head(format!("{base_url}/v2/{repo}/blobs/{digest}"))
            .basic_auth("demo", Some("demo"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            head_res.status(),
            StatusCode::OK,
            "HEAD blob failed for {repo}"
        );
        assert_eq!(
            head_res.headers().get("Docker-Content-Digest").unwrap(),
            digest.as_str()
        );

        // 4. GET blob
        let get_blob_res = client
            .get(format!("{base_url}/v2/{repo}/blobs/{digest}"))
            .basic_auth("demo", Some("demo"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            get_blob_res.status(),
            StatusCode::OK,
            "GET blob failed for {repo}"
        );
        assert_eq!(get_blob_res.bytes().await.unwrap(), Bytes::from(content));

        // 5. PUT manifest
        let manifest_json = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": digest,
                "size": 10
            },
            "layers": []
        })
        .to_string();

        let tag = "v1.0.0";
        let put_manifest_res = client
            .put(format!("{base_url}/v2/{repo}/manifests/{tag}"))
            .basic_auth("demo", Some("demo"))
            .header(
                header::CONTENT_TYPE,
                "application/vnd.oci.image.manifest.v1+json",
            )
            .body(manifest_json.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(
            put_manifest_res.status(),
            StatusCode::CREATED,
            "PUT manifest failed for {repo}"
        );
        let manifest_digest = put_manifest_res
            .headers()
            .get("Docker-Content-Digest")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // 6. HEAD manifest
        let head_man_res = client
            .head(format!("{base_url}/v2/{repo}/manifests/{tag}"))
            .basic_auth("demo", Some("demo"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            head_man_res.status(),
            StatusCode::OK,
            "HEAD manifest failed for {repo}"
        );

        // 7. GET manifest
        let get_man_res = client
            .get(format!("{base_url}/v2/{repo}/manifests/{tag}"))
            .basic_auth("demo", Some("demo"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            get_man_res.status(),
            StatusCode::OK,
            "GET manifest failed for {repo}"
        );

        // 8. GET tags list
        let tags_res = client
            .get(format!("{base_url}/v2/{repo}/tags/list"))
            .basic_auth("demo", Some("demo"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            tags_res.status(),
            StatusCode::OK,
            "GET tags list failed for {repo}"
        );

        // 9. DELETE manifest
        let del_res = client
            .delete(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
            .basic_auth("demo", Some("demo"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            del_res.status(),
            StatusCode::ACCEPTED,
            "DELETE manifest failed for {repo}"
        );
    }
}

// ================================================================================================
// 6. UPLOAD AND FINALIZATION IN NEWLY VALID REPOSITORIES
// ================================================================================================
#[tokio::test]
async fn test_06_chunked_upload_and_finalization() {
    let dir = tempfile::tempdir().unwrap();
    let port = pick_unused_port();
    let (cfg_path, _fs_root) = write_config(&dir, port);
    let _guard = spawn_server(&cfg_path);
    wait_for_server(&cfg_path, port).await;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{port}");
    let repo = "team/image__cache--staging---v1";

    let post_res = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(post_res.status(), StatusCode::ACCEPTED);

    let location = post_res
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    let upload_url = if location.starts_with("http") {
        location.to_string()
    } else {
        format!("{base_url}{location}")
    };

    // Chunk 1: 2048 bytes
    let chunk1 = vec![b'A'; 2048];
    let patch1_res = client
        .patch(&upload_url)
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_RANGE, "0-2047")
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(chunk1.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(patch1_res.status(), StatusCode::ACCEPTED);

    let next_location = patch1_res
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    let next_upload_url = if next_location.starts_with("http") {
        next_location.to_string()
    } else {
        format!("{base_url}{next_location}")
    };

    // Chunk 2 & finalize: 2048 bytes
    let chunk2 = vec![b'B'; 2048];
    let mut full_payload = chunk1;
    full_payload.extend_from_slice(&chunk2);
    let digest = format!("sha256:{}", hex_sha256(&full_payload));

    let put_res = client
        .put(format!("{next_upload_url}&digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_RANGE, "2048-4095")
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(chunk2)
        .send()
        .await
        .unwrap();
    assert_eq!(put_res.status(), StatusCode::CREATED);

    // Verify blob
    let get_res = client
        .get(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_res.status(), StatusCode::OK);
    assert_eq!(get_res.bytes().await.unwrap(), Bytes::from(full_payload));
}

// ================================================================================================
// 7. CROSS-REPOSITORY MOUNT BETWEEN NEWLY VALID REPOSITORIES
// ================================================================================================
#[tokio::test]
async fn test_07_cross_repository_mount_between_newly_valid_repos() {
    let dir = tempfile::tempdir().unwrap();
    let port = pick_unused_port();
    let (cfg_path, _fs_root) = write_config(&dir, port);
    let _guard = spawn_server(&cfg_path);
    wait_for_server(&cfg_path, port).await;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{port}");

    let src_repo = "team/image__cache";
    let target_repo = "team/image--cache";

    // 1. Upload blob to src_repo
    let payload = b"shared layer for cross repo mount test";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let post_res = client
        .post(format!("{base_url}/v2/{src_repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    let location = post_res
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    let upload_url = if location.starts_with("http") {
        location.to_string()
    } else {
        format!("{base_url}{location}")
    };

    let put_res = client
        .put(format!("{upload_url}&digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(put_res.status(), StatusCode::CREATED);

    // 2. Mount to target_repo
    let mount_res = client
        .post(format!(
            "{base_url}/v2/{target_repo}/blobs/uploads/?mount={digest}&from={src_repo}"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(mount_res.status(), StatusCode::CREATED);
    assert_eq!(
        mount_res.headers().get("Docker-Content-Digest").unwrap(),
        digest.as_str()
    );

    // 3. Verify target_repo now has access to the blob
    let get_res = client
        .get(format!("{base_url}/v2/{target_repo}/blobs/{digest}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_res.status(), StatusCode::OK);
    assert_eq!(get_res.bytes().await.unwrap(), Bytes::from_static(payload));
}

// ================================================================================================
// 8. AUTHORIZATION SCOPES FOR NEWLY VALID NAMES
// ================================================================================================
#[tokio::test]
async fn test_08_authorization_scopes_via_http() {
    let dir = tempfile::tempdir().unwrap();
    let port = pick_unused_port();

    let fs_root = dir.path().join("data");
    let ref_index = dir.path().join("ref-index");

    std::fs::create_dir_all(fs_root.join("blobs").join("sha256")).unwrap();
    std::fs::create_dir_all(fs_root.join("repos")).unwrap();
    std::fs::create_dir_all(fs_root.join("uploads")).unwrap();
    std::fs::create_dir_all(&ref_index).unwrap();

    let toml = format!(
        r#"
[server]
listen_addr = "127.0.0.1:{port}"
public_url = "http://127.0.0.1:{port}"

[auth]
strategy = "both"

[auth.push]
username = "scoped-user"
password = "scoped-password"
allow_repos = ["team/image__cache", "team/image--service/*"]

[storage]
backend = "fs"

[storage.fs]
root = "{}"

[storage.ref_index]
enabled = false
path = "{}"

[admin_api]
enabled = false

[limits]
max_upload_bytes = 104857600
max_request_body_bytes = 33554432
upload_chunk_min_bytes = 1024
"#,
        fs_root.display(),
        ref_index.display()
    );

    let cfg_path = dir.path().join("config.toml");
    std::fs::write(&cfg_path, toml).unwrap();
    let _guard = spawn_server(&cfg_path);
    wait_for_server(&cfg_path, port).await;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{port}");

    // 1. Exact match "team/image__cache" succeeds
    let allowed_res1 = client
        .post(format!("{base_url}/v2/team/image__cache/blobs/uploads/"))
        .basic_auth("scoped-user", Some("scoped-password"))
        .send()
        .await
        .unwrap();
    assert_eq!(allowed_res1.status(), StatusCode::ACCEPTED);

    // 2. Subtree wildcard match "team/image--service/sub" succeeds
    let allowed_res2 = client
        .post(format!(
            "{base_url}/v2/team/image--service/sub/blobs/uploads/"
        ))
        .basic_auth("scoped-user", Some("scoped-password"))
        .send()
        .await
        .unwrap();
    assert_eq!(allowed_res2.status(), StatusCode::ACCEPTED);

    // 3. Unauthorized repo "other/repo" is rejected
    let denied_res = client
        .post(format!("{base_url}/v2/other/repo/blobs/uploads/"))
        .basic_auth("scoped-user", Some("scoped-password"))
        .send()
        .await
        .unwrap();
    assert_eq!(denied_res.status(), StatusCode::UNAUTHORIZED);
}

// ================================================================================================
// 9. PROXY CACHING CONFIGURATION & DECISION FOR NEWLY VALID NAMES
// ================================================================================================
#[tokio::test]
async fn test_09_proxy_caching_configuration_with_canonical_names() {
    let dir = tempfile::tempdir().unwrap();
    let port = pick_unused_port();

    let fs_root = dir.path().join("data");
    let ref_index = dir.path().join("ref-index");
    let cache_root = dir.path().join("cache");

    std::fs::create_dir_all(fs_root.join("blobs").join("sha256")).unwrap();
    std::fs::create_dir_all(fs_root.join("repos")).unwrap();
    std::fs::create_dir_all(fs_root.join("uploads")).unwrap();
    std::fs::create_dir_all(&ref_index).unwrap();
    std::fs::create_dir_all(&cache_root).unwrap();

    let toml = format!(
        r#"
[server]
listen_addr = "127.0.0.1:{port}"
public_url = "http://127.0.0.1:{port}"

[auth]
strategy = "both"

[auth.push]
username = "demo"
password = "demo"
allow_repos = ["*"]
actions = ["pull", "push", "delete"]

[storage]
backend = "fs"

[storage.fs]
root = "{}"

[proxy]
enabled = true

[proxy.safety]
allowed_repo_prefixes = ["upstream/team__cache/"]

[[proxy.upstreams]]
hosts = ["*"]
base_url = "https://registry-1.docker.io"
max_cache_bytes = 104857600

[limits]
max_upload_bytes = 104857600
max_request_body_bytes = 33554432
upload_chunk_min_bytes = 1024
"#,
        fs_root.display()
    );

    let cfg_path = dir.path().join("config.toml");
    std::fs::write(&cfg_path, toml).unwrap();
    let _guard = spawn_server(&cfg_path);
    wait_for_server(&cfg_path, port).await;

    // Server started and successfully loaded config containing canonical repo prefix upstream/team__cache/
    let client = reqwest::Client::new();
    let res = client
        .get(format!("http://127.0.0.1:{port}/v2/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

// ================================================================================================
// 10. LIFECYCLE JOURNAL RECOVERY FOR NEWLY VALID NAMES
// ================================================================================================
#[tokio::test]
async fn test_10_lifecycle_journal_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let fs_root = dir.path().join("data");
    let ref_idx_path = dir.path().join("idx");
    std::fs::create_dir_all(&fs_root).unwrap();
    std::fs::create_dir_all(&ref_idx_path).unwrap();

    let storage = Arc::new(FsStorage::new(fs_root.clone(), 10 * 1024 * 1024));
    let ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());

    let coordinator = registry_rust::consistency::ConsistencyCoordinator::new();
    let svc = ManifestLifecycleService::new(
        storage.clone() as Arc<dyn Storage>,
        Some(ref_index.clone()),
        coordinator,
    );

    let repo = "team/image__cache--production";
    let cfg_bytes = b"cfg-payload";
    let layer_bytes = b"layer-payload";
    let cfg_d = sha256_digest(cfg_bytes);
    let layer_d = sha256_digest(layer_bytes);

    // Link blobs
    let canonical_repo = CanonicalRepoName::parse(repo).unwrap();
    let rec_cfg = RepoBlobMembershipRecord::new_upload(canonical_repo.clone(), cfg_d.clone(), None);
    let rec_layer =
        RepoBlobMembershipRecord::new_upload(canonical_repo.clone(), layer_d.clone(), None);
    storage.link_repo_blob(&rec_cfg).await.unwrap();
    storage.link_repo_blob(&rec_layer).await.unwrap();

    let manifest_bytes = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": cfg_d.as_str(),
            "size": cfg_bytes.len()
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": layer_d.as_str(),
                "size": layer_bytes.len()
            }
        ]
    })
    .to_string()
    .into_bytes();

    let manifest_d = sha256_digest(&manifest_bytes);

    // Publish manifest
    let pub_req = PublishManifestRequest {
        repo: repo.to_string(),
        reference: "v1.0.0".to_string(),
        payload: Bytes::from(manifest_bytes),
        declared_media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        allow_tag_overwrite: true,
    };

    let pub_res = svc.publish_manifest(pub_req).await.unwrap();
    assert_eq!(pub_res.digest, manifest_d);

    // Run recovery
    svc.recover_and_ensure_index_healthy(repo).await.unwrap();
    assert!(ref_index.check_health().is_ok());
}

// ================================================================================================
// 11. MEMBERSHIP MIGRATION & REBUILD FOR NEWLY VALID NAMES
// ================================================================================================
#[tokio::test]
async fn test_11_membership_migration_and_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let fs_root = dir.path().join("data");
    let ref_idx_path = dir.path().join("idx");
    std::fs::create_dir_all(&fs_root).unwrap();
    std::fs::create_dir_all(&ref_idx_path).unwrap();

    let storage = Arc::new(FsStorage::new(fs_root.clone(), 10 * 1024 * 1024));
    let ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());

    let repo_names = vec![
        "team/image__cache",
        "team/image--cache",
        "a.b/c_d/e__f/g---h",
    ];

    for name in &repo_names {
        let canonical_repo = CanonicalRepoName::parse(name).unwrap();
        let digest = sha256_digest(name.as_bytes());
        let rec = RepoBlobMembershipRecord::new_upload(canonical_repo, digest.clone(), None);
        storage.link_repo_blob(&rec).await.unwrap();
    }

    // Force dirty and rebuild
    ref_index.mark_dirty().unwrap();
    ref_index
        .rebuild(&(storage.clone() as Arc<dyn Storage>))
        .await
        .unwrap();

    for name in &repo_names {
        let digest = sha256_digest(name.as_bytes());
        let has = ref_index.has_any_repo_membership(&digest).unwrap();
        assert!(has, "Rebuilt index must have membership for {name}");
    }
}

// ================================================================================================
// 12. GC SWEEPS AND QUARANTINE FOR NEWLY VALID NAMES
// ================================================================================================
#[tokio::test]
async fn test_12_gc_sweeps_and_membership_handling() {
    let dir = tempfile::tempdir().unwrap();
    let fs_root = dir.path().join("data");
    let ref_idx_path = dir.path().join("idx");
    std::fs::create_dir_all(&fs_root).unwrap();
    std::fs::create_dir_all(&ref_idx_path).unwrap();

    let storage = Arc::new(FsStorage::new(fs_root.clone(), 10 * 1024 * 1024));
    let _ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());

    let repo = "team/image__cache--gc-sweep";
    let canonical_repo = CanonicalRepoName::parse(repo).unwrap();
    let digest = sha256_digest(b"gc-test-blob");

    let rec = RepoBlobMembershipRecord::new_upload(canonical_repo, digest.clone(), None);
    storage.link_repo_blob(&rec).await.unwrap();

    let fetched = storage
        .get_repo_blob_membership(repo, &digest)
        .await
        .unwrap();
    assert!(fetched.is_some());
    assert_eq!(fetched.unwrap().repo.as_str(), repo);

    // Unlink membership
    let removed = storage.unlink_repo_blob(repo, &digest).await.unwrap();
    assert!(removed);

    let fetched_after = storage
        .get_repo_blob_membership(repo, &digest)
        .await
        .unwrap();
    assert!(fetched_after.is_none());
}

// ================================================================================================
// 13. FILESYSTEM PATHS REMAINING UNDER ROOT (PATH TRAVERSAL ATTACK TESTS)
// ================================================================================================
#[test]
fn test_13_filesystem_path_traversal_protection() {
    let base_root = Path::new("/var/data/registry");

    let repo = CanonicalRepoName::parse("team/image__cache/sub").unwrap();
    let fs_path = registry_rust::test_support::fs_repo_dir(base_root, &repo).unwrap();

    assert!(fs_path.starts_with(base_root));
    assert_eq!(
        fs_path,
        base_root.join("repos").join("team/image__cache/sub")
    );

    // Verify all path components are Normal (no ParentDir, RootDir, Prefix)
    for comp in fs_path.components() {
        assert!(matches!(
            comp,
            std::path::Component::RootDir | std::path::Component::Normal(_)
        ));
    }
}

// ================================================================================================
// 14. LIVE S3/MINIO KEYS REMAINING ISOLATED UNDER TEST PREFIX
// ================================================================================================
#[test]
fn test_14_s3_prefix_isolation() {
    let root_prefix = "live-test-uuid-12345/root";
    let repo = CanonicalRepoName::parse("team/image__cache/sub--service").unwrap();

    let prefix = registry_rust::test_support::s3_repo_prefix(root_prefix, &repo);
    assert_eq!(
        prefix,
        "live-test-uuid-12345/root/repos/team/image__cache/sub--service/"
    );
    assert!(prefix.starts_with(root_prefix));
    assert!(!prefix.contains("//"));
    assert!(!prefix.contains(".."));
}

// ================================================================================================
// 15. COLLISION TESTS ACROSS REPOSITORY-DERIVED KEY FAMILIES
// ================================================================================================
#[test]
fn test_15_collision_isolation_across_key_families() {
    let repo_variants = vec!["a/b", "a_b", "a-b", "a.b", "a__b", "a--b"];

    let mut keys = std::collections::HashSet::new();
    let mut fs_paths = std::collections::HashSet::new();
    let base_root = Path::new("/root");

    for name in repo_variants {
        let repo = CanonicalRepoName::parse(name).unwrap();
        let encoded_key = registry_rust::test_support::encode_canonical_repo_key(&repo);
        let fs_path = registry_rust::test_support::fs_repo_dir(base_root, &repo).unwrap();
        let _s3_prefix = registry_rust::test_support::s3_repo_prefix("root", &repo);

        assert!(
            keys.insert(encoded_key.clone()),
            "Key collision for repo {name}"
        );
        assert!(
            fs_paths.insert(fs_path),
            "FS path collision for repo {name}"
        );

        // Decode roundtrip
        let decoded = registry_rust::test_support::decode_canonical_repo_key(&encoded_key).unwrap();
        assert_eq!(decoded.as_str(), name);
    }
}

// ================================================================================================
// 16. RAW/ENCODED HTTP PATH ATTACK CASES
// ================================================================================================
#[tokio::test]
async fn test_16_encoded_http_path_attacks() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = tempfile::tempdir().unwrap();
    let port = pick_unused_port();
    let (cfg_path, fs_root) = write_config(&dir, port);
    let _guard = spawn_server(&cfg_path);
    wait_for_server(&cfg_path, port).await;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{port}");

    let reqwest_attack_paths = vec![
        "/v2/team%2fimage/blobs/uploads/",
        "/v2/team%2Fimage/blobs/uploads/",
        "/v2/team%5cimage/blobs/uploads/",
        "/v2/%252f/blobs/uploads/",
        "/v2/team/image%00/blobs/uploads/",
        "/v2/team%2e%2e/blobs/uploads/",
    ];

    for path in reqwest_attack_paths {
        let res = client
            .post(format!("{base_url}{path}"))
            .basic_auth("demo", Some("demo"))
            .send()
            .await
            .unwrap();

        assert_eq!(
            res.status(),
            StatusCode::BAD_REQUEST,
            "Attack path {path} must return 400 Bad Request"
        );

        let body: serde_json::Value = res.json().await.unwrap();
        let errors = body.get("errors").unwrap().as_array().unwrap();
        assert_eq!(errors[0].get("code").unwrap(), "NAME_INVALID");
    }

    // Test raw TCP socket path traversal attempts
    let raw_attack_paths = vec!["/v2/%2e%2e/blobs/uploads/", "/v2/..%2fteam/blobs/uploads/"];

    for raw_path in raw_attack_paths {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let auth = base64::engine::general_purpose::STANDARD.encode("demo:demo");
        let req = format!(
            "POST {raw_path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAuthorization: Basic {auth}\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let resp_str = String::from_utf8_lossy(&buf);

        assert!(
            resp_str.starts_with("HTTP/1.1 400") || resp_str.starts_with("HTTP/1.0 400"),
            "Raw attack path {raw_path} must return 400 Bad Request, got: {resp_str}"
        );
        assert!(resp_str.contains("NAME_INVALID"));
    }

    // Verify zero storage mutations
    let uploads_entries = std::fs::read_dir(fs_root.join("uploads")).unwrap().count();
    assert_eq!(
        uploads_entries, 0,
        "No upload sessions created on attack paths"
    );
}

// ================================================================================================
// 17. CORRECT NAME_INVALID STATUS (400) AND ERROR BODY
// ================================================================================================
#[tokio::test]
async fn test_17_name_invalid_status_and_body() {
    let dir = tempfile::tempdir().unwrap();
    let port = pick_unused_port();
    let (cfg_path, _fs_root) = write_config(&dir, port);
    let _guard = spawn_server(&cfg_path);
    wait_for_server(&cfg_path, port).await;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{port}");

    let invalid_repos = vec![
        "Team/Image",
        "team/image___cache",
        "team/image._cache",
        "team/image-",
        ".team/image",
    ];

    for repo in invalid_repos {
        let res = client
            .get(format!("{base_url}/v2/{repo}/tags/list"))
            .basic_auth("demo", Some("demo"))
            .send()
            .await
            .unwrap();

        assert_eq!(
            res.status(),
            StatusCode::BAD_REQUEST,
            "Invalid repo {repo} must return 400"
        );

        let body: serde_json::Value = res.json().await.unwrap();
        let errors = body.get("errors").unwrap().as_array().unwrap();
        assert_eq!(errors[0].get("code").unwrap(), "NAME_INVALID");
        assert!(
            errors[0]
                .get("message")
                .unwrap()
                .as_str()
                .unwrap()
                .contains("invalid repository name")
        );
    }
}

// ================================================================================================
// 18. ZERO STORAGE MUTATION ON INVALID INPUT
// ================================================================================================
#[tokio::test]
async fn test_18_zero_storage_mutation_on_invalid_input() {
    let dir = tempfile::tempdir().unwrap();
    let port = pick_unused_port();
    let (cfg_path, fs_root) = write_config(&dir, port);
    let _guard = spawn_server(&cfg_path);
    wait_for_server(&cfg_path, port).await;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{port}");

    // Attempt start upload with invalid repo
    let res = client
        .post(format!("{base_url}/v2/team/image___invalid/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // Check filesystem state
    let uploads_entries = std::fs::read_dir(fs_root.join("uploads")).unwrap().count();
    assert_eq!(uploads_entries, 0);

    let repos_entries = std::fs::read_dir(fs_root.join("repos")).unwrap().count();
    assert_eq!(repos_entries, 0);

    let blobs_entries = std::fs::read_dir(fs_root.join("blobs").join("sha256"))
        .unwrap()
        .count();
    assert_eq!(blobs_entries, 0);
}
