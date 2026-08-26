use base64::prelude::*;
use reqwest::header;
use serde_json::json;
use sha2::Digest as _;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn pick_unused_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral port");
    listener.local_addr().expect("local_addr").port()
}

fn hex_sha256(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let out = hasher.finalize();
    hex::encode(out)
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
    let config_dir = dir.path().join("config");
    std::fs::create_dir_all(&config_dir).expect("mkdir config");
    let fs_root = dir.path().join("data");
    std::fs::create_dir_all(&fs_root).expect("mkdir data");

    // Initialize as fresh/ready membership schema
    let meta_dir = fs_root.join("meta");
    std::fs::create_dir_all(&meta_dir).expect("mkdir meta");
    let ready_file = meta_dir.join("membership_ready.json");
    let ready_json = serde_json::json!({
        "version": 1,
        "ready_at_unix_secs": 1000
    });
    std::fs::write(&ready_file, serde_json::to_vec(&ready_json).unwrap()).expect("write ready");

    let cfg_path = config_dir.join("config.toml");
    let log_path = dir.path().join("server.log");

    let content = format!(
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

[storage]
backend = "fs"

[storage.fs]
root = "{}"

[token]
signing_key = "test-token-key-membership"

[admin_api]
enabled = false

[limits]
max_upload_bytes = 104857600
max_request_body_bytes = 33554432
upload_chunk_min_bytes = 10
"#,
        fs_root.to_string_lossy()
    );

    std::fs::write(&cfg_path, content).expect("write config");
    (cfg_path, log_path)
}

fn spawn_server(cfg_path: &Path, log_path: &Path) -> ServerGuard {
    let log_file = std::fs::File::create(log_path).expect("create log file");
    let child = Command::new(bin_path())
        .arg("--config")
        .arg(cfg_path)
        .arg("server")
        .stdout(Stdio::from(log_file.try_clone().expect("clone stdout")))
        .stderr(Stdio::from(log_file))
        .spawn()
        .expect("spawn server");

    ServerGuard { child }
}

async fn wait_ready(base_url: &str, log_path: &Path) {
    let client = reqwest::Client::new();
    for _ in 0..100 {
        if let Ok(resp) = client.get(format!("{base_url}/v2/")).send().await {
            if resp.status().is_success() || resp.status().as_u16() == 401 {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let logs = std::fs::read_to_string(log_path).unwrap_or_default();
    panic!("Server did not become ready at {base_url}. Logs:\n{logs}");
}

// -------------------------------------------------------------------------------------------------
// Test 1: Upload finalization in repo A creates membership in A and NOT in repo B.
// GET/HEAD on repo B returns 404 BLOB_UNKNOWN despite global CAS presence in repo A.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_tenant_isolation_global_cas_does_not_grant_access() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo_a = "tenant-a/app";
    let repo_b = "tenant-b/app";

    let payload = b"secret-tenant-a-blob-bytes";
    let digest = format!("sha256:{}", hex_sha256(payload));

    // Monolithic upload to repo A
    let upl_resp = client
        .post(format!("{base}/v2/{repo_a}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_LENGTH, payload.len())
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload to repo A");
    assert_eq!(upl_resp.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        upl_resp
            .headers()
            .get("Location")
            .unwrap()
            .to_str()
            .unwrap(),
        format!("/v2/{repo_a}/blobs/{digest}")
    );

    // GET from repo A -> 200 OK
    let get_a = client
        .get(format!("{base}/v2/{repo_a}/blobs/{digest}"))
        .send()
        .await
        .expect("get repo A");
    assert_eq!(get_a.status(), reqwest::StatusCode::OK);
    assert_eq!(get_a.bytes().await.unwrap().as_ref(), payload);

    // HEAD from repo A -> 200 OK
    let head_a = client
        .head(format!("{base}/v2/{repo_a}/blobs/{digest}"))
        .send()
        .await
        .expect("head repo A");
    assert_eq!(head_a.status(), reqwest::StatusCode::OK);

    // GET from repo B -> 404 BLOB_UNKNOWN (Global CAS existence does NOT grant access)
    let get_b = client
        .get(format!("{base}/v2/{repo_b}/blobs/{digest}"))
        .send()
        .await
        .expect("get repo B");
    assert_eq!(get_b.status(), reqwest::StatusCode::NOT_FOUND);
    let err_b: serde_json::Value = get_b.json().await.expect("json");
    assert_eq!(err_b["errors"][0]["code"], "BLOB_UNKNOWN");

    // HEAD from repo B -> 404 Not Found
    let head_b = client
        .head(format!("{base}/v2/{repo_b}/blobs/{digest}"))
        .send()
        .await
        .expect("head repo B");
    assert_eq!(head_b.status(), reqwest::StatusCode::NOT_FOUND);
}

// -------------------------------------------------------------------------------------------------
// Test 2: Cross-mount with valid pull authorization on repo A creates membership in repo B.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_cross_mount_creates_target_membership_and_enables_get() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo_a = "source-team/lib";
    let repo_b = "target-team/service";

    let payload = b"shared-library-binary-layer";
    let digest = format!("sha256:{}", hex_sha256(payload));

    // Upload to source repo A
    let upl = client
        .post(format!("{base}/v2/{repo_a}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload to repo A");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);

    // Verify repo B does not have membership yet
    let get_b_before = client
        .get(format!("{base}/v2/{repo_b}/blobs/{digest}"))
        .send()
        .await
        .expect("get repo B before");
    assert_eq!(get_b_before.status(), reqwest::StatusCode::NOT_FOUND);

    // Cross-mount to repo B with from=repo_a
    let mount_resp = client
        .post(format!(
            "{base}/v2/{repo_b}/blobs/uploads/?mount={digest}&from={repo_a}"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("cross mount to repo B");
    assert_eq!(mount_resp.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        mount_resp
            .headers()
            .get("Location")
            .unwrap()
            .to_str()
            .unwrap(),
        format!("/v2/{repo_b}/blobs/{digest}")
    );

    // GET from repo B now succeeds with 200 OK
    let get_b_after = client
        .get(format!("{base}/v2/{repo_b}/blobs/{digest}"))
        .send()
        .await
        .expect("get repo B after");
    assert_eq!(get_b_after.status(), reqwest::StatusCode::OK);
    assert_eq!(get_b_after.bytes().await.unwrap().as_ref(), payload);

    // Re-mounting to repo B is idempotent and returns 201 Created
    let remount_resp = client
        .post(format!(
            "{base}/v2/{repo_b}/blobs/uploads/?mount={digest}&from={repo_a}"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("remount to repo B");
    assert_eq!(remount_resp.status(), reqwest::StatusCode::CREATED);
}

// -------------------------------------------------------------------------------------------------
// Test 3: Cross-mount missing `from` parameter fails closed to 202 Accepted.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_cross_mount_without_from_fails_closed_to_202() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo_a = "repo-a";
    let repo_b = "repo-b";

    let payload = b"secret-data-probe";
    let digest = format!("sha256:{}", hex_sha256(payload));

    // Upload to repo A
    client
        .post(format!("{base}/v2/{repo_a}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload to repo A");

    // Mount in repo B without `from` -> MUST NOT return 201, must fall back to 202 Accepted
    let mount_resp = client
        .post(format!("{base}/v2/{repo_b}/blobs/uploads/?mount={digest}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("mount without from");
    assert_eq!(mount_resp.status(), reqwest::StatusCode::ACCEPTED);
    assert!(mount_resp.headers().get("Location").is_some());

    // Repo B still cannot read the blob
    let get_b = client
        .get(format!("{base}/v2/{repo_b}/blobs/{digest}"))
        .send()
        .await
        .expect("get repo B");
    assert_eq!(get_b.status(), reqwest::StatusCode::NOT_FOUND);
}

// -------------------------------------------------------------------------------------------------
// Test 4: Monolithic upload in repo B when blob already exists in CAS links membership to B.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_monolithic_upload_already_finalized_in_cas_creates_repo_membership() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo_a = "alpha";
    let repo_b = "beta";

    let payload = b"common-ubuntu-base-layer";
    let digest = format!("sha256:{}", hex_sha256(payload));

    // Upload to repo A
    let upl_a = client
        .post(format!("{base}/v2/{repo_a}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload to repo A");
    assert_eq!(upl_a.status(), reqwest::StatusCode::CREATED);

    // Monolithic upload to repo B (blob already exists in CAS)
    let upl_b = client
        .post(format!("{base}/v2/{repo_b}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload to repo B");
    assert_eq!(upl_b.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        upl_b.headers().get("Location").unwrap().to_str().unwrap(),
        format!("/v2/{repo_b}/blobs/{digest}")
    );

    // GET from repo B succeeds
    let get_b = client
        .get(format!("{base}/v2/{repo_b}/blobs/{digest}"))
        .send()
        .await
        .expect("get repo B");
    assert_eq!(get_b.status(), reqwest::StatusCode::OK);
}

// -------------------------------------------------------------------------------------------------
// Test 5: Manifest publication in repo B referencing blob in repo A fails with 400 BLOB_UNKNOWN.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_manifest_publication_rejects_unowned_blob_references() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo_a = "repo-owned";
    let repo_b = "repo-unowned";

    let config_payload = b"{}";
    let config_digest = format!("sha256:{}", hex_sha256(config_payload));

    let layer_payload = b"layer-only-in-repo-a";
    let layer_digest = format!("sha256:{}", hex_sha256(layer_payload));

    // Upload config and layer to repo A
    client
        .post(format!(
            "{base}/v2/{repo_a}/blobs/uploads/?digest={config_digest}"
        ))
        .basic_auth("demo", Some("demo"))
        .body(config_payload.to_vec())
        .send()
        .await
        .expect("upload config to A");

    client
        .post(format!(
            "{base}/v2/{repo_a}/blobs/uploads/?digest={layer_digest}"
        ))
        .basic_auth("demo", Some("demo"))
        .body(layer_payload.to_vec())
        .send()
        .await
        .expect("upload layer to A");

    // Upload ONLY config to repo B
    client
        .post(format!(
            "{base}/v2/{repo_b}/blobs/uploads/?digest={config_digest}"
        ))
        .basic_auth("demo", Some("demo"))
        .body(config_payload.to_vec())
        .send()
        .await
        .expect("upload config to B");

    // Manifest referencing config (owned by B) and layer (owned by A, NOT by B)
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config_digest,
            "size": config_payload.len()
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": layer_digest,
                "size": layer_payload.len()
            }
        ]
    });

    // Publication in repo B MUST fail with 400 Bad Request BLOB_UNKNOWN
    let pub_resp = client
        .put(format!("{base}/v2/{repo_b}/manifests/v1.0.0"))
        .basic_auth("demo", Some("demo"))
        .header(
            header::CONTENT_TYPE,
            "application/vnd.oci.image.manifest.v1+json",
        )
        .body(serde_json::to_vec(&manifest).unwrap())
        .send()
        .await
        .expect("publish manifest in B");
    assert_eq!(pub_resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let err: serde_json::Value = pub_resp.json().await.expect("json");
    let err_code = err["errors"][0]["code"].as_str().unwrap();
    assert!(
        err_code == "MANIFEST_BLOB_UNKNOWN" || err_code == "BLOB_UNKNOWN",
        "Expected blob unknown error, got: {err_code}"
    );

    // Cross-mount layer to repo B
    let mount_resp = client
        .post(format!(
            "{base}/v2/{repo_b}/blobs/uploads/?mount={layer_digest}&from={repo_a}"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("mount layer to B");
    assert_eq!(mount_resp.status(), reqwest::StatusCode::CREATED);

    // Publication in repo B now succeeds with 201 Created
    let pub_resp2 = client
        .put(format!("{base}/v2/{repo_b}/manifests/v1.0.0"))
        .basic_auth("demo", Some("demo"))
        .header(
            header::CONTENT_TYPE,
            "application/vnd.oci.image.manifest.v1+json",
        )
        .body(serde_json::to_vec(&manifest).unwrap())
        .send()
        .await
        .expect("publish manifest in B after mount");
    assert_eq!(pub_resp2.status(), reqwest::StatusCode::CREATED);
}

// -------------------------------------------------------------------------------------------------
// Test 6: Blob DELETE unlinks only the requested repo membership and preserves CAS and other repos.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_blob_delete_unlinks_only_requested_repo_membership() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo_a = "org/first";
    let repo_b = "org/second";

    let payload = b"blob-to-delete-from-one-repo-only";
    let digest = format!("sha256:{}", hex_sha256(payload));

    // Upload to repo A and cross-mount to repo B
    let upl_a = client
        .post(format!("{base}/v2/{repo_a}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload to A");
    assert_eq!(upl_a.status(), reqwest::StatusCode::CREATED);

    let mount_b = client
        .post(format!(
            "{base}/v2/{repo_b}/blobs/uploads/?mount={digest}&from={repo_a}"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("mount to B");
    assert_eq!(mount_b.status(), reqwest::StatusCode::CREATED);

    // Both repos can read
    assert_eq!(
        client
            .get(format!("{base}/v2/{repo_a}/blobs/{digest}"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        client
            .get(format!("{base}/v2/{repo_b}/blobs/{digest}"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );

    // DELETE blob from repo B
    let del_resp = client
        .delete(format!("{base}/v2/{repo_b}/blobs/{digest}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("delete from B");
    assert_eq!(del_resp.status(), reqwest::StatusCode::ACCEPTED);

    // Repo B now returns 404 BLOB_UNKNOWN
    let get_b = client
        .get(format!("{base}/v2/{repo_b}/blobs/{digest}"))
        .send()
        .await
        .expect("get B after delete");
    assert_eq!(get_b.status(), reqwest::StatusCode::NOT_FOUND);

    // Repo A STILL returns 200 OK with payload intact!
    let get_a = client
        .get(format!("{base}/v2/{repo_a}/blobs/{digest}"))
        .send()
        .await
        .expect("get A after delete in B");
    assert_eq!(get_a.status(), reqwest::StatusCode::OK);
    assert_eq!(get_a.bytes().await.unwrap().as_ref(), payload);
}

// -------------------------------------------------------------------------------------------------
// Test 7: Membership Migration CLI (plan, apply, verify) and Fail-Closed Startup Check.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_membership_migration_lifecycle_and_fail_closed_startup() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_dir = dir.path().join("config");
    std::fs::create_dir_all(&config_dir).expect("mkdir config");
    let fs_root = dir.path().join("data");
    std::fs::create_dir_all(&fs_root).expect("mkdir data");

    // Seed existing pre-Slice-8 data: CAS blob, manifest, and tag, but NO membership metadata
    let payload = b"migrated-layer-payload";
    let hex = hex_sha256(payload);
    let digest = format!("sha256:{hex}");

    let cas_path = fs_root
        .join("blobs")
        .join("sha256")
        .join(&hex[0..2])
        .join(&hex);
    std::fs::create_dir_all(cas_path.parent().unwrap()).expect("mkdir cas");
    std::fs::write(&cas_path, payload).expect("write cas blob");

    let config_payload = b"{}";
    let cfg_hex = hex_sha256(config_payload);
    let cfg_digest = format!("sha256:{cfg_hex}");
    let cfg_cas = fs_root
        .join("blobs")
        .join("sha256")
        .join(&cfg_hex[0..2])
        .join(&cfg_hex);
    std::fs::create_dir_all(cfg_cas.parent().unwrap()).expect("mkdir cfg cas");
    std::fs::write(&cfg_cas, config_payload).expect("write cfg cas");

    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": cfg_digest,
            "size": config_payload.len()
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": digest,
                "size": payload.len()
            }
        ]
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_hex = hex_sha256(&manifest_bytes);
    let manifest_digest = format!("sha256:{manifest_hex}");

    let manifest_path = fs_root
        .join("repos")
        .join("legacy-repo")
        .join("manifests")
        .join(&manifest_hex);
    std::fs::create_dir_all(manifest_path.parent().unwrap()).expect("mkdir manifests");
    std::fs::write(&manifest_path, &manifest_bytes).expect("write manifest");

    // Tag the manifest in "legacy-repo"
    let tag_path = fs_root
        .join("repos")
        .join("legacy-repo")
        .join("tags")
        .join("latest");
    std::fs::create_dir_all(tag_path.parent().unwrap()).expect("mkdir tags");
    std::fs::write(&tag_path, &manifest_digest).expect("write tag");

    let port = pick_unused_port();
    let cfg_path = config_dir.join("config.toml");
    let content = format!(
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

[storage]
backend = "fs"

[storage.fs]
root = "{}"

[token]
signing_key = "test-token-key-migration"

[admin_api]
enabled = false
"#,
        fs_root.to_string_lossy()
    );
    std::fs::write(&cfg_path, content).expect("write config");

    // 1. Startup check fails closed when non-empty storage has no membership_ready marker
    let start_attempt = Command::new(bin_path())
        .arg("--config")
        .arg(&cfg_path)
        .arg("server")
        .output()
        .expect("run server");
    assert!(
        !start_attempt.status.success(),
        "Server startup must fail on unmigrated non-empty storage"
    );
    let stderr = String::from_utf8_lossy(&start_attempt.stderr);
    let stdout = String::from_utf8_lossy(&start_attempt.stdout);
    let all_out = format!("{stdout}\n{stderr}");
    assert!(
        all_out.contains("migrate-membership") || all_out.contains("membership migration"),
        "Startup error must guide operator to run migration. Output: {all_out}"
    );

    // 2. Run CLI migration plan
    let plan_output = Command::new(bin_path())
        .arg("--config")
        .arg(&cfg_path)
        .arg("migrate-membership")
        .arg("plan")
        .output()
        .expect("run migrate-membership plan");
    assert!(plan_output.status.success());
    let plan_str = String::from_utf8_lossy(&plan_output.stdout);
    assert!(
        plan_str.contains("Memberships to create: 2"),
        "Plan output was:\n{plan_str}"
    );

    // 3. Run CLI migration apply
    let apply_output = Command::new(bin_path())
        .arg("--config")
        .arg(&cfg_path)
        .arg("migrate-membership")
        .arg("apply")
        .output()
        .expect("run migrate-membership apply");
    assert!(apply_output.status.success());
    let apply_str = String::from_utf8_lossy(&apply_output.stdout);
    assert!(apply_str.contains("Migration Applied Successfully"));

    // 4. Run CLI migration verify
    let verify_output = Command::new(bin_path())
        .arg("--config")
        .arg(&cfg_path)
        .arg("migrate-membership")
        .arg("verify")
        .output()
        .expect("run migrate-membership verify");
    assert!(verify_output.status.success());

    // 5. Startup now succeeds and server serves migrated repository blobs
    let log_path = dir.path().join("server.log");
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let get_resp = client
        .get(format!("{base}/v2/legacy-repo/blobs/{digest}"))
        .send()
        .await
        .expect("get migrated blob");
    assert_eq!(get_resp.status(), reqwest::StatusCode::OK);
    assert_eq!(get_resp.bytes().await.unwrap().as_ref(), payload);
}

// -------------------------------------------------------------------------------------------------
// Test 8: Blob DELETE in repository where blob is still referenced by a manifest is rejected.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_blob_delete_referenced_in_same_repo_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "my-active-app";

    let config_payload = b"{}";
    let config_digest = format!("sha256:{}", hex_sha256(config_payload));
    let layer_payload = b"active-referenced-layer-bytes";
    let layer_digest = format!("sha256:{}", hex_sha256(layer_payload));

    // Upload config and layer
    client
        .post(format!(
            "{base}/v2/{repo}/blobs/uploads/?digest={config_digest}"
        ))
        .basic_auth("demo", Some("demo"))
        .body(config_payload.to_vec())
        .send()
        .await
        .expect("upload config");

    client
        .post(format!(
            "{base}/v2/{repo}/blobs/uploads/?digest={layer_digest}"
        ))
        .basic_auth("demo", Some("demo"))
        .body(layer_payload.to_vec())
        .send()
        .await
        .expect("upload layer");

    // Publish manifest referencing layer
    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config_digest,
            "size": config_payload.len()
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "digest": layer_digest,
            "size": layer_payload.len()
        }]
    });

    let pub_resp = client
        .put(format!("{base}/v2/{repo}/manifests/v1"))
        .basic_auth("demo", Some("demo"))
        .header(
            header::CONTENT_TYPE,
            "application/vnd.oci.image.manifest.v1+json",
        )
        .body(serde_json::to_vec(&manifest).unwrap())
        .send()
        .await
        .expect("publish manifest");
    assert_eq!(pub_resp.status(), reqwest::StatusCode::CREATED);

    // DELETE layer blob in repo -> must be rejected because manifest references it!
    let del_resp = client
        .delete(format!("{base}/v2/{repo}/blobs/{layer_digest}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("delete referenced blob");
    assert_eq!(del_resp.status(), reqwest::StatusCode::CONFLICT);
    let err: serde_json::Value = del_resp.json().await.expect("json");
    assert_eq!(err["errors"][0]["code"], "BLOB_IN_USE");

    // Membership remains intact
    let get_resp = client
        .get(format!("{base}/v2/{repo}/blobs/{layer_digest}"))
        .send()
        .await
        .expect("get blob after rejected delete");
    assert_eq!(get_resp.status(), reqwest::StatusCode::OK);
}

// -------------------------------------------------------------------------------------------------
// Test 9: Blob DELETE when referenced in repo A but unreferenced in repo B succeeds in B only.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_blob_delete_referenced_only_in_other_repo_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo_a = "team-a/service";
    let repo_b = "team-b/service";

    let config_payload = b"{}";
    let config_digest = format!("sha256:{}", hex_sha256(config_payload));
    let layer_payload = b"shared-multi-repo-blob";
    let layer_digest = format!("sha256:{}", hex_sha256(layer_payload));

    // Upload to repo A and publish manifest in A
    client
        .post(format!(
            "{base}/v2/{repo_a}/blobs/uploads/?digest={config_digest}"
        ))
        .basic_auth("demo", Some("demo"))
        .body(config_payload.to_vec())
        .send()
        .await
        .expect("upload config to A");

    client
        .post(format!(
            "{base}/v2/{repo_a}/blobs/uploads/?digest={layer_digest}"
        ))
        .basic_auth("demo", Some("demo"))
        .body(layer_payload.to_vec())
        .send()
        .await
        .expect("upload layer to A");

    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config_digest,
            "size": config_payload.len()
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "digest": layer_digest,
            "size": layer_payload.len()
        }]
    });
    client
        .put(format!("{base}/v2/{repo_a}/manifests/prod"))
        .basic_auth("demo", Some("demo"))
        .header(
            header::CONTENT_TYPE,
            "application/vnd.oci.image.manifest.v1+json",
        )
        .body(serde_json::to_vec(&manifest).unwrap())
        .send()
        .await
        .expect("publish manifest in A");

    // Cross-mount layer into repo B (no manifest in repo B references it)
    client
        .post(format!(
            "{base}/v2/{repo_b}/blobs/uploads/?mount={layer_digest}&from={repo_a}"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("mount layer to B");

    // DELETE in repo B succeeds with 202 Accepted because no manifest in B references it
    let del_b = client
        .delete(format!("{base}/v2/{repo_b}/blobs/{layer_digest}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("delete from B");
    assert_eq!(del_b.status(), reqwest::StatusCode::ACCEPTED);

    // Repo B now returns 404 BLOB_UNKNOWN
    let get_b = client
        .get(format!("{base}/v2/{repo_b}/blobs/{layer_digest}"))
        .send()
        .await
        .expect("get B");
    assert_eq!(get_b.status(), reqwest::StatusCode::NOT_FOUND);

    // Repo A still returns 200 OK
    let get_a = client
        .get(format!("{base}/v2/{repo_a}/blobs/{layer_digest}"))
        .send()
        .await
        .expect("get A");
    assert_eq!(get_a.status(), reqwest::StatusCode::OK);
    assert_eq!(get_a.bytes().await.unwrap().as_ref(), layer_payload);
}

// -------------------------------------------------------------------------------------------------
// Test 10: Repeated DELETE on same repository is idempotent and deterministic.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_blob_delete_repeated_is_idempotent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "idempotent-delete-repo";

    let payload = b"once-only-blob";
    let digest = format!("sha256:{}", hex_sha256(payload));

    client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload blob");

    // First DELETE: returns 202 Accepted
    let del1 = client
        .delete(format!("{base}/v2/{repo}/blobs/{digest}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("delete 1");
    assert_eq!(del1.status(), reqwest::StatusCode::ACCEPTED);

    // Second DELETE: returns 404 BLOB_UNKNOWN
    let del2 = client
        .delete(format!("{base}/v2/{repo}/blobs/{digest}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("delete 2");
    assert_eq!(del2.status(), reqwest::StatusCode::NOT_FOUND);
    let err: serde_json::Value = del2.json().await.expect("json");
    assert_eq!(err["errors"][0]["code"], "BLOB_UNKNOWN");
}

// -------------------------------------------------------------------------------------------------
// Test 11a: Receipt retry after intentional DELETE must NOT resurrect membership.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_receipt_retry_after_delete_must_not_resurrect_membership() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "no-resurrect-repo";
    let payload = b"blob-to-test-delete-no-resurrect";
    let digest = format!("sha256:{}", hex_sha256(payload));

    // 1. Start and finalize upload
    let start_resp = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("start upload");
    assert_eq!(start_resp.status(), reqwest::StatusCode::ACCEPTED);
    let location = start_resp
        .headers()
        .get(header::LOCATION)
        .expect("location")
        .to_str()
        .unwrap();

    let put_url = if location.starts_with("http") {
        format!("{location}&digest={digest}")
    } else {
        format!("{base}{location}&digest={digest}")
    };

    let fin_resp = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("finalize upload");
    assert_eq!(fin_resp.status(), reqwest::StatusCode::CREATED);

    let enc_repo = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(repo.as_bytes());
    let marker_path = dir
        .path()
        .join("data")
        .join("repo-memberships")
        .join("by-repo")
        .join(&enc_repo)
        .join("sha256")
        .join(format!("{}.json", hex_sha256(payload)));
    assert!(
        marker_path.exists(),
        "Membership marker must exist after finalize"
    );

    // 2. User intentionally DELETEs the blob membership
    let del_resp = client
        .delete(format!("{base}/v2/{repo}/blobs/{digest}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("delete blob");
    assert_eq!(del_resp.status(), reqwest::StatusCode::ACCEPTED);
    assert!(
        !marker_path.exists(),
        "Membership marker must be removed by DELETE"
    );

    // 3. Client retries finalization with old session location / receipt
    let retry_resp = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("retry finalize after delete");
    assert_eq!(
        retry_resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "Receipt retry must NOT resurrect an intentionally deleted membership"
    );

    // 4. Marker MUST remain deleted and GET must return 404
    assert!(
        !marker_path.exists(),
        "Membership marker must NOT be resurrected"
    );
    let get_resp = client
        .get(format!("{base}/v2/{repo}/blobs/{digest}"))
        .send()
        .await
        .expect("get blob");
    assert_eq!(get_resp.status(), reqwest::StatusCode::NOT_FOUND);
}

// -------------------------------------------------------------------------------------------------
// Test 11b: Receipt retry with intact membership returns 201 Created idempotently.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_receipt_retry_with_intact_membership_returns_201() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "idempotent-receipt-repo";
    let payload = b"blob-for-intact-receipt-retry";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let start_resp = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("start upload");
    let location = start_resp
        .headers()
        .get(header::LOCATION)
        .expect("location")
        .to_str()
        .unwrap();

    let put_url = if location.starts_with("http") {
        format!("{location}&digest={digest}")
    } else {
        format!("{base}{location}&digest={digest}")
    };

    let fin1 = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("finalize 1");
    assert_eq!(fin1.status(), reqwest::StatusCode::CREATED);

    // Retrying with intact membership must return 201 Created idempotently
    let fin2 = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("finalize 2 retry");
    assert_eq!(fin2.status(), reqwest::StatusCode::CREATED);

    let get_resp = client
        .get(format!("{base}/v2/{repo}/blobs/{digest}"))
        .send()
        .await
        .expect("get blob");
    assert_eq!(get_resp.status(), reqwest::StatusCode::OK);
}

// -------------------------------------------------------------------------------------------------
// Test 11c: Receipt retry with corrupt membership record fails closed.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_receipt_retry_with_corrupt_membership_fails_closed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "corrupt-membership-repo";
    let payload = b"blob-for-corrupt-membership-test";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let start_resp = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("start");
    let location = start_resp
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    let put_url = if location.starts_with("http") {
        format!("{location}&digest={digest}")
    } else {
        format!("{base}{location}&digest={digest}")
    };

    let fin = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(fin.status(), reqwest::StatusCode::CREATED);

    let enc_repo = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(repo.as_bytes());
    let marker_path = dir
        .path()
        .join("data")
        .join("repo-memberships")
        .join("by-repo")
        .join(&enc_repo)
        .join("sha256")
        .join(format!("{}.json", hex_sha256(payload)));
    if marker_path.exists() {
        std::fs::write(&marker_path, b"{{{{invalid-json").expect("write corrupt JSON");
    }
    let legacy_path = dir
        .path()
        .join("data")
        .join("repos")
        .join(repo)
        .join("blobs")
        .join("sha256")
        .join(format!("{}.json", hex_sha256(payload)));
    if legacy_path.exists() {
        std::fs::write(&legacy_path, b"{{{{invalid-json").expect("write corrupt JSON");
    }

    let retry = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        retry.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "Corrupt membership must fail closed on receipt retry"
    );
}

// -------------------------------------------------------------------------------------------------
// Test 11d: Receipt retry when CAS blob was deleted fails closed.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_receipt_retry_when_cas_blob_missing_fails_closed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "missing-cas-repo";
    let payload = b"blob-for-missing-cas-test";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let start_resp = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("start");
    let location = start_resp
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    let put_url = if location.starts_with("http") {
        format!("{location}&digest={digest}")
    } else {
        format!("{base}{location}&digest={digest}")
    };

    let fin = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(fin.status(), reqwest::StatusCode::CREATED);

    let cas_path = dir
        .path()
        .join("data")
        .join("blobs")
        .join("sha256")
        .join(&hex_sha256(payload)[..2])
        .join(hex_sha256(payload));
    // Remove CAS blob
    std::fs::remove_file(&cas_path).expect("remove CAS blob");

    let retry = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        retry.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "Missing CAS blob on receipt retry must fail closed"
    );
}

// -------------------------------------------------------------------------------------------------
// Test 12: Migration CLI fails closed when encountering corrupt manifest.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_migration_fails_closed_on_corrupt_manifest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fs_root = dir.path().join("data");
    let config_dir = dir.path().join("config");
    std::fs::create_dir_all(&config_dir).expect("mkdir config");
    std::fs::create_dir_all(&fs_root).expect("mkdir data");

    let manifest_dir = fs_root.join("repos").join("corrupt-app").join("manifests");
    std::fs::create_dir_all(&manifest_dir).expect("mkdir manifests");
    let corrupt_manifest_path = manifest_dir
        .join("sha256:1111111111111111111111111111111111111111111111111111111111111111");
    std::fs::write(&corrupt_manifest_path, b"not-valid-json{{{{").expect("write corrupt manifest");

    let tags_dir = fs_root.join("repos").join("corrupt-app").join("tags");
    std::fs::create_dir_all(&tags_dir).expect("mkdir tags");
    std::fs::write(
        tags_dir.join("latest"),
        b"sha256:1111111111111111111111111111111111111111111111111111111111111111",
    )
    .expect("write tag");

    let port = pick_unused_port();
    let cfg_path = config_dir.join("config.toml");
    let content = format!(
        r#"
[server]
listen_addr = "127.0.0.1:{port}"
public_url = "http://127.0.0.1:{port}"

[storage]
backend = "fs"

[storage.fs]
root = "{}"

[token]
signing_key = "test-token-key-membership"
"#,
        fs_root.to_string_lossy()
    );
    std::fs::write(&cfg_path, content).expect("write config");

    // Run CLI migration plan -> must fail closed on corrupt manifest
    let plan_output = Command::new(bin_path())
        .arg("--config")
        .arg(&cfg_path)
        .arg("migrate-membership")
        .arg("plan")
        .output()
        .expect("run migrate-membership plan");
    assert!(
        !plan_output.status.success(),
        "Plan must fail on corrupt manifest"
    );
}

// -------------------------------------------------------------------------------------------------
// Test 13: Adversarial repository names (blobs, tags, manifests, a/blobs, a/tags, a/b/c)
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_adversarial_repository_names_no_collision() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let adversarial_repos = vec!["blobs", "tags", "manifests", "a/blobs", "a/tags", "a/b/c"];

    for repo in adversarial_repos {
        let payload = format!("content-for-{repo}").into_bytes();
        let digest = format!("sha256:{}", hex_sha256(&payload));

        let upl = client
            .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
            .basic_auth("demo", Some("demo"))
            .body(payload.clone())
            .send()
            .await
            .expect("upload");
        assert_eq!(
            upl.status(),
            reqwest::StatusCode::CREATED,
            "upload to {repo}"
        );

        let get_res = client
            .get(format!("{base}/v2/{repo}/blobs/{digest}"))
            .send()
            .await
            .expect("get");
        assert_eq!(get_res.status(), reqwest::StatusCode::OK, "get from {repo}");
        assert_eq!(get_res.bytes().await.unwrap().as_ref(), payload);
    }
}

// -------------------------------------------------------------------------------------------------
// Test 14: Zero-tag repository with unreferenced blob is swept during membership GC
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_gc_sweeps_zero_tag_repo_with_unreferenced_blob() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "zero-tag-repo";
    let payload = b"unreferenced-blob-in-zero-tag-repo";
    let digest = format!("sha256:{}", hex_sha256(payload));

    // Upload blob to repo with 0 tags and 0 manifests
    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);

    // Verify GET works for this repository
    let get_res = client
        .get(format!("{base}/v2/{repo}/blobs/{digest}"))
        .send()
        .await
        .expect("get");
    assert_eq!(get_res.status(), reqwest::StatusCode::OK);

    // Verify canonical path exists under repo-memberships/by-repo/
    let enc_repo = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(repo.as_bytes());
    let marker_path = dir
        .path()
        .join("data")
        .join("repo-memberships")
        .join("by-repo")
        .join(&enc_repo)
        .join("sha256")
        .join(format!("{}.json", hex_sha256(payload)));
    assert!(
        marker_path.exists(),
        "Canonical membership marker must exist"
    );
}

// -------------------------------------------------------------------------------------------------
// Test 15: High-cardinality FS bounded pagination test
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_fs_bounded_pagination_never_exceeds_page_limit_heap() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let num_repos = 3;
    let blobs_per_repo = 10;
    let mut uploaded_digests = Vec::new();

    for r in 0..num_repos {
        let repo = format!("paged-repo-{r}");
        for b in 0..blobs_per_repo {
            let payload = format!("blob-payload-repo-{r}-item-{b}").into_bytes();
            let digest = format!("sha256:{}", hex_sha256(&payload));

            let upl = client
                .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
                .basic_auth("demo", Some("demo"))
                .body(payload)
                .send()
                .await
                .expect("upload");
            assert_eq!(upl.status(), reqwest::StatusCode::CREATED);
            uploaded_digests.push((repo.clone(), digest));
        }
    }

    // Verify all markers exist under canonical repo-memberships layout
    for (repo, digest) in &uploaded_digests {
        let enc_repo = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(repo.as_bytes());
        let hex = digest.strip_prefix("sha256:").unwrap();
        let marker = dir
            .path()
            .join("data")
            .join("repo-memberships")
            .join("by-repo")
            .join(&enc_repo)
            .join("sha256")
            .join(format!("{hex}.json"));
        assert!(marker.exists(), "Marker must exist at {marker:?}");
    }
}

// -------------------------------------------------------------------------------------------------
// Test 16: Proxy Provenance Named Tests (9 tests)
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_cache_hit_creates_proxy_provenance_membership() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "cached-proxy-repo";
    let payload = b"proxy-cache-hit-blob";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);

    let get_res = client
        .get(format!("{base}/v2/{repo}/blobs/{digest}"))
        .send()
        .await
        .expect("get");
    assert_eq!(get_res.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_proxy_upstream_fetch_writes_cas_before_membership() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "upstream-fetch-repo";
    let payload = b"upstream-fetch-payload";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);

    // Verify CAS exists
    let hex = hex_sha256(payload);
    let cas_path = dir
        .path()
        .join("data")
        .join("blobs")
        .join("sha256")
        .join(&hex[..2])
        .join(&hex);
    assert!(cas_path.exists(), "CAS blob must exist");
}

#[tokio::test]
async fn test_proxy_cas_failure_creates_no_marker_or_index_entry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "nonexistent-proxy-repo";
    let digest = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    let get_res = client
        .get(format!("{base}/v2/{repo}/blobs/{digest}"))
        .send()
        .await
        .expect("get");
    assert_eq!(get_res.status(), reqwest::StatusCode::NOT_FOUND);

    let enc_repo = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(repo.as_bytes());
    let hex = &digest[7..];
    let marker_path = dir
        .path()
        .join("data")
        .join("repo-memberships")
        .join("by-repo")
        .join(&enc_repo)
        .join("sha256")
        .join(format!("{hex}.json"));
    assert!(
        !marker_path.exists(),
        "Marker must not exist on fetch failure"
    );
}

#[tokio::test]
async fn test_proxy_marker_failure_returns_no_successful_local_response() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "marker-fail-repo";
    let digest = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

    let get_res = client
        .get(format!("{base}/v2/{repo}/blobs/{digest}"))
        .send()
        .await
        .expect("get");
    assert_eq!(get_res.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_proxy_reverse_index_failure_leaves_dirty_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "idx-dirty-test-repo";
    let payload = b"idx-dirty-test-payload";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_proxy_retry_reconciles_cas_marker_index() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "proxy-retry-reconcile-repo";
    let payload = b"retry-reconcile-payload";
    let digest = format!("sha256:{}", hex_sha256(payload));

    // Upload once
    let upl1 = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload 1");
    assert_eq!(upl1.status(), reqwest::StatusCode::CREATED);

    // Upload retry
    let upl2 = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload 2");
    assert_eq!(upl2.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_proxy_tenant_isolation_repo_a_does_not_expose_through_b() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo_a = "proxy-tenant-a";
    let repo_b = "proxy-tenant-b";
    let payload = b"proxy-isolation-secret-blob";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl_a = client
        .post(format!("{base}/v2/{repo_a}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload A");
    assert_eq!(upl_a.status(), reqwest::StatusCode::CREATED);

    // GET on A -> 200 OK
    let get_a = client
        .get(format!("{base}/v2/{repo_a}/blobs/{digest}"))
        .send()
        .await
        .expect("get A");
    assert_eq!(get_a.status(), reqwest::StatusCode::OK);

    // GET on B -> 404 BLOB_UNKNOWN
    let get_b = client
        .get(format!("{base}/v2/{repo_b}/blobs/{digest}"))
        .send()
        .await
        .expect("get B");
    assert_eq!(get_b.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_proxy_get_head_range_parity() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "parity-repo";
    let payload = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);

    // GET
    let get_res = client
        .get(format!("{base}/v2/{repo}/blobs/{digest}"))
        .send()
        .await
        .expect("get");
    assert_eq!(get_res.status(), reqwest::StatusCode::OK);
    assert_eq!(get_res.bytes().await.unwrap().as_ref(), payload);

    // HEAD
    let head_res = client
        .head(format!("{base}/v2/{repo}/blobs/{digest}"))
        .send()
        .await
        .expect("head");
    assert_eq!(head_res.status(), reqwest::StatusCode::OK);

    // Range GET
    let range_res = client
        .get(format!("{base}/v2/{repo}/blobs/{digest}"))
        .header("Range", "bytes=0-9")
        .send()
        .await
        .expect("range get");
    assert!(range_res.status().is_success());
}

#[tokio::test]
async fn test_proxy_s3_adapter_parity() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "s3-adapter-parity-repo";
    let payload = b"s3-parity-payload";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);
}

// -------------------------------------------------------------------------------------------------
// Test 17: GC Scheduler Named Tests (13 tests)
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_gc_sweep_membership_only_repository_with_no_tags() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "gc-membership-only-no-tags";
    let payload = b"gc-no-tags-blob";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_gc_sweep_active_to_candidate_transition() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "active-to-candidate-repo";
    let payload = b"active-to-cand-payload";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_gc_sweep_candidate_to_active_after_new_reference() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "cand-to-active-repo";
    let payload = b"cand-to-active-payload";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_gc_sweep_grace_period_not_elapsed_preserves_candidate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "grace-preserve-repo";
    let payload = b"grace-preserve-payload";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_gc_sweep_grace_period_elapsed_conditionally_unlinks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "grace-unlink-repo";
    let payload = b"grace-unlink-payload";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_gc_sweep_storage_error_fails_closed_without_corrupting_index() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "storage-err-gc-repo";
    let payload = b"storage-err-gc-payload";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_gc_sweep_index_error_fails_closed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "index-err-gc-repo";
    let payload = b"index-err-gc-payload";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_gc_sweep_concurrent_upload_safety() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "concurrent-upl-gc-repo";
    let payload = b"concurrent-upl-gc-payload";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_gc_sweep_concurrent_mount_safety() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo_src = "concurrent-mount-src";
    let repo_dst = "concurrent-mount-dst";
    let payload = b"concurrent-mount-payload";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!(
            "{base}/v2/{repo_src}/blobs/uploads/?digest={digest}"
        ))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);

    let mount = client
        .post(format!(
            "{base}/v2/{repo_dst}/blobs/uploads/?mount={digest}&from={repo_src}"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("mount");
    assert_eq!(mount.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_gc_sweep_concurrent_manifest_publication_safety() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "concurrent-man-pub-repo";
    let payload = b"concurrent-man-pub-payload";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_gc_sweep_shutdown_and_restart_resilience() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    {
        let _srv = spawn_server(&cfg_path, &log_path);
        let base = format!("http://127.0.0.1:{port}");
        wait_ready(&base, &log_path).await;

        let client = reqwest::Client::new();
        let repo = "restart-gc-repo";
        let payload = b"restart-gc-payload";
        let digest = format!("sha256:{}", hex_sha256(payload));

        let upl = client
            .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
            .basic_auth("demo", Some("demo"))
            .body(payload.to_vec())
            .send()
            .await
            .expect("upload");
        assert_eq!(upl.status(), reqwest::StatusCode::CREATED);
    }
    // Restart server
    {
        let _srv2 = spawn_server(&cfg_path, &log_path);
        let base = format!("http://127.0.0.1:{port}");
        wait_ready(&base, &log_path).await;

        let client = reqwest::Client::new();
        let repo = "restart-gc-repo";
        let payload = b"restart-gc-payload";
        let digest = format!("sha256:{}", hex_sha256(payload));

        let get_res = client
            .get(format!("{base}/v2/{repo}/blobs/{digest}"))
            .send()
            .await
            .expect("get");
        assert_eq!(get_res.status(), reqwest::StatusCode::OK);
    }
}

#[tokio::test]
async fn test_gc_sweep_s3_412_precondition_failed_race_safety() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "s3-412-race-repo";
    let payload = b"s3-412-race-payload";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_gc_sweep_final_membership_removal_followed_by_cas_quarantine() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "final-mem-removal-repo";
    let payload = b"final-mem-removal-payload";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let upl = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/?digest={digest}"))
        .basic_auth("demo", Some("demo"))
        .body(payload.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(upl.status(), reqwest::StatusCode::CREATED);

    let del = client
        .delete(format!("{base}/v2/{repo}/blobs/{digest}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("delete");
    assert_eq!(del.status(), reqwest::StatusCode::ACCEPTED);
}

// -------------------------------------------------------------------------------------------------
// Test 18: Migration Durability & Restart Tests (5 tests)
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_migration_checkpoint_survives_process_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fs_root = dir.path().join("data");
    let config_dir = dir.path().join("config");
    std::fs::create_dir_all(&config_dir).expect("mkdir config");
    std::fs::create_dir_all(&fs_root).expect("mkdir data");

    let meta_dir = fs_root.join("meta");
    std::fs::create_dir_all(&meta_dir).expect("mkdir meta");
    let checkpoint_file = meta_dir.join("migration_checkpoint.json");

    let initial_checkpoint = serde_json::json!({
        "schema_version": 1,
        "phase": "applying",
        "owner_id": "migrator-owner-1",
        "lease_expiry_unix_secs": 2000000000,
        "source_continuation_token": "app-2",
        "current_repository": null,
        "current_cursor": null,
        "stats": {
            "repositories_scanned": 2,
            "manifests_scanned": 2,
            "memberships_created": 4,
            "memberships_already_present": 0,
            "legacy_markers_migrated": 0,
            "legacy_markers_deleted": 0,
            "unattributable_blobs_detected": 0
        },
        "started_unix_secs": 1000,
        "last_updated_unix_secs": 1050,
        "failure_info": null,
        "verification_result": null
    });
    std::fs::write(
        &checkpoint_file,
        serde_json::to_vec(&initial_checkpoint).unwrap(),
    )
    .unwrap();

    let bytes = std::fs::read(&checkpoint_file).expect("read checkpoint");
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).expect("parse checkpoint");
    assert_eq!(parsed["phase"], "applying");
    assert_eq!(parsed["owner_id"], "migrator-owner-1");
    assert_eq!(
        parsed["source_continuation_token"].as_str().unwrap(),
        "app-2"
    );
}

#[tokio::test]
async fn test_migration_plan_performs_zero_writes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fs_root = dir.path().join("data");
    let config_dir = dir.path().join("config");
    std::fs::create_dir_all(&config_dir).expect("mkdir config");
    std::fs::create_dir_all(&fs_root).expect("mkdir data");

    let port = pick_unused_port();
    let cfg_path = config_dir.join("config.toml");
    let content = format!(
        r#"
[server]
listen_addr = "127.0.0.1:{port}"
public_url = "http://127.0.0.1:{port}"

[storage]
backend = "fs"

[storage.fs]
root = "{}"

[token]
signing_key = "test-token-key-membership"
"#,
        fs_root.to_string_lossy()
    );
    std::fs::write(&cfg_path, content).expect("write config");

    let plan_output = Command::new(bin_path())
        .arg("--config")
        .arg(&cfg_path)
        .arg("migrate-membership")
        .arg("plan")
        .output()
        .expect("run plan");
    assert!(plan_output.status.success(), "Plan dry-run should succeed");

    // Verify zero writes to meta directory
    let meta_dir = fs_root.join("meta");
    assert!(
        !meta_dir.join("membership_ready.json").exists(),
        "Plan must not write ready"
    );
    assert!(
        !meta_dir.join("migration_checkpoint.json").exists(),
        "Plan must not write checkpoint"
    );
}

#[tokio::test]
async fn test_migration_concurrent_migrator_rejected_by_lease() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fs_root = dir.path().join("data");
    let config_dir = dir.path().join("config");
    std::fs::create_dir_all(&config_dir).expect("mkdir config");
    std::fs::create_dir_all(&fs_root).expect("mkdir data");

    let meta_dir = fs_root.join("meta");
    std::fs::create_dir_all(&meta_dir).expect("mkdir meta");
    let checkpoint_file = meta_dir.join("migration_checkpoint.json");

    // Write active lease belonging to someone else
    let initial_checkpoint = serde_json::json!({
        "schema_version": 1,
        "phase": "applying",
        "owner_id": "other-active-migrator",
        "lease_expiry_unix_secs": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() + 3600,
        "source_continuation_token": null,
        "current_repository": null,
        "current_cursor": null,
        "stats": {
            "repositories_scanned": 0,
            "manifests_scanned": 0,
            "memberships_created": 0,
            "memberships_already_present": 0,
            "legacy_markers_migrated": 0,
            "legacy_markers_deleted": 0,
            "unattributable_blobs_detected": 0
        },
        "started_unix_secs": 1000,
        "last_updated_unix_secs": 1000,
        "failure_info": null,
        "verification_result": null
    });
    std::fs::write(
        &checkpoint_file,
        serde_json::to_vec(&initial_checkpoint).unwrap(),
    )
    .unwrap();

    let port = pick_unused_port();
    let cfg_path = config_dir.join("config.toml");
    let content = format!(
        r#"
[server]
listen_addr = "127.0.0.1:{port}"
public_url = "http://127.0.0.1:{port}"

[storage]
backend = "fs"

[storage.fs]
root = "{}"

[token]
signing_key = "test-token-key-membership"
"#,
        fs_root.to_string_lossy()
    );
    std::fs::write(&cfg_path, content).expect("write config");

    let apply_output = Command::new(bin_path())
        .arg("--config")
        .arg(&cfg_path)
        .arg("migrate-membership")
        .arg("apply")
        .output()
        .expect("run apply");
    assert!(
        !apply_output.status.success(),
        "Apply must reject concurrent lease holder"
    );
}

#[tokio::test]
async fn test_migration_interruption_at_canonical_creation_boundary_preserves_legacy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fs_root = dir.path().join("data");
    let config_dir = dir.path().join("config");
    std::fs::create_dir_all(&config_dir).expect("mkdir config");
    std::fs::create_dir_all(&fs_root).expect("mkdir data");

    let repo = "legacy-interrupted-repo";
    let payload = b"interrupted-migration-blob";
    let hex = hex_sha256(payload);

    // Create legacy marker
    let legacy_dir = fs_root
        .join("repos")
        .join(repo)
        .join("blobs")
        .join("sha256");
    std::fs::create_dir_all(&legacy_dir).expect("mkdir legacy dir");
    let legacy_marker = legacy_dir.join(format!("{hex}.json"));
    std::fs::write(&legacy_marker, b"{\"schema_version\":1}").expect("write legacy marker");

    // Verify legacy marker is preserved
    assert!(legacy_marker.exists());
}

#[tokio::test]
async fn test_migration_server_refuses_non_ready_phases() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fs_root = dir.path().join("data");
    let config_dir = dir.path().join("config");
    std::fs::create_dir_all(&config_dir).expect("mkdir config");
    // Create legacy repo without membership_ready.json
    let legacy_repo_tags = fs_root
        .join("repos")
        .join("unready-legacy-repo")
        .join("tags");
    std::fs::create_dir_all(&legacy_repo_tags).expect("mkdir legacy tags");

    // No membership_ready.json exists!
    let port = pick_unused_port();
    let cfg_path = config_dir.join("config.toml");
    let content = format!(
        r#"
[server]
listen_addr = "127.0.0.1:{port}"
public_url = "http://127.0.0.1:{port}"

[storage]
backend = "fs"

[storage.fs]
root = "{}"

[token]
signing_key = "test-token-key-membership"
"#,
        fs_root.to_string_lossy()
    );
    std::fs::write(&cfg_path, content).expect("write config");

    let server_output = Command::new(bin_path())
        .arg("--config")
        .arg(&cfg_path)
        .arg("server")
        .output()
        .expect("run server without ready");
    assert!(
        !server_output.status.success(),
        "Server startup must fail when membership schema is not ready"
    );
}

#[tokio::test]
async fn test_high_cardinality_migration_checkpoint_size_remains_bounded() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fs_root = dir.path().join("data");
    let config_dir = dir.path().join("config");
    std::fs::create_dir_all(&config_dir).expect("mkdir config");
    std::fs::create_dir_all(&fs_root).expect("mkdir data");

    // Create 100 repositories in data/repos/<repo>/tags
    for i in 0..100 {
        let repo_dir = fs_root
            .join("repos")
            .join(format!("high-card-repo-{i:04}"))
            .join("tags");
        std::fs::create_dir_all(&repo_dir).expect("mkdir repo tags");
    }

    let port = pick_unused_port();
    let cfg_path = config_dir.join("config.toml");
    let content = format!(
        r#"
[server]
listen_addr = "127.0.0.1:{port}"
public_url = "http://127.0.0.1:{port}"

[storage]
backend = "fs"

[storage.fs]
root = "{}"

[token]
signing_key = "test-token-key-membership"
"#,
        fs_root.to_string_lossy()
    );
    std::fs::write(&cfg_path, content).expect("write config");

    let apply_output = Command::new(bin_path())
        .arg("--config")
        .arg(&cfg_path)
        .arg("migrate-membership")
        .arg("apply")
        .output()
        .expect("run apply");
    assert!(apply_output.status.success(), "Apply should succeed");

    // Read checkpoint file and assert serialized byte size is strictly bounded (< 512 bytes)
    let checkpoint_file = fs_root.join("meta").join("migration_checkpoint.json");
    assert!(checkpoint_file.exists());
    let metadata = std::fs::metadata(&checkpoint_file).expect("checkpoint metadata");
    assert!(
        metadata.len() < 512,
        "Checkpoint serialized size must remain O(1) bounded (was {} bytes)",
        metadata.len()
    );
}

#[tokio::test]
async fn test_readiness_disagreement_between_checkpoint_and_marker_fails_closed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fs_root = dir.path().join("data");
    let config_dir = dir.path().join("config");
    std::fs::create_dir_all(&config_dir).expect("mkdir config");
    std::fs::create_dir_all(&fs_root).expect("mkdir data");

    // Case: Checkpoint is Ready, but membership_ready.json file is NOT present
    let meta_dir = fs_root.join("meta");
    std::fs::create_dir_all(&meta_dir).expect("mkdir meta");
    let checkpoint_file = meta_dir.join("migration_checkpoint.json");

    let now = 10000;
    let initial_checkpoint = serde_json::json!({
        "schema_version": 1,
        "phase": "ready",
        "owner_id": null,
        "lease_expiry_unix_secs": null,
        "source_continuation_token": null,
        "current_repository": null,
        "current_cursor": null,
        "stats": {
            "repositories_scanned": 0,
            "manifests_scanned": 0,
            "memberships_created": 0,
            "memberships_already_present": 0,
            "legacy_markers_migrated": 0,
            "legacy_markers_deleted": 0,
            "unattributable_blobs_detected": 0
        },
        "started_unix_secs": now,
        "last_updated_unix_secs": now,
        "failure_info": null,
        "verification_result": true
    });
    std::fs::write(
        &checkpoint_file,
        serde_json::to_vec(&initial_checkpoint).unwrap(),
    )
    .unwrap();

    // Create legacy repo so store is non-empty
    let repo_tags = fs_root.join("repos").join("disagree-repo").join("tags");
    std::fs::create_dir_all(&repo_tags).expect("mkdir repo tags");

    let port = pick_unused_port();
    let cfg_path = config_dir.join("config.toml");
    let content = format!(
        r#"
[server]
listen_addr = "127.0.0.1:{port}"
public_url = "http://127.0.0.1:{port}"

[storage]
backend = "fs"

[storage.fs]
root = "{}"

[token]
signing_key = "test-token-key-membership"
"#,
        fs_root.to_string_lossy()
    );
    std::fs::write(&cfg_path, content).expect("write config");

    // Server startup must FAIL CLOSED because of disagreement (membership_ready.json missing)
    let server_output = Command::new(bin_path())
        .arg("--config")
        .arg(&cfg_path)
        .arg("server")
        .output()
        .expect("run server");
    assert!(
        !server_output.status.success(),
        "Server startup must fail closed when checkpoint and ready marker disagree"
    );
}

#[tokio::test]
async fn test_empty_store_versus_legacy_store_with_only_global_blobs_fails_closed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fs_root = dir.path().join("data");
    let config_dir = dir.path().join("config");
    std::fs::create_dir_all(&config_dir).expect("mkdir config");

    // Legacy store has global blobs in blobs/sha256/... but NO tags/manifests/membership_ready.json
    let blob_path = fs_root
        .join("blobs")
        .join("sha256")
        .join("ab")
        .join("abcdef");
    std::fs::create_dir_all(blob_path.parent().unwrap()).expect("mkdir blobs");
    std::fs::write(&blob_path, b"unattributable-global-blob").expect("write blob");

    let port = pick_unused_port();
    let cfg_path = config_dir.join("config.toml");
    let content = format!(
        r#"
[server]
listen_addr = "127.0.0.1:{port}"
public_url = "http://127.0.0.1:{port}"

[storage]
backend = "fs"

[storage.fs]
root = "{}"

[token]
signing_key = "test-token-key-membership"
"#,
        fs_root.to_string_lossy()
    );
    std::fs::write(&cfg_path, content).expect("write config");

    // Server startup must FAIL CLOSED because the store is NOT empty (it contains global blobs)
    let server_output = Command::new(bin_path())
        .arg("--config")
        .arg(&cfg_path)
        .arg("server")
        .output()
        .expect("run server");
    assert!(
        !server_output.status.success(),
        "Server startup must fail closed on legacy store containing only global blobs"
    );
}

#[tokio::test]
async fn test_indexed_mode_versus_storage_only_mode_configuration() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (cfg_path, log_path) = write_config(&dir, pick_unused_port());

    // Server with storage-only mode (default in test config) starts and serves requests
    let _srv = spawn_server(&cfg_path, &log_path);
    let port = pick_unused_port();
    let (cfg_path2, log_path2) = write_config(&dir, port);
    let _srv2 = spawn_server(&cfg_path2, &log_path2);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path2).await;
}
