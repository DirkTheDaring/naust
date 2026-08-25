use reqwest::header;
use serde_json::json;
use sha2::Digest as _;
use std::io::Write as _;
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

fn live_blob_path(fs_root: &Path, hex: &str) -> PathBuf {
    fs_root
        .join("blobs")
        .join("sha256")
        .join(&hex[0..2])
        .join(hex)
}

fn quarantine_blob_path(fs_root: &Path, hex: &str) -> PathBuf {
    fs_root
        .join("quarantine")
        .join("blobs")
        .join("sha256")
        .join(&hex[0..2])
        .join(hex)
}

fn quarantine_meta_path(fs_root: &Path, hex: &str) -> PathBuf {
    fs_root
        .join("quarantine")
        .join("meta")
        .join("sha256")
        .join(&hex[0..2])
        .join(format!("{hex}.ts"))
}

fn bin_path() -> String {
    // Prefer Cargo-provided env var when available, otherwise derive the path from the
    // integration test executable location (target/<profile>/deps/...).
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

fn write_config(
    dir: &tempfile::TempDir,
    port: u16,
    blob_gc_enabled: bool,
    blob_gc_enable_delete: bool,
) -> PathBuf {
    let fs_root = dir.path().join("data");
    let ref_index = dir.path().join("ref-index");

    // Pre-create the on-disk layout the server expects so ref-index initialization
    // (which may scan storage) doesn't fail due to missing directories.
    std::fs::create_dir_all(fs_root.join("blobs").join("sha256")).expect("mkdir blobs");
    std::fs::create_dir_all(fs_root.join("repos")).expect("mkdir repos");
    std::fs::create_dir_all(fs_root.join("uploads")).expect("mkdir uploads");
    std::fs::create_dir_all(fs_root.join("quarantine").join("blobs").join("sha256"))
        .expect("mkdir quarantine blobs");
    std::fs::create_dir_all(fs_root.join("quarantine").join("meta").join("sha256"))
        .expect("mkdir quarantine meta");
    std::fs::create_dir_all(&ref_index).expect("mkdir ref-index");

    let toml = format!(
        r#"
[server]
listen_addr = "127.0.0.1:{port}"
public_url = "http://127.0.0.1:{port}"

[auth.push]
mode = "basic_or_token"
username = "demo"
password = "demo"
allow_repos = ["*"]

[token]
service = "registry-rust"
ttl_secs = 600
[[token.signing_keys]]
kid = "k1"
key = "test-key"

[storage]
backend = "fs"

[storage.ref_index]
enabled = true
path = "{ref_index}"
rebuild_on_start = false
auto_rebuild_on_corruption = true

[storage.fs]
root = "{fs_root}"

[admin_api]
enabled = true
username = "admin"
password = "pw"

[blob_gc]
enabled = {blob_gc_enabled}
enable_delete = {blob_gc_enable_delete}
finalize_grace_secs = 259200
default_min_age_secs = 604800
default_quarantine_delay_secs = 86400
default_max_blobs = 1000
default_max_seconds = 60
"#,
        fs_root = fs_root.display(),
        ref_index = ref_index.display(),
    );

    let path = dir.path().join("config.toml");
    let mut f = std::fs::File::create(&path).expect("create config.toml");
    f.write_all(toml.as_bytes()).expect("write config.toml");
    path
}

fn spawn_server(cfg_path: &Path, log_path: PathBuf) -> ServerGuard {
    let log = std::fs::File::create(&log_path).expect("create server log");
    let log2 = log.try_clone().expect("clone log");

    let child = Command::new(bin_path())
        .arg("server")
        .arg("--config")
        .arg(cfg_path)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log2))
        .spawn()
        .expect("spawn registry server");

    ServerGuard { child }
}

async fn wait_ready(base: &str, log_path: &Path) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("client");

    let url = format!("{base}/v2/");

    let mut last_err: Option<String> = None;
    for _ in 0..100 {
        match client.get(&url).send().await {
            Ok(resp) => {
                // Server is up if it returns any valid HTTP response.
                // /v2/ may be 200 or 401 depending on auth config.
                if resp.status().as_u16() >= 200 {
                    return;
                }
            }
            Err(e) => last_err = Some(e.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let log = std::fs::read_to_string(log_path).unwrap_or_else(|_| "<no log>".to_string());
    let tail = if log.len() > 8000 {
        &log[log.len() - 8000..]
    } else {
        &log
    };

    panic!("server did not become ready: {last_err:?}\n--- server log tail ---\n{tail}");
}

async fn admin_post_json(base: &str, path: &str, body: serde_json::Value) -> reqwest::Response {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("client");

    client
        .post(format!("{base}{path}"))
        .basic_auth("admin", Some("pw"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.to_string())
        .send()
        .await
        .expect("admin POST")
}

async fn get_token(base: &str, scope: &str) -> String {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("client");

    let resp = client
        .get(format!("{base}/token"))
        .query(&[("service", "registry-rust"), ("scope", scope)])
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("token request");

    assert!(
        resp.status().is_success(),
        "token status: {}",
        resp.status()
    );
    let v: serde_json::Value = resp.json().await.expect("token json");
    v.get("token")
        .and_then(|t| t.as_str())
        .unwrap_or_default()
        .to_string()
}

async fn http_get_blob(
    base: &str,
    repo: &str,
    digest: &str,
    maybe_bearer: Option<&str>,
) -> reqwest::Response {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("client");

    let mut req = client.get(format!("{base}/v2/{repo}/blobs/{digest}"));
    if let Some(t) = maybe_bearer {
        req = req.bearer_auth(t);
    }
    req.send().await.expect("get blob")
}

async fn push_blob_via_upload(base: &str, repo: &str, bytes: &[u8]) -> String {
    let scope = format!("repository:{repo}:pull,push");
    let token = get_token(base, &scope).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("client");

    // Start upload.
    let start = client
        .post(format!("{base}/v2/{repo}/blobs/uploads/"))
        .bearer_auth(&token)
        .send()
        .await
        .expect("start upload");
    assert_eq!(
        start.status().as_u16(),
        202,
        "start status: {}",
        start.status()
    );
    let loc = start
        .headers()
        .get(header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .expect("Location header")
        .to_string();

    let upload_url = if loc.starts_with("http") {
        loc
    } else {
        format!("{base}{loc}")
    };

    // Send one chunk.
    let patch = client
        .patch(&upload_url)
        .bearer_auth(&token)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(bytes.to_vec())
        .send()
        .await
        .expect("patch upload");
    assert_eq!(
        patch.status().as_u16(),
        202,
        "patch status: {}",
        patch.status()
    );

    // Finalize.
    let hex = hex_sha256(bytes);
    let digest = format!("sha256:{hex}");

    let patch_loc = patch
        .headers()
        .get(header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(&upload_url);
    let fin_url = if patch_loc.starts_with("http") {
        patch_loc.to_string()
    } else {
        format!("{base}{patch_loc}")
    };

    let put_url = if fin_url.contains('?') {
        format!("{fin_url}&digest={digest}")
    } else {
        format!("{fin_url}?digest={digest}")
    };
    let fin = client
        .put(&put_url)
        .bearer_auth(&token)
        .body(Vec::<u8>::new())
        .send()
        .await
        .expect("finalize upload");

    assert!(
        fin.status().is_success(),
        "finalize status: {}",
        fin.status()
    );

    digest
}

#[tokio::test]
async fn phases_1_to_4_quarantine_readable_budgeted_and_pins_skip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let cfg = write_config(&dir, port, true, false);

    let log_path = dir.path().join("server.log");
    let _srv = spawn_server(&cfg, log_path.clone());
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    // Phase 4: admin auth required.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("client");
    let resp = client
        .get(format!("{base}/_admin/gc/health"))
        .send()
        .await
        .expect("health");
    assert_eq!(resp.status().as_u16(), 401);

    let ok = client
        .get(format!("{base}/_admin/gc/health"))
        .basic_auth("admin", Some("pw"))
        .send()
        .await
        .expect("health auth");
    assert!(ok.status().is_success());

    // Create an unpinned blob directly in live store.
    let blob_a = b"integration-blob-a".to_vec();
    let hex_a = hex_sha256(&blob_a);
    let digest_a = format!("sha256:{hex_a}");

    let fs_root = dir.path().join("data");
    let a_live = live_blob_path(&fs_root, &hex_a);
    std::fs::create_dir_all(a_live.parent().unwrap()).expect("mkdir live");
    std::fs::write(&a_live, &blob_a).expect("write blob A");

    // Read works before quarantine.
    let mut get = http_get_blob(&base, "myrepo", &digest_a, None).await;
    if get.status().as_u16() == 401 {
        let t = get_token(&base, "repository:myrepo:pull").await;
        get = http_get_blob(&base, "myrepo", &digest_a, Some(&t)).await;
    }
    assert!(get.status().is_success(), "GET status: {}", get.status());
    assert_eq!(
        get.bytes().await.expect("bytes").as_ref(),
        blob_a.as_slice()
    );

    // Phase 3: budgeted quarantine (max_blobs=1) + Phase 1: quarantine-readable reads.
    let q = admin_post_json(
        &base,
        "/_admin/gc/quarantine",
        json!({
            "policy": "manifest_rooted",
            "min_age_secs": 0,
            "budgets": {"max_blobs": 1, "max_seconds": 30}
        }),
    )
    .await;
    assert!(q.status().is_success(), "quarantine status: {}", q.status());

    let a_quarantine = quarantine_blob_path(&fs_root, &hex_a);
    assert!(!a_live.exists(), "blob A should no longer be live");
    assert!(a_quarantine.exists(), "blob A should be quarantined");

    // Read still works after quarantine (Phase 1).
    let mut get2 = http_get_blob(&base, "myrepo", &digest_a, None).await;
    if get2.status().as_u16() == 401 {
        let t = get_token(&base, "repository:myrepo:pull").await;
        get2 = http_get_blob(&base, "myrepo", &digest_a, Some(&t)).await;
    }
    assert!(get2.status().is_success(), "GET2 status: {}", get2.status());
    assert_eq!(
        get2.bytes().await.expect("bytes").as_ref(),
        blob_a.as_slice()
    );

    // Phase 2: finalize pinning -> pinned blob should be skipped by quarantine.
    let pinned_digest = push_blob_via_upload(&base, "myrepo", b"pinned-finalize-blob").await;
    let pinned_hex = pinned_digest.strip_prefix("sha256:").unwrap();
    let pinned_live = live_blob_path(&fs_root, pinned_hex);

    // Create another unpinned blob in live store.
    let blob_b = b"integration-blob-b".to_vec();
    let hex_b = hex_sha256(&blob_b);
    let digest_b = format!("sha256:{hex_b}");
    let b_live = live_blob_path(&fs_root, &hex_b);
    std::fs::create_dir_all(b_live.parent().unwrap()).expect("mkdir live b");
    std::fs::write(&b_live, &blob_b).expect("write blob B");

    // Quarantine everything eligible (budget large): should move B but not the pinned blob.
    let q2 = admin_post_json(
        &base,
        "/_admin/gc/quarantine",
        json!({
            "policy": "manifest_rooted",
            "min_age_secs": 0,
            "budgets": {"max_blobs": 1000, "max_seconds": 30}
        }),
    )
    .await;
    assert!(
        q2.status().is_success(),
        "quarantine2 status: {}",
        q2.status()
    );

    let b_quarantine = quarantine_blob_path(&fs_root, &hex_b);
    assert!(!b_live.exists(), "blob B should be quarantined");
    assert!(b_quarantine.exists(), "blob B should be quarantined");

    assert!(pinned_live.exists(), "pinned blob should remain live");

    // Sanity: pinned blob is readable.
    let mut getp = http_get_blob(&base, "myrepo", &pinned_digest, None).await;
    if getp.status().as_u16() == 401 {
        let t = get_token(&base, "repository:myrepo:pull").await;
        getp = http_get_blob(&base, "myrepo", &pinned_digest, Some(&t)).await;
    }
    assert!(
        getp.status().is_success(),
        "GET pinned status: {}",
        getp.status()
    );

    // Ensure we did not accidentally delete quarantine metadata path conventions.
    // (GC writes quarantine timestamps under quarantine/meta/sha256/..)
    let meta_a = quarantine_meta_path(&fs_root, &hex_a);
    assert!(meta_a.exists(), "quarantine meta should exist for blob A");

    // Avoid unused warning.
    let _ = digest_b;
}

#[tokio::test]
async fn phase_5_kill_switch_and_delete_gate_and_delete_flow() {
    // Kill switch off: quarantine should be forbidden.
    {
        let dir = tempfile::tempdir().expect("tempdir");
        let port = pick_unused_port();
        let cfg = write_config(&dir, port, false, false);

        let log_path = dir.path().join("server.log");
        let _srv = spawn_server(&cfg, log_path.clone());
        let base = format!("http://127.0.0.1:{port}");
        wait_ready(&base, &log_path).await;

        let resp = admin_post_json(
            &base,
            "/_admin/gc/quarantine",
            json!({"policy": "manifest_rooted", "min_age_secs": 0}),
        )
        .await;
        assert_eq!(resp.status().as_u16(), 403);
    }

    // Delete gate off: delete should be forbidden even if GC is enabled.
    {
        let dir = tempfile::tempdir().expect("tempdir");
        let port = pick_unused_port();
        let cfg = write_config(&dir, port, true, false);

        let log_path = dir.path().join("server.log");
        let _srv = spawn_server(&cfg, log_path.clone());
        let base = format!("http://127.0.0.1:{port}");
        wait_ready(&base, &log_path).await;

        let resp = admin_post_json(
            &base,
            "/_admin/gc/delete",
            json!({"policy": "manifest_rooted", "quarantine_delay_secs": 0}),
        )
        .await;
        assert_eq!(resp.status().as_u16(), 403);
    }

    // Full flow: quarantine then delete with quarantine_delay=0.
    {
        let dir = tempfile::tempdir().expect("tempdir");
        let port = pick_unused_port();
        let cfg = write_config(&dir, port, true, true);

        let log_path = dir.path().join("server.log");
        let _srv = spawn_server(&cfg, log_path.clone());
        let base = format!("http://127.0.0.1:{port}");
        wait_ready(&base, &log_path).await;

        let fs_root = dir.path().join("data");

        // Create a live blob and ensure it is readable.
        let blob = b"delete-me".to_vec();
        let hex = hex_sha256(&blob);
        let digest = format!("sha256:{hex}");
        let live = live_blob_path(&fs_root, &hex);
        std::fs::create_dir_all(live.parent().unwrap()).expect("mkdir live");
        std::fs::write(&live, &blob).expect("write blob");

        let mut get = http_get_blob(&base, "myrepo", &digest, None).await;
        if get.status().as_u16() == 401 {
            let t = get_token(&base, "repository:myrepo:pull").await;
            get = http_get_blob(&base, "myrepo", &digest, Some(&t)).await;
        }
        assert!(get.status().is_success());

        // Quarantine it.
        let q = admin_post_json(
            &base,
            "/_admin/gc/quarantine",
            json!({"policy": "manifest_rooted", "min_age_secs": 0, "budgets": {"max_blobs": 1000}}),
        )
        .await;
        assert!(q.status().is_success());

        let quarantined = quarantine_blob_path(&fs_root, &hex);
        assert!(!live.exists());
        assert!(quarantined.exists());

        // Delete immediately.
        let d = admin_post_json(
            &base,
            "/_admin/gc/delete",
            json!({"policy": "manifest_rooted", "quarantine_delay_secs": 0, "budgets": {"max_blobs": 1000}}),
        )
        .await;
        assert!(d.status().is_success(), "delete status: {}", d.status());

        assert!(
            !quarantined.exists(),
            "blob should be deleted from quarantine"
        );
        assert!(
            !quarantine_meta_path(&fs_root, &hex).exists(),
            "meta should be removed"
        );

        // Now reads should fail.
        let resp = http_get_blob(&base, "myrepo", &digest, None).await;
        assert!(resp.status().as_u16() == 404 || resp.status().as_u16() == 401);
    }
}

#[tokio::test]
async fn test_gc_quarantine_aborts_when_manifest_unparsable_and_protects_blobs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let cfg_path = write_config(&dir, port, true, true);
    let fs_root = dir.path().join("data");

    let log_path = dir.path().join("server.log");
    let _srv = spawn_server(&cfg_path, log_path.clone());
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &log_path).await;

    // 1. Create an unreferenced blob
    let blob = b"potentially-orphan-blob".to_vec();
    let hex = hex_sha256(&blob);
    let live = live_blob_path(&fs_root, &hex);
    std::fs::create_dir_all(live.parent().unwrap()).expect("mkdir live");
    std::fs::write(&live, &blob).expect("write blob");

    // 2. Create a malformed manifest on disk
    let malformed_hex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let malformed_manifest = serde_json::json!({
        "schemaVersion": 2,
        "config": { "digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" },
        "layers": [{ "digest": "sha256:not-valid-hex-digest-at-all" }]
    });
    let manifests_dir = fs_root.join("repos").join("myrepo").join("manifests");
    std::fs::create_dir_all(&manifests_dir).expect("mkdir manifests");
    let malformed_path = manifests_dir.join(malformed_hex);
    std::fs::write(
        &malformed_path,
        serde_json::to_vec(&malformed_manifest).unwrap(),
    )
    .expect("write malformed manifest");

    // 3. Attempt to run GC quarantine: must fail and NOT quarantine the blob
    let q = admin_post_json(
        &base,
        "/_admin/gc/quarantine",
        json!({"policy": "manifest_rooted", "min_age_secs": 0, "budgets": {"max_blobs": 1000}}),
    )
    .await;
    assert_eq!(q.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);

    let quarantined = quarantine_blob_path(&fs_root, &hex);
    assert!(live.exists(), "live blob must still exist");
    assert!(
        !quarantined.exists(),
        "blob must NOT be quarantined when graph is incomplete"
    );

    // 4. Remove the malformed manifest and rerun quarantine
    std::fs::remove_file(&malformed_path).expect("remove malformed manifest");

    let q2 = admin_post_json(
        &base,
        "/_admin/gc/quarantine",
        json!({"policy": "manifest_rooted", "min_age_secs": 0, "budgets": {"max_blobs": 1000}}),
    )
    .await;
    assert!(
        q2.status().is_success(),
        "quarantine must succeed after removing malformed manifest"
    );
    assert!(!live.exists(), "blob should now be quarantined");
    assert!(quarantined.exists(), "blob should now exist in quarantine");
}
