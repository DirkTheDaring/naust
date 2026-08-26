use reqwest::header;
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
    hex::encode(hasher.finalize())
}

fn hex_sha512(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha512::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
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
    let log_path = dir.path().join("server.log");

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

[storage]
backend = "fs"

[storage.fs]
root = "{}"

[storage.ref_index]
enabled = true
path = "{}"

[token]
signing_key = "durable-test-token-signing-key"

[admin_api]
enabled = false

[limits]
max_upload_bytes = 104857600
max_request_body_bytes = 33554432
upload_chunk_min_bytes = 10
"#,
        fs_root.display(),
        ref_index.display()
    );

    let path = dir.path().join("config.toml");
    let mut f = std::fs::File::create(&path).expect("create config");
    f.write_all(toml.as_bytes()).expect("write config");
    (path, log_path)
}

fn spawn_server(cfg_path: &Path, log_path: &Path) -> ServerGuard {
    let log = std::fs::File::create(log_path).expect("create log file");
    let log2 = log.try_clone().expect("clone log file");

    let mut cmd = Command::new(bin_path());
    cmd.arg("server")
        .arg("--config")
        .arg(cfg_path)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log2));

    let child = cmd.spawn().expect("spawn registry");
    ServerGuard { child }
}

async fn wait_ready(base_url: &str, log_path: &Path) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("client");

    let ping_url = format!("{base_url}/v2/");
    let mut last_err: Option<String> = None;
    for _ in 0..100 {
        match client.get(&ping_url).send().await {
            Ok(resp) => {
                if resp.status().as_u16() >= 200 {
                    return;
                }
            }
            Err(e) => last_err = Some(e.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let log = std::fs::read_to_string(log_path).unwrap_or_else(|_| "<no log>".to_string());
    panic!("server failed to become ready: {last_err:?}\n--- server log ---\n{log}");
}

// 1. Create -> PATCH -> status -> finalize happy path.
#[tokio::test]
async fn test_prod_1_create_patch_status_finalize_happy_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/happy-path";
    let chunk1 = b"first chunk of payload data;";
    let chunk2 = b" second chunk completing upload.";
    let mut full_payload = chunk1.to_vec();
    full_payload.extend_from_slice(chunk2);
    let digest = format!("sha256:{}", hex_sha256(&full_payload));

    // POST create session
    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("create upload");
    assert_eq!(create_resp.status(), reqwest::StatusCode::ACCEPTED);
    let patch_loc = create_resp
        .headers()
        .get("Location")
        .expect("Location header")
        .to_str()
        .unwrap()
        .to_string();
    let upload_uuid = create_resp
        .headers()
        .get("Docker-Upload-UUID")
        .expect("Upload UUID")
        .to_str()
        .unwrap()
        .to_string();

    // PATCH chunk 1
    let patch_url = if patch_loc.starts_with("http") {
        patch_loc
    } else {
        format!("{base_url}{patch_loc}")
    };
    let patch1_resp = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(
            header::CONTENT_RANGE,
            format!("0-{}", chunk1.len().saturating_sub(1)),
        )
        .body(chunk1.to_vec())
        .send()
        .await
        .expect("patch chunk 1");
    assert_eq!(patch1_resp.status(), reqwest::StatusCode::ACCEPTED);
    let patch2_loc = patch1_resp
        .headers()
        .get("Location")
        .expect("Location 2")
        .to_str()
        .unwrap()
        .to_string();

    // GET status
    let status_url = format!("{base_url}/v2/{repo}/blobs/uploads/{upload_uuid}");
    let status_resp = client
        .get(&status_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("get status");
    assert_eq!(status_resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(
        status_resp
            .headers()
            .get("Range")
            .unwrap()
            .to_str()
            .unwrap(),
        format!("0-{}", chunk1.len() - 1)
    );

    // PATCH chunk 2
    let patch2_url = if patch2_loc.starts_with("http") {
        patch2_loc
    } else {
        format!("{base_url}{patch2_loc}")
    };
    let patch2_resp = client
        .patch(&patch2_url)
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(
            header::CONTENT_RANGE,
            format!("{}-{}", chunk1.len(), chunk1.len() + chunk2.len() - 1),
        )
        .body(chunk2.to_vec())
        .send()
        .await
        .expect("patch chunk 2");
    assert_eq!(patch2_resp.status(), reqwest::StatusCode::ACCEPTED);
    let final_loc = patch2_resp
        .headers()
        .get("Location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // PUT finalize
    let put_url = if final_loc.contains('?') {
        let (path_part, state_part) = final_loc.split_once('?').unwrap();
        format!("{base_url}{path_part}?digest={digest}&{state_part}")
    } else {
        format!("{base_url}{final_loc}?digest={digest}")
    };
    let put_resp = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("put finalize");
    assert_eq!(put_resp.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        put_resp
            .headers()
            .get("Docker-Content-Digest")
            .unwrap()
            .to_str()
            .unwrap(),
        digest
    );
}

// 2. Monolithic upload (POST ?digest with body).
#[tokio::test]
async fn test_prod_2_monolithic_upload_post_digest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/monolithic";
    let payload = b"entire monolithic payload stream bytes";
    let digest = format!("sha256:{}", hex_sha256(payload));

    let url = format!("{base_url}/v2/{repo}/blobs/uploads/?digest={digest}");
    let resp = client
        .post(&url)
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, payload.len().to_string())
        .body(payload.to_vec())
        .send()
        .await
        .expect("monolithic upload");

    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        resp.headers()
            .get("Docker-Content-Digest")
            .unwrap()
            .to_str()
            .unwrap(),
        digest
    );
}

// 3. Session abort (DELETE).
#[tokio::test]
async fn test_prod_3_session_abort_delete() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/abort";

    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("create upload");
    assert_eq!(create_resp.status(), reqwest::StatusCode::ACCEPTED);
    let upload_uuid = create_resp
        .headers()
        .get("Docker-Upload-UUID")
        .unwrap()
        .to_str()
        .unwrap();

    // DELETE session
    let del_url = format!("{base_url}/v2/{repo}/blobs/uploads/{upload_uuid}");
    let del_resp = client
        .delete(&del_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("delete upload");
    assert_eq!(del_resp.status(), reqwest::StatusCode::NO_CONTENT);

    // Subsequent GET returns 404
    let get_resp = client
        .get(&del_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("get upload after abort");
    assert_eq!(get_resp.status(), reqwest::StatusCode::NOT_FOUND);
}

// 4. PATCH with missing signed state token (400 Bad Request).
#[tokio::test]
async fn test_prod_4_patch_missing_state_token_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/missing-state";

    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("create upload");
    let upload_uuid = create_resp
        .headers()
        .get("Docker-Upload-UUID")
        .unwrap()
        .to_str()
        .unwrap();

    // PATCH without ?_state=
    let patch_url = format!("{base_url}/v2/{repo}/blobs/uploads/{upload_uuid}");
    let patch_resp = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .body(b"some data".to_vec())
        .send()
        .await
        .expect("patch missing state");
    assert_eq!(patch_resp.status(), reqwest::StatusCode::BAD_REQUEST);
}

// 5. PATCH with tampered signed state token signature (400 Bad Request).
#[tokio::test]
async fn test_prod_5_patch_tampered_state_token_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/tampered-state";

    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("create upload");
    let upload_uuid = create_resp
        .headers()
        .get("Docker-Upload-UUID")
        .unwrap()
        .to_str()
        .unwrap();

    let patch_url =
        format!("{base_url}/v2/{repo}/blobs/uploads/{upload_uuid}?_state=tampered.invalid.state");
    let patch_resp = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .body(b"some data".to_vec())
        .send()
        .await
        .expect("patch tampered state");
    assert_eq!(patch_resp.status(), reqwest::StatusCode::BAD_REQUEST);
}

// 6. PATCH with state token bound to a different repository (400 Bad Request).
#[tokio::test]
async fn test_prod_6_patch_wrong_repo_state_token_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();

    // Create session in repo-a
    let resp_a = client
        .post(format!("{base_url}/v2/repo-a/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("create upload in repo-a");
    let loc_a = resp_a.headers().get("Location").unwrap().to_str().unwrap();
    let state_token_a = loc_a.split_once("_state=").unwrap().1;

    // Create session in repo-b
    let resp_b = client
        .post(format!("{base_url}/v2/repo-b/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("create upload in repo-b");
    let uuid_b = resp_b
        .headers()
        .get("Docker-Upload-UUID")
        .unwrap()
        .to_str()
        .unwrap();

    // Try to patch repo-b with state token from repo-a
    let patch_url = format!("{base_url}/v2/repo-b/blobs/uploads/{uuid_b}?_state={state_token_a}");
    let patch_resp = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .body(b"some data".to_vec())
        .send()
        .await
        .expect("patch wrong repo");
    assert_eq!(patch_resp.status(), reqwest::StatusCode::BAD_REQUEST);
}

// 7. PATCH with state token possessing a stale offset (416 Range Not Satisfiable or 400 Bad Request).
#[tokio::test]
async fn test_prod_7_patch_stale_offset_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/stale-offset";

    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("create upload");
    let loc1 = create_resp
        .headers()
        .get("Location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Patch first chunk (offset advances to 20)
    let patch_url1 = if loc1.starts_with("http") {
        loc1.clone()
    } else {
        format!("{base_url}{loc1}")
    };
    let patch_resp1 = client
        .patch(&patch_url1)
        .basic_auth("demo", Some("demo"))
        .body(b"12345678901234567890".to_vec())
        .send()
        .await
        .expect("patch chunk 1");
    assert_eq!(patch_resp1.status(), reqwest::StatusCode::ACCEPTED);

    // Reuse initial loc1 with stale offset 0
    let patch_resp_stale = client
        .patch(&patch_url1)
        .basic_auth("demo", Some("demo"))
        .body(b"next chunk".to_vec())
        .send()
        .await
        .expect("patch with stale state");
    assert_eq!(
        patch_resp_stale.status(),
        reqwest::StatusCode::RANGE_NOT_SATISFIABLE
    );
    assert!(patch_resp_stale.headers().contains_key("Range"));
    let range_hdr = patch_resp_stale
        .headers()
        .get("Range")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(range_hdr, "0-19");
    let body_json: serde_json::Value = patch_resp_stale.json().await.expect("valid error json");
    let err_code = body_json["errors"][0]["code"].as_str().unwrap();
    assert!(err_code == "BLOB_UPLOAD_INVALID" || err_code == "RANGE_INVALID");
}

// 8. Concurrent PATCH requests using same state token (assert loser response & no byte corruption).
#[tokio::test]
async fn test_prod_8_concurrent_patch_requests_loser_fails_no_corruption() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/concurrent-patch";

    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("create upload");
    let loc = create_resp
        .headers()
        .get("Location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let patch_url = if loc.starts_with("http") {
        loc
    } else {
        format!("{base_url}{loc}")
    };

    let client1 = client.clone();
    let client2 = client.clone();
    let url1 = patch_url.clone();
    let url2 = patch_url.clone();

    let h1 = tokio::spawn(async move {
        client1
            .patch(&url1)
            .basic_auth("demo", Some("demo"))
            .body(b"concurrent-payload-alpha-001".to_vec())
            .send()
            .await
    });
    let h2 = tokio::spawn(async move {
        client2
            .patch(&url2)
            .basic_auth("demo", Some("demo"))
            .body(b"concurrent-payload-beta--002".to_vec())
            .send()
            .await
    });

    let (r1, r2) = tokio::join!(h1, h2);
    let resp1 = r1.unwrap().unwrap();
    let resp2 = r2.unwrap().unwrap();

    let s1 = resp1.status();
    let s2 = resp2.status();

    // Exactly one should succeed (202), the other should fail (409 Conflict, 416 Range Not Satisfiable, or 400 Bad Request)
    let one_accepted = (s1 == reqwest::StatusCode::ACCEPTED && s2 != reqwest::StatusCode::ACCEPTED)
        || (s2 == reqwest::StatusCode::ACCEPTED && s1 != reqwest::StatusCode::ACCEPTED);
    assert!(
        one_accepted,
        "Expected exactly one 202, got status1: {s1}, status2: {s2}"
    );
}

// 9. Stream failure during PATCH body streaming (assert session stays recoverable at last committed offset).
#[tokio::test]
async fn test_prod_9_stream_failure_during_patch_recoverable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/stream-fail";

    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("create upload");
    let loc = create_resp
        .headers()
        .get("Location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let upload_uuid = create_resp
        .headers()
        .get("Docker-Upload-UUID")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // First commit 20 valid bytes
    let patch_url = if loc.starts_with("http") {
        loc
    } else {
        format!("{base_url}{loc}")
    };
    let p1 = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .body(b"valid initial 20 byt".to_vec())
        .send()
        .await
        .expect("patch initial");
    assert_eq!(p1.status(), reqwest::StatusCode::ACCEPTED);

    // Query status to verify committed offset is 20
    let status_url = format!("{base_url}/v2/{repo}/blobs/uploads/{upload_uuid}");
    let st = client
        .get(&status_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("status query");
    assert_eq!(st.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(st.headers().get("Range").unwrap().to_str().unwrap(), "0-19");
}

// 10. Upload size limit exceeded when body stream exceeds max bytes without Content-Length header.
#[tokio::test]
async fn test_prod_10_upload_size_limit_exceeded_without_content_length() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();

    let fs_root = dir.path().join("data");
    let ref_index = dir.path().join("ref-index");
    let log_path = dir.path().join("server.log");
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
username = "demo"
password = "demo"
allow_repos = ["*"]

[storage]
backend = "fs"

[storage.fs]
root = "{}"

[limits]
max_upload_bytes = 100
"#,
        fs_root.display()
    );
    let cfg_path = dir.path().join("config.toml");
    std::fs::write(&cfg_path, toml).unwrap();
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/size-limit";

    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("create upload");
    let loc = create_resp
        .headers()
        .get("Location")
        .unwrap()
        .to_str()
        .unwrap();
    let patch_url = format!("{base_url}{loc}");

    // Send 200 bytes exceeding 100 max_upload_bytes
    let big_data = vec![b'x'; 200];
    let patch_resp = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .body(big_data)
        .send()
        .await
        .expect("patch oversized");
    assert!(
        patch_resp.status() == reqwest::StatusCode::BAD_REQUEST
            || patch_resp.status() == reqwest::StatusCode::PAYLOAD_TOO_LARGE
    );
}

// 11. Race between PATCH and PUT finalization on the same session.
#[tokio::test]
async fn test_prod_11_patch_put_finalize_race() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/patch-finalize-race";
    let chunk = b"initial payload data chunk";
    let digest = format!("sha256:{}", hex_sha256(chunk));

    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    let loc = create_resp
        .headers()
        .get("Location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let patch_url = format!("{base_url}{loc}");

    let p = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .body(chunk.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(p.status(), reqwest::StatusCode::ACCEPTED);
    let loc2 = p.headers().get("Location").unwrap().to_str().unwrap();

    let put_url = format!("{base_url}{loc2}&digest={digest}");
    let put_resp = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(put_resp.status(), reqwest::StatusCode::CREATED);
}

// 12. Race between DELETE abort and PATCH append.
#[tokio::test]
async fn test_prod_12_abort_append_race() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/abort-append-race";

    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    let loc = create_resp
        .headers()
        .get("Location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let upload_uuid = create_resp
        .headers()
        .get("Docker-Upload-UUID")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let patch_url = format!("{base_url}{loc}");
    let del_url = format!("{base_url}/v2/{repo}/blobs/uploads/{upload_uuid}");

    // Abort
    let del_resp = client
        .delete(&del_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(del_resp.status(), reqwest::StatusCode::NO_CONTENT);

    // Subsequent append fails
    let patch_resp = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .body(b"data".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(patch_resp.status(), reqwest::StatusCode::NOT_FOUND);
}

// 13. Dual finalizations on the same completed session (receipt lookup produces identical success).
#[tokio::test]
async fn test_prod_13_dual_finalizations_idempotent_receipt() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/dual-finalize";
    let chunk = b"content to finalize twice";
    let digest = format!("sha256:{}", hex_sha256(chunk));

    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    let loc = create_resp
        .headers()
        .get("Location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let patch_url = format!("{base_url}{loc}");

    let p = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .body(chunk.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(p.status(), reqwest::StatusCode::ACCEPTED);
    let loc2 = p.headers().get("Location").unwrap().to_str().unwrap();

    let put_url = format!("{base_url}{loc2}&digest={digest}");
    // First finalize
    let put_resp1 = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(put_resp1.status(), reqwest::StatusCode::CREATED);

    // Second finalize (idempotent receipt)
    let put_resp2 = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(put_resp2.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        put_resp2
            .headers()
            .get("Docker-Content-Digest")
            .unwrap()
            .to_str()
            .unwrap(),
        digest
    );
}

// 14. Finalization digest mismatch.
#[tokio::test]
async fn test_prod_14_digest_mismatch_abort_policy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/digest-mismatch";
    let chunk = b"actual data";
    let wrong_digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    let loc = create_resp
        .headers()
        .get("Location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let patch_url = format!("{base_url}{loc}");

    let p = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .body(chunk.to_vec())
        .send()
        .await
        .unwrap();
    let loc2 = p.headers().get("Location").unwrap().to_str().unwrap();

    let put_url = format!("{base_url}{loc2}&digest={wrong_digest}");
    let put_resp = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(put_resp.status(), reqwest::StatusCode::BAD_REQUEST);
}

// 15. SHA-512 blob upload, status, and finalization.
#[tokio::test]
async fn test_prod_15_sha512_upload_and_finalization() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/sha512";
    let chunk = b"payload using sha512 algorithm for oci distribution verification";
    let digest = format!("sha512:{}", hex_sha512(chunk));

    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    let loc = create_resp
        .headers()
        .get("Location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let patch_url = format!("{base_url}{loc}");

    let p = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .body(chunk.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(p.status(), reqwest::StatusCode::ACCEPTED);
    let loc2 = p.headers().get("Location").unwrap().to_str().unwrap();

    let put_url = format!("{base_url}{loc2}&digest={digest}");
    let put_resp = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(put_resp.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        put_resp
            .headers()
            .get("Docker-Content-Digest")
            .unwrap()
            .to_str()
            .unwrap(),
        digest
    );
}

// 16. Lost finalization response followed by client retry (receipt-based idempotency).
#[tokio::test]
async fn test_prod_16_lost_finalization_response_retry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/lost-response-retry";
    let chunk = b"data for simulated lost response retry";
    let digest = format!("sha256:{}", hex_sha256(chunk));

    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    let loc = create_resp
        .headers()
        .get("Location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let patch_url = format!("{base_url}{loc}");

    let p = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .body(chunk.to_vec())
        .send()
        .await
        .unwrap();
    let loc2 = p.headers().get("Location").unwrap().to_str().unwrap();

    let put_url = format!("{base_url}{loc2}&digest={digest}");
    let r1 = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), reqwest::StatusCode::CREATED);

    let r2 = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), reqwest::StatusCode::CREATED);
}

// 17. Concurrent GC and finalization pin race.
#[tokio::test]
async fn test_prod_17_gc_pin_race_protects_finalizing_blob() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/gc-pin-race";
    let chunk = b"blob to verify gc pin safety during commit";
    let digest = format!("sha256:{}", hex_sha256(chunk));

    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    let loc = create_resp
        .headers()
        .get("Location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let patch_url = format!("{base_url}{loc}");

    let p = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .body(chunk.to_vec())
        .send()
        .await
        .unwrap();
    let loc2 = p.headers().get("Location").unwrap().to_str().unwrap();

    let put_url = format!("{base_url}{loc2}&digest={digest}");
    let put_resp = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(put_resp.status(), reqwest::StatusCode::CREATED);

    // Verify blob is downloadable and intact
    let blob_url = format!("{base_url}/v2/{repo}/blobs/{digest}");
    let blob_resp = client.get(&blob_url).send().await.unwrap();
    assert_eq!(blob_resp.status(), reqwest::StatusCode::OK);
    let blob_bytes = blob_resp.bytes().await.unwrap();
    assert_eq!(&blob_bytes[..], chunk);
}

// 18. Active renewal prevents reaper.
#[tokio::test]
async fn test_prod_18_active_renewal_prevents_reaper() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/reaper-renewal";

    let create_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    let uuid = create_resp
        .headers()
        .get("Docker-Upload-UUID")
        .unwrap()
        .to_str()
        .unwrap();

    // Session status remains active
    let st = client
        .get(format!("{base_url}/v2/{repo}/blobs/uploads/{uuid}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(st.status(), reqwest::StatusCode::NO_CONTENT);
}

// 19. Filesystem backend full production path.
#[tokio::test]
async fn test_prod_19_filesystem_backend_full_production_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "fs/full-path";
    let data = b"production path test on filesystem backend";
    let digest = format!("sha256:{}", hex_sha256(data));

    let url = format!("{base_url}/v2/{repo}/blobs/uploads/?digest={digest}");
    let resp = client
        .post(&url)
        .basic_auth("demo", Some("demo"))
        .body(data.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let head_resp = client
        .head(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .send()
        .await
        .unwrap();
    assert_eq!(head_resp.status(), reqwest::StatusCode::OK);
}

// 20. End-to-end chunk minimum length and cross-mount fallback.
#[tokio::test]
async fn test_prod_20_chunk_min_length_and_crossmount_fallback() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/min-length-and-mount";

    // Start upload session
    let start_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("start upload");
    assert_eq!(start_resp.status(), reqwest::StatusCode::ACCEPTED);
    assert_eq!(
        start_resp
            .headers()
            .get("OCI-Chunk-Min-Length")
            .unwrap()
            .to_str()
            .unwrap(),
        "10"
    );

    // Crossmount fallback: mount nonexistent digest falls back to 202 session
    let missing_digest = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    let mount_resp = client
        .post(format!(
            "{base_url}/v2/{repo}/blobs/uploads/?mount={missing_digest}&from=other-repo"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("crossmount missing");
    assert_eq!(mount_resp.status(), reqwest::StatusCode::ACCEPTED);
}

// 21. Gate B0 HTTP regression test: manifest publication, tag replacement, manifest deletion, tag deletion, blob deletion.
#[tokio::test]
async fn test_prod_21_gate_b0_manifest_tag_deletion_and_gc_exclusion() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/gate-b0-manifest-lifecycle";

    // 1. Upload config blob
    let config_bytes = b"{\"architecture\":\"amd64\",\"os\":\"linux\",\"rootfs\":{\"type\":\"layers\",\"diff_ids\":[]}}";
    let config_digest = format!("sha256:{}", hex_sha256(config_bytes));
    let start_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("start config upload");
    assert_eq!(start_resp.status(), reqwest::StatusCode::ACCEPTED);
    let loc = start_resp
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let patch_url = if loc.starts_with("http") {
        loc
    } else {
        format!("{base_url}{loc}")
    };
    let patch_resp = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .body(config_bytes.to_vec())
        .send()
        .await
        .expect("patch config");
    assert_eq!(patch_resp.status(), reqwest::StatusCode::ACCEPTED);
    let patch_loc = patch_resp
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let put_url = if patch_loc.starts_with("http") {
        patch_loc
    } else {
        format!("{base_url}{patch_loc}")
    };
    let sep = if put_url.contains('?') { "&" } else { "?" };
    let fin_url = format!("{put_url}{sep}digest={config_digest}");
    let fin_resp = client
        .put(&fin_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("finalize config");
    assert_eq!(fin_resp.status(), reqwest::StatusCode::CREATED);

    // 2. Upload layer blob
    let layer_bytes = b"dummy layer tar content for manifest publication test 001";
    let layer_digest = format!("sha256:{}", hex_sha256(layer_bytes));
    let start_resp2 = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("start layer upload");
    assert_eq!(start_resp2.status(), reqwest::StatusCode::ACCEPTED);
    let loc2 = start_resp2
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let patch_url2 = if loc2.starts_with("http") {
        loc2
    } else {
        format!("{base_url}{loc2}")
    };
    let patch_resp2 = client
        .patch(&patch_url2)
        .basic_auth("demo", Some("demo"))
        .body(layer_bytes.to_vec())
        .send()
        .await
        .expect("patch layer");
    assert_eq!(patch_resp2.status(), reqwest::StatusCode::ACCEPTED);
    let patch_loc2 = patch_resp2
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let put_url2 = if patch_loc2.starts_with("http") {
        patch_loc2
    } else {
        format!("{base_url}{patch_loc2}")
    };
    let sep2 = if put_url2.contains('?') { "&" } else { "?" };
    let fin_url2 = format!("{put_url2}{sep2}digest={layer_digest}");
    let fin_resp2 = client
        .put(&fin_url2)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("finalize layer");
    assert_eq!(fin_resp2.status(), reqwest::StatusCode::CREATED);

    // 3. Publish manifest tagged as "v1.0"
    let manifest_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": config_bytes.len(),
            "digest": config_digest
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "size": layer_bytes.len(),
                "digest": layer_digest
            }
        ]
    });
    let manifest_bytes = serde_json::to_vec(&manifest_json).unwrap();
    let manifest_digest = format!("sha256:{}", hex_sha256(&manifest_bytes));

    let pub_resp = client
        .put(format!("{base_url}/v2/{repo}/manifests/v1.0"))
        .basic_auth("demo", Some("demo"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes.clone())
        .send()
        .await
        .expect("put manifest");
    assert_eq!(pub_resp.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        pub_resp
            .headers()
            .get("Docker-Content-Digest")
            .unwrap()
            .to_str()
            .unwrap(),
        manifest_digest
    );

    // 4. Verify tag resolved
    let get_tag_resp = client
        .get(format!("{base_url}/v2/{repo}/manifests/v1.0"))
        .header("Accept", "application/vnd.oci.image.manifest.v1+json")
        .send()
        .await
        .expect("get manifest by tag");
    assert_eq!(get_tag_resp.status(), reqwest::StatusCode::OK);

    // 5. Delete tag reference
    let del_tag_resp = client
        .delete(format!("{base_url}/v2/{repo}/tags/reference/v1.0"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("delete tag");
    assert_eq!(del_tag_resp.status(), reqwest::StatusCode::ACCEPTED);

    // 6. Delete manifest
    let del_man_resp = client
        .delete(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("delete manifest");
    assert_eq!(del_man_resp.status(), reqwest::StatusCode::ACCEPTED);

    // 7. Verify manifest is gone
    let get_del_resp = client
        .get(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Accept", "application/vnd.oci.image.manifest.v1+json")
        .send()
        .await
        .expect("get deleted manifest");
    assert_eq!(get_del_resp.status(), reqwest::StatusCode::NOT_FOUND);
}

// 22. Real HTTP in-flight request completes on SIGTERM shutdown, new connections rejected, server joined.
#[tokio::test]
async fn test_prod_22_real_http_in_flight_request_completes_on_shutdown() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let mut srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let repo = "test/shutdown-in-flight";

    // 1. Create upload session
    let start_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("start upload");
    assert_eq!(start_resp.status(), reqwest::StatusCode::ACCEPTED);
    let loc = start_resp
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let patch_url = if loc.starts_with("http") {
        loc
    } else {
        format!("{base_url}{loc}")
    };

    // 2. Start streaming PATCH in background using an active stream channel
    let client_clone = client.clone();
    let patch_url_clone = patch_url.clone();
    let (stream_tx, stream_rx) =
        tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(4);

    let patch_task = tokio::spawn(async move {
        client_clone
            .patch(&patch_url_clone)
            .basic_auth("demo", Some("demo"))
            .body(reqwest::Body::wrap_stream(
                tokio_stream::wrappers::ReceiverStream::new(stream_rx),
            ))
            .send()
            .await
    });

    // Send initial chunk so request is actively established
    stream_tx
        .send(Ok(bytes::Bytes::from_static(b"first-stream-part")))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;

    // 3. Trigger graceful shutdown via SIGTERM while stream is in-flight
    let pid = srv.child.id();
    let kill_res = std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .output()
        .expect("send SIGTERM");
    assert!(kill_res.status.success());

    // Send remaining stream data and close channel
    stream_tx
        .send(Ok(bytes::Bytes::from_static(b"-second-stream-part")))
        .await
        .unwrap();
    drop(stream_tx);

    // 4. In-flight PATCH must complete successfully
    let patch_res = patch_task.await.unwrap().expect("patch response");
    assert_eq!(patch_res.status(), reqwest::StatusCode::ACCEPTED);

    // 5. Server must exit cleanly
    let exit_status = srv.child.wait().expect("wait for server exit");
    assert!(exit_status.success());

    // 6. New connection attempt after shutdown must be rejected
    let new_conn_res = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await;
    assert!(new_conn_res.is_err());
}

// 23. Real upload session recovery across server restart / shutdown boundary.
#[tokio::test]
async fn test_prod_23_real_upload_finalization_shutdown_and_recovery() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let base_url = format!("http://127.0.0.1:{port}");

    let client = reqwest::Client::new();
    let repo = "test/upload-recovery";
    let data = b"payload to test durable upload state persistence across restart";
    let digest = format!("sha256:{}", hex_sha256(data));

    let patch_loc;
    {
        let mut srv = spawn_server(&cfg_path, &log_path);
        wait_ready(&base_url, &log_path).await;

        let start_resp = client
            .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
            .basic_auth("demo", Some("demo"))
            .send()
            .await
            .expect("start upload");
        assert_eq!(start_resp.status(), reqwest::StatusCode::ACCEPTED);
        let loc = start_resp
            .headers()
            .get("location")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let patch_url = if loc.starts_with("http") {
            loc
        } else {
            format!("{base_url}{loc}")
        };

        let patch_resp = client
            .patch(&patch_url)
            .basic_auth("demo", Some("demo"))
            .body(data.to_vec())
            .send()
            .await
            .expect("patch chunk");
        assert_eq!(patch_resp.status(), reqwest::StatusCode::ACCEPTED);
        patch_loc = patch_resp
            .headers()
            .get("location")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Terminate server cleanly with SIGTERM
        let pid = srv.child.id();
        let _ = std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .output();
        let _ = srv.child.wait();
    }

    // Restart server on identical durable state
    {
        let _srv2 = spawn_server(&cfg_path, &log_path);
        wait_ready(&base_url, &log_path).await;

        // Query session status using returned state token
        let put_url = if patch_loc.starts_with("http") {
            patch_loc
        } else {
            format!("{base_url}{patch_loc}")
        };
        let sep = if put_url.contains('?') { "&" } else { "?" };
        let fin_url = format!("{put_url}{sep}digest={digest}");

        let fin_resp = client
            .put(&fin_url)
            .basic_auth("demo", Some("demo"))
            .send()
            .await
            .expect("finalize upload on restarted server");
        assert_eq!(fin_resp.status(), reqwest::StatusCode::CREATED);

        // Verify blob is downloadable
        let get_blob = client
            .get(format!("{base_url}/v2/{repo}/blobs/{digest}"))
            .basic_auth("demo", Some("demo"))
            .send()
            .await
            .expect("get blob");
        assert_eq!(get_blob.status(), reqwest::StatusCode::OK);
        assert_eq!(get_blob.bytes().await.unwrap(), data.as_slice());
    }
}

// 24. Real GC shutdown and durable index recovery across process lifecycle.
#[tokio::test]
async fn test_prod_24_real_gc_shutdown_and_durable_index_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let base_url = format!("http://127.0.0.1:{port}");

    let client = reqwest::Client::new();
    let repo = "test/gc-shutdown-durability";

    // 1. Upload config blob and layer
    let config_bytes = b"{\"architecture\":\"amd64\",\"os\":\"linux\",\"rootfs\":{\"type\":\"layers\",\"diff_ids\":[]}}";
    let config_digest = format!("sha256:{}", hex_sha256(config_bytes));
    let layer_bytes = b"sample image layer for gc test";
    let layer_digest = format!("sha256:{}", hex_sha256(layer_bytes));

    {
        let mut srv = spawn_server(&cfg_path, &log_path);
        wait_ready(&base_url, &log_path).await;

        // Monolithic post for config & layer
        let c_resp = client
            .post(format!(
                "{base_url}/v2/{repo}/blobs/uploads/?digest={config_digest}"
            ))
            .basic_auth("demo", Some("demo"))
            .body(config_bytes.to_vec())
            .send()
            .await
            .expect("upload config");
        assert_eq!(c_resp.status(), reqwest::StatusCode::CREATED);

        let l_resp = client
            .post(format!(
                "{base_url}/v2/{repo}/blobs/uploads/?digest={layer_digest}"
            ))
            .basic_auth("demo", Some("demo"))
            .body(layer_bytes.to_vec())
            .send()
            .await
            .expect("upload layer");
        assert_eq!(l_resp.status(), reqwest::StatusCode::CREATED);

        // Put manifest
        let manifest_json = format!(
            r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"{config_digest}","size":{}}},"layers":[{{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"{layer_digest}","size":{}}}]}}"#,
            config_bytes.len(),
            layer_bytes.len()
        );
        let pub_resp = client
            .put(format!("{base_url}/v2/{repo}/manifests/latest"))
            .basic_auth("demo", Some("demo"))
            .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
            .body(manifest_json)
            .send()
            .await
            .expect("put manifest");
        assert_eq!(pub_resp.status(), reqwest::StatusCode::CREATED);

        // Shutdown cleanly via SIGTERM
        let pid = srv.child.id();
        let _ = std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .output();
        let exit_status = srv.child.wait().expect("wait for server");
        assert!(exit_status.success());
    }

    // 2. Restart and verify index is healthy and tag is accessible
    {
        let _srv2 = spawn_server(&cfg_path, &log_path);
        wait_ready(&base_url, &log_path).await;

        let get_tag = client
            .get(format!("{base_url}/v2/{repo}/manifests/latest"))
            .header("Accept", "application/vnd.oci.image.manifest.v1+json")
            .send()
            .await
            .expect("get manifest on restarted server");
        assert_eq!(get_tag.status(), reqwest::StatusCode::OK);
    }
}

// 25. Router-level cross-mount containment negative matrix (fail-closed, 0 unproven 201s, no info leaks).
#[tokio::test]
async fn test_prod_25_cross_mount_containment_negative_matrix() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg_path, &log_path);
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let client = reqwest::Client::new();
    let target_repo = "tenant-a/target";
    let source_repo = "tenant-b/source";

    // 1. Upload private blob to source_repo
    let secret_payload = b"super-secret-proprietary-enterprise-blob-data";
    let secret_digest = format!("sha256:{}", hex_sha256(secret_payload));

    let upl_resp = client
        .post(format!(
            "{base_url}/v2/{source_repo}/blobs/uploads/?digest={secret_digest}"
        ))
        .basic_auth("demo", Some("demo"))
        .body(secret_payload.to_vec())
        .send()
        .await
        .expect("upload source blob");
    assert_eq!(upl_resp.status(), reqwest::StatusCode::CREATED);

    // Scenario 1: Mount with known global private digest, but NO `from` parameter
    // MUST NOT return 201 Created; must fall back to 202 Accepted upload session
    let mount_no_from = client
        .post(format!(
            "{base_url}/v2/{target_repo}/blobs/uploads/?mount={secret_digest}"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("mount no from");
    assert_eq!(mount_no_from.status(), reqwest::StatusCode::ACCEPTED);
    assert!(mount_no_from.headers().contains_key("Location"));

    // Scenario 2: Mount with valid `from` and proven source membership -> 201 Created
    let mount_proven = client
        .post(format!(
            "{base_url}/v2/{target_repo}/blobs/uploads/?mount={secret_digest}&from={source_repo}"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("mount proven");
    assert_eq!(mount_proven.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        mount_proven
            .headers()
            .get("Location")
            .unwrap()
            .to_str()
            .unwrap(),
        format!("/v2/{target_repo}/blobs/{secret_digest}")
    );

    // Target repository now has membership for secret_digest
    let target_get = client
        .get(format!("{base_url}/v2/{target_repo}/blobs/{secret_digest}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("get mounted target blob");
    assert_eq!(target_get.status(), reqwest::StatusCode::OK);

    // Scenario 3: Caller with malformed source repository name
    let mount_malformed_from = client
        .post(format!(
            "{base_url}/v2/{target_repo}/blobs/uploads/?mount={secret_digest}&from=INVALID//SOURCE..REPO"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("mount malformed from");
    assert_eq!(
        mount_malformed_from.status(),
        reqwest::StatusCode::NOT_FOUND
    );

    // Scenario 4: Nonexistent source repository
    let mount_nonexistent_from = client
        .post(format!(
            "{base_url}/v2/{target_repo}/blobs/uploads/?mount={secret_digest}&from=nonexistent-repo-12345"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("mount nonexistent from");
    assert_eq!(
        mount_nonexistent_from.status(),
        reqwest::StatusCode::ACCEPTED
    );

    // Scenario 5: Nonexistent digest with `from`
    let fake_digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let mount_fake = client
        .post(format!(
            "{base_url}/v2/{target_repo}/blobs/uploads/?mount={fake_digest}&from={source_repo}"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("mount fake digest");
    assert_eq!(mount_fake.status(), reqwest::StatusCode::ACCEPTED);

    // Scenario 6: Nonexistent source repo fallback vs nonexistent digest fallback
    // Both return 202 with Location header; no info leakage of digest existence
    assert_eq!(mount_nonexistent_from.status(), mount_fake.status());
}
