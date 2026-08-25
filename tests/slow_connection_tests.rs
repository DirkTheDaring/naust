use reqwest::header;
use sha2::Digest as _;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

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

fn write_config(
    dir: &tempfile::TempDir,
    port: u16,
    idle_timeout_secs: u64,
    rate_grace_secs: u64,
    rate_window_secs: u64,
    min_bytes_per_sec: u64,
    policy: &str,
    max_per_ip: usize,
    trusted_proxies: &[&str],
) -> (PathBuf, PathBuf) {
    let root = dir.path().join("data");
    let cfg_path = dir.path().join("config.toml");

    std::fs::create_dir_all(root.join("blobs").join("sha256")).expect("mkdir blobs");
    std::fs::create_dir_all(root.join("repos")).expect("mkdir repos");
    std::fs::create_dir_all(root.join("uploads")).expect("mkdir uploads");

    let proxies_toml = trusted_proxies
        .iter()
        .map(|p| format!("\"{p}\""))
        .collect::<Vec<_>>()
        .join(", ");

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

[timeouts]
request_timeout_secs = 60
upload_request_timeout_secs = 60
upload_chunk_idle_timeout_secs = {idle_timeout_secs}
upload_rate_grace_period_secs = {rate_grace_secs}
upload_rate_window_secs = {rate_window_secs}
min_upload_bytes_per_sec = {min_bytes_per_sec}
header_read_timeout_secs = 10
slow_connection_policy = "{policy}"

[limits]
max_upload_bytes = 104857600
max_request_body_bytes = 10485760
max_concurrent_requests = 100
max_concurrent_upload_requests = 100
max_connections_per_ip = {max_per_ip}
trusted_proxies = [{proxies_toml}]
"#,
        root.display()
    );

    std::fs::write(&cfg_path, content).expect("write config.toml");
    let log_path = dir.path().join("server.log");
    (cfg_path, log_path)
}

fn spawn_server(cfg_path: &Path, log_path: &Path) -> ServerGuard {
    let log = std::fs::File::create(log_path).expect("create log file");
    let log2 = log.try_clone().expect("clone log file");

    let child = Command::new(bin_path())
        .arg("server")
        .arg("--config")
        .arg(cfg_path)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log2))
        .spawn()
        .expect("spawn server");

    ServerGuard { child }
}

async fn wait_for_server(port: u16, log_path: &Path) {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/v2/");

    for _ in 0..60 {
        if let Ok(resp) = client.get(&url).send().await {
            if resp.status().as_u16() >= 200 {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let server_log = std::fs::read_to_string(log_path).unwrap_or_default();
    panic!("server failed to start on port {port}:\n--- SERVER LOG ---\n{server_log}\n--- END ---");
}

#[tokio::test]
async fn test_stream_idle_timeout_aborts_on_silence() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(
        &temp_dir,
        port,
        2,
        10,
        5,
        1000,
        "enforce",
        50,
        &["127.0.0.1/32"],
    );

    let _server = spawn_server(&cfg_path, &log_path);
    wait_for_server(port, &log_path).await;

    let client = reqwest::Client::new();
    let start_url = format!("http://127.0.0.1:{port}/v2/test/repo/blobs/uploads/");

    let resp = client
        .post(&start_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("start upload session");
    assert_eq!(resp.status(), reqwest::StatusCode::ACCEPTED);

    let location = resp
        .headers()
        .get(header::LOCATION)
        .expect("location")
        .to_str()
        .unwrap()
        .to_string();
    let patch_url = format!("http://127.0.0.1:{port}{location}");

    // Create a slow stream that sends chunk 1, then stops sending for 3s (idle timeout is 2s)
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(2);
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    tokio::spawn(async move {
        let _ = tx
            .send(Ok(bytes::Bytes::from_static(b"initial-chunk-bytes")))
            .await;
        // Idle for 3.5s (exceeding the 2s idle timeout)
        tokio::time::sleep(Duration::from_millis(3500)).await;
        let _ = tx.send(Ok(bytes::Bytes::from_static(b"late-bytes"))).await;
    });

    let resp = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(reqwest::Body::wrap_stream(stream))
        .send()
        .await;

    // Either reqwest reports connection closed / reset or receives 408 Request Timeout
    match resp {
        Ok(r) => {
            assert_eq!(
                r.status(),
                reqwest::StatusCode::REQUEST_TIMEOUT,
                "server should return 408 Request Timeout on stream idle"
            );
        }
        Err(e) => {
            assert!(e.is_body() || e.is_request() || e.is_connect());
        }
    }
}

#[tokio::test]
async fn test_drip_feed_below_min_rate_aborts_after_grace() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    // 1s grace period, 1s rate window, required 100,000 B/s (100 KB/s)
    let (cfg_path, log_path) = write_config(
        &temp_dir,
        port,
        10,
        1,
        1,
        100_000,
        "enforce",
        50,
        &["127.0.0.1/32"],
    );

    let _server = spawn_server(&cfg_path, &log_path);
    wait_for_server(port, &log_path).await;

    let client = reqwest::Client::new();
    let start_url = format!("http://127.0.0.1:{port}/v2/test/repo/blobs/uploads/");

    let resp = client
        .post(&start_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("start upload session");
    assert_eq!(resp.status(), reqwest::StatusCode::ACCEPTED);

    let location = resp
        .headers()
        .get(header::LOCATION)
        .expect("location")
        .to_str()
        .unwrap()
        .to_string();
    let patch_url = format!("http://127.0.0.1:{port}{location}");

    // Send 10 bytes every 400ms (rate = 25 B/s, far below 100,000 B/s requirement)
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(10);
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    tokio::spawn(async move {
        for _ in 0..10 {
            let _ = tx.send(Ok(bytes::Bytes::from_static(b"0123456789"))).await;
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
    });

    let resp = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(reqwest::Body::wrap_stream(stream))
        .send()
        .await;

    match resp {
        Ok(r) => {
            assert_eq!(
                r.status(),
                reqwest::StatusCode::REQUEST_TIMEOUT,
                "server must abort upload when rate falls below min_upload_bytes_per_sec"
            );
        }
        Err(e) => {
            assert!(e.is_body() || e.is_request() || e.is_connect());
        }
    }
}

#[tokio::test]
async fn test_legitimate_upload_and_resumption() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(
        &temp_dir,
        port,
        5,
        2,
        2,
        100,
        "enforce",
        50,
        &["127.0.0.1/32"],
    );

    let _server = spawn_server(&cfg_path, &log_path);
    wait_for_server(port, &log_path).await;

    let client = reqwest::Client::new();
    let start_url = format!("http://127.0.0.1:{port}/v2/test/repo/blobs/uploads/");

    let resp = client
        .post(&start_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("start upload session");
    assert_eq!(resp.status(), reqwest::StatusCode::ACCEPTED);

    let location = resp
        .headers()
        .get(header::LOCATION)
        .expect("location")
        .to_str()
        .unwrap()
        .to_string();
    let upload_url = format!("http://127.0.0.1:{port}{location}");

    // Upload chunk 1 (1000 bytes)
    let chunk1 = vec![0x41u8; 1000];
    let patch_resp1 = client
        .patch(&upload_url)
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header("Content-Range", "0-999")
        .body(chunk1.clone())
        .send()
        .await
        .expect("send chunk 1");
    assert_eq!(patch_resp1.status(), reqwest::StatusCode::ACCEPTED);
    assert_eq!(
        patch_resp1
            .headers()
            .get("Range")
            .unwrap()
            .to_str()
            .unwrap(),
        "0-999"
    );

    // Verify GET upload status returns current Range
    let get_resp = client
        .get(&upload_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("get upload status");
    assert_eq!(get_resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(
        get_resp.headers().get("Range").unwrap().to_str().unwrap(),
        "0-999"
    );

    // Upload chunk 2 (1000 bytes) and finalize with PUT
    let chunk2 = vec![0x42u8; 1000];
    let patch_resp2 = client
        .patch(&upload_url)
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header("Content-Range", "1000-1999")
        .body(chunk2.clone())
        .send()
        .await
        .expect("send chunk 2");
    assert_eq!(patch_resp2.status(), reqwest::StatusCode::ACCEPTED);

    // Compute expected sha256
    let mut all_bytes = chunk1;
    all_bytes.extend_from_slice(&chunk2);
    let mut hasher = sha2::Sha256::new();
    hasher.update(&all_bytes);
    let digest = format!("sha256:{}", hex::encode(hasher.finalize()));

    let put_url = if upload_url.contains('?') {
        format!("{upload_url}&digest={digest}")
    } else {
        format!("{upload_url}?digest={digest}")
    };
    let put_resp = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("finalize upload");
    assert_eq!(put_resp.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_untrusted_xff_spoofing_prevented() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    // max 2 connections per IP, only 10.0.0.1 trusted as proxy
    let (cfg_path, log_path) = write_config(
        &temp_dir,
        port,
        30,
        15,
        10,
        100,
        "enforce",
        2,
        &["10.0.0.1/32"],
    );

    let _server = spawn_server(&cfg_path, &log_path);
    wait_for_server(port, &log_path).await;

    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/_meta/catalog");

    // Untrusted peer 127.0.0.1 sends fake X-Forwarded-For headers
    let r1 = client
        .get(&url)
        .header("X-Forwarded-For", "198.51.100.1")
        .send()
        .await
        .expect("r1");
    assert_eq!(r1.status(), reqwest::StatusCode::OK);

    let r2 = client
        .get(&url)
        .header("X-Forwarded-For", "198.51.100.2")
        .send()
        .await
        .expect("r2");
    assert_eq!(r2.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_audit_only_policy_logs_without_dropping() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg_path, log_path) = write_config(
        &temp_dir,
        port,
        1,
        1,
        1,
        100_000,
        "audit_only",
        50,
        &["127.0.0.1/32"],
    );

    let _server = spawn_server(&cfg_path, &log_path);
    wait_for_server(port, &log_path).await;

    let client = reqwest::Client::new();
    let start_url = format!("http://127.0.0.1:{port}/v2/test/repo/blobs/uploads/");

    let resp = client
        .post(&start_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("start upload session");
    assert_eq!(resp.status(), reqwest::StatusCode::ACCEPTED);

    let location = resp
        .headers()
        .get(header::LOCATION)
        .expect("location")
        .to_str()
        .unwrap()
        .to_string();
    let patch_url = format!("http://127.0.0.1:{port}{location}");

    // In audit_only mode, slow stream with 1.5s delay (exceeding 1s timeout) should SUCCEED
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(2);
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    tokio::spawn(async move {
        let _ = tx.send(Ok(bytes::Bytes::from_static(b"part1"))).await;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let _ = tx.send(Ok(bytes::Bytes::from_static(b"part2"))).await;
    });

    let resp = client
        .patch(&patch_url)
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(reqwest::Body::wrap_stream(stream))
        .send()
        .await
        .expect("send slow stream in audit mode");

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::ACCEPTED,
        "in audit_only mode, slow upload must be accepted"
    );
}
