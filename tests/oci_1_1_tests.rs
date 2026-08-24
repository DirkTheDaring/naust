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
    listener
        .local_addr()
        .expect("local_addr")
        .port()
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

    profile_dir
        .join(bin_name)
        .to_string_lossy()
        .to_string()
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

async fn upload_blob(client: &reqwest::Client, base_url: &str, repo: &str, bytes: &[u8]) -> String {
    let digest_hex = hex_sha256(bytes);
    let digest = format!("sha256:{digest_hex}");

    let start_url = format!("{base_url}/v2/{repo}/blobs/uploads/");
    let start_resp = client
        .post(&start_url)
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_LENGTH, "0")
        .send()
        .await
        .expect("start upload");
    assert_eq!(start_resp.status(), reqwest::StatusCode::ACCEPTED);

    let location = start_resp
        .headers()
        .get(header::LOCATION)
        .expect("location header")
        .to_str()
        .expect("utf8 location")
        .to_string();

    let put_url = if location.starts_with("http://") || location.starts_with("https://") {
        format!("{location}&digest={digest}")
    } else {
        format!("{base_url}{location}?digest={digest}")
    };

    let put_resp = client
        .put(&put_url)
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(bytes.to_vec())
        .send()
        .await
        .expect("finish upload");

    assert_eq!(put_resp.status(), reqwest::StatusCode::CREATED);
    digest
}

#[tokio::test]
async fn test_oci_1_1_referrers_api_full_lifecycle() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg, &log_path);

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let repo = "test/app";

    // 1. Upload base image config + layer + manifest
    let config_bytes = b"{\"architecture\":\"amd64\",\"os\":\"linux\",\"rootfs\":{\"type\":\"layers\",\"diff_ids\":[]}}";
    let config_digest = upload_blob(&client, &base_url, repo, config_bytes).await;

    let layer_bytes = b"hello layer";
    let layer_digest = upload_blob(&client, &base_url, repo, layer_bytes).await;

    let base_manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config_digest,
            "size": config_bytes.len()
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": layer_digest,
                "size": layer_bytes.len()
            }
        ]
    });

    let base_manifest_bytes = serde_json::to_vec(&base_manifest).unwrap();
    let base_manifest_hex = hex_sha256(&base_manifest_bytes);
    let base_manifest_digest = format!("sha256:{base_manifest_hex}");

    let put_base_resp = client
        .put(format!("{base_url}/v2/{repo}/manifests/v1.0.0"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(base_manifest_bytes.clone())
        .send()
        .await
        .expect("put base manifest");
    assert_eq!(put_base_resp.status(), reqwest::StatusCode::CREATED);

    // 2. Query referrers before any artifact pushed -> should return 200 with empty manifests array
    let empty_ref_resp = client
        .get(format!("{base_url}/v2/{repo}/referrers/{base_manifest_digest}"))
        .send()
        .await
        .expect("get empty referrers");
    assert_eq!(empty_ref_resp.status(), reqwest::StatusCode::OK);
    let empty_ref_body: serde_json::Value = empty_ref_resp.json().await.expect("json");
    assert_eq!(empty_ref_body["manifests"].as_array().unwrap().len(), 0);

    // 3. Push Artifact 1 (SBOM) with subject pointing to base manifest
    let sbom_layer_bytes = b"{\"spdxVersion\":\"SPDX-2.3\",\"packages\":[]}";
    let sbom_layer_digest = upload_blob(&client, &base_url, repo, sbom_layer_bytes).await;

    let sbom_manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "artifactType": "application/vnd.example.sbom.v1",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
            "size": 2
        },
        "layers": [
            {
                "mediaType": "text/spdx+json",
                "digest": sbom_layer_digest,
                "size": sbom_layer_bytes.len()
            }
        ],
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": base_manifest_digest,
            "size": base_manifest_bytes.len()
        },
        "annotations": {
            "org.opencontainers.image.created": "2026-08-24T10:00:00Z"
        }
    });

    let sbom_manifest_bytes = serde_json::to_vec(&sbom_manifest).unwrap();
    let sbom_manifest_hex = hex_sha256(&sbom_manifest_bytes);
    let sbom_manifest_digest = format!("sha256:{sbom_manifest_hex}");

    let put_sbom_resp = client
        .put(format!("{base_url}/v2/{repo}/manifests/{sbom_manifest_digest}"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(sbom_manifest_bytes.clone())
        .send()
        .await
        .expect("put sbom manifest");
    assert_eq!(put_sbom_resp.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        put_sbom_resp.headers().get("OCI-Subject").unwrap().to_str().unwrap(),
        base_manifest_digest
    );

    // 4. Push Artifact 2 (Signature) with subject pointing to base manifest
    let sig_manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "artifactType": "application/vnd.example.sig.v1",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
            "size": 2
        },
        "layers": [],
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": base_manifest_digest,
            "size": base_manifest_bytes.len()
        }
    });

    let sig_manifest_bytes = serde_json::to_vec(&sig_manifest).unwrap();
    let sig_manifest_hex = hex_sha256(&sig_manifest_bytes);
    let sig_manifest_digest = format!("sha256:{sig_manifest_hex}");

    let put_sig_resp = client
        .put(format!("{base_url}/v2/{repo}/manifests/{sig_manifest_digest}"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(sig_manifest_bytes.clone())
        .send()
        .await
        .expect("put sig manifest");
    assert_eq!(put_sig_resp.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        put_sig_resp.headers().get("OCI-Subject").unwrap().to_str().unwrap(),
        base_manifest_digest
    );

    // 5. Query Referrers list -> both descriptors returned
    let ref_resp = client
        .get(format!("{base_url}/v2/{repo}/referrers/{base_manifest_digest}"))
        .send()
        .await
        .expect("get referrers");
    assert_eq!(ref_resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        ref_resp.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap(),
        "application/vnd.oci.image.index.v1+json"
    );
    let ref_body: serde_json::Value = ref_resp.json().await.expect("json");
    let manifests = ref_body["manifests"].as_array().expect("manifests array");
    assert_eq!(manifests.len(), 2);

    // 6. Query HEAD referrers
    let head_ref_resp = client
        .head(format!("{base_url}/v2/{repo}/referrers/{base_manifest_digest}"))
        .send()
        .await
        .expect("head referrers");
    assert_eq!(head_ref_resp.status(), reqwest::StatusCode::OK);

    // 7. Filter by artifactType
    let filtered_resp = client
        .get(format!(
            "{base_url}/v2/{repo}/referrers/{base_manifest_digest}?artifactType=application/vnd.example.sbom.v1"
        ))
        .send()
        .await
        .expect("get filtered referrers");
    assert_eq!(filtered_resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        filtered_resp.headers().get("OCI-Filters-Applied").unwrap().to_str().unwrap(),
        "artifactType"
    );
    let filtered_body: serde_json::Value = filtered_resp.json().await.expect("json");
    let filtered_manifests = filtered_body["manifests"].as_array().unwrap();
    assert_eq!(filtered_manifests.len(), 1);
    assert_eq!(
        filtered_manifests[0]["artifactType"].as_str().unwrap(),
        "application/vnd.example.sbom.v1"
    );

    // 8. Test Pagination with `n=1` and `Link` header
    let page1_resp = client
        .get(format!("{base_url}/v2/{repo}/referrers/{base_manifest_digest}?n=1"))
        .send()
        .await
        .expect("page1 referrers");
    assert_eq!(page1_resp.status(), reqwest::StatusCode::OK);
    let link_header = page1_resp.headers().get("Link").expect("Link header").to_str().unwrap().to_string();
    assert!(link_header.contains("rel=\"next\""));
    let page1_body: serde_json::Value = page1_resp.json().await.unwrap();
    assert_eq!(page1_body["manifests"].as_array().unwrap().len(), 1);

    // Extract next url from Link header
    let link_url = link_header.trim_start_matches('<').split('>').next().unwrap();
    let page2_resp = client
        .get(format!("{base_url}{link_url}"))
        .send()
        .await
        .expect("page2 referrers");
    assert_eq!(page2_resp.status(), reqwest::StatusCode::OK);
    let page2_body: serde_json::Value = page2_resp.json().await.unwrap();
    assert_eq!(page2_body["manifests"].as_array().unwrap().len(), 1);

    // 9. Pull manifest GET and HEAD -> verify OCI-Subject header
    let get_manifest_resp = client
        .get(format!("{base_url}/v2/{repo}/manifests/{sbom_manifest_digest}"))
        .send()
        .await
        .expect("get sbom manifest");
    assert_eq!(get_manifest_resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        get_manifest_resp.headers().get("OCI-Subject").unwrap().to_str().unwrap(),
        base_manifest_digest
    );

    let head_manifest_resp = client
        .head(format!("{base_url}/v2/{repo}/manifests/{sbom_manifest_digest}"))
        .send()
        .await
        .expect("head sbom manifest");
    assert_eq!(head_manifest_resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        head_manifest_resp.headers().get("OCI-Subject").unwrap().to_str().unwrap(),
        base_manifest_digest
    );

    // 10. Delete Artifact 1 -> verify referrer is removed from list
    let del_resp = client
        .delete(format!("{base_url}/v2/{repo}/manifests/{sbom_manifest_digest}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("delete sbom manifest");
    assert_eq!(del_resp.status(), reqwest::StatusCode::ACCEPTED);

    let after_del_resp = client
        .get(format!("{base_url}/v2/{repo}/referrers/{base_manifest_digest}"))
        .send()
        .await
        .expect("get referrers after delete");
    assert_eq!(after_del_resp.status(), reqwest::StatusCode::OK);
    let after_del_body: serde_json::Value = after_del_resp.json().await.unwrap();
    let after_del_manifests = after_del_body["manifests"].as_array().unwrap();
    assert_eq!(after_del_manifests.len(), 1);
    assert_eq!(after_del_manifests[0]["digest"].as_str().unwrap(), sig_manifest_digest);
}

#[tokio::test]
async fn test_oci_1_1_extension_discovery_and_resumable_upload() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg, &log_path);

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    // 1. Test Extension discovery endpoint
    let ext_resp = client
        .get(format!("{base_url}/v2/_oci/ext/discover"))
        .send()
        .await
        .expect("extension discover");
    assert_eq!(ext_resp.status(), reqwest::StatusCode::OK);
    let ext_json: serde_json::Value = ext_resp.json().await.expect("ext json");
    let extensions = ext_json["extensions"].as_array().expect("extensions array");
    assert!(extensions.iter().any(|e| e["name"] == "_oci"));
    assert!(extensions.iter().any(|e| e["name"] == "referrers"));

    // 2. Test Resumable Upload Status with OCI-Chunk-Min-Length
    let upload_start = client
        .post(format!("{base_url}/v2/test/upload/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_LENGTH, "0")
        .send()
        .await
        .expect("start upload");
    assert_eq!(upload_start.status(), reqwest::StatusCode::ACCEPTED);
    assert_eq!(
        upload_start.headers().get("OCI-Chunk-Min-Length").unwrap().to_str().unwrap(),
        "1024"
    );

    let location = upload_start
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();

    let status_url = if location.starts_with("http") {
        location.to_string()
    } else {
        format!("{base_url}{location}")
    };

    let status_resp = client.get(&status_url).send().await.expect("upload status");
    assert_eq!(status_resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert!(status_resp.headers().contains_key("Docker-Upload-UUID"));

    // 3. Test Repo-scoped Extension discovery endpoint
    let repo_ext_resp = client
        .get(format!("{base_url}/v2/test/upload/_oci/ext/discover"))
        .send()
        .await
        .expect("repo extension discover");
    assert_eq!(repo_ext_resp.status(), reqwest::StatusCode::OK);
    let repo_ext_json: serde_json::Value = repo_ext_resp.json().await.expect("repo ext json");
    let repo_extensions = repo_ext_json["extensions"].as_array().expect("extensions array");
    assert!(repo_extensions.iter().any(|e| e["name"] == "_oci"));
    assert!(repo_extensions.iter().any(|e| e["name"] == "referrers"));
}

#[tokio::test]
async fn test_oci_1_1_index_referrer_and_empty_config() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg, &log_path);

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let repo = "test/index-referrers";

    // 1. Upload base image
    let config_bytes = b"{\"architecture\":\"arm64\",\"os\":\"linux\",\"rootfs\":{\"type\":\"layers\",\"diff_ids\":[]}}";
    let config_digest = upload_blob(&client, &base_url, repo, config_bytes).await;

    let layer_bytes = b"base layer content";
    let layer_digest = upload_blob(&client, &base_url, repo, layer_bytes).await;

    let base_manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config_digest,
            "size": config_bytes.len()
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": layer_digest,
                "size": layer_bytes.len()
            }
        ]
    });

    let base_manifest_bytes = serde_json::to_vec(&base_manifest).unwrap();
    let base_manifest_hex = hex_sha256(&base_manifest_bytes);
    let base_manifest_digest = format!("sha256:{base_manifest_hex}");

    let put_base_resp = client
        .put(format!("{base_url}/v2/{repo}/manifests/latest"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(base_manifest_bytes.clone())
        .send()
        .await
        .expect("put base manifest");
    assert_eq!(put_base_resp.status(), reqwest::StatusCode::CREATED);

    // 2. Upload child manifest for index
    let child_manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
            "size": 2
        },
        "layers": []
    });
    let child_bytes = serde_json::to_vec(&child_manifest).unwrap();
    let child_hex = hex_sha256(&child_bytes);
    let child_digest = format!("sha256:{child_hex}");
    let put_child = client
        .put(format!("{base_url}/v2/{repo}/manifests/{child_digest}"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(child_bytes.clone())
        .send()
        .await
        .expect("put child");
    assert_eq!(put_child.status(), reqwest::StatusCode::CREATED);

    // 3. Upload OCI Index Referrer with subject pointing to base image
    let index_referrer = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "artifactType": "application/vnd.example.attestation.v1",
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": base_manifest_digest,
            "size": base_manifest_bytes.len()
        },
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": child_digest,
                "size": child_bytes.len()
            }
        ]
    });
    let index_bytes = serde_json::to_vec(&index_referrer).unwrap();
    let index_hex = hex_sha256(&index_bytes);
    let index_digest = format!("sha256:{index_hex}");

    let put_index_resp = client
        .put(format!("{base_url}/v2/{repo}/manifests/{index_digest}"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/vnd.oci.image.index.v1+json")
        .body(index_bytes)
        .send()
        .await
        .expect("put index referrer");
    assert_eq!(put_index_resp.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        put_index_resp.headers().get("OCI-Subject").unwrap().to_str().unwrap(),
        base_manifest_digest
    );

    // 4. Query Referrers on base image -> index referrer should be returned with correct artifactType
    let ref_resp = client
        .get(format!("{base_url}/v2/{repo}/referrers/{base_manifest_digest}"))
        .send()
        .await
        .expect("get referrers");
    assert_eq!(ref_resp.status(), reqwest::StatusCode::OK);
    let ref_json: serde_json::Value = ref_resp.json().await.unwrap();
    let manifests = ref_json["manifests"].as_array().unwrap();
    assert_eq!(manifests.len(), 1);
    assert_eq!(manifests[0]["digest"].as_str().unwrap(), index_digest);
    assert_eq!(manifests[0]["mediaType"].as_str().unwrap(), "application/vnd.oci.image.index.v1+json");
    assert_eq!(manifests[0]["artifactType"].as_str().unwrap(), "application/vnd.example.attestation.v1");
}

#[tokio::test]
async fn test_concurrent_referrers_push_race_condition() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg, &log_path);

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let repo = "test/concurrency";

    // 1. Upload base image
    let config_bytes = b"{\"architecture\":\"amd64\",\"os\":\"linux\",\"rootfs\":{\"type\":\"layers\",\"diff_ids\":[]}}";
    let config_digest = upload_blob(&client, &base_url, repo, config_bytes).await;

    let base_manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config_digest,
            "size": config_bytes.len()
        },
        "layers": []
    });

    let base_manifest_bytes = serde_json::to_vec(&base_manifest).unwrap();
    let base_manifest_hex = hex_sha256(&base_manifest_bytes);
    let base_manifest_digest = format!("sha256:{base_manifest_hex}");

    let put_base_resp = client
        .put(format!("{base_url}/v2/{repo}/manifests/v1.0.0"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(base_manifest_bytes.clone())
        .send()
        .await
        .expect("put base manifest");
    assert_eq!(put_base_resp.status(), reqwest::StatusCode::CREATED);

    // 2. Spawn 10 concurrent tasks simultaneously pushing 10 distinct artifacts referencing the base image
    const CONCURRENT_ARTIFACTS: usize = 10;
    let mut handles = Vec::new();

    for i in 0..CONCURRENT_ARTIFACTS {
        let base_url_c = base_url.clone();
        let base_digest_c = base_manifest_digest.clone();
        let base_size = base_manifest_bytes.len();

        let handle = tokio::spawn(async move {
            let client = reqwest::Client::new();
            let art_manifest = json!({
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "artifactType": format!("application/vnd.example.item.{i}"),
                "config": {
                    "mediaType": "application/vnd.oci.empty.v1+json",
                    "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
                    "size": 2
                },
                "layers": [],
                "subject": {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": base_digest_c,
                    "size": base_size
                },
                "annotations": {
                    "index": format!("{i}")
                }
            });

            let bytes = serde_json::to_vec(&art_manifest).unwrap();
            let hex = hex_sha256(&bytes);
            let digest = format!("sha256:{hex}");

            let resp = client
                .put(format!("{base_url_c}/v2/test/concurrency/manifests/{digest}"))
                .basic_auth("demo", Some("demo"))
                .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
                .body(bytes)
                .send()
                .await
                .expect("put concurrent artifact");
            assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
            digest
        });
        handles.push(handle);
    }

    let mut pushed_digests = Vec::new();
    for handle in handles {
        let d = handle.await.expect("task completed");
        pushed_digests.push(d);
    }
    assert_eq!(pushed_digests.len(), CONCURRENT_ARTIFACTS);

    // 3. Query referrers: verify ALL 10 artifacts are present with 0 dropped entries
    let ref_resp = client
        .get(format!("{base_url}/v2/{repo}/referrers/{base_manifest_digest}"))
        .send()
        .await
        .expect("get referrers");
    assert_eq!(ref_resp.status(), reqwest::StatusCode::OK);
    let ref_json: serde_json::Value = ref_resp.json().await.unwrap();
    let manifests = ref_json["manifests"].as_array().expect("manifests array");
    assert_eq!(manifests.len(), CONCURRENT_ARTIFACTS, "all concurrent referrers must be preserved");

    for digest in pushed_digests {
        assert!(
            manifests.iter().any(|m| m["digest"].as_str() == Some(&digest)),
            "digest {digest} must be in referrers list"
        );
    }
}

#[tokio::test]
async fn test_malformed_and_traversal_upload_uuid_rejection() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg, &log_path);

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let invalid_uuids = [
        "not-a-valid-uuid",
        "../../etc/passwd",
        "..",
        ".",
        "12345",
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee-extra",
    ];

    for bad_uuid in invalid_uuids {
        let get_resp = client
            .get(format!("{base_url}/v2/test/app/blobs/uploads/{bad_uuid}"))
            .basic_auth("demo", Some("demo"))
            .send()
            .await
            .expect("get bad upload uuid");
        assert!(!get_resp.status().is_success(), "must reject bad upload uuid on GET");

        let patch_resp = client
            .patch(format!("{base_url}/v2/test/app/blobs/uploads/{bad_uuid}"))
            .basic_auth("demo", Some("demo"))
            .body(vec![1, 2, 3])
            .send()
            .await
            .expect("patch bad upload uuid");
        assert!(!patch_resp.status().is_success(), "must reject bad upload uuid on PATCH");
    }
}

#[tokio::test]
async fn test_concurrent_chunk_uploads_integrity() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = pick_unused_port();
    let (cfg, log_path) = write_config(&dir, port);
    let _srv = spawn_server(&cfg, &log_path);

    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    const NUM_PARALLEL_UPLOADS: usize = 6;
    let mut handles = Vec::new();

    for i in 0..NUM_PARALLEL_UPLOADS {
        let base_url_c = base_url.clone();
        let handle = tokio::spawn(async move {
            let client = reqwest::Client::new();
            let repo = format!("test/chunks-{i}");

            // Start upload
            let start_resp = client
                .post(format!("{base_url_c}/v2/{repo}/blobs/uploads/"))
                .basic_auth("demo", Some("demo"))
                .header(header::CONTENT_LENGTH, "0")
                .send()
                .await
                .unwrap();
            assert_eq!(start_resp.status(), reqwest::StatusCode::ACCEPTED);
            let location = start_resp.headers().get(header::LOCATION).unwrap().to_str().unwrap().to_string();

            // Prepare 4 chunks of 32KB each
            let mut total_bytes = Vec::new();
            let mut current_offset: usize = 0;

            for c in 0..4 {
                let chunk_data = vec![(i * 10 + c) as u8; 32 * 1024];
                total_bytes.extend_from_slice(&chunk_data);

                let patch_url = if location.starts_with("http") {
                    location.clone()
                } else {
                    format!("{base_url_c}{location}")
                };

                let range_hdr = format!("{}-{}", current_offset, current_offset + chunk_data.len() - 1);
                current_offset += chunk_data.len();

                let patch_resp = client
                    .patch(&patch_url)
                    .basic_auth("demo", Some("demo"))
                    .header("Content-Range", range_hdr)
                    .header(header::CONTENT_TYPE, "application/octet-stream")
                    .body(chunk_data)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(patch_resp.status(), reqwest::StatusCode::ACCEPTED);
            }

            let expected_digest = format!("sha256:{}", hex_sha256(&total_bytes));
            let put_url = if location.starts_with("http") {
                format!("{location}&digest={expected_digest}")
            } else {
                format!("{base_url_c}{location}?digest={expected_digest}")
            };

            let put_resp = client
                .put(&put_url)
                .basic_auth("demo", Some("demo"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .send()
                .await
                .unwrap();
            assert_eq!(put_resp.status(), reqwest::StatusCode::CREATED);

            // Verify blob HEAD
            let head_resp = client
                .head(format!("{base_url_c}/v2/{repo}/blobs/{expected_digest}"))
                .send()
                .await
                .unwrap();
            assert_eq!(head_resp.status(), reqwest::StatusCode::OK);
            assert_eq!(
                head_resp.headers().get(header::CONTENT_LENGTH).unwrap().to_str().unwrap(),
                total_bytes.len().to_string()
            );
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("upload task completed successfully");
    }
}

#[tokio::test]
async fn test_audit_remediation_suite() {
    let port = pick_unused_port();
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let (cfg_path, log_path) = write_config(&temp_dir, port);
    let _server = spawn_server(&cfg_path, &log_path);

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{port}");
    wait_ready(&base_url, &log_path).await;

    let repo = "audit/test-repo";

    // 1. Unauthenticated GET /v2/_catalog returns 401 with WWW-Authenticate scope="registry:catalog:*"
    let cat_resp = client.get(format!("{base_url}/v2/_catalog")).send().await.expect("get catalog");
    assert_eq!(cat_resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    let auth_header = cat_resp.headers().get(header::WWW_AUTHENTICATE).unwrap().to_str().unwrap();
    assert!(auth_header.contains("registry:catalog:*"));
    assert_eq!(
        cat_resp.headers().get("docker-distribution-api-version").unwrap().to_str().unwrap(),
        "registry/2.0"
    );

    // 2. GET /v2/<repo>/blobs/notadigestformat returns 400 Bad Request with application/json
    let invalid_blob_resp = client.get(format!("{base_url}/v2/{repo}/blobs/notadigestformat")).send().await.expect("get blob");
    assert_eq!(invalid_blob_resp.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        invalid_blob_resp.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap(),
        "application/json"
    );
    let invalid_body: serde_json::Value = invalid_blob_resp.json().await.expect("json");
    assert_eq!(invalid_body["errors"][0]["code"], "DIGEST_INVALID");

    // 3. Blob Upload Session: HEAD and DELETE /v2/<repo>/blobs/uploads/<uuid>
    let upload_start_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_LENGTH, "0")
        .send()
        .await
        .expect("start upload");
    assert_eq!(upload_start_resp.status(), reqwest::StatusCode::ACCEPTED);
    let location = upload_start_resp.headers().get(header::LOCATION).unwrap().to_str().unwrap();
    let upload_uuid = upload_start_resp.headers().get("docker-upload-uuid").unwrap().to_str().unwrap();

    let upload_url = if location.starts_with("http") {
        location.to_string()
    } else {
        format!("{base_url}{location}")
    };

    // HEAD upload session -> 204 No Content
    let head_upload_resp = client.head(&upload_url).send().await.expect("head upload");
    assert_eq!(head_upload_resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(
        head_upload_resp.headers().get("docker-upload-uuid").unwrap().to_str().unwrap(),
        upload_uuid
    );

    // DELETE upload session -> 204 No Content
    let del_upload_resp = client
        .delete(&upload_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("delete upload");
    assert_eq!(del_upload_resp.status(), reqwest::StatusCode::NO_CONTENT);

    // Subsequent HEAD after DELETE -> 404 Not Found
    let head_after_del = client.head(&upload_url).send().await.expect("head deleted upload");
    assert_eq!(head_after_del.status(), reqwest::StatusCode::NOT_FOUND);

    // 4. SHA-512 Blob Upload and Verification
    let sha512_data = b"cryptographic sha512 payload test content";
    let mut hasher = sha2::Sha512::new();
    hasher.update(sha512_data);
    let sha512_hex = hex::encode(hasher.finalize());
    let sha512_digest = format!("sha512:{sha512_hex}");

    let upload_512_start = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_LENGTH, "0")
        .send()
        .await
        .expect("start upload sha512");
    let loc_512 = upload_512_start.headers().get(header::LOCATION).unwrap().to_str().unwrap();
    let put_512_url = if loc_512.starts_with("http") {
        format!("{loc_512}&digest={sha512_digest}")
    } else {
        format!("{base_url}{loc_512}?digest={sha512_digest}")
    };

    let put_512_resp = client
        .put(&put_512_url)
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(sha512_data.to_vec())
        .send()
        .await
        .expect("put sha512 blob");
    assert_eq!(put_512_resp.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        put_512_resp.headers().get("docker-content-digest").unwrap().to_str().unwrap(),
        sha512_digest
    );

    let get_512_resp = client.get(format!("{base_url}/v2/{repo}/blobs/{sha512_digest}")).send().await.expect("get sha512 blob");
    assert_eq!(get_512_resp.status(), reqwest::StatusCode::OK);
    let downloaded_512 = get_512_resp.bytes().await.expect("bytes");
    assert_eq!(downloaded_512.as_ref(), sha512_data);

    // 5. Manifest Upload: Missing Layer Blob Check -> 400 MANIFEST_BLOB_UNKNOWN
    let unuploaded_blob_digest = "sha256:0000000000000000000000000000000000000000000000000000000000000001";
    let manifest_missing_blob = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
            "size": 2
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": unuploaded_blob_digest,
                "size": 100
            }
        ]
    });
    let missing_blob_resp = client
        .put(format!("{base_url}/v2/{repo}/manifests/missing-blob-tag"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(serde_json::to_vec(&manifest_missing_blob).unwrap())
        .send()
        .await
        .expect("put manifest missing blob");
    assert_eq!(missing_blob_resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let missing_err: serde_json::Value = missing_blob_resp.json().await.expect("json");
    assert_eq!(missing_err["errors"][0]["code"], "MANIFEST_BLOB_UNKNOWN");

    // 6. Manifest Upload: Schema 1 Rejection -> 400 MANIFEST_INVALID
    let schema1_manifest = json!({
        "schemaVersion": 1,
        "name": repo,
        "tag": "schema1-tag",
        "architecture": "amd64",
        "fsLayers": []
    });
    let schema1_resp = client
        .put(format!("{base_url}/v2/{repo}/manifests/schema1-tag"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/vnd.docker.distribution.manifest.v1+json")
        .body(serde_json::to_vec(&schema1_manifest).unwrap())
        .send()
        .await
        .expect("put schema1");
    assert_eq!(schema1_resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let s1_err: serde_json::Value = schema1_resp.json().await.expect("json");
    assert_eq!(s1_err["errors"][0]["code"], "MANIFEST_INVALID");

    // 7. Manifest Upload: Malformed JSON -> 400 MANIFEST_INVALID
    let malformed_resp = client
        .put(format!("{base_url}/v2/{repo}/manifests/malformed-tag"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(b"{invalid-json:".to_vec())
        .send()
        .await
        .expect("put malformed");
    assert_eq!(malformed_resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let malformed_err: serde_json::Value = malformed_resp.json().await.expect("json");
    assert_eq!(malformed_err["errors"][0]["code"], "MANIFEST_INVALID");

    // 8. Manifest Upload: Canonical Location Header returned
    let layer_data = b"layer content for canonical location test";
    let layer_digest = upload_blob(&client, &base_url, repo, layer_data).await;
    let config_data = b"{\"architecture\":\"amd64\",\"os\":\"linux\",\"rootfs\":{\"type\":\"layers\",\"diff_ids\":[]}}";
    let config_digest = upload_blob(&client, &base_url, repo, config_data).await;

    let valid_manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config_digest,
            "size": config_data.len()
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": layer_digest,
                "size": layer_data.len()
            }
        ]
    });
    let valid_manifest_bytes = serde_json::to_vec(&valid_manifest).unwrap();
    let computed_digest = format!("sha256:{}", hex_sha256(&valid_manifest_bytes));

    let put_valid_resp = client
        .put(format!("{base_url}/v2/{repo}/manifests/valid-tag"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(valid_manifest_bytes.clone())
        .send()
        .await
        .expect("put valid manifest");
    assert_eq!(put_valid_resp.status(), reqwest::StatusCode::CREATED);
    let loc_header = put_valid_resp.headers().get(header::LOCATION).unwrap().to_str().unwrap();
    assert_eq!(loc_header, format!("/v2/{repo}/manifests/{computed_digest}"));

    // 9. OCI 1.1 Tag Deletion: DELETE /v2/<repo>/tags/reference/<tag>
    let del_tag_resp = client
        .delete(format!("{base_url}/v2/{repo}/tags/reference/valid-tag"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("delete tag");
    assert_eq!(del_tag_resp.status(), reqwest::StatusCode::ACCEPTED);

    // Subsequent GET manifest by tag -> 404
    let get_deleted_tag = client
        .get(format!("{base_url}/v2/{repo}/manifests/valid-tag"))
        .send()
        .await
        .expect("get deleted tag");
    assert_eq!(get_deleted_tag.status(), reqwest::StatusCode::NOT_FOUND);

    // 10. Tags Pagination Cursor: last query past end returns [] with no next Link
    let tags_resp = client
        .get(format!("{base_url}/v2/{repo}/tags/list?last=zzzzzzzz"))
        .send()
        .await
        .expect("get tags cursor");
    assert_eq!(tags_resp.status(), reqwest::StatusCode::OK);
    assert!(tags_resp.headers().get(header::LINK).is_none());
    let tags_body: serde_json::Value = tags_resp.json().await.expect("json");
    assert_eq!(tags_body["tags"].as_array().unwrap().len(), 0);

    // 11. Tags on empty repo returns 200 OK with empty array
    let empty_tags_resp = client
        .get(format!("{base_url}/v2/nonexistent/empty/tags/list"))
        .send()
        .await
        .expect("get empty tags");
    assert_eq!(empty_tags_resp.status(), reqwest::StatusCode::OK);
    let empty_tags_body: serde_json::Value = empty_tags_resp.json().await.expect("json");
    assert_eq!(empty_tags_body["tags"].as_array().unwrap().len(), 0);

    // 12. HTTP 405 Method Not Allowed on mutation methods
    // Catalog mutation -> 405
    let cat_post = client.post(format!("{base_url}/v2/_catalog")).send().await.expect("cat post");
    assert_eq!(cat_post.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);
    let cat_put = client.put(format!("{base_url}/v2/_catalog")).send().await.expect("cat put");
    assert_eq!(cat_put.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);
    let cat_del = client.delete(format!("{base_url}/v2/_catalog")).send().await.expect("cat del");
    assert_eq!(cat_del.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);

    // Tags list mutation -> 405
    let tags_post = client.post(format!("{base_url}/v2/{repo}/tags/list")).send().await.expect("tags post");
    assert_eq!(tags_post.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);
    let tags_put = client.put(format!("{base_url}/v2/{repo}/tags/list")).send().await.expect("tags put");
    assert_eq!(tags_put.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);
    let tags_del = client.delete(format!("{base_url}/v2/{repo}/tags/list")).send().await.expect("tags del");
    assert_eq!(tags_del.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);

    // Referrers mutation -> 405
    let ref_post = client.post(format!("{base_url}/v2/{repo}/referrers/{computed_digest}")).send().await.expect("ref post");
    assert_eq!(ref_post.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);
    let ref_del = client.delete(format!("{base_url}/v2/{repo}/referrers/{computed_digest}")).send().await.expect("ref del");
    assert_eq!(ref_del.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);

    // Tag reference invalid method -> 405
    let tag_ref_post = client.post(format!("{base_url}/v2/{repo}/tags/reference/dummy")).send().await.expect("tag ref post");
    assert_eq!(tag_ref_post.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);

    // 13. Referrers for subject with 0 attached referrers -> 200 OK with empty index
    let zero_ref_resp = client
        .get(format!("{base_url}/v2/{repo}/referrers/{computed_digest}"))
        .send()
        .await
        .expect("get zero referrers");
    assert_eq!(zero_ref_resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        zero_ref_resp.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap(),
        "application/vnd.oci.image.index.v1+json"
    );
    let zero_ref_body: serde_json::Value = zero_ref_resp.json().await.expect("json");
    assert_eq!(zero_ref_body["manifests"].as_array().unwrap().len(), 0);

    // 14. Cross-Repository Blob Mount
    // 14a. Mount existing blob -> 201 Created
    let mount_resp = client
        .post(format!("{base_url}/v2/target/repo/blobs/uploads/?mount={layer_digest}&from={repo}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("mount existing");
    assert_eq!(mount_resp.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        mount_resp.headers().get(header::LOCATION).unwrap().to_str().unwrap(),
        format!("/v2/target/repo/blobs/{layer_digest}")
    );

    // 14b. Mount non-existent blob -> fallback to 202 Accepted upload session
    let missing_digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let fallback_resp = client
        .post(format!("{base_url}/v2/target/repo/blobs/uploads/?mount={missing_digest}&from={repo}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("mount fallback");
    assert_eq!(fallback_resp.status(), reqwest::StatusCode::ACCEPTED);
    assert!(fallback_resp.headers().get(header::LOCATION).is_some());

    // 15. Blob upload cancellation DELETE returns Docker-Upload-UUID
    let new_upload = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("new upload");
    let new_loc = new_upload.headers().get(header::LOCATION).unwrap().to_str().unwrap();
    let new_uuid = new_upload.headers().get("docker-upload-uuid").unwrap().to_str().unwrap();
    let new_upload_url = if new_loc.starts_with("http") { new_loc.to_string() } else { format!("{base_url}{new_loc}") };
    let del_resp = client
        .delete(&new_upload_url)
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("del upload");
    assert_eq!(del_resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(
        del_resp.headers().get("docker-upload-uuid").unwrap().to_str().unwrap(),
        new_uuid
    );

    // 16. Manifest PUT with mismatching digest -> 400 MANIFEST_UNVERIFIED
    let bad_digest_ref = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let unverified_resp = client
        .put(format!("{base_url}/v2/{repo}/manifests/{bad_digest_ref}"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(valid_manifest_bytes.clone())
        .send()
        .await
        .expect("put mismatch");
    assert_eq!(unverified_resp.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        unverified_resp.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap(),
        "application/json"
    );
    let unverified_err: serde_json::Value = unverified_resp.json().await.expect("json");
    assert_eq!(unverified_err["errors"][0]["code"], "MANIFEST_UNVERIFIED");

    // 17. Base /v2 strict redirect to /v2/
    let no_redirect_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let v2_resp = no_redirect_client
        .get(format!("{base_url}/v2"))
        .send()
        .await
        .expect("get /v2");
    assert_eq!(v2_resp.status(), reqwest::StatusCode::MOVED_PERMANENTLY);
    assert_eq!(
        v2_resp.headers().get(header::LOCATION).unwrap().to_str().unwrap(),
        "/v2/"
    );

    // 18. Reject malformed repository names in tags route
    for bad_name in &["INVALID/UPPERCASE", "-invalid-leading-dash", "invalid__double_dot", "invalid..dots"] {
        let bad_repo_resp = client
            .get(format!("{base_url}/v2/{bad_name}/tags/list"))
            .send()
            .await
            .expect("get bad repo tags");
        assert_eq!(bad_repo_resp.status(), reqwest::StatusCode::NOT_FOUND);
    }

    // 19. Delete unreferenced blob layer by digest -> 202 Accepted
    let unreferenced_blob = b"unreferenced temporary blob data";
    let unref_digest = upload_blob(&client, &base_url, repo, unreferenced_blob).await;
    let del_blob_resp = client
        .delete(format!("{base_url}/v2/{repo}/blobs/{unref_digest}"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("delete blob");
    assert_eq!(del_blob_resp.status(), reqwest::StatusCode::ACCEPTED);

    // 20. Cross-repository blob mount graceful fallback to 202
    let missing_digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
    let mount_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/?mount={missing_digest}&from=other-repo"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("mount fallback");
    assert_eq!(mount_resp.status(), reqwest::StatusCode::ACCEPTED);

    // 21. Private repository challenge before repo validation
    let priv_resp = client
        .get(format!("{base_url}/v2/<PRIVATE_REPO>/tags/list"))
        .send()
        .await
        .expect("priv tags");
    assert_eq!(priv_resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // 22. Token endpoint issues delete action in scopes
    let token_resp = client
        .get(format!("{base_url}/token?service=registry-rust&scope=repository:{repo}:pull,push,delete"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("get token with delete");
    assert_eq!(token_resp.status(), reqwest::StatusCode::OK);
    let token_body: serde_json::Value = token_resp.json().await.expect("token json");
    let delete_token = token_body["token"].as_str().unwrap();
    let scopes_arr = token_body["scopes"].as_array().unwrap();
    assert!(scopes_arr[0]["actions"].as_array().unwrap().iter().any(|a| a.as_str() == Some("delete")));

    // Push tag to delete
    let put_del_resp = client
        .put(format!("{base_url}/v2/{repo}/manifests/delete-me"))
        .basic_auth("demo", Some("demo"))
        .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(valid_manifest_bytes)
        .send()
        .await
        .expect("put manifest");
    assert_eq!(put_del_resp.status(), reqwest::StatusCode::CREATED);

    // Delete tag using the issued delete token
    let del_tag_resp = client
        .delete(format!("{base_url}/v2/{repo}/tags/reference/delete-me"))
        .bearer_auth(delete_token)
        .send()
        .await
        .expect("del tag with bearer");
    assert_eq!(del_tag_resp.status(), reqwest::StatusCode::ACCEPTED);

    // 23. Deny push operation on target repository when token is scoped for a different repository -> 403 Forbidden
    let wrong_token_resp = client
        .get(format!("{base_url}/token?service=registry-rust&scope=repository:other-repo:push"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("get token for other-repo");
    assert_eq!(wrong_token_resp.status(), reqwest::StatusCode::OK);
    let wrong_token_body: serde_json::Value = wrong_token_resp.json().await.expect("token json");
    let other_repo_token = wrong_token_body["token"].as_str().unwrap();

    let denied_upload_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .bearer_auth(other_repo_token)
        .send()
        .await
        .expect("denied upload create");
    assert_eq!(denied_upload_resp.status(), reqwest::StatusCode::FORBIDDEN);

    // 24. Unauthenticated request to private or uppercase repo returns 401 challenge
    let challenge_resp = client
        .get(format!("{base_url}/v2/PRIVATE_TEST_REPO/tags/list"))
        .send()
        .await
        .expect("unauth private tags");
    assert_eq!(challenge_resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert!(challenge_resp.headers().get(header::WWW_AUTHENTICATE).is_some());

    // 25. Tags pagination on terminal page omits Link header
    let terminal_page_resp = client
        .get(format!("{base_url}/v2/{repo}/tags/list?n=100"))
        .send()
        .await
        .expect("get terminal tags page");
    assert_eq!(terminal_page_resp.status(), reqwest::StatusCode::OK);
    assert!(terminal_page_resp.headers().get(header::LINK).is_none());

    // 26. Deny delete operation when token only grants pull,push scope -> 403 Forbidden
    let pull_push_token_resp = client
        .get(format!("{base_url}/token?service=registry-rust&scope=repository:{repo}:pull,push"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("get pull,push token");
    assert_eq!(pull_push_token_resp.status(), reqwest::StatusCode::OK);
    let pull_push_body: serde_json::Value = pull_push_token_resp.json().await.expect("token json");
    let pull_push_token = pull_push_body["token"].as_str().unwrap();

    let denied_delete_resp = client
        .delete(format!("{base_url}/v2/{repo}/tags/reference/some-tag"))
        .bearer_auth(pull_push_token)
        .send()
        .await
        .expect("denied delete");
    assert_eq!(denied_delete_resp.status(), reqwest::StatusCode::FORBIDDEN);

    // 27. Repositories starting with 'v' or '2' work without prefix stripping corruption
    let v_repo = "v2-compliance-test";
    let token_v_resp = client
        .get(format!("{base_url}/token?service=registry-rust&scope=repository:{v_repo}:pull,push"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("get token for v_repo");
    assert_eq!(token_v_resp.status(), reqwest::StatusCode::OK);
    let token_v_body: serde_json::Value = token_v_resp.json().await.expect("token json");
    let v_token = token_v_body["token"].as_str().unwrap();

    let upload_v_resp = client
        .post(format!("{base_url}/v2/{v_repo}/blobs/uploads/"))
        .bearer_auth(v_token)
        .send()
        .await
        .expect("upload to v_repo");
    assert_eq!(upload_v_resp.status(), reqwest::StatusCode::ACCEPTED);

    // 28. Manifest with schema 1 signatures returns 400 MANIFEST_UNVERIFIED with application/json
    let signed_manifest = serde_json::json!({
        "schemaVersion": 1,
        "name": repo,
        "tag": "signed-test",
        "signatures": [{
            "header": { "jwk": { "kty": "RSA" } },
            "signature": "invalid_sig"
        }]
    });
    let manifest_resp = client
        .put(format!("{base_url}/v2/{repo}/manifests/signed-test"))
        .bearer_auth(delete_token)
        .header(header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&signed_manifest).unwrap())
        .send()
        .await
        .expect("put signed manifest");
    assert_eq!(manifest_resp.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        manifest_resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let manifest_err_body: serde_json::Value = manifest_resp.json().await.expect("manifest err json");
    assert_eq!(manifest_err_body["errors"][0]["code"], "MANIFEST_UNVERIFIED");

    // 29. Upload initiation with token scoped for a different repository -> 403 Forbidden (DENIED)
    let other_repo = "other-unauthorized-repo";
    let token_other_resp = client
        .get(format!("{base_url}/token?service=registry-rust&scope=repository:{other_repo}:pull,push"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("get token for other_repo");
    assert_eq!(token_other_resp.status(), reqwest::StatusCode::OK);
    let token_other_body: serde_json::Value = token_other_resp.json().await.expect("token json");
    let other_token = token_other_body["token"].as_str().unwrap();

    let target_repo = "target-enforced-repo";
    let denied_upload_resp = client
        .post(format!("{base_url}/v2/{target_repo}/blobs/uploads/"))
        .bearer_auth(other_token)
        .send()
        .await
        .expect("upload with wrong token");
    assert_eq!(denied_upload_resp.status(), reqwest::StatusCode::FORBIDDEN);
    let denied_body: serde_json::Value = denied_upload_resp.json().await.expect("denied json");
    assert_eq!(denied_body["errors"][0]["code"], "DENIED");

    // 30. Tags pagination subsequent page with last cursor omits Link header when no more pages
    let page_repo = "tags-multi-page-repo";
    let token_page_resp = client
        .get(format!("{base_url}/token?service=registry-rust&scope=repository:{page_repo}:pull,push"))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .expect("get token for page_repo");
    let token_page_body: serde_json::Value = token_page_resp.json().await.expect("token json");
    let page_token = token_page_body["token"].as_str().unwrap();

    for tag_name in &["tag-alpha", "tag-beta", "tag-gamma"] {
        let manifest = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.empty.v1+json",
                "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
                "size": 2
            },
            "layers": [{
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": sha512_digest,
                "size": sha512_data.len()
            }]
        });
        client
            .put(format!("{base_url}/v2/{page_repo}/manifests/{tag_name}"))
            .bearer_auth(page_token)
            .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
            .body(serde_json::to_vec(&manifest).unwrap())
            .send()
            .await
            .expect("put manifest");
    }

    let tags_page1_resp = client
        .get(format!("{base_url}/v2/{page_repo}/tags/list?n=1"))
        .send()
        .await
        .expect("get tags page 1");
    assert_eq!(tags_page1_resp.status(), reqwest::StatusCode::OK);
    assert!(tags_page1_resp.headers().get(header::LINK).is_some());
    let p1_body: serde_json::Value = tags_page1_resp.json().await.expect("p1 json");
    assert_eq!(p1_body["tags"].as_array().unwrap().len(), 1);
    let last_p1 = p1_body["tags"][0].as_str().unwrap();

    let tags_page2_resp = client
        .get(format!("{base_url}/v2/{page_repo}/tags/list?n=100&last={last_p1}"))
        .send()
        .await
        .expect("get tags page 2");
    assert_eq!(tags_page2_resp.status(), reqwest::StatusCode::OK);
    assert!(tags_page2_resp.headers().get(header::LINK).is_none());
    let p2_body: serde_json::Value = tags_page2_resp.json().await.expect("p2 json");
    assert_eq!(p2_body["tags"].as_array().unwrap().len(), 2);

    // 31. Cross-repository blob mount without pull permissions on source repo -> gracefully falls back to 202 Accepted upload session per OCI spec
    let mount_fallback_resp = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/?mount={sha512_digest}&from=unauthorized-secret-repo"))
        .bearer_auth(pull_push_token)
        .header(header::CONTENT_LENGTH, "0")
        .send()
        .await
        .expect("mount fallback");
    assert_eq!(mount_fallback_resp.status(), reqwest::StatusCode::ACCEPTED);
    assert!(mount_fallback_resp.headers().get(header::LOCATION).is_some());
}

