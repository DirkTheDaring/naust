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
