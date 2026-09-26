use reqwest::header;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn pick_unused_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral port");
    listener.local_addr().expect("local_addr").port()
}

fn hex_sha256(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let out = hasher.finalize();
    hex::encode(out)
}

fn bin_path() -> String {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_naust") {
        return p;
    }
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_naust") {
        return p;
    }

    let exe = std::env::current_exe().expect("current_exe");
    let deps_dir = exe.parent().expect("exe parent");
    let profile_dir = deps_dir.parent().expect("deps parent");

    let bin_name = if cfg!(windows) { "naust.exe" } else { "naust" };

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

struct ServerOptions<'a> {
    auth_strategy: &'a str,
    username: Option<&'a str>,
    password: Option<&'a str>,
    push_actions: Option<&'a str>,
    push_implies_delete: bool,
    anonymous_pull: bool,
}

impl<'a> Default for ServerOptions<'a> {
    fn default() -> Self {
        Self {
            auth_strategy: "both",
            username: Some("demo"),
            password: Some("demo"),
            push_actions: None,
            push_implies_delete: false,
            anonymous_pull: true,
        }
    }
}

async fn start_server_with_opts(
    dir: &tempfile::TempDir,
    opts: ServerOptions<'_>,
) -> (ServerGuard, String) {
    let port = pick_unused_port();
    let fs_root = dir.path().join("data");
    std::fs::create_dir_all(&fs_root).expect("mkdir data");

    let exe = bin_path();
    let mut cmd = Command::new(exe);
    cmd.arg("server");
    cmd.env("STORAGE_BACKEND", "fs");
    cmd.env("STORAGE_FS_ROOT", &fs_root);
    cmd.env("LISTEN_ADDR", format!("127.0.0.1:{port}"));
    cmd.env("TOKEN_SIGNING_KEY", "test-signing-key");
    cmd.env("PUBLIC_URL", format!("http://127.0.0.1:{port}"));
    cmd.env("REGISTRY_PUSH_ALLOW_REPOS", "*");
    cmd.env("REGISTRY_AUTH_STRATEGY", opts.auth_strategy);
    cmd.env("ALLOW_TAG_OVERWRITE", "1");
    cmd.env("RUST_LOG", "debug");
    cmd.env(
        "REGISTRY_AUTH_ANONYMOUS_PULL",
        if opts.anonymous_pull { "1" } else { "0" },
    );

    if let Some(actions) = opts.push_actions {
        cmd.env("REGISTRY_PUSH_ACTIONS", actions);
    }
    if opts.push_implies_delete {
        cmd.env("REGISTRY_PUSH_IMPLIES_DELETE", "1");
    }

    if let (Some(u), Some(p)) = (opts.username, opts.password) {
        cmd.env("REGISTRY_USERNAME", u);
        cmd.env("REGISTRY_PASSWORD", p);
    } else {
        cmd.env_remove("REGISTRY_USERNAME");
        cmd.env_remove("REGISTRY_PASSWORD");
    }

    let child = cmd
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn registry");

    let guard = ServerGuard { child };
    let base_url = format!("http://127.0.0.1:{port}");

    // Wait for health
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .unwrap();

    let mut ready = false;
    for _ in 0..100 {
        if let Ok(res) = client.get(format!("{base_url}/v2/")).send().await
            && (res.status().is_success() || res.status() == reqwest::StatusCode::UNAUTHORIZED)
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ready, "registry did not become ready on port {port}");

    (guard, base_url)
}

async fn start_server(
    dir: &tempfile::TempDir,
    auth_strategy: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> (ServerGuard, String) {
    let opts = ServerOptions {
        auth_strategy,
        username,
        password,
        push_actions: Some("pull,push,delete"),
        push_implies_delete: false,
        anonymous_pull: true,
    };
    start_server_with_opts(dir, opts).await
}

#[tokio::test]
async fn test_upload_status_get_preserves_state_token_for_resumption() {
    let tmp = tempfile::tempdir().unwrap();
    let (_guard, base_url) = start_server(&tmp, "token", Some("demo"), Some("demo")).await;
    let client = reqwest::Client::new();

    // 1. Authenticate to get token
    let token_res = client
        .get(format!(
            "{base_url}/token?service=naust&scope=repository:test/repo:pull,push,delete"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    assert_eq!(token_res.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = token_res.json().await.unwrap();
    let token = body["token"].as_str().unwrap();

    // 2. Start upload session
    let start_res = client
        .post(format!("{base_url}/v2/test/repo/blobs/uploads/"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(start_res.status(), reqwest::StatusCode::ACCEPTED);
    let location = start_res
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    let uuid = start_res
        .headers()
        .get("Docker-Upload-UUID")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(location.contains("_state="));

    // 3. Send first chunk (10 bytes) via PATCH
    let chunk1 = b"0123456789";
    let patch_res = client
        .patch(format!("{base_url}{location}"))
        .bearer_auth(token)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, chunk1.len().to_string())
        .header(header::CONTENT_RANGE, "0-9")
        .body(chunk1.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(patch_res.status(), reqwest::StatusCode::ACCEPTED);

    // 4. Issue GET to query upload status (simulating network resumption check)
    let get_status_res = client
        .get(format!("{base_url}/v2/test/repo/blobs/uploads/{uuid}"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(get_status_res.status(), reqwest::StatusCode::NO_CONTENT);
    let resumed_location = get_status_res
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    let range_hdr = get_status_res
        .headers()
        .get(header::RANGE)
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(range_hdr, "0-9");
    assert!(
        resumed_location.contains("_state="),
        "Location from GET upload_status must include signed _state token"
    );

    // 5. Send second chunk (10 bytes) using the Location received from GET
    let chunk2 = b"abcdefghij";
    let patch2_res = client
        .patch(format!("{base_url}{resumed_location}"))
        .bearer_auth(token)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, chunk2.len().to_string())
        .header(header::CONTENT_RANGE, "10-19")
        .body(chunk2.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(patch2_res.status(), reqwest::StatusCode::ACCEPTED);
    let final_loc = patch2_res
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();

    // 6. Finalize upload with PUT
    let mut full_bytes = Vec::new();
    full_bytes.extend_from_slice(chunk1);
    full_bytes.extend_from_slice(chunk2);
    let digest = format!("sha256:{}", hex_sha256(&full_bytes));

    let put_res = client
        .put(format!("{base_url}{final_loc}&digest={digest}"))
        .bearer_auth(token)
        .header(header::CONTENT_LENGTH, "0")
        .send()
        .await
        .unwrap();
    assert_eq!(put_res.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn test_cross_mount_fallback_non_disclosure_matrix() {
    let tmp = tempfile::tempdir().unwrap();
    let (_guard, base_url) = start_server(&tmp, "token", Some("demo"), Some("demo")).await;
    let client = reqwest::Client::new();

    // Setup target token
    let token_res = client
        .get(format!(
            "{base_url}/token?service=naust&scope=repository:test/target:pull,push"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = token_res.json().await.unwrap();
    let target_token = body["token"].as_str().unwrap();

    // Case 1: Source repo exists and blob exists, but caller lacks Pull on source
    // (First upload blob to real source repo using full admin token)
    let admin_token_res = client
        .get(format!(
            "{base_url}/token?service=naust&scope=repository:secret/source:pull,push,delete"
        ))
        .basic_auth("demo", Some("demo"))
        .send()
        .await
        .unwrap();
    let admin_body: serde_json::Value = admin_token_res.json().await.unwrap();
    let admin_token = admin_body["token"].as_str().unwrap();

    let blob_bytes = b"top-secret-content";
    let blob_digest = format!("sha256:{}", hex_sha256(blob_bytes));

    let upload_init = client
        .post(format!("{base_url}/v2/secret/source/blobs/uploads/"))
        .bearer_auth(admin_token)
        .send()
        .await
        .unwrap();
    let upload_loc = upload_init
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    client
        .put(format!("{base_url}{upload_loc}&digest={blob_digest}"))
        .bearer_auth(admin_token)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, blob_bytes.len().to_string())
        .body(blob_bytes.to_vec())
        .send()
        .await
        .unwrap();

    // Try cross-mount with target_token (no access to secret/source)
    let res1 = client
        .post(format!(
            "{base_url}/v2/test/target/blobs/uploads/?mount={blob_digest}&from=secret/source"
        ))
        .bearer_auth(target_token)
        .send()
        .await
        .unwrap();
    assert_eq!(res1.status(), reqwest::StatusCode::ACCEPTED);
    let loc1 = res1
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(loc1.starts_with("/v2/test/target/blobs/uploads/"));

    // Case 2: Source repo does not exist
    let res2 = client
        .post(format!(
            "{base_url}/v2/test/target/blobs/uploads/?mount={blob_digest}&from=nonexistent/source"
        ))
        .bearer_auth(target_token)
        .send()
        .await
        .unwrap();
    assert_eq!(res2.status(), reqwest::StatusCode::ACCEPTED);
    let loc2 = res2
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(loc2.starts_with("/v2/test/target/blobs/uploads/"));

    // Case 3: Blob does not exist
    let fake_digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
    let res3 = client
        .post(format!(
            "{base_url}/v2/test/target/blobs/uploads/?mount={fake_digest}&from=secret/source"
        ))
        .bearer_auth(target_token)
        .send()
        .await
        .unwrap();
    assert_eq!(res3.status(), reqwest::StatusCode::ACCEPTED);
    let loc3 = res3
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(loc3.starts_with("/v2/test/target/blobs/uploads/"));

    // Case 4: Missing 'from' parameter
    let res4 = client
        .post(format!(
            "{base_url}/v2/test/target/blobs/uploads/?mount={blob_digest}"
        ))
        .bearer_auth(target_token)
        .send()
        .await
        .unwrap();
    assert_eq!(res4.status(), reqwest::StatusCode::ACCEPTED);
    let loc4 = res4
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(loc4.starts_with("/v2/test/target/blobs/uploads/"));

    // Case 5: Malformed 'from' parameter returns 400 NAME_INVALID
    let res5 = client
        .post(format!(
            "{base_url}/v2/test/target/blobs/uploads/?mount={blob_digest}&from=..%2F..%2Fetc"
        ))
        .bearer_auth(target_token)
        .send()
        .await
        .unwrap();
    assert_eq!(res5.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_least_privilege_challenges_and_action_separation() {
    let tmp = tempfile::tempdir().unwrap();
    // Start server with push-only permissions (delete disallowed for default single-user)
    let opts = ServerOptions {
        auth_strategy: "both",
        username: Some("pushuser"),
        password: Some("pushpass"),
        push_actions: Some("pull,push"),
        push_implies_delete: false,
        anonymous_pull: false,
    };
    let (_guard, base_url) = start_server_with_opts(&tmp, opts).await;
    let client = reqwest::Client::new();

    // 1. Unauthenticated GET challenge requests only "pull"
    let get_res = client
        .get(format!("{base_url}/v2/test/repo/manifests/latest"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_res.status(), reqwest::StatusCode::UNAUTHORIZED);
    let get_auth = get_res
        .headers()
        .get(header::WWW_AUTHENTICATE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        get_auth.contains("scope=\"repository:test/repo:pull\""),
        "GET challenge must request only pull: {get_auth}"
    );

    // 2. Unauthenticated POST/PUT challenge requests only "push" (NOT delete)
    let post_res = client
        .post(format!("{base_url}/v2/test/repo/blobs/uploads/"))
        .send()
        .await
        .unwrap();
    assert_eq!(post_res.status(), reqwest::StatusCode::UNAUTHORIZED);
    let post_auth = post_res
        .headers()
        .get(header::WWW_AUTHENTICATE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        post_auth.contains("scope=\"repository:test/repo:push\""),
        "POST challenge must request only push: {post_auth}"
    );

    let put_res = client
        .put(format!("{base_url}/v2/test/repo/manifests/v1.0"))
        .header(
            header::CONTENT_TYPE,
            "application/vnd.oci.image.manifest.v1+json",
        )
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(put_res.status(), reqwest::StatusCode::UNAUTHORIZED);
    let put_auth = put_res
        .headers()
        .get(header::WWW_AUTHENTICATE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        put_auth.contains("scope=\"repository:test/repo:push\""),
        "PUT challenge must request only push: {put_auth}"
    );

    // 3. Unauthenticated DELETE challenge requests only "delete" (NOT push)
    let del_res = client
        .delete(format!("{base_url}/v2/test/repo/manifests/sha256:0000000000000000000000000000000000000000000000000000000000000000"))
        .send()
        .await
        .unwrap();
    assert_eq!(del_res.status(), reqwest::StatusCode::UNAUTHORIZED);
    let del_auth = del_res
        .headers()
        .get(header::WWW_AUTHENTICATE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        del_auth.contains("scope=\"repository:test/repo:delete\""),
        "DELETE challenge must request only delete: {del_auth}"
    );

    // 4. Token endpoint never grants unrequested actions
    let token_pull_res = client
        .get(format!(
            "{base_url}/token?service=naust&scope=repository:test/repo:pull"
        ))
        .basic_auth("pushuser", Some("pushpass"))
        .send()
        .await
        .unwrap();
    assert_eq!(token_pull_res.status(), reqwest::StatusCode::OK);
    let pull_body: serde_json::Value = token_pull_res.json().await.unwrap();
    let pull_scopes = pull_body["access"].as_array().unwrap();
    assert_eq!(pull_scopes[0]["actions"], serde_json::json!(["pull"]));

    // 5. Token endpoint never grants unpermitted actions (pushuser has only pull,push)
    let token_del_res = client
        .get(format!(
            "{base_url}/token?service=naust&scope=repository:test/repo:delete"
        ))
        .basic_auth("pushuser", Some("pushpass"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        token_del_res.status(),
        reqwest::StatusCode::FORBIDDEN,
        "Token request for unpermitted delete action must be denied 403"
    );

    // 6. Push-only token cannot delete
    let token_push_res = client
        .get(format!(
            "{base_url}/token?service=naust&scope=repository:test/repo:pull,push"
        ))
        .basic_auth("pushuser", Some("pushpass"))
        .send()
        .await
        .unwrap();
    assert_eq!(token_push_res.status(), reqwest::StatusCode::OK);
    let push_body: serde_json::Value = token_push_res.json().await.unwrap();
    let push_token = push_body["token"].as_str().unwrap();

    let del_blob_res = client
        .delete(format!("{base_url}/v2/test/repo/blobs/sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"))
        .bearer_auth(push_token)
        .send()
        .await
        .unwrap();
    assert_eq!(del_blob_res.status(), reqwest::StatusCode::UNAUTHORIZED);
    let del_blob_auth = del_blob_res
        .headers()
        .get(header::WWW_AUTHENTICATE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(del_blob_auth.contains("scope=\"repository:test/repo:delete\""));

    let del_tag_res = client
        .delete(format!("{base_url}/v2/test/repo/tags/reference/latest"))
        .bearer_auth(push_token)
        .send()
        .await
        .unwrap();
    assert_eq!(del_tag_res.status(), reqwest::StatusCode::UNAUTHORIZED);

    // 7. Basic auth respects push_actions (denies direct delete when pushuser lacks delete)
    let basic_del_res = client
        .delete(format!("{base_url}/v2/test/repo/tags/reference/latest"))
        .basic_auth("pushuser", Some("pushpass"))
        .send()
        .await
        .unwrap();
    assert_eq!(basic_del_res.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_delete_only_and_pull_only_identities() {
    let tmp = tempfile::tempdir().unwrap();
    // Delete-only server
    let del_opts = ServerOptions {
        auth_strategy: "both",
        username: Some("deluser"),
        password: Some("delpass"),
        push_actions: Some("delete"),
        push_implies_delete: false,
        anonymous_pull: false,
    };
    let (_guard, base_url) = start_server_with_opts(&tmp, del_opts).await;
    let client = reqwest::Client::new();

    // 1. Delete-only identity cannot upload / push
    let token_push_res = client
        .get(format!(
            "{base_url}/token?service=naust&scope=repository:test/repo:push"
        ))
        .basic_auth("deluser", Some("delpass"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        token_push_res.status(),
        reqwest::StatusCode::FORBIDDEN,
        "Delete-only identity must be denied token with push scope"
    );

    // Direct basic auth upload fails
    let basic_upload_res = client
        .post(format!("{base_url}/v2/test/repo/blobs/uploads/"))
        .basic_auth("deluser", Some("delpass"))
        .send()
        .await
        .unwrap();
    assert_eq!(basic_upload_res.status(), reqwest::StatusCode::UNAUTHORIZED);

    // 2. Delete-only identity CAN obtain delete token and delete
    let token_del_res = client
        .get(format!(
            "{base_url}/token?service=naust&scope=repository:test/repo:delete"
        ))
        .basic_auth("deluser", Some("delpass"))
        .send()
        .await
        .unwrap();
    assert_eq!(token_del_res.status(), reqwest::StatusCode::OK);
    let del_body: serde_json::Value = token_del_res.json().await.unwrap();
    let del_scopes = del_body["access"].as_array().unwrap();
    assert_eq!(del_scopes[0]["actions"], serde_json::json!(["delete"]));
}

/// The official conformance suite (v1.1.1) probes GET/HEAD of
/// `/v2/<name>/manifests/.INVALID_MANIFEST_NAME` and requires 404: a reference
/// that is neither a well-formed digest nor a well-formed tag cannot name any
/// manifest, so read paths must report MANIFEST_UNKNOWN rather than a 400
/// validation error. Regression: this returned 400 TAG_INVALID at one point.
#[tokio::test]
async fn test_invalid_manifest_reference_is_unknown_on_read_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let (_guard, base_url) = start_server(&tmp, "both", Some("demo"), Some("demo")).await;
    let client = reqwest::Client::new();

    for reference in [".INVALID_MANIFEST_NAME", "sha256:zzz", "-leadingdash"] {
        let get_res = client
            .get(format!("{base_url}/v2/test/repo/manifests/{reference}"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            get_res.status(),
            reqwest::StatusCode::NOT_FOUND,
            "GET manifest with invalid reference {reference:?} must be 404"
        );
        let body: serde_json::Value = get_res.json().await.unwrap();
        assert_eq!(
            body["errors"][0]["code"], "MANIFEST_UNKNOWN",
            "GET invalid reference {reference:?} must report MANIFEST_UNKNOWN"
        );

        let head_res = client
            .head(format!("{base_url}/v2/test/repo/manifests/{reference}"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            head_res.status(),
            reqwest::StatusCode::NOT_FOUND,
            "HEAD manifest with invalid reference {reference:?} must be 404"
        );

        let del_res = client
            .delete(format!("{base_url}/v2/test/repo/manifests/{reference}"))
            .basic_auth("demo", Some("demo"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            del_res.status(),
            reqwest::StatusCode::NOT_FOUND,
            "DELETE manifest with invalid reference {reference:?} must be 404"
        );
        let body: serde_json::Value = del_res.json().await.unwrap();
        assert_eq!(
            body["errors"][0]["code"], "MANIFEST_UNKNOWN",
            "DELETE invalid reference {reference:?} must report MANIFEST_UNKNOWN"
        );
    }
}

/// Counterpart to the read-path 404 behavior: pushing a manifest to an invalid
/// tag is a client error and must keep returning 400 TAG_INVALID.
#[tokio::test]
async fn test_invalid_tag_on_manifest_push_stays_tag_invalid() {
    let tmp = tempfile::tempdir().unwrap();
    let (_guard, base_url) = start_server(&tmp, "both", Some("demo"), Some("demo")).await;
    let client = reqwest::Client::new();

    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            // sha256 of "{}" (the canonical empty config)
            "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
            "size": 2
        },
        "layers": []
    });

    let put_res = client
        .put(format!(
            "{base_url}/v2/test/repo/manifests/.INVALID_MANIFEST_NAME"
        ))
        .basic_auth("demo", Some("demo"))
        .header(
            header::CONTENT_TYPE,
            "application/vnd.oci.image.manifest.v1+json",
        )
        .body(manifest.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(
        put_res.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "PUT manifest to invalid tag must remain 400"
    );
    let body: serde_json::Value = put_res.json().await.unwrap();
    assert_eq!(
        body["errors"][0]["code"], "TAG_INVALID",
        "PUT manifest to invalid tag must report TAG_INVALID"
    );
}
