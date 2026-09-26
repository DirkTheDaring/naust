use std::path::PathBuf;
use std::process::Command;

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

#[test]
fn test_cli_check_config_valid_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        r#"
[server]
listen_addr = "127.0.0.1:5000"
[storage]
backend = "fs"
[storage.fs]
root = "./data"
[token]
signing_key = "test-signing-key"
"#,
    )
    .unwrap();

    let output = Command::new(bin_path())
        .arg("--config")
        .arg(&config_path)
        .arg("check-config")
        .output()
        .expect("run binary");

    assert!(
        output.status.success(),
        "check-config must exit 0 on valid config"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("OK"), "stdout must contain OK");
}

#[test]
fn test_cli_check_config_invalid_fails_without_panic() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("invalid.toml");
    std::fs::write(
        &config_path,
        r#"
[server.tls.acme]
enabled = true
# missing required email, names, output_dir
"#,
    )
    .unwrap();

    let output = Command::new(bin_path())
        .arg("--config")
        .arg(&config_path)
        .arg("check-config")
        .output()
        .expect("run binary");

    assert_eq!(
        output.status.code(),
        Some(2),
        "check-config must exit with code 2 on invalid config"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("check-config: failed to load config"),
        "stderr should contain actionable error prefix: {stderr}"
    );
    assert!(
        stderr.contains("server.tls.acme.email"),
        "stderr should mention missing required field: {stderr}"
    );
    assert!(
        !stderr.contains("thread 'main' panicked"),
        "stderr must NOT contain panic output: {stderr}"
    );
    assert!(
        !stderr.contains("stack backtrace:"),
        "stderr must NOT contain backtrace: {stderr}"
    );
}

#[test]
fn test_cli_server_invalid_config_fails_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("invalid_server.toml");
    let secret = "VERY_SECRET_KEY_NEVER_LOG_ME_12345";
    std::fs::write(
        &config_path,
        format!(
            r#"
[config]
strict = true

[server]
listen_addr = "127.0.0.1:5000"
typo_field = "unknown"

[[token.signing_keys]]
kid = "dup-key"
key = "{secret}"

[[token.signing_keys]]
kid = "dup-key"
key = "{secret}"
"#
        ),
    )
    .unwrap();

    let output = Command::new(bin_path())
        .arg("--config")
        .arg(&config_path)
        .arg("server")
        .output()
        .expect("run binary");

    assert_eq!(
        output.status.code(),
        Some(2),
        "server must exit with code 2 on invalid config"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("server: failed to load config"),
        "stderr should contain server config error prefix: {stderr}"
    );
    assert!(
        !stderr.contains(secret),
        "stderr must NOT leak configured secret values: {stderr}"
    );
    assert!(
        !stderr.contains("thread 'main' panicked"),
        "stderr must NOT contain panic output: {stderr}"
    );
}

#[test]
fn test_cli_blob_gc_invalid_config_fails_cleanly() {
    let non_existent = PathBuf::from("/non/existent/path/for/gc.toml");
    let output = Command::new(bin_path())
        .arg("--config")
        .arg(&non_existent)
        .arg("blob-gc")
        .arg("plan")
        .output()
        .expect("run binary");

    assert_eq!(
        output.status.code(),
        Some(2),
        "blob-gc must exit with code 2 on missing config"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("blob-gc: failed to load config"),
        "stderr should contain blob-gc config error prefix: {stderr}"
    );
    assert!(
        !stderr.contains("thread 'main' panicked"),
        "stderr must NOT contain panic output: {stderr}"
    );
}

#[test]
fn test_cli_env_invalid_value_redacts_secrets() {
    let sentinel = "SENTINEL_TOP_SECRET_PASSWORD_CLI_TEST_98765";
    let output = Command::new(bin_path())
        .env("LISTEN_ADDR", sentinel)
        .arg("check-config")
        .output()
        .expect("run binary");

    assert_eq!(
        output.status.code(),
        Some(2),
        "check-config must exit 2 on invalid env value"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("LISTEN_ADDR"),
        "stderr should identify the environment variable key: {stderr}"
    );
    assert!(
        stderr.contains("expected valid socket address"),
        "stderr should specify expected format: {stderr}"
    );
    assert!(
        !stderr.contains(sentinel),
        "stderr must NEVER contain the raw input value: {stderr}"
    );
    assert!(
        !stderr.contains("SENTINEL_TOP_SECRET"),
        "stderr must NOT contain substrings of the secret: {stderr}"
    );
    assert!(
        !stderr.contains("thread 'main' panicked"),
        "stderr must NOT contain panic output: {stderr}"
    );
}

#[test]
fn test_topology_1_fs_with_online_gc_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("fs_gc.toml");
    std::fs::write(
        &config_path,
        r#"
[server]
listen_addr = "127.0.0.1:5000"

[storage]
backend = "fs"

[storage.fs]
root = "./data"

[ref_index]
enabled = true
path = "./data/ref-index"

[blob_gc]
enabled = true
enable_delete = true
"#,
    )
    .unwrap();

    let output = Command::new(bin_path())
        .arg("--config")
        .arg(&config_path)
        .arg("check-config")
        .output()
        .expect("run binary");

    assert!(
        output.status.success(),
        "Filesystem backend with online destructive GC must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_topology_2_s3_local_index_online_gc_without_single_instance_fails() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("s3_unsafe_gc.toml");
    std::fs::write(
        &config_path,
        r#"
[server]
listen_addr = "127.0.0.1:5000"

[storage]
backend = "s3"

[storage.s3]
bucket = "my-bucket"
region = "us-east-1"
single_instance_mode = false

[ref_index]
enabled = true
path = "./data/ref-index"

[blob_gc]
enabled = true
enable_delete = true
"#,
    )
    .unwrap();

    let output = Command::new(bin_path())
        .arg("--config")
        .arg(&config_path)
        .arg("check-config")
        .output()
        .expect("run binary");

    assert_eq!(
        output.status.code(),
        Some(2),
        "S3 with local Sled index and destructive GC must fail closed unless single_instance_mode=true"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("single_instance_mode")
            || stderr.contains("S3 storage with local reference index"),
        "stderr should mention single_instance_mode safety requirement: {stderr}"
    );
}

#[test]
fn test_topology_3_s3_local_index_online_gc_disabled_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("s3_gc_disabled.toml");
    std::fs::write(
        &config_path,
        r#"
[server]
listen_addr = "127.0.0.1:5000"

[storage]
backend = "s3"

[storage.s3]
bucket = "my-bucket"
region = "us-east-1"
single_instance_mode = false

[ref_index]
enabled = true
path = "./data/ref-index"

[blob_gc]
enabled = false
enable_delete = false
"#,
    )
    .unwrap();

    let output = Command::new(bin_path())
        .arg("--config")
        .arg(&config_path)
        .arg("check-config")
        .output()
        .expect("run binary");

    assert!(
        output.status.success(),
        "S3 with local index and online GC disabled must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_topology_4_s3_with_affirmative_single_instance_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("s3_single_instance.toml");
    std::fs::write(
        &config_path,
        r#"
[server]
listen_addr = "127.0.0.1:5000"

[storage]
backend = "s3"

[storage.s3]
bucket = "my-bucket"
region = "us-east-1"
single_instance_mode = true

[ref_index]
enabled = true
path = "./data/ref-index"

[blob_gc]
enabled = true
enable_delete = true
"#,
    )
    .unwrap();

    let output = Command::new(bin_path())
        .arg("--config")
        .arg(&config_path)
        .arg("check-config")
        .output()
        .expect("run binary");

    assert!(
        output.status.success(),
        "S3 with affirmative single_instance_mode=true and online GC must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_cli_offline_s3_gc_rejected_without_confirmation() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("s3_cfg.toml");
    let ref_path = dir.path().join("ref-index");
    std::fs::create_dir_all(&ref_path).unwrap();

    std::fs::write(
        &config_path,
        format!(
            r#"
[server]
listen_addr = "127.0.0.1:5000"

[storage]
backend = "s3"

[storage.s3]
bucket = "test-bucket"
region = "us-east-1"
single_instance_mode = true

[storage.ref_index]
enabled = true
path = "{}"
"#,
            ref_path.display()
        ),
    )
    .unwrap();

    // 1. Destructive 'quarantine' without --confirm-all-writers-stopped fails with exit code 2
    let output = Command::new(bin_path())
        .arg("--config")
        .arg(&config_path)
        .arg("blob-gc")
        .arg("quarantine")
        .output()
        .expect("run binary");

    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("confirm-all-writers-stopped"),
        "stderr must explain confirmation requirement: {stderr}"
    );

    // 2. Destructive 'delete' without --confirm-all-writers-stopped fails with exit code 2
    let output_del = Command::new(bin_path())
        .arg("--config")
        .arg(&config_path)
        .arg("blob-gc")
        .arg("delete")
        .output()
        .expect("run binary");

    assert_eq!(output_del.status.code(), Some(2));
}

#[test]
fn test_cli_offline_s3_gc_plan_dry_run_allowed_without_confirmation() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("s3_cfg.toml");
    let ref_path = dir.path().join("ref-index");
    std::fs::create_dir_all(&ref_path).unwrap();

    std::fs::write(
        &config_path,
        format!(
            r#"
[server]
listen_addr = "127.0.0.1:5000"

[storage]
backend = "s3"

[storage.s3]
bucket = "test-bucket"
region = "us-east-1"
single_instance_mode = true

[storage.ref_index]
enabled = true
path = "{}"
"#,
            ref_path.display()
        ),
    )
    .unwrap();

    // Dry run 'plan' is allowed without confirmation (does not exit with code 2)
    let output = Command::new(bin_path())
        .arg("--config")
        .arg(&config_path)
        .arg("blob-gc")
        .arg("plan")
        .output()
        .expect("run binary");

    // Exit code should not be 2 (the confirmation error)
    assert_ne!(output.status.code(), Some(2));
}
