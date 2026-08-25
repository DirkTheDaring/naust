use std::path::PathBuf;
use std::process::Command;

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
