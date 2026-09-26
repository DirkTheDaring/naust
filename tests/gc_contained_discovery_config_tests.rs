use std::process::Command;

use registry_rust::config::{Config, ConfigError, StorageBackend};
use registry_rust::storage::StorageErrorKind;

fn run_isolated(worker_name: &str, env_vars: &[(&str, &str)]) {
    let mut cmd = Command::new(std::env::current_exe().expect("current_exe"));
    cmd.env_clear();
    cmd.env("RUN_ISOLATED", "1");
    if let Ok(path) = std::env::var("PATH") {
        cmd.env("PATH", path);
    }
    for (k, v) in env_vars {
        cmd.env(k, v);
    }
    cmd.args(["--exact", worker_name, "--nocapture"]);
    let output = cmd.output().expect("execute isolated worker process");
    if !output.status.success() {
        panic!(
            "Isolated test worker '{}' failed with status {:?}\n--- STDOUT ---\n{}\n--- STDERR ---\n{}",
            worker_name,
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

// =================================================================================================
// 1. Compiled Defaults (Process-Isolated)
// =================================================================================================

#[test]
fn test_config_compiled_defaults_isolated() {
    run_isolated("worker_config_compiled_defaults", &[]);
}

#[test]
fn worker_config_compiled_defaults() {
    if std::env::var("RUN_ISOLATED").as_deref() != Ok("1") {
        return;
    }
    let cfg = Config::from_env().expect("Config::from_env must succeed in clean environment");
    assert_eq!(cfg.fs_gc_discovery_max_depth, 32);
    assert_eq!(cfg.fs_gc_discovery_max_dir_enumerations, 10_000);
    assert_eq!(cfg.fs_gc_discovery_max_total_discovery_entries, 250_000);
    assert_eq!(cfg.fs_gc_discovery_max_manifest_dirs, 10_000);
    assert_eq!(
        cfg.fs_gc_discovery_max_discovery_retained_path_bytes,
        10_485_760
    );
    assert_eq!(cfg.fs_gc_discovery_intermediate_dir_max_entries, 1_000);
    assert_eq!(cfg.fs_gc_discovery_intermediate_dir_max_name_bytes, 100_000);
    assert_eq!(cfg.fs_gc_discovery_max_terminal_dir_enumerations, 10_000);
    assert_eq!(cfg.fs_gc_discovery_terminal_dir_max_entries, 10_000);
    assert_eq!(cfg.fs_gc_discovery_terminal_dir_max_name_bytes, 1_500_000);
    assert_eq!(cfg.fs_gc_discovery_max_total_manifest_entries, 250_000);
    assert_eq!(cfg.fs_gc_discovery_max_manifests_read, 50_000);
    assert_eq!(cfg.fs_gc_discovery_max_total_references, 250_000);
    assert_eq!(cfg.fs_gc_discovery_max_retained_logical_bytes, 33_554_432);
    assert_eq!(cfg.fs_gc_discovery_max_manifest_payload_bytes, None);
}

// =================================================================================================
// 2. TOML Overrides (Process-Isolated)
// =================================================================================================

#[test]
fn test_config_toml_overrides_isolated() {
    let temp = tempfile::tempdir().unwrap();
    let toml_path = temp.path().join("config.toml");
    std::fs::write(
        &toml_path,
        r#"
[server]
listen_addr = "127.0.0.1:5000"

[storage]
backend = "filesystem"

[storage.fs]
root = "./data"

[storage.fs.gc.discovery]
max_depth = 48
max_dir_enumerations = 15000
max_total_discovery_entries = 300000
max_manifest_dirs = 12000
max_discovery_retained_path_bytes = 20000000
intermediate_dir_max_entries = 2000
intermediate_dir_max_name_bytes = 200000
max_terminal_dir_enumerations = 15000
terminal_dir_max_entries = 20000
terminal_dir_max_name_bytes = 3000000
max_total_manifest_entries = 400000
max_manifests_read = 60000
max_total_references = 350000
max_retained_logical_bytes = 50000000
max_manifest_payload_bytes = 2097152
"#,
    )
    .unwrap();

    let toml_path_str = toml_path.to_string_lossy().to_string();
    run_isolated(
        "worker_config_toml_overrides",
        &[("CONFIG_PATH", &toml_path_str)],
    );
}

#[test]
fn worker_config_toml_overrides() {
    if std::env::var("RUN_ISOLATED").as_deref() != Ok("1") {
        return;
    }
    let cfg = Config::from_env().expect("Config::from_env with CONFIG_PATH must succeed");
    assert_eq!(cfg.fs_gc_discovery_max_depth, 48);
    assert_eq!(cfg.fs_gc_discovery_max_dir_enumerations, 15_000);
    assert_eq!(cfg.fs_gc_discovery_max_total_discovery_entries, 300_000);
    assert_eq!(cfg.fs_gc_discovery_max_manifest_dirs, 12_000);
    assert_eq!(
        cfg.fs_gc_discovery_max_discovery_retained_path_bytes,
        20_000_000
    );
    assert_eq!(cfg.fs_gc_discovery_intermediate_dir_max_entries, 2_000);
    assert_eq!(cfg.fs_gc_discovery_intermediate_dir_max_name_bytes, 200_000);
    assert_eq!(cfg.fs_gc_discovery_max_terminal_dir_enumerations, 15_000);
    assert_eq!(cfg.fs_gc_discovery_terminal_dir_max_entries, 20_000);
    assert_eq!(cfg.fs_gc_discovery_terminal_dir_max_name_bytes, 3_000_000);
    assert_eq!(cfg.fs_gc_discovery_max_total_manifest_entries, 400_000);
    assert_eq!(cfg.fs_gc_discovery_max_manifests_read, 60_000);
    assert_eq!(cfg.fs_gc_discovery_max_total_references, 350_000);
    assert_eq!(cfg.fs_gc_discovery_max_retained_logical_bytes, 50_000_000);
    assert_eq!(
        cfg.fs_gc_discovery_max_manifest_payload_bytes,
        Some(2_097_152)
    );
}

// =================================================================================================
// 3. Hierarchical vs. Flat Environment Aliases (Process-Isolated)
// =================================================================================================

#[test]
fn test_config_hierarchical_env_isolated() {
    run_isolated(
        "worker_config_hierarchical_env",
        &[
            ("REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_DEPTH", "64"),
            (
                "REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_MANIFEST_PAYLOAD_BYTES",
                "4194304",
            ),
        ],
    );
}

#[test]
fn worker_config_hierarchical_env() {
    if std::env::var("RUN_ISOLATED").as_deref() != Ok("1") {
        return;
    }
    let cfg = Config::from_env().expect("Config::from_env must parse hierarchical env");
    assert_eq!(cfg.fs_gc_discovery_max_depth, 64);
    assert_eq!(
        cfg.fs_gc_discovery_max_manifest_payload_bytes,
        Some(4_194_304)
    );
}

#[test]
fn test_config_flat_env_alias_isolated() {
    run_isolated(
        "worker_config_flat_env_alias",
        &[
            ("REGISTRY_BLOB_GC_DISCOVERY_MAX_DEPTH", "55"),
            (
                "REGISTRY_BLOB_GC_DISCOVERY_MAX_MANIFEST_PAYLOAD_BYTES",
                "8388608",
            ),
        ],
    );
}

#[test]
fn worker_config_flat_env_alias() {
    if std::env::var("RUN_ISOLATED").as_deref() != Ok("1") {
        return;
    }
    let cfg = Config::from_env().expect("Config::from_env must parse flat env alias");
    assert_eq!(cfg.fs_gc_discovery_max_depth, 55);
    assert_eq!(
        cfg.fs_gc_discovery_max_manifest_payload_bytes,
        Some(8_388_608)
    );
}

// =================================================================================================
// 4. Environment vs. TOML Precedence (Process-Isolated)
// =================================================================================================

#[test]
fn test_config_precedence_env_over_toml_isolated() {
    let temp = tempfile::tempdir().unwrap();
    let toml_path = temp.path().join("config.toml");
    std::fs::write(
        &toml_path,
        r#"
[server]
listen_addr = "127.0.0.1:5000"
[storage]
backend = "filesystem"
[storage.fs]
root = "./data"
[storage.fs.gc.discovery]
max_depth = 40
max_manifest_dirs = 5000
"#,
    )
    .unwrap();

    let toml_path_str = toml_path.to_string_lossy().to_string();
    run_isolated(
        "worker_config_precedence_env_over_toml",
        &[
            ("CONFIG_PATH", &toml_path_str),
            ("REGISTRY_BLOB_GC_DISCOVERY_MAX_DEPTH", "70"),
            ("REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_DEPTH", "85"),
            ("REGISTRY_BLOB_GC_DISCOVERY_MAX_MANIFEST_DIRS", "6000"),
        ],
    );
}

#[test]
fn worker_config_precedence_env_over_toml() {
    if std::env::var("RUN_ISOLATED").as_deref() != Ok("1") {
        return;
    }
    let cfg = Config::from_env().expect("must parse with env precedence");
    // Hierarchical env (85) beats flat env (70) and TOML (40)
    assert_eq!(cfg.fs_gc_discovery_max_depth, 85);
    // Flat env (6000) beats TOML (5000) when hierarchical is unset
    assert_eq!(cfg.fs_gc_discovery_max_manifest_dirs, 6000);
}

// =================================================================================================
// 5. Invalid Numeric Input and Overflow (Process-Isolated)
// =================================================================================================

#[test]
fn test_config_invalid_numeric_input_isolated() {
    run_isolated(
        "worker_config_invalid_numeric_input",
        &[(
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_DEPTH",
            "not_a_number",
        )],
    );
}

#[test]
fn worker_config_invalid_numeric_input() {
    if std::env::var("RUN_ISOLATED").as_deref() != Ok("1") {
        return;
    }
    let err = Config::from_env().expect_err("non-numeric input must fail");
    match err {
        ConfigError::InvalidEnvValue { key, .. } => {
            assert!(
                key.contains("MAX_DEPTH") || key.contains("max_depth"),
                "error must mention invalid key: {key}"
            );
        }
        other => panic!("expected InvalidEnvValue, got: {other:?}"),
    }
}

#[test]
fn test_config_numeric_overflow_isolated() {
    run_isolated(
        "worker_config_numeric_overflow",
        &[(
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_DEPTH",
            "99999999999999999999999999999999999999999999999999999999999999999999",
        )],
    );
}

#[test]
fn worker_config_numeric_overflow() {
    if std::env::var("RUN_ISOLATED").as_deref() != Ok("1") {
        return;
    }
    let err = Config::from_env().expect_err("overflowing input must fail");
    match err {
        ConfigError::InvalidEnvValue { key, .. } => {
            assert!(
                key.contains("MAX_DEPTH") || key.contains("max_depth"),
                "error must mention overflowing key: {key}"
            );
        }
        other => panic!("expected InvalidEnvValue, got: {other:?}"),
    }
}

// =================================================================================================
// 6. Validation Boundaries and Optional Payload Ceiling Behavior (Process-Isolated)
// =================================================================================================

#[test]
fn test_config_validation_boundaries_isolated() {
    run_isolated(
        "worker_config_validation_boundaries",
        &[
            ("REGISTRY__STORAGE__BACKEND", "filesystem"),
            ("REGISTRY__STORAGE__FS__ROOT", "./data"),
        ],
    );
}

#[test]
fn worker_config_validation_boundaries() {
    if std::env::var("RUN_ISOLATED").as_deref() != Ok("1") {
        return;
    }

    let test_boundary_failure = |toml_snippet: &str, expected_message: &str| {
        let temp = tempfile::tempdir().unwrap();
        let p = temp.path().join("c.toml");
        std::fs::write(
            &p,
            format!(
                r#"
[server]
listen_addr = "127.0.0.1:5000"
[storage]
backend = "filesystem"
[storage.fs]
root = "./data"
{toml_snippet}
"#
            ),
        )
        .unwrap();

        let err = Config::from_env_with_files(&[p]).expect_err("should fail boundary");
        match err {
            ConfigError::InvalidValue { message, .. } => {
                assert!(
                    message.contains(expected_message),
                    "expected message containing '{expected_message}', got: {message}"
                );
            }
            other => panic!("expected InvalidValue, got: {other:?}"),
        }
    };

    // 1. max_depth < 1
    test_boundary_failure(
        "[storage.fs.gc.discovery]\nmax_depth = 0",
        "must be at least 1",
    );

    // 2. intermediate_dir_max_name_bytes < 128
    test_boundary_failure(
        "[storage.fs.gc.discovery]\nintermediate_dir_max_name_bytes = 100",
        "must be at least 128 bytes",
    );

    // 3. payload ceiling < 1024
    test_boundary_failure(
        "[storage.fs.gc.discovery]\nmax_manifest_payload_bytes = 512",
        "must be at least 1024",
    );

    // 4. Valid ceiling at exact boundary: 1024
    let temp = tempfile::tempdir().unwrap();
    let p = temp.path().join("c.toml");
    std::fs::write(
        &p,
        r#"
[server]
listen_addr = "127.0.0.1:5000"
[storage]
backend = "filesystem"
[storage.fs]
root = "./data"
[storage.fs.gc.discovery]
max_manifest_payload_bytes = 1024
"#,
    )
    .unwrap();
    let cfg = Config::from_env_with_files(&[p]).expect("1024 payload ceiling is valid");
    assert_eq!(cfg.fs_gc_discovery_max_manifest_payload_bytes, Some(1024));

    // 5. Unset ceiling defaults to None
    let temp2 = tempfile::tempdir().unwrap();
    let p2 = temp2.path().join("c.toml");
    std::fs::write(
        &p2,
        r#"
[server]
listen_addr = "127.0.0.1:5000"
[storage]
backend = "filesystem"
[storage.fs]
root = "./data"
"#,
    )
    .unwrap();
    let cfg2 = Config::from_env_with_files(&[p2]).expect("unset payload ceiling is valid");
    assert_eq!(cfg2.fs_gc_discovery_max_manifest_payload_bytes, None);
}

#[test]
fn test_config_payload_ceiling_boundary_u64_max_isolated() {
    run_isolated(
        "worker_config_payload_ceiling_boundary_u64_max",
        &[(
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_MANIFEST_PAYLOAD_BYTES",
            "18446744073709551615",
        )],
    );
}

#[test]
fn worker_config_payload_ceiling_boundary_u64_max() {
    if std::env::var("RUN_ISOLATED").as_deref() != Ok("1") {
        return;
    }
    let err = Config::from_env().expect_err("u64::MAX ceiling must fail validation");
    match err {
        ConfigError::InvalidValue { field, message } => {
            assert_eq!(field, "storage.fs.gc.discovery.max_manifest_payload_bytes");
            assert!(
                message.contains("less than u64::MAX"),
                "expected message containing 'less than u64::MAX', got: {message}"
            );
        }
        other => panic!("expected InvalidValue, got: {other:?}"),
    }
}

// =================================================================================================
// 7. Storage Wiring Constructor Validation (Process-Isolated)
// =================================================================================================

#[test]
fn test_config_storage_wiring_constructor_validation_isolated() {
    run_isolated("worker_config_storage_wiring_constructor_validation", &[]);
}

#[test]
fn worker_config_storage_wiring_constructor_validation() {
    if std::env::var("RUN_ISOLATED").as_deref() != Ok("1") {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let mut cfg = Config::from_env_with_files(&[]).unwrap();
    cfg.storage_backend = StorageBackend::Filesystem;
    cfg.fs_root = temp.path().to_path_buf();

    // Valid configuration succeeds
    let wiring = registry_rust::storage_wiring::storage_wiring_try_from_config(&cfg)
        .expect("valid limits must succeed");
    assert_eq!(wiring.gc_service_port().kind(), "fs");

    // Invalid max_depth = 0
    cfg.fs_gc_discovery_max_depth = 0;
    let err = match registry_rust::storage_wiring::storage_wiring_try_from_config(&cfg) {
        Ok(_) => panic!("max_depth = 0 must fail constructor validation"),
        Err(e) => e,
    };
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::Configuration));
    assert!(err.to_string().contains("max_depth"));

    // Reset and test invalid terminal_dir_max_name_bytes < 128
    cfg.fs_gc_discovery_max_depth = 32;
    cfg.fs_gc_discovery_terminal_dir_max_name_bytes = 64;
    let err = match registry_rust::storage_wiring::storage_wiring_try_from_config(&cfg) {
        Ok(_) => panic!("name bytes < 128 must fail constructor validation"),
        Err(e) => e,
    };
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::Configuration));
    assert!(err.to_string().contains("max_name_bytes"));
}
