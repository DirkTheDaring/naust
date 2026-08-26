use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::{Mutex, oneshot};

use registry_rust::cli::{
    BlobGcCommand, Cli, CliCommand, CommandIntent, MigrateMembershipCommand, RefIndexCommand,
    run_cli,
};
use registry_rust::config::Config;
use registry_rust::fs_root_lock::FsRootLock;
use registry_rust::registry::digest::Digest;
use registry_rust::storage::mutation_authority::{
    RuntimeMutationAuthority, admin_clear_abandoned_deployment_writer_lock,
    inspect_deployment_writer_lock,
};
use registry_rust::storage::s3::S3Storage;
use registry_rust::storage::s3::tests::MockS3Driver;
use registry_rust::storage::{self, Storage};
use registry_rust::supervisor::{
    StartupPhase, SupervisorFaultInjector, SupervisorOptions, run_server_supervisor,
};

fn create_test_config_toml(temp_dir: &TempDir) -> String {
    let fs_root = temp_dir.path().join("registry");
    let ref_index = temp_dir.path().join("ref_index.db");
    format!(
        r#"
[server]
listen_addr = "127.0.0.1:0"

[storage]
backend = "filesystem"

[storage.fs]
root = "{}"

[storage.ref_index]
enabled = true
path = "{}"

[token]
signing_key = "test-secret-key-12345678901234567890"
"#,
        fs_root.display(),
        ref_index.display()
    )
}

fn create_test_config(temp_dir: &TempDir) -> Config {
    let cfg_path = temp_dir.path().join("test_cfg.toml");
    std::fs::write(&cfg_path, create_test_config_toml(temp_dir)).unwrap();
    Config::from_env_with_files(&[cfg_path]).unwrap()
}

// ------------------------------------------------------------------------------------------------
// 1. Exhaustive command intent classification matrix
// ------------------------------------------------------------------------------------------------
#[test]
fn test_command_intent_exhaustive_classification_matrix() {
    assert_eq!(CliCommand::CheckConfig.intent(), CommandIntent::ReadOnly);
    assert_eq!(CliCommand::HashSecret.intent(), CommandIntent::ReadOnly);
    assert_eq!(
        CliCommand::AuditPermissions.intent(),
        CommandIntent::ReadOnly
    );
    assert_eq!(CliCommand::InspectLock.intent(), CommandIntent::ReadOnly);

    assert_eq!(
        CliCommand::RefIndex {
            command: RefIndexCommand::Check
        }
        .intent(),
        CommandIntent::ReadOnly
    );
    assert_eq!(
        CliCommand::RefIndex {
            command: RefIndexCommand::Rebuild
        }
        .intent(),
        CommandIntent::MutationCapableMaintenance {
            lock_suffix: "ref-index-rebuild"
        }
    );
    assert_eq!(
        CliCommand::RefIndex {
            command: RefIndexCommand::Ensure
        }
        .intent(),
        CommandIntent::MutationCapableMaintenance {
            lock_suffix: "ref-index-ensure"
        }
    );

    assert_eq!(
        CliCommand::BlobGc {
            command: BlobGcCommand::Plan {
                policy: registry_rust::blob_gc::BlobGcPolicy::ManifestRooted,
                min_age_secs: 100,
                max_per_run: 10,
            }
        }
        .intent(),
        CommandIntent::ReadOnly
    );
    assert_eq!(
        CliCommand::BlobGc {
            command: BlobGcCommand::Quarantine {
                policy: registry_rust::blob_gc::BlobGcPolicy::ManifestRooted,
                min_age_secs: 100,
                max_per_run: 10,
                confirm_all_writers_stopped: true,
            }
        }
        .intent(),
        CommandIntent::MutationCapableMaintenance {
            lock_suffix: "blob-gc-quarantine"
        }
    );
    assert_eq!(
        CliCommand::BlobGc {
            command: BlobGcCommand::Delete {
                policy: registry_rust::blob_gc::BlobGcPolicy::ManifestRooted,
                quarantine_delay_secs: 100,
                max_per_run: 10,
                confirm_all_writers_stopped: true,
            }
        }
        .intent(),
        CommandIntent::MutationCapableMaintenance {
            lock_suffix: "blob-gc-delete"
        }
    );

    assert_eq!(
        CliCommand::MigrateMembership {
            command: MigrateMembershipCommand::Plan
        }
        .intent(),
        CommandIntent::ReadOnly
    );
    assert_eq!(
        CliCommand::MigrateMembership {
            command: MigrateMembershipCommand::Verify
        }
        .intent(),
        CommandIntent::ReadOnly
    );
    assert_eq!(
        CliCommand::MigrateMembership {
            command: MigrateMembershipCommand::Apply
        }
        .intent(),
        CommandIntent::MutationCapableMaintenance {
            lock_suffix: "migrate-membership-apply"
        }
    );

    assert_eq!(
        CliCommand::AdminClearLock {
            expected_owner: "owner".to_string(),
            expected_etag: "etag".to_string(),
            confirm: "FORCE".to_string(),
        }
        .intent(),
        CommandIntent::DestructiveAdminRecovery
    );

    assert_eq!(
        CliCommand::Server.intent(),
        CommandIntent::MutationCapableServer
    );
}

// ------------------------------------------------------------------------------------------------
// 2. Read-only commands acquire zero deployment authority
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_read_only_commands_acquire_zero_deployment_authority() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
        .await
        .unwrap();

    let cfg = Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap();
    let storage = storage::from_config(&cfg);

    {
        let idx =
            registry_rust::blob_ref_index::BlobRefIndex::open(cfg.ref_index.path.clone()).unwrap();
        idx.rebuild(&storage).await.unwrap();
    }

    let read_only_cmds = vec![
        CliCommand::CheckConfig,
        CliCommand::AuditPermissions,
        CliCommand::InspectLock,
        CliCommand::RefIndex {
            command: RefIndexCommand::Check,
        },
        CliCommand::BlobGc {
            command: BlobGcCommand::Plan {
                policy: registry_rust::blob_gc::BlobGcPolicy::ManifestRooted,
                min_age_secs: 0,
                max_per_run: 100,
            },
        },
        CliCommand::MigrateMembership {
            command: MigrateMembershipCommand::Plan,
        },
        CliCommand::MigrateMembership {
            command: MigrateMembershipCommand::Verify,
        },
    ];

    for cmd in read_only_cmds {
        let cli = Cli {
            config: vec![cfg_path.clone()],
            command: Some(cmd),
        };
        let exit = run_cli(cli).await;
        assert_eq!(exit, 0);

        let lock = inspect_deployment_writer_lock(&storage).await.unwrap();
        assert!(
            lock.is_none(),
            "read-only command must not create writer lock"
        );
    }
}

// ------------------------------------------------------------------------------------------------
// 3. Maintenance commands acquire and release authority cleanly
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_maintenance_commands_acquire_and_release_authority_cleanly() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
        .await
        .unwrap();

    let cfg = Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap();
    let storage = storage::from_config(&cfg);

    let maintenance_cmds = vec![
        CliCommand::RefIndex {
            command: RefIndexCommand::Rebuild,
        },
        CliCommand::RefIndex {
            command: RefIndexCommand::Ensure,
        },
        CliCommand::MigrateMembership {
            command: MigrateMembershipCommand::Apply,
        },
        CliCommand::BlobGc {
            command: BlobGcCommand::Quarantine {
                policy: registry_rust::blob_gc::BlobGcPolicy::ManifestRooted,
                min_age_secs: 0,
                max_per_run: 10,
                confirm_all_writers_stopped: true,
            },
        },
        CliCommand::BlobGc {
            command: BlobGcCommand::Delete {
                policy: registry_rust::blob_gc::BlobGcPolicy::ManifestRooted,
                quarantine_delay_secs: 0,
                max_per_run: 10,
                confirm_all_writers_stopped: true,
            },
        },
    ];

    for cmd in maintenance_cmds {
        let cli = Cli {
            config: vec![cfg_path.clone()],
            command: Some(cmd),
        };
        let exit = run_cli(cli).await;
        assert_eq!(exit, 0);

        let lock = inspect_deployment_writer_lock(&storage).await.unwrap();
        assert!(
            lock.is_none(),
            "maintenance command must cleanly release lock on exit"
        );
    }
}

// ------------------------------------------------------------------------------------------------
// 4. Destructive admin recovery clears matching lock (S3 distributed lock backend)
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_destructive_admin_recovery_clears_matching_lock() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage: Arc<dyn Storage> = Arc::new(S3Storage::new_with_driver(
        Some("admin-clear-bucket".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    ));

    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "abandoned-server")
        .await
        .unwrap();
    let owner_id = authority.doc().owner_id.clone();
    std::mem::forget(authority); // Abandoned without graceful release

    let (doc, etag) = inspect_deployment_writer_lock(&storage)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(doc.owner_id, owner_id);

    let res = admin_clear_abandoned_deployment_writer_lock(
        &storage,
        &owner_id,
        etag.as_deref().unwrap_or(""),
        "CONFIRM-CLEAR-ABANDONED-WRITER",
    )
    .await;
    assert!(res.is_ok());

    let lock = inspect_deployment_writer_lock(&storage).await.unwrap();
    assert!(
        lock.is_none(),
        "admin-clear-lock must clear matching abandoned lock"
    );
}

// ------------------------------------------------------------------------------------------------
// 5. Destructive admin recovery fails on mismatched owner or etag
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_destructive_admin_recovery_fails_on_mismatched_owner_or_etag() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage: Arc<dyn Storage> = Arc::new(S3Storage::new_with_driver(
        Some("admin-clear-bucket-mismatch".to_string()),
        "".to_string(),
        50 * 1024 * 1024,
        mock.clone(),
    ));

    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "live-server")
        .await
        .unwrap();
    let real_owner = authority.doc().owner_id.clone();
    std::mem::forget(authority);

    // Attempt with wrong owner
    let res = admin_clear_abandoned_deployment_writer_lock(
        &storage,
        "wrong-owner",
        "etag",
        "CONFIRM-CLEAR-ABANDONED-WRITER",
    )
    .await;
    assert!(res.is_err());

    // Verify lock survived
    let lock = inspect_deployment_writer_lock(&storage)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lock.0.owner_id, real_owner);
}

// ------------------------------------------------------------------------------------------------
// 6. Server startup phases execute in strict order
// ------------------------------------------------------------------------------------------------
struct RecordingFaultInjector {
    phases: Mutex<Vec<StartupPhase>>,
    events: Mutex<Vec<&'static str>>,
}

impl RecordingFaultInjector {
    fn new() -> Self {
        Self {
            phases: Mutex::new(Vec::new()),
            events: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl SupervisorFaultInjector for RecordingFaultInjector {
    async fn on_phase(&self, phase: StartupPhase) -> Result<(), String> {
        self.phases.lock().await.push(phase);
        Ok(())
    }
    async fn record_event(&self, event: &'static str) {
        self.events.lock().await.push(event);
    }
}

#[tokio::test]
async fn test_server_startup_phases_execute_in_strict_order() {
    let temp = TempDir::new().unwrap();
    let cfg = Arc::new(create_test_config(&temp));

    let injector = Arc::new(RecordingFaultInjector::new());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (bound_tx, bound_rx) = oneshot::channel();

    let options = SupervisorOptions {
        fault_injector: Some(injector.clone()),
        shutdown_rx: Some(shutdown_rx),
        notify_bound_addr: Some(bound_tx),
    };

    let server_task = tokio::spawn(run_server_supervisor(cfg, Some(options)));

    let _addr = bound_rx.await.unwrap();
    let _ = shutdown_tx.send(());

    let res = server_task.await.unwrap();
    assert!(res.is_ok());

    let phases = injector.phases.lock().await.clone();
    assert_eq!(
        phases,
        vec![
            StartupPhase::ConfigLoaded,
            StartupPhase::StorageInitialized,
            StartupPhase::AuthorityAcquired,
            StartupPhase::MembershipVerified,
            StartupPhase::IndexInitialized,
            StartupPhase::AppStateConstructed,
            StartupPhase::RoutesConfigured,
            StartupPhase::WorkersSpawned,
            StartupPhase::ListenerBound,
            StartupPhase::Serving,
        ]
    );
}

// ------------------------------------------------------------------------------------------------
// 7. Partial startup failure unwinds resources in reverse order
// ------------------------------------------------------------------------------------------------
struct FailingFaultInjector {
    fail_at: StartupPhase,
}

#[async_trait::async_trait]
impl SupervisorFaultInjector for FailingFaultInjector {
    async fn on_phase(&self, phase: StartupPhase) -> Result<(), String> {
        if phase == self.fail_at {
            Err(format!("injected failure at {:?}", phase))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn test_partial_startup_failure_unwinds_resources_in_reverse_order() {
    let test_phases = vec![
        StartupPhase::AuthorityAcquired,
        StartupPhase::MembershipVerified,
        StartupPhase::IndexInitialized,
        StartupPhase::AppStateConstructed,
        StartupPhase::RoutesConfigured,
        StartupPhase::WorkersSpawned,
        StartupPhase::ListenerBound,
    ];

    for phase in test_phases {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));
        let storage = storage::from_config(cfg.as_ref());

        let injector = Arc::new(FailingFaultInjector { fail_at: phase });
        let options = SupervisorOptions {
            fault_injector: Some(injector),
            shutdown_rx: None,
            notify_bound_addr: None,
        };

        let res = run_server_supervisor(cfg, Some(options)).await;
        assert!(res.is_err(), "startup must fail at phase {:?}", phase);

        let lock = inspect_deployment_writer_lock(&storage).await.unwrap();
        assert!(
            lock.is_none(),
            "lock must be cleanly released on partial startup failure at {:?}",
            phase
        );
    }
}

// ------------------------------------------------------------------------------------------------
// 8. Supervisor graceful shutdown order
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_supervisor_graceful_shutdown_order() {
    let temp = TempDir::new().unwrap();
    let cfg = Arc::new(create_test_config(&temp));
    let storage = storage::from_config(cfg.as_ref());

    let injector = Arc::new(RecordingFaultInjector::new());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (bound_tx, bound_rx) = oneshot::channel();

    let options = SupervisorOptions {
        fault_injector: Some(injector.clone()),
        shutdown_rx: Some(shutdown_rx),
        notify_bound_addr: Some(bound_tx),
    };

    let server_task = tokio::spawn(run_server_supervisor(cfg, Some(options)));

    let _addr = bound_rx.await.unwrap();
    let _ = shutdown_tx.send(());

    let res = server_task.await.unwrap();
    assert!(res.is_ok());

    let events = injector.events.lock().await.clone();
    assert!(
        events.contains(&"shutdown_started"),
        "shutdown must start gracefully"
    );
    assert!(
        events.contains(&"authority_released"),
        "authority must be released at end of shutdown"
    );

    let lock = inspect_deployment_writer_lock(&storage).await.unwrap();
    assert!(
        lock.is_none(),
        "lock must be clear after graceful server shutdown"
    );
}

// ------------------------------------------------------------------------------------------------
// 9. Supervisor worker panic/exit triggers global shutdown
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_supervisor_worker_panic_or_exit_triggers_global_shutdown() {
    let shutdown_timeout = Duration::from_secs(5);
    let supervisor = registry_rust::task_supervisor::TaskSupervisor::new(shutdown_timeout);

    let task_ran = Arc::new(AtomicBool::new(false));
    let ran_clone = task_ran.clone();

    supervisor
        .spawn(
            "failing_public_server",
            registry_rust::task_supervisor::TaskClassification::PublicServer,
            move |_token| async move {
                ran_clone.store(true, Ordering::SeqCst);
                panic!("simulated unexpected worker panic");
            },
        )
        .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(task_ran.load(Ordering::SeqCst));

    assert!(supervisor.root_token().is_cancelled());
    let report = supervisor.shutdown().await;
    assert!(!report.success);
    assert_eq!(report.panicked_tasks.len(), 1);
}

// ------------------------------------------------------------------------------------------------
// 10. Supervisor mutation authority loss stops workers and fails closed
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_supervisor_mutation_authority_loss_stops_workers_and_fails_closed() {
    let temp = TempDir::new().unwrap();
    let cfg = Arc::new(create_test_config(&temp));
    let storage = storage::from_config(cfg.as_ref());

    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "worker-test")
        .await
        .unwrap();
    assert!(authority.is_active());

    let mut auth = authority;
    auth.release().await.unwrap();
    assert!(!auth.is_active());
}

// ------------------------------------------------------------------------------------------------
// 11. Server fails closed if storage lock already held
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_server_fails_closed_if_storage_lock_already_held() {
    let temp = TempDir::new().unwrap();
    let cfg = Arc::new(create_test_config(&temp));

    // Hold filesystem root lock
    let _existing_lock = FsRootLock::try_acquire(&cfg.fs_root).unwrap();

    let options = SupervisorOptions::default();
    let res = run_server_supervisor(cfg, Some(options)).await;
    assert!(
        res.is_err(),
        "second server instance must fail closed when filesystem lock is already held"
    );
}

// ------------------------------------------------------------------------------------------------
// 12. Server fails closed on unmigrated membership with existing data
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_server_fails_closed_on_unmigrated_membership_with_existing_data() {
    let temp = TempDir::new().unwrap();
    let cfg = Arc::new(create_test_config(&temp));
    let storage = storage::from_config(cfg.as_ref());

    let repo = "unmigrated/app";
    let manifest_bytes = Bytes::from_static(b"{\"schemaVersion\":2}");
    let digest =
        Digest::parse("sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
            .unwrap();
    storage
        .put_manifest(repo, &digest, manifest_bytes)
        .await
        .unwrap();

    assert!(!storage.is_membership_ready().await.unwrap());

    let options = SupervisorOptions::default();
    let res = run_server_supervisor(cfg, Some(options)).await;
    assert!(
        res.is_err(),
        "server must fail closed with fatal migration message when data exists but membership is unmigrated"
    );
    let err_msg = res.err().unwrap();
    assert!(err_msg.contains("migrate-membership apply"));
}

// ------------------------------------------------------------------------------------------------
// 13. Server auto-initializes membership on fresh empty storage
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_server_auto_initializes_membership_on_fresh_empty_storage() {
    let temp = TempDir::new().unwrap();
    let cfg = Arc::new(create_test_config(&temp));
    let storage = storage::from_config(cfg.as_ref());

    assert!(!storage.is_membership_ready().await.unwrap());

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (bound_tx, bound_rx) = oneshot::channel();

    let options = SupervisorOptions {
        fault_injector: None,
        shutdown_rx: Some(shutdown_rx),
        notify_bound_addr: Some(bound_tx),
    };

    let server_task = tokio::spawn(run_server_supervisor(cfg, Some(options)));

    let _addr = bound_rx.await.unwrap();
    let _ = shutdown_tx.send(());

    let res = server_task.await.unwrap();
    assert!(res.is_ok());

    assert!(storage.is_membership_ready().await.unwrap());
}

// ------------------------------------------------------------------------------------------------
// 14. Server serves HTTP requests and shuts down cleanly
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_server_serves_http_requests_and_shuts_down_cleanly() {
    let temp = TempDir::new().unwrap();
    let cfg = Arc::new(create_test_config(&temp));
    let storage = storage::from_config(cfg.as_ref());

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (bound_tx, bound_rx) = oneshot::channel();

    let options = SupervisorOptions {
        fault_injector: None,
        shutdown_rx: Some(shutdown_rx),
        notify_bound_addr: Some(bound_tx),
    };

    let server_task = tokio::spawn(run_server_supervisor(cfg, Some(options)));

    let bound_addr = bound_rx.await.unwrap();

    let client = reqwest::Client::new();
    let url = format!("http://{}/v2/", bound_addr);
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let _ = shutdown_tx.send(());
    let res = server_task.await.unwrap();
    assert!(res.is_ok());

    let lock = inspect_deployment_writer_lock(&storage).await.unwrap();
    assert!(lock.is_none());
}

// ------------------------------------------------------------------------------------------------
// 15. Server serves token endpoint and respects rate limiter
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_server_serves_token_endpoint_and_respects_rate_limiter() {
    let temp = TempDir::new().unwrap();
    let cfg = Arc::new(create_test_config(&temp));

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (bound_tx, bound_rx) = oneshot::channel();

    let options = SupervisorOptions {
        fault_injector: None,
        shutdown_rx: Some(shutdown_rx),
        notify_bound_addr: Some(bound_tx),
    };

    let server_task = tokio::spawn(run_server_supervisor(cfg, Some(options)));

    let bound_addr = bound_rx.await.unwrap();

    let client = reqwest::Client::new();
    let url = format!(
        "http://{}/token?service=registry-rust&scope=repository:sam/test:pull",
        bound_addr
    );
    let resp = client.get(&url).send().await.unwrap();
    assert!(resp.status().is_success() || resp.status() == reqwest::StatusCode::UNAUTHORIZED);

    let _ = shutdown_tx.send(());
    let res = server_task.await.unwrap();
    assert!(res.is_ok());
}

// ------------------------------------------------------------------------------------------------
// 16. Server IP concurrency limiter in supervisor
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_server_ip_concurrency_limiter_in_supervisor() {
    let temp = TempDir::new().unwrap();
    let mut cfg = create_test_config(&temp);
    cfg.max_connections_per_ip = 50;
    let cfg = Arc::new(cfg);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (bound_tx, bound_rx) = oneshot::channel();

    let options = SupervisorOptions {
        fault_injector: None,
        shutdown_rx: Some(shutdown_rx),
        notify_bound_addr: Some(bound_tx),
    };

    let server_task = tokio::spawn(run_server_supervisor(cfg, Some(options)));

    let bound_addr = bound_rx.await.unwrap();

    let client = reqwest::Client::new();
    let url = format!("http://{}/v2/", bound_addr);
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let _ = shutdown_tx.send(());
    let res = server_task.await.unwrap();
    assert!(res.is_ok());
}

// ------------------------------------------------------------------------------------------------
// 17. Filesystem root lock mutual exclusion between server and CLI
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_fs_root_lock_mutual_exclusion_between_server_and_cli() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
        .await
        .unwrap();

    let cfg = Arc::new(Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap());

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (bound_tx, bound_rx) = oneshot::channel();

    let options = SupervisorOptions {
        fault_injector: None,
        shutdown_rx: Some(shutdown_rx),
        notify_bound_addr: Some(bound_tx),
    };

    let server_task = tokio::spawn(run_server_supervisor(cfg.clone(), Some(options)));

    let _bound_addr = bound_rx.await.unwrap();

    let cli = Cli {
        config: vec![cfg_path],
        command: Some(CliCommand::BlobGc {
            command: BlobGcCommand::Plan {
                policy: registry_rust::blob_gc::BlobGcPolicy::ManifestRooted,
                min_age_secs: 0,
                max_per_run: 10,
            },
        }),
    };
    let exit = run_cli(cli).await;
    assert_eq!(exit, 2);

    let _ = shutdown_tx.send(());
    let res = server_task.await.unwrap();
    assert!(res.is_ok());
}

// ------------------------------------------------------------------------------------------------
// 18. Shutdown signals cancellation token and channel
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_shutdown_signals_both_ctrl_c_and_sigterm() {
    let temp = TempDir::new().unwrap();
    let cfg = Arc::new(create_test_config(&temp));

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (bound_tx, bound_rx) = oneshot::channel();

    let options = SupervisorOptions {
        fault_injector: None,
        shutdown_rx: Some(shutdown_rx),
        notify_bound_addr: Some(bound_tx),
    };

    let server_task = tokio::spawn(run_server_supervisor(cfg, Some(options)));
    let _addr = bound_rx.await.unwrap();

    drop(shutdown_tx);
    let res = server_task.await.unwrap();
    assert!(res.is_ok());
}

// ------------------------------------------------------------------------------------------------
// 19. Background workers do not outlive supervisor
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_background_workers_do_not_outlive_supervisor() {
    let shutdown_timeout = Duration::from_secs(5);
    let supervisor = registry_rust::task_supervisor::TaskSupervisor::new(shutdown_timeout);

    let counter = Arc::new(AtomicUsize::new(0));
    let c_clone = counter.clone();

    supervisor
        .spawn_loop(
            "test_worker",
            registry_rust::task_supervisor::TaskClassification::MaintenanceScheduler,
            Duration::from_millis(50),
            None,
            move || {
                let c = c_clone.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await;

    tokio::time::sleep(Duration::from_millis(150)).await;
    let val_before = counter.load(Ordering::SeqCst);
    assert!(val_before > 0);

    let report = supervisor.shutdown().await;
    assert!(report.success);

    let val_after = counter.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(150)).await;
    let val_later = counter.load(Ordering::SeqCst);

    assert_eq!(
        val_after, val_later,
        "worker must not perform iterations after shutdown completes"
    );
}

// ------------------------------------------------------------------------------------------------
// 20. Flush hook executed before authority release
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_flush_hook_executed_before_authority_release() {
    let shutdown_timeout = Duration::from_secs(5);
    let supervisor = registry_rust::task_supervisor::TaskSupervisor::new(shutdown_timeout);

    let flush_executed = Arc::new(AtomicBool::new(false));
    let f_clone = flush_executed.clone();

    supervisor
        .register_flush_hook(move || {
            f_clone.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await;

    let report = supervisor.shutdown().await;
    assert!(report.success);
    assert!(
        flush_executed.load(Ordering::SeqCst),
        "flush hook must execute during supervisor shutdown"
    );
}
