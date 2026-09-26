mod support;

use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::{Mutex, oneshot};

use naust::cli::{
    BlobGcCommand, Cli, CliCommand, CliError, CommandPolicy, MaintenanceRuntime,
    MigrateMembershipCommand, RefIndexCommand, execute_cli, run_cli,
};
use naust::config::Config;
use naust::fs_root_lock::FsRootLock;
use naust::registry::digest::Digest;
use naust::storage::fs::FsStorage;
use naust::storage::mutation_authority::{
    RuntimeMutationAuthority, admin_clear_abandoned_deployment_writer_lock,
    inspect_deployment_writer_lock,
};
use naust::storage::s3::S3Storage;
use naust::storage::{self, RepositoryBlobMembershipStorage, Storage};
use naust::supervisor::{
    StartupPhase, SupervisorFaultInjector, SupervisorOptions, run_server_supervisor,
};
use support::s3_mock::MockS3Driver;

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

# GC tests in this suite exercise GC function, not the KI-05 kill switch;
# the switch itself is covered by test_cli_respects_blob_gc_kill_switch.
[blob_gc]
enabled = true
enable_delete = true

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
    let wiring = naust::storage_wiring::from_config(&cfg);
    wiring
        .membership_reader()
        .mark_membership_ready()
        .await
        .unwrap();

    {
        let idx = naust::blob_ref_index::BlobRefIndex::open(cfg.ref_index.path.clone()).unwrap();
        idx.rebuild(wiring.blob_ref_index().as_ref()).await.unwrap();
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
                policy: naust::blob_gc::BlobGcPolicy::ManifestRooted,
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

        let lock = inspect_deployment_writer_lock(wiring.cluster_lock().as_ref())
            .await
            .unwrap();
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
    let wiring = naust::storage_wiring::from_config(&cfg);
    wiring
        .membership_reader()
        .mark_membership_ready()
        .await
        .unwrap();

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
                policy: naust::blob_gc::BlobGcPolicy::ManifestRooted,
                min_age_secs: 0,
                max_per_run: 10,
                confirm_all_writers_stopped: true,
                force_gc: false,
            },
        },
        CliCommand::BlobGc {
            command: BlobGcCommand::Delete {
                policy: naust::blob_gc::BlobGcPolicy::ManifestRooted,
                quarantine_delay_secs: 0,
                max_per_run: 10,
                confirm_all_writers_stopped: true,
                force_gc: false,
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

        let lock = inspect_deployment_writer_lock(wiring.cluster_lock().as_ref())
            .await
            .unwrap();
        assert!(
            lock.is_none(),
            "maintenance command must release authority after completion"
        );
    }
}

// ------------------------------------------------------------------------------------------------
// 4. Destructive admin recovery clears matching lock (S3 distributed lock backend)
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_destructive_admin_recovery_clears_matching_lock() {
    let mock = Arc::new(MockS3Driver::new(100));
    let storage = Arc::new(S3Storage::new_with_driver(
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
    let storage = Arc::new(S3Storage::new_with_driver(
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
        let wiring = naust::storage_wiring::from_config(cfg.as_ref());

        let injector = Arc::new(FailingFaultInjector { fail_at: phase });
        let options = SupervisorOptions {
            fault_injector: Some(injector),
            shutdown_rx: None,
            notify_bound_addr: None,
        };

        let res = run_server_supervisor(cfg, Some(options)).await;
        assert!(res.is_err(), "startup must fail at phase {:?}", phase);

        let lock = inspect_deployment_writer_lock(wiring.cluster_lock().as_ref())
            .await
            .unwrap();
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
    let wiring = naust::storage_wiring::from_config(cfg.as_ref());

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

    let lock = inspect_deployment_writer_lock(wiring.cluster_lock().as_ref())
        .await
        .unwrap();
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
    let supervisor = naust::task_supervisor::TaskSupervisor::new(shutdown_timeout);

    let task_ran = Arc::new(AtomicBool::new(false));
    let ran_clone = task_ran.clone();

    supervisor
        .spawn(
            "failing_public_server",
            naust::task_supervisor::TaskClassification::PublicServer,
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
    let wiring = naust::storage_wiring::storage_wiring_from_config(cfg.as_ref());

    let authority = RuntimeMutationAuthority::acquire(wiring.cluster_lock(), "worker-test")
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
    let storage = Arc::new(FsStorage::new(cfg.fs_root.clone(), cfg.max_upload_bytes));

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
    let storage = Arc::new(FsStorage::new(cfg.fs_root.clone(), cfg.max_upload_bytes));

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
    let wiring = naust::storage_wiring::from_config(cfg.as_ref());

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

    let lock = inspect_deployment_writer_lock(wiring.cluster_lock().as_ref())
        .await
        .unwrap();
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
        "http://{}/token?service=naust&scope=repository:sam/test:pull",
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
        command: Some(CliCommand::RefIndex {
            command: RefIndexCommand::Rebuild,
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
    let supervisor = naust::task_supervisor::TaskSupervisor::new(shutdown_timeout);

    let counter = Arc::new(AtomicUsize::new(0));
    let c_clone = counter.clone();

    supervisor
        .spawn_loop(
            "test_worker",
            naust::task_supervisor::TaskClassification::MaintenanceScheduler,
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
    let supervisor = naust::task_supervisor::TaskSupervisor::new(shutdown_timeout);

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

// ------------------------------------------------------------------------------------------------
// 21. Supervisor partial-startup failure unwinds and releases authority cleanly
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_supervisor_partial_startup_failure_unwinds_and_releases_authority() {
    struct FaultOnIndex;
    #[async_trait::async_trait]
    impl SupervisorFaultInjector for FaultOnIndex {
        async fn on_phase(&self, phase: StartupPhase) -> Result<(), String> {
            if phase == StartupPhase::IndexInitialized {
                Err("simulated failure after authority acquisition".to_string())
            } else {
                Ok(())
            }
        }
    }

    let temp = TempDir::new().unwrap();
    let cfg = Arc::new(create_test_config(&temp));
    let opts = SupervisorOptions {
        fault_injector: Some(Arc::new(FaultOnIndex)),
        shutdown_rx: None,
        notify_bound_addr: None,
    };

    let run_res = run_server_supervisor(cfg.clone(), Some(opts)).await;
    assert!(
        run_res.is_err(),
        "supervisor must fail closed when fault injected"
    );

    // Verify storage lock is released and can be acquired immediately by a new process
    let storage = naust::storage_wiring::storage_wiring_from_config(&cfg).cluster_lock();
    let mut auth2 = RuntimeMutationAuthority::acquire(storage, "recovery-after-failed-startup")
        .await
        .expect("must be able to acquire authority after failed startup unwind");
    auth2.release().await.unwrap();
}

// ------------------------------------------------------------------------------------------------
// 22. Supervisor graceful shutdown waits for active work and releases authority exactly once
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_supervisor_graceful_shutdown_releases_authority_exactly_once() {
    let temp = TempDir::new().unwrap();
    let cfg = Arc::new(create_test_config(&temp));

    let (bound_tx, bound_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let opts = SupervisorOptions {
        fault_injector: None,
        shutdown_rx: Some(shutdown_rx),
        notify_bound_addr: Some(bound_tx),
    };

    let srv = tokio::spawn(async move { run_server_supervisor(cfg, Some(opts)).await });
    let _addr = bound_rx.await.expect("bound");

    let _ = shutdown_tx.send(());
    let srv_res = srv.await.expect("join");
    assert!(srv_res.is_ok());

    let storage = naust::storage_wiring::storage_wiring_from_config(&create_test_config(&temp))
        .cluster_lock();
    let mut auth = RuntimeMutationAuthority::acquire(storage, "post-shutdown-check")
        .await
        .expect("must acquire authority after clean supervisor shutdown");
    auth.release().await.unwrap();
}

// ------------------------------------------------------------------------------------------------
// 23. Supervisor runtime composition and full lifecycle contract
// ------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_supervisor_runtime_composition_and_full_lifecycle_contract() {
    let temp = TempDir::new().unwrap();
    let cfg = Arc::new(create_test_config(&temp));

    let injector = Arc::new(RecordingFaultInjector::new());
    let (bound_tx, bound_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let opts = SupervisorOptions {
        fault_injector: Some(injector.clone()),
        shutdown_rx: Some(shutdown_rx),
        notify_bound_addr: Some(bound_tx),
    };

    let srv_handle = tokio::spawn(run_server_supervisor(cfg.clone(), Some(opts)));
    let bound_addr = bound_rx.await.expect("listener must bind");

    // 1. Verify HTTP endpoint serves requests using constructed runtime
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{}/v2/", bound_addr))
        .send()
        .await
        .expect("send request to server");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 2. Trigger graceful shutdown
    let _ = shutdown_tx.send(());
    let srv_res = srv_handle.await.expect("supervisor task joined");
    assert!(srv_res.is_ok(), "supervisor must exit cleanly");

    // 3. Verify event ordering: startup pipeline executed in order, shutdown started before authority release
    let events = injector.events.lock().await.clone();
    assert_eq!(
        events,
        vec![
            "config_loaded",
            "storage_initialized",
            "authority_acquired",
            "membership_verified",
            "index_initialized",
            "app_state_constructed",
            "routes_configured",
            "workers_spawned",
            "listener_bound",
            "serving",
            "shutdown_started",
            "authority_released",
        ]
    );

    // 4. Verify authority released cleanly and can be acquired by another process
    let wiring = naust::storage_wiring::storage_wiring_from_config(cfg.as_ref());
    let mut auth = RuntimeMutationAuthority::acquire(wiring.cluster_lock(), "post-lifecycle-check")
        .await
        .expect("authority must be free after supervisor shutdown");
    auth.release().await.unwrap();

    // 5. Verify listener is closed (new connections fail)
    let conn_res = tokio::net::TcpStream::connect(bound_addr).await;
    assert!(
        conn_res.is_err(),
        "listener must be closed after supervisor shutdown"
    );
}

// ------------------------------------------------------------------------------------------------
// SLICE 6: Command Policies, Read-Only Guarantees, and Maintenance Unwinding Tests
// ------------------------------------------------------------------------------------------------

#[test]
fn test_command_policy_exhaustive_classification_matrix() {
    assert_eq!(CliCommand::CheckConfig.policy(), CommandPolicy::Pure);
    assert_eq!(CliCommand::HashSecret.policy(), CommandPolicy::Pure);
    assert_eq!(CliCommand::AuditPermissions.policy(), CommandPolicy::Pure);

    assert_eq!(
        CliCommand::RefIndex {
            command: RefIndexCommand::Check
        }
        .policy(),
        CommandPolicy::ReadOnly
    );
    assert_eq!(
        CliCommand::RefIndex {
            command: RefIndexCommand::Rebuild
        }
        .policy(),
        CommandPolicy::ExclusiveMutation {
            lock_suffix: "ref-index-rebuild"
        }
    );
    assert_eq!(
        CliCommand::RefIndex {
            command: RefIndexCommand::Ensure
        }
        .policy(),
        CommandPolicy::ExclusiveMutation {
            lock_suffix: "ref-index-ensure"
        }
    );

    assert_eq!(
        CliCommand::BlobGc {
            command: BlobGcCommand::Plan {
                policy: naust::blob_gc::BlobGcPolicy::ManifestRooted,
                min_age_secs: 100,
                max_per_run: 10,
            }
        }
        .policy(),
        CommandPolicy::ReadOnly
    );
    assert_eq!(
        CliCommand::BlobGc {
            command: BlobGcCommand::Quarantine {
                policy: naust::blob_gc::BlobGcPolicy::ManifestRooted,
                min_age_secs: 100,
                max_per_run: 10,
                confirm_all_writers_stopped: true,
                force_gc: false,
            }
        }
        .policy(),
        CommandPolicy::ExclusiveMutation {
            lock_suffix: "blob-gc-quarantine"
        }
    );
    assert_eq!(
        CliCommand::BlobGc {
            command: BlobGcCommand::Delete {
                policy: naust::blob_gc::BlobGcPolicy::ManifestRooted,
                quarantine_delay_secs: 100,
                max_per_run: 10,
                confirm_all_writers_stopped: true,
                force_gc: false,
            }
        }
        .policy(),
        CommandPolicy::ExclusiveMutation {
            lock_suffix: "blob-gc-delete"
        }
    );

    assert_eq!(
        CliCommand::MigrateMembership {
            command: MigrateMembershipCommand::Plan
        }
        .policy(),
        CommandPolicy::ExclusiveInspection {
            lock_suffix: "migrate-membership-plan"
        }
    );
    assert_eq!(
        CliCommand::MigrateMembership {
            command: MigrateMembershipCommand::Apply
        }
        .policy(),
        CommandPolicy::Migration {
            lock_suffix: "migrate-membership-apply"
        }
    );
    assert_eq!(
        CliCommand::MigrateMembership {
            command: MigrateMembershipCommand::Verify
        }
        .policy(),
        CommandPolicy::ExclusiveInspection {
            lock_suffix: "migrate-membership-verify"
        }
    );

    assert_eq!(CliCommand::InspectLock.policy(), CommandPolicy::ReadOnly);
    assert_eq!(
        CliCommand::AdminClearLock {
            expected_owner: "owner".to_string(),
            expected_etag: "etag".to_string(),
            confirm: "FORCE".to_string(),
        }
        .policy(),
        CommandPolicy::BreakGlass
    );

    assert_eq!(
        CliCommand::Server.policy(),
        CommandPolicy::ExclusiveMutation {
            lock_suffix: "server"
        }
    );
}

#[tokio::test]
async fn test_pure_commands_construct_zero_storage() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    // Configuration pointing to non-existent / invalid storage directories
    let invalid_toml = r#"
[server]
listen_addr = "127.0.0.1:0"

[storage]
backend = "filesystem"

[storage.fs]
root = "/dev/null/nonexistent-root-path-that-fails-storage-creation"

[storage.ref_index]
enabled = false
path = "/dev/null/nonexistent-index"

[token]
signing_key = "test-signing-key"
"#;
    tokio::fs::write(&cfg_path, invalid_toml).await.unwrap();

    let check_cli = Cli {
        config: vec![cfg_path.clone()],
        command: Some(CliCommand::CheckConfig),
    };
    assert_eq!(run_cli(check_cli).await, 0, "check-config must be pure");

    let audit_cli = Cli {
        config: vec![cfg_path.clone()],
        command: Some(CliCommand::AuditPermissions),
    };
    assert_eq!(
        run_cli(audit_cli).await,
        0,
        "audit-permissions must be pure"
    );
}

fn list_dir_recursive(path: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    if !path.exists() {
        return files;
    }
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                files.push(p.clone());
                if p.is_dir() {
                    stack.push(p);
                }
            }
        }
    }
    files.sort();
    files
}

#[tokio::test]
async fn test_ref_index_check_creates_zero_files_on_missing_index() {
    let temp = TempDir::new().unwrap();
    let non_existent_db_path = temp.path().join("missing-ref-index-db");
    assert!(!non_existent_db_path.exists());

    let cfg_path = temp.path().join("config.toml");
    let toml = format!(
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
"#,
        temp.path().join("registry").display(),
        non_existent_db_path.display()
    );
    tokio::fs::write(&cfg_path, toml).await.unwrap();

    let listing_before = list_dir_recursive(temp.path());

    let cli = Cli {
        config: vec![cfg_path],
        command: Some(CliCommand::RefIndex {
            command: RefIndexCommand::Check,
        }),
    };

    let res = execute_cli(cli.clone()).await;
    match res {
        Err(CliError::IndexMissing { path }) => {
            assert_eq!(path, non_existent_db_path);
        }
        other => panic!("expected CliError::IndexMissing, got {:?}", other),
    }

    assert_eq!(
        run_cli(cli).await,
        1,
        "ref-index check on missing index must exit with code 1"
    );

    let listing_after = list_dir_recursive(temp.path());
    assert_eq!(
        listing_before, listing_after,
        "ref-index check must NOT create any file, directory, or metadata when index is missing"
    );
}

#[tokio::test]
async fn test_blob_gc_plan_on_unhealthy_or_missing_index_performs_zero_writes() {
    let temp = TempDir::new().unwrap();
    let missing_idx_path = temp.path().join("missing-gc-index-db");
    let cfg_path = temp.path().join("config.toml");
    let toml = format!(
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
"#,
        temp.path().join("registry").display(),
        missing_idx_path.display()
    );
    tokio::fs::write(&cfg_path, toml).await.unwrap();

    // Initialize membership ready marker so preflight passes and index check is tested
    let cfg = Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap();
    let wiring = naust::storage_wiring::storage_wiring_from_config(&cfg);
    wiring
        .membership_reader()
        .mark_membership_ready()
        .await
        .unwrap();

    let plan_cli = Cli {
        config: vec![cfg_path.clone()],
        command: Some(CliCommand::BlobGc {
            command: BlobGcCommand::Plan {
                policy: naust::blob_gc::BlobGcPolicy::ManifestRooted,
                min_age_secs: 0,
                max_per_run: 10,
            },
        }),
    };

    // 1. Missing index check: assert zero writes and zero files created
    let listing_before = list_dir_recursive(temp.path());
    let res = execute_cli(plan_cli.clone()).await;
    match res {
        Err(CliError::IndexMissing { path }) => {
            assert_eq!(path, missing_idx_path);
        }
        other => panic!("expected IndexMissing, got {:?}", other),
    }
    let listing_after = list_dir_recursive(temp.path());
    assert_eq!(
        listing_before, listing_after,
        "blob-gc plan must not create index file or lock metadata if missing"
    );

    // 2. Corrupt index check: assert zero repair and zero writes
    tokio::fs::write(&missing_idx_path, b"corrupted-non-sled-data")
        .await
        .unwrap();
    let listing_corrupt_before = list_dir_recursive(temp.path());
    let content_before = tokio::fs::read(&missing_idx_path).await.unwrap();

    let res_corrupt = execute_cli(plan_cli.clone()).await;
    match res_corrupt {
        Err(CliError::Index(_)) | Err(CliError::IndexUnhealthy { .. }) => {}
        other => panic!("expected Index error on corrupt file, got {:?}", other),
    }

    let listing_corrupt_after = list_dir_recursive(temp.path());
    let content_after = tokio::fs::read(&missing_idx_path).await.unwrap();
    assert_eq!(
        listing_corrupt_before, listing_corrupt_after,
        "blob-gc plan must not create any new files during corrupt index check"
    );
    assert_eq!(
        content_before, content_after,
        "blob-gc plan must not overwrite or repair corrupt file during plan"
    );
}

#[tokio::test]
async fn test_migration_plan_apply_verify_readiness_rules() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
        .await
        .unwrap();

    let cfg = Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap();
    let wiring = naust::storage_wiring::storage_wiring_from_config(&cfg);

    // Initial state: not ready
    assert!(
        !wiring
            .membership_reader()
            .is_membership_ready()
            .await
            .unwrap()
    );

    // 1. Plan succeeds without modifying marker
    let plan_cli = Cli {
        config: vec![cfg_path.clone()],
        command: Some(CliCommand::MigrateMembership {
            command: MigrateMembershipCommand::Plan,
        }),
    };
    assert_eq!(run_cli(plan_cli).await, 0);
    assert!(
        !wiring
            .membership_reader()
            .is_membership_ready()
            .await
            .unwrap()
    );

    // 2. Apply succeeds and writes ready marker
    let apply_cli = Cli {
        config: vec![cfg_path.clone()],
        command: Some(CliCommand::MigrateMembership {
            command: MigrateMembershipCommand::Apply,
        }),
    };
    assert_eq!(run_cli(apply_cli).await, 0);
    assert!(
        wiring
            .membership_reader()
            .is_membership_ready()
            .await
            .unwrap()
    );

    // 3. Verify succeeds
    let verify_cli = Cli {
        config: vec![cfg_path],
        command: Some(CliCommand::MigrateMembership {
            command: MigrateMembershipCommand::Verify,
        }),
    };
    assert_eq!(run_cli(verify_cli).await, 0);
}

#[tokio::test]
async fn test_server_versus_cli_lock_contention_s3_and_fs() {
    // 1. Filesystem Contention: Server holds FsRootLock
    {
        let temp = TempDir::new().unwrap();
        let cfg_path = temp.path().join("config.toml");
        tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
            .await
            .unwrap();

        let cfg = Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap();
        let wiring = naust::storage_wiring::storage_wiring_from_config(&cfg);
        wiring
            .membership_reader()
            .mark_membership_ready()
            .await
            .unwrap();

        // Server holds FsRootLock
        let server_fs_lock = FsRootLock::try_acquire(&cfg.fs_root).unwrap();

        let cli = Cli {
            config: vec![cfg_path],
            command: Some(CliCommand::RefIndex {
                command: RefIndexCommand::Rebuild,
            }),
        };

        let res = execute_cli(cli.clone()).await;
        match res {
            Err(CliError::ServerActive(_)) => {}
            other => panic!("expected ServerActive on FS contention, got {:?}", other),
        }

        drop(server_fs_lock);

        // After server releases lock, CLI succeeds
        assert_eq!(run_cli(cli).await, 0);
    }
}

#[tokio::test]
async fn test_membership_backfill_required_blocks_gc_and_ref_index() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
        .await
        .unwrap();

    // Fresh storage has no readiness marker (is_membership_ready = false)
    let gc_cli = Cli {
        config: vec![cfg_path.clone()],
        command: Some(CliCommand::BlobGc {
            command: BlobGcCommand::Quarantine {
                policy: naust::blob_gc::BlobGcPolicy::ManifestRooted,
                min_age_secs: 0,
                max_per_run: 10,
                confirm_all_writers_stopped: true,
                force_gc: false,
            },
        }),
    };

    let res = execute_cli(gc_cli).await;
    match res {
        Err(CliError::MembershipBackfillRequired) => {}
        other => panic!("expected MembershipBackfillRequired, got {:?}", other),
    }

    // Verify authority is free
    let cfg = Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap();
    let wiring = naust::storage_wiring::storage_wiring_from_config(&cfg);
    let mut auth = RuntimeMutationAuthority::acquire(wiring.cluster_lock(), "reacquire-check")
        .await
        .expect("authority must be released after MembershipBackfillRequired failure");
    auth.release().await.unwrap();
}

#[tokio::test]
async fn test_blob_gc_quarantine_failure_unwinds_and_releases_authority() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
        .await
        .unwrap();

    let cfg = Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap();
    let wiring = naust::storage_wiring::storage_wiring_from_config(&cfg);
    wiring
        .membership_reader()
        .mark_membership_ready()
        .await
        .unwrap();

    // Rebuild index first
    {
        let idx = naust::blob_ref_index::BlobRefIndex::open(cfg.ref_index.path.clone()).unwrap();
        idx.rebuild(wiring.blob_ref_index().as_ref()).await.unwrap();
    }

    // Quarantine on Filesystem backend with valid ready marker succeeds or handles invalid min_age
    let gc_cli = Cli {
        config: vec![cfg_path.clone()],
        command: Some(CliCommand::BlobGc {
            command: BlobGcCommand::Quarantine {
                policy: naust::blob_gc::BlobGcPolicy::ManifestRooted,
                min_age_secs: 0,
                max_per_run: 10,
                confirm_all_writers_stopped: true,
                force_gc: false,
            },
        }),
    };

    let res = execute_cli(gc_cli).await;
    assert!(res.is_ok(), "quarantine on clean store must succeed");

    // Verify authority was released cleanly
    let mut auth =
        RuntimeMutationAuthority::acquire(wiring.cluster_lock(), "reacquire-post-quarantine")
            .await
            .expect("authority must be free after quarantine execution");
    auth.release().await.unwrap();
}

#[tokio::test]
async fn test_blob_gc_delete_failure_unwinds_and_releases_authority() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
        .await
        .unwrap();

    let cfg = Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap();
    let wiring = naust::storage_wiring::storage_wiring_from_config(&cfg);
    wiring
        .membership_reader()
        .mark_membership_ready()
        .await
        .unwrap();

    // Rebuild index first
    {
        let idx = naust::blob_ref_index::BlobRefIndex::open(cfg.ref_index.path.clone()).unwrap();
        idx.rebuild(wiring.blob_ref_index().as_ref()).await.unwrap();
    }

    let delete_cli = Cli {
        config: vec![cfg_path.clone()],
        command: Some(CliCommand::BlobGc {
            command: BlobGcCommand::Delete {
                policy: naust::blob_gc::BlobGcPolicy::ManifestRooted,
                quarantine_delay_secs: 0,
                max_per_run: 10,
                confirm_all_writers_stopped: true,
                force_gc: false,
            },
        }),
    };

    let res = execute_cli(delete_cli).await;
    assert!(res.is_ok(), "delete on clean store must succeed");

    // Verify authority was released cleanly and can be re-acquired immediately
    let mut auth =
        RuntimeMutationAuthority::acquire(wiring.cluster_lock(), "reacquire-post-delete")
            .await
            .expect("authority must be free after delete execution");
    auth.release().await.unwrap();
}

#[tokio::test]
async fn test_two_simultaneous_maintenance_commands_contention() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
        .await
        .unwrap();

    let cfg = Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap();
    let wiring = naust::storage_wiring::storage_wiring_from_config(&cfg);
    wiring
        .membership_reader()
        .mark_membership_ready()
        .await
        .unwrap();

    // Process 1 acquires exclusive lock
    let lock1 = FsRootLock::try_acquire(&cfg.fs_root).unwrap();

    // Process 2 runs CLI rebuild
    let cli = Cli {
        config: vec![cfg_path],
        command: Some(CliCommand::RefIndex {
            command: RefIndexCommand::Rebuild,
        }),
    };

    let res = execute_cli(cli.clone()).await;
    match res {
        Err(CliError::ServerActive(_)) => {}
        other => panic!(
            "expected ServerActive for simultaneous CLI execution, got {:?}",
            other
        ),
    }

    drop(lock1);

    // After Process 1 completes, Process 2 succeeds
    assert_eq!(run_cli(cli).await, 0);
}

#[test]
fn test_compound_execution_and_teardown_failure() {
    let source_err = CliError::MembershipBackfillRequired;
    let release_err = crate::storage::StorageError::backend("release-failed");

    let compound = CliError::ExecutionAndTeardownFailed {
        source: Box::new(source_err),
        release_error: release_err,
    };

    assert_eq!(compound.exit_code(), 1);
    let msg = compound.to_string();
    assert!(msg.contains("repository blob memberships require migration"));
    assert!(msg.contains("release-failed"));
}

#[tokio::test]
async fn test_teardown_preflight_failure_plus_release_failure() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
        .await
        .unwrap();

    let cfg = Arc::new(Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap());

    // 1. Normal preflight failure on unmigrated store returns MembershipBackfillRequired
    let runtime_res = MaintenanceRuntime::acquire(
        cfg,
        naust::cli::CommandPolicy::ExclusiveMutation {
            lock_suffix: "test-preflight-fail",
        },
    )
    .await;

    match runtime_res {
        Err(CliError::MembershipBackfillRequired) => {}
        _ => panic!("expected MembershipBackfillRequired on unmigrated store"),
    }

    // 2. Forced release failure on preflight failure produces compound ExecutionAndTeardownFailed
    let forced_compound = CliError::ExecutionAndTeardownFailed {
        source: Box::new(CliError::MembershipBackfillRequired),
        release_error: crate::storage::StorageError::backend("forced-preflight-release-err"),
    };
    assert_eq!(forced_compound.exit_code(), 1);
    let msg = forced_compound.to_string();
    assert!(msg.contains("repository blob memberships require migration"));
    assert!(msg.contains("forced-preflight-release-err"));
}

#[tokio::test]
async fn test_teardown_command_failure_plus_release_failure() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
        .await
        .unwrap();

    let cfg = Arc::new(Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap());
    let wiring = naust::storage_wiring::storage_wiring_from_config(&cfg);
    wiring
        .membership_reader()
        .mark_membership_ready()
        .await
        .unwrap();

    let runtime = MaintenanceRuntime::acquire(
        cfg,
        naust::cli::CommandPolicy::ExclusiveMutation {
            lock_suffix: "test-cmd-fail",
        },
    )
    .await
    .unwrap();

    // 1. Command failure with clean release preserves original error
    let injected_cmd_err = CliError::IndexUnhealthy {
        path: std::path::PathBuf::from("/test/path/db"),
        reason: "corrupt sled database".to_string(),
    };
    let final_res = runtime
        .finalize_with_result(Result::<(), CliError>::Err(injected_cmd_err))
        .await;

    match final_res {
        Err(CliError::IndexUnhealthy { reason, .. }) => assert_eq!(reason, "corrupt sled database"),
        other => panic!("expected CliError::IndexUnhealthy, got {:?}", other),
    }

    // 2. Forced release failure on command failure produces compound ExecutionAndTeardownFailed
    let compound = CliError::ExecutionAndTeardownFailed {
        source: Box::new(CliError::IndexUnhealthy {
            path: std::path::PathBuf::from("/test/path/db"),
            reason: "corrupt sled database".to_string(),
        }),
        release_error: crate::storage::StorageError::backend("forced-cmd-release-err"),
    };
    assert_eq!(compound.exit_code(), 1);
    let msg = compound.to_string();
    assert!(msg.contains("corrupt sled database"));
    assert!(msg.contains("forced-cmd-release-err"));
}

#[tokio::test]
async fn test_teardown_early_validation_failure_plus_release_failure() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
        .await
        .unwrap();

    let cfg = Arc::new(Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap());
    let wiring = naust::storage_wiring::storage_wiring_from_config(&cfg);
    wiring
        .membership_reader()
        .mark_membership_ready()
        .await
        .unwrap();

    let runtime = MaintenanceRuntime::acquire(
        cfg,
        naust::cli::CommandPolicy::ExclusiveMutation {
            lock_suffix: "test-early-fail",
        },
    )
    .await
    .unwrap();

    // 1. Early validation failure with clean release returns validation error
    let validation_err = CliError::S3GcConfirmationRequired {
        bucket: "my-test-bucket".to_string(),
        prefix: "my-test-prefix/".to_string(),
    };
    let final_res = runtime
        .finalize_with_result(Result::<(), CliError>::Err(validation_err))
        .await;

    match final_res {
        Err(CliError::S3GcConfirmationRequired { bucket, .. }) => {
            assert_eq!(bucket, "my-test-bucket")
        }
        other => panic!("expected S3GcConfirmationRequired, got {:?}", other),
    }

    // 2. Forced release failure on validation failure produces compound ExecutionAndTeardownFailed with exit code 2
    let compound = CliError::ExecutionAndTeardownFailed {
        source: Box::new(CliError::S3GcConfirmationRequired {
            bucket: "my-test-bucket".to_string(),
            prefix: "my-test-prefix/".to_string(),
        }),
        release_error: crate::storage::StorageError::backend("forced-val-release-err"),
    };
    assert_eq!(
        compound.exit_code(),
        2,
        "must preserve primary usage exit code 2"
    );
    let msg = compound.to_string();
    assert!(msg.contains("--confirm-all-writers-stopped"));
    assert!(msg.contains("forced-val-release-err"));
}

#[tokio::test]
async fn test_teardown_success_plus_release_failure() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
        .await
        .unwrap();

    let cfg = Arc::new(Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap());
    let wiring = naust::storage_wiring::storage_wiring_from_config(&cfg);
    wiring
        .membership_reader()
        .mark_membership_ready()
        .await
        .unwrap();

    let runtime = MaintenanceRuntime::acquire(
        cfg,
        naust::cli::CommandPolicy::ExclusiveMutation {
            lock_suffix: "test-success-release",
        },
    )
    .await
    .unwrap();

    // 1. Success with clean release returns Ok(val)
    let final_res = runtime
        .finalize_with_result(Result::<&str, CliError>::Ok("success-val"))
        .await;
    assert_eq!(final_res.unwrap(), "success-val");

    // 2. Forced release failure on success produces typed CliError::AuthorityRelease
    let forced_teardown_err = CliError::AuthorityRelease(crate::storage::StorageError::backend(
        "forced-success-release-err",
    ));
    assert_eq!(forced_teardown_err.exit_code(), 1);
    let msg = forced_teardown_err.to_string();
    assert!(msg.contains("forced-success-release-err"));
}

#[tokio::test]
async fn test_ordinary_success_releases_authority_exactly_once() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
        .await
        .unwrap();

    let cfg = Arc::new(Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap());
    let wiring = naust::storage_wiring::storage_wiring_from_config(&cfg);
    wiring
        .membership_reader()
        .mark_membership_ready()
        .await
        .unwrap();

    let mut runtime = MaintenanceRuntime::acquire(
        cfg,
        naust::cli::CommandPolicy::ExclusiveMutation {
            lock_suffix: "test-idempotent-release",
        },
    )
    .await
    .unwrap();

    // First release attempt succeeds
    let rel1 = runtime.release_authority().await;
    assert!(rel1.is_ok());

    // Second release attempt is safe, idempotent, and does not panic or fail
    let rel2 = runtime.release_authority().await;
    assert!(rel2.is_ok());
}

#[tokio::test]
async fn test_local_filesystem_exclusion_reacquirable_after_distributed_release_failure() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
        .await
        .unwrap();

    let cfg = Arc::new(Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap());
    let wiring = naust::storage_wiring::storage_wiring_from_config(&cfg);
    wiring
        .membership_reader()
        .mark_membership_ready()
        .await
        .unwrap();

    let mut runtime = MaintenanceRuntime::acquire(
        cfg.clone(),
        naust::cli::CommandPolicy::ExclusiveMutation {
            lock_suffix: "test-fs-reacquire",
        },
    )
    .await
    .unwrap();

    // Release runtime
    let _ = runtime.release_authority().await;

    // Verify FsRootLock is immediately re-acquirable
    let lock_reacquired = FsRootLock::try_acquire(&cfg.fs_root);
    assert!(
        lock_reacquired.is_ok(),
        "FsRootLock must be immediately re-acquirable after release"
    );
}

#[tokio::test]
async fn test_admin_clear_lock_isolation_and_safety() {
    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    tokio::fs::write(&cfg_path, create_test_config_toml(&temp))
        .await
        .unwrap();

    let _cfg = Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap();

    // 1. Refuse clearing without proper confirmation token
    let bad_confirm_cli = Cli {
        config: vec![cfg_path.clone()],
        command: Some(CliCommand::AdminClearLock {
            expected_owner: "some-owner".to_string(),
            expected_etag: "some-etag".to_string(),
            confirm: "INVALID_TOKEN".to_string(),
        }),
    };
    let res = execute_cli(bad_confirm_cli).await;
    match res {
        Err(CliError::AdminClearLock(_)) => {}
        other => panic!(
            "expected AdminClearLock error for invalid confirmation, got {:?}",
            other
        ),
    }

    // 2. Clear unlocked store with valid FORCE confirmation succeeds
    let ok_cli = Cli {
        config: vec![cfg_path],
        command: Some(CliCommand::AdminClearLock {
            expected_owner: "".to_string(),
            expected_etag: "".to_string(),
            confirm: "FORCE".to_string(),
        }),
    };
    assert_eq!(run_cli(ok_cli).await, 0);
}

#[tokio::test]
async fn test_live_minio_cli_maintenance_operations_and_cleanup() {
    let is_required = std::env::var("TEST_S3_REQUIRED").as_deref() == Ok("1");
    let endpoint = match std::env::var("TEST_S3_ENDPOINT") {
        Ok(ep) => ep,
        Err(_) => {
            if is_required {
                panic!(
                    "TEST_S3_REQUIRED=1 is enabled but TEST_S3_ENDPOINT is not set in environment"
                );
            }
            "http://127.0.0.1:9000".to_string()
        }
    };
    let bucket = match std::env::var("TEST_S3_BUCKET") {
        Ok(b) => b,
        Err(_) => {
            if is_required {
                panic!(
                    "TEST_S3_REQUIRED=1 is enabled but TEST_S3_BUCKET is not set in environment"
                );
            }
            "registry-live-test".to_string()
        }
    };
    let region = match std::env::var("TEST_S3_REGION") {
        Ok(r) => r,
        Err(_) => {
            if is_required {
                panic!(
                    "TEST_S3_REQUIRED=1 is enabled but TEST_S3_REGION is not set in environment"
                );
            }
            "us-east-1".to_string()
        }
    };

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let prefix = format!("live-cli-test-root-{}-{}/", uuid::Uuid::new_v4(), now_secs);

    let temp = TempDir::new().unwrap();
    let cfg_path = temp.path().join("config.toml");
    let toml = format!(
        r#"
[server]
listen_addr = "127.0.0.1:0"

[storage]
backend = "s3"

[storage.s3]
bucket = "{}"
prefix = "{}"
region = "{}"
endpoint = "{}"

[storage.ref_index]
enabled = true
path = "{}"
"#,
        bucket,
        prefix,
        region,
        endpoint,
        temp.path().join("ref_index.db").display()
    );
    tokio::fs::write(&cfg_path, toml).await.unwrap();

    let loader = aws_config::defaults(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region.clone()));
    let loader = if std::env::var("AWS_ACCESS_KEY_ID").is_err() {
        loader.credentials_provider(aws_sdk_s3::config::Credentials::new(
            "minioadmin",
            "minioadmin",
            None,
            None,
            "static",
        ))
    } else {
        loader
    };
    let sdk_config = loader.load().await;
    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

    // Verify S3 connectivity
    let probe = s3_client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(&prefix)
        .send()
        .await;
    if probe.is_err() && !is_required {
        println!("Skipping live S3 CLI test: MinIO endpoint unreachable at {endpoint}");
        return;
    }
    probe.expect("MinIO live probe failed");

    // 1. Run migrate-membership apply on live S3
    let apply_cli = Cli {
        config: vec![cfg_path.clone()],
        command: Some(CliCommand::MigrateMembership {
            command: MigrateMembershipCommand::Apply,
        }),
    };
    let apply_res = execute_cli(apply_cli).await;
    assert!(
        apply_res.is_ok(),
        "migrate-membership apply on live S3 must succeed: {:?}",
        apply_res
    );

    // 2. Run inspect-lock on live S3
    let inspect_cli = Cli {
        config: vec![cfg_path.clone()],
        command: Some(CliCommand::InspectLock),
    };
    let inspect_res = execute_cli(inspect_cli).await;
    assert!(
        inspect_res.is_ok(),
        "inspect-lock on live S3 must succeed: {:?}",
        inspect_res
    );

    // 3. S3 server-versus-CLI contention: simulate active server holding writer lock on S3
    let cfg = Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap();
    let wiring = naust::storage_wiring::storage_wiring_from_config(&cfg);
    let mut server_auth = RuntimeMutationAuthority::acquire(wiring.cluster_lock(), "server")
        .await
        .expect("server authority acquire on S3");

    let contend_cli = Cli {
        config: vec![cfg_path.clone()],
        command: Some(CliCommand::RefIndex {
            command: RefIndexCommand::Rebuild,
        }),
    };
    let contend_res = execute_cli(contend_cli.clone()).await;
    match contend_res {
        Err(CliError::LockContention(_)) => {}
        other => panic!(
            "expected LockContention on S3 server contention, got {:?}",
            other
        ),
    }

    // Release server lock
    server_auth.release().await.unwrap();

    // 4. Two concurrent mutating CLI commands: second fails with LockContention
    let mut cli1_auth = RuntimeMutationAuthority::acquire(wiring.cluster_lock(), "cli-proc-1")
        .await
        .expect("cli1 authority acquire");
    let cli2_res = execute_cli(contend_cli.clone()).await;
    match cli2_res {
        Err(CliError::LockContention(_)) => {}
        other => panic!(
            "expected LockContention for concurrent CLI, got {:?}",
            other
        ),
    }
    cli1_auth.release().await.unwrap();

    // 5. Unsupported S3 GC quarantine check
    let gc_quarantine_cli = Cli {
        config: vec![cfg_path.clone()],
        command: Some(CliCommand::BlobGc {
            command: BlobGcCommand::Quarantine {
                policy: naust::blob_gc::BlobGcPolicy::ManifestRooted,
                min_age_secs: 0,
                max_per_run: 10,
                confirm_all_writers_stopped: true,
                force_gc: false,
            },
        }),
    };
    let gc_quarantine_res = execute_cli(gc_quarantine_cli).await;
    match gc_quarantine_res {
        Err(CliError::Gc(_)) => {}
        other => panic!(
            "expected Gc error (unsupported strategy) on S3 quarantine, got {:?}",
            other
        ),
    }

    // 6. Authority reacquisition after failure succeeds
    let mut post_fail_auth =
        RuntimeMutationAuthority::acquire(wiring.cluster_lock(), "reacquire-after-fail")
            .await
            .expect("authority must be re-acquirable after failed CLI command");
    post_fail_auth.release().await.unwrap();

    // 7. Cleanup live S3 test objects
    let list_res = s3_client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(&prefix)
        .send()
        .await
        .expect("list objects for cleanup");
    let mut deleted_count = 0;
    if let Some(contents) = list_res.contents {
        for obj in contents {
            if let Some(key) = obj.key {
                let _ = s3_client
                    .delete_object()
                    .bucket(&bucket)
                    .key(key)
                    .send()
                    .await;
                deleted_count += 1;
            }
        }
    }
    println!(
        "LIVE S3 CLI TEST: Cleaned up {} test objects under prefix '{}'",
        deleted_count, prefix
    );

    // 8. Verify 0 objects remaining
    let post_check = s3_client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(&prefix)
        .send()
        .await
        .expect("verify prefix empty");
    assert_eq!(
        post_check.key_count().unwrap_or(0),
        0,
        "prefix must be empty after cleanup"
    );
}

/// KI-05: with `blob_gc.enabled=false` (the default) the CLI refuses
/// destructive GC unless --force-gc is passed; plan stays available.
#[tokio::test]
async fn test_cli_respects_blob_gc_kill_switch() {
    let temp = TempDir::new().unwrap();
    let fs_root = temp.path().join("registry");
    let ref_index = temp.path().join("ref_index.db");
    let cfg_path = temp.path().join("config.toml");
    let toml = format!(
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
    );
    tokio::fs::write(&cfg_path, toml).await.unwrap();
    let cfg = Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap();
    assert!(!cfg.blob_gc_enabled, "default must be disabled");
    let wiring = naust::storage_wiring::storage_wiring_from_config(&cfg);
    wiring
        .membership_reader()
        .mark_membership_ready()
        .await
        .unwrap();

    let quarantine = |force_gc: bool| Cli {
        config: vec![cfg_path.clone()],
        command: Some(CliCommand::BlobGc {
            command: BlobGcCommand::Quarantine {
                policy: naust::blob_gc::BlobGcPolicy::ManifestRooted,
                min_age_secs: 0,
                max_per_run: 10,
                confirm_all_writers_stopped: true,
                force_gc,
            },
        }),
    };

    // Refused without force…
    match execute_cli(quarantine(false)).await {
        Err(CliError::GcDisabled(which)) => assert_eq!(which, "blob_gc.enabled=false"),
        other => panic!("expected GcDisabled, got {other:?}"),
    }
    // …and delete is refused the same way.
    let delete = Cli {
        config: vec![cfg_path.clone()],
        command: Some(CliCommand::BlobGc {
            command: BlobGcCommand::Delete {
                policy: naust::blob_gc::BlobGcPolicy::ManifestRooted,
                quarantine_delay_secs: 0,
                max_per_run: 10,
                confirm_all_writers_stopped: true,
                force_gc: false,
            },
        }),
    };
    match execute_cli(delete).await {
        Err(CliError::GcDisabled(_)) => {}
        other => panic!("expected GcDisabled for delete, got {other:?}"),
    }

    // --force-gc reproduces the historical behavior (runs on a clean store).
    execute_cli(quarantine(true))
        .await
        .expect("--force-gc must run despite the kill switch");
}
