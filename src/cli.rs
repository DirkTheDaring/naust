use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;

use crate::config::{Config, StorageBackend};
use crate::storage;

#[derive(Parser, Debug)]
#[command(
    name = "registry-rust",
    about = "OCI/Docker Distribution Registry in Rust",
    version
)]
pub struct Cli {
    /// Path to config TOML file (can be repeated)
    #[arg(
        short = 'c',
        long = "config",
        value_name = "FILE",
        global = true,
        action = clap::ArgAction::Append
    )]
    pub config: Vec<PathBuf>,

    #[command(subcommand)]
    pub command: Option<CliCommand>,
}

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum CliCommand {
    /// Run the OCI registry server (default).
    #[command(name = "server")]
    Server,

    /// Validate the configuration and exit (0 = valid, 2 = invalid).
    #[command(name = "check-config")]
    CheckConfig,

    /// Read a secret from stdin and print an Argon2id hash (for robots/users).
    #[command(name = "hash-secret")]
    HashSecret,

    /// Print effective RBAC permissions (robots + users/groups) and exit.
    #[command(name = "audit-permissions")]
    AuditPermissions,

    /// Inspect / rebuild the persistent blob reference index.
    #[command(name = "ref-index")]
    RefIndex {
        #[command(subcommand)]
        command: RefIndexCommand,
    },

    /// Reclaim storage by quarantining/deleting unreferenced blobs.
    #[command(name = "blob-gc")]
    BlobGc {
        #[command(subcommand)]
        command: BlobGcCommand,
    },

    /// Migrate or verify repository-scoped blob memberships.
    #[command(name = "migrate-membership")]
    MigrateMembership {
        #[command(subcommand)]
        command: MigrateMembershipCommand,
    },

    /// Inspect the current deployment writer lock metadata.
    #[command(name = "inspect-lock")]
    InspectLock,

    /// Administratively clear an abandoned deployment writer lock matching expected owner and ETag.
    #[command(name = "admin-clear-lock", alias = "force-unlock")]
    AdminClearLock {
        /// Expected owner ID or token from `inspect-lock`
        #[arg(long, default_value = "")]
        expected_owner: String,

        /// Observed object version / ETag from `inspect-lock`
        #[arg(long, default_value = "")]
        expected_etag: String,

        /// Destructive confirmation token (must be 'CONFIRM-CLEAR-ABANDONED-WRITER' or 'FORCE')
        #[arg(long)]
        confirm: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum MigrateMembershipCommand {
    /// Plan repository blob membership backfill (dry-run).
    #[command(name = "plan")]
    Plan,

    /// Apply repository blob membership backfill and mark storage Ready.
    #[command(name = "apply")]
    Apply,

    /// Verify that all repository-referenced blobs have durable membership records.
    #[command(name = "verify")]
    Verify,
}

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum RefIndexCommand {
    /// Verify the index is healthy (schema + ready state). Exit 0 if OK, 1 if corrupt.
    #[command(name = "check")]
    Check,

    /// Rebuild the index from the registry storage.
    #[command(name = "rebuild")]
    Rebuild,

    /// Check and rebuild if corrupt (respects auto-rebuild config).
    #[command(name = "ensure")]
    Ensure,
}

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum BlobGcCommand {
    /// Print what would be quarantined (dry-run).
    #[command(name = "plan")]
    Plan {
        /// Reference policy to decide whether a blob is considered in use.
        #[arg(long, value_enum, default_value_t = crate::blob_gc::BlobGcPolicy::ManifestRooted)]
        policy: crate::blob_gc::BlobGcPolicy,

        /// Only consider blobs older than this age.
        #[arg(long, default_value_t = 7 * 24 * 3600)]
        min_age_secs: u64,

        /// Maximum number of blobs to report.
        #[arg(long, default_value_t = 10_000)]
        max_per_run: usize,
    },

    /// Move eligible blobs into quarantine (reversible).
    #[command(name = "quarantine")]
    Quarantine {
        #[arg(long, value_enum, default_value_t = crate::blob_gc::BlobGcPolicy::ManifestRooted)]
        policy: crate::blob_gc::BlobGcPolicy,

        #[arg(long, default_value_t = 7 * 24 * 3600)]
        min_age_secs: u64,

        #[arg(long, default_value_t = 10_000)]
        max_per_run: usize,

        /// Explicit confirmation that all active registry writers operating against this S3 bucket are stopped.
        #[arg(long, default_value_t = false)]
        confirm_all_writers_stopped: bool,
    },

    /// Permanently delete blobs from quarantine after a delay (re-checks reachability).
    #[command(name = "delete")]
    Delete {
        #[arg(long, value_enum, default_value_t = crate::blob_gc::BlobGcPolicy::ManifestRooted)]
        policy: crate::blob_gc::BlobGcPolicy,

        /// A quarantined blob must be at least this old before it can be deleted.
        #[arg(long, default_value_t = 24 * 3600)]
        quarantine_delay_secs: u64,

        #[arg(long, default_value_t = 10_000)]
        max_per_run: usize,

        /// Explicit confirmation that all active registry writers operating against this S3 bucket are stopped.
        #[arg(long, default_value_t = false)]
        confirm_all_writers_stopped: bool,
    },
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CommandIntent {
    ReadOnly,
    MutationCapableServer,
    MutationCapableMaintenance { lock_suffix: &'static str },
    DestructiveAdminRecovery,
}

impl CliCommand {
    pub fn intent(&self) -> CommandIntent {
        match self {
            CliCommand::CheckConfig
            | CliCommand::HashSecret
            | CliCommand::AuditPermissions
            | CliCommand::InspectLock => CommandIntent::ReadOnly,

            CliCommand::RefIndex { command } => match command {
                RefIndexCommand::Check => CommandIntent::ReadOnly,
                RefIndexCommand::Rebuild => CommandIntent::MutationCapableMaintenance {
                    lock_suffix: "ref-index-rebuild",
                },
                RefIndexCommand::Ensure => CommandIntent::MutationCapableMaintenance {
                    lock_suffix: "ref-index-ensure",
                },
            },

            CliCommand::BlobGc { command } => match command {
                BlobGcCommand::Plan { .. } => CommandIntent::ReadOnly,
                BlobGcCommand::Quarantine { .. } => CommandIntent::MutationCapableMaintenance {
                    lock_suffix: "blob-gc-quarantine",
                },
                BlobGcCommand::Delete { .. } => CommandIntent::MutationCapableMaintenance {
                    lock_suffix: "blob-gc-delete",
                },
            },

            CliCommand::MigrateMembership { command } => match command {
                MigrateMembershipCommand::Plan | MigrateMembershipCommand::Verify => {
                    CommandIntent::ReadOnly
                }
                MigrateMembershipCommand::Apply => CommandIntent::MutationCapableMaintenance {
                    lock_suffix: "migrate-membership-apply",
                },
            },

            CliCommand::AdminClearLock { .. } => CommandIntent::DestructiveAdminRecovery,

            CliCommand::Server => CommandIntent::MutationCapableServer,
        }
    }
}

pub fn load_config_or_exit(command: &str, config_paths: &[PathBuf]) -> Config {
    let res = if config_paths.is_empty() {
        Config::from_env()
    } else {
        Config::from_env_with_files(config_paths)
    };
    match res {
        Ok(c) => c,
        Err(err) => {
            eprintln!("{command}: failed to load config: {err}");
            std::process::exit(2);
        }
    }
}

pub async fn run_cli(cli: Cli) -> i32 {
    let command = cli.command.unwrap_or(CliCommand::Server);
    let config_paths = cli.config;

    match command {
        CliCommand::CheckConfig => {
            let _cfg = load_config_or_exit("check-config", &config_paths);
            println!("OK");
            0
        }
        CliCommand::HashSecret => {
            use std::io::Read as _;
            let mut secret = String::new();
            if let Err(e) = std::io::stdin().read_to_string(&mut secret) {
                eprintln!("hash-secret: failed to read from stdin: {e}");
                return 1;
            }
            let secret = secret.trim();
            if secret.is_empty() {
                eprintln!("hash-secret: empty secret provided");
                return 1;
            }
            match crate::robot_secrets::hash_robot_secret(secret) {
                Ok(hash) => {
                    println!("{hash}");
                    0
                }
                Err(e) => {
                    eprintln!("hash-secret: hashing failed: {e}");
                    1
                }
            }
        }
        CliCommand::AuditPermissions => {
            let cfg = load_config_or_exit("audit-permissions", &config_paths);
            let _report = crate::audit::print_audit(&cfg);
            0
        }
        CliCommand::RefIndex { command } => {
            let cfg = load_config_or_exit("ref-index", &config_paths);
            if !cfg.ref_index.enabled {
                eprintln!("ref-index is disabled (storage.ref_index.enabled=false)");
                return 2;
            }

            let _fs_root_lock = if cfg.storage_backend == StorageBackend::Filesystem {
                match crate::fs_root_lock::FsRootLock::try_acquire(&cfg.fs_root) {
                    Ok(l) => Some(l),
                    Err(e) => {
                        eprintln!(
                            "ref-index: refusing to run while registry is active ({e}); stop the server first"
                        );
                        return 2;
                    }
                }
            } else {
                None
            };

            let idx = match crate::blob_ref_index::BlobRefIndex::open(cfg.ref_index.path.clone()) {
                Ok(i) => i,
                Err(e) => {
                    eprintln!(
                        "ref-index: failed to open {}: {e}",
                        cfg.ref_index.path.display()
                    );
                    return 1;
                }
            };

            match command {
                RefIndexCommand::Check => match idx.check_health() {
                    Ok(()) => {
                        println!("OK");
                        0
                    }
                    Err(e) => {
                        eprintln!("ref-index: {e}");
                        1
                    }
                },
                RefIndexCommand::Rebuild => {
                    let storage = storage::from_config(&cfg);
                    let mut authority =
                        match storage::mutation_authority::RuntimeMutationAuthority::acquire(
                            storage.clone(),
                            "ref-index-rebuild",
                        )
                        .await
                        {
                            Ok(a) => a,
                            Err(e) => {
                                eprintln!(
                                    "ref-index: failed to acquire exclusive deployment writer authority: {e}"
                                );
                                return 1;
                            }
                        };

                    if let Err(e) = idx.rebuild(&storage).await {
                        eprintln!("ref-index: rebuild failed: {e}");
                        let _ = authority.release().await;
                        return 1;
                    }
                    let _ = authority.release().await;
                    println!("OK");
                    0
                }
                RefIndexCommand::Ensure => {
                    let storage = storage::from_config(&cfg);
                    let mut authority =
                        match storage::mutation_authority::RuntimeMutationAuthority::acquire(
                            storage.clone(),
                            "ref-index-ensure",
                        )
                        .await
                        {
                            Ok(a) => a,
                            Err(e) => {
                                eprintln!(
                                    "ref-index: failed to acquire exclusive deployment writer authority: {e}"
                                );
                                return 1;
                            }
                        };

                    if let Err(e) = idx
                        .ensure_healthy_or_rebuild(
                            &storage,
                            cfg.ref_index.auto_rebuild_on_corruption,
                            cfg.ref_index.rebuild_on_start,
                        )
                        .await
                    {
                        eprintln!("ref-index: ensure failed: {e}");
                        let _ = authority.release().await;
                        return 1;
                    }
                    let _ = authority.release().await;
                    println!("OK");
                    0
                }
            }
        }
        CliCommand::BlobGc { command } => {
            let cfg = load_config_or_exit("blob-gc", &config_paths);

            if cfg.storage_backend == StorageBackend::S3 {
                match &command {
                    BlobGcCommand::Plan { .. } => {}
                    BlobGcCommand::Quarantine {
                        confirm_all_writers_stopped,
                        ..
                    }
                    | BlobGcCommand::Delete {
                        confirm_all_writers_stopped,
                        ..
                    } => {
                        if !confirm_all_writers_stopped {
                            eprintln!(
                                "blob-gc: destructive offline GC on S3 storage requires explicit confirmation that all writers using bucket '{}' (prefix '{}') are stopped.\n\
                                 Re-run with --confirm-all-writers-stopped to proceed, or use 'plan' for dry-run.",
                                cfg.s3_bucket.as_deref().unwrap_or(""),
                                cfg.s3_prefix
                            );
                            return 2;
                        }
                        println!(
                            "blob-gc: S3 destructive operation confirmed (all writers stopped) for bucket='{}' prefix='{}'",
                            cfg.s3_bucket.as_deref().unwrap_or(""),
                            cfg.s3_prefix
                        );
                    }
                }
            }

            let _fs_root_lock = if cfg.storage_backend == StorageBackend::Filesystem {
                match crate::fs_root_lock::FsRootLock::try_acquire(&cfg.fs_root) {
                    Ok(l) => Some(l),
                    Err(e) => {
                        eprintln!(
                            "blob-gc: refusing to run while registry is active ({e}); stop the server first"
                        );
                        return 2;
                    }
                }
            } else {
                None
            };

            let idx = match crate::blob_ref_index::BlobRefIndex::open(cfg.ref_index.path.clone()) {
                Ok(i) => i,
                Err(e) => {
                    eprintln!(
                        "blob-gc: failed to open ref-index {}: {e}",
                        cfg.ref_index.path.display()
                    );
                    return 1;
                }
            };

            let storage = storage::from_config(&cfg);
            if let Err(e) = idx
                .ensure_healthy_or_rebuild(
                    &storage,
                    cfg.ref_index.auto_rebuild_on_corruption,
                    cfg.ref_index.rebuild_on_start,
                )
                .await
            {
                eprintln!("blob-gc: ref-index ensure failed: {e}");
                return 1;
            }

            let mut gc_cfg = cfg.clone();
            gc_cfg.blob_gc_enabled = true;
            gc_cfg.blob_gc_enable_delete = true;

            match command {
                BlobGcCommand::Plan {
                    policy,
                    min_age_secs,
                    max_per_run,
                } => {
                    let service = crate::gc_service::GcService::new(
                        std::sync::Arc::new(gc_cfg),
                        storage.clone(),
                        std::sync::Arc::new(idx),
                    );
                    let budgets = crate::gc_service::GcBudgets {
                        max_blobs: max_per_run,
                        max_bytes: u64::MAX,
                        max_seconds: u64::MAX,
                    };
                    let stats = match service
                        .plan(
                            policy,
                            std::time::Duration::from_secs(min_age_secs),
                            budgets,
                        )
                        .await
                    {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("blob-gc: plan failed: {e}");
                            return 1;
                        }
                    };

                    println!(
                        "scanned_blobs={} scanned_bytes={} eligible_blobs={} eligible_bytes={}",
                        stats.scanned_blobs,
                        stats.scanned_bytes,
                        stats.eligible_blobs,
                        stats.eligible_bytes
                    );
                    0
                }
                BlobGcCommand::Quarantine {
                    policy,
                    min_age_secs,
                    max_per_run,
                    ..
                } => {
                    let authority =
                        match storage::mutation_authority::RuntimeMutationAuthority::acquire(
                            storage.clone(),
                            "blob-gc-quarantine",
                        )
                        .await
                        {
                            Ok(a) => a,
                            Err(e) => {
                                eprintln!(
                                    "blob-gc: failed to acquire exclusive deployment writer authority: {e}"
                                );
                                return 1;
                            }
                        };

                    let service = crate::gc_service::GcService::with_authority(
                        std::sync::Arc::new(gc_cfg),
                        storage.clone(),
                        std::sync::Arc::new(idx),
                        authority,
                    );

                    let budgets = crate::gc_service::GcBudgets {
                        max_blobs: max_per_run,
                        max_bytes: u64::MAX,
                        max_seconds: u64::MAX,
                    };
                    let stats = match service
                        .quarantine(
                            policy,
                            std::time::Duration::from_secs(min_age_secs),
                            budgets,
                        )
                        .await
                    {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("blob-gc: quarantine failed: {e}");
                            return 1;
                        }
                    };

                    let _ = service.release_authority().await;

                    println!(
                        "scanned_blobs={} scanned_bytes={} quarantined_blobs={} quarantined_bytes={}",
                        stats.scanned_blobs,
                        stats.scanned_bytes,
                        stats.quarantined_blobs,
                        stats.quarantined_bytes
                    );
                    0
                }
                BlobGcCommand::Delete {
                    policy,
                    quarantine_delay_secs,
                    max_per_run,
                    ..
                } => {
                    let authority =
                        match storage::mutation_authority::RuntimeMutationAuthority::acquire(
                            storage.clone(),
                            "blob-gc-delete",
                        )
                        .await
                        {
                            Ok(a) => a,
                            Err(e) => {
                                eprintln!(
                                    "blob-gc: failed to acquire exclusive deployment writer authority: {e}"
                                );
                                return 1;
                            }
                        };

                    let service = crate::gc_service::GcService::with_authority(
                        std::sync::Arc::new(gc_cfg),
                        storage.clone(),
                        std::sync::Arc::new(idx),
                        authority,
                    );

                    let budgets = crate::gc_service::GcBudgets {
                        max_blobs: max_per_run,
                        max_bytes: u64::MAX,
                        max_seconds: u64::MAX,
                    };
                    let stats = match service
                        .delete(
                            policy,
                            std::time::Duration::from_secs(quarantine_delay_secs),
                            budgets,
                        )
                        .await
                    {
                        Ok(s) => s,
                        Err(e) => {
                            let _ = service.release_authority().await;
                            eprintln!("blob-gc: delete failed: {e}");
                            return 1;
                        }
                    };

                    let _ = service.release_authority().await;

                    println!(
                        "restored_blobs={} restored_bytes={} deleted_blobs={} deleted_bytes={}",
                        stats.restored_blobs,
                        stats.restored_bytes,
                        stats.deleted_blobs,
                        stats.deleted_bytes
                    );
                    0
                }
            }
        }
        CliCommand::MigrateMembership { command } => {
            let config = Arc::new(load_config_or_exit("migrate-membership", &config_paths));
            let storage = storage::from_config(config.as_ref());
            match command {
                MigrateMembershipCommand::Plan => {
                    println!("Planning repository blob membership migration (dry-run)...");
                    match crate::membership_migration::plan_membership_migration(&storage).await {
                        Ok(stats) => {
                            println!("Migration Plan Summary:");
                            println!("  Repositories scanned: {}", stats.repositories_scanned);
                            println!("  Manifests scanned:    {}", stats.manifests_scanned);
                            println!("  Memberships to create: {}", stats.memberships_created);
                            println!(
                                "  Memberships present:  {}",
                                stats.memberships_already_present
                            );
                            0
                        }
                        Err(err) => {
                            eprintln!("migrate-membership plan failed: {err}");
                            1
                        }
                    }
                }
                MigrateMembershipCommand::Apply => {
                    let mut authority =
                        match storage::mutation_authority::RuntimeMutationAuthority::acquire(
                            storage.clone(),
                            "migrate-membership-apply",
                        )
                        .await
                        {
                            Ok(a) => a,
                            Err(err) => {
                                eprintln!(
                                    "migrate-membership: failed to acquire exclusive deployment writer authority: {err}"
                                );
                                return 1;
                            }
                        };

                    println!("Applying repository blob membership migration...");
                    match crate::membership_migration::apply_membership_migration(&storage).await {
                        Ok(stats) => {
                            println!("Migration Applied Successfully:");
                            println!("  Repositories scanned: {}", stats.repositories_scanned);
                            println!("  Manifests scanned:    {}", stats.manifests_scanned);
                            println!("  Memberships created:  {}", stats.memberships_created);
                            println!(
                                "  Memberships present:  {}",
                                stats.memberships_already_present
                            );
                            println!("  Membership marker marked Ready.");
                            let _ = authority.release().await;
                            0
                        }
                        Err(err) => {
                            eprintln!("migrate-membership apply failed: {err}");
                            let _ = authority.release().await;
                            1
                        }
                    }
                }
                MigrateMembershipCommand::Verify => {
                    println!("Verifying repository blob memberships...");
                    match crate::membership_migration::verify_membership_migration(&storage).await {
                        Ok(true) => {
                            println!(
                                "Verification passed: all repository-referenced blobs have valid membership records."
                            );
                            0
                        }
                        Ok(false) => {
                            eprintln!(
                                "Verification failed: one or more referenced blobs lack membership records. Run `migrate-membership apply`."
                            );
                            1
                        }
                        Err(err) => {
                            eprintln!("migrate-membership verify failed: {err}");
                            1
                        }
                    }
                }
            }
        }
        CliCommand::InspectLock => {
            let config = Arc::new(load_config_or_exit("inspect-lock", &config_paths));
            let storage = storage::from_config(config.as_ref());
            match storage::mutation_authority::inspect_deployment_writer_lock(&storage).await {
                Ok(Some((doc, etag))) => {
                    println!("Deployment Writer Lock Status: ACTIVE");
                    println!("  Format Version:    {}", doc.format_version);
                    println!("  Owner ID:          {}", doc.owner_id);
                    println!("  Hostname:          {}", doc.hostname);
                    println!("  PID:               {}", doc.pid);
                    println!("  Command Mode:      {}", doc.command_mode);
                    println!("  Acquired (Unix):   {}", doc.acquired_unix_secs);
                    if let Some(etag) = etag {
                        println!("  Object Version:    {}", etag);
                    }
                    0
                }
                Ok(None) => {
                    println!("Deployment Writer Lock Status: UNLOCKED (no active writer lock)");
                    0
                }
                Err(err) => {
                    eprintln!("inspect-lock failed: {err}");
                    1
                }
            }
        }
        CliCommand::AdminClearLock {
            expected_owner,
            expected_etag,
            confirm,
        } => {
            let config = Arc::new(load_config_or_exit("admin-clear-lock", &config_paths));
            let storage = storage::from_config(config.as_ref());
            let result = if confirm == "CONFIRM-CLEAR-ABANDONED-WRITER" {
                storage::mutation_authority::admin_clear_abandoned_deployment_writer_lock(
                    &storage,
                    &expected_owner,
                    &expected_etag,
                    &confirm,
                )
                .await
            } else {
                storage::mutation_authority::force_unlock_deployment_writer(&storage, &confirm)
                    .await
            };

            match result {
                Ok(()) => {
                    println!("Deployment writer lock successfully cleared.");
                    0
                }
                Err(err) => {
                    eprintln!("admin-clear-lock failed: {err}");
                    1
                }
            }
        }
        CliCommand::Server => {
            let config = Arc::new(load_config_or_exit("server", &config_paths));
            match crate::supervisor::run_server_supervisor(config, None).await {
                Ok(()) => 0,
                Err(err) => {
                    eprintln!("server error: {err}");
                    1
                }
            }
        }
    }
}
