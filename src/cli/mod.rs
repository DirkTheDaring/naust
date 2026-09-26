pub mod errors;
pub mod policy;
pub mod runtime;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;

pub use errors::CliError;
pub use policy::CommandPolicy;
pub use runtime::MaintenanceRuntime;

use crate::blob_ref_index::{BlobRefIndex, RefIndexError};
use crate::config::{Config, StorageBackend};

#[derive(Parser, Debug, Clone)]
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

impl MigrateMembershipCommand {
    pub fn policy(&self) -> CommandPolicy {
        match self {
            MigrateMembershipCommand::Plan => CommandPolicy::ExclusiveInspection {
                lock_suffix: "migrate-membership-plan",
            },
            MigrateMembershipCommand::Apply => CommandPolicy::Migration {
                lock_suffix: "migrate-membership-apply",
            },
            MigrateMembershipCommand::Verify => CommandPolicy::ExclusiveInspection {
                lock_suffix: "migrate-membership-verify",
            },
        }
    }
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

impl RefIndexCommand {
    pub fn policy(&self) -> CommandPolicy {
        match self {
            RefIndexCommand::Check => CommandPolicy::ReadOnly,
            RefIndexCommand::Rebuild => CommandPolicy::ExclusiveMutation {
                lock_suffix: "ref-index-rebuild",
            },
            RefIndexCommand::Ensure => CommandPolicy::ExclusiveMutation {
                lock_suffix: "ref-index-ensure",
            },
        }
    }
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

        /// Run even though blob_gc.enabled=false in the configuration (KI-05):
        /// without this flag the CLI respects the kill switch and refuses.
        #[arg(long, default_value_t = false)]
        force_gc: bool,
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

        /// Run even though blob_gc.enabled=false / blob_gc.enable_delete=false
        /// in the configuration (KI-05): without this flag the CLI respects
        /// the kill switches and refuses.
        #[arg(long, default_value_t = false)]
        force_gc: bool,
    },
}

impl BlobGcCommand {
    pub fn policy(&self) -> CommandPolicy {
        match self {
            BlobGcCommand::Plan { .. } => CommandPolicy::ReadOnly,
            BlobGcCommand::Quarantine { .. } => CommandPolicy::ExclusiveMutation {
                lock_suffix: "blob-gc-quarantine",
            },
            BlobGcCommand::Delete { .. } => CommandPolicy::ExclusiveMutation {
                lock_suffix: "blob-gc-delete",
            },
        }
    }
}

impl CliCommand {
    pub fn policy(&self) -> CommandPolicy {
        match self {
            CliCommand::CheckConfig | CliCommand::HashSecret | CliCommand::AuditPermissions => {
                CommandPolicy::Pure
            }

            CliCommand::RefIndex { command } => command.policy(),
            CliCommand::BlobGc { command } => command.policy(),
            CliCommand::MigrateMembership { command } => command.policy(),

            CliCommand::InspectLock => CommandPolicy::ReadOnly,
            CliCommand::AdminClearLock { .. } => CommandPolicy::BreakGlass,
            CliCommand::Server => CommandPolicy::ExclusiveMutation {
                lock_suffix: "server",
            },
        }
    }
}

pub fn load_config(config_paths: &[PathBuf]) -> Result<Config, CliError> {
    if config_paths.is_empty() {
        Config::from_env().map_err(CliError::Config)
    } else {
        Config::from_env_with_files(config_paths).map_err(CliError::Config)
    }
}

pub fn load_config_or_exit(command: &str, config_paths: &[PathBuf]) -> Config {
    match load_config(config_paths) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("{command}: {err}");
            std::process::exit(err.exit_code());
        }
    }
}

/// Execute CLI command with typed error result.
pub async fn execute_cli(cli: Cli) -> Result<(), CliError> {
    let command = cli.command.unwrap_or(CliCommand::Server);
    let config_paths = &cli.config;

    match command {
        CliCommand::CheckConfig => {
            let _cfg = load_config(config_paths)?;
            println!("OK");
            Ok(())
        }
        CliCommand::HashSecret => {
            use std::io::Read as _;
            let mut secret = String::new();
            std::io::stdin()
                .read_to_string(&mut secret)
                .map_err(CliError::StdinRead)?;
            let secret = secret.trim();
            if secret.is_empty() {
                return Err(CliError::EmptySecret);
            }
            let hash = crate::robot_secrets::hash_robot_secret(secret)
                .map_err(|e| CliError::Hashing(e.to_string()))?;
            println!("{hash}");
            Ok(())
        }
        CliCommand::AuditPermissions => {
            let cfg = load_config(config_paths)?;
            crate::audit::print_audit(&cfg);
            Ok(())
        }
        CliCommand::Server => {
            let config = Arc::new(load_config(config_paths)?);
            crate::supervisor::run_server_supervisor(config, None)
                .await
                .map_err(|e| CliError::Server(e.to_string()))
        }
        CliCommand::AdminClearLock {
            expected_owner,
            expected_etag,
            confirm,
        } => {
            let config = load_config(config_paths)?;
            MaintenanceRuntime::admin_clear_lock(
                &config,
                &expected_owner,
                &expected_etag,
                &confirm,
            )
            .await?;
            println!("Deployment writer lock successfully cleared.");
            Ok(())
        }
        CliCommand::RefIndex { command } => {
            let cfg = Arc::new(load_config(config_paths)?);
            if !cfg.ref_index.enabled {
                return Err(CliError::RefIndexDisabled);
            }

            if command == RefIndexCommand::Check {
                let res = match BlobRefIndex::check_path_health(&cfg.ref_index.path) {
                    Ok(()) => Ok(()),
                    Err(RefIndexError::NotFound(path)) => Err(CliError::IndexMissing { path }),
                    Err(RefIndexError::Corrupt(reason)) => Err(CliError::IndexUnhealthy {
                        path: cfg.ref_index.path.clone(),
                        reason,
                    }),
                    Err(other) => Err(CliError::Index(other)),
                };
                res?;
                println!("OK");
                return Ok(());
            }

            let policy = command.policy();
            let runtime = MaintenanceRuntime::acquire(cfg, policy).await?;

            let res = match command {
                RefIndexCommand::Check => unreachable!(),
                RefIndexCommand::Rebuild => runtime.ref_index_rebuild().await,
                RefIndexCommand::Ensure => runtime.ref_index_ensure().await,
            };

            runtime.finalize_with_result(res).await?;

            println!("OK");
            Ok(())
        }
        CliCommand::BlobGc { command } => {
            let cfg = Arc::new(load_config(config_paths)?);

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
                            return Err(CliError::S3GcConfirmationRequired {
                                bucket: cfg.s3_bucket.clone().unwrap_or_default(),
                                prefix: cfg.s3_prefix.clone(),
                            });
                        }
                        println!(
                            "blob-gc: S3 destructive operation confirmed (all writers stopped) for bucket='{}' prefix='{}'",
                            cfg.s3_bucket.as_deref().unwrap_or(""),
                            cfg.s3_prefix
                        );
                    }
                }
            }

            let policy = command.policy();
            let runtime = MaintenanceRuntime::acquire(cfg, policy).await?;

            let res = match command {
                BlobGcCommand::Plan {
                    policy,
                    min_age_secs,
                    max_per_run,
                } => runtime
                    .blob_gc_plan(policy, min_age_secs, max_per_run)
                    .await
                    .map(|stats| {
                        println!(
                            "scanned_blobs={} scanned_bytes={} eligible_blobs={} eligible_bytes={}",
                            stats.scanned_blobs,
                            stats.scanned_bytes,
                            stats.eligible_blobs,
                            stats.eligible_bytes
                        );
                    }),
                BlobGcCommand::Quarantine {
                    policy,
                    min_age_secs,
                    max_per_run,
                    force_gc,
                    ..
                } => runtime
                    .blob_gc_quarantine(policy, min_age_secs, max_per_run, force_gc)
                    .await
                    .map(|stats| {
                        println!(
                            "scanned_blobs={} scanned_bytes={} quarantined_blobs={} quarantined_bytes={}",
                            stats.scanned_blobs,
                            stats.scanned_bytes,
                            stats.quarantined_blobs,
                            stats.quarantined_bytes
                        );
                    }),
                BlobGcCommand::Delete {
                    policy,
                    quarantine_delay_secs,
                    max_per_run,
                    force_gc,
                    ..
                } => runtime
                    .blob_gc_delete(policy, quarantine_delay_secs, max_per_run, force_gc)
                    .await
                    .map(|stats| {
                        println!(
                            "restored_blobs={} restored_bytes={} deleted_blobs={} deleted_bytes={}",
                            stats.restored_blobs,
                            stats.restored_bytes,
                            stats.deleted_blobs,
                            stats.deleted_bytes
                        );
                    }),
            };

            runtime.finalize_with_result(res).await?;

            Ok(())
        }
        CliCommand::MigrateMembership { command } => {
            let cfg = Arc::new(load_config(config_paths)?);
            let policy = command.policy();
            let runtime = MaintenanceRuntime::acquire(cfg, policy).await?;

            let res = match command {
                MigrateMembershipCommand::Plan => {
                    println!("Planning repository blob membership migration (dry-run)...");
                    runtime.migrate_membership_plan().await.map(|stats| {
                        println!("Migration Plan Summary:");
                        println!("  Repositories scanned: {}", stats.repositories_scanned);
                        println!("  Manifests scanned:    {}", stats.manifests_scanned);
                        println!("  Memberships to create: {}", stats.memberships_created);
                        println!(
                            "  Memberships present:  {}",
                            stats.memberships_already_present
                        );
                    })
                }
                MigrateMembershipCommand::Apply => {
                    println!("Applying repository blob membership migration...");
                    runtime.migrate_membership_apply().await.map(|stats| {
                        println!("Migration Applied Successfully:");
                        println!("  Repositories scanned: {}", stats.repositories_scanned);
                        println!("  Manifests scanned:    {}", stats.manifests_scanned);
                        println!("  Memberships created:  {}", stats.memberships_created);
                        println!(
                            "  Memberships present:  {}",
                            stats.memberships_already_present
                        );
                        println!("  Membership marker marked Ready.");
                    })
                }
                MigrateMembershipCommand::Verify => {
                    println!("Verifying repository blob memberships...");
                    runtime.migrate_membership_verify().await.and_then(|ok| {
                        if ok {
                            println!(
                                "Verification passed: all repository-referenced blobs have valid membership records."
                            );
                            Ok(())
                        } else {
                            Err(CliError::MembershipVerificationFailed)
                        }
                    })
                }
            };

            runtime.finalize_with_result(res).await?;

            Ok(())
        }
        CliCommand::InspectLock => {
            let cfg = Arc::new(load_config(config_paths)?);
            let runtime = MaintenanceRuntime::acquire(cfg, CommandPolicy::ReadOnly).await?;
            let lock_doc = runtime.inspect_lock().await;
            let doc_opt = runtime.finalize_with_result(lock_doc).await?;

            match doc_opt {
                Some((doc, etag)) => {
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
                    Ok(())
                }
                None => {
                    println!("Deployment Writer Lock Status: UNLOCKED (no active writer lock)");
                    Ok(())
                }
            }
        }
    }
}

/// Run CLI and return process exit code (0 = success, 1 = operational failure, 2 = config/usage error).
pub async fn run_cli(cli: Cli) -> i32 {
    let command_name = match &cli.command {
        Some(CliCommand::Server) => "server",
        Some(CliCommand::CheckConfig) => "check-config",
        Some(CliCommand::HashSecret) => "hash-secret",
        Some(CliCommand::AuditPermissions) => "audit-permissions",
        Some(CliCommand::RefIndex { .. }) => "ref-index",
        Some(CliCommand::BlobGc { .. }) => "blob-gc",
        Some(CliCommand::MigrateMembership { .. }) => "migrate-membership",
        Some(CliCommand::InspectLock) => "inspect-lock",
        Some(CliCommand::AdminClearLock { .. }) => "admin-clear-lock",
        None => "server",
    };

    match execute_cli(cli).await {
        Ok(()) => 0,
        Err(err) => {
            let code = err.exit_code();
            eprintln!("{command_name}: {err}");
            code
        }
    }
}
