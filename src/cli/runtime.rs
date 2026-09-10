use std::sync::Arc;
use std::time::Duration;

use crate::blob_gc::{BlobGcPolicy, BlobGcStats};
use crate::blob_ref_index::{BlobRefIndex, RefIndexError};
use crate::cli::errors::CliError;
use crate::cli::policy::CommandPolicy;
use crate::config::{Config, StorageBackend};
use crate::consistency::ConsistencyCoordinator;
use crate::fs_root_lock::FsRootLock;
use crate::gc_service::{GcBudgets, GcService};
use crate::membership_migration::{
    MigrationStats, apply_membership_migration, plan_membership_migration,
    verify_membership_migration,
};
use crate::storage::mutation_authority::{
    DeploymentWriterLockDoc, RuntimeMutationAuthority,
    admin_clear_abandoned_deployment_writer_lock, force_unlock_deployment_writer,
    inspect_deployment_writer_lock,
};
use crate::storage::ports::StorageWiring;
use crate::storage::{self, StorageError};

/// Bounded maintenance runtime managing configuration, storage wiring, filesystem exclusion,
/// and distributed mutation authority leases with strict single-ownership unwinding.
pub struct MaintenanceRuntime {
    config: Arc<Config>,
    storage_wiring: StorageWiring,
    fs_root_lock: Option<FsRootLock>,
    authority: Option<RuntimeMutationAuthority>,
}

impl MaintenanceRuntime {
    /// Acquire and initialize the maintenance runtime according to the given command policy.
    pub async fn acquire(config: Arc<Config>, policy: CommandPolicy) -> Result<Self, CliError> {
        Self::acquire_with_storage_factory(config, policy, storage::storage_wiring_try_from_config)
            .await
    }

    pub(crate) async fn acquire_with_storage_factory<F>(
        config: Arc<Config>,
        policy: CommandPolicy,
        storage_factory: F,
    ) -> Result<Self, CliError>
    where
        F: FnOnce(&Config) -> Result<StorageWiring, StorageError> + Send + 'static,
    {
        // 1. Filesystem root lock (where applicable) - mirrors server acquisition order
        let fs_root_lock = if policy.requires_fs_root_lock()
            && config.storage_backend == StorageBackend::Filesystem
        {
            match FsRootLock::try_acquire(&config.fs_root) {
                Ok(lock) => Some(lock),
                Err(err) => {
                    return Err(CliError::ServerActive(err.to_string()));
                }
            }
        } else {
            None
        };

        // 2. Storage wiring initialization
        let storage_wiring = match storage::storage_wiring_try_from_config_async_with_factory(
            config.as_ref(),
            storage_factory,
        )
        .await
        {
            Ok(wiring) => wiring,
            Err(err) => {
                return Err(CliError::Storage(err));
            }
        };

        // 3. Distributed mutation authority acquisition (where applicable)
        let authority = if policy.requires_mutation_authority() {
            let lock_suffix = policy.lock_suffix().unwrap_or("maintenance");
            match RuntimeMutationAuthority::acquire(storage_wiring.cluster_lock(), lock_suffix)
                .await
            {
                Ok(auth) => Some(auth),
                Err(err) => {
                    return Err(CliError::LockContention(err));
                }
            }
        } else {
            None
        };

        // 4. Command-specific readiness preflight
        if matches!(policy, CommandPolicy::ExclusiveMutation { .. }) {
            match storage_wiring
                .membership_reader()
                .is_membership_ready()
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    let this = Self {
                        config,
                        storage_wiring,
                        fs_root_lock,
                        authority,
                    };
                    return this
                        .finalize_with_result(Result::<Self, CliError>::Err(
                            CliError::MembershipBackfillRequired,
                        ))
                        .await;
                }
                Err(err) => {
                    let this = Self {
                        config,
                        storage_wiring,
                        fs_root_lock,
                        authority,
                    };
                    return this
                        .finalize_with_result(Result::<Self, CliError>::Err(CliError::Storage(err)))
                        .await;
                }
            }
        }

        Ok(Self {
            config,
            storage_wiring,
            fs_root_lock,
            authority,
        })
    }

    /// Teardown runtime resources, releasing distributed mutation authority and dropping filesystem lock.
    ///
    /// SAFETY & INVARIANTS:
    /// 1. `self.authority.take()` ensures distributed release is executed AT MOST ONCE.
    /// 2. If authority was already taken or absent, returns `Ok(())` safely without panicking.
    /// 3. `self.fs_root_lock = None` executes unconditionally to ensure local filesystem exclusion
    ///    is released even if distributed release encounters a transport or storage error.
    pub async fn release_authority(&mut self) -> Result<(), crate::storage::StorageError> {
        let res = if let Some(mut auth) = self.authority.take() {
            auth.release().await
        } else {
            Ok(())
        };
        self.fs_root_lock = None;
        res
    }

    /// Single coherent finalization mechanism for all post-acquisition execution paths.
    ///
    /// TRUTH TABLE:
    /// - Primary Ok(v),   Release Ok(())   => Ok(v)
    /// - Primary Err(e),  Release Ok(())   => Err(e)
    /// - Primary Ok(_),   Release Err(rel) => Err(CliError::AuthorityRelease(rel))
    /// - Primary Err(e),  Release Err(rel) => Err(CliError::ExecutionAndTeardownFailed { source: Box::new(e), release_error: rel })
    pub async fn finalize_with_result<T>(
        mut self,
        primary_res: Result<T, CliError>,
    ) -> Result<T, CliError> {
        match primary_res {
            Ok(val) => match self.release_authority().await {
                Ok(()) => Ok(val),
                Err(rel_err) => Err(CliError::AuthorityRelease(rel_err)),
            },
            Err(cmd_err) => match self.release_authority().await {
                Ok(()) => Err(cmd_err),
                Err(rel_err) => Err(CliError::ExecutionAndTeardownFailed {
                    source: Box::new(cmd_err),
                    release_error: rel_err,
                }),
            },
        }
    }

    pub fn config(&self) -> &Arc<Config> {
        &self.config
    }

    // --------------------------------------------------------------------------------------------
    // Ref-Index Operations
    // --------------------------------------------------------------------------------------------

    /// Non-mutating health check of reference index on disk.
    /// Creates no databases, tables, files, or lock metadata if absent or corrupt.
    pub fn ref_index_check(&self) -> Result<(), CliError> {
        match BlobRefIndex::check_path_health(&self.config.ref_index.path) {
            Ok(()) => Ok(()),
            Err(RefIndexError::NotFound(path)) => Err(CliError::IndexMissing { path }),
            Err(RefIndexError::Corrupt(reason)) => Err(CliError::IndexUnhealthy {
                path: self.config.ref_index.path.clone(),
                reason,
            }),
            Err(other) => Err(CliError::Index(other)),
        }
    }

    /// Rebuild the persistent reference index from storage metadata.
    pub async fn ref_index_rebuild(&self) -> Result<(), CliError> {
        let idx =
            BlobRefIndex::open(self.config.ref_index.path.clone()).map_err(CliError::Index)?;
        idx.rebuild(self.storage_wiring.blob_ref_index().as_ref())
            .await
            .map_err(CliError::Index)?;
        Ok(())
    }

    /// Ensure the reference index is healthy, rebuilding if corrupt.
    pub async fn ref_index_ensure(&self) -> Result<(), CliError> {
        let idx =
            BlobRefIndex::open(self.config.ref_index.path.clone()).map_err(CliError::Index)?;
        idx.ensure_healthy_or_rebuild(
            self.storage_wiring.blob_ref_index().as_ref(),
            self.config.ref_index.auto_rebuild_on_corruption,
            self.config.ref_index.rebuild_on_start,
        )
        .await
        .map_err(CliError::Index)?;
        Ok(())
    }

    // --------------------------------------------------------------------------------------------
    // Blob GC Operations
    // --------------------------------------------------------------------------------------------

    /// Non-mutating GC plan generation.
    /// Verifies existing index health without repairing or modifying SQLite/sled state.
    pub async fn blob_gc_plan(
        &self,
        policy: BlobGcPolicy,
        min_age_secs: u64,
        max_per_run: usize,
    ) -> Result<BlobGcStats, CliError> {
        // Preflight membership readiness (read-only query)
        match self
            .storage_wiring
            .membership_reader()
            .is_membership_ready()
            .await
        {
            Ok(true) => {}
            Ok(false) => return Err(CliError::MembershipBackfillRequired),
            Err(err) => return Err(CliError::Storage(err)),
        }

        // Open existing index non-mutatingly
        let idx = match BlobRefIndex::open_existing(&self.config.ref_index.path) {
            Ok(idx) => {
                if let Err(RefIndexError::Corrupt(reason)) = idx.check_health() {
                    return Err(CliError::IndexUnhealthy {
                        path: self.config.ref_index.path.clone(),
                        reason,
                    });
                }
                idx
            }
            Err(RefIndexError::NotFound(path)) => return Err(CliError::IndexMissing { path }),
            Err(err) => return Err(CliError::Index(err)),
        };

        let mut gc_cfg = (*self.config).clone();
        gc_cfg.blob_gc_enabled = true;
        gc_cfg.blob_gc_enable_delete = true;

        let consistency = ConsistencyCoordinator::new();
        let service = GcService::new(
            Arc::new(gc_cfg),
            self.storage_wiring.gc_service_port(),
            Arc::new(idx),
            consistency,
        );

        let budgets = GcBudgets {
            max_blobs: max_per_run,
            max_bytes: u64::MAX,
            max_seconds: u64::MAX,
        };

        let stats = service
            .plan(policy, Duration::from_secs(min_age_secs), budgets)
            .await
            .map_err(CliError::Gc)?;

        Ok(stats)
    }

    /// Move eligible unreferenced blobs into quarantine.
    pub async fn blob_gc_quarantine(
        &self,
        policy: BlobGcPolicy,
        min_age_secs: u64,
        max_per_run: usize,
    ) -> Result<BlobGcStats, CliError> {
        let auth = self.authority.as_ref().ok_or_else(|| {
            CliError::LockContention(crate::storage::StorageError::ExclusiveWriterLocked(
                "mutation authority missing for quarantine".to_string(),
            ))
        })?;

        let idx =
            BlobRefIndex::open(self.config.ref_index.path.clone()).map_err(CliError::Index)?;
        idx.ensure_healthy_or_rebuild(
            self.storage_wiring.blob_ref_index().as_ref(),
            self.config.ref_index.auto_rebuild_on_corruption,
            self.config.ref_index.rebuild_on_start,
        )
        .await
        .map_err(CliError::Index)?;

        let mut gc_cfg = (*self.config).clone();
        gc_cfg.blob_gc_enabled = true;
        gc_cfg.blob_gc_enable_delete = true;

        let consistency = ConsistencyCoordinator::new();
        let service = GcService::new(
            Arc::new(gc_cfg),
            self.storage_wiring.gc_service_port(),
            Arc::new(idx),
            consistency,
        );

        let budgets = GcBudgets {
            max_blobs: max_per_run,
            max_bytes: u64::MAX,
            max_seconds: u64::MAX,
        };

        let stats = service
            .quarantine_with_authority(auth, policy, Duration::from_secs(min_age_secs), budgets)
            .await
            .map_err(CliError::Gc)?;

        Ok(stats)
    }

    /// Permanently delete quarantined blobs.
    pub async fn blob_gc_delete(
        &self,
        policy: BlobGcPolicy,
        quarantine_delay_secs: u64,
        max_per_run: usize,
    ) -> Result<BlobGcStats, CliError> {
        let auth = self.authority.as_ref().ok_or_else(|| {
            CliError::LockContention(crate::storage::StorageError::ExclusiveWriterLocked(
                "mutation authority missing for delete".to_string(),
            ))
        })?;

        let idx =
            BlobRefIndex::open(self.config.ref_index.path.clone()).map_err(CliError::Index)?;
        idx.ensure_healthy_or_rebuild(
            self.storage_wiring.blob_ref_index().as_ref(),
            self.config.ref_index.auto_rebuild_on_corruption,
            self.config.ref_index.rebuild_on_start,
        )
        .await
        .map_err(CliError::Index)?;

        let mut gc_cfg = (*self.config).clone();
        gc_cfg.blob_gc_enabled = true;
        gc_cfg.blob_gc_enable_delete = true;

        let consistency = ConsistencyCoordinator::new();
        let service = GcService::new(
            Arc::new(gc_cfg),
            self.storage_wiring.gc_service_port(),
            Arc::new(idx),
            consistency,
        );

        let budgets = GcBudgets {
            max_blobs: max_per_run,
            max_bytes: u64::MAX,
            max_seconds: u64::MAX,
        };

        let stats = service
            .delete_with_authority(
                auth,
                policy,
                Duration::from_secs(quarantine_delay_secs),
                budgets,
            )
            .await
            .map_err(CliError::Gc)?;

        Ok(stats)
    }

    // --------------------------------------------------------------------------------------------
    // Membership Migration Operations
    // --------------------------------------------------------------------------------------------

    pub async fn migrate_membership_plan(&self) -> Result<MigrationStats, CliError> {
        plan_membership_migration(self.storage_wiring.blob_ref_index().as_ref())
            .await
            .map_err(CliError::Storage)
    }

    pub async fn migrate_membership_apply(&self) -> Result<MigrationStats, CliError> {
        let stats = apply_membership_migration(self.storage_wiring.blob_mutation().as_ref())
            .await
            .map_err(CliError::Storage)?;

        // Mark repository membership Ready upon successful durable migration
        self.storage_wiring
            .membership_reader()
            .mark_membership_ready()
            .await
            .map_err(CliError::Storage)?;

        Ok(stats)
    }

    pub async fn migrate_membership_verify(&self) -> Result<bool, CliError> {
        verify_membership_migration(self.storage_wiring.blob_mutation().as_ref())
            .await
            .map_err(CliError::Storage)
    }

    // --------------------------------------------------------------------------------------------
    // Lock Inspection Operation
    // --------------------------------------------------------------------------------------------

    pub async fn inspect_lock(
        &self,
    ) -> Result<Option<(DeploymentWriterLockDoc, Option<String>)>, CliError> {
        inspect_deployment_writer_lock(self.storage_wiring.cluster_lock().as_ref())
            .await
            .map_err(CliError::Storage)
    }

    // --------------------------------------------------------------------------------------------
    // Break-Glass Administrative Recovery (No MaintenanceRuntime instance)
    // --------------------------------------------------------------------------------------------

    pub async fn admin_clear_lock(
        config: &Config,
        expected_owner: &str,
        expected_etag: &str,
        confirm: &str,
    ) -> Result<(), CliError> {
        Self::admin_clear_lock_with_storage_factory(
            config,
            expected_owner,
            expected_etag,
            confirm,
            storage::storage_wiring_try_from_config,
        )
        .await
    }

    pub(crate) async fn admin_clear_lock_with_storage_factory<F>(
        config: &Config,
        expected_owner: &str,
        expected_etag: &str,
        confirm: &str,
        storage_factory: F,
    ) -> Result<(), CliError>
    where
        F: FnOnce(&Config) -> Result<StorageWiring, StorageError> + Send + 'static,
    {
        let wiring =
            storage::storage_wiring_try_from_config_async_with_factory(config, storage_factory)
                .await
                .map_err(CliError::Storage)?;
        if confirm == "CONFIRM-CLEAR-ABANDONED-WRITER" {
            admin_clear_abandoned_deployment_writer_lock(
                wiring.cluster_lock().as_ref(),
                expected_owner,
                expected_etag,
                confirm,
            )
            .await
            .map_err(|e| CliError::AdminClearLock(e.to_string()))
        } else if confirm == "FORCE" {
            force_unlock_deployment_writer(wiring.cluster_lock().as_ref(), confirm)
                .await
                .map_err(|e| CliError::AdminClearLock(e.to_string()))
        } else {
            Err(CliError::AdminClearLock(
                "destructive lock clearing requires exact confirmation token 'CONFIRM-CLEAR-ABANDONED-WRITER' or 'FORCE'".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::storage::StorageErrorKind;
    use tempfile::TempDir;

    fn create_test_config(temp: &TempDir) -> Config {
        let mut cfg = Config::from_env().expect("config from env");
        cfg.storage_backend = StorageBackend::Filesystem;
        cfg.fs_root = temp.path().join("storage_root");
        cfg.max_upload_bytes = 10 * 1024 * 1024;
        cfg
    }

    #[tokio::test]
    async fn test_maintenance_runtime_acquire_offloads_filesystem_storage_to_blocking_thread() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));
        let calling_thread_id = std::thread::current().id();
        let (worker_tx, worker_rx) = tokio::sync::oneshot::channel();

        let mut runtime = MaintenanceRuntime::acquire_with_storage_factory(
            cfg.clone(),
            CommandPolicy::ReadOnly,
            move |c| {
                let current_id = std::thread::current().id();
                let _ = worker_tx.send(current_id);
                crate::storage::storage_wiring_try_from_config(c)
            },
        )
        .await
        .expect("maintenance acquire must succeed");

        let construction_thread_id = worker_rx.await.expect("worker thread id must be sent");
        assert_ne!(
            calling_thread_id, construction_thread_id,
            "filesystem storage construction in MaintenanceRuntime::acquire must execute on a separate blocking thread"
        );
        assert!(runtime.release_authority().await.is_ok());
    }

    #[tokio::test]
    async fn test_admin_clear_lock_offloads_filesystem_storage_to_blocking_thread() {
        let temp = TempDir::new().unwrap();
        let cfg = create_test_config(&temp);
        let calling_thread_id = std::thread::current().id();
        let (worker_tx, worker_rx) = tokio::sync::oneshot::channel();

        let res = MaintenanceRuntime::admin_clear_lock_with_storage_factory(
            &cfg,
            "owner",
            "etag",
            "INVALID_CONFIRM",
            move |c| {
                let current_id = std::thread::current().id();
                let _ = worker_tx.send(current_id);
                crate::storage::storage_wiring_try_from_config(c)
            },
        )
        .await;

        let construction_thread_id = worker_rx.await.expect("worker thread id must be sent");
        assert_ne!(
            calling_thread_id, construction_thread_id,
            "filesystem storage construction in admin_clear_lock must execute on a separate blocking thread"
        );
        assert!(matches!(res, Err(CliError::AdminClearLock(_))));
    }

    #[tokio::test]
    async fn test_maintenance_acquire_awaits_construction_before_authority_acquisition() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        struct ReleaseGuard(Option<std::sync::mpsc::Sender<()>>);
        impl Drop for ReleaseGuard {
            fn drop(&mut self) {
                if let Some(tx) = self.0.take() {
                    let _ = tx.send(());
                }
            }
        }
        let guard = ReleaseGuard(Some(release_tx));

        let cfg_clone = cfg.clone();
        let acquire_handle = tokio::spawn(async move {
            MaintenanceRuntime::acquire_with_storage_factory(
                cfg_clone,
                CommandPolicy::ExclusiveInspection {
                    lock_suffix: "test_await",
                },
                move |c| {
                    let _ = started_tx.send(());
                    if let Err(err) = release_rx.recv_timeout(std::time::Duration::from_secs(5)) {
                        panic!("worker wait failed: {err}");
                    }
                    crate::storage::storage_wiring_try_from_config(c)
                },
            )
            .await
        });

        started_rx.await.expect("worker must start");
        assert!(
            !acquire_handle.is_finished(),
            "acquire must not complete while construction is pending"
        );

        drop(guard);

        let mut runtime = acquire_handle
            .await
            .expect("join handle must succeed")
            .expect("maintenance acquire must succeed after release");
        assert!(runtime.release_authority().await.is_ok());
    }

    #[tokio::test]
    async fn test_maintenance_acquire_factory_failure_preserves_error() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));

        let res =
            MaintenanceRuntime::acquire_with_storage_factory(cfg, CommandPolicy::ReadOnly, |_| {
                Err(StorageError::permission_denied("denied by custom factory"))
            })
            .await;

        match res {
            Err(CliError::Storage(err)) => {
                assert_eq!(
                    err.internal_kind(),
                    Some(StorageErrorKind::PermissionDenied)
                );
                assert!(err.to_string().contains("denied by custom factory"));
            }
            Err(other) => {
                panic!("expected CliError::Storage with PermissionDenied, got other error: {other}")
            }
            Ok(_) => panic!("expected CliError::Storage with PermissionDenied, got Ok"),
        }
    }

    #[tokio::test]
    async fn test_maintenance_acquire_blocking_task_failure_maps_to_backend() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));

        let res =
            MaintenanceRuntime::acquire_with_storage_factory(cfg, CommandPolicy::ReadOnly, |_| {
                panic!("simulated blocking worker panic")
            })
            .await;

        match res {
            Err(CliError::Storage(err)) => {
                assert_eq!(err.internal_kind(), Some(StorageErrorKind::Backend));
                assert!(
                    err.to_string()
                        .contains("filesystem storage initialization task failed")
                );
            }
            Err(other) => {
                panic!("expected CliError::Storage with Backend, got other error: {other}")
            }
            Ok(_) => panic!("expected CliError::Storage with Backend, got Ok"),
        }
    }

    #[tokio::test]
    async fn test_maintenance_acquire_s3_retains_existing_construction_path() {
        let temp = TempDir::new().unwrap();
        let mut cfg = create_test_config(&temp);
        cfg.storage_backend = StorageBackend::S3;
        cfg.s3_endpoint = Some("http://localhost:9000".to_string());
        cfg.s3_region = Some("us-east-1".to_string());
        cfg.s3_bucket = Some("test-bucket".to_string());
        cfg.s3_prefix = "test".to_string();
        let cfg = Arc::new(cfg);

        let calling_thread_id = std::thread::current().id();
        let (worker_tx, worker_rx) = tokio::sync::oneshot::channel();

        let _res = MaintenanceRuntime::acquire_with_storage_factory(
            cfg,
            CommandPolicy::ReadOnly,
            move |c| {
                let current_id = std::thread::current().id();
                let _ = worker_tx.send(current_id);
                crate::storage::storage_wiring_try_from_config(c)
            },
        )
        .await;

        let construction_thread_id = worker_rx.await.expect("worker thread id must be sent");
        assert_eq!(
            calling_thread_id, construction_thread_id,
            "S3 storage construction must execute synchronously on the calling thread without spawn_blocking"
        );
    }
}
