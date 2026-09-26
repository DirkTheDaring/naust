use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::app_state::{AppState, AuthMetrics};
use crate::application::{
    BlobMutationService, BlobReadService, CatalogQueryService, ManifestMutationService,
    ManifestReadService, ProxyTarget, ReferrersQueryService, TagQueryService,
};
use crate::blob_ref_index::BlobRefIndex;
use crate::config::{Config, StorageBackend};
use crate::consistency::ConsistencyCoordinator;
use crate::gc_service::GcService;
use crate::ip_concurrency::IpConcurrencyLimiter;
use crate::storage::facade::StorageWiringFacade;
use crate::storage::mutation_authority::RuntimeMutationAuthority;
use crate::storage::{StorageError, StorageWiring};
use crate::upload_coordinator::BlobUploadCoordinatorConfig;

/// Strongly typed errors that can occur during server runtime dependency graph construction.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RuntimeBuildError {
    #[error("storage initialization failed: {0}")]
    Storage(#[from] crate::storage::StorageError),

    #[error("failed to acquire deployment writer mutation authority: {0}")]
    Authority(#[source] crate::storage::StorageError),

    #[error("failed to inspect repository-scoped blob membership readiness: {0}")]
    MembershipInspection(#[source] crate::storage::StorageError),

    #[error("failed to initialize membership marker on fresh storage: {0}")]
    MembershipInit(#[source] crate::storage::StorageError),

    #[error(
        "FATAL: Storage contains existing data (repositories, blobs, uploads, or legacy markers) but repository-scoped blob membership is not initialized.\n\
         Silent fallback to global visibility is disabled for security and tenant isolation.\n\
         Please run: `registry-rust migrate-membership apply` to backfill membership records before starting the server."
    )]
    MembershipBackfillRequired,

    #[error("reference index open failed at {path}: {source}")]
    IndexOpen {
        path: PathBuf,
        #[source]
        source: crate::blob_ref_index::RefIndexError,
    },

    #[error("reference index initialization failed at {path}: {source}")]
    IndexInit {
        path: PathBuf,
        #[source]
        source: crate::blob_ref_index::RefIndexError,
    },

    #[error("proxy upstream {index} initialization failed: {message}")]
    ProxyUpstreamInit { index: usize, message: String },

    #[error("proxy initialization failed: {message}")]
    ProxyInit { message: String },

    #[error("proxy cache storage construction failed: {0}")]
    ProxyCache(#[source] crate::storage::StorageError),

    #[error("startup phase hook '{phase:?}' failed: {message}")]
    PhaseHook {
        phase: crate::supervisor::StartupPhase,
        message: String,
    },

    #[error(
        "runtime build failed ({source}) and mutation authority release during unwinding also failed: {release_error}"
    )]
    UnwindFailed {
        #[source]
        source: Box<RuntimeBuildError>,
        release_error: crate::storage::StorageError,
    },
}

/// A bundle of all pure application services constructed for the runtime.
pub(crate) struct ApplicationServices {
    pub blob_service: Arc<BlobMutationService>,
    pub manifest_service: Arc<ManifestMutationService>,
    pub blob_read_service: Arc<BlobReadService>,
    pub manifest_read_service: Arc<ManifestReadService>,
    pub catalog_query_service: Arc<CatalogQueryService>,
    pub tag_query_service: Arc<TagQueryService>,
    pub referrers_query_service: Arc<ReferrersQueryService>,
}

/// Central authoritative factory for constructing the seven application services.
pub(crate) fn assemble_application_services(
    config: &Config,
    storage_wiring: &StorageWiring,
    ref_index: Option<Arc<BlobRefIndex>>,
    consistency: &ConsistencyCoordinator,
    buffered_body_sem: Option<Arc<Semaphore>>,
) -> ApplicationServices {
    let upload_coord_config = BlobUploadCoordinatorConfig {
        signing_key: config
            .token_signing_keys
            .first()
            .map(|k| k.key.as_bytes().to_vec())
            .unwrap_or_else(|| b"registry-rust-state-secret".to_vec()),
        max_upload_bytes: config.max_upload_bytes,
        abort_on_digest_mismatch: config.upload_policy.abort_on_digest_mismatch,
        disallow_monolithic_uploads: config.disallow_monolithic_uploads,
        upload_chunk_min_bytes: config.upload_chunk_min_bytes.map(|v| v as u64),
        gc_pin_duration_secs: config.gc_pin_duration_secs,
        finalize_grace_secs: config.blob_gc_finalize_grace_secs,
    };

    let blob_service = Arc::new(BlobMutationService::new(
        storage_wiring.blob_mutation(),
        ref_index.clone(),
        consistency.clone(),
        upload_coord_config,
    ));
    let manifest_service = Arc::new(ManifestMutationService::new(
        storage_wiring.manifest_lifecycle(),
        ref_index.clone(),
        consistency.clone(),
    ));
    let storage_facade = StorageWiringFacade::new(storage_wiring.clone());
    let blob_read_service = Arc::new(BlobReadService::new(
        storage_facade.blob_reader(),
        storage_wiring.membership_reader(),
        blob_service.clone(),
    ));
    let manifest_read_service = Arc::new(ManifestReadService::new(
        storage_wiring.manifest_reader(),
        storage_wiring.tag_reader(),
        manifest_service.clone(),
        config.max_request_body_bytes,
        buffered_body_sem,
    ));
    let catalog_query_service = Arc::new(CatalogQueryService::new(
        storage_wiring.catalog_reader(),
        storage_wiring.tag_reader(),
        storage_wiring.manifest_reader(),
        storage_wiring.blob_reader(),
    ));
    let tag_query_service = Arc::new(TagQueryService::new(storage_wiring.tag_reader()));
    let referrers_query_service = Arc::new(ReferrersQueryService::new(
        storage_wiring.referrers_reader(),
    ));

    ApplicationServices {
        blob_service,
        manifest_service,
        blob_read_service,
        manifest_read_service,
        catalog_query_service,
        tag_query_service,
        referrers_query_service,
    }
}

/// Fully assembled server runtime holding application state and managing lifecycle termination.
#[derive(Clone)]
pub(crate) struct ServerRuntime {
    app_state: AppState,
    ref_index: Option<Arc<BlobRefIndex>>,
    mutation_authority: Arc<tokio::sync::Mutex<Option<RuntimeMutationAuthority>>>,
}

impl std::fmt::Debug for ServerRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerRuntime")
            .field("ref_index_enabled", &self.ref_index.is_some())
            .finish()
    }
}

impl ServerRuntime {
    /// Returns a reference to the fully assembled `AppState`.
    pub(crate) fn app_state(&self) -> &AppState {
        &self.app_state
    }

    /// Synchronously flushes durable reference index state during shutdown Step 6.
    pub(crate) fn flush_for_shutdown(&self) -> Result<(), String> {
        if let Some(idx) = &self.ref_index {
            idx.flush().map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Asynchronously releases the deployment writer mutation authority during shutdown Step 7.
    pub(crate) async fn release_mutation_authority(
        &self,
    ) -> Result<(), crate::storage::StorageError> {
        let mut guard = self.mutation_authority.lock().await;
        if let Some(mut authority) = guard.take() {
            authority.release().await?;
        }
        Ok(())
    }
}

/// Helper that releases mutation authority upon a build failure, preserving both the original error
/// and any release failure without discarding either.
async fn unwind_and_fail(
    mut authority: RuntimeMutationAuthority,
    error: RuntimeBuildError,
) -> RuntimeBuildError {
    if let Err(release_err) = authority.release().await {
        tracing::error!(
            "failed to release mutation authority during runtime build failure unwinding: {}",
            release_err
        );
        RuntimeBuildError::UnwindFailed {
            source: Box::new(error),
            release_error: release_err,
        }
    } else {
        error
    }
}

/// Helper for unwinding when `ServerRuntime` was already constructed before a final phase hook failure.
async fn unwind_runtime_and_fail(
    runtime: &ServerRuntime,
    error: RuntimeBuildError,
) -> RuntimeBuildError {
    if let Err(release_err) = runtime.release_mutation_authority().await {
        tracing::error!(
            "failed to release runtime mutation authority during app state hook unwinding: {}",
            release_err
        );
        RuntimeBuildError::UnwindFailed {
            source: Box::new(error),
            release_error: release_err,
        }
    } else {
        error
    }
}

/// Helper to initialize storage wiring for `build_server_runtime`.
///
/// For the filesystem backend, construction is offloaded to Tokio's blocking thread pool via
/// `tokio::task::spawn_blocking` to ensure that synchronous directory validation (`ensure_dir`)
/// and storage initialization do not stall the async executor worker thread.
///
/// Note on cancellation: Dropping or cancelling the awaiter future does not terminate or abort
/// an already-running blocking task in the OS thread pool; the blocking thread will continue
/// executing until the kernel filesystem call finishes or the process exits.
pub(crate) async fn init_server_storage_wiring<F>(
    config: Arc<Config>,
    storage_factory: F,
) -> Result<StorageWiring, RuntimeBuildError>
where
    F: FnOnce(&Config) -> Result<StorageWiring, StorageError> + Send + 'static,
{
    crate::storage_wiring::storage_wiring_try_from_config_async_with_factory(
        config.as_ref(),
        storage_factory,
    )
    .await
    .map_err(RuntimeBuildError::Storage)
}

/// Assembles the production `ServerRuntime` from configuration with failure unwinding.
pub(crate) async fn build_server_runtime(
    config: Arc<Config>,
    injector: Option<Arc<dyn crate::supervisor::SupervisorFaultInjector>>,
) -> Result<ServerRuntime, RuntimeBuildError> {
    build_server_runtime_with_storage_factory(
        config,
        injector,
        crate::storage_wiring::storage_wiring_try_from_config,
    )
    .await
}

/// Assembles `ServerRuntime` using a pluggable storage factory for startup coordination testing.
pub(crate) async fn build_server_runtime_with_storage_factory<F>(
    config: Arc<Config>,
    injector: Option<Arc<dyn crate::supervisor::SupervisorFaultInjector>>,
    storage_factory: F,
) -> Result<ServerRuntime, RuntimeBuildError>
where
    F: FnOnce(&Config) -> Result<StorageWiring, StorageError> + Send + 'static,
{
    build_server_runtime_with_factories(
        config,
        injector,
        storage_factory,
        crate::storage_wiring::proxy_cache_storage_try_from_config,
    )
    .await
}

/// Assembles `ServerRuntime` using pluggable storage and proxy cache factories for startup testing.
pub(crate) async fn build_server_runtime_with_factories<F, P>(
    config: Arc<Config>,
    injector: Option<Arc<dyn crate::supervisor::SupervisorFaultInjector>>,
    storage_factory: F,
    proxy_cache_factory: P,
) -> Result<ServerRuntime, RuntimeBuildError>
where
    F: FnOnce(&Config) -> Result<StorageWiring, StorageError> + Send + 'static,
    P: Fn(
            &Config,
            Option<&crate::config::ProxyUpstreamRoute>,
        ) -> Result<Arc<dyn crate::storage::ports::ProxyStoragePort>, StorageError>
        + Send
        + Sync
        + 'static,
{
    let injector = injector.unwrap_or_else(|| Arc::new(crate::supervisor::NoopFaultInjector));
    let proxy_cache_factory = Arc::new(proxy_cache_factory);

    let storage_wiring = init_server_storage_wiring(config.clone(), storage_factory).await?;
    injector.record_event("storage_initialized").await;
    if let Err(msg) = injector
        .on_phase(crate::supervisor::StartupPhase::StorageInitialized)
        .await
    {
        return Err(RuntimeBuildError::PhaseHook {
            phase: crate::supervisor::StartupPhase::StorageInitialized,
            message: msg,
        });
    }

    let mutation_authority =
        RuntimeMutationAuthority::acquire(storage_wiring.cluster_lock(), "server")
            .await
            .map_err(RuntimeBuildError::Authority)?;
    injector.record_event("authority_acquired").await;
    if let Err(msg) = injector
        .on_phase(crate::supervisor::StartupPhase::AuthorityAcquired)
        .await
    {
        return Err(unwind_and_fail(
            mutation_authority,
            RuntimeBuildError::PhaseHook {
                phase: crate::supervisor::StartupPhase::AuthorityAcquired,
                message: msg,
            },
        )
        .await);
    }

    // Fail-closed repository blob membership startup check using backend capability emptiness
    match storage_wiring
        .membership_reader()
        .is_membership_ready()
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            let is_empty = storage_wiring
                .readiness_inspector()
                .is_storage_empty()
                .await;
            match is_empty {
                Ok(true) => {
                    if let Err(e) = storage_wiring
                        .membership_reader()
                        .mark_membership_ready()
                        .await
                    {
                        return Err(unwind_and_fail(
                            mutation_authority,
                            RuntimeBuildError::MembershipInit(e),
                        )
                        .await);
                    }
                }
                Ok(false) => {
                    return Err(unwind_and_fail(
                        mutation_authority,
                        RuntimeBuildError::MembershipBackfillRequired,
                    )
                    .await);
                }
                Err(e) => {
                    return Err(unwind_and_fail(
                        mutation_authority,
                        RuntimeBuildError::MembershipInspection(e),
                    )
                    .await);
                }
            }
        }
        Err(e) => {
            return Err(unwind_and_fail(
                mutation_authority,
                RuntimeBuildError::MembershipInspection(e),
            )
            .await);
        }
    }
    injector.record_event("membership_verified").await;
    if let Err(msg) = injector
        .on_phase(crate::supervisor::StartupPhase::MembershipVerified)
        .await
    {
        return Err(unwind_and_fail(
            mutation_authority,
            RuntimeBuildError::PhaseHook {
                phase: crate::supervisor::StartupPhase::MembershipVerified,
                message: msg,
            },
        )
        .await);
    }

    let ref_index: Option<Arc<BlobRefIndex>> = if config.ref_index.enabled {
        match BlobRefIndex::open(config.ref_index.path.clone()) {
            Ok(idx) => {
                if let Err(err) = idx
                    .ensure_healthy_or_rebuild(
                        &storage_wiring.blob_ref_index(),
                        config.ref_index.auto_rebuild_on_corruption,
                        config.ref_index.rebuild_on_start,
                    )
                    .await
                {
                    return Err(unwind_and_fail(
                        mutation_authority,
                        RuntimeBuildError::IndexInit {
                            path: config.ref_index.path.clone(),
                            source: err,
                        },
                    )
                    .await);
                }
                Some(Arc::new(idx))
            }
            Err(err) => {
                return Err(unwind_and_fail(
                    mutation_authority,
                    RuntimeBuildError::IndexOpen {
                        path: config.ref_index.path.clone(),
                        source: err,
                    },
                )
                .await);
            }
        }
    } else {
        None
    };
    injector.record_event("index_initialized").await;
    if let Err(msg) = injector
        .on_phase(crate::supervisor::StartupPhase::IndexInitialized)
        .await
    {
        return Err(unwind_and_fail(
            mutation_authority,
            RuntimeBuildError::PhaseHook {
                phase: crate::supervisor::StartupPhase::IndexInitialized,
                message: msg,
            },
        )
        .await);
    }

    let mut proxy_upstreams: Vec<ProxyTarget> = Vec::new();
    let mut proxy_upstream_engines: Vec<Arc<crate::proxy::Proxy>> = Vec::new();
    if config.proxy.enabled && !config.proxy.upstreams.is_empty() {
        for (i, up) in config.proxy.upstreams.iter().enumerate() {
            let mut per = config.proxy.clone();
            per.upstreams = vec![];
            per.upstream_base_url = Some(up.upstream_base_url.clone());
            per.upstream_username = up.upstream_username.clone();
            per.upstream_password = up.upstream_password.clone();
            per.allowed_upstream_hosts = up.allowed_upstream_hosts.clone();
            per.allowed_repo_prefixes = up.allowed_repo_prefixes.clone();
            per.block_private_networks = up.block_private_networks;
            per.redirect_policy = up.redirect_policy;
            per.max_concurrent_upstream = up.max_concurrent_upstream;
            per.index_path = up.index_path.clone();
            per.cache_fs_root = up.cache_fs_root.clone();
            per.cache_s3_prefix = up.cache_s3_prefix.clone();
            per.max_cache_bytes = Some(up.max_cache_bytes);

            let proxy = match crate::proxy::Proxy::new(&per) {
                Ok(p) => match p {
                    Some(p) => Arc::new(p),
                    None => {
                        return Err(unwind_and_fail(
                            mutation_authority,
                            RuntimeBuildError::ProxyUpstreamInit {
                                index: i,
                                message: "proxy upstream enabled but returned None".to_string(),
                            },
                        )
                        .await);
                    }
                },
                Err(err) => {
                    return Err(unwind_and_fail(
                        mutation_authority,
                        RuntimeBuildError::ProxyUpstreamInit {
                            index: i,
                            message: err.to_string(),
                        },
                    )
                    .await);
                }
            };

            let pcf = proxy_cache_factory.clone();
            let cache =
                match crate::storage_wiring::proxy_cache_storage_try_from_config_async_with_factory(
                    config.as_ref(),
                    Some(up),
                    move |c, u| pcf(c, u),
                )
                .await
                {
                    Ok(c) => c,
                    Err(err) => {
                        return Err(unwind_and_fail(
                            mutation_authority,
                            RuntimeBuildError::ProxyCache(err),
                        )
                        .await);
                    }
                };

            proxy_upstream_engines.push(proxy.clone());
            proxy_upstreams.push(ProxyTarget {
                proxy,
                cache_storage: cache,
            });
        }
    }

    let proxy = if config.proxy.enabled && config.proxy.upstreams.is_empty() {
        match crate::proxy::Proxy::new(&config.proxy) {
            Ok(p) => p.map(Arc::new),
            Err(err) => {
                return Err(unwind_and_fail(
                    mutation_authority,
                    RuntimeBuildError::ProxyInit {
                        message: err.to_string(),
                    },
                )
                .await);
            }
        }
    } else {
        None
    };

    let proxy_cache: Option<Arc<dyn crate::storage::ports::ProxyStoragePort>> =
        if config.proxy.enabled && config.proxy.upstreams.is_empty() {
            let pcf = proxy_cache_factory.clone();
            match crate::storage_wiring::proxy_cache_storage_try_from_config_async_with_factory(
                config.as_ref(),
                None,
                move |c, u| pcf(c, u),
            )
            .await
            {
                Ok(c) => Some(c),
                Err(err) => {
                    return Err(unwind_and_fail(
                        mutation_authority,
                        RuntimeBuildError::ProxyCache(err),
                    )
                    .await);
                }
            }
        } else {
            None
        };

    let buffered_body_sem = Arc::new(Semaphore::new(
        config.max_concurrent_buffered_requests.max(1),
    ));
    let request_sem = Arc::new(Semaphore::new(config.max_concurrent_requests.max(1)));
    let upload_request_sem = Arc::new(Semaphore::new(config.max_concurrent_upload_requests.max(1)));
    let consistency = ConsistencyCoordinator::new();
    let mutation_authority_arc = Arc::new(tokio::sync::Mutex::new(Some(mutation_authority)));

    let gc_service = match &ref_index {
        Some(idx) => Some(Arc::new(GcService::with_coordinator_and_authority(
            Arc::new(crate::policy::GcPolicy::from(config.as_ref())),
            storage_wiring.gc_service_port(),
            idx.clone(),
            consistency.clone(),
            mutation_authority_arc.clone(),
        ))),
        None => None,
    };

    if config.storage_backend == StorageBackend::S3
        && config.blob_gc_enabled
        && config.blob_gc_enable_delete
    {
        if let Err(e) = storage_wiring
            .gc_port()
            .check_bucket_versioning_for_gc()
            .await
        {
            tracing::warn!(
                "S3 bucket versioning preflight check: {}; physical GC deletion will fail closed",
                e
            );
        }
    }

    let ip_limiter = Arc::new(IpConcurrencyLimiter::new(
        config.max_connections_per_ip,
        config.trusted_bypass_cidrs.clone(),
    ));
    let is_high_pressure = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let services = assemble_application_services(
        config.as_ref(),
        &storage_wiring,
        ref_index.clone(),
        &consistency,
        Some(buffered_body_sem.clone()),
    );

    let auth_metrics = Arc::new(AuthMetrics::default());
    let state = AppState {
        transfer_policy: Arc::new(crate::http_api::policy::HttpTransferPolicy::from(
            config.as_ref(),
        )),
        token_svc: Arc::new(crate::token_service::TokenService::new(
            config.clone(),
            auth_metrics.clone(),
        )),
        config: config.clone(),
        auth_metrics,
        ref_index: ref_index.clone(),
        gc_admin: Arc::new(crate::gc_admin::GcAdminService::new(
            gc_service.clone(),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            crate::gc_admin::GcAdminPolicy::from(config.as_ref()),
        )),
        gc_service,
        proxy,
        proxy_cache,
        proxy_upstreams,
        proxy_upstream_engines,
        buffered_body_sem,
        request_sem,
        upload_request_sem,
        active_non_upload_requests: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        active_upload_requests: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        last_sem_saturation_log_unix_secs: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        ip_limiter,
        is_high_pressure,
        blob_service: services.blob_service,
        manifest_service: services.manifest_service,
        blob_read_service: services.blob_read_service,
        manifest_read_service: services.manifest_read_service,
        catalog_query_service: services.catalog_query_service,
        tag_query_service: services.tag_query_service,
        referrers_query_service: services.referrers_query_service,
    };

    let runtime = ServerRuntime {
        app_state: state,
        ref_index,
        mutation_authority: mutation_authority_arc,
    };

    injector.record_event("app_state_constructed").await;
    if let Err(msg) = injector
        .on_phase(crate::supervisor::StartupPhase::AppStateConstructed)
        .await
    {
        return Err(unwind_runtime_and_fail(
            &runtime,
            RuntimeBuildError::PhaseHook {
                phase: crate::supervisor::StartupPhase::AppStateConstructed,
                message: msg,
            },
        )
        .await);
    }

    Ok(runtime)
}

/// Helper for tests constructing `AppState` through the single authoritative application service assembly.
pub(crate) fn build_test_app_state(
    cfg: Arc<Config>,
    storage_wiring: StorageWiring,
    ref_index: Option<Arc<BlobRefIndex>>,
    proxy: Option<Arc<crate::proxy::Proxy>>,
    proxy_cache: Option<Arc<dyn crate::storage::ports::ProxyStoragePort>>,
    gc_service: Option<Arc<GcService>>,
) -> AppState {
    let ip_limiter = Arc::new(IpConcurrencyLimiter::new(
        cfg.max_connections_per_ip,
        cfg.trusted_bypass_cidrs.clone(),
    ));
    let consistency = ConsistencyCoordinator::new();
    let services = assemble_application_services(
        cfg.as_ref(),
        &storage_wiring,
        ref_index.clone(),
        &consistency,
        Some(Arc::new(Semaphore::new(1))),
    );

    let gc_admin = Arc::new(crate::gc_admin::GcAdminService::new(
        gc_service.clone(),
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        crate::gc_admin::GcAdminPolicy::from(cfg.as_ref()),
    ));
    let auth_metrics = Arc::new(AuthMetrics::default());
    AppState {
        transfer_policy: Arc::new(crate::http_api::policy::HttpTransferPolicy::from(
            cfg.as_ref(),
        )),
        token_svc: Arc::new(crate::token_service::TokenService::new(
            cfg.clone(),
            auth_metrics.clone(),
        )),
        config: cfg,
        auth_metrics,
        ref_index,
        gc_admin,
        gc_service,
        proxy,
        proxy_cache,
        proxy_upstreams: Vec::new(),
        proxy_upstream_engines: Vec::new(),
        buffered_body_sem: Arc::new(Semaphore::new(1)),
        request_sem: Arc::new(Semaphore::new(1)),
        upload_request_sem: Arc::new(Semaphore::new(1)),
        active_non_upload_requests: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        active_upload_requests: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        last_sem_saturation_log_unix_secs: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        ip_limiter,
        is_high_pressure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        blob_service: services.blob_service,
        manifest_service: services.manifest_service,
        blob_read_service: services.blob_read_service,
        manifest_read_service: services.manifest_read_service,
        catalog_query_service: services.catalog_query_service,
        tag_query_service: services.tag_query_service,
        referrers_query_service: services.referrers_query_service,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageErrorKind;
    use crate::storage::ports::StorageReadinessInspector;
    use crate::supervisor::{StartupPhase, SupervisorFaultInjector};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    struct RecordingFaultInjector {
        events: Mutex<Vec<String>>,
        phases: Mutex<Vec<StartupPhase>>,
    }

    impl RecordingFaultInjector {
        fn new() -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                phases: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl SupervisorFaultInjector for RecordingFaultInjector {
        async fn record_event(&self, event: &'static str) {
            self.events.lock().await.push(event.to_string());
        }

        async fn on_phase(&self, phase: StartupPhase) -> Result<(), String> {
            self.phases.lock().await.push(phase);
            Ok(())
        }
    }

    struct FailingFaultInjector {
        fail_at_phase: StartupPhase,
    }

    impl FailingFaultInjector {
        fn new(fail_at_phase: StartupPhase) -> Self {
            Self { fail_at_phase }
        }
    }

    #[async_trait::async_trait]
    impl SupervisorFaultInjector for FailingFaultInjector {
        async fn record_event(&self, _event: &'static str) {}

        async fn on_phase(&self, phase: StartupPhase) -> Result<(), String> {
            if phase == self.fail_at_phase {
                Err(format!("injected failure at phase {phase:?}"))
            } else {
                Ok(())
            }
        }
    }

    fn create_test_config(temp: &TempDir) -> Config {
        let mut cfg = Config::from_env().unwrap();
        cfg.fs_root = temp.path().join("registry");
        cfg.listen_addr = "127.0.0.1:0".parse().unwrap();
        cfg.ref_index.path = temp.path().join("ref_index.db");
        cfg.ref_index.enabled = true;
        cfg.blob_gc_enabled = true;
        cfg.proxy.enabled = false;
        cfg
    }

    #[tokio::test]
    async fn test_filesystem_runtime_graph_construction() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));

        let injector = Arc::new(RecordingFaultInjector::new());
        let runtime = build_server_runtime(cfg.clone(), Some(injector.clone()))
            .await
            .expect("ServerRuntime must build successfully on filesystem backend");

        let state = runtime.app_state();
        assert!(state.gc_service.is_some());
        assert!(state.ref_index.is_some());

        let events = injector.events.lock().await.clone();
        assert_eq!(
            events,
            vec![
                "storage_initialized",
                "authority_acquired",
                "membership_verified",
                "index_initialized",
                "app_state_constructed",
            ]
        );

        assert!(runtime.flush_for_shutdown().is_ok());
        assert!(runtime.release_mutation_authority().await.is_ok());
    }

    #[tokio::test]
    async fn test_runtime_shared_coordinator_and_gc_service() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));

        let runtime = build_server_runtime(cfg, None)
            .await
            .expect("runtime build must succeed");

        let state = runtime.app_state();
        assert!(state.gc_service.is_some());
        assert!(state.ref_index.is_some());
        assert!(state.blob_service.start_upload("test-repo").await.is_ok());

        assert!(runtime.flush_for_shutdown().is_ok());
        assert!(runtime.release_mutation_authority().await.is_ok());
    }

    #[tokio::test]
    async fn test_preflight_and_startup_phase_ordering() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));

        let injector = Arc::new(RecordingFaultInjector::new());
        let runtime = build_server_runtime(cfg, Some(injector.clone()))
            .await
            .expect("ServerRuntime build must succeed");

        let phases = injector.phases.lock().await.clone();
        assert_eq!(
            phases,
            vec![
                StartupPhase::StorageInitialized,
                StartupPhase::AuthorityAcquired,
                StartupPhase::MembershipVerified,
                StartupPhase::IndexInitialized,
                StartupPhase::AppStateConstructed,
            ]
        );

        assert!(runtime.flush_for_shutdown().is_ok());
        assert!(runtime.release_mutation_authority().await.is_ok());
    }

    // --------------------------------------------------------------------------------------------
    // Filesystem Storage Startup Offload & Thread Boundary Tests
    // --------------------------------------------------------------------------------------------

    #[tokio::test]
    async fn test_filesystem_storage_construction_executes_off_async_thread() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));
        let calling_thread_id = std::thread::current().id();
        let (worker_tx, worker_rx) = tokio::sync::oneshot::channel();

        let runtime = build_server_runtime_with_storage_factory(cfg.clone(), None, move |c| {
            let current_id = std::thread::current().id();
            let _ = worker_tx.send(current_id);
            crate::storage_wiring::storage_wiring_try_from_config(c)
        })
        .await
        .expect("runtime build must succeed");

        let construction_thread_id = worker_rx.await.expect("worker thread id must be sent");
        assert_ne!(
            calling_thread_id, construction_thread_id,
            "filesystem storage construction must execute on a separate blocking thread"
        );

        assert!(runtime.flush_for_shutdown().is_ok());
        assert!(runtime.release_mutation_authority().await.is_ok());
    }

    #[tokio::test]
    async fn test_startup_does_not_report_initialization_before_construction_completes() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));
        let injector = Arc::new(RecordingFaultInjector::new());

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
        let injector_clone = injector.clone();
        let runtime_handle = tokio::spawn(async move {
            build_server_runtime_with_storage_factory(cfg_clone, Some(injector_clone), move |c| {
                let _ = started_tx.send(());
                // Block worker until test releases it, bounded to avoid indefinite hang if regressed
                if let Err(err) = release_rx.recv_timeout(std::time::Duration::from_secs(5)) {
                    panic!(
                        "test worker synchronization wait failed (timeout or disconnection): {err}"
                    );
                }
                crate::storage_wiring::storage_wiring_try_from_config(c)
            })
            .await
        });

        // Wait deterministically until the blocking task starts
        started_rx.await.expect("worker must signal start");

        // Verify that while construction is in progress, storage initialization has NOT been recorded
        {
            let events = injector.events.lock().await;
            assert!(
                events.is_empty(),
                "storage_initialized must not be recorded while construction is pending"
            );
        }
        assert!(
            !runtime_handle.is_finished(),
            "runtime build task must not complete before construction is finished"
        );

        // Release the worker deterministically
        drop(guard);

        let runtime = runtime_handle
            .await
            .expect("join handle must succeed")
            .expect("runtime build must succeed after release");

        // Verify that after completion, storage initialization was recorded
        {
            let events = injector.events.lock().await;
            assert_eq!(
                events.first().map(|s| s.as_str()),
                Some("storage_initialized"),
                "storage_initialized must be the first recorded event after completion"
            );
        }

        assert!(runtime.flush_for_shutdown().is_ok());
        assert!(runtime.release_mutation_authority().await.is_ok());
    }

    #[tokio::test]
    async fn test_filesystem_startup_returns_usable_storage_wiring() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));

        let runtime = build_server_runtime(cfg, None)
            .await
            .expect("build_server_runtime must succeed on valid filesystem config");

        let state = runtime.app_state();
        let start_res = state
            .blob_service
            .start_upload("startup-test-repo")
            .await
            .expect("storage wiring must be fully usable for upload operations");
        assert!(!start_res.session.uuid.is_empty());

        assert!(runtime.flush_for_shutdown().is_ok());
        assert!(runtime.release_mutation_authority().await.is_ok());
    }

    #[tokio::test]
    async fn test_real_filesystem_construction_failure_preserves_storage_error() {
        let temp = TempDir::new().unwrap();
        let mut cfg = create_test_config(&temp);

        // Create a regular file where the storage directory should be, causing ensure_dir to fail
        let blocking_file = temp.path().join("blocking_file.txt");
        tokio::fs::write(&blocking_file, b"cannot-create-dir-under-file")
            .await
            .unwrap();
        cfg.fs_root = blocking_file.join("sub_storage_dir");
        let cfg = Arc::new(cfg);

        // Obtain expected error directly from existing synchronous factory on the same fixture
        let expected_err = match crate::storage_wiring::storage_wiring_try_from_config(cfg.as_ref())
        {
            Err(err) => err,
            Ok(_) => panic!("synchronous factory must fail on invalid root fixture"),
        };

        let build_res = build_server_runtime(cfg, None).await;
        match build_res {
            Err(RuntimeBuildError::Storage(actual_err)) => match (&actual_err, &expected_err) {
                (
                    StorageError::Internal {
                        kind: actual_kind,
                        message: actual_msg,
                    },
                    StorageError::Internal {
                        kind: expected_kind,
                        message: expected_msg,
                    },
                ) => {
                    assert_eq!(
                        actual_kind, expected_kind,
                        "StorageErrorKind must match synchronous factory failure exactly"
                    );
                    assert_eq!(
                        actual_msg, expected_msg,
                        "StorageError message must match synchronous factory failure exactly"
                    );
                }
                _ => panic!(
                    "expected StorageError::Internal matching {expected_err:?}, got: {actual_err:?}"
                ),
            },
            other => panic!(
                "expected RuntimeBuildError::Storage matching {expected_err:?}, got {other:?}"
            ),
        }
    }

    #[tokio::test]
    async fn test_blocking_task_panic_fixture_maps_to_backend() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));
        let injector = Arc::new(RecordingFaultInjector::new());

        // Note: This fixture tests runtime JoinError translation upon a task panic;
        // it does not claim that a real filesystem syscall panicked.
        let build_res =
            build_server_runtime_with_storage_factory(cfg, Some(injector.clone()), |_c| {
                panic!("deliberate test panic in blocking storage factory");
            })
            .await;

        match build_res {
            Err(RuntimeBuildError::Storage(StorageError::Internal { kind, message })) => {
                assert_eq!(
                    kind,
                    crate::storage::StorageErrorKind::Backend,
                    "JoinError from panicked task must map to StorageErrorKind::Backend"
                );
                assert!(
                    message.contains("filesystem storage initialization task failed"),
                    "diagnostic message must describe task failure, got: {message}"
                );
            }
            other => {
                panic!("expected RuntimeBuildError::Storage(Internal(Backend)), got {other:?}")
            }
        }

        let events = injector.events.lock().await;
        assert!(
            !events.contains(&"storage_initialized".to_string()),
            "storage_initialized must not be recorded if storage construction panics"
        );
    }

    // --------------------------------------------------------------------------------------------
    // REQUIREMENT 1: Fail-Closed Emptiness Preflight Test Suite
    // --------------------------------------------------------------------------------------------

    #[tokio::test]
    async fn test_preflight_succeeds_on_truly_empty_fs_storage() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));

        let wiring = crate::storage_wiring::storage_wiring_from_config(cfg.as_ref());
        assert!(
            wiring
                .readiness_inspector()
                .is_storage_empty()
                .await
                .unwrap(),
            "new temp directory must be detected as empty"
        );

        let runtime = build_server_runtime(cfg.clone(), None)
            .await
            .expect("fresh empty store must auto-initialize membership");

        // Verify marker was written
        assert!(
            wiring
                .membership_reader()
                .is_membership_ready()
                .await
                .unwrap(),
            "membership marker must be initialized on fresh store"
        );

        assert!(runtime.release_mutation_authority().await.is_ok());
    }

    #[tokio::test]
    async fn test_preflight_fails_on_unlinked_cas_blob_existing() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));

        // Create an unlinked CAS blob directory and file without ready marker
        let blobs_dir = cfg.fs_root.join("blobs/sha256/ea");
        tokio::fs::create_dir_all(&blobs_dir).await.unwrap();
        tokio::fs::write(
            blobs_dir.join("ea020102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e"),
            b"test-blob-payload",
        )
        .await
        .unwrap();

        let wiring = crate::storage_wiring::storage_wiring_from_config(cfg.as_ref());
        assert!(
            !wiring
                .readiness_inspector()
                .is_storage_empty()
                .await
                .unwrap(),
            "storage with blob must not be empty"
        );

        let build_res = build_server_runtime(cfg.clone(), None).await;
        match build_res {
            Err(RuntimeBuildError::MembershipBackfillRequired) => {}
            other => panic!("expected MembershipBackfillRequired, got {:?}", other),
        }

        // Verify marker was NOT written and authority can be acquired
        assert!(
            !wiring
                .membership_reader()
                .is_membership_ready()
                .await
                .unwrap()
        );
        let mut auth = RuntimeMutationAuthority::acquire(wiring.cluster_lock(), "reacquire-check")
            .await
            .expect("authority must be freed after failure");
        assert!(auth.release().await.is_ok());
    }

    #[tokio::test]
    async fn test_preflight_fails_on_upload_session_existing() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));

        // Create an upload session file without ready marker
        let uploads_dir = cfg.fs_root.join("uploads");
        tokio::fs::create_dir_all(&uploads_dir).await.unwrap();
        tokio::fs::write(uploads_dir.join("test-session-uuid.data"), b"partial-data")
            .await
            .unwrap();

        let wiring = crate::storage_wiring::storage_wiring_from_config(cfg.as_ref());
        assert!(
            !wiring
                .readiness_inspector()
                .is_storage_empty()
                .await
                .unwrap(),
            "storage with uploads must not be empty"
        );

        let build_res = build_server_runtime(cfg.clone(), None).await;
        match build_res {
            Err(RuntimeBuildError::MembershipBackfillRequired) => {}
            other => panic!("expected MembershipBackfillRequired, got {:?}", other),
        }

        // Verify marker was NOT written and authority can be acquired
        assert!(
            !wiring
                .membership_reader()
                .is_membership_ready()
                .await
                .unwrap()
        );
        let mut auth = RuntimeMutationAuthority::acquire(wiring.cluster_lock(), "reacquire-check")
            .await
            .expect("authority must be freed after failure");
        assert!(auth.release().await.is_ok());
    }

    #[tokio::test]
    async fn test_preflight_fails_when_membership_ready_inspection_errors() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));

        // Make root directory a file to cause I/O error during membership inspection
        tokio::fs::write(&cfg.fs_root, b"not-a-directory")
            .await
            .unwrap();

        let build_res = build_server_runtime(cfg.clone(), None).await;
        match build_res {
            Err(RuntimeBuildError::MembershipInspection(_))
            | Err(RuntimeBuildError::Storage(_))
            | Err(RuntimeBuildError::Authority(_)) => {}
            other => panic!(
                "expected MembershipInspection/Storage/Authority error, got {:?}",
                other
            ),
        }
    }

    #[tokio::test]
    async fn test_preflight_fails_when_readiness_inspector_errors_after_not_ready() {
        let temp = TempDir::new().unwrap();
        let cfg = Arc::new(create_test_config(&temp));

        // Create repos dir as a file to cause I/O error during readiness inspection
        let repos_path = cfg.fs_root.join("repos");
        tokio::fs::create_dir_all(&cfg.fs_root).await.unwrap();
        tokio::fs::write(&repos_path, b"not-a-dir").await.unwrap();

        let wiring = crate::storage_wiring::storage_wiring_from_config(cfg.as_ref());
        let inspect_res = wiring.readiness_inspector().is_storage_empty().await;
        assert!(inspect_res.is_err(), "I/O error must propagate as Err");

        let build_res = build_server_runtime(cfg.clone(), None).await;
        match build_res {
            Err(RuntimeBuildError::MembershipInspection(_))
            | Err(RuntimeBuildError::Storage(_)) => {}
            other => panic!("expected MembershipInspection, got {:?}", other),
        }

        // Verify marker was NOT written and authority can be reacquired
        assert!(
            !wiring
                .membership_reader()
                .is_membership_ready()
                .await
                .unwrap_or(false)
        );
        let mut auth = RuntimeMutationAuthority::acquire(
            wiring.cluster_lock(),
            "reacquire-check-inspector-err",
        )
        .await
        .expect("authority must be freed after failure");
        assert!(auth.release().await.is_ok());
    }

    #[tokio::test]
    async fn test_preflight_s3_exact_lock_key_alone_is_empty() {
        use crate::storage::s3::S3Storage;
        use crate::storage::s3::mock::MockS3Driver;

        // 1. Unprefixed storage with exact lock key
        let driver = Arc::new(MockS3Driver::new(1000));
        {
            let mut objs = driver.objects.lock().unwrap();
            objs.insert(
                "meta/exclusive_writer.lock".to_string(),
                (
                    bytes::Bytes::from(b"lock-doc".to_vec()),
                    "\"etag-1\"".to_string(),
                ),
            );
        }
        let storage = S3Storage::new_with_driver(
            Some("test-bucket".to_string()),
            "".to_string(),
            100 * 1024 * 1024,
            driver,
        );
        assert!(
            storage.is_storage_empty().await.unwrap(),
            "exact lock key alone must evaluate to empty storage"
        );

        // 2. Prefixed storage with exact lock key under prefix
        let driver_prefixed = Arc::new(MockS3Driver::new(1000));
        {
            let mut objs = driver_prefixed.objects.lock().unwrap();
            objs.insert(
                "custom-prefix/meta/exclusive_writer.lock".to_string(),
                (
                    bytes::Bytes::from(b"lock-doc".to_vec()),
                    "\"etag-1\"".to_string(),
                ),
            );
        }
        let storage_prefixed = S3Storage::new_with_driver(
            Some("test-bucket".to_string()),
            "custom-prefix/".to_string(),
            100 * 1024 * 1024,
            driver_prefixed,
        );
        assert!(
            storage_prefixed.is_storage_empty().await.unwrap(),
            "prefixed exact lock key alone must evaluate to empty storage"
        );
    }

    #[tokio::test]
    async fn test_preflight_s3_lock_key_plus_ordinary_object_is_non_empty() {
        use crate::storage::s3::S3Storage;
        use crate::storage::s3::mock::MockS3Driver;

        let driver = Arc::new(MockS3Driver::new(1000));
        {
            let mut objs = driver.objects.lock().unwrap();
            objs.insert(
                "meta/exclusive_writer.lock".to_string(),
                (
                    bytes::Bytes::from(b"lock-doc".to_vec()),
                    "\"etag-1\"".to_string(),
                ),
            );
            objs.insert(
                "blobs/sha256/123".to_string(),
                (
                    bytes::Bytes::from(b"blob-content".to_vec()),
                    "\"etag-2\"".to_string(),
                ),
            );
        }
        let storage = S3Storage::new_with_driver(
            Some("test-bucket".to_string()),
            "".to_string(),
            100 * 1024 * 1024,
            driver,
        );
        assert!(
            !storage.is_storage_empty().await.unwrap(),
            "lock key plus ordinary blob must evaluate to non-empty"
        );
    }

    #[tokio::test]
    async fn test_preflight_s3_unexpected_nested_lock_is_non_empty() {
        use crate::storage::s3::S3Storage;
        use crate::storage::s3::mock::MockS3Driver;

        let driver = Arc::new(MockS3Driver::new(1000));
        {
            let mut objs = driver.objects.lock().unwrap();
            // Unexpected nested location with identical suffix
            objs.insert(
                "unexpected/exclusive_writer.lock".to_string(),
                (
                    bytes::Bytes::from(b"nested-lock".to_vec()),
                    "\"etag-nested\"".to_string(),
                ),
            );
        }
        let storage = S3Storage::new_with_driver(
            Some("test-bucket".to_string()),
            "".to_string(),
            100 * 1024 * 1024,
            driver,
        );
        assert!(
            !storage.is_storage_empty().await.unwrap(),
            "unexpected nested lock-like key must evaluate to non-empty"
        );
    }

    #[tokio::test]
    async fn test_preflight_s3_other_meta_lock_is_non_empty() {
        use crate::storage::s3::S3Storage;
        use crate::storage::s3::mock::MockS3Driver;

        let driver = Arc::new(MockS3Driver::new(1000));
        {
            let mut objs = driver.objects.lock().unwrap();
            objs.insert(
                "meta/other.lock".to_string(),
                (
                    bytes::Bytes::from(b"other-lock".to_vec()),
                    "\"etag-other\"".to_string(),
                ),
            );
        }
        let storage = S3Storage::new_with_driver(
            Some("test-bucket".to_string()),
            "".to_string(),
            100 * 1024 * 1024,
            driver,
        );
        assert!(
            !storage.is_storage_empty().await.unwrap(),
            "meta/other.lock must evaluate to non-empty"
        );
    }

    #[tokio::test]
    async fn test_preflight_s3_lock_like_object_on_page_one_aborts_non_empty() {
        use crate::storage::s3::S3Storage;
        use crate::storage::s3::mock::MockS3Driver;

        let driver = Arc::new(MockS3Driver::new(1000));
        {
            let mut objs = driver.objects.lock().unwrap();
            objs.insert(
                "meta/exclusive_writer.lock.bak".to_string(),
                (
                    bytes::Bytes::from(b"bak-doc".to_vec()),
                    "\"etag-bak\"".to_string(),
                ),
            );
        }
        let storage = S3Storage::new_with_driver(
            Some("test-bucket".to_string()),
            "".to_string(),
            100 * 1024 * 1024,
            driver,
        );
        assert!(
            !storage.is_storage_empty().await.unwrap(),
            "lock-like object on page 1 must abort immediately as non-empty"
        );
    }

    #[tokio::test]
    async fn test_preflight_s3_page_one_lock_only_and_page_two_has_data() {
        use crate::storage::s3::S3Storage;
        use crate::storage::s3::mock::MockS3Driver;

        let driver = Arc::new(MockS3Driver::new(1000));
        // Page 1: only writer lock
        {
            let mut objs = driver.objects.lock().unwrap();
            objs.insert(
                "meta/exclusive_writer.lock".to_string(),
                (
                    bytes::Bytes::from(b"lock-doc".to_vec()),
                    "\"etag-1\"".to_string(),
                ),
            );
            // Page 2: unlinked blob
            objs.insert(
                "blobs/sha256/ea/ea020102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e"
                    .to_string(),
                (
                    bytes::Bytes::from(b"blob-data".to_vec()),
                    "\"etag-2\"".to_string(),
                ),
            );
        }

        let storage = S3Storage::new_with_driver(
            Some("test-bucket".to_string()),
            "".to_string(),
            100 * 1024 * 1024,
            driver,
        );

        let is_empty = storage
            .is_storage_empty()
            .await
            .expect("is_storage_empty check");
        assert!(
            !is_empty,
            "S3 store with data on page 2 must be detected as not empty"
        );
    }

    #[tokio::test]
    async fn test_preflight_s3_page_two_request_fails() {
        use crate::storage::s3::S3Storage;
        use crate::storage::s3::mock::MockS3Driver;

        let driver = Arc::new(MockS3Driver::new(1000));
        {
            let mut objs = driver.objects.lock().unwrap();
            objs.insert(
                "meta/exclusive_writer.lock".to_string(),
                (
                    bytes::Bytes::from(b"lock-doc".to_vec()),
                    "\"etag-1\"".to_string(),
                ),
            );
        }

        driver.set_hook_before(|method, _key| {
            if method == "list_objects_v2_page" {
                Some(crate::storage::StorageError::backend(
                    "simulated S3 connection reset on page 2",
                ))
            } else {
                None
            }
        });

        let storage = S3Storage::new_with_driver(
            Some("test-bucket".to_string()),
            "".to_string(),
            100 * 1024 * 1024,
            driver,
        );

        let inspect_res = storage.is_storage_empty().await;
        assert!(inspect_res.is_err(), "page 2 failure must fail closed");
        match inspect_res {
            Err(crate::storage::StorageError::Internal { kind, message }) => {
                assert_eq!(kind, crate::storage::StorageErrorKind::Backend);
                assert!(message.contains("simulated S3 connection reset"));
            }
            other => panic!("expected StorageError::Internal, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_preflight_s3_repeated_continuation_token_fails_closed() {
        let err = crate::storage::StorageError::backend(
            "repeated S3 continuation token detected during storage readiness check",
        );
        let compound = unwind_and_fail(
            RuntimeMutationAuthority::acquire(
                crate::storage_wiring::storage_wiring_from_config(&create_test_config(
                    &TempDir::new().unwrap(),
                ))
                .cluster_lock(),
                "test-loop-token",
            )
            .await
            .unwrap(),
            RuntimeBuildError::MembershipInspection(err),
        )
        .await;

        match compound {
            RuntimeBuildError::MembershipInspection(crate::storage::StorageError::Internal {
                kind,
                message,
            }) => {
                assert_eq!(kind, crate::storage::StorageErrorKind::Backend);
                assert!(message.contains("repeated S3 continuation token"));
            }
            other => panic!("expected MembershipInspection error, got {:?}", other),
        }
    }

    // --------------------------------------------------------------------------------------------
    // REQUIREMENT 3: Authority Unwinding & Release Failure Preservation Tests
    // --------------------------------------------------------------------------------------------

    #[tokio::test]
    async fn test_failure_unwinding_releases_authority_at_each_phase() {
        let failure_phases = [
            StartupPhase::AuthorityAcquired,
            StartupPhase::MembershipVerified,
            StartupPhase::IndexInitialized,
            StartupPhase::AppStateConstructed,
        ];

        for fail_phase in failure_phases {
            let temp = TempDir::new().unwrap();
            let cfg = Arc::new(create_test_config(&temp));

            let injector = Arc::new(FailingFaultInjector::new(fail_phase));
            let build_res = build_server_runtime(cfg.clone(), Some(injector)).await;

            match build_res {
                Err(RuntimeBuildError::PhaseHook { phase, message }) => {
                    assert_eq!(phase, fail_phase);
                    assert!(message.contains("injected failure"));
                }
                other => panic!(
                    "expected PhaseHook error at {:?}, got {:?}",
                    fail_phase, other
                ),
            }

            // Verify authority was released and can be reacquired cleanly
            let wiring = crate::storage_wiring::storage_wiring_from_config(cfg.as_ref());
            let reacquired =
                RuntimeMutationAuthority::acquire(wiring.cluster_lock(), "verify_authority_freed")
                    .await;
            assert!(
                reacquired.is_ok(),
                "authority must be cleanly released when failing at {:?}",
                fail_phase
            );
            let mut a = reacquired.unwrap();
            assert!(a.release().await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_failure_unwinding_on_invalid_ref_index_path() {
        let temp = TempDir::new().unwrap();
        let mut cfg = create_test_config(&temp);
        // Point ref_index to an invalid directory path
        let uncreatable_dir = temp.path().join("file.txt");
        tokio::fs::write(&uncreatable_dir, b"already_a_file")
            .await
            .unwrap();
        cfg.ref_index.path = uncreatable_dir.join("sub/cannot_create.db");
        let cfg = Arc::new(cfg);

        let build_res = build_server_runtime(cfg.clone(), None).await;
        assert!(build_res.is_err());

        // Verify authority was freed and can be reacquired
        let wiring = crate::storage_wiring::storage_wiring_from_config(cfg.as_ref());
        let mut a = RuntimeMutationAuthority::acquire(wiring.cluster_lock(), "post-index-fail")
            .await
            .expect("authority must be released after index failure");
        assert!(a.release().await.is_ok());
    }

    #[tokio::test]
    async fn test_unwind_failed_execution_preserves_both_errors() {
        struct FailingReleaseLockStore;

        #[async_trait::async_trait]
        impl crate::storage::ports::ClusterLockStore for FailingReleaseLockStore {
            async fn acquire_deployment_writer_lock(
                &self,
                _doc: &crate::storage::mutation_authority::DeploymentWriterLockDoc,
            ) -> Result<(bool, Option<String>), crate::storage::StorageError> {
                Ok((true, Some("etag-123".to_string())))
            }

            async fn release_deployment_writer_lock(
                &self,
                _doc: &crate::storage::mutation_authority::DeploymentWriterLockDoc,
                _expected_etag: Option<&str>,
            ) -> Result<bool, crate::storage::StorageError> {
                Err(crate::storage::StorageError::backend(
                    "simulated lease release failure",
                ))
            }

            async fn inspect_deployment_writer_lock(
                &self,
            ) -> Result<
                Option<(
                    crate::storage::mutation_authority::DeploymentWriterLockDoc,
                    Option<String>,
                )>,
                crate::storage::StorageError,
            > {
                Ok(None)
            }

            async fn admin_clear_deployment_writer_lock(
                &self,
                _expected_owner: &str,
                _expected_etag: &str,
            ) -> Result<(), crate::storage::StorageError> {
                Ok(())
            }
        }

        let store: Arc<dyn crate::storage::ports::ClusterLockStore> =
            Arc::new(FailingReleaseLockStore);
        let authority = RuntimeMutationAuthority::acquire(store, "test-failing-release")
            .await
            .expect("acquire must succeed");

        let root_err = RuntimeBuildError::MembershipBackfillRequired;
        let compound = unwind_and_fail(authority, root_err).await;

        match compound {
            RuntimeBuildError::UnwindFailed {
                source,
                release_error,
            } => {
                match *source {
                    RuntimeBuildError::MembershipBackfillRequired => {}
                    other => panic!("expected MembershipBackfillRequired root, got {:?}", other),
                }
                match release_error {
                    crate::storage::StorageError::Internal { kind, message } => {
                        assert_eq!(kind, crate::storage::StorageErrorKind::Backend);
                        assert!(message.contains("simulated lease release failure"));
                    }
                    other => panic!("expected StorageError::Internal, got {:?}", other),
                }
            }
            other => panic!("expected RuntimeBuildError::UnwindFailed, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_optional_proxy_behavior() {
        // 1. Proxy disabled
        {
            let temp = TempDir::new().unwrap();
            let mut cfg = create_test_config(&temp);
            cfg.proxy.enabled = false;
            let cfg = Arc::new(cfg);

            let runtime = build_server_runtime(cfg, None)
                .await
                .expect("runtime build with proxy disabled");
            let state = runtime.app_state();
            assert!(state.proxy.is_none());
            assert!(state.proxy_cache.is_none());
            assert!(runtime.release_mutation_authority().await.is_ok());
        }

        // 2. Single upstream proxy enabled
        {
            let temp = TempDir::new().unwrap();
            let mut cfg = create_test_config(&temp);
            cfg.proxy.enabled = true;
            cfg.proxy.upstream_base_url = Some("https://registry-1.docker.io".to_string());
            cfg.proxy.cache_fs_root = Some(temp.path().join("proxy_cache"));
            cfg.proxy.index_path = temp.path().join("proxy_index.db");
            let cfg = Arc::new(cfg);

            let runtime = build_server_runtime(cfg, None)
                .await
                .expect("runtime build with single proxy");
            let state = runtime.app_state();
            assert!(state.proxy.is_some());
            assert!(state.proxy_cache.is_some());
            assert!(runtime.release_mutation_authority().await.is_ok());
        }
    }

    // --------------------------------------------------------------------------------------------
    // REQUIREMENT 4: Required Live Mode & Genuine MinIO Verification Tests
    // --------------------------------------------------------------------------------------------

    #[tokio::test]
    async fn test_required_live_mode_missing_config_fails_nonzero() {
        // When live mode is required but configuration is absent/invalid, preflight must fail closed
        let temp = TempDir::new().unwrap();
        let mut cfg = create_test_config(&temp);
        cfg.storage_backend = StorageBackend::S3;
        cfg.s3_endpoint = Some("http://127.0.0.1:1".to_string()); // Unreachable port
        cfg.s3_bucket = Some("nonexistent-bucket".to_string());
        let cfg = Arc::new(cfg);

        let build_res = build_server_runtime(cfg, None).await;
        assert!(
            build_res.is_err(),
            "missing/unreachable live S3 configuration must fail closed nonzero"
        );
    }

    #[tokio::test]
    async fn test_server_runtime_s3_minio_graph_construction_and_teardown() {
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

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let prefix = format!("live-test-runtime-root-{}-{}/", Uuid::new_v4(), now_secs);

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
        let shared = loader.load().await;
        let builder = aws_sdk_s3::config::Builder::from(&shared)
            .endpoint_url(&endpoint)
            .force_path_style(true);
        let s3_client = aws_sdk_s3::Client::from_conf(builder.build());

        let probe = s3_client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix(&prefix)
            .send()
            .await;
        if probe.is_err() && !is_required {
            println!("Skipping live S3 test: MinIO endpoint unreachable at {endpoint}");
            return;
        }
        probe.expect("MinIO live probe failed");

        let create_res = s3_client.create_bucket().bucket(&bucket).send().await;
        if let Err(e) = create_res {
            let is_already_exists = match &e {
                aws_sdk_s3::error::SdkError::ServiceError(se) => {
                    let code = se.err().meta().code().unwrap_or("");
                    code == "BucketAlreadyOwnedByYou" || code == "BucketAlreadyExists"
                }
                _ => false,
            };
            if !is_already_exists {
                s3_client
                    .head_bucket()
                    .bucket(&bucket)
                    .send()
                    .await
                    .expect("MinIO live test endpoint must be reachable");
            }
        }

        let temp = TempDir::new().unwrap();
        let mut cfg = Config::from_env().unwrap();
        cfg.storage_backend = StorageBackend::S3;
        cfg.s3_endpoint = Some(endpoint.clone());
        cfg.s3_region = Some(region.clone());
        cfg.s3_bucket = Some(bucket.clone());
        cfg.s3_prefix = prefix.clone();
        cfg.ref_index.path = temp.path().join("ref_index_s3.db");
        cfg.ref_index.enabled = true;
        cfg.blob_gc_enabled = true;
        cfg.proxy.enabled = false;

        let cfg = Arc::new(cfg);
        println!(
            "LIVE S3 TEST: Building ServerRuntime on S3/MinIO at prefix={}",
            prefix
        );

        let runtime = build_server_runtime(cfg.clone(), None)
            .await
            .expect("ServerRuntime must build successfully on S3/MinIO");

        let state = runtime.app_state();

        // 1. Verify backend kind and catalog query on S3
        let repos = state
            .catalog_query_service
            .list_repositories(None)
            .await
            .expect("list repositories on S3");
        assert!(repos.is_empty());

        // 2. Positive proof of S3 storage backend mutation: start and abort upload session
        let start_res = state
            .blob_service
            .start_upload("s3-test-repo")
            .await
            .expect("create upload session on S3");
        let uuid = start_res.session.uuid.clone();
        assert!(!uuid.is_empty(), "session uuid must not be empty");
        let abort_res = state
            .blob_service
            .abort_upload("s3-test-repo", &uuid, Some(&start_res.state_token))
            .await;
        assert!(abort_res.is_ok(), "abort upload session on S3");

        // 3. Flush and release authority
        assert!(runtime.flush_for_shutdown().is_ok());
        assert!(runtime.release_mutation_authority().await.is_ok());

        // 4. Cleanup S3 test prefix
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
            "LIVE S3 TEST: Cleaned up {} test objects under prefix '{}'",
            deleted_count, prefix
        );

        // 5. Verify 0 objects remaining
        let post_check = s3_client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix(&prefix)
            .send()
            .await
            .expect("verify prefix is absent");
        let remaining_count = post_check.key_count().unwrap_or(0);
        println!(
            "LIVE S3 TEST: Verified {} objects remaining under prefix '{}'",
            remaining_count, prefix
        );
        assert_eq!(
            remaining_count, 0,
            "prefix '{}' must be completely empty after test cleanup",
            prefix
        );
    }

    #[tokio::test]
    async fn test_preflight_succeeds_on_truly_empty_s3_storage() {
        let is_required = std::env::var("TEST_S3_REQUIRED").as_deref() == Ok("1");
        let endpoint = std::env::var("TEST_S3_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
        let region = std::env::var("TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let bucket =
            std::env::var("TEST_S3_BUCKET").unwrap_or_else(|_| "registry-live-test".to_string());

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let prefix = format!("live-test-empty-check-{}-{}/", Uuid::new_v4(), now_secs);

        let temp = TempDir::new().unwrap();
        let mut cfg = Config::from_env().unwrap();
        cfg.storage_backend = StorageBackend::S3;
        cfg.s3_endpoint = Some(endpoint.clone());
        cfg.s3_region = Some(region);
        cfg.s3_bucket = Some(bucket);
        cfg.s3_prefix = prefix;
        cfg.ref_index.path = temp.path().join("ref_index.db");
        cfg.ref_index.enabled = false;
        cfg.blob_gc_enabled = false;
        cfg.proxy.enabled = false;

        let wiring = crate::storage_wiring::storage_wiring_from_config(&cfg);
        let empty_res = wiring.readiness_inspector().is_storage_empty().await;
        if empty_res.is_err() && !is_required {
            println!("Skipping empty check: MinIO endpoint unreachable at {endpoint}");
            return;
        }
        assert!(
            empty_res.is_ok() && empty_res.unwrap(),
            "truly empty S3 prefix must return Ok(true)"
        );
    }

    #[tokio::test]
    async fn test_proxy_cache_per_upstream_construction_offloads_filesystem_to_blocking_thread() {
        let temp = TempDir::new().unwrap();
        let mut cfg = create_test_config(&temp);
        cfg.proxy.enabled = true;
        cfg.proxy.upstreams = vec![crate::config::ProxyUpstreamRoute {
            hosts: vec![crate::proxy::ProxyHostPattern::parse("test.upstream.local").unwrap()],
            trust_x_forwarded_host: false,
            upstream_base_url: "http://localhost:5000".to_string(),
            upstream_username: None,
            upstream_password: None,
            allowed_upstream_hosts: vec!["localhost".to_string()],
            allowed_repo_prefixes: vec![],
            block_private_networks: false,
            redirect_policy: crate::config::RedirectPolicy::AnyPublic,
            max_concurrent_upstream: 10,
            index_path: temp.path().join("upstream_proxy.db"),
            cache_fs_root: Some(temp.path().join("upstream_cache")),
            cache_s3_prefix: None,
            max_cache_bytes: 1024 * 1024,
        }];
        let cfg = Arc::new(cfg);
        let calling_thread_id = std::thread::current().id();
        let (worker_tx, worker_rx) = tokio::sync::oneshot::channel();
        let worker_tx = Arc::new(std::sync::Mutex::new(Some(worker_tx)));

        let runtime = build_server_runtime_with_factories(
            cfg.clone(),
            None,
            crate::storage_wiring::storage_wiring_try_from_config,
            move |c, u| {
                let current_id = std::thread::current().id();
                if let Some(tx) = worker_tx.lock().unwrap().take() {
                    let _ = tx.send(current_id);
                }
                crate::storage_wiring::proxy_cache_storage_try_from_config(c, u)
            },
        )
        .await
        .expect("runtime build with proxy upstream must succeed");

        let construction_thread_id = worker_rx.await.expect("worker thread id must be sent");
        assert_ne!(
            calling_thread_id, construction_thread_id,
            "filesystem proxy cache construction for per-upstream route must execute on a separate blocking thread"
        );
        assert!(runtime.flush_for_shutdown().is_ok());
        assert!(runtime.release_mutation_authority().await.is_ok());
    }

    #[tokio::test]
    async fn test_proxy_cache_default_construction_offloads_filesystem_to_blocking_thread() {
        let temp = TempDir::new().unwrap();
        let mut cfg = create_test_config(&temp);
        cfg.proxy.enabled = true;
        cfg.proxy.upstreams = vec![];
        cfg.proxy.upstream_base_url = Some("http://localhost:5000".to_string());
        cfg.proxy.index_path = temp.path().join("default_proxy_index.db");
        cfg.proxy.cache_fs_root = Some(temp.path().join("default_cache"));
        let cfg = Arc::new(cfg);
        let calling_thread_id = std::thread::current().id();
        let (worker_tx, worker_rx) = tokio::sync::oneshot::channel();
        let worker_tx = Arc::new(std::sync::Mutex::new(Some(worker_tx)));

        let runtime = build_server_runtime_with_factories(
            cfg.clone(),
            None,
            crate::storage_wiring::storage_wiring_try_from_config,
            move |c, u| {
                let current_id = std::thread::current().id();
                if let Some(tx) = worker_tx.lock().unwrap().take() {
                    let _ = tx.send(current_id);
                }
                crate::storage_wiring::proxy_cache_storage_try_from_config(c, u)
            },
        )
        .await
        .expect("runtime build with default proxy cache must succeed");

        let construction_thread_id = worker_rx.await.expect("worker thread id must be sent");
        assert_ne!(
            calling_thread_id, construction_thread_id,
            "filesystem default proxy cache construction must execute on a separate blocking thread"
        );
        assert!(runtime.flush_for_shutdown().is_ok());
        assert!(runtime.release_mutation_authority().await.is_ok());
    }

    #[tokio::test]
    async fn test_proxy_cache_construction_failure_preserves_error_kind_and_message() {
        let temp = TempDir::new().unwrap();
        let mut cfg = create_test_config(&temp);
        cfg.proxy.enabled = true;
        cfg.proxy.upstreams = vec![];
        cfg.proxy.upstream_base_url = Some("http://localhost:5000".to_string());
        cfg.proxy.index_path = temp.path().join("fail_proxy_index.db");
        let cfg = Arc::new(cfg);

        let res = build_server_runtime_with_factories(
            cfg,
            None,
            crate::storage_wiring::storage_wiring_try_from_config,
            |_c, _u| {
                Err(StorageError::configuration(
                    "custom proxy cache config failure",
                ))
            },
        )
        .await;

        match res {
            Err(RuntimeBuildError::ProxyCache(err)) => {
                assert_eq!(err.internal_kind(), Some(StorageErrorKind::Configuration));
                assert!(
                    err.to_string()
                        .contains("custom proxy cache config failure")
                );
            }
            other => {
                panic!("expected RuntimeBuildError::ProxyCache with Configuration, got: {other:?}")
            }
        }
    }

    #[tokio::test]
    async fn test_proxy_cache_blocking_task_failure_maps_to_backend() {
        let temp = TempDir::new().unwrap();
        let mut cfg = create_test_config(&temp);
        cfg.proxy.enabled = true;
        cfg.proxy.upstreams = vec![];
        cfg.proxy.upstream_base_url = Some("http://localhost:5000".to_string());
        cfg.proxy.index_path = temp.path().join("panic_proxy_index.db");
        let cfg = Arc::new(cfg);

        let res = build_server_runtime_with_factories(
            cfg,
            None,
            crate::storage_wiring::storage_wiring_try_from_config,
            |_c, _u| panic!("simulated proxy cache blocking worker panic"),
        )
        .await;

        match res {
            Err(RuntimeBuildError::ProxyCache(err)) => {
                assert_eq!(err.internal_kind(), Some(StorageErrorKind::Backend));
                assert!(
                    err.to_string()
                        .contains("filesystem proxy cache storage initialization task failed")
                );
            }
            other => panic!("expected RuntimeBuildError::ProxyCache with Backend, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_proxy_cache_s3_retains_existing_construction_path_on_calling_thread() {
        let temp = TempDir::new().unwrap();
        let fs_cfg = create_test_config(&temp);
        let fs_wiring = crate::storage_wiring::storage_wiring_try_from_config(&fs_cfg).unwrap();

        let mut cfg = create_test_config(&temp);
        cfg.storage_backend = StorageBackend::S3;
        cfg.s3_endpoint = Some("http://localhost:9000".to_string());
        cfg.s3_region = Some("us-east-1".to_string());
        cfg.s3_bucket = Some("test-bucket".to_string());
        cfg.s3_prefix = "test".to_string();
        cfg.proxy.enabled = true;
        cfg.proxy.upstreams = vec![];
        cfg.proxy.upstream_base_url = Some("http://localhost:5000".to_string());
        cfg.proxy.index_path = temp.path().join("s3_proxy_index.db");
        cfg.proxy.cache_s3_prefix = Some("test/cache".to_string());
        let cfg = Arc::new(cfg);

        let calling_thread_id = std::thread::current().id();
        let (worker_tx, worker_rx) = tokio::sync::oneshot::channel();
        let worker_tx = Arc::new(std::sync::Mutex::new(Some(worker_tx)));

        let _res = build_server_runtime_with_factories(
            cfg,
            None,
            move |_c| Ok(fs_wiring),
            move |c, u| {
                let current_id = std::thread::current().id();
                if let Some(tx) = worker_tx.lock().unwrap().take() {
                    let _ = tx.send(current_id);
                }
                crate::storage_wiring::proxy_cache_storage_try_from_config(c, u)
            },
        )
        .await;

        let construction_thread_id = worker_rx.await.expect("worker thread id must be sent");
        assert_eq!(
            calling_thread_id, construction_thread_id,
            "S3 proxy cache storage construction must execute synchronously on the calling thread without spawn_blocking"
        );
    }

    #[tokio::test]
    async fn test_proxy_cache_callers_await_construction_before_proceeding() {
        let temp = TempDir::new().unwrap();
        let mut cfg = create_test_config(&temp);
        cfg.proxy.enabled = true;
        cfg.proxy.upstreams = vec![];
        cfg.proxy.upstream_base_url = Some("http://localhost:5000".to_string());
        cfg.proxy.index_path = temp.path().join("await_proxy_index.db");
        let cfg = Arc::new(cfg);

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

        let started_tx = Arc::new(std::sync::Mutex::new(Some(started_tx)));
        let release_rx = Arc::new(std::sync::Mutex::new(release_rx));

        let cfg_clone = cfg.clone();
        let runtime_handle = tokio::spawn(async move {
            build_server_runtime_with_factories(
                cfg_clone,
                None,
                crate::storage_wiring::storage_wiring_try_from_config,
                move |c, u| {
                    if let Some(tx) = started_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    if let Ok(rx) = release_rx.lock() {
                        if let Err(err) = rx.recv_timeout(std::time::Duration::from_secs(5)) {
                            panic!("worker wait failed: {err}");
                        }
                    }
                    crate::storage_wiring::proxy_cache_storage_try_from_config(c, u)
                },
            )
            .await
        });

        started_rx.await.expect("worker must start");
        assert!(
            !runtime_handle.is_finished(),
            "runtime build must not complete while proxy cache construction is pending"
        );

        drop(guard);

        let runtime = runtime_handle
            .await
            .expect("join handle must succeed")
            .expect("runtime build must succeed after release");
        assert!(runtime.flush_for_shutdown().is_ok());
        assert!(runtime.release_mutation_authority().await.is_ok());
    }
}
