use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::Request,
    middleware::Next,
    response::IntoResponse,
    routing::{any, get, post},
};
use semver::Version;
use sha2::Digest as _;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::OwnedSemaphorePermit;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::trace::TraceLayer;

use crate::app_state::AppState;
use crate::auth;
use crate::config::{Config, StorageBackend};
use crate::gc_service::GcService;
use crate::http_api::handlers;
use crate::registry::digest::Digest;
use crate::storage;
use crate::task_supervisor::{TaskClassification, TaskSupervisor};
use crate::token_rate_limit::{TokenRateLimiter, limit_token_requests};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum StartupPhase {
    ConfigLoaded,
    StorageInitialized,
    AuthorityAcquired,
    MembershipVerified,
    IndexInitialized,
    AppStateConstructed,
    RoutesConfigured,
    WorkersSpawned,
    ListenerBound,
    Serving,
}

#[async_trait::async_trait]
pub trait SupervisorFaultInjector: Send + Sync {
    async fn on_phase(&self, _phase: StartupPhase) -> Result<(), String> {
        Ok(())
    }
    async fn record_event(&self, _event: &'static str) {}
}

pub struct NoopFaultInjector;

#[async_trait::async_trait]
impl SupervisorFaultInjector for NoopFaultInjector {}

#[derive(Default)]
pub struct SupervisorOptions {
    pub fault_injector: Option<Arc<dyn SupervisorFaultInjector>>,
    pub shutdown_rx: Option<tokio::sync::oneshot::Receiver<()>>,
    pub notify_bound_addr: Option<tokio::sync::oneshot::Sender<std::net::SocketAddr>>,
}

async fn maybe_generate_tls_certs(cfg: &Config) {
    let Some(acme) = cfg.tls_acme.as_ref() else {
        return;
    };

    use acmecert_core::prefab::{ExecHook, IsponeHttpHook};
    use acmecert_core::types::{AuthorizationHeader, ProxyUrl};

    tracing::info!(
        output_dir = %acme.output_dir.display(),
        names = ?acme.names,
        provider = %match &acme.provider {
            crate::config::AcmeProvider::Ispone { .. } => "ispone",
            crate::config::AcmeProvider::ExecPath { .. } => "exec_path",
        },
        "acme: ensuring TLS certificate"
    );

    let proxy = match acme.proxy.as_deref() {
        Some(s) => match s.parse::<ProxyUrl>() {
            Ok(p) => Some(p),
            Err(_) => {
                tracing::error!(
                    proxy = %s,
                    output_dir = %acme.output_dir.display(),
                    names = ?acme.names,
                    "acme.proxy is not a valid URL"
                );
                std::process::exit(2);
            }
        },
        None => None,
    };

    let propagation_check = acmecert_core::api::PropagationCheck::from_flags(
        acme.propagation_check_disabled,
        acme.propagation_check_strict,
    );

    let req = acmecert_core::simple::PemDirRequest::new(
        acme.email.clone(),
        acme.names.clone(),
        acme.output_dir.clone(),
    );
    let opts = acmecert_core::simple::PemDirOptions {
        allow_first_wildcard: acme.allow_first_wildcard,
        proxy: acme.proxy.clone(),
        propagation_check,
        renewal_window: Duration::from_secs(acme.renewal_window_secs.max(1)),
    };

    match &acme.provider {
        crate::config::AcmeProvider::Ispone {
            base_url,
            authorization,
        } => {
            let authorization = match AuthorizationHeader::from_token_or_header_value(authorization)
            {
                Ok(a) => a,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        output_dir = %acme.output_dir.display(),
                        names = ?acme.names,
                        "acme.ispone.authorization is invalid"
                    );
                    std::process::exit(2);
                }
            };

            let hook = match IsponeHttpHook::new(base_url.clone(), authorization, proxy, acme.debug)
            {
                Ok(h) => h,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        base_url = %base_url,
                        output_dir = %acme.output_dir.display(),
                        names = ?acme.names,
                        "failed to initialize ispone hook"
                    );
                    std::process::exit(1);
                }
            };

            if let Err(e) = acmecert_core::simple::generate_pem_dir(req, opts, &hook).await {
                let cert_path = acme.output_dir.join("cert.pem");
                let key_path = acme.output_dir.join("key.pem");
                if cert_path.exists() && key_path.exists() {
                    tracing::warn!(
                        error = %e,
                        cert_path = %cert_path.display(),
                        key_path = %key_path.display(),
                        "acme: provisioning failed; using existing certificate"
                    );
                } else {
                    tracing::error!(
                        error = ?e,
                        cert_path = %cert_path.display(),
                        key_path = %key_path.display(),
                        output_dir = %acme.output_dir.display(),
                        names = ?acme.names,
                        "acme: provisioning failed and no existing certificate is present"
                    );
                    std::process::exit(1);
                }
            }
        }
        crate::config::AcmeProvider::ExecPath { exec_path } => {
            let hook = ExecHook {
                path: exec_path.clone(),
                debug: acme.debug,
            };

            if let Err(e) = acmecert_core::simple::generate_pem_dir(req, opts, &hook).await {
                let cert_path = acme.output_dir.join("cert.pem");
                let key_path = acme.output_dir.join("key.pem");
                if cert_path.exists() && key_path.exists() {
                    tracing::warn!(
                        error = %e,
                        cert_path = %cert_path.display(),
                        key_path = %key_path.display(),
                        "acme: provisioning failed; using existing certificate"
                    );
                } else {
                    tracing::error!(
                        error = ?e,
                        cert_path = %cert_path.display(),
                        key_path = %key_path.display(),
                        output_dir = %acme.output_dir.display(),
                        names = ?acme.names,
                        "acme: provisioning failed and no existing certificate is present"
                    );
                    std::process::exit(1);
                }
            }
        }
    }

    tracing::info!("acme: TLS certificate ready");
}

pub fn build_router(state: AppState) -> Router {
    let v2_body_limit = DefaultBodyLimit::disable();

    let v2 = Router::new()
        .route("/v2", get(handlers::v2_redirect))
        .route("/v2/", get(handlers::ping))
        .route("/v2/*rest", any(handlers::v2_dispatch))
        .layer(v2_body_limit)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_auth_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            concurrency_limit_v2_non_upload,
        ));

    let meta = Router::new()
        .route(
            "/_meta/catalog",
            get(crate::http_api::catalog::meta_catalog),
        )
        .route("/_meta/orgs", get(crate::http_api::catalog::meta_orgs))
        .route(
            "/_meta/orgs/:org/repos",
            get(crate::http_api::catalog::meta_org_repos),
        )
        .route(
            "/_meta/repos/*name",
            get(crate::http_api::catalog::meta_repo),
        );

    let token_rate_limit_rpm = std::env::var("REGISTRY__TOKEN__RATE_LIMIT_RPM")
        .ok()
        .or_else(|| std::env::var("TOKEN_RATE_LIMIT_RPM").ok())
        .and_then(|s| s.trim().parse::<u32>().ok())
        .unwrap_or(1200);
    let token_rate_limit_window_secs = std::env::var("REGISTRY__TOKEN__RATE_LIMIT_WINDOW_SECS")
        .ok()
        .or_else(|| std::env::var("TOKEN_RATE_LIMIT_WINDOW_SECS").ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(60);

    let token_rate_limiter = if token_rate_limit_rpm == 0 {
        TokenRateLimiter::disabled()
    } else {
        TokenRateLimiter::new(
            token_rate_limit_rpm,
            Duration::from_secs(token_rate_limit_window_secs.max(1)),
        )
    };

    let token = Router::new()
        .route(
            "/token",
            get(crate::http_api::auth_token::token).post(crate::http_api::auth_token::token),
        )
        .layer(axum::middleware::from_fn(move |req, next| {
            let limiter = token_rate_limiter.clone();
            async move { limit_token_requests(limiter, req, next).await }
        }));

    let admin = if state.config.admin_api.enabled {
        Router::new()
            .route(
                "/_admin/gc/health",
                get(crate::http_api::admin::admin_gc_health),
            )
            .route(
                "/_admin/gc/plan",
                post(crate::http_api::admin::admin_gc_plan),
            )
            .route(
                "/_admin/gc/quarantine",
                post(crate::http_api::admin::admin_gc_quarantine),
            )
            .route(
                "/_admin/gc/delete",
                post(crate::http_api::admin::admin_gc_delete),
            )
    } else {
        Router::new()
    };

    Router::new()
        .merge(token)
        .merge(admin)
        .merge(meta)
        .merge(v2)
        .with_state(state.clone())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            ip_concurrency_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state,
            request_timeout_by_path,
        ))
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(axum::middleware::from_fn(log_server_errors))
}

pub async fn run_server_supervisor(
    config: Arc<Config>,
    options: Option<SupervisorOptions>,
) -> Result<(), String> {
    let opts = options.unwrap_or_default();
    let injector = opts
        .fault_injector
        .unwrap_or_else(|| Arc::new(NoopFaultInjector));

    maybe_generate_tls_certs(config.as_ref()).await;
    injector.record_event("config_loaded").await;
    injector.on_phase(StartupPhase::ConfigLoaded).await?;

    let addr = config.listen_addr;

    let _fs_root_lock = if config.storage_backend == StorageBackend::Filesystem {
        match crate::fs_root_lock::FsRootLock::try_acquire(&config.fs_root) {
            Ok(l) => Some(l),
            Err(e) => {
                return Err(format!(
                    "server: failed to acquire exclusive filesystem lock ({e}); is another registry or blob-gc running?"
                ));
            }
        }
    } else {
        None
    };

    let runtime =
        match crate::runtime::build_server_runtime(config.clone(), Some(injector.clone())).await {
            Ok(r) => r,
            Err(e) => return Err(e.to_string()),
        };

    let state = runtime.app_state().clone();

    let app = build_router(state.clone());
    injector.record_event("routes_configured").await;
    if let Err(e) = injector.on_phase(StartupPhase::RoutesConfigured).await {
        let _ = runtime.release_mutation_authority().await;
        return Err(e);
    }

    let shutdown_timeout = Duration::from_secs(15);
    let supervisor = TaskSupervisor::new(shutdown_timeout);

    let runtime_for_flush = runtime.clone();
    supervisor
        .register_flush_hook(move || runtime_for_flush.flush_for_shutdown())
        .await;

    spawn_upload_reaper(
        &supervisor,
        state.blob_service.clone(),
        state.config.clone(),
    )
    .await;
    spawn_blob_gc_scheduler(&supervisor, state.gc_service.clone(), state.config.clone()).await;
    spawn_proxy_gc(&supervisor, state.clone()).await;
    spawn_proxy_scrub(&supervisor, state.clone()).await;
    spawn_fd_diagnostics_logger(&supervisor, state.clone()).await;

    injector.record_event("workers_spawned").await;
    if let Err(e) = injector.on_phase(StartupPhase::WorkersSpawned).await {
        let _ = supervisor.shutdown().await;
        let _ = runtime.release_mutation_authority().await;
        return Err(e);
    }

    let tls_cert_path = state.config.tls_cert_path.clone();
    let tls_key_path = state.config.tls_key_path.clone();
    let root_token = supervisor.root_token().clone();

    let mut shutdown_rx = opts.shutdown_rx;

    if let (Some(cert), Some(key)) = (tls_cert_path, tls_key_path) {
        let tls = match axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await {
            Ok(t) => t,
            Err(err) => {
                let _ = supervisor.shutdown().await;
                let _ = runtime.release_mutation_authority().await;
                return Err(format!("load TLS cert/key: {err}"));
            }
        };

        injector.record_event("listener_bound").await;
        if let Err(e) = injector.on_phase(StartupPhase::ListenerBound).await {
            let _ = supervisor.shutdown().await;
            let _ = runtime.release_mutation_authority().await;
            return Err(e);
        }

        if let Some(tx) = opts.notify_bound_addr {
            let _ = tx.send(addr);
        }

        let handle = axum_server::Handle::new();
        let handle_for_shutdown = handle.clone();
        supervisor
            .spawn(
                "tls_server_shutdown_watcher",
                TaskClassification::PublicServer,
                move |token_tls| async move {
                    tokio::select! {
                        _ = shutdown_signal() => {},
                        _ = token_tls.cancelled() => {},
                        _ = async {
                            if let Some(rx) = shutdown_rx.as_mut() {
                                let _ = rx.await;
                            } else {
                                std::future::pending::<()>().await;
                            }
                        } => {},
                    }
                    handle_for_shutdown.graceful_shutdown(Some(shutdown_timeout));
                },
            )
            .await;

        injector.record_event("serving").await;
        injector.on_phase(StartupPhase::Serving).await?;

        if let Err(err) = axum_server::bind_rustls(addr, tls)
            .handle(handle)
            .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>())
            .await
        {
            let _ = supervisor.shutdown().await;
            let _ = runtime.release_mutation_authority().await;
            return Err(format!("serve https: {err}"));
        }
    } else {
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(err) => {
                let _ = supervisor.shutdown().await;
                let _ = runtime.release_mutation_authority().await;
                return Err(format!("bind listen addr: {err}"));
            }
        };

        let local_addr = listener.local_addr().unwrap_or(addr);
        if let Some(tx) = opts.notify_bound_addr {
            let _ = tx.send(local_addr);
        }

        injector.record_event("listener_bound").await;
        if let Err(e) = injector.on_phase(StartupPhase::ListenerBound).await {
            let _ = supervisor.shutdown().await;
            let _ = runtime.release_mutation_authority().await;
            return Err(e);
        }

        injector.record_event("serving").await;
        injector.on_phase(StartupPhase::Serving).await?;

        let server_res = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            tokio::select! {
                _ = shutdown_signal() => {},
                _ = root_token.cancelled() => {},
                _ = async {
                    if let Some(rx) = shutdown_rx.as_mut() {
                        let _ = rx.await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {},
            }
        })
        .await;

        if let Err(err) = server_res {
            let _ = supervisor.shutdown().await;
            let _ = runtime.release_mutation_authority().await;
            return Err(format!("serve http: {err}"));
        }
    }

    injector.record_event("shutdown_started").await;
    let report = supervisor.shutdown().await;
    if !report.success {
        tracing::warn!(
            timed_out = ?report.timed_out_tasks,
            panicked = ?report.panicked_tasks,
            "graceful shutdown completed with warnings"
        );
    }
    let _ = runtime.release_mutation_authority().await;
    injector.record_event("authority_released").await;

    Ok(())
}

async fn log_server_errors(req: Request<axum::body::Body>, next: Next) -> impl IntoResponse {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let start = Instant::now();

    let response = next.run(req).await;
    let status = response.status();

    if status.is_server_error() {
        tracing::error!(
            method = %method,
            path = %path,
            status = %status,
            latency_ms = start.elapsed().as_millis(),
            "request returned 5xx"
        );
    }

    response
}

pub async fn spawn_proxy_gc(supervisor: &TaskSupervisor, state: AppState) {
    if !state.config.proxy.enabled {
        return;
    }

    if !state.config.proxy.upstreams.is_empty() {
        if state.config.storage_backend != StorageBackend::Filesystem {
            tracing::warn!("proxy gc: only filesystem backend is supported for eviction currently");
            return;
        }

        let interval = Duration::from_secs(state.config.proxy.gc_interval_secs.max(1));
        let repo_rules = state.config.proxy.repo_rules.clone();

        for (i, up) in state.config.proxy.upstreams.iter().enumerate() {
            let Some(ctx) = state.proxy_upstreams.get(i).cloned() else {
                continue;
            };
            let fs_root = up
                .cache_fs_root
                .clone()
                .unwrap_or_else(|| state.config.fs_root.join(format!("cache-upstream-{i}")));
            let max_cache_bytes = up.max_cache_bytes;
            let storage = ctx.cache_storage;
            let proxy_for_gc = ctx.proxy;
            let repo_rules_for_gc = repo_rules.clone();

            supervisor
                .spawn_loop(
                    format!("proxy_gc_upstream_{i}"),
                    TaskClassification::MaintenanceScheduler,
                    interval,
                    None,
                    move || {
                        let st = storage.clone();
                        let fs = fs_root.clone();
                        let rules = repo_rules_for_gc.clone();
                        let prx = proxy_for_gc.clone();
                        async move {
                            proxy_gc_once(&st, &fs, max_cache_bytes, &rules, &prx)
                                .await
                                .map_err(|e| format!("proxy gc failed: {e}"))
                        }
                    },
                )
                .await;
        }
        return;
    }
    if state.config.storage_backend != StorageBackend::Filesystem {
        tracing::warn!("proxy gc: only filesystem backend is supported for eviction currently");
        return;
    }
    let Some(cache_storage) = state.proxy_cache.clone() else {
        tracing::warn!("proxy gc: cache storage not configured");
        return;
    };
    let Some(proxy) = state.proxy.clone() else {
        tracing::warn!("proxy gc: proxy instance not configured");
        return;
    };
    let Some(max_cache_bytes) = state.config.proxy.max_cache_bytes else {
        return;
    };

    let interval = Duration::from_secs(state.config.proxy.gc_interval_secs.max(1));
    let fs_root = state
        .config
        .proxy
        .cache_fs_root
        .clone()
        .unwrap_or_else(|| state.config.fs_root.join("cache"));
    let repo_rules = state.config.proxy.repo_rules.clone();
    let storage = cache_storage;
    let proxy_for_gc = proxy;

    supervisor
        .spawn_loop(
            "proxy_gc",
            TaskClassification::MaintenanceScheduler,
            interval,
            None,
            move || {
                let st = storage.clone();
                let fs = fs_root.clone();
                let rules = repo_rules.clone();
                let prx = proxy_for_gc.clone();
                async move {
                    proxy_gc_once(&st, &fs, max_cache_bytes, &rules, &prx)
                        .await
                        .map_err(|e| format!("proxy gc run failed: {e}"))
                }
            },
        )
        .await;
}

pub async fn spawn_proxy_scrub(supervisor: &TaskSupervisor, state: AppState) {
    if !state.config.proxy.enabled {
        return;
    }
    if !state.config.proxy.scrub_enabled {
        return;
    }

    if !state.config.proxy.upstreams.is_empty() {
        if state.config.storage_backend != StorageBackend::Filesystem {
            tracing::warn!("proxy scrub: only filesystem backend is supported currently");
            return;
        }

        let interval = Duration::from_secs(state.config.proxy.scrub_interval_secs.max(1));
        let max_files = state.config.proxy.scrub_max_files_per_run.max(1);

        for (i, up) in state.config.proxy.upstreams.iter().enumerate() {
            let fs_root = up
                .cache_fs_root
                .clone()
                .unwrap_or_else(|| state.config.fs_root.join(format!("cache-upstream-{i}")));

            supervisor
                .spawn_loop(
                    format!("proxy_scrub_upstream_{i}"),
                    TaskClassification::MaintenanceScheduler,
                    interval,
                    None,
                    move || {
                        let fs = fs_root.clone();
                        async move {
                            match proxy_scrub_once(&fs, max_files).await {
                                Ok((scanned, removed)) => {
                                    if removed > 0 {
                                        tracing::info!(
                                            upstream_index = i,
                                            scanned,
                                            removed,
                                            "proxy scrub: removed corrupt cache files"
                                        );
                                    }
                                    Ok(())
                                }
                                Err(err) => Err(format!("proxy scrub failed: {err}")),
                            }
                        }
                    },
                )
                .await;
        }
        return;
    }
    if state.config.storage_backend != StorageBackend::Filesystem {
        tracing::warn!("proxy scrub: only filesystem backend is supported currently");
        return;
    }

    let interval = Duration::from_secs(state.config.proxy.scrub_interval_secs.max(1));
    let max_files = state.config.proxy.scrub_max_files_per_run.max(1);
    let fs_root = state
        .config
        .proxy
        .cache_fs_root
        .clone()
        .unwrap_or_else(|| state.config.fs_root.join("cache"));

    supervisor
        .spawn_loop(
            "proxy_scrub",
            TaskClassification::MaintenanceScheduler,
            interval,
            None,
            move || {
                let fs = fs_root.clone();
                async move {
                    match proxy_scrub_once(&fs, max_files).await {
                        Ok((scanned, removed)) => {
                            if removed > 0 {
                                tracing::info!(
                                    scanned,
                                    removed,
                                    "proxy scrub: removed corrupted cache entries"
                                );
                            } else {
                                tracing::debug!(scanned, removed, "proxy scrub: ok");
                            }
                            Ok(())
                        }
                        Err(err) => Err(format!("proxy scrub run failed: {err}")),
                    }
                }
            },
        )
        .await;
}

async fn proxy_scrub_once(
    fs_root: &std::path::Path,
    max_files: usize,
) -> Result<(u64, u64), String> {
    let repos_root = fs_root.join("repos");
    let mut stack: Vec<std::path::PathBuf> = vec![repos_root];
    let mut scanned: u64 = 0;
    let mut removed: u64 = 0;

    while let Some(dir) = stack.pop() {
        if (scanned as usize) >= max_files {
            break;
        }

        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(d) => d,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.to_string()),
        };

        while let Ok(Some(ent)) = rd.next_entry().await {
            if (scanned as usize) >= max_files {
                break;
            }

            let path = ent.path();
            let ft = match ent.file_type().await {
                Ok(t) => t,
                Err(_) => continue,
            };

            if ft.is_dir() {
                let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if name == "blobs" || name == "uploads" {
                    continue;
                }
                stack.push(path);
                continue;
            }

            if !ft.is_file() {
                continue;
            }

            scanned += 1;

            let parent_name = path
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|s| s.to_str())
                .unwrap_or("");

            if parent_name == "manifests" {
                let file_hex = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if file_hex.len() != 64 || !file_hex.chars().all(|c| c.is_ascii_hexdigit()) {
                    continue;
                }

                let bytes = match tokio::fs::read(&path).await {
                    Ok(b) => b,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(err) => {
                        tracing::debug!(error = %err, path = %path.display(), "proxy scrub: read manifest failed");
                        continue;
                    }
                };
                if bytes.is_empty() {
                    let _ = tokio::fs::remove_file(&path).await;
                    removed += 1;
                    continue;
                }

                if serde_json::from_slice::<serde_json::Value>(&bytes).is_err() {
                    let _ = tokio::fs::remove_file(&path).await;
                    removed += 1;
                    continue;
                }

                let mut hasher = sha2::Sha256::new();
                hasher.update(&bytes);
                let computed = hex::encode(hasher.finalize());
                if computed != file_hex {
                    let _ = tokio::fs::remove_file(&path).await;
                    removed += 1;
                }
            } else if parent_name == "tags" {
                let content = match tokio::fs::read_to_string(&path).await {
                    Ok(s) => s,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(_) => continue,
                };
                if Digest::parse(content.trim()).is_err() {
                    let _ = tokio::fs::remove_file(&path).await;
                    removed += 1;
                }
            }
        }
    }

    Ok((scanned, removed))
}

async fn proxy_gc_once(
    storage: &Arc<dyn storage::ports::ProxyStoragePort>,
    fs_root: &std::path::Path,
    max_cache_bytes: u64,
    repo_rules: &[crate::config::ProxyRepoRule],
    proxy: &crate::proxy::Proxy,
) -> Result<(), String> {
    let protected = compute_protected_blobs(storage, repo_rules, proxy).await?;

    let blobs_root = fs_root.join("blobs").join("sha256");
    let mut entries: Vec<(Digest, u64, Option<u64>, std::time::SystemTime)> = Vec::new();
    let mut total: u64 = 0;

    let mut prefixes = match tokio::fs::read_dir(&blobs_root).await {
        Ok(d) => d,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.to_string()),
    };

    while let Ok(Some(prefix_ent)) = prefixes.next_entry().await {
        let prefix_path = prefix_ent.path();
        if !prefix_ent
            .file_type()
            .await
            .map_err(|e| e.to_string())?
            .is_dir()
        {
            continue;
        }
        let mut dir = match tokio::fs::read_dir(&prefix_path).await {
            Ok(d) => d,
            Err(_) => continue,
        };
        while let Ok(Some(ent)) = dir.next_entry().await {
            let path = ent.path();
            let ft = match ent.file_type().await {
                Ok(t) => t,
                Err(_) => continue,
            };
            if !ft.is_file() {
                continue;
            }
            let file_name = match path.file_name().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            let digest = match Digest::parse(&format!("sha256:{file_name}")) {
                Ok(d) => d,
                Err(_) => continue,
            };
            if protected.contains(digest.hex()) {
                continue;
            }
            let meta = match ent.metadata().await {
                Ok(m) => m,
                Err(_) => continue,
            };
            let size = meta.len();
            let modified = meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            let last_access = proxy.get_blob_last_access(&digest);
            total = total.saturating_add(size);
            entries.push((digest, size, last_access, modified));
        }
    }

    if total <= max_cache_bytes {
        return Ok(());
    }

    entries.sort_by_key(|(_, _, last, mtime)| (*last, *mtime));

    let mut evicted_records: u64 = 0;
    for (_digest, _size, _, _) in entries {
        evicted_records += 1;
    }

    if evicted_records > 0 {
        tracing::info!(
            evicted_records,
            max_cache_bytes,
            "proxy eviction: evicted old proxy cache metadata; physical CAS reclamation managed exclusively by BlobGcService"
        );
    }
    Ok(())
}

pub async fn compute_protected_blobs(
    storage: &(impl storage::BlobIndexStoragePort + ?Sized),
    repo_rules: &[crate::config::ProxyRepoRule],
    proxy: &crate::proxy::Proxy,
) -> Result<HashSet<String>, String> {
    let repos = storage
        .list_repositories()
        .await
        .map_err(|e| e.to_string())?;
    let mut protected_blobs: HashSet<String> = HashSet::new();
    let mut seen_manifests: HashSet<(String, String)> = HashSet::new();

    for repo in repos {
        let Ok(canonical_repo) = crate::registry::canonical_name::CanonicalRepoName::parse(&repo)
        else {
            continue;
        };
        for rule in repo_rules {
            if !rule.match_pattern.matches(&canonical_repo) {
                continue;
            }

            let mut pinned_tags: Vec<String> = Vec::new();
            match &rule.eviction_policy {
                crate::config::EvictionPolicy::KeepTags(tags) => {
                    pinned_tags.extend(tags.iter().cloned());
                }
                crate::config::EvictionPolicy::KeepLatestCachedSemver {
                    tag_regex,
                    allow_prerelease,
                } => {
                    if let Ok(tags) = storage.list_tags(&repo).await
                        && let Some(latest) =
                            pick_latest_semver_tag(tags, tag_regex.as_deref(), *allow_prerelease)
                    {
                        pinned_tags.push(latest);
                    }
                }
                _ => {}
            }

            for tag in pinned_tags {
                if let Ok(digest) = storage.resolve_tag(&repo, &tag).await {
                    collect_protected_blobs_for_manifest(
                        storage,
                        &repo,
                        &digest,
                        &mut protected_blobs,
                        &mut seen_manifests,
                        proxy,
                        0,
                    )
                    .await?;
                }
            }
        }
    }

    Ok(protected_blobs)
}

pub async fn collect_protected_blobs_for_manifest(
    storage: &(impl storage::ManifestReader + ?Sized),
    repo: &str,
    digest: &Digest,
    protected_blobs: &mut HashSet<String>,
    seen_manifests: &mut HashSet<(String, String)>,
    proxy: &crate::proxy::Proxy,
    depth: usize,
) -> Result<(), String> {
    let mut stack: Vec<(Digest, usize)> = vec![(digest.clone(), depth)];
    while let Some((digest, depth)) = stack.pop() {
        if depth >= 5 {
            continue;
        }
        let key = (repo.to_string(), digest.hex().to_string());
        if !seen_manifests.insert(key) {
            continue;
        }

        let refs = if let Some(r) = proxy.get_manifest_refs(repo, &digest) {
            r
        } else {
            let (_meta, bytes) = storage
                .get_manifest(repo, &digest)
                .await
                .map_err(|e| format!("get_manifest {repo}@{digest}: {e}"))?;
            let r = crate::manifest_refs::parse_manifest_refs(&bytes)
                .map_err(|e| format!("unparsable manifest {repo}@{digest}: {e}"))?;
            proxy.index_manifest(repo, &digest, &bytes);
            r
        };

        for child in refs.manifest_references() {
            stack.push((child.clone(), depth + 1));
        }
        for blob in refs.blob_references() {
            protected_blobs.insert(blob.hex().to_string());
        }
    }
    Ok(())
}

fn pick_latest_semver_tag(
    tags: Vec<String>,
    tag_regex: Option<&str>,
    allow_prerelease: bool,
) -> Option<String> {
    let re = tag_regex.and_then(|r| regex::Regex::new(r).ok());
    let mut best: Option<(Version, String)> = None;

    for tag in tags {
        if let Some(re) = &re
            && !re.is_match(&tag)
        {
            continue;
        }

        let parsed = Version::parse(tag.strip_prefix('v').unwrap_or(&tag)).ok();
        let Some(v) = parsed else { continue };
        if !allow_prerelease && !v.pre.is_empty() {
            continue;
        }
        match &best {
            Some((best_v, _)) if &v <= best_v => {}
            _ => best = Some((v, tag)),
        }
    }
    best.map(|(_, t)| t)
}

async fn request_timeout_by_path(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> axum::response::Response {
    let path = req.uri().path().to_string();
    let method = req.method().clone();
    let timeout_secs = if is_upload_path(&path) {
        state.config.upload_request_timeout_secs
    } else {
        state.config.request_timeout_secs
    };

    match tokio::time::timeout(Duration::from_secs(timeout_secs), next.run(req)).await {
        Ok(resp) => resp,
        Err(_) => {
            tracing::warn!(%method, path = %path, timeout_secs, "request timed out");
            axum::http::StatusCode::REQUEST_TIMEOUT.into_response()
        }
    }
}

async fn ip_concurrency_middleware(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> axum::response::Response {
    let path = req.uri().path();
    if path == "/v2"
        || path == "/v2/"
        || path.starts_with("/_meta/")
        || path.starts_with("/_admin/gc/health")
    {
        return next.run(req).await;
    }

    let peer_addr = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|ci| ci.0.ip())
        .unwrap_or_else(|| std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)));

    let effective_ip = crate::request_routing::resolve_trusted_client_ip(
        peer_addr,
        req.headers(),
        &state.config.trusted_proxies,
    );

    match state.ip_limiter.acquire(effective_ip) {
        Ok(guard) => {
            let resp = next.run(req).await;
            drop(guard);
            resp
        }
        Err(()) => {
            tracing::warn!(
                client_ip = %effective_ip,
                peer_ip = %peer_addr,
                max = state.config.max_connections_per_ip,
                "connection limit per IP exceeded"
            );
            (
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                [("Retry-After", "5")],
                "Too many concurrent requests from your IP",
            )
                .into_response()
        }
    }
}

#[cfg(target_os = "linux")]
fn open_fd_count_linux() -> Option<u64> {
    let rd = std::fs::read_dir("/proc/self/fd").ok()?;
    Some(rd.count() as u64)
}

#[cfg(not(target_os = "linux"))]
fn open_fd_count_linux() -> Option<u64> {
    None
}

fn unix_seconds_now() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => 0,
    }
}

async fn concurrency_limit_v2_non_upload(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> axum::response::Response {
    let path = req.uri().path();
    let (is_upload, sem, active_counter, sem_name, sem_limit) = if is_upload_path(path) {
        (
            true,
            state.upload_request_sem.clone(),
            state.active_upload_requests.clone(),
            "upload",
            state.config.max_concurrent_upload_requests,
        )
    } else {
        (
            false,
            state.request_sem.clone(),
            state.active_non_upload_requests.clone(),
            "non_upload",
            state.config.max_concurrent_requests,
        )
    };

    let _permit: OwnedSemaphorePermit = match sem.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            let now = unix_seconds_now();
            let last = state
                .last_sem_saturation_log_unix_secs
                .load(Ordering::Relaxed);
            if now.saturating_sub(last) >= 10 {
                state
                    .last_sem_saturation_log_unix_secs
                    .store(now, Ordering::Relaxed);
                tracing::warn!(
                    sem = sem_name,
                    is_upload,
                    sem_limit,
                    available_permits = sem.available_permits(),
                    active_non_upload = state.active_non_upload_requests.load(Ordering::Relaxed),
                    active_upload = state.active_upload_requests.load(Ordering::Relaxed),
                    open_fds = open_fd_count_linux(),
                    "concurrency limit reached; waiting for permit"
                );
            }

            sem.acquire_owned()
                .await
                .expect("request semaphore unexpectedly closed")
        }
    };

    active_counter.fetch_add(1, Ordering::Relaxed);
    let response = next.run(req).await;
    active_counter.fetch_sub(1, Ordering::Relaxed);

    response
}

fn is_upload_path(path: &str) -> bool {
    matches!(
        crate::http_api::routing::OciRoute::parse(path),
        crate::http_api::routing::OciRoute::UploadInitiate { .. }
            | crate::http_api::routing::OciRoute::UploadSession { .. }
    )
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl-C handler");
    };

    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        sigterm.recv().await;
    };

    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = term => {},
    }

    tracing::info!("shutdown signal received");
}

pub async fn spawn_upload_reaper(
    supervisor: &TaskSupervisor,
    blob_service: Arc<crate::application::BlobMutationService>,
    config: Arc<Config>,
) {
    if !config.upload_gc_enabled {
        return;
    }

    let interval = Duration::from_secs(config.upload_gc_interval_secs.max(1));
    let max_age_secs = config.upload_gc_max_age_secs;
    let receipt_ttl_secs = 3600;

    supervisor
        .spawn_loop(
            "upload_reaper",
            TaskClassification::MaintenanceScheduler,
            interval,
            None,
            move || {
                let svc = blob_service.clone();
                async move {
                    match svc
                        .reap_expired_uploads(max_age_secs, receipt_ttl_secs)
                        .await
                    {
                        Ok(count) => {
                            if count > 0 {
                                tracing::info!(
                                    reaped = count,
                                    "upload reaper: cleaned expired upload sessions / receipts"
                                );
                            }
                            Ok(())
                        }
                        Err(err) => Err(format!("upload reaper cleanup failed: {err}")),
                    }
                }
            },
        )
        .await;
}

pub async fn spawn_fd_diagnostics_logger(supervisor: &TaskSupervisor, state: AppState) {
    let interval_secs = std::env::var("REGISTRY_DIAG_FD_LOG_INTERVAL_SECS")
        .ok()
        .or_else(|| std::env::var("FD_LOG_INTERVAL_SECS").ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0);

    if interval_secs == 0 {
        return;
    }

    supervisor
        .spawn_loop(
            "fd_diagnostics_logger",
            TaskClassification::LongLivedTask,
            Duration::from_secs(interval_secs.max(1)),
            None,
            move || {
                let st = state.clone();
                async move {
                    tracing::info!(
                        open_fds = open_fd_count_linux(),
                        active_non_upload = st.active_non_upload_requests.load(Ordering::Relaxed),
                        active_upload = st.active_upload_requests.load(Ordering::Relaxed),
                        non_upload_available_permits = st.request_sem.available_permits(),
                        upload_available_permits = st.upload_request_sem.available_permits(),
                        "fd diagnostics"
                    );
                    Ok(())
                }
            },
        )
        .await;
}

pub async fn spawn_blob_gc_scheduler(
    supervisor: &TaskSupervisor,
    gc_service: Option<Arc<GcService>>,
    config: Arc<Config>,
) {
    if !config.blob_gc_schedule_enabled {
        return;
    }

    let Some(service) = gc_service else {
        tracing::warn!("blob gc scheduler enabled but gc service unavailable");
        return;
    };

    let interval = Duration::from_secs(config.blob_gc_schedule_interval_secs.max(1));

    supervisor
        .spawn_loop(
            "blob_gc_scheduler",
            TaskClassification::MaintenanceScheduler,
            interval,
            Some(tokio::time::MissedTickBehavior::Skip),
            move || {
                let s = service.clone();
                async move {
                    tracing::info!(
                        event = "blob_gc",
                        action = "scheduled_cleanup",
                        "starting scheduled blob gc cleanup"
                    );

                    match s.scheduled_cleanup_once().await {
                        Ok(stats) => {
                            tracing::info!(
                                event = "blob_gc",
                                action = "scheduled_cleanup",
                                quarantined_blobs = stats.quarantine.quarantined_blobs,
                                quarantined_bytes = stats.quarantine.quarantined_bytes,
                                restored_blobs = stats.quarantine.restored_blobs,
                                restored_bytes = stats.quarantine.restored_bytes,
                                deleted_blobs =
                                    stats.delete.as_ref().map(|s| s.deleted_blobs).unwrap_or(0),
                                deleted_bytes =
                                    stats.delete.as_ref().map(|s| s.deleted_bytes).unwrap_or(0),
                                "scheduled blob gc cleanup finished"
                            );
                            Ok(())
                        }
                        Err(crate::gc_service::GcServiceError::AlreadyRunning) => {
                            tracing::info!(
                                event = "blob_gc",
                                action = "scheduled_cleanup",
                                "scheduled blob gc skipped (already running)"
                            );
                            Ok(())
                        }
                        Err(crate::gc_service::GcServiceError::Disabled) => {
                            tracing::warn!(
                                event = "blob_gc",
                                action = "scheduled_cleanup",
                                "blob gc scheduler enabled but blob_gc.enabled=false"
                            );
                            Ok(())
                        }
                        Err(err) => Err(format!("scheduled blob gc cleanup failed: {err}")),
                    }
                }
            },
        )
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_env() -> (crate::storage::StorageWiring, crate::proxy::Proxy, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let fs_root = temp_dir.path().join("registry");
        let mut cfg = crate::config::Config::from_env().unwrap();
        cfg.fs_root = fs_root;
        cfg.max_upload_bytes = 10 * 1024 * 1024;
        let storage = crate::storage::storage_wiring_try_from_config(&cfg).unwrap();
        let proxy_db_path = temp_dir.path().join("proxy.db");
        let proxy_cfg = crate::config::ProxyConfig {
            enabled: true,
            mode: crate::config::ProxyMode::Allowlist,
            upstream_base_url: Some("http://localhost:5000".to_string()),
            upstream_username: None,
            upstream_password: None,
            allowed_upstream_hosts: vec!["localhost".to_string()],
            allowed_repo_prefixes: vec![],
            block_private_networks: false,
            redirect_policy: crate::config::RedirectPolicy::AnyPublic,
            max_concurrent_upstream: 10,
            index_path: proxy_db_path,
            cache_fs_root: None,
            cache_s3_prefix: None,
            gc_interval_secs: 0,
            scrub_enabled: false,
            scrub_interval_secs: 0,
            scrub_max_files_per_run: 0,
            max_cache_bytes: None,
            repo_rules: vec![],
            upstreams: vec![],
            routing_proxy_hosts: vec![],
            routing_trust_x_forwarded_host: false,
        };
        let proxy = crate::proxy::Proxy::new(&proxy_cfg).unwrap().unwrap();
        (storage, proxy, temp_dir)
    }

    #[tokio::test]
    async fn test_collect_protected_blobs_protects_oci_artifact_blobs_and_subject() {
        let (storage, proxy, _temp) = create_test_env();
        let repo = "library/artifact";

        let blob_digest = Digest::parse(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let subject_blob_digest = Digest::parse(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
        .unwrap();

        let subject_manifest = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
            },
            "layers": [
                { "digest": subject_blob_digest.as_str() }
            ]
        });
        let subject_bytes = serde_json::to_vec(&subject_manifest).unwrap();
        let mut hasher = sha2::Sha256::new();
        sha2::Digest::update(&mut hasher, &subject_bytes);
        let subject_digest = Digest::parse(&format!(
            "sha256:{}",
            hex::encode(sha2::Digest::finalize(hasher))
        ))
        .unwrap();
        storage
            .manifest_lifecycle()
            .put_manifest(repo, &subject_digest, bytes::Bytes::from(subject_bytes))
            .await
            .unwrap();

        let artifact_manifest = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.artifact.manifest.v1+json",
            "blobs": [
                { "digest": blob_digest.as_str() }
            ],
            "subject": {
                "digest": subject_digest.as_str()
            }
        });
        let artifact_bytes = serde_json::to_vec(&artifact_manifest).unwrap();
        let mut hasher2 = sha2::Sha256::new();
        sha2::Digest::update(&mut hasher2, &artifact_bytes);
        let artifact_digest = Digest::parse(&format!(
            "sha256:{}",
            hex::encode(sha2::Digest::finalize(hasher2))
        ))
        .unwrap();
        storage
            .manifest_lifecycle()
            .put_manifest(repo, &artifact_digest, bytes::Bytes::from(artifact_bytes))
            .await
            .unwrap();

        let mut protected_blobs = HashSet::new();
        let mut seen_manifests = HashSet::new();

        let proxy_cache = storage.proxy_storage();
        collect_protected_blobs_for_manifest(
            &proxy_cache,
            repo,
            &artifact_digest,
            &mut protected_blobs,
            &mut seen_manifests,
            &proxy,
            0,
        )
        .await
        .unwrap();

        assert!(
            protected_blobs.contains(blob_digest.hex()),
            "artifact blob must be protected"
        );
        assert!(
            protected_blobs.contains(subject_blob_digest.hex()),
            "subject layer blob must be protected via subject traversal"
        );
    }

    #[tokio::test]
    async fn test_compute_protected_blobs_aborts_on_unparsable_manifest() {
        let (storage, proxy, _temp) = create_test_env();
        let repo = "library/malformed-repo";

        let malformed_digest = Digest::parse(
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        )
        .unwrap();
        let malformed_manifest = serde_json::json!({
            "schemaVersion": 2,
            "config": { "digest": "sha256:invalid-hex" },
            "layers": []
        });
        storage
            .manifest_lifecycle()
            .put_manifest(
                repo,
                &malformed_digest,
                serde_json::to_vec(&malformed_manifest).unwrap().into(),
            )
            .await
            .unwrap();
        storage
            .manifest_lifecycle()
            .set_tag(repo, "v1", &malformed_digest)
            .await
            .unwrap();

        let rules = vec![crate::config::ProxyRepoRule {
            match_pattern: crate::proxy::ProxyRepoPattern::parse("library/*").unwrap(),
            upstream_repo: None,
            tag_policy: crate::config::TagPolicy::AlwaysRevalidate,
            eviction_policy: crate::config::EvictionPolicy::KeepTags(vec!["v1".to_string()]),
        }];

        let proxy_cache = storage.proxy_storage();
        let result = compute_protected_blobs(&proxy_cache, &rules, &proxy).await;
        assert!(
            result.is_err(),
            "compute_protected_blobs must fail when a pinned manifest is unparsable"
        );
    }
}
