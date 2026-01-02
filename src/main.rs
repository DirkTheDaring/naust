mod auth;
mod config;
mod http_api;
mod registry;
mod storage;

use axum::{
    extract::DefaultBodyLimit,
    http::Request,
    middleware::Next,
    response::IntoResponse,
    routing::{any, get},
    Router,
};
use config::{Config, StorageBackend};
use http_api::handlers;
use std::sync::Arc;
use std::time::Duration;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub storage: Arc<dyn storage::Storage>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Arc::new(Config::from_env());
    let addr = config.listen_addr;
    let storage = storage::from_config(config.as_ref());
    let state = AppState {
        config,
        storage,
    };

    // For large blobs we stream request bodies; enforce blob size via MAX_UPLOAD_BYTES and
    // enforce manifest size in-handler (read_body_limited). So we disable the default body
    // limit on the registry API router.
    let v2_body_limit = DefaultBodyLimit::disable();

    // `/v2/*rest` owns all registry API subpaths (repo names can contain `/`).
    // We gate write methods (push) via middleware; GET/HEAD stay anonymous.
    let v2 = Router::new()
        .route("/v2", get(handlers::ping))
        .route("/v2/", get(handlers::ping))
        .route("/v2/*rest", any(handlers::v2_dispatch))
        .layer(v2_body_limit)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_push_basic_auth,
        ));

    // Operational metadata / inventory endpoints (non-standard).
    let meta = Router::new()
        .route("/_meta/catalog", get(handlers::meta_catalog))
        .route("/_meta/orgs", get(handlers::meta_orgs))
        .route("/_meta/orgs/:org/repos", get(handlers::meta_org_repos))
        .route("/_meta/repos/*name", get(handlers::meta_repo));

    let tls_cert_path = state.config.tls_cert_path.clone();
    let tls_key_path = state.config.tls_key_path.clone();

    spawn_upload_gc(state.clone());

    let app = Router::new()
        .route("/token", get(handlers::token))
        .merge(meta)
        .merge(v2)
        .with_state(state.clone())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            request_timeout_by_path,
        ))
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http());

    tracing::info!(%addr, "registry listening");

    if let (Some(cert), Some(key)) = (tls_cert_path, tls_key_path) {
        let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
            .await
            .expect("load TLS cert/key");
        let handle = axum_server::Handle::new();
        let handle_for_shutdown = handle.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            handle_for_shutdown.graceful_shutdown(Some(Duration::from_secs(10)));
        });

        axum_server::bind_rustls(addr, tls)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .expect("serve https");
    } else {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("bind listen addr");
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .expect("serve http");
    }
}

async fn request_timeout_by_path(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> axum::response::Response {
    let path = req.uri().path();
    let timeout_secs = if is_upload_path(path) {
        state.config.upload_request_timeout_secs
    } else {
        state.config.request_timeout_secs
    };

    match tokio::time::timeout(Duration::from_secs(timeout_secs), next.run(req)).await {
        Ok(resp) => resp,
        Err(_) => axum::http::StatusCode::REQUEST_TIMEOUT.into_response(),
    }
}

fn is_upload_path(path: &str) -> bool {
    path.starts_with("/v2/") && path.contains("/blobs/uploads")
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("install Ctrl-C handler");
    };

    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{signal, SignalKind};
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

fn spawn_upload_gc(state: AppState) {
    if state.config.storage_backend != StorageBackend::Filesystem {
        return;
    }
    if !state.config.upload_gc_enabled {
        return;
    }

    let uploads_dir = state.config.fs_root.join("uploads");
    let interval = Duration::from_secs(state.config.upload_gc_interval_secs.max(1));
    let max_age = Duration::from_secs(state.config.upload_gc_max_age_secs);

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let now = std::time::SystemTime::now();

            let mut dir = match tokio::fs::read_dir(&uploads_dir).await {
                Ok(d) => d,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    tracing::warn!(error = %err, path = %uploads_dir.display(), "upload gc: read_dir failed");
                    continue;
                }
            };

            let mut removed = 0u64;
            let mut scanned = 0u64;
            while let Ok(Some(entry)) = dir.next_entry().await {
                scanned += 1;
                let path = entry.path();

                let meta = match entry.metadata().await {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                let modified = match meta.modified() {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                let age = match now.duration_since(modified) {
                    Ok(d) => d,
                    Err(_) => Duration::from_secs(0),
                };

                if age >= max_age {
                    if tokio::fs::remove_file(&path).await.is_ok() {
                        removed += 1;
                    }
                }
            }

            if removed > 0 {
                tracing::info!(scanned, removed, path = %uploads_dir.display(), "upload gc: removed stale temp files");
            }
        }
    });
}
