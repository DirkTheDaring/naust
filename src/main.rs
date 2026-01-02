mod auth;
mod config;
mod http_api;
mod registry;
mod storage;

use axum::{
    error_handling::HandleErrorLayer,
    extract::DefaultBodyLimit,
    routing::{any, get},
    Router,
};
use config::Config;
use http_api::handlers;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceBuilder;
use tower::timeout::TimeoutLayer;
use tower::BoxError;
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

    let timeout = TimeoutLayer::new(Duration::from_secs(state.config.request_timeout_secs));
    let body_limit = DefaultBodyLimit::max(state.config.max_request_body_bytes);

    let hardening = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(|_err: BoxError| async {
            axum::http::StatusCode::REQUEST_TIMEOUT
        }))
        .layer(timeout);

    // `/v2/*rest` owns all registry API subpaths (repo names can contain `/`).
    // We gate write methods (push) via middleware; GET/HEAD stay anonymous.
    let v2 = Router::new()
        .route("/v2", get(handlers::ping))
        .route("/v2/", get(handlers::ping))
        .route("/v2/*rest", any(handlers::v2_dispatch))
        .layer(body_limit)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_push_basic_auth,
        ));

    let app = v2
        .with_state(state)
        .layer(hardening)
        .layer(TraceLayer::new_for_http());

    tracing::info!(%addr, "registry listening");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind listen addr");
    axum::serve(listener, app).await.expect("serve");
}
