mod auth;
mod config;
mod http_api;
mod storage;

use axum::{
    routing::{any, get},
    Router,
};
use config::Config;
use http_api::handlers;
use std::sync::Arc;
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
    let storage = Arc::new(storage::from_config(&config));
    let state = AppState {
        config,
        storage,
    };

    // Anonymous pull routes (MVP will expand these).
    let pull = Router::new()
        .route("/v2", get(handlers::ping))
        .route("/v2/", get(handlers::ping));

    // Authenticated push routes (handlers stubbed for now).
    let push = Router::new()
        .route("/v2/:name/blobs/uploads/", any(handlers::not_implemented))
        .route(
            "/v2/:name/blobs/uploads/:uuid",
            any(handlers::not_implemented),
        )
        .route("/v2/:name/manifests/:reference", any(handlers::not_implemented));

    let app = pull
        .merge(
            push.layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth::require_push_basic_auth,
            )),
        )
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    tracing::info!(%addr, "registry listening");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind listen addr");
    axum::serve(listener, app).await.expect("serve");
}
