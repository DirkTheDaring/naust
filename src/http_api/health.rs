//! Process probes. These routes sit outside `/v2` auth: a registry that
//! challenges anonymous clients with 401 is still ready to serve (ADR-017).

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;

use crate::app_state::{AppState, AuthMetrics};

pub async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, json_headers(), r#"{"status":"ok"}"#)
}

pub async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let body = render_metrics(
        &state.auth_metrics,
        state
            .active_non_upload_requests
            .load(std::sync::atomic::Ordering::Relaxed),
        state
            .active_upload_requests
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    (StatusCode::OK, prometheus_headers(), body)
}

pub(crate) fn render_metrics(
    metrics: &AuthMetrics,
    active_non_upload: u64,
    active_upload: u64,
) -> String {
    format!(
        "\
# HELP naust_token_issued_total Bearer tokens minted by /token.\n\
# TYPE naust_token_issued_total counter\n\
naust_token_issued_total {issued}\n\
# HELP naust_token_denied_total Token requests denied before a token was minted.\n\
# TYPE naust_token_denied_total counter\n\
naust_token_denied_total {denied}\n\
# HELP naust_token_internal_error_total Token requests that failed while minting.\n\
# TYPE naust_token_internal_error_total counter\n\
naust_token_internal_error_total {errors}\n\
# HELP naust_active_non_upload_requests Non-upload HTTP requests currently in flight.\n\
# TYPE naust_active_non_upload_requests gauge\n\
naust_active_non_upload_requests {active_non_upload}\n\
# HELP naust_active_upload_requests Upload HTTP requests currently in flight.\n\
# TYPE naust_active_upload_requests gauge\n\
naust_active_upload_requests {active_upload}\n",
        issued = metrics.token_issued_total(),
        denied = metrics.token_denied_total(),
        errors = metrics.token_internal_error_total(),
        active_non_upload = active_non_upload,
        active_upload = active_upload,
    )
}

fn json_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
    headers.insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    headers
}

fn prometheus_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "text/plain; version=0.0.4; charset=utf-8".parse().unwrap(),
    );
    headers.insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_text_exposes_token_counters_and_in_flight_gauges() {
        let metrics = AuthMetrics::default();
        metrics.inc_token_issued();
        metrics.inc_token_denied();
        metrics.inc_token_denied();
        let body = render_metrics(&metrics, 3, 1);
        assert!(body.contains("naust_token_issued_total 1\n"));
        assert!(body.contains("naust_token_denied_total 2\n"));
        assert!(body.contains("naust_token_internal_error_total 0\n"));
        assert!(body.contains("naust_active_non_upload_requests 3\n"));
        assert!(body.contains("naust_active_upload_requests 1\n"));
        assert!(!body.contains("repository"));
    }
}
