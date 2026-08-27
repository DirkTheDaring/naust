use crate::AppState;
use axum::{
    extract::{Json, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use headers::{Authorization, HeaderMapExt, authorization::Basic};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct AdminGcBudgetsRequest {
    #[serde(default)]
    pub max_blobs: Option<usize>,
    #[serde(default)]
    pub max_bytes: Option<u64>,
    #[serde(default)]
    pub max_seconds: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct AdminGcPlanRequest {
    #[serde(default)]
    pub policy: Option<String>,
    #[serde(default)]
    pub min_age_secs: Option<u64>,
    #[serde(default)]
    pub budgets: Option<AdminGcBudgetsRequest>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct AdminGcQuarantineRequest {
    #[serde(default)]
    pub policy: Option<String>,
    #[serde(default)]
    pub min_age_secs: Option<u64>,
    #[serde(default)]
    pub budgets: Option<AdminGcBudgetsRequest>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct AdminGcDeleteRequest {
    #[serde(default)]
    pub policy: Option<String>,
    #[serde(default)]
    pub quarantine_delay_secs: Option<u64>,
    #[serde(default)]
    pub budgets: Option<AdminGcBudgetsRequest>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AdminGcStatsResponse {
    pub scanned_blobs: u64,
    pub scanned_bytes: u64,
    pub eligible_blobs: u64,
    pub eligible_bytes: u64,
    pub quarantined_blobs: u64,
    pub quarantined_bytes: u64,
    pub restored_blobs: u64,
    pub restored_bytes: u64,
    pub deleted_blobs: u64,
    pub deleted_bytes: u64,
}

impl From<crate::blob_gc::BlobGcStats> for AdminGcStatsResponse {
    fn from(s: crate::blob_gc::BlobGcStats) -> Self {
        Self {
            scanned_blobs: s.scanned_blobs,
            scanned_bytes: s.scanned_bytes,
            eligible_blobs: s.eligible_blobs,
            eligible_bytes: s.eligible_bytes,
            quarantined_blobs: s.quarantined_blobs,
            quarantined_bytes: s.quarantined_bytes,
            restored_blobs: s.restored_blobs,
            restored_bytes: s.restored_bytes,
            deleted_blobs: s.deleted_blobs,
            deleted_bytes: s.deleted_bytes,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct AdminGcResponse {
    pub run_id: u64,
    pub stats: AdminGcStatsResponse,
}

pub fn admin_basic_subject(
    cfg: &crate::config::Config,
    headers: &HeaderMap,
) -> Result<String, Response> {
    if !cfg.admin_api.enabled {
        return Err(StatusCode::NOT_FOUND.into_response());
    }

    let Some(expected_user) = cfg.admin_api.username.as_deref() else {
        return Err((
            StatusCode::FORBIDDEN,
            "admin api enabled but no credentials configured",
        )
            .into_response());
    };
    let Some(expected_pass) = cfg.admin_api.password.as_deref() else {
        return Err((
            StatusCode::FORBIDDEN,
            "admin api enabled but no credentials configured",
        )
            .into_response());
    };

    let basic = headers
        .typed_get::<Authorization<Basic>>()
        .map(|Authorization(b)| (b.username().to_string(), b.password().to_string()));

    let Some((user, pass)) = basic else {
        let mut resp = StatusCode::UNAUTHORIZED.into_response();
        resp.headers_mut().insert(
            http::header::WWW_AUTHENTICATE,
            http::HeaderValue::from_static("Basic realm=\"registry-admin\""),
        );
        return Err(resp);
    };

    if user != expected_user || pass != expected_pass {
        let mut resp = StatusCode::UNAUTHORIZED.into_response();
        resp.headers_mut().insert(
            http::header::WWW_AUTHENTICATE,
            http::HeaderValue::from_static("Basic realm=\"registry-admin\""),
        );
        return Err(resp);
    }

    Ok(user)
}

fn parse_admin_policy(s: &str) -> Result<crate::blob_gc::BlobGcPolicy, Response> {
    match s.trim().to_ascii_lowercase().as_str() {
        "tag_rooted" | "tag" => Ok(crate::blob_gc::BlobGcPolicy::TagRooted),
        "manifest_rooted" | "manifest" => Ok(crate::blob_gc::BlobGcPolicy::ManifestRooted),
        _ => Err((
            StatusCode::BAD_REQUEST,
            "invalid policy (expected tag_rooted or manifest_rooted)",
        )
            .into_response()),
    }
}

fn defaults_budgets(
    cfg: &crate::config::Config,
    b: Option<AdminGcBudgetsRequest>,
) -> crate::gc_service::GcBudgets {
    let b = b.unwrap_or_default();
    crate::gc_service::GcBudgets {
        max_blobs: b.max_blobs.unwrap_or(cfg.blob_gc_default_max_blobs),
        max_bytes: b.max_bytes.unwrap_or(cfg.blob_gc_default_max_bytes),
        max_seconds: b.max_seconds.unwrap_or(cfg.blob_gc_default_max_seconds),
    }
}

pub async fn admin_gc_plan(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<AdminGcPlanRequest>,
) -> Response {
    let subject = match admin_basic_subject(&state.config, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };

    let Some(service) = state.gc_service.as_ref() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "gc service unavailable").into_response();
    };

    let policy = match req.policy.as_deref() {
        None => crate::blob_gc::BlobGcPolicy::ManifestRooted,
        Some(s) => match parse_admin_policy(s) {
            Ok(p) => p,
            Err(resp) => return resp,
        },
    };

    let run_id = state
        .gc_run_seq
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        + 1;

    let budgets = defaults_budgets(&state.config, req.budgets);
    let min_age_secs = req
        .min_age_secs
        .unwrap_or(state.config.blob_gc_default_min_age_secs);
    let min_age = Duration::from_secs(min_age_secs);

    tracing::info!(event = "admin_gc", action = "plan", %subject, run_id, policy = ?policy, min_age_secs);

    match service.plan(policy, min_age, budgets).await {
        Ok(stats) => Json(AdminGcResponse {
            run_id,
            stats: stats.into(),
        })
        .into_response(),
        Err(crate::gc_service::GcServiceError::AlreadyRunning) => {
            (StatusCode::CONFLICT, "gc already running").into_response()
        }
        Err(crate::gc_service::GcServiceError::Disabled)
        | Err(crate::gc_service::GcServiceError::DeleteDisabled) => {
            (StatusCode::FORBIDDEN, "gc disabled").into_response()
        }
        Err(crate::gc_service::GcServiceError::RefIndex(e)) => {
            (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
        }
        Err(crate::gc_service::GcServiceError::StrategyUnsupported { message, .. }) => {
            (StatusCode::UNPROCESSABLE_ENTITY, message).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn admin_gc_quarantine(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<AdminGcQuarantineRequest>,
) -> Response {
    let subject = match admin_basic_subject(&state.config, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };

    let Some(service) = state.gc_service.as_ref() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "gc service unavailable").into_response();
    };

    let policy = match req.policy.as_deref() {
        None => crate::blob_gc::BlobGcPolicy::ManifestRooted,
        Some(s) => match parse_admin_policy(s) {
            Ok(p) => p,
            Err(resp) => return resp,
        },
    };

    let run_id = state
        .gc_run_seq
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        + 1;

    let budgets = defaults_budgets(&state.config, req.budgets);
    let min_age_secs = req
        .min_age_secs
        .unwrap_or(state.config.blob_gc_default_min_age_secs);
    let min_age = Duration::from_secs(min_age_secs);

    tracing::info!(event = "admin_gc", action = "quarantine", %subject, run_id, policy = ?policy, min_age_secs);

    match service.quarantine(policy, min_age, budgets).await {
        Ok(stats) => Json(AdminGcResponse {
            run_id,
            stats: stats.into(),
        })
        .into_response(),
        Err(crate::gc_service::GcServiceError::AlreadyRunning) => {
            (StatusCode::CONFLICT, "gc already running").into_response()
        }
        Err(crate::gc_service::GcServiceError::Disabled)
        | Err(crate::gc_service::GcServiceError::DeleteDisabled) => {
            (StatusCode::FORBIDDEN, "gc disabled").into_response()
        }
        Err(crate::gc_service::GcServiceError::RefIndex(e)) => {
            (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
        }
        Err(crate::gc_service::GcServiceError::StrategyUnsupported { message, .. }) => {
            (StatusCode::UNPROCESSABLE_ENTITY, message).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn admin_gc_delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<AdminGcDeleteRequest>,
) -> Response {
    let subject = match admin_basic_subject(&state.config, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };

    let Some(service) = state.gc_service.as_ref() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "gc service unavailable").into_response();
    };

    let policy = match req.policy.as_deref() {
        None => crate::blob_gc::BlobGcPolicy::ManifestRooted,
        Some(s) => match parse_admin_policy(s) {
            Ok(p) => p,
            Err(resp) => return resp,
        },
    };

    let run_id = state
        .gc_run_seq
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        + 1;

    let budgets = defaults_budgets(&state.config, req.budgets);
    let quarantine_delay_secs = req
        .quarantine_delay_secs
        .unwrap_or(state.config.blob_gc_default_quarantine_delay_secs);
    let quarantine_delay = Duration::from_secs(quarantine_delay_secs);

    tracing::info!(event = "admin_gc", action = "delete", %subject, run_id, policy = ?policy, quarantine_delay_secs);

    match service.delete(policy, quarantine_delay, budgets).await {
        Ok(stats) => Json(AdminGcResponse {
            run_id,
            stats: stats.into(),
        })
        .into_response(),
        Err(crate::gc_service::GcServiceError::AlreadyRunning) => {
            (StatusCode::CONFLICT, "gc already running").into_response()
        }
        Err(crate::gc_service::GcServiceError::Disabled) => {
            (StatusCode::FORBIDDEN, "gc disabled").into_response()
        }
        Err(crate::gc_service::GcServiceError::DeleteDisabled) => {
            (StatusCode::FORBIDDEN, "gc delete disabled").into_response()
        }
        Err(crate::gc_service::GcServiceError::RefIndex(e)) => {
            (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
        }
        Err(crate::gc_service::GcServiceError::StrategyUnsupported { message, .. }) => {
            (StatusCode::UNPROCESSABLE_ENTITY, message).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn admin_gc_health(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let _subject = match admin_basic_subject(&state.config, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };

    let Some(service) = state.gc_service.as_ref() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "gc service unavailable").into_response();
    };

    match service.health().await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(crate::gc_service::GcServiceError::RefIndex(e)) => {
            (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
        }
        Err(crate::gc_service::GcServiceError::AlreadyRunning) => {
            (StatusCode::OK, "gc running").into_response()
        }
        Err(crate::gc_service::GcServiceError::Disabled)
        | Err(crate::gc_service::GcServiceError::DeleteDisabled) => StatusCode::OK.into_response(),
        Err(crate::gc_service::GcServiceError::StrategyUnsupported { message, .. }) => {
            (StatusCode::UNPROCESSABLE_ENTITY, message).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
