use crate::AppState;
use axum::{
    extract::{Json, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use headers::{Authorization, HeaderMapExt, authorization::Basic};
use serde::{Deserialize, Serialize};

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

    let user_ok = crate::security::constant_time_eq(&user, expected_user);
    let pass_ok = crate::security::constant_time_eq(&pass, expected_pass);
    if !user_ok || !pass_ok {
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

fn overrides(b: Option<AdminGcBudgetsRequest>) -> crate::gc_admin::BudgetOverrides {
    let b = b.unwrap_or_default();
    crate::gc_admin::BudgetOverrides {
        max_blobs: b.max_blobs,
        max_bytes: b.max_bytes,
        max_seconds: b.max_seconds,
    }
}

fn gc_admin_error_to_response(err: crate::gc_admin::GcAdminError) -> Response {
    use crate::gc_service::GcServiceError;
    match err {
        crate::gc_admin::GcAdminError::Unavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "gc service unavailable").into_response()
        }
        crate::gc_admin::GcAdminError::Service(GcServiceError::AlreadyRunning) => {
            (StatusCode::CONFLICT, "gc already running").into_response()
        }
        crate::gc_admin::GcAdminError::Service(
            GcServiceError::Disabled | GcServiceError::DeleteDisabled,
        ) => (StatusCode::FORBIDDEN, "gc disabled").into_response(),
        crate::gc_admin::GcAdminError::Service(GcServiceError::RefIndex(e)) => {
            (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
        }
        crate::gc_admin::GcAdminError::Service(GcServiceError::StrategyUnsupported {
            message,
            ..
        }) => (StatusCode::UNPROCESSABLE_ENTITY, message).into_response(),
        crate::gc_admin::GcAdminError::Service(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

fn parse_policy_param(policy: Option<&str>) -> Result<crate::blob_gc::BlobGcPolicy, Response> {
    match policy {
        None => Ok(crate::blob_gc::BlobGcPolicy::ManifestRooted),
        Some(s) => parse_admin_policy(s),
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
    let policy = match parse_policy_param(req.policy.as_deref()) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    tracing::info!(event = "admin_gc", action = "plan", %subject, policy = ?policy, min_age_secs = ?req.min_age_secs);
    match state
        .gc_admin
        .plan(policy, req.min_age_secs, overrides(req.budgets))
        .await
    {
        Ok((run_id, stats)) => Json(AdminGcResponse {
            run_id,
            stats: stats.into(),
        })
        .into_response(),
        Err(err) => gc_admin_error_to_response(err),
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
    let policy = match parse_policy_param(req.policy.as_deref()) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    tracing::info!(event = "admin_gc", action = "quarantine", %subject, policy = ?policy, min_age_secs = ?req.min_age_secs);
    match state
        .gc_admin
        .quarantine(policy, req.min_age_secs, overrides(req.budgets))
        .await
    {
        Ok((run_id, stats)) => Json(AdminGcResponse {
            run_id,
            stats: stats.into(),
        })
        .into_response(),
        Err(err) => gc_admin_error_to_response(err),
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
    let policy = match parse_policy_param(req.policy.as_deref()) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    tracing::info!(event = "admin_gc", action = "delete", %subject, policy = ?policy, quarantine_delay_secs = ?req.quarantine_delay_secs);
    match state
        .gc_admin
        .delete(policy, req.quarantine_delay_secs, overrides(req.budgets))
        .await
    {
        Ok((run_id, stats)) => Json(AdminGcResponse {
            run_id,
            stats: stats.into(),
        })
        .into_response(),
        Err(err) => gc_admin_error_to_response(err),
    }
}

pub async fn admin_gc_health(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let _subject = match admin_basic_subject(&state.config, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    use crate::gc_service::GcServiceError;
    match state.gc_admin.health().await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(crate::gc_admin::GcAdminError::Service(GcServiceError::AlreadyRunning)) => {
            (StatusCode::OK, "gc running").into_response()
        }
        Err(crate::gc_admin::GcAdminError::Service(
            GcServiceError::Disabled | GcServiceError::DeleteDisabled,
        )) => StatusCode::OK.into_response(),
        Err(err) => gc_admin_error_to_response(err),
    }
}
