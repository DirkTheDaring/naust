use crate::{AppState, http_api::errors, http_api::handlers::registry_headers, security};
use axum::{
    body::Body,
    extract::{RawQuery, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use headers::{Authorization, HeaderMapExt, authorization::Basic};
use url::form_urlencoded;

pub use naust_auth::policy::{
    TokenDecision, TokenRejection, parse_scopes, sanitize_token_scopes, service_param_is_valid,
    token_scope_requests_repo_action, wants_push_from_token_scopes,
};

pub fn wants_auth_from_token_scopes(
    cfg: &crate::config::Config,
    token_scopes: &[security::TokenScope],
) -> bool {
    naust_auth::policy::wants_auth_from_token_scopes(&cfg.into(), token_scopes)
}

pub fn decide_token_scopes_for_request(
    cfg: &crate::config::Config,
    token_scopes: &[security::TokenScope],
    basic: Option<(String, String)>,
) -> Result<TokenDecision, TokenRejection> {
    naust_auth::policy::decide_token_scopes_for_request(&cfg.into(), token_scopes, basic)
}

pub fn token_unauthorized() -> Response {
    let mut resp = errors::unauthorized("authentication required");
    let basic = "Basic realm=\"registry\"";
    if let Ok(v) = http::HeaderValue::from_str(basic) {
        resp.headers_mut().insert(http::header::WWW_AUTHENTICATE, v);
    }
    resp
}

pub fn issue_token(
    state: &AppState,
    subject: Option<&str>,
    scopes: &[security::TokenScope],
    iat: u64,
    exp: u64,
) -> Result<String, security::TokenError> {
    let signing_key = state
        .config
        .token_signing_keys
        .first()
        .cloned()
        .unwrap_or_else(|| security::TokenSigningKey {
            kid: "default".to_string(),
            key: state.config.token_signing_key.clone(),
        });

    security::issue_bearer_token_with_key(
        &signing_key,
        &state.config.token_service,
        subject,
        scopes,
        iat,
        exp,
    )
}

pub async fn token(
    State(state): State<AppState>,
    raw_query: RawQuery,
    headers: HeaderMap,
) -> Response {
    // Parse (transport concern) …
    let mut service_param: Option<String> = None;
    let mut scopes_raw: Vec<String> = Vec::new();
    let raw = raw_query.0.unwrap_or_default();
    for (k, v) in form_urlencoded::parse(raw.as_bytes()) {
        if k == "service" {
            service_param = Some(v.into_owned());
        } else if k == "scope" {
            scopes_raw.push(v.into_owned());
        }
    }
    let basic = headers
        .typed_get::<Authorization<Basic>>()
        .map(|Authorization(b)| (b.username().to_string(), b.password().to_string()));

    // … delegate (R4/KI-26: the token bypass is closed — issuance lives in
    // TokenService) …
    let outcome = state
        .token_svc
        .issue(service_param.as_deref(), &scopes_raw, basic);

    // … format.
    match outcome {
        crate::token_service::TokenOutcome::Issued { body } => {
            let bytes = match serde_json::to_vec(&body) {
                Ok(b) => b,
                Err(_) => return errors::internal_error().into_response(),
            };
            let mut resp_headers = registry_headers();
            resp_headers.insert("Content-Type", "application/json".parse().unwrap());
            resp_headers.insert("Content-Length", bytes.len().to_string().parse().unwrap());
            (StatusCode::OK, resp_headers, Body::from(bytes)).into_response()
        }
        crate::token_service::TokenOutcome::Unauthorized => token_unauthorized(),
        crate::token_service::TokenOutcome::Denied(msg) => errors::denied(msg).into_response(),
        crate::token_service::TokenOutcome::NameInvalid => errors::name_invalid(),
        crate::token_service::TokenOutcome::Internal => errors::internal_error().into_response(),
    }
}
