use crate::{AppState, http_api::errors};
use axum::{
    body::Body,
    extract::State,
    http::HeaderMap,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use headers::{Authorization, HeaderMapExt, authorization::Basic};
use tracing::info;

use crate::security;

fn bearer_token_from_headers(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::trim)
}

pub(crate) fn extract_repo_from_v2_path(path: &str) -> Option<String> {
    // Path is expected to look like:
    //   /v2/<name>/blobs/...
    //   /v2/<name>/manifests/...
    //   /v2/<name>/tags/list
    // where <name> may contain '/'.
    if !path.starts_with("/v2/") {
        return None;
    }
    let segments: Vec<&str> = path
        .trim_start_matches("/v2/")
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    if segments.is_empty() {
        return None;
    }

    let marker_idx = segments
        .iter()
        .position(|s| *s == "blobs" || *s == "manifests" || *s == "tags");
    let Some(marker_idx) = marker_idx else {
        return None;
    };
    if marker_idx == 0 {
        return None;
    }
    Some(segments[..marker_idx].join("/"))
}

pub(crate) fn repo_allowed(allowlist: &[String], repo: &str) -> bool {
    allowlist.iter().any(|pat| {
        if pat == "*" {
            return true;
        }
        if let Some(prefix) = pat.strip_suffix("/*") {
            return repo == prefix || repo.starts_with(&format!("{prefix}/"));
        }
        pat == repo
    })
}

fn unauthorized_registry_challenge(state: &AppState, repo: Option<&str>) -> Response {
    let mut resp: Response = StatusCode::UNAUTHORIZED.into_response();

    let realm = state
        .config
        .public_url
        .as_deref()
        .unwrap_or("http://127.0.0.1:5000")
        .trim_end_matches('/');

    let mut bearer = format!(
        "Bearer realm=\"{realm}/token\",service=\"{}\"",
        state.config.token_service
    );

    if let Some(repo) = repo {
        bearer.push_str(&format!(",scope=\"repository:{repo}:pull,push\""));
    }

    if let Ok(v) = http::HeaderValue::from_str(&bearer) {
        resp.headers_mut().insert(http::header::WWW_AUTHENTICATE, v);
    }
    // Also advertise Basic so curl workflows keep working.
    resp.headers_mut().append(
        http::header::WWW_AUTHENTICATE,
        http::HeaderValue::from_static("Basic realm=\"registry\""),
    );

    resp.headers_mut().insert(
        http::header::HeaderName::from_static("docker-distribution-api-version"),
        http::HeaderValue::from_static("registry/2.0"),
    );
    resp
}

pub(crate) fn unauthorized_catalog_challenge(state: &AppState) -> Response {
    unauthorized_registry_challenge(state, None)
}

pub(crate) fn is_authenticated(state: &AppState, headers: &HeaderMap) -> bool {
    // If auth isn't configured, treat as unauthenticated.
    let Some(expected_user) = state.config.push_username.as_deref() else {
        return false;
    };
    let Some(expected_pass) = state.config.push_password.as_deref() else {
        return false;
    };

    // Bearer: accept any valid, unexpired token minted by this registry.
    if let Some(token) = bearer_token_from_headers(headers) {
        if let Ok(claims) = security::verify_bearer_token(&state.config.token_signing_key, token) {
            // Treat only tokens with an authenticated subject as "authenticated".
            // Anonymous pull tokens (sub missing) should not satisfy catalog auth.
            if claims.sub.as_deref().is_some_and(|s| !s.is_empty()) {
                return true;
            }
        }
    }

    // Basic: accept configured push credentials.
    if let Some(Authorization(basic)) = headers.typed_get::<Authorization<Basic>>() {
        return basic.username() == expected_user && basic.password() == expected_pass;
    }

    false
}

pub async fn require_push_basic_auth(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    // Policy: anonymous pull. Only gate push (write) methods.
    // This keeps `/v2/` ping and all GET/HEAD endpoints anonymous.
    match *request.method() {
        http::Method::GET | http::Method::HEAD => return next.run(request).await,
        _ => {}
    }

    let repo = extract_repo_from_v2_path(request.uri().path());

    // If auth is not configured, reject pushes by default (safe default).
    let Some(expected_user) = state.config.push_username.as_deref() else {
        return unauthorized_registry_challenge(&state, repo.as_deref());
    };
    let Some(expected_pass) = state.config.push_password.as_deref() else {
        return unauthorized_registry_challenge(&state, repo.as_deref());
    };

    // Prefer Bearer for container clients; they typically expect token flows.
    if let (Some(token), Some(repo)) = (
        bearer_token_from_headers(request.headers()),
        repo.as_deref(),
    ) {
        if let Ok(claims) = security::verify_bearer_token(&state.config.token_signing_key, token) {
            if security::token_allows_repo_action(&claims, repo, security::RepoAction::Push) {
                if let Some(allowlist) = state.config.push_allow_repos.as_deref() {
                    if !repo_allowed(allowlist, repo) {
                        return errors::denied("push not allowed for this repository")
                            .into_response();
                    }
                }
                return next.run(request).await;
            }
        }
    }

    if let Some(Authorization(basic)) = request.headers().typed_get::<Authorization<Basic>>() {
        let user_ok = basic.username() == expected_user;
        let pass_ok = basic.password() == expected_pass;
        if user_ok && pass_ok {
            if let Some(allowlist) = state.config.push_allow_repos.as_deref() {
                if let Some(repo) = repo.as_deref() {
                    if !repo_allowed(allowlist, repo) {
                        return errors::denied("push not allowed for this repository")
                            .into_response();
                    }
                }
            }
            return next.run(request).await;
        }
    }

    let auth_scheme = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split_whitespace().next())
        .unwrap_or("<none>");
    let user_agent = request
        .headers()
        .get(http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("<none>");
    info!(
        method = %request.method(),
        uri = %request.uri(),
        auth_scheme = auth_scheme,
        user_agent = user_agent,
        "push auth denied"
    );

    unauthorized_registry_challenge(&state, repo.as_deref())
}
