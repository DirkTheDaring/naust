use crate::{http_api::errors, AppState};
use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::Engine as _;
use headers::{authorization::Basic, Authorization, HeaderMapExt};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

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

#[derive(Debug, Deserialize)]
struct TokenScope {
    #[serde(rename = "type")]
    typ: String,
    name: String,
    actions: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct TokenPayload {
    exp: u64,
    #[serde(default)]
    scopes: Vec<TokenScope>,
}

fn verify_bearer_token(signing_key: &str, token: &str) -> Option<TokenPayload> {
    let (payload_b64, sig_b64) = token.split_once('.')?;

    let sig = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(sig_b64.as_bytes())
        .ok()?;

    let mut mac = Hmac::<Sha256>::new_from_slice(signing_key.as_bytes()).ok()?;
    mac.update(payload_b64.as_bytes());
    mac.verify_slice(&sig).ok()?;

    let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64.as_bytes())
        .ok()?;
    let payload: TokenPayload = serde_json::from_slice(&payload_bytes).ok()?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_secs();
    if now > payload.exp {
        return None;
    }

    Some(payload)
}

fn bearer_allows_push(payload: &TokenPayload, repo: &str) -> bool {
    payload.scopes.iter().any(|s| {
        s.typ == "repository"
            && s.name == repo
            && s.actions.iter().any(|a| a == "push")
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
    if let Some(authz) = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(token) = authz.strip_prefix("Bearer ") {
            if let (Some(repo), Some(payload)) = (repo.as_deref(), verify_bearer_token(&state.config.token_signing_key, token.trim())) {
                if bearer_allows_push(&payload, repo) {
                    if let Some(allowlist) = state.config.push_allow_repos.as_deref() {
                        if !repo_allowed(allowlist, repo) {
                            return errors::denied("push not allowed for this repository").into_response();
                        }
                    }
                    return next.run(request).await;
                }
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
                        return errors::denied("push not allowed for this repository").into_response();
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
