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
    const MAX_BEARER_TOKEN_LEN: usize = 8192;

    let token = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::trim)?;

    if token.is_empty() || token.len() > MAX_BEARER_TOKEN_LEN {
        return None;
    }
    Some(token)
}

fn bearer_claims_are_authenticated(claims: &security::TokenClaims) -> bool {
    claims.sub.as_deref().is_some_and(|s| !s.is_empty())
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
        if let Ok(claims) = security::verify_bearer_token_bound(
            &state.config.token_signing_key,
            token,
            &state.config.token_service,
            state.config.token_ttl_secs,
        ) {
            if bearer_claims_are_authenticated(&claims) {
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
    // If this request is routed to proxy-only mode, disallow all write methods.
    // This prevents ambiguous behavior where a push could populate local storage while pulls
    // on the same repo are expected to come from proxy/cache.
    if crate::request_routing::v2_route_mode_for_request(&state.config.proxy, request.headers())
        == crate::request_routing::V2RouteMode::ProxyOnly
    {
        match *request.method() {
            http::Method::GET | http::Method::HEAD => {}
            _ => return StatusCode::METHOD_NOT_ALLOWED.into_response(),
        }
    }

    // Policy: anonymous pull. Only gate push (write) methods.
    // This keeps `/v2/` ping and all GET/HEAD endpoints anonymous.
    match *request.method() {
        http::Method::GET | http::Method::HEAD => return next.run(request).await,
        _ => {}
    }

    let repo = extract_repo_from_v2_path(request.uri().path());
    let Some(repo_name) = repo.as_deref() else {
        return unauthorized_registry_challenge(&state, None);
    };

    // If auth is not configured, reject pushes by default (safe default).
    let Some(expected_user) = state.config.push_username.as_deref() else {
        return unauthorized_registry_challenge(&state, Some(repo_name));
    };
    let Some(expected_pass) = state.config.push_password.as_deref() else {
        return unauthorized_registry_challenge(&state, Some(repo_name));
    };

    // Prefer Bearer for container clients; they typically expect token flows.
    if let Some(token) = bearer_token_from_headers(request.headers()) {
        if let Ok(claims) = security::verify_bearer_token_bound(
            &state.config.token_signing_key,
            token,
            &state.config.token_service,
            state.config.token_ttl_secs,
        ) {
            if !bearer_claims_are_authenticated(&claims) {
                return unauthorized_registry_challenge(&state, Some(repo_name));
            }

            if security::token_allows_repo_action(&claims, repo_name, security::RepoAction::Push) {
                if let Some(allowlist) = state.config.push_allow_repos.as_deref() {
                    if !repo_allowed(allowlist, repo_name) {
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
                if !repo_allowed(allowlist, repo_name) {
                    return errors::denied("push not allowed for this repository").into_response();
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

    unauthorized_registry_challenge(&state, Some(repo_name))
}

#[cfg(test)]
mod tests {
    use super::{
        bearer_claims_are_authenticated, bearer_token_from_headers, extract_repo_from_v2_path,
        repo_allowed,
    };
    use crate::security;
    use axum::http::HeaderMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_secs()
    }

    #[test]
    fn bearer_claims_authentication_requires_non_empty_sub() {
        let signing_key = "test-signing-key";
        let aud = "registry";
        let now = now_secs();
        let scopes: Vec<security::TokenScope> = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "org/repo".to_string(),
            actions: vec!["pull".to_string()],
        }];

        let anon = security::issue_bearer_token(signing_key, aud, None, &scopes, now, now + 3600)
            .expect("issue token");
        let claims_anon =
            security::verify_bearer_token_bound(signing_key, &anon, aud, 3600).expect("verify");
        assert!(!bearer_claims_are_authenticated(&claims_anon));

        let user =
            security::issue_bearer_token(signing_key, aud, Some("user"), &scopes, now, now + 3600)
                .expect("issue token");
        let claims_user =
            security::verify_bearer_token_bound(signing_key, &user, aud, 3600).expect("verify");
        assert!(bearer_claims_are_authenticated(&claims_user));
    }

    #[test]
    fn extract_repo_from_v2_path_parses_repo_names() {
        assert_eq!(
            extract_repo_from_v2_path("/v2/library/alpine/manifests/latest").as_deref(),
            Some("library/alpine")
        );
        assert_eq!(
            extract_repo_from_v2_path("/v2/org/repo/blobs/sha256:deadbeef").as_deref(),
            Some("org/repo")
        );
        assert_eq!(
            extract_repo_from_v2_path("/v2/org/repo/tags/list").as_deref(),
            Some("org/repo")
        );

        assert!(extract_repo_from_v2_path("/v2/").is_none());
        assert!(extract_repo_from_v2_path("/v2").is_none());
        assert!(extract_repo_from_v2_path("/notv2/org/repo/manifests/latest").is_none());

        // Upload paths are repository-scoped too (auth gating needs the repo).
        assert_eq!(
            extract_repo_from_v2_path("/v2/org/repo/blobs/uploads/").as_deref(),
            Some("org/repo")
        );

        // Missing marker segment should not be treated as a repo.
        assert!(extract_repo_from_v2_path("/v2/org/repo/somethingelse").is_none());
    }

    #[test]
    fn repo_allowed_matches_exact_and_prefix() {
        let allowlist = vec!["org/repo".to_string(), "org/*".to_string()];
        assert!(repo_allowed(&allowlist, "org/repo"));
        assert!(repo_allowed(&allowlist, "org/other"));
        assert!(!repo_allowed(&allowlist, "other/repo"));
    }

    #[test]
    fn repo_allowed_star_allows_everything() {
        let allowlist = vec!["*".to_string()];
        assert!(repo_allowed(&allowlist, "anything/here"));
        assert!(repo_allowed(&allowlist, "single"));
    }

    #[test]
    fn repo_allowed_prefix_matches_exact_prefix_repo_too() {
        let allowlist = vec!["org/*".to_string()];
        // This registry treats 'org/*' as allowing 'org' and 'org/...'.
        assert!(repo_allowed(&allowlist, "org"));
        assert!(repo_allowed(&allowlist, "org/repo"));
        assert!(!repo_allowed(&allowlist, "org2/repo"));
    }

    #[test]
    fn bearer_token_from_headers_rejects_empty_and_oversized() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::AUTHORIZATION, "Bearer ".parse().unwrap());
        assert!(bearer_token_from_headers(&headers).is_none());

        let huge = format!("Bearer {}", "a".repeat(9000));
        headers.insert(http::header::AUTHORIZATION, huge.parse().unwrap());
        assert!(bearer_token_from_headers(&headers).is_none());
    }
}
