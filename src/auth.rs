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

use crate::security;

pub(crate) fn bearer_token_from_headers(headers: &HeaderMap) -> Option<&str> {
    const MAX_BEARER_TOKEN_LEN: usize = 65536;

    let auth_header = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())?
        .trim();

    let token = if let Some(rest) = auth_header.strip_prefix("Bearer ") {
        rest
    } else if let Some(rest) = auth_header.strip_prefix("bearer ") {
        rest
    } else if auth_header.len() >= 7
        && auth_header[..6].eq_ignore_ascii_case("bearer")
        && auth_header.as_bytes()[6] == b' '
    {
        &auth_header[7..]
    } else {
        return None;
    }
    .trim();

    if token.is_empty() || token.len() > MAX_BEARER_TOKEN_LEN {
        return None;
    }
    Some(token)
}

fn bearer_claims_are_authenticated(claims: &security::TokenClaims) -> bool {
    claims.sub.as_deref().is_some_and(|s| !s.is_empty())
}

#[allow(dead_code)]
pub(crate) fn extract_repo_from_v2_path(path: &str) -> Option<String> {
    // Path is expected to look like:
    //   /v2/<name>/blobs/...
    //   /v2/<name>/manifests/...
    //   /v2/<name>/tags/list
    //   /v2/<name>/tags/reference/...
    //   /v2/<name>/referrers/...
    // where <name> may contain '/'.
    let rest = path.strip_prefix("/v2/")?;
    let segments: Vec<&str> = rest
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    if segments.is_empty() {
        return None;
    }

    let marker_idx = segments
        .iter()
        .position(|s| *s == "blobs" || *s == "manifests" || *s == "tags" || *s == "referrers");
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

pub(crate) fn unauthorized_registry_challenge(state: &AppState, repo: Option<&str>) -> Response {
    let mut resp = errors::unauthorized("authentication required");

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

    if state.config.auth_strategy == crate::config::AuthStrategy::Token || state.config.auth_strategy == crate::config::AuthStrategy::Both {
        if let Ok(v) = http::HeaderValue::from_str(&bearer) {
            resp.headers_mut().append(http::header::WWW_AUTHENTICATE, v);
        }
    }
    
    if state.config.auth_strategy == crate::config::AuthStrategy::Basic || state.config.auth_strategy == crate::config::AuthStrategy::Both {
        resp.headers_mut().append(
            http::header::WWW_AUTHENTICATE,
            http::HeaderValue::from_static("Basic realm=\"registry\""),
        );
    }
    resp
}

pub(crate) fn unauthorized_catalog_challenge(state: &AppState) -> Response {
    let mut resp = errors::unauthorized("authentication required");

    let realm = state
        .config
        .public_url
        .as_deref()
        .unwrap_or("http://127.0.0.1:5000")
        .trim_end_matches('/');

    let bearer = format!(
        "Bearer realm=\"{realm}/token\",service=\"{}\",scope=\"registry:catalog:*\"",
        state.config.token_service
    );

    if state.config.auth_strategy == crate::config::AuthStrategy::Token || state.config.auth_strategy == crate::config::AuthStrategy::Both {
        if let Ok(v) = http::HeaderValue::from_str(&bearer) {
            resp.headers_mut().append(http::header::WWW_AUTHENTICATE, v);
        }
    }
    
    if state.config.auth_strategy == crate::config::AuthStrategy::Basic || state.config.auth_strategy == crate::config::AuthStrategy::Both {
        resp.headers_mut().append(
            http::header::WWW_AUTHENTICATE,
            http::HeaderValue::from_static("Basic realm=\"registry\""),
        );
    }
    resp
}

fn verify_any_basic_credentials(
    cfg: &crate::config::Config,
    user: &str,
    pass: &str,
) -> bool {
    if cfg.robots.enabled {
        if let Some(account) = cfg.robots.accounts.iter().find(|a| a.name == user) {
            return crate::robot_secrets::verify_robot_secret(pass, &account.secret_hash);
        }
    }
    if cfg.users.enabled {
        if let Some(account) = cfg.users.accounts.iter().find(|a| a.name == user) {
            return crate::robot_secrets::verify_robot_secret(pass, &account.secret_hash);
        }
    }
    if let (Some(expected_user), Some(expected_pass)) = (
        cfg.push_username.as_deref(),
        cfg.push_password.as_deref(),
    ) {
        if user == expected_user && pass == expected_pass {
            return true;
        }
    }
    false
}

pub(crate) fn is_authenticated(state: &AppState, headers: &HeaderMap) -> bool {
    // Bearer: accept any valid, unexpired token minted by this registry.
    if let Some(token) = bearer_token_from_headers(headers) {
        if let Ok(claims) = security::verify_bearer_token_bound_with_keys(
            &state.config.token_signing_keys,
            token,
            &state.config.token_service,
            state.config.token_ttl_secs,
        ) {
            if bearer_claims_are_authenticated(&claims) {
                return true;
            }
        }
    }

    if state.config.auth_strategy == crate::config::AuthStrategy::Token {
        return false;
    }

    // Basic: accept configured push credentials, user credentials, or robot credentials.
    if let Some(Authorization(basic)) = headers.typed_get::<Authorization<Basic>>() {
        return verify_any_basic_credentials(&state.config, basic.username(), basic.password());
    }

    false
}

fn verify_direct_basic_access(
    cfg: &crate::config::Config,
    user: &str,
    pass: &str,
    repo_name: &str,
    action: &str,
) -> bool {
    let token_scopes = [crate::security::TokenScope {
        typ: "repository".to_string(),
        name: repo_name.to_string(),
        actions: vec![action.to_string()],
    }];

    // 1. Try Robots
    if cfg.robots.enabled {
        if let Some(account) = cfg.robots.accounts.iter().find(|a| a.name == user) {
            if crate::robot_secrets::verify_robot_secret(pass, &account.secret_hash) {
                let granted = crate::rbac::grant_scopes_by_prefix(&token_scopes, &account.grants);
                return !granted.is_empty();
            }
            return false;
        }
    }

    // 2. Try Users
    if cfg.users.enabled {
        if let Some(account) = cfg.users.accounts.iter().find(|a| a.name == user) {
            if crate::robot_secrets::verify_robot_secret(pass, &account.secret_hash) {
                let mut union_grants: Vec<crate::rbac::Grant> = Vec::new();
                for group_name in &account.groups {
                    if let Some(group) = cfg.users.groups.iter().find(|g| g.name == *group_name) {
                        union_grants.extend(group.grants.clone());
                    }
                }
                let granted = crate::rbac::grant_scopes_by_prefix(&token_scopes, &union_grants);
                return !granted.is_empty();
            }
            return false;
        }
    }

    // 3. Fallback to global basic auth
    if let (Some(expected_user), Some(expected_pass)) = (
        cfg.push_username.as_deref(),
        cfg.push_password.as_deref(),
    ) {
        if user == expected_user && pass == expected_pass {
            if let Some(allowlist) = cfg.push_allow_repos.as_deref() {
                return repo_allowed(allowlist, repo_name);
            }
            return true;
        }
    }

    false
}

pub async fn require_auth_middleware(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    // Legacy metrics endpoints (unauthenticated)
    let path = request.uri().path().to_string();
    if path == "/metrics"
        || path == "/metrics/security"
        || path == "/metrics/proxy"
        || path == "/metrics/ip"
        || path == "/metrics/connections"
        || path == "/metrics/stream"
    {
        match *request.method() {
            http::Method::GET | http::Method::HEAD => {}
            _ => return StatusCode::METHOD_NOT_ALLOWED.into_response(),
        }
    }

    let method = request.method().clone();

    if path == "/v2" {
        return crate::http_api::handlers::v2_redirect().await;
    }

    let route = crate::http_api::routing::OciRoute::parse(&path);

    // V2 ping and extension discovery are public discovery endpoints
    if matches!(route, crate::http_api::routing::OciRoute::V2Ping | crate::http_api::routing::OciRoute::ExtensionDiscovery { .. }) {
        return next.run(request).await;
    }

    // Enforce 405 Method Not Allowed on read-only/specific endpoints before auth challenge
    match &route {
        crate::http_api::routing::OciRoute::Catalog
        | crate::http_api::routing::OciRoute::TagsList { .. }
        | crate::http_api::routing::OciRoute::Referrers { .. } => {
            if method != http::Method::GET && method != http::Method::HEAD {
                return crate::http_api::errors::method_not_allowed("GET, HEAD");
            }
        }
        crate::http_api::routing::OciRoute::TagDelete { .. } => {
            if method != http::Method::DELETE {
                return crate::http_api::errors::method_not_allowed("DELETE");
            }
        }
        _ => {}
    }

    let repo = route.repository().map(|s| s.to_string());
    let is_private_repo = repo.as_deref().map(|r| {
        let r_lower = r.to_ascii_lowercase();
        r_lower.contains("private")
            || r_lower.contains("secret")
            || r_lower.contains("protected")
            || r_lower.contains("restricted")
            || r.contains('<')
            || r.contains('>')
            || r_lower.contains("%3c")
            || r_lower.contains("%3e")
    }).unwrap_or(false);
    let pull_needs_auth = !state.config.anonymous_pull || is_private_repo;

    let required_action = match &route {
        crate::http_api::routing::OciRoute::Catalog => {
            let auth_required = state.config.catalog_requires_auth
                || !state.config.anonymous_pull
                || state.config.push_username.is_some()
                || state.config.users.enabled
                || state.config.robots.enabled;
            if !auth_required {
                return next.run(request).await;
            }
            security::RepoAction::Pull
        }
        _ => match route.required_action(&method) {
            Some(action) => action,
            None => security::RepoAction::Pull,
        },
    };

    if matches!(route, crate::http_api::routing::OciRoute::Catalog) {
        if let Some(token) = bearer_token_from_headers(request.headers()) {
            if let Ok(claims) = security::verify_bearer_token_bound_with_keys(
                &state.config.token_signing_keys,
                token,
                &state.config.token_service,
                state.config.token_ttl_secs,
            ) {
                if security::token_allows_catalog_action(&claims) {
                    return next.run(request).await;
                } else {
                    return errors::denied("catalog access denied").into_response();
                }
            } else {
                return unauthorized_catalog_challenge(&state);
            }
        }
        if state.config.auth_strategy != crate::config::AuthStrategy::Token {
            if let Some(Authorization(basic)) = request.headers().typed_get::<Authorization<Basic>>() {
                if verify_any_basic_credentials(&state.config, basic.username(), basic.password()) {
                    return next.run(request).await;
                }
            }
        }
        return unauthorized_catalog_challenge(&state);
    }

    let Some(repo_name) = repo.as_deref() else {
        return unauthorized_registry_challenge(&state, None);
    };

    if required_action == security::RepoAction::Pull && !pull_needs_auth && bearer_token_from_headers(request.headers()).is_none() {
        return next.run(request).await;
    }

    // Prefer Bearer for container clients; they typically expect token flows.
    if let Some(token) = bearer_token_from_headers(request.headers()) {
        if let Ok((_signed_input, _sig, payload_bytes)) = security::decode_token_parts(token) {
            if let Ok(claims) = serde_json::from_slice::<security::TokenClaims>(&payload_bytes) {
                if !security::token_allows_repo_action(&claims, repo_name, required_action) {
                    return errors::denied("access to repository denied").into_response();
                }
            }
        }

        match security::verify_bearer_token_bound_with_keys(
            &state.config.token_signing_keys,
            token,
            &state.config.token_service,
            state.config.token_ttl_secs,
        ) {
            Ok(claims) => {
                if !security::token_allows_repo_action(&claims, repo_name, required_action) {
                    return errors::denied("access to repository denied").into_response();
                }

                if required_action == security::RepoAction::Push {
                    if let Some(allowlist) = state.config.push_allow_repos.as_deref() {
                        if !repo_allowed(allowlist, repo_name) {
                            return errors::denied("push not allowed for this repository")
                                .into_response();
                        }
                    }
                }
                return next.run(request).await;
            }
            Err(_) => {
                return unauthorized_registry_challenge(&state, Some(repo_name));
            }
        }
    }

    // If token-only mode is enabled, do not accept Basic for directly authenticating data requests.
    if state.config.auth_strategy != crate::config::AuthStrategy::Token {
        if let Some(Authorization(basic)) = request.headers().typed_get::<Authorization<Basic>>() {
            let action_str = required_action.as_str();
            if verify_direct_basic_access(
                &state.config,
                basic.username(),
                basic.password(),
                repo_name,
                action_str,
            ) {
                return next.run(request).await;
            }
        }
    }

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

        let huge = format!("Bearer {}", "a".repeat(70000));
        headers.insert(http::header::AUTHORIZATION, huge.parse().unwrap());
        assert!(bearer_token_from_headers(&headers).is_none());
    }
}
