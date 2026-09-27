use crate::{AppState, http_api::errors};
use axum::{
    body::Body,
    extract::State,
    http::HeaderMap,
    http::Request,
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
    let segments: Vec<&str> = rest.split('/').filter(|s| !s.is_empty()).collect();
    if segments.is_empty() {
        return None;
    }

    let marker_idx = segments
        .iter()
        .position(|s| *s == "blobs" || *s == "manifests" || *s == "tags" || *s == "referrers")?;
    if marker_idx == 0 {
        return None;
    }
    Some(segments[..marker_idx].join("/"))
}

// Delegated to the core layer (ADR-010): the push-allowlist matching logic lives
// with `RepositoryAccessPattern` in `registry::access_pattern`.
pub(crate) use crate::registry::access_pattern::push_repository_allowed;

pub(crate) fn unauthorized_registry_challenge(
    state: &AppState,
    repo: Option<&str>,
    action: Option<crate::security::RepoAction>,
) -> Response {
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
        let action_str = action.map(|a| a.as_str()).unwrap_or("pull");
        bearer.push_str(&format!(",scope=\"repository:{repo}:{action_str}\""));
    }

    if (state.config.auth_strategy == crate::config::AuthStrategy::Token
        || state.config.auth_strategy == crate::config::AuthStrategy::Both)
        && let Ok(v) = http::HeaderValue::from_str(&bearer) {
            resp.headers_mut().append(http::header::WWW_AUTHENTICATE, v);
        }

    if state.config.auth_strategy == crate::config::AuthStrategy::Basic
        || state.config.auth_strategy == crate::config::AuthStrategy::Both
    {
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

    if (state.config.auth_strategy == crate::config::AuthStrategy::Token
        || state.config.auth_strategy == crate::config::AuthStrategy::Both)
        && let Ok(v) = http::HeaderValue::from_str(&bearer) {
            resp.headers_mut().append(http::header::WWW_AUTHENTICATE, v);
        }

    if state.config.auth_strategy == crate::config::AuthStrategy::Basic
        || state.config.auth_strategy == crate::config::AuthStrategy::Both
    {
        resp.headers_mut().append(
            http::header::WWW_AUTHENTICATE,
            http::HeaderValue::from_static("Basic realm=\"registry\""),
        );
    }
    resp
}

pub(crate) fn verify_any_basic_credentials(
    cfg: &crate::config::Config,
    user: &str,
    pass: &str,
) -> bool {
    let auth_cfg = naust_auth::AuthConfig::from(cfg);
    naust_auth::policy::verify_any_basic_credentials(&auth_cfg, user, pass)
}

pub(crate) fn catalog_auth_required(cfg: &crate::config::Config) -> bool {
    let auth_cfg = naust_auth::AuthConfig::from(cfg);
    auth_cfg.catalog_auth_required()
}

pub(crate) use naust_auth::policy::CatalogAccess;

pub(crate) fn authorize_catalog(state: &AppState, headers: &HeaderMap) -> CatalogAccess {
    if catalog_shows_private_names(state, headers) {
        CatalogAccess::Full
    } else if catalog_auth_required(&state.config) {
        CatalogAccess::Denied
    } else {
        CatalogAccess::PublicOnly
    }
}

fn catalog_shows_private_names(state: &AppState, headers: &HeaderMap) -> bool {
    if let Some(token) = bearer_token_from_headers(headers)
        && let Ok(claims) = security::verify_bearer_token_bound_with_keys(
            &state.config.token_signing_keys,
            token,
            &state.config.token_service,
            state.config.token_ttl_secs,
        )
            && bearer_claims_are_authenticated(&claims)
                && security::token_allows_catalog_action(&claims)
            {
                return true;
            }

    if state.config.auth_strategy != crate::config::AuthStrategy::Token
        && let Some(Authorization(basic)) = headers.typed_get::<Authorization<Basic>>()
            && basic_allows_catalog(&state.config, basic.username(), basic.password()) {
                return true;
            }
    false
}

pub(crate) fn basic_allows_catalog(cfg: &crate::config::Config, user: &str, pass: &str) -> bool {
    let auth_cfg = naust_auth::AuthConfig::from(cfg);
    naust_auth::policy::basic_allows_catalog(&auth_cfg, user, pass)
}

pub(crate) fn is_authenticated(state: &AppState, headers: &HeaderMap) -> bool {
    // Bearer: accept any valid, unexpired token minted by this registry.
    if let Some(token) = bearer_token_from_headers(headers)
        && let Ok(claims) = security::verify_bearer_token_bound_with_keys(
            &state.config.token_signing_keys,
            token,
            &state.config.token_service,
            state.config.token_ttl_secs,
        )
            && bearer_claims_are_authenticated(&claims) {
                return true;
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

pub(crate) fn verify_direct_basic_access(
    cfg: &crate::config::Config,
    user: &str,
    pass: &str,
    repo: &crate::registry::CanonicalRepoName,
    action: &str,
) -> bool {
    let auth_cfg = naust_auth::AuthConfig::from(cfg);
    naust_auth::policy::verify_direct_basic_access(&auth_cfg, user, pass, repo, action)
}

pub async fn require_auth_middleware(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let path = request.uri().path().to_string();
    let method = request.method().clone();

    if path == "/v2" {
        return crate::http_api::handlers::v2_redirect().await;
    }

    let route = crate::http_api::routing::OciRoute::parse(&path);

    if matches!(
        route,
        crate::http_api::routing::OciRoute::InvalidRepoName { .. }
    ) {
        return crate::http_api::errors::name_invalid().into_response();
    }

    // V2 ping and extension discovery are public discovery endpoints
    if matches!(
        route,
        crate::http_api::routing::OciRoute::V2Ping
            | crate::http_api::routing::OciRoute::ExtensionDiscovery { .. }
    ) {
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
        crate::http_api::routing::OciRoute::TagDelete { .. }
            if method != http::Method::DELETE => {
                return crate::http_api::errors::method_not_allowed("DELETE");
            }
        _ => {}
    }

    let repo = route.repository().map(|r| r.as_str().to_string());
    let is_private_repo = repo
        .as_deref()
        .map(|r| state.config.is_repo_private(r))
        .unwrap_or(false);
    let pull_needs_auth = !state.config.anonymous_pull || is_private_repo;

    // Catalog visibility is only `authorize_catalog` (ADR-015). A catalog-scoped
    // bearer with an empty subject is not enough to pass this gate.
    if matches!(route, crate::http_api::routing::OciRoute::Catalog) {
        return match authorize_catalog(&state, request.headers()) {
            CatalogAccess::Denied => unauthorized_catalog_challenge(&state),
            CatalogAccess::Full | CatalogAccess::PublicOnly => next.run(request).await,
        };
    }

    let required_action = match route.required_action(&method) {
        Some(action) => action,
        None => security::RepoAction::Pull,
    };

    let Some(canonical_repo) = route.repository() else {
        return unauthorized_registry_challenge(&state, None, None);
    };
    let repo_name = canonical_repo.as_str();

    if required_action == security::RepoAction::Pull
        && !pull_needs_auth
        && bearer_token_from_headers(request.headers()).is_none()
    {
        return next.run(request).await;
    }

    // Prefer Bearer for container clients; they typically expect token flows.
    if let Some(token) = bearer_token_from_headers(request.headers()) {
        match security::verify_bearer_token_bound_with_keys(
            &state.config.token_signing_keys,
            token,
            &state.config.token_service,
            state.config.token_ttl_secs,
        ) {
            Ok(claims) => {
                if !security::token_allows_repo_action(&claims, repo_name, required_action) {
                    return unauthorized_registry_challenge(
                        &state,
                        Some(repo_name),
                        Some(required_action),
                    );
                }

                if (required_action == security::RepoAction::Push
                    || required_action == security::RepoAction::Delete)
                    && let Some(allowlist) = state.config.push_allow_repos.as_deref()
                        && !push_repository_allowed(allowlist, canonical_repo) {
                            return errors::denied(
                                "push or delete not allowed for this repository",
                            )
                            .into_response();
                        }
                return next.run(request).await;
            }
            Err(_) => {
                return unauthorized_registry_challenge(
                    &state,
                    Some(repo_name),
                    Some(required_action),
                );
            }
        }
    }

    // If token-only mode is enabled, do not accept Basic for directly authenticating data requests.
    if state.config.auth_strategy != crate::config::AuthStrategy::Token
        && let Some(Authorization(basic)) = request.headers().typed_get::<Authorization<Basic>>() {
            let action_str = required_action.as_str();
            if verify_direct_basic_access(
                &state.config,
                basic.username(),
                basic.password(),
                canonical_repo,
                action_str,
            ) {
                return next.run(request).await;
            }
        }

    unauthorized_registry_challenge(&state, Some(repo_name), Some(required_action))
}

#[cfg(test)]
mod tests {
    use super::{
        bearer_claims_are_authenticated, bearer_token_from_headers, extract_repo_from_v2_path,
        push_repository_allowed,
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
    fn push_repository_allowed_matches_exact_and_prefix() {
        let allowlist = vec![
            crate::registry::RepositoryAccessPattern::parse("org/repo").unwrap(),
            crate::registry::RepositoryAccessPattern::parse("org/*").unwrap(),
        ];
        let cand_repo = crate::registry::CanonicalRepoName::parse("org/repo").unwrap();
        let cand_other = crate::registry::CanonicalRepoName::parse("org/other").unwrap();
        let cand_diff = crate::registry::CanonicalRepoName::parse("other/repo").unwrap();
        assert!(push_repository_allowed(&allowlist, &cand_repo));
        assert!(push_repository_allowed(&allowlist, &cand_other));
        assert!(!push_repository_allowed(&allowlist, &cand_diff));
    }

    #[test]
    fn push_repository_allowed_star_allows_everything() {
        let allowlist = vec![crate::registry::RepositoryAccessPattern::parse("*").unwrap()];
        let cand_anything = crate::registry::CanonicalRepoName::parse("anything/here").unwrap();
        let cand_single = crate::registry::CanonicalRepoName::parse("single").unwrap();
        assert!(push_repository_allowed(&allowlist, &cand_anything));
        assert!(push_repository_allowed(&allowlist, &cand_single));
    }

    #[test]
    fn push_repository_allowed_prefix_matches_exact_prefix_repo_too() {
        let allowlist = vec![crate::registry::RepositoryAccessPattern::parse("org/*").unwrap()];
        let cand_org = crate::registry::CanonicalRepoName::parse("org").unwrap();
        let cand_org_repo = crate::registry::CanonicalRepoName::parse("org/repo").unwrap();
        let cand_org2_repo = crate::registry::CanonicalRepoName::parse("org2/repo").unwrap();
        assert!(push_repository_allowed(&allowlist, &cand_org));
        assert!(push_repository_allowed(&allowlist, &cand_org_repo));
        assert!(!push_repository_allowed(&allowlist, &cand_org2_repo));
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

    #[tokio::test]
    async fn test_route_level_authentication_matrix() {
        use super::require_auth_middleware;
        use axum::body::Body;
        use axum::middleware;
        use axum::routing::get;
        use http::Request;
        use tower::ServiceExt;

        let fs_root = std::env::temp_dir().join(format!("auth-test-{}", uuid::Uuid::new_v4()));
        let _ = std::fs::create_dir_all(&fs_root);
        let cfg_path = fs_root.join("config.toml");
        std::fs::write(
            &cfg_path,
            format!(
                r#"
[server]
listen_addr = "127.0.0.1:5000"

[auth]
anonymous_pull = false

[[token.signing_keys]]
kid = "default"
key = "test-key-material-12345"

[storage]
backend = "fs"
[storage.fs]
root = "{}"
"#,
                fs_root.display()
            ),
        )
        .unwrap();

        let cfg = crate::config::Config::from_env_with_files(&[cfg_path]).unwrap();
        let cfg = std::sync::Arc::new(cfg);
        let storage = std::sync::Arc::new(crate::storage::fs::FsStorage::new(
            fs_root.clone(),
            cfg.max_upload_bytes,
        ));
        let state = crate::AppState::new_test(cfg, storage, None);

        let app = axum::Router::new()
            .route("/v2", get(|| async { axum::http::StatusCode::OK }))
            .route("/v2/", get(|| async { axum::http::StatusCode::OK }))
            .route(
                "/v2/*rest",
                axum::routing::any(|| async { axum::http::StatusCode::OK }),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                require_auth_middleware,
            ))
            .with_state(state);

        // 1. /v2 redirect and /v2/ ping are public discovery
        let res = app
            .clone()
            .oneshot(Request::builder().uri("/v2").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::MOVED_PERMANENTLY);

        let res = app
            .clone()
            .oneshot(Request::builder().uri("/v2/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::OK);

        // 2. /v2/_oci/ext/discover is public discovery
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v2/_oci/ext/discover")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::OK);

        // 3. /v2/my-repo/_oci/ext/discover is public discovery
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v2/my-repo/_oci/ext/discover")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::OK);

        // 4. Protected endpoint /v2/my-repo/manifests/latest requires auth (401)
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v2/my-repo/manifests/latest")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::UNAUTHORIZED);

        // 5. Lookalike / suffix endpoint does NOT bypass auth
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v2/_oci/ext/discover_fake")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::UNAUTHORIZED);

        let _ = std::fs::remove_dir_all(&fs_root);
    }

    #[tokio::test]
    async fn test_route_normalization_and_auth_bypass_resistance() {
        use super::require_auth_middleware;
        use crate::security::{TokenScope, issue_bearer_token_with_key};
        use axum::body::Body;
        use axum::middleware;
        use http::Request;
        use tower::ServiceExt;

        let fs_root = std::env::temp_dir().join(format!("auth-norm-{}", uuid::Uuid::new_v4()));
        let _ = std::fs::create_dir_all(&fs_root);
        let cfg_path = fs_root.join("config.toml");
        std::fs::write(
            &cfg_path,
            format!(
                r#"
[server]
listen_addr = "127.0.0.1:5000"

[auth]
anonymous_pull = false

[[token.signing_keys]]
kid = "default"
key = "test-key-material-12345"

[storage]
backend = "fs"
[storage.fs]
root = "{}"
"#,
                fs_root.display()
            ),
        )
        .unwrap();

        let cfg = crate::config::Config::from_env_with_files(&[cfg_path]).unwrap();
        let cfg = std::sync::Arc::new(cfg);
        let storage = std::sync::Arc::new(crate::storage::fs::FsStorage::new(
            fs_root.clone(),
            cfg.max_upload_bytes,
        ));
        let state = crate::AppState::new_test(cfg.clone(), storage, None);

        let app = axum::Router::new()
            .route(
                "/v2/*rest",
                axum::routing::any(|| async { axum::http::StatusCode::OK }),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                require_auth_middleware,
            ))
            .with_state(state);

        let now = now_secs();
        let signing_key = &cfg.token_signing_keys[0];

        // Token scoped exclusively to "org/app"
        let token_org_app = issue_bearer_token_with_key(
            signing_key,
            &cfg.token_service,
            Some("user1"),
            &[TokenScope {
                typ: "repository".to_string(),
                name: "org/app".to_string(),
                actions: vec!["pull".to_string()],
            }],
            now,
            now + 300,
        )
        .unwrap();

        // 1. Legitimate request with matching token succeeds (200 OK)
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v2/org/app/manifests/latest")
                    .header(
                        http::header::AUTHORIZATION,
                        format!("Bearer {token_org_app}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::OK);

        // 2. Cross-boundary tenant attack (org2/app) with org/app token is challenged (401)
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v2/org2/app/manifests/latest")
                    .header(
                        http::header::AUTHORIZATION,
                        format!("Bearer {token_org_app}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::UNAUTHORIZED);

        // 3. Extended name attack (org/application) with org/app token is challenged (401)
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v2/org/application/manifests/latest")
                    .header(
                        http::header::AUTHORIZATION,
                        format!("Bearer {token_org_app}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::UNAUTHORIZED);

        // 4. Sub-path attack (org/app/sub) with org/app token is challenged (401)
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v2/org/app/sub/manifests/latest")
                    .header(
                        http::header::AUTHORIZATION,
                        format!("Bearer {token_org_app}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::UNAUTHORIZED);

        // 5. Method restriction: write (PUT) with pull-only token is challenged (401)
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::PUT)
                    .uri("/v2/org/app/manifests/latest")
                    .header(
                        http::header::AUTHORIZATION,
                        format!("Bearer {token_org_app}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::UNAUTHORIZED);

        let _ = std::fs::remove_dir_all(&fs_root);
    }
}
