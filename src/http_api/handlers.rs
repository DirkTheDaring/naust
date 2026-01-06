use crate::{
    AppState, ProxyContext,
    registry::digest::Digest,
    storage::{ReferrerDescriptor, RepoTimestamps, StorageError},
};
use axum::{
    body::Body,
    extract::Query,
    extract::RawQuery,
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::StreamExt;
use headers::{Authorization, HeaderMapExt, authorization::Basic};
use sha2::Digest as _;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio_util::io::ReaderStream;
use url::form_urlencoded;

use super::errors;
use crate::request_routing::{V2RouteMode, v2_route_mode_for_request};
use crate::security;

#[derive(Clone, Debug, PartialEq, Eq)]
enum TokenRejection {
    Unauthorized,
    Denied(&'static str),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TokenDecision {
    subject: Option<String>,
    scopes: Vec<security::TokenScope>,
    ttl_secs: u64,
}

fn decide_token_scopes_for_request(
    cfg: &crate::config::Config,
    token_scopes: &[security::TokenScope],
    basic: Option<(String, String)>,
) -> Result<TokenDecision, TokenRejection> {
    let wants_push = wants_push_from_token_scopes(token_scopes);

    // Pull-only tokens: keep behavior simple and backwards compatible.
    // (No auth required; we mint exactly the sanitized requested scopes.)
    if !wants_push {
        return Ok(TokenDecision {
            subject: None,
            scopes: token_scopes.to_vec(),
            ttl_secs: cfg.token_ttl_secs,
        });
    }

    // Push requested: first try robot auth (if enabled), then user/group auth (if enabled),
    // then fall back to legacy push user/pass.
    if cfg.robots.enabled {
        if let Some((user, pass)) = basic.as_ref() {
            if let Some(account) = cfg.robots.accounts.iter().find(|a| a.name == *user) {
                if crate::robot_secrets::verify_robot_secret(pass, &account.secret_hash) {
                    let granted =
                        crate::rbac::grant_scopes_by_prefix(token_scopes, &account.grants);
                    if granted.is_empty() {
                        return Err(TokenRejection::Denied("push not allowed by robot policy"));
                    }

                    // Ensure we did not implicitly drop all requested push actions.
                    // (If push was requested, at least one push must remain granted.)
                    let granted_wants_push = wants_push_from_token_scopes(&granted);
                    if !granted_wants_push {
                        return Err(TokenRejection::Denied("push not allowed by robot policy"));
                    }

                    let ttl_secs = match account.max_ttl_secs {
                        Some(max) if max > 0 => cfg.token_ttl_secs.min(max),
                        _ => cfg.token_ttl_secs,
                    };

                    return Ok(TokenDecision {
                        subject: Some(format!("robot:{}", account.name)),
                        scopes: granted,
                        ttl_secs,
                    });
                }
            }
        }
    }

    if cfg.users.enabled {
        if let Some((user, pass)) = basic.as_ref() {
            if let Some(account) = cfg.users.accounts.iter().find(|a| a.name == *user) {
                if crate::robot_secrets::verify_robot_secret(pass, &account.secret_hash) {
                    let mut union_grants: Vec<crate::rbac::Grant> = Vec::new();
                    for group_name in &account.groups {
                        if let Some(group) = cfg.users.groups.iter().find(|g| g.name == *group_name)
                        {
                            union_grants.extend(group.grants.clone());
                        }
                    }

                    let granted = crate::rbac::grant_scopes_by_prefix(token_scopes, &union_grants);
                    if granted.is_empty() {
                        return Err(TokenRejection::Denied("push not allowed by user policy"));
                    }

                    let granted_wants_push = wants_push_from_token_scopes(&granted);
                    if !granted_wants_push {
                        return Err(TokenRejection::Denied("push not allowed by user policy"));
                    }

                    let ttl_secs = match account.max_ttl_secs {
                        Some(max) if max > 0 => cfg.token_ttl_secs.min(max),
                        _ => cfg.token_ttl_secs,
                    };

                    return Ok(TokenDecision {
                        subject: Some(format!("user:{}", account.name)),
                        scopes: granted,
                        ttl_secs,
                    });
                }
            }
        }
    }

    // Legacy global push auth.
    let Some(expected_user) = cfg.push_username.as_deref() else {
        return Err(TokenRejection::Unauthorized);
    };
    let Some(expected_pass) = cfg.push_password.as_deref() else {
        return Err(TokenRejection::Unauthorized);
    };
    let Some((user, pass)) = basic else {
        return Err(TokenRejection::Unauthorized);
    };
    if user != expected_user || pass != expected_pass {
        return Err(TokenRejection::Unauthorized);
    }

    Ok(TokenDecision {
        subject: Some(user),
        scopes: token_scopes.to_vec(),
        ttl_secs: cfg.token_ttl_secs,
    })
}

pub async fn ping(State(state): State<AppState>, req_headers: HeaderMap) -> Response {
    // Many clients (Docker/Podman) perform auth negotiation via GET /v2/.
    // For "anonymous pull + authenticated push" we still advertise auth here so
    // clients learn the Bearer realm and can fetch an anonymous pull token or an
    // authenticated push token.
    let auth_scheme = req_headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split_whitespace().next())
        .unwrap_or("<none>");
    tracing::info!(auth_scheme = auth_scheme, "v2 ping");
    let has_auth = auth_scheme != "<none>";

    if state.config.push_auth_configured() && !has_auth {
        let mut resp: Response = StatusCode::UNAUTHORIZED.into_response();

        let realm = state
            .config
            .public_url
            .as_deref()
            .unwrap_or("http://127.0.0.1:5000")
            .trim_end_matches('/');
        let bearer = format!(
            "Bearer realm=\"{realm}/token\",service=\"{}\"",
            state.config.token_service
        );
        if let Ok(v) = http::HeaderValue::from_str(&bearer) {
            resp.headers_mut().insert(http::header::WWW_AUTHENTICATE, v);
        }
        resp.headers_mut().append(
            http::header::WWW_AUTHENTICATE,
            http::HeaderValue::from_static("Basic realm=\"registry\""),
        );
        resp.headers_mut().insert(
            http::header::HeaderName::from_static("docker-distribution-api-version"),
            http::HeaderValue::from_static("registry/2.0"),
        );
        return resp;
    }

    let mut headers = HeaderMap::new();
    headers.insert(
        "Docker-Distribution-API-Version",
        "registry/2.0".parse().unwrap(),
    );
    (StatusCode::OK, headers).into_response()
}

pub async fn token(
    State(state): State<AppState>,
    raw_query: RawQuery,
    headers: HeaderMap,
) -> Response {
    // Docker/OCI token endpoint (very small subset).
    // Expected query params:
    //   service=<name>
    //   scope=repository:<repo>:pull,push
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

    if !service_param_is_valid(service_param.as_deref(), &state.config.token_service) {
        if let Some(svc) = service_param.as_deref() {
            let denied_total = state.auth_metrics.inc_token_denied();
            tracing::warn!(
                event = "token_denied",
                reason = "invalid_service",
                requested_service = %svc,
                configured_service = %state.config.token_service,
                token_denied_total = denied_total,
            );
        }
        return errors::denied("invalid token service").into_response();
    }
    let scopes = scopes_raw
        .iter()
        .flat_map(|s| parse_scopes(s))
        .collect::<Vec<_>>();

    // Be lenient in parsing, but never mint unexpected permissions.
    // We only mint repository scopes and only the actions we understand.
    let token_scopes = sanitize_token_scopes(&scopes);
    let requested_wants_push = wants_push_from_token_scopes(&token_scopes);

    let basic = headers
        .typed_get::<Authorization<Basic>>()
        .map(|Authorization(b)| (b.username().to_string(), b.password().to_string()));

    let decision = match decide_token_scopes_for_request(&state.config, &token_scopes, basic) {
        Ok(d) => d,
        Err(TokenRejection::Unauthorized) => {
            let denied_total = state.auth_metrics.inc_token_denied();
            tracing::warn!(
                event = "token_denied",
                reason = "unauthorized",
                service = %state.config.token_service,
                requested_scopes_len = token_scopes.len(),
                wants_push = requested_wants_push,
                token_denied_total = denied_total,
            );
            return token_unauthorized(&state);
        }
        Err(TokenRejection::Denied(msg)) => {
            let denied_total = state.auth_metrics.inc_token_denied();
            tracing::warn!(
                event = "token_denied",
                reason = %msg,
                service = %state.config.token_service,
                requested_scopes_len = token_scopes.len(),
                wants_push = requested_wants_push,
                token_denied_total = denied_total,
            );
            return errors::denied(msg).into_response();
        }
    };

    tracing::debug!(
        event = "token_decision",
        service = %state.config.token_service,
        requested_scopes = ?token_scopes,
        granted_scopes = ?decision.scopes,
        subject = decision.subject.as_deref().unwrap_or("<anon>"),
        ttl_secs = decision.ttl_secs,
    );

    // Enforce legacy repo allowlist for push tokens as an additional safety net.
    if wants_push_from_token_scopes(&decision.scopes) {
        if let Some(allowlist) = state.config.push_allow_repos.as_deref() {
            for scope in &decision.scopes {
                if scope.typ == "repository" {
                    if !crate::auth::repo_allowed(allowlist, &scope.name) {
                        let denied_total = state.auth_metrics.inc_token_denied();
                        tracing::warn!(
                            event = "token_denied",
                            reason = "push_repo_not_allowed",
                            service = %state.config.token_service,
                            repo = %scope.name,
                            subject = decision.subject.as_deref().unwrap_or("<anon>"),
                            token_denied_total = denied_total,
                        );
                        return errors::denied("push not allowed for this repository")
                            .into_response();
                    }
                }
            }
        }
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs();
    let exp = now.saturating_add(decision.ttl_secs);

    let token = match issue_token(
        &state,
        decision.subject.as_deref(),
        &decision.scopes,
        now,
        exp,
    ) {
        Ok(t) => t,
        Err(_) => {
            let err_total = state.auth_metrics.inc_token_internal_error();
            tracing::error!(
                event = "token_error",
                reason = "issue_failed",
                service = %state.config.token_service,
                token_internal_error_total = err_total,
            );
            return errors::internal_error().into_response();
        }
    };

    let issued_total = state.auth_metrics.inc_token_issued();
    tracing::info!(
        event = "token_issued",
        service = %state.config.token_service,
        subject = decision.subject.as_deref().unwrap_or("<anon>"),
        ttl_secs = decision.ttl_secs,
        requested_scopes_len = token_scopes.len(),
        granted_scopes_len = decision.scopes.len(),
        wants_push = wants_push_from_token_scopes(&decision.scopes),
        token_issued_total = issued_total,
    );

    let body = serde_json::json!({
        "token": token,
        "access_token": token,
        "expires_in": decision.ttl_secs,
        "issued_at": format_rfc3339(now),
    });

    let bytes = match serde_json::to_vec(&body) {
        Ok(b) => b,
        Err(_) => return errors::internal_error().into_response(),
    };

    let mut resp_headers = registry_headers();
    resp_headers.insert("Content-Type", "application/json".parse().unwrap());
    resp_headers.insert("Content-Length", bytes.len().to_string().parse().unwrap());
    (StatusCode::OK, resp_headers, Body::from(bytes)).into_response()
}

fn service_param_is_valid(service_param: Option<&str>, configured_service: &str) -> bool {
    match service_param {
        None => true,
        Some(s) => s == configured_service,
    }
}

fn token_unauthorized(state: &AppState) -> Response {
    let mut resp: Response = StatusCode::UNAUTHORIZED.into_response();
    // Challenge so the client knows it can present Basic creds to obtain a token.
    resp.headers_mut().insert(
        http::header::WWW_AUTHENTICATE,
        http::HeaderValue::from_static("Basic realm=\"registry\""),
    );
    resp.headers_mut().insert(
        http::header::HeaderName::from_static("docker-distribution-api-version"),
        http::HeaderValue::from_static("registry/2.0"),
    );
    // Also include Bearer parameters for completeness.
    let realm = state
        .config
        .public_url
        .as_deref()
        .unwrap_or("http://127.0.0.1:5000")
        .trim_end_matches('/');
    if let Ok(v) = http::HeaderValue::from_str(&format!(
        "Bearer realm=\"{realm}/token\",service=\"{}\"",
        state.config.token_service
    )) {
        resp.headers_mut().append(http::header::WWW_AUTHENTICATE, v);
    }
    resp
}

#[derive(Clone, Debug)]
struct Scope {
    typ: String,
    name: String,
    actions: Vec<String>,
}

fn token_scope_requests_repo_action(
    scope: &security::TokenScope,
    action: security::RepoAction,
) -> bool {
    if scope.typ != "repository" {
        return false;
    }
    let action = action.as_str();
    scope.actions.iter().any(|a| a == action)
}

fn sanitize_token_scopes(scopes: &[Scope]) -> Vec<security::TokenScope> {
    let mut out: Vec<security::TokenScope> = Vec::new();
    for s in scopes {
        if s.typ != "repository" {
            continue;
        }

        if s.name.trim().is_empty() {
            continue;
        }

        let mut actions: Vec<String> = Vec::new();
        for a in &s.actions {
            if a == security::RepoAction::Pull.as_str() || a == security::RepoAction::Push.as_str()
            {
                actions.push(a.clone());
            }
        }

        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        actions.retain(|a| seen.insert(a.clone()));

        if actions.is_empty() {
            continue;
        }

        out.push(security::TokenScope {
            typ: s.typ.clone(),
            name: s.name.clone(),
            actions,
        });
    }
    out
}

fn wants_push_from_token_scopes(token_scopes: &[security::TokenScope]) -> bool {
    token_scopes
        .iter()
        .any(|s| token_scope_requests_repo_action(s, security::RepoAction::Push))
}

fn parse_scopes(scope: &str) -> Vec<Scope> {
    // scope can be repeated in the URL; many clients send exactly one.
    // We accept a single comma-separated action list.
    // Example: repository:myrepo:pull,push
    if scope.trim().is_empty() {
        return Vec::new();
    }

    // Some clients include multiple scopes separated by spaces.
    scope
        .split_whitespace()
        .filter_map(|item| {
            let mut parts = item.splitn(3, ':');
            let typ = parts.next()?.trim().to_ascii_lowercase();
            let name = parts.next()?.to_string();
            let mut actions = parts
                .next()
                .unwrap_or("")
                .split(',')
                .map(|a| a.trim().to_ascii_lowercase())
                .filter(|a| !a.is_empty())
                .collect::<Vec<_>>();

            let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
            actions.retain(|a| seen.insert(a.clone()));
            Some(Scope { typ, name, actions })
        })
        .collect()
}

fn issue_token(
    state: &AppState,
    subject: Option<&str>,
    scopes: &[security::TokenScope],
    iat: u64,
    exp: u64,
) -> Result<String, ()> {
    security::issue_bearer_token_with_key(
        state.config.token_primary_signing_key(),
        &state.config.token_service,
        subject,
        scopes,
        iat,
        exp,
    )
    .map_err(|_| ())
}

fn format_rfc3339(unix_secs: u64) -> String {
    OffsetDateTime::from_unix_timestamp(unix_secs as i64)
        .ok()
        .and_then(|t| t.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

fn url_encode_component(s: &str) -> String {
    form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>()
}

pub async fn v2_dispatch(
    State(state): State<AppState>,
    method: Method,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    Path(rest): Path<String>,
    body: Body,
) -> Response {
    let request_host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok());
    let route_mode = v2_route_mode_for_request(&state.config.proxy, &headers);
    let proxy_ctx = state.proxy_context_for_request(&headers);

    let segments: Vec<&str> = rest.split('/').filter(|s| !s.is_empty()).collect();

    // Registry catalog:
    //   GET/HEAD /v2/_catalog
    if segments.len() == 1 && segments[0] == "_catalog" {
        // Optional privacy policy: require auth for catalog.
        if state.config.catalog_requires_auth && !crate::auth::is_authenticated(&state, &headers) {
            return crate::auth::unauthorized_catalog_challenge(&state).into_response();
        }
        return catalog_list(state, method, &query, route_mode, proxy_ctx.clone()).await;
    }
    // Uploads:
    //   POST /v2/<name>/blobs/uploads/
    //   PATCH/PUT/GET /v2/<name>/blobs/uploads/<uuid>
    if segments.len() >= 2
        && segments[segments.len() - 1] == "uploads"
        && segments[segments.len() - 2] == "blobs"
    {
        let name = segments[..segments.len() - 2].join("/");
        return upload_create(state, method, &name, &query, body).await;
    }

    if segments.len() >= 3
        && segments[segments.len() - 2] == "uploads"
        && segments[segments.len() - 3] == "blobs"
    {
        let uuid = segments[segments.len() - 1];
        let name = segments[..segments.len() - 3].join("/");
        return upload_session(state, method, &headers, &name, uuid, query, body).await;
    }

    // Tags list:
    //   GET/HEAD /v2/<name>/tags/list
    if segments.len() >= 2
        && segments[segments.len() - 2] == "tags"
        && segments[segments.len() - 1] == "list"
    {
        let name = segments[..segments.len() - 2].join("/");
        return tags_list(state, method, &name, &query, route_mode, proxy_ctx.clone()).await;
    }

    // Referrers:
    //   GET/HEAD /v2/<name>/referrers/<digest>
    if segments.len() >= 2 && segments[segments.len() - 2] == "referrers" {
        let digest_str = segments[segments.len() - 1];
        let name = segments[..segments.len() - 2].join("/");
        return referrers_list(
            state,
            method,
            &name,
            digest_str,
            &query,
            route_mode,
            proxy_ctx.clone(),
        )
        .await;
    }

    if segments.len() >= 2 && segments[segments.len() - 2] == "manifests" {
        let reference = segments[segments.len() - 1];
        let name = segments[..segments.len() - 2].join("/");
        if method == Method::PUT {
            return manifest_put(state, &headers, &name, reference, body).await;
        }
        return manifest_by_reference(
            state,
            method,
            &name,
            reference,
            route_mode,
            proxy_ctx.clone(),
            request_host,
        )
        .await;
    }

    // /v2/<name>/blobs/<digest>
    // Avoid catching /blobs/uploads by requiring the digest format.
    if segments.len() >= 2
        && segments[segments.len() - 2] == "blobs"
        && segments[segments.len() - 1].contains(':')
    {
        let digest_str = segments[segments.len() - 1];
        let name = segments[..segments.len() - 2].join("/");
        return blob_by_digest(
            state,
            method,
            &name,
            digest_str,
            route_mode,
            proxy_ctx.clone(),
        )
        .await;
    }

    errors::not_implemented().into_response()
}

async fn catalog_list(
    state: AppState,
    method: Method,
    query: &HashMap<String, String>,
    route_mode: V2RouteMode,
    proxy_ctx: Option<ProxyContext>,
) -> Response {
    match method {
        Method::GET | Method::HEAD => {
            let storage = match route_mode {
                V2RouteMode::Default => state.storage.clone(),
                V2RouteMode::ProxyOnly => match proxy_ctx.as_ref() {
                    Some(ctx) => ctx.cache.clone(),
                    None => return errors::internal_error().into_response(),
                },
            };

            match storage.list_repositories().await {
                Ok(all) => {
                    let total = all.len();
                    let n_opt = query.get("n").and_then(|s| s.parse::<usize>().ok());
                    let n = n_opt.unwrap_or(usize::MAX);

                    let start_idx = query
                        .get("last")
                        .and_then(|last| all.iter().position(|t| t == last))
                        .map(|i| i.saturating_add(1))
                        .unwrap_or(0);

                    let end_idx = start_idx.saturating_add(n).min(total);
                    let repos: Vec<String> = all
                        .into_iter()
                        .skip(start_idx)
                        .take(end_idx.saturating_sub(start_idx))
                        .collect();
                    let has_more = end_idx < total;

                    let payload = serde_json::json!({
                        "repositories": repos,
                    });
                    let bytes = match serde_json::to_vec(&payload) {
                        Ok(b) => b,
                        Err(_) => return errors::internal_error().into_response(),
                    };

                    let mut headers = registry_headers();
                    headers.insert("Content-Type", "application/json".parse().unwrap());
                    headers.insert("Content-Length", bytes.len().to_string().parse().unwrap());

                    if has_more {
                        if let (Some(n_raw), Some(last_repo)) = (
                            query.get("n"),
                            payload
                                .get("repositories")
                                .and_then(|v| v.as_array())
                                .and_then(|a| a.last())
                                .and_then(|x| x.as_str()),
                        ) {
                            let last_repo = url_encode_component(last_repo);
                            let link =
                                format!("</v2/_catalog?n={n_raw}&last={last_repo}>; rel=\"next\"");
                            if let Ok(v) = http::HeaderValue::from_str(&link) {
                                headers.insert(http::header::LINK, v);
                            }
                        }
                    }

                    if method == Method::HEAD {
                        return (StatusCode::OK, headers).into_response();
                    }
                    (StatusCode::OK, headers, Body::from(bytes)).into_response()
                }
                Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
                Err(_) => errors::internal_error().into_response(),
            }
        }
        _ => errors::not_implemented().into_response(),
    }
}

fn repo_org(name: &str) -> Option<&str> {
    name.split_once('/').map(|(org, _)| org)
}

fn system_time_to_rfc3339_opt(t: Option<SystemTime>) -> Option<String> {
    let t = t?;
    let secs = t.duration_since(UNIX_EPOCH).ok()?.as_secs();
    Some(format_rfc3339(secs))
}

fn repo_meta_from_timestamps(name: &str, ts: RepoTimestamps) -> serde_json::Value {
    let last_push = system_time_to_rfc3339_opt(ts.last_tag_update);
    let last_manifest_change = system_time_to_rfc3339_opt(ts.last_manifest_update);
    let last_change_time = match (ts.last_tag_update, ts.last_manifest_update) {
        (Some(a), Some(b)) => Some(if a >= b { a } else { b }),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    let last_change = system_time_to_rfc3339_opt(last_change_time);

    serde_json::json!({
        "name": name,
        "org": repo_org(name),
        "last_push": last_push,
        "last_manifest_change": last_manifest_change,
        "last_change": last_change,
    })
}

pub async fn meta_orgs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if state.config.catalog_requires_auth && !crate::auth::is_authenticated(&state, &headers) {
        return crate::auth::unauthorized_catalog_challenge(&state).into_response();
    }

    let repos = match state.storage.list_repositories().await {
        Ok(r) => r,
        Err(_) => return errors::internal_error().into_response(),
    };

    let mut orgs: Vec<String> = repos
        .iter()
        .filter_map(|r| repo_org(r).map(|o| o.to_string()))
        .collect();
    orgs.sort();
    orgs.dedup();

    let total = orgs.len();
    let n_opt = query.get("n").and_then(|s| s.parse::<usize>().ok());
    let n = n_opt.unwrap_or(usize::MAX);
    let start_idx = query
        .get("last")
        .and_then(|last| orgs.iter().position(|t| t == last))
        .map(|i| i.saturating_add(1))
        .unwrap_or(0);
    let end_idx = start_idx.saturating_add(n).min(total);
    let page: Vec<String> = orgs
        .into_iter()
        .skip(start_idx)
        .take(end_idx.saturating_sub(start_idx))
        .collect();
    let has_more = end_idx < total;

    let payload = serde_json::json!({
        "orgs": page,
    });
    let bytes = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(_) => return errors::internal_error().into_response(),
    };

    let mut resp_headers = registry_headers();
    resp_headers.insert("Content-Type", "application/json".parse().unwrap());
    resp_headers.insert("Content-Length", bytes.len().to_string().parse().unwrap());
    if has_more {
        if let (Some(n_raw), Some(last_org)) = (
            query.get("n"),
            payload
                .get("orgs")
                .and_then(|v| v.as_array())
                .and_then(|a| a.last())
                .and_then(|x| x.as_str()),
        ) {
            let last_org = url_encode_component(last_org);
            let link = format!("</_meta/orgs?n={n_raw}&last={last_org}>; rel=\"next\"");
            if let Ok(v) = http::HeaderValue::from_str(&link) {
                resp_headers.insert(http::header::LINK, v);
            }
        }
    }

    (StatusCode::OK, resp_headers, Body::from(bytes)).into_response()
}

pub async fn meta_org_repos(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(org): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if state.config.catalog_requires_auth && !crate::auth::is_authenticated(&state, &headers) {
        return crate::auth::unauthorized_catalog_challenge(&state).into_response();
    }

    let repos = match state.storage.list_repositories().await {
        Ok(r) => r,
        Err(_) => return errors::internal_error().into_response(),
    };
    let prefix = format!("{org}/");
    let mut filtered: Vec<String> = repos
        .into_iter()
        .filter(|r| r == &org || r.starts_with(&prefix))
        .collect();
    filtered.sort();

    let total = filtered.len();
    let n_opt = query.get("n").and_then(|s| s.parse::<usize>().ok());
    let n = n_opt.unwrap_or(usize::MAX);
    let start_idx = query
        .get("last")
        .and_then(|last| filtered.iter().position(|t| t == last))
        .map(|i| i.saturating_add(1))
        .unwrap_or(0);
    let end_idx = start_idx.saturating_add(n).min(total);
    let page: Vec<String> = filtered
        .into_iter()
        .skip(start_idx)
        .take(end_idx.saturating_sub(start_idx))
        .collect();
    let has_more = end_idx < total;

    let mut repos_out: Vec<serde_json::Value> = Vec::new();
    for repo in &page {
        match state.storage.repo_timestamps(repo).await {
            Ok(ts) => repos_out.push(repo_meta_from_timestamps(repo, ts)),
            Err(StorageError::NotFound) => {
                repos_out.push(serde_json::json!({"name": repo, "org": repo_org(repo)}));
            }
            Err(_) => return errors::internal_error().into_response(),
        }
    }

    let payload = serde_json::json!({
        "org": org,
        "repositories": repos_out,
    });
    let bytes = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(_) => return errors::internal_error().into_response(),
    };

    let mut resp_headers = registry_headers();
    resp_headers.insert("Content-Type", "application/json".parse().unwrap());
    resp_headers.insert("Content-Length", bytes.len().to_string().parse().unwrap());
    if has_more {
        if let (Some(n_raw), Some(last_repo)) = (query.get("n"), page.last()) {
            let last_repo = url_encode_component(last_repo);
            let link =
                format!("</_meta/orgs/{org}/repos?n={n_raw}&last={last_repo}>; rel=\"next\"");
            if let Ok(v) = http::HeaderValue::from_str(&link) {
                resp_headers.insert(http::header::LINK, v);
            }
        }
    }

    (StatusCode::OK, resp_headers, Body::from(bytes)).into_response()
}

pub async fn meta_repo(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if state.config.catalog_requires_auth && !crate::auth::is_authenticated(&state, &headers) {
        return crate::auth::unauthorized_catalog_challenge(&state).into_response();
    }

    match state.storage.repo_timestamps(&name).await {
        Ok(ts) => {
            let payload = repo_meta_from_timestamps(&name, ts);
            let bytes = match serde_json::to_vec(&payload) {
                Ok(b) => b,
                Err(_) => return errors::internal_error().into_response(),
            };
            let mut resp_headers = registry_headers();
            resp_headers.insert("Content-Type", "application/json".parse().unwrap());
            resp_headers.insert("Content-Length", bytes.len().to_string().parse().unwrap());
            (StatusCode::OK, resp_headers, Body::from(bytes)).into_response()
        }
        Err(StorageError::NotFound) => errors::name_unknown().into_response(),
        Err(_) => errors::internal_error().into_response(),
    }
}

pub async fn meta_catalog(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if state.config.catalog_requires_auth && !crate::auth::is_authenticated(&state, &headers) {
        return crate::auth::unauthorized_catalog_challenge(&state).into_response();
    }

    let mut repos = match state.storage.list_repositories().await {
        Ok(r) => r,
        Err(_) => return errors::internal_error().into_response(),
    };

    if let Some(org) = query.get("org") {
        let prefix = format!("{org}/");
        repos.retain(|r| r == org || r.starts_with(&prefix));
    }

    repos.sort();

    let total = repos.len();
    let n_opt = query.get("n").and_then(|s| s.parse::<usize>().ok());
    let n = n_opt.unwrap_or(usize::MAX);
    let start_idx = query
        .get("last")
        .and_then(|last| repos.iter().position(|t| t == last))
        .map(|i| i.saturating_add(1))
        .unwrap_or(0);
    let end_idx = start_idx.saturating_add(n).min(total);
    let page: Vec<String> = repos
        .into_iter()
        .skip(start_idx)
        .take(end_idx.saturating_sub(start_idx))
        .collect();
    let has_more = end_idx < total;

    let mut repos_out: Vec<serde_json::Value> = Vec::new();
    for repo in &page {
        match state.storage.repo_timestamps(repo).await {
            Ok(ts) => repos_out.push(repo_meta_from_timestamps(repo, ts)),
            Err(StorageError::NotFound) => {
                repos_out.push(serde_json::json!({"name": repo, "org": repo_org(repo)}));
            }
            Err(_) => return errors::internal_error().into_response(),
        }
    }

    let payload = serde_json::json!({
        "repositories": repos_out,
    });
    let bytes = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(_) => return errors::internal_error().into_response(),
    };

    let mut resp_headers = registry_headers();
    resp_headers.insert("Content-Type", "application/json".parse().unwrap());
    resp_headers.insert("Content-Length", bytes.len().to_string().parse().unwrap());
    if has_more {
        if let (Some(n_raw), Some(last_repo)) = (query.get("n"), page.last()) {
            let last_repo = url_encode_component(last_repo);
            let mut link = format!("</_meta/catalog?n={n_raw}&last={last_repo}");
            if let Some(org) = query.get("org") {
                let org = url_encode_component(org);
                link.push_str(&format!("&org={org}"));
            }
            link.push_str(">; rel=\"next\"");
            if let Ok(v) = http::HeaderValue::from_str(&link) {
                resp_headers.insert(http::header::LINK, v);
            }
        }
    }

    (StatusCode::OK, resp_headers, Body::from(bytes)).into_response()
}

async fn tags_list(
    state: AppState,
    method: Method,
    name: &str,
    query: &HashMap<String, String>,
    route_mode: V2RouteMode,
    proxy_ctx: Option<ProxyContext>,
) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }

    if route_mode == V2RouteMode::ProxyOnly {
        match method {
            Method::GET | Method::HEAD => {}
            _ => return StatusCode::METHOD_NOT_ALLOWED.into_response(),
        }
    }

    let storage = match route_mode {
        V2RouteMode::Default => state.storage.clone(),
        V2RouteMode::ProxyOnly => match proxy_ctx.as_ref() {
            Some(ctx) => ctx.cache.clone(),
            None => return errors::internal_error().into_response(),
        },
    };

    match method {
        Method::GET | Method::HEAD => match storage.list_tags(name).await {
            Ok(all_tags) => {
                // Pagination per OCI/Docker distribution spec:
                // - `n` limits the number of tags
                // - `last` starts listing after the provided tag
                let total = all_tags.len();
                let n_opt = query.get("n").and_then(|s| s.parse::<usize>().ok());
                let n = n_opt.unwrap_or(usize::MAX);

                let start_idx = query
                    .get("last")
                    .and_then(|last| all_tags.iter().position(|t| t == last))
                    .map(|i| i.saturating_add(1))
                    .unwrap_or(0);

                let end_idx = start_idx.saturating_add(n).min(total);
                let tags: Vec<String> = all_tags
                    .into_iter()
                    .skip(start_idx)
                    .take(end_idx.saturating_sub(start_idx))
                    .collect();

                let has_more = end_idx < total;

                let payload = serde_json::json!({
                    "name": name,
                    "tags": tags,
                });
                let bytes = match serde_json::to_vec(&payload) {
                    Ok(b) => b,
                    Err(_) => return errors::internal_error().into_response(),
                };

                let mut headers = registry_headers();
                headers.insert("Content-Type", "application/json".parse().unwrap());
                headers.insert("Content-Length", bytes.len().to_string().parse().unwrap());

                // Best-effort Link header for next page.
                if has_more {
                    if let (Some(n_raw), Some(last_tag)) = (query.get("n"), tags.last()) {
                        let last_tag = url_encode_component(last_tag);
                        let link = format!(
                            "</v2/{name}/tags/list?n={n_raw}&last={last_tag}>; rel=\"next\""
                        );
                        if let Ok(v) = http::HeaderValue::from_str(&link) {
                            headers.insert(http::header::LINK, v);
                        }
                    }
                }

                if method == Method::HEAD {
                    return (StatusCode::OK, headers).into_response();
                }
                (StatusCode::OK, headers, Body::from(bytes)).into_response()
            }
            Err(StorageError::NotFound) => errors::name_unknown().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
            Err(StorageError::InsufficientStorage) => {
                errors::insufficient_storage().into_response()
            }
            Err(StorageError::TooLarge) => errors::internal_error().into_response(),
            Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
            Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
        },
        _ => errors::not_implemented().into_response(),
    }
}

async fn blob_by_digest(
    state: AppState,
    method: Method,
    name: &str,
    digest_str: &str,
    route_mode: V2RouteMode,
    proxy_ctx: Option<ProxyContext>,
) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }

    let digest = match Digest::parse(digest_str) {
        Ok(d) => d,
        Err(_) => return errors::digest_invalid().into_response(),
    };

    if route_mode == V2RouteMode::ProxyOnly {
        return blob_by_digest_proxy_only(state, method, name, digest, proxy_ctx).await;
    }

    match method {
        Method::DELETE => {
            // Fast path: if the blob doesn't exist, don't do an expensive reference scan.
            match state.storage.head_blob(&digest).await {
                Ok(_) => {}
                Err(StorageError::NotFound) => return errors::blob_unknown().into_response(),
                Err(StorageError::Unsupported) => return errors::not_implemented().into_response(),
                Err(StorageError::InsufficientStorage) => {
                    return errors::insufficient_storage().into_response();
                }
                Err(_) => return errors::internal_error().into_response(),
            }

            // Prefer the persistent ref-index (fast). On index errors, fall back to scanning.
            if let Some(idx) = state.ref_index.as_ref() {
                match idx.is_blob_referenced(&digest) {
                    Ok(true) => return errors::blob_in_use("blob is still referenced").into_response(),
                    Ok(false) => {}
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            digest = digest.as_str(),
                            "ref-index lookup failed; attempting rebuild"
                        );

                        if state.config.ref_index.auto_rebuild_on_corruption {
                            let _ = idx
                                .ensure_healthy_or_rebuild(
                                    &state.storage,
                                    true,
                                    false,
                                )
                                .await;
                        }

                        match idx.is_blob_referenced(&digest) {
                            Ok(true) => {
                                return errors::blob_in_use("blob is still referenced")
                                    .into_response();
                            }
                            Ok(false) => {}
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    digest = digest.as_str(),
                                    "ref-index still unhealthy; falling back to full scan"
                                );
                                match crate::blob_delete_safety::find_blob_reference(
                                    &state.storage,
                                    &digest,
                                )
                                .await
                                {
                                    Ok(Some(r)) => {
                                        let mut msg = format!(
                                            "blob is still referenced by manifest {}",
                                            r.manifest
                                        );
                                        if let Some(tag) = r.tag {
                                            msg = format!("{msg} (repo={}, tag={})", r.repo, tag);
                                        } else {
                                            msg = format!("{msg} (repo={})", r.repo);
                                        }
                                        return errors::blob_in_use(&msg).into_response();
                                    }
                                    Ok(None) => {}
                                    Err(err) => {
                                        tracing::warn!(
                                            error = %err,
                                            digest = digest.as_str(),
                                            "safe blob delete: failed to scan for references"
                                        );
                                        return errors::internal_error().into_response();
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                match crate::blob_delete_safety::find_blob_reference(&state.storage, &digest).await {
                    Ok(Some(r)) => {
                        let mut msg = format!("blob is still referenced by manifest {}", r.manifest);
                        if let Some(tag) = r.tag {
                            msg = format!("{msg} (repo={}, tag={})", r.repo, tag);
                        } else {
                            msg = format!("{msg} (repo={})", r.repo);
                        }
                        return errors::blob_in_use(&msg).into_response();
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            digest = digest.as_str(),
                            "safe blob delete: failed to scan for references"
                        );
                        return errors::internal_error().into_response();
                    }
                }
            }

            match state.storage.delete_blob(&digest).await {
                Ok(()) => (StatusCode::ACCEPTED, registry_headers()).into_response(),
                Err(StorageError::NotFound) => errors::blob_unknown().into_response(),
                Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
                Err(StorageError::InsufficientStorage) => {
                    errors::insufficient_storage().into_response()
                }
                Err(StorageError::TooLarge) => errors::internal_error().into_response(),
                Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
                Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
            }
        }
        Method::HEAD => match state.storage.head_blob(&digest).await {
            Ok(meta) => {
                let mut headers = registry_headers();
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                (StatusCode::OK, headers).into_response()
            }
            Err(StorageError::NotFound) => {
                if let Some(ctx) = proxy_ctx.as_ref() {
                    if let Ok(meta) = ctx.cache.head_blob(&digest).await {
                        ctx.proxy.note_blob_access(&digest);
                        let mut headers = registry_headers();
                        headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                        headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                        headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                        return (StatusCode::OK, headers).into_response();
                    }
                }

                if let Some(ctx) = proxy_ctx.as_ref() {
                    if let Ok(decision) = ctx.proxy.decision_for_repo(name) {
                        match ctx.proxy.head_blob_upstream(&decision, &digest).await {
                            Ok(size) => {
                                let mut headers = registry_headers();
                                headers.insert(
                                    "Docker-Content-Digest",
                                    digest.as_str().parse().unwrap(),
                                );
                                headers.insert(
                                    "Content-Type",
                                    "application/octet-stream".parse().unwrap(),
                                );
                                headers.insert("Content-Length", size.to_string().parse().unwrap());
                                return (StatusCode::OK, headers).into_response();
                            }
                            Err(crate::proxy::ProxyError::NotFound) => {}
                            Err(err) => {
                                tracing::warn!(error = %err, repo = name, digest = digest.as_str(), "proxy: head blob failed");
                                return errors::internal_error().into_response();
                            }
                        }
                    }
                }
                errors::blob_unknown().into_response()
            }
            Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
            Err(StorageError::TooLarge) => errors::internal_error().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
            Err(StorageError::InsufficientStorage) => {
                errors::insufficient_storage().into_response()
            }
            Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
        },
        Method::GET => match state.storage.open_blob(&digest).await {
            Ok((meta, reader)) => {
                let stream = ReaderStream::new(reader);
                let body = Body::from_stream(stream);

                let mut headers = registry_headers();
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                (StatusCode::OK, headers, body).into_response()
            }
            Err(StorageError::NotFound) => {
                if let Some(ctx) = proxy_ctx.as_ref() {
                    if let Ok((meta, reader)) = ctx.cache.open_blob(&digest).await {
                        ctx.proxy.note_blob_access(&digest);
                        let stream = ReaderStream::new(reader);
                        let body = Body::from_stream(stream);

                        let mut headers = registry_headers();
                        headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                        headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                        headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                        return (StatusCode::OK, headers, body).into_response();
                    }
                }

                if let Some(ctx) = proxy_ctx.as_ref() {
                    if let Ok(decision) = ctx.proxy.decision_for_repo(name) {
                        match ctx
                            .proxy
                            .fetch_blob_into_storage(&decision, &digest, &ctx.cache)
                            .await
                        {
                            Ok(()) => {
                                // Retry from cache storage.
                                if let Ok((meta, reader)) = ctx.cache.open_blob(&digest).await {
                                    ctx.proxy.note_blob_access(&digest);
                                    let stream = ReaderStream::new(reader);
                                    let body = Body::from_stream(stream);

                                    let mut headers = registry_headers();
                                    headers.insert(
                                        "Docker-Content-Digest",
                                        digest.as_str().parse().unwrap(),
                                    );
                                    headers.insert(
                                        "Content-Type",
                                        "application/octet-stream".parse().unwrap(),
                                    );
                                    headers.insert(
                                        "Content-Length",
                                        meta.size.to_string().parse().unwrap(),
                                    );
                                    return (StatusCode::OK, headers, body).into_response();
                                }
                            }
                            Err(crate::proxy::ProxyError::NotFound) => {}
                            Err(err) => {
                                tracing::warn!(error = %err, repo = name, digest = digest.as_str(), "proxy: fetch blob failed");
                                return errors::internal_error().into_response();
                            }
                        }
                    }
                }
                errors::blob_unknown().into_response()
            }
            Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
            Err(StorageError::TooLarge) => errors::internal_error().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
            Err(StorageError::InsufficientStorage) => {
                errors::insufficient_storage().into_response()
            }
            Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
        },
        _ => errors::not_implemented().into_response(),
    }
}

async fn blob_by_digest_proxy_only(
    _state: AppState,
    method: Method,
    name: &str,
    digest: Digest,
    proxy_ctx: Option<ProxyContext>,
) -> Response {
    let Some(ctx) = proxy_ctx.as_ref() else {
        return errors::internal_error().into_response();
    };

    match method {
        Method::GET => {
            if let Ok((meta, reader)) = ctx.cache.open_blob(&digest).await {
                ctx.proxy.note_blob_access(&digest);
                let stream = ReaderStream::new(reader);
                let body = Body::from_stream(stream);

                let mut headers = registry_headers();
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                return (StatusCode::OK, headers, body).into_response();
            }

            if let Ok(decision) = ctx.proxy.decision_for_repo(name) {
                match ctx
                    .proxy
                    .fetch_blob_into_storage(&decision, &digest, &ctx.cache)
                    .await
                {
                    Ok(()) => {
                        if let Ok((meta, reader)) = ctx.cache.open_blob(&digest).await {
                            ctx.proxy.note_blob_access(&digest);
                            let stream = ReaderStream::new(reader);
                            let body = Body::from_stream(stream);

                            let mut headers = registry_headers();
                            headers
                                .insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                            headers.insert(
                                "Content-Type",
                                "application/octet-stream".parse().unwrap(),
                            );
                            headers
                                .insert("Content-Length", meta.size.to_string().parse().unwrap());
                            return (StatusCode::OK, headers, body).into_response();
                        }
                    }
                    Err(crate::proxy::ProxyError::NotFound) => {}
                    Err(err) => {
                        tracing::warn!(error = %err, repo = name, digest = digest.as_str(), "proxy: fetch blob failed");
                        return errors::internal_error().into_response();
                    }
                }
            }

            errors::blob_unknown().into_response()
        }
        Method::HEAD => {
            if let Ok(meta) = ctx.cache.head_blob(&digest).await {
                ctx.proxy.note_blob_access(&digest);
                let mut headers = registry_headers();
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                return (StatusCode::OK, headers).into_response();
            }

            if let Ok(decision) = ctx.proxy.decision_for_repo(name) {
                match ctx.proxy.head_blob_upstream(&decision, &digest).await {
                    Ok(size) => {
                        let mut headers = registry_headers();
                        headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                        headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                        headers.insert("Content-Length", size.to_string().parse().unwrap());
                        return (StatusCode::OK, headers).into_response();
                    }
                    Err(crate::proxy::ProxyError::NotFound) => {}
                    Err(err) => {
                        tracing::warn!(error = %err, repo = name, digest = digest.as_str(), "proxy: head blob failed");
                        return errors::internal_error().into_response();
                    }
                }
            }

            errors::blob_unknown().into_response()
        }
        // Proxy-only host: never mutate local storage.
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn manifest_by_reference(
    state: AppState,
    method: Method,
    name: &str,
    reference: &str,
    route_mode: V2RouteMode,
    proxy_ctx: Option<ProxyContext>,
    request_host: Option<&str>,
) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }

    // Reference can be a digest or a tag.
    let is_digest_ref = Digest::parse(reference).is_ok();

    if route_mode == V2RouteMode::ProxyOnly {
        return manifest_by_reference_proxy_only(
            state,
            method,
            name,
            reference,
            is_digest_ref,
            proxy_ctx,
            request_host,
        )
        .await;
    }

    // Resolve tag references to a digest (with optional proxying).
    let digest = if let Ok(d) = Digest::parse(reference) {
        d
    } else {
        match state.storage.resolve_tag(name, reference).await {
            Ok(d) => d,
            Err(StorageError::NotFound) => {
                // Check cache storage for an existing cached tag.
                let Some(ctx) = proxy_ctx.as_ref() else {
                    // Tag not known locally and no cache store.
                    return errors::manifest_unknown().into_response();
                };

                if let Ok(d) = ctx.cache.resolve_tag(name, reference).await {
                    d
                } else {
                    // Tag not known locally. If proxying is enabled and repo is allowed, resolve from upstream.
                    let Ok(decision) = ctx.proxy.decision_for_repo(name) else {
                        return errors::manifest_unknown().into_response();
                    };

                    match decision.tag_policy.clone() {
                        crate::config::TagPolicy::DigestOnly => {
                            let _permit =
                                match state.buffered_body_sem.clone().acquire_owned().await {
                                    Ok(p) => p,
                                    Err(_) => return errors::internal_error().into_response(),
                                };
                            match ctx
                                .proxy
                                .fetch_manifest_and_cache(
                                    &decision,
                                    reference,
                                    &ctx.cache,
                                    state.config.max_request_body_bytes,
                                    false,
                                    None,
                                )
                                .await
                            {
                                Ok(crate::proxy::FetchManifestResult::Fetched {
                                    digest, ..
                                }) => digest,
                                Ok(_) => return errors::internal_error().into_response(),
                                Err(crate::proxy::ProxyError::NotFound) => {
                                    return errors::manifest_unknown().into_response();
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        error = %err,
                                        request_host = request_host.unwrap_or("<missing>"),
                                        proxy_upstream = ctx.proxy.upstream_base_url_for_log().unwrap_or("<unset>"),
                                        repo = name,
                                        tag = reference,
                                        "proxy: resolve tag failed"
                                    );
                                    return errors::internal_error().into_response();
                                }
                            }
                        }
                        crate::config::TagPolicy::TtlSeconds(ttl) => {
                            if let Err(resp) = ensure_tag_fresh(
                                &state, &ctx.proxy, &decision, &ctx.cache, reference, ttl, false,
                            )
                            .await
                            {
                                return resp;
                            }
                            match ctx.cache.resolve_tag(name, reference).await {
                                Ok(d) => d,
                                Err(StorageError::NotFound) => {
                                    return errors::manifest_unknown().into_response();
                                }
                                Err(_) => return errors::internal_error().into_response(),
                            }
                        }
                        crate::config::TagPolicy::AlwaysRevalidate => {
                            if let Err(resp) = ensure_tag_fresh(
                                &state, &ctx.proxy, &decision, &ctx.cache, reference, 0, true,
                            )
                            .await
                            {
                                return resp;
                            }
                            match ctx.cache.resolve_tag(name, reference).await {
                                Ok(d) => d,
                                Err(StorageError::NotFound) => {
                                    return errors::manifest_unknown().into_response();
                                }
                                Err(_) => return errors::internal_error().into_response(),
                            }
                        }
                    }
                }
            }
            Err(StorageError::DigestMismatch) => return errors::internal_error().into_response(),
            Err(StorageError::TooLarge) => return errors::internal_error().into_response(),
            Err(StorageError::Unsupported) => return errors::not_implemented().into_response(),
            Err(StorageError::InsufficientStorage) => {
                return errors::insufficient_storage().into_response();
            }
            Err(StorageError::Internal(_)) => return errors::internal_error().into_response(),
        }
    };

    // Record tag access for eviction/observability (best-effort).
    if Digest::parse(reference).is_err() {
        if let Some(ctx) = proxy_ctx.as_ref() {
            ctx.proxy.note_tag_access(name, reference);
        }
    }

    match method {
        Method::DELETE => match state.storage.delete_manifest(name, &digest).await {
            Ok(()) => {
                if let Some(idx) = state.ref_index.as_ref() {
                    if let Err(err) = idx.sync_repo_tags(&state.storage, name).await {
                        tracing::warn!(
                            error = %err,
                            repo = name,
                            "ref-index: failed to resync tags after manifest delete"
                        );
                    }
                }
                (StatusCode::ACCEPTED, registry_headers()).into_response()
            }
            Err(StorageError::NotFound) => errors::manifest_unknown().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
            Err(StorageError::InsufficientStorage) => {
                errors::insufficient_storage().into_response()
            }
            Err(StorageError::TooLarge) => errors::internal_error().into_response(),
            Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
            Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
        },
        Method::HEAD => match state.storage.head_manifest(name, &digest).await {
            Ok(meta) => {
                let mut headers = registry_headers();
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert("Content-Type", meta.media_type.parse().unwrap());
                headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                (StatusCode::OK, headers).into_response()
            }
            Err(StorageError::NotFound) => {
                if let Some(ctx) = proxy_ctx.as_ref() {
                    if let Ok(meta) = ctx.cache.head_manifest(name, &digest).await {
                        ctx.proxy.note_manifest_access(name, &digest);
                        if let Some(refs) = ctx.proxy.get_manifest_refs(name, &digest) {
                            for blob in &refs.blobs {
                                if let Ok(d) = crate::registry::digest::Digest::parse(blob) {
                                    ctx.proxy.note_blob_access(&d);
                                }
                            }
                        }
                        let mut headers = registry_headers();
                        headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                        headers.insert("Content-Type", meta.media_type.parse().unwrap());
                        headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                        return (StatusCode::OK, headers).into_response();
                    }
                }

                // For manifests, on miss we fetch+cache on HEAD too (small), to avoid extra upstream roundtrips.
                if let Some(ctx) = proxy_ctx.as_ref() {
                    if let Ok(decision) = ctx.proxy.decision_for_repo(name) {
                        let _permit = match state.buffered_body_sem.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => return errors::internal_error().into_response(),
                        };
                        let digest_ref;
                        let upstream_ref: &str = if is_digest_ref {
                            digest_ref = digest.as_str();
                            digest_ref.as_str()
                        } else {
                            reference
                        };
                        match ctx
                            .proxy
                            .fetch_manifest_and_cache(
                                &decision,
                                upstream_ref,
                                &ctx.cache,
                                state.config.max_request_body_bytes,
                                false,
                                None,
                            )
                            .await
                        {
                            Ok(_) => {
                                if let Ok(meta) = ctx.cache.head_manifest(name, &digest).await {
                                    let mut headers = registry_headers();
                                    headers.insert(
                                        "Docker-Content-Digest",
                                        digest.as_str().parse().unwrap(),
                                    );
                                    headers
                                        .insert("Content-Type", meta.media_type.parse().unwrap());
                                    headers.insert(
                                        "Content-Length",
                                        meta.size.to_string().parse().unwrap(),
                                    );
                                    return (StatusCode::OK, headers).into_response();
                                }
                            }
                            Err(crate::proxy::ProxyError::NotFound) => {}
                            Err(err) => {
                                tracing::warn!(error = %err, repo = name, reference, "proxy: fetch manifest failed");
                                return errors::internal_error().into_response();
                            }
                        }
                    }
                }
                errors::manifest_unknown().into_response()
            }
            Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
            Err(StorageError::TooLarge) => errors::internal_error().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
            Err(StorageError::InsufficientStorage) => {
                errors::insufficient_storage().into_response()
            }
            Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
        },
        Method::GET => match state.storage.get_manifest(name, &digest).await {
            Ok((meta, bytes)) => {
                let mut headers = registry_headers();
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert("Content-Type", meta.media_type.parse().unwrap());
                headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                (StatusCode::OK, headers, Body::from(bytes)).into_response()
            }
            Err(StorageError::NotFound) => {
                if let Some(ctx) = proxy_ctx.as_ref() {
                    if let Ok((meta, bytes)) = ctx.cache.get_manifest(name, &digest).await {
                        ctx.proxy.note_manifest_access(name, &digest);
                        if let Some(refs) = ctx.proxy.get_manifest_refs(name, &digest) {
                            for blob in &refs.blobs {
                                if let Ok(d) = crate::registry::digest::Digest::parse(blob) {
                                    ctx.proxy.note_blob_access(&d);
                                }
                            }
                        }
                        let mut headers = registry_headers();
                        headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                        headers.insert("Content-Type", meta.media_type.parse().unwrap());
                        headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                        return (StatusCode::OK, headers, Body::from(bytes)).into_response();
                    }
                }

                if let Some(ctx) = proxy_ctx.as_ref() {
                    if let Ok(decision) = ctx.proxy.decision_for_repo(name) {
                        let _permit = match state.buffered_body_sem.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => return errors::internal_error().into_response(),
                        };
                        let digest_ref;
                        let upstream_ref: &str = if is_digest_ref {
                            digest_ref = digest.as_str();
                            digest_ref.as_str()
                        } else {
                            reference
                        };
                        match ctx
                            .proxy
                            .fetch_manifest_and_cache(
                                &decision,
                                upstream_ref,
                                &ctx.cache,
                                state.config.max_request_body_bytes,
                                false,
                                None,
                            )
                            .await
                        {
                            Ok(_) => {
                                if let Ok((meta, bytes)) =
                                    ctx.cache.get_manifest(name, &digest).await
                                {
                                    let mut headers = registry_headers();
                                    headers.insert(
                                        "Docker-Content-Digest",
                                        digest.as_str().parse().unwrap(),
                                    );
                                    headers
                                        .insert("Content-Type", meta.media_type.parse().unwrap());
                                    headers.insert(
                                        "Content-Length",
                                        meta.size.to_string().parse().unwrap(),
                                    );
                                    return (StatusCode::OK, headers, Body::from(bytes))
                                        .into_response();
                                }
                            }
                            Err(crate::proxy::ProxyError::NotFound) => {}
                            Err(err) => {
                                tracing::warn!(error = %err, repo = name, reference, "proxy: fetch manifest failed");
                                return errors::internal_error().into_response();
                            }
                        }
                    }
                }
                errors::manifest_unknown().into_response()
            }
            Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
            Err(StorageError::TooLarge) => errors::internal_error().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
            Err(StorageError::InsufficientStorage) => {
                errors::insufficient_storage().into_response()
            }
            Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
        },
        _ => errors::not_implemented().into_response(),
    }
}

async fn manifest_by_reference_proxy_only(
    state: AppState,
    method: Method,
    name: &str,
    reference: &str,
    is_digest_ref: bool,
    proxy_ctx: Option<ProxyContext>,
    request_host: Option<&str>,
) -> Response {
    match method {
        Method::GET | Method::HEAD => {}
        _ => return StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }

    let Some(ctx) = proxy_ctx.as_ref() else {
        return errors::internal_error().into_response();
    };

    // Resolve tag references to a digest using cache/upstream only.
    let digest = if let Ok(d) = Digest::parse(reference) {
        d
    } else {
        match ctx.cache.resolve_tag(name, reference).await {
            Ok(d) => d,
            Err(StorageError::NotFound) => {
                let Ok(decision) = ctx.proxy.decision_for_repo(name) else {
                    return errors::manifest_unknown().into_response();
                };

                match decision.tag_policy.clone() {
                    crate::config::TagPolicy::DigestOnly => {
                        let _permit = match state.buffered_body_sem.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => return errors::internal_error().into_response(),
                        };
                        match ctx
                            .proxy
                            .fetch_manifest_and_cache(
                                &decision,
                                reference,
                                &ctx.cache,
                                state.config.max_request_body_bytes,
                                false,
                                None,
                            )
                            .await
                        {
                            Ok(crate::proxy::FetchManifestResult::Fetched { digest, .. }) => digest,
                            Ok(_) => return errors::internal_error().into_response(),
                            Err(crate::proxy::ProxyError::NotFound) => {
                                return errors::manifest_unknown().into_response();
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    request_host = request_host.unwrap_or("<missing>"),
                                    proxy_upstream = ctx.proxy.upstream_base_url_for_log().unwrap_or("<unset>"),
                                    repo = name,
                                    tag = reference,
                                    "proxy: resolve tag failed"
                                );
                                return errors::internal_error().into_response();
                            }
                        }
                    }
                    crate::config::TagPolicy::TtlSeconds(ttl) => {
                        if let Err(resp) = ensure_tag_fresh(
                            &state, &ctx.proxy, &decision, &ctx.cache, reference, ttl, false,
                        )
                        .await
                        {
                            return resp;
                        }
                        match ctx.cache.resolve_tag(name, reference).await {
                            Ok(d) => d,
                            Err(StorageError::NotFound) => {
                                return errors::manifest_unknown().into_response();
                            }
                            Err(_) => return errors::internal_error().into_response(),
                        }
                    }
                    crate::config::TagPolicy::AlwaysRevalidate => {
                        if let Err(resp) = ensure_tag_fresh(
                            &state, &ctx.proxy, &decision, &ctx.cache, reference, 0, true,
                        )
                        .await
                        {
                            return resp;
                        }
                        match ctx.cache.resolve_tag(name, reference).await {
                            Ok(d) => d,
                            Err(StorageError::NotFound) => {
                                return errors::manifest_unknown().into_response();
                            }
                            Err(_) => return errors::internal_error().into_response(),
                        }
                    }
                }
            }
            Err(_) => return errors::internal_error().into_response(),
        }
    };

    // Record tag access for eviction/observability (best-effort).
    if Digest::parse(reference).is_err() {
        ctx.proxy.note_tag_access(name, reference);
    }

    match method {
        Method::HEAD => {
            if let Ok(meta) = ctx.cache.head_manifest(name, &digest).await {
                ctx.proxy.note_manifest_access(name, &digest);
                if let Some(refs) = ctx.proxy.get_manifest_refs(name, &digest) {
                    for blob in &refs.blobs {
                        if let Ok(d) = crate::registry::digest::Digest::parse(blob) {
                            ctx.proxy.note_blob_access(&d);
                        }
                    }
                }
                let mut headers = registry_headers();
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert("Content-Type", meta.media_type.parse().unwrap());
                headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                return (StatusCode::OK, headers).into_response();
            }

            if let Ok(decision) = ctx.proxy.decision_for_repo(name) {
                let _permit = match state.buffered_body_sem.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return errors::internal_error().into_response(),
                };
                let digest_ref;
                let upstream_ref: &str = if is_digest_ref {
                    digest_ref = digest.as_str();
                    digest_ref.as_str()
                } else {
                    reference
                };

                match ctx
                    .proxy
                    .fetch_manifest_and_cache(
                        &decision,
                        upstream_ref,
                        &ctx.cache,
                        state.config.max_request_body_bytes,
                        false,
                        None,
                    )
                    .await
                {
                    Ok(_) => {
                        if let Ok(meta) = ctx.cache.head_manifest(name, &digest).await {
                            let mut headers = registry_headers();
                            headers
                                .insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                            headers.insert("Content-Type", meta.media_type.parse().unwrap());
                            headers
                                .insert("Content-Length", meta.size.to_string().parse().unwrap());
                            return (StatusCode::OK, headers).into_response();
                        }
                    }
                    Err(crate::proxy::ProxyError::NotFound) => {}
                    Err(err) => {
                        tracing::warn!(error = %err, repo = name, reference, "proxy: fetch manifest failed");
                        return errors::internal_error().into_response();
                    }
                }
            }

            errors::manifest_unknown().into_response()
        }
        Method::GET => {
            if let Ok((meta, bytes)) = ctx.cache.get_manifest(name, &digest).await {
                ctx.proxy.note_manifest_access(name, &digest);
                if let Some(refs) = ctx.proxy.get_manifest_refs(name, &digest) {
                    for blob in &refs.blobs {
                        if let Ok(d) = crate::registry::digest::Digest::parse(blob) {
                            ctx.proxy.note_blob_access(&d);
                        }
                    }
                }
                let mut headers = registry_headers();
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert("Content-Type", meta.media_type.parse().unwrap());
                headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                return (StatusCode::OK, headers, Body::from(bytes)).into_response();
            }

            if let Ok(decision) = ctx.proxy.decision_for_repo(name) {
                let _permit = match state.buffered_body_sem.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return errors::internal_error().into_response(),
                };
                let digest_ref;
                let upstream_ref: &str = if is_digest_ref {
                    digest_ref = digest.as_str();
                    digest_ref.as_str()
                } else {
                    reference
                };

                match ctx
                    .proxy
                    .fetch_manifest_and_cache(
                        &decision,
                        upstream_ref,
                        &ctx.cache,
                        state.config.max_request_body_bytes,
                        false,
                        None,
                    )
                    .await
                {
                    Ok(_) => {
                        if let Ok((meta, bytes)) = ctx.cache.get_manifest(name, &digest).await {
                            let mut headers = registry_headers();
                            headers
                                .insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                            headers.insert("Content-Type", meta.media_type.parse().unwrap());
                            headers
                                .insert("Content-Length", meta.size.to_string().parse().unwrap());
                            return (StatusCode::OK, headers, Body::from(bytes)).into_response();
                        }
                    }
                    Err(crate::proxy::ProxyError::NotFound) => {}
                    Err(err) => {
                        tracing::warn!(error = %err, repo = name, reference, "proxy: fetch manifest failed");
                        return errors::internal_error().into_response();
                    }
                }
            }

            errors::manifest_unknown().into_response()
        }
        _ => errors::not_implemented().into_response(),
    }
}

fn is_valid_repo_name(name: &str) -> bool {
    if name.is_empty() || name.starts_with('/') {
        return false;
    }
    for segment in name.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return false;
        }
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-'))
}

fn is_valid_tag(tag: &str) -> bool {
    if tag.is_empty() || tag.len() > 128 {
        return false;
    }
    if tag.contains('/') || tag.contains(char::is_whitespace) {
        return false;
    }
    let mut chars = tag.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphanumeric() || first == '_') {
        return false;
    }
    tag.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
}

#[cfg(test)]
mod tests {
    use super::{
        decide_token_scopes_for_request, is_valid_repo_name, is_valid_tag, parse_scopes,
        sanitize_token_scopes, service_param_is_valid, token_scope_requests_repo_action,
        wants_push_from_token_scopes,
    };
    use crate::config::{Config, ProxyConfig, ProxyMode, RobotsConfig, UploadPolicyConfig};
    use crate::rbac::Grant;
    use crate::robot_secrets;
    use crate::security;
    use std::net::SocketAddr;
    use std::path::PathBuf;

    #[test]
    fn repo_name_validation() {
        assert!(is_valid_repo_name("library/alpine"));
        assert!(is_valid_repo_name("org.name/repo_name-1"));
        assert!(!is_valid_repo_name(""));
        assert!(!is_valid_repo_name("/leading"));
        assert!(!is_valid_repo_name(".."));
        assert!(!is_valid_repo_name("a/../b"));
        assert!(!is_valid_repo_name("a b"));
    }

    #[test]
    fn tag_validation() {
        assert!(is_valid_tag("latest"));
        assert!(is_valid_tag("v1.2.3"));
        assert!(is_valid_tag("_start_ok"));
        assert!(!is_valid_tag(""));
        assert!(!is_valid_tag("has space"));
        assert!(!is_valid_tag("has/slash"));
        assert!(!is_valid_tag("-badstart"));
    }

    #[test]
    fn parse_scopes_normalizes_typ_and_actions() {
        let scopes = parse_scopes("RePoSiToRy:org/repo:PUSH,Pull");
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].typ, "repository");
        assert_eq!(scopes[0].name, "org/repo");
        assert_eq!(
            scopes[0].actions,
            vec!["push".to_string(), "pull".to_string()]
        );
    }

    #[test]
    fn parse_scopes_splits_by_whitespace_into_multiple_items() {
        let scopes = parse_scopes("repository:org/repo:pull  repository:org/repo2:push");
        assert_eq!(scopes.len(), 2);
        assert_eq!(scopes[0].typ, "repository");
        assert_eq!(scopes[0].name, "org/repo");
        assert_eq!(scopes[0].actions, vec!["pull".to_string()]);
        assert_eq!(scopes[1].name, "org/repo2");
        assert_eq!(scopes[1].actions, vec!["push".to_string()]);
    }

    #[test]
    fn sanitize_token_scopes_drops_non_repository_scope_types() {
        let scopes = parse_scopes("registry:catalog:*:push repository:org/repo:pull");
        let token_scopes = sanitize_token_scopes(&scopes);
        assert_eq!(token_scopes.len(), 1);
        assert_eq!(token_scopes[0].typ, "repository");
        assert_eq!(token_scopes[0].name, "org/repo");
    }

    #[test]
    fn service_param_validation_allows_missing_and_requires_exact_match() {
        assert!(service_param_is_valid(None, "registry"));
        assert!(service_param_is_valid(Some("registry"), "registry"));
        assert!(!service_param_is_valid(Some("other"), "registry"));
    }

    #[test]
    fn parse_scopes_deduplicates_actions_preserving_order() {
        let scopes = parse_scopes("repository:org/repo:pull,pull,push,pull");
        assert_eq!(scopes.len(), 1);
        assert_eq!(
            scopes[0].actions,
            vec!["pull".to_string(), "push".to_string()]
        );
    }

    #[test]
    fn scope_requests_repo_action_requires_repository_type() {
        let scopes = parse_scopes("registry:catalog:*:push repository:org/repo:pull");
        assert_eq!(scopes.len(), 2);

        let token_scopes = sanitize_token_scopes(&scopes);
        assert_eq!(token_scopes.len(), 1);

        // Only repository scopes are minted, and this one is pull-only.
        assert!(!token_scope_requests_repo_action(
            &token_scopes[0],
            security::RepoAction::Push
        ));
    }

    #[test]
    fn sanitize_token_scopes_drops_unknown_actions() {
        let scopes = parse_scopes("repository:org/repo:pull,delete,push");
        let token_scopes = sanitize_token_scopes(&scopes);
        assert_eq!(token_scopes.len(), 1);
        assert_eq!(
            token_scopes[0].actions,
            vec!["pull".to_string(), "push".to_string()]
        );
    }

    #[test]
    fn sanitize_token_scopes_drops_empty_scopes() {
        let scopes = parse_scopes("repository:org/repo:delete");
        let token_scopes = sanitize_token_scopes(&scopes);
        assert!(token_scopes.is_empty());
    }

    #[test]
    fn sanitize_token_scopes_drops_empty_repo_names() {
        let scopes = parse_scopes("repository::pull");
        let token_scopes = sanitize_token_scopes(&scopes);
        assert!(token_scopes.is_empty());
    }

    #[test]
    fn parse_scopes_ignores_whitespace_only() {
        let scopes = parse_scopes("   \t  ");
        assert!(scopes.is_empty());
    }

    #[test]
    fn sanitize_token_scopes_drops_empty_action_lists() {
        let scopes = parse_scopes("repository:org/repo:");
        let token_scopes = sanitize_token_scopes(&scopes);
        assert!(token_scopes.is_empty());
    }

    #[test]
    fn wants_push_ignores_unknown_only_actions() {
        let scopes = parse_scopes("repository:org/repo:delete");
        let token_scopes = sanitize_token_scopes(&scopes);
        assert!(!wants_push_from_token_scopes(&token_scopes));
    }

    #[test]
    fn wants_push_true_only_when_repository_push_present() {
        let scopes = parse_scopes(
            "repository:org/repo:pull repository:org/repo2:pull,push registry:catalog:*:push",
        );
        let token_scopes = sanitize_token_scopes(&scopes);

        assert!(wants_push_from_token_scopes(&token_scopes));
    }

    #[test]
    fn wants_push_false_for_repository_pull_only() {
        let scopes = parse_scopes("repository:org/repo:pull registry:catalog:*:push");
        let token_scopes = sanitize_token_scopes(&scopes);

        assert!(!wants_push_from_token_scopes(&token_scopes));
    }

    fn minimal_config_for_token_tests() -> Config {
        Config {
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 5000)),
            tls_cert_path: None,
            tls_key_path: None,
            tls_acme: None,
            push_auth_mode: crate::config::PushAuthMode::TokenOnly,
            push_username: None,
            push_password: None,
            push_allow_repos: Some(vec!["*".to_string()]),
            storage_backend: crate::config::StorageBackend::Filesystem,
            fs_root: PathBuf::from("./data"),
            s3_endpoint: None,
            s3_region: None,
            s3_bucket: None,
            s3_prefix: "registry".to_string(),
            ref_index: crate::config::RefIndexConfig {
                enabled: true,
                path: PathBuf::from("./data/ref-index"),
                rebuild_on_start: false,
                auto_rebuild_on_corruption: true,
            },
            allow_tag_overwrite: true,
            automatic_crossmount: false,
            upload_gc_enabled: true,
            upload_gc_interval_secs: 3600,
            upload_gc_max_age_secs: 86400,
            blob_gc_finalize_grace_secs: 72 * 3600,
            max_upload_bytes: 5 * 1024 * 1024 * 1024,
            max_request_body_bytes: 32 * 1024 * 1024,
            max_concurrent_buffered_requests: 8,
            max_concurrent_requests: 256,
            request_timeout_secs: 300,
            upload_request_timeout_secs: 3600,
            disallow_monolithic_uploads: false,
            upload_policy: UploadPolicyConfig {
                abort_on_error: false,
                abort_on_digest_mismatch: false,
                repo_rules: Vec::new(),
            },
            catalog_requires_auth: false,
            public_url: Some("http://127.0.0.1:5000".to_string()),
            token_service: "registry-rust".to_string(),
            token_signing_key: "test-key".to_string(),
            token_signing_keys: vec![security::TokenSigningKey {
                kid: "default".to_string(),
                key: "test-key".to_string(),
            }],
            token_ttl_secs: 600,
            robots: RobotsConfig::default(),
            users: crate::config::UsersConfig::default(),
            proxy: ProxyConfig {
                enabled: false,
                mode: ProxyMode::Allowlist,
                upstream_base_url: None,
                upstream_username: None,
                upstream_password: None,
                allowed_upstream_hosts: Vec::new(),
                allowed_repo_prefixes: Vec::new(),
                block_private_networks: true,
                redirect_policy: crate::config::RedirectPolicy::AnyPublic,
                max_concurrent_upstream: 16,
                index_path: PathBuf::from("./data/cache/proxy-index"),
                cache_fs_root: None,
                cache_s3_prefix: None,
                gc_interval_secs: 3600,
                scrub_enabled: false,
                scrub_interval_secs: 3600,
                scrub_max_files_per_run: 2000,
                max_cache_bytes: None,
                repo_rules: Vec::new(),
                upstreams: Vec::new(),
                routing_proxy_hosts: Vec::new(),
                routing_trust_x_forwarded_host: false,
            },
        }
    }

    #[test]
    fn token_primary_signing_key_follows_key_order() {
        let mut cfg = minimal_config_for_token_tests();

        cfg.token_signing_keys = vec![
            security::TokenSigningKey {
                kid: "k_new".to_string(),
                key: "new-key".to_string(),
            },
            security::TokenSigningKey {
                kid: "k_old".to_string(),
                key: "old-key".to_string(),
            },
        ];
        assert_eq!(cfg.token_primary_signing_key().kid, "k_new");

        cfg.token_signing_keys.swap(0, 1);
        assert_eq!(cfg.token_primary_signing_key().kid, "k_old");
    }

    #[test]
    fn robot_push_token_is_scoped_by_prefix_grants() {
        let mut cfg = minimal_config_for_token_tests();
        cfg.robots.enabled = true;

        let hash = robot_secrets::hash_robot_secret("s3cr3t").expect("hash");
        cfg.robots.accounts.push(crate::config::RobotAccountConfig {
            name: "ci".to_string(),
            secret_hash: hash,
            grants: vec![Grant {
                repo_prefix: "org/".to_string(),
                actions: vec!["pull".to_string(), "push".to_string()],
            }],
            max_ttl_secs: Some(120),
        });

        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "org/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let decision = decide_token_scopes_for_request(
            &cfg,
            &requested,
            Some(("ci".to_string(), "s3cr3t".to_string())),
        )
        .expect("should authorize");

        assert_eq!(decision.subject.as_deref(), Some("robot:ci"));
        assert_eq!(decision.scopes, requested);
        assert_eq!(decision.ttl_secs, 120);
    }

    #[test]
    fn robot_push_token_denied_when_repo_not_in_grants() {
        let mut cfg = minimal_config_for_token_tests();
        cfg.robots.enabled = true;

        let hash = robot_secrets::hash_robot_secret("s3cr3t").expect("hash");
        cfg.robots.accounts.push(crate::config::RobotAccountConfig {
            name: "ci".to_string(),
            secret_hash: hash,
            grants: vec![Grant {
                repo_prefix: "org/".to_string(),
                actions: vec!["pull".to_string(), "push".to_string()],
            }],
            max_ttl_secs: None,
        });

        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "other/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let err = decide_token_scopes_for_request(
            &cfg,
            &requested,
            Some(("ci".to_string(), "s3cr3t".to_string())),
        )
        .expect_err("should deny");

        assert_eq!(
            err,
            super::TokenRejection::Denied("push not allowed by robot policy")
        );
    }

    #[test]
    fn robot_auth_failure_does_not_allow_push_without_legacy_creds() {
        let mut cfg = minimal_config_for_token_tests();
        cfg.robots.enabled = true;

        let hash = robot_secrets::hash_robot_secret("s3cr3t").expect("hash");
        cfg.robots.accounts.push(crate::config::RobotAccountConfig {
            name: "ci".to_string(),
            secret_hash: hash,
            grants: vec![Grant {
                repo_prefix: "org/".to_string(),
                actions: vec!["pull".to_string(), "push".to_string()],
            }],
            max_ttl_secs: None,
        });

        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "org/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let err = decide_token_scopes_for_request(
            &cfg,
            &requested,
            Some(("ci".to_string(), "wrong".to_string())),
        )
        .expect_err("should deny");

        assert_eq!(err, super::TokenRejection::Unauthorized);
    }

    #[test]
    fn legacy_push_creds_work_even_when_robots_enabled() {
        let mut cfg = minimal_config_for_token_tests();
        cfg.robots.enabled = true;
        cfg.push_username = Some("admin".to_string());
        cfg.push_password = Some("pw".to_string());

        // Add a robot too; we should still allow legacy when legacy creds match.
        let hash = robot_secrets::hash_robot_secret("s3cr3t").expect("hash");
        cfg.robots.accounts.push(crate::config::RobotAccountConfig {
            name: "ci".to_string(),
            secret_hash: hash,
            grants: vec![Grant {
                repo_prefix: "org/".to_string(),
                actions: vec!["pull".to_string(), "push".to_string()],
            }],
            max_ttl_secs: None,
        });

        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "org/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let decision = decide_token_scopes_for_request(
            &cfg,
            &requested,
            Some(("admin".to_string(), "pw".to_string())),
        )
        .expect("should authorize");

        assert_eq!(decision.subject.as_deref(), Some("admin"));
        assert_eq!(decision.scopes, requested);
    }

    #[test]
    fn user_push_token_is_scoped_by_group_grants() {
        let mut cfg = minimal_config_for_token_tests();
        cfg.users.enabled = true;

        cfg.users.groups.push(crate::config::GroupConfig {
            name: "dev".to_string(),
            grants: vec![Grant {
                repo_prefix: "org/".to_string(),
                actions: vec!["pull".to_string(), "push".to_string()],
            }],
        });

        let hash = robot_secrets::hash_robot_secret("pw").expect("hash");
        cfg.users.accounts.push(crate::config::UserAccountConfig {
            name: "alice".to_string(),
            secret_hash: hash,
            groups: vec!["dev".to_string()],
            max_ttl_secs: Some(120),
        });

        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "org/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let decision = decide_token_scopes_for_request(
            &cfg,
            &requested,
            Some(("alice".to_string(), "pw".to_string())),
        )
        .expect("should authorize");

        assert_eq!(decision.subject.as_deref(), Some("user:alice"));
        assert_eq!(decision.scopes, requested);
        assert_eq!(decision.ttl_secs, 120);
    }

    #[test]
    fn user_push_token_denied_when_repo_not_in_group_grants() {
        let mut cfg = minimal_config_for_token_tests();
        cfg.users.enabled = true;

        cfg.users.groups.push(crate::config::GroupConfig {
            name: "dev".to_string(),
            grants: vec![Grant {
                repo_prefix: "other/".to_string(),
                actions: vec!["pull".to_string(), "push".to_string()],
            }],
        });

        let hash = robot_secrets::hash_robot_secret("pw").expect("hash");
        cfg.users.accounts.push(crate::config::UserAccountConfig {
            name: "bob".to_string(),
            secret_hash: hash,
            groups: vec!["dev".to_string()],
            max_ttl_secs: None,
        });

        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "org/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let err = decide_token_scopes_for_request(
            &cfg,
            &requested,
            Some(("bob".to_string(), "pw".to_string())),
        )
        .expect_err("should deny");

        assert_eq!(
            err,
            super::TokenRejection::Denied("push not allowed by user policy")
        );
    }

    #[test]
    fn robot_precedence_on_name_collision_denies_even_if_user_would_allow() {
        let mut cfg = minimal_config_for_token_tests();
        cfg.robots.enabled = true;
        cfg.users.enabled = true;

        // Robot has the colliding name and valid creds but does NOT allow this repo.
        let shared_hash = robot_secrets::hash_robot_secret("pw").expect("hash");
        cfg.robots.accounts.push(crate::config::RobotAccountConfig {
            name: "sam".to_string(),
            secret_hash: shared_hash.clone(),
            grants: vec![Grant {
                repo_prefix: "org/".to_string(),
                actions: vec!["pull".to_string(), "push".to_string()],
            }],
            max_ttl_secs: None,
        });

        // User would allow it via group grants, but must not be reached.
        cfg.users.groups.push(crate::config::GroupConfig {
            name: "writers".to_string(),
            grants: vec![Grant {
                repo_prefix: "other/".to_string(),
                actions: vec!["pull".to_string(), "push".to_string()],
            }],
        });
        cfg.users.accounts.push(crate::config::UserAccountConfig {
            name: "sam".to_string(),
            secret_hash: shared_hash,
            groups: vec!["writers".to_string()],
            max_ttl_secs: None,
        });

        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "other/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let err = decide_token_scopes_for_request(
            &cfg,
            &requested,
            Some(("sam".to_string(), "pw".to_string())),
        )
        .expect_err("should deny");

        assert_eq!(
            err,
            super::TokenRejection::Denied("push not allowed by robot policy")
        );
    }
}

fn detect_media_type_from_manifest(bytes: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    value
        .get("mediaType")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn is_supported_manifest_media_type(media_type: &str) -> bool {
    matches!(
        media_type,
        "application/vnd.oci.image.manifest.v1+json"
            | "application/vnd.oci.artifact.manifest.v1+json"
            | "application/vnd.oci.image.index.v1+json"
            | "application/vnd.docker.distribution.manifest.v2+json"
            | "application/vnd.docker.distribution.manifest.list.v2+json"
    )
}

async fn ensure_tag_fresh(
    state: &AppState,
    proxy: &crate::proxy::Proxy,
    decision: &crate::proxy::RepoDecision,
    storage: &Arc<dyn crate::storage::Storage>,
    tag: &str,
    ttl_secs: u64,
    always_revalidate: bool,
) -> Result<(), Response> {
    let now = crate::proxy::Proxy::now_unix();
    let current_digest = storage.resolve_tag(&decision.local_repo, tag).await.ok();
    let meta = proxy.get_tag_meta(&decision.local_repo, tag);

    if !always_revalidate {
        if let (Some(d), Some(m)) = (&current_digest, &meta) {
            if m.expires_at_unix > now && m.digest == d.as_str() {
                return Ok(());
            }
        }
    }

    // Revalidate via HEAD (conditional if we have an ETag).
    let if_none_match = meta.as_ref().and_then(|m| m.etag.clone());
    let head = proxy
        .fetch_manifest_and_cache(
            decision,
            tag,
            storage,
            state.config.max_request_body_bytes,
            true,
            if_none_match,
        )
        .await;

    match head {
        Ok(crate::proxy::FetchManifestResult::NotModified { etag, digest }) => {
            // If we don't have the manifest locally (or no tag pointer), fetch the body.
            if current_digest.is_none()
                || storage
                    .head_manifest(&decision.local_repo, current_digest.as_ref().unwrap())
                    .await
                    .is_err()
            {
                let _permit = state
                    .buffered_body_sem
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| errors::internal_error().into_response())?;
                let _ = proxy
                    .fetch_manifest_and_cache(
                        decision,
                        tag,
                        storage,
                        state.config.max_request_body_bytes,
                        false,
                        None,
                    )
                    .await
                    .map_err(|e| {
                        tracing::warn!(error = %e, repo = decision.local_repo, tag, "proxy: fetch manifest after 304 failed");
                        errors::internal_error().into_response()
                    })?;
            }

            if let Some(d) = storage
                .resolve_tag(&decision.local_repo, tag)
                .await
                .ok()
                .or(digest)
            {
                let m = crate::proxy::TagMeta {
                    digest: d.as_str(),
                    expires_at_unix: if always_revalidate {
                        now
                    } else {
                        crate::proxy::Proxy::ttl_expires_at(ttl_secs)
                    },
                    etag,
                };
                proxy.put_tag_meta(&decision.local_repo, tag, &m);
            }
            Ok(())
        }
        Ok(crate::proxy::FetchManifestResult::HeadOk { etag, digest, .. }) => {
            let needs_get = match (&current_digest, &digest) {
                (Some(local), Some(up)) if local.hex() == up.hex() => storage
                    .head_manifest(&decision.local_repo, local)
                    .await
                    .is_err(),
                _ => true,
            };

            if needs_get {
                let _permit = state
                    .buffered_body_sem
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| errors::internal_error().into_response())?;
                let fetched = proxy
                    .fetch_manifest_and_cache(
                        decision,
                        tag,
                        storage,
                        state.config.max_request_body_bytes,
                        false,
                        None,
                    )
                    .await
                    .map_err(|e| {
                        tracing::warn!(error = %e, repo = decision.local_repo, tag, "proxy: fetch manifest failed");
                        errors::internal_error().into_response()
                    })?;
                if let crate::proxy::FetchManifestResult::Fetched { digest, etag, .. } = fetched {
                    let m = crate::proxy::TagMeta {
                        digest: digest.as_str(),
                        expires_at_unix: if always_revalidate {
                            now
                        } else {
                            crate::proxy::Proxy::ttl_expires_at(ttl_secs)
                        },
                        etag,
                    };
                    proxy.put_tag_meta(&decision.local_repo, tag, &m);
                }
            } else if let Some(d) = current_digest {
                let m = crate::proxy::TagMeta {
                    digest: d.as_str(),
                    expires_at_unix: if always_revalidate {
                        now
                    } else {
                        crate::proxy::Proxy::ttl_expires_at(ttl_secs)
                    },
                    etag,
                };
                proxy.put_tag_meta(&decision.local_repo, tag, &m);
            }
            Ok(())
        }
        Ok(crate::proxy::FetchManifestResult::Fetched { digest, etag, .. }) => {
            let m = crate::proxy::TagMeta {
                digest: digest.as_str(),
                expires_at_unix: if always_revalidate {
                    now
                } else {
                    crate::proxy::Proxy::ttl_expires_at(ttl_secs)
                },
                etag,
            };
            proxy.put_tag_meta(&decision.local_repo, tag, &m);
            Ok(())
        }
        Err(crate::proxy::ProxyError::NotFound) => Err(errors::manifest_unknown().into_response()),
        Err(crate::proxy::ProxyError::TooLarge) => Err(errors::payload_too_large().into_response()),
        Err(err) => {
            tracing::warn!(error = %err, repo = decision.local_repo, tag, "proxy: revalidate failed");
            Err(errors::internal_error().into_response())
        }
    }
}

async fn manifest_put(
    state: AppState,
    headers: &HeaderMap,
    name: &str,
    reference: &str,
    body: Body,
) -> Response {
    // This endpoint buffers the full manifest into memory for hashing and validation.
    // Limit concurrency so memory usage stays bounded under load.
    let _permit = match state.buffered_body_sem.clone().acquire_owned().await {
        Ok(p) => p,
        Err(_) => return errors::internal_error().into_response(),
    };

    let content_length = headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<usize>().ok());

    let bytes =
        match read_body_limited(body, content_length, state.config.max_request_body_bytes).await {
            Ok(b) => b,
            Err(resp) => return resp,
        };

    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }
    if bytes.is_empty() {
        return errors::manifest_invalid().into_response();
    }

    let media_type = detect_media_type_from_manifest(&bytes)
        .unwrap_or_else(|| "application/vnd.oci.image.manifest.v1+json".to_string());
    if !is_supported_manifest_media_type(&media_type) {
        return errors::not_implemented().into_response();
    }

    // Best-effort: pre-parse referrer info. We only persist it if the manifest is accepted.
    let referrer_info = parse_referrer_info(&bytes);
    let subject_for_headers = referrer_info.as_ref().map(|(s, _, _)| s.clone());

    // Compute manifest digest over the raw bytes.
    let mut hasher = sha2::Sha256::new();
    hasher.update(&bytes);
    let digest_hex = hex::encode(hasher.finalize());
    let computed =
        Digest::parse(&format!("sha256:{digest_hex}")).expect("computed sha256 is valid");

    // If reference is a digest, it must match the computed digest.
    if let Ok(ref_digest) = Digest::parse(reference) {
        if ref_digest.hex() != computed.hex() {
            return errors::digest_invalid().into_response();
        }
    } else {
        // Otherwise treat it as a tag.
        if !is_valid_tag(reference) {
            return errors::tag_invalid().into_response();
        }
        if !state.config.allow_tag_overwrite {
            match state.storage.resolve_tag(name, reference).await {
                Ok(_) => {
                    return (StatusCode::CONFLICT, registry_headers(), Body::empty())
                        .into_response();
                }
                Err(StorageError::NotFound) => {}
                Err(StorageError::Unsupported) => return errors::not_implemented().into_response(),
                Err(StorageError::InsufficientStorage) => {
                    return errors::insufficient_storage().into_response();
                }
                Err(StorageError::TooLarge) => return errors::internal_error().into_response(),
                Err(StorageError::DigestMismatch) | Err(StorageError::Internal(_)) => {
                    return errors::internal_error().into_response();
                }
            }
        }
    }

    let meta = match state.storage.put_manifest(name, &computed, bytes).await {
        Ok(m) => m,
        Err(StorageError::Unsupported) => return errors::not_implemented().into_response(),
        Err(StorageError::NotFound) => return errors::internal_error().into_response(),
        Err(StorageError::DigestMismatch) => return errors::digest_invalid().into_response(),
        Err(StorageError::TooLarge) => return errors::internal_error().into_response(),
        Err(StorageError::InsufficientStorage) => {
            return errors::insufficient_storage().into_response();
        }
        Err(StorageError::Internal(_)) => return errors::internal_error().into_response(),
    };

    // If reference is a tag, update tag pointer.
    if Digest::parse(reference).is_err() {
        // Best-effort: capture old tag root for ref-index count updates.
        let old_root_for_index = if state.ref_index.is_some() {
            match state.storage.resolve_tag(name, reference).await {
                Ok(d) => Some(d),
                Err(StorageError::NotFound) => None,
                Err(_) => None,
            }
        } else {
            None
        };

        if let Err(err) = state.storage.set_tag(name, reference, &computed).await {
            return match err {
                StorageError::Unsupported => errors::not_implemented().into_response(),
                StorageError::NotFound => errors::internal_error().into_response(),
                StorageError::DigestMismatch => errors::internal_error().into_response(),
                StorageError::TooLarge => errors::internal_error().into_response(),
                StorageError::InsufficientStorage => errors::insufficient_storage().into_response(),
                StorageError::Internal(_) => errors::internal_error().into_response(),
            };
        }

        // Keep the ref-index up to date (best-effort). Errors here should not fail the push.
        if let Some(idx) = state.ref_index.as_ref() {
            if let Err(err) = idx
                .on_tag_set(&state.storage, name, reference, &computed, old_root_for_index)
                .await
            {
                tracing::warn!(
                    error = %err,
                    repo = name,
                    tag = reference,
                    digest = computed.as_str(),
                    "ref-index: failed to update on tag set"
                );
            }
        }
    }

    // If this manifest declares a `subject`, index it for the referrers API.
    // Errors here should not fail the manifest push.
    if let Some((subject, artifact_type, annotations)) = referrer_info {
        let descriptor = ReferrerDescriptor {
            media_type: meta.media_type.clone(),
            digest: computed.as_str(),
            size: meta.size,
            artifact_type,
            annotations,
        };
        let _ = state.storage.add_referrer(name, &subject, descriptor).await;
    }

    let mut headers = registry_headers();
    headers.insert("Docker-Content-Digest", computed.as_str().parse().unwrap());
    headers.insert("Content-Type", meta.media_type.parse().unwrap());
    headers.insert(
        "Location",
        format!("/v2/{name}/manifests/{}", reference)
            .parse()
            .unwrap(),
    );
    if let Some(subject) = subject_for_headers {
        headers.insert("OCI-Subject", subject.as_str().parse().unwrap());
    }
    (StatusCode::CREATED, headers).into_response()
}

fn parse_referrer_info(
    manifest_bytes: &[u8],
) -> Option<(Digest, Option<String>, Option<HashMap<String, String>>)> {
    let v: serde_json::Value = serde_json::from_slice(manifest_bytes).ok()?;
    let subject_digest = v
        .get("subject")
        .and_then(|s| s.get("digest"))
        .and_then(|d| d.as_str())?;
    let subject = Digest::parse(subject_digest).ok()?;

    let artifact_type = v
        .get("artifactType")
        .and_then(|a| a.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            v.get("config")
                .and_then(|c| c.get("mediaType"))
                .and_then(|m| m.as_str())
                .map(|s| s.to_string())
        });

    let annotations = v
        .get("annotations")
        .and_then(|a| a.as_object())
        .and_then(|obj| {
            let map = obj
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect::<HashMap<_, _>>();
            if map.is_empty() { None } else { Some(map) }
        });

    Some((subject, artifact_type, annotations))
}

async fn referrers_list(
    state: AppState,
    method: Method,
    name: &str,
    digest_str: &str,
    query: &HashMap<String, String>,
    route_mode: V2RouteMode,
    proxy_ctx: Option<ProxyContext>,
) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }

    let subject = match Digest::parse(digest_str) {
        Ok(d) => d,
        Err(_) => return errors::digest_invalid().into_response(),
    };

    match method {
        Method::GET | Method::HEAD => {
            let storage = match route_mode {
                V2RouteMode::Default => state.storage.clone(),
                V2RouteMode::ProxyOnly => match proxy_ctx.as_ref() {
                    Some(ctx) => ctx.cache.clone(),
                    None => return errors::internal_error().into_response(),
                },
            };

            let mut entries = match storage.list_referrers(name, &subject).await {
                Ok(v) => v,
                Err(StorageError::Unsupported) => return errors::not_implemented().into_response(),
                Err(StorageError::Internal(_)) => return errors::internal_error().into_response(),
                Err(StorageError::TooLarge) => return errors::internal_error().into_response(),
                Err(StorageError::DigestMismatch) => {
                    return errors::internal_error().into_response();
                }
                Err(StorageError::NotFound) => Vec::new(),
                Err(StorageError::InsufficientStorage) => {
                    return errors::insufficient_storage().into_response();
                }
            };

            let artifact_type_filter = query.get("artifactType").map(|s| s.as_str());
            if let Some(filter) = artifact_type_filter {
                entries.retain(|d| d.artifact_type.as_deref() == Some(filter));
            }

            // Return OCI index.
            let manifests = entries
                .into_iter()
                .map(|d| {
                    let mut obj = serde_json::json!({
                        "mediaType": d.media_type,
                        "digest": d.digest,
                        "size": d.size,
                    });
                    if let Some(at) = d.artifact_type {
                        obj["artifactType"] = serde_json::Value::String(at);
                    }
                    if let Some(ann) = d.annotations {
                        obj["annotations"] =
                            serde_json::to_value(ann).unwrap_or(serde_json::Value::Null);
                    }
                    obj
                })
                .collect::<Vec<_>>();

            let payload = serde_json::json!({
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.index.v1+json",
                "manifests": manifests,
            });

            let bytes = match serde_json::to_vec(&payload) {
                Ok(b) => b,
                Err(_) => return errors::internal_error().into_response(),
            };
            let mut headers = registry_headers();
            headers.insert(
                "Content-Type",
                "application/vnd.oci.image.index.v1+json".parse().unwrap(),
            );
            headers.insert("Content-Length", bytes.len().to_string().parse().unwrap());
            if artifact_type_filter.is_some() {
                headers.insert("OCI-Filters-Applied", "artifactType".parse().unwrap());
            }

            if method == Method::HEAD {
                return (StatusCode::OK, headers).into_response();
            }
            (StatusCode::OK, headers, Body::from(bytes)).into_response()
        }
        _ => errors::not_implemented().into_response(),
    }
}

fn registry_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Docker-Distribution-API-Version",
        "registry/2.0".parse().unwrap(),
    );
    headers
}

async fn read_body_limited(
    body: Body,
    content_length: Option<usize>,
    limit: usize,
) -> Result<Bytes, Response> {
    if let Some(len) = content_length {
        if len > limit {
            return Err(errors::payload_too_large().into_response());
        }
    }

    let initial_capacity = content_length.unwrap_or(0).min(limit).min(1024 * 1024);
    let mut buf: Vec<u8> = Vec::with_capacity(initial_capacity);
    let mut stream = body.into_data_stream();
    while let Some(next) = stream.next().await {
        let chunk = match next {
            Ok(c) => c,
            Err(_) => return Err(errors::internal_error().into_response()),
        };
        if chunk.is_empty() {
            continue;
        }
        if buf.len().saturating_add(chunk.len()) > limit {
            return Err(errors::payload_too_large().into_response());
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(buf))
}

async fn upload_create(
    state: AppState,
    method: Method,
    name: &str,
    query: &HashMap<String, String>,
    body: Body,
) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }

    if method != Method::POST {
        return errors::not_implemented().into_response();
    }

    // Cross-repository blob mount:
    //   POST /v2/<name>/blobs/uploads/?mount=<digest>[&from=<repo>]
    // If the blob exists, registry may respond 201 and skip upload.
    if let Some(mount_str) = query.get("mount").map(|s| s.as_str()) {
        let digest = match Digest::parse(mount_str) {
            Ok(d) => d,
            Err(_) => return errors::digest_invalid().into_response(),
        };

        let has_from = query.get("from").is_some();
        let allow_without_from = state.config.automatic_crossmount;

        if has_from || allow_without_from {
            if state.storage.head_blob(&digest).await.is_ok() {
                let mut headers = registry_headers();
                headers.insert(
                    "Location",
                    format!("/v2/{name}/blobs/{}", digest.as_str())
                        .parse()
                        .unwrap(),
                );
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert("Content-Length", "0".parse().unwrap());
                return (StatusCode::CREATED, headers).into_response();
            }
        }
    }

    // Monolithic upload (body on POST): POST /v2/<name>/blobs/uploads/?digest=<digest>
    // Conformance allows this to either create an upload session (202) or create the blob (201).
    // If the blob already exists, we return 201.
    if let Some(digest_str) = query.get("digest").map(|s| s.as_str()) {
        let digest = match Digest::parse(digest_str) {
            Ok(d) => d,
            Err(_) => return errors::digest_invalid().into_response(),
        };

        let policy = state.config.resolved_upload_policy_for_repo(name);

        if state.storage.head_blob(&digest).await.is_ok() {
            let mut headers = registry_headers();
            headers.insert(
                "Location",
                format!("/v2/{name}/blobs/{}", digest.as_str())
                    .parse()
                    .unwrap(),
            );
            headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
            headers.insert("Content-Length", "0".parse().unwrap());
            return (StatusCode::CREATED, headers).into_response();
        }

        // Stream monolithic upload (no buffering of multi-GB body).
        let mut stream = body.into_data_stream();
        let first = stream.next().await;
        let Some(first) = first else {
            // No body -> behave like normal upload creation.
            match state.storage.create_upload().await {
                Ok(meta) => {
                    let mut headers = registry_headers();
                    let location = format!("/v2/{name}/blobs/uploads/{}", meta.uuid);
                    headers.insert("Location", location.parse().unwrap());
                    headers.insert("Docker-Upload-UUID", meta.uuid.parse().unwrap());
                    return (StatusCode::ACCEPTED, headers).into_response();
                }
                Err(StorageError::Unsupported) => return errors::not_implemented().into_response(),
                Err(StorageError::InsufficientStorage) => {
                    return errors::insufficient_storage().into_response();
                }
                Err(_) => return errors::internal_error().into_response(),
            }
        };

        // We saw a request body on POST ?digest => this is a monolithic upload.
        // Some clients do this; operators might want to forbid it for robustness.
        tracing::warn!(
            repo = name,
            "monolithic blob upload detected (POST ?digest with body)"
        );
        if state.config.disallow_monolithic_uploads {
            return errors::blob_upload_invalid(
                "monolithic uploads are disabled; use PATCH-based chunked upload",
            )
            .into_response();
        }

        let meta = match state.storage.create_upload().await {
            Ok(meta) => meta,
            Err(StorageError::Unsupported) => return errors::not_implemented().into_response(),
            Err(StorageError::InsufficientStorage) => {
                return errors::insufficient_storage().into_response();
            }
            Err(StorageError::Internal(msg)) => {
                tracing::error!(storage = state.storage.kind(), error = %msg, repo = name, "create_upload failed");
                return errors::internal_error().into_response();
            }
            Err(_) => return errors::internal_error().into_response(),
        };

        let first_chunk = match first {
            Ok(c) => c,
            Err(_) => {
                if policy.abort_on_error {
                    let _ = state.storage.abort_upload(&meta.uuid).await;
                }
                return errors::internal_error().into_response();
            }
        };

        if !first_chunk.is_empty() {
            let chunk_len = first_chunk.len();
            if let Err(err) = state.storage.append_upload(&meta.uuid, first_chunk).await {
                tracing::warn!(storage = state.storage.kind(), repo = name, uuid = %meta.uuid, chunk_len, "append_upload failed");
                if policy.abort_on_error {
                    let _ = state.storage.abort_upload(&meta.uuid).await;
                }
                return match err {
                    StorageError::NotFound => errors::blob_upload_unknown().into_response(),
                    StorageError::TooLarge => {
                        errors::blob_upload_invalid("upload too large").into_response()
                    }
                    StorageError::InsufficientStorage => {
                        errors::insufficient_storage().into_response()
                    }
                    StorageError::Unsupported => errors::not_implemented().into_response(),
                    StorageError::Internal(_) | StorageError::DigestMismatch => {
                        errors::internal_error().into_response()
                    }
                };
            }
        }

        while let Some(next) = stream.next().await {
            let chunk = match next {
                Ok(c) => c,
                Err(_) => {
                    if policy.abort_on_error {
                        let _ = state.storage.abort_upload(&meta.uuid).await;
                    }
                    return errors::internal_error().into_response();
                }
            };
            if chunk.is_empty() {
                continue;
            }
            let chunk_len = chunk.len();
            if let Err(err) = state.storage.append_upload(&meta.uuid, chunk).await {
                tracing::warn!(storage = state.storage.kind(), repo = name, uuid = %meta.uuid, chunk_len, "append_upload failed");
                if policy.abort_on_error {
                    let _ = state.storage.abort_upload(&meta.uuid).await;
                }
                return match err {
                    StorageError::NotFound => errors::blob_upload_unknown().into_response(),
                    StorageError::TooLarge => {
                        errors::blob_upload_invalid("upload too large").into_response()
                    }
                    StorageError::InsufficientStorage => {
                        errors::insufficient_storage().into_response()
                    }
                    StorageError::Unsupported => errors::not_implemented().into_response(),
                    StorageError::Internal(_) | StorageError::DigestMismatch => {
                        errors::internal_error().into_response()
                    }
                };
            }
        }

        return match state.storage.finalize_upload(&meta.uuid, &digest).await {
            Ok(final_meta) => {
                if let Some(idx) = state.ref_index.as_ref() {
                    let grace = std::time::Duration::from_secs(state.config.blob_gc_finalize_grace_secs);
                    if let Some(until) = std::time::SystemTime::now().checked_add(grace) {
                        if let Err(err) = idx.pin_blob(&digest, until, "finalize_upload") {
                            tracing::warn!(
                                error = %err,
                                digest = %digest.as_str(),
                                grace_secs = state.config.blob_gc_finalize_grace_secs,
                                "ref-index: failed to pin blob on finalize"
                            );
                        }
                    }
                }

                let mut headers = registry_headers();
                headers.insert(
                    "Location",
                    format!("/v2/{name}/blobs/{}", digest.as_str())
                        .parse()
                        .unwrap(),
                );
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert(
                    "Content-Length",
                    final_meta.size.to_string().parse().unwrap(),
                );
                (StatusCode::CREATED, headers).into_response()
            }
            Err(StorageError::NotFound) => errors::blob_upload_unknown().into_response(),
            Err(StorageError::DigestMismatch) => {
                if policy.abort_on_digest_mismatch {
                    let _ = state.storage.abort_upload(&meta.uuid).await;
                }
                errors::digest_invalid().into_response()
            }
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
            Err(StorageError::TooLarge) => errors::internal_error().into_response(),
            Err(StorageError::InsufficientStorage) => {
                errors::insufficient_storage().into_response()
            }
            Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
        };
    }

    match state.storage.create_upload().await {
        Ok(meta) => {
            let mut headers = registry_headers();
            let location = format!("/v2/{name}/blobs/uploads/{}", meta.uuid);
            headers.insert("Location", location.parse().unwrap());
            headers.insert("Docker-Upload-UUID", meta.uuid.parse().unwrap());
            (StatusCode::ACCEPTED, headers).into_response()
        }
        Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
        Err(StorageError::Internal(msg)) => {
            tracing::error!(
                storage = state.storage.kind(),
                error = %msg,
                repo = name,
                "create_upload failed"
            );
            errors::internal_error().into_response()
        }
        Err(StorageError::DigestMismatch) | Err(StorageError::NotFound) => {
            tracing::error!(
                storage = state.storage.kind(),
                repo = name,
                "create_upload failed"
            );
            errors::internal_error().into_response()
        }
        Err(StorageError::TooLarge) => errors::internal_error().into_response(),
        Err(StorageError::InsufficientStorage) => errors::insufficient_storage().into_response(),
    }
}

async fn upload_session(
    state: AppState,
    method: Method,
    req_headers: &HeaderMap,
    name: &str,
    uuid: &str,
    query: HashMap<String, String>,
    body: Body,
) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }

    let location = format!("/v2/{name}/blobs/uploads/{uuid}");

    let policy = state.config.resolved_upload_policy_for_repo(name);

    match method {
        Method::GET => match state.storage.upload_status(uuid).await {
            Ok(meta) => {
                let mut headers = registry_headers();
                headers.insert("Location", location.parse().unwrap());
                headers.insert("Docker-Upload-UUID", meta.uuid.parse().unwrap());
                if meta.offset > 0 {
                    headers.insert("Range", format!("0-{}", meta.offset - 1).parse().unwrap());
                }
                (StatusCode::NO_CONTENT, headers).into_response()
            }
            Err(StorageError::NotFound) => errors::blob_upload_unknown().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
            Err(StorageError::TooLarge) => errors::internal_error().into_response(),
            Err(StorageError::InsufficientStorage) => {
                errors::insufficient_storage().into_response()
            }
            Err(StorageError::Internal(_)) | Err(StorageError::DigestMismatch) => {
                errors::internal_error().into_response()
            }
        },
        Method::PATCH => {
            // Enforce that Content-Range starts at the current offset.
            if let Some((start, _end)) = parse_content_range(req_headers) {
                match state.storage.upload_status(uuid).await {
                    Ok(meta) if meta.offset == start => {}
                    Ok(_) => {
                        return (StatusCode::RANGE_NOT_SATISFIABLE, registry_headers())
                            .into_response();
                    }
                    Err(StorageError::NotFound) => {
                        return errors::blob_upload_unknown().into_response();
                    }
                    Err(_) => return errors::internal_error().into_response(),
                }
            }

            let mut stream = body.into_data_stream();
            let mut last_meta = match state.storage.upload_status(uuid).await {
                Ok(m) => m,
                Err(StorageError::NotFound) => {
                    return errors::blob_upload_unknown().into_response();
                }
                Err(StorageError::Unsupported) => return errors::not_implemented().into_response(),
                Err(StorageError::InsufficientStorage) => {
                    return errors::insufficient_storage().into_response();
                }
                Err(_) => return errors::internal_error().into_response(),
            };

            while let Some(next) = stream.next().await {
                let chunk = match next {
                    Ok(c) => c,
                    Err(_) => {
                        if policy.abort_on_error {
                            let _ = state.storage.abort_upload(uuid).await;
                        }
                        return errors::internal_error().into_response();
                    }
                };
                if chunk.is_empty() {
                    continue;
                }
                last_meta = match state.storage.append_upload(uuid, chunk).await {
                    Ok(m) => m,
                    Err(StorageError::NotFound) => {
                        return errors::blob_upload_unknown().into_response();
                    }
                    Err(StorageError::TooLarge) => {
                        if policy.abort_on_error {
                            let _ = state.storage.abort_upload(uuid).await;
                        }
                        return errors::blob_upload_invalid("upload too large").into_response();
                    }
                    Err(StorageError::InsufficientStorage) => {
                        if policy.abort_on_error {
                            let _ = state.storage.abort_upload(uuid).await;
                        }
                        return errors::insufficient_storage().into_response();
                    }
                    Err(StorageError::Unsupported) => {
                        return errors::not_implemented().into_response();
                    }
                    Err(StorageError::Internal(_)) | Err(StorageError::DigestMismatch) => {
                        if policy.abort_on_error {
                            let _ = state.storage.abort_upload(uuid).await;
                        }
                        return errors::internal_error().into_response();
                    }
                };
            }

            let mut headers = registry_headers();
            headers.insert("Location", location.parse().unwrap());
            headers.insert("Docker-Upload-UUID", last_meta.uuid.parse().unwrap());
            if last_meta.offset > 0 {
                headers.insert(
                    "Range",
                    format!("0-{}", last_meta.offset - 1).parse().unwrap(),
                );
            }
            (StatusCode::ACCEPTED, headers).into_response()
        }
        Method::PUT => {
            let Some(digest_str) = query.get("digest").map(|s| s.as_str()) else {
                return errors::digest_invalid().into_response();
            };
            let digest = match Digest::parse(digest_str) {
                Ok(d) => d,
                Err(_) => return errors::digest_invalid().into_response(),
            };

            // Optional Content-Range enforcement.
            if let Some((start, _end)) = parse_content_range(req_headers) {
                match state.storage.upload_status(uuid).await {
                    Ok(meta) if meta.offset == start => {}
                    Ok(_) => {
                        return (StatusCode::RANGE_NOT_SATISFIABLE, registry_headers())
                            .into_response();
                    }
                    Err(StorageError::NotFound) => {
                        return errors::blob_upload_unknown().into_response();
                    }
                    Err(_) => return errors::internal_error().into_response(),
                }
            }

            // Stream any body bytes into the upload (some clients do monolithic finalize-on-PUT).
            let mut stream = body.into_data_stream();
            let first = stream.next().await;
            if let Some(first) = first {
                tracing::warn!(
                    repo = name,
                    uuid = uuid,
                    "monolithic upload detected (PUT finalize with body)"
                );
                if state.config.disallow_monolithic_uploads {
                    return errors::blob_upload_invalid(
                        "monolithic uploads are disabled; use PATCH-based chunked upload",
                    )
                    .into_response();
                }

                let first_chunk = match first {
                    Ok(c) => c,
                    Err(_) => {
                        if policy.abort_on_error {
                            let _ = state.storage.abort_upload(uuid).await;
                        }
                        return errors::internal_error().into_response();
                    }
                };
                if !first_chunk.is_empty() {
                    match state.storage.append_upload(uuid, first_chunk).await {
                        Ok(_) => {}
                        Err(StorageError::NotFound) => {
                            return errors::blob_upload_unknown().into_response();
                        }
                        Err(StorageError::TooLarge) => {
                            if policy.abort_on_error {
                                let _ = state.storage.abort_upload(uuid).await;
                            }
                            return errors::blob_upload_invalid("upload too large").into_response();
                        }
                        Err(StorageError::InsufficientStorage) => {
                            if policy.abort_on_error {
                                let _ = state.storage.abort_upload(uuid).await;
                            }
                            return errors::insufficient_storage().into_response();
                        }
                        Err(StorageError::Unsupported) => {
                            return errors::not_implemented().into_response();
                        }
                        Err(StorageError::Internal(_)) | Err(StorageError::DigestMismatch) => {
                            if policy.abort_on_error {
                                let _ = state.storage.abort_upload(uuid).await;
                            }
                            return errors::internal_error().into_response();
                        }
                    }
                }

                while let Some(next) = stream.next().await {
                    let chunk = match next {
                        Ok(c) => c,
                        Err(_) => {
                            if policy.abort_on_error {
                                let _ = state.storage.abort_upload(uuid).await;
                            }
                            return errors::internal_error().into_response();
                        }
                    };
                    if chunk.is_empty() {
                        continue;
                    }
                    match state.storage.append_upload(uuid, chunk).await {
                        Ok(_) => {}
                        Err(StorageError::NotFound) => {
                            return errors::blob_upload_unknown().into_response();
                        }
                        Err(StorageError::TooLarge) => {
                            if policy.abort_on_error {
                                let _ = state.storage.abort_upload(uuid).await;
                            }
                            return errors::blob_upload_invalid("upload too large").into_response();
                        }
                        Err(StorageError::InsufficientStorage) => {
                            if policy.abort_on_error {
                                let _ = state.storage.abort_upload(uuid).await;
                            }
                            return errors::insufficient_storage().into_response();
                        }
                        Err(StorageError::Unsupported) => {
                            return errors::not_implemented().into_response();
                        }
                        Err(StorageError::Internal(_)) | Err(StorageError::DigestMismatch) => {
                            if policy.abort_on_error {
                                let _ = state.storage.abort_upload(uuid).await;
                            }
                            return errors::internal_error().into_response();
                        }
                    }
                }
            }

            match state.storage.finalize_upload(uuid, &digest).await {
                Ok(meta) => {
                    let mut headers = registry_headers();
                    headers.insert(
                        "Location",
                        format!("/v2/{name}/blobs/{}", digest.as_str())
                            .parse()
                            .unwrap(),
                    );
                    headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                    headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                    (StatusCode::CREATED, headers).into_response()
                }
                Err(StorageError::NotFound) => errors::blob_upload_unknown().into_response(),
                Err(StorageError::DigestMismatch) => {
                    if policy.abort_on_digest_mismatch {
                        let _ = state.storage.abort_upload(uuid).await;
                    }
                    errors::digest_invalid().into_response()
                }
                Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
                Err(StorageError::TooLarge) => errors::internal_error().into_response(),
                Err(StorageError::InsufficientStorage) => {
                    errors::insufficient_storage().into_response()
                }
                Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
            }
        }
        _ => errors::not_implemented().into_response(),
    }
}

fn parse_content_range(headers: &HeaderMap) -> Option<(u64, u64)> {
    let raw = headers
        .get(http::header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())?
        .trim();
    // Accept: "0-21" or "bytes 0-21/42".
    let raw = raw.strip_prefix("bytes ").unwrap_or(raw);
    let (range, _total) = raw.split_once('/').unwrap_or((raw, ""));
    let (start_s, end_s) = range.split_once('-')?;
    let start = start_s.trim().parse::<u64>().ok()?;
    let end = end_s.trim().parse::<u64>().ok()?;
    Some((start, end))
}
