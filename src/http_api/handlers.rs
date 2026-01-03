use crate::{
    AppState,
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
use crate::security;

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
    let mut scopes_raw: Vec<String> = Vec::new();
    let raw = raw_query.0.unwrap_or_default();
    for (k, v) in form_urlencoded::parse(raw.as_bytes()) {
        if k == "scope" {
            scopes_raw.push(v.into_owned());
        }
    }
    let scopes = scopes_raw
        .iter()
        .flat_map(|s| parse_scopes(s))
        .collect::<Vec<_>>();

    // Be lenient in parsing, but never mint unexpected permissions.
    // We only mint repository scopes and only the actions we understand.
    let token_scopes = sanitize_token_scopes(&scopes);

    // If push is requested, require Basic auth and validate creds.
    let wants_push = token_scopes
        .iter()
        .any(|s| token_scope_requests_repo_action(s, security::RepoAction::Push));
    let subject = if wants_push {
        let Some(expected_user) = state.config.push_username.as_deref() else {
            return token_unauthorized(&state);
        };
        let Some(expected_pass) = state.config.push_password.as_deref() else {
            return token_unauthorized(&state);
        };

        match headers.typed_get::<Authorization<Basic>>() {
            Some(Authorization(basic))
                if basic.username() == expected_user && basic.password() == expected_pass =>
            {
                Some(basic.username().to_string())
            }
            _ => return token_unauthorized(&state),
        }
    } else {
        None
    };

    // Enforce repo allowlist for push tokens too.
    if wants_push {
        if let Some(allowlist) = state.config.push_allow_repos.as_deref() {
            for scope in &token_scopes {
                if scope.typ == "repository" {
                    if !crate::auth::repo_allowed(allowlist, &scope.name) {
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
    let exp = now.saturating_add(state.config.token_ttl_secs);

    let token = match issue_token(&state, subject.as_deref(), &token_scopes, now, exp) {
        Ok(t) => t,
        Err(_) => return errors::internal_error().into_response(),
    };

    let body = serde_json::json!({
        "token": token,
        "access_token": token,
        "expires_in": state.config.token_ttl_secs,
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

fn token_scope_requests_repo_action(scope: &security::TokenScope, action: security::RepoAction) -> bool {
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

        let mut actions: Vec<String> = Vec::new();
        for a in &s.actions {
            if a == security::RepoAction::Pull.as_str() || a == security::RepoAction::Push.as_str() {
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
    security::issue_bearer_token(
        &state.config.token_signing_key,
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
    let segments: Vec<&str> = rest.split('/').filter(|s| !s.is_empty()).collect();

    // Registry catalog:
    //   GET/HEAD /v2/_catalog
    if segments.len() == 1 && segments[0] == "_catalog" {
        // Optional privacy policy: require auth for catalog.
        if state.config.catalog_requires_auth && !crate::auth::is_authenticated(&state, &headers) {
            return crate::auth::unauthorized_catalog_challenge(&state).into_response();
        }
        return catalog_list(state, method, &query).await;
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
        return tags_list(state, method, &name, &query).await;
    }

    // Referrers:
    //   GET/HEAD /v2/<name>/referrers/<digest>
    if segments.len() >= 2 && segments[segments.len() - 2] == "referrers" {
        let digest_str = segments[segments.len() - 1];
        let name = segments[..segments.len() - 2].join("/");
        return referrers_list(state, method, &name, digest_str, &query).await;
    }

    if segments.len() >= 2 && segments[segments.len() - 2] == "manifests" {
        let reference = segments[segments.len() - 1];
        let name = segments[..segments.len() - 2].join("/");
        if method == Method::PUT {
            return manifest_put(state, &headers, &name, reference, body).await;
        }
        return manifest_by_reference(state, method, &name, reference).await;
    }

    // /v2/<name>/blobs/<digest>
    // Avoid catching /blobs/uploads by requiring the digest format.
    if segments.len() >= 2
        && segments[segments.len() - 2] == "blobs"
        && segments[segments.len() - 1].contains(':')
    {
        let digest_str = segments[segments.len() - 1];
        let name = segments[..segments.len() - 2].join("/");
        return blob_by_digest(state, method, &name, digest_str).await;
    }

    errors::not_implemented().into_response()
}

async fn catalog_list(
    state: AppState,
    method: Method,
    query: &HashMap<String, String>,
) -> Response {
    match method {
        Method::GET | Method::HEAD => match state.storage.list_repositories().await {
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
        },
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
) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }

    match method {
        Method::GET | Method::HEAD => match state.storage.list_tags(name).await {
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

async fn blob_by_digest(state: AppState, method: Method, name: &str, digest_str: &str) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }

    let digest = match Digest::parse(digest_str) {
        Ok(d) => d,
        Err(_) => return errors::digest_invalid().into_response(),
    };

    match method {
        Method::DELETE => match state.storage.delete_blob(&digest).await {
            Ok(()) => (StatusCode::ACCEPTED, registry_headers()).into_response(),
            Err(StorageError::NotFound) => errors::blob_unknown().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
            Err(StorageError::InsufficientStorage) => {
                errors::insufficient_storage().into_response()
            }
            Err(StorageError::TooLarge) => errors::internal_error().into_response(),
            Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
            Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
        },
        Method::HEAD => match state.storage.head_blob(&digest).await {
            Ok(meta) => {
                let mut headers = registry_headers();
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                (StatusCode::OK, headers).into_response()
            }
            Err(StorageError::NotFound) => {
                if let Some(cache) = state.proxy_cache.as_ref() {
                    if let Ok(meta) = cache.head_blob(&digest).await {
                        if let Some(proxy) = state.proxy.as_ref() {
                            proxy.note_blob_access(&digest);
                        }
                        let mut headers = registry_headers();
                        headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                        headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                        headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                        return (StatusCode::OK, headers).into_response();
                    }
                }

                if let Some(proxy) = state.proxy.as_ref() {
                    if let Ok(decision) = proxy.decision_for_repo(name) {
                        match proxy.head_blob_upstream(&decision, &digest).await {
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
                if let Some(cache) = state.proxy_cache.as_ref() {
                    if let Ok((meta, reader)) = cache.open_blob(&digest).await {
                        if let Some(proxy) = state.proxy.as_ref() {
                            proxy.note_blob_access(&digest);
                        }
                        let stream = ReaderStream::new(reader);
                        let body = Body::from_stream(stream);

                        let mut headers = registry_headers();
                        headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                        headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                        headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                        return (StatusCode::OK, headers, body).into_response();
                    }
                }

                if let Some(proxy) = state.proxy.as_ref() {
                    if let Ok(decision) = proxy.decision_for_repo(name) {
                        let Some(cache) = state.proxy_cache.as_ref() else {
                            return errors::internal_error().into_response();
                        };
                        match proxy
                            .fetch_blob_into_storage(&decision, &digest, cache)
                            .await
                        {
                            Ok(()) => {
                                // Retry from cache storage.
                                if let Ok((meta, reader)) = cache.open_blob(&digest).await {
                                    proxy.note_blob_access(&digest);
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

async fn manifest_by_reference(
    state: AppState,
    method: Method,
    name: &str,
    reference: &str,
) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }

    // Reference can be a digest or a tag.
    let is_digest_ref = Digest::parse(reference).is_ok();

    // Resolve tag references to a digest (with optional proxying).
    let digest = if let Ok(d) = Digest::parse(reference) {
        d
    } else {
        match state.storage.resolve_tag(name, reference).await {
            Ok(d) => d,
            Err(StorageError::NotFound) => {
                // Check cache storage for an existing cached tag.
                if let Some(cache) = state.proxy_cache.as_ref() {
                    if let Ok(d) = cache.resolve_tag(name, reference).await {
                        d
                    } else {
                        // Tag not known locally. If proxying is enabled and repo is allowed, resolve from upstream.
                        if let Some(proxy) = state.proxy.as_ref() {
                            let Some(cache) = state.proxy_cache.as_ref() else {
                                return errors::internal_error().into_response();
                            };
                            if let Ok(decision) = proxy.decision_for_repo(name) {
                                match decision.tag_policy.clone() {
                                    crate::config::TagPolicy::DigestOnly => {
                                        let _permit = match state
                                            .buffered_body_sem
                                            .clone()
                                            .acquire_owned()
                                            .await
                                        {
                                            Ok(p) => p,
                                            Err(_) => {
                                                return errors::internal_error().into_response();
                                            }
                                        };
                                        match proxy
                                            .fetch_manifest_and_cache(
                                                &decision,
                                                reference,
                                                cache,
                                                state.config.max_request_body_bytes,
                                                false,
                                                None,
                                            )
                                            .await
                                        {
                                            Ok(crate::proxy::FetchManifestResult::Fetched {
                                                digest,
                                                ..
                                            }) => digest,
                                            Ok(_) => {
                                                return errors::internal_error().into_response();
                                            }
                                            Err(crate::proxy::ProxyError::NotFound) => {
                                                return errors::manifest_unknown().into_response();
                                            }
                                            Err(err) => {
                                                tracing::warn!(error = %err, repo = name, tag = reference, "proxy: resolve tag failed");
                                                return errors::internal_error().into_response();
                                            }
                                        }
                                    }
                                    crate::config::TagPolicy::TtlSeconds(ttl) => {
                                        if let Err(resp) = ensure_tag_fresh(
                                            &state, proxy, &decision, cache, reference, ttl, false,
                                        )
                                        .await
                                        {
                                            return resp;
                                        }
                                        match cache.resolve_tag(name, reference).await {
                                            Ok(d) => d,
                                            Err(StorageError::NotFound) => {
                                                return errors::manifest_unknown().into_response();
                                            }
                                            Err(_) => {
                                                return errors::internal_error().into_response();
                                            }
                                        }
                                    }
                                    crate::config::TagPolicy::AlwaysRevalidate => {
                                        if let Err(resp) = ensure_tag_fresh(
                                            &state, proxy, &decision, cache, reference, 0, true,
                                        )
                                        .await
                                        {
                                            return resp;
                                        }
                                        match cache.resolve_tag(name, reference).await {
                                            Ok(d) => d,
                                            Err(StorageError::NotFound) => {
                                                return errors::manifest_unknown().into_response();
                                            }
                                            Err(_) => {
                                                return errors::internal_error().into_response();
                                            }
                                        }
                                    }
                                }
                            } else {
                                return errors::manifest_unknown().into_response();
                            }
                        } else {
                            return errors::manifest_unknown().into_response();
                        }
                    }
                } else {
                    // Tag not known locally and no cache store.
                    return errors::manifest_unknown().into_response();
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
        if let Some(proxy) = state.proxy.as_ref() {
            proxy.note_tag_access(name, reference);
        }
    }

    match method {
        Method::DELETE => match state.storage.delete_manifest(name, &digest).await {
            Ok(()) => (StatusCode::ACCEPTED, registry_headers()).into_response(),
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
                if let Some(cache) = state.proxy_cache.as_ref() {
                    if let Ok(meta) = cache.head_manifest(name, &digest).await {
                        if let Some(proxy) = state.proxy.as_ref() {
                            proxy.note_manifest_access(name, &digest);
                            if let Some(refs) = proxy.get_manifest_refs(name, &digest) {
                                for blob in refs.blobs {
                                    if let Ok(d) = crate::registry::digest::Digest::parse(&blob) {
                                        proxy.note_blob_access(&d);
                                    }
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
                if let Some(proxy) = state.proxy.as_ref() {
                    let Some(cache) = state.proxy_cache.as_ref() else {
                        return errors::internal_error().into_response();
                    };
                    if let Ok(decision) = proxy.decision_for_repo(name) {
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
                        match proxy
                            .fetch_manifest_and_cache(
                                &decision,
                                upstream_ref,
                                cache,
                                state.config.max_request_body_bytes,
                                false,
                                None,
                            )
                            .await
                        {
                            Ok(_) => {
                                if let Ok(meta) = cache.head_manifest(name, &digest).await {
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
                if let Some(cache) = state.proxy_cache.as_ref() {
                    if let Ok((meta, bytes)) = cache.get_manifest(name, &digest).await {
                        if let Some(proxy) = state.proxy.as_ref() {
                            proxy.note_manifest_access(name, &digest);
                            if let Some(refs) = proxy.get_manifest_refs(name, &digest) {
                                for blob in refs.blobs {
                                    if let Ok(d) = crate::registry::digest::Digest::parse(&blob) {
                                        proxy.note_blob_access(&d);
                                    }
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

                if let Some(proxy) = state.proxy.as_ref() {
                    let Some(cache) = state.proxy_cache.as_ref() else {
                        return errors::internal_error().into_response();
                    };
                    if let Ok(decision) = proxy.decision_for_repo(name) {
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
                        match proxy
                            .fetch_manifest_and_cache(
                                &decision,
                                upstream_ref,
                                cache,
                                state.config.max_request_body_bytes,
                                false,
                                None,
                            )
                            .await
                        {
                            Ok(_) => {
                                if let Ok((meta, bytes)) = cache.get_manifest(name, &digest).await {
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
        is_valid_repo_name, is_valid_tag, parse_scopes, sanitize_token_scopes,
        token_scope_requests_repo_action,
    };
    use crate::security;

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
        assert_eq!(scopes[0].actions, vec!["push".to_string(), "pull".to_string()]);
    }

    #[test]
    fn parse_scopes_deduplicates_actions_preserving_order() {
        let scopes = parse_scopes("repository:org/repo:pull,pull,push,pull");
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].actions, vec!["pull".to_string(), "push".to_string()]);
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
        assert_eq!(token_scopes[0].actions, vec!["pull".to_string(), "push".to_string()]);
    }

    #[test]
    fn sanitize_token_scopes_drops_empty_scopes() {
        let scopes = parse_scopes("repository:org/repo:delete");
        let token_scopes = sanitize_token_scopes(&scopes);
        assert!(token_scopes.is_empty());
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
            let mut entries = match state.storage.list_referrers(name, &subject).await {
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
