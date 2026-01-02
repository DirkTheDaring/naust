use crate::{
    registry::digest::Digest,
    storage::{ReferrerDescriptor, RepoTimestamps, StorageError},
    AppState,
};
use axum::{
    body::Body,
    extract::RawQuery,
    extract::Query,
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use base64::Engine as _;
use bytes::Bytes;
use headers::{authorization::Basic, Authorization, HeaderMapExt};
use hmac::{Hmac, Mac};
use sha2::Digest as _;
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio_util::io::ReaderStream;
use url::form_urlencoded;

use super::errors;

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

    // If push is requested, require Basic auth and validate creds.
    let wants_push = scopes.iter().any(|s| s.actions.iter().any(|a| a == "push"));
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
            for scope in &scopes {
                if scope.typ == "repository" {
                    if !crate::auth::repo_allowed(allowlist, &scope.name) {
                        return errors::denied("push not allowed for this repository").into_response();
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

    let token = match issue_token(&state, subject.as_deref(), &scopes, now, exp) {
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
            let typ = parts.next()?.to_string();
            let name = parts.next()?.to_string();
            let actions = parts
                .next()
                .unwrap_or("")
                .split(',')
                .map(|a| a.trim().to_string())
                .filter(|a| !a.is_empty())
                .collect::<Vec<_>>();
            Some(Scope { typ, name, actions })
        })
        .collect()
}

fn issue_token(
    state: &AppState,
    subject: Option<&str>,
    scopes: &[Scope],
    iat: u64,
    exp: u64,
) -> Result<String, ()> {
    let payload = serde_json::json!({
        "sub": subject,
        "iat": iat,
        "exp": exp,
        "scopes": scopes.iter().map(|s| serde_json::json!({
            "type": s.typ,
            "name": s.name,
            "actions": s.actions,
        })).collect::<Vec<_>>(),
    });

    let payload_bytes = serde_json::to_vec(&payload).map_err(|_| ())?;
    let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&payload_bytes);

    let mut mac = Hmac::<Sha256>::new_from_slice(state.config.token_signing_key.as_bytes())
        .map_err(|_| ())?;
    mac.update(payload_b64.as_bytes());
    let sig = mac.finalize().into_bytes();
    let sig_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig);

    Ok(format!("{payload_b64}.{sig_b64}"))
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
    body: Bytes,
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
            return manifest_put(state, &name, reference, body).await;
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

async fn catalog_list(state: AppState, method: Method, query: &HashMap<String, String>) -> Response {
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
                    if let (Some(n_raw), Some(last_repo)) = (query.get("n"), payload.get("repositories").and_then(|v| v.as_array()).and_then(|a| a.last()).and_then(|x| x.as_str())) {
                        let last_repo = url_encode_component(last_repo);
                        let link = format!("</v2/_catalog?n={n_raw}&last={last_repo}>; rel=\"next\"");
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
        if let (Some(n_raw), Some(last_org)) = (query.get("n"), payload.get("orgs").and_then(|v| v.as_array()).and_then(|a| a.last()).and_then(|x| x.as_str())) {
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
            let link = format!("</_meta/orgs/{org}/repos?n={n_raw}&last={last_repo}>; rel=\"next\"");
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
            Err(StorageError::NotFound) => errors::blob_unknown().into_response(),
            Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
            Err(StorageError::TooLarge) => errors::internal_error().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
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
            Err(StorageError::NotFound) => errors::blob_unknown().into_response(),
            Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
            Err(StorageError::TooLarge) => errors::internal_error().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
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
    let digest = match Digest::parse(reference) {
        Ok(d) => d,
        Err(_) => match state.storage.resolve_tag(name, reference).await {
            Ok(d) => d,
            Err(StorageError::NotFound) => return errors::manifest_unknown().into_response(),
            Err(StorageError::DigestMismatch) => return errors::internal_error().into_response(),
            Err(StorageError::TooLarge) => return errors::internal_error().into_response(),
            Err(StorageError::Unsupported) => return errors::not_implemented().into_response(),
            Err(StorageError::Internal(_)) => return errors::internal_error().into_response(),
        },
    };

    match method {
        Method::DELETE => match state.storage.delete_manifest(name, &digest).await {
            Ok(()) => (StatusCode::ACCEPTED, registry_headers()).into_response(),
            Err(StorageError::NotFound) => errors::manifest_unknown().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
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
            Err(StorageError::NotFound) => errors::manifest_unknown().into_response(),
            Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
            Err(StorageError::TooLarge) => errors::internal_error().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
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
            Err(StorageError::NotFound) => errors::manifest_unknown().into_response(),
            Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
            Err(StorageError::TooLarge) => errors::internal_error().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
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
    name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-'))
}

fn is_valid_tag(tag: &str) -> bool {
    if tag.is_empty() || tag.len() > 128 {
        return false;
    }
    if tag.contains('/') || tag.contains(char::is_whitespace) {
        return false;
    }
    let mut chars = tag.chars();
    let Some(first) = chars.next() else { return false };
    if !(first.is_ascii_alphanumeric() || first == '_') {
        return false;
    }
    tag.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
}

#[cfg(test)]
mod tests {
    use super::{is_valid_repo_name, is_valid_tag};

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
    )
}

async fn manifest_put(state: AppState, name: &str, reference: &str, bytes: Bytes) -> Response {
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
    let computed = Digest::parse(&format!("sha256:{digest_hex}")).expect("computed sha256 is valid");

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
                Ok(_) => return (StatusCode::CONFLICT, registry_headers(), Body::empty()).into_response(),
                Err(StorageError::NotFound) => {}
                Err(StorageError::Unsupported) => return errors::not_implemented().into_response(),
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
        let _ = state
            .storage
            .add_referrer(name, &subject, descriptor)
            .await;
    }

    let mut headers = registry_headers();
    headers.insert("Docker-Content-Digest", computed.as_str().parse().unwrap());
    headers.insert("Content-Type", meta.media_type.parse().unwrap());
    headers.insert("Location", format!("/v2/{name}/manifests/{}", reference).parse().unwrap());
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
                Err(StorageError::DigestMismatch) => return errors::internal_error().into_response(),
                Err(StorageError::NotFound) => Vec::new(),
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
                        obj["annotations"] = serde_json::to_value(ann).unwrap_or(serde_json::Value::Null);
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

async fn upload_create(
    state: AppState,
    method: Method,
    name: &str,
    query: &HashMap<String, String>,
    body: Bytes,
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
        if body.is_empty() {
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
                        format!("/v2/{name}/blobs/{}", digest.as_str()).parse().unwrap(),
                    );
                    headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                    headers.insert("Content-Length", "0".parse().unwrap());
                    return (StatusCode::CREATED, headers).into_response();
                }
            }
        }
    }

    // Monolithic upload (body on POST): POST /v2/<name>/blobs/uploads/?digest=<digest>
    // Conformance allows this to either create an upload session (202) or create the blob (201).
    // If the blob already exists, we return 201.
    if let Some(digest_str) = query.get("digest").map(|s| s.as_str()) {
        if !body.is_empty() {
            let digest = match Digest::parse(digest_str) {
                Ok(d) => d,
                Err(_) => return errors::digest_invalid().into_response(),
            };

            if state.storage.head_blob(&digest).await.is_ok() {
                let mut headers = registry_headers();
                headers.insert(
                    "Location",
                    format!("/v2/{name}/blobs/{}", digest.as_str()).parse().unwrap(),
                );
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert("Content-Length", "0".parse().unwrap());
                return (StatusCode::CREATED, headers).into_response();
            }

            // Create a session, append the body, and finalize.
            let meta = match state.storage.create_upload().await {
                Ok(meta) => meta,
                Err(StorageError::Unsupported) => return errors::not_implemented().into_response(),
                Err(StorageError::Internal(msg)) => {
                    tracing::error!(
                        storage = state.storage.kind(),
                        error = %msg,
                        repo = name,
                        "create_upload failed"
                    );
                    return errors::internal_error().into_response();
                }
                Err(StorageError::DigestMismatch) | Err(StorageError::NotFound) => {
                    tracing::error!(storage = state.storage.kind(), repo = name, "create_upload failed");
                    return errors::internal_error().into_response();
                }
                Err(StorageError::TooLarge) => return errors::internal_error().into_response(),
            };

            if let Err(err) = state.storage.append_upload(&meta.uuid, body).await {
                return match err {
                    StorageError::NotFound => errors::blob_upload_unknown().into_response(),
                    StorageError::TooLarge => {
                        errors::blob_upload_invalid("upload too large").into_response()
                    }
                    StorageError::Unsupported => errors::not_implemented().into_response(),
                    StorageError::Internal(_) | StorageError::DigestMismatch => {
                        errors::internal_error().into_response()
                    }
                };
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
                    headers.insert("Content-Length", final_meta.size.to_string().parse().unwrap());
                    (StatusCode::CREATED, headers).into_response()
                }
                Err(StorageError::NotFound) => errors::blob_upload_unknown().into_response(),
                Err(StorageError::DigestMismatch) => errors::digest_invalid().into_response(),
                Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
                Err(StorageError::TooLarge) => errors::internal_error().into_response(),
                Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
            };
        }
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
    }
}

async fn upload_session(
    state: AppState,
    method: Method,
    req_headers: &HeaderMap,
    name: &str,
    uuid: &str,
    query: HashMap<String, String>,
    body: Bytes,
) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }

    let location = format!("/v2/{name}/blobs/uploads/{uuid}");

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
            Err(StorageError::Internal(_)) | Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
        },
        Method::PATCH => {
            // Chunked uploads: enforce that Content-Range starts at the current offset.
            if let Some((start, _end)) = parse_content_range(req_headers) {
                match state.storage.upload_status(uuid).await {
                    Ok(meta) if meta.offset == start => {}
                    Ok(_) => {
                        return (
                            StatusCode::RANGE_NOT_SATISFIABLE,
                            registry_headers(),
                        )
                            .into_response();
                    }
                    Err(StorageError::NotFound) => {
                        return errors::blob_upload_unknown().into_response();
                    }
                    Err(_) => return errors::internal_error().into_response(),
                }
            }

            match state.storage.append_upload(uuid, body).await {
                Ok(meta) => {
                    let mut headers = registry_headers();
                    headers.insert("Location", location.parse().unwrap());
                    headers.insert("Docker-Upload-UUID", meta.uuid.parse().unwrap());
                    if meta.offset > 0 {
                        headers.insert("Range", format!("0-{}", meta.offset - 1).parse().unwrap());
                    }
                    (StatusCode::ACCEPTED, headers).into_response()
                }
                Err(StorageError::NotFound) => errors::blob_upload_unknown().into_response(),
                Err(StorageError::TooLarge) => {
                    errors::blob_upload_invalid("upload too large").into_response()
                }
                Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
                Err(StorageError::Internal(_)) | Err(StorageError::DigestMismatch) => {
                    errors::internal_error().into_response()
                }
            }
        }
        Method::PUT => {
            let Some(digest_str) = query.get("digest").map(|s| s.as_str()) else {
                return errors::digest_invalid().into_response();
            };
            let digest = match Digest::parse(digest_str) {
                Ok(d) => d,
                Err(_) => return errors::digest_invalid().into_response(),
            };

            // Monolithic upload: some clients send the full blob body on the PUT.
            // If a body is present, append it before finalizing.
            if !body.is_empty() {
                // Best-effort Content-Range enforcement (optional for monolithic), matching the PATCH behavior.
                if let Some((start, _end)) = parse_content_range(req_headers) {
                    match state.storage.upload_status(uuid).await {
                        Ok(meta) if meta.offset == start => {}
                        Ok(_) => {
                            return (
                                StatusCode::RANGE_NOT_SATISFIABLE,
                                registry_headers(),
                            )
                                .into_response();
                        }
                        Err(StorageError::NotFound) => {
                            return errors::blob_upload_unknown().into_response();
                        }
                        Err(_) => return errors::internal_error().into_response(),
                    }
                }

                if let Err(err) = state.storage.append_upload(uuid, body).await {
                    return match err {
                        StorageError::NotFound => errors::blob_upload_unknown().into_response(),
                        StorageError::TooLarge => {
                            errors::blob_upload_invalid("upload too large").into_response()
                        }
                        StorageError::Unsupported => errors::not_implemented().into_response(),
                        StorageError::Internal(_) | StorageError::DigestMismatch => {
                            errors::internal_error().into_response()
                        }
                    };
                }
            }

            match state.storage.finalize_upload(uuid, &digest).await {
                Ok(meta) => {
                    let mut headers = registry_headers();
                    headers.insert("Location", format!("/v2/{name}/blobs/{}", digest.as_str()).parse().unwrap());
                    headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                    headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                    (StatusCode::CREATED, headers).into_response()
                }
                Err(StorageError::NotFound) => errors::blob_upload_unknown().into_response(),
                Err(StorageError::DigestMismatch) => errors::digest_invalid().into_response(),
                Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
                Err(StorageError::TooLarge) => errors::internal_error().into_response(),
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
