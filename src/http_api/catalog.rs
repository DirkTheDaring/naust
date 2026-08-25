use crate::{
    AppState, ProxyContext,
    http_api::errors,
    http_api::handlers::{
        format_rfc3339, query_bool, registry_headers, tag_platforms_for_repo, url_encode_component,
    },
    request_routing::V2RouteMode,
    storage::{RepoTimestamps, StorageError},
};
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt as _;
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

pub async fn catalog_list(
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
                Ok(mut all) => {
                    all.sort();
                    let total = all.len();
                    let n_opt = query.get("n").and_then(|s| s.parse::<usize>().ok());
                    let n = n_opt.unwrap_or(usize::MAX);

                    let start_idx = match query.get("last") {
                        Some(last) => all.iter().position(|t| t > last).unwrap_or(all.len()),
                        None => 0,
                    };

                    let end_idx = start_idx.saturating_add(n).min(total);
                    let repos: Vec<String> = all
                        .into_iter()
                        .skip(start_idx)
                        .take(end_idx.saturating_sub(start_idx))
                        .collect();
                    let has_more =
                        n_opt.is_some() && !repos.is_empty() && repos.len() == n && end_idx < total;

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
        _ => errors::method_not_allowed("GET, HEAD"),
    }
}

pub fn repo_org(name: &str) -> Option<&str> {
    name.split_once('/').map(|(org, _)| org)
}

pub fn system_time_to_rfc3339_opt(t: Option<SystemTime>) -> Option<String> {
    let t = t?;
    let secs = t.duration_since(UNIX_EPOCH).ok()?.as_secs();
    Some(format_rfc3339(secs))
}

pub fn repo_meta_from_timestamps(name: &str, ts: RepoTimestamps) -> serde_json::Value {
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
    let include_tags = query_bool(&query, "include_tags");
    for repo in &page {
        match state.storage.repo_timestamps(repo).await {
            Ok(ts) => {
                let mut meta = repo_meta_from_timestamps(repo, ts);
                if include_tags {
                    let tags = match state.storage.list_tags(repo).await {
                        Ok(t) => t,
                        Err(StorageError::NotFound) => Vec::new(),
                        Err(_) => return errors::internal_error().into_response(),
                    };
                    if let Some(obj) = meta.as_object_mut() {
                        obj.insert("tag_count".to_string(), serde_json::json!(tags.len()));
                        obj.insert("tags".to_string(), serde_json::json!(tags));
                    }
                }
                repos_out.push(meta);
            }
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
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if state.config.catalog_requires_auth && !crate::auth::is_authenticated(&state, &headers) {
        return crate::auth::unauthorized_catalog_challenge(&state).into_response();
    }

    let include_tags = query_bool(&query, "include_tags");

    match state.storage.repo_timestamps(&name).await {
        Ok(ts) => {
            let mut payload = repo_meta_from_timestamps(&name, ts);
            if include_tags {
                let tags = match state.storage.list_tags(&name).await {
                    Ok(t) => t,
                    Err(StorageError::NotFound) => Vec::new(),
                    Err(_) => return errors::internal_error().into_response(),
                };
                if let Some(obj) = payload.as_object_mut() {
                    obj.insert("tag_count".to_string(), serde_json::json!(tags.len()));
                    obj.insert("tags".to_string(), serde_json::json!(tags));
                }
            }
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
    let include_platforms = query_bool(&query, "include_platforms");
    let include_tags = query_bool(&query, "include_tags") || include_platforms;
    for repo in &page {
        match state.storage.repo_timestamps(repo).await {
            Ok(ts) => {
                let mut meta = repo_meta_from_timestamps(repo, ts);
                if include_tags {
                    let tags = match state.storage.list_tags(repo).await {
                        Ok(t) => t,
                        Err(StorageError::NotFound) => Vec::new(),
                        Err(_) => return errors::internal_error().into_response(),
                    };
                    if let Some(obj) = meta.as_object_mut() {
                        obj.insert("tag_count".to_string(), serde_json::json!(tags.len()));
                        obj.insert("tags".to_string(), serde_json::json!(tags));

                        if include_platforms {
                            let storage = state.storage.clone();
                            let repo = repo.to_string();
                            let tag_details = futures_util::stream::iter(
                                obj.get("tags")
                                    .and_then(|t| t.as_array())
                                    .into_iter()
                                    .flatten()
                                    .filter_map(|t| t.as_str().map(|s| s.to_string()))
                                    .collect::<Vec<_>>(),
                            )
                            .map(|tag| {
                                let storage = storage.clone();
                                let repo = repo.clone();
                                async move { tag_platforms_for_repo(&storage, &repo, &tag).await }
                            })
                            .buffer_unordered(16)
                            .collect::<Vec<_>>()
                            .await;

                            let tag_details = tag_details
                                .into_iter()
                                .filter_map(Result::ok)
                                .collect::<Vec<_>>();
                            obj.insert("tag_details".to_string(), serde_json::json!(tag_details));
                        }
                    }
                }
                repos_out.push(meta);
            }
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
