use crate::{
    AppState, ProxyContext,
    http_api::errors,
    http_api::handlers::{registry_headers, url_encode_component},
    registry::validation::{is_valid_repo_name, is_valid_tag},
    request_routing::V2RouteMode,
    storage::StorageError,
};
use axum::{
    body::Body,
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
};
use std::collections::HashMap;

pub async fn tags_list(
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
            Ok(mut all_tags) => {
                all_tags.sort();
                // Pagination per OCI/Docker distribution spec:
                // - `n` limits the number of tags
                // - `last` starts listing after the provided tag
                let total = all_tags.len();
                let n_opt = query.get("n").and_then(|s| s.parse::<usize>().ok());
                let n = n_opt.unwrap_or(usize::MAX);

                let start_idx = match query.get("last") {
                    Some(last) => all_tags
                        .iter()
                        .position(|t| t > last)
                        .unwrap_or(all_tags.len()),
                    None => 0,
                };

                let end_idx = start_idx.saturating_add(n).min(total);
                let tags: Vec<String> = all_tags
                    .into_iter()
                    .skip(start_idx)
                    .take(end_idx.saturating_sub(start_idx))
                    .collect();

                let has_more =
                    n_opt.is_some() && !tags.is_empty() && tags.len() == n && end_idx < total;

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

                if has_more {
                    if let (Some(n_val), Some(last_tag)) = (n_opt, tags.last()) {
                        let last_tag = url_encode_component(last_tag);
                        let link = format!(
                            "</v2/{name}/tags/list?last={last_tag}&n={n_val}>; rel=\"next\""
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
            Err(StorageError::TagAlreadyExists) => errors::internal_error().into_response(),
            Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
        },
        _ => errors::method_not_allowed("GET, HEAD"),
    }
}

pub async fn tag_delete(state: AppState, method: Method, name: &str, tag: &str) -> Response {
    if method != Method::DELETE {
        return errors::method_not_allowed("DELETE");
    }
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }
    if !is_valid_tag(tag) {
        return errors::tag_invalid().into_response();
    }
    if !state.config.allow_tag_overwrite {
        return errors::denied("tag is immutable and cannot be deleted").into_response();
    }

    let _gate = state.consistency_gate.lock().await;

    if let Some(idx) = state.ref_index.as_ref() {
        if idx.check_health().is_err() {
            if let Err(err) = idx
                .ensure_healthy_or_rebuild(&state.storage, true, false)
                .await
            {
                tracing::error!(
                    repo = name,
                    error = %err,
                    "failed to recover dirty ref-index before tag delete"
                );
                return errors::internal_error().into_response();
            }
        }
        if let Err(err) = idx.mark_dirty() {
            tracing::error!(
                repo = name,
                error = %err,
                "failed to mark ref-index dirty before tag delete"
            );
            return errors::internal_error().into_response();
        }
    }

    match state.storage.delete_tag(name, tag).await {
        Ok(()) => {
            if let Some(idx) = state.ref_index.as_ref() {
                if let Err(err) = idx.sync_repo_tags(&state.storage, name).await {
                    tracing::warn!(
                        repo = name,
                        error = %err,
                        "failed to sync ref-index after tag delete"
                    );
                } else if let Err(err) = idx.mark_ready() {
                    tracing::warn!(
                        repo = name,
                        error = %err,
                        "failed to mark ref-index ready after tag delete"
                    );
                }
            }
            (StatusCode::ACCEPTED, registry_headers()).into_response()
        }
        Err(StorageError::NotFound) => {
            if let Some(idx) = state.ref_index.as_ref() {
                let _ = idx.mark_ready();
            }
            errors::tag_unknown().into_response()
        }
        Err(StorageError::Unsupported) => {
            if let Some(idx) = state.ref_index.as_ref() {
                let _ = idx.mark_ready();
            }
            errors::method_not_allowed("DELETE")
        }
        Err(_) => errors::internal_error().into_response(),
    }
}
