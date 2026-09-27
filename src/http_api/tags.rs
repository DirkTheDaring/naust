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

    let n = query.get("n").and_then(|s| s.parse::<usize>().ok());
    let last = query.get("last").cloned();
    let params = crate::application::TagQueryParams { n, last };

    let proxy_target = match route_mode {
        V2RouteMode::Default => None,
        V2RouteMode::ProxyOnly => match proxy_ctx.as_ref() {
            Some(ctx) => Some(ctx),
            None => return errors::internal_error().into_response(),
        },
    };

    match method {
        Method::GET | Method::HEAD => {
            match state
                .tag_query_service
                .query_tags(name, params, proxy_target)
                .await
            {
                Ok(page) => {
                    let payload = serde_json::json!({
                        "name": page.repo,
                        "tags": page.tags,
                    });
                    let bytes = match serde_json::to_vec(&payload) {
                        Ok(b) => b,
                        Err(_) => return errors::internal_error().into_response(),
                    };

                    let mut headers = registry_headers();
                    headers.insert("Content-Type", "application/json".parse().unwrap());
                    headers.insert("Content-Length", bytes.len().to_string().parse().unwrap());

                    if page.has_more
                        && let (Some(n_val), Some(last_tag)) = (n, page.next_last.as_deref()) {
                            let last_tag = url_encode_component(last_tag);
                            let link = format!(
                                "</v2/{name}/tags/list?last={last_tag}&n={n_val}>; rel=\"next\""
                            );
                            if let Ok(v) = http::HeaderValue::from_str(&link) {
                                headers.insert(http::header::LINK, v);
                            }
                        }

                    if method == Method::HEAD {
                        return (StatusCode::OK, headers).into_response();
                    }
                    (StatusCode::OK, headers, Body::from(bytes)).into_response()
                }
                Err(crate::application::TagQueryError::InvalidRepoName { .. }) => {
                    errors::name_invalid().into_response()
                }
                Err(crate::application::TagQueryError::NotFound) => {
                    errors::name_unknown().into_response()
                }
                Err(crate::application::TagQueryError::Storage(StorageError::NotFound)) => {
                    errors::name_unknown().into_response()
                }
                Err(crate::application::TagQueryError::Storage(StorageError::Unsupported)) => {
                    errors::not_implemented().into_response()
                }
                Err(crate::application::TagQueryError::Storage(
                    StorageError::InsufficientStorage,
                )) => errors::insufficient_storage().into_response(),
                Err(_) => errors::internal_error().into_response(),
            }
        }
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

    match state
        .manifest_service
        .delete_tag(name, tag, state.config.allow_tag_overwrite)
        .await
    {
        Ok(_) => (StatusCode::ACCEPTED, registry_headers()).into_response(),
        Err(crate::application::ManifestMutationError::TagImmutable) => {
            errors::denied("tag is immutable and cannot be deleted").into_response()
        }
        Err(crate::application::ManifestMutationError::TagNotFound) => {
            errors::tag_unknown().into_response()
        }
        Err(crate::application::ManifestMutationError::InvalidRepoName { .. }) => {
            errors::name_invalid().into_response()
        }
        Err(crate::application::ManifestMutationError::InvalidTag) => {
            errors::tag_invalid().into_response()
        }
        Err(crate::application::ManifestMutationError::Storage(StorageError::Unsupported)) => {
            errors::method_not_allowed("DELETE")
        }
        Err(_) => errors::internal_error().into_response(),
    }
}
