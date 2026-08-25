use crate::{
    AppState, ProxyContext,
    http_api::errors,
    http_api::handlers::{is_valid_repo_name, registry_headers},
    registry::digest::Digest,
    request_routing::V2RouteMode,
    storage::StorageError,
};
use axum::{
    body::Body,
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
};
use std::collections::HashMap;

pub async fn referrers_list(
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

            // Sort deterministically by digest for stable pagination.
            entries.sort_by(|a, b| a.digest.cmp(&b.digest));

            // Apply pagination if `last` is provided.
            let start_idx = if let Some(last) = query.get("last") {
                match entries.iter().position(|d| d.digest == *last) {
                    Some(pos) => pos + 1,
                    None => 0,
                }
            } else {
                0
            };

            let remaining = if start_idx < entries.len() {
                &entries[start_idx..]
            } else {
                &[]
            };

            let n_opt = query.get("n").and_then(|s| s.parse::<usize>().ok());
            let (page_entries, next_last) = match n_opt {
                Some(n) if n < remaining.len() => (&remaining[..n], Some(&remaining[n - 1].digest)),
                _ => (remaining, None),
            };

            // Return OCI index.
            let manifests = page_entries
                .iter()
                .map(|d| {
                    let mut obj = serde_json::json!({
                        "mediaType": d.media_type,
                        "digest": d.digest,
                        "size": d.size,
                    });
                    if let Some(at) = &d.artifact_type {
                        obj["artifactType"] = serde_json::Value::String(at.clone());
                    }
                    if let Some(ann) = &d.annotations {
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

            if let (Some(n), Some(last_digest)) = (n_opt, next_last) {
                let mut link_params = vec![format!("n={}", n)];
                if let Some(filter) = artifact_type_filter {
                    let encoded_filter: String =
                        url::form_urlencoded::byte_serialize(filter.as_bytes()).collect();
                    link_params.push(format!("artifactType={}", encoded_filter));
                }
                link_params.push(format!("last={}", last_digest));
                let link_header = format!(
                    r#"</v2/{}/referrers/{}?{}>; rel="next""#,
                    name,
                    subject.as_str(),
                    link_params.join("&")
                );
                if let Ok(val) = link_header.parse() {
                    headers.insert("Link", val);
                }
            }

            if method == Method::HEAD {
                return (StatusCode::OK, headers).into_response();
            }
            (StatusCode::OK, headers, Body::from(bytes)).into_response()
        }
        _ => errors::method_not_allowed("GET, HEAD"),
    }
}
