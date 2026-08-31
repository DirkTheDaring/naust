use crate::{
    AppState, ProxyContext, http_api::errors, http_api::handlers::registry_headers,
    registry::digest::Digest, registry::validation::is_valid_repo_name,
    request_routing::V2RouteMode, storage::StorageError,
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
            let artifact_type = query.get("artifactType").cloned();
            let last = query.get("last").cloned();
            let n = query.get("n").and_then(|s| s.parse::<usize>().ok());
            let params = crate::application::ReferrersQueryParams {
                artifact_type: artifact_type.clone(),
                last,
                n,
            };

            let proxy_target = match route_mode {
                V2RouteMode::Default => None,
                V2RouteMode::ProxyOnly => match proxy_ctx.as_ref() {
                    Some(ctx) => Some(ctx),
                    None => return errors::internal_error().into_response(),
                },
            };

            let page = match state
                .referrers_query_service
                .query_referrers(name, &subject, params, proxy_target)
                .await
            {
                Ok(p) => p,
                Err(crate::application::ReferrersQueryError::InvalidRepoName { .. }) => {
                    return errors::name_invalid().into_response();
                }
                Err(crate::application::ReferrersQueryError::InvalidDigest(_)) => {
                    return errors::digest_invalid().into_response();
                }
                Err(crate::application::ReferrersQueryError::Storage(
                    StorageError::Unsupported,
                )) => {
                    return errors::not_implemented().into_response();
                }
                Err(crate::application::ReferrersQueryError::Storage(
                    StorageError::InsufficientStorage,
                )) => {
                    return errors::insufficient_storage().into_response();
                }
                Err(crate::application::ReferrersQueryError::Storage(
                    StorageError::InvalidRepoName(_),
                )) => {
                    return errors::name_invalid().into_response();
                }
                Err(_) => return errors::internal_error().into_response(),
            };

            let page_entries = page.descriptors;
            let next_last = page.next_last;

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
            if artifact_type.is_some() {
                headers.insert("OCI-Filters-Applied", "artifactType".parse().unwrap());
            }

            if let (Some(n_val), Some(last_digest)) = (n, next_last) {
                let mut link_params = vec![format!("n={}", n_val)];
                if let Some(ref filter) = artifact_type {
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
