use crate::AppState;
use axum::{
    body::Body,
    extract::Query,
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use std::collections::HashMap;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use url::form_urlencoded;

use super::errors;
use crate::request_routing::v2_route_mode_for_request;

// Family handlers split out of this dispatcher (remediation R4); re-exported so
// existing paths (incl. the sidecar tests) keep resolving.
pub(crate) use super::blobs::blob_by_digest;
pub(crate) use super::manifests::{manifest_by_reference, manifest_put};
pub(crate) use super::uploads::{read_body_limited, upload_create, upload_session};

pub async fn v2_redirect() -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::LOCATION,
        http::HeaderValue::from_static("/v2/"),
    );
    headers.insert(
        http::header::HeaderName::from_static("docker-distribution-api-version"),
        http::HeaderValue::from_static("registry/2.0"),
    );
    (StatusCode::MOVED_PERMANENTLY, headers).into_response()
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

    let mut is_valid_auth = false;
    if auth_scheme != "<none>" {
        is_valid_auth = crate::auth::is_authenticated(&state, &req_headers);
    }

    if state.config.auth_configured() && !is_valid_auth {
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
        if (state.config.auth_strategy == crate::config::AuthStrategy::Token
            || state.config.auth_strategy == crate::config::AuthStrategy::Both)
            && let Ok(v) = http::HeaderValue::from_str(&bearer)
        {
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

pub(crate) fn format_rfc3339(unix_secs: u64) -> String {
    OffsetDateTime::from_unix_timestamp(unix_secs as i64)
        .ok()
        .and_then(|t| t.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

pub(crate) fn url_encode_component(s: &str) -> String {
    form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>()
}

pub(crate) fn query_bool(map: &HashMap<String, String>, key: &str) -> bool {
    map.get(key)
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
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

    let full_path = format!("/v2/{rest}");
    let route = crate::http_api::routing::OciRoute::parse(&full_path);

    match route {
        crate::http_api::routing::OciRoute::InvalidRepoName { .. } => {
            errors::name_invalid().into_response()
        }
        crate::http_api::routing::OciRoute::Catalog => {
            if method != Method::GET && method != Method::HEAD {
                return errors::method_not_allowed("GET, HEAD");
            }
            crate::http_api::catalog::catalog_list(
                state,
                &headers,
                method,
                &query,
                route_mode,
                proxy_ctx.clone(),
            )
            .await
        }
        crate::http_api::routing::OciRoute::ExtensionDiscovery { .. } => {
            if method != Method::GET && method != Method::HEAD {
                return errors::method_not_allowed("GET, HEAD");
            }
            oci_extension_discover(method).await
        }
        crate::http_api::routing::OciRoute::UploadInitiate { repo } => {
            if method != Method::POST {
                return errors::method_not_allowed("POST");
            }
            upload_create(state, &headers, method, repo.as_str(), &query, body).await
        }
        crate::http_api::routing::OciRoute::UploadSession { repo, uuid } => {
            if method != Method::GET
                && method != Method::HEAD
                && method != Method::PATCH
                && method != Method::PUT
                && method != Method::DELETE
            {
                return errors::method_not_allowed("GET, HEAD, PATCH, PUT, DELETE");
            }
            upload_session(state, method, &headers, repo.as_str(), &uuid, query, body).await
        }
        crate::http_api::routing::OciRoute::TagsList { repo } => {
            if method != Method::GET && method != Method::HEAD {
                return errors::method_not_allowed("GET, HEAD");
            }
            crate::http_api::tags::tags_list(
                state,
                method,
                repo.as_str(),
                &query,
                route_mode,
                proxy_ctx.clone(),
            )
            .await
        }
        crate::http_api::routing::OciRoute::TagDelete { repo, tag } => {
            if method != Method::DELETE {
                return errors::method_not_allowed("DELETE");
            }
            crate::http_api::tags::tag_delete(state, method, repo.as_str(), &tag).await
        }
        crate::http_api::routing::OciRoute::Referrers { repo, digest } => {
            if method != Method::GET && method != Method::HEAD {
                return errors::method_not_allowed("GET, HEAD");
            }
            crate::http_api::referrers::referrers_list(
                state,
                method,
                repo.as_str(),
                &digest,
                &query,
                route_mode,
                proxy_ctx.clone(),
            )
            .await
        }
        crate::http_api::routing::OciRoute::Manifest { repo, reference } => {
            if method == Method::PUT {
                return manifest_put(state, &headers, repo.as_str(), &reference, body).await;
            }
            if method != Method::GET && method != Method::HEAD && method != Method::DELETE {
                return errors::method_not_allowed("GET, HEAD, PUT, DELETE");
            }
            manifest_by_reference(
                state,
                &headers,
                method,
                repo.as_str(),
                &reference,
                route_mode,
                proxy_ctx.clone(),
                request_host,
            )
            .await
        }
        crate::http_api::routing::OciRoute::Blob { repo, digest } => {
            if method != Method::GET && method != Method::HEAD && method != Method::DELETE {
                return errors::method_not_allowed("GET, HEAD, DELETE");
            }
            blob_by_digest(
                state,
                &headers,
                method,
                repo.as_str(),
                &digest,
                route_mode,
                proxy_ctx.clone(),
            )
            .await
        }
        _ => errors::not_implemented().into_response(),
    }
}

pub(crate) use crate::registry::validation::is_valid_repo_name;

#[cfg(test)]
#[path = "handlers/tests.rs"]
mod tests;

async fn oci_extension_discover(method: Method) -> Response {
    match method {
        Method::GET | Method::HEAD => {
            let payload = serde_json::json!({
                "extensions": [
                    {
                        "name": "_oci",
                        "description": "OCI standard extension discovery",
                        "url": "https://github.com/opencontainers/distribution-spec/blob/main/extensions/README.md",
                        "endpoints": ["discover"]
                    },
                    {
                        "name": "referrers",
                        "description": "OCI 1.1 Referrers API",
                        "url": "https://github.com/opencontainers/distribution-spec/blob/v1.1.0/spec.md#listing-referrers",
                        "endpoints": ["referrers"]
                    }
                ]
            });
            let bytes = serde_json::to_vec(&payload).unwrap();
            let mut headers = registry_headers();
            headers.insert("Content-Type", "application/json".parse().unwrap());
            headers.insert("Content-Length", bytes.len().to_string().parse().unwrap());
            if method == Method::HEAD {
                (StatusCode::OK, headers).into_response()
            } else {
                (StatusCode::OK, headers, Body::from(bytes)).into_response()
            }
        }
        _ => errors::not_implemented().into_response(),
    }
}

pub(crate) fn registry_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Docker-Distribution-API-Version",
        "registry/2.0".parse().unwrap(),
    );
    headers
}
