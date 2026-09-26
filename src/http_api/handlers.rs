use crate::registry::validation::is_valid_tag;
use crate::{AppState, ProxyContext, registry::digest::Digest, storage::StorageError};
use axum::{
    body::Body,
    extract::Query,
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::StreamExt;
use headers::HeaderMapExt;
use std::collections::HashMap;
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use url::form_urlencoded;

use super::errors;
use crate::request_routing::{V2RouteMode, v2_route_mode_for_request};

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
        if state.config.auth_strategy == crate::config::AuthStrategy::Token
            || state.config.auth_strategy == crate::config::AuthStrategy::Both
        {
            if let Ok(v) = http::HeaderValue::from_str(&bearer) {
                resp.headers_mut().append(http::header::WWW_AUTHENTICATE, v);
            }
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

pub async fn blob_by_digest(
    state: AppState,
    headers: &HeaderMap,
    method: Method,
    name: &str,
    digest_str: &str,
    route_mode: V2RouteMode,
    proxy_ctx: Option<ProxyContext>,
) -> Response {
    let digest = match Digest::parse(digest_str) {
        Ok(d) => d,
        Err(_) => return errors::digest_invalid().into_response(),
    };

    let proxy_only = route_mode == V2RouteMode::ProxyOnly;

    match method {
        Method::DELETE => {
            if proxy_only {
                return StatusCode::METHOD_NOT_ALLOWED.into_response();
            }
            match state.blob_service.delete_repo_blob(name, &digest).await {
                Ok(crate::blob_delete_safety::BlobDeleteResult::Success) => {
                    (StatusCode::ACCEPTED, registry_headers()).into_response()
                }
                Ok(crate::blob_delete_safety::BlobDeleteResult::NotFound) => {
                    errors::blob_unknown().into_response()
                }
                Ok(crate::blob_delete_safety::BlobDeleteResult::InUse { message }) => {
                    errors::blob_in_use(&message).into_response()
                }
                Err(crate::application::BlobMutationError::InvalidRepoName { .. }) => {
                    errors::name_invalid().into_response()
                }
                Err(_) => errors::internal_error().into_response(),
            }
        }
        Method::HEAD => {
            match state
                .blob_read_service
                .head_blob(name, &digest, proxy_ctx.as_ref(), proxy_only)
                .await
            {
                Ok(meta) => {
                    let mut headers = registry_headers();
                    headers.insert(
                        "Docker-Content-Digest",
                        meta.digest.as_str().parse().unwrap(),
                    );
                    headers.insert("Content-Type", meta.media_type.parse().unwrap());
                    headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                    (StatusCode::OK, headers).into_response()
                }
                Err(crate::application::BlobReadError::NotFound) => {
                    errors::blob_unknown().into_response()
                }
                Err(crate::application::BlobReadError::InvalidRepoName { .. }) => {
                    errors::name_invalid().into_response()
                }
                Err(crate::application::BlobReadError::InvalidDigest(_)) => {
                    errors::digest_invalid().into_response()
                }
                Err(_) => errors::internal_error().into_response(),
            }
        }
        Method::GET => {
            match state
                .blob_read_service
                .get_blob(name, &digest, proxy_ctx.as_ref(), proxy_only)
                .await
            {
                Ok(out) => {
                    if let Some(range_header) = headers
                        .get(axum::http::header::RANGE)
                        .and_then(|v| v.to_str().ok())
                    {
                        if let Some(spec) = range_header.strip_prefix("bytes=") {
                            let parts: Vec<&str> = spec.split('-').collect();
                            if parts.len() == 2 {
                                let start = parts[0].parse::<u64>();
                                let end = parts[1].parse::<u64>();
                                match (start, end) {
                                    (Ok(s), Ok(e)) if s <= e && e < out.size => {
                                        let mut collected =
                                            Vec::with_capacity((e - s + 1) as usize);
                                        let mut stream = out.stream;
                                        let mut curr = 0u64;
                                        while let Some(chunk) = stream.next().await {
                                            if let Ok(c) = chunk {
                                                let c_len = c.len() as u64;
                                                let c_start = curr;
                                                let c_end = curr + c_len;
                                                if c_end > s && c_start <= e {
                                                    let slice_s = if s > c_start {
                                                        (s - c_start) as usize
                                                    } else {
                                                        0
                                                    };
                                                    let slice_e = if e + 1 < c_end {
                                                        (e + 1 - c_start) as usize
                                                    } else {
                                                        c.len()
                                                    };
                                                    collected
                                                        .extend_from_slice(&c[slice_s..slice_e]);
                                                }
                                                curr += c_len;
                                                if curr > e {
                                                    break;
                                                }
                                            }
                                        }
                                        let mut resp_headers = registry_headers();
                                        resp_headers.insert(
                                            "Content-Range",
                                            format!("bytes {s}-{e}/{}", out.size).parse().unwrap(),
                                        );
                                        resp_headers.insert(
                                            "Content-Length",
                                            collected.len().to_string().parse().unwrap(),
                                        );
                                        resp_headers.insert(
                                            "Content-Type",
                                            out.media_type.parse().unwrap(),
                                        );
                                        resp_headers.insert(
                                            "Docker-Content-Digest",
                                            out.digest.as_str().parse().unwrap(),
                                        );
                                        return (
                                            StatusCode::PARTIAL_CONTENT,
                                            resp_headers,
                                            Body::from(collected),
                                        )
                                            .into_response();
                                    }
                                    _ => {
                                        let mut resp_headers = registry_headers();
                                        resp_headers.insert(
                                            "Content-Range",
                                            format!("bytes */{}", out.size).parse().unwrap(),
                                        );
                                        return (StatusCode::RANGE_NOT_SATISFIABLE, resp_headers)
                                            .into_response();
                                    }
                                }
                            }
                        }
                    }
                    let body = Body::from_stream(out.stream);
                    let mut headers = registry_headers();
                    headers.insert(
                        "Docker-Content-Digest",
                        out.digest.as_str().parse().unwrap(),
                    );
                    headers.insert("Content-Type", out.media_type.parse().unwrap());
                    headers.insert("Content-Length", out.size.to_string().parse().unwrap());
                    (StatusCode::OK, headers, body).into_response()
                }
                Err(crate::application::BlobReadError::NotFound) => {
                    errors::blob_unknown().into_response()
                }
                Err(crate::application::BlobReadError::InvalidRepoName { .. }) => {
                    errors::name_invalid().into_response()
                }
                Err(crate::application::BlobReadError::InvalidDigest(_)) => {
                    errors::digest_invalid().into_response()
                }
                Err(_) => errors::internal_error().into_response(),
            }
        }
        _ => errors::method_not_allowed("GET, HEAD, DELETE").into_response(),
    }
}

pub async fn manifest_by_reference(
    state: AppState,
    headers: &HeaderMap,
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

    let is_digest = Digest::parse(reference).is_ok();
    if !is_digest && !is_valid_tag(reference) {
        // Only GET/HEAD/DELETE are routed here (PUT validates inside
        // publish_manifest and keeps 400 TAG_INVALID). A reference that is
        // neither a well-formed digest nor a well-formed tag cannot name any
        // manifest, so per the distribution spec (and its conformance suite)
        // these methods treat it as unknown rather than as a client error.
        return errors::manifest_unknown().into_response();
    }

    let proxy_only = route_mode == V2RouteMode::ProxyOnly;

    match method {
        Method::DELETE => {
            if proxy_only {
                return StatusCode::METHOD_NOT_ALLOWED.into_response();
            }
            if is_digest {
                let digest = match Digest::parse(reference) {
                    Ok(d) => d,
                    Err(_) => return errors::digest_invalid().into_response(),
                };
                match state.manifest_service.delete_manifest(name, &digest).await {
                    Ok(_) => (StatusCode::ACCEPTED, registry_headers()).into_response(),
                    Err(crate::application::ManifestMutationError::ManifestNotFound) => {
                        errors::manifest_unknown().into_response()
                    }
                    Err(crate::application::ManifestMutationError::InvalidRepoName { .. }) => {
                        errors::name_invalid().into_response()
                    }
                    Err(crate::application::ManifestMutationError::Storage(
                        StorageError::Unsupported,
                    )) => errors::not_implemented().into_response(),
                    Err(_) => errors::internal_error().into_response(),
                }
            } else {
                match state
                    .manifest_service
                    .delete_tag(name, reference, state.config.allow_tag_overwrite)
                    .await
                {
                    Ok(_) => (StatusCode::ACCEPTED, registry_headers()).into_response(),
                    Err(crate::application::ManifestMutationError::TagNotFound) => {
                        errors::manifest_unknown().into_response()
                    }
                    Err(crate::application::ManifestMutationError::InvalidRepoName { .. }) => {
                        errors::name_invalid().into_response()
                    }
                    Err(crate::application::ManifestMutationError::InvalidTag) => {
                        errors::tag_invalid().into_response()
                    }
                    Err(crate::application::ManifestMutationError::TagImmutable) => {
                        errors::denied("tag is immutable and cannot be deleted").into_response()
                    }
                    Err(crate::application::ManifestMutationError::Storage(
                        StorageError::Unsupported,
                    )) => errors::not_implemented().into_response(),
                    Err(_) => errors::internal_error().into_response(),
                }
            }
        }
        Method::HEAD => {
            match state
                .manifest_read_service
                .head_manifest(
                    name,
                    reference,
                    proxy_ctx.as_ref(),
                    proxy_only,
                    request_host,
                )
                .await
            {
                Ok(meta) => {
                    let mut headers = registry_headers();
                    headers.insert(
                        "Docker-Content-Digest",
                        meta.digest.as_str().parse().unwrap(),
                    );
                    headers.insert("Content-Type", meta.media_type.parse().unwrap());
                    headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                    if let Some(subject) = meta.subject {
                        headers.insert("OCI-Subject", subject.as_str().parse().unwrap());
                    }
                    (StatusCode::OK, headers).into_response()
                }
                Err(
                    crate::application::ManifestReadError::NotFound
                    | crate::application::ManifestReadError::TagNotFound,
                ) => errors::manifest_unknown().into_response(),
                Err(crate::application::ManifestReadError::InvalidRepoName { .. }) => {
                    errors::name_invalid().into_response()
                }
                Err(crate::application::ManifestReadError::InvalidTag(_)) => {
                    // Read path: an invalid tag cannot exist, so report unknown (404).
                    errors::manifest_unknown().into_response()
                }
                Err(crate::application::ManifestReadError::Storage(StorageError::Unsupported)) => {
                    errors::not_implemented().into_response()
                }
                Err(crate::application::ManifestReadError::Storage(
                    StorageError::InsufficientStorage,
                )) => errors::insufficient_storage().into_response(),
                Err(_) => errors::internal_error().into_response(),
            }
        }
        Method::GET => {
            match state
                .manifest_read_service
                .get_manifest(
                    name,
                    reference,
                    proxy_ctx.as_ref(),
                    proxy_only,
                    request_host,
                )
                .await
            {
                Ok(out) => {
                    let mut resp_headers = registry_headers();
                    resp_headers.insert(
                        "Docker-Content-Digest",
                        out.digest.as_str().parse().unwrap(),
                    );
                    resp_headers.insert("ETag", format!("\"{}\"", out.digest).parse().unwrap());
                    resp_headers.insert("Content-Type", out.media_type.parse().unwrap());
                    resp_headers.insert("Content-Length", out.size.to_string().parse().unwrap());
                    if let Some(subject) = out.subject {
                        resp_headers.insert("OCI-Subject", subject.as_str().parse().unwrap());
                    }
                    if let Some(inm) = headers
                        .get(axum::http::header::IF_NONE_MATCH)
                        .and_then(|v| v.to_str().ok())
                    {
                        let trimmed = inm.trim().trim_matches('"');
                        if trimmed == out.digest.as_str() || inm.trim() == "*" {
                            return (StatusCode::NOT_MODIFIED, resp_headers).into_response();
                        }
                    }
                    (StatusCode::OK, resp_headers, Body::from(out.payload)).into_response()
                }
                Err(
                    crate::application::ManifestReadError::NotFound
                    | crate::application::ManifestReadError::TagNotFound,
                ) => errors::manifest_unknown().into_response(),
                Err(crate::application::ManifestReadError::InvalidRepoName { .. }) => {
                    errors::name_invalid().into_response()
                }
                Err(crate::application::ManifestReadError::InvalidTag(_)) => {
                    // Read path: an invalid tag cannot exist, so report unknown (404).
                    errors::manifest_unknown().into_response()
                }
                Err(crate::application::ManifestReadError::Storage(StorageError::Unsupported)) => {
                    errors::not_implemented().into_response()
                }
                Err(crate::application::ManifestReadError::Storage(
                    StorageError::InsufficientStorage,
                )) => errors::insufficient_storage().into_response(),
                Err(_) => errors::internal_error().into_response(),
            }
        }
        _ => errors::method_not_allowed("GET, HEAD, DELETE").into_response(),
    }
}

pub(crate) use crate::registry::validation::is_valid_repo_name;

#[cfg(test)]
#[path = "handlers/tests.rs"]
mod tests;

#[allow(dead_code)]
fn detect_media_type_from_manifest(bytes: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    value
        .get("mediaType")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

async fn manifest_put(
    state: AppState,
    headers: &HeaderMap,
    name: &str,
    reference: &str,
    body: Body,
) -> Response {
    // Limit concurrency so memory usage stays bounded under load.
    let _permit = match state.buffered_body_sem.clone().acquire_owned().await {
        Ok(p) => p,
        Err(_) => return errors::internal_error().into_response(),
    };

    let content_length = headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<usize>().ok());

    let limit = state
        .config
        .max_request_body_bytes
        .min(crate::manifest_lifecycle::MAX_MANIFEST_SIZE);

    let (idle_timeout, min_rate) = state.current_stream_guard_params();
    let audit_only =
        state.config.slow_connection_policy == crate::config::SlowConnectionPolicy::AuditOnly;
    let bytes = match read_body_limited(
        body,
        content_length,
        limit,
        idle_timeout,
        Duration::from_secs(state.config.upload_rate_grace_period_secs),
        Duration::from_secs(state.config.upload_rate_window_secs),
        min_rate,
        audit_only,
    )
    .await
    {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let declared_media_type = headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let req = crate::application::PublishManifestRequest::new(
        name,
        reference,
        bytes,
        declared_media_type,
        state.config.allow_tag_overwrite,
    );

    match state.manifest_service.publish_manifest(req).await {
        Ok(published) => {
            let mut headers = registry_headers();
            headers.insert(
                "Docker-Content-Digest",
                published.digest.as_str().parse().unwrap(),
            );
            headers.insert("Content-Type", published.media_type.parse().unwrap());
            headers.insert(
                "Location",
                format!("/v2/{name}/manifests/{}", published.digest.as_str())
                    .parse()
                    .unwrap(),
            );
            if let Some(subject) = published.subject {
                headers.insert("OCI-Subject", subject.as_str().parse().unwrap());
            }
            (StatusCode::CREATED, headers).into_response()
        }
        Err(err) => manifest_mutation_error_to_response(err),
    }
}

fn manifest_mutation_error_to_response(err: crate::application::ManifestMutationError) -> Response {
    use crate::application::ManifestMutationError;
    use crate::storage::StorageError;
    match err {
        ManifestMutationError::InvalidRepoName { .. } => errors::name_invalid().into_response(),
        ManifestMutationError::EmptyPayload
        | ManifestMutationError::PayloadTooLarge
        | ManifestMutationError::InvalidManifest(_) => errors::manifest_invalid().into_response(),
        ManifestMutationError::Unverified(reason) => {
            errors::manifest_unverified(&reason.to_string())
        }
        ManifestMutationError::UnsupportedMediaType(_) => errors::not_implemented().into_response(),
        ManifestMutationError::MissingBlob(blob_d) => {
            errors::manifest_blob_unknown(&blob_d).into_response()
        }
        ManifestMutationError::MissingManifest(manifest_d) => {
            errors::manifest_blob_unknown(&manifest_d).into_response()
        }
        ManifestMutationError::InvalidTag => errors::tag_invalid().into_response(),
        ManifestMutationError::TagAlreadyExists => {
            (StatusCode::CONFLICT, registry_headers(), Body::empty()).into_response()
        }
        ManifestMutationError::TagImmutable => {
            errors::denied("tag is immutable and cannot be deleted").into_response()
        }
        ManifestMutationError::TagNotFound | ManifestMutationError::ManifestNotFound => {
            errors::manifest_unknown().into_response()
        }
        ManifestMutationError::DigestMismatch { .. } => {
            errors::manifest_unverified("manifest digest mismatch")
        }
        ManifestMutationError::Storage(StorageError::InsufficientStorage) => {
            errors::insufficient_storage().into_response()
        }
        ManifestMutationError::Storage(StorageError::Unsupported) => {
            errors::not_implemented().into_response()
        }
        ManifestMutationError::Storage(StorageError::DigestMismatch) => {
            errors::digest_invalid().into_response()
        }
        _ => errors::internal_error().into_response(),
    }
}

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

async fn read_body_limited(
    body: axum::body::Body,
    content_length: Option<usize>,
    limit: usize,
    idle_timeout: Duration,
    grace_period: Duration,
    window_duration: Duration,
    min_bytes_per_sec: u64,
    audit_only: bool,
) -> Result<Bytes, Response> {
    let mut is_oversized = false;
    if let Some(len) = content_length {
        if len > limit {
            is_oversized = true;
        }
    }

    let initial_capacity = content_length.unwrap_or(0).min(limit).min(1024 * 1024);
    let mut buf: Vec<u8> = Vec::with_capacity(initial_capacity);
    let mut stream = crate::http_api::stream_guard::MonitoredUploadStream::new(
        body.into_data_stream(),
        idle_timeout,
        grace_period,
        window_duration,
        min_bytes_per_sec,
        audit_only,
    );
    while let Some(next) = stream.next().await {
        let chunk = match next {
            Ok(c) => c,
            Err(crate::http_api::stream_guard::StreamGuardError::IdleTimeout(_))
            | Err(crate::http_api::stream_guard::StreamGuardError::InsufficientThroughput {
                ..
            }) => {
                return Err(errors::request_timeout(
                    "request stream timed out or throughput too low",
                )
                .into_response());
            }
            Err(_) => return Err(errors::internal_error().into_response()),
        };
        if chunk.is_empty() {
            continue;
        }
        if buf.len().saturating_add(chunk.len()) > limit {
            is_oversized = true;
        } else {
            buf.extend_from_slice(&chunk);
        }
    }
    if is_oversized {
        return Err(errors::manifest_invalid().into_response());
    }
    Ok(Bytes::from(buf))
}

async fn upload_create(
    state: AppState,
    headers: &HeaderMap,
    method: Method,
    name: &str,
    query: &HashMap<String, String>,
    body: Body,
) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }

    if method != Method::POST {
        return errors::method_not_allowed("POST");
    }

    if let Some(token) = crate::auth::bearer_token_from_headers(headers) {
        if let Ok(claims) = crate::security::verify_bearer_token_bound_with_keys(
            &state.config.token_signing_keys,
            token,
            &state.config.token_service,
            state.config.token_ttl_secs,
        ) {
            if !crate::security::token_allows_repo_action(
                &claims,
                name,
                crate::security::RepoAction::Push,
            ) {
                return crate::auth::unauthorized_registry_challenge(
                    &state,
                    Some(name),
                    Some(crate::security::RepoAction::Push),
                );
            }
        } else {
            return crate::auth::unauthorized_registry_challenge(
                &state,
                Some(name),
                Some(crate::security::RepoAction::Push),
            );
        }
    } else if state.config.auth_strategy != crate::config::AuthStrategy::Token {
        if let Some(headers::Authorization(basic)) =
            headers.typed_get::<headers::Authorization<headers::authorization::Basic>>()
        {
            let Ok(canonical_target) = crate::registry::CanonicalRepoName::parse(name) else {
                return errors::name_invalid().into_response();
            };
            if !crate::auth::verify_direct_basic_access(
                &state.config,
                basic.username(),
                basic.password(),
                &canonical_target,
                "push",
            ) {
                return crate::auth::unauthorized_registry_challenge(
                    &state,
                    Some(name),
                    Some(crate::security::RepoAction::Push),
                );
            }
        } else if state.config.push_username.is_some()
            || state.config.robots.enabled
            || state.config.users.enabled
        {
            return crate::auth::unauthorized_registry_challenge(
                &state,
                Some(name),
                Some(crate::security::RepoAction::Push),
            );
        }
    } else if state.config.push_username.is_some()
        || state.config.robots.enabled
        || state.config.users.enabled
    {
        return crate::auth::unauthorized_registry_challenge(
            &state,
            Some(name),
            Some(crate::security::RepoAction::Push),
        );
    }

    let req_content_len = headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    // Configured upload limit check
    if state.config.max_upload_bytes > 0 && req_content_len > state.config.max_upload_bytes {
        return errors::payload_too_large();
    }

    // Cross-repository blob mount:
    //   POST /v2/<name>/blobs/uploads/?mount=<digest>[&from=<repo>]
    if let Some(mount_str) = query.get("mount").map(|s| s.as_str()) {
        let digest = match Digest::parse(mount_str) {
            Ok(d) => d,
            Err(_) => return errors::digest_invalid().into_response(),
        };

        let mut from_repo = query.get("from").map(|s| s.as_str());
        if let Some(src_repo) = from_repo {
            if !is_valid_repo_name(src_repo) {
                return errors::name_invalid().into_response();
            }

            let from_is_private = state.config.is_repo_private(src_repo);
            let allows_pull = if let Some(token) = crate::auth::bearer_token_from_headers(headers) {
                if let Ok(claims) = crate::security::verify_bearer_token_bound_with_keys(
                    &state.config.token_signing_keys,
                    token,
                    &state.config.token_service,
                    state.config.token_ttl_secs,
                ) {
                    crate::security::token_allows_repo_action(
                        &claims,
                        src_repo,
                        crate::security::RepoAction::Pull,
                    )
                } else {
                    false
                }
            } else if let Some(headers::Authorization(basic)) =
                headers.typed_get::<headers::Authorization<headers::authorization::Basic>>()
            {
                if let Ok(canonical_src) = crate::registry::CanonicalRepoName::parse(src_repo) {
                    crate::auth::verify_direct_basic_access(
                        &state.config,
                        basic.username(),
                        basic.password(),
                        &canonical_src,
                        "pull",
                    )
                } else {
                    false
                }
            } else {
                !from_is_private && state.config.anonymous_pull
            };

            if !allows_pull {
                from_repo = None;
            }
        }

        match state
            .blob_service
            .cross_mount(name, from_repo, &digest)
            .await
        {
            Ok(crate::application::CrossMountResult::Mounted(fin)) => {
                let mut resp_headers = registry_headers();
                resp_headers.insert(
                    "Location",
                    format!("/v2/{name}/blobs/{}", fin.digest.as_str())
                        .parse()
                        .unwrap(),
                );
                resp_headers.insert(
                    "Docker-Content-Digest",
                    fin.digest.as_str().parse().unwrap(),
                );
                resp_headers.insert("Content-Length", "0".parse().unwrap());
                return (StatusCode::CREATED, resp_headers).into_response();
            }
            Ok(crate::application::CrossMountResult::Fallback(start)) => {
                let mut resp_headers = registry_headers();
                let location = format!(
                    "/v2/{name}/blobs/uploads/{}?_state={}",
                    start.session.uuid, start.state_token
                );
                resp_headers.insert("Location", location.parse().unwrap());
                resp_headers.insert("Docker-Upload-UUID", start.session.uuid.parse().unwrap());
                if let Some(min_len) = state.config.upload_chunk_min_bytes {
                    resp_headers
                        .insert("OCI-Chunk-Min-Length", min_len.to_string().parse().unwrap());
                }
                return (StatusCode::ACCEPTED, resp_headers).into_response();
            }
            Err(err) => return blob_mutation_error_to_response(err),
        }
    }

    // Monolithic upload (body on POST): POST /v2/<name>/blobs/uploads/?digest=<digest>
    // Conformance allows this to either create an upload session (202) or create the blob (201).
    // If the blob already exists in this repository, we return 201.
    if let Some(digest_str) = query.get("digest").map(|s| s.as_str()) {
        let digest = match Digest::parse(digest_str) {
            Ok(d) => d,
            Err(_) => return errors::digest_invalid().into_response(),
        };

        // Stream monolithic upload (no buffering of multi-GB body).
        let (idle_timeout, min_rate) = state.current_stream_guard_params();
        let audit_only =
            state.config.slow_connection_policy == crate::config::SlowConnectionPolicy::AuditOnly;
        let mut stream = crate::http_api::stream_guard::MonitoredUploadStream::new(
            body.into_data_stream(),
            idle_timeout,
            Duration::from_secs(state.config.upload_rate_grace_period_secs),
            Duration::from_secs(state.config.upload_rate_window_secs),
            min_rate,
            audit_only,
        );
        let first = stream.next().await;
        let Some(first) = first else {
            // No body -> behave like normal upload creation.
            return match state.blob_service.start_upload(name).await {
                Ok(start) => {
                    let mut headers = registry_headers();
                    let location = format!(
                        "/v2/{name}/blobs/uploads/{}?_state={}",
                        start.session.uuid, start.state_token
                    );
                    headers.insert("Location", location.parse().unwrap());
                    headers.insert("Docker-Upload-UUID", start.session.uuid.parse().unwrap());
                    if let Some(min_len) = state.config.upload_chunk_min_bytes {
                        headers
                            .insert("OCI-Chunk-Min-Length", min_len.to_string().parse().unwrap());
                    }
                    (StatusCode::ACCEPTED, headers).into_response()
                }
                Err(err) => blob_mutation_error_to_response(err),
            };
        };

        let first_chunk = match first {
            Ok(c) => c,
            Err(_) => {
                return errors::request_timeout("upload aborted while reading request body")
                    .into_response();
            }
        };

        let first_stream = futures_util::stream::once(async move { Ok(first_chunk) });
        let rest = stream.map(|res| {
            res.map_err(|e| match e {
                crate::http_api::stream_guard::StreamGuardError::IdleTimeout(_) => {
                    crate::storage::upload_session::UploadStreamError::IdleTimeout
                }
                crate::http_api::stream_guard::StreamGuardError::InsufficientThroughput {
                    ..
                } => crate::storage::upload_session::UploadStreamError::RateTooLow,
                crate::http_api::stream_guard::StreamGuardError::BodyError(err) => {
                    crate::storage::upload_session::UploadStreamError::Io(std::io::Error::other(
                        err,
                    ))
                }
            })
        });
        let body_stream: crate::storage::upload_session::UploadByteStream =
            Box::pin(first_stream.chain(rest));

        match state
            .blob_service
            .monolithic_upload(name, &digest, Some(body_stream))
            .await
        {
            Ok(crate::application::MonolithicUploadResult::AlreadyFinalized(res))
            | Ok(crate::application::MonolithicUploadResult::Created(res)) => {
                let mut headers = registry_headers();
                headers.insert(
                    "Location",
                    format!("/v2/{name}/blobs/{}", res.digest.as_str())
                        .parse()
                        .unwrap(),
                );
                headers.insert(
                    "Docker-Content-Digest",
                    res.digest.as_str().parse().unwrap(),
                );
                headers.insert("Content-Length", res.size.to_string().parse().unwrap());
                (StatusCode::CREATED, headers).into_response()
            }
            Ok(crate::application::MonolithicUploadResult::SessionStarted(start)) => {
                let mut headers = registry_headers();
                let location = format!(
                    "/v2/{name}/blobs/uploads/{}?_state={}",
                    start.session.uuid, start.state_token
                );
                headers.insert("Location", location.parse().unwrap());
                headers.insert("Docker-Upload-UUID", start.session.uuid.parse().unwrap());
                (StatusCode::ACCEPTED, headers).into_response()
            }
            Err(err) => blob_mutation_error_to_response(err),
        }
    } else {
        match state.blob_service.start_upload(name).await {
            Ok(start) => {
                let mut headers = registry_headers();
                let location = format!(
                    "/v2/{name}/blobs/uploads/{}?_state={}",
                    start.session.uuid, start.state_token
                );
                headers.insert("Location", location.parse().unwrap());
                headers.insert("Docker-Upload-UUID", start.session.uuid.parse().unwrap());
                if let Some(min_len) = state.config.upload_chunk_min_bytes {
                    headers.insert("OCI-Chunk-Min-Length", min_len.to_string().parse().unwrap());
                }
                (StatusCode::ACCEPTED, headers).into_response()
            }
            Err(err) => blob_mutation_error_to_response(err),
        }
    }
}

fn blob_mutation_error_to_response(err: crate::application::BlobMutationError) -> Response {
    use crate::application::BlobMutationError;
    use crate::http_api::upload_state::StateTokenError;
    use crate::storage::StorageError;
    match err {
        BlobMutationError::InvalidRepoName { .. } => errors::name_invalid().into_response(),
        BlobMutationError::SessionNotFound => errors::blob_upload_unknown().into_response(),
        BlobMutationError::BlobNotFound => errors::blob_unknown().into_response(),
        BlobMutationError::BlobInUse(msg) => errors::blob_in_use(&msg).into_response(),
        BlobMutationError::StateToken(StateTokenError::Missing) => {
            errors::blob_upload_invalid("missing _state parameter").into_response()
        }
        BlobMutationError::StateToken(StateTokenError::OffsetMismatch { expected, .. }) => {
            let mut resp = errors::range_invalid("storage size does not match state offset");
            if expected > 0 {
                resp.headers_mut()
                    .insert("Range", format!("0-{}", expected - 1).parse().unwrap());
            }
            resp.into_response()
        }
        BlobMutationError::StateToken(_) => {
            errors::blob_upload_invalid("invalid _state parameter").into_response()
        }
        BlobMutationError::OffsetMismatch { current, .. } => {
            let mut resp = errors::range_invalid("storage size does not match state offset");
            if current > 0 {
                resp.headers_mut()
                    .insert("Range", format!("0-{}", current - 1).parse().unwrap());
            }
            resp.into_response()
        }
        BlobMutationError::RangeInvalid(msg) => errors::range_invalid(&msg).into_response(),
        BlobMutationError::DigestMismatch { .. } => errors::digest_invalid().into_response(),
        BlobMutationError::SizeInvalid(msg) => errors::size_invalid(&msg).into_response(),
        BlobMutationError::TooLarge => {
            errors::blob_upload_invalid("upload too large").into_response()
        }
        BlobMutationError::Conflict => {
            (StatusCode::CONFLICT, "concurrent operation conflict").into_response()
        }
        BlobMutationError::MonolithicDisallowed => errors::blob_upload_invalid(
            "monolithic uploads are disabled; use PATCH-based chunked upload",
        )
        .into_response(),
        BlobMutationError::InvalidPreparedHandle => errors::internal_error().into_response(),
        BlobMutationError::Storage(StorageError::NotFound) => {
            errors::blob_upload_unknown().into_response()
        }
        BlobMutationError::Storage(StorageError::DigestMismatch) => {
            errors::digest_invalid().into_response()
        }
        BlobMutationError::Storage(StorageError::TooLarge) => {
            errors::blob_upload_invalid("upload too large").into_response()
        }
        BlobMutationError::Storage(StorageError::InsufficientStorage) => {
            errors::insufficient_storage().into_response()
        }
        BlobMutationError::Storage(StorageError::Unsupported) => {
            errors::not_implemented().into_response()
        }
        BlobMutationError::Storage(_) => errors::internal_error().into_response(),
        BlobMutationError::Ledger(crate::application::LedgerError::Storage(
            StorageError::NotFound,
        )) => errors::blob_unknown().into_response(),
        BlobMutationError::Ledger(crate::application::LedgerError::Storage(
            StorageError::InsufficientStorage,
        )) => errors::insufficient_storage().into_response(),
        BlobMutationError::Ledger(crate::application::LedgerError::Storage(
            StorageError::Unsupported,
        )) => errors::not_implemented().into_response(),
        BlobMutationError::Ledger(crate::application::LedgerError::Storage(
            StorageError::InvalidRepoName(_),
        )) => errors::name_invalid().into_response(),
        BlobMutationError::Ledger(_) => errors::internal_error().into_response(),
        BlobMutationError::Stream(
            crate::storage::upload_session::UploadStreamError::IdleTimeout,
        ) => errors::request_timeout("upload aborted while reading request body").into_response(),
        BlobMutationError::Stream(
            crate::storage::upload_session::UploadStreamError::RateTooLow,
        ) => errors::request_timeout("upload aborted due to low transfer rate").into_response(),
        BlobMutationError::Stream(crate::storage::upload_session::UploadStreamError::Io(
            io_err,
        )) => errors::request_timeout(&format!("stream io error: {io_err}")).into_response(),
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
        let _ = axum::body::to_bytes(body, usize::MAX).await;
        return errors::name_invalid().into_response();
    }

    if uuid::Uuid::parse_str(uuid).is_err() {
        let _ = axum::body::to_bytes(body, usize::MAX).await;
        return errors::blob_upload_unknown().into_response();
    }

    let required_action = match method {
        Method::DELETE | Method::PATCH | Method::PUT => crate::security::RepoAction::Push,
        _ => crate::security::RepoAction::Pull,
    };

    if let Some(token) = crate::auth::bearer_token_from_headers(req_headers) {
        if let Ok(claims) = crate::security::verify_bearer_token_bound_with_keys(
            &state.config.token_signing_keys,
            token,
            &state.config.token_service,
            state.config.token_ttl_secs,
        ) {
            if !crate::security::token_allows_repo_action(&claims, name, required_action) {
                let _ = axum::body::to_bytes(body, usize::MAX).await;
                return crate::auth::unauthorized_registry_challenge(
                    &state,
                    Some(name),
                    Some(required_action),
                );
            }
        } else {
            return crate::auth::unauthorized_registry_challenge(
                &state,
                Some(name),
                Some(required_action),
            );
        }
    } else if state.config.auth_strategy != crate::config::AuthStrategy::Token {
        if let Some(headers::Authorization(basic)) =
            req_headers.typed_get::<headers::Authorization<headers::authorization::Basic>>()
        {
            let Ok(canonical_target) = crate::registry::CanonicalRepoName::parse(name) else {
                let _ = axum::body::to_bytes(body, usize::MAX).await;
                return errors::name_invalid().into_response();
            };
            if !crate::auth::verify_direct_basic_access(
                &state.config,
                basic.username(),
                basic.password(),
                &canonical_target,
                required_action.as_str(),
            ) {
                let _ = axum::body::to_bytes(body, usize::MAX).await;
                return crate::auth::unauthorized_registry_challenge(
                    &state,
                    Some(name),
                    Some(required_action),
                );
            }
        } else if (required_action != crate::security::RepoAction::Pull
            || !state.config.anonymous_pull
            || state.config.is_repo_private(name))
            && (state.config.push_username.is_some()
                || state.config.robots.enabled
                || state.config.users.enabled)
        {
            let _ = axum::body::to_bytes(body, usize::MAX).await;
            return crate::auth::unauthorized_registry_challenge(
                &state,
                Some(name),
                Some(required_action),
            );
        }
    } else if (required_action != crate::security::RepoAction::Pull
        || !state.config.anonymous_pull
        || state.config.is_repo_private(name))
        && (state.config.push_username.is_some()
            || state.config.robots.enabled
            || state.config.users.enabled)
    {
        let _ = axum::body::to_bytes(body, usize::MAX).await;
        return crate::auth::unauthorized_registry_challenge(
            &state,
            Some(name),
            Some(required_action),
        );
    }

    match method {
        Method::GET | Method::HEAD => {
            let _ = axum::body::to_bytes(body, usize::MAX).await;
            match state
                .blob_service
                .get_upload_status(name, uuid, query.get("_state").map(|s| s.as_str()))
                .await
            {
                Ok(st) => {
                    let mut headers = registry_headers();
                    let location =
                        format!("/v2/{name}/blobs/uploads/{uuid}?_state={}", st.state_token);
                    headers.insert("Location", location.parse().unwrap());
                    headers.insert("Docker-Upload-UUID", uuid.parse().unwrap());
                    if st.offset > 0 {
                        headers.insert("Range", format!("0-{}", st.offset - 1).parse().unwrap());
                    }
                    (StatusCode::NO_CONTENT, headers).into_response()
                }
                Err(err) => blob_mutation_error_to_response(err),
            }
        }
        Method::DELETE => {
            let _ = axum::body::to_bytes(body, usize::MAX).await;
            match state
                .blob_service
                .abort_upload(name, uuid, query.get("_state").map(|s| s.as_str()))
                .await
            {
                Ok(()) => {
                    let mut headers = registry_headers();
                    headers.insert("Docker-Upload-UUID", uuid.parse().unwrap());
                    (StatusCode::NO_CONTENT, headers).into_response()
                }
                Err(err) => blob_mutation_error_to_response(err),
            }
        }
        Method::PATCH => {
            let range = parse_content_range(req_headers);
            let content_len = req_headers
                .get(http::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());

            let Some(state_param) = query.get("_state").map(|s| s.as_str()) else {
                let _ = axum::body::to_bytes(body, usize::MAX).await;
                return errors::blob_upload_invalid("missing _state parameter").into_response();
            };

            let (idle_timeout, min_rate) = state.current_stream_guard_params();
            let audit_only = state.config.slow_connection_policy
                == crate::config::SlowConnectionPolicy::AuditOnly;
            let stream = crate::http_api::stream_guard::MonitoredUploadStream::new(
                body.into_data_stream(),
                idle_timeout,
                Duration::from_secs(state.config.upload_rate_grace_period_secs),
                Duration::from_secs(state.config.upload_rate_window_secs),
                min_rate,
                audit_only,
            )
            .map(|res| {
                res.map_err(|e| match e {
                    crate::http_api::stream_guard::StreamGuardError::IdleTimeout(_) => {
                        crate::storage::upload_session::UploadStreamError::IdleTimeout
                    }
                    crate::http_api::stream_guard::StreamGuardError::InsufficientThroughput {
                        ..
                    } => crate::storage::upload_session::UploadStreamError::RateTooLow,
                    crate::http_api::stream_guard::StreamGuardError::BodyError(err) => {
                        crate::storage::upload_session::UploadStreamError::Io(
                            std::io::Error::other(err),
                        )
                    }
                })
            });

            match state
                .blob_service
                .append_chunk(
                    name,
                    uuid,
                    state_param,
                    range,
                    content_len,
                    Box::pin(stream),
                )
                .await
            {
                Ok(app) => {
                    let mut headers = registry_headers();
                    let patch_location = format!(
                        "/v2/{name}/blobs/uploads/{}?_state={}",
                        app.session.uuid, app.state_token
                    );
                    headers.insert("Location", patch_location.parse().unwrap());
                    headers.insert("Docker-Upload-UUID", app.session.uuid.parse().unwrap());
                    if app.new_offset > 0 {
                        headers.insert(
                            "Range",
                            format!("0-{}", app.new_offset - 1).parse().unwrap(),
                        );
                    }
                    if let Some(min_len) = state.config.upload_chunk_min_bytes {
                        headers
                            .insert("OCI-Chunk-Min-Length", min_len.to_string().parse().unwrap());
                    }
                    (StatusCode::ACCEPTED, headers).into_response()
                }
                Err(err) => blob_mutation_error_to_response(err),
            }
        }
        Method::PUT => {
            let Some(digest_str) = query.get("digest").map(|s| s.as_str()) else {
                let _ = axum::body::to_bytes(body, usize::MAX).await;
                return errors::digest_invalid().into_response();
            };
            let digest = match Digest::parse(digest_str) {
                Ok(d) => d,
                Err(_) => {
                    let _ = axum::body::to_bytes(body, usize::MAX).await;
                    return errors::digest_invalid().into_response();
                }
            };

            let range = parse_content_range(req_headers);
            let (idle_timeout, min_rate) = state.current_stream_guard_params();
            let audit_only = state.config.slow_connection_policy
                == crate::config::SlowConnectionPolicy::AuditOnly;
            let mut stream = crate::http_api::stream_guard::MonitoredUploadStream::new(
                body.into_data_stream(),
                idle_timeout,
                Duration::from_secs(state.config.upload_rate_grace_period_secs),
                Duration::from_secs(state.config.upload_rate_window_secs),
                min_rate,
                audit_only,
            );
            let first = stream.next().await;
            let trailing_stream: Option<crate::storage::upload_session::UploadByteStream> =
                if let Some(first_res) = first {
                    let chunk = match first_res {
                        Ok(c) => c,
                        Err(_) => {
                            return errors::request_timeout(
                                "upload aborted while reading request body",
                            )
                            .into_response();
                        }
                    };
                    if chunk.is_empty() {
                        None
                    } else {
                        let first_stream = futures_util::stream::once(async move { Ok(chunk) });
                        let rest = stream.map(|res| {
                            res.map_err(|e| match e {
                                crate::http_api::stream_guard::StreamGuardError::IdleTimeout(_) => {
                                    crate::storage::upload_session::UploadStreamError::IdleTimeout
                                }
                                crate::http_api::stream_guard::StreamGuardError::InsufficientThroughput { .. } => {
                                    crate::storage::upload_session::UploadStreamError::RateTooLow
                                }
                                crate::http_api::stream_guard::StreamGuardError::BodyError(err) => {
                                    crate::storage::upload_session::UploadStreamError::Io(
                                        std::io::Error::other(err),
                                    )
                                }
                            })
                        });
                        Some(Box::pin(first_stream.chain(rest)))
                    }
                } else {
                    None
                };

            match state
                .blob_service
                .finalize_upload(
                    name,
                    uuid,
                    query.get("_state").map(|s| s.as_str()),
                    range,
                    trailing_stream,
                    &digest,
                )
                .await
            {
                Ok(fin) => {
                    let mut headers = registry_headers();
                    headers.insert(
                        "Location",
                        format!("/v2/{name}/blobs/{}", fin.digest.as_str())
                            .parse()
                            .unwrap(),
                    );
                    headers.insert(
                        "Docker-Content-Digest",
                        fin.digest.as_str().parse().unwrap(),
                    );
                    headers.insert("Content-Length", fin.size.to_string().parse().unwrap());
                    (StatusCode::CREATED, headers).into_response()
                }
                Err(err) => blob_mutation_error_to_response(err),
            }
        }
        _ => errors::method_not_allowed("GET, HEAD, PATCH, PUT, DELETE"),
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
