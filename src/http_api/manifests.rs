//! Manifest read/put/delete handlers (parse/delegate/format). Split out of the historical
//! monolithic dispatcher (remediation R4, KI-26).

use crate::registry::validation::is_valid_tag;
use crate::{AppState, ProxyContext, registry::digest::Digest, storage::StorageError};
use axum::{
    body::Body,
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use std::time::Duration;

use super::errors;
use crate::request_routing::V2RouteMode;

use super::handlers::*;

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
                    .delete_tag(name, reference, state.transfer_policy.allow_tag_overwrite)
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

#[allow(dead_code)]
pub(crate) fn detect_media_type_from_manifest(bytes: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    value
        .get("mediaType")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

pub(crate) async fn manifest_put(
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
    let audit_only = state.transfer_policy.slow_connection_policy
        == crate::config::SlowConnectionPolicy::AuditOnly;
    let bytes = match read_body_limited(
        body,
        content_length,
        limit,
        idle_timeout,
        Duration::from_secs(state.transfer_policy.upload_rate_grace_period_secs),
        Duration::from_secs(state.transfer_policy.upload_rate_window_secs),
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
        state.transfer_policy.allow_tag_overwrite,
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

pub(crate) fn manifest_mutation_error_to_response(
    err: crate::application::ManifestMutationError,
) -> Response {
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
