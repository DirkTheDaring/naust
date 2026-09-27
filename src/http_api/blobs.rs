//! Blob read/head/delete handlers (parse/delegate/format). Split out of the historical
//! monolithic dispatcher (remediation R4, KI-26).

use super::errors;
use crate::request_routing::V2RouteMode;
use crate::{AppState, ProxyContext, registry::digest::Digest};
use axum::{
    body::Body,
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};

use super::handlers::*;

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
            blob_get(
                state,
                headers,
                name,
                &digest,
                proxy_ctx.as_ref(),
                proxy_only,
            )
            .await
        }
        _ => errors::method_not_allowed("GET, HEAD, DELETE").into_response(),
    }
}

async fn blob_get(
    state: AppState,
    headers: &HeaderMap,
    name: &str,
    digest: &Digest,
    proxy_ctx: Option<&ProxyContext>,
    proxy_only: bool,
) -> Response {
    let closed_range = headers
        .get(axum::http::header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|header| header.strip_prefix("bytes="))
        .and_then(|spec| {
            let parts: Vec<&str> = spec.split('-').collect();
            if parts.len() == 2 {
                Some((parts[0].to_string(), parts[1].to_string()))
            } else {
                None
            }
        });

    if let Some((start_text, end_text)) = closed_range {
        let size = match state
            .blob_read_service
            .head_blob(name, digest, proxy_ctx, proxy_only)
            .await
        {
            Ok(meta) => meta.size,
            Err(err) => return blob_read_error(err),
        };
        match (start_text.parse::<u64>(), end_text.parse::<u64>()) {
            (Ok(start), Ok(end)) if start <= end && end < size => {
                match state
                    .blob_read_service
                    .get_blob_range(name, digest, start, end, proxy_ctx, proxy_only)
                    .await
                {
                    Ok(out) => {
                        let span = end - start + 1;
                        let mut resp_headers = registry_headers();
                        resp_headers.insert(
                            "Content-Range",
                            format!("bytes {start}-{end}/{size}").parse().unwrap(),
                        );
                        resp_headers.insert("Content-Length", span.to_string().parse().unwrap());
                        resp_headers.insert("Content-Type", out.media_type.parse().unwrap());
                        resp_headers.insert(
                            "Docker-Content-Digest",
                            out.digest.as_str().parse().unwrap(),
                        );
                        (
                            StatusCode::PARTIAL_CONTENT,
                            resp_headers,
                            Body::from_stream(out.stream),
                        )
                            .into_response()
                    }
                    Err(err) => blob_read_error(err),
                }
            }
            _ => {
                let mut resp_headers = registry_headers();
                resp_headers.insert("Content-Range", format!("bytes */{size}").parse().unwrap());
                (StatusCode::RANGE_NOT_SATISFIABLE, resp_headers).into_response()
            }
        }
    } else {
        match state
            .blob_read_service
            .get_blob(name, digest, proxy_ctx, proxy_only)
            .await
        {
            Ok(out) => {
                let body = Body::from_stream(out.stream);
                let mut resp_headers = registry_headers();
                resp_headers.insert(
                    "Docker-Content-Digest",
                    out.digest.as_str().parse().unwrap(),
                );
                resp_headers.insert("Content-Type", out.media_type.parse().unwrap());
                resp_headers.insert("Content-Length", out.size.to_string().parse().unwrap());
                (StatusCode::OK, resp_headers, body).into_response()
            }
            Err(err) => blob_read_error(err),
        }
    }
}

fn blob_read_error(err: crate::application::BlobReadError) -> Response {
    match err {
        crate::application::BlobReadError::NotFound => errors::blob_unknown().into_response(),
        crate::application::BlobReadError::InvalidRepoName { .. } => {
            errors::name_invalid().into_response()
        }
        crate::application::BlobReadError::InvalidDigest(_) => {
            errors::digest_invalid().into_response()
        }
        _ => errors::internal_error().into_response(),
    }
}
