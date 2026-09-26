//! Blob read/head/delete handlers (parse/delegate/format). Split out of the historical
//! monolithic dispatcher (remediation R4, KI-26).

use crate::{AppState, ProxyContext, registry::digest::Digest};
use axum::{
    body::Body,
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;

use super::errors;
use crate::request_routing::V2RouteMode;

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
