use crate::{registry::digest::Digest, storage::StorageError, AppState};
use axum::{
    body::Body,
    extract::Query,
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use sha2::Digest as _;
use std::collections::HashMap;
use tokio_util::io::ReaderStream;

use super::errors;

pub async fn ping() -> impl IntoResponse {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Docker-Distribution-API-Version",
        "registry/2.0".parse().unwrap(),
    );
    (StatusCode::OK, headers)
}

pub async fn v2_dispatch(
    State(state): State<AppState>,
    method: Method,
    Query(query): Query<HashMap<String, String>>,
    Path(rest): Path<String>,
    body: Bytes,
) -> Response {
    let segments: Vec<&str> = rest.split('/').filter(|s| !s.is_empty()).collect();
    // Uploads:
    //   POST /v2/<name>/blobs/uploads/
    //   PATCH/PUT/GET /v2/<name>/blobs/uploads/<uuid>
    if segments.len() >= 2
        && segments[segments.len() - 1] == "uploads"
        && segments[segments.len() - 2] == "blobs"
    {
        let name = segments[..segments.len() - 2].join("/");
        return upload_create(state, method, &name).await;
    }

    if segments.len() >= 3
        && segments[segments.len() - 2] == "uploads"
        && segments[segments.len() - 3] == "blobs"
    {
        let uuid = segments[segments.len() - 1];
        let name = segments[..segments.len() - 3].join("/");
        return upload_session(state, method, &name, uuid, query, body).await;
    }

    if segments.len() >= 2 && segments[segments.len() - 2] == "manifests" {
        let reference = segments[segments.len() - 1];
        let name = segments[..segments.len() - 2].join("/");
        if method == Method::PUT {
            return manifest_put(state, &name, reference, body).await;
        }
        return manifest_by_reference(state, method, &name, reference).await;
    }

    // /v2/<name>/blobs/<digest>
    // Avoid catching /blobs/uploads by requiring the digest format.
    if segments.len() >= 2
        && segments[segments.len() - 2] == "blobs"
        && segments[segments.len() - 1].contains(':')
    {
        let digest_str = segments[segments.len() - 1];
        let name = segments[..segments.len() - 2].join("/");
        return blob_by_digest(state, method, &name, digest_str).await;
    }

    errors::not_implemented().into_response()
}

async fn blob_by_digest(state: AppState, method: Method, name: &str, digest_str: &str) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }

    let digest = match Digest::parse(digest_str) {
        Ok(d) => d,
        Err(_) => return errors::digest_invalid().into_response(),
    };

    match method {
        Method::HEAD => match state.storage.head_blob(&digest).await {
            Ok(meta) => {
                let mut headers = registry_headers();
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                (StatusCode::OK, headers).into_response()
            }
            Err(StorageError::NotFound) => errors::blob_unknown().into_response(),
            Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
            Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
        },
        Method::GET => match state.storage.open_blob(&digest).await {
            Ok((meta, reader)) => {
                let stream = ReaderStream::new(reader);
                let body = Body::from_stream(stream);

                let mut headers = registry_headers();
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                (StatusCode::OK, headers, body).into_response()
            }
            Err(StorageError::NotFound) => errors::blob_unknown().into_response(),
            Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
            Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
        },
        _ => errors::not_implemented().into_response(),
    }
}

async fn manifest_by_reference(
    state: AppState,
    method: Method,
    name: &str,
    reference: &str,
) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }

    // Reference can be a digest or a tag.
    let digest = match Digest::parse(reference) {
        Ok(d) => d,
        Err(_) => match state.storage.resolve_tag(name, reference).await {
            Ok(d) => d,
            Err(StorageError::NotFound) => return errors::manifest_unknown().into_response(),
            Err(StorageError::DigestMismatch) => return errors::internal_error().into_response(),
            Err(StorageError::Unsupported) => return errors::not_implemented().into_response(),
            Err(StorageError::Internal(_)) => return errors::internal_error().into_response(),
        },
    };

    match method {
        Method::HEAD => match state.storage.head_manifest(name, &digest).await {
            Ok(meta) => {
                let mut headers = registry_headers();
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert("Content-Type", meta.media_type.parse().unwrap());
                headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                (StatusCode::OK, headers).into_response()
            }
            Err(StorageError::NotFound) => errors::manifest_unknown().into_response(),
            Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
            Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
        },
        Method::GET => match state.storage.get_manifest(name, &digest).await {
            Ok((meta, bytes)) => {
                let mut headers = registry_headers();
                headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                headers.insert("Content-Type", meta.media_type.parse().unwrap());
                headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                (StatusCode::OK, headers, Body::from(bytes)).into_response()
            }
            Err(StorageError::NotFound) => errors::manifest_unknown().into_response(),
            Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
            Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
        },
        _ => errors::not_implemented().into_response(),
    }
}

fn is_valid_repo_name(name: &str) -> bool {
    if name.is_empty() || name.starts_with('/') {
        return false;
    }
    for segment in name.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return false;
        }
    }
    name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-'))
}

fn is_valid_tag(tag: &str) -> bool {
    if tag.is_empty() || tag.len() > 128 {
        return false;
    }
    if tag.contains('/') || tag.contains(char::is_whitespace) {
        return false;
    }
    let mut chars = tag.chars();
    let Some(first) = chars.next() else { return false };
    if !(first.is_ascii_alphanumeric() || first == '_') {
        return false;
    }
    tag.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
}

fn detect_media_type_from_manifest(bytes: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    value
        .get("mediaType")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn is_supported_manifest_media_type(media_type: &str) -> bool {
    matches!(
        media_type,
        "application/vnd.oci.image.manifest.v1+json"
            | "application/vnd.docker.distribution.manifest.v2+json"
    )
}

async fn manifest_put(state: AppState, name: &str, reference: &str, bytes: Bytes) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }
    if bytes.is_empty() {
        return errors::manifest_invalid().into_response();
    }

    let media_type = detect_media_type_from_manifest(&bytes)
        .unwrap_or_else(|| "application/vnd.oci.image.manifest.v1+json".to_string());
    if !is_supported_manifest_media_type(&media_type) {
        return errors::not_implemented().into_response();
    }

    // Compute manifest digest over the raw bytes.
    let mut hasher = sha2::Sha256::new();
    hasher.update(&bytes);
    let digest_hex = hex::encode(hasher.finalize());
    let computed = Digest::parse(&format!("sha256:{digest_hex}")).expect("computed sha256 is valid");

    // If reference is a digest, it must match the computed digest.
    if let Ok(ref_digest) = Digest::parse(reference) {
        if ref_digest.hex() != computed.hex() {
            return errors::digest_invalid().into_response();
        }
    } else {
        // Otherwise treat it as a tag.
        if !is_valid_tag(reference) {
            return errors::tag_invalid().into_response();
        }
        if !state.config.allow_tag_overwrite {
            match state.storage.resolve_tag(name, reference).await {
                Ok(_) => return (StatusCode::CONFLICT, registry_headers(), Body::empty()).into_response(),
                Err(StorageError::NotFound) => {}
                Err(StorageError::Unsupported) => return errors::not_implemented().into_response(),
                Err(StorageError::DigestMismatch) | Err(StorageError::Internal(_)) => {
                    return errors::internal_error().into_response();
                }
            }
        }
    }

    let meta = match state.storage.put_manifest(name, &computed, bytes).await {
        Ok(m) => m,
        Err(StorageError::Unsupported) => return errors::not_implemented().into_response(),
        Err(StorageError::NotFound) => return errors::internal_error().into_response(),
        Err(StorageError::DigestMismatch) => return errors::digest_invalid().into_response(),
        Err(StorageError::Internal(_)) => return errors::internal_error().into_response(),
    };

    // If reference is a tag, update tag pointer.
    if Digest::parse(reference).is_err() {
        if let Err(err) = state.storage.set_tag(name, reference, &computed).await {
            return match err {
                StorageError::Unsupported => errors::not_implemented().into_response(),
                StorageError::NotFound => errors::internal_error().into_response(),
                StorageError::DigestMismatch => errors::internal_error().into_response(),
                StorageError::Internal(_) => errors::internal_error().into_response(),
            };
        }
    }

    let mut headers = registry_headers();
    headers.insert("Docker-Content-Digest", computed.as_str().parse().unwrap());
    headers.insert("Content-Type", meta.media_type.parse().unwrap());
    headers.insert("Location", format!("/v2/{name}/manifests/{}", reference).parse().unwrap());
    (StatusCode::CREATED, headers).into_response()
}

fn registry_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Docker-Distribution-API-Version",
        "registry/2.0".parse().unwrap(),
    );
    headers
}

async fn upload_create(state: AppState, method: Method, name: &str) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }

    if method != Method::POST {
        return errors::not_implemented().into_response();
    }

    match state.storage.create_upload().await {
        Ok(meta) => {
            let mut headers = registry_headers();
            let location = format!("/v2/{name}/blobs/uploads/{}", meta.uuid);
            headers.insert("Location", location.parse().unwrap());
            headers.insert("Docker-Upload-UUID", meta.uuid.parse().unwrap());
            (StatusCode::ACCEPTED, headers).into_response()
        }
        Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
        Err(StorageError::Internal(_)) | Err(StorageError::DigestMismatch) | Err(StorageError::NotFound) => {
            errors::internal_error().into_response()
        }
    }
}

async fn upload_session(
    state: AppState,
    method: Method,
    name: &str,
    uuid: &str,
    query: HashMap<String, String>,
    body: Bytes,
) -> Response {
    if !is_valid_repo_name(name) {
        return errors::name_invalid().into_response();
    }

    let location = format!("/v2/{name}/blobs/uploads/{uuid}");

    match method {
        Method::GET => match state.storage.upload_status(uuid).await {
            Ok(meta) => {
                let mut headers = registry_headers();
                headers.insert("Location", location.parse().unwrap());
                headers.insert("Docker-Upload-UUID", meta.uuid.parse().unwrap());
                if meta.offset > 0 {
                    headers.insert("Range", format!("0-{}", meta.offset - 1).parse().unwrap());
                }
                (StatusCode::NO_CONTENT, headers).into_response()
            }
            Err(StorageError::NotFound) => errors::blob_upload_unknown().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
            Err(StorageError::Internal(_)) | Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
        },
        Method::PATCH => match state.storage.append_upload(uuid, body).await {
            Ok(meta) => {
                let mut headers = registry_headers();
                headers.insert("Location", location.parse().unwrap());
                headers.insert("Docker-Upload-UUID", meta.uuid.parse().unwrap());
                if meta.offset > 0 {
                    headers.insert("Range", format!("0-{}", meta.offset - 1).parse().unwrap());
                }
                (StatusCode::ACCEPTED, headers).into_response()
            }
            Err(StorageError::NotFound) => errors::blob_upload_unknown().into_response(),
            Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
            Err(StorageError::Internal(_)) | Err(StorageError::DigestMismatch) => errors::internal_error().into_response(),
        },
        Method::PUT => {
            let Some(digest_str) = query.get("digest").map(|s| s.as_str()) else {
                return errors::digest_invalid().into_response();
            };
            let digest = match Digest::parse(digest_str) {
                Ok(d) => d,
                Err(_) => return errors::digest_invalid().into_response(),
            };
            match state.storage.finalize_upload(uuid, &digest).await {
                Ok(meta) => {
                    let mut headers = registry_headers();
                    headers.insert("Location", format!("/v2/{name}/blobs/{}", digest.as_str()).parse().unwrap());
                    headers.insert("Docker-Content-Digest", digest.as_str().parse().unwrap());
                    headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
                    (StatusCode::CREATED, headers).into_response()
                }
                Err(StorageError::NotFound) => errors::blob_upload_unknown().into_response(),
                Err(StorageError::DigestMismatch) => errors::digest_invalid().into_response(),
                Err(StorageError::Unsupported) => errors::not_implemented().into_response(),
                Err(StorageError::Internal(_)) => errors::internal_error().into_response(),
            }
        }
        _ => errors::not_implemented().into_response(),
    }
}
