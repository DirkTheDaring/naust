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
        return errors::tag_invalid().into_response();
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
                    errors::tag_invalid().into_response()
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
                    errors::tag_invalid().into_response()
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
mod tests {
    use crate::AppState;
    use crate::http_api::admin::{
        AdminGcDeleteRequest, AdminGcPlanRequest, AdminGcQuarantineRequest, admin_gc_delete,
        admin_gc_plan, admin_gc_quarantine,
    };
    use crate::http_api::auth_token::{
        TokenRejection, decide_token_scopes_for_request, parse_scopes, sanitize_token_scopes,
        service_param_is_valid, token_scope_requests_repo_action, wants_push_from_token_scopes,
    };
    use crate::http_api::catalog::meta_catalog;
    use crate::registry::validation::{is_valid_repo_name, is_valid_tag};
    use crate::storage::Storage;
    use axum::extract::{Json, State};
    use axum::http::{HeaderMap, StatusCode};
    use headers::{Authorization, HeaderMapExt};
    use http_body_util::BodyExt;
    use std::sync::Arc;

    fn with_admin_creds(mut cfg: Config) -> Config {
        cfg.admin_api = crate::config::AdminApiConfig {
            enabled: true,
            username: Some("admin".to_string()),
            password: Some("secret".to_string()),
        };
        cfg
    }

    fn admin_headers_ok() -> HeaderMap {
        let mut headers = HeaderMap::new();
        let auth = Authorization::basic("admin", "secret");
        headers.typed_insert(auth);
        headers
    }

    fn test_app_state(
        cfg: Arc<crate::config::Config>,
        storage: Arc<crate::storage::fs::FsStorage>,
        gc_service: Option<Arc<crate::gc_service::GcService>>,
    ) -> AppState {
        AppState::new_test(cfg, storage, gc_service)
    }

    #[tokio::test]
    async fn admin_gc_requires_auth() {
        let cfg = Arc::new(with_admin_creds(minimal_config_for_token_tests()));
        let storage = Arc::new(crate::storage::fs::FsStorage::new(
            cfg.fs_root.clone(),
            cfg.max_upload_bytes,
        ));
        let state = test_app_state(cfg, storage, None);

        let req = AdminGcPlanRequest::default();

        let resp = admin_gc_plan(State(state), HeaderMap::new(), Json(req)).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_gc_returns_503_when_service_missing() {
        let cfg = Arc::new(with_admin_creds(minimal_config_for_token_tests()));
        let storage = Arc::new(crate::storage::fs::FsStorage::new(
            cfg.fs_root.clone(),
            cfg.max_upload_bytes,
        ));
        let state = test_app_state(cfg, storage, None);

        let req = AdminGcPlanRequest::default();

        let resp = admin_gc_plan(State(state), admin_headers_ok(), Json(req)).await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn admin_gc_maps_already_running_to_conflict() {
        let fs_root =
            std::env::temp_dir().join(format!("registry-rust-admin-gc-{}", uuid::Uuid::new_v4()));
        let _ = std::fs::create_dir_all(&fs_root);
        let ref_index_path = fs_root.join("ref-index");
        let _ = std::fs::create_dir_all(&ref_index_path);

        let mut cfg = minimal_config_for_token_tests();
        cfg.fs_root = fs_root.clone();
        cfg.ref_index.path = ref_index_path.clone();
        let cfg = Arc::new(with_admin_creds(cfg));

        let storage_raw = Arc::new(crate::storage::fs::FsStorage::new(
            cfg.fs_root.clone(),
            cfg.max_upload_bytes,
        ));
        let idx = Arc::new(crate::blob_ref_index::BlobRefIndex::open(ref_index_path).expect("idx"));
        idx.rebuild(&storage_raw).await.expect("rebuild");
        let service = Arc::new(crate::gc_service::GcService::new(
            cfg.clone(),
            storage_raw.clone(),
            idx,
            crate::consistency::ConsistencyCoordinator::new(),
        ));
        let service_for_state = service.clone();
        let held = service.test_try_lock().expect("lock");

        let state = test_app_state(cfg, storage_raw, Some(service_for_state));

        let req = AdminGcPlanRequest::default();

        let resp = admin_gc_plan(State(state), admin_headers_ok(), Json(req)).await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        drop(held);
        let _ = std::fs::remove_dir_all(&fs_root);
    }

    #[tokio::test]
    async fn admin_gc_quarantine_blocked_when_kill_switch_off() {
        let fs_root =
            std::env::temp_dir().join(format!("registry-rust-admin-gc-{}", uuid::Uuid::new_v4()));
        let _ = std::fs::create_dir_all(&fs_root);
        let ref_index_path = fs_root.join("ref-index");
        let _ = std::fs::create_dir_all(&ref_index_path);

        let mut cfg = minimal_config_for_token_tests();
        cfg.fs_root = fs_root.clone();
        cfg.ref_index.path = ref_index_path.clone();
        cfg.blob_gc_enabled = false;
        let cfg = Arc::new(with_admin_creds(cfg));

        let storage_raw = Arc::new(crate::storage::fs::FsStorage::new(
            cfg.fs_root.clone(),
            cfg.max_upload_bytes,
        ));
        let idx = Arc::new(crate::blob_ref_index::BlobRefIndex::open(ref_index_path).expect("idx"));
        idx.rebuild(&storage_raw).await.expect("rebuild");
        let service = Arc::new(crate::gc_service::GcService::new(
            cfg.clone(),
            storage_raw.clone(),
            idx,
            crate::consistency::ConsistencyCoordinator::new(),
        ));

        let state = test_app_state(cfg, storage_raw, Some(service));

        let resp = admin_gc_quarantine(
            State(state),
            admin_headers_ok(),
            Json(AdminGcQuarantineRequest::default()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        let _ = std::fs::remove_dir_all(&fs_root);
    }

    #[tokio::test]
    async fn admin_gc_delete_blocked_when_delete_gate_off() {
        let fs_root =
            std::env::temp_dir().join(format!("registry-rust-admin-gc-{}", uuid::Uuid::new_v4()));
        let _ = std::fs::create_dir_all(&fs_root);
        let ref_index_path = fs_root.join("ref-index");
        let _ = std::fs::create_dir_all(&ref_index_path);

        let mut cfg = minimal_config_for_token_tests();
        cfg.fs_root = fs_root.clone();
        cfg.ref_index.path = ref_index_path.clone();
        cfg.blob_gc_enabled = true;
        cfg.blob_gc_enable_delete = false;
        let cfg = Arc::new(with_admin_creds(cfg));

        let storage_raw = Arc::new(crate::storage::fs::FsStorage::new(
            cfg.fs_root.clone(),
            cfg.max_upload_bytes,
        ));
        let idx = Arc::new(crate::blob_ref_index::BlobRefIndex::open(ref_index_path).expect("idx"));
        idx.rebuild(&storage_raw).await.expect("rebuild");
        let service = Arc::new(crate::gc_service::GcService::new(
            cfg.clone(),
            storage_raw.clone(),
            idx,
            crate::consistency::ConsistencyCoordinator::new(),
        ));

        let state = test_app_state(cfg, storage_raw, Some(service));

        let resp = admin_gc_delete(
            State(state),
            admin_headers_ok(),
            Json(AdminGcDeleteRequest::default()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        let _ = std::fs::remove_dir_all(&fs_root);
    }

    #[tokio::test]
    async fn meta_catalog_include_tags_adds_tags_and_tag_count() {
        let fs_root =
            std::env::temp_dir().join(format!("registry-rust-meta-tags-{}", uuid::Uuid::new_v4()));
        let _ = std::fs::create_dir_all(&fs_root);

        // Create repos + tags on disk.
        let repo_dir = fs_root
            .join("repos")
            .join("org1")
            .join("repoa")
            .join("tags");
        let _ = std::fs::create_dir_all(&repo_dir);
        std::fs::write(repo_dir.join("latest"), "sha256:deadbeef\n").unwrap();
        std::fs::write(repo_dir.join("v1"), "sha256:cafebabe\n").unwrap();

        let mut cfg = minimal_config_for_token_tests();
        cfg.fs_root = fs_root.clone();
        cfg.catalog_requires_auth = false;
        let cfg = Arc::new(cfg);

        let storage = Arc::new(crate::storage::fs::FsStorage::new(
            cfg.fs_root.clone(),
            cfg.max_upload_bytes,
        ));

        let state = test_app_state(cfg, storage, None);

        let mut q = std::collections::HashMap::new();
        q.insert("include_tags".to_string(), "1".to_string());

        let resp = meta_catalog(State(state), HeaderMap::new(), axum::extract::Query(q)).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let repos = v.get("repositories").and_then(|x| x.as_array()).unwrap();
        assert_eq!(repos.len(), 1);
        let repo0 = repos[0].as_object().unwrap();
        assert_eq!(
            repo0.get("name").and_then(|x| x.as_str()),
            Some("org1/repoa")
        );
        assert_eq!(repo0.get("tag_count").and_then(|x| x.as_u64()), Some(2));

        let tags = repo0.get("tags").and_then(|x| x.as_array()).unwrap();
        let tags: Vec<&str> = tags.iter().filter_map(|x| x.as_str()).collect();
        assert_eq!(tags, vec!["latest", "v1"]);

        let _ = std::fs::remove_dir_all(&fs_root);
    }

    #[tokio::test]
    async fn meta_catalog_include_platforms_adds_tag_details_with_platforms() {
        let fs_root = std::env::temp_dir().join(format!(
            "registry-rust-meta-platforms-{}",
            uuid::Uuid::new_v4()
        ));
        let _ = std::fs::create_dir_all(&fs_root);

        let repo = "org1/repoa";

        let idx_digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let single_digest =
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let cfg_digest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

        // Tags.
        let tags_dir = fs_root
            .join("repos")
            .join("org1")
            .join("repoa")
            .join("tags");
        let _ = std::fs::create_dir_all(&tags_dir);
        std::fs::write(tags_dir.join("multi"), format!("{idx_digest}\n")).unwrap();
        std::fs::write(tags_dir.join("single"), format!("{single_digest}\n")).unwrap();

        // Manifests.
        let manifests_dir = fs_root
            .join("repos")
            .join("org1")
            .join("repoa")
            .join("manifests");
        let _ = std::fs::create_dir_all(&manifests_dir);

        let idx_hex = idx_digest.split_once(':').unwrap().1;
        let single_hex = single_digest.split_once(':').unwrap().1;

        let idx_manifest = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                    "size": 1,
                    "platform": {"os": "linux", "architecture": "amd64"}
                },
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                    "size": 1,
                    "platform": {"os": "linux", "architecture": "arm64"}
                }
            ]
        });
        std::fs::write(
            manifests_dir.join(idx_hex),
            serde_json::to_vec(&idx_manifest).unwrap(),
        )
        .unwrap();

        let single_manifest = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": cfg_digest,
                "size": 123
            },
            "layers": []
        });
        std::fs::write(
            manifests_dir.join(single_hex),
            serde_json::to_vec(&single_manifest).unwrap(),
        )
        .unwrap();

        // Config blob for single-manifest platform inference.
        let cfg_hex = cfg_digest.split_once(':').unwrap().1;
        let cfg_prefix2 = &cfg_hex[..2];
        let cfg_blob_dir = fs_root.join("blobs").join("sha256").join(cfg_prefix2);
        let _ = std::fs::create_dir_all(&cfg_blob_dir);
        let cfg_json = serde_json::json!({
            "architecture": "amd64",
            "os": "linux"
        });
        std::fs::write(
            cfg_blob_dir.join(cfg_hex),
            serde_json::to_vec(&cfg_json).unwrap(),
        )
        .unwrap();

        let mut cfg = minimal_config_for_token_tests();
        cfg.fs_root = fs_root.clone();
        cfg.catalog_requires_auth = false;
        let cfg = Arc::new(cfg);

        let storage = Arc::new(crate::storage::fs::FsStorage::new(
            cfg.fs_root.clone(),
            cfg.max_upload_bytes,
        ));

        let state = test_app_state(cfg, storage, None);

        let mut q = std::collections::HashMap::new();
        q.insert("include_platforms".to_string(), "1".to_string());

        let resp = meta_catalog(State(state), HeaderMap::new(), axum::extract::Query(q)).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let repos = v.get("repositories").and_then(|x| x.as_array()).unwrap();
        assert_eq!(repos.len(), 1);
        let repo0 = repos[0].as_object().unwrap();
        assert_eq!(repo0.get("name").and_then(|x| x.as_str()), Some(repo));
        assert_eq!(repo0.get("tag_count").and_then(|x| x.as_u64()), Some(2));

        let tag_details = repo0.get("tag_details").and_then(|x| x.as_array()).unwrap();
        assert_eq!(tag_details.len(), 2);

        let mut by_tag: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for d in tag_details {
            let tag = d.get("tag").and_then(|x| x.as_str()).unwrap().to_string();
            let plats = d
                .get("platforms")
                .and_then(|x| x.as_array())
                .unwrap()
                .iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>();
            by_tag.insert(tag, plats);
        }

        assert_eq!(
            by_tag.get("multi").cloned().unwrap(),
            vec!["linux/amd64".to_string(), "linux/arm64".to_string()]
        );
        assert_eq!(
            by_tag.get("single").cloned().unwrap(),
            vec!["linux/amd64".to_string()]
        );

        let _ = std::fs::remove_dir_all(&fs_root);
    }
    use crate::config::{Config, ProxyConfig, ProxyMode, RobotsConfig, UploadPolicyConfig};
    use crate::rbac::Grant;
    use crate::robot_secrets;
    use crate::security;
    use std::net::SocketAddr;
    use std::path::PathBuf;

    #[test]
    fn repo_name_validation() {
        assert!(is_valid_repo_name("library/alpine"));
        assert!(is_valid_repo_name("org.name/repo_name-1"));
        assert!(!is_valid_repo_name(""));
        assert!(!is_valid_repo_name("/leading"));
        assert!(!is_valid_repo_name(".."));
        assert!(!is_valid_repo_name("a/../b"));
        assert!(!is_valid_repo_name("a b"));
        assert!(!is_valid_repo_name("INVALID/UPPERCASE"));
        assert!(!is_valid_repo_name("-invalid-leading-dash"));
        assert!(is_valid_repo_name("valid__double_underscore"));
        assert!(!is_valid_repo_name("invalid___triple_underscore"));
        assert!(!is_valid_repo_name("invalid..dots"));
    }

    #[test]
    fn tag_validation() {
        assert!(is_valid_tag("latest"));
        assert!(is_valid_tag("v1.2.3"));
        assert!(is_valid_tag("_start_ok"));
        assert!(!is_valid_tag(""));
        assert!(!is_valid_tag("has space"));
        assert!(!is_valid_tag("has/slash"));
        assert!(!is_valid_tag("-badstart"));
    }

    #[test]
    fn parse_scopes_normalizes_typ_and_actions() {
        let scopes = parse_scopes("RePoSiToRy:org/repo:PUSH,Pull");
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].typ, "repository");
        assert_eq!(scopes[0].name, "org/repo");
        assert_eq!(
            scopes[0].actions,
            vec!["push".to_string(), "pull".to_string()]
        );
    }

    #[test]
    fn parse_scopes_splits_by_whitespace_into_multiple_items() {
        let scopes = parse_scopes("repository:org/repo:pull  repository:org/repo2:push");
        assert_eq!(scopes.len(), 2);
        assert_eq!(scopes[0].typ, "repository");
        assert_eq!(scopes[0].name, "org/repo");
        assert_eq!(scopes[0].actions, vec!["pull".to_string()]);
        assert_eq!(scopes[1].name, "org/repo2");
        assert_eq!(scopes[1].actions, vec!["push".to_string()]);
    }

    #[test]
    fn sanitize_token_scopes_preserves_catalog_and_repository_scopes() {
        let scopes = parse_scopes("registry:catalog:* repository:org/repo:pull");
        let token_scopes = sanitize_token_scopes(&scopes);
        assert_eq!(token_scopes.len(), 2);
        assert_eq!(token_scopes[0].typ, "registry");
        assert_eq!(token_scopes[0].name, "catalog");
        assert_eq!(token_scopes[1].typ, "repository");
        assert_eq!(token_scopes[1].name, "org/repo");
    }

    #[test]
    fn service_param_validation_allows_missing_and_requires_exact_match() {
        assert!(service_param_is_valid(None, "registry"));
        assert!(service_param_is_valid(Some("registry"), "registry"));
        assert!(!service_param_is_valid(Some("other"), "registry"));
    }

    #[test]
    fn parse_scopes_deduplicates_actions_preserving_order() {
        let scopes = parse_scopes("repository:org/repo:pull,pull,push,pull");
        assert_eq!(scopes.len(), 1);
        assert_eq!(
            scopes[0].actions,
            vec!["pull".to_string(), "push".to_string()]
        );
    }

    #[test]
    fn scope_requests_repo_action_requires_repository_type() {
        let scopes = parse_scopes("registry:catalog:* repository:org/repo:pull");
        assert_eq!(scopes.len(), 2);

        let token_scopes = sanitize_token_scopes(&scopes);
        assert_eq!(token_scopes.len(), 2);

        assert!(!token_scope_requests_repo_action(
            &token_scopes[0],
            security::RepoAction::Push
        ));
    }

    #[test]
    fn sanitize_token_scopes_drops_unknown_actions() {
        let scopes = parse_scopes("repository:org/repo:pull,unknown,push");
        let token_scopes = sanitize_token_scopes(&scopes);
        assert_eq!(token_scopes.len(), 1);
        assert_eq!(
            token_scopes[0].actions,
            vec!["pull".to_string(), "push".to_string()]
        );
    }

    #[test]
    fn sanitize_token_scopes_preserves_delete_action() {
        let scopes = parse_scopes("repository:org/repo:pull,push,delete");
        let token_scopes = sanitize_token_scopes(&scopes);
        assert_eq!(token_scopes.len(), 1);
        assert_eq!(
            token_scopes[0].actions,
            vec!["pull".to_string(), "push".to_string(), "delete".to_string()]
        );
    }

    #[test]
    fn sanitize_token_scopes_drops_empty_scopes() {
        let scopes = parse_scopes("repository:org/repo:unknown");
        let token_scopes = sanitize_token_scopes(&scopes);
        assert!(token_scopes.is_empty());
    }

    #[test]
    fn sanitize_token_scopes_drops_empty_repo_names() {
        let scopes = parse_scopes("repository::pull");
        let token_scopes = sanitize_token_scopes(&scopes);
        assert!(token_scopes.is_empty());
    }

    #[test]
    fn parse_scopes_ignores_whitespace_only() {
        let scopes = parse_scopes("   \t  ");
        assert!(scopes.is_empty());
    }

    #[test]
    fn sanitize_token_scopes_drops_empty_action_lists() {
        let scopes = parse_scopes("repository:org/repo:");
        let token_scopes = sanitize_token_scopes(&scopes);
        assert!(token_scopes.is_empty());
    }

    #[test]
    fn wants_push_ignores_unknown_only_actions() {
        let scopes = parse_scopes("repository:org/repo:delete");
        let token_scopes = sanitize_token_scopes(&scopes);
        assert!(!wants_push_from_token_scopes(&token_scopes));
    }

    #[test]
    fn wants_push_true_only_when_repository_push_present() {
        let scopes = parse_scopes(
            "repository:org/repo:pull repository:org/repo2:pull,push registry:catalog:*:push",
        );
        let token_scopes = sanitize_token_scopes(&scopes);

        assert!(wants_push_from_token_scopes(&token_scopes));
    }

    #[test]
    fn wants_push_false_for_repository_pull_only() {
        let scopes = parse_scopes("repository:org/repo:pull registry:catalog:*:push");
        let token_scopes = sanitize_token_scopes(&scopes);

        assert!(!wants_push_from_token_scopes(&token_scopes));
    }

    fn minimal_config_for_token_tests() -> Config {
        Config {
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 5000)),
            tls_cert_path: None,
            tls_key_path: None,
            tls_acme: None,
            auth_strategy: crate::config::AuthStrategy::Token,
            anonymous_pull: true,
            push_username: None,
            push_password: None,
            push_allow_repos: Some(vec![crate::registry::RepositoryAccessPattern::All]),
            push_actions: vec!["pull".to_string(), "push".to_string()],
            push_implies_delete: false,
            storage_backend: crate::config::StorageBackend::Filesystem,
            fs_root: PathBuf::from("./data"),
            s3_endpoint: None,
            s3_region: None,
            s3_bucket: None,
            s3_prefix: "registry".to_string(),
            s3_single_instance_mode: false,
            s3_lease_duration_secs: 60,
            s3_lease_renewal_interval_secs: 20,
            s3_max_retry_attempts: 3,
            s3_legacy_multipart_cleanup_policy:
                crate::config::LegacyMultipartCleanupPolicy::Disabled,
            upload_receipt_lifetime_secs: 86400,
            gc_pin_duration_secs: 1800,
            ref_index: crate::config::RefIndexConfig {
                enabled: true,
                path: PathBuf::from("./data/ref-index"),
                rebuild_on_start: false,
                auto_rebuild_on_corruption: true,
            },
            allow_tag_overwrite: true,
            automatic_crossmount: false,
            upload_gc_enabled: true,
            upload_gc_interval_secs: 3600,
            upload_gc_max_age_secs: 86400,
            blob_gc_finalize_grace_secs: 72 * 3600,
            blob_gc_enabled: true,
            blob_gc_enable_delete: true,
            blob_gc_default_min_age_secs: 7 * 24 * 3600,
            blob_gc_default_quarantine_delay_secs: 24 * 3600,
            blob_gc_default_max_blobs: 1000,
            blob_gc_default_max_bytes: u64::MAX,
            blob_gc_default_max_seconds: 60,
            blob_gc_schedule_enabled: false,
            blob_gc_schedule_interval_secs: 7 * 24 * 3600,
            admin_api: crate::config::AdminApiConfig {
                enabled: false,
                username: None,
                password: None,
            },
            max_upload_bytes: 5 * 1024 * 1024 * 1024,
            max_request_body_bytes: 32 * 1024 * 1024,
            upload_chunk_min_bytes: None,
            max_concurrent_buffered_requests: 8,
            max_concurrent_requests: 256,
            max_concurrent_upload_requests: 256,
            request_timeout_secs: 300,
            upload_request_timeout_secs: 3600,
            upload_chunk_idle_timeout_secs: 20,
            upload_rate_window_secs: 10,
            upload_rate_grace_period_secs: 15,
            min_upload_bytes_per_sec: 32768,
            header_read_timeout_secs: 10,
            slow_connection_policy: crate::config::SlowConnectionPolicy::Enforce,
            max_connections_per_ip: 50,
            trusted_bypass_cidrs: vec![],
            trusted_proxies: vec![],
            disallow_monolithic_uploads: false,
            upload_policy: UploadPolicyConfig {
                abort_on_error: false,
                abort_on_digest_mismatch: false,
                repo_rules: Vec::new(),
            },
            catalog_requires_auth: false,
            public_url: Some("http://127.0.0.1:5000".to_string()),
            token_service: "registry-rust".to_string(),
            token_signing_key: "test-key".to_string(),
            token_signing_keys: vec![security::TokenSigningKey {
                kid: "default".to_string(),
                key: "test-key".to_string(),
            }],
            token_ttl_secs: 600,
            robots: RobotsConfig::default(),
            users: crate::config::UsersConfig::default(),
            proxy: ProxyConfig {
                enabled: false,
                mode: ProxyMode::Allowlist,
                upstream_base_url: None,
                upstream_username: None,
                upstream_password: None,
                allowed_upstream_hosts: Vec::new(),
                allowed_repo_prefixes: Vec::new(),
                block_private_networks: true,
                redirect_policy: crate::config::RedirectPolicy::AnyPublic,
                max_concurrent_upstream: 16,
                index_path: PathBuf::from("./data/cache/proxy-index"),
                cache_fs_root: None,
                cache_s3_prefix: None,
                gc_interval_secs: 3600,
                scrub_enabled: false,
                scrub_interval_secs: 3600,
                scrub_max_files_per_run: 2000,
                max_cache_bytes: None,
                repo_rules: Vec::new(),
                upstreams: Vec::new(),
                routing_proxy_hosts: Vec::new(),
                routing_trust_x_forwarded_host: false,
            },
        }
    }

    #[test]
    fn token_primary_signing_key_follows_key_order() {
        let mut cfg = minimal_config_for_token_tests();

        cfg.token_signing_keys = vec![
            security::TokenSigningKey {
                kid: "k_new".to_string(),
                key: "new-key".to_string(),
            },
            security::TokenSigningKey {
                kid: "k_old".to_string(),
                key: "old-key".to_string(),
            },
        ];
        assert_eq!(cfg.token_primary_signing_key().kid, "k_new");

        cfg.token_signing_keys.swap(0, 1);
        assert_eq!(cfg.token_primary_signing_key().kid, "k_old");
    }

    #[test]
    fn robot_push_token_is_scoped_by_prefix_grants() {
        let mut cfg = minimal_config_for_token_tests();
        cfg.robots.enabled = true;

        let hash = robot_secrets::hash_robot_secret("s3cr3t").expect("hash");
        cfg.robots.accounts.push(crate::config::RobotAccountConfig {
            name: "ci".to_string(),
            secret_hash: hash,
            grants: vec![
                Grant::try_new("org/", vec!["pull".to_string(), "push".to_string()]).unwrap(),
            ],
            max_ttl_secs: Some(120),
        });

        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "org/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let decision = decide_token_scopes_for_request(
            &cfg,
            &requested,
            Some(("ci".to_string(), "s3cr3t".to_string())),
        )
        .expect("should authorize");

        assert_eq!(decision.subject.as_deref(), Some("robot:ci"));
        assert_eq!(decision.scopes, requested);
        assert_eq!(decision.ttl_secs, 120);
    }

    #[test]
    fn robot_push_token_denied_when_repo_not_in_grants() {
        let mut cfg = minimal_config_for_token_tests();
        cfg.robots.enabled = true;

        let hash = robot_secrets::hash_robot_secret("s3cr3t").expect("hash");
        cfg.robots.accounts.push(crate::config::RobotAccountConfig {
            name: "ci".to_string(),
            secret_hash: hash,
            grants: vec![
                Grant::try_new("org/", vec!["pull".to_string(), "push".to_string()]).unwrap(),
            ],
            max_ttl_secs: None,
        });

        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "other/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let err = decide_token_scopes_for_request(
            &cfg,
            &requested,
            Some(("ci".to_string(), "s3cr3t".to_string())),
        )
        .expect_err("should deny");

        assert_eq!(
            err,
            TokenRejection::Denied("action not allowed by robot policy")
        );
    }

    #[test]
    fn robot_auth_failure_does_not_allow_push_without_legacy_creds() {
        let mut cfg = minimal_config_for_token_tests();
        cfg.robots.enabled = true;

        let hash = robot_secrets::hash_robot_secret("s3cr3t").expect("hash");
        cfg.robots.accounts.push(crate::config::RobotAccountConfig {
            name: "ci".to_string(),
            secret_hash: hash,
            grants: vec![
                Grant::try_new("org/", vec!["pull".to_string(), "push".to_string()]).unwrap(),
            ],
            max_ttl_secs: None,
        });

        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "org/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let err = decide_token_scopes_for_request(
            &cfg,
            &requested,
            Some(("ci".to_string(), "wrong".to_string())),
        )
        .expect_err("should deny");

        assert_eq!(err, TokenRejection::Unauthorized);
    }

    #[test]
    fn legacy_push_creds_work_even_when_robots_enabled() {
        let mut cfg = minimal_config_for_token_tests();
        cfg.robots.enabled = true;
        cfg.push_username = Some("admin".to_string());
        cfg.push_password = Some("pw".to_string());

        // Add a robot too; we should still allow legacy when legacy creds match.
        let hash = robot_secrets::hash_robot_secret("s3cr3t").expect("hash");
        cfg.robots.accounts.push(crate::config::RobotAccountConfig {
            name: "ci".to_string(),
            secret_hash: hash,
            grants: vec![
                Grant::try_new("org/", vec!["pull".to_string(), "push".to_string()]).unwrap(),
            ],
            max_ttl_secs: None,
        });

        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "org/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let decision = decide_token_scopes_for_request(
            &cfg,
            &requested,
            Some(("admin".to_string(), "pw".to_string())),
        )
        .expect("should authorize");

        assert_eq!(decision.subject.as_deref(), Some("admin"));
        assert_eq!(decision.scopes, requested);
    }

    #[test]
    fn user_push_token_is_scoped_by_group_grants() {
        let mut cfg = minimal_config_for_token_tests();
        cfg.users.enabled = true;

        cfg.users.groups.push(crate::config::GroupConfig {
            name: "dev".to_string(),
            grants: vec![
                Grant::try_new("org/", vec!["pull".to_string(), "push".to_string()]).unwrap(),
            ],
        });

        let hash = robot_secrets::hash_robot_secret("pw").expect("hash");
        cfg.users.accounts.push(crate::config::UserAccountConfig {
            name: "alice".to_string(),
            secret_hash: hash,
            groups: vec!["dev".to_string()],
            max_ttl_secs: Some(120),
        });

        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "org/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let decision = decide_token_scopes_for_request(
            &cfg,
            &requested,
            Some(("alice".to_string(), "pw".to_string())),
        )
        .expect("should authorize");

        assert_eq!(decision.subject.as_deref(), Some("user:alice"));
        assert_eq!(decision.scopes, requested);
        assert_eq!(decision.ttl_secs, 120);
    }

    #[test]
    fn user_push_token_denied_when_repo_not_in_group_grants() {
        let mut cfg = minimal_config_for_token_tests();
        cfg.users.enabled = true;

        cfg.users.groups.push(crate::config::GroupConfig {
            name: "dev".to_string(),
            grants: vec![
                Grant::try_new("other/", vec!["pull".to_string(), "push".to_string()]).unwrap(),
            ],
        });

        let hash = robot_secrets::hash_robot_secret("pw").expect("hash");
        cfg.users.accounts.push(crate::config::UserAccountConfig {
            name: "bob".to_string(),
            secret_hash: hash,
            groups: vec!["dev".to_string()],
            max_ttl_secs: None,
        });

        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "org/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let err = decide_token_scopes_for_request(
            &cfg,
            &requested,
            Some(("bob".to_string(), "pw".to_string())),
        )
        .expect_err("should deny");

        assert_eq!(
            err,
            TokenRejection::Denied("action not allowed by user policy")
        );
    }

    #[test]
    fn robot_precedence_on_name_collision_denies_even_if_user_would_allow() {
        let mut cfg = minimal_config_for_token_tests();
        cfg.robots.enabled = true;
        cfg.users.enabled = true;

        // Robot has the colliding name and valid creds but does NOT allow this repo.
        let shared_hash = robot_secrets::hash_robot_secret("pw").expect("hash");
        cfg.robots.accounts.push(crate::config::RobotAccountConfig {
            name: "sam".to_string(),
            secret_hash: shared_hash.clone(),
            grants: vec![
                Grant::try_new("org/", vec!["pull".to_string(), "push".to_string()]).unwrap(),
            ],
            max_ttl_secs: None,
        });

        // User would allow it via group grants, but must not be reached.
        cfg.users.groups.push(crate::config::GroupConfig {
            name: "writers".to_string(),
            grants: vec![
                Grant::try_new("other/", vec!["pull".to_string(), "push".to_string()]).unwrap(),
            ],
        });
        cfg.users.accounts.push(crate::config::UserAccountConfig {
            name: "sam".to_string(),
            secret_hash: shared_hash,
            groups: vec!["writers".to_string()],
            max_ttl_secs: None,
        });

        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "other/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let err = decide_token_scopes_for_request(
            &cfg,
            &requested,
            Some(("sam".to_string(), "pw".to_string())),
        )
        .expect_err("should deny");

        assert_eq!(
            err,
            TokenRejection::Denied("action not allowed by robot policy")
        );
    }

    use sha2::Digest as _;

    async fn setup_upload_test_env() -> (
        AppState,
        tempfile::TempDir,
        String,
        Arc<crate::storage::fs::FsStorage>,
    ) {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut cfg = minimal_config_for_token_tests();
        cfg.fs_root = temp_dir.path().to_path_buf();
        cfg.token_signing_keys = vec![crate::security::TokenSigningKey {
            kid: "default".to_string(),
            key: "test-signing-key".to_string(),
        }];
        let cfg = Arc::new(cfg);
        let storage = Arc::new(crate::storage::fs::FsStorage::new(
            cfg.fs_root.clone(),
            cfg.max_upload_bytes,
        ));
        let upload = storage.create_upload().await.unwrap();
        let state = test_app_state(cfg, storage.clone(), None);
        (state, temp_dir, upload.uuid, storage)
    }

    #[tokio::test]
    async fn test_handler_put_finalize_with_valid_state_accepted() {
        let (state, _tmp, uuid, storage) = setup_upload_test_env().await;
        let repo = "library/test";
        let chunk = b"chunk of 1000 bytes";
        storage
            .append_upload(&uuid, bytes::Bytes::from_static(chunk))
            .await
            .unwrap();

        let key = b"test-signing-key";
        let state_token =
            crate::http_api::upload_state::UploadStateData::new(repo, &uuid, chunk.len() as u64)
                .encode_and_sign(key);

        let mut hasher = sha2::Sha256::new();
        sha2::Digest::update(&mut hasher, chunk);
        let digest = format!("sha256:{}", hex::encode(hasher.finalize()));

        let mut query = std::collections::HashMap::new();
        query.insert("digest".to_string(), digest);
        query.insert("_state".to_string(), state_token);

        let resp = super::upload_session(
            state,
            axum::http::Method::PUT,
            &HeaderMap::new(),
            repo,
            &uuid,
            query,
            axum::body::Body::empty(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_handler_put_finalize_with_stale_offset_rejected() {
        let (state, _tmp, uuid, storage) = setup_upload_test_env().await;
        let repo = "library/test";
        let chunk = b"chunk of 1000 bytes";
        storage
            .append_upload(&uuid, bytes::Bytes::from_static(chunk))
            .await
            .unwrap();

        let key = b"test-signing-key";
        // Stale offset 0 when stored offset is 20
        let state_token = crate::http_api::upload_state::UploadStateData::new(repo, &uuid, 0)
            .encode_and_sign(key);

        let mut hasher = sha2::Sha256::new();
        sha2::Digest::update(&mut hasher, chunk);
        let digest = format!("sha256:{}", hex::encode(hasher.finalize()));

        let mut query = std::collections::HashMap::new();
        query.insert("digest".to_string(), digest);
        query.insert("_state".to_string(), state_token);

        let resp = super::upload_session(
            state,
            axum::http::Method::PUT,
            &HeaderMap::new(),
            repo,
            &uuid,
            query,
            axum::body::Body::empty(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    }

    #[tokio::test]
    async fn test_handler_put_finalize_with_wrong_uuid_rejected() {
        let (state, _tmp, uuid, storage) = setup_upload_test_env().await;
        let repo = "library/test";
        let chunk = b"chunk of 1000 bytes";
        storage
            .append_upload(&uuid, bytes::Bytes::from_static(chunk))
            .await
            .unwrap();

        let key = b"test-signing-key";
        let state_token = crate::http_api::upload_state::UploadStateData::new(
            repo,
            "00000000-0000-0000-0000-000000000000",
            chunk.len() as u64,
        )
        .encode_and_sign(key);

        let mut hasher = sha2::Sha256::new();
        sha2::Digest::update(&mut hasher, chunk);
        let digest = format!("sha256:{}", hex::encode(hasher.finalize()));

        let mut query = std::collections::HashMap::new();
        query.insert("digest".to_string(), digest);
        query.insert("_state".to_string(), state_token);

        let resp = super::upload_session(
            state,
            axum::http::Method::PUT,
            &HeaderMap::new(),
            repo,
            &uuid,
            query,
            axum::body::Body::empty(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_handler_put_finalize_with_wrong_repo_rejected() {
        let (state, _tmp, uuid, storage) = setup_upload_test_env().await;
        let repo = "library/test";
        let chunk = b"chunk of 1000 bytes";
        storage
            .append_upload(&uuid, bytes::Bytes::from_static(chunk))
            .await
            .unwrap();

        let key = b"test-signing-key";
        let state_token = crate::http_api::upload_state::UploadStateData::new(
            "library/other-repo",
            &uuid,
            chunk.len() as u64,
        )
        .encode_and_sign(key);

        let mut hasher = sha2::Sha256::new();
        sha2::Digest::update(&mut hasher, chunk);
        let digest = format!("sha256:{}", hex::encode(hasher.finalize()));

        let mut query = std::collections::HashMap::new();
        query.insert("digest".to_string(), digest);
        query.insert("_state".to_string(), state_token);

        let resp = super::upload_session(
            state,
            axum::http::Method::PUT,
            &HeaderMap::new(),
            repo,
            &uuid,
            query,
            axum::body::Body::empty(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_handler_put_finalize_with_tampered_sig_rejected() {
        let (state, _tmp, uuid, storage) = setup_upload_test_env().await;
        let repo = "library/test";
        let chunk = b"chunk of 1000 bytes";
        storage
            .append_upload(&uuid, bytes::Bytes::from_static(chunk))
            .await
            .unwrap();

        let mut hasher = sha2::Sha256::new();
        sha2::Digest::update(&mut hasher, chunk);
        let digest = format!("sha256:{}", hex::encode(hasher.finalize()));

        let mut query = std::collections::HashMap::new();
        query.insert("digest".to_string(), digest);
        query.insert("_state".to_string(), "invalid.signature_data".to_string());

        let resp = super::upload_session(
            state,
            axum::http::Method::PUT,
            &HeaderMap::new(),
            repo,
            &uuid,
            query,
            axum::body::Body::empty(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_handler_put_finalize_missing_state_accepted_for_monolithic() {
        let (state, _tmp, uuid, _storage) = setup_upload_test_env().await;
        let repo = "library/test";
        let chunk = b"monolithic upload bytes";

        let mut hasher = sha2::Sha256::new();
        sha2::Digest::update(&mut hasher, chunk);
        let digest = format!("sha256:{}", hex::encode(hasher.finalize()));

        let mut query = std::collections::HashMap::new();
        query.insert("digest".to_string(), digest);
        // No _state query param

        let resp = super::upload_session(
            state,
            axum::http::Method::PUT,
            &HeaderMap::new(),
            repo,
            &uuid,
            query,
            axum::body::Body::from(chunk.to_vec()),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_handler_put_finalize_with_final_body_pre_append_offset_validated() {
        let (state, _tmp, uuid, storage) = setup_upload_test_env().await;
        let repo = "library/test";
        let chunk1 = b"chunk1-bytes-";
        let chunk2 = b"chunk2-bytes-final";
        storage
            .append_upload(&uuid, bytes::Bytes::from_static(chunk1))
            .await
            .unwrap();

        let key = b"test-signing-key";
        // Pre-append offset is chunk1.len()
        let state_token =
            crate::http_api::upload_state::UploadStateData::new(repo, &uuid, chunk1.len() as u64)
                .encode_and_sign(key);

        let mut total_bytes = chunk1.to_vec();
        total_bytes.extend_from_slice(chunk2);
        let mut hasher = sha2::Sha256::new();
        sha2::Digest::update(&mut hasher, &total_bytes);
        let digest = format!("sha256:{}", hex::encode(hasher.finalize()));

        let mut query = std::collections::HashMap::new();
        query.insert("digest".to_string(), digest);
        query.insert("_state".to_string(), state_token);

        let resp = super::upload_session(
            state,
            axum::http::Method::PUT,
            &HeaderMap::new(),
            repo,
            &uuid,
            query,
            axum::body::Body::from(chunk2.to_vec()),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_handler_patch_missing_state_rejected() {
        let (state, _tmp, uuid, _storage) = setup_upload_test_env().await;
        let repo = "library/test";

        let resp = super::upload_session(
            state,
            axum::http::Method::PATCH,
            &HeaderMap::new(),
            repo,
            &uuid,
            std::collections::HashMap::new(),
            axum::body::Body::from("data"),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_handler_patch_stale_offset_rejected() {
        let (state, _tmp, uuid, storage) = setup_upload_test_env().await;
        let repo = "library/test";
        storage
            .append_upload(&uuid, bytes::Bytes::from_static(b"existing-1000"))
            .await
            .unwrap();

        let key = b"test-signing-key";
        let state_token = crate::http_api::upload_state::UploadStateData::new(repo, &uuid, 0)
            .encode_and_sign(key);

        let mut query = std::collections::HashMap::new();
        query.insert("_state".to_string(), state_token);

        let resp = super::upload_session(
            state,
            axum::http::Method::PATCH,
            &HeaderMap::new(),
            repo,
            &uuid,
            query,
            axum::body::Body::from("next-chunk"),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    }

    #[tokio::test]
    async fn test_handler_get_session_with_invalid_state_rejected() {
        let (state, _tmp, uuid, _storage) = setup_upload_test_env().await;
        let repo = "library/test";

        let mut query = std::collections::HashMap::new();
        query.insert("_state".to_string(), "invalid-tampered-state".to_string());

        let resp = super::upload_session(
            state,
            axum::http::Method::GET,
            &HeaderMap::new(),
            repo,
            &uuid,
            query,
            axum::body::Body::empty(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_handler_delete_session_with_invalid_state_rejected() {
        let (state, _tmp, uuid, _storage) = setup_upload_test_env().await;
        let repo = "library/test";

        let mut query = std::collections::HashMap::new();
        query.insert("_state".to_string(), "invalid-tampered-state".to_string());

        let resp = super::upload_session(
            state,
            axum::http::Method::DELETE,
            &HeaderMap::new(),
            repo,
            &uuid,
            query,
            axum::body::Body::empty(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_manifest_put_rejects_malformed_layer_digest() {
        let fs_root =
            std::env::temp_dir().join(format!("registry-rust-test-{}", uuid::Uuid::new_v4()));
        let _ = std::fs::create_dir_all(&fs_root);
        let mut cfg = minimal_config_for_token_tests();
        cfg.fs_root = fs_root.clone();
        let cfg = Arc::new(cfg);
        let storage = Arc::new(crate::storage::fs::FsStorage::new(
            cfg.fs_root.clone(),
            cfg.max_upload_bytes,
        ));
        let state = test_app_state(cfg, storage, None);
        let repo = "library/malformed";

        let malformed_manifest = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
                "size": 2
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": "sha256:not-valid-hex-digest",
                    "size": 100
                }
            ]
        });
        let bytes = serde_json::to_vec(&malformed_manifest).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            "application/vnd.oci.image.manifest.v1+json"
                .parse()
                .unwrap(),
        );
        headers.insert(
            axum::http::header::CONTENT_LENGTH,
            bytes.len().to_string().parse().unwrap(),
        );

        let resp = super::manifest_put(
            state,
            &headers,
            repo,
            "latest",
            axum::body::Body::from(bytes),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body_bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let err_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(err_json["errors"][0]["code"], "MANIFEST_INVALID");

        let _ = std::fs::remove_dir_all(&fs_root);
    }

    #[tokio::test]
    async fn test_manifest_put_rejects_malformed_config_structure() {
        let fs_root =
            std::env::temp_dir().join(format!("registry-rust-test-{}", uuid::Uuid::new_v4()));
        let _ = std::fs::create_dir_all(&fs_root);
        let mut cfg = minimal_config_for_token_tests();
        cfg.fs_root = fs_root.clone();
        let cfg = Arc::new(cfg);
        let storage = Arc::new(crate::storage::fs::FsStorage::new(
            cfg.fs_root.clone(),
            cfg.max_upload_bytes,
        ));
        let state = test_app_state(cfg, storage, None);
        let repo = "library/malformed-cfg";

        let malformed_manifest = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": "not_an_object",
            "layers": []
        });
        let bytes = serde_json::to_vec(&malformed_manifest).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            "application/vnd.oci.image.manifest.v1+json"
                .parse()
                .unwrap(),
        );
        headers.insert(
            axum::http::header::CONTENT_LENGTH,
            bytes.len().to_string().parse().unwrap(),
        );

        let resp = super::manifest_put(
            state,
            &headers,
            repo,
            "latest",
            axum::body::Body::from(bytes),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body_bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let err_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(err_json["errors"][0]["code"], "MANIFEST_INVALID");

        let _ = std::fs::remove_dir_all(&fs_root);
    }

    #[tokio::test]
    async fn test_repo_named_quota_or_limited_behaves_normally() {
        let fs_root =
            std::env::temp_dir().join(format!("registry-rust-test-{}", uuid::Uuid::new_v4()));
        let _ = std::fs::create_dir_all(&fs_root);
        let mut cfg = minimal_config_for_token_tests();
        cfg.fs_root = fs_root.clone();
        cfg.max_upload_bytes = 10_000_000; // 10 MB limit
        let cfg = Arc::new(cfg);
        let storage = Arc::new(crate::storage::fs::FsStorage::new(
            cfg.fs_root.clone(),
            cfg.max_upload_bytes,
        ));
        let state = test_app_state(cfg, storage, None);

        // 1. Repo containing "quota" with 2.5 MB (> old 1 MB test-hook) succeeds (202 Accepted)
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_LENGTH,
            "2500000".parse().unwrap(),
        );
        let query = std::collections::HashMap::new();

        let resp_quota = super::upload_create(
            state.clone(),
            &headers,
            axum::http::Method::POST,
            "quota-app",
            &query,
            axum::body::Body::empty(),
        )
        .await;
        assert_eq!(resp_quota.status(), StatusCode::ACCEPTED);

        // 2. Repo containing "limited" with 2.5 MB succeeds (202 Accepted)
        let resp_limited = super::upload_create(
            state.clone(),
            &headers,
            axum::http::Method::POST,
            "limited-repo",
            &query,
            axum::body::Body::empty(),
        )
        .await;
        assert_eq!(resp_limited.status(), StatusCode::ACCEPTED);

        // 3. Exceeding configured max_upload_bytes (e.g. 15 MB > 10 MB) fails with 413 Payload Too Large
        let mut headers_oversize = HeaderMap::new();
        headers_oversize.insert(
            axum::http::header::CONTENT_LENGTH,
            "15000000".parse().unwrap(),
        );
        let resp_oversize = super::upload_create(
            state,
            &headers_oversize,
            axum::http::Method::POST,
            "quota-app",
            &query,
            axum::body::Body::empty(),
        )
        .await;
        assert_eq!(resp_oversize.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let _ = std::fs::remove_dir_all(&fs_root);
    }
}

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
