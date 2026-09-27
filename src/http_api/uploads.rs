//! Blob upload session handlers (parse/delegate/format). Split out of the historical
//! monolithic dispatcher (remediation R4, KI-26).

use crate::{AppState, registry::digest::Digest};
use axum::{
    body::Body,
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::StreamExt;
use headers::HeaderMapExt;
use std::collections::HashMap;
use std::time::Duration;

use super::errors;

use super::handlers::*;

pub(crate) async fn read_body_limited(
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

pub(crate) enum RejectedBody {
    Reuse,
    Close,
}

pub(crate) async fn discard_rejected_body(
    body: axum::body::Body,
    headers: &HeaderMap,
    limit: usize,
) -> RejectedBody {
    if let Some(len) = headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<usize>().ok())
    {
        if len > limit {
            return RejectedBody::Close;
        }
    }
    let mut seen = 0usize;
    let mut stream = body.into_data_stream();
    while let Some(next) = stream.next().await {
        let Ok(chunk) = next else {
            return RejectedBody::Close;
        };
        if chunk.len() > limit.saturating_sub(seen) {
            return RejectedBody::Close;
        }
        seen += chunk.len();
    }
    RejectedBody::Reuse
}

fn finish_reject(mut response: Response, discarded: RejectedBody) -> Response {
    if matches!(discarded, RejectedBody::Close) {
        response.headers_mut().insert(
            http::header::CONNECTION,
            http::HeaderValue::from_static("close"),
        );
    }
    response
}

pub(crate) async fn upload_create(
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
    if state.transfer_policy.max_upload_bytes > 0
        && req_content_len > state.transfer_policy.max_upload_bytes
    {
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
                if let Some(min_len) = state.transfer_policy.upload_chunk_min_bytes {
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
        let audit_only = state.transfer_policy.slow_connection_policy
            == crate::config::SlowConnectionPolicy::AuditOnly;
        let mut stream = crate::http_api::stream_guard::MonitoredUploadStream::new(
            body.into_data_stream(),
            idle_timeout,
            Duration::from_secs(state.transfer_policy.upload_rate_grace_period_secs),
            Duration::from_secs(state.transfer_policy.upload_rate_window_secs),
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
                    if let Some(min_len) = state.transfer_policy.upload_chunk_min_bytes {
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
                if let Some(min_len) = state.transfer_policy.upload_chunk_min_bytes {
                    headers.insert("OCI-Chunk-Min-Length", min_len.to_string().parse().unwrap());
                }
                (StatusCode::ACCEPTED, headers).into_response()
            }
            Err(err) => blob_mutation_error_to_response(err),
        }
    }
}

pub(crate) fn blob_mutation_error_to_response(
    err: crate::application::BlobMutationError,
) -> Response {
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

pub(crate) async fn upload_session(
    state: AppState,
    method: Method,
    req_headers: &HeaderMap,
    name: &str,
    uuid: &str,
    query: HashMap<String, String>,
    body: Body,
) -> Response {
    if !is_valid_repo_name(name) {
        let discarded =
            discard_rejected_body(body, req_headers, state.config.max_request_body_bytes).await;
        return finish_reject(errors::name_invalid().into_response(), discarded);
    }

    if uuid::Uuid::parse_str(uuid).is_err() {
        let discarded =
            discard_rejected_body(body, req_headers, state.config.max_request_body_bytes).await;
        return finish_reject(errors::blob_upload_unknown().into_response(), discarded);
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
                let discarded =
                    discard_rejected_body(body, req_headers, state.config.max_request_body_bytes)
                        .await;
                return finish_reject(
                    crate::auth::unauthorized_registry_challenge(
                        &state,
                        Some(name),
                        Some(required_action),
                    ),
                    discarded,
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
                let discarded =
                    discard_rejected_body(body, req_headers, state.config.max_request_body_bytes)
                        .await;
                return finish_reject(errors::name_invalid().into_response(), discarded);
            };
            if !crate::auth::verify_direct_basic_access(
                &state.config,
                basic.username(),
                basic.password(),
                &canonical_target,
                required_action.as_str(),
            ) {
                let discarded =
                    discard_rejected_body(body, req_headers, state.config.max_request_body_bytes)
                        .await;
                return finish_reject(
                    crate::auth::unauthorized_registry_challenge(
                        &state,
                        Some(name),
                        Some(required_action),
                    ),
                    discarded,
                );
            }
        } else if (required_action != crate::security::RepoAction::Pull
            || !state.config.anonymous_pull
            || state.config.is_repo_private(name))
            && (state.config.push_username.is_some()
                || state.config.robots.enabled
                || state.config.users.enabled)
        {
            let discarded =
                discard_rejected_body(body, req_headers, state.config.max_request_body_bytes).await;
            return finish_reject(
                crate::auth::unauthorized_registry_challenge(
                    &state,
                    Some(name),
                    Some(required_action),
                ),
                discarded,
            );
        }
    } else if (required_action != crate::security::RepoAction::Pull
        || !state.config.anonymous_pull
        || state.config.is_repo_private(name))
        && (state.config.push_username.is_some()
            || state.config.robots.enabled
            || state.config.users.enabled)
    {
        let discarded =
            discard_rejected_body(body, req_headers, state.config.max_request_body_bytes).await;
        return finish_reject(
            crate::auth::unauthorized_registry_challenge(&state, Some(name), Some(required_action)),
            discarded,
        );
    }

    match method {
        Method::GET | Method::HEAD => {
            let discarded =
                discard_rejected_body(body, req_headers, state.config.max_request_body_bytes).await;
            let response = match state
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
            };
            finish_reject(response, discarded)
        }
        Method::DELETE => {
            let discarded =
                discard_rejected_body(body, req_headers, state.config.max_request_body_bytes).await;
            let response = match state
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
            };
            finish_reject(response, discarded)
        }
        Method::PATCH => {
            let range = parse_content_range(req_headers);
            let content_len = req_headers
                .get(http::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());

            let Some(state_param) = query.get("_state").map(|s| s.as_str()) else {
                let discarded =
                    discard_rejected_body(body, req_headers, state.config.max_request_body_bytes)
                        .await;
                return finish_reject(
                    errors::blob_upload_invalid("missing _state parameter").into_response(),
                    discarded,
                );
            };

            let (idle_timeout, min_rate) = state.current_stream_guard_params();
            let audit_only = state.transfer_policy.slow_connection_policy
                == crate::config::SlowConnectionPolicy::AuditOnly;
            let stream = crate::http_api::stream_guard::MonitoredUploadStream::new(
                body.into_data_stream(),
                idle_timeout,
                Duration::from_secs(state.transfer_policy.upload_rate_grace_period_secs),
                Duration::from_secs(state.transfer_policy.upload_rate_window_secs),
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
                    if let Some(min_len) = state.transfer_policy.upload_chunk_min_bytes {
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
                let discarded =
                    discard_rejected_body(body, req_headers, state.config.max_request_body_bytes)
                        .await;
                return finish_reject(errors::digest_invalid().into_response(), discarded);
            };
            let digest = match Digest::parse(digest_str) {
                Ok(d) => d,
                Err(_) => {
                    let discarded = discard_rejected_body(
                        body,
                        req_headers,
                        state.config.max_request_body_bytes,
                    )
                    .await;
                    return finish_reject(errors::digest_invalid().into_response(), discarded);
                }
            };

            let range = parse_content_range(req_headers);
            let (idle_timeout, min_rate) = state.current_stream_guard_params();
            let audit_only = state.transfer_policy.slow_connection_policy
                == crate::config::SlowConnectionPolicy::AuditOnly;
            let mut stream = crate::http_api::stream_guard::MonitoredUploadStream::new(
                body.into_data_stream(),
                idle_timeout,
                Duration::from_secs(state.transfer_policy.upload_rate_grace_period_secs),
                Duration::from_secs(state.transfer_policy.upload_rate_window_secs),
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

pub(crate) fn parse_content_range(headers: &HeaderMap) -> Option<(u64, u64)> {
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

#[cfg(test)]
mod discard_tests {
    use super::{RejectedBody, discard_rejected_body};
    use axum::body::Body;
    use axum::http::HeaderMap;
    use bytes::Bytes;

    #[tokio::test]
    async fn content_length_over_the_limit_closes_without_reading() {
        let body = Body::from(vec![1u8; 32]);
        let mut headers = HeaderMap::new();
        headers.insert(http::header::CONTENT_LENGTH, "32".parse().unwrap());
        let discarded = discard_rejected_body(body, &headers, 8).await;
        assert!(matches!(discarded, RejectedBody::Close));
    }

    #[tokio::test]
    async fn small_body_is_fully_discarded() {
        let body = Body::from(Bytes::from_static(b"no"));
        let mut headers = HeaderMap::new();
        headers.insert(http::header::CONTENT_LENGTH, "2".parse().unwrap());
        let discarded = discard_rejected_body(body, &headers, 8).await;
        assert!(matches!(discarded, RejectedBody::Reuse));
    }

    #[tokio::test]
    async fn streamed_body_over_the_limit_closes() {
        let stream = futures_util::stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from(vec![1u8; 6])),
            Ok(Bytes::from(vec![2u8; 6])),
        ]);
        let body = Body::from_stream(stream);
        let discarded = discard_rejected_body(body, &HeaderMap::new(), 8).await;
        assert!(matches!(discarded, RejectedBody::Close));
    }
}
