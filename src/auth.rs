use crate::{http_api::errors, AppState};
use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use headers::{authorization::Basic, Authorization, HeaderMapExt};

fn extract_repo_from_v2_path(path: &str) -> Option<String> {
    // Path is expected to look like:
    //   /v2/<name>/blobs/...
    //   /v2/<name>/manifests/...
    //   /v2/<name>/tags/list
    // where <name> may contain '/'.
    if !path.starts_with("/v2/") {
        return None;
    }
    let segments: Vec<&str> = path
        .trim_start_matches("/v2/")
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    if segments.is_empty() {
        return None;
    }

    let marker_idx = segments
        .iter()
        .position(|s| *s == "blobs" || *s == "manifests" || *s == "tags");
    let Some(marker_idx) = marker_idx else {
        return None;
    };
    if marker_idx == 0 {
        return None;
    }
    Some(segments[..marker_idx].join("/"))
}

fn repo_allowed(allowlist: &[String], repo: &str) -> bool {
    allowlist.iter().any(|pat| {
        if pat == "*" {
            return true;
        }
        if let Some(prefix) = pat.strip_suffix("/*") {
            return repo == prefix || repo.starts_with(&format!("{prefix}/"));
        }
        pat == repo
    })
}

pub async fn require_push_basic_auth(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    // Policy: anonymous pull. Only gate push (write) methods.
    // This keeps `/v2/` ping and all GET/HEAD endpoints anonymous.
    match *request.method() {
        http::Method::GET | http::Method::HEAD => return next.run(request).await,
        _ => {}
    }

    // If auth is not configured, reject pushes by default (safe default).
    let Some(expected_user) = state.config.push_username.as_deref() else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Some(expected_pass) = state.config.push_password.as_deref() else {
        return StatusCode::UNAUTHORIZED.into_response();
    };

    if let Some(Authorization(basic)) = request.headers().typed_get::<Authorization<Basic>>() {
        let user_ok = basic.username() == expected_user;
        let pass_ok = basic.password() == expected_pass;
        if user_ok && pass_ok {
            if let Some(allowlist) = state.config.push_allow_repos.as_deref() {
                if let Some(repo) = extract_repo_from_v2_path(request.uri().path()) {
                    if !repo_allowed(allowlist, &repo) {
                        return errors::denied("push not allowed for this repository").into_response();
                    }
                }
            }
            return next.run(request).await;
        }
    }

    // Docker expects a Basic challenge for basic-auth registries.
    let mut resp: Response = StatusCode::UNAUTHORIZED.into_response();
    resp.headers_mut().insert(
        http::header::WWW_AUTHENTICATE,
        http::HeaderValue::from_static("Basic realm=\"registry\""),
    );
    resp
}
