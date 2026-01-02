use crate::AppState;
use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use headers::{authorization::Basic, Authorization, HeaderMapExt};

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
