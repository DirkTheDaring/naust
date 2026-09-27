use axum::http::{Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
pub use naust_auth::token_rate_limit::TokenRateLimiter;

pub async fn limit_token_requests(
    limiter: TokenRateLimiter,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    if let Some(retry_after) = limiter.retry_after().await {
        let secs = retry_after.as_secs().max(1).to_string();
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, secs)],
            "too many token requests",
        )
            .into_response();
    }

    next.run(req).await
}
