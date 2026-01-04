use axum::http::{Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub struct TokenRateLimiter {
    inner: Arc<Mutex<Window>>,
    max_requests: u32,
    window: Duration,
}

#[derive(Debug)]
struct Window {
    start: Instant,
    count: u32,
}

impl TokenRateLimiter {
    pub fn disabled() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Window {
                start: Instant::now(),
                count: 0,
            })),
            max_requests: 0,
            window: Duration::from_secs(0),
        }
    }

    pub fn new(max_requests: u32, window: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Window {
                start: Instant::now(),
                count: 0,
            })),
            max_requests,
            window: if window.is_zero() {
                Duration::from_secs(60)
            } else {
                window
            },
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.max_requests > 0
    }

    pub async fn retry_after(&self) -> Option<Duration> {
        if !self.is_enabled() {
            return None;
        }

        let now = Instant::now();
        let mut w = self.inner.lock().await;
        if now.duration_since(w.start) >= self.window {
            w.start = now;
            w.count = 0;
        }

        if w.count >= self.max_requests {
            let elapsed = now.duration_since(w.start);
            let remaining = self.window.saturating_sub(elapsed);
            return Some(remaining.max(Duration::from_secs(1)));
        }

        w.count += 1;
        None
    }
}

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
