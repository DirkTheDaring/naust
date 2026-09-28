use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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

    pub fn check_and_record(&self) -> Option<Duration> {
        if !self.is_enabled() {
            return None;
        }

        let now = Instant::now();
        let mut w = self.inner.lock().unwrap();
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

    pub fn retry_after(&self) -> Option<Duration> {
        self.check_and_record()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_limiter_never_limits() {
        let l = TokenRateLimiter::disabled();
        assert!(!l.is_enabled());
        for _ in 0..100 {
            assert!(
                l.retry_after().is_none(),
                "disabled limiter must always admit"
            );
        }
    }

    #[test]
    fn fixed_window_admits_up_to_max_then_rejects_with_retry_after() {
        let l = TokenRateLimiter::new(3, Duration::from_secs(3600));
        assert!(l.is_enabled());
        for _ in 0..3 {
            assert!(l.retry_after().is_none());
        }
        let retry = l
            .retry_after()
            .expect("4th request in the window must be limited");
        assert!(retry >= Duration::from_secs(1));
    }

    #[test]
    fn zero_window_defaults_to_sixty_seconds() {
        let l = TokenRateLimiter::new(10, Duration::from_secs(0));
        assert_eq!(l.window, Duration::from_secs(60));
    }
}
