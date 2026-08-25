use bytes::Bytes;
use futures_util::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::{Instant, Sleep};

#[derive(Debug, thiserror::Error)]
pub enum StreamGuardError {
    #[error("upload stream idle timeout: no data received for {0:?}")]
    IdleTimeout(Duration),

    #[error("upload stream throughput too low: {actual_bps} B/s < required {min_bps} B/s")]
    InsufficientThroughput { actual_bps: u64, min_bps: u64 },

    #[error("underlying body stream error: {0}")]
    BodyError(String),
}

pub struct MonitoredUploadStream<S> {
    inner: S,
    idle_timeout: Duration,
    grace_period: Duration,
    window_duration: Duration,
    min_bytes_per_sec: u64,
    audit_only: bool,

    // Runtime state
    started_at: Instant,
    window_started_at: Instant,
    window_bytes: u64,

    // Tokio Timer Reactor Integration (Pinned Sleep)
    idle_timer: Pin<Box<Sleep>>,
    window_timer: Pin<Box<Sleep>>,
}

impl<S> MonitoredUploadStream<S>
where
    S: Stream<Item = Result<Bytes, axum::Error>> + Unpin,
{
    pub fn new(
        inner: S,
        idle_timeout: Duration,
        grace_period: Duration,
        window_duration: Duration,
        min_bytes_per_sec: u64,
        audit_only: bool,
    ) -> Self {
        let now = Instant::now();
        Self {
            inner,
            idle_timeout,
            grace_period,
            window_duration,
            min_bytes_per_sec,
            audit_only,
            started_at: now,
            window_started_at: now,
            window_bytes: 0,
            idle_timer: Box::pin(tokio::time::sleep_until(now + idle_timeout)),
            window_timer: Box::pin(tokio::time::sleep_until(now + window_duration)),
        }
    }
}

impl<S> Stream for MonitoredUploadStream<S>
where
    S: Stream<Item = Result<Bytes, axum::Error>> + Unpin,
{
    type Item = Result<Bytes, StreamGuardError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        let now = Instant::now();

        // 1. Poll Idle Timer (wakes task even if network socket is completely silent)
        if this.idle_timer.as_mut().poll(cx).is_ready() {
            if !this.audit_only {
                return Poll::Ready(Some(Err(StreamGuardError::IdleTimeout(this.idle_timeout))));
            } else {
                tracing::warn!(idle_secs = ?this.idle_timeout, "slow upload idle timeout (audit mode)");
                this.idle_timer.as_mut().reset(now + this.idle_timeout);
                let _ = this.idle_timer.as_mut().poll(cx);
            }
        }

        // 2. Poll Sliding-Window Throughput Timer
        if this.window_timer.as_mut().poll(cx).is_ready() {
            let elapsed_since_start = now.duration_since(this.started_at);
            if elapsed_since_start >= this.grace_period && this.min_bytes_per_sec > 0 {
                let window_secs = this.window_duration.as_secs_f64().max(0.001);
                let actual_bps = (this.window_bytes as f64 / window_secs) as u64;

                if actual_bps < this.min_bytes_per_sec {
                    if !this.audit_only {
                        return Poll::Ready(Some(Err(StreamGuardError::InsufficientThroughput {
                            actual_bps,
                            min_bps: this.min_bytes_per_sec,
                        })));
                    } else {
                        tracing::warn!(
                            actual_bps,
                            min_bps = this.min_bytes_per_sec,
                            "slow upload throughput below minimum (audit mode)"
                        );
                    }
                }
            }
            // Reset sliding window
            this.window_bytes = 0;
            this.window_started_at = now;
            this.window_timer.as_mut().reset(now + this.window_duration);
            let _ = this.window_timer.as_mut().poll(cx);
        }

        // 3. Poll Incoming Socket Data Frame
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                let len = bytes.len() as u64;
                this.window_bytes += len;

                // Reset idle deadline on successful packet receipt
                this.idle_timer.as_mut().reset(now + this.idle_timeout);

                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(err))) => {
                Poll::Ready(Some(Err(StreamGuardError::BodyError(err.to_string()))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
