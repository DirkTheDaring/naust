use axum::http::HeaderMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;

use crate::blob_delete_safety;
use crate::blob_ref_index;
use crate::config::Config;
use crate::gc_service;
use crate::ip_concurrency;
use crate::manifest_lifecycle;
use crate::proxy;
use crate::repository_membership_ledger;
use crate::storage;
use crate::upload_coordinator;

#[derive(Debug, Default)]
pub struct AuthMetrics {
    token_issued_total: AtomicU64,
    token_denied_total: AtomicU64,
    token_internal_error_total: AtomicU64,
}

impl AuthMetrics {
    pub fn inc_token_issued(&self) -> u64 {
        self.token_issued_total.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn inc_token_denied(&self) -> u64 {
        self.token_denied_total.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn inc_token_internal_error(&self) -> u64 {
        self.token_internal_error_total
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }
}

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub auth_metrics: Arc<AuthMetrics>,
    pub storage: Arc<dyn storage::Storage>,
    pub ref_index: Option<Arc<blob_ref_index::BlobRefIndex>>,
    pub gc_service: Option<Arc<gc_service::GcService>>,
    pub gc_run_seq: Arc<AtomicU64>,
    pub proxy: Option<Arc<proxy::Proxy>>,
    pub proxy_cache: Option<Arc<dyn storage::Storage>>,
    // Multi-upstream: proxy/cache selected per request host.
    pub proxy_upstreams: Vec<ProxyContext>,
    pub buffered_body_sem: Arc<Semaphore>,
    pub request_sem: Arc<Semaphore>,
    pub upload_request_sem: Arc<Semaphore>,

    // Diagnostics: in-flight request counters and rate-limited saturation logs.
    pub active_non_upload_requests: Arc<AtomicU64>,
    pub active_upload_requests: Arc<AtomicU64>,
    pub last_sem_saturation_log_unix_secs: Arc<AtomicU64>,

    // Security & Anti-Slowloris:
    pub ip_limiter: Arc<ip_concurrency::IpConcurrencyLimiter>,
    pub is_high_pressure: Arc<std::sync::atomic::AtomicBool>,

    // Authoritative Repository Membership Ledger:
    pub membership_ledger: Arc<repository_membership_ledger::RepositoryMembershipLedger>,

    // Centralized Upload Session Lifecycle Coordinator:
    pub upload_coordinator: Arc<upload_coordinator::BlobUploadCoordinator>,

    // Isolated Blob Deletion Service:
    pub delete_service: Arc<blob_delete_safety::BlobDeleteService>,

    // Consolidated Manifest and Tag Lifecycle Service:
    pub manifest_lifecycle: Arc<manifest_lifecycle::ManifestLifecycleService>,
}

#[derive(Clone)]
pub struct ProxyContext {
    pub proxy: Arc<proxy::Proxy>,
    pub cache: Arc<dyn storage::Storage>,
}

impl AppState {
    pub fn proxy_context_for_request(&self, headers: &HeaderMap) -> Option<ProxyContext> {
        // Multi-upstream mode: choose the upstream by request host.
        if !self.config.proxy.upstreams.is_empty() {
            let idx = crate::request_routing::proxy_upstream_index_for_request(
                &self.config.proxy,
                headers,
            )?;
            return self.proxy_upstreams.get(idx).cloned();
        }

        // Single-upstream mode.
        match (self.proxy.as_ref(), self.proxy_cache.as_ref()) {
            (Some(proxy), Some(cache)) => Some(ProxyContext {
                proxy: proxy.clone(),
                cache: cache.clone(),
            }),
            _ => None,
        }
    }

    pub fn current_stream_guard_params(&self) -> (Duration, u64) {
        let total_permits = self.config.max_concurrent_upload_requests.max(1) as f64;
        let active = self.active_upload_requests.load(Ordering::Relaxed) as f64;
        let load_ratio = active / total_permits;

        let currently_high = self.is_high_pressure.load(Ordering::Relaxed);
        let new_high = if currently_high {
            load_ratio >= 0.70
        } else {
            load_ratio >= 0.85
        };

        if new_high != currently_high {
            self.is_high_pressure.store(new_high, Ordering::Relaxed);
            tracing::info!(
                high_pressure = new_high,
                load_ratio = %format!("{:.1}%", load_ratio * 100.0),
                "adaptive slowloris defense mode transitioned"
            );
        }

        if new_high {
            (
                Duration::from_secs(self.config.upload_chunk_idle_timeout_secs.min(8)),
                self.config.min_upload_bytes_per_sec.saturating_mul(2),
            )
        } else {
            (
                Duration::from_secs(self.config.upload_chunk_idle_timeout_secs),
                self.config.min_upload_bytes_per_sec,
            )
        }
    }
}
