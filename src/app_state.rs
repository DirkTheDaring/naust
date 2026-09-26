use axum::http::HeaderMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;

use crate::application::{
    BlobMutationService, BlobReadService, CatalogQueryService, ManifestMutationService,
    ManifestReadService, ProxyTarget, ReferrersQueryService, TagQueryService,
};
use crate::blob_ref_index;
use crate::config::Config;
use crate::gc_service;
use crate::ip_concurrency;
use crate::proxy;

#[derive(Debug, Default)]
pub struct AuthMetrics {
    token_issued_total: AtomicU64,
    token_denied_total: AtomicU64,
    token_internal_error_total: AtomicU64,
}

impl AuthMetrics {
    pub fn token_issued_total(&self) -> u64 {
        self.token_issued_total.load(Ordering::Relaxed)
    }

    pub fn token_denied_total(&self) -> u64 {
        self.token_denied_total.load(Ordering::Relaxed)
    }

    pub fn token_internal_error_total(&self) -> u64 {
        self.token_internal_error_total.load(Ordering::Relaxed)
    }

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

pub type ProxyContext = ProxyTarget;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    /// Per-family handler policy snapshot (R4/KI-26): handlers read this,
    /// not `config`, for transfer knobs.
    pub transfer_policy: Arc<crate::http_api::policy::HttpTransferPolicy>,
    pub auth_metrics: Arc<AuthMetrics>,
    pub ref_index: Option<Arc<blob_ref_index::BlobRefIndex>>,
    pub gc_service: Option<Arc<gc_service::GcService>>,
    /// Admin GC facade (R4/KI-26): owns the run-id sequence and defaults.
    pub gc_admin: Arc<crate::gc_admin::GcAdminService>,
    /// Token issuance facade (R4/KI-26): the /token handler delegates here.
    pub token_svc: Arc<crate::token_service::TokenService>,
    pub proxy: Option<Arc<proxy::Proxy>>,
    pub proxy_cache: Option<Arc<dyn crate::storage::ports::ProxyStoragePort>>,
    // Multi-upstream: proxy/cache selected per request host.
    pub proxy_upstreams: Vec<ProxyTarget>,
    // Concrete per-upstream engines for server-side workers (GC/scrub); the
    // application layer only ever sees the ProxyTarget trait objects above.
    pub proxy_upstream_engines: Vec<Arc<proxy::Proxy>>,
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

    // Focused Application Services:
    pub blob_service: Arc<BlobMutationService>,
    pub manifest_service: Arc<ManifestMutationService>,
    pub blob_read_service: Arc<BlobReadService>,
    pub manifest_read_service: Arc<ManifestReadService>,
    pub catalog_query_service: Arc<CatalogQueryService>,
    pub tag_query_service: Arc<TagQueryService>,
    pub referrers_query_service: Arc<ReferrersQueryService>,
}

impl AppState {
    pub fn proxy_context_for_request(&self, headers: &HeaderMap) -> Option<ProxyTarget> {
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
            (Some(proxy), Some(cache)) => Some(ProxyTarget {
                proxy: proxy.clone(),
                cache_storage: cache.clone(),
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

    pub fn new_test(
        cfg: Arc<Config>,
        storage: Arc<crate::storage::fs::FsStorage>,
        gc_service: Option<Arc<crate::gc_service::GcService>>,
    ) -> Self {
        let wiring = crate::storage::ports::StorageWiring::from_backend(storage);
        crate::runtime::build_test_app_state(cfg, wiring, None, None, None, gc_service)
    }

    pub fn new_test_with_proxy(
        cfg: Arc<Config>,
        storage: Arc<crate::storage::fs::FsStorage>,
        ref_index: Option<Arc<crate::blob_ref_index::BlobRefIndex>>,
        proxy: Option<Arc<crate::proxy::Proxy>>,
        proxy_cache: Option<Arc<dyn crate::storage::ports::ProxyStoragePort>>,
    ) -> Self {
        let wiring = crate::storage::ports::StorageWiring::from_backend(storage);
        crate::runtime::build_test_app_state(cfg, wiring, ref_index, proxy, proxy_cache, None)
    }
}
