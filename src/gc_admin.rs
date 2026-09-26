//! Server-side GC admin facade (remediation R4/A1, KI-26): `/_admin/gc/*`
//! handlers delegate here instead of reaching into `state.gc_service`,
//! `state.gc_run_seq`, and `state.config` directly. Owns the run-id sequence
//! and the config-derived defaults snapshot.

use crate::blob_gc::{BlobGcPolicy, BlobGcStats};
use crate::config::Config;
use crate::gc_service::{GcBudgets, GcService, GcServiceError};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Admin-facing GC defaults (GcPolicy pattern; exhaustive mapping test below).
#[derive(Clone, Debug)]
pub struct GcAdminPolicy {
    pub default_min_age_secs: u64,
    pub default_quarantine_delay_secs: u64,
    pub default_max_blobs: usize,
    pub default_max_bytes: u64,
    pub default_max_seconds: u64,
}

impl From<&Config> for GcAdminPolicy {
    fn from(cfg: &Config) -> Self {
        Self {
            default_min_age_secs: cfg.blob_gc_default_min_age_secs,
            default_quarantine_delay_secs: cfg.blob_gc_default_quarantine_delay_secs,
            default_max_blobs: cfg.blob_gc_default_max_blobs,
            default_max_bytes: cfg.blob_gc_default_max_bytes,
            default_max_seconds: cfg.blob_gc_default_max_seconds,
        }
    }
}

#[derive(Debug)]
pub enum GcAdminError {
    Unavailable,
    Service(GcServiceError),
}

pub struct BudgetOverrides {
    pub max_blobs: Option<usize>,
    pub max_bytes: Option<u64>,
    pub max_seconds: Option<u64>,
}

pub struct GcAdminService {
    gc: Option<Arc<GcService>>,
    run_seq: Arc<AtomicU64>,
    policy: GcAdminPolicy,
}

impl GcAdminService {
    pub fn new(gc: Option<Arc<GcService>>, run_seq: Arc<AtomicU64>, policy: GcAdminPolicy) -> Self {
        Self {
            gc,
            run_seq,
            policy,
        }
    }

    fn service(&self) -> Result<&Arc<GcService>, GcAdminError> {
        self.gc.as_ref().ok_or(GcAdminError::Unavailable)
    }

    fn budgets(&self, o: BudgetOverrides) -> GcBudgets {
        GcBudgets {
            max_blobs: o.max_blobs.unwrap_or(self.policy.default_max_blobs),
            max_bytes: o.max_bytes.unwrap_or(self.policy.default_max_bytes),
            max_seconds: o.max_seconds.unwrap_or(self.policy.default_max_seconds),
        }
    }

    fn next_run_id(&self) -> u64 {
        self.run_seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub async fn health(&self) -> Result<(), GcAdminError> {
        self.service()?
            .health()
            .await
            .map_err(GcAdminError::Service)
    }

    pub async fn plan(
        &self,
        policy: BlobGcPolicy,
        min_age_secs: Option<u64>,
        budgets: BudgetOverrides,
    ) -> Result<(u64, BlobGcStats), GcAdminError> {
        let service = self.service()?;
        let run_id = self.next_run_id();
        let min_age = Duration::from_secs(min_age_secs.unwrap_or(self.policy.default_min_age_secs));
        let stats = service
            .plan(policy, min_age, self.budgets(budgets))
            .await
            .map_err(GcAdminError::Service)?;
        Ok((run_id, stats))
    }

    pub async fn quarantine(
        &self,
        policy: BlobGcPolicy,
        min_age_secs: Option<u64>,
        budgets: BudgetOverrides,
    ) -> Result<(u64, BlobGcStats), GcAdminError> {
        let service = self.service()?;
        let run_id = self.next_run_id();
        let min_age = Duration::from_secs(min_age_secs.unwrap_or(self.policy.default_min_age_secs));
        let stats = service
            .quarantine(policy, min_age, self.budgets(budgets))
            .await
            .map_err(GcAdminError::Service)?;
        Ok((run_id, stats))
    }

    pub async fn delete(
        &self,
        policy: BlobGcPolicy,
        quarantine_delay_secs: Option<u64>,
        budgets: BudgetOverrides,
    ) -> Result<(u64, BlobGcStats), GcAdminError> {
        let service = self.service()?;
        let run_id = self.next_run_id();
        let delay = Duration::from_secs(
            quarantine_delay_secs.unwrap_or(self.policy.default_quarantine_delay_secs),
        );
        let stats = service
            .delete(policy, delay, self.budgets(budgets))
            .await
            .map_err(GcAdminError::Service)?;
        Ok((run_id, stats))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_admin_policy_mapping_covers_every_field() {
        let mut cfg = Config::from_env().unwrap();
        cfg.blob_gc_default_min_age_secs = 1;
        cfg.blob_gc_default_quarantine_delay_secs = 2;
        cfg.blob_gc_default_max_blobs = 3;
        cfg.blob_gc_default_max_bytes = 4;
        cfg.blob_gc_default_max_seconds = 5;
        let GcAdminPolicy {
            default_min_age_secs,
            default_quarantine_delay_secs,
            default_max_blobs,
            default_max_bytes,
            default_max_seconds,
        } = GcAdminPolicy::from(&cfg);
        assert_eq!(
            (
                default_min_age_secs,
                default_quarantine_delay_secs,
                default_max_blobs,
                default_max_bytes,
                default_max_seconds
            ),
            (1, 2, 3, 4, 5)
        );
    }

    #[tokio::test]
    async fn unavailable_without_service() {
        let svc = GcAdminService::new(
            None,
            Arc::new(AtomicU64::new(0)),
            GcAdminPolicy {
                default_min_age_secs: 0,
                default_quarantine_delay_secs: 0,
                default_max_blobs: 1,
                default_max_bytes: 1,
                default_max_seconds: 1,
            },
        );
        assert!(matches!(svc.health().await, Err(GcAdminError::Unavailable)));
    }
}
