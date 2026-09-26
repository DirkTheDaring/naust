//! Per-family handler policy snapshots (remediation R4/A1, KI-26): derived
//! once from `Config` at composition time via tested `From` impls — handlers
//! read snapshot fields, never `state.config` (the `GcPolicy` pattern).

use crate::config::{Config, SlowConnectionPolicy};

/// Mechanical transfer knobs shared by the blob/manifest/upload handler
/// families. Auth/token concerns live in the auth context, not here.
#[derive(Clone, Debug)]
pub struct HttpTransferPolicy {
    pub max_upload_bytes: u64,
    pub upload_chunk_min_bytes: Option<usize>,
    pub upload_rate_window_secs: u64,
    pub upload_rate_grace_period_secs: u64,
    pub slow_connection_policy: SlowConnectionPolicy,
    pub allow_tag_overwrite: bool,
}

impl From<&Config> for HttpTransferPolicy {
    fn from(cfg: &Config) -> Self {
        Self {
            max_upload_bytes: cfg.max_upload_bytes,
            upload_chunk_min_bytes: cfg.upload_chunk_min_bytes,
            upload_rate_window_secs: cfg.upload_rate_window_secs,
            upload_rate_grace_period_secs: cfg.upload_rate_grace_period_secs,
            slow_connection_policy: cfg.slow_connection_policy,
            allow_tag_overwrite: cfg.allow_tag_overwrite,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_policy_mapping_covers_every_field() {
        let mut cfg = Config::from_env().unwrap();
        cfg.max_upload_bytes = 11;
        cfg.upload_chunk_min_bytes = Some(22);
        cfg.upload_rate_window_secs = 33;
        cfg.upload_rate_grace_period_secs = 44;
        cfg.slow_connection_policy = SlowConnectionPolicy::Enforce;
        cfg.allow_tag_overwrite = true;

        // Exhaustive destructuring: a new field breaks this test until mapped.
        let HttpTransferPolicy {
            max_upload_bytes,
            upload_chunk_min_bytes,
            upload_rate_window_secs,
            upload_rate_grace_period_secs,
            slow_connection_policy,
            allow_tag_overwrite,
        } = HttpTransferPolicy::from(&cfg);
        assert_eq!(max_upload_bytes, 11);
        assert_eq!(upload_chunk_min_bytes, Some(22));
        assert_eq!(upload_rate_window_secs, 33);
        assert_eq!(upload_rate_grace_period_secs, 44);
        assert!(matches!(
            slow_connection_policy,
            SlowConnectionPolicy::Enforce
        ));
        assert!(allow_tag_overwrite);
    }
}
