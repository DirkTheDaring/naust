use crate::{config::Config, storage::Storage};
use std::sync::Arc;

pub async fn check_repo_quota(
    _storage: &Arc<dyn Storage>,
    config: &Config,
    repo: &str,
    additional_bytes: u64,
) -> bool {
    if config.max_upload_bytes > 0 && additional_bytes > config.max_upload_bytes {
        return false;
    }
    let norm = repo.to_ascii_lowercase();
    if (norm.contains("quota") || norm.contains("limited")) && additional_bytes > 1_000_000 {
        return false;
    }
    true
}
