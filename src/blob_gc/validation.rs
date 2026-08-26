use super::policy::PolicyContext;
use crate::blob_ref_index::BlobRefIndex;
use crate::storage;
use std::sync::Arc;
use std::time::SystemTime;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PreDeleteValidation {
    Eligible,
    Protected(String),
}

/// Authoritative pre-delete candidate revalidation executed immediately before physical deletion.
///
/// Invariants:
/// - 1. Fails closed if the durable reference index is unhealthy or requires repair.
/// - 2. Protects any blob actively pinned by in-flight uploads.
/// - 3. Protects any blob with active or pending repository memberships.
/// - 4. Protects any blob reachable from stored manifests/tags under configured policy.
/// - 5. Protects any blob referenced by an active lifecycle journal across all repositories.
/// - 6. Fails closed if any repository lifecycle journal is unreadable or malformed.
pub(crate) async fn revalidate_candidate_before_delete(
    storage: &Arc<dyn storage::Storage>,
    idx: &BlobRefIndex,
    candidate: &storage::GcBlobCandidate,
    now: SystemTime,
    policy_ctx: &mut PolicyContext,
) -> Result<PreDeleteValidation, String> {
    // 1. Ref-index health
    idx.check_health()
        .map_err(|e| format!("ref-index unhealthy during pre-delete check: {e}"))?;

    // 2. In-flight upload pins
    if policy_ctx.is_pinned(&candidate.digest, now)? {
        return Ok(PreDeleteValidation::Protected(
            "pinned in-flight/finalizing upload in ref-index".to_string(),
        ));
    }

    // 3. Authoritative repository-membership count
    let mem_count = storage
        .count_repo_blob_memberships(&candidate.digest)
        .await
        .map_err(|e| format!("count repository memberships for {}: {e}", candidate.digest))?;
    if mem_count > 0 {
        return Ok(PreDeleteValidation::Protected(format!(
            "blob has {mem_count} active or pending repository memberships"
        )));
    }

    // 4. Manifest reachability check (tag-rooted or manifest-rooted)
    if policy_ctx.is_referenced(&candidate.digest).await? {
        return Ok(PreDeleteValidation::Protected(
            "reachable from existing manifest root".to_string(),
        ));
    }

    // 5. Active lifecycle journal check across all repositories (fails closed on error/corruption)
    let repos = storage
        .list_repositories()
        .await
        .map_err(|e| format!("list repositories for journal pre-delete check: {e}"))?;
    for repo in repos {
        match storage.read_lifecycle_journal(&repo).await {
            Ok(Some(bytes)) => {
                match serde_json::from_slice::<crate::manifest_lifecycle::LifecycleJournalRecord>(
                    &bytes,
                ) {
                    Ok(journal) => {
                        if journal.target_digest == candidate.digest
                            || journal.subject_digest.as_ref() == Some(&candidate.digest)
                        {
                            return Ok(PreDeleteValidation::Protected(format!(
                                "active lifecycle journal op {} in repo {} references target blob",
                                journal.op_id, repo
                            )));
                        }
                    }
                    Err(e) => {
                        return Err(format!(
                            "corrupt lifecycle journal record in repo '{repo}': {e}"
                        ));
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                return Err(format!(
                    "failed to read lifecycle journal for repo '{repo}': {e}"
                ));
            }
        }
    }

    Ok(PreDeleteValidation::Eligible)
}
