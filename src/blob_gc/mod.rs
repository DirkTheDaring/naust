pub mod policy;
pub mod traverser;
pub mod validation;

pub use policy::{
    AgeEligibility, BlobGcPolicy, PolicyContext, build_manifest_protected_set,
    build_manifest_protected_set_fs, check_candidate_age,
};
pub use traverser::{CasBlobTraverser, GcPaginationError};
pub(crate) use validation::{PreDeleteValidation, revalidate_candidate_before_delete};

use crate::blob_ref_index::BlobRefIndex;
use crate::registry::digest::Digest;
use crate::storage;
use crate::storage::mutation_authority::RuntimeMutationAuthority;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

#[derive(Debug, Default, Clone)]
pub struct BlobGcStats {
    pub scanned_blobs: u64,
    pub scanned_bytes: u64,
    pub eligible_blobs: u64,
    pub eligible_bytes: u64,
    pub quarantined_blobs: u64,
    pub quarantined_bytes: u64,
    pub restored_blobs: u64,
    pub restored_bytes: u64,
    pub deleted_blobs: u64,
    pub deleted_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct BlobGcLimits {
    pub max_per_run: usize,
    pub max_bytes: u64,
    pub max_seconds: u64,
}

impl BlobGcLimits {
    pub fn unlimited(max_per_run: usize) -> Self {
        Self {
            max_per_run,
            max_bytes: u64::MAX,
            max_seconds: u64::MAX,
        }
    }
}

pub async fn blob_gc_plan(
    cfg: &crate::config::Config,
    storage: &Arc<dyn storage::Storage>,
    idx: &BlobRefIndex,
    policy: BlobGcPolicy,
    min_age: Duration,
    limits: BlobGcLimits,
) -> Result<BlobGcStats, String> {
    let mut policy_ctx = PolicyContext::build(cfg, storage, idx, policy).await?;
    let mut stats = BlobGcStats::default();

    let t0 = Instant::now();
    let now = SystemTime::now();

    let mut traverser = CasBlobTraverser::new(storage, 100);

    while let Some(items) = traverser.next_batch().await.map_err(|e| e.to_string())? {
        for candidate in items {
            if t0.elapsed() > Duration::from_secs(limits.max_seconds) {
                return Ok(stats);
            }

            match check_candidate_age(candidate.last_modified, now, min_age) {
                AgeEligibility::Eligible => {}
                _ => continue,
            }

            if policy_ctx.is_pinned(&candidate.digest, now)? {
                continue;
            }

            stats.scanned_blobs += 1;
            stats.scanned_bytes = stats.scanned_bytes.saturating_add(candidate.size);

            if stats.eligible_blobs as usize >= limits.max_per_run {
                continue;
            }
            if policy_ctx.is_referenced(&candidate.digest).await? {
                continue;
            }

            let mem_count = storage
                .count_repo_blob_memberships(&candidate.digest)
                .await
                .map_err(|e| format!("count memberships: {e}"))?;
            if mem_count > 0 {
                continue;
            }

            if stats.eligible_bytes >= limits.max_bytes {
                continue;
            }
            stats.eligible_blobs += 1;
            stats.eligible_bytes = stats.eligible_bytes.saturating_add(candidate.size);
        }

        if t0.elapsed() > Duration::from_secs(limits.max_seconds) {
            break;
        }
    }

    Ok(stats)
}

pub(crate) async fn blob_gc_quarantine(
    cfg: &crate::config::Config,
    storage: &Arc<dyn storage::Storage>,
    idx: &BlobRefIndex,
    consistency_gate: &Arc<Mutex<()>>,
    mutation_authority: &Arc<Mutex<Option<RuntimeMutationAuthority>>>,
    policy: BlobGcPolicy,
    min_age: Duration,
    limits: BlobGcLimits,
) -> Result<BlobGcStats, String> {
    if storage.gc_strategy() == storage::GcStorageStrategy::S3DirectConditional {
        return Ok(BlobGcStats::default());
    }

    let mut stats = BlobGcStats::default();

    let t0 = Instant::now();
    let now = SystemTime::now();

    let mut traverser = CasBlobTraverser::new(storage, 100);

    while let Some(items) = traverser.next_batch().await.map_err(|e| e.to_string())? {
        for candidate in items {
            if t0.elapsed() > Duration::from_secs(limits.max_seconds) {
                return Ok(stats);
            }
            if stats.quarantined_blobs as usize >= limits.max_per_run {
                return Ok(stats);
            }
            if stats.quarantined_bytes >= limits.max_bytes {
                return Ok(stats);
            }

            match check_candidate_age(candidate.last_modified, now, min_age) {
                AgeEligibility::Eligible => {}
                _ => continue,
            }

            stats.scanned_blobs += 1;
            stats.scanned_bytes = stats.scanned_bytes.saturating_add(candidate.size);

            let auth_guard = mutation_authority.lock().await;
            let Some(ref auth) = *auth_guard else {
                return Err("mutation authority unavailable or inactive".to_string());
            };
            if !auth.is_active() {
                return Err("mutation authority released".to_string());
            }
            let permit = auth.gc_mutation_permit();

            let _gate = consistency_gate.lock().await;

            let mut policy_ctx = PolicyContext::build(cfg, storage, idx, policy).await?;

            if policy_ctx.is_pinned(&candidate.digest, now)? {
                drop(_gate);
                drop(auth_guard);
                continue;
            }

            if policy_ctx.is_referenced(&candidate.digest).await? {
                drop(_gate);
                drop(auth_guard);
                continue;
            }

            let q_res = storage
                .quarantine_blob(&permit, &candidate.digest, &candidate.version)
                .await;

            drop(_gate);
            drop(auth_guard);

            match q_res {
                Ok(storage::GcQuarantineResult::Quarantined { size }) => {
                    stats.quarantined_blobs += 1;
                    stats.quarantined_bytes = stats.quarantined_bytes.saturating_add(size);
                }
                Ok(storage::GcQuarantineResult::Skipped) => {}
                Err(e) => {
                    return Err(format!("quarantine blob {}: {e}", candidate.digest));
                }
            }
        }

        if stats.quarantined_blobs as usize >= limits.max_per_run
            || stats.quarantined_bytes >= limits.max_bytes
            || t0.elapsed() > Duration::from_secs(limits.max_seconds)
        {
            break;
        }
    }

    Ok(stats)
}

pub(crate) async fn blob_gc_delete(
    cfg: &crate::config::Config,
    storage: &Arc<dyn storage::Storage>,
    idx: &BlobRefIndex,
    consistency_gate: &Arc<Mutex<()>>,
    mutation_authority: &Arc<Mutex<Option<RuntimeMutationAuthority>>>,
    policy: BlobGcPolicy,
    quarantine_delay: Duration,
    limits: BlobGcLimits,
) -> Result<BlobGcStats, String> {
    match storage.gc_strategy() {
        storage::GcStorageStrategy::FilesystemQuarantine => {
            blob_gc_delete_fs(
                cfg,
                storage,
                idx,
                consistency_gate,
                mutation_authority,
                policy,
                quarantine_delay,
                limits,
            )
            .await
        }
        storage::GcStorageStrategy::S3DirectConditional => {
            blob_gc_delete_s3(
                cfg,
                storage,
                idx,
                consistency_gate,
                mutation_authority,
                policy,
                quarantine_delay,
                limits,
            )
            .await
        }
    }
}

async fn blob_gc_delete_fs(
    cfg: &crate::config::Config,
    storage: &Arc<dyn storage::Storage>,
    idx: &BlobRefIndex,
    consistency_gate: &Arc<Mutex<()>>,
    mutation_authority: &Arc<Mutex<Option<RuntimeMutationAuthority>>>,
    policy: BlobGcPolicy,
    quarantine_delay: Duration,
    limits: BlobGcLimits,
) -> Result<BlobGcStats, String> {
    let mut stats = BlobGcStats::default();

    let t0 = Instant::now();
    let now = SystemTime::now();

    let root = cfg.fs_root.join("quarantine").join("blobs").join("sha256");

    let mut prefixes = match tokio::fs::read_dir(&root).await {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(stats),
        Err(e) => return Err(format!("read_dir {}: {e}", root.display())),
    };

    while let Ok(Some(prefix_ent)) = prefixes.next_entry().await {
        if t0.elapsed() > Duration::from_secs(limits.max_seconds) {
            break;
        }
        if stats.deleted_blobs as usize >= limits.max_per_run {
            break;
        }
        if stats.deleted_bytes >= limits.max_bytes {
            break;
        }

        let ft = match prefix_ent.file_type().await {
            Ok(t) => t,
            Err(_) => continue,
        };
        if !ft.is_dir() {
            continue;
        }

        let mut dir = match tokio::fs::read_dir(prefix_ent.path()).await {
            Ok(d) => d,
            Err(_) => continue,
        };

        while let Ok(Some(ent)) = dir.next_entry().await {
            if t0.elapsed() > Duration::from_secs(limits.max_seconds) {
                break;
            }
            if stats.deleted_blobs as usize >= limits.max_per_run {
                break;
            }
            if stats.deleted_bytes >= limits.max_bytes {
                break;
            }

            let ft = match ent.file_type().await {
                Ok(t) => t,
                Err(_) => continue,
            };
            if !ft.is_file() {
                continue;
            }

            let path = ent.path();
            let file_hex = match path.file_name().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            if file_hex.len() != 64 || !file_hex.chars().all(|c| c.is_ascii_hexdigit()) {
                continue;
            }

            let meta = match ent.metadata().await {
                Ok(m) => m,
                Err(_) => continue,
            };

            let digest = match Digest::parse(&format!("sha256:{file_hex}")) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let q_at = match read_quarantine_time(cfg, &digest).await? {
                Some(t) => t,
                None => {
                    let _ = write_quarantine_time(cfg, &digest, now).await;
                    continue;
                }
            };

            match check_candidate_age(q_at, now, quarantine_delay) {
                AgeEligibility::Eligible => {}
                _ => continue,
            }

            if stats.deleted_bytes >= limits.max_bytes {
                continue;
            }

            let auth_guard = mutation_authority.lock().await;
            let Some(ref auth) = *auth_guard else {
                return Err("mutation authority unavailable or inactive".to_string());
            };
            if !auth.is_active() {
                return Err("mutation authority released".to_string());
            }
            let permit = auth.gc_mutation_permit();

            let _gate = consistency_gate.lock().await;

            let mut policy_ctx = PolicyContext::build(cfg, storage, idx, policy).await?;

            if policy_ctx.is_pinned(&digest, now)? {
                drop(_gate);
                drop(auth_guard);
                continue;
            }

            if policy_ctx.is_referenced(&digest).await? {
                let rest_res = storage.restore_quarantined_blob(&permit, &digest).await;
                drop(_gate);
                drop(auth_guard);
                match rest_res {
                    Ok(Some(size)) => {
                        stats.restored_blobs += 1;
                        stats.restored_bytes = stats.restored_bytes.saturating_add(size);
                    }
                    Ok(None) => {}
                    Err(e) => return Err(e.to_string()),
                }
                continue;
            }

            let version = match storage.quarantined_blob_version(&digest).await {
                Ok(Some(v)) => v,
                Ok(None) => {
                    drop(_gate);
                    drop(auth_guard);
                    continue;
                }
                Err(e) => {
                    drop(_gate);
                    drop(auth_guard);
                    return Err(e.to_string());
                }
            };

            let candidate = storage::GcBlobCandidate {
                digest: digest.clone(),
                size: meta.len(),
                last_modified: meta.modified().unwrap_or(UNIX_EPOCH),
                version,
            };

            match revalidate_candidate_before_delete(storage, idx, &candidate, now, &mut policy_ctx)
                .await
            {
                Ok(PreDeleteValidation::Eligible) => {}
                Ok(PreDeleteValidation::Protected(_)) => {
                    drop(_gate);
                    drop(auth_guard);
                    continue;
                }
                Err(e) => {
                    drop(_gate);
                    drop(auth_guard);
                    return Err(e);
                }
            }

            let del_res = storage
                .delete_blob_conditional(&permit, &digest, Some(&candidate.version))
                .await;

            drop(_gate);
            drop(auth_guard);

            match del_res {
                Ok(storage::GcDeleteResult::Deleted) => {
                    stats.deleted_blobs += 1;
                    stats.deleted_bytes = stats.deleted_bytes.saturating_add(meta.len());
                }
                Ok(storage::GcDeleteResult::PreconditionFailed { .. }) => {
                    tracing::warn!(
                        "quarantined blob {} version changed concurrently, preserving object",
                        digest
                    );
                }
                Ok(_) => {}
                Err(e) => return Err(e.to_string()),
            }
        }
    }

    Ok(stats)
}

async fn blob_gc_delete_s3(
    cfg: &crate::config::Config,
    storage: &Arc<dyn storage::Storage>,
    idx: &BlobRefIndex,
    consistency_gate: &Arc<Mutex<()>>,
    mutation_authority: &Arc<Mutex<Option<RuntimeMutationAuthority>>>,
    policy: BlobGcPolicy,
    min_age: Duration,
    limits: BlobGcLimits,
) -> Result<BlobGcStats, String> {
    storage
        .check_bucket_versioning_for_gc()
        .await
        .map_err(|e| e.to_string())?;

    let mut stats = BlobGcStats::default();

    let t0 = Instant::now();
    let now = SystemTime::now();

    let mut traverser = CasBlobTraverser::new(storage, 100);

    while let Some(items) = traverser.next_batch().await.map_err(|e| e.to_string())? {
        for candidate in items {
            if t0.elapsed() > Duration::from_secs(limits.max_seconds) {
                return Ok(stats);
            }
            if stats.deleted_blobs as usize >= limits.max_per_run {
                return Ok(stats);
            }
            if stats.deleted_bytes >= limits.max_bytes {
                return Ok(stats);
            }

            match check_candidate_age(candidate.last_modified, now, min_age) {
                AgeEligibility::Eligible => {}
                _ => continue,
            }

            stats.scanned_blobs += 1;
            stats.scanned_bytes = stats.scanned_bytes.saturating_add(candidate.size);

            let auth_guard = mutation_authority.lock().await;
            let Some(ref auth) = *auth_guard else {
                return Err("mutation authority unavailable or inactive".to_string());
            };
            if !auth.is_active() {
                return Err("mutation authority released".to_string());
            }
            let permit = auth.gc_mutation_permit();

            let _gate = consistency_gate.lock().await;

            let mut policy_ctx = PolicyContext::build(cfg, storage, idx, policy).await?;

            match revalidate_candidate_before_delete(storage, idx, &candidate, now, &mut policy_ctx)
                .await
            {
                Ok(PreDeleteValidation::Eligible) => {}
                Ok(PreDeleteValidation::Protected(_reason)) => {
                    drop(_gate);
                    drop(auth_guard);
                    continue;
                }
                Err(e) => {
                    drop(_gate);
                    drop(auth_guard);
                    return Err(e);
                }
            }

            let del_res = storage
                .delete_blob_conditional(&permit, &candidate.digest, Some(&candidate.version))
                .await;

            drop(_gate);
            drop(auth_guard);

            match del_res {
                Ok(storage::GcDeleteResult::Deleted) => {
                    stats.deleted_blobs += 1;
                    stats.deleted_bytes = stats.deleted_bytes.saturating_add(candidate.size);
                }
                Ok(storage::GcDeleteResult::NotFound) => {}
                Ok(storage::GcDeleteResult::PreconditionFailed { .. }) => {}
                Err(e) => return Err(e.to_string()),
            }
        }

        if stats.deleted_blobs as usize >= limits.max_per_run
            || stats.deleted_bytes >= limits.max_bytes
            || t0.elapsed() > Duration::from_secs(limits.max_seconds)
        {
            break;
        }
    }

    Ok(stats)
}

pub async fn blob_gc_sweep(
    cfg: &crate::config::Config,
    storage: &Arc<dyn storage::Storage>,
    idx: &BlobRefIndex,
    consistency_gate: &Arc<Mutex<()>>,
    mutation_authority: &Arc<Mutex<Option<RuntimeMutationAuthority>>>,
    policy: BlobGcPolicy,
    min_age: Duration,
    limits: BlobGcLimits,
) -> Result<BlobGcStats, String> {
    match storage.gc_strategy() {
        storage::GcStorageStrategy::FilesystemQuarantine => {
            let mut stats = blob_gc_quarantine(
                cfg,
                storage,
                idx,
                consistency_gate,
                mutation_authority,
                policy,
                min_age,
                limits,
            )
            .await?;

            let del_stats = blob_gc_delete_fs(
                cfg,
                storage,
                idx,
                consistency_gate,
                mutation_authority,
                policy,
                min_age,
                limits,
            )
            .await?;
            stats.deleted_blobs = del_stats.deleted_blobs;
            stats.deleted_bytes = del_stats.deleted_bytes;
            stats.restored_blobs = del_stats.restored_blobs;
            stats.restored_bytes = del_stats.restored_bytes;
            Ok(stats)
        }
        storage::GcStorageStrategy::S3DirectConditional => {
            blob_gc_delete(
                cfg,
                storage,
                idx,
                consistency_gate,
                mutation_authority,
                policy,
                min_age,
                limits,
            )
            .await
        }
    }
}

fn quarantine_meta_path(cfg: &crate::config::Config, digest: &Digest) -> PathBuf {
    cfg.fs_root
        .join("quarantine")
        .join("meta")
        .join(digest.algorithm())
        .join(digest.prefix2())
        .join(format!("{}.ts", digest.hex()))
}

async fn write_quarantine_time(
    cfg: &crate::config::Config,
    digest: &Digest,
    at: SystemTime,
) -> Result<(), String> {
    let path = quarantine_meta_path(cfg, digest);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }

    let secs = at
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs();
    tokio::fs::write(&path, format!("{secs}\n"))
        .await
        .map_err(|e| format!("write {}: {e}", path.display()))?;

    Ok(())
}

async fn read_quarantine_time(
    cfg: &crate::config::Config,
    digest: &Digest,
) -> Result<Option<SystemTime>, String> {
    let path = quarantine_meta_path(cfg, digest);
    if let Ok(s) = tokio::fs::read_to_string(&path).await {
        if let Ok(secs) = s.trim().parse::<u64>() {
            return Ok(Some(UNIX_EPOCH + Duration::from_secs(secs)));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mutation_authority::RuntimeMutationAuthority;
    use sha2::Digest as Sha2Digest;

    #[test]
    fn test_candidate_age_boundary_matrix() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let min_age = Duration::from_secs(3600);

        // 1. Missing timestamp -> MissingTimestamp (fails closed)
        assert_eq!(
            check_candidate_age(UNIX_EPOCH, now, min_age),
            AgeEligibility::MissingTimestamp
        );

        // 2. Future timestamp -> FutureTimestamp (fails closed)
        let future = now + Duration::from_secs(60);
        assert_eq!(
            check_candidate_age(future, now, min_age),
            AgeEligibility::FutureTimestamp
        );

        // 3. Below limit -> IneligibleAge (preserved)
        let below = now - Duration::from_secs(3599);
        assert_eq!(
            check_candidate_age(below, now, min_age),
            AgeEligibility::IneligibleAge
        );

        // 4. Exactly at limit -> Eligible
        let exact = now - Duration::from_secs(3600);
        assert_eq!(
            check_candidate_age(exact, now, min_age),
            AgeEligibility::Eligible
        );

        // 5. Above limit -> Eligible
        let above = now - Duration::from_secs(7200);
        assert_eq!(
            check_candidate_age(above, now, min_age),
            AgeEligibility::Eligible
        );

        // 6. Explicit zero grace -> Eligible for past timestamp, fails closed on future
        assert_eq!(
            check_candidate_age(now - Duration::from_secs(1), now, Duration::ZERO),
            AgeEligibility::Eligible
        );
        assert_eq!(
            check_candidate_age(future, now, Duration::ZERO),
            AgeEligibility::FutureTimestamp
        );
    }

    #[tokio::test]
    async fn test_build_manifest_protected_set_aborts_on_unparsable_manifest() {
        let temp = tempfile::TempDir::new().unwrap();
        let fs_root = temp.path();

        let manifests_dir = fs_root
            .join("repos")
            .join("library")
            .join("test")
            .join("manifests");
        tokio::fs::create_dir_all(&manifests_dir).await.unwrap();

        let valid_hex = "1111111111111111111111111111111111111111111111111111111111111111";
        let valid_manifest = serde_json::json!({
            "schemaVersion": 2,
            "config": { "digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222" },
            "layers": [{ "digest": "sha256:3333333333333333333333333333333333333333333333333333333333333333" }]
        });
        tokio::fs::write(
            manifests_dir.join(valid_hex),
            serde_json::to_vec(&valid_manifest).unwrap(),
        )
        .await
        .unwrap();

        let protected = build_manifest_protected_set_fs(fs_root).await.unwrap();
        assert!(
            protected.contains(
                "sha256:2222222222222222222222222222222222222222222222222222222222222222"
            )
        );
        assert!(
            protected.contains(
                "sha256:3333333333333333333333333333333333333333333333333333333333333333"
            )
        );

        let malformed_hex = "4444444444444444444444444444444444444444444444444444444444444444";
        let malformed_manifest = serde_json::json!({
            "schemaVersion": 2,
            "config": { "digest": "sha256:5555555555555555555555555555555555555555555555555555555555555555" },
            "layers": [{ "digest": "sha256:invalid-hex" }]
        });
        tokio::fs::write(
            manifests_dir.join(malformed_hex),
            serde_json::to_vec(&malformed_manifest).unwrap(),
        )
        .await
        .unwrap();

        let err = build_manifest_protected_set_fs(fs_root).await.unwrap_err();
        assert!(err.contains("unparsable manifest"));
    }

    #[tokio::test]
    async fn test_revalidate_candidate_malformed_journal_fails_closed() {
        let (s3_storage, driver) = crate::storage::s3::tests::create_mock_storage();
        let storage: Arc<dyn storage::Storage> = Arc::new(s3_storage);

        let temp = tempfile::TempDir::new().unwrap();
        let idx = Arc::new(BlobRefIndex::open(temp.path().join("index.sled")).unwrap());
        idx.ensure_healthy_or_rebuild(&storage, true, true)
            .await
            .unwrap();

        let mut cfg = crate::config::Config::from_env().unwrap();
        cfg.fs_root = temp.path().to_path_buf();

        let candidate_bytes = b"test blob for malformed journal check";
        let hash = sha2::Sha256::digest(candidate_bytes);
        let hex = hex::encode(hash);
        let digest = Digest::parse(&format!("sha256:{hex}")).unwrap();
        let key = format!("blobs/sha256/{}/{}", &hex[0..2], &hex);
        driver.objects.lock().unwrap().insert(
            key,
            (
                bytes::Bytes::from_static(candidate_bytes),
                "\"etag\"".to_string(),
            ),
        );

        // Write corrupt/unparseable json to repo lifecycle journal
        let journal_key = "repos/journal-repo/meta/lifecycle_journal.json".to_string();
        driver.objects.lock().unwrap().insert(
            journal_key,
            (
                bytes::Bytes::from_static(b"{invalid-json-content}"),
                "\"etag\"".to_string(),
            ),
        );

        let candidate = storage::GcBlobCandidate {
            digest,
            size: candidate_bytes.len() as u64,
            last_modified: UNIX_EPOCH + Duration::from_secs(100),
            version: storage::BlobObjectVersion("\"etag\"".to_string()),
        };

        let mut policy_ctx =
            PolicyContext::build(&cfg, &storage, &idx, BlobGcPolicy::ManifestRooted)
                .await
                .unwrap();
        let err = revalidate_candidate_before_delete(
            &storage,
            &idx,
            &candidate,
            SystemTime::now(),
            &mut policy_ctx,
        )
        .await
        .unwrap_err();
        assert!(err.contains("corrupt lifecycle journal record"));
    }

    #[tokio::test]
    async fn test_blob_gc_plan_and_sweep_s3() {
        let (s3_storage, driver) = crate::storage::s3::tests::create_mock_storage();
        let storage: Arc<dyn storage::Storage> = Arc::new(s3_storage);

        let temp = tempfile::TempDir::new().unwrap();
        let idx = Arc::new(BlobRefIndex::open(temp.path().join("index.sled")).unwrap());
        idx.ensure_healthy_or_rebuild(&storage, true, true)
            .await
            .unwrap();

        let mut cfg = crate::config::Config::from_env().unwrap();
        cfg.fs_root = temp.path().to_path_buf();

        let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-gc")
            .await
            .unwrap();

        let orphan_bytes = b"orphan payload for gc";
        let orphan_hash = sha2::Sha256::digest(orphan_bytes);
        let orphan_hex = hex::encode(orphan_hash);
        let orphan_key = format!("blobs/sha256/{}/{}", &orphan_hex[0..2], &orphan_hex);
        driver.objects.lock().unwrap().insert(
            orphan_key.clone(),
            (
                bytes::Bytes::from_static(orphan_bytes),
                "\"etag_orphan\"".to_string(),
            ),
        );

        let pinned_bytes = b"pinned payload in-flight";
        let pinned_hash = sha2::Sha256::digest(pinned_bytes);
        let pinned_hex = hex::encode(pinned_hash);
        let pinned_digest = Digest::parse(&format!("sha256:{pinned_hex}")).unwrap();
        let pinned_key = format!("blobs/sha256/{}/{}", &pinned_hex[0..2], &pinned_hex);
        driver.objects.lock().unwrap().insert(
            pinned_key.clone(),
            (
                bytes::Bytes::from_static(pinned_bytes),
                "\"etag_pinned\"".to_string(),
            ),
        );
        idx.pin_blob(
            &pinned_digest,
            SystemTime::now() + Duration::from_secs(3600),
            "op-1",
        )
        .unwrap();

        let consistency_gate = Arc::new(Mutex::new(()));
        let authority_arc = Arc::new(Mutex::new(Some(authority)));

        let stats = blob_gc_plan(
            &cfg,
            &storage,
            &idx,
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            BlobGcLimits::unlimited(100),
        )
        .await
        .unwrap();

        assert_eq!(stats.eligible_blobs, 1);

        let sweep_stats = blob_gc_sweep(
            &cfg,
            &storage,
            &idx,
            &consistency_gate,
            &authority_arc,
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            BlobGcLimits::unlimited(100),
        )
        .await
        .unwrap();

        assert_eq!(sweep_stats.deleted_blobs, 1);
        assert!(!driver.objects.lock().unwrap().contains_key(&orphan_key));
        assert!(driver.objects.lock().unwrap().contains_key(&pinned_key));
    }
}
