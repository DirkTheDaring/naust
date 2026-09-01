mod support;
use bytes::Bytes;
use registry_rust::blob_gc::BlobGcPolicy;
use registry_rust::blob_ref_index::BlobRefIndex;
use registry_rust::config::Config;
use registry_rust::consistency::ConsistencyCoordinator;
use registry_rust::gc_service::{GcBudgets, GcService};
use registry_rust::manifest_lifecycle::{LifecycleJournalRecord, LifecycleOpKind, LifecyclePhase};
use registry_rust::registry::canonical_name::CanonicalRepoName;
use registry_rust::registry::digest::Digest;
use registry_rust::storage::fs::FsStorage;
use registry_rust::storage::mutation_authority::RuntimeMutationAuthority;
use registry_rust::storage::repo_membership::RepoBlobMembershipRecord;
use registry_rust::storage::repo_membership::RepositoryBlobMembershipStorage;
use registry_rust::storage::{self, GcDeleteResult, GcQuarantineResult, GcStorage, Storage};
use sha2::Digest as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use support::gc_coordination::*;
use tokio::sync::{Barrier, Mutex};

#[allow(dead_code)]
async fn write_manifest(
    fs_root: &std::path::Path,
    repo: &str,
    tag: &str,
    manifest_digest: &Digest,
    manifest_bytes: &[u8],
) {
    let repo_dir = fs_root.join("repos").join(repo);
    let tags_dir = repo_dir.join("tags");
    let manifests_dir = repo_dir.join("manifests");
    tokio::fs::create_dir_all(&tags_dir)
        .await
        .expect("mkdir tags");
    tokio::fs::create_dir_all(&manifests_dir)
        .await
        .expect("mkdir manifests");

    tokio::fs::write(
        tags_dir.join(tag),
        format!("{}\n", manifest_digest.as_str()),
    )
    .await
    .expect("write tag");
    tokio::fs::write(manifests_dir.join(manifest_digest.hex()), manifest_bytes)
        .await
        .expect("write manifest");
}

// -------------------------------------------------------------------------------------------------
// 1. Manifest publication acquires the shared gate first; GC waits, revalidates afterward,
//    and preserves the now-referenced blob.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_manifest_publication_acquires_gate_first_gc_revalidates_and_preserves_blob() {
    let fs_root = tmp_dir("adv-pub-first");
    let ref_index_path = fs_root.join("ref-index");
    let cfg = Arc::new(test_config(fs_root.clone(), ref_index_path.clone()));

    let storage = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .expect("ensure idx");

    let coordinator = ConsistencyCoordinator::new();
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-adv-1")
        .await
        .expect("authority");

    let service = GcService::with_coordinator_and_authority(
        cfg.clone(),
        storage.clone(),
        idx.clone(),
        coordinator.clone(),
        Arc::new(Mutex::new(Some(authority))),
    );

    // Create an unreferenced blob and quarantine it.
    let data = b"shared-gate-test-blob";
    let hex = hex::encode(sha2::Sha256::digest(data));
    let digest = Digest::parse(&format!("sha256:{hex}")).expect("digest");
    write_live_blob(&fs_root, &digest, data).await;

    let budgets = GcBudgets {
        max_blobs: 1000,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };

    let q_stats = service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets.clone(),
        )
        .await
        .expect("quarantine");
    assert_eq!(q_stats.quarantined_blobs, 1);

    let pub_started = Arc::new(Barrier::new(2));
    let pub_started_clone = pub_started.clone();
    let coordinator_clone = coordinator.clone();
    let fs_root_clone = fs_root.clone();
    let storage_clone = storage.clone();
    let idx_clone = idx.clone();
    let digest_clone = digest.clone();

    // Manifest publication task holds consistency coordinator, writes manifest + membership
    let pub_handle = tokio::spawn(async move {
        let _guard = coordinator_clone.acquire_mutation().await;
        pub_started_clone.wait().await;

        // Publish manifest referencing the blob
        let root = Digest::parse(&format!("sha256:{}", "b".repeat(64))).expect("digest");
        let cfg_digest = Digest::parse(&format!("sha256:{}", "c".repeat(64))).expect("digest");
        let manifest = format!(
            "{{\"schemaVersion\":2,\"config\":{{\"digest\":\"{}\"}},\"layers\":[{{\"digest\":\"{}\"}}]}}",
            cfg_digest.as_str(),
            digest_clone.as_str()
        );
        write_manifest(&fs_root_clone, "myrepo", "v1.0", &root, manifest.as_bytes()).await;

        let canonical_repo = CanonicalRepoName::parse("myrepo").unwrap();
        let record =
            RepoBlobMembershipRecord::new_upload(canonical_repo, digest_clone.clone(), None);
        storage_clone.link_repo_blob(&record).await.unwrap();

        idx_clone.rebuild(&storage_clone).await.unwrap();
    });

    pub_started.wait().await;

    // GC delete runs concurrently; it must wait for the gate, revalidate under the gate,
    // and see that the blob is now referenced and restore it!
    let d_stats = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("delete");

    pub_handle.await.expect("join pub");

    assert_eq!(d_stats.deleted_blobs, 0, "blob must NOT be deleted");
    assert_eq!(d_stats.restored_blobs, 1, "blob must be restored to live");

    let live = fs_root
        .join("blobs")
        .join("sha256")
        .join(digest.prefix2())
        .join(digest.hex());
    assert!(live.exists(), "blob must exist in live store");

    let _ = std::fs::remove_dir_all(&fs_root);
}

// -------------------------------------------------------------------------------------------------
// 2. GC acquires the shared gate first; publication cannot enter its mutation section until GC finishes.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_gc_acquires_gate_first_publication_blocked_until_gc_finishes() {
    let fs_root = tmp_dir("adv-gc-first");
    let ref_index_path = fs_root.join("ref-index");
    let cfg = Arc::new(test_config(fs_root.clone(), ref_index_path.clone()));

    let storage = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .expect("ensure idx");

    let coordinator = ConsistencyCoordinator::new();
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-adv-2")
        .await
        .expect("authority");

    let service = GcService::with_coordinator_and_authority(
        cfg.clone(),
        storage.clone(),
        idx.clone(),
        coordinator.clone(),
        Arc::new(Mutex::new(Some(authority))),
    );

    // Create an unreferenced blob and quarantine it.
    let data = b"unreferenced-deleted-blob";
    let hex = hex::encode(sha2::Sha256::digest(data));
    let digest = Digest::parse(&format!("sha256:{hex}")).expect("digest");
    write_live_blob(&fs_root, &digest, data).await;

    let budgets = GcBudgets {
        max_blobs: 1000,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };

    service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets.clone(),
        )
        .await
        .expect("quarantine");

    let d_stats = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("delete ok");

    let coordinator_clone = coordinator.clone();
    let fs_root_clone = fs_root.clone();
    let digest_clone = digest.clone();

    // Spawn publication task after GC to verify mutation enters cleanly and observes deleted state
    let pub_handle = tokio::spawn(async move {
        let _guard = coordinator_clone.acquire_mutation().await;

        let q = fs_root_clone
            .join("quarantine")
            .join("blobs")
            .join("sha256")
            .join(digest_clone.prefix2())
            .join(digest_clone.hex());
        let live = fs_root_clone
            .join("blobs")
            .join("sha256")
            .join(digest_clone.prefix2())
            .join(digest_clone.hex());
        (!q.exists(), !live.exists())
    });

    let (q_deleted, live_deleted) = pub_handle.await.expect("join pub");

    assert_eq!(d_stats.deleted_blobs, 1);
    assert!(
        q_deleted,
        "quarantined blob must be deleted when publication enters"
    );
    assert!(live_deleted, "live blob must not exist");

    let _ = std::fs::remove_dir_all(&fs_root);
}

// -------------------------------------------------------------------------------------------------
// 3. Repository membership created before final deletion causes the candidate to be skipped.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_repo_membership_created_before_final_deletion_causes_candidate_skipped() {
    let fs_root = tmp_dir("adv-membership-skip");
    let ref_index_path = fs_root.join("ref-index");
    let cfg = Arc::new(test_config(fs_root.clone(), ref_index_path.clone()));

    let storage = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .expect("ensure idx");

    let coordinator = ConsistencyCoordinator::new();
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-adv-3")
        .await
        .expect("authority");

    let service = GcService::with_coordinator_and_authority(
        cfg.clone(),
        storage.clone(),
        idx.clone(),
        coordinator.clone(),
        Arc::new(Mutex::new(Some(authority))),
    );

    let data = b"membership-protected-blob";
    let hex = hex::encode(sha2::Sha256::digest(data));
    let digest = Digest::parse(&format!("sha256:{hex}")).expect("digest");
    write_live_blob(&fs_root, &digest, data).await;

    let budgets = GcBudgets {
        max_blobs: 1000,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };

    service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets.clone(),
        )
        .await
        .expect("quarantine");

    // Add repository membership before deletion
    let canonical_repo = CanonicalRepoName::parse("protected-repo").unwrap();
    let record = RepoBlobMembershipRecord::new_upload(canonical_repo, digest.clone(), None);
    storage.link_repo_blob(&record).await.unwrap();

    let d_stats = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("delete");

    assert_eq!(
        d_stats.deleted_blobs, 0,
        "blob with active membership must NOT be deleted"
    );

    let q = fs_root
        .join("quarantine")
        .join("blobs")
        .join("sha256")
        .join(digest.prefix2())
        .join(digest.hex());
    assert!(q.exists(), "quarantined blob must be preserved");

    let _ = std::fs::remove_dir_all(&fs_root);
}

// -------------------------------------------------------------------------------------------------
// 4. A pin/finalizing-upload state introduced before final validation causes a skip.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_pin_finalizing_upload_state_causes_candidate_skipped() {
    let fs_root = tmp_dir("adv-pin-skip");
    let ref_index_path = fs_root.join("ref-index");
    let cfg = Arc::new(test_config(fs_root.clone(), ref_index_path.clone()));

    let storage = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .expect("ensure idx");

    let coordinator = ConsistencyCoordinator::new();
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-adv-4")
        .await
        .expect("authority");

    let service = GcService::with_coordinator_and_authority(
        cfg.clone(),
        storage.clone(),
        idx.clone(),
        coordinator.clone(),
        Arc::new(Mutex::new(Some(authority))),
    );

    let data = b"pin-protected-blob";
    let hex = hex::encode(sha2::Sha256::digest(data));
    let digest = Digest::parse(&format!("sha256:{hex}")).expect("digest");
    write_live_blob(&fs_root, &digest, data).await;

    let budgets = GcBudgets {
        max_blobs: 1000,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };

    service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets.clone(),
        )
        .await
        .expect("quarantine");

    // Pin the blob in the ref index
    idx.pin_blob(
        &digest,
        SystemTime::now() + Duration::from_secs(3600),
        "in-flight-test",
    )
    .expect("pin blob");

    let d_stats = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("delete");

    assert_eq!(d_stats.deleted_blobs, 0, "pinned blob must NOT be deleted");

    let q = fs_root
        .join("quarantine")
        .join("blobs")
        .join("sha256")
        .join(digest.prefix2())
        .join(digest.hex());
    assert!(q.exists(), "quarantined blob must be preserved");

    let _ = std::fs::remove_dir_all(&fs_root);
}

// -------------------------------------------------------------------------------------------------
// 5. A lifecycle journal appearing before final validation causes a skip.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_lifecycle_journal_appearing_before_validation_causes_candidate_skipped() {
    let fs_root = tmp_dir("adv-journal-skip");
    let ref_index_path = fs_root.join("ref-index");
    let cfg = Arc::new(test_config(fs_root.clone(), ref_index_path.clone()));

    let storage = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .expect("ensure idx");

    let coordinator = ConsistencyCoordinator::new();
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-adv-5")
        .await
        .expect("authority");

    let service = GcService::with_coordinator_and_authority(
        cfg.clone(),
        storage.clone(),
        idx.clone(),
        coordinator.clone(),
        Arc::new(Mutex::new(Some(authority))),
    );

    let data = b"journal-protected-blob";
    let hex = hex::encode(sha2::Sha256::digest(data));
    let digest = Digest::parse(&format!("sha256:{hex}")).expect("digest");
    write_live_blob(&fs_root, &digest, data).await;

    let budgets = GcBudgets {
        max_blobs: 1000,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };

    service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets.clone(),
        )
        .await
        .expect("quarantine");

    // Write an active lifecycle journal record referencing this blob
    let journal = LifecycleJournalRecord {
        op_id: "op-12345".to_string(),
        repo: CanonicalRepoName::parse("journal-repo").unwrap(),
        op_kind: LifecycleOpKind::Publish,
        target_digest: digest.clone(),
        target_reference: Some("latest".to_string()),
        phase: LifecyclePhase::ManifestStored,
        owner_id: "test-owner".to_string(),
        lease_expiry_unix_secs: 10000,
        started_unix_secs: 1000,
        updated_unix_secs: 1000,
        relevant_tags: vec![],
        subject_digest: None,
        artifact_type: None,
        annotations: None,
        media_type: None,
        manifest_size: None,
    };
    let journal_bytes = Bytes::from(serde_json::to_vec(&journal).unwrap());
    storage
        .write_lifecycle_journal("journal-repo", journal_bytes)
        .await
        .unwrap();

    let d_stats = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("delete");

    assert_eq!(
        d_stats.deleted_blobs, 0,
        "blob referenced in active journal must NOT be deleted"
    );

    let q = fs_root
        .join("quarantine")
        .join("blobs")
        .join("sha256")
        .join(digest.prefix2())
        .join(digest.hex());
    assert!(q.exists(), "quarantined blob must be preserved");

    let _ = std::fs::remove_dir_all(&fs_root);
}

// -------------------------------------------------------------------------------------------------
// 6. Filesystem quarantined object replacement/version change causes PreconditionFailed and preserves.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_fs_quarantined_object_version_mismatch_returns_precondition_failed_and_preserves() {
    let fs_root = tmp_dir("adv-version-mismatch");
    let ref_index_path = fs_root.join("ref-index");
    let cfg = Arc::new(test_config(fs_root.clone(), ref_index_path.clone()));

    let storage = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-adv-6")
        .await
        .expect("authority");
    let permit = authority.gc_mutation_permit();

    let blob_a = b"original-quarantined-content";
    let hex = hex::encode(sha2::Sha256::digest(blob_a));
    let digest = Digest::parse(&format!("sha256:{hex}")).expect("digest");
    write_live_blob(&fs_root, &digest, blob_a).await;

    // Quarantine original blob
    let q_res = storage
        .quarantine_blob(
            &permit,
            &digest,
            &storage::BlobObjectVersion("init".to_string()),
        )
        .await
        .unwrap();
    assert!(matches!(q_res, GcQuarantineResult::Quarantined { .. }));

    let v1 = storage
        .quarantined_blob_version(&digest)
        .await
        .unwrap()
        .expect("v1");

    // Tamper with the quarantined file (simulate concurrent overwrite / corruption)
    let q_path = fs_root
        .join("quarantine")
        .join("blobs")
        .join("sha256")
        .join(digest.prefix2())
        .join(digest.hex());
    tokio::fs::write(&q_path, b"tampered-or-replaced-content")
        .await
        .unwrap();

    // Conditional delete using the original expected version V1
    let del_res = storage
        .delete_blob_conditional(&permit, &digest, Some(&v1))
        .await
        .unwrap();
    assert!(
        matches!(del_res, GcDeleteResult::PreconditionFailed { .. }),
        "must return PreconditionFailed on version mismatch"
    );

    // Quarantined file must be preserved
    assert!(
        q_path.exists(),
        "tampered/replaced quarantined file must be preserved on disk"
    );

    let _ = std::fs::remove_dir_all(&fs_root);
}

// -------------------------------------------------------------------------------------------------
// 7. Admin and scheduler code cannot/must not mint permits directly.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_admin_and_scheduler_have_no_permit_access() {
    let fs_root = tmp_dir("adv-no-permits");
    let ref_index_path = fs_root.join("ref-index");
    let cfg = Arc::new(test_config(fs_root.clone(), ref_index_path.clone()));

    let storage = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-adv-7")
        .await
        .expect("authority");

    let service = GcService::with_authority(
        cfg.clone(),
        storage.clone(),
        idx.clone(),
        ConsistencyCoordinator::new(),
        authority,
    );

    // Call service methods directly without passing any permit
    let budgets = GcBudgets {
        max_blobs: 10,
        max_bytes: 1024,
        max_seconds: 10,
    };

    let q = service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets.clone(),
        )
        .await;
    assert!(q.is_ok());

    let d = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await;
    assert!(d.is_ok());

    let s = service.scheduled_cleanup_once().await;
    assert!(s.is_ok());

    let _ = std::fs::remove_dir_all(&fs_root);
}

// -------------------------------------------------------------------------------------------------
// 8. CLI acquires mutation authority once and routes mutation through GcService.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_cli_acquires_authority_once_and_routes_through_service() {
    let fs_root = tmp_dir("adv-cli-once");
    let ref_index_path = fs_root.join("ref-index");
    let cfg = Arc::new(test_config(fs_root.clone(), ref_index_path.clone()));

    let storage = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .expect("ensure idx");

    // CLI workflow: acquire authority once
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "cli-test")
        .await
        .expect("cli authority");

    let service = GcService::with_authority(
        cfg.clone(),
        storage.clone(),
        idx.clone(),
        ConsistencyCoordinator::new(),
        authority,
    );

    let budgets = GcBudgets {
        max_blobs: 100,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };

    let stats = service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets.clone(),
        )
        .await
        .expect("cli quarantine");
    assert_eq!(stats.scanned_blobs, 0);

    let del_stats = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("cli delete");
    assert_eq!(del_stats.deleted_blobs, 0);

    let _ = std::fs::remove_dir_all(&fs_root);
}

// -------------------------------------------------------------------------------------------------
// 9. Concurrent lifecycle mutation progresses between two GC candidates (Bounded Transaction)
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_concurrent_lifecycle_mutation_progresses_between_gc_candidates() {
    let fs_root = tmp_dir("adv-bounded-tx");
    let ref_index_path = fs_root.join("ref-index");
    let cfg = Arc::new(test_config(fs_root.clone(), ref_index_path.clone()));

    let base_storage = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    idx.ensure_healthy_or_rebuild(&base_storage, true, true)
        .await
        .expect("ensure idx");

    let coordinator = ConsistencyCoordinator::new();
    let authority = RuntimeMutationAuthority::acquire(base_storage.clone(), "adv-bounded-test")
        .await
        .expect("authority");

    // Create 2 unreferenced blobs and quarantine them
    let data1 = b"first-candidate-to-delete";
    let hex1 = hex::encode(sha2::Sha256::digest(data1));
    let digest1 = Digest::parse(&format!("sha256:{hex1}")).expect("digest1");
    write_live_blob(&fs_root, &digest1, data1).await;

    let data2 = b"second-candidate-to-save";
    let hex2 = hex::encode(sha2::Sha256::digest(data2));
    let digest2 = Digest::parse(&format!("sha256:{hex2}")).expect("digest2");
    write_live_blob(&fs_root, &digest2, data2).await;

    let (cand_1_deleted_tx, cand_1_deleted_rx) = tokio::sync::oneshot::channel();
    let (mutation_queued_tx, mutation_queued_rx) = tokio::sync::oneshot::channel();
    let cand_1_deleted_tx = Arc::new(tokio::sync::Mutex::new(Some(cand_1_deleted_tx)));
    let mutation_queued_rx = Arc::new(tokio::sync::Mutex::new(Some(mutation_queued_rx)));
    let deleted_first = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut hooks = StorageHooks::default();
    let c1_tx = cand_1_deleted_tx.clone();
    let m_q_rx = mutation_queued_rx.clone();
    let del_first = deleted_first.clone();
    hooks.after_delete_blob_conditional = Some(Arc::new(move |d| {
        let c1_tx = c1_tx.clone();
        let m_q_rx = m_q_rx.clone();
        let del_first = del_first.clone();
        let d_clone = d.clone();
        Box::pin(async move {
            if !del_first.swap(true, std::sync::atomic::Ordering::SeqCst) {
                if let Some(tx) = c1_tx.lock().await.take() {
                    let _ = tx.send(d_clone);
                }
                // Wait until the mutation task is queued on coordinator.acquire_mutation()
                // BEFORE candidate 1 drops its guard.
                if let Some(rx) = m_q_rx.lock().await.take() {
                    let _ = rx.await;
                }
            }
        })
    }));

    let hooked_storage = Arc::new(HookedStorage::new(base_storage.clone(), hooks));

    let service = GcService::with_coordinator_and_authority(
        cfg.clone(),
        hooked_storage.clone(),
        idx.clone(),
        coordinator.clone(),
        Arc::new(Mutex::new(Some(authority))),
    );

    let budgets = GcBudgets {
        max_blobs: 100,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };

    service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets.clone(),
        )
        .await
        .expect("quarantine");

    // Spawn concurrent publication task that waits for whichever blob is deleted first,
    // acquires consistency coordinator between items, and links the remaining blob
    // to repository membership before GC can revalidate it.
    let coordinator_clone = coordinator.clone();
    let storage_clone = base_storage.clone();
    let idx_clone = idx.clone();
    let digest1_pub = digest1.clone();
    let digest2_pub = digest2.clone();

    let pub_task = tokio::spawn(async move {
        let first_d = cand_1_deleted_rx.await.expect("candidate 1 deleted signal");
        let remaining_digest = if first_d == digest1_pub {
            digest2_pub
        } else {
            digest1_pub
        };

        // Notify hook that we are immediately acquiring the coordinator mutex
        let _ = mutation_queued_tx.send(());

        // Acquire consistency coordinator between candidate 1 and candidate 2
        let _guard = coordinator_clone.acquire_mutation().await;

        // Mutate lifecycle under the gate: link remaining blob to repository membership
        let canonical_repo =
            registry_rust::registry::canonical_name::CanonicalRepoName::parse("saved-repo")
                .unwrap();
        let record = RepoBlobMembershipRecord::new_upload(
            canonical_repo,
            remaining_digest,
            Some(uuid::Uuid::new_v4().to_string()),
        );
        storage_clone.link_repo_blob(&record).await.unwrap();
        idx_clone
            .record_membership(&record.digest, record.repo.as_str())
            .unwrap();
    });

    let gc_task = tokio::spawn(async move {
        service
            .delete(
                BlobGcPolicy::ManifestRooted,
                Duration::from_secs(0),
                budgets,
            )
            .await
    });

    pub_task.await.expect("join pub");
    let d_stats = gc_task.await.unwrap().expect("delete");

    assert_eq!(
        d_stats.deleted_blobs, 1,
        "only 1 blob should be deleted; the other must be preserved by concurrent mutation"
    );

    let q1 = fs_root
        .join("quarantine")
        .join("blobs")
        .join("sha256")
        .join(digest1.prefix2())
        .join(digest1.hex());
    let q2 = fs_root
        .join("quarantine")
        .join("blobs")
        .join("sha256")
        .join(digest2.prefix2())
        .join(digest2.hex());

    assert!(
        (!q1.exists() && q2.exists()) || (q1.exists() && !q2.exists()),
        "exactly one blob must be deleted from quarantine and one preserved"
    );

    let _ = std::fs::remove_dir_all(&fs_root);
}

// -------------------------------------------------------------------------------------------------
// 10. Proxy blob publication coordinator invariant & race protection against GC.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_proxy_blob_publication_vs_gc_race_barrier_and_idempotency() {
    let fs_root = tmp_dir("proxy-pub-race");
    let ref_index_path = fs_root.join("index.sled");
    let cfg = test_config(fs_root.clone(), ref_index_path.clone());

    let storage = Arc::new(FsStorage::try_new(fs_root.clone(), 0).unwrap());
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let coordinator = ConsistencyCoordinator::new();
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-gc")
        .await
        .unwrap();
    let authority_arc = Arc::new(Mutex::new(Some(authority)));

    let upload_coord_config = registry_rust::upload_coordinator::BlobUploadCoordinatorConfig {
        signing_key: b"secret".to_vec(),
        max_upload_bytes: 0,
        abort_on_digest_mismatch: true,
        disallow_monolithic_uploads: false,
        upload_chunk_min_bytes: None,
        gc_pin_duration_secs: 300,
    };
    let upload_coordinator = registry_rust::upload_coordinator::BlobUploadCoordinator::new(
        storage.clone(),
        Some(idx.clone()),
        coordinator.clone(),
        upload_coord_config,
    );

    let service = GcService::with_coordinator_and_authority(
        Arc::new(cfg.clone()),
        storage.clone(),
        idx.clone(),
        coordinator.clone(),
        authority_arc.clone(),
    );

    let proxy_bytes = b"proxy-fetched-blob-data-payload";
    let hash = sha2::Sha256::digest(proxy_bytes);
    let hex = hex::encode(hash);
    let digest = Digest::parse(&format!("sha256:{hex}")).unwrap();
    let canonical_repo = CanonicalRepoName::parse("proxy/target-repo").unwrap();

    // 1. Manually simulate state right after CAS publication with active durable pin (before membership link)
    let op_id = "test-proxy-in-flight-op";
    idx.pin_blob(
        &digest,
        SystemTime::now() + Duration::from_secs(3600),
        op_id,
    )
    .unwrap();
    write_live_blob(&fs_root, &digest, proxy_bytes).await;

    // 2. Run GC quarantine sweep while pin is held: blob MUST remain protected and NOT quarantined
    let budgets = GcBudgets {
        max_blobs: 1000,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };
    let q_stats = service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("quarantine");
    assert_eq!(
        q_stats.quarantined_blobs, 0,
        "blob with active pin must NOT be quarantined"
    );

    // Release simulated in-flight pin before coordinator publication
    idx.unpin_blob(&digest, "default").unwrap();

    // 3. Now execute full publication via publish_proxy_blob
    let stream: registry_rust::storage::upload_session::UploadByteStream =
        Box::pin(futures_util::stream::once(async move {
            Ok::<_, registry_rust::storage::upload_session::UploadStreamError>(
                bytes::Bytes::from_static(proxy_bytes),
            )
        }));
    upload_coordinator
        .publish_proxy_blob(&canonical_repo, &digest, stream)
        .await
        .expect("publish proxy blob");

    // 4. Verify membership was durably linked and pin was released
    let mem = storage
        .get_repo_blob_membership(canonical_repo.as_str(), &digest)
        .await
        .expect("get membership")
        .expect("membership must exist");
    assert_eq!(mem.digest, digest);
    assert!(!idx.is_blob_pinned(&digest, SystemTime::now()).unwrap());

    // 5. Verify duplicate publication is idempotent
    let stream2: registry_rust::storage::upload_session::UploadByteStream =
        Box::pin(futures_util::stream::once(async move {
            Ok::<_, registry_rust::storage::upload_session::UploadStreamError>(
                bytes::Bytes::from_static(proxy_bytes),
            )
        }));
    upload_coordinator
        .publish_proxy_blob(&canonical_repo, &digest, stream2)
        .await
        .expect("idempotent duplicate publish");

    let _ = std::fs::remove_dir_all(&fs_root);
}

// -------------------------------------------------------------------------------------------------
// 11. S3 Versioning Capability 4-state matrix fails closed and never calls delete driver for rejected states.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_s3_bucket_versioning_capability_4_state_matrix_fails_closed() {
    let (s3_storage, driver) = support::s3_mock::create_mock_storage();
    let storage = Arc::new(s3_storage);

    let temp = tempfile::TempDir::new().unwrap();
    let idx = Arc::new(BlobRefIndex::open(temp.path().join("index.sled")).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let mut cfg = Config::from_env().unwrap();
    cfg.fs_root = temp.path().to_path_buf();
    cfg.blob_gc_enabled = true;
    cfg.blob_gc_enable_delete = true;

    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-gc")
        .await
        .unwrap();
    let authority_arc = Arc::new(Mutex::new(Some(authority)));
    let coordinator = ConsistencyCoordinator::new();

    let service = GcService::with_coordinator_and_authority(
        Arc::new(cfg),
        storage.clone(),
        idx.clone(),
        coordinator,
        authority_arc,
    );

    let budgets = GcBudgets {
        max_blobs: 100,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };

    // State 1: Enabled -> StrategyUnsupported error, delete driver never called
    *driver.versioning_state.lock().unwrap() =
        registry_rust::storage::s3::S3BucketVersioningState::Enabled;
    let err1 = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets.clone(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err1,
            registry_rust::gc_service::GcServiceError::StrategyUnsupported { .. }
        ),
        "expected StrategyUnsupported for Enabled, got: {:?}",
        err1
    );

    // State 2: Suspended -> StrategyUnsupported error, delete driver never called
    *driver.versioning_state.lock().unwrap() =
        registry_rust::storage::s3::S3BucketVersioningState::Suspended;
    let err2 = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets.clone(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err2,
            registry_rust::gc_service::GcServiceError::StrategyUnsupported { .. }
        ),
        "expected StrategyUnsupported for Suspended, got: {:?}",
        err2
    );

    // State 3: Unknown/Denied -> StrategyUnsupported error, delete driver never called
    *driver.versioning_state.lock().unwrap() =
        registry_rust::storage::s3::S3BucketVersioningState::UnknownOrDenied(
            "AccessDenied: 403 Forbidden".to_string(),
        );
    let err3 = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets.clone(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err3,
            registry_rust::gc_service::GcServiceError::StrategyUnsupported { .. }
        ),
        "expected StrategyUnsupported for UnknownOrDenied, got: {:?}",
        err3
    );

    // Assert that delete was never called in call log for any of the rejected states
    let log = driver.get_call_log();
    for entry in log {
        assert_ne!(
            entry.method, "delete_object_conditional",
            "delete_object_conditional must NEVER be called when versioning is not Unversioned"
        );
    }

    // State 4: Unversioned -> succeeds
    *driver.versioning_state.lock().unwrap() =
        registry_rust::storage::s3::S3BucketVersioningState::Unversioned;
    let ok = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await;
    assert!(ok.is_ok(), "Unversioned state must succeed: {:?}", ok);
}

// -------------------------------------------------------------------------------------------------
// 12. S3 Repository enumeration pagination & fail-closed error matrix.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn test_s3_repository_enumeration_pagination_and_fail_closed_matrix() {
    let (s3_storage, driver) = support::s3_mock::create_mock_storage();

    // 1. Page 1 empty with continuation, Page 2 contains journal-only repo
    driver.objects.lock().unwrap().insert(
        "repos/page2-repo/meta/lifecycle_journal.json".to_string(),
        (bytes::Bytes::from_static(b"{}"), "\"etag\"".to_string()),
    );

    let repos = s3_storage.list_repositories().await.unwrap();
    assert!(
        repos.contains(&"page2-repo".to_string()),
        "journal-only repo on page 2 must be found"
    );

    // 2. Malformed repository membership key in S3 -> fails closed
    driver.objects.lock().unwrap().insert(
        "repo-memberships/by-repo/invalid!!!base64==/sha256/11/1111111111111111111111111111111111111111111111111111111111111111".to_string(),
        (bytes::Bytes::from_static(b"{}"), "\"etag\"".to_string()),
    );

    let err = s3_storage.list_repositories().await.unwrap_err();
    assert!(
        err.to_string()
            .contains("malformed repository membership key"),
        "malformed repo key must fail closed: {err}"
    );
}

// =================================================================================================
// Integration tests: Proving ConsistencyCoordinator correctness across application use cases
// =================================================================================================

// 1. Manifest publication holds MutationGuard across its full critical section; GC revalidation cannot enter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_integration_manifest_publication_holds_mutation_guard_excluding_gc_revalidation() {
    let fs_root = tmp_dir("integ-manifest-excl-gc");
    let ref_idx_path = tmp_dir("integ-manifest-excl-gc-idx");
    let base_storage = Arc::new(FsStorage::new(fs_root.clone(), 10 * 1024 * 1024));
    let ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());
    ref_index
        .ensure_healthy_or_rebuild(&base_storage, true, true)
        .await
        .unwrap();
    let coordinator = ConsistencyCoordinator::new();

    let (in_crit_tx, in_crit_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let in_crit_tx = Arc::new(tokio::sync::Mutex::new(Some(in_crit_tx)));
    let resume_rx = Arc::new(tokio::sync::Mutex::new(Some(resume_rx)));

    let mut hooks = StorageHooks::default();
    let in_tx = in_crit_tx.clone();
    let res_rx = resume_rx.clone();
    // Hook after mutate_tag has durably written the tag to storage, before journal deletion
    hooks.after_mutate_tag = Some(Arc::new(move |(_repo, _tag, _target)| {
        let in_tx = in_tx.clone();
        let res_rx = res_rx.clone();
        Box::pin(async move {
            if let Some(tx) = in_tx.lock().await.take() {
                let _ = tx.send(());
            }
            if let Some(rx) = res_rx.lock().await.take() {
                let _ = rx.await;
            }
        })
    }));

    let hooked_storage = Arc::new(HookedStorage::new(base_storage.clone(), hooks));

    let service = registry_rust::manifest_lifecycle::ManifestLifecycleService::new(
        hooked_storage.clone(),
        Some(ref_index.clone()),
        coordinator.clone(),
    );

    // Pre-create blob in CAS and repository membership
    let blob_bytes = bytes::Bytes::from_static(b"pre-existing manifest layer data");
    let mut hasher = sha2::Sha256::default();
    sha2::Digest::update(&mut hasher, &blob_bytes);
    let hex = hex::encode(sha2::Digest::finalize(hasher));
    let blob_digest = Digest::parse(&format!("sha256:{hex}")).unwrap();

    write_live_blob(&fs_root, &blob_digest, &blob_bytes).await;
    let rec = RepoBlobMembershipRecord::new_upload(
        CanonicalRepoName::parse("team/app").unwrap(),
        blob_digest.clone(),
        None,
    );
    base_storage.link_repo_blob(&rec).await.unwrap();

    let manifest_json = format!(
        r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","size":{size},"digest":"{d}"}},"layers":[{{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","size":{size},"digest":"{d}"}}]}}"#,
        size = blob_bytes.len(),
        d = blob_digest
    );
    let m_bytes = bytes::Bytes::from(manifest_json);
    let mut m_hasher = sha2::Sha256::default();
    sha2::Digest::update(&mut m_hasher, &m_bytes);
    let m_hex = hex::encode(sha2::Digest::finalize(m_hasher));
    let m_digest = Digest::parse(&format!("sha256:{m_hex}")).unwrap();

    let service_clone = service.clone();
    let pub_task = tokio::spawn(async move {
        let req = registry_rust::manifest_lifecycle::PublishManifestRequest::new(
            "team/app",
            "v1.0",
            m_bytes,
            Some("application/vnd.oci.image.manifest.v1+json".to_string()),
            true,
        );
        service_clone.publish_manifest(req).await
    });

    // 1. Wait until manifest publication has durably written the tag to storage and is paused before journal deletion
    in_crit_rx
        .await
        .expect("reached internal barrier after tag mutation");

    // Verify delegated storage write already completed
    assert_eq!(
        base_storage.resolve_tag("team/app", "v1.0").await.unwrap(),
        m_digest
    );
    assert!(
        base_storage
            .read_lifecycle_journal("team/app")
            .await
            .unwrap()
            .is_some()
    );

    // 2. Concurrently attempt GC revalidation -> MUST remain pending while publication holds mutation guard
    let gc_coord = coordinator.clone();
    let (gc_acquired_tx, mut gc_acquired_rx) = tokio::sync::oneshot::channel();
    let gc_task = tokio::spawn(async move {
        let _guard = gc_coord.acquire_gc_revalidation().await;
        let _ = gc_acquired_tx.send(());
    });

    tokio::task::yield_now().await;
    assert!(
        !pub_task.is_finished(),
        "publication task must remain incomplete while barrier is held"
    );
    assert!(
        gc_acquired_rx.try_recv().is_err(),
        "GC revalidation must NOT acquire while manifest publication holds mutation guard"
    );

    // 3. Release barrier
    let _ = resume_tx.send(());

    let pub_res = pub_task.await.unwrap();
    assert!(pub_res.is_ok(), "publication must succeed: {:?}", pub_res);

    // 4. Verify final durable state
    assert_eq!(
        base_storage.resolve_tag("team/app", "v1.0").await.unwrap(),
        m_digest
    );
    assert!(
        base_storage
            .read_lifecycle_journal("team/app")
            .await
            .unwrap()
            .is_none()
    );

    // 5. GC revalidation acquires only afterward
    gc_acquired_rx
        .await
        .expect("GC revalidation acquires after publication finishes");
    gc_task.await.unwrap();
}

// 2. Upload finalization holds MutationGuard through membership durability and before pin release.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_integration_upload_finalization_holds_guard_through_membership_durability() {
    let fs_root = tmp_dir("integ-upload-fin-guard");
    let ref_idx_path = tmp_dir("integ-upload-fin-guard-idx");
    let base_storage = Arc::new(FsStorage::new(fs_root.clone(), 10 * 1024 * 1024));
    let ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());
    ref_index
        .ensure_healthy_or_rebuild(&base_storage, true, true)
        .await
        .unwrap();
    let coordinator = ConsistencyCoordinator::new();

    let (in_crit_tx, in_crit_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let in_crit_tx = Arc::new(tokio::sync::Mutex::new(Some(in_crit_tx)));
    let resume_rx = Arc::new(tokio::sync::Mutex::new(Some(resume_rx)));

    let mut hooks = StorageHooks::default();
    let in_tx = in_crit_tx.clone();
    let res_rx = resume_rx.clone();
    // Intercept in after_commit_blob: CAS blob physically exists on disk, pin is active,
    // guard is held, but index flush and mark_ready have not yet occurred!
    hooks.after_commit_blob = Some(Arc::new(move |_d| {
        let in_tx = in_tx.clone();
        let res_rx = res_rx.clone();
        Box::pin(async move {
            if let Some(tx) = in_tx.lock().await.take() {
                let _ = tx.send(());
            }
            if let Some(rx) = res_rx.lock().await.take() {
                let _ = rx.await;
            }
        })
    }));

    let hooked_storage = Arc::new(HookedStorage::new(base_storage.clone(), hooks));

    let upload_coord = Arc::new(
        registry_rust::upload_coordinator::BlobUploadCoordinator::new(
            hooked_storage.clone(),
            Some(ref_index.clone()),
            coordinator.clone(),
            registry_rust::upload_coordinator::BlobUploadCoordinatorConfig {
                signing_key: b"test-key".to_vec(),
                max_upload_bytes: 10 * 1024 * 1024,
                abort_on_digest_mismatch: true,
                disallow_monolithic_uploads: false,
                upload_chunk_min_bytes: None,
                gc_pin_duration_secs: 3600,
            },
        ),
    );

    let start_res = upload_coord.start_upload("myrepo").await.unwrap();
    let chunk = bytes::Bytes::from_static(b"upload finalization data payload");
    let mut hasher = sha2::Sha256::default();
    sha2::Digest::update(&mut hasher, &chunk);
    let hex = hex::encode(sha2::Digest::finalize(hasher));
    let digest = Digest::parse(&format!("sha256:{hex}")).unwrap();

    let stream: registry_rust::storage::upload_session::UploadByteStream =
        Box::pin(futures_util::stream::once(futures_util::future::ready(
            Ok::<bytes::Bytes, registry_rust::storage::upload_session::UploadStreamError>(chunk),
        )));
    let append_res = upload_coord
        .append_upload(
            "myrepo",
            &start_res.session.uuid,
            &start_res.state_token,
            None,
            None,
            stream,
        )
        .await
        .unwrap();

    let upload_coord_clone = upload_coord.clone();
    let uuid = start_res.session.uuid.clone();
    let token = append_res.state_token.clone();
    let d_clone = digest.clone();
    let fin_task = tokio::spawn(async move {
        upload_coord_clone
            .finalize_upload("myrepo", &uuid, Some(&token), None, None, &d_clone)
            .await
    });

    // 1. Wait until upload finalization has committed CAS to storage and is paused before index durability
    in_crit_rx
        .await
        .expect("reached membership durability boundary");

    // Verify CAS exists directly on storage
    assert!(base_storage.open_blob(&digest).await.is_ok());
    // Verify publication pin is active in ref_index
    assert!(
        ref_index
            .is_blob_pinned(&digest, SystemTime::now())
            .unwrap()
    );
    // Verify repository membership is durable in storage
    assert!(
        base_storage
            .get_repo_blob_membership("myrepo", &digest)
            .await
            .unwrap()
            .is_some()
    );

    // 2. Attempt GC revalidation concurrently -> MUST remain pending
    let gc_coord = coordinator.clone();
    let (gc_acquired_tx, mut gc_acquired_rx) = tokio::sync::oneshot::channel();
    let gc_task = tokio::spawn(async move {
        let _guard = gc_coord.acquire_gc_revalidation().await;
        let _ = gc_acquired_tx.send(());
    });

    tokio::task::yield_now().await;
    assert!(
        !fin_task.is_finished(),
        "finalize task must remain incomplete while barrier is held"
    );
    assert!(
        gc_acquired_rx.try_recv().is_err(),
        "GC revalidation must NOT acquire while finalize_upload holds mutation guard before membership durability"
    );

    // 3. Release barrier
    let _ = resume_tx.send(());

    let fin_res = fin_task.await.unwrap();
    assert!(fin_res.is_ok(), "finalize must succeed: {:?}", fin_res);

    // 4. Verify final durable state: membership is durable, pin is released
    assert!(
        base_storage
            .get_repo_blob_membership("myrepo", &digest)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        !ref_index
            .is_blob_pinned(&digest, SystemTime::now())
            .unwrap()
    );

    // 5. GC revalidation acquires only afterward
    gc_acquired_rx
        .await
        .expect("GC revalidation acquires after finalize completes");
    gc_task.await.unwrap();
}

// 3. Proxy blob publication holds MutationGuard through membership durability and proves idempotency.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_integration_proxy_blob_publication_holds_guard_through_membership_durability() {
    let fs_root = tmp_dir("integ-proxy-blob-pub");
    let ref_idx_path = tmp_dir("integ-proxy-blob-pub-idx");
    let base_storage = Arc::new(FsStorage::new(fs_root.clone(), 10 * 1024 * 1024));
    let ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());
    ref_index
        .ensure_healthy_or_rebuild(&base_storage, true, true)
        .await
        .unwrap();
    let coordinator = ConsistencyCoordinator::new();

    let (in_crit_tx, in_crit_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let in_crit_tx = Arc::new(tokio::sync::Mutex::new(Some(in_crit_tx)));
    let resume_rx = Arc::new(tokio::sync::Mutex::new(Some(resume_rx)));

    let mut hooks = StorageHooks::default();
    let in_tx = in_crit_tx.clone();
    let res_rx = resume_rx.clone();
    hooks.after_commit_blob = Some(Arc::new(move |_d| {
        let in_tx = in_tx.clone();
        let res_rx = res_rx.clone();
        Box::pin(async move {
            if let Some(tx) = in_tx.lock().await.take() {
                let _ = tx.send(());
            }
            if let Some(rx) = res_rx.lock().await.take() {
                let _ = rx.await;
            }
        })
    }));

    let hooked_storage = Arc::new(HookedStorage::new(base_storage.clone(), hooks));

    let upload_coord = Arc::new(
        registry_rust::upload_coordinator::BlobUploadCoordinator::new(
            hooked_storage.clone(),
            Some(ref_index.clone()),
            coordinator.clone(),
            registry_rust::upload_coordinator::BlobUploadCoordinatorConfig {
                signing_key: b"test-key".to_vec(),
                max_upload_bytes: 10 * 1024 * 1024,
                abort_on_digest_mismatch: true,
                disallow_monolithic_uploads: false,
                upload_chunk_min_bytes: None,
                gc_pin_duration_secs: 3600,
            },
        ),
    );

    let blob_data = bytes::Bytes::from_static(b"proxy-cached blob content data");
    let mut hasher = sha2::Sha256::default();
    sha2::Digest::update(&mut hasher, &blob_data);
    let hex = hex::encode(sha2::Digest::finalize(hasher));
    let digest = Digest::parse(&format!("sha256:{hex}")).unwrap();

    let stream: registry_rust::storage::upload_session::UploadByteStream = Box::pin(
        futures_util::stream::once(futures_util::future::ready(Ok::<
            bytes::Bytes,
            registry_rust::storage::upload_session::UploadStreamError,
        >(blob_data.clone()))),
    );

    let local_repo = CanonicalRepoName::parse("proxyrepo").unwrap();
    let upload_coord_clone = upload_coord.clone();
    let local_repo_clone = local_repo.clone();
    let d_clone = digest.clone();
    let pub_task = tokio::spawn(async move {
        upload_coord_clone
            .publish_proxy_blob(&local_repo_clone, &d_clone, stream)
            .await
    });

    // 1. Wait until proxy blob publication pauses after CAS write and before membership durability
    in_crit_rx
        .await
        .expect("reached proxy membership durability boundary");

    // Verify CAS exists directly in storage
    assert!(base_storage.open_blob(&digest).await.is_ok());
    // Verify membership is in storage
    assert!(
        base_storage
            .get_repo_blob_membership("proxyrepo", &digest)
            .await
            .unwrap()
            .is_some()
    );

    // 2. Concurrently attempt GC revalidation -> MUST remain pending
    let gc_coord = coordinator.clone();
    let (gc_acquired_tx, mut gc_acquired_rx) = tokio::sync::oneshot::channel();
    let gc_task = tokio::spawn(async move {
        let _guard = gc_coord.acquire_gc_revalidation().await;
        let _ = gc_acquired_tx.send(());
    });

    tokio::task::yield_now().await;
    assert!(
        !pub_task.is_finished(),
        "proxy publication task must remain incomplete while barrier is held"
    );
    assert!(
        gc_acquired_rx.try_recv().is_err(),
        "GC revalidation must NOT acquire while proxy blob publication holds mutation guard"
    );

    // 3. Release barrier and allow publication to complete
    let _ = resume_tx.send(());

    let pub_res = pub_task.await.unwrap();
    assert!(
        pub_res.is_ok(),
        "proxy publication must succeed: {:?}",
        pub_res
    );

    // 4. Verify final durable state: CAS exists, proxy membership durable
    assert!(base_storage.open_blob(&digest).await.is_ok());
    assert!(
        base_storage
            .get_repo_blob_membership("proxyrepo", &digest)
            .await
            .unwrap()
            .is_some()
    );

    // 5. GC revalidation acquires only afterward
    gc_acquired_rx
        .await
        .expect("GC revalidation acquires after proxy publish completes");
    gc_task.await.unwrap();

    // 6. Test idempotent duplicate publication
    let stream2: registry_rust::storage::upload_session::UploadByteStream = Box::pin(
        futures_util::stream::once(futures_util::future::ready(Ok::<
            bytes::Bytes,
            registry_rust::storage::upload_session::UploadStreamError,
        >(blob_data.clone()))),
    );
    let pub_res2 = upload_coord
        .publish_proxy_blob(&local_repo, &digest, stream2)
        .await;
    assert!(
        pub_res2.is_ok(),
        "duplicate proxy blob publication must be idempotent and succeed"
    );
}

// 4. Cross-mount cannot race GC deletion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_integration_cross_mount_cannot_race_gc_deletion() {
    let fs_root = tmp_dir("integ-cross-mount-gc");
    let ref_idx_path = tmp_dir("integ-cross-mount-gc-idx");
    let base_storage = Arc::new(FsStorage::new(fs_root.clone(), 10 * 1024 * 1024));
    let ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());
    ref_index
        .ensure_healthy_or_rebuild(&base_storage, true, true)
        .await
        .unwrap();
    let coordinator = ConsistencyCoordinator::new();

    let (in_crit_tx, in_crit_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let in_crit_tx = Arc::new(tokio::sync::Mutex::new(Some(in_crit_tx)));
    let resume_rx = Arc::new(tokio::sync::Mutex::new(Some(resume_rx)));

    let mut hooks = StorageHooks::default();
    let in_tx = in_crit_tx.clone();
    let res_rx = resume_rx.clone();
    // Intercept inside cross_mount_blob after target repository membership is written, while MutationGuard is held
    hooks.after_link_repo_blob = Some(Arc::new(move |_rec| {
        let in_tx = in_tx.clone();
        let res_rx = res_rx.clone();
        Box::pin(async move {
            if let Some(tx) = in_tx.lock().await.take() {
                let _ = tx.send(());
            }
            if let Some(rx) = res_rx.lock().await.take() {
                let _ = rx.await;
            }
        })
    }));

    let hooked_storage = Arc::new(HookedStorage::new(base_storage.clone(), hooks));

    let upload_coord = Arc::new(
        registry_rust::upload_coordinator::BlobUploadCoordinator::new(
            hooked_storage.clone(),
            Some(ref_index.clone()),
            coordinator.clone(),
            registry_rust::upload_coordinator::BlobUploadCoordinatorConfig {
                signing_key: b"test-key".to_vec(),
                max_upload_bytes: 10 * 1024 * 1024,
                abort_on_digest_mismatch: true,
                disallow_monolithic_uploads: false,
                upload_chunk_min_bytes: None,
                gc_pin_duration_secs: 3600,
            },
        ),
    );

    let blob_bytes = bytes::Bytes::from_static(b"source blob content for cross mount");
    let mut hasher = sha2::Sha256::default();
    sha2::Digest::update(&mut hasher, &blob_bytes);
    let hex = hex::encode(sha2::Digest::finalize(hasher));
    let digest = Digest::parse(&format!("sha256:{hex}")).unwrap();

    write_live_blob(&fs_root, &digest, &blob_bytes).await;
    let source_rec = RepoBlobMembershipRecord::new_upload(
        CanonicalRepoName::parse("source-repo").unwrap(),
        digest.clone(),
        None,
    );
    base_storage.link_repo_blob(&source_rec).await.unwrap();

    let upload_coord_clone = upload_coord.clone();
    let d_clone = digest.clone();
    let mount_task = tokio::spawn(async move {
        upload_coord_clone
            .cross_mount_blob("target-repo", Some("source-repo"), &d_clone)
            .await
    });

    // 1. Wait until cross-mount pauses after target membership record is written to storage
    in_crit_rx
        .await
        .expect("reached cross-mount membership boundary");

    // Verify target membership is written in storage while guard is held
    assert!(
        base_storage
            .get_repo_blob_membership("target-repo", &digest)
            .await
            .unwrap()
            .is_some()
    );

    // 2. Concurrently attempt GC revalidation -> MUST remain pending
    let gc_coord = coordinator.clone();
    let (gc_acquired_tx, mut gc_acquired_rx) = tokio::sync::oneshot::channel();
    let gc_task = tokio::spawn(async move {
        let _guard = gc_coord.acquire_gc_revalidation().await;
        let _ = gc_acquired_tx.send(());
    });

    tokio::task::yield_now().await;
    assert!(
        !mount_task.is_finished(),
        "cross-mount task must remain incomplete while barrier is held"
    );
    assert!(
        gc_acquired_rx.try_recv().is_err(),
        "GC revalidation must NOT acquire while cross-mount mutation guard is held"
    );

    // 3. Release barrier
    let _ = resume_tx.send(());

    let mount_res = mount_task.await.unwrap();
    assert!(
        mount_res.is_ok(),
        "cross-mount must succeed: {:?}",
        mount_res
    );

    // 4. Verify target repo membership is durable in storage and index
    assert!(
        base_storage
            .get_repo_blob_membership("target-repo", &digest)
            .await
            .unwrap()
            .is_some()
    );

    // 5. GC revalidation acquires only afterward
    gc_acquired_rx
        .await
        .expect("GC revalidation acquires after cross-mount finishes");
    gc_task.await.unwrap();
}

// 5. Repository unlink and GC revalidation strictly serialize; no race window exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_integration_repository_unlink_and_gc_revalidation_serialize() {
    let fs_root = tmp_dir("integ-unlink-gc-serialize");
    let ref_idx_path = tmp_dir("integ-unlink-gc-serialize-idx");
    let base_storage = Arc::new(FsStorage::new(fs_root.clone(), 10 * 1024 * 1024));
    let ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());
    ref_index
        .ensure_healthy_or_rebuild(&base_storage, true, true)
        .await
        .unwrap();
    let coordinator = ConsistencyCoordinator::new();

    let (in_crit_tx, in_crit_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let in_crit_tx = Arc::new(tokio::sync::Mutex::new(Some(in_crit_tx)));
    let resume_rx = Arc::new(tokio::sync::Mutex::new(Some(resume_rx)));

    let mut hooks = StorageHooks::default();
    let in_tx = in_crit_tx.clone();
    let res_rx = resume_rx.clone();
    // Intercept inside unlink after physical storage unlink has completed, while MutationGuard is held
    hooks.after_unlink_repo_blob = Some(Arc::new(move |_arg| {
        let in_tx = in_tx.clone();
        let res_rx = res_rx.clone();
        Box::pin(async move {
            if let Some(tx) = in_tx.lock().await.take() {
                let _ = tx.send(());
            }
            if let Some(rx) = res_rx.lock().await.take() {
                let _ = rx.await;
            }
        })
    }));

    let hooked_storage = Arc::new(HookedStorage::new(base_storage.clone(), hooks));

    let ledger = Arc::new(
        registry_rust::repository_membership_ledger::RepositoryMembershipLedger::new(
            hooked_storage.clone(),
            Some(ref_index.clone()),
            coordinator.clone(),
        ),
    );

    let blob_bytes = bytes::Bytes::from_static(b"shared blob data between two repos");
    let mut hasher = sha2::Sha256::default();
    sha2::Digest::update(&mut hasher, &blob_bytes);
    let hex = hex::encode(sha2::Digest::finalize(hasher));
    let digest = Digest::parse(&format!("sha256:{hex}")).unwrap();

    write_live_blob(&fs_root, &digest, &blob_bytes).await;
    let rec1 = RepoBlobMembershipRecord::new_upload(
        CanonicalRepoName::parse("unlinkrepo").unwrap(),
        digest.clone(),
        None,
    );
    ledger.link(&rec1).await.expect("link unlinkrepo");
    let rec2 = RepoBlobMembershipRecord::new_upload(
        CanonicalRepoName::parse("keeperrepo").unwrap(),
        digest.clone(),
        None,
    );
    ledger.link(&rec2).await.expect("link keeperrepo");

    let ledger_clone = ledger.clone();
    let d_clone = digest.clone();
    let unlink_task =
        tokio::spawn(async move { ledger_clone.unlink("unlinkrepo", &d_clone).await });

    // 1. Wait until unlink pauses after physical storage unlink has completed
    in_crit_rx
        .await
        .expect("reached unlink critical section boundary");

    // Verify storage unlink already completed
    assert!(
        base_storage
            .get_repo_blob_membership("unlinkrepo", &digest)
            .await
            .unwrap()
            .is_none()
    );

    // 2. Concurrently attempt GC revalidation -> MUST remain pending
    let gc_coord = coordinator.clone();
    let (gc_acquired_tx, mut gc_acquired_rx) = tokio::sync::oneshot::channel();
    let gc_task = tokio::spawn(async move {
        let _guard = gc_coord.acquire_gc_revalidation().await;
        let _ = gc_acquired_tx.send(());
    });

    tokio::task::yield_now().await;
    assert!(
        !unlink_task.is_finished(),
        "unlink task must remain incomplete while barrier is held"
    );
    assert!(
        gc_acquired_rx.try_recv().is_err(),
        "GC revalidation must NOT acquire while unlink mutation guard is held"
    );

    // 3. Release barrier
    let _ = resume_tx.send(());

    let unlink_res = unlink_task.await.unwrap();
    assert!(unlink_res.is_ok(), "unlink must succeed: {:?}", unlink_res);

    // 4. Verify final durable state in storage and ledger/index
    assert!(
        base_storage
            .get_repo_blob_membership("unlinkrepo", &digest)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        ledger
            .get_membership("unlinkrepo", &digest)
            .await
            .unwrap()
            .is_none()
    );

    // 5. GC revalidation acquires only afterward
    gc_acquired_rx
        .await
        .expect("GC revalidation acquires after unlink finishes");
    gc_task.await.unwrap();
}

// 6. Filesystem deletion requires both GC proof types.
#[tokio::test]
async fn test_integration_fs_deletion_requires_both_gc_proof_types() {
    let fs_root = tmp_dir("integ-fs-both-proofs");
    let ref_idx_path = tmp_dir("integ-fs-both-proofs-idx");
    let cfg = test_config(fs_root.clone(), ref_idx_path.clone());
    let storage = Arc::new(FsStorage::new(fs_root.clone(), 10 * 1024 * 1024));
    let ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());
    ref_index
        .ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();
    let coordinator = ConsistencyCoordinator::new();
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-fs-proofs")
        .await
        .unwrap();

    let blob_bytes = bytes::Bytes::from_static(b"unreferenced candidate for fs delete");
    let mut hasher = sha2::Sha256::default();
    sha2::Digest::update(&mut hasher, &blob_bytes);
    let hex = hex::encode(sha2::Digest::finalize(hasher));
    let digest = Digest::parse(&format!("sha256:{hex}")).unwrap();

    write_live_blob(&fs_root, &digest, &blob_bytes).await;

    let permit = authority.gc_mutation_permit();
    let version = storage
        .quarantined_blob_version(&digest)
        .await
        .unwrap()
        .unwrap_or(storage::BlobObjectVersion("v1".to_string()));
    let q_res = storage
        .quarantine_blob(&permit, &digest, &version)
        .await
        .unwrap();
    assert_eq!(
        q_res,
        GcQuarantineResult::Quarantined {
            size: blob_bytes.len() as u64
        }
    );

    let gc_service = GcService::with_coordinator_and_authority(
        Arc::new(cfg),
        storage.clone(),
        ref_index.clone(),
        coordinator,
        Arc::new(Mutex::new(Some(authority))),
    );

    let budgets = GcBudgets {
        max_blobs: 100,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };

    let del_stats = gc_service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .unwrap();

    assert_eq!(del_stats.deleted_blobs, 1);
    assert_eq!(del_stats.deleted_bytes, blob_bytes.len() as u64);
    assert!(storage.open_blob(&digest).await.is_err());
}

// 7. S3 deletion requires both GC proof types.
#[tokio::test]
async fn test_integration_s3_deletion_requires_both_gc_proof_types() {
    let (s3_storage, driver) = support::s3_mock::create_mock_storage();
    let s3_storage_dyn = Arc::new(s3_storage);
    let ref_idx_path = tmp_dir("integ-s3-both-proofs-idx");
    let ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());
    ref_index
        .ensure_healthy_or_rebuild(&s3_storage_dyn, true, true)
        .await
        .unwrap();
    let coordinator = ConsistencyCoordinator::new();
    let authority = RuntimeMutationAuthority::acquire(s3_storage_dyn.clone(), "test-s3-proofs")
        .await
        .unwrap();
    let mut cfg = test_config(PathBuf::from("/tmp"), PathBuf::from("/tmp"));
    cfg.blob_gc_enabled = true;
    cfg.blob_gc_enable_delete = true;

    let digest =
        Digest::parse("sha256:7777777777777777777777777777777777777777777777777777777777777777")
            .unwrap();
    let blob_bytes = bytes::Bytes::from_static(b"s3 candidate payload");
    driver.objects.lock().unwrap().insert(
        format!("blobs/sha256/{}/{}", digest.prefix2(), digest.hex()),
        (blob_bytes.clone(), "\"etag777\"".to_string()),
    );

    let gc_service = GcService::with_coordinator_and_authority(
        Arc::new(cfg),
        s3_storage_dyn.clone(),
        ref_index.clone(),
        coordinator,
        Arc::new(Mutex::new(Some(authority))),
    );

    let budgets = GcBudgets {
        max_blobs: 100,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };

    let del_stats = gc_service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .unwrap();

    assert_eq!(del_stats.deleted_blobs, 1);
    assert_eq!(del_stats.deleted_bytes, blob_bytes.len() as u64);
    assert!(s3_storage_dyn.open_blob(&digest).await.is_err());
}

// 8. Deterministically proves that candidate A releases its GcRevalidationGuard allowing
// an interleaved mutation to acquire MutationGuard before candidate B enters revalidation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_integration_gc_candidate_releases_guard_between_items() {
    let fs_root = tmp_dir("integ-gc-release-between");
    let ref_idx_path = tmp_dir("integ-gc-release-between-idx");
    let cfg = test_config(fs_root.clone(), ref_idx_path.clone());
    let base_storage = Arc::new(FsStorage::new(fs_root.clone(), 10 * 1024 * 1024));
    let ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());
    ref_index
        .ensure_healthy_or_rebuild(&base_storage, true, true)
        .await
        .unwrap();
    let coordinator = ConsistencyCoordinator::new();
    let authority = RuntimeMutationAuthority::acquire(base_storage.clone(), "test-gc-rel")
        .await
        .unwrap();

    // Create two unreferenced blobs
    let data_a = b"candidate-a-payload-data";
    let hex_a = "000000000000000000000000000000000000000000000000000000000000000a";
    let digest_a = Digest::parse(&format!("sha256:{hex_a}")).unwrap();
    write_live_blob(&fs_root, &digest_a, data_a).await;

    let data_b = b"candidate-b-payload-data";
    let hex_b = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    let digest_b = Digest::parse(&format!("sha256:{hex_b}")).unwrap();
    write_live_blob(&fs_root, &digest_b, data_b).await;

    let (cand_1_deleted_tx, cand_1_deleted_rx) = tokio::sync::oneshot::channel();
    let (mutation_queued_tx, mutation_queued_rx) = tokio::sync::oneshot::channel();

    let cand_1_deleted_tx = Arc::new(tokio::sync::Mutex::new(Some(cand_1_deleted_tx)));
    let mutation_queued_rx = Arc::new(tokio::sync::Mutex::new(Some(mutation_queued_rx)));
    let deleted_first = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut hooks = StorageHooks::default();

    // Hook after the first candidate's conditional delete completes
    let a_tx = cand_1_deleted_tx.clone();
    let m_q_rx = mutation_queued_rx.clone();
    let del_first = deleted_first.clone();
    hooks.after_delete_blob_conditional = Some(Arc::new(move |_d| {
        let a_tx = a_tx.clone();
        let m_q_rx = m_q_rx.clone();
        let del_first = del_first.clone();
        Box::pin(async move {
            if !del_first.swap(true, std::sync::atomic::Ordering::SeqCst) {
                if let Some(tx) = a_tx.lock().await.take() {
                    let _ = tx.send(());
                }
                // Wait until the interleaved mutation task is queued on coordinator.acquire_mutation()
                // BEFORE candidate 1 drops its guard.
                if let Some(rx) = m_q_rx.lock().await.take() {
                    let _ = rx.await;
                }
            }
        })
    }));

    let hooked_storage = Arc::new(HookedStorage::new(base_storage.clone(), hooks));

    let gc_service = GcService::with_coordinator_and_authority(
        Arc::new(cfg),
        hooked_storage.clone(),
        ref_index.clone(),
        coordinator.clone(),
        Arc::new(Mutex::new(Some(authority))),
    );

    let budgets = GcBudgets {
        max_blobs: 10,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };

    gc_service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets.clone(),
        )
        .await
        .unwrap();

    let gc_task = tokio::spawn(async move {
        gc_service
            .delete(
                BlobGcPolicy::ManifestRooted,
                Duration::from_secs(0),
                budgets,
            )
            .await
    });

    // 1. Wait until the first candidate has been conditionally deleted
    cand_1_deleted_rx
        .await
        .expect("candidate 1 deletion completed");

    // 2. Notify hook that we are queuing on the coordinator mutex
    let _ = mutation_queued_tx.send(());

    // 3. Interleave a REAL production mutation using the coordinator
    let mut_guard = coordinator.acquire_mutation().await;
    let new_blob_d =
        Digest::parse("sha256:5555555555555555555555555555555555555555555555555555555555555555")
            .unwrap();
    let rec = RepoBlobMembershipRecord::new_upload(
        CanonicalRepoName::parse("interleaved-repo").unwrap(),
        new_blob_d.clone(),
        None,
    );
    base_storage.link_repo_blob(&rec).await.unwrap();
    ref_index
        .record_membership(&rec.digest, rec.repo.as_str())
        .unwrap();
    drop(mut_guard);

    // 4. GC completes candidate 2 deletion
    let del_stats = gc_task.await.unwrap().unwrap();
    assert_eq!(del_stats.deleted_blobs, 2);

    // 5. Verify both candidates were deleted and interleaved mutation remains intact
    assert!(base_storage.open_blob(&digest_a).await.is_err());
    assert!(base_storage.open_blob(&digest_b).await.is_err());
    assert!(
        base_storage
            .get_repo_blob_membership("interleaved-repo", &new_blob_d)
            .await
            .unwrap()
            .is_some()
    );
}

// 9. Nested manifest/membership composition completes without reacquisition or deadlock.
#[tokio::test]
async fn test_integration_nested_manifest_composition_no_reacquisition_deadlock() {
    let fs_root = tmp_dir("integ-nested-manifest");
    let ref_idx_path = tmp_dir("integ-nested-manifest-idx");
    let storage = Arc::new(FsStorage::new(fs_root.clone(), 10 * 1024 * 1024));
    let ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());
    ref_index
        .ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();
    let coordinator = ConsistencyCoordinator::new();

    let service = registry_rust::manifest_lifecycle::ManifestLifecycleService::new(
        storage.clone(),
        Some(ref_index.clone()),
        coordinator.clone(),
    );

    // Link config and layer
    let cfg_d =
        Digest::parse("sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
            .unwrap();
    let layer_d =
        Digest::parse("sha256:cb34a7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f2001")
            .unwrap();

    for d in [&cfg_d, &layer_d] {
        write_live_blob(&fs_root, d, b"{}").await;
        let rec = RepoBlobMembershipRecord::new_upload(
            CanonicalRepoName::parse("nested-app").unwrap(),
            (*d).clone(),
            None,
        );
        storage.link_repo_blob(&rec).await.unwrap();
    }

    let manifest_json = format!(
        r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","size":2,"digest":"{}"}},"layers":[{{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","size":2,"digest":"{}"}}]}}"#,
        cfg_d, layer_d
    );
    let m_bytes = bytes::Bytes::from(manifest_json);

    let req = registry_rust::manifest_lifecycle::PublishManifestRequest::new(
        "nested-app",
        "v1.0",
        m_bytes,
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
    );

    let result = service.publish_manifest(req).await;
    assert!(
        result.is_ok(),
        "nested manifest publication must not deadlock: {:?}",
        result
    );
}

// 10. Cancellation of a real production operation after durable CAS publication releases coordinator
// and allows subsequent GC recovery following established pin expiry semantics.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_integration_cancellation_during_guarded_mutation_releases_coordinator() {
    let fs_root = tmp_dir("integ-cancel-prod-op");
    let ref_idx_path = tmp_dir("integ-cancel-prod-op-idx");
    let cfg = test_config(fs_root.clone(), ref_idx_path.clone());
    let base_storage = Arc::new(FsStorage::new(fs_root.clone(), 10 * 1024 * 1024));
    let ref_index = Arc::new(BlobRefIndex::open(ref_idx_path).unwrap());
    ref_index
        .ensure_healthy_or_rebuild(&base_storage, true, true)
        .await
        .unwrap();
    let coordinator = ConsistencyCoordinator::new();
    let authority = RuntimeMutationAuthority::acquire(base_storage.clone(), "test-cancel-auth")
        .await
        .unwrap();

    let (in_crit_tx, in_crit_rx) = tokio::sync::oneshot::channel();
    let in_crit_tx = Arc::new(tokio::sync::Mutex::new(Some(in_crit_tx)));

    let mut hooks = StorageHooks::default();
    let in_tx = in_crit_tx.clone();
    let root_clone = fs_root.clone();
    // Intercept inside finalize_upload after CAS publication has completed, holding MutationGuard
    hooks.before_commit_blob = Some(Arc::new(move |d| {
        let in_tx = in_tx.clone();
        let root = root_clone.clone();
        Box::pin(async move {
            write_live_blob(&root, &d, b"data that will be cancelled post-CAS").await;
            if let Some(tx) = in_tx.lock().await.take() {
                let _ = tx.send(());
            }
            // Intentionally pend until task abortion
            futures_util::future::pending::<()>().await;
        })
    }));

    let hooked_storage = Arc::new(HookedStorage::new(base_storage.clone(), hooks));

    let upload_coord = Arc::new(
        registry_rust::upload_coordinator::BlobUploadCoordinator::new(
            hooked_storage.clone(),
            Some(ref_index.clone()),
            coordinator.clone(),
            registry_rust::upload_coordinator::BlobUploadCoordinatorConfig {
                signing_key: b"test-key".to_vec(),
                max_upload_bytes: 10 * 1024 * 1024,
                abort_on_digest_mismatch: true,
                disallow_monolithic_uploads: false,
                upload_chunk_min_bytes: None,
                gc_pin_duration_secs: 3600,
            },
        ),
    );

    let start_res = upload_coord.start_upload("canceldemo").await.unwrap();
    let chunk = bytes::Bytes::from_static(b"data that will be cancelled post-CAS");
    let mut hasher = sha2::Sha256::default();
    sha2::Digest::update(&mut hasher, &chunk);
    let hex = hex::encode(sha2::Digest::finalize(hasher));
    let digest = Digest::parse(&format!("sha256:{hex}")).unwrap();

    let stream: registry_rust::storage::upload_session::UploadByteStream =
        Box::pin(futures_util::stream::once(futures_util::future::ready(
            Ok::<bytes::Bytes, registry_rust::storage::upload_session::UploadStreamError>(chunk),
        )));
    let append_res = upload_coord
        .append_upload(
            "canceldemo",
            &start_res.session.uuid,
            &start_res.state_token,
            None,
            None,
            stream,
        )
        .await
        .unwrap();

    let upload_coord_clone = upload_coord.clone();
    let uuid = start_res.session.uuid.clone();
    let token = append_res.state_token.clone();
    let d_clone = digest.clone();
    let fin_handle = tokio::spawn(async move {
        upload_coord_clone
            .finalize_upload("canceldemo", &uuid, Some(&token), None, None, &d_clone)
            .await
    });

    // 1. Wait until operation is inside its critical section after CAS publication
    in_crit_rx.await.expect("reached post-CAS critical section");

    // 2. Directly verify CAS blob physically exists on disk and publication pin is active
    assert!(base_storage.open_blob(&digest).await.is_ok());
    assert!(
        ref_index
            .is_blob_pinned(&digest, SystemTime::now())
            .unwrap()
    );
    assert!(
        base_storage
            .get_repo_blob_membership("canceldemo", &digest)
            .await
            .unwrap()
            .is_none()
    );

    // 3. Cancel / abort the production operation while it holds the guard
    fin_handle.abort();
    let _ = fin_handle.await;

    // 4. Coordinator guard must be released immediately without deadlock
    let gc_guard = coordinator.acquire_gc_revalidation().await;

    // 5. Simulate established restart/recovery path: ensure index is healthy
    ref_index
        .ensure_healthy_or_rebuild(&base_storage, true, false)
        .await
        .unwrap();

    // 6. Verify orphan remains protected while durable pin lease is valid
    let policy_ctx = registry_rust::blob_gc::PolicyContext::build(
        &cfg,
        &base_storage,
        &ref_index,
        BlobGcPolicy::ManifestRooted,
    )
    .await
    .unwrap();

    assert!(policy_ctx.is_pinned(&digest, SystemTime::now()).unwrap());

    drop(gc_guard);

    // 7. Simulate standard pin expiry recovery path (time advances past pin lease)
    let future_time = SystemTime::now() + Duration::from_secs(4000);
    assert!(!policy_ctx.is_pinned(&digest, future_time).unwrap());
    ref_index.purge_expired_pins(future_time).unwrap();

    // 8. Service execution at expiry time: candidate is quarantined and deleted
    let gc_service = GcService::with_coordinator_and_authority(
        Arc::new(cfg),
        base_storage.clone(),
        ref_index.clone(),
        coordinator.clone(),
        Arc::new(Mutex::new(Some(authority))),
    );

    let budgets = GcBudgets {
        max_blobs: 100,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };

    let q_stats = gc_service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets.clone(),
        )
        .await
        .unwrap();
    assert_eq!(q_stats.quarantined_blobs, 1);

    let d_stats = gc_service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .unwrap();
    assert_eq!(d_stats.deleted_blobs, 1);

    // 9. Verify the orphaned CAS blob is deleted and no dangling membership remains
    assert!(base_storage.open_blob(&digest).await.is_err());
    assert!(
        base_storage
            .get_repo_blob_membership("canceldemo", &digest)
            .await
            .unwrap()
            .is_none()
    );
}
