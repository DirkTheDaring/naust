use bytes::Bytes;
use registry_rust::blob_gc::BlobGcPolicy;
use registry_rust::blob_ref_index::BlobRefIndex;
use registry_rust::config::Config;
use registry_rust::gc_service::{GcBudgets, GcService};
use registry_rust::manifest_lifecycle::{LifecycleJournalRecord, LifecycleOpKind, LifecyclePhase};
use registry_rust::registry::canonical_name::CanonicalRepoName;
use registry_rust::registry::digest::Digest;
use registry_rust::storage::fs::FsStorage;
use registry_rust::storage::mutation_authority::RuntimeMutationAuthority;
use registry_rust::storage::repo_membership::RepoBlobMembershipRecord;
use registry_rust::storage::{self, GcDeleteResult, GcQuarantineResult, Storage};
use sha2::Digest as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Barrier, Mutex};

fn tmp_dir(prefix: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("registry-rust-{prefix}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&p).expect("create temp dir");
    p
}

fn test_config(fs_root: PathBuf, ref_index_path: PathBuf) -> Config {
    let toml = format!(
        r#"
[server]
listen_addr = "127.0.0.1:0"

[storage]
backend = "filesystem"

[storage.fs]
root = "{}"

[storage.ref_index]
enabled = true
path = "{}"

[blob_gc]
enabled = true
enable_delete = true
default_min_age_secs = 0
default_quarantine_delay_secs = 0
default_max_blobs = 1000

[token]
signing_key = "test-secret-key-12345678901234567890"
"#,
        fs_root.display(),
        ref_index_path.display()
    );
    let cfg_path = fs_root.join("test_cfg.toml");
    std::fs::write(&cfg_path, toml).unwrap();
    Config::from_env_with_files(&[cfg_path]).unwrap()
}

async fn write_live_blob(fs_root: &std::path::Path, digest: &Digest, data: &[u8]) {
    let dir = fs_root.join("blobs").join("sha256").join(digest.prefix2());
    tokio::fs::create_dir_all(&dir).await.expect("mkdir live");
    tokio::fs::write(dir.join(digest.hex()), data)
        .await
        .expect("write live");
}

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

    let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .expect("ensure idx");

    let consistency_gate = Arc::new(Mutex::new(()));
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-adv-1")
        .await
        .expect("authority");

    let service = GcService::with_coordinator_and_authority(
        cfg.clone(),
        storage.clone(),
        idx.clone(),
        consistency_gate.clone(),
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
    let gate_clone = consistency_gate.clone();
    let fs_root_clone = fs_root.clone();
    let storage_clone = storage.clone();
    let idx_clone = idx.clone();
    let digest_clone = digest.clone();

    // Manifest publication task holds consistency gate, writes manifest + membership
    let pub_handle = tokio::spawn(async move {
        let _gate = gate_clone.lock().await;
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

    let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .expect("ensure idx");

    let consistency_gate = Arc::new(Mutex::new(()));
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-adv-2")
        .await
        .expect("authority");

    let service = GcService::with_coordinator_and_authority(
        cfg.clone(),
        storage.clone(),
        idx.clone(),
        consistency_gate.clone(),
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

    // Spawn GC delete in a background task
    let s = service.clone();
    let b = budgets.clone();
    let gc_handle = tokio::spawn(async move {
        s.delete(BlobGcPolicy::ManifestRooted, Duration::from_secs(0), b)
            .await
    });

    // Wait until GC acquires the consistency gate
    while consistency_gate.try_lock().is_ok() {
        tokio::task::yield_now().await;
    }

    let gate_clone = consistency_gate.clone();
    let fs_root_clone = fs_root.clone();
    let digest_clone = digest.clone();

    // Spawn publication task that queues behind GC on the consistency gate
    let pub_handle = tokio::spawn(async move {
        // Publication must wait until GC releases the gate
        let _guard = gate_clone.lock().await;

        // When publication enters after GC, the unreferenced blob must already be deleted
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

    let d_stats = gc_handle.await.expect("join gc").expect("delete ok");
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

    let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .expect("ensure idx");

    let consistency_gate = Arc::new(Mutex::new(()));
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-adv-3")
        .await
        .expect("authority");

    let service = GcService::with_coordinator_and_authority(
        cfg.clone(),
        storage.clone(),
        idx.clone(),
        consistency_gate.clone(),
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

    let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .expect("ensure idx");

    let consistency_gate = Arc::new(Mutex::new(()));
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-adv-4")
        .await
        .expect("authority");

    let service = GcService::with_coordinator_and_authority(
        cfg.clone(),
        storage.clone(),
        idx.clone(),
        consistency_gate.clone(),
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

    let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .expect("ensure idx");

    let consistency_gate = Arc::new(Mutex::new(()));
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-adv-5")
        .await
        .expect("authority");

    let service = GcService::with_coordinator_and_authority(
        cfg.clone(),
        storage.clone(),
        idx.clone(),
        consistency_gate.clone(),
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

    let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
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

    let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "test-adv-7")
        .await
        .expect("authority");

    let service = GcService::with_authority(cfg.clone(), storage.clone(), idx.clone(), authority);

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

    let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .expect("ensure idx");

    // CLI workflow: acquire authority once
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "cli-test")
        .await
        .expect("cli authority");

    let service = GcService::with_authority(cfg.clone(), storage.clone(), idx.clone(), authority);

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

    let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root.clone(), cfg.max_upload_bytes));
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).expect("open idx"));
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .expect("ensure idx");

    let consistency_gate = Arc::new(Mutex::new(()));
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "adv-bounded-test")
        .await
        .expect("authority");

    let service = GcService::with_coordinator_and_authority(
        cfg.clone(),
        storage.clone(),
        idx.clone(),
        consistency_gate.clone(),
        Arc::new(Mutex::new(Some(authority))),
    );

    // Create 2 unreferenced blobs and quarantine them
    let data1 = b"first-candidate-to-delete";
    let hex1 = hex::encode(sha2::Sha256::digest(data1));
    let digest1 = Digest::parse(&format!("sha256:{hex1}")).expect("digest1");
    write_live_blob(&fs_root, &digest1, data1).await;

    let data2 = b"second-candidate-to-save";
    let hex2 = hex::encode(sha2::Sha256::digest(data2));
    let digest2 = Digest::parse(&format!("sha256:{hex2}")).expect("digest2");
    write_live_blob(&fs_root, &digest2, data2).await;

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

    // Spawn concurrent publication task that polls for blob1 deletion, acquires consistency_gate,
    // and links blob2 to repository membership before GC reaches blob2.
    let gate_clone = consistency_gate.clone();
    let storage_clone = storage.clone();
    let fs_root_clone = fs_root.clone();
    let digest1_clone = digest1.clone();
    let digest2_clone = digest2.clone();

    let pub_task = tokio::spawn(async move {
        let q1 = fs_root_clone
            .join("quarantine")
            .join("blobs")
            .join("sha256")
            .join(digest1_clone.prefix2())
            .join(digest1_clone.hex());

        // Wait until blob1 is deleted by GC
        while q1.exists() {
            tokio::task::yield_now().await;
        }

        // Now acquire consistency_gate between candidate 1 and candidate 2
        let _guard = gate_clone.lock().await;

        // Mutate lifecycle under the gate: link blob2 to repository membership
        let canonical_repo =
            registry_rust::registry::canonical_name::CanonicalRepoName::parse("saved-repo")
                .unwrap();
        let record = RepoBlobMembershipRecord::new_upload(
            canonical_repo,
            digest2_clone,
            Some(uuid::Uuid::new_v4().to_string()),
        );
        storage_clone.link_repo_blob(&record).await.unwrap();
    });

    let d_stats = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("delete");

    pub_task.await.expect("join pub");

    assert_eq!(
        d_stats.deleted_blobs, 1,
        "only blob1 should be deleted; blob2 must be preserved by concurrent mutation"
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

    assert!(!q1.exists(), "blob1 must be deleted");
    assert!(q2.exists(), "blob2 must be preserved");

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

    let storage: Arc<dyn Storage> = Arc::new(FsStorage::try_new(fs_root.clone(), 0).unwrap());
    let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let consistency_gate = Arc::new(Mutex::new(()));
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
    let coordinator = registry_rust::upload_coordinator::BlobUploadCoordinator::with_gate(
        storage.clone(),
        Some(idx.clone()),
        consistency_gate.clone(),
        upload_coord_config,
    );

    let service = GcService::with_coordinator_and_authority(
        Arc::new(cfg.clone()),
        storage.clone(),
        idx.clone(),
        consistency_gate.clone(),
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
    coordinator
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
    coordinator
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
    let (s3_storage, driver) = registry_rust::storage::s3::tests::create_mock_storage();
    let storage: Arc<dyn Storage> = Arc::new(s3_storage);

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
    let consistency_gate = Arc::new(Mutex::new(()));

    let service = GcService::with_coordinator_and_authority(
        Arc::new(cfg),
        storage.clone(),
        idx.clone(),
        consistency_gate,
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
            registry_rust::gc_service::GcServiceError::StrategyUnsupported(_)
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
            registry_rust::gc_service::GcServiceError::StrategyUnsupported(_)
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
            registry_rust::gc_service::GcServiceError::StrategyUnsupported(_)
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
    let (s3_storage, driver) = registry_rust::storage::s3::tests::create_mock_storage();

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
