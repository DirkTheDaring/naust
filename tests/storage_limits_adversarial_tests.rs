use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fs2::FileExt;
use naust::registry::digest::Digest;
use naust::storage::fs::FsStorage;
use naust::storage::{
    GcCursor, GcStorage, Storage, StorageError, StorageErrorKind, UploadSessionStorage,
};

fn create_test_storage(dir: &Path) -> Arc<FsStorage> {
    Arc::new(FsStorage::new(dir.to_path_buf(), 50 * 1024 * 1024))
}

// =================================================================================================
// 1. Adversarial Repo Probe: High Cardinality, Alien Files, and Escape Attempts
// =================================================================================================

#[tokio::test]
async fn test_adversarial_repo_probe_massive_alien_entries_o1() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let storage = create_test_storage(root);

    let repo_dir = root.join("repos").join("massive-alien-repo");
    std::fs::create_dir_all(&repo_dir).unwrap();

    // Generate 3,000 diverse alien entries (files, directories, dotfiles)
    // under repos/massive-alien-repo WITHOUT creating a tags/ directory.
    // Under the old 64-entry enumeration probe, this would fail with LimitExceeded.
    // Under O(1) descriptor open probing, this must succeed instantly.
    for i in 0..2_500 {
        let file_path = repo_dir.join(format!("alien_entry_{i:04}.dat"));
        std::fs::write(file_path, b"random alien payload").unwrap();
    }
    for i in 0..500 {
        let dir_path = repo_dir.join(format!("alien_subdir_{i:04}"));
        std::fs::create_dir_all(dir_path).unwrap();
    }
    // Dotfiles
    std::fs::write(repo_dir.join(".hidden"), b"hidden").unwrap();
    std::fs::write(repo_dir.join(".git"), b"git").unwrap();
    // Subdirectories like referrers and manifests
    std::fs::create_dir_all(repo_dir.join("manifests")).unwrap();
    std::fs::create_dir_all(repo_dir.join("referrers")).unwrap();

    // Probe via list_tags (since tags/ does not exist, triggers repo existence probe)
    let start = Instant::now();
    let tags = storage
        .list_tags("massive-alien-repo")
        .await
        .expect("probe on massive repo must succeed");
    let elapsed = start.elapsed();

    assert!(tags.is_empty(), "repo exists but has no tags");
    assert!(
        elapsed < Duration::from_millis(50),
        "O(1) existence probe must be fast (<50ms), took {elapsed:?}"
    );

    // Missing repo returns NotFound
    let missing_err = storage.list_tags("non-existent-repo").await.unwrap_err();
    assert!(
        matches!(missing_err, StorageError::NotFound),
        "missing repo must return NotFound, got {missing_err:?}"
    );

    // Regular file at repo path returns NotFound (not a directory)
    let file_repo = root.join("repos").join("regular-file-repo");
    std::fs::write(&file_repo, b"just a regular file").unwrap();
    let file_err = storage.list_tags("regular-file-repo").await.unwrap_err();
    assert!(
        matches!(file_err, StorageError::NotFound),
        "regular file repo must return NotFound, got {file_err:?}"
    );

    // Symlink escaping storage root
    let outside_dir = tempfile::tempdir().unwrap();
    let symlink_repo = root.join("repos").join("escaped-repo");
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(outside_dir.path(), &symlink_repo).unwrap();
        let sym_res = storage.list_tags("escaped-repo").await;
        assert!(
            sym_res.is_err(),
            "symlink pointing outside root must be rejected under openat2 containment"
        );
    }
}

// =================================================================================================
// 2. Adversarial CAS Shard Pagination: Massive Entry Set, Uneven Pages, Monotonicity
// =================================================================================================

#[tokio::test]
async fn test_adversarial_cas_shard_massive_pagination_and_monotonicity() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let storage = create_test_storage(root);

    let shard_prefix = "e8";
    let shard_dir = root.join("blobs").join("sha256").join(shard_prefix);
    std::fs::create_dir_all(&shard_dir).unwrap();

    let total_blobs = 1_234;
    let mut expected_digests = Vec::with_capacity(total_blobs);

    for i in 0..total_blobs {
        let hex = format!("{shard_prefix}{:062x}", i);
        std::fs::write(shard_dir.join(&hex), format!("blob content {i}")).unwrap();
        expected_digests.push(format!("sha256:{hex}"));
    }
    expected_digests.sort();

    // Paginate through with a prime limit (limit = 11) to stress uneven page boundaries
    let page_limit = 11;
    let mut cursor: Option<GcCursor> = None;
    let mut observed_digests = Vec::with_capacity(total_blobs);
    let mut page_count = 0;

    loop {
        let page = storage
            .list_cas_blobs_page(cursor.as_ref(), page_limit)
            .await
            .expect("CAS listing page must succeed");

        if page.items.is_empty() {
            assert!(page.next_cursor.is_none(), "empty page must have no cursor");
            break;
        }

        page_count += 1;

        for item in page.items {
            observed_digests.push(item.digest.as_str().to_string());
        }

        if let Some(next) = page.next_cursor {
            cursor = Some(next);
        } else {
            break;
        }
    }

    // Verify complete coverage and strict monotonicity
    assert_eq!(observed_digests.len(), total_blobs);
    assert_eq!(observed_digests, expected_digests);

    // Verify strictly monotonic
    for window in observed_digests.windows(2) {
        assert!(
            window[0] < window[1],
            "digests must be strictly monotonically increasing: {} < {}",
            window[0],
            window[1]
        );
    }

    // Verify page count: 1234 / 11 = 112 full pages + 1 partial page (2 items) = 113 pages
    assert_eq!(page_count, total_blobs.div_ceil(page_limit));

    // Test midpoint resumption: start at item #600
    let midpoint_cursor = GcCursor(expected_digests[599].clone());
    let mid_page = storage
        .list_cas_blobs_page(Some(&midpoint_cursor), 10)
        .await
        .expect("midpoint page must succeed");
    assert_eq!(mid_page.items.len(), 10);
    assert_eq!(mid_page.items[0].digest.as_str(), expected_digests[600]);

    // Test cursor beyond last item
    let beyond_cursor = GcCursor(
        "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string(),
    );
    let empty_page = storage
        .list_cas_blobs_page(Some(&beyond_cursor), 10)
        .await
        .expect("beyond-end page must succeed");
    assert!(empty_page.items.is_empty());
    assert!(empty_page.next_cursor.is_none());
}

// =================================================================================================
// 3. Adversarial CAS Poisoned Shard: Fail-Closed Integrity Guarantees
// =================================================================================================

#[tokio::test]
async fn test_adversarial_cas_shard_poisoned_entries_fail_closed() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let storage = create_test_storage(root);

    let shard_prefix = "d4";
    let shard_dir = root.join("blobs").join("sha256").join(shard_prefix);
    std::fs::create_dir_all(&shard_dir).unwrap();

    // 1. Create 30 valid blobs (d4...00 to d4...29)
    for i in 0..30 {
        let hex = format!("{shard_prefix}{:062x}", i);
        std::fs::write(shard_dir.join(hex), b"valid blob").unwrap();
    }

    // 2. Poison scenario A: Add a malformed non-hex filename AFTER the top 5
    // E.g. "d4...99_non_hex!" will be sorted after the first 5 elements.
    // An unsafe top-K that doesn't validate all shard entries might return the top 5
    // without discovering the corrupt entry. Our architecture must validate ALL entries
    // and fail closed immediately with CorruptData!
    let poison_filename =
        format!("{shard_prefix}fffffffffffffffffffffffffffffffffffffffffffffffffffffffffff_bad!");
    std::fs::write(shard_dir.join(&poison_filename), b"poison").unwrap();

    let res = storage.list_cas_blobs_page(None, 5).await;
    assert!(
        res.is_err(),
        "poisoned entry in shard must fail closed with CorruptData, got {res:?}"
    );
    let err = res.unwrap_err();
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::CorruptData));

    // Remove poison A
    std::fs::remove_file(shard_dir.join(&poison_filename)).unwrap();

    // 3. Poison scenario B: Subdirectory inside the CAS shard
    let bad_subdir = shard_dir.join("nested_folder");
    std::fs::create_dir_all(&bad_subdir).unwrap();

    let res_subdir = storage.list_cas_blobs_page(None, 5).await;
    assert!(
        res_subdir.is_err(),
        "subdirectory inside shard must fail closed with CorruptData"
    );
    assert_eq!(
        res_subdir.unwrap_err().internal_kind(),
        Some(StorageErrorKind::CorruptData)
    );

    // Remove bad subdir
    std::fs::remove_dir(bad_subdir).unwrap();

    // 4. Poison scenario C: Wrong length filename (63 characters)
    let bad_len_filename = format!("{shard_prefix}{:061x}", 1);
    assert_eq!(bad_len_filename.len(), 63);
    std::fs::write(shard_dir.join(&bad_len_filename), b"short").unwrap();

    let res_len = storage.list_cas_blobs_page(None, 5).await;
    assert!(
        res_len.is_err(),
        "63-char filename inside shard must fail closed with CorruptData"
    );
    assert_eq!(
        res_len.unwrap_err().internal_kind(),
        Some(StorageErrorKind::CorruptData)
    );
}

// =================================================================================================
// 4. Adversarial Upload Session Reaper: High Concurrency, Locks, and Malformed Files
// =================================================================================================

#[tokio::test]
async fn test_adversarial_upload_reaper_streaming_and_corrupt_files() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let storage = create_test_storage(root);

    let uploads_dir = root.join("uploads");
    std::fs::create_dir_all(&uploads_dir).unwrap();

    // 1. Create 200 real sessions via storage API to stress the streaming reaper
    let mut session_uuids = Vec::with_capacity(200);
    for _ in 0..200 {
        let session = storage.create_session("adversarial-repo").await.unwrap();
        session_uuids.push(session.uuid);
    }

    // 2. Lock 1 session (simulate active worker holding flock)
    let locked_uuid = session_uuids[0].clone();
    let lock_file_path = uploads_dir.join(format!(".lock.{locked_uuid}"));
    let lock_file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_file_path)
        .unwrap();
    lock_file.lock_exclusive().expect("acquire exclusive flock");

    // 3. Infiltrate corrupt/garbage files into uploads directory
    std::fs::write(uploads_dir.join(".meta.json"), b"missing uuid").unwrap();
    std::fs::write(uploads_dir.join("not-uuid.meta.json.tmp"), b"tmp file").unwrap();
    std::fs::write(uploads_dir.join("random_garbage.dat"), b"garbage").unwrap();
    std::fs::write(uploads_dir.join("exp-0001.hash.notanumber"), b"123").unwrap();
    std::fs::write(
        uploads_dir.join("exp-0001.hash.99999999999999999999999999999999999999999"),
        b"overflow",
    )
    .unwrap();
    std::fs::write(
        uploads_dir.join("corrupt-session.meta.json"),
        b"invalid json content",
    )
    .unwrap();

    // 4. Run the stream-based reaper with max_age_secs = 0, receipt_ttl_secs = 0
    // Under our streaming architecture, this must stream through all 200+ entries without memory blowout,
    // skip the locked session, skip/ignore corrupt/alien entries, and reap the 199 unlocked sessions.
    let reaped_count = storage
        .reap_expired_sessions(0, 0)
        .await
        .expect("reaper must succeed despite corrupt entries");
    assert_eq!(
        reaped_count, 199,
        "reaper must reap exactly the 199 unlocked sessions"
    );

    // Locked session meta must survive!
    let locked_meta_path = uploads_dir.join(format!("{locked_uuid}.meta.json"));
    assert!(
        locked_meta_path.exists(),
        "active locked session must NOT be deleted"
    );

    // Release the lock
    lock_file.unlock().unwrap();
    drop(lock_file);

    // After unlocking, the next reaper pass cleanly reaps the final session
    let reaped_second_pass = storage
        .reap_expired_sessions(0, 0)
        .await
        .expect("second pass reaper must succeed");
    assert_eq!(
        reaped_second_pass, 1,
        "reaper must reap the previously locked session now that it is unlocked"
    );
    assert!(
        !locked_meta_path.exists(),
        "unlocked session must now be reaped"
    );
}

// =================================================================================================
// 5. Adversarial GC Discovery: Deep Hierarchies and Wide Sibling Trees
// =================================================================================================

#[tokio::test]
async fn test_adversarial_gc_discovery_deep_hierarchy_and_wide_tree() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let storage = create_test_storage(root);

    // 1. Deep Hierarchy Test (Depth = 45 levels)
    // Under the old max_depth = 32 limit, this would abort discovery with LimitExceeded.
    let mut deep_path = root.join("repos");
    for i in 1..=45 {
        deep_path = deep_path.join(format!("level_{i}"));
    }
    let manifests_dir = deep_path.join("manifests");
    std::fs::create_dir_all(&manifests_dir).unwrap();

    let manifest_hex = "1111111111111111111111111111111111111111111111111111111111111111";
    let manifest_content = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": 10,
            "digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222"
        },
        "layers": []
    });
    std::fs::write(
        manifests_dir.join(manifest_hex),
        manifest_content.to_string(),
    )
    .unwrap();

    // 2. Wide Sibling Tree Test (1,200 sibling repos under repos/wide_org/)
    // Under the old intermediate_dir_max_entries = 1,000 limit, this would fail.
    let wide_parent = root.join("repos").join("wide_org");
    for i in 0..1_200 {
        let repo = wide_parent.join(format!("repo_{i:04}")).join("manifests");
        std::fs::create_dir_all(repo).unwrap();
    }

    // Run discover_manifest_references through GcStorage
    let refs = storage
        .discover_manifest_references()
        .await
        .expect("unbounded discovery must succeed on depth=45 and 1,200 sibling repos")
        .expect("must return Some(references)");

    // Verify deep manifest was discovered
    let deep_manifest_digest = Digest::parse(&format!("sha256:{manifest_hex}")).unwrap();
    let config_digest =
        Digest::parse("sha256:2222222222222222222222222222222222222222222222222222222222222222")
            .unwrap();

    assert!(
        refs.contains(&deep_manifest_digest),
        "manifest at depth=45 must be discovered under unbounded limits"
    );
    assert!(
        refs.contains(&config_digest),
        "referenced config blob must be tracked in protected set"
    );
}
