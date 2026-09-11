use super::*;
use crate::storage::StorageErrorKind;
use std::sync::Arc;
use tokio::io::AsyncReadExt;

async fn expect_open_blob_err(
    storage: &FsStorage,
    digest: &Digest,
    panic_msg: &str,
) -> StorageError {
    match storage.open_blob(digest).await {
        Ok(_) => panic!("{panic_msg}"),
        Err(err) => err,
    }
}

fn tmp_fs_root() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "registry-rust-fsstorage-test-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&p).expect("create temp fs_root");
    p
}

fn write_file(path: &Path, bytes: &[u8]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dirs");
    }
    std::fs::write(path, bytes).expect("write file");
}

fn hex_sha256(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[tokio::test]
async fn referrers_add_list_remove_and_delete_manifest() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let subject =
        Digest::parse("sha256:1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let ref1 =
        Digest::parse("sha256:2222222222222222222222222222222222222222222222222222222222222222")
            .unwrap();
    let ref2 =
        Digest::parse("sha256:3333333333333333333333333333333333333333333333333333333333333333")
            .unwrap();

    let desc1 = ReferrerDescriptor {
        media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
        digest: ref1.as_str().to_string(),
        size: 100,
        artifact_type: Some("application/vnd.example.sbom.v1".to_string()),
        annotations: None,
    };

    let desc2 = ReferrerDescriptor {
        media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
        digest: ref2.as_str().to_string(),
        size: 200,
        artifact_type: Some("application/vnd.example.sig.v1".to_string()),
        annotations: None,
    };

    // Add both referrers
    storage
        .add_referrer("testrepo", &subject, desc1)
        .await
        .expect("add ref1");
    storage
        .add_referrer("testrepo", &subject, desc2)
        .await
        .expect("add ref2");

    let list = storage
        .list_referrers("testrepo", &subject)
        .await
        .expect("list referrers");
    assert_eq!(list.len(), 2);

    // Remove ref1 directly
    storage
        .remove_referrer("testrepo", &subject, &ref1)
        .await
        .expect("remove ref1");
    let list = storage
        .list_referrers("testrepo", &subject)
        .await
        .expect("list referrers");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].digest, ref2.as_str());

    // Put a manifest for ref2 that declares subject
    let manifest_ref2 = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": subject.as_str(),
            "size": 500
        }
    });
    let bytes = serde_json::to_vec(&manifest_ref2).unwrap();
    storage
        .put_manifest("testrepo", &ref2, bytes.into())
        .await
        .expect("put manifest");

    // Delete ref2 manifest -> should remove from referrers
    storage
        .delete_manifest("testrepo", &ref2)
        .await
        .expect("delete manifest");
    let list = storage
        .list_referrers("testrepo", &subject)
        .await
        .expect("list referrers");
    assert_eq!(list.len(), 0);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn head_and_open_blob_fall_back_to_quarantine() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
            .expect("valid digest");

    let content = b"hello from quarantine";
    let qpath = storage.quarantine_blob_path(&digest);
    write_file(&qpath, content);

    let meta = storage.head_blob(&digest).await.expect("head_blob");
    assert_eq!(meta.size, content.len() as u64);

    let (meta, mut reader) = storage.open_blob(&digest).await.expect("open_blob");
    assert_eq!(meta.size, content.len() as u64);

    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.expect("read blob");
    assert_eq!(buf, content);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn open_blob_prefers_live_over_quarantine() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
            .expect("valid digest");

    let live_content = b"live";
    let quarantine_content = b"quarantine";

    write_file(&storage.blob_path(&digest), live_content);
    write_file(&storage.quarantine_blob_path(&digest), quarantine_content);

    let (meta, mut reader) = storage.open_blob(&digest).await.expect("open_blob");
    assert_eq!(meta.size, live_content.len() as u64);

    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.expect("read blob");
    assert_eq!(buf, live_content);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn delete_manifest_fails_safe_on_malformed_manifest() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);
    let repo = "library/delete-malformed";
    let digest =
        Digest::parse("sha256:1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();

    // Write a malformed manifest on disk
    let malformed_manifest = serde_json::json!({
        "schemaVersion": 2,
        "subject": { "digest": "sha256:invalid-subject-hex" }
    });
    storage
        .put_manifest(
            repo,
            &digest,
            serde_json::to_vec(&malformed_manifest).unwrap().into(),
        )
        .await
        .unwrap();
    storage.set_tag(repo, "tag1", &digest).await.unwrap();

    // Attempt deletion
    let err = storage
        .delete_manifest(repo, &digest)
        .await
        .expect_err("should abort on malformed manifest");
    match err {
        StorageError::Internal { kind, message } => {
            assert_eq!(kind, crate::storage::StorageErrorKind::CorruptData);
            assert!(message.contains("malformed"));
        }
        other => panic!("expected StorageError::Internal, got {other:?}"),
    }

    // Verify manifest and tag are still present (not mutated)
    assert!(storage.get_manifest(repo, &digest).await.is_ok());
    assert_eq!(storage.resolve_tag(repo, "tag1").await.unwrap(), digest);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn test_fs_direct_concurrent_create_only() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(FsStorage::new(
        temp_dir.path().to_path_buf(),
        10 * 1024 * 1024,
    ));

    let d1 =
        Digest::parse("sha256:1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let d2 =
        Digest::parse("sha256:2222222222222222222222222222222222222222222222222222222222222222")
            .unwrap();

    let s1 = storage.clone();
    let d1_clone = d1.clone();
    let h1 = tokio::spawn(async move {
        s1.mutate_tag(
            "repo",
            "tag",
            &d1_clone,
            crate::storage::TagMutationPolicy::CreateOnly,
        )
        .await
    });

    let s2 = storage.clone();
    let d2_clone = d2.clone();
    let h2 = tokio::spawn(async move {
        s2.mutate_tag(
            "repo",
            "tag",
            &d2_clone,
            crate::storage::TagMutationPolicy::CreateOnly,
        )
        .await
    });

    let (r1, r2) = tokio::join!(h1, h2);
    let res1 = r1.unwrap();
    let res2 = r2.unwrap();

    let success_count = (res1.is_ok() as usize) + (res2.is_ok() as usize);
    assert_eq!(success_count, 1, "Exactly one CreateOnly must succeed");

    let conflict_count = (matches!(res1, Err(StorageError::TagAlreadyExists)) as usize)
        + (matches!(res2, Err(StorageError::TagAlreadyExists)) as usize);
    assert_eq!(conflict_count, 1, "The loser must get TagAlreadyExists");

    // The winning tag on disk must match the winning mutation result
    let final_d = storage.resolve_tag("repo", "tag").await.unwrap();
    if let Ok(mut_res) = res1 {
        assert_eq!(final_d, d1);
        assert_eq!(mut_res, crate::storage::TagMutation::Created);
    } else {
        assert_eq!(final_d, d2);
        assert_eq!(res2.unwrap(), crate::storage::TagMutation::Created);
    }
}

#[tokio::test]
async fn test_fs_direct_concurrent_replacements_chain() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(FsStorage::new(
        temp_dir.path().to_path_buf(),
        10 * 1024 * 1024,
    ));

    let d1 =
        Digest::parse("sha256:1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let d2 =
        Digest::parse("sha256:2222222222222222222222222222222222222222222222222222222222222222")
            .unwrap();

    let s1 = storage.clone();
    let d1_clone = d1.clone();
    let h1 = tokio::spawn(async move {
        s1.mutate_tag(
            "repo",
            "tag",
            &d1_clone,
            crate::storage::TagMutationPolicy::Replace,
        )
        .await
    });

    let s2 = storage.clone();
    let d2_clone = d2.clone();
    let h2 = tokio::spawn(async move {
        s2.mutate_tag(
            "repo",
            "tag",
            &d2_clone,
            crate::storage::TagMutationPolicy::Replace,
        )
        .await
    });

    let (r1, r2) = tokio::join!(h1, h2);
    let res1 = r1.unwrap().unwrap();
    let res2 = r2.unwrap().unwrap();

    // One of them was Created (first), and the other was Overwritten with the first's digest!
    let final_d = storage.resolve_tag("repo", "tag").await.unwrap();

    if final_d == d2 {
        assert_eq!(res1, crate::storage::TagMutation::Created);
        assert_eq!(res2, crate::storage::TagMutation::Replaced { previous: d1 });
    } else {
        assert_eq!(final_d, d1);
        assert_eq!(res2, crate::storage::TagMutation::Created);
        assert_eq!(res1, crate::storage::TagMutation::Replaced { previous: d2 });
    }
}

#[tokio::test]
async fn test_fs_direct_repeated_replacements() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(FsStorage::new(
        temp_dir.path().to_path_buf(),
        10 * 1024 * 1024,
    ));

    let d1 =
        Digest::parse("sha256:1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let d2 =
        Digest::parse("sha256:2222222222222222222222222222222222222222222222222222222222222222")
            .unwrap();

    let m1 = storage
        .mutate_tag(
            "repo",
            "v1",
            &d1,
            crate::storage::TagMutationPolicy::Replace,
        )
        .await
        .unwrap();
    assert_eq!(m1, crate::storage::TagMutation::Created);

    // Same digest -> Unchanged
    let m1_same = storage
        .mutate_tag(
            "repo",
            "v1",
            &d1,
            crate::storage::TagMutationPolicy::Replace,
        )
        .await
        .unwrap();
    assert_eq!(m1_same, crate::storage::TagMutation::Unchanged);

    // Overwrite -> Replaced { previous: d1 }
    let m2 = storage
        .mutate_tag(
            "repo",
            "v1",
            &d2,
            crate::storage::TagMutationPolicy::Replace,
        )
        .await
        .unwrap();
    assert_eq!(m2, crate::storage::TagMutation::Replaced { previous: d1 });
}

fn make_test_stream(chunks: Vec<Bytes>) -> UploadByteStream {
    let items: Vec<Result<Bytes, UploadStreamError>> = chunks.into_iter().map(Ok).collect();
    Box::pin(futures_util::stream::iter(items))
}

fn make_failing_stream(first_chunk: Bytes, err: UploadStreamError) -> UploadByteStream {
    let items = vec![Ok(first_chunk), Err(err)];
    Box::pin(futures_util::stream::iter(items))
}

#[tokio::test]
async fn test_fs_session_same_offset_concurrent_append() {
    let root = tmp_fs_root();
    let s1 = Arc::new(FsStorage::new(root.clone(), 10 * 1024 * 1024));
    let s2 = Arc::new(FsStorage::new(root.clone(), 10 * 1024 * 1024));

    let session = s1.create_session("myrepo").await.unwrap();

    let s1_clone = s1.clone();
    let session1 = session.clone();
    let handle1 = tokio::spawn(async move {
        let stream = make_test_stream(vec![Bytes::from_static(b"CHUNK_AAAAA")]);
        s1_clone
            .append_if_offset(
                &session1,
                UploadOffsetPrecondition::Exact(0),
                stream,
                10 * 1024 * 1024,
            )
            .await
    });

    let s2_clone = s2.clone();
    let session2 = session.clone();
    let handle2 = tokio::spawn(async move {
        let stream = make_test_stream(vec![Bytes::from_static(b"CHUNK_BBBBB")]);
        s2_clone
            .append_if_offset(
                &session2,
                UploadOffsetPrecondition::Exact(0),
                stream,
                10 * 1024 * 1024,
            )
            .await
    });

    let res1 = handle1.await.unwrap().unwrap();
    let res2 = handle2.await.unwrap().unwrap();

    let mut committed_count = 0;
    let mut mismatch_or_conflict_count = 0;

    for r in [res1, res2] {
        match r {
            UploadAppendResult::Committed { new_offset } => {
                assert_eq!(new_offset, 11);
                committed_count += 1;
            }
            UploadAppendResult::OffsetMismatch { current_offset } => {
                assert_eq!(current_offset, 11);
                mismatch_or_conflict_count += 1;
            }
            UploadAppendResult::Conflict => {
                mismatch_or_conflict_count += 1;
            }
        }
    }

    assert_eq!(
        committed_count, 1,
        "Exactly one concurrent append from offset 0 must commit"
    );
    assert_eq!(
        mismatch_or_conflict_count, 1,
        "Loser must receive offset mismatch or conflict"
    );

    let status = s1.session_status(&session).await.unwrap();
    assert_eq!(status.committed_offset, 11);
}

#[tokio::test]
async fn test_fs_session_stream_failure_rollback() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 10 * 1024 * 1024);

    let session = storage.create_session("myrepo").await.unwrap();

    // 1. Successful initial append
    let stream = make_test_stream(vec![Bytes::from_static(b"INITIAL_BYTES_100_")]);
    let res = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            stream,
            10 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(res, UploadAppendResult::Committed { new_offset: 18 });

    // 2. Failing append mid-chunk
    let failing_stream = make_failing_stream(
        Bytes::from_static(b"PARTIAL_FAIL_CHUNK"),
        UploadStreamError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "client disconnected",
        )),
    );
    let err = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(18),
            failing_stream,
            10 * 1024 * 1024,
        )
        .await
        .unwrap_err();

    assert!(matches!(err, UploadTransitionError::Stream(_)));

    // 3. Verify physical file is truncated back to exact committed offset 18
    let data_path = storage.session_data_path(&session.uuid);
    let file_meta = tokio::fs::metadata(&data_path).await.unwrap();
    assert_eq!(file_meta.len(), 18);

    let status = storage.session_status(&session).await.unwrap();
    assert_eq!(status.committed_offset, 18);
}

#[tokio::test]
async fn test_fs_session_size_overflow_rollback() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 500);

    let session = storage.create_session("myrepo").await.unwrap();

    // 1. Append 400 bytes -> Ok
    let chunk1 = vec![b'A'; 400];
    let stream1 = make_test_stream(vec![Bytes::from(chunk1)]);
    let res1 = storage
        .append_if_offset(&session, UploadOffsetPrecondition::Exact(0), stream1, 500)
        .await
        .unwrap();
    assert_eq!(res1, UploadAppendResult::Committed { new_offset: 400 });

    // 2. Append 200 bytes -> exceeds limit 500
    let chunk2 = vec![b'B'; 200];
    let stream2 = make_test_stream(vec![Bytes::from(chunk2)]);
    let err = storage
        .append_if_offset(&session, UploadOffsetPrecondition::Exact(400), stream2, 500)
        .await
        .unwrap_err();

    assert!(matches!(err, UploadTransitionError::TooLarge));

    // 3. Staging file is truncated back to 400
    let data_path = storage.session_data_path(&session.uuid);
    assert_eq!(tokio::fs::metadata(&data_path).await.unwrap().len(), 400);

    let status = storage.session_status(&session).await.unwrap();
    assert_eq!(status.committed_offset, 400);
}

#[tokio::test]
async fn test_fs_session_crash_recovery_extra_staging_bytes() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let session = storage.create_session("myrepo").await.unwrap();

    let stream = make_test_stream(vec![Bytes::from_static(b"COMMITTED_DATA_300")]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            stream,
            1024 * 1024,
        )
        .await
        .unwrap();

    // Simulate crash: simulate orphan uncommitted bytes written to .data
    let data_path = storage.session_data_path(&session.uuid);
    let mut f = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&data_path)
        .await
        .unwrap();
    f.write_all(b"UNCOMMITTED_CRASH_BYTES").await.unwrap();
    f.sync_all().await.unwrap();
    drop(f);

    assert_eq!(
        tokio::fs::metadata(&data_path).await.unwrap().len(),
        18 + 23
    );

    // Next operation recovers physical file to 18
    let stream2 = make_test_stream(vec![Bytes::from_static(b"_NEXT_CHUNK")]);
    let res = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(18),
            stream2,
            1024 * 1024,
        )
        .await
        .unwrap();

    assert_eq!(
        res,
        UploadAppendResult::Committed {
            new_offset: 18 + 11
        }
    );
    let content = tokio::fs::read(&data_path).await.unwrap();
    assert_eq!(content, b"COMMITTED_DATA_300_NEXT_CHUNK");
}

#[tokio::test]
async fn test_fs_session_crash_orphan_hash_generation() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let session = storage.create_session("myrepo").await.unwrap();
    let stream = make_test_stream(vec![Bytes::from_static(b"HELLO_GEN_0")]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            stream,
            1024 * 1024,
        )
        .await
        .unwrap();

    // Create orphan hash gen 2 file on disk
    let orphan_hash = storage.session_hash_path(&session.uuid, 2);
    tokio::fs::write(&orphan_hash, b"CORRUPTED_ORPHAN")
        .await
        .unwrap();

    // Next append moves from gen 1 to gen 2 atomically overwriting orphan
    let stream2 = make_test_stream(vec![Bytes::from_static(b"_WORLD")]);
    let res = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(11),
            stream2,
            1024 * 1024,
        )
        .await
        .unwrap();

    assert_eq!(res, UploadAppendResult::Committed { new_offset: 17 });
}

#[tokio::test]
async fn test_fs_session_missing_referenced_hash_generation() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let session = storage.create_session("myrepo").await.unwrap();
    let stream = make_test_stream(vec![Bytes::from_static(b"HELLO_WORLD_TEST")]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            stream,
            1024 * 1024,
        )
        .await
        .unwrap();

    // Delete gen 1 hash file
    let hash_gen_1 = storage.session_hash_path(&session.uuid, 1);
    tokio::fs::remove_file(&hash_gen_1).await.unwrap();

    // Next append automatically recomputes hash from .data and commits gen 2
    let stream2 = make_test_stream(vec![Bytes::from_static(b"_AGAIN")]);
    let res = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(16),
            stream2,
            1024 * 1024,
        )
        .await
        .unwrap();

    assert_eq!(res, UploadAppendResult::Committed { new_offset: 22 });
}

#[tokio::test]
async fn test_fs_session_patch_finalize_race() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let session = storage.create_session("myrepo").await.unwrap();
    let data = b"BLOB_CONTENT_FOR_FINALIZATION";
    let stream = make_test_stream(vec![Bytes::from_static(data)]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            stream,
            1024 * 1024,
        )
        .await
        .unwrap();

    let mut hasher = sha2::Sha256::new();
    hasher.update(data);
    let digest_hex = hex::encode(hasher.finalize());
    let digest = Digest::parse(&format!("sha256:{digest_hex}")).unwrap();

    // Begin finalize transitions to Finalizing
    let prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(data.len() as u64),
            None,
            &digest,
            1024 * 1024,
            true,
        )
        .await
        .unwrap();

    // Concurrent append is rejected with Conflict
    let stream2 = make_test_stream(vec![Bytes::from_static(b"LATE_CHUNK")]);
    let append_res = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(data.len() as u64),
            stream2,
            1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(append_res, UploadAppendResult::Conflict);

    // Commit finalize succeeds
    let outcome = storage.commit_finalize(&prepared).await.unwrap();
    assert_eq!(
        outcome,
        FinalizeOutcome::Published(BlobMeta {
            size: data.len() as u64
        })
    );
}

#[tokio::test]
async fn test_fs_session_abort_append_race() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let session = storage.create_session("myrepo").await.unwrap();
    storage.abort_session(&session).await.unwrap();

    let stream = make_test_stream(vec![Bytes::from_static(b"LATE_CHUNK")]);
    let err = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            stream,
            1024 * 1024,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, UploadTransitionError::NotFound));
}

#[tokio::test]
async fn test_fs_session_two_begin_finalize_attempts() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let session = storage.create_session("myrepo").await.unwrap();
    let data = b"LAYER_BYTES";
    let stream = make_test_stream(vec![Bytes::from_static(data)]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            stream,
            1024 * 1024,
        )
        .await
        .unwrap();

    let mut hasher = sha2::Sha256::new();
    hasher.update(data);
    let digest_hex = hex::encode(hasher.finalize());
    let digest = Digest::parse(&format!("sha256:{digest_hex}")).unwrap();

    let p1 = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(data.len() as u64),
            None,
            &digest,
            1024 * 1024,
            true,
        )
        .await
        .unwrap();

    let p2_err = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(data.len() as u64),
            None,
            &digest,
            1024 * 1024,
            true,
        )
        .await
        .unwrap_err();

    assert!(matches!(p2_err, UploadTransitionError::Conflict));

    let outcome = storage.commit_finalize(&p1).await.unwrap();
    assert_eq!(
        outcome,
        FinalizeOutcome::Published(BlobMeta {
            size: data.len() as u64
        })
    );
}

#[tokio::test]
async fn test_fs_session_duplicate_commit_finalize() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let session = storage.create_session("myrepo").await.unwrap();
    let data = b"BLOB_FOR_DUPLICATE_COMMIT";
    let stream = make_test_stream(vec![Bytes::from_static(data)]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            stream,
            1024 * 1024,
        )
        .await
        .unwrap();

    let mut hasher = sha2::Sha256::new();
    hasher.update(data);
    let digest_hex = hex::encode(hasher.finalize());
    let digest = Digest::parse(&format!("sha256:{digest_hex}")).unwrap();

    let prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(data.len() as u64),
            None,
            &digest,
            1024 * 1024,
            true,
        )
        .await
        .unwrap();

    let outcome1 = storage.commit_finalize(&prepared).await.unwrap();
    assert_eq!(
        outcome1,
        FinalizeOutcome::Published(BlobMeta {
            size: data.len() as u64
        })
    );

    let outcome2 = storage.commit_finalize(&prepared).await.unwrap();
    assert_eq!(
        outcome2,
        FinalizeOutcome::AlreadyFinalized(BlobMeta {
            size: data.len() as u64
        })
    );
}

#[tokio::test]
async fn test_fs_session_lost_response_receipt_lookup() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let session = storage.create_session("myrepo").await.unwrap();
    let data = b"RECEIPT_LOOKUP_DATA";
    let stream = make_test_stream(vec![Bytes::from_static(data)]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            stream,
            1024 * 1024,
        )
        .await
        .unwrap();

    let mut hasher = sha2::Sha256::new();
    hasher.update(data);
    let digest_hex = hex::encode(hasher.finalize());
    let digest = Digest::parse(&format!("sha256:{digest_hex}")).unwrap();

    let prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(data.len() as u64),
            None,
            &digest,
            1024 * 1024,
            true,
        )
        .await
        .unwrap();

    storage.commit_finalize(&prepared).await.unwrap();

    let receipt = storage
        .get_finalized_receipt(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.repo.as_str(), "myrepo");
    assert_eq!(receipt.uuid, session.uuid);
    assert_eq!(receipt.digest, digest.as_str());
    assert_eq!(receipt.size, data.len() as u64);
}

#[tokio::test]
async fn test_fs_session_reaper_skips_locked() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let session = storage.create_session("myrepo").await.unwrap();
    let lock_path = storage.session_lock_path(&session.uuid);
    let _active_lock = acquire_fs_session_lock(lock_path).await.unwrap();

    // Reaper with 0 max_age attempts to reap, but skips locked session
    let reaped = storage.reap_expired_sessions(0, 0).await.unwrap();
    assert_eq!(reaped, 0, "Reaper must skip active locked session");

    drop(_active_lock);

    // Once unlocked, reaper reaps expired session
    let reaped2 = storage.reap_expired_sessions(0, 0).await.unwrap();
    assert_eq!(reaped2, 1, "Reaper must reap unlocked expired session");
}

#[tokio::test]
async fn test_fs_session_reaper_recovers_expired_finalizing() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let session = storage.create_session("myrepo").await.unwrap();
    let data = b"EXP_FIN_RECOVER_DATA";
    let stream = make_test_stream(vec![Bytes::from_static(data)]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            stream,
            1024 * 1024,
        )
        .await
        .unwrap();

    let mut hasher = sha2::Sha256::new();
    hasher.update(data);
    let digest_hex = hex::encode(hasher.finalize());
    let digest = Digest::parse(&format!("sha256:{digest_hex}")).unwrap();

    let _prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(data.len() as u64),
            None,
            &digest,
            1024 * 1024,
            true,
        )
        .await
        .unwrap();

    // Simulate crash after blob publication: move blob to destination manually
    let dest_dir = root
        .join("blobs")
        .join(digest.algorithm())
        .join(digest.prefix2());
    ensure_dir(&dest_dir).unwrap();
    let dest_path = dest_dir.join(digest.hex());
    tokio::fs::write(&dest_path, data).await.unwrap();

    // Reaper runs recovery with max_age = 0 and receipt_ttl = 600
    let _ = storage.reap_expired_sessions(0, 600).await.unwrap();

    // Receipt should now be created
    let receipt = storage.get_finalized_receipt(&session).await.unwrap();
    assert!(receipt.is_some());
}

#[tokio::test]
async fn test_fs_session_digest_mismatch_policy() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    // 1. abort_on_digest_mismatch = true
    let session1 = storage.create_session("myrepo").await.unwrap();
    let stream1 = make_test_stream(vec![Bytes::from_static(b"REAL_BYTES_1")]);
    storage
        .append_if_offset(
            &session1,
            UploadOffsetPrecondition::Exact(0),
            stream1,
            1024 * 1024,
        )
        .await
        .unwrap();

    let wrong_digest =
        Digest::parse("sha256:0000000000000000000000000000000000000000000000000000000000000000")
            .unwrap();

    let err1 = storage
        .begin_finalize(
            &session1,
            UploadOffsetPrecondition::Exact(12),
            None,
            &wrong_digest,
            1024 * 1024,
            true,
        )
        .await
        .unwrap_err();
    assert!(matches!(err1, UploadTransitionError::DigestMismatch { .. }));

    // Staging files cleaned up
    let data1 = storage.session_data_path(&session1.uuid);
    assert!(!tokio::fs::try_exists(&data1).await.unwrap());

    // 2. abort_on_digest_mismatch = false
    let session2 = storage.create_session("myrepo").await.unwrap();
    let stream2 = make_test_stream(vec![Bytes::from_static(b"REAL_BYTES_2")]);
    storage
        .append_if_offset(
            &session2,
            UploadOffsetPrecondition::Exact(0),
            stream2,
            1024 * 1024,
        )
        .await
        .unwrap();

    let err2 = storage
        .begin_finalize(
            &session2,
            UploadOffsetPrecondition::Exact(12),
            None,
            &wrong_digest,
            1024 * 1024,
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err2, UploadTransitionError::DigestMismatch { .. }));

    // Staging files retained
    let data2 = storage.session_data_path(&session2.uuid);
    assert!(tokio::fs::try_exists(&data2).await.unwrap());
}

#[tokio::test]
async fn test_fs_session_sha512_finalization() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let session = storage.create_session("myrepo").await.unwrap();
    let data = b"SHA512_STREAMING_FINALIZATION_PAYLOAD";
    let stream = make_test_stream(vec![Bytes::from_static(data)]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            stream,
            1024 * 1024,
        )
        .await
        .unwrap();

    let mut hasher = sha2::Sha512::new();
    hasher.update(data);
    let digest_hex = hex::encode(hasher.finalize());
    let digest = Digest::parse(&format!("sha512:{digest_hex}")).unwrap();

    let prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(data.len() as u64),
            None,
            &digest,
            1024 * 1024,
            true,
        )
        .await
        .unwrap();

    let outcome = storage.commit_finalize(&prepared).await.unwrap();
    assert_eq!(
        outcome,
        FinalizeOutcome::Published(BlobMeta {
            size: data.len() as u64
        })
    );
}

#[tokio::test]
async fn test_fs_session_restart_resume_finalize_restart_receipt() {
    let root = tmp_fs_root();
    let repo = "restart/repo";
    let chunk1 = b"first chunk before restart;";
    let chunk2 = b" second chunk after restart.";
    let mut full = chunk1.to_vec();
    full.extend_from_slice(chunk2);
    let digest = Digest::parse(&format!("sha256:{}", hex_sha256(&full))).unwrap();

    // 1. Initial process: create session & append chunk 1
    let storage1 = FsStorage::new(root.clone(), 1024 * 1024);
    let session = storage1.create_session(repo).await.unwrap();
    let stream1 = make_test_stream(vec![Bytes::from_static(chunk1)]);
    let app1 = storage1
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            stream1,
            1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(
        app1,
        UploadAppendResult::Committed {
            new_offset: chunk1.len() as u64
        }
    );
    drop(storage1);

    // 2. Second process (simulated restart): resume session & append chunk 2 & finalize
    let storage2 = FsStorage::new(root.clone(), 1024 * 1024);
    let status2 = storage2.session_status(&session).await.unwrap();
    assert_eq!(status2.committed_offset, chunk1.len() as u64);
    assert_eq!(status2.state, UploadSessionState::Active);

    let stream2 = make_test_stream(vec![Bytes::from_static(chunk2)]);
    let app2 = storage2
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(chunk1.len() as u64),
            stream2,
            1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(
        app2,
        UploadAppendResult::Committed {
            new_offset: full.len() as u64
        }
    );

    let prepared = storage2
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(full.len() as u64),
            None,
            &digest,
            1024 * 1024,
            false,
        )
        .await
        .unwrap();
    let outcome = storage2.commit_finalize(&prepared).await.unwrap();
    assert_eq!(
        outcome,
        FinalizeOutcome::Published(BlobMeta {
            size: full.len() as u64
        })
    );
    drop(storage2);

    // 3. Third process (second restart): retrieve receipt
    let storage3 = FsStorage::new(root.clone(), 1024 * 1024);
    let receipt = storage3
        .get_finalized_receipt(&session)
        .await
        .unwrap()
        .expect("receipt exists");
    assert_eq!(receipt.repo.as_str(), repo);
    assert_eq!(receipt.uuid, session.uuid);
    assert_eq!(receipt.digest, digest.as_str());
    assert_eq!(receipt.size, full.len() as u64);

    // Verify canonical files exist
    let receipt_file = storage3.finalized_receipt_path(&session.uuid);
    assert!(tokio::fs::try_exists(&receipt_file).await.unwrap());
    let blob_file = storage3.blob_path(&digest);
    assert!(tokio::fs::try_exists(&blob_file).await.unwrap());
}

#[tokio::test]
async fn test_fs_legacy_fixture_migration() {
    let root = tmp_fs_root();
    let uploads_dir = root.join("uploads");
    tokio::fs::create_dir_all(&uploads_dir).await.unwrap();

    let legacy_uuid = uuid::Uuid::new_v4().to_string();
    let legacy_data = b"LEGACY_UPLOAD_FIXTURE_PAYLOAD";

    // Write raw legacy flat file at uploads/<uuid>
    let legacy_file = uploads_dir.join(&legacy_uuid);
    tokio::fs::write(&legacy_file, legacy_data).await.unwrap();

    let storage = FsStorage::new(root.clone(), 1024 * 1024);
    let session = UploadSessionId::new(
        crate::registry::canonical_name::CanonicalRepoName::parse("legacy/repo").unwrap(),
        &legacy_uuid,
    );

    // First access via session_status triggers migration
    let status = storage.session_status(&session).await.unwrap();
    assert_eq!(status.committed_offset, legacy_data.len() as u64);
    assert_eq!(status.state, UploadSessionState::Active);

    // Verify canonical layout exists
    let data_path = storage.session_data_path(&legacy_uuid);
    let meta_path = storage.session_meta_path(&legacy_uuid);
    let hash_path = storage.session_hash_path(&legacy_uuid, 0);

    assert!(tokio::fs::try_exists(&data_path).await.unwrap());
    assert!(tokio::fs::try_exists(&meta_path).await.unwrap());
    assert!(tokio::fs::try_exists(&hash_path).await.unwrap());
    assert!(!tokio::fs::try_exists(&legacy_file).await.unwrap());

    // Finalize the migrated session
    let digest = Digest::parse(&format!("sha256:{}", hex_sha256(legacy_data))).unwrap();
    let prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(legacy_data.len() as u64),
            None,
            &digest,
            1024 * 1024,
            false,
        )
        .await
        .unwrap();
    let outcome = storage.commit_finalize(&prepared).await.unwrap();
    assert_eq!(
        outcome,
        FinalizeOutcome::Published(BlobMeta {
            size: legacy_data.len() as u64
        })
    );
}

#[tokio::test]
async fn test_storage_try_from_config_invalid_path_fails_cleanly() {
    let temp_dir = tempfile::tempdir().unwrap();
    // Create a regular file at the target root path to make directory creation fail
    let file_path = temp_dir.path().join("existing_file");
    std::fs::write(&file_path, b"not a directory").unwrap();
    let invalid_root = file_path.join("sub_dir");

    let res = FsStorage::try_new(invalid_root.clone(), 1024 * 1024);
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io)
    );
    let err_msg = err.to_string();
    assert!(err_msg.contains(&invalid_root.display().to_string()));
    assert!(err_msg.contains("failed to create storage dir"));
}

#[tokio::test]
async fn test_fs_membership_record_round_trip_and_lifecycle() {
    use crate::storage::repo_membership::{
        MembershipProvenance, MembershipState, RepoBlobMembershipRecord,
        RepositoryBlobMembershipStorage,
    };
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let repo = "my-test-repo";
    let d1 =
        Digest::parse("sha256:0000000000000000000000000000000000000000000000000000000000000001")
            .unwrap();
    let d2 =
        Digest::parse("sha256:0000000000000000000000000000000000000000000000000000000000000002")
            .unwrap();

    let canonical_repo = crate::registry::canonical_name::CanonicalRepoName::parse(repo).unwrap();
    let canonical_source =
        crate::registry::canonical_name::CanonicalRepoName::parse("source-repo").unwrap();

    let r_upload = RepoBlobMembershipRecord::new_upload(
        canonical_repo.clone(),
        d1.clone(),
        Some("sess-1".to_string()),
    );
    let r_cross = RepoBlobMembershipRecord::new_cross_mount(
        canonical_repo.clone(),
        d2.clone(),
        canonical_source.clone(),
    );

    storage.link_repo_blob(&r_upload).await.unwrap();
    storage.link_repo_blob(&r_cross).await.unwrap();

    let fetched1 = storage
        .get_repo_blob_membership(repo, &d1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched1.provenance, MembershipProvenance::Upload);
    assert_eq!(fetched1.session_id, Some("sess-1".to_string()));
    assert_eq!(fetched1.state, MembershipState::Active);

    let fetched2 = storage
        .get_repo_blob_membership(repo, &d2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        fetched2.provenance,
        MembershipProvenance::CrossMount {
            from_repo: canonical_source
        }
    );

    // Candidate aging transition
    storage
        .set_membership_candidate(repo, &d1, 1000)
        .await
        .unwrap();
    let cand = storage
        .get_repo_blob_membership(repo, &d1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cand.state, MembershipState::Candidate);
    assert_eq!(cand.unreferenced_since_unix_secs, Some(1000));

    // Candidate clearing transition
    storage.clear_membership_candidate(repo, &d1).await.unwrap();
    let active = storage
        .get_repo_blob_membership(repo, &d1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.state, MembershipState::Active);
    assert_eq!(active.unreferenced_since_unix_secs, None);

    // Pagination
    let (page, next_tok) = storage
        .list_repo_blob_memberships_page(repo, None, 1)
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert!(next_tok.is_some());
    let (page2, next_tok2) = storage
        .list_repo_blob_memberships_page(repo, next_tok.as_deref(), 1)
        .await
        .unwrap();
    assert_eq!(page2.len(), 1);
    assert!(next_tok2.is_none());

    // Unlink
    assert!(storage.unlink_repo_blob(repo, &d1).await.unwrap());
    assert!(!storage.unlink_repo_blob(repo, &d1).await.unwrap());
    assert!(
        storage
            .get_repo_blob_membership(repo, &d1)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_fs_cas_enumeration_fails_closed_on_malformed_prefix_dir() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    // Put an invalid file directly in blobs/sha256/
    let invalid_file = root.join("blobs").join("sha256").join("not_a_dir.txt");
    tokio::fs::create_dir_all(invalid_file.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&invalid_file, b"corrupted").await.unwrap();

    let res = storage.list_cas_blobs_page(None, 100).await;
    assert!(
        res.is_err(),
        "enumeration must fail closed on non-dir prefix"
    );
    assert_eq!(
        res.unwrap_err().internal_kind(),
        Some(crate::storage::StorageErrorKind::CorruptData)
    );
}

#[tokio::test]
async fn test_fs_cas_enumeration_fails_closed_on_malformed_blob_file() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    // Put an invalid file name in blobs/sha256/ab/
    let invalid_blob = root
        .join("blobs")
        .join("sha256")
        .join("ab")
        .join("short_hex");
    tokio::fs::create_dir_all(invalid_blob.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&invalid_blob, b"corrupted").await.unwrap();

    let res = storage.list_cas_blobs_page(None, 100).await;
    assert!(
        res.is_err(),
        "enumeration must fail closed on malformed hex filename"
    );
    assert_eq!(
        res.unwrap_err().internal_kind(),
        Some(crate::storage::StorageErrorKind::CorruptData)
    );
}

#[tokio::test]
async fn test_atomic_write_file_invalid_path_invariant() {
    let res = atomic_write_file(Path::new(""), b"test-payload").await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::InternalInvariant),
        "atomic_write_file with path having no parent must return StorageErrorKind::InternalInvariant"
    );
    assert_eq!(err.message(), Some("invalid path"));
    assert_eq!(err.to_string(), "internal error: invalid path");
}

#[tokio::test]
async fn test_detect_manifest_media_type_malformed_json_is_corrupt_data() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root, 1024 * 1024);

    let malformed_bytes = b"{{{ malformed JSON";
    let expected_err = serde_json::from_slice::<serde_json::Value>(malformed_bytes).unwrap_err();
    let expected_message = expected_err.to_string();

    let res = storage.detect_manifest_media_type(malformed_bytes).await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::CorruptData),
        "Malformed manifest JSON in detect_manifest_media_type must classify as CorruptData"
    );

    assert_eq!(err.message(), Some(expected_message.as_str()));
    assert_eq!(
        err.to_string(),
        format!("internal error: {expected_message}")
    );
}

#[tokio::test]
async fn test_repository_enumeration_io_failure_is_io() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    // Create a regular file at the 'repos' path so read_dir fails with ENOTDIR (deterministic, root-safe)
    let repos_path = root.join("repos");
    std::fs::write(&repos_path, b"not a directory").unwrap();

    let expected_err = tokio::fs::read_dir(&repos_path).await.unwrap_err();
    let expected_message = expected_err.to_string();

    let res = storage.list_repositories().await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io),
        "read_dir on non-directory file must classify as StorageErrorKind::Io"
    );

    assert_eq!(err.message(), Some(expected_message.as_str()));
    assert_eq!(
        err.to_string(),
        format!("internal error: {expected_message}")
    );
}

#[tokio::test]
async fn test_get_upload_session_malformed_json_is_corrupt_data() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let canonical_repo =
        crate::registry::canonical_name::CanonicalRepoName::parse("testrepo").unwrap();
    let uuid = uuid::Uuid::new_v4().to_string();
    let session = UploadSessionId::new(canonical_repo, &uuid);

    // Ensure uploads directory exists and write malformed session metadata JSON
    let uploads_dir = storage.uploads_dir();
    std::fs::create_dir_all(&uploads_dir).unwrap();
    let meta_path = storage.session_meta_path(&session.uuid);
    let malformed_bytes = b"{{{ malformed session json";
    std::fs::write(&meta_path, malformed_bytes).unwrap();

    let expected_err = serde_json::from_slice::<FsSessionMetaRecord>(malformed_bytes).unwrap_err();
    let expected_message = expected_err.to_string();

    let res = storage.session_status(&session).await;
    match res {
        Err(UploadTransitionError::Storage(err)) => {
            assert_eq!(
                err.internal_kind(),
                Some(crate::storage::StorageErrorKind::CorruptData),
                "Malformed session metadata in session_status must classify as CorruptData"
            );
            assert_eq!(err.message(), Some(expected_message.as_str()));
            assert_eq!(
                err.to_string(),
                format!("internal error: {expected_message}")
            );
        }
        other => panic!("expected UploadTransitionError::Storage with CorruptData, got {other:?}"),
    }
}

#[tokio::test]
async fn test_upload_session_mutation_malformed_json_is_corrupt_data() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let canonical_repo =
        crate::registry::canonical_name::CanonicalRepoName::parse("testrepo").unwrap();
    let uuid = uuid::Uuid::new_v4().to_string();
    let session = UploadSessionId::new(canonical_repo, &uuid);

    // Ensure uploads directory exists and write malformed session metadata JSON
    let uploads_dir = storage.uploads_dir();
    std::fs::create_dir_all(&uploads_dir).unwrap();
    let meta_path = storage.session_meta_path(&session.uuid);
    let malformed_bytes = b"{{{ malformed mutation session json";
    std::fs::write(&meta_path, malformed_bytes).unwrap();

    let expected_err = serde_json::from_slice::<FsSessionMetaRecord>(malformed_bytes).unwrap_err();
    let expected_message = expected_err.to_string();

    let res = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![]),
            1024 * 1024,
        )
        .await;

    match res {
        Err(UploadTransitionError::Storage(err)) => {
            assert_eq!(
                err.internal_kind(),
                Some(crate::storage::StorageErrorKind::CorruptData),
                "Malformed session metadata in append_if_offset must classify as CorruptData"
            );
            assert_eq!(err.message(), Some(expected_message.as_str()));
            assert_eq!(
                err.to_string(),
                format!("internal error: {expected_message}")
            );
        }
        other => panic!("expected UploadTransitionError::Storage with CorruptData, got {other:?}"),
    }
}

#[tokio::test]
async fn test_begin_finalize_malformed_json_is_corrupt_data() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let canonical_repo =
        crate::registry::canonical_name::CanonicalRepoName::parse("testrepo").unwrap();
    let uuid = uuid::Uuid::new_v4().to_string();
    let session = UploadSessionId::new(canonical_repo, &uuid);

    // Ensure uploads directory exists and write malformed session metadata JSON
    let uploads_dir = storage.uploads_dir();
    std::fs::create_dir_all(&uploads_dir).unwrap();
    let meta_path = storage.session_meta_path(&session.uuid);
    let malformed_bytes = b"{{{ malformed finalize session json";
    std::fs::write(&meta_path, malformed_bytes).unwrap();

    let expected_err = serde_json::from_slice::<FsSessionMetaRecord>(malformed_bytes).unwrap_err();
    let expected_message = expected_err.to_string();

    let digest =
        Digest::parse("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .unwrap();

    let res = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(0),
            None,
            &digest,
            1024 * 1024,
            false,
        )
        .await;

    match res {
        Err(UploadTransitionError::Storage(err)) => {
            assert_eq!(
                err.internal_kind(),
                Some(crate::storage::StorageErrorKind::CorruptData),
                "Malformed session metadata in begin_finalize must classify as CorruptData"
            );
            assert_eq!(err.message(), Some(expected_message.as_str()));
            assert_eq!(
                err.to_string(),
                format!("internal error: {expected_message}")
            );
        }
        other => panic!("expected UploadTransitionError::Storage with CorruptData, got {other:?}"),
    }
}

#[tokio::test]
async fn test_commit_finalize_malformed_json_is_corrupt_data() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let canonical_repo =
        crate::registry::canonical_name::CanonicalRepoName::parse("testrepo").unwrap();
    let uuid = uuid::Uuid::new_v4().to_string();
    let session = UploadSessionId::new(canonical_repo, &uuid);

    // Ensure uploads directory exists and write malformed session metadata JSON
    let uploads_dir = storage.uploads_dir();
    std::fs::create_dir_all(&uploads_dir).unwrap();
    let meta_path = storage.session_meta_path(&session.uuid);
    let malformed_bytes = b"{{{ malformed commit finalize session json";
    std::fs::write(&meta_path, malformed_bytes).unwrap();

    let expected_err = serde_json::from_slice::<FsSessionMetaRecord>(malformed_bytes).unwrap_err();
    let expected_message = expected_err.to_string();

    let digest =
        Digest::parse("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .unwrap();

    let prepared = crate::storage::upload_session::PreparedFinalize {
        session,
        operation_id: "op-test-123".to_string(),
        expected_digest: digest,
        committed_offset: 0,
        size: 0,
    };

    let res = storage.commit_finalize(&prepared).await;

    match res {
        Err(UploadTransitionError::Storage(err)) => {
            assert_eq!(
                err.internal_kind(),
                Some(crate::storage::StorageErrorKind::CorruptData),
                "Malformed session metadata in commit_finalize must classify as CorruptData"
            );
            assert_eq!(err.message(), Some(expected_message.as_str()));
            assert_eq!(
                err.to_string(),
                format!("internal error: {expected_message}")
            );
        }
        other => panic!("expected UploadTransitionError::Storage with CorruptData, got {other:?}"),
    }
}

#[tokio::test]
async fn test_get_finalized_receipt_malformed_json_is_corrupt_data() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let canonical_repo =
        crate::registry::canonical_name::CanonicalRepoName::parse("testrepo").unwrap();
    let uuid = uuid::Uuid::new_v4().to_string();
    let session = UploadSessionId::new(canonical_repo, &uuid);

    // Ensure finalized directory exists and write malformed receipt JSON
    let finalized_dir = storage.finalized_dir();
    std::fs::create_dir_all(&finalized_dir).unwrap();
    let receipt_path = storage.finalized_receipt_path(&session.uuid);
    let malformed_bytes = b"{{{ malformed receipt json";
    std::fs::write(&receipt_path, malformed_bytes).unwrap();

    let expected_err = serde_json::from_slice::<FinalizedReceipt>(malformed_bytes).unwrap_err();
    let expected_message = expected_err.to_string();

    let res = storage.get_finalized_receipt(&session).await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::CorruptData),
        "Malformed finalized receipt JSON must classify as CorruptData"
    );
    assert_eq!(err.message(), Some(expected_message.as_str()));
    assert_eq!(
        err.to_string(),
        format!("internal error: {expected_message}")
    );
}

#[tokio::test]
async fn test_get_repo_blob_membership_malformed_json_is_corrupt_data() {
    use crate::storage::repo_membership::RepositoryBlobMembershipStorage;

    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let canonical = CanonicalRepoName::parse("testrepo").unwrap();
    let digest =
        Digest::parse("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .unwrap();

    let path = storage.repo_blob_path(&canonical, &digest);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let malformed_bytes = b"{{{ malformed membership json";
    std::fs::write(&path, malformed_bytes).unwrap();

    let expected_err = serde_json::from_slice::<
        crate::storage::repo_membership::RepoBlobMembershipRecord,
    >(malformed_bytes)
    .unwrap_err();
    let expected_message = format!(
        "corrupt membership record in {}: {expected_err}",
        path.display()
    );

    let res = storage.get_repo_blob_membership("testrepo", &digest).await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::CorruptData),
        "Malformed repo blob membership JSON must classify as CorruptData"
    );
    assert_eq!(err.message(), Some(expected_message.as_str()));
    assert_eq!(
        err.to_string(),
        format!("internal error: {expected_message}")
    );
}

#[tokio::test]
async fn test_set_membership_candidate_malformed_json_is_corrupt_data() {
    use crate::storage::repo_membership::RepositoryBlobMembershipStorage;

    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let canonical = CanonicalRepoName::parse("testrepo").unwrap();
    let digest =
        Digest::parse("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .unwrap();

    let path = storage.repo_blob_path(&canonical, &digest);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let malformed_bytes = b"{{{ malformed candidate membership json";
    std::fs::write(&path, malformed_bytes).unwrap();

    let expected_err = serde_json::from_slice::<
        crate::storage::repo_membership::RepoBlobMembershipRecord,
    >(malformed_bytes)
    .unwrap_err();
    let expected_message = format!("corrupt membership record: {expected_err}");

    let res = storage
        .set_membership_candidate("testrepo", &digest, 12345)
        .await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::CorruptData),
        "Malformed candidate membership JSON must classify as CorruptData"
    );
    assert_eq!(err.message(), Some(expected_message.as_str()));
    assert_eq!(
        err.to_string(),
        format!("internal error: {expected_message}")
    );
}

#[tokio::test]
async fn test_list_all_repo_blob_memberships_page_corrupt_repo_dir_is_corrupt_data() {
    use crate::storage::repo_membership::RepositoryBlobMembershipStorage;

    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let invalid_repo_encoded = "invalid!!repo++name";
    let repo_dir = root
        .join("repo-memberships")
        .join("by-repo")
        .join(invalid_repo_encoded);
    std::fs::create_dir_all(&repo_dir).unwrap();

    let expected_err =
        crate::storage::repo_membership::decode_canonical_repo_key(invalid_repo_encoded)
            .unwrap_err();
    let expected_message =
        format!("corrupt repository membership directory '{invalid_repo_encoded}': {expected_err}");

    let res = storage.list_all_repo_blob_memberships_page(None, 10).await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::CorruptData),
        "Corrupt repository membership directory name must classify as CorruptData"
    );
    assert_eq!(err.message(), Some(expected_message.as_str()));
    assert_eq!(
        err.to_string(),
        format!("internal error: {expected_message}")
    );
}

#[tokio::test]
async fn test_list_repo_blob_memberships_page_io_error_is_io() {
    use crate::storage::repo_membership::RepositoryBlobMembershipStorage;

    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let canonical = CanonicalRepoName::parse("testrepo").unwrap();
    let encoded_repo = crate::storage::repo_membership::encode_canonical_repo_key(&canonical);
    let algo_dir = root
        .join("repo-memberships")
        .join("by-repo")
        .join(encoded_repo)
        .join("sha256");

    // Create a directory instead of a regular file ending in .json to deterministically trigger an OS I/O error on file read
    let cand_path =
        algo_dir.join("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855.json");
    std::fs::create_dir_all(&cand_path).unwrap();

    let raw_io_err = tokio::fs::read(&cand_path).await.unwrap_err();
    let expected_message = format!(
        "failed to read membership in {}: {raw_io_err}",
        cand_path.display()
    );

    let res = storage
        .list_repo_blob_memberships_page("testrepo", None, 10)
        .await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io),
        "Filesystem read failure during membership enumeration must classify as Io"
    );
    assert_eq!(err.message(), Some(expected_message.as_str()));
    assert_eq!(
        err.to_string(),
        format!("internal error: {expected_message}")
    );
}

#[tokio::test]
async fn test_get_migration_checkpoint_malformed_json_is_corrupt_data() {
    use crate::storage::repo_membership::RepositoryBlobMembershipStorage;

    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let meta_dir = root.join("meta");
    std::fs::create_dir_all(&meta_dir).unwrap();
    let cp_path = meta_dir.join("migration_checkpoint.json");
    let malformed_bytes = b"{{{ malformed migration checkpoint json";
    std::fs::write(&cp_path, malformed_bytes).unwrap();

    let expected_err = serde_json::from_slice::<
        crate::storage::repo_membership::MigrationCheckpointRecord,
    >(malformed_bytes)
    .unwrap_err();
    let expected_message = format!("corrupt migration checkpoint: {expected_err}");

    let res = storage.get_migration_checkpoint().await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::CorruptData),
        "Malformed migration checkpoint JSON must classify as CorruptData"
    );
    assert_eq!(err.message(), Some(expected_message.as_str()));
    assert_eq!(
        err.to_string(),
        format!("internal error: {expected_message}")
    );
}

#[tokio::test]
async fn test_list_cas_blobs_for_gc_malformed_prefix_is_corrupt_data() {
    use crate::storage::GcStorage;

    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let invalid_prefix_name = "invalid_prefix_name";
    let invalid_prefix = root.join("blobs").join("sha256").join(invalid_prefix_name);
    std::fs::create_dir_all(&invalid_prefix).unwrap();

    let expected_message =
        format!("malformed 2-char prefix directory name in CAS root: {invalid_prefix_name}");

    let res = storage.list_cas_blobs_page(None, 10).await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::CorruptData),
        "Malformed 2-char prefix directory name in CAS root must classify as CorruptData"
    );
    assert_eq!(err.message(), Some(expected_message.as_str()));
    assert_eq!(
        err.to_string(),
        format!("internal error: {expected_message}")
    );
}

#[tokio::test]
async fn test_list_cas_blobs_for_gc_malformed_blob_filename_is_corrupt_data() {
    use crate::storage::GcStorage;

    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let shard_name = "e3";
    let shard_dir = root.join("blobs").join("sha256").join(shard_name);
    std::fs::create_dir_all(&shard_dir).unwrap();

    let invalid_filename = "not_a_valid_64_char_hex_hash.bin";
    let invalid_file = shard_dir.join(invalid_filename);
    std::fs::write(&invalid_file, b"test content").unwrap();

    // Contained listing identifies the shard by its 2-char prefix rather than host path
    let expected_message =
        format!("malformed blob file name in CAS shard {shard_name}: {invalid_filename}");

    let res = storage.list_cas_blobs_page(None, 10).await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::CorruptData),
        "Malformed blob filename in CAS shard must classify as CorruptData"
    );
    assert_eq!(err.message(), Some(expected_message.as_str()));
    assert_eq!(
        err.to_string(),
        format!("internal error: {expected_message}")
    );
}

#[tokio::test]
async fn test_blocking_task_join_error_is_internal_invariant() {
    let join_handle = tokio::task::spawn_blocking(|| {
        panic!("deliberate worker panic for join error test");
    });
    let join_res = join_handle.await;
    assert!(join_res.is_err(), "deliberate panic must yield JoinError");
    let join_err = join_res.unwrap_err();
    let expected_message = join_err.to_string();

    let err = map_blocking_join_error(join_err);
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::InternalInvariant)
    );
    assert_eq!(err.message(), Some(expected_message.as_str()));
    assert_eq!(
        err.to_string(),
        format!("internal error: {expected_message}")
    );
}

#[tokio::test]
async fn test_list_cas_blobs_for_gc_non_directory_root_is_corrupt_data() {
    use crate::storage::GcStorage;

    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let blobs_dir = root.join("blobs");
    std::fs::create_dir_all(&blobs_dir).unwrap();
    let cas_root_file = blobs_dir.join("sha256");
    std::fs::write(&cas_root_file, b"not a directory").unwrap();

    // Under the approved cutover, an intermediate or final non-directory component
    // maps to StorageErrorKind::CorruptData rather than legacy generic Io.
    let expected_message = "target path is not a directory: Some(\"blobs/sha256\")";

    let res = storage.list_cas_blobs_page(None, 10).await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::CorruptData),
        "Non-directory CAS root component must classify as CorruptData"
    );
    assert_eq!(err.message(), Some(expected_message));
    assert_eq!(
        err.to_string(),
        format!("internal error: {expected_message}")
    );
}

#[tokio::test]
async fn test_delete_blob_conditional_missing_version_precondition_is_conflict() {
    use crate::storage::GcStorage;
    use crate::storage::mutation_authority::RuntimeMutationAuthority;

    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let authority = RuntimeMutationAuthority::acquire(
        Arc::new(FsStorage::new(root.clone(), 1024 * 1024)),
        "test-gc-node",
    )
    .await
    .expect("acquire mutation authority");
    let permit = authority.gc_mutation_permit();
    let digest =
        Digest::parse("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .expect("valid digest");

    let expected_message = "conditional delete on filesystem storage requires expected version";
    let res = storage
        .delete_blob_conditional(&permit, &digest, None)
        .await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Conflict),
        "Missing version precondition must classify as Conflict"
    );
    assert_eq!(err.message(), Some(expected_message));
    assert_eq!(
        err.to_string(),
        format!("internal error: {expected_message}")
    );
}

#[tokio::test]
async fn test_quarantine_blob_invalid_permit_is_permission_denied() {
    use crate::storage::GcStorage;
    use crate::storage::mutation_authority::RuntimeMutationAuthority;

    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let authority = RuntimeMutationAuthority::acquire(
        Arc::new(FsStorage::new(root.clone(), 1024 * 1024)),
        "test-gc-node",
    )
    .await
    .expect("acquire mutation authority");
    let permit = authority.gc_mutation_permit();
    let _guard = authority.set_test_inactive_guard();
    assert!(!permit.is_valid());

    let digest =
        Digest::parse("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .expect("valid digest");
    let version = BlobObjectVersion("fs:0:0:dummy".to_string());

    let expected_message = "invalid or inactive GC mutation permit";
    let res = storage.quarantine_blob(&permit, &digest, &version).await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::PermissionDenied),
        "Invalid permit must classify as PermissionDenied"
    );
    assert_eq!(err.message(), Some(expected_message));
    assert_eq!(
        err.to_string(),
        format!("internal error: {expected_message}")
    );
}

#[tokio::test]
async fn test_fs_metadata_size_sparse_file_preserves_exact_size_above_u32() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 10 * 1024 * 1024 * 1024);

    let hex = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    let digest = Digest::parse(&format!("sha256:{hex}")).unwrap();
    let path = storage.blob_path(&digest);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dirs");
    }

    let expected_size: u64 = 8_589_934_592; // 8 GiB (> 4 GiB u32 boundary)
    let file = std::fs::File::create(&path).expect("create sparse file");
    file.set_len(expected_size).expect("set sparse file length");

    // Test the low-level mechanism directly
    let size = fs_metadata_size(&path)
        .await
        .expect("fs_metadata_size must succeed");
    assert_eq!(size, expected_size);

    // Test delegation through head_blob
    let meta = storage
        .head_blob(&digest)
        .await
        .expect("head_blob must succeed");
    assert_eq!(meta.size, expected_size);

    let _ = std::fs::remove_dir_all(&root);
}

#[cfg(unix)]
struct PermGuard<'a>(&'a Path);

#[cfg(unix)]
impl<'a> Drop for PermGuard<'a> {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(self.0, std::fs::Permissions::from_mode(0o700));
    }
}

#[tokio::test]
async fn test_fs_metadata_size_deterministic_typed_io_errors() {
    let root = tmp_fs_root();

    // 1. NotFound case: nonexistent path returns typed NotFound
    let missing_path = root.join("nonexistent_blob.bin");
    let err_not_found = fs_metadata_size(&missing_path)
        .await
        .expect_err("nonexistent file must fail");
    assert_eq!(err_not_found.kind(), std::io::ErrorKind::NotFound);

    // 2. Intermediate regular file component: non-NotFound typed I/O error
    let intermediate_file = root.join("intermediate_regular_file.bin");
    std::fs::write(&intermediate_file, b"content").expect("write intermediate regular file");
    let child_path = intermediate_file.join("sub_item");
    let err_not_dir = fs_metadata_size(&child_path)
        .await
        .expect_err("metadata lookup through regular file must fail");
    assert_ne!(
        err_not_dir.kind(),
        std::io::ErrorKind::NotFound,
        "Intermediate regular file must yield a non-NotFound I/O error"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
#[cfg(unix)]
#[ignore = "requires unprivileged user environment where chmod 0o000 denies filesystem access"]
async fn test_fs_metadata_size_environment_permission_denied() {
    use std::os::unix::fs::PermissionsExt;
    let root = tmp_fs_root();
    let restricted_dir = root.join("restricted_dir");
    std::fs::create_dir_all(&restricted_dir).expect("create restricted dir");
    let inaccessible_path = restricted_dir.join("inaccessible.bin");
    std::fs::write(&inaccessible_path, b"secret").expect("write test file");

    {
        let _guard = PermGuard(&restricted_dir);
        std::fs::set_permissions(&restricted_dir, std::fs::Permissions::from_mode(0o000))
            .expect("set permissions 0o000");

        let err_perm = fs_metadata_size(&inaccessible_path)
            .await
            .expect_err("metadata query on inaccessible path must fail");
        assert_eq!(err_perm.kind(), std::io::ErrorKind::PermissionDenied);
    }

    std::fs::remove_dir_all(&root).expect("cleanup test temp directory");
}

#[tokio::test]
async fn test_head_blob_ordinary_wins_over_quarantine() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
            .unwrap();
    let live_content = b"live payload";
    let quarantine_content = b"quarantine payload is different length";

    write_file(&storage.blob_path(&digest), live_content);
    write_file(&storage.quarantine_blob_path(&digest), quarantine_content);

    let meta = storage
        .head_blob(&digest)
        .await
        .expect("head_blob must succeed");
    assert_eq!(meta.size, live_content.len() as u64);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn test_head_blob_quarantine_only_and_both_missing_baselines() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest_quarantine =
        Digest::parse("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .unwrap();
    let quarantine_content = b"quarantined content";
    write_file(
        &storage.quarantine_blob_path(&digest_quarantine),
        quarantine_content,
    );

    // Quarantine-only succeeds
    let meta = storage
        .head_blob(&digest_quarantine)
        .await
        .expect("head_blob must fall back to quarantine");
    assert_eq!(meta.size, quarantine_content.len() as u64);

    // Both missing returns StorageError::NotFound
    let digest_missing =
        Digest::parse("sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
            .unwrap();
    let res = storage.head_blob(&digest_missing).await;
    assert!(matches!(res, Err(StorageError::NotFound)));

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn test_head_blob_deterministic_non_not_found_suppresses_quarantine_and_preserves_error() {
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
            .unwrap();

    // Quarantine blob exists
    let quarantine_content = b"quarantine blob exists";
    write_file(&storage.quarantine_blob_path(&digest), quarantine_content);

    // Live blob path is: <root>/blobs/<prefix>/<hex_rest>
    // Create <root>/blobs/<prefix> as a regular file so directory traversal fails
    let blob_path = storage.blob_path(&digest);
    let parent = blob_path.parent().expect("blob path parent");
    if let Some(grandparent) = parent.parent() {
        std::fs::create_dir_all(grandparent).expect("create grandparent dirs");
    }
    std::fs::write(parent, b"regular file blocking directory").expect("write blocking file");

    // Capture the exact low-level I/O error from fs_metadata_size
    let expected_io_err = fs_metadata_size(&blob_path)
        .await
        .expect_err("metadata on child of regular file must fail");
    assert_ne!(expected_io_err.kind(), std::io::ErrorKind::NotFound);

    let expected_msg = expected_io_err.to_string();
    let expected_display = format!("internal error: {expected_msg}");

    let res = storage.head_blob(&digest).await;
    assert!(res.is_err(), "head_blob must fail on non-not-found error");
    let err = res.unwrap_err();

    // Outward error must remain StorageErrorKind::Io without falling back to quarantine
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io),
        "Non-NotFound error must produce StorageErrorKind::Io without falling back to quarantine"
    );
    // Compare exact diagnostic and Display string by equality against original reference
    assert_eq!(err.message(), Some(expected_msg.as_str()));
    assert_eq!(err.to_string(), expected_display);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
#[cfg(unix)]
#[ignore = "requires unprivileged user environment where chmod 0o000 denies filesystem access"]
async fn test_head_blob_environment_permission_denied_suppresses_quarantine() {
    use std::os::unix::fs::PermissionsExt;
    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
            .unwrap();

    // Quarantine blob exists
    let quarantine_content = b"quarantine blob exists";
    write_file(&storage.quarantine_blob_path(&digest), quarantine_content);

    let blob_path = storage.blob_path(&digest);
    let parent = blob_path.parent().expect("blob path parent");
    std::fs::create_dir_all(parent).expect("create parent dirs");
    write_file(&blob_path, b"live blob");

    {
        // RAII guard ensures 0o700 is restored on drop even if an assertion panics
        let _guard = PermGuard(parent);
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o000))
            .expect("set parent permissions 0o000");

        // Capture expected I/O error directly from the low-level helper
        let expected_io_err = fs_metadata_size(&blob_path)
            .await
            .expect_err("metadata on permission-denied path must fail");
        assert_eq!(expected_io_err.kind(), std::io::ErrorKind::PermissionDenied);

        let expected_msg = expected_io_err.to_string();
        let expected_display = format!("internal error: {expected_msg}");

        let res = storage.head_blob(&digest).await;
        assert!(res.is_err(), "head_blob must fail on permission error");
        let err = res.unwrap_err();

        // Outward error must remain StorageErrorKind::Io without falling back to quarantine
        assert_eq!(
            err.internal_kind(),
            Some(crate::storage::StorageErrorKind::Io),
            "Permission error must produce StorageErrorKind::Io without falling back to quarantine"
        );
        // Compare exact diagnostic and Display string by equality against OS reference
        assert_eq!(err.message(), Some(expected_msg.as_str()));
        assert_eq!(err.to_string(), expected_display);
    }

    std::fs::remove_dir_all(&root).expect("cleanup test temp directory");
}

#[tokio::test]
async fn test_storage_error_conversion_evidence_permission_denied() {
    // Synthetic conversion evidence: verifies outward boundary conversion preserves
    // StorageErrorKind::Io and formatting for PermissionDenied without needing OS chmod.
    let synthetic_io_err = std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "synthetic permission denied for conversion verification",
    );
    let expected_msg = synthetic_io_err.to_string();
    let expected_display = format!("internal error: {expected_msg}");

    let outward_err = StorageError::io(synthetic_io_err.to_string());
    assert_eq!(
        outward_err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io)
    );
    assert_eq!(outward_err.message(), Some(expected_msg.as_str()));
    assert_eq!(outward_err.to_string(), expected_display);
}

#[tokio::test]
#[cfg(unix)]
async fn test_fs_metadata_containment_symlink_inside_root() {
    // Characterizes Case 1: Final blob entry is a symlink to an ordinary file inside configured root.
    // Under accepted Policy C: symlinks below root are strictly rejected during acquisition.
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage_root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:1111111111111111111111111111111111111111111111111111111111111101")
            .unwrap();
    let target_inside = root.join("blobs").join("target_inside.bin");
    let target_content = b"inside root regular file target";
    write_file(&target_inside, target_content);

    let blob_path = storage.blob_path(&digest);
    if let Some(parent) = blob_path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dirs");
    }
    std::os::unix::fs::symlink(&target_inside, &blob_path).expect("create inside-root symlink");

    // Legacy helper fs_metadata_size follows symlink (historical baseline preserved)
    let size = fs_metadata_size(&blob_path)
        .await
        .expect("fs_metadata_size follows inside-root symlink");
    assert_eq!(size, target_content.len() as u64);

    // Production head_blob rejects symlink under accepted containment policy
    let head_err = storage
        .head_blob(&digest)
        .await
        .expect_err("production head_blob must reject inside-root symlink under containment");
    assert_eq!(
        head_err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io),
        "containment rejection must map to StorageErrorKind::Io"
    );

    // Production open_blob rejects symlink under accepted containment policy
    let open_err = expect_open_blob_err(
        &storage,
        &digest,
        "production open_blob must reject inside-root symlink under containment",
    )
    .await;
    assert_eq!(
        open_err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io),
        "containment rejection must map to StorageErrorKind::Io"
    );
}

#[tokio::test]
#[cfg(unix)]
async fn test_fs_metadata_containment_symlink_outside_root() {
    // Characterizes Case 2: Final blob entry points outside configured root but inside test fixture.
    // Under accepted Policy C: symlinks escaping root boundary are strictly rejected.
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage_root");
    let outside = fixture.path().join("outside_target");
    std::fs::create_dir_all(&root).expect("create storage root");
    std::fs::create_dir_all(&outside).expect("create outside target dir");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:2222222222222222222222222222222222222222222222222222222222222202")
            .unwrap();
    let target_outside = outside.join("target_outside.bin");
    let outside_content = b"outside root regular file payload";
    write_file(&target_outside, outside_content);

    let blob_path = storage.blob_path(&digest);
    if let Some(parent) = blob_path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dirs");
    }
    std::os::unix::fs::symlink(&target_outside, &blob_path).expect("create outside-root symlink");

    // Legacy helper fs_metadata_size follows symlink (historical baseline preserved)
    let size = fs_metadata_size(&blob_path)
        .await
        .expect("fs_metadata_size follows outside-root symlink");
    assert_eq!(size, outside_content.len() as u64);

    // Production head_blob rejects outside-root symlink under accepted containment policy
    let head_err = storage
        .head_blob(&digest)
        .await
        .expect_err("production head_blob must reject outside-root symlink under containment");
    assert_eq!(
        head_err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io)
    );

    // Production open_blob rejects outside-root symlink under accepted containment policy
    let open_err = expect_open_blob_err(
        &storage,
        &digest,
        "production open_blob must reject outside-root symlink under containment",
    )
    .await;
    assert_eq!(
        open_err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io)
    );
}

#[tokio::test]
#[cfg(unix)]
async fn test_fs_metadata_containment_intermediate_dir_symlink_outside_root() {
    // Characterizes Case 3: Intermediate directory is a symlink pointing outside root.
    // Under accepted Policy C: intermediate directory symlinks are rejected during openat2 resolution.
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage_root");
    let outside = fixture.path().join("outside_dir");
    std::fs::create_dir_all(&root).expect("create storage root");
    std::fs::create_dir_all(&outside).expect("create outside dir");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:3333333333333333333333333333333333333333333333333333333333333303")
            .unwrap();
    let blob_path = storage.blob_path(&digest);
    let parent = blob_path.parent().expect("blob path parent");
    let grandparent = parent.parent().expect("blob path grandparent");
    std::fs::create_dir_all(grandparent).expect("create grandparent dirs");

    // parent is <root>/blobs/sha256/33; link it to outside
    std::os::unix::fs::symlink(&outside, parent).expect("create intermediate dir symlink");

    // Write target file in outside directory with name matching digest.hex()
    let target_file = outside.join(digest.hex());
    let content = b"intermediate directory symlink outside target";
    write_file(&target_file, content);

    // Legacy helper fs_metadata_size traverses intermediate dir symlink (historical baseline preserved)
    let size = fs_metadata_size(&blob_path)
        .await
        .expect("fs_metadata_size traverses intermediate dir symlink");
    assert_eq!(size, content.len() as u64);

    // Production head_blob rejects intermediate directory symlink
    let head_err = storage
        .head_blob(&digest)
        .await
        .expect_err("production head_blob must reject intermediate directory symlink");
    assert_eq!(
        head_err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io)
    );

    // Production open_blob rejects intermediate directory symlink
    let open_err = expect_open_blob_err(
        &storage,
        &digest,
        "production open_blob must reject intermediate directory symlink",
    )
    .await;
    assert_eq!(
        open_err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io)
    );
}

#[tokio::test]
#[cfg(unix)]
async fn test_fs_metadata_containment_dangling_symlink_falls_back_to_quarantine() {
    // Characterizes Case 4: Dangling ordinary blob symlink with a valid quarantine blob.
    // Under accepted Policy C: Dangling and ordinary primary symlinks suppress quarantine fallback.
    // Only genuine primary NotFound permits quarantine fallback.
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage_root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:4444444444444444444444444444444444444444444444444444444444444404")
            .unwrap();

    // Quarantine blob exists with valid content
    let quarantine_content = b"quarantine fallback for dangling symlink";
    write_file(&storage.quarantine_blob_path(&digest), quarantine_content);

    // Ordinary blob path is a dangling symlink to a non-existent file
    let blob_path = storage.blob_path(&digest);
    if let Some(parent) = blob_path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dirs");
    }
    let nonexistent_target = root.join("nonexistent_target.bin");
    std::os::unix::fs::symlink(&nonexistent_target, &blob_path).expect("create dangling symlink");

    // Direct fs_metadata_size on dangling symlink yields NotFound (historical baseline preserved)
    let io_err = fs_metadata_size(&blob_path)
        .await
        .expect_err("metadata on dangling symlink must fail");
    assert_eq!(io_err.kind(), std::io::ErrorKind::NotFound);

    // Production head_blob rejects dangling symlink via openat2 containment (ResolutionRejected),
    // strictly suppressing quarantine fallback and returning StorageErrorKind::Io.
    let head_err = storage
        .head_blob(&digest)
        .await
        .expect_err("production head_blob must suppress quarantine fallback on dangling symlink");
    assert_eq!(
        head_err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io),
        "dangling symlink containment rejection must map to StorageErrorKind::Io"
    );

    // Production open_blob also suppresses quarantine fallback on dangling symlink
    let open_err = expect_open_blob_err(
        &storage,
        &digest,
        "production open_blob must suppress quarantine fallback on dangling symlink",
    )
    .await;
    assert_eq!(
        open_err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io),
        "dangling symlink containment rejection must map to StorageErrorKind::Io"
    );
}

#[tokio::test]
#[cfg(unix)]
async fn test_fs_metadata_containment_quarantine_symlink_outside_root() {
    // Characterizes Case 5: Quarantine-path symlink to target outside configured root, ordinary path absent.
    // Under accepted Policy C: Ordinary lookup fails with NotFound (permitting quarantine fallback),
    // but quarantine lookup fails containment on the symlink, returning StorageErrorKind::Io.
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage_root");
    let outside = fixture.path().join("outside_quarantine");
    std::fs::create_dir_all(&root).expect("create storage root");
    std::fs::create_dir_all(&outside).expect("create outside dir");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:5555555555555555555555555555555555555555555555555555555555555505")
            .unwrap();

    // Ordinary blob is absent (does not exist)

    // Quarantine path is a symlink pointing outside root
    let qpath = storage.quarantine_blob_path(&digest);
    if let Some(parent) = qpath.parent() {
        std::fs::create_dir_all(parent).expect("create parent dirs");
    }
    let target_outside = outside.join("quarantine_target.bin");
    let outside_content = b"quarantine outside root payload";
    write_file(&target_outside, outside_content);
    std::os::unix::fs::symlink(&target_outside, &qpath).expect("create quarantine symlink");

    // Legacy helper fs_metadata_size follows quarantine symlink (historical baseline preserved)
    let size = fs_metadata_size(&qpath)
        .await
        .expect("fs_metadata_size follows quarantine symlink outside root");
    assert_eq!(size, outside_content.len() as u64);

    // Production head_blob falls back to quarantine on primary NotFound, then rejects quarantine symlink
    let head_err = storage
        .head_blob(&digest)
        .await
        .expect_err("production head_blob must reject quarantine symlink");
    assert_eq!(
        head_err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io)
    );

    // Production open_blob also rejects quarantine symlink
    let open_err = expect_open_blob_err(
        &storage,
        &digest,
        "production open_blob must reject quarantine symlink",
    )
    .await;
    assert_eq!(
        open_err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io)
    );
}

#[tokio::test]
#[cfg(unix)]
async fn test_fs_metadata_containment_storage_root_is_symlink() {
    // Characterizes Case 6: Configured storage root itself supplied through directory symlink.
    // Behavior: FsStorage initialization succeeds and metadata lookup succeeds through symlinked root.
    // Distinguishes root configuration policy from symlinks below the established root.
    let fixture = tempfile::tempdir().expect("create test fixture");
    let real_root = fixture.path().join("real_storage_root");
    let symlink_root = fixture.path().join("symlink_storage_root");
    std::fs::create_dir_all(&real_root).expect("create real storage root");
    std::os::unix::fs::symlink(&real_root, &symlink_root).expect("create symlink storage root");

    let storage = FsStorage::try_new(symlink_root.clone(), 1024 * 1024)
        .expect("FsStorage::try_new succeeds with symlinked root");

    let digest =
        Digest::parse("sha256:6666666666666666666666666666666666666666666666666666666666666606")
            .unwrap();
    let content = b"blob stored under symlinked root";
    write_file(&storage.blob_path(&digest), content);

    let blob_path = storage.blob_path(&digest);
    let size = fs_metadata_size(&blob_path)
        .await
        .expect("fs_metadata_size succeeds through symlinked root");
    assert_eq!(size, content.len() as u64);

    let meta = storage
        .head_blob(&digest)
        .await
        .expect("head_blob succeeds through symlinked root");
    assert_eq!(meta.size, content.len() as u64);

    let (payload_meta, mut stream) = storage
        .open_blob(&digest)
        .await
        .expect("open_blob succeeds through symlinked root");
    assert_eq!(payload_meta.size, content.len() as u64);

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read stream");
    assert_eq!(buf, content);
}

#[tokio::test]
async fn test_fs_metadata_containment_directory_blob_returns_metadata_size() {
    // Characterizes Case 7: Ordinary blob path resolves to a directory rather than a regular file.
    // Under accepted Policy C: Directories and other non-regular objects fail during acquisition
    // with UnsupportedObjectType -> StorageErrorKind::Io.
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage_root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:7777777777777777777777777777777777777777777777777777777777777707")
            .unwrap();
    let blob_path = storage.blob_path(&digest);
    std::fs::create_dir_all(&blob_path).expect("create directory at blob path");

    // Legacy helper fs_metadata_size returns directory metadata length (historical baseline preserved)
    let expected_dir_size = std::fs::metadata(&blob_path)
        .expect("query directory metadata")
        .len();

    let size = fs_metadata_size(&blob_path)
        .await
        .expect("fs_metadata_size returns directory size");
    assert_eq!(size, expected_dir_size);

    // Under Policy C: Production head_blob rejects directory blob during acquisition (UnsupportedObjectType)
    let head_err = storage
        .head_blob(&digest)
        .await
        .expect_err("production head_blob must reject directory blob");
    assert_eq!(
        head_err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io),
        "directory rejection must map to StorageErrorKind::Io"
    );

    // Production open_blob also rejects directory blob
    let open_err = expect_open_blob_err(
        &storage,
        &digest,
        "production open_blob must reject directory blob",
    )
    .await;
    assert_eq!(
        open_err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io),
        "directory rejection must map to StorageErrorKind::Io"
    );
}

// --------------------------------------------------------------------------------------------
// Focused Production Cutover Verification Tests (Policies A, B, and C)
// --------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_production_read_cutover_head_and_open_blob_execute_extracted_path() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage_root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:8888888888888888888888888888888888888888888888888888888888888808")
            .unwrap();
    let content = b"verified payload through extracted production cutover adapter";
    write_file(&storage.blob_path(&digest), content);

    // Verify head_blob executes extracted path and returns accurate metadata
    let meta = storage
        .head_blob(&digest)
        .await
        .expect("production head_blob succeeds on regular blob file");
    assert_eq!(meta.size, content.len() as u64);

    // Verify open_blob executes extracted path and streams identical payload bytes
    let (payload_meta, mut stream) = storage
        .open_blob(&digest)
        .await
        .expect("production open_blob succeeds on regular blob file");
    assert_eq!(payload_meta.size, content.len() as u64);

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read stream");
    assert_eq!(buf, content);

    // Verify underlying read_adapter points to the configured root
    assert_eq!(storage.read_adapter().reader().root_path(), &root);
}

#[tokio::test]
async fn test_production_read_cutover_quarantine_fallback_on_primary_not_found() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage_root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:9999999999999999999999999999999999999999999999999999999999999909")
            .unwrap();
    let quarantine_content = b"quarantine payload for genuine primary not found fallback";
    write_file(&storage.quarantine_blob_path(&digest), quarantine_content);

    // Primary is absent: head_blob falls back to quarantine
    let meta = storage
        .head_blob(&digest)
        .await
        .expect("head_blob falls back to quarantine on genuine primary NotFound");
    assert_eq!(meta.size, quarantine_content.len() as u64);

    // Primary is absent: open_blob falls back to quarantine and streams payload
    let (payload_meta, mut stream) = storage
        .open_blob(&digest)
        .await
        .expect("open_blob falls back to quarantine on genuine primary NotFound");
    assert_eq!(payload_meta.size, quarantine_content.len() as u64);

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read stream");
    assert_eq!(buf, quarantine_content);

    // When both primary and quarantine are absent, returns NotFound
    let missing_digest =
        Digest::parse("sha256:9999999999999999999999999999999999999999999999999999999999999999")
            .unwrap();
    let head_missing = storage.head_blob(&missing_digest).await;
    assert!(matches!(head_missing, Err(StorageError::NotFound)));

    let open_missing = storage.open_blob(&missing_digest).await;
    assert!(matches!(open_missing, Err(StorageError::NotFound)));
}

#[tokio::test]
#[cfg(unix)]
async fn test_production_read_cutover_primary_symlinks_suppress_quarantine_fallback() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage_root");
    let outside = fixture.path().join("outside_store");
    std::fs::create_dir_all(&root).expect("create storage root");
    std::fs::create_dir_all(&outside).expect("create outside store");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    // Case A: Ordinary symlink pointing to an existing file outside root
    let digest_symlink =
        Digest::parse("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa01")
            .unwrap();
    let outside_target = outside.join("target.bin");
    write_file(&outside_target, b"outside target payload");
    write_file(
        &storage.quarantine_blob_path(&digest_symlink),
        b"valid quarantine payload",
    );

    let blob_path_a = storage.blob_path(&digest_symlink);
    if let Some(parent) = blob_path_a.parent() {
        std::fs::create_dir_all(parent).expect("create parent dirs");
    }
    std::os::unix::fs::symlink(&outside_target, &blob_path_a).expect("create symlink");

    // Both head_blob and open_blob must reject the symlink and suppress quarantine fallback
    let head_err_a = storage
        .head_blob(&digest_symlink)
        .await
        .expect_err("head_blob must reject ordinary symlink and suppress quarantine fallback");
    assert_eq!(head_err_a.internal_kind(), Some(StorageErrorKind::Io));

    let open_err_a = expect_open_blob_err(
        &storage,
        &digest_symlink,
        "open_blob must reject ordinary symlink and suppress quarantine fallback",
    )
    .await;
    assert_eq!(open_err_a.internal_kind(), Some(StorageErrorKind::Io));

    // Case B: Dangling symlink pointing to a nonexistent file
    let digest_dangling =
        Digest::parse("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa02")
            .unwrap();
    write_file(
        &storage.quarantine_blob_path(&digest_dangling),
        b"valid quarantine payload for dangling case",
    );

    let blob_path_b = storage.blob_path(&digest_dangling);
    if let Some(parent) = blob_path_b.parent() {
        std::fs::create_dir_all(parent).expect("create parent dirs");
    }
    let nonexistent = root.join("nonexistent_path.bin");
    std::os::unix::fs::symlink(&nonexistent, &blob_path_b).expect("create dangling symlink");

    // Both head_blob and open_blob must reject the dangling symlink and suppress quarantine fallback
    let head_err_b = storage
        .head_blob(&digest_dangling)
        .await
        .expect_err("head_blob must reject dangling symlink and suppress quarantine fallback");
    assert_eq!(head_err_b.internal_kind(), Some(StorageErrorKind::Io));

    let open_err_b = expect_open_blob_err(
        &storage,
        &digest_dangling,
        "open_blob must reject dangling symlink and suppress quarantine fallback",
    )
    .await;
    assert_eq!(open_err_b.internal_kind(), Some(StorageErrorKind::Io));
}

#[tokio::test]
async fn test_production_read_cutover_nonregular_objects_rejected_during_acquisition() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage_root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb01")
            .unwrap();
    let blob_path = storage.blob_path(&digest);
    std::fs::create_dir_all(&blob_path).expect("create directory at blob path");

    // head_blob must reject directory blob during acquisition
    let head_err = storage
        .head_blob(&digest)
        .await
        .expect_err("head_blob must reject directory at blob path");
    assert_eq!(head_err.internal_kind(), Some(StorageErrorKind::Io));
    assert!(
        head_err.to_string().contains("unsupported object type"),
        "expected unsupported object type diagnostic, got: {head_err}"
    );

    // open_blob must reject directory blob during acquisition
    let open_err = expect_open_blob_err(
        &storage,
        &digest,
        "open_blob must reject directory at blob path",
    )
    .await;
    assert_eq!(open_err.internal_kind(), Some(StorageErrorKind::Io));
    assert!(
        open_err.to_string().contains("unsupported object type"),
        "expected unsupported object type diagnostic, got: {open_err}"
    );
}

#[tokio::test]
async fn test_production_read_cutover_both_methods_share_reader_and_root_ownership() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage_root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    // Verify that storage owns exactly one shared FsBlobCasReadAdapter
    let adapter_a = storage.read_adapter();
    let adapter_b = storage.read_adapter();
    assert!(std::sync::Arc::ptr_eq(adapter_a, adapter_b));

    // Verify that the adapter's reader has the exact root path
    assert_eq!(adapter_a.reader().root_path(), &root);
}

#[test]
fn test_production_read_cutover_startup_error_mapping_categories_and_diagnostics() {
    use crate::storage::fs::read_adapter::map_fs_startup_error;

    // 1. PlatformUnsupported -> StorageErrorKind::Configuration
    let err = map_fs_startup_error(storage_fs::FsMetadataError::PlatformUnsupported);
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::Configuration));
    assert!(err.to_string().contains("platform unsupported"));

    // 2. SyscallUnsupported -> StorageErrorKind::Configuration
    let err = map_fs_startup_error(storage_fs::FsMetadataError::SyscallUnsupported(
        std::io::Error::from_raw_os_error(libc::ENOSYS),
    ));
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::Configuration));
    assert!(err.to_string().contains("openat2 is unavailable"));

    // 3. EmptyRootPath -> StorageErrorKind::Configuration
    let err = map_fs_startup_error(storage_fs::FsMetadataError::EmptyRootPath);
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::Configuration));
    assert!(err.to_string().contains("cannot be empty"));

    // 4. NulInRootPath -> StorageErrorKind::Configuration
    let err = map_fs_startup_error(storage_fs::FsMetadataError::NulInRootPath);
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::Configuration));
    assert!(err.to_string().contains("embedded NUL byte"));

    // 5. UnsupportedObjectType -> StorageErrorKind::Configuration
    let err = map_fs_startup_error(storage_fs::FsMetadataError::UnsupportedObjectType {
        mode: libc::S_IFREG as u32 | 0o644,
    });
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::Configuration));
    assert!(err.to_string().contains("not a directory"));

    // 6. ProbeDenied -> StorageErrorKind::Backend
    let err = map_fs_startup_error(storage_fs::FsMetadataError::ProbeDenied(
        std::io::Error::from_raw_os_error(libc::EACCES),
    ));
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::Backend));
    assert!(err.to_string().contains("probe denied"));

    // 7. ProbeFailed -> StorageErrorKind::Backend
    let err = map_fs_startup_error(storage_fs::FsMetadataError::ProbeFailed {
        source: std::io::Error::from_raw_os_error(libc::EMFILE),
    });
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::Backend));
    assert!(err.to_string().contains("probe failed"));

    // 8. RootOpenFailed -> StorageErrorKind::Io
    let err = map_fs_startup_error(storage_fs::FsMetadataError::RootOpenFailed {
        source: std::io::Error::from_raw_os_error(libc::ENOENT),
    });
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::Io));
    assert!(err.to_string().contains("failed to open root directory"));

    // 9. Conservative fallback for unexpected/non-exhaustive variants -> StorageErrorKind::Backend
    let err = map_fs_startup_error(storage_fs::FsMetadataError::ResolutionRejected {
        raw_os_error: libc::ELOOP,
        source: std::io::Error::from_raw_os_error(libc::ELOOP),
    });
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::Backend));
    assert!(
        err.to_string()
            .contains("unexpected storage initialization failure")
    );
}

// --- CAS Blob Listing & Pagination Characterization Tests ---

fn put_cas_blob_file(root: &Path, hex: &str, content: &[u8]) {
    let p2 = &hex[0..2];
    let path = root.join("blobs").join("sha256").join(p2).join(hex);
    write_file(&path, content);
}

#[tokio::test]
async fn test_list_cas_blobs_missing_or_empty_root_returns_empty_page() {
    use crate::storage::GcStorage;

    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    // Case 1: Configured storage root exists, but CAS listing directory (blobs/sha256) is absent
    let page = storage
        .list_cas_blobs_page(None, 100)
        .await
        .expect("absent CAS listing directory returns empty page");
    assert!(page.items.is_empty());
    assert_eq!(page.next_cursor, None);

    // Case 2: blobs/sha256 exists but has no shard directories
    let cas_root = root.join("blobs").join("sha256");
    std::fs::create_dir_all(&cas_root).expect("create cas root");
    let page = storage
        .list_cas_blobs_page(None, 100)
        .await
        .expect("empty cas root returns empty page");
    assert!(page.items.is_empty());
    assert_eq!(page.next_cursor, None);

    // Case 3: blobs/sha256 contains an empty shard directory
    let empty_shard = cas_root.join("aa");
    std::fs::create_dir_all(&empty_shard).expect("create empty shard");
    let page = storage
        .list_cas_blobs_page(None, 100)
        .await
        .expect("empty shard returns empty page");
    assert!(page.items.is_empty());
    assert_eq!(page.next_cursor, None);

    drop(storage);
    drop(fixture);
}

#[tokio::test]
async fn test_list_cas_blobs_initial_metadata_error_not_suppressed() {
    use crate::storage::{GcStorage, StorageError, StorageErrorKind};

    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    // Make 'blobs' a regular file so contained openat2("blobs/sha256") fails with NotADirectory (ENOTDIR)
    let blobs_file = root.join("blobs");
    std::fs::write(&blobs_file, b"not-a-directory").expect("write blobs as file");

    // Under production cutover authorization, initial non-directory errors are NO LONGER
    // suppressed into an empty page; they fail closed with StorageErrorKind::CorruptData.
    let err = storage
        .list_cas_blobs_page(None, 100)
        .await
        .expect_err("initial non-directory error must fail closed with CorruptData");
    match err {
        StorageError::Internal { kind, .. } => {
            assert_eq!(
                kind,
                StorageErrorKind::CorruptData,
                "initial NotADirectory must map to CorruptData"
            );
        }
        other => panic!("expected StorageErrorKind::CorruptData, got {other:?}"),
    }

    drop(storage);
    drop(fixture);
}

#[tokio::test]
async fn test_list_cas_blobs_ordering_and_pagination_boundaries() {
    use crate::storage::GcStorage;

    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let hexes = [
        "0a00000000000000000000000000000000000000000000000000000000000001",
        "0a00000000000000000000000000000000000000000000000000000000000002",
        "1b00000000000000000000000000000000000000000000000000000000000001",
        "ff00000000000000000000000000000000000000000000000000000000000001",
        "ff00000000000000000000000000000000000000000000000000000000000002",
    ];
    for hex in &hexes {
        put_cas_blob_file(&root, hex, b"blob-payload");
    }

    // Page 1 with limit = 2
    let page1 = storage.list_cas_blobs_page(None, 2).await.expect("page 1");
    assert_eq!(page1.items.len(), 2);
    assert_eq!(page1.items[0].digest.hex(), hexes[0]);
    assert_eq!(page1.items[1].digest.hex(), hexes[1]);
    let expected_c1 = format!("sha256:{}", hexes[1]);
    assert_eq!(
        page1.next_cursor.as_ref().map(|c| c.0.as_str()),
        Some(expected_c1.as_str())
    );

    // Page 2 with cursor from Page 1 and limit = 2
    let page2 = storage
        .list_cas_blobs_page(page1.next_cursor.as_ref(), 2)
        .await
        .expect("page 2");
    assert_eq!(page2.items.len(), 2);
    assert_eq!(page2.items[0].digest.hex(), hexes[2]);
    assert_eq!(page2.items[1].digest.hex(), hexes[3]);
    let expected_c2 = format!("sha256:{}", hexes[3]);
    assert_eq!(
        page2.next_cursor.as_ref().map(|c| c.0.as_str()),
        Some(expected_c2.as_str())
    );

    // Page 3 with cursor from Page 2 and limit = 2 (final item)
    let page3 = storage
        .list_cas_blobs_page(page2.next_cursor.as_ref(), 2)
        .await
        .expect("page 3");
    assert_eq!(page3.items.len(), 1);
    assert_eq!(page3.items[0].digest.hex(), hexes[4]);
    // Since items.len() (1) < limit (2), next_cursor must be None
    assert_eq!(page3.next_cursor, None);

    // Calling with a cursor matching the final element yields an empty page and None
    let cursor_final = crate::storage::GcCursor(format!("sha256:{}", hexes[4]));
    let page_empty = storage
        .list_cas_blobs_page(Some(&cursor_final), 2)
        .await
        .expect("page empty");
    assert!(page_empty.items.is_empty());
    assert_eq!(page_empty.next_cursor, None);

    drop(storage);
    drop(fixture);
}

#[tokio::test]
async fn test_list_cas_blobs_exact_full_final_page() {
    use crate::storage::GcStorage;

    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let hexes = [
        "0a00000000000000000000000000000000000000000000000000000000000001",
        "0a00000000000000000000000000000000000000000000000000000000000002",
        "1b00000000000000000000000000000000000000000000000000000000000001",
        "1b00000000000000000000000000000000000000000000000000000000000002",
    ];
    for hex in &hexes {
        put_cas_blob_file(&root, hex, b"exact-page-payload");
    }

    // Limit = 2 with exactly 4 items (2 full pages):
    // Page 1: items 0 and 1
    let page1 = storage.list_cas_blobs_page(None, 2).await.expect("page 1");
    assert_eq!(page1.items.len(), 2);
    let c1 = page1.next_cursor.expect("page 1 cursor");

    // Page 2: items 2 and 3 (exact-full final page, items.len() == limit)
    let page2 = storage
        .list_cas_blobs_page(Some(&c1), 2)
        .await
        .expect("page 2");
    assert_eq!(page2.items.len(), 2);
    assert_eq!(page2.items[0].digest.hex(), hexes[2]);
    assert_eq!(page2.items[1].digest.hex(), hexes[3]);
    // Since items.len() == limit, list_cas_blobs_page cannot know it was the final object; returns cursor:
    let c2 = page2
        .next_cursor
        .expect("exact-full page must return cursor");
    assert_eq!(c2.0, format!("sha256:{}", hexes[3]));

    // Page 3: subsequent query with c2 returns empty terminal page with next_cursor None
    let page3 = storage
        .list_cas_blobs_page(Some(&c2), 2)
        .await
        .expect("page 3 terminal");
    assert!(page3.items.is_empty(), "terminal page must be empty");
    assert_eq!(page3.next_cursor, None, "terminal page must have no cursor");

    drop(storage);
    drop(fixture);
}

#[tokio::test]
async fn test_list_cas_blobs_limit_clamping_zero_and_large_fixture() {
    use crate::storage::GcStorage;

    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    // Populate exactly 1,001 distinct valid CAS objects across shards:
    // Shards: "00" (items 0..499), "01" (items 500..999), "02" (item 1000)
    let total_objects = 1001;
    let mut hexes = Vec::with_capacity(total_objects);
    for i in 0..total_objects {
        let p2 = format!("{:02x}", i / 500);
        let hex = format!("{}{:062x}", p2, i);
        put_cas_blob_file(&root, &hex, b"large-fixture-payload");
        hexes.push(hex);
    }
    hexes.sort();

    // 1. Zero-limit assertion: limit 0 is clamped to 1
    let page_zero = storage
        .list_cas_blobs_page(None, 0)
        .await
        .expect("limit 0 clamped to 1");
    assert_eq!(page_zero.items.len(), 1);
    assert_eq!(page_zero.items[0].digest.hex(), hexes[0]);
    assert_eq!(
        page_zero.next_cursor.as_ref().map(|c| c.0.as_str()),
        Some(format!("sha256:{}", hexes[0]).as_str())
    );

    // 2. Upper-limit assertion: request limit 50,000, clamped to 1,000
    let page1 = storage
        .list_cas_blobs_page(None, 50_000)
        .await
        .expect("request limit 50000");
    assert_eq!(
        page1.items.len(),
        1000,
        "upper limit must be clamped to exactly 1000 items"
    );
    assert_eq!(page1.items[0].digest.hex(), hexes[0]);
    assert_eq!(page1.items[999].digest.hex(), hexes[999]);
    let c1 = page1
        .next_cursor
        .expect("page 1 of large fixture must return next cursor");
    assert_eq!(c1.0, format!("sha256:{}", hexes[999]));

    // Page 2: fetches the 1,001st item
    let page2 = storage
        .list_cas_blobs_page(Some(&c1), 50_000)
        .await
        .expect("page 2 of large fixture");
    assert_eq!(
        page2.items.len(),
        1,
        "remaining item must be returned on page 2"
    );
    assert_eq!(page2.items[0].digest.hex(), hexes[1000]);
    assert_eq!(
        page2.next_cursor, None,
        "page 2 has fewer than limit items; next_cursor must be None"
    );

    drop(storage);
    drop(fixture);
}

#[tokio::test]
async fn test_list_cas_blobs_cursor_lexical_filtering_and_malformed_values() {
    use crate::storage::GcStorage;

    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let hex1 = "0a00000000000000000000000000000000000000000000000000000000000001";
    let hex2 = "1b00000000000000000000000000000000000000000000000000000000000001";
    put_cas_blob_file(&root, hex1, b"item1");
    put_cas_blob_file(&root, hex2, b"item2");

    // Cursor "zzz": all valid sha256 digests are lexicographically <= "zzz", so all are skipped
    let cursor_zzz = crate::storage::GcCursor("zzz".to_string());
    let page_zzz = storage
        .list_cas_blobs_page(Some(&cursor_zzz), 10)
        .await
        .expect("cursor zzz");
    assert!(page_zzz.items.is_empty());
    assert_eq!(page_zzz.next_cursor, None);

    // Cursor "aaa": all valid sha256 digests are lexicographically > "aaa", so none are skipped
    let cursor_aaa = crate::storage::GcCursor("aaa".to_string());
    let page_aaa = storage
        .list_cas_blobs_page(Some(&cursor_aaa), 10)
        .await
        .expect("cursor aaa");
    assert_eq!(page_aaa.items.len(), 2);

    // Cursor lexicographically between item1 and item2 but malformed (non-digest string)
    let cursor_mid = crate::storage::GcCursor("sha256:0a_synthetic_middle_marker".to_string());
    let page_mid = storage
        .list_cas_blobs_page(Some(&cursor_mid), 10)
        .await
        .expect("cursor mid");
    assert_eq!(page_mid.items.len(), 1);
    assert_eq!(page_mid.items[0].digest.hex(), hex2);

    drop(storage);
    drop(fixture);
}

#[tokio::test]
async fn test_list_cas_blobs_fails_closed_on_symlinks() {
    use crate::storage::GcStorage;
    use std::os::unix::fs::symlink;

    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let cas_root = root.join("blobs").join("sha256");
    std::fs::create_dir_all(&cas_root).expect("create cas root");

    // Case 1: Symlinked shard directory (e.g. blobs/sha256/2b -> target_dir)
    // DirEntry::file_type().is_dir() returns false for a symlink, triggering CorruptData
    let target_shard = fixture.path().join("external_shard");
    std::fs::create_dir_all(&target_shard).expect("create target shard");
    let link_shard = cas_root.join("2b");
    symlink(&target_shard, &link_shard).expect("create shard symlink");

    let res = storage.list_cas_blobs_page(None, 10).await;
    assert!(res.is_err(), "symlinked shard directory must fail closed");
    let err = res.unwrap_err();
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::CorruptData));
    assert!(
        err.to_string()
            .contains("malformed non-directory entry in CAS prefix directory root"),
        "error must report non-directory entry in CAS prefix directory root: got {err}"
    );

    // Clean up shard symlink to test file symlink inside a valid shard
    std::fs::remove_file(&link_shard).expect("remove shard symlink");

    // Case 2: Symlinked blob file inside a valid shard directory
    // DirEntry::file_type().is_file() returns false for a symlink, triggering CorruptData
    let real_shard = cas_root.join("0a");
    std::fs::create_dir_all(&real_shard).expect("create real shard");
    let target_file = fixture.path().join("target_blob.bin");
    std::fs::write(&target_file, b"symlinked blob data").expect("write target blob");
    let valid_hex_name = "0a00000000000000000000000000000000000000000000000000000000000001";
    let link_file = real_shard.join(valid_hex_name);
    symlink(&target_file, &link_file).expect("create blob symlink");

    let res = storage.list_cas_blobs_page(None, 10).await;
    assert!(res.is_err(), "symlinked blob file must fail closed");
    let err = res.unwrap_err();
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::CorruptData));
    assert!(
        err.to_string()
            .contains("malformed non-file entry in CAS shard directory"),
        "error must report non-file entry in CAS shard directory: got {err}"
    );

    drop(storage);
    drop(fixture);
}

#[tokio::test]
async fn test_list_cas_blobs_fails_closed_on_nested_subdirectories_in_shard() {
    use crate::storage::GcStorage;

    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let shard = root.join("blobs").join("sha256").join("0a");
    let nested_subdir = shard.join("nested_subdir");
    std::fs::create_dir_all(&nested_subdir).expect("create nested subdir in shard");

    let res = storage.list_cas_blobs_page(None, 10).await;
    assert!(
        res.is_err(),
        "nested directory inside shard must fail closed"
    );
    let err = res.unwrap_err();
    assert_eq!(err.internal_kind(), Some(StorageErrorKind::CorruptData));
    assert!(
        err.to_string()
            .contains("malformed non-file entry in CAS shard directory"),
        "nested directory in shard must be rejected as non-file entry: got {err}"
    );

    drop(storage);
    drop(fixture);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_list_cas_blobs_symlink_resolution_through_ancestor_paths() {
    use crate::storage::GcStorage;
    use std::os::unix::fs::symlink;

    let fixture = tempfile::tempdir().expect("create test fixture");
    let hex = "0a00000000000000000000000000000000000000000000000000000000000001";

    // Case A: Configured storage root itself is a symlink pointing to another directory
    {
        let target_root = fixture.path().join("target_root_a");
        put_cas_blob_file(&target_root, hex, b"payload-root-symlink");
        let sym_root = fixture.path().join("sym_root_a");
        symlink(&target_root, &sym_root).expect("symlink storage root");

        let storage = FsStorage::new(sym_root, 1024 * 1024);
        let page = storage
            .list_cas_blobs_page(None, 10)
            .await
            .expect("listing through symlinked storage root succeeds");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].digest.hex(), hex);
        drop(storage);
    }

    // Case B: 'blobs' directory is a symlink pointing to an external directory
    // Under descriptor containment (openat2 with RESOLVE_NO_SYMLINKS), symlinks beneath
    // the root descriptor are rejected with StorageErrorKind::Io.
    {
        let root_b = fixture.path().join("root_b");
        std::fs::create_dir_all(&root_b).expect("create root_b");
        let target_blobs = fixture.path().join("target_blobs_b");
        let target_cas = target_blobs.join("sha256");
        let target_shard = target_cas.join("0a");
        std::fs::create_dir_all(&target_shard).expect("create target shard");
        std::fs::write(target_shard.join(hex), b"payload-blobs-symlink").expect("write blob");
        symlink(&target_blobs, root_b.join("blobs")).expect("symlink blobs dir");

        let storage = FsStorage::new(root_b, 1024 * 1024);
        let err = storage
            .list_cas_blobs_page(None, 10)
            .await
            .expect_err("listing through symlinked blobs dir must fail closed with Io");
        match err {
            crate::storage::StorageError::Internal { kind, .. } => {
                assert_eq!(kind, crate::storage::StorageErrorKind::Io);
            }
            other => panic!("expected StorageErrorKind::Io, got {other:?}"),
        }
        drop(storage);
    }

    // Case C: 'blobs/sha256' is a symlink pointing to an external CAS root
    // Under descriptor containment (openat2 with RESOLVE_NO_SYMLINKS), symlinks beneath
    // the root descriptor are rejected with StorageErrorKind::Io.
    {
        let root_c = fixture.path().join("root_c");
        let blobs_dir = root_c.join("blobs");
        std::fs::create_dir_all(&blobs_dir).expect("create root_c/blobs");
        let target_cas_c = fixture.path().join("target_cas_c");
        let target_shard_c = target_cas_c.join("0a");
        std::fs::create_dir_all(&target_shard_c).expect("create target shard c");
        std::fs::write(target_shard_c.join(hex), b"payload-cas-symlink").expect("write blob");
        symlink(&target_cas_c, blobs_dir.join("sha256")).expect("symlink blobs/sha256 dir");

        let storage = FsStorage::new(root_c, 1024 * 1024);
        let err = storage
            .list_cas_blobs_page(None, 10)
            .await
            .expect_err("listing through symlinked blobs/sha256 dir must fail closed with Io");
        match err {
            crate::storage::StorageError::Internal { kind, .. } => {
                assert_eq!(kind, crate::storage::StorageErrorKind::Io);
            }
            other => panic!("expected StorageErrorKind::Io, got {other:?}"),
        }
        drop(storage);
    }

    drop(fixture);
}

#[tokio::test]
async fn test_list_cas_blobs_deterministic_inter_page_mutation_no_snapshot() {
    use crate::storage::GcStorage;

    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let hex_0a = "0a00000000000000000000000000000000000000000000000000000000000001";
    let hex_ff = "ff00000000000000000000000000000000000000000000000000000000000001";
    put_cas_blob_file(&root, hex_0a, b"first");
    put_cas_blob_file(&root, hex_ff, b"last");

    // Fetch page 1 (limit 1): returns 0a
    let page1 = storage.list_cas_blobs_page(None, 1).await.expect("page 1");
    assert_eq!(page1.items.len(), 1);
    assert_eq!(page1.items[0].digest.hex(), hex_0a);
    let cursor1 = page1.next_cursor.expect("cursor after page 1");

    // Deterministic mutation between page calls (characterizing absence of snapshot isolation):
    // 1. Insert a blob behind the cursor (hex_01 < hex_0a)
    let hex_01 = "0100000000000000000000000000000000000000000000000000000000000001";
    put_cas_blob_file(&root, hex_01, b"behind-cursor");

    // 2. Insert a blob ahead of the cursor (hex_88 between 0a and ff)
    let hex_88 = "8800000000000000000000000000000000000000000000000000000000000001";
    put_cas_blob_file(&root, hex_88, b"ahead-of-cursor");

    // Fetch page 2 (cursor = cursor1, limit = 10):
    // hex_01 is skipped because "sha256:01..." <= "sha256:0a..." (missed by this traversal cycle)
    // hex_88 is observed because "sha256:88..." > "sha256:0a..."
    // hex_ff is observed because "sha256:ff..." > "sha256:0a..."
    let page2 = storage
        .list_cas_blobs_page(Some(&cursor1), 10)
        .await
        .expect("page 2");
    assert_eq!(page2.items.len(), 2);
    assert_eq!(page2.items[0].digest.hex(), hex_88);
    assert_eq!(page2.items[1].digest.hex(), hex_ff);

    drop(storage);
    drop(fixture);
}

#[tokio::test]
async fn test_shared_reader_allocation_pointer_equality() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    // Verify byte-for-byte that FsStorage.reader and read_adapter share the exact same Arc
    assert!(
        std::sync::Arc::ptr_eq(storage.reader(), storage.read_adapter().reader()),
        "FsStorage.reader and read_adapter must share the identical Arc<FsMetadataReader>"
    );

    drop(storage);
    drop(fixture);
}

#[tokio::test]
async fn test_fs_storage_list_cas_blobs_page_production_delegation() {
    use crate::storage::GcStorage;

    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    let hex = "0a00000000000000000000000000000000000000000000000000000000000001";
    put_cas_blob_file(&root, hex, b"production-delegation-payload");

    let storage = FsStorage::new(root.clone(), 1024 * 1024);
    let page = storage
        .list_cas_blobs_page(None, 10)
        .await
        .expect("production listing must succeed");
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].digest.hex(), hex);
    assert_eq!(
        page.items[0].size,
        b"production-delegation-payload".len() as u64
    );
    assert_eq!(page.next_cursor, None);

    drop(storage);
    drop(fixture);
}

#[tokio::test]
async fn test_fs_storage_list_cas_blobs_page_custom_budgets_exhaustion() {
    use crate::storage::{StorageError, StorageErrorKind};
    use storage_fs::DirEnumerationLimits;

    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    put_cas_blob_file(
        &root,
        "0a00000000000000000000000000000000000000000000000000000000000001",
        b"blob 1",
    );
    put_cas_blob_file(
        &root,
        "0b00000000000000000000000000000000000000000000000000000000000002",
        b"blob 2",
    );

    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    // Root budget max_entries = 1, but there are 2 shards (0a and 0b)
    let small_root_budget = super::listing::FsListingBudgets::new(
        DirEnumerationLimits::new(1, 1024),
        DirEnumerationLimits::new(100, 1024 * 1024),
    );

    let err = storage
        .list_cas_blobs_page_with_budgets(None, 10, small_root_budget)
        .await
        .expect_err("exceeding root budget must fail closed with Backend");
    match err {
        StorageError::Internal { kind, message } => {
            assert_eq!(kind, StorageErrorKind::Backend);
            assert!(message.contains("enumeration resource limit exceeded"));
        }
        other => panic!("expected Backend, got {other:?}"),
    }

    drop(storage);
    drop(fixture);
}

#[tokio::test]
async fn test_cas_blob_traverser_over_real_fs_storage() {
    use crate::blob_gc::traverser::CasBlobTraverser;

    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");

    let hexes = [
        "0a00000000000000000000000000000000000000000000000000000000000001",
        "0a00000000000000000000000000000000000000000000000000000000000002",
        "0b00000000000000000000000000000000000000000000000000000000000001",
        "0c00000000000000000000000000000000000000000000000000000000000001",
        "0c00000000000000000000000000000000000000000000000000000000000002",
    ];

    for hex in &hexes {
        put_cas_blob_file(&root, hex, hex.as_bytes());
    }

    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    // Run CasBlobTraverser with batch size 2 over production FsStorage
    let mut traverser = CasBlobTraverser::new(&storage, 2);

    let batch1 = traverser.next_batch().await.unwrap().expect("batch 1");
    assert_eq!(batch1.len(), 2);
    assert_eq!(batch1[0].digest.hex(), hexes[0]);
    assert_eq!(batch1[1].digest.hex(), hexes[1]);

    let batch2 = traverser.next_batch().await.unwrap().expect("batch 2");
    assert_eq!(batch2.len(), 2);
    assert_eq!(batch2[0].digest.hex(), hexes[2]);
    assert_eq!(batch2[1].digest.hex(), hexes[3]);

    let batch3 = traverser.next_batch().await.unwrap().expect("batch 3");
    assert_eq!(batch3.len(), 1);
    assert_eq!(batch3[0].digest.hex(), hexes[4]);

    let batch4 = traverser.next_batch().await.unwrap();
    assert!(
        batch4.is_none(),
        "traversal must terminate at end of repository"
    );

    drop(storage);
    drop(fixture);
}

// --- Filesystem Manifest Read Characterization Tests (head_manifest & get_manifest) ---

fn put_manifest_file(root: &Path, repo: &str, hex: &str, content: &[u8]) {
    let path = root.join("repos").join(repo).join("manifests").join(hex);
    write_file(&path, content);
}

#[tokio::test]
async fn test_manifest_read_representative_valid_oci_manifest() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let repo = "testrepo";
    let manifest_bytes = br#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "size": 0
        },
        "layers": []
    }"#;
    let hex = hex_sha256(manifest_bytes);
    let digest = Digest::parse(&format!("sha256:{hex}")).expect("valid digest");
    put_manifest_file(&root, repo, &hex, manifest_bytes);

    // 1. Characterize head_manifest via Storage
    let meta = storage
        .head_manifest(repo, &digest)
        .await
        .expect("head_manifest must succeed for valid manifest");
    assert_eq!(meta.size, manifest_bytes.len() as u64);
    assert_eq!(
        meta.media_type,
        "application/vnd.oci.image.manifest.v1+json"
    );

    // 2. Characterize get_manifest via Storage
    let (get_meta, payload) = storage
        .get_manifest(repo, &digest)
        .await
        .expect("get_manifest must succeed for valid manifest");
    assert_eq!(get_meta.size, manifest_bytes.len() as u64);
    assert_eq!(
        get_meta.media_type,
        "application/vnd.oci.image.manifest.v1+json"
    );
    assert_eq!(get_meta, meta);
    assert_eq!(payload.as_ref(), manifest_bytes);
    assert_eq!(payload.len() as u64, get_meta.size);

    // 3. Characterize identical behavior through ManifestReader port trait
    let port_meta = <FsStorage as crate::storage::ports::ManifestReader>::head_manifest(
        &storage, repo, &digest,
    )
    .await
    .expect("ManifestReader::head_manifest must succeed");
    assert_eq!(port_meta, meta);

    let (port_get_meta, port_payload) =
        <FsStorage as crate::storage::ports::ManifestReader>::get_manifest(&storage, repo, &digest)
            .await
            .expect("ManifestReader::get_manifest must succeed");
    assert_eq!(port_get_meta, meta);
    assert_eq!(port_payload, payload);
}

#[tokio::test]
async fn test_manifest_read_media_type_detection_variants() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);
    let repo = "testrepo";

    // Variant 1: Explicit custom mediaType string
    let custom_json =
        br#"{"schemaVersion": 2, "mediaType": "application/vnd.custom.manifest.v1+json"}"#;
    let hex1 = hex_sha256(custom_json);
    let d1 = Digest::parse(&format!("sha256:{hex1}")).expect("digest 1");
    put_manifest_file(&root, repo, &hex1, custom_json);

    let meta1 = storage.head_manifest(repo, &d1).await.unwrap();
    assert_eq!(meta1.media_type, "application/vnd.custom.manifest.v1+json");
    let (get_meta1, _) = storage.get_manifest(repo, &d1).await.unwrap();
    assert_eq!(
        get_meta1.media_type,
        "application/vnd.custom.manifest.v1+json"
    );

    // Variant 2: Missing mediaType field in valid JSON object -> falls back to OCI manifest default
    let missing_media_json = br#"{"schemaVersion": 2, "layers": []}"#;
    let hex2 = hex_sha256(missing_media_json);
    let d2 = Digest::parse(&format!("sha256:{hex2}")).expect("digest 2");
    put_manifest_file(&root, repo, &hex2, missing_media_json);

    let meta2 = storage.head_manifest(repo, &d2).await.unwrap();
    assert_eq!(
        meta2.media_type,
        "application/vnd.oci.image.manifest.v1+json"
    );
    let (get_meta2, _) = storage.get_manifest(repo, &d2).await.unwrap();
    assert_eq!(
        get_meta2.media_type,
        "application/vnd.oci.image.manifest.v1+json"
    );

    // Variant 3: Non-string mediaType value (e.g. integer 42) -> falls back to OCI default
    let non_string_json = br#"{"schemaVersion": 2, "mediaType": 42}"#;
    let hex3 = hex_sha256(non_string_json);
    let d3 = Digest::parse(&format!("sha256:{hex3}")).expect("digest 3");
    put_manifest_file(&root, repo, &hex3, non_string_json);

    let meta3 = storage.head_manifest(repo, &d3).await.unwrap();
    assert_eq!(
        meta3.media_type,
        "application/vnd.oci.image.manifest.v1+json"
    );
    let (get_meta3, _) = storage.get_manifest(repo, &d3).await.unwrap();
    assert_eq!(
        get_meta3.media_type,
        "application/vnd.oci.image.manifest.v1+json"
    );

    // Variant 4: Valid JSON but not an object (e.g. top-level string or array) -> falls back to OCI default without error
    let scalar_json = br#""just a json string""#;
    let hex4 = hex_sha256(scalar_json);
    let d4 = Digest::parse(&format!("sha256:{hex4}")).expect("digest 4");
    put_manifest_file(&root, repo, &hex4, scalar_json);

    let meta4 = storage.head_manifest(repo, &d4).await.unwrap();
    assert_eq!(
        meta4.media_type,
        "application/vnd.oci.image.manifest.v1+json"
    );
    let (get_meta4, payload4) = storage.get_manifest(repo, &d4).await.unwrap();
    assert_eq!(
        get_meta4.media_type,
        "application/vnd.oci.image.manifest.v1+json"
    );
    assert_eq!(payload4.as_ref(), scalar_json);
}

#[tokio::test]
async fn test_manifest_read_empty_and_malformed_payloads_classify_as_corrupt_data() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);
    let repo = "testrepo";

    // Case 1: Empty file (0 bytes) -> serde_json EOF error classified as CorruptData
    let empty_bytes = b"";
    let hex_empty = hex_sha256(empty_bytes);
    let d_empty = Digest::parse(&format!("sha256:{hex_empty}")).expect("digest empty");
    put_manifest_file(&root, repo, &hex_empty, empty_bytes);

    let head_err1 = storage.head_manifest(repo, &d_empty).await.unwrap_err();
    match head_err1 {
        StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::CorruptData),
        other => panic!("expected StorageErrorKind::CorruptData for empty payload, got: {other:?}"),
    }

    let get_err1 = storage.get_manifest(repo, &d_empty).await.unwrap_err();
    match get_err1 {
        StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::CorruptData),
        other => panic!("expected StorageErrorKind::CorruptData for empty payload, got: {other:?}"),
    }

    // Case 2: Malformed non-JSON payload -> classified as CorruptData
    let malformed_bytes = b"<html><head><title>502 Bad Gateway</title></head></html>";
    let hex_malformed = hex_sha256(malformed_bytes);
    let d_malformed = Digest::parse(&format!("sha256:{hex_malformed}")).expect("digest malformed");
    put_manifest_file(&root, repo, &hex_malformed, malformed_bytes);

    let head_err2 = storage.head_manifest(repo, &d_malformed).await.unwrap_err();
    match head_err2 {
        StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::CorruptData),
        other => {
            panic!("expected StorageErrorKind::CorruptData for malformed json, got: {other:?}")
        }
    }

    let get_err2 = storage.get_manifest(repo, &d_malformed).await.unwrap_err();
    match get_err2 {
        StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::CorruptData),
        other => {
            panic!("expected StorageErrorKind::CorruptData for malformed json, got: {other:?}")
        }
    }
}

#[tokio::test]
async fn test_manifest_read_missing_paths_return_not_found() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
            .expect("valid digest");

    // Case 1: Entire repository directory absent
    let err_missing_repo_head = storage
        .head_manifest("nonexistent_repo", &digest)
        .await
        .unwrap_err();
    assert!(matches!(err_missing_repo_head, StorageError::NotFound));

    let err_missing_repo_get = storage
        .get_manifest("nonexistent_repo", &digest)
        .await
        .unwrap_err();
    assert!(matches!(err_missing_repo_get, StorageError::NotFound));

    // Case 2: Repository directory exists, but manifests/ subdirectory is absent
    let repo_dir = root.join("repos").join("existing_repo");
    std::fs::create_dir_all(&repo_dir).expect("create repo dir");

    let err_missing_manifests_head = storage
        .head_manifest("existing_repo", &digest)
        .await
        .unwrap_err();
    assert!(matches!(err_missing_manifests_head, StorageError::NotFound));

    let err_missing_manifests_get = storage
        .get_manifest("existing_repo", &digest)
        .await
        .unwrap_err();
    assert!(matches!(err_missing_manifests_get, StorageError::NotFound));

    // Case 3: manifests/ directory exists, but the manifest digest file is absent
    let manifests_dir = repo_dir.join("manifests");
    std::fs::create_dir_all(&manifests_dir).expect("create manifests dir");

    let err_missing_file_head = storage
        .head_manifest("existing_repo", &digest)
        .await
        .unwrap_err();
    assert!(matches!(err_missing_file_head, StorageError::NotFound));

    let err_missing_file_get = storage
        .get_manifest("existing_repo", &digest)
        .await
        .unwrap_err();
    assert!(matches!(err_missing_file_get, StorageError::NotFound));
}

#[tokio::test]
async fn test_manifest_read_nondirectory_components_return_io() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
            .expect("valid digest");

    // Case 1: Repository component is a regular file instead of a directory
    let repos_dir = root.join("repos");
    std::fs::create_dir_all(&repos_dir).expect("create repos dir");
    let repo_file = repos_dir.join("file_repo");
    write_file(&repo_file, b"not a dir");

    let err_repo_file_head = storage
        .head_manifest("file_repo", &digest)
        .await
        .unwrap_err();
    match err_repo_file_head {
        StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
        other => panic!("expected StorageErrorKind::Io for ENOTDIR on repo, got: {other:?}"),
    }
    let err_repo_file_get = storage
        .get_manifest("file_repo", &digest)
        .await
        .unwrap_err();
    match err_repo_file_get {
        StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
        other => panic!("expected StorageErrorKind::Io for ENOTDIR on repo, got: {other:?}"),
    }

    // Case 2: manifests component is a regular file instead of a directory
    let repo2_dir = repos_dir.join("repo_with_file_manifests");
    std::fs::create_dir_all(&repo2_dir).expect("create repo2 dir");
    let manifests_file = repo2_dir.join("manifests");
    write_file(&manifests_file, b"not a dir");

    let err_manifests_file_head = storage
        .head_manifest("repo_with_file_manifests", &digest)
        .await
        .unwrap_err();
    match err_manifests_file_head {
        StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
        other => {
            panic!("expected StorageErrorKind::Io for ENOTDIR on manifests dir, got: {other:?}")
        }
    }
    let err_manifests_file_get = storage
        .get_manifest("repo_with_file_manifests", &digest)
        .await
        .unwrap_err();
    match err_manifests_file_get {
        StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
        other => {
            panic!("expected StorageErrorKind::Io for ENOTDIR on manifests dir, got: {other:?}")
        }
    }

    // Case 3: Target manifest path is a directory instead of a regular file
    let repo3_manifests = repos_dir.join("repo3").join("manifests");
    std::fs::create_dir_all(&repo3_manifests).expect("create repo3 manifests dir");
    let target_as_dir = repo3_manifests.join(digest.hex());
    std::fs::create_dir_all(&target_as_dir).expect("create dir at manifest path");

    let err_target_dir_head = storage.head_manifest("repo3", &digest).await.unwrap_err();
    match err_target_dir_head {
        StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
        other => panic!("expected StorageErrorKind::Io for directory-as-manifest, got: {other:?}"),
    }
    let err_target_dir_get = storage.get_manifest("repo3", &digest).await.unwrap_err();
    match err_target_dir_get {
        StorageError::Internal { kind, .. } => assert_eq!(kind, StorageErrorKind::Io),
        other => panic!("expected StorageErrorKind::Io for directory-as-manifest, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_manifest_read_repository_naming_single_and_multisegment() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let manifest_bytes =
        br#"{"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json"}"#;
    let hex = hex_sha256(manifest_bytes);
    let digest = Digest::parse(&format!("sha256:{hex}")).expect("valid digest");

    let test_repos = [
        "alpine",
        "library/ubuntu",
        "org/team/sub/service",
        "a/b/c/d/e",
    ];

    for repo in &test_repos {
        put_manifest_file(&root, repo, &hex, manifest_bytes);

        // Verify head_manifest and get_manifest succeed
        let head_res = storage.head_manifest(repo, &digest).await;
        assert!(
            head_res.is_ok(),
            "head_manifest failed for repo '{repo}': {head_res:?}"
        );

        let get_res = storage.get_manifest(repo, &digest).await;
        assert!(
            get_res.is_ok(),
            "get_manifest failed for repo '{repo}': {get_res:?}"
        );

        // Compare with proposed relative ObjectKey representation
        let key_str = format!("repos/{repo}/manifests/{hex}");
        let obj_key = storage_core::ObjectKey::parse(&key_str);
        assert!(
            obj_key.is_ok(),
            "ObjectKey::parse failed for '{key_str}': {obj_key:?}"
        );
        assert_eq!(obj_key.unwrap().as_str(), key_str);
    }
}

#[tokio::test]
async fn test_manifest_read_supported_digest_algorithms_and_filename_forms() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);
    let repo = "algo_repo";

    let manifest_bytes =
        br#"{"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json"}"#;

    // 1. SHA-256 (64-char hex)
    let hex256 = hex_sha256(manifest_bytes);
    assert_eq!(hex256.len(), 64);
    let d256 = Digest::parse(&format!("sha256:{hex256}")).expect("valid sha256");
    put_manifest_file(&root, repo, &hex256, manifest_bytes);

    let head256 = storage
        .head_manifest(repo, &d256)
        .await
        .expect("head sha256");
    assert_eq!(head256.size, manifest_bytes.len() as u64);
    let (get256, _) = storage.get_manifest(repo, &d256).await.expect("get sha256");
    assert_eq!(get256.size, manifest_bytes.len() as u64);

    // 2. SHA-512 (128-char hex)
    use sha2::Digest as ShaDigest;
    let mut hasher512 = sha2::Sha512::new();
    hasher512.update(manifest_bytes);
    let hex512 = hex::encode(hasher512.finalize());
    assert_eq!(hex512.len(), 128);
    let d512 = Digest::parse(&format!("sha512:{hex512}")).expect("valid sha512");
    put_manifest_file(&root, repo, &hex512, manifest_bytes);

    let head512 = storage
        .head_manifest(repo, &d512)
        .await
        .expect("head sha512");
    assert_eq!(head512.size, manifest_bytes.len() as u64);
    let (get512, _) = storage.get_manifest(repo, &d512).await.expect("get sha512");
    assert_eq!(get512.size, manifest_bytes.len() as u64);

    // Filename form invariant: raw hex string in manifests/ directory without algorithm prefix
    let path256 = root
        .join("repos")
        .join(repo)
        .join("manifests")
        .join(&hex256);
    let path512 = root
        .join("repos")
        .join(repo)
        .join("manifests")
        .join(&hex512);
    assert!(path256.is_file());
    assert!(path512.is_file());
}

#[tokio::test]
async fn test_manifest_read_unvalidated_caller_path_traversal_gap() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let manifest_bytes =
        br#"{"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json"}"#;
    let hex = hex_sha256(manifest_bytes);
    let digest = Digest::parse(&format!("sha256:{hex}")).expect("valid digest");

    // Construct a path that escapes root/repos via dot-dot traversal
    let repos_dir = root.join("repos");
    std::fs::create_dir_all(&repos_dir).expect("create repos dir");
    let escaped_target_dir = fixture.path().join("escaped_repo").join("manifests");
    std::fs::create_dir_all(&escaped_target_dir).expect("create escaped dir");
    let escaped_file = escaped_target_dir.join(&hex);
    write_file(&escaped_file, manifest_bytes);

    let traversal_repo_input = "../../escaped_repo";

    // Contained production behavior: manifest_key strictly rejects dot-dot segments with InvalidRepoName
    let head_res = storage.head_manifest(traversal_repo_input, &digest).await;
    assert!(
        matches!(head_res, Err(StorageError::InvalidRepoName(_))),
        "head_manifest must reject '..' traversal with InvalidRepoName: got {head_res:?}"
    );

    let get_res = storage.get_manifest(traversal_repo_input, &digest).await;
    assert!(
        matches!(get_res, Err(StorageError::InvalidRepoName(_))),
        "get_manifest must reject '..' traversal with InvalidRepoName: got {get_res:?}"
    );

    // Also verify that ManifestReader port forwarding rejects '..' traversal with InvalidRepoName
    let port_head_res = <FsStorage as crate::storage::ports::ManifestReader>::head_manifest(
        &storage,
        traversal_repo_input,
        &digest,
    )
    .await;
    assert!(
        matches!(port_head_res, Err(StorageError::InvalidRepoName(_))),
        "ManifestReader::head_manifest must reject '..' traversal with InvalidRepoName: got {port_head_res:?}"
    );

    let port_get_res = <FsStorage as crate::storage::ports::ManifestReader>::get_manifest(
        &storage,
        traversal_repo_input,
        &digest,
    )
    .await;
    assert!(
        matches!(port_get_res, Err(StorageError::InvalidRepoName(_))),
        "ManifestReader::get_manifest must reject '..' traversal with InvalidRepoName: got {port_get_res:?}"
    );

    // Focused production-path checks for pre-composition rejection cases:
    let unsafe_repo_inputs = &[
        ("", "empty repository name"),
        ("/leading_slash", "leading slash"),
        ("trailing_slash/", "trailing slash"),
        ("back\\slash", "backslash"),
        ("repo\0nul", "embedded NUL"),
        ("repo\x1fcontrol", "ASCII control character"),
        ("double//slash", "repeated slashes"),
        ("dot/./segment", "single dot segment"),
    ];

    for (unsafe_repo, desc) in unsafe_repo_inputs {
        let head_err = storage
            .head_manifest(unsafe_repo, &digest)
            .await
            .expect_err(&format!("head_manifest must reject {desc}"));
        assert!(
            matches!(head_err, StorageError::InvalidRepoName(_)),
            "head_manifest must return InvalidRepoName for {desc}: got {head_err:?}"
        );

        let get_err = storage
            .get_manifest(unsafe_repo, &digest)
            .await
            .expect_err(&format!("get_manifest must reject {desc}"));
        assert!(
            matches!(get_err, StorageError::InvalidRepoName(_)),
            "get_manifest must return InvalidRepoName for {desc}: got {get_err:?}"
        );
    }

    // Preserved acceptance of C:/repo on Linux:
    // When repo is "C:/repo", manifest_key composes "repos/C:/repo/manifests/<hex>".
    // On Linux, this is a valid relative path with a colon-bearing segment.
    #[cfg(target_os = "linux")]
    {
        // 1. Missing C:/repo manifests returns NotFound, proving manifest_key accepted it
        let missing_digest = Digest::parse(
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .expect("valid digest");
        let missing_c_head = storage.head_manifest("C:/repo", &missing_digest).await;
        assert!(
            matches!(missing_c_head, Err(StorageError::NotFound)),
            "head_manifest for non-existent C:/repo must return NotFound on Linux (not InvalidRepoName): got {missing_c_head:?}"
        );

        // 2. Existing C:/repo manifest succeeds and reads content
        put_manifest_file(&root, "C:/repo", &hex, manifest_bytes);
        let c_head = storage
            .head_manifest("C:/repo", &digest)
            .await
            .expect("head_manifest for C:/repo must succeed on Linux");
        assert_eq!(c_head.size, manifest_bytes.len() as u64);
        assert_eq!(
            c_head.media_type,
            "application/vnd.oci.image.manifest.v1+json"
        );

        let (c_get_meta, c_payload) = storage
            .get_manifest("C:/repo", &digest)
            .await
            .expect("get_manifest for C:/repo must succeed on Linux");
        assert_eq!(c_get_meta, c_head);
        assert_eq!(c_payload.as_ref(), manifest_bytes);
    }
}

#[tokio::test]
#[cfg(unix)]
async fn test_manifest_read_containment_symlink_traversal() {
    use std::os::unix::fs::symlink;

    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    let outside = fixture.path().join("outside");
    std::fs::create_dir_all(&outside).expect("create outside dir");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let repo = "symlink_repo";
    let manifests_dir = root.join("repos").join(repo).join("manifests");
    std::fs::create_dir_all(&manifests_dir).expect("create manifests dir");

    let manifest_bytes =
        br#"{"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json"}"#;
    let hex = hex_sha256(manifest_bytes);
    let digest = Digest::parse(&format!("sha256:{hex}")).expect("valid digest");

    // Scenario 1: Manifest file is a symlink pointing to an outside file
    let outside_file = outside.join("external_manifest.json");
    write_file(&outside_file, manifest_bytes);
    let symlink_file = manifests_dir.join(&hex);
    symlink(&outside_file, &symlink_file).expect("create symlink to outside file");

    // Contained behavior: openat2 resolution rejection fails closed with StorageErrorKind::Io
    let head_sym_outside = storage.head_manifest(repo, &digest).await;
    match head_sym_outside {
        Err(StorageError::Internal { kind, .. }) => {
            assert_eq!(
                kind,
                StorageErrorKind::Io,
                "External symlink must be rejected with StorageErrorKind::Io"
            );
        }
        other => panic!("expected StorageErrorKind::Io for external symlink, got {other:?}"),
    }
    let get_sym_outside = storage.get_manifest(repo, &digest).await;
    match get_sym_outside {
        Err(StorageError::Internal { kind, .. }) => {
            assert_eq!(
                kind,
                StorageErrorKind::Io,
                "External symlink must be rejected with StorageErrorKind::Io"
            );
        }
        other => panic!("expected StorageErrorKind::Io for external symlink, got {other:?}"),
    }

    // Scenario 2: Manifest file is a symlink pointing inside storage root
    std::fs::remove_file(&symlink_file).expect("remove symlink 1");
    let inside_target = root.join("repos").join(repo).join("inside_target.json");
    write_file(&inside_target, manifest_bytes);
    symlink(&inside_target, &symlink_file).expect("create symlink inside root");

    let head_sym_inside = storage.head_manifest(repo, &digest).await;
    match head_sym_inside {
        Err(StorageError::Internal { kind, .. }) => {
            assert_eq!(
                kind,
                StorageErrorKind::Io,
                "Internal symlink must be rejected with StorageErrorKind::Io"
            );
        }
        other => panic!("expected StorageErrorKind::Io for internal symlink, got {other:?}"),
    }
    let get_sym_inside = storage.get_manifest(repo, &digest).await;
    match get_sym_inside {
        Err(StorageError::Internal { kind, .. }) => {
            assert_eq!(
                kind,
                StorageErrorKind::Io,
                "Internal symlink must be rejected with StorageErrorKind::Io"
            );
        }
        other => panic!("expected StorageErrorKind::Io for internal symlink, got {other:?}"),
    }

    // Scenario 3: Intermediate manifests directory is a symlink to an outside directory
    let repo_outside_manifests = root.join("repos").join("repo_sym_dir");
    std::fs::create_dir_all(&repo_outside_manifests).expect("create repo_sym_dir");
    let outside_manifests_dir = outside.join("manifests_store");
    std::fs::create_dir_all(&outside_manifests_dir).expect("create outside manifests store");
    let outside_manifest_file = outside_manifests_dir.join(&hex);
    write_file(&outside_manifest_file, manifest_bytes);

    let symlink_manifests_dir = repo_outside_manifests.join("manifests");
    symlink(&outside_manifests_dir, &symlink_manifests_dir).expect("symlink manifests dir");

    let head_dir_sym = storage.head_manifest("repo_sym_dir", &digest).await;
    match head_dir_sym {
        Err(StorageError::Internal { kind, .. }) => {
            assert_eq!(
                kind,
                StorageErrorKind::Io,
                "Ancestor directory symlink must be rejected with StorageErrorKind::Io"
            );
        }
        other => panic!("expected StorageErrorKind::Io for ancestor symlink, got {other:?}"),
    }
    let get_dir_sym = storage.get_manifest("repo_sym_dir", &digest).await;
    match get_dir_sym {
        Err(StorageError::Internal { kind, .. }) => {
            assert_eq!(
                kind,
                StorageErrorKind::Io,
                "Ancestor directory symlink must be rejected with StorageErrorKind::Io"
            );
        }
        other => panic!("expected StorageErrorKind::Io for ancestor symlink, got {other:?}"),
    }

    // Scenario 4: Dangling symlink fails closed with Io (openat2 resolution rejection overrides NotFound)
    std::fs::remove_file(&symlink_file).expect("remove symlink");
    let nonexistent_target = root.join("nonexistent_target_file");
    symlink(&nonexistent_target, &symlink_file).expect("create dangling symlink");

    let head_dangling = storage.head_manifest(repo, &digest).await;
    match head_dangling {
        Err(StorageError::Internal { kind, .. }) => {
            assert_eq!(
                kind,
                StorageErrorKind::Io,
                "Dangling symlink must produce StorageErrorKind::Io (ResolutionRejected overrides NotFound)"
            );
        }
        other => panic!("expected StorageErrorKind::Io for dangling symlink, got {other:?}"),
    }
    let get_dangling = storage.get_manifest(repo, &digest).await;
    match get_dangling {
        Err(StorageError::Internal { kind, .. }) => {
            assert_eq!(
                kind,
                StorageErrorKind::Io,
                "Dangling symlink must produce StorageErrorKind::Io (ResolutionRejected overrides NotFound)"
            );
        }
        other => panic!("expected StorageErrorKind::Io for dangling symlink, got {other:?}"),
    }

    // Genuine missing paths (without a rejected symlink) remain NotFound:
    let missing_digest =
        Digest::parse("sha256:baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .expect("valid digest");
    let genuine_missing_head = storage.head_manifest(repo, &missing_digest).await;
    assert!(
        matches!(genuine_missing_head, Err(StorageError::NotFound)),
        "Genuine missing path must return NotFound: got {genuine_missing_head:?}"
    );
    let genuine_missing_get = storage.get_manifest(repo, &missing_digest).await;
    assert!(
        matches!(genuine_missing_get, Err(StorageError::NotFound)),
        "Genuine missing path must return NotFound: got {genuine_missing_get:?}"
    );
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn test_manifest_read_production_pinned_root_across_rename() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    let renamed_root = fixture.path().join("storage-root-renamed");

    let repo = "pinned_repo";
    let content_a = br#"{"schemaVersion": 2, "mediaType": "application/vnd.manifest.a+json"}"#;
    let hex_a = hex_sha256(content_a);
    let digest_a = Digest::parse(&format!("sha256:{hex_a}")).expect("valid digest");

    // Put manifest A in root before storage initialization
    put_manifest_file(&root, repo, &hex_a, content_a);

    // Initialize production FsStorage (opens shared reader pinned to root descriptor)
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    // Rename root to renamed_root
    std::fs::rename(&root, &renamed_root).expect("rename storage root");

    // Recreate the old pathname with distinguishable content B under the same repo & digest
    let content_b = br#"{"schemaVersion": 2, "mediaType": "application/vnd.manifest.b+json"}"#;
    put_manifest_file(&root, repo, &hex_a, content_b);

    // 1. Production head_manifest must observe content A via pinned reader
    let head_meta = storage
        .head_manifest(repo, &digest_a)
        .await
        .expect("head_manifest succeeds through pinned reader");
    assert_eq!(head_meta.media_type, "application/vnd.manifest.a+json");
    assert_eq!(head_meta.size, content_a.len() as u64);

    // 2. Production get_manifest must observe content A bytes via pinned reader
    let (get_meta, payload) = storage
        .get_manifest(repo, &digest_a)
        .await
        .expect("get_manifest succeeds through pinned reader");
    assert_eq!(get_meta.media_type, "application/vnd.manifest.a+json");
    assert_eq!(get_meta.size, content_a.len() as u64);
    assert_eq!(payload.as_ref(), content_a);

    // 3. Port forwarding through ManifestReader must also observe content A
    let port_head_meta = <FsStorage as crate::storage::ports::ManifestReader>::head_manifest(
        &storage, repo, &digest_a,
    )
    .await
    .expect("port head_manifest succeeds through pinned reader");
    assert_eq!(port_head_meta.media_type, "application/vnd.manifest.a+json");
    assert_eq!(port_head_meta.size, content_a.len() as u64);

    let (port_get_meta, port_payload) =
        <FsStorage as crate::storage::ports::ManifestReader>::get_manifest(
            &storage, repo, &digest_a,
        )
        .await
        .expect("port get_manifest succeeds through pinned reader");
    assert_eq!(port_get_meta.media_type, "application/vnd.manifest.a+json");
    assert_eq!(port_get_meta.size, content_a.len() as u64);
    assert_eq!(port_payload.as_ref(), content_a);
}

#[tokio::test]
#[cfg(unix)]
#[ignore = "requires unprivileged user environment where chmod 0o000 denies filesystem access"]
async fn test_manifest_read_permission_denied_ignored() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let repo = "perm_repo";
    let manifest_bytes =
        br#"{"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json"}"#;
    let hex = hex_sha256(manifest_bytes);
    let digest = Digest::parse(&format!("sha256:{hex}")).expect("valid digest");
    put_manifest_file(&root, repo, &hex, manifest_bytes);

    let manifest_path = root.join("repos").join(repo).join("manifests").join(&hex);
    let orig_perms = std::fs::metadata(&manifest_path)
        .expect("metadata")
        .permissions();

    struct ScopedPermReset<'a> {
        path: &'a Path,
        original_permissions: std::fs::Permissions,
    }

    impl<'a> Drop for ScopedPermReset<'a> {
        fn drop(&mut self) {
            if let Err(err) = std::fs::set_permissions(self.path, self.original_permissions.clone())
            {
                if std::thread::panicking() {
                    eprintln!(
                        "ScopedPermReset: failed to restore permissions on {:?} during unwinding: {err}",
                        self.path
                    );
                } else {
                    panic!(
                        "ScopedPermReset: failed to restore permissions on {:?}: {err}",
                        self.path
                    );
                }
            }
        }
    }

    {
        // Install guard BEFORE permissions are restricted
        let _guard = ScopedPermReset {
            path: &manifest_path,
            original_permissions: orig_perms.clone(),
        };
        std::fs::set_permissions(&manifest_path, std::fs::Permissions::from_mode(0o000))
            .expect("set mode 0o000");

        // Fail fast if permissions are ineffective (e.g. running under root UID 0)
        if std::fs::read(&manifest_path).is_ok() {
            panic!("ineffective permissions: std::fs::read succeeded under mode 0o000");
        }

        let head_err = storage.head_manifest(repo, &digest).await.unwrap_err();
        match head_err {
            StorageError::Internal { kind, .. } => {
                assert_eq!(
                    kind,
                    StorageErrorKind::Io,
                    "PermissionDenied maps to StorageErrorKind::Io"
                );
            }
            StorageError::NotFound => panic!("Permission denied must NOT map to NotFound"),
            other => panic!("expected StorageErrorKind::Io, got: {other:?}"),
        }

        let get_err = storage.get_manifest(repo, &digest).await.unwrap_err();
        match get_err {
            StorageError::Internal { kind, .. } => {
                assert_eq!(
                    kind,
                    StorageErrorKind::Io,
                    "PermissionDenied maps to StorageErrorKind::Io"
                );
            }
            StorageError::NotFound => panic!("Permission denied must NOT map to NotFound"),
            other => panic!("expected StorageErrorKind::Io, got: {other:?}"),
        }
    }

    // On normal path, verify restored permissions against saved original permissions
    let restored_perms = std::fs::metadata(&manifest_path)
        .expect("metadata after permission restore")
        .permissions();
    assert_eq!(
        restored_perms.mode(),
        orig_perms.mode(),
        "restored permission bits must match saved original permissions"
    );
    assert!(
        std::fs::read(&manifest_path).is_ok(),
        "permissions must be restored and file readable after guard drop"
    );

    // Drop storage handles before checking fixture cleanup
    drop(storage);
    fixture
        .close()
        .expect("fixture directory close must succeed");
}
