use super::*;
use std::sync::Arc;
use tokio::io::AsyncReadExt;

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
        StorageError::Internal(msg) => {
            assert!(msg.contains("malformed"));
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
    assert!(matches!(err, StorageError::Internal(_)));
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
    assert!(matches!(res.unwrap_err(), StorageError::Internal(_)));
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
    assert!(matches!(res.unwrap_err(), StorageError::Internal(_)));
}
