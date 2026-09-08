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

    let expected_message = format!(
        "malformed blob file name in CAS shard {}: {invalid_filename}",
        shard_dir.display()
    );

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
async fn test_list_cas_blobs_for_gc_io_error_is_io() {
    use crate::storage::GcStorage;

    let root = tmp_fs_root();
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let blobs_dir = root.join("blobs");
    std::fs::create_dir_all(&blobs_dir).unwrap();
    let cas_root_file = blobs_dir.join("sha256");
    std::fs::write(&cas_root_file, b"not a directory").unwrap();

    let raw_io_err = tokio::fs::read_dir(&cas_root_file).await.unwrap_err();
    let expected_message = format!("read_dir {}: {raw_io_err}", cas_root_file.display());

    let res = storage.list_cas_blobs_page(None, 10).await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
        err.internal_kind(),
        Some(crate::storage::StorageErrorKind::Io),
        "Filesystem read_dir failure during GC blob listing must classify as Io"
    );
    assert_eq!(err.message(), Some(expected_message.as_str()));
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
    // Legacy behavior: tokio::fs::metadata follows the symlink and returns the target size.
    // Note: In an extracted, contained backend, symlinks below the root must be rejected.
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

    // fs_metadata_size and head_blob follow symlink
    let size = fs_metadata_size(&blob_path)
        .await
        .expect("fs_metadata_size follows inside-root symlink");
    assert_eq!(size, target_content.len() as u64);

    let meta = storage
        .head_blob(&digest)
        .await
        .expect("head_blob succeeds via inside-root symlink");
    assert_eq!(meta.size, target_content.len() as u64);
}

#[tokio::test]
#[cfg(unix)]
async fn test_fs_metadata_containment_symlink_outside_root() {
    // Characterizes Case 2: Final blob entry points outside configured root but inside test fixture.
    // Legacy behavior: tokio::fs::metadata follows the symlink outside root and returns target size.
    // Open containment gap: Escapes storage root boundary.
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
    let outside_content = b"outside root target payload with unique length";
    write_file(&target_outside, outside_content);

    let blob_path = storage.blob_path(&digest);
    if let Some(parent) = blob_path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dirs");
    }
    std::os::unix::fs::symlink(&target_outside, &blob_path).expect("create outside-root symlink");

    let size = fs_metadata_size(&blob_path)
        .await
        .expect("fs_metadata_size follows outside-root symlink");
    assert_eq!(size, outside_content.len() as u64);

    let meta = storage
        .head_blob(&digest)
        .await
        .expect("head_blob succeeds via outside-root symlink");
    assert_eq!(meta.size, outside_content.len() as u64);
}

#[tokio::test]
#[cfg(unix)]
async fn test_fs_metadata_containment_intermediate_dir_symlink_outside_root() {
    // Characterizes Case 3: Intermediate path component is a directory symlink to sibling fixture directory outside root.
    // Legacy behavior: tokio::fs::metadata traverses through intermediate directory symlink.
    // Open containment gap: Path traversal through symlinked directory escapes storage root.
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

    let size = fs_metadata_size(&blob_path)
        .await
        .expect("fs_metadata_size traverses intermediate dir symlink");
    assert_eq!(size, content.len() as u64);

    let meta = storage
        .head_blob(&digest)
        .await
        .expect("head_blob succeeds via intermediate dir symlink");
    assert_eq!(meta.size, content.len() as u64);
}

#[tokio::test]
#[cfg(unix)]
async fn test_fs_metadata_containment_dangling_symlink_falls_back_to_quarantine() {
    // Characterizes Case 4: Dangling ordinary blob symlink with a valid quarantine blob.
    // Behavior: stat() on dangling symlink fails with NotFound, which causes head_blob
    // to fall back to the quarantine blob rather than surfacing an invalid symlink error.
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

    // Direct fs_metadata_size on dangling symlink yields NotFound
    let io_err = fs_metadata_size(&blob_path)
        .await
        .expect_err("metadata on dangling symlink must fail");
    assert_eq!(io_err.kind(), std::io::ErrorKind::NotFound);

    // head_blob treats NotFound as missing ordinary blob and falls back to quarantine
    let meta = storage
        .head_blob(&digest)
        .await
        .expect("head_blob falls back to quarantine on dangling symlink");
    assert_eq!(meta.size, quarantine_content.len() as u64);
}

#[tokio::test]
#[cfg(unix)]
async fn test_fs_metadata_containment_quarantine_symlink_outside_root() {
    // Characterizes Case 5: Quarantine-path symlink to target outside configured root, ordinary path absent.
    // Legacy behavior: Ordinary lookup fails with NotFound, fallback to quarantine follows symlink outside root.
    // Open containment gap: Quarantine lookup also escapes storage root.
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

    let size = fs_metadata_size(&qpath)
        .await
        .expect("fs_metadata_size follows quarantine symlink outside root");
    assert_eq!(size, outside_content.len() as u64);

    let meta = storage
        .head_blob(&digest)
        .await
        .expect("head_blob succeeds via quarantine symlink outside root");
    assert_eq!(meta.size, outside_content.len() as u64);
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
}

#[tokio::test]
async fn test_fs_metadata_containment_directory_blob_returns_metadata_size() {
    // Characterizes Case 7: Ordinary blob path resolves to a directory rather than a regular file.
    // Current behavior: fs_metadata_size returns directory metadata size (e.g. 4096 on Linux)
    // without error, and head_blob returns Ok(BlobMeta { size }) because is_file() is not checked.
    // Open containment/contract gap: Directories are not rejected at the metadata boundary.
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage_root");
    std::fs::create_dir_all(&root).expect("create storage root");
    let storage = FsStorage::new(root.clone(), 1024 * 1024);

    let digest =
        Digest::parse("sha256:7777777777777777777777777777777777777777777777777777777777777707")
            .unwrap();
    let blob_path = storage.blob_path(&digest);
    std::fs::create_dir_all(&blob_path).expect("create directory at blob path");

    // Obtain actual platform directory metadata length as ground truth
    let expected_dir_size = std::fs::metadata(&blob_path)
        .expect("query directory metadata")
        .len();

    let size = fs_metadata_size(&blob_path)
        .await
        .expect("fs_metadata_size returns directory size");
    assert_eq!(size, expected_dir_size);

    let meta = storage
        .head_blob(&digest)
        .await
        .expect("head_blob returns directory size without error");
    assert_eq!(meta.size, expected_dir_size);
}
