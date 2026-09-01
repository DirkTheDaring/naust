use super::*;
use crate::storage::ConditionalDeleteResult;
use std::collections::HashMap;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone)]
pub struct S3CallLogEntry {
    pub method: String,
    pub key: String,
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    #[allow(dead_code)]
    pub body_len: usize,
}

type MockPartMap = HashMap<i32, (Bytes, String)>;
type MockMultipartStore = HashMap<String, (String, MockPartMap, u64)>;
type MockObjectStore = HashMap<String, (Bytes, String)>;
type MockHookFn = Arc<dyn Fn(&str, &str) -> Option<StorageError> + Send + Sync>;

pub struct MockS3Driver {
    pub objects: StdMutex<MockObjectStore>,
    pub multiparts: StdMutex<MockMultipartStore>,
    pub clock_secs: AtomicU64,
    pub etag_seq: AtomicU64,
    pub call_log: StdMutex<Vec<S3CallLogEntry>>,
    pub injected_412_keys: StdMutex<HashSet<String>>,
    pub versioning_state: StdMutex<S3BucketVersioningState>,
    pub hook_before_op: StdMutex<Option<MockHookFn>>,
    pub hook_after_op: StdMutex<Option<MockHookFn>>,
}

impl MockS3Driver {
    pub fn new(initial_time: u64) -> Self {
        Self {
            objects: StdMutex::new(HashMap::new()),
            multiparts: StdMutex::new(HashMap::new()),
            clock_secs: AtomicU64::new(initial_time),
            etag_seq: AtomicU64::new(1),
            call_log: StdMutex::new(Vec::new()),
            injected_412_keys: StdMutex::new(HashSet::new()),
            versioning_state: StdMutex::new(S3BucketVersioningState::Unversioned),
            hook_before_op: StdMutex::new(None),
            hook_after_op: StdMutex::new(None),
        }
    }

    pub fn set_hook_before<F>(&self, f: F)
    where
        F: Fn(&str, &str) -> Option<StorageError> + Send + Sync + 'static,
    {
        *self.hook_before_op.lock().unwrap() = Some(Arc::new(f));
    }

    #[allow(dead_code)]
    pub fn set_hook_after<F>(&self, f: F)
    where
        F: Fn(&str, &str) -> Option<StorageError> + Send + Sync + 'static,
    {
        *self.hook_after_op.lock().unwrap() = Some(Arc::new(f));
    }

    pub fn clear_hooks(&self) {
        *self.hook_before_op.lock().unwrap() = None;
        *self.hook_after_op.lock().unwrap() = None;
    }

    pub fn advance_time(&self, secs: u64) {
        self.clock_secs.fetch_add(secs, Ordering::SeqCst);
    }

    pub fn set_time(&self, secs: u64) {
        self.clock_secs.store(secs, Ordering::SeqCst);
    }

    pub fn inject_412_on_key(&self, key: &str) {
        self.injected_412_keys
            .lock()
            .unwrap()
            .insert(key.to_string());
    }

    pub fn clear_injected_412(&self) {
        self.injected_412_keys.lock().unwrap().clear();
    }

    pub fn get_call_log(&self) -> Vec<S3CallLogEntry> {
        self.call_log.lock().unwrap().clone()
    }

    fn check_before_hook(&self, method: &str, key: &str) -> Result<(), StorageError> {
        if let Some(ref hook) = *self.hook_before_op.lock().unwrap()
            && let Some(err) = hook(method, key)
        {
            return Err(err);
        }
        Ok(())
    }

    fn check_after_hook(&self, method: &str, key: &str) -> Result<(), StorageError> {
        if let Some(ref hook) = *self.hook_after_op.lock().unwrap()
            && let Some(err) = hook(method, key)
        {
            return Err(err);
        }
        Ok(())
    }
}

#[async_trait]
impl S3Driver for MockS3Driver {
    async fn get_bucket_versioning_state(&self, _bucket: &str) -> S3BucketVersioningState {
        self.versioning_state.lock().unwrap().clone()
    }

    async fn create_multipart_upload(
        &self,
        _bucket: &str,
        key: &str,
    ) -> Result<String, StorageError> {
        let mut log = self.call_log.lock().unwrap();
        log.push(S3CallLogEntry {
            method: "create_multipart_upload".to_string(),
            key: key.to_string(),
            if_match: None,
            if_none_match: None,
            body_len: 0,
        });
        drop(log);

        self.check_before_hook("create_multipart_upload", key)?;

        let upload_id = uuid::Uuid::new_v4().to_string();
        let now = self.now_unix_secs();
        let mut mps = self.multiparts.lock().unwrap();
        mps.insert(upload_id.clone(), (key.to_string(), HashMap::new(), now));
        drop(mps);

        self.check_after_hook("create_multipart_upload", key)?;

        Ok(upload_id)
    }

    async fn upload_part(
        &self,
        _bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: Bytes,
    ) -> Result<String, StorageError> {
        let mut log = self.call_log.lock().unwrap();
        log.push(S3CallLogEntry {
            method: "upload_part".to_string(),
            key: key.to_string(),
            if_match: None,
            if_none_match: None,
            body_len: body.len(),
        });
        drop(log);

        self.check_before_hook("upload_part", key)?;

        let mut mps = self.multiparts.lock().unwrap();
        let Some((_, parts, _)) = mps.get_mut(upload_id) else {
            return Err(StorageError::NotFound);
        };
        let etag = format!("\"etag_p{}_{}\"", part_number, body.len());
        parts.insert(part_number, (body, etag.clone()));
        drop(mps);

        self.check_after_hook("upload_part", key)?;

        Ok(etag)
    }

    async fn complete_multipart_upload(
        &self,
        _bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<(), StorageError> {
        let mut log = self.call_log.lock().unwrap();
        log.push(S3CallLogEntry {
            method: "complete_multipart_upload".to_string(),
            key: key.to_string(),
            if_match: None,
            if_none_match: None,
            body_len: 0,
        });
        drop(log);

        self.check_before_hook("complete_multipart_upload", key)?;

        let mut mps = self.multiparts.lock().unwrap();
        let Some((_mp_key, stored_parts, _)) = mps.remove(upload_id) else {
            return Err(StorageError::NotFound);
        };
        let mut assembled = Vec::new();
        for (num, _) in parts {
            let Some((p_bytes, _)) = stored_parts.get(&num) else {
                return Err(StorageError::Internal(format!("missing part {num}")));
            };
            assembled.extend_from_slice(p_bytes);
        }
        let mut objs = self.objects.lock().unwrap();
        let etag = format!("\"mp_etag_{}\"", assembled.len());
        objs.insert(key.to_string(), (Bytes::from(assembled), etag));
        drop(objs);

        self.check_after_hook("complete_multipart_upload", key)?;

        Ok(())
    }

    async fn abort_multipart_upload(
        &self,
        _bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageError> {
        let mut log = self.call_log.lock().unwrap();
        log.push(S3CallLogEntry {
            method: "abort_multipart_upload".to_string(),
            key: key.to_string(),
            if_match: None,
            if_none_match: None,
            body_len: 0,
        });
        drop(log);

        self.check_before_hook("abort_multipart_upload", key)?;

        let mut mps = self.multiparts.lock().unwrap();
        mps.remove(upload_id);
        drop(mps);

        self.check_after_hook("abort_multipart_upload", key)?;

        Ok(())
    }

    async fn list_multipart_uploads(
        &self,
        _bucket: &str,
        prefix: &str,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<S3MultipartListResult, StorageError> {
        let mut log = self.call_log.lock().unwrap();
        log.push(S3CallLogEntry {
            method: "list_multipart_uploads".to_string(),
            key: prefix.to_string(),
            if_match: None,
            if_none_match: None,
            body_len: 0,
        });
        drop(log);

        self.check_before_hook("list_multipart_uploads", prefix)?;

        let mps = self.multiparts.lock().unwrap();
        let mut items = Vec::new();
        for (uid, (k, _, initiated)) in mps.iter() {
            if k.starts_with(prefix) {
                items.push(S3MultipartUploadSummary {
                    key: k.clone(),
                    upload_id: uid.clone(),
                    initiated_at_unix_secs: *initiated,
                });
            }
        }
        items.sort_by(|a, b| a.key.cmp(&b.key).then(a.upload_id.cmp(&b.upload_id)));

        let mut start_idx = 0;
        if let Some(km) = key_marker {
            if let Some(pos) = items.iter().position(|u| {
                u.key.as_str() > km
                    || (u.key == km
                        && upload_id_marker
                            .map(|uim| u.upload_id.as_str() > uim)
                            .unwrap_or(false))
            }) {
                start_idx = pos;
            } else {
                start_idx = items.len();
            }
        }

        let slice = &items[start_idx..];
        Ok(S3MultipartListResult {
            uploads: slice.to_vec(),
            next_key_marker: None,
            next_upload_id_marker: None,
            is_truncated: false,
        })
    }

    async fn get_object(
        &self,
        _bucket: &str,
        key: &str,
    ) -> Result<Option<(Bytes, String)>, StorageError> {
        let mut log = self.call_log.lock().unwrap();
        log.push(S3CallLogEntry {
            method: "get_object".to_string(),
            key: key.to_string(),
            if_match: None,
            if_none_match: None,
            body_len: 0,
        });
        drop(log);

        self.check_before_hook("get_object", key)?;

        let objs = self.objects.lock().unwrap();
        let res = objs.get(key).cloned();
        drop(objs);

        self.check_after_hook("get_object", key)?;

        Ok(res)
    }

    async fn head_object(&self, _bucket: &str, key: &str) -> Result<Option<u64>, StorageError> {
        let mut log = self.call_log.lock().unwrap();
        log.push(S3CallLogEntry {
            method: "head_object".to_string(),
            key: key.to_string(),
            if_match: None,
            if_none_match: None,
            body_len: 0,
        });
        drop(log);

        self.check_before_hook("head_object", key)?;

        let objs = self.objects.lock().unwrap();
        let res = objs.get(key).map(|(b, _)| b.len() as u64);
        drop(objs);

        self.check_after_hook("head_object", key)?;

        Ok(res)
    }

    async fn put_object_conditional(
        &self,
        _bucket: &str,
        key: &str,
        body: Bytes,
        if_match: Option<String>,
        if_none_match: Option<String>,
    ) -> Result<String, StorageError> {
        let mut log = self.call_log.lock().unwrap();
        log.push(S3CallLogEntry {
            method: "put_object".to_string(),
            key: key.to_string(),
            if_match: if_match.clone(),
            if_none_match: if_none_match.clone(),
            body_len: body.len(),
        });
        drop(log);

        self.check_before_hook("put_object", key)?;

        if self.injected_412_keys.lock().unwrap().contains(key) {
            return Err(StorageError::TagAlreadyExists);
        }

        let mut objs = self.objects.lock().unwrap();
        let existing = objs.get(key);

        if matches!(if_none_match.as_deref(), Some("*")) && existing.is_some() {
            return Err(StorageError::TagAlreadyExists);
        }

        if let Some(ref m) = if_match {
            let m_clean = m.trim_matches('"');
            match existing {
                Some((_, cur_etag)) => {
                    if cur_etag.trim_matches('"') != m_clean {
                        return Err(StorageError::TagAlreadyExists);
                    }
                }
                None => return Err(StorageError::TagAlreadyExists),
            }
        }

        let seq = self.etag_seq.fetch_add(1, Ordering::SeqCst);
        let new_etag = format!("\"etag_{}_{}_{}\"", key.replace('/', "_"), body.len(), seq);
        objs.insert(key.to_string(), (body, new_etag.clone()));
        drop(objs);

        self.check_after_hook("put_object", key)?;

        Ok(new_etag.trim_matches('"').to_string())
    }

    async fn delete_object(&self, _bucket: &str, key: &str) -> Result<(), StorageError> {
        let mut log = self.call_log.lock().unwrap();
        log.push(S3CallLogEntry {
            method: "delete_object".to_string(),
            key: key.to_string(),
            if_match: None,
            if_none_match: None,
            body_len: 0,
        });
        drop(log);

        self.check_before_hook("delete_object", key)?;

        let mut objs = self.objects.lock().unwrap();
        objs.remove(key);
        drop(objs);

        self.check_after_hook("delete_object", key)?;

        Ok(())
    }

    async fn delete_object_conditional(
        &self,
        _bucket: &str,
        key: &str,
        if_match: Option<String>,
    ) -> Result<ConditionalDeleteResult, StorageError> {
        let mut log = self.call_log.lock().unwrap();
        log.push(S3CallLogEntry {
            method: "delete_object_conditional".to_string(),
            key: key.to_string(),
            if_match: if_match.clone(),
            if_none_match: None,
            body_len: 0,
        });
        drop(log);

        self.check_before_hook("delete_object", key)?;

        let mut objs = self.objects.lock().unwrap();
        let Some((_bytes, existing_etag)) = objs.get(key) else {
            return Ok(ConditionalDeleteResult::NotFound);
        };

        if let Some(ref expected_etag) = if_match {
            let norm_expected = expected_etag.trim_matches('"');
            let norm_actual = existing_etag.trim_matches('"');
            if norm_expected != "*" && norm_expected != norm_actual {
                let current_etag = existing_etag.trim_matches('"').to_string();
                drop(objs);
                return Ok(ConditionalDeleteResult::PreconditionFailed {
                    current_version: Some(current_etag),
                });
            }
        }

        objs.remove(key);
        drop(objs);

        self.check_after_hook("delete_object", key)?;
        Ok(ConditionalDeleteResult::Deleted)
    }

    async fn copy_object(
        &self,
        _src_bucket: &str,
        src_key: &str,
        _dst_bucket: &str,
        dst_key: &str,
    ) -> Result<(), StorageError> {
        let mut log = self.call_log.lock().unwrap();
        log.push(S3CallLogEntry {
            method: "copy_object".to_string(),
            key: format!("{src_key} -> {dst_key}"),
            if_match: None,
            if_none_match: None,
            body_len: 0,
        });
        drop(log);

        self.check_before_hook("copy_object", dst_key)?;

        let mut objs = self.objects.lock().unwrap();
        let Some((bytes, _)) = objs.get(src_key).cloned() else {
            return Err(StorageError::NotFound);
        };
        let new_etag = format!("\"copy_etag_{}\"", bytes.len());
        objs.insert(dst_key.to_string(), (bytes, new_etag));
        drop(objs);

        self.check_after_hook("copy_object", dst_key)?;

        Ok(())
    }

    async fn list_objects_v2(
        &self,
        _bucket: &str,
        prefix: &str,
    ) -> Result<Vec<S3ObjectSummary>, StorageError> {
        let mut log = self.call_log.lock().unwrap();
        log.push(S3CallLogEntry {
            method: "list_objects_v2".to_string(),
            key: prefix.to_string(),
            if_match: None,
            if_none_match: None,
            body_len: 0,
        });
        drop(log);

        self.check_before_hook("list_objects_v2", prefix)?;

        let objs = self.objects.lock().unwrap();
        let clock = self.now_unix_secs();
        let mut out = Vec::new();
        for (k, (b, etag)) in objs.iter() {
            if k.starts_with(prefix) {
                out.push(S3ObjectSummary {
                    key: k.clone(),
                    size: b.len() as u64,
                    last_modified_unix_secs: clock,
                    e_tag: Some(etag.clone()),
                });
            }
        }
        drop(objs);

        self.check_after_hook("list_objects_v2", prefix)?;

        Ok(out)
    }

    async fn list_objects_v2_page(
        &self,
        _bucket: &str,
        prefix: &str,
        continuation_token: Option<&str>,
        max_keys: i32,
    ) -> Result<S3ObjectsPage, StorageError> {
        let mut log = self.call_log.lock().unwrap();
        log.push(S3CallLogEntry {
            method: "list_objects_v2_page".to_string(),
            key: prefix.to_string(),
            if_match: None,
            if_none_match: None,
            body_len: 0,
        });
        drop(log);

        self.check_before_hook("list_objects_v2_page", prefix)?;

        let objs = self.objects.lock().unwrap();
        let clock = self.now_unix_secs();
        let mut matched = Vec::new();
        for (k, (b, etag)) in objs.iter() {
            if k.starts_with(prefix) {
                if let Some(tok) = continuation_token {
                    if k.as_str() <= tok {
                        continue;
                    }
                }
                matched.push(S3ObjectSummary {
                    key: k.clone(),
                    size: b.len() as u64,
                    last_modified_unix_secs: clock,
                    e_tag: Some(etag.clone()),
                });
            }
        }
        drop(objs);

        matched.sort_by(|a, b| a.key.cmp(&b.key));
        let has_more = matched.len() as i32 > max_keys;
        if has_more {
            matched.truncate(max_keys as usize);
        }
        let next_continuation_token = if has_more {
            matched.last().map(|o| o.key.clone())
        } else {
            None
        };

        self.check_after_hook("list_objects_v2_page", prefix)?;

        Ok(S3ObjectsPage {
            objects: matched,
            next_continuation_token,
        })
    }

    fn now_unix_secs(&self) -> u64 {
        self.clock_secs.load(Ordering::SeqCst)
    }
}

fn make_test_stream(chunks: Vec<Bytes>) -> UploadByteStream {
    Box::pin(futures_util::stream::iter(chunks.into_iter().map(Ok)))
}

fn compute_sha256_digest(bytes: &[u8]) -> Digest {
    let hash = sha2::Sha256::digest(bytes);
    Digest::parse(&format!("sha256:{}", hex::encode(hash))).unwrap()
}

pub fn create_mock_storage() -> (S3Storage, Arc<MockS3Driver>) {
    let driver = Arc::new(MockS3Driver::new(1000));
    let storage = S3Storage::new_with_driver(
        Some("test-bucket".to_string()),
        "".to_string(),
        100 * 1024 * 1024,
        driver.clone(),
    );
    (storage, driver)
}

// ==========================================
// 1. Serialization Round-Trip Tests
// ==========================================

#[test]
fn test_s3_session_doc_serialization_roundtrip() {
    let doc = S3SessionDoc {
        format_version: 1,
        repo: crate::registry::canonical_name::CanonicalRepoName::parse("test/repo").unwrap(),
        uuid: "12345678-1234-1234-1234-1234567890ab".to_string(),
        multipart_upload_id: "mp_upload_id_123".to_string(),
        state: UploadSessionState::Active,
        committed_offset: 10485760,
        committed_parts: vec![
            S3CommittedPart {
                part_number: 1,
                size: 5242880,
                etag: "\"etag_1\"".to_string(),
            },
            S3CommittedPart {
                part_number: 2,
                size: 5242880,
                etag: "\"etag_2\"".to_string(),
            },
        ],
        pending_buffer_key: Some("uploads/1234/pending/op_1.bin".to_string()),
        pending_bytes: 65536,
        created_at_unix_secs: 1740000000,
        last_active_at_unix_secs: 1740000050,
        current_operation: Some(S3CurrentOperation {
            operation_id: "op_2".to_string(),
            expected_offset: 10551296,
            target_part_number: 3,
            lease_expires_at_unix_secs: 1740000350,
        }),
        finalizing_info: None,
    };

    let json = serde_json::to_vec(&doc).unwrap();
    let decoded: S3SessionDoc = serde_json::from_slice(&json).unwrap();

    assert_eq!(decoded.format_version, 1);
    assert_eq!(decoded.repo, "test/repo");
    assert_eq!(decoded.uuid, "12345678-1234-1234-1234-1234567890ab");
    assert_eq!(decoded.state, UploadSessionState::Active);
    assert_eq!(decoded.committed_offset, 10485760);
    assert_eq!(decoded.committed_parts.len(), 2);
    assert_eq!(decoded.pending_bytes, 65536);
    assert_eq!(
        decoded.current_operation.as_ref().unwrap().operation_id,
        "op_2"
    );
}

#[test]
fn test_s3_finalized_receipt_serialization_roundtrip() {
    let receipt = FinalizedReceipt {
        repo: crate::registry::canonical_name::CanonicalRepoName::parse("my/repo").unwrap(),
        uuid: "98765432-1234-1234-1234-1234567890ab".to_string(),
        digest: "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            .to_string(),
        size: 10485760,
        finalized_at_unix_secs: 1740000100,
        format_version: 1,
    };

    let json = serde_json::to_vec(&receipt).unwrap();
    let decoded: FinalizedReceipt = serde_json::from_slice(&json).unwrap();

    assert_eq!(decoded.repo.as_str(), "my/repo");
    assert_eq!(decoded.uuid, "98765432-1234-1234-1234-1234567890ab");
    assert_eq!(decoded.size, 10485760);
    assert_eq!(decoded.format_version, 1);
}

// ==========================================
// 2. Lease Renewal & Concurrency Tests
// ==========================================

#[tokio::test]
async fn test_s3_lease_renewal_during_long_stream() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("myrepo").await.unwrap();

    // 10 chunks of 600 KiB (total 6 MiB).
    let chunks: Vec<Bytes> = (0..10)
        .map(|i| Bytes::from(vec![b'A' + i; 600 * 1024]))
        .collect();
    let stream = make_test_stream(chunks);

    let res = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            stream,
            100 * 1024 * 1024,
        )
        .await
        .unwrap();

    assert_eq!(
        res,
        UploadAppendResult::Committed {
            new_offset: 10 * 600 * 1024
        }
    );

    let status = storage.session_status(&session).await.unwrap();
    assert_eq!(status.committed_offset, 10 * 600 * 1024);
    assert_eq!(status.state, UploadSessionState::Active);

    // Verify that 1 part of 5 MiB was committed and remaining 1 MiB is in pending buffer
    let doc_bytes = driver
        .objects
        .lock()
        .unwrap()
        .get(&format!("uploads/{}/session.json", session.uuid))
        .unwrap()
        .0
        .clone();
    let doc: S3SessionDoc = serde_json::from_slice(&doc_bytes).unwrap();
    assert_eq!(doc.committed_parts.len(), 1);
    assert_eq!(doc.committed_parts[0].size, S3_PART_SIZE as u64);
    assert_eq!(doc.pending_bytes, 10 * 600 * 1024 - S3_PART_SIZE as u64);
}

#[tokio::test]
async fn test_s3_active_renewal_prevents_recovery() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("myrepo").await.unwrap();

    // Reserve session
    let key = format!("uploads/{}/session.json", session.uuid);
    let (doc_bytes, etag) = driver.objects.lock().unwrap().get(&key).unwrap().clone();
    let mut doc: S3SessionDoc = serde_json::from_slice(&doc_bytes).unwrap();
    doc.state = UploadSessionState::Appending;
    doc.current_operation = Some(S3CurrentOperation {
        operation_id: "op-active".to_string(),
        expected_offset: 0,
        target_part_number: 1,
        lease_expires_at_unix_secs: 1300,
    });
    driver
        .put_object_conditional(
            "test-bucket",
            &key,
            Bytes::from(serde_json::to_vec(&doc).unwrap()),
            Some(etag),
            None,
        )
        .await
        .unwrap();

    // Clock is at 1100 (within lease)
    driver.set_time(1100);
    let recovered = storage.recover_session(&session).await.unwrap();
    // Lease active -> state remains Appending, not rolled back
    assert_eq!(recovered.state, UploadSessionState::Appending);
}

#[tokio::test]
async fn test_s3_renewal_cas_412_stops_operation() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("myrepo").await.unwrap();

    // Inject 412 on session.json during streaming
    let session_key = format!("uploads/{}/session.json", session.uuid);
    driver.inject_412_on_key(&session_key);

    let chunk = vec![Bytes::from(vec![b'X'; 100 * 1024])];
    let res = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(chunk),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    assert_eq!(res, UploadAppendResult::Conflict);
}

#[tokio::test]
async fn test_s3_expired_non_renewing_owner_can_be_recovered() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("myrepo").await.unwrap();

    let key = format!("uploads/{}/session.json", session.uuid);
    let (doc_bytes, etag) = driver.objects.lock().unwrap().get(&key).unwrap().clone();
    let mut doc: S3SessionDoc = serde_json::from_slice(&doc_bytes).unwrap();
    doc.state = UploadSessionState::Appending;
    doc.current_operation = Some(S3CurrentOperation {
        operation_id: "op-stalled".to_string(),
        expected_offset: 0,
        target_part_number: 1,
        lease_expires_at_unix_secs: 1300,
    });
    driver
        .put_object_conditional(
            "test-bucket",
            &key,
            Bytes::from(serde_json::to_vec(&doc).unwrap()),
            Some(etag),
            None,
        )
        .await
        .unwrap();

    // Advance clock past lease expiration
    driver.set_time(1400);
    let recovered = storage.recover_session(&session).await.unwrap();
    assert_eq!(recovered.state, UploadSessionState::Active);
}

#[tokio::test]
async fn test_s3_original_worker_cannot_commit_after_recovery() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("myrepo").await.unwrap();

    // Worker 1 reserves at etag 1
    let key = format!("uploads/{}/session.json", session.uuid);
    let (doc_bytes, etag1) = driver.objects.lock().unwrap().get(&key).unwrap().clone();
    let mut doc: S3SessionDoc = serde_json::from_slice(&doc_bytes).unwrap();
    doc.state = UploadSessionState::Appending;
    doc.current_operation = Some(S3CurrentOperation {
        operation_id: "op-w1".to_string(),
        expected_offset: 0,
        target_part_number: 1,
        lease_expires_at_unix_secs: 1300,
    });
    driver
        .put_object_conditional(
            "test-bucket",
            &key,
            Bytes::from(serde_json::to_vec(&doc).unwrap()),
            Some(etag1),
            None,
        )
        .await
        .unwrap();

    // Clock expires and recovery runs (changes ETag)
    driver.set_time(1400);
    let _ = storage.recover_session(&session).await.unwrap();

    // Worker 1 tries to commit using its old etag1 -> receives TagAlreadyExists / 412
    doc.committed_offset = 500;
    doc.state = UploadSessionState::Active;
    let commit_res = driver
        .put_object_conditional(
            "test-bucket",
            &key,
            Bytes::from(serde_json::to_vec(&doc).unwrap()),
            Some("stale_etag".to_string()),
            None,
        )
        .await;
    assert!(matches!(commit_res, Err(StorageError::TagAlreadyExists)));
}

// ==========================================
// 3. Conditional S3 Requests Tests
// ==========================================

#[tokio::test]
async fn test_s3_create_session_uses_if_none_match() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("repo1").await.unwrap();

    let log = driver.get_call_log();
    let put_entry = log
        .iter()
        .find(|e| e.method == "put_object" && e.key.ends_with("/session.json"))
        .unwrap();
    assert_eq!(put_entry.if_none_match.as_deref(), Some("*"));
    assert_eq!(session.repo.as_str(), "repo1");
}

#[tokio::test]
async fn test_s3_reservation_uses_if_match_with_observed_etag() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("repo1").await.unwrap();

    let stream = make_test_stream(vec![Bytes::from_static(b"HELLO")]);
    let res = storage
        .append_if_offset(&session, UploadOffsetPrecondition::Exact(0), stream, 1000)
        .await
        .unwrap();
    assert_eq!(res, UploadAppendResult::Committed { new_offset: 5 });

    let log = driver.get_call_log();
    let reservations: Vec<&S3CallLogEntry> = log
        .iter()
        .filter(|e| e.method == "put_object" && e.key.ends_with("/session.json"))
        .collect();
    assert!(reservations.len() >= 2); // Initial create + reservation + final commit
    assert!(reservations[1].if_match.is_some());
}

#[tokio::test]
async fn test_s3_competing_reservation_receives_412_and_does_not_consume_body() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("repo1").await.unwrap();

    // Put session into Appending state with active lease
    let key = format!("uploads/{}/session.json", session.uuid);
    let (doc_bytes, etag) = driver.objects.lock().unwrap().get(&key).unwrap().clone();
    let mut doc: S3SessionDoc = serde_json::from_slice(&doc_bytes).unwrap();
    doc.state = UploadSessionState::Appending;
    doc.current_operation = Some(S3CurrentOperation {
        operation_id: "op-competing".to_string(),
        expected_offset: 0,
        target_part_number: 1,
        lease_expires_at_unix_secs: 1300,
    });
    driver
        .put_object_conditional(
            "test-bucket",
            &key,
            Bytes::from(serde_json::to_vec(&doc).unwrap()),
            Some(etag),
            None,
        )
        .await
        .unwrap();

    // Second worker tries append -> receives Conflict immediately without consuming stream or writing parts
    let stream = make_test_stream(vec![Bytes::from_static(b"LOSER_PAYLOAD")]);
    let res = storage
        .append_if_offset(&session, UploadOffsetPrecondition::Exact(0), stream, 1000)
        .await
        .unwrap();
    assert_eq!(res, UploadAppendResult::Conflict);
}

#[tokio::test]
async fn test_s3_retry_exhaustion_returns_conflict() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("exhaust_repo").await.unwrap();

    let session_key = format!("uploads/{}/session.json", session.uuid);
    driver.inject_412_on_key(&session_key);

    let stream = make_test_stream(vec![Bytes::from_static(b"DATA")]);
    let res = storage
        .append_if_offset(&session, UploadOffsetPrecondition::Exact(0), stream, 1000)
        .await
        .unwrap();

    assert_eq!(res, UploadAppendResult::Conflict);
}

// ==========================================
// 4. Small-Chunk S3 Implementation Tests
// ==========================================

#[tokio::test]
async fn test_s3_small_chunk_below_5mib() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("smallrepo").await.unwrap();

    let chunk = Bytes::from(vec![b'A'; 1024 * 1024]); // 1 MiB
    let res = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![chunk]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(
        res,
        UploadAppendResult::Committed {
            new_offset: 1024 * 1024
        }
    );

    let status = storage.session_status(&session).await.unwrap();
    assert_eq!(status.committed_offset, 1024 * 1024);

    let key = format!("uploads/{}/session.json", session.uuid);
    let (doc_bytes, _) = driver.objects.lock().unwrap().get(&key).unwrap().clone();
    let doc: S3SessionDoc = serde_json::from_slice(&doc_bytes).unwrap();
    assert_eq!(doc.pending_bytes, 1024 * 1024);
    assert!(doc.pending_buffer_key.is_some());
    assert_eq!(doc.committed_parts.len(), 0);
}

#[tokio::test]
async fn test_s3_multiple_small_patch_requests() {
    let (storage, _driver) = create_mock_storage();
    let session = storage.create_session("smallrepo").await.unwrap();

    // PATCH 1: 1 MiB
    let chunk1 = Bytes::from(vec![b'1'; 1024 * 1024]);
    let res1 = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![chunk1]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(
        res1,
        UploadAppendResult::Committed {
            new_offset: 1024 * 1024
        }
    );

    // PATCH 2: 2 MiB
    let chunk2 = Bytes::from(vec![b'2'; 2 * 1024 * 1024]);
    let res2 = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(1024 * 1024),
            make_test_stream(vec![chunk2]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(
        res2,
        UploadAppendResult::Committed {
            new_offset: 3 * 1024 * 1024
        }
    );

    let status = storage.session_status(&session).await.unwrap();
    assert_eq!(status.committed_offset, 3 * 1024 * 1024);
}

#[tokio::test]
async fn test_s3_pending_plus_incoming_crosses_5mib() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("smallrepo").await.unwrap();

    // 1. 3 MiB
    let chunk1 = Bytes::from(vec![b'A'; 3 * 1024 * 1024]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![chunk1]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    // 2. 4 MiB -> total 7 MiB -> 1 part of 5 MiB + 2 MiB pending
    let chunk2 = Bytes::from(vec![b'B'; 4 * 1024 * 1024]);
    let res2 = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(3 * 1024 * 1024),
            make_test_stream(vec![chunk2]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(
        res2,
        UploadAppendResult::Committed {
            new_offset: 7 * 1024 * 1024
        }
    );

    let key = format!("uploads/{}/session.json", session.uuid);
    let (doc_bytes, _) = driver.objects.lock().unwrap().get(&key).unwrap().clone();
    let doc: S3SessionDoc = serde_json::from_slice(&doc_bytes).unwrap();
    assert_eq!(doc.committed_parts.len(), 1);
    assert_eq!(doc.committed_parts[0].size, 5 * 1024 * 1024);
    assert_eq!(doc.pending_bytes, 2 * 1024 * 1024);
}

#[tokio::test]
async fn test_s3_large_stream_produces_multiple_parts_bounded() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("largerepo").await.unwrap();

    // 13 MiB stream -> Part 1 (5 MiB), Part 2 (5 MiB), Pending (3 MiB)
    let chunk = Bytes::from(vec![b'Z'; 13 * 1024 * 1024]);
    let res = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![chunk]),
            20 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(
        res,
        UploadAppendResult::Committed {
            new_offset: 13 * 1024 * 1024
        }
    );

    let key = format!("uploads/{}/session.json", session.uuid);
    let (doc_bytes, _) = driver.objects.lock().unwrap().get(&key).unwrap().clone();
    let doc: S3SessionDoc = serde_json::from_slice(&doc_bytes).unwrap();
    assert_eq!(doc.committed_parts.len(), 2);
    assert_eq!(doc.committed_parts[0].size, 5 * 1024 * 1024);
    assert_eq!(doc.committed_parts[1].size, 5 * 1024 * 1024);
    assert_eq!(doc.pending_bytes, 3 * 1024 * 1024);
}

#[tokio::test]
async fn test_s3_failed_session_cas_preserves_authoritative_pending() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("failed_cas").await.unwrap();

    // 1. Initial 2 MiB
    let chunk1 = Bytes::from(vec![b'P'; 2 * 1024 * 1024]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![chunk1]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    // 2. Inject 412 for subsequent CAS
    let key = format!("uploads/{}/session.json", session.uuid);
    driver.inject_412_on_key(&key);

    let chunk2 = Bytes::from(vec![b'Q'; 1024 * 1024]);
    let res = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(2 * 1024 * 1024),
            make_test_stream(vec![chunk2]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(res, UploadAppendResult::Conflict);

    // Authoritative offset remains 2 MiB
    driver.clear_injected_412();
    let status = storage.session_status(&session).await.unwrap();
    assert_eq!(status.committed_offset, 2 * 1024 * 1024);
}

#[tokio::test]
async fn test_s3_size_overflow_preserves_pending() {
    let (storage, _driver) = create_mock_storage();
    let session = storage.create_session("overflow_repo").await.unwrap();

    // 1. Initial 100 bytes
    let chunk1 = Bytes::from(vec![b'A'; 100]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![chunk1]),
            500,
        )
        .await
        .unwrap();

    // 2. Next chunk 600 bytes exceeds limit of 500
    let chunk2 = Bytes::from(vec![b'B'; 600]);
    let err = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(100),
            make_test_stream(vec![chunk2]),
            500,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, UploadTransitionError::TooLarge));

    let status = storage.session_status(&session).await.unwrap();
    assert_eq!(status.committed_offset, 100);
}

// ==========================================
// 5. Two-Phase S3 Finalization Tests
// ==========================================

#[tokio::test]
async fn test_s3_two_phase_finalization_with_pending_buffer() {
    let (storage, _driver) = create_mock_storage();
    let session = storage.create_session("finrepo").await.unwrap();

    // Append 2 MiB
    let payload = Bytes::from(vec![b'F'; 2 * 1024 * 1024]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![payload.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    let digest = compute_sha256_digest(&payload);

    // Phase 1: begin_finalize
    let prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(2 * 1024 * 1024),
            None,
            &digest,
            10 * 1024 * 1024,
            true,
        )
        .await
        .unwrap();
    assert_eq!(prepared.size, 2 * 1024 * 1024);
    assert_eq!(prepared.expected_digest, digest);

    // Phase 2: commit_finalize
    let outcome = storage.commit_finalize(&prepared).await.unwrap();
    assert_eq!(
        outcome,
        FinalizeOutcome::Published(BlobMeta {
            size: 2 * 1024 * 1024
        })
    );

    // Duplicate commit_finalize -> AlreadyFinalized
    let dup = storage.commit_finalize(&prepared).await.unwrap();
    assert_eq!(
        dup,
        FinalizeOutcome::AlreadyFinalized(BlobMeta {
            size: 2 * 1024 * 1024
        })
    );

    // Receipt check
    let receipt = storage
        .get_finalized_receipt(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.digest, digest.as_str());
    assert_eq!(receipt.size, 2 * 1024 * 1024);
}

#[tokio::test]
async fn test_s3_begin_finalize_with_trailing_stream() {
    let (storage, _driver) = create_mock_storage();
    let session = storage.create_session("trailingrepo").await.unwrap();

    // 1 MiB initial
    let chunk1 = Bytes::from(vec![b'1'; 1024 * 1024]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![chunk1]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    // 1 MiB trailing
    let chunk2 = Bytes::from(vec![b'2'; 1024 * 1024]);
    let mut full = Vec::new();
    full.extend_from_slice(&vec![b'1'; 1024 * 1024]);
    full.extend_from_slice(&vec![b'2'; 1024 * 1024]);
    let digest = compute_sha256_digest(&full);

    let prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(1024 * 1024),
            Some(make_test_stream(vec![chunk2])),
            &digest,
            10 * 1024 * 1024,
            true,
        )
        .await
        .unwrap();
    assert_eq!(prepared.size, 2 * 1024 * 1024);

    let outcome = storage.commit_finalize(&prepared).await.unwrap();
    assert_eq!(
        outcome,
        FinalizeOutcome::Published(BlobMeta {
            size: 2 * 1024 * 1024
        })
    );
}

#[tokio::test]
async fn test_s3_stale_prepared_handle_rejected() {
    let (storage, _driver) = create_mock_storage();
    let session = storage.create_session("finrepo").await.unwrap();

    let payload = Bytes::from(vec![b'X'; 100 * 1024]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![payload.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    let digest = compute_sha256_digest(&payload);
    let prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(100 * 1024),
            None,
            &digest,
            10 * 1024 * 1024,
            true,
        )
        .await
        .unwrap();

    let mut fake_prepared = prepared.clone();
    fake_prepared.operation_id = "stale-op-id".to_string();

    let err = storage.commit_finalize(&fake_prepared).await.unwrap_err();
    assert!(matches!(err, UploadTransitionError::InvalidPreparedHandle));
}

// ==========================================
// 6. Reaper Behavior Tests
// ==========================================

#[tokio::test]
async fn test_s3_reaper_skips_unexpired_and_reaps_expired() {
    let (storage, driver) = create_mock_storage();
    let session1 = storage.create_session("reap1").await.unwrap();
    let session2 = storage.create_session("reap2").await.unwrap();

    // Advance clock past expiration
    driver.advance_time(400);

    // Update session 1 last_active to current time (active)
    let key1 = format!("uploads/{}/session.json", session1.uuid);
    let (doc1_bytes, etag1) = driver.objects.lock().unwrap().get(&key1).unwrap().clone();
    let mut doc1: S3SessionDoc = serde_json::from_slice(&doc1_bytes).unwrap();
    doc1.last_active_at_unix_secs = driver.now_unix_secs();
    driver
        .put_object_conditional(
            "test-bucket",
            &key1,
            Bytes::from(serde_json::to_vec(&doc1).unwrap()),
            Some(etag1),
            None,
        )
        .await
        .unwrap();

    // Reaper with max_age = 300
    let reaped = storage.reap_expired_sessions(300, 300).await.unwrap();
    assert_eq!(reaped, 1);

    // session1 still exists, session2 was deleted
    assert!(storage.session_status(&session1).await.is_ok());
    assert!(matches!(
        storage.session_status(&session2).await,
        Err(UploadTransitionError::NotFound)
    ));
}

#[tokio::test]
async fn test_s3_reaper_recovers_expired_finalizing_to_receipt() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("reap_fin").await.unwrap();

    let payload = Bytes::from(vec![b'Z'; 50 * 1024]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![payload.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    let digest = compute_sha256_digest(&payload);
    let _prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(50 * 1024),
            None,
            &digest,
            10 * 1024 * 1024,
            true,
        )
        .await
        .unwrap();

    // Advance clock past expiration
    driver.advance_time(500);

    // Reaper recovers finalizing session into CAS destination + receipt
    let reaped = storage.reap_expired_sessions(300, 300).await.unwrap();
    assert_eq!(reaped, 1);

    let receipt = storage
        .get_finalized_receipt(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.digest, digest.as_str());
    assert_eq!(receipt.size, 50 * 1024);
}

#[tokio::test]
async fn test_s3_small_chunk_end_to_end_lifecycle() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("lifecycle_repo").await.unwrap();

    // 1. Initial status: 0 bytes committed, pending buffer None
    let status = storage.session_status(&session).await.unwrap();
    assert_eq!(status.committed_offset, 0);

    // 2. PATCH 1 MiB chunk at offset 0
    let chunk1 = Bytes::from(vec![b'A'; 1024 * 1024]);
    let res1 = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![chunk1.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(
        res1,
        UploadAppendResult::Committed {
            new_offset: 1024 * 1024
        }
    );
    let s_key = format!("uploads/{}/session.json", session.uuid);
    let (doc1_bytes, _) = driver.objects.lock().unwrap().get(&s_key).unwrap().clone();
    let doc1: S3SessionDoc = serde_json::from_slice(&doc1_bytes).unwrap();
    assert_eq!(doc1.committed_offset, 1024 * 1024);
    assert_eq!(doc1.committed_parts.len(), 0);
    assert_eq!(doc1.pending_bytes, 1024 * 1024);

    // 3. PATCH 2 MiB chunk at offset 1 MiB -> committed 3 MiB
    let chunk2 = Bytes::from(vec![b'B'; 2 * 1024 * 1024]);
    let res2 = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(1024 * 1024),
            make_test_stream(vec![chunk2.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(
        res2,
        UploadAppendResult::Committed {
            new_offset: 3 * 1024 * 1024
        }
    );
    let (doc2_bytes, _) = driver.objects.lock().unwrap().get(&s_key).unwrap().clone();
    let doc2: S3SessionDoc = serde_json::from_slice(&doc2_bytes).unwrap();
    assert_eq!(doc2.committed_offset, 3 * 1024 * 1024);
    assert_eq!(doc2.committed_parts.len(), 0);
    assert_eq!(doc2.pending_bytes, 3 * 1024 * 1024);

    // 4. PATCH 4 MiB chunk at offset 3 MiB -> total 7 MiB (1 part of 5 MiB + 2 MiB pending)
    let chunk3 = Bytes::from(vec![b'C'; 4 * 1024 * 1024]);
    let res3 = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(3 * 1024 * 1024),
            make_test_stream(vec![chunk3.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(
        res3,
        UploadAppendResult::Committed {
            new_offset: 7 * 1024 * 1024
        }
    );
    let (doc3_bytes, _) = driver.objects.lock().unwrap().get(&s_key).unwrap().clone();
    let doc3: S3SessionDoc = serde_json::from_slice(&doc3_bytes).unwrap();
    assert_eq!(doc3.committed_offset, 7 * 1024 * 1024);
    assert_eq!(doc3.committed_parts.len(), 1);
    assert_eq!(doc3.committed_parts[0].size, 5 * 1024 * 1024);
    assert_eq!(doc3.pending_bytes, 2 * 1024 * 1024);

    // 5. Finalize at offset 7 MiB
    let mut full_payload = Vec::new();
    full_payload.extend_from_slice(&chunk1);
    full_payload.extend_from_slice(&chunk2);
    full_payload.extend_from_slice(&chunk3);
    let full_digest = compute_sha256_digest(&full_payload);

    let prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(7 * 1024 * 1024),
            None,
            &full_digest,
            10 * 1024 * 1024,
            true,
        )
        .await
        .unwrap();
    assert_eq!(prepared.size, 7 * 1024 * 1024);

    let outcome = storage.commit_finalize(&prepared).await.unwrap();
    assert_eq!(
        outcome,
        FinalizeOutcome::Published(BlobMeta {
            size: 7 * 1024 * 1024
        })
    );

    // CAS destination blob exists and receipt exists
    let receipt = storage
        .get_finalized_receipt(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.digest, full_digest.as_str());
    assert_eq!(receipt.size, 7 * 1024 * 1024);
}

#[tokio::test]
async fn test_s3_monolithic_single_request_upload() {
    let (storage, _driver) = create_mock_storage();
    let session = storage.create_session("mono_repo").await.unwrap();

    let payload = Bytes::from(vec![b'M'; 1024 * 1024]);
    let digest = compute_sha256_digest(&payload);

    let prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(0),
            Some(make_test_stream(vec![payload.clone()])),
            &digest,
            10 * 1024 * 1024,
            true,
        )
        .await
        .unwrap();
    assert_eq!(prepared.size, 1024 * 1024);

    let outcome = storage.commit_finalize(&prepared).await.unwrap();
    assert_eq!(
        outcome,
        FinalizeOutcome::Published(BlobMeta { size: 1024 * 1024 })
    );
}

#[tokio::test]
async fn test_s3_stream_failure_preserves_pending() {
    let (storage, _driver) = create_mock_storage();
    let session = storage.create_session("stream_fail").await.unwrap();

    // 1. Initial 1 MiB
    let chunk1 = Bytes::from(vec![b'1'; 1024 * 1024]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![chunk1]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    // 2. Stream that errors mid-transfer
    let failing_stream: UploadByteStream = Box::pin(futures_util::stream::iter(vec![
        Ok(Bytes::from(vec![b'2'; 100])),
        Err(UploadStreamError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "stream broken",
        ))),
    ]));

    let err = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(1024 * 1024),
            failing_stream,
            10 * 1024 * 1024,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, UploadTransitionError::Stream(_)));

    // Authoritative offset preserved at 1 MiB
    let status = storage.session_status(&session).await.unwrap();
    assert_eq!(status.committed_offset, 1024 * 1024);
}

#[tokio::test]
async fn test_s3_recovery_multipart_part_uploaded_before_cas() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("crash_part").await.unwrap();

    // Reserve session
    let key = format!("uploads/{}/session.json", session.uuid);
    let (doc_bytes, etag) = driver.objects.lock().unwrap().get(&key).unwrap().clone();
    let mut doc: S3SessionDoc = serde_json::from_slice(&doc_bytes).unwrap();
    doc.state = UploadSessionState::Appending;
    doc.current_operation = Some(S3CurrentOperation {
        operation_id: "op-crashed".to_string(),
        expected_offset: 0,
        target_part_number: 1,
        lease_expires_at_unix_secs: 1200,
    });
    driver
        .put_object_conditional(
            "test-bucket",
            &key,
            Bytes::from(serde_json::to_vec(&doc).unwrap()),
            Some(etag),
            None,
        )
        .await
        .unwrap();

    // Upload an orphan part directly to mock
    let _ = driver
        .upload_part(
            "test-bucket",
            &storage.multipart_data_key(&session.uuid),
            &doc.multipart_upload_id,
            1,
            Bytes::from(vec![b'U'; 5 * 1024 * 1024]),
        )
        .await
        .unwrap();

    // Advance clock past expiration
    driver.advance_time(500);

    // Recovery runs
    let recovered = storage.recover_session(&session).await.unwrap();
    assert_eq!(recovered.state, UploadSessionState::Active);
    assert_eq!(recovered.committed_offset, 0);
}

#[tokio::test]
async fn test_s3_recovery_pending_buffer_uploaded_before_cas() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("crash_pending").await.unwrap();

    // Write orphan pending buffer
    let orphan_key = format!("uploads/{}/pending/orphan.bin", session.uuid);
    driver
        .put_object_conditional(
            "test-bucket",
            &orphan_key,
            Bytes::from(vec![b'P'; 100 * 1024]),
            None,
            None,
        )
        .await
        .unwrap();

    // Set session into Appending with expired lease
    let key = format!("uploads/{}/session.json", session.uuid);
    let (doc_bytes, etag) = driver.objects.lock().unwrap().get(&key).unwrap().clone();
    let mut doc: S3SessionDoc = serde_json::from_slice(&doc_bytes).unwrap();
    doc.state = UploadSessionState::Appending;
    doc.current_operation = Some(S3CurrentOperation {
        operation_id: "op-orphan".to_string(),
        expected_offset: 0,
        target_part_number: 1,
        lease_expires_at_unix_secs: 1100,
    });
    driver
        .put_object_conditional(
            "test-bucket",
            &key,
            Bytes::from(serde_json::to_vec(&doc).unwrap()),
            Some(etag),
            None,
        )
        .await
        .unwrap();

    driver.advance_time(500);

    // Recovery rolls back to Active and clears uncommitted operation
    let recovered = storage.recover_session(&session).await.unwrap();
    assert_eq!(recovered.state, UploadSessionState::Active);
    assert_eq!(recovered.committed_offset, 0);
}

#[tokio::test]
async fn test_s3_recovered_session_requires_no_state_token() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("notoken_repo").await.unwrap();

    // 1. Initial 1 MiB
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![Bytes::from(vec![b'1'; 1024 * 1024])]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    // 2. Put into stalled Appending state
    let key = format!("uploads/{}/session.json", session.uuid);
    let (doc_bytes, etag) = driver.objects.lock().unwrap().get(&key).unwrap().clone();
    let mut doc: S3SessionDoc = serde_json::from_slice(&doc_bytes).unwrap();
    doc.state = UploadSessionState::Appending;
    doc.current_operation = Some(S3CurrentOperation {
        operation_id: "op-stall".to_string(),
        expected_offset: 1024 * 1024,
        target_part_number: 1,
        lease_expires_at_unix_secs: 1100,
    });
    driver
        .put_object_conditional(
            "test-bucket",
            &key,
            Bytes::from(serde_json::to_vec(&doc).unwrap()),
            Some(etag),
            None,
        )
        .await
        .unwrap();

    // Expire lease and recover
    driver.advance_time(500);
    let recovered = storage.recover_session(&session).await.unwrap();
    assert_eq!(recovered.state, UploadSessionState::Active);
    assert_eq!(recovered.committed_offset, 1024 * 1024);

    // Next append with expected_offset = 1 MiB succeeds directly
    let res = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(1024 * 1024),
            make_test_stream(vec![Bytes::from(vec![b'2'; 1024 * 1024])]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(
        res,
        UploadAppendResult::Committed {
            new_offset: 2 * 1024 * 1024
        }
    );
}

#[tokio::test]
async fn test_s3_begin_finalize_exact_multiple_no_pending() {
    let (storage, _driver) = create_mock_storage();
    let session = storage.create_session("exact_5mib").await.unwrap();

    // Exactly 5 MiB
    let payload = Bytes::from(vec![b'E'; 5 * 1024 * 1024]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![payload.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    let digest = compute_sha256_digest(&payload);
    let prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(5 * 1024 * 1024),
            None,
            &digest,
            10 * 1024 * 1024,
            true,
        )
        .await
        .unwrap();
    assert_eq!(prepared.size, 5 * 1024 * 1024);

    let outcome = storage.commit_finalize(&prepared).await.unwrap();
    assert_eq!(
        outcome,
        FinalizeOutcome::Published(BlobMeta {
            size: 5 * 1024 * 1024
        })
    );
}

#[tokio::test]
async fn test_s3_begin_finalize_digest_mismatch_abort_true() {
    let (storage, _driver) = create_mock_storage();
    let session = storage.create_session("mismatch_abort_true").await.unwrap();

    let payload = Bytes::from(vec![b'M'; 100 * 1024]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![payload.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    let wrong_digest =
        Digest::parse("sha256:0000000000000000000000000000000000000000000000000000000000000000")
            .unwrap();
    let err = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(100 * 1024),
            None,
            &wrong_digest,
            10 * 1024 * 1024,
            true,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, UploadTransitionError::DigestMismatch { .. }));

    // Session was deleted on abort_true
    assert!(matches!(
        storage.session_status(&session).await,
        Err(UploadTransitionError::NotFound)
    ));
}

#[tokio::test]
async fn test_s3_begin_finalize_digest_mismatch_abort_false() {
    let (storage, _driver) = create_mock_storage();
    let session = storage
        .create_session("mismatch_abort_false")
        .await
        .unwrap();

    let payload = Bytes::from(vec![b'N'; 100 * 1024]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![payload.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    let wrong_digest =
        Digest::parse("sha256:0000000000000000000000000000000000000000000000000000000000000000")
            .unwrap();
    let err = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(100 * 1024),
            None,
            &wrong_digest,
            10 * 1024 * 1024,
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, UploadTransitionError::DigestMismatch { .. }));

    // Session preserved at committed offset
    let status = storage.session_status(&session).await.unwrap();
    assert_eq!(status.committed_offset, 100 * 1024);
}

#[tokio::test]
async fn test_s3_begin_finalize_offset_mismatch() {
    let (storage, _driver) = create_mock_storage();
    let session = storage.create_session("offset_mismatch").await.unwrap();

    let payload = Bytes::from(vec![b'O'; 100 * 1024]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![payload.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    let digest = compute_sha256_digest(&payload);
    let err = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(50 * 1024), // Wrong offset
            None,
            &digest,
            10 * 1024 * 1024,
            true,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, UploadTransitionError::OffsetMismatch { .. }));
}

#[tokio::test]
async fn test_s3_finalize_retry_different_digest_fails() {
    let (storage, _driver) = create_mock_storage();
    let session = storage.create_session("diff_digest_repo").await.unwrap();

    let payload = Bytes::from(vec![b'D'; 100 * 1024]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![payload.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    let digest = compute_sha256_digest(&payload);
    let prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(100 * 1024),
            None,
            &digest,
            10 * 1024 * 1024,
            true,
        )
        .await
        .unwrap();

    storage.commit_finalize(&prepared).await.unwrap();

    // Second begin_finalize with different digest -> already finalized or NotFound
    let diff_digest =
        Digest::parse("sha256:1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let err = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(100 * 1024),
            None,
            &diff_digest,
            10 * 1024 * 1024,
            true,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        UploadTransitionError::NotFound | UploadTransitionError::DigestMismatch { .. }
    ));
}

#[tokio::test]
async fn test_s3_recovery_cas_blob_exists_receipt_absent() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("cas_exists").await.unwrap();

    let payload = Bytes::from(vec![b'C'; 50 * 1024]);
    let digest = compute_sha256_digest(&payload);

    // Put blob in CAS destination
    let dest_key = storage.blob_key2(&digest);
    driver
        .put_object_conditional("test-bucket", &dest_key, payload.clone(), None, None)
        .await
        .unwrap();

    // Delete session doc to simulate crash after blob copy
    let key = format!("uploads/{}/session.json", session.uuid);
    driver.delete_object("test-bucket", &key).await.unwrap();

    // Commit finalize with prepared handle detects CAS blob and writes receipt
    let prepared = PreparedFinalize {
        session: session.clone(),
        operation_id: "op-cas".to_string(),
        expected_digest: digest.clone(),
        committed_offset: 50 * 1024,
        size: 50 * 1024,
    };
    let outcome = storage.commit_finalize(&prepared).await.unwrap();
    assert_eq!(
        outcome,
        FinalizeOutcome::AlreadyFinalized(BlobMeta { size: 50 * 1024 })
    );

    let receipt = storage
        .get_finalized_receipt(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.digest, digest.as_str());
}

#[tokio::test]
async fn test_s3_recovery_staging_intact_completes_finalization() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("staging_intact").await.unwrap();

    let payload = Bytes::from(vec![b'S'; 60 * 1024]);
    storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![payload.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    let digest = compute_sha256_digest(&payload);
    let _prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(60 * 1024),
            None,
            &digest,
            10 * 1024 * 1024,
            true,
        )
        .await
        .unwrap();

    // Crash before commit_finalize. Advance clock and run recover_session
    driver.advance_time(500);
    let recovered = storage.recover_session(&session).await.unwrap();
    assert_eq!(recovered.state, UploadSessionState::Finalizing);

    let receipt = storage
        .get_finalized_receipt(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.digest, digest.as_str());
}

#[tokio::test]
async fn test_s3_recovery_crash_after_finalizing_cas_before_multipart_completion() {
    let (storage, driver) = create_mock_storage();
    let session = storage
        .create_session("crash_before_complete")
        .await
        .unwrap();

    let part_bytes = Bytes::from(vec![b'Z'; 5 * 1024 * 1024]);
    let digest = compute_sha256_digest(&part_bytes);

    // Upload part directly to multipart upload
    let (doc, etag) = storage
        .get_session_doc_with_etag(&session.uuid)
        .await
        .unwrap()
        .unwrap();
    let data_key = storage.multipart_data_key(&session.uuid);
    let part_etag = driver
        .upload_part(
            "test-bucket",
            &data_key,
            &doc.multipart_upload_id,
            1,
            part_bytes.clone(),
        )
        .await
        .unwrap();

    // Simulate crash right after Finalizing CAS: state is Finalizing, multipart_completed is false, staging object does not exist yet
    let now = driver.now_unix_secs();
    let finalizing_doc = S3SessionDoc {
        format_version: 1,
        repo: session.repo.clone(),
        uuid: session.uuid.clone(),
        multipart_upload_id: doc.multipart_upload_id.clone(),
        state: UploadSessionState::Finalizing,
        committed_offset: 5 * 1024 * 1024,
        committed_parts: vec![S3CommittedPart {
            part_number: 1,
            size: 5 * 1024 * 1024,
            etag: part_etag,
        }],
        pending_buffer_key: None,
        pending_bytes: 0,
        created_at_unix_secs: now,
        last_active_at_unix_secs: now,
        current_operation: None,
        finalizing_info: Some(S3FinalizingInfo {
            operation_id: "crash-op-1".to_string(),
            expected_digest: digest.as_str().to_string(),
            size: 5 * 1024 * 1024,
            finalizing_at_unix_secs: now,
            multipart_completed: false,
        }),
    };
    storage
        .put_session_doc_conditional(&session.uuid, &finalizing_doc, Some(&etag))
        .await
        .unwrap();

    // Verify staging object does not exist yet
    assert!(
        driver
            .head_object("test-bucket", &data_key)
            .await
            .unwrap()
            .is_none()
    );

    // Run recovery
    driver.advance_time(500);
    let recovered = storage.recover_session(&session).await.unwrap();
    assert_eq!(recovered.state, UploadSessionState::Finalizing);

    // Receipt is written and CAS destination blob exists with correct content
    let receipt = storage
        .get_finalized_receipt(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.digest, digest.as_str());
    assert_eq!(receipt.size, 5 * 1024 * 1024);

    let cas_key = format!("blobs/sha256/{}/{}", &digest.hex()[..2], digest.hex());
    let (cas_bytes, _) = driver
        .get_object("test-bucket", &cas_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cas_bytes, part_bytes);
}

#[tokio::test]
async fn test_s3_recovery_neither_cas_blob_nor_staging_errors() {
    let (storage, _driver) = create_mock_storage();
    let session = storage.create_session("neither_repo").await.unwrap();

    let digest =
        Digest::parse("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .unwrap();
    let fake_prepared = PreparedFinalize {
        session: session.clone(),
        operation_id: "op-fake".to_string(),
        expected_digest: digest,
        committed_offset: 100,
        size: 100,
    };

    let err = storage.commit_finalize(&fake_prepared).await.unwrap_err();
    assert!(matches!(err, UploadTransitionError::InvalidPreparedHandle));
}

#[tokio::test]
async fn test_s3_reaper_aborts_expired_active_session() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("reap_active").await.unwrap();

    // Advance past expiration
    driver.advance_time(600);

    let reaped = storage.reap_expired_sessions(300, 300).await.unwrap();
    assert_eq!(reaped, 1);

    assert!(matches!(
        storage.session_status(&session).await,
        Err(UploadTransitionError::NotFound)
    ));
}

#[tokio::test]
async fn test_s3_reaper_preserves_young_receipt() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("young_receipt").await.unwrap();

    let payload = Bytes::from(vec![b'Y'; 10 * 1024]);
    let digest = compute_sha256_digest(&payload);

    let prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(0),
            Some(make_test_stream(vec![payload])),
            &digest,
            10 * 1024 * 1024,
            true,
        )
        .await
        .unwrap();
    storage.commit_finalize(&prepared).await.unwrap();

    // Advance clock by 100s (receipt_ttl is 300s)
    driver.advance_time(100);

    let reaped = storage.reap_expired_sessions(300, 300).await.unwrap();
    assert_eq!(reaped, 0);

    let receipt = storage
        .get_finalized_receipt(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.digest, digest.as_str());
}

#[tokio::test]
async fn test_s3_reaper_deletes_expired_receipt() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("old_receipt").await.unwrap();

    let payload = Bytes::from(vec![b'O'; 10 * 1024]);
    let digest = compute_sha256_digest(&payload);

    let prepared = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(0),
            Some(make_test_stream(vec![payload])),
            &digest,
            10 * 1024 * 1024,
            true,
        )
        .await
        .unwrap();
    storage.commit_finalize(&prepared).await.unwrap();

    // Advance clock past receipt TTL
    driver.advance_time(500);

    let reaped = storage.reap_expired_sessions(300, 300).await.unwrap();
    assert_eq!(reaped, 1);

    assert!(
        storage
            .get_finalized_receipt(&session)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_s3_reaper_cleans_orphan_pending_buffers() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("orphan_reap").await.unwrap();

    // Write orphan pending buffer
    let orphan_key = format!("uploads/{}/pending/orphan123.bin", session.uuid);
    driver
        .put_object_conditional(
            "test-bucket",
            &orphan_key,
            Bytes::from_static(b"ORPHAN"),
            None,
            None,
        )
        .await
        .unwrap();

    // Advance time past expiration
    driver.advance_time(600);

    let reaped = storage.reap_expired_sessions(300, 300).await.unwrap();
    assert_eq!(reaped, 1);

    // Orphan pending object was deleted
    assert!(driver.objects.lock().unwrap().get(&orphan_key).is_none());
}

#[tokio::test]
async fn test_s3_appending_lease_recovery() {
    let (storage, driver) = create_mock_storage();
    let session = storage.create_session("appending_recovery").await.unwrap();

    // 1. Session begins an append and acquires lease
    let (mut doc, etag) = storage
        .get_session_doc_with_etag(&session.uuid)
        .await
        .unwrap()
        .unwrap();
    doc.state = UploadSessionState::Appending;
    doc.current_operation = Some(S3CurrentOperation {
        operation_id: "op-stalled-append".to_string(),
        expected_offset: 0,
        target_part_number: 1,
        lease_expires_at_unix_secs: driver.now_unix_secs() + 10,
    });
    storage
        .put_session_doc_conditional(&session.uuid, &doc, Some(&etag))
        .await
        .unwrap();

    // While lease is active, concurrent append is rejected with Conflict
    let conflict_res = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![Bytes::from_static(b"HELLO")]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(conflict_res, UploadAppendResult::Conflict);

    // 2. Advance time past lease expiration
    driver.advance_time(50);

    // 3. Recovery resets state to Active and clears stale lease operation
    let recovered = storage.recover_session(&session).await.unwrap();
    assert_eq!(recovered.state, UploadSessionState::Active);

    let doc_after = storage
        .get_session_doc_with_etag(&session.uuid)
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(doc_after.state, UploadSessionState::Active);
    assert!(doc_after.current_operation.is_none());

    // 4. Now a new append succeeds cleanly
    let append_ok = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![Bytes::from_static(b"SUCCESS")]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(append_ok, UploadAppendResult::Committed { new_offset: 7 });
}

#[tokio::test]
async fn test_s3_raw_multipart_reaper_comprehensive_safety() {
    let (mut storage, driver) = create_mock_storage();
    storage = storage.with_session_config(S3SessionConfig {
        legacy_multipart_cleanup_policy:
            crate::config::LegacyMultipartCleanupPolicy::OperatorConfirmedAllUnknown,
        ..S3SessionConfig::default()
    });
    driver.advance_time(100_000);

    let now = driver.now_unix_secs();
    let cutoff = now.saturating_sub(3600);

    // 1. Create an expired orphan multipart upload under registry prefix (no session doc exists)
    let orphan_uuid = uuid::Uuid::new_v4().to_string();
    let orphan_key = format!("uploads/{orphan_uuid}/multipart.data");
    let orphan_mp_id = driver
        .create_multipart_upload("test-bucket", &orphan_key)
        .await
        .unwrap();
    // Set its initiation time in the past
    driver
        .multiparts
        .lock()
        .unwrap()
        .get_mut(&orphan_mp_id)
        .unwrap()
        .2 = now.saturating_sub(7200);

    // 2. Create an active valid session upload (should NOT be reaped)
    let active_session = storage.create_session("active_repo").await.unwrap();
    let active_doc = storage
        .get_session_doc_with_etag(&active_session.uuid)
        .await
        .unwrap()
        .unwrap()
        .0;

    // 3. Create a multipart upload OUTSIDE the registry prefix (must NEVER be touched)
    let external_key = "other_service/uploads/data.bin";
    let external_mp_id = driver
        .create_multipart_upload("test-bucket", external_key)
        .await
        .unwrap();
    driver
        .multiparts
        .lock()
        .unwrap()
        .get_mut(&external_mp_id)
        .unwrap()
        .2 = now.saturating_sub(7200);

    // 4. Create a young orphan multipart upload under registry prefix (younger than cutoff -> should NOT be reaped)
    let young_uuid = uuid::Uuid::new_v4().to_string();
    let young_key = format!("uploads/{young_uuid}/multipart.data");
    let young_mp_id = driver
        .create_multipart_upload("test-bucket", &young_key)
        .await
        .unwrap();
    driver
        .multiparts
        .lock()
        .unwrap()
        .get_mut(&young_mp_id)
        .unwrap()
        .2 = now.saturating_sub(300);

    // 5. Run reaper
    let reaped_count = storage
        .reap_orphaned_multipart_uploads(cutoff)
        .await
        .unwrap();
    assert_eq!(
        reaped_count, 1,
        "Exactly one expired orphan should be reaped"
    );

    // Verify:
    // - Expired orphan was aborted
    assert!(
        !driver
            .multiparts
            .lock()
            .unwrap()
            .contains_key(&orphan_mp_id)
    );
    // - Active session multipart upload is untouched
    assert!(
        driver
            .multiparts
            .lock()
            .unwrap()
            .contains_key(&active_doc.multipart_upload_id)
    );
    // - Unrelated external multipart upload outside prefix is untouched
    assert!(
        driver
            .multiparts
            .lock()
            .unwrap()
            .contains_key(&external_mp_id)
    );
    // - Young multipart upload is untouched
    assert!(driver.multiparts.lock().unwrap().contains_key(&young_mp_id));

    // Verify adapter observed calls
    let call_log = driver.get_call_log();
    assert!(
        call_log
            .iter()
            .any(|c| c.method == "list_multipart_uploads" && c.key == "uploads/")
    );
    assert!(
        call_log
            .iter()
            .any(|c| c.method == "abort_multipart_upload" && c.key == orphan_key)
    );
    assert!(
        !call_log
            .iter()
            .any(|c| c.method == "abort_multipart_upload" && c.key == external_key),
        "Must never abort multipart upload outside registry-owned prefix"
    );
}

#[tokio::test]
async fn test_s3_all_eight_finalization_crash_boundaries() {
    let (storage, driver) = create_mock_storage();

    // --------------------------------------------------------------------
    // Boundary 1: Before Finalizing CAS
    // Fault injection: inject 412 / failure on put_object when setting state=Finalizing
    // --------------------------------------------------------------------
    let session1 = storage.create_session("boundary1").await.unwrap();
    let chunk1 = Bytes::from(vec![b'1'; 5 * 1024 * 1024]);
    let d1 = compute_sha256_digest(&chunk1);
    storage
        .append_if_offset(
            &session1,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![chunk1.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    let s1_key = format!("uploads/{}/session.json", session1.uuid);
    driver.set_hook_before(move |method, key| {
        if method == "put_object" && key == s1_key {
            Some(StorageError::TagAlreadyExists)
        } else {
            None
        }
    });

    let err1 = storage
        .begin_finalize(
            &session1,
            UploadOffsetPrecondition::Exact(5 * 1024 * 1024),
            None,
            &d1,
            10 * 1024 * 1024,
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err1, UploadTransitionError::Storage(_)));

    // Verify state is still Active
    driver.clear_hooks();
    let status1 = storage.session_status(&session1).await.unwrap();
    assert_eq!(status1.state, UploadSessionState::Active);

    // Retry succeeds
    let prep1 = storage
        .begin_finalize(
            &session1,
            UploadOffsetPrecondition::Exact(5 * 1024 * 1024),
            None,
            &d1,
            10 * 1024 * 1024,
            false,
        )
        .await
        .unwrap();
    storage.commit_finalize(&prep1).await.unwrap();

    // --------------------------------------------------------------------
    // Boundary 2: After Finalizing CAS, before multipart completion
    // Fault injection: complete_multipart_upload fails
    // --------------------------------------------------------------------
    let session2 = storage.create_session("boundary2").await.unwrap();
    let chunk2 = Bytes::from(vec![b'2'; 5 * 1024 * 1024]);
    let d2 = compute_sha256_digest(&chunk2);
    storage
        .append_if_offset(
            &session2,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![chunk2.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    driver.set_hook_before(move |method, _key| {
        if method == "complete_multipart_upload" {
            Some(StorageError::Internal(
                "simulated s3 network cut".to_string(),
            ))
        } else {
            None
        }
    });

    let err2 = storage
        .begin_finalize(
            &session2,
            UploadOffsetPrecondition::Exact(5 * 1024 * 1024),
            None,
            &d2,
            10 * 1024 * 1024,
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err2, UploadTransitionError::Storage(_)));

    driver.clear_hooks();
    let doc2 = storage
        .get_session_doc_with_etag(&session2.uuid)
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(doc2.state, UploadSessionState::Finalizing);
    assert!(!doc2.finalizing_info.as_ref().unwrap().multipart_completed);

    // Recovery / retry successfully completes multipart and publishes CAS blob
    driver.advance_time(500);
    let rec2 = storage.recover_session(&session2).await.unwrap();
    assert_eq!(rec2.state, UploadSessionState::Finalizing);
    let r2 = storage
        .get_finalized_receipt(&session2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r2.digest, d2.as_str());

    // --------------------------------------------------------------------
    // Boundary 3: After multipart completion, before begin_finalize returns
    // Staged object exists, session doc has multipart_completed: true
    // --------------------------------------------------------------------
    let session3 = storage.create_session("boundary3").await.unwrap();
    let chunk3 = Bytes::from(vec![b'3'; 5 * 1024 * 1024]);
    let d3 = compute_sha256_digest(&chunk3);
    storage
        .append_if_offset(
            &session3,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![chunk3.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();

    let _prep3 = storage
        .begin_finalize(
            &session3,
            UploadOffsetPrecondition::Exact(5 * 1024 * 1024),
            None,
            &d3,
            10 * 1024 * 1024,
            false,
        )
        .await
        .unwrap();

    // Verify staging object exists, CAS blob does not exist yet
    let upload_key3 = storage.multipart_data_key(&session3.uuid);
    assert!(
        driver
            .head_object("test-bucket", &upload_key3)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        driver
            .head_object("test-bucket", &storage.blob_key2(&d3))
            .await
            .unwrap()
            .is_none()
    );

    // Recovery drives staging object to CAS blob and receipt
    driver.advance_time(500);
    let rec3 = storage.recover_session(&session3).await.unwrap();
    assert_eq!(rec3.state, UploadSessionState::Finalizing);
    assert!(
        driver
            .head_object("test-bucket", &storage.blob_key2(&d3))
            .await
            .unwrap()
            .is_some()
    );

    // --------------------------------------------------------------------
    // Boundary 4: After coordinator pin acquisition, before CAS publication
    // --------------------------------------------------------------------
    // Tested end-to-end in coordinator integration suite (proves GC cannot delete)

    // --------------------------------------------------------------------
    // Boundary 5: After CAS publication, before receipt persistence
    // Fault injection: copy_object succeeds, but put_object on finalized.json fails
    // --------------------------------------------------------------------
    let session5 = storage.create_session("boundary5").await.unwrap();
    let chunk5 = Bytes::from(vec![b'5'; 5 * 1024 * 1024]);
    let d5 = compute_sha256_digest(&chunk5);
    storage
        .append_if_offset(
            &session5,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![chunk5.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();
    let prep5 = storage
        .begin_finalize(
            &session5,
            UploadOffsetPrecondition::Exact(5 * 1024 * 1024),
            None,
            &d5,
            10 * 1024 * 1024,
            false,
        )
        .await
        .unwrap();

    let f5_key = storage.finalized_key(&session5.uuid);
    driver.set_hook_before(move |method, key| {
        if method == "put_object" && key == f5_key {
            Some(StorageError::Internal(
                "disk full writing receipt".to_string(),
            ))
        } else {
            None
        }
    });

    let err5 = storage.commit_finalize(&prep5).await.unwrap_err();
    assert!(matches!(err5, UploadTransitionError::Storage(_)));

    // State: CAS blob exists, receipt does not
    driver.clear_hooks();
    assert!(
        driver
            .head_object("test-bucket", &storage.blob_key2(&d5))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        storage
            .get_finalized_receipt(&session5)
            .await
            .unwrap()
            .is_none()
    );

    // Recovery / retry detects CAS blob already exists and writes receipt
    driver.advance_time(500);
    let rec5 = storage.recover_session(&session5).await.unwrap();
    assert_eq!(rec5.state, UploadSessionState::Finalizing);
    let r5 = storage
        .get_finalized_receipt(&session5)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r5.digest, d5.as_str());

    // --------------------------------------------------------------------
    // Boundary 6: After receipt persistence, before session cleanup
    // Fault injection: delete_object on session.json fails
    // --------------------------------------------------------------------
    let session6 = storage.create_session("boundary6").await.unwrap();
    let chunk6 = Bytes::from(vec![b'6'; 5 * 1024 * 1024]);
    let d6 = compute_sha256_digest(&chunk6);
    storage
        .append_if_offset(
            &session6,
            UploadOffsetPrecondition::Exact(0),
            make_test_stream(vec![chunk6.clone()]),
            10 * 1024 * 1024,
        )
        .await
        .unwrap();
    let prep6 = storage
        .begin_finalize(
            &session6,
            UploadOffsetPrecondition::Exact(5 * 1024 * 1024),
            None,
            &d6,
            10 * 1024 * 1024,
            false,
        )
        .await
        .unwrap();

    let s6_key = storage.session_key(&session6.uuid);
    driver.set_hook_before(move |method, key| {
        if method == "delete_object" && key == s6_key {
            Some(StorageError::Internal(
                "failed to delete session.json".to_string(),
            ))
        } else {
            None
        }
    });

    // commit_finalize succeeds or completes publication with receipt
    let _ = storage.commit_finalize(&prep6).await;
    driver.clear_hooks();

    // Receipt exists
    let r6 = storage
        .get_finalized_receipt(&session6)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r6.digest, d6.as_str());

    // --------------------------------------------------------------------
    // Boundary 7: After session cleanup, before HTTP response
    // Client retries PUT .../blobs/uploads/<uuid>?digest=...
    // --------------------------------------------------------------------
    let outcome6_retry = storage.commit_finalize(&prep6).await.unwrap();
    assert_eq!(
        outcome6_retry,
        FinalizeOutcome::AlreadyFinalized(BlobMeta {
            size: 5 * 1024 * 1024
        })
    );

    // --------------------------------------------------------------------
    // Boundary 8: Retrying begin_finalize on already finalized session
    // --------------------------------------------------------------------
    let prep6_again = storage
        .begin_finalize(
            &session6,
            UploadOffsetPrecondition::Exact(5 * 1024 * 1024),
            None,
            &d6,
            10 * 1024 * 1024,
            false,
        )
        .await
        .unwrap();
    assert_eq!(prep6_again.operation_id, "already-finalized");
}

#[tokio::test]
async fn test_s3_reaper_fail_closed_on_metadata_read_error() {
    let (mut storage, driver) = create_mock_storage();
    storage = storage.with_session_config(S3SessionConfig {
        legacy_multipart_cleanup_policy:
            crate::config::LegacyMultipartCleanupPolicy::OperatorConfirmedAllUnknown,
        ..S3SessionConfig::default()
    });

    // Create an orphaned multipart upload older than cutoff
    let raw_key = storage.key("uploads/orphan-err-uuid/data");
    let mp_id = driver
        .create_multipart_upload("test-bucket", &raw_key)
        .await
        .unwrap();
    driver.multiparts.lock().unwrap().get_mut(&mp_id).unwrap().2 = 100;

    // Inject S3 error when fetching session.json (e.g. transient 500 / network error)
    let s_key = storage.session_key("orphan-err-uuid");
    driver.set_hook_before(move |method, key| {
        if method == "get_object" && key == s_key {
            Some(StorageError::Internal("transient S3 500 error".to_string()))
        } else {
            None
        }
    });

    // Run orphan reaper with older_than = 200
    let reaped = storage.reap_orphaned_multipart_uploads(200).await.unwrap();
    // FAIL CLOSED: Must NOT abort multipart upload
    assert_eq!(reaped, 0);

    // Verify raw multipart upload still exists and was not aborted
    let res = driver
        .list_multipart_uploads("test-bucket", &storage.key("uploads/"), None, None)
        .await
        .unwrap();
    assert_eq!(res.uploads.len(), 1);
}

#[tokio::test]
async fn test_s3_reaper_legacy_cleanup_policy_modes() {
    let (storage_disabled, driver) = create_mock_storage();
    // Default policy: Disabled
    assert_eq!(
        storage_disabled
            .session_config
            .legacy_multipart_cleanup_policy,
        crate::config::LegacyMultipartCleanupPolicy::Disabled
    );

    // Create raw legacy multipart upload without session.json
    let raw_key = storage_disabled.key("uploads/legacy-uuid/data");
    let mp_id = driver
        .create_multipart_upload("test-bucket", &raw_key)
        .await
        .unwrap();
    driver.multiparts.lock().unwrap().get_mut(&mp_id).unwrap().2 = 100;

    // 1. Under Disabled policy -> 0 reaped
    let reaped = storage_disabled
        .reap_orphaned_multipart_uploads(200)
        .await
        .unwrap();
    assert_eq!(reaped, 0);

    // 2. Under CurrentFormatOnly policy -> 0 reaped (cannot prove current format)
    let storage_current = storage_disabled
        .clone()
        .with_session_config(S3SessionConfig {
            legacy_multipart_cleanup_policy:
                crate::config::LegacyMultipartCleanupPolicy::CurrentFormatOnly,
            ..S3SessionConfig::default()
        });
    let reaped_current = storage_current
        .reap_orphaned_multipart_uploads(200)
        .await
        .unwrap();
    assert_eq!(reaped_current, 0);

    // 3. Under OperatorConfirmedAllUnknown policy -> 1 reaped
    let storage_confirmed = storage_disabled
        .clone()
        .with_session_config(S3SessionConfig {
            legacy_multipart_cleanup_policy:
                crate::config::LegacyMultipartCleanupPolicy::OperatorConfirmedAllUnknown,
            ..S3SessionConfig::default()
        });
    let reaped_confirmed = storage_confirmed
        .reap_orphaned_multipart_uploads(200)
        .await
        .unwrap();
    assert_eq!(reaped_confirmed, 1);

    // Multipart upload was aborted
    let res = driver
        .list_multipart_uploads("test-bucket", &storage_disabled.key("uploads/"), None, None)
        .await
        .unwrap();
    assert!(res.uploads.is_empty());
}

#[tokio::test]
async fn test_s3_reaper_revalidation_race_protects_upload() {
    let (mut storage, driver) = create_mock_storage();
    storage = storage.with_session_config(S3SessionConfig {
        legacy_multipart_cleanup_policy:
            crate::config::LegacyMultipartCleanupPolicy::OperatorConfirmedAllUnknown,
        ..S3SessionConfig::default()
    });

    let raw_key = storage.key("uploads/race-uuid/data");
    let mp_id = driver
        .create_multipart_upload("test-bucket", &raw_key)
        .await
        .unwrap();
    driver.multiparts.lock().unwrap().get_mut(&mp_id).unwrap().2 = 100;

    // Simulate a race where during the second (pre-abort) check, a session doc appears
    let lookups = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let lookups_clone = Arc::clone(&lookups);
    let driver_clone = Arc::clone(&driver);
    let s_key = storage.session_key("race-uuid");

    driver.set_hook_before(move |method, key| {
        if method == "get_object" && key == s_key {
            let count = lookups_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if count == 0 {
                // First lookup: Not found
                None
            } else {
                // Second lookup: A session doc was created concurrently!
                let doc = S3SessionDoc {
                    format_version: 1,
                    state: UploadSessionState::Active,
                    repo: crate::registry::canonical_name::CanonicalRepoName::parse("race-repo")
                        .unwrap(),
                    uuid: "race-uuid".into(),
                    created_at_unix_secs: 100,
                    last_active_at_unix_secs: 150,
                    multipart_upload_id: "race-upload-id".into(),
                    committed_offset: 0,
                    committed_parts: vec![],
                    pending_buffer_key: None,
                    pending_bytes: 0,
                    current_operation: None,
                    finalizing_info: None,
                };
                let bytes = Bytes::from(serde_json::to_vec(&doc).unwrap());
                driver_clone
                    .objects
                    .lock()
                    .unwrap()
                    .insert(s_key.clone(), (bytes, "\"etag\"".to_string()));
                None
            }
        } else {
            None
        }
    });

    let reaped = storage.reap_orphaned_multipart_uploads(200).await.unwrap();
    // Abort must be skipped because second check detected the new session doc
    assert_eq!(reaped, 0);

    // Upload was NOT aborted
    let res = driver
        .list_multipart_uploads("test-bucket", &storage.key("uploads/"), None, None)
        .await
        .unwrap();
    assert_eq!(res.uploads.len(), 1);
}

#[tokio::test]
async fn test_s3_reaper_error_matrix_all_non_not_found_fail_closed() {
    let test_cases = vec![
        (
            "timeout",
            StorageError::Internal("RequestTimeout: connection timed out".into()),
        ),
        (
            "throttling",
            StorageError::Internal("SlowDown: Please reduce your request rate".into()),
        ),
        (
            "access_denied",
            StorageError::Internal("AccessDenied: 403 Forbidden".into()),
        ),
        (
            "internal_error",
            StorageError::Internal("InternalError: 500 Internal Server Error".into()),
        ),
    ];

    for (name, error) in test_cases {
        let (mut storage, driver) = create_mock_storage();
        storage = storage.with_session_config(S3SessionConfig {
            legacy_multipart_cleanup_policy:
                crate::config::LegacyMultipartCleanupPolicy::OperatorConfirmedAllUnknown,
            ..S3SessionConfig::default()
        });

        let raw_key = storage.key(&format!("uploads/orphan-{name}/data"));
        let mp_id = driver
            .create_multipart_upload("test-bucket", &raw_key)
            .await
            .unwrap();
        driver.multiparts.lock().unwrap().get_mut(&mp_id).unwrap().2 = 100;

        let s_key = storage.session_key(&format!("orphan-{name}"));
        let err_clone = match &error {
            StorageError::Internal(msg) => StorageError::Internal(msg.clone()),
            _ => StorageError::Internal("error".into()),
        };

        driver.set_hook_before(move |method, key| {
            if method == "get_object" && key == s_key {
                Some(match &err_clone {
                    StorageError::Internal(m) => StorageError::Internal(m.clone()),
                    _ => StorageError::Internal("err".into()),
                })
            } else {
                None
            }
        });

        let reaped = storage.reap_orphaned_multipart_uploads(200).await.unwrap();
        assert_eq!(
            reaped, 0,
            "Error case {name} must fail closed and produce 0 aborts"
        );

        // Verify upload was NOT aborted
        let res = driver
            .list_multipart_uploads("test-bucket", &storage.key("uploads/"), None, None)
            .await
            .unwrap();
        assert_eq!(
            res.uploads.len(),
            1,
            "Upload for {name} must remain present"
        );
    }
}

#[tokio::test]
async fn test_s3_reaper_malformed_session_json_fails_closed() {
    let (mut storage, driver) = create_mock_storage();
    storage = storage.with_session_config(S3SessionConfig {
        legacy_multipart_cleanup_policy:
            crate::config::LegacyMultipartCleanupPolicy::OperatorConfirmedAllUnknown,
        ..S3SessionConfig::default()
    });

    let raw_key = storage.key("uploads/orphan-malformed/data");
    let mp_id = driver
        .create_multipart_upload("test-bucket", &raw_key)
        .await
        .unwrap();
    driver.multiparts.lock().unwrap().get_mut(&mp_id).unwrap().2 = 100;

    // Put malformed JSON in session.json
    let s_key = storage.session_key("orphan-malformed");
    driver.objects.lock().unwrap().insert(
        s_key.clone(),
        (
            Bytes::from_static(b"THIS_IS_NOT_VALID_JSON{:::"),
            "\"etag\"".into(),
        ),
    );

    let reaped = storage.reap_orphaned_multipart_uploads(200).await.unwrap();
    // Malformed session doc must fail closed (0 aborts)
    assert_eq!(reaped, 0);

    let res = driver
        .list_multipart_uploads("test-bucket", &storage.key("uploads/"), None, None)
        .await
        .unwrap();
    assert_eq!(res.uploads.len(), 1);
}

#[tokio::test]
async fn test_s3_reaper_pre_abort_revalidation_error_fails_closed() {
    let (mut storage, driver) = create_mock_storage();
    storage = storage.with_session_config(S3SessionConfig {
        legacy_multipart_cleanup_policy:
            crate::config::LegacyMultipartCleanupPolicy::OperatorConfirmedAllUnknown,
        ..S3SessionConfig::default()
    });

    let raw_key = storage.key("uploads/orphan-preabort-err/data");
    let mp_id = driver
        .create_multipart_upload("test-bucket", &raw_key)
        .await
        .unwrap();
    driver.multiparts.lock().unwrap().get_mut(&mp_id).unwrap().2 = 100;

    let lookups = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let lookups_clone = Arc::clone(&lookups);
    let s_key = storage.session_key("orphan-preabort-err");

    driver.set_hook_before(move |method, key| {
        if method == "get_object" && key == s_key {
            let count = lookups_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if count == 0 {
                // First lookup: Not found (proceeds toward abort)
                None
            } else {
                // Second (pre-abort) lookup: Transient error!
                Some(StorageError::Internal(
                    "S3 500 during pre-abort check".into(),
                ))
            }
        } else {
            None
        }
    });

    let reaped = storage.reap_orphaned_multipart_uploads(200).await.unwrap();
    // Must fail closed when pre-abort revalidation errors
    assert_eq!(reaped, 0);

    let res = driver
        .list_multipart_uploads("test-bucket", &storage.key("uploads/"), None, None)
        .await
        .unwrap();
    assert_eq!(res.uploads.len(), 1);
}

#[tokio::test]
async fn test_s3_membership_two_concurrent_link_operations_are_idempotent() {
    let (storage, _driver) = create_mock_storage();
    let digest =
        Digest::parse("sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
            .unwrap();
    let rec1 = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
        crate::registry::canonical_name::CanonicalRepoName::parse("repo-a").unwrap(),
        digest.clone(),
        Some("uuid-1".into()),
    );
    let rec2 = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
        crate::registry::canonical_name::CanonicalRepoName::parse("repo-a").unwrap(),
        digest.clone(),
        Some("uuid-2".into()),
    );

    // Both link calls succeed idempotently
    storage.link_repo_blob(&rec1).await.unwrap();
    storage.link_repo_blob(&rec2).await.unwrap();

    let fetched = storage
        .get_repo_blob_membership("repo-a", &digest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.repo.as_str(), "repo-a");
    assert_eq!(fetched.digest, digest);
}

#[tokio::test]
async fn test_s3_membership_candidate_transition_racing_activation_fails_safe_on_412() {
    let (storage, driver) = create_mock_storage();
    let digest =
        Digest::parse("sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
            .unwrap();
    let rec = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
        crate::registry::canonical_name::CanonicalRepoName::parse("racing-repo").unwrap(),
        digest.clone(),
        None,
    );
    storage.link_repo_blob(&rec).await.unwrap();

    // Inject hook to simulate concurrent modification (etag change) during set_candidate
    let canonical_racing =
        crate::registry::canonical_name::CanonicalRepoName::parse("racing-repo").unwrap();
    let key = storage.repo_blob_key(&canonical_racing, &digest);
    let key_clone = key.clone();
    driver.set_hook_before(move |method, k| {
        if method == "put_object" && k == key_clone {
            Some(StorageError::Internal(
                "412 PreconditionFailed: ETag mismatch".into(),
            ))
        } else {
            None
        }
    });

    let res = storage
        .set_membership_candidate("racing-repo", &digest, 1740000000)
        .await;
    assert_eq!(
        res.unwrap(),
        false,
        "412 ETag mismatch on candidate transition must fail-safe returning Ok(false)"
    );
}

#[tokio::test]
async fn test_s3_membership_corrupt_record_fails_closed() {
    let (storage, driver) = create_mock_storage();
    let digest =
        Digest::parse("sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
            .unwrap();
    let canonical_corrupt =
        crate::registry::canonical_name::CanonicalRepoName::parse("corrupt-repo").unwrap();
    let key = storage.repo_blob_key(&canonical_corrupt, &digest);

    // Put invalid JSON in the membership key
    driver.objects.lock().unwrap().insert(
        key.clone(),
        (Bytes::from_static(b"NOT_VALID_JSON{:::"), "\"etag\"".into()),
    );

    let res = storage
        .get_repo_blob_membership("corrupt-repo", &digest)
        .await;
    assert!(
        res.is_err(),
        "Corrupt membership JSON must fail closed with error"
    );
}

#[tokio::test]
async fn test_s3_membership_pagination_bounded_and_consistent() {
    let (storage, _driver) = create_mock_storage();
    let repo = "paged-repo";

    let d_sha256 =
        Digest::parse("sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
            .unwrap();
    let d_sha512 = Digest::parse("sha512:ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f").unwrap();

    let rec1 = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
        crate::registry::canonical_name::CanonicalRepoName::parse(repo).unwrap(),
        d_sha256.clone(),
        None,
    );
    let rec2 = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
        crate::registry::canonical_name::CanonicalRepoName::parse(repo).unwrap(),
        d_sha512.clone(),
        None,
    );

    storage.link_repo_blob(&rec1).await.unwrap();
    storage.link_repo_blob(&rec2).await.unwrap();

    // Page size 1
    let (p1, next_tok) = storage
        .list_repo_blob_memberships_page(repo, None, 1)
        .await
        .unwrap();
    assert_eq!(p1.len(), 1);
    assert!(next_tok.is_some());

    let (p2, next_tok2) = storage
        .list_repo_blob_memberships_page(repo, next_tok.as_deref(), 1)
        .await
        .unwrap();
    assert_eq!(p2.len(), 1);
    assert!(next_tok2.is_none());

    assert_ne!(p1[0].digest, p2[0].digest);
}

#[tokio::test]
async fn test_s3_membership_repo_prefix_encoding_isolation() {
    let (storage, _driver) = create_mock_storage();
    let digest =
        Digest::parse("sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
            .unwrap();

    // Repo "foo" vs Repo "foo/bar" vs Repo "foo-bar"
    let rec_foo = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
        crate::registry::canonical_name::CanonicalRepoName::parse("foo").unwrap(),
        digest.clone(),
        None,
    );
    let rec_foo_bar = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
        crate::registry::canonical_name::CanonicalRepoName::parse("foo/bar").unwrap(),
        digest.clone(),
        None,
    );
    let rec_foo_dash = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
        crate::registry::canonical_name::CanonicalRepoName::parse("foo-bar").unwrap(),
        digest.clone(),
        None,
    );

    storage.link_repo_blob(&rec_foo).await.unwrap();
    storage.link_repo_blob(&rec_foo_bar).await.unwrap();
    storage.link_repo_blob(&rec_foo_dash).await.unwrap();

    let (p_foo, _) = storage
        .list_repo_blob_memberships_page("foo", None, 10)
        .await
        .unwrap();
    let (p_bar, _) = storage
        .list_repo_blob_memberships_page("foo/bar", None, 10)
        .await
        .unwrap();
    let (p_dash, _) = storage
        .list_repo_blob_memberships_page("foo-bar", None, 10)
        .await
        .unwrap();

    assert_eq!(p_foo.len(), 1);
    assert_eq!(p_bar.len(), 1);
    assert_eq!(p_dash.len(), 1);
    assert_eq!(p_foo[0].repo.as_str(), "foo");
    assert_eq!(p_bar[0].repo.as_str(), "foo/bar");
    assert_eq!(p_dash[0].repo.as_str(), "foo-bar");
}

#[tokio::test]
async fn test_s3_migration_plan_performs_zero_writes() {
    let (storage, driver) = create_mock_storage();
    let storage_arc = Arc::new(storage);

    let stats = crate::membership_migration::plan_membership_migration(&storage_arc)
        .await
        .expect("plan");
    assert_eq!(stats.repositories_scanned, 0);

    // Verify driver object store is completely empty
    let objects = driver.objects.lock().unwrap();
    assert!(
        objects.is_empty(),
        "Plan must perform zero writes to S3 object store"
    );
}

#[tokio::test]
async fn test_s3_migration_conditional_state_acquisition_and_lease_renewal() {
    let (storage, _driver) = create_mock_storage();
    let storage_arc = Arc::new(storage);

    // First apply acquires lease and succeeds
    let stats = crate::membership_migration::apply_membership_migration(&storage_arc)
        .await
        .expect("apply");
    assert_eq!(stats.repositories_scanned, 0);

    // Checkpoint is Ready
    let ready = storage_arc.is_membership_ready().await.unwrap();
    assert!(ready);
}

#[tokio::test]
async fn test_s3_migration_concurrent_owner_rejection() {
    let (storage, _driver) = create_mock_storage();
    let storage_arc = Arc::new(storage);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let active_lease = crate::storage::repo_membership::MigrationCheckpointRecord {
        schema_version: 1,
        phase: crate::storage::repo_membership::MigrationPhase::Applying,
        owner_id: Some("migrator-owner-A".to_string()),
        lease_expiry_unix_secs: Some(now + 3600),
        source_continuation_token: None,
        current_repository: None,
        current_cursor: None,
        stats: crate::storage::repo_membership::MigrationStats::default(),
        started_unix_secs: now,
        last_updated_unix_secs: now,
        failure_info: None,
        verification_result: None,
    };
    storage_arc
        .save_migration_checkpoint(&active_lease)
        .await
        .unwrap();

    // Second owner attempts apply -> fails closed
    let res = crate::membership_migration::apply_membership_migration(&storage_arc).await;
    assert!(res.is_err(), "Must reject concurrent migrator");
}

#[tokio::test]
async fn test_s3_migration_interrupted_apply_and_cursor_resume() {
    let (storage, _driver) = create_mock_storage();
    let storage_arc = Arc::new(storage);

    let now = 10000;
    // Pre-seed checkpoint with completed token "repo-a"
    let cp = crate::storage::repo_membership::MigrationCheckpointRecord {
        schema_version: 1,
        phase: crate::storage::repo_membership::MigrationPhase::Applying,
        owner_id: None,
        lease_expiry_unix_secs: None,
        source_continuation_token: Some("repo-a".to_string()),
        current_repository: None,
        current_cursor: None,
        stats: crate::storage::repo_membership::MigrationStats::default(),
        started_unix_secs: now,
        last_updated_unix_secs: now,
        failure_info: None,
        verification_result: None,
    };
    storage_arc.save_migration_checkpoint(&cp).await.unwrap();

    // Apply resumes from cursor and finishes
    let res = crate::membership_migration::apply_membership_migration(&storage_arc).await;
    assert!(res.is_ok());
    assert!(storage_arc.is_membership_ready().await.unwrap());
}

#[tokio::test]
async fn test_s3_no_normal_membership_op_reads_or_deletes_legacy_markers() {
    let (storage, driver) = create_mock_storage();
    let digest =
        Digest::parse("sha256:4444444444444444444444444444444444444444444444444444444444444444")
            .unwrap();
    let repo = "legacy-test-repo";

    // Seed a legacy key directly in S3 driver
    let legacy_key = format!("repos/{repo}/blobs/sha256/{}.json", digest.hex());
    let legacy_payload = serde_json::json!({
        "schema_version": 1,
        "repo": repo,
        "digest": digest.to_string(),
        "created_at_unix_secs": 1000,
        "provenance": { "type": "upload" },
        "format_version": 1
    });
    driver.objects.lock().unwrap().insert(
        format!("test-bucket/{legacy_key}"),
        (
            Bytes::from(serde_json::to_vec(&legacy_payload).unwrap()),
            "etag-1".to_string(),
        ),
    );

    // Normal get_repo_blob_membership must return None (no silent fallback or auto-migration)
    let mem = storage
        .get_repo_blob_membership(repo, &digest)
        .await
        .unwrap();
    assert!(mem.is_none(), "Normal read must not query legacy key");

    // Normal unlink must NOT delete legacy key
    let unlinked = storage.unlink_repo_blob(repo, &digest).await.unwrap();
    assert!(unlinked);

    // Legacy key must remain intact in driver
    let objects = driver.objects.lock().unwrap();
    assert!(
        objects.contains_key(&format!("test-bucket/{legacy_key}")),
        "Normal unlink must not delete legacy key"
    );
}

#[tokio::test]
async fn test_s3_cas_enumeration_fails_closed_on_malformed_key() {
    let (storage, driver) = create_mock_storage();

    // Insert a malformed CAS key directly into S3
    driver.objects.lock().unwrap().insert(
        "blobs/sha256/invalid_key_format".to_string(),
        (Bytes::from_static(b"data"), "etag-bad".to_string()),
    );

    let res = storage.list_cas_blobs_page(None, 100).await;
    assert!(
        res.is_err(),
        "S3 enumeration must fail closed on malformed key format"
    );
    assert!(matches!(res.unwrap_err(), StorageError::Internal(_)));
}
