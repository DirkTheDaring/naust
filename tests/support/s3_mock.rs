#![allow(dead_code)]

use async_trait::async_trait;
use bytes::Bytes;
use registry_rust::storage::s3::{
    S3BucketVersioningState, S3Driver, S3MultipartListResult, S3MultipartUploadSummary,
    S3ObjectSummary, S3ObjectsPage, S3Storage,
};
use registry_rust::storage::{ConditionalDeleteResult, StorageError};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

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
                if let Some(tok) = continuation_token
                    && k.as_str() <= tok
                {
                    continue;
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
