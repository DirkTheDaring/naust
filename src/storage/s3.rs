use super::upload_session::*;
use super::{
    BlobMeta, ManifestMeta, ReferrerDescriptor, RepoTimestamps, Storage, StorageError, UploadMeta,
};
use crate::registry::digest::Digest;
use crate::storage::repo_membership::RepositoryBlobMembershipStorage;
use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use base64::Engine as _;
use bytes::Bytes;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncRead;
use tokio::sync::{Mutex, OnceCell};

const REFERRER_SHARDS: usize = 64;

fn shard_index(key: &str, num_shards: usize) -> usize {
    let mut hasher = std::hash::DefaultHasher::new();
    std::hash::Hash::hash(key, &mut hasher);
    std::hash::Hasher::finish(&hasher) as usize % num_shards
}

#[derive(Debug, Clone)]
pub struct S3ObjectSummary {
    pub key: String,
    #[allow(dead_code)]
    pub size: u64,
    pub last_modified_unix_secs: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct S3MultipartUploadSummary {
    pub key: String,
    pub upload_id: String,
    pub initiated_at_unix_secs: u64,
}

#[derive(Debug, Clone, Default)]
pub struct S3MultipartListResult {
    pub uploads: Vec<S3MultipartUploadSummary>,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
    pub is_truncated: bool,
}

#[async_trait]
pub trait S3Driver: Send + Sync + 'static {
    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<String, StorageError>;
    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: Bytes,
    ) -> Result<String, StorageError>;
    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<(), StorageError>;
    async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageError>;
    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<S3MultipartListResult, StorageError>;

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<(Bytes, String)>, StorageError>;
    async fn head_object(&self, bucket: &str, key: &str) -> Result<Option<u64>, StorageError>;
    async fn put_object_conditional(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        if_match: Option<String>,
        if_none_match: Option<String>,
    ) -> Result<String, StorageError>;
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StorageError>;
    async fn delete_object_conditional(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<String>,
    ) -> Result<super::ConditionalDeleteResult, StorageError>;
    async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Result<(), StorageError>;
    async fn list_objects_v2(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<S3ObjectSummary>, StorageError>;

    fn now_unix_secs(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

pub struct AwsS3Driver {
    endpoint: Option<String>,
    region: Option<String>,
    client: OnceCell<Client>,
}

impl AwsS3Driver {
    pub fn new(endpoint: Option<String>, region: Option<String>) -> Self {
        Self {
            endpoint,
            region,
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> Result<Client, StorageError> {
        let region_str = self
            .region
            .as_deref()
            .ok_or_else(|| StorageError::Internal("STORAGE_S3_REGION is required".to_string()))?;
        let c = self
            .client
            .get_or_try_init(|| async {
                let loader = aws_config::defaults(BehaviorVersion::latest())
                    .region(Region::new(region_str.to_string()));
                let shared = loader.load().await;

                let mut builder = aws_sdk_s3::config::Builder::from(&shared);
                if let Some(ep) = self.endpoint.as_deref() {
                    builder = builder.endpoint_url(ep);
                    builder = builder.force_path_style(true);
                }
                Ok::<_, StorageError>(Client::from_conf(builder.build()))
            })
            .await?;
        Ok(c.clone())
    }
}

#[async_trait]
impl S3Driver for AwsS3Driver {
    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<String, StorageError> {
        let client = self.client().await?;
        let resp = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;
        resp.upload_id()
            .map(|s| s.to_string())
            .ok_or_else(|| StorageError::Internal("missing upload_id".to_string()))
    }

    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: Bytes,
    ) -> Result<String, StorageError> {
        let client = self.client().await?;
        let resp = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;
        Ok(resp.e_tag().unwrap_or("").trim_matches('"').to_string())
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<(), StorageError> {
        let client = self.client().await?;
        let completed_parts: Vec<CompletedPart> = parts
            .into_iter()
            .map(|(num, etag)| {
                CompletedPart::builder()
                    .set_part_number(Some(num))
                    .set_e_tag(Some(etag))
                    .build()
            })
            .collect();
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();
        client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(upload)
            .send()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;
        Ok(())
    }

    async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageError> {
        let client = self.client().await?;
        let _ = client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        Ok(())
    }

    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<S3MultipartListResult, StorageError> {
        let client = self.client().await?;
        let mut req = client
            .list_multipart_uploads()
            .bucket(bucket)
            .prefix(prefix);
        if let Some(km) = key_marker {
            req = req.key_marker(km);
        }
        if let Some(uim) = upload_id_marker {
            req = req.upload_id_marker(uim);
        }
        let resp = req
            .send()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;

        let uploads = resp
            .uploads()
            .iter()
            .map(|u| {
                let initiated = u
                    .initiated()
                    .and_then(|t| t.to_millis().ok())
                    .map(|ms| (ms / 1000) as u64)
                    .unwrap_or(0);
                S3MultipartUploadSummary {
                    key: u.key().unwrap_or("").to_string(),
                    upload_id: u.upload_id().unwrap_or("").to_string(),
                    initiated_at_unix_secs: initiated,
                }
            })
            .collect();

        Ok(S3MultipartListResult {
            uploads,
            next_key_marker: resp.next_key_marker().map(ToString::to_string),
            next_upload_id_marker: resp.next_upload_id_marker().map(ToString::to_string),
            is_truncated: resp.is_truncated().unwrap_or(false),
        })
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<(Bytes, String)>, StorageError> {
        let client = self.client().await?;
        let resp = match client.get_object().bucket(bucket).key(key).send().await {
            Ok(r) => r,
            Err(err) => {
                let s3_err = map_s3_err(err);
                if matches!(s3_err, StorageError::NotFound) {
                    return Ok(None);
                }
                return Err(s3_err);
            }
        };
        let etag = resp.e_tag().unwrap_or("").trim_matches('"').to_string();
        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .into_bytes();
        Ok(Some((bytes, etag)))
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<Option<u64>, StorageError> {
        let client = self.client().await?;
        match client.head_object().bucket(bucket).key(key).send().await {
            Ok(resp) => Ok(Some(resp.content_length().unwrap_or(0) as u64)),
            Err(err) => {
                let s3_err = map_head_err(err);
                if matches!(s3_err, StorageError::NotFound) {
                    Ok(None)
                } else {
                    Err(s3_err)
                }
            }
        }
    }

    async fn put_object_conditional(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        if_match: Option<String>,
        if_none_match: Option<String>,
    ) -> Result<String, StorageError> {
        let client = self.client().await?;
        let mut req = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body));

        if let Some(ref m) = if_match {
            req = req.if_match(format!("\"{}\"", m.trim_matches('"')));
        }
        if let Some(ref nm) = if_none_match {
            req = req.if_none_match(nm);
        }

        let resp = match req.send().await {
            Ok(r) => r,
            Err(err) => {
                let err_str = err.to_string();
                if err_str.contains("PreconditionFailed")
                    || err_str.contains("AtLeastOnePreconditionFailed")
                    || err_str.contains("412")
                {
                    return Err(StorageError::TagAlreadyExists);
                }
                return Err(StorageError::Internal(err_str));
            }
        };
        Ok(resp.e_tag().unwrap_or("").trim_matches('"').to_string())
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        let client = self.client().await?;
        let _ = client.delete_object().bucket(bucket).key(key).send().await;
        Ok(())
    }

    async fn delete_object_conditional(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<String>,
    ) -> Result<super::ConditionalDeleteResult, StorageError> {
        let client = self.client().await?;
        let mut req = client.delete_object().bucket(bucket).key(key);
        if let Some(ref m) = if_match {
            req = req.if_match(m);
        }
        match req.send().await {
            Ok(_) => Ok(super::ConditionalDeleteResult::Deleted),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("PreconditionFailed")
                    || err_str
                        .contains("At least one of the pre-conditions you specified did not hold")
                    || err_str.contains("412")
                {
                    let current = match self.get_object(bucket, key).await {
                        Ok(Some((_, etag))) => Some(etag),
                        _ => None,
                    };
                    Ok(super::ConditionalDeleteResult::PreconditionFailed {
                        current_version: current,
                    })
                } else if err_str.contains("NoSuchKey") || err_str.contains("404") {
                    Ok(super::ConditionalDeleteResult::NotFound)
                } else {
                    Err(StorageError::Internal(err_str))
                }
            }
        }
    }

    async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Result<(), StorageError> {
        let client = self.client().await?;
        client
            .copy_object()
            .bucket(dst_bucket)
            .key(dst_key)
            .copy_source(format!("{src_bucket}/{src_key}"))
            .send()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn list_objects_v2(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<S3ObjectSummary>, StorageError> {
        let client = self.client().await?;
        let mut token: Option<String> = None;
        let mut out = Vec::new();

        loop {
            let mut req = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(prefix.to_string());
            if let Some(t) = token.as_deref() {
                req = req.continuation_token(t);
            }
            let resp = req
                .send()
                .await
                .map_err(|err| StorageError::Internal(err.to_string()))?;

            for obj in resp.contents() {
                if let Some(k) = obj.key() {
                    let size = obj.size().unwrap_or(0) as u64;
                    let last_modified_unix_secs = obj
                        .last_modified()
                        .map(|dt| dt.secs().max(0) as u64)
                        .unwrap_or(0);
                    out.push(S3ObjectSummary {
                        key: k.to_string(),
                        size,
                        last_modified_unix_secs,
                    });
                }
            }

            if resp.is_truncated().unwrap_or(false) {
                token = resp.next_continuation_token().map(|s| s.to_string());
                if token.is_none() {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(out)
    }
}

#[derive(Clone, Debug)]
pub struct S3SessionConfig {
    pub lease_duration_secs: u64,
    pub lease_renewal_interval_secs: u64,
    pub max_retry_attempts: u32,
    pub receipt_lifetime_secs: u64,
    pub upload_expiration_secs: u64,
    pub legacy_multipart_cleanup_policy: crate::config::LegacyMultipartCleanupPolicy,
}

impl Default for S3SessionConfig {
    fn default() -> Self {
        Self {
            lease_duration_secs: S3_LEASE_DURATION_SECS,
            lease_renewal_interval_secs: S3_LEASE_RENEWAL_INTERVAL_SECS,
            max_retry_attempts: S3_MAX_RETRY_ATTEMPTS,
            receipt_lifetime_secs: 72 * 3600,
            upload_expiration_secs: 24 * 3600,
            legacy_multipart_cleanup_policy: crate::config::LegacyMultipartCleanupPolicy::Disabled,
        }
    }
}

#[derive(Clone)]
pub struct S3Storage {
    bucket: Option<String>,
    prefix: String,
    max_upload_bytes: u64,
    pub session_config: S3SessionConfig,
    driver: Arc<dyn S3Driver>,
    referrer_locks: Arc<Vec<Mutex<()>>>,
}

impl std::fmt::Debug for S3Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Storage")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("max_upload_bytes", &self.max_upload_bytes)
            .field("session_config", &self.session_config)
            .finish()
    }
}

impl S3Storage {
    pub fn new(
        endpoint: Option<String>,
        region: Option<String>,
        bucket: Option<String>,
        prefix: String,
        max_upload_bytes: u64,
    ) -> Self {
        let driver = Arc::new(AwsS3Driver::new(endpoint, region));
        let mut referrer_locks = Vec::with_capacity(REFERRER_SHARDS);
        for _ in 0..REFERRER_SHARDS {
            referrer_locks.push(Mutex::new(()));
        }
        Self {
            bucket,
            prefix,
            max_upload_bytes,
            session_config: S3SessionConfig::default(),
            driver,
            referrer_locks: Arc::new(referrer_locks),
        }
    }

    #[allow(dead_code)]
    pub fn new_with_driver(
        bucket: Option<String>,
        prefix: String,
        max_upload_bytes: u64,
        driver: Arc<dyn S3Driver>,
    ) -> Self {
        let mut referrer_locks = Vec::with_capacity(REFERRER_SHARDS);
        for _ in 0..REFERRER_SHARDS {
            referrer_locks.push(Mutex::new(()));
        }
        Self {
            bucket,
            prefix,
            max_upload_bytes,
            session_config: S3SessionConfig::default(),
            driver,
            referrer_locks: Arc::new(referrer_locks),
        }
    }

    pub fn with_session_config(mut self, session_config: S3SessionConfig) -> Self {
        self.session_config = session_config;
        self
    }

    fn referrer_lock_shard(&self, name: &str, subject: &Digest) -> &Mutex<()> {
        let key = format!("{name}:{}", subject.hex());
        let idx = shard_index(&key, REFERRER_SHARDS);
        &self.referrer_locks[idx]
    }

    fn bucket(&self) -> Result<&str, StorageError> {
        self.bucket
            .as_deref()
            .ok_or_else(|| StorageError::Internal("STORAGE_S3_BUCKET is required".to_string()))
    }

    fn key(&self, suffix: &str) -> String {
        let p = self.prefix.trim_matches('/');
        if p.is_empty() {
            suffix.trim_start_matches('/').to_string()
        } else {
            format!("{p}/{}", suffix.trim_start_matches('/'))
        }
    }

    fn blob_key2(&self, digest: &Digest) -> String {
        self.key(&format!(
            "blobs/{}/{}/{}",
            digest.algorithm(),
            digest.prefix2(),
            digest.hex()
        ))
    }

    fn manifest_key(&self, name: &str, digest: &Digest) -> String {
        self.key(&format!("repos/{name}/manifests/{}", digest.hex()))
    }

    fn tag_key(&self, name: &str, tag: &str) -> String {
        self.key(&format!("repos/{name}/tags/{tag}"))
    }

    fn repo_blob_key(&self, name: &str, digest: &Digest) -> String {
        self.key(&crate::storage::repo_membership::canonical_repo_membership_relpath(name, digest))
    }

    fn repo_blobs_prefix(&self, name: &str) -> String {
        self.key(&crate::storage::repo_membership::canonical_repo_membership_prefix(name))
    }

    fn all_memberships_prefix(&self) -> String {
        self.key(crate::storage::repo_membership::canonical_all_memberships_prefix())
    }

    fn referrers_key(&self, name: &str, subject: &Digest) -> String {
        self.key(&format!("repos/{name}/referrers/{}.json", subject.hex()))
    }

    fn tags_prefix(&self, name: &str) -> String {
        self.key(&format!("repos/{name}/tags/"))
    }

    fn upload_key(&self, base_uuid: &str) -> String {
        self.key(&format!("uploads/{base_uuid}.data"))
    }

    fn session_key(&self, uuid: &str) -> String {
        self.key(&format!("uploads/{uuid}/session.json"))
    }

    pub async fn reap_orphaned_multipart_uploads(
        &self,
        older_than_unix_secs: u64,
    ) -> Result<usize, StorageError> {
        let bucket = self.bucket()?;
        let staging_prefix = self.key("uploads/");
        let mut count = 0;
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;

        loop {
            let res = self
                .driver
                .list_multipart_uploads(
                    bucket,
                    &staging_prefix,
                    key_marker.as_deref(),
                    upload_id_marker.as_deref(),
                )
                .await?;

            for u in &res.uploads {
                // Safety restriction: Must strictly start with our staging prefix
                if !u.key.starts_with(&staging_prefix) {
                    continue;
                }

                // Check age cutoff
                if u.initiated_at_unix_secs >= older_than_unix_secs {
                    continue;
                }

                // Extract UUID: uploads/<uuid>/...
                let rel = u.key.strip_prefix(&staging_prefix).unwrap_or("");
                let uuid = rel.split('/').next().unwrap_or("");
                if uuid.is_empty() {
                    continue;
                }

                // Check session document state explicitly distinguishing Ok(Some), Ok(None), and Err
                match self.get_session_doc_with_etag(uuid).await {
                    Ok(Some((doc, _))) => {
                        let now = self.driver.now_unix_secs();
                        let is_active = match doc.state {
                            UploadSessionState::Active => {
                                now.saturating_sub(doc.last_active_at_unix_secs)
                                    < self.session_config.upload_expiration_secs
                            }
                            UploadSessionState::Appending | UploadSessionState::Finalizing => doc
                                .current_operation
                                .as_ref()
                                .map(|op| now <= op.lease_expires_at_unix_secs)
                                .unwrap_or(false),
                        };
                        if is_active {
                            continue;
                        }
                        // Non-active session documents will be handled by the authoritative session reaper
                        continue;
                    }
                    Ok(None) => {
                        // No session doc exists. Evaluate against explicit cleanup policy.
                        match self.session_config.legacy_multipart_cleanup_policy {
                            crate::config::LegacyMultipartCleanupPolicy::Disabled => {
                                tracing::debug!(
                                    key = %u.key,
                                    upload_id = %u.upload_id,
                                    "orphan multipart upload found without session doc, but legacy cleanup policy is disabled; skipping abort"
                                );
                                continue;
                            }
                            crate::config::LegacyMultipartCleanupPolicy::CurrentFormatOnly => {
                                tracing::debug!(
                                    key = %u.key,
                                    upload_id = %u.upload_id,
                                    "orphan multipart upload without session.json cannot be proven current format; skipping abort"
                                );
                                continue;
                            }
                            crate::config::LegacyMultipartCleanupPolicy::OperatorConfirmedAllUnknown => {
                                // Operator explicitly confirmed all unknown uploads may be cleaned up.
                            }
                        }
                    }
                    Err(err) => {
                        // FAIL CLOSED: S3 read error or corrupt metadata must NEVER trigger an abort!
                        tracing::warn!(
                            error = %err,
                            key = %u.key,
                            uuid = %uuid,
                            "failed to fetch session doc during multipart reaper; failing closed (skipping abort)"
                        );
                        continue;
                    }
                }

                // Immediately revalidate before destructive abort
                match self.get_session_doc_with_etag(uuid).await {
                    Ok(None) => {
                        // Authoritative absence reconfirmed. Proceed with abort.
                        match self
                            .driver
                            .abort_multipart_upload(bucket, &u.key, &u.upload_id)
                            .await
                        {
                            Ok(_) => {
                                count += 1;
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    key = %u.key,
                                    upload_id = %u.upload_id,
                                    "failed to abort orphaned multipart upload"
                                );
                            }
                        }
                    }
                    Ok(Some(_)) => {
                        tracing::info!(
                            key = %u.key,
                            uuid = %uuid,
                            "session doc appeared during pre-abort revalidation; abort skipped"
                        );
                        continue;
                    }
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            key = %u.key,
                            uuid = %uuid,
                            "error during pre-abort revalidation; failing closed (abort skipped)"
                        );
                        continue;
                    }
                }
            }

            if !res.is_truncated {
                break;
            }
            key_marker = res.next_key_marker;
            upload_id_marker = res.next_upload_id_marker;
            if key_marker.is_none() && upload_id_marker.is_none() {
                break;
            }
        }

        Ok(count)
    }

    fn finalized_key(&self, uuid: &str) -> String {
        self.key(&format!("uploads/{uuid}/finalized.json"))
    }

    fn multipart_data_key(&self, uuid: &str) -> String {
        self.key(&format!("uploads/{uuid}/multipart.data"))
    }

    fn pending_buffer_key(&self, uuid: &str, op_id: &str) -> String {
        self.key(&format!("uploads/{uuid}/pending/{op_id}.bin"))
    }

    fn encode_upload_token(base_uuid: &str, upload_id: &str) -> String {
        let upload_id_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(upload_id);
        format!("{base_uuid}~{upload_id_b64}")
    }

    fn decode_upload_token(token: &str) -> Result<(String, String), StorageError> {
        let (base, b64) = token.split_once('~').ok_or(StorageError::NotFound)?;
        let upload_id_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(b64.as_bytes())
            .map_err(|_| StorageError::NotFound)?;
        let upload_id = String::from_utf8(upload_id_bytes).map_err(|_| StorageError::NotFound)?;
        Ok((base.to_string(), upload_id))
    }

    async fn get_object_bytes(&self, key: &str) -> Result<Bytes, StorageError> {
        let bucket = self.bucket()?;
        let res = self.driver.get_object(bucket, key).await?;
        match res {
            Some((bytes, _)) => Ok(bytes),
            None => Err(StorageError::NotFound),
        }
    }

    async fn detect_manifest_media_type(&self, bytes: &[u8]) -> Result<String, StorageError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|err| StorageError::Internal(err.to_string()))?;
        let media_type = value
            .get("mediaType")
            .and_then(|v| v.as_str())
            .unwrap_or("application/vnd.oci.image.manifest.v1+json");
        Ok(media_type.to_string())
    }

    async fn get_tag_with_etag(
        &self,
        name: &str,
        tag: &str,
    ) -> Result<Option<(Digest, String)>, StorageError> {
        let bucket = self.bucket()?;
        let key = self.tag_key(name, tag);
        let res = self.driver.get_object(bucket, &key).await?;
        match res {
            None => Ok(None),
            Some((bytes, etag)) => {
                let s = std::str::from_utf8(&bytes)
                    .map_err(|_| StorageError::Internal("invalid tag pointer".to_string()))?;
                let digest = Digest::parse(s.trim()).map_err(|_| StorageError::NotFound)?;
                Ok(Some((digest, etag)))
            }
        }
    }

    async fn get_session_doc_with_etag(
        &self,
        uuid: &str,
    ) -> Result<Option<(S3SessionDoc, String)>, StorageError> {
        let bucket = self.bucket()?;
        let key = self.session_key(uuid);
        let res = self.driver.get_object(bucket, &key).await?;
        match res {
            None => Ok(None),
            Some((bytes, etag)) => {
                let doc: S3SessionDoc = serde_json::from_slice(&bytes)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                Ok(Some((doc, etag)))
            }
        }
    }

    async fn put_session_doc_conditional(
        &self,
        uuid: &str,
        doc: &S3SessionDoc,
        expected_etag: Option<&str>,
    ) -> Result<String, StorageError> {
        let bucket = self.bucket()?;
        let key = self.session_key(uuid);
        let body = serde_json::to_vec(doc).map_err(|e| StorageError::Internal(e.to_string()))?;
        let if_match = expected_etag.map(|s| s.to_string());
        let if_none_match = if expected_etag.is_none() {
            Some("*".to_string())
        } else {
            None
        };
        self.driver
            .put_object_conditional(bucket, &key, Bytes::from(body), if_match, if_none_match)
            .await
    }
}

fn map_s3_err(
    err: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
) -> StorageError {
    match err {
        aws_sdk_s3::error::SdkError::ServiceError(se) => {
            let code = se.err().meta().code().unwrap_or("");
            if code == "NoSuchKey" || code == "NotFound" {
                StorageError::NotFound
            } else {
                StorageError::Internal(se.err().to_string())
            }
        }
        other => StorageError::Internal(other.to_string()),
    }
}

fn map_head_err(
    err: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::head_object::HeadObjectError>,
) -> StorageError {
    match err {
        aws_sdk_s3::error::SdkError::ServiceError(se) => {
            let code = se.err().meta().code().unwrap_or("");
            if code == "NoSuchKey" || code == "NotFound" {
                StorageError::NotFound
            } else {
                StorageError::Internal(se.err().to_string())
            }
        }
        other => StorageError::Internal(other.to_string()),
    }
}

#[allow(dead_code)]
fn map_put_err(
    err: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
) -> StorageError {
    match err {
        aws_sdk_s3::error::SdkError::ServiceError(se) => {
            StorageError::Internal(se.err().to_string())
        }
        other => StorageError::Internal(other.to_string()),
    }
}

#[async_trait]
impl Storage for S3Storage {
    fn kind(&self) -> &'static str {
        "s3"
    }

    async fn list_repositories(&self) -> Result<Vec<String>, StorageError> {
        let bucket = self.bucket()?;
        let repos_prefix = self.key("repos/");
        let objects = self.driver.list_objects_v2(bucket, &repos_prefix).await?;

        let mut set: HashSet<String> = HashSet::new();
        for obj in objects {
            let Some(rest) = obj.key.strip_prefix(&repos_prefix) else {
                continue;
            };
            let repo = if let Some((repo, _)) = rest.split_once("/tags/") {
                Some(repo)
            } else if let Some((repo, _)) = rest.split_once("/manifests/") {
                Some(repo)
            } else if let Some((repo, _)) = rest.split_once("/referrers/") {
                Some(repo)
            } else {
                None
            };
            if let Some(repo) = repo
                && !repo.is_empty()
            {
                set.insert(repo.to_string());
            }
        }

        let mut repos: Vec<String> = set.into_iter().collect();
        repos.sort();
        Ok(repos)
    }

    async fn repo_timestamps(&self, name: &str) -> Result<RepoTimestamps, StorageError> {
        let bucket = self.bucket()?;
        let tags_prefix = self.tags_prefix(name);
        let manifests_prefix = self.key(&format!("repos/{name}/manifests/"));

        let tag_objects = self.driver.list_objects_v2(bucket, &tags_prefix).await?;
        let manifest_objects = self
            .driver
            .list_objects_v2(bucket, &manifests_prefix)
            .await?;

        if tag_objects.is_empty() && manifest_objects.is_empty() {
            return Err(StorageError::NotFound);
        }

        let last_tag_update = tag_objects
            .into_iter()
            .map(|o| UNIX_EPOCH + Duration::from_secs(o.last_modified_unix_secs))
            .max();
        let last_manifest_update = manifest_objects
            .into_iter()
            .map(|o| UNIX_EPOCH + Duration::from_secs(o.last_modified_unix_secs))
            .max();

        Ok(RepoTimestamps {
            last_tag_update,
            last_manifest_update,
        })
    }

    async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError> {
        let bucket = self.bucket()?;
        let key = self.blob_key2(digest);
        let res = self.driver.head_object(bucket, &key).await?;
        match res {
            Some(size) => Ok(BlobMeta { size }),
            None => Err(StorageError::NotFound),
        }
    }

    async fn open_blob(
        &self,
        digest: &Digest,
    ) -> Result<(BlobMeta, Pin<Box<dyn AsyncRead + Send>>), StorageError> {
        let bucket = self.bucket()?;
        let key = self.blob_key2(digest);
        let res = self.driver.get_object(bucket, &key).await?;
        match res {
            Some((bytes, _)) => {
                let size = bytes.len() as u64;
                let reader = std::io::Cursor::new(bytes);
                Ok((BlobMeta { size }, Box::pin(reader)))
            }
            None => Err(StorageError::NotFound),
        }
    }

    async fn resolve_tag(&self, name: &str, tag: &str) -> Result<Digest, StorageError> {
        let key = self.tag_key(name, tag);
        let bytes = self.get_object_bytes(&key).await?;
        let s = std::str::from_utf8(&bytes)
            .map_err(|_| StorageError::Internal("invalid tag pointer".to_string()))?;
        Digest::parse(s.trim()).map_err(|_| StorageError::NotFound)
    }

    async fn list_tags(&self, name: &str) -> Result<Vec<String>, StorageError> {
        let bucket = self.bucket()?;
        let prefix = self.tags_prefix(name);
        let objects = self.driver.list_objects_v2(bucket, &prefix).await?;

        let mut tags = Vec::new();
        for obj in objects {
            if let Some(rest) = obj.key.strip_prefix(&prefix)
                && !rest.is_empty()
            {
                tags.push(rest.to_string());
            }
        }
        tags.sort();
        Ok(tags)
    }

    async fn head_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<ManifestMeta, StorageError> {
        let key = self.manifest_key(name, digest);
        let bytes = self.get_object_bytes(&key).await?;
        let media_type = self.detect_manifest_media_type(&bytes).await?;
        Ok(ManifestMeta {
            size: bytes.len() as u64,
            media_type,
        })
    }

    async fn get_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<(ManifestMeta, Bytes), StorageError> {
        let key = self.manifest_key(name, digest);
        let bytes = self.get_object_bytes(&key).await?;
        let media_type = self.detect_manifest_media_type(&bytes).await?;
        Ok((
            ManifestMeta {
                size: bytes.len() as u64,
                media_type,
            },
            bytes,
        ))
    }

    async fn put_manifest(
        &self,
        name: &str,
        digest: &Digest,
        bytes: Bytes,
    ) -> Result<ManifestMeta, StorageError> {
        let bucket = self.bucket()?;
        let key = self.manifest_key(name, digest);
        let media_type = self.detect_manifest_media_type(&bytes).await?;

        self.driver
            .put_object_conditional(bucket, &key, bytes.clone(), None, None)
            .await?;

        Ok(ManifestMeta {
            size: bytes.len() as u64,
            media_type,
        })
    }

    async fn set_tag(&self, name: &str, tag: &str, digest: &Digest) -> Result<(), StorageError> {
        self.mutate_tag(name, tag, digest, super::TagMutationPolicy::Replace)
            .await?;
        Ok(())
    }

    async fn mutate_tag(
        &self,
        name: &str,
        tag: &str,
        digest: &Digest,
        policy: super::TagMutationPolicy,
    ) -> Result<super::TagMutation, StorageError> {
        let bucket = self.bucket()?;
        let key = self.tag_key(name, tag);
        let body = format!("{}\n", digest.as_str());

        match policy {
            super::TagMutationPolicy::CreateOnly => {
                let res = self
                    .driver
                    .put_object_conditional(
                        bucket,
                        &key,
                        Bytes::from(body.into_bytes()),
                        None,
                        Some("*".to_string()),
                    )
                    .await;

                match res {
                    Ok(_) => Ok(super::TagMutation::Created),
                    Err(StorageError::TagAlreadyExists) => {
                        if let Ok(existing_d) = self.resolve_tag(name, tag).await
                            && existing_d == *digest
                        {
                            return Ok(super::TagMutation::Unchanged);
                        }
                        Err(StorageError::TagAlreadyExists)
                    }
                    Err(err) => Err(err),
                }
            }
            super::TagMutationPolicy::Replace => {
                let max_retries = 5;
                for attempt in 0..max_retries {
                    let current = self.get_tag_with_etag(name, tag).await?;
                    match current {
                        None => {
                            let res = self
                                .driver
                                .put_object_conditional(
                                    bucket,
                                    &key,
                                    Bytes::from(body.clone().into_bytes()),
                                    None,
                                    Some("*".to_string()),
                                )
                                .await;
                            match res {
                                Ok(_) => return Ok(super::TagMutation::Created),
                                Err(StorageError::TagAlreadyExists) => {
                                    if attempt + 1 < max_retries {
                                        tokio::time::sleep(std::time::Duration::from_millis(
                                            10 * (1 << attempt),
                                        ))
                                        .await;
                                        continue;
                                    }
                                    return Err(StorageError::Internal(
                                        "tag mutation contention limit exceeded".to_string(),
                                    ));
                                }
                                Err(err) => return Err(err),
                            }
                        }
                        Some((existing_d, etag)) => {
                            if existing_d == *digest {
                                return Ok(super::TagMutation::Unchanged);
                            }

                            let res = self
                                .driver
                                .put_object_conditional(
                                    bucket,
                                    &key,
                                    Bytes::from(body.clone().into_bytes()),
                                    Some(etag),
                                    None,
                                )
                                .await;
                            match res {
                                Ok(_) => {
                                    return Ok(super::TagMutation::Replaced {
                                        previous: existing_d,
                                    });
                                }
                                Err(StorageError::TagAlreadyExists) => {
                                    if attempt + 1 < max_retries {
                                        tokio::time::sleep(std::time::Duration::from_millis(
                                            10 * (1 << attempt),
                                        ))
                                        .await;
                                        continue;
                                    }
                                    return Err(StorageError::Internal(
                                        "tag mutation contention limit exceeded".to_string(),
                                    ));
                                }
                                Err(err) => return Err(err),
                            }
                        }
                    }
                }
                Err(StorageError::Internal(
                    "tag mutation contention limit exceeded".to_string(),
                ))
            }
        }
    }

    async fn delete_tag(&self, name: &str, tag: &str) -> Result<(), StorageError> {
        let bucket = self.bucket()?;
        let key = self.tag_key(name, tag);
        self.driver.delete_object(bucket, &key).await
    }

    async fn list_manifest_digests_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<Digest>, Option<String>), StorageError> {
        let bucket = self.bucket()?;
        let prefix = self.key(&format!("repos/{repo}/manifests/"));
        let objects = self.driver.list_objects_v2(bucket, &prefix).await?;

        let mut all_digests: Vec<Digest> = Vec::new();
        for obj in objects {
            let rel = match obj.key.strip_prefix(&prefix) {
                Some(r) => r,
                None => continue,
            };
            let hex = rel.trim_end_matches(".json");
            if let Ok(d) = Digest::parse(&format!("sha256:{hex}")) {
                all_digests.push(d);
            } else if let Ok(d) = Digest::parse(hex) {
                all_digests.push(d);
            }
        }
        all_digests.sort_by(|a, b| a.hex().cmp(b.hex()));

        let start_idx = if let Some(token) = continuation_token {
            match all_digests.binary_search_by(|d| d.as_str().as_str().cmp(token)) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            }
        } else {
            0
        };

        let end_idx = (start_idx + page_limit).min(all_digests.len());
        let page_slice = &all_digests[start_idx..end_idx];

        let next_token = if end_idx < all_digests.len() {
            page_slice.last().map(|d| d.as_str().to_string())
        } else {
            None
        };

        Ok((page_slice.to_vec(), next_token))
    }

    async fn list_tags_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<(String, Digest)>, Option<String>), StorageError> {
        let bucket = self.bucket()?;
        let prefix = self.tags_prefix(repo);
        let tag_objects = self.driver.list_objects_v2(bucket, &prefix).await?;

        let mut tags_with_digest: Vec<(String, Digest)> = Vec::new();
        for obj in tag_objects {
            let tag_name = match obj.key.strip_prefix(&prefix) {
                Some(t) => t.to_string(),
                None => continue,
            };
            if let Some((bytes, _etag)) = self.driver.get_object(bucket, &obj.key).await? {
                let s = String::from_utf8_lossy(&bytes);
                if let Ok(d) = Digest::parse(s.trim()) {
                    tags_with_digest.push((tag_name, d));
                }
            }
        }
        tags_with_digest.sort_by(|a, b| a.0.cmp(&b.0));

        let start_idx = if let Some(token) = continuation_token {
            match tags_with_digest.binary_search_by(|(t, _)| t.as_str().cmp(token)) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            }
        } else {
            0
        };

        let end_idx = (start_idx + page_limit).min(tags_with_digest.len());
        let page_slice = &tags_with_digest[start_idx..end_idx];

        let next_token = if end_idx < tags_with_digest.len() {
            page_slice.last().map(|(t, _)| t.clone())
        } else {
            None
        };

        Ok((page_slice.to_vec(), next_token))
    }

    async fn list_referrers_page(
        &self,
        repo: &str,
        subject: &Digest,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<ReferrerDescriptor>, Option<String>), StorageError> {
        let mut refs = self.list_referrers(repo, subject).await.unwrap_or_default();
        refs.sort_by(|a, b| a.digest.cmp(&b.digest));

        let start_idx = if let Some(token) = continuation_token {
            match refs.binary_search_by(|r| r.digest.as_str().cmp(token)) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            }
        } else {
            0
        };

        let end_idx = (start_idx + page_limit).min(refs.len());
        let page_slice = &refs[start_idx..end_idx];

        let next_token = if end_idx < refs.len() {
            page_slice.last().map(|r| r.digest.clone())
        } else {
            None
        };

        Ok((page_slice.to_vec(), next_token))
    }

    async fn get_tag_with_version(
        &self,
        repo: &str,
        tag: &str,
    ) -> Result<Option<(Digest, String)>, StorageError> {
        let bucket = self.bucket()?;
        let key = self.tag_key(repo, tag);
        if let Some((bytes, etag)) = self.driver.get_object(bucket, &key).await? {
            let s = String::from_utf8_lossy(&bytes);
            let digest = Digest::parse(s.trim())
                .map_err(|e| StorageError::Internal(format!("corrupt tag {tag}: {e}")))?;
            return Ok(Some((digest, etag)));
        }
        Ok(None)
    }

    async fn delete_tag_conditional(
        &self,
        repo: &str,
        tag: &str,
        expected_version: Option<&str>,
    ) -> Result<super::ConditionalDeleteResult, StorageError> {
        let bucket = self.bucket()?;
        let key = self.tag_key(repo, tag);
        self.driver
            .delete_object_conditional(bucket, &key, expected_version.map(|s| s.to_string()))
            .await
    }

    async fn read_lifecycle_journal(&self, repo: &str) -> Result<Option<Bytes>, StorageError> {
        let canonical = crate::registry::canonical_name::CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let bucket = self.bucket()?;
        let key = self.key(&format!(
            "repos/{}/meta/lifecycle_journal.json",
            canonical.as_str()
        ));
        match self.driver.get_object(bucket, &key).await? {
            Some((bytes, _etag)) => Ok(Some(bytes)),
            None => Ok(None),
        }
    }

    async fn write_lifecycle_journal(&self, repo: &str, data: Bytes) -> Result<(), StorageError> {
        let canonical = crate::registry::canonical_name::CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let bucket = self.bucket()?;
        let key = self.key(&format!(
            "repos/{}/meta/lifecycle_journal.json",
            canonical.as_str()
        ));
        self.driver
            .put_object_conditional(bucket, &key, data, None, None)
            .await?;
        Ok(())
    }

    async fn delete_lifecycle_journal(&self, repo: &str) -> Result<(), StorageError> {
        let canonical = crate::registry::canonical_name::CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let bucket = self.bucket()?;
        let key = self.key(&format!(
            "repos/{}/meta/lifecycle_journal.json",
            canonical.as_str()
        ));
        let _ = self.driver.delete_object(bucket, &key).await;
        Ok(())
    }

    async fn acquire_repo_lease(
        &self,
        repo: &str,
        owner_id: &str,
        lease_id: &str,
        ttl_secs: u64,
    ) -> Result<bool, StorageError> {
        let canonical = crate::registry::canonical_name::CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let bucket = self.bucket()?;
        let key = self.key(&format!(
            "repos/{}/meta/repo_lease.json",
            canonical.as_str()
        ));
        let now = self.driver.now_unix_secs();

        #[derive(serde::Serialize, serde::Deserialize)]
        struct LeaseDoc {
            owner_id: String,
            lease_id: String,
            acquired_unix_secs: u64,
            expiry_unix_secs: u64,
        }

        let existing = self.driver.get_object(bucket, &key).await?;
        match existing {
            None => {
                let doc = LeaseDoc {
                    owner_id: owner_id.to_string(),
                    lease_id: lease_id.to_string(),
                    acquired_unix_secs: now,
                    expiry_unix_secs: now + ttl_secs,
                };
                let body = Bytes::from(serde_json::to_vec(&doc).unwrap());
                match self
                    .driver
                    .put_object_conditional(bucket, &key, body, None, Some("*".to_string()))
                    .await
                {
                    Ok(_) => Ok(true),
                    Err(StorageError::TagAlreadyExists) => Ok(false),
                    Err(e) => Err(e),
                }
            }
            Some((bytes, _etag)) => {
                if let Ok(parsed) = serde_json::from_slice::<LeaseDoc>(&bytes) {
                    if parsed.owner_id == owner_id && parsed.lease_id == lease_id {
                        return Ok(true);
                    }
                }
                // Under Option B (explicit single-writer / no automatic expiry takeover),
                // an active or unreleased lease held by another owner fails closed.
                Ok(false)
            }
        }
    }

    async fn renew_repo_lease(
        &self,
        repo: &str,
        owner_id: &str,
        lease_id: &str,
        ttl_secs: u64,
    ) -> Result<bool, StorageError> {
        let canonical = crate::registry::canonical_name::CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let bucket = self.bucket()?;
        let key = self.key(&format!(
            "repos/{}/meta/repo_lease.json",
            canonical.as_str()
        ));
        let now = self.driver.now_unix_secs();

        #[derive(serde::Serialize, serde::Deserialize)]
        struct LeaseDoc {
            owner_id: String,
            lease_id: String,
            acquired_unix_secs: u64,
            expiry_unix_secs: u64,
        }

        let Some((bytes, etag)) = self.driver.get_object(bucket, &key).await? else {
            return Ok(false);
        };

        let Ok(parsed) = serde_json::from_slice::<LeaseDoc>(&bytes) else {
            return Ok(false);
        };

        if parsed.owner_id != owner_id || parsed.lease_id != lease_id {
            return Ok(false);
        }

        let doc = LeaseDoc {
            owner_id: owner_id.to_string(),
            lease_id: lease_id.to_string(),
            acquired_unix_secs: parsed.acquired_unix_secs,
            expiry_unix_secs: now + ttl_secs,
        };
        let body = Bytes::from(serde_json::to_vec(&doc).unwrap());
        match self
            .driver
            .put_object_conditional(bucket, &key, body, Some(etag), None)
            .await
        {
            Ok(_) => Ok(true),
            Err(StorageError::TagAlreadyExists) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn release_repo_lease(
        &self,
        repo: &str,
        owner_id: &str,
        lease_id: &str,
    ) -> Result<(), StorageError> {
        let canonical = crate::registry::canonical_name::CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        let bucket = self.bucket()?;
        let key = self.key(&format!(
            "repos/{}/meta/repo_lease.json",
            canonical.as_str()
        ));

        #[derive(serde::Serialize, serde::Deserialize)]
        struct LeaseDoc {
            owner_id: String,
            lease_id: String,
            acquired_unix_secs: u64,
            expiry_unix_secs: u64,
        }

        if let Some((bytes, etag)) = self.driver.get_object(bucket, &key).await? {
            if let Ok(parsed) = serde_json::from_slice::<LeaseDoc>(&bytes) {
                if parsed.owner_id == owner_id && parsed.lease_id == lease_id {
                    let _ = self
                        .driver
                        .delete_object_conditional(bucket, &key, Some(etag))
                        .await;
                }
            }
        }
        Ok(())
    }

    async fn acquire_deployment_writer_lock(
        &self,
        doc: &super::mutation_authority::DeploymentWriterLockDoc,
    ) -> Result<(bool, Option<String>), StorageError> {
        let bucket = self.bucket()?;
        let key = self.key("meta/exclusive_writer.lock");

        if let Some((bytes, etag)) = self.driver.get_object(bucket, &key).await? {
            if let Ok(existing) =
                serde_json::from_slice::<super::mutation_authority::DeploymentWriterLockDoc>(&bytes)
            {
                if existing.owner_token == doc.owner_token {
                    return Ok((true, Some(etag)));
                }
                return Err(StorageError::ExclusiveWriterLocked(format!(
                    "held by {} on {} (pid {}) in mode '{}' acquired at {}",
                    existing.owner_id,
                    existing.hostname,
                    existing.pid,
                    existing.command_mode,
                    existing.acquired_unix_secs
                )));
            } else {
                let s = String::from_utf8_lossy(&bytes).to_string();
                return Err(StorageError::ExclusiveWriterLocked(s));
            }
        }

        let body = Bytes::from(serde_json::to_vec(doc).unwrap());
        match self
            .driver
            .put_object_conditional(bucket, &key, body, None, Some("*".to_string()))
            .await
        {
            Ok(_) => {
                let etag = self
                    .driver
                    .get_object(bucket, &key)
                    .await?
                    .map(|(_, tag)| tag);
                Ok((true, etag))
            }
            Err(StorageError::TagAlreadyExists) => {
                let current_owner = self
                    .driver
                    .get_object(bucket, &key)
                    .await?
                    .map(|(b, _)| {
                        if let Ok(d) = serde_json::from_slice::<
                            super::mutation_authority::DeploymentWriterLockDoc,
                        >(&b)
                        {
                            format!("{} ({})", d.owner_id, d.command_mode)
                        } else {
                            String::from_utf8_lossy(&b).to_string()
                        }
                    })
                    .unwrap_or_else(|| "unknown".to_string());
                Err(StorageError::ExclusiveWriterLocked(current_owner))
            }
            Err(e) => Err(e),
        }
    }

    async fn release_deployment_writer_lock(
        &self,
        doc: &super::mutation_authority::DeploymentWriterLockDoc,
        expected_etag: Option<&str>,
    ) -> Result<bool, StorageError> {
        let bucket = self.bucket()?;
        let key = self.key("meta/exclusive_writer.lock");

        if let Some((bytes, etag)) = self.driver.get_object(bucket, &key).await? {
            if let Ok(existing) =
                serde_json::from_slice::<super::mutation_authority::DeploymentWriterLockDoc>(&bytes)
            {
                if existing.owner_token != doc.owner_token {
                    // Lock belongs to someone else; do not touch
                    return Ok(false);
                }
            }
            let version_to_delete = expected_etag.or(Some(&etag)).map(ToString::to_string);
            let _ = self
                .driver
                .delete_object_conditional(bucket, &key, version_to_delete)
                .await;
            return Ok(true);
        }
        Ok(false)
    }

    async fn inspect_deployment_writer_lock(
        &self,
    ) -> Result<
        Option<(
            super::mutation_authority::DeploymentWriterLockDoc,
            Option<String>,
        )>,
        StorageError,
    > {
        let bucket = self.bucket()?;
        let key = self.key("meta/exclusive_writer.lock");

        if let Some((bytes, etag)) = self.driver.get_object(bucket, &key).await? {
            if let Ok(doc) =
                serde_json::from_slice::<super::mutation_authority::DeploymentWriterLockDoc>(&bytes)
            {
                return Ok(Some((doc, Some(etag))));
            }
        }
        Ok(None)
    }

    async fn admin_clear_deployment_writer_lock(
        &self,
        expected_owner: &str,
        expected_etag: &str,
    ) -> Result<(), StorageError> {
        let bucket = self.bucket()?;
        let key = self.key("meta/exclusive_writer.lock");

        let (bytes, current_etag) = match self.driver.get_object(bucket, &key).await? {
            Some(res) => res,
            None => return Err(StorageError::NotFound),
        };

        if !expected_etag.is_empty() && current_etag != expected_etag {
            return Err(StorageError::ExclusiveWriterLocked(format!(
                "lock etag mismatch: expected '{expected_etag}', current is '{current_etag}'"
            )));
        }

        if let Ok(doc) =
            serde_json::from_slice::<super::mutation_authority::DeploymentWriterLockDoc>(&bytes)
        {
            if !expected_owner.is_empty()
                && doc.owner_id != expected_owner
                && doc.owner_token != expected_owner
            {
                return Err(StorageError::Internal(format!(
                    "lock owner mismatch: expected '{expected_owner}', current is '{}'",
                    doc.owner_id
                )));
            }
        }

        let version = if expected_etag.is_empty() {
            Some(current_etag)
        } else {
            Some(expected_etag.to_string())
        };

        let res = self
            .driver
            .delete_object_conditional(bucket, &key, version)
            .await?;
        match res {
            super::ConditionalDeleteResult::Deleted => Ok(()),
            super::ConditionalDeleteResult::PreconditionFailed { current_version } => {
                Err(StorageError::ExclusiveWriterLocked(format!(
                    "lock changed concurrently to generation {:?}",
                    current_version
                )))
            }
            super::ConditionalDeleteResult::NotFound => Err(StorageError::NotFound),
        }
    }

    async fn create_upload(&self) -> Result<UploadMeta, StorageError> {
        let bucket = self.bucket()?;
        let base_uuid = uuid::Uuid::new_v4().to_string();
        let key = self.upload_key(&base_uuid);

        let upload_id = self.driver.create_multipart_upload(bucket, &key).await?;

        Ok(UploadMeta {
            uuid: Self::encode_upload_token(&base_uuid, &upload_id),
            offset: 0,
        })
    }

    async fn upload_status(&self, uuid: &str) -> Result<UploadMeta, StorageError> {
        let (base_uuid, _upload_id) = Self::decode_upload_token(uuid)?;
        let bucket = self.bucket()?;
        let key = self.upload_key(&base_uuid);
        let head = self.driver.head_object(bucket, &key).await?;
        let offset = head.unwrap_or(0);
        Ok(UploadMeta {
            uuid: uuid.to_string(),
            offset,
        })
    }

    async fn append_upload(&self, uuid: &str, chunk: Bytes) -> Result<UploadMeta, StorageError> {
        let bucket = self.bucket()?;
        let (base_uuid, upload_id) = Self::decode_upload_token(uuid)?;
        let key = self.upload_key(&base_uuid);

        let part_number = 1;
        self.driver
            .upload_part(bucket, &key, &upload_id, part_number, chunk.clone())
            .await?;

        Ok(UploadMeta {
            uuid: uuid.to_string(),
            offset: chunk.len() as u64,
        })
    }

    async fn finalize_upload(&self, uuid: &str, digest: &Digest) -> Result<BlobMeta, StorageError> {
        let bucket = self.bucket()?;
        let (base_uuid, upload_id) = Self::decode_upload_token(uuid)?;
        let upload_key = self.upload_key(&base_uuid);

        self.driver
            .complete_multipart_upload(
                bucket,
                &upload_key,
                &upload_id,
                vec![(1, "dummy".to_string())],
            )
            .await?;

        let dest_key = self.blob_key2(digest);
        self.driver
            .copy_object(bucket, &upload_key, bucket, &dest_key)
            .await?;
        let _ = self.driver.delete_object(bucket, &upload_key).await;

        let meta = self.head_blob(digest).await?;
        Ok(meta)
    }

    async fn abort_upload(&self, uuid: &str) -> Result<(), StorageError> {
        let bucket = self.bucket()?;
        let (base_uuid, upload_id) = Self::decode_upload_token(uuid)?;
        let key = self.upload_key(&base_uuid);

        let _ = self
            .driver
            .abort_multipart_upload(bucket, &key, &upload_id)
            .await;
        let _ = self.driver.delete_object(bucket, &key).await;
        Ok(())
    }

    async fn delete_blob(&self, digest: &Digest) -> Result<(), StorageError> {
        let bucket = self.bucket()?;
        let key = self.blob_key2(digest);
        self.driver.delete_object(bucket, &key).await
    }

    async fn list_referrers(
        &self,
        name: &str,
        subject: &Digest,
    ) -> Result<Vec<ReferrerDescriptor>, StorageError> {
        let key = self.referrers_key(name, subject);
        let bytes = match self.get_object_bytes(&key).await {
            Ok(b) => b,
            Err(StorageError::NotFound) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        serde_json::from_slice::<Vec<ReferrerDescriptor>>(&bytes)
            .map_err(|err| StorageError::Internal(err.to_string()))
    }

    async fn add_referrer(
        &self,
        name: &str,
        subject: &Digest,
        descriptor: ReferrerDescriptor,
    ) -> Result<(), StorageError> {
        let _lock = self.referrer_lock_shard(name, subject).lock().await;
        let bucket = self.bucket()?;
        let key = self.referrers_key(name, subject);

        let mut existing = self.list_referrers(name, subject).await?;
        if !existing.iter().any(|d| d.digest == descriptor.digest) {
            existing.push(descriptor);
        }
        let body =
            serde_json::to_vec(&existing).map_err(|err| StorageError::Internal(err.to_string()))?;

        self.driver
            .put_object_conditional(bucket, &key, Bytes::from(body), None, None)
            .await?;
        Ok(())
    }

    async fn remove_referrer(
        &self,
        name: &str,
        subject: &Digest,
        referrer: &Digest,
    ) -> Result<(), StorageError> {
        let _lock = self.referrer_lock_shard(name, subject).lock().await;
        let bucket = self.bucket()?;
        let key = self.referrers_key(name, subject);

        let mut existing = self.list_referrers(name, subject).await?;
        let orig_len = existing.len();
        let referrer_str = referrer.as_str();
        existing.retain(|d| d.digest != referrer_str);
        if existing.len() == orig_len {
            return Ok(());
        }

        if existing.is_empty() {
            let _ = self.driver.delete_object(bucket, &key).await;
        } else {
            let body = serde_json::to_vec(&existing)
                .map_err(|err| StorageError::Internal(err.to_string()))?;
            self.driver
                .put_object_conditional(bucket, &key, Bytes::from(body), None, None)
                .await?;
        }
        Ok(())
    }

    async fn delete_manifest(&self, name: &str, digest: &Digest) -> Result<(), StorageError> {
        let bucket = self.bucket()?;
        let key = self.manifest_key(name, digest);

        let bytes = self.get_object_bytes(&key).await?;
        let maybe_subject = crate::manifest_refs::extract_subject_digest(&bytes).map_err(|e| {
            StorageError::Internal(format!(
                "cannot delete manifest with malformed structure: {e}"
            ))
        })?;

        self.driver.delete_object(bucket, &key).await?;

        let digest_str = digest.as_str();
        let prefix = self.tags_prefix(name);
        let tag_objects = self.driver.list_objects_v2(bucket, &prefix).await?;

        for obj in tag_objects {
            let bytes = match self.get_object_bytes(&obj.key).await {
                Ok(b) => b,
                Err(_) => continue,
            };
            let s = match std::str::from_utf8(&bytes) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if s.trim() == digest_str {
                let _ = self.driver.delete_object(bucket, &obj.key).await;
            }
        }

        if let Some(subject) = maybe_subject {
            let _ = self.remove_referrer(name, &subject, digest).await;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3SessionDoc {
    pub format_version: u32,
    pub repo: String,
    pub uuid: String,
    pub multipart_upload_id: String,
    pub state: UploadSessionState,
    pub committed_offset: u64,
    pub committed_parts: Vec<S3CommittedPart>,
    pub pending_buffer_key: Option<String>,
    pub pending_bytes: u64,
    pub created_at_unix_secs: u64,
    pub last_active_at_unix_secs: u64,
    pub current_operation: Option<S3CurrentOperation>,
    pub finalizing_info: Option<S3FinalizingInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3CommittedPart {
    pub part_number: i32,
    pub size: u64,
    pub etag: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3CurrentOperation {
    pub operation_id: String,
    pub expected_offset: u64,
    pub target_part_number: i32,
    pub lease_expires_at_unix_secs: u64,
}

/// Default lease duration for active Appending and Finalizing upload operations.
pub const S3_LEASE_DURATION_SECS: u64 = 300;

/// Interval at which active upload stream workers renew their session lease.
pub const S3_LEASE_RENEWAL_INTERVAL_SECS: u64 = 60;

/// S3 multipart part size threshold (5 MiB standard minimum chunk size).
pub const S3_PART_SIZE: usize = 5 * 1024 * 1024;

/// Maximum number of CAS retry attempts under transient contention.
pub const S3_MAX_RETRY_ATTEMPTS: u32 = 3;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3FinalizingInfo {
    pub operation_id: String,
    pub expected_digest: String,
    pub size: u64,
    pub finalizing_at_unix_secs: u64,
    #[serde(default)]
    pub multipart_completed: bool,
}

#[async_trait]
impl UploadSessionStorage for S3Storage {
    async fn create_session(&self, repo: &str) -> Result<UploadSessionId, StorageError> {
        let bucket = self.bucket()?;
        let uuid = uuid::Uuid::new_v4().to_string();
        let data_key = self.multipart_data_key(&uuid);

        let multipart_upload_id = self
            .driver
            .create_multipart_upload(bucket, &data_key)
            .await?;
        let now = self.driver.now_unix_secs();

        let doc = S3SessionDoc {
            format_version: 1,
            repo: repo.to_string(),
            uuid: uuid.clone(),
            multipart_upload_id,
            state: UploadSessionState::Active,
            committed_offset: 0,
            committed_parts: Vec::new(),
            pending_buffer_key: None,
            pending_bytes: 0,
            created_at_unix_secs: now,
            last_active_at_unix_secs: now,
            current_operation: None,
            finalizing_info: None,
        };

        self.put_session_doc_conditional(&uuid, &doc, None).await?;
        Ok(UploadSessionId::new(repo, uuid))
    }

    async fn session_status(
        &self,
        session: &UploadSessionId,
    ) -> Result<UploadSessionStatus, UploadTransitionError> {
        let (doc, _) = match self
            .get_session_doc_with_etag(&session.uuid)
            .await
            .map_err(UploadTransitionError::Storage)?
        {
            Some(pair) => pair,
            None => {
                if let Some(receipt) = self
                    .get_finalized_receipt(session)
                    .await
                    .map_err(UploadTransitionError::Storage)?
                {
                    return Ok(UploadSessionStatus {
                        session: session.clone(),
                        state: UploadSessionState::Finalizing,
                        committed_offset: receipt.size,
                        created_at: UNIX_EPOCH
                            + Duration::from_secs(receipt.finalized_at_unix_secs),
                        last_active_at: UNIX_EPOCH
                            + Duration::from_secs(receipt.finalized_at_unix_secs),
                    });
                }
                return Err(UploadTransitionError::NotFound);
            }
        };

        if doc.repo != session.repo || doc.uuid != session.uuid {
            return Err(UploadTransitionError::NotFound);
        }

        Ok(UploadSessionStatus {
            session: session.clone(),
            state: doc.state,
            committed_offset: doc.committed_offset,
            created_at: UNIX_EPOCH + Duration::from_secs(doc.created_at_unix_secs),
            last_active_at: UNIX_EPOCH + Duration::from_secs(doc.last_active_at_unix_secs),
        })
    }

    async fn append_if_offset(
        &self,
        session: &UploadSessionId,
        expected_offset: UploadOffsetPrecondition,
        mut stream: UploadByteStream,
        max_upload_bytes: u64,
    ) -> Result<UploadAppendResult, UploadTransitionError> {
        let bucket = self.bucket().map_err(UploadTransitionError::Storage)?;

        // 1. Acquire reservation lease on session.json with bounded retry on CAS contention
        let mut reserved_doc: S3SessionDoc;
        let mut reserved_etag: String;
        let operation_id = uuid::Uuid::new_v4().to_string();

        let mut attempts = 0;
        loop {
            attempts += 1;
            let (mut doc, etag) = match self
                .get_session_doc_with_etag(&session.uuid)
                .await
                .map_err(UploadTransitionError::Storage)?
            {
                Some(p) => p,
                None => return Err(UploadTransitionError::NotFound),
            };

            if doc.repo != session.repo || doc.uuid != session.uuid {
                return Err(UploadTransitionError::NotFound);
            }

            let now = self.driver.now_unix_secs();

            match doc.state {
                UploadSessionState::Active => {
                    match expected_offset {
                        UploadOffsetPrecondition::Exact(off) => {
                            if off != doc.committed_offset {
                                return Ok(UploadAppendResult::OffsetMismatch {
                                    current_offset: doc.committed_offset,
                                });
                            }
                        }
                        UploadOffsetPrecondition::CurrentForServerComposedMonolithicOperation => {}
                    }

                    let target_part_number = doc
                        .committed_parts
                        .iter()
                        .map(|p| p.part_number)
                        .max()
                        .unwrap_or(0)
                        .saturating_add(1);
                    doc.state = UploadSessionState::Appending;
                    doc.current_operation = Some(S3CurrentOperation {
                        operation_id: operation_id.clone(),
                        expected_offset: match expected_offset {
                            UploadOffsetPrecondition::Exact(off) => off,
                            UploadOffsetPrecondition::CurrentForServerComposedMonolithicOperation => {
                                doc.committed_offset
                            }
                        },
                        target_part_number,
                        lease_expires_at_unix_secs: now.saturating_add(self.session_config.lease_duration_secs),
                    });
                    doc.last_active_at_unix_secs = now;

                    match self
                        .put_session_doc_conditional(&session.uuid, &doc, Some(&etag))
                        .await
                    {
                        Ok(new_etag) => {
                            reserved_doc = doc;
                            reserved_etag = new_etag;
                            break;
                        }
                        Err(StorageError::TagAlreadyExists) => {
                            if attempts >= self.session_config.max_retry_attempts {
                                return Ok(UploadAppendResult::Conflict);
                            }
                            tokio::time::sleep(Duration::from_millis(10 * (1 << attempts))).await;
                            continue;
                        }
                        Err(err) => return Err(UploadTransitionError::Storage(err)),
                    }
                }
                UploadSessionState::Appending => {
                    let lease_expired = doc
                        .current_operation
                        .as_ref()
                        .map(|op| now > op.lease_expires_at_unix_secs)
                        .unwrap_or(true);
                    if lease_expired {
                        let _ = self.recover_session(session).await;
                        if attempts >= self.session_config.max_retry_attempts {
                            return Ok(UploadAppendResult::Conflict);
                        }
                        tokio::time::sleep(Duration::from_millis(10 * (1 << attempts))).await;
                        continue;
                    }
                    return Ok(UploadAppendResult::Conflict);
                }
                UploadSessionState::Finalizing => return Ok(UploadAppendResult::Conflict),
            }
        }

        // 2. Read existing pending bytes if any (strictly < 5 MiB)
        let mut buffer = Vec::with_capacity(S3_PART_SIZE + 64 * 1024);
        if let Some(ref pending_key) = reserved_doc.pending_buffer_key
            && let Ok(Some((b, _))) = self.driver.get_object(bucket, pending_key).await
        {
            buffer.extend_from_slice(&b);
        }

        let limit = if max_upload_bytes > 0 {
            max_upload_bytes
        } else {
            self.max_upload_bytes
        };
        let initial_offset = reserved_doc.committed_offset;
        let mut incoming_written = 0u64;
        let mut parts_to_commit = reserved_doc.committed_parts.clone();
        let mut target_part_number = parts_to_commit
            .iter()
            .map(|p| p.part_number)
            .max()
            .unwrap_or(0)
            .saturating_add(1);

        let mut last_renewed_at = self.driver.now_unix_secs();
        let data_key = self.multipart_data_key(&session.uuid);

        // 3. Streaming loop: read chunk, buffer, upload 5 MiB parts when full, renew lease
        while let Some(chunk_res) = stream.next().await {
            let chunk = match chunk_res {
                Ok(c) => c,
                Err(err) => return Err(UploadTransitionError::Stream(err)),
            };
            if chunk.is_empty() {
                continue;
            }
            let next_total = initial_offset
                .saturating_add(incoming_written)
                .saturating_add(chunk.len() as u64);
            if limit > 0 && next_total > limit {
                return Err(UploadTransitionError::TooLarge);
            }
            buffer.extend_from_slice(&chunk);
            incoming_written = incoming_written.saturating_add(chunk.len() as u64);

            let now = self.driver.now_unix_secs();
            // Check lease renewal interval
            if now.saturating_sub(last_renewed_at)
                >= self.session_config.lease_renewal_interval_secs
            {
                let (cur_doc, cur_etag) = match self
                    .get_session_doc_with_etag(&session.uuid)
                    .await
                    .map_err(UploadTransitionError::Storage)?
                {
                    Some(p) => p,
                    None => return Ok(UploadAppendResult::Conflict),
                };

                let same_owner = cur_doc
                    .current_operation
                    .as_ref()
                    .map(|op| op.operation_id == operation_id)
                    .unwrap_or(false);

                if !same_owner {
                    return Ok(UploadAppendResult::Conflict);
                }

                let mut renew_doc = cur_doc;
                if let Some(ref mut op) = renew_doc.current_operation {
                    op.lease_expires_at_unix_secs =
                        now.saturating_add(self.session_config.lease_duration_secs);
                    op.target_part_number = target_part_number;
                }
                renew_doc.last_active_at_unix_secs = now;

                match self
                    .put_session_doc_conditional(&session.uuid, &renew_doc, Some(&cur_etag))
                    .await
                {
                    Ok(new_etag) => {
                        reserved_etag = new_etag;
                        last_renewed_at = now;
                    }
                    Err(StorageError::TagAlreadyExists) => {
                        // Lost lease / 412
                        return Ok(UploadAppendResult::Conflict);
                    }
                    Err(err) => return Err(UploadTransitionError::Storage(err)),
                }
            }

            // Upload full 5 MiB parts as they accumulate
            while buffer.len() >= S3_PART_SIZE {
                let part_bytes = Bytes::copy_from_slice(&buffer[..S3_PART_SIZE]);
                let part_etag = self
                    .driver
                    .upload_part(
                        bucket,
                        &data_key,
                        &reserved_doc.multipart_upload_id,
                        target_part_number,
                        part_bytes,
                    )
                    .await
                    .map_err(UploadTransitionError::Storage)?;

                parts_to_commit.push(S3CommittedPart {
                    part_number: target_part_number,
                    size: S3_PART_SIZE as u64,
                    etag: part_etag,
                });
                target_part_number += 1;
                buffer.drain(..S3_PART_SIZE);
            }
        }

        let old_pending_key = reserved_doc.pending_buffer_key.clone();
        let mut new_pending_key = None;
        let new_pending_bytes = buffer.len() as u64;

        if new_pending_bytes > 0 {
            let key = self.pending_buffer_key(&session.uuid, &operation_id);
            self.driver
                .put_object_conditional(
                    bucket,
                    &key,
                    Bytes::from(buffer),
                    None,
                    Some("*".to_string()),
                )
                .await
                .map_err(UploadTransitionError::Storage)?;
            new_pending_key = Some(key);
        }

        let now = self.driver.now_unix_secs();
        reserved_doc.committed_offset = initial_offset.saturating_add(incoming_written);
        reserved_doc.committed_parts = parts_to_commit;
        reserved_doc.pending_buffer_key = new_pending_key.clone();
        reserved_doc.pending_bytes = new_pending_bytes;
        reserved_doc.state = UploadSessionState::Active;
        reserved_doc.current_operation = None;
        reserved_doc.last_active_at_unix_secs = now;

        match self
            .put_session_doc_conditional(&session.uuid, &reserved_doc, Some(&reserved_etag))
            .await
        {
            Ok(_) => {
                if let Some(old_key) = old_pending_key {
                    let _ = self.driver.delete_object(bucket, &old_key).await;
                }
                Ok(UploadAppendResult::Committed {
                    new_offset: reserved_doc.committed_offset,
                })
            }
            Err(StorageError::TagAlreadyExists) => {
                if let Some(ref new_key) = new_pending_key {
                    let _ = self.driver.delete_object(bucket, new_key).await;
                }
                Ok(UploadAppendResult::Conflict)
            }
            Err(err) => {
                if let Some(ref new_key) = new_pending_key {
                    let _ = self.driver.delete_object(bucket, new_key).await;
                }
                Err(UploadTransitionError::Storage(err))
            }
        }
    }

    async fn begin_finalize(
        &self,
        session: &UploadSessionId,
        expected_offset: UploadOffsetPrecondition,
        trailing_stream: Option<UploadByteStream>,
        expected_digest: &Digest,
        max_upload_bytes: u64,
        abort_on_digest_mismatch: bool,
    ) -> Result<PreparedFinalize, UploadTransitionError> {
        let bucket = self.bucket().map_err(UploadTransitionError::Storage)?;

        // If upload is already finalized, return immediately with idempotent prepared handle
        if let Some(receipt) = self
            .get_finalized_receipt(session)
            .await
            .map_err(UploadTransitionError::Storage)?
        {
            if receipt.digest == expected_digest.as_str() {
                return Ok(PreparedFinalize {
                    session: session.clone(),
                    operation_id: "already-finalized".to_string(),
                    expected_digest: expected_digest.clone(),
                    committed_offset: receipt.size,
                    size: receipt.size,
                });
            } else {
                return Err(UploadTransitionError::DigestMismatch {
                    expected: expected_digest.clone(),
                    computed: receipt.digest.clone(),
                });
            }
        }

        let had_trailing = trailing_stream.is_some();
        if let Some(stream) = trailing_stream {
            let append_res = self
                .append_if_offset(session, expected_offset, stream, max_upload_bytes)
                .await?;
            match append_res {
                UploadAppendResult::Committed { .. } => {}
                UploadAppendResult::OffsetMismatch { current_offset } => {
                    return Err(UploadTransitionError::OffsetMismatch {
                        expected: expected_offset,
                        current: current_offset,
                    });
                }
                UploadAppendResult::Conflict => return Err(UploadTransitionError::Conflict),
            }
        }

        let (mut doc, etag) = match self
            .get_session_doc_with_etag(&session.uuid)
            .await
            .map_err(UploadTransitionError::Storage)?
        {
            Some(p) => p,
            None => {
                if let Some(receipt) = self
                    .get_finalized_receipt(session)
                    .await
                    .map_err(UploadTransitionError::Storage)?
                {
                    if receipt.digest == expected_digest.as_str() {
                        return Ok(PreparedFinalize {
                            session: session.clone(),
                            operation_id: "already-finalized".to_string(),
                            expected_digest: expected_digest.clone(),
                            committed_offset: receipt.size,
                            size: receipt.size,
                        });
                    } else {
                        return Err(UploadTransitionError::DigestMismatch {
                            expected: expected_digest.clone(),
                            computed: receipt.digest.clone(),
                        });
                    }
                }
                return Err(UploadTransitionError::NotFound);
            }
        };

        if doc.repo != session.repo || doc.uuid != session.uuid {
            return Err(UploadTransitionError::NotFound);
        }

        if !had_trailing {
            match expected_offset {
                UploadOffsetPrecondition::Exact(off) => {
                    if off != doc.committed_offset {
                        return Err(UploadTransitionError::OffsetMismatch {
                            expected: expected_offset,
                            current: doc.committed_offset,
                        });
                    }
                }
                UploadOffsetPrecondition::CurrentForServerComposedMonolithicOperation => {}
            }
        }

        let data_key = self.multipart_data_key(&session.uuid);

        // Upload any remaining pending bytes as the final part (S3 allows the last part to be < 5 MiB)
        if let Some(ref pending_key) = doc.pending_buffer_key
            && let Ok(Some((bytes, _))) = self.driver.get_object(bucket, pending_key).await
            && !bytes.is_empty()
        {
            let final_part_num = doc
                .committed_parts
                .iter()
                .map(|p| p.part_number)
                .max()
                .unwrap_or(0)
                .saturating_add(1);

            let part_etag = self
                .driver
                .upload_part(
                    bucket,
                    &data_key,
                    &doc.multipart_upload_id,
                    final_part_num,
                    bytes.clone(),
                )
                .await
                .map_err(UploadTransitionError::Storage)?;

            doc.committed_parts.push(S3CommittedPart {
                part_number: final_part_num,
                size: bytes.len() as u64,
                etag: part_etag,
            });
        }
        if let Some(ref pending_key) = doc.pending_buffer_key {
            let _ = self.driver.delete_object(bucket, pending_key).await;
            doc.pending_buffer_key = None;
            doc.pending_bytes = 0;
        }

        let operation_id = uuid::Uuid::new_v4().to_string();
        let now = self.driver.now_unix_secs();

        // STEP 1: Persist Finalizing state via conditional CAS BEFORE completing multipart upload
        doc.state = UploadSessionState::Finalizing;
        doc.finalizing_info = Some(S3FinalizingInfo {
            operation_id: operation_id.clone(),
            expected_digest: expected_digest.as_str().to_string(),
            size: doc.committed_offset,
            finalizing_at_unix_secs: now,
            multipart_completed: false,
        });

        let mut finalizing_etag = self
            .put_session_doc_conditional(&session.uuid, &doc, Some(&etag))
            .await
            .map_err(UploadTransitionError::Storage)?;

        // STEP 2: Complete multipart upload
        let completed_parts: Vec<(i32, String)> = doc
            .committed_parts
            .iter()
            .map(|p| (p.part_number, p.etag.clone()))
            .collect();

        self.driver
            .complete_multipart_upload(bucket, &data_key, &doc.multipart_upload_id, completed_parts)
            .await
            .map_err(UploadTransitionError::Storage)?;

        // STEP 3: Record multipart completion in session metadata
        doc.finalizing_info.as_mut().unwrap().multipart_completed = true;
        if let Ok(updated_etag) = self
            .put_session_doc_conditional(&session.uuid, &doc, Some(&finalizing_etag))
            .await
        {
            finalizing_etag = updated_etag;
        }
        let _ = finalizing_etag;

        // STEP 4: Verify digest by reading staged object
        let staged_bytes = match self.driver.get_object(bucket, &data_key).await {
            Ok(Some((b, _))) => b,
            Ok(None) => return Err(UploadTransitionError::NotFound),
            Err(e) => return Err(UploadTransitionError::Storage(e)),
        };

        let computed_hex = if expected_digest.algorithm() == "sha512" {
            let mut hasher = sha2::Sha512::new();
            hasher.update(&staged_bytes);
            hex::encode(hasher.finalize())
        } else {
            let mut hasher = sha2::Sha256::new();
            hasher.update(&staged_bytes);
            hex::encode(hasher.finalize())
        };

        if computed_hex != expected_digest.hex() {
            if abort_on_digest_mismatch {
                let _ = self.driver.delete_object(bucket, &data_key).await;
                let _ = self
                    .driver
                    .delete_object(bucket, &self.session_key(&session.uuid))
                    .await;
            }
            return Err(UploadTransitionError::DigestMismatch {
                expected: expected_digest.clone(),
                computed: computed_hex,
            });
        }

        Ok(PreparedFinalize {
            session: session.clone(),
            operation_id,
            expected_digest: expected_digest.clone(),
            committed_offset: doc.committed_offset,
            size: doc.committed_offset,
        })
    }

    async fn commit_finalize(
        &self,
        prepared: &PreparedFinalize,
    ) -> Result<FinalizeOutcome, UploadTransitionError> {
        let bucket = self.bucket().map_err(UploadTransitionError::Storage)?;

        // 1. Check receipt
        if let Some(receipt) = self
            .get_finalized_receipt(&prepared.session)
            .await
            .map_err(UploadTransitionError::Storage)?
            && receipt.digest == prepared.expected_digest.as_str()
        {
            return Ok(FinalizeOutcome::AlreadyFinalized(BlobMeta {
                size: receipt.size,
            }));
        }

        // 2. Validate session
        let (doc, _etag) = match self
            .get_session_doc_with_etag(&prepared.session.uuid)
            .await
            .map_err(UploadTransitionError::Storage)?
        {
            Some(p) => p,
            None => {
                // Check if blob exists in CAS store
                let dest_key = self.blob_key2(&prepared.expected_digest);
                if let Ok(Some(size)) = self.driver.head_object(bucket, &dest_key).await {
                    let receipt = FinalizedReceipt {
                        repo: prepared.session.repo.clone(),
                        uuid: prepared.session.uuid.clone(),
                        digest: prepared.expected_digest.as_str().to_string(),
                        size,
                        finalized_at_unix_secs: self.driver.now_unix_secs(),
                        format_version: 1,
                    };
                    let receipt_bytes = serde_json::to_vec(&receipt).unwrap();
                    let _ = self
                        .driver
                        .put_object_conditional(
                            bucket,
                            &self.finalized_key(&prepared.session.uuid),
                            Bytes::from(receipt_bytes),
                            None,
                            None,
                        )
                        .await;
                    return Ok(FinalizeOutcome::AlreadyFinalized(BlobMeta { size }));
                }
                return Err(UploadTransitionError::NotFound);
            }
        };

        if doc.state != UploadSessionState::Finalizing {
            return Err(UploadTransitionError::InvalidPreparedHandle);
        }
        let Some(ref fin_info) = doc.finalizing_info else {
            return Err(UploadTransitionError::InvalidPreparedHandle);
        };
        if fin_info.operation_id != prepared.operation_id
            || fin_info.expected_digest != prepared.expected_digest.as_str()
        {
            return Err(UploadTransitionError::InvalidPreparedHandle);
        }

        // Copy into CAS destination
        let data_key = self.multipart_data_key(&prepared.session.uuid);
        let dest_key = self.blob_key2(&prepared.expected_digest);

        self.driver
            .copy_object(bucket, &data_key, bucket, &dest_key)
            .await
            .map_err(UploadTransitionError::Storage)?;

        // STEP 5: Durably create target repository membership BEFORE receipt
        let membership = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
            prepared.session.repo.clone(),
            prepared.expected_digest.clone(),
            Some(prepared.session.uuid.clone()),
        );
        self.link_repo_blob(&membership)
            .await
            .map_err(UploadTransitionError::Storage)?;

        // Write finalized receipt
        let now = self.driver.now_unix_secs();
        let receipt = FinalizedReceipt {
            repo: prepared.session.repo.clone(),
            uuid: prepared.session.uuid.clone(),
            digest: prepared.expected_digest.as_str().to_string(),
            size: prepared.size,
            finalized_at_unix_secs: now,
            format_version: 1,
        };
        let receipt_bytes = serde_json::to_vec(&receipt)
            .map_err(|e| UploadTransitionError::Storage(StorageError::Internal(e.to_string())))?;
        self.driver
            .put_object_conditional(
                bucket,
                &self.finalized_key(&prepared.session.uuid),
                Bytes::from(receipt_bytes),
                None,
                None,
            )
            .await
            .map_err(UploadTransitionError::Storage)?;

        // Clean staging objects
        let _ = self.driver.delete_object(bucket, &data_key).await;
        let _ = self
            .driver
            .delete_object(bucket, &self.session_key(&prepared.session.uuid))
            .await;

        Ok(FinalizeOutcome::Published(BlobMeta {
            size: prepared.size,
        }))
    }

    async fn abort_session(&self, session: &UploadSessionId) -> Result<(), StorageError> {
        let bucket = self.bucket()?;

        if let Some((doc, _)) = self.get_session_doc_with_etag(&session.uuid).await? {
            let data_key = self.multipart_data_key(&session.uuid);
            let _ = self
                .driver
                .abort_multipart_upload(bucket, &data_key, &doc.multipart_upload_id)
                .await;
        }

        // Delete all objects under uploads/<uuid>/ (pending buffers, staging, session.json)
        let prefix = format!("uploads/{}/", session.uuid);
        if let Ok(objs) = self.driver.list_objects_v2(bucket, &prefix).await {
            for obj in objs {
                let _ = self.driver.delete_object(bucket, &obj.key).await;
            }
        }

        let _ = self
            .driver
            .delete_object(bucket, &self.session_key(&session.uuid))
            .await;
        Ok(())
    }

    async fn recover_session(
        &self,
        session: &UploadSessionId,
    ) -> Result<UploadSessionStatus, UploadTransitionError> {
        let bucket = self.bucket().map_err(UploadTransitionError::Storage)?;

        let (mut doc, _etag) = match self
            .get_session_doc_with_etag(&session.uuid)
            .await
            .map_err(UploadTransitionError::Storage)?
        {
            Some(p) => p,
            None => {
                if let Some(receipt) = self
                    .get_finalized_receipt(session)
                    .await
                    .map_err(UploadTransitionError::Storage)?
                {
                    return Ok(UploadSessionStatus {
                        session: session.clone(),
                        state: UploadSessionState::Finalizing,
                        committed_offset: receipt.size,
                        created_at: UNIX_EPOCH
                            + Duration::from_secs(receipt.finalized_at_unix_secs),
                        last_active_at: UNIX_EPOCH
                            + Duration::from_secs(receipt.finalized_at_unix_secs),
                    });
                }
                return Err(UploadTransitionError::NotFound);
            }
        };

        let now = self.driver.now_unix_secs();

        if doc.state == UploadSessionState::Finalizing
            && let Some(ref fin_info) = doc.finalizing_info
            && let Ok(digest) = Digest::parse(&fin_info.expected_digest)
        {
            let dest_key = self.blob_key2(&digest);
            if let Ok(Some(size)) = self.driver.head_object(bucket, &dest_key).await {
                let membership =
                    crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
                        session.repo.clone(),
                        digest.clone(),
                        Some(session.uuid.clone()),
                    );
                let _ = self.link_repo_blob(&membership).await;

                let receipt = FinalizedReceipt {
                    repo: session.repo.clone(),
                    uuid: session.uuid.clone(),
                    digest: fin_info.expected_digest.clone(),
                    size,
                    finalized_at_unix_secs: fin_info.finalizing_at_unix_secs,
                    format_version: 1,
                };
                let receipt_bytes = serde_json::to_vec(&receipt).unwrap();
                let _ = self
                    .driver
                    .put_object_conditional(
                        bucket,
                        &self.finalized_key(&session.uuid),
                        Bytes::from(receipt_bytes),
                        None,
                        None,
                    )
                    .await;
                let _ = self
                    .driver
                    .delete_object(bucket, &self.multipart_data_key(&session.uuid))
                    .await;
                let _ = self
                    .driver
                    .delete_object(bucket, &self.session_key(&session.uuid))
                    .await;
            } else {
                // Staged object may still exist, or multipart upload may need completion
                let data_key = self.multipart_data_key(&session.uuid);
                let mut staging_size = self
                    .driver
                    .head_object(bucket, &data_key)
                    .await
                    .ok()
                    .flatten();

                if staging_size.is_none() && !doc.multipart_upload_id.is_empty() {
                    let completed_parts: Vec<(i32, String)> = doc
                        .committed_parts
                        .iter()
                        .map(|p| (p.part_number, p.etag.clone()))
                        .collect();
                    if self
                        .driver
                        .complete_multipart_upload(
                            bucket,
                            &data_key,
                            &doc.multipart_upload_id,
                            completed_parts,
                        )
                        .await
                        .is_ok()
                    {
                        staging_size = self
                            .driver
                            .head_object(bucket, &data_key)
                            .await
                            .ok()
                            .flatten();
                    }
                }

                if let Some(size) = staging_size {
                    let _ = self
                        .driver
                        .copy_object(bucket, &data_key, bucket, &dest_key)
                        .await;
                    let membership =
                        crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
                            session.repo.clone(),
                            digest.clone(),
                            Some(session.uuid.clone()),
                        );
                    let _ = self.link_repo_blob(&membership).await;

                    let receipt = FinalizedReceipt {
                        repo: session.repo.clone(),
                        uuid: session.uuid.clone(),
                        digest: fin_info.expected_digest.clone(),
                        size,
                        finalized_at_unix_secs: fin_info.finalizing_at_unix_secs,
                        format_version: 1,
                    };
                    let receipt_bytes = serde_json::to_vec(&receipt).unwrap();
                    let _ = self
                        .driver
                        .put_object_conditional(
                            bucket,
                            &self.finalized_key(&session.uuid),
                            Bytes::from(receipt_bytes),
                            None,
                            None,
                        )
                        .await;
                    let _ = self.driver.delete_object(bucket, &data_key).await;
                    let _ = self
                        .driver
                        .delete_object(bucket, &self.session_key(&session.uuid))
                        .await;
                } else {
                    return Err(UploadTransitionError::Storage(StorageError::Internal(
                        "neither CAS blob nor staged object found for finalizing session"
                            .to_string(),
                    )));
                }
            }
        } else if doc.state == UploadSessionState::Appending {
            let lease_expired = doc
                .current_operation
                .as_ref()
                .map(|op| now > op.lease_expires_at_unix_secs)
                .unwrap_or(true);
            if lease_expired {
                doc.state = UploadSessionState::Active;
                doc.current_operation = None;
                let bytes = serde_json::to_vec(&doc).map_err(|e| {
                    UploadTransitionError::Storage(StorageError::Internal(e.to_string()))
                })?;
                let _ = self
                    .driver
                    .put_object_conditional(
                        bucket,
                        &self.session_key(&session.uuid),
                        Bytes::from(bytes),
                        None,
                        None,
                    )
                    .await;
            }
        }

        self.session_status(session).await
    }

    async fn get_finalized_receipt(
        &self,
        session: &UploadSessionId,
    ) -> Result<Option<FinalizedReceipt>, StorageError> {
        let bucket = self.bucket()?;
        let key = self.finalized_key(&session.uuid);
        match self.driver.get_object(bucket, &key).await? {
            Some((bytes, _)) => {
                let receipt: FinalizedReceipt = serde_json::from_slice(&bytes)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                if receipt.repo == session.repo && receipt.uuid == session.uuid {
                    Ok(Some(receipt))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    async fn reap_expired_sessions(
        &self,
        max_age_secs: u64,
        receipt_ttl_secs: u64,
    ) -> Result<usize, StorageError> {
        let bucket = self.bucket()?;
        let prefix = self.key("uploads/");
        let mut count = 0;

        let objects = self.driver.list_objects_v2(bucket, &prefix).await?;
        let now = self.driver.now_unix_secs();

        let effective_receipt_ttl = if receipt_ttl_secs > 0 {
            receipt_ttl_secs
        } else {
            self.session_config.receipt_lifetime_secs
        };

        for obj in &objects {
            let key = &obj.key;
            if key.ends_with("/session.json") {
                if let Ok(Some((bytes, _))) = self.driver.get_object(bucket, key).await
                    && let Ok(doc) = serde_json::from_slice::<S3SessionDoc>(&bytes)
                    && now.saturating_sub(doc.last_active_at_unix_secs) >= max_age_secs
                {
                    let session = UploadSessionId::new(&doc.repo, &doc.uuid);
                    if doc.state == UploadSessionState::Finalizing {
                        let _ = self.recover_session(&session).await;
                        count += 1;
                    } else if doc.state == UploadSessionState::Appending {
                        let _ = self.recover_session(&session).await;
                        let _ = self.abort_session(&session).await;
                        count += 1;
                    } else {
                        let _ = self.abort_session(&session).await;
                        count += 1;
                    }
                }
            } else if key.ends_with("/finalized.json") {
                if let Ok(Some((bytes, _))) = self.driver.get_object(bucket, key).await
                    && let Ok(receipt) = serde_json::from_slice::<FinalizedReceipt>(&bytes)
                    && now.saturating_sub(receipt.finalized_at_unix_secs) >= effective_receipt_ttl
                {
                    let _ = self.driver.delete_object(bucket, key).await;
                    count += 1;
                }
            } else if key.contains("/pending/")
                && now.saturating_sub(obj.last_modified_unix_secs) >= max_age_secs
            {
                let _ = self.driver.delete_object(bucket, key).await;
                count += 1;
            }
        }

        let mp_cutoff = now.saturating_sub(max_age_secs);
        if let Ok(mp_count) = self.reap_orphaned_multipart_uploads(mp_cutoff).await {
            count += mp_count;
        }

        Ok(count)
    }
}

#[async_trait]
impl crate::storage::repo_membership::RepositoryBlobMembershipStorage for S3Storage {
    async fn get_repo_blob_membership(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<Option<crate::storage::repo_membership::RepoBlobMembershipRecord>, StorageError>
    {
        let bucket = self.bucket()?;
        let key = self.repo_blob_key(repo, digest);
        if let Some((bytes, _etag)) = self.driver.get_object(bucket, &key).await? {
            let record = serde_json::from_slice::<
                crate::storage::repo_membership::RepoBlobMembershipRecord,
            >(&bytes)
            .map_err(|e| {
                StorageError::Internal(format!("corrupt membership record in s3 key {key}: {e}"))
            })?;
            return Ok(Some(record));
        }

        Ok(None)
    }

    async fn link_repo_blob(
        &self,
        record: &crate::storage::repo_membership::RepoBlobMembershipRecord,
    ) -> Result<(), StorageError> {
        let bucket = self.bucket()?;
        let key = self.repo_blob_key(&record.repo, &record.digest);
        let bytes = serde_json::to_vec(record)
            .map_err(|e| StorageError::Internal(format!("serialize membership error: {e}")))?;

        self.driver
            .put_object_conditional(bucket, &key, Bytes::from(bytes), None, None)
            .await?;
        Ok(())
    }

    async fn set_membership_candidate(
        &self,
        repo: &str,
        digest: &Digest,
        since_unix_secs: u64,
    ) -> Result<bool, StorageError> {
        let bucket = self.bucket()?;
        let key = self.repo_blob_key(repo, digest);
        if let Some((bytes, etag)) = self.driver.get_object(bucket, &key).await? {
            let mut record = serde_json::from_slice::<
                crate::storage::repo_membership::RepoBlobMembershipRecord,
            >(&bytes)
            .map_err(|e| StorageError::Internal(format!("corrupt membership record in s3: {e}")))?;

            if record.state != crate::storage::repo_membership::MembershipState::Candidate
                || record.unreferenced_since_unix_secs != Some(since_unix_secs)
            {
                record.mark_candidate(since_unix_secs);
                let new_bytes = serde_json::to_vec(&record).map_err(|e| {
                    StorageError::Internal(format!("serialize membership error: {e}"))
                })?;

                match self
                    .driver
                    .put_object_conditional(bucket, &key, Bytes::from(new_bytes), Some(etag), None)
                    .await
                {
                    Ok(_) => Ok(true),
                    Err(StorageError::TagAlreadyExists) => Ok(false),
                    Err(StorageError::Internal(err))
                        if err.contains("412")
                            || err.contains("PreconditionFailed")
                            || err.contains("AtLeastOneConditionFailed") =>
                    {
                        // Stale ETag: concurrent modification occurred, return false safely
                        Ok(false)
                    }
                    Err(e) => Err(e),
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    async fn clear_membership_candidate(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<bool, StorageError> {
        let bucket = self.bucket()?;
        let key = self.repo_blob_key(repo, digest);
        if let Some((bytes, etag)) = self.driver.get_object(bucket, &key).await? {
            let mut record = serde_json::from_slice::<
                crate::storage::repo_membership::RepoBlobMembershipRecord,
            >(&bytes)
            .map_err(|e| StorageError::Internal(format!("corrupt membership record in s3: {e}")))?;

            if record.state != crate::storage::repo_membership::MembershipState::Active
                || record.unreferenced_since_unix_secs.is_some()
            {
                record.mark_active();
                let new_bytes = serde_json::to_vec(&record).map_err(|e| {
                    StorageError::Internal(format!("serialize membership error: {e}"))
                })?;

                match self
                    .driver
                    .put_object_conditional(bucket, &key, Bytes::from(new_bytes), Some(etag), None)
                    .await
                {
                    Ok(_) => Ok(true),
                    Err(StorageError::TagAlreadyExists) => Ok(false),
                    Err(StorageError::Internal(err))
                        if err.contains("412")
                            || err.contains("PreconditionFailed")
                            || err.contains("AtLeastOneConditionFailed") =>
                    {
                        // Stale ETag: concurrent modification occurred, return false safely
                        Ok(false)
                    }
                    Err(e) => Err(e),
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    async fn unlink_repo_blob(&self, repo: &str, digest: &Digest) -> Result<bool, StorageError> {
        let bucket = self.bucket()?;
        let key = self.repo_blob_key(repo, digest);
        let _ = self.driver.delete_object(bucket, &key).await;
        Ok(true)
    }

    async fn list_repo_blob_memberships_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<
        (
            Vec<crate::storage::repo_membership::RepoBlobMembershipRecord>,
            Option<String>,
        ),
        StorageError,
    > {
        let bucket = self.bucket()?;
        let prefix = self.repo_blobs_prefix(repo);
        let objects = self.driver.list_objects_v2(bucket, &prefix).await?;

        let mut all_keys: Vec<String> = objects
            .into_iter()
            .map(|o| o.key)
            .filter(|k| k.starts_with(&prefix))
            .collect();
        all_keys.sort();

        let start_idx = if let Some(token) = continuation_token {
            match all_keys.binary_search_by(|k| k.as_str().cmp(token)) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            }
        } else {
            0
        };

        let end_idx = (start_idx + page_limit).min(all_keys.len());
        let page_slice = &all_keys[start_idx..end_idx];

        let mut records = Vec::new();
        for key in page_slice {
            if let Some((bytes, _etag)) = self.driver.get_object(bucket, key).await?
                && let Ok(rec) = serde_json::from_slice::<
                    crate::storage::repo_membership::RepoBlobMembershipRecord,
                >(&bytes)
            {
                records.push(rec);
            }
        }

        let next_token = if end_idx < all_keys.len() {
            page_slice.last().cloned()
        } else {
            None
        };

        Ok((records, next_token))
    }

    async fn list_all_repo_blob_memberships_page(
        &self,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<
        (
            Vec<crate::storage::repo_membership::RepoBlobMembershipRecord>,
            Option<String>,
        ),
        StorageError,
    > {
        let bucket = self.bucket()?;
        let prefix = self.all_memberships_prefix();
        let objects = self.driver.list_objects_v2(bucket, &prefix).await?;

        let mut all_keys: Vec<String> = objects
            .into_iter()
            .map(|o| o.key)
            .filter(|k| k.starts_with(&prefix))
            .collect();
        all_keys.sort();

        let start_idx = if let Some(token) = continuation_token {
            match all_keys.binary_search_by(|k| k.as_str().cmp(token)) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            }
        } else {
            0
        };

        let end_idx = (start_idx + page_limit).min(all_keys.len());
        let page_slice = &all_keys[start_idx..end_idx];

        let mut records = Vec::new();
        for key in page_slice {
            if let Some((bytes, _etag)) = self.driver.get_object(bucket, key).await?
                && let Ok(rec) = serde_json::from_slice::<
                    crate::storage::repo_membership::RepoBlobMembershipRecord,
                >(&bytes)
            {
                records.push(rec);
            }
        }

        let next_token = if end_idx < all_keys.len() {
            page_slice.last().cloned()
        } else {
            None
        };

        Ok((records, next_token))
    }

    async fn count_repo_blob_memberships(&self, digest: &Digest) -> Result<usize, StorageError> {
        let repos = self.list_repositories().await?;
        let mut count = 0;
        for repo in repos {
            if self
                .get_repo_blob_membership(&repo, digest)
                .await?
                .is_some()
            {
                count += 1;
            }
        }
        Ok(count)
    }

    async fn is_membership_ready(&self) -> Result<bool, StorageError> {
        let checkpoint = self.get_migration_checkpoint().await?;
        let bucket = self.bucket()?;
        let key = self.key("meta/membership_ready.json");
        let ready_marker_exists = self.driver.head_object(bucket, &key).await?.is_some();
        match checkpoint {
            Some(cp) => Ok(
                cp.phase == crate::storage::repo_membership::MigrationPhase::Ready
                    && ready_marker_exists,
            ),
            None => Ok(ready_marker_exists),
        }
    }

    async fn mark_membership_ready(&self) -> Result<(), StorageError> {
        let bucket = self.bucket()?;
        let key = self.key("meta/membership_ready.json");
        let now = self.driver.now_unix_secs();
        let payload = serde_json::json!({
            "version": 1,
            "ready_at_unix_secs": now,
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        self.driver
            .put_object_conditional(bucket, &key, Bytes::from(bytes), None, None)
            .await?;

        let cp = self.get_migration_checkpoint().await?;
        let ready_cp = match cp {
            Some(mut c) => {
                c.phase = crate::storage::repo_membership::MigrationPhase::Ready;
                c.verification_result = Some(true);
                c.last_updated_unix_secs = now;
                c
            }
            None => crate::storage::repo_membership::MigrationCheckpointRecord {
                schema_version: 1,
                phase: crate::storage::repo_membership::MigrationPhase::Ready,
                owner_id: None,
                lease_expiry_unix_secs: None,
                source_continuation_token: None,
                current_repository: None,
                current_cursor: None,
                stats: crate::storage::repo_membership::MigrationStats::default(),
                started_unix_secs: now,
                last_updated_unix_secs: now,
                failure_info: None,
                verification_result: Some(true),
            },
        };
        self.save_migration_checkpoint(&ready_cp).await?;
        Ok(())
    }

    async fn get_migration_checkpoint(
        &self,
    ) -> Result<Option<crate::storage::repo_membership::MigrationCheckpointRecord>, StorageError>
    {
        let bucket = self.bucket()?;
        let key = self.key("meta/migration_checkpoint.json");
        if let Some((bytes, _etag)) = self.driver.get_object(bucket, &key).await? {
            let rec = serde_json::from_slice::<
                crate::storage::repo_membership::MigrationCheckpointRecord,
            >(&bytes)
            .map_err(|e| StorageError::Internal(format!("corrupt s3 migration checkpoint: {e}")))?;
            return Ok(Some(rec));
        }
        Ok(None)
    }

    async fn save_migration_checkpoint(
        &self,
        checkpoint: &crate::storage::repo_membership::MigrationCheckpointRecord,
    ) -> Result<(), StorageError> {
        let bucket = self.bucket()?;
        let key = self.key("meta/migration_checkpoint.json");
        let bytes = serde_json::to_vec(checkpoint).map_err(|e| {
            StorageError::Internal(format!("serialize s3 migration checkpoint: {e}"))
        })?;
        self.driver
            .put_object_conditional(bucket, &key, Bytes::from(bytes), None, None)
            .await?;
        Ok(())
    }
}

#[allow(dead_code)]
pub mod tests {
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
            for (k, (b, _)) in objs.iter() {
                if k.starts_with(prefix) {
                    out.push(S3ObjectSummary {
                        key: k.clone(),
                        size: b.len() as u64,
                        last_modified_unix_secs: clock,
                    });
                }
            }
            drop(objs);

            self.check_after_hook("list_objects_v2", prefix)?;

            Ok(out)
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

    pub(crate) fn create_mock_storage() -> (S3Storage, Arc<MockS3Driver>) {
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
            repo: "test/repo".to_string(),
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
            repo: "my/repo".to_string(),
            uuid: "98765432-1234-1234-1234-1234567890ab".to_string(),
            digest: "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
                .to_string(),
            size: 10485760,
            finalized_at_unix_secs: 1740000100,
            format_version: 1,
        };

        let json = serde_json::to_vec(&receipt).unwrap();
        let decoded: FinalizedReceipt = serde_json::from_slice(&json).unwrap();

        assert_eq!(decoded.repo, "my/repo");
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
        assert_eq!(session.repo, "repo1");
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

        let wrong_digest = Digest::parse(
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        )
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

        let wrong_digest = Digest::parse(
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        )
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
        let diff_digest = Digest::parse(
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        )
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

        let digest = Digest::parse(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
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
                        repo: "race-repo".into(),
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
        let digest = Digest::parse(
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )
        .unwrap();
        let rec1 = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
            "repo-a",
            digest.clone(),
            Some("uuid-1".into()),
        );
        let rec2 = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
            "repo-a",
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
        assert_eq!(fetched.repo, "repo-a");
        assert_eq!(fetched.digest, digest);
    }

    #[tokio::test]
    async fn test_s3_membership_candidate_transition_racing_activation_fails_safe_on_412() {
        let (storage, driver) = create_mock_storage();
        let digest = Digest::parse(
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )
        .unwrap();
        let rec = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
            "racing-repo",
            digest.clone(),
            None,
        );
        storage.link_repo_blob(&rec).await.unwrap();

        // Inject hook to simulate concurrent modification (etag change) during set_candidate
        let key = storage.repo_blob_key("racing-repo", &digest);
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

        let changed = storage
            .set_membership_candidate("racing-repo", &digest, 1000)
            .await
            .unwrap();
        assert!(
            !changed,
            "Stale ETag update on set_candidate must return false safely without error or corrupting state"
        );
    }

    #[tokio::test]
    async fn test_s3_membership_corrupt_record_fails_closed() {
        let (storage, driver) = create_mock_storage();
        let digest = Digest::parse(
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )
        .unwrap();
        let key = storage.repo_blob_key("corrupt-repo", &digest);

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
    async fn test_s3_membership_pagination_with_continuation_and_sha512() {
        let (storage, _driver) = create_mock_storage();
        let repo = "paged-repo";

        let d_sha256 = Digest::parse(
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )
        .unwrap();
        let d_sha512 = Digest::parse("sha512:ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f").unwrap();

        let rec1 = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
            repo,
            d_sha256.clone(),
            None,
        );
        let rec2 = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
            repo,
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
        let digest = Digest::parse(
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )
        .unwrap();

        // Repo "foo" vs Repo "foo/bar" vs Repo "foo-bar"
        let rec_foo = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
            "foo",
            digest.clone(),
            None,
        );
        let rec_foo_bar = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
            "foo/bar",
            digest.clone(),
            None,
        );
        let rec_foo_dash = crate::storage::repo_membership::RepoBlobMembershipRecord::new_upload(
            "foo-bar",
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
        assert_eq!(p_foo[0].repo, "foo");
        assert_eq!(p_bar[0].repo, "foo/bar");
        assert_eq!(p_dash[0].repo, "foo-bar");
    }

    #[tokio::test]
    async fn test_s3_migration_plan_performs_zero_writes() {
        let (storage, driver) = create_mock_storage();
        let storage_arc: Arc<dyn Storage> = Arc::new(storage);

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
        let storage_arc: Arc<dyn Storage> = Arc::new(storage);

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
        let storage_arc: Arc<dyn Storage> = Arc::new(storage);

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
        let storage_arc: Arc<dyn Storage> = Arc::new(storage);

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
        let digest = Digest::parse(
            "sha256:4444444444444444444444444444444444444444444444444444444444444444",
        )
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
}
