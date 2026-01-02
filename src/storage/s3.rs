use super::{BlobMeta, ManifestMeta, ReferrerDescriptor, Storage, StorageError, UploadMeta};
use crate::registry::digest::Digest;
use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_s3::config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use base64::Engine as _;
use bytes::Bytes;
use sha2::Digest as _;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::OnceCell;

#[derive(Debug)]
pub struct S3Storage {
    endpoint: Option<String>,
    region: Option<String>,
    bucket: Option<String>,
    prefix: String,
    max_upload_bytes: u64,
    client: OnceCell<Client>,
}

impl S3Storage {
    pub fn new(
        endpoint: Option<String>,
        region: Option<String>,
        bucket: Option<String>,
        prefix: String,
        max_upload_bytes: u64,
    ) -> Self {
        Self {
            endpoint,
            region,
            bucket,
            prefix,
            max_upload_bytes,
            client: OnceCell::new(),
        }
    }

    fn bucket(&self) -> Result<&str, StorageError> {
        self.bucket
            .as_deref()
            .ok_or_else(|| StorageError::Internal("STORAGE_S3_BUCKET is required".to_string()))
    }

    fn region(&self) -> Result<&str, StorageError> {
        self.region
            .as_deref()
            .ok_or_else(|| StorageError::Internal("STORAGE_S3_REGION is required".to_string()))
    }

    async fn client(&self) -> Result<Client, StorageError> {
        let c = self
            .client
            .get_or_try_init(|| async {
                let loader = aws_config::defaults(BehaviorVersion::latest()).region(Region::new(
                    self.region()?.to_string(),
                ));
                // Credentials are loaded from the standard AWS env/metadata chain.
                let shared = loader.load().await;

                let mut builder = aws_sdk_s3::config::Builder::from(&shared);
                if let Some(ep) = self.endpoint.as_deref() {
                    builder = builder.endpoint_url(ep);
                    // Most S3-compatible endpoints (e.g. MinIO) prefer path-style.
                    builder = builder.force_path_style(true);
                }
                Ok::<_, StorageError>(Client::from_conf(builder.build()))
            })
            .await?;
        Ok(c.clone())
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
        self.key(&format!("blobs/sha256/{}/{}", digest.prefix2(), digest.hex()))
    }

    fn manifest_key(&self, name: &str, digest: &Digest) -> String {
        self.key(&format!("repos/{name}/manifests/{}", digest.hex()))
    }

    fn tag_key(&self, name: &str, tag: &str) -> String {
        self.key(&format!("repos/{name}/tags/{tag}"))
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

    fn encode_upload_token(base_uuid: &str, upload_id: &str) -> String {
        let upload_id_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(upload_id);
        format!("{base_uuid}~{upload_id_b64}")
    }

    fn decode_upload_token(token: &str) -> Result<(String, String), StorageError> {
        let (base, b64) = token
            .split_once('~')
            .ok_or_else(|| StorageError::NotFound)?;
        let upload_id_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(b64.as_bytes())
            .map_err(|_| StorageError::NotFound)?;
        let upload_id = String::from_utf8(upload_id_bytes)
            .map_err(|_| StorageError::NotFound)?;
        Ok((base.to_string(), upload_id))
    }

    async fn get_object_bytes(&self, key: &str) -> Result<Bytes, StorageError> {
        let client = self.client().await?;
        let bucket = self.bucket()?;
        let resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| map_s3_err(err))?;
        let data = resp
            .body
            .collect()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?
            .into_bytes();
        Ok(Bytes::from(data))
    }

    async fn detect_manifest_media_type(&self, bytes: &[u8]) -> Result<String, StorageError> {
        let value: serde_json::Value = serde_json::from_slice(bytes)
            .map_err(|err| StorageError::Internal(err.to_string()))?;
        let media_type = value
            .get("mediaType")
            .and_then(|v| v.as_str())
            .unwrap_or("application/vnd.oci.image.manifest.v1+json");
        Ok(media_type.to_string())
    }
}

fn map_s3_err(err: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>) -> StorageError {
    // Basic mapping for our usage.
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

fn map_head_err(err: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::head_object::HeadObjectError>) -> StorageError {
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

fn map_put_err(err: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>) -> StorageError {
    match err {
        aws_sdk_s3::error::SdkError::ServiceError(se) => StorageError::Internal(se.err().to_string()),
        other => StorageError::Internal(other.to_string()),
    }
}

#[async_trait]
impl Storage for S3Storage {
    fn kind(&self) -> &'static str {
        "s3"
    }

    async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError> {
        let client = self.client().await?;
        let bucket = self.bucket()?;
        let key = self.blob_key2(digest);
        let resp = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| map_head_err(err))?;
        Ok(BlobMeta {
            size: resp.content_length().unwrap_or(0) as u64,
        })
    }

    async fn open_blob(
        &self,
        digest: &Digest,
    ) -> Result<(BlobMeta, Pin<Box<dyn AsyncRead + Send>>), StorageError> {
        let client = self.client().await?;
        let bucket = self.bucket()?;
        let key = self.blob_key2(digest);
        let resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| map_s3_err(err))?;

        let size = resp.content_length().unwrap_or(0) as u64;
        // ByteStream provides an AsyncRead adapter.
        let reader = resp
            .body
            .into_async_read();
        Ok((BlobMeta { size }, Box::pin(reader)))
    }

    async fn resolve_tag(&self, name: &str, tag: &str) -> Result<Digest, StorageError> {
        let key = self.tag_key(name, tag);
        let bytes = self.get_object_bytes(&key).await?;
        let s = std::str::from_utf8(&bytes)
            .map_err(|_| StorageError::Internal("invalid tag pointer".to_string()))?;
        Digest::parse(s.trim()).map_err(|_| StorageError::NotFound)
    }

    async fn list_tags(&self, name: &str) -> Result<Vec<String>, StorageError> {
        let client = self.client().await?;
        let bucket = self.bucket()?;
        let prefix = self.tags_prefix(name);
        let resp = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix.clone())
            .send()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;

        let mut tags = Vec::new();
        for obj in resp.contents() {
            if let Some(k) = obj.key() {
                if let Some(rest) = k.strip_prefix(&prefix) {
                    if !rest.is_empty() {
                        tags.push(rest.to_string());
                    }
                }
            }
        }
        tags.sort();
        Ok(tags)
    }

    async fn head_manifest(&self, name: &str, digest: &Digest) -> Result<ManifestMeta, StorageError> {
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
        let client = self.client().await?;
        let bucket = self.bucket()?;
        let key = self.manifest_key(name, digest);

        let media_type = self.detect_manifest_media_type(&bytes).await?;

        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(bytes.clone()))
            .send()
            .await
            .map_err(|err| map_put_err(err))?;

        Ok(ManifestMeta {
            size: bytes.len() as u64,
            media_type,
        })
    }

    async fn set_tag(&self, name: &str, tag: &str, digest: &Digest) -> Result<(), StorageError> {
        let client = self.client().await?;
        let bucket = self.bucket()?;
        let key = self.tag_key(name, tag);
        let body = format!("{}\n", digest.as_str());
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.into_bytes()))
            .send()
            .await
            .map_err(|err| map_put_err(err))?;
        Ok(())
    }

    async fn create_upload(&self) -> Result<UploadMeta, StorageError> {
        let client = self.client().await?;
        let bucket = self.bucket()?;
        let base_uuid = uuid::Uuid::new_v4().to_string();
        let key = self.upload_key(&base_uuid);

        let resp = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;

        let upload_id = resp
            .upload_id()
            .ok_or_else(|| StorageError::Internal("missing upload_id".to_string()))?;

        Ok(UploadMeta {
            uuid: Self::encode_upload_token(&base_uuid, upload_id),
            offset: 0,
        })
    }

    async fn upload_status(&self, uuid: &str) -> Result<UploadMeta, StorageError> {
        let client = self.client().await?;
        let bucket = self.bucket()?;
        let (base_uuid, upload_id) = Self::decode_upload_token(uuid)?;
        let key = self.upload_key(&base_uuid);

        let resp = client
            .list_parts()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;

        let mut offset = 0u64;
        for p in resp.parts() {
            offset = offset.saturating_add(p.size().unwrap_or(0) as u64);
        }

        Ok(UploadMeta {
            uuid: uuid.to_string(),
            offset,
        })
    }

    async fn append_upload(&self, uuid: &str, chunk: Bytes) -> Result<UploadMeta, StorageError> {
        let client = self.client().await?;
        let bucket = self.bucket()?;
        let (base_uuid, upload_id) = Self::decode_upload_token(uuid)?;
        let key = self.upload_key(&base_uuid);

        // Determine next part number and current offset.
        let resp = client
            .list_parts()
            .bucket(bucket)
            .key(&key)
            .upload_id(&upload_id)
            .send()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;

        let mut offset = 0u64;
        let mut max_part = 0i32;
        for p in resp.parts() {
            offset = offset.saturating_add(p.size().unwrap_or(0) as u64);
            if let Some(n) = p.part_number() {
                max_part = max_part.max(n);
            }
        }

        let next_len = offset.saturating_add(chunk.len() as u64);
        if next_len > self.max_upload_bytes {
            return Err(StorageError::TooLarge);
        }

        let part_number = max_part.saturating_add(1);
        client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(chunk.clone()))
            .send()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;

        Ok(UploadMeta {
            uuid: uuid.to_string(),
            offset: next_len,
        })
    }

    async fn finalize_upload(&self, uuid: &str, digest: &Digest) -> Result<BlobMeta, StorageError> {
        let client = self.client().await?;
        let bucket = self.bucket()?;
        let (base_uuid, upload_id) = Self::decode_upload_token(uuid)?;
        let upload_key = self.upload_key(&base_uuid);

        // List all parts (need ETags) and complete.
        let parts_resp = client
            .list_parts()
            .bucket(bucket)
            .key(&upload_key)
            .upload_id(&upload_id)
            .send()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;

        let mut completed_parts: Vec<CompletedPart> = Vec::new();
        let mut total = 0u64;
        for p in parts_resp.parts() {
            total = total.saturating_add(p.size().unwrap_or(0) as u64);
            let Some(etag) = p.e_tag() else {
                return Err(StorageError::Internal("missing part etag".to_string()));
            };
            let Some(num) = p.part_number() else {
                return Err(StorageError::Internal("missing part number".to_string()));
            };
            completed_parts.push(
                CompletedPart::builder()
                    .set_e_tag(Some(etag.to_string()))
                    .set_part_number(Some(num))
                    .build(),
            );
        }

        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(&upload_key)
            .upload_id(&upload_id)
            .multipart_upload(upload)
            .send()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;

        // Verify digest by streaming the staged object.
        let get = client
            .get_object()
            .bucket(bucket)
            .key(&upload_key)
            .send()
            .await
            .map_err(|err| map_s3_err(err))?;

        let mut hasher = sha2::Sha256::new();
        let mut reader = get.body.into_async_read();
        let mut buf = vec![0u8; 1024 * 64];
        loop {
            let n = reader
                .read(&mut buf)
                .await
                .map_err(|err| StorageError::Internal(err.to_string()))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let computed_hex = hex::encode(hasher.finalize());
        if computed_hex != digest.hex() {
            // Best-effort cleanup.
            let _ = client
                .delete_object()
                .bucket(bucket)
                .key(&upload_key)
                .send()
                .await;
            return Err(StorageError::DigestMismatch);
        }

        // Copy into CAS blob key.
        let dest_key = self.blob_key2(digest);
        client
            .copy_object()
            .bucket(bucket)
            .key(&dest_key)
            .copy_source(format!("{bucket}/{upload_key}"))
            .send()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;

        // Clean up staged upload object.
        let _ = client
            .delete_object()
            .bucket(bucket)
            .key(&upload_key)
            .send()
            .await;

        Ok(BlobMeta { size: total })
    }

    async fn delete_blob(&self, digest: &Digest) -> Result<(), StorageError> {
        let client = self.client().await?;
        let bucket = self.bucket()?;
        let key = self.blob_key2(digest);

        // S3 deletion is idempotent; treat missing objects as NotFound when we can
        // detect it, otherwise return success.
        if let Err(err) = client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            let msg = err.to_string();
            if msg.contains("NoSuchKey") || msg.contains("NotFound") {
                return Err(StorageError::NotFound);
            }
            return Err(StorageError::Internal(msg));
        }
        Ok(())
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
        let client = self.client().await?;
        let bucket = self.bucket()?;
        let key = self.referrers_key(name, subject);

        let mut existing = self.list_referrers(name, subject).await?;
        if !existing.iter().any(|d| d.digest == descriptor.digest) {
            existing.push(descriptor);
        }
        let body = serde_json::to_vec(&existing)
            .map_err(|err| StorageError::Internal(err.to_string()))?;

        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(|err| map_put_err(err))?;
        Ok(())
    }

    async fn delete_manifest(&self, name: &str, digest: &Digest) -> Result<(), StorageError> {
        let client = self.client().await?;
        let bucket = self.bucket()?;

        let key = self.manifest_key(name, digest);
        // If the object doesn't exist, S3 can still return 204; treat it as success
        // unless we can clearly map it to NotFound.
        if let Err(err) = client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            let msg = err.to_string();
            if msg.contains("NoSuchKey") || msg.contains("NotFound") {
                return Err(StorageError::NotFound);
            }
            return Err(StorageError::Internal(msg));
        }

        // Remove any tags pointing to this digest.
        let digest_str = digest.as_str();
        let prefix = self.tags_prefix(name);
        let resp = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix.clone())
            .send()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;

        for obj in resp.contents() {
            let Some(tag_key) = obj.key() else { continue };
            let bytes = match self.get_object_bytes(tag_key).await {
                Ok(b) => b,
                Err(_) => continue,
            };
            let s = match std::str::from_utf8(&bytes) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if s.trim() == digest_str {
                let _ = client
                    .delete_object()
                    .bucket(bucket)
                    .key(tag_key)
                    .send()
                    .await;
            }
        }
        Ok(())
    }
}
