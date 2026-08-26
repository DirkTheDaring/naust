use crate::{
    config::{Config, StorageBackend},
    registry::digest::Digest,
};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;
use std::{pin::Pin, sync::Arc};
use thiserror::Error;
use tokio::io::AsyncRead;

pub mod fs;
pub mod repo_membership;
pub mod s3;
pub mod upload_session;

pub use repo_membership::*;
pub use upload_session::*;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("not found")]
    NotFound,

    #[error("digest mismatch")]
    DigestMismatch,

    #[error("unsupported")]
    Unsupported,

    #[error("too large")]
    TooLarge,

    #[error("insufficient storage")]
    InsufficientStorage,

    #[error("tag already exists")]
    TagAlreadyExists,

    #[error("migration required: {0}")]
    MigrationRequired(String),

    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TagMutationPolicy {
    CreateOnly,
    Replace,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TagMutation {
    Created,
    Unchanged,
    Replaced { previous: Digest },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobMeta {
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestMeta {
    pub size: u64,
    pub media_type: String,
}

#[derive(Clone, Debug)]
pub struct UploadMeta {
    pub uuid: String,
    pub offset: u64,
}

#[derive(Clone, Debug, Default)]
pub struct RepoTimestamps {
    pub last_tag_update: Option<SystemTime>,
    pub last_manifest_update: Option<SystemTime>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReferrerDescriptor {
    pub media_type: String,
    pub digest: String,
    pub size: u64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_type: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<HashMap<String, String>>,
}

#[async_trait]
pub trait Storage: Send + Sync + UploadSessionStorage + RepositoryBlobMembershipStorage {
    fn kind(&self) -> &'static str;

    // Best-effort listing of repositories known to the backend.
    // Returned names are in canonical OCI/Docker form, e.g. "org/repo".
    async fn list_repositories(&self) -> Result<Vec<String>, StorageError>;

    // Best-effort timestamp metadata for a repository.
    // - last_tag_update: latest modification in repo's tag pointers (often correlates with last push/tag)
    // - last_manifest_update: latest modification in repo's manifest blobs
    async fn repo_timestamps(&self, name: &str) -> Result<RepoTimestamps, StorageError>;

    async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError>;

    async fn open_blob(
        &self,
        digest: &Digest,
    ) -> Result<(BlobMeta, Pin<Box<dyn AsyncRead + Send>>), StorageError>;

    async fn resolve_tag(&self, name: &str, tag: &str) -> Result<Digest, StorageError>;

    async fn list_tags(&self, name: &str) -> Result<Vec<String>, StorageError>;

    async fn head_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<ManifestMeta, StorageError>;

    async fn get_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<(ManifestMeta, Bytes), StorageError>;

    async fn put_manifest(
        &self,
        name: &str,
        digest: &Digest,
        bytes: Bytes,
    ) -> Result<ManifestMeta, StorageError>;

    async fn set_tag(&self, name: &str, tag: &str, digest: &Digest) -> Result<(), StorageError>;

    async fn mutate_tag(
        &self,
        name: &str,
        tag: &str,
        digest: &Digest,
        policy: TagMutationPolicy,
    ) -> Result<TagMutation, StorageError>;

    async fn delete_tag(&self, name: &str, tag: &str) -> Result<(), StorageError>;

    async fn create_upload(&self) -> Result<UploadMeta, StorageError>;

    async fn upload_status(&self, uuid: &str) -> Result<UploadMeta, StorageError>;

    async fn append_upload(&self, uuid: &str, chunk: Bytes) -> Result<UploadMeta, StorageError>;

    async fn finalize_upload(&self, uuid: &str, digest: &Digest) -> Result<BlobMeta, StorageError>;

    // Best-effort cleanup for failed/abandoned uploads.
    async fn abort_upload(&self, uuid: &str) -> Result<(), StorageError>;

    // Content Management: blob deletion.
    async fn delete_blob(&self, digest: &Digest) -> Result<(), StorageError>;

    // Content Discovery: referrers API.
    async fn list_referrers(
        &self,
        name: &str,
        subject: &Digest,
    ) -> Result<Vec<ReferrerDescriptor>, StorageError>;

    async fn add_referrer(
        &self,
        name: &str,
        subject: &Digest,
        descriptor: ReferrerDescriptor,
    ) -> Result<(), StorageError>;

    async fn remove_referrer(
        &self,
        name: &str,
        subject: &Digest,
        referrer: &Digest,
    ) -> Result<(), StorageError>;

    // Content Management: manifest deletion.
    async fn delete_manifest(&self, name: &str, digest: &Digest) -> Result<(), StorageError>;
}

pub fn try_from_config(config: &Config) -> Result<Arc<dyn Storage>, StorageError> {
    match config.storage_backend {
        StorageBackend::Filesystem => {
            let fs_storage =
                fs::FsStorage::try_new(config.fs_root.clone(), config.max_upload_bytes)?;
            Ok(Arc::new(fs_storage))
        }
        StorageBackend::S3 => {
            let session_cfg = s3::S3SessionConfig {
                lease_duration_secs: config.s3_lease_duration_secs,
                lease_renewal_interval_secs: config.s3_lease_renewal_interval_secs,
                max_retry_attempts: config.s3_max_retry_attempts,
                receipt_lifetime_secs: config.upload_receipt_lifetime_secs,
                upload_expiration_secs: config.upload_gc_max_age_secs,
                legacy_multipart_cleanup_policy: config.s3_legacy_multipart_cleanup_policy,
            };
            Ok(Arc::new(
                s3::S3Storage::new(
                    config.s3_endpoint.clone(),
                    config.s3_region.clone(),
                    config.s3_bucket.clone(),
                    config.s3_prefix.clone(),
                    config.max_upload_bytes,
                )
                .with_session_config(session_cfg),
            ))
        }
    }
}

pub fn from_config(config: &Config) -> Arc<dyn Storage> {
    try_from_config(config).unwrap_or_else(|err| {
        eprintln!("storage: initialization failed: {err}");
        std::process::exit(1);
    })
}

pub(crate) fn ensure_dir(path: impl AsRef<std::path::Path>) -> Result<(), StorageError> {
    let p = path.as_ref();
    std::fs::create_dir_all(p).map_err(|err| {
        StorageError::Internal(format!(
            "failed to create storage dir {}: {err}",
            p.display()
        ))
    })
}
