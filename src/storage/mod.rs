use crate::{
    config::{Config, StorageBackend},
    registry::digest::Digest,
};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::{path::PathBuf, pin::Pin, sync::Arc};
use std::time::SystemTime;
use thiserror::Error;
use tokio::io::AsyncRead;

pub mod fs;
pub mod s3;

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

    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Clone, Debug)]
pub struct BlobMeta {
    pub size: u64,
}

#[derive(Clone, Debug)]
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
pub trait Storage: Send + Sync {
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

    async fn head_manifest(&self, name: &str, digest: &Digest) -> Result<ManifestMeta, StorageError>;

    async fn get_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<(ManifestMeta, Bytes), StorageError>;

    async fn put_manifest(&self, name: &str, digest: &Digest, bytes: Bytes) -> Result<ManifestMeta, StorageError>;

    async fn set_tag(&self, name: &str, tag: &str, digest: &Digest) -> Result<(), StorageError>;

    async fn create_upload(&self) -> Result<UploadMeta, StorageError>;

    async fn upload_status(&self, uuid: &str) -> Result<UploadMeta, StorageError>;

    async fn append_upload(&self, uuid: &str, chunk: Bytes) -> Result<UploadMeta, StorageError>;

    async fn finalize_upload(&self, uuid: &str, digest: &Digest) -> Result<BlobMeta, StorageError>;

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

    // Content Management: manifest deletion.
    async fn delete_manifest(&self, name: &str, digest: &Digest) -> Result<(), StorageError>;
}

pub fn from_config(config: &Config) -> Arc<dyn Storage> {
    match config.storage_backend {
        StorageBackend::Filesystem => Arc::new(fs::FsStorage::new(
            config.fs_root.clone(),
            config.max_upload_bytes,
        )),
        StorageBackend::S3 => Arc::new(s3::S3Storage::new(
            config.s3_endpoint.clone(),
            config.s3_region.clone(),
            config.s3_bucket.clone(),
            config.s3_prefix.clone(),
            config.max_upload_bytes,
        )),
    }
}

pub(crate) fn ensure_dir(path: &PathBuf) {
    if let Err(err) = std::fs::create_dir_all(path) {
        panic!("failed to create storage dir {}: {err}", path.display());
    }
}
