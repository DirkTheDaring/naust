use crate::{config::{Config, StorageBackend}, registry::digest::Digest};
use async_trait::async_trait;
use bytes::Bytes;
use std::{path::PathBuf, pin::Pin, sync::Arc};
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

#[async_trait]
pub trait Storage: Send + Sync {
    fn kind(&self) -> &'static str;

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
