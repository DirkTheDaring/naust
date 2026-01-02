use crate::{config::{Config, StorageBackend}, registry::digest::Digest};
use async_trait::async_trait;
use std::{path::PathBuf, pin::Pin, sync::Arc};
use thiserror::Error;
use tokio::io::AsyncRead;

pub mod fs;
pub mod s3;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("not found")]
    NotFound,

    #[error("unsupported")]
    Unsupported,

    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Clone, Debug)]
pub struct BlobMeta {
    pub size: u64,
}

#[async_trait]
pub trait Storage: Send + Sync {
    fn kind(&self) -> &'static str;

    async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError>;

    async fn open_blob(
        &self,
        digest: &Digest,
    ) -> Result<(BlobMeta, Pin<Box<dyn AsyncRead + Send>>), StorageError>;
}

pub fn from_config(config: &Config) -> Arc<dyn Storage> {
    match config.storage_backend {
        StorageBackend::Filesystem => Arc::new(fs::FsStorage::new(config.fs_root.clone())),
        StorageBackend::S3 => Arc::new(s3::S3Storage::new(
            config.s3_endpoint.clone(),
            config.s3_region.clone(),
            config.s3_bucket.clone(),
            config.s3_prefix.clone(),
        )),
    }
}

pub(crate) fn ensure_dir(path: &PathBuf) {
    if let Err(err) = std::fs::create_dir_all(path) {
        panic!("failed to create storage dir {}: {err}", path.display());
    }
}
