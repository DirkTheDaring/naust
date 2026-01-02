use super::{ensure_dir, BlobMeta, Storage, StorageError};
use crate::registry::digest::Digest;
use async_trait::async_trait;
use std::path::PathBuf;
use tokio::io::AsyncRead;

#[derive(Debug)]
pub struct FsStorage {
    root: PathBuf,
}

impl FsStorage {
    pub fn new(root: PathBuf) -> Self {
        ensure_dir(&root);
        Self { root }
    }

    pub fn root(&self) -> &PathBuf {
        &self.root
    }

    fn blob_path(&self, digest: &Digest) -> PathBuf {
        // data/blobs/sha256/ab/<hex>
        self.root
            .join("blobs")
            .join("sha256")
            .join(digest.prefix2())
            .join(digest.hex())
    }
}

#[async_trait]
impl Storage for FsStorage {
    fn kind(&self) -> &'static str {
        "fs"
    }

    async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError> {
        let path = self.blob_path(digest);
        match tokio::fs::metadata(&path).await {
            Ok(meta) => Ok(BlobMeta {
                size: meta.len(),
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(StorageError::NotFound),
            Err(err) => Err(StorageError::Internal(err.to_string())),
        }
    }

    async fn open_blob(
        &self,
        digest: &Digest,
    ) -> Result<(BlobMeta, std::pin::Pin<Box<dyn AsyncRead + Send>>), StorageError> {
        let path = self.blob_path(digest);
        let file = match tokio::fs::File::open(&path).await {
            Ok(f) => f,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(StorageError::NotFound),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };
        let meta = match file.metadata().await {
            Ok(m) => m,
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };
        Ok((BlobMeta { size: meta.len() }, Box::pin(file)))
    }
}
