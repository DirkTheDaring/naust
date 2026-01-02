use super::{BlobMeta, Storage, StorageError};
use crate::registry::digest::Digest;
use async_trait::async_trait;
use tokio::io::AsyncRead;

#[derive(Debug)]
pub struct S3Storage {
    endpoint: Option<String>,
    region: Option<String>,
    bucket: Option<String>,
    prefix: String,
}

impl S3Storage {
    pub fn new(
        endpoint: Option<String>,
        region: Option<String>,
        bucket: Option<String>,
        prefix: String,
    ) -> Self {
        Self {
            endpoint,
            region,
            bucket,
            prefix,
        }
    }
}

#[async_trait]
impl Storage for S3Storage {
    fn kind(&self) -> &'static str {
        "s3"
    }

    async fn head_blob(&self, _digest: &Digest) -> Result<BlobMeta, StorageError> {
        Err(StorageError::Unsupported)
    }

    async fn open_blob(
        &self,
        _digest: &Digest,
    ) -> Result<(BlobMeta, std::pin::Pin<Box<dyn AsyncRead + Send>>), StorageError> {
        Err(StorageError::Unsupported)
    }
}
