use super::{BlobMeta, ManifestMeta, Storage, StorageError};
use crate::registry::digest::Digest;
use async_trait::async_trait;
use bytes::Bytes;
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
        // Keep fields "used" until the real S3 implementation lands.
        let _ = (&self.endpoint, &self.region, &self.bucket, &self.prefix);
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

    async fn resolve_tag(&self, _name: &str, _tag: &str) -> Result<Digest, StorageError> {
        Err(StorageError::Unsupported)
    }

    async fn head_manifest(&self, _name: &str, _digest: &Digest) -> Result<ManifestMeta, StorageError> {
        Err(StorageError::Unsupported)
    }

    async fn get_manifest(
        &self,
        _name: &str,
        _digest: &Digest,
    ) -> Result<(ManifestMeta, Bytes), StorageError> {
        Err(StorageError::Unsupported)
    }

    async fn put_manifest(
        &self,
        _name: &str,
        _digest: &Digest,
        _bytes: Bytes,
    ) -> Result<ManifestMeta, StorageError> {
        Err(StorageError::Unsupported)
    }

    async fn set_tag(&self, _name: &str, _tag: &str, _digest: &Digest) -> Result<(), StorageError> {
        Err(StorageError::Unsupported)
    }

    async fn create_upload(&self) -> Result<super::UploadMeta, StorageError> {
        Err(StorageError::Unsupported)
    }

    async fn upload_status(&self, _uuid: &str) -> Result<super::UploadMeta, StorageError> {
        Err(StorageError::Unsupported)
    }

    async fn append_upload(&self, _uuid: &str, _chunk: Bytes) -> Result<super::UploadMeta, StorageError> {
        Err(StorageError::Unsupported)
    }

    async fn finalize_upload(&self, _uuid: &str, _digest: &Digest) -> Result<BlobMeta, StorageError> {
        Err(StorageError::Unsupported)
    }
}
