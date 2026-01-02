use super::{ensure_dir, BlobMeta, ManifestMeta, Storage, StorageError};
use crate::registry::digest::Digest;
use async_trait::async_trait;
use std::path::PathBuf;
use bytes::Bytes;
use sha2::Digest as _;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

#[derive(Debug)]
pub struct FsStorage {
    root: PathBuf,
}

impl FsStorage {
    pub fn new(root: PathBuf) -> Self {
        ensure_dir(&root);
        Self { root }
    }

    fn blob_path(&self, digest: &Digest) -> PathBuf {
        // data/blobs/sha256/ab/<hex>
        self.root
            .join("blobs")
            .join("sha256")
            .join(digest.prefix2())
            .join(digest.hex())
    }

    fn manifest_path(&self, name: &str, digest: &Digest) -> PathBuf {
        // data/repos/<name>/manifests/<hex>
        self.root
            .join("repos")
            .join(name)
            .join("manifests")
            .join(digest.hex())
    }

    fn tag_path(&self, name: &str, tag: &str) -> PathBuf {
        // data/repos/<name>/tags/<tag>
        self.root
            .join("repos")
            .join(name)
            .join("tags")
            .join(tag)
    }

    fn uploads_dir(&self) -> PathBuf {
        self.root.join("uploads")
    }

    fn upload_path(&self, uuid: &str) -> PathBuf {
        self.uploads_dir().join(format!("{uuid}.data"))
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

    async fn resolve_tag(&self, name: &str, tag: &str) -> Result<Digest, StorageError> {
        let path = self.tag_path(name, tag);
        let content = match tokio::fs::read_to_string(&path).await {
            Ok(s) => s,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(StorageError::NotFound),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };
        let reference = content.trim();
        Digest::parse(reference).map_err(|_| StorageError::NotFound)
    }

    async fn head_manifest(&self, name: &str, digest: &Digest) -> Result<ManifestMeta, StorageError> {
        let path = self.manifest_path(name, digest);
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(StorageError::NotFound),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };
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
    ) -> Result<(ManifestMeta, bytes::Bytes), StorageError> {
        let path = self.manifest_path(name, digest);
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(StorageError::NotFound),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };
        let media_type = self.detect_manifest_media_type(&bytes).await?;
        let meta = ManifestMeta {
            size: bytes.len() as u64,
            media_type,
        };
        Ok((meta, bytes::Bytes::from(bytes)))
    }

    async fn create_upload(&self) -> Result<super::UploadMeta, StorageError> {
        let dir = self.uploads_dir();
        ensure_dir(&dir);

        let uuid = uuid::Uuid::new_v4().to_string();
        let path = self.upload_path(&uuid);

        tokio::fs::File::create(&path)
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;

        Ok(super::UploadMeta { uuid, offset: 0 })
    }

    async fn upload_status(&self, uuid: &str) -> Result<super::UploadMeta, StorageError> {
        let path = self.upload_path(uuid);
        let meta = match tokio::fs::metadata(&path).await {
            Ok(m) => m,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(StorageError::NotFound),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };
        Ok(super::UploadMeta {
            uuid: uuid.to_string(),
            offset: meta.len(),
        })
    }

    async fn append_upload(&self, uuid: &str, chunk: Bytes) -> Result<super::UploadMeta, StorageError> {
        let path = self.upload_path(uuid);
        let mut file = match tokio::fs::OpenOptions::new().append(true).open(&path).await {
            Ok(f) => f,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(StorageError::NotFound),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };
        file.write_all(&chunk)
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;
        file.flush()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;

        let meta = file
            .metadata()
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;
        Ok(super::UploadMeta {
            uuid: uuid.to_string(),
            offset: meta.len(),
        })
    }

    async fn finalize_upload(&self, uuid: &str, digest: &Digest) -> Result<BlobMeta, StorageError> {
        let upload_path = self.upload_path(uuid);
        let mut file = match tokio::fs::File::open(&upload_path).await {
            Ok(f) => f,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(StorageError::NotFound),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };

        let mut hasher = sha2::Sha256::new();
        let mut buf = vec![0u8; 1024 * 64];
        loop {
            let n = file
                .read(&mut buf)
                .await
                .map_err(|err| StorageError::Internal(err.to_string()))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let computed = hasher.finalize();
        let computed_hex = hex::encode(computed);

        if computed_hex != digest.hex() {
            return Err(StorageError::DigestMismatch);
        }

        // Move into blob store.
        let dest_dir = self
            .root
            .join("blobs")
            .join("sha256")
            .join(digest.prefix2());
        ensure_dir(&dest_dir);

        let dest_path = dest_dir.join(digest.hex());
        tokio::fs::rename(&upload_path, &dest_path)
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;

        let meta = tokio::fs::metadata(&dest_path)
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;
        Ok(BlobMeta { size: meta.len() })
    }
}
