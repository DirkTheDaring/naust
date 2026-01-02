use super::{ensure_dir, BlobMeta, ManifestMeta, ReferrerDescriptor, RepoTimestamps, Storage, StorageError};
use crate::registry::digest::Digest;
use async_trait::async_trait;
use std::path::PathBuf;
use bytes::Bytes;
use sha2::Digest as _;
use std::time::SystemTime;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

#[derive(Debug)]
pub struct FsStorage {
    root: PathBuf,
    max_upload_bytes: u64,
}

impl FsStorage {
    pub fn new(root: PathBuf, max_upload_bytes: u64) -> Self {
        ensure_dir(&root);
        Self {
            root,
            max_upload_bytes,
        }
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

    fn referrers_path(&self, name: &str, subject: &Digest) -> PathBuf {
        // data/repos/<name>/referrers/<hex>.json
        self.root
            .join("repos")
            .join(name)
            .join("referrers")
            .join(format!("{}.json", subject.hex()))
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

    async fn list_tag_files(&self, name: &str) -> Result<Vec<PathBuf>, StorageError> {
        let tags_dir = self.root.join("repos").join(name).join("tags");
        let mut dir = match tokio::fs::read_dir(&tags_dir).await {
            Ok(d) => d,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };

        let mut files = Vec::new();
        loop {
            match dir.next_entry().await {
                Ok(Some(entry)) => files.push(entry.path()),
                Ok(None) => break,
                Err(err) => return Err(StorageError::Internal(err.to_string())),
            }
        }
        Ok(files)
    }

    async fn list_repo_names(&self) -> Result<Vec<String>, StorageError> {
        let repos_root = self.root.join("repos");
        let mut repos = Vec::new();

        let mut stack: Vec<(PathBuf, String)> = vec![(repos_root.clone(), String::new())];
        while let Some((dir_path, rel)) = stack.pop() {
            let mut dir = match tokio::fs::read_dir(&dir_path).await {
                Ok(d) => d,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(StorageError::Internal(err.to_string())),
            };

            while let Ok(Some(entry)) = dir.next_entry().await {
                let file_type = match entry.file_type().await {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                if !file_type.is_dir() {
                    continue;
                }

                let name = match entry.file_name().to_str() {
                    Some(s) => s.to_string(),
                    None => continue,
                };

                // Do not descend into internal leaf dirs.
                if name == "tags" || name == "manifests" || name == "referrers" {
                    continue;
                }

                let child_path = entry.path();
                let child_rel = if rel.is_empty() {
                    name
                } else {
                    format!("{rel}/{name}")
                };

                // Consider this a repo if it has tags/ or manifests/ directories.
                let tags_dir = child_path.join("tags");
                let manifests_dir = child_path.join("manifests");
                let has_tags = tokio::fs::metadata(&tags_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                let has_manifests = tokio::fs::metadata(&manifests_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                if has_tags || has_manifests {
                    repos.push(child_rel.clone());
                }

                stack.push((child_path, child_rel));
            }
        }

        repos.sort();
        repos.dedup();
        Ok(repos)
    }

    async fn max_mtime_in_dir(&self, dir: &PathBuf) -> Result<Option<SystemTime>, StorageError> {
        let mut rd = match tokio::fs::read_dir(dir).await {
            Ok(d) => d,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };

        let mut max_time: Option<SystemTime> = None;
        while let Ok(Some(entry)) = rd.next_entry().await {
            let meta = match entry.metadata().await {
                Ok(m) => m,
                Err(_) => continue,
            };
            if !meta.is_file() {
                continue;
            }
            let modified = match meta.modified() {
                Ok(t) => t,
                Err(_) => continue,
            };
            max_time = Some(match max_time {
                Some(cur) if cur >= modified => cur,
                _ => modified,
            });
        }
        Ok(max_time)
    }
}

#[async_trait]
impl Storage for FsStorage {
    fn kind(&self) -> &'static str {
        "fs"
    }

    async fn list_repositories(&self) -> Result<Vec<String>, StorageError> {
        self.list_repo_names().await
    }

    async fn repo_timestamps(&self, name: &str) -> Result<RepoTimestamps, StorageError> {
        let repo_dir = self.root.join("repos").join(name);
        match tokio::fs::metadata(&repo_dir).await {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(StorageError::NotFound),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        }

        let tags_dir = repo_dir.join("tags");
        let manifests_dir = repo_dir.join("manifests");

        let last_tag_update = self.max_mtime_in_dir(&tags_dir).await?;
        let last_manifest_update = self.max_mtime_in_dir(&manifests_dir).await?;

        Ok(RepoTimestamps {
            last_tag_update,
            last_manifest_update,
        })
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

    async fn list_tags(&self, name: &str) -> Result<Vec<String>, StorageError> {
        let repo_dir = self.root.join("repos").join(name);
        match tokio::fs::metadata(&repo_dir).await {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(StorageError::NotFound),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        }

        let tags_dir = repo_dir.join("tags");
        let mut dir = match tokio::fs::read_dir(&tags_dir).await {
            Ok(d) => d,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };

        let mut tags = Vec::new();
        loop {
            match dir.next_entry().await {
                Ok(Some(entry)) => {
                    if let Some(file_name) = entry.file_name().to_str() {
                        tags.push(file_name.to_string());
                    }
                }
                Ok(None) => break,
                Err(err) => return Err(StorageError::Internal(err.to_string())),
            }
        }

        tags.sort();
        Ok(tags)
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

    async fn put_manifest(
        &self,
        name: &str,
        digest: &Digest,
        bytes: Bytes,
    ) -> Result<ManifestMeta, StorageError> {
        let dir = self.root.join("repos").join(name).join("manifests");
        ensure_dir(&dir);

        let media_type = self.detect_manifest_media_type(&bytes).await?;
        let path = dir.join(digest.hex());
        tokio::fs::write(&path, &bytes)
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;

        Ok(ManifestMeta {
            size: bytes.len() as u64,
            media_type,
        })
    }

    async fn set_tag(&self, name: &str, tag: &str, digest: &Digest) -> Result<(), StorageError> {
        let dir = self.root.join("repos").join(name).join("tags");
        ensure_dir(&dir);
        let path = dir.join(tag);
        tokio::fs::write(&path, format!("{}\n", digest.as_str()))
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;
        Ok(())
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

        let current_len = match tokio::fs::metadata(&path).await {
            Ok(m) => m.len(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(StorageError::NotFound),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        };

        let next_len = current_len.saturating_add(chunk.len() as u64);
        if next_len > self.max_upload_bytes {
            return Err(StorageError::TooLarge);
        }

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

    async fn delete_blob(&self, digest: &Digest) -> Result<(), StorageError> {
        let path = self.blob_path(digest);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(StorageError::NotFound),
            Err(err) => Err(StorageError::Internal(err.to_string())),
        }
    }

    async fn list_referrers(
        &self,
        name: &str,
        subject: &Digest,
    ) -> Result<Vec<ReferrerDescriptor>, StorageError> {
        let path = self.referrers_path(name, subject);
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
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
        let dir = self
            .root
            .join("repos")
            .join(name)
            .join("referrers");
        ensure_dir(&dir);

        let path = self.referrers_path(name, subject);
        let mut existing = self.list_referrers(name, subject).await?;
        if !existing.iter().any(|d| d.digest == descriptor.digest) {
            existing.push(descriptor);
        }

        let bytes = serde_json::to_vec(&existing)
            .map_err(|err| StorageError::Internal(err.to_string()))?;
        tokio::fs::write(&path, bytes)
            .await
            .map_err(|err| StorageError::Internal(err.to_string()))?;
        Ok(())
    }

    async fn delete_manifest(&self, name: &str, digest: &Digest) -> Result<(), StorageError> {
        let manifest_path = self.manifest_path(name, digest);
        match tokio::fs::remove_file(&manifest_path).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(StorageError::NotFound),
            Err(err) => return Err(StorageError::Internal(err.to_string())),
        }

        // Remove any tags pointing to this digest.
        let digest_str = digest.as_str();
        for path in self.list_tag_files(name).await? {
            let content = match tokio::fs::read_to_string(&path).await {
                Ok(s) => s,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(StorageError::Internal(err.to_string())),
            };
            if content.trim() == digest_str {
                let _ = tokio::fs::remove_file(&path).await;
            }
        }
        Ok(())
    }
}
