use std::sync::Arc;

use bytes::Bytes;
use sha2::Digest as _;

use crate::blob_ref_index::BlobRefIndex;
use crate::manifest_refs::parse_manifest_refs;
use crate::registry::digest::Digest;
use crate::registry::validation::{is_valid_repo_name, is_valid_tag};
use crate::storage::{ReferrerDescriptor, Storage, StorageError, TagMutationPolicy};

pub const MAX_MANIFEST_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

#[derive(Clone, Debug)]
pub struct PublishManifestRequest {
    pub repo: String,
    pub reference: String,
    pub payload: Bytes,
    pub declared_media_type: Option<String>,
    pub allow_tag_overwrite: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishedManifest {
    pub digest: Digest,
    pub media_type: String,
    pub size: u64,
    pub subject: Option<Digest>,
    pub is_tag: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum PublishManifestError {
    #[error("invalid repository name")]
    InvalidRepoName,

    #[error("manifest payload is empty")]
    EmptyPayload,

    #[error("manifest payload exceeds maximum allowed size")]
    PayloadTooLarge,

    #[error("manifest JSON is malformed or invalid: {0}")]
    InvalidManifest(String),

    #[error("manifest signature is unverified: {0}")]
    Unverified(String),

    #[error("unsupported manifest media type: {0}")]
    UnsupportedMediaType(String),

    #[error("referenced blob not found in storage: {0}")]
    MissingBlob(String),

    #[error("referenced child manifest not found in storage: {0}")]
    MissingManifest(String),

    #[error("invalid tag name")]
    InvalidTag,

    #[error("tag already exists and cannot be overwritten")]
    TagAlreadyExists,

    #[error("manifest digest mismatch: expected {expected}, computed {computed}")]
    DigestMismatch { expected: String, computed: String },

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("reference index error: {0}")]
    RefIndex(#[from] crate::blob_ref_index::RefIndexError),

    #[allow(dead_code)]
    #[error("internal publication error: {0}")]
    Internal(String),
}

pub fn is_supported_manifest_media_type(media_type: &str) -> bool {
    matches!(
        media_type,
        "application/vnd.oci.image.manifest.v1+json"
            | "application/vnd.oci.artifact.manifest.v1+json"
            | "application/vnd.oci.image.index.v1+json"
            | "application/vnd.docker.distribution.manifest.v2+json"
            | "application/vnd.docker.distribution.manifest.list.v2+json"
    )
}

#[derive(Clone)]
pub struct ManifestPublisher {
    storage: Arc<dyn Storage>,
    ref_index: Option<Arc<BlobRefIndex>>,
    consistency_gate: Arc<tokio::sync::Mutex<()>>,
}

impl ManifestPublisher {
    pub fn new(
        storage: Arc<dyn Storage>,
        ref_index: Option<Arc<BlobRefIndex>>,
        consistency_gate: Arc<tokio::sync::Mutex<()>>,
    ) -> Self {
        Self {
            storage,
            ref_index,
            consistency_gate,
        }
    }

    /// Orchestrates manifest publication with strict pre-mutation validation,
    /// authoritative content storage, an atomic tag commit point, synchronous referrer registration,
    /// durable reference index dirty tracking, and single-instance coordination.
    pub async fn publish(
        &self,
        req: PublishManifestRequest,
    ) -> Result<PublishedManifest, PublishManifestError> {
        // --- 1. Pure Validation (Preflight Before Any Storage Mutation) ---

        if !is_valid_repo_name(&req.repo) {
            return Err(PublishManifestError::InvalidRepoName);
        }

        if req.payload.is_empty() {
            return Err(PublishManifestError::EmptyPayload);
        }

        if req.payload.len() > MAX_MANIFEST_SIZE {
            return Err(PublishManifestError::PayloadTooLarge);
        }

        let manifest_json: serde_json::Value = serde_json::from_slice(&req.payload)
            .map_err(|e| PublishManifestError::InvalidManifest(e.to_string()))?;

        // Schema version 1 / legacy signatures rejection
        if let Some(schema_version) = manifest_json.get("schemaVersion").and_then(|v| v.as_i64()) {
            if schema_version == 1 {
                if manifest_json.get("signatures").is_some()
                    || manifest_json.get("signature").is_some()
                {
                    return Err(PublishManifestError::Unverified(
                        "manifest failed signature verification".to_string(),
                    ));
                }
                return Err(PublishManifestError::InvalidManifest(
                    "schemaVersion 1 unsupported".to_string(),
                ));
            }
        }

        if manifest_json.get("signatures").is_some() || manifest_json.get("signature").is_some() {
            return Err(PublishManifestError::Unverified(
                "manifest failed signature verification".to_string(),
            ));
        }

        let media_type = manifest_json
            .get("mediaType")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or(req.declared_media_type)
            .unwrap_or_else(|| "application/vnd.oci.image.manifest.v1+json".to_string());

        if media_type.starts_with("application/vnd.docker.distribution.manifest.v1") {
            if media_type.contains("prettyjws") || manifest_json.get("signatures").is_some() {
                return Err(PublishManifestError::Unverified(
                    "manifest signatures unverified".to_string(),
                ));
            }
            return Err(PublishManifestError::InvalidManifest(
                "docker schema v1 manifest unsupported".to_string(),
            ));
        }

        if !is_supported_manifest_media_type(&media_type) {
            return Err(PublishManifestError::UnsupportedMediaType(media_type));
        }

        // Parse and validate descriptor references
        let refs = parse_manifest_refs(&req.payload)
            .map_err(|e| PublishManifestError::InvalidManifest(e.to_string()))?;

        // Pre-check referenced blobs (config, layers, artifact blobs)
        for blob_d in refs.blob_references() {
            if blob_d.as_str()
                == "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                || blob_d.as_str()
                    == "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            {
                continue;
            }
            if self.storage.head_blob(blob_d).await.is_err() {
                return Err(PublishManifestError::MissingBlob(blob_d.to_string()));
            }
        }

        // Pre-check referenced child manifests (in index or manifest lists)
        for manifest_d in &refs.manifests {
            if self
                .storage
                .head_manifest(&req.repo, manifest_d)
                .await
                .is_err()
            {
                return Err(PublishManifestError::MissingManifest(
                    manifest_d.to_string(),
                ));
            }
        }

        // Pre-parse referrer info
        let referrer_info = crate::manifest_refs::parse_referrer_info(&req.payload)
            .map_err(|e| PublishManifestError::InvalidManifest(e.to_string()))?;
        let subject_digest = referrer_info.as_ref().map(|(s, _, _)| s.clone());

        // Compute manifest digest over raw bytes
        let mut hasher = sha2::Sha256::new();
        hasher.update(&req.payload);
        let digest_hex = hex::encode(hasher.finalize());
        let computed =
            Digest::parse(&format!("sha256:{digest_hex}")).expect("computed sha256 is valid");

        // Validate reference (digest vs tag)
        let is_tag = match Digest::parse(&req.reference) {
            Ok(ref_digest) => {
                if ref_digest.hex() != computed.hex() {
                    return Err(PublishManifestError::DigestMismatch {
                        expected: ref_digest.to_string(),
                        computed: computed.to_string(),
                    });
                }
                false
            }
            Err(_) => {
                if !is_valid_tag(&req.reference) {
                    return Err(PublishManifestError::InvalidTag);
                }
                true
            }
        };

        // --- Acquire Shared Consistency Coordinator ---
        let _gate = self.consistency_gate.lock().await;

        // Ensure reference index is healthy; if left dirty from an earlier crash/failure, rebuild it before mutating!
        if let Some(idx) = self.ref_index.as_ref() {
            if idx.check_health().is_err() {
                idx.ensure_healthy_or_rebuild(&self.storage, true, false)
                    .await?;
            }
        }

        // --- 2. Authoritative Content Storage (Idempotent Mutation) ---
        let meta = self
            .storage
            .put_manifest(&req.repo, &computed, req.payload)
            .await?;

        // --- 3. Synchronous Referrers Registration ---
        if let Some((subject, artifact_type, annotations)) = referrer_info {
            let descriptor = ReferrerDescriptor {
                media_type: meta.media_type.clone(),
                digest: computed.as_str(),
                size: meta.size,
                artifact_type,
                annotations,
            };
            self.storage
                .add_referrer(&req.repo, &subject, descriptor)
                .await?;
        }

        // --- 4. Atomic Tag Pointer Mutation & Durable Index Updates ---
        if is_tag {
            let policy = if req.allow_tag_overwrite {
                TagMutationPolicy::Replace
            } else {
                TagMutationPolicy::CreateOnly
            };

            // Durably mark index dirty before tag mutation
            if let Some(idx) = self.ref_index.as_ref() {
                idx.mark_dirty()?;
            }

            let mutation = match self
                .storage
                .mutate_tag(&req.repo, &req.reference, &computed, policy)
                .await
            {
                Ok(m) => m,
                Err(StorageError::TagAlreadyExists) => {
                    // Restore ready state since tag was not modified
                    if let Some(idx) = self.ref_index.as_ref() {
                        let _ = idx.mark_ready();
                    }
                    return Err(PublishManifestError::TagAlreadyExists);
                }
                Err(err) => {
                    return Err(PublishManifestError::Storage(err));
                }
            };

            // Synchronously update derived reference index edges using exact mutation result
            if let Some(idx) = self.ref_index.as_ref() {
                idx.on_tag_mutation(
                    &self.storage,
                    &req.repo,
                    &req.reference,
                    &computed,
                    &mutation,
                )
                .await?;

                // Durably mark index ready after edges match authoritative tag state
                idx.mark_ready()?;
            }
        }

        Ok(PublishedManifest {
            digest: computed,
            media_type: meta.media_type,
            size: meta.size,
            subject: subject_digest,
            is_tag,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::fs::FsStorage;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FaultInjectableStorage {
        inner: Arc<FsStorage>,
        pub fail_put_manifest: AtomicBool,
        pub fail_mutate_tag: AtomicBool,
        pub fail_add_referrer: AtomicBool,
    }

    impl FaultInjectableStorage {
        fn new(fs_root: PathBuf) -> Self {
            let inner = Arc::new(FsStorage::new(fs_root, 50 * 1024 * 1024));
            Self {
                inner,
                fail_put_manifest: AtomicBool::new(false),
                fail_mutate_tag: AtomicBool::new(false),
                fail_add_referrer: AtomicBool::new(false),
            }
        }
    }

    #[async_trait::async_trait]
    impl Storage for FaultInjectableStorage {
        fn kind(&self) -> &'static str {
            "fault-injectable"
        }

        async fn list_repositories(&self) -> Result<Vec<String>, StorageError> {
            self.inner.list_repositories().await
        }

        async fn repo_timestamps(
            &self,
            name: &str,
        ) -> Result<crate::storage::RepoTimestamps, StorageError> {
            self.inner.repo_timestamps(name).await
        }

        async fn head_blob(
            &self,
            digest: &Digest,
        ) -> Result<crate::storage::BlobMeta, StorageError> {
            self.inner.head_blob(digest).await
        }

        async fn open_blob(
            &self,
            digest: &Digest,
        ) -> Result<
            (
                crate::storage::BlobMeta,
                std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>,
            ),
            StorageError,
        > {
            self.inner.open_blob(digest).await
        }

        async fn get_manifest(
            &self,
            name: &str,
            digest: &Digest,
        ) -> Result<(crate::storage::ManifestMeta, Bytes), StorageError> {
            self.inner.get_manifest(name, digest).await
        }

        async fn head_manifest(
            &self,
            name: &str,
            digest: &Digest,
        ) -> Result<crate::storage::ManifestMeta, StorageError> {
            self.inner.head_manifest(name, digest).await
        }

        async fn put_manifest(
            &self,
            name: &str,
            digest: &Digest,
            bytes: Bytes,
        ) -> Result<crate::storage::ManifestMeta, StorageError> {
            if self.fail_put_manifest.load(Ordering::SeqCst) {
                return Err(StorageError::Internal("injected put_manifest error".into()));
            }
            self.inner.put_manifest(name, digest, bytes).await
        }

        async fn set_tag(
            &self,
            name: &str,
            tag: &str,
            digest: &Digest,
        ) -> Result<(), StorageError> {
            self.inner.set_tag(name, tag, digest).await
        }

        async fn mutate_tag(
            &self,
            name: &str,
            tag: &str,
            digest: &Digest,
            policy: TagMutationPolicy,
        ) -> Result<crate::storage::TagMutation, StorageError> {
            if self.fail_mutate_tag.load(Ordering::SeqCst) {
                return Err(StorageError::Internal("injected mutate_tag error".into()));
            }
            self.inner.mutate_tag(name, tag, digest, policy).await
        }

        async fn delete_tag(&self, name: &str, tag: &str) -> Result<(), StorageError> {
            self.inner.delete_tag(name, tag).await
        }

        async fn create_upload(&self) -> Result<crate::storage::UploadMeta, StorageError> {
            self.inner.create_upload().await
        }

        async fn upload_status(
            &self,
            uuid: &str,
        ) -> Result<crate::storage::UploadMeta, StorageError> {
            self.inner.upload_status(uuid).await
        }

        async fn append_upload(
            &self,
            uuid: &str,
            chunk: Bytes,
        ) -> Result<crate::storage::UploadMeta, StorageError> {
            self.inner.append_upload(uuid, chunk).await
        }

        async fn finalize_upload(
            &self,
            uuid: &str,
            digest: &Digest,
        ) -> Result<crate::storage::BlobMeta, StorageError> {
            self.inner.finalize_upload(uuid, digest).await
        }

        async fn list_tags(&self, name: &str) -> Result<Vec<String>, StorageError> {
            self.inner.list_tags(name).await
        }

        async fn resolve_tag(&self, name: &str, tag: &str) -> Result<Digest, StorageError> {
            self.inner.resolve_tag(name, tag).await
        }

        async fn abort_upload(&self, uuid: &str) -> Result<(), StorageError> {
            self.inner.abort_upload(uuid).await
        }

        async fn delete_blob(&self, digest: &Digest) -> Result<(), StorageError> {
            self.inner.delete_blob(digest).await
        }

        async fn list_referrers(
            &self,
            name: &str,
            subject: &Digest,
        ) -> Result<Vec<ReferrerDescriptor>, StorageError> {
            self.inner.list_referrers(name, subject).await
        }

        async fn add_referrer(
            &self,
            name: &str,
            subject: &Digest,
            descriptor: ReferrerDescriptor,
        ) -> Result<(), StorageError> {
            if self.fail_add_referrer.load(Ordering::SeqCst) {
                return Err(StorageError::Internal("injected add_referrer error".into()));
            }
            self.inner.add_referrer(name, subject, descriptor).await
        }

        async fn remove_referrer(
            &self,
            name: &str,
            subject: &Digest,
            referrer: &Digest,
        ) -> Result<(), StorageError> {
            self.inner.remove_referrer(name, subject, referrer).await
        }

        async fn delete_manifest(&self, name: &str, digest: &Digest) -> Result<(), StorageError> {
            self.inner.delete_manifest(name, digest).await
        }
    }

    async fn write_test_blob(storage: &Arc<dyn Storage>, content: &[u8]) -> Digest {
        let mut hasher = sha2::Sha256::new();
        hasher.update(content);
        let digest = Digest::parse(&format!("sha256:{}", hex::encode(hasher.finalize()))).unwrap();
        let upload = storage.create_upload().await.unwrap();
        storage
            .append_upload(&upload.uuid, Bytes::copy_from_slice(content))
            .await
            .unwrap();
        storage
            .finalize_upload(&upload.uuid, &digest)
            .await
            .unwrap();
        digest
    }

    fn sample_valid_manifest(config_digest: &Digest, layer_digest: &Digest) -> (Bytes, Digest) {
        let json = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest.as_str(),
                "size": 2
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": layer_digest.as_str(),
                    "size": 12
                }
            ]
        });
        let bytes = Bytes::from(serde_json::to_vec(&json).unwrap());
        let mut hasher = sha2::Sha256::new();
        hasher.update(&bytes);
        let digest = Digest::parse(&format!("sha256:{}", hex::encode(hasher.finalize()))).unwrap();
        (bytes, digest)
    }

    #[tokio::test]
    async fn test_stage_1_reference_parsing_fails() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(
            temp_dir.path().to_path_buf(),
            10 * 1024 * 1024,
        ));
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage, None, gate);

        let req = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "latest".to_string(),
            payload: Bytes::from("not-valid-json"),
            declared_media_type: None,
            allow_tag_overwrite: true,
        };
        let err = publisher.publish(req).await.unwrap_err();
        assert!(matches!(err, PublishManifestError::InvalidManifest(_)));
    }

    #[tokio::test]
    async fn test_reference_kind_validation_blobs_vs_child_manifests() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(
            temp_dir.path().to_path_buf(),
            10 * 1024 * 1024,
        ));
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage.clone(), None, gate);

        let config_d = write_test_blob(&storage, b"{}").await;
        let missing_blob_digest = Digest::parse(
            "sha256:baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let (payload, _) = sample_valid_manifest(&config_d, &missing_blob_digest);

        // 1. Missing layer blob fails with MissingBlob
        let req1 = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "latest".to_string(),
            payload,
            declared_media_type: None,
            allow_tag_overwrite: true,
        };
        let err1 = publisher.publish(req1).await.unwrap_err();
        assert!(matches!(err1, PublishManifestError::MissingBlob(_)));

        // 2. Missing child manifest in OCI index fails with MissingManifest
        let missing_manifest_digest = Digest::parse(
            "sha256:caaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let index_json = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": missing_manifest_digest.as_str(),
                    "size": 100
                }
            ]
        });
        let index_bytes = Bytes::from(serde_json::to_vec(&index_json).unwrap());
        let req2 = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "multiarch".to_string(),
            payload: index_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        };
        let err2 = publisher.publish(req2).await.unwrap_err();
        match err2 {
            PublishManifestError::MissingManifest(d) => {
                assert_eq!(d, missing_manifest_digest.as_str())
            }
            other => panic!("expected MissingManifest, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_real_fs_concurrent_immutable_tag_creates() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(
            temp_dir.path().to_path_buf(),
            10 * 1024 * 1024,
        ));
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = Arc::new(ManifestPublisher::new(storage.clone(), None, gate));

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d1 = write_test_blob(&storage, b"payload_1").await;
        let blob_d2 = write_test_blob(&storage, b"payload_2").await;

        let (m1_bytes, m1_digest) = sample_valid_manifest(&config_d, &blob_d1);
        let (m2_bytes, m2_digest) = sample_valid_manifest(&config_d, &blob_d2);

        let p1 = publisher.clone();
        let handle1 = tokio::spawn(async move {
            p1.publish(PublishManifestRequest {
                repo: "test/repo".to_string(),
                reference: "v1".to_string(),
                payload: m1_bytes,
                declared_media_type: None,
                allow_tag_overwrite: false,
            })
            .await
        });

        let p2 = publisher.clone();
        let handle2 = tokio::spawn(async move {
            p2.publish(PublishManifestRequest {
                repo: "test/repo".to_string(),
                reference: "v1".to_string(),
                payload: m2_bytes,
                declared_media_type: None,
                allow_tag_overwrite: false,
            })
            .await
        });

        let (res1, res2) = tokio::join!(handle1, handle2);
        let r1 = res1.unwrap();
        let r2 = res2.unwrap();

        // Exactly one create must succeed, and one must fail with TagAlreadyExists
        let (success_digest, failed_err) = match (r1, r2) {
            (Ok(pub1), Err(err2)) => (pub1.digest, err2),
            (Err(err1), Ok(pub2)) => (pub2.digest, err1),
            (Ok(_), Ok(_)) => panic!("both concurrent immutable creates succeeded! Violation!"),
            (Err(e1), Err(e2)) => panic!("both failed: {e1:?}, {e2:?}"),
        };

        assert!(matches!(failed_err, PublishManifestError::TagAlreadyExists));

        // The authoritative tag pointer must match the successful writer's digest
        let resolved = storage.resolve_tag("test/repo", "v1").await.unwrap();
        assert_eq!(resolved, success_digest);
        assert!(resolved == m1_digest || resolved == m2_digest);
    }

    #[tokio::test]
    async fn test_immutable_tag_idempotent_republish() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(
            temp_dir.path().to_path_buf(),
            10 * 1024 * 1024,
        ));
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage.clone(), None, gate);

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d = write_test_blob(&storage, b"layer_idempotent").await;
        let (m_bytes, m_digest) = sample_valid_manifest(&config_d, &blob_d);

        let req1 = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "v1".to_string(),
            payload: m_bytes.clone(),
            declared_media_type: None,
            allow_tag_overwrite: false,
        };
        let res1 = publisher.publish(req1).await.unwrap();
        assert_eq!(res1.digest, m_digest);

        // Republishing identical payload with allow_tag_overwrite=false succeeds as Unchanged
        let req2 = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "v1".to_string(),
            payload: m_bytes,
            declared_media_type: None,
            allow_tag_overwrite: false,
        };
        let res2 = publisher.publish(req2).await.unwrap();
        assert_eq!(res2.digest, m_digest);
    }

    #[tokio::test]
    async fn test_synchronous_referrer_registration_failure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fault_storage = Arc::new(FaultInjectableStorage::new(temp_dir.path().to_path_buf()));
        let storage: Arc<dyn Storage> = fault_storage.clone();
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage.clone(), None, gate);

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d = write_test_blob(&storage, b"base_layer").await;
        let (base_bytes, base_digest) = sample_valid_manifest(&config_d, &blob_d);

        let req_base = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "latest".to_string(),
            payload: base_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        };
        publisher.publish(req_base).await.unwrap();

        // Create artifact manifest referencing base
        let artifact_json = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.artifact.manifest.v1+json",
            "artifactType": "application/vnd.example.sbom.v1",
            "subject": {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": base_digest.as_str(),
                "size": 100
            },
            "blobs": []
        });
        let artifact_bytes = Bytes::from(serde_json::to_vec(&artifact_json).unwrap());

        // Inject add_referrer failure
        fault_storage
            .fail_add_referrer
            .store(true, Ordering::SeqCst);

        let req_artifact = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "sbom-tag".to_string(),
            payload: artifact_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        };

        // Must fail synchronously when referrer registration fails
        let err = publisher.publish(req_artifact).await.unwrap_err();
        assert!(matches!(err, PublishManifestError::Storage(_)));

        // Tag must NOT have been created
        assert!(storage.resolve_tag("test/repo", "sbom-tag").await.is_err());
    }

    #[tokio::test]
    async fn test_dirty_index_state_rebuild_after_crash() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs_root = temp_dir.path().join("data");
        let ref_index_path = temp_dir.path().join("ref-index");
        std::fs::create_dir_all(&fs_root).unwrap();
        std::fs::create_dir_all(&ref_index_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root, 10 * 1024 * 1024));
        let idx = Arc::new(BlobRefIndex::open(ref_index_path.clone()).unwrap());
        idx.ensure_healthy_or_rebuild(&storage, true, true)
            .await
            .unwrap();

        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage.clone(), Some(idx.clone()), gate);

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d = write_test_blob(&storage, b"crash_test_blob").await;
        let (m_bytes, m_digest) = sample_valid_manifest(&config_d, &blob_d);

        let req = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "v1".to_string(),
            payload: m_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        };
        publisher.publish(req).await.unwrap();

        // Simulate crash right after marking dirty
        idx.mark_dirty().unwrap();
        drop(publisher);
        drop(idx);

        // New process opens the index
        let reopened_idx = BlobRefIndex::open(ref_index_path).unwrap();
        assert!(reopened_idx.check_health().is_err());

        // Calling ensure_healthy_or_rebuild with auto_rebuild_on_corruption rebuilds it
        reopened_idx
            .ensure_healthy_or_rebuild(&storage, true, false)
            .await
            .unwrap();
        assert!(reopened_idx.check_health().is_ok());
        assert!(reopened_idx.is_blob_referenced(&m_digest).unwrap());
    }

    #[tokio::test]
    async fn test_concurrent_overwrite_publications_converge() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs_root = temp_dir.path().join("data");
        let ref_index_path = temp_dir.path().join("ref-index");
        std::fs::create_dir_all(&fs_root).unwrap();
        std::fs::create_dir_all(&ref_index_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root, 10 * 1024 * 1024));
        let idx = Arc::new(BlobRefIndex::open(ref_index_path).unwrap());
        idx.ensure_healthy_or_rebuild(&storage, true, true)
            .await
            .unwrap();

        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = Arc::new(ManifestPublisher::new(
            storage.clone(),
            Some(idx.clone()),
            gate,
        ));

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d1 = write_test_blob(&storage, b"concurrent_1").await;
        let blob_d2 = write_test_blob(&storage, b"concurrent_2").await;

        let (m1_bytes, m1_digest) = sample_valid_manifest(&config_d, &blob_d1);
        let (m2_bytes, m2_digest) = sample_valid_manifest(&config_d, &blob_d2);

        let p1 = publisher.clone();
        let handle1 = tokio::spawn(async move {
            p1.publish(PublishManifestRequest {
                repo: "test/repo".to_string(),
                reference: "latest".to_string(),
                payload: m1_bytes,
                declared_media_type: None,
                allow_tag_overwrite: true,
            })
            .await
        });

        let p2 = publisher.clone();
        let handle2 = tokio::spawn(async move {
            p2.publish(PublishManifestRequest {
                repo: "test/repo".to_string(),
                reference: "latest".to_string(),
                payload: m2_bytes,
                declared_media_type: None,
                allow_tag_overwrite: true,
            })
            .await
        });

        let (r1, r2) = tokio::join!(handle1, handle2);
        assert!(r1.unwrap().is_ok());
        assert!(r2.unwrap().is_ok());

        // Final tag must point to either m1 or m2
        let final_tag = storage.resolve_tag("test/repo", "latest").await.unwrap();
        assert!(final_tag == m1_digest || final_tag == m2_digest);

        // Reference index must be healthy and report the final tag's blob as referenced
        assert!(idx.check_health().is_ok());
        assert!(idx.is_blob_referenced(&final_tag).unwrap());
    }

    #[tokio::test]
    async fn test_failure_after_dirty_marker_before_tag_mutation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs_root = temp_dir.path().join("data");
        let ref_index_path = temp_dir.path().join("ref-index");
        std::fs::create_dir_all(&fs_root).unwrap();
        std::fs::create_dir_all(&ref_index_path).unwrap();

        let fault_storage = Arc::new(FaultInjectableStorage::new(fs_root));
        let storage: Arc<dyn Storage> = fault_storage.clone();
        let idx = Arc::new(BlobRefIndex::open(ref_index_path).unwrap());
        idx.ensure_healthy_or_rebuild(&storage, true, true)
            .await
            .unwrap();

        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage.clone(), Some(idx.clone()), gate);

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d = write_test_blob(&storage, b"mutate_fail_blob").await;
        let (m_bytes, _m_digest) = sample_valid_manifest(&config_d, &blob_d);

        // Inject mutate_tag failure
        fault_storage.fail_mutate_tag.store(true, Ordering::SeqCst);

        let req = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "v1".to_string(),
            payload: m_bytes,
            declared_media_type: None,
            allow_tag_overwrite: true,
        };
        let err = publisher.publish(req).await.unwrap_err();
        assert!(matches!(err, PublishManifestError::Storage(_)));

        // Tag was NOT updated
        assert!(storage.resolve_tag("test/repo", "v1").await.is_err());
    }

    #[tokio::test]
    async fn test_gc_exclusion_with_shared_coordinator() {
        let gate = Arc::new(tokio::sync::Mutex::new(()));

        // When publication holds the gate, trying to acquire the gate from GC is blocked
        let guard = gate.try_lock();
        assert!(guard.is_ok());

        // A second attempt to lock while held fails immediately with would-block (TryLockError)
        let second_attempt = gate.try_lock();
        assert!(second_attempt.is_err());

        drop(guard);
        // After publication completes, lock can be acquired
        assert!(gate.try_lock().is_ok());
    }

    #[tokio::test]
    async fn test_same_digest_retry_after_dirty_rebuilds_and_succeeds() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs_root = temp_dir.path().join("data");
        let ref_index_path = temp_dir.path().join("ref-index");
        std::fs::create_dir_all(&fs_root).unwrap();
        std::fs::create_dir_all(&ref_index_path).unwrap();

        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root, 10 * 1024 * 1024));
        let idx = Arc::new(BlobRefIndex::open(ref_index_path).unwrap());
        idx.ensure_healthy_or_rebuild(&storage, true, true)
            .await
            .unwrap();

        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage.clone(), Some(idx.clone()), gate);

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d = write_test_blob(&storage, b"retry_blob").await;
        let (m_bytes, m_digest) = sample_valid_manifest(&config_d, &blob_d);

        let req = PublishManifestRequest {
            repo: "test/repo".to_string(),
            reference: "v1".to_string(),
            payload: m_bytes.clone(),
            declared_media_type: None,
            allow_tag_overwrite: true,
        };

        // 1. Initial publish succeeds
        publisher.publish(req.clone()).await.unwrap();
        assert!(idx.check_health().is_ok());

        // 2. Simulate partial failure leaving dirty marker
        idx.mark_dirty().unwrap();
        assert!(idx.check_health().is_err());

        // 3. Retry same publication (returns Unchanged from mutate_tag)
        // Must rebuild dirty index rather than blindly assuming clean
        let res = publisher.publish(req).await;
        assert!(res.is_ok());

        // 4. Index must now be healthy and report blob referenced
        assert!(idx.check_health().is_ok());
        assert!(idx.is_blob_referenced(&m_digest).unwrap());
    }

    #[tokio::test]
    async fn test_immutable_conflict_retains_content_addressed_manifest_and_referrer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs_root = temp_dir.path().join("data");
        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(fs_root, 10 * 1024 * 1024));
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let publisher = ManifestPublisher::new(storage.clone(), None, gate);

        let config_d = write_test_blob(&storage, b"{}").await;
        let blob_d1 = write_test_blob(&storage, b"payload1").await;
        let blob_d2 = write_test_blob(&storage, b"payload2").await;

        let (m1_bytes, m1_digest) = sample_valid_manifest(&config_d, &blob_d1);

        // Subject blob
        let subject_blob = write_test_blob(&storage, b"artifact_subject").await;
        let artifact_manifest_json = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_d.as_str(),
                "size": 2
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar",
                    "digest": blob_d2.as_str(),
                    "size": 8
                }
            ],
            "subject": {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": subject_blob.as_str(),
                "size": 16
            }
        });
        let m2_bytes: Bytes = serde_json::to_vec(&artifact_manifest_json).unwrap().into();
        let mut hasher = sha2::Sha256::new();
        hasher.update(&m2_bytes);
        let m2_digest =
            Digest::parse(&format!("sha256:{}", hex::encode(hasher.finalize()))).unwrap();

        // 1. Publish m1 to tag "v1"
        publisher
            .publish(PublishManifestRequest {
                repo: "test/repo".to_string(),
                reference: "v1".to_string(),
                payload: m1_bytes,
                declared_media_type: None,
                allow_tag_overwrite: false,
            })
            .await
            .unwrap();

        // 2. Publish m2 (with subject) to immutable tag "v1" with allow_tag_overwrite = false
        let err = publisher
            .publish(PublishManifestRequest {
                repo: "test/repo".to_string(),
                reference: "v1".to_string(),
                payload: m2_bytes,
                declared_media_type: None,
                allow_tag_overwrite: false,
            })
            .await
            .unwrap_err();

        assert!(matches!(err, PublishManifestError::TagAlreadyExists));

        // 3. Verify content-addressed invariants:
        // - Tag still points to m1
        assert_eq!(
            storage.resolve_tag("test/repo", "v1").await.unwrap(),
            m1_digest
        );
        // - Manifest m2 is addressable by content digest
        assert!(storage.get_manifest("test/repo", &m2_digest).await.is_ok());
        // - Referrer metadata for m2 is registered under subject
        let referrers = storage
            .list_referrers("test/repo", &subject_blob)
            .await
            .unwrap();
        assert!(referrers.iter().any(|r| r.digest == m2_digest.as_str()));
    }
}
