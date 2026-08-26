use std::sync::Arc;

use bytes::Bytes;
use sha2::Digest as _;

use crate::blob_ref_index::BlobRefIndex;
use crate::manifest_refs::parse_manifest_refs;
use crate::registry::digest::Digest;
use crate::registry::validation::{is_valid_repo_name, is_valid_tag};
use crate::storage::{ReferrerDescriptor, Storage, StorageError, TagMutation, TagMutationPolicy};

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestDeleteResult {
    pub digest: Digest,
    pub removed_tags: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagDeleteResult {
    pub tag: String,
    pub target_digest: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagMutationResult {
    pub tag: String,
    pub digest: Digest,
    pub mutation: TagMutation,
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestLifecycleError {
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

    #[error("tag not found")]
    TagNotFound,

    #[error("manifest not found")]
    ManifestNotFound,

    #[error("tag already exists and cannot be overwritten")]
    TagAlreadyExists,

    #[error("tag precondition failed")]
    TagPreconditionFailed,

    #[error("manifest digest mismatch: expected {expected}, computed {computed}")]
    DigestMismatch { expected: String, computed: String },

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("reference index error: {0}")]
    RefIndex(#[from] crate::blob_ref_index::RefIndexError),

    #[error("internal lifecycle error: {0}")]
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
pub struct ManifestLifecycleService {
    storage: Arc<dyn Storage>,
    ref_index: Option<Arc<BlobRefIndex>>,
    consistency_gate: Arc<tokio::sync::Mutex<()>>,
}

impl ManifestLifecycleService {
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

    pub async fn publish(
        &self,
        req: PublishManifestRequest,
    ) -> Result<PublishedManifest, ManifestLifecycleError> {
        self.publish_manifest(req).await
    }

    /// Orchestrates manifest publication with strict pre-mutation validation,
    /// authoritative content storage, an atomic tag commit point, synchronous referrer registration,
    /// durable reference index dirty tracking, and single-instance coordination.
    pub async fn publish_manifest(
        &self,
        req: PublishManifestRequest,
    ) -> Result<PublishedManifest, ManifestLifecycleError> {
        // --- 1. Pure Validation (Preflight Before Any Storage Mutation) ---
        if !is_valid_repo_name(&req.repo) {
            return Err(ManifestLifecycleError::InvalidRepoName);
        }

        if req.payload.is_empty() {
            return Err(ManifestLifecycleError::EmptyPayload);
        }

        if req.payload.len() > MAX_MANIFEST_SIZE {
            return Err(ManifestLifecycleError::PayloadTooLarge);
        }

        let manifest_json: serde_json::Value = serde_json::from_slice(&req.payload)
            .map_err(|e| ManifestLifecycleError::InvalidManifest(e.to_string()))?;

        // Schema version 1 / legacy signatures rejection
        if let Some(schema_version) = manifest_json.get("schemaVersion").and_then(|v| v.as_i64()) {
            if schema_version == 1 {
                if manifest_json.get("signatures").is_some()
                    || manifest_json.get("signature").is_some()
                {
                    return Err(ManifestLifecycleError::Unverified(
                        "manifest failed signature verification".to_string(),
                    ));
                }
                return Err(ManifestLifecycleError::InvalidManifest(
                    "schemaVersion 1 unsupported".to_string(),
                ));
            }
        }

        if manifest_json.get("signatures").is_some() || manifest_json.get("signature").is_some() {
            return Err(ManifestLifecycleError::Unverified(
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
                return Err(ManifestLifecycleError::Unverified(
                    "manifest signatures unverified".to_string(),
                ));
            }
            return Err(ManifestLifecycleError::InvalidManifest(
                "docker schema v1 manifest unsupported".to_string(),
            ));
        }

        if !is_supported_manifest_media_type(&media_type) {
            return Err(ManifestLifecycleError::UnsupportedMediaType(media_type));
        }

        // Parse and validate descriptor references
        let refs = parse_manifest_refs(&req.payload)
            .map_err(|e| ManifestLifecycleError::InvalidManifest(e.to_string()))?;

        // Pre-parse referrer info
        let referrer_info = crate::manifest_refs::parse_referrer_info(&req.payload)
            .map_err(|e| ManifestLifecycleError::InvalidManifest(e.to_string()))?;
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
                    return Err(ManifestLifecycleError::DigestMismatch {
                        expected: ref_digest.to_string(),
                        computed: computed.to_string(),
                    });
                }
                false
            }
            Err(_) => {
                if !is_valid_tag(&req.reference) {
                    return Err(ManifestLifecycleError::InvalidTag);
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

        // Pre-check referenced blobs under consistency gate (config, layers, artifact blobs)
        for blob_d in refs.blob_references() {
            if blob_d.as_str()
                == "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                || blob_d.as_str()
                    == "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            {
                continue;
            }
            // Require repository-scoped membership for referenced blobs
            match self
                .storage
                .get_repo_blob_membership(&req.repo, blob_d)
                .await
            {
                Ok(Some(_)) => {}
                Ok(None) => return Err(ManifestLifecycleError::MissingBlob(blob_d.to_string())),
                Err(e) => return Err(ManifestLifecycleError::Storage(e)),
            }
        }

        // Pre-check referenced child manifests (in index or manifest lists)
        for manifest_d in &refs.manifests {
            match self.storage.head_manifest(&req.repo, manifest_d).await {
                Ok(_) => {}
                Err(StorageError::NotFound) => {
                    return Err(ManifestLifecycleError::MissingManifest(
                        manifest_d.to_string(),
                    ));
                }
                Err(e) => return Err(ManifestLifecycleError::Storage(e)),
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

            let _mutation = match self
                .storage
                .mutate_tag(&req.repo, &req.reference, &computed, policy)
                .await
            {
                Ok(m) => m,
                Err(StorageError::TagAlreadyExists) => {
                    if let Some(idx) = self.ref_index.as_ref() {
                        let _ = idx.mark_ready();
                    }
                    return Err(ManifestLifecycleError::TagAlreadyExists);
                }
                Err(err) => {
                    return Err(ManifestLifecycleError::Storage(err));
                }
            };

            // Synchronously update reference index
            if let Some(idx) = self.ref_index.as_ref() {
                idx.on_manifest_published(
                    &self.storage,
                    &req.repo,
                    &computed,
                    Some(&req.reference),
                )
                .await?;
                idx.mark_ready()?;
            }
        } else if let Some(idx) = self.ref_index.as_ref() {
            idx.mark_dirty()?;
            idx.on_manifest_published(&self.storage, &req.repo, &computed, None)
                .await?;
            idx.mark_ready()?;
        }

        Ok(PublishedManifest {
            digest: computed,
            media_type: meta.media_type,
            size: meta.size,
            subject: subject_digest,
            is_tag,
        })
    }

    /// Deletes a stored manifest by digest according to Policy B:
    /// 1. Preflight verifies manifest exists.
    /// 2. Durably marks index dirty.
    /// 3. Removes all tags referencing this manifest digest.
    /// 4. Cleans up from subject referrer index if it was a referrer.
    /// 5. Deletes the stored manifest CAS object.
    /// 6. Reconciles reference index and marks ready.
    pub async fn delete_manifest(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<ManifestDeleteResult, ManifestLifecycleError> {
        if !is_valid_repo_name(repo) {
            return Err(ManifestLifecycleError::InvalidRepoName);
        }

        let _gate = self.consistency_gate.lock().await;

        if let Some(idx) = self.ref_index.as_ref() {
            if idx.check_health().is_err() {
                idx.ensure_healthy_or_rebuild(&self.storage, true, false)
                    .await?;
            }
        }

        // Verify manifest exists in storage before marking index dirty
        let (_meta, bytes) = match self.storage.get_manifest(repo, digest).await {
            Ok(res) => res,
            Err(StorageError::NotFound) => return Err(ManifestLifecycleError::ManifestNotFound),
            Err(e) => return Err(ManifestLifecycleError::Storage(e)),
        };

        let maybe_subject = crate::manifest_refs::extract_subject_digest(&bytes)
            .ok()
            .flatten();

        if let Some(idx) = self.ref_index.as_ref() {
            idx.mark_dirty()?;
        }

        // 1. Scan and remove all tags pointing to this manifest digest
        let mut removed_tags: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        let digest_str = digest.as_str();

        loop {
            let (page, next_tok) = match self
                .storage
                .list_tags_page(repo, token.as_deref(), 128)
                .await
            {
                Ok(res) => res,
                Err(StorageError::NotFound) => (Vec::new(), None),
                Err(e) => return Err(ManifestLifecycleError::Storage(e)),
            };

            for (tag, target) in page {
                if target.as_str() == digest_str {
                    let _ = self.storage.delete_tag(repo, &tag).await;
                    removed_tags.push(tag);
                }
            }

            match next_tok {
                Some(tok) => token = Some(tok),
                None => break,
            }
        }

        // 2. Clean up from referrers list if this manifest referenced a subject
        if let Some(subject) = maybe_subject {
            let _ = self.storage.remove_referrer(repo, &subject, digest).await;
        }

        // 3. Delete stored manifest
        self.storage.delete_manifest(repo, digest).await?;

        // 4. Reconcile reference index
        if let Some(idx) = self.ref_index.as_ref() {
            idx.on_manifest_deleted(repo, digest)?;
            idx.mark_ready()?;
        }

        Ok(ManifestDeleteResult {
            digest: digest.clone(),
            removed_tags,
        })
    }

    /// Deletes a tag alias without deleting the underlying stored manifest or its blob references.
    pub async fn delete_tag(
        &self,
        repo: &str,
        tag: &str,
    ) -> Result<TagDeleteResult, ManifestLifecycleError> {
        if !is_valid_repo_name(repo) {
            return Err(ManifestLifecycleError::InvalidRepoName);
        }
        if !is_valid_tag(tag) {
            return Err(ManifestLifecycleError::InvalidTag);
        }

        let _gate = self.consistency_gate.lock().await;

        if let Some(idx) = self.ref_index.as_ref() {
            if idx.check_health().is_err() {
                idx.ensure_healthy_or_rebuild(&self.storage, true, false)
                    .await?;
            }
        }

        let target_digest = match self.storage.resolve_tag(repo, tag).await {
            Ok(d) => d,
            Err(StorageError::NotFound) => return Err(ManifestLifecycleError::TagNotFound),
            Err(e) => return Err(ManifestLifecycleError::Storage(e)),
        };

        if let Some(idx) = self.ref_index.as_ref() {
            idx.mark_dirty()?;
        }

        self.storage.delete_tag(repo, tag).await?;

        if let Some(idx) = self.ref_index.as_ref() {
            idx.on_tag_deleted(repo, tag)?;
            idx.mark_ready()?;
        }

        Ok(TagDeleteResult {
            tag: tag.to_string(),
            target_digest,
        })
    }

    /// Atomically mutates a tag pointer pointing to an already-stored manifest.
    pub async fn mutate_tag(
        &self,
        repo: &str,
        tag: &str,
        target_digest: &Digest,
        policy: TagMutationPolicy,
    ) -> Result<TagMutationResult, ManifestLifecycleError> {
        if !is_valid_repo_name(repo) {
            return Err(ManifestLifecycleError::InvalidRepoName);
        }
        if !is_valid_tag(tag) {
            return Err(ManifestLifecycleError::InvalidTag);
        }

        let _gate = self.consistency_gate.lock().await;

        if let Some(idx) = self.ref_index.as_ref() {
            if idx.check_health().is_err() {
                idx.ensure_healthy_or_rebuild(&self.storage, true, false)
                    .await?;
            }
        }

        // Verify target manifest exists
        match self.storage.head_manifest(repo, target_digest).await {
            Ok(_) => {}
            Err(StorageError::NotFound) => return Err(ManifestLifecycleError::ManifestNotFound),
            Err(e) => return Err(ManifestLifecycleError::Storage(e)),
        }

        if let Some(idx) = self.ref_index.as_ref() {
            idx.mark_dirty()?;
        }

        let mutation = match self
            .storage
            .mutate_tag(repo, tag, target_digest, policy)
            .await
        {
            Ok(m) => m,
            Err(StorageError::TagAlreadyExists) => {
                if let Some(idx) = self.ref_index.as_ref() {
                    let _ = idx.mark_ready();
                }
                return Err(ManifestLifecycleError::TagAlreadyExists);
            }
            Err(e) => return Err(ManifestLifecycleError::Storage(e)),
        };

        if let Some(idx) = self.ref_index.as_ref() {
            idx.on_manifest_published(&self.storage, repo, target_digest, Some(tag))
                .await?;
            idx.mark_ready()?;
        }

        Ok(TagMutationResult {
            tag: tag.to_string(),
            digest: target_digest.clone(),
            mutation,
        })
    }
}
