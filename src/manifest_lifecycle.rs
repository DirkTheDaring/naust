use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;

use crate::blob_ref_index::BlobRefIndex;
use crate::manifest_refs::parse_manifest_refs;
use crate::registry::canonical_name::CanonicalRepoName;
use crate::registry::digest::Digest;
use crate::registry::validation::is_valid_tag;
use crate::storage::{
    ConditionalDeleteResult, ReferrerDescriptor, Storage, StorageError, TagMutation,
    TagMutationPolicy,
};

pub const MAX_MANIFEST_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
pub const REPO_LEASE_TTL_SECS: u64 = 15;
pub const REPO_LEASE_RENEW_SECS: u64 = 4;
pub const POLICY_B_TAG_PAGE_SIZE: usize = 64;

#[derive(Clone, Debug)]
pub struct PublishManifestRequest {
    pub repo: String,
    pub reference: String,
    pub payload: Bytes,
    pub declared_media_type: Option<String>,
    pub allow_tag_overwrite: bool,
}

impl PublishManifestRequest {
    pub fn new(
        repo: impl Into<String>,
        reference: impl Into<String>,
        payload: Bytes,
        declared_media_type: Option<String>,
        allow_tag_overwrite: bool,
    ) -> Self {
        Self {
            repo: repo.into(),
            reference: reference.into(),
            payload,
            declared_media_type,
            allow_tag_overwrite,
        }
    }
}

/// Unforgeable capability for proxy-origin manifest publication.
///
/// Only trusted proxy fetch routines (`crate::proxy`) can construct this type after
/// validating upstream response headers, content digests, media types, and structural validity.
#[derive(Clone, Debug)]
pub struct ProxyPublicationEvidence {
    pub(crate) repo: String,
    pub(crate) reference: String,
    pub(crate) payload: Bytes,
    pub(crate) declared_media_type: Option<String>,
    pub(crate) allow_tag_overwrite: bool,
    pub(crate) verified_digest: Digest,
}

impl ProxyPublicationEvidence {
    pub(crate) fn new(
        repo: impl Into<String>,
        reference: impl Into<String>,
        payload: Bytes,
        declared_media_type: Option<String>,
        allow_tag_overwrite: bool,
        verified_digest: Digest,
    ) -> Self {
        Self {
            repo: repo.into(),
            reference: reference.into(),
            payload,
            declared_media_type,
            allow_tag_overwrite,
            verified_digest,
        }
    }

    /// Convenience constructor for tests.
    pub fn new_for_test(
        repo: impl Into<String>,
        reference: impl Into<String>,
        payload: Bytes,
        declared_media_type: Option<String>,
        allow_tag_overwrite: bool,
        verified_digest: Digest,
    ) -> Self {
        Self::new(
            repo,
            reference,
            payload,
            declared_media_type,
            allow_tag_overwrite,
            verified_digest,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyEvictionResult {
    pub repo: String,
    pub target_digest: Digest,
    pub tag_removed: Option<String>,
    pub manifest_removed: bool,
    pub memberships_unlinked: usize,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum LifecycleOpKind {
    Publish,
    DeleteManifest,
    DeleteTag,
    ProxyEvict,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum LifecyclePhase {
    // --- Publication Phases ---
    ManifestStored,
    ReferrerRegistered,
    TagMutated,

    // --- Manifest Deletion (Policy B) Phases ---
    TagsSnapshotted,
    TagsDeleted,
    ReferrerCleaned,
    ManifestDeleted,

    // --- Tag Deletion Phases ---
    TagDeleteInitiated,
    TagDeletedOnly,

    // --- Proxy Eviction Phases ---
    ProxyEvictInitiated,
    ProxyTagDeleted,
    ProxyManifestDeleted,
    ProxyMembershipsUnlinked,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TagSnapshot {
    pub tag: String,
    pub observed_version: String,
    pub target_digest: Digest,
    pub deleted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LifecycleJournalRecord {
    pub op_id: String,
    pub repo: String,
    pub op_kind: LifecycleOpKind,
    pub target_digest: Digest,
    pub target_reference: Option<String>,
    pub phase: LifecyclePhase,
    pub owner_id: String,
    pub lease_expiry_unix_secs: u64,
    pub started_unix_secs: u64,
    pub updated_unix_secs: u64,
    pub relevant_tags: Vec<TagSnapshot>,
    pub subject_digest: Option<Digest>,
    pub artifact_type: Option<String>,
    pub annotations: Option<HashMap<String, String>>,
    pub media_type: Option<String>,
    pub manifest_size: Option<u64>,
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

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub struct RepoCoordinationGuard<'a> {
    _gate: tokio::sync::MutexGuard<'a, ()>,
    storage: Arc<dyn Storage>,
    repo: String,
    owner_id: String,
    lease_id: String,
    renew_handle: Option<tokio::task::JoinHandle<()>>,
    failure_rx: tokio::sync::mpsc::Receiver<String>,
    released: bool,
}

impl<'a> RepoCoordinationGuard<'a> {
    pub async fn check_lease(&mut self) -> Result<(), ManifestLifecycleError> {
        if let Ok(err) = self.failure_rx.try_recv() {
            return Err(ManifestLifecycleError::Internal(format!(
                "repository lease lost: {err}"
            )));
        }
        Ok(())
    }

    pub async fn release(&mut self) -> Result<(), ManifestLifecycleError> {
        if self.released {
            return Ok(());
        }
        self.released = true;
        if let Some(handle) = self.renew_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
        self.storage
            .release_repo_lease(&self.repo, &self.owner_id, &self.lease_id)
            .await
            .map_err(ManifestLifecycleError::Storage)?;
        Ok(())
    }
}

impl<'a> Drop for RepoCoordinationGuard<'a> {
    fn drop(&mut self) {
        if let Some(handle) = self.renew_handle.take() {
            handle.abort();
        }
        if !self.released {
            self.released = true;
            let storage = Arc::clone(&self.storage);
            let repo = self.repo.clone();
            let owner_id = self.owner_id.clone();
            let lease_id = self.lease_id.clone();
            tokio::spawn(async move {
                let _ = storage
                    .release_repo_lease(&repo, &owner_id, &lease_id)
                    .await;
            });
        }
    }
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

    async fn acquire_coordination<'a>(
        &'a self,
        repo: &str,
    ) -> Result<RepoCoordinationGuard<'a>, ManifestLifecycleError> {
        let gate = self.consistency_gate.lock().await;

        let owner_id = uuid::Uuid::new_v4().to_string();
        let lease_id = uuid::Uuid::new_v4().to_string();

        let mut acquired = false;
        for attempt in 0..10 {
            match self
                .storage
                .acquire_repo_lease(repo, &owner_id, &lease_id, REPO_LEASE_TTL_SECS)
                .await
            {
                Ok(true) => {
                    acquired = true;
                    break;
                }
                Ok(false) => {
                    tokio::time::sleep(Duration::from_millis(50 * (1 << attempt.min(4)))).await;
                }
                Err(e) => return Err(ManifestLifecycleError::Storage(e)),
            }
        }

        if !acquired {
            return Err(ManifestLifecycleError::Internal(
                "repository coordination lease held by concurrent writer".to_string(),
            ));
        }

        let (failure_tx, failure_rx) = tokio::sync::mpsc::channel(1);
        let storage_clone = Arc::clone(&self.storage);
        let repo_string = repo.to_string();
        let owner_clone = owner_id.clone();
        let lease_clone = lease_id.clone();

        let renew_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(REPO_LEASE_RENEW_SECS)).await;
                match storage_clone
                    .renew_repo_lease(
                        &repo_string,
                        &owner_clone,
                        &lease_clone,
                        REPO_LEASE_TTL_SECS,
                    )
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        let _ = failure_tx
                            .send("repository lease renewal failed: lease lost".to_string())
                            .await;
                        break;
                    }
                    Err(e) => {
                        let _ = failure_tx
                            .send(format!("repository lease renewal error: {e}"))
                            .await;
                        break;
                    }
                }
            }
        });

        Ok(RepoCoordinationGuard {
            _gate: gate,
            storage: Arc::clone(&self.storage),
            repo: repo.to_string(),
            owner_id,
            lease_id,
            renew_handle: Some(renew_handle),
            failure_rx,
            released: false,
        })
    }

    async fn read_journal(
        &self,
        repo: &str,
    ) -> Result<Option<LifecycleJournalRecord>, ManifestLifecycleError> {
        let bytes = match self.storage.read_lifecycle_journal(repo).await? {
            Some(b) => b,
            None => return Ok(None),
        };
        let record: LifecycleJournalRecord = serde_json::from_slice(&bytes)
            .map_err(|e| ManifestLifecycleError::Internal(format!("corrupt journal: {e}")))?;
        Ok(Some(record))
    }

    async fn write_journal(
        &self,
        repo: &str,
        record: &LifecycleJournalRecord,
    ) -> Result<(), ManifestLifecycleError> {
        let bytes = Bytes::from(serde_json::to_vec(record).map_err(|e| {
            ManifestLifecycleError::Internal(format!("serialize journal failed: {e}"))
        })?);
        self.storage
            .write_lifecycle_journal(repo, bytes)
            .await
            .map_err(ManifestLifecycleError::Storage)?;
        Ok(())
    }

    async fn delete_journal(&self, repo: &str) -> Result<(), ManifestLifecycleError> {
        self.storage
            .delete_lifecycle_journal(repo)
            .await
            .map_err(ManifestLifecycleError::Storage)
    }

    pub async fn recover_and_ensure_index_healthy(
        &self,
        repo: &str,
    ) -> Result<(), ManifestLifecycleError> {
        if let Some(journal) = self.read_journal(repo).await? {
            self.recover_pending_journal_under_lock(repo, &journal)
                .await?;
        }

        if let Some(idx) = self.ref_index.as_ref() {
            if idx.check_health().is_err() {
                idx.ensure_healthy_or_rebuild(&self.storage, true, false)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn recover_pending_journal_under_lock(
        &self,
        repo: &str,
        journal: &LifecycleJournalRecord,
    ) -> Result<(), ManifestLifecycleError> {
        if let Some(idx) = self.ref_index.as_ref() {
            idx.mark_dirty()?;
        }

        match journal.op_kind {
            LifecycleOpKind::Publish => {
                if self
                    .storage
                    .head_manifest(repo, &journal.target_digest)
                    .await
                    .is_ok()
                {
                    if let Some(ref subject) = journal.subject_digest {
                        let desc = ReferrerDescriptor {
                            media_type: journal.media_type.clone().unwrap_or_default(),
                            digest: journal.target_digest.as_str(),
                            size: journal.manifest_size.unwrap_or(0),
                            artifact_type: journal.artifact_type.clone(),
                            annotations: journal.annotations.clone(),
                        };
                        let _ = self.storage.add_referrer(repo, subject, desc).await;
                    }

                    if let Some(ref tag) = journal.target_reference {
                        let _ = self
                            .storage
                            .mutate_tag(
                                repo,
                                tag,
                                &journal.target_digest,
                                TagMutationPolicy::Replace,
                            )
                            .await;
                    }

                    if let Some(idx) = self.ref_index.as_ref() {
                        idx.on_manifest_published(
                            &self.storage,
                            repo,
                            &journal.target_digest,
                            journal.target_reference.as_deref(),
                        )
                        .await?;
                        idx.flush()?;
                        idx.mark_ready()?;
                    }
                }
                self.delete_journal(repo).await?;
            }
            LifecycleOpKind::DeleteManifest => {
                // Resume tag deletion for current batch
                for tag_snap in &journal.relevant_tags {
                    if !tag_snap.deleted {
                        let _ = self
                            .storage
                            .delete_tag_conditional(
                                repo,
                                &tag_snap.tag,
                                Some(&tag_snap.observed_version),
                            )
                            .await;
                    }
                }

                // If interrupted during tag deletion, finish deleting any remaining tags
                if journal.phase == LifecyclePhase::TagsSnapshotted {
                    let mut proof_token: Option<String> = None;
                    loop {
                        let (page, next_tok) = match self
                            .storage
                            .list_tags_page(repo, proof_token.as_deref(), POLICY_B_TAG_PAGE_SIZE)
                            .await
                        {
                            Ok(res) => res,
                            Err(_) => (Vec::new(), None),
                        };

                        for (t, d) in page {
                            if d.as_str() == journal.target_digest.as_str() {
                                let _ = self.storage.delete_tag(repo, &t).await;
                            }
                        }

                        match next_tok {
                            Some(tok) => proof_token = Some(tok),
                            None => break,
                        }
                    }
                }

                if let Some(ref subject) = journal.subject_digest {
                    let _ = self
                        .storage
                        .remove_referrer(repo, subject, &journal.target_digest)
                        .await;
                }

                let _ = self
                    .storage
                    .delete_manifest(repo, &journal.target_digest)
                    .await;

                if let Some(idx) = self.ref_index.as_ref() {
                    let _ = idx.on_manifest_deleted(repo, &journal.target_digest);
                    idx.flush()?;
                    idx.mark_ready()?;
                }
                self.delete_journal(repo).await?;
            }
            LifecycleOpKind::DeleteTag => {
                if let Some(ref tag) = journal.target_reference {
                    if let Ok(Some((target, _version))) =
                        self.storage.get_tag_with_version(repo, tag).await
                    {
                        if target == journal.target_digest {
                            let _ = self.storage.delete_tag(repo, tag).await;
                        }
                    }

                    if let Some(idx) = self.ref_index.as_ref() {
                        let _ = idx.on_tag_deleted(repo, tag);
                        idx.flush()?;
                        idx.mark_ready()?;
                    }
                }
                self.delete_journal(repo).await?;
            }
            LifecycleOpKind::ProxyEvict => {
                // 1. If tag specified in journal, finish conditionally deleting tag alias
                if let Some(ref tag) = journal.target_reference {
                    if let Ok(Some((target, version))) =
                        self.storage.get_tag_with_version(repo, tag).await
                    {
                        if target == journal.target_digest {
                            let _ = self
                                .storage
                                .delete_tag_conditional(repo, tag, Some(&version))
                                .await;
                            if let Some(idx) = self.ref_index.as_ref() {
                                let _ = idx.on_tag_deleted(repo, tag);
                            }
                        }
                    }
                }

                // 2. Check if any other tags in the repo resolve to target_digest
                let mut has_other_tags = false;
                let mut page_tok: Option<String> = None;
                loop {
                    let (page, next_tok) = match self
                        .storage
                        .list_tags_page(repo, page_tok.as_deref(), POLICY_B_TAG_PAGE_SIZE)
                        .await
                    {
                        Ok(p) => p,
                        Err(_) => (Vec::new(), None),
                    };
                    for (_t_name, t_d) in page {
                        if t_d == journal.target_digest {
                            has_other_tags = true;
                            break;
                        }
                    }
                    if has_other_tags {
                        break;
                    }
                    match next_tok {
                        Some(tok) => page_tok = Some(tok),
                        None => break,
                    }
                }

                // 3. If no remaining tags resolve to target_digest, finish removing manifest root & proxy memberships
                if !has_other_tags {
                    let refs = match self
                        .storage
                        .get_manifest(repo, &journal.target_digest)
                        .await
                    {
                        Ok((_meta, bytes)) => {
                            crate::manifest_refs::parse_manifest_refs(&bytes).ok()
                        }
                        Err(_) => None,
                    };

                    let _ = self
                        .storage
                        .delete_manifest(repo, &journal.target_digest)
                        .await;

                    if let Some(idx) = self.ref_index.as_ref() {
                        let _ = idx.on_manifest_deleted(repo, &journal.target_digest);
                        idx.mark_ready()?;
                        idx.flush()?;
                    }

                    if let Some(refs) = refs {
                        for blob_d in refs.blob_references() {
                            let still_referenced =
                                self.is_blob_referenced_in_repo(repo, blob_d).await;

                            if !still_referenced {
                                if let Ok(Some(record)) =
                                    self.storage.get_repo_blob_membership(repo, blob_d).await
                                {
                                    if record.provenance
                                        == crate::storage::repo_membership::MembershipProvenance::Proxy
                                    {
                                        let _ = self.storage.unlink_repo_blob(repo, blob_d).await;
                                    }
                                }
                            }
                        }
                    }
                }

                if let Some(idx) = self.ref_index.as_ref() {
                    idx.mark_ready()?;
                    idx.flush()?;
                }

                self.delete_journal(repo).await?;
            }
        }
        Ok(())
    }

    async fn is_blob_referenced_in_repo(&self, repo: &str, target_blob: &Digest) -> bool {
        let mut tok: Option<String> = None;
        loop {
            let (page, next_tok) = match self
                .storage
                .list_manifest_digests_page(repo, tok.as_deref(), 100)
                .await
            {
                Ok(p) => p,
                Err(_) => return false,
            };
            for m_d in page {
                if let Ok((_meta, bytes)) = self.storage.get_manifest(repo, &m_d).await {
                    if let Ok(refs) = crate::manifest_refs::parse_manifest_refs(&bytes) {
                        for b in refs.blob_references() {
                            if b == target_blob {
                                return true;
                            }
                        }
                    }
                }
            }
            match next_tok {
                Some(t) => tok = Some(t),
                None => break,
            }
        }
        false
    }

    /// Orchestrates manifest publication with strict pre-mutation validation,
    /// durable mark_dirty BEFORE any storage mutation, durable operation journaling,
    /// authoritative content storage, atomic tag commit point, synchronous referrer registration,
    /// durable reference index reconciliation, and multi-process/instance coordination.
    /// Publishes a client-pushed manifest.
    ///
    /// Pure client push path: strictly verifies that all referenced config and layer blobs
    /// already exist and have active repository membership in `req.repo`.
    pub async fn publish_manifest(
        &self,
        req: PublishManifestRequest,
    ) -> Result<PublishedManifest, ManifestLifecycleError> {
        self.publish_internal(
            req.repo,
            req.reference,
            req.payload,
            req.declared_media_type,
            req.allow_tag_overwrite,
            false,
        )
        .await
    }

    /// Publishes an upstream proxy-cached manifest.
    ///
    /// Requires verified `ProxyPublicationEvidence` produced by `crate::proxy`.
    /// Allows lazy blob downloading while establishing the manifest as an authoritative
    /// reachability root in `BlobRefIndex` and journaled tag alias.
    pub async fn publish_proxy_cached_manifest(
        &self,
        evidence: ProxyPublicationEvidence,
    ) -> Result<PublishedManifest, ManifestLifecycleError> {
        let mut hasher = sha2::Sha256::new();
        hasher.update(&evidence.payload);
        let digest_hex = hex::encode(hasher.finalize());
        let computed =
            Digest::parse(&format!("sha256:{digest_hex}")).expect("computed sha256 is valid");

        if evidence.verified_digest.hex() != computed.hex() {
            return Err(ManifestLifecycleError::DigestMismatch {
                expected: evidence.verified_digest.to_string(),
                computed: computed.to_string(),
            });
        }

        self.publish_internal(
            evidence.repo,
            evidence.reference,
            evidence.payload,
            evidence.declared_media_type,
            evidence.allow_tag_overwrite,
            true,
        )
        .await
    }

    async fn publish_internal(
        &self,
        repo: String,
        reference: String,
        payload: Bytes,
        declared_media_type: Option<String>,
        allow_tag_overwrite: bool,
        allow_lazy_blobs: bool,
    ) -> Result<PublishedManifest, ManifestLifecycleError> {
        // --- 1. Pure Validation (Preflight Before Any Mutation) ---
        if CanonicalRepoName::parse(&repo).is_err() {
            return Err(ManifestLifecycleError::InvalidRepoName);
        }

        if payload.is_empty() {
            return Err(ManifestLifecycleError::EmptyPayload);
        }

        if payload.len() > MAX_MANIFEST_SIZE {
            return Err(ManifestLifecycleError::PayloadTooLarge);
        }

        let manifest_json: serde_json::Value = serde_json::from_slice(&payload)
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
            .or(declared_media_type)
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
        let refs = parse_manifest_refs(&payload)
            .map_err(|e| ManifestLifecycleError::InvalidManifest(e.to_string()))?;

        // Pre-parse referrer info
        let referrer_info = crate::manifest_refs::parse_referrer_info(&payload)
            .map_err(|e| ManifestLifecycleError::InvalidManifest(e.to_string()))?;
        let subject_digest = referrer_info.as_ref().map(|(s, _, _)| s.clone());

        // Compute manifest digest over raw bytes
        let mut hasher = sha2::Sha256::new();
        hasher.update(&payload);
        let digest_hex = hex::encode(hasher.finalize());
        let computed =
            Digest::parse(&format!("sha256:{digest_hex}")).expect("computed sha256 is valid");

        // Validate reference (digest vs tag)
        let is_tag = match Digest::parse(&reference) {
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
                if !is_valid_tag(&reference) {
                    return Err(ManifestLifecycleError::InvalidTag);
                }
                true
            }
        };

        // --- 2. Acquire Repository-Scoped Coordination ---
        let mut guard = self.acquire_coordination(&repo).await?;

        // Recover any interrupted operation and ensure healthy index
        self.recover_and_ensure_index_healthy(&repo).await?;

        // Pre-check referenced blobs and child manifests for client push
        if !allow_lazy_blobs {
            for blob_d in refs.blob_references() {
                if blob_d.as_str()
                    == "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                    || blob_d.as_str()
                        == "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                {
                    continue;
                }
                match self.storage.get_repo_blob_membership(&repo, blob_d).await {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        return Err(ManifestLifecycleError::MissingBlob(blob_d.to_string()));
                    }
                    Err(e) => return Err(ManifestLifecycleError::Storage(e)),
                }
            }

            for manifest_d in &refs.manifests {
                match self.storage.head_manifest(&repo, manifest_d).await {
                    Ok(_) => {}
                    Err(StorageError::NotFound) => {
                        return Err(ManifestLifecycleError::MissingManifest(
                            manifest_d.to_string(),
                        ));
                    }
                    Err(e) => return Err(ManifestLifecycleError::Storage(e)),
                }
            }
        }

        guard.check_lease().await?;

        // --- 3. Durably Mark Index Dirty BEFORE Authoritative Mutations ---
        if let Some(idx) = self.ref_index.as_ref() {
            idx.mark_dirty()?;
        }

        // --- 4. Write Initial Operation Journal ---
        let op_id = uuid::Uuid::new_v4().to_string();
        let now = now_unix_secs();
        let (ref_subject, ref_artifact, ref_annotations) = match referrer_info.clone() {
            Some((s, a, ann)) => (Some(s), a, ann),
            None => (None, None, None),
        };

        let mut journal = LifecycleJournalRecord {
            op_id: op_id.clone(),
            repo: repo.clone(),
            op_kind: LifecycleOpKind::Publish,
            target_digest: computed.clone(),
            target_reference: if is_tag {
                Some(reference.clone())
            } else {
                None
            },
            phase: LifecyclePhase::ManifestStored,
            owner_id: guard.owner_id.clone(),
            lease_expiry_unix_secs: now + REPO_LEASE_TTL_SECS,
            started_unix_secs: now,
            updated_unix_secs: now,
            relevant_tags: Vec::new(),
            subject_digest: ref_subject.clone(),
            artifact_type: ref_artifact.clone(),
            annotations: ref_annotations.clone(),
            media_type: Some(media_type.clone()),
            manifest_size: Some(payload.len() as u64),
        };

        // --- 5. CAS Manifest Storage ---
        guard.check_lease().await?;
        let meta = match self
            .storage
            .put_manifest(&repo, &computed, payload.clone())
            .await
        {
            Ok(m) => m,
            Err(e) => return Err(ManifestLifecycleError::Storage(e)),
        };

        // Write initial journal phase
        self.write_journal(&repo, &journal).await?;

        // --- 6. Referrer Registration (Atomic Step 2) ---
        if let Some(ref subject) = ref_subject {
            guard.check_lease().await?;
            let desc = ReferrerDescriptor {
                digest: computed.to_string(),
                media_type: media_type.clone(),
                size: meta.size,
                artifact_type: ref_artifact.clone(),
                annotations: ref_annotations.clone(),
            };
            self.storage.add_referrer(&repo, subject, desc).await?;

            journal.phase = LifecyclePhase::ReferrerRegistered;
            journal.updated_unix_secs = now_unix_secs();
            self.write_journal(&repo, &journal).await?;
        }

        // --- 7. Tag Mutation (Atomic Step 3) ---
        if is_tag {
            guard.check_lease().await?;
            let policy = if allow_tag_overwrite {
                TagMutationPolicy::Replace
            } else {
                TagMutationPolicy::CreateOnly
            };

            let _mutation_res = match self
                .storage
                .mutate_tag(&repo, &reference, &computed, policy)
                .await
            {
                Ok(m) => m,
                Err(StorageError::TagAlreadyExists) => {
                    if let Some(idx) = self.ref_index.as_ref() {
                        // Reconcile manifest root even if tag mutation was rejected
                        idx.on_manifest_published(&self.storage, &repo, &computed, None)
                            .await?;
                        idx.flush()?;
                        idx.mark_ready()?;
                    }
                    self.delete_journal(&repo).await?;
                    let _ = guard.release().await;
                    return Err(ManifestLifecycleError::TagAlreadyExists);
                }
                Err(err) => {
                    return Err(ManifestLifecycleError::Storage(err));
                }
            };

            if let Some(idx) = self.ref_index.as_ref() {
                idx.on_manifest_published(&self.storage, &repo, &computed, Some(&reference))
                    .await?;
                idx.flush()?;
                idx.mark_ready()?;
            }
        } else if let Some(idx) = self.ref_index.as_ref() {
            idx.on_manifest_published(&self.storage, &repo, &computed, None)
                .await?;
            idx.flush()?;
            idx.mark_ready()?;
        }

        self.delete_journal(&repo).await?;
        guard.release().await?;

        Ok(PublishedManifest {
            digest: computed,
            media_type: meta.media_type,
            size: meta.size,
            subject: subject_digest,
            is_tag,
        })
    }

    /// Performs logical proxy cache eviction under repository coordination:
    /// 1. Conditionally removes only the tag alias that still points to the cached digest.
    /// 2. If no other tags in the repository point to the digest, unindexes the manifest root and removes it.
    /// 3. For any referenced blobs with Proxy provenance not referenced by any other manifest in the repository,
    ///    unlinks the proxy repository membership.
    /// 4. Physical CAS blobs are NOT deleted; physical reclamation is left exclusively to `BlobGcService`.
    pub async fn evict_proxy_cached_entry(
        &self,
        repo: &str,
        tag: Option<&str>,
        target_digest: &Digest,
    ) -> Result<ProxyEvictionResult, ManifestLifecycleError> {
        if CanonicalRepoName::parse(repo).is_err() {
            return Err(ManifestLifecycleError::InvalidRepoName);
        }

        let mut guard = self.acquire_coordination(repo).await?;
        self.recover_and_ensure_index_healthy(repo).await?;

        // 1. Snapshot target tag if provided
        let mut relevant_tags = Vec::new();
        if let Some(t) = tag {
            if let Ok(Some((target, version))) = self.storage.get_tag_with_version(repo, t).await {
                if target == *target_digest {
                    relevant_tags.push(TagSnapshot {
                        tag: t.to_string(),
                        observed_version: version,
                        target_digest: target,
                        deleted: false,
                    });
                }
            }
        }

        // 2. Durably mark index dirty before authoritative mutations
        guard.check_lease().await?;
        if let Some(idx) = self.ref_index.as_ref() {
            idx.mark_dirty()?;
        }

        // 3. Write initial lifecycle journal
        let mut journal = LifecycleJournalRecord {
            op_id: uuid::Uuid::new_v4().to_string(),
            repo: repo.to_string(),
            op_kind: LifecycleOpKind::ProxyEvict,
            target_digest: target_digest.clone(),
            target_reference: tag.map(|s| s.to_string()),
            phase: LifecyclePhase::ProxyEvictInitiated,
            owner_id: guard.owner_id.clone(),
            lease_expiry_unix_secs: now_unix_secs() + REPO_LEASE_TTL_SECS,
            started_unix_secs: now_unix_secs(),
            updated_unix_secs: now_unix_secs(),
            relevant_tags: relevant_tags.clone(),
            subject_digest: None,
            artifact_type: None,
            annotations: None,
            media_type: None,
            manifest_size: None,
        };
        self.write_journal(repo, &journal).await?;

        // 4. Conditionally remove tag alias
        let mut tag_removed = None;
        if let Some(tag_snap) = relevant_tags.first() {
            let res = self
                .storage
                .delete_tag_conditional(repo, &tag_snap.tag, Some(&tag_snap.observed_version))
                .await;
            if matches!(res, Ok(crate::storage::ConditionalDeleteResult::Deleted)) {
                if let Some(idx) = self.ref_index.as_ref() {
                    let _ = idx.on_tag_deleted(repo, &tag_snap.tag);
                }
                tag_removed = Some(tag_snap.tag.clone());

                journal.phase = LifecyclePhase::ProxyTagDeleted;
                journal.updated_unix_secs = now_unix_secs();
                let _ = self.write_journal(repo, &journal).await;
            }
        }

        // 5. Check if any other tags in repo resolve to target_digest
        let mut has_other_tags = false;
        let mut page_tok: Option<String> = None;
        loop {
            let (page, next_tok) = match self
                .storage
                .list_tags_page(repo, page_tok.as_deref(), POLICY_B_TAG_PAGE_SIZE)
                .await
            {
                Ok(p) => p,
                Err(_) => (Vec::new(), None),
            };
            for (_t_name, t_d) in page {
                if t_d.hex() == target_digest.hex() {
                    has_other_tags = true;
                    break;
                }
            }
            if has_other_tags {
                break;
            }
            match next_tok {
                Some(tok) => page_tok = Some(tok),
                None => break,
            }
        }

        let mut manifest_removed = false;
        let mut memberships_unlinked = 0;

        // 6. If no tags point to target_digest, remove manifest root and unneeded proxy memberships
        if !has_other_tags {
            if let Ok((_meta, bytes)) = self.storage.get_manifest(repo, target_digest).await {
                let refs = crate::manifest_refs::parse_manifest_refs(&bytes).ok();

                // Delete manifest from storage
                let _ = self.storage.delete_manifest(repo, target_digest).await;
                manifest_removed = true;

                journal.phase = LifecyclePhase::ProxyManifestDeleted;
                journal.updated_unix_secs = now_unix_secs();
                let _ = self.write_journal(repo, &journal).await;

                // Reconcile index
                if let Some(idx) = self.ref_index.as_ref() {
                    let _ = idx.on_manifest_deleted(repo, target_digest);
                    idx.mark_ready()?;
                    idx.flush()?;
                }

                // If refs were parsed, check if remaining manifests in repo reference each blob
                if let Some(refs) = refs {
                    for blob_d in refs.blob_references() {
                        let still_referenced = self.is_blob_referenced_in_repo(repo, blob_d).await;

                        if !still_referenced {
                            // Check if blob membership is of Proxy provenance
                            if let Ok(Some(record)) =
                                self.storage.get_repo_blob_membership(repo, blob_d).await
                            {
                                if record.provenance
                                    == crate::storage::repo_membership::MembershipProvenance::Proxy
                                {
                                    let _ = self.storage.unlink_repo_blob(repo, blob_d).await;
                                    memberships_unlinked += 1;
                                }
                            }
                        }
                    }
                }

                journal.phase = LifecyclePhase::ProxyMembershipsUnlinked;
                journal.updated_unix_secs = now_unix_secs();
                let _ = self.write_journal(repo, &journal).await;
            }
        }

        if let Some(idx) = self.ref_index.as_ref() {
            idx.mark_ready()?;
            idx.flush()?;
        }

        self.delete_journal(repo).await?;
        guard.release().await?;

        Ok(ProxyEvictionResult {
            repo: repo.to_string(),
            target_digest: target_digest.clone(),
            tag_removed,
            manifest_removed,
            memberships_unlinked,
        })
    }

    /// Deletes a stored manifest by digest according to Policy B:
    /// 1. Acquire repository coordination.
    /// 2. Recover any pending journal and ensure healthy index.
    /// 3. Preflight verifies manifest exists; extracts subject.
    /// 4. Durable Tag Snapshotting: collects all tags currently pointing to this digest.
    /// 5. Durably marks reference index dirty.
    /// 6. Writes operation journal with snapshot.
    /// 7. Conditional Tag Deletion with retry/resnapshot loop until zero tags remain.
    /// 8. Authoritative zero-tag rescan proof.
    /// 9. Referrer descriptor cleanup from subject index if applicable.
    /// 10. Authoritative CAS manifest deletion.
    /// 11. Reference index reconciliation & flush.
    /// 12. Durably marks index ready and deletes journal.
    pub async fn delete_manifest(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<ManifestDeleteResult, ManifestLifecycleError> {
        if CanonicalRepoName::parse(repo).is_err() {
            return Err(ManifestLifecycleError::InvalidRepoName);
        }

        let mut guard = self.acquire_coordination(repo).await?;
        self.recover_and_ensure_index_healthy(repo).await?;

        // 1. Verify manifest exists in storage
        let (_meta, bytes) = match self.storage.get_manifest(repo, digest).await {
            Ok(res) => res,
            Err(StorageError::NotFound) => return Err(ManifestLifecycleError::ManifestNotFound),
            Err(e) => return Err(ManifestLifecycleError::Storage(e)),
        };

        let maybe_subject = crate::manifest_refs::extract_subject_digest(&bytes)
            .ok()
            .flatten();

        // 2. Durably Mark Index Dirty BEFORE any journal or storage mutation
        guard.check_lease().await?;
        if let Some(idx) = self.ref_index.as_ref() {
            idx.mark_dirty()?;
        }

        // 3. Write Initial Operation Journal
        let op_id = uuid::Uuid::new_v4().to_string();
        let now = now_unix_secs();
        let mut journal = LifecycleJournalRecord {
            op_id: op_id.clone(),
            repo: repo.to_string(),
            op_kind: LifecycleOpKind::DeleteManifest,
            target_digest: digest.clone(),
            target_reference: None,
            phase: LifecyclePhase::TagsSnapshotted,
            owner_id: guard.owner_id.clone(),
            lease_expiry_unix_secs: now + REPO_LEASE_TTL_SECS,
            started_unix_secs: now,
            updated_unix_secs: now,
            relevant_tags: Vec::new(),
            subject_digest: maybe_subject.clone(),
            artifact_type: None,
            annotations: None,
            media_type: None,
            manifest_size: None,
        };
        self.write_journal(repo, &journal).await?;

        // 4. Safe Bounded Tag Snapshotting & Deletion Loop
        let mut removed_tags: Vec<String> = Vec::new();
        let digest_str = digest.as_str();

        let mut fixed_point_reached = false;
        while !fixed_point_reached {
            let mut page_token: Option<String> = None;
            let mut matching_found_in_pass = 0;

            loop {
                let (page, next_tok) = match self
                    .storage
                    .list_tags_page(repo, page_token.as_deref(), POLICY_B_TAG_PAGE_SIZE)
                    .await
                {
                    Ok(res) => res,
                    Err(StorageError::NotFound) => (Vec::new(), None),
                    Err(e) => return Err(ManifestLifecycleError::Storage(e)),
                };

                let mut current_batch: Vec<TagSnapshot> = Vec::new();
                for (tag, target) in page {
                    if target.as_str() == digest_str {
                        let version = match self.storage.get_tag_with_version(repo, &tag).await? {
                            Some((_, v)) => v,
                            None => "initial".to_string(),
                        };
                        current_batch.push(TagSnapshot {
                            tag,
                            observed_version: version,
                            target_digest: digest.clone(),
                            deleted: false,
                        });
                    }
                }

                if !current_batch.is_empty() {
                    matching_found_in_pass += current_batch.len();
                    journal.relevant_tags = current_batch;
                    journal.updated_unix_secs = now_unix_secs();
                    self.write_journal(repo, &journal).await?;

                    let batch_len = journal.relevant_tags.len();
                    for i in 0..batch_len {
                        let mut attempts = 0;
                        loop {
                            attempts += 1;
                            let tag_name = journal.relevant_tags[i].tag.clone();
                            let observed_ver = journal.relevant_tags[i].observed_version.clone();
                            match self
                                .storage
                                .delete_tag_conditional(repo, &tag_name, Some(&observed_ver))
                                .await?
                            {
                                ConditionalDeleteResult::Deleted => {
                                    journal.relevant_tags[i].deleted = true;
                                    removed_tags.push(tag_name);
                                    break;
                                }
                                ConditionalDeleteResult::NotFound => {
                                    journal.relevant_tags[i].deleted = true;
                                    break;
                                }
                                ConditionalDeleteResult::PreconditionFailed { .. } => {
                                    match self.storage.get_tag_with_version(repo, &tag_name).await?
                                    {
                                        Some((new_target, new_version)) => {
                                            if new_target.as_str() == digest_str {
                                                if attempts > 3 {
                                                    return Err(ManifestLifecycleError::TagPreconditionFailed);
                                                }
                                                journal.relevant_tags[i].observed_version =
                                                    new_version;
                                                continue;
                                            } else {
                                                // Tag was moved to different manifest; no longer points here
                                                journal.relevant_tags[i].deleted = true;
                                                break;
                                            }
                                        }
                                        None => {
                                            journal.relevant_tags[i].deleted = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        self.write_journal(repo, &journal).await?;
                    }
                }

                match next_tok {
                    Some(tok) => page_token = Some(tok),
                    None => break,
                }
            }

            if matching_found_in_pass == 0 {
                fixed_point_reached = true;
            }
        }

        // 5. Pre-delete Authoritative Proof: verify 0 tags resolve to this digest across full pagination
        let mut proof_token: Option<String> = None;
        loop {
            let (page, next_tok) = self
                .storage
                .list_tags_page(repo, proof_token.as_deref(), POLICY_B_TAG_PAGE_SIZE)
                .await?;
            for (t, d) in page {
                if d.as_str() == digest_str {
                    return Err(ManifestLifecycleError::TagPreconditionFailed);
                }
                let _ = t;
            }
            match next_tok {
                Some(tok) => proof_token = Some(tok),
                None => break,
            }
        }

        journal.phase = LifecyclePhase::TagsDeleted;
        journal.updated_unix_secs = now_unix_secs();
        self.write_journal(repo, &journal).await?;

        // 7. Clean up from referrers list if this manifest referenced a subject
        if let Some(ref subject) = maybe_subject {
            let _ = self.storage.remove_referrer(repo, subject, digest).await;
            journal.phase = LifecyclePhase::ReferrerCleaned;
            journal.updated_unix_secs = now_unix_secs();
            self.write_journal(repo, &journal).await?;
        }

        // 8. Delete stored manifest bytes
        self.storage.delete_manifest(repo, digest).await?;
        journal.phase = LifecyclePhase::ManifestDeleted;
        journal.updated_unix_secs = now_unix_secs();
        self.write_journal(repo, &journal).await?;

        // 9. Reconcile reference index & flush
        if let Some(idx) = self.ref_index.as_ref() {
            idx.on_manifest_deleted(repo, digest)?;
            idx.flush()?;
            idx.mark_ready()?;
        }

        self.delete_journal(repo).await?;
        guard.release().await?;

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
        if CanonicalRepoName::parse(repo).is_err() {
            return Err(ManifestLifecycleError::InvalidRepoName);
        }
        if !is_valid_tag(tag) {
            return Err(ManifestLifecycleError::InvalidTag);
        }

        let mut guard = self.acquire_coordination(repo).await?;
        self.recover_and_ensure_index_healthy(repo).await?;

        let (target_digest, version) = match self.storage.get_tag_with_version(repo, tag).await? {
            Some(res) => res,
            None => return Err(ManifestLifecycleError::TagNotFound),
        };

        guard.check_lease().await?;

        // Durably mark index dirty
        if let Some(idx) = self.ref_index.as_ref() {
            idx.mark_dirty()?;
        }

        let op_id = uuid::Uuid::new_v4().to_string();
        let now = now_unix_secs();
        let mut journal = LifecycleJournalRecord {
            op_id: op_id.clone(),
            repo: repo.to_string(),
            op_kind: LifecycleOpKind::DeleteTag,
            target_digest: target_digest.clone(),
            target_reference: Some(tag.to_string()),
            phase: LifecyclePhase::TagDeleteInitiated,
            owner_id: guard.owner_id.clone(),
            lease_expiry_unix_secs: now + REPO_LEASE_TTL_SECS,
            started_unix_secs: now,
            updated_unix_secs: now,
            relevant_tags: vec![TagSnapshot {
                tag: tag.to_string(),
                observed_version: version.clone(),
                target_digest: target_digest.clone(),
                deleted: false,
            }],
            subject_digest: None,
            artifact_type: None,
            annotations: None,
            media_type: None,
            manifest_size: None,
        };
        self.write_journal(repo, &journal).await?;

        match self
            .storage
            .delete_tag_conditional(repo, tag, Some(&version))
            .await?
        {
            ConditionalDeleteResult::Deleted => {}
            ConditionalDeleteResult::NotFound => {
                self.delete_journal(repo).await?;
                if let Some(idx) = self.ref_index.as_ref() {
                    let _ = idx.mark_ready();
                }
                return Err(ManifestLifecycleError::TagNotFound);
            }
            ConditionalDeleteResult::PreconditionFailed { .. } => {
                self.delete_journal(repo).await?;
                if let Some(idx) = self.ref_index.as_ref() {
                    let _ = idx.mark_ready();
                }
                return Err(ManifestLifecycleError::TagPreconditionFailed);
            }
        }

        journal.phase = LifecyclePhase::TagDeletedOnly;
        journal.updated_unix_secs = now_unix_secs();
        self.write_journal(repo, &journal).await?;

        if let Some(idx) = self.ref_index.as_ref() {
            idx.on_tag_deleted(repo, tag)?;
            idx.flush()?;
            idx.mark_ready()?;
        }

        self.delete_journal(repo).await?;
        guard.release().await?;

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
        if CanonicalRepoName::parse(repo).is_err() {
            return Err(ManifestLifecycleError::InvalidRepoName);
        }
        if !is_valid_tag(tag) {
            return Err(ManifestLifecycleError::InvalidTag);
        }

        let mut guard = self.acquire_coordination(repo).await?;
        self.recover_and_ensure_index_healthy(repo).await?;

        // Verify target manifest exists
        match self.storage.head_manifest(repo, target_digest).await {
            Ok(_) => {}
            Err(StorageError::NotFound) => return Err(ManifestLifecycleError::ManifestNotFound),
            Err(e) => return Err(ManifestLifecycleError::Storage(e)),
        }

        guard.check_lease().await?;

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
            idx.flush()?;
            idx.mark_ready()?;
        }

        guard.release().await?;

        Ok(TagMutationResult {
            tag: tag.to_string(),
            digest: target_digest.clone(),
            mutation,
        })
    }
}
