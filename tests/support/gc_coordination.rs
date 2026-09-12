#![allow(dead_code, clippy::type_complexity, clippy::collapsible_if)]

use bytes::Bytes;
use futures_util::future::BoxFuture;
use registry_rust::config::*;
use registry_rust::registry::digest::Digest;
use registry_rust::storage::mutation_authority::GcMutationPermit;
use registry_rust::storage::repo_membership::*;
use registry_rust::storage::upload_session::*;
use registry_rust::storage::*;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncRead;

pub type AsyncHook<T> = Arc<dyn Fn(T) -> BoxFuture<'static, ()> + Send + Sync>;

#[derive(Default, Clone)]
pub struct StorageHooks {
    pub before_commit_blob: Option<AsyncHook<Digest>>,
    pub after_commit_blob: Option<AsyncHook<Digest>>,
    pub before_link_repo_blob: Option<AsyncHook<RepoBlobMembershipRecord>>,
    pub after_link_repo_blob: Option<AsyncHook<RepoBlobMembershipRecord>>,
    pub before_unlink_repo_blob: Option<AsyncHook<(String, Digest)>>,
    pub after_unlink_repo_blob: Option<AsyncHook<(String, Digest)>>,
    pub before_write_lifecycle_journal: Option<AsyncHook<String>>,
    pub after_write_lifecycle_journal: Option<AsyncHook<String>>,
    pub before_delete_lifecycle_journal: Option<AsyncHook<String>>,
    pub after_delete_lifecycle_journal: Option<AsyncHook<String>>,
    pub before_mutate_tag: Option<AsyncHook<(String, String, Digest)>>,
    pub after_mutate_tag: Option<AsyncHook<(String, String, Digest)>>,
    pub before_delete_blob_conditional: Option<AsyncHook<Digest>>,
    pub after_delete_blob_conditional: Option<AsyncHook<Digest>>,
    pub fail_link_repo_blob:
        Option<Arc<dyn Fn(&RepoBlobMembershipRecord) -> Option<StorageError> + Send + Sync>>,
    pub fail_commit_finalize:
        Option<Arc<dyn Fn(&PreparedFinalize) -> Option<UploadTransitionError> + Send + Sync>>,
    pub custom_commit_finalize: Option<
        Arc<
            dyn Fn(
                    Arc<dyn Storage>,
                    &PreparedFinalize,
                )
                    -> BoxFuture<'static, Result<FinalizeOutcome, UploadTransitionError>>
                + Send
                + Sync,
        >,
    >,
}

#[derive(Clone)]
pub struct HookedStorage {
    pub inner: Arc<dyn Storage>,
    pub hooks: StorageHooks,
}

impl HookedStorage {
    pub fn new(inner: Arc<dyn Storage>, hooks: StorageHooks) -> Self {
        Self { inner, hooks }
    }
}

#[async_trait::async_trait]
impl RepositoryBlobMembershipStorage for HookedStorage {
    async fn get_repo_blob_membership(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<Option<RepoBlobMembershipRecord>, StorageError> {
        self.inner.get_repo_blob_membership(repo, digest).await
    }

    async fn link_repo_blob(&self, record: &RepoBlobMembershipRecord) -> Result<(), StorageError> {
        if let Some(hook) = &self.hooks.before_link_repo_blob {
            hook(record.clone()).await;
        }
        if let Some(fail_fn) = &self.hooks.fail_link_repo_blob {
            if let Some(err) = fail_fn(record) {
                return Err(err);
            }
        }
        let res = self.inner.link_repo_blob(record).await;
        if res.is_ok() {
            if let Some(hook) = &self.hooks.after_link_repo_blob {
                hook(record.clone()).await;
            }
        }
        res
    }

    async fn set_membership_candidate(
        &self,
        repo: &str,
        digest: &Digest,
        since_unix_secs: u64,
    ) -> Result<bool, StorageError> {
        self.inner
            .set_membership_candidate(repo, digest, since_unix_secs)
            .await
    }

    async fn clear_membership_candidate(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<bool, StorageError> {
        self.inner.clear_membership_candidate(repo, digest).await
    }

    async fn unlink_repo_blob(&self, repo: &str, digest: &Digest) -> Result<bool, StorageError> {
        if let Some(hook) = &self.hooks.before_unlink_repo_blob {
            hook((repo.to_string(), digest.clone())).await;
        }
        let res = self.inner.unlink_repo_blob(repo, digest).await;
        if let Some(hook) = &self.hooks.after_unlink_repo_blob {
            hook((repo.to_string(), digest.clone())).await;
        }
        res
    }

    async fn list_repo_blob_memberships_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<RepoBlobMembershipRecord>, Option<String>), StorageError> {
        self.inner
            .list_repo_blob_memberships_page(repo, continuation_token, page_limit)
            .await
    }

    async fn list_all_repo_blob_memberships_page(
        &self,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<RepoBlobMembershipRecord>, Option<String>), StorageError> {
        self.inner
            .list_all_repo_blob_memberships_page(continuation_token, page_limit)
            .await
    }

    async fn count_repo_blob_memberships(&self, digest: &Digest) -> Result<usize, StorageError> {
        self.inner.count_repo_blob_memberships(digest).await
    }
}

#[async_trait::async_trait]
impl UploadSessionStorage for HookedStorage {
    async fn create_session(&self, repo: &str) -> Result<UploadSessionId, StorageError> {
        self.inner.create_session(repo).await
    }

    async fn session_status(
        &self,
        session: &UploadSessionId,
    ) -> Result<UploadSessionStatus, UploadTransitionError> {
        self.inner.session_status(session).await
    }

    async fn append_if_offset(
        &self,
        session: &UploadSessionId,
        expected_offset: UploadOffsetPrecondition,
        stream: UploadByteStream,
        max_upload_bytes: u64,
    ) -> Result<UploadAppendResult, UploadTransitionError> {
        self.inner
            .append_if_offset(session, expected_offset, stream, max_upload_bytes)
            .await
    }

    async fn begin_finalize(
        &self,
        session: &UploadSessionId,
        expected_offset: UploadOffsetPrecondition,
        trailing_stream: Option<UploadByteStream>,
        expected_digest: &Digest,
        max_upload_bytes: u64,
        abort_on_digest_mismatch: bool,
    ) -> Result<PreparedFinalize, UploadTransitionError> {
        self.inner
            .begin_finalize(
                session,
                expected_offset,
                trailing_stream,
                expected_digest,
                max_upload_bytes,
                abort_on_digest_mismatch,
            )
            .await
    }

    async fn commit_finalize(
        &self,
        prepared: &PreparedFinalize,
    ) -> Result<FinalizeOutcome, UploadTransitionError> {
        if let Some(custom) = &self.hooks.custom_commit_finalize {
            return custom(self.inner.clone(), prepared).await;
        }
        if let Some(fail_fn) = &self.hooks.fail_commit_finalize {
            if let Some(err) = fail_fn(prepared) {
                return Err(err);
            }
        }
        if let Some(hook) = &self.hooks.before_commit_blob {
            hook(prepared.expected_digest.clone()).await;
        }
        let res = self.inner.commit_finalize(prepared).await;
        if let Some(hook) = &self.hooks.after_commit_blob {
            hook(prepared.expected_digest.clone()).await;
        }
        res
    }

    async fn abort_session(&self, session: &UploadSessionId) -> Result<(), StorageError> {
        self.inner.abort_session(session).await
    }

    async fn recover_session(
        &self,
        session: &UploadSessionId,
    ) -> Result<UploadSessionStatus, UploadTransitionError> {
        self.inner.recover_session(session).await
    }

    async fn get_finalized_receipt(
        &self,
        session: &UploadSessionId,
    ) -> Result<Option<FinalizedReceipt>, StorageError> {
        self.inner.get_finalized_receipt(session).await
    }
}

#[async_trait::async_trait]
impl GcStorage for HookedStorage {
    fn gc_strategy(&self) -> GcStorageStrategy {
        GcStorage::gc_strategy(&self.inner)
    }

    async fn check_bucket_versioning_for_gc(&self) -> Result<(), StorageError> {
        GcStorage::check_bucket_versioning_for_gc(&self.inner).await
    }

    async fn list_cas_blobs_page(
        &self,
        cursor: Option<&GcCursor>,
        limit: usize,
    ) -> Result<GcBlobPage, StorageError> {
        GcStorage::list_cas_blobs_page(&self.inner, cursor, limit).await
    }

    async fn quarantine_blob(
        &self,
        permit: &GcMutationPermit<'_>,
        digest: &Digest,
        version: &BlobObjectVersion,
    ) -> Result<GcQuarantineResult, StorageError> {
        GcStorage::quarantine_blob(&self.inner, permit, digest, version).await
    }

    async fn restore_quarantined_blob(
        &self,
        permit: &GcMutationPermit<'_>,
        digest: &Digest,
    ) -> Result<Option<u64>, StorageError> {
        GcStorage::restore_quarantined_blob(&self.inner, permit, digest).await
    }

    async fn quarantined_blob_version(
        &self,
        digest: &Digest,
    ) -> Result<Option<BlobObjectVersion>, StorageError> {
        GcStorage::quarantined_blob_version(&self.inner, digest).await
    }

    async fn delete_blob_conditional(
        &self,
        permit: &GcMutationPermit<'_>,
        digest: &Digest,
        version: Option<&BlobObjectVersion>,
    ) -> Result<GcDeleteResult, StorageError> {
        if let Some(hook) = &self.hooks.before_delete_blob_conditional {
            hook(digest.clone()).await;
        }
        let res = GcStorage::delete_blob_conditional(&self.inner, permit, digest, version).await;
        if let Some(hook) = &self.hooks.after_delete_blob_conditional {
            hook(digest.clone()).await;
        }
        res
    }
}

#[async_trait::async_trait]
impl Storage for HookedStorage {
    fn kind(&self) -> &'static str {
        "hooked-storage"
    }

    async fn list_repositories(&self) -> Result<Vec<String>, StorageError> {
        Storage::list_repositories(&self.inner).await
    }

    async fn repo_timestamps(&self, name: &str) -> Result<RepoTimestamps, StorageError> {
        Storage::repo_timestamps(&self.inner, name).await
    }

    async fn is_storage_empty(&self) -> Result<bool, StorageError> {
        Storage::is_storage_empty(&self.inner).await
    }

    async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError> {
        Storage::head_blob(&self.inner, digest).await
    }

    async fn open_blob(
        &self,
        digest: &Digest,
    ) -> Result<(BlobMeta, Pin<Box<dyn AsyncRead + Send>>), StorageError> {
        Storage::open_blob(&self.inner, digest).await
    }

    async fn resolve_tag(&self, name: &str, tag: &str) -> Result<Digest, StorageError> {
        Storage::resolve_tag(&self.inner, name, tag).await
    }

    async fn list_tags(&self, name: &str) -> Result<Vec<String>, StorageError> {
        Storage::list_tags(&self.inner, name).await
    }

    async fn head_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<ManifestMeta, StorageError> {
        Storage::head_manifest(&self.inner, name, digest).await
    }

    async fn get_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<(ManifestMeta, Bytes), StorageError> {
        Storage::get_manifest(&self.inner, name, digest).await
    }

    async fn put_manifest(
        &self,
        name: &str,
        digest: &Digest,
        bytes: Bytes,
    ) -> Result<ManifestMeta, StorageError> {
        Storage::put_manifest(&self.inner, name, digest, bytes).await
    }

    async fn set_tag(&self, name: &str, tag: &str, digest: &Digest) -> Result<(), StorageError> {
        Storage::set_tag(&self.inner, name, tag, digest).await
    }

    async fn mutate_tag(
        &self,
        name: &str,
        tag: &str,
        digest: &Digest,
        policy: TagMutationPolicy,
    ) -> Result<TagMutation, StorageError> {
        if let Some(hook) = &self.hooks.before_mutate_tag {
            hook((name.to_string(), tag.to_string(), digest.clone())).await;
        }
        let res = Storage::mutate_tag(&self.inner, name, tag, digest, policy).await;
        if let Some(hook) = &self.hooks.after_mutate_tag {
            hook((name.to_string(), tag.to_string(), digest.clone())).await;
        }
        res
    }

    async fn delete_tag(&self, name: &str, tag: &str) -> Result<(), StorageError> {
        Storage::delete_tag(&self.inner, name, tag).await
    }

    async fn list_manifest_digests_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<Digest>, Option<String>), StorageError> {
        Storage::list_manifest_digests_page(&self.inner, repo, continuation_token, page_limit).await
    }

    async fn list_tags_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<(String, Digest)>, Option<String>), StorageError> {
        Storage::list_tags_page(&self.inner, repo, continuation_token, page_limit).await
    }

    async fn list_referrers_page(
        &self,
        repo: &str,
        subject: &Digest,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<ReferrerDescriptor>, Option<String>), StorageError> {
        Storage::list_referrers_page(&self.inner, repo, subject, continuation_token, page_limit)
            .await
    }

    async fn get_tag_with_version(
        &self,
        repo: &str,
        tag: &str,
    ) -> Result<Option<(Digest, String)>, StorageError> {
        Storage::get_tag_with_version(&self.inner, repo, tag).await
    }

    async fn delete_tag_conditional(
        &self,
        repo: &str,
        tag: &str,
        expected_version: Option<&str>,
    ) -> Result<ConditionalDeleteResult, StorageError> {
        Storage::delete_tag_conditional(&self.inner, repo, tag, expected_version).await
    }

    async fn read_lifecycle_journal(&self, repo: &str) -> Result<Option<Bytes>, StorageError> {
        Storage::read_lifecycle_journal(&self.inner, repo).await
    }

    async fn write_lifecycle_journal(&self, repo: &str, data: Bytes) -> Result<(), StorageError> {
        if let Some(hook) = &self.hooks.before_write_lifecycle_journal {
            hook(repo.to_string()).await;
        }
        let res = Storage::write_lifecycle_journal(&self.inner, repo, data).await;
        if let Some(hook) = &self.hooks.after_write_lifecycle_journal {
            hook(repo.to_string()).await;
        }
        res
    }

    async fn delete_lifecycle_journal(&self, repo: &str) -> Result<(), StorageError> {
        if let Some(hook) = &self.hooks.before_delete_lifecycle_journal {
            hook(repo.to_string()).await;
        }
        let res = Storage::delete_lifecycle_journal(&self.inner, repo).await;
        if let Some(hook) = &self.hooks.after_delete_lifecycle_journal {
            hook(repo.to_string()).await;
        }
        res
    }

    async fn acquire_repo_lease(
        &self,
        repo: &str,
        owner_id: &str,
        lease_id: &str,
        ttl_secs: u64,
    ) -> Result<bool, StorageError> {
        Storage::acquire_repo_lease(&self.inner, repo, owner_id, lease_id, ttl_secs).await
    }

    async fn renew_repo_lease(
        &self,
        repo: &str,
        owner_id: &str,
        lease_id: &str,
        ttl_secs: u64,
    ) -> Result<bool, StorageError> {
        Storage::renew_repo_lease(&self.inner, repo, owner_id, lease_id, ttl_secs).await
    }

    async fn release_repo_lease(
        &self,
        repo: &str,
        owner_id: &str,
        lease_id: &str,
    ) -> Result<(), StorageError> {
        Storage::release_repo_lease(&self.inner, repo, owner_id, lease_id).await
    }

    async fn create_upload(&self) -> Result<UploadMeta, StorageError> {
        Storage::create_upload(&self.inner).await
    }

    async fn upload_status(&self, uuid: &str) -> Result<UploadMeta, StorageError> {
        Storage::upload_status(&self.inner, uuid).await
    }

    async fn append_upload(&self, uuid: &str, chunk: Bytes) -> Result<UploadMeta, StorageError> {
        Storage::append_upload(&self.inner, uuid, chunk).await
    }

    async fn finalize_upload(&self, uuid: &str, digest: &Digest) -> Result<BlobMeta, StorageError> {
        Storage::finalize_upload(&self.inner, uuid, digest).await
    }

    async fn abort_upload(&self, uuid: &str) -> Result<(), StorageError> {
        Storage::abort_upload(&self.inner, uuid).await
    }

    async fn list_referrers(
        &self,
        name: &str,
        subject: &Digest,
    ) -> Result<Vec<ReferrerDescriptor>, StorageError> {
        Storage::list_referrers(&self.inner, name, subject).await
    }

    async fn add_referrer(
        &self,
        name: &str,
        subject: &Digest,
        descriptor: ReferrerDescriptor,
    ) -> Result<(), StorageError> {
        Storage::add_referrer(&self.inner, name, subject, descriptor).await
    }

    async fn remove_referrer(
        &self,
        name: &str,
        subject: &Digest,
        referrer: &Digest,
    ) -> Result<(), StorageError> {
        Storage::remove_referrer(&self.inner, name, subject, referrer).await
    }

    async fn delete_manifest(&self, name: &str, digest: &Digest) -> Result<(), StorageError> {
        Storage::delete_manifest(&self.inner, name, digest).await
    }
}

registry_rust::impl_storage_ports!(HookedStorage);
registry_rust::impl_gc_storage_port!(HookedStorage);

#[allow(dead_code)]
pub fn tmp_dir(prefix: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("registry-rust-{prefix}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&p).expect("create temp dir");
    p
}

pub fn test_config(fs_root: PathBuf, ref_index_path: PathBuf) -> Config {
    Config {
        listen_addr: SocketAddr::from(([127, 0, 0, 1], 5000)),
        tls_cert_path: None,
        tls_key_path: None,
        tls_acme: None,
        push_username: None,
        push_password: None,
        push_allow_repos: None,
        push_actions: vec!["pull".to_string(), "push".to_string()],
        push_implies_delete: false,
        auth_strategy: AuthStrategy::Token,
        anonymous_pull: true,
        storage_backend: StorageBackend::Filesystem,
        fs_root,
        fs_manifest_listing_max_entries: 10_000,
        fs_manifest_listing_max_name_bytes: 1_500_000,
        s3_endpoint: None,
        s3_region: None,
        s3_bucket: None,
        s3_prefix: "registry".to_string(),
        s3_single_instance_mode: false,
        s3_lease_duration_secs: 300,
        s3_lease_renewal_interval_secs: 60,
        s3_max_retry_attempts: 3,
        s3_legacy_multipart_cleanup_policy: Default::default(),
        upload_receipt_lifetime_secs: 72 * 3600,
        gc_pin_duration_secs: 3600,
        ref_index: RefIndexConfig {
            enabled: true,
            path: ref_index_path,
            rebuild_on_start: false,
            auto_rebuild_on_corruption: true,
        },
        allow_tag_overwrite: true,
        automatic_crossmount: false,
        upload_gc_enabled: false,
        upload_gc_interval_secs: 3600,
        upload_gc_max_age_secs: 86400,
        blob_gc_finalize_grace_secs: 72 * 3600,
        blob_gc_enabled: true,
        blob_gc_enable_delete: true,
        blob_gc_default_min_age_secs: 7 * 24 * 3600,
        blob_gc_default_quarantine_delay_secs: 24 * 3600,
        blob_gc_default_max_blobs: 1000,
        blob_gc_default_max_bytes: u64::MAX,
        blob_gc_default_max_seconds: 60,
        blob_gc_schedule_enabled: false,
        blob_gc_schedule_interval_secs: 7 * 24 * 3600,
        admin_api: AdminApiConfig {
            enabled: false,
            username: None,
            password: None,
        },
        max_upload_bytes: 5 * 1024 * 1024,
        max_request_body_bytes: 1024 * 1024,
        upload_chunk_min_bytes: None,
        max_concurrent_buffered_requests: 1,
        max_concurrent_requests: 1,
        max_concurrent_upload_requests: 1,
        request_timeout_secs: 60,
        upload_request_timeout_secs: 60,
        upload_chunk_idle_timeout_secs: 20,
        upload_rate_window_secs: 10,
        upload_rate_grace_period_secs: 15,
        min_upload_bytes_per_sec: 32768,
        header_read_timeout_secs: 10,
        slow_connection_policy: SlowConnectionPolicy::Enforce,
        max_connections_per_ip: 50,
        trusted_bypass_cidrs: vec![],
        trusted_proxies: vec![],
        disallow_monolithic_uploads: false,
        upload_policy: UploadPolicyConfig {
            abort_on_error: false,
            abort_on_digest_mismatch: false,
            repo_rules: vec![],
        },
        catalog_requires_auth: false,
        public_url: None,
        token_service: "registry-rust".to_string(),
        token_signing_key: "test".to_string(),
        token_signing_keys: vec![registry_rust::security::TokenSigningKey {
            kid: "default".to_string(),
            key: "test".to_string(),
        }],
        token_ttl_secs: 600,
        robots: RobotsConfig::default(),
        users: UsersConfig::default(),
        proxy: ProxyConfig {
            enabled: false,
            mode: ProxyMode::Allowlist,
            upstream_base_url: None,
            upstream_username: None,
            upstream_password: None,
            allowed_upstream_hosts: vec![],
            allowed_repo_prefixes: vec![],
            block_private_networks: true,
            redirect_policy: RedirectPolicy::AnyPublic,
            max_concurrent_upstream: 1,
            index_path: PathBuf::from("./data/proxy-index"),
            cache_fs_root: None,
            cache_s3_prefix: None,
            gc_interval_secs: 3600,
            scrub_enabled: false,
            scrub_interval_secs: 3600,
            scrub_max_files_per_run: 1,
            max_cache_bytes: None,
            repo_rules: vec![],
            upstreams: vec![],
            routing_proxy_hosts: vec![],
            routing_trust_x_forwarded_host: false,
        },
    }
}

#[allow(dead_code)]
pub async fn write_live_blob(fs_root: &std::path::Path, digest: &Digest, bytes: &[u8]) {
    let dir = fs_root.join("blobs").join("sha256").join(digest.prefix2());
    tokio::fs::create_dir_all(&dir).await.expect("mkdir");
    tokio::fs::write(dir.join(digest.hex()), bytes)
        .await
        .expect("write blob");
}
