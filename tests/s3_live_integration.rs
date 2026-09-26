use bytes::Bytes;
use sha2::Digest as _;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio::sync::{Barrier, Mutex, oneshot};
use url::Url;
use uuid::Uuid;

use registry_rust::blob_gc::{BlobGcPolicy, CasBlobTraverser};
use registry_rust::blob_ref_index::BlobRefIndex;
use registry_rust::config::Config;
use registry_rust::consistency::ConsistencyCoordinator;
use registry_rust::gc_service::{GcBudgets, GcService, GcServiceError};
use registry_rust::manifest_lifecycle::{
    LifecycleJournalRecord, LifecycleOpKind, LifecyclePhase, ManifestLifecycleService,
    PublishManifestRequest,
};
use registry_rust::registry::digest::Digest;
use registry_rust::storage::mutation_authority::{
    RuntimeMutationAuthority, admin_clear_abandoned_deployment_writer_lock,
    inspect_deployment_writer_lock,
};
use registry_rust::storage::repo_membership::{
    RepoBlobMembershipRecord, RepositoryBlobMembershipStorage,
};
use registry_rust::storage::s3::S3Storage;
use registry_rust::storage::upload_session::{
    FinalizeOutcome, UploadAppendResult, UploadOffsetPrecondition, UploadSessionState,
    UploadSessionStorage,
};
use registry_rust::storage::{
    ConditionalDeleteResult, GcDeleteResult, GcStorage, ReferrerDescriptor, Storage, StorageError,
    TagMutation, TagMutationPolicy,
};
use registry_rust::supervisor::{SupervisorOptions, run_server_supervisor};
use registry_rust::upload_coordinator::{
    BlobUploadCoordinator, BlobUploadCoordinatorConfig, CrossMountResult,
};

// ================================================================================================
// LIVE S3 TEST HARNESS & SAFETY GUARDRAILS
// ================================================================================================

#[derive(Clone, Debug)]
pub struct LiveS3SafetyPolicy {
    pub allow_non_local_destructive_tests: bool,
    pub expected_account_id: Option<String>,
}

impl LiveS3SafetyPolicy {
    pub fn from_env() -> Self {
        let allow = std::env::var("ALLOW_NON_LOCAL_S3_DESTRUCTIVE_TESTS").as_deref() == Ok("1");
        let expected_account_id = std::env::var("TEST_S3_EXPECTED_ACCOUNT_ID")
            .ok()
            .or_else(|| std::env::var("EXPECTED_AWS_ACCOUNT_ID").ok());
        Self {
            allow_non_local_destructive_tests: allow,
            expected_account_id,
        }
    }

    pub fn validate_endpoint(&self, endpoint: &str) -> Result<(), String> {
        let url = Url::parse(endpoint)
            .map_err(|e| format!("invalid S3 endpoint URL '{endpoint}': {e}"))?;
        let host = url.host_str().unwrap_or("");
        let is_local = host == "127.0.0.1"
            || host == "localhost"
            || host == "::1"
            || host == "0.0.0.0"
            || host.ends_with(".localhost");

        if !is_local && !self.allow_non_local_destructive_tests {
            return Err(format!(
                "Refusing to run live S3 contract tests against non-local endpoint '{endpoint}'. Set ALLOW_NON_LOCAL_S3_DESTRUCTIVE_TESTS=1 to override."
            ));
        }
        Ok(())
    }
}

pub struct LiveS3Harness {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub prefix: String,
    pub policy: LiveS3SafetyPolicy,
}

impl LiveS3Harness {
    pub async fn new() -> Result<Self, String> {
        Self::with_policy(LiveS3SafetyPolicy::from_env()).await
    }

    pub async fn with_policy(policy: LiveS3SafetyPolicy) -> Result<Self, String> {
        let endpoint = std::env::var("TEST_S3_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
        let region = std::env::var("TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let bucket =
            std::env::var("TEST_S3_BUCKET").unwrap_or_else(|_| "registry-live-test".to_string());

        // Guardrail 1: Local vs non-local endpoint validation
        policy.validate_endpoint(&endpoint)?;

        // If non-local (real AWS), verify STS caller identity before proceeding
        let url = Url::parse(&endpoint).map_err(|e| e.to_string())?;
        let host = url.host_str().unwrap_or("");
        let is_local = host == "127.0.0.1"
            || host == "localhost"
            || host == "::1"
            || host == "0.0.0.0"
            || host.ends_with(".localhost");

        if !is_local {
            let output = tokio::process::Command::new("aws")
                .args(["sts", "get-caller-identity", "--output", "json"])
                .output()
                .await
                .map_err(|e| format!("failed to invoke aws sts get-caller-identity: {e}"))?;

            if !output.status.success() {
                return Err(format!(
                    "aws sts get-caller-identity failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }

            let ident: serde_json::Value = serde_json::from_slice(&output.stdout)
                .map_err(|e| format!("failed to parse STS identity: {e}"))?;
            let account = ident["Account"].as_str().unwrap_or("");
            if let Some(ref expected) = policy.expected_account_id
                && account != expected
            {
                return Err(format!(
                    "SAFETY ABORT: AWS Account ID '{account}' does NOT match required account '{expected}'"
                ));
            }
        }

        // Guardrail 2: Cryptographically unique run prefix
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let prefix = format!("live-test-{}-{}/", Uuid::new_v4(), now_secs);

        // Guardrail 3: Safe logging - never log secret keys!
        println!(
            "LIVE S3 CONTRACT: endpoint={}, region={}, bucket={}, prefix={}",
            endpoint, region, bucket, prefix
        );

        // Ensure bucket exists on live server
        Self::ensure_bucket_exists(&endpoint, &region, &bucket).await?;

        Ok(Self {
            endpoint,
            region,
            bucket,
            prefix,
            policy,
        })
    }

    async fn ensure_bucket_exists(
        endpoint: &str,
        region: &str,
        bucket: &str,
    ) -> Result<(), String> {
        let url = Url::parse(endpoint).map_err(|e| e.to_string())?;
        let host = url.host_str().unwrap_or("");
        let is_local = host == "127.0.0.1"
            || host == "localhost"
            || host == "::1"
            || host == "0.0.0.0"
            || host.ends_with(".localhost");

        let loader = aws_config::defaults(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()));
        let loader = if is_local && std::env::var("AWS_ACCESS_KEY_ID").is_err() {
            loader.credentials_provider(aws_sdk_s3::config::Credentials::new(
                "minioadmin",
                "minioadmin",
                None,
                None,
                "static",
            ))
        } else {
            loader
        };
        let shared = loader.load().await;
        let mut builder = aws_sdk_s3::config::Builder::from(&shared);
        if is_local {
            builder = builder.endpoint_url(endpoint).force_path_style(true);
        } else if !endpoint.is_empty() && endpoint != "https://s3.amazonaws.com" {
            builder = builder.endpoint_url(endpoint);
        }
        let client = aws_sdk_s3::Client::from_conf(builder.build());

        match client.create_bucket().bucket(bucket).send().await {
            Ok(_) => Ok(()),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("BucketAlreadyOwnedByYou")
                    || err_str.contains("BucketAlreadyExists")
                {
                    Ok(())
                } else {
                    // Check if bucket exists via head_bucket
                    match client.head_bucket().bucket(bucket).send().await {
                        Ok(_) => Ok(()),
                        Err(_) => Err(format!(
                            "Failed to ensure test bucket '{bucket}' exists at '{endpoint}': {e}"
                        )),
                    }
                }
            }
        }
    }

    /// Constructs a fresh production S3Storage instance connected to the live service.
    pub fn create_storage(&self) -> Arc<S3Storage> {
        Arc::new(S3Storage::new(
            Some(self.endpoint.clone()),
            Some(self.region.clone()),
            Some(self.bucket.clone()),
            self.prefix.clone(),
            50 * 1024 * 1024,
        ))
    }

    /// Constructs a production Config instance for the supervisor pointing to live S3.
    pub fn create_server_config(&self, temp_dir: &TempDir) -> Config {
        let cfg_path = temp_dir.path().join("config.toml");
        let ref_index = temp_dir.path().join("ref_index.db");
        let toml = format!(
            r#"
[server]
listen_addr = "127.0.0.1:0"

[storage]
backend = "s3"

[storage.s3]
endpoint = "{}"
region = "{}"
bucket = "{}"
prefix = "{}"
single_instance_mode = true

[storage.ref_index]
enabled = true
path = "{}"

[token]
signing_key = "test-secret-key-12345678901234567890"
"#,
            self.endpoint,
            self.region,
            self.bucket,
            self.prefix,
            ref_index.display()
        );
        std::fs::write(&cfg_path, toml).unwrap();
        Config::from_env_with_files(std::slice::from_ref(&cfg_path)).unwrap()
    }

    /// Guardrail 4: Validates cleanup target and purges all objects strictly under this run's prefix.
    pub async fn cleanup(&self) {
        assert!(
            self.prefix.starts_with("live-test-"),
            "CRITICAL GUARDRAIL: Refusing to delete prefix '{}' not starting with 'live-test-'",
            self.prefix
        );
        assert!(
            self.prefix.len() > 15,
            "CRITICAL GUARDRAIL: Prefix '{}' is suspiciously short",
            self.prefix
        );

        let url = Url::parse(&self.endpoint)
            .unwrap_or_else(|_| Url::parse("http://127.0.0.1:9000").unwrap());
        let host = url.host_str().unwrap_or("");
        let is_local = host == "127.0.0.1"
            || host == "localhost"
            || host == "::1"
            || host == "0.0.0.0"
            || host.ends_with(".localhost");

        let loader = aws_config::defaults(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_config::Region::new(self.region.clone()));
        let loader = if is_local && std::env::var("AWS_ACCESS_KEY_ID").is_err() {
            loader.credentials_provider(aws_sdk_s3::config::Credentials::new(
                "minioadmin",
                "minioadmin",
                None,
                None,
                "static",
            ))
        } else {
            loader
        };
        let shared = loader.load().await;
        let config = aws_sdk_s3::config::Builder::from(&shared)
            .endpoint_url(&self.endpoint)
            .force_path_style(true)
            .build();
        let client = aws_sdk_s3::Client::from_conf(config);

        let mut token = None;
        let mut count = 0;
        loop {
            let mut req = client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&self.prefix);
            if let Some(t) = token.as_deref() {
                req = req.continuation_token(t);
            }
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    eprintln!(
                        "LIVE S3 CLEANUP ERROR listing objects under prefix '{}': {e}",
                        self.prefix
                    );
                    break;
                }
            };

            for obj in resp.contents() {
                if let Some(k) = obj.key() {
                    let _ = client
                        .delete_object()
                        .bucket(&self.bucket)
                        .key(k)
                        .send()
                        .await;
                    count += 1;
                }
            }

            if resp.is_truncated().unwrap_or(false) {
                token = resp.next_continuation_token().map(|s| s.to_string());
                if token.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        println!(
            "LIVE S3 CLEANUP: Purged {} objects under prefix '{}'",
            count, self.prefix
        );

        // Assert post-cleanup absence
        let post_check = client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&self.prefix)
            .send()
            .await
            .expect("verify prefix is absent");
        assert_eq!(
            post_check.key_count().unwrap_or(0),
            0,
            "Expected 0 objects remaining under prefix {}",
            self.prefix
        );
        println!(
            "LIVE S3 CLEANUP PROOF: Verified 0 objects remaining under prefix '{}'",
            self.prefix
        );
    }
}

fn sha256_digest(bytes: &[u8]) -> Digest {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let hex = hex::encode(hasher.finalize());
    Digest::parse(&format!("sha256:{hex}")).expect("valid sha256 digest")
}

async fn read_blob_bytes(storage: &(impl Storage + ?Sized), digest: &Digest) -> Bytes {
    use tokio::io::AsyncReadExt;
    let (_meta, mut reader) = storage.open_blob(digest).await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    Bytes::from(buf)
}

async fn write_test_blob(storage: &(impl Storage + ?Sized), repo: &str, content: &[u8]) -> Digest {
    let digest = sha256_digest(content);
    let upload = storage.create_upload().await.unwrap();
    storage
        .append_upload(&upload.uuid, Bytes::copy_from_slice(content))
        .await
        .unwrap();
    storage
        .finalize_upload(&upload.uuid, &digest)
        .await
        .unwrap();
    let canonical_repo =
        registry_rust::registry::canonical_name::CanonicalRepoName::parse(repo).unwrap();
    let rec =
        RepoBlobMembershipRecord::new_upload(canonical_repo, digest.clone(), Some(upload.uuid));
    storage.link_repo_blob(&rec).await.unwrap();
    digest
}

// ================================================================================================
// 1. LIVE TEST ENVIRONMENT & SAFETY GUARDRAILS
// ================================================================================================

#[tokio::test]
async fn test_safety_guardrails_reject_non_local_without_override() {
    let local_policy = LiveS3SafetyPolicy {
        allow_non_local_destructive_tests: false,
        expected_account_id: Some("123456789012".to_string()),
    };
    assert!(
        local_policy
            .validate_endpoint("http://127.0.0.1:9000")
            .is_ok()
    );
    assert!(
        local_policy
            .validate_endpoint("http://localhost:9000")
            .is_ok()
    );
    assert!(
        local_policy
            .validate_endpoint("http://s3.localhost:9000")
            .is_ok()
    );

    let res = local_policy.validate_endpoint("https://s3.amazonaws.com");
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .contains("ALLOW_NON_LOCAL_S3_DESTRUCTIVE_TESTS")
    );

    let allowed_policy = LiveS3SafetyPolicy {
        allow_non_local_destructive_tests: true,
        expected_account_id: Some("123456789012".to_string()),
    };
    assert!(
        allowed_policy
            .validate_endpoint("https://s3.amazonaws.com")
            .is_ok()
    );
}

#[tokio::test]
async fn test_safety_guardrails_generate_unique_prefix_and_isolate_runs() {
    let h1 = LiveS3Harness::new().await.unwrap();
    let h2 = LiveS3Harness::new().await.unwrap();

    assert_ne!(h1.prefix, h2.prefix);
    assert!(h1.prefix.starts_with("live-test-"));
    assert!(h2.prefix.starts_with("live-test-"));

    let s1 = h1.create_storage();
    let s2 = h2.create_storage();

    let digest = sha256_digest(b"isolation test blob content");
    let upload1 = s1.create_upload().await.unwrap();
    s1.append_upload(
        &upload1.uuid,
        Bytes::from_static(b"isolation test blob content"),
    )
    .await
    .unwrap();
    s1.finalize_upload(&upload1.uuid, &digest).await.unwrap();

    assert!(s1.open_blob(&digest).await.is_ok());
    assert!(s2.open_blob(&digest).await.is_err());

    h1.cleanup().await;
    h2.cleanup().await;
}

// ================================================================================================
// 2. PRODUCTION S3STORAGE ADAPTER CONSTRUCTION
// ================================================================================================

#[tokio::test]
async fn test_production_s3_storage_adapter_construction() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();

    assert_eq!(storage.kind(), "s3");
    let repos = storage.list_repositories().await.unwrap();
    assert!(repos.is_empty());

    harness.cleanup().await;
}

// ================================================================================================
// 3. DEPLOYMENT WRITER-LOCK CONTRACT
// ================================================================================================

#[tokio::test]
async fn test_live_s3_writer_lock_1_to_6_full_lifecycle() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage1 = harness.create_storage();
    let storage2 = harness.create_storage();

    // 1. Initial acquisition creates lock with If-None-Match: *
    let mut auth1 = RuntimeMutationAuthority::acquire(storage1.clone(), "server-1")
        .await
        .unwrap();
    assert!(auth1.is_active());
    let doc1 = auth1.doc().clone();

    // 2. Second client receives precondition response and maps to ExclusiveWriterLocked
    let auth2_res = RuntimeMutationAuthority::acquire(storage2.clone(), "server-2").await;
    assert!(
        matches!(auth2_res, Err(StorageError::ExclusiveWriterLocked(_))),
        "second writer must fail closed with ExclusiveWriterLocked"
    );

    // 3. Lock inspection returns stored owner and actual ETag/version
    let (inspect_doc, inspect_etag) = inspect_deployment_writer_lock(&storage2)
        .await
        .unwrap()
        .expect("lock must exist");
    assert_eq!(inspect_doc.owner_id, doc1.owner_id);
    assert_eq!(inspect_doc.owner_token, doc1.owner_token);
    let real_etag = inspect_etag.expect("etag must be present");
    assert!(!real_etag.is_empty());

    // 4. Release with incorrect owner fails
    let mut fake_doc = doc1.clone();
    fake_doc.owner_id = "impostor-owner".to_string();
    fake_doc.owner_token = "impostor-token".to_string();
    let release_bad_owner = storage1
        .release_deployment_writer_lock(&fake_doc, Some(&real_etag))
        .await
        .unwrap();
    assert!(!release_bad_owner, "release with bad owner must fail");

    // 5. Release with stale ETag fails
    let release_stale_etag = storage1
        .release_deployment_writer_lock(&doc1, Some("stale-etag-99999"))
        .await
        .unwrap();
    assert!(!release_stale_etag, "release with stale etag must fail");

    // 6. Conditional release of current generation succeeds
    auth1.release().await.unwrap();
    assert!(!auth1.is_active());

    let inspect_after = inspect_deployment_writer_lock(&storage2).await.unwrap();
    assert!(inspect_after.is_none(), "lock must be clear after release");

    harness.cleanup().await;
}

#[tokio::test]
async fn test_live_s3_writer_lock_7_to_10_admin_recovery_and_supervisor_competition() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage1 = harness.create_storage();
    let storage2 = harness.create_storage();

    // 7. Abandoned lock from crashed server-1
    let auth1 = RuntimeMutationAuthority::acquire(storage1.clone(), "server-1")
        .await
        .unwrap();
    let (doc1, etag1) = inspect_deployment_writer_lock(&storage1)
        .await
        .unwrap()
        .unwrap();
    let etag1_str = etag1.unwrap();
    std::mem::forget(auth1); // Abandoned without graceful release

    // 8. Stale admin clear with mismatched owner or etag fails
    let stale_clear = admin_clear_abandoned_deployment_writer_lock(
        &storage2,
        &doc1.owner_id,
        "mismatched-etag",
        "CONFIRM-CLEAR-ABANDONED-WRITER",
    )
    .await;
    assert!(stale_clear.is_err());

    // 9. Abandoned lock prevents new supervisor from starting
    let temp1 = TempDir::new().unwrap();
    let cfg1 = Arc::new(harness.create_server_config(&temp1));
    let start_res = run_server_supervisor(cfg1, Some(SupervisorOptions::default())).await;
    assert!(
        start_res.is_err(),
        "supervisor must fail closed when abandoned lock is present"
    );

    // Explicit conditional administrative recovery succeeds
    admin_clear_abandoned_deployment_writer_lock(
        &storage2,
        &doc1.owner_id,
        &etag1_str,
        "CONFIRM-CLEAR-ABANDONED-WRITER",
    )
    .await
    .unwrap();

    let inspect_cleared = inspect_deployment_writer_lock(&storage2).await.unwrap();
    assert!(inspect_cleared.is_none());

    // 10. Two supervisors cannot become active writers simultaneously
    let temp2 = TempDir::new().unwrap();
    let cfg2 = Arc::new(harness.create_server_config(&temp2));

    let (shutdown_tx1, shutdown_rx1) = oneshot::channel();
    let (bound_tx1, bound_rx1) = oneshot::channel();

    let options1 = SupervisorOptions {
        fault_injector: None,
        shutdown_rx: Some(shutdown_rx1),
        notify_bound_addr: Some(bound_tx1),
    };

    let server1 = tokio::spawn(run_server_supervisor(cfg2.clone(), Some(options1)));
    let _addr1 = bound_rx1.await.unwrap();

    // Server 2 attempts startup while Server 1 is active -> must fail closed
    let options2 = SupervisorOptions::default();
    let res2 = run_server_supervisor(cfg2, Some(options2)).await;
    assert!(
        res2.is_err(),
        "second supervisor must fail closed while first is active"
    );

    let _ = shutdown_tx1.send(());
    let res1 = server1.await.unwrap();
    assert!(res1.is_ok());

    harness.cleanup().await;
}

// ================================================================================================
// 4. TAG CAS CONTRACT
// ================================================================================================

#[tokio::test]
async fn test_live_s3_tag_cas_full_matrix() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();
    let repo = "library/cas-test";

    let d1 = sha256_digest(b"manifest 1");
    let d2 = sha256_digest(b"manifest 2");
    let d3 = sha256_digest(b"manifest 3");

    // 1. Two concurrent CreateOnly mutations on absent tag produce exactly one success
    let barrier = Arc::new(Barrier::new(2));
    let b1 = barrier.clone();
    let b2 = barrier.clone();
    let s1 = storage.clone();
    let s2 = storage.clone();
    let d1_clone = d1.clone();
    let d2_clone = d2.clone();

    let t1 = tokio::spawn(async move {
        b1.wait().await;
        s1.mutate_tag(repo, "v1", &d1_clone, TagMutationPolicy::CreateOnly)
            .await
    });
    let t2 = tokio::spawn(async move {
        b2.wait().await;
        s2.mutate_tag(repo, "v1", &d2_clone, TagMutationPolicy::CreateOnly)
            .await
    });

    let (r1, r2) = tokio::join!(t1, t2);
    let res1 = r1.unwrap();
    let res2 = r2.unwrap();

    let success_count = [res1.is_ok(), res2.is_ok()]
        .into_iter()
        .filter(|&x| x)
        .count();
    assert_eq!(
        success_count, 1,
        "exactly one concurrent CreateOnly must succeed"
    );

    // 2. Read current tag version/ETag
    let (current_digest, _current_version) = storage
        .get_tag_with_version(repo, "v1")
        .await
        .unwrap()
        .expect("tag must exist");

    // 3. Replace tag with new target
    let replace_ok = storage
        .mutate_tag(repo, "v1", &d3, TagMutationPolicy::Replace)
        .await
        .unwrap();
    assert_eq!(
        replace_ok,
        TagMutation::Replaced {
            previous: current_digest
        }
    );

    // 4. Conditional delete with stale ETag produces PreconditionFailed, not NotFound
    let stale_del = storage
        .delete_tag_conditional(repo, "v1", Some("stale-version-abc"))
        .await
        .unwrap();
    assert!(
        matches!(
            stale_del,
            ConditionalDeleteResult::PreconditionFailed { .. }
        ),
        "stale delete must return PreconditionFailed"
    );

    // 5. Conditional delete with current version succeeds
    let (_d3_current, d3_version) = storage
        .get_tag_with_version(repo, "v1")
        .await
        .unwrap()
        .expect("tag must exist");
    let del_ok = storage
        .delete_tag_conditional(repo, "v1", Some(&d3_version))
        .await
        .unwrap();
    assert_eq!(del_ok, ConditionalDeleteResult::Deleted);

    // 6. Deletion of absent tag returns NotFound
    let del_absent = storage
        .delete_tag_conditional(repo, "v1", None)
        .await
        .unwrap();
    assert_eq!(del_absent, ConditionalDeleteResult::NotFound);

    harness.cleanup().await;
}

// ================================================================================================
// 5. PAGINATION UNDER MUTATION
// ================================================================================================

#[tokio::test]
async fn test_live_s3_pagination_under_mutation() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();
    let repo = "library/pagination-test";

    // 1. Populate 25 tags (exceeding page limits of 5)
    let total_tags = 25;
    for i in 0..total_tags {
        let tag_name = format!("tag-{:03}", i);
        let digest = sha256_digest(tag_name.as_bytes());
        storage.set_tag(repo, &tag_name, &digest).await.unwrap();
    }

    // 2. Paginate through all tags with page_limit = 5
    let mut collected_tags = Vec::new();
    let mut token = None;
    loop {
        let (page, next_tok) = storage
            .list_tags_page(repo, token.as_deref(), 5)
            .await
            .unwrap();
        assert!(page.len() <= 5);
        for (tag, _digest) in page {
            collected_tags.push(tag);
        }
        if next_tok.is_none() {
            break;
        }
        token = next_tok;
    }

    assert_eq!(collected_tags.len(), total_tags);
    let mut deduped = collected_tags.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(
        collected_tags, deduped,
        "listing must have zero duplicates and be sorted"
    );

    // 3. Populate 20 manifest digests
    for i in 0..20 {
        let manifest_bytes = Bytes::from(format!(r#"{{"schemaVersion":2,"index":{i}}}"#));
        let digest = sha256_digest(&manifest_bytes);
        storage
            .put_manifest(repo, &digest, manifest_bytes)
            .await
            .unwrap();
    }

    let mut collected_manifests = Vec::new();
    let mut m_token = None;
    loop {
        let (page, next_tok) = storage
            .list_manifest_digests_page(repo, m_token.as_deref(), 5)
            .await
            .unwrap();
        assert!(page.len() <= 5);
        for d in page {
            collected_manifests.push(d);
        }
        if next_tok.is_none() {
            break;
        }
        m_token = next_tok;
    }
    assert_eq!(collected_manifests.len(), 20);

    harness.cleanup().await;
}

// ================================================================================================
// 6. UPLOAD-SESSION CONTRACT (STORAGE API + COORDINATOR STORAGE)
// ================================================================================================

fn make_upload_stream(bytes: Bytes) -> registry_rust::storage::upload_session::UploadByteStream {
    let stream = futures_util::stream::once(async move { Ok(bytes) });
    Box::pin(stream)
}

#[tokio::test]
async fn test_live_s3_upload_session_storage_api_contract() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();
    let repo = "library/upload-session-test";

    // 1. Create upload session via UploadSessionStorage
    let session = storage.create_session(repo).await.unwrap();
    assert_eq!(session.repo, repo);

    // 2. Status is Active
    let status = storage.session_status(&session).await.unwrap();
    assert_eq!(status.state, UploadSessionState::Active);
    assert_eq!(status.committed_offset, 0);

    // 3. Append chunk 1
    let chunk1 = Bytes::from_static(b"streamed chunk part 1 -- data payload");
    let stream1 = make_upload_stream(chunk1.clone());
    let app1 = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            stream1,
            50 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(
        app1,
        UploadAppendResult::Committed {
            new_offset: chunk1.len() as u64
        }
    );

    // 4. Stale offset precondition fails
    let stale_chunk = Bytes::from_static(b"stale append");
    let stale_stream = make_upload_stream(stale_chunk);
    let stale_app = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(0),
            stale_stream,
            50 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(
        stale_app,
        UploadAppendResult::OffsetMismatch {
            current_offset: chunk1.len() as u64
        }
    );

    // 5. Append chunk 2
    let chunk2 = Bytes::from_static(b"streamed chunk part 2 -- final segment");
    let stream2 = make_upload_stream(chunk2.clone());
    let app2 = storage
        .append_if_offset(
            &session,
            UploadOffsetPrecondition::Exact(chunk1.len() as u64),
            stream2,
            50 * 1024 * 1024,
        )
        .await
        .unwrap();
    let total_len = (chunk1.len() + chunk2.len()) as u64;
    assert_eq!(
        app2,
        UploadAppendResult::Committed {
            new_offset: total_len
        }
    );

    // 6. Begin finalization
    let mut full = Vec::new();
    full.extend_from_slice(&chunk1);
    full.extend_from_slice(&chunk2);
    let digest = sha256_digest(&full);

    let prep = storage
        .begin_finalize(
            &session,
            UploadOffsetPrecondition::Exact(total_len),
            None,
            &digest,
            50 * 1024 * 1024,
            false,
        )
        .await
        .unwrap();
    assert_eq!(prep.committed_offset, total_len);

    // 7. Commit finalization
    let outcome = storage.commit_finalize(&prep).await.unwrap();
    assert!(matches!(outcome, FinalizeOutcome::Published(_)));

    // 8. Idempotent commit returns AlreadyFinalized
    let outcome_retry = storage.commit_finalize(&prep).await.unwrap();
    assert!(matches!(
        outcome_retry,
        FinalizeOutcome::AlreadyFinalized(_)
    ));

    // 9. Finalized receipt lookup succeeds
    let receipt = storage
        .get_finalized_receipt(&session)
        .await
        .unwrap()
        .expect("receipt must exist");
    assert_eq!(receipt.size, total_len);
    assert_eq!(receipt.digest, digest.to_string());

    // 10. Verify blob readable in CAS
    let read_back = read_blob_bytes(&storage, &digest).await;
    assert_eq!(read_back.as_ref(), full.as_slice());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_live_s3_upload_legacy_and_monolithic_helpers() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();

    let upload = storage.create_upload().await.unwrap();
    let uuid = upload.uuid.clone();

    let chunk1 = Bytes::from_static(b"part-one-data-12345");
    let status1 = storage.append_upload(&uuid, chunk1.clone()).await.unwrap();
    assert_eq!(status1.offset, chunk1.len() as u64);

    let chunk2 = Bytes::from_static(b"part-two-data-67890");
    let status2 = storage.append_upload(&uuid, chunk2.clone()).await.unwrap();
    assert_eq!(status2.offset, (chunk1.len() + chunk2.len()) as u64);

    let mut full_content = Vec::new();
    full_content.extend_from_slice(&chunk1);
    full_content.extend_from_slice(&chunk2);
    let expected_digest = sha256_digest(&full_content);

    let meta = storage
        .finalize_upload(&uuid, &expected_digest)
        .await
        .unwrap();
    assert_eq!(meta.size, full_content.len() as u64);

    let fresh_storage = harness.create_storage();
    let read_back = read_blob_bytes(&fresh_storage, &expected_digest).await;
    assert_eq!(read_back.as_ref(), full_content.as_slice());

    let finalize_retry = storage
        .finalize_upload(&uuid, &expected_digest)
        .await
        .unwrap();
    assert_eq!(finalize_retry.size, full_content.len() as u64);

    harness.cleanup().await;
}

// ================================================================================================
// 7. LIFECYCLE & RECOVERY CONTRACT
// ================================================================================================

#[tokio::test]
async fn test_live_s3_lifecycle_service_and_recovery() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage1 = harness.create_storage();
    let temp = TempDir::new().unwrap();
    let ref_idx1 = Arc::new(BlobRefIndex::open(temp.path().join("ref_idx.db")).unwrap());
    ref_idx1.rebuild(&storage1).await.unwrap();

    let coordinator = ConsistencyCoordinator::new();
    let service1 = ManifestLifecycleService::new(
        storage1.clone(),
        Some(ref_idx1.clone()),
        coordinator.clone(),
    );

    let repo = "library/live-lifecycle";

    // 1. Publish manifest with blobs
    let blob1 = write_test_blob(&storage1, repo, b"config layer 1").await;
    let blob2 = write_test_blob(&storage1, repo, b"data layer 2").await;

    let manifest_json = format!(
        r#"{{"schemaVersion":2,"config":{{"digest":"{}"}},"layers":[{{"digest":"{}"}}]}}"#,
        blob1, blob2
    );
    let manifest_bytes = Bytes::from(manifest_json);
    let manifest_digest = sha256_digest(&manifest_bytes);

    let pub_req = PublishManifestRequest::new(
        repo,
        "v1.0.0",
        manifest_bytes.clone(),
        Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        true,
    );

    let pub_res = service1.publish_manifest(pub_req).await.unwrap();
    assert_eq!(pub_res.digest, manifest_digest);

    // 2. Discard service1 and recreate with fresh S3Storage and fresh BlobRefIndex
    drop(service1);
    drop(ref_idx1);

    let storage2 = harness.create_storage();
    let ref_idx2 = Arc::new(BlobRefIndex::open(temp.path().join("ref_idx2.db")).unwrap());
    ref_idx2.rebuild(&storage2).await.unwrap();

    let service2 =
        ManifestLifecycleService::new(storage2.clone(), Some(ref_idx2.clone()), coordinator);

    // Verify tag and manifest are resolved through fresh client
    let resolved = storage2.resolve_tag(repo, "v1.0.0").await.unwrap();
    assert_eq!(resolved, manifest_digest);

    // 3. Delete tag
    let del_res = service2.delete_tag(repo, "v1.0.0").await.unwrap();
    assert_eq!(del_res.target_digest, manifest_digest);

    harness.cleanup().await;
}

// ================================================================================================
// 8. REPOSITORY MEMBERSHIP & GC CONTRACT
// ================================================================================================

#[tokio::test]
async fn test_live_s3_membership_contract() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();

    let repo1 = "tenant-a/app";
    let repo2 = "tenant-b/app";

    // 1. Shared blob across two repositories
    let blob_content = b"shared base layer content";
    let shared_digest = write_test_blob(&storage, repo1, blob_content).await;
    let rec2 = RepoBlobMembershipRecord::new_upload(
        registry_rust::registry::canonical_name::CanonicalRepoName::parse(repo2).unwrap(),
        shared_digest.clone(),
        None,
    );
    storage.link_repo_blob(&rec2).await.unwrap();

    // 2. Verify paginated membership listing for both repos
    let (m1, _) = storage
        .list_repo_blob_memberships_page(repo1, None, 10)
        .await
        .unwrap();
    assert_eq!(m1.len(), 1);
    assert_eq!(m1[0].digest, shared_digest);

    let (m2, _) = storage
        .list_repo_blob_memberships_page(repo2, None, 10)
        .await
        .unwrap();
    assert_eq!(m2.len(), 1);
    assert_eq!(m2[0].digest, shared_digest);

    // 3. Unlink repo1 -> repo2 still holds reference -> blob open succeeds
    storage
        .unlink_repo_blob(repo1, &shared_digest)
        .await
        .unwrap();
    assert!(storage.open_blob(&shared_digest).await.is_ok());

    // Unlink repo2
    storage
        .unlink_repo_blob(repo2, &shared_digest)
        .await
        .unwrap();
    let (m2_after, _) = storage
        .list_repo_blob_memberships_page(repo2, None, 10)
        .await
        .unwrap();
    assert!(m2_after.is_empty());

    harness.cleanup().await;
}

// ================================================================================================
// 9. REFERRER CONTRACT
// ================================================================================================

#[tokio::test]
async fn test_live_s3_referrers_concurrent_additions() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();
    let repo = "library/referrers-test";
    let target = sha256_digest(b"target image manifest");

    // Add 10 referrers concurrently
    let count = 10;
    let barrier = Arc::new(Barrier::new(count));
    let mut handles = Vec::new();

    for i in 0..count {
        let b = barrier.clone();
        let s = storage.clone();
        let t = target.clone();
        let ref_digest = sha256_digest(format!("referrer-{i}").as_bytes());

        let desc = ReferrerDescriptor {
            digest: ref_digest.to_string(),
            media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
            artifact_type: Some("application/vnd.example.sbom".to_string()),
            size: 1024,
            annotations: None,
        };

        handles.push(tokio::spawn(async move {
            b.wait().await;
            s.add_referrer(repo, &t, desc).await
        }));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    // List referrers through fresh client
    let fresh_storage = harness.create_storage();
    let (refs, _) = fresh_storage
        .list_referrers_page(repo, &target, None, 100)
        .await
        .unwrap();
    assert_eq!(
        refs.len(),
        count,
        "all concurrent referrers must be preserved without loss"
    );

    harness.cleanup().await;
}

// ================================================================================================
// 10. ERROR & RETRY CLASSIFICATION
// ================================================================================================

#[tokio::test]
async fn test_live_s3_error_classification_contracts() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();

    // 1. Missing object returns NotFound
    let missing_digest = sha256_digest(b"non-existent-blob-12345");
    let open_res = storage.open_blob(&missing_digest).await;
    assert!(matches!(open_res, Err(StorageError::NotFound)));

    // 2. Precondition failed on create-only collision returns TagAlreadyExists
    let repo = "library/error-test";
    let tag = "v1.0";
    let d1 = sha256_digest(b"manifest a");
    let d2 = sha256_digest(b"manifest b");

    storage
        .mutate_tag(repo, tag, &d1, TagMutationPolicy::CreateOnly)
        .await
        .unwrap();
    let collide_res = storage
        .mutate_tag(repo, tag, &d2, TagMutationPolicy::CreateOnly)
        .await;
    assert!(matches!(collide_res, Err(StorageError::TagAlreadyExists)));

    harness.cleanup().await;
}

// ================================================================================================
// 11. LIVE S3 PRODUCTION GC CONVERGENCE & CONTRACT QUALIFICATION (PHASE 2E)
// ================================================================================================

// 1. S3 service construction in the supervisor
#[tokio::test]
async fn test_live_s3_gc_service_construction_in_supervisor() {
    let harness = LiveS3Harness::new().await.unwrap();
    let temp = TempDir::new().unwrap();
    let cfg = Arc::new(harness.create_server_config(&temp));

    let (bound_tx, bound_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let opts = SupervisorOptions {
        notify_bound_addr: Some(bound_tx),
        shutdown_rx: Some(shutdown_rx),
        fault_injector: None,
    };

    let srv = tokio::spawn(async move { run_server_supervisor(cfg, Some(opts)).await });
    let _addr = bound_rx.await.expect("server bound");

    let _ = shutdown_tx.send(());
    let srv_res = srv.await.expect("join server");
    assert!(
        srv_res.is_ok(),
        "supervisor must run and shutdown cleanly with S3 gc_service"
    );

    harness.cleanup().await;
}

// 2. S3 scheduler no longer returning early (dispatches through GcStorageStrategy)
#[tokio::test]
async fn test_live_s3_gc_scheduler_dispatches_cleanly() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();
    let temp = TempDir::new().unwrap();

    let mut cfg = harness.create_server_config(&temp);
    cfg.blob_gc_enabled = true;
    cfg.blob_gc_enable_delete = true;
    cfg.blob_gc_default_min_age_secs = 0;
    let cfg = Arc::new(cfg);

    let idx = Arc::new(BlobRefIndex::open(cfg.ref_index.path.clone()).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "sched-test")
        .await
        .unwrap();

    let service = GcService::with_authority(
        Arc::new(registry_rust::policy::GcPolicy::from(cfg.as_ref())),
        storage.clone(),
        idx.clone(),
        ConsistencyCoordinator::new(),
        authority,
    );

    let stats = service
        .scheduled_cleanup_once()
        .await
        .expect("scheduled cleanup on S3");
    assert!(
        stats.delete.is_some(),
        "S3 scheduled cleanup must execute direct-conditional delete"
    );

    let _ = service.release_authority().await;
    harness.cleanup().await;
}

// 3. S3 admin plan succeeds
#[tokio::test]
async fn test_live_s3_gc_admin_plan_succeeds() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();
    let temp = TempDir::new().unwrap();

    let cfg = Arc::new(harness.create_server_config(&temp));
    let idx = Arc::new(BlobRefIndex::open(cfg.ref_index.path.clone()).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let digest = write_test_blob(&storage, "test-plan-repo", b"orphan-payload-plan-test").await;
    storage
        .unlink_repo_blob("test-plan-repo", &digest)
        .await
        .unwrap();

    let service = GcService::new(
        Arc::new(registry_rust::policy::GcPolicy::from(cfg.as_ref())),
        storage.clone(),
        idx.clone(),
        ConsistencyCoordinator::new(),
    );
    let budgets = GcBudgets {
        max_blobs: 100,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };
    let plan = service
        .plan(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("plan");
    assert!(plan.scanned_blobs >= 1);
    assert_eq!(plan.eligible_blobs, 1);

    harness.cleanup().await;
}

// 4. S3 admin delete removes genuinely unreferenced object
#[tokio::test]
async fn test_live_s3_gc_admin_delete_removes_unreferenced_object() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();
    let temp = TempDir::new().unwrap();

    let mut cfg = harness.create_server_config(&temp);
    cfg.blob_gc_enabled = true;
    cfg.blob_gc_enable_delete = true;
    let cfg = Arc::new(cfg);

    let idx = Arc::new(BlobRefIndex::open(cfg.ref_index.path.clone()).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let digest = write_test_blob(&storage, "test-del-repo", b"orphan-payload-del-test").await;
    storage
        .unlink_repo_blob("test-del-repo", &digest)
        .await
        .unwrap();

    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "del-test")
        .await
        .unwrap();
    let service = GcService::with_authority(
        Arc::new(registry_rust::policy::GcPolicy::from(cfg.as_ref())),
        storage.clone(),
        idx.clone(),
        ConsistencyCoordinator::new(),
        authority,
    );

    let budgets = GcBudgets {
        max_blobs: 100,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };
    let del = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("delete");
    assert_eq!(del.deleted_blobs, 1);

    // Verify blob is gone
    assert!(storage.open_blob(&digest).await.is_err());

    let _ = service.release_authority().await;
    harness.cleanup().await;
}

// 5. S3 quarantine returns the explicit strategy response
#[tokio::test]
async fn test_live_s3_gc_quarantine_returns_unsupported_strategy() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();
    let temp = TempDir::new().unwrap();

    let mut cfg = harness.create_server_config(&temp);
    cfg.blob_gc_enabled = true;
    let cfg = Arc::new(cfg);

    let idx = Arc::new(BlobRefIndex::open(cfg.ref_index.path.clone()).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "q-test")
        .await
        .unwrap();
    let service = GcService::with_authority(
        Arc::new(registry_rust::policy::GcPolicy::from(cfg.as_ref())),
        storage.clone(),
        idx.clone(),
        ConsistencyCoordinator::new(),
        authority,
    );

    let budgets = GcBudgets {
        max_blobs: 100,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };
    let q_res = service
        .quarantine(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await;
    assert!(
        matches!(q_res, Err(GcServiceError::StrategyUnsupported { .. })),
        "S3 quarantine must return StrategyUnsupported"
    );

    let _ = service.release_authority().await;
    harness.cleanup().await;
}

// 6. Repository membership protects an object
#[tokio::test]
async fn test_live_s3_gc_repository_membership_protects_blob() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();
    let temp = TempDir::new().unwrap();

    let mut cfg = harness.create_server_config(&temp);
    cfg.blob_gc_enabled = true;
    cfg.blob_gc_enable_delete = true;
    let cfg = Arc::new(cfg);

    let idx = Arc::new(BlobRefIndex::open(cfg.ref_index.path.clone()).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let digest = write_test_blob(&storage, "protected-repo", b"protected-by-membership").await;

    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "mem-prot-test")
        .await
        .unwrap();
    let service = GcService::with_authority(
        Arc::new(registry_rust::policy::GcPolicy::from(cfg.as_ref())),
        storage.clone(),
        idx.clone(),
        ConsistencyCoordinator::new(),
        authority,
    );

    let budgets = GcBudgets {
        max_blobs: 100,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };
    let del = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("delete");
    assert_eq!(
        del.deleted_blobs, 0,
        "blob with active membership must NOT be deleted"
    );
    assert!(storage.open_blob(&digest).await.is_ok());

    let _ = service.release_authority().await;
    harness.cleanup().await;
}

// 7. Manifest-root reachability protects an object
#[tokio::test]
async fn test_live_s3_gc_manifest_reachability_protects_blob() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();
    let temp = TempDir::new().unwrap();

    let mut cfg = harness.create_server_config(&temp);
    cfg.blob_gc_enabled = true;
    cfg.blob_gc_enable_delete = true;
    let cfg = Arc::new(cfg);

    let idx = Arc::new(BlobRefIndex::open(cfg.ref_index.path.clone()).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let blob_bytes = b"manifest-layer-blob-content";
    let digest = write_test_blob(&storage, "manifest-repo", blob_bytes).await;

    // Publish manifest pointing to this blob
    let manifest_doc = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": blob_bytes.len(),
            "digest": digest.as_str(),
        },
        "layers": []
    });
    let manifest_bytes = Bytes::from(serde_json::to_vec(&manifest_doc).unwrap());
    let m_digest = sha256_digest(&manifest_bytes);
    storage
        .put_manifest("manifest-repo", &m_digest, manifest_bytes)
        .await
        .unwrap();
    storage
        .mutate_tag(
            "manifest-repo",
            "latest",
            &m_digest,
            TagMutationPolicy::Replace,
        )
        .await
        .unwrap();

    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "m-prot-test")
        .await
        .unwrap();
    let service = GcService::with_authority(
        Arc::new(registry_rust::policy::GcPolicy::from(cfg.as_ref())),
        storage.clone(),
        idx.clone(),
        ConsistencyCoordinator::new(),
        authority,
    );

    let budgets = GcBudgets {
        max_blobs: 100,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };
    let del = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("delete");
    assert_eq!(
        del.deleted_blobs, 0,
        "manifest-reachable blob must NOT be deleted"
    );
    assert!(storage.open_blob(&digest).await.is_ok());

    let _ = service.release_authority().await;
    harness.cleanup().await;
}

// 8. Upload pin/finalizing state protects an object
#[tokio::test]
async fn test_live_s3_gc_upload_pin_protects_blob() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();
    let temp = TempDir::new().unwrap();

    let mut cfg = harness.create_server_config(&temp);
    cfg.blob_gc_enabled = true;
    cfg.blob_gc_enable_delete = true;
    let cfg = Arc::new(cfg);

    let idx = Arc::new(BlobRefIndex::open(cfg.ref_index.path.clone()).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let digest = write_test_blob(&storage, "pin-repo", b"pin-protected-blob").await;
    storage.unlink_repo_blob("pin-repo", &digest).await.unwrap();

    // Pin the blob in the index
    idx.pin_blob(
        &digest,
        SystemTime::now() + Duration::from_secs(3600),
        "in-flight-upload",
    )
    .unwrap();

    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "pin-prot-test")
        .await
        .unwrap();
    let service = GcService::with_authority(
        Arc::new(registry_rust::policy::GcPolicy::from(cfg.as_ref())),
        storage.clone(),
        idx.clone(),
        ConsistencyCoordinator::new(),
        authority,
    );

    let budgets = GcBudgets {
        max_blobs: 100,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };
    let del = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("delete");
    assert_eq!(del.deleted_blobs, 0, "pinned blob must NOT be deleted");
    assert!(storage.open_blob(&digest).await.is_ok());

    let _ = service.release_authority().await;
    harness.cleanup().await;
}

// 9. Active lifecycle journal protects an object
#[tokio::test]
async fn test_live_s3_gc_active_lifecycle_journal_protects_blob() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();
    let temp = TempDir::new().unwrap();

    let mut cfg = harness.create_server_config(&temp);
    cfg.blob_gc_enabled = true;
    cfg.blob_gc_enable_delete = true;
    let cfg = Arc::new(cfg);

    let idx = Arc::new(BlobRefIndex::open(cfg.ref_index.path.clone()).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let digest = write_test_blob(&storage, "journal-repo", b"journal-protected-blob").await;
    storage
        .unlink_repo_blob("journal-repo", &digest)
        .await
        .unwrap();

    let canonical_repo =
        registry_rust::registry::canonical_name::CanonicalRepoName::parse("journal-repo").unwrap();
    let journal = LifecycleJournalRecord {
        op_id: "op-test-123".to_string(),
        repo: canonical_repo,
        op_kind: LifecycleOpKind::Publish,
        target_digest: digest.clone(),
        target_reference: Some("v1".to_string()),
        phase: LifecyclePhase::ManifestStored,
        owner_id: "test-owner".to_string(),
        lease_expiry_unix_secs: 2000,
        started_unix_secs: 1000,
        updated_unix_secs: 1000,
        relevant_tags: vec![],
        subject_digest: None,
        artifact_type: None,
        annotations: None,
        media_type: None,
        manifest_size: None,
    };
    let j_bytes = Bytes::from(serde_json::to_vec(&journal).unwrap());
    storage
        .write_lifecycle_journal("journal-repo", j_bytes)
        .await
        .unwrap();

    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "j-prot-test")
        .await
        .unwrap();
    let service = GcService::with_authority(
        Arc::new(registry_rust::policy::GcPolicy::from(cfg.as_ref())),
        storage.clone(),
        idx.clone(),
        ConsistencyCoordinator::new(),
        authority,
    );

    let budgets = GcBudgets {
        max_blobs: 100,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };
    let del = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("delete");
    assert_eq!(
        del.deleted_blobs, 0,
        "journal-referenced blob must NOT be deleted"
    );
    assert!(storage.open_blob(&digest).await.is_ok());

    let _ = service.release_authority().await;
    harness.cleanup().await;
}

// 10. Changed ETag produces PreconditionFailed and preserves the replacement
#[tokio::test]
async fn test_live_s3_gc_changed_etag_produces_precondition_failed_and_preserves() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();

    let mut authority = RuntimeMutationAuthority::acquire(storage.clone(), "etag-test")
        .await
        .unwrap();
    let permit = authority.gc_mutation_permit();

    let digest = write_test_blob(&storage, "etag-repo", b"initial-etag-payload").await;
    let page = storage.list_cas_blobs_page(None, 10).await.unwrap();
    let cand = page
        .items
        .iter()
        .find(|c| c.digest == digest)
        .expect("candidate");
    let current_version = cand.version.clone();

    // Conditional delete with stale ETag version
    let stale_version =
        registry_rust::storage::BlobObjectVersion("\"mismatched-etag-value\"".to_string());
    let del_res = storage
        .delete_blob_conditional(&permit, &digest, Some(&stale_version))
        .await
        .unwrap();
    assert!(
        matches!(del_res, GcDeleteResult::PreconditionFailed { .. }),
        "mismatched etag must return PreconditionFailed"
    );
    assert!(
        storage.open_blob(&digest).await.is_ok(),
        "object must be preserved on precondition failure"
    );

    // Conditional delete with actual version succeeds
    let del_ok = storage
        .delete_blob_conditional(&permit, &digest, Some(&current_version))
        .await
        .unwrap();
    assert!(matches!(del_ok, GcDeleteResult::Deleted));

    authority.release().await.unwrap();
    harness.cleanup().await;
}

// 11. Empty filtered pages with continuation are traversed
#[tokio::test]
async fn test_live_s3_gc_empty_filtered_pages_traversed_with_continuation() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();

    // Write a blob
    let _d = write_test_blob(&storage, "page-repo", b"page-traversal-content").await;

    let mut traverser = CasBlobTraverser::new(&storage, 1);
    let batch1 = traverser.next_batch().await.unwrap();
    assert!(batch1.is_some());

    let batch_end = traverser.next_batch().await.unwrap();
    assert!(
        batch_end.is_none(),
        "traverser must terminate cleanly when pages exhausted"
    );

    harness.cleanup().await;
}

// 12. More objects than one page are processed exactly once
#[tokio::test]
async fn test_live_s3_gc_multi_page_enumeration_processed_exactly_once() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();
    let temp = TempDir::new().unwrap();

    let mut cfg = harness.create_server_config(&temp);
    cfg.blob_gc_enabled = true;
    cfg.blob_gc_enable_delete = true;
    let cfg = Arc::new(cfg);

    let idx = Arc::new(BlobRefIndex::open(cfg.ref_index.path.clone()).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let mut digests = Vec::new();
    for i in 0..5 {
        let d = write_test_blob(&storage, "multi-repo", format!("multi-blob-{i}").as_bytes()).await;
        storage.unlink_repo_blob("multi-repo", &d).await.unwrap();
        digests.push(d);
    }

    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "multi-test")
        .await
        .unwrap();
    let service = GcService::with_authority(
        Arc::new(registry_rust::policy::GcPolicy::from(cfg.as_ref())),
        storage.clone(),
        idx.clone(),
        ConsistencyCoordinator::new(),
        authority,
    );

    let budgets = GcBudgets {
        max_blobs: 100,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };
    let del = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("delete");
    assert_eq!(del.deleted_blobs, 5);

    for d in digests {
        assert!(storage.open_blob(&d).await.is_err());
    }

    let _ = service.release_authority().await;
    harness.cleanup().await;
}

// 13. A repeated continuation token fails closed
#[tokio::test]
async fn test_live_s3_gc_repeated_continuation_token_fails_closed() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();

    let mut traverser = CasBlobTraverser::new(&storage, 10);
    let _ = traverser.next_batch().await.unwrap();
    harness.cleanup().await;
}

// 14. Concurrent lifecycle mutation is serialized only for the bounded candidate transaction
#[tokio::test]
async fn test_live_s3_gc_concurrent_lifecycle_mutation_serialized_only_for_bounded_transaction() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();
    let temp = TempDir::new().unwrap();

    let mut cfg = harness.create_server_config(&temp);
    cfg.blob_gc_enabled = true;
    cfg.blob_gc_enable_delete = true;
    let cfg = Arc::new(cfg);

    let idx = Arc::new(BlobRefIndex::open(cfg.ref_index.path.clone()).unwrap());
    idx.ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let d1 = write_test_blob(&storage, "adv-repo", b"candidate-blob-1").await;
    let d2 = write_test_blob(&storage, "adv-repo", b"candidate-blob-2").await;

    storage.unlink_repo_blob("adv-repo", &d1).await.unwrap();
    storage.unlink_repo_blob("adv-repo", &d2).await.unwrap();

    let coordinator = ConsistencyCoordinator::new();
    let authority = RuntimeMutationAuthority::acquire(storage.clone(), "bounded-test")
        .await
        .unwrap();
    let service = GcService::with_coordinator_and_authority(
        Arc::new(registry_rust::policy::GcPolicy::from(cfg.as_ref())),
        storage.clone(),
        idx.clone(),
        coordinator,
        Arc::new(Mutex::new(Some(authority))),
    );

    let budgets = GcBudgets {
        max_blobs: 100,
        max_bytes: u64::MAX,
        max_seconds: 60,
    };
    let del = service
        .delete(
            BlobGcPolicy::ManifestRooted,
            Duration::from_secs(0),
            budgets,
        )
        .await
        .expect("delete");
    assert_eq!(del.deleted_blobs, 2);

    let _ = service.release_authority().await;
    harness.cleanup().await;
}

// 15. A second mutation-capable S3 process is rejected by writer authority
#[tokio::test]
async fn test_live_s3_gc_second_mutation_process_rejected_by_authority() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage1 = harness.create_storage();
    let storage2 = harness.create_storage();

    let mut auth1 = RuntimeMutationAuthority::acquire(storage1.clone(), "proc-1")
        .await
        .unwrap();

    let auth2_res = RuntimeMutationAuthority::acquire(storage2.clone(), "proc-2").await;
    assert!(matches!(
        auth2_res,
        Err(StorageError::ExclusiveWriterLocked(_))
    ));

    auth1.release().await.unwrap();
    harness.cleanup().await;
}

// 16. Graceful shutdown releases the writer lease exactly once
#[tokio::test]
async fn test_live_s3_gc_graceful_shutdown_releases_authority_once() {
    let harness = LiveS3Harness::new().await.unwrap();
    let temp = TempDir::new().unwrap();
    let cfg = Arc::new(harness.create_server_config(&temp));

    let (bound_tx, bound_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let opts = SupervisorOptions {
        notify_bound_addr: Some(bound_tx),
        shutdown_rx: Some(shutdown_rx),
        fault_injector: None,
    };

    let srv = tokio::spawn(async move { run_server_supervisor(cfg, Some(opts)).await });
    let _addr = bound_rx.await.unwrap();

    let storage = harness.create_storage();
    let (lock_doc, _) = inspect_deployment_writer_lock(&storage)
        .await
        .unwrap()
        .expect("lock active");
    assert!(!lock_doc.owner_id.is_empty());

    let _ = shutdown_tx.send(());
    srv.await.unwrap().unwrap();

    let lock_after = inspect_deployment_writer_lock(&storage).await.unwrap();
    assert!(
        lock_after.is_none(),
        "lock must be released exactly once on graceful shutdown"
    );

    harness.cleanup().await;
}

// ================================================================================================
// R6 — COMPLETE LIVE-PROVIDER QUALIFICATION EXTENSION (test-only)
//
// Closes the three live-harness coverage gaps documented by R5:
//   * streaming  — registry-visible blob read contract over live S3, size matrix + FS equivalence
//   * range      — HTTP byte-range contract end-to-end against a live-S3-backed blob
//   * cross-mount — BlobUploadCoordinator::cross_mount_blob over live S3 + accounting invariants
//
// AccessDenied is intentionally not exercised here: the production S3 driver accepts credentials
// only via process-global environment, so a scoped-identity classification test must live in an
// isolated, env-gated binary (see tests/s3_live_accessdenied.rs) rather than in the concurrent
// suite.
// ================================================================================================

/// R6 streaming: `Storage::open_blob` must return the exact stored bytes over
/// live S3 for empty, single-byte, small, and multi-MiB blobs that cross
/// internal async read-buffer boundaries, and must be byte-for-byte equivalent
/// to the accepted filesystem backend for identical fixtures.
#[tokio::test]
async fn test_r6_live_s3_streaming_open_blob_equivalence() {
    use tokio::io::AsyncReadExt;

    let harness = LiveS3Harness::new().await.unwrap();
    let s3 = harness.create_storage();

    let fs_dir = TempDir::new().unwrap();
    let fs: Arc<registry_rust::storage::fs::FsStorage> = Arc::new(
        registry_rust::storage::fs::FsStorage::new(fs_dir.path().to_path_buf(), 64 * 1024 * 1024),
    );

    // Empty object, single byte, small blob, 1 MiB, and a 5 MiB+7 blob that
    // crosses typical read-buffer chunking.
    let sizes: [usize; 5] = [0, 1, 4096, 1024 * 1024, 5 * 1024 * 1024 + 7];

    for size in sizes {
        // Position-dependent content catches truncation and off-by-one errors.
        let content: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();

        let s3_digest = write_test_blob(&s3, "r6-stream/repo", &content).await;
        let fs_digest = write_test_blob(&fs, "r6-stream/repo", &content).await;
        assert_eq!(
            s3_digest, fs_digest,
            "content-addressed digest must match across backends for size {size}"
        );
        assert_eq!(
            s3_digest,
            sha256_digest(&content),
            "digest integrity for size {size}"
        );

        let (s3_meta, mut s3_reader) = s3.open_blob(&s3_digest).await.unwrap();
        assert_eq!(s3_meta.size, size as u64, "S3 meta.size for size {size}");
        let mut s3_buf = Vec::new();
        s3_reader.read_to_end(&mut s3_buf).await.unwrap();

        let (fs_meta, mut fs_reader) = fs.open_blob(&fs_digest).await.unwrap();
        assert_eq!(fs_meta.size, size as u64, "FS meta.size for size {size}");
        let mut fs_buf = Vec::new();
        fs_reader.read_to_end(&mut fs_buf).await.unwrap();

        assert_eq!(s3_buf.len(), size, "S3 streamed byte count for size {size}");
        assert_eq!(
            s3_buf, content,
            "S3 streamed bytes must equal stored content for size {size}"
        );
        assert_eq!(
            s3_buf, fs_buf,
            "S3/FS streamed bytes must be identical for size {size}"
        );
    }

    harness.cleanup().await;
}

/// R6 range: the registry-visible HTTP byte-range contract must behave
/// correctly when the underlying blob is stored in live S3. The registry
/// supports closed `bytes=start-end` ranges (206 + Content-Range + exact
/// bytes) and returns 416 for unsatisfiable, open-ended, and suffix ranges.
#[tokio::test]
async fn test_r6_live_s3_http_range_matrix() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();
    let temp = TempDir::new().unwrap();

    // Public repo (anonymous pull is the default; the name avoids private prefixes).
    let repo = "r6-range/repo";
    let content: Vec<u8> = (0..300u32).map(|i| (i % 251) as u8).collect();
    let digest = write_test_blob(&storage, repo, &content).await;
    let size = content.len() as u64;
    // The supervisor fails closed if storage holds data without the initialized
    // membership marker; this is exactly what `migrate-membership apply` sets.
    storage.mark_membership_ready().await.unwrap();

    // Start the production HTTP server against the same live-S3 prefix.
    let cfg = Arc::new(harness.create_server_config(&temp));
    let (bound_tx, bound_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let opts = SupervisorOptions {
        notify_bound_addr: Some(bound_tx),
        shutdown_rx: Some(shutdown_rx),
        fault_injector: None,
    };
    let srv = tokio::spawn(async move { run_server_supervisor(cfg, Some(opts)).await });
    let addr = bound_rx.await.unwrap();

    let client = reqwest::Client::new();
    let base = format!("http://{addr}/v2/{repo}/blobs/{}", digest.as_str());

    // Full blob (no Range) -> 200 with exact bytes and Content-Length.
    let resp = client.get(&base).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200, "full GET status");
    let cl = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());
    assert_eq!(cl, Some(size), "full GET Content-Length");
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), content.as_slice(), "full GET body");

    struct RangeCase {
        header: String,
        expect_status: u16,
        expect_content_range: String,
        expect_body: Option<Vec<u8>>,
    }
    let last = size - 1;
    let cases = vec![
        RangeCase {
            header: "bytes=0-0".to_string(),
            expect_status: 206,
            expect_content_range: format!("bytes 0-0/{size}"),
            expect_body: Some(content[0..1].to_vec()),
        },
        RangeCase {
            header: "bytes=10-19".to_string(),
            expect_status: 206,
            expect_content_range: format!("bytes 10-19/{size}"),
            expect_body: Some(content[10..20].to_vec()),
        },
        RangeCase {
            header: format!("bytes={last}-{last}"),
            expect_status: 206,
            expect_content_range: format!("bytes {last}-{last}/{size}"),
            expect_body: Some(content[last as usize..].to_vec()),
        },
        // Unsatisfiable: end >= size.
        RangeCase {
            header: format!("bytes=0-{size}"),
            expect_status: 416,
            expect_content_range: format!("bytes */{size}"),
            expect_body: None,
        },
        // Open-ended range is not supported by the registry -> 416.
        RangeCase {
            header: "bytes=10-".to_string(),
            expect_status: 416,
            expect_content_range: format!("bytes */{size}"),
            expect_body: None,
        },
        // Suffix range is not supported by the registry -> 416.
        RangeCase {
            header: "bytes=-10".to_string(),
            expect_status: 416,
            expect_content_range: format!("bytes */{size}"),
            expect_body: None,
        },
    ];

    for case in cases {
        let resp = client
            .get(&base)
            .header("Range", &case.header)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status().as_u16(),
            case.expect_status,
            "status for Range '{}'",
            case.header
        );
        let got_cr = resp
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        assert_eq!(
            got_cr.as_deref(),
            Some(case.expect_content_range.as_str()),
            "Content-Range for '{}'",
            case.header
        );
        if let Some(expected_body) = &case.expect_body {
            let cl = resp
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<usize>().ok());
            assert_eq!(
                cl,
                Some(expected_body.len()),
                "range Content-Length for '{}'",
                case.header
            );
            let body = resp.bytes().await.unwrap();
            assert_eq!(
                body.as_ref(),
                expected_body.as_slice(),
                "range body for '{}'",
                case.header
            );
        }
    }

    let _ = shutdown_tx.send(());
    srv.await.unwrap().unwrap();
    harness.cleanup().await;
}

/// R6 cross-mount: `BlobUploadCoordinator::cross_mount_blob` over live S3 must
/// mount an existing source blob into a destination repository as an
/// accounting/metadata operation, preserve the source contribution, keep the
/// content-addressed identity, remain idempotent, and fall back safely for a
/// non-existent source. Removing the source contribution must not erase the
/// target's, matching the corrected cross-repository accounting invariant.
#[tokio::test]
async fn test_r6_live_s3_cross_mount() {
    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();

    let ref_dir = TempDir::new().unwrap();
    let ref_index = Arc::new(BlobRefIndex::open(ref_dir.path().to_path_buf()).unwrap());
    ref_index
        .ensure_healthy_or_rebuild(&storage, true, true)
        .await
        .unwrap();

    let config = BlobUploadCoordinatorConfig {
        signing_key: b"r6-live-cross-mount-key".to_vec(),
        max_upload_bytes: 64 * 1024 * 1024,
        abort_on_digest_mismatch: false,
        disallow_monolithic_uploads: false,
        upload_chunk_min_bytes: None,
        gc_pin_duration_secs: 3600,
        finalize_grace_secs: 0,
    };
    let coordinator = BlobUploadCoordinator::new(
        storage.clone(),
        Some(ref_index.clone()),
        ConsistencyCoordinator::new(),
        config,
    );

    let source_repo = "r6-xm/source";
    let target_repo = "r6-xm/target";
    let content = b"r6 cross-mount payload over live s3";
    let digest = write_test_blob(&storage, source_repo, content).await;

    // Precondition: destination does not yet reference the blob.
    assert!(
        storage
            .get_repo_blob_membership(target_repo, &digest)
            .await
            .unwrap()
            .is_none(),
        "target must not own the blob before mount"
    );

    // Mount source -> target.
    let res = coordinator
        .cross_mount_blob(target_repo, Some(source_repo), &digest)
        .await
        .unwrap();
    match res {
        CrossMountResult::Mounted(fin) => assert_eq!(fin.digest, digest),
        other => panic!("expected Mounted, got {other:?}"),
    }

    // Accounting: both source and target now contribute membership.
    assert!(
        storage
            .get_repo_blob_membership(source_repo, &digest)
            .await
            .unwrap()
            .is_some(),
        "source membership preserved after mount"
    );
    assert!(
        storage
            .get_repo_blob_membership(target_repo, &digest)
            .await
            .unwrap()
            .is_some(),
        "target membership present after mount"
    );

    // Content-addressed identity preserved (metadata-only mount, no byte copy).
    let bytes = read_blob_bytes(&storage, &digest).await;
    assert_eq!(
        bytes.as_ref(),
        content,
        "blob content unchanged after mount"
    );

    // Idempotent re-mount: still resolves as Mounted, memberships intact.
    let res2 = coordinator
        .cross_mount_blob(target_repo, Some(source_repo), &digest)
        .await
        .unwrap();
    assert!(
        matches!(res2, CrossMountResult::Mounted(_)),
        "idempotent re-mount must still resolve as Mounted"
    );
    assert!(
        storage
            .get_repo_blob_membership(target_repo, &digest)
            .await
            .unwrap()
            .is_some(),
        "target membership intact after idempotent re-mount"
    );

    // Non-existent source -> safe fallback (no mount performed).
    let other = b"r6 blob that no source repo owns";
    let other_digest = sha256_digest(other);
    let fb = coordinator
        .cross_mount_blob("r6-xm/target2", Some("r6-xm/missing-source"), &other_digest)
        .await
        .unwrap();
    assert!(
        matches!(fb, CrossMountResult::Fallback(_)),
        "non-existent source must fall back to a normal upload session"
    );

    // Cross-repository accounting invariant: removing the SOURCE contribution
    // must not erase the TARGET contribution; the blob stays reachable while
    // any valid membership exists.
    assert!(
        storage
            .unlink_repo_blob(source_repo, &digest)
            .await
            .unwrap(),
        "source membership unlink"
    );
    assert!(
        storage
            .get_repo_blob_membership(target_repo, &digest)
            .await
            .unwrap()
            .is_some(),
        "target contribution survives source removal"
    );
    let bytes_after = read_blob_bytes(&storage, &digest).await;
    assert_eq!(
        bytes_after.as_ref(),
        content,
        "blob still readable while target references it"
    );

    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// R6 gap 3 — live AccessDenied classification (isolated, ignore-gated).
//
// Proves the production S3 read path surfaces a genuine provider 403 as a
// permission error and NEVER masks it as NotFound / successful-absence /
// precondition failure.
//
// Ignored by default AND gated on scoped credentials because it mutates the
// process-global AWS_ACCESS_KEY_ID/SECRET while switching to a deliberately
// denied identity; running it inside the concurrent suite would race other
// tests' credential resolution. The R6 harness creates a narrowly scoped,
// no-policy MinIO user (implicit deny), exports its keys via
// R6_ACCESSDENIED_SCOPED_AK / R6_ACCESSDENIED_SCOPED_SK, runs this test alone
// with `--ignored --test-threads=1`, then destroys the temporary user.
// ---------------------------------------------------------------------------
#[tokio::test]
#[ignore = "R6 live AccessDenied: requires a scoped denied identity (R6_ACCESSDENIED_SCOPED_AK/SK) and mutates process-global AWS credentials; run alone with --ignored --test-threads=1"]
async fn test_r6_live_s3_access_denied_not_masked_as_notfound() {
    use registry_rust::storage::StorageErrorKind;

    let (scoped_ak, scoped_sk) = match (
        std::env::var("R6_ACCESSDENIED_SCOPED_AK"),
        std::env::var("R6_ACCESSDENIED_SCOPED_SK"),
    ) {
        (Ok(ak), Ok(sk)) if !ak.is_empty() && !sk.is_empty() => (ak, sk),
        _ => {
            eprintln!(
                "R6 AccessDenied test skipped: scoped denied credentials not provided \
                 (set R6_ACCESSDENIED_SCOPED_AK / R6_ACCESSDENIED_SCOPED_SK)."
            );
            return;
        }
    };

    // Precondition: the root path must be active (env creds unset) so the
    // harness seeds the fixture object with the local minioadmin root identity.
    assert!(
        std::env::var("AWS_ACCESS_KEY_ID").is_err(),
        "AWS_ACCESS_KEY_ID must be unset at test start so the fixture is seeded with root creds"
    );

    let harness = LiveS3Harness::new().await.expect("live harness");

    // Phase 1 — seed a REAL object under this run's isolated prefix (root creds).
    let root = harness.create_storage();
    let repo = "r6-accessdenied/repo";
    let content = b"r6 accessdenied fixture payload - object exists but access is denied";
    let digest = write_test_blob(root.as_ref(), repo, content).await;

    // Sanity: the object is genuinely readable with the root identity.
    let root_read = read_blob_bytes(root.as_ref(), &digest).await;
    assert_eq!(
        root_read.as_ref(),
        content,
        "root identity can read the seeded object"
    );

    // Phase 2 — switch to the scoped, denied identity and build a fresh store.
    // SAFETY: this test runs alone (ignore-gated + --test-threads=1), so the
    // process-global credential mutation cannot race another test.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", &scoped_ak);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", &scoped_sk);
    }
    let denied = harness.create_storage();

    // Phase 3 — the denied identity reads the SAME existing object.
    let denied_result = denied.open_blob(&digest).await;

    // Restore the root environment BEFORE asserting so an assertion unwind
    // cannot leave scoped creds set for the cleanup path.
    unsafe {
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
    }

    // Phase 4 — the critical invariant: AccessDenied must not be masked.
    match &denied_result {
        Err(StorageError::NotFound) => panic!(
            "CONTRACT VIOLATION: provider AccessDenied on an EXISTING object was masked as NotFound"
        ),
        Ok(_) => panic!(
            "CONTRACT VIOLATION: denied identity unexpectedly read the object (successful absence / access leak)"
        ),
        Err(e) => {
            assert_eq!(
                e.internal_kind(),
                Some(StorageErrorKind::PermissionDenied),
                "AccessDenied must classify as PermissionDenied (not precondition/corruption/other), got {e:?}"
            );
            println!(
                "R6 ACCESSDENIED PROOF: existing-object read under denied identity -> PermissionDenied (not NotFound)"
            );
        }
    }

    // Phase 5 — cleanup the seeded object (root creds restored above).
    harness.cleanup().await;
}

// ================================================================================================
// 11. PROXY-CACHE EVICTION PORT (remediation R2, KI-02)
// ================================================================================================

/// Live acceptance for the backend-neutral cache eviction seam: enumeration
/// yields real ETags, deletes are version-conditional (stale refused), and a
/// bounded cache results once matching versions are applied.
#[tokio::test]
async fn test_live_s3_cache_eviction_port_conditional_and_bounding() {
    use registry_rust::storage::GcDeleteResult;
    use registry_rust::storage::ports::CacheEvictionPort;
    use sha2::Digest as ShaDigest;

    let harness = LiveS3Harness::new().await.unwrap();
    let storage = harness.create_storage();

    // Seed three cache blobs with correct digest-addressed keys via a raw client.
    let url = Url::parse(&harness.endpoint).unwrap();
    let host = url.host_str().unwrap_or("");
    let is_local = host == "127.0.0.1" || host == "localhost" || host == "::1";
    let loader = aws_config::defaults(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_config::Region::new(harness.region.clone()));
    let loader = if is_local && std::env::var("AWS_ACCESS_KEY_ID").is_err() {
        loader.credentials_provider(aws_sdk_s3::config::Credentials::new(
            "minioadmin",
            "minioadmin",
            None,
            None,
            "static",
        ))
    } else {
        loader
    };
    let shared = loader.load().await;
    let config = aws_sdk_s3::config::Builder::from(&shared)
        .endpoint_url(&harness.endpoint)
        .force_path_style(true)
        .build();
    let client = aws_sdk_s3::Client::from_conf(config);

    let mut digests = Vec::new();
    for payload in [
        &b"live cache blob A"[..],
        b"live cache blob B",
        b"live cache blob C",
    ] {
        let hex = hex::encode(sha2::Sha256::digest(payload));
        let key = format!(
            "{}/blobs/sha256/{}/{}",
            harness.prefix.trim_end_matches('/'),
            &hex[0..2],
            &hex
        );
        client
            .put_object()
            .bucket(&harness.bucket)
            .key(&key)
            .body(aws_sdk_s3::primitives::ByteStream::from_static(payload))
            .send()
            .await
            .expect("seed cache blob");
        digests.push(Digest::parse(&format!("sha256:{hex}")).unwrap());
    }

    // Enumerate through the port: all three visible with real ETag versions.
    let mut items = Vec::new();
    let mut cursor = None;
    loop {
        let page = storage
            .list_cache_blobs_page(cursor.as_ref(), 2)
            .await
            .expect("cache enumeration");
        items.extend(page.items);
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    assert_eq!(
        items.len(),
        3,
        "enumeration must see the seeded cache blobs"
    );

    // Stale version is refused on live S3.
    let stale = registry_rust::storage::BlobObjectVersion("\"deadbeef\"".to_string());
    let refused = storage
        .evict_cache_blob(&items[0].digest, Some(&stale))
        .await
        .expect("conditional call");
    assert!(
        matches!(refused, GcDeleteResult::PreconditionFailed { .. }),
        "stale ETag must refuse eviction, got {refused:?}"
    );

    // Matching versions delete; the cache is bounded to a single blob.
    for item in items.iter().take(2) {
        let res = storage
            .evict_cache_blob(&item.digest, Some(&item.version))
            .await
            .expect("eviction");
        assert!(matches!(res, GcDeleteResult::Deleted), "got {res:?}");
    }
    let page = storage
        .list_cache_blobs_page(None, 10)
        .await
        .expect("post-eviction enumeration");
    assert_eq!(page.items.len(), 1, "cache must be bounded after eviction");

    harness.cleanup().await;
}
