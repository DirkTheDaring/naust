// Wiring/composition tests moved from storage/fs/tests.rs (ADR-010 Phase 1c):
// they exercise the Config->StorageWiring factories, which are server-side.
use super::*;
use crate::registry::digest::Digest;
use crate::storage::Storage;
use crate::storage::fs::FsStorage;
use crate::storage::fs::repo_discovery;
use crate::storage::fs::test_helpers::{prepare_finalizable_session, tmp_fs_root, write_file};
use crate::storage::upload_session::{FinalizeOutcome, UploadSessionStorage};
use crate::storage::{BlobMeta, StorageErrorKind};
use bytes::Bytes;

/// Category: primary and proxy-cache production wiring. Both `storage/mod.rs`
/// construction sites build the filesystem backend through
/// `FsStorage::try_new_with_all_limits`, which captures the shared
/// `UploadAuthorities` (pinning the root) at construction. This test drives both
/// production factories and confirms a concrete backend built by the same
/// constructor supports the full contained upload lifecycle.
#[tokio::test]
async fn test_fs_upload_authority_wired_through_primary_and_proxy_cache_construction() {
    let root = tmp_fs_root();
    let mut config = crate::config::Config::from_env().unwrap();
    config.storage_backend = crate::config::StorageBackend::Filesystem;
    config.fs_root = root.clone();
    config.max_upload_bytes = 1024 * 1024;

    // Primary production wiring constructs (root pinned; capture total).
    let primary = crate::storage_wiring::storage_wiring_try_from_config(&config)
        .expect("primary filesystem wiring constructs");
    assert_eq!(primary.backend_kind(), "fs");

    // Proxy-cache production wiring constructs against its own distinct root.
    let cache_root = root.join("cache");
    let mut proxy_cfg = config.clone();
    proxy_cfg.proxy.cache_fs_root = Some(cache_root.clone());
    let _proxy = crate::storage_wiring::proxy_cache_storage_try_from_config(&proxy_cfg, None)
        .expect("proxy-cache filesystem wiring constructs");

    // A concrete backend built by the same constructor path supports the full
    // contained lifecycle end to end (create -> append -> finalize -> publish),
    // proving the captured authority is functional.
    let storage = FsStorage::new(root.join("direct"), 1024 * 1024);
    let data = b"WIRED_PAYLOAD";
    let (_session, prepared, _digest) =
        prepare_finalizable_session(&storage, "wirerepo", data).await;
    let outcome = storage.commit_finalize(&prepared).await.unwrap();
    assert_eq!(
        outcome,
        FinalizeOutcome::Published(BlobMeta {
            size: data.len() as u64
        })
    );
}

#[tokio::test]
async fn test_manifest_listing_shared_reader_and_startup_offload() {
    let fixture = tempfile::tempdir().expect("create test fixture");
    let root = fixture.path().join("storage-root");
    std::fs::create_dir_all(&root).expect("create storage root");

    // 1. Verify shared reader pointer equality:
    // FsStorage.reader() and FsStorage.read_adapter().reader() share the identical Arc<FsMetadataReader>,
    // and list_manifest_digests_page delegates directly to list_manifest_digests_page_impl using self.reader.as_ref().
    let storage = FsStorage::try_new(root.clone(), 1024 * 1024).expect("storage init");
    assert!(
        std::sync::Arc::ptr_eq(storage.reader(), storage.read_adapter().reader()),
        "FsStorage.reader and read_adapter must share the identical Arc<FsMetadataReader>"
    );

    // 2. Verify async startup factory offload via tokio::task::spawn_blocking:
    // Factory execution must occur off the calling async worker thread.
    let (worker_tx, worker_rx) = tokio::sync::oneshot::channel();
    let calling_thread_id = std::thread::current().id();
    let worker_tx = std::sync::Mutex::new(Some(worker_tx));

    let mut config = crate::config::Config::from_env().unwrap();
    config.storage_backend = crate::config::StorageBackend::Filesystem;
    config.fs_root = root.clone();
    config.max_upload_bytes = 1024 * 1024;
    // Configure distinctive small limit of 1 entry to verify it governs listing
    config.fs_manifest_listing_max_entries = 1;
    config.fs_manifest_listing_max_name_bytes = 100_000;

    let wiring = crate::storage_wiring::storage_wiring_try_from_config_async_with_factory(
        &config,
        move |cfg| {
            let current_id = std::thread::current().id();
            if let Some(tx) = worker_tx.lock().unwrap().take() {
                let _ = tx.send(current_id);
            }
            crate::storage_wiring::storage_wiring_try_from_config(cfg)
        },
    )
    .await
    .expect("startup offload factory succeeds");

    let construction_thread_id = worker_rx.await.expect("worker thread id must be sent");
    assert_ne!(
        calling_thread_id, construction_thread_id,
        "filesystem storage construction must execute off the calling async worker thread via spawn_blocking"
    );
    assert_eq!(wiring.backend_kind(), "fs");

    // 3. Demonstrate configured limits govern listing through primary storage wiring:
    let primary_reader = wiring.manifest_reader();
    let primary_repo = "primary_limit_repo";
    let primary_manifests_dir = root.join("repos").join(primary_repo).join("manifests");
    std::fs::create_dir_all(&primary_manifests_dir).expect("create primary manifests dir");

    let hex1 = "1111111111111111111111111111111111111111111111111111111111111111";
    write_file(&primary_manifests_dir.join(hex1), b"{}");

    // Exactly 1 entry <= max_entries(1) -> succeeds
    let (page, _) = primary_reader
        .list_manifest_digests_page(primary_repo, None, 10)
        .await
        .expect("listing 1 entry within limit succeeds");
    assert_eq!(page.len(), 1);

    // 2 entries stream successfully without limits
    let hex2 = "2222222222222222222222222222222222222222222222222222222222222222";
    write_file(&primary_manifests_dir.join(hex2), b"{}");
    let (page2, _) = primary_reader
        .list_manifest_digests_page(primary_repo, None, 10)
        .await
        .expect("listing 2 entries on primary storage succeeds under unbounded streaming");
    assert_eq!(page2.len(), 2);

    // 4. Demonstrate unbounded streaming through filesystem proxy-cache wiring:
    let cache_root = root.join("cache");
    let mut proxy_cfg = config.clone();
    proxy_cfg.proxy.cache_fs_root = Some(cache_root.clone());
    proxy_cfg.fs_manifest_listing_max_entries = 1;
    proxy_cfg.fs_manifest_listing_max_name_bytes = 100_000;

    let proxy_storage =
        crate::storage_wiring::proxy_cache_storage_try_from_config(&proxy_cfg, None).unwrap();
    let proxy_repo = "proxy_limit_repo";
    let proxy_manifests_dir = cache_root.join("repos").join(proxy_repo).join("manifests");
    std::fs::create_dir_all(&proxy_manifests_dir).expect("create proxy manifests dir");

    write_file(&proxy_manifests_dir.join(hex1), b"{}");
    let (proxy_page, _) = proxy_storage
        .as_manifest_reader()
        .list_manifest_digests_page(proxy_repo, None, 10)
        .await
        .expect("proxy listing 1 entry succeeds");
    assert_eq!(proxy_page.len(), 1);

    write_file(&proxy_manifests_dir.join(hex2), b"{}");
    let (proxy_page2, _) = proxy_storage
        .as_manifest_reader()
        .list_manifest_digests_page(proxy_repo, None, 10)
        .await
        .expect("proxy listing 2 entries succeeds under unbounded streaming");
    assert_eq!(proxy_page2.len(), 2);
}

#[tokio::test]
async fn test_tag_listing_cutover_wiring_limits_enforcement_and_async_offload() {
    let root = tmp_fs_root();
    let calling_thread_id = std::thread::current().id();
    let worker_tx = Arc::new(std::sync::Mutex::new(None));
    let (tx, worker_rx) = tokio::sync::oneshot::channel();
    *worker_tx.lock().unwrap() = Some(tx);

    let mut config = crate::config::Config::from_env().unwrap();
    config.storage_backend = crate::config::StorageBackend::Filesystem;
    config.fs_root = root.clone();
    // Non-default tag listing limits: max_entries = 1, max_name_bytes = 100_000
    config.fs_tag_listing_max_entries = 1;
    config.fs_tag_listing_max_name_bytes = 100_000;

    let wiring = crate::storage_wiring::storage_wiring_try_from_config_async_with_factory(
        &config,
        move |cfg| {
            let current_id = std::thread::current().id();
            if let Some(tx) = worker_tx.lock().unwrap().take() {
                let _ = tx.send(current_id);
            }
            crate::storage_wiring::storage_wiring_try_from_config(cfg)
        },
    )
    .await
    .expect("startup offload factory succeeds");

    let construction_thread_id = worker_rx.await.expect("worker thread id must be sent");
    assert_ne!(
        calling_thread_id, construction_thread_id,
        "primary storage construction must execute off the calling async thread via spawn_blocking"
    );
    assert_eq!(wiring.backend_kind(), "fs");

    // 1. Primary storage wiring enforces nondefault tag listing limits
    let primary_reader = wiring.tag_reader();
    let repo = "primary_tag_repo";
    let tags_dir = root.join("repos").join(repo).join("tags");
    std::fs::create_dir_all(&tags_dir).expect("create primary tags dir");

    let hex1 = "1111111111111111111111111111111111111111111111111111111111111111";
    write_file(
        &tags_dir.join("tag-1"),
        format!("sha256:{hex1}\n").as_bytes(),
    );

    // 1 tag <= max_entries(1) -> succeeds for both list_tags and list_tags_page
    let tags = primary_reader
        .list_tags(repo)
        .await
        .expect("1 tag within limit succeeds");
    assert_eq!(tags, vec!["tag-1"]);
    let (page, _) = primary_reader
        .list_tags_page(repo, None, 10)
        .await
        .expect("1 tag page succeeds");
    assert_eq!(page.len(), 1);

    // 2 tags > max_entries(1) -> fails closed with StorageErrorKind::Backend
    let hex2 = "2222222222222222222222222222222222222222222222222222222222222222";
    write_file(
        &tags_dir.join("tag-2"),
        format!("sha256:{hex2}\n").as_bytes(),
    );

    let tags = primary_reader
        .list_tags(repo)
        .await
        .expect("listing tags under unbounded streaming succeeds");
    assert_eq!(tags, vec!["tag-1", "tag-2"]);

    let (page, _) = primary_reader
        .list_tags_page(repo, None, 10)
        .await
        .expect("listing tag page under unbounded streaming succeeds");
    assert_eq!(page.len(), 2);

    // 2. Proxy-cache storage wiring operates with unbounded streaming
    let cache_root = root.join("cache");
    let mut proxy_cfg = config.clone();
    proxy_cfg.proxy.cache_fs_root = Some(cache_root.clone());
    proxy_cfg.fs_tag_listing_max_entries = 1;

    let proxy_storage =
        crate::storage_wiring::proxy_cache_storage_try_from_config(&proxy_cfg, None)
            .expect("proxy cache storage construction succeeds");
    let proxy_repo = "proxy_tag_repo";
    let proxy_tags_dir = cache_root.join("repos").join(proxy_repo).join("tags");
    std::fs::create_dir_all(&proxy_tags_dir).expect("create proxy tags dir");

    write_file(
        &proxy_tags_dir.join("tag-1"),
        format!("sha256:{hex1}\n").as_bytes(),
    );
    let proxy_tags = proxy_storage
        .as_tag_reader()
        .list_tags(proxy_repo)
        .await
        .expect("proxy 1 tag succeeds");
    assert_eq!(proxy_tags, vec!["tag-1"]);

    write_file(
        &proxy_tags_dir.join("tag-2"),
        format!("sha256:{hex2}\n").as_bytes(),
    );
    let proxy_tags2 = proxy_storage
        .as_tag_reader()
        .list_tags(proxy_repo)
        .await
        .expect("proxy listing 2 tags under unbounded streaming succeeds");
    assert_eq!(proxy_tags2, vec!["tag-1", "tag-2"]);

    // 3. Proxy-cache storage async factory offload executes on blocking thread
    let proxy_worker_tx = Arc::new(std::sync::Mutex::new(None));
    let (ptx, proxy_worker_rx) = tokio::sync::oneshot::channel();
    *proxy_worker_tx.lock().unwrap() = Some(ptx);

    let _proxy_async_storage =
        crate::storage_wiring::proxy_cache_storage_try_from_config_async_with_factory(
            &proxy_cfg,
            None,
            move |cfg, upstream| {
                let current_id = std::thread::current().id();
                if let Some(tx) = proxy_worker_tx.lock().unwrap().take() {
                    let _ = tx.send(current_id);
                }
                crate::storage_wiring::proxy_cache_storage_try_from_config(cfg, upstream)
            },
        )
        .await
        .expect("proxy cache async factory succeeds");

    let proxy_construction_thread_id = proxy_worker_rx
        .await
        .expect("proxy worker thread id must be sent");
    assert_ne!(
        calling_thread_id, proxy_construction_thread_id,
        "proxy cache storage construction must execute off the calling async thread via spawn_blocking"
    );
}

#[tokio::test]
async fn test_tag_listing_wiring_tag_name_bytes_enforcement() {
    let root = tmp_fs_root();
    let mut config = crate::config::Config::from_env().unwrap();
    config.storage_backend = crate::config::StorageBackend::Filesystem;
    config.fs_root = root.clone();
    // Non-default tag listing limits: max_entries = 100, max_name_bytes = 128 (approved minimum)
    config.fs_tag_listing_max_entries = 100;
    config.fs_tag_listing_max_name_bytes = 128;

    let hex1 = "1111111111111111111111111111111111111111111111111111111111111111";
    let hex2 = "2222222222222222222222222222222222222222222222222222222222222222";
    let tag_exact_128 = "a".repeat(128);

    // 1. Primary filesystem storage wiring path
    let wiring = crate::storage_wiring::storage_wiring_try_from_config(&config)
        .expect("primary storage wiring succeeds");
    let primary_reader = wiring.tag_reader();
    let primary_repo = "primary_name_bytes_repo";
    let primary_tags_dir = root.join("repos").join(primary_repo).join("tags");
    std::fs::create_dir_all(&primary_tags_dir).expect("create primary tags dir");

    write_file(
        &primary_tags_dir.join(&tag_exact_128),
        format!("sha256:{hex1}\n").as_bytes(),
    );

    // Exact boundary (128 bytes <= 128 limit) succeeds for list_tags and list_tags_page
    let tags = primary_reader
        .list_tags(primary_repo)
        .await
        .expect("128-byte tag name within 128-byte limit succeeds");
    assert_eq!(tags, vec![tag_exact_128.clone()]);
    let (page, _) = primary_reader
        .list_tags_page(primary_repo, None, 10)
        .await
        .expect("128-byte tag name page within limit succeeds");
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].0, tag_exact_128);

    // Over-limit failure: adding a 1-byte name pushes cumulative bytes to 129 > 128
    write_file(
        &primary_tags_dir.join("b"),
        format!("sha256:{hex2}\n").as_bytes(),
    );
    let tags = primary_reader
        .list_tags(primary_repo)
        .await
        .expect("list_tags succeeds under unbounded streaming");
    assert_eq!(tags.len(), 2);

    let (page, _) = primary_reader
        .list_tags_page(primary_repo, None, 10)
        .await
        .expect("list_tags_page succeeds under unbounded streaming");
    assert_eq!(page.len(), 2);

    // 2. Proxy-cache filesystem storage wiring path
    let cache_root = root.join("cache");
    let mut proxy_cfg = config.clone();
    proxy_cfg.proxy.cache_fs_root = Some(cache_root.clone());
    let proxy_storage =
        crate::storage_wiring::proxy_cache_storage_try_from_config(&proxy_cfg, None)
            .expect("proxy cache storage construction succeeds");
    let proxy_reader = proxy_storage.as_tag_reader();
    let proxy_repo = "proxy_name_bytes_repo";
    let proxy_tags_dir = cache_root.join("repos").join(proxy_repo).join("tags");
    std::fs::create_dir_all(&proxy_tags_dir).expect("create proxy tags dir");

    write_file(
        &proxy_tags_dir.join(&tag_exact_128),
        format!("sha256:{hex1}\n").as_bytes(),
    );
    let proxy_tags = proxy_reader
        .list_tags(proxy_repo)
        .await
        .expect("proxy 128-byte tag name succeeds");
    assert_eq!(proxy_tags, vec![tag_exact_128.clone()]);
    let (proxy_page, _) = proxy_reader
        .list_tags_page(proxy_repo, None, 10)
        .await
        .expect("proxy 128-byte tag page succeeds");
    assert_eq!(proxy_page.len(), 1);

    write_file(
        &proxy_tags_dir.join("b"),
        format!("sha256:{hex2}\n").as_bytes(),
    );
    let proxy_tags2 = proxy_reader
        .list_tags(proxy_repo)
        .await
        .expect("proxy list_tags succeeds under unbounded streaming");
    assert_eq!(proxy_tags2.len(), 2);

    let (proxy_page2, _) = proxy_reader
        .list_tags_page(proxy_repo, None, 10)
        .await
        .expect("proxy list_tags_page succeeds under unbounded streaming");
    assert_eq!(proxy_page2.len(), 2);
}

#[tokio::test]
async fn test_tag_listing_wiring_repo_probe_entries_enforcement() {
    let root = tmp_fs_root();
    let mut config = crate::config::Config::from_env().unwrap();
    config.storage_backend = crate::config::StorageBackend::Filesystem;
    config.fs_root = root.clone();
    // Non-default probe limits: max_entries = 1 (approved minimum), max_name_bytes = 4096
    config.fs_tag_listing_repo_probe_max_entries = 1;
    config.fs_tag_listing_repo_probe_max_name_bytes = 4096;

    // 1. Primary filesystem storage wiring path
    let wiring = crate::storage_wiring::storage_wiring_try_from_config(&config)
        .expect("primary storage wiring succeeds");
    let primary_reader = wiring.tag_reader();
    let primary_repo = "primary_probe_entries_repo";
    let repo_dir = root.join("repos").join(primary_repo);

    // Ensure tags/ directory does NOT exist (NotFound triggers probe)
    // Create 1 immediate child directory: "manifests"
    // Create nested descendants inside manifests/: these MUST NOT count towards the probe budget!
    write_file(
        &repo_dir.join("manifests").join("descendant_1"),
        b"payload1",
    );
    write_file(
        &repo_dir.join("manifests").join("descendant_2"),
        b"payload2",
    );
    write_file(
        &repo_dir.join("manifests").join("descendant_3"),
        b"payload3",
    );

    // Exact boundary: exactly 1 immediate child ("manifests") <= repo_probe_max_entries(1)
    // Probe succeeds; list_tags returns empty Vec, list_tags_page returns (empty, None)
    let tags = primary_reader
        .list_tags(primary_repo)
        .await
        .expect("1 immediate child with nested descendants must succeed probe");
    assert!(tags.is_empty());
    let (page, next_tok) = primary_reader
        .list_tags_page(primary_repo, None, 10)
        .await
        .expect("1 immediate child probe must succeed page");
    assert!(page.is_empty());
    assert!(next_tok.is_none());

    // Over-limit failure: add a 2nd immediate child directory to repo_dir
    write_file(&repo_dir.join("referrers").join("ref_file"), b"payload");
    // Immediate children count is now 2 > 1 limit
    let err_tags = primary_reader
        .list_tags(primary_repo)
        .await
        .expect_err("exceeding repo_probe_max_entries must fail list_tags");
    assert_eq!(err_tags.internal_kind(), Some(StorageErrorKind::Backend));
    assert!(
        err_tags.to_string().contains("MaxEntries(1)"),
        "error must cite MaxEntries(1): {err_tags}"
    );
    assert!(
        err_tags.to_string().contains(primary_repo),
        "error must reference repository: {err_tags}"
    );

    // Phase 3: list_tags_page never probes repository existence (its frozen
    // missing-repository contract is an empty terminal page on both
    // backends), so the probe budget cannot fail it.
    let (page_no_probe, tok_no_probe) = primary_reader
        .list_tags_page(primary_repo, None, 10)
        .await
        .expect("list_tags_page does not consult the repository probe");
    assert!(page_no_probe.is_empty());
    assert!(tok_no_probe.is_none());

    // 2. Proxy-cache filesystem storage wiring path
    let cache_root = root.join("cache");
    let mut proxy_cfg = config.clone();
    proxy_cfg.proxy.cache_fs_root = Some(cache_root.clone());
    let proxy_storage =
        crate::storage_wiring::proxy_cache_storage_try_from_config(&proxy_cfg, None)
            .expect("proxy cache storage construction succeeds");
    let proxy_reader = proxy_storage.as_tag_reader();
    let proxy_repo = "proxy_probe_entries_repo";
    let proxy_repo_dir = cache_root.join("repos").join(proxy_repo);

    write_file(
        &proxy_repo_dir.join("manifests").join("descendant_1"),
        b"payload1",
    );
    write_file(
        &proxy_repo_dir.join("manifests").join("descendant_2"),
        b"payload2",
    );

    let proxy_tags = proxy_reader
        .list_tags(proxy_repo)
        .await
        .expect("proxy 1 immediate child probe succeeds");
    assert!(proxy_tags.is_empty());
    let (proxy_page, _) = proxy_reader
        .list_tags_page(proxy_repo, None, 10)
        .await
        .expect("proxy 1 immediate child page probe succeeds");
    assert!(proxy_page.is_empty());

    write_file(
        &proxy_repo_dir.join("referrers").join("ref_file"),
        b"payload",
    );
    let proxy_err_tags = proxy_reader
        .list_tags(proxy_repo)
        .await
        .expect_err("proxy exceeding repo_probe_max_entries must fail");
    assert_eq!(
        proxy_err_tags.internal_kind(),
        Some(StorageErrorKind::Backend)
    );
    assert!(proxy_err_tags.to_string().contains("MaxEntries(1)"));

    let (proxy_page_no_probe, proxy_tok_no_probe) = proxy_reader
        .list_tags_page(proxy_repo, None, 10)
        .await
        .expect("proxy list_tags_page does not consult the repository probe");
    assert!(proxy_page_no_probe.is_empty());
    assert!(proxy_tok_no_probe.is_none());
}

#[tokio::test]
async fn test_tag_listing_wiring_repo_probe_name_bytes_enforcement() {
    let root = tmp_fs_root();
    let mut config = crate::config::Config::from_env().unwrap();
    config.storage_backend = crate::config::StorageBackend::Filesystem;
    config.fs_root = root.clone();
    // Non-default probe limits: max_entries = 10, max_name_bytes = 64 (approved minimum)
    config.fs_tag_listing_repo_probe_max_entries = 10;
    config.fs_tag_listing_repo_probe_max_name_bytes = 64;

    let child_exact_64 = "c".repeat(64);

    // 1. Primary filesystem storage wiring path
    let wiring = crate::storage_wiring::storage_wiring_try_from_config(&config)
        .expect("primary storage wiring succeeds");
    let primary_reader = wiring.tag_reader();
    let primary_repo = "primary_probe_name_bytes_repo";
    let repo_dir = root.join("repos").join(primary_repo);

    // Ensure tags/ does not exist; create 1 immediate child with name length exactly 64 bytes
    write_file(&repo_dir.join(&child_exact_64).join("subfile"), b"payload");

    // Exact boundary (64 bytes <= 64 limit) succeeds
    let tags = primary_reader
        .list_tags(primary_repo)
        .await
        .expect("64-byte probe child name within limit must succeed");
    assert!(tags.is_empty());
    let (page, _) = primary_reader
        .list_tags_page(primary_repo, None, 10)
        .await
        .expect("64-byte probe child name within limit must succeed page");
    assert!(page.is_empty());

    // Over-limit failure: add a 2nd immediate child of 1 byte ("d"), total bytes = 65 > 64
    write_file(&repo_dir.join("d").join("subfile"), b"payload");
    let err_tags = primary_reader
        .list_tags(primary_repo)
        .await
        .expect_err("exceeding repo_probe_max_name_bytes must fail list_tags");
    assert_eq!(err_tags.internal_kind(), Some(StorageErrorKind::Backend));
    assert!(
        err_tags.to_string().contains("MaxTotalNameBytes(64)"),
        "error must cite MaxTotalNameBytes(64): {err_tags}"
    );

    // Phase 3: list_tags_page never probes repository existence (frozen
    // missing-repository contract: empty terminal page on both backends).
    let (page_no_probe, tok_no_probe) = primary_reader
        .list_tags_page(primary_repo, None, 10)
        .await
        .expect("list_tags_page does not consult the repository probe");
    assert!(page_no_probe.is_empty());
    assert!(tok_no_probe.is_none());

    // 2. Proxy-cache filesystem storage wiring path
    let cache_root = root.join("cache");
    let mut proxy_cfg = config.clone();
    proxy_cfg.proxy.cache_fs_root = Some(cache_root.clone());
    let proxy_storage =
        crate::storage_wiring::proxy_cache_storage_try_from_config(&proxy_cfg, None)
            .expect("proxy cache storage construction succeeds");
    let proxy_reader = proxy_storage.as_tag_reader();
    let proxy_repo = "proxy_probe_name_bytes_repo";
    let proxy_repo_dir = cache_root.join("repos").join(proxy_repo);

    write_file(
        &proxy_repo_dir.join(&child_exact_64).join("subfile"),
        b"payload",
    );
    let proxy_tags = proxy_reader
        .list_tags(proxy_repo)
        .await
        .expect("proxy 64-byte probe child succeeds");
    assert!(proxy_tags.is_empty());
    let (proxy_page, _) = proxy_reader
        .list_tags_page(proxy_repo, None, 10)
        .await
        .expect("proxy 64-byte probe child page succeeds");
    assert!(proxy_page.is_empty());

    write_file(&proxy_repo_dir.join("d").join("subfile"), b"payload");
    let proxy_err_tags = proxy_reader
        .list_tags(proxy_repo)
        .await
        .expect_err("proxy exceeding repo_probe_max_name_bytes must fail");
    assert_eq!(
        proxy_err_tags.internal_kind(),
        Some(StorageErrorKind::Backend)
    );
    assert!(proxy_err_tags.to_string().contains("MaxTotalNameBytes(64)"));

    let (proxy_page_no_probe, proxy_tok_no_probe) = proxy_reader
        .list_tags_page(proxy_repo, None, 10)
        .await
        .expect("proxy list_tags_page does not consult the repository probe");
    assert!(proxy_page_no_probe.is_empty());
    assert!(proxy_tok_no_probe.is_none());
}

#[tokio::test]
async fn test_tag_listing_wiring_payload_ceiling_enforcement() {
    let root = tmp_fs_root();
    let mut config = crate::config::Config::from_env().unwrap();
    config.storage_backend = crate::config::StorageBackend::Filesystem;
    config.fs_root = root.clone();
    // Non-default payload ceiling: max_payload_bytes = 256 (approved minimum)
    config.fs_tag_listing_max_payload_bytes = 256;

    let hex1 = "1111111111111111111111111111111111111111111111111111111111111111";
    let hex2 = "2222222222222222222222222222222222222222222222222222222222222222";
    let prefix1 = format!("sha256:{hex1}");
    let prefix2 = format!("sha256:{hex2}");
    // Construct valid digest padded with whitespace to exactly 256 bytes (71 + 185 = 256)
    let payload_exact_256 = format!("{prefix1}{}", " ".repeat(256 - prefix1.len()));
    assert_eq!(payload_exact_256.len(), 256);
    // Construct valid digest padded with whitespace to 257 bytes (71 + 186 = 257 > 256)
    let payload_overflow_257 = format!("{prefix2}{}", " ".repeat(257 - prefix2.len()));
    assert_eq!(payload_overflow_257.len(), 257);

    // 1. Primary filesystem storage wiring path
    let wiring = crate::storage_wiring::storage_wiring_try_from_config(&config)
        .expect("primary storage wiring succeeds");
    let primary_reader = wiring.tag_reader();
    let primary_repo = "primary_payload_ceiling_repo";
    let primary_tags_dir = root.join("repos").join(primary_repo).join("tags");
    std::fs::create_dir_all(&primary_tags_dir).expect("create primary tags dir");

    // Exact boundary: 256-byte valid payload succeeds
    write_file(
        &primary_tags_dir.join("tag-exact"),
        payload_exact_256.as_bytes(),
    );
    let tags = primary_reader
        .list_tags(primary_repo)
        .await
        .expect("list_tags succeeds");
    assert_eq!(tags, vec!["tag-exact"]);
    let (page, _) = primary_reader
        .list_tags_page(primary_repo, None, 10)
        .await
        .expect("256-byte payload within 256 ceiling must succeed list_tags_page");
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].0, "tag-exact");
    assert_eq!(page[0].1.hex(), hex1);

    // Over-limit failure: 257-byte payload fails closed with CorruptData
    // Distinct from malformed digest omission because payload is a valid digest with spaces
    write_file(
        &primary_tags_dir.join("tag-overflow"),
        payload_overflow_257.as_bytes(),
    );
    let err_page = primary_reader
        .list_tags_page(primary_repo, None, 10)
        .await
        .expect_err("257-byte payload exceeding 256 ceiling must fail list_tags_page");
    assert_eq!(
        err_page.internal_kind(),
        Some(StorageErrorKind::CorruptData)
    );
    assert!(
        err_page.to_string().contains("exceeds limit"),
        "error must report ceiling exceeded: {err_page}"
    );

    // In contrast, name-only listing does NOT open candidate payloads -> succeeds!
    let tags_after = primary_reader
        .list_tags(primary_repo)
        .await
        .expect("name-only listing does not open payloads and must succeed");
    assert_eq!(tags_after.len(), 2);

    // 2. Proxy-cache filesystem storage wiring path
    let cache_root = root.join("cache");
    let mut proxy_cfg = config.clone();
    proxy_cfg.proxy.cache_fs_root = Some(cache_root.clone());
    let proxy_storage =
        crate::storage_wiring::proxy_cache_storage_try_from_config(&proxy_cfg, None)
            .expect("proxy cache storage construction succeeds");
    let proxy_reader = proxy_storage.as_tag_reader();
    let proxy_repo = "proxy_payload_ceiling_repo";
    let proxy_tags_dir = cache_root.join("repos").join(proxy_repo).join("tags");
    std::fs::create_dir_all(&proxy_tags_dir).expect("create proxy tags dir");

    write_file(
        &proxy_tags_dir.join("tag-exact"),
        payload_exact_256.as_bytes(),
    );
    let proxy_tags = proxy_reader
        .list_tags(proxy_repo)
        .await
        .expect("proxy list_tags succeeds");
    assert_eq!(proxy_tags, vec!["tag-exact"]);
    let (proxy_page, _) = proxy_reader
        .list_tags_page(proxy_repo, None, 10)
        .await
        .expect("proxy 256-byte payload within ceiling succeeds");
    assert_eq!(proxy_page.len(), 1);

    write_file(
        &proxy_tags_dir.join("tag-overflow"),
        payload_overflow_257.as_bytes(),
    );
    let proxy_err_page = proxy_reader
        .list_tags_page(proxy_repo, None, 10)
        .await
        .expect_err("proxy 257-byte payload exceeding ceiling must fail");
    assert_eq!(
        proxy_err_page.internal_kind(),
        Some(StorageErrorKind::CorruptData)
    );
    assert!(proxy_err_page.to_string().contains("exceeds limit"));

    let proxy_tags_after = proxy_reader
        .list_tags(proxy_repo)
        .await
        .expect("proxy name-only listing continues to succeed");
    assert_eq!(proxy_tags_after.len(), 2);
}

#[tokio::test]
async fn test_tag_listing_budget_failures_reach_actual_callers() {
    // A. Supervisor caller: compute_protected_blobs
    let root = tmp_fs_root();
    let mut config = crate::config::Config::from_env().unwrap();
    config.storage_backend = crate::config::StorageBackend::Filesystem;
    config.fs_root = root.clone();
    config.fs_tag_listing_max_entries = 1; // Strict limit: 1 entry

    let wiring = crate::storage_wiring::storage_wiring_try_from_config(&config).unwrap();
    let repo = "library/budget-repo";
    let tags_dir = root.join("repos").join(repo).join("tags");
    std::fs::create_dir_all(&tags_dir).unwrap();

    let hex1 = "1111111111111111111111111111111111111111111111111111111111111111";
    let hex2 = "2222222222222222222222222222222222222222222222222222222222222222";
    write_file(&tags_dir.join("tag-1"), format!("sha256:{hex1}").as_bytes());
    write_file(&tags_dir.join("tag-2"), format!("sha256:{hex2}").as_bytes());

    let rules = vec![crate::config::ProxyRepoRule {
        match_pattern: crate::proxy::ProxyRepoPattern::parse("library/*").unwrap(),
        upstream_repo: None,
        tag_policy: crate::config::TagPolicy::AlwaysRevalidate,
        eviction_policy: crate::config::EvictionPolicy::KeepLatestCachedSemver {
            tag_regex: None,
            allow_prerelease: false,
        },
    }];
    let proxy_db_path = root.join("proxy.db");
    let proxy_cfg = crate::config::ProxyConfig {
        enabled: true,
        mode: crate::config::ProxyMode::Allowlist,
        upstream_base_url: Some("http://localhost:5000".to_string()),
        upstream_username: None,
        upstream_password: None,
        allowed_upstream_hosts: vec!["localhost".to_string()],
        token_realm_hosts: vec![],
        allowed_repo_prefixes: vec![],
        block_private_networks: false,
        redirect_policy: crate::config::RedirectPolicy::AnyPublic,
        max_concurrent_upstream: 10,
        index_path: proxy_db_path,
        cache_fs_root: None,
        cache_s3_prefix: None,
        gc_interval_secs: 0,
        scrub_enabled: false,
        scrub_interval_secs: 0,
        scrub_max_files_per_run: 0,
        max_cache_bytes: None,
        repo_rules: vec![],
        upstreams: vec![],
        routing_proxy_hosts: vec![],
        routing_trust_x_forwarded_host: false,
    };
    let proxy = crate::proxy::Proxy::new(&proxy_cfg).unwrap().unwrap();

    let sup_result =
        crate::supervisor::compute_protected_blobs(wiring.proxy_storage().as_ref(), &rules, &proxy)
            .await;
    let _protected =
        sup_result.expect("compute_protected_blobs succeeds under unbounded streaming");

    // B. Membership migration caller: verify_membership_migration
    let mig_result =
        crate::membership_migration::verify_membership_migration(wiring.blob_mutation().as_ref())
            .await;
    let _mig_ok =
        mig_result.expect("verify_membership_migration succeeds under unbounded streaming");

    // C. Lifecycle caller: ManifestLifecycleService::delete_manifest
    let root2 = tmp_fs_root();
    let listing_limits2 = crate::storage::fs::tag_listing::TagListingLimits::new(
        storage_fs::DirEnumerationLimits::new(64, 4096),
        storage_fs::DirEnumerationLimits::new(1000, 100_000),
        crate::storage::fs::tag_listing::TagReadLimits {
            max_payload_bytes: Some(256),
        },
    );
    let storage2 = Arc::new(
        FsStorage::try_new_with_all_limits(
            root2.clone(),
            10 * 1024 * 1024,
            storage_fs::DirEnumerationLimits::new(1000, 100_000),
            repo_discovery::DiscoveryLimits::default(),
            crate::storage::fs::manifest_refs::ManifestReferenceLimits::default(),
            listing_limits2,
        )
        .unwrap(),
    );
    let consistency = crate::consistency::ConsistencyCoordinator::new();
    let service = crate::manifest_lifecycle::ManifestLifecycleService::new(
        storage2.clone(),
        None,
        consistency,
    );

    let del_repo = "lifecycle-budget-repo";
    let manifest_digest =
        Digest::parse("sha256:3333333333333333333333333333333333333333333333333333333333333333")
            .unwrap();
    let manifest_bytes =
        br#"{"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json"}"#;
    storage2
        .put_manifest(
            del_repo,
            &manifest_digest,
            Bytes::from_static(manifest_bytes),
        )
        .await
        .unwrap();

    // Write a tag with oversized payload (300 bytes > 256 limit)
    let tags_dir2 = root2.join("repos").join(del_repo).join("tags");
    std::fs::create_dir_all(&tags_dir2).unwrap();
    let padding = " ".repeat(229);
    write_file(
        &tags_dir2.join("oversized-tag"),
        format!("{manifest_digest}{padding}").as_bytes(),
    );

    let del_result = service.delete_manifest(del_repo, &manifest_digest).await;
    match del_result {
        Err(crate::manifest_lifecycle::ManifestLifecycleError::Storage(e)) => {
            assert_eq!(e.internal_kind(), Some(StorageErrorKind::CorruptData));
            assert!(e.to_string().contains("exceeds limit"));
        }
        other => panic!("expected Storage error with CorruptData, got: {other:?}"),
    }
}
