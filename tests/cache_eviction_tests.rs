//! R2 commit-1 acceptance: the port-based cache enumeration sees exactly the
//! blobs the historical ambient `read_dir` walker saw (parity on FS), and the
//! same surface works on S3 — the capability the old walker could never have.

use registry_rust::registry::digest::Digest;
use registry_rust::storage::fs::FsStorage;
use registry_rust::storage::ports::CacheEvictionPort;
use sha2::Digest as _;
use std::collections::BTreeMap;

fn write_cache_blob(root: &std::path::Path, payload: &[u8]) -> (String, u64) {
    let hex = hex::encode(sha2::Sha256::digest(payload));
    let dir = root.join("blobs").join("sha256").join(&hex[0..2]);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(&hex), payload).unwrap();
    (hex, payload.len() as u64)
}

/// The candidate set the pre-R2 ambient walker would have produced:
/// every regular file under blobs/sha256/<prefix>/ whose name parses as a
/// sha256 digest.
fn walker_reference(root: &std::path::Path) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    let blobs_root = root.join("blobs").join("sha256");
    let Ok(prefixes) = std::fs::read_dir(&blobs_root) else {
        return out;
    };
    for prefix in prefixes.flatten() {
        if !prefix.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let Ok(dir) = std::fs::read_dir(prefix.path()) else {
            continue;
        };
        for ent in dir.flatten() {
            if !ent.file_type().map(|t| t.is_file()).unwrap_or(false) {
                continue;
            }
            let Some(name) = ent
                .path()
                .file_name()
                .and_then(|s| s.to_str())
                .map(String::from)
            else {
                continue;
            };
            if Digest::parse(&format!("sha256:{name}")).is_err() {
                continue;
            }
            out.insert(name, ent.metadata().unwrap().len());
        }
    }
    out
}

#[tokio::test]
async fn fs_port_enumeration_matches_the_old_walker() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().to_path_buf();

    // Real cached blobs.
    let mut expected = BTreeMap::new();
    for payload in [
        &b"cache blob one"[..],
        b"cache blob two",
        b"cache blob three",
    ] {
        let (hex, size) = write_cache_blob(&root, payload);
        expected.insert(hex, size);
    }
    // An empty valid-hex shard dir is tolerated (the old walker also yielded
    // nothing for it). ANY malformed entry instead fails closed in the
    // contained listing — covered separately below.
    std::fs::create_dir_all(root.join("blobs/sha256/aa")).unwrap();

    assert_eq!(
        walker_reference(&root),
        expected,
        "reference fixture sanity"
    );

    let storage = FsStorage::new(root.clone(), 64 * 1024 * 1024);
    let mut listed = BTreeMap::new();
    let mut cursor = None;
    loop {
        let page = storage
            .list_cache_blobs_page(cursor.as_ref(), 2)
            .await
            .expect("port listing");
        for item in page.items {
            listed.insert(item.digest.hex().to_string(), item.size);
        }
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    assert_eq!(
        listed, expected,
        "port enumeration must match the historical walker's candidate set"
    );
}

/// Intentional strictness difference vs. the historical ambient walker (which
/// silently skipped anything unexpected): the cache root is process-owned, so
/// ANY malformed entry indicates corruption and the contained listing FAILS
/// CLOSED (root-level files, non-hex prefix dirs, non-digest leaves alike).
/// The eviction worker logs and retries instead of guessing.
#[tokio::test]
async fn fs_port_enumeration_fails_closed_on_malformed_entries() {
    for junk in [
        ("blobs/sha256/ab_not_a_shard", false),
        ("blobs/sha256/zz", true),
        ("blobs/sha256/ab/nothexname", false),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        write_cache_blob(&root, b"legit");
        let (path, is_dir) = junk;
        let full = root.join(path);
        if is_dir {
            std::fs::create_dir_all(&full).unwrap();
        } else {
            std::fs::create_dir_all(full.parent().unwrap()).unwrap();
            std::fs::write(&full, b"junk").unwrap();
        }

        let storage = FsStorage::new(root, 64 * 1024 * 1024);
        let err = storage.list_cache_blobs_page(None, 10).await.unwrap_err();
        assert_eq!(
            err.internal_kind(),
            Some(registry_rust::storage::StorageErrorKind::CorruptData),
            "junk {path} must fail closed"
        );
    }
}

#[tokio::test]
async fn fs_evict_cache_blob_unlinks_and_is_version_conditional() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().to_path_buf();
    let (hex, _) = write_cache_blob(&root, b"evict me");
    let digest = Digest::parse(&format!("sha256:{hex}")).unwrap();

    let storage = FsStorage::new(root.clone(), 64 * 1024 * 1024);
    let page = storage.list_cache_blobs_page(None, 10).await.unwrap();
    assert_eq!(page.items.len(), 1);
    let enumerated = &page.items[0];

    // Stale-version delete is refused.
    let stale = registry_rust::storage::BlobObjectVersion("bogus".to_string());
    let refused = storage
        .evict_cache_blob(&digest, Some(&stale))
        .await
        .unwrap();
    assert!(matches!(
        refused,
        registry_rust::storage::GcDeleteResult::PreconditionFailed { .. }
    ));
    assert!(
        root.join("blobs/sha256")
            .join(&hex[0..2])
            .join(&hex)
            .exists()
    );

    // Matching version deletes the file.
    let deleted = storage
        .evict_cache_blob(&digest, Some(&enumerated.version))
        .await
        .unwrap();
    assert!(matches!(
        deleted,
        registry_rust::storage::GcDeleteResult::Deleted
    ));
    assert!(
        !root
            .join("blobs/sha256")
            .join(&hex[0..2])
            .join(&hex)
            .exists()
    );

    // Second delete reports NotFound.
    let missing = storage
        .evict_cache_blob(&digest, Some(&enumerated.version))
        .await
        .unwrap();
    assert!(matches!(
        missing,
        registry_rust::storage::GcDeleteResult::NotFound
    ));
}
