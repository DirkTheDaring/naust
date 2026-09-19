> **Note (2026-09-19, documentation reconciliation at `master` `2718bc16`):** this file holds the canonical O-05 *definition* wording; gate STATUS is tracked only in [acceptance-gates.md](../../technical-debt.md) (GATE-O05). The uncontained-read list below was found empty at `2718bc16` by the 2026-09-19 audit; closure acceptance remains OPEN.

# Architecture Assessment: Remaining Filesystem Read-Containment Gaps

> **Historical snapshot — not HEAD remaining work.** Written at `2fc21aab`. Later `master` contained CAS/manifest/tag/referrer/catalog/membership/journal reads, mutation cutover, and the upload reaper. There is no production `list_tag_files`. Current residual inventory: [`current-state.md`](current-state.md). Index: [`README.md`](README.md).

- **Document:** `docs/architecture/filesystem-read-containment-remaining-gaps.md`
- **Status:** Historical read-only assessment (superseded as current inventory by `current-state.md`)
- **Canonical Quality Gates:** `O-03`, `O-04`, `O-05`, `O-06`, `O-13`, `O-15`, `O-16`, and filesystem-doc `D-06` remain explicitly **OPEN** as *acceptance* criteria, not as “cutovers did not happen.”
- **Registry-Rust Baseline:** `2fc21aabdae9c64ba1dd8b3d8c1a1cbc69ddcb2e`
- **Storage-Layer-Rust Baseline:** `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`

---

## 1. Executive Summary & Purpose

Following the local commit of contained filesystem GC manifest discovery in commit [`2fc21aabdae9c64ba1dd8b3d8c1a1cbc69ddcb2e`](file:///home/dietmar/devel/rust/registry-rust), garbage collection protected-set discovery operates beneath a pinned directory descriptor via [`storage_fs::FsMetadataReader`](file:///home/dietmar/devel/rust/storage-layer-rust/crates/storage-fs/src/reader.rs).

Completing this cutover does not satisfy broader filesystem read containment under canonical Quality Gate **O-05 (Broader filesystem read containment)**. Multiple production filesystem read paths in [`FsStorage`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs) remain uncontained, continuing to execute ambient path-based operations via `tokio::fs` or `std::fs`.

This document performs a read-only architectural source assessment across the production codebase:
1. Classifies production filesystem read paths across six functional areas, recording exact entry points, callers, filesystem operations, descriptor pinning status, error propagation, resource limits, and test coverage.
2. Distinguishes observation-only read paths from reads embedded in stateful mutation workflows, analyzing callers and root coherence implications.
3. Reconciles completed milestones with Quality Gate **O-05**, demonstrating why O-05 remains **OPEN**.
4. Recommends the single smallest next step: **Tag-Read Characterization (`resolve_tag` & `get_tag_with_version`)**, freezing current production behavior in controlled test fixtures without changing production routing or prematurely enforcing unapproved containment policies.

---

## 2. Mechanical Source File Evidence Inventory

All findings, line ranges, and source excerpts in this assessment are mechanically derived from verified repository states:

| Repository | File Path | Line Range | SHA-256 Checksum |
| :--- | :--- | :--- | :--- |
| `registry-rust` | [`src/storage/fs.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs) | 1–3691 | `1b7a234e35471a700a19892012367ee2547c674c416f79268ed2f40335347845` |
| `registry-rust` | [`src/storage/mod.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/mod.rs) | 1–770 | `db6320f52288283ba9efd3c631900b9f24c5447e67a1bbd1a66e68111faa29fa` |
| `registry-rust` | [`src/storage/fs/read_adapter.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/read_adapter.rs) | 1–280 | `7e31c3bd7e38ef7ab86f0388810bacc8c18ad8b68e8223b7470c4f8001d222e6` |
| `registry-rust` | [`src/storage/fs/manifest.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/manifest.rs) | 1–230 | `202e9b9104bc4f70e9eb533de1e020373b7cfc9e82aa9c69d7d6fdfcd68c86a1` |
| `registry-rust` | [`src/storage/fs/listing.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/listing.rs) | 1–740 | `d4bc75ab359259b0e932f17b9232a0766a28a10173486612e2ea4a965cd47217` |
| `registry-rust` | [`src/storage/fs/manifest_listing.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/manifest_listing.rs) | 1–365 | `d44b0b024be2a6e23d29a186abf70c029d412446c1fe576cd979726930a32bf8` |
| `registry-rust` | [`src/storage/fs/repo_discovery.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/repo_discovery.rs) | 1–440 | `449365ffca516b2a913efc3b65122ab82f8f0c9d74a29859f86b9080d25807e4` |
| `registry-rust` | [`src/storage/fs/manifest_refs.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/manifest_refs.rs) | 1–620 | `69f00e22a7a3b8b6dc9d809d32e872ae275af230d4bf47ffe05f888c442ed300` |
| `registry-rust` | [`src/storage/repo_membership.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/repo_membership.rs) | 1–360 | `f1b0a5d172db3d5ef374b256885ef85c74284552ef7f291aa8986a734c11d58c` |
| `registry-rust` | [`src/manifest_lifecycle.rs`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs) | 1–2010 | `e11491351474b2d03179c7c0939bbb60041647ea2088ec6b8894b78efd23886b` |
| `registry-rust` | [`src/blob_gc/mod.rs`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/mod.rs) | 1–770 | `045dbc368414d4b959aa892dfb2fbb4f0ac4525891c38780e694d955d8c38e2e` |
| `registry-rust` | [`src/blob_gc/policy.rs`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/policy.rs) | 1–850 | `e425905972f18c53546ae2eabdee38a4ad0768dcd5516625ac375b50152217a9` |
| `storage-layer-rust` | [`crates/storage-fs/src/reader.rs`](file:///home/dietmar/devel/rust/storage-layer-rust/crates/storage-fs/src/reader.rs) | 1–880 | `d5b797e28a975ea0583e82ca82ac2ee9da969873feb684f691e13c4b0ee41c5b` |
| `storage-layer-rust` | [`crates/storage-fs/src/dir.rs`](file:///home/dietmar/devel/rust/storage-layer-rust/crates/storage-fs/src/dir.rs) | 1–550 | `061bc61d8ff2e01d8158a0a0c118e8441410b8903d8f306e431ef7624e28df32` |

---

## 3. Classification of Production Filesystem Reads

### Area 1: Blob and Manifest HEAD/GET

#### 1.1 CAS Blob Reads (`head_blob`, `open_blob`)
- **Status:** **Contained** (Cutover Complete).
- **Entry Points & Callers:**
  - `Storage::head_blob` ([`src/storage/fs.rs:926-929`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L926-L929)) and `Storage::open_blob` ([`src/storage/fs.rs:931-937`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L931-L937)).
  - Primary callers: OCI blob pull HTTP endpoints (`GET /v2/<name>/blobs/<digest>`, `HEAD /v2/<name>/blobs/<digest>`), GC candidate verification.
- **Filesystem Operations:**
  - `head_blob`: Delegates to `FsBlobCasReadAdapter::head_blob` -> `FsMetadataReader::get_metadata`. Opens file via Linux `openat2` (`O_PATH | RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`), verifies `S_IFREG`, and stats descriptor.
  - `open_blob`: Delegates to `FsBlobCasReadAdapter::open_blob` -> `FsMetadataReader::open_payload`. Reopens file via `/proc/self/fd/<fd>`, verifies device and inode identity match, returning streaming `AsyncRead`.
- **Resolution Type:** **Pinned-Descriptor Resolution** (pinned to storage root descriptor).
- **Error Propagation:**
  - Strongly typed `ReadError` translated to `StorageError` via `translate_read_error`.
  - Non-existent blobs return `StorageError::NotFound`.
  - Resolution rejections (`EXDEV`, `ELOOP`) map to `StorageError::io` without quarantine fallback.
- **Resource Limits:** Per-stream memory is bounded by internal read buffer size; aggregate memory usage scales linearly with concurrent streaming reads (`N_concurrent * buffer_size`).
- **Relationship to Mutation/GC:** Observation-only. Does not mutate storage.
- **Test Evidence & Gaps:** Characterized and verified under Linux `openat2` in `src/storage/fs/tests.rs` (lines 2200–2860). Untested behavior: OS file descriptor exhaustion under high concurrent load.

```rust
// [src/storage/fs.rs:926-937]
    async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError> {
        use crate::storage::ports::BlobCasReader;
        self.read_adapter.head_blob(digest).await
    }

    async fn open_blob(
        &self,
        digest: &Digest,
    ) -> Result<(BlobMeta, std::pin::Pin<Box<dyn AsyncRead + Send>>), StorageError> {
        use crate::storage::ports::BlobCasReader;
        self.read_adapter.open_blob(digest).await
    }
```

#### 1.2 Manifest Reads (`head_manifest`, `get_manifest`)
- **Status:** **Contained** (Cutover Complete).
- **Entry Points & Callers:**
  - `Storage::head_manifest` ([`src/storage/fs.rs:988-994`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L988-L994)) and `Storage::get_manifest` ([`src/storage/fs.rs:996-1002`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L996-L1002)).
  - Primary callers: OCI manifest pull HTTP endpoints (`GET /v2/<name>/manifests/<reference>`), manifest lifecycle validation, catalog reconciliation.
- **Filesystem Operations:**
  - Pre-validates repo path safety against control characters, `..`, and repeated slashes via `manifest_key`.
  - Opens payload once via `self.reader.open_payload(&key)` using `openat2` and procfs reopen.
  - Drains payload stream into memory buffer to parse JSON `mediaType` via `detect_manifest_media_type`.
- **Resolution Type:** **Pinned-Descriptor Resolution**.
- **Error Propagation:** Corrupted JSON maps to `StorageErrorKind::CorruptData`. Missing manifests return `StorageError::NotFound`. Traversal attempts return `StorageError::InvalidRepoName`.
- **Resource Limits:** Manifest bytes are fully buffered in memory per request to parse `mediaType`; no hard size limit enforced during stream drain.
- **Relationship to Mutation/GC:** Pure observation.
- **Test Evidence & Gaps:** Tested in `src/storage/fs/tests.rs` (lines 3700–4100). Untested behavior: concurrent file truncation while stream drain is in progress.

```rust
// [src/storage/fs.rs:988-1002]
    async fn head_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<ManifestMeta, StorageError> {
        manifest::head_manifest_impl(self.reader.as_ref(), name, digest).await
    }

    async fn get_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<(ManifestMeta, bytes::Bytes), StorageError> {
        manifest::get_manifest_impl(self.reader.as_ref(), name, digest).await
    }
```

---

### Area 2: CAS and Manifest Listing

#### 2.1 CAS Blob Listing (`list_cas_blobs_page`)
- **Status:** **Contained** (Cutover Complete).
- **Entry Points & Callers:**
  - `GcStorage::list_cas_blobs_page` ([`src/storage/fs.rs:3461-3473`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L3461-L3473)).
  - Primary caller: `blob_gc::traverser::CasBlobTraverser::next_page` ([`src/blob_gc/traverser.rs:57`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/traverser.rs#L57)) during GC sweep candidate generation (`blob_gc::policy::run_sweep_phase`). Internal test caller: `list_cas_blobs_page_with_budgets` ([`src/storage/fs.rs:410`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L410)).
- **Filesystem Operations:**
  - Enumerates sharded directory structure `blobs/sha256/<prefix2>` using descriptor-pinned `CasDirEnumerator::enumerate_dir`.
  - Inspects file metadata with `CasMetadataInspector::inspect_file_metadata`.
- **Resolution Type:** **Pinned-Descriptor Resolution**.
- **Resource Limits:** Enforces precise budget dimensions via `FsListingBudgets` ([`src/storage/fs/listing.rs:302-335`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/listing.rs#L302-L335)):
  - Root directory (`blobs/sha256`): `DEFAULT_ROOT_MAX_ENTRIES = 512`, `DEFAULT_ROOT_MAX_NAME_BYTES = 16_384` (16 KB).
  - Shard directory (`blobs/sha256/<p2>`): `DEFAULT_SHARD_MAX_ENTRIES = 100_000`, `DEFAULT_SHARD_MAX_NAME_BYTES = 8_388_608` (8 MB).
- **Error Propagation:** Fails closed on directory resolution errors or entry disappearance between listing and inspection.
- **Relationship to Mutation/GC:** Observation-only input into GC candidate generation.
- **Test Evidence & Gaps:** Verified in `src/storage/fs/tests.rs` (lines 3040–3400).

```rust
// [src/storage/fs.rs:3461-3473]
    async fn list_cas_blobs_page(
        &self,
        cursor: Option<&GcCursor>,
        limit: usize,
    ) -> Result<GcBlobPage, StorageError> {
        listing::list_cas_blobs_page_impl(
            self.reader.as_ref(),
            cursor,
            limit,
            listing::FsListingBudgets::default(),
        )
        .await
    }
```

#### 2.2 Manifest Digest Listing (`list_manifest_digests_page`)
- **Status:** **Contained** (Cutover Complete).
- **Entry Points & Callers:**
  - `Storage::list_manifest_digests_page` ([`src/storage/fs.rs:1156-1170`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1156-L1170)).
  - Primary callers: Manifest lifecycle management ([`src/manifest_lifecycle.rs:480`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs#L480)), repository reference indexing ([`src/blob_ref_index.rs:728`](file:///home/dietmar/devel/rust/registry-rust/src/blob_ref_index.rs#L728)).
- **Filesystem Operations:**
  - Validates repository name grammar.
  - Opens `repos/<repo>/manifests` via descriptor-pinned `enumerate_dir`.
  - Supports **both lowercase SHA-256 (64-hex) and SHA-512 (128-hex) filenames** ([`src/storage/fs/manifest_listing.rs:890-905`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/manifest_listing.rs#L890-L905)).
  - Enforces continuation token ordering.
- **Resolution Type:** **Pinned-Descriptor Resolution**.
- **Resource Limits:** Enforces entry count and total name byte length bounds via `DirEnumerationLimits` (`self.manifest_listing_limits`).
- **Error Propagation:** Missing manifest directory returns empty page `Ok((vec![], None))`. Malformed repo names return `InvalidRepoName`.
- **Relationship to Mutation/GC:** Observation input for lifecycle and index synchronization.
- **Test Evidence & Gaps:** Verified in `src/storage/fs/tests.rs` (lines 4580–5000).

```rust
// [src/storage/fs.rs:1156-1170]
    async fn list_manifest_digests_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<Digest>, Option<String>), StorageError> {
        manifest_listing::list_manifest_digests_page_impl(
            self.reader.as_ref(),
            repo,
            continuation_token,
            page_limit,
            self.manifest_listing_limits,
        )
        .await
    }
```

---

### Area 3: GC Manifest Reference Discovery

- **Status:** **Contained** (Cutover Complete in Commit `2fc21aabdae9c64ba1dd8b3d8c1a1cbc69ddcb2e`).
- **Entry Points & Callers:**
  - `GcStorage::discover_manifest_references` ([`src/storage/fs.rs:3666-3676`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L3666-L3676)).
  - Primary caller: `blob_gc::policy::build_manifest_protected_set` ([`src/blob_gc/policy.rs:175`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/policy.rs#L175)).
- **Filesystem Operations:**
  - Calls `manifest_refs::collect_manifest_references_end_to_end` ([`src/storage/fs/manifest_refs.rs:523-533`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/manifest_refs.rs#L523-L533)).
  - Discovers terminal manifest directories via `repo_discovery::discover_manifest_dirs_impl` using `enumerate_dir`.
  - Enumerates manifest files in each terminal directory via `reader.enumerate_dir(Some(dir_key), limits.per_dir_limits)` ([`src/storage/fs/manifest_refs.rs:396`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/manifest_refs.rs#L396)).
  - Opens manifest payloads via `ObjectPayloadReader::open_payload`, parses JSON, and records referenced digests into a flat `HashSet<Digest>`.
  - **Note on Graph Mechanics:** The discovery phase records a flat set of referenced blob and subject digests (`protected_digests: HashSet<Digest>`); it does **not** construct or persist an in-memory reachability graph.
- **Resolution Type:** **Pinned-Descriptor Resolution**.
- **Error Propagation:** Fail-closed semantics. Budget exhaustion, path resolution rejections, or corrupt manifests fail GC discovery, preventing destructive quarantine or deletion.
- **Resource Limits:** Bounded by `DiscoveryLimits` (depth, enumerations, total entries, manifest dirs, retained path bytes) and `ManifestReferenceLimits`.
- **Relationship to Mutation/GC:** Authority input for GC planning. Fails closed to protect valid assets.
- **Test Evidence & Gaps:** Integration verified with end-to-end fixtures in `src/storage/fs/tests.rs` and `src/blob_gc/policy.rs`.

```rust
// [src/storage/fs.rs:3666-3676]
    async fn discover_manifest_references(
        &self,
    ) -> Result<Option<std::collections::HashSet<Digest>>, StorageError> {
        let obs = manifest_refs::collect_manifest_references_end_to_end(
            self.reader.as_ref(),
            self.gc_discovery_limits,
            self.gc_ref_limits.clone(),
        )
        .await?;
        Ok(Some(obs.protected_digests))
    }
```

---

### Area 4: Repository Catalog Discovery

#### 4.1 Repository Enumeration (`list_repositories`, `list_repo_names`)
- **Status:** **UNCONTAINED GAP**.
- **Entry Points & Callers:**
  - `Storage::list_repositories` ([`src/storage/fs.rs:878-880`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L878-L880)) calling private `list_repo_names` ([`src/storage/fs.rs:671-746`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L671-L746)).
  - Callers: OCI catalog route `GET /v2/_catalog` ([`src/http_api/catalog.rs:136`](file:///home/dietmar/devel/rust/registry-rust/src/http_api/catalog.rs#L136)), `Storage::is_storage_empty` ([`src/storage/fs.rs:905`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L905)), `blob_delete_safety` ([`src/blob_delete_safety.rs:93`](file:///home/dietmar/devel/rust/registry-rust/src/blob_delete_safety.rs#L93)), blob reference index rebuild ([`src/blob_ref_index.rs:722`](file:///home/dietmar/devel/rust/registry-rust/src/blob_ref_index.rs#L722)), membership migration ([`src/membership_migration.rs:15`](file:///home/dietmar/devel/rust/registry-rust/src/membership_migration.rs#L15)), runtime supervisor health checks ([`src/supervisor.rs:915`](file:///home/dietmar/devel/rust/registry-rust/src/supervisor.rs#L915)), GC policy fallback ([`src/blob_gc/policy.rs:355`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/policy.rs#L355)).
- **Filesystem Operations:**
  - Constructs `self.root.join("repos")`.
  - Executes iterative directory traversal via `tokio::fs::read_dir` using an in-memory stack (`Vec<(PathBuf, String)>` with `while let Some((dir_path, rel)) = stack.pop()`).
  - For every directory visited, executes four ambient `tokio::fs::metadata` calls to test for child subdirectories: `tags/`, `manifests/`, `blobs/`, `meta/`.
- **Resolution Type:** **Ambient Pathname Resolution (Uncontained)**.
- **Symlink Semantics Distinction:**
  - **Initial / Ancestor Paths:** OS pathname resolution follows symlinks on `repos_root` or ancestor components. If `repos/` is a symlink pointing outside storage root, `tokio::fs::read_dir` traverses the external directory.
  - **Child Directory Entries:** Child dirents encountered during `read_dir` are filtered by `if !file_type.is_dir() { continue; }`. In Linux, a symlink dirent has `file_type.is_symlink() == true` and `file_type.is_dir() == false`, so intermediate symlinks are skipped during dirent iteration.
  - **Leaf Recognition Directories:** However, leaf recognition uses `tokio::fs::metadata(&tags_dir)`, which executes `stat()` and follows symlinks. A symlinked `tags` leaf pointing anywhere is recognized as a valid repository indicator.
- **Resource Limits & Actual Mechanics:**
  - Traversal is **iterative**, not recursive; it does not consume call stack frames.
  - Holds only one directory stream open at a time; does not exhaust file descriptors across traversal depth.
  - Heap usage scales with the number of pending directories enqueued on `stack`.
  - I/O overhead includes four `metadata()` calls per directory visited.
- **Error Propagation:** Missing `repos/` returns `Ok(vec![])`. Unreadable subdirectories return `StorageError::io`. Non-UTF8 entries and non-directories are ignored.
- **Relationship to Mutation/GC:** Observation-only fallback for GC validation and reference index synchronization.
- **Test Evidence & Gaps:** Characterized in `src/storage/fs/tests.rs:5545-6140`.

```rust
// [src/storage/fs.rs:671-746]
    async fn list_repo_names(&self) -> Result<Vec<String>, StorageError> {
        let repos_root = self.root.join("repos");
        let mut repos = Vec::new();

        let mut stack: Vec<(PathBuf, String)> = vec![(repos_root.clone(), String::new())];
        while let Some((dir_path, rel)) = stack.pop() {
            let mut dir = match tokio::fs::read_dir(&dir_path).await {
                Ok(d) => d,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(StorageError::io(err.to_string())),
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
                if name == "tags"
                    || name == "manifests"
                    || name == "referrers"
                    || name == "blobs"
                    || name == "meta"
                {
                    continue;
                }

                let child_path = entry.path();
                let child_rel = if rel.is_empty() {
                    name
                } else {
                    format!("{rel}/{name}")
                };

                // Consider this a repo if it has tags/, manifests/, blobs/, or meta/ directories.
                let tags_dir = child_path.join("tags");
                let manifests_dir = child_path.join("manifests");
                let blobs_dir = child_path.join("blobs");
                let meta_dir = child_path.join("meta");
                let has_tags = tokio::fs::metadata(&tags_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                let has_manifests = tokio::fs::metadata(&manifests_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                let has_blobs = tokio::fs::metadata(&blobs_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                let has_meta = tokio::fs::metadata(&meta_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                if has_tags || has_manifests || has_blobs || has_meta {
                    repos.push(child_rel.clone());
                }

                stack.push((child_path, child_rel));
            }
        }

        repos.sort();
        repos.dedup();
        Ok(repos)
    }
```

#### 4.2 Repository Timestamps and Storage Emptiness
- **Status:** **UNCONTAINED GAP**.
- **Operations:**
  - `Storage::repo_timestamps` ([`src/storage/fs.rs:882-902`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L882-L902)): Path `self.root.join("repos").join(name)`. Calls `max_mtime_in_dir` ([`src/storage/fs.rs:748-774`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L748-L774)) which iterates `tags/` and `manifests/` via raw `tokio::fs::read_dir`.
  - `Storage::is_storage_empty` ([`src/storage/fs.rs:904-925`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L904-L925)): Calls `list_repositories()`, then checks subdirectories `["blobs", "uploads", "quarantine", "repo-blobs", "repo-memberships", "repos", "journals"]` via recursive `fs_dir_has_any_entry` ([`src/storage/fs.rs:835-857`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L835-L857)).
- **Resolution Type:** **Ambient Pathname Resolution (Uncontained)**.

---

### Area 5: Tag and Referrer Reads and Listing

#### 5.1 Tag Direct Reads (`resolve_tag`, `get_tag_with_version`)
- **Status:** **UNCONTAINED GAP**.
- **Entry Points & Callers:**
  - `Storage::resolve_tag` ([`src/storage/fs.rs:939-951`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L939-L951)):
    - Callers: `src/application/manifest_read.rs:92, 177, 293, 307, 320, 368, 384` (resolving tag references to manifest digests during manifest fetches).
    - Callers: `src/application/tags.rs:117` (application tag resolution service).
    - Callers: `src/application/catalog.rs:158` (catalog tag checks).
    - Callers: `src/blob_delete_safety.rs:105, 140` (checking tag references before authorizing blob delete).
    - Callers: `src/blob_ref_index.rs:776, 1284, 1297` (reference index rebuild and synchronization).
    - Callers: `src/membership_migration.rs:21, 129, 218` (discovering manifest digests for membership linking).
    - Callers: `src/supervisor.rs:951` (supervisor tag check).
  - `Storage::get_tag_with_version` ([`src/storage/fs.rs:1250-1269`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1250-L1269)):
    - Callers: `src/manifest_lifecycle.rs:629, 648, 1156, 1401, 1442, 1550`.
    - Specifically, line 1550 in `ManifestLifecycleManager::delete_tag` calls `get_tag_with_version`, records the returned version into the durable lifecycle journal `relevant_tags` snapshot, and passes that version into `delete_tag_conditional(repo, tag, Some(&version))`.
- **Filesystem Operations & Resolution:**
  - Path: `self.tag_path(name, tag)` = `self.root.join("repos").join(name).join("tags").join(tag)`.
  - `resolve_tag`: Ambient `tokio::fs::read_to_string(&path)`.
  - `get_tag_with_version`: Ambient `tokio::fs::read(&path)`.
- **Tag Content Handling Details:**
  - **`resolve_tag` Content Flow:**
    1. Reads via `tokio::fs::read_to_string(&path)`. If the file contains invalid UTF-8 bytes, `read_to_string` fails with `std::io::ErrorKind::InvalidData`, returning `StorageError::io(...)`.
    2. Strips leading and trailing whitespace with `content.trim()`. Trimming removes arbitrary whitespace (spaces, tabs, `\r`, `\n`).
    3. Parses digest via `Digest::parse(reference)`. Supports both lowercase SHA-256 (`sha256:<64-hex>`) and SHA-512 (`sha512:<128-hex>`).
    4. On parse failure (empty content, non-hex, unknown algorithm, malformed text), maps to `StorageError::NotFound` via `.map_err(|_| StorageError::NotFound)`.
  - **`get_tag_with_version` Content Flow:**
    1. Reads raw bytes via `tokio::fs::read(&path)`.
    2. Converts lossily to UTF-8 with `String::from_utf8_lossy(&bytes)`.
    3. Parses digest via `Digest::parse(s.trim())`. Supports SHA-256 and SHA-512.
    4. On parse failure, maps to `StorageError::corrupt_data(...)`.
    5. Computes version by hashing the **original raw `&bytes`** with SHA-256 (`hasher.update(&bytes)`), producing a hex string.
    6. **Version Sensitivity:** Any byte-level modification (trailing newline `\n` vs `\r\n`, extra padding spaces, whitespace differences) produces a completely different version ETag, even when `Digest::parse` yields the exact same parsed `Digest`.
  - **Size Boundaries:** Current code enforces no maximum size on stored tag files before reading; trimming permits arbitrarily long whitespace-padded content. A 72-byte ceiling applies only to a bare single-line SHA-256 tag (`sha256:<64-hex>\n`), not arbitrary tag file contents.

```rust
// [src/storage/fs.rs:939-951]
    async fn resolve_tag(&self, name: &str, tag: &str) -> Result<Digest, StorageError> {
        let path = self.tag_path(name, tag);
        let content = match tokio::fs::read_to_string(&path).await {
            Ok(s) => s,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(err) => return Err(StorageError::io(err.to_string())),
        };
        let reference = content.trim();
        Digest::parse(reference).map_err(|_| StorageError::NotFound)
    }

// [src/storage/fs.rs:1250-1269]
    async fn get_tag_with_version(
        &self,
        repo: &str,
        tag: &str,
    ) -> Result<Option<(Digest, String)>, StorageError> {
        let tag_path = self.tag_path(repo, tag);
        match tokio::fs::read(&tag_path).await {
            Ok(bytes) => {
                let s = String::from_utf8_lossy(&bytes);
                let digest = Digest::parse(s.trim())
                    .map_err(|e| StorageError::corrupt_data(format!("corrupt tag {tag}: {e}")))?;
                let mut hasher = sha2::Sha256::new();
                hasher.update(&bytes);
                let version = hex::encode(hasher.finalize());
                Ok(Some((digest, version)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::io(e.to_string())),
        }
    }
```

#### 5.2 Tag Directory Listing (`list_tags`, `list_tags_page`)
- **Status:** **UNCONTAINED GAP**.
- **Operations:**
  - `Storage::list_tags` ([`src/storage/fs.rs:953-986`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L953-L986)). Callers: OCI tag listing route (`GET /v2/<name>/tags/list`).
  - `Storage::list_tags_page` ([`src/storage/fs.rs:1172-1217`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1172-L1217)). Callers: Paginated tag enumeration.
  - Private helper `list_tag_files` ([`src/storage/fs.rs:646-669`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L646-L669)).
- **Filesystem Operations & Resolution:**
  - Ambient `tokio::fs::read_dir` on `self.root.join("repos").join(name).join("tags")`.
  - In `list_tags_page`: Calls `list_tag_files`, then executes `tokio::fs::read_to_string` on **every single tag file** in the repository, parsing digests into an in-memory vector before sorting and slicing the requested page.
- **Resource Limits:** Number of file reads scales with total tag count in the repository per page request.

#### 5.3 Referrer Reads and Listing (`list_referrers`, `list_referrers_page`)
- **Status:** **UNCONTAINED GAP**.
- **Operations:**
  - `Storage::list_referrers` ([`src/storage/fs.rs:1715-1728`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1715-L1728)) and `list_referrers_page` ([`src/storage/fs.rs:1219-1248`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1219-L1248)). Callers: OCI referrers API (`GET /v2/<name>/referrers/<digest>`).
- **Filesystem Operations:** Reads `self.referrers_path(name, subject)` = `self.root.join("repos").join(name).join("referrers").join(format!("{}.json", subject.hex()))` via ambient `tokio::fs::read`.

---

### Area 6: Metadata, Membership, Journal, and Quarantine Reads

#### 6.1 Repository-Blob Membership Reads
- **Status:** **UNCONTAINED GAP**.
- **Operations:**
  - `RepositoryBlobMembershipStorage::get_repo_blob_membership` ([`src/storage/fs.rs:2867-2890`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L2867-L2890)). Path: `self.repo_blob_path(&canonical, digest)` = `self.root.join("repo-memberships").join("by-repo").join(encode_canonical_repo_key(repo)).join(digest.algorithm()).join(format!("{}.json", digest.hex()))`.
  - `list_repo_blob_memberships_page` ([`src/storage/fs.rs:2947-3075`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L2947-L3075)): Iterates algorithm directories via ambient `read_dir` and reads JSON records.
  - `list_all_repo_blob_memberships_page` ([`src/storage/fs.rs:3077-3236`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L3077-L3236)): Traverses all repositories under `repo-memberships/by-repo`.
  - `count_repo_blob_memberships` ([`src/storage/fs.rs:3238-3255`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L3238-L3255)).
  - `is_membership_ready` ([`src/storage/fs.rs:3257-3269`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L3257-L3269)): Checks `self.root.join("meta").join("membership_ready.json")`.
  - `get_migration_checkpoint` ([`src/storage/fs.rs:3307-3323`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L3307-L3323)): Reads `self.root.join("meta").join("migration_checkpoint.json")`.

#### 6.2 Durable Lifecycle Journal Reads
- **Status:** **UNCONTAINED GAP**.
- **Operations:** `Storage::read_lifecycle_journal` ([`src/storage/fs.rs:1337-1348`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1337-L1348)).
- **Exact Path Verified from Source:**
  - Computes `fs_repo_dir(&self.root, &canonical)?.join("meta").join("lifecycle_journal.json")` ([`src/storage/fs.rs:1340-1342`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1340-L1342)).
  - Resolves to: `self.root.join("repos").join(repo).join("meta").join("lifecycle_journal.json")`.
  - Reads via ambient `tokio::fs::read(&path)`.

#### 6.3 Quarantine Reads & Conditional Blob Deletion
- **Status:** **UNCONTAINED GAP**.
- **Operations & Paths Verified from Source:**
  - `quarantined_blob_version` ([`src/storage/fs.rs:3584-3596`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L3584-L3596)): Checks `self.root.join("quarantine").join("blobs").join(digest.algorithm()).join(digest.prefix2()).join(digest.hex())`.
  - `compute_fs_blob_version` ([`src/storage/fs.rs:3433-3458`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L3433-L3458)): Ambient `tokio::fs::File::open(path)` streaming 64 KB chunks to hash contents into `BlobObjectVersion(format!("fs:{len}:{mtime}:{hash}"))`.
  - `read_quarantine_timestamp` ([`src/storage/fs.rs:3396-3413`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L3396-L3413)): Ambient `tokio::fs::read_to_string` on `self.root.join("quarantine").join("meta").join(digest.algorithm()).join(digest.prefix2()).join(format!("{}.ts", digest.hex()))`.
  - `delete_blob_conditional` ([`src/storage/fs.rs:3600-3655`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L3600-L3655)): Targets the **quarantined blob** path (`self.root.join("quarantine").join("blobs").join(digest.algorithm()).join(digest.prefix2()).join(digest.hex())`), verifying version with `compute_fs_blob_version` before unlinking the quarantined blob and its timestamp metadata.

---

## 4. Observation Reads vs Mutation-Embedded Reads: Concurrency & Coherence

### 4.1 Callers and Mutation Relationships for Tag Reads
While `resolve_tag` is predominantly used in observation paths (manifest pull endpoints), `get_tag_with_version` directly participates in conditional mutation workflows:

- **Primary Mutation Caller:** `ManifestLifecycleManager::delete_tag` ([`src/manifest_lifecycle.rs:1550`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs#L1550)):
  1. Calls `self.storage.get_tag_with_version(repo, tag)`.
  2. Observes `(target_digest, version)`.
  3. Writes `version` into the durable lifecycle journal `relevant_tags: vec![TagSnapshot { tag, observed_version: version.clone(), ... }]`.
  4. Calls `self.storage.delete_tag_conditional(repo, tag, Some(&version))`.
  5. If the version mismatches at deletion time, `delete_tag_conditional` returns `ConditionalDeleteResult::PreconditionFailed`, and lifecycle deletes the journal and returns `ManifestLifecycleError::TagPreconditionFailed`.

### 4.2 Pinned-Read / Pathname-Delete Coherence Analysis
Before proposing promotion of `get_tag_with_version` to descriptor-relative containment, the coherence implications between pinned reads and pathname deletes must be understood:

1. **Root Coherence:**
   - If `get_tag_with_version` were descriptor-pinned (`openat2` relative to `self.reader`) while `delete_tag_conditional` remains ambient path-based (`self.root.join(...)`):
   - A concurrent replacement or relocation of directory components beneath `repos/<repo>/tags` could cause `get_tag_with_version` to resolve one physical file inode via the pinned descriptor while `delete_tag_conditional` locks and unlinks a different file via pathname resolution.
2. **Advisory Locking Invariants:**
   - In `FsStorage`:
     - `mutate_tag` ([`src/storage/fs.rs:1055`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1055)) and `delete_tag_conditional` ([`src/storage/fs.rs:1290`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1290)) take exclusive advisory locks on `.lock.{tag}` via `fs2::FileExt::lock_exclusive()`.
     - `get_tag_with_version` ([`src/storage/fs.rs:1250`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1250)) does **not** acquire `.lock.{tag}`.
   - An interleaved `mutate_tag` executing between `get_tag_with_version` observing the version and `delete_tag_conditional` locking the tag file will safely trigger `PreconditionFailed` because `delete_tag_conditional` checks version under the lock.
   - However, any refactoring of `get_tag_with_version` must account for the fact that its return value is not purely informational: it provides the concurrency precondition token for mutation commits.

---

## 5. Reconciliation with Quality Gate O-05

- **Quality Gate O-05 Definition:** **Broader filesystem read containment**.
- **Scope & Scope Boundaries:** O-05 governs filesystem read operations across the storage layer. It does not dictate filesystem write operations (which are governed by O-04).

### Status Summary
- **Contained Production Read Operations:**
  - CAS blob reads (`head_blob`, `open_blob`)
  - CAS blob listing (`list_cas_blobs_page`)
  - Manifest reads (`head_manifest`, `get_manifest`)
  - Manifest listing (`list_manifest_digests_page`)
  - GC manifest reference discovery (`discover_manifest_references`)
- **Uncontained Production Read Operations Remaining:**
  - Repository catalog discovery (`list_repositories`, `repo_timestamps`, `is_storage_empty`)
  - Tag direct reads (`resolve_tag`, `get_tag_with_version`)
  - Tag directory listing (`list_tags`, `list_tags_page`)
  - Referrer reads and listing (`list_referrers`, `list_referrers_page`)
  - Repository-blob membership reads (`get_repo_blob_membership`, listing, counts)
  - Lifecycle journal reads (`read_lifecycle_journal`)
  - Quarantine reads (`quarantined_blob_version`, timestamps)
  - Reads embedded in mutation workflows (`mutate_tag`, `delete_tag_conditional`, `delete_manifest`, upload sessions)

Because numerous production read paths remain uncontained ambient pathname operations, **Quality Gate O-05 remains explicitly OPEN**.

---

## 6. Smallest Next Step Recommendation: Tag-Read Characterization

We recommend that the next slice be **Tag-Read Characterization (`resolve_tag` & `get_tag_with_version`)**, freezing and characterizing existing production behavior before proposing containment cutover.

### 6.1 Characterization Objectives
1. **Characterize Current Production Behavior:** Execute characterization tests directly against existing `FsStorage` without changing production code, routing, or dependencies.
2. **Do Not Assume Premature Fail-Closed Containment:**
   - Current production code uses ambient `tokio::fs` pathname resolution. It does **not** enforce `RESOLVE_NO_SYMLINKS` or `openat2`.
   - Characterization tests must use controlled temporary fixtures to establish what current code actually does when encountering symlinks, whitespace variations, and non-canonical content, without expecting symlinks or traversal inputs to necessarily fail closed today.
3. **Separate Current Behavior from Future Design Choices:**
   - The following choices must be treated as separate, unapproved future design choices:
     - Strict OCI tag grammar enforcement on read keys.
     - Changing `resolve_tag` parse error mapping from `StorageError::NotFound` to `StorageError::corrupt_data`.
     - Enforcing a 256-byte maximum read ceiling.

### 6.2 Characterization Test Matrix (Using Controlled Fixtures)
Controlled temporary fixtures should be constructed to observe and record:
1. **Missing Tag Outcomes:**
   - `resolve_tag` on non-existent tag -> `Err(StorageError::NotFound)`.
   - `get_tag_with_version` on non-existent tag -> `Ok(None)`.
2. **Standard Digest Formats:**
   - Valid SHA-256 (`sha256:<64-hex>`) with trailing newline `\n`.
   - Valid SHA-256 without trailing newline.
   - Valid SHA-512 (`sha512:<128-hex>`) with and without trailing newline.
3. **Whitespace and Padding Permissiveness:**
   - Leading/trailing whitespace (spaces, tabs, carriage returns `\r\n`).
   - Verify that `trim()` succeeds in parsing `Digest` across these variations.
4. **Invalid UTF-8 Byte Sequences:**
   - Tag file containing raw non-UTF-8 bytes (e.g. `[0xFF, 0xFE]`).
   - Observe `resolve_tag` (`read_to_string` failure -> `StorageError::io`).
   - Observe `get_tag_with_version` (`from_utf8_lossy` -> `Digest::parse` failure -> `StorageError::corrupt_data`).
5. **Malformed Tag Content:**
   - Empty file (0 bytes).
   - Non-digest ASCII text (e.g. `"not-a-digest"`).
   - Observe `resolve_tag` returning `StorageError::NotFound`.
   - Observe `get_tag_with_version` returning `StorageError::corrupt_data`.
6. **Version ETag Sensitivity:**
   - Compare version output of `sha256:<hex>\n` vs `sha256:<hex>` (no newline) vs `sha256:<hex>  \n`.
   - Record that `get_tag_with_version` produces distinct version hashes for raw-byte differences even when parsed `Digest` is identical.
7. **Symlink and Directory Entry Outcomes:**
   - Controlled symlink inside repository tags directory pointing to valid tag file in same directory.
   - Record actual ambient behavior under `tokio::fs` without asserting failure.
8. **Missing Repository:**
   - Reading tag in non-existent repository.

### 6.3 Explicit Exclusions
- **No Production Code Modifications:** `src/storage/fs.rs` remains 100% unchanged during characterization.
- **No Tag Mutation Changes:** `mutate_tag`, `set_tag`, `delete_tag`, and `delete_tag_conditional` remain untouched.
- **No Listing Changes:** `list_tags` and `list_tags_page` remain untouched.
- **No Premature Routing:** Production calls continue routing through existing methods until characterization is complete and reviewed.

---

## 7. Canonical Quality Gate Status

All eight canonical quality gates remain explicitly **OPEN**:

| Quality Gate | Canonical Title | Current Status | Description & Scope |
| :--- | :--- | :--- | :--- |
| **O-03** | Key and continuation-token contracts | **OPEN** | Page limit contracts, memory bounding, and continuation token mechanics active; full ecosystem validation ongoing. |
| **O-04** | Filesystem write durability and containment | **OPEN** | Write durability, synchronization barriers, and directory lock containment remain open. |
| **O-05** | Broader filesystem read containment | **OPEN** | CAS blobs, manifests, and GC discovery contained; catalog, tags, referrers, and membership reads remain uncontained. |
| **O-06** | Typed AWS mapping and pinned-MinIO evidence | **OPEN** | Typed AWS SDK error mapping and MinIO CI test-harness evidence remain open. |
| **O-13** | Hosting, distribution, and release strategy | **OPEN** | Permanent hosting, crate publishing, and release packaging strategy for `storage-layer-rust` pending. |
| **O-15** | Non-Linux verification | **OPEN** | Non-Linux platform fallback behavior and cross-platform verification pending. |
| **O-16** | Earlier Slice 11 audit/test-inventory evidence | **OPEN** | Slice 11 audit evidence, test inventory reconciliation, and accounting open. |
| **D-06** | Broader extraction, cutover, compatibility, and distribution acceptance | **OPEN** | End-to-end extraction acceptance and downstream distribution validation ongoing. |

---

## 8. Summary Conclusion

Production filesystem reads have been assessed and classified against actual source code. GC manifest reference discovery is contained in commit `2fc21aa`. However, multiple uncontained read paths remain across repository catalog discovery, tag/referrer operations, and repository-blob membership. Quality Gate O-05 remains explicitly OPEN.

The recommended immediate next step is **Tag-Read Characterization (`resolve_tag` & `get_tag_with_version`)**, freezing current behavior in controlled test fixtures without changing production code or prematurely declaring containment policies.
