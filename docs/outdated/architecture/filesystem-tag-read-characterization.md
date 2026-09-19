> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Contained tag reads landed (`5a0b424`); tags later moved onto the shared `tag_domain` ObjectStore family (`32c42c6`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Architecture Assessment: Filesystem Tag Read Characterization

- **Document:** `docs/architecture/filesystem-tag-read-characterization.md`
- **Status:** Characterization Record (Corrected) — Production Behavior Unchanged — Not Committed
- **Canonical Quality Gates:** `O-03`, `O-04`, `O-05`, `O-06`, `O-13`, `O-15`, `O-16`, and `D-06` remain explicitly **OPEN**
- **Registry-Rust Baseline:** `2fc21aabdae9c64ba1dd8b3d8c1a1cbc69ddcb2e`
- **Storage-Layer-Rust Baseline:** `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`

---

## 1. Executive Summary & Characterization Scope

This document records the empirical characterization of filesystem tag read and conditional mutation operations in [`FsStorage`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs), specifically:
1. Direct tag resolution: [`FsStorage::resolve_tag`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L939-L951)
2. Version-aware tag retrieval: [`FsStorage::get_tag_with_version`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1250-L1269)
3. Version-dependent conditional tag deletion: [`FsStorage::delete_tag_conditional`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1271-L1335)

Following the recommendation of the architectural assessment ([`docs/architecture/filesystem-read-containment-remaining-gaps.md`](file:///home/dietmar/devel/rust/registry-rust/docs/architecture/filesystem-read-containment-remaining-gaps.md)), this slice performs **characterization only**. Production implementations, configurations, dependencies, and external crates remain completely unchanged. No production routing or containment semantics are altered in this step.

The empirical observations are codified in eleven focused characterization tests in [`src/storage/fs/tests.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/tests.rs), establishing an automated behavioral baseline across ten distinct dimensions:
- Missing tag files and missing repository directories.
- Valid SHA-256 and SHA-512 digest formats, both with and without trailing newlines.
- Arbitrary whitespace handling: spaces, tabs, carriage returns (`\r\n`), and substantial whitespace padding exceeding 256 bytes.
- Error taxonomy divergence on empty files, malformed text, and invalid UTF-8 byte sequences.
- Raw-byte version hashing semantics: whitespace sensitivity and reproducibility.
- Symlink resolution under ambient path semantics: internal symlinks, external symlinks escaping storage root, ancestor directory symlinks, and dangling symlinks.
- Non-regular files (directories in place of tags) and filesystem permission denial (distinguishing expected/source-derived behavior from unexecuted test results under privileged environments).
- Path traversal inputs (`..`) and multi-segment names (`sub/nested_tag`) exercised across both tag and repository arguments.
- Sequential root directory replacement demonstrating pathname observation and explaining split-brain divergence against existing pinned readers.
- The version token's role in conditional tag deletion: precondition validation, file preservation on mismatch, advisory locking, and inability to detect identical-content replacement.

---

## 2. Production Source Inventory & Callers

### 2.1 Direct Production Method Definitions

#### 2.1.1 Tag Read Methods (`resolve_tag` & `get_tag_with_version`)

Both tag read methods reside in [`src/storage/fs.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs):

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

Path construction relies on private helper [`tag_path`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L364-L367):
```rust
// [src/storage/fs.rs:364-367]
fn tag_path(&self, name: &str, tag: &str) -> PathBuf {
    self.root.join("repos").join(name).join("tags").join(tag)
}
```

#### 2.1.2 Actual Conditional Mutation Implementation (`delete_tag_conditional`)

The actual production implementation of [`delete_tag_conditional`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1271-L1335) does **not** call `get_tag_with_version` or `tokio::fs::remove_file`. Instead, it executes an offloaded synchronous blocking routine via `tokio::task::spawn_blocking`:

```rust
// [src/storage/fs.rs:1271-1335]
async fn delete_tag_conditional(
    &self,
    repo: &str,
    tag: &str,
    expected_version: Option<&str>,
) -> Result<super::ConditionalDeleteResult, StorageError> {
    let dir = self.root.join("repos").join(repo).join("tags");
    let path = self.tag_path(repo, tag);
    let lock_path = dir.join(format!(".lock.{tag}"));
    let exp_v = expected_version.map(|s| s.to_string());

    tokio::task::spawn_blocking(move || {
        use fs2::FileExt;
        if let Some(parent) = lock_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let lock_file = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
        {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(super::ConditionalDeleteResult::NotFound);
            }
            Err(e) => return Err(map_fs_io_err(e)),
        };

        lock_file.lock_exclusive().map_err(map_fs_io_err)?;

        let res = (|| {
            let bytes = match std::fs::read(&path) {
                Ok(b) => b,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(super::ConditionalDeleteResult::NotFound);
                }
                Err(e) => return Err(map_fs_io_err(e)),
            };

            if let Some(ref exp) = exp_v {
                let mut hasher = sha2::Sha256::new();
                hasher.update(&bytes);
                let current_version = hex::encode(hasher.finalize());
                if current_version != *exp {
                    return Ok(super::ConditionalDeleteResult::PreconditionFailed {
                        current_version: Some(current_version),
                    });
                }
            }

            std::fs::remove_file(&path).map_err(map_fs_io_err)?;
            if let Ok(dir_file) = std::fs::File::open(&dir) {
                let _ = dir_file.sync_all();
            }
            Ok(super::ConditionalDeleteResult::Deleted)
        })();

        let _ = lock_file.unlock();
        res
    })
    .await
    .map_err(map_blocking_join_error)?
}
```

Key architectural mechanics of `delete_tag_conditional`:
1. **Thread Offloading:** Uses `tokio::task::spawn_blocking` to prevent blocking the async runtime during filesystem locking and I/O.
2. **Advisory Locking:** Acquires an advisory exclusive flock via `fs2::FileExt::lock_exclusive` on a sibling file `.lock.{tag}` in the repository's `tags/` directory.
3. **Raw Byte Hashing (No Parsing):** Reads raw bytes via synchronous `std::fs::read(&path)`. When `expected_version` is `Some`, computes the SHA-256 hex digest of the raw bytes. **The content does not need to parse as a valid OCI digest.** A malformed, non-hex, or padded file is conditionally verified and deleted purely on byte equality.
4. **Precondition & Missing Semantics:** If the file does not exist at read time, returns `ConditionalDeleteResult::NotFound`. If the version does not match `expected_version`, returns `ConditionalDeleteResult::PreconditionFailed { current_version: Some(...) }` and leaves the file intact.
5. **Durability & Cleanup:** Unlinks the tag file synchronously via `std::fs::remove_file(&path)`, executes a best-effort `dir_file.sync_all()` on the parent `tags/` directory, releases the lock via `lock_file.unlock()`, and maps any worker join errors via `map_blocking_join_error`.

### 2.2 Production Call Sites

Production invocation points across the codebase are inventory-verified as follows:

| Target Method | Caller Location | Caller Method & Type | Functional Purpose |
| :--- | :--- | :--- | :--- |
| `resolve_tag` | [`src/application/manifest_read.rs:92, 177, 293, 307, 320, 368, 384`](file:///home/dietmar/devel/rust/registry-rust/src/application/manifest_read.rs) | `ManifestReadService` methods | Resolving tag references to manifest digests during manifest fetch workflows |
| `resolve_tag` | [`src/application/tags.rs:117`](file:///home/dietmar/devel/rust/registry-rust/src/application/tags.rs#L117) | `TagQueryService::resolve_tag` | Application-level tag inspection service |
| `resolve_tag` | [`src/application/catalog.rs:158`](file:///home/dietmar/devel/rust/registry-rust/src/application/catalog.rs#L158) | `CatalogQueryService::tag_platforms_for_repo` | Catalog tag resolution |
| `resolve_tag` | [`src/blob_delete_safety.rs:105, 140`](file:///home/dietmar/devel/rust/registry-rust/src/blob_delete_safety.rs#L105) | Free functions `scan_storage_for_blob` (L105) & `find_repo_blob_reference` (L140) | Verifying tag target existence before authorizing blob deletion |
| `resolve_tag` | [`src/blob_ref_index.rs:776`](file:///home/dietmar/devel/rust/registry-rust/src/blob_ref_index.rs#L776) | `BlobRefIndex::refresh_tag_rooted_conservative` (tests at L1284, L1297) | Reference index reconstruction and active reconciliation |
| `resolve_tag` | [`src/membership_migration.rs:21, 129`](file:///home/dietmar/devel/rust/registry-rust/src/membership_migration.rs#L21) | Free functions `plan_membership_migration` (L21) & `execute_membership_migration` (L129) | Extracting manifest digests for repository-blob membership linking |
| `resolve_tag` | [`src/supervisor.rs:951`](file:///home/dietmar/devel/rust/registry-rust/src/supervisor.rs#L951) | Free function `compute_protected_blobs` in `src/supervisor.rs` | Background supervisor repository consistency check |
| `get_tag_with_version` | [`src/manifest_lifecycle.rs:629, 648`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs#L629) | `ManifestLifecycleService::recover_pending_journal_under_lock` | Inspecting tag versions during journal replay for `DeleteTag` and `ProxyEvict` |
| `get_tag_with_version` | [`src/manifest_lifecycle.rs:1156`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs#L1156) | `ManifestLifecycleService::evict_proxy_cached_entry` | Snapshotting target tag version into `journal.relevant_tags` before proxy cache eviction |
| `get_tag_with_version` | [`src/manifest_lifecycle.rs:1401, 1442`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs#L1401) | `ManifestLifecycleService::delete_manifest` | Observing tag versions for batch tag snapshotting and retry loops |
| `get_tag_with_version` | [`src/manifest_lifecycle.rs:1550`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs#L1550) | `ManifestLifecycleService::delete_tag` | Observing tag version, snapshotting to journal, and feeding `delete_tag_conditional` |
| `delete_tag_conditional` | [`src/manifest_lifecycle.rs:572, 653`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs#L653) | `ManifestLifecycleService::recover_pending_journal_under_lock` | Journal recovery replay of conditional tag deletions |
| `delete_tag_conditional` | [`src/manifest_lifecycle.rs:1202`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs#L1202) | `ManifestLifecycleService::evict_proxy_cached_entry` | Conditionally removing tag alias using snapshotted version token |
| `delete_tag_conditional` | [`src/manifest_lifecycle.rs:1429`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs#L1429) | `ManifestLifecycleService::delete_manifest` | Conditionally removing snapshotted tag batch |
| `delete_tag_conditional` | [`src/manifest_lifecycle.rs:1593`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs#L1593) | `ManifestLifecycleService::delete_tag` | Authoritative optimistic-concurrency tag deletion |

#### Lifecycle Service Source Excerpt

In [`src/manifest_lifecycle.rs:1550-1615`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs#L1550-L1615), `ManifestLifecycleService::delete_tag` coordinates optimistic tag removal:

```rust
// [src/manifest_lifecycle.rs:1550-1605]
let (target_digest, version) = match self.storage.get_tag_with_version(repo, tag).await? {
    Some(res) => res,
    None => return Err(ManifestLifecycleError::TagNotFound),
};

guard.check_lease().await?;

if let Some(idx) = self.ref_index.as_ref() {
    idx.mark_dirty()?;
}

let op_id = uuid::Uuid::new_v4().to_string();
let now = now_unix_secs();
let canonical_repo =
    CanonicalRepoName::parse(repo).map_err(|_| ManifestLifecycleError::InvalidRepoName)?;
let mut journal = LifecycleJournalRecord {
    op_id: op_id.clone(),
    repo: canonical_repo,
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
```

---

## 3. Behavioral Characterization Findings

The eleven automated test functions in [`src/storage/fs/tests.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/tests.rs) characterize behavior across ten empirical dimensions:

### 3.1 Missing Tag and Missing Repository
- **Behavior:**
  - `resolve_tag("missing-repo", "missing-tag")` returns `Err(StorageError::NotFound)`.
  - `get_tag_with_version("missing-repo", "missing-tag")` returns `Ok(None)`.
  - When repository and `tags/` directory exist but the specific tag file is missing, `resolve_tag` returns `Err(StorageError::NotFound)` and `get_tag_with_version` returns `Ok(None)`.
- **Cause:** Both methods intercept `std::io::ErrorKind::NotFound` from the underlying asynchronous read call and translate it to their respective missing-value representation.

### 3.2 Valid SHA-256 and SHA-512 Digests (With and Without Newline)
- **Behavior:**
  - Lowercase SHA-256 (`sha256:<64 hex>`) and SHA-512 (`sha512:<128 hex>`) are accepted and parsed by `Digest::parse`.
  - Trailing newlines (`\n`) are stripped during parsing by `content.trim()` and `s.trim()`.
  - Both `resolve_tag` and `get_tag_with_version` return the identical logical `Digest` struct regardless of newline presence.
  - However, the version string from `get_tag_with_version` differs when a newline is present versus omitted (`hex_sha256(bytes + "\n") != hex_sha256(bytes)`), because version generation hashes the unparsed raw bytes.

### 3.3 Leading/Trailing Whitespace, Tabs, CRLF, and Padding > 256 Bytes
- **Behavior:**
  - Mixed whitespace consisting of ASCII spaces, tabs (`\t`), and carriage returns (`\r\n`) surrounding a valid digest is accepted without error.
  - Substantial whitespace padding exceeding 256 bytes (e.g., 300 spaces before and 300 spaces after the digest, totaling > 600 bytes) is successfully parsed by both read methods.
  - Neither method enforces a maximum byte-length cap on tag files before reading or trimming.
  - The generated version string is uniquely tied to the exact byte content (including all padding spaces and newline conventions).

### 3.4 Empty Content, Malformed Digest Text, and Invalid UTF-8
- **Behavior:**
  - **Empty file (0 bytes):**
    - `resolve_tag` parses `""`, which fails `Digest::parse("")` -> mapped to `StorageError::NotFound`.
    - `get_tag_with_version` parses `""` -> mapped to `StorageError::Internal { kind: StorageErrorKind::CorruptData, .. }`.
  - **Malformed text (e.g., `not-a-valid-digest\n`):**
    - `resolve_tag` fails `Digest::parse` -> mapped to `StorageError::NotFound`.
    - `get_tag_with_version` fails `Digest::parse` -> mapped to `StorageErrorKind::CorruptData`.
  - **Invalid UTF-8 byte sequences (e.g., `b"\xff\xfe\xfd"`):**
    - `resolve_tag` calls `tokio::fs::read_to_string`, which fails at the I/O layer with `std::io::ErrorKind::InvalidData`. This maps to `StorageError::Internal { kind: StorageErrorKind::Io, .. }`.
    - `get_tag_with_version` calls `tokio::fs::read` (succeeding at the I/O layer), converts via `String::from_utf8_lossy` (which substitutes replacement characters `\u{FFFD}`), and attempts `Digest::parse`. Parse failure maps to `StorageError::Internal { kind: StorageErrorKind::CorruptData, .. }`.

### 3.5 Raw-Byte Version Hashes and Concurrency Limitations
- **Behavior:**
  - `get_tag_with_version` computes `version = hex::encode(Sha256::digest(&bytes))`.
  - Tag files containing the identical logical digest but varying in whitespace or line terminators (e.g., `sha256:<hex>\n`, `sha256:<hex>`, `sha256:<hex>\r\n`, and `  sha256:<hex>\n`) produce **four distinct version strings**.
  - Consecutive reads of an unmodified tag file deterministically yield the identical version string.
  - **Limitation on Replacement Detection:** Because versioning is purely a cryptographic hash of raw content bytes (not an inode number, ctime, or monotonically increasing sequence), replacing a tag file with identical byte content produces the exact same version string. The version token cannot detect intervening writes or file recreation if the payload bytes match.
  - **Sequential vs Concurrency:** The characterization tests verify deterministic sequential behavior. Ambient reads do not provide atomic concurrency guarantees against concurrent filesystem mutation.

### 3.6 Symlink Resolution Under Ambient Path Semantics
- **Behavior:**
  - **Internal symlink:** A symlink within `tags/` targeting a regular file elsewhere inside the fixture storage root is resolved transparently by ambient `tokio::fs` calls.
  - **External symlink:** A symlink within `tags/` targeting a file located outside the storage root (in an external fixture directory) is followed transparently, reading the external file content and computing its digest and version. No path containment boundary is enforced.
  - **Ancestor symlink:** If the intermediate `tags/` directory itself is a symlink pointing to an external directory, ambient reads traverse through the symlink and access the target files.
  - **Dangling symlink:** An unresolvable symlink causes `read_to_string` and `read` to return `std::io::ErrorKind::NotFound`. Consequently, `resolve_tag` returns `StorageError::NotFound` and `get_tag_with_version` returns `Ok(None)`.

### 3.7 Non-Regular Files and Permission Denial
- **Behavior:**
  - **Directory in place of a tag file:** Attempting to read a directory via `read_to_string` or `read` fails with an OS I/O error (`EISDIR` on Unix). Both methods map this failure to `StorageError::Internal { kind: StorageErrorKind::Io, .. }`.
  - **Permission denial (`chmod 0o000`):** Expected behavior from source inspection is that an unreadable regular tag file returning `std::io::ErrorKind::PermissionDenied` propagates as `StorageError::Internal { kind: StorageErrorKind::Io, .. }`.
  - **Execution Accounting:** The test `test_tag_read_permission_denied` is gated under `#[cfg(unix)]` and explicitly marked `#[ignore]` by default because in standard root/privileged environments, mode `0o000` does not deny read permissions to the process. It was **not executed** in the recorded test run.

### 3.8 Path Component Traversal Across Tag and Repository Arguments
- **Behavior:**
  - **Tag Traversal:** When `tag` contains `"../outside.txt"`, `self.tag_path(name, tag)` evaluates to `self.root/repos/<repo>/tags/../outside.txt`. In a temporary fixture with an existing `tags/` directory, the OS evaluates `tags/..` to `<repo>/`, resolving `self.root/repos/<repo>/outside.txt` successfully for both read methods.
  - **Repository Traversal:** When `repo` contains `"../sibling_repo"`, `self.tag_path("../sibling_repo", "mytag")` evaluates to `self.root/repos/../sibling_repo/tags/mytag`. With `self.root/repos` present, `repos/..` evaluates to `self.root`, successfully resolving `self.root/sibling_repo/tags/mytag` for both `resolve_tag` and `get_tag_with_version`.
  - **Subdirectory Components:** Multi-segment names containing slashes (e.g., `"sub/nested_tag"`) are joined directly, traversing into nested child directories.
  - All paths remain confined strictly within temporary test fixtures.

### 3.9 Sequential Root Replacement & Divergence Against Pinned Reads
- **Behavior:**
  - In `test_tag_read_sequential_root_replacement_observed_tree`, when the physical directory at `self.root` is renamed to `root_old` and replaced with a fresh directory tree containing different tag content at the identical pathname:
    - `resolve_tag("myrepo", "latest")` immediately observes the replacement digest.
    - `get_tag_with_version("myrepo", "latest")` immediately observes the replacement digest and computes the version from the replacement bytes.
  - **Divergence Against Contained Readers:**
    - `FsStorage` already shares a pinned `FsMetadataReader` (`self.reader` and `self.read_adapter`) for contained operations (CAS blob reads and GC manifest discovery), which hold open directory file descriptors (`openat2`).
    - Tag reads bypass that reader and resolve pathnames starting from `self.root` dynamically on each call.
    - If `self.root` is renamed or replaced, contained reads continue to observe the original directory inode via their pinned descriptor, while ambient tag reads immediately observe the newly created directory tree at `self.root`. This creates a split-brain divergence between contained and uncontained operations.

### 3.10 Conditional Deletion & The Version Token's Role
- **Behavior:**
  - `delete_tag_conditional` checks `current_version != expected_version`.
  - **Precondition failure on modified content:** If content or whitespace is altered after observing `v1`, calling `delete_tag_conditional(repo, tag, Some(&v1))` returns `ConditionalDeleteResult::PreconditionFailed { current_version: Some(v2) }`. The modified file is preserved on disk.
  - **Successful deletion:** Calling `delete_tag_conditional` with the matching current version successfully unlinks the tag file and returns `ConditionalDeleteResult::Deleted`.
  - **Raw Bytes vs Parsing:** Unlike `resolve_tag` or `get_tag_with_version`, `delete_tag_conditional` never parses digest syntax. It hashes raw bytes, meaning malformed or invalid text can be conditionally verified and deleted solely by version match.
  - **Inability to Detect Identical Content Overwrite:** If a tag file is overwritten or recreated with identical byte content, the version hash is unchanged. The version token cannot detect intervening file replacement if content is identical.

---

## 4. UTF-8 Differences and Error Taxonomy Matrix

The two read methods exhibit distinct error handling and UTF-8 processing behavior across input conditions:

| Scenario / Input | File System Operation | `resolve_tag` Result | `get_tag_with_version` Result | Root Cause of Divergence |
| :--- | :--- | :--- | :--- | :--- |
| Missing Tag / Repo | `read_to_string` / `read` | `Err(StorageError::NotFound)` | `Ok(None)` | API contract: `resolve_tag` returns error; `get_tag_with_version` returns optional |
| Dangling Symlink | `read_to_string` / `read` | `Err(StorageError::NotFound)` | `Ok(None)` | OS returns `ENOENT`, handled as missing file |
| Directory as Tag | `read_to_string` / `read` | `Err(StorageErrorKind::Io)` | `Err(StorageErrorKind::Io)` | OS returns `EISDIR`, propagating as I/O error |
| Permission Denied (Source-derived) | `read_to_string` / `read` | `Err(StorageErrorKind::Io)` | `Err(StorageErrorKind::Io)` | OS returns `EACCES`, propagating as I/O error (test ignored) |
| Valid Digest (`\n`) | Successful read | `Ok(Digest)` | `Ok(Some((Digest, version_A)))` | Version reflects bytes with trailing `\n` |
| Valid Digest (No `\n`) | Successful read | `Ok(Digest)` | `Ok(Some((Digest, version_B)))` | Version reflects bytes without `\n` (`version_A != version_B`) |
| Whitespace > 256 B | Successful read | `Ok(Digest)` | `Ok(Some((Digest, version_C)))` | Trimming succeeds; no size ceiling enforced |
| Empty File (0 Bytes) | Successful read | `Err(StorageError::NotFound)` | `Err(StorageErrorKind::CorruptData)` | `resolve_tag` maps parse failure to `NotFound`; `get_tag_with_version` maps to `CorruptData` |
| Malformed Text | Successful read | `Err(StorageError::NotFound)` | `Err(StorageErrorKind::CorruptData)` | `resolve_tag` maps parse failure to `NotFound`; `get_tag_with_version` maps to `CorruptData` |
| Invalid UTF-8 Bytes | `read_to_string` vs `read` | `Err(StorageErrorKind::Io)` | `Err(StorageErrorKind::CorruptData)` | `read_to_string` fails at I/O layer (`InvalidData`); `read` succeeds and `from_utf8_lossy` fails at parse layer |

---

## 5. Existing Behavior vs Unapproved Future Choices

To maintain architectural clarity and prevent premature breaking changes, current production behavior is explicitly contrasted with potential future options:

| Design Dimension | Current Production Behavior (Characterized) | Unapproved Future Choice (Not in Scope) | Architectural Impact & Rationale |
| :--- | :--- | :--- | :--- |
| **Tag Name Grammar** | Arbitrary strings accepted; forward slashes traverse directories; `..` traverses upward | Strict OCI grammar rejection (`^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$`) | Enforcing grammar at the storage layer without application consensus could break non-standard clients or internal tag conventions. |
| **Path Containment** | Ambient resolution follows symlinks inside and outside storage root; bypasses pinned reader | Pinned descriptor containment via `openat2` (`RESOLVE_BENEATH`) | Requires coordinated cutover to avoid split-brain divergence. Non-Linux fallback behavior is NOT approved (`O-15` remains OPEN). |
| **Error Taxonomy** | `resolve_tag` maps corrupt/empty tags to `NotFound`; `get_tag_with_version` maps to `CorruptData`; invalid UTF-8 maps to `Io` vs `CorruptData` | Harmonized error mapping returning `NotFound` for missing files and `CorruptData` for malformed payload syntax | Missing tags must not be mapped to `CorruptData`. Changing `resolve_tag` error mapping alters HTTP status codes (e.g. 500 instead of 404). |
| **Payload Size Ceiling** | Reads arbitrary file sizes into memory; trims arbitrary whitespace padding | Hard size cap (e.g., 256 bytes or 72 bytes) | Enforcing a tight ceiling without migration could cause existing padded tag files to suddenly fail resolution. |

---

## 6. Canonical Quality Gates

All canonical quality gates remain explicitly **OPEN**:

- **`O-03` (Key and continuation-token contracts):** **OPEN**.
- **`O-04` (Filesystem write durability and containment):** **OPEN**.
- **`O-05` (Broader filesystem read containment):** **OPEN**. (The present characterization freezes baseline behavior; tag reads and other paths remain uncontained).
- **`O-06` (Typed AWS mapping and pinned-MinIO evidence):** **OPEN**.
- **`O-13` (Hosting, distribution, and release strategy):** **OPEN**.
- **`O-15` (Non-Linux verification):** **OPEN**.
- **`O-16` (Earlier Slice 11 audit/test-inventory evidence):** **OPEN**.
- **`D-06` (Broader extraction, cutover, compatibility, and distribution acceptance):** **OPEN**.
