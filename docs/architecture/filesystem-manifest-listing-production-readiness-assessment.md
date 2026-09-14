# Architecture Assessment: Filesystem Manifest-Listing Production Readiness (Corrected)

- **Status**: DOCUMENTATION & SOURCE REVIEW ONLY — PENDING USER DECISIONS — NOT COMMITTED
- **Date**: 2026-09-11
- **Target File**: `docs/architecture/filesystem-manifest-listing-production-readiness-assessment.md`
- **Authoritative Baseline HEADs**:
  - `registry-rust`: `2f7fcc8b46c30c0008fc60ba15e15b0c1bf3eea3`
  - `storage-layer-rust`: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`
- **Canonical Quality Gates**: All open (O-03, O-04, O-05, O-06, O-13, O-15, O-16, D-06).

---

## 1. Executive Summary & Reconciliation with Authoritative Source

This assessment revisits production manifest-listing promotion decisions following the completion and commit of two caller-hardening slices:
1. **Commit `ea5cdaa4429f0ef265172ae10b4d9e112e0e1bc6`**: `fix(lifecycle): propagate manifest reference discovery failures`
2. **Commit `2f7fcc8b46c30c0008fc60ba15e15b0c1bf3eea3`**: `fix(ref-index): stage discovery before synchronization mutations`

Before these commits, promoting contained manifest listing was blocked because callers (`is_blob_referenced_in_repo` in `src/manifest_lifecycle.rs` and `sync_repo_manifests_and_tags` in `src/blob_ref_index.rs`) either silently swallowed errors or interleaved destructive mutations with fallible discovery.

### 1.1 Source-Grounded State: What Changed vs. What Remains Unresolved

#### A. Lifecycle Reference Discovery (`src/manifest_lifecycle.rs`)
- **What Changed**:
  - `is_blob_referenced_in_repo` signature was changed from `async fn is_blob_referenced_in_repo(...) -> bool` to `async fn is_blob_referenced_in_repo(...) -> Result<bool, StorageError>`.
  - All discovery steps (`list_manifest_digests_page`, `get_manifest`, `parse_manifest_refs`) propagate errors via `?` rather than silently returning `false`.
  - Continuation token cycles in manifest pagination are detected via an explicit `HashSet<String>` (`seen_tokens`) and return `StorageError::backend(...)`.
  - Errors propagate through all caller sites in `recover_pending_journal_under_lock` (lines 716 and 747) and `evict_proxy_cached_entry` (line 1270).
- **What Remains Unresolved**:
  - **Prior Authoritative Mutations Not Rolled Back**: In `evict_proxy_cached_entry`, tag alias deletion and manifest CAS deletion are attempted *before* blob reference discovery. Underlying deletion calls (`let _ = self.storage.delete_manifest(...)`) ignore errors, so deletions are attempted rather than guaranteed. If a subsequent listing error occurs during blob reference checking, the tag and manifest remain deleted from storage and index; only proxy blob unlinking is aborted.
  - **Retained Journal Without In-Process Retry Daemon**: On listing error, the journal remains on disk in `ProxyManifestDeleted` phase, ensuring the incomplete operation is not forgotten. However, there is no automatic background retry daemon; retry occurs only when a process acquires coordination on the repository and executes recovery.
  - **Fallback Membership Listing**: If manifest bytes are missing during recovery, fallback membership listing (`list_repo_blob_memberships_page`) iterates proxy records without page bounds.

#### B. Reference-Index Synchronization Staging (`src/blob_ref_index.rs`)
- **What Changed**:
  - `sync_repo_manifests_and_tags` is separated into two strictly demarcated phases:
    1. Read-only backend discovery (`discover_repo_manifests_and_tags`), collecting `roots`, `edges`, and `tags` into `DiscoveredRepoData`.
    2. Local sled index application (tag removal, root count increments, DAG edge insertion, tag insertion, database flush).
  - Any discovery failure (listing failure, manifest read error, parse error, continuation token cycle) aborts before any sled tree is modified by this invocation.
  - Independent continuation token cycle detection (`HashSet<String>`) is enforced for both manifest pagination and tag pagination streams.
- **What Remains Unresolved**:
  - **Successful-Sync Root Count Inflation**: Staging guarantees that *failed* discovery causes zero count increments. However, repeated *successful* calls to `sync_repo_manifests_and_tags` continue to increment global root counts (`manifest_root_counts`) on each run. Resolving this requires per-repository root provenance and remains explicitly deferred.
  - **Unbounded Staging Memory**: `DiscoveredRepoData` and token tracking sets grow in memory proportional to repository size, with no page limit or entry count caps.
  - **Non-Transactional Sled Application**: Phase 2 increments all staged root counts before inserting staged reverse edges. A crash or sled error during application leaves partial writes.
  - **Rebuild Tree-Clearing Prior to Discovery**: `rebuild()` calls `clear_all_trees()` and transitions `meta` to `META_STATE_BUILDING` *before* invoking `sync_repo_manifests_and_tags`. If discovery fails during a rebuild, the index remains cleared and in `BUILDING` state (subsequent `check_health()` fails with `RefIndexError::Corrupt`).
  - **No Snapshot Isolation**: Backend storage listing and manifest retrieval are not snapshot-isolated across pagination requests.

#### C. Production Manifest Listing (`src/storage/fs.rs`)
- **Current Production State**:
  - Production `FsStorage::list_manifest_digests_page` (`src/storage/fs.rs:1000-1046`) remains **unmodified** on legacy uncontained `tokio::fs::read_dir`.
  - Contained manifest listing (`src/storage/fs/manifest_listing.rs`) remains strictly test-gated under `#[cfg(test)]`.
  - Production enumeration limits and configuration remain unapproved.
  - Earlier recommendations and "approve" labels in previous design documents were architectural proposals and **do not constitute user authorization**.

---

## 2. Caller Behavior Under Listing Errors

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Caller 1: manifest_lifecycle.rs -> is_blob_referenced_in_repo               │
│                                                                             │
│ 1. evict_proxy_cached_entry:                                                │
│    - Step 4: Tag deleted (Storage + RefIndex). Journal -> ProxyTagDeleted   │
│    - Step 6: Manifest deleted (Storage + RefIndex). Journal -> ProxyManifestDeleted
│    - is_blob_referenced_in_repo fails:                                      │
│      * Returns Err(ManifestLifecycleError::Storage(...))                    │
│      * Function aborts immediately via `?`                                  │
│      * Tag and manifest deletions are NOT rolled back                       │
│      * Proxy blob memberships are NOT unlinked                              │
│      * Journal remains persisted on disk with phase ProxyManifestDeleted    │
│      * NO in-process automatic retry; retried on next coordination attempt │
│                                                                             │
│ 2. recover_pending_journal_under_lock:                                      │
│    - Replay detects phase ProxyManifestDeleted                              │
│    - is_blob_referenced_in_repo fails:                                      │
│      * Returns Err(ManifestLifecycleError::Storage(...))                    │
│      * Recovery aborts via `?`; journal NOT deleted                         │
│      * Next restart or coordination attempt will retry recovery             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ Caller 2: blob_ref_index.rs -> sync_repo_manifests_and_tags                 │
│                                                                             │
│ 1. sync_repo(repo):                                                         │
│    - Phase 1: discover_repo_manifests_and_tags fails                        │
│      * Zero sled mutations by this invocation                               │
│      * Pre-entry index state preserved (subject to concurrent callers)      │
│      * Returns Err(RefIndexError::Storage(...))                             │
│                                                                             │
│ 2. rebuild():                                                               │
│    - clear_all_trees() ALREADY executed                                     │
│    - meta ALREADY transitioned to META_STATE_BUILDING                       │
│    - Phase 1 discovery fails:                                               │
│      * Zero additional mutations written                                    │
│      * Index remains CLEARED and in BUILDING state                          │
│      * Subsequent check_health() fails with RefIndexError::Corrupt          │
│      * Pre-rebuild index is NOT restored                                    │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ Caller 3: blob_gc/policy.rs -> build_manifest_protected_set                 │
│                                                                             │
│ - Storage check: storage.kind() == "fs" && fs_root/repos exists             │
│ - DIRECT FILESYSTEM BYPASS: Calls build_manifest_protected_set_fs            │
│   * Completely bypasses FsStorage::list_manifest_digests_page               │
│   * Performs raw tokio::fs::read_dir directly over fs_root/repos            │
│ - list_manifest_digests_page is ONLY invoked for non-filesystem backends    │
│ - Listing promotion DOES NOT AFFECT OR HARDEN the GC filesystem path        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.1 Critical Distinctions
1. **Lifecycle Discovery Failure vs. Prior Authoritative Mutations**:
   In `evict_proxy_cached_entry`, tag and manifest deletion calls (`let _ = self.storage.delete_manifest(...)`) occur *before* `is_blob_referenced_in_repo` is called. A listing error does not undo these deletions; it only aborts unlinking proxy blob memberships. Underlying deletions ignore return results, so deletions are attempted rather than guaranteed.
2. **Retained Journals vs. Automatic Retry**:
   On listing failure, the lifecycle journal is retained in `ProxyManifestDeleted` phase. There is no automated background polling loop to retry it; retry occurs only when a process acquires coordination on the repository and executes `recover_and_ensure_index_healthy` / `recover_pending_journal_under_lock`.
3. **Reference-Index Discovery Preservation vs. Rebuild State**:
   Preservation on discovery failure guarantees that `sync_repo_manifests_and_tags` writes nothing to sled trees, preserving the state *at function entry*. During `rebuild()`, all trees were already wiped clean prior to function entry; a listing error leaves the index empty and corrupt.
4. **Discovery Failure vs. Application Failure**:
   Discovery failure occurs before Phase 2 begins. If discovery succeeds, Phase 2 begins: root counts are incremented first, followed by DAG edge insertion and tag insertion. If an application failure occurs (e.g. sled error or crash), partial writes are left without rollback.
5. **GC Filesystem Bypass**:
   `src/blob_gc/policy.rs:170-176` conditionally bypasses `list_manifest_digests_page` whenever `storage.kind() == "fs"`. Promoting contained manifest listing has zero effect on GC's filesystem path.

### 2.2 Remaining Promotion Blockers
Is there any blocker specifically introduced or materially worsened by listing promotion?
- **No new caller-side failure modes are introduced**: Both `src/manifest_lifecycle.rs` and `src/blob_ref_index.rs` safely handle listing errors without corrupting the index or entering infinite pagination loops.
- **Fail-Closed Semantics**: Promoting contained listing changes legacy behavior from silently returning empty pages on `EACCES` or `ENOTDIR` to returning typed errors (`PermissionDenied`, `CorruptData`). Hardened callers now correctly propagate these errors instead of treating unreadable directories as empty repositories.
- **The Only Operational Concern is Budget Exhaustion**: Exceeding `DirEnumerationLimits` returns `StorageError::Internal { kind: StorageErrorKind::Backend, message }`. If configured limits are too low, legitimate repositories will fail listing, pausing lifecycle eviction and index synchronization until the limit is raised or files are removed. This makes robust, configurable limits essential.

---

## 3. Compatibility Decision Table

The table below contrasts current production behavior with contained seam behavior across all key dimensions, detailing caller-visible consequences, existing test coverage, and whether user approval is required.

| # | Behavioral Dimension | Current Production Behavior (`src/storage/fs.rs`) | Contained Seam Behavior (`fs/manifest_listing.rs`) | Caller-Visible Consequence | Existing Test Evidence | Approval Status |
|---|----------------------|----------------------------------------------------|---------------------------------------------------|----------------------------|------------------------|-----------------|
| 1 | **Repository Validation** | Joins unvalidated `repo` onto path; vulnerable to traversal (`..`), absolute paths, and backslashes. | Validates `repo` via `manifest_dir_key` helper (checking empty, leading/trailing slashes, backslashes, control characters, empty segments, `.`, `..`) before `ObjectKey::parse`; fails closed with `StorageError::InvalidRepoName`. | Rejects malicious or malformed repository names immediately; prevents storage root escape. | `manifest_listing.rs:320-390`, `fs/tests.rs:5102-5144` | **PENDING USER DECISION** |
| 2 | **Missing & Empty Paths** | Returns `Ok(([], None))` if `repos/<repo>/manifests` is missing or empty. | Returns `Ok(([], None))` if repository or manifests directory does not exist or is empty. | Zero behavioral difference; clean empty page on missing paths. | `fs/tests.rs:4580-4613`, `manifest_listing.rs:850-870` | **NO CHANGE** (Compatible) |
| 3 | **Permission Denied (`EACCES`)** | Swallows `EACCES` silently via `if let Ok(...)` and returns `Ok(([], None))`. | Returns `StorageError::permission_denied(...)` (`StorageErrorKind::PermissionDenied`). | Hardened callers propagate error; prevents silently misidentifying unreadable repos as empty. | `fs/tests.rs:5176-5250`, `manifest_listing.rs:995-1005` | **PENDING USER DECISION** |
| 4 | **Component Not a Directory (`ENOTDIR`)** | Swallows `ENOTDIR` silently and returns `Ok(([], None))`. | Returns `StorageError::corrupt_data(...)` (`StorageErrorKind::CorruptData`). | Fails closed with typed error when filesystem metadata layout is corrupted. | `fs/tests.rs:5145-5175`, `manifest_listing.rs:137-139` | **PENDING USER DECISION** |
| 5 | **Resolution Rejection & Symlinks** | Follows symlinks; enumerates symlinked files and parent directories outside root. | Uses `RESOLVE_BENEATH \| RESOLVE_NO_SYMLINKS`; rejects symlink traversal with `StorageError::io(...)`. (Note: Descriptor containment does not establish mount or hard-link isolation). | Prevents symlink attacks and containment escapes beneath the storage root. | `fs/tests.rs:5055-5101`, `manifest_listing.rs:1010-1025` | **PENDING USER DECISION** |
| 6 | **Entry-Type Filtering** | Includes subdirectories, symlinks, and FIFOs as manifest digests if name matches. | Filters strictly for `DirEntryType::Regular`; skips non-regular entries. (Note: Entry type is an observation during directory enumeration, not a guarantee against later replacement). | Prevents non-file entries from being treated as manifest digests. | `fs/tests.rs:5003-5054`, `manifest_listing.rs:196-198` | **PENDING USER DECISION** |
| 7 | **Canonical Filenames** | Discovers SHA-256 files and accepts prefixed `sha512:` names; ignores raw (unprefixed) 128-hex SHA-512 files; accepts uppercase hex and `sha256:` / `sha512:` prefixes. | Discovers canonical raw lowercase 64-hex SHA-256 and 128-hex SHA-512 regular files. Filters out uppercase hex, algorithm prefixes, `.tmp.*`, `.lock.*`. | Standardizes discovery on canonical read-path filenames; adds raw SHA-512 support. | `fs/tests.rs:4709-4771`, `manifest_listing.rs:206-220` | **PENDING USER DECISION** |
| 8 | **Sorting & Deduplication** | Sorts by `hex()` only; duplicate digests not deduplicated; cursor mismatch on mixed algorithms. | Sorts by `Digest::cmp` (algorithm ascending, then hex ascending); deduplicates entries via `dedup()`. | Deterministic pagination ordering; eliminates binary search cursor mismatch across algorithms. | `fs/tests.rs:4772-5002`, `manifest_listing.rs:222-224` | **PENDING USER DECISION** |
| 9 | **Continuation Tokens** | Evaluates tokens using full-string lexical comparison on hex-sorted list. | Evaluates tokens using full-string lexical comparison on `Digest::cmp`-sorted list. | Consistent pagination continuation across page boundaries. | `fs/tests.rs:4653-4708`, `manifest_listing.rs:225-231` | **PENDING USER DECISION** |
| 10 | **Zero Page Limit** | Executes full directory enumeration, sorts, binary-searches token, slices empty range, returns `None` for next token. | Validates `repo` via `manifest_dir_key`, then immediately returns `Ok(([], None))` without performing directory I/O, ignoring any continuation token. | Eliminates unnecessary filesystem I/O on zero-limit probe requests. | `fs/tests.rs:4822-4849`, `manifest_listing.rs:184-186` | **PENDING USER DECISION** |
| 11 | **Oversized Page Limit** | Returns all available entries up to `all_digests.len()`. | Returns all available entries up to `all_digests.len()`. | Zero behavioral difference; returns all matching items. | `fs/tests.rs:4822-4849`, `manifest_listing.rs:234-235` | **NO CHANGE** (Compatible) |
| 12 | **Inter-Page Mutation** | Stateless re-scan per page; sequential additions/removals observed across pages; no snapshot isolation. | Stateless re-scan per page; sequential additions/removals observed across pages; no snapshot isolation. | Documented identical semantic: manifest listing does not provide transactional snapshot isolation. | `fs/tests.rs:5251-5289` | **NO CHANGE** (Compatible) |

---

## 4. Resource Accounting and Configuration Architecture

### 4.1 What Enumeration Limits Count vs. Exclude

It is critical to distinguish directory enumeration limits from caller-side memory usage:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ WHAT DirEnumerationLimits COUNTS:                                           │
│ - Every directory entry evaluated during readdir in `manifests/`            │
│ - Includes regular files, subdirectories, symlinks, FIFOs                   │
│ - Includes temporary files (.tmp.*), lock files (.lock.*), non-hex files    │
│ - Evaluated BEFORE filtering: 4,000 temp files + 7,000 manifests = 11,000  │
│   entries evaluated -> EXCEEDS a 10,000 entry limit!                       │
│ - Counts raw OsString byte length for each entry name                      │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ WHAT DirEnumerationLimits EXCLUDES:                                         │
│ - Filesystem metadata, kernel dentries, and inode table memory              │
│ - Vec<DirEntry> struct overhead (pointer, length, capacity, enum tag)       │
│ - Vec<Digest> heap allocations in manifest_listing.rs                       │
│ - Caller-side DiscoveredRepoData in blob_ref_index.rs (roots, edges, tags)  │
│ - In-memory manifest JSON ASTs and reference parsing buffers                │
│ - Memory consumed across multiple pages in paginated loops                  │
│ - Sled database B-tree cache memory                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Correct Byte and Entry Accounting Analysis

1. **Exact Filename Byte Sizing**:
   - A canonical raw SHA-256 filename is 64 lowercase ASCII hex characters (64 bytes).
   - A canonical raw SHA-512 filename is 128 lowercase ASCII hex characters (128 bytes).
   - **Minimum Byte Bound**: Exactly 128 raw name bytes can hold one canonical SHA-512 filename. A proposed minimum of 128 bytes (`name_bytes >= 128`) is technically grounded. A higher minimum (e.g. 130 bytes) is strictly an optional policy margin, not a technical requirement.
   - **Provisional Defaults**: 10,000 entries and 1,500,000 name bytes remain **explicitly provisional test fixtures pending user approval**. There is no empirical evidence that repositories rarely exceed 10,000 manifests, nor that enumeration completes within any fixed latency bound. An entry limit restricts the count of directory entries evaluated, but does *not* bound syscall latency or Tokio worker thread occupancy.
   - **Byte Margin Allocation**: In the provisional 1,500,000-byte budget, 10,000 SHA-512 entries consume `10,000 * 128 = 1,280,000` bytes. The remaining 220,000 bytes accommodate additional raw filename bytes only (e.g. temporary upload files like `.tmp.upload-xyz` or lock files like `.lock.exclusive`). These bytes do not cover filesystem overhead, operating system structures, or caller allocations.
   - **Independent Entry Exhaustion**: Temporary and lock files also consume entry capacity (1 entry each). Having spare byte capacity does not allow a directory to exceed its entry limit.
2. **Zero Budget Semantics**:
   - In `storage-fs/src/dir.rs`, an empty directory containing only `.` and `..` returns successfully without calling `account_entry`. Therefore, a zero budget (`max_entries = 0` or `max_total_name_bytes = 0`) succeeds on an empty directory; it does not cause every listing call to fail.
   - Rejecting zero in configuration is a **proposed defensive policy** to prevent misconfigured non-empty repositories from failing on their first entry.
3. **Upper Bound Representation**:
   - `usize::MAX` is a platform-dependent integer representability ceiling (e.g. `2^64 - 1` on 64-bit systems), not a safe memory ceiling. No separate operational maximum is proposed beyond host system memory constraints, leaving operators responsible for appropriate upper bounds.

### 4.3 Grounded Configuration Architecture

1. **Existing Source Types**:
   - `FileStorageFs` in `src/config.rs:1002-1005`:
     ```rust
     #[derive(Clone, Debug, Default, Deserialize)]
     struct FileStorageFs {
         #[serde(default)]
         root: Option<String>,
     }
     ```
   - `FileStorage` in `src/config.rs:976-987`:
     ```rust
     #[derive(Clone, Debug, Default, Deserialize)]
     struct FileStorage {
         #[serde(default)]
         backend: Option<String>,
         #[serde(default)]
         fs: FileStorageFs,
         #[serde(default)]
         s3: FileStorageS3,
         #[serde(default)]
         ref_index: FileStorageRefIndex,
     }
     ```
   - Configuration Loader in `src/config.rs:1132`:
     `Config::from_env_with_files(config_paths: &[PathBuf]) -> Result<Self, ConfigError>`
   - Existing Constructors in `src/storage/fs.rs:201-236`:
     ```rust
     impl FsStorage {
         pub fn try_new(root: PathBuf, max_upload_bytes: u64) -> Result<Self, StorageError>
         pub fn new(root: PathBuf, max_upload_bytes: u64) -> Self
     ```
   - Existing Storage Wiring in `src/storage/mod.rs:829-835` and `877-893`:
     ```rust
     pub fn storage_wiring_try_from_config(config: &Config) -> Result<StorageWiring, StorageError>
     pub fn proxy_cache_storage_try_from_config(...) -> Result<Arc<dyn ports::ProxyStoragePort>, StorageError>
     ```
   - Existing Startup Offload in `src/storage/mod.rs:927-947`:
     ```rust
     pub(crate) async fn storage_wiring_try_from_config_async_with_factory<F>(
         config: &Config,
         storage_factory: F,
     ) -> Result<StorageWiring, StorageError>
     ```
     offloads blocking initialization to `tokio::task::spawn_blocking`.
2. **Proposed Field Additions to `FileStorageFs`**:
   ```rust
   #[derive(Clone, Debug, Default, Deserialize)]
   struct FileStorageFs {
       #[serde(default)]
       root: Option<String>,
       #[serde(default)]
       manifest_listing_max_entries: Option<usize>,
       #[serde(default)]
       manifest_listing_max_name_bytes: Option<usize>,
   }
   ```
3. **Environment Variable Parsing & Numeric Overflow**:
   - `env_usize_opt` in `src/config.rs:2956-2967`:
     ```rust
     fn env_usize_opt(keys: &[&'static str]) -> Result<Option<usize>, ConfigError> {
         let Some((key, v)) = env_str_any(keys) else {
             return Ok(None);
         };
         v.trim()
             .parse::<usize>()
             .map(Some)
             .map_err(|_| ConfigError::InvalidEnvValue {
                 key,
                 expected: "unsigned integer",
             })
     }
     ```
   - `env_str_any` searches keys in slice order. Hierarchical alias precedes flat alias:
     - For entries: `&["REGISTRY__STORAGE__FS__MANIFEST_LISTING_MAX_ENTRIES", "STORAGE_FS_MANIFEST_LISTING_MAX_ENTRIES"]`
     - For bytes: `&["REGISTRY__STORAGE__FS__MANIFEST_LISTING_MAX_NAME_BYTES", "STORAGE_FS_MANIFEST_LISTING_MAX_NAME_BYTES"]`
   - Numeric overflow (value > `usize::MAX`) causes `parse::<usize>()` to fail with `PosOverflow`, which `env_usize_opt` maps to `ConfigError::InvalidEnvValue`.
4. **Validation in Config and Direct Constructors**:
   - In `Config::from_env_with_files`:
     If `manifest_listing_max_entries == Some(0)`:
     Returns `Err(ConfigError::InvalidValue { field: "manifest_listing_max_entries", message: "must be at least 1".to_string() })`.
     If `manifest_listing_max_name_bytes < Some(128)`:
     Returns `Err(ConfigError::InvalidValue { field: "manifest_listing_max_name_bytes", message: "must be at least 128".to_string() })`.
   - In `FsStorage::try_new_with_limits(root: PathBuf, max_upload_bytes: u64, limits: storage_fs::DirEnumerationLimits) -> Result<Self, StorageError>`:
     Enforces the same validation:
     ```rust
     if limits.max_entries() < 1 {
         return Err(StorageError::configuration("manifest_listing_max_entries must be at least 1"));
     }
     if limits.max_total_name_bytes() < 128 {
         return Err(StorageError::configuration("manifest_listing_max_name_bytes must be at least 128"));
     }
     ```
   - Filesystem-only applicability: limits apply only when `config.storage_backend == StorageBackend::Filesystem`. S3 remains unaffected.

---

## 5. Recommended Next Implementation Slice

We recommend a single, strictly bounded implementation slice to promote contained manifest listing to production:

### 5.1 Target Files and Changes
1. `src/storage/fs/manifest_listing.rs`:
   - Remove file-level `#![cfg(test)]`.
   - Expose `pub fn default_manifest_dir_limits() -> DirEnumerationLimits` with validated provisional defaults (10,000 entries, 1,500,000 name bytes).
   - Keep `mod tests` strictly guarded under `#[cfg(test)]`.
2. `src/storage/fs.rs`:
   - Remove `#[cfg(test)]` on `pub(crate) mod manifest_listing;`.
   - Add `manifest_listing_limits: storage_fs::DirEnumerationLimits` field to `FsStorage`.
   - Preserve existing constructor arguments on `FsStorage::try_new(root: PathBuf, max_upload_bytes: u64)` and add `FsStorage::try_new_with_limits(root: PathBuf, max_upload_bytes: u64, limits: storage_fs::DirEnumerationLimits)`.
   - Replace legacy `tokio::fs::read_dir` implementation of `list_manifest_digests_page` with delegation to `manifest_listing::list_manifest_digests_page_impl`.
3. `src/config.rs`:
   - Add `manifest_listing_max_entries` and `manifest_listing_max_name_bytes` to `FileStorageFs`.
   - Add parsing with hierarchical and flat environment variable aliases in `from_env_with_files`.
   - Enforce bounds validation (`entries >= 1`, `name_bytes >= 128`) returning `ConfigError::InvalidValue`.
4. `src/storage/mod.rs`:
   - In `storage_wiring_try_from_config` and `proxy_cache_storage_try_from_config`, pass configured limits from `Config` to `FsStorage::try_new_with_limits`.
5. `src/storage/fs/tests.rs`:
   - Update assertions for the 8 characterization tests to reflect contained listing semantics (raw SHA-512 discovery, canonical filename filtering, `Digest::cmp` sort, fail-closed on `EACCES`/`ENOTDIR`).

### 5.2 Rollback Scope
If issues arise during verification or deployment, the slice can be cleanly reverted in git without altering database schemas, on-disk directory layouts, or public HTTP APIs.

### 5.3 Explicit Decisions Requiring User Approval Prior to Implementation
The user must review and approve the following decisions before implementation begins:
1. **Approval of Configuration Names & Precedence**:
   - File fields: `[storage.fs].manifest_listing_max_entries`, `manifest_listing_max_name_bytes`.
   - Environment aliases: `REGISTRY__STORAGE__FS__MANIFEST_LISTING_MAX_*` over `STORAGE_FS_MANIFEST_LISTING_MAX_*`.
2. **Approval of Default Resource Limits**:
   - Provisional defaults: 10,000 entries and 1,500,000 name bytes.
   - Minimum floors: 1 entry and 128 name bytes.
3. **Approval of Compatibility Changes**:
   - Failing closed with typed `StorageError` on `EACCES`, `ENOTDIR`, and symlink rejection instead of returning empty pages.
   - Filtering non-regular files, algorithm-prefixed filenames, and uppercase hex filenames.
   - Discovering raw SHA-512 regular files.
   - Sorting via `Digest::cmp` rather than hex-only.

---

## 6. Planned Verification Suite

The following verification commands are planned for the implementation slice. **(Status: PLANNED — NOT EXECUTED)**.
All proposed test names and cargo test filter prefixes are strictly aligned under `test_config_storage_fs_manifest_listing_`.

```bash
# 1. Code Formatting
cargo fmt --check

# 2. Strict Compilation and Clippy Lint Audit
cargo clippy --locked --all-targets -- -D warnings

# 3. Contained Manifest Listing Unit Tests
cargo test --locked --lib storage::fs::manifest_listing

# 4. Storage Filesystem Characterization Tests (covering promoted listing)
cargo test --locked --lib storage::fs::tests::test_manifest_listing_

# 5. Configuration Parsing and Bounds Validation Tests
# Covers: defaults, toml parsing, env overrides, numeric overflow, malformed values, bounds, filesystem-only applicability
cargo test --locked --lib config::tests::test_config_storage_fs_manifest_listing_

# 6. Hardened Caller Integration Tests (BlobRefIndex under listing errors)
cargo test --locked --lib blob_ref_index::tests::test_sync_repo_

# 7. Hardened Caller Integration Tests (ManifestLifecycle under listing errors)
cargo test --locked --test manifest_lifecycle_tests

# 8. Clean Diff and Whitespace Verification
git diff --check
```

---

## 7. Historical Reporting Correction

The commit report for commit `2f7fcc8b46c30c0008fc60ba15e15b0c1bf3eea3` inadvertently misstated the sizes of three earlier review archives due to a copy-paste error from previous terminal listings.

The verified on-disk archive sizes and SHA-256 hashes are:
- `session-20260911-2245/lifecycle-reference-discovery-hardening.tar.gz`:
  - Verified size: **54,418 bytes** (previously misreported as 54,992 bytes)
  - SHA-256: `413a603ed85c105e5dd50a93d8a2b2d8d58499fe4547280e2aa0a23fe97c1c4e`
- `session-20260911-2300/filesystem-reference-index-sync-hardening-design.tar.gz`:
  - Verified size: **20,302 bytes** (previously misreported as 24,397 bytes)
  - SHA-256: `bf8f0d7ed9a96011b3633a71ebdff05803ec8a7147ec69b977b75f38133efda4`
- `session-20260911-2310/filesystem-reference-index-sync-hardening-design.tar.gz`:
  - Verified size: **15,807 bytes** (previously misreported as 25,880 bytes)
  - SHA-256: `49059745e7409d29bd139c53ed72dacb8b13d4ec9179fc92797524d8813ca3a4`

The local files on disk match the verified sizes and hashes above. No archives were modified or rewritten.
