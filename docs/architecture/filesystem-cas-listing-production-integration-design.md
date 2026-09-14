# Architecture Design: Production Filesystem CAS Listing Integration

**Repository:** `registry-rust`
**Target Path:** `docs/architecture/filesystem-cas-listing-production-integration-design.md`
**Authoritative Baselines:**
- `registry-rust` HEAD: `a977d196e176f54942737cafa21c01a0ab73371d`
- `storage-layer-rust` HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`

**Scope:** Design-only specification for integrating extracted, descriptor-relative CAS listing into production `FsStorage`.
**Status:** **DESIGN ONLY — NOT AUTHORIZED FOR PRODUCTION CUTOVER**.
**Quality Gates:** **O-03, O-04, O-05, O-06, O-13, O-15, O-16, and D-06 remain OPEN**.

---

## 1. Executive Summary & Problem Context

In earlier extraction slices, generic filesystem reading was successfully extracted into `storage-layer-rust`. In production, CAS metadata lookups (`head_blob`) and payload reads (`open_blob`) execute through the extracted `storage_fs::FsMetadataReader` via `FsBlobCasReadAdapter` beneath a single pinned directory descriptor using Linux `openat2` containment (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`).

Subsequently, `storage-fs` introduced:
1. Bounded, descriptor-relative directory enumeration (`FsMetadataReader::enumerate_dir`), bounded by entry counts and cumulative name bytes (`DirEnumerationLimits`).
2. Contained, descriptor-relative regular-file metadata inspection (`FsMetadataReader::inspect_file_metadata`), yielding genuine file size and modification timestamps.

In `registry-rust`, a test-only integration seam (`src/storage/fs/listing_seam.rs`) proved that these extracted primitives can supply complete garbage collection candidate records (`GcBlobCandidate`) and paginated candidate pages (`GcBlobPage`) while preserving registry-owned layout rules, ASCII lexical ordering, cursor progression, timestamp fallback, and listing version formatting.

However, **production CAS listing (`FsStorage::list_cas_blobs_page` in `src/storage/fs.rs:3348-3491`) remains entirely on legacy, uncontained pathname operations** (`tokio::fs::read_dir` and `tokio::fs::metadata`).

This document specifies the concrete production integration architecture to cut over `FsStorage::list_cas_blobs_page` to the extracted descriptor-relative implementation while resolving:
- **Architectural ownership and code deduplication:** Promoting the seam to a single production implementation (`src/storage/fs/listing.rs`) with crate-private production traits, removing `listing_seam.rs`.
- **Reader sharing and startup offloading:** Reusing the single probed `FsMetadataReader` initialized during blocking startup offload without reopening the root path.
- **Exact compatibility against actual source:** Distinguishing actual legacy behavior, committed seam mappings, and proposed production changes (notably missing-shard translation and initial root error handling).
- **Dual-budget scalability architecture:** Carrying distinct root and shard enumeration budgets (`FsListingBudgets`) with verified accounting.
- **Source-grounded configuration:** Aligning configuration parsing, aliasing precedence, and range validation with `src/config.rs`.
- **Operational limits:** Preserving caller GC mutation semantics (no rollback of earlier batches), explaining re-enumeration scaling, and maintaining established quality gates.

---

## 2. Source Implementation Inspection & Current Architecture

This design is derived from empirical inspection of the active codebase across `registry-rust` and `storage-layer-rust`.

### 2.1 Production Storage Construction & Reader Ownership
In `src/storage/fs.rs:190-227`, `FsStorage` is defined and initialized:

```rust
// src/storage/fs.rs:190-197
pub struct FsStorage {
    root: PathBuf,
    max_upload_bytes: u64,
    upload_hashes: Vec<Mutex<std::collections::HashMap<String, SerializableSha256>>>,
    referrer_locks: Vec<Mutex<()>>,
    repo_locks: std::sync::Mutex<std::collections::HashMap<String, std::fs::File>>,
    read_adapter: std::sync::Arc<read_adapter::FsBlobCasReadAdapter<storage_fs::FsMetadataReader>>,
}
```

Constructor initialization sequence (`src/storage/fs.rs:200-210`):
1. Ensures storage root directory exists on disk: `ensure_dir(&root)?` (`src/storage/fs.rs:201`).
2. Opens the root directory descriptor via host OS path resolution:
   `let reader = storage_fs::FsMetadataReader::open(&root).map_err(read_adapter::map_fs_startup_error)?;` (`src/storage/fs.rs:202-203`).
3. Probes the opened descriptor for `openat2` containment support:
   `reader.probe_capability().map_err(read_adapter::map_fs_startup_error)?;` (`src/storage/fs.rs:204-206`).
4. Wraps the probed reader in an `Arc` and passes it to the read adapter:
   `let read_adapter = std::sync::Arc::new(read_adapter::FsBlobCasReadAdapter::new(std::sync::Arc::new(reader)));` (`src/storage/fs.rs:207-209`).

In `src/storage/fs/read_adapter.rs:256-271`, `FsBlobCasReadAdapter` stores `reader: Arc<R>` privately. It exposes `pub(crate) fn reader(&self) -> &Arc<R>` strictly under `#[cfg(test)]` (`src/storage/fs/read_adapter.rs:268`).

### 2.2 Production Legacy Listing (`FsStorage::list_cas_blobs_page`)
The production implementation (`src/storage/fs.rs:3348-3491`) implements `GcStorage::list_cas_blobs_page`:
- **Path Resolution:** Operates entirely by joining path strings to `self.root`:
  `let root = self.root.join("blobs").join("sha256");` (`src/storage/fs.rs:3355`).
- **Missing Root Masking:** Masks any error from `metadata(&root)` as an empty page:
  ```rust
  // src/storage/fs.rs:3357-3362
  if tokio::fs::metadata(&root).await.is_err() {
      return Ok(GcBlobPage {
          items: Vec::new(),
          next_cursor: None,
      });
  }
  ```
- **Uncontained Traversal:** Uses Tokio asynchronous filesystem wrappers (`tokio::fs::read_dir` and `tokio::fs::metadata`). These dispatch to Tokio's internal blocking thread pool, resolving paths through standard OS VFS and traversing intermediate symlinks.
- **Entry Type Validation vs Replacement Timing:**
  During directory enumeration, legacy listing validates `ft.is_dir()` on prefixes (`src/storage/fs.rs:3386`) and `ft.is_file()` on blobs (`src/storage/fs.rs:3426`). A pre-existing directory inside a shard fails closed immediately with `StorageErrorKind::CorruptData`.
  However, **if a regular file is replaced with a directory after `read_dir` validates `ft.is_file()`**, the subsequent `tokio::fs::metadata(&path).await` (`src/storage/fs.rs:3457`) succeeds on the directory! It sets `size = meta.len()` (the directory inode size, typically 4096 bytes) and yields the directory as a valid `GcBlobCandidate`.

### 2.3 Committed Test-Only Seam (`src/storage/fs/listing_seam.rs`)
The committed seam defines:
- Narrow abstractions: `CasDirEnumerator` (`listing_seam.rs:80-87`), `CasMetadataInspector` (`listing_seam.rs:94-97`), and combined `CasListingSource` (`listing_seam.rs:100-101`).
- Implementations for `storage_fs::FsMetadataReader`, `&T`, and `Arc<T>` (`listing_seam.rs:103-155`).
- Core listing algorithm: `list_cas_blobs_page_seam` (`listing_seam.rs:320-447`).
- **Committed Error Mappings in Source:**
  - `translate_dir_error` (`listing_seam.rs:158-189`):
    - `FsDirError::NotFound { .. } => StorageError::NotFound` (`listing_seam.rs:160`).
    - `FsDirError::PermissionDenied { ref source, .. } => StorageError::permission_denied(source.to_string())` (`listing_seam.rs:164-166`).
    - `FsDirError::NotADirectory { ref path } => StorageError::corrupt_data(...)` (`listing_seam.rs:161-163`).
    - `FsDirError::ResolutionRejected { ref source, .. } => StorageError::io(source.to_string())` (`listing_seam.rs:167`).
    - `FsDirError::LimitExceeded { reason } => StorageError::backend(...)` (`listing_seam.rs:174-176`).
    - `FsDirError::Io { ref source } => StorageError::io(source.to_string())` (`listing_seam.rs:180`).
    - `FsDirError::RuntimeMissing(ref err)` / `TaskJoinFailed(ref err) => StorageError::backend(...)` (`listing_seam.rs:181-186`).
  - `translate_inspect_error` (`listing_seam.rs:195-270`):
    - `ReadError::NotFound { key, .. } => StorageError::io("candidate blob disappeared before metadata inspection: {key}")` (`listing_seam.rs:197-204`).
    - `ReadError::PermissionDenied => StorageError::io(...)` (`listing_seam.rs:205-216`).
    - `FsMetadataError::UnsupportedObjectType => StorageError::corrupt_data(...)` (`listing_seam.rs:226-230`).
    - `FsMetadataError::InvalidMetadata => StorageError::corrupt_data(...)` (`listing_seam.rs:244-246`).
    - `std::io::Error` with raw OS error `ENOTDIR => StorageError::corrupt_data(...)` (`listing_seam.rs:256-258`).
  - *Initial CAS Root Handling:* In `listing_seam.rs:335-339`, only `FsDirError::NotFound` on `blobs/sha256` is intercepted to return `Ok(empty page)`. Non-NotFound errors (e.g. `PermissionDenied` or `NotADirectory`) propagate through `translate_dir_error`.
  - *Shard Directory Handling:* In `listing_seam.rs:380`, shard directory enumeration errors map directly via `translate_dir_error`. If a shard directory is missing (`FsDirError::NotFound`), it **currently propagates `StorageError::NotFound`**, not `StorageError::io`.

### 2.4 Consumer Layer: `CasBlobTraverser` & GC Calling Semantics
In `src/blob_gc/traverser.rs:27-86`, `CasBlobTraverser<'a>` consumes `&'a dyn storage::GcStoragePort`.
Calls to `CasBlobTraverser::new(storage, 100)` occur in:
1. `plan_blob_gc` (`src/blob_gc/mod.rs:143`): scan phase planning unreferenced candidates.
2. `run_blob_gc` (`src/blob_gc/mod.rs:249`): quarantine sweep phase.
3. `verify_or_estimate_s3_sweep` (`src/blob_gc/mod.rs:608`): dry run estimation.

**Per-Batch Mutation and Non-Rollback in GC:**
In `run_blob_gc` (`src/blob_gc/mod.rs:270-317`), candidates on each page batch are processed and mutated immediately:
`storage.quarantine_blob(&permit, &candidate.digest, &candidate.version).await` (`src/blob_gc/mod.rs:290-292`).
If a subsequent listing call on a later page fails (e.g. returning an `Io` error), the outer loop aborts and returns `Err(BlobGcError::StorageTraversal(err))`.
**Earlier pages that already completed quarantine mutations are NOT rolled back**. Any blobs quarantined during preceding batches remain in quarantine on disk.

### 2.5 Extracted Directory Enumeration Architecture
In `crates/storage-fs/src/dir.rs:27-45, 275-330, 488-520`, directory enumeration is implemented by:
1. Opening a fresh directory descriptor beneath the pinned root via `openat2` (`O_RDONLY | O_DIRECTORY | O_CLOEXEC`).
2. Transferring descriptor ownership to `libc::fdopendir`.
3. Iterating directory entries via a `libc::readdir` loop (`crates/storage-fs/src/dir.rs:500-520`). Libc buffers directory entries from the kernel via `getdents64` syscalls until end-of-directory.
4. Calling `libc::closedir` via `DirGuard` RAII guard upon task exit.
Enumeration is an iteration loop calling `libc::readdir` over the `DIR*` stream, bounded by `DirEnumerationLimits`; it is not an atomic `getdents64` syscall.

### 2.6 Probe Scope & Explicit Non-Guarantees
`FsMetadataReader::probe_capability` (`crates/storage-fs/src/reader.rs:202-235`) executes `openat2` on `"."` with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS` and flags `O_PATH | O_DIRECTORY | O_CLOEXEC`, followed by `fstat`.
As established in `crates/storage-fs/src/reader.rs:210-220`, the probe **does not**:
- Test directory enumeration (`getdents64` / `fdopendir`).
- Validate child path existence or permissions (`blobs/sha256`).
- Verify regular-file access or payload procfs reopening (`/proc/self/fd/<fd>`).
- Guarantee future syscall availability across spawned threads.

### 2.7 Existing Startup Offload Boundary
In `registry-rust`, storage initialization is offloaded away from the asynchronous runtime worker threads:
- In `src/runtime.rs:250-262` and `src/storage/mod.rs:927-947`, `storage_wiring_try_from_config_async_with_factory` inspects `config.storage_backend`:
  ```rust
  // src/storage/mod.rs:934-946
  match config.storage_backend {
      StorageBackend::Filesystem => {
          let config_clone = config.clone();
          tokio::task::spawn_blocking(move || storage_factory(&config_clone))
              .await
              .map_err(|join_err| {
                  StorageError::backend(format!(
                      "filesystem storage initialization task failed: {join_err}"
                  ))
              })?
      }
      StorageBackend::S3 => storage_factory(config),
  }
  ```
- The synchronous factory passed in `src/runtime.rs:272` is `crate::storage::storage_wiring_try_from_config`.
- Inside `storage_wiring_try_from_config` (`src/storage/mod.rs:829-835`), execution takes place on the spawned blocking thread. It is here that `FsStorage::try_new` invokes `ensure_dir(&root)?`, `FsMetadataReader::open(&root)?`, and `reader.probe_capability()?`.
- Preserving this boundary ensures that filesystem startup operations (path creation, descriptor acquisition, capability probing) never block Tokio async worker threads.

### 2.8 Version Strings and Call Sites of `compute_fs_blob_version`
In `src/storage/fs.rs`:
- `compute_fs_blob_version` (`src/storage/fs.rs:3318-3345`) computes a composite version string `fs:{len}:{mtime}:{content_sha256}` by hashing file content if `len > 0`.
- Its **only** call sites are:
  1. `get_blob_version(&self, digest: &Digest)` (`src/storage/fs.rs:3623`).
  2. `delete_blob_conditional(&self, digest: &Digest, expected_version: &BlobObjectVersion)` (`src/storage/fs.rs:3657`).
- In contrast, `quarantine_blob` (`src/storage/fs.rs:3495-3545`) receives `_version: &BlobObjectVersion` as an unused argument (`src/storage/fs.rs:3497`). It performs a direct `tokio::fs::rename` into quarantine and records the quarantine timestamp; it **does not** compute or verify `compute_fs_blob_version`.
- In listing, `convert_candidate` produces `BlobObjectVersion(format!("{version_seconds}:{size}"))` (`listing_seam.rs:287`), exactly matching legacy listing (`src/storage/fs.rs:3466`).

---

## 3. Concrete Production Integration Structure

### 3.1 Crate-Private Production Abstractions & Single Policy Implementation
To prevent split policy implementations between test and production:
1. **Rename and Promote Seam:**
   Promote `src/storage/fs/listing_seam.rs` into `src/storage/fs/listing.rs`, removing `listing_seam.rs` completely.
   In `src/storage/fs.rs`:
   Replace `#[cfg(test)] mod listing_seam;` with:
   ```rust
   #[path = "fs/listing.rs"]
   pub(crate) mod listing;
   ```
2. **Crate-Private Production Traits:**
   `CasDirEnumerator`, `CasMetadataInspector`, and `CasListingSource` are declared as `pub(crate)` production traits in `src/storage/fs/listing.rs`.
   `storage_fs::FsMetadataReader` implements these traits in production code.
3. **Test-Only Abstractions Kept Under `cfg(test)`:**
   `RecordingFakeDirEnumerator`, `InterceptingListingWrapper`, and `SeamGcStorageBridge` remain strictly inside `#[cfg(test)] mod tests` in `listing.rs`.
4. **Architectural Boundary Invariants:**
   - **No listing contract in `storage-core`:** Quality Gate **O-03** remains OPEN.
   - **No registry policy in `storage-fs`:** CAS structure, shard prefixes, digest parsing, lexical cursors, and listing version formats remain 100% in `registry-rust`.

### 3.2 Dual-Budget Architecture: `FsListingBudgets`
Rather than passing a single `DirEnumerationLimits` through an interface that requires two distinct operational bounds, we define a registry-owned dual-budget type:

```rust
// In src/storage/fs/listing.rs
use storage_fs::DirEnumerationLimits;
use crate::storage::StorageError;

/// Production resource limits for CAS listing enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsListingBudgets {
    /// Budget applied when enumerating the CAS root directory (`blobs/sha256`).
    pub root: DirEnumerationLimits,
    /// Budget applied when enumerating individual shard directories (`blobs/sha256/<p2>`).
    pub shard: DirEnumerationLimits,
}

impl Default for FsListingBudgets {
    fn default() -> Self {
        Self {
            root: DirEnumerationLimits::new(512, 16 * 1024),
            shard: DirEnumerationLimits::new(100_000, 8 * 1024 * 1024),
        }
    }
}

impl FsListingBudgets {
    /// Default maximum entries permitted in the CAS root directory (`blobs/sha256`).
    pub const DEFAULT_ROOT_MAX_ENTRIES: usize = 512;
    /// Default maximum total raw filename bytes permitted in the CAS root directory.
    pub const DEFAULT_ROOT_MAX_NAME_BYTES: usize = 16 * 1024; // 16,384

    /// Default maximum entries permitted in a single CAS shard directory (`blobs/sha256/<p2>`).
    pub const DEFAULT_SHARD_MAX_ENTRIES: usize = 100_000;
    /// Default maximum total raw filename bytes permitted in a single CAS shard directory.
    pub const DEFAULT_SHARD_MAX_NAME_BYTES: usize = 8 * 1024 * 1024; // 8,388,608

    /// Constructs a new budget pair with distinct root and shard enumeration limits.
    pub fn new(root: DirEnumerationLimits, shard: DirEnumerationLimits) -> Self {
        Self { root, shard }
    }

    /// Validates budget bounds against proposed system limits (unresolved, deferred).
    pub fn validate(&self) -> Result<(), StorageError> {
        // Root limits validation
        if self.root.max_entries() == 0 || self.root.max_entries() > 4_096 {
            return Err(StorageError::configuration(format!(
                "invalid root max_entries {}: must be between 1 and 4096",
                self.root.max_entries()
            )));
        }
        if self.root.max_total_name_bytes() < 2 || self.root.max_total_name_bytes() > 64 * 1024 {
            return Err(StorageError::configuration(format!(
                "invalid root max_total_name_bytes {}: must be between 2 and 65536",
                self.root.max_total_name_bytes()
            )));
        }
        // Shard limits validation
        if self.shard.max_entries() == 0 || self.shard.max_entries() > 1_000_000 {
            return Err(StorageError::configuration(format!(
                "invalid shard max_entries {}: must be between 1 and 1000000",
                self.shard.max_entries()
            )));
        }
        if self.shard.max_total_name_bytes() < 64 || self.shard.max_total_name_bytes() > 128 * 1024 * 1024 {
            return Err(StorageError::configuration(format!(
                "invalid shard max_total_name_bytes {}: must be between 64 and 134217728",
                self.shard.max_total_name_bytes()
            )));
        }
        Ok(())
    }
}
```

> [!NOTE]
> In the separate budgets slice, `FsListingBudgets` has been implemented and verified in the test-only listing seam (`src/storage/fs/listing_seam.rs`). Provisional defaults (Root: 512 entries / 16,384 raw filename bytes; Shard: 100,000 entries / 8,388,608 raw filename bytes) are accepted as defensive safeguards on in-memory tmpfs, not Docker standards or production capacity guarantees. Production routing (`FsStorage::list_cas_blobs_page`) remains unchanged on the legacy implementation. Higher configurable maxima (`validate()`) and production cutover remain unresolved.

The production listing entrypoint accepts `FsListingBudgets`:
```rust
pub(crate) async fn list_cas_blobs_page_impl(
    source: &(impl CasListingSource + ?Sized),
    cursor: Option<&GcCursor>,
    limit: usize,
    budgets: FsListingBudgets,
) -> Result<GcBlobPage, StorageError>
```
- When enumerating `blobs/sha256`, it passes `budgets.root`.
- When enumerating each `blobs/sha256/<p2>` shard, it passes `budgets.shard`.

### 3.3 Storage Struct and Shared Reader Wiring
`FsStorage` stores `reader: Arc<storage_fs::FsMetadataReader>` and `listing_budgets: FsListingBudgets`:

```rust
pub struct FsStorage {
    root: PathBuf,
    max_upload_bytes: u64,
    upload_hashes: Vec<Mutex<std::collections::HashMap<String, SerializableSha256>>>,
    referrer_locks: Vec<Mutex<()>>,
    repo_locks: std::sync::Mutex<std::collections::HashMap<String, std::fs::File>>,
    reader: std::sync::Arc<storage_fs::FsMetadataReader>,
    read_adapter: std::sync::Arc<read_adapter::FsBlobCasReadAdapter<storage_fs::FsMetadataReader>>,
    listing_budgets: FsListingBudgets,
}
```

**Checked Construction Path (`src/storage/fs.rs`):**
```rust
impl FsStorage {
    /// Preserves backward compatibility for callers using default budgets.
    pub fn try_new(root: PathBuf, max_upload_bytes: u64) -> Result<Self, StorageError> {
        Self::try_new_with_budgets(root, max_upload_bytes, FsListingBudgets::default())
    }

    /// Constructs FsStorage with explicit listing budgets, validating bounds.
    pub fn try_new_with_budgets(
        root: PathBuf,
        max_upload_bytes: u64,
        listing_budgets: FsListingBudgets,
    ) -> Result<Self, StorageError> {
        listing_budgets.validate()?;
        ensure_dir(&root)?;
        let reader = storage_fs::FsMetadataReader::open(&root)
            .map_err(read_adapter::map_fs_startup_error)?;
        reader
            .probe_capability()
            .map_err(read_adapter::map_fs_startup_error)?;
        let reader = std::sync::Arc::new(reader);
        let read_adapter = std::sync::Arc::new(read_adapter::FsBlobCasReadAdapter::new(
            std::sync::Arc::clone(&reader),
        ));

        Ok(Self {
            root,
            max_upload_bytes,
            upload_hashes: (0..256).map(|_| Mutex::new(std::collections::HashMap::new())).collect(),
            referrer_locks: (0..256).map(|_| Mutex::new(())).collect(),
            repo_locks: std::sync::Mutex::new(std::collections::HashMap::new()),
            reader,
            read_adapter,
            listing_budgets,
        })
    }
}
```
Both `read_adapter` and `list_cas_blobs_page` share the exact same `Arc<FsMetadataReader>` instance opened and capability-probed once at startup. Direct callers cannot bypass validation.

### 3.4 Delegation Pattern in `FsStorage`
In `src/storage/fs.rs:3346-3491`, `GcStorage::list_cas_blobs_page` delegates directly:

```rust
#[async_trait]
impl GcStorage for FsStorage {
    async fn list_cas_blobs_page(
        &self,
        cursor: Option<&GcCursor>,
        limit: usize,
    ) -> Result<GcBlobPage, StorageError> {
        listing::list_cas_blobs_page_impl(
            self.reader.as_ref(),
            cursor,
            limit,
            self.listing_budgets,
        )
        .await
    }
    // ... quarantine_blob, restore_quarantined_blob, delete_blob_conditional preserved
}
```

---

## 4. Reader Ownership, Startup Offloading, and Pinned Root Lifetime

### 4.1 Single Pinned Root Descriptor Sharing
- `FsStorage` opens `root` once during startup.
- `head_blob`, `open_blob`, and `list_cas_blobs_page` all operate beneath the same pinned file description (`root_fd`).
- **Root Coherence Across Reads:** Any entry observed during `enumerate_dir` is inspected via `inspect_file_metadata` beneath the same root descriptor.
- **Reopening Prohibited:** Reopening `self.root` on each listing call would introduce pathname TOCTOU race conditions and break descriptor containment guarantees.

### 4.2 Preserving Existing Startup Offloading
The integration preserves the established startup offload pattern:
1. `build_server_runtime` (`src/runtime.rs:265-275`) dispatches to `storage_wiring_try_from_config_async_with_factory`.
2. For `StorageBackend::Filesystem`, `tokio::task::spawn_blocking` offloads the synchronous factory to a worker thread:
   ```rust
   // src/storage/mod.rs:934-944
   match config.storage_backend {
       StorageBackend::Filesystem => {
           let config_clone = config.clone();
           tokio::task::spawn_blocking(move || storage_factory(&config_clone))
               .await
               .map_err(|join_err| {
                   StorageError::backend(format!(
                       "filesystem storage initialization task failed: {join_err}"
                   ))
               })?
       }
       StorageBackend::S3 => storage_factory(config),
   }
   ```
3. Inside the offloaded task, `storage_wiring_try_from_config` extracts the validated `FsListingBudgets` and calls `FsStorage::try_new_with_budgets`:
   ```rust
   // Proposed replacement in src/storage/mod.rs:829-835
   pub fn storage_wiring_try_from_config(config: &Config) -> Result<StorageWiring, StorageError> {
       match config.storage_backend {
           StorageBackend::Filesystem => {
               let fs_storage = fs::FsStorage::try_new_with_budgets(
                   config.fs_root.clone(),
                   config.max_upload_bytes,
                   config.fs_listing_budgets,
               )?;
               Ok(StorageWiring::from_backend(Arc::new(fs_storage)))
           }
           StorageBackend::S3 => {
               // ... preserved s3 wiring
           }
       }
   }
   ```
4. This preserves execution away from the async runtime, maintains task-join error handling, and ensures root opening and capability probing execute synchronously before the server accepts traffic.

### 4.3 Async and Blocking Runtime Boundaries
- Legacy production calls `tokio::fs::read_dir` and `tokio::fs::metadata`, which dispatch internally to Tokio's blocking thread pool.
- The extracted primitives (`enumerate_dir`, `inspect_file_metadata`) explicitly query `tokio::runtime::Handle::try_current()` and invoke `handle.spawn_blocking`. If called outside a Tokio runtime, they return strongly typed `FsDirError::RuntimeMissing` or `FsMetadataError::RuntimeMissing`.
- If a blocking task panics or fails to join, the API returns strongly typed `TaskJoinFailed`.
- Blocking workers execute POSIX syscalls on worker threads; they do not hold locks on `FsStorage`.

### 4.4 Pathname Mutation Asymmetry & Operational Root Stability (Policy A)
- Mutation operations remain strictly pathname-based:
  - `quarantine_blob` (`src/storage/fs.rs:3537`): `tokio::fs::rename(&src, &dest)`.
  - `restore_quarantined_blob` (`src/storage/fs.rs:3595`): `tokio::fs::rename(&src, &dest)`.
  - `delete_blob_conditional` (`src/storage/fs.rs:3664`): `tokio::fs::remove_file(&path)`.
- If the root directory is moved while the process is running, `self.reader` continues to read the original pinned inode, while mutations target the new pathname. Operational root stability (Policy A from `filesystem-production-read-cutover.md`) remains an absolute operational requirement.

---

## 5. Comprehensive Compatibility & Error-Policy Decision Table

The table below contrasts actual legacy production behavior, actual committed seam behavior, and recommended production behavior across all failure scenarios:

| Failure Scenario | Actual Legacy Production (`FsStorage::list_cas_blobs_page`) | Actual Committed Seam (`listing_seam.rs`) | Recommended Production Behavior | Approval Status / Proposed Change | Existing Evidence & Missing Tests |
|---|---|---|---|---|---|
| **1. Missing CAS root** (`blobs/sha256` absent) | `Ok(empty page)` via `metadata(&root).is_err()`. | `Ok(empty page)` via intercepted `FsDirError::NotFound` on root. | `Ok(empty page)`. | Unchanged. | Evidence: `test_real_fs_absent_cas_root_returns_empty_page`. |
| **2. Initial CAS root: Intermediate path is regular file** (e.g. `blobs` is a regular file) | Suppressed into `Ok(empty page)` by `metadata(&root).await.is_err()`. | Fails closed with `StorageErrorKind::CorruptData` (`FsDirError::NotADirectory` from `openat2`). | Fails closed with `StorageErrorKind::CorruptData`. | **REQUIRES APPROVAL** (surfaces structural corruption instead of masking it as empty repository). | Evidence: `test_real_fs_initial_not_a_directory_error_not_suppressed`. Legacy test `test_list_cas_blobs_initial_metadata_error_suppression` will need intentional update. |
| **3. Initial CAS root: Final `blobs/sha256` is regular file** | `metadata(&root)` succeeds, then `read_dir(&root)` fails with `StorageErrorKind::Io` (`ENOTDIR`). | Fails closed with `StorageErrorKind::CorruptData` (`FsDirError::NotADirectory` from `openat2`). | Fails closed with `StorageErrorKind::CorruptData`. | **REQUIRES APPROVAL** (unifies non-directory taxonomy to `CorruptData`). | Evidence: `test_fake_corrupt_entry_types_and_names`. **Missing test**: real fs final-component test (to be added). |
| **4. Initial CAS root: Permission Denied** (`EACCES`) | Suppressed into `Ok(empty page)` by `metadata(&root).await.is_err()`. | Fails closed with `StorageErrorKind::PermissionDenied` (`FsDirError::PermissionDenied`). | Fails closed with `StorageErrorKind::PermissionDenied`. | **REQUIRES APPROVAL** (prevents false-empty GC passes on permission failure). | Evidence: `test_fake_typed_dir_error_mappings`. **Missing test**: real fs unprivileged permission test (must run unprivileged or be explicitly ignored; skipping not counted as passing). |
| **5. Initial CAS root: Ordinary I/O Failure** (`EIO`) | Suppressed into `Ok(empty page)` if on `metadata`; `StorageErrorKind::Io` if on `read_dir`. | Fails closed with `StorageErrorKind::Io` (`FsDirError::Io`). | Fails closed with `StorageErrorKind::Io`. | **REQUIRES APPROVAL** (eliminates error suppression). | Evidence: `test_fake_typed_dir_error_mappings`. |
| **6. Shard disappears during listing** (deleted after root enumeration) | Fails whole page with `StorageErrorKind::Io` via `read_dir(&shard)`. | Propagates `StorageError::NotFound` via `translate_dir_error(FsDirError::NotFound)`. | Fails whole page with `StorageErrorKind::Io`. | **PROPOSED CHANGE REQUIRING APPROVAL**: Promoted seam at shard call site must intercept `FsDirError::NotFound` and translate it to `StorageError::io` to preserve legacy whole-page I/O failure. | Evidence: `test_fake_suppression_on_shard_failure` (proves whole-page abort). **Missing test**: missing-shard translation test (to be added). |
| **7. Ancestor symlink beneath root** (e.g. `blobs` -> external dir) | Traverses symlink via OS VFS; succeeds. | Fails closed with `StorageErrorKind::Io` (`ResolutionRejected` / `ELOOP`). | Fails closed with `StorageErrorKind::Io`. | **REQUIRES APPROVAL** (enforces Policy C containment; rejects uncontained storage setups). | Evidence: `test_real_fs_ancestor_symlink_rejection`. Legacy test `test_list_cas_blobs_symlink_resolution_through_ancestor_paths` cases B/C will change. |
| **8. Non-regular / symlink entry in CAS root or shard** | Fails closed with `StorageErrorKind::CorruptData` via `ft.is_dir()` / `ft.is_file()`. | Fails closed with `StorageErrorKind::CorruptData` via `DirEntryType` check. | Fails closed with `StorageErrorKind::CorruptData`. | Unchanged. | Evidence: `test_real_fs_fails_closed_on_symlinked_shard`, `test_real_fs_fails_closed_on_symlinked_blob`. |
| **9. Candidate disappears before inspection** (unlinked after `readdir`) | Fails whole page with `StorageErrorKind::Io` via `tokio::fs::metadata`. | Fails whole page with `StorageErrorKind::Io` via `ReadError::NotFound`. | Fails whole page with `StorageErrorKind::Io`. | Unchanged (preserves whole-page failure; rejects silent skipping). | Evidence: `test_real_fs_disappeared_blob_between_enumeration_and_inspection_fails_closed_io`. |
| **10. Candidate replaced with a symlink** | Traverses symlink on `metadata()`; succeeds if target exists. | Fails closed with `StorageErrorKind::Io` (`ResolutionRejected` / `ELOOP`). | Fails closed with `StorageErrorKind::Io`. | **REQUIRES APPROVAL** (strictly prevents symlink injection into GC). | Evidence: `test_real_fs_symlink_substituted_blob_fails_closed_io`. |
| **11. Candidate replaced with a directory after `readdir`** | Succeeds! `metadata()` succeeds on directories; candidate gets directory size (e.g. 4096). | Fails closed with `StorageErrorKind::CorruptData` (`UnsupportedObjectType`). | Fails closed with `StorageErrorKind::CorruptData`. | **REQUIRES APPROVAL** (fixes legacy vulnerability where directories became GC candidates). | Evidence: `test_real_fs_directory_substituted_blob_fails_closed_corrupt_data`. |
| **12. Candidate replaced with another regular file** | Reads replacement metadata. | Observes replacement metadata at resolution time. | Observes replacement metadata at resolution time. | Unchanged (point-in-time observation). | Evidence: `test_real_fs_regular_file_replacement_observed_at_resolution_time`. |
| **13. Raw `ENOTDIR` during metadata inspection** | Fails with `StorageErrorKind::Io`. | Maps to `StorageErrorKind::CorruptData` via downcast check on `libc::ENOTDIR`. | Fails closed with `StorageErrorKind::CorruptData`. | **REQUIRES APPROVAL** (treats non-directory resolution failure as corrupt storage). | **Missing test**: raw `libc::ENOTDIR` inspection downcast test (to be added; not exercised in fake test). |
| **14. Metadata inspection failures** (`PermissionDenied`, `StatFailed`) | Fails whole page with `StorageErrorKind::Io`. | Maps to `StorageErrorKind::Io`. | Fails closed with `StorageErrorKind::Io`. | Unchanged. | Evidence: `test_fake_typed_error_mappings_unaffected_by_diagnostic_words`. |
| **15. InvalidMetadata error** (e.g. negative file size, invalid nanoseconds, unrepresentable conversion) | N/A (legacy lacked checked conversion; used epoch fallback). | Maps to `StorageErrorKind::CorruptData`. | Fails closed with `StorageErrorKind::CorruptData`. | **REQUIRES APPROVAL** (enforces storage-fs metadata validation contract). | Evidence: `test_fake_typed_error_mappings_unaffected_by_diagnostic_words`. |
| **16. Runtime / task join failure** (`RuntimeMissing`, `TaskJoinFailed`) | N/A (Tokio wrappers dispatch directly). | Maps to `StorageErrorKind::Backend`. | Fails closed with `StorageErrorKind::Backend`. | Unchanged from `read_adapter.rs` precedent. | **Missing test**: direct construction test in seam (to be added). |
| **17. Unsupported platform / syscall** (`ENOSYS`, non-Linux) | N/A (standard POSIX calls). | Maps to `StorageErrorKind::Configuration`. | Fails closed with `StorageErrorKind::Configuration`. | Unchanged from `read_adapter.rs` precedent. | Evidence: `test_fake_typed_error_mappings_unaffected_by_diagnostic_words`. |
| **18. Enumeration limit exceeded** (`DirEnumerationLimits`) | Unbounded (memory exhaustion risk). | Maps to `StorageErrorKind::Backend` (`FsDirError::LimitExceeded`). | Fails closed with `StorageErrorKind::Backend`. | **REQUIRES APPROVAL** (introduces explicit capacity limits). | Evidence: `test_real_fs_budget_limits_entry_count`, `test_real_fs_budget_limits_name_bytes`. |

---

## 6. Directory Enumeration Budgets & Scalability Strategy

### 6.1 Total Scan Work and Re-Enumeration of Shards Across Pages
- In `listing_seam.rs:371` (and the promoted `listing.rs`), pagination iterates:
  ```rust
  'outer: for p2 in prefix_dirs {
      let shard_key_str = format!("blobs/sha256/{p2}");
      // ...
      let shard_entries = source.enumerate_dir(Some(&shard_key), budget).await...
  ```
- **Critical Scaling Factor:** On every page call, the algorithm enumerates the root CAS directory and then begins enumerating shard directories from `p2 = "00"` upwards. Shards that were fully processed on earlier pages are re-enumerated until the loop reaches entries exceeding the continuation cursor (`digest_str.as_str() > cursor`).
- If a repository has 256 shards and a GC pass requires 100 pages to traverse, earlier shards are re-enumerated on each page. Total scan work across a complete GC pass is therefore $O(P 	imes S)$, where $P$ is the page count and $S$ is the number of entries across traversed shards.

### 6.2 Qualitative Memory & Allocation Accounting
`max_total_name_bytes` in `DirEnumerationLimits` measures **only** the cumulative byte length of filename strings collected during `readdir`. It does **not** represent a total heap memory ceiling because it excludes:
1. **Entry Struct and Vector Allocations:**
   `DirEntry` structs, dynamic vector reallocations, and libc directory stream buffers inside the worker task.
2. **Temporary Allocations & Sorting:**
   Converting entries to lowercase `String` instances (`listing_seam.rs:405`) and sorting them in memory before applying cursor filtering.
3. **Concurrent Listing Calls:**
   Each concurrent call to `list_cas_blobs_page` allocates its own working buffers on the blocking thread pool.
4. **Per-Directory Scope:**
   The budget bounds a single directory enumeration call. It does not bound aggregate memory across multiple shards or repeated shard scans.

### 6.3 Proposed Default Budgets (Unvalidated Operational Proposals)
The following values are **unvalidated proposed defaults** requiring operator approval:
- **Root CAS Directory (`blobs/sha256`):**
  - Expected entries: at most 256 two-character hexadecimal shard directories (`00`..`ff`).
  - Proposed limits: `max_entries = 512`, `max_total_name_bytes = 16 * 1024` (16 KiB).
- **Shard Directories (`blobs/sha256/<p2>`):**
  - Proposed limits: `max_entries = 100_000`, `max_total_name_bytes = 8 * 1024 * 1024` (8 MiB).
  - *Rationale:* 64-character hex blob names require 64 bytes each; 100,000 names consume ~6.4 MiB of raw name bytes.
  - *Capacity Warning:* This per-shard limit is exceeded by **any single oversized shard** regardless of total repository blob count or average distribution. If an unbalanced workload places 100,001 blobs in shard `0a`, listing fails closed immediately with `StorageErrorKind::Backend`.

### 6.4 Cancellation & Worker Task Semantics
- In Tokio, dropping an awaiting future (`tokio::select!`, timeout, or client disconnect) **does not cancel** a task dispatched to `spawn_blocking`.
- The synchronous `readdir` loop continues executing on the worker thread until `closedir` completes or the budget is exhausted.
- Worker-allocated resources remain held until the blocking closure finishes.
- `DirEnumerationLimits` bounds work per directory read; it is not a syscall execution deadline or global concurrency throttle.

---

## 7. Configuration Parsing, Validation, and Startup Wiring

### 7.1 Existing Configuration Loader Precedence
In `src/config.rs:1133-1135`, configuration loading follows the established precedence:
`defaults < config file(s) < env vars`.

### 7.2 Proposed Configuration Options & Environment Aliasing
We propose four optional configuration fields in `Config` (`src/config.rs`), read via `env_usize_opt`:

| Configuration Field | Type | Default | Evaluated Environment Aliases (in precedence order) |
|---|---|---|---|
| `fs_listing_max_shard_entries` | `Option<usize>` | `100_000` | 1. `REGISTRY__STORAGE__FS__LISTING__MAX_SHARD_ENTRIES`<br>2. `STORAGE_FS_LISTING_MAX_SHARD_ENTRIES` |
| `fs_listing_max_shard_name_bytes` | `Option<usize>` | `8 * 1024 * 1024` (8 MiB) | 1. `REGISTRY__STORAGE__FS__LISTING__MAX_SHARD_NAME_BYTES`<br>2. `STORAGE_FS_LISTING_MAX_SHARD_NAME_BYTES` |
| `fs_listing_max_root_entries` | `Option<usize>` | `512` | 1. `REGISTRY__STORAGE__FS__LISTING__MAX_ROOT_ENTRIES`<br>2. `STORAGE_FS_LISTING_MAX_ROOT_ENTRIES` |
| `fs_listing_max_root_name_bytes` | `Option<usize>` | `16 * 1024` (16 KiB) | 1. `REGISTRY__STORAGE__FS__LISTING__MAX_ROOT_NAME_BYTES`<br>2. `STORAGE_FS_LISTING_MAX_ROOT_NAME_BYTES` |

**Environment Aliasing Resolution:**
In `src/config.rs:2912-2922`, `env_str_any` iterates through alias keys in slice order. If both the hierarchical alias (`REGISTRY__STORAGE__FS__...`) and flat alias (`STORAGE_FS_...`) are set, **the hierarchical alias wins**.

**Config File Parsing:**
In `FileStorageFs` (`src/config.rs:1002-1005`), we add optional deserialization fields under `[storage.fs]`:
`listing_max_shard_entries`, `listing_max_shard_name_bytes`, `listing_max_root_entries`, `listing_max_root_name_bytes`.
No new configuration-file parsing mechanism is introduced; it uses the existing Serde TOML loader.

> [!IMPORTANT]
> Operators cannot currently configure these values. That capability is only proposed here and must be implemented during the integration slice.

### 7.3 Numeric Parsing, Overflow, and Range Validation Rules
- **Numeric Parsing & Overflow:**
  `env_usize_opt` uses `v.trim().parse::<usize>()`. Non-numeric strings or values overflowing `usize::MAX` return `Err(ConfigError::InvalidEnvValue { key, expected: "unsigned integer" })`.
- **Validation Scope:**
  Validation applies when `storage_backend == StorageBackend::Filesystem`.
- **Proposed Limit Ranges with Rationale:**
  - `root_max_entries`: Must be in `[1, 4_096]`. (CAS root has at most 256 hex shards; 4,096 provides ample room for maintenance subdirectories while preventing memory attacks).
  - `root_max_name_bytes`: Must be in `[2, 65_536]` (64 KiB). (A hex shard name is 2 bytes; 256 entries * 2 bytes = 512 bytes. 64 KiB allows substantial margin).
  - `shard_max_entries`: Must be in `[1, 1_000_000]`. (1 million entries per shard at 64 bytes/hash is 64 MB raw name bytes; beyond this, single-directory sorting creates unacceptable memory spikes).
  - `shard_max_name_bytes`: Must be in `[64, 134_217_728]` (128 MiB). (A SHA-256 hex name is 64 bytes; 128 MiB bounds the maximum raw name buffer).
- **Shared Validation Enforcement:**
  Validation is located inside `FsListingBudgets::validate(&self)` and called directly by `FsStorage::try_new_with_budgets`. Direct callers and tests cannot bypass validation.
- **Explicit Non-Guarantee:**
  These upper bounds do **not** guarantee safe total memory consumption across concurrent listing calls or large worker heaps.

---

## 8. Preserved Registry Invariants

The promoted integration maintains complete parity with all registry invariants:
1. **Candidate Metadata Parity & Pre-Epoch Timestamps:**
   - `digest`: lowercased SHA-256 string validated via:
     `name_str.len() == 64 && name_str.to_ascii_lowercase().starts_with(&p2) && name_str.chars().all(|c| c.is_ascii_hexdigit())` followed by `Digest::parse(&digest_str)`.
   - `size`: actual file byte size from `inspected.size()`.
   - `last_modified`: exact `SystemTime` from `inspected.modified().unwrap_or(SystemTime::UNIX_EPOCH)`.
   - **Pre-epoch timestamps:** If `last_modified` is pre-epoch, `candidate.last_modified` retains the genuine pre-epoch `SystemTime`. In version calculation, `duration_since(UNIX_EPOCH)` yields `Duration::ZERO`, producing `version_seconds = 0`:
     `BlobObjectVersion(format!("{version_seconds}:{size}"))`.
2. **Deterministic Pagination & Terminal Pages:**
   - User limit is clamped to `[1, 1000]`: `limit = limit.min(1000).max(1)`.
   - Terminal pagination: If a page fills its limit (`candidates.len() >= limit`), `next_cursor` is `Some(GcCursor(last_digest))`. If fewer candidates remain (`candidates.len() < limit`), `next_cursor` is `None`. **Exhaustion does not require an empty page**; a final page can return candidates with `next_cursor = None`.
3. **Cursor Filtering and Inter-Page Mutation Observation:**
   - Cursors filter candidate digests lexicographically: `digest_str > cursor`.
   - Whether a concurrently added blob is observed depends on the timing of subsequent enumeration calls relative to the filesystem modification; if added ahead of the cursor before the shard is enumerated for that page, it may be observed, but point-in-time snapshot consistency is not guaranteed.

---

## 9. Next-Slice File Scope and Modification Plan

The cutover implementation slice will modify or create the following files:

| File Path | Nature of Change | Purpose |
|---|---|---|
| `src/storage/fs/listing.rs` | **[NEW]** (promoted from `listing_seam.rs`) | Promotes listing seam to production module; defines `pub(crate)` traits; translates shard `NotFound` to `StorageError::io`. |
| `src/storage/fs/listing_seam.rs` | **[DELETE]** | Removed to ensure a single policy implementation. |
| `src/storage/fs.rs` | **[MODIFY]** | Adds `listing_budgets: FsListingBudgets` and `reader: Arc<FsMetadataReader>` to `FsStorage`; implements `try_new_with_budgets`; delegates `GcStorage::list_cas_blobs_page`. |
| `src/config.rs` | **[MODIFY]** | Adds proposed configuration options, Serde fields under `FileStorageFs`, env var aliasing, and range validation. |
| `src/storage/mod.rs` | **[MODIFY]** | Updates `storage_wiring_try_from_config` to construct `FsListingBudgets` and call `try_new_with_budgets`. |
| `src/storage/fs/tests.rs` | **[MODIFY]** | Updates legacy suppression tests; adds shared-reader test; adds startup-offload regression test; adds real `FsStorage` traverser tests. |
| `docs/configuration.md` | **[MODIFY]** | Documents new configuration options, defaults, and override environment variables. |

---

## 10. Comprehensive Verification & Rollback Plan

### 10.1 Planned Verification Coverage
The cutover implementation slice must include automated tests covering:
1. **Distinct Root & Shard Budgets:**
   Unit tests verifying that exceeding `budgets.root` on `blobs/sha256` fails with `StorageErrorKind::Backend`, while `budgets.shard` governs shard enumeration.
2. **Missing-Shard Translation:**
   Test proving that a missing shard directory (`FsDirError::NotFound`) returns `StorageErrorKind::Io`, preserving legacy whole-page failure.
3. **Initial Root Failure Distinctions:**
   - Real fs test with intermediate regular file (`blobs` as file) verifying `StorageErrorKind::CorruptData`.
   - Real fs test with final component regular file (`blobs/sha256` as file) verifying `StorageErrorKind::CorruptData`.
   - Real fs test with unreadable permissions (`EACCES`) verifying `StorageErrorKind::PermissionDenied`. (Must execute with effective unprivileged permissions or be explicitly ignored with an explanatory reason; skipped behavior will not be counted as passing).
4. **Missing Inspection Downcast & Task Error Mappings:**
   - Unit test exercising raw `libc::ENOTDIR` downcast in `translate_inspect_error` mapping to `StorageErrorKind::CorruptData`.
   - Unit test exercising `FsDirError::RuntimeMissing` and `TaskJoinFailed` mapping to `StorageErrorKind::Backend`.
5. **Configuration Parsing, Precedence & Propagation:**
   Tests verifying env var precedence (hierarchical over flat), default fallback, and invalid value rejection (< 2 bytes root, < 64 bytes shard, 0 entries, excessive values).
6. **Production Delegation, Shared Reader & Traverser Integration:**
   - Full integration test running `CasBlobTraverser` over real `FsStorage` backed by the extracted implementation.
   - Shared reader ownership test asserting that `read_adapter.reader()` and `FsStorage.reader` share the exact same `Arc<FsMetadataReader>` allocation without reopening the root path.
   - Startup offload regression test asserting that constructor execution occurs on a blocking thread.

### 10.2 Planned Build and Test Commands
Verification of the cutover implementation will execute the following exact commands:
```bash
# 1. Format and Clippy validation
cargo clippy --all-targets -- -D warnings

# 2. Unit and integration tests for listing module
cargo test --package registry-rust --lib storage::fs::listing

# 3. Storage filesystem tests including updated legacy tests and shared reader
cargo test --package registry-rust --lib storage::fs::tests

# 4. Configuration parsing and validation tests
cargo test --package registry-rust --lib config::tests

# 5. GC Traverser integration tests
cargo test --package registry-rust --test gc_blob_traverser_tests

# 6. Comprehensive repo check
cargo check --all-targets
```

### 10.3 Rollback Plan
If unexpected production issues occur after cutover:
1. **Code Rollback Scope:**
   Revert changes across all modified and deleted files (`src/storage/fs.rs`, `src/storage/fs/listing.rs`, restoring `src/storage/fs/listing_seam.rs`, `src/config.rs`, `src/storage/mod.rs`, `src/storage/fs/tests.rs`, `docs/configuration.md`).
2. **Configuration Compatibility:**
   If configuration options were introduced, ensure removing or ignoring them does not break startup.
3. **Mutation Irreversibility Warning:**
   Rollback of listing code cannot restore or undo mutations performed by earlier GC passes (e.g. blobs already moved to quarantine). External observation of failed GC passes remains in system logs.

---

## 11. Quality Gates & Open Decisions Summary

### 11.1 Established Quality Gates Status
All established quality gates retain their exact canonical meanings and remain **OPEN**:
- **O-03 (Key and Continuation-Token Validation/Contracts):** OPEN.
- **O-04 (Filesystem Write Durability and Containment):** OPEN.
- **O-05 (Broader Filesystem Read Containment):** OPEN.
- **O-06 (Typed AWS Error Mapping and Genuine Pinned-MinIO Evidence):** OPEN.
- **O-13 (Permanent Hosting, Distribution, and Release Strategy):** OPEN.
- **O-15 (Non-Linux Verification):** OPEN.
- **O-16 (Earlier Slice 11 Audit/Test-Inventory Evidence Completeness):** OPEN.
- **D-06 (Broader Extraction, Cutover, Compatibility, and Distribution Acceptance):** OPEN.

### 11.2 Decisions Requiring Approval Prior to Implementation
Before cutover implementation begins, operator approval is required for:
1. **Changing Shard Translation:** Approving mapping `FsDirError::NotFound` at the shard call site to `StorageError::io` to preserve legacy whole-page I/O failure.
2. **Eliminating Initial Error Suppression:** Approving failing closed on initial non-directory (`CorruptData`), permission denied (`PermissionDenied`), and ordinary I/O (`Io`), removing legacy suppression into an empty page.
3. **Symlink Rejection:** Approving `StorageErrorKind::Io` when encountering symlinks in intermediate or ancestor paths, enforcing Policy C containment.
4. **Fixing Directory-as-Blob Timing:** Approving `StorageErrorKind::CorruptData` when a candidate is replaced with a directory after `readdir`, fixing the legacy vulnerability.
5. **Separate Listing Budgets Implemented & Safeguards Accepted:** Separate root and shard budget plumbing (`FsListingBudgets`) is now implemented and verified in `src/storage/fs/listing_seam.rs`. Provisional defaults (512 / 16 KiB root, 100,000 / 8 MiB shard) are accepted as defensive safeguards. Configurable upper maxima, configuration file/env wiring, and production routing cutover remain unresolved.
