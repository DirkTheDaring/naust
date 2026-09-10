# Architecture Note: Bounded Filesystem Production Read Cutover

**Repository:** `registry-rust`  
**Date:** 2026-09-10  
**Scope:** Cutover of production `FsStorage` read methods (`head_blob` and `open_blob`) to the extracted `FsBlobCasReadAdapter` and `storage-fs` descriptor-relative reader.  
**Status:** Bounded read cutover implemented. Quality Gates **O-03, O-04, O-05, O-06, O-13, O-15, O-16, and D-06 remain OPEN** with their established definitions. Gate O-13 concerns permanent repository hosting and release/distribution strategy, not range reads.

---

## 1. Explicitly Accepted Policies

The bounded filesystem production read cutover operates under three explicitly accepted architectural and operational policies:

### Policy A: Operational Root Stability
The configured storage root directory, all of its ancestor path resolutions, all relevant filesystem mount points, and the process working directory (for relative paths) must remain stable throughout operation.
- This implementation does not enforce root or ancestor mount stability at runtime.
- A pinned root file descriptor does not make descendant lookups immune to mount changes or filesystem restructuring beneath the root.
- Descriptor-based reads can diverge from pathname-based mutations if this operational requirement is violated.
- Restarting the process or reverting the software to legacy pathname reads does **not** reconcile split-root or divergent data.

### Policy B: Fail Initialization on Probe or Reader Failure
Initialization fails immediately if the extracted reader cannot open or if its explicit capability probe (`probe_capability`) fails, including on unsupported platforms or kernels lacking `openat2` support.
- No automatic fallback to legacy pathname reads is permitted.
- Synchronous constructor signatures (`FsStorage::try_new`, `FsStorage::new`) are preserved.
- The existing asynchronous server startup offload via `tokio::task::spawn_blocking` in `init_server_storage_wiring` is preserved to keep synchronous I/O off the async reactor.
- A genuine, accessible, and stable `/proc` filesystem (specifically `/proc/self/fd/<fd>`) remains an operational requirement for descriptor-based payload acquisition.
- Non-Linux compilation and execution remain unverified in this slice; intended failure behavior on other platforms is not execution evidence.

### Policy C: Rejection of Symlinks and Non-Regular Objects
The reader strictly rejects symlinks beneath the opened root—including dangling symlinks—and non-regular objects (e.g., directories, FIFOs, sockets, device nodes) during path resolution and payload acquisition.
- A symlink encountered during the configured root's initial path resolution at startup remains permitted under the accepted root stability requirement (establishing a canonical, pinned directory descriptor). Rejection strictly applies to any symlinks located beneath that established root.
- Only a genuine primary `NotFound` error permits fallback to quarantine storage.
- Primary symlinks beneath root (both dangling and valid targets) return `StorageErrorKind::Io` and strictly suppress quarantine fallback.
- The committed adapter's error taxonomy, error kinds, and diagnostics are preserved byte-for-byte.
- In-flight stream errors during payload streaming do not trigger reacquisition or quarantine fallback.
- These containment and type restrictions apply identically to internal consumers (manifest processing, tag operations, garbage collection) and external HTTP requests.

---

## 2. Construction and Request Routing Behavior

### Shared Reader Ownership
`FsStorage` owns a single shared instance of `FsBlobCasReadAdapter<storage_fs::FsMetadataReader>` wrapped in an `Arc`:

```rust
pub struct FsStorage {
    root: PathBuf,
    read_adapter: Arc<FsBlobCasReadAdapter<storage_fs::FsMetadataReader>>,
    // ... preserved upload locks, byte limits, and hash states
}
```

The ownership structure and direct delegation are visible directly in source. (Note: The unit test `test_production_read_cutover_both_methods_share_reader_and_root_ownership` verifies pointer equality between two cloned `Arc` handles to the same struct field; it does not independently demonstrate root pinning across inode replacement at runtime.)

### Initialization Sequence (`FsStorage::try_new`)
1. Preserves existing root-directory creation (`ensure_dir(&root)?`).
2. Opens a single root `storage_fs::FsMetadataReader::open(&root)`, mapping initialization errors via `map_fs_startup_error`.
3. Explicitly invokes `reader.probe_capability()`, which probes resolution beneath the root using flags `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`.
4. Constructs the `FsBlobCasReadAdapter` wrapping the verified reader only after successful probe.
5. Preserves upload hash shards, mutexes, byte limits, and public constructor signatures.

### Production Read Delegation
Both production CAS read operations on `FsStorage` delegate exclusively to the shared `read_adapter`:

- `FsStorage::head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError>`:
  Delegates directly to `self.read_adapter.head_blob(digest)`.
- `FsStorage::open_blob(&self, digest: &Digest) -> Result<(BlobMeta, Pin<Box<dyn AsyncRead + Send>>), StorageError>`:
  Delegates directly to `self.read_adapter.open_blob(digest)`.

Both methods share the exact same root directory descriptor, capability probe status, and quarantine fallback logic provided by `FsBlobCasReadAdapter`.

---

## 3. Startup versus Read Error Taxonomy Mapping

The implementation maintains strict separation between startup initialization error translation and read-time operation error translation:

### Startup Error Mapping (`map_fs_startup_error`)
Maps `storage_fs::FsMetadataError` encountered during root opening or capability probing to `StorageError` using typed enum pattern matching (never string inspection):

| `FsMetadataError` Variant | Target `StorageErrorKind` | Rationale |
|---|---|---|
| `PlatformUnsupported` | `Configuration` | Operating system platform is not supported. |
| `SyscallUnsupported` | `Configuration` | Host kernel lacks required `openat2` system call. |
| `EmptyRootPath` | `Configuration` | Provided root configuration path is empty. |
| `NulInRootPath` | `Configuration` | Provided root configuration path contains interior NUL bytes. |
| `UnsupportedObjectType` | `Configuration` | Configured root path exists but is not a directory. |
| `ProbeDenied` | `Backend` | `openat2` capability probe denied by system security policy (e.g. seccomp). |
| `ProbeFailed` | `Backend` | Capability probe execution failed due to environment error. |
| `RootOpenFailed` | `Io` | Failed to open root directory path. |
| *Other / Non-exhaustive* | `Backend` | Documented conservative fallback preserving inner diagnostic details. |

*(Note: Unit test fixtures exercise translator mappings directly; they do not independently induce real kernel probe failures through startup.)*

### Read-Time Error Mapping (`FsBlobCasReadAdapter`)
Read-time failures originating from `storage-fs` resolution or payload reopening map as follows, exactly matching `read_adapter.rs`:
- `storage_core::ReadError::NotFound`: maps to `StorageError::NotFound`. Genuine primary `NotFound` triggers quarantine fallback; quarantine `NotFound` returns `StorageError::NotFound`.
- `storage_core::ReadError::PermissionDenied`: maps to `StorageErrorKind::Io`. Suppresses quarantine fallback.
- `storage_fs::FsMetadataError::ResolutionRejected`: maps to `StorageErrorKind::Io`. Suppresses quarantine fallback.
- `storage_fs::FsMetadataError::UnsupportedObjectType`: maps to `StorageErrorKind::Io`. Suppresses quarantine fallback.
- `storage_fs::FsMetadataError::SyscallUnsupported`: maps to `StorageErrorKind::Configuration`. Suppresses quarantine fallback.
- `storage_fs::FsMetadataError::StatFailed`: maps to `StorageErrorKind::Io`. Suppresses quarantine fallback.
- `storage_fs::FsMetadataError::ProcfsReopenFailed`: maps to `StorageErrorKind::Io`. Suppresses quarantine fallback.
- `storage_fs::FsMetadataError::IdentityMismatch`: maps to `StorageErrorKind::Io`. Suppresses quarantine fallback.
- `storage_fs::FsMetadataError::InvalidMetadata`: maps to `StorageErrorKind::Io`. Suppresses quarantine fallback.
- `storage_fs::FsMetadataError::PlatformUnsupported`: maps to `StorageErrorKind::Io`. Suppresses quarantine fallback.
- `storage_fs::FsMetadataError::RuntimeMissing`: maps to `StorageErrorKind::Backend`. Suppresses quarantine fallback.
- `storage_fs::FsMetadataError::TaskJoinFailed`: maps to `StorageErrorKind::Backend`. Suppresses quarantine fallback.
- Any unmapped inner error defaults to `StorageErrorKind::Io`.
- Read stream I/O errors occurring during body streaming propagate directly via Tokio/hyper without re-triggering storage reacquisition.

---

## 4. Construction Caller Contexts & Offload Boundary Architecture

An empirical source trace of all construction calls to `storage_wiring_try_from_config`, `proxy_cache_storage_try_from_config`, and `FsStorage::try_new` establishes the following execution contexts and offload boundaries:

### A. Server Runtime Primary Storage (`src/runtime.rs`)
- **Call site:** `src/runtime.rs:256-265` inside `init_server_storage_wiring`, called by `build_server_runtime_with_factories` via `build_server_runtime`.
- **Classification:** **Production**.
- **Execution Boundary:**
  - **Async Calling Thread:** Execution before offload occurs on the async calling thread; it does not establish a dedicated OS thread.
  - **Blocking Closure:** For `StorageBackend::Filesystem`, synchronous directory creation (`ensure_dir`), root directory opening, and capability probing are executed inside `tokio::task::spawn_blocking` and awaited.
  - For `StorageBackend::S3`, construction executes synchronously on the calling thread without offload.
- **Error Mapping:** `JoinError` maps to `StorageErrorKind::Backend` via `StorageError::backend("filesystem storage initialization task failed: ...")`. Underlying storage errors preserve their kind and diagnostic.

### B. Server Runtime Proxy Cache Storage (`src/runtime.rs`)
- **Call site:** `src/runtime.rs:509-524` (per-upstream route cache in `upstreams` loop) and `src/runtime.rs:553-568` (default proxy cache when `upstreams` is empty), invoking `crate::storage::proxy_cache_storage_try_from_config_async_with_factory`.
- **Classification:** **Production** (active when `config.proxy.enabled` is true).
- **Execution Boundary:**
  - **Async Calling Thread:** Both branches are called from the async calling thread within the `build_server_runtime_with_factories` async initialization workflow.
  - **Blocking Closure:** For `StorageBackend::Filesystem`, `FsStorage::try_new`—including root validation, directory creation, reader open, and `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS` capability probing—is executed inside `tokio::task::spawn_blocking` on Tokio's blocking thread pool and awaited before proceeding.
  - For `StorageBackend::S3`, construction executes synchronously on the calling thread without offload.
- **Error Policy & Propagation:**
  - If constructor execution fails, `build_server_runtime` immediately unwinds acquired mutation authority and aborts startup:
    `return Err(unwind_and_fail(mutation_authority, RuntimeBuildError::ProxyCache(err)).await);`
  - Constructor failures are **never** silently swallowed or tolerated. Server initialization strictly aborts on probe or directory failure, adhering to Policy B.
  - `JoinError` from `spawn_blocking` is translated to `StorageErrorKind::Backend` via `StorageError::backend("filesystem proxy cache storage initialization task failed: ...")`. (Note: The new JoinError unit tests inject blocking-task panics; they do not exercise task cancellation.) Factory error kinds (e.g. `Configuration`) and diagnostics are preserved unchanged.
- **Test Coverage:** The proxy waiting test (`test_proxy_cache_callers_await_construction_before_proceeding`) exercises the default-cache branch, while the separate off-thread execution tests cover both per-upstream and default branches.

### C. CLI Maintenance Runtime (`src/cli/runtime.rs`)
- **Call site:** `src/cli/runtime.rs:35-47` inside `MaintenanceRuntime::acquire`, delegating to `acquire_with_storage_factory`.
- **Classification:** **Production** (dispatched by maintenance CLI commands: `ref-index`, `blob-gc`, `migrate-membership`, `inspect-lock` under `#[tokio::main] async fn main()`).
- **Execution Boundary:**
  - **Async Calling Thread:** `MaintenanceRuntime::acquire` executes before offload on the async calling thread; it does not establish a dedicated OS thread.
  - **Blocking Closure:** For `StorageBackend::Filesystem`, `storage_wiring_try_from_config_async_with_factory` executes the synchronous constructor inside `tokio::task::spawn_blocking` and awaits completion.
  - For `StorageBackend::S3`, construction executes synchronously on the calling thread without offload.
- **Ordering & Authority:** The maintenance waiting test (`test_maintenance_acquire_awaits_construction_before_authority_acquisition`) proves that acquisition remains pending while construction is paused. It does not independently observe whether authority acquisition has occurred; operation ordering (storage construction occurring before distributed authority lease acquisition and readiness preflight) is established by source inspection (`src/cli/runtime.rs:62-88`).
- **Error Mapping:** `JoinError` maps to `StorageErrorKind::Backend` wrapped in `CliError::Storage`. (Note: Unit tests exercise this by injecting blocking-task panics, not cancellation.) Factory errors (e.g. `PermissionDenied`) preserve their kind and message.

### D. CLI Admin Clear Lock (`src/cli/runtime.rs`)
- **Call site:** `src/cli/runtime.rs:437-466` inside `admin_clear_lock`, delegating to `admin_clear_lock_with_storage_factory`.
- **Classification:** **Production** (dispatched by `admin-clear-lock` break-glass CLI command under `#[tokio::main]`).
- **Execution Boundary:**
  - **Async Calling Thread:** Executes before offload on the async calling thread.
  - **Blocking Closure:** For `StorageBackend::Filesystem`, offloaded inside `tokio::task::spawn_blocking` via `storage_wiring_try_from_config_async_with_factory` and awaited.
  - For `StorageBackend::S3`, construction executes synchronously on the calling thread without offload.
- **Ordering & Authority:** Operation ordering (construction occurring before evaluating confirmation tokens `CONFIRM-CLEAR-ABANDONED-WRITER` or `FORCE` and calling `admin_clear_abandoned_deployment_writer_lock`) is established by source inspection (`src/cli/runtime.rs:463-475`).
- **Error Mapping:** `JoinError` maps to `StorageErrorKind::Backend` wrapped in `CliError::Storage`. Factory errors preserve their kind and message.

### E. Cited Supervisor Storage Construction (`src/supervisor.rs`)
- **Call site:** `src/supervisor.rs:1383` inside `create_test_env`.
- **Classification:** **TEST-ONLY**.
- **Execution Boundary:** N/A (synchronous test helper in `#[cfg(test)] mod tests`).
- **Production Status:** Production `supervisor.rs` contains **zero** storage construction calls. Production supervisor receives composed dependencies from `build_server_runtime`.

### F. Internal Asynchronous Factory Helpers (`src/storage/mod.rs`)
- `pub(crate) storage_wiring_try_from_config_async_with_factory<F>`
- `pub(crate) proxy_cache_storage_try_from_config_async_with_factory<F>`
- These internal `pub(crate)` helpers isolate the `spawn_blocking` boundary and `JoinError` translation across server runtime, proxy cache, and CLI callers without exposing new public API surface. Unused non-factory async wrappers were audited and removed. Public synchronous APIs (`storage_wiring_try_from_config`, `proxy_cache_storage_try_from_config`, `FsStorage::try_new`) remain preserved.

---

## 5. Operational Assumptions and Probe Limitations

### Assumptions
1. **Genuine procfs:** Payload descriptor acquisition depends on reopening file descriptors via `/proc/self/fd/<fd>`. A genuine, unmasked `/proc` filesystem must be mounted and accessible.
2. **Descriptor Stability:** The open file description for the storage root remains valid across worker thread lifecycles.

### Probe Scope and Explicit Limitations
`FsMetadataReader::probe_capability` executes a probe at initialization time using:
```c
RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS
```
- **Probe Scope:** Its success establishes only that the particular lookup succeeded on the executing thread at that specific time.
- **The probe DOES NOT:**
  - Establish process-wide permissions or future kernel availability.
  - Verify capability or permissions for future spawned worker threads or sub-processes.
  - Check existence or permissions of descendant child directories or individual payload blobs.
  - Verify procfs availability or permissions for reopening descriptors during runtime payload reads.
  - Guarantee root or descendant mount coherence across the lifetime of the process.

---

## 6. Impact on Internal Storage Consumers

Internal registry operations accessing CAS blobs—including manifest parsing, signature verification, referrers discovery, catalog indexing, and garbage collection—route through `Storage::head_blob` and `Storage::open_blob`.
Under this cutover:
- Internal consumers strictly enforce Policy C.
- Corrupt or invalid entries (such as directory structures or symlinks placed beneath CAS blob trees) are rejected immediately at acquisition time rather than processed as regular blobs.
- Dangling symlinks beneath root fail closed with `Io` errors instead of being masked as missing files.

---

## 7. Rollback Behavior and Limitations

If this cutover is rolled back:
1. Reverting the code restores legacy pathname reads (`fs_metadata_size` and `tokio::fs::File::open`).
2. **Non-reconciliation of divergent data:** If Policy A was violated while the cutover was active (e.g., storage root was moved or ancestor mounts shifted while descriptor reads held the original directory inode), mutations and reads may have diverged. Reverting code or restarting the service does not reconcile or reconstruct split-root data.

---

## 8. Dependency and Packaging Boundaries

Dependencies on `storage-fs` and `storage-core` in `Cargo.toml` are configured via relative sibling development paths:
```toml
storage-core = { path = "../storage-layer-rust/crates/storage-core" }
storage-fs = { path = "../storage-layer-rust/crates/storage-fs" }
```
- This setup is for local workspace development and integration verification only.
- Permanent repository hosting and release/distribution strategy remain unresolved (tracked under Gate O-13).

---

## 9. Quality Gate Status and Explicit Non-Claims

This slice implements and verifies the production read cutover only.
- **We DO NOT claim:**
  - Complete filesystem containment across mutation paths (writes, uploads, and deletions remain pathname-based).
  - Complete backend extraction (mutation and upload lifecycle remain in-tree in `registry-rust`).
  - Cross-platform support (Linux `openat2` is required; non-Linux platforms fail at initialization under Policy B; non-Linux execution is unverified).
  - Production release readiness.

The following quality gates retain their established definitions and remain **OPEN**:
- **O-03:** Exact validated key and continuation-token contract.
- **O-04:** Filesystem write durability, descriptor-relative containment, symlink safety, and crash outcomes.
- **O-05:** Filesystem read containment and symlink behavior.
- **O-06:** Typed AWS mapping and genuine pinned-MinIO evidence.
- **O-13:** Permanent repository hosting and release/distribution strategy.
- **O-15:** Non-Linux filesystem support.
- **O-16:** Earlier Slice 11 audit, test-inventory, and MinIO-report completeness.
- **D-06:** Remains unresolved until extracted implementations, cutover evidence, compatibility assessment, and distribution strategy are accepted.
