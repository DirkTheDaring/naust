# Filesystem CAS Listing Production Cutover

## 1. Executive Summary and Scope

This document records the approved production cutover of Content Addressable Storage (CAS) blob listing in `registry-rust` to the contained, descriptor-relative listing implementation backed by `storage-fs`.

### Accepted Decisions
- **Option A (Fixed Internal Budgets):** Approved and implemented. Fixed safeguards are enforced internally without operator-facing configuration.
  - **Root Directory Budget:** 512 entries / 16,384 raw filename bytes (16 KiB).
  - **Shard Directory Budget:** 100,000 entries / 8,388,608 raw filename bytes (8 MiB).
- **Option B (Operator Configuration & Higher Configurable Maxima):** Deferred. No configuration fields, environment variables, TOML settings, or public configurable constructors are introduced in this slice.
- **Single Listing Implementation:** `src/storage/fs/listing_seam.rs` is promoted to `src/storage/fs/listing.rs`, and the previous seam module file is deleted. Production `FsStorage::list_cas_blobs_page` delegates exclusively to the promoted module.
- **Shared Reader:** Production `FsStorage` shares a single `Arc<storage_fs::FsMetadataReader>` between `read_adapter` and `listing`, avoiding root descriptor reopenings.
- **Preserved Startup Offloading:** `FsStorage::try_new` performs synchronous construction; the existing async startup factory offloads construction through `tokio::task::spawn_blocking`, translating join errors to `StorageErrorKind::Backend`.
- **No Push or Commit:** Changes are verified in the working tree and prepared for independent review packaging.

---

## 2. Architectural Design & Shared Resources

### 2.1 Promotion and Module Boundaries
`src/storage/fs/listing_seam.rs` has been promoted to `src/storage/fs/listing.rs`. The test-only seam module declaration has been replaced with:
```rust
#[path = "fs/listing.rs"]
pub(crate) mod listing;
```
Production `FsStorage` delegates its `GcStorage::list_cas_blobs_page` implementation directly to `listing::list_cas_blobs_page_impl`:
```rust
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

### 2.2 Shared Reader Allocation & Construction
`FsStorage` retains both the read adapter and a shared handle to the underlying metadata reader:
```rust
pub struct FsStorage {
    root: PathBuf,
    max_upload_bytes: u64,
    upload_hashes: Vec<Mutex<std::collections::HashMap<String, SerializableSha256>>>,
    referrer_locks: Vec<Mutex<()>>,
    repo_locks: std::sync::Mutex<std::collections::HashMap<String, std::fs::File>>,
    reader: std::sync::Arc<storage_fs::FsMetadataReader>,
    read_adapter: std::sync::Arc<read_adapter::FsBlobCasReadAdapter<storage_fs::FsMetadataReader>>,
}
```
During synchronous construction in `FsStorage::try_new`:
1. `storage_fs::FsMetadataReader::open(&root)` opens the root descriptor and `reader.probe_capability()` validates `openat2` support.
2. The opened reader is wrapped in `let reader = std::sync::Arc::new(reader);`.
3. The read adapter is instantiated via `read_adapter::FsBlobCasReadAdapter::new(std::sync::Arc::clone(&reader))`.
4. `FsStorage` stores both `reader` and `read_adapter`, ensuring listing and reading share the exact same opened root reader allocation (`Arc::ptr_eq`).

In asynchronous server startup flows, the existing startup factory (`storage_wiring_try_from_config_async_with_factory`) offloads synchronous `FsStorage` construction via `tokio::task::spawn_blocking`, mapping task join errors to `StorageErrorKind::Backend`. Direct `FsStorage::try_new` invocations remain synchronous.

Listing calls reuse the existing root directory file descriptor established at construction as the base handle for descriptor-relative resolution. Child directory descriptors (for shards) and file descriptors (for candidate metadata inspection) are opened beneath this root handle via `openat2` with `RESOLVE_BENEATH`, without reopening the configured root directory itself.

---

## 3. Approved Error Classification & Behavior

All error handling uses typed matching over `FsDirError`, `storage_fs::FsMetadataError`, and `ReadError`. No diagnostic string matching is employed.

### 3.1 Initial CAS Root Directory Enumeration (`blobs/sha256`)
- `NotFound`: Returns an empty terminal page with no items and no continuation cursor (`Ok(GcBlobPage { items: Vec::new(), next_cursor: None })`). This accommodates newly initialized or unpopulated storage roots.
- `PermissionDenied`: Returns `StorageErrorKind::PermissionDenied`. Permissions failures are never swallowed or masked as empty.
- `Io`: Returns `StorageErrorKind::Io`. Ordinary filesystem I/O failures are reported directly.
- `NotADirectory`: Returns `StorageErrorKind::CorruptData`. If an intermediate path component (`blobs`) or the final component (`sha256`) is not a directory (e.g. a regular file in place of the expected directory), it indicates namespace corruption. (Descriptor-relative resolution rejection caused by symlinks maps to `Io`; symlink entries encountered within directories remain governed by existing entry-type validation).
- `ResolutionRejected`: Returns `StorageErrorKind::Io`. Descriptor-relative resolution rejection (e.g. symlink traversal outside root) fails closed.

### 3.2 Shard Directory Enumeration (`blobs/sha256/<p2>`)
- Shard Disappearance (`FsDirError::NotFound`): Intercepted at the shard enumeration call site and mapped to `StorageError::io(...)`. This preserves legacy whole-page failure semantics if a shard directory vanishes concurrently between root enumeration and shard enumeration.
- Shard `PermissionDenied`: Returns `StorageErrorKind::PermissionDenied`.
- Shard `NotADirectory`: Returns `StorageErrorKind::CorruptData`.
- Shard Resource Exhaustion (`FsDirError::LimitExceeded`): Returns `StorageErrorKind::Backend`.

### 3.3 Candidate Blob Inspection
- **Candidate Disappearance (`ReadError::NotFound`):** Mapped to `StorageErrorKind::Io`. A candidate that was enumerated but vanishes before metadata inspection fails the listing page with an I/O error; it is not silently dropped.
- **Candidate Permission Denied (`ReadError::PermissionDenied`):** Mapped to `StorageErrorKind::Io` for candidate inspection (matching registry listing failure semantics; if a typed `std::io::Error` source is present, its message is preserved).
- **Stat Failures (`FsMetadataError::StatFailed`):** Carried within `ReadError::Backend { source }`, downcast as typed `FsMetadataError::StatFailed` and mapped to `StorageErrorKind::Io`.
- **Unsupported Object Type (`FsMetadataError::UnsupportedObjectType`):** Carried within `ReadError::Backend { source }`, downcast as typed `FsMetadataError::UnsupportedObjectType` and mapped to `StorageErrorKind::CorruptData`. Non-regular files (directories, fifos, sockets, block/char devices) discovered in the CAS shard fail closed as corrupt data.
- **Invalid Metadata (`FsMetadataError::InvalidMetadata`):** Carried within `ReadError::Backend { source }`, downcast and mapped to `StorageErrorKind::CorruptData`.
- **Raw `libc::ENOTDIR`:** Carried within `ReadError::Backend { source }` wrapping `std::io::Error` with raw OS error `ENOTDIR`, downcast and mapped to `StorageErrorKind::CorruptData`. Other `std::io::Error` sources map to `StorageErrorKind::Io`.
- **Descriptor-Relative Resolution Violations (`FsMetadataError::ResolutionRejected`):** Carried within `ReadError::Backend { source }`, downcast and mapped to `StorageErrorKind::Io`.
- **Platform / Syscall Limitations (`FsMetadataError::SyscallUnsupported`, `FsMetadataError::PlatformUnsupported`):** Mapped to `StorageErrorKind::Configuration`.

### 3.4 Runtime & Task-Join Failures
- **Tokio Runtime Missing (`FsDirError::RuntimeMissing`, `FsMetadataError::RuntimeMissing`):** Mapped to `StorageErrorKind::Backend`. Diagnostic error message text is preserved from the underlying failure.
- **Task Join Failures (`FsDirError::TaskJoinFailed`, `FsMetadataError::TaskJoinFailed`):** Mapped to `StorageErrorKind::Backend`. Diagnostic message text is preserved from the underlying Tokio task join failure.
- **Preservation Boundary:** Registry `StorageError::internal(StorageErrorKind::Backend, message)` stores the error kind and a diagnostic string. The typed underlying source object is inspected and downcast during translation to extract diagnostic text, but is not retained as a typed source on `StorageError`.

### 3.5 Resource Limit Exhaustion
- Exceeding root directory budgets (> 512 entries or > 16 KiB names) or shard directory budgets (> 100,000 entries or > 8 MiB names) maps to `StorageErrorKind::Backend`.
- Exhaustion produces no partial page; the entire listing page call fails immediately.

---

## 4. Characterization Changes from Legacy Implementation

| Area | Legacy Pathname Implementation | Promoted Contained Implementation | Rationale |
|---|---|---|---|
| **Non-Directory CAS Root** | `read_dir` on file returned `ENOTDIR` mapped to `Io` | Typed `FsDirError::NotADirectory` maps to `CorruptData` | Approved error behavior: non-directory CAS path components represent namespace corruption. |
| **Malformed Shard Filename Diagnostic** | Absolute host path in message: `.../blobs/sha256/e3: <name>` | Relative shard identifier: `CAS shard e3: <name>` | Contained listing operates descriptor-relative and does not retain or expose host filesystem paths. |
| **Initial Metadata / Root Permissions** | Legacy code in some paths ignored certain initial metadata errors | Errors are strictly propagated without suppression | Unprivileged access errors and corrupted filesystem objects must fail closed. |
| **Shard Disappearance** | Fails whole page with `Io` | Explicitly intercepted `FsDirError::NotFound` mapped to `StorageError::io` | Preserves legacy whole-page failure contract; prevents masquerading as empty shard. |

---

## 5. Operational Constraints and Failure Modes

### 5.1 Fixed Safeguards & Absences
- Operators cannot tune or increase root or shard budgets via configuration in this slice.
- `FsStorage::try_new` success verifies root descriptor acquisition and capability probing; it does **not** pre-scan the filesystem or guarantee that existing shards fit within fixed limits.
- Repositories or installations with shards containing more than 100,000 files will fail listing operations with `StorageErrorKind::Backend`.

### 5.2 Benchmark & Environment Limitations
- Prior capacity benchmarks were measured on Linux `tmpfs`. Real-world block storage (ext4, XFS, NFS) may exhibit differing readdir performance, directory block fragmentation, and I/O latency.
- The raw filename byte budgets (16 KiB root, 8 MiB shard) bound directory entry name lengths; they do not measure Rust heap allocations, string formatting overhead, or aggregate memory across concurrent listing tasks.

### 5.3 Concurrency and Mutation Semantics
- **No Point-in-Time Snapshot:** Filesystem CAS listing is not transactional. Directory enumeration and candidate metadata inspection occur sequentially across multiple descriptors. Modifications between enumeration and inspection are observed at inspection time.
- **Non-Rollback of Preceding GC Actions:** If a multi-page garbage collection run succeeds on pages 1 through N but encounters a failure (e.g. limit exhaustion or I/O failure) on page N+1, earlier mutations (quarantined blobs, deleted objects) are **not** rolled back.
- **Containment Scope:** Descriptor-relative containment (`openat2` with `RESOLVE_BENEATH`) prevents symlink traversal outside the opened storage root directory tree. It does not prevent external modification of ancestors above the root path.

### 5.4 Test Accounting & Permission Test Limitations
- **Exact Test Accounting:**
  - Full library test suite (`cargo test --locked --lib`): **585 passed, 0 failed, 3 ignored**.
  - Promoted listing module tests (`storage::fs::listing`): **57 passed, 0 failed, 1 ignored**.
- **Ignored Permission Tests:**
  Three tests across `registry-rust` are explicitly marked with `#[ignore]` requiring an unprivileged execution environment where `chmod 0o000` denies access to directory objects (because running under root UID 0 bypasses standard DAC file permissions):
  1. `storage::fs::tests::test_fs_metadata_size_environment_permission_denied`
  2. `storage::fs::tests::test_head_blob_environment_permission_denied_suppresses_quarantine`
  3. `storage::fs::listing::tests::linux_fs_tests::test_real_fs_initial_permission_denied_not_suppressed`
- **Execution under Unprivileged Environments:**
  When `test_real_fs_initial_permission_denied_not_suppressed` is explicitly run (`-- --ignored`), it verifies effective permissions (failing immediately if UID == 0 or if directory traversal succeeds under mode 0o000), protects cleanup with a scoped `PermGuard` that avoids double panics during unwinding, verifies normal-path permission restoration, and checks fixture cleanup.

---

## 6. Canonical Quality Gates

All canonical quality gates remain explicitly **OPEN**. This slice addresses only the internal production filesystem CAS listing cutover under fixed budgets.

| Gate | Title | Status | Scope / Reason |
|---|---|---|---|
| **O-03** | Key and continuation-token contracts | **OPEN** | Broader key validation and cross-backend token standardization remain pending. |
| **O-04** | Filesystem write durability and containment | **OPEN** | Write path containment and durability fsync policies remain under separate tracking. |
| **O-05** | Broader filesystem read containment | **OPEN** | Remaining legacy pathname reads outside CAS blob head/open/listing remain to be transitioned. |
| **O-06** | Typed AWS mapping and pinned-MinIO evidence | **OPEN** | S3-specific error mappings and live MinIO integration remain outside filesystem cutover. |
| **O-13** | Hosting, distribution, and release strategy | **OPEN** | Packaging and artifact distribution governance remain deferred. |
| **O-15** | Non-Linux verification | **OPEN** | Contained descriptor-relative operations rely on Linux `openat2`; fallback/platform parity remains open. |
| **O-16** | Earlier Slice 11 audit/test-inventory evidence | **OPEN** | Historical audit evidence retained pending comprehensive extraction sign-off. |
| **D-06** | Broader extraction, cutover, compatibility, and distribution acceptance | **OPEN** | Overall storage-layer extraction and multi-backend cutover acceptance remain open. |
