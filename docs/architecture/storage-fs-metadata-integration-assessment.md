# Storage-FS Metadata Reader: Registry Integration Assessment

**Document**: `docs/architecture/storage-fs-metadata-integration-assessment.md`
**Repository**: `registry-rust`
**Date**: 2026-09-08
**Scope**: Technical assessment and integration design for evaluating the standalone `storage-fs` descriptor-relative metadata reader in `registry-rust` via a test-only integration seam, recording completed milestones, and defining remaining decisions before production cutover.
**Completed Milestone Commits**:
- `storage-layer-rust`: `72062016a127ee435979615d959396d7d7212bc2` — Standalone descriptor-relative metadata reader.
- `registry-rust`: `f7e174bbf74ef48ba13ed57229eebcc1fd70d1c9` — Test-only metadata integration seam.
- `storage-layer-rust`: `6757b0f2cb82ed92e9415578616b323db8c81907` — Internal Tokio blocking execution boundary.
- `registry-rust`: `56d6911c1fee16faa24936aaa88107d0979b7b4b` — Downstream lockfile update following successful revalidation.

**Current Repository HEADs**:
- `registry-rust`: `56d6911c1fee16faa24936aaa88107d0979b7b4b`
- `storage-layer-rust`: `6757b0f2cb82ed92e9415578616b323db8c81907`

**Status**: Technical assessment and architecture decision record. Production code, routing, and outward API behavior remain unchanged. Quality gates **O-05**, **O-03**, **O-06**, **O-13**, **O-16**, and **D-06** remain **OPEN**. Non-Linux execution remains source-designed but execution-unverified.

---

## 1. Actual Integration Points and Source Analysis

### 1.1 `FsStorage` Initialization and Root Lifetime
- **Source**: `src/storage/fs.rs:190-225`
- **Definition**:
  ```rust
  pub struct FsStorage {
      root: PathBuf,
      max_upload_bytes: u64,
      upload_hashes: Vec<Mutex<std::collections::HashMap<String, SerializableSha256>>>,
      referrer_locks: Vec<Mutex<()>>,
      repo_locks: std::sync::Mutex<std::collections::HashMap<String, std::fs::File>>,
  }
  ```
- **Constructor**:
  ```rust
  pub fn try_new(root: PathBuf, max_upload_bytes: u64) -> Result<Self, StorageError>
  ```
- **Analysis**:
  - `FsStorage` holds the configured storage root directory as an owned `PathBuf`.
  - Construction verifies or creates the directory via `ensure_dir(&root)?;` (`src/storage/fs.rs:199`).
  - Production composition occurs in `src/storage/mod.rs:829` (`storage_wiring_try_from_config`), which constructs `FsStorage::try_new(config.storage.fs.data_dir.clone(), config.storage.max_upload_bytes)?` and wraps it in `Arc<FsStorage>`.
  - Lifetime: The `FsStorage` instance lives for the entire lifetime of the registry process within `StorageWiring` and `AppState`.

### 1.2 `BlobCasReader::head_blob` and Production Callers
- **Port Definition**: `src/storage/ports/mod.rs:76-80`
  ```rust
  #[async_trait]
  pub trait BlobCasReader: Send + Sync {
      async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError>;
      async fn open_blob(&self, digest: &Digest) -> Result<(BlobMeta, Pin<Box<dyn AsyncRead + Send>>), StorageError>;
  }
  ```
  where `BlobMeta { pub size: u64 }` (`src/storage/ports/mod.rs:43`).
- **`FsStorage` Implementation**: `src/storage/fs.rs:735-752`
- **Production Callers**:
  1. `BlobReadService::head_blob` (`src/application/blob_read.rs:56-90`): Validates canonical repository name and tenant membership, invokes `self.blob_reader.head_blob(digest).await`. Maps `StorageError::NotFound` to `BlobReadError::NotFound` (translating outward to HTTP `404 Not Found` with `BLOB_UNKNOWN` body), and all other `StorageError` variants to `BlobReadError::Storage(e)` (HTTP `500 Internal Server Error`).
  2. `UploadCoordinator` (`src/upload_coordinator.rs:532`, `686`, `791`): Checks blob existence prior to finalizing chunked uploads and during write deduplication. Unexpected `NotFound` on finalized receipts is classified as `StorageErrorKind::CorruptData`, while other failures map to `StorageErrorKind::Backend` or `StorageErrorKind::PermissionDenied`.
  3. `Proxy` / Cache Storage (`src/proxy.rs:589`): Evaluates whether a requested blob is already present in local cache before initiating upstream retrieval.
  4. `StorageWiringFacade::blob_reader` (`src/storage/facade.rs:20-22`): Provides architectural isolation for runtime composition.
  5. `GcService` (`src/gc_service.rs`): Queries blob metadata during garbage collection sweep and mark phases.
  6. `MembershipMigration` (`src/membership_migration.rs:231`): Scans blob presence during database schema migrations.

### 1.3 `fs_metadata_size` and Quarantine Fallback Orchestration
- **Source**: `src/storage/fs.rs:676-678` and `735-752`
- **Current Helper**:
  ```rust
  async fn fs_metadata_size(path: &Path) -> Result<u64, std::io::Error> {
      tokio::fs::metadata(path).await.map(|m| m.len())
  }
  ```
- **Current `head_blob` Orchestration**:
  ```rust
  async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError> {
      let path = self.blob_path(digest);
      match fs_metadata_size(&path).await {
          Ok(size) => Ok(BlobMeta { size }),
          Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
              let qpath = self.quarantine_blob_path(digest);
              match fs_metadata_size(&qpath).await {
                  Ok(size) => Ok(BlobMeta { size }),
                  Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                      Err(StorageError::NotFound)
                  }
                  Err(err) => Err(StorageError::io(err.to_string())),
              }
          }
          Err(err) => Err(StorageError::io(err.to_string())),
      }
  }
  ```
- **Analysis**:
  - Quarantine fallback is an **orchestration policy owned strictly by `registry-rust`**.
  - Quarantine lookup is attempted **only** when the primary lookup returns `NotFound` (`ENOENT`).
  - If the primary lookup fails with `PermissionDenied` (`EACCES`/`EPERM`), I/O error (`EIO`), or any non-`NotFound` condition, the query immediately terminates and never attempts quarantine lookup.

### 1.4 Digest-to-Filesystem-Path Construction
- **Source**: `src/storage/fs.rs:357-375`
- **Primary Path**:
  ```rust
  fn blob_path(&self, digest: &Digest) -> PathBuf {
      self.root
          .join("blobs")
          .join(digest.algorithm())
          .join(digest.prefix2())
          .join(digest.hex())
  }
  ```
- **Quarantine Path**:
  ```rust
  fn quarantine_blob_path(&self, digest: &Digest) -> PathBuf {
      self.root
          .join("quarantine")
          .join("blobs")
          .join(digest.algorithm())
          .join(digest.prefix2())
          .join(digest.hex())
  }
  ```
- **Translation to Generic `ObjectKey`**:
  - Primary relative key: `blobs/<algo>/<prefix2>/<hex>`
  - Quarantine relative key: `quarantine/blobs/<algo>/<prefix2>/<hex>`
  - Both strings consist of exactly 4 or 5 clean relative segments, containing strictly lowercase ASCII alphanumeric characters and hyphens. They contain no leading/trailing slashes, no dot segments (`.` or `..`), no backslashes, and no NUL bytes.
  - They parse deterministically into `storage_core::ObjectKey` without normalization or syntactic rejection.

### 1.5 `StorageWiring` and `StorageWiringFacade`
- **Source**: `src/storage/ports/mod.rs:1031-1055` and `src/storage/facade.rs:9-24`
- **Structure**:
  - `StorageWiring` holds `blob_reader: Arc<dyn BlobCasReader>`.
  - `StorageWiringFacade` wraps `StorageWiring` and delegates `blob_reader()` directly.
  - A test-only integration seam requires no changes to `StorageWiring`, `StorageWiringFacade`, or the trait signature of `BlobCasReader`.

### 1.6 `StorageError` Taxonomy and Outward Mappings
- **Source**: `src/storage/mod.rs:29-180` and `docs/architecture/adr-009-structured-storage-error-taxonomy.md`
- **Taxonomy**:
  - `StorageError::NotFound`: Represents missing objects; maps to HTTP `404 Not Found` (`BLOB_UNKNOWN`).
  - `StorageError::Internal { kind: StorageErrorKind, message: String }`:
    - `StorageErrorKind::Io`: Local filesystem and OS errors (`StorageError::io(err)`).
    - `StorageErrorKind::PermissionDenied`: Access and search permission errors (`StorageError::permission_denied(err)`).
    - `StorageErrorKind::Backend`: Remote or backend driver failures (`StorageError::backend(err)`).
    - `StorageErrorKind::Configuration`: Unusable or invalid configuration (`StorageError::configuration(err)`).
  - Note on Structure: `StorageError::Internal` stores only `kind: StorageErrorKind` and `message: String`. It does **not** store a boxed source chain (`source: Option<Box<dyn StdError>>`). Therefore, error translation in `registry-rust` preserves diagnostic text (`err.to_string()`), but does not preserve typed causal chains without a separate architectural contract change.
  - Outward HTTP Mapping (`src/http_api/handlers.rs:300-315`):
    - `BlobReadError::NotFound` -> `blob_unknown()` (`404 Not Found`).
    - `BlobReadError::Storage(e)` -> `internal_error()` (`500 Internal Server Error`).

### 1.7 Committed Characterization Baseline (O-05)
- **Source**: `src/storage/fs/tests.rs:2372-2635` and `docs/architecture/o-05-filesystem-metadata-containment.md`
- **Seven Measured Behaviors**:
  Each test explicitly exercises **both** `fs_metadata_size` and `FsStorage::head_blob`:
  1. `test_fs_metadata_containment_symlink_inside_root`: Final blob entry is a symlink to an ordinary file inside root; both `fs_metadata_size` and `head_blob` follow symlink and return target file size.
  2. `test_fs_metadata_containment_symlink_outside_root`: Final blob entry is a symlink escaping root; both follow symlink outside root and return target size (containment gap).
  3. `test_fs_metadata_containment_intermediate_dir_symlink_outside_root`: Intermediate directory is a symlink escaping root; both traverse symlink and return target size (containment gap).
  4. `test_fs_metadata_containment_dangling_symlink_falls_back_to_quarantine`: Dangling symlink on primary path causes `fs_metadata_size` to fail with `NotFound`, which causes `head_blob` to fall back to quarantine.
  5. `test_fs_metadata_containment_quarantine_symlink_outside_root`: Quarantine path is a symlink escaping root; both follow symlink and return target size (containment gap).
  6. `test_fs_metadata_containment_storage_root_is_symlink`: Configured storage root itself is a symlink; both `FsStorage::try_new` and subsequent lookups succeed.
  7. `test_fs_metadata_containment_directory_blob_returns_metadata_size`: Ordinary blob path resolves to a directory; `fs_metadata_size` returns directory metadata size (e.g. 4096 B) and `head_blob` returns `Ok(BlobMeta { size })` because `FileType::is_file()` is not enforced.

### 1.8 Generic and Standalone Contracts in `storage-layer-rust`
- **`storage-core`** (`crates/storage-core/src/error.rs`):
  - `ObjectKey`: Validated relative key, rejects absolute paths, `..`, `.`, repeated slashes, Windows prefixes, and control characters. Note that `ObjectKey` explicitly rejects `.` with `ObjectKeyError::DotSegment`.
  - `ObjectMetadata`: Container preserving exact byte size (`size(&self) -> u64`).
  - `ObjectMetadataReader`: Object-safe trait `async fn head(&self, key: &ObjectKey) -> Result<ObjectMetadata, ReadError>`.
  - `ReadError` (both enum and struct variants are `#[non_exhaustive]`):
    ```rust
    #[derive(Debug, Error)]
    #[non_exhaustive]
    pub enum ReadError {
        #[error("object not found: {key}")]
        #[non_exhaustive]
        NotFound { key: ObjectKey },

        #[error("permission denied: {key}")]
        #[non_exhaustive]
        PermissionDenied {
            key: ObjectKey,
            #[source]
            source: Option<Box<dyn StdError + Send + Sync>>,
        },

        #[error("storage backend error: {message}")]
        #[non_exhaustive]
        Backend {
            message: String,
            #[source]
            source: Option<Box<dyn StdError + Send + Sync>>,
        },
    }
    ```
- **`storage-fs`** (`crates/storage-fs/src/error.rs`, `src/reader.rs`):
  - `FsMetadataReader`: Holds pinned `Arc<OwnedFd>` to root directory (`O_PATH | O_DIRECTORY | O_CLOEXEC`).
  - `FsMetadataReader::open`: Synchronous root directory acquisition on the caller thread using `libc::open`. Does not issue `openat2` and does not test `openat2` kernel availability.
  - `FsMetadataReader::head`: Offloads path resolution and metadata inquiry to Tokio's blocking pool via `tokio::task::spawn_blocking`. Requires an active, entered Tokio runtime context.
  - Path resolution inside blocking task uses Linux `openat2` beneath the pinned descriptor with flags `O_PATH | O_CLOEXEC` and `resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`.
  - Inspects opened descriptor with `fstat`, enforcing `st_mode & S_IFMT == S_IFREG`.
  - Committed error variants in `FsMetadataError`:
    - `ResolutionRejected { raw_os_error, source: io_err }`: Containment violations (`ELOOP`, `EXDEV`) wrapped in `ReadError::Backend`.
    - `UnsupportedObjectType { mode }`: Non-regular objects (directories, FIFOs, devices) wrapped in `ReadError::Backend`.
    - `SyscallUnsupported(io_err)`: `openat2` returning `ENOSYS` wrapped in `ReadError::Backend`.
    - `RuntimeMissing(tokio::runtime::TryCurrentError)`: Polled outside an entered Tokio runtime context wrapped in `ReadError::Backend`.
    - `TaskJoinFailed(tokio::task::JoinError)`: Preserves Tokio's causal `JoinError` when a blocking task panics or fails to join.
    - Note: `FsMetadataError` currently has **no `PermissionDenied` variant**; permission denials encountered during lookup are mapped directly at the `ReadError` level as `ReadError::PermissionDenied`.
  - Cancellation semantics: Dropping the future returned by `head` cancels the await point, but does not abort or stop blocking work that has already started on Tokio's blocking thread pool.
  - Non-Linux behavior: Source-designed to fail closed with `FsMetadataError::PlatformUnsupported`, but execution remains unverified.

---

## 2. Compatibility and Behavior Matrix

The following table compares current legacy behavior with proposed `storage-fs` integration across all operational and boundary scenarios:

| Scenario | Current In-Tree Behavior | Standalone `storage-fs` Behavior | Proposed Registry Integration | Preservation vs Intentional Change | Required Decision / Evidence |
|---|---|---|---|---|---|
| **1. Ordinary Regular File** | `fs_metadata_size` calls `tokio::fs::metadata`; returns exact byte size. | `openat2` + `fstat` on blocking pool; verifies `S_IFREG`; returns exact `u64` byte size. | Returns `Ok(BlobMeta { size })`. | **Preserved**: exact size returned for regular files. | Deterministic seam tests. |
| **2. Missing Primary Blob (No Quarantine)** | Primary returns `NotFound` (`ENOENT`); quarantine returns `NotFound`; `head_blob` returns `StorageError::NotFound`. | Primary returns `ReadError::NotFound { key, .. }`; quarantine returns `NotFound`. | Registry orchestrates primary then quarantine; returns `StorageError::NotFound`. | **Preserved**: maps outward to HTTP 404 `BLOB_UNKNOWN`. | Seam test asserting two-stage lookup. |
| **3. Missing Primary Blob (Valid Quarantine)** | Primary returns `NotFound`; quarantine returns `Ok(size)`; `head_blob` returns `Ok(BlobMeta { size })`. | Primary returns `ReadError::NotFound { key, .. }`; quarantine returns `Ok(size)`. | Registry inspects primary `NotFound`; invokes quarantine; returns `Ok(BlobMeta { size })`. | **Preserved**: quarantine fallback succeeds. | Seam test asserting fallback on missing primary. |
| **4. Permission Denial (`EACCES`/`EPERM`)** | Returns `Err(StorageError::io(err.to_string()))`; quarantine fallback **suppressed**. | Returns `ReadError::PermissionDenied { source, .. }` with boxed `std::io::Error`. | Translates to `StorageError::io(io_err.to_string())`; quarantine fallback **suppressed**. | **Preserved**: preserves `StorageErrorKind::Io` and original OS display string; fallback suppressed. | Verify DAC unprivileged execution in test. |
| **5. Ordinary I/O Error (`EIO`, Disk Fault)** | Returns `Err(StorageError::io(err.to_string()))`; quarantine fallback **suppressed**. | Returns `ReadError::Backend { source, .. }` with boxed `std::io::Error`. | Translates to `StorageError::io(io_err.to_string())`; quarantine fallback **suppressed**. | **Preserved**: preserves `StorageErrorKind::Io` and outward HTTP 500. | Synthetic I/O error mapping test. |
| **6a. Final Symlink Inside Root** | `stat()` follows symlink; returns target file size (`Ok`). | `openat2` fails with `ELOOP`; returns `ReadError::Backend` wrapping `FsMetadataError::ResolutionRejected`. | Translates to `StorageError::io(...)`; quarantine fallback **suppressed**. | **Intentionally Changed**: closes containment gap; symlinks below root forbidden. | Requires acceptance that symlinked blobs are rejected (D7). |
| **6b. Final Symlink Escaping Root** | `stat()` follows symlink escaping root; returns outside target size (`Ok`). | `openat2` fails with `EXDEV` or `ELOOP`; returns `ReadError::Backend` wrapping `ResolutionRejected`. | Translates to `StorageError::io(...)`; quarantine fallback **suppressed**. | **Intentionally Changed**: eliminates directory traversal escape. | Primary security objective of O-05. |
| **6c. Dangling Symlink on Primary Blob** | `stat()` fails with `ENOENT`; **erroneously falls back to quarantine**. | `openat2` fails with `ELOOP`; returns `ReadError::Backend` wrapping `ResolutionRejected`. | Translates to `StorageError::io(...)`; quarantine fallback **suppressed**. | **Intentionally Changed**: prevents masking broken symlinks as absent files. | Seam test proving fallback suppression. |
| **7. Intermediate Directory Symlink** | `stat()` traverses intermediate directory symlink escaping root; returns target size. | `openat2` fails with `ELOOP` or `EXDEV`; returns `ReadError::Backend` wrapping `ResolutionRejected`. | Translates to `StorageError::io(...)`; quarantine fallback **suppressed**. | **Intentionally Changed**: intermediate directory escaping forbidden. | Seam test verifying rejection. |
| **8. Directory or Non-Regular Object (FIFO, Socket)** | `stat()` succeeds on directory; returns directory metadata size (e.g. 4096 B). | `openat2` succeeds; post-open `fstat` detects `!S_IFREG`; returns `ReadError::Backend` wrapping `UnsupportedObjectType`. | Translates to `StorageError::io(...)`; quarantine fallback **suppressed**. | **Intentionally Changed**: closes object contract gap; directories rejected. | Seam test verifying rejection (D2). |
| **9. Configured Root Symlink & Root Rename** | Resolves pathname on every query; renaming root dynamically redirects queries to new target. | Root descriptor pinned at startup (`libc::open`); subsequent queries operate via `dirfd`. | Initial startup resolves root once; queries operate on pinned inode. Binds acquired root descriptor; does not prove immunity to all filesystem rename/mount/hard-link scenarios (see Section 4.2). | **Intentionally Changed**: binds metadata lookup to startup inode; pathname replacement does not redirect. | See Section 4.2 for root-lifetime decision (D6). |
| **10. `openat2` Unusable in Execution Environment** | N/A (`tokio::fs::metadata` uses legacy `statx`/`stat`). | `openat2` syscall returns `ENOSYS`; returns `ReadError::Backend` wrapping `SyscallUnsupported`. | Fails closed with `StorageError::configuration(...)`; **no legacy fallback**. | **Intentionally Changed**: requires usable `openat2`; no fallback to uncontained pathname lookup. | Operational requirement of usable `openat2` (D3); see Section 4.3 for startup probe. |
| **11. Missing Tokio Runtime Context** | N/A (invokes async `tokio::fs::metadata` which expects runtime). | `head` returns `ReadError::Backend` wrapping `FsMetadataError::RuntimeMissing`. | Fails closed; maps to `StorageError::internal(Backend, ...)`. | **New Execution Error**: requires runtime context. | See Section 4.1 for error translation decision. |
| **12. Blocking Task Join Failure / Panic** | N/A (executes inline future). | `head` returns `ReadError::Backend` wrapping `FsMetadataError::TaskJoinFailed`. | Fails closed; maps to `StorageError::internal(Backend, ...)`. | **New Execution Error**: captures pool failure. | See Section 4.1 for error translation decision. |
| **13. Non-Linux Platforms (macOS, Windows, BSD)** | Uses platform `tokio::fs::metadata`. | Source-designed to return `PlatformUnsupported`; execution unverified. | Fails closed with `StorageError::configuration("platform unsupported")`. | **Intentionally Changed**: non-Linux execution unsupported in this slice. | Source-designed but execution-unverified. |
| **14. Mount Crossings Beneath Root** | Crosses mount points transparently via pathname resolution. | `RESOLVE_BENEATH` permits mount crossings within subtree (`RESOLVE_NO_XDEV` is unset). | Preserves mount crossings beneath root unless explicit restriction is configured. | **Preserved**: partitioned disk mounts under root remain functional. | Decision on cross-device mounts (D5). |
| **15. Hard Links Beneath Root** | Reads hard link metadata identical to target file. | Kernel resolves hard link within root; `fstat` succeeds; returns exact size. | Handled as ordinary regular file. | **Preserved**: hard links within root permitted; outside hard links remain OS limitation. | Documented hard-link containment limitation. |

> [!IMPORTANT]
> **Distinguishing Missing Objects from Containment Rejection**
> `ReadError::NotFound` arises strictly from kernel `ENOENT` (file does not exist). Containment rejections (`ELOOP` for symlinks, `EXDEV` for root boundary escapes, and `UnsupportedObjectType` for directories/FIFOs) return `ReadError::Backend`. The registry must **never** treat containment violations as missing files, and must **never fall back to quarantine or legacy pathname lookup** when a containment violation occurs.

> [!WARNING]
> **Behavioral Change Warning**
> Preserving the outward error shape (`StorageError::Internal { kind: StorageErrorKind::Io, .. }` mapping to HTTP 500) does **not** mean behavior is preserved. Workloads that previously relied on placing symlinks or directories under `data/blobs` will experience hard rejections under the integrated reader. This is the intended security outcome of O-05.

---

## 3. Completed Milestones and Verification Evidence

### 3.1 Completed Milestones Summary
1. **Standalone Descriptor-Relative Reader (`storage-layer-rust@72062016a127ee435979615d959396d7d7212bc2`)**:
   - Implemented `storage-fs` crate containing `FsMetadataReader`.
   - Binds root directory descriptor on construction with `O_DIRECTORY | O_PATH | O_CLOEXEC`.
   - Executes kernel-enforced descriptor-relative lookup via Linux `openat2` beneath the pinned descriptor with flags `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`.
   - Validates regular file object contract via `fstat` (`st_mode & S_IFMT == S_IFREG`).
2. **Test-Only Metadata Seam in Registry (`registry-rust@f7e174bbf74ef48ba13ed57229eebcc1fd70d1c9`)**:
   - Integrated `storage-core` and `storage-fs` path dependencies under `[dev-dependencies]`.
   - Implemented `head_blob_seam` and `translate_read_error` in `src/storage/fs/metadata_seam.rs` under `#[cfg(test)]`.
   - Added 12 deterministic fake-reader orchestration tests and 12 real filesystem containment tests.
   - Preserved all production code, struct fields, constructors, and callers unchanged.
3. **Internal Tokio Blocking Execution Boundary (`storage-layer-rust@6757b0f2cb82ed92e9415578616b323db8c81907`)**:
   - Established internal blocking offload in `FsMetadataReader::head` via `tokio::task::spawn_blocking`.
   - Ensured descriptor validity across caller cancellation by capturing an owned `Arc<OwnedFd>` reference and owned `ObjectKey` in each task.
   - Introduced typed errors `FsMetadataError::RuntimeMissing` and `FsMetadataError::TaskJoinFailed`.
   - Isolated library dependency to `tokio = { version = "1", default-features = false, features = ["rt"] }`, with `features = ["macros"]` retained strictly in dev-dependencies.
   - Established dedicated runtime shutdown completion boundaries for controlled cancellation testing.
4. **Downstream Revalidation & Lockfile Update (`registry-rust@56d6911c1fee16faa24936aaa88107d0979b7b4b`)**:
   - Revalidated registry test-only seam against the committed blocking boundary.
   - Updated `registry-rust/Cargo.lock` with the single necessary addition of `"tokio"` to `storage-fs` dependencies without modifying versions of any other crates.

### 3.2 Accepted Verification Evidence
- **Storage Workspace Suite (`storage-layer-rust`)**:
  - `storage-core`: All 22 tests passed.
  - `storage-fs`: 16 unit tests passed, 1 ignored by default.
  - The ignored permission test (`reader::linux_tests::test_fs_metadata_real_unprivileged_permission_denied`) passed when explicitly executed in an unprivileged user environment.
  - Formatting, check, clippy, doctests, and git whitespace checks all passed with exit status 0.
- **Registry Filesystem Suite (`registry-rust`)**:
  - Total discovered: 96 tests (94 passed, 2 ignored, 370 filtered out).
  - 12 recording-fake seam tests: passed (exercising two-stage lookup, fallback suppression on non-NotFound errors, and exact size preservation).
  - 12 real filesystem seam tests: passed (all executing inside entered Tokio runtime contexts, confirming descriptor-relative resolution and containment).
  - 70 existing filesystem characterization and regression tests: passed. (The seven specific containment characterization tests under O-05 are a subset of these 70 existing tests).
  - 2 pre-existing permission tests remained ignored: `test_fs_metadata_size_environment_permission_denied` and `test_head_blob_environment_permission_denied_suppresses_quarantine` (accurately reported as ignored; not claimed as executed).
- **Reported History Note**:
  - Reconstructed records of the initial locked check failure prior to lockfile update represent documented historical context of dependency resolution, not newly executed test failures.

### 3.3 Historical Characterization Baseline Retained
All seven historical containment characterization tests in `src/storage/fs/tests.rs` remain completely unchanged:
1. `test_fs_metadata_containment_symlink_inside_root`
2. `test_fs_metadata_containment_symlink_outside_root`
3. `test_fs_metadata_containment_intermediate_dir_symlink_outside_root`
4. `test_fs_metadata_containment_dangling_symlink_falls_back_to_quarantine`
5. `test_fs_metadata_containment_quarantine_symlink_outside_root`
6. `test_fs_metadata_containment_storage_root_is_symlink`
7. `test_fs_metadata_containment_directory_blob_returns_metadata_size`

They preserve the empirical baseline of legacy uncontained behavior, while the seam tests demonstrate containment under the descriptor-relative reader without modifying production code.

---

## 4. Production Prerequisites: Required Before Production Cutover

The following technical prerequisites must be resolved and accepted before promoting descriptor-relative metadata lookups to production `FsStorage::head_blob`:

### 4.1 Committed Runtime Contract and Boundary Semantics
- **Completed Blocking Offload**:
  `FsMetadataReader::head` now internally offloads filesystem syscalls (`openat2`, `fstat`) to Tokio's blocking thread pool via `tokio::task::spawn_blocking`. Obsolete proposals requiring `head_sync` as a prerequisite have been fulfilled by this internal boundary.
- **State Ownership and Lifetimes**:
  Each spawned task captures an owned `Arc<OwnedFd>` reference to the pinned root directory and an owned, cloned `ObjectKey`. If the caller's awaiting future or the `FsMetadataReader` instance is dropped, the descriptor remains open and valid until the in-flight blocking task completes, preventing `EBADF`.
- **Typed Error Classification**:
  - Missing Runtime: Polling `head` outside an active Tokio runtime produces `ReadError::Backend` wrapping `FsMetadataError::RuntimeMissing(tokio::runtime::TryCurrentError)`.
  - Task Join Failure: Blocking task panics or runtime cancellation produce `ReadError::Backend` wrapping `FsMetadataError::TaskJoinFailed(tokio::task::JoinError)`.
  - Permission Denied: Permission failures during lookup are mapped at the `ReadError` level as `ReadError::PermissionDenied`. `FsMetadataError` itself has no `PermissionDenied` variant.
- **Synchronous Constructor**:
  `FsMetadataReader::open` remains synchronous and uses `libc::open` with flags `O_DIRECTORY | O_PATH | O_CLOEXEC`. It does **not** issue `openat2` and does not establish `openat2` kernel availability at startup.
- **Cancellation Semantics**:
  Dropping the caller's future cancels the await point, but does **not** reliably stop or abort blocking work that has already started in Tokio's blocking thread pool.
- **Registry Translation Status in Test Seam**:
  Downstream compatibility testing confirmed that existing seam tests pass against the new blocking boundary. However, in `src/storage/fs/metadata_seam.rs`, `translate_read_error` currently falls through to a generic string formatting for `RuntimeMissing` and `TaskJoinFailed`; dedicated tests verifying explicit translation of these variants in the registry remain pending.

### 4.2 Unresolved Root-Lifetime Decision: Read-Side vs Write-Side Root Coherence
- **The Divergence Problem**:
  `FsStorage` currently performs all backend operations by resolving `self.root: PathBuf` dynamically by pathname:
  - Payload streaming: `BlobCasReader::open_blob` constructs `self.blob_path(digest)` and calls `tokio::fs::File::open(&path)`.
  - Mutations and writes: `put_blob`, `put_blob_direct`, chunked uploads (`self.root.join("uploads")`), CAS commits, and session cleanups.
  - Namespace operations: manifests (`self.root.join("repos")...join("manifests")`), tags, referrers, repo memberships, and migration checkpoints.
  If only `head_blob` uses a pinned descriptor while other operations resolve `self.root` by pathname, the backend enters a mixed root ownership state.
- **Read-Side Coherence vs Full Backend Coherence**:
  - *Read-Side Coherence*: Sharing a pinned root descriptor between `head_blob` and `open_blob` ensures that metadata inquiry and payload streaming query the identical directory inode. This eliminates the read-side split-brain where `head_blob` reports blob presence from an old inode but `open_blob` reads from a replaced pathname.
  - *Remaining Write-Side Coherence Requirements*: Even if `head_blob` and `open_blob` share a descriptor, upload deduplication hazards are **not eliminated** while mutations resolve `self.root` by pathname. `UploadCoordinator` calls `head_blob` to verify blob presence before committing an upload. If `head_blob` checks the pinned descriptor but upload staging and CAS finalization write via pathname to a replaced root, deduplication and writes still diverge: `head_blob` may report a blob present on the old descriptor (skipping the write), while the new storage tree lacks the blob entirely.
  - *Quarantine Moves and Deletions*: Operations moving blobs between quarantine and primary paths or deleting blobs during GC resolve by pathname, and can diverge from descriptor-pinned reads if the root path is swapped.
- **Remaining Deployment Assumptions**:
  A descriptor-relative streaming slice (`open_blob`) is a valuable read-side prerequisite, but it does **not** establish complete backend root coherence. Until *all* reads, writes, staging, commits, quarantine moves, and deletions share descriptor-relative root ownership, the system continues to rely on the deployment assumption that `self.root` is strictly static and never renamed, replaced, or unmounted during the process lifetime.
- **Comparison of Bounded Options**:
  - **Option A: Defer production metadata cutover until the required operations share coherent root ownership.**
    - *What code enforces*: Code guarantees descriptor-relative containment and inode coherence across read operations (`head_blob` and `open_blob`), establishing the foundation for subsequent write-side descriptor migration.
    - *Remaining deployment assumptions*: Write-side operations (uploads, CAS commits, quarantine moves) still rely on path-based immutability until fully migrated.
    - *Residual risks*: Write-side path divergence remains until writes are migrated; however, `BlobCasReader` port operations (`head_blob` and `open_blob`) remain internally coherent.
    - *Prerequisite*: Extract descriptor-relative payload streaming (`BlobCasReader::open_blob`) in `storage-fs` and wire both `head_blob` and `open_blob` together in `FsStorage`.
  - **Option B: Permit metadata-only cutover under an explicitly accepted deployment constraint forbidding root replacement during process lifetime.**
    - *What code enforces*: Code enforces descriptor-relative containment strictly for `head_blob`.
    - *Remaining deployment assumptions*: Assumes the deployment environment guarantees that `self.root` pathname is strictly static, immutable, and never renamed, replaced, or unmounted during the process lifetime.
    - *Residual risks*: Immediate split-brain between `head_blob` and `open_blob` if the root path changes; upload deduplication can claim presence on an old inode while `open_blob` fails on the new path; path traversal in `open_blob` and writes remains unmitigated.
    - *Prerequisite*: Wire `FsMetadataReader` into `FsStorage` for `head_blob` only, with an accepted operational constraint and deployment documentation.
- **Recommendation**:
  Recommend **Option A**: Defer production cutover of `head_blob` until at least `open_blob` shares descriptor-relative root ownership. Permitting `head_blob` and `open_blob` to diverge across separate root directory inodes violates basic CAS semantics of `BlobCasReader`. While Option A does not eliminate write-side pathname risks, it ensures read-side coherence and prevents `open_blob` from failing on blobs that `head_blob` reported present.

### 4.3 Unresolved Startup-Capability Decision: Execution Environment Usability of `openat2`
- **The Usability Problem**:
  `FsMetadataReader::open` acquires its directory descriptor using `libc::open` (`O_PATH | O_DIRECTORY | O_CLOEXEC`), which does not invoke `openat2`. If `openat2` is unsupported by the host kernel (< 5.6) or blocked by a container seccomp filter, this is only discovered on the first lookup.
- **Why Probing a Non-Existent Filename is Flawed**:
  Treating `ENOENT` on a reserved-looking filename as proof of capability is unsound:
  1. A filename is never guaranteed absent on arbitrary storage volumes.
  2. Receiving `ENOENT` does not prove that `openat2` was executed with the intended containment semantics or that seccomp permits descriptor acquisition; treating an error as a success signal risks false positives.
- **Recommended Backend-Private Probe Design**:
  The probe must obtain a real file descriptor using `openat2` beneath the pinned root descriptor and validate it, rather than treating an error as success:
  - *Target Path*: Opens `"."` relative to the pinned root directory descriptor (`root_fd`).
  - *Syscall Invocation*: Calls Linux `openat2(root_fd.as_raw_fd(), c".", &how, sizeof(how))` with:
    - `how.flags = O_PATH | O_DIRECTORY | O_CLOEXEC`
    - `how.resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`
  - *Crucial Contract Distinction*:
    - `"."` remains **strictly invalid** as a generic `ObjectKey` (rejected by `ObjectKey::parse` with `ObjectKeyError::DotSegment`).
    - The capability probe is a **backend-private syscall probe** on `FsMetadataReader`. It operates directly on the pinned raw descriptor via the C string `c"."` and does **not** construct an `ObjectKey` or route through the regular-file-only `head` method.
    - `ObjectKey` validation rules are not weakened.
  - *Descriptor Validation and Cleanup*:
    - On success, `openat2` returns a new raw file descriptor referencing the root directory.
    - The raw descriptor is immediately wrapped in `OwnedFd` (`unsafe { OwnedFd::from_raw_fd(raw_fd) }`).
    - The probe validates the opened descriptor via `fstat`, verifying that `st_mode & S_IFMT == S_IFDIR`.
    - The `OwnedFd` is dropped immediately, guaranteeing deterministic RAII closure without descriptor leakage.
- **Syscall Outcomes and Error Mapping**:
  - *Success*: Returns a valid descriptor for `"."` -> descriptor validated and closed -> returns `Ok(())`.
  - *`ENOSYS`*: The host kernel does not support `openat2(2)` -> returns `FsMetadataError::SyscallUnsupported(io_err)`.
  - *`EACCES` / `EPERM`*: Permission or access denied by kernel DAC, LSM, mount options, or a container seccomp filter -> maps to a typed error (e.g. proposed `FsMetadataError::ProbeDenied(io_err)`). Note: Seccomp filters commonly return `EPERM`, but `EPERM`/`EACCES` cannot be inferred as uniquely caused by seccomp without kernel auditing.
  - *Other OS Errors (`EMFILE`, `ENFILE`, `EIO`)*: Mapped to a typed I/O error with underlying `std::io::Error`.
  - *Non-Linux Platforms*: Returns `FsMetadataError::PlatformUnsupported`.
- **What the Probe Exercises vs What It Does Not Establish**:
  - *What It Exercises*:
    1. Kernel support for the `openat2(2)` syscall number.
    2. Kernel acceptance of the `open_how` struct and flags (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`).
    3. Absence of seccomp / LSM rules denying `openat2` execution on the process.
    4. Validity and directory access permissions on the pinned root descriptor.
  - *What It Does NOT Establish*:
    1. It does not establish that child paths or subdirectories exist or can be created.
    2. It does not exercise multi-component path resolution across directory boundaries.
    3. It does not verify regular-file lookup (`S_IFREG`), because `"."` is a directory (`S_IFDIR`).
    4. It does not establish that payload read permissions (`O_RDONLY`) or write permissions will be granted on child files.
    5. It does not establish future runtime readiness or guarantee against dynamic seccomp reconfiguration, filesystem remounts, or storage media failures during process lifetime.
- **Execution Context: Synchronous vs Async Blocking-Pool Probe**:
  - *Option 1: Synchronous Startup Probe (`probe_capability(&self) -> Result<(), ...>`) [RECOMMENDED]*:
    - Executed directly on the initialization thread during application startup or constructor execution (`FsStorage::try_new`).
    - *Advantage*: Deterministic boot-time failure before starting HTTP listeners; no dependency on an entered Tokio runtime or thread pool dispatch.
    - *Limitation*: The syscall runs on the calling thread; if the root directory is on an unresponsive or hung network filesystem (NFS/FUSE), the startup thread could block. This limitation is identical to `FsMetadataReader::open`, which is already synchronous.
  - *Option 2: Async Blocking-Pool Probe*:
    - Offloaded via `tokio::task::spawn_blocking`.
    - *Advantage*: Does not block the caller thread if executed in an async context.
    - *Limitation*: Requires an active, entered Tokio runtime; adds scheduling overhead for a one-time boot check.
  - *Recommendation*: Implement the probe as a synchronous method on `FsMetadataReader` in `storage-fs`.
- **Comparison of Policy Choices**:
  - **Option A: Lookup-time fail-closed errors using current reader.**
    - At first lookup, `openat2` fails with `ENOSYS` or `EPERM`. The registry maps this to `StorageError::configuration` (or `io`), failing closed without uncontained fallback.
    - *Drawback*: Failure occurs during live client requests rather than during startup or readiness probing.
  - **Option B: Explicit startup capability check before enabling the filesystem backend.**
    - Invokes `probe_capability()` during `FsStorage::try_new`.
    - *Advantage*: Fails fast during initialization before serving traffic.
  - *Recommendation*: Recommend **Option B** (explicit startup probe in `storage-fs`), while noting that Option A remains the current baseline behavior.

### 4.4 Acceptance of Intentional Behavioral Changes
Prior to production cutover, the following intentional behavioral changes must be formally accepted:
1. *Rejection of Symlinks Below Root (D7)*: Blobs symlinked to other files inside or outside the storage root will be hard-rejected with `StorageError::io` instead of succeeding.
2. *Rejection of Directory Objects (D2)*: Paths resolving to directories will return `StorageError::io` rather than returning the directory's metadata size.
3. *Strict Failure on Unusable `openat2` (D3)*: Environments lacking usable `openat2` will fail closed at query time (or at startup if a probe is implemented) rather than falling back to uncontained path lookup.
4. *Root Inode Binding (D1)*: Lookups remain bound to the root directory inode opened at startup; renaming the path on disk will not redirect lookups.

### 4.5 Future Production Design Sketch (Informational Only — Not Part of Next Slice)

> [!NOTE]
> **Informational Design Sketch Only**
> The following code blocks illustrate how production `FsStorage` might integrate in a future cutover slice after all production prerequisites (blocking execution boundary, root ownership, and capability probe) are resolved and approved. This is **NOT** part of the current slice.

```rust
// FUTURE PRODUCTION SKETCH ONLY - NOT PART OF CURRENT SLICE
pub struct FsStorage {
    root: PathBuf,
    metadata_reader: std::sync::Arc<dyn storage_core::ObjectMetadataReader>,
    // ... existing fields ...
}

impl FsStorage {
    pub fn try_new(root: PathBuf, max_upload_bytes: u64) -> Result<Self, StorageError> {
        ensure_dir(&root)?;
        let reader = storage_fs::FsMetadataReader::open(&root)
            .map_err(|e| StorageError::configuration(format!("failed to open metadata reader: {e}")))?;

        // Optional synchronous startup probe if Option B is accepted:
        // reader.probe_capability()
        //     .map_err(|e| StorageError::configuration(format!("openat2 capability check failed: {e}")))?;

        let metadata_reader = std::sync::Arc::new(reader);
        // ...
    }
}

#[async_trait]
impl BlobCasReader for FsStorage {
    async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError> {
        // Production delegation using the internal blocking execution boundary
        // and typed error translation.
    }
}
```

---

## 5. Architectural Decisions: Completed vs Remaining Status

### 5.1 Completed Mechanisms and Evidence
- **Descriptor-Relative Containment (D7, D2)**: Standalone reader implementation with `O_PATH` descriptor pinning, `openat2` resolution restrictions, and `S_IFREG` object validation. Completed and verified in `storage-layer-rust@72062016a127ee435979615d959396d7d7212bc2`.
- **Test-Only Integration Seam**: Registry-owned `head_blob_seam` and `translate_read_error` under `#[cfg(test)]`. Completed and verified in `registry-rust@f7e174bbf74ef48ba13ed57229eebcc1fd70d1c9`.
- **Internal Blocking Execution Boundary (D4)**: `FsMetadataReader::head` offloaded to `tokio::task::spawn_blocking` with owned descriptor and key. Completed and verified in `storage-layer-rust@6757b0f2cb82ed92e9415578616b323db8c81907`.
- **Downstream Dependency Compatibility**: Revalidation passed across 94 tests (12 fake seam tests, 12 real seam tests, 70 existing characterization/regression tests) with `Cargo.lock` locked. Completed in `registry-rust@56d6911c1fee16faa24936aaa88107d0979b7b4b`.

### 5.2 Recommended but Unaccepted Decisions
| # | Decision Item | Recommended Default | Status | Rationale & Consequence |
|---|---|---|---|---|
| **D1** | **Configured Root Symlink Policy** | Allow root symlink during initialization; pin resolved descriptor. | **Recommendation** (Awaiting Acceptance) | Container environments often mount storage via symlinks. Pinning the descriptor at startup guarantees subsequent lookups cannot escape or be swapped. |
| **D2** | **Directory & Non-Regular Object Rejection** | Return `StorageError::io` (`Internal`). | **Recommendation** (Awaiting Acceptance) | Directories at blob paths are layout corruptions, not missing files. Returning `NotFound` would trigger quarantine fallback or HTTP 404, masking corruption. |
| **D3** | **Execution Environment Usability of `openat2`** | Implement explicit startup capability probe opening `"."` relative to pinned descriptor (Option B); fail closed if unusable; do not fall back to legacy pathname lookup. | **Recommendation** (Awaiting Acceptance) | `openat2` provides kernel-guaranteed atomicity. Fast startup failure is preferable to delayed query-time failure. Systems without usable `openat2` fail closed without insecure fallback. |
| **D5** | **Subtree Mount Crossings (`RESOLVE_NO_XDEV`)** | Leave `RESOLVE_NO_XDEV` unset (allow mounts beneath root). | **Recommendation** (Awaiting Acceptance) | Deployments may partition storage across multiple mount points beneath the root (e.g. separate mount for `quarantine` or `blobs`). `RESOLVE_BENEATH` still prevents escaping the root. |
| **D6** | **Root Lifetime Coherence Policy** | Defer production metadata cutover until payload reads (`open_blob`) share coherent descriptor root ownership (Option A). | **Recommendation** (Awaiting Acceptance) | Metadata-only cutover creates split-brain risks with `open_blob` and upload deduplication. Full CAS coherence requires unified descriptor ownership. |
| **D7** | **Rejection of Symlinks Below Root** | Reject all symlinks beneath root with `StorageError::io` without quarantine fallback. | **Recommendation** (Awaiting Acceptance) | Closes symlink traversal vulnerabilities inside and outside root. Core security objective of O-05. |
| **D8** | **Registry Mapping for Runtime/Task Errors** | Map `RuntimeMissing` and `TaskJoinFailed` to `StorageError::internal(StorageErrorKind::Backend, ...)` preserving diagnostic message text. | **Recommendation** (Awaiting Acceptance) | `StorageError::Internal` stores kind and message, not a boxed source chain. Diagnostic text is preserved; typed source-chain preservation would require a separate contract change. |
| **D9** | **Non-Linux Platform Policy** | Fail closed with `StorageError::configuration("platform unsupported")`. | **Recommendation** (Awaiting Acceptance) | Containment guarantees require Linux `openat2`. Non-Linux platforms fail closed without insecure fallback. |

### 5.3 Remaining Implementation Prerequisites
1. **Startup Capability Probe**: Implement synchronous non-mutating capability probe opening `"."` in `storage-fs`.
2. **Registry Runtime Error Mapping**: Update `translate_read_error` in `registry-rust` to explicitly map `RuntimeMissing` and `TaskJoinFailed` with focused test evidence.
3. **Descriptor-Relative Payload Streaming**: Implement descriptor-relative `open_blob` in `storage-fs` to resolve read-side root ownership before production cutover.

### 5.4 Unverified Platform or Operational Behavior
- Non-Linux platforms: Source-designed to return `PlatformUnsupported`; execution unverified.
- Syscall Duration: Userspace cannot guarantee bounded completion of arbitrary filesystem syscalls (e.g. unresponsive network filesystems).

---

## 6. Recommended Next Implementation Slice: Standalone `openat2` Capability Probe in `storage-fs`

The smallest useful prerequisite supported by the findings is to establish the backend-private startup capability probe mechanism in `storage-fs`.

### 6.1 Objective
Implement a backend-private, synchronous capability probe method on `FsMetadataReader` in `storage-layer-rust` that executes Linux `openat2` on `"."` beneath the pinned root directory descriptor, validating that the host kernel and container execution environment permit descriptor-relative resolution before queries are served.

### 6.2 Exact Repository and File Scope
- Repository: `/home/dietmar/devel/rust/storage-layer-rust`
- Files to modify:
  1. `crates/storage-fs/src/reader.rs`:
     - Implement `pub fn probe_capability(&self) -> Result<(), FsMetadataError>`.
     - Opens `c"."` relative to `root_fd` with `O_PATH | O_DIRECTORY | O_CLOEXEC` and `resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`.
     - Wraps returned descriptor in `OwnedFd`, verifies `fstat` mode is `S_IFDIR`, and drops the descriptor immediately.
     - Maps `ENOSYS` to `FsMetadataError::SyscallUnsupported`.
     - Maps `EACCES` or `EPERM` to a proposed typed error variant (e.g. `FsMetadataError::ProbeDenied`).
  2. `crates/storage-fs/src/error.rs`:
     - Add proposed typed error variant: `ProbeDenied(#[source] std::io::Error)` (explicitly labeled as a proposed API addition).
  3. `crates/storage-fs/src/lib.rs`:
     - Document probe capability method.
  4. `crates/storage-fs/README.md`:
     - Document capability probe semantics, flags, and limitations.
- `registry-rust` is kept **strictly read-only** in this slice (startup invocation in `FsStorage::try_new` is deferred to a subsequent slice).

### 6.3 Public API and Behavioral Impact
- Adds `pub fn probe_capability(&self) -> Result<(), FsMetadataError>` to `storage_fs::FsMetadataReader`.
- Zero changes to existing `head` behavior, generic `ObjectKey` validation, or production registry code.

### 6.4 Focused Acceptance Tests
1. *Real Descriptor-Success Test*: `test_fs_metadata_probe_capability_success_on_valid_root` executes on a real temporary directory, verifies `Ok(())`, and asserts no file descriptor leak.
2. *Non-Mutation Test*: `test_fs_metadata_probe_capability_does_not_mutate_directory` checks directory entry count and timestamps before and after probing, confirming zero disk mutations.
3. *Synthetic Failure-Classification Tests* (clearly distinguished from real syscall observations):
   - Simulated `ENOSYS` produces `FsMetadataError::SyscallUnsupported`.
   - Simulated `EPERM` / `EACCES` produces `FsMetadataError::ProbeDenied` without inferring seccomp as the unique cause.
4. *Non-Linux Platform Test*: Synthetic compilation/cfg test verifying `PlatformUnsupported` is returned on non-Linux targets.

### 6.5 Required Verification Commands
```bash
cd /home/dietmar/devel/rust/storage-layer-rust
cargo fmt --check
cargo check --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
cargo test --locked --workspace --doc
git diff --check
```

### 6.6 What Remains Unresolved Afterward
- Registry startup invocation in `FsStorage::try_new`.
- Registry mapping of runtime/task errors (`RuntimeMissing` / `TaskJoinFailed`).
- Read-side vs write-side root coherence (descriptor-relative payload streaming and writes).
- Production routing cutover.
- Quality gates (**O-05**, **O-03**, **O-06**, **O-13**, **O-16**, **D-06**).

---

## 7. Quality Gate Status Summary

- **Gate O-05 (Filesystem Metadata Containment)**: Remains **OPEN**. This document assesses integration feasibility, records completed milestones, and defines remaining decisions; production cutover has not occurred.
- **Gates O-03, O-06, O-13, O-16, D-06**: Retain existing meanings and remain **OPEN**.
- **Platform Scope**: Non-Linux execution remains source-designed but execution-unverified and deferred.
