> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Contained manifest reads landed (`1924225`); manifests later moved onto `manifest_domain` (`76209a9`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Architecture Design: Production Filesystem Manifest Read Integration

**Repository:** `registry-rust`
**Target Path:** `docs/architecture/filesystem-manifest-read-production-integration-design.md`
**Authoritative Baselines:**
- `registry-rust` HEAD: `e36ca43b50707e44b463e93defcb1b8ce8f045b8`
- `storage-layer-rust` HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`

**Scope:** Concrete production integration design for cutting over filesystem manifest reads (`head_manifest`, `get_manifest`) to descriptor-relative containment using the shared `Arc<FsMetadataReader>`.
**Status:** **DESIGN ONLY — NOT AUTHORIZED FOR PRODUCTION CUTOVER**.
**Quality Gates:** **O-03, O-04, O-05, O-06, O-13, O-15, O-16, and D-06 remain OPEN**.

---

## 1. Executive Summary & Problem Context

In previous storage extraction milestones:
1. Production CAS blob metadata inspection (`head_blob`) and payload streaming (`open_blob`) were successfully cut over to the extracted `storage_fs::FsMetadataReader` via `FsBlobCasReadAdapter` beneath a pinned directory descriptor using Linux `openat2` containment flags (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`).
2. Production CAS listing (`list_cas_blobs_page`) was cut over to extracted descriptor-relative directory enumeration (`enumerate_dir`) and file metadata inspection (`inspect_file_metadata`), sharing the same `Arc<FsMetadataReader>` initialized during startup offload.
3. Manifest read behavior across `head_manifest` and `get_manifest` was characterized against production code (`docs/architecture/filesystem-manifest-read-characterization.md`).
4. A test-only integration seam (`src/storage/fs/manifest_seam.rs`) and architectural assessment (`docs/architecture/filesystem-manifest-read-integration-assessment.md`) proved that generic payload reader primitives (`ObjectPayloadReader::open_payload`) can satisfy all registry manifest read requirements: pre-composition key safety validation, complete stream buffering, size derivation from consumed bytes, JSON media-type extraction with OCI default fallback, corruption detection, and typed error mapping.

However, **production manifest reads in `src/storage/fs.rs:839-878` remain on legacy, uncontained pathname operations** (`tokio::fs::read` over `self.manifest_path(name, digest)`). These uncontained operations:
- Traverse symlinks transparently, permitting arbitrary file access outside the repository root if a symlink exists inside the repository directory tree.
- Rely on unvalidated string concatenation via `PathBuf::join(name)`. Direct storage callers supplying path traversal sequences (`..`) can navigate outside the storage root, and leading slashes (`/`) cause `PathBuf::join` to discard the `repos` prefix and target a different path hierarchy on the host filesystem.
- Do not utilize the descriptor containment guarantees provided by the existing `storage_fs::FsMetadataReader`.

This document specifies the concrete production integration architecture to cut over `FsStorage::head_manifest` and `FsStorage::get_manifest` to the contained reader while:
- **Reusing the Shared Reader:** Routing calls through the existing `Arc<FsMetadataReader>` already owned by `FsStorage` and initialized during asynchronous startup offload, avoiding root directory reopening while acquiring fresh per-call payload file descriptors.
- **Consolidating Policy into a Single Implementation:** Promoting `manifest_seam.rs` into a crate-private production module (`src/storage/fs/manifest.rs`), moving tests alongside the implementation, and retiring `manifest_seam.rs`.
- **Eliminating Duplicated Media-Type Parsing:** Refactoring `detect_manifest_media_type` into a single shared helper used across reads, writes (`put_manifest`), and tests, preserving write behavior without mutation extraction.
- **Enforcing Pre-Composition Key Validation:** Explicitly validating repository names for direct storage callers before key composition, rejecting traversal sequences and unsafe characters with `StorageError::InvalidRepoName`.
- **Preserving Compatibility Contracts:** Preserving full payload buffering, stream-derived size, OCI fallback rules, corrupt data classification, lack of digest verification, and the `repos/<repo>/manifests/<digest.hex()>` layout.
- **Formulating Exact Error Mappings:** Specifying error translations strictly based on the established `read_adapter.rs` taxonomy.
- **Documenting Operational & Concurrency Boundaries:** Articulating descriptor pinning vs pathname mutations, absence of atomic snapshot guarantees during concurrent writes, procfs reopening assumptions, and platform boundaries.
- **Providing a Concrete Verification & Rollback Plan:** Defining test coverage, cargo verification commands, and a zero-data-migration rollback path.

---

## 2. Source Implementation Inspection & Current Architecture

This design is strictly grounded in empirical source inspection across `registry-rust` and `storage-layer-rust`.

### 2.1 Production Storage Construction & Shared Reader Ownership
In `registry-rust/src/storage/fs.rs:190-230`, `FsStorage` is defined and initialized:

```rust
// src/storage/fs.rs:190-198
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

The construction sequence in `FsStorage::try_new` (`src/storage/fs.rs:200-230`):
1. Verifies/creates root directory: `ensure_dir(&root)?` (`src/storage/fs.rs:202`).
2. Opens the root directory descriptor via host OS path resolution:
   `let reader = storage_fs::FsMetadataReader::open(&root).map_err(read_adapter::map_fs_startup_error)?;` (`src/storage/fs.rs:203-204`).
3. Probes the opened descriptor for Linux `openat2` containment support:
   `reader.probe_capability().map_err(read_adapter::map_fs_startup_error)?;` (`src/storage/fs.rs:205-207`).
4. Wraps the probed reader in an `Arc`:
   `let reader = std::sync::Arc::new(reader);` (`src/storage/fs.rs:208`).
5. Passes an `Arc` clone to the CAS read adapter:
   `let read_adapter = std::sync::Arc::new(read_adapter::FsBlobCasReadAdapter::new(std::sync::Arc::clone(&reader)));` (`src/storage/fs.rs:209-211`).
6. Stores `reader` directly in `self.reader` (`src/storage/fs.rs:227`).

In `src/storage/mod.rs:927-947`, `storage_wiring_try_from_config_async_with_factory` offloads this blocking initialization to a dedicated thread pool:
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
`FsStorage` already owns an initialized, capability-probed `Arc<FsMetadataReader>` that is shared across blob reads and CAS directory listing.

### 2.2 Production Legacy Manifest Read Operations
In `src/storage/fs.rs:416-423, 486-494, 839-878`, legacy manifest operations are implemented:

```rust
// src/storage/fs.rs:416-423
fn manifest_path(&self, name: &str, digest: &Digest) -> PathBuf {
    // data/repos/<name>/manifests/<hex>
    self.root
        .join("repos")
        .join(name)
        .join("manifests")
        .join(digest.hex())
}

// src/storage/fs.rs:486-494
async fn detect_manifest_media_type(&self, bytes: &[u8]) -> Result<String, StorageError> {
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|err| StorageError::corrupt_data(err.to_string()))?;
    let media_type = value
        .get("mediaType")
        .and_then(|v| v.as_str())
        .unwrap_or("application/vnd.oci.image.manifest.v1+json");
    Ok(media_type.to_string())
}

// src/storage/fs.rs:839-857
async fn head_manifest(
    &self,
    name: &str,
    digest: &Digest,
) -> Result<ManifestMeta, StorageError> {
    let path = self.manifest_path(name, digest);
    let bytes = match tokio::fs::read(&path).await {
        Ok(b) => b,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(StorageError::NotFound);
        }
        Err(err) => return Err(StorageError::io(err.to_string())),
    };
    let media_type = self.detect_manifest_media_type(&bytes).await?;
    Ok(ManifestMeta {
        size: bytes.len() as u64,
        media_type,
    })
}

// src/storage/fs.rs:859-878
async fn get_manifest(
    &self,
    name: &str,
    digest: &Digest,
) -> Result<(ManifestMeta, bytes::Bytes), StorageError> {
    let path = self.manifest_path(name, digest);
    let bytes = match tokio::fs::read(&path).await {
        Ok(b) => b,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(StorageError::NotFound);
        }
        Err(err) => return Err(StorageError::io(err.to_string())),
    };
    let media_type = self.detect_manifest_media_type(&bytes).await?;
    let meta = ManifestMeta {
        size: bytes.len() as u64,
        media_type,
    };
    Ok((meta, bytes::Bytes::from(bytes)))
}
```

Critical observations from active source:
1. `head_manifest` and `get_manifest` both read the entire file into memory via `tokio::fs::read(&path).await` because `detect_manifest_media_type` requires the full JSON byte slice.
2. `detect_manifest_media_type` is declared `async` and takes `&self`, but performs synchronous in-memory parsing via `serde_json::from_slice` without using `self` or performing I/O.
3. Path calculation `self.manifest_path(name, digest)` uses unvalidated `PathBuf::join(name)`. If `name` begins with `/`, `PathBuf::join` discards the preceding `repos` prefix and resolves to `/<name>/manifests/<digest.hex()>`. If `name` contains `..`, it traverses above the storage root.
4. `tokio::fs::read` traverses all symlinks encountered along the path, allowing references outside the repository.

### 2.3 Port Forwarding & Caller Call Chains
In `src/storage/ports/mod.rs:45-62`, the `ManifestReader` port trait is defined:
```rust
// src/storage/ports/mod.rs:45-62
#[async_trait]
pub trait ManifestReader: Send + Sync {
    async fn head_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<ManifestMeta, StorageError>;
    async fn get_manifest(
        &self,
        name: &str,
        digest: &Digest,
    ) -> Result<(ManifestMeta, Bytes), StorageError>;
    async fn list_manifest_digests_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<Digest>, Option<String>), StorageError>;
}
```

In `src/storage/ports/mod.rs:432-456, 697`, the `impl_storage_ports!` macro delegates port calls directly to `Storage`:
```rust
// src/storage/ports/mod.rs:432-447
#[async_trait::async_trait]
impl $crate::storage::ports::ManifestReader for $target {
    async fn head_manifest(
        &self,
        name: &str,
        digest: &$crate::registry::digest::Digest,
    ) -> Result<$crate::storage::ManifestMeta, $crate::storage::StorageError> {
        $crate::storage::Storage::head_manifest(self, name, digest).await
    }
    async fn get_manifest(
        &self,
        name: &str,
        digest: &$crate::registry::digest::Digest,
    ) -> Result<($crate::storage::ManifestMeta, bytes::Bytes), $crate::storage::StorageError> {
        $crate::storage::Storage::get_manifest(self, name, digest).await
    }
    // ...
}

// line 697:
impl_storage_ports!(crate::storage::fs::FsStorage);
```

Callers invoking `ManifestReader`:
- `ManifestReadService` (`src/application/manifest_read.rs:145, 199, 413`): Invoked during HTTP manifest HEAD/GET handling and proxy cache validation. Note that `ManifestReadService::head_manifest` (`src/application/manifest_read.rs:413`) actually calls `self.manifest_reader.get_manifest(repo, &digest)` in order to extract the optional OCI subject digest (`crate::manifest_refs::extract_subject_digest(&bytes)`).
- `ManifestLifecycleService` (`src/manifest_lifecycle.rs:525, 986, 1640`): Verifies manifest presence during publication journaling and lifecycle sweeps.
- Direct storage callers in tests and maintenance utilities.

### 2.4 Extracted Payload Reader Contracts & Implementation
In `storage-layer-rust/crates/storage-core/src/read.rs:150-158`:
```rust
#[async_trait]
pub trait ObjectPayloadReader: Send + Sync {
    async fn open_payload(&self, key: &ObjectKey) -> Result<ObjectPayload, ReadError>;
}
```
`ObjectPayload` (`storage-core/src/read.rs:94-116`) packages initial `ObjectMetadata` with an `ObjectStream: Pin<Box<dyn AsyncRead + Send + 'static>>`.

In `storage-layer-rust/crates/storage-fs/src/reader.rs:458-520` and `crates/storage-fs/src/reader/payload.rs:109-270`:
- `FsMetadataReader` implements `ObjectPayloadReader`.
- Execution is dispatched to a blocking task via `tokio::runtime::Handle::try_current()`.
- **Phase 1 (Contained Resolution):** Opens `key` relative to the pinned `root_fd` using `libc::SYS_openat2` with flags `O_PATH | O_CLOEXEC` and resolve flags `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`.
- **Post-Resolution Type Validation:** The opened descriptor is inspected via `libc::fstat`. Non-regular files (`mode & S_IFMT != S_IFREG`, such as directories, FIFOs, sockets, or devices) are rejected here with `FsMetadataError::UnsupportedObjectType { mode }`. They are rejected by `fstat` type validation after `O_PATH` acquisition, not by `openat2` itself. Symlinks are rejected directly by `openat2` due to `RESOLVE_NO_SYMLINKS`, returning `ResolutionRejected`.
- **Phase 2 (Readable Reopening):** Reopens `/proc/self/fd/<phase1_fd>` with `O_RDONLY | O_CLOEXEC`. Rechecks regular-file type, `st_dev`/`st_ino` identity equality against Phase 1, and non-negative size.
- Returns verified metadata and an owned `tokio::fs::File` stream as `ObjectStream`.

### 2.5 Committed Test Seam (`src/storage/fs/manifest_seam.rs`)
The test seam establishes:
1. Pre-composition key validation: `manifest_key(repo, digest)` (`manifest_seam.rs:51-93`).
2. Test-only media-type detection duplicate: `detect_manifest_media_type(bytes)` (`manifest_seam.rs:108-116`).
3. Payload reader delegation: `get_manifest_seam` and `head_manifest_seam` (`manifest_seam.rs:124-163`).
4. Error translation delegation to `super::read_adapter::translate_payload_read_error`.

---

## 3. Section 1: Production Call Flow & Shared Reader Ownership

### 3.1 Architectural Call Graph
Upon cutover, `FsStorage::head_manifest` and `FsStorage::get_manifest` will delegate directly to contained manifest read logic using the **same** existing `Arc<FsMetadataReader>` already owned by `FsStorage`:

```text
[HTTP Handlers / ManifestReadService / ManifestLifecycleService]
                     |
                     v
   [<FsStorage as ManifestReader> Port Forwarding]
       src/storage/ports/mod.rs:434, 442
                     |
                     v
        [FsStorage::head_manifest / get_manifest]
            src/storage/fs.rs:839, 859
                     |
                     v
   [crate::storage::fs::manifest::head_manifest_impl / get_manifest_impl]
            src/storage/fs/manifest.rs (PROPOSED)
     - Pre-composition validation: manifest_key(name, digest)
     - Reader invocation: reader.open_payload(&key)
     - Stream drain: tokio::io::AsyncReadExt::read_to_end(&mut bytes)
     - Consumed byte size: bytes.len() as u64
     - Media type extraction: manifest::detect_manifest_media_type(&bytes)
     - Error translation: read_adapter::translate_payload_read_error
                     |
                     v
   [storage_fs::FsMetadataReader::open_payload]
     - Pinned root directory file descriptor (self.reader)
     - Linux openat2(RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS)
     - Phase 1 O_PATH acquisition + fstat regular-file validation
     - Phase 2 /proc/self/fd/<fd> reopening with O_RDONLY | O_CLOEXEC
     - Identity (st_dev/st_ino) and type verification
```

### 3.2 Concrete Delegation Implementation
In `src/storage/fs.rs`, `head_manifest` and `get_manifest` delegate to the promoted module:

```rust
// Proposed implementation in src/storage/fs.rs
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

### 3.3 Strict Lifecycle & Invariant Guarantees
1. **Single Reader Instance:** Manifest reads borrow `self.reader.as_ref()`. No second `FsMetadataReader` is opened, no duplicate root directory file descriptor is created, and no per-request root opening occurs.
2. **Per-Call File Descriptors:** Reusing `self.reader` avoids reopening the root directory; individual payload read operations still acquire fresh per-call file descriptors (`openat2` Phase 1 descriptor and Phase 2 readable descriptor via `/proc/self/fd/<phase1_fd>`), wrapped in RAII guards (`OwnedFd` / `std::fs::File`).
3. **Preservation of Synchronous Constructor & Startup Offload:** `FsStorage::try_new` (`src/storage/fs.rs:200-230`) remains completely unchanged. Its existing sequence (`ensure_dir`, `FsMetadataReader::open`, `probe_capability`, `Arc::new`) and the enclosing `tokio::task::spawn_blocking` offload in `src/storage/mod.rs:937` are fully preserved.
4. **No Fallback to Pathname Operations:** If `open_payload` fails (e.g. `NotFound`, `PermissionDenied`, `ResolutionRejected`), the error is translated immediately through `translate_payload_read_error`. The implementation never falls back to `tokio::fs::read` or uncontained path resolution.
5. **Preservation of Other Storage Operations:** CAS blob reading (`head_blob`, `open_blob`) and CAS listing (`list_cas_blobs_page`) remain completely untouched, continuing to share the same `Arc<FsMetadataReader>`.

---

## 4. Section 2: Consolidation into a Single Manifest Policy Implementation

### 4.1 Module Promotion and File Changes
To avoid maintaining divergent policy between production code and test code, the test seam is promoted to a production module:

1. **Move / Promote File:**
   - Create: `src/storage/fs/manifest.rs` (containing the production implementation and all unit/recording-fake/real-fs tests).
   - Remove: `src/storage/fs/manifest_seam.rs`.
2. **Update Module Declaration in `src/storage/fs.rs:3596-3598`:**
   ```rust
   // Replace:
   // #[cfg(test)]
   // #[path = "fs/manifest_seam.rs"]
   // mod manifest_seam;

   // With:
   #[path = "fs/manifest.rs"]
   pub(crate) mod manifest;
   ```
   This follows the exact established pattern used for `src/storage/fs/listing.rs` (`src/storage/fs.rs:3593-3594`).

### 4.2 Consolidating Media-Type Parsing
Currently, `detect_manifest_media_type` is duplicated across:
- `src/storage/fs.rs:486-494` (`FsStorage::detect_manifest_media_type`, an `async` instance method).
- `src/storage/fs/manifest_seam.rs:108-116` (`manifest_seam::detect_manifest_media_type`, a synchronous free function).

Every caller of `detect_manifest_media_type` in `src/storage/fs.rs` and its tests:
1. `head_manifest` (`src/storage/fs.rs:852`) — promoted to `manifest::head_manifest_impl`.
2. `get_manifest` (`src/storage/fs.rs:872`) — promoted to `manifest::get_manifest_impl`.
3. `put_manifest` (`src/storage/fs.rs:889`) — writes a manifest payload after detecting its media type.
4. `test_detect_manifest_media_type_malformed_json_is_corrupt_data` (`src/storage/fs/tests.rs:1456`) — unit tests the helper against malformed JSON.

#### Proposed Consolidation Plan:
1. **Single Source of Truth in `src/storage/fs/manifest.rs`:**
   ```rust
   // src/storage/fs/manifest.rs
   pub(crate) fn detect_manifest_media_type(bytes: &[u8]) -> Result<String, StorageError> {
       let value: serde_json::Value = serde_json::from_slice(bytes)
           .map_err(|err| StorageError::corrupt_data(err.to_string()))?;
       let media_type = value
           .get("mediaType")
           .and_then(|v| v.as_str())
           .unwrap_or("application/vnd.oci.image.manifest.v1+json");
       Ok(media_type.to_string())
   }
   ```
2. **Preserve `FsStorage::detect_manifest_media_type` as a Facade:**
   In `src/storage/fs.rs:486-494`, preserve the method signature for internal callers (specifically `put_manifest`) and existing tests by delegating directly:
   ```rust
   // src/storage/fs.rs:486-488
   async fn detect_manifest_media_type(&self, bytes: &[u8]) -> Result<String, StorageError> {
       manifest::detect_manifest_media_type(bytes)
   }
   ```
   - `put_manifest` (`src/storage/fs.rs:889`) continues to call `self.detect_manifest_media_type(&bytes).await?` unchanged.
   - Write behavior is completely preserved. This slice does not extract or modify manifest mutation logic.
3. **Retire Parity Test:**
   The seam test `test_parity_with_fs_storage_detect_manifest_media_type` (`manifest_seam.rs:571-657`) is retired, as there is no longer a duplicated implementation. In its place, comprehensive unit tests in `manifest.rs` verify `detect_manifest_media_type` directly across all variants (custom media type, schema 2, schema 1, missing `mediaType`, non-string value, scalar JSON, empty 0-byte file, malformed JSON).

### 4.3 Domain-Specific Manifest Policy Stays in `registry-rust`
`storage-layer-rust` remains strictly backend-neutral and domain-free. All registry-specific concepts:
- Key layout `repos/<name>/manifests/<digest.hex()>`.
- Pre-composition repository safety checks.
- OCI manifest media-type defaults and fallback rules.
- JSON structure parsing and `CorruptData` classification.
- Representation of `ManifestMeta`.
remain exclusively within `registry-rust`.

---

## 5. Section 3: Compatibility Contract & Behavioral Invariants

The cutover preserves the characterized contracts and behavioral invariants:

| Contract / Invariant | Legacy Behavior | Proposed Production Behavior | Preservation Mechanism |
|---|---|---|---|
| **Full Payload Consumption on HEAD** | Reads entire file via `tokio::fs::read`. | Drains entire `ObjectStream` into memory buffer via `read_to_end`. | HEAD invokes `get_manifest_impl` and discards bytes, ensuring identical validation of JSON structure and complete payload availability. |
| **HEAD Parses JSON (Not Stat-Only)** | `serde_json::from_slice` parses JSON to extract `mediaType`. | `manifest::detect_manifest_media_type` parses JSON. | HEAD fails with `CorruptData` on malformed/empty files, matching legacy behavior. It is never replaced with a stat-only operation. |
| **Original Bytes Returned on GET** | Returns `Bytes` read directly from filesystem. | Returns `Bytes::from(bytes)` containing exact stream contents. | Returns raw consumed bytes without re-serialization or normalization. |
| **Size Derived from Consumed Bytes** | Derived from `bytes.len() as u64`. | Derived from `bytes.len() as u64`. | Derived strictly from consumed stream length; does not use `payload.metadata().size()`. |
| **Media-Type Extraction & Fallback** | Extracts string `"mediaType"`; defaults to `"application/vnd.oci.image.manifest.v1+json"` for missing, non-string, or scalar JSON. | Identical extraction and fallback logic. | Single consolidated helper `detect_manifest_media_type`. |
| **Corrupted Payload Classification** | Empty file (0 bytes) or malformed JSON yields `StorageErrorKind::CorruptData`. | Empty file or malformed JSON yields `StorageErrorKind::CorruptData`. | Identical `serde_json::from_slice` error mapping to `StorageError::corrupt_data(err.to_string())`. |
| **No Digest Content Verification** | Does not compute digest over read bytes; requested digest is used solely for filename lookup. | Does not compute digest over read bytes. | Omits hash computation during reads; matches existing production behavior. |
| **Storage Layout Conventions** | Looks up `repos/<name>/manifests/<digest.hex()>`. Supports SHA-256 (64 hex) and SHA-512 (128 hex) without algorithm prefix. | Composes `repos/<name>/manifests/<digest.hex()>`. Supported algorithms use raw hex. | `manifest_key` formats `repos/{repo}/manifests/{digest.hex()}`. |

### Operational Boundaries and Non-Prerequisites:
- **No Size Limit Imposed:** Manifests continue to be buffered into memory in their entirety without an artificial byte ceiling. While recorded as an operational consideration, introducing a size limit is **not** an implementation prerequisite for read extraction.
- **No Streaming Response API:** Manifest responses remain buffered `(ManifestMeta, Bytes)`.
- **No In-Memory Caching:** Manifests are read directly from storage on every request.
- **No Manifest Listing Changes:** `list_manifest_digests_page` remains untouched in this slice.

---

## 6. Section 4: Repository-Name and Containment Behavior

### 6.1 Pre-Composition Key Validation
In `src/storage/fs/manifest.rs`, `manifest_key(repo: &str, digest: &Digest)` performs strict validation before composing the relative `ObjectKey`:

```rust
// Proposed implementation in src/storage/fs/manifest.rs
pub(crate) fn manifest_key(repo: &str, digest: &Digest) -> Result<ObjectKey, StorageError> {
    if repo.is_empty() {
        return Err(StorageError::InvalidRepoName(
            "repository name cannot be empty".to_string(),
        ));
    }
    if repo.starts_with('/') || repo.ends_with('/') {
        return Err(StorageError::InvalidRepoName(
            "repository name cannot have leading or trailing slashes".to_string(),
        ));
    }
    if repo.contains('\\') {
        return Err(StorageError::InvalidRepoName(
            "repository name cannot contain backslashes".to_string(),
        ));
    }
    if repo.contains(|c: char| c == '\0' || c.is_ascii_control()) {
        return Err(StorageError::InvalidRepoName(
            "repository name cannot contain NUL bytes or control characters".to_string(),
        ));
    }

    for segment in repo.split('/') {
        if segment.is_empty() {
            return Err(StorageError::InvalidRepoName(
                "repository name cannot contain empty segments (repeated slashes)".to_string(),
            ));
        }
        if segment == "." {
            return Err(StorageError::InvalidRepoName(
                "repository name cannot contain '.' segments".to_string(),
            ));
        }
        if segment == ".." {
            return Err(StorageError::InvalidRepoName(
                "repository name cannot contain '..' segments (path traversal attempt)".to_string(),
            ));
        }
    }

    let key_str = format!("repos/{repo}/manifests/{}", digest.hex());
    ObjectKey::parse(&key_str).map_err(|e| StorageError::InvalidRepoName(e.to_string()))
}
```

### 6.2 Distinction Across Validation Layers

| Validation Layer | Component & Scope | Enforced Rules & Invariants | What It Accepts That Others Reject | What It Rejects |
|---|---|---|---|---|
| **`CanonicalRepoName`** | `src/registry/canonical_name.rs`<br>Enforced at HTTP API & service boundaries | Normative OCI Distribution Spec grammar: lowercase alphanumerics `[a-z0-9]`, strict segment separators (`.`, `_`, `__`, `-+`), max 255 bytes. | Accepts only fully compliant canonical OCI repository names. | Rejects uppercase characters (`Ubuntu`), control chars, repeated slashes, traversal sequences, and unapproved separators. |
| **`ObjectKey`** | `storage-core/src/key.rs`<br>Generic storage containment contract | Neutral relative path safety: non-empty, no leading/trailing `/`, no `//`, no `.` or `..` segments, no `\`, no `\0`, no Unicode control characters (`c.is_control()`), no Windows drive prefixes at string start (`bytes[0].is_ascii_alphabetic() && bytes[1] == b':'`), no UNC (`//`). | Accepts arbitrary safe relative paths, including uppercase, dots within segments, colon-bearing interior segments, and non-canonical characters. | Rejects absolute paths, leading drive prefixes at key start, empty segments, traversal, and invalid characters. |
| **`manifest_key`** | `src/storage/fs/manifest.rs`<br>Storage-level pre-composition check | Validates `repo` parameter before formatting `repos/{repo}/manifests/{hex}`: rejects empty, leading/trailing `/`, `\`, `\0`, ASCII control characters (`c.is_ascii_control()`), `//`, `.`, and `..` segments with `StorageError::InvalidRepoName`. | Accepts safe non-canonical names (e.g. uppercase names in test fixtures or internal tools) and interior colon-bearing segments. | Rejects any repository string that would manipulate path hierarchy or attempt traversal. |

### 6.3 Detailed Analysis of Key Validation Differences

1. **Drive-Prefix Analysis:**
   - `ObjectKey::parse` (`storage-core/src/key.rs:45-48`) validates Windows drive prefixes strictly at the **beginning of the entire key**:
     ```rust
     let bytes = s.as_bytes();
     if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
         return Err(ObjectKeyError::WindowsDrivePrefix);
     }
     ```
   - The seam constructs `format!("repos/{repo}/manifests/{}", digest.hex())`.
   - Therefore, a repository input such as `"C:/repo"` becomes `"repos/C:/repo/manifests/<hex>"`.
   - In `ObjectKey::parse`, the key begins with `'r'`, not `'C'`. Furthermore, segment-level validation in `ObjectKey` checks only for empty segments, `"."`, and `".."`—it does not reject colons in interior segments.
   - Neither `manifest_key` nor `ObjectKey::parse` rejects colon-bearing segments.
   - On Linux, colons are ordinary valid filename characters, so `"repos/C:/repo/manifests/<hex>"` is accepted as a valid relative pathname containing a colon-bearing directory segment `"C:"`.
   - This document describes the **actual current behavior** of the seam. Rejecting colons in repository segments would be a new, separate policy change that is not part of this cutover.

2. **Control Character Checks (ASCII vs Unicode):**
   - Pre-composition validation in `manifest_key` explicitly checks:
     `c == '\0' || c.is_ascii_control()`
     This targets the ASCII control range (`0x00..=0x1F` and `0x7F`) and NUL.
   - `ObjectKey::parse` checks:
     `s.chars().any(|c| c.is_control())`
     This evaluates Rust's standard `char::is_control()`, which corresponds to Unicode General Category `Cc` (control characters). This includes both ASCII controls (`U+0000..=U+001F`, `U+007F`) and Latin-1 supplement control characters (`U+0080..=U+009F`).
   - If a caller supplies a Latin-1 supplement control character that passes `manifest_key`, it is subsequently rejected when `ObjectKey::parse` runs, producing `ObjectKeyError::ControlCharacter`, which `manifest_key` maps to `StorageError::InvalidRepoName`.

3. **Absolute-Path Handling:**
   - In legacy code:
     `self.manifest_path(name, digest)` joins `self.root.join("repos").join(name).join("manifests").join(digest.hex())`.
   - If a caller supplies an absolute path like `"/etc/passwd"`:
     `PathBuf::join("/etc/passwd")` discards the preceding `self.root.join("repos")` prefix, but the subsequent `.join("manifests").join(digest.hex())` still appends `manifests/<hex>`.
     The resulting path is `/etc/passwd/manifests/<digest.hex()>`. It does **not** directly read `/etc/passwd` itself. Because `/etc/passwd` is a regular file on Linux, attempting to traverse a path beneath it fails with `ENOTDIR` (`StorageErrorKind::Io`).
     However, if a caller supplies an absolute path to a directory (e.g. `"/tmp"`), legacy code targets `/tmp/manifests/<hex>` on the host filesystem.
   - Under the proposed contained implementation:
     Any leading slash (`/`) is rejected immediately by `manifest_key` with `StorageError::InvalidRepoName("repository name cannot have leading or trailing slashes")`. No filesystem path is resolved.

4. **Path Traversal (`../`):**
   - *Legacy:* `self.root.join("repos").join("../../escaped_repo")` navigated above `self.root` and read files if they existed.
   - *Proposed:* Rejected immediately by `manifest_key` with `StorageError::InvalidRepoName("repository name cannot contain '..' segments (path traversal attempt)")`. No filesystem access occurs.

5. **Repeated Slashes (`//`):**
   - *Legacy:* Normalized by OS path resolution.
   - *Proposed:* Rejected with `StorageError::InvalidRepoName("repository name cannot contain empty segments (repeated slashes)")`.

6. **Current Directory Segments (`.`):**
   - *Legacy:* Resolved by OS.
   - *Proposed:* Rejected with `StorageError::InvalidRepoName("repository name cannot contain '.' segments")`.

7. **Backslashes (`\`):**
   - *Legacy:* Treated as a literal character in filenames on Linux.
   - *Proposed:* Rejected with `StorageError::InvalidRepoName("repository name cannot contain backslashes")`.

### 6.4 Containment Boundaries: Symlinks, Dangling Links, and Non-Regular Objects
Under `storage_fs::FsMetadataReader::open_payload`:
- **Symlink Rejection:** Linux `openat2` is configured with the complete flag combination:
  `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`.
  - `RESOLVE_BENEATH` alone only confines resolution beneath the root directory descriptor; it does not prohibit symlinks that remain inside the root.
  - The complete combination including `RESOLVE_NO_SYMLINKS` ensures that if any component of the path (the repository directory, the `manifests/` directory, or the manifest file itself) is a symlink, resolution is rejected by the kernel with `ELOOP` or `EXDEV`.
  - `storage-fs` maps this to `FsMetadataError::ResolutionRejected`.
  - Translated by `read_adapter.rs` to `StorageErrorKind::Io`.
  - **Both external symlinks and internal symlinks pointing within the storage root are rejected.** Legacy behavior allowed symlinks to be followed; proposed production behavior rejects all symlinks.

- **Dangling Symlinks vs Missing Paths:**
  - *Legacy Behavior:* When a manifest path is a dangling symlink (symlink exists, but its target does not), `tokio::fs::read` followed the symlink, failed with `ENOENT` on the absent target, and returned `StorageError::NotFound`.
  - *Proposed Contained Behavior:* `openat2` resolution with `RESOLVE_NO_SYMLINKS` encounters the symlink itself and fails immediately with `ELOOP` / `EXDEV`, returning `ResolutionRejected`. This maps via `translate_payload_read_error` to `StorageErrorKind::Io`.
  - *Genuine Missing Paths:* If the manifest path or an ancestor directory simply does not exist (no symlink present), `openat2` returns `ENOENT`, which maps to `ReadError::NotFound` -> `StorageError::NotFound`.

- **Non-Regular Objects (Directories, FIFOs, Sockets, Devices):**
  - `openat2` with `O_PATH | O_CLOEXEC` succeeds on non-regular objects.
  - Non-regular objects are rejected by subsequent `fstat` type validation after `O_PATH` acquisition (`mode & S_IFMT != S_IFREG`).
  - `storage-fs` returns `FsMetadataError::UnsupportedObjectType { mode }`.
  - Translated by `read_adapter.rs` to `StorageErrorKind::Io`.
  - *Note on Legacy FIFO Behavior:* Legacy `tokio::fs::read` on a directory fails with `EISDIR` (mapped to `StorageErrorKind::Io`). A FIFO without an active writer fails differently (e.g. `ENXIO` or hanging indefinitely); `EISDIR` applies strictly to directories.

---

## 7. Section 5: Exact Error Mapping Taxonomy

The authoritative source of error translation is [`super::read_adapter::translate_payload_read_error`](src/storage/fs/read_adapter.rs#L176-L178), which delegates to `translate_read_error(err, ReadOp::Payload)` (`src/storage/fs/read_adapter.rs:93-168`).

The table below contrasts legacy error behavior against the proposed production cutover:

| Error Condition / Trigger | Source Variant / Error Type | Legacy `FsStorage` Outcome | Proposed `StorageError` Outcome | Resulting `StorageErrorKind` / Value | Formatted Diagnostic Message | Evidence Status |
|---|---|---|---|---|---|---|
| **Input / Traversal Rejection** | `StorageError::InvalidRepoName` (from `manifest_key`) | Allowed uncontained traversal / path escape | `StorageError::InvalidRepoName` | `InvalidRepoName` | e.g. `"repository name cannot contain '..' segments (path traversal attempt)"` | Existing executed test: `test_manifest_key_rejects_unsafe_inputs`, `test_recording_fake_unsafe_input_suppresses_reader_invocation`. |
| **Absent File or Directory** | `ReadError::NotFound { key }` (`ENOENT`) | `StorageError::NotFound` | `StorageError::NotFound` | `NotFound` | None (unit variant) | Existing executed test: `test_real_fs_missing_manifest_and_missing_repo`, `test_recording_fake_acquisition_failures_suppress_stream_and_fallback`. |
| **Permission Denied (Acquisition)** | `ReadError::PermissionDenied { source: Some(io_err) }` (`EACCES`/`EPERM`) | `StorageError::Internal { kind: Io, message }` | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | `io_err.to_string()` (e.g. `"Permission denied (os error 13)"`) | Existing executed test: `test_recording_fake_acquisition_failures_suppress_stream_and_fallback`, `test_real_fs_permission_denied_ignored` (`#[ignore]`). |
| **Permission Denied (No Source)** | `ReadError::PermissionDenied { source: None }` | N/A | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | `"permission denied"` | Existing executed test: `read_adapter.rs` unit tests. |
| **Symlink Rejection (Internal or External)** | `ReadError::Backend { source: FsMetadataError::ResolutionRejected { source, .. } }` | Succeeded (followed symlink!) | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | `source.to_string()` (e.g. `"Too many levels of symbolic links (os error 40)"`) | Existing executed test: `test_real_fs_symlinks_rejected_without_reading_outside_content`, `test_recording_fake_acquisition_failures_suppress_stream_and_fallback`. |
| **Dangling Symlink Rejection** | `ReadError::Backend { source: FsMetadataError::ResolutionRejected { source, .. } }` | `StorageError::NotFound` | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | `source.to_string()` | Proposed updated test: `test_manifest_read_containment_symlink_traversal` (Scenario 4). |
| **Non-Regular Object (Directory/FIFO/Socket)** | `ReadError::Backend { source: FsMetadataError::UnsupportedObjectType { mode, .. } }` | `StorageErrorKind::Io` (`EISDIR` for dir) | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | `"unsupported object type (mode: {mode:#o})"` | Existing executed test: `test_real_fs_directory_substituted_for_manifest_rejected` (real fs directory). *Note: not covered by recording fake.* |
| **Phase 1 / Phase 2 Stat Failed** | `ReadError::Backend { source: FsMetadataError::StatFailed { stage, source } }` | N/A | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | `"failed to stat {stage} descriptor: {source}"` | Source-inspected branch (`read_adapter.rs:127-131`). |
| **Phase 2 Procfs Reopen Failed** | `ReadError::Backend { source: FsMetadataError::ProcfsReopenFailed { source } }` | N/A | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | `"failed to reopen descriptor via procfs: {source}"` | Source-inspected branch (`read_adapter.rs:132-136`). |
| **Target Identity Mismatch** | `ReadError::Backend { source: FsMetadataError::IdentityMismatch { .. } }` | N/A (TOCTOU silently accepted) | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | `fs_err.to_string()` | Source-inspected branch (`read_adapter.rs:137-139`). |
| **Invalid Metadata (Negative Size)** | `ReadError::Backend { source: FsMetadataError::InvalidMetadata { .. } }` | N/A | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | `fs_err.to_string()` | Source-inspected branch (`read_adapter.rs:140-142`). |
| **Syscall Unsupported (`openat2`)** | `ReadError::Backend { source: FsMetadataError::SyscallUnsupported(io_err) }` | N/A | `StorageError::Internal { kind: Configuration, message }` | `StorageErrorKind::Configuration` | `"openat2 is unavailable in this execution environment: {io_err}"` | Existing executed test: `test_recording_fake_typed_runtime_and_task_failures`. |
| **Platform Unsupported (Non-Linux Read)** | `ReadError::Backend { source: FsMetadataError::PlatformUnsupported }` | Succeeded via standard fs | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | `"platform unsupported: descriptor-relative containment requires Linux openat2"` | Source-inspected branch (`read_adapter.rs:143-145`). |
| **Tokio Runtime Missing** | `ReadError::Backend { source: FsMetadataError::RuntimeMissing(_) }` | N/A / Panic | `StorageError::Internal { kind: Backend, message }` | `StorageErrorKind::Backend` | `fs_err.to_string()` | Existing executed test: `test_recording_fake_typed_runtime_and_task_failures`. |
| **Task Join Failed (Panic in Thread)** | `ReadError::Backend { source: FsMetadataError::TaskJoinFailed(_) }` | N/A / Panic | `StorageError::Internal { kind: Backend, message }` | `StorageErrorKind::Backend` | `fs_err.to_string()` | Existing executed test: `test_recording_fake_typed_runtime_and_task_failures`. |
| **Ordinary / Unknown Backend Error** | `ReadError::Backend { source: Some(io_err) }` | `StorageErrorKind::Io` | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | `io_err.to_string()` | Source-inspected branch (`read_adapter.rs:154-158`). |
| **Mid-Stream I/O Failure** | `stream.read_to_end` fails (`std::io::Error`) | `StorageErrorKind::Io` | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | `"failed to read manifest payload: {e}"` | Existing executed test: `test_recording_fake_mid_stream_io_failure`. |
| **Empty File (0 Bytes)** | `detect_manifest_media_type` JSON parse EOF | `StorageErrorKind::CorruptData` | `StorageError::Internal { kind: CorruptData, message }` | `StorageErrorKind::CorruptData` | `"EOF while parsing a value at line 1 column 0"` | Existing executed test: `test_recording_fake_media_type_variants_and_corrupt_data`. |
| **Malformed JSON Payload** | `detect_manifest_media_type` JSON parse syntax error | `StorageErrorKind::CorruptData` | `StorageError::Internal { kind: CorruptData, message }` | `StorageErrorKind::CorruptData` | JSON syntax error description | Existing executed test: `test_recording_fake_media_type_variants_and_corrupt_data`. |

---

## 8. Section 6: Ownership, Concurrency & Operational Assumptions

### 8.1 Shared Reader Lifecycle & Root Pinning
1. **Startup Offload:** `FsStorage` initializes its single `storage_fs::FsMetadataReader` during `try_new` inside `tokio::task::spawn_blocking` (`src/storage/mod.rs:937`). It opens an owned file descriptor to the storage root and verifies `openat2` capability via `probe_capability()`.
2. **Descriptor Pinning vs Pathname Mutations:**
   - Manifest reads (`head_manifest`, `get_manifest`), CAS blob reads (`head_blob`, `open_blob`), and CAS listing (`list_cas_blobs_page`) resolve objects relative to the pinned root directory descriptor using `openat2`.
   - Manifest mutations (`put_manifest`, `delete_manifest`), tag operations (`set_tag`, `delete_tag`), upload management, and referrer updates still operate through uncontained pathname operations using `self.root: PathBuf` (`tokio::fs::create_dir_all`, `atomic_write_file`, `tokio::fs::remove_file`).
   - **Coherence Limitation Under Root Rename:** If the storage root directory is renamed on the host filesystem while the registry is running:
     - The pinned descriptor in `FsMetadataReader` continues to point to the original filesystem inode. Read operations continue to resolve against the original subtree.
     - Uncontained pathname mutations continue to target the old pathname string (`self.root`), causing mutations either to fail with `ENOENT` or to create a new directory tree at the old path.
     - Mixed-operation coherence across root rename/replacement is not supported.

### 8.2 Concurrency, Snapshot Isolation & Buffering Limits
1. **No Transactional Snapshot Isolation:**
   - Payload acquisition (`open_payload`) and payload stream consumption (`read_to_end`) do not form an atomic snapshot.
   - Concurrent writes or truncation may produce changed or mixed bytes, valid JSON, invalid JSON, or an I/O error. Neither an I/O error nor `CorruptData` is guaranteed.
   - Separate calls to `head_manifest` and `get_manifest` are separate point-in-time observations and may observe different content if the file is modified concurrently.
2. **Unbounded Buffering:**
   - Both `head_manifest` and `get_manifest` buffer the entire manifest payload into a `Vec<u8>` in heap memory.
   - Determining the OCI media type requires parsing the top-level `"mediaType"` field from the JSON document. Detecting structural corruption requires parsing the payload.
   - Manifests are buffered without an upper size bound. While recorded as an operational limitation, imposing a size limit is an application/transport policy decision and is not a prerequisite for read containment extraction.

### 8.3 Operating System & Procfs Assumptions
1. **Linux Dependency:**
   - Contained resolution strictly depends on Linux `openat2` (Linux kernel >= 5.6) with the full configured flag combination `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`.
2. **Phase 2 Procfs Reopening Mechanism:**
   - Phase 1 acquires an `O_PATH | O_CLOEXEC` descriptor and validates the object type via `fstat`.
   - Phase 2 opens `/proc/self/fd/<phase1_fd>` with `O_RDONLY | O_CLOEXEC` to obtain a readable descriptor, then rechecks regular-file type, `st_dev`/`st_ino` identity equality, and size.
   - This relies on the documented genuine, accessible, stable procfs assumption.
3. **Mount Namespace & Hardlink Limits:**
   - Descriptor-relative resolution prevents symlink traversal and escapes outside the pinned root directory descriptor.
   - It does **not** provide mount namespace isolation or prevent access to pre-existing hardlinks pointing to external inodes created prior to runtime.
4. **Non-Linux Platforms:**
   - Compilation and execution on non-Linux platforms remain explicitly unverified (Gate O-15).

---

## 9. Section 7: Concrete Implementation and Verification Plan

### 9.1 Exact Proposed File Modifications

1. **`src/storage/fs/manifest.rs` [NEW / PROMOTED]**:
   - Promoted from `src/storage/fs/manifest_seam.rs`.
   - Exports:
     - `pub(crate) fn manifest_key(repo: &str, digest: &Digest) -> Result<ObjectKey, StorageError>`
     - `pub(crate) fn detect_manifest_media_type(bytes: &[u8]) -> Result<String, StorageError>`
     - `pub(crate) async fn head_manifest_impl(reader: &(impl ObjectPayloadReader + ?Sized), repo: &str, digest: &Digest) -> Result<ManifestMeta, StorageError>`
     - `pub(crate) async fn get_manifest_impl(reader: &(impl ObjectPayloadReader + ?Sized), repo: &str, digest: &Digest) -> Result<(ManifestMeta, bytes::Bytes), StorageError>`
   - Contains unit tests, recording fake tests, and real-fs tests.
2. **`src/storage/fs/manifest_seam.rs` [DELETED]**:
   - Removed completely upon promotion to `manifest.rs`.
3. **`src/storage/fs.rs` [MODIFY]**:
   - Update module declaration at lines 3596-3598:
     ```rust
     #[path = "fs/manifest.rs"]
     pub(crate) mod manifest;
     ```
   - Update `head_manifest` (`src/storage/fs.rs:839-857`):
     ```rust
     async fn head_manifest(
         &self,
         name: &str,
         digest: &Digest,
     ) -> Result<ManifestMeta, StorageError> {
         manifest::head_manifest_impl(self.reader.as_ref(), name, digest).await
     }
     ```
   - Update `get_manifest` (`src/storage/fs.rs:859-878`):
     ```rust
     async fn get_manifest(
         &self,
         name: &str,
         digest: &Digest,
     ) -> Result<(ManifestMeta, bytes::Bytes), StorageError> {
         manifest::get_manifest_impl(self.reader.as_ref(), name, digest).await
     }
     ```
   - Delegate `detect_manifest_media_type` (`src/storage/fs.rs:486-494`):
     ```rust
     async fn detect_manifest_media_type(&self, bytes: &[u8]) -> Result<String, StorageError> {
         manifest::detect_manifest_media_type(bytes)
     }
     ```
4. **`src/storage/fs/tests.rs` [MODIFY]**:
   - Update `test_manifest_read_unvalidated_caller_path_traversal_gap`: Update assertion from expecting legacy traversal success to expecting `Err(StorageError::InvalidRepoName(_))` from `head_manifest` and `get_manifest`.
   - Update `test_manifest_read_containment_symlink_traversal`:
     - Scenarios 1, 2, 3: Update assertions from expecting legacy symlink traversal success to expecting `Err(StorageError::Internal { kind: StorageErrorKind::Io, .. })` caused by `ResolutionRejected`.
     - Scenario 4 (dangling symlink): Update assertion from legacy `Err(StorageError::NotFound)` to expecting `Err(StorageError::Internal { kind: StorageErrorKind::Io, .. })` for both `head_manifest` and `get_manifest`, verifying that `openat2` resolution rejection overrides missing-target semantics.
   - Add new production test `test_manifest_read_production_pinned_root_across_rename`:
     Constructs `FsStorage` with `initial_root`, writes a manifest file, renames `initial_root` to `renamed_root`, and verifies that `storage.head_manifest(repo, &digest).await` and `storage.get_manifest(repo, &digest).await` continue to succeed through the existing shared reader.

### 9.2 Verification Test Matrix

1. **Promoted Module Unit & Seam Tests (`src/storage/fs/manifest.rs`):**
   - `test_manifest_key_valid_single_and_multisegment`: Validates single and nested multi-segment repository keys.
   - `test_manifest_key_rejects_unsafe_inputs`: Verifies rejection of `..`, `.`, leading/trailing slashes, repeated slashes, backslashes, NUL, and ASCII control characters.
   - `test_recording_fake_exact_key_and_single_open_call`: Verifies exact key composition and exactly one `open_payload` call per HEAD/GET.
   - `test_recording_fake_size_derived_from_consumed_bytes_not_metadata`: Proves size is derived from bytes read, not acquisition metadata.
   - `test_recording_fake_media_type_variants_and_corrupt_data`: Covers custom media type, default OCI fallback, empty file (`CorruptData`), and malformed JSON (`CorruptData`).
   - `test_recording_fake_acquisition_failures_suppress_stream_and_fallback`: Verifies `NotFound`, `PermissionDenied`, and `ResolutionRejected`.
   - `test_recording_fake_mid_stream_io_failure`: Verifies mid-stream I/O error translation.
   - `test_recording_fake_typed_runtime_and_task_failures`: Verifies `RuntimeMissing`, `TaskJoinFailed`, and `SyscallUnsupported`.
   - `test_recording_fake_unsafe_input_suppresses_reader_invocation`: Confirms reader is not invoked when input is rejected.
   - `test_real_fs_representative_valid_manifests_and_nested_repos`: Real filesystem valid manifest reads.
   - `test_real_fs_supported_digest_algorithms`: Validates SHA-256 and SHA-512 raw hex paths.
   - `test_real_fs_missing_manifest_and_missing_repo`: Verifies `NotFound` on real filesystem.
   - `test_real_fs_symlinks_rejected_without_reading_outside_content`: Verifies symlinks fail closed with `Io`.
   - `test_real_fs_directory_substituted_for_manifest_rejected`: Verifies directory at manifest path fails closed with `UnsupportedObjectType` -> `Io`.
   - `test_real_fs_pinned_root_across_rename`: Validates reader retains access through pinned descriptor across directory rename.
   - `test_real_fs_permission_denied_ignored`: Real permission test marked `#[ignore]`.

2. **Production Storage & Port Tests (`src/storage/fs/tests.rs`):**
   - `test_manifest_read_representative_valid_oci_manifest`: Validates production `FsStorage::head_manifest`, `get_manifest`, and `ManifestReader` port forwarding.
   - `test_manifest_read_media_type_detection_variants`: Validates production media-type variants.
   - `test_manifest_read_empty_and_malformed_payloads_classify_as_corrupt_data`: Validates production corrupt data classification.
   - `test_manifest_read_missing_paths_return_not_found`: Validates missing repository, missing `manifests/`, and missing file.
   - `test_manifest_read_nondirectory_components_return_io`: Validates non-directory component failure.
   - `test_manifest_read_repository_naming_single_and_multisegment`: Validates nested repositories.
   - `test_manifest_read_supported_digest_algorithms_and_filename_forms`: Validates SHA-256 and SHA-512.
   - `test_manifest_read_unvalidated_caller_path_traversal_gap`: Updated to assert `InvalidRepoName` rejection.
   - `test_manifest_read_containment_symlink_traversal`: Updated across Scenarios 1–4 to assert `Io` rejection on symlinks, including dangling symlinks.
   - `test_manifest_read_production_pinned_root_across_rename`: Proposed production test verifying delegation through `self.reader` across root directory rename.
   - `test_manifest_read_permission_denied_ignored`: Ignored unprivileged permission test.

3. **Existing Startup Offload Regression Tests (Identified by Source Name):**
   - In `src/runtime.rs`:
     - `test_proxy_cache_per_upstream_construction_offloads_filesystem_to_blocking_thread`
     - `test_proxy_cache_default_construction_offloads_filesystem_to_blocking_thread`
     - `test_proxy_cache_construction_failure_preserves_error_kind_and_message`
     - `test_proxy_cache_blocking_task_failure_maps_to_backend`
     - `test_proxy_cache_callers_await_construction_before_proceeding`
   - In `src/cli/runtime.rs`:
     - `test_maintenance_runtime_acquire_offloads_filesystem_storage_to_blocking_thread`
     - `test_admin_clear_lock_offloads_filesystem_storage_to_blocking_thread`
     - `test_maintenance_acquire_awaits_construction_before_authority_acquisition`
     - `test_maintenance_acquire_factory_failure_preserves_error`
     - `test_maintenance_acquire_blocking_task_failure_maps_to_backend`

4. **Existing Write & Lifecycle Regression Targets:**
   - `delete_manifest_fails_safe_on_malformed_manifest` (`src/storage/fs/tests.rs:180`)
   - `test_referrers_tracked_on_manifest_put_and_delete` (`src/storage/fs/tests.rs:82`)
   - `test_detect_manifest_media_type_malformed_json_is_corrupt_data` (`src/storage/fs/tests.rs:1448`)
   - Integration test targets:
     - `tests/manifest_lifecycle_tests.rs`
     - `tests/application_read_tests.rs`
     - `tests/ports_wiring_tests.rs`
   *(Live S3/MinIO integration tests such as `tests/s3_live_integration.rs` are explicitly excluded from filesystem test runs).*

### 9.3 Verified Cargo Test & Quality Commands
*(Note: These commands are proposed for execution during the future implementation slice; Cargo must NOT be run during this documentation-only slice.)*

```bash
# 1. Formatting and Static Analysis
cargo fmt --check
cargo check --locked --all-targets --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings

# 2. Promoted Manifest Module Tests
cargo test --locked --lib storage::fs::manifest::tests

# 3. Production Manifest Read Tests in tests.rs
cargo test --locked --lib test_manifest_read_

# 4. Ignored Permission Tests (requires unprivileged environment)
cargo test --locked --lib test_manifest_read_permission_denied_ignored -- --ignored
cargo test --locked --lib test_real_fs_permission_denied_ignored -- --ignored

# 5. Manifest Write and Media-Type Regression Tests
cargo test --locked --lib delete_manifest_fails_safe_on_malformed_manifest
cargo test --locked --lib test_referrers_tracked_on_manifest_put_and_delete
cargo test --locked --lib test_detect_manifest_media_type_malformed_json_is_corrupt_data

# 6. Relevant Integration Targets
cargo test --locked --test manifest_lifecycle_tests
cargo test --locked --test application_read_tests
cargo test --locked --test ports_wiring_tests
```

### 9.4 Permission Test Guard Requirements
Permission tests requiring restricted file modes (`chmod 0o000`) must strictly observe:
1. Marked `#[ignore = "requires unprivileged user environment where chmod 0o000 denies filesystem access"]`.
2. Fail-fast assertion: If run under an environment where `chmod 0o000` is ineffective (e.g. root UID 0), the test must `panic!("ineffective permissions: ...")` rather than silently returning early. Early returns must never be counted as passing assertions.
3. RAII cleanup: Permissions must be restored via a `Drop` guard (`ScopedPermReset`) to ensure clean test fixture teardown even on panic.

---

## 10. Section 8: Rollback Plan & Approval Decisions

### 10.1 Code-Only Rollback Plan
If an operational regression is detected after production cutover:
1. **Revert Single Commit:** Reverting the cutover commit restores `src/storage/fs.rs` to legacy `tokio::fs::read` operations and restores `manifest_seam.rs`.
2. **Zero Schema or Disk Layout Changes:** Contained manifest reads do not alter file format, on-disk directory layout, database schema, or cache state. Manifest files on disk remain standard JSON files stored at `repos/<name>/manifests/<digest.hex()>`.
3. **No Rollback of In-Flight Mutations:** Rollback is code-only. It does not retroactively undo concurrent writes, deletions, or tag mutations performed while the cutover code was running.

### 10.2 Concrete Production Behavior Changes Requiring Acceptance
The following concrete behavioral differences between legacy `FsStorage` and the contained implementation require explicit stakeholder acceptance prior to cutover:

1. **Rejection of Unsafe Repository Inputs:** Direct storage callers passing repository names with `..`, `.`, leading/trailing slashes, backslashes, NUL, or control characters will fail immediately with `StorageError::InvalidRepoName` instead of attempting uncontained filesystem resolution.
2. **Rejection of Symlinks:** Manifest files or ancestor directories that are symlinks (whether pointing outside or inside the storage root, and whether target exists or is dangling) will fail closed with `StorageErrorKind::Io` (`ResolutionRejected`) instead of being followed or returning `NotFound`.
3. **Rejection of Non-Regular Objects:** A directory, FIFO, socket, or device substituted for a manifest will fail with `StorageErrorKind::Io` (`UnsupportedObjectType`) during Phase 1 `fstat` validation.
4. **Typed Runtime Failure Categorization:** Failures originating from the runtime task scheduler (`RuntimeMissing`, `TaskJoinFailed`) will surface as `StorageErrorKind::Backend` rather than being masked as filesystem `Io`.
5. **Read Containment vs Pathname Mutation Coherence:** Read operations will resolve beneath the pinned root descriptor, while mutations (`put_manifest`, `delete_manifest`) continue to use pathname operations until write containment is extracted under Gate O-04.

---

## 11. Section 9: Canonical Quality Gate Status

All canonical quality gates remain explicitly **OPEN**:

- **O-03: Key and continuation-token contracts** — **OPEN**. Manifest key construction rules and safety boundaries are documented; continuation token contracts remain uncertified.
- **O-04: Filesystem write durability and containment** — **OPEN**. Manifest writes (`put_manifest`), deletions (`delete_manifest`), and tempfile containment remain unextracted and continue using pathname operations.
- **O-05: Broader filesystem read containment** — **OPEN**. CAS blob reads and listing are cut over; manifest reads are fully characterized, validated in a seam, and designed for production cutover here; cutover implementation has not yet occurred.
- **O-06: Typed AWS mapping and pinned-MinIO evidence** — **OPEN**. S3/MinIO backend verification is independent of filesystem extraction.
- **O-13: Hosting, distribution, and release strategy** — **OPEN**. Distribution boundaries remain uncertified.
- **O-15: Non-Linux verification** — **OPEN**. Contained resolution relies on Linux `openat2`; non-Linux platforms remain explicitly unverified.
- **O-16: Earlier Slice 11 audit/test-inventory evidence** — **OPEN**. Audit trails preserved.
- **D-06: Broader extraction, cutover, compatibility, and distribution acceptance** — **OPEN**. Overall milestone acceptance pending.

---

**END OF DESIGN DOCUMENT**
