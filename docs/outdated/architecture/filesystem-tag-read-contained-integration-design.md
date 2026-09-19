> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Contained tag reads landed (`5a0b424`); tags later moved onto `tag_domain` (`32c42c6`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Architecture Design: Contained Filesystem Tag Read Test Seam

- **Document:** `docs/architecture/filesystem-tag-read-contained-integration-design.md`
- **Status:** Test-Only Seam Design (Final Revision) — Under Review — Production Routing Unchanged — Not Staged — Not Committed
- **Canonical Quality Gates:** `O-03`, `O-04`, `O-05`, `O-06`, `O-13`, `O-15`, `O-16`, and `D-06` remain explicitly **OPEN**
- **Registry-Rust Baseline:** `7d6649d45e855aaefa4335782515e121d69afa26`
- **Storage-Layer-Rust Baseline:** `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`

---

## 1. Executive Summary & Design Scope

Following the empirical baseline recorded in [`docs/architecture/filesystem-tag-read-characterization.md`](file:///home/dietmar/devel/rust/registry-rust/docs/architecture/filesystem-tag-read-characterization.md) and the containment assessment in [`docs/architecture/filesystem-read-containment-remaining-gaps.md`](file:///home/dietmar/devel/rust/registry-rust/docs/architecture/filesystem-read-containment-remaining-gaps.md), this document specifies the architecture for a **test-only integration seam** evaluating descriptor-relative contained reads for filesystem tags in `registry-rust`.

### 1.1 Scope Demarcation & Module Boundary
- **Authorized Deliverable:** Architecture design document [`docs/architecture/filesystem-tag-read-contained-integration-design.md`](file:///home/dietmar/devel/rust/registry-rust/docs/architecture/filesystem-tag-read-contained-integration-design.md) and supporting evidence package.
- **Proposed Module Scope:** The seam will reside strictly behind `#[cfg(test)]` in a dedicated test module:
  ```rust
  #[cfg(test)]
  #[path = "fs/tag_seam.rs"]
  mod tag_seam;
  ```
- **Production Code Status:** **Strictly Unchanged**. Production routing in [`FsStorage::resolve_tag`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L939-L951) and [`FsStorage::get_tag_with_version`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1250-L1269) remains 100% on legacy uncontained ambient path operations. No public interfaces, runtime configurations, dependencies, or mutation implementations are modified.
- **`storage-layer-rust` Status:** **Strictly Read-Only**. No crates or files in `storage-layer-rust` are edited.
- **Git Actions:** No staging, no commits, and no pushes.
- **Test Execution:** No Cargo execution is performed for this documentation-only task. Prior characterization test executions (10 passed, 1 permission test ignored) remain prior recorded evidence.

### 1.2 Architectural Problem Statement
In current production code, **both tag reads and tag mutations** execute via ambient pathname operations:
1. Direct tag resolution ([`FsStorage::resolve_tag`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L939-L951)) calls `tokio::fs::read_to_string(&path)`.
2. Version-aware retrieval ([`FsStorage::get_tag_with_version`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1250-L1269)) calls `tokio::fs::read(&path)`.
3. Tag mutation ([`FsStorage::mutate_tag`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1030-L1140)) locks `.lock.{tag}` and writes through an atomic temporary file rename.
4. Conditional tag deletion ([`FsStorage::delete_tag_conditional`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1271-L1335)) locks `.lock.{tag}` and unlinks the tag via ambient `std::fs` pathnames.

All operations construct paths dynamically using `self.root.join("repos").join(name).join("tags").join(tag)`. Consequently:
- **Symlink Traversal:** Sibling and external symlinks are followed transparently, allowing file reads and potential manipulation outside the repository storage root.
- **Path Traversal:** Unvalidated path segments (`..`) allow callers to escape the `repos/` and `tags/` directories.
- **Pathname Resolution Reality:** Both tag reads and tag mutations currently resolve pathnames, but separate operations can still observe different trees when replacement occurs between them. Shared pathname resolution is not a coherence guarantee.
- **Divergence Against Contained Readers:** Furthermore, `FsStorage` already shares a pinned directory descriptor ([`storage_fs::FsMetadataReader`](file:///home/dietmar/devel/rust/storage-layer-rust/crates/storage-fs/src/reader.rs#L458)) for contained CAS blob reads and GC discovery. If `self.root` is replaced, contained operations continue to observe the original pinned tree while uncontained operations observe the replacement tree.

This design defines a test-only seam leveraging the existing [`storage_core::ObjectPayloadReader`](file:///home/dietmar/devel/rust/storage-layer-rust/crates/storage-core/src/read.rs#L150-L158) implementation in [`storage_fs::FsMetadataReader`](file:///home/dietmar/devel/rust/storage-layer-rust/crates/storage-fs/src/reader/payload.rs#L109-L272) to evaluate descriptor-relative containment beneath the pinned root without altering production behavior.

---

## 2. Existing Primitives & Shared Reader Inventory

The proposed test seam builds entirely upon existing primitives from `storage-core` and `storage-fs` without inventing new abstractions or altering crate boundaries.

### 2.1 Domain-Neutral Contracts (`storage-core`)

The read port abstractions reside in [`crates/storage-core/src/read.rs`](file:///home/dietmar/devel/rust/storage-layer-rust/crates/storage-core/src/read.rs):

```rust
// [crates/storage-core/src/read.rs:150-158]
#[async_trait]
pub trait ObjectPayloadReader: Send + Sync {
    async fn open_payload(&self, key: &ObjectKey) -> Result<ObjectPayload, ReadError>;
}

// [crates/storage-core/src/read.rs:74]
pub type ObjectStream = Pin<Box<dyn AsyncRead + Send + 'static>>;

// [crates/storage-core/src/read.rs:94-116]
pub struct ObjectPayload {
    metadata: ObjectMetadata,
    stream: ObjectStream,
}

impl ObjectPayload {
    pub fn metadata(&self) -> &ObjectMetadata { &self.metadata }
    pub fn into_parts(self) -> (ObjectMetadata, ObjectStream) {
        (self.metadata, self.stream)
    }
}
```

- **`ObjectKey` ([`crates/storage-core/src/key.rs`](file:///home/dietmar/devel/rust/storage-layer-rust/crates/storage-core/src/key.rs)):** Enforces relative path safety (rejects leading/trailing slashes, `..`, empty segments, control characters).
- **`ReadError` ([`crates/storage-core/src/error.rs`](file:///home/dietmar/devel/rust/storage-layer-rust/crates/storage-core/src/error.rs)):** Domain-neutral error enum:
  - `ReadError::NotFound { key, message }`
  - `ReadError::PermissionDenied { key, message, source }`
  - `ReadError::Backend { key, message, source }`

### 2.2 Filesystem Containment Implementation (`storage-fs`)

`FsMetadataReader` implements `ObjectPayloadReader` in [`crates/storage-fs/src/reader.rs:458-510`](file:///home/dietmar/devel/rust/storage-layer-rust/crates/storage-fs/src/reader.rs#L458-L510) and [`crates/storage-fs/src/reader/payload.rs:109-272`](file:///home/dietmar/devel/rust/storage-layer-rust/crates/storage-fs/src/reader/payload.rs#L109-L272):

1. **Phase 1 (Contained Resolution):** Resolves the relative key against the pinned directory file descriptor (`root_fd`) using Linux `openat2` with `O_PATH | O_CLOEXEC` and containment flags:
   ```c
   RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS
   ```
   Inspects the resulting descriptor with `fstat` and rejects non-regular files (`S_IFREG` required; directories, FIFOs, and special files fail).
2. **Phase 2 (Readable Reopening):** Reopens the file via `/proc/self/fd/{phase1_fd}` with `O_RDONLY | O_CLOEXEC`. Re-verifies identity using `fstat` (`st_dev` and `st_ino` must match Phase 1 exactly).
3. **Execution Model:** Offloaded via `tokio::task::spawn_blocking` to avoid blocking runtime executor threads. Returns `ObjectPayload` containing verified `ObjectMetadata` and a boxed `tokio::fs::File` as `ObjectStream`.

### 2.3 Shared Reader Ownership in `FsStorage`

In [`src/storage/fs.rs:190-230`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L190-L230), `FsStorage` already owns an initialized, capability-probed `Arc<storage_fs::FsMetadataReader>`:

```rust
// [src/storage/fs.rs:190-198]
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

The reader is opened during startup and verified for `openat2` support:
```rust
// [src/storage/fs.rs:203-208]
let reader = storage_fs::FsMetadataReader::open(&root).map_err(read_adapter::map_fs_startup_error)?;
reader.probe_capability().map_err(read_adapter::map_fs_startup_error)?;
let reader = std::sync::Arc::new(reader);
```

#### Runtime Capability Probing vs Kernel Version
The availability of descriptor-relative containment is determined by **runtime capability probing** (`reader.probe_capability()`), not merely kernel version. A kernel version $\ge 5.6$ is necessary but not sufficient: container namespaces, seccomp filters, or Linux Security Modules (LSMs) can block `openat2` system calls even on modern kernels.

The proposed tag-read seam directly accepts `reader: &(impl ObjectPayloadReader + ?Sized)`, allowing tests to exercise either deterministic mock readers ([`RecordingFakePayloadReader`]) or the actual shared reader `self.reader` without allocating new descriptors.

---

## 3. Compatibility-Preserving Parsing Contracts

The empirical characterization revealed significant semantic divergence between `resolve_tag` and `get_tag_with_version` regarding missing files, malformed text, and invalid UTF-8. The proposed test seam preserves these distinct contracts exactly.

### 3.1 Contract for `resolve_tag_seam`

Legacy `resolve_tag` uses `tokio::fs::read_to_string`, trims whitespace, and calls `Digest::parse(reference).map_err(|_| StorageError::NotFound)`.

```rust
pub(crate) async fn resolve_tag_seam(
    reader: &(impl ObjectPayloadReader + ?Sized),
    repo: &str,
    tag: &str,
    limits: &TagReadLimits,
) -> Result<Digest, StorageError> {
    let key = tag_key(repo, tag)?;

    let payload = match reader.open_payload(&key).await {
        Ok(p) => p,
        Err(storage_core::ReadError::NotFound { .. }) => return Err(StorageError::NotFound),
        Err(other) => return Err(super::read_adapter::translate_payload_read_error(other)),
    };

    let bytes = drain_tag_stream(payload, limits).await?;

    // UTF-8 validation: legacy read_to_string fails on invalid UTF-8 with std::io::ErrorKind::InvalidData
    let s = std::str::from_utf8(&bytes).map_err(|e| {
        StorageError::io(format!("invalid utf-8 sequence in tag file {key}: {e}"))
    })?;

    // Parse digest: legacy maps ALL parse errors (empty string, invalid characters, invalid length) to NotFound
    let reference = s.trim();
    Digest::parse(reference).map_err(|_| StorageError::NotFound)
}
```

#### Key Contract Invariants:
1. **Missing File:** Returns `StorageError::NotFound`.
2. **Invalid UTF-8:** Returns `StorageError::io(...)` (matching `read_to_string` I/O error translation).
3. **Empty File / Malformed Digest Text:** Returns `StorageError::NotFound` (matching `Digest::parse(...).map_err(|_| StorageError::NotFound)`).
4. **Whitespace Trimming:** Trims spaces, tabs, `\r`, `\n`, and leading/trailing padding.
5. **Stream I/O Failures:** Stream read errors during draining return `StorageError::io(...)`.
6. **Limit Rejection:** Exceeding an explicit payload limit returns `StorageError::corrupt_data(...)` (proposed seam decision).

### 3.2 Contract for `get_tag_with_version_seam`

Legacy `get_tag_with_version` reads raw bytes, uses `String::from_utf8_lossy(&bytes)`, parses the digest mapping parse errors to `StorageError::corrupt_data`, and computes the SHA-256 hash over the **raw bytes**.

```rust
pub(crate) async fn get_tag_with_version_seam(
    reader: &(impl ObjectPayloadReader + ?Sized),
    repo: &str,
    tag: &str,
    limits: &TagReadLimits,
) -> Result<Option<(Digest, String)>, StorageError> {
    let key = tag_key(repo, tag)?;

    let payload = match reader.open_payload(&key).await {
        Ok(p) => p,
        Err(storage_core::ReadError::NotFound { .. }) => return Ok(None),
        Err(other) => return Err(super::read_adapter::translate_payload_read_error(other)),
    };

    let bytes = drain_tag_stream(payload, limits).await?;

    // Lossy UTF-8 conversion followed by trim and parse
    let s = String::from_utf8_lossy(&bytes);
    let digest = Digest::parse(s.trim())
        .map_err(|e| StorageError::corrupt_data(format!("corrupt tag {tag}: {e}")))?;

    // Raw-byte version hashing: NEVER hash normalized or trimmed text
    let mut hasher = sha2::Sha256::new();
    hasher.update(&bytes);
    let version = hex::encode(hasher.finalize());

    Ok(Some((digest, version)))
}
```

#### Key Contract Invariants:
1. **Missing File:** Returns `Ok(None)`.
2. **Empty File / Malformed Digest Text:** Returns `StorageError::corrupt_data(...)`.
3. **Invalid UTF-8:** Lossy decoding substitutes `\u{FFFD}`, which causes `Digest::parse` to fail and return `StorageError::corrupt_data(...)`.
4. **Raw-Byte Version Preservation:** Version computation is performed strictly over `&bytes`. Two tag files with identical logical digests but differing whitespace or line endings (e.g. `sha256:...` vs `sha256:...\n`) yield distinct versions.
5. **No Snapshot Isolation Guarantee:** Version hashing merely describes the exact bytes that were read through the stream; it does not prove or guarantee an atomic snapshot against concurrent mutations.

---

## 4. Object Key Construction & Grammar Separation

### 4.1 Storage Layout & Key Definition
In standard repository layout, tag files reside at:
```
repos/<repository>/tags/<tag>
```
The corresponding `ObjectKey` is:
```rust
repos/{repo}/tags/{tag}
```

### 4.2 Structural Validation vs Stricter Grammar

A critical requirement of this design is separating **structural path containment safety** from **application-level repository and tag grammar**:

```
+-----------------------------------------------------------------------------+
| Structural Path Safety (ObjectKey Invariants - Enforced by Seam)           |
| - Non-empty repo and tag strings                                            |
| - Rejection of leading / trailing slashes ('/repo' or 'repo/')              |
| - Rejection of backslashes ('\')                                            |
| - Rejection of NUL bytes ('\0') and ASCII control characters                |
| - Rejection of empty segments ('repos//tags')                               |
| - Rejection of path traversal segments ('.' and '..')                       |
+--------------------------------------+--------------------------------------+
                                       |
                                       v
+-----------------------------------------------------------------------------+
| Application-Level OCI Grammar (DEFERRED - NOT Approved by this Design)      |
| - CanonicalRepoName (src/registry/canonical_name.rs):                       |
|   lowercase ASCII alphanumerics, segment length bounds, strict separator     |
|   rules (single '.', up to two '__', one or more '-', no mixed separators)   |
| - Tag Name Grammar: ^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ (no slashes)        |
+-----------------------------------------------------------------------------+
```

### 4.3 Proposed Key Construction Helper

```rust
pub(crate) fn tag_key(repo: &str, tag: &str) -> Result<ObjectKey, StorageError> {
    // 1. Validate repository component for structural path safety
    validate_path_component(repo, "repository name")?;

    // 2. Validate tag component for structural path safety
    validate_path_component(tag, "tag name")?;

    // 3. Compose relative object key
    let key_str = format!("repos/{repo}/tags/{tag}");
    ObjectKey::parse(&key_str).map_err(|e| StorageError::InvalidRepoName(e.to_string()))
}

fn validate_path_component(component: &str, field_name: &str) -> Result<(), StorageError> {
    if component.is_empty() {
        return Err(StorageError::InvalidRepoName(format!(
            "{field_name} cannot be empty"
        )));
    }
    if component.starts_with('/') || component.ends_with('/') {
        return Err(StorageError::InvalidRepoName(format!(
            "{field_name} cannot have leading or trailing slashes"
        )));
    }
    if component.contains('\\') {
        return Err(StorageError::InvalidRepoName(format!(
            "{field_name} cannot contain backslashes"
        )));
    }
    if component.contains(|c: char| c == '\0' || c.is_ascii_control()) {
        return Err(StorageError::InvalidRepoName(format!(
            "{field_name} cannot contain NUL bytes or control characters"
        )));
    }

    for segment in component.split('/') {
        if segment.is_empty() {
            return Err(StorageError::InvalidRepoName(format!(
                "{field_name} cannot contain empty segments (repeated slashes)"
            )));
        }
        if segment == "." {
            return Err(StorageError::InvalidRepoName(format!(
                "{field_name} cannot contain '.' segments"
            )));
        }
        if segment == ".." {
            return Err(StorageError::InvalidRepoName(format!(
                "{field_name} cannot contain '..' segments (path traversal attempt)"
            )));
        }
    }
    Ok(())
}
```

### 4.4 Explicit Accounting of Proposed Behavior Changes

| Input Condition | Current Ambient Behavior | Proposed Seam Behavior | Status & Authority |
| :--- | :--- | :--- | :--- |
| `repo = "../sibling"` | Traverses out of `repos/` via ambient pathname | Rejected with `StorageError::InvalidRepoName` | **Proposed Seam Decision** — Not approved for production |
| `tag = "../outside.txt"` | Traverses out of `tags/` via ambient pathname | Rejected with `StorageError::InvalidRepoName` | **Proposed Seam Decision** — Not approved for production |
| `repo = "/absolute/path"` | Discards `self.root` via `PathBuf::join` | Rejected with `StorageError::InvalidRepoName` | **Proposed Seam Decision** — Not approved for production |
| `tag = "nested/tag"` | Creates/reads `tags/nested/tag` | Permitted by structural validation (if valid segments) | Preserved in seam; OCI slash rejection **deferred** |
| `repo = "UPPERCASE"` | Resolves on case-sensitive / case-insensitive FS | Permitted by structural validation | Preserved in seam; lowercase requirement **deferred** |

Mapping invalid tag components or traversal attempts to `StorageError::InvalidRepoName` is an explicit **proposed seam decision** (current production code has no `InvalidTagName` variant).

---

## 5. Comprehensive Error-Mapping Matrix

The table below contrasts current production behavior with the proposed test seam across all failure modes:

| Scenario / Error Condition | Underlying Cause / Event | `storage-core` / `storage-fs` Error | Legacy `resolve_tag` | Proposed `resolve_tag_seam` | Legacy `get_tag_with_version` | Proposed `get_tag_with_version_seam` | Architectural Justification |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Missing Tag File** | File does not exist | `ReadError::NotFound` | `Err(NotFound)` | `Err(NotFound)` | `Ok(None)` | `Ok(None)` | Preserves exact missing-value semantics for both callers. |
| **Missing Repo Directory** | Parent directory missing | `ReadError::NotFound` | `Err(NotFound)` | `Err(NotFound)` | `Ok(None)` | `Ok(None)` | Missing directory handled identically to missing file. |
| **Dangling Symlink** | Symlink target missing | `FsMetadataError::ResolutionRejected` | `Err(NotFound)` | `Err(StorageError::io)` | `Ok(None)` | `Err(StorageError::io)` | In contained mode, `RESOLVE_NO_SYMLINKS` rejects symlinks before target resolution; fails with `ELOOP`. |
| **Final File Symlink** | Tag is a symlink | `FsMetadataError::ResolutionRejected` | Follows symlink (`Ok`) | `Err(StorageError::io)` | Follows symlink (`Ok`) | `Err(StorageError::io)` | Contained reads strictly forbid symlinks (`RESOLVE_NO_SYMLINKS`). |
| **Ancestor Symlink** | `repos/` or `tags/` is a symlink | `FsMetadataError::ResolutionRejected` | Follows symlink (`Ok`) | `Err(StorageError::io)` | Follows symlink (`Ok`) | `Err(StorageError::io)` | Prevents escaping root via directory symlink swaps. |
| **Non-Regular Object** | Directory at tag path | `FsMetadataError::UnsupportedObjectType` | `Err(StorageErrorKind::Io)` (`EISDIR`) | `Err(StorageErrorKind::Io)` | `Err(StorageErrorKind::Io)` (`EISDIR`) | `Err(StorageErrorKind::Io)` | Phase 1 checks `S_IFREG`; non-regular files reject as I/O error. |
| **Permission Denied** | Mode `0o000` / `EACCES` | `ReadError::PermissionDenied` | `Err(StorageErrorKind::Io)` | `Err(StorageErrorKind::Io)` | `Err(StorageErrorKind::Io)` | `Err(StorageErrorKind::Io)` | OS access denial propagates as typed I/O error. |
| **Empty File (0 Bytes)** | Tag file has 0 bytes | Read succeeds; 0 bytes | `Err(NotFound)` | `Err(NotFound)` | `Err(CorruptData)` | `Err(CorruptData)` | `Digest::parse("")` fails; preserves divergent error mapping. |
| **Malformed Digest Text** | Non-hex, bad length | Read succeeds; text fails parse | `Err(NotFound)` | `Err(NotFound)` | `Err(CorruptData)` | `Err(CorruptData)` | `resolve_tag` maps parse failure to `NotFound`; `get_tag` to `CorruptData`. |
| **Invalid UTF-8 Bytes** | Non-UTF-8 sequence (`0xFF`) | Read succeeds; bytes invalid | `Err(StorageErrorKind::Io)` | `Err(StorageErrorKind::Io)` | `Err(CorruptData)` | `Err(CorruptData)` | `resolve_tag` checks `from_utf8`; `get_tag` uses lossy decode then fails parse. |
| **Payload Exceeds Limit** | Stream > `max_payload_bytes` | Bounded draining detects limit breach | N/A (unbounded) | `Err(CorruptData)` | N/A (unbounded) | `Err(CorruptData)` | Proposed seam decision: rejects oversized tags as corrupt data. |
| **Stream I/O Error** | Read failure during stream drain | `std::io::Error` during stream polling | `Err(StorageErrorKind::Io)` | `Err(StorageErrorKind::Io)` | `Err(StorageErrorKind::Io)` | `Err(StorageErrorKind::Io)` | Failures during active stream consumption map to `StorageError::io`. |
| **Procfs Reopen Failure** | `/proc/self/fd` unavailable | `FsMetadataError::ProcfsReopenFailed` | N/A (ambient) | `Err(StorageErrorKind::Io)` | N/A (ambient) | `Err(StorageErrorKind::Io)` | Phase 2 reopening mechanism failure classified as backend I/O error. |
| **Stat Failure** | `fstat` fails in Phase 1 or 2 | `FsMetadataError::StatFailed` | N/A (ambient) | `Err(StorageErrorKind::Io)` | N/A (ambient) | `Err(StorageErrorKind::Io)` | Descriptor stat error mapped to `StorageError::io`. |
| **Identity Mismatch** | Target swapped between phases | `FsMetadataError::IdentityMismatch` | N/A (ambient) | `Err(StorageErrorKind::Io)` | N/A (ambient) | `Err(StorageErrorKind::Io)` | Detects race replacement during Phase 2 acquisition. |
| **Invalid Metadata** | Negative file size | `FsMetadataError::InvalidMetadata` | N/A (ambient) | `Err(StorageErrorKind::Io)` | N/A (ambient) | `Err(StorageErrorKind::Io)` | Invariant violation in metadata mapped to `StorageError::io`. |
| **Kernel Unsupported** | `openat2` returns `ENOSYS` | `FsMetadataError::SyscallUnsupported` | N/A (ambient) | `Err(Configuration)` | N/A (ambient) | `Err(Configuration)` | Maps syscall absence to configuration error via `read_adapter`. |
| **Platform Unsupported** | Non-Linux OS | `FsMetadataError::PlatformUnsupported` | N/A (ambient) | `Err(StorageErrorKind::Io)` | N/A (ambient) | `Err(StorageErrorKind::Io)` | Non-Linux fallback remains unapproved (`O-15` OPEN). |
| **Runtime Missing** | No active Tokio runtime | `FsMetadataError::RuntimeMissing` | N/A | `Err(Backend)` | N/A | `Err(Backend)` | Offload runtime absence mapped to `StorageErrorKind::Backend`. |
| **Task Join Error** | Tokio worker panicked | `FsMetadataError::TaskJoinFailed` | N/A | `Err(Backend)` | N/A | `Err(Backend)` | Offload worker failure mapped to `StorageErrorKind::Backend`. |

### 5.1 Generic Backend Source Translation Pipeline
In `read_adapter.rs`, translation of `storage_core::ReadError::Backend { message, source }` proceeds through strongly typed downcasting:
1. If `source` downcasts to `storage_fs::FsMetadataError`:
   - `ResolutionRejected`, `UnsupportedObjectType`, `StatFailed`, `ProcfsReopenFailed`, `IdentityMismatch`, `InvalidMetadata`, `PlatformUnsupported` $\rightarrow$ `StorageError::io(...)`.
   - `SyscallUnsupported` $\rightarrow$ `StorageError::configuration(...)`.
   - `RuntimeMissing`, `TaskJoinFailed` $\rightarrow$ `StorageError::backend(...)`.
2. Else if `source` downcasts to `std::io::Error` $\rightarrow$ `StorageError::io(io_err.to_string())`.
3. Else $\rightarrow$ `StorageError::io(source.to_string())` (or `message` if `source` is `None`).

**Important Testing Rule:** Fake readers in unit tests return `ReadError`. To simulate filesystem errors, tests must wrap `FsMetadataError` inside `ReadError::backend_with_source(msg, Box::new(fs_err))`, rather than returning a raw error struct.

---

## 6. Payload Limits & Precise Bounded Stream Draining

### 6.1 Digest Sizes & Real-World Whitespace
- **SHA-256 Digest:** 71 bytes (`sha256:` prefix + 64 lowercase hex characters).
- **SHA-512 Digest:** 135 bytes (`sha512:` prefix + 128 lowercase hex characters).
- **Padding:** Characterization test `test_tag_read_whitespace_tabs_crlf_and_substantial_padding` demonstrated that tag files with >256 bytes of spaces before and after the digest are accepted by production code.

### 6.2 Removing Hidden Ceilings
- **No Hidden Default Limit:** `TagReadLimits::max_payload_bytes = None` explicitly means **no seam-imposed ceiling**, preserving the existing unbounded behavior of production `FsStorage`.
- **Default Policy:** `TagReadLimits::default()` sets `max_payload_bytes = None`.
- **Bounded Testing:** `Some(limit)` is used strictly for explicit test cases verifying boundary enforcement.
- **Memory Risk of `None`:** Reading unbounded streams into memory creates a denial-of-service vector if an attacker writes or links a large file into `tags/`. However, no production default limit is approved by this design; introducing a default limit requires auditing real-world deployments.

```rust
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) struct TagReadLimits {
    /// Maximum bytes to read from a tag payload stream.
    /// If None, reads without a seam-imposed ceiling (preserving existing unbounded behavior).
    /// If Some(limit), enforces an exact ceiling against stream bytes.
    pub max_payload_bytes: Option<u64>,
}
```

### 6.3 Precise Bounded Draining Specification

A bounded drain consumes **at most $N + 1$ bytes for overflow detection and accepts at most $N$ bytes**.

```rust
async fn drain_tag_stream(
    payload: storage_core::ObjectPayload,
    limits: &TagReadLimits,
) -> Result<Vec<u8>, StorageError> {
    let (_metadata, stream) = payload.into_parts();

    match limits.max_payload_bytes {
        None => {
            // Unbounded draining: preserves current production behavior
            let mut buffer = Vec::new();
            let mut pinned_stream = stream;
            pinned_stream.read_to_end(&mut buffer).await.map_err(|e| {
                StorageError::io(format!("failed to read tag payload stream: {e}"))
            })?;
            Ok(buffer)
        }
        Some(limit) => {
            // Safe checked increment to detect overflow at u64::MAX
            let take_limit = limit.checked_add(1).ok_or_else(|| {
                StorageError::corrupt_data(format!(
                    "tag payload limit {limit} cannot be represented for bounded draining"
                ))
            })?;

            // Drain at most limit + 1 bytes for overflow detection
            let mut limited_stream = stream.take(take_limit);
            let mut buffer = Vec::new();
            limited_stream.read_to_end(&mut buffer).await.map_err(|e| {
                StorageError::io(format!("failed to read tag payload stream: {e}"))
            })?;

            // Accepts at most limit bytes
            if buffer.len() as u64 > limit {
                return Err(StorageError::corrupt_data(format!(
                    "tag payload stream length exceeds limit of {limit} bytes"
                )));
            }

            Ok(buffer)
        }
    }
}
```

### 6.4 Handling Metadata vs Stream Bytes
- **Metadata as Acquisition Observation:** `ObjectMetadata.size()` is recorded at open time. It is not an authoritative guarantee of stream length under concurrent modifications.
- **No Early Metadata Rejection:** The seam does **not** reject early based on metadata size. Rejecting early would falsely fail a file whose metadata was large at open time but which was truncated to a small size before stream reading commenced. All limits are enforced strictly against actual consumed stream bytes.
- **Zero-Limit Behavior (`Some(0)`):**
  - `take_limit = 0 + 1 = 1`.
  - An empty file produces 0 bytes: `buffer.len() == 0 <= 0` $\rightarrow$ `drain_tag_stream` accepts and returns empty bytes (`[]`).
  - Subsequent caller parsing: `resolve_tag_seam` fails `Digest::parse("")` returning `StorageError::NotFound`; `get_tag_with_version_seam` fails `Digest::parse("")` returning `StorageError::corrupt_data`.
  - A file with $\ge 1$ byte produces 1 byte: `buffer.len() == 1 > 0` $\rightarrow$ rejected by `drain_tag_stream` with `StorageError::corrupt_data`.
- **Memory Distinctions:**
  - **Payload Limit ($N$ bytes):** Bounds the maximum application bytes read from the object stream.
  - **`Vec<u8>` Capacity:** Memory allocated by `read_to_end` grows dynamically, potentially exceeding $N$ bytes before settling.
  - **Stream Buffer Overhead:** Underlying asynchronous file readers and executors allocate internal stream buffers.
  - **Concurrent Footprint:** In a multi-task runtime, peak memory is an aggregate over concurrent tasks. A per-object limit bounds individual streams, not total server memory.

---

## 7. Read/Mutation Coherence & Concurrency Analysis

A critical architectural distinction must be maintained between current production behavior and the proposed test seam.

```
                    CURRENT PRODUCTION ARCHITECTURE
                    
                         FsStorage (self.root)
                                  |
                   +--------------+--------------+
                   |                             |
                   v                             v
            [Tag Reads]                   [Tag Mutations]
        (resolve_tag, get_tag)        (mutate_tag, delete_tag)
                   |                             |
                   v                             v
           tokio::fs::read*                std::fs::*
           self.root.join(...)            self.root.join(...)
                   |                             |
                   +--------------+--------------+
                                  |
                                  v
                  Dynamic Path Resolution Under self.root
      (Both resolve pathnames; replacement between calls can observe different trees)
```

```
                   PROPOSED TEST SEAM ARCHITECTURE
                    
                         FsStorage (self.root)
                                  |
                   +--------------+--------------+
                   |                             |
                   v                             v
         [Contained Read Seam]            [Tag Mutations]
        (resolve_tag_seam, etc.)      (mutate_tag, delete_tag)
                   |                             |
                   v                             v
           openat2(root_fd)               std::fs::*
         Pinned Root Inode               self.root.join(...)
                   |                             |
                   +--------------+--------------+
                                  |
                                  v
                  NEW Asymmetric Split-Brain Risk:
          Reads observe OLD root; mutations observe NEW root!
```

### 7.1 What Advisory Locks and Version Tokens Do and Do Not Guarantee
- **Advisory Locks (`.lock.{tag}`):**
  - Acquired exclusively via `fs2::FileExt::lock_exclusive` by both [`mutate_tag`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1030-L1140) and [`delete_tag_conditional`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1271-L1335).
  - **Serializes cooperating operations only** when they open and lock the **identical lock-file inode**.
  - **Tag reads do not acquire this lock.** Tag reads remain unlocked. A read can execute concurrently with a mutation or deletion.
  - If `self.root` is replaced, or `.lock.{tag}` is unlinked and recreated, cooperating processes may open different inodes and fail to serialize. Non-cooperating writers ignore the lock entirely.
- **Byte-Based Version Tokens:**
  - Generated via `hex::encode(sha256(raw_bytes))`.
  - **Detects mismatch only when raw bytes differ** at the moment of the conditional delete read.
  - **Does not prove an atomic snapshot:** If a file is concurrently modified during a read, concurrent modifications during stream draining may affect the bytes returned; detection is not guaranteed. If the altered data still parses as a valid digest, the read succeeds with a version reflecting that altered state.
  - **Does not detect identical-byte overwrites:** Replacing a tag file with identical bytes produces the identical version token, masking intervening deletion or recreation.

### 7.2 Clear Architectural Boundary
> [!WARNING]
> A read-only cutover does **NOT** solve mutation containment or root coherence.
> The proposed seam would cause tag reads to observe the pinned `root_fd` while mutations continue to follow dynamic pathnames under `self.root`.

### 7.3 Evaluative Alternatives Before Production Cutover
Before any production cutover can be considered, the following alternatives must be formally evaluated:
1. **Contained Tag Mutations:** Evaluating descriptor-relative tag writing and deletion (`openat2` + `unlinkat`) beneath `root_fd`.
2. **Lock Containment:** Evaluating descriptor-relative opening of `.lock.{tag}` to ensure locks and reads target the same directory inode.
3. **Operational Root-Stability Controls:** Evaluating whether storage roots can be declared immutable during runtime, requiring process restart for directory relocation.

---

## 8. Focused Test Matrix Specification

The proposed test seam defines a comprehensive test suite divided into **fake-reader unit tests** and **Linux-gated real filesystem tests**.

### 8.1 Fake-Reader Unit Tests (`RecordingFakePayloadReader`)

| Test Name | Test Conditions & Inputs | Expected Outcome | Verification Purpose |
| :--- | :--- | :--- | :--- |
| `test_seam_resolve_tag_valid_sha256` | Fake returns `sha256:<64 hex>\n` | `Ok(Digest)` matching hex | Verifies valid SHA-256 parsing in `resolve_tag`. |
| `test_seam_resolve_tag_valid_sha512` | Fake returns `sha512:<128 hex>` | `Ok(Digest)` matching hex | Verifies valid SHA-512 parsing in `resolve_tag`. |
| `test_seam_get_tag_valid_sha512` | Fake returns `sha512:<128 hex>\n` | `Ok(Some((Digest, version)))` | Verifies valid SHA-512 parsing and versioning. |
| `test_seam_resolve_tag_padded_whitespace` | 300 spaces before/after digest | `Ok(Digest)` matching hex | Verifies whitespace stripping. |
| `test_seam_resolve_tag_missing_not_found` | Fake returns `ReadError::NotFound` | `Err(StorageError::NotFound)` | Verifies missing file mapping in `resolve_tag`. |
| `test_seam_resolve_tag_empty_not_found` | Fake returns empty payload (`""`) | `Err(StorageError::NotFound)` | Verifies empty file mapped to `NotFound`. |
| `test_seam_resolve_tag_malformed_not_found`| Fake returns `"not-a-digest"` | `Err(StorageError::NotFound)` | Verifies corrupt text mapped to `NotFound`. |
| `test_seam_resolve_tag_invalid_utf8_io` | Fake returns `b"\xff\xfe\xfd"` | `Err(StorageErrorKind::Io)` | Verifies invalid UTF-8 mapped to `Io`. |
| `test_seam_get_tag_missing_none` | Fake returns `ReadError::NotFound` | `Ok(None)` | Verifies missing tag returns `None`. |
| `test_seam_get_tag_empty_corrupt` | Fake returns empty payload (`""`) | `Err(StorageErrorKind::CorruptData)` | Verifies empty tag mapped to `CorruptData`. |
| `test_seam_get_tag_malformed_corrupt` | Fake returns `"invalid-digest"` | `Err(StorageErrorKind::CorruptData)` | Verifies corrupt text mapped to `CorruptData`.|
| `test_seam_get_tag_invalid_utf8_corrupt` | Fake returns `b"\xff\xfe\xfd"` | `Err(StorageErrorKind::CorruptData)` | Verifies invalid UTF-8 mapped to `CorruptData`. |
| `test_seam_get_tag_raw_byte_version_hash` | Valid digest with `\n` vs without `\n` | Same digest; **different version strings** | Verifies version hashes raw bytes, not trimmed text. |
| `test_seam_unbounded_large_padded_payload` | `limits.max_payload_bytes = None`, payload 70 KiB | `Ok(...)` | Proves `None` preserves unbounded reading for tags > 64 KiB. |
| `test_seam_limit_zero_empty_success` | `limits.max_payload_bytes = Some(0)`, payload empty | Drain accepts `[]`; caller maps to `NotFound`/`CorruptData` | Verifies zero-limit allows empty payload through drain. |
| `test_seam_limit_zero_nonempty_failure` | `limits.max_payload_bytes = Some(0)`, payload 1 byte | `Err(StorageErrorKind::CorruptData)` | Verifies zero-limit rejects non-empty payload in drain. |
| `test_seam_limit_exact_success` | Payload length == `limit` | `Ok(...)` | Boundary test: exact limit succeeds. |
| `test_seam_limit_one_over_failure` | Payload length == `limit + 1` | `Err(StorageErrorKind::CorruptData)` | Boundary test: 1 byte over fails. |
| `test_seam_limit_u64_max_overflow_rejection`| `limits.max_payload_bytes = Some(u64::MAX)` | `Err(StorageErrorKind::CorruptData)` | Verifies `checked_add(1)` overflow rejection. |
| `test_seam_understated_metadata_valid_digest` | Metadata says 10 B, stream produces valid 71 B SHA-256 (limit 100) | `Ok(Digest)` | Proves metadata size is not authoritative for valid parse. |
| `test_seam_drain_understated_metadata_helper` | Metadata says 10 B, stream produces 50 B (limit 100) | `Ok(vec![... 50 bytes])` | Tests drain helper directly without digest parsing. |
| `test_seam_stream_io_failure_partial` | Stream fails with I/O error after partial read | `Err(StorageErrorKind::Io)` | Verifies mid-stream I/O error mapping. |
| `test_seam_key_validation_zero_reader_calls` | Key with `..` or leading `/` | `Err(StorageError::InvalidRepoName)` | Proves structural validation rejects before reader call. |
| `test_seam_error_mapping_permission` | Fake returns `ReadError::PermissionDenied` | `Err(StorageErrorKind::Io)` | Verifies permission error mapping. |
| `test_seam_error_mapping_resolution` | Fake returns `Backend(ResolutionRejected)` | `Err(StorageErrorKind::Io)` | Verifies symlink rejection error mapping. |
| `test_seam_error_mapping_unsupported_type` | Fake returns `Backend(UnsupportedObjectType)` | `Err(StorageErrorKind::Io)` | Verifies non-regular file mapping. |
| `test_seam_error_mapping_syscall` | Fake returns `Backend(SyscallUnsupported)` | `Err(StorageErrorKind::Configuration)` | Verifies `openat2` absence mapped to configuration error. |
| `test_seam_error_mapping_stat` | Fake returns `Backend(StatFailed)` | `Err(StorageErrorKind::Io)` | Verifies stat failure mapped to `Io`. |
| `test_seam_error_mapping_procfs` | Fake returns `Backend(ProcfsReopenFailed)` | `Err(StorageErrorKind::Io)` | Verifies procfs reopen failure mapped to `Io`. |
| `test_seam_error_mapping_identity` | Fake returns `Backend(IdentityMismatch)` | `Err(StorageErrorKind::Io)` | Verifies identity mismatch mapped to `Io`. |
| `test_seam_error_mapping_invalid_meta` | Fake returns `Backend(InvalidMetadata)` | `Err(StorageErrorKind::Io)` | Verifies negative size mapped to `Io`. |
| `test_seam_error_mapping_platform` | Fake returns `Backend(PlatformUnsupported)`| `Err(StorageErrorKind::Io)` | Verifies non-Linux mapped to `Io`. |
| `test_seam_error_mapping_runtime_missing` | Fake returns `Backend(RuntimeMissing)` | `Err(StorageErrorKind::Backend)` | Verifies runtime failure mapped to `Backend`.|
| `test_seam_error_mapping_task_join` | Fake returns `Backend(TaskJoinFailed)` | `Err(StorageErrorKind::Backend)` | Verifies task join failure mapped to `Backend`.|

### 8.2 Real Filesystem Linux-Gated Tests (`storage_fs::FsMetadataReader`)

| Test Name | Fixture Setup | Expected Outcome | Verification Purpose |
| :--- | :--- | :--- | :--- |
| `test_seam_real_shared_reader_identity` | `FsStorage` fixture | Seam reader pointer matches `self.reader` | Proves seam reuses shared reader without re-opening root. |
| `test_seam_real_contained_read_success` | Standard `repos/testrepo/tags/latest` | `Ok(Digest)`, `Ok(Some((Digest, version)))` | Verifies end-to-end descriptor containment. |
| `test_seam_real_final_symlink_rejected` | Tag file is symlink to outside file | `Err(StorageErrorKind::Io)` (`ELOOP`) | Proves `RESOLVE_NO_SYMLINKS` blocks symlink tags. |
| `test_seam_real_ancestor_symlink_rejected` | `repos/testrepo/tags` is symlink to outside dir | `Err(StorageErrorKind::Io)` (`ELOOP`) | Proves intermediate symlink directories blocked. |
| `test_seam_real_non_regular_rejected` | Directory created at `tags/latest` | `Err(StorageErrorKind::Io)` (`UnsupportedObjectType`) | Proves Phase 1 `S_IFREG` check blocks directories. |
| `test_seam_real_path_traversal_rejected` | Tag name `"../outside"` or `"/abs"` | `Err(StorageError::InvalidRepoName)` | Proves structural validation blocks traversal. |
| `test_seam_real_root_replacement_observed`| Rename root directory, create new directory at same path | Seam continues reading **old tree**; legacy reads **new tree** | Proves split-brain divergence between descriptor and pathname. |
| `test_seam_real_permission_denied` | Tag file with mode `0o000` | `Err(StorageErrorKind::Io)` | Tested under unprivileged check or explicitly `#[ignore]`. |

---

## 9. Operating Limitations & Environmental Assumptions

1. **Procfs Accessibility & Stability:** Phase 2 reopening relies on `/proc/self/fd/{phase1_fd}`. In containerized environments with restricted or unmounted procfs, reopening fails (`ProcfsReopenFailed`). This is treated as a backend failure, never falling back to uncontained path resolution.
2. **Capability Probing vs Kernel Version:** The presence of a modern kernel does not guarantee `openat2` availability; container profiles and seccomp filters can disable it. Runtime capability probing is authoritative.
3. **No Atomic Snapshot Isolation:** Neither ambient nor contained reads take a shared lock. Concurrent modifications during stream draining may affect the bytes returned; detection is not guaranteed. Reads do not form an atomic transaction.
4. **No Mount / Hard-Link Boundary Isolation:** `RESOLVE_BENEATH` confines path resolution beneath the directory descriptor, but does not isolate hard links created inside the directory pointing to inodes outside.
5. **Platform Boundary:** Contained tag reads strictly require Linux kernel >= 5.6 supporting `openat2`. Non-Linux platforms return `PlatformUnsupported` (`O-15` remains OPEN). No uncontained pathname fallback is approved.
6. **Logical Limits vs Physical Memory Allocation:** A byte limit bounds the payload data transferred over the stream. It does not bound dynamic `Vec` capacity reallocations, stream reader internal buffers, or aggregate concurrent request memory.

---

## 10. Canonical Quality Gates & Deferred Decisions

### 10.1 Quality Gate Status
All canonical quality gates remain explicitly **OPEN**:
- **`O-03` (Key and continuation-token contracts):** **OPEN**.
- **`O-04` (Filesystem write durability and containment):** **OPEN**.
- **`O-05` (Broader filesystem read containment):** **OPEN**. (The proposed design specifies a test seam only; production routing remains on ambient paths).
- **`O-06` (Typed AWS mapping and pinned-MinIO evidence):** **OPEN**.
- **`O-13` (Hosting, distribution, and release strategy):** **OPEN**.
- **`O-15` (Non-Linux verification):** **OPEN**.
- **`O-16` (Earlier Slice 11 audit/test-inventory evidence):** **OPEN**.
- **`D-06` (Broader extraction, cutover, compatibility, and distribution acceptance):** **OPEN**.

### 10.2 Unresolved Decisions Summary
- **`UD-01` (Tag Name Grammar):** Whether to enforce strict OCI tag grammar (`^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$`) at the storage boundary or retain structural path validation only.
- **`UD-02` (Production Payload Size Limit):** Selecting a production default payload limit after surveying deployed repository data.
- **`UD-03` (Contained Tag Mutation):** Formulating descriptor-relative tag writing and conditional deletion (`openat2` + `unlinkat`) beneath `root_fd`.
- **`UD-04` (Advisory Locking Topology):** Adapting `.lock.{tag}` locking to descriptor-relative containment.
- **`UD-05` (Non-Linux Fallback Strategy):** Determining whether non-Linux platforms receive an uncontained fallback or remain unsupported for filesystem containment (`O-15`).
- **`UD-06` (Error Taxonomy Harmonization):** Deciding whether `resolve_tag` should continue mapping malformed/empty tags to `NotFound` or harmonize with `CorruptData` in a future major version.
