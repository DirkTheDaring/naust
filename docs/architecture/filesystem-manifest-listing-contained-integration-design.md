# Architecture Design: Contained Filesystem Manifest Listing Integration

**Repository:** `registry-rust`
**Target Path:** `docs/architecture/filesystem-manifest-listing-contained-integration-design.md`
**Authoritative Baselines:**
- `registry-rust` HEAD: `f59812f8d4ad49a02066dd673f0be3663eaeb1df`
- `storage-layer-rust` HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`

**Scope:** Source-grounded design for cutting over filesystem manifest listing (`FsStorage::list_manifest_digests_page` and its forwarding via `ManifestReader::list_manifest_digests_page`) to descriptor-relative directory enumeration using the shared `Arc<FsMetadataReader>`.
**Status:** **DESIGN ONLY — NOT IMPLEMENTED — NOT COMMITTED**.
**Quality Gates:** **O-03, O-04, O-05, O-06, O-13, O-15, O-16, and D-06 remain OPEN**.

---

## 1. Executive Summary & Architectural Motivation

In previous storage extraction milestones:
1. **CAS Blob Operations**: Metadata inspection (`head_blob`), payload streaming (`open_blob`), and directory listing (`list_cas_blobs_page`) were cut over to descriptor-relative containment via `storage_fs::FsMetadataReader` using Linux `openat2` flags (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`).
2. **Manifest Reads**: Manifest HEAD and GET operations (`head_manifest`, `get_manifest`) were cut over to descriptor-relative payload streaming (`open_payload`) using the shared `Arc<FsMetadataReader>` owned by `FsStorage` (`src/storage/fs/manifest.rs`).
3. **Manifest Listing Characterization**: Existing manifest listing semantics in `FsStorage::list_manifest_digests_page` (`src/storage/fs.rs:1000-1046`) were comprehensively characterized (`docs/architecture/filesystem-manifest-listing-characterization.md`).

### 1.1 The Problem: Legacy Pathname Enumeration in Manifest Listing
While blob reads, CAS listing, and manifest reads operate beneath the pinned directory descriptor, **manifest listing still relies on legacy, uncontained pathname operations**:
- **Uncontained Path Construction**: `self.root.join("repos").join(repo).join("manifests")` performs raw path concatenation without repository validation. Traversal sequences (`..`) escape the repository tree, and leading slashes (`/`) discard root prefixes.
- **Uncontained Traversal & Symlinks**: `tokio::fs::read_dir` transparently follows symlinks in intermediate path components and inside the `manifests/` directory, exposing external host paths.
- **Unchecked Entry Types**: Entries are enumerated without checking `file_type()`. Subdirectories, dangling symlinks, and non-regular files are parsed as manifest digests.
- **Silent Error Suppression**: Missing paths, permission denials (`EACCES`), and non-directory components (`ENOTDIR`) are all swallowed as empty success (`Ok((Vec::new(), None))`). Mid-stream `next_entry()` iteration errors terminate early and report partial success.
- **Raw SHA-512 Omission**: Raw 128-hex filenames produced by standard SHA-512 manifest writes are omitted due to a hardcoded `sha256:` prefix parsing failure.
- **Comparator Mismatch**: In-memory sorting uses `digest.hex()`, but pagination binary search uses `digest.as_str()`, causing pagination anomalies and omissions when mixed digest algorithms are present.
- **Arithmetic Overflow**: `start_idx + page_limit` uses unchecked addition on `usize`, risking overflow or panics.

### 1.2 Design Objectives
This document designs the integration of the committed, extracted primitive:
```rust
FsMetadataReader::enumerate_dir(
    &self,
    target: Option<&ObjectKey>,
    limits: DirEnumerationLimits,
) -> Result<Vec<DirEntry>, FsDirError>
```
The design achieves:
1. **Descriptor Containment**: Resolves `repos/<repo>/manifests` strictly beneath the pinned root descriptor via `openat2`, opening and closing a directory descriptor relative to the existing pinned root, rejecting path escapes and intermediate symlinks.
2. **Shared Reader Reuse**: Utilizes the existing `Arc<FsMetadataReader>` already owned by `FsStorage`, preserving reader ownership and asynchronous startup offloading without constructing a new root reader.
3. **Fail-Closed Repository Validation**: Enforces strict pre-composition validation on `repo` strings before touching the filesystem or composing `ObjectKey`, even when `page_limit == 0`.
4. **Regular File Filtering**: Only retains directory entries with point-in-time observed type `DirEntryType::Regular`.
5. **Read Compatibility Alignment**: Fixes raw SHA-512 ingestion so that all stored canonical manifests are discoverable, aligning listed digests with the canonical read-path convention (`repos/<repo>/manifests/<digest.hex()>`).
6. **Consistent Pagination Contract**: Retains whole-string lexical comparison for arbitrary continuation tokens, while avoiding temporary string allocations during sorting via derived `Ord` and in-place `sort_unstable()`, with overflow-safe slicing.
7. **Explicit Error Taxonomy**: Replaces silent error suppression with fail-closed translations aligned with existing CAS listing and manifest-read taxonomy, while cleanly mapping expected missing repositories to empty listings.
8. **Bounded Resource Budgets**: Establishes candidate directory enumeration limits for entry counts and raw name bytes, explicitly analyzing the consumption of budgets by non-manifest entries.
9. **Staged Migration Path**: Defines a `#[cfg(test)]` test seam stage followed by production promotion, with a verified rollback strategy.

---

## 2. Source-Grounded Architectural Inspection

This design is strictly grounded in verified source code across `registry-rust` and `storage-layer-rust`.

### 2.1 Interface Definitions and Forwarding
Manifest listing is defined across three layers in `registry-rust`:

1. **`ManifestReader` Trait** (`src/storage/ports/mod.rs:56-62`):
```rust
// src/storage/ports/mod.rs:56-62
#[async_trait]
pub trait ManifestReader: Send + Sync {
    async fn list_manifest_digests_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<Digest>, Option<String>), StorageError>;
}
```

2. **`Storage` Trait Forwarding** (`src/storage/ports/mod.rs:448-456`):
```rust
// src/storage/ports/mod.rs:448-456
async fn list_manifest_digests_page(
    &self,
    repo: &str,
    continuation_token: Option<&str>,
    page_limit: usize,
) -> Result<(Vec<$crate::registry::digest::Digest>, Option<String>), $crate::storage::StorageError> {
    $crate::storage::Storage::list_manifest_digests_page(self, repo, continuation_token, page_limit).await
}
```

3. **Omnibus `Storage` Trait** (`src/storage/mod.rs:411-416` and forwarded for `&T` at lines 669-677):
```rust
// src/storage/mod.rs:411-416
async fn list_manifest_digests_page(
    &self,
    repo: &str,
    continuation_token: Option<&str>,
    page_limit: usize,
) -> Result<(Vec<Digest>, Option<String>), StorageError>;
```

### 2.2 Concrete Legacy Implementation in `FsStorage`
In `src/storage/fs.rs:1000-1046`:
```rust
// src/storage/fs.rs:1000-1046
async fn list_manifest_digests_page(
    &self,
    repo: &str,
    continuation_token: Option<&str>,
    page_limit: usize,
) -> Result<(Vec<Digest>, Option<String>), StorageError> {
    let manifests_dir = self.root.join("repos").join(repo).join("manifests");
    if !manifests_dir.exists() {
        return Ok((Vec::new(), None));
    }

    let mut all_digests: Vec<Digest> = Vec::new();
    if let Ok(mut entries) = tokio::fs::read_dir(&manifests_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let file_name = entry.file_name().to_string_lossy().to_string();
            if file_name.starts_with(".tmp.") || file_name.starts_with(".lock.") {
                continue;
            }
            if let Ok(d) = Digest::parse(&format!("sha256:{file_name}")) {
                all_digests.push(d);
            } else if let Ok(d) = Digest::parse(&file_name) {
                all_digests.push(d);
            }
        }
    }
    all_digests.sort_by(|a, b| a.hex().cmp(b.hex()));

    let start_idx = if let Some(token) = continuation_token {
        match all_digests.binary_search_by(|d| d.as_str().as_str().cmp(token)) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        }
    } else {
        0
    };

    let end_idx = (start_idx + page_limit).min(all_digests.len());
    let page_slice = &all_digests[start_idx..end_idx];

    let next_token = if end_idx < all_digests.len() {
        page_slice.last().map(|d| d.as_str().to_string())
    } else {
        None
    };

    Ok((page_slice.to_vec(), next_token))
}
```

### 2.3 Shared Reader Ownership in `FsStorage`
In `src/storage/fs.rs:189-230`:
```rust
// src/storage/fs.rs:189-198
#[derive(Debug)]
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
During initialization (`FsStorage::try_new`, lines 201-230), `storage_fs::FsMetadataReader::open(&root)` opens the root directory, performs `reader.probe_capability()`, wraps it in `Arc`, and stores it in `self.reader`. This shared reader is already used for production blob reads, CAS listing, and manifest reads.

### 2.4 Existing Manifest Key Construction & Read Path
In `src/storage/fs/manifest.rs:51-93`:
```rust
// src/storage/fs/manifest.rs:51-93
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

Standard manifest write operations (`src/storage/fs.rs:849-866`) store manifests using:
```rust
// src/storage/fs.rs:855-860
let dir = self.root.join("repos").join(name).join("manifests");
ensure_dir(&dir)?;
let media_type = self.detect_manifest_media_type(&bytes).await?;
let path = dir.join(digest.hex());
atomic_write_file(&path, &bytes).await?;
```
Standard writers store files strictly as `<digest.hex()>`, which is:
- Raw 64 lowercase hex characters for SHA-256 (`[0-9a-f]{64}`).
- Raw 128 lowercase hex characters for SHA-512 (`[0-9a-f]{128}`).

### 2.5 Extracted Directory Enumeration Primitive in `storage-fs`
In `storage-layer-rust/crates/storage-fs/src/reader.rs:291-313` and `dir.rs:67-221`:
```rust
// storage-fs/src/reader.rs:291-295
pub async fn enumerate_dir(
    &self,
    target: Option<&ObjectKey>,
    limits: crate::dir::DirEnumerationLimits,
) -> Result<Vec<crate::dir::DirEntry>, crate::dir::FsDirError>
```

### 2.6 `Digest` Ordering and Structure
In `src/registry/digest.rs:3-7`:
```rust
// src/registry/digest.rs:3-7
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest {
    algo: String,
    hex: String,
}
```
In Rust, derived `Ord` compares fields in lexical declaration order: `algo` is compared first, and if equal, `hex` is compared. Because supported algorithms are `"sha256"` (6 ASCII bytes) and `"sha512"` (6 ASCII bytes), comparing `(self.algo, self.hex)` is equivalent to canonical algorithm-prefixed lexical order without requiring temporary formatted `String` allocations during comparisons.

---

## 3. Detailed Architectural Design

### 3.1 Integration, Reader Ownership, and Execution Boundaries

#### 3.1.1 Minimal Module Scope and Delegation
We propose adding an internal module `src/storage/fs/manifest_listing.rs` (following the pattern of `src/storage/fs/listing.rs` for CAS listing and `src/storage/fs/manifest.rs` for manifest reads).

```
FsStorage (src/storage/fs.rs)
  │ owns: reader: Arc<FsMetadataReader>
  │
  ├─ head_manifest / get_manifest ──► manifest::*_impl(self.reader.as_ref(), ...)
  ├─ list_cas_blobs_page         ──► listing::list_cas_blobs_page_impl(self.reader.as_ref(), ...)
  │
  └─ list_manifest_digests_page  ──► manifest_listing::list_manifest_digests_page_impl(
                                         self.reader.as_ref(),
                                         repo,
                                         continuation_token,
                                         page_limit,
                                         limits,
                                     )
```

#### 3.1.2 Precise Ownership & Descriptor Boundaries
- **No New Root Reader**: No new root reader is constructed; enumeration opens and closes a directory descriptor relative to the existing pinned root.
- **Fresh Directory Descriptor per Call**: Each call to `enumerate_dir` invokes Linux `openat2` to open a new readable directory file descriptor (`O_RDONLY | O_DIRECTORY | O_CLOEXEC`) beneath the pinned root descriptor. Ownership is transferred to an internal `DIR*` stream via `fdopendir` and closed cleanly upon task completion via `closedir`.
- **Preserving the `spawn_blocking` Boundary**: In `crates/storage-fs/src/dir.rs`, `enumerate_dir_async` offloads the blocking syscalls (`openat2`, `fdopendir`, `readdir`, `closedir`) to a dedicated blocking thread via `tokio::task::spawn_blocking`. Enumeration does not run on the asynchronous worker thread.
- **Capability Probing Timing**: Capability probing occurs during reader construction (`storage_fs::FsMetadataReader::open` in `FsStorage::try_new`), not per-call, and not necessarily once per process.
- **Symlink Component Rejection vs Entry Observation**:
  - *Symlinked path components*: `openat2` with `RESOLVE_NO_SYMLINKS` rejects any intermediate symlink in the path components leading to `manifests/` at the kernel boundary (`ResolutionRejected`).
  - *Symlink entries*: `readdir` reads directory entries inside `manifests/` by name and observes `DirEntryType::Symlink` without following the symlink target.
- **No Payload Reading**: Directory enumeration observes only directory entry names and their `DirEntryType`. It does not read payload bytes behind each entry.

#### 3.1.3 Internal Test Abstraction Seam
To enable deterministic unit testing and fault injection without modifying `storage-fs` or production reader code, we define a narrow test trait in `manifest_listing.rs`:
```rust
#[async_trait]
pub(crate) trait ManifestDirEnumerator: Send + Sync {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError>;
}

#[async_trait]
impl ManifestDirEnumerator for storage_fs::FsMetadataReader {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError> {
        self.enumerate_dir(target, limits).await
    }
}
```

### 3.2 Repository Validation and Key Composition

#### 3.2.1 Pre-Composition Path Safety Validation
Direct storage callers invoke `Storage::list_manifest_digests_page(repo, ...)` with arbitrary string slices. Unlike the external HTTP API which validates against OCI repository grammar (`CanonicalRepoName`), direct storage calls must enforce fail-closed path safety checks before performing any filesystem interaction or key composition.

The directory key helper is defined as:
```rust
pub(crate) fn manifest_dir_key(repo: &str) -> Result<ObjectKey, StorageError> {
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

    let key_str = format!("repos/{repo}/manifests");
    ObjectKey::parse(&key_str).map_err(|e| StorageError::InvalidRepoName(e.to_string()))
}
```

#### 3.2.2 Key Composition and `ObjectKey` Validation Analysis
Composing `repos/{repo}/manifests`:
1. **Leading & Trailing Slashes**: Pre-composition check verifies `repo` does not start or end with `/`. The composed string starts with `repos/` and ends with `/manifests`. `ObjectKey::parse` confirms no leading or trailing slashes.
2. **Consecutive Slashes (`//`)**: Pre-composition check splits by `/` and rejects empty segments. Therefore, no `//` occurs in `{repo}`, and the composition with `repos/` and `/manifests` is free of consecutive slashes.
3. **Traversal Segments (`.` and `..`)**: Neither `repos` nor `manifests` is `.` or `..`. Pre-composition checks ensure no segment in `{repo}` is `.` or `..`. Path traversal is rejected before `ObjectKey::parse`.
4. **Nested Repositories**: e.g. `"library/ubuntu"` or `"company/team/project"`. Pre-composition validates each segment independently. The resulting key `repos/company/team/project/manifests` is a valid hierarchical key.
5. **Backslashes, NUL, and Control Characters**: Pre-composition checks reject `\\`, `\0`, and `c.is_ascii_control()` explicitly. `ObjectKey::parse` also enforces `!s.contains('\\')`, `!s.contains('\0')`, and `!s.chars().any(|c| c.is_control())`.
6. **Windows Drive Prefix (`C:/repo`)**:
   - `ObjectKey::parse` checks whether byte 0 is ASCII alphabetic and byte 1 is `:`.
   - In `repos/C:/repo/manifests`, byte 0 is `'r'` and byte 1 is `'e'`. The drive prefix check on the composed key is **not** triggered.
   - On Linux host systems, `C:` is a valid relative directory name containing a colon (`:`). It passes pre-composition validation and `ObjectKey::parse`.
   - On Windows (non-Linux), descriptor containment is unsupported (`PlatformUnsupported`).
   - This exact behavior matches `manifest_key` in `src/storage/fs/manifest.rs`.

---

### 3.3 Filename Interpretation and Read Compatibility Alignment

#### 3.3.1 Canonical Layout vs. Anomalous Filenames
The table below contrasts standard writer output against anomalous and legacy filenames observed in characterization:

| Filename Category | Example Filename on Disk | Standard Writer Produces? | Legacy Listing Action | Contained Listing Action | Canonical Read Path (`get_manifest`) | Alignment with Canonical Path |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Raw SHA-256 (64 hex)** | `e3b0c44...` (64 chars) | **Yes** (`put_manifest`) | Parsed as `sha256:<hex>` | **Parsed as `sha256:<hex>`** | `repos/<repo>/manifests/<hex>` | Matches exact canonical name |
| **Raw SHA-512 (128 hex)** | `cf83e13...` (128 chars) | **Yes** (`put_manifest`) | **Omitted** (parse bug) | **Parsed as `sha512:<hex>`** | `repos/<repo>/manifests/<hex>` | Matches exact canonical name |
| **Prefixed SHA-256** | `sha256:e3b0c44...` | No (legacy/manual) | Parsed via fallback | **Ignored** (non-canonical) | `repos/<repo>/manifests/<hex>` | Mismatched (name on disk has prefix) |
| **Prefixed SHA-512** | `sha512:cf83e13...` | No (legacy/manual) | Parsed via fallback | **Ignored** (non-canonical) | `repos/<repo>/manifests/<hex>` | Mismatched (name on disk has prefix) |
| **Uppercase Hex** | `E3B0C44...` | No (manual) | Parsed & lowercased | **Ignored** (non-canonical) | `repos/<repo>/manifests/<hex>` | Mismatched (Linux is case-sensitive) |
| **Temporary / Lock Files** | `.tmp.upload-123`, `.lock.manifest` | Temporary writer artifacts | Skipped | **Skipped** | N/A | Ignored non-manifest files |
| **Malformed / Non-Hex** | `manifest.json`, `index.lock` | Corrupt / foreign file | Skipped | **Skipped** | N/A | Ignored non-manifest files |
| **Non-UTF-8** | `ÿþ...` | Corrupt filesystem name | Skipped (`to_string_lossy`) | **Skipped** (`to_str().is_none()`) | N/A | Ignored non-manifest files |

#### 3.3.2 Parsing Policy & Scope of Read Compatibility
**Policy: Strict Canonical Raw Hex Parsing**.
A directory entry is parsed into a `Digest` if and only if its filename meets the canonical layout:
1. Contains only lowercase ASCII hexadecimal digits (`b'0'..=b'9' | b'a'..=b'f'`).
2. Length is exactly 64 chars -> `Digest::parse(&format!("sha256:{name}"))`.
3. Length is exactly 128 chars -> `Digest::parse(&format!("sha512:{name}"))`.
4. Any filename with uppercase characters, algorithm prefixes, colons, or non-hex characters is ignored.

**Clarification of Non-Guaranteed Readability**:
Canonical filenames align listing output with the canonical read-path convention (`repos/<repo>/manifests/<digest.hex()>`). This alignment **does not guarantee that a subsequent `get_manifest` call will succeed**:
- The file may be deleted or unlinked by a concurrent mutation (TOCTOU race).
- The file may have restrictive permissions preventing reading (`EACCES`).
- The payload may contain invalid JSON or corrupt data (`StorageErrorKind::CorruptData`).
- Underlying filesystem I/O errors may occur during stream reading.
- Listing guarantees syntactic alignment with the read adapter, not subsequent read success.

#### 3.3.3 Deduplication
- Standard writers write distinct digest hex strings; each canonical regular file has a unique name in a single directory.
- Calling `all_digests.dedup()` ensures no duplicate digests are returned, avoiding duplicate root count increments in `BlobRefIndex::sync_repo_manifests_and_tags`.

---

### 3.4 Entry Types, Containment, and Error Taxonomy

#### 3.4.1 Entry Type Handling
`storage_fs::DirEntry::file_type()` returns `DirEntryType`. The contained listing implementation handles entry types as follows:
- `DirEntryType::Regular`: **Accepted**. This represents a standard stored manifest payload.
- `DirEntryType::Directory`: **Ignored**. Subdirectories inside `manifests/` are skipped.
- `DirEntryType::Symlink`: **Ignored**. Symlinks inside `manifests/` are skipped.
  - *Rationale*: `storage_fs` payload reads (`open_payload`) use `RESOLVE_NO_SYMLINKS`. Any symlink inside `manifests/` will be rejected by `open_payload` with `ResolutionRejected`. Enumerating symlinks would advertise entries that the read adapter will reject.
- `DirEntryType::Other`: **Ignored**. FIFOs, UNIX domain sockets, character/block devices are skipped.

#### 3.4.2 Error Taxonomy and Comparison with Existing Modules
`StorageError` in `registry-rust` is defined as:
```rust
pub enum StorageError {
    NotFound,
    InvalidRepoName(String),
    Internal { kind: StorageErrorKind, message: String },
    // ...
}
```
Helper constructors include `StorageError::io(...)`, `StorageError::corrupt_data(...)`, `StorageError::permission_denied(...)`, `StorageError::configuration(...)`, and `StorageError::backend(...)`.

The table below compares the error translations across CAS listing (`src/storage/fs/listing.rs:158-189`), manifest payload reads (`src/storage/fs/read_adapter.rs:92-140`), and the proposed manifest listing implementation:

| `FsDirError` Variant | CAS Listing Translation (`listing.rs:158-189`) | Manifest Read Translation (`read_adapter.rs:92-140`) | Proposed Manifest Listing Translation | Comparison & Approval Status |
| :--- | :--- | :--- | :--- | :--- |
| `NotFound { .. }` | `StorageError::NotFound` | `StorageError::NotFound` | `Ok((Vec::new(), None))` | Expected empty repository: returns empty listing (requires approval) |
| `NotADirectory { path }` | `StorageError::corrupt_data(...)` | N/A (single file open) | `StorageError::corrupt_data(...)` | **Identical to CAS listing** (`StorageErrorKind::CorruptData`) |
| `PermissionDenied { source, .. }` | `StorageError::permission_denied(...)` | `StorageError::io(...)` | `StorageError::permission_denied(...)` | **Identical to CAS listing** (`StorageErrorKind::PermissionDenied`) |
| `ResolutionRejected { source, .. }` | `StorageError::io(...)` | `StorageError::io(...)` | `StorageError::io(...)` | **Identical to CAS listing & Manifest Reads** (`StorageErrorKind::Io`) |
| `SyscallUnsupported(source)` | `StorageError::configuration(...)` | `StorageError::configuration(...)` | `StorageError::configuration(...)` | **Identical to CAS listing & Manifest Reads** (`StorageErrorKind::Configuration`) |
| `PlatformUnsupported` | `StorageError::configuration(...)` | N/A (platform macro) | `StorageError::configuration(...)` | **Identical to CAS listing** (`StorageErrorKind::Configuration`) |
| `LimitExceeded { reason }` | `StorageError::backend(...)` | N/A (no dir limits) | `StorageError::backend(...)` | **Identical to CAS listing** (`StorageErrorKind::Backend`) |
| `EntryDisappeared { name }` | `StorageError::io(...)` | N/A | `StorageError::io(...)` | **Identical to CAS listing** (`StorageErrorKind::Io`) |
| `Io { source }` | `StorageError::io(...)` | `StorageError::io(...)` | `StorageError::io(...)` | **Identical to CAS listing & Manifest Reads** (`StorageErrorKind::Io`) |
| `RuntimeMissing(source)` | `StorageError::backend(...)` | N/A | `StorageError::backend(...)` | **Identical to CAS listing** (`StorageErrorKind::Backend`) |
| `TaskJoinFailed(source)` | `StorageError::backend(...)` | N/A | `StorageError::backend(...)` | **Identical to CAS listing** (`StorageErrorKind::Backend`) |
| `other` (non-exhaustive fallback) | `StorageError::backend(...)` | N/A | `StorageError::backend(...)` | **Identical to CAS listing** (`StorageErrorKind::Backend`) |

---

### 3.5 Ordering, Pagination, and Continuation Contract

#### 3.5.1 In-Place Sorting and Derived `Ord`
`Digest` in `src/registry/digest.rs:3-7` derives `Ord`:
```rust
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest {
    algo: String,
    hex: String,
}
```
In Rust's derived `Ord`, fields are compared in declaration order: `algo` first, then `hex`.
Because supported algorithms are `"sha256"` (6 chars) and `"sha512"` (6 chars), comparing `(a.algorithm(), a.hex())` is lexicographically identical to comparing canonical string format `format!("{}:{}", a.algorithm(), a.hex())`.

By using `all_digests.sort_unstable()`, sorting executes in-place:
- Avoids temporary canonical `String` allocations in the sort comparator.
- Does not allocate auxiliary slice storage, unlike standard stable `sort()`.

#### 3.5.2 Arbitrary Token Comparison and Lexical Contract
In legacy code, binary search evaluates:
```rust
match all_digests.binary_search_by(|d| d.as_str().as_str().cmp(token)) {
    Ok(idx) => idx + 1,
    Err(idx) => idx,
}
```
**Why Whole-String Comparison is Preserved**:
Splitting every colon-containing token into `(algorithm, hex)` changes lexical insertion behavior for arbitrary tokens.
*Counterexample*:
- Stored Digest: `"sha256:abcd..."`
- Caller Token: `"sha2560:anything"`
- Under whole-string comparison: The token `"sha2560:anything"` sorts BEFORE `"sha256:abcd..."` because `'0'` (ASCII 0x30) sorts before `':'` (ASCII 0x3A). The search starts at the digest.
- Under tuple splitting `("sha2560", "anything")`: `"sha256"` is shorter than `"sha2560"`, so the tuple comparator places `"sha256"` before `"sha2560"`, reversing the relative order!

To preserve strict backwards compatibility with legacy arbitrary-token lookup without imposing premature token validation rules, the contained design retains **whole-string raw lexical comparison** during binary search:
```rust
match all_digests.binary_search_by(|d| d.as_str().as_str().cmp(token)) {
    Ok(idx) => idx + 1,
    Err(idx) => idx,
}
```
Because binary search performs at most $\lceil \log_2(N) ceil$ comparisons (e.g. 14 string allocations for 10,000 items), this allocation cost is bounded and negligible, while guaranteeing exact compatibility with arbitrary caller tokens.

#### 3.5.3 Token Boundaries and Exact Behavior
- **`"sha2560:anything"`**: Lexicographically sorts before `"sha256:"` in whole-string comparison (ASCII `'0'` < `':'`). Resumes at index 0.
- **`"sha256:anything"`**: Directly compared against `"sha256:<hex>"`.
- **Multiple Colons (`"sha256:abc:def"`)**: Compared directly as a full string without parse error.
- **Empty Token (`""`) and No-Colon Tokens (`"sha256"`, `"nocolon"`)**: Compared directly; `"sha256" < "sha256:"`, so restarts at index 0.
- **Uppercase Token (`"SHA256:..."`)**: In ASCII, `'S' < 's'`, so sorts before all lowercase digests, restarting at index 0.
- **Valid Previously Issued Tokens**: Resumes at the exact item following the token.

#### 3.5.4 Traversal In-Flight Across Cutover
If a client begins paginating before cutover and resumes under contained listing:
- **Unchanged Canonical Regular-File Collections**: For homogeneous SHA-256 collections of regular files without non-regular entries or duplicates, sort order and tokens are identical; traversal continues seamlessly.
- **Altered Collections**: If a repository contained non-regular files, symlinks, or duplicates, filtering and deduplication alter relative index offsets.
- **Mixed-Algorithm Repositories**: Legacy listing parsed prefixed SHA-512 files (`sha512:<hex>`) and sorted them by `hex()`, whereas contained listing sorts by canonical `(algo, hex)`. A client paginating across cutover on a mixed repository may observe entries repeated or skipped. Unchanged token syntax does not guarantee unchanged traversal across deployment.

#### 3.5.5 Zero-Limit Validation Order and Qualified Progress
- **Zero-Limit Validation Order**:
  The implementation validates the repository name first, before checking `page_limit == 0`:
  ```rust
  let dir_key = manifest_dir_key(repo)?;
  if page_limit == 0 {
      return Ok((Vec::new(), None));
  }
  ```
  - An invalid repository name (e.g. traversal `..` or leading `/`) fails closed with `StorageError::InvalidRepoName`, even when `page_limit == 0`.
  - A valid repository name with `page_limit == 0` returns `Ok((Vec::new(), None))` immediately without calling `enumerator.enumerate_dir(...)` (zero filesystem I/O).
- **Progress Guarantees (Qualified)**:
  - For an unchanged repository directory and successful calls with `page_limit > 0`:
    - If empty ($N = 0$): returns `Ok(([], None))` on call 1.
    - If containing $N > 0$ valid manifests: traversal completes in exactly `ceil(N / page_limit)` calls, with monotonically advancing tokens.
- **Concurrent Mutations**:
  - Additions after the cursor *may* be observed on subsequent pages; they are not guaranteed to be observed. Additions before the cursor will not be observed.

---

### 3.6 Resource Accounting, Budgets, and Scale

#### 3.6.1 Candidate Limits and Lack of Empirical Guarantees
We define candidate limits:
- `DEFAULT_MAX_MANIFEST_ENTRIES: usize = 10_000;`
- `DEFAULT_MAX_MANIFEST_NAME_BYTES: usize = 1_500_000;`

**Explicit Disclaimer**:
These values are provisional candidates. They do **not** guarantee:
- Accommodating all large production repositories.
- Representing typical registry scale.
- Preventing heap exhaustion under high concurrency.
- Bounding kernel blocking duration.
There is currently no empirical benchmark data for manifest directory scale.

#### 3.6.2 Interaction of Both Limits
The two limits constrain enumeration together:
- Budgets permit up to and including the exact configured limits; `LimitExceeded` occurs when an entry would exceed either limit.
- Even though 1,500,000 name bytes could hold ~23,000 64-char SHA-256 filenames, the 10,000-entry cap halts enumeration once 10,000 entries are retained.
- For 128-char SHA-512 names, 10,000 entries require $10,000 	imes 128 = 1,280,000$ bytes, fitting within the 1.5 MB byte cap.
- Whichever budget is exhausted first triggers `FsDirError::LimitExceeded`.

#### 3.6.3 Budget Consumption by Ignored Entries
`enumerate_dir` evaluates limits as entries are read from the kernel:
- Temporary files (`.tmp.*`), locks (`.lock.*`), subdirectories, symlinks, and malformed names are counted toward `max_entries` and `max_total_name_bytes` before filtering!
- If a directory contains 10,000 temporary files and 5 valid manifests, enumeration will exceed the budget and fail with `LimitExceeded`, even though only 5 manifests would have survived filtering.

#### 3.6.4 Memory Contributors and Operational Impact
- **Peak Memory**:
  - Userspace heap: Retained entry counts and raw name bytes are bounded by limits, but struct overhead (`DirEntry`), vectors, and libc buffers are not.
  - Kernel memory: dentries, directory page cache.
  - Libc buffers: internal `getdents64` buffers.
- **Concurrency**: Memory scales with $C$ concurrent requests ($C 	imes 	ext{heap}$).
- **Repeated Scans**: Paginating over $P$ pages enumerates and sorts the directory $P$ times.
- **Syscall Duration**: `openat2` and `readdir` are blocking kernel operations. Limits bound retained heap bytes, not disk latency or directory traversal time.
- **Operational Impact**: With hardcoded candidate budgets, changing a limit requires a code/configuration change and deployment as applicable; there is no existing runtime configuration to adjust. If an existing repository exceeds either limit, listing fails immediately with `StorageErrorKind::Backend`, disrupting manifest discovery and index reconciliation.
- **Implementation Options**:
  - *Option A*: Hardcoded candidate constants (simplest, rigid).
  - *Option B*: Storage configuration settings in `Config` (configurable, larger surface).
  - *Decision*: Candidate budget values and hardcoded-versus-configurable policy remain explicitly awaiting approval.

---

### 3.7 Caller Inspection & Control Flow Analysis

#### 3.7.1 Complete Analysis of `BlobRefIndex::sync_repo_manifests_and_tags`
In `src/blob_ref_index.rs:486-537`:
```rust
// src/blob_ref_index.rs:486-537
pub async fn sync_repo_manifests_and_tags(
    &self,
    storage: &(impl crate::storage::BlobRefIndexStoragePort + ?Sized),
    repo: &str,
) -> Result<(), RefIndexError> {
    // 1. Remove all existing tags for this repo from the index.
    let prefix = tag_prefix(repo);
    let existing_tags: Vec<Vec<u8>> = self
        .tag_to_root
        .scan_prefix(prefix)
        .filter_map(|r| r.ok())
        .map(|(k, _v)| k.to_vec())
        .collect();
    for k in existing_tags {
        let _ = self.tag_to_root.remove(k);
    }

    // 2. Bounded pagination of ALL stored manifests in this repo (stored manifests are roots)
    let mut manifest_token: Option<String> = None;
    loop {
        let (manifests, next_tok) = storage
            .list_manifest_digests_page(repo, manifest_token.as_deref(), 128)
            .await?;
        for digest in manifests {
            self.inc_root_count(digest.as_str().as_bytes())?;
            self.ingest_root(storage, repo, &digest).await?;
        }
        match next_tok {
            Some(tok) => manifest_token = Some(tok),
            None => break,
        }
    }

    // 3. Bounded pagination of tags in this repo (tags map alias -> digest)
    let mut tag_token: Option<String> = None;
    loop {
        let (tags, next_tok) = storage
            .list_tags_page(repo, tag_token.as_deref(), 128)
            .await?;
        for (tag, digest) in tags {
            self.tag_to_root
                .insert(tag_key(repo, &tag), digest.as_str().as_bytes())?;
        }
        match next_tok {
            Some(tok) => tag_token = Some(tok),
            None => break,
        }
    }

    self.db.flush()?;
    Ok(())
}
```

**Precise Control Flow Findings**:
1. **State Mutation Before Listing**: Step 1 attempts to remove existing tags for `repo` from `self.tag_to_root` before manifest listing begins. Tag removals are attempted and removal errors are ignored (`let _ = self.tag_to_root.remove(k);`); success of every removal is not guaranteed.
2. **Error Path (Fail-Closed)**: If `storage.list_manifest_digests_page` returns an `Err`, the function aborts immediately via `?`:
   - Existing tags that were removed remain removed from `self.tag_to_root`.
   - Any roots incremented in earlier pages of the loop remain incremented.
   - There is **no transactional rollback** or reversal of earlier state changes.
   - Propagating an error does not "preserve existing counts or roll back earlier changes."
3. **Legacy Empty Suppression**: If listing returns `Ok(([], None))` (legacy behavior under `EACCES`):
   - Step 2 terminates with 0 manifests ingested.
   - Step 1 only touched tags, not root counts. Existing root counts in `self.root_counts` are NOT wiped by listing returning empty.
   - However, stored manifests are not re-registered as roots.
   - Step 3 continues to sync tags, and `self.db.flush()?` commits the state.
   - Legacy empty listing does not "wipe all references," but it fails to register manifests as roots.

#### 3.7.2 `ManifestLifecycleService::is_blob_referenced_in_repo`
In `src/manifest_lifecycle.rs:773-801`:
```rust
async fn is_blob_referenced_in_repo(&self, repo: &str, target_blob: &Digest) -> bool {
    let mut tok: Option<String> = None;
    loop {
        let (page, next_tok) = match self
            .storage
            .list_manifest_digests_page(repo, tok.as_deref(), 100)
            .await
        {
            Ok(p) => p,
            Err(_) => return false,
        };
        // ... inspect references via get_manifest ...
```
- **Error Behavior**: On any `Err(_)`, `is_blob_referenced_in_repo` returns `false` (concluding the blob is unreferenced).
- Returning a backend error from `list_manifest_digests_page` causes `is_blob_referenced_in_repo` to report `false`. This is **not** a fail-closed reference discovery at the caller level.
- Contained listing fixing raw SHA-512 discovery allows SHA-512 manifests to be inspected, preventing false negatives where blobs referenced only by SHA-512 manifests were reported as unreferenced.

#### 3.7.3 `blob_gc::build_manifest_protected_set`
In `src/blob_gc/policy.rs:170-176`:
```rust
if storage.kind() == "fs"
    && tokio::fs::metadata(&cfg.fs_root.join("repos"))
        .await
        .is_ok()
{
    return build_manifest_protected_set_fs(&cfg.fs_root).await;
}
```
- **Verified Dispatch**: When `storage.kind() == "fs"` and `cfg.fs_root.join("repos")` exists, GC calls `build_manifest_protected_set_fs(&cfg.fs_root)`.
- `build_manifest_protected_set_fs` performs its own recursive directory walk over `repos/*/manifests/*` and **does not call `list_manifest_digests_page`**.
- Changing `FsStorage::list_manifest_digests_page` has **zero direct effect on filesystem GC protected set construction** under standard configuration.
- The abstract storage fallback loop (lines 184-222) calls `list_manifest_digests_page` only when `storage.kind() != "fs"` or `repos/` does not exist.

---

## 4. Staged Implementation and Verification Plan

### 4.1 Staged Rollout Strategy
To decouple functional testing from production exposure, the cutover is divided into two distinct stages:

```
Stage 1: Internal Test Seam (Scoped to #[cfg(test)])
  ├─ Declare #[cfg(test)] pub(crate) mod manifest_listing; in src/storage/fs.rs
  ├─ Create src/storage/fs/manifest_listing.rs
  ├─ Exercise ManifestDirEnumerator with fakes and real filesystem tests
  └─ Production FsStorage::list_manifest_digests_page is UNCHANGED

Stage 2: Production Promotion & Cutover
  ├─ Remove #[cfg(test)] on mod manifest_listing; in src/storage/fs.rs
  ├─ Wire FsStorage::list_manifest_digests_page to manifest_listing_impl
  └─ Update regression tests in src/storage/fs/tests.rs
```

### 4.2 Exact File Sets for Both Stages

#### Stage 1 File Set:
- **New**: `registry-rust/src/storage/fs/manifest_listing.rs`
- **Modify**: `registry-rust/src/storage/fs.rs` (add `#[cfg(test)] pub(crate) mod manifest_listing;`)

#### Stage 2 File Set (Promotion):
- **Modify**: `registry-rust/src/storage/fs.rs`
  - Remove `#[cfg(test)]` from `mod manifest_listing;` (making it unconditional `pub(crate) mod manifest_listing;`).
  - Modify `list_manifest_digests_page` (lines 1000-1046) to delegate to `manifest_listing::list_manifest_digests_page_impl`.
- **Modify**: `registry-rust/src/storage/fs/tests.rs`
  - Update characterization tests to assert contained semantics.

### 4.3 Proposed Implementation: `src/storage/fs/manifest_listing.rs` (Uncompiled Proposal)
*(Note: The following code snippet is an uncompiled architectural proposal for review).*

```rust
//! Contained filesystem manifest listing implementation for `registry-rust`.
//! (PROPOSED IMPLEMENTATION — UNCOMPILED)

use async_trait::async_trait;
use storage_core::ObjectKey;
use storage_fs::{DirEntry, DirEntryType, DirEnumerationLimits, FsDirError};

use crate::registry::digest::Digest;
use crate::storage::StorageError;

/// Candidate limit constants for manifest directory enumeration.
pub const DEFAULT_MAX_MANIFEST_ENTRIES: usize = 10_000;
pub const DEFAULT_MAX_MANIFEST_NAME_BYTES: usize = 1_500_000;

/// Runtime helper to construct default manifest directory limits.
pub fn default_manifest_dir_limits() -> DirEnumerationLimits {
    DirEnumerationLimits::new(DEFAULT_MAX_MANIFEST_ENTRIES, DEFAULT_MAX_MANIFEST_NAME_BYTES)
}

/// Seam trait for directory enumeration beneath the pinned root.
#[async_trait]
pub(crate) trait ManifestDirEnumerator: Send + Sync {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError>;
}

#[async_trait]
impl ManifestDirEnumerator for storage_fs::FsMetadataReader {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError> {
        self.enumerate_dir(target, limits).await
    }
}

/// Validates repository name and composes directory key `repos/<repo>/manifests`.
pub(crate) fn manifest_dir_key(repo: &str) -> Result<ObjectKey, StorageError> {
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

    let key_str = format!("repos/{repo}/manifests");
    ObjectKey::parse(&key_str).map_err(|e| StorageError::InvalidRepoName(e.to_string()))
}

/// Translates `FsDirError` into `StorageError` aligned with CAS listing conventions.
pub(crate) fn translate_fs_dir_error(err: FsDirError) -> StorageError {
    match err {
        FsDirError::NotFound { .. } => StorageError::NotFound,
        FsDirError::NotADirectory { path } => {
            StorageError::corrupt_data(format!("target path is not a directory: {path:?}"))
        }
        FsDirError::PermissionDenied { source, .. } => {
            StorageError::permission_denied(source.to_string())
        }
        FsDirError::ResolutionRejected { source, .. } => {
            StorageError::io(source.to_string())
        }
        FsDirError::SyscallUnsupported(source) => {
            StorageError::configuration(format!(
                "openat2 is unavailable in this execution environment: {source}"
            ))
        }
        FsDirError::PlatformUnsupported => {
            StorageError::configuration(
                "platform unsupported: descriptor-relative containment requires Linux openat2"
            )
        }
        FsDirError::LimitExceeded { reason } => {
            StorageError::backend(format!("enumeration resource limit exceeded: {reason:?}"))
        }
        FsDirError::EntryDisappeared { name } => {
            StorageError::io(format!("directory entry disappeared during type inspection: {name:?}"))
        }
        FsDirError::Io { source } => {
            StorageError::io(source.to_string())
        }
        FsDirError::RuntimeMissing(err) => {
            StorageError::backend(format!("tokio runtime missing: {err}"))
        }
        FsDirError::TaskJoinFailed(err) => {
            StorageError::backend(format!("blocking enumeration task join failed: {err}"))
        }
        other => {
            StorageError::backend(format!("unexpected directory enumeration error: {other}"))
        }
    }
}

/// Core implementation of contained manifest listing.
pub(crate) async fn list_manifest_digests_page_impl(
    enumerator: &(impl ManifestDirEnumerator + ?Sized),
    repo: &str,
    continuation_token: Option<&str>,
    page_limit: usize,
    limits: DirEnumerationLimits,
) -> Result<(Vec<Digest>, Option<String>), StorageError> {
    let dir_key = manifest_dir_key(repo)?;
    if page_limit == 0 {
        return Ok((Vec::new(), None));
    }

    let entries = match enumerator.enumerate_dir(Some(&dir_key), limits).await {
        Ok(entries) => entries,
        Err(FsDirError::NotFound { .. }) => return Ok((Vec::new(), None)),
        Err(err) => return Err(translate_fs_dir_error(err)),
    };

    let mut all_digests: Vec<Digest> = Vec::new();
    for entry in entries {
        if entry.file_type() != DirEntryType::Regular {
            continue;
        }
        let Some(name) = entry.name().to_str() else {
            continue;
        };
        if name.starts_with(".tmp.") || name.starts_with(".lock.") {
            continue;
        }

        // Canonical hex validation: lowercase ascii hex only
        if !name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            continue;
        }

        if name.len() == 64 {
            if let Ok(d) = Digest::parse(&format!("sha256:{name}")) {
                all_digests.push(d);
            }
        } else if name.len() == 128 {
            if let Ok(d) = Digest::parse(&format!("sha512:{name}")) {
                all_digests.push(d);
            }
        }
    }

    all_digests.sort_unstable();
    all_digests.dedup();

    let start_idx = if let Some(token) = continuation_token {
        match all_digests.binary_search_by(|d| d.as_str().as_str().cmp(token)) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        }
    } else {
        0
    };

    let end_idx = start_idx.saturating_add(page_limit).min(all_digests.len());
    let page_slice = &all_digests[start_idx..end_idx];

    let next_token = if end_idx < all_digests.len() {
        page_slice.last().map(|d| d.as_str().to_string())
    } else {
        None
    };

    Ok((page_slice.to_vec(), next_token))
}
```

### 4.4 Test Plan and Verification Specification
The test plan specifies concrete proposed coverage:

1. **Shared-Root Pinning Across Rename / Replacement**:
   - Verify that operations through the pinned reader continue resolving relative to the pinned directory handle after renaming or unlinking the original path.
2. **Canonical Listing-to-Read Compatibility (Without Guaranteed Readability)**:
   - Verify that listed digests align with the canonical `repos/<repo>/manifests/<digest.hex()>` path convention.
   - Verify that if a listed manifest file is concurrently unlinked or set to `chmod 000`, `get_manifest` fails with expected errors without compromising listing consistency.
3. **Exact Budget Boundaries & Ignored Entries**:
   - Verify that non-manifest entries (subdirectories, symlinks, `.tmp.*` files) consume budget capacity.
   - Test that exceeding `DEFAULT_MAX_MANIFEST_ENTRIES` triggers `LimitExceeded` mapped to `StorageErrorKind::Backend`.
4. **Zero Limits and Arithmetic Overflow**:
   - Test invalid repository with `page_limit == 0` fails closed with `StorageError::InvalidRepoName`.
   - Test valid repository with `page_limit == 0` returns `Ok(([], None))` with zero calls to `enumerator.enumerate_dir`.
   - Test `page_limit = usize::MAX` with `start_idx > 0` executes safely without panic via saturating addition.
5. **Token Compatibility & Bounded Traversal**:
   - Test arbitrary token strings: `"sha2560:anything"`, `"sha256:anything"`, multiple colons (`"sha256:abc:def"`), empty token `""`, no-colon token (`"sha256"`), uppercase tokens (`"SHA256:..."`), and previously issued tokens.
   - Verify that pagination terminates in bounded iterations.
6. **Typed Error Mapping Fixtures**:
   - Downstream unit tests will cover all current library variants of `FsDirError` using `RecordingFakeManifestEnumerator`.
   - The non-exhaustive wildcard branch (`other => StorageError::backend(...)`) is an explicit forward-compatibility branch verified by code inspection and compiler check.
7. **Permission Denied Restoration Guards**:
   - Verify `PermissionDenied` returns `StorageErrorKind::PermissionDenied` under `chmod 000`, with RAII guard restoring directory permissions on drop.
8. **Verified Cargo Commands (Planned)**:
   - `cargo test --locked --package registry-rust --lib storage::fs::manifest_listing`
   - `cargo test --locked --package registry-rust --lib storage::fs::tests`

---

## 5. Compatibility Decisions, Rollback, and Operational Limitations

### 5.1 Compatibility Decision Matrix

| Dimension | Existing Characterized Behavior | Recommended Contained Behavior | Justification | Compatibility Impact | Approval Required? |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Path Containment** | Unvalidated `Path::join`; allows `..` escapes and leading `/` | Pre-composition validation + descriptor-relative `openat2` | Eliminates directory escapes | Rejects illegal names with `InvalidRepoName` | **Yes** |
| **Missing Repo / Manifests** | Returns `Ok(([], None))` | Returns `Ok(([], None))` on `NotFound` | Preserves empty repo semantics | Zero impact (fully compatible) | Recommended |
| **Non-Directory Component** | Swallowed as `Ok(([], None))` | Fails closed with `StorageErrorKind::CorruptData` | Masks filesystem corruption | Breaking change for corrupt repos (fail closed) | **Yes** |
| **Permission Denied (`EACCES`)** | Swallowed as `Ok(([], None))` | Fails closed with `StorageErrorKind::PermissionDenied` | Prevents false empty listings | Breaking change for unreadable dirs (fail closed) | **Yes** |
| **Raw SHA-512 Filenames** | Silently omitted (parser bug) | Parsed as `sha512:<hex>` | Standard writers produce SHA-512 manifests | Fixes bug; manifests discoverable | **Yes** |
| **Prefixed / Uppercase Filenames** | Parsed and normalized to lowercase `Digest` | Filtered out (strict raw hex only) | Files with prefixes or uppercase cannot be opened on Linux | Ignores unreadable non-canonical files | **Yes** |
| **Entry Type Filtering** | Unchecked; dirs & symlinks listed | Only `DirEntryType::Regular` accepted | Symlinks/dirs fail on `get_manifest` | Non-regular entries excluded | **Yes** |
| **Ordering & Comparator** | Sorted by `hex()`, searched by `as_str()` | Sorted by `sort_unstable()`, searched by whole string | Fixes broken pagination and omissions on mixed algorithms | Restores binary search partition invariant without comparator string allocation | **Yes** |
| **Duplicate Entries** | Retained duplicates | Deduplicated via `all_digests.dedup()` | Prevents inflating reference counts | Clean deduplicated output | **Yes** |
| **Arithmetic Overflow** | Unchecked `start_idx + page_limit` | Saturating addition `start_idx.saturating_add(...)` | Prevents panic on `usize::MAX` | Eliminates panic vulnerability | **Yes** |
| **Directory Budgets** | Unbounded directory read | Bounded by candidate limits (10k entries, 1.5MB names) | Protects heap memory against unbounded growth | Fails closed with `LimitExceeded` | **Yes** |

### 5.2 Rollback Scope & Operating Guarantees
- **No Data Migration**: This design modifies read-only listing. It introduces no storage schema migrations or file format changes.
- **Rollback Procedure**: Reverting `src/storage/fs.rs` delegates back to legacy pathname enumeration.
- **Rollback Limitations**:
  - Rollback does not undo writes, deletions, or other mutations performed while the cutover was active.
  - Deployment/restart requirements depend on the operating environment (no zero-downtime promises).

### 5.3 Explicit Architectural Limitations Restored
1. **No Mount or Hard-Link Isolation**: Linux `openat2` with `RESOLVE_BENEATH` prevents escaping the pinned root descriptor, but does not isolate child mounts attached beneath the root, nor does it track hard-link aliases.
2. **No Snapshot Isolation**: Directory iteration reflects concurrent filesystem modifications. An observed entry may disappear or be replaced before a subsequent read.
3. **No Guaranteed Subsequent Readability**: Canonical filename parsing aligns listing output with the canonical read path, but permissions, deletion races, corrupt JSON payloads, or I/O failures may cause subsequent `get_manifest` reads to fail.
4. **Pinned Descriptor vs. Pathname Mutation Coherence**: Renaming or replacing ancestor directories above the storage root separates pinned-reader operations from pathname-based mutations.
5. **Procfs Relevance**: Genuine, accessible procfs (`/proc/self/fd/N`) is required by Phase 2 payload reads (`open_payload`), but is **not required** by directory enumeration (`enumerate_dir`), which operates directly on the directory descriptor via `fdopendir`.
6. **Non-Linux Unverified**: Descriptor-relative containment requires Linux `openat2`. Non-Linux platforms remain unverified and return `PlatformUnsupported`.

### 5.4 Canonical Quality Gates
All eight canonical quality gates remain explicitly **OPEN**:
- **O-03**: Key and continuation-token contracts.
- **O-04**: Filesystem write durability and containment.
- **O-05**: Broader filesystem read containment.
- **O-06**: Typed AWS mapping and pinned-MinIO evidence.
- **O-13**: Hosting, distribution, and release strategy.
- **O-15**: Non-Linux verification.
- **O-16**: Earlier Slice 11 audit/test-inventory evidence.
- **D-06**: Broader extraction, cutover, compatibility, and distribution acceptance.

---

## 6. Document Metadata & Preserved Baselines

- Author: Antigravity Agent
- Date: 2026-09-11
- Mode: Design Only — Not Authorized for Implementation or Commit
