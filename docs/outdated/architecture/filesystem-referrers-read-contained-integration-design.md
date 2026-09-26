# Contained Filesystem Referrers Read Integration Design

## Executive Summary & Baseline

This document presents the corrected architectural design for a bounded, contained filesystem referrers-read integration in `registry-rust`. Following the empirical characterization established in [filesystem-referrers-read-characterization.md](docs/architecture/filesystem-referrers-read-characterization.md) (committed at `2419d46e89d88151c18972210ba79826bf776b59`), this design specifies the integration of Linux descriptor-relative containment (`openat2`) using existing `storage-core` and `storage-fs` abstractions, evaluates compatibility trade-offs across storage, application, and mutation boundaries, and defines a strictly bounded test-only seam as the recommended immediate implementation slice.

This task is **analysis and documentation only**. Production code, storage traits, mutation workflows, HTTP routing, and configuration remain 100% byte-for-byte unchanged.

> **Implementation Status Addendum (2026-09-12):** The contained referrers read described here has
> since been implemented and promoted to production in the current working tree, combining Slice 1
> (contained read helper `src/storage/fs/referrers_read.rs`) and Slice 2 (production routing of
> `FsStorage::list_referrers`) in one reviewed batch with mutation-read compatibility tests.
> Decision resolutions and observable compatibility changes are recorded in
> [filesystem-referrers-read-production-cutover.md](filesystem-referrers-read-production-cutover.md).
> Slice 3 (pagination policy for `list_referrers_page`) and Slice 4 (write containment, Gate O-04)
> remain open. The "PENDING REVIEW" decision rows below reflect the state at authoring time.

### Verified Repository Baselines

- **Primary Repository**: `~/devel/rust/registry-rust`
  - Current HEAD: `2419d46e89d88151c18972210ba79826bf776b59`
  - Latest Commit: `test(storage): characterize filesystem referrers read semantics`
  - Active Branch: `master`
  - Index & Worktree Status: Clean
- **Dependency Repository**: `~/devel/rust/storage-layer-rust`
  - Current HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`
  - Active Branch: `main`
  - Read-Only Status: Preserved strictly read-only throughout.
- **Authoritative Characterization Evidence**:
  - Archive: `~/devel/rust/manifest-read-review-evidence/session-20260913-0005/filesystem-referrers-read-characterization.tar.gz`
  - Archive Size: 89,087 bytes | SHA-256: `97ad50ac6200b5e2c7d26b85a2afaf69f2604eb3e86325e6aef49e6af3ca5d39`
  - Committed Sources:
    - [src/storage/fs/tests.rs](src/storage/fs/tests.rs): `02156e6eb7648719ced2f61d3d143de8387676ece9c670584f0a66a510034763`
    - [docs/architecture/filesystem-referrers-read-characterization.md](docs/architecture/filesystem-referrers-read-characterization.md): `76d06bc1810f8448134a14320aa5b395e8ade89523bb65ee6272ed5d0db314ea`
- **Related Prior Assessment**:
  - Archive: `~/devel/rust/manifest-read-review-evidence/session-20260912-2345/filesystem-read-containment-post-tag-listing-assessment.tar.gz`
  - Archive Size: 39,799 bytes | SHA-256: `d99b526451e8b8c8c7cbfc37dfaa16afdcc68ce983262da1e3bde1d5b63dda05`

---

## Current Behavior & Call-Chain Evidence

OCI referrers reads in `registry-rust` involve multiple layers: the public HTTP API, the application query service, storage traits and forwarding adapters, and filesystem storage mutation methods.

### 1. Direct Public Query Handling

The public OCI route for referrers is defined by the OCI Image Specification (`GET /v2/<name>/referrers/<digest>`).

```
[Client Request: GET /v2/<name>/referrers/<digest>]
                      │
                      ▼
  http_api::referrers::referrers_list
      ├─ is_valid_repo_name(name) -> HTTP 400 NAME_INVALID
      ├─ Digest::parse(digest_str) -> HTTP 400 DIGEST_INVALID
      │
      ▼
  ReferrersQueryService::query_referrers
      ├─ CanonicalRepoName::parse(repo) -> ReferrersQueryError::InvalidRepoName
      ├─ reader.list_referrers(repo, subject).await  <─── [Direct Storage Call]
      │     ├─ Ok(entries) -> processes entries
      │     ├─ Err(StorageError::NotFound) -> treats as empty: Vec::new()
      │     └─ Err(e) -> returns Err(ReferrersQueryError::Storage(e))
      ├─ entries.retain(|d| d.artifact_type == filter)
      ├─ entries.sort_by(|a, b| a.digest.cmp(&b.digest))  <─── [Deterministic Sort]
      ├─ Slices page using 'last' cursor via linear .position()
      └─ Returns ReferrersPage { descriptors, has_more, next_last }
                      │
                      ▼
  http_api::referrers response construction
      ├─ Formats OCI Image Index JSON (application/vnd.oci.image.index.v1+json)
      ├─ Formats RFC 8288 Link header for next page if has_more is true
      └─ Translates storage errors:
            ├─ InvalidRepoName -> HTTP 400 NAME_INVALID
            ├─ Unsupported -> HTTP 501 UNSUPPORTED
            ├─ InsufficientStorage -> HTTP 507 INSUFFICIENT_STORAGE
            └─ All other errors (Io, CorruptData, Backend) -> HTTP 500 INTERNAL_ERROR
```

Authoritative Source References:
- Handler: [src/http_api/referrers.rs:25-162](src/http_api/referrers.rs#L25-L162)
- Service: [src/application/referrers.rs:36-95](src/application/referrers.rs#L36-L95)

Key Findings:
1. **Public Query Service Calls `list_referrers` Directly**: The application service delegates directly to `reader.list_referrers(repo, subject)`. It does **not** call `list_referrers_page`.
2. **Error Propagation**: The application service fails closed on storage errors (other than `NotFound`), propagating them to the HTTP handler which returns HTTP 500. Corrupted JSON, permission denial, or I/O errors are never swallowed on the public HTTP route.
3. **Application Pagination & Ordering**: Slicing, filtering, and lexicographical sorting are performed entirely in-memory within `ReferrersQueryService`, independent of any storage pagination mechanism.

---

### 2. Paged Method Status & Audit

The paged storage read method `list_referrers_page` exhibits significant divergence from the direct read method:

```rust
// src/storage/fs.rs:1232-1261
async fn list_referrers_page(
    &self,
    repo: &str,
    subject: &Digest,
    continuation_token: Option<&str>,
    page_limit: usize,
) -> Result<(Vec<ReferrerDescriptor>, Option<String>), StorageError> {
    let mut refs = self.list_referrers(repo, subject).await.unwrap_or_default();
    refs.sort_by(|a, b| a.digest.cmp(&b.digest));

    let start_idx = if let Some(token) = continuation_token {
        match refs.binary_search_by(|r| r.digest.as_str().cmp(token)) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        }
    } else {
        0
    };

    let end_idx = (start_idx + page_limit).min(refs.len());
    let page_slice = &refs[start_idx..end_idx];

    let next_token = if end_idx < refs.len() {
        page_slice.last().map(|r| r.digest.clone())
    } else {
        None
    };

    Ok((page_slice.to_vec(), next_token))
}
```

Key Findings from Audit and Characterization:
1. **Unconditional Error Suppression (`unwrap_or_default`)**: By calling `.unwrap_or_default()`, `list_referrers_page` converts any disk read error, permission denial, or corrupted JSON into an empty vector (`Ok((vec![], None))`). A corrupted index file is indistinguishable from a subject with zero referrers.
2. **Arithmetic & Slicing Panics**: Slicing arithmetic `end_idx = (start_idx + page_limit).min(refs.len())` performs unchecked addition. When `start_idx > 0` and `page_limit == usize::MAX`, it panics with an overflow panic under the standard `test` build profile (`overflow-checks = true`), and wraps to `0` with a subsequent slice bounds panic (`&refs[start_idx..0]`) under release profiles without overflow checks.
3. **Duplicate Digest Advancement**: Rust's standard library `binary_search_by` makes an unspecified choice among equal elements. In `list_referrers_page`, duplicate digests cause non-deterministic token advancement.
4. **Zero Active Production Callers**: An exhaustive audit of the codebase confirms:
   - Trait declarations on `Storage` ([src/storage/mod.rs:431](src/storage/mod.rs#L431)) and `ReferrersReader` ([src/storage/ports/mod.rs:123](src/storage/ports/mod.rs#L123)).
   - Forwarding adapter implementations in `Arc<dyn Storage>`, `Arc<dyn ReferrersReader>`, and macro `impl_referrers_reader!`.
   - Dead forwarding methods in `supervisor.rs:1705`, `manifest_lifecycle.rs:1904`, and `blob_ref_index.rs:1268` have **zero call sites**.
   - The method is only exercised in test mock harnesses and live S3 integration tests.
   - Therefore, the flaws in `list_referrers_page` are latent capability defects with **zero production reachability**.

---

### 3. Mutation Callers, Ordering, and Failure Boundaries

Referrers reads are also consumed internally during write and deletion workflows. The exact sequence of operations and failure boundaries must be traced precisely without assuming atomic rollback or untouched filesystems.

#### A. `FsStorage::add_referrer` ([src/storage/fs.rs:1736-1755](src/storage/fs.rs#L1736-L1755))
```rust
async fn add_referrer(
    &self,
    name: &str,
    subject: &Digest,
    descriptor: ReferrerDescriptor,
) -> Result<(), StorageError> {
    let _lock = self.referrer_lock_shard(name, subject).lock().await;
    let dir = self.root.join("repos").join(name).join("referrers");
    ensure_dir(&dir)?;

    let path = self.referrers_path(name, subject);
    let mut existing = self.list_referrers(name, subject).await?;
    if !existing.iter().any(|d| d.digest == descriptor.digest) {
        existing.push(descriptor);
    }

    let bytes = serde_json::to_vec(&existing)
        .map_err(|err| StorageError::serialization(err.to_string()))?;
    atomic_write_file(&path, &bytes).await?;
    Ok(())
}
```
Exact Operation Ordering & Failure Boundaries:
1. **Actions before the read**:
   - Acquires the in-memory shard lock: `self.referrer_lock_shard(name, subject).lock().await`.
   - Resolves ambient directory path: `self.root.join("repos").join(name).join("referrers")`.
   - **Calls `ensure_dir(&dir)?` to create the directory tree before calling `list_referrers`**.
   - Consequence: If `ensure_dir` succeeds, directory creation has physically occurred on disk. If a subsequent read step fails (or if a future contained reader rejects `name`), earlier directory creation is **not** rolled back.
2. **Read call & error propagation**:
   - Calls `self.list_referrers(name, subject).await?`.
   - Propagates any error (`?`) immediately, aborting the method.
   - Fail-closed invariant: If `list_referrers` returns `Err`, `add_referrer` halts without overwriting the file. If `list_referrers` were to swallow errors (like `list_referrers_page`), an existing corrupted file would be overwritten with only `[descriptor]`, permanently destroying corrupted data without notice.
3. **Actions after successful reading**:
   - Checks for duplicate digest; appends `descriptor` if not present.
   - Serializes updated vector: `serde_json::to_vec(&existing)`.
   - Calls `atomic_write_file(&path, &bytes).await?` to write to a temporary file, fsync, and rename.
4. **Ignored mutation errors**: None in `add_referrer`.
5. **Partial-work boundaries**:
   - If `ensure_dir` fails: aborts before the read is attempted. `ensure_dir` uses `std::fs::create_dir_all`, which can partially succeed before returning an error (creating some ancestor directories) and can succeed by reusing directories that already exist. Directories created before the failure are **not** rolled back.
   - If `list_referrers` fails: `repos/<name>/referrers/` directory hierarchy remains on disk; target referrers file was not created or modified.
   - If serialization or `atomic_write_file` fails: directories remain created; temporary files may be removed or left behind depending on atomic write error state; target file remains unchanged.

#### B. `FsStorage::remove_referrer` ([src/storage/fs.rs:1757-1785](src/storage/fs.rs#L1757-L1785))
```rust
async fn remove_referrer(
    &self,
    name: &str,
    subject: &Digest,
    referrer: &Digest,
) -> Result<(), StorageError> {
    let _lock = self.referrer_lock_shard(name, subject).lock().await;
    let path = self.referrers_path(name, subject);
    let mut existing = self.list_referrers(name, subject).await?;
    let orig_len = existing.len();
    let referrer_str = referrer.as_str();
    existing.retain(|d| d.digest != referrer_str);
    if existing.len() == orig_len {
        return Ok(());
    }

    if existing.is_empty() {
        let _ = tokio::fs::remove_file(&path).await;
    } else {
        let bytes = serde_json::to_vec(&existing)
            .map_err(|err| StorageError::serialization(err.to_string()))?;
        atomic_write_file(&path, &bytes).await?;
    }
    Ok(())
}
```
Exact Operation Ordering & Failure Boundaries:
1. **Actions before the read**:
   - Acquires the in-memory shard lock: `self.referrer_lock_shard(name, subject).lock().await`.
   - Computes ambient path: `self.referrers_path(name, subject)`.
2. **Read call & error propagation**:
   - Calls `self.list_referrers(name, subject).await?`.
   - Propagates read errors via `?`. If reading fails (e.g. corrupted JSON or permission denied), aborts immediately without attempting removal or writeback.
3. **Actions after successful reading**:
   - Retains descriptors not matching `referrer_str`.
   - If `existing.len() == orig_len`: returns `Ok(())` (no-op, referrer wasn't registered).
   - If `existing.is_empty()`: **attempts file unlinking via `let _ = tokio::fs::remove_file(&path).await;`**.
   - Else: serializes `existing` and calls `atomic_write_file(&path, &bytes).await?`.
4. **Ignored mutation errors**:
   - The result of `tokio::fs::remove_file(&path)` is **explicitly ignored** (`let _ = ...`). This is an **attempted unlink**, not a guaranteed successful deletion. If `remove_file` fails (e.g. permission error, file disappeared, read-only filesystem), `remove_referrer` suppresses the error and returns `Ok(())`.
5. **Partial-work boundaries**:
   - If `list_referrers` fails: aborts immediately; no file modifications attempted.
   - If `remove_file` fails: error suppressed; returns `Ok(())` even though the file still exists on disk.
   - If `atomic_write_file` fails: returns error; referrers file remains in pre-removal state.

#### C. `FsStorage::delete_manifest` ([src/storage/fs.rs:1787-1830](src/storage/fs.rs#L1787-L1830))
```rust
async fn delete_manifest(&self, name: &str, digest: &Digest) -> Result<(), StorageError> {
    let manifest_path = self.manifest_path(name, digest);

    // Pre-read manifest bytes to extract subject if present for referrers cleanup.
    let bytes = match tokio::fs::read(&manifest_path).await {
        Ok(b) => b,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(StorageError::NotFound);
        }
        Err(err) => return Err(StorageError::io(err.to_string())),
    };

    let maybe_subject = crate::manifest_refs::extract_subject_digest(&bytes).map_err(|e| {
        StorageError::corrupt_data(format!(
            "cannot delete manifest with malformed structure: {e}"
        ))
    })?;

    match tokio::fs::remove_file(&manifest_path).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(StorageError::NotFound);
        }
        Err(err) => return Err(StorageError::io(err.to_string())),
    }

    // Remove any tags pointing to this digest.
    let digest_str = digest.as_str();
    for path in self.list_tag_files(name).await? {
        let content = match tokio::fs::read_to_string(&path).await {
            Ok(s) => s,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(StorageError::io(err.to_string())),
        };
        if content.trim() == digest_str {
            let _ = tokio::fs::remove_file(&path).await;
        }
    }

    // Clean up from referrers list if this manifest referenced a subject.
    if let Some(subject) = maybe_subject {
        let _ = self.remove_referrer(name, &subject, digest).await;
    }

    Ok(())
}
```
Exact Operation Ordering & Failure Boundaries:
1. **Actions before `remove_referrer`**:
   - Reads manifest bytes: `tokio::fs::read(&manifest_path).await` (fails if NotFound or I/O error).
   - Parses manifest to extract optional subject: `extract_subject_digest(&bytes)?` (fails if malformed JSON structure).
   - **Removes the manifest file from disk**: `tokio::fs::remove_file(&manifest_path).await?`. The manifest file is unlinked at this step.
   - Scans tag files and unlinks tags pointing to this digest (`let _ = tokio::fs::remove_file(&path)`).
2. **Referrer cleanup attempt**:
   - **Only after manifest removal and tag unlinking**, if `maybe_subject` is `Some(subject)`, it calls `self.remove_referrer(name, &subject, digest).await`.
3. **Ignored mutation errors**:
   - The result of `remove_referrer` is **explicitly ignored** (`let _ = ...`). If `remove_referrer` fails (e.g. because `list_referrers` fails on a corrupted referrers file, or uncontained traversal is rejected), `delete_manifest` suppresses the error and returns `Ok(())`.
4. **Partial-work boundaries**:
   - Manifest deletion and tag unlinking have already succeeded before `remove_referrer` is invoked.
   - If `remove_referrer` fails, the manifest and tags remain deleted; the referrers index file remains unmodified. No atomic rollback is attempted.

---

## Containment Architecture

### 1. Reuse of Shared Storage Reader Instance

In production `FsStorage` ([src/storage/fs.rs:190-202](src/storage/fs.rs#L190-L202)), the instance holds:
```rust
pub struct FsStorage {
    root: PathBuf,
    ...
    reader: std::sync::Arc<storage_fs::FsMetadataReader>,
    ...
}
```
- The extracted crate `storage-fs` provides `FsMetadataReader`, which implements both [`storage_core::ObjectMetadataReader`] and [`storage_core::ObjectPayloadReader`].
- In Linux environments, `FsMetadataReader` opens a pinned directory file descriptor to the configured root (`root_fd`) at initialization, capability-probes `openat2` resolution, and executes two-phase payload acquisition:
  - Phase 1: Resolves relative path beneath `root_fd` with flags `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS` using `O_PATH`.
  - Phase 2: Validates regular file status (`S_IFREG`) and reopens a readable file descriptor via `/proc/self/fd/N`.
- For referrers reads, the contained implementation can directly reuse `storage_core::ObjectPayloadReader` through `self.reader.as_ref()`.
- For test evaluation, the seam function accepts `reader: &(impl ObjectPayloadReader + ?Sized)`, allowing tests to inject mock readers, recording fakes, or the live `FsMetadataReader`.

---

### 2. Relative Key Construction & Subject Algorithm Support

The ambient path is:
`<root>/repos/<repo>/referrers/<subject.hex()>.json`

The corresponding domain-neutral [`storage_core::ObjectKey`] is:
`repos/<repo>/referrers/<subject.hex()>.json`

#### Key Construction & Pre-Composition Validation Rules
Before composing the key string, the inputs must be validated:
1. **Repository Name Validation (`repo`)**:
   - Must not be empty.
   - Must not start or end with `/`.
   - Must not contain backslashes (`\`), NUL bytes (`\0`), or ASCII control characters.
   - Must not contain empty path segments (`//`).
   - Must not contain `.` (current directory) or `..` (parent traversal) segments.
   - Any violation is rejected immediately with `StorageError::InvalidRepoName` before reader invocation.
2. **Subject Digest Validation (`subject`)**:
   - `subject: &Digest` is strongly typed.
   - `subject.hex()` contains lowercase hexadecimal characters (`[0-9a-f]`).
   - OCI Image Spec allows SHA-256 (64 hex characters) and SHA-512 (128 hex characters). Characterization confirmed both formats produce valid filenames `<64hex>.json` and `<128hex>.json`.
3. **Key String Composition**:
   ```rust
   let key_str = format!("repos/{repo}/referrers/{}.json", subject.hex());
   let object_key = ObjectKey::parse(&key_str)
       .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
   ```

---

### 3. Asynchronous Payload Acquisition & Bounded Draining

Acquisition and stream draining follow the established pattern from `tag_read.rs` and `payload_seam.rs`:

```rust
let payload = match reader.open_payload(&key).await {
    Ok(p) => p,
    Err(storage_core::ReadError::NotFound { .. }) => return Ok(Vec::new()),
    Err(err) => return Err(super::read_adapter::translate_payload_read_error(err)),
};
```

#### Stream Draining Pipeline
```rust
let (_metadata, stream) = payload.into_parts();

let bytes = match limits.max_payload_bytes {
    None => {
        let mut buffer = Vec::new();
        let mut pinned = stream;
        pinned.read_to_end(&mut buffer).await
            .map_err(|e| StorageError::io(format!("failed to read referrers stream: {e}")))?;
        buffer
    }
    Some(limit) => {
        let take_limit = limit.checked_add(1).ok_or_else(|| {
            StorageError::corrupt_data(format!(
                "payload limit {limit} cannot be represented for bounded draining"
            ))
        })?;
        let mut limited_stream = stream.take(take_limit);
        let mut buffer = Vec::new();
        limited_stream.read_to_end(&mut buffer).await
            .map_err(|e| StorageError::io(format!("failed to read referrers stream: {e}")))?;
        if buffer.len() as u64 > limit {
            return Err(StorageError::corrupt_data(format!(
                "referrers payload stream length exceeds limit of {limit} bytes"
            )));
        }
        buffer
    }
};
```

#### Zero-Byte and Limit Edge-Case Contracts
- **Missing File**: Handled during acquisition (`ReadError::NotFound`); immediately returns `Ok(Vec::new())` without stream draining, parsing, or count checks.
- **Existing 0-byte File with `max_payload_bytes = Some(0)`**: `take_limit = 1`. `read_to_end` collects 0 bytes. Length check `0 > 0` is false (passes byte check). Next, Serde deserialization `serde_json::from_slice(&[])` fails with EOF. Under the proposed legacy mapping (DEC-04), it returns `StorageError::Internal { kind: Io, .. }`.
- **Existing Non-empty File with `max_payload_bytes = Some(0)`**: `take_limit = 1`. `read_to_end` collects 1 byte. Length check `1 > 0` is true; immediately returns `Err(StorageError::corrupt_data("... exceeds limit of 0 bytes"))` **before** parsing.
- **Empty JSON Array `[]`**: Occupies 2 bytes. With `Some(0)`, fails payload overflow before parsing. With `Some(2)` or `None`, passes byte check and deserializes to `vec![]` (length 0).
- **`max_payload_bytes = Some(u64::MAX)`**: `u64::MAX.checked_add(1)` returns `None`. Immediately returns `Err(StorageError::corrupt_data("payload limit 18446744073709551615 cannot be represented for bounded draining"))` before stream reading.

---

### 4. Serde Deserialization & Allocation Realities

```rust
let descriptors: Vec<ReferrerDescriptor> = serde_json::from_slice(&bytes)
    .map_err(|e| StorageError::io(e.to_string()))?;

if let Some(max_descs) = limits.max_descriptors {
    if descriptors.len() > max_descs {
        return Err(StorageError::corrupt_data(format!(
            "referrers count {} exceeds configured limit of {}",
            descriptors.len(), max_descs
        )));
    }
}
```

Allocation & Resource Realities:
1. **Buffer vs Vector Capacity**: Bounded draining collects at most $N + 1$ bytes into a `Vec<u8>`. Vector capacity and allocator chunking overhead may exceed the collected length.
2. **Coexisting Allocations**: During and after deserialization, the raw byte vector buffer and the deserialized `Vec<ReferrerDescriptor>` (including heap-allocated strings for `media_type`, `digest`, `artifact_type`, and `HashMap` entries for `annotations`) coexist in memory simultaneously.
3. **Post-Parse Count Check**: `max_descriptors` is evaluated **after full deserialization**. It rejects the result if the count exceeds the threshold, but does **not** prevent peak memory allocations during Serde parsing. `max_payload_bytes` bounds only the bytes *collected* from the payload stream (at most $N + 1$); it is not a precise peak-memory or peak-allocation bound, because vector capacity growth, allocator overhead, and the simultaneous retention of the raw buffer and parsed structures add costs beyond the collected byte count.
4. **No Global Budget**: Per-read limits bound an individual read operation; they do not establish a global memory or concurrency budget.
5. **No Decompression**: There is no decompression step in the referrers JSON pipeline; claims of decompression-bomb protection are inapplicable.
6. **Concurrent Writes & JSON Validity**: Concurrent modifications during stream draining can produce truncated bytes (failing parsing), or changed but syntactically valid JSON. Parsing verifies schema syntax, not snapshot consistency.

---

### 5. Concurrency, Containment, and Coherence Demarcation

#### What Containment Guarantees:
- **Root Traversal Confinement**: Linux `openat2` with `RESOLVE_BENEATH` prevents pathname traversal out of the storage root, even if intermediate symlinks exist.
- **Symlink Neutralization**: `RESOLVE_NO_SYMLINKS` rejects symlinks at any path component, preventing escape to external files or devices.
- **Magiclink Neutralization**: `RESOLVE_NO_MAGICLINKS` rejects traversal through procfs magic links (e.g. `/proc/self/fd/..`).
- **Regular File Enforcement**: Phase 2 verifies `S_IFREG`, rejecting directories, character devices, FIFOs, and sockets.

#### What Containment Does NOT Guarantee:
- **No Snapshot Isolation**: Once opened, reading the file stream is not atomic. Concurrent writes or file truncations may return partial bytes or inconsistent states.
- **No Concurrent Replacement Invariant**: Descendants beneath the root descriptor are resolved afresh on each `open_payload`. If a directory segment is renamed or replaced between operations, subsequent reads resolve the new path.
- **No Namespace Snapshot on Root Replacement**: The root descriptor refers to the directory inode opened at startup. If the host filesystem replaces the root directory on disk, contained reads continue to resolve beneath the original inode, while uncontained mutations operate on the newly placed directory.
- **No Hard Link Isolation**: If a hard link inside the storage tree refers to an inode outside the root, `openat2` resolves to that inode without containment violation.
- **No Mount Isolation**: Resolution crosses mount points within the tree unless `RESOLVE_NO_XDEV` is specified.
- **Non-Linux Limitations**: Non-Linux environments lack `openat2` and return `StorageErrorKind::Configuration`. Non-Linux verification remains unperformed.

---

## Compatibility Decision Table

| Decision ID | Focus Area | Current Characterized Behavior | Evaluated Alternatives | Recommended Path | Caller Impact | Required Evidence | Status |
|---|---|---|---|---|---|---|---|
| **DEC-01** | Repository Validation Contract | Storage concatenates ambient paths without validation; repo traversal (e.g. `../`) escapes `repos/` if intermediate dirs exist. Query service enforces `CanonicalRepoName::parse(repo)`. | **Alt A**: Structural path safety checks (`validate_path_component`).<br>**Alt B**: Strict `CanonicalRepoName` grammar in storage.<br>**Alt C**: Pure kernel `openat2` rejection without pre-checks. | **Alt A (Structural Path Safety)**: Enforce structural rules (no `..`, no `/` prefix/suffix, no control chars). Keeps storage decoupled from OCI registry naming rules. | Valid repo names unchanged; storage rejects path traversal early with `InvalidRepoName`. | Unit tests for valid names, multi-segment names, and traversal rejections. | **PENDING REVIEW** |
| **DEC-02** | Missing Paths & Probes | Missing repository directory, missing `referrers/` dir, or missing `<hex>.json` returns empty vector `Ok(vec![])`. | **Alt A**: `ReadError::NotFound` maps to `Ok(Vec::new())` without repository probes.<br>**Alt B**: Probe repository directory; return error if repo missing, empty if only referrers file missing. | **Alt A (No Repo Probes)**: OCI Referrers API defines non-existent referrers as empty list. Avoids expensive directory stat probes on the read path. | 100% compatible with existing callers. | Characterization tests `test_missing_repository`, `test_missing_referrers_dir`, `test_missing_subject_file`. | **PENDING REVIEW** |
| **DEC-03** | Error Taxonomy Mapping | All disk acquisition and permission errors currently map to `StorageError::Internal { kind: StorageErrorKind::Io, .. }`. | **Alt A**: Standardize on `read_adapter::translate_payload_read_error`.<br>**Alt B**: Map all non-NotFound errors to `StorageErrorKind::Io`. | **Alt A (Standardized Adapter)**: Maps `NotFound` -> `NotFound`, permission/stat/symlink rejection -> `Io`, unsupported syscall -> `Configuration`, runtime errors -> `Backend`. | HTTP status remains HTTP 500 for internal errors; provides clearer diagnostic messages. | Test coverage across permission denial, symlink rejection, EISDIR. | **PENDING REVIEW** |
| **DEC-04** | Parsing & Deserialization Errors | `serde_json` deserialization errors map to `StorageError::io(err.to_string())` (`StorageErrorKind::Io`). | **Alt A**: Maintain legacy `StorageError::io(...)`.<br>**Alt B**: Map deserialization failures to `StorageError::corrupt_data(...)`. | **Alt A (Legacy Io)** for initial contained seam; propose **Alt B (CorruptData)** for production cutover evaluation. | Public query service and HTTP handler return HTTP 500 under both options. | Verifies `test_invalid_json_syntax`, `test_truncated_json`, `test_wrong_toplevel_json_type`. | **PENDING REVIEW** |
| **DEC-05** | Resource Ceilings (Bytes & Descriptors) | Unbounded read and deserialization; no payload byte ceiling, no descriptor count ceiling. | **Alt A**: `ReferrersReadLimits` with `Option<u64>` and `Option<usize>`, defaulting to `None` (unbounded).<br>**Alt B**: Enforce hardcoded numeric defaults.<br>**Alt C**: Couple to `TagReadLimits`. | **Alt A (Explicit Optional Limits)**: Default `None` preserves exact characterization baseline. Seam allows configuring explicit limits without imposing unreviewed defaults. | `None` leaves existing callers unchanged; bounded callers gain protection against stream memory exhaustion. | Limit-plus-one stream overflow tests; descriptor count threshold tests. | **PENDING REVIEW** |
| **DEC-06** | Direct Read Ordering | `FsStorage::list_referrers` preserves the exact physical sequence of descriptors from the stored JSON array. | **Alt A**: Strictly preserve stored descriptor array order in the direct contained reader.<br>**Alt B**: Sort descriptors lexicographically by digest in storage. | **Alt A (Preserve Storage Order)**: Preserves physical fidelity. Query service sorts in-memory; mutation callers depend on append-order stability. | Zero divergence from characterization baseline. | Characterization test `test_valid_multi_descriptor_ordering_and_fields`. | **PENDING REVIEW** |
| **DEC-07** | Paged Error Suppression Policy | `FsStorage::list_referrers_page` unconditionally swallows all read errors via `.unwrap_or_default()`, returning `Ok((vec![], None))`. | **Alt A**: Retain error suppression in `list_referrers_page` for initial seam.<br>**Alt B**: Make `list_referrers_page` fail closed.<br>**Alt C**: Deprecate/remove `list_referrers_page` (0 active callers). | **Alt A** for initial seam (no changes to `list_referrers_page`). Propose **Alt C** (deprecation/removal) for later refactoring slice. | Zero production impact (no production callers). | Test inventory maintains distinction between direct error propagation and paged suppression. | **PENDING REVIEW** |
| **DEC-08** | Pagination Arithmetic & Duplicates | `(start_idx + page_limit).min(refs.len())` panics on overflow. Binary search among duplicate digests makes unspecified choice. | **Alt A**: Leave `list_referrers_page` unchanged in read-seam slice.<br>**Alt B**: Fix saturating arithmetic and duplicate handling in `list_referrers_page`. | **Alt A (Unchanged in Initial Seam)**: Keep initial slice strictly bounded to contained read abstraction. Address pagination arithmetic in a separate pagination slice. | Zero production impact. | Complete-method panic observation remains documented. | **PENDING REVIEW** |
| **DEC-09** | Mutation Seam Integration Precondition | Mutations (`add_referrer`, `remove_referrer`) call `self.list_referrers` directly. Shared-helper promotion changes mutation read behavior immediately. | **Alt A**: Confine initial contained referrers read to an isolated test-only helper module; do NOT promote `FsStorage::list_referrers` without explicit mutation compatibility analysis.<br>**Alt B**: Promote `list_referrers` immediately.<br>**Alt C**: Introduce separate query-only storage port. | **Alt A (Decoupled Initial Seam)**: Do not promote `list_referrers` in initial slice. Production promotion requires explicit mutation-caller compatibility review and approval. | Mutations remain 100% unchanged during read-seam evaluation. | Mutation regression test `referrers_add_list_remove_and_delete_manifest` verified. | **PENDING REVIEW** |

---

## Mutation-Caller Implications & Integration Preconditions

### Shared-Helper Promotion Changes Mutation Reads Immediately

Because `add_referrer` and `remove_referrer` call `self.list_referrers` directly, **promoting `FsStorage::list_referrers` to use the contained reader is NOT a query-only change**. It immediately alters the read semantics of mutation workflows:

1. **Path Traversal Rejection**:
   - If a caller calls `add_referrer("../escaped", ...)`, the directory `repos/../escaped/referrers/` is created by `ensure_dir` *before* `list_referrers`. When `read_referrers_contained` rejects the repository name, `add_referrer` fails closed with `StorageError::InvalidRepoName`, but the earlier directory creation is **not** rolled back.
2. **Symlink Rejection on Read**:
   - If `repos/<repo>/referrers/<hex>.json` is a symlink, `openat2` rejects it with `ResolutionRejected` (`StorageErrorKind::Io`). In `add_referrer` and `remove_referrer`, the `?` operator aborts the mutation, preventing reading or overwriting the symlink target.
3. **Payload Limit Rejection**:
   - If a byte ceiling is configured and an existing referrers file exceeds that ceiling, `list_referrers` fails with `StorageErrorKind::CorruptData`. `add_referrer` aborts via `?`, preventing reading or overwriting the oversized file.
4. **Distinction from Write Containment (Gate O-04)**:
   - Contained reading hardens the pre-read step of mutations. However, `add_referrer` and `remove_referrer` continue to execute uncontained writes via `atomic_write_file`.
   - Accepting changed mutation-associated read behavior is distinct from implementing broader write containment and fsync durability (Gate O-04). While write containment remains open under O-04, approving contained reads for mutations does not strictly require full write containment first, provided the read behavior change is analyzed, verified, and explicitly approved.
5. **Alternative Query-Only Routing**:
   - If the project desires query containment before approving mutation read changes, a separate query-only routing path (e.g. having `ReferrersQueryService` consume a dedicated contained reader port while `FsStorage::list_referrers` remains legacy) could be designed. That alternative would require interface and routing design and is not invented or approved here.

---

## Bounded Slice Sequence

To maintain reviewability and safety, the transition from characterization to production containment must follow a disciplined, staged progression:

```
┌────────────────────────────────────────────────────────────────────────┐
│ Slice 1: Test-Only Contained Referrers Read Seam [RECOMMENDED NEXT]   │
│ - Module declaration: #[cfg(test)] #[path = "fs/referrers_read.rs"]    │
│ - Evaluates openat2 containment, key construction, bounded draining    │
│ - Zero production entry point modifications                            │
│ - Characterization tests and new acceptance tests                      │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ (Review, Mutation Decision & Approval)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Slice 2: Production Referrers Read Cutover                             │
│ - Requires explicit approval of mutation-read compatibility            │
│ - Wire FsStorage::list_referrers to use read_referrers_contained       │
│ - Configure ReferrersReadLimits via FsStorage constructors             │
│ - Verify public query service and mutation regression suites           │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ (Review & Approval)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Slice 3: Pagination Port Evaluation / Deprecation                      │
│ - Settle DEC-07: Deprecate or remove unused list_referrers_page        │
│ - If retained: fix error swallowing and saturating arithmetic (DEC-08) │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ (Review & Approval)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Slice 4: Mutation Containment & Durability Integration (Gate O-04)     │
│ - Hardening ensure_dir, contained atomic write, and fsync durability   │
└────────────────────────────────────────────────────────────────────────┘
```

---

### Specification of Recommended Immediate Slice (Slice 1)

#### Objective
Implement a contained referrers read helper module in `src/storage/fs/referrers_read.rs` and verify its behavior using test-owned fixtures, without changing production code or mutation call paths.

#### Proposed Compilation Boundary and Visibility
- **Declaration in `src/storage/fs.rs`**:
  ```rust
  #[cfg(test)]
  #[path = "fs/referrers_read.rs"]
  pub(crate) mod referrers_read;
  ```
  - This declaration follows the exact module convention in `src/storage/fs.rs` (e.g. `#[cfg(test)] #[path = "fs/payload_seam.rs"] mod payload_seam;`).
  - Because it is guarded by `#[cfg(test)]`, the helper module is compiled **only during test builds**, ensuring zero impact on production binaries.
- **New File**: `src/storage/fs/referrers_read.rs`
- **Modified Test File**: `src/storage/fs/tests.rs`
  - Adds acceptance tests for the contained read helper under submodule `referrers_read_contained`.

#### Proposed Types and Function Signatures
```rust
// src/storage/fs/referrers_read.rs

use crate::registry::digest::Digest;
use crate::storage::{ReferrerDescriptor, StorageError};
use storage_core::{ObjectKey, ObjectPayloadReader};

/// Caller-supplied resource limits for referrers payload reading.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) struct ReferrersReadLimits {
    /// Maximum bytes to read from the payload stream.
    /// If None, reads without a seam-imposed ceiling (preserves current unbounded behavior).
    pub max_payload_bytes: Option<u64>,

    /// Maximum number of parsed descriptors permitted in the referrers array.
    /// If None, permits any array length allowed by the payload byte limit.
    pub max_descriptors: Option<usize>,
}

/// Constructs the relative [`ObjectKey`] for a repository referrers file:
/// `repos/<repository>/referrers/<subject.hex()>.json`.
pub(crate) fn referrers_key(repo: &str, subject: &Digest) -> Result<ObjectKey, StorageError>;

/// Reads and deserializes a referrers index file using descriptor-relative containment.
///
/// - Missing file returns `Ok(Vec::new())`.
/// - Symlinks and uncontained paths fail with `StorageErrorKind::Io`.
/// - Non-regular objects (directories, FIFOs) fail with `StorageErrorKind::Io`.
/// - Corrupted JSON / invalid UTF-8 fails with `StorageErrorKind::Io` (DEC-04).
/// - Exceeding `max_payload_bytes` or `max_descriptors` fails with `StorageErrorKind::CorruptData`.
/// - Preserves the physical descriptor order from the stored JSON array.
pub(crate) async fn read_referrers_contained(
    reader: &(impl ObjectPayloadReader + ?Sized),
    repo: &str,
    subject: &Digest,
    limits: &ReferrersReadLimits,
) -> Result<Vec<ReferrerDescriptor>, StorageError>;
```

#### Unchanged Production Entry Points
- `FsStorage::list_referrers` remains unchanged.
- `FsStorage::list_referrers_page` remains unchanged.
- `ReferrersQueryService::query_referrers` remains unchanged.
- `FsStorage::add_referrer`, `remove_referrer`, and `delete_manifest` remain unchanged.

#### Explicit Exclusions from Slice 1
- No production wiring or constructor changes in `FsStorage`.
- No modifications to `ReferrersReader` or `Storage` traits.
- No changes to `list_referrers_page` pagination logic.
- No changes to mutation write logic (`atomic_write_file`, `ensure_dir`).
- No changes to HTTP API handlers or routes.

---

## Acceptance-Test Design

The following test suite is designed for the Slice 1 contained read seam, using test-owned fixtures only:

| # | Test Identifier | Target Condition / Invariant | Expected Outcome |
|---|---|---|---|
| 1 | `test_referrers_key_construction_valid` | Valid repo (`testrepo`, `org/subrepo`) and SHA-256 / SHA-512 subject | Returns `Ok(ObjectKey)` matching `repos/<repo>/referrers/<hex>.json`. |
| 2 | `test_referrers_key_construction_traversal` | Repo with `../`, leading/trailing `/`, backslash, control chars, empty segments | Returns `Err(StorageError::InvalidRepoName)` with 0 reader calls. |
| 3 | `test_read_referrers_contained_missing_file` | Non-existent subject file on reader returning `ReadError::NotFound` | Returns `Ok(vec![])`. |
| 4 | `test_read_referrers_contained_empty_array` | File containing `[]` (2 bytes) with `max_payload_bytes >= 2` and `max_descriptors = Some(0)` | Returns `Ok(vec![])` (valid empty array permitted under 0-count limit). |
| 5 | `test_read_referrers_contained_zero_byte_file_some_zero` | Existing 0-byte file with `max_payload_bytes = Some(0)` | Passes byte check (0 <= 0); fails JSON deserialization with `StorageErrorKind::Io`. |
| 6 | `test_read_referrers_contained_nonempty_file_some_zero` | Existing non-empty file (e.g. 2 bytes `[]`) with `max_payload_bytes = Some(0)` | Fails payload overflow before parsing with `StorageErrorKind::CorruptData`. |
| 7 | `test_read_referrers_contained_ordering_and_fields` | Multi-descriptor JSON `[C, A, B]` with artifact_type and annotations | Returns `Ok(vec![C, A, B])` preserving stored file order; all fields deserialized intact. |
| 8 | `test_read_referrers_contained_sha256_and_sha512` | 64-hex SHA-256 and 128-hex SHA-512 subject files | Both algorithms correctly resolved and read. |
| 9 | `test_read_referrers_contained_symlink_rejection` | Final file is a symlink pointing inside or outside storage root | Linux `openat2` rejects with `ResolutionRejected`; maps to `StorageErrorKind::Io`. |
| 10 | `test_read_referrers_contained_directory_symlink` | Directory `referrers/` is a symlink | Linux `openat2` rejects with `ResolutionRejected`; maps to `StorageErrorKind::Io`. |
| 11 | `test_read_referrers_contained_directory_in_place_of_file` | Directory at `<hex>.json` path | Linux `openat2` Phase 2 rejects `!S_IFREG` (`UnsupportedObjectType`); maps to `StorageErrorKind::Io` (`EISDIR`). |
| 12 | `test_read_referrers_contained_permission_denied` | Mode `0o000` under unprivileged user environment | Returns `Err(StorageError::Internal { kind: Io, .. })`. (Marked `#[ignore]` for non-root environments; verified separately). |
| 13 | `test_read_referrers_contained_corrupted_json_taxonomy` | Truncated JSON, invalid UTF-8 bytes, object instead of array | Returns `Err(StorageError::Internal { kind: Io, .. })`. |
| 14 | `test_read_referrers_contained_payload_byte_ceiling_exact` | Payload exactly equal to `max_payload_bytes` | Returns `Ok(descriptors)`. |
| 15 | `test_read_referrers_contained_payload_byte_ceiling_exceeded` | Payload of $N + 1$ bytes when limit is $N$ | Bounded draining detects excess; returns `Err(StorageError::Internal { kind: CorruptData, .. })`. |
| 16 | `test_read_referrers_contained_u64_max_limit_representation` | `max_payload_bytes = Some(u64::MAX)` | Fails checked addition (`checked_add(1)`); returns `Err(StorageError::Internal { kind: CorruptData, .. })` before read. |
| 17 | `test_read_referrers_contained_descriptor_count_ceiling` | Array containing 11 descriptors when `max_descriptors = Some(10)` | Post-parse check detects count; returns `Err(StorageError::Internal { kind: CorruptData, .. })`. |
| 18 | `test_read_referrers_contained_shared_reader_identity` | Uses `FsStorage::reader` instance (`Arc<FsMetadataReader>`) | Confirms compatibility with the live storage instance reader. |

---

## Limitations, Residual Risks & Quality Gates

### Operational Assumptions & Limitations
1. **Linux openat2 Prerequisite**: Descriptor-relative containment requires Linux kernel 5.6+ with `openat2` support and a genuine, stable procfs mount at `/proc/self/fd`. Environments lacking `openat2` return `StorageErrorKind::Configuration`.
2. **Absence of Snapshot Isolation**: Concurrent writers or file truncation during read stream draining may cause deserialization errors. Read containment does not provide snapshot isolation.
3. **Lack of Namespace Snapshots**: Pinned root descriptors refer to directory inodes. Pathname replacements on disk do not affect the open descriptor, while uncontained mutations operate on the new path.
4. **Non-Linux Verification**: Verification on non-Linux platforms (macOS, Windows, BSD) remains unperformed (Quality Gate O-15).

### Canonical Quality Gates

All canonical quality gates remain explicitly **OPEN**:
- **O-03**: Key and continuation-token contracts.
- **O-04**: Filesystem write durability and containment.
- **O-05**: Broader filesystem read containment.
- **O-06**: Typed AWS mapping and pinned-MinIO evidence.
- **O-13**: Hosting, distribution, and release strategy.
- **O-15**: Non-Linux verification.
- **O-16**: Earlier Slice 11 audit/test-inventory evidence.
- **D-06**: Broader extraction, cutover, compatibility, and distribution acceptance.
