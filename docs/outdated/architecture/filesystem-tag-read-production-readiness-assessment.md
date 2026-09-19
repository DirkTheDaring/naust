> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. The cutover assessed here shipped (`5a0b424`, later `tag_domain` `32c42c6`) while its TAG-DEC-01…06 decision rows were still pending; those items remain OPEN and are now tracked canonically in [acceptance-gates.md](../../technical-debt.md).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Architecture Assessment: Contained Filesystem Tag Read Production Readiness (Final)

- **Document**: `docs/architecture/filesystem-tag-read-production-readiness-assessment.md`
- **Repository**: `registry-rust`
- **Date**: 2026-09-12
- **Status**: DOCUMENTATION & ARCHITECTURAL REVIEW ONLY — PENDING USER DECISIONS — NOT COMMITTED
- **Authoritative Baseline HEADs**:
  - `registry-rust`: `20f939040849a20cd9c72159e572f563200de31b`
  - `storage-layer-rust`: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (strictly read-only)
- **Canonical Quality Gates**: All eight gates remain explicitly **OPEN**:
  - `O-03`: Key and continuation-token contracts.
  - `O-04`: Filesystem write durability and containment.
  - `O-05`: Broader filesystem read containment.
  - `O-06`: Typed AWS mapping and pinned-MinIO evidence.
  - `O-13`: Hosting, distribution, and release strategy.
  - `O-15`: Non-Linux verification.
  - `O-16`: Earlier Slice 11 audit/test-inventory evidence.
  - `D-06`: Broader extraction, cutover, compatibility, and distribution acceptance.

---

## 1. Executive Summary & Review Baseline

This assessment analyzes the architectural, operational, and caller-level implications of promoting the contained filesystem tag-read test seam committed in `20f939040849a20cd9c72159e572f563200de31b` to active production routing.

### 1.1 Current Baseline State
1. **Seam Status**: The contained tag-read seam implemented in `src/storage/fs/tag_seam.rs` is strictly **test-only**, gated behind `#[cfg(test)]` in `src/storage/fs.rs`.
2. **Production Routing Status**: Active production methods [`FsStorage::resolve_tag`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L940-L951) and [`FsStorage::get_tag_with_version`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1250-L1269) remain **100% on legacy ambient pathname operations** (`tokio::fs::read_to_string` and `tokio::fs::read`).
3. **Accepted Test Scope**:
   - Recorded evidence: 28 seam tests passed, 10 characterization tests passed, two permission tests remained unexecuted, and non-Linux verification remains outstanding.
   - Neither ignored test was executed (kept unexecuted pending genuine unprivileged execution).
   - Outstanding platform verification remains under Gate `O-15`.
   - Test-seam acceptance establishes verification of the seam's mechanics but **does not authorize production cutover**.
4. **Core Invariants & Behavioral Distinctions**:
   - **Parsing Contracts**: Seam faithfully preserves the contrasting caller parsing contracts (`resolve_tag` uses strict UTF-8, trims whitespace, maps invalid UTF-8 to `Io`, and missing/empty/malformed to `NotFound`; `get_tag_with_version` uses lossy UTF-8, trims whitespace, maps missing to `Ok(None)`, and empty/malformed/replacement-char to `CorruptData`).
   - **Version Hashing**: `get_tag_with_version_seam` hashes **unmodified raw bytes** via SHA-256, guaranteeing bit-exact version compatibility with legacy optimistic concurrency checks in `delete_tag_conditional`.
   - **Structural Rejection & Symlink Rejection**: Seam introduces intentional security differences: rejecting directory traversal (`..`), empty components, backslashes, and null characters prior to reader invocation, and kernel-rejecting symlinks (`RESOLVE_NO_SYMLINKS`).
   - **Unbounded Default**: `TagReadLimits { max_payload_bytes: None }` explicitly enforces no seam-level ceiling, preserving legacy unbounded behavior. Bounded limits (`max_payload_bytes: Some(limit)`) are explicit test controls.
   - **Asymmetric Mutability & Advisory Locking**: Tag mutations (`set_tag`, `mutate_tag`, `delete_tag_conditional`) remain pathname-based. Mutating operations coordinate via exclusive advisory locks (`.lock.{tag}`) only when resolving the identical lock-file inode. Tag reads remain lockless.
   - **Lack of Coherence & Snapshot Guarantees**: Contained reads resolve relative to a pinned `root_fd` and do not provide atomic snapshots or coherence across separate calls when directory trees are replaced or modified concurrently.

---

## 2. Exact Cutover Scope

```
+---------------------------------------------------------------------------------------+
| FsStorage (src/storage/fs.rs)                                                         |
|                                                                                       |
|  [CURRENT PRODUCTION CODE]                                                            |
|  pub async fn resolve_tag(&self, name: &str, tag: &str) -> Result<Digest, StorageError>|
|    └─> tokio::fs::read_to_string(self.tag_path(name, tag)) [Ambient pathname]         |
|                                                                                       |
|  [PROPOSED CUTOVER PSEUDOCODE]                                                        |
|  pub async fn resolve_tag(&self, name: &str, tag: &str) -> Result<Digest, StorageError>|
|    └─> tag_seam::resolve_tag_seam(self.reader.as_ref(), name, tag, &TagReadLimits)    |
|                                                                                       |
|  [CURRENT PRODUCTION CODE]                                                            |
|  pub async fn get_tag_with_version(&self, repo: &str, tag: &str)                     |
|         -> Result<Option<(Digest, String)>, StorageError>                             |
|    └─> tokio::fs::read(self.tag_path(repo, tag)) [Ambient pathname]                   |
|                                                                                       |
|  [PROPOSED CUTOVER PSEUDOCODE]                                                        |
|  pub async fn get_tag_with_version(&self, repo: &str, tag: &str)                     |
|         -> Result<Option<(Digest, String)>, StorageError>                             |
|    └─> tag_seam::get_tag_with_version_seam(self.reader.as_ref(), repo, tag, &Limits)  |
|                                                                                       |
|  [SHARED DESCRIPTOR-RELATIVE READER]                                                  |
|  self.reader: Arc<storage_fs::FsMetadataReader>                                       |
|    ├─> Blob CAS reading: self.read_adapter (FsBlobCasReadAdapter)                     |
|    ├─> Manifest reading: manifest::head_manifest_impl / get_manifest_impl             |
|    └─> [PROPOSED] Tag reading: tag_seam::resolve_tag_seam / get_tag_with_version_seam |
|                                                                                       |
|  [EXCLUDED FROM CUTOVER - REMAINS UNMODIFIED PATHNAME LOGIC]                          |
|  - list_tags(&self, name: &str)                                                       |
|  - list_tags_page(&self, name: &str, ...)                                             |
|  - set_tag(&self, name: &str, tag: &str, digest: &Digest)                             |
|  - mutate_tag(&self, name: &str, tag: &str, ...) [Advisory Lock: .lock.{tag}]          |
|  - delete_tag_conditional(&self, repo: &str, tag: &str, ...) [Advisory Lock]          |
|  - read_lifecycle_journal / write_lifecycle_journal / delete_lifecycle_journal         |
+---------------------------------------------------------------------------------------+
```

### 2.1 Proposed Production Entry Points
A production cutover would modify exactly two methods inside [`src/storage/fs.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs):
1. **[`FsStorage::resolve_tag`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L940-L951)**: Replace `let path = self.tag_path(name, tag); tokio::fs::read_to_string(&path)...` with delegation to `resolve_tag_seam` using `self.reader.as_ref()`.
2. **[`FsStorage::get_tag_with_version`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1250-L1269)**: Replace `let tag_path = self.tag_path(repo, tag); tokio::fs::read(&tag_path)...` with delegation to `get_tag_with_version_seam` using `self.reader.as_ref()`.

### 2.2 Module Promotion
`src/storage/fs/tag_seam.rs` is currently declared exclusively in `src/storage/fs.rs` as:
```rust
#[cfg(test)]
#[path = "fs/tag_seam.rs"]
mod tag_seam;
```
Promoting to production requires:
- Removing the `#[cfg(test)]` attribute from the module declaration in `src/storage/fs.rs` (e.g. `pub(crate) mod tag_seam;` or renaming to `pub(crate) mod tag_read;`).
- Retaining test harnesses (`RecordingFakePayloadReader`, `FailingStream`, unit tests, real filesystem integration tests) strictly within inner `#[cfg(test)] mod tests`.

### 2.3 Shared-Reader Utilization
The cutover passes `self.reader.as_ref()` (`&storage_fs::FsMetadataReader`), which implements `storage_core::ObjectPayloadReader`.
- Demonstrates zero descriptor reopening overhead for `root_fd`.
- Reuses the identical `FsMetadataReader` instance shared by `FsBlobCasReadAdapter` and manifest reading routines ([`manifest::head_manifest_impl`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/manifest.rs#L149), [`manifest::get_manifest_impl`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/manifest.rs#L117)).

### 2.4 Affected Callers
The cutover impacts all downstream consumers of the `Storage` and `TagReader` capability ports:
1. **Manifest Fetch & Cache Validation** ([`src/application/manifest_read.rs:92, 177, 293, 307, 320, 368, 384`](file:///home/dietmar/devel/rust/registry-rust/src/application/manifest_read.rs)): Calls `resolve_tag` during [`ManifestReadService::resolve_reference_digest`](file:///home/dietmar/devel/rust/registry-rust/src/application/manifest_read.rs#L271) and `ensure_tag_fresh`.
2. **Tag Inspection API** ([`src/application/tags.rs:117`](file:///home/dietmar/devel/rust/registry-rust/src/application/tags.rs#L117)): Calls `resolve_tag` via `TagQueryService::resolve_tag`.
3. **Catalog Platform Inspection** ([`src/application/catalog.rs:158`](file:///home/dietmar/devel/rust/registry-rust/src/application/catalog.rs#L158)): Calls `resolve_tag` via `CatalogQueryService::tag_platforms_for_repo`.
4. **Blob Deletion Safety Pre-checks** ([`src/blob_delete_safety.rs:105, 140`](file:///home/dietmar/devel/rust/registry-rust/src/blob_delete_safety.rs#L105)): Calls `resolve_tag` via `scan_storage_for_blob` and `find_repo_blob_reference` to verify whether a tag targets a blob before deletion.
5. **Reference Index Reconciliation** ([`src/blob_ref_index.rs:776`](file:///home/dietmar/devel/rust/registry-rust/src/blob_ref_index.rs#L776)): Calls `resolve_tag` during `BlobRefIndex::refresh_tag_rooted_conservative`.
6. **Membership Migration** ([`src/membership_migration.rs:21, 129, 218`](file:///home/dietmar/devel/rust/registry-rust/src/membership_migration.rs#L21)): Calls `resolve_tag` to discover target digests for repository-blob membership linking.
7. **Supervisor Consistency Checks** ([`src/supervisor.rs:951`](file:///home/dietmar/devel/rust/registry-rust/src/supervisor.rs#L951)): Calls `resolve_tag` during `compute_protected_blobs`.
8. **Tag Lifecycle Deletion** ([`src/manifest_lifecycle.rs:1550`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs#L1550)): Calls `get_tag_with_version` to inspect tag version, snapshot to journal, and feed optimistic concurrency check in `delete_tag_conditional`.
9. **Manifest Deletion & Proxy Eviction** ([`src/manifest_lifecycle.rs:629, 648, 1156, 1401, 1442`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs#L1156)): Calls `get_tag_with_version` during proxy cache eviction and manifest deletion to snapshot associated tag versions.

### 2.5 Strict Non-Goals: What Remains Outside the Cutover
- **Public Port Interfaces**: `Storage::resolve_tag`, `Storage::get_tag_with_version`, `TagReader` trait methods retain identical signatures and error return types (`StorageError`).
- **Tag Mutations**: `set_tag`, `mutate_tag`, and `delete_tag_conditional` remain 100% on legacy ambient pathname operations and advisory file locking (`.lock.{tag}`).
- **Tag Listing**: `list_tags` and `list_tags_page` remain on ambient pathname `read_dir` operations.
- **Dependencies**: No external crate additions or version upgrades.
- **Unrelated Storage Reads**: Blobs, manifests, and lifecycle journals are not modified by this proposal.

---

## 3. Compatibility & Caller Error Propagation Chains

The proposed contained seam introduces deliberate security-driven divergences from legacy behavior. Error mappings cannot be generalized into a single status code across endpoints; they must be traced through the exact conversion branches of each caller.

### 3.1 Tracing Endpoint 1: `GET /v2/<name>/manifests/<reference>`
Handler: [`src/http_api/handlers.rs::manifest_get`](file:///home/dietmar/devel/rust/registry-rust/src/http_api/handlers.rs#L536-L589)  
Service: [`ManifestReadService::resolve_reference_digest`](file:///home/dietmar/devel/rust/registry-rust/src/application/manifest_read.rs#L271-L316)

```
[Incoming Request: GET /v2/<name>/manifests/<reference>]
   │
   ├─> Outer Axum Extractors / Route Matching
   │     ├─> is_valid_repo_name(name) == false ──> HTTP 400 (NAME_INVALID)
   │     └─> is_valid_tag(reference) == false  ──> HTTP 400 (TAG_INVALID)
   │
   v
[ManifestReadService::get_manifest / resolve_reference_digest(repo, reference, ...)]
   │
   ├─> CanonicalRepoName::parse(repo).is_err()
   │     └─> returns ManifestReadError::InvalidRepoName { name, source }
   │           └─> Handlers:575 matches Err(ManifestReadError::InvalidRepoName { .. })
   │                 └─> errors::name_invalid() ──> HTTP 400 Bad Request (NAME_INVALID)
   │
   v
[Storage Call: self.tag_reader.resolve_tag(repo, reference)]
   │
   ├─> StorageError::NotFound
   │     └─> Service:296 maps to ManifestReadError::TagNotFound
   │           └─> Handlers:572 matches Err(ManifestReadError::TagNotFound)
   │                 └─> errors::manifest_unknown() ──> HTTP 404 Not Found (MANIFEST_UNKNOWN)
   │
   ├─> StorageError::InvalidRepoName (e.g. seam validate_path_component rejecting "..")
   │     └─> Service:297 falls into other => ManifestReadError::Storage(StorageError::InvalidRepoName(...))
   │           └─> Handlers:587 falls into Err(_) => errors::internal_error()
   │                 └─> HTTP 500 Internal Server Error (UNKNOWN)  <-- NOT HTTP 400!
   │
   ├─> StorageError::Internal { kind: Io, .. } (symlink rejected, directory, invalid UTF-8)
   │     └─> Service:297 maps to ManifestReadError::Storage(...)
   │           └─> Handlers:587 falls into Err(_) => errors::internal_error()
   │                 └─> HTTP 500 Internal Server Error (UNKNOWN)
   │
   ├─> StorageError::Internal { kind: Configuration, .. } (openat2 ENOSYS)
   │     └─> Service:297 maps to ManifestReadError::Storage(...)
   │           └─> Handlers:587 falls into Err(_) => errors::internal_error()
   │                 └─> HTTP 500 Internal Server Error (UNKNOWN)
   │
   └─> StorageError::Internal { kind: Backend, .. } (runtime missing, join failure)
         └─> Service:297 maps to ManifestReadError::Storage(...)
               └─> Handlers:587 falls into Err(_) => errors::internal_error()
                     └─> HTTP 500 Internal Server Error (UNKNOWN)
```

**Architectural Finding on Rejection Origins**:
`ManifestReadError::InvalidRepoName` and `ManifestReadError::Storage(StorageError::InvalidRepoName)` are strictly distinct enum variants:
```rust
// src/application/errors.rs:192-205
pub enum ManifestReadError {
    InvalidRepoName { name: String, source: crate::registry::canonical_name::CanonicalRepoError },
    InvalidTag(String),
    NotFound,
    TagNotFound,
    Storage(StorageError),
    ...
}
```
In `src/http_api/handlers.rs:575`:
- Input rejected *before* storage by application-layer pre-validation (`CanonicalRepoName::parse`) returns `ManifestReadError::InvalidRepoName` -> `errors::name_invalid()` (**HTTP 400**).
- Input rejected *by the storage seam* (`tag_key` path component validation) returns `StorageError::InvalidRepoName`, which the service converts into `ManifestReadError::Storage(...)`. The HTTP handler does not match `Storage(InvalidRepoName)` specifically; it falls through to the wildcard `Err(_) => errors::internal_error()` (**HTTP 500**).

---

### 3.2 Tracing Endpoint 2: `DELETE /v2/<name>/manifests/<reference>` & `DELETE /v2/<name>/tags/<tag>`
Handlers:
- [`src/http_api/handlers.rs::manifest_delete`](file:///home/dietmar/devel/rust/registry-rust/src/http_api/handlers.rs#L448-L491) (DELETE `/v2/<name>/manifests/<reference>`)
- [`src/http_api/tags.rs::tag_delete`](file:///home/dietmar/devel/rust/registry-rust/src/http_api/tags.rs#L107-L141) (DELETE `/v2/<name>/tags/<tag>`)  
Application Service: [`ManifestService::delete_tag`](file:///home/dietmar/devel/rust/registry-rust/src/application/manifest.rs#L69-L88)  
Lifecycle Service: [`ManifestLifecycleService::delete_tag`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs#L1540-L1620)

```
[Incoming Request: DELETE /v2/<name>/manifests/<reference> or /tags/<tag>]
   │
   ├─> Outer Axum Extractors / Route Validation
   │     ├─> !is_valid_repo_name(name) ──> HTTP 400 (NAME_INVALID)
   │     └─> !is_valid_tag(tag)        ──> HTTP 400 (TAG_INVALID)
   │
   v
[ManifestService::delete_tag(repo, tag, allow_tag_overwrite)] (src/application/manifest.rs:69)
   │
   ├─> CanonicalRepoName::parse(repo) failure
   │     └─> returns ManifestMutationError::InvalidRepoName { name, source }
   │           └─> Handlers match InvalidRepoName ──> HTTP 400 (NAME_INVALID)
   │
   ├─> !allow_tag_overwrite
   │     └─> returns ManifestMutationError::TagImmutable
   │           └─> Handlers match TagImmutable ──> HTTP 403 Forbidden (DENIED)
   │
   v
[ManifestLifecycleService::delete_tag(repo, tag)] (src/manifest_lifecycle.rs:1545)
   │
   ├─> 1. Prior Coordination: acquire_coordination(repo).await?
   │     └─> Acquires exclusive repository coordination lease / lock
   │
   ├─> 2. Prior Recovery: recover_and_ensure_index_healthy(repo).await?
   │     ├─> Replays existing orphaned journal (mutates storage/index, deletes old journal)
   │     └─> Rebuilds sled index if unhealthy
   │
   v
[Storage Call: self.storage.get_tag_with_version(repo, tag)] (src/manifest_lifecycle.rs:1550)
   │
   ├─> Ok(None) (tag missing on disk)
   │     └─> Returns Err(ManifestLifecycleError::TagNotFound)
   │           └─> ManifestMutationError::from(TagNotFound) = ManifestMutationError::TagNotFound
   │                 ├─> handlers.rs:473 matches TagNotFound ──> HTTP 404 (MANIFEST_UNKNOWN)
   │                 └─> tags.rs:127 matches TagNotFound     ──> HTTP 404 (TAG_UNKNOWN)
   │
   ├─> Err(StorageError::Internal { kind: CorruptData, .. }) (corrupt text, 0 bytes, invalid UTF-8)
   │     └─> Returns Err(ManifestLifecycleError::Storage(CorruptData))
   │           └─> ManifestMutationError::from(Storage(e)) = ManifestMutationError::Storage(e)
   │                 └─> Both handlers fall into Err(_) => errors::internal_error()
   │                       └─> HTTP 500 Internal Server Error (UNKNOWN)
   │
   ├─> Err(StorageError::Internal { kind: Io, .. }) (symlink rejected, directory, stream I/O error)
   │     └─> Returns Err(ManifestLifecycleError::Storage(Io))
   │           └─> ManifestMutationError::from(Storage(e)) = ManifestMutationError::Storage(e)
   │                 └─> Both handlers fall into Err(_) => errors::internal_error()
   │                       └─> HTTP 500 Internal Server Error (UNKNOWN)
   │
   v [READ SUCCEEDED: Ok(Some((target_digest, version)))]
   │
   ├─> Action: Durably marks ref index dirty (idx.mark_dirty())
   ├─> Action: Writes new journal record (phase: LifecyclePhase::TagDeleteInitiated)
   │
   v
[Storage Call: self.storage.delete_tag_conditional(repo, tag, Some(&version))]
   │
   ├─> Ok(ConditionalDeleteResult::Deleted)
   │     └─> Advances journal -> updates ref index -> attempts mark_ready -> deletes journal
   │           └─> Handlers return HTTP 202 Accepted (ACCEPTED)
   │
   ├─> Ok(ConditionalDeleteResult::NotFound)
   │     └─> Cleanup: attempts delete_journal -> attempts mark_ready
   │           └─> Returns Err(ManifestLifecycleError::TagNotFound)
   │                 └─> HTTP 404 (MANIFEST_UNKNOWN / TAG_UNKNOWN)
   │
   ├─> Ok(ConditionalDeleteResult::PreconditionFailed { .. })
   │     └─> Cleanup: attempts delete_journal -> attempts mark_ready
   │           └─> Returns Err(ManifestLifecycleError::TagPreconditionFailed)
   │                 └─> ManifestMutationError::from converts to ManifestMutationError::TagPreconditionFailed
   │                       └─> Handlers have NO explicit match arm for TagPreconditionFailed!
   │                             └─> Falls into Err(_) => errors::internal_error().into_response()
   │                                   └─> HTTP 500 Internal Server Error (UNKNOWN)  <-- VERIFIED FROM SOURCE!
   │
   ├─> Err(StorageError) (conditional delete failed with I/O or backend error)
   │     └─> Propagates Err via `?` (cleanup bypassed)
   │           └─> [RESIDUAL STATE]: New journal written; tag removal and journal persistence depend on failure stage; index remains dirty
   │                 └─> Handlers fall into Err(_) => errors::internal_error() ──> HTTP 500 (UNKNOWN)
   │
   └─> Cleanup Failure (e.g. self.delete_journal fails after PreconditionFailed or NotFound)
         └─> Propagates cleanup error via `?` (idx.mark_ready() bypassed)
               └─> [RESIDUAL STATE]: Cleanup error propagates and prevents subsequent mark_ready; actual journal presence depends on failure stage
                     └─> Handlers fall into Err(_) => errors::internal_error() ──> HTTP 500 (UNKNOWN)
```

**Verification of `TagPreconditionFailed` HTTP Status**:
Tracing the conversion from source:
1. `ManifestLifecycleService::delete_tag` lines 1603-1610 returns `Err(ManifestLifecycleError::TagPreconditionFailed)`.
2. In `src/application/errors.rs:149-151`:
   ```rust
   ManifestLifecycleError::TagPreconditionFailed => {
       ManifestMutationError::TagPreconditionFailed
   }
   ```
3. In `src/http_api/handlers.rs:471-489` and `src/http_api/tags.rs:118-140`, the match arms check:
   - `TagNotFound`
   - `InvalidRepoName`
   - `InvalidTag`
   - `TagImmutable`
   - `Storage(StorageError::Unsupported)`
   - `Err(_) => errors::internal_error().into_response()`
4. `TagPreconditionFailed` has **no explicit match arm** in either handler. It falls through to `Err(_) => errors::internal_error()`.
5. [`errors::internal_error()`](file:///home/dietmar/devel/rust/registry-rust/src/http_api/errors.rs#L301-L310) constructs:
   `error_response(StatusCode::INTERNAL_SERVER_ERROR, body)` with code `"UNKNOWN"`.
6. Therefore, `TagPreconditionFailed` returns **HTTP 500 Internal Server Error**, not HTTP 412 Precondition Failed or HTTP 409 Conflict.

**Testing Layer Distinction**:
- **Service-Level Unit/Integration Tests**: Calling `ManifestLifecycleService::delete_tag` or `ManifestService::delete_tag` directly asserts typed Rust errors: `Err(ManifestLifecycleError::TagPreconditionFailed)` or `Err(ManifestMutationError::TagPreconditionFailed)`. These tests do not exercise HTTP status codes or serialization.
- **Router/HTTP Integration Tests**: Requests submitted through the Axum router assert wire HTTP behavior: status code 500, header `Docker-Distribution-API-Version: registry/2.0`, and JSON error body `{"errors":[{"code":"UNKNOWN","message":"internal error"}]}`.

---

### 3.3 Comprehensive Comparative Compatibility Matrix

| Dimension / Condition | Underlying Cause / Event | Legacy `FsStorage` Behavior | Proposed Contained Behavior | Application Service Mapping | HTTP Handler Response & OCI Error Code |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Empty Repo or Tag** | Empty string (`""`) passed | Attempts path open; fails with `NotFound` or `Io` | `tag_key` returns `StorageError::InvalidRepoName` before reader call | `ManifestReadError::Storage(InvalidRepoName)` | **HTTP 500 Internal Server Error** (`UNKNOWN`) *(Note: Outer HTTP extractors return HTTP 400; internal caller storage errors return HTTP 500)* |
| **Path Traversal (`..`, `\`, `\0`)** | Contains `..`, backslashes, control chars | Ambient traversal escapes storage root if file exists | `tag_key` returns `StorageError::InvalidRepoName` with 0 reader calls | `ManifestReadError::Storage(InvalidRepoName)` | **HTTP 500 Internal Server Error** (`UNKNOWN`) |
| **Nested Repository Names** | e.g. `org/team/app`, tag `v1` | Formats path `repos/org/team/app/tags/v1`; reads file | Validates segments; formats `ObjectKey`; reads file | Normal manifest resolution | **HTTP 200 OK** (Preserved compatibility) |
| **Final Symlink Outside Root** | Tag file is symlink pointing to `/etc/passwd` | Resolves symlink; reads external file content | Kernel `openat2` returns `ELOOP`; mapped to `StorageErrorKind::Io` | `ManifestReadError::Storage(Io)` | **HTTP 500 Internal Server Error** (`UNKNOWN`) |
| **Ancestor Directory Symlink** | `repos/repo/tags` is symlink outside root | Resolves symlink; reads external directory tree | Kernel `openat2` returns `ELOOP`; mapped to `StorageErrorKind::Io` | `ManifestReadError::Storage(Io)` | **HTTP 500 Internal Server Error** (`UNKNOWN`) |
| **Dangling Symlink** | Symlink target does not exist | `tokio::fs` follows link; returns `ENOENT` -> `NotFound` | Kernel `openat2` rejects symlink first (`ELOOP`); mapped to `Io` | `ManifestReadError::Storage(Io)` | **HTTP 500 Internal Server Error** (`UNKNOWN`) *(Semantic shift from legacy HTTP 404)* |
| **Directory in place of Tag** | Tag name exists as directory (`S_IFDIR`) | `read_to_string`/`read` returns `EISDIR`; mapped to `Io` | `fstat` detects `S_IFDIR`; `UnsupportedObjectType` -> `Io` | `ManifestReadError::Storage(Io)` | **HTTP 500 Internal Server Error** (`UNKNOWN`) |
| **Missing Tag File** | Tag file does not exist | `resolve_tag` -> `NotFound`; `get_tag` -> `Ok(None)` | `resolve_tag` -> `NotFound`; `get_tag` -> `Ok(None)` | `resolve` -> `TagNotFound`; `get` -> `TagNotFound` | `resolve` -> **HTTP 404** (`MANIFEST_UNKNOWN`); `delete_tag` -> **HTTP 404** (`TAG_UNKNOWN`) |
| **Missing Repository Dir** | Repo directory does not exist | `resolve_tag` -> `NotFound`; `get_tag` -> `Ok(None)` | `resolve_tag` -> `NotFound`; `get_tag` -> `Ok(None)` | `resolve` -> `TagNotFound`; `get` -> `TagNotFound` | **HTTP 404 Not Found** (`MANIFEST_UNKNOWN` / `TAG_UNKNOWN`) |
| **Malformed Digest Text** | Content is `"not-a-digest"` | `resolve_tag` -> `NotFound`; `get_tag` -> `CorruptData` | `resolve_tag` -> `NotFound`; `get_tag` -> `CorruptData` | `resolve` -> `TagNotFound`; `get` -> `Storage(CorruptData)` | `resolve` -> **HTTP 404** (`MANIFEST_UNKNOWN`); `delete_tag` -> **HTTP 500** (`UNKNOWN`) |
| **Empty Tag File (0 bytes)** | Tag file is 0 bytes | `resolve_tag` -> `NotFound`; `get_tag` -> `CorruptData` | Drain helper accepts 0 bytes; `resolve` -> `NotFound`; `get` -> `CorruptData` | `resolve` -> `TagNotFound`; `get` -> `Storage(CorruptData)` | `resolve` -> **HTTP 404** (`MANIFEST_UNKNOWN`); `delete_tag` -> **HTTP 500** (`UNKNOWN`) |
| **Invalid UTF-8 Bytes** | File contains non-UTF8 bytes (`0xFF`) | `resolve_tag` -> `Io`; `get_tag` -> `CorruptData` | `resolve_tag` -> `Io`; `get_tag` -> `CorruptData` | `resolve` -> `Storage(Io)`; `get` -> `Storage(CorruptData)` | **HTTP 500 Internal Server Error** (`UNKNOWN`) |
| **Syscall Unsupported (`ENOSYS`)** | Host kernel lacks `openat2` (< Linux 5.6) | N/A (ambient syscalls succeed) | `FsMetadataError::SyscallUnsupported` -> `StorageError::Configuration` | `ManifestReadError::Storage(Configuration)` | **HTTP 500 Internal Server Error** (`UNKNOWN`) |
| **Runtime Missing / Task Join Failed** | Tokio runtime missing or background task panicked | Ambient calls panic or fail join | `FsMetadataError::RuntimeMissing`/`TaskJoinFailed` -> `Backend` | `ManifestReadError::Storage(Backend)` | **HTTP 500 Internal Server Error** (`UNKNOWN`) |

---

## 4. Lifecycle Side-Effects & Recovery Accounting

### 4.1 Separate Accounting of Prior Operations in `delete_tag`
Prior to invoking `self.storage.get_tag_with_version(repo, tag)`, [`ManifestLifecycleService::delete_tag`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs#L1547-L1548) executes two discrete operations:
```rust
let mut guard = self.acquire_coordination(repo).await?;
self.recover_and_ensure_index_healthy(repo).await?;
```

These operations occur **before** the tag read:
1. **Coordination Acquisition (`acquire_coordination(repo)`)**:
   - Acquires an exclusive coordination lease on the repository (e.g. creating/refreshing `.repo_lock`).
   - Sets lease ownership and lease expiry timestamp.
2. **Prior Recovery (`recover_and_ensure_index_healthy(repo)`)**:
   - **Journal Replay**: Calls `self.read_journal(repo)`. If an incomplete operation's journal exists from a prior crash:
     - Marks the sled reference index dirty (`idx.mark_dirty()`).
     - Replays pending actions: for `Publish`, mutates storage (`mutate_tag`), adds referrers, and rebuilds DAG edges; for `DeleteTag`, completes deletion.
     - Deletes the prior journal from storage (`self.delete_journal(repo)`).
     - Attempts to mark index ready (`let _ = idx.mark_ready()`).
   - **Index Health & Rebuild**: If `idx.check_health()` fails, executes `idx.ensure_healthy_or_rebuild(...)`, which re-scans storage and rebuilds sled database trees.

### 4.2 State Preservation Under Failed Tag Read
When `self.storage.get_tag_with_version(repo, tag)` fails (e.g. tag missing, I/O error, corrupt data):
- **Clean Precondition (Pre-requisite for Preservation)**:
  - The repository must have **no pre-existing journal** in `repos/<repo>/meta/lifecycle_journal.json`.
  - The sled reference index must be healthy (`idx.check_health().is_ok()`) and marked clean/ready.
  - The repository coordination lease must be uncontended.
- **Observable State Preservation Under Clean Preconditions**:
  - The call aborts **after** coordination acquisition and prior recovery check.
  - It aborts **before** new-operation index dirtying (`idx.mark_dirty()` is not called).
  - It aborts **before** new-operation journal writing (`write_journal` is not called).
  - It aborts **before** `delete_tag_conditional` is invoked.
  - The tag file on disk is unchanged.
  - No journal file is created; `meta/lifecycle_journal.json` does not exist before or after.
  - The reference index dirty state is unchanged (remains clean/ready).
  - Upon return, `guard` is dropped, releasing the coordination lease.
- **Recovery Precondition Accounting**:
  - If a pending journal or corrupt index existed prior to `delete_tag`, `recover_and_ensure_index_healthy` **ALREADY altered on-disk storage and sled database state** before `get_tag_with_version` was reached. A subsequent failure in `get_tag_with_version` does not retroactively undo those recovery mutations.

### 4.3 Post-Read Deletion Outcomes & Qualified Cleanup
If `get_tag_with_version` succeeds, `delete_tag` marks the index dirty and writes a journal in `TagDeleteInitiated` phase. It then calls `delete_tag_conditional(repo, tag, Some(&version))`. Four distinct outcomes must be distinguished:

1. **Successful Deletion (`Ok(ConditionalDeleteResult::Deleted)`)**:
   - Tag file deleted from storage. Directory synced.
   - Journal advanced to `TagDeletedOnly`, index updated, attempts to mark index ready (`let _ = idx.mark_ready()`), journal deleted.
2. **Controlled Version Mismatch (`Ok(ConditionalDeleteResult::PreconditionFailed { .. })`)**:
   - Tag file on disk is **NOT** unlinked.
   - Deletion routine executes cleanup: calls `self.delete_journal(repo)` and attempts to mark index ready (`let _ = idx.mark_ready()`).
   - Returns `Err(ManifestLifecycleError::TagPreconditionFailed)` -> HTTP 500 (`UNKNOWN`).
3. **Execution Error in Conditional Deletion (`Err(StorageError)`)**:
   - e.g. I/O error reading tag file under lock, or storage error removing file.
   - **Crucial Qualification**: An `Err(StorageError)` is **not proof that no mutation occurred**:
     - `write_journal` **already wrote** `lifecycle_journal.json` to disk in `TagDeleteInitiated` phase.
     - `idx.mark_dirty()` **already marked** the reference index dirty.
     - The tested failure stage and assumptions determine actual side effects: if failure occurred during lock acquisition or initial read, the tag file remains on disk; if failure occurred during `std::fs::remove_file(&path)` (e.g. storage error after inode unlinking), the tag file on disk may have been removed.
     - The error propagates immediately via `?`. Cleanup is bypassed: the journal remains in `TagDeleteInitiated` phase, and the index remains marked dirty until subsequent recovery.
4. **Cleanup Deletion Failure**:
   - `delete_tag_conditional` returns `PreconditionFailed` or `NotFound`, but `self.delete_journal(repo).await` fails.
   - **Crucial Qualification**: Cleanup failure propagates and prevents subsequent `mark_ready`; actual journal presence depends on the failure stage:
     - Inspecting `FsStorage::delete_lifecycle_journal` ([`src/storage/fs.rs:1367-1380`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1367)):
     - If `tokio::fs::remove_file(&path)` fails (e.g. `EACCES`, `EPERM`, `EIO` before unlink), the journal file was NOT removed and remains on disk.
     - If `remove_file` succeeded, the file was unlinked from storage (and subsequent `fsync_dir` errors are ignored with `let _ = fsync_dir(...)`).
     - If `delete_journal` fails with `Err`, the `?` returns early, bypassing the subsequent `idx.mark_ready()` attempt; index remains dirty.
   - Note that throughout cleanup, the code uses `let _ = idx.mark_ready();`. If marking ready fails, the error is ignored and the routine continues. Therefore, the code only **attempts to mark ready**; it does not guarantee that the index is ready.

---

## 5. Root Directory Replacement Coherence & Failure Modes

### 5.1 Asymmetric Resolution Topology
The contained seam operates on a pinned directory descriptor (`root_fd` acquired during `FsStorage::try_new`). Conversely, tag mutations operate on dynamic pathnames starting from `self.root`.

```
                      +------------------------------------------+
                      | FsStorage instance created at T0         |
                      | - self.root: PathBuf("/data/storage")    |
                      | - self.reader: root_fd pinned to Inode A |
                      +------------------------------------------+
                                           |
                   +-----------------------+-----------------------+
                   |                                               |
                   v                                               v
    [Contained Tag Reads]                           [Pathname Tag Mutations]
    resolve_tag_seam / get_tag_seam                 mutate_tag / delete_tag_conditional
    - openat2(root_fd, ...)                         - std::fs::File::open("/data/storage/...")
    - Reads always resolve in Inode A               - Resolves current path on filesystem
                   |                                               |
                   +-----------------------+-----------------------+
                                           |
                                  [EVENT AT TIME T1]
                     Root replaced out-of-band / via rename:
                     mv /data/storage /data/storage.old
                     mv /data/storage.new /data/storage  (Inode B)
                                           |
                   +-----------------------+-----------------------+
                   |                                               |
                   v                                               v
    Reads STILL resolve in Inode A                  Mutations NOW resolve in Inode B
    (Original pinned directory tree)                (New replacement directory tree)
```

### 5.2 Read-Old / Delete-New Race Scenarios
Consider `ManifestLifecycleService::delete_tag` executing when root directory replacement occurs between `get_tag_with_version` and `delete_tag_conditional`:

1. **Scenario 1: Tag Missing in Replacement Tree (Tree B)**:
   - `get_tag_with_version` reads from Inode A, returning `(target_digest, version_A)`.
   - Storage root is replaced to Inode B.
   - `delete_tag_conditional` opens `tags/tag` in Tree B, returning `ENOENT`.
   - Result: `ConditionalDeleteResult::NotFound`.
   - Lifecycle service cleans up journal in Tree B, returns `ManifestLifecycleError::TagNotFound`.
   - **Observed State**: Tag in Tree A was untouched. Tree B has no tag.
2. **Scenario 2: Different Bytes Causing Precondition Failure**:
   - Tag exists in Tree B with different content, yielding `version_B != version_A`.
   - `delete_tag_conditional` calculates `current_version = version_B`.
   - Since `version_B != version_A`, returns `ConditionalDeleteResult::PreconditionFailed { current_version: Some(version_B) }`.
   - Lifecycle service cleans up journal in Tree B, returns `ManifestLifecycleError::TagPreconditionFailed`.
   - **Observed State**: Tag in Tree A was untouched. Tag in Tree B is preserved.
3. **Scenario 3: Identical Bytes Allowing Deletion in Replacement Tree**:
   - Tag exists in Tree B with **identical raw bytes**, yielding `version_B == version_A`.
   - `delete_tag_conditional` computes `current_version == version_A` (match!).
   - It executes `std::fs::remove_file(&path)` in **Tree B**!
   - Result: `ConditionalDeleteResult::Deleted`.
   - **Observed State Mutation**: The version token is a SHA-256 hash of the tag file's raw bytes. It does **not bind a filesystem root, device, or inode**. Identical bytes produce an identical token, causing `delete_tag_conditional` to delete the tag from the *new active tree* based on a read of the *old tree*.

### 5.3 Advisory Lock Inode Breakdown
- `mutate_tag` and `delete_tag_conditional` synchronize via `.lock.{tag}` using `fs2::FileExt::lock_exclusive`.
- In Linux, advisory locks are associated with the underlying open file description / inode, not the pathname string.
- If directory replacement occurs, callers resolving before and after rename lock **different inodes**, completely breaking mutual exclusion and allowing concurrent mutations to race.

### 5.4 Operational Stability Proposal: Assumptions & Limitations

The proposal under **`TAG-DEC-04`** evaluates establishing an **Operational Root Stability Requirement** (requiring that the root directory not be renamed or replaced during daemon execution).

**Crucial Clarification: What Operational Stability Does and Does Not Enforce**:
1. **Root Inode Stability Alone is Insufficient**:
   - Keeping the root directory inode stable alone does **not** prevent replacement, renaming, or symlink manipulation of descendant directories (`repos/`, `repos/<repo>/`, `repos/<repo>/tags/`, or `.lock.{tag}`).
   - Pathname mutations (`mutate_tag`, `delete_tag_conditional`) traverse from `self.root` using ambient syscalls. If any descendant directory is replaced or symlinked, ambient pathname mutations will follow those paths, bypassing descriptor containment.
2. **Required Namespace-Stability Assumptions**:
   - *Complete Hierarchy Invariance*: The entire filesystem namespace under `self.root`—including intermediate repository directories, tag directories, and lock files—must remain stable, non-replaced, and free from out-of-band directory renames or symlink insertions during daemon operation.
   - *Exclusive Process Ownership*: The storage tree must be dedicated exclusively to the registry daemon, with no uncoordinated writers or untrusted processes manipulating descendant paths.
   - *Administrative Discipline*: Any storage migration, directory restructuring, or filesystem rebalancing requires stopping the registry service beforehand.
3. **Residual Risks**:
   - If descendant directories are manipulated, ambient mutations can write outside the expected hierarchy or suffer lock evasion.
   - Advisory locks bind inodes, not path strings; descendant directory replacements break mutual exclusion.
   - Version tokens bind only raw bytes, offering zero defense against directory swapping.
4. **Policy vs. Technical Containment**:
   - Operational stability is an **administrative deployment assumption and policy proposal**, NOT an enforced technical containment guarantee.
   - True technical containment for writes requires file descriptor-relative operations (`openat2` with `RESOLVE_BENEATH`/`RESOLVE_NO_SYMLINKS`) for all mutations and lock files (`TAG-DEC-04` Option C).

---

## 6. Payload Policy & Memory Protection Analysis

### 6.1 Unbounded Default (`TagReadLimits { max_payload_bytes: None }`)
- Streams bytes directly into an expanding `Vec<u8>` via `read_to_end`.
- Matches legacy `tokio::fs::read_to_string` and `tokio::fs::read` behavior byte-for-byte.
- **Memory Risk**: If an unauthorized process or corrupted storage creates a multi-gigabyte tag file, reading it into memory causes excessive memory allocation.
- **Allocation Failure Behavior**: Allocation exhaustion may terminate the process; this path does not provide recoverable allocation-failure handling.
- **Writer Inventory**:
  - In `registry-rust`, internal tag creation and mutation route through `FsStorage::mutate_tag`.
  - However, tag files can also be created or modified by out-of-band administrative processes, external synchronization tools, direct filesystem access, or backup restoration.
  - Tag sizes vary: SHA-256 with newline is 72 bytes (`sha256:` + 64 hex + `\n`); SHA-512 with newline is 136 bytes (`sha512:` + 128 hex + `\n`); arbitrary padded whitespace is also accepted by characterization.

### 6.2 Bounded Enforcement (`TagReadLimits { max_payload_bytes: Some(limit) }`)
- Constrains reading using `stream.take(limit + 1)`.
- Rejects streams larger than `limit` bytes with `StorageErrorKind::CorruptData`.
- Uses checked arithmetic (`limit.checked_add(1)`); overflow (e.g. `u64::MAX`) returns `CorruptData`.
- **Concurrency Risk**: A per-object ceiling limits memory allocation per individual stream, but **does not eliminate overall process memory exhaustion risk** under high concurrency with many concurrent readers. Managing aggregate memory pressure requires broader application-level concurrency limits.
- **Recommendation**: An initial cutover should preserve `None` (exact legacy parity) unless an explicit ceiling configuration is formally approved under **`TAG-DEC-02`**. Raw-byte version hashing and current parsing semantics remain unchanged.

---

## 7. Caller-Level Verification Plan

A complete production cutover verification plan must test the integrated system through actual lifecycle and application service callers, strictly separating simulated fake-reader tests, real filesystem integration tests, and unexecuted permission tests.

```
+---------------------------------------------------------------------------------------+
| CALLER-LEVEL VERIFICATION TOPOLOGY                                                    |
|                                                                                       |
|  1. ManifestReadService Suite (Application Service Layer)                             |
|     ├─> Valid tag -> Resolves manifest digest -> Ok(ManifestGetResult)                |
|     ├─> Missing tag -> Returns Err(ManifestReadError::TagNotFound)                    |
|     ├─> Symlink tag -> Returns Err(ManifestReadError::Storage(Io))                    |
|     ├─> Corrupt digest -> Returns Err(ManifestReadError::NotFound) (legacy parity)    |
|     └─> Traversal tag -> Returns Err(ManifestReadError::Storage(InvalidRepoName))     |
|                                                                                       |
|  2. HTTP Router Integration Suite (Router / Wire Layer)                               |
|     ├─> GET valid tag -> HTTP 200 OK (Docker-Content-Digest header)                   |
|     ├─> GET missing tag -> HTTP 404 Not Found (MANIFEST_UNKNOWN)                      |
|     ├─> GET symlink tag -> HTTP 500 Internal Server Error (UNKNOWN)                   |
|     ├─> GET seam traversal -> HTTP 500 Internal Server Error (UNKNOWN)                |
|     ├─> DELETE valid tag -> HTTP 202 Accepted                                         |
|     ├─> DELETE missing tag -> HTTP 404 Not Found (MANIFEST_UNKNOWN / TAG_UNKNOWN)     |
|     └─> DELETE precondition failure -> HTTP 500 Internal Server Error (UNKNOWN)       |
|                                                                                       |
|  3. ManifestLifecycleService Suite (Lifecycle Layer)                                 |
|     ├─> delete_tag happy path: get_tag -> journal written -> delete_conditional -> OK|
|     ├─> Tag read fails (Io / CorruptData):                                            |
|     │     * Occurs AFTER coordination acquisition and prior recovery check            |
|     │     * ABORTS BEFORE new-operation index dirtying (idx.mark_dirty())             |
|     │     * ABORTS BEFORE new-operation journal written to disk                       |
|     │     * ABORTS BEFORE delete_tag_conditional                                      |
|     │     * Releases coordination lease on return (guard dropped)                     |
|     │     * Tag file and index PRESERVED UNCHANGED (under clean precondition)         |
|     ├─> Prior recovery accounting: verify prior pending journal replay is independent |
|     ├─> PreconditionFailed cleanup: verify journal deleted and attempts mark_ready    |
|     ├─> Conditional delete Err: verify journal phase and index dirty under tested fault|
|     └─> Proposed root replacement tests (missing, different, identical bytes)         |
|                                                                                       |
|  4. Real Filesystem Integration Suite (Linux openat2)                                 |
|     ├─> Pinned root descriptor containment under live directory rename                |
|     ├─> Symlink rejection across multiple directory nesting levels                    |
|     └─> Non-regular file rejection (directory, FIFO)                                  |
|                                                                                       |
|  5. Permission-Denied Test Discipline                                                 |
|     └─> Keep explicitly unexecuted unless running in genuine unprivileged environment |
+---------------------------------------------------------------------------------------+
```

### 7.1 Test Suite Execution Roster for Cutover Verification

| Test Identifier | Category | Caller / Entry Point | Scenario / Verification Objective |
| :--- | :--- | :--- | :--- |
| `test_caller_manifest_read_contained_success` | Application Service | `ManifestReadService::get_manifest` | Valid tag resolves digest; asserts `Ok(ManifestGetResult)` with expected digest and payload bytes. |
| `test_caller_manifest_read_missing_tag_error` | Application Service | `ManifestReadService::get_manifest` | Missing tag returns typed service error `Err(ManifestReadError::TagNotFound)`. |
| `test_caller_manifest_read_symlink_rejected_error` | Application Service | `ManifestReadService::get_manifest` | Symlink tag rejected as `Io`; asserts typed error `Err(ManifestReadError::Storage(StorageError::Internal { kind: Io, .. }))`. |
| `test_caller_manifest_read_corrupt_tag_error` | Application Service | `ManifestReadService::get_manifest` | Corrupt tag text parsed as not-found; asserts typed error `Err(ManifestReadError::NotFound)` (legacy parity). |
| `test_caller_manifest_read_traversal_seam_error` | Application Service | `ManifestReadService::get_manifest` | Seam-rejected traversal asserts typed error `Err(ManifestReadError::Storage(StorageError::InvalidRepoName(_)))`. |
| `test_caller_lifecycle_delete_tag_success` | Lifecycle Service | `ManifestLifecycleService::delete_tag` | Happy path: version read, journal written, tag deleted, index updated. |
| `test_caller_lifecycle_delete_tag_clean_precondition_preservation` | Lifecycle Service | `ManifestLifecycleService::delete_tag` | Tag read failure under clean precondition (no journal, clean index) preserves clean state: no new journal, no index dirtying, lease dropped. |
| `test_caller_lifecycle_delete_tag_recovery_accounting` | Lifecycle Service | `ManifestLifecycleService::delete_tag` | Pre-existing journal replayed before tag read; verify prior recovery state is distinct from subsequent tag read failure. |
| `test_caller_lifecycle_delete_tag_precondition_failed_cleanup` | Lifecycle Service | `ManifestLifecycleService::delete_tag` | Conditional delete returns `PreconditionFailed`; journal deleted from disk, attempts to mark index ready, returns `Err(ManifestLifecycleError::TagPreconditionFailed)`. |
| `test_caller_lifecycle_delete_tag_execution_err_journal_retained` | Lifecycle Service | `ManifestLifecycleService::delete_tag` | Conditional delete returns `Err(StorageError)` before file removal; journal retained on disk in `TagDeleteInitiated`, index remains dirty. |
| `test_proposed_lifecycle_root_replacement_missing_tag` | Proposed Lifecycle Test (Unexecuted) | `ManifestLifecycleService::delete_tag` | Pinned reader reads version from Tree A; root replaced to Tree B where tag is missing; `delete_tag_conditional` returns `NotFound`, cleans up journal in Tree B, returns `TagNotFound`. |
| `test_proposed_lifecycle_root_replacement_different_bytes` | Proposed Lifecycle Test (Unexecuted) | `ManifestLifecycleService::delete_tag` | Pinned reader reads version from Tree A; root replaced to Tree B with different tag bytes; `delete_tag_conditional` detects version mismatch, returns `PreconditionFailed`, cleans up journal, returns `TagPreconditionFailed`. |
| `test_proposed_lifecycle_root_replacement_identical_bytes_deletion` | Proposed Lifecycle Test (Unexecuted) | `ManifestLifecycleService::delete_tag` | Pinned reader reads version from Tree A; root replaced to Tree B with identical raw bytes; `delete_tag_conditional` computes matching version and deletes tag in replacement Tree B (demonstrating version token does not bind root/inode). |
| `test_http_router_manifest_get_success_200` | HTTP Router Integration | `axum::Router` (`GET /v2/<repo>/manifests/<tag>`) | Wire response: HTTP 200 OK, `Docker-Content-Digest` and `Content-Type` headers, payload bytes. |
| `test_http_router_manifest_get_missing_tag_404` | HTTP Router Integration | `axum::Router` (`GET /v2/<repo>/manifests/<tag>`) | Wire response: HTTP 404 Not Found, `Docker-Distribution-API-Version`, body `{"errors":[{"code":"MANIFEST_UNKNOWN",...}]}`. |
| `test_http_router_manifest_get_symlink_rejected_500` | HTTP Router Integration | `axum::Router` (`GET /v2/<repo>/manifests/<tag>`) | Wire response: HTTP 500 Internal Server Error, `Docker-Distribution-API-Version`, body `{"errors":[{"code":"UNKNOWN",...}]}`. |
| `test_http_router_manifest_get_seam_traversal_500` | HTTP Router Integration | `axum::Router` (`GET /v2/<repo>/manifests/<tag>`) | Wire response: internal storage `InvalidRepoName` returns HTTP 500 (`UNKNOWN`), distinct from outer pre-validation 400 (`NAME_INVALID`). |
| `test_http_router_delete_tag_missing_404` | HTTP Router Integration | `axum::Router` (`DELETE /v2/<repo>/manifests/<tag>`) | Wire response: missing tag returns HTTP 404 Not Found (`MANIFEST_UNKNOWN` / `TAG_UNKNOWN`). |
| `test_http_router_delete_tag_precondition_failed_500` | HTTP Router Integration | `axum::Router` (`DELETE /v2/<repo>/manifests/<tag>`) | Wire response: simulates version precondition failure; verifies response is HTTP 500 Internal Server Error (`UNKNOWN`), not 412 or 409. |

---

## 8. Rollback Safety & Invariant Guarantees

### 8.1 Zero On-Disk Schema Changes
- Contained tag reading introduces **zero changes to storage layout, directory naming, or tag serialization**.
- Tag files remain plain UTF-8 text containing `<digest>\n` inside `repos/<name>/tags/<tag>`.
- No database migration or journal format change is introduced.

### 8.2 Rollback Mechanics & Constraints
- Reverting restores the prior source implementation. Restoring running behavior requires rebuilding/redeploying and restarting as appropriate.
- Because `TagReadLimits { max_payload_bytes: None }` maintains the legacy unbounded policy, rollback introduces no change to payload limits.
- No serialization/layout migration is introduced, while readability still depends on file contents, permissions, and filesystem state.

### 8.3 Operational Caveats of Rollback
- Reverting code does **not** undo mutations or deletions executed while the service was running.
- If a tag was deleted during cutover under a root-replacement race (Scenario 3), code rollback will not restore the unlinked file.

---

## 9. Decision Inventory & Sign-Off Requirements

The following explicit operational and architectural decisions remain unresolved prior to cutover:

| Decision ID | Canonical / Scope Title | Description | Current Status & Next Step |
| :--- | :--- | :--- | :--- |
| **`O-03`** | **Key and Continuation-Token Contracts** | Canonical quality gate for pagination and token contracts across storage backends. | **OPEN**. Tag listing pagination remains on legacy pathname implementation. |
| **`O-04`** | **Filesystem Write Durability and Containment** | Canonical quality gate for write containment and crash durability. | **OPEN**. Tag mutations remain ambient pathname operations. |
| **`O-05`** | **Broader Filesystem Read Containment** | Canonical quality gate for contained reading across registry storage. | **OPEN**. Tag reading verified in seam; production routing deferred. |
| **`O-06`** | **Typed AWS Mapping and Pinned-MinIO Evidence** | Canonical quality gate for S3 / object storage capability parity. | **OPEN**. Tag operations evaluation ongoing. |
| **`O-13`** | **Hosting, Distribution, and Release Strategy** | Canonical quality gate for deployment topology and distribution. | **OPEN**. Deployment constraints open. |
| **`O-15`** | **Non-Linux Verification** | Canonical quality gate for portability and fallback on non-Linux platforms. | **OPEN**. Seam relies on Linux-specific `openat2`. |
| **`O-16`** | **Earlier Slice 11 Audit/Test-Inventory Evidence** | Canonical quality gate for historical evidence and test coverage audit. | **OPEN**. Reconciliation active. |
| **`D-06`** | **Broader Extraction, Cutover, Compatibility, and Distribution Acceptance** | Overarching acceptance gate for production cutovers. | **OPEN**. Cutover remains unapproved. |
| **`TAG-DEC-01`** | **Production Routing Cutover Authorization** | Authorizing re-routing of `FsStorage::resolve_tag` and `FsStorage::get_tag_with_version` to `tag_seam`. | **DEFERRED**. Production routing remains on legacy ambient operations. |
| **`TAG-DEC-02`** | **Production Tag Payload Ceiling Policy** | Formal determination of whether to enforce a bounded limit in production or maintain unbounded `None`. | **PENDING APPROVAL** (Defaults to `None`). |
| **`TAG-DEC-03`** | **Dangling Symlink Semantic Shift Acceptance** | Formal sign-off on shifting dangling symlink tag reads from HTTP 404 (`NotFound`) to HTTP 500 (`Io`). | **PENDING REVIEW**. Recommended to accept for containment integrity. |
| **`TAG-DEC-04`** | **Root Directory Replacement Coherence Policy** | Evaluating operational root stability requirement versus full descriptor-contained write migration. | **PENDING REVIEW**. Operational stability is an administrative assumption, not a technical guarantee. |
| **`TAG-DEC-05`** | **Error Taxonomy Harmonization for `resolve_tag`** | Evaluation of whether to preserve legacy `NotFound` for corrupt tag content or harmonize with `CorruptData`. | **DEFERRED** (Seam strictly preserves legacy `NotFound` mapping). |
| **`TAG-DEC-06`** | **Rollback Mutation Non-Reversibility Acknowledgment** | Acknowledging that code rollback does not revert on-disk mutations or race-induced deletions. | **PENDING REVIEW**. |

---

## 10. Conclusion & Recommendation

Recorded evidence: 28 seam tests passed, 10 characterization tests passed, two permission tests remained unexecuted, and non-Linux verification remains outstanding. The seam faithfully preserves caller parsing contracts, raw-byte version hashing, and unbounded payload policies while enforcing kernel-level descriptor containment against symlinks and path traversals.

However, **production cutover must remain unapproved** until:
1. **Decision `TAG-DEC-04`** is resolved, acknowledging that operational stability is an administrative assumption rather than a technical containment guarantee, and evaluating descendant directory stability.
2. **Decision `TAG-DEC-02`** formally confirms whether production tag reading should default to unbounded `None` or an explicit bounded limit.
3. **Canonical Gates `O-04`, `O-05`, `O-15`, and `D-06`** remain explicitly **OPEN**.

Until formal authorization is granted, `FsStorage::resolve_tag` and `FsStorage::get_tag_with_version` must remain 100% on their existing ambient implementations.
