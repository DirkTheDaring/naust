> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. The promotion shipped (`f1d6d9c`, later `tag_domain` `32c42c6`); references `list_tag_files`, which no longer exists. Supersedes the earlier readiness assessment in substance (same promotion, later baseline).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Production-Cutover Readiness Assessment: Contained Filesystem Tag Listing

- **Document:** `docs/architecture/filesystem-tag-listing-production-cutover-readiness.md`
- **Target Repository:** `registry-rust` (HEAD: `02cfa0789e5b3ad274c93632af90b7fadb66c62f`)
- **Dependency Repository:** `storage-layer-rust` (HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`, strictly read-only)
- **Status:** **ASSESSMENT COMPLETED — WIRING AND RESOURCE CONTRACTS CORRECTED — DECISIONS PENDING — NOT COMMITTED**
- **Canonical Quality Gates:** All eight quality gates remain explicitly **OPEN** (`O-03, O-04, O-05, O-06, O-13, O-15, O-16, D-06`).

---

## 1. Executive Summary & Assessment Scope

This document establishes the current production-readiness state for promoting contained filesystem tag listing from its test-only integration seam (`src/storage/fs/tag_listing.rs`) into production routing (`src/storage/fs.rs`).

Prior readiness assessments (`docs/architecture/filesystem-tag-listing-production-readiness-assessment.md`) identified critical caller-side failure swallowing: multiple lifecycle, migration, and supervisor callers suppressed storage errors into empty discovery results. Under certain retention configurations, treating transient I/O or permissions failures as empty tag listings risked premature manifest deletion, unlinking proxy blob memberships, or skipping repository migration. Over three successive slices, all identified caller sites were hardened:
1. **Lifecycle Hardening** (`96c0729bcd02c5f44fbfa13c5b0d4ea77bff6a86`): Replaced wildcard suppression at three recovery and eviction sites in `src/manifest_lifecycle.rs` with fail-closed error propagation while preserving explicit `NotFound` compatibility.
2. **Membership Migration Hardening** (`5779f7f97f1109bcbb15bb6ad47e845bd4686bbf`): Hardened planning, application, and verification in `src/membership_migration.rs` to propagate errors, attempt persistence of bounded failure checkpoints, and prevent cursor advancement past unmigrated repositories.
3. **Supervisor Hardening** (`02cfa0789e5b3ad274c93632af90b7fadb66c62f`): Hardened `compute_protected_blobs` in `src/supervisor.rs` under `KeepLatestCachedSemver` to propagate non-`NotFound` listing errors before candidate directory scanning in `proxy_gc_once`.

While caller-side error swallowing has been hardened across the three targeted domains, **production cutover cannot be declared ready solely because those three commits exist**. This assessment:
- Conducts an exhaustive review of all callers across the codebase.
- Evaluates the exact behavioral differences between legacy production storage and the contained seam.
- Clarifies the startup execution boundary, distinguishing synchronous constructors from asynchronous factory callers wrapping initialization in `tokio::task::spawn_blocking`.
- Resolves the runtime reader vs. payload adapter wiring, establishing that passing `self.reader.as_ref()` for both directory enumeration and payload reading is directly supported without introducing new adapters.
- Audits `Config` struct literals across the codebase and specifies a bounded `Clone`-based constructor-extension proposal.
- Corrects the payload limit model, validating against canonical SHA-512 text length and whitespace handling.
- Distinguishes sequential candidate memory and `take(limit + 1)` overflow detection from cumulative I/O work and repeated pagination scans.
- Corrects enumeration accounting to document that `.` and `..` are skipped prior to limit checks, while dotfiles and non-regular files consume budgets.
- Defines the smallest safe production cutover, mandating retention of `FsStorage::list_tag_files`.
- Details the acceptance test contract (including point reads `resolve_tag` and `get_tag_with_version`) and formulates the specific pending decision gates.

Production storage routing remains legacy (`FsStorage::list_tags` and `FsStorage::list_tags_page`); descriptor-relative `contained_list_tags_seam` and `contained_list_tags_page_seam` remain strictly test-only under `#[cfg(test)]`.

---

## 2. Established Architectural Boundaries

To preserve strict precision, the following boundaries are established and maintained throughout this assessment:

1. **Enumeration vs. Payload Reading**:
   `list_tags` enumerates entry names without reading tag payload files. `list_tags_page` reads and parses candidate payload files. Subsequent tag resolution (`resolve_tag`, `get_tag_with_version`) and manifest reading (`get_manifest`) operate on separate call branches with independent error handling.
2. **Full Candidate Reading in Paged Listing**:
   Contained nonzero-page listing (`contained_list_tags_page_seam`) opens and parses all retained candidate files in the directory before sorting and slicing the requested page. It does **not** read only the requested page slice.
3. **Zero-Page Behavior**:
   Zero-sized pages (`page_limit == 0`) validate the repository path and probe/enumerate the directory, but short-circuit prior to candidate processing, opening zero candidate payload files.
4. **Filtering and Rejection Scope**:
   The seam skips non-UTF-8 names, observed non-regular directory entries (directories, symlinks, FIFOs), dot-prefixed filenames, and malformed digest strings. Symlinks in the directory path and post-enumeration file substitutions are rejected by Linux `openat2` resolution restrictions (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`).
5. **Containment Limits**:
   Containment relies on `storage_fs::FsMetadataReader` and Linux `openat2` on a pinned root directory descriptor. It does **not** provide transaction isolation, mount isolation, hard-link isolation, or coherence between pinned reads and ambient pathname mutations after root directory replacement.
6. **`NotFound` Compatibility Scope**:
   The `NotFound`-as-empty compatibility policy was explicitly selected for specific caller sites in lifecycle, migration, and supervisor. In legacy filesystem storage, a missing `tags/` directory within an existing repository directory already returns an empty successful listing (`Ok(Vec::new())`).
7. **Supervisor Limits & Invariants**:
   Supervisor `resolve_tag` error suppression remains unchanged and is documented as a retained limitation. `proxy_gc_once` calculates candidate sizes and emits logs; it does **not** persistently evict or delete files.
8. **Membership Sweep Policies**:
   In `src/gc_service.rs`, initial reference-check errors abort the sweep iteration (Site 1, line 611 via `?`), while pre-unlink revalidation errors treat the blob as referenced to preserve the candidate and continue (Site 2, line 654 via `Err(_) => true`). Both policies are preserved unmodified.
9. **Index Sync vs. Rebuild**:
   Per-repository index sync (`sync_repo_manifests_and_tags`) stages discovery before applying Sled writes, but application is not established as atomic across discrete Sled operations. Index rebuild (`rebuild`) clears four named trees (`tag_to_root`, `root_counts`, `rev_edges`, `repo_memberships`) before repository discovery; a listing failure during rebuild leaves partial state marked `Building`.

---

## 3. Caller Trace & Production Readiness Mapping

Every call site of `list_tags` and `list_tags_page` across `registry-rust` was inspected against committed source. The table below classifies each caller into one of four readiness categories:
- **Addressed by committed hardening**: Hardened by commits `96c0729`, `5779f7f`, or `02cfa07`.
- **Compatible under explicit decision**: Existing caller behavior aligns with safe fail-closed or read-only requirements.
- **Retained limitation**: Known behavioral boundary documented and accepted without blocking cutover.
- **Pending decision**: Area requiring explicit architectural approval before cutover.

### Caller Classification Matrix

| Subsystem & Call Site | Invoked Method | Error Handling & Mutation Ordering | Classification | Architectural Analysis & Safety Guarantees |
|---|---|---|---|---|
| **Lifecycle Recovery: Delete**<br>`src/manifest_lifecycle.rs:585` | `list_tags_page` | `StorageError::NotFound => (Vec::new(), None)`<br>`Err(e) => return Err(Storage(e))` | **Addressed by committed hardening** (`96c0729`) | In `recover_pending_journal_under_lock`. Halts recovery loop on storage failure. Prevents subsequent manifest deletion, referrer removal, and journal deletion. Earlier completed tag deletions remain recorded in journal. |
| **Lifecycle Recovery: ProxyEvict**<br>`src/manifest_lifecycle.rs:666` | `list_tags_page` | `StorageError::NotFound => (Vec::new(), None)`<br>`Err(e) => return Err(Storage(e))` | **Addressed by committed hardening** (`96c0729`) | In `recover_pending_journal_under_lock`. Halts recovery on storage failure. Prevents subsequent manifest deletion, proxy blob unlinking, and journal cleanup. Prior tag alias deletion remains recorded in journal. |
| **Lifecycle Proxy Eviction**<br>`src/manifest_lifecycle.rs:1220` | `list_tags_page` | `StorageError::NotFound => (Vec::new(), None)`<br>`Err(e) => return Err(Storage(e))` | **Addressed by committed hardening** (`96c0729`) | In `evict_proxy_cached_entry`. Halts active eviction on storage failure. Step 4 tag alias deletion remains in journal at `ProxyTagDeleted`, but Step 6 manifest deletion and proxy blob unlinking are bypassed. |
| **Lifecycle Delete (Step 4)**<br>`src/manifest_lifecycle.rs:1390` | `list_tags_page` | `StorageError::NotFound => (Vec::new(), None)`<br>`Err(e) => return Err(Storage(e))` | **Compatible under explicit decision** | In `delete_manifest`. Halts tag deletion loop on error. Earlier pages already deleted tags; journal records partial state for subsequent recovery. |
| **Lifecycle Delete (Step 5)**<br>`src/manifest_lifecycle.rs:1487` | `list_tags_page` | Propagates error directly via `?` | **Compatible under explicit decision** | In `delete_manifest`. Authoritative pre-delete check fails closed before manifest deletion attempt. |
| **Migration: Planning**<br>`src/membership_migration.rs:29` | `list_tags` | `StorageError::NotFound => Vec::new()`<br>`Err(e) => return Err(e)` | **Addressed by committed hardening** (`5779f7f`) | Dry-run operation in `plan_membership_migration` with zero writes. Fails closed immediately on non-`NotFound` listing errors. |
| **Migration: Application**<br>`src/membership_migration.rs:141` | `list_tags` | `StorageError::NotFound => Vec::new()`<br>`Err(e) => record Failed & return Err(e)` | **Addressed by committed hardening** (`5779f7f`) | In `apply_membership_migration`. On error: transitions checkpoint to `Failed`, records bounded diagnostic info ($\le 512$ UTF-8 bytes), attempts checkpoint save (logs warning if save fails), preserves cursor, and propagates error. Prevents bypassing failed repo. |
| **Migration: Verification**<br>`src/membership_migration.rs:248` | `list_tags` | `StorageError::NotFound => Vec::new()`<br>`Err(e) => return Err(e)` | **Addressed by committed hardening** (`5779f7f`) | In `verify_membership_migration`, propagates error via `?`. In caller `apply_membership_migration`, halts before `mark_membership_ready`. Checkpoint remains in `Verifying`. |
| **Supervisor: Protection**<br>`src/supervisor.rs:940` | `list_tags` | `StorageError::NotFound => Vec::new()`<br>`Err(e) => return Err(format!(...))` | **Addressed by committed hardening** (`02cfa07`) | In `compute_protected_blobs`. Propagates error to `proxy_gc_once`, which aborts via `?` before candidate directory scanning or size calculations. Task supervisor logs warning and retries on next interval. |
| **Supervisor: Resolve Tag**<br>`src/supervisor.rs:958` | `resolve_tag` | `if let Ok(digest) = storage.resolve_tag(...)` | **Retained limitation** | Tag resolution errors continue to be silently swallowed in this slice. Characterized by dedicated unit test; does not cause physical eviction. |
| **Supervisor: Candidate Scan**<br>`src/supervisor.rs:830` | N/A (`proxy_gc_once`) | Calculates candidates against `max_cache_bytes` | **Compatible under explicit decision** | `proxy_gc_once` performs zero persistent evictions, zero unlinks, and zero file deletions. Physical reclamation managed by `BlobGcService`. |
| **Catalog API**<br>`src/http_api/catalog.rs:360` | `list_tags` | `StorageError::NotFound => Vec::new()`<br>Other errors return HTTP 500 | **Compatible under explicit decision** | Read-only catalog enumeration. Fails closed on infrastructure error. |
| **Tag Query Service**<br>`src/application/tags.rs:49, 97` | `list_tags` | Maps `NotFound` to `TagQueryError::NotFound`; others to `TagQueryError::Storage(e)` | **Compatible under explicit decision** | Handled by `TagQueryService::list_tags` and `TagQueryService::query_tags`. Mapped to HTTP 404 (`NAME_UNKNOWN`) or HTTP 500 (`errors::internal_error()`). |
| **Ref-Index: Sync**<br>`src/blob_ref_index.rs:634` | `list_tags_page` | Phase 1 discovery uses `?`; aborts before Phase 2 | **Compatible under explicit decision** | Read-only storage discovery stages tags before index application. Listing error aborts before any Sled index modification. |
| **Ref-Index: Rebuild**<br>`src/blob_ref_index.rs:724` | `list_tags_page` (via sync) | Aborts loop on error via `?` | **Retained limitation** | `rebuild` clears 4 Sled trees prior to repo discovery. Listing error aborts rebuild, leaving index in `Building` state and partial cleared trees. |
| **Ref-Index: Conservative Refresh**<br>`src/blob_ref_index.rs:768` | `list_tags` | `NotFound` continues loop; other errors return `Err` | **Retained limitation** | Eager Sled inserts applied per tag; single `db.flush()` at end. Listing error aborts loop; prior in-memory Sled mutations remain visible. |
| **Blob Delete Safety (Any Repo)**<br>`src/blob_delete_safety.rs:96` | `list_tags` | `NotFound` continues loop; other errors return `Err` | **Compatible under explicit decision** | Fail-closed: storage error aborts check, preventing blob deletion. |
| **Blob Delete Safety (Single Repo)**<br>`src/blob_delete_safety.rs:132` | `list_tags` | `NotFound` returns `Ok(None)`; other errors return `Err` | **Compatible under explicit decision** | Fail-closed: storage error aborts check, preventing blob deletion. |
| **GC Membership Sweep: Site 1**<br>`src/gc_service.rs:611` | `list_tags` (via safety) | Propagates error directly via `?` | **Compatible under explicit decision** | Sweep iteration aborts immediately on reference-check error. |
| **GC Membership Sweep: Site 2**<br>`src/gc_service.rs:654` | `list_tags` (via safety) | `Err(_) => true` (preserves candidate) | **Compatible under explicit decision** | Pre-unlink revalidation error treats blob as referenced; preserves candidate and continues sweep without unlinking. |

---

## 4. Detailed Behavioral Compatibility Matrix

The table below contrasts legacy production implementation (`FsStorage::list_tags` and `FsStorage::list_tags_page`) with the committed contained seam (`contained_list_tags_seam` and `contained_list_tags_page_seam`).

> [!NOTE]
> In `registry-rust` error taxonomy, `StorageErrorKind` is a C-like unit enum (`StorageErrorKind::Io`, `StorageErrorKind::PermissionDenied`, etc.). String messages are carried by the container struct `StorageError::Internal { kind: StorageErrorKind, message: String }`, typically constructed via helpers such as `StorageError::io(msg)` and `StorageError::permission_denied(msg)`.

| Dimension | Legacy Production Storage (`FsStorage`) | Contained Seam (`tag_listing.rs`) | Compatibility Assessment & Required Decisions |
|---|---|---|---|
| **Repository Path Validation** | No upfront path validation. Ambient `self.root.join("repos").join(name)`. Allows `../` path traversal. Embedded NUL rejected by stdlib `ErrorKind::InvalidInput`. | Upfront structural validation via `validate_path_component`: rejects empty strings, leading/trailing slashes, repeated slashes, `.`, `..`, backslashes, NUL, and ASCII controls with `StorageError::InvalidRepoName`. | **Proposed change requiring approval**. Eliminates directory traversal hazards; rejects malformed names before any filesystem syscall. |
| **Containment Enforcement** | None. Ambient path operations follow symlinks and can escape storage root. | Linux `openat2` on pinned root descriptor with `RESOLVE_BENEATH \| RESOLVE_NO_SYMLINKS \| RESOLVE_NO_MAGICLINKS`. | **Proposed change requiring approval**. Guarantees kernel-enforced descriptor containment beneath root. |
| **Missing Repository (`list_tags`)** | Probes `repos/<repo>` via `tokio::fs::metadata`. Missing repo returns `StorageError::NotFound`. | Enumerates `repos/<repo>/tags`. Only on `NotFound`, probes `repos/<repo>` descriptor up to probe limits. Missing repo returns `StorageError::NotFound`. | **Compatible (Preserved)**. Caller receives `StorageError::NotFound` without redundant probe when `tags/` exists. |
| **Missing Repository (`list_tags_page`)** | Calls `list_tag_files(repo)`, which queries `tags/` directly without probing repo. Missing repo swallowed as `Ok(Vec::new())`, returning `Ok((Vec::new(), None))`. | Enumerates `repos/<repo>/tags`. Only on `NotFound`, probes `repos/<repo>`. Missing repo returns empty terminal page `Ok((Vec::new(), None))`. | **Compatible (Preserved)**. Preserves legacy caller expectation of empty page for nonexistent repository. |
| **Missing `tags/` in Existing Repo** | `list_tags` and `list_tags_page` return empty success `Ok(Vec::new())` and `Ok((Vec::new(), None))`. | Repo probe confirms repo exists; returns empty success `Ok(Vec::new())` and `Ok((Vec::new(), None))`. | **Compatible (Preserved)**. Preserves empty tag collection for repositories without tags. |
| **Directory & Probe Error Mapping** | `tokio::fs::read_dir` errors mapped to `StorageError::Internal { kind: StorageErrorKind::Io, message }`. | Mapped via `translate_tag_dir_error`:<br>- `FsDirError::PermissionDenied` -> `StorageError::permission_denied(...)`<br>- `FsDirError::LimitExceeded` -> `StorageError::backend(...)`<br>- `FsDirError::SyscallUnsupported` / `PlatformUnsupported` -> `StorageError::configuration(...)`<br>- `FsDirError::NotADirectory` -> `StorageError::corrupt_data(...)`<br>- Vanished dir (`NotFound` during readdir) -> `StorageError::io(...)` | **Proposed change requiring approval**. Enriches error classification with typed taxonomy; surfaces directory permission denials as `StorageErrorKind::PermissionDenied`. |
| **Error Precedence: Tags vs. Repo** | `list_tags` probes repo first, then tags. `list_tags_page` queries tags directly. | Tags-first: tags directory errors take precedence. Repo probe errors evaluated only if tags directory returns `NotFound`. | **Proposed change requiring approval**. Eliminates redundant repo probe on happy path; prioritizes tags directory errors. |
| **Child Symlinks & Non-Regular Files** | `list_tags` includes symlink names. `list_tags_page` follows symlinks via `tokio::fs::read_to_string`. | Skips observed non-regular entries (`entry.file_type() != DirEntryType::Regular`). Symlinks, subdirectories, and FIFOs are omitted from listing. | **Proposed change requiring approval**. Rejects non-regular entries; eliminates symlink-following vulnerabilities. |
| **Non-UTF-8 Entry Names** | Silently skipped via `entry.file_name().to_str()`. | Silently skipped via `entry.name().to_str()`. | **Compatible (Preserved)**. Unrepresentable non-UTF-8 entries omitted from UTF-8 string results. |
| **Dot-Prefixed Filenames** | `list_tag_files` already skips all `!file_name.starts_with('.')` before `list_tags_page` inspects `.tmp.` and `.lock.`. | Skips all dotfiles (`name_str.starts_with('.')`). | **Compatible (Preserved)**. Identical dotfile exclusion behavior; redundant legacy checks eliminated. |
| **Payload Acquisition `NotFound`** | `list_tags_page` swallows read errors via `if let Ok(content)`. | File deleted between enumeration and payload open (`ReadError::NotFound`) is silently skipped (`continue`). | **Compatible (Preserved)**. Gracefully handles concurrent tag deletion during paged read. |
| **Payload Acquisition Errors** | `list_tags_page` silently swallows permission errors and backend errors. | Mapped via `super::read_adapter::translate_payload_read_error`:<br>- `ReadError::PermissionDenied` -> `StorageError::io(...)`<br>- `ReadError::Backend` -> `StorageError::io(...)` or backend. | **Proposed change requiring approval**. Acquisition failures fail closed instead of silently omitting tags. Note payload permission denials map to `StorageErrorKind::Io`, unlike directory permission denials. |
| **Payload Stream Errors** | `list_tags_page` silently swallows stream aborts. | Mapped in `drain_tag_stream`:<br>- Stream read error -> `StorageError::io(...)`<br>- Stream length exceeds ceiling -> `StorageError::corrupt_data(...)`. | **Proposed change requiring approval**. Stream reading failures fail closed rather than reporting zero tags. |
| **Invalid UTF-8 in Payload** | `list_tags_page` silently swallows invalid UTF-8 via `read_to_string` error. | Fails closed with `StorageError::io(format!("invalid UTF-8 in tag payload..."))`. | **Proposed change requiring approval**. Detects payload corruption rather than omitting entry. |
| **Empty or Malformed Digest Text** | `list_tags_page` silently skips file if `Digest::parse` fails. | Candidate omitted (`Digest::parse(content_str.trim())` must succeed). | **Compatible (Preserved)**. Corrupt or unparsable digest text omitted from valid `(tag, digest)` results. |
| **Zero-Page Requests (`page_limit == 0`)** | Enumerates directory and reads/parses ALL tag payloads into memory before slicing to 0. | Validates repo path, enumerates `tags/` (or probes repo), then short-circuits returning `Ok((Vec::new(), None))` with zero payload opens. | **Proposed change requiring approval**. Enforces directory validation while saving all payload read I/O. |
| **Pagination Arithmetic & Token** | Sorts by tag name. Binary search on cursor; uses `start_idx + page_limit` without overflow protection. | Sorts unstable by tag name. Binary search on cursor; uses `idx.saturating_add(1)` and `start_idx.saturating_add(page_limit)`. Returns last item as continuation token if more entries remain. | **Proposed change requiring approval**. Eliminates integer overflow hazard; ensures saturating pagination bounds. |
| **Enumeration Limits** | Completely unbounded. Can consume unlimited memory on large directories. | Enforces caller-specified limits on entries and cumulative filename bytes (`DirEnumerationLimits`). Counts all `readdir` entries (except `.` and `..`) before filtering. | **Proposed change requiring approval**. Prevents memory exhaustion from runaway directories. |
| **Payload Limits** | Completely unbounded. Reads entire file into memory via `read_to_string`. | Enforces `payload_limits.max_payload_bytes` per candidate stream during `drain_tag_stream` (conditional on `Some(limit)`). | **Proposed change requiring approval**. Protects against oversized payload file exhaustion. |

---

## 5. Runtime Wiring, Configuration Architecture & Resource Contracts

### 5.1 Reader vs. Payload Adapter Wiring & Startup Offload

#### Structural Wiring
An inspection of `FsStorage` fields in `src/storage/fs.rs:189-201` reveals:
```rust
pub struct FsStorage {
    ...
    reader: std::sync::Arc<storage_fs::FsMetadataReader>,
    read_adapter: std::sync::Arc<read_adapter::FsBlobCasReadAdapter<storage_fs::FsMetadataReader>>,
    ...
}
```

1. **`storage_fs::FsMetadataReader`**:
   - Implements [`storage_core::ObjectMetadataReader`] for descriptor-relative metadata lookups (`head`).
   - Implements [`storage_core::ObjectPayloadReader`] for descriptor-relative payload opening (`open_payload`), defined in `crates/storage-fs/src/reader/payload.rs:4`.
   - Implements [`TagDirEnumerator`] in `src/storage/fs/tag_listing.rs:29-37` by delegating directly to inherent `self.enumerate_dir(target, limits)`.
2. **`read_adapter::FsBlobCasReadAdapter<storage_fs::FsMetadataReader>`**:
   - Implements [`BlobCasReader`] (`head_blob`, `open_blob`) for CAS blobs at primary (`blobs/<alg>/<prefix2>/<hex>`) and quarantine keys.
   - It does **not** implement [`ObjectPayloadReader`] or [`TagDirEnumerator`], and has zero knowledge of repository tags or `repos/...` key hierarchies.
3. **Delegation Without Adapter Changes**:
   Because `storage_fs::FsMetadataReader` implements both [`TagDirEnumerator`] and [`ObjectPayloadReader`], passing `self.reader.as_ref()` directly satisfies both trait bounds:
   ```rust
   async fn list_tags(&self, name: &str) -> Result<Vec<String>, StorageError> {
       tag_listing::contained_list_tags_seam(
           self.reader.as_ref(), // D: TagDirEnumerator + ?Sized
           name,
           self.tag_listing_limits.repo_probe_limits,
           self.tag_listing_limits.tags_dir_limits,
       )
       .await
   }

   async fn list_tags_page(
       &self,
       name: &str,
       continuation_token: Option<&str>,
       page_limit: usize,
   ) -> Result<(Vec<(String, Digest)>, Option<String>), StorageError> {
       tag_listing::contained_list_tags_page_seam(
           self.reader.as_ref(), // D: TagDirEnumerator + ?Sized
           self.reader.as_ref(), // P: ObjectPayloadReader + ?Sized
           name,
           continuation_token,
           page_limit,
           self.tag_listing_limits.repo_probe_limits,
           self.tag_listing_limits.tags_dir_limits,
           self.tag_listing_limits.payload_limits.clone(),
       )
       .await
   }
   ```
   No new adapter types or wrapper layers are required.

#### Startup Offload Boundary
A critical architectural distinction must be maintained between the synchronous storage constructor and its asynchronous factory callers:
- **Synchronous Constructor:** `FsStorage::try_new_with_gc_limits` (`src/storage/fs.rs:308-312`) executes synchronously on whichever thread invokes it:
  ```rust
  let reader = storage_fs::FsMetadataReader::open(&root)
      .map_err(read_adapter::map_fs_startup_error)?;
  reader
      .probe_capability()
      .map_err(read_adapter::map_fs_startup_error)?;
  ```
- **Asynchronous Factory Callers:** In `src/storage/mod.rs:971-1020`, asynchronous factory functions wrap filesystem storage construction in `tokio::task::spawn_blocking`:
  ```rust
  pub(crate) async fn storage_wiring_try_from_config_async_with_factory<F>(
      config: &Config,
      storage_factory: F,
  ) -> Result<StorageWiring, StorageError>
  where
      F: FnOnce(&Config) -> Result<StorageWiring, StorageError> + Send + 'static,
  {
      match config.storage_backend {
          StorageBackend::Filesystem => {
              let config_clone = config.clone();
              tokio::task::spawn_blocking(move || storage_factory(&config_clone))
                  .await
                  .map_err(...)
          }
          StorageBackend::S3 => storage_factory(config),
      }
  }
  ```
  Likewise, `proxy_cache_storage_try_from_config_async_with_factory` wraps proxy-cache filesystem storage construction in `spawn_blocking`.
  Therefore, in production runtime paths, capability probing and initial directory descriptor acquisition execute within Tokio's blocking threadpool. This offload pattern must be preserved and tested during cutover.

---

### 5.2 Configuration Conventions & Config Literal Audit

#### Proposed Configuration Keys & Precedence
```toml
[storage.fs]
tag_listing_max_entries = 10000
tag_listing_max_name_bytes = 1500000
tag_listing_repo_probe_max_entries = 64
tag_listing_repo_probe_max_name_bytes = 4096
tag_listing_max_payload_bytes = 1024
```

Environment variable resolution for each field follows the established pattern:
```rust
let fs_tag_listing_max_entries = env_usize_opt(&[
    "REGISTRY__STORAGE__FS__TAG_LISTING_MAX_ENTRIES",
    "STORAGE_FS_TAG_LISTING_MAX_ENTRIES",
])?
.or(file_cfg.storage.fs.tag_listing_max_entries)
.unwrap_or(DEFAULT_TAG_LISTING_MAX_ENTRIES);
```

#### Audit of `Config` Struct Literals
`Config` does not use `#[non_exhaustive]`, and four locations in the codebase construct `Config` using raw struct literals:
1. `src/config.rs:2568` (`Config::load_with_overrides`)
2. `src/gc_service.rs:835` (`minimal_config`)
3. `src/http_api/handlers/tests.rs:565` (`minimal_config_for_token_tests`)
4. `tests/support/gc_coordination.rs:1130` (`test_config`)

Adding top-level `fs_tag_listing_*` fields to `Config` will require updating all four sites, exactly as occurred during earlier manifest listing and GC discovery cutovers.

#### Bounded `Clone`-Based Limits Proposal
`TagReadLimits` derives `Clone`, but does **not** derive `Copy` (`src/storage/fs/tag_read.rs:64`). Therefore, any container struct must use a `Clone`-based design:
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagListingLimits {
    pub repo_probe_limits: storage_fs::DirEnumerationLimits,
    pub tags_dir_limits: storage_fs::DirEnumerationLimits,
    pub payload_limits: tag_read::TagReadLimits,
}

impl Default for TagListingLimits {
    fn default() -> Self {
        Self {
            repo_probe_limits: storage_fs::DirEnumerationLimits::new(64, 4_096),
            tags_dir_limits: storage_fs::DirEnumerationLimits::new(10_000, 1_500_000),
            payload_limits: tag_read::TagReadLimits {
                max_payload_bytes: Some(1_024),
            },
        }
    }
}
```
In `src/storage/fs.rs`, introduce `FsStorage::try_new_with_all_limits(...)` taking all limit structures, while retaining `new`, `try_new`, `try_new_with_limits`, and `try_new_with_gc_limits` (delegating to `TagListingLimits::default()`).

---

### 5.3 Enumeration Accounting Contract

An inspection of `crates/storage-fs/src/dir.rs:527-532` reveals:
```rust
if name_bytes == b"." || name_bytes == b".." {
    continue;
}

let name_len = name_bytes.len();
let new_total = account_entry(entries.len(), total_name_bytes, name_len, &limits)?;
```

- **`.` and `..` Exclusion**: The standard dot entries (`.` and `..`) are skipped **before** `account_entry` is called. They do **not** consume `DirEnumerationLimits` entries or name bytes.
- **Entries That DO Consume Budgets**:
  All other directory entries returned by `readdir` consume enumeration budgets before any tag-listing filtering occurs:
  1. *Dotfiles*: Files beginning with `.` (e.g. `.tmp.*`, `.lock.*`, `.gitignore`) consume entry and filename-byte budgets.
  2. *Subdirectories*: Any subdirectories within `tags/` consume budgets.
  3. *Non-Regular Entries*: Symlinks, FIFOs, sockets, and character/block devices consume budgets.
  4. *Non-UTF-8 Entries*: Files with invalid UTF-8 names consume budgets before being skipped during string conversion.
- If a directory contains 5,000 dotfiles and 5,000 tag files, total observed entries will be 10,000. If `max_entries` is configured to 8,000, enumeration fails with `LimitExceeded` before caller filtering can isolate the 5,000 tag files.

---

### 5.4 Payload Memory/Work Bounds & Overflow Detection

#### Sequential Draining and `take(limit + 1)`
In `drain_tag_stream` (`src/storage/fs/tag_read.rs:160-178`):
```rust
let take_limit = limit.checked_add(1)...;
let mut limited_stream = stream.take(take_limit);
let mut buffer = Vec::new();
limited_stream.read_to_end(&mut buffer).await...;

if buffer.len() as u64 > limit {
    return Err(StorageError::corrupt_data(format!(
        "tag payload stream length exceeds limit of {limit} bytes"
    )));
}
```

1. **Active Payload Buffer Bounds**:
   - For a candidate within limits, `buffer` allocates up to `limit` logical bytes (plus `Vec` reallocation capacity).
   - For an oversized candidate, `buffer` reads up to **`limit + 1` bytes** before failing the check and being rejected with `StorageErrorKind::CorruptData`.
   - At any instant during candidate reading, exactly **one** candidate payload buffer is active in memory. It is dropped at the end of each candidate loop iteration.
2. **Cumulative Bytes Read vs. Rejection**:
   - On a successful call with $K$ valid candidates, cumulative streaming I/O across candidates reaches $\sum_{i=1}^K \text{len}_i \le K \times \text{limit}$.
   - If candidate $j$ is oversized, up to $\text{limit} + 1$ bytes are read for that candidate before the call fails closed.
3. **Parsed Result Retained Memory**:
   - Parsed candidates are stored as `(String, Digest)` pairs in `tags_with_digest`.
   - Total heap consumption depends on individual tag name lengths, `String` capacity, and vector allocation growth; it cannot be characterized by a flat constant estimate.
4. **Repeated Pagination Work**:
   - The seam performs no caching and provides no cursor pushdown to the filesystem. Successive page requests re-enumerate the directory and re-acquire every candidate payload before sorting and slicing.

---

### 5.5 Proposed Limits and Operational Choices

1. **Proposed Validation Bounds**:
   - `tag_listing_max_entries`: validated $\ge 1$.
   - `tag_listing_max_name_bytes`: validated $\ge 128$ (accommodates a 128-byte OCI tag name).
   - `tag_listing_repo_probe_max_entries`: validated $\ge 1$.
   - `tag_listing_repo_probe_max_name_bytes`: validated $\ge 64$.
   - `tag_listing_max_payload_bytes`: validated $\ge 256$ and $< \text{u64::MAX}$.
2. **Operational Choice on Minimum Payload Limit**:
   - The strict canonical SHA-512 text length is 135 bytes (`sha512:` + 128 hex digits).
   - The proposed 256-byte minimum validation lower bound is an **operational choice** to provide safe headroom for standard trailing newlines (`\n`, `\r\n`) and whitespace. No finite limit preserves arbitrary surrounding whitespace.
3. **Decoupling from Point Reads**:
   Point reads in `FsStorage::resolve_tag` and `FsStorage::get_tag_with_version` continue to use `TagReadLimits::default()`, where `max_payload_bytes: None`.

---

## 6. Definition of the Smallest Safe Production Cutover

### 6.1 Exact Proposed Modification Paths

1. **`src/storage/fs/tag_listing.rs`**:
   - Remove `#[cfg(test)]` attribute from the module header and functions.
   - Retain `TagDirEnumerator` trait and blanket implementation for `storage_fs::FsMetadataReader`.
   - Keep test suite in `tag_listing.rs` under `#[cfg(test)] mod tests`.
2. **`src/storage/fs.rs`**:
   - Change `#[cfg(test)] pub(crate) mod tag_listing;` to `pub(crate) mod tag_listing;`.
   - Store configured `tag_listing_limits: TagListingLimits` in `FsStorage`.
   - Route `FsStorage::list_tags` and `FsStorage::list_tags_page` to the contained seam functions.
   - **MANDATORY RETENTION: `list_tag_files`**:
     [`FsStorage::list_tag_files`](src/storage/fs.rs#L646) must **NOT** be deleted or altered. It is actively called by [`FsStorage::delete_manifest`](src/storage/fs.rs#L1796) on the manifest deletion mutation path.
3. **`src/config.rs`**:
   - Add tag listing configuration fields, default constants, env parsing, and startup validation.
   - Update `Config::load_with_overrides`.
4. **`src/gc_service.rs`, `src/http_api/handlers/tests.rs`, `tests/support/gc_coordination.rs`**:
   - Update `Config { ... }` struct literals to populate new tag listing fields.
5. **`src/storage/mod.rs`**:
   - Wire tag listing limits into primary and proxy-cache storage instances.

### 6.2 Rollback Contract
- Rollback can be executed by reverting the routing commit in `src/storage/fs.rs`.
- Rollback requires a binary redeployment or service restart.
- Rollback does not reverse intervening filesystem mutations, journal records, or Sled index updates.

---

## 7. Acceptance Test Contract

### 7.1 Existing Recorded Coverage
- **Contained Seam Suite** (`src/storage/fs/tag_listing.rs`): 27 unit tests validating descriptor containment, limits, dotfile exclusion, zero-page short-circuiting, and lexical cursor arithmetic.
- **Lifecycle Error Hardening Suite** (`tests/manifest_lifecycle_tests.rs`): Tests validating fail-closed recovery and eviction aborts on listing errors.
- **Membership Migration Suite** (`tests/repository_membership_tests.rs`): Tests validating checkpoint failure recording, diagnostic capture, and cursor preservation.
- **Supervisor Suite** (`src/supervisor.rs`): Unit tests validating error propagation before candidate scanning, `NotFound` compatibility, and fault recovery.

### 7.2 Required New Acceptance Tests for Production Cutover
1. **Production Method Routing Tests**:
   - Verify `FsStorage::list_tags` executes through `contained_list_tags_seam` by asserting path-safety rejections (`../` returns `InvalidRepoName`).
   - Verify `FsStorage::list_tags_page` executes through `contained_list_tags_page_seam`.
2. **Dual-Wiring Tests**:
   - Verify primary and proxy-cache storage instances both receive and enforce non-default configured limits from TOML and environment variables.
3. **Process-Isolated Configuration Tests**:
   - Verify environment variable overrides (`REGISTRY__STORAGE__FS__TAG_LISTING_MAX_ENTRIES` and `STORAGE_FS_TAG_LISTING_MAX_ENTRIES`) in process-isolated tests to prevent global environment races.
4. **Reader Identity & Async Factory Offload Tests**:
   - Verify `FsStorage` shares `Arc<FsMetadataReader>` without opening new reader roots.
   - Verify that `storage_wiring_try_from_config_async_with_factory` and `proxy_cache_storage_try_from_config_async_with_factory` execute construction inside `tokio::task::spawn_blocking`.
5. **Point-Read Independence Tests**:
   - Verify `FsStorage::resolve_tag` continues to use `TagReadLimits::default()` (`max_payload_bytes: None`).
   - Verify `FsStorage::get_tag_with_version` continues to use `TagReadLimits::default()`, computing valid SHA-256 version hashes over raw tag bytes.
6. **Limit Boundary & Exhaustion Tests**:
   - Test directory entry count exceeding `tag_listing_max_entries` fails with `StorageErrorKind::Backend`.
   - Test filename byte length exceeding `tag_listing_max_name_bytes` fails with `StorageErrorKind::Backend`.
   - Test candidate payload exceeding `tag_listing_max_payload_bytes` fails closed with `StorageErrorKind::CorruptData` (verifying `limit + 1` overflow detection).
7. **Missing Directory & Error Precedence Tests**:
   - Nonexistent repository returns `StorageError::NotFound` for `list_tags` and `Ok((Vec::new(), None))` for `list_tags_page`.
   - Existing repository with missing `tags/` returns `Ok(Vec::new())` and `Ok((Vec::new(), None))`.
8. **Zero-Page Payload-Open Recording Test**:
   - Request `page_limit == 0` on a recording mock reader and assert that `open_payload` is called exactly zero times.
9. **Normal Multi-Page Pagination Test**:
   - Populate 25 tags; verify pagination across multiple pages with page size 5 using lexical continuation tokens, checking sorted tag names and next token progression.
10. **Privilege-Disclosed Permission Tests**:
    - Tag permission-denied tests must distinguish recorded executions under specific file modes from default ignored tests (`#[ignore]`).
11. **Real Caller Propagation Tests**:
    - Verify budget failure propagation through actual callers: `delete_manifest`, `evict_proxy_cached_entry`, `apply_membership_migration`, and `compute_protected_blobs`.
12. **Mutation Path Preservation Test**:
    - Execute `FsStorage::delete_manifest` and verify that `list_tag_files` successfully discovers and unlinks referencing tags.

---

## 8. Decision Gate & Implementation Prerequisites

### 8.1 Decisions Already Authorized
- **`DEC-TL-AUTH-01`**: Lifecycle error hardening contract and `NotFound` compatibility policy (`96c0729`).
- **`DEC-TL-AUTH-02`**: Membership migration failure contract, bounded diagnostic info, and cursor preservation (`5779f7f`).
- **`DEC-TL-AUTH-03`**: Supervisor error propagation and `NotFound` compatibility in `compute_protected_blobs` (`02cfa07`).
- **`DEC-TL-AUTH-04`**: Retention of `resolve_tag` error suppression as a characterized limitation (`02cfa07`).
- **`DEC-TL-AUTH-05`**: Retention of existing membership sweep policies (Site 1 abort, Site 2 preserve candidate) (`02cfa07`).

### 8.2 Remaining Decisions Requiring User Approval
- **`DEC-TL-PEND-01` (Production Cutover Authorization)**:
  Approval to promote `tag_listing.rs` from `#[cfg(test)]` to production, route `FsStorage::list_tags` and `FsStorage::list_tags_page` through the contained implementation, and retain `list_tag_files`.
- **`DEC-TL-PEND-02` (Configuration Budgets, Ranges & Defaults)**:
  Approval of the proposed limit defaults and validation ranges:
  - `tag_listing_max_entries = 10_000` (min: 1)
  - `tag_listing_max_name_bytes = 1_500_000` (min: 128)
  - `tag_listing_repo_probe_max_entries = 64` (min: 1)
  - `tag_listing_repo_probe_max_name_bytes = 4_096` (min: 64)
  - `tag_listing_max_payload_bytes = 1_024` (min: 256 operational choice, max: `< u64::MAX`)
- **`DEC-TL-PEND-03` (Error Mappings & Behavioral Divergence Acceptance)**:
  Approval of behavioral divergences: upfront path component validation (`InvalidRepoName`), exclusion of non-regular entries (symlinks/subdirectories/FIFOs), zero-page short-circuiting without payload opens, and typed error mappings (directory permission denials to `StorageError::permission_denied`; payload permission denials to `StorageError::io`; stream overflow to `StorageError::corrupt_data`).
- **`DEC-TL-PEND-04` (Paged Missing Repo Policy)**:
  Approval to retain legacy `Ok((Vec::new(), None))` behavior for `list_tags_page` on nonexistent repositories.
- **`DEC-TL-PEND-05` (Constructor Extension & Config Literal Updates)**:
  Approval to add `TagListingLimits`, extend `FsStorage` constructors with `try_new_with_all_limits`, and update the four identified `Config { ... }` struct literal sites.

### 8.3 Implementation Blockers & Status
- **Blockers:** Implementation of the cutover slice is blocked strictly pending review and approval of decisions `DEC-TL-PEND-01` through `DEC-TL-PEND-05`.
- **Scope Boundary:** Conclusion applies strictly to inspected callers and does not authorize unreviewed changes to mutation paths or point reads.

### 8.4 Bounded Implementation Plan (Post-Approval)
Once decisions `DEC-TL-PEND-01` through `DEC-TL-PEND-05` are approved:
1. Update `src/config.rs` to parse, validate, and store the five tag listing configuration parameters.
2. Update `src/gc_service.rs`, `src/http_api/handlers/tests.rs`, and `tests/support/gc_coordination.rs` to populate `Config` literals.
3. Update `src/storage/fs/tag_listing.rs` to remove `#[cfg(test)]` from module items.
4. Update `src/storage/fs.rs` to store `TagListingLimits` and route `list_tags` and `list_tags_page` to contained seam functions, preserving `list_tag_files`.
5. Update `src/storage/mod.rs` to wire limits in `storage_wiring_try_from_config` and `proxy_cache_storage_try_from_config`.
6. Implement the acceptance test suite in `tests/` and verify clean execution under `cargo test`.
7. Package evidence session and present for commit review.

---

## 9. Canonical Quality Gates

All eight canonical quality gates remain explicitly **OPEN**:
- **`O-03`**: Key and continuation-token contracts.
- **`O-04`**: Filesystem write durability and containment.
- **`O-05`**: Broader filesystem read containment.
- **`O-06`**: Typed AWS mapping and pinned-MinIO evidence.
- **`O-13`**: Hosting, distribution, and release strategy.
- **`O-15`**: Non-Linux verification.
- **`O-16`**: Earlier Slice 11 audit/test-inventory evidence.
- **`D-06`**: Broader extraction, cutover, compatibility, and distribution acceptance.
