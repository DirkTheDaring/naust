# Filesystem Tag Listing Production Cutover

> **Partial supersession:** production `list_tags` / `list_tags_page` / `resolve_tag` now go through `crate::storage::tag_domain` over `ObjectStore` (`32c42c6`). Tag **mutation** is the same domain (not this listing-cutover document). `src/storage/fs/tag_listing.rs` retains limits wiring and the repository-existence probe only. Current inventory: [`current-state.md`](current-state.md).

## 1. Executive Summary and Scope

This document records the bounded production cutover of contained filesystem tag listing in `registry-rust`, promoting the contained test seam into active production service and routing both `FsStorage::list_tags` and `FsStorage::list_tags_page` through it.

### 1.1 Baselines and Authorization
- **Primary Repository:** `~/devel/rust/registry-rust`
  - Baseline HEAD: `02cfa0789e5b3ad274c93632af90b7fadb66c62f`
- **Dependency Repository:** `~/devel/rust/storage-layer-rust`
  - Baseline HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (strictly read-only)
- **Approved Assessment:** `~/devel/rust/manifest-read-review-evidence/session-20260912-2215/filesystem-tag-listing-production-cutover-readiness.tar.gz`
  - Size: 24,644 bytes
  - SHA-256: `6558ccf288b8f471f1fe895c2355ca8e05697d948d7041d64f5641f838cfc9dc`

### 1.2 Canonical Quality Gates
All canonical quality gates remain explicitly **OPEN**:
- `O-03`: Key and continuation-token contracts.
- `O-04`: Filesystem write durability and containment.
- `O-05`: Broader filesystem read containment.
- `O-06`: Typed AWS mapping and pinned-MinIO evidence.
- `O-13`: Hosting, distribution, and release strategy.
- `O-15`: Non-Linux verification.
- `O-16`: Earlier Slice 11 audit/test-inventory evidence.
- `D-06`: Broader extraction, cutover, compatibility, and distribution acceptance.

---

## 2. Approved Limits and Configuration

### 2.1 Approved Operational Limits
The cutover introduces five configurable operational limits under `[storage.fs]`:

| Limit Name | Default | Minimum | Ceiling / Upper Bound | Scope / Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `tag_listing_max_entries` | `10,000` | `1` | `usize::MAX` | Maximum directory entries evaluated during tags directory enumeration |
| `tag_listing_max_name_bytes` | `1,500,000` | `128` | `usize::MAX` | Maximum cumulative raw filename bytes during tags directory enumeration |
| `tag_listing_repo_probe_max_entries` | `64` | `1` | `usize::MAX` | Maximum directory entries evaluated during repository probing on tags NotFound |
| `tag_listing_repo_probe_max_name_bytes` | `4,096` | `64` | `usize::MAX` | Maximum cumulative raw filename bytes during repository probing on tags NotFound |
| `tag_listing_max_payload_bytes` | `1,024` | `256` | `< u64::MAX` | Maximum bytes read per candidate tag payload during paged listing (`Some(ceiling)`) |

> [!IMPORTANT]
> These bounds are approved operational choices, not measured production capacity guarantees. Temporary files (`.tmp.*`), locks (`.lock.*`), and non-regular children consume directory enumeration budgets before tag filtering occurs.

### 2.2 TOML Schema & Environment Variable Precedence
The configuration keys are located under `[storage.fs]` in TOML:
```toml
[storage.fs]
tag_listing_max_entries = 10000
tag_listing_max_name_bytes = 1500000
tag_listing_repo_probe_max_entries = 64
tag_listing_repo_probe_max_name_bytes = 4096
tag_listing_max_payload_bytes = 1024
```

Environment variable resolution follows established hierarchical/flat parser precedence:
1. **Hierarchical scoped variable** (e.g. `REGISTRY__STORAGE__FS__TAG_LISTING_MAX_ENTRIES`) takes highest precedence.
2. **Flat variable** (e.g. `STORAGE_FS_TAG_LISTING_MAX_ENTRIES`) takes secondary precedence.
3. **TOML configuration key** takes tertiary precedence.
4. **Compiled default** applies if neither environment variables nor TOML keys are present.

### 2.3 Validation Rules and Failure Taxonomy
Configuration validation is enforced at startup when `storage_backend == StorageBackend::Filesystem`:
- Non-numeric strings or values overflowing integer limits fail with `ConfigError::InvalidEnvValue { expected: "unsigned integer" }`.
- Values violating lower bounds or the payload upper bound fail closed with descriptive `ConfigError::InvalidValue`.
- Direct programmatic construction via `FsStorage::try_new_with_all_limits` validates all five limits, returning `StorageError::configuration` on violation.

---

## 3. Exact Wiring and Modified Paths

### 3.1 Promoted Seam Module
- File: [`src/storage/fs/tag_listing.rs`](src/storage/fs/tag_listing.rs)
  - Removed top-level `#![cfg(test)]`.
  - Introduced `TagListingLimits` container (`#[derive(Clone, Debug, PartialEq, Eq)]`) encapsulating probe limits, tags directory limits, and payload read limits.
  - Preserved internal test suite strictly under `#[cfg(test)] mod tests`.
  - Exported public seam functions: `contained_list_tags_seam` and `contained_list_tags_page_seam`.

### 3.2 Filesystem Storage Engine
- File: [`src/storage/fs.rs`](src/storage/fs.rs)
  - Exposed module: `pub(crate) mod tag_listing;`.
  - Stored `tag_listing_limits: tag_listing::TagListingLimits` in `FsStorage`.
  - Implemented `try_new_with_all_limits` constructor enforcing all limits.
  - Delegated `try_new_with_gc_limits` to `try_new_with_all_limits` with `TagListingLimits::default()`.
  - Routed `FsStorage::list_tags` through `tag_listing::contained_list_tags_seam`.
  - Routed `FsStorage::list_tags_page` through `tag_listing::contained_list_tags_page_seam`.
  - Preserved private helper `list_tag_files` **byte-for-byte** for the `delete_manifest` mutation path.
  - Added crate-visible `tag_listing_limits(&self)` accessor for verification.

### 3.3 Application Configuration
- File: [`src/config.rs`](src/config.rs)
  - Added `fs_tag_listing_*` fields to `Config` struct.
  - Added `tag_listing_*` fields to `FileStorageFs` TOML parsing struct.
  - Implemented env var parsing and startup limit validation.
  - Updated `Config { ... }` literal in `Config::load_with_overrides`.
- Updated test `Config` struct literals:
  - [`src/config.rs`](src/config.rs) (test helper)
  - [`src/gc_service.rs`](src/gc_service.rs) (test helper)
  - [`src/http_api/handlers/tests.rs`](src/http_api/handlers/tests.rs) (test helper)
  - [`tests/support/gc_coordination.rs`](tests/support/gc_coordination.rs) (test helper)

### 3.4 Factory Storage Wiring and Blocking Offload
- File: [`src/storage/mod.rs`](src/storage/mod.rs)
  - Wired configured `TagListingLimits` into primary filesystem storage in `storage_wiring_try_from_config`.
  - Wired configured `TagListingLimits` into proxy-cache filesystem storage in `proxy_cache_storage_try_from_config`.
  - Preserved existing asynchronous factory offload via `tokio::task::spawn_blocking` in `storage_wiring_try_from_config_async_with_factory` and `proxy_cache_storage_try_from_config_async_with_factory`.

---

## 4. Behavioral Semantics & Compatibility

### 4.1 Structural Validation and Probing Order
1. **Upfront Repository Validation:** Both `list_tags` and `list_tags_page` validate the repository name structurally using `validate_path_component` prior to any filesystem interaction. Nested repository names (such as `library/example`) are fully valid and supported. The actual structural checks reject: empty names, absolute paths (leading slash), trailing slashes, empty segments (consecutive repeated slashes), relative traversal segments (`.` and `..`), backslashes (`\`), NUL bytes (`\0`), and ASCII control characters, failing immediately with `StorageError::InvalidRepoName`.
2. **Tags-Directory-First Enumeration:** The storage engine attempts directory enumeration directly on `repos/<repo>/tags` without upfront repository probing.
3. **Missing Directory vs Missing Repository:**
   - If `repos/<repo>/tags` exists: tags are enumerated.
   - If `repos/<repo>/tags` returns `NotFound`: the engine probes `repos/<repo>`.
     - Missing repository: `list_tags` returns `StorageError::NotFound`; `list_tags_page` returns `Ok((Vec::new(), None))` (preserving legacy caller expectations).
     - Existing repository with missing `tags/`: both return empty success (`Ok(vec![])` and `Ok((vec![], None))`).

### 4.2 Entry Filtering and Path Safety
- **Entry Type Filtering:** Only entries observed as `DirEntryType::Regular` via `openat2` inspection are retained. Symlinks, subdirectories, fifos, and sockets are omitted.
- **Dotfile Exclusion:** All filenames beginning with `.` (including `.tmp.*` and `.lock.*`) are excluded.
- **UTF-8 Validation:** Non-UTF-8 filenames are safely omitted without terminating listing.
- **Directory Path Containment:** Directory-path symlinks and post-enumeration file substitutions are rejected by `openat2` resolution flags (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS`).

### 4.3 Candidate Reading and Pagination
- **Candidate Acquisition:** For `list_tags_page`, candidate payload reading is performed sequentially.
- **Omission Rules:**
  - Candidate payload `NotFound` (e.g. entry unlinked concurrently after directory enumeration) is omitted from the page.
  - Empty or malformed digest text is omitted from the page.
- **Error Propagation:** Malformed digest text omission is strictly distinguished from I/O and format errors: invalid UTF-8 payload contents, payload acquisition failures (other than `NotFound`), stream I/O failures, and payload byte limit overflow (`StorageErrorKind::CorruptData`) propagate immediately according to existing mappings.
- **Zero-Page Optimization:** Requests with `page_limit == 0` validate the repository and enumerate the directory/probe repository, but open **zero** candidate payloads.
- **Nonzero-Page Eager Candidate Acquisition:** Every nonzero page (`page_limit > 0`) enumerates and processes all retained directory candidates before sorting and slicing. An invalid or oversized payload among candidates fails the entire page request even if the candidate would sort past the requested page limit. Successive pages repeat those reads regardless of cursor position; they do not stop at the cursor.
- **Pagination Token:** Tokens use whole-string lexical comparison with saturating arithmetic.

### 4.4 Error Taxonomy Mapping
The contained listing implementation adheres to the approved fail-closed error taxonomy:
- Directory enumeration permission denied: `StorageErrorKind::PermissionDenied`.
- Candidate payload permission denied and stream I/O failures: `StorageErrorKind::Io`.
- Directory enumeration limit exhaustion: `StorageErrorKind::Backend`.
- Candidate payload byte overflow: `StorageErrorKind::CorruptData`.

---

## 5. Point-Read and Mutation Preservation

### 5.1 Tag Point-Reads Unchanged
Both public point-read methods remain entirely untouched and unconstrained by tag listing limits:
- `FsStorage::resolve_tag`
- `FsStorage::get_tag_with_version`

Both methods continue to use `TagReadLimits::default()`, which sets `max_payload_bytes: None`. Valid tag payloads that exceed `tag_listing_max_payload_bytes` (e.g. 1,024 bytes) remain fully readable through `resolve_tag` and `get_tag_with_version`.

### 5.2 Preservation of `list_tag_files` for Mutations
[`FsStorage::list_tag_files`](src/storage/fs.rs) is preserved **byte-for-byte** and remains actively used by `FsStorage::delete_manifest`. This ensures manifest deletion reliably unlinks referencing tags without modifying mutation paths.

### 5.3 Shared Reader Identity
Both production listing methods continue using `self.reader.as_ref()`. For `list_tags_page`, the same root reader (`self.reader.as_ref()`) is passed for both directory enumeration and `ObjectPayloadReader` access. Pointer identity between `storage.reader()` and `storage.read_adapter().reader()` is verified (`Arc::ptr_eq`), ensuring no duplicate root descriptor opening or redundant adapter allocation is introduced. Asynchronous construction offload via `tokio::task::spawn_blocking` is maintained and tested across both primary and proxy-cache factory paths.

---

## 6. Resource Wording & Memory Model

The resource and concurrency characteristics of contained tag listing reflect the active implementation:
1. **Sequential Buffer Processing:** Candidate payload buffers are allocated and processed sequentially, rather than concurrently in parallel tasks.
2. **Boundary Detection Overhead:** Detection of oversized payloads may read `tag_listing_max_payload_bytes + 1` bytes before returning `StorageErrorKind::CorruptData`.
3. **Cumulative I/O Accounting:** Cumulative I/O and budget consumption include candidate files that are subsequently omitted due to malformed digest text or whitespace.
4. **Distinct Retained Allocations:** Directory-entry storage and parsed-result allocations are distinct. Directory-entry vectors retain names for all evaluated directory entries that pass initial type and dotfile filters prior to candidate payload reading, whereas parsed-result allocations retain valid parsed digests. Retained allocation is therefore not proportional only to valid parsed digests.
5. **Repeated Enumeration Across Pages:** Every nonzero page enumerates and processes all retained candidates before sorting and slicing. Successive pages repeat those reads regardless of cursor position; they do not stop at the cursor. No persistent cursor handle or server-side snapshot is retained.
6. **No Global Concurrency/Memory Budget:** No snapshot isolation, global concurrency limit, or global memory budget is established. Each request independently enforces its own configured per-operation bounds.

---

## 7. Verification & Test Inventory

### 7.1 Test Suites Executed
1. **Tag Listing Seam Unit Tests:**
   - Command: `cargo test --lib storage::fs::tag_listing`
   - Result: 26 passed, 0 failed, 1 ignored (privilege test).
2. **Production Cutover Integration Tests:**
   - Command: `cargo test --lib storage::fs::tests::test_tag_listing`
   - Result: 23 passed, 0 failed, 1 ignored (privilege test).
   - Covers dual wiring (primary and proxy-cache) for all five configured limits (`max_entries`, `max_name_bytes`, `repo_probe_max_entries`, `repo_probe_max_name_bytes`, and `max_payload_bytes`), `spawn_blocking` offload, shared reader pointer identity, point-read limit independence, zero-page payload-open avoidance, off-page candidate failure, mutation-path preservation (`list_tag_files`), and real caller error propagation.
3. **Process-Isolated Configuration Tests:**
   - Command: `cargo test --lib config::tests::test_config_storage_fs_tag_listing`
   - Result: 11 passed, 0 failed, 0 ignored.
   - Covers default bounds, TOML parsing, hierarchical and flat env variable precedence, lower/upper bound rejections, and backend isolation.
4. **Caller & System Regressions:**
   - Tag Reads: `cargo test --lib storage::fs::tag_read` (28 passed, 1 ignored).
   - Supervisor: `cargo test --lib supervisor::tests` (21 passed).
   - Membership Migration: `cargo test --test repository_membership_tests test_membership_migration` (8 passed).
   - Manifest Lifecycle: `cargo test --test manifest_lifecycle_tests test_delete_manifest` (5 passed).
5. **Linter & Code Format Verification:**
   - `cargo fmt --check`: Clean exit code 0.
   - `git diff --check`: Clean exit code 0.
   - `cargo check --locked --all-targets --all-features`: Clean exit code 0.
   - `cargo clippy --locked --all-targets --all-features -- -D warnings`: Clean exit code 0.

### 7.2 Privilege Disclosure
- The tests `storage::fs::tests::test_tag_listing_permission_denied` and `storage::fs::tag_listing::tests::test_real_filesystem_permission_denied_unprivileged` require execution under an unprivileged user environment where `chmod 0o000` denies filesystem access. When executed as `root` (or container root), these tests are automatically skipped/ignored with an explicit diagnostic explanation.

---

## 8. Rollback and Deployment Considerations

### 8.1 Deployment
- The cutover introduces new optional configuration keys under `[storage.fs]`. Existing deployments without these keys will cleanly adopt the approved defaults.
- Deploying updated binaries requires no database migration or filesystem reformatting.

### 8.2 Rollback
- In the event of a rollback, reverting the binary to a prior release restores legacy unbounded tag listing without storage format incompatibility.

> [!CAUTION]
> Rollback does not provide atomic reversal of intervening caller mutations. If an operation (such as manifest deletion) completes a mutation before encountering a listing error or rollback, that completed mutation remains committed to disk. No atomic hardware durability or cross-operation transactional rollback is claimed.
