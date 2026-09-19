> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Provenance (reconstructed 2026-09-19): the original carries no date/commit stamps; it was added in commit `acddfe7` (2026-09-12), the cutover commit itself. Manifests later moved onto `manifest_domain` (`76209a9`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Filesystem Manifest Listing Production Cutover

## 1. Executive Summary and Scope

This implementation record documents the approved promotion of the contained, descriptor-relative filesystem manifest listing implementation (`src/storage/fs/manifest_listing.rs`) into production in `registry-rust`, and the delegation of `FsStorage::list_manifest_digests_page` to it.

### Approved Decisions
1. **Production Promotion:** The contained listing implementation is promoted to production by removing `#![cfg(test)]` from `src/storage/fs/manifest_listing.rs` and removing `#[cfg(test)]` from `pub(crate) mod manifest_listing;` in `src/storage/fs.rs`. Its test suite remains strictly cfg-gated under `#[cfg(test)]`.
2. **Delegation:** `FsStorage::list_manifest_digests_page` delegates directly to `manifest_listing::list_manifest_digests_page_impl` using the shared metadata reader (`self.reader.as_ref()`) and configured directory enumeration limits.
3. **Shared Root Reader Reuse:** Listing operations reuse the shared `Arc<storage_fs::FsMetadataReader>` initialized at storage construction time, avoiding duplicate root directory descriptor acquisition and capability probing.
4. **Preserved Startup Offload:** Synchronous `FsStorage` construction (`try_new`, `try_new_with_limits`) and asynchronous offloading via `tokio::task::spawn_blocking` (in `src/storage/mod.rs:storage_wiring_try_from_config_async_with_factory`, `src/runtime.rs`, and `src/cli/runtime.rs`) are preserved unchanged.
5. **Configurable Limits with Validated Defaults:** Configurable resource limits are introduced under `[storage.fs]` with approved provisional defaults and runtime validation across configuration loaders and direct constructors.
6. **Caller Hardening Preconditions Met:** Promotion occurs after both prerequisite caller-hardening milestones are committed:
   - Commit `ea5cdaa4429f0ef265172ae10b4d9e112e0e1bc6`: Lifecycle reference discovery propagates listing failures via `?`.
   - Commit `2f7fcc8b46c30c0008fc60ba15e15b0c1bf3eea3`: Reference-index synchronization stages discovery before index tree mutation.

---

## 2. Approved Resource Policy & Configuration

Directory enumeration in `storage-fs` requires finite bounds (`DirEnumerationLimits`) to protect worker threads from unbounded memory allocation during directory reads.

### 2.1 Approved Resource Defaults & Invariants
- **Default Maximum Directory Entries:** `10,000` (valid range: `1` through `usize::MAX`).
- **Default Cumulative Raw Filename Bytes:** `1,500,000` bytes (~1.43 MiB, valid range: `128` through `usize::MAX`).
- **Independent Binding:** Entry count and cumulative filename byte limits bind independently; reaching either limit terminates directory enumeration with `FsDirError::LimitExceeded`, mapped to `StorageErrorKind::Backend`.
- **Filesystem-Only Scope:** Limits apply strictly to filesystem manifest-directory enumeration (`StorageBackend::Filesystem`). The S3 storage backend does not use or enforce filesystem enumeration limits.
- **Provisional Nature:** These values represent approved configurable operational defaults, **not** measured capacity or latency guarantees. Temporary files (`.tmp.*`) and lock files (`.lock.*`) consume enumeration budgets before filtering. Limits do not bound total reference-index staging memory or the execution time of individual `getdents64` system calls.

### 2.2 Configuration Schema & Environment Precedence
The following fields are added under `[storage.fs]` in the TOML configuration:
```toml
[storage.fs]
manifest_listing_max_entries = 10000
manifest_listing_max_name_bytes = 1500000
```

Environment variable aliases are parsed in precedence order (hierarchical scoped alias takes precedence over flat legacy alias, which in turn takes precedence over TOML configuration):

1. **Max Entries:**
   - Precedence 1: `REGISTRY__STORAGE__FS__MANIFEST_LISTING_MAX_ENTRIES`
   - Precedence 2: `STORAGE_FS_MANIFEST_LISTING_MAX_ENTRIES`
   - Precedence 3: TOML `storage.fs.manifest_listing_max_entries`
   - Default: `10,000`
2. **Max Filename Bytes:**
   - Precedence 1: `REGISTRY__STORAGE__FS__MANIFEST_LISTING_MAX_NAME_BYTES`
   - Precedence 2: `STORAGE_FS_MANIFEST_LISTING_MAX_NAME_BYTES`
   - Precedence 3: TOML `storage.fs.manifest_listing_max_name_bytes`
   - Default: `1,500,000`

### 2.3 Numeric Parsing & Validation
- **Checked Numeric Parsing:** Environment variable values are parsed via `env_usize_opt`. Malformed strings (e.g. `"abc"`) and values overflowing `usize::MAX` fail closed with `ConfigError::InvalidEnvValue { key, expected: "unsigned integer" }`.
- **Floor Validation in Configuration:** In `Config::from_env_with_files`, when `storage_backend == StorageBackend::Filesystem`:
  - `manifest_listing_max_entries < 1` returns `ConfigError::InvalidValue { field: "manifest_listing_max_entries", message: "must be at least 1" }`.
  - `manifest_listing_max_name_bytes < 128` returns `ConfigError::InvalidValue { field: "manifest_listing_max_name_bytes", message: "must be at least 128" }` (128 bytes is the minimum needed to accommodate a 128-byte SHA-512 filename).
- **Direct Constructor Validation:** In `FsStorage::try_new_with_limits(root, max_upload_bytes, limits)`:
  - `limits.max_entries() < 1` returns `StorageError::configuration("manifest_listing_max_entries must be at least 1")`.
  - `limits.max_total_name_bytes() < 128` returns `StorageError::configuration("manifest_listing_max_name_bytes must be at least 128")`.
- Existing `FsStorage::try_new(root, max_upload_bytes)` callers remain source-compatible by supplying approved defaults via `manifest_listing::default_manifest_dir_limits()`.

### 2.4 Test Helper Compatibility Inventory
Adding `fs_manifest_listing_max_entries` and `fs_manifest_listing_max_name_bytes` to `Config` requires updating existing test fixtures that construct `Config` using direct struct literal syntax. The following test files were updated with approved default values (10,000 entries, 1,500,000 name bytes):
- `src/gc_service.rs` (lines 620-621 in test helper `test_config`)
- `src/http_api/handlers/tests.rs` (lines 142-143, 207-208, 269-270 in test helper `test_config`)
- `tests/support/gc_coordination.rs` (lines 405-406 in test helper `create_test_config`)

---

## 3. Behavioral Changes & Architectural Boundaries

Promoting contained manifest listing establishes the following production behavioral properties:

### 3.1 Repository Name Validation
Direct callers invoking `FsStorage::list_manifest_digests_page(repo, ...)` with arbitrary string slices are subject to fail-closed repository validation (`manifest_dir_key`) prior to any filesystem interaction:
- Rejects empty strings.
- Rejects leading or trailing slashes (`/`).
- Rejects backslashes (`\\`), NUL bytes, and ASCII/non-ASCII control characters.
- Rejects empty segments (e.g. `//`).
- Rejects `.` and `..` segments (preventing path traversal).
- When validation fails, returns `StorageError::InvalidRepoName` immediately, even when `page_limit == 0`.

### 3.2 Canonical Regular-File Discovery & Filtering
- **Entry Type Filtering:** Only directory entries verified as `DirEntryType::Regular` via `openat2` inspection are accepted. Subdirectories, symlinks, FIFOs, and devices are skipped.
- **Filename Validation:** Filenames must consist exclusively of lowercase ASCII hex (`[0-9a-f]`).
- **Algorithm Support:**
  - 64-hex lowercase ascii filenames are parsed as `sha256:<hex>`.
  - 128-hex lowercase ascii filenames are parsed as `sha512:<hex>`.
- **Rejected Legacy Formats:**
  - Algorithm-prefixed filenames (`sha256:<hex>`, `sha512:<hex>`) contain `:` and are skipped.
  - Uppercase hex filenames are skipped.
  - Temporary (`.tmp.*`) and lock (`.lock.*`) filenames are skipped.

### 3.3 Ordering, Deduplication, and Pagination
- **Ordering:** Manifest digests are sorted in-place via derived `Ord` (`Digest::cmp`). This sorts algorithm ascending (`"sha256"` before `"sha512"`), and hex ascending within algorithms.
- **Sorting vs. Cursor Alignment:** Because `Digest::cmp` matches canonical string representation ordering (`<algo>:<hex>`), binary search over continuation tokens (`d.as_str().as_str().cmp(token)`) is fully consistent with slice partitioning, eliminating pagination omissions.
- **Deduplication:** In-place `all_digests.dedup()` ensures no duplicate digests are returned.
- **Continuation Tokens:** Slicing uses whole-string raw lexical comparison against the continuation token. If the slice boundary falls within the list, the last digest's string is returned as the next continuation token; otherwise `None` is returned.
- **Zero Page Limit:** When `page_limit == 0` for a valid repository, returns `Ok((Vec::new(), None))` without performing directory enumeration.

### 3.4 Typed Error Translation
Strongly typed `FsDirError` outcomes from `storage-fs` are mapped deterministically to registry `StorageError` taxonomy:
- `FsDirError::NotFound`: Returns empty page `Ok((Vec::new(), None))`.
- `FsDirError::NotADirectory`: Returns `StorageErrorKind::CorruptData`.
- `FsDirError::PermissionDenied`: Returns `StorageErrorKind::PermissionDenied`.
- `FsDirError::ResolutionRejected`: Returns `StorageErrorKind::Io`.
- `FsDirError::LimitExceeded`: Returns `StorageErrorKind::Backend`.
- `FsDirError::EntryDisappeared`: Returns `StorageErrorKind::Io`.
- `FsDirError::Io`: Returns `StorageErrorKind::Io`.
- `FsDirError::RuntimeMissing`, `FsDirError::TaskJoinFailed`: Returns `StorageErrorKind::Backend`.
- `FsDirError::SyscallUnsupported`, `FsDirError::PlatformUnsupported`: Returns `StorageErrorKind::Configuration`.

---

## 4. Upstream Integration & Hardened Callers

### 4.1 Lifecycle Reference Discovery (`src/manifest_lifecycle.rs`)
In `is_blob_referenced_in_repo`, `list_manifest_digests_page` pagination propagates listing errors immediately via `?`:
- If listing fails closed (e.g. on permission denial, wrong-type component, or resource limit exhaustion), the operation aborts and returns `ManifestLifecycleError::Storage` to the caller.
- It does **not** treat listing errors as unreferenced (`false`), preventing premature proxy cache blob unlinking or data loss.
- Integration test `test_manifest_listing_lifecycle_error_propagation_on_promoted_listing_failure` in `tests/manifest_lifecycle_tests.rs` exercises `service.evict_proxy_cached_entry(repo, Some("v1"), &m1_d)` on a valid repo using real `FsStorage` with limits, verifying error propagation, journal persistence at `ProxyManifestDeleted`, preserved proxy memberships, and continued CAS accessibility.

### 4.2 Reference-Index Synchronization (`src/blob_ref_index.rs`)
In `BlobRefIndex::sync_repo_manifests_and_tags`:
- Phase 1 (Discovery) collects all manifests and tags into memory before modifying any persistent sled trees.
- If `list_manifest_digests_page` fails closed on page 1 or subsequent continuation pages, the function aborts immediately via `?`.
- Sled trees (`tag_to_root`, `root_counts`, `rev_edges`, `pins`, `repo_memberships`, `meta`) remain completely untouched.
- Unit test `test_sync_repo_real_fs_promoted_listing_failure_preserves_populated_index` in `src/blob_ref_index.rs` asserts byte-for-byte equality across all 6 sled trees before and after listing failure using real `FsStorage` with limits, with an unaffected repository verified intact.

### 4.3 Garbage Collection (`src/blob_gc/policy.rs`)
Online blob GC policy (`live_manifest_digests_in_repo`) already conditionally bypasses `list_manifest_digests_page` on filesystem storage (`storage.kind() == "fs"`). Cutting over `FsStorage::list_manifest_digests_page` introduces zero change to online GC discovery.

---

## 5. Explicit Limitations & Preserved Gaps

The following architectural limitations remain explicit, active, and preserved:

1. **Iterative Enumeration, Not a Snapshot:** Directory enumeration is an iterative observation over successive `getdents64` system calls, not an instantaneous observation at syscall invocation or a snapshot of the directory tree. Concurrent file additions, unlinks, or renames while enumeration proceeds can produce omitted entries or duplicate observations across pages.
2. **No Mount or Hard-Link Isolation:** Descriptor containment confines path resolution beneath the storage root (`RESOLVE_BENEATH`), but does not isolate hard links or filesystem mount points beneath the root.
3. **Root Replacement Divergence:** `FsStorage` pins an open directory file descriptor to the configured root at construction. If an external operator renames or replaces the root directory tree on disk, `FsStorage` continues enumerating the pinned original directory descriptor.
4. **Reference-Index Staging Memory Unbounded:** While directory enumeration enforces finite limits (`DirEnumerationLimits`), reference-index synchronization stages all collected roots, edges, and tags in memory before application. Repositories with large numbers of manifests will consume proportional RAM during synchronization.
5. **Successful-Sync Count Inflation Unresolved:** When synchronization succeeds repeatedly across runs, manifest root counts continue to increment monotonically in `root_counts` until an index rebuild is performed. Resolving this requires structural idempotence (deferred).
6. **Broader Lifecycle & GC Discovery Gaps Deferred:** GC filesystem discovery bypasses listing and inspects tags directly. Broader lifecycle error consolidation remains an independent roadmap item.
7. **Rollback State:** Rolling back code does not revert on-disk mutations performed while the promoted listing was active.
8. **Linux-Specific Containment Verification & Non-Linux Unverified:** Descriptor-relative containment relies on Linux `openat2` and `getdents64`. Compilation and execution on non-Linux platforms (e.g. macOS, Windows, BSD) are explicitly unverified in this slice and remain unsupported.

---

## 6. Verification Evidence Summary

Comprehensive verification commands were executed with results captured in individual raw logs:
- `cargo fmt --check`: Confirms clean tree formatting.
- `cargo clippy --locked --all-targets -- -D warnings`: Passes with zero warnings under strict lints.
- `cargo test --locked --lib storage::fs::manifest_listing`: Passes 16 tests covering contained listing logic, mock enumerator boundaries, canonical hex filtering, and pagination.
- `cargo test --locked --lib storage::fs::tests::test_manifest_listing_`: Passes 16 tests covering live filesystem storage, updated characterization semantics, constructor validation, exact entry and name-byte boundaries, shared reader offload, and wiring verification.
- `cargo test --locked --lib config::tests`: Passes full configuration test suite (33 tests) including 10 process-isolated tests covering defaults, TOML parsing, hierarchical and flat environment variable precedence, invalid bounds, malformed inputs, numeric overflow, and S3 independence.
- `cargo test --locked --lib blob_ref_index::tests::test_sync_repo_`: Passes all reference-index sync tests, verifying that listing discovery failure leaves index trees untouched.
- `cargo test --locked --test manifest_lifecycle_tests`: Passes integration test suite verifying lifecycle reference discovery error propagation.

---

## 7. Rollback Procedure

If operational defects emerge, the cutover can be reverted cleanly using standard git operations:
1. Revert the working tree changes:
   ```bash
   git checkout HEAD -- src/storage/fs/manifest_listing.rs src/storage/fs.rs src/config.rs src/storage/mod.rs src/storage/fs/tests.rs src/gc_service.rs src/http_api/handlers/tests.rs src/blob_ref_index.rs tests/manifest_lifecycle_tests.rs tests/support/gc_coordination.rs
   rm -f docs/architecture/filesystem-manifest-listing-production-cutover.md
   ```
2. Re-verify compilation and tests:
   ```bash
   cargo test --locked --lib storage::fs
   ```
3. Operational considerations:
   - Rollback restores legacy `tokio::fs::read_dir` listing behavior.
   - Any manifests written during cutover remain readable on disk because disk storage layout was not altered.
   - Any reference-index databases synchronized during cutover remain compatible with previous code.

---

## 8. Status of Canonical Quality Gates

All canonical quality gates remain explicitly **OPEN**:
- **O-03:** Open
- **O-04:** Open
- **O-05:** Open
- **O-06:** Open
- **O-13:** Open
- **O-15:** Open
- **O-16:** Open
- **D-06:** Open

