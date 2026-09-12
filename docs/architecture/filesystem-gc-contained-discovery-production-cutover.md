# Filesystem GC Contained Discovery Production Cutover Record

**Document Version:** 1.0.0  
**Date:** 2026-09-12  
**Status:** IMPLEMENTED (Uncommitted, Unstaged, Ready for Review)  
**Baseline Git Commits:**  
- `registry-rust`: `d51ea1ac69921dfdbd583b6b197b80b1f149e232` (HEAD)  
- `storage-layer-rust`: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (HEAD)  

---

## Quality Gates Status Register

| Gate ID | Area | Subject | Status |
| :--- | :--- | :--- | :--- |
| **O-03** | Architecture | Filesystem storage namespace confinement | **OPEN** |
| **O-04** | Architecture | Directory traversal bound enforcement | **OPEN** |
| **O-05** | GC Safety | Protected set reachability guarantees | **OPEN** |
| **O-06** | GC Safety | Mutation race safety during mark phase | **OPEN** |
| **O-13** | Storage Core | Pinned descriptor root validation | **OPEN** |
| **O-15** | Platform | Linux `openat2` resolution flag validation | **OPEN** |
| **O-16** | Security | Unprivileged execution and DAC override safety | **OPEN** |
| **D-06** | Data Integrity | Non-UTF-8 directory name fail-closed validation | **OPEN** |

*All canonical quality gates remain explicitly OPEN and unapproved for final production merge.*

---

## 1. Executive Summary & Cutover Architecture

This document records the production cutover from the uncontained raw filesystem GC bypass walker (`build_manifest_protected_set_fs`) to the reviewed contained filesystem GC discovery implementation (`FsStorage::discover_manifest_references`).

### Core Architectural Accomplishments

1. **Elimination of Raw Filesystem Bypass**:
   - The uncontained filesystem traversal routine (`build_manifest_protected_set_fs`) in [`src/blob_gc/policy.rs`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/policy.rs), which directly evaluated `cfg.fs_root.join("repos")` and performed unchecked `tokio::fs` operations, has been completely retired.
   - All manifest reference discovery now routes through `build_manifest_protected_set`, invoking the required capability method `storage.discover_manifest_references()`.

2. **Required Capability Methods Without Defaults**:
   - Added `discover_manifest_references(&self) -> Result<Option<HashSet<Digest>>, StorageError>` as a required method on both `GcStorage` and `GcStoragePort`.
   - Intentionally omitted default implementations (`Ok(None)`) to guarantee that any new backend, wrapper, or adapter must explicitly declare its capability handling at compile time.
   - `FsStorage` implements contained discovery via its pinned root reader (`self.reader`), returning `Ok(Some(HashSet<Digest>))` (including `Ok(Some(HashSet::new()))` when no manifests exist).
   - Non-filesystem backends (`S3Storage`, `BlobRefIndexBridge`, and test fake storages) explicitly return `Ok(None)`, preserving existing generic catalog traversal.
   - All adapters, wrappers, and `Arc<T>` implementations explicitly delegate the call to the inner backend.

3. **Fail-Closed Policy Integration**:
   - Added a dedicated error variant `GcPolicyError::ManifestDiscovery(#[source] StorageError)` to [`src/blob_gc/policy.rs`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/policy.rs).
   - If `discover_manifest_references` returns an `Err(StorageError)`, the GC policy immediately halts with `GcPolicyError::ManifestDiscovery`. It never swallows the error, never falls back to catalog listing, and never treats errors as an empty protected set.

4. **15 Configurable Resource Limits**:
   - Implemented 15 independently configurable resource limit settings in [`src/config.rs`](file:///home/dietmar/devel/rust/registry-rust/src/config.rs).
   - Followed hierarchical environment variable > flat environment variable > configuration file > compiled default precedence.
   - Intermediate and terminal enumeration limits are decoupled, allowing granular tuning.
   - The production payload ceiling is left unset by default (`max_manifest_payload_bytes = None`), preserving compatibility with arbitrarily sized manifest payloads unless an operator chooses to set an explicit cap.

5. **Runtime Root Pinning & Symlink Containment**:
   - Discovery traverses directory hierarchies strictly relative to the file descriptor opened and pinned inside `FsStorage.reader` at startup (`openat2` with `RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS`).
   - Symlink directory entries are skipped during enumeration; symlink path components are rejected at kernel resolution level.
   - Root directory replacement or symlink manipulation in parent paths does not affect ongoing discovery.

---

## 2. Inventory of Actual Source and Test Modifications

### 2.1 Storage Core & Trait Interfaces

- **[`src/storage/fs/repo_discovery.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/repo_discovery.rs)**:
  - Promoted from test seam to production module.
  - Made `DiscoveryLimits` public in crate, implementing `Default` with compiled production defaults (depth: 16, enumerations: 10,000, entries: 100,000, manifest dirs: 10,000, retained path bytes: 1,500,000, intermediate batch entries: 1,000, intermediate name bytes: 100,000).
  - Aliased `DiscoveryTestLimits` to `DiscoveryLimits` under `#[cfg(test)]`.
  - Retained all unit tests and test fake directory engines.

- **[`src/storage/fs/manifest_refs.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/manifest_refs.rs)**:
  - Promoted from seam (`manifest_refs_seam.rs` deleted) to production module.
  - Made `ManifestReferenceLimits` public in crate, implementing `Default` with compiled production defaults (terminal enumerations: 10,000, terminal batch entries: 10,000, terminal name bytes: 1,500,000, total manifest entries: 100,000, manifests read: 10,000, total references: 100,000, retained logical bytes: 10,000,000, max manifest payload bytes: `None`).
  - Aliased `ManifestRefTestLimits` to `ManifestReferenceLimits` under `#[cfg(test)]`.
  - Strictly partitioned imports: production code imports only standard and crate items (`Arc`, `HashSet`, `Digest`, `ObjectKey`, `StorageError`, `StorageErrorKind`); test mock items (`tokio::sync::Mutex`, `Cursor`, etc.) remain gated behind `#[cfg(test)]`.

- **[`src/storage/fs.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs)**:
  - Declared `pub(crate) mod manifest_refs;` and `pub(crate) mod repo_discovery;`.
  - Added `gc_discovery_limits: repo_discovery::DiscoveryLimits` and `gc_ref_limits: manifest_refs::ManifestReferenceLimits` fields to `FsStorage`.
  - Implemented crate-visible constructor `try_new_with_gc_limits` enforcing non-zero and non-underflow validation across all 15 limit parameters.
  - Preserved existing `try_new` and `new_with_default_limits` by delegating to `try_new_with_gc_limits` with `Default::default()`.
  - Implemented required method `discover_manifest_references(&self) -> Result<Option<HashSet<Digest>>, StorageError>` on `FsStorage`, delegating to `manifest_refs::collect_manifest_references_end_to_end` using `self.reader` and the configured limits.

- **[`src/storage/mod.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/mod.rs)**:
  - Added required method `discover_manifest_references` to `GcStorage` trait without default implementation.
  - Implemented required forwarding method on `Arc<T> where T: GcStorage + ?Sized`.
  - Updated `storage_wiring_try_from_config` to construct `FsStorage` via `try_new_with_gc_limits(&cfg.fs_root, discovery_limits, ref_limits)`.

- **[`src/storage/ports/mod.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/ports/mod.rs)**:
  - Added required method `discover_manifest_references` to `GcStoragePort` trait without default implementation.
  - Updated `impl_gc_storage_port!` macro to forward `discover_manifest_references` to `$target`.
  - Implemented required forwarding method on `Arc<T> where T: GcStoragePort + ?Sized`.

- **[`src/storage/s3.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/s3.rs)**:
  - Implemented `discover_manifest_references` on `S3Storage` returning `Ok(None)`.

- **[`src/blob_ref_index.rs`](file:///home/dietmar/devel/rust/registry-rust/src/blob_ref_index.rs)**:
  - Implemented `discover_manifest_references` on `BlobRefIndexBridge` delegating to `self.storage.discover_manifest_references()`.

- **[`src/storage/fs/listing.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/listing.rs)**:
  - Implemented `discover_manifest_references` on `SeamGcStorageBridge` delegating to inner storage, and on `ScriptedCursorAdapter` returning `Ok(None)`.

### 2.2 Configuration

- **[`src/config.rs`](file:///home/dietmar/devel/rust/registry-rust/src/config.rs)**:
  - Added 15 configuration fields to `Config` struct with full serde and default annotations:
    - `fs_gc_discovery_max_depth: usize` (default: 16)
    - `fs_gc_discovery_max_dir_enumerations: usize` (default: 10,000)
    - `fs_gc_discovery_max_total_entries: usize` (default: 100,000)
    - `fs_gc_discovery_max_manifest_dirs: usize` (default: 10,000)
    - `fs_gc_discovery_max_retained_path_bytes: usize` (default: 1,500,000)
    - `fs_gc_discovery_intermediate_max_entries: usize` (default: 1,000)
    - `fs_gc_discovery_intermediate_max_name_bytes: usize` (default: 100,000)
    - `fs_gc_discovery_max_terminal_dir_enumerations: usize` (default: 10,000)
    - `fs_gc_discovery_terminal_max_entries: usize` (default: 10,000)
    - `fs_gc_discovery_terminal_max_name_bytes: usize` (default: 1,500,000)
    - `fs_gc_discovery_max_total_manifest_entries: usize` (default: 100,000)
    - `fs_gc_discovery_max_manifests_read: usize` (default: 10,000)
    - `fs_gc_discovery_max_total_references: usize` (default: 100,000)
    - `fs_gc_discovery_max_retained_logical_bytes: usize` (default: 10,000,000)
    - `fs_gc_discovery_max_manifest_payload_bytes: Option<u64>` (default: `None`)
  - Supported environment variable parsing with precedence:
    - Hierarchical: `REGISTRY_FS_GC_DISCOVERY_*`
    - Flat alias: `REGISTRY_GC_DISCOVERY_*`
    - Config file value
    - Default value
  - Implemented comprehensive config validation rejecting zero/underflow budgets and invalid integer literals.
  - Added helper methods `gc_discovery_limits(&self)` and `gc_manifest_ref_limits(&self)`.

### 2.3 GC Policy and Engine

- **[`src/blob_gc/policy.rs`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/policy.rs)**:
  - Added `GcPolicyError::ManifestDiscovery(#[source] StorageError)` error variant.
  - Completely deleted raw filesystem bypass walker `build_manifest_protected_set_fs`.
  - Updated `build_manifest_protected_set`:
    - First calls `storage.discover_manifest_references()`.
    - If `Ok(Some(set))`, uses the contained discovery set directly.
    - If `Ok(None)`, falls back to generic catalog enumeration via `storage.list_repositories()` and manifest reads.
    - If `Err(e)`, aborts immediately with `GcPolicyError::ManifestDiscovery(e)`.
  - Updated all policy unit tests to test contained discovery behavior through mocked/wrapped `GcStorage` implementations.

- **[`src/blob_gc/mod.rs`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/mod.rs)**:
  - Removed re-export of retired `build_manifest_protected_set_fs`.
  - Updated unit test `test_build_manifest_protected_set_aborts_on_unparsable_manifest` to exercise `build_manifest_protected_set`.

### 2.4 Test Suites & Test Support

- **[`tests/gc_contained_discovery_integration_tests.rs`](file:///home/dietmar/devel/rust/registry-rust/tests/gc_contained_discovery_integration_tests.rs)** (NEW):
  - Comprehensive integration test suite verifying:
    1. Full capability forwarding through real `StorageWiring` port views.
    2. Explicit non-filesystem (`S3Storage`) returns `Ok(None)` and invokes generic catalog traversal.
    3. `Ok(Some(empty))` behavior when no manifests are present proves catalog fallback is strictly avoided via fail-on-call catalog wrappers.
    4. Fail-closed propagation on storage errors (`Err`) proves catalog fallback is strictly avoided via fail-on-call catalog wrappers.
    5. Crate-visible constructor limit validation rejecting zero and underflow values.
    6. Independent budget enforcement (D-12) between intermediate and terminal limits.
    7. Preservation of pinned root reader across root rename/replacement (D-11).
    8. Storage wrappers (`HookedStorage` and `LifecycleFaultStorage`) correctly forward contained discovery through `GcServiceStoragePort`.
    9. Production GC planning (`blob_gc_plan`) fails closed on discovery error (`CorruptData`), asserting precise error reaches caller and live candidate data remains intact.
    10. Production GC quarantine (`blob_gc_quarantine_with_authority`) fails closed on discovery error, asserting `quarantine_blob` is never invoked and live candidate remains in storage.
    11. Production GC deletion (`blob_gc_delete_with_authority`) fails closed on discovery error, asserting neither `delete_blob_conditional` nor `restore_quarantined_blob` is invoked and candidate remains in quarantine.
    12. Real deletion loop initializes missing quarantine timestamp metadata on disk (`quarantine/meta/sha256/<prefix>/<hex>.ts`), and subsequent discovery failure aborts without rolling back or corrupting the persisted timestamp.

- **[`tests/gc_contained_discovery_config_tests.rs`](file:///home/dietmar/devel/rust/registry-rust/tests/gc_contained_discovery_config_tests.rs)** (NEW):
  - Dedicated process-isolated configuration test suite verifying:
    1. Compiled production defaults across all 15 discovery and reference limit fields in a clean isolated environment (`run_isolated`).
    2. TOML file overrides updating all 15 fields from config files.
    3. Hierarchical environment variable overrides (`REGISTRY__STORAGE__FS__GC__DISCOVERY__*`).
    4. Flat environment variable aliases (`REGISTRY_BLOB_GC_DISCOVERY_*`).
    5. Precedence resolution: hierarchical env > flat env > TOML file > compiled default.
    6. Invalid numeric input and overflow handling returning `ConfigError::InvalidEnvValue`.
    7. Validation boundary enforcement: sub-minimum rejected, payload ceiling bound (`1024` valid boundary, `u64::MAX` rejected, unset defaulting to `None`).
    8. Storage wiring constructor validation (`storage_wiring_try_from_config`) validating limits and backend kind (`"fs"`).

- **[`tests/ports_wiring_tests.rs`](file:///home/dietmar/devel/rust/registry-rust/tests/ports_wiring_tests.rs)**:
  - Updated test fake storage implementations (`MinimalFakeStorage`, etc.) to explicitly implement `discover_manifest_references` returning `Ok(None)`.

- **[`tests/manifest_lifecycle_tests.rs`](file:///home/dietmar/devel/rust/registry-rust/tests/manifest_lifecycle_tests.rs)**:
  - Updated test fake storage implementations (`TestStorage`, etc.) to explicitly implement `discover_manifest_references` returning `Ok(None)`.
  - Replaced duplicate in-file `LifecycleFaultStorage` with shared implementation from `support::gc_coordination`.

- **[`tests/support/gc_coordination.rs`](file:///home/dietmar/devel/rust/registry-rust/tests/support/gc_coordination.rs)**:
  - Added public `LifecycleFaultStorage` implementing all storage ports and `GcServiceStoragePort` registered via macros.
  - Implemented `discover_manifest_references` on `HookedStorage` and `LifecycleFaultStorage`.
  - Updated helper config instantiations to provide default values for the 15 new fields.

- **[`src/gc_service.rs`](file:///home/dietmar/devel/rust/registry-rust/src/gc_service.rs)** & **[`src/http_api/handlers/tests.rs`](file:///home/dietmar/devel/rust/registry-rust/src/http_api/handlers/tests.rs)**:
  - Updated test configuration struct literals with default discovery limit fields.

---

## 3. Approved Production Compatibility Decisions (D-01 through D-12)

The cutover enacts the 12 production compatibility decisions approved by the user:

| Decision ID | Dimension | Cutover Behavior | Operational Rationale | Verification Coverage |
| :--- | :--- | :--- | :--- | :--- |
| **D-01** | **Root-Adjacent Manifests** | Enqueues `ObjectKey("repos/manifests")` as a terminal directory; enumerates candidates. | Preserves reachability for legacy single-tenant roots where manifests reside directly under `repos/manifests/`. | Unit test in `repo_discovery`, integration test in `gc_contained_discovery_integration_tests`. |
| **D-02** | **Reserved-Ancestor Manifests** | Traverses all intermediate subdirectories regardless of name; treats `manifests` as terminal leaf. | Preserves parity for nested repository hierarchies (e.g. `repos/tags/sub/manifests/<hex>`). | Tested against deep nested repository layouts. |
| **D-03** | **Missing Initial `repos/`** | Returns `Ok(Some(HashSet::new()))` on clean startup before first push. | Clean deployment without existing repository directory must not fail GC startup or error out. | Unit tests in `repo_discovery` and `gc_contained_discovery_integration_tests`. |
| **D-04** | **Disappearance After Observation** | If an observed child directory returns `NotFound` when opened, returns `StorageError::io` (fails closed). | Prevents race condition where concurrent deletion yields a partial protected set, risking blob deletion. | Fake engine tests asserting fail-closed on directory disappearance. |
| **D-05** | **Symlink Directory Entries** | Skips `DirEntryType::Symlink` dirents; symlink path components rejected with `ResolutionRejected`. | Prevents symlink breakout attacks from storage root. | Linux real filesystem tests verifying symlink dirents are ignored and symlinked paths fail. |
| **D-06** | **Non-UTF-8 Directory Names** | Enforces valid UTF-8 and `ObjectKey` validation; fails closed with `StorageError::corrupt_data`. | Corrupted filesystem paths must halt GC rather than silently bypassing manifests. | Real FS and fake engine tests verifying non-UTF-8 entries trigger corrupt data errors. |
| **D-07** | **SHA-512 Support** | Supports canonical 64-char (SHA-256) AND 128-char (SHA-512) lowercase hex filenames. | Enables multi-algorithm manifest protection while strictly verifying hex digest formats. | Unit tests in `manifest_refs` verifying SHA-256 and SHA-512 parsing. |
| **D-08** | **Uppercase Hex Filenames** | Requires lowercase hex; skips uppercase filenames and charges budget. | OCI and Docker digest specifications require lowercase hexadecimal characters. Non-canonical files skipped safely. | Tested against mixed uppercase and invalid candidate names. |
| **D-09** | **Manifest Payload Disappearance** | Fails closed with `StorageError::io`; aborts entire discovery run. | Disappearance during read indicates concurrent mutation or filesystem anomaly; must not proceed with partial set. | Unit tests in `manifest_refs` asserting fail-closed on missing payload. |
| **D-10** | **Manifest Parse Errors** | Fails closed with `StorageError::corrupt_data`; aborts entire discovery run. | Corrupted manifest must halt GC rather than risking premature layer deletion. | Unit tests in `manifest_refs` and `blob_gc` asserting fail-closed on parse errors. |
| **D-11** | **Root Configuration Divergence** | Uses the file descriptor pinned inside `FsStorage.reader` at constructor time. | Immunizes GC against runtime root replacement, symlink swaps, and pathname mutations. | Real filesystem integration tests verifying traversal remains bound to pinned descriptor across directory rename. |
| **D-12** | **Resource Budgeting** | Enforces strict, caller-supplied limits with checked arithmetic; fails closed on exhaustion. | Protects production nodes from stack overflow, memory exhaustion, and runaway I/O. Intermediate and terminal limits decoupled. | Unit and integration tests verifying budget exhaustion triggers fail-closed error without further I/O. |

---

## 4. Resource Policy, Configuration Defaults & Operational Consequences

### 4.1 Production Limits Configuration Matrix

| Configuration Key | Environment Variable | Compiled Default | Operational Role |
| :--- | :--- | :--- | :--- |
| `fs_gc_discovery_max_depth` | `REGISTRY_FS_GC_DISCOVERY_MAX_DEPTH` | `16` | Maximum directory tree descent depth below `repos/`. Prevents stack exhaustion in adversarial nested trees. |
| `fs_gc_discovery_max_dir_enumerations` | `REGISTRY_FS_GC_DISCOVERY_MAX_DIR_ENUMERATIONS` | `10,000` | Maximum intermediate directory enumerations. Bounds discovery filesystem traversal. |
| `fs_gc_discovery_max_total_entries` | `REGISTRY_FS_GC_DISCOVERY_MAX_TOTAL_ENTRIES` | `100,000` | Cumulative directory entries processed across all intermediate directories. |
| `fs_gc_discovery_max_manifest_dirs` | `REGISTRY_FS_GC_DISCOVERY_MAX_MANIFEST_DIRS` | `10,000` | Maximum terminal `manifests/` directories retained for subsequent reference collection. |
| `fs_gc_discovery_max_retained_path_bytes` | `REGISTRY_FS_GC_DISCOVERY_MAX_RETAINED_PATH_BYTES` | `1,500,000` | Maximum logical bytes accounted for intermediate pending queue and discovered manifest directories. |
| `fs_gc_discovery_intermediate_max_entries` | `REGISTRY_FS_GC_DISCOVERY_INTERMEDIATE_MAX_ENTRIES` | `1,000` | Per-directory entry limit passed to `enumerate_dir` on intermediate folders. |
| `fs_gc_discovery_intermediate_max_name_bytes` | `REGISTRY_FS_GC_DISCOVERY_INTERMEDIATE_MAX_NAME_BYTES` | `100,000` | Per-directory name byte limit passed to `enumerate_dir` on intermediate folders. |
| `fs_gc_discovery_max_terminal_dir_enumerations` | `REGISTRY_FS_GC_DISCOVERY_MAX_TERMINAL_DIR_ENUMERATIONS` | `10,000` | Maximum terminal `manifests/` directories opened for candidate enumeration. |
| `fs_gc_discovery_terminal_max_entries` | `REGISTRY_FS_GC_DISCOVERY_TERMINAL_MAX_ENTRIES` | `10,000` | Per-directory entry limit passed to `enumerate_dir` on terminal `manifests/` folders. |
| `fs_gc_discovery_terminal_max_name_bytes` | `REGISTRY_FS_GC_DISCOVERY_TERMINAL_MAX_NAME_BYTES` | `1,500,000` | Per-directory name byte limit passed to `enumerate_dir` on terminal `manifests/` folders. |
| `fs_gc_discovery_max_total_manifest_entries` | `REGISTRY_FS_GC_DISCOVERY_MAX_TOTAL_MANIFEST_ENTRIES` | `100,000` | Cumulative dirents processed across all terminal directories combined. |
| `fs_gc_discovery_max_manifests_read` | `REGISTRY_FS_GC_DISCOVERY_MAX_MANIFESTS_READ` | `10,000` | Maximum manifest payload files opened, buffered, and parsed for references. |
| `fs_gc_discovery_max_total_references` | `REGISTRY_FS_GC_DISCOVERY_MAX_TOTAL_REFERENCES` | `100,000` | Maximum unique protected blob digests retained in the resulting protected set. |
| `fs_gc_discovery_max_retained_logical_bytes` | `REGISTRY_FS_GC_DISCOVERY_MAX_RETAINED_LOGICAL_BYTES` | `10,000,000` | Maximum cumulative logical string bytes accounted across terminal keys and protected digests. |
| `fs_gc_discovery_max_manifest_payload_bytes` | `REGISTRY_FS_GC_DISCOVERY_MAX_MANIFEST_PAYLOAD_BYTES` | `None` (Unset) | Optional ceiling on individual manifest payload size in bytes. Unbounded by default to avoid rejecting valid large manifests. |

### 4.2 Constructor Validation

Validation is strictly enforced within the crate-visible constructor `FsStorage::try_new_with_gc_limits`:
- Rejects any zero-valued count limits (`max_depth == 0`, `max_dir_enumerations == 0`, etc.).
- Rejects zero-valued byte limits (`max_retained_path_bytes == 0`, `max_retained_logical_bytes == 0`, etc.).
- Rejects `max_manifest_payload_bytes == Some(0)`.
- Validates that `intermediate_dir_limits.max_entries <= max_total_discovery_entries` and `intermediate_dir_limits.max_total_name_bytes <= max_discovery_retained_path_bytes`.
- Validates that `terminal_per_dir_limits.max_entries <= max_total_manifest_entries` and `terminal_per_dir_limits.max_total_name_bytes <= max_retained_logical_bytes`.

---

## 5. Verification Results & Test Inventory

### 5.1 Test Execution Summary

All compilation checks, linters, unit tests, focused integration tests, and coordination regressions were executed and verified clean. Complete logs and exit statuses were captured in `/home/dietmar/devel/rust/manifest-read-review-evidence/session-20260912-0340/logs/`.

- `cargo fmt --check`: **PASSED** (Exit 0, 0 formatting discrepancies)
- `cargo check --all-targets --all-features --locked`: **PASSED** (Exit 0)
- `cargo clippy --all-targets --all-features --locked -- -D warnings`: **PASSED** (Exit 0, 0 warnings)

#### 5.2 Corrected Test Inventory & Execution Accounting

To ensure rigorous accounting, tests are categorized by their execution mechanism and assertions:
- **Unique Exercised Test Scenarios:** **189**
- **Parent Orchestration Tests:** **10** (in `gc_contained_discovery_config_tests`, spawning isolated child processes and asserting zero exit status)
- **Worker Executions in Isolated Child Processes:** **10** (in child processes with `RUN_ISOLATED=1`, exercising actual configuration assertions)
- **Worker Entries Returning Without Assertions in Parent:** **10** (in `gc_contained_discovery_config_tests`, returning immediately when `RUN_ISOLATED` is unset; these do not count as additional verified scenarios)
- **Repeated Executions Across Suites:** **12** (the 12 passing tests from `blob_gc::policy` re-executed during the `cargo test --lib blob_gc` run)
- **Total Parent Harness Passing Entries:** **211** (189 unique exercised + 10 parent worker no-ops + 12 policy repeats)
- **Statically Ignored Tests:** **4** (annotated with `#[ignore = "..."]`, not counted as executed)
- **Failed Tests:** **0**

| Test Suite / Scope | Unique Exercised Scenarios | Parent No-Op Worker Entries | Repeated Cross-Suite Runs | Ignored | Failed | Notes |
| :--- | :---: | :---: | :---: | :---: | :---: | :--- |
| `storage::fs::repo_discovery` | 25 | 0 | 0 | 1 | 0 | 1 test statically ignored: requires unprivileged execution (chmod 0o000 DAC bypass under root). |
| `storage::fs::manifest_refs` | 17 | 0 | 0 | 1 | 0 | 1 test statically ignored: requires unprivileged execution (chmod 0o000 DAC bypass under root). |
| `storage::fs::tests` (limits validation) | 1 | 0 | 0 | 0 | 0 | Direct constructor limits validation and rejection of invalid configurations. |
| `gc_contained_discovery_config_tests` | 10 | 10 | 0 | 0 | 0 | 10 parent orchestration tests executing 10 isolated worker processes. 10 worker harness entries returned without assertions. |
| `gc_contained_discovery_integration_tests` | 13 | 0 | 0 | 0 | 0 | Comprehensive integration suite covering forwarding, fallback avoidance, planning/quarantine/deletion fail-closed, real timestamp init. |
| `ports_wiring_tests` | 10 | 0 | 0 | 0 | 0 | Verifies adapter forwarding and backend decoupling across port views. |
| `blob_gc::policy` | 12 | 0 | 0 | 2 | 0 | 2 tests statically ignored: requires unprivileged execution (chmod 0o000 DAC bypass under root). (Re-executed in `blob_gc`). |
| `blob_gc` (lib root tests) | 4 | 0 | 12 | 0 | 0 | 4 unique root tests. 12 policy tests re-executed from submodule. |
| `gc_adversarial_coordination_tests` | 22 | 0 | 0 | 0 | 0 | Full adversarial coordination suite: mutation races, lock clearing, permission checks, quota coordination. |
| `manifest_lifecycle_tests` | 75 | 0 | 0 | 0 | 0 | Comprehensive manifest lifecycle and storage recovery regression suite. |
| **Total Test Accounting** | **189** | **10** | **12** | **4** | **0** | **189 Unique Exercised Scenarios (211 total harness passes, 0 failures)** |

### 5.3 Ignored Test Explanation

The 4 ignored tests are permission-denied tests:
1. `storage::fs::repo_discovery::tests::test_repo_discovery_real_fs_permission_denied_restoration_guard`
2. `storage::fs::manifest_refs::tests::test_manifest_refs_linux_real_fs_permission_denied_restoration_guard`
3. `blob_gc::policy::tests::test_gc_manifest_discovery_unreadable_directory_permission_denied_ignored`
4. `blob_gc::policy::tests::test_gc_manifest_discovery_unreadable_manifest_permission_denied_ignored`

**Reason for Ignored Status**: These tests are statically decorated with `#[ignore = "..."]` in the source code. They do not dynamically self-skipping at runtime; rather, the Cargo test harness statically skips them during standard test runs. They require an unprivileged execution environment where `chmod 0o000` is enforced by the operating system. In environments running under `root` (UID 0) or possessing `CAP_DAC_OVERRIDE`, the Linux kernel bypasses mode `000` DAC permissions and allows reads to succeed, making permission checks non-reproducible. Following strict evidence accounting, these 4 statically ignored tests are not counted as executed tests. All canonical quality gates, including O-16, remain explicitly **OPEN**.

### 5.4 Source Hash Continuity & Documentation-Only Updates

- **Source and Test File Continuity**: All 19 modified source and test files (`src/**/*.rs` and `tests/**/*.rs`) remained 100% byte-for-byte identical across verification and match the pre-verification (`source_hashes_pre_verification.sha256`) and post-verification (`source_hashes_post_verification.sha256`) records exactly.
- **Documentation Updates**: This implementation record (`docs/architecture/filesystem-gc-contained-discovery-production-cutover.md`) was updated as a documentation-only update between verification records:
  - Pre-verification hash: `2bf5ab7a28a1ce0976605841342850e468186dd4d190709f9896172ce274e601`
  - Post-verification hash: `8d9909a0fedda4b77ab71761ef563a908fc445d6e6848151a20e19896cdb6c28`
  - Final evidence update: documentation-only accounting update verified in package `MANIFEST.sha256`.
No product code or test logic was modified after verification.

---

## 6. Explicit Limitations & Operational Constraints

1. **No Snapshot Isolation or Global Rollback**:
   Discovery iterates across filesystem directories incrementally. It does not hold a global filesystem snapshot or read-consistent transaction. Manifest publications or deletions concurrent with discovery may be partially observed. Safety is maintained by the mutation coordinator, writer locks, and mandatory per-candidate revalidation.

2. **Repeated Per-Candidate Scan Cost**:
   Per-candidate revalidation deliberately re-checks manifest references prior to each candidate deletion. This incurs repeated discovery and reference parsing overhead, prioritizing safety against deletion races over GC execution speed.

3. **Logical Accounting Is Not a Total Heap Bound**:
   The resource limiters account for logical path bytes, key strings, and parsed digest sets using checked arithmetic. However, logical accounting does not capture allocator overhead, internal tree nodes, page table overhead, or jemalloc heap fragmentation.

4. **Unbounded Payload Buffering by Default**:
   By user approval and production design, `max_manifest_payload_bytes` is left unset (`None`) by default. Individual manifest files are buffered into heap memory up to available system memory or until `take(limit + 1)` triggers if configured. Operators must configure `REGISTRY_FS_GC_DISCOVERY_MAX_MANIFEST_PAYLOAD_BYTES` if hard payload buffering limits are required in memory-constrained environments.

5. **Genuine, Accessible, Stable procfs Requirement**:
   Linux `openat2` resolution and `/proc/self/fd/<n>` operations require a genuine, accessible, mount-stable `procfs`. Environments lacking procfs (e.g., restricted chroots or minimal containers without `/proc` mounted) will fail initialization.

6. **No Mount or Hard-Link Isolation**:
   `openat2` with `RESOLVE_BENEATH` prevents symlink escapes, but hard links within the same filesystem or bind-mounted subtrees within `repos/` can theoretically link to outside inodes without triggering `openat2` resolution failure if they resolve beneath the root fd.

7. **Pinned-Read/Pathname-Mutation Divergence**:
   `FsStorage.reader` pins the initial storage root descriptor at constructor time. If the physical path on disk is subsequently renamed or unlinked while keeping the file descriptor open, contained discovery continues to operate on the original pinned inode, while pathname-based operations (like external shell scripts or monitoring tools) may observe the new path.

8. **Non-Linux Verification Status**:
   Production contained discovery relies on Linux-specific kernel capabilities (`openat2`, `RESOLVE_BENEATH`, `RESOLVE_NO_MAGICLINKS`). On non-Linux platforms (macOS, Windows, BSD), the implementation falls back to emulated pathname validation, which has not been certified for production concurrency safety.
