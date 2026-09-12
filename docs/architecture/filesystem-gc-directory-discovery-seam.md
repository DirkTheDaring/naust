# Implementation & Evidence Record: Filesystem GC Directory Discovery Test Seam

**Repository:** `registry-rust`
**Target Document:** `docs/architecture/filesystem-gc-directory-discovery-seam.md`
**Authoritative Baselines:**
- `registry-rust` HEAD: `12533aedd00d5f2f6b76942a80a1dc40fbaf344d`
- `storage-layer-rust` HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`
- Reviewed Contract Archive SHA-256: `cdee25fc78ff8864ea526c59d4a5d11d7461adb01a48dee6733273e5a774f637`
- Preserved Initial Seam Review Archive SHA-256: `72447779084eea37af3657e733070be5fbd03d7f246536b2c2cf123d55484de6`

**Scope:** Implementation of the test-only contained manifest-directory discovery seam for filesystem garbage collection reachability.
**Status:** **TEST SEAM ONLY — PRODUCTION CODE UNCHANGED — NOT AUTHORIZED FOR PRODUCTION CUTOVER — NOT COMMITTED**.
**Quality Gates:** **O-03, O-04, O-05, O-06, O-13, O-15, O-16, and D-06 remain explicitly OPEN**.

---

## 1. Executive Summary & Experimental Boundary

### 1.1 Incremental Extraction Trajectory
The filesystem storage extraction initiative decomposes host filesystem interactions into safe descriptor-relative containment primitives in `storage-fs` and integrates them into `registry-rust`. Prior milestones established contained payload reads, bounded single-directory enumeration (`FsMetadataReader::enumerate_dir`), contained file metadata inspection, contained manifest reads, contained manifest listing (`FsStorage::list_manifest_digests_page`), lifecycle reference discovery hardening, reference-index synchronization hardening, GC manifest discovery characterization, and filesystem repository discovery characterization.

Following the finalized decision contract in `docs/architecture/filesystem-gc-repository-discovery-decisions.md` (reviewed archive SHA-256 `cdee25fc78ff8864ea526c59d4a5d11d7461adb01a48dee6733273e5a774f637`), this slice implements the **contained manifest-directory discovery test seam** in `src/storage/fs/repo_discovery.rs`.

### 1.2 Strict Production Boundary
Authorization covers this test-only experiment. The following boundaries are strictly enforced:
- **Production GC Routing Unchanged:** The direct filesystem bypass walker (`build_manifest_protected_set_fs` in `src/blob_gc/policy.rs`) remains active and unmodified.
- **Production Catalog Discovery Unchanged:** The public OCI repository catalog reader (`FsStorage::list_repositories` / `list_repo_names` in `src/storage/fs.rs`) remains active and unmodified.
- **Storage Layer Unchanged:** `storage-layer-rust` is preserved byte-for-byte without edits.
- **No Dependencies or Public API Changes:** No dependencies were added, modified, or bumped in `Cargo.toml`. No public API signatures were altered.
- **Authorized File Changes:**
  1. `src/storage/fs.rs`: Added only the `#[cfg(test)] #[path = "fs/repo_discovery.rs"] mod repo_discovery;` module declaration.
  2. `src/storage/fs/repo_discovery.rs`: New test-only seam and test suite.
  3. `docs/architecture/filesystem-gc-directory-discovery-seam.md`: This implementation and evidence record.

---

## 2. Implementation Contract & Architecture

### 2.1 Core Output & Path Representation
The discovery seam discovers and returns a collection of representable **manifest-directory `ObjectKey`s**, not user-facing repository strings (`Vec<String>`) or parsed manifest digests (`HashSet<String>`):

```rust
pub(crate) async fn discover_manifest_dirs_impl(
    enumerator: &(impl DiscoveryDirEnumerator + ?Sized),
    limits: DiscoveryTestLimits,
) -> Result<Vec<ObjectKey>, StorageError>
```

Discovered paths represent terminal manifest directories beneath the pinned root descriptor:
- `repos/library/ubuntu/manifests` (ordinary nested layout)
- `repos/tags/sub1/sub2/manifests` (deep reserved ancestor `tags`)
- `repos/blobs/internal/manifests` (deep reserved ancestor `blobs`)
- `repos/manifests` (root-adjacent manifest directory)
- `repos/C:drive/manifests` (valid non-root colon component)

This output replaces the previous ambiguous pairing of `Vec<String>` repository names with a boolean `root_manifests_present` flag, providing usable, strongly typed storage keys for subsequent contained enumeration.

### 2.2 Injectable Enumeration Abstraction
To enable deterministic unit testing and fault injection without requiring private testing hooks in `storage-fs`, the seam defines a narrow async enumeration trait:

```rust
#[async_trait]
pub(crate) trait DiscoveryDirEnumerator: Send + Sync {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError>;
}

#[async_trait]
impl DiscoveryDirEnumerator for FsMetadataReader {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError> {
        self.enumerate_dir(target, limits).await
    }
}
```

---

## 3. Implementation Clarifications & Technical Realities

The implementation in `src/storage/fs/repo_discovery.rs` explicitly resolves core architectural questions:

### 3.1 Enumeration Count Accounting
- **Attempted Call Accounting:** The counter `dir_enumerations` counts attempted enumeration calls, including the initial call on `repos/` that may return `NotFound`.
- **Pre-Call Capacity Check:** Capacity is checked *before* invoking `enumerator.enumerate_dir`:
  ```rust
  dir_enumerations = checked_increment_enumerations(dir_enumerations, limits.max_dir_enumerations)?;
  ```
- **Missing Root Behavior:** If `repos/` does not exist on disk, discovery requires **one permitted call attempt** (`max_dir_enumerations >= 1`) and returns empty success `Ok(Vec::new())`. If `max_dir_enumerations == 0`, traversal fails immediately with `StorageError::backend` before calling `enumerate_dir`. A missing root does not mean zero calls were performed.

### 3.2 Traversal Depth Accounting
- **Depth Origin:** Root `repos/` has depth 0.
- **Checked Arithmetic:** Child depth is computed using checked arithmetic via helper `checked_increment_depth(current_depth, limits.max_depth)`.
- **Universal Application:** The depth limit `limits.max_depth` is applied to **every** directory child encountered, including terminal `manifests` directories, before enqueueing or retaining it:
  ```rust
  let child_depth = checked_increment_depth(current_depth, limits.max_depth)?;
  ```
  Consequently, with `max_depth == 0`, discovery of any directory child (even `repos/manifests` at depth 1) fails closed.

### 3.3 Counters and Bounds
- **Inclusive Limits & Checked Arithmetic:** All counters (`dir_enumerations`, `total_entries`, `manifest_dirs.len()`, `retained_path_bytes`, `current_depth`) use checked arithmetic (`checked_add`, `checked_sub`) and inclusive limits throughout.
- **Pre-Retention Output Check:** Output capacity is checked before retaining a terminal path:
  ```rust
  checked_check_manifest_dirs_capacity(manifest_dirs.len(), limits.max_manifest_dirs)?;
  ```
- **Explicit Zero-Limit Semantics:**
  - `max_dir_enumerations == 0`: fails before first call.
  - `max_depth == 0`: succeeds only if `repos/` is empty or missing; fails if any child directory exists.
  - `max_total_entries == 0`: succeeds if 0 entries returned; fails on first entry.
  - `max_manifest_dirs == 0`: fails if any `manifests` directory is encountered.
  - `max_retained_path_bytes < 5`: fails before enqueueing initial `repos` (5 bytes).
- **All Returned Entries Counted:** The counter `total_entries` increments for every `DirEntry` returned in a batch from `enumerate_dir`, including entries that are subsequently skipped (regular files, symlinks, FIFOs).
- **Allocation Sequencing:** Whole-walk budget checks occur as the returned `Vec<DirEntry>` is iterated, meaning that one bounded enumeration batch (up to `per_dir_limits.max_entries`) has already been allocated by `enumerate_dir` before a whole-walk entry limit triggers abort.

### 3.4 Retained Paths Accounting
- **Logical Path Byte Tracking:** `RetainedPathBytesTracker` tracks the cumulative logical path bytes retained in:
  1. The pending traversal collection (`queue: VecDeque<(ObjectKey, usize)>`).
  2. The output collection (`manifest_dirs: Vec<ObjectKey>`).
- **Capacity Check & Move Accounting:**
  - Initial `repos` (5 bytes): charged before enqueueing.
  - Dequeue: when popped from queue for enumeration, `current_key.len()` is debited (`tracker.debit`).
  - Child enqueue: `child_key.len()` is charged against remaining capacity (`tracker.charge`).
  - Manifest retention: `child_key.len()` is charged against remaining capacity, added, and permanently retained in output.
- **Heap Memory Scope:** This tracks logical path bytes retained in data structures. It is not an absolute heap-memory guarantee: temporary string allocations, enumeration batch buffers, and collection capacity overhead remain separate.
- **Exact Sibling and Retained Output Boundaries:** A deterministic fixture verifies that when multiple siblings and terminal outputs are queued, path bytes are debited on dequeue while previously discovered terminal outputs continue charging against the budget.

### 3.5 Error and Entry Policy
- **Initial `repos` NotFound:** Returns `Ok(Vec::new())` (clean empty-registry state).
- **Previously Observed Child NotFound (TOCTOU):** If a child directory was observed during parent listing but returns `NotFound` when opened, fails closed with:
  `StorageError::io(format!("observed directory disappeared before enumeration: {}", key.as_str()))`.
- **Symlink Directory Entries:** Skipped during directory iteration (`entry.file_type() == DirEntryType::Symlink`), matching existing GC behavior.
- **Path-Resolution Rejection:** Kernel `openat2` resolution rejection (`RESOLVE_NO_SYMLINKS`, `ELOOP`) is mapped to `StorageError::io`.
- **Non-Directory Entries:** Regular files, FIFOs, sockets, and character/block devices are skipped.
- **Non-UTF-8 Directory Names:** Fails closed immediately with `StorageError::corrupt_data(format!("unrepresentable non-UTF-8 directory name: {:?}", name))`.
- **Component Validation & ObjectKey Semantics:** Raw directory entry names are validated via `validate_dir_component` before key composition. Rejects empty names, `.`, `..`, `/`, `\\`, `\0`, and control characters. Segments containing colons (e.g. `C:drive`) are permitted because `ObjectKey` permits colons in non-root positions (`repos/C:drive` is fully valid). The composed key is validated using `ObjectKey::parse`.
- **Wrong-Type `repos` Root:** When `repos` is a regular file, `FsDirError::NotADirectory` maps to `StorageError::corrupt_data("target path is not a directory: repos")`.
- **Permission Errors:** `FsDirError::PermissionDenied` maps to `StorageError::permission_denied(...)`.
- **Budget Exhaustion & Overflow:** Limit exhaustion or arithmetic overflow maps to `StorageError::backend(...)`.
- **Remaining `FsDirError` Mappings:**
  - `LimitExceeded` -> `StorageError::backend(...)`
  - `EntryDisappeared` -> `StorageError::io(...)`
  - `Io` -> `StorageError::io(...)`
  - `SyscallUnsupported` -> `StorageError::configuration(...)`
  - `PlatformUnsupported` -> `StorageError::configuration(...)`
  - `RuntimeMissing` -> `StorageError::backend(...)`
  - `TaskJoinFailed` -> `StorageError::backend(...)`
  - Fallback `other` -> `StorageError::backend(...)`

### 3.6 Terminal-Directory Limitation & TOCTOU Realities
When a directory entry named `manifests` is observed in a parent directory listing, its composed `ObjectKey` is recorded as a terminal leaf directly from the parent listing without opening it.

**Residual Race & Isolation Boundaries:**
- The seam does not verify whether the terminal `manifests` directory remains present, was replaced by a symlink or file, has readable permissions, or contains valid manifests.
- Downstream manifest listing (`list_manifest_digests_page_impl`) performs descriptor-relative opening under `openat2` containment and validates file types.
- Disappearance or modification between discovery and listing remains an inherent characteristic of non-transactional host filesystems. No claim of point-in-time snapshot isolation across system calls is made.

---

## 4. Deferrals & Out-of-Scope Concerns

The following concerns are explicitly **deferred** to subsequent slices:
1. **Manifest Payload Reads:** Bounding payload buffers and streaming manifest bytes remain outside this slice.
2. **Digest Validation & Filtering:** Canonical 64-hex SHA-256 and 128-hex SHA-512 filtering belongs in manifest listing.
3. **Protected-Set Construction:** Parsing manifest payloads and populating `HashSet<String>` of protected blob digests remains in `src/blob_gc/policy.rs`.
4. **Production Defaults:** The discovery seam uses caller-supplied, test-only limits (`DiscoveryTestLimits`). Production defaults are not defined or approved here.
5. **Production Routing Cutover:** Replacing `build_manifest_protected_set_fs` or changing the storage port in `src/blob_gc/policy.rs` is not authorized.

---

## 5. Verification Evidence & Test Matrix

### 5.1 Test Execution Matrix

| Test Name | Harness | Scenario / Verification Objective | Result | Exit Status |
| :--- | :--- | :--- | :--- | :--- |
| `test_fake_missing_root_returns_empty_success_with_one_call` | Recording Fake | Initial `repos/` returning `NotFound` returns `Ok([])` with exactly 1 call recorded. | **PASSED** | 0 |
| `test_fake_zero_dir_enumerations_limit_fails_before_call` | Recording Fake | `max_dir_enumerations == 0` fails closed with `Backend` error before any calls are made; 0 calls recorded. | **PASSED** | 0 |
| `test_fake_dir_enumerations_limit_boundary_and_one_over` | Recording Fake | Boundary success at 2 calls; one-over failure at 1 call when 2 are needed. | **PASSED** | 0 |
| `test_fake_depth_limits_zero_boundary_and_terminal_leaf_check` | Recording Fake | Depth 0 with empty root succeeds; depth 0 with child fails; depth 0 with terminal manifests fails; depth 1 with terminal manifests succeeds. | **PASSED** | 0 |
| `test_fake_depth_limits_nested_boundary_and_one_over` | Recording Fake | Nested `repos/a/manifests` fails under `max_depth: 1`; succeeds under `max_depth: 2`. | **PASSED** | 0 |
| `test_fake_total_entries_counter_counts_skipped_entries` | Recording Fake | Skipped symlinks/regular/other count toward `max_total_entries`; boundary success at 4; fails at 3. | **PASSED** | 0 |
| `test_fake_manifest_dirs_output_limit_boundary_and_zero` | Recording Fake | Zero limit fails; 1-limit fails on 2nd terminal leaf; 2-limit succeeds. | **PASSED** | 0 |
| `test_fake_retained_path_bytes_limit_accounting_and_boundaries` | Recording Fake | Cap < 5 fails before enqueue; child exceeding cap fails; boundary success at exact cap. | **PASSED** | 0 |
| `test_fake_retained_path_bytes_siblings_and_retained_output_exact_boundary` | Recording Fake | Multiple queued siblings plus retained terminal output; exact combined boundary success (45 bytes) and 1-byte-under failure (44 bytes); debit on dequeue and continued output charging. | **PASSED** | 0 |
| `test_fake_observed_child_disappearance_returns_io` | Recording Fake | Observed child returning `NotFound` on opening maps to `StorageError::io` with informative message. | **PASSED** | 0 |
| `test_fake_atomic_failure_on_error_after_earlier_discoveries` | Recording Fake | Subsequent directory error aborts whole walk; zero partial results returned. | **PASSED** | 0 |
| `test_fake_failure_after_earlier_discovery_due_to_budget_limit_no_further_calls` | Recording Fake | Budget limit failure occurs after earlier discovery; verifies Err returned and zero subsequent calls issued. | **PASSED** | 0 |
| `test_fake_terminal_manifests_leaves_never_opened_or_traversed` | Recording Fake | Verified `fake.calls()` contains only ancestors; zero calls beneath `manifests`. | **PASSED** | 0 |
| `test_fake_invalid_injected_components_rejected_with_corrupt_data` | Recording Fake | Slashes, backslashes, NUL bytes, dots, control characters rejected with `CorruptData`. | **PASSED** | 0 |
| `test_fake_component_with_colon_allowed_and_preserved` | Recording Fake | Positive test: `repos/C:drive/manifests` parsed and preserved byte-for-byte matching `ObjectKey` semantics. | **PASSED** | 0 |
| `test_accounting_helpers_overflow_and_underflow_handling` | Pure Unit Test | Verifies `charge` overflow, `debit` underflow, `depth` overflow, `entries` overflow; documents unreachable branch in `enumerations`. | **PASSED** | 0 |
| `test_fake_non_utf8_directory_rejected_with_corrupt_data` | Recording Fake | Non-UTF-8 directory entry rejected with `StorageError::corrupt_data`. | **PASSED** | 0 |
| `test_fake_explicit_fs_dir_error_mappings` | Recording Fake | Verified explicit mapping of `NotADirectory`, `PermissionDenied`, `ResolutionRejected`, `LimitExceeded`, `EntryDisappeared`, `SyscallUnsupported`, `PlatformUnsupported`. | **PASSED** | 0 |
| `test_repo_discovery_real_fs_ordinary_nested_and_reserved_ancestor_layouts` | Real Linux FS | Discovers `repos/manifests`, `repos/library/ubuntu/manifests`, `repos/tags/sub1/sub2/manifests`, `repos/blobs/internal/manifests`. Non-manifest meta directory ignored. | **PASSED** | 0 |
| `test_repo_discovery_real_fs_terminal_manifests_leaf_non_recursion` | Real Linux FS | Subdirectory inside `repos/app/manifests/nested` is not traversed or returned. | **PASSED** | 0 |
| `test_repo_discovery_real_fs_symlink_entries_skipped_vs_path_symlink_failure` | Real Linux FS | Symlink dirent inside `repos/` skipped; `repos/` path symlink rejected by kernel `openat2`. | **PASSED** | 0 |
| `test_repo_discovery_real_fs_non_utf8_directory_rejection` | Real Linux FS | Directory with invalid UTF-8 bytes fails closed with `StorageError::corrupt_data`. | **PASSED** | 0 |
| `test_repo_discovery_real_fs_wrong_type_root_returns_corrupt_data` | Real Linux FS | `repos` regular file maps to `StorageError::corrupt_data("target path is not a directory: repos")`. | **PASSED** | 0 |
| `test_repo_discovery_real_fs_per_directory_exhaustion` | Real Linux FS | Single-directory limit exhaustion in `enumerate_dir` maps to `StorageError::backend`. | **PASSED** | 0 |
| `test_repo_discovery_real_fs_pinned_root_across_replacement` | Real Linux FS | Pinned descriptor continues resolving original tree when `storage_root` is renamed/replaced on host filesystem. | **PASSED** | 0 |
| `test_repo_discovery_real_fs_permission_denied_restoration_guard` | Real Linux FS (`#[ignore]`) | `chmod 000` on directory returns `PermissionDenied`; `ScopedPermReset` verifies permission restoration. | **PASSED** | 0 |

### 5.2 Test Execution Totals

- **Focused Seam Tests (`cargo test --lib storage::fs::repo_discovery`):**
  - Standard run: 25 passed; 0 failed; 1 ignored (duration: 0.00s).
  - Explicit ignored run: 1 passed; 0 failed (duration: 0.00s).
  - Unique tests: 26; Total executed: 26; Total passed: 26; Failed: 0.
- **Strict Clippy (`cargo clippy --locked --all-targets --all-features`):**
  - Exit status: 0 (clean, 0 warnings, 0 errors).
- **Formatting (`cargo fmt --check`):**
  - Exit status: 0 (clean formatting).
- **Git Diff & Whitespace Verification:**
  - `git diff --check`: Exit status 0.
  - Python strict error-returning whitespace validator: Exit status 0.
- **Regression Test Suites:**
  - `cargo test blob_gc`: 16 passed; 0 failed; 2 ignored.
  - `cargo test --lib storage::fs::manifest_listing`: 15 passed; 0 failed; 1 ignored.
  - `cargo test --test manifest_lifecycle_tests`: 75 passed; 0 failed; 0 ignored.

---

## 6. Canonical Quality Gates

All canonical quality gates remain explicitly **OPEN**:
- `O-03`: Key and continuation-token contracts. (**OPEN**)
- `O-04`: Filesystem write durability and containment. (**OPEN**)
- `O-05`: Broader filesystem read containment. (**OPEN**)
- `O-06`: Typed AWS mapping and pinned-MinIO evidence. (**OPEN**)
- `O-13`: Hosting, distribution, and release strategy. (**OPEN**)
- `O-15`: Non-Linux verification. (**OPEN**)
- `O-16`: Earlier Slice 11 audit/test-inventory evidence. (**OPEN**)
- `D-06`: Broader extraction, cutover, compatibility, and distribution acceptance. (**OPEN**)
