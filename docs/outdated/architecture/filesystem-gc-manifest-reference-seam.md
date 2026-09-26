> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. The seam landed and was promoted to production (`d51ea1a`, `2fc21aa`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Implementation & Evidence Record: Filesystem GC Manifest Reference Test Seam

**Repository:** `registry-rust`
**Target Document:** `docs/architecture/filesystem-gc-manifest-reference-seam.md`
**Authoritative Baselines:**
- `registry-rust` baseline commit: `3bbe006148c83add6acfb2f7e0c5df9a21d38b7e`
- `storage-layer-rust` baseline commit: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`
- Reviewed Design Archive:
  `~/devel/rust/manifest-read-review-evidence/session-20260912-0210/filesystem-gc-manifest-reference-seam-design.tar.gz`
  Size: 16886 bytes
  SHA-256: `f5ded1aade189f726c7c9d9047ccaeaee69f756ada0b959c767823e943fd90d4`

**Scope:** Test-only implementation of contained filesystem GC manifest enumeration and reference collection.
**Status:** **TEST SEAM ONLY — PRODUCTION CODE UNCHANGED — NOT AUTHORIZED FOR PRODUCTION CUTOVER — NOT COMMITTED**.
**Quality Gates:** **O-03, O-04, O-05, O-06, O-13, O-15, O-16, and D-06 remain explicitly OPEN**.

---

## 1. Executive Summary & Experimental Boundary

### 1.1 Trajectory
The filesystem storage extraction initiative isolates host filesystem operations into safe descriptor-relative containment primitives in `storage-fs` and integrates them into `registry-rust`. Following the committed directory discovery seam (`src/storage/fs/repo_discovery.rs`), this component implements the test-only **manifest reference collection seam** in `src/storage/fs/manifest_refs_seam.rs`.

The seam consumes observed terminal manifest-directory `ObjectKey`s produced by directory discovery, enumerates their contents beneath the pinned storage root, filters entries against an experimental digest-filename policy, opens and parses manifest payloads using `parse_manifest_refs`, and populates a protected digest set.

### 1.2 Strict Production Boundary
This implementation is strictly experimental and confined to test execution:
- **Production GC Routing Unchanged:** `src/blob_gc/policy.rs` (`build_manifest_protected_set_fs`) continues to use the uncontained `walkdir` walker.
- **Production Catalog Discovery Unchanged:** `FsStorage::list_repositories` / `list_repo_names` in `src/storage/fs.rs` remain unmodified.
- **Storage Layer Unchanged:** `storage-layer-rust` is preserved byte-for-byte with zero modifications.
- **No Dependencies or Configuration Changes:** `Cargo.toml` dependencies, features, and production configuration structures are unmodified.
- **Authorized File Changes:**
  1. `src/storage/fs.rs`: Added only the `#[cfg(test)] #[path = "fs/manifest_refs_seam.rs"] mod manifest_refs_seam;` declaration.
  2. `src/storage/fs/manifest_refs_seam.rs`: New test-only seam and test suite.
  3. `docs/architecture/filesystem-gc-manifest-reference-seam.md`: This implementation and verification record.

---

## 2. Architecture & Seam Contract

### 2.1 Trait Composition & Reader Instance
The seam introduces a unifying trait that composes directory enumeration and payload opening without modifying `storage-fs`:

```rust
pub(crate) trait ManifestRefReader:
    DiscoveryDirEnumerator + ObjectPayloadReader + Send + Sync
{
}

impl<T> ManifestRefReader for T where
    T: DiscoveryDirEnumerator + ObjectPayloadReader + Send + Sync + ?Sized
{
}
```

In end-to-end execution, **one single reader instance** (`FsMetadataReader`) is used across directory discovery, terminal directory enumeration, and manifest payload opening. This guarantees that all path resolutions operate relative to the same pinned root directory descriptor.

### 2.2 Core Seam APIs
The seam provides two entrypoints:

1. **Contained Manifest Reference Collection from Terminal Keys:**
```rust
pub(crate) async fn collect_manifest_references_impl(
    reader: &(impl ManifestRefReader + ?Sized),
    terminal_manifest_dirs: &[ObjectKey],
    limits: ManifestReferenceTestLimits,
) -> Result<ManifestReferenceObservationSet, StorageError>
```

2. **End-to-End Discovery and Manifest Reference Collection:**
```rust
pub(crate) async fn collect_manifest_references_end_to_end(
    reader: &(impl ManifestRefReader + ?Sized),
    discovery_limits: super::repo_discovery::DiscoveryTestLimits,
    ref_limits: ManifestReferenceTestLimits,
) -> Result<ManifestReferenceObservationSet, StorageError>
```

### 2.3 Observation Set Structure
The observation set captures all parsed manifests and protected digests alongside accounting telemetry:

```rust
pub(crate) struct ManifestReferenceObservationSet {
    pub(crate) manifest_keys: Vec<ObjectKey>,
    pub(crate) protected_digests: HashSet<Digest>,
    pub(crate) terminal_dirs_enumerated: usize,
    pub(crate) total_dirents_observed: usize,
    pub(crate) manifests_parsed: usize,
    pub(crate) total_references_recorded: usize,
    pub(crate) retained_logical_bytes: usize,
}
```

---

## 3. Concrete Implementation Semantics

### 3.1 Experimental Filename Policy & Entry Filtering
- **Accepted Formats:**
  - Lowercase 64-character hexadecimal strings -> interpreted as SHA-256 (`sha256:<hex>`).
  - Lowercase 128-character hexadecimal strings -> interpreted as SHA-512 (`sha512:<hex>`).
- **Skipped Entries:**
  - Uppercase hex (e.g. `AAA...`).
  - Algorithm prefixes (e.g. `sha256:...`).
  - Non-hex characters, invalid lengths, tags, temporary files, hidden files.
  - Non-regular files (subdirectories, symlinks, FIFOs, devices, sockets).
- **Accounting Invariant:** Every entry returned by `enumerate_dir` is charged against `limits.max_total_dirents` before name validation or file-type checking. Skipped entries consume budget.

### 3.2 Path Deduplication & Identical Digest Handling
- **Terminal Key Deduplication:** Duplicate terminal keys passed by the caller are deduplicated in order of appearance.
- **Manifest Path Deduplication:** If duplicate dirents appear within a terminal directory, only the first occurrence is processed.
- **Distinct Paths with Identical Digests:** If two distinct object paths (e.g. `repos/app1/manifests/<hex>` and `repos/app2/manifests/<hex>`) contain identical digests, **both paths are opened and read**. Manifest reference extraction must not skip distinct paths, ensuring distinct child layers or manifests are fully recorded.

### 3.3 Parser Behavior & Record-Only References
- For each parsed manifest payload, its own root digest (from the filename) is added to `protected_digests`.
- `parse_manifest_refs(&payload)` is executed to extract direct references via `parsed.all_references()`.
- **Record-Only:** Referenced child manifests (e.g. in multi-arch manifest lists) and blobs are recorded into `protected_digests`. The seam does **not** recursively fetch child manifests or verify payload content hashes against the filename.

### 3.4 Strict Fail-Closed Guarantee
If any terminal directory enumeration, payload opening, payload stream, or JSON parsing encounters an error, the operation returns `Err(StorageError)` immediately. **No partial observation set is returned on failure.**

---

## 4. Concrete Error Handling & Downcasts

`ReadError::Backend` does not carry a structured mode field. The seam inspects its underlying source via typed `downcast_ref::<FsMetadataError>()`. Message text pattern matching is strictly prohibited. Both `translate_terminal_dir_error` and `translate_manifest_payload_error` attach the relevant `ObjectKey` context to all generated `StorageError`s, including unsupported platform and syscall errors.

### 4.1 Terminal Directory Error Mappings

| Underlying Cause | Detection Mechanism | Mapped `StorageError` |
| :--- | :--- | :--- |
| Missing observed terminal directory | `FsDirError::NotFound` | `StorageError::Internal { kind: Io }` |
| Permission denied | `FsDirError::PermissionDenied` | `StorageError::Internal { kind: PermissionDenied }` |
| Resolution rejected (symlink/path boundary) | `FsDirError::ResolutionRejected` | `StorageError::Internal { kind: Io }` |
| Wrong type / not a directory | `FsDirError::NotADirectory` | `StorageError::Internal { kind: CorruptData }` |
| Traversal limit exceeded | `FsDirError::LimitExceeded` | `StorageError::Internal { kind: Backend }` |
| Entry disappeared during traversal | `FsDirError::EntryDisappeared` | `StorageError::Internal { kind: Io }` |
| Unsupported syscall (includes dir key) | `FsDirError::SyscallUnsupported` | `StorageError::Internal { kind: Configuration }` |
| Unsupported platform (includes dir key) | `FsDirError::PlatformUnsupported` | `StorageError::Internal { kind: Configuration }` |
| Runtime missing | `FsDirError::RuntimeMissing` | `StorageError::Internal { kind: Backend }` |
| Task join failure | `FsDirError::TaskJoinFailed` | `StorageError::Internal { kind: Backend }` |

### 4.2 Manifest Payload Error Mappings & Typed Downcasts

| Underlying Cause | Detection Mechanism | Mapped `StorageError` |
| :--- | :--- | :--- |
| Missing observed payload | `ReadError::NotFound` | `StorageError::Internal { kind: Io }` |
| Permission denied on payload | `ReadError::PermissionDenied` | `StorageError::Internal { kind: PermissionDenied }` |
| Stream consumption failure | `std::io::Error` during byte stream reading (not a `ReadError` variant) | `StorageError::Internal { kind: Io }` |
| Non-regular object / wrong type | `ReadError::Backend` downcast to `FsMetadataError::UnsupportedObjectType` | `StorageError::Internal { kind: CorruptData }` |
| Symlink / resolution rejection | `ReadError::Backend` downcast to `FsMetadataError::ResolutionRejected` | `StorageError::Internal { kind: Io }` |
| Unsupported syscall (includes manifest key) | `ReadError::Backend` downcast to `FsMetadataError::SyscallUnsupported` | `StorageError::Internal { kind: Configuration }` |
| Unsupported platform (includes manifest key) | `ReadError::Backend` downcast to `FsMetadataError::PlatformUnsupported` | `StorageError::Internal { kind: Configuration }` |
| Runtime missing | `ReadError::Backend` downcast to `FsMetadataError::RuntimeMissing` | `StorageError::Internal { kind: Backend }` |
| Task join failure | `ReadError::Backend` downcast to `FsMetadataError::TaskJoinFailed` | `StorageError::Internal { kind: Backend }` |
| Underlying IO error source | `ReadError::Backend` downcast to `std::io::Error` | `StorageError::Internal { kind: Io }` |
| Unrelated error type source | `ReadError::Backend` with non-matching downcast (`UnrelatedCustomError`) | `StorageError::Internal { kind: Backend }` |
| Absent source | `ReadError::Backend` with `source == None` | `StorageError::Internal { kind: Backend }` |
| Malformed manifest JSON | `ManifestParseError` from `parse_manifest_refs` | `StorageError::Internal { kind: CorruptData }` |

### 4.3 Forward-Compatibility Fallback Branches
Both error translation functions include wildcard fallback branches (`_ => ...`) for non-exhaustive enum safety. Under the current `storage-fs` error definitions, all existing variants are explicitly mapped and exercised. The wildcard branches represent unexercised defensive fallbacks that can only be reached if new enum variants are added upstream without seam updates.

---

## 5. Resource Accounting & Capacity Limits

### 5.1 Checked Accounting
All increments for counts and logical bytes use checked arithmetic (`checked_add`), failing closed with `StorageErrorKind::Backend` before limit violations or arithmetic overflow occur.

### 5.2 Logical Byte Charging
Logical bytes are charged strictly for newly retained data:
1. Retained terminal keys: `key.as_str().len()`.
2. Retained manifest keys: `key.as_str().len()`.
3. Retained protected digests: `digest.as_str().len()`.

**Duplicate-at-Capacity Rule:** If a digest is already present in `protected_digests`, encountering it again as a child reference does not charge additional bytes or count against `max_total_references`. Re-observation of existing digests succeeds even if capacity is reached.

### 5.3 Excluded Allocations
The following allocations are intentionally excluded from the logical byte budget:
- Caller-owned input slices (`&[ObjectKey]`).
- Internal dedup sets (`HashSet<&str>`).
- Temporary payload stream chunks and manifest JSON buffers before parsing.
- Intermediate `ParsedManifestRefs` structs discarded after populating `protected_digests`.

### 5.4 Sentinel-Byte Payload Ceiling
When `limits.max_payload_size_bytes` is specified, the payload reader wraps the byte stream with `.take(limit + 1)`.
- If the stream terminates within `limit` bytes, reading succeeds.
- An exact boundary (`length == limit`) succeeds.
- If a sentinel byte is read (`length > limit`), reading aborts immediately with `StorageError::Internal { kind: Backend, message: ... }` citing payload size ceiling violation. (Payload ceiling exhaustion is a backend resource bound, not corrupt data).
- No production payload ceiling or production defaults are authorized.

### 5.5 Exhaustive Budget Category Coverage
The test suite deterministically verifies exact-boundary and one-over conditions across all six accounting dimensions:
1. **`max_manifests_read`:** Exact boundary (2 manifests) succeeds; 1-over boundary (limit 1 with 2 files) fails with `Backend`.
2. **`max_terminal_dir_enumerations`:** Exact boundary (2 directories) succeeds; 1-over boundary (limit 1 with 2 directories) fails before the 2nd reader call. Duplicate terminal inputs are deduplicated without consuming enumerations.
3. **`max_total_manifest_entries` (cumulative dirents):** Directory returns 3 entries (1 valid regular, 1 skipped symlink, 1 skipped duplicate name). Exact boundary (3) succeeds; 1-over boundary (limit 2 with 3 entries) fails before opening payloads. Asserts that filtered and duplicate entries consume dirent budget.
4. **`max_total_references`:** Manifest references 4 unique digests (root + config + 2 layers, with 1 duplicate layer). Exact boundary (4) succeeds; 1-over boundary (limit 3 with 4 digests) fails with `Backend`. Duplicate references do not consume reference capacity.
5. **`max_retained_logical_bytes`:** Derived directly from fixture key/digest lengths and independently asserts literal lengths: `dir` ("repos/app/manifests": 5 + 1 + 3 + 1 + 9 = 19 bytes), `manifest_key` (19 + 1 + 64 = 84 bytes), `root_digest` (7 + 64 = 71 bytes). Exact total boundary (174 bytes) succeeds even with duplicate terminal directory inputs and duplicate manifest dirents, proving deduplication consumes zero extra retained bytes. One-over rejection budget (173 bytes) fails during retain. Pre-call manifest key rejection budget (102 bytes) fails when charging manifest key (19 + 84 = 103 > 102). Pre-call dir key rejection budget (18 bytes) fails when charging terminal dir key (19 > 18).
6. **Retained-Byte Arithmetic Overflow:** Tracker method `charge_retained_bytes` is directly exercised near `usize::MAX`, confirming checked overflow returns `StorageErrorKind::Backend`. Unreachable counter-overflow branches (such as `usize::MAX` entry counts impossible on physical media) are documented as unexercised defensive checks.

---

## 6. Test Inventory & Execution Results

### 6.1 Focused Seam Test Suite (18 Unique Tests)

All 18 focused seam tests pass deterministically on Linux:

| Test Name | Category | Status |
| :--- | :--- | :--- |
| `test_manifest_refs_filename_sha256_and_sha512_accepted_and_roots_recorded` | Policy & Roots | PASS |
| `test_manifest_refs_uppercase_hex_and_invalid_names_skipped` | Policy | PASS |
| `test_manifest_refs_nonregular_entries_skipped` | Dirent Filter | PASS |
| `test_manifest_refs_duplicate_terminal_inputs_and_dirents_deduplicated` | Deduplication | PASS |
| `test_manifest_refs_identical_digests_at_distinct_paths_both_read` | Distinct Paths | PASS |
| `test_manifest_refs_parser_behavior_and_record_only_child_references` | Parser Behavior | PASS |
| `test_manifest_refs_missing_observed_terminal_or_payload_fails_closed` | Error Mapping | PASS |
| `test_manifest_refs_terminal_error_mappings` | Terminal Errors | PASS |
| `test_manifest_refs_typed_error_downcasts_and_fallbacks` | Downcasts & Fallbacks | PASS |
| `test_manifest_refs_stream_io_and_parse_failures` | Stream & Parse | PASS |
| `test_manifest_refs_failure_after_earlier_success_asserts_no_subsequent_reads` | Fail Closed | PASS |
| `test_manifest_refs_exact_and_one_over_budgets` | Budget Boundaries (6 Categories) | PASS |
| `test_manifest_refs_duplicate_at_capacity_succeeds` | Duplicate at Capacity | PASS |
| `test_manifest_refs_payload_ceiling_exact_and_sentinel_oversize` | Payload Ceiling | PASS |
| `test_manifest_refs_linux_real_fs_same_reader_end_to_end` | Real FS End-to-End | PASS |
| `test_manifest_refs_linux_real_fs_symlink_entries_skipped` | Real FS Symlinks | PASS |
| `test_manifest_refs_linux_real_fs_pinned_root_across_replacement` | Real FS Pinned Root | PASS |
| `test_manifest_refs_linux_real_fs_permission_denied_restoration_guard` | Real FS Permission | PASS (explicit unprivileged execution) |

### 6.2 Environment-Appropriate Permission Testing
`test_manifest_refs_linux_real_fs_permission_denied_restoration_guard` is marked with `#[ignore = "requires unprivileged user environment where chmod 0o000 denies filesystem access"]` by default. Under root or containerized environments where `geteuid() == 0`, Linux bypasses standard Unix permissions and allows opening `0o000` files/directories, which would cause an unprivileged test to panic or fail assertions.

The test is run separately in an unprivileged environment (UID != 0):
- Actively verifies that `chmod 0o000` denies directory enumeration with `EACCES` and maps to `StorageErrorKind::PermissionDenied`.
- Uses a scoped RAII cleanup guard to restore file permissions.
- On cleanup, asserts that the restored permission mode matches the original permission mode (`restored_mode == orig_perms.mode()`), rather than merely asserting metadata lookup succeeds.

### 6.3 Test Execution Accounting & Traceability

1. **Focused Seam Default Suite:**
   - Command: `cargo test --lib storage::fs::manifest_refs_seam`
   - Result: 17 passed; 0 failed; 1 ignored; 0 measured; 730 filtered out.
2. **Focused Seam Explicit Permission Suite:**
   - Command: `cargo test --lib storage::fs::manifest_refs_seam -- test_manifest_refs_linux_real_fs_permission_denied_restoration_guard --ignored`
   - Result: 1 passed; 0 failed; 0 ignored; 0 measured; 747 filtered out.
   - **Total Unique Passing Seam Tests:** 18.
3. **Directory Discovery Seam Suite:**
   - Command: `cargo test --lib storage::fs::repo_discovery`
   - Result: 25 passed; 0 failed; 1 ignored; 0 measured; 722 filtered out.
4. **Historical Broader Regression Suites (Preserved from Session `session-20260912-0220`):**
   - Repository Baseline: `registry-rust` at commit `3bbe006148c83add6acfb2f7e0c5df9a21d38b7e`.
   - `cargo test blob_gc`: 16 library tests passed + 5 integration tests passed (21 total passed).
   - `cargo test --test manifest_lifecycle_tests`: 3 integration tests passed.
   - Preserved without re-execution to avoid misrepresenting historical baseline evidence as newly executed.

---

## 7. Systemic Realities & Safety Considerations

1. **Experimental Scope Only:** This implementation is not wired into production GC and must not be used for production deletion decisions.
2. **GC Safety Defense-in-Depth:** An omitted manifest reference does not directly or immediately cause blob deletion. Production safety relies on independent safeguards: reachability indices, upload/manifest pins, repository memberships, minimum blob age thresholds, quarantine periods, and lease revalidation.
3. **No Snapshot Isolation or Global GC Transaction:** Filesystem operations do not provide snapshot isolation. Pinned directory file descriptors guarantee containment within the directory tree, but concurrent unlinks, renames, or writes can occur during traversal.
4. **No Content-Digest Verification:** Filename digests are assumed to represent payload identity for reference tracking. The seam does not calculate or verify SHA-256/SHA-512 hashes over raw manifest payloads.
5. **No Total Heap Bound:** While logical retained bytes are strictly accounted, intermediate allocations (deserialization buffers, AST nodes) scale temporarily with manifest payload sizes.
6. **Procfs & Linux Specifics:** Contained path resolution via `openat2` relies on Linux `RESOLVE_BENEATH` and `/proc` file descriptor re-opening. Behavior on non-Linux operating systems remains unverified.
7. **Canonical Gates:** Quality gates O-03, O-04, O-05, O-06, O-13, O-15, O-16, and D-06 remain OPEN.
