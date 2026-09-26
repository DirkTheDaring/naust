> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. References `list_tag_files`, which no longer exists; contained tag listing landed (`f1d6d9c`) and later moved onto `tag_domain` (`32c42c6`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Architectural Record: Contained Filesystem Tag Listing Test Seam (Corrected Coverage & Documentation)

**Repository:** `registry-rust`
**Target Path:** `docs/architecture/filesystem-tag-listing-contained-seam.md`
**Authoritative Baselines:**
- `registry-rust` HEAD: `6d13cd983e10c15fa806d2d23385ae5752fe6362`
  - Latest Commit: `test(storage): characterize filesystem tag listing semantics`
- `storage-layer-rust` HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (strictly read-only)

**Reviewed Design Reference:**
- Document: `docs/architecture/filesystem-tag-listing-contained-integration-design.md`
- Archive: `~/devel/rust/manifest-read-review-evidence/session-20260912-1145/filesystem-tag-listing-contained-integration-design.tar.gz`
  - SHA-256: `5b6229f35fc3781ab69786c88e17fc3c2e4abc6f6d21c47e32e6cf1f03dbf930` (25553 bytes)

**Authorization Boundary:**
This implementation fulfills the reviewed test-only integration seam.
- **Test-Only Scope**: Implemented entirely under `#[cfg(test)]` in [`src/storage/fs/tag_listing.rs`](src/storage/fs/tag_listing.rs) and declared under `#[cfg(test)]` in [`src/storage/fs.rs`](src/storage/fs.rs).
- **Production Unchanged**: Production `FsStorage::list_tags`, `FsStorage::list_tags_page`, `list_tag_files`, and `delete_manifest` are completely untouched.
- **Production Policy Decisions Remain Open**: Passing seam tests does not authorize production cutover.
- **Canonical Quality Gates**: All eight quality gates remain explicitly **OPEN**: `O-03`, `O-04`, `O-05`, `O-06`, `O-13`, `O-15`, `O-16`, `D-06`.

---

## 1. Executive Summary & Implemented Scope

This document records the completed implementation, verified test coverage, and refined architectural specifications of the test-only contained filesystem tag-listing seam. The seam integrates descriptor-relative directory enumeration from `storage-layer-rust` (`storage_fs::FsMetadataReader`) with contained tag payload acquisition, exercising strict kernel containment flags (`openat2` with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`).

The seam provides two pure async entry points:
1. `contained_list_tags_seam`: Point-in-time name-only tag enumeration.
2. `contained_list_tags_page_seam`: Paged tag listing returning sorted `(tag_name, Digest)` pairs and raw lexical continuation tokens.

Both operations are decoupled from production storage structs through narrow trait contracts and require explicit caller-specified limits, eliminating hidden ambient defaults.

---

## 2. Implemented Signatures & Ownership Boundaries

### 2.1 The `TagDirEnumerator` Trait
To decouple listing logic from concrete filesystem handles and facilitate deterministic mocking of directory enumeration, limits, and transient races:

```rust
#[async_trait]
pub(crate) trait TagDirEnumerator: Send + Sync {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: storage_fs::DirEnumerationLimits,
    ) -> Result<Vec<storage_fs::DirEntry>, storage_fs::FsDirError>;
}
```

A blanket implementation is provided for `storage_fs::FsMetadataReader`, allowing direct integration with the real contained reader without adapter boilerplate.

### 2.2 Seam Functions

```rust
pub(crate) async fn contained_list_tags_seam<D>(
    dir_enumerator: &D,
    repository: &str,
    repo_probe_limits: storage_fs::DirEnumerationLimits,
    tags_dir_limits: storage_fs::DirEnumerationLimits,
) -> Result<Vec<String>, StorageError>
where
    D: TagDirEnumerator + ?Sized,
```

```rust
pub(crate) async fn contained_list_tags_page_seam<D, R>(
    dir_enumerator: &D,
    payload_reader: &R,
    repository: &str,
    cursor: Option<&str>,
    page_limit: usize,
    repo_probe_limits: storage_fs::DirEnumerationLimits,
    tags_dir_limits: storage_fs::DirEnumerationLimits,
    payload_limits: super::tag_read::TagReadLimits,
) -> Result<(Vec<(String, Digest)>, Option<String>), StorageError>
where
    D: TagDirEnumerator + ?Sized,
    R: ObjectPayloadReader + ?Sized,
```

### 2.3 Ownership and Borrowing
- Both functions borrow enumerator and reader references (`&D`, `&R`) and accept string slices for repository and cursor.
- Limits are passed by value (`DirEnumerationLimits` is `Copy`, `TagReadLimits` is `Clone`).
- Return values own their data (`Vec<String>` and `(Vec<(String, Digest)>, Option<String>)`), decoupling result processing from backend lifetime.

---

## 3. Behavioral Contracts & Divergence from Current Production

The contained seam implements the policies designed and reviewed in `filesystem-tag-listing-contained-integration-design.md`. The table below highlights key divergences between current production and the new contained seam:

| Feature / Behavior | Current Production (`FsStorage`) | Contained Seam (`tag_listing.rs`) | Rationale / Classification |
| :--- | :--- | :--- | :--- |
| **Path Traversal & Repo Validation** | Ambient path join; allows `../` traversal and arbitrary repo names without upfront validation. Paths with embedded NUL are rejected by standard library conversions with `ErrorKind::InvalidInput` prior to any OS filesystem syscall. | Upfront structural validation rejects empty input, leading/trailing slashes, repeated slashes, `..` or `.` components, backslashes, NUL bytes, and ASCII controls with `StorageError::InvalidRepoName`. | **Proposed change — requires approval.** Eliminates path traversal outside root and establishes clean repository path grammar before any filesystem access. |
| **Containment Guarantee** | None; kernel ambient path resolution traverses symlinks and escapes root. | Pinned root descriptor (`openat2` with `RESOLVE_BENEATH \| RESOLVE_NO_SYMLINKS \| RESOLVE_NO_MAGICLINKS`). | **Proposed change — requires approval.** Guarantees kernel-enforced descriptor containment. |
| **Missing Repo: Name-Only** | Checks `metadata("repos/<repo>")`; returns `StorageError::NotFound`. | Enumerates `repos/<repo>/tags`. Only on `NotFound`, probes `repos/<repo>`. If repo probe returns `NotFound`, returns `StorageError::NotFound`. | **Existing behavior proposed for preservation.** Preserves caller expectation without redundant probe when `tags/` exists. |
| **Missing Repo: Paged** | Queries `tags/` directly; returns `Ok(([], None))`. | Enumerates `repos/<repo>/tags`. Only on `NotFound`, probes `repos/<repo>`. If repo probe returns `NotFound`, returns empty terminal page `Ok(([], None))`. | **Existing behavior proposed for preservation.** Retains compatibility with legacy paged caller contract. |
| **Missing `tags/` on Existing Repo** | Returns empty success `Ok([])` or `Ok(([], None))`. | Probe confirms repo exists; returns empty success `Ok([])` or `Ok(([], None))`. | **Existing behavior proposed for preservation.** Idempotent empty tag set. |
| **Payload Opens in Name-Only** | None. Returns directory entry names. | None. Returns regular file candidate names sorted. | **Existing behavior proposed for preservation.** Fast name enumeration. |
| **Candidate Filtering** | Skips `.` prefixes and non-UTF-8. Includes subdirectories and symlinks as tags. | Skips `.` prefixes and non-UTF-8. Retains **only** entries observed as `DirEntryType::Regular`. | **Proposed change — requires approval.** Disallows subdirectories/symlinks as valid tag names. |
| **Candidate Disappearance Race** | `tokio::fs::read_to_string` fails with `NotFound`; ignored silently in paged listing. | Payload acquisition returns `ReadError::NotFound`; omitted silently from page. | **Existing behavior proposed for preservation.** Resilient to concurrently deleted tags. |
| **Candidate Replaced by Directory** | `tokio::fs::read_to_string` encounters `EISDIR`; fails with OS error; silently omitted from page. | Contained open detects `S_IFDIR` (`FsMetadataError::UnsupportedObjectType`); translated to `StorageError::io(...)` (`StorageErrorKind::Io`), failing closed. | **Proposed change — requires approval.** Prevents silent masking of adversarial or accidental directory replacement. |
| **Candidate Replaced by Symlink** | Ambient `tokio::fs::read_to_string` follows symlink; if target is readable and contains valid digest text, tag is successfully parsed and yielded. | Contained open with `RESOLVE_NO_SYMLINKS` rejects symlinks (`FsMetadataError::ResolutionRejected`); translated to `StorageError::io(...)` (`StorageErrorKind::Io`), failing closed. | **Proposed change — requires approval.** Guarantees symlinks are never followed and fails closed on substitution. |
| **Payload Read Errors & Stream I/O** | `tokio::fs::read_to_string` failure (permission denied, I/O error, truncation) is silently omitted from page (`if let Ok(...)`). | Non-NotFound acquisition errors (e.g. permission denied) and stream I/O errors propagate as `StorageError::io(...)` (`StorageErrorKind::Io`) or `StorageErrorKind::Backend`, failing closed. | **Proposed change — requires approval.** Surfaces I/O and permission errors rather than silently suppressing corrupted or inaccessible candidates. |
| **Empty / Malformed Digest Text & Stream EOF** | Silently omitted without error in paged listing. | Clean stream EOF parses received bytes; empty or malformed digest text is omitted under proposed omission policy. Metadata length is not enforced as an expected stream length. | **Existing behavior proposed for preservation.** Tolerates corrupt tag payload files without halting listing. |
| **Invalid UTF-8 in Tag Payload** | Silently omitted without error (`if let Ok(...)`). | Fails closed with `StorageError::io(...)` (`StorageErrorKind::Io`). | **Proposed change — requires approval.** Experimental seam policy flags payload encoding corruption. |
| **Zero Page Limit (`page_limit == 0`)** | Enumerates directory and reads/parses all candidate tag files before applying pagination slicing; does not short-circuit before directory access. | Retains repository validation, directory enumeration, and probing (preserving error precedence), but avoids candidate payload opens when `page_limit == 0`, returning `Ok(([], None))`. | **Proposed change — requires approval.** Avoids wasteful payload opens at zero page size while preserving directory validation and probe error precedence. |
| **Payload Byte Limit Overflow** | No limits in production listing. | Checked limit arithmetic; `checked_add(1)` overflow or stream byte excess fails closed with `StorageError::corrupt_data(...)` (`StorageErrorKind::CorruptData`). | **Proposed change — requires approval.** Bounded memory defense-in-depth. |

---

## 4. Missing-Directory Probe Costs and Compatibility

### 4.1 Probe Sequencing
In standard OCI registry workloads, `tags/` exists for any repository that has had tags pushed. The seam implements deferred probing:
1. **Fast Path**: Enumerate `repos/<repo>/tags` with `tags_dir_limits`. If successful, candidate processing proceeds immediately. Zero probe overhead.
2. **Missing Path**: If and only if `tags/` returns `FsDirError::NotFound`, the seam issues an existence probe against `repos/<repo>` using `repo_probe_limits`.

### 4.2 Budget Allocation and Cost Implications
- `enumerate_dir` against `repos/<repo>` enumerates only the immediate child entries of the repository directory (`repos/<repo>`), such as `manifests/`, `revisions/`, or other direct entries.
- Only immediate repository-directory entries consume probe enumeration budgets (`max_entries` and `max_total_name_bytes`). Manifest files or revisions nested inside subdirectories (e.g. `repos/<repo>/manifests/<digest>`) do **not** each count toward the probe budget; only the immediate subdirectory entry (`manifests`) is counted during enumeration of `repos/<repo>`.
- Limits remain explicit experimental caller inputs; production default budgets remain undecided. Unsupported recommended minimums (such as `max_entries >= 100` and `max_total_name_bytes >= 10240`) are not prescribed.
- Probing is a separate, point-in-time directory observation, not an atomic existence guarantee. Because existence probing and tag enumeration are distinct operations, concurrent repository creation or deletion between probing and enumeration remains possible.
- Contained directory probing involves a blocking `openat2` and `getdents64` on Tokio's blocking threadpool. While amortized on hit, missing repository lookups will pay the latency of two directory enumeration attempts.

---

## 5. Distinct Error Mappings: Directory vs. Payload

The seam strictly separates directory enumeration errors from individual tag payload read errors using the canonical `StorageError` and `StorageErrorKind` taxonomy:

### 5.1 Directory Error Mapping (`translate_tag_dir_error`)
Translates `FsDirError` into `StorageError`:
- `FsDirError::NotFound`: Handled directly by caller logic (missing repo vs. missing tags); unexpected directory disappearance maps to `StorageError::io(...)` (`StorageErrorKind::Io`).
- `FsDirError::NotADirectory`: Mapped to `StorageError::corrupt_data(...)` (`StorageErrorKind::CorruptData`) (repository or tags component is a file instead of directory).
- `FsDirError::PermissionDenied`: Mapped to `StorageError::permission_denied(...)` (`StorageErrorKind::PermissionDenied`).
- `FsDirError::ResolutionRejected`: Mapped to `StorageError::io(...)` (`StorageErrorKind::Io`) (path resolution aborted due to symlink or root escape).
- `FsDirError::SyscallUnsupported`: Mapped to `StorageError::configuration(...)` (`StorageErrorKind::Configuration`) (`ENOSYS` on legacy kernel).
- `FsDirError::PlatformUnsupported`: Mapped to `StorageError::configuration(...)` (`StorageErrorKind::Configuration`) (non-Linux OS).
- `FsDirError::LimitExceeded`: Mapped to `StorageError::backend(...)` (`StorageErrorKind::Backend`) (enumeration budget exhausted).
- `FsDirError::EntryDisappeared`: Mapped to `StorageError::io(...)` (`StorageErrorKind::Io`) (concurrent modification during `readdir`).
- `FsDirError::Io`: Mapped to `StorageError::io(...)` (`StorageErrorKind::Io`).
- `FsDirError::RuntimeMissing` / `TaskJoinFailed`: Mapped to `StorageError::backend(...)` (`StorageErrorKind::Backend`).

### 5.2 Payload Error Mapping (`translate_payload_read_error`)
Translates `storage_core::ReadError` into `StorageError`:
- `ReadError::NotFound`: Omitted silently during paged iteration (normal concurrent tag deletion).
- `ReadError::PermissionDenied`: Mapped to `StorageError::io(...)` (`StorageErrorKind::Io`). This mapping is intentionally distinct from directory enumeration `PermissionDenied` (which maps to `StorageErrorKind::PermissionDenied`). Unlike legacy production paged listing which silently suppressed/omitted files on permission or read errors, the seam propagates this error, failing closed.
- `ReadError::Backend`: Unpacked source inspected:
  - `FsMetadataError::RuntimeMissing`: Mapped to `StorageError::backend(...)` (`StorageErrorKind::Backend`).
  - `FsMetadataError::TaskJoinFailed`: Mapped to `StorageError::backend(...)` (`StorageErrorKind::Backend`).
  - `FsMetadataError::SyscallUnsupported`: Mapped to `StorageError::configuration(...)` (`StorageErrorKind::Configuration`).
  - `FsMetadataError::UnsupportedObjectType`: Mapped to `StorageError::io(...)` (`StorageErrorKind::Io`) (candidate was replaced by a non-regular object such as a directory or socket).
  - `FsMetadataError::ResolutionRejected`: Mapped to `StorageError::io(...)` (`StorageErrorKind::Io`) (candidate was replaced by a symlink).
  - General backend error: Mapped to `StorageError::io(...)` (`StorageErrorKind::Io`).
- **Payload Stream Draining (`drain_tag_stream`)**: Any underlying `std::io::Error` during stream consumption propagates as `StorageError::io(...)` (`StorageErrorKind::Io`). Clean EOF alone does not prove truncation; metadata length is not enforced as an expected stream length. Valid digest text parses and succeeds; empty or malformed digest text is omitted under the proposed omission policy. Invalid UTF-8 bytes fail closed with `StorageErrorKind::Io`.

---

## 6. Full-Scan Allocation, Concurrency, and Pagination Limits

1. **Full-Scan Nature**: Filesystem directory enumeration beneath POSIX/Linux cannot perform index seeks. Both name-only and paged listings must read all entries in `repos/<repo>/tags` into memory before sorting.
2. **Page Boundaries Do Not Bound Filesystem Work**: Requesting `page_limit == 1` still enumerates the entire `tags/` directory. However, candidate payload reads **are** bounded: payloads are only fetched for up to `page_limit + 1` entries starting from `cursor`.
3. **Memory Limits**: Bounded by `tags_dir_limits.max_total_name_bytes` and `max_entries`. A repository with 100,000 tags will consume memory proportional to total tag name length.
4. **No Read/Write Coherence or Snapshot Isolation**:
   - Directory enumeration and payload reads are separate, non-atomic observations.
   - A tag observed during directory enumeration may be modified or deleted before its payload is opened.
   - Concurrent additions or deletions between page requests may cause items to appear twice or be skipped across page boundaries (standard lexical cursor semantics).
5. **Unbounded Payload Buffering When Limits Are None**: When `payload_limits.max_payload_bytes` is `None`, tag payload streaming reads until EOF into memory. Production deployments must provide bounded limits to prevent memory exhaustion from oversized tag files.

---

## 7. OS and Environment Dependencies

1. **Kernel openat2 Support**: Requires Linux kernel >= 5.6 for `openat2` and `RESOLVE_BENEATH`.
2. **Procfs Requirement**: Pinned payload reopening via `openat2` relies on `/proc/self/fd/<n>`. A mounted, accessible, and unmasked `/proc` is strictly required. Containers with masked procfs will fail closed with `StorageErrorKind::Configuration` or `StorageErrorKind::Io`.
3. **No Mount or Hard-Link Isolation Guarantee**: `RESOLVE_BENEATH` prevents symlink and path traversal escapes, but does not isolate hard links created inside the directory pointing outside, nor does it isolate separate mount points beneath the root unless `RESOLVE_NO_XDEV` is enforced.
4. **Non-Linux Status**: On macOS, BSD, or Windows, the seam compiles (under cfg/mocking) but real filesystem execution returns `FsDirError::PlatformUnsupported` -> `StorageErrorKind::Configuration`.

---

## 8. Caller-Hardening Prerequisites Before Production Promotion

Passing seam tests confirms unit-level conformance but **does not authorize production cutover**. Before replacing production listing, the following prerequisites must be met:
1. **Caller Audit**:
   - Audit `manifest_lifecycle` (e.g. `delete_manifest`, `evict_proxy_cached_entry`): verify handling of `StorageError::NotFound` vs empty pages.
   - Audit `blob_ref_index::sync_repo_tags`: verify tolerance for concurrent tag omissions and ensure dirty markers are set appropriately.
   - Audit GC discovery (`gc_contained_discovery`): distinguish between normal tag eviction, unreferenced blob sweeping, and journal crash recovery. Do **not** assume an omitted tag proves successful GC deletion.
2. **Standardized Limits Configuration**: Establish configuration keys for `tags_dir_limits` (max entries and name bytes) and `repo_probe_limits` across all runtime profiles. Production defaults remain undecided.
3. **Formal Policy Alignment**: Resolve open architectural questions regarding whether invalid UTF-8 payload bytes should omit or fail closed in production, and whether non-NotFound payload read errors should propagate or be suppressed as in legacy paged listing.

---

## 9. Test Evidence & Verified Coverage

The test suite in `src/storage/fs/tag_listing.rs` comprises 27 total test functions (26 active unignored tests and 1 explicitly ignored unprivileged permission test):

### 9.1 Unit & Contract Tests
1. `test_name_only_zero_payload_opens`: Verifies name-only listing makes zero payload opens.
2. `test_paged_valid_sha256_sha512_whitespace_padding`: Verifies SHA-256, SHA-512, whitespace trimming, and sorted paged traversal.
3. `test_missing_repo_vs_missing_tags_directory`: Verifies `StorageError::NotFound` for missing repo in name-only, empty terminal page in paged listing, and empty success when `tags/` is missing on an existing repository.
4. `test_mock_independent_probe_and_tags_budgets_translation`: Translation test verifying mock routing of independent limits and `StorageErrorKind::Backend` on simulated probe exhaustion.
5. `test_mock_directory_limit_exceeded_error_translation`: Translation test verifying injected `MaxEntries` and `MaxTotalNameBytes` directory errors map to `StorageErrorKind::Backend`.
6. `test_payload_byte_exact_boundary`: Exact boundary test verifying limit of 71 bytes succeeds while 70 bytes fails with `StorageErrorKind::CorruptData`.
7. `test_payload_limit_none_and_checked_overflow_u64_max`: Verifies `None` limit succeeds and `Some(u64::MAX)` checked-add overflow fails closed with `StorageErrorKind::CorruptData`.
8. `test_invalid_repository_input_causes_zero_reader_calls`: Verifies invalid repos (traversals, control characters, leading/trailing slashes, NUL bytes) cause zero reader invocations and return `StorageError::InvalidRepoName`.
9. `test_dotfiles_non_utf8_and_non_regular_entries_skipped`: Verifies non-regular entries (subdirectories, symlinks, FIFOs), dotfiles, and non-UTF-8 entries are filtered.
10. `test_symlinks_path_resolution_rejected_vs_child_entries_skipped`: Verifies path-level symlinks fail closed with `StorageErrorKind::Io` while child symlinks in `tags/` are skipped.
11. `test_candidate_observed_regular_replaced_before_acquisition`: Injected translation test verifying replacement by directory, symlink, or permission change maps to `StorageErrorKind::Io`.
12. `test_dir_entry_disappeared_propagates_payload_not_found_omits`: Verifies `EntryDisappeared` during directory read propagates while payload `NotFound` omits candidate cleanly.
13. `test_permission_denied_directory_and_payload_mappings`: Portable mock test verifying directory `PermissionDenied` maps to `StorageErrorKind::PermissionDenied`, platform unsupported maps to `StorageErrorKind::Configuration`, and payload `PermissionDenied` maps to `StorageErrorKind::Io`.
14. `test_zero_page_error_precedence_and_zero_payload_opens`: Verifies `page_limit == 0` still evaluates repo validation and directory permissions before returning empty page with 0 payload opens.
15. `test_raw_unusual_cursors_terminal_missing_zero_size_usize_max`: Verifies non-existent, emoji, oversized, and terminal cursors with `usize::MAX` limit avoid panic or overflow.
16. `test_deterministic_inter_page_mutations_insertion_deletion_modification`: Verifies deterministic mutations between page requests.
17. `test_stream_partial_io_error_vs_clean_eof_valid_digest_vs_malformed_omission`: Verifies partial stream I/O errors and immediate I/O errors fail with `StorageErrorKind::Io`, while clean EOF with malformed text is omitted and invalid UTF-8 fails with `StorageErrorKind::Io`.
18. `test_metadata_length_not_enforced_as_stream_length`: Verifies stream parser uses actual bytes read, ignoring discrepancies with metadata content length.

### 9.2 Real Filesystem Enforcement & Containment Evidence
19. `test_real_directory_boundaries_and_filtered_consumption`:
    - **Real Entry Count Boundary**: Exactly $N=3$ directory entries succeeds; $N-1=2$ fails with `StorageErrorKind::Backend`.
    - **Real Total Name Bytes Boundary**: Exactly $B=6$ filename bytes succeeds; $B-1=5$ fails with `StorageErrorKind::Backend`.
    - **Filtered Budget Consumption**: Non-regular entries (subdirectory, dotfile, symlink) consume directory enumeration budget; limit 5 with 6 entries fails with `StorageErrorKind::Backend`, while limit 6 succeeds and filters them out.
    - **Dual Seam Propagation**: Both name-only and paged operations propagate real enumeration exhaustion.
20. `test_real_repository_probe_exhaustion_and_exact_boundary`:
    - Real repository with `tags/` absent and 3 immediate child entries (`manifests/`, `revisions/`, `metadata_file`).
    - With probe limit 2, both name-only and paged listings fail with `StorageErrorKind::Backend` (not `NotFound` or empty success).
    - With probe limit 3 (exact boundary), the existence probe succeeds, and because `tags/` is absent on an existing repository, both operations succeed with empty results: `Ok([])` for name-only, `Ok(([], None))` for paged.
    - With `tags/` present, tiny probe limit is not exercised, proving tags-first contract.
21. `test_real_filesystem_contained_listing_and_root_pinning`:
    - Tests root renaming and replacement in a test-owned parent directory.
    - Original root is opened with `FsMetadataReader::open` and then renamed.
    - A replacement root is created at the original pathname with different tag names and digests.
    - Verifies both name-only listing and paged payload reads continue using the pinned original descriptor with shared reader identity (`&*reader`).
22. `test_real_filesystem_candidate_replaced_with_symlink`:
    - Uses deterministic hook `ReplacingDirEnumerator`.
    - Candidate regular file is replaced with a symlink pointing to a controlled external fixture in a test-owned temporary directory (no `/etc/passwd` reference).
    - Contained payload acquisition fails closed with `StorageErrorKind::Io` (`FsMetadataError::ResolutionRejected`).
23. `test_real_filesystem_candidate_replaced_with_directory`:
    - Candidate regular file is replaced with a directory before payload acquisition.
    - Contained payload acquisition fails closed with `StorageErrorKind::Io` (`FsMetadataError::UnsupportedObjectType` for `S_IFDIR`).
24. `test_real_filesystem_candidate_replaced_with_unix_socket`:
    - Candidate regular file is replaced with a Unix domain socket (`UnixListener::bind`) before payload acquisition, avoiding blocking concerns.
    - Contained payload acquisition fails closed with `StorageErrorKind::Io` (`FsMetadataError::UnsupportedObjectType` for `S_IFSOCK`).

### 9.3 Comprehensive Error Mapping Coverage
25. `test_directory_error_mappings_complete`:
    - Tests `NotADirectory` -> `StorageErrorKind::CorruptData`.
    - Tests `SyscallUnsupported` -> `StorageErrorKind::Configuration`.
    - Tests `Io` -> `StorageErrorKind::Io`.
    - Tests `RuntimeMissing` using a real `tokio::runtime::TryCurrentError` -> `StorageErrorKind::Backend`.
    - Tests `TaskJoinFailed` using a real cancelled `tokio::task::JoinError` -> `StorageErrorKind::Backend`.
26. `test_payload_error_mappings_complete_and_distinct_from_directory`:
    - Proves payload `PermissionDenied` maps to `StorageErrorKind::Io`, distinct from directory `PermissionDenied` which maps to `StorageErrorKind::PermissionDenied`.
    - Tests payload `Backend` with `RuntimeMissing` -> `StorageErrorKind::Backend`.
    - Tests payload `Backend` with `TaskJoinFailed` -> `StorageErrorKind::Backend`.
    - Tests payload `Backend` with `SyscallUnsupported` -> `StorageErrorKind::Configuration`.
    - Tests payload `Backend` generic -> `StorageErrorKind::Io`.

### 9.4 Portable Permission Testing
27. `test_real_filesystem_permission_denied_unprivileged`:
    - Marked with `#[ignore = "requires unprivileged user environment where chmod 0o000 denies filesystem access"]`.
    - Kept ignored so that the default seam test suite executes portably across privilege levels (including root).
    - When executed explicitly under unprivileged permissions (`cargo test -- --ignored`), asserts genuine non-root execution, makes directory `chmod 0o000`, verifies `StorageErrorKind::PermissionDenied`, and restores permissions via RAII `PermGuard`.

### 9.5 Remaining Untested Behavior
1. **Non-Linux Kernel Execution**: Execution on macOS, Windows, or BSD (where `openat2` is unsupported) cannot be tested live on this Linux runner; it is covered via mocked `PlatformUnsupported` translation tests.
2. **Real Kernel ENOSYS**: The host kernel supports `openat2` (Linux >= 5.6); `ENOSYS` is verified via injected error translation.
3. **Subprocess FIFO Timeout**: Candidate replacement with a FIFO requiring bounded subprocess timeout was intentionally replaced with a Unix domain socket fixture (`UnixListener::bind`), which exercises non-regular object rejection (`S_IFSOCK`) without risk of test hang.
4. **Masked Procfs**: Container environments with `/proc` unmounted or masked cannot be simulated safely without root mount namespace capabilities; failure closed behavior is covered via unit error translation.
