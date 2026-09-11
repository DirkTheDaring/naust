# Filesystem CAS Listing Integration Assessment & Compatibility Record

**Repository:** `registry-rust`
**Scope:** Test-only integration seam evaluating `storage-fs` directory enumeration (`FsMetadataReader::enumerate_dir`) against `registry-rust` CAS listing requirements, GC candidate pagination, error taxonomy, and containment boundaries.
**Slice:** Bounded Slice 2B (Test-Only Listing Seam). Production listing is **not** cut over; `storage-layer-rust` remains read-only.

---

## 1. Actual Call Chain and Ownership Boundaries

The integration seam establishes a strict architectural boundary between registry domain rules and storage-layer descriptor containment:

```text
[GC Orchestration Layer]
  src/blob_gc/mod.rs (run_blob_gc, plan_blob_gc, verify_or_estimate)
        |
        v
[Cursor Tracking & Cycle Detection]
  src/blob_gc/traverser.rs: CasBlobTraverser<'a>
    - Controls next_batch() loop across paginated batches
    - Detects cycles (GcPaginationError::CursorCycle) and repeated cursors (GcPaginationError::RepeatedCursor)
    - Receives batches of GcBlobCandidate records
        |
        v
[Port Definition Layer]
  src/storage/ports/mod.rs: GcStoragePort::list_cas_blobs_page
    - Public signature: cursor: Option<&GcCursor>, limit: usize -> Result<GcBlobPage, StorageError>
    - Candidate model: GcBlobCandidate { digest, size, last_modified, version }
        |
        +---> Production Filesystem Path (UNTOUCHED):
        |       src/storage/fs.rs: FsStorage::list_cas_blobs_page
        |       - Pathname-based iteration (self.root.join("blobs").join("sha256"))
        |       - Uncontained tokio::fs::read_dir and tokio::fs::metadata calls
        |
        +---> Test-Only Integration Seam (THIS SLICE):
                src/storage/fs/listing_seam.rs: list_cas_blobs_page_seam
                - Consumes CasDirEnumerator (e.g. FsMetadataReader or RecordingFakeDirEnumerator)
                - Operates over pinned root descriptor with Linux openat2 containment flags:
                  RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS
                - Yields IncompleteGcBlobPage { items: Vec<IncompleteGcCandidate>, next_cursor }
                - Explicitly documents and labels candidate metadata gap
```

### Separation of Responsibilities

| Responsibility Area | Owning Component | Specific Rules & Contracts |
|---|---|---|
| **CAS Namespace & Layout** | `registry-rust` (`listing_seam`) | CAS root `blobs/sha256`; 2-character lowercase hexadecimal shard subdirectories (`00`..`ff`); 64-character lowercase hexadecimal blob files matching shard prefix. |
| **Digest Parsing & Normalization** | `registry-rust` (`listing_seam`) | Constructs and validates `sha256:<64-hex>` via `Digest::parse`. Rejects non-hex, malformed, or mismatching entries. |
| **Cursor Progression & Clamping** | `registry-rust` (`listing_seam`) | Public limit clamped to `limit.min(1000).max(1)`. Lexical cursor comparison `digest_str <= cursor`. Exact-full-page sets `next_cursor = Some(GcCursor(digest_str))`. |
| **Proposed Error Classification** | `registry-rust` (`listing_seam`) | Proposes mapping typed `FsDirError` to registry `StorageError` taxonomy (`CorruptData`, `Io`, `Backend`, `PermissionDenied`, `Configuration`). All mappings are test-seam proposals requiring a deliberate compatibility assessment before production cutover. |
| **Descriptor Containment** | `storage-fs` (`FsMetadataReader`) | Pinned directory descriptor; descriptor-relative `openat2` resolution preventing path escapes and following symlinks beneath the pinned descriptor. Distinguishes the configured root pathname (resolved via host OS resolution on reader open) from paths resolved beneath the pinned descriptor. |
| **Single-Directory Enumeration** | `storage-fs` (`dir`) | Bounded iteration via `getdents64`/`readdir`; per-enumeration entry and name-byte limit enforcement (`DirEnumerationLimits`); point-in-time observation of entry names and `DirEntryType`. |

---

## 2. Comprehensive Compatibility Table

The table below contrasts legacy production listing behavior (`FsStorage::list_cas_blobs_page`) with the proposed test seam behavior (`list_cas_blobs_page_seam`):

| Feature / Behavior Aspect | Legacy Behavior (`FsStorage::list_cas_blobs_page`) | Proposed Seam Behavior (`list_cas_blobs_page_seam`) | Verification Evidence | Unresolved Production Decision |
|---|---|---|---|---|
| **Absent CAS Directory** (`blobs/sha256` missing) | Returns `Ok(GcBlobPage { items: [], next_cursor: None })`. | Returns `Ok(IncompleteGcBlobPage { items: [], next_cursor: None })`. | `test_real_fs_absent_cas_root_returns_empty_page`, `test_fake_empty_root_not_found_returns_empty_page` | None: consensus that an absent CAS namespace represents an empty repository state. |
| **Empty CAS Root** (`blobs/sha256` exists, 0 shards) | Returns `Ok(empty page)`. | Returns `Ok(empty page)`. | `test_real_fs_empty_cas_root_returns_empty_page`, `test_fake_empty_root_empty_entries_returns_empty_page` | None. |
| **Empty Shard Directory** (`0a/` exists, 0 files) | Returns `Ok(empty page)`. | Returns `Ok(empty page)`. | `test_real_fs_empty_shard_returns_empty_page` | None. |
| **Initial Non-NotFound Error** (e.g. `blobs` is a regular file) | Silently suppressed into empty page by `if tokio::fs::metadata(&root).await.is_err()`. | Fails closed with typed `CorruptData` (`NotADirectory`). | `test_real_fs_initial_not_a_directory_error_not_suppressed`, `test_fake_typed_error_mappings` | **DECISION REQUIRED**: Confirm that production listing cutover should surface structural filesystem corruption rather than masking it as empty results. |
| **Initial Permission Error** (`EACCES` on CAS root) | Silently suppressed into empty page by `metadata(&root).is_err()`. | Fails closed with `PermissionDenied`. | `test_fake_suppression_on_root_failure` | **DECISION REQUIRED**: Confirm production policy that permission faults fail closed. |
| **Malformed Shard Entry Type** (e.g. regular file in `blobs/sha256/`) | Fails closed with `CorruptData` ("malformed non-directory entry..."). | Fails closed with `CorruptData` ("malformed non-directory entry..."). | `test_real_fs_fails_closed_on_symlinked_shard`, `test_fake_corrupt_entry_types_and_names` | None: registry policy preserved. |
| **Malformed Shard Name** (e.g. `0aa` or `xyz`) | Fails closed with `CorruptData` ("malformed 2-char prefix directory name..."). | Fails closed with `CorruptData` ("malformed 2-char prefix directory name..."). | `test_fake_corrupt_entry_types_and_names` | None: registry policy preserved. |
| **Malformed Blob Entry Type** (e.g. directory or FIFO in shard) | Fails closed with `CorruptData` ("malformed non-file entry..."). | Fails closed with `CorruptData` ("malformed non-file entry..."). | `test_real_fs_fails_closed_on_nested_subdirectories_in_shard`, `test_fake_corrupt_entry_types_and_names` | None: registry policy preserved. |
| **Malformed Blob Name** (not 64-hex or prefix mismatch) | Fails closed with `CorruptData` ("malformed blob file name..."). | Fails closed with `CorruptData` ("malformed blob file name..."). | `test_fake_corrupt_entry_types_and_names` | None: registry policy preserved. |
| **Symlinked Shard Directory** | Fails closed with `CorruptData` because `ft.is_dir()` returns false. | Observed as `DirEntryType::Symlink` during root enumeration; registry validation fails closed with `CorruptData` ("malformed non-directory entry in CAS prefix directory root"). | `test_real_fs_fails_closed_on_symlinked_shard` | None: fails closed with `CorruptData` in both. |
| **Symlinked Blob File** | Fails closed with `CorruptData` because `ft.is_file()` returns false. | Fails closed with `CorruptData` because `DirEntryType` is `Symlink`. | `test_real_fs_fails_closed_on_symlinked_blob` | None. |
| **Ancestor Symlink Traversal Beneath Root** (`blobs` is symlink pointing outside) | Resolves pathnames via OS VFS, following symlink to external target directory. | Fails closed with `ResolutionRejected` (`ELOOP` / `Io`) under `openat2` containment flags beneath the pinned root descriptor. | `test_real_fs_ancestor_symlink_rejection` | **CONTAINMENT ADVANCEMENT**: Aligns listing containment with activated read paths (`head_blob`, `open_blob`). |
| **Page Limit Clamping** | Clamped to `limit.min(1000).max(1)`. | Clamped to `limit.min(1000).max(1)`. Verified with 1,001 candidate fixture that 50,000 clamps to 1,000. | `test_real_fs_limit_clamping_zero_to_one`, `test_fake_limit_clamping_upper_bound_with_1001_candidates` | None: legacy contract preserved. |
| **Lexical Cursor Filtering** | Evaluated via literal string comparison `digest_str <= cursor`. | Evaluated via literal string comparison `digest_str <= cursor`. | `test_real_fs_cursor_lexical_filtering_and_malformed` | None: legacy contract preserved. |
| **Out-of-Range Cursors** (`"aaa"`, `"zzz"`) | Evaluated directly; `"zzz"` returns empty page, `"aaa"` returns all items. | Evaluated directly; `"zzz"` returns empty page, `"aaa"` returns all items. | `test_real_fs_cursor_lexical_filtering_and_malformed` | None: legacy contract preserved. |
| **Exact-Full Page Cursor** (`items.len() == limit`) | Returns `Some(cursor)` pointing to last candidate. | Returns `Some(cursor)` pointing to last candidate. | `test_real_fs_exact_full_final_page`, `test_fake_ordered_pagination_and_boundaries`, `test_fake_limit_clamping_upper_bound_with_1001_candidates` | None: legacy contract preserved. |
| **Terminal Empty Page** (cursor past all items) | Returns `items: []`, `next_cursor: None`. | Returns `items: []`, `next_cursor: None`. | `test_real_fs_exact_full_final_page`, `test_fake_ordered_pagination_and_boundaries`, `test_fake_direct_seam_multi_page_progression_and_terminal_behavior` | None: legacy contract preserved. |
| **Enumeration Resource Limits** | No resource limits; allocates unbounded directory entries in memory. | Explicit caller-supplied `DirEnumerationLimits` per call. Exceeding limits fails closed immediately. | `test_real_fs_budget_limits_entry_count`, `test_real_fs_budget_limits_name_bytes`, `test_real_fs_zero_limit_empty_vs_non_empty` | **DECISION REQUIRED**: Select production budget defaults for directory enumeration. |
| **Candidate Size** | Obtained via `tokio::fs::metadata(&path).await?.len()`. | **UNAVAILABLE**: `enumerate_dir` does not provide file size. | `test_metadata_gap_incomplete_candidate_cannot_produce_gc_blob_candidate` | **METADATA GAP**: Requires production decision (see Section 3). |
| **Candidate Last-Modified (`mtime`)** | Obtained via `meta.modified()`. Failure falls back to `UNIX_EPOCH`; pre-epoch duration defaults to 0 secs. | **UNAVAILABLE**: `enumerate_dir` and `head` do not provide timestamps. | `test_metadata_gap_head_lacks_mtime_and_version` | **METADATA GAP**: Requires production decision (see Section 3). |
| **Candidate Version Identifier** | Computed as `format!("{mtime_secs}:{size}")`. | **UNAVAILABLE**: Cannot be computed without `mtime` and `size`. | `test_metadata_gap_incomplete_candidate_cannot_produce_gc_blob_candidate` | **METADATA GAP**: Requires production decision (see Section 3). |
| **Inter-Page Mutation Semantics** | No snapshot isolation; mutations ahead of cursor are observed, behind are missed. | No snapshot isolation; mutations ahead of cursor are observed, behind are missed. | `test_real_fs_deterministic_inter_page_mutation_no_snapshot` | Inherent filesystem listing characteristic. |
| **Early Shard Skipping on Cursor Resumption** | Does not skip shards; calls `read_dir` on all prefix dirs. | Does not skip shards; validates all prefix dirs up to cursor. | `test_fake_ordered_pagination_and_boundaries` | **DECISION REQUIRED**: Evaluate whether future listing should optimize away prefix enumeration prior to cursor. |

---

## 3. Metadata and Version API Gap Analysis

### Nature of the Gap
In `registry-rust`, garbage collection planning and candidate verification rely on [`crate::storage::GcBlobCandidate`]:

```rust
pub struct GcBlobCandidate {
    pub digest: Digest,
    pub size: u64,
    pub last_modified: SystemTime,
    pub version: BlobObjectVersion,
}
```

In the legacy implementation (`FsStorage::list_cas_blobs_page`), constructing this struct required calling `tokio::fs::metadata(&path)` on every discovered blob file during directory iteration:
1. `size` was populated from `fs_metadata.len()`.
2. `last_modified` was populated from `fs_metadata.modified().unwrap_or(UNIX_EPOCH)`.
3. `version` was formatted as `BlobObjectVersion(format!("{mtime_secs}:{size}"))`, where `mtime_secs` defaulted to 0 if conversion before epoch failed.

In contrast, the extracted and committed APIs in `storage-layer-rust` expose:
- [`storage_fs::FsMetadataReader::enumerate_dir`]: Returns `Vec<DirEntry>`. Each `DirEntry` provides only:
  - `name: &OsStr`
  - `file_type: DirEntryType` (from `readdir` `d_type` or `fstatat` with `AT_SYMLINK_NOFOLLOW`).
  It does **not** provide byte length, timestamps, inode generation, or version strings.
- [`storage_core::ObjectMetadataReader::head`]: Accepts `&ObjectKey` and returns [`storage_core::ObjectMetadata`].
  In Slice 2A, `ObjectMetadata` encapsulates **only** `size: u64`. It explicitly omits timestamps and version identifiers.

### Refactoring Boundary Compliance
In accordance with project guidelines:
1. **No Value Fabrication**: The test seam does **not** fabricate synthetic timestamps (e.g. `UNIX_EPOCH`), dummy lengths (`0`), or placeholder versions (`"0:0"`).
2. **No Uncontained Pathname Reopening**: The seam does **not** invoke `tokio::fs::metadata` behind the reader's back, which would violate descriptor-relative containment and reintroduce TOCTOU races.
3. **No Unilateral Crate Extension**: Neither `storage-core` nor `storage-fs` was modified during this slice.

### Proposed Candidate Seam Representation
The seam introduces explicit types documenting this boundary:
- [`IncompleteGcCandidate`]: Retains validated `digest: Digest` and `shard: String`.
- [`IncompleteGcBlobPage`]: Retains paginated `items: Vec<IncompleteGcCandidate>` and `next_cursor: Option<GcCursor>`.
- [`IncompleteCandidateTranslationGap`]: Returned by `IncompleteGcCandidate::try_into_legacy_candidate()` to explicitly document why conversion to `GcBlobCandidate` cannot occur with the current APIs.

### Options for Future Production Resolution
To bridge this gap before cutting over production listing in a subsequent slice, three architectural paths exist:
1. **Statx-Enhanced Directory Enumeration in `storage-fs`**: Extend `storage-fs::dir` on Linux to optionally retrieve file size and modification timestamps during directory traversal (via `statx` or `fstatat`), populating a richer entry structure.
2. **Contained Two-Phase Inspection in Registry Layer**: For each candidate returned by `enumerate_dir`, call an enhanced `ObjectMetadataReader::head` through the pinned descriptor. Note that this requires extending `ObjectMetadata` in `storage-core` to include timestamps/generation, and issues N descriptor lookups per page.
3. **Revision of GC Candidate Version Contract**: Re-evaluate whether filesystem GC candidates strictly require filesystem `mtime` as their version identifier, or if content-addressed identity (`sha256:<hex>`) combined with quarantine leases provides sufficient safety guarantees without filesystem timestamps.

---

## 4. Enumeration Budget Policy Requiring Production Decision

### Distinction: Public Page Limit vs. Per-Enumeration Resource Budget
- **Public Page Limit (`limit`)**: A public domain parameter clamped to `[1, 1000]`. It bounds the number of candidate items returned to the caller per page.
- **Enumeration Limits (`DirEnumerationLimits`)**: Systems-level bounds (`max_entries`, `max_total_name_bytes`) applied to each single `enumerate_dir` call to constrain per-call kernel iteration and userspace vector allocation. It is not a global bound on total seam memory, all calls, or syscall duration.

### Failure Semantics
In `storage-fs`, exceeding an enumeration limit returns [`storage_fs::FsDirError::LimitExceeded`]. The test seam translates this into `StorageErrorKind::Backend` and fails closed immediately:
- It does **not** silently truncate the directory.
- It does **not** return a partial page.
- It does **not** synthesize a continuation token.

### Required Production Decisions
1. **Production Budget Values**: Shard directory naming does not impose an upper cardinality bound on valid CAS contents; a single prefix directory in a large registry could hold a substantial number of blobs. Production budget defaults remain undecided and require operational capacity planning.
2. **Budget Exhaustion Handling**: Confirm that failing closed with an operational error on budget exhaustion is acceptable for registry GC tasks, or whether streaming or intra-directory pagination is desired in the future. Whether streaming or continuation-token contracts are introduced into `storage-core` is a future design choice, not an established requirement.

---

## 5. Mutation and Root-Coherence Limitations

### Absence of Snapshot Isolation
Both legacy listing and the descriptor-relative test seam execute without snapshot isolation:
- Resuming a listing query at cursor $C$ re-enumerates shard directories on the active filesystem.
- If a blob $B_1$ ($B_1 < C$) is added after the first query, it is **missed** in the current GC traversal cycle.
- If a blob $B_2$ ($B_2 > C$) is added after the first query, it is **observed** when pagination reaches its shard.
- As verified in `test_real_fs_deterministic_inter_page_mutation_no_snapshot`, this behavior is an inherent property of directory traversal on POSIX filesystems without filesystem-level snapshots.

### TOCTOU Races and Directory Entry Observations
Directory entry types returned by `enumerate_dir` are **point-in-time observations**, not file handles authorizing subsequent operations. Descriptor containment guarantees that operations cannot escape the directory hierarchy or traverse symlinks beneath the pinned descriptor. However, descriptor containment must be clearly separated from identity, transactional snapshot consistency, and coherence with pathname-based mutations:
- A concurrent replacement of an enumerated entry with another regular file or directory of the same name may be accessed successfully by subsequent operations rather than detected as a modification.
- If an unlinked entry is replaced with a symlink, subsequent descriptor-relative `openat2` operations will reject it with `ResolutionRejected`.

### Root Coherence and Configured Root
- **Configured Root vs. Descriptor Containment**: Opening the reader (`FsMetadataReader::open(&root)`) resolves the configured root path via standard OS resolution to obtain the initial `root_fd`. It does not reject a symlink used as the configured root itself. Descriptor containment (`openat2` flags `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`) strictly governs all operations resolved beneath that pinned root descriptor.
- **Inode Pinning**: Once opened, descriptor-relative operations continue operating on the pinned inode even if the configured root path on disk is renamed or moved. Full root coherence across the application requires completing the migration of mutation paths to descriptor-relative operations.

---

## 6. Scope Tested vs. Scope Not Implemented

### Scope Implemented and Tested (This Slice)
1. **Test-Only Seam Module (`src/storage/fs/listing_seam.rs`)**:
   - `CasDirEnumerator` trait abstraction with implementations for `FsMetadataReader` and `RecordingFakeDirEnumerator`.
   - `list_cas_blobs_page_seam` function implementing CAS layout traversal, validation, sorting, lexical cursor filtering, and limit clamping over `enumerate_dir`.
   - Proposed typed error translation from `FsDirError` to `StorageError`.
   - Explicit `IncompleteGcCandidate` and `IncompleteCandidateTranslationGap` modeling.
2. **Seam Test Suite (30 Unique Unit & Integration Tests)**:
   - **10 Recording Fake Tests**:
     1. `storage::fs::listing_seam::tests::test_fake_empty_root_not_found_returns_empty_page`
     2. `storage::fs::listing_seam::tests::test_fake_empty_root_empty_entries_returns_empty_page`
     3. `storage::fs::listing_seam::tests::test_fake_suppression_on_root_failure`
     4. `storage::fs::listing_seam::tests::test_fake_suppression_on_shard_failure`
     5. `storage::fs::listing_seam::tests::test_fake_typed_error_mappings`
     6. `storage::fs::listing_seam::tests::test_fake_budget_forwarded_to_enumerator`
     7. `storage::fs::listing_seam::tests::test_fake_corrupt_entry_types_and_names`
     8. `storage::fs::listing_seam::tests::test_fake_ordered_pagination_and_boundaries`
     9. `storage::fs::listing_seam::tests::test_fake_limit_clamping_upper_bound_with_1001_candidates` (proves 50,000 clamps to 1,000 with 1,001 candidates and explicit budget)
     10. `storage::fs::listing_seam::tests::test_fake_direct_seam_multi_page_progression_and_terminal_behavior` (direct `IncompleteGcBlobPage` multi-page progression and terminal behavior)
   - **16 Linux Filesystem Tests**:
     11. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_absent_cas_root_returns_empty_page`
     12. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_empty_cas_root_returns_empty_page`
     13. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_empty_shard_returns_empty_page`
     14. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_ordering_and_multi_page_pagination`
     15. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_exact_full_final_page`
     16. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_limit_clamping_zero_to_one`
     17. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_cursor_lexical_filtering_and_malformed`
     18. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_fails_closed_on_symlinked_shard`
     19. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_fails_closed_on_symlinked_blob`
     20. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_fails_closed_on_nested_subdirectories_in_shard`
     21. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_ancestor_symlink_rejection`
     22. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_initial_not_a_directory_error_not_suppressed`
     23. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_budget_limits_entry_count`
     24. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_budget_limits_name_bytes`
     25. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_zero_limit_empty_vs_non_empty`
     26. `storage::fs::listing_seam::tests::linux_fs_tests::test_real_fs_deterministic_inter_page_mutation_no_snapshot`
   - **2 Metadata Gap Tests**:
     27. `storage::fs::listing_seam::tests::test_metadata_gap_incomplete_candidate_cannot_produce_gc_blob_candidate`
     28. `storage::fs::listing_seam::tests::test_metadata_gap_head_lacks_mtime_and_version`
   - **2 Independent Traverser Tests (Scripted Complete Fixtures Only)**:
     29. `storage::fs::listing_seam::tests::test_traverser_batch_progression_with_scripted_fixtures` (verifies `CasBlobTraverser` pagination with mock complete fixtures, without wrapping or synthesizing seam candidates)
     30. `storage::fs::listing_seam::tests::test_traverser_detects_cursor_cycle_and_repeated_cursor` (verifies cycle and repeated cursor error detection in isolation)
3. **Characterization Preservation & Existing Tests**:
   - All 13 existing characterization tests in `src/storage/fs/tests.rs` pass via `cargo test --locked --lib test_list_cas_blobs`.
   - All 4 existing GC unit tests in `src/blob_gc/mod.rs` pass via `cargo test --locked --lib blob_gc::tests`.

### Scope Not Implemented (Deferred to Subsequent Slices)
1. **Production Cutover**: `FsStorage::list_cas_blobs_page` remains on legacy uncontained `tokio::fs::read_dir`. No production callers invoke `listing_seam`.
2. **Metadata Extension**: No changes to `storage-core` (`ObjectMetadata`) or `storage-fs` (`DirEntry`).
3. **Generic Listing Contract**: No generic listing trait or continuation-token contract added to `storage-core`. Quality gate **O-03** remains OPEN.
4. **Mutations and Quarantine**: Quarantine and delete operations remain on their existing implementations.

---

## 7. Open Quality Gates

All established quality gates remain **OPEN**:
- **O-03 (Keys & Continuation Tokens)**: OPEN. Generic continuation tokens and listing abstractions are deferred.
- **O-04 (Durability & Containment)**: OPEN.
- **O-05 (Read Containment)**: OPEN. Activated read paths (`head_blob`, `open_blob`) are contained; listing seam is verified in test-only mode; production listing remains uncontained.
- **O-06 (AWS Mapping & Pinned MinIO)**: OPEN.
- **O-13 (Distribution & Release Strategy)**: OPEN.
- **O-15 (Non-Linux Filesystem Support)**: OPEN.
- **O-16 (Slice 11 Inventory Completeness)**: OPEN.
- **D-06**: OPEN.
