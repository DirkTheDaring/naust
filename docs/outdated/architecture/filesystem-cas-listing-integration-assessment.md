> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. The CAS listing work landed (`0087264`); fixed listing budgets were later removed in favor of streaming (`1772f0a`…`9405991`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Filesystem CAS Listing Integration Assessment & Compatibility Record

**Repository:** `registry-rust`
**Scope:** Test-only integration seam evaluating `storage-fs` directory enumeration (`FsMetadataReader::enumerate_dir`) and contained file metadata inspection (`FsMetadataReader::inspect_file_metadata`) against `registry-rust` CAS listing requirements, GC candidate completion, error taxonomy, and containment boundaries.
**Slice:** Bounded Slice (Test-Only Candidate Metadata Completion). Production listing is **not** cut over; `storage-layer-rust` remains read-only.

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
    - Receives batches of completed GcBlobCandidate records
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
                - Consumes CasListingSource (CasDirEnumerator + CasMetadataInspector)
                - Operates over pinned root descriptor with Linux openat2 containment flags:
                  RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS
                - Enumerates shard directories and inspects selected candidate metadata beneath pinned root
                - Converts inspected metadata to complete GcBlobCandidate { digest, size, last_modified, version }
                - Yields complete GcBlobPage { items: Vec<GcBlobCandidate>, next_cursor }
                - Connects to CasBlobTraverser via test-only read-only SeamGcStorageBridge
```

### Separation of Responsibilities

| Responsibility Area | Owning Component | Specific Rules & Contracts |
|---|---|---|
| **CAS Namespace & Layout** | `registry-rust` (`listing_seam`) | CAS root `blobs/sha256`; 2-character lowercase hexadecimal shard subdirectories (`00`..`ff`); 64-character lowercase hexadecimal blob files matching shard prefix. |
| **Digest Parsing & Normalization** | `registry-rust` (`listing_seam`) | Constructs and validates `sha256:<64-hex>` via `Digest::parse`. Rejects non-hex, malformed, or mismatching entries. |
| **Cursor Progression & Clamping** | `registry-rust` (`listing_seam`) | Public limit clamped to `limit.min(1000).max(1)`. Lexical cursor comparison `digest_str <= cursor`. Exact-full-page sets `next_cursor = Some(GcCursor(digest_str))`. |
| **Metadata & Version Conversion** | `registry-rust` (`listing_seam`) | Translates inspected `FsFileMetadata` into `GcBlobCandidate`: preserves exact `u64` size; preserves `last_modified` (with `UNIX_EPOCH` fallback on `None`); formats listing version as `BlobObjectVersion(format!("{version_seconds}:{size}"))`. |
| **Proposed Error Classification** | `registry-rust` (`listing_seam`) | Maps typed `FsDirError` and `ReadError` outcomes into registry `StorageError` taxonomy (`CorruptData`, `Io`, `Backend`, `PermissionDenied`, `Configuration`). All mappings are test-seam proposals requiring a deliberate compatibility assessment before production cutover. |
| **Descriptor Containment** | `storage-fs` (`FsMetadataReader`) | Pinned directory descriptor; descriptor-relative `openat2` resolution preventing path escapes and following symlinks beneath the pinned descriptor. Distinguishes the configured root pathname (resolved via host OS resolution on reader open) from paths resolved beneath the pinned descriptor. |
| **Single-Directory Enumeration** | `storage-fs` (`dir`) | Bounded iteration via `getdents64`/`readdir`; per-enumeration entry and name-byte limit enforcement (`DirEnumerationLimits`); point-in-time observation of entry names and `DirEntryType`. |
| **Contained Metadata Inspection** | `storage-fs` (`inspect`) | Descriptor-relative `openat2` resolution of regular file targets; `fstat` attribute acquisition; checked timestamp conversion returning `FsFileMetadata` (exact byte length and optional `SystemTime`). |

---

## 2. Comprehensive Compatibility Table

The table below contrasts legacy production listing behavior (`FsStorage::list_cas_blobs_page`) with the completed test seam behavior (`list_cas_blobs_page_seam`):

| Feature / Behavior Aspect | Legacy Behavior (`FsStorage::list_cas_blobs_page`) | Completed Seam Behavior (`list_cas_blobs_page_seam`) | Verification Evidence | Unresolved Production Decision |
|---|---|---|---|---|
| **Absent CAS Directory** (`blobs/sha256` missing) | Returns `Ok(GcBlobPage { items: [], next_cursor: None })`. | Returns `Ok(GcBlobPage { items: [], next_cursor: None })`. | `test_real_fs_absent_cas_root_returns_empty_page`, `test_fake_empty_root_not_found_returns_empty_page` | None: consensus that an absent CAS namespace represents an empty repository state. |
| **Empty CAS Root** (`blobs/sha256` exists, 0 shards) | Returns `Ok(empty page)`. | Returns `Ok(empty page)`. | `test_real_fs_empty_cas_root_returns_empty_page`, `test_fake_empty_root_empty_entries_returns_empty_page` | None. |
| **Empty Shard Directory** (`0a/` exists, 0 files) | Returns `Ok(empty page)`. | Returns `Ok(empty page)`. | `test_real_fs_empty_shard_returns_empty_page` | None. |
| **Initial Non-NotFound Error** (e.g. `blobs` is a regular file) | Silently suppressed into empty page by `if tokio::fs::metadata(&root).await.is_err()`. | Fails closed with typed `CorruptData` (`NotADirectory`). | `test_real_fs_initial_not_a_directory_error_not_suppressed`, `test_fake_typed_dir_error_mappings` | **DECISION REQUIRED**: Confirm that production listing cutover should surface structural filesystem corruption rather than masking it as empty results. |
| **Initial Permission Error** (`EACCES` on CAS root) | Silently suppressed into empty page by `metadata(&root).is_err()`. | Fails closed with `PermissionDenied`. | `test_fake_suppression_on_root_failure` | **DECISION REQUIRED**: Confirm production policy that permission faults fail closed. |
| **Malformed Shard Entry Type** (e.g. regular file in `blobs/sha256/`) | Fails closed with `CorruptData` ("malformed non-directory entry..."). | Fails closed with `CorruptData` ("malformed non-directory entry..."). | `test_real_fs_fails_closed_on_symlinked_shard`, `test_fake_corrupt_entry_types_and_names` | None: registry policy preserved. |
| **Malformed Shard Name** (e.g. `0aa` or `xyz`) | Fails closed with `CorruptData` ("malformed 2-char prefix directory name..."). | Fails closed with `CorruptData` ("malformed 2-char prefix directory name..."). | `test_fake_corrupt_entry_types_and_names` | None: registry policy preserved. |
| **Malformed Blob Entry Type** (e.g. directory or FIFO in shard) | Fails closed with `CorruptData` ("malformed non-file entry..."). | Fails closed with `CorruptData` ("malformed non-file entry..."). | `test_real_fs_fails_closed_on_nested_subdirectories_in_shard`, `test_fake_corrupt_entry_types_and_names` | None: registry policy preserved. |
| **Malformed Blob Name** (not 64-hex or prefix mismatch) | Fails closed with `CorruptData` ("malformed blob file name..."). | Fails closed with `CorruptData` ("malformed blob file name..."). | `test_fake_corrupt_entry_types_and_names` | None: registry policy preserved. |
| **Symlinked Shard Directory** | Fails closed with `CorruptData` because `ft.is_dir()` returns false. | Observed as `DirEntryType::Symlink` during root enumeration; registry validation fails closed with `CorruptData`. | `test_real_fs_fails_closed_on_symlinked_shard` | None: fails closed with `CorruptData` in both. |
| **Symlinked Blob File** | Fails closed with `CorruptData` because `ft.is_file()` returns false. | Fails closed with `CorruptData` because `DirEntryType` is `Symlink`. | `test_real_fs_fails_closed_on_symlinked_blob` | None. |
| **Ancestor Symlink Traversal Beneath Root** (`blobs` is symlink pointing outside) | Resolves pathnames via OS VFS, following symlink to external target directory. | Fails closed with `ResolutionRejected` (`ELOOP` / `Io`) under `openat2` containment flags beneath the pinned root descriptor. | `test_real_fs_ancestor_symlink_rejection` | **CONTAINMENT ADVANCEMENT**: Aligns listing containment with activated read paths (`head_blob`, `open_blob`). |
| **Page Limit Clamping** | Clamped to `limit.min(1000).max(1)`. | Clamped to `limit.min(1000).max(1)`. Verified with 1,001 candidate fixture that 50,000 clamps to 1,000. | `test_real_fs_limit_clamping_zero_to_one`, `test_fake_limit_clamping_upper_bound_with_1001_candidates` | None: legacy contract preserved. |
| **Lexical Cursor Filtering** | Evaluated via literal string comparison `digest_str <= cursor`. | Evaluated via literal string comparison `digest_str <= cursor`. Entries `<= cursor` are never inspected. | `test_real_fs_cursor_lexical_filtering_and_malformed`, `test_fake_inspect_selected_keys_order_and_count` | None: legacy contract preserved. |
| **Out-of-Range Cursors** (`"aaa"`, `"zzz"`) | Evaluated directly; `"zzz"` returns empty page, `"aaa"` returns all items. | Evaluated directly; `"zzz"` returns empty page, `"aaa"` returns all items. | `test_real_fs_cursor_lexical_filtering_and_malformed` | None: legacy contract preserved. |
| **Exact-Full Page Cursor** (`items.len() == limit`) | Returns `Some(cursor)` pointing to last candidate. | Returns `Some(cursor)` pointing to last candidate. | `test_real_fs_exact_full_final_page`, `test_fake_ordered_pagination_and_boundaries`, `test_fake_limit_clamping_upper_bound_with_1001_candidates` | None: legacy contract preserved. |
| **Terminal Empty Page** (cursor past all items) | Returns `items: []`, `next_cursor: None`. | Returns `items: []`, `next_cursor: None`. | `test_real_fs_exact_full_final_page`, `test_fake_ordered_pagination_and_boundaries`, `test_fake_direct_seam_multi_page_progression_and_terminal_behavior` | None: legacy contract preserved. |
| **Enumeration Resource Limits** | No resource limits; allocates unbounded directory entries in memory. | Explicit caller-supplied `DirEnumerationLimits` per call. Exceeding limits fails closed immediately without partial pages. | `test_real_fs_budget_limits_entry_count`, `test_real_fs_budget_limits_name_bytes`, `test_real_fs_zero_limit_empty_vs_non_empty`, `test_fake_budget_failure_prevents_partial_page` | **DECISION REQUIRED**: Select production budget defaults for directory enumeration. |
| **Candidate Size** | Obtained via `tokio::fs::metadata(&path).await?.len()`. | **COMPLETED**: Obtained via `inspect_file_metadata`. Exact `u64` size preserved in candidate and version string. | `test_real_fs_candidate_metadata_accuracy_and_formatting`, `test_fake_candidate_size_above_u32_max` | Mechanism in `storage-fs`, translation in `registry-rust`. |
| **Candidate Last-Modified (`mtime`)** | Obtained via `meta.modified()`. Failure falls back to `UNIX_EPOCH`; pre-epoch duration defaults to 0 secs. | **COMPLETED**: Obtained via `inspect_file_metadata`. Genuine timestamps and pre-epoch timestamps preserved; `None` falls back to `UNIX_EPOCH`. | `test_real_fs_candidate_metadata_accuracy_and_formatting`, `test_fake_fractional_timestamp_preservation`, `test_fake_genuine_epoch_handling`, `test_fake_none_timestamp_fallback`, `test_fake_pre_epoch_timestamp_retention_and_zero_version_seconds`, `test_fake_future_timestamp_retention` | Preserves legacy fallback and age evaluation policy. |
| **Candidate Version Identifier** | Computed as `format!("{mtime_secs}:{size}")`. | **COMPLETED**: Computed as `format!("{version_seconds}:{size}")`. | `test_real_fs_candidate_metadata_accuracy_and_formatting`, `test_fake_ordered_pagination_and_boundaries` | Matches legacy listing format exactly. Distinct from quarantine version. |
| **Disappeared Candidate Between Enum and Inspect** | Fails whole page with `StorageError::io`. | Fails whole page with `StorageError::io` ("candidate blob disappeared before metadata inspection"). | `test_real_fs_disappeared_blob_between_enumeration_and_inspection_fails_closed_io`, `test_fake_missing_selected_entry_fails_whole_page_io`, `test_fake_later_inspection_failure_does_not_yield_partial_page` | **DECISION REQUIRED**: Confirm that failing the page with `Io` remains desired for production, or if an explicit skip policy should be authorized. |
| **Substituted Non-Regular Object (Symlink / Dir)** | Symlink traverses through uncontained OS VFS; dir fails later on open. | Contained resolution rejects symlink with `ResolutionRejected` (`Io`); inspection rejects dir with `UnsupportedObjectType` (`CorruptData`). | `test_real_fs_symlink_substituted_blob_fails_closed_io`, `test_real_fs_directory_substituted_blob_fails_closed_corrupt_data` | Fails closed with typed error beneath pinned root descriptor. |
| **Substituted Regular File of Same Name** | Reads attributes of replacement file. | Atomically renames a distinct regular file (verified via `MetadataExt` with different inode while holding original open) over the path before inspection; inspection observes the replacement file's attributes at resolution time. Point-in-time observation, not an identity or snapshot guarantee. | `test_real_fs_regular_file_replacement_observed_at_resolution_time` | Inherent filesystem characteristic without transactional snapshots. |
| **Inter-Page Mutation Semantics** | No snapshot isolation; mutations ahead of cursor are observed, behind are missed. | No snapshot isolation; mutations ahead of cursor are observed, behind are missed. | `test_real_fs_deterministic_inter_page_mutation_no_snapshot` | Inherent filesystem listing characteristic. |

---

## 3. Metadata and Version Gap Completion Analysis

### Resolution of the Candidate Metadata Gap
In earlier slices, `list_cas_blobs_page_seam` returned `IncompleteGcCandidate` instances because directory enumeration alone (`enumerate_dir`) returned only entry names and entry types. `storage_core::ObjectMetadataReader::head` exposed only `size: u64` and lacked timestamps.

In this slice, the gap has been completed strictly within the test-only seam by integrating the contained metadata inspection API (`storage_fs::FsMetadataReader::inspect_file_metadata`):
1. `storage-fs` supplies generic descriptor-relative metadata inquiry beneath the pinned root descriptor via `openat2` (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`) and `fstat`, returning `FsFileMetadata { size: u64, modified: Option<SystemTime> }`.
2. `registry-rust` (`listing_seam.rs`) applies registry policy to convert `FsFileMetadata` into complete `GcBlobCandidate` records:
   - `digest: Digest`: parsed and normalized from directory entry name.
   - `size: u64`: exact byte size from `inspected.size()`.
   - `last_modified: SystemTime`: `inspected.modified().unwrap_or(SystemTime::UNIX_EPOCH)`.
   - `version: BlobObjectVersion`: formatted as `BlobObjectVersion(format!("{version_seconds}:{size}"))`, where `version_seconds = last_modified.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()`.

### Exact Timestamp & Version Conversion Rules
- **Fractional Timestamps**: POSIX nanosecond precision from `stat` is preserved in `last_modified`. Only the version string formats whole seconds (`version_seconds`), matching legacy listing.
- **Genuine Epoch Timestamp**: An inspected timestamp of `Some(UNIX_EPOCH)` remains `last_modified == UNIX_EPOCH` and `version == "0:{size}"`. It is not an inspection error. Registry age policy (`check_candidate_age`) evaluates `last_modified == UNIX_EPOCH` as `AgeEligibility::MissingTimestamp`.
- **None Timestamp Fallback**: When filesystem metadata cannot provide modification time (`None`), registry policy falls back to `UNIX_EPOCH`, producing `version == "0:{size}"` and evaluating as `MissingTimestamp`.
- **Pre-Epoch Timestamps**: Genuine pre-epoch timestamps (e.g. `UNIX_EPOCH - 500s`) are retained in `last_modified`. Duration conversion relative to `UNIX_EPOCH` fails, defaulting `version_seconds` to `0` (`version == "0:{size}"`). Registry age policy calculates `now.duration_since(last_modified)` which succeeds with an elapsed age > 50 years, evaluating as `AgeEligibility::Eligible`.
- **Future Timestamps**: Future timestamps are retained in `last_modified`. Registry age policy detects `now < last_modified` and evaluates as `AgeEligibility::FutureTimestamp`.

### Demarcation: Two Separate Filesystem Version Formats
The codebase uses two separate, non-interchangeable version string formats:
1. **Listing Version (`"{version_seconds}:{size}"`)**:
   - Synthesized during CAS listing to satisfy `GcBlobCandidate.version`.
   - Passed into `FsStorage::quarantine_blob(permit, &digest, &version)` where it is **unused** (`_version: &BlobObjectVersion`).
   - Does not include hashes or nanoseconds; does not guarantee identity across replacement.
2. **Quarantined Version (`"fs:{len}:{mtime}:{content_sha256}"`)**:
   - Synthesized exclusively by `compute_fs_blob_version` on quarantined files.
   - Evaluated exclusively during quarantine sweep deletion (`delete_blob_conditional`).
   - Neither format nor mutation behavior is modified by this slice.

---

## 4. Typed Error Taxonomy and Translation

Candidate metadata inspection translates strongly typed `storage_core::ReadError` into `registry-rust` `StorageError` taxonomy. The mapping is driven strictly by typed error variants and downcast types, **never** by parsing diagnostic error message strings:

| Failure Condition | Source Type (`ReadError` / `FsMetadataError` / `std::io::Error`) | Mapped Registry `StorageError` | Diagnostic Treatment & Invariants |
|---|---|---|---|
| **Disappeared Candidate** | `ReadError::NotFound { key }` | `StorageError::Internal(StorageErrorKind::Io)` | Fails the entire page closed with message `"candidate blob disappeared before metadata inspection: {key}"`. Does not return a partial page or skip candidate. |
| **Permission Denied** | `ReadError::PermissionDenied { key, source }` | `StorageError::Internal(StorageErrorKind::Io)` | Fails closed with underlying OS error message or `"permission denied inspecting candidate blob: {key}"`. |
| **Ancestor / File Symlink** | `FsMetadataError::ResolutionRejected { source, .. }` | `StorageError::Internal(StorageErrorKind::Io)` | Fails closed with kernel `ELOOP`/`EXDEV` message. Confined resolution beneath pinned root. |
| **Substituted Non-Regular Object** | `FsMetadataError::UnsupportedObjectType { mode }` | `StorageError::Internal(StorageErrorKind::CorruptData)` | Fails closed with `"unsupported object type (mode: {mode:#o})"`. Non-regular files in CAS violate storage invariants. |
| **Unsupported Syscall (`ENOSYS`)** | `FsMetadataError::SyscallUnsupported(err)` | `StorageError::Internal(StorageErrorKind::Configuration)` | Fails closed with `"openat2 is unavailable in this execution environment: {err}"`. |
| **Unsupported Platform** | `FsMetadataError::PlatformUnsupported` | `StorageError::Internal(StorageErrorKind::Configuration)` | Fails closed with `"platform unsupported: descriptor-relative containment requires Linux openat2"`. |
| **Stat Failure** | `FsMetadataError::StatFailed { stage, source }` | `StorageError::Internal(StorageErrorKind::Io)` | Fails closed with `"failed to stat {stage} descriptor: {source}"`. |
| **Invalid Metadata** | `FsMetadataError::InvalidMetadata { message }` | `StorageError::Internal(StorageErrorKind::CorruptData)` | Fails closed with `"invalid metadata: {message}"`. |
| **Runtime Missing** | `FsMetadataError::RuntimeMissing(err)` | `StorageError::Internal(StorageErrorKind::Backend)` | Fails closed with `"tokio runtime missing: {err}"`. |
| **Task Join Failed** | `FsMetadataError::TaskJoinFailed(err)` | `StorageError::Internal(StorageErrorKind::Backend)` | Fails closed with `"blocking metadata task join failed: {err}"`. |
| **`ENOTDIR` during resolution** | `std::io::Error(raw_os_error: ENOTDIR)` | `StorageError::Internal(StorageErrorKind::CorruptData)` | Fails closed with corrupt data taxonomy. |
| **Other `std::io::Error`** | `std::io::Error` | `StorageError::Internal(StorageErrorKind::Io)` | Fails closed with underlying I/O error message. |
| **Unknown Error** | `ReadError::Backend { message, source: None }` | `StorageError::Internal(StorageErrorKind::Io)` | Mapped to `Io` using provided message. Diagnostic text words do not change category. |

---

## 5. Preserved Pagination Policy & Containment Rules

1. **Limit Clamping**: Public limit parameter clamped to `[1, 1000]`. Limit 0 clamps to 1; 50,000 clamps to 1,000.
2. **Lexical Filtering**: `digest_str <= cursor`. Candidates excluded by cursor are **never** inspected for metadata.
3. **Lazy Candidate Inspection**: Metadata is inspected only for validated candidates surviving cursor filtering. Entries beyond `limit` are **never** inspected.
4. **All-or-Error / No Partial Pages**: If any candidate inspection fails (e.g. file disappeared or was replaced with a symlink), the entire page fails closed immediately. No partial page is returned.
5. **Full-Page Continuation**: Exactly full page returns `Some(next_cursor)` pointing to the last returned candidate. Terminal page returns `None`.
6. **Enumeration Budget Enforcement**: Directory enumeration limits (`DirEnumerationLimits`) are enforced per-directory call. Budget exhaustion fails closed with `Backend` without partial results.

---

## 6. Concurrency, Mutation, and Containment Limitations

1. **Absence of Snapshot Isolation**: Multi-directory traversal and subsequent metadata inspection execute without filesystem snapshot isolation. Mutations ahead of the cursor are observed; mutations behind the cursor are missed.
2. **Two-Phase Observation, Not Handle Retention**: Directory enumeration and metadata inspection are separate operations. An entry observed as regular file during enumeration may be replaced before inspection. Contained `openat2` ensures that a replacement symlink is rejected, but replacing an entry with another distinct regular file (demonstrated in tests by atomically renaming a separate file with distinct inode while holding the original open) succeeds and observes the replacement file's attributes at resolution time. This is a point-in-time observation, not an identity guarantee.
3. **No Atomic Attribute Snapshot**: A single `fstat` on an open descriptor reflects attributes at the moment of the syscall; under concurrent in-place mutation, fields are not guaranteed to represent an atomic snapshot.
4. **No Atomic Conditional Deletion**: The listing version string (`"{version_seconds}:{size}"`) provides no CAS concurrency guarantee against concurrent replacement or deletion.
5. **Root Coherence**: Reader opens resolve the configured root path via standard OS resolution to pin `root_fd`. Operations beneath the pinned descriptor use `openat2` containment flags. Registry mutation paths currently use pathname-based resolution; full root coherence requires migrating mutation paths.

---

## 7. Scope Tested vs. Scope Not Implemented

### Test Inventory and Evidence Scope (44 Unique Unit Tests in `listing_seam.rs`)

| Test Category | Test Identifier | Evidence Scope | Verification Method |
|---|---|---|---|
| **Recording Fake: Root & Shards** | `test_fake_empty_root_not_found_returns_empty_page` | Absent CAS namespace returns empty page | Constructed fake |
| | `test_fake_empty_root_empty_entries_returns_empty_page` | Empty CAS root returns empty page | Constructed fake |
| | `test_fake_suppression_on_root_failure` | Root permission failure fails closed, suppresses shards | Constructed fake |
| | `test_fake_suppression_on_shard_failure` | Shard read failure fails closed, suppresses subsequent shards | Constructed fake |
| | `test_fake_typed_dir_error_mappings` | Directory enumeration typed error mappings | Constructed fake |
| | `test_fake_budget_forwarded_to_enumerator` | Limits forwarded to enumerator | Constructed fake |
| | `test_fake_corrupt_entry_types_and_names` | CAS namespace syntax and type validation | Constructed fake |
| | `test_fake_ordered_pagination_and_boundaries` | Pagination ordering, cursors, and limits | Constructed fake with default metadata |
| | `test_fake_limit_clamping_upper_bound_with_1001_candidates` | 1,001 entries fixture proving 50,000 clamps to 1,000 | Constructed fake with 1,001 entries |
| | `test_fake_direct_seam_multi_page_progression_and_terminal_behavior` | Multi-page progression and terminal None | Constructed fake with default metadata |
| **Recording Fake: Metadata Inspection** | `test_fake_inspect_selected_keys_order_and_count` | Exact selected keys, call order, cursor exclusion, and limit truncation | Constructed fake recording inspection calls |
| | `test_fake_candidate_size_above_u32_max` | 5GB size preservation in candidate and version string | Constructed fake with 5,000,000,000 size |
| | `test_fake_fractional_timestamp_preservation` | Nanoseconds preserved in `last_modified`; whole seconds in version | Constructed fake with fractional timestamp |
| | `test_fake_genuine_epoch_handling` | `Some(UNIX_EPOCH)` produces `"0:{size}"`, evaluates as `MissingTimestamp` | Constructed fake with epoch timestamp |
| | `test_fake_none_timestamp_fallback` | `None` falls back to `UNIX_EPOCH`, evaluates as `MissingTimestamp` | Constructed fake with None timestamp |
| | `test_fake_pre_epoch_timestamp_retention_and_zero_version_seconds` | Pre-epoch SystemTime preserved; version seconds defaults to 0; age evaluates as Eligible | Constructed fake with pre-epoch timestamp |
| | `test_fake_future_timestamp_retention` | Future timestamp preserved; age evaluates as FutureTimestamp | Constructed fake with future timestamp |
| | `test_fake_missing_selected_entry_fails_whole_page_io` | Disappeared entry (`NotFound`) fails page with `StorageErrorKind::Io` | Constructed fake injecting `NotFound` |
| | `test_fake_later_inspection_failure_does_not_yield_partial_page` | Later failure after successful candidate aborts entire page | Constructed fake injecting partial failure |
| | `test_fake_typed_error_mappings_unaffected_by_diagnostic_words` | Error mappings immune to misleading text words | Constructed fake error matrix |
| | `test_fake_budget_failure_prevents_partial_page` | Enumeration budget failure aborts entire page | Constructed fake injecting limit failure |
| **Real Linux Filesystem Tests** | `test_real_fs_absent_cas_root_returns_empty_page` | Real disk: missing CAS root returns empty page | Real filesystem (`storage_fs::FsMetadataReader`) |
| | `test_real_fs_empty_cas_root_returns_empty_page` | Real disk: empty CAS root returns empty page | Real filesystem |
| | `test_real_fs_empty_shard_returns_empty_page` | Real disk: empty shard returns empty page | Real filesystem |
| | `test_real_fs_ordering_and_multi_page_pagination` | Real disk: 5 blobs across multiple shards, 3 pages | Real filesystem with actual files |
| | `test_real_fs_exact_full_final_page` | Real disk: exact-full-page continuation and terminal None | Real filesystem |
| | `test_real_fs_limit_clamping_zero_to_one` | Real disk: limit 0 clamps to 1 candidate | Real filesystem |
| | `test_real_fs_cursor_lexical_filtering_and_malformed` | Real disk: cursor filtering and boundary strings | Real filesystem |
| | `test_real_fs_fails_closed_on_symlinked_shard` | Real disk: symlinked shard rejected as CorruptData | Real filesystem |
| | `test_real_fs_fails_closed_on_symlinked_blob` | Real disk: symlinked blob rejected as CorruptData | Real filesystem |
| | `test_real_fs_fails_closed_on_nested_subdirectories_in_shard` | Real disk: subdirectory in shard rejected as CorruptData | Real filesystem |
| | `test_real_fs_ancestor_symlink_rejection` | Real disk: ancestor symlink rejected under openat2 containment (`ELOOP`/`Io`) | Real filesystem with symlink ancestor |
| | `test_real_fs_initial_not_a_directory_error_not_suppressed` | Real disk: regular file as CAS root fails closed with CorruptData | Real filesystem |
| | `test_real_fs_budget_limits_entry_count` | Real disk: max_entries exceeded fails closed with Backend | Real filesystem |
| | `test_real_fs_budget_limits_name_bytes` | Real disk: max_total_name_bytes exceeded fails closed with Backend | Real filesystem |
| | `test_real_fs_zero_limit_empty_vs_non_empty` | Real disk: zero budget succeeds on empty, fails on non-empty | Real filesystem |
| | `test_real_fs_deterministic_inter_page_mutation_no_snapshot` | Real disk: mutation behind cursor missed, ahead observed | Real filesystem with inter-page disk writes |
| | `test_real_fs_candidate_metadata_accuracy_and_formatting` | Real disk: complete candidate matches filesystem size and readback mtime | Real filesystem with file stat comparison |
| | `test_real_fs_disappeared_blob_between_enumeration_and_inspection_fails_closed_io` | Real disk: deleting file before inspect fails closed with `Io` | Real filesystem via `InterceptingListingWrapper` |
| | `test_real_fs_symlink_substituted_blob_fails_closed_io` | Real disk: replacing file with symlink fails closed with `Io` (`ELOOP`) | Real filesystem via `InterceptingListingWrapper` |
| | `test_real_fs_directory_substituted_blob_fails_closed_corrupt_data` | Real disk: replacing file with directory fails closed with `CorruptData` | Real filesystem via `InterceptingListingWrapper` |
| | `test_real_fs_regular_file_replacement_observed_at_resolution_time` | Real disk: atomically renaming a separate regular file with distinct inode over blob path observes replacement size, timestamp, and version at resolution time | Real filesystem via `InterceptingListingWrapper` |
| **Traverser Integration Tests** | `test_traverser_progression_over_seam_bridge` | `CasBlobTraverser` batch progression consuming completed seam pages | Real filesystem via `SeamGcStorageBridge` |
| | `test_traverser_detects_cursor_cycle_and_repeated_cursor` | `CasBlobTraverser` cycle and repeated cursor error detection | Scripted adapter fixture |

### Scope Not Implemented (Deferred to Subsequent Slices)
1. **Production Cutover**: `FsStorage::list_cas_blobs_page` remains on legacy uncontained `tokio::fs::read_dir`. No production callers invoke `listing_seam`.
2. **Production Shard Limits**: Capacity-planned default values for `DirEnumerationLimits` are not configured in production storage settings.
3. **Generic Listing Contract**: No generic listing trait or continuation-token contract added to `storage-core`. Quality gate **O-03** remains OPEN.
4. **Mutations and Quarantine**: Quarantine and delete operations remain on legacy implementations.

---

## 8. Open Quality Gates

All established quality gates remain **OPEN**:
- **O-03 (Keys & Continuation Tokens)**: OPEN. Generic continuation tokens and listing abstractions are deferred.
- **O-04 (Durability & Containment)**: OPEN.
- **O-05 (Read Containment)**: OPEN. Activated read paths (`head_blob`, `open_blob`) are contained; listing seam is verified in test-only mode; production listing remains uncontained.
- **O-06 (AWS Mapping & Pinned MinIO)**: OPEN.
- **O-13 (Distribution & Release Strategy)**: OPEN.
- **O-15 (Non-Linux Filesystem Support)**: OPEN.
- **O-16 (Slice 11 Inventory Completeness)**: OPEN.
- **D-06 (Broader Extraction & Cutover Decision)**: OPEN.
