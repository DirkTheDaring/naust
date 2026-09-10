# Filesystem CAS Listing & Pagination Characterization Record

**Repository:** `registry-rust`
**Scope:** Characterization of `FsStorage::list_cas_blobs_page`, pagination/cursor contracts, path containment, and boundary analysis before designing a generic listing implementation.

---

## 1. Call Chain & Ownership Boundary

The filesystem CAS listing boundary spans three layers:

```text
[Production Callers]
  src/blob_gc/mod.rs (run_blob_gc, plan_blob_gc, verify_or_estimate)
        |
        v
[Cursor Tracking & Cycle Detection]
  src/blob_gc/traverser.rs: CasBlobTraverser<'a>
    - Implements next_batch() loop
    - Tracks seen cursors to prevent loops (GcPaginationError::CursorCycle / RepeatedCursor)
    - Receives Vec<GcBlobCandidate> batches
        |
        v
[Port Definition]
  src/storage/ports/mod.rs: GcStoragePort::list_cas_blobs_page
    - cursor: Option<&GcCursor>, limit: usize -> Result<GcBlobPage, StorageError>
    - GcCursor(pub String)
    - GcBlobCandidate { digest: Digest, size: u64, last_modified: SystemTime, version: BlobObjectVersion }
    - BlobObjectVersion(pub String)
        |
        +---> S3 Backend: src/storage/s3.rs: S3Storage::list_cas_blobs_page
        |       - Delegates to list_objects_v2_page(bucket, prefix, continuation_token, limit)
        |       - Next cursor is AWS/MinIO continuation token
        |       - Version is S3 ETag (or "{unix_secs}:{size}")
        |
        +---> Filesystem Backend: src/storage/fs.rs: FsStorage::list_cas_blobs_page
                - Directory traversal over self.root.join("blobs").join("sha256")
                - Next cursor is registry digest string "sha256:<hex>"
                - Version is "{mtime_secs}:{size}"
```

### Separation of Concerns
- **Registry-Owned Domain Logic:**
  - Digest parsing, validation, and domain types (`Digest::parse("sha256:<hex>")`).
  - Sharded CAS directory layout rules (`blobs/sha256/<2-char-prefix>/<64-char-hex>`).
  - GC candidate policy, candidate age verification, and pin inspection (`src/blob_gc/mod.rs`).
  - Traversal pagination loop control, cycle detection, and cursor deduplication (`CasBlobTraverser` in `src/blob_gc/traverser.rs`).
- **Storage Layer / Generic Candidate Responsibilities:**
  - Domain-free directory enumeration and entry discovery.
  - Entry metadata retrieval (`size`, `mtime`).
  - Descriptor-relative directory iteration.
  - Generic continuation tokens must not acquire registry SHA-256 layout rules or assume specific string formats. Quality gate O-03 remains unresolved regarding generic listing token representations.

---

## 2. Tested Findings vs. Source Inspection

The characterization distinguishes empirically verified behavior from source code inspection:

### A. Empirically Verified Findings (Characterization Unit Tests)

All entries below correspond to unit tests in `src/storage/fs/tests.rs`:

| Behavior Aspect | Verified Implementation Behavior | Characterization Test Name |
|---|---|---|
| **Absent CAS Directory** | Returns `Ok(GcBlobPage { items: [], next_cursor: None })` when the CAS listing directory (`blobs/sha256`) is absent (the configured storage root itself is created during storage construction) | `test_list_cas_blobs_missing_or_empty_root_returns_empty_page` |
| **Empty CAS Root** | Returns `Ok(GcBlobPage { items: [], next_cursor: None })` when `blobs/sha256` exists but has no shard subdirectories | `test_list_cas_blobs_missing_or_empty_root_returns_empty_page` |
| **Empty Shard Directory** | Returns `Ok(GcBlobPage { items: [], next_cursor: None })` when shard directories contain no files | `test_list_cas_blobs_missing_or_empty_root_returns_empty_page` |
| **Initial Metadata Error Suppression** | Returns `Ok(GcBlobPage { items: [], next_cursor: None })` for non-NotFound errors (e.g. `NotADirectory` when `blobs` is a regular file) | `test_list_cas_blobs_initial_metadata_error_suppression` |
| **Canonical Ordering & Boundaries** | For canonical fixtures, 2-char shards and 64-char filenames are sorted ASCII-lexicographically; page boundary splits items at the limit | `test_list_cas_blobs_ordering_and_pagination_boundaries` |
| **Exact-Full Final Page** | A page containing exactly the effective limit returns `Some(cursor)`; the subsequent query returns an empty terminal page with `None` cursor | `test_list_cas_blobs_exact_full_final_page` |
| **Limit Clamping: Zero** | `limit = 0` is clamped to `1` via `limit.min(1000).max(1)`, returning 1 item rather than 0 | `test_list_cas_blobs_limit_clamping_zero_and_large_fixture` |
| **Limit Clamping: Upper Limit** | On a fixture of 1,001 valid CAS objects, `limit = 50,000` is clamped to 1,000, returning 1,000 items and a cursor to item 999; next page returns the remaining item | `test_list_cas_blobs_limit_clamping_zero_and_large_fixture` |
| **Lexical Cursor Filtering** | Cursors are filtered by raw lexical string comparison (`digest_str <= cursor`) without format validation; out-of-range cursors (`"zzz"`, `"aaa"`) execute without error | `test_list_cas_blobs_cursor_lexical_filtering_and_malformed_values` |
| **Symlinked Shard Directory** | Fails closed with `CorruptData` because `DirEntry::file_type().is_dir()` returns `false` for symlinks | `test_list_cas_blobs_fails_closed_on_symlinks` |
| **Symlinked Blob File** | Fails closed with `CorruptData` because `DirEntry::file_type().is_file()` returns `false` for symlinks | `test_list_cas_blobs_fails_closed_on_symlinks` |
| **Nested Subdirectory in Shard** | Fails closed with `CorruptData` because `DirEntry::file_type().is_file()` returns `false` for subdirectories | `test_list_cas_blobs_fails_closed_on_nested_subdirectories_in_shard` |
| **Ancestor Symlink Resolution** | On Linux, pathname resolution follows symlinks through configured storage root, `blobs`, and `blobs/sha256` leading to external target directories | `test_list_cas_blobs_symlink_resolution_through_ancestor_paths` |
| **Deterministic Inter-Page Mutation** | Mutations between page calls show absence of snapshot isolation: mutations behind the cursor are missed in the current cycle; mutations ahead are observed | `test_list_cas_blobs_deterministic_inter_page_mutation_no_snapshot` |
| **Malformed Shard Directory Name** | Directory names not matching 2-char hex fail closed with `CorruptData` | `test_list_cas_blobs_for_gc_malformed_prefix_is_corrupt_data` |
| **Malformed Blob Filename** | Files not matching 64-char hex starting with shard prefix fail closed with `CorruptData` | `test_list_cas_blobs_for_gc_malformed_blob_filename_is_corrupt_data` |
| **Non-Directory in CAS Root** | Regular file directly in `blobs/sha256/` fails closed with `CorruptData` | `test_fs_cas_enumeration_fails_closed_on_malformed_prefix_dir` |
| **IO Error on CAS Root** | Unreadable CAS root path fails with `StorageErrorKind::Io` | `test_list_cas_blobs_for_gc_io_error_is_io` |

### B. Source Inspection Findings (Implementation Mechanics)

1. **Initial Metadata Check (`src/storage/fs.rs:3357`):**
   ```rust
   let root = self.root.join("blobs").join("sha256");
   if tokio::fs::metadata(&root).await.is_err() {
       return Ok(GcBlobPage { items: Vec::new(), next_cursor: None });
   }
   ```
   Inspection confirms that ANY error returned by `tokio::fs::metadata(&root)` is silently converted into an empty page. If `blobs` is a regular file (causing `ENOTDIR`), or if permissions prevent reading `blobs/sha256` (causing `EACCES`), `list_cas_blobs_page` does not propagate the error; it treats the CAS directory as absent.

2. **TOCTOU Gap in Path-Based Listing:**
   `list_cas_blobs_page` checks `ent.file_type()` during `read_dir` iteration, then subsequently invokes `tokio::fs::metadata(&path)` using a reconstructed `PathBuf`. This two-step check is not race-safe: an entry could be replaced with a symlink between the `file_type()` check and the `metadata()` call. It does not provide atomic or kernel-enforced descriptor-relative containment.

---

## 3. Ordering, Pagination, and Mutation Semantics

1. **Ordering Model:**
   - Shard prefix directories are enumerated, validated, lowercased, and sorted: `prefix_dirs.sort()`.
   - Files within each shard directory are enumerated, validated, lowercased, and sorted: `entries.sort()`.
   - Digest strings are formatted as `"sha256:{hex}"`.
   - Ordering and no-omission guarantees are verified for canonical, valid CAS layouts. Arbitrary malformed layouts fail closed with `CorruptData`.

2. **Cursor Representation & Filtering:**
   - In the filesystem implementation, `GcCursor` holds an exact digest string (e.g. `GcCursor("sha256:0a00...01")`).
   - Resumption filters entries via raw lexical comparison: `if digest_str.as_str() <= cursor_str { continue; }`.
   - The cursor string is not parsed into a typed `Digest`; any string value is evaluated directly.

3. **Exact-Full Final Page Semantics:**
   - When a page contains exactly the effective limit (e.g. 1,000 items), `candidates.len() >= limit` causes `next_cursor` to be populated with the last candidate's digest string.
   - The listing implementation cannot know whether further items exist without querying again.
   - A subsequent query with that cursor finds no further items strictly greater than the cursor and returns an empty page (`items: []`) with `next_cursor: None`, cleanly terminating pagination.

4. **Deterministic Inter-Page Mutation (Absence of Snapshot Isolation):**
   - Each page re-reads directories from disk via `tokio::fs::read_dir`.
   - Mutations occurring between page calls demonstrate the absence of snapshot isolation:
     - Inserting a file behind the cursor (`hex < cursor`) is missed in the current traversal cycle.
     - Inserting a file ahead of the cursor (`hex > cursor`) is observed when pagination reaches its shard.

---

## 4. Path Containment vs. Activated Read Paths

A fundamental architectural difference exists between the activated read cutover and CAS listing:

1. **Activated Read Paths (`open_blob`, `head_blob`):**
   - Root directory descriptor ownership is established when `FsMetadataReader::open` opens the root directory.
   - Enforce path containment at the kernel level via `openat2` resolution flags: `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`.
   - Symlinks at all levels beneath the root are unconditionally rejected by the kernel.

2. **Current Listing Path (`list_cas_blobs_page`):**
   - Operates via pathname resolution: `self.root.join("blobs").join("sha256").join(&p2).join(&hex)`.
   - Does **not** use the reader's pinned root directory descriptor or `openat2`.
   - Follows symlinks through ancestor paths: as verified on Linux, if `self.root`, `blobs`, or `blobs/sha256` is a symlink pointing outside the configured directory, path resolution traverses the symlink and enumerates the external target.
   - Rejection of entries beneath `blobs/sha256/` is an indirect side-effect of `DirEntry::file_type()` checks (`!is_dir()` / `!is_file()`), not kernel-enforced descriptor containment.

---

## 5. Compatibility Constraints for a Later Generic Listing Port

When designing an extracted generic listing primitive in `storage-fs` (`storage-layer-rust`), the following constraints apply:

1. **Domain-Free Contract:**
   - Generic listing must remain domain-free: directory enumeration must not know about OCI digests, algorithms, 2-char shard prefixes, or 64-char hex filenames.
   - CAS path conventions and candidate conversion belong strictly in the registry layer.

2. **Continuation Token Abstraction:**
   - S3 listing uses opaque AWS continuation tokens; filesystem listing currently uses digest strings.
   - Do not assume generic continuation tokens must be unrestricted strings or force S3 tokens into digest formats. Quality gate O-03 remains unresolved.

3. **Descriptor-Relative Traversal:**
   - Generic filesystem listing should utilize descriptor-relative directory iteration beneath the opened root descriptor (`openat`/`fdopendir`) to establish race-safe containment and prevent following ancestor symlinks.

4. **Initial Metadata Check Correction:**
   - The initial metadata check should distinguish `NotFound` from fatal I/O or permission errors (`EACCES`, `ENOTDIR`), avoiding silent suppression of system faults.

---

## 6. Range-Read Handling Scope

- **Repository Audit:**
  - A workspace search for HTTP `Range` and `Content-Range` headers confirms range reading lives exclusively in the presentation layer:
    - [`src/http_api/handlers.rs:319-358`](file:///home/dietmar/devel/rust/registry-rust/src/http_api/handlers.rs#L319-L358): in `get_blob_or_manifest`, byte ranges (`bytes=s-e`) are sliced from the asynchronous chunk stream returned by `app.get_blob()`.
    - Chunked upload handlers in `src/http_api/handlers.rs:1357,1443` evaluate `Content-Range` for upload offset validation.
- **Backend Primitive:**
  - There is **no backend range-read primitive** in `StoragePort`, `BlobStoragePort`, `FsStorage`, or `S3Storage`.
  - Range reads are entirely presentation-layer stream slicing over full object reads and do not represent an unextracted storage backend primitive.

---

## 7. Open Quality Gates

The quality gates retain their established definitions and remain **OPEN**:
- **O-03 (Keys & Continuation):** OPEN. Generic listing port must preserve contract compatibility without conflating S3 tokens with filesystem digests.
- **O-04 (Durability & Containment):** OPEN.
- **O-05 (Read Containment):** OPEN. `head_blob` and `open_blob` are descriptor-relative; `list_cas_blobs_page` remains path-based.
- **O-06 (AWS Mapping & Pinned MinIO):** OPEN.
- **O-13 (Distribution & Release Strategy):** OPEN.
- **O-15 (Non-Linux Filesystem Support):** OPEN.
- **O-16 (Slice 11 Inventory Completeness):** OPEN.
- **D-06:** OPEN.

---

## 8. Recommended Next Implementation Slice

- **Scope:** Design a domain-free descriptor-relative directory enumeration primitive in `storage-fs` (`storage-layer-rust`) that operates beneath an opened directory file descriptor using the existing reader root ownership.
- **Adapter Seam:** Provide a registry-side adapter mapping descriptor-enumerated shard entries to `GcBlobCandidate` records, preserving `GcStoragePort::list_cas_blobs_page` signatures, error classifications (`CorruptData`), and pagination semantics without altering GC callers.
