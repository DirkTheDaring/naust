> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Provenance (reconstructed 2026-09-19): the original carries no date/commit stamps; it was added in commit `5a0b424` (2026-09-12), the cutover commit itself. Tags later moved onto `tag_domain` (`32c42c6`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Production Filesystem Tag Read Cutover Implementation Record

## 1. Executive Summary & Cutover Scope

This document records the production cutover of filesystem tag read operations (`resolve_tag` and `get_tag_with_version`) in `registry-rust` from legacy uncontained pathname-based file reads (`tokio::fs::read` via `self.tag_path(repo, tag)`) to descriptor-relative contained reads via the shared `storage_fs::FsMetadataReader` instance.

### Cutover Scope Summary:
- **Production Delegation**: `FsStorage::resolve_tag` and `FsStorage::get_tag_with_version` now delegate directly to `tag_read::resolve_tag` and `tag_read::get_tag_with_version`.
- **Shared Reader Reuse**: Delegates directly through `self.reader.as_ref()`. Reuses the existing `Arc<FsMetadataReader>` pinned root file descriptor without allocating a secondary reader or reopening the root directory per operation.
- **Module Promotion**: Promoted `src/storage/fs/tag_seam.rs` to production module `src/storage/fs/tag_read.rs` (`pub(crate) mod tag_read;` in `src/storage/fs.rs`), and removed the obsolete seam file from disk (unstaged).
- **Public Entry Points & Test Aliases**: Exported production entry points `resolve_tag` and `get_tag_with_version`, with test aliases `resolve_tag_seam` and `get_tag_with_version_seam` preserved for backward-compatibility with existing unit tests.
- **Exact Contract Preservation**:
  - `resolve_tag`:
    - Validates UTF-8 via `std::str::from_utf8`. Stream read errors and invalid UTF-8 bytes map to `StorageErrorKind::Io`.
    - Trims Unicode whitespace via Rust `str::trim` and parses raw digest text into a typed `Digest` (supporting SHA-256 64-hex and SHA-512 128-hex).
    - Missing files, empty content, and malformed digest text map to `StorageError::NotFound`.
  - `get_tag_with_version`:
    - Missing file maps to `Ok(None)`.
    - Stream read errors map to `StorageErrorKind::Io`.
    - Decodes bytes using `String::from_utf8_lossy`, trims Unicode whitespace via `str::trim`, and parses the digest with `Digest::parse`.
    - Empty content, malformed digest text, and invalid UTF-8 replacement characters (`U+FFFD`) map to `StorageErrorKind::CorruptData`.
    - Computes optimistic-concurrency version token as lowercase hex SHA-256 over the **unmodified original raw bytes** (`&bytes`).
    - Returns a typed `(Digest, String)` (wrapped in `Ok(Some(...))`), returning the parsed `Digest` and version token.
  - Limits: Uses `TagReadLimits::default()` (`max_payload_bytes: None`), matching legacy unbounded read behavior.
- **Application & HTTP Wire Mappings**:
  - In `ManifestReadService::get_manifest` and `head_manifest` (and `resolve_reference_digest`), when a reference is a tag, `resolve_tag` is invoked.
  - Missing tag or malformed tag content (which yields `StorageError::NotFound` from `resolve_tag`) reaches `ManifestReadError::TagNotFound`, returning HTTP 404 `MANIFEST_UNKNOWN` across GET and HEAD.
  - Symlink tag read rejection (which yields `StorageErrorKind::Io` from `openat2` containment) reaches `ManifestReadError::Storage(...)`, returning HTTP 500 `UNKNOWN` across GET and HEAD.
- **Strict Structural Validation**: Rejects invalid caller repository and tag strings (`..`, `.`, leading/trailing `/`, repeated slashes `//`, backslashes `\`, NUL bytes `\0`, and ASCII control characters `\x01..\x1f`) with `StorageError::InvalidRepoName` before composing `repos/<repo>/tags/<tag>`. Safe non-canonical inputs on Linux (such as colons in repo or tag names) remain accepted after composition.
- **Descriptor-Relative Kernel Containment**: Tag reads resolve through `openat2` with flags `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`. Symlinks fail closed with `StorageErrorKind::Io`. Non-regular files fail closed with `StorageErrorKind::Io` (`UnsupportedObjectType`).
- **Operational Namespace-Stability Assumption & Residual Risks**:
  - Production tag reads now use the pinned reader (`self.reader`); they no longer dynamically resolve from `self.root`.
  - Renaming/replacing the configured root pathname leaves the existing root descriptor referring to the originally opened directory.
  - Descendant paths are resolved afresh beneath that descriptor for each payload acquisition; replacing a descendant directory or file can therefore affect subsequent acquisitions.
  - Once acquired, a file descriptor refers to that opened object, but concurrent modification of its contents can still affect reading.
  - Root pinning provides neither a namespace snapshot nor read/write coherence.
  - Mutating operations (`set_tag`, `delete_tag_conditional`, `put_manifest`) continue to resolve ambient pathnames starting from `self.root`.
  - Pathname mutation divergence is a current residual limitation under the operational namespace-stability assumption approved for this cutover: if the root path is replaced concurrently, pinned reads continue to resolve beneath the originally opened root, while pathname mutations operate on the replacement tree.
  - Identical byte content produces identical version hashes across different roots or inodes; version hashing does not bind a version to a root directory or inode.
- **Quality Gates**: All 8 canonical quality gates (`O-03, O-04, O-05, O-06, O-13, O-15, O-16, D-06`) remain explicitly **OPEN**.

---

## 2. Production Call Flow & Shared Reader Ownership

### 2.1 Call Graph
```text
Client Request / HTTP Layer / Downstream Application Service
  │
  ├─► ManifestReadService::get_manifest / head_manifest
  │     │
  │     ├─► ManifestReadService::resolve_reference_digest (when reference is a tag)
  │     │     │
  │     │     ▼
  │     └─► TagReader::resolve_tag (Trait Port)
  │
  ├─► ManifestLifecycleService::delete_tag (precondition read phase)
  │     │
  │     ▼
  ├─► TagReader::get_tag_with_version (Trait Port)
  │     │
  │     ▼
  └─► FsStorage::resolve_tag / get_tag_with_version (src/storage/fs.rs)
        │
        ▼
      tag_read::resolve_tag / get_tag_with_version (src/storage/fs/tag_read.rs)
        │
        ├─► tag_key(repo, tag) -> Result<ObjectKey, StorageError>
        │     - Validates repo & tag: no .., ., leading/trailing /, //, \, NUL, controls
        │     - Composes "repos/<repo>/tags/<tag>"
        │     - ObjectKey::parse validates relative key safety beneath root
        │
        ├─► self.reader.open_payload(&key) [via Arc<FsMetadataReader>]
        │     - Descriptor-relative openat2(root_fd, "repos/...", RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS)
        │     - Phase 1: O_PATH | O_CLOEXEC acquisition
        │     - fstat type check: S_IFREG (rejects directories, FIFOs, sockets, devices)
        │     - Phase 2: Reopen via /proc/self/fd/<phase1_fd> with O_RDONLY | O_CLOEXEC
        │     - Re-verifies regular file type and identity (st_dev, st_ino)
        │     - Returns ObjectPayload (metadata + async stream)
        │     - Acquisition errors mapped via super::read_adapter::translate_payload_read_error
        │
        ├─► Stream consumption: stream.read_to_end(&mut bytes)
        │     - Consumes payload into memory buffer
        │     - Stream read errors mapped to StorageErrorKind::Io
        │
        ├─► [resolve_tag]:
        │     - std::str::from_utf8(&bytes) -> Err maps to StorageErrorKind::Io
        │     - s.trim() (Unicode whitespace trimming)
        │     - Digest::parse(...) -> Err maps to StorageError::NotFound (empty/malformed)
        │     - Returns typed Digest
        │
        └─► [get_tag_with_version]:
              - String::from_utf8_lossy(&bytes)
              - s.trim() (Unicode whitespace trimming)
              - Digest::parse(...) -> Err maps to StorageErrorKind::CorruptData
              - version = hex::encode(Sha256::digest(&bytes)) [unmodified original raw-byte hash]
              - Returns (Digest, version) wrapped in Ok(Some(...))
```

### 2.2 Shared Reader Guarantees
1. **Zero New Root Descriptors**: `FsStorage::resolve_tag` and `FsStorage::get_tag_with_version` borrow `self.reader.as_ref()`. The pinned root directory descriptor acquired during `FsStorage::try_new` is reused across all read operations (blobs, CAS listing, manifests, and tags).
2. **Per-Call File Descriptors**: Reusing the root reader avoids reopening the storage root directory; each individual tag read operation continues to acquire fresh per-call file descriptors (`openat2` Phase 1 descriptor and Phase 2 readable descriptor via `/proc/self/fd/<phase1_fd>`), managed with RAII ownership (`OwnedFd` and `std::fs::File`).
3. **No Pathname Fallback**: If payload acquisition fails (e.g. `NotFound`, `PermissionDenied`, `ResolutionRejected`), the error is translated immediately through `translate_payload_read_error`. The implementation never falls back to uncontained path resolution (`tokio::fs::read`).

---

## 3. Preserved Contracts & Digest Parsing

### 3.1 Contract Differences Between Operations
- **`resolve_tag`**:
  - Caller requires only the target `Digest`.
  - Parses text into `Digest` (supporting SHA-256 and SHA-512 hex strings).
  - Empty file: returns `StorageError::NotFound`.
  - Malformed text / invalid hex / wrong length: returns `StorageError::NotFound`.
  - Invalid UTF-8: returns `StorageError::Internal { kind: StorageErrorKind::Io, ... }`.
  - Stream I/O failure: returns `StorageError::Internal { kind: StorageErrorKind::Io, ... }`.
- **`get_tag_with_version`**:
  - Caller requires the target `Digest` and an optimistic-concurrency version token.
  - Missing file: returns `Ok(None)`.
  - Invalid UTF-8 (replacement char `U+FFFD`), empty content, or malformed digest: returns `StorageError::Internal { kind: StorageErrorKind::CorruptData, ... }`.
  - Stream I/O failure: returns `StorageError::Internal { kind: StorageErrorKind::Io, ... }`.
  - Version token: lowercase hex SHA-256 over **unmodified original raw file bytes** (`hex::encode(Sha256::digest(&raw_bytes))`).
  - Raw-byte sensitivity: whitespace differences (e.g. trailing newline `\n` vs no newline) produce distinct version strings, matching the conditional delete expectation.

### 3.2 Unbounded Read Limits
In accordance with characterization findings, `TagReadLimits::default()` sets `max_payload_bytes: None`. Large padded tag files (e.g., > 64 KiB of whitespace) are read completely and parsed successfully. There is currently no seam-enforced production payload ceiling.

---

## 4. Accepted Path Validation, Symlink Containment, and Error Differences

| Aspect | Legacy Pathname Behavior (`tokio::fs::read`) | Production Contained Behavior (`tag_read`) | Rationale / Security Benefit |
| :--- | :--- | :--- | :--- |
| **Path Traversal (`..`)** | Unvalidated `Path::join` allowed escaping the storage root if target existed. | `tag_key` rejects `..` segments before key parsing with `StorageError::InvalidRepoName`. | Prevents directory traversal attacks escaping repository boundaries. |
| **Dot Segments (`.`)** | Normalizes / skips segment during path resolution. | `tag_key` rejects `.` segments with `StorageError::InvalidRepoName`. | Enforces canonical non-normalized segment structure. |
| **Leading / Trailing `/`** | Resolves as absolute path (ignoring prefix) or directory. | Rejected with `StorageError::InvalidRepoName`. | Prevents root-escape and directory confusion. |
| **Backslashes (`\`)** | Treated as literal filename character on Linux. | Rejected with `StorageError::InvalidRepoName`. | Prevents cross-platform path confusion. |
| **NUL / ASCII Controls** | Embedded NUL is rejected before an OS pathname syscall; controls accepted. | `tag_key` rejects NUL and ASCII controls with `StorageError::InvalidRepoName`. | Fails fast before filesystem interaction. |
| **Non-Canonical Keys (`C:/repo`)** | Accepted as relative path containing colon on Linux. | Accepted on Linux: composes `repos/C:/repo/tags/<tag>` as a valid relative `ObjectKey`. | Preserves current Linux behavior without introducing unapproved colon rejection. |
| **External Symlinks** | Followed symlinks to outside files/directories. | Rejected by `openat2` (`RESOLVE_BENEATH \| RESOLVE_NO_SYMLINKS`), mapped to `StorageErrorKind::Io`. | Contained resolution; symlink traversal is disallowed. |
| **Internal Symlinks** | Followed symlinks to inside files. | Rejected by `openat2` (`RESOLVE_NO_SYMLINKS`), mapped to `StorageErrorKind::Io`. | Storage repository layout requires regular files; symlink aliasing is disallowed. |
| **Ancestor Symlinks** | Traversed symlinked directories within path. | Rejected by `openat2` (`RESOLVE_NO_SYMLINKS`), mapped to `StorageErrorKind::Io`. | Prevents directory aliasing or redirection. |
| **Dangling Symlinks** | Failed target resolution returning `ENOENT` -> `StorageError::NotFound`. | Resolution fails closed at the symlink itself -> `StorageErrorKind::Io` (`ResolutionRejected`). | Security boundary: presence of a prohibited symlink is an I/O containment error, not an absent file. |
| **Genuine Missing Paths** | Returned `StorageError::NotFound`. | Returns `StorageError::NotFound`. | Preserved semantic parity for genuinely missing objects. |
| **Non-Regular Objects** | Directory read returned `EISDIR` -> `StorageErrorKind::Io`; FIFO read may block. | Rejected during Phase 1 `fstat` validation (`S_IFREG`), returning `StorageErrorKind::Io` (`UnsupportedObjectType`). | Prevents potential FIFO read blocking and invalid object type consumption. |
| **Root Directory Replacement** | Reads followed pathname `self.root` at call time. | Reads follow pinned file descriptor of initial root directory across renames. | Contained reads remain bound to initial root descriptor; pathname mutations operate on replacement tree. |
| **Descendant Path Replacement** | Reads followed pathname at call time. | Descendant paths are resolved afresh beneath pinned root descriptor per acquisition. | Replacing a descendant directory or file affects subsequent acquisitions; root pinning is not a namespace snapshot. |

---

## 5. Comprehensive Test Execution Matrix & Verification Accounting

### 5.1 Verification Commands Executed & Log-Derived Status

| Command | Scope | Passed | Failed | Ignored | Exit Code | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `cargo fmt --check` | Formatting compliance | - | - | - | 0 | PASSED |
| `cargo check --locked --all-targets --all-features` | Static type checking | - | - | - | 0 | PASSED |
| `cargo clippy --locked --all-targets --all-features -- -D warnings` | Linter rules | - | - | - | 0 | PASSED |
| `git diff --check` | Whitespace & conflict markers | - | - | - | 0 | PASSED |
| `cargo test --locked --lib storage::fs::tag_read` | Contained tag read unit tests | 28 | 0 | 1 | 0 | PASSED |
| `cargo test --locked --lib storage::fs::tests` | Storage fs tests (filter `storage::fs::tests`) | 130 | 0 | 6 | 0 | PASSED |
| `cargo test --locked --test application_read_tests` | Application read service & HTTP wire tests | 49 | 0 | 0 | 0 | PASSED |
| `cargo test --locked --test manifest_lifecycle_tests` | Manifest lifecycle & root replacement tests | 79 | 0 | 0 | 0 | PASSED |

**Total Log-Derived Passing Executions across these four recorded test runs**: 286 passed, 0 failed, 7 ignored.
*Note on external service tests*: In `application_read_tests`, `test_application_read_and_proxy_publication_services_s3_minio` completed with status `ok` because it returned early when the optional local MinIO probe endpoint was unreachable (`TEST_S3_REQUIRED` not set). It does **not** constitute live S3/MinIO verification evidence.

### 5.2 Test Coverage Inventory

#### Promoted Tag Read Module Tests (`src/storage/fs/tag_read.rs`):
1. `test_tag_key_valid_single_and_nested`: Validates relative key composition for single-segment and nested multi-segment repository and tag paths.
2. `test_tag_key_structural_rejections`: Validates pre-composition rejection of path traversal (`..`), dot segments (`.`), leading/trailing slashes, double slashes (`//`), backslashes (`\`), NUL bytes (`\0`), and ASCII control characters with `StorageError::InvalidRepoName`.
3. `test_seam_resolve_and_get_tag_valid_sha256`: Validates `resolve_tag` parsing into typed `Digest` and `get_tag_with_version` version computation for SHA-256 tags.
4. `test_seam_resolve_and_get_tag_valid_sha512`: Validates `resolve_tag` parsing and `get_tag_with_version` version computation for SHA-512 tags.
5. `test_seam_resolve_and_get_tag_padded_whitespace`: Validates Unicode whitespace trimming across both operations.
6. `test_seam_raw_byte_version_hash_whitespace_sensitivity`: Validates that version hash is computed over unmodified raw bytes (verifying distinct versions for trailing newline vs no newline).
7. `test_seam_missing_tag_taxonomy`: Validates `StorageError::NotFound` returned for absent tag files.
8. `test_seam_empty_file_taxonomy`: Validates `resolve_tag` -> `NotFound`, `get_tag_with_version` -> `CorruptData`.
9. `test_seam_malformed_text_taxonomy`: Validates `resolve_tag` -> `NotFound`, `get_tag_with_version` -> `CorruptData`.
10. `test_seam_invalid_utf8_taxonomy`: Validates `resolve_tag` -> `Io`, `get_tag_with_version` -> `CorruptData`.
11. `test_seam_typed_fs_error_mappings`: Validates mapped translations from `storage_core::ReadError` (`NotFound`, `PermissionDenied`, `ResolutionRejected`, etc.) to `StorageError`.
12. `test_seam_stream_io_failure_partial`: Validates translation of mid-stream read failures to `StorageErrorKind::Io`.
13. `test_seam_key_validation_zero_reader_calls`: Proves that invalid inputs fail fast without invoking `open_payload`.
14. `test_seam_limit_zero_behavior`: Validates behavior when byte limits are configured.
15. `test_seam_limit_exact_and_one_over`: Validates boundary enforcement when payload length limits are active.
16. `test_seam_limit_u64_max_overflow_rejection`: Validates arithmetic overflow protection in length bounds checks.
17. `test_seam_drain_understated_metadata_helper`: Validates payload stream draining when file metadata understates actual byte count.
18. `test_seam_understated_metadata_valid_digest`: Validates successful digest read when metadata understates file size.
19. `test_seam_overstated_metadata_valid_digest`: Validates successful digest read when metadata overstates file size.
20. `test_seam_unbounded_large_padded_payload`: Validates unbounded reading of payloads > 64 KiB with `max_payload_bytes: None`.
21. `test_seam_real_contained_read_success`: Verifies real Linux filesystem descriptor-relative tag reading.
22. `test_seam_real_path_traversal_rejected`: Verifies real Linux filesystem traversal rejection before filesystem access.
23. `test_seam_real_final_symlink_rejected`: Verifies symlink tag files fail closed with `StorageErrorKind::Io`.
24. `test_seam_real_ancestor_symlink_rejected`: Verifies ancestor directory symlinks fail closed with `StorageErrorKind::Io`.
25. `test_seam_real_dangling_symlink_rejected`: Verifies dangling symlinks fail closed with `StorageErrorKind::Io` (`ResolutionRejected`).
26. `test_seam_real_non_regular_rejected`: Verifies directory substituted in place of tag fails closed with `StorageErrorKind::Io` (`UnsupportedObjectType`).
27. `test_seam_real_shared_reader_identity`: Verifies reuse of the shared `FsMetadataReader` instance.
28. `test_seam_real_root_replacement_divergence_demonstrated`: Proves pinned descriptor reads observe original tree across rename while pathname access addresses the new tree.
29. `test_seam_real_permission_denied`: Unprivileged permission test (ignored by default in unprivileged user environments).

#### Key Production Storage Tests (`src/storage/fs/tests.rs`):
1. `test_tag_read_valid_sha256_and_sha512_with_and_without_newline`: Validates SHA-256 and SHA-512 parsing through `FsStorage`.
2. `test_tag_read_version_hashes_raw_byte_sensitivity`: Validates version hash sensitivity across `FsStorage::get_tag_with_version`.
3. `test_tag_read_whitespace_tabs_crlf_and_substantial_padding`: Validates Unicode whitespace and padding handling.
4. `test_tag_read_missing_tag_and_missing_repository`: Validates `NotFound` on missing repository and tag.
5. `test_tag_read_empty_malformed_digest_and_invalid_utf8`: Validates empty (`NotFound` / `CorruptData`), malformed (`NotFound` / `CorruptData`), and invalid UTF-8 (`Io` for resolve, `CorruptData` for version).
6. `test_tag_read_path_component_and_traversal_cases`: Validates traversal rejections and nested tag component acceptance.
7. `test_tag_read_controlled_symlinks`: Validates symlink rejection (`StorageErrorKind::Io`) for internal, external, ancestor, and dangling symlinks.
8. `test_tag_read_directory_in_place_of_file`: Validates non-regular object rejection (`UnsupportedObjectType` -> `Io`).
9. `test_tag_read_sequential_root_replacement_observed_tree`: Validates pinned root reading across directory replacement.
10. `test_fs_storage_tag_read_production_contract_and_entry_points`: Comprehensive production contract validation covering both `resolve_tag` and `get_tag_with_version`.

#### Application & HTTP Wire Tests (`tests/application_read_tests.rs`):
1. `test_manifest_read_service_tag_fixtures_cutover`: Validates `ManifestReadService::get_manifest` with valid, missing, malformed (verified `TagNotFound`), and symlink (`Storage(Io)`) tag fixtures.
2. `test_http_manifest_tag_read_endpoints_and_error_mappings`: Validates HTTP wire endpoints `/v2/:repo/manifests/:tag` across **both GET and HEAD**:
   - Valid tag: GET returns 200 with exact bytes; HEAD returns 200 with digest, content-type, content-length headers, and empty body.
   - Missing tag: GET returns 404 `MANIFEST_UNKNOWN`; HEAD returns 404 with empty body.
   - Malformed tag: GET returns 404 `MANIFEST_UNKNOWN`; HEAD returns 404 with empty body.
   - Symlink tag: GET returns 500 `UNKNOWN`; HEAD returns 500 with empty body.
   - Pre-storage rejection: returns 400 `NAME_INVALID`.
3. `test_http_distinguish_valid_input_storage_error_and_tag_precondition_failed`:
   - Proves distinction between HTTP 400 pre-storage rejection (`NAME_INVALID`) and HTTP 500 storage-injected `InvalidRepoName` (`UNKNOWN`).
   - Checks HTTP DELETE with mismatched preconditions, asserting HTTP 500 `UNKNOWN` and post-request state outcomes consistent with the intended `TagPreconditionFailed` path: no journal file left on disk (`read_lifecycle_journal` is `None`), reference index health is ready (`check_health().is_ok()`), and tag files in both Tree A and Tree B remain intact with unchanged bytes.
   - Note on evidence boundary: These HTTP-level final-state checks alone do not uniquely prove an internal error variant or prove that a journal was created and then deleted. Typed-error evidence is provided by the separate lifecycle unit test (`test_manifest_lifecycle_delete_tag_precondition_failed_cleanup`), which directly inspects and asserts `ManifestLifecycleError::TagPreconditionFailed`.

#### Lifecycle & Root Replacement Tests (`tests/manifest_lifecycle_tests.rs`):
1. `test_manifest_lifecycle_delete_tag_preservation_on_read_failure`: Verifies tag read failure during DELETE aborts before journal creation or ref-index modification, asserts unchanged tag bytes before/after via exact byte comparison, and asserts preservation of tested config/layer reachability and index health (`is_blob_referenced` and `check_health`).
2. `test_manifest_lifecycle_delete_tag_precondition_failed_cleanup`: Explicitly matches `ManifestLifecycleError::TagPreconditionFailed` for typed-error evidence, verifies journal cleanup (`read_lifecycle_journal` is `None`), asserts preservation of tested config/layer reachability and index health, and asserts unchanged tag bytes in both Tree A and Tree B before/after.
3. `test_manifest_lifecycle_delete_tag_conditional_delete_failure_before_removal`: Induces storage failure before tag removal (via EISDIR on lock path); proves tag file and journal physically persist on disk with unchanged tag bytes.
4. `test_manifest_lifecycle_root_replacement_sequencing`:
   - Case 1: Missing tag in Tree B -> returns `TagNotFound`, Tree A tag preserved with unchanged bytes, Tree B tag absent.
   - Case 2: Modified tag in Tree B -> returns `TagPreconditionFailed`, Tree A and Tree B tags both preserved with unchanged bytes.
   - Case 3: Identical tag in Tree B -> returns `Ok(...)`, Tree B tag deleted, Tree A tag preserved with unchanged bytes.

*Note on index assertions*: Index assertions across these lifecycle tests verify preservation of tested config and layer reachability (`ref_index.is_blob_referenced`) and index health (`check_health().is_ok()`). They do not imply a byte-for-byte snapshot of all index contents.

---

## 6. Known Limitations & Operational Considerations

1. **Operational Namespace-Stability Assumption & Root vs Descendant Semantics**:
   - `FsStorage` shares a single `FsMetadataReader` with a pinned root file descriptor for read operations (`head_manifest`, `get_manifest`, `resolve_tag`, `get_tag_with_version`, `list_cas_blobs_for_gc`).
   - Renaming or replacing the configured root pathname leaves the existing root descriptor referring to the originally opened directory.
   - Descendant paths are resolved afresh beneath that descriptor for each payload acquisition.
   - Replacing a descendant directory or file can therefore affect subsequent acquisitions.
   - Once acquired, a file descriptor refers to that opened object, but concurrent modification of its contents can still affect reading.
   - Root pinning provides neither a namespace snapshot nor read/write coherence.
   - Mutation operations (`put_manifest`, `delete_manifest`, `set_tag`, `delete_tag_conditional`, `put_cas_blob`) resolve ambient pathnames against `self.root`.
   - Pathname mutation divergence is a current residual limitation under the operational namespace-stability assumption approved for this cutover: if the root path is replaced concurrently, pinned reads continue to resolve beneath the originally opened root, while pathname mutations operate on the replacement tree.
   - In production, the storage root directory and its repository namespaces must remain stable during node runtime.
2. **Unbounded Full Read Buffering**:
   - Tag reads consume the entire tag file into memory before parsing. While tag files in standard OCI registries are short ASCII hex strings, `TagReadLimits::default()` leaves `max_payload_bytes: None`. There is no enforced production payload ceiling.
3. **Absence of Read-Side Synchronization & No Read Snapshot**:
   - Tag reads do not acquire advisory file locks (`.lock.{tag}`). Concurrent tag overwrite or truncation may result in `StorageErrorKind::Io` or `StorageErrorKind::CorruptData` during read. Downstream services must handle these transient conditions or retry.
   - Tag payload reads do not provide snapshot isolation; concurrent writes during stream draining may return changed or partial bytes, and detection is not guaranteed.
4. **Version Hash Properties**:
   - The version token is the SHA-256 hash of the raw bytes actually read. It does not bind a version to a root directory or inode. Identical byte contents produce identical version hashes across different roots or files.
5. **Operating System & Kernel Containment Assumptions**:
   - **Procfs Mount**: Phase 2 readable reopening via `/proc/self/fd/<phase1_fd>` requires a genuine, accessible, stable procfs mount.
   - **Kernel Version**: Requires Linux 5.6+ with `openat2` support (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`).
   - **Mount & Hard-Link Isolation**: Kernel containment does not guarantee mount or hard-link isolation.
   - **Non-Linux Verification**: Non-Linux compilation and execution remain unverified.

---

## 7. Code-Only Rollback Plan

- **Zero Schema or Storage Layout Migration**:
  The directory and file structure on disk (`repos/<repo>/tags/<tag>`) is completely unchanged. No database migrations, index alterations, or file moves occurred.
- **Rollback Procedure**:
  Reverting restores the prior source implementation. Restoring running behavior requires rebuilding/redeploying and restarting as appropriate. Rollback does not undo writes, deletions, or other mutations performed while the cutover was active.

---

## 8. Canonical Quality Gate Status

All 8 canonical quality gates remain explicitly **OPEN**:

- **Gate O-03: Key and continuation-token contracts** — OPEN.
- **Gate O-04: Filesystem write durability and containment** — OPEN.
- **Gate O-05: Broader filesystem read containment** — OPEN.
- **Gate O-06: Typed AWS mapping and pinned-MinIO evidence** — OPEN.
- **Gate O-13: Hosting, distribution, and release strategy** — OPEN.
- **Gate O-15: Non-Linux verification** — OPEN.
- **Gate O-16: Earlier Slice 11 audit/test-inventory evidence** — OPEN.
- **Gate D-06: Broader extraction, cutover, compatibility, and distribution acceptance** — OPEN.
