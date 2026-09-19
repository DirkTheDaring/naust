> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Contained tag reads landed (`5a0b424`); tags later moved onto `tag_domain` (`32c42c6`). GATE WARNING: this document uses "O-03" for repository-grammar validation — a non-canonical redefinition; canonical gate register: [acceptance-gates.md](../../technical-debt.md).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Filesystem Tag Read Contained Test Seam: Implementation Assessment

**Document**: `docs/architecture/filesystem-tag-read-contained-seam.md`  
**Repository**: `registry-rust`  
**Date**: 2026-09-12  
**Scope**: Technical assessment, verification record, and architectural analysis for the contained filesystem tag-read test seam in `registry-rust`.  
**Authorized Scope**: Test-only code and tests (`src/storage/fs.rs` cfg(test) module declaration, `src/storage/fs/tag_seam.rs`, and this assessment). Production code, routing, and outward API behavior remain strictly unchanged.

---

## 1. Executive Summary and Repository Baselines

This document assesses the implementation of the test-only contained filesystem tag-read seam designed in `docs/architecture/filesystem-tag-read-contained-integration-design.md`. The seam validates contained descriptor-relative tag reading using Linux `openat2` (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`) through the shared [`storage_fs::FsMetadataReader`], preserving all established caller parsing contracts and raw-byte version semantics while eliminating ambient path traversal vulnerabilities.

### 1.1 Repository Baselines
- **`registry-rust`**:
  - Baseline HEAD: `7d6649d45e855aaefa4335782515e121d69afa26`
  - Working tree status: Clean tracked baseline with only authorized `cfg(test)` additions.
- **`storage-layer-rust`**:
  - Baseline HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`
  - Status: Strictly read-only, uncommitted, untouched.

### 1.2 Quality Gates Status
All canonical quality gates remain **OPEN**:
- **O-03** (Strict OCI Grammar Validation vs Nested Repositories): OPEN. Seam permits nested repository components while rejecting empty/relative path traversal segments. Strict OCI grammar is not imposed.
- **O-04** (Tag Payload Ceiling & Memory Policy): OPEN. Seam defaults to unbounded reading (`None`), preserving existing production behavior. Bounded limits are explicit opt-in test controls (`Some(N)`).
- **O-05** (Descriptor-Relative Tag Read Production Cutover): OPEN. Production routing remains on legacy ambient pathname operations.
- **O-06** (Advisory Lock Coexistence): OPEN. Tag reads remain unlocked; mutation and conditional deletion locking via `.lock.{tag}` remains uncontained.
- **O-13** (Root Directory Replacement Coherence): OPEN. Contained reads observe pinned root inode; pathname mutations observe current path target.
- **O-15** (Non-Linux Tag Read Strategy): OPEN. Target is Linux-contained descriptor operations.
- **O-16** (Concurrent Mutation Detection During Tag Stream Drain): OPEN. Version hashing reflects drained bytes without snapshot guarantees.
- **D-06** (Cross-Component Production Read Readiness): OPEN.

---

## 2. Architectural Ownership and Component Demarcation

```
+-----------------------------------------------------------------------------------+
| registry-rust                                                                     |
|                                                                                   |
|  Tag Key Composition: "repos/<repo>/tags/<tag>"                                   |
|  Pre-composition Path Checks: reject empty segments, ".", "..", "/", "\"         |
|                                                                                   |
|  +-------------------------------------+  +------------------------------------+  |
|  | resolve_tag_seam                    |  | get_tag_with_version_seam          |  |
|  | - Strict UTF-8 validation (Io)      |  | - Lossy UTF-8 (from_utf8_lossy)    |  |
|  | - Trim whitespace                   |  | - Trim whitespace                  |  |
|  | - Parse Digest (NotFound on error)  |  | - Parse Digest (CorruptData on err)|  |
|  |                                     |  | - SHA-256 over UNMODIFIED raw bytes|  |
|  +-------------------------------------+  +------------------------------------+  |
|                     |                                       |                     |
|                     +-------------------+-------------------+                     |
|                                         |                                         |
|                               drain_tag_stream Helper                             |
|                             - None: Unbounded (Vec::new)                          |
|                             - Some(N): Take(N+1), exact overflow check            |
|                             - Checked arithmetic (u64::MAX -> CorruptData)        |
|                             - Stream I/O error -> abort (no partial results)      |
|                                         |                                         |
|                             translate_payload_read_error Translator               |
|                             - Typed FsMetadataError unwrapping                    |
|                             - Backend source inspection                           |
|                                         |                                         |
+-----------------------------------------|-----------------------------------------+
                                          | storage_core::ObjectPayloadReader
                                          v
+-----------------------------------------------------------------------------------+
| storage-fs (via FsMetadataReader)                                                 |
|                                                                                   |
|  Phase 1: openat2(root_fd, key, O_PATH | RESOLVE_BENEATH | NO_SYMLINKS)           |
|  Validation: S_IFREG verification via fstat                                       |
|  Phase 2: openat(proc_self_fd, O_RDONLY | O_CLOEXEC)                              |
|  Returns: ObjectPayload with length observation and AsyncRead stream              |
+-----------------------------------------------------------------------------------+
```

### 2.1 Component Boundaries
1. **`storage-core`**: Defines domain-neutral abstractions (`ObjectPayloadReader`, `ObjectPayload`, `ObjectStream`, `ObjectKey`, `ReadError`). Contains no registry or tag-specific concepts.
2. **`storage-fs`**: Implements kernel-enforced descriptor-relative containment via Linux `openat2`. Ensures that tag paths cannot escape `root_fd` or traverse symlinks. Enforces regular file (`S_IFREG`) semantics. Reopens payload descriptors safely via `/proc/self/fd/N`.
3. **`registry-rust`**: Owns tag key composition, pre-composition validation, separate caller parsing contracts, raw-byte version hashing, stream draining policies, and mapping to registry domain errors (`StorageError`).

---

## 3. Implementation Details

### 3.1 Reader Lifecycle and Identity Verification
The seam directly accepts `&(impl ObjectPayloadReader + ?Sized)`. In real filesystem tests, it operates directly on `&*storage.reader` (the existing `Arc<storage_fs::FsMetadataReader>`), verifying:
- Identity sharing with the existing production read adapter: `Arc::as_ptr(&storage.reader) == Arc::as_ptr(storage.read_adapter.reader())`.
- Zero descriptor reopening overhead for `root_fd` between components.
- Direct operational execution through both `resolve_tag_seam` and `get_tag_with_version_seam` using the shared reader.
- Pointer comparison demonstrates instance sharing with `FsBlobCasReadAdapter`; behavioral evidence that calls resolve relative to the pinned directory tree is established by `test_seam_real_root_replacement_divergence_demonstrated`. Pointer comparison alone does not prove the absence of underlying descriptor operations.

### 3.2 Pre-Composition Path Validation
Before converting path components to an `ObjectKey`, `validate_path_component` strictly checks both `repository` and `tag`:
- Rejects empty strings.
- Rejects paths containing `\0`, ASCII control characters, `\`, or leading/trailing slashes.
- Iterates over `/`-separated components:
  - Disallows `.` (current directory) and `..` (parent traversal).
  - Disallows empty inner components (e.g. `repo//sub`).
- Permits nested repository components (e.g. `org/team/app`), satisfying O-03 while preventing path escapes before any reader call.
- On validation failure, returns `StorageError::InvalidRepoName` immediately with **zero reader invocations**.

### 3.3 Separate Caller Parsing Contracts
The seam faithfully preserves the contrasting parsing contracts of the two tag read entrypoints:

| Dimension | `resolve_tag_seam` | `get_tag_with_version_seam` |
| :--- | :--- | :--- |
| **UTF-8 Decoding** | Strict `std::str::from_utf8` | Lossy `String::from_utf8_lossy` |
| **Invalid UTF-8 Failure** | `StorageError::Internal { kind: Io, .. }` | `StorageError::Internal { kind: CorruptData, .. }` (replacement char causes parse failure) |
| **Whitespace Handling** | `str::trim()` | `str::trim()` |
| **Missing Tag** | `StorageError::NotFound` | `Ok(None)` |
| **Empty Content** | `StorageError::NotFound` | `StorageError::Internal { kind: CorruptData, .. }` |
| **Malformed Digest Text** | `StorageError::NotFound` | `StorageError::Internal { kind: CorruptData, .. }` |
| **Valid Digest Result** | `Ok(Digest)` | `Ok(Some((Digest, String)))` |
| **Version Generation** | None | SHA-256 over original unmodified raw bytes (`hex::encode(hasher.finalize())`) |

### 3.4 Raw-Byte Version Hashing
`get_tag_with_version_seam` hashes the **exact bytes yielded by the stream** prior to trimming, lossy conversion, or newline stripping. Whitespace differences (e.g. trailing `\n` vs no `\n`, CRLF, leading spaces) produce distinct SHA-256 version hashes, guaranteeing bit-exact compatibility with legacy `get_tag_with_version` and ensuring optimistic concurrency checks in `delete_tag_conditional` remain completely accurate.

### 3.5 Bounded Stream Draining and Limit Semantics
`drain_tag_stream` enforces caller-provided limits without ambient memory or truncation hazards:
1. **Unbounded Reading (`max_payload_bytes = None`)**:
   - Explicitly imposes **no seam-level ceiling**.
   - Streams bytes via `read_to_end` directly into memory.
   - Preserves legacy `FsStorage` unbounded behavior.
   - Documented memory consideration: callers should supply limits when reading untrusted streams.
2. **Bounded Reading (`max_payload_bytes = Some(N)`)**:
   - Uses checked arithmetic: `limit.checked_add(1)`. If overflow occurs (e.g. `u64::MAX`), immediately returns `StorageError::Internal { kind: CorruptData, .. }`, matching the reviewed design.
   - Constrains stream reader using `tokio::io::AsyncReadExt::take(limit + 1)`.
   - Reads at most $N + 1$ bytes into a pre-sized buffer.
   - If buffer length exceeds $N$, immediately returns `StorageError::Internal { kind: CorruptData, .. }`.
3. **Zero-Limit (`Some(0)`) and Empty Payload Distinction**:
   - At the stream draining layer (`drain_tag_stream`), an empty stream (0 bytes) is successfully accepted, returning `Ok(Vec::new())`.
   - Non-empty streams (>0 bytes) are rejected by `drain_tag_stream` with `CorruptData`.
   - In subsequent caller API parsing, empty content returned by `drain_tag_stream` is handled according to each caller's established domain contract: `resolve_tag_seam` maps empty content to `StorageError::NotFound`, while `get_tag_with_version_seam` maps empty content to `StorageErrorKind::CorruptData`.
4. **Metadata Size Independence**:
   - Acquisition-time metadata `size` observation is treated as informational only.
   - It is not used for early rejection or pre-allocation, preventing false rejections if the underlying file changes size between descriptor acquisition and stream consumption.
5. **Partial Stream Failures**:
   - If an I/O error occurs while polling the payload stream, draining aborts immediately, propagating `StorageError::Internal { kind: Io, .. }`. No partial success is returned by either API.

### 3.6 Error Mapping Taxonomy
Seam reuses the shared `read_adapter::translate_payload_read_error` translator:
- `ReadError::NotFound`:
  - `resolve_tag_seam` maps to `StorageError::NotFound`.
  - `get_tag_with_version_seam` maps to `Ok(None)`.
- `ReadError::PermissionDenied` $\rightarrow$ `StorageError::Internal { kind: Io, .. }`.
- `ReadError::Backend` wrapping `storage_fs::FsMetadataError`:
  - `ResolutionRejected` (symlink, escape) $\rightarrow$ `Io`.
  - `UnsupportedObjectType` (directory, FIFO, socket) $\rightarrow$ `Io`.
  - `SyscallUnsupported` (`ENOSYS` on `openat2`) $\rightarrow$ `Configuration`.
  - `StatFailed` $\rightarrow$ `Io`.
  - `ProcfsReopenFailed` $\rightarrow$ `Io`.
  - `IdentityMismatch` $\rightarrow$ `Io`.
  - `InvalidMetadata` $\rightarrow$ `Io`.
  - `PlatformUnsupported` $\rightarrow$ `Io`.
  - `RuntimeMissing` $\rightarrow$ `Backend`.
  - `TaskJoinFailed` $\rightarrow$ `Backend`.
- Generic `ReadError::Backend`:
  - Non-FsMetadataError, non-`std::io::Error` sources (e.g. `CustomBackendError`) $\rightarrow$ `Io`.
  - Underlying `std::io::Error` source $\rightarrow$ `Io`.
  - Without source $\rightarrow$ `Io`.

*(Note: `ProbeDenied` and `ProbeFailed` are startup and capability-probing variants handled by `map_fs_startup_error`; they are not part of read-error translation and are not claimed as exercised in read-error tests).*

---

## 4. Concurrency, Advisory Locking, and Coherence Demarcation

1. **Advisory Locking Coexistence**:
   - Neither legacy `FsStorage::resolve_tag`/`get_tag_with_version` nor the contained seam acquires advisory locks (`.lock.{tag}`).
   - Advisory exclusive locks are acquired solely by mutating operations: `FsStorage::mutate_tag` and `FsStorage::delete_tag_conditional`.
   - Cooperating mutations synchronize via file-lock inodes. Contained tag reads operate locklessly and do not block or get blocked by mutation locks.
2. **Root Replacement and Coherence**:
   - In current production, tag reads and mutations resolve dynamic pathnames starting from `self.root`. However, separate operations can observe different trees if directory replacement occurs between them; pathname resolution provides no atomicity or coherence guarantee.
   - The contained seam resolves relative to the pinned `root_fd`. When `self.root` is replaced, the contained seam continues to read the original pinned directory hierarchy, while pathname mutations operate on the newly created directory tree.
   - Contained mutations and root-stability controls remain open design decisions prior to any production cutover.
3. **Concurrent Stream Modification**:
   - Concurrent modifications during stream draining can alter the bytes received. Containment does not guarantee atomic snapshots.
   - The SHA-256 version hash represents the actual bytes read by the stream, providing an accurate descriptor for optimistic concurrency checking at the time of reading.

---

## 5. Test Coverage and Verification Inventory

The test suite in `src/storage/fs/tag_seam.rs` contains 29 tests organized into unit tests (using `RecordingFakePayloadReader`) and integration tests (using real `FsStorage` and Linux `openat2`):

### 5.1 Test Inventory Table

| Test Identifier | Category | Purpose / Covered Invariant | Status |
| :--- | :--- | :--- | :--- |
| `test_tag_key_valid_single_and_nested` | Unit | Valid single-level and nested repository paths (`repos/repo/tags/tag`, `repos/a/b/c/tags/tag`) | PASSED |
| `test_tag_key_structural_rejections` | Unit | Structural rejection of traversal (`..`), empty components, slashes, backslashes, nulls | PASSED |
| `test_seam_key_validation_zero_reader_calls` | Unit | Confirms structural errors return `InvalidRepoName` with 0 reader calls | PASSED |
| `test_seam_resolve_and_get_tag_valid_sha256` | Unit | Valid SHA-256 digest resolution and version generation on both APIs | PASSED |
| `test_seam_resolve_and_get_tag_valid_sha512` | Unit | Valid SHA-512 digest resolution and version generation on both APIs | PASSED |
| `test_seam_resolve_and_get_tag_padded_whitespace` | Unit | Leading/trailing spaces, tabs, and newlines trimmed; valid digest parsed | PASSED |
| `test_seam_raw_byte_version_hash_whitespace_sensitivity` | Unit | Raw byte version hash differs when whitespace differs, matching legacy behavior | PASSED |
| `test_seam_missing_tag_taxonomy` | Unit | Missing tag returns `NotFound` for `resolve` and `Ok(None)` for `get_with_version` | PASSED |
| `test_seam_empty_file_taxonomy` | Unit | Empty file returns `NotFound` for `resolve` and `CorruptData` for `get_with_version` | PASSED |
| `test_seam_malformed_text_taxonomy` | Unit | Malformed text returns `NotFound` for `resolve` and `CorruptData` for `get_with_version` | PASSED |
| `test_seam_invalid_utf8_taxonomy` | Unit | Invalid UTF-8 returns `Io` for `resolve` and `CorruptData` for `get_with_version` | PASSED |
| `test_seam_unbounded_large_padded_payload` | Unit | Valid padded payload exceeding 64 KiB succeeds when `max_payload_bytes = None` | PASSED |
| `test_seam_limit_exact_and_one_over` | Unit | Limit accepts exact length, rejects one byte over as `CorruptData` | PASSED |
| `test_seam_limit_zero_behavior` | Unit | Limit `Some(0)` accepts empty stream; rejects non-empty stream as `CorruptData` | PASSED |
| `test_seam_limit_u64_max_overflow_rejection` | Unit | `max_payload_bytes = Some(u64::MAX)` rejects safely with `CorruptData` | PASSED |
| `test_seam_understated_metadata_valid_digest` | Unit | Understated metadata size does not cause false rejection; stream drained | PASSED |
| `test_seam_overstated_metadata_valid_digest` | Unit | Overstated metadata size does not cause false rejection; stream drained | PASSED |
| `test_seam_drain_understated_metadata_helper` | Unit | Direct helper test verifying understated metadata does not truncate stream | PASSED |
| `test_seam_stream_io_failure_partial` | Unit | Stream I/O error during drain returns `Io` without returning partial output for both APIs | PASSED |
| `test_seam_typed_fs_error_mappings` | Unit | Maps 14 error variants through both `resolve_tag_seam` and `get_tag_with_version_seam` (PermissionDenied, ResolutionRejected, UnsupportedObjectType, SyscallUnsupported, StatFailed, ProcfsReopenFailed, IdentityMismatch, InvalidMetadata, PlatformUnsupported, RuntimeMissing, TaskJoinFailed, CustomBackendError, Generic Backend with io::Error, Generic Backend without source) | PASSED |
| `test_seam_real_shared_reader_identity` | Integration | Verifies reader identity against production read adapter and executes both seam calls with shared reader | PASSED |
| `test_seam_real_contained_read_success` | Integration | End-to-end contained read and version calculation on disk | PASSED |
| `test_seam_real_final_symlink_rejected` | Integration | Final symlink pointing outside storage root rejected as `Io` on both APIs | PASSED |
| `test_seam_real_ancestor_symlink_rejected` | Integration | Intermediate directory symlink pointing outside root rejected as `Io` | PASSED |
| `test_seam_real_dangling_symlink_rejected` | Integration | Dangling symlink rejected as `Io` | PASSED |
| `test_seam_real_non_regular_rejected` | Integration | Directory in place of tag file rejected as `Io` | PASSED |
| `test_seam_real_path_traversal_rejected` | Integration | Traversal attempts in repo/tag names rejected with `InvalidRepoName` | PASSED |
| `test_seam_real_root_replacement_divergence_demonstrated` | Integration | Demonstrates seam observes pinned root while legacy observes replaced root | PASSED |
| `test_seam_real_permission_denied` | Integration | Explicitly ignored permission denial test restoring mode in `Drop` | IGNORED (env) |

### 5.2 Test Execution Summary
- **Total tests**: 29
- **Passed**: 28
- **Failed**: 0
- **Ignored**: 1 (`test_seam_real_permission_denied`, explicitly ignored unless executed under unprivileged non-root user where `chmod 0o000` denies read).

---

## 6. Verification Results

### 6.1 Tool Execution Results
1. **`cargo fmt --check`**: Exit code `0`.
2. **`cargo clippy --locked --all-targets -- -D warnings`**: Exit code `0`.
3. **`cargo test --locked --lib storage::fs::tag_seam`**: Exit code `0` (28 passed, 0 failed, 1 ignored).
4. **`cargo test --locked --lib storage::fs::tests::test_tag_`**: Exit code `0` (10 passed, 0 failed, 1 ignored).
5. **`git diff --check`**: Exit code `0`.

### 6.2 Source File Hashes
- **`src/storage/fs.rs`**:
  - Baseline SHA-256: `1b7a234e35471a700a19892012367ee2547c674c416f79268ed2f40335347845`
  - Current SHA-256: `6d05e4802b5519ee69118c731028e06ca489dcc381fbc786b7c0712ad9406a62`
  - Diff: Adds exclusively `#[cfg(test)] #[path = "fs/tag_seam.rs"] mod tag_seam;`.
- **`src/storage/fs/tag_seam.rs`**:
  - Current SHA-256: `b3aa431cefa4b1b1bcd8ded2e7a12583d157225f8c5b35f31ff7ab7c9c2ce815`
- **`docs/architecture/filesystem-tag-read-contained-integration-design.md`**:
  - Preserved unchanged SHA-256: `53f27a526ab781321782ce334d5e78b61a26c58a1e03a8b7f385f1a977a79728`
- **`docs/architecture/filesystem-tag-read-characterization.md`**:
  - Preserved unchanged SHA-256: `502d3ccf122ec6bea3af19a4370fae24e1fa53b8ac485fc520075a7e84b4cad8`
- **`docs/architecture/filesystem-tag-read-contained-seam.md`**:
  - Implementation assessment document.

---

## 7. Open Quality Gates and Next Steps

1. **Gate O-03 (Repository Path Grammar)**:
   Seam preserves nested repository components while rejecting empty/relative components. OCI grammar alignment remains an independent policy decision.
2. **Gate O-04 (Tag Payload Ceiling & Memory Policy)**:
   Seam preserves unbounded legacy reading by default. The introduction of an active production ceiling requires an operational decision regarding acceptable tag file size.
3. **Gate O-05 (Production Cutover)**:
   Production cutover requires approving the divergence behavior when storage root directory replacement occurs and wiring the seam into `FsStorage::resolve_tag` and `FsStorage::get_tag_with_version`.
4. **Gate O-06 & O-13 (Locking & Root Coherence)**:
   Contained mutation operations and lock containment must be addressed before unified read/write descriptor containment can be established.
