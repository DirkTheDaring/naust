> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Contained manifest reads landed (`1924225`); manifests later moved onto `manifest_domain` (`76209a9`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Filesystem Manifest Read Integration Assessment & Compatibility Record

**Repository:** `registry-rust`
**Scope:** Test-only integration seam evaluating `storage-fs` payload reader facilities (`ObjectPayloadReader::open_payload`) against `registry-rust` manifest read operations (`head_manifest_seam`, `get_manifest_seam`), media-type detection, error taxonomy, and containment boundaries.
**Slice:** Bounded Slice (Test-Only Contained Manifest-Read Integration Seam). Production manifest reads are **not** cut over; `storage-layer-rust` remains strictly read-only.

---

## 1. Actual Call Chain and Ownership Boundaries

The test seam establishes an explicit, contained integration boundary between registry manifest read semantics and storage-layer descriptor containment. In this slice, the seam is invoked directly by unit and integration tests (`src/storage/fs/manifest_seam.rs`). It is **not** wired into the `ManifestReader` port trait or application services (`ManifestReadService`), and production `FsStorage` methods remain completely untouched.

```text
[Unit & Integration Tests (THIS SLICE)]
  src/storage/fs/manifest_seam.rs: tests
        |
        v
[Test-Only Contained Seam]
  src/storage/fs/manifest_seam.rs: head_manifest_seam, get_manifest_seam
    - Pre-composition key validation: rejects traversal, dot/dot-dot, empty segments
    - Composes relative key: repos/<repo>/manifests/<digest.hex()>
    - Parses ObjectKey according to storage-core contract
    - Invokes ObjectPayloadReader::open_payload(&key)
    - Drains payload stream into memory once per invocation
    - Derives ManifestMeta.size from actual bytes read (not acquisition metadata)
    - Detects mediaType from JSON body with OCI default fallback
    - Delegates error mapping to read_adapter::translate_payload_read_error
        |
        v
[Storage Abstraction & Implementation Layers]
  storage-core: ObjectPayloadReader trait
        |
        v
  storage-fs: FsMetadataReader
    - Pinned root directory file descriptor
    - Descriptor-relative openat2 resolution with flags:
      RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS
    - Phase 1 O_PATH descriptor acquisition and validation
    - Phase 2 /proc/self/fd/<phase1_fd> reopening with O_RDONLY | O_CLOEXEC
    - Rechecks regular-file type, st_dev/st_ino identity, and size
    - Rejects non-regular file objects and symlinks beneath the pinned descriptor
```

Production `FsStorage::head_manifest` and `FsStorage::get_manifest` in `src/storage/fs.rs` remain completely uncontained and continue to use legacy pathname resolution via `tokio::fs::read`.

### Separation of Responsibilities

| Responsibility Area | Owning Component | Specific Rules & Contracts |
|---|---|---|
| **Pre-Composition Key Validation** | `registry-rust` (`manifest_seam`) | Explicitly checks repository string before key composition: rejects empty strings, leading/trailing `/`, backslashes, control and NUL characters, empty path segments (`//`), `.` segments, and `..` traversal segments with `StorageError::InvalidRepoName`. Does not silently normalize unsafe input. |
| **Manifest ObjectKey Construction** | `registry-rust` (`manifest_seam`) | Composes relative path `repos/<repository>/manifests/<digest.hex()>`. Supported digest algorithms (`sha256`, `sha512`) use the raw hex representation without algorithm prefix. Parses into `storage_core::ObjectKey`. |
| **Contained Descriptor Resolution** | `storage-fs` (`FsMetadataReader`) | Pinned directory descriptor; descriptor-relative `openat2` resolution preventing path escapes and rejecting symlinks beneath the pinned descriptor (`RESOLVE_BENEATH \| RESOLVE_NO_SYMLINKS \| RESOLVE_NO_MAGICLINKS`). |
| **Phase 2 Descriptor Reopening** | `storage-fs` (`FsMetadataReader`) | Opens `/proc/self/fd/<phase1_fd>` with `O_RDONLY \| O_CLOEXEC` to obtain a readable descriptor, rechecking regular-file type, `st_dev`/`st_ino` identity, and size. |
| **Payload Stream Acquisition** | `storage-core` / `storage-fs` | `ObjectPayloadReader::open_payload(&key)` returns `ObjectPayload` containing acquisition metadata and `ObjectStream`. Rejects non-regular files (`UnsupportedObjectType`) and symlinks (`ResolutionRejected`). |
| **Payload Consumption & Buffering** | `registry-rust` (`manifest_seam`) | Opens payload once per invocation; consumes the complete payload stream asynchronously into a byte buffer; returns mid-stream I/O errors immediately rather than partial content. |
| **Size & Media-Type Derivation** | `registry-rust` (`manifest_seam`) | Sets `ManifestMeta.size` strictly from the length of consumed bytes, never trusting pre-read acquisition metadata. Parses JSON body to extract top-level `"mediaType"`. Falls back to `"application/vnd.oci.image.manifest.v1+json"` for missing, non-string, or JSON scalars. Returns `StorageErrorKind::CorruptData` on 0-byte or malformed non-JSON payloads. Preserves absence of digest-content verification. |
| **Error Translation Delegation** | `registry-rust` (`manifest_seam`) | Delegates all `storage_core::ReadError` translation directly to `super::read_adapter::translate_payload_read_error`. Does not implement a separate or divergent translator. |

---

## 2. Comprehensive Compatibility & Differences Record

The table below contrasts legacy production manifest read behavior with the completed test seam behavior:

| Scenario / Behavior Aspect | Legacy Behavior (`FsStorage`) | Completed Seam Behavior (`manifest_seam`) | Verification Evidence | Cutover Decision & Rationale |
|---|---|---|---|---|
| **Representative Valid OCI Manifest** | Reads file via `tokio::fs::read`; extracts `mediaType`; derives size from bytes read. | Reads via `open_payload`; extracts `mediaType`; derives size from bytes read; returns identical `ManifestMeta` and byte-for-byte payload. | `test_manifest_read_representative_valid_oci_manifest`, `test_real_fs_representative_valid_manifests_and_nested_repos` | **PRESERVED**: Exact functional parity for valid manifests. |
| **Nested Repository Names** (e.g. `library/ubuntu`) | Resolves pathname `repos/library/ubuntu/manifests/<hex>`. | Validates segments, constructs `ObjectKey`, resolves beneath pinned root. | `test_manifest_read_repository_naming_single_and_multisegment`, `test_real_fs_representative_valid_manifests_and_nested_repos` | **PRESERVED**: Multi-segment repositories supported cleanly. |
| **Supported Digest Algorithms** (`sha256` and `sha512`) | Stores file at `<digest.hex()>` without algorithm prefix. | Key composed with `<digest.hex()>`; both 64-hex and 128-hex supported. | `test_manifest_read_supported_digest_algorithms_and_filename_forms`, `test_real_fs_supported_digest_algorithms` | **PRESERVED**: Unprefixed hex layout preserved across algorithms. |
| **Explicit Custom Media Type** | Preserves string `mediaType` from JSON body. | Preserves string `mediaType` from JSON body. | `test_manifest_read_media_type_detection_variants`, `test_recording_fake_media_type_variants_and_corrupt_data`, `test_parity_with_fs_storage_detect_manifest_media_type` | **PRESERVED**: Exact custom media types preserved; 100% parity verified. |
| **Missing / Non-String / Scalar Media Type** | Falls back to `"application/vnd.oci.image.manifest.v1+json"`. | Falls back to `"application/vnd.oci.image.manifest.v1+json"`. | `test_manifest_read_media_type_detection_variants`, `test_recording_fake_media_type_variants_and_corrupt_data`, `test_parity_with_fs_storage_detect_manifest_media_type` | **PRESERVED**: Standard OCI default fallback maintained; 100% parity verified. |
| **Empty File (0 bytes)** | `serde_json::from_slice` returns EOF error, mapped to `CorruptData`. | Consumes 0 bytes; JSON parse fails with `CorruptData`. | `test_manifest_read_empty_and_malformed_payloads_classify_as_corrupt_data`, `test_recording_fake_media_type_variants_and_corrupt_data`, `test_parity_with_fs_storage_detect_manifest_media_type` | **PRESERVED**: Fails closed as corrupted data with matching diagnostic. |
| **Malformed Non-JSON Content** | Fails JSON parse with `StorageErrorKind::CorruptData`. | Fails JSON parse with `StorageErrorKind::CorruptData`. | `test_manifest_read_empty_and_malformed_payloads_classify_as_corrupt_data`, `test_recording_fake_media_type_variants_and_corrupt_data`, `test_parity_with_fs_storage_detect_manifest_media_type` | **PRESERVED**: Fails closed as corrupted data with matching diagnostic. |
| **Absent Manifest File or Repo Dir** | `tokio::fs::read` fails with `NotFound`; returns `StorageError::NotFound`. | `open_payload` fails with `ReadError::NotFound`; translated to `StorageError::NotFound`. Tested through both HEAD and GET; exactly one open call asserted per execution. | `test_manifest_read_missing_paths_return_not_found`, `test_real_fs_missing_manifest_and_missing_repo`, `test_recording_fake_acquisition_failures_suppress_stream_and_fallback` | **PRESERVED**: Exact `NotFound` error taxonomy match. |
| **Digest Content Verification** | Does not check payload bytes against requested digest. | Does not check payload bytes against requested digest. | `test_real_fs_supported_digest_algorithms`, `test_recording_fake_size_derived_from_consumed_bytes_not_metadata` | **PRESERVED**: Preserves existing omission of inline digest validation. |
| **Single Open & Stream Consumption** | Reads file into memory via `tokio::fs::read`. | Opens payload exactly once per helper call; full stream consumption. | `test_recording_fake_exact_key_and_single_open_call` | **PRESERVED**: No redundant opens or probes. |
| **Size from Stream, Not Metadata** | Derived from `bytes.len() as u64`. | Derived from `bytes.len() as u64` even if acquisition metadata reports a different size. | `test_recording_fake_size_derived_from_consumed_bytes_not_metadata` | **PRESERVED**: Stream byte length is the authoritative source of size. |
| **Mid-Stream I/O Failure** | `tokio::fs::read` is asynchronous at its API boundary and is not guaranteed to execute in a single read syscall; mid-stream errors produce `StorageErrorKind::Io`. | Fails immediately with `StorageErrorKind::Io`; injected diagnostic survives translation; stream-failure test covers both HEAD and GET with complete valid JSON prefix. | `test_recording_fake_mid_stream_io_failure` | **PRESERVED / ROBUST**: Seam consumes through completion rather than accepting early valid prefix; diagnostics preserved. |
| **Unsafe Repo Input (`..`, `.`, leading `/`)** | **VULNERABILITY**: Accepts raw string; `manifest_path` joins raw string; reads outside root if target exists. | **CONTAINMENT ADVANCEMENT**: Explicitly rejected before invoking reader; returns `StorageError::InvalidRepoName`. Reader invocation suppressed (0 calls). | `test_manifest_read_unvalidated_caller_path_traversal_gap` (legacy gap), `test_manifest_key_rejects_unsafe_inputs`, `test_recording_fake_unsafe_input_suppresses_reader_invocation` | **PROPOSED DIFFERENCE**: Direct callers supplying dot-dot or illegal characters fail closed with `InvalidRepoName`. |
| **Symlink Traversal (Ancestor or Target)** | **VULNERABILITY**: Transparently follows symlinks pointing anywhere in the filesystem. | **CONTAINMENT ADVANCEMENT**: Fails closed with `ResolutionRejected` -> mapped to `StorageErrorKind::Io`. ResolutionRejected acquisition is explicitly tested through HEAD; reader never reads outside root. | `test_manifest_read_containment_symlink_traversal` (legacy gap), `test_real_fs_symlinks_rejected_without_reading_outside_content`, `test_recording_fake_acquisition_failures_suppress_stream_and_fallback` | **PROPOSED DIFFERENCE**: Symlinks are rejected by descriptor-relative `openat2`. |
| **Directory Substituted for Manifest** | Fails with `StorageErrorKind::Io` (`EISDIR`). | Fails with `UnsupportedObjectType` -> mapped to `StorageErrorKind::Io`. | `test_manifest_read_nondirectory_components_return_io`, `test_real_fs_directory_substituted_for_manifest_rejected` | **PRESERVED**: Both fail with `StorageErrorKind::Io`. |
| **Unreadable Permissions (`0o000`)** | Fails with `StorageErrorKind::Io` (`EACCES`). | `open_payload` fails with `ReadError::PermissionDenied` -> mapped to `StorageErrorKind::Io`. Tested through both HEAD and GET in recording fake; tested with `-- --ignored` in real fs. | `test_manifest_read_permission_denied_ignored`, `test_real_fs_permission_denied_ignored`, `test_recording_fake_acquisition_failures_suppress_stream_and_fallback` | **PRESERVED**: Both classify permission denial under `StorageErrorKind::Io`. |
| **Worker Thread Join Failure** | Not applicable to legacy read. | Fails closed with `StorageErrorKind::Backend` (distinguished from storage I/O). | `test_recording_fake_typed_runtime_and_task_failures` | **PROPOSED DIFFERENCE**: Surfaces internal execution failure without masquerading as filesystem I/O. |
| **Missing Async Runtime Handle** | Not applicable to legacy read. | Fails closed with `StorageErrorKind::Backend`. | `test_recording_fake_typed_runtime_and_task_failures` | **PROPOSED DIFFERENCE**: Correctly categorizes environment setup failure. |
| **Pinned Root Descriptor Across Rename** | Vulnerable to TOCTOU if root directory pathname is replaced. | Pinned directory file descriptor retains access to original inode even after directory is renamed on disk. | `test_real_fs_pinned_root_across_rename` | **CONTAINMENT ADVANCEMENT**: Inherited from `storage-fs`. |

---

## 3. Detailed Analysis of Proposed Differences & Contracts

### 3.1 Pre-Composition Key Validation vs Canonical OCI Repository Names

The test seam introduces strict pre-composition validation in `manifest_key(repo: &str, digest: &Digest)`:
- Rejects empty repository strings.
- Rejects leading or trailing `/`.
- Rejects backslash (`\`), control characters, and NUL bytes.
- Rejects empty path segments (consecutive slashes `//`).
- Rejects `.` (current directory) segments.
- Rejects `..` (parent directory traversal) segments.

**Distinction from CanonicalRepoName:**
- `CanonicalRepoName` enforces OCI distribution specification compliance (e.g. lowercase characters only, maximum 255 bytes, strictly defined namespace separators).
- `manifest_key` does **not** assert that a repository name is canonically valid under OCI rules. It strictly enforces that the input is safe for composition into an `ObjectKey` without path escapes or directory hierarchy manipulation.
- Direct storage callers passing non-canonical names that are safe relative paths (e.g. uppercase names in test fixtures) are accepted by `ObjectKey`, whereas traversal sequences (`../`) are rejected with `StorageError::InvalidRepoName`.
- This resolves the legacy traversal vulnerability while maintaining clear architectural separation between application-layer OCI validation and storage-layer key construction.

### 3.2 Symlink Rejection Under Contained Resolution

In legacy production code, `tokio::fs::read` executes standard OS pathname resolution. If an operator or attacker places a symlink inside `repos/<repo>/manifests/` pointing to `/etc/passwd` or an external volume, the file is read and parsed.

Under the contained seam:
- `storage-fs` opens files relative to the pinned root directory descriptor using `openat2` with flags:
  `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`.
- If any component of the path (whether an ancestor repository directory or the final manifest filename) is a symlink, the kernel rejects the open with `EXDEV` or `ELOOP`.
- `storage-fs` categorizes this as `FsMetadataError::ResolutionRejected`.
- The seam maps this to `StorageErrorKind::Io`, failing closed without reading external data.
- **Containment Scope Boundary:** This protection relies strictly on descriptor-relative resolution constraints enforced by `openat2`. It does not provide mount namespace isolation or prevent access to hard links created inside the repository pointing to external inodes before runtime.

### 3.3 Media-Type Detection Parity & Test-Only Duplicate

`src/storage/fs/manifest_seam.rs` retains a test-only duplicate of `detect_manifest_media_type`:
- **Rationale:** The production helper `FsStorage::detect_manifest_media_type` is defined as an async instance method (`async fn detect_manifest_media_type(&self, bytes: &[u8]) -> Result<String, StorageError>`) requiring an instantiated `&FsStorage`. Modifying production methods in `src/storage/fs.rs` (e.g. converting to an associated function or moving to a shared utility) is strictly outside the authorized scope of this test-only slice.
- **Verification of Parity:** Full functional and diagnostic parity between `manifest_seam::detect_manifest_media_type` and `FsStorage::detect_manifest_media_type` is verified directly by test `test_parity_with_fs_storage_detect_manifest_media_type` across custom media types, standard schema versions, missing `mediaType`, non-string values, scalar JSON, empty files (0 bytes), and malformed non-JSON payloads. Both successful values and error diagnostics match identically.

### 3.4 Actual Error Translation Taxonomy (Delegation to `read_adapter.rs`)

The test seam does **not** implement a custom error translator. It delegates directly to [`super::read_adapter::translate_payload_read_error`](src/storage/fs/read_adapter.rs#L176-L178), which invokes `translate_read_error(err, ReadOp::Payload)` (`read_adapter.rs:93-168`).

The table below reflects the exact, source-checked mapping implemented in `read_adapter.rs`:

| Incoming `storage_core::ReadError` Variant | Inner Source Condition | Resulting `StorageError` Variant | Resulting `StorageErrorKind` / Value | Source Reference (`read_adapter.rs`) |
|---|---|---|---|---|
| `ReadError::NotFound { key }` | Any / None | `StorageError::NotFound` | `NotFound` | Lines 95 |
| `ReadError::PermissionDenied { source, .. }` | `source: Some(io_err)` (`std::io::Error`) | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | Lines 98–100 |
| `ReadError::PermissionDenied { source, .. }` | `source: Some(src)` (non-IO error) | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | Line 101 |
| `ReadError::PermissionDenied { source: None, .. }` | Absent source (`None`) | `StorageError::Internal { kind: Io, message: "permission denied" }` | `StorageErrorKind::Io` | Line 103 |
| `ReadError::Backend { source, .. }` | `source: Some(FsMetadataError::ResolutionRejected { source, .. })` | `StorageError::Internal { kind: Io, message: source.to_string() }` | `StorageErrorKind::Io` | Lines 114–116 |
| `ReadError::Backend { source, .. }` | `source: Some(FsMetadataError::UnsupportedObjectType { mode, .. })` | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | Lines 117–121 |
| `ReadError::Backend { source, .. }` | `source: Some(FsMetadataError::SyscallUnsupported(io_err))` | `StorageError::Internal { kind: Configuration, message }` | `StorageErrorKind::Configuration` | Lines 122–126 |
| `ReadError::Backend { source, .. }` | `source: Some(FsMetadataError::StatFailed { stage, source })` | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | Lines 127–131 |
| `ReadError::Backend { source, .. }` | `source: Some(FsMetadataError::ProcfsReopenFailed { source })` | `StorageError::Internal { kind: Io, message }` | `StorageErrorKind::Io` | Lines 132–136 |
| `ReadError::Backend { source, .. }` | `source: Some(FsMetadataError::IdentityMismatch { .. })` | `StorageError::Internal { kind: Io, message: fs_err.to_string() }` | `StorageErrorKind::Io` | Lines 137–139 |
| `ReadError::Backend { source, .. }` | `source: Some(FsMetadataError::InvalidMetadata { .. })` | `StorageError::Internal { kind: Io, message: fs_err.to_string() }` | `StorageErrorKind::Io` | Lines 140–142 |
| `ReadError::Backend { source, .. }` | `source: Some(FsMetadataError::PlatformUnsupported)` | `StorageError::Internal { kind: Io, message: fs_err.to_string() }` | `StorageErrorKind::Io` | Lines 143–145 |
| `ReadError::Backend { source, .. }` | `source: Some(FsMetadataError::RuntimeMissing(_))` | `StorageError::Internal { kind: Backend, message: fs_err.to_string() }` | `StorageErrorKind::Backend` | Lines 146–148 |
| `ReadError::Backend { source, .. }` | `source: Some(FsMetadataError::TaskJoinFailed(_))` | `StorageError::Internal { kind: Backend, message: fs_err.to_string() }` | `StorageErrorKind::Backend` | Lines 149–151 |
| `ReadError::Backend { source, .. }` | `source: Some(other FsMetadataError)` | `StorageError::Internal { kind: Io, message: fs_err.to_string() }` | `StorageErrorKind::Io` | Line 152 |
| `ReadError::Backend { source, .. }` | `source: Some(io_err)` (`std::io::Error`, ordinary I/O) | `StorageError::Internal { kind: Io, message: io_err.to_string() }` | `StorageErrorKind::Io` | Lines 154–155 |
| `ReadError::Backend { source, .. }` | `source: Some(src)` (unknown boxed source) | `StorageError::Internal { kind: Io, message: src.to_string() }` | `StorageErrorKind::Io` | Lines 156–157 |
| `ReadError::Backend { message, source: None, .. }` | Absent source (`None`) | `StorageError::Internal { kind: Io, message: message.clone() }` | `StorageErrorKind::Io` | Lines 159–160 |
| Non-exhaustive `ReadError` variant (`_`) | Any / None | `StorageError::Internal { kind: Io, message: "unknown storage payload read failure" }` | `StorageErrorKind::Io` | Lines 163–166 |

**Key Mapping Observations:**
1. `NotFound` is preserved exactly without downcasting.
2. Resolution rejections, unsupported object types, permission denials, and missing-procfs failures surface as `StorageErrorKind::Io`.
3. Internal runtime dispatch failures (`TaskJoinFailed`, `RuntimeMissing`) surface as `StorageErrorKind::Backend`, distinguishing runtime execution faults from storage I/O errors.
4. Missing kernel system call support (`SyscallUnsupported`) surfaces as `StorageErrorKind::Configuration`.
5. Non-Linux `PlatformUnsupported` during read maps to `StorageErrorKind::Io` (in contrast to startup initialization where `map_fs_startup_error` maps it to `Configuration`).

---

## 4. Resource Utilization & Operational Considerations

### 4.1 Full-Read Memory Buffering
Both `head_manifest_seam` and `get_manifest_seam` read the complete manifest payload stream into a contiguous memory buffer (`Vec<u8>`):
- **Why HEAD must read the payload:** Manifest media types are not encoded in filenames or filesystem extended attributes. Determining the media type requires parsing the JSON document. Detecting structural corruption (empty files or invalid JSON) requires reading and parsing the content.
- **Resource Concern:** Manifests are buffered into memory without an upper size bound. A maliciously crafted or abnormal manifest file would be buffered entirely into memory, creating a potential memory-exhaustion denial-of-service vector.
- **Slice Scope:** This slice records unbounded buffering as an unresolved resource concern without introducing a new size limit or streaming response contract.

### 4.2 Lack of Snapshot Isolation
Filesystem operations do not provide transactional snapshot isolation:
- Manifest files are immutable by convention, but the filesystem does not prevent external processes from concurrently modifying or replacing files.
- Payload acquisition and stream consumption do not form an atomic snapshot. Concurrent writes or truncation may produce changed or mixed bytes, valid JSON, invalid JSON, or an I/O error. Neither an I/O error nor CorruptData is guaranteed. Separate HEAD and GET calls may observe different content.

### 4.3 Platform and Procfs Assumptions
The contained reader inherits platform assumptions from `storage-fs`:
- **Linux Dependency:** Strict path containment relies on Linux `openat2` (kernel >= 5.6) with `RESOLVE_BENEATH`.
- **Phase 2 Procfs Reopening:** Phase 1 acquires an `O_PATH | O_CLOEXEC` descriptor and validates the object type. Phase 2 opens `/proc/self/fd/<phase1_fd>` with `O_RDONLY | O_CLOEXEC` to obtain a readable descriptor, then rechecks regular-file type, `st_dev`/`st_ino` identity, and size. This relies on the documented genuine, accessible, stable procfs assumption.
- **Distinct Error Mappings:** Non-Linux `PlatformUnsupported` (mapped to `Io` during read) and missing-procfs (`ProcfsReopenFailed`, mapped to `Io`) are distinct from `SyscallUnsupported` (mapped to `Configuration`).
- **Non-Linux Status:** Compilation and execution on non-Linux operating systems remain explicitly unverified.

---

## 5. Scope Exclusions & Deferrals

The following areas are intentionally excluded from this slice:
1. **Manifest Listing (`list_manifest_digests_page`):**
   Manifest listing involves directory enumeration over `repos/<repo>/manifests/`, error suppression on missing directories, pagination cursors, and sorting. This requires a dedicated characterization and design slice similar to CAS listing.
2. **Mutation Containment (`put_manifest`, `delete_manifest`, `mutate_tag`):**
   Contained writes, atomic tempfile creation, durability syncing (`fsync`), and parent directory creation are governed by Gate O-04 and remain outside read extraction.
3. **Production Cutover & Routing:**
   `FsStorage::head_manifest` and `FsStorage::get_manifest` in `src/storage/fs.rs` remain completely untouched. No production traffic routes through the seam in this slice.
4. **Mandatory Size Limit Prerequisite:**
   Evaluating whether an application-level or transport-level manifest size policy is appropriate remains an open operational question; it is not established as a mandatory prerequisite in this slice.

---

## 6. Concrete Decisions Required Prior to Production Cutover

Before authorizing a production cutover of filesystem manifest reads, the following decisions must be resolved:

1. **Acceptance of Symlink Rejection:**
   Confirm that rejecting symlinks in `repos/<repository>/manifests/` is accepted, ending legacy behavior that followed symlinks outside the repository root.
2. **Acceptance of Unsafe Path Rejection:**
   Confirm that direct storage calls passing repository strings containing `..` or illegal characters will fail with `StorageError::InvalidRepoName` rather than attempting uncontained filesystem traversal.
3. **Operational Manifest Buffering Policy:**
   Evaluate whether a maximum manifest read limit is necessary to protect against memory exhaustion from rogue or oversized files.
4. **Runtime Failure Error Mapping:**
   Approve mapping `RuntimeMissing` and `TaskJoinFailed` to `StorageErrorKind::Backend` rather than generic `Io`.
5. **Shared Reader Lifecycle Integration:**
   A later production cutover should reuse the existing shared reader (`Arc<FsMetadataReader>`) and startup boundary already established for CAS blob reads and directory listing in `FsStorage`.

---

## 7. Canonical Quality Gate Status

All canonical quality gates remain explicitly **OPEN**:

- **O-03: Key and continuation-token contracts** — OPEN. Key validation rules documented; continuation tokens remain uncertified.
- **O-04: Filesystem write durability and containment** — OPEN. Manifest mutations and tempfile containment remain unextracted.
- **O-05: Broader filesystem read containment** — OPEN. Manifest reads characterized and verified in test seam; production cutover not yet authorized.
- **O-06: Typed AWS mapping and pinned-MinIO evidence** — OPEN. S3/MinIO backend verification is independent of this slice.
- **O-13: Hosting, distribution, and release strategy** — OPEN. Distribution boundaries remain uncertified.
- **O-15: Non-Linux verification** — OPEN. Contained reads require Linux `openat2`; non-Linux platforms remain explicitly unverified.
- **O-16: Earlier Slice 11 audit/test-inventory evidence** — OPEN. Audit trails preserved.
- **D-06: Broader extraction, cutover, compatibility, and distribution acceptance** — OPEN. Overall milestone acceptance pending.
