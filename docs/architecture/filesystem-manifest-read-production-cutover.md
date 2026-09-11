# Production Filesystem Manifest Read Cutover Implementation Record

## 1. Executive Summary & Cutover Scope

This document records the production cutover of filesystem manifest reads (`head_manifest` and `get_manifest`) in `registry-rust` from legacy pathname-based reads (`tokio::fs::read`) to descriptor-relative contained reads via the shared `storage_fs::FsMetadataReader` instance.

### Cutover Scope Summary:
- **Production Delegation**: `FsStorage::head_manifest` and `FsStorage::get_manifest` now delegate to `manifest::head_manifest_impl` and `manifest::get_manifest_impl`.
- **Shared Reader Reuse**: Delegates directly through `self.reader.as_ref()`. Reuses the existing `Arc<FsMetadataReader>` pinned root file descriptor without opening a second reader or reopening the root directory per request.
- **Module Promotion**: Promoted `src/storage/fs/manifest_seam.rs` to production module `src/storage/fs/manifest.rs` (`pub(crate) mod manifest;`) and removed the obsolete test seam file.
- **Media-Type Helper Consolidation**: Single synchronous source of truth `manifest::detect_manifest_media_type` in `src/storage/fs/manifest.rs`. `FsStorage::detect_manifest_media_type` preserved as a thin facade delegating to it, ensuring `put_manifest` and external callers retain exact behavior.
- **Strict Pre-Composition Validation**: Validates caller repository strings against traversal sequences (`..`), dot segments (`.`), leading/trailing slashes, repeated slashes, backslashes, NUL bytes, and ASCII control characters with `StorageError::InvalidRepoName` before composing `repos/<repo>/manifests/<digest.hex()>`. Safe non-canonical inputs on Linux such as `C:/repo` remain accepted after composition.
- **Descriptor-Relative Kernel Containment**: Manifest reads resolve through `openat2` with flags `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`. All symlinks (external, internal, ancestor, dangling) fail closed with `StorageErrorKind::Io`. Non-regular files fail closed with `StorageErrorKind::Io` (`UnsupportedObjectType`).
- **Preserved Boundaries**: Blob reads, CAS listing, and startup offload are preserved untouched. Manifest mutation operations (`put_manifest`, `delete_manifest`, `set_tag`) remain unchanged.
- **Quality Gates**: All 8 canonical quality gates (`O-03, O-04, O-05, O-06, O-13, O-15, O-16, D-06`) remain explicitly **OPEN**.

---

## 2. Production Call Flow & Shared Reader Ownership

### 2.1 Call Graph
```text
Client Request / HTTP Layer / Downstream Caller
  │
  ├─► ManifestReader::head_manifest / get_manifest (Trait Port)
  │     │
  │     ▼
  └─► FsStorage::head_manifest / get_manifest (src/storage/fs.rs)
        │
        ▼
      manifest::head_manifest_impl / get_manifest_impl (src/storage/fs/manifest.rs)
        │
        ├─► manifest_key(repo, digest) -> Result<ObjectKey, StorageError>
        │     - Validates repo: no .., ., leading/trailing /, //, \, NUL, controls
        │     - Composes "repos/<repo>/manifests/<digest.hex()>"
        │     - ObjectKey::parse validates relative key safety
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
        │     - Consumes complete payload into memory buffer
        │     - Stream read errors mapped to StorageErrorKind::Io
        │
        ├─► manifest::detect_manifest_media_type(&bytes)
        │     - Synchronous JSON parse
        │     - Extracts top-level string "mediaType" or defaults to OCI manifest v1
        │     - Empty / non-JSON payloads map to StorageErrorKind::CorruptData
        │
        └─► Return ManifestMeta { size: bytes.len(), media_type } (+ bytes for GET)
```

### 2.2 Shared Reader Reuse Guarantees
1. **Zero New Root Descriptors**: `FsStorage::head_manifest` and `FsStorage::get_manifest` borrow `self.reader.as_ref()`. The pinned root directory descriptor acquired during `FsStorage::try_new` is reused across all read operations (blobs, CAS listing, and manifests).
2. **Per-Call File Descriptors**: Reusing the root reader avoids reopening the storage root directory; each individual manifest read operation continues to acquire fresh per-call file descriptors (`openat2` Phase 1 descriptor and Phase 2 readable descriptor via `/proc/self/fd/<phase1_fd>`), managed with RAII ownership (`OwnedFd` and `std::fs::File`).
3. **Preservation of Startup Offload**: `FsStorage::try_new` synchronous construction and its asynchronous offload boundary (`tokio::task::spawn_blocking` in `src/storage/mod.rs:937`, `src/runtime.rs`, and `src/cli/runtime.rs`) remain completely unchanged.
4. **No Pathname Fallback**: If payload acquisition fails (e.g. `NotFound`, `PermissionDenied`, `ResolutionRejected`), the error is translated immediately through `translate_payload_read_error`. The implementation never falls back to uncontained path resolution (`tokio::fs::read`).

---

## 3. Single Consolidated Media-Type Implementation & Preserved Write Behavior

### 3.1 Single Source of Truth
`manifest::detect_manifest_media_type(bytes: &[u8]) -> Result<String, StorageError>` in `src/storage/fs/manifest.rs` is now the single implementation of manifest media-type parsing:
- Parses JSON looking for a top-level string `"mediaType"`.
- If present and string: returns the specified media type.
- If missing, non-string, or scalar JSON: defaults to `"application/vnd.oci.image.manifest.v1+json"`.
- If empty (0 bytes) or malformed non-JSON: returns `StorageErrorKind::CorruptData`.

### 3.2 Preserved Write Behavior
- In `src/storage/fs.rs`, `FsStorage::detect_manifest_media_type` is preserved as an asynchronous instance method delegating directly to `manifest::detect_manifest_media_type(bytes)`.
- `put_manifest` (`src/storage/fs.rs:889`) continues to invoke `self.detect_manifest_media_type(&bytes).await?` unchanged.
- Write ordering, directory creation (`ensure_dir`), and atomic file writes (`atomic_write_file`) remain completely untouched.

---

## 4. Accepted Path Validation, Symlink Containment, and Error Differences

| Aspect | Legacy Pathname Behavior (`tokio::fs::read`) | Production Contained Behavior (`FsMetadataReader`) | Rationale / Security Benefit |
| :--- | :--- | :--- | :--- |
| **Path Traversal (`..`)** | Unvalidated `Path::join` allowed escaping the storage root if target existed. | `manifest_key` rejects `..` segments before key parsing with `StorageError::InvalidRepoName`. | Prevents directory traversal attacks escaping repository boundaries. |
| **Dot Segments (`.`)** | Normalizes / skips segment during path resolution. | `manifest_key` rejects `.` segments with `StorageError::InvalidRepoName`. | Enforces canonical non-normalized segment structure. |
| **Leading / Trailing `/`** | Resolves as absolute path (ignoring prefix) or directory. | Rejected with `StorageError::InvalidRepoName`. | Prevents root-escape and directory confusion. |
| **Backslashes (`\`)** | Treated as literal filename character on Linux. | Rejected with `StorageError::InvalidRepoName`. | Prevents cross-platform path confusion. |
| **NUL / ASCII Controls** | Embedded NUL is rejected before an OS pathname syscall; controls accepted. | `manifest_key` rejects NUL and ASCII controls with `StorageError::InvalidRepoName`. | Fails fast before filesystem interaction. |
| **Non-Canonical Keys (`C:/repo`)** | Accepted as relative path containing colon on Linux. | Accepted on Linux: composes `repos/C:/repo/manifests/<hex>` as a valid relative `ObjectKey`. | Preserves current Linux behavior without introducing unapproved colon rejection. |
| **External Symlinks** | Followed symlinks to outside files/directories. | Rejected by `openat2` (`RESOLVE_BENEATH \| RESOLVE_NO_SYMLINKS`), mapped to `StorageErrorKind::Io`. | Strict containment enforcement; zero escape outside storage root. |
| **Internal Symlinks** | Followed symlinks to inside files. | Rejected by `openat2` (`RESOLVE_NO_SYMLINKS`), mapped to `StorageErrorKind::Io`. | Storage repository layout requires regular files; symlink aliasing is disallowed. |
| **Ancestor Symlinks** | Traversed symlinked directories within path. | Rejected by `openat2` (`RESOLVE_NO_SYMLINKS`), mapped to `StorageErrorKind::Io`. | Prevents directory aliasing or redirection. |
| **Dangling Symlinks** | Failed target resolution returning `ENOENT` -> `StorageError::NotFound`. | Resolution fails closed at the symlink itself -> `StorageErrorKind::Io` (`ResolutionRejected`). | Security boundary: presence of a prohibited symlink is an I/O containment error, not an absent file. |
| **Genuine Missing Paths** | Returned `StorageError::NotFound`. | Returns `StorageError::NotFound`. | Preserved semantic parity for genuinely missing objects. |
| **Non-Regular Objects** | Directory read returned `EISDIR` -> `StorageErrorKind::Io`; FIFO read may block. | Rejected during Phase 1 `fstat` validation (`S_IFREG`), returning `StorageErrorKind::Io` (`UnsupportedObjectType`). | Prevents potential FIFO read blocking and invalid object type consumption. |
| **Root Directory Replacement** | Reads followed pathname `self.root` at call time. | Reads follow pinned file descriptor of initial root directory across renames. | Prevents symlink / race attacks against the storage root pathname. |

---

## 5. Comprehensive Test Execution Matrix

### 5.1 Verification Commands Executed

| Command | Working Directory | Exit Status | Result Summary |
| :--- | :--- | :--- | :--- |
| `cargo fmt --check` | `registry-rust` | 0 | PASSED: Strict Rust formatting compliance. |
| `cargo check --locked --all-targets --all-features` | `registry-rust` | 0 | PASSED: Full static type check across all targets and features. |
| `cargo clippy --locked --all-targets --all-features -- -D warnings` | `registry-rust` | 0 | PASSED: Zero warnings across all targets and features. |
| `cargo test --locked --lib storage::fs::manifest::tests` | `registry-rust` | 0 | PASSED: 16 passed, 0 failed, 1 ignored (unprivileged permission test). |
| `cargo test --locked --lib test_manifest_read_` | `registry-rust` | 0 | PASSED: 10 passed, 0 failed, 1 ignored (unprivileged permission test). |
| `cargo test --locked --lib test_manifest_read_permission_denied_ignored -- --ignored` | `registry-rust` | 0 | PASSED: 1 passed (unprivileged permission denial verified, `ScopedPermReset` restored). |
| `cargo test --locked --lib test_real_fs_permission_denied_ignored -- --ignored` | `registry-rust` | 0 | PASSED: 1 passed (unprivileged permission denial verified in promoted module). |
| `cargo test --locked --lib delete_manifest_fails_safe_on_malformed_manifest` | `registry-rust` | 0 | PASSED: 1 passed (write regression test). |
| `cargo test --locked --lib referrers_add_list_remove_and_delete_manifest` | `registry-rust` | 0 | PASSED: 1 passed (write regression test; matches `src/storage/fs/tests.rs:40`). |
| `cargo test --locked --lib test_detect_manifest_media_type_malformed_json_is_corrupt_data` | `registry-rust` | 0 | PASSED: 2 passed (facade regression test in fs and s3). |
| `cargo test --locked --test manifest_lifecycle_tests` | `registry-rust` | 0 | PASSED: 65 passed, 0 failed. |
| `cargo test --locked --test application_read_tests` | `registry-rust` | 0 | PASSED: 46 passed, 0 failed. |
| `cargo test --locked --test ports_wiring_tests` | `registry-rust` | 0 | PASSED: 10 passed, 0 failed. |
| `cargo test --locked --lib runtime::tests::test_proxy_cache_` | `registry-rust` | 0 | PASSED: 6 passed (all 5 identified proxy-cache startup offload tests + s3 parity). |
| `cargo test --locked --lib cli::runtime::tests::` | `registry-rust` | 0 | PASSED: 6 passed (all 5 identified maintenance/admin startup offload tests + s3 parity). |
| `git diff --check` | `registry-rust` | 0 | PASSED: Zero whitespace or conflict marker errors. |

### 5.2 Test Coverage Inventory

#### Promoted Unit & Seam Tests (`src/storage/fs/manifest.rs`):
1. `test_manifest_key_valid_single_and_multisegment`: Validates single and multi-segment repository keys.
2. `test_manifest_key_rejects_unsafe_inputs`: Validates rejection of traversal sequences, dots, slashes, backslashes, NUL, and ASCII controls.
3. `test_detect_manifest_media_type_direct`: Directly tests media-type parsing, default OCI fallback, scalar JSON, empty payloads, and malformed non-JSON.
4. `test_recording_fake_exact_key_and_single_open_call`: Verifies exact key composition and exactly one `open_payload` invocation per HEAD/GET.
5. `test_recording_fake_size_derived_from_consumed_bytes_not_metadata`: Proves size is derived from bytes actually consumed from the stream.
6. `test_recording_fake_media_type_variants_and_corrupt_data`: Verifies custom media types, OCI fallback, and `CorruptData` errors.
7. `test_recording_fake_acquisition_failures_suppress_stream_and_fallback`: Proves `NotFound`, `PermissionDenied`, and `ResolutionRejected` suppress stream reading.
8. `test_recording_fake_mid_stream_io_failure`: Verifies mid-stream stream errors translate to `StorageErrorKind::Io` with preserved diagnostic message.
9. `test_recording_fake_typed_runtime_and_task_failures`: Verifies `RuntimeMissing` -> `Backend`, `TaskJoinFailed` -> `Backend`, `SyscallUnsupported` -> `Configuration`.
10. `test_recording_fake_unsafe_input_suppresses_reader_invocation`: Proves invalid repository inputs reject before calling `open_payload`.
11. `test_real_fs_representative_valid_manifests_and_nested_repos`: Real filesystem valid manifest reads on Linux.
12. `test_real_fs_supported_digest_algorithms`: Validates SHA-256 and SHA-512 raw hex paths on Linux.
13. `test_real_fs_missing_manifest_and_missing_repo`: Verifies `NotFound` on real filesystem for missing components.
14. `test_real_fs_symlinks_rejected_without_reading_outside_content`: Verifies symlinks fail closed with `Io`.
15. `test_real_fs_directory_substituted_for_manifest_rejected`: Verifies non-regular objects fail closed with `UnsupportedObjectType` -> `Io`.
16. `test_real_fs_pinned_root_across_rename`: Validates reader retains access through pinned descriptor across directory rename.
17. `test_real_fs_permission_denied_ignored`: Unprivileged permission test verified with `ScopedPermReset`.

#### Production Storage & Port Tests (`src/storage/fs/tests.rs`):
1. `test_manifest_read_representative_valid_oci_manifest`: Validates production `FsStorage::head_manifest`, `get_manifest`, and `ManifestReader` port forwarding.
2. `test_manifest_read_media_type_detection_variants`: Validates production media-type variants.
3. `test_manifest_read_empty_and_malformed_payloads_classify_as_corrupt_data`: Validates production corrupt data classification.
4. `test_manifest_read_missing_paths_return_not_found`: Validates missing repository, missing `manifests/`, and missing file all return `NotFound`.
5. `test_manifest_read_nondirectory_components_return_io`: Validates non-directory component failure.
6. `test_manifest_read_repository_naming_single_and_multisegment`: Validates nested repositories.
7. `test_manifest_read_supported_digest_algorithms_and_filename_forms`: Validates SHA-256 and SHA-512.
8. `test_manifest_read_unvalidated_caller_path_traversal_gap`: Asserts `InvalidRepoName` rejection on `..` traversal across `head_manifest`, `get_manifest`, and `ManifestReader` port methods; checks pre-composition rejection cases; verifies preserved acceptance of `C:/repo` on Linux.
9. `test_manifest_read_containment_symlink_traversal`: Asserts `Io` rejection on symlinks across Scenarios 1 (external), 2 (internal), 3 (ancestor), and 4 (dangling); verifies genuine missing paths remain `NotFound`.
10. `test_manifest_read_production_pinned_root_across_rename`: Verifies production `FsStorage::head_manifest`, `get_manifest`, and `ManifestReader` observe initial content across storage root rename through the shared reader.
11. `test_manifest_read_permission_denied_ignored`: Unprivileged permission test verified with `ScopedPermReset`.

---

## 6. Known Limitations & Operational Considerations

1. **Unbounded Full Read Buffering**:
   Both `head_manifest` and `get_manifest` read the complete manifest payload into memory before returning. This is required because `mediaType` is dynamically parsed from the JSON body. Full buffering has no enforced read-size ceiling and remains an operational limitation.
2. **Concurrency, Snapshot Isolation & Atomic Reads**:
   Payload acquisition (`openat2` Phase 1 + Phase 2 readable reopen) and subsequent stream consumption do not form an atomic snapshot. Concurrent writes or file truncation may produce changed bytes, valid JSON, invalid JSON (`CorruptData`), or an I/O error (`StorageErrorKind::Io`). Separate HEAD and GET calls may observe different content if modified concurrently.
3. **Operating System & Kernel Containment Assumptions**:
   - **Stable Procfs Requirement**: Phase 2 readable reopening via `/proc/self/fd/<phase1_fd>` requires a genuine, accessible, stable procfs mount. Sandboxes, chroot environments, or containers with restricted `/proc` mounts or masked file descriptor pseudo-symlinks cannot perform Phase 2 reopening.
   - **Mount & Hard-Link Isolation**: Descriptor-relative resolution does not guarantee mount or hard-link isolation. Hard links to objects outside the root or bind mounts within the tree are governed by standard kernel containment semantics.
   - **Non-Linux Verification**: Non-Linux compilation and execution remain unverified. The storage-fs containment backend requires Linux 5.6+ `openat2`.
4. **Pathname Mutation vs Contained Read Coherence Boundary**:
   Manifest read operations resolve strictly through the pinned root file descriptor. Manifest mutation operations (`put_manifest`, `delete_manifest`, `set_tag`) still resolve via pathname `self.root`. Root rename/replacement can make pinned reads and pathname mutations address different trees or cause mutation failures; this slice does not enforce their coherence. Full coherence requires write containment extraction (Gate O-04).

---

## 7. Code-Only Rollback Plan

- **Zero Schema or Storage Layout Migration**:
  The directory and file structure on disk (`repos/<repo>/manifests/<digest.hex()>`) is completely unchanged. No database migrations, index alterations, or file moves occurred.
- **Rollback Procedure**:
  Rollback restores the prior code without a schema or storage-layout migration. Deployment/restart requirements depend on the operating environment. Rollback does not undo writes, deletions, or other mutations performed while the cutover was active.

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
