# Characterization Note: Filesystem Metadata Containment (O-05)

**Repository:** `registry-rust`
**Date:** 2026-09-08
**Scope:** Characterization of in-tree `FsStorage` metadata lookup (`fs_metadata_size`, `head_blob`) at symlinks and containment boundaries prior to descriptor-relative extraction.
**Status:** Gate O-05 remains **OPEN** (characterization portion completed; containment enforcement and backend extraction deferred). Gates O-03, O-06, O-13, O-16, and D-06 retain their existing meanings and remain **OPEN**.

---

## 1. Context and Objective

Quality Gate **O-05** requires establishing strict path containment and symlink safety for filesystem-backed storage. Before designing or extracting a descriptor-relative storage implementation into `storage-fs`, this characterization slice establishes and documents the actual current behavior of the in-tree `FsStorage` metadata query path (`fs_metadata_size` and `head_blob`).

The tests added in this slice record current legacy behavior as an empirical baseline. Where lookups succeed through symlinks or on directories, these results represent **legacy behavior and open containment gaps**, not accepted security guarantees.

---

## 2. Measured Metadata Behaviors and Test Inventory

Seven focused characterization tests in `src/storage/fs/tests.rs` exercise `fs_metadata_size` and `FsStorage::head_blob` across symlink and boundary conditions:

| Case | Scenario | Current Measured Behavior | Test Name | Containment Implication |
|---|---|---|---|---|
| **1** | Final blob entry is a symlink to an ordinary file inside root | Follows symlink via `stat()`; returns target file byte size | `test_fs_metadata_containment_symlink_inside_root` | Legacy behavior: symlinks below root are followed rather than rejected. |
| **2** | Final blob entry is a symlink escaping outside the configured root | Follows symlink via `stat()`; returns outside target byte size | `test_fs_metadata_containment_symlink_outside_root` | **Open containment gap**: symlink escapes configured storage boundary. |
| **3** | Intermediate directory path component is a symlink to outside directory | Traverses intermediate directory symlink; returns target size | `test_fs_metadata_containment_intermediate_dir_symlink_outside_root` | **Open containment gap**: intermediate directory traversal escapes root. |
| **4** | Dangling ordinary blob symlink with valid quarantine blob | `stat()` on dangling symlink fails with `NotFound`; `head_blob` falls back to quarantine | `test_fs_metadata_containment_dangling_symlink_falls_back_to_quarantine` | Current behavior treats broken symlinks as missing files, masking invalid link state and triggering fallback. |
| **5** | Quarantine path is a symlink escaping outside root (ordinary absent) | Follows quarantine symlink via `stat()`; returns outside target size | `test_fs_metadata_containment_quarantine_symlink_outside_root` | **Open containment gap**: quarantine fallback path also escapes storage boundary. |
| **6** | Configured storage root itself is a directory symlink | `FsStorage::try_new` and subsequent metadata lookups succeed through symlink | `test_fs_metadata_containment_storage_root_is_symlink` | Distinguishes root initialization policy from symlinks created below an established root. |
| **7** | Ordinary blob path resolves to a directory instead of a regular file | `stat()` succeeds; returns directory metadata byte size without error | `test_fs_metadata_containment_directory_blob_returns_metadata_size` | **Open object contract gap**: directories are not rejected at the metadata boundary (`is_file()` is not checked). |

---

## 3. Settled Architectural Direction

1. **Linux-First Pinned Root & Descriptor-Relative Resolution:**
   The future extracted implementation will resolve paths descriptor-relatively beneath a pinned root file descriptor (e.g., using Linux `openat2` with flags `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS`, or `O_NOFOLLOW` descriptor walks).
2. **Prohibition of Symlinks Below Root:**
   Symlinks located below the storage root must be rejected at the boundary rather than traversed.
3. **Rejection of Pseudo-Containment Checks:**
   Lexical path checks (e.g., scanning `Path::components` for `ParentDir`) and two-step pathname checks (`symlink_metadata` followed by pathname `open`/`metadata`) are explicitly **not** containment solutions due to time-of-check to time-of-use (TOCTOU) races and concurrent filesystem mutation.

---

## 4. Remaining Decisions and Evidence Needs

The following architectural decisions and evidentiary requirements must not be settled by implication from current behavior and remain open for future slices:

1. **Configured Root vs Sub-Root Policy:**
   Determine whether the configured root directory itself may be a symlink resolved once during initialization (establishing a canonical, pinned directory descriptor), while strictly forbidding any symlinks below that descriptor.
2. **Object Contract & File Type Validation:**
   Decide whether `fs_metadata_size` (or its extracted counterpart) must enforce `FileType::is_file()`, returning a typed error (or `NotFound`) when a directory or special file is encountered, and how this relates to generic object store semantics.
3. **Quarantine Orchestration Preservation & Ownership Boundaries:**
   Preserve registry-owned quarantine orchestration (primary lookup with fallback to quarantine on `NotFound`, while suppressing fallback on `PermissionDenied`, I/O, or containment violation errors) across explicit crate ownership boundaries:
   - `storage-core`: backend-neutral contracts, types, and errors.
   - `storage-fs`: filesystem-specific descriptor-relative resolution and metadata mechanisms.
   - `registry-rust`: blob namespace selection, quarantine orchestration, and outward compatibility translation.
4. **Concurrency, TOCTOU Proof, and Platform Fallback:**
   Formulate formal proof/evidence of TOCTOU resistance under concurrent directory replacement; ensure exact preservation of `io::ErrorKind` translation (e.g., `PermissionDenied` -> `StorageErrorKind::Io`); and distinguish platform scopes:
   - Older Linux kernels lacking `openat2`: require a separately reviewed descriptor-relative fallback or an explicit unsupported result.
   - Non-Linux support: remains deferred and is not a requirement of the initial Linux implementation.

---

## 5. Recommended Smallest Next Bounded Gate

- **Scope:** Introduce a bounded, metadata-only descriptor-relative lookup helper (e.g., `fs_metadata_size_contained`) tested in isolation without modifying production read streams, directory listing, or mutation paths.
- **Compatibility Risks:**
  - Environments where callers intentionally deployed symlinked directory partitions beneath the storage root will experience lookup rejections.
  - Older Linux kernels lacking `openat2` (or running Linux kernels < 5.6) require a separately reviewed descriptor-relative fallback or an explicit unsupported result, while non-Linux platforms remain deferred.
  - Potential error kind translation shifts (e.g., `ELOOP`, `EXDEV`) must map cleanly into `StorageError` without corrupting quarantine fallback logic.
