# Experimental Design Note: Linux Descriptor-Relative Metadata Reader (O-05)

**Repository:** `registry-rust`
**Date:** 2026-09-08
**Scope:** Isolated Linux descriptor-relative metadata query exploration (`openat2`) prior to backend extraction.
**Reference:** [Linux openat2(2) man page](https://man7.org/linux/man-pages/man2/openat2.2.html)
**Status:** Gate O-05 remains **OPEN** (experimental mechanism characterized in-tree; production cutover, generic integration, and containment enforcement deferred). Gates O-03, O-06, O-13, O-16, and D-06 retain their established meanings and remain **OPEN**.

---

## 1. Architectural Ownership Boundaries

- **`storage-core`**: Backend-neutral storage capability ports, object contracts, metadata types, and error taxonomy.
- **`storage-fs`**: Filesystem-specific implementation, including Linux descriptor-relative resolution, directory traversal protection, and file I/O.
- **`registry-rust`**: Repository and blob namespace routing, quarantine fallback orchestration, and outward HTTP/OCI API compatibility translation.

---

## 2. Implementation Location and Private Contract

- **Module Location**: `src/storage/fs/contained_metadata.rs` (declared in `src/storage/fs.rs` under `#[cfg(all(target_os = "linux", test))]`).
- **Core Types**:
  - `PinnedRoot`: Encapsulates an owned directory descriptor (`std::os::fd::OwnedFd`) acting as the pinned authority.
  - `ContainedMetadataError`: Minimal private error representation distinguishing failure causes.
- **API Surface**:
  - `PinnedRoot::open(path: &Path) -> Result<PinnedRoot, ContainedMetadataError>`: Opens an existing directory once and retains its descriptor.
  - `PinnedRoot::metadata_size(&self, relative_path: &str) -> Result<u64, ContainedMetadataError>`: Queries object size via kernel-enforced descriptor resolution.
- **Contract Guarantees**:
  - Validates relative paths with strictly normal components.
  - Resolves beneath the pinned descriptor using `openat2` with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`, where the kernel enforces the selected lookup constraints during resolution.
  - Rejects symlinks at every path component (including the leaf).
  - Rejects directories, FIFOs, sockets, block devices, and character devices (`S_IFREG` required).
  - Returns file size as `u64` via descriptor-based `fstat`.
- **Limitation Notice**: `O_PATH` metadata lookup inspects file existence, file type, and size without opening the file for reading. It does **not** prove that read permissions would be granted for payload streams and does not establish legacy permission semantics.

---

## 3. Algorithms & Lifecycle

### Root Acquisition
1. The configured storage directory is opened once via `libc::open` with flags `O_DIRECTORY | O_PATH | O_CLOEXEC`.
2. For this isolated experiment, an initial symlink at the root pathname may resolve during this initial open. The resulting descriptor becomes the sole pinned authority.
3. The pathname is **never** re-resolved during subsequent metadata queries.
4. *Decision Note*: Production acceptance of configured-root symlinks versus strict canonical root enforcement remains a separate future architectural decision.

### Relative Lookup Algorithm
1. **Input Validation**: Rejects empty strings, absolute paths (`/`), trailing slashes, embedded NUL, and segments that are empty, `.`, or `..`. Components that standard `Path::components` would normalize away are strictly rejected.
2. **Kernel Resolution**: Invokes `libc::syscall(libc::SYS_openat2, dirfd, c_path, &open_how, sizeof(open_how))` with:
   - `flags = O_PATH | O_CLOEXEC`
   - `resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`
3. **Descriptor Inspection**: Wraps the resulting file descriptor in `OwnedFd`. Calls `fstat(fd, &mut stat)`.
4. **Type Enforcement (Application-Level Safeguard)**:
   - While `RESOLVE_NO_SYMLINKS` causes `openat2` to reject symlinks with `ELOOP` during path resolution in the kernel, post-open inspection of the opened descriptor provides an additional safeguard.
   - Enforces `st_mode & S_IFMT == S_IFREG`. Rejects any non-regular file (directories, FIFOs, sockets, character/block devices, or an opened symlink descriptor) at the application level as `UnsupportedObjectType { mode }` without fabricating a causal OS error.
5. **Size Conversion**: Safely converts `st_size` (`off_t`) to `u64` via checked conversion.

### Descriptor Ownership & Cleanup
Every raw descriptor returned by `open` or `openat2` is immediately transferred to Rust's `OwnedFd`. `OwnedFd` guarantees deterministic, leak-free descriptor closure on all return paths, errors, and panic unwinding.

---

## 4. Failure Behavior and Classification

The private `ContainedMetadataError` taxonomy distinguishes genuine kernel syscall errors from application-level object-type rejections without string parsing:

| Variant | Condition | Mapping Rule | Error Classification Nature |
|---|---|---|---|
| `InvalidInput(String)` | Input validation failure (empty, absolute, `.`, `..`, repeated/trailing `/`, NUL) | Evaluated before any syscall | Application-level validation error |
| `NotFound(std::io::Error)` | Object does not exist (`ENOENT`) | Preserved raw OS error; distinct from symlink rejection | Real kernel syscall error from `openat2` |
| `ResolutionRejected { raw_os_error, source }` | Kernel resolution policy violation (`ELOOP` for symlinks, `EXDEV` for boundary escapes beneath root) | Never mapped to `NotFound`; preserves numeric OS error code and cause | Real kernel syscall error from `openat2` |
| `UnsupportedObjectType { mode }` | Opened object is not a regular file (`S_IFDIR`, `S_IFIFO`, `S_IFSOCK`, `S_IFLNK`, etc.) | Retains raw mode bitmask | Source-free application-level rejection after successful `openat2` + `fstat` |
| `SyscallUnsupported(std::io::Error)` | `openat2` syscall unimplemented by kernel (`ENOSYS`) | Explicitly distinct from `EINVAL` and `EPERM` | Real kernel syscall error from `openat2` |
| `OsError(std::io::Error)` | Ordinary OS failure (`EACCES`, `EPERM`, `EIO`, etc.) | Retains causal OS error | Real kernel syscall error |

---

## 5. Test Inventory (`storage::fs::contained_metadata::tests`)

1. `test_contained_metadata_empty_and_nonempty_and_sparse_files`: Verifies exact sizes for empty files (0 B), nonempty files (27 B), and sparse files > 4 GiB (8 GiB).
2. `test_contained_metadata_invalid_relative_inputs`: Verifies rejection of empty paths, `/abs`, `a/b/`, `a//b`, `a/./b`, `a/../b`, `a\0b`, `.`, and `..`.
3. `test_contained_metadata_missing_object_vs_rejected_symlink`: Proves missing files yield `NotFound` while symlinks yield `ResolutionRejected` via kernel `openat2` (`ELOOP`), preventing erroneous quarantine fallback.
4. `test_contained_metadata_final_symlinks_rejected`: Proves leaf symlinks to inside-root, outside-root, and dangling targets are rejected with `ResolutionRejected` during kernel resolution.
5. `test_contained_metadata_intermediate_dir_symlink_rejected`: Proves intermediate directory symlinks escaping the root are rejected by kernel resolution policy (`ResolutionRejected`).
6. `test_contained_metadata_directory_and_fifo_rejected`: Verifies directories and FIFOs return `UnsupportedObjectType` without blocking and without reading payloads.
7. `test_contained_metadata_root_pinning`: Renames the original root directory and places an attacker directory at its old pathname; proves lookup remains pinned to the original directory inode.
8. `test_contained_metadata_configured_root_symlink_initialization`: Proves initial root symlink resolution during open and subsequent descriptor durability after symlink deletion.
9. `test_contained_metadata_synthetic_error_classification`: Narrow seam proving `openat2` OS error classification (`ENOSYS` -> `SyscallUnsupported`, `ENOENT` -> `NotFound`, `EXDEV`/`ELOOP` -> `ResolutionRejected`, and `EINVAL`/`EPERM` -> `OsError`). Labeled as synthetic error classification evidence.
10. `test_contained_metadata_synthetic_descriptor_type_safeguard`: Narrow seam verifying application-level mode enforcement (`S_IFLNK`, `S_IFDIR`, `S_IFREG`, negative size) on synthetic stat structures, distinguishing post-open type rejection from kernel `openat2` lookup failures without altering production flags. Labeled as synthetic mode classification evidence.

---

## 6. Security Guarantees & Unproven Limitations

### Kernel-Enforced Lookup Constraints vs. Pathname Checks
Lexical path checks (`Path::components`) and two-phase pathname checks (`symlink_metadata` followed by pathname access) are fundamentally susceptible to races where a directory path component is swapped with a symlink between check and access. In contrast, `openat2` delegates path resolution to the Linux VFS, which enforces the selected lookup constraints (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`) during resolution.

### What Root Pinning Proves
Retaining an owned directory descriptor binds all subsequent lookups to the exact directory inode and filesystem object acquired at initialization. The deterministic root-replacement test proves continued use of the originally pinned root: directory renaming, pathname swapping, or parent directory replacement at the configured path string cannot redirect lookups performed through that pinned descriptor.

However, root pinning does not establish general immunity to every concurrent filesystem change. An opened object can subsequently be renamed outside the root by concurrent processes, and mount and hard-link limitations remain.

### What Remains Unproven
1. **Mount Crossings**:
   - The experiment does not prohibit mount crossings.
   - Under Linux `openat2`, `RESOLVE_BENEATH` restricts resolution beneath the root directory but does **not** disallow crossing mount points on subdirectories beneath that root.
   - `RESOLVE_NO_XDEV` is the separate Linux flag for that restriction (disallowing traversal of mount points during path resolution, including bind mounts).
   - This experiment does not enable `RESOLVE_NO_XDEV`; mount policy remains a separate later decision.
2. **Hard Links**: Hard links inside the root pointing to file data outside the root share identical inodes; `openat2` cannot distinguish a hard link from an ordinary directory entry.
3. **Concurrent Modifications & Renames**: While the kernel enforces lookup constraints during resolution, it does not lock the hierarchy against concurrent changes. A resolved object or directory can subsequently be renamed outside the root while open.
4. **Environment & Platform Support**:
   - Older Linux kernels (< 5.6) lacking `openat2` return `ENOSYS`. Handling older kernels requires a separately reviewed descriptor-walk fallback or explicit rejection.
   - Non-Linux platforms remain completely deferred.
   - Filesystem types (e.g. NFS, CIFS, FUSE) may exhibit varying kernel VFS support for `openat2` resolution flags.
