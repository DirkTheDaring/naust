> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. The gap this design addresses was closed by later contained-discovery slices (`3bbe006`…`2fc21aa`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Contained Filesystem Metadata Design for Registry GC Listing

**Document Status:** PROPOSAL / DESIGN ONLY — NOT IMPLEMENTED
**Target Slices:** Storage Layer Contained Metadata Extension & Registry CAS Listing Integration
**Authoritative Baseline Commits:**
- `registry-rust`: `c38a726d3f2dc78631a4e0944522d51258746145`
- `storage-layer-rust`: `caaba4817b972c35a65c41d0af7d7ec748dbe85e`

---

## 1. Executive Summary & Problem Statement

In Bounded Slice 2B, `registry-rust` implemented and verified a test-only CAS listing integration seam ([`listing_seam.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/listing_seam.rs)) using the descriptor-relative directory enumeration API ([`FsMetadataReader::enumerate_dir`](file:///home/dietmar/devel/rust/storage-layer-rust/crates/storage-fs/src/reader.rs)). Production listing (`FsStorage::list_cas_blobs_page`) continues to execute uncontained pathname traversal via `tokio::fs::read_dir`.

The test integration confirmed an architectural gap between directory enumeration and the candidate metadata required by the registry garbage collector:
1. **Directory Enumeration (`storage-fs::dir`)**: Returns `Vec<DirEntry>`, exposing only the raw entry name (`name: OsString`) and point-in-time file type ([`DirEntryType`](file:///home/dietmar/devel/rust/storage-layer-rust/crates/storage-fs/src/dir.rs)). It does not expose file size, modification timestamps, or version identifiers.
2. **Core Metadata Inquiry (`storage-core::ObjectMetadataReader::head`)**: Accepts an [`ObjectKey`](file:///home/dietmar/devel/rust/storage-layer-rust/crates/storage-core/src/key.rs) and returns [`ObjectMetadata`](file:///home/dietmar/devel/rust/storage-layer-rust/crates/storage-core/src/read.rs). In the Slice 2A baseline, `ObjectMetadata` encapsulates only byte size (`size: u64`), omitting timestamps and version identifiers.
3. **Legacy Registry GC Listing (`FsStorage::list_cas_blobs_page`)**: Constructs full [`GcBlobCandidate`](file:///home/dietmar/devel/rust/registry-rust/src/storage/mod.rs) instances containing:
   - `digest: Digest`
   - `size: u64`
   - `last_modified: SystemTime`
   - `version: BlobObjectVersion` (formatted as `"{mtime_secs}:{size}"`)

Because project guidelines prohibit value fabrication (e.g. synthesizing dummy timestamps), uncontained pathname reopening (`tokio::fs::metadata`), or uncoordinated cross-crate modifications, the test seam yielded [`IncompleteGcCandidate`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/listing_seam.rs) and recorded an explicit gap ([`IncompleteCandidateTranslationGap`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/listing_seam.rs)).

This design specifies the smallest contained metadata extension required to complete `GcBlobCandidate` generation beneath the pinned directory descriptor.

---

## 2. Source-Backed GC Metadata Contract & Consumer Analysis

### 2.1 Consumer Inventory and Supporting Excerpts

The following inventory examines all components in `registry-rust` (at commit `c38a726d3f2dc78631a4e0944522d51258746145`) interacting with candidate metadata:

#### Consumer Summary Table
| Consumer Component | Source File & Lines | Metadata Consumed | Provenance | Operational Role | Failure & Fallback Behavior |
|---|---|---|---|---|---|
| **Legacy CAS Listing** (`FsStorage::list_cas_blobs_page`) | [`src/storage/fs.rs:3448-3478`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L3448-L3478) | `len()`, `modified()` | `tokio::fs::metadata(&path).await` | Formats `BlobObjectVersion(format!("{mtime_secs}:{size}"))`; populates `GcBlobCandidate` | `metadata()` error fails the page with `StorageError::io`; `modified()` error falls back to `UNIX_EPOCH`; pre-epoch `duration_since` yields 0 secs |
| **GC Planning** (`blob_gc_plan`) | [`src/blob_gc/mod.rs:163-176`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/mod.rs#L163-L176) | `candidate.last_modified`, `candidate.size` | Candidate from `CasBlobTraverser` | Evaluates `check_candidate_age`; tracks `scanned_bytes`, `eligible_bytes` | `last_modified == UNIX_EPOCH` -> `MissingTimestamp` (skipped); future/young -> skipped |
| **GC Age Policy** (`check_candidate_age`) | [`src/blob_gc/policy.rs:40-58`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/policy.rs#L40-L58) | `last_modified: SystemTime` | Passed by GC loops | Computes `now.duration_since(last_modified) >= min_age` | `UNIX_EPOCH` -> `MissingTimestamp`; future -> `FutureTimestamp`; young -> `IneligibleAge` |
| **GC Quarantine Phase** (`blob_gc_quarantine_with_authority`) | [`src/blob_gc/mod.rs:275-295`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/mod.rs#L275-L295) | `candidate.last_modified`, `candidate.size`, `candidate.version` | Candidate from `CasBlobTraverser` | Age evaluation; invokes `storage.quarantine_blob` | Ineligible candidates skipped; storage errors abort run |
| **Filesystem Quarantine** (`FsStorage::quarantine_blob`) | [`src/storage/fs.rs:3493-3550`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L3493-L3550) | `meta.len()` (src metadata) | `tokio::fs::metadata(&src).await` | Checks src/dest; renames to `quarantine/`; writes timestamp | **Ignores** `_version` parameter; returns `Quarantined { size }` or `Skipped` |
| **Pre-Delete Revalidation** (`revalidate_candidate_before_delete`) | [`src/blob_gc/validation.rs:101-181`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/validation.rs#L101-L181) | `candidate.digest` | Parameter from GC callers | Queries upload pins, membership, manifests, WAL | Fails closed on unreadable index/journal; protects referenced blobs |
| **Guarded Deletion** (`execute_guarded_gc_deletion`) | [`src/blob_gc/validation.rs:201-239`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/validation.rs#L201-L239) | `candidate.version`, `candidate.size` | Parameter from GC callers | Revalidates; invokes `storage.delete_blob_conditional` | Revalidation protection returns `Protected`; mismatch returns `PreconditionFailed` |
| **Quarantine Sweep** (`blob_gc_sweep_quarantine_with_authority`) | [`src/blob_gc/mod.rs:533-561`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/mod.rs#L533-L561) | `quarantined_blob_version`, `meta.len()`, `meta.modified()` | `tokio::fs::metadata(&qpath)` and `quarantined_blob_version` | Synthesizes `GcBlobCandidate` containing `quarantined_blob_version`; executes guarded deletion | Absent version query skips candidate; `PreconditionFailed` logs warning and preserves file |
| **Quarantined Version Query** (`FsStorage::quarantined_blob_version`) | [`src/storage/fs.rs:3609-3624`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L3609-L3624) | Quarantined file path | Calls `compute_fs_blob_version(&path)` | Computes hash and nanosecond timestamp of quarantined file | Returns `None` if metadata lookup fails; otherwise `Some(BlobObjectVersion)` |
| **Filesystem Conditional Deletion** (`FsStorage::delete_blob_conditional`) | [`src/storage/fs.rs:3626-3678`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L3626-L3678) | `expected_version: Option<&BlobObjectVersion>` | Argument from caller | Recomputes `compute_fs_blob_version`; compares against `expected_version` | Missing expected version returns `Conflict`; mismatch returns `PreconditionFailed`; match calls `remove_file` |
| **Quarantine Restore** (`FsStorage::restore_quarantined_blob`) | [`src/storage/fs.rs:3552-3607`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L3552-L3607) | `permit: &GcMutationPermit`, `digest: &Digest` | Arguments from caller | Renames quarantined file back to CAS path | **No version parameter**; restores strictly by digest; returns `Ok(Some(size))` or `Ok(None)` if missing |
| **S3 Direct Conditional Deletion** (`blob_gc_direct_conditional_with_authority`) | [`src/blob_gc/mod.rs:600-680`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/mod.rs#L600-L680) | `candidate.version` from S3 listing | Candidate from traverser | Calls `s3.delete_blob_conditional` with `If-Match` | S3 strategy only; never executed for filesystem storage |

#### Exact Supporting Excerpt 1: Legacy Candidate Construction
```rust
// Source: registry-rust/src/storage/fs.rs:3456-3478
let path = dir_path.join(&hex);
let meta = tokio::fs::metadata(&path)
    .await
    .map_err(|e| StorageError::io(format!("metadata {}: {e}", path.display())))?;
let modified = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
let size = meta.len();
let mtime_secs = modified
    .duration_since(std::time::UNIX_EPOCH)
    .unwrap_or_default()
    .as_secs();
let version = BlobObjectVersion(format!("{mtime_secs}:{size}"));
let digest = Digest::parse(&digest_str).map_err(|e| {
    StorageError::internal_invariant(format!(
        "failed to parse digest from hex {hex}: {e}"
    ))
})?;

candidates.push(GcBlobCandidate {
    digest,
    size,
    last_modified: modified,
    version,
});
```

#### Exact Supporting Excerpt 2: Quarantine Sweep Candidate Construction
```rust
// Source: registry-rust/src/blob_gc/mod.rs:533-551
let version = match storage.quarantined_blob_version(&digest).await {
    Ok(Some(v)) => v,
    Ok(None) => {
        drop(reval_guard);
        continue;
    }
    Err(source) => {
        drop(reval_guard);
        return Err(BlobGcError::QuarantineVersionQuery { digest, source });
    }
};

let candidate = storage::GcBlobCandidate {
    digest: digest.clone(),
    size: meta.len(),
    last_modified: meta.modified().unwrap_or(UNIX_EPOCH),
    version,
};
```

---

### 2.2 Distinction: Two Separate Filesystem Version Formats

The codebase employs two distinct, non-interchangeable filesystem version formats:

```rust
// Format 1: CAS Listing Candidate Version (src/storage/fs.rs:3466)
let version = BlobObjectVersion(format!("{mtime_secs}:{size}"));

// Format 2: Quarantined Blob Version (src/storage/fs.rs:3318-3344)
// Generated by compute_fs_blob_version:
let version = BlobObjectVersion(format!("fs:{len}:{mtime}:{content_sha256}"));
```

1. **Format 1 (`"{mtime_secs}:{size}"`)**:
   - Synthesized during CAS listing from whole seconds since Unix epoch and byte length.
   - It is passed into `FsStorage::quarantine_blob(permit, &digest, &version)` where it is **unused** (`_version: &BlobObjectVersion`).
   - It is never evaluated or compared against filesystem storage state.
2. **Format 2 (`"fs:{len}:{mtime}:{hash}"`)**:
   - Synthesized exclusively by `compute_fs_blob_version` on files inside `quarantine/blobs/`.
   - Incorporates nanosecond timestamp and full SHA-256 content hash.
   - Evaluated exclusively during quarantine sweep deletion (`delete_blob_conditional`).

---

### 2.3 Safety Role and Limitations of Current Version Checks

1. **Safety Role**:
   - `last_modified`: Serves as a **grace-period filter** in `check_candidate_age`. Candidates with `last_modified == UNIX_EPOCH` evaluate to `MissingTimestamp` and are skipped. Blobs younger than `min_age` or bearing future timestamps are skipped.
   - `size`: Drives quota accounting (`limits.max_bytes`) and telemetry.
   - `version`: Satisfies the domain interface of `GcBlobCandidate`.
2. **Inherent Limitations**:
   - **No Unique Identity**: Size and whole-second modification time do not uniquely identify a file or guarantee against replacement. An identical-length file written within the same second produces the exact same version string.
   - **No Snapshot Isolation**: Multi-directory traversal is not atomic. Blobs added or removed during traversal may be observed or missed depending on cursor position.
   - **Replacement Detection Limitations**: A concurrent replacement of an enumerated blob by another regular file with the same name before metadata acquisition will succeed and report the replacement file's attributes.
   - **Non-Atomic Conditional Delete**: In `delete_blob_conditional`, computing the version via `compute_fs_blob_version(&path)` followed by `tokio::fs::remove_file(&path)` is not an atomic conditional delete. A concurrent mutator can replace the file after `compute_fs_blob_version` closes its descriptor and before `remove_file` executes. Furthermore, an in-place mutation during streaming SHA-256 computation can produce a torn hash.

---

## 3. Options Comparison & Recommendation

| Option | Architecture | Impact on `storage-core` | Syscall Overhead | Assessment |
|---|---|---|---|---|
| **Option A** *(Recommended)* | Storage-FS specific contained file inspection method on `FsMetadataReader` | None (preserves core traits and open gate O-03) | 1 `fstat` per paginated candidate (max 1,000 per page) | **Recommended**: Minimal compatible step, keeps mechanism in `storage-fs` and policy in `registry-rust`. |
| **Option B** | Additive extension to `storage_core::ObjectMetadata` and `head()` | Expands `storage-core` metadata contracts prematurely | 1 `fstat` per candidate | Rejected: Expands cross-crate surface while O-03 is open. |
| **Option C** | Richer `DirEntry` carrying metadata from `enumerate_dir` | None | $N$ `fstatat` calls for *every* entry in shard (e.g. 50,000 calls) | Rejected: Prohibitive syscall latency during directory iteration. |

### Recommendation Rationale
Option A is recommended because:
1. It confines all modifications to `storage-fs` and `registry-rust`, leaving `storage-core` untouched.
2. It incurs inspection syscalls only for entries surviving lexical cursor filtering (at most `limit`, clamped to 1,000).
3. It reuses the contained `openat2` (`O_PATH`) resolution pattern already established in `head` and `open_payload`.

---

## 4. Proposed API Shape (Proposal Only)

> [!IMPORTANT]
> The following API signatures and types represent a proposed design for review. They are not implemented in this documentation slice.

### 4.1 Storage Layer: `storage-fs` Additions

In `crates/storage-fs/src/reader.rs`:

```rust
// [PROPOSED] Neutral filesystem attributes container
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FsFileMetadata {
    size: u64,
    modified: Option<std::time::SystemTime>,
}

impl FsFileMetadata {
    pub fn new(size: u64, modified: Option<std::time::SystemTime>) -> Self {
        Self { size, modified }
    }

    pub fn size(&self) -> u64 { self.size }
    pub fn modified(&self) -> Option<std::time::SystemTime> { self.modified }
}
```

> [!NOTE]
> Because `inspect_file_metadata` only returns `Ok` for regular files (`S_IFREG`) and explicitly rejects non-regular objects with `UnsupportedObjectType`, exposing a `DirEntryType` field on `FsFileMetadata` is redundant and omitted.

#### Proposed Method Signature
```rust
impl FsMetadataReader {
    /// Descriptor-relative inspection of filesystem attributes beneath the pinned root.
    ///
    /// Resolves `key` beneath the pinned root descriptor via `openat2` with:
    /// `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`
    /// and queries attributes via `fstat`.
    ///
    /// Only regular files (`S_IFREG`) succeed; non-regular files reject with `UnsupportedObjectType`.
    pub async fn inspect_file_metadata(
        &self,
        key: &storage_core::ObjectKey,
    ) -> Result<FsFileMetadata, storage_core::ReadError>;
}
```

---

## 5. Exact Timestamp Semantics & Checked Conversion

### 5.1 Checked Conversion from Linux `stat`

In `libc::stat`, timestamps are represented as:
- `st_mtime`: `time_t` (signed integer seconds relative to 1970-01-01 00:00:00 UTC).
- `st_mtime_nsec`: `c_long` / `i64` (fractional nanoseconds).

The conversion must preserve signed values, fractional precision, and avoid signed-minimum overflow:

```rust
// [PROPOSED PSEUDOCODE] Checked stat timestamp conversion
pub(crate) fn convert_stat_mtime(
    sec: i64,
    nsec: i64,
) -> Result<std::time::SystemTime, crate::error::FsMetadataError> {
    // 1. Validate nanosecond bounds
    if !(0..1_000_000_000).contains(&nsec) {
        return Err(crate::error::FsMetadataError::InvalidMetadata {
            message: "nanoseconds out of valid range [0, 999_999_999]",
        });
    }

    let nsec_u32 = nsec as u32;

    if sec >= 0 {
        // 2. Post-epoch conversion
        let duration = std::time::Duration::new(sec as u64, nsec_u32);
        std::time::UNIX_EPOCH
            .checked_add(duration)
            .ok_or(crate::error::FsMetadataError::InvalidMetadata {
                message: "timestamp exceeds supported SystemTime range",
            })
    } else {
        // 3. Pre-epoch conversion (avoiding signed-minimum abs overflow via i128)
        let sec_i128 = sec as i128;
        let (duration_sec, duration_nsec) = if nsec_u32 == 0 {
            ((-sec_i128) as u64, 0)
        } else {
            // sec is negative, e.g. -1 with 500_000_000 ns means -0.5s (0s + 500ms before epoch)
            (((-sec_i128 - 1) as u64), 1_000_000_000 - nsec_u32)
        };

        let duration = std::time::Duration::new(duration_sec, duration_nsec);
        std::time::UNIX_EPOCH
            .checked_sub(duration)
            .ok_or(crate::error::FsMetadataError::InvalidMetadata {
                message: "pre-epoch timestamp underflows supported SystemTime range",
            })
    }
}
```

### 5.2 Explicit Timestamp Test Vectors
- **Vector 1 (`sec = -1, nsec = 500_000_000`)**: Time is $-1 + 0.5 = -0.5$ seconds. Duration before epoch: `Duration::new(0, 500_000_000)`. Result is `UNIX_EPOCH - 0.5s`.
- **Vector 2 (`sec = -1, nsec = 999_999_999`)**: Time is $-1 + 0.999999999 = -0.000000001$ seconds. Duration before epoch: `Duration::new(0, 1)`. Result is `UNIX_EPOCH - 1ns`.
- **Vector 3 (`sec = 0, nsec = 0`)**: Exactly `UNIX_EPOCH`.
- **Vector 4 (`sec = i64::MIN`)**: `sec_i128 = -9223372036854775808`. `(-sec_i128)` evaluates to `9223372036854775808` without overflow, then checked via `checked_sub`.
- **Vector 5 (`nsec = -1` or `nsec = 1_000_000_000`)**: Rejected with `InvalidMetadata`.

### 5.3 Demarcation of Timestamp & Failure Outcomes
1. **Metadata Acquisition Failure**: `fstat` syscall returns error. Handled as `FsMetadataError::StatFailed` wrapped in `ReadError::Backend`.
2. **Timestamp Genuinely Unavailable**: Represented explicitly by `modified: Option<SystemTime> = None`.
3. **Invalid Timestamp Fields**: Nanoseconds out of bounds or negative file size. Returns `FsMetadataError::InvalidMetadata`.
4. **Out-of-Range Timestamp**: `checked_add` or `checked_sub` overflow. Returns `FsMetadataError::InvalidMetadata`.
5. **Valid Epoch Timestamp**: Exactly `Some(UNIX_EPOCH)`. Evaluates to `MissingTimestamp` in `check_candidate_age` without error.

### 5.4 Registry Fallback Policy
- **Age Checking**: An actual pre-epoch `SystemTime` remains pre-epoch. In `check_candidate_age`, `now.duration_since(last_modified)` succeeds with an age > 50 years, evaluating as eligible.
- **Version Formatting**: Legacy listing uses `duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()`. For pre-epoch timestamps, `duration_since` fails, defaulting to `0` seconds (`format!("0:{size}")`). This behavior is preserved.

---

## 6. Error Taxonomy & Implementation Mapping

### 6.1 Return Type Selection: `storage_core::ReadError`
Existing `head_sync` returns `Result<ObjectMetadata, ReadError>`. The committed `FsMetadataError` does not contain `NotFound` or `PermissionDenied` variants. To avoid premature error enum churning in `storage-fs` or `storage-core`, `inspect_file_metadata` returns `storage_core::ReadError`, wrapping backend-specific errors in `ReadError::Backend`:

| Failure Condition | Representation in Proposed API (`storage_core::ReadError`) | Underlying Source (`storage_fs::FsMetadataError` or `std::io::Error`) | Mapped Registry `StorageError` |
|---|---|---|---|
| **Missing target (`ENOENT`)** | `ReadError::NotFound { key }` | None | `StorageError::NotFound` (or `StorageError::io` during candidate listing) |
| **Permission denial (`EACCES`/`EPERM`)** | `ReadError::PermissionDenied { key, source }` | `std::io::Error` | `StorageError::io` |
| **`ENOTDIR` during resolution** | `ReadError::Backend { message, source }` | `std::io::Error` | `StorageError::corrupt_data` |
| **Kernel containment rejection (`ELOOP`/`EXDEV`)** | `ReadError::Backend { message, source }` | `FsMetadataError::ResolutionRejected { raw_os_error, source }` | `StorageError::io` |
| **Unsupported syscall (`ENOSYS`)** | `ReadError::Backend { message, source }` | `FsMetadataError::SyscallUnsupported(source)` | `StorageError::configuration` |
| **Unsupported platform** | `ReadError::Backend { message, source }` | `FsMetadataError::PlatformUnsupported` | `StorageError::configuration` |
| **Failed `fstat`** | `ReadError::Backend { message, source }` | `FsMetadataError::StatFailed { stage: "file inspection", source }` | `StorageError::io` |
| **Invalid size or timestamp** | `ReadError::Backend { message, source }` | `FsMetadataError::InvalidMetadata { message }` | `StorageError::corrupt_data` |
| **Non-regular object type (`S_IFDIR`, `S_IFIFO`, etc.)** | `ReadError::Backend { message, source }` | `FsMetadataError::UnsupportedObjectType { mode }` | `StorageError::corrupt_data` |
| **Runtime missing** | `ReadError::Backend { message, source }` | `FsMetadataError::RuntimeMissing(err)` | `StorageError::backend` |
| **Task join failed** | `ReadError::Backend { message, source }` | `FsMetadataError::TaskJoinFailed(err)` | `StorageError::backend` |

> [!IMPORTANT]
> `RuntimeMissing` and `TaskJoinFailed` are mapped to `StorageErrorKind::Backend`, matching the established behavior in `read_adapter.rs` and `listing_seam.rs`. They are not mapped to `InternalInvariant`.

### 6.2 Symlink vs. Non-Regular File Demarcation
- **Symlinks**: Rejected during `openat2` resolution by kernel flag `RESOLVE_NO_SYMLINKS`. The syscall returns `ELOOP` (or `EXDEV`), mapping to `FsMetadataError::ResolutionRejected`.
- **Directories / FIFOs / Devices**: `openat2` with `O_PATH` succeeds in opening the descriptor. Userspace mode validation detects `(st.st_mode & S_IFMT) != S_IFREG`, mapping to `FsMetadataError::UnsupportedObjectType`.

---

## 7. Policy Preservation & GC Listing Failure Semantics

1. **Preserve Current Page-Failure Behavior**:
   In legacy listing (`FsStorage::list_cas_blobs_page`), failure to inspect a candidate's metadata (including `ENOENT` from a concurrently disappeared blob) fails the whole page with `StorageError::io`:
   ```rust
   let meta = tokio::fs::metadata(&path)
       .await
       .map_err(|e| StorageError::io(format!("metadata {}: {e}", path.display())))?;
   ```
   The contained inspection seam will **preserve this exact behavior**: if candidate inspection fails, listing aborts and fails closed. It does **not** silently skip the candidate.
2. **Production Budgets Undecided**:
   Production `DirEnumerationLimits` defaults remain undecided and are not required to specify the metadata inspection API.
3. **Production Cutover Conditional**:
   Production listing continues using legacy pathname traversal. Cutting over `FsStorage::list_cas_blobs_page` to contained listing remains strictly conditional on future review and explicit authorization.

---

## 8. Concurrency, Containment & Platform Assumptions

### 8.1 Syscall Support & Platform Assumptions
- `openat2` requires Linux 5.6+.
- `fstat` on `O_PATH` descriptors requires Linux 3.6+ (prior kernels returned `EBADF`).
- Primary references:
  - [`stat(2)`](https://man7.org/linux/man-pages/man2/stat.2.html)
  - [`open(2)`](https://man7.org/linux/man-pages/man2/open.2.html)
- Attributes are retrieved from the same acquired descriptor in one `fstat` result, but this does not establish an atomic snapshot under concurrent mutation.

### 8.2 Descriptor Lifecycle
- **Target `OwnedFd`**: Wrapped in `std::os::fd::OwnedFd` immediately after `openat2`. Closed upon function exit or unwinding.
- **Root Descriptor**: The worker task holds an `Arc<OwnedFd>`. Root descriptor closure occurs only when the final `Arc` owner is dropped.
- **Cancellation**: Dropping the awaiting Tokio future does not abort the blocking kernel syscall on the threadpool thread; the worker thread continues until syscall completion.

### 8.3 Remaining Concurrency & Containment Boundaries
- **No Snapshot Isolation**: Traversal and metadata inspection are separate operations.
- **Inter-Call Replacement**: If an entry is unlinked and replaced by another regular file between enumeration and inspection, `inspect_file_metadata` will succeed and inspect the new file.
- **Mount Crossing**: `RESOLVE_BENEATH` does not isolate child mounts attached beneath the root.
- **Hard Links & In-Place Mutation**: Modifying a file in-place alters contents without changing inode or path.
- **Root Coherence**: Registry mutations still use host path resolution (`tokio::fs`). Full root coherence is deferred until mutation paths are migrated.

---

## 9. Verification & Acceptance Plan

Rollout is partitioned into three strictly bounded, sequential slices:

### 9.1 Slice 3A: Storage-FS Contained Metadata Inspection API
- **Target Repository:** `storage-layer-rust`
- **Scope:**
  - Implement `FsFileMetadata` and `FsMetadataReader::inspect_file_metadata`.
  - Implement checked timestamp conversion.
- **Acceptance Tests:**
  1. *Exact Size & Fractional Timestamps*: Retrieve matching size, post-epoch fractional timestamps, and pre-epoch fractional timestamps.
  2. *Checked Pre-Epoch Conversion*: Verify test vectors (`-0.5s`, `-1ns`, `i64::MIN`, invalid nanoseconds).
  3. *Epoch vs. Missing*: Verify `Some(UNIX_EPOCH)` vs `None`.
  4. *Typed Errors*: Verify `ResolutionRejected` on symlinks, `UnsupportedObjectType` on dirs/FIFOs, `NotFound` on missing files, and `StatFailed`.
  5. *Blocking Lifecycle*: Verify descriptor cleanup and root `Arc` retention.

### 9.2 Slice 3B: Registry Test Seam Candidate Completion
- **Target Repository:** `registry-rust`
- **Scope:**
  - Update `listing_seam.rs` to call `inspect_file_metadata` and construct complete `GcBlobCandidate`.
- **Acceptance Tests:**
  1. *Compatibility*: Verify `check_candidate_age` and `format!("{mtime_secs}:{size}")` formatting on complete candidates.
  2. *No Partial Pages*: Verify failure during candidate inspection (after earlier candidates accumulated) fails closed without returning a partial page.
  3. *Inter-Call Replacement*: Deterministically replace an enumerated entry with a symlink, directory, or unlinked state, verifying typed failure.

### 9.3 Slice 3C: Production Listing Cutover (Conditional)
- **Target Repository:** `registry-rust`
- **Scope:** Cut over `FsStorage::list_cas_blobs_page` to contained listing upon explicit authorization.

---

## 10. Open Decisions & Quality Gate Status

### 10.1 Decisions Required Prior to Production Cutover
1. **Disappeared Candidate Handling**: Confirm that failing the page with `StorageError::io` on missing candidates remains desired for production, or if an explicit skip policy should be authorized.
2. **Production Shard Limits**: Establish capacity-planned defaults for `DirEnumerationLimits`.

### 10.2 Quality Gate Status
All established quality gates remain **OPEN**:
- **O-03 (Keys & Continuation Tokens)**: OPEN.
- **O-04 (Durability & Containment)**: OPEN.
- **O-05 (Read Containment)**: OPEN.
- **O-06 (AWS Mapping & Pinned MinIO)**: OPEN.
- **O-13 (Permanent Hosting & Distribution)**: OPEN.
- **O-15 (Non-Linux Execution)**: OPEN.
- **O-16 (Inventory Completeness)**: OPEN.
- **D-06 (Broader Extraction & Cutover Decision)**: OPEN.
