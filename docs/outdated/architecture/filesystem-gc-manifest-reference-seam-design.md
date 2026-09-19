> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. The seam landed and was promoted to production (`d51ea1a`, `2fc21aa`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Filesystem GC Manifest Reference Seam Design (Corrected)

## Status
- **Date**: 2026-09-12
- **State**: Documentation-Only Design — Ready for Review — Production Unchanged — Not Committed
- **Target Subsystems**:
  - `src/storage/fs/repo_discovery.rs` (preceding discovery seam returning terminal `ObjectKey`s)
  - `src/blob_gc/policy.rs` (`build_manifest_protected_set_fs`, current bypass walker)
  - `src/storage/fs/manifest_listing.rs` (contained manifest enumeration)
  - `src/storage/fs/manifest.rs` (contained manifest payload reading)
  - `src/storage/fs/read_adapter.rs` (payload read error translation)
  - `src/manifest_refs.rs` (`parse_manifest_refs`, reference extraction)
  - `storage_fs::FsMetadataReader` (pinned descriptor containment in `storage-layer-rust`)
- **Required Baselines**:
  - `registry-rust`: `3bbe006148c83add6acfb2f7e0c5df9a21d38b7e`
  - `storage-layer-rust`: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`

---

## Executive Summary

The committed discovery seam in `src/storage/fs/repo_discovery.rs` identifies terminal manifest directory paths (`repos/**/manifests`) beneath the storage root, returning a collection of verified `ObjectKey`s (including root-adjacent `repos/manifests` and reserved-ancestor locations such as `repos/blobs/internal/manifests`). By design, that seam intentionally does **not** open terminal directories, enumerate their contents, read payloads, parse references, or compute reachability.

This document designs the smallest next test-only layer: **Contained GC Manifest Enumeration and Reference Collection**.

This layer consumes observed terminal `ObjectKey`s, enumerates candidate manifest entries within them beneath the same pinned directory descriptor, reads and buffers their payloads, and extracts referenced content digests using canonical `parse_manifest_refs`.

```
+-----------------------------------------------------------------------------+
|                                Storage Root                                 |
|            (Pinned descriptor via O_PATH | O_DIRECTORY | O_CLOEXEC)         |
+-----------------------------------------------------------------------------+
                                       |
                   1. Directory Discovery (Committed Seam)
                      (discover_manifest_dirs_impl via openat2)
                                       v
         Observed Terminal ObjectKeys: &[ObjectKey]
         - repos/library/ubuntu/manifests
         - repos/tags/sub1/sub2/manifests
         - repos/manifests  (root-adjacent)
         - repos/blobs/internal/manifests  (reserved ancestor)
                                       |
                   2. Terminal Directory Enumeration (This Seam)
                      (reader.enumerate_dir(Some(&dir_key), limits))
                                       v
         Candidate Manifest Entries: HashSet<ObjectKey>
         - repos/library/ubuntu/manifests/<sha256_hex>
         - repos/manifests/<sha256_hex>
                                       |
                   3. Descriptor-Relative Payload Reading (This Seam)
                      (reader.open_payload(&manifest_key) via procfs reopen)
                                       v
         Buffered Payload Bytes: Vec<u8> (with sentinel-byte oversize check)
                                       |
                   4. Reference Extraction (This Seam)
                      (parse_manifest_refs(&bytes))
                                       v
         Observed Reference Set: ManifestReferenceObservationSet
         - Protected digests: HashSet<Digest> (roots + references)
         - Verified accounting: exact logical byte and counter budgets
```

This proposal remains strictly documentation-only. Production GC routing, catalog discovery, and `storage-layer-rust` remain 100% unchanged. This seam has no production caller. All eight canonical quality gates (**O-03, O-04, O-05, O-06, O-13, O-15, O-16, D-06**) remain OPEN.

---

## 1. Exact Inputs, Output, and Reader Ownership

### 1.1 Exact Inputs and Outputs

#### Inputs
The reference collection seam accepts:
1. `reader`: Reference to the pinned root descriptor instance (`&impl ManifestRefReader`).
2. `terminal_dirs`: A slice of discovered manifest directory `ObjectKey`s (`&[ObjectKey]`) produced by `discover_manifest_dirs_impl`.
3. `limits`: Caller-supplied test-only limits [`ManifestReferenceTestLimits`](#4-implementable-resource-accounting).

#### Output
```rust
pub struct ManifestReferenceObservationSet {
    /// Deduplicated set of all observed protected digests (manifest roots + referenced blobs/manifests).
    pub protected_digests: HashSet<Digest>,
    /// Exact count of terminal directories where enumeration was attempted.
    pub terminal_dirs_enumerated: usize,
    /// Exact count of manifest files successfully opened, read, and parsed.
    pub manifests_parsed: usize,
    /// Exact total directory entries inspected across all terminal directories.
    pub total_dirents_observed: usize,
    /// Total logical bytes accounted across seen object keys and protected digests.
    pub retained_logical_bytes: usize,
    /// Cumulative payload bytes read across all manifests.
    pub total_payload_bytes_read: u64,
}
```
The output is returned as `Result<ManifestReferenceObservationSet, StorageError>`.

### 1.2 Reader Ownership and Unified Descriptor Topology

A central requirement is demonstrating how the **same reader instance** is used across all three operations:
1. Directory discovery (`repos/**/manifests`)
2. Terminal directory enumeration (`repos/.../manifests/*`)
3. Payload opening (`repos/.../manifests/<digest>`)

In production `FsStorage` (`src/storage/fs.rs:196`), the reader is held as:
```rust
reader: std::sync::Arc<storage_fs::FsMetadataReader>
```
`storage_fs::FsMetadataReader` owns a pinned Linux file descriptor to the storage root directory (`root_fd: Arc<std::fs::File>`) opened with `O_PATH | O_DIRECTORY | O_CLOEXEC`.

Relative path containment operates beneath this root descriptor using Linux `openat2` with flags:
`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`.

The reader exposes two core capabilities:
1. **Directory Enumeration**:
   ```rust
   // storage-fs/src/reader.rs
   pub async fn enumerate_dir(
       &self,
       target: Option<&ObjectKey>,
       limits: DirEnumerationLimits,
   ) -> Result<Vec<DirEntry>, FsDirError>
   ```
2. **Payload Reading** (via `storage_core::ObjectPayloadReader` implementation):
   ```rust
   // storage-core/src/lib.rs
   #[async_trait]
   pub trait ObjectPayloadReader: Send + Sync {
       async fn open_payload(&self, key: &ObjectKey) -> Result<ObjectPayload, ReadError>;
   }
   ```
   On Linux, `open_payload` performs an initial `openat2` under containment with `O_PATH`, validates `S_IFREG`, and reopens a readable file descriptor via `/proc/self/fd/N`.

### 1.3 Eliminating Flawed Routing Conventions

#### Prohibition: No Routing Through Empty Repository Strings
In existing production manifest reading (`src/storage/fs/manifest.rs:51`), the key is formed via:
```rust
pub(crate) fn manifest_key(repo: &str, digest: &Digest) -> Result<ObjectKey, StorageError>
```
That function explicitly enforces `if repo.is_empty() { return Err(StorageError::InvalidRepoName(...)); }`.
Attempting to route a root-adjacent manifest (`repos/manifests/<digest>`) by passing `repo = ""` is rejected by validation. Attempting to bypass that check would compose an invalid double-slash key `repos//manifests/<digest>`.

**Direct Resolution**:
The reference collection seam operates directly on [`ObjectKey`].
Given a terminal directory `dir_key: &ObjectKey` (e.g. `repos/manifests` or `repos/library/ubuntu/manifests`) and an observed entry `filename: &str`:
```rust
let manifest_key_str = format!("{}/{}", dir_key.as_str(), filename);
let manifest_key = ObjectKey::parse(&manifest_key_str)
    .map_err(|e| StorageError::corrupt_data(format!("invalid manifest object key: {e}")))?;
```
Because `dir_key` is already a validated `ObjectKey` under `repos/` and `filename` is a validated lowercase hexadecimal string, `manifest_key` is safely composed without invoking repository-name parsers or passing empty strings.

#### Prohibition: No Invented `read_manifest` Method
The seam does not invent a fictional `read_manifest(repo, digest)` helper.
Instead, it invokes the established descriptor-relative payload opening API:
```rust
let payload = reader
    .open_payload(&manifest_key)
    .await
    .map_err(|err| translate_manifest_payload_error(err, &manifest_key))?;

let (_meta, stream) = payload.into_parts();
let bytes = read_payload_stream_bounded(stream, limits.max_manifest_payload_bytes).await?;
```
This reuses the identical containment semantics, `openat2` resolution flags, and `/proc/self/fd/N` reopening path validated by `storage-fs`.

---

## 2. Discovery Scope, Completeness Boundaries, and GC Safety Claims

### 2.1 Nature of Observed Terminal Keys and Traversal Boundaries
Terminal `ObjectKey`s returned by `discover_manifest_dirs_impl` are **observed paths** yielded by parent directory listings during breadth-first traversal. They are not pre-opened, locked, or verified-open directory descriptors.

The seam does **not** provide a guarantee that every child manifest on disk will be discovered or read:
1. **Filename / Type Filtering**: Files with non-hex names, uppercase hex, temporary prefixes (`.tmp.*`), lock prefixes (`.lock.*`), non-regular entry types (symlinks, subdirectories, FIFOs), or non-UTF-8 bytes are skipped during enumeration.
2. **Traversal Boundaries**: Directory discovery limits (`max_depth`, `max_dir_enumerations`, `max_total_entries`, `max_retained_path_bytes`) and terminal directory limits (`max_entries`, `max_total_name_bytes`) strictly bound traversal. If a budget trips, uninspected directories or files remain unread.
3. **Concurrent Storage Changes**: Because directory enumeration occurs over iterative `getdents64` system calls without filesystem snapshots, files added, moved, or deleted during traversal are not transactionally isolated.

### 2.2 Record-Only Reference Collection as an Explicit Experimental Choice
In `BlobRefIndex::ingest_root`, child manifests referenced in an index are added to a queue and recursively fetched via `storage.get_manifest(repo, &child_digest)`.

In contrast, the direct GC walker (`build_manifest_protected_set_fs` in `src/blob_gc/policy.rs:255`) merely records `refs.all_references()` into its protected set without recursive fetch loops.

**Explicit Compatibility Choice**:
This seam preserves that record-only behavior without recursive fetching:
- It records all extracted references (`config`, `layers`, `blobs`, `manifests`, `subject`) into `protected_digests`.
- It does **not** attempt to recursively open child manifests referenced by image indexes.
- *Rationale*: In standard filesystem storage, child manifests pushed to a repository are written as separate files in `manifests/` and are encountered during terminal directory enumeration if present. If a child manifest is missing on disk, recording its digest protects it if it exists in CAS, while avoiding spurious read failures for dangling references.
- This is an **explicit experimental compatibility choice** aligning with `build_manifest_protected_set_fs`, not a completeness proof.

### 2.3 Corrected GC Safety Claims and Layered Defense

> [!IMPORTANT]
> **Corrected Safety Understanding**:
> An omission in manifest reference extraction does **not** directly cause immediate permanent deletion of a blob, nor does a discovery error guarantee that no earlier candidate mutations occurred.

#### A. Multi-Layered GC Safety Architecture
In `registry-rust`, garbage collection reachability is protected by multiple independent safeguards operating in series:
1. **Manifest-Rooted Protection**: Evaluates reachability from repository manifests (the mechanism addressed by this seam).
2. **Reference-Index Reachability (`BlobRefIndex`)**: Evaluates DAG reachability from tagged roots and reverse edges.
3. **Pinned Manifests & Upload Leases**: In-memory and index pins protect active pushes.
4. **Repository Memberships**: Validates blob associations.
5. **Minimum Candidate Age (`min_age`)**: Unreferenced blobs younger than `min_age` are ineligible for quarantine.
6. **Quarantine Delay (`quarantine_delay`)**: Quarantined blobs must age past `quarantine_delay` before deletion.
7. **Mutation Authority (`GcMutationAuthority`)**: Requires an active authority lease before executing mutations.
8. **Consistency Revalidation (`acquire_gc_revalidation`)**: Re-evaluates `policy_ctx.is_referenced(&candidate.digest)` under a consistency lock immediately before each mutation.

An omitted manifest reference **weakens one protection mechanism** (manifest-rooted protection for untagged manifests). A blob is only at risk of deletion if all other safeguards (tag reachability, minimum age, quarantine delay, revalidation) also fail to protect it.

#### B. Error Propagation vs. Irreversible Prior Mutations
- A failed observation in this seam returns `Err(StorageError)` with **no partial successful set**.
- Future caller integration must propagate this error to halt the GC process.
- However, as documented in `filesystem-gc-manifest-discovery-characterization.md`, callers in quarantine (`blob_gc_quarantine_with_authority`) and deletion (`blob_gc_delete_with_authority`) invoke policy checks **inside candidate loops**. If an error occurs on candidate $N$, the run halts, but **earlier mutations (quarantines or deletions) for candidates $1 \dots N-1$ cannot be undone**.
- **Current Status**: This seam remains test-only and has no production caller.

---

## 3. Source-Checked Parser Behavior (`parse_manifest_refs`)

### 3.1 Mechanically Verified Parser Analysis

Source inspection of `src/manifest_refs.rs` (commit `3bbe006148c83add6acfb2f7e0c5df9a21d38b7e`, lines 60–164) establishes the exact behavior of `parse_manifest_refs`:

#### What `parse_manifest_refs` Validates
1. **JSON Syntax**: Validates that input bytes are valid JSON via `serde_json::from_slice(bytes)`. Returns `ManifestParseError::InvalidJson` on syntax errors.
2. **Root JSON Object**: Validates that top-level JSON is an object (`v.as_object()`). Returns `ManifestParseError::NotAnObject` for arrays, numbers, strings, or booleans.
3. **Descriptor Structures**:
   - For `"manifests"`, `"layers"`, `"blobs"`: If present and not null, validates that the value is a JSON array. Each element must be a JSON object containing a string field `"digest"`.
   - For `"config"`, `"subject"`: If present and not null, validates that the value is a JSON object containing a string field `"digest"`.
4. **Digest Grammar**: Validates the string in `"digest"` using `Digest::parse(digest_str)`. Returns `ManifestParseError::InvalidDigest` if algorithm prefix or hex characters are malformed.

#### What `parse_manifest_refs` Ignores
1. **`schemaVersion`**: Does **not** inspect, check, or validate `schemaVersion`. (The variants `SchemaV1Unsupported` and `DockerV1Unsupported` exist in the enum `ManifestParseError`, but are never returned by `parse_manifest_refs`).
2. **`mediaType`**: Does **not** validate `mediaType` or `config.mediaType`.
3. **Descriptor Metadata**: Ignores `size`, `urls`, `annotations`, `data`, `platform`, and custom fields.
4. **Unknown / Extra Fields**: Silently ignores all unrecognized top-level fields (e.g. `"signatures"`, `"history"`).
5. **Missing Fields**: If optional fields (`config`, `layers`, etc.) are absent or null, it treats them as empty and returns `Ok(ManifestRefs::default())`. An empty JSON object `{}` parses successfully with 0 references.

#### Three Distinct Verification Layers
It is critical to separate three distinct concepts:
- **Reference Extraction (`parse_manifest_refs`)**: Deserializes JSON and extracts descriptor digests. Performs no schema enforcement or payload integrity hashing.
- **Schema Validation (`detect_manifest_media_type`)**: Separate registry helper (`src/storage/fs/manifest.rs:101`) that extracts the top-level string `"mediaType"`. Not invoked by `parse_manifest_refs`.
- **Payload Content-Digest Verification**: Computing cryptographic hashes over payload bytes to verify they match the requested digest. **Not performed** by existing production code or this seam.

---

## 4. Implementable Resource Accounting

To prevent unbounded resource consumption, all memory and iteration limits are defined in terms of **deterministic logical accounting**.

```rust
pub struct ManifestReferenceTestLimits {
    /// Maximum directory enumeration calls across all terminal directories.
    pub max_terminal_dir_enumerations: usize,
    /// Per-directory limits passed to each reader.enumerate_dir call.
    pub per_dir_limits: storage_fs::DirEnumerationLimits,
    /// Maximum directory entries inspected across all terminal directories combined.
    pub max_total_manifest_entries: usize,
    /// Maximum manifest payload files opened and parsed.
    pub max_manifests_read: usize,
    /// Maximum unique digests permitted in the protected reference set.
    pub max_total_references: usize,
    /// Maximum cumulative logical path and digest bytes retained.
    pub max_retained_logical_bytes: usize,
    /// Optional ceiling on single manifest payload size (test-only).
    pub max_manifest_payload_bytes: Option<u64>,
}
```

### 4.1 Supplying Terminal Directory Limits
Per-directory enumeration limits are supplied via `limits.per_dir_limits` (of type `storage_fs::DirEnumerationLimits`). This limits the entries and name bytes fetched during each `reader.enumerate_dir(Some(&dir_key), limits.per_dir_limits)` invocation.

### 4.2 Deduplication of Terminal Inputs & Enumeration Accounting
1. **Deduplicating Terminal Inputs**:
   - The caller supplies `terminal_dirs: &[ObjectKey]`.
   - The seam maintains a set `seen_terminal_dirs: HashSet<ObjectKey>`.
   - If an input `dir_key` was already processed, it is skipped without incrementing enumeration counters or re-enumerating the directory.
2. **Enumeration Attempt Counter**:
   - `terminal_dirs_enumerated` counts attempted directory enumeration calls.
   - **Pre-check**: Before calling `reader.enumerate_dir`, the seam checks `if terminal_dirs_enumerated >= limits.max_terminal_dir_enumerations`. If equal or greater, it fails closed with `StorageError::backend("terminal directory enumeration limit exceeded")`.
   - The counter is incremented using `checked_add(1)`.
   - A directory that returns `NotFound` or fails resolution consumed an attempt and is counted before returning an error.

### 4.3 Seen Object Keys vs. Identical Digests at Distinct Paths
The seam maintains:
- `seen_manifest_keys: HashSet<ObjectKey>`: Tracks exact manifest paths that have been processed.
- `protected_digests: HashSet<Digest>`: The accumulated protected digest set.

#### Behavior for Identical Digests at Distinct Paths
> [!IMPORTANT]
> **Path Deduplication Policy**:
> Deduplicate repeated identical object keys, but **do not skip reading distinct paths solely because their filename digests match**.

- If the exact same object key (e.g. `repos/ubuntu/manifests/sha256_abc`) is encountered twice (e.g. duplicate dirents yielded across `getdents64`), `seen_manifest_keys.insert` returns `false` and it is skipped.
- However, if the same digest appears at different object keys:
  - Path 1: `repos/ubuntu/manifests/sha256_abc`
  - Path 2: `repos/debian/manifests/sha256_abc`
- **Both paths must be read and parsed**. Distinct filesystem paths must not be skipped based on filename digests alone, because concurrent mutations or corruptions could cause payloads to diverge.
- If both manifests parse successfully and yield identical references, `protected_digests` naturally deduplicates the extracted digests.

### 4.4 Exact Counter and Logical Byte Accounting

All counters use checked arithmetic. Any overflow returns `StorageError::backend("arithmetic overflow in accounting")`.

| Metric | When Checked | When Charged | Overflow Handling |
| :--- | :--- | :--- | :--- |
| `terminal_dirs_enumerated` | Before `enumerate_dir` | Immediately prior to call | `checked_add(1)` |
| `total_manifest_entries` | Before processing each dirent | For every dirent returned | `checked_add(1)` |
| `manifests_read` | Before `open_payload` | Immediately prior to call | `checked_add(1)` |
| `protected_digests` count | Before insertion | On `insert == true` | Checked against `max_total_references` |
| `retained_logical_bytes` | Before retaining key / digest | When string is stored | `checked_add(len)` |

#### Precise Logical Byte Accounting (Excluding Allocator Overhead)
To replace vague "maximum memory allocated" claims, the seam enforces **strictly defined logical byte accounting**:
- **Included**:
  1. Exact byte length of `ObjectKey.as_str().len()` for each key stored in `seen_terminal_dirs`.
  2. Exact byte length of `ObjectKey.as_str().len()` for each key stored in `seen_manifest_keys`.
  3. Exact byte length of `Digest.as_str().len()` for each digest stored in `protected_digests`.
- **Explicitly Excluded**:
  - Rust allocator capacity and heap growth factors (`capacity() > len()`).
  - Standard library collection overhead (`HashSet` hash tables, buckets, control bytes).
  - Temporary allocations during JSON deserialization (`serde_json::Value`).
  - Directory enumeration buffer batches allocated inside `storage-fs`.
  - Buffered payload bytes in memory during streaming.

### 4.5 Test-Only Payload Ceiling: Sentinel-Byte Oversize Detection

Streaming an unbounded file into memory with `stream.take(limit).read_to_end(&mut buf)` will silently truncate oversized files at `limit` bytes and report success, causing corrupt JSON errors downstream.

To implement exact-boundary success and oversized-stream detection:
```rust
async fn read_payload_stream_bounded<S>(
    mut stream: S,
    max_bytes: Option<u64>,
) -> Result<Vec<u8>, StorageError>
where
    S: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let Some(limit) = max_bytes else {
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await
            .map_err(|e| StorageError::io(format!("payload read stream error: {e}")))?;
        return Ok(buf);
    };

    // Sentinel byte: read up to limit + 1
    let sentinel_limit = limit.checked_add(1)
        .ok_or_else(|| StorageError::backend("payload limit arithmetic overflow"))?;

    let mut buf = Vec::new();
    let mut take_stream = stream.take(sentinel_limit);
    take_stream.read_to_end(&mut buf).await
        .map_err(|e| StorageError::io(format!("payload read stream error: {e}")))?;

    if buf.len() as u64 > limit {
        return Err(StorageError::backend(format!(
            "manifest payload exceeded size ceiling of {limit} bytes"
        )));
    }

    Ok(buf)
}
```
- If `buf.len() == limit`: Exact boundary succeeds.
- If `buf.len() > limit`: Oversized stream detected immediately via the sentinel byte and rejected with `StorageError::backend`.
- This is a test-only bounded stream guard, not a whole-process heap bound.

---

## 5. Complete Error Mapping & Payload Translator Override

### 5.1 Analysis of Existing `translate_payload_read_error`
In production `src/storage/fs/read_adapter.rs:93`, `translate_read_error` handles `storage_core::ReadError`:
```rust
storage_core::ReadError::NotFound { .. } => StorageError::NotFound,
storage_core::ReadError::PermissionDenied { .. } => StorageError::io(...),
```
1. **Flaw 1 (Missing Observed Object)**: For public CAS reads, `ReadError::NotFound` returns `StorageError::NotFound`. But in this seam, an observed manifest disappearing between enumeration and open is a **TOCTOU error**. Silently returning `StorageError::NotFound` would cause ambiguity with missing roots. It must be mapped to `StorageError::io` with object-key context.
2. **Flaw 2 (Permission Denied)**: `translate_read_error` maps `PermissionDenied` to `StorageError::io("permission denied")`. This loses the strongly typed `StorageErrorKind::PermissionDenied` taxonomy used by `FsDirError::PermissionDenied`.

### 5.2 Seam-Specific Error Mapping Specification

The seam explicitly overrides these mappings to preserve context and fail closed:

| Cause / System Error | Source Type | Seam Error Mapping | Context Included |
| :--- | :--- | :--- | :--- |
| **Observed Terminal Dir Vanished** | `FsDirError::NotFound` | `StorageError::io(...)` | `format!("observed terminal directory vanished: {dir_key}")` |
| **Observed Manifest Vanished** | `ReadError::NotFound` | `StorageError::io(...)` | `format!("observed manifest vanished: {manifest_key}")` |
| **Terminal Permission Denied** | `FsDirError::PermissionDenied` | `StorageError::permission_denied(...)` | `source.to_string()` |
| **Payload Permission Denied** | `ReadError::PermissionDenied` | `StorageError::permission_denied(...)` | `format!("permission denied opening: {manifest_key}")` |
| **Syscall Unsupported (openat2)** | `FsDirError / ReadError` | `StorageError::configuration(...)` | `"openat2 is unavailable in this execution environment"` |
| **Platform Unsupported (non-Linux)**| `FsDirError / ReadError` | `StorageError::configuration(...)` | `"descriptor containment requires Linux openat2"` |
| **Runtime Missing / Task Join** | `FsDirError / ReadError` | `StorageError::backend(...)` | `"tokio runtime missing" / "task join failed"` |
| **Resource Limit Exceeded** | `FsDirError::LimitExceeded` | `StorageError::backend(...)` | Reason details from enumeration limits |
| **Seam Accounting Exceeded** | Seam counter check | `StorageError::backend(...)` | Exact limit and counter values |
| **Wrong Object Type (not regular)** | `ReadError::Backend (mode)` | `StorageError::corrupt_data(...)` | `format!("target is not a regular file: {manifest_key}")` |
| **Path Symlink Resolution Rejected**| `ReadError / FsDirError` | `StorageError::io(...)` | `"path resolution rejected (symlink disallowed)"` |
| **Payload Stream I/O Error** | `std::io::Error` | `StorageError::io(...)` | Stream read failure message |
| **Malformed Manifest JSON / Ref** | `ManifestParseError` | `StorageError::corrupt_data(...)` | `format!("malformed manifest {manifest_key}: {err}")` |

All errors use existing canonical constructors (`StorageError::io`, `StorageError::permission_denied`, `StorageError::configuration`, `StorageError::corrupt_data`, `StorageError::backend`) without inventing new error variants.

---

## 6. Injectable Test Interfaces & Orchestration Topology

### 6.1 Composing with `DiscoveryDirEnumerator`

The committed discovery seam in `src/storage/fs/repo_discovery.rs:75` defines:
```rust
#[async_trait]
pub(crate) trait DiscoveryDirEnumerator: Send + Sync {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError>;
}
```

To leave `repo_discovery.rs` **completely unchanged**, the reference collection seam defines its reader contract by composing `DiscoveryDirEnumerator` with `storage_core::ObjectPayloadReader`:

```rust
#[async_trait]
pub(crate) trait ManifestRefReader:
    super::repo_discovery::DiscoveryDirEnumerator + storage_core::ObjectPayloadReader
{
    // Blanket or marker composition; inherits enumerate_dir and open_payload
}

// Blanket implementation for any type implementing both traits
impl<T> ManifestRefReader for T where
    T: super::repo_discovery::DiscoveryDirEnumerator + storage_core::ObjectPayloadReader + ?Sized
{
}
```

Because `storage_fs::FsMetadataReader` already implements both `DiscoveryDirEnumerator` (in `repo_discovery.rs`) and `ObjectPayloadReader` (in `crates/storage-fs`), it **automatically implements `ManifestRefReader`** without any glue code!

### 6.2 Test-Only Orchestration Signature
Using this composition, a unified test-only orchestration function executes discovery, terminal enumeration, and payload reading over the **exact same reader instance**:

```rust
pub(crate) async fn collect_manifest_references_end_to_end<R>(
    reader: &R,
    discovery_limits: super::repo_discovery::DiscoveryTestLimits,
    ref_limits: ManifestReferenceTestLimits,
) -> Result<ManifestReferenceObservationSet, StorageError>
where
    R: ManifestRefReader + ?Sized,
{
    // 1. Directory Discovery: delegates to committed discovery seam
    let terminal_dirs = super::repo_discovery::discover_manifest_dirs_impl(reader, discovery_limits).await?;

    // 2. Manifest Reference Collection: processes discovered terminal directories
    collect_manifest_references_impl(reader, &terminal_dirs, ref_limits).await
}
```

---

## 7. Extended Test Matrix (Planned Tests)

> [!NOTE]
> The following test scenarios represent the planned test matrix for the subsequent implementation slice. None of these tests are claimed as executed evidence at this stage.

### 7.1 Planned Unit and Deterministic Fake Scenarios
1. **Identical Digest at Distinct Object Paths**:
   - Fixture provides `repos/app1/manifests/sha256_1111...` and `repos/app2/manifests/sha256_1111...` with different child layers.
   - Asserts that both paths are opened and read. Asserts all distinct references from both are present in `protected_digests`.
2. **Duplicate Terminal Inputs**:
   - Caller passes `&[repos/manifests, repos/manifests]`.
   - Asserts directory is enumerated exactly once; `terminal_dirs_enumerated == 1`.
3. **Duplicate Dirent Observations**:
   - `enumerate_dir` returns two identical dirents for `sha256_1111...` in the same directory.
   - Asserts payload is opened and read exactly once.
4. **Missing Observed Terminal Directory**:
   - `enumerate_dir` returns `FsDirError::NotFound` for an input terminal key.
   - Asserts immediate fail-closed error `StorageError::io` containing the terminal key.
5. **Missing Observed Manifest Payload**:
   - `open_payload` returns `ReadError::NotFound` for an observed manifest file.
   - Asserts immediate fail-closed error `StorageError::io` containing the manifest key.
6. **Wrong-Type Payload Replacement**:
   - File changes to a FIFO or directory before open; returns `UnsupportedObjectType`.
   - Asserts fail-closed with `StorageError::corrupt_data`.
7. **Symlink Resolution Rejection**:
   - Target is a symlink; returns `ResolutionRejected`.
   - Asserts fail-closed with `StorageError::io`.
8. **Payload Stream Failure Mid-Read**:
   - Stream yields partial bytes then returns I/O error.
   - Asserts fail-closed with `StorageError::io`.
9. **Malformed Manifest JSON**:
   - Payload contains invalid JSON or malformed descriptor digest.
   - Asserts fail-closed with `StorageError::corrupt_data`.
10. **Failure After Earlier Success With No Later Reads**:
    - Directory 1 succeeds and records references.
    - Directory 2 fails (e.g. read error on manifest 1).
    - Asserts operation halts immediately: returns `Err`, yields zero partial results, and records zero subsequent `open_payload` calls.
11. **Exact vs. One-Over Limit Boundaries**:
    - Exact boundary success for `max_manifests_read == 2`.
    - One-over failure for `max_manifests_read == 1` when 2 manifests are present.
    - Exact boundary and one-over failure for `max_total_manifest_entries` and `max_retained_logical_bytes`.
12. **Arithmetic Overflow Protection**:
    - Helpers tested with `usize::MAX` values to verify `checked_add` failure paths.
13. **Payload Ceiling Boundary and Sentinel Detection**:
    - Manifest payload of exactly $N$ bytes succeeds when limit is $N$.
    - Manifest payload of $N + 1$ bytes fails closed with `StorageError::backend` (oversize stream detected via sentinel byte).
14. **Same-Reader End-to-End Fake Integration**:
    - Verifies `collect_manifest_references_end_to_end` executes discovery and reference collection sequentially over a single `RecordingFakeManifestRefReader`.

### 7.2 Planned Real Filesystem Linux Tests (`#[cfg(target_os = "linux")]`)
15. **Real Root-Adjacent & Reserved-Ancestor Manifest Layout**:
    - Verifies real filesystem discovery and reading for `repos/manifests/<digest>` and `repos/tags/sub/manifests/<digest>`.
16. **Pinned Reader Across Root Renaming**:
    - Verifies pinned descriptor continues reading original manifests after storage root directory is renamed.

---

## 8. Operational Limitations & Environmental Dependencies

1. **Iterative Enumeration Without Snapshot Isolation**:
   - Traversal observes entries across iterative `getdents64` and `openat2` calls.
   - Files added, unlinked, or renamed concurrently are not transactionally isolated.
2. **Unbounded Production Manifest Buffering**:
   - Production `manifest::get_manifest_impl` buffers the entire payload in memory without an enforced size ceiling.
   - The test-only ceiling proposed here is experimental; establishing a production ceiling requires an independent RFC.
3. **Genuine, Accessible, Stable Procfs Requirement**:
   - Reopening `O_PATH` descriptors into readable file handles via `/proc/self/fd/N` requires an accessible, stable Linux procfs mount.
4. **No Mount or Hard-Link Isolation**:
   - `openat2` with `RESOLVE_BENEATH` confines lookups beneath the storage root descriptor, but does not isolate nested mounts or hard links within the tree.
5. **Divergence Across Host Root Replacement**:
   - A pinned reader remains bound to the original directory inode. If an administrator renames or swaps the root directory on the host, the pinned reader continues reading the old tree.
6. **Non-Linux Platforms Unverified**:
   - Descriptor-relative containment and `openat2` are Linux-specific. On other platforms, calls fail closed with `Configuration` errors.
7. **Experimental Digest Conventions**:
   - Accepting SHA-512 (128-hex) filenames and skipping uppercase hex are experimental choices that require separate production approval before altering production GC policy.

---

## 9. Mechanically Extracted Source Evidence

### 9.1 Parser Behavior: What `parse_manifest_refs` Validates and Ignores
**File**: `registry-rust/src/manifest_refs.rs` (commit `3bbe006148c83add6acfb2f7e0c5df9a21d38b7e`)
```rust
// Lines 60-70
pub fn parse_manifest_refs(bytes: &[u8]) -> Result<ManifestRefs, ManifestParseError> {
    let v: serde_json::Value =
        serde_json::from_slice(bytes).map_err(ManifestParseError::InvalidJson)?;

    let obj = v.as_object().ok_or(ManifestParseError::NotAnObject)?;
    let mut refs = ManifestRefs::default();

    // 1. manifests: optional array of descriptor objects
    if let Some(val) = obj.get("manifests") {
// Lines 123-132
    // 5. subject: optional descriptor object
    if let Some(val) = obj.get("subject") {
        if !val.is_null() {
            let digest = parse_descriptor_digest(val, "subject")?;
            refs.subject = Some(digest);
        }
    }

    Ok(refs)
}
```
*Proof*: The parser extracts `manifests`, `config`, `layers`, `blobs`, and `subject`. It never inspects `schemaVersion` or `mediaType`.

### 9.2 Existing Payload Read Translator Maps `NotFound` to `StorageError::NotFound`
**File**: `registry-rust/src/storage/fs/read_adapter.rs` (commit `3bbe006148c83add6acfb2f7e0c5df9a21d38b7e`)
```rust
// Lines 93-96
pub(crate) fn translate_read_error(err: storage_core::ReadError, op: ReadOp) -> StorageError {
    match err {
        storage_core::ReadError::NotFound { .. } => StorageError::NotFound,
// Lines 175-178
pub(crate) fn translate_payload_read_error(err: storage_core::ReadError) -> StorageError {
    translate_read_error(err, ReadOp::Payload)
}
```
*Proof*: Demonstrates why the seam must override `translate_payload_read_error` to map missing observed manifest payloads to `StorageError::io` instead of `StorageError::NotFound`.

### 9.3 Discovery Trait Definition in `repo_discovery.rs`
**File**: `registry-rust/src/storage/fs/repo_discovery.rs` (commit `3bbe006148c83add6acfb2f7e0c5df9a21d38b7e`)
```rust
// Lines 75-88
#[async_trait]
pub(crate) trait DiscoveryDirEnumerator: Send + Sync {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError>;
}
```

---

## 10. Baseline and Document Integrity Register

| File Path | Repository | Commit HEAD / State | SHA-256 Checksum |
| :--- | :--- | :--- | :--- |
| `docs/architecture/filesystem-gc-manifest-reference-seam-design.md` | `registry-rust` | Untracked (This Document) | *(Computed upon generation)* |
| `src/storage/fs/repo_discovery.rs` | `registry-rust` | `3bbe006148c83add6acfb2f7e0c5df9a21d38b7e` | `45559e1590828ba296a2dca276e49f335e70e901b673848742a9d38741254ca7` |
| `src/storage/fs/manifest_listing.rs` | `registry-rust` | `3bbe006148c83add6acfb2f7e0c5df9a21d38b7e` | `d44b0b024be2a6e23d29a186abf70c029d412446c1fe576cd979726930a32bf8` |
| `src/storage/fs/manifest.rs` | `registry-rust` | `3bbe006148c83add6acfb2f7e0c5df9a21d38b7e` | `202e9b9104bc4f70e9eb533de1e020373b7cfc9e82aa9c69d7d6fdfcd68c86a1` |
| `src/storage/fs/read_adapter.rs` | `registry-rust` | `3bbe006148c83add6acfb2f7e0c5df9a21d38b7e` | `cfba78d2b27dfc1d56350eb93aa426c11da8be96752763aa7bb7df5e97148fb1` |
| `src/storage/fs.rs` | `registry-rust` | `3bbe006148c83add6acfb2f7e0c5df9a21d38b7e` | `4c8ec8a86b307d6697082388b4bfeaf96b01cfa54ad90a4f99f2f75fff63c96b` |
| `src/manifest_refs.rs` | `registry-rust` | `3bbe006148c83add6acfb2f7e0c5df9a21d38b7e` | `3227b030b9479e0ae29d62a83f726ebb7841391c8af97eefc0a7f16fe37f3367` |
| `src/blob_gc/policy.rs` | `registry-rust` | `3bbe006148c83add6acfb2f7e0c5df9a21d38b7e` | `b5fb40df976f8cb9d3ad062a2497b58dcfe2612584aa42a13465704d9ac19094` |
| `crates/storage-fs/src/reader.rs` | `storage-layer-rust` | `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` | `d5b797e28a975ea0583e82ca82ac2ee9da969873feb684f691e13c4b0ee41c5b` |
| `crates/storage-core/src/lib.rs` | `storage-layer-rust` | `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` | `04c6ee00e3364f495bf07a571a77deb19cbc1742a57b2dfcf5dbc2f12304e7b4` |
| `crates/storage-core/src/key.rs` | `storage-layer-rust` | `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` | `d4fc9afd1e77ec79b0f7786b2313606a2f71e939f73da240f54c43baa48004af` |
