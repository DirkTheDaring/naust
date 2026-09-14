# Contained Filesystem GC Manifest Discovery: Production Integration Design (Corrected)

## Status & Governance
- **Date**: 2026-09-12
- **Document Path**: `docs/architecture/filesystem-gc-contained-discovery-production-integration-design.md`
- **State**: **DOCUMENTATION-ONLY DESIGN — READY FOR REVIEW — PRODUCTION UNCHANGED — NOT COMMITTED**
- **Authoritative Baselines**:
  - `registry-rust` HEAD: `d51ea1ac69921dfdbd583b6b197b80b1f149e232`
  - `storage-layer-rust` HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`
- **Preserved Archive Register**:
  - `session-20260912-0310/filesystem-gc-contained-discovery-production-integration-design.tar.gz`
    - Size: 25,521 bytes
    - SHA-256: `b2dd186feb61f72c2e276c6cebdb7f370d0124f1773805e0e99766f1d2758959`
  - `session-20260912-0300/filesystem-gc-contained-discovery-production-integration-design.tar.gz`
    - Size: 19,989 bytes
    - SHA-256: `f59018c7d31c0c89aa21f657597be74089641347f5dd4d74f4d625ad6df6fd9b`
  - `session-20260912-0250/filesystem-gc-contained-discovery-production-integration-design.tar.gz`
    - Size: 22,016 bytes
    - SHA-256: `78a6bcb120d30c71714fa18a04bf1a112419baf7da49136fc8dbe829db254b7c`
- **Quality Gates**:
  - Canonical gates **O-03, O-04, O-05, O-06, O-13, O-15, O-16, and D-06 remain explicitly OPEN**.

---

## 1. Executive Summary & Routing Architecture

Under `BlobGcPolicy::ManifestRooted`, garbage collection (GC) reachability analysis in `registry-rust` constructs an in-memory set of protected content digests reachable from stored container manifests. Currently, production GC on the filesystem backend bypasses contained storage abstractions:
1. `build_manifest_protected_set` in [`src/blob_gc/policy.rs:166-225`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/policy.rs#L166-L225) executes an uncontained bypass walker [`build_manifest_protected_set_fs`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/policy.rs#L227-L299) whenever `storage.kind() == "fs"` and `cfg.fs_root.join("repos")` exists on disk.
2. This direct bypass traverses raw disk paths using uncontained `tokio::fs::read_dir`, completely bypassing the pinned directory descriptor owned by [`FsStorage`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L190-L200) and ignoring descriptor containment constraints (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`).
3. If the bypass is not taken, execution falls back to `storage.list_repositories()`, which invokes [`FsStorage::list_repo_names`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L566-L641). As established in empirical characterization, public catalog discovery is **not** equivalent to GC reachability discovery: it omits root-adjacent manifests (`repos/manifests`), skips repositories beneath reserved segments (`repos/tags/...`, `repos/blobs/...`, `repos/meta/...`, `repos/referrers/...`), suppresses structural errors, and operates on unpinned pathnames.

Two test-only seams are committed to `registry-rust`:
- [`src/storage/fs/repo_discovery.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/repo_discovery.rs) (commit `3bbe006148c83add6acfb2f7e0c5df9a21d38b7e`): Discovers terminal manifest-directory `ObjectKey`s beneath the pinned root descriptor.
- [`src/storage/fs/manifest_refs_seam.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/manifest_refs_seam.rs) (commit `d51ea1ac69921dfdbd583b6b197b80b1f149e232`): Enumerates terminal directories beneath the same reader, opens payloads, parses references via [`parse_manifest_refs`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_refs.rs#L62-L110), and produces an exact reference set with zero partial sets on error.

This document designs the **production integration** connecting GC reachability to the contained reader, resolving ownership, delegation, compatibility, resource budgeting, and execution safety.

```
+---------------------------------------------------------------------------------------------------+
|                                  Storage Root File Descriptor                                     |
|                       Owned by FsStorage.reader: Arc<storage_fs::FsMetadataReader>                |
|                       Pinned via O_PATH | O_DIRECTORY | O_CLOEXEC at constructor time             |
+---------------------------------------------------------------------------------------------------+
                                                  |
                         +------------------------+------------------------+
                         |                                                 |
                         v                                                 v
  +----------------------------------------------+  +-----------------------------------------------+
  |        Step 1: Manifest Dir Discovery        |  |    Non-Filesystem Backends (S3, Memory)       |
  |  (repo_discovery beneath pinned root fd)     |  |    (Unchanged Storage Port Traversal)         |
  |  - Starts at ObjectKey("repos")              |  |    - storage.list_repositories()              |
  |  - Bounded BFS over intermediate directories |  |    - storage.list_manifest_digests_page()     |
  |  - Discovers terminal manifests directories  |  |    - storage.get_manifest()                   |
  |    (including root-adjacent & reserved dirs) |  |    - parse_manifest_refs()                    |
  +----------------------------------------------+  +-----------------------------------------------+
                         |
                         v
       Observed Terminal ObjectKeys: &[ObjectKey]
       (e.g., repos/app/manifests, repos/manifests)
                         |
                         v
  +----------------------------------------------+
  |    Step 2: Contained Reference Collection    |
  |  (manifest_refs beneath exact same reader)   |
  |  - reader.enumerate_dir(terminal_dir)        |
  |  - Filter regular files with canonical hex   |
  |  - reader.open_payload(manifest_key)         |
  |  - parse_manifest_refs(&payload)             |
  |  - Deduplicated HashSet<Digest>              |
  |  - Strict error halting (zero partial sets)  |
  +----------------------------------------------+
                         |
                         v
  +-------------------------------------------------------------------------------------------------+
  |                       PolicyContext::build / Protected Set Construction                        |
  |  - Populates PolicyContext.manifest_protected: Option<HashSet<String>>                          |
  |  - Consumed by is_referenced(&digest) during planning, quarantine, and deletion                 |
  +-------------------------------------------------------------------------------------------------+
```

---

## 2. Production Ownership, Trait Delegation & Capability Forwarding

### 2.1 Concrete Storage Trait Architecture: Required Methods Without Defaults

In `registry-rust`, storage capabilities are partitioned into port traits in [`src/storage/ports/mod.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/ports/mod.rs) and underlying backend traits in [`src/storage/mod.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/mod.rs).

To guarantee compile-time enforcement of capability delegation and avoid silent runtime fallbacks, `discover_manifest_references` **must be defined as a required trait method without a default implementation** on both `GcStorage` and `GcStoragePort`:

```rust
// Proposed addition in src/storage/mod.rs: GcStorage
#[async_trait]
pub trait GcStorage: Send + Sync {
    // Existing methods...
    async fn check_bucket_versioning_for_gc(&self) -> Result<(), StorageError>;
    async fn list_cas_blobs_page(...);
    async fn quarantine_blob(...);
    async fn restore_quarantined_blob(...);
    async fn quarantined_blob_version(...);
    async fn delete_blob_conditional(...);

    /// Discovers all manifest roots and referenced content digests using an internal,
    /// contained discovery mechanism beneath the storage root.
    ///
    /// Required trait method without default implementation:
    /// - FsStorage returns `Ok(Some(set))`, including `Ok(Some(HashSet::new()))` when empty.
    /// - Non-filesystem backends (S3, Memory) explicitly return `Ok(None)` to instruct
    ///   the caller to use generic catalog pagination.
    /// - Wrappers and adapters explicitly forward the call to their inner backend.
    /// - StorageError always propagates to the caller.
    async fn discover_manifest_references(
        &self,
    ) -> Result<Option<HashSet<Digest>>, StorageError>;
}
```

```rust
// Proposed addition in src/storage/ports/mod.rs: GcStoragePort
#[async_trait]
pub trait GcStoragePort: Send + Sync {
    // Existing methods...
    fn kind(&self) -> &'static str;
    fn gc_strategy(&self) -> GcStorageStrategy;
    async fn check_bucket_versioning_for_gc(&self) -> Result<(), StorageError>;
    async fn list_cas_blobs_page(...);
    async fn quarantine_blob(...);
    async fn restore_quarantined_blob(...);
    async fn quarantined_blob_version(...);
    async fn delete_blob_conditional(...);

    /// Port view forwarding native contained manifest reference discovery.
    /// Required trait method without default implementation.
    async fn discover_manifest_references(
        &self,
    ) -> Result<Option<HashSet<Digest>>, StorageError>;
}
```

#### Rationale for Required Methods vs. Default Implementations
1. **Compile-Time Enforcement:** In Rust, if a trait method has a default implementation (such as `Ok(None)`), omitting the method in any wrapper, adapter, or mock compiles cleanly. At runtime, the wrapper silently intercepts the call and returns `Ok(None)`, causing the caller to drop through to generic catalog discovery. By requiring explicit implementations, the compiler forces every wrapper and backend to define its forwarding behavior explicitly.
2. **Analysis of Default Implementation Risk (Rejected Alternative):** If defaults returning `Ok(None)` were retained, developers adding new test wrappers or decorating storage backends would not receive compiler errors if they omit delegation. The resulting silent fallback would bypass descriptor containment and silently omit root-adjacent or reserved-ancestor manifests in test harnesses and wrapper-wrapped production deployments. Therefore, default implementations are rejected.

### 2.2 Complete Delegation & Wrapper Inventory

Every implementation, blanket delegation, macro, mock, and test wrapper in the codebase must explicitly implement `discover_manifest_references`:

| Component | Location | Role / Type | Required Implementation & Behavior | Consequence if Delegation Omitted |
| :--- | :--- | :--- | :--- | :--- |
| **`FsStorage`** | `src/storage/fs.rs:3355` | Concrete Production Backend | Executes contained BFS over `self.reader` via promoted `repo_discovery` and `manifest_refs`. Returns `Ok(Some(set))`, including `Ok(Some(HashSet::new()))` if `repos/` is empty. | Fails compilation (required method). |
| **`S3Storage`** | `src/storage/s3.rs:1270` | Concrete Production Backend | Explicitly returns `Ok(None)`. Instructs GC caller to traverse catalog via storage ports. | Fails compilation (required method). |
| **`MockStorage`** | `src/blob_ref_index.rs:1088` | Sled Unit Test Mock | Explicitly returns `Ok(None)`. | Fails compilation (required method). |
| **`Arc<T> for GcStorage`** | `src/storage/mod.rs:556` | Blanket Adapter | Explicitly forwards: `(**self).discover_manifest_references().await`. | Fails compilation (required method). |
| **`impl_gc_storage_port!`** | `src/storage/ports/mod.rs:640` | Port Generation Macro | Emits: `$crate::storage::GcStorage::discover_manifest_references(self).await`. | Fails compilation for all macro targets. |
| **`Arc<T> for GcStoragePort`** | `src/storage/ports/mod.rs:974` | Blanket Port Adapter | Explicitly forwards: `(**self).discover_manifest_references().await`. | Fails compilation (required method). |
| **`StorageWiring`** | `src/storage/ports/mod.rs:1031` | Production Wiring Container | Stores `gc_port: Arc<dyn GcStoragePort>` and `gc_service_port: Arc<dyn GcServiceStoragePort>`. Relies on `Arc<T>` port delegation. | Transparent via `Arc<dyn GcStoragePort>` delegation. |
| **`HookedStorage`** | `tests/support/gc_coordination.rs:236` | Test Adversarial Wrapper | Explicitly forwards: `self.inner.discover_manifest_references().await`. | Fails compilation (required method). |
| **`LifecycleFaultStorage`** | `tests/manifest_lifecycle_tests.rs:3607` | Test Fault Wrapper (wraps `inner: Arc<FsStorage>`) | **Inspect inner backend:** Since `self.inner` is `Arc<FsStorage>`, it **must explicitly forward**: `registry_rust::storage::GcStorage::discover_manifest_references(&self.inner).await`. Macro `impl_gc_storage_port!(LifecycleFaultStorage)` delegates to this method. | Fails compilation (required method). |
| **`FakeGcServiceStorage`** | `tests/ports_wiring_tests.rs:1078` | Test Port Mock | Implements `GcStoragePort` directly. Explicitly returns `Ok(None)`. | Fails compilation (required method). |
| **`SeamGcStorageBridge`** | `src/storage/fs/listing.rs:2898` | Test Listing Bridge | Implements `GcStoragePort` directly. Explicitly returns `Ok(None)`. | Fails compilation (required method). |
| **`ScriptedCursorAdapter`** | `src/storage/fs/listing.rs:3001` | Test Listing Adapter | Implements `GcStoragePort` directly. Explicitly returns `Ok(None)`. | Fails compilation (required method). |

### 2.3 Verification Matrix for Capability Forwarding

Tests must explicitly verify capability preservation through actual production wiring and relevant wrappers:
1. **Some(Empty) Behavior:** When `FsStorage` has an empty `repos/` directory, `discover_manifest_references` returns `Ok(Some(HashSet::new()))`. Verify that calling through `StorageWiring::gc_service_port()` receives `Ok(Some(empty))` — **NOT** `Ok(None)` (which would trigger catalog fallback) and **NOT** `Err`.
2. **None Behavior (Non-Filesystem):** When `S3Storage` is wired through `StorageWiring`, `discover_manifest_references` returns `Ok(None)`, and `build_manifest_protected_set` correctly takes the storage-port pagination branch.
3. **Err Propagation:** When `FsStorage` encounters budget exhaustion (e.g. `max_manifests_read = 1` with 2 manifests), `discover_manifest_references` returns `Err(StorageError::backend(...))`. Verify that calling through `StorageWiring`, `HookedStorage`, and `LifecycleFaultStorage` propagates `Err` to the caller without being swallowed into `None` or converted to an empty set.

---

## 3. Mechanical Call Chain Analysis & Verification

### 3.1 Verification of Call Sites and Existing vs. Proposed Functions

Every named production function, error type, and test harness was verified directly against repository source:

- **`PolicyContext::build`**: [`src/blob_gc/policy.rs:115-136`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/policy.rs#L115-L136)
  - Constructs `PolicyContext` under a given `BlobGcPolicy`.
  - Line 122: `idx.check_health()?;`
  - Line 128: `BlobGcPolicy::ManifestRooted => Some(build_manifest_protected_set(cfg, storage).await?),`
- **`build_manifest_protected_set`**: [`src/blob_gc/policy.rs:166-225`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/policy.rs#L166-L225)
  - Existing signature: `pub async fn build_manifest_protected_set(cfg: &crate::config::Config, storage: &(impl storage::GcServiceStoragePort + ?Sized)) -> Result<HashSet<String>, GcPolicyError>`
  - Lines 170–176: Direct uncontained bypass branch (`storage.kind() == "fs"` and `tokio::fs::metadata(&cfg.fs_root.join("repos")).await.is_ok()`).
  - Lines 178–224: Storage-port fallback loop.
  - **Proposed Change**: Replace lines 170–176 with a call to `storage.discover_manifest_references().await`. If `Some(set)` is returned, convert to `HashSet<String>` and return. If `None` is returned, take lines 178–224.
- **`build_manifest_protected_set_fs`**: [`src/blob_gc/policy.rs:227-299`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/policy.rs#L227-L299)
  - Existing uncontained raw filesystem traversal. Retired upon cutover.
- **`blob_gc_plan`**: [`src/blob_gc/mod.rs:129-194`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/mod.rs#L129-L194)
  - Line 137: `let mut policy_ctx = PolicyContext::build(cfg, storage, idx, policy).await?;`
  - Called **once** up-front before candidate batch iteration. Read-only.
- **`blob_gc_quarantine_with_authority`**: [`src/blob_gc/mod.rs:226-320`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/mod.rs#L226-L320)
  - Line 276: `let _reval_guard = consistency.acquire_gc_revalidation().await;`
  - Line 278: `let mut policy_ctx = PolicyContext::build(cfg, storage, idx, policy).await?;`
  - Called **per candidate** under `_reval_guard`.
- **`blob_gc_delete_with_authority`**: [`src/blob_gc/mod.rs:352-393`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/mod.rs#L352-L393)
  - Public deletion entry point. Dispatches to `blob_gc_delete_fs_with_authority` for filesystem strategy.
- **`blob_gc_delete_fs_with_authority`**: [`src/blob_gc/mod.rs:395-586`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/mod.rs#L395-L586)
  - Lines 486–491: Reads quarantine timestamp; if `None`, **writes quarantine timestamp file** (`write_quarantine_time(cfg, &digest, now)`). This disk mutation occurs **before** age evaluation and **before** discovery!
  - Line 494: `check_candidate_age(q_at, now, quarantine_delay)`
  - Line 508: `let reval_guard = consistency.acquire_gc_revalidation().await;`
  - Line 510: `let mut policy_ctx = PolicyContext::build(cfg, storage, idx, policy).await?;`
  - Line 552: Calls `execute_guarded_gc_deletion(storage, idx, &candidate, now, &mut policy_ctx, &permit, &reval_guard).await`, which conditionally calls `storage.delete_blob_conditional(...)`.

### 3.2 Explicit Error Modeling: `GcPolicyError::ManifestDiscovery`

In [`src/blob_gc/policy.rs:60-108`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/policy.rs#L60-L108), `GcPolicyError` does not contain a general storage error conversion function. A new dedicated variant must be added:

```rust
// Proposed addition in src/blob_gc/policy.rs
#[derive(Debug, thiserror::Error)]
pub enum GcPolicyError {
    // Existing variants...
    #[error("reference index health check failed: {0}")]
    IndexHealth(#[from] crate::blob_ref_index::RefIndexError),

    #[error("repository enumeration failed: {0}")]
    ListRepositories(#[source] crate::storage::StorageError),

    // NEW variant for contained GC manifest discovery failures:
    #[error("contained manifest discovery failed: {0}")]
    ManifestDiscovery(#[source] crate::storage::StorageError),

    #[error("manifest listing failed for repository '{repository}': {source}")]
    ListManifests { repository: String, #[source] source: crate::storage::StorageError },
    // Remainder unchanged...
}
```

In `build_manifest_protected_set`, the contained discovery call maps its error directly:
```rust
let discovery_res = storage
    .discover_manifest_references()
    .await
    .map_err(GcPolicyError::ManifestDiscovery)?;
```

### 3.3 Representation of Storage Errors: `StorageError::backend(...)`

All error constructions in the proposed code conform to the actual constructors in [`src/storage/mod.rs:102-165`](file:///home/dietmar/devel/rust/registry-rust/src/storage/mod.rs#L102-L165):
- `StorageError::backend(msg)` constructs `StorageError::Internal { kind: StorageErrorKind::Backend, message: msg.to_string() }`.
- `StorageError::io(msg)` constructs `StorageError::Internal { kind: StorageErrorKind::Io, message: msg.to_string() }`.
- `StorageError::corrupt_data(msg)` constructs `StorageError::Internal { kind: StorageErrorKind::CorruptData, message: msg.to_string() }`.
- `StorageError::configuration(msg)` constructs `StorageError::Internal { kind: StorageErrorKind::Configuration, message: msg.to_string() }`.

There is no `StorageError::Backend(...)` enum variant; all backend errors use `StorageError::backend(...)` or the explicit `StorageError::Internal` representation.

### 3.4 Proposed File Modification Inventory

The complete inventory of files to be modified or created upon authorization:

| Proposed Action | File Path | Scope of Changes |
| :--- | :--- | :--- |
| **Promote Module** | `src/storage/fs/repo_discovery.rs` | Remove `#[cfg(test)]` from module declaration. Rename `DiscoveryTestLimits` to `DiscoveryLimits`. Retain unit tests in `mod tests`. |
| **Promote Module** | `src/storage/fs/manifest_refs.rs` | Rename from `src/storage/fs/manifest_refs_seam.rs`. Remove `#[cfg(test)]`. Rename `ManifestReferenceTestLimits` to `ManifestReferenceLimits`. |
| **Modify Code** | `src/storage/fs.rs` | Add fields `gc_discovery_limits` and `gc_ref_limits` to `FsStorage`. Add crate-visible constructor `try_new_with_gc_limits`. Implement `GcStorage::discover_manifest_references` on `FsStorage`. |
| **Modify Code** | `src/storage/mod.rs` | Add required method `discover_manifest_references` to `GcStorage`. Implement delegation for `Arc<T>`. Update `storage_wiring_try_from_config` to wire GC limits. |
| **Modify Code** | `src/storage/ports/mod.rs` | Add required method `discover_manifest_references` to `GcStoragePort`. Update `impl_gc_storage_port!` macro. Implement delegation for `Arc<T>`. |
| **Modify Code** | `src/blob_gc/policy.rs` | Add `GcPolicyError::ManifestDiscovery`. Update `build_manifest_protected_set` to query port capability. Retire `build_manifest_protected_set_fs`. |
| **Modify Code** | `src/config.rs` | Add `storage.fs.gc.discovery` configuration settings, validation rules, and environment variable parsing helpers. |
| **Modify Tests** | `tests/support/gc_coordination.rs` | Delegate `discover_manifest_references` in `HookedStorage`. |
| **Modify Tests** | `tests/manifest_lifecycle_tests.rs` | Delegate `discover_manifest_references` to `&self.inner` in `LifecycleFaultStorage`. |
| **Modify Tests** | `src/blob_ref_index.rs` | Return `Ok(None)` in `MockStorage`. |
| **Modify Tests** | `tests/ports_wiring_tests.rs` | Return `Ok(None)` in `FakeGcServiceStorage`. |
| **Modify Tests** | `src/storage/fs/listing.rs` | Return `Ok(None)` in `SeamGcStorageBridge` and `ScriptedCursorAdapter`. |
| **New Integration Tests** | `tests/gc_contained_discovery_integration_tests.rs` | Real caller integration tests verifying planning, quarantine, deletion, and capability forwarding. |

---

## 4. Explicit Production Compatibility Decisions & Approval Table

The committed test seams resolved several semantic differences compared to the direct bypass walker. **The committed test seams do NOT authorize those changes in production.** Each intentional change requires production approval.

| Decision ID | Dimension | Current Production Behavior (`build_manifest_protected_set_fs`) | Proposed Contained Path (`repo_discovery` + `manifest_refs`) | Justification & Alternatives | Supporting Source / Verified Existing Test | Production Approval Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **D-01** | **Root-Adjacent Manifests** | Scans `repos/manifests/<hex>` directly. | Enqueues `ObjectKey("repos/manifests")` as a terminal directory; enumerates candidates. | **Preserve parity**. Omitting root manifests would orphan legacy single-tenant roots. | `policy.rs:250-293`; `repo_discovery.rs:22-24`; test `test_repo_discovery_real_fs_ordinary_nested_and_reserved_ancestor_layouts`. | **EXPLICITLY UNAPPROVED (Requires Approval)** |
| **D-02** | **Reserved-Ancestor Manifests** | Traverses any directory not named `manifests`, finding `repos/tags/sub/manifests/<hex>`. | Traverses all intermediate subdirectories regardless of name; treats `manifests` as terminal leaf. | **Preserve parity**. Rejection would cause reachability omissions for nested namespaces. | `policy.rs:294-297`; `repo_discovery.rs:20-22`; test `test_repo_discovery_real_fs_ordinary_nested_and_reserved_ancestor_layouts`. | **EXPLICITLY UNAPPROVED (Requires Approval)** |
| **D-03** | **Missing Initial `repos/`** | If `cfg.fs_root/repos` is missing, bypass condition fails; falls back to `list_repositories()`, returning `Ok(HashSet::new())`. | Initial enumeration of `repos` returns `FsDirError::NotFound` at `depth == 0`; returns `Ok(HashSet::new())`. | **Preserve parity**. Clean startup before first push must not error. | `policy.rs:170-176`; `repo_discovery.rs:339-344`; test `test_fake_missing_root_returns_empty_success_with_one_call`. | **EXPLICITLY UNAPPROVED (Requires Approval)** |
| **D-04** | **Disappearance After Observation** | If a directory returns `NotFound` during `tokio::fs::read_dir`, catches `ErrorKind::NotFound` and `continue`s (silent omission). | If an observed child directory returns `NotFound` when opened, returns `StorageError::io` (fails closed). | **Intentional change**: Silent continuation yields a partial protected set, risking premature blob deletion. | `policy.rs:237, 253`; `repo_discovery.rs:345-349`; test `test_fake_observed_child_disappearance_returns_io`. | **EXPLICITLY UNAPPROVED (Requires Approval)** |
| **D-05** | **Symlink Directory Entries** | `file_type().await` follows symlinks if supported; traverses target directory. | Skips `DirEntryType::Symlink` dirents. If opened as a path component, `openat2` returns `ResolutionRejected`. | **Intentional change**: Prevents symlink breakout attacks from storage root. | `policy.rs:248`; `repo_discovery.rs:417-421`; test `test_repo_discovery_real_fs_symlink_entries_skipped_vs_path_symlink_failure`. | **EXPLICITLY UNAPPROVED (Requires Approval)** |
| **D-06** | **Non-UTF-8 Directory Names** | Maps non-UTF-8 names to empty string `""` via `.unwrap_or("")`; since `"" != "manifests"`, descends into them. | Enforces valid UTF-8 and `ObjectKey` validation; fails closed with `StorageError::corrupt_data`. | **Intentional change**: Prevents silently omitting or misattributing corrupted filesystem hierarchies. | `policy.rs:249`; `repo_discovery.rs:423-431`; test `test_repo_discovery_real_fs_non_utf8_directory_rejection`. | **EXPLICITLY UNAPPROVED (Requires Approval)** |
| **D-07** | **SHA-512 Support** | Hardcodes `hex.len() != 64`; completely ignores SHA-512 manifests. | Supports canonical 64-char (SHA-256) AND 128-char (SHA-512) lowercase hex filenames. | **Intentional change**: Enables multi-algorithm manifest protection while enforcing canonical formats. | `policy.rs:271`; `manifest_refs_seam.rs:21-23`; test `test_manifest_refs_filename_sha256_and_sha512_accepted_and_roots_recorded`. | **EXPLICITLY UNAPPROVED (Requires Approval)** |
| **D-08** | **Uppercase Hex Filenames** | Uses `hex.chars().all(|c| c.is_ascii_hexdigit())`, accepting uppercase `[A-F]`, producing non-canonical digests. | Requires lowercase hex; skips uppercase filenames and charges budget. | **Intentional change**: OCI and Docker digest specifications require lowercase hexadecimal characters. | `policy.rs:271-274`; `manifest_refs_seam.rs:388-393`; test `test_manifest_refs_uppercase_hex_and_invalid_names_skipped`. | **EXPLICITLY UNAPPROVED (Requires Approval)** |
| **D-09** | **Manifest Payload Disappearance** | Fails with `GcPolicyError::FsReadManifest` if `tokio::fs::read` fails. | Fails closed with `StorageError::io`; aborts entire discovery run. | **Preserve fail-closed semantics**. Prevents executing GC when manifest read fails. | `policy.rs:279-281`; `manifest_refs_seam.rs:220-223`; test `test_manifest_refs_missing_observed_terminal_or_payload_fails_closed`. | **EXPLICITLY UNAPPROVED (Requires Approval)** |
| **D-10** | **Manifest Parse Errors** | Fails with `GcPolicyError::ParseManifest` if `parse_manifest_refs` fails. | Fails closed with `StorageError::corrupt_data`; aborts entire discovery run. | **Preserve fail-closed semantics**. Corrupted manifest must halt GC rather than risking layer deletion. | `policy.rs:283-289`; `manifest_refs_seam.rs:479-483`; test `test_manifest_refs_stream_io_and_parse_failures`. | **EXPLICITLY UNAPPROVED (Requires Approval)** |
| **D-11** | **Root Configuration Divergence** | Direct walker evaluates `cfg.fs_root.join("repos")`, which can diverge from `storage.root`. | Uses the file descriptor pinned inside `FsStorage.reader` at constructor time. | **Intentional change**: Immunizes GC against runtime root replacement, symlink swaps, and config divergence. | `policy.rs:171, 230`; `storage/fs.rs:219-224`; test `test_manifest_refs_linux_real_fs_pinned_root_across_replacement`. | **EXPLICITLY UNAPPROVED (Requires Approval)** |
| **D-12** | **Resource Budgeting** | Traversal depth, total directories, manifest count, and heap memory are completely unbounded. | Enforces strict, caller-supplied limits with checked arithmetic; fails closed on exhaustion. | **Intentional change**: Protects production nodes from stack overflow, memory exhaustion, and runaway I/O. | `policy.rs:234-298`; `repo_discovery.rs:93-107`; `manifest_refs_seam.rs:53-68`; test `test_manifest_refs_exact_and_one_over_budgets`. | **EXPLICITLY UNAPPROVED (Requires Approval)** |

---

## 5. Resource Policy, Memory Accounting & Configuration Architecture

### 5.1 Resource Limit Taxonomy

The discovery and reference collection architecture requires limits across seven distinct dimensions:

1. **Intermediate Directory Discovery Bounds ([`repo_discovery`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/repo_discovery.rs)):**
   - `max_depth`: Maximum directory traversal depth beneath `repos/` (depth 0).
   - `max_dir_enumerations`: Maximum directory enumeration attempts across intermediate folders.
   - `max_total_discovery_entries`: Cumulative dirents inspected across intermediate directory enumerations.
   - `max_manifest_dirs`: Maximum terminal manifest directory `ObjectKey`s retained.
   - `max_discovery_retained_path_bytes`: Logical path bytes retained in pending queue and terminal list.
2. **Intermediate Directory Single-Batch Bounds:**
   - `intermediate_dir_limits: DirEnumerationLimits`: Per-directory limits (`max_entries`, `max_total_name_bytes`) passed to `enumerate_dir` on intermediate folders.
3. **Terminal Directory Enumeration Bounds ([`manifest_refs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/manifest_refs_seam.rs)):**
   - `max_terminal_dir_enumerations`: Maximum terminal manifest directories enumerated.
   - `terminal_per_dir_limits: DirEnumerationLimits`: Per-directory limits passed to `enumerate_dir` on each terminal `manifests/` directory.
   - `max_total_manifest_entries`: Cumulative dirents inspected across all terminal directories combined.
4. **Manifest Reading & Reference Extraction Bounds:**
   - `max_manifests_read`: Maximum manifest payload files opened, buffered, and parsed.
   - `max_total_references`: Maximum unique protected content digests retained in `HashSet<Digest>`.
5. **Memory Accounting Limits:**
   - `max_retained_logical_bytes`: Maximum cumulative logical string bytes accounted across terminal keys, manifest keys, and protected digests.
6. **Payload Buffering Ceilings:**
   - `max_manifest_payload_bytes: Option<u64>`: Optional ceiling on individual manifest payload size in bytes.
7. **Concurrency Controls:**
   - `run_lock`: In-process mutual exclusion (`Mutex<()>`).
   - `fs_gc_lock`: Cross-process advisory lock on `quarantine/gc.lock`.

### 5.2 Decoupling Intermediate vs. Terminal Directory Budgets

Intermediate directory discovery **must maintain independent configuration** from terminal manifest directory listing:
- Intermediate directories in container registries typically contain tens or hundreds of repository namespace segments, whereas terminal `manifests/` directories can contain tens of thousands of digest files.
- Coupling intermediate directory limits to public manifest listing (`fs_manifest_listing_max_entries` = 10,000, `fs_manifest_listing_max_name_bytes` = 1,500,000) would prevent independent tuning of namespace traversal batches versus terminal manifest batches.
- Intermediate directory discovery defaults to modest per-directory limits (`intermediate_dir_max_entries = 1000`, `intermediate_dir_max_name_bytes = 100_000`), while terminal directories default to `terminal_dir_max_entries = 10_000`, `terminal_dir_max_name_bytes = 1_500_000`.

### 5.3 Mathematical Accounting Assumptions vs. Total Heap Realities

All calculations presented below are **engineering assumptions and accounting models**, NOT total heap guarantees:

1. **Logical Byte Accounting Model:**
   - Evaluated using string byte lengths (`key.as_str().len()` and `digest.as_str().len()`).
   - For SHA-256 digests: 71 bytes (`sha256:` + 64 lowercase hex characters).
   - For SHA-512 digests: 135 bytes (`sha512:` + 128 lowercase hex characters).
   - Bounded by `max_retained_logical_bytes`.
2. **Total Heap Consumption Factors (Beyond Logical Bytes):**
   - **Directory Batches:** Each `reader.enumerate_dir` call allocates a `Vec<DirEntry>`. Each `DirEntry` contains an `OsString` filename and entry metadata allocated on the heap during the blocking enumeration task.
   - **JSON DOM Trees:** `parse_manifest_refs` calls `serde_json::from_slice`, which constructs an in-memory `serde_json::Value` DOM representing the complete manifest JSON structure (objects, arrays, strings) before extracting descriptor digests.
   - **Caller-Owned Terminal Keys:** `discover_manifest_dirs` allocates and returns `Vec<ObjectKey>`, which is retained in memory while `collect_manifest_references` executes.
   - **Type Conversion Allocation:** `manifest_refs` accumulates `HashSet<Digest>`. However, `PolicyContext.manifest_protected` expects `HashSet<String>`. Converting `HashSet<Digest>` into `HashSet<String>` consumes an additional allocation cycle, cloning each digest string into a new `HashSet`.
   - **Hash Table Overhead:** Standard `HashSet` (HashBrown) maintains capacity buckets and control bytes, consuming memory beyond the logical string payloads.
3. **Scale Assumptions (Explicitly Unvalidated):**
   - Proposed defaults (such as 10,000 terminal directories, 50,000 manifests read, and 250,000 references) are **provisional unvalidated proposals** to prevent runaway execution. They are **not** claims that single-node registries cannot exceed these values. Large production installations may have millions of manifests and must tune these bounds accordingly.

### 5.4 Complete Production Configuration Contract

The following table provides the exhaustive production configuration contract across all 15 limits:

| Field Name & Rust Type | TOML Path (`storage.fs.gc.discovery.*`) | Hierarchical Environment Variable Alias | Flat Environment Variable Alias | Precedence Rule | Proposed Default | Exact Validation Rule | Target Struct Field |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`max_depth`**<br>`usize` | `max_depth` | `REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_DEPTH` | `REGISTRY_BLOB_GC_DISCOVERY_MAX_DEPTH` | Hierarchical > Flat > TOML > Default | `32` | `val >= 1`<br>(Err: "must be at least 1") | `DiscoveryLimits.max_depth` |
| **`max_dir_enumerations`**<br>`usize` | `max_dir_enumerations` | `REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_DIR_ENUMERATIONS` | `REGISTRY_BLOB_GC_DISCOVERY_MAX_DIR_ENUMERATIONS` | Hierarchical > Flat > TOML > Default | `10000` | `val >= 1`<br>(Err: "must be at least 1") | `DiscoveryLimits.max_dir_enumerations` |
| **`max_total_discovery_entries`**<br>`usize` | `max_total_discovery_entries` | `REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_TOTAL_DISCOVERY_ENTRIES` | `REGISTRY_BLOB_GC_DISCOVERY_MAX_TOTAL_DISCOVERY_ENTRIES` | Hierarchical > Flat > TOML > Default | `250000` | `val >= 1`<br>(Err: "must be at least 1") | `DiscoveryLimits.max_total_entries` |
| **`max_manifest_dirs`**<br>`usize` | `max_manifest_dirs` | `REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_MANIFEST_DIRS` | `REGISTRY_BLOB_GC_DISCOVERY_MAX_MANIFEST_DIRS` | Hierarchical > Flat > TOML > Default | `10000` | `val >= 1`<br>(Err: "must be at least 1") | `DiscoveryLimits.max_manifest_dirs` |
| **`max_discovery_retained_path_bytes`**<br>`usize` | `max_discovery_retained_path_bytes` | `REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_DISCOVERY_RETAINED_PATH_BYTES` | `REGISTRY_BLOB_GC_DISCOVERY_MAX_DISCOVERY_RETAINED_PATH_BYTES` | Hierarchical > Flat > TOML > Default | `10485760`<br>(10 MiB) | `val >= 65536`<br>(Err: "must be at least 65536 bytes") | `DiscoveryLimits.max_retained_path_bytes` |
| **`intermediate_dir_max_entries`**<br>`usize` | `intermediate_dir_max_entries` | `REGISTRY__STORAGE__FS__GC__DISCOVERY__INTERMEDIATE_DIR_MAX_ENTRIES` | `REGISTRY_BLOB_GC_DISCOVERY_INTERMEDIATE_DIR_MAX_ENTRIES` | Hierarchical > Flat > TOML > Default | `1000` | `val >= 1`<br>(Err: "must be at least 1") | `DiscoveryLimits.per_dir_limits.max_entries` |
| **`intermediate_dir_max_name_bytes`**<br>`usize` | `intermediate_dir_max_name_bytes` | `REGISTRY__STORAGE__FS__GC__DISCOVERY__INTERMEDIATE_DIR_MAX_NAME_BYTES` | `REGISTRY_BLOB_GC_DISCOVERY_INTERMEDIATE_DIR_MAX_NAME_BYTES` | Hierarchical > Flat > TOML > Default | `100000` | `val >= 128`<br>(Err: "must be at least 128 bytes") | `DiscoveryLimits.per_dir_limits.max_total_name_bytes` |
| **`max_terminal_dir_enumerations`**<br>`usize` | `max_terminal_dir_enumerations` | `REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_TERMINAL_DIR_ENUMERATIONS` | `REGISTRY_BLOB_GC_DISCOVERY_MAX_TERMINAL_DIR_ENUMERATIONS` | Hierarchical > Flat > TOML > Default | `10000` | `val >= 1`<br>(Err: "must be at least 1") | `ManifestReferenceLimits.max_terminal_dir_enumerations` |
| **`terminal_dir_max_entries`**<br>`usize` | `terminal_dir_max_entries` | `REGISTRY__STORAGE__FS__GC__DISCOVERY__TERMINAL_DIR_MAX_ENTRIES` | `REGISTRY_BLOB_GC_DISCOVERY_TERMINAL_DIR_MAX_ENTRIES` | Hierarchical > Flat > TOML > Default | `10000` | `val >= 1`<br>(Err: "must be at least 1") | `ManifestReferenceLimits.per_dir_limits.max_entries` |
| **`terminal_dir_max_name_bytes`**<br>`usize` | `terminal_dir_max_name_bytes` | `REGISTRY__STORAGE__FS__GC__DISCOVERY__TERMINAL_DIR_MAX_NAME_BYTES` | `REGISTRY_BLOB_GC_DISCOVERY_TERMINAL_DIR_MAX_NAME_BYTES` | Hierarchical > Flat > TOML > Default | `1500000` | `val >= 128`<br>(Err: "must be at least 128 bytes") | `ManifestReferenceLimits.per_dir_limits.max_total_name_bytes` |
| **`max_total_manifest_entries`**<br>`usize` | `max_total_manifest_entries` | `REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_TOTAL_MANIFEST_ENTRIES` | `REGISTRY_BLOB_GC_DISCOVERY_MAX_TOTAL_MANIFEST_ENTRIES` | Hierarchical > Flat > TOML > Default | `250000` | `val >= 1`<br>(Err: "must be at least 1") | `ManifestReferenceLimits.max_total_manifest_entries` |
| **`max_manifests_read`**<br>`usize` | `max_manifests_read` | `REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_MANIFESTS_READ` | `REGISTRY_BLOB_GC_DISCOVERY_MAX_MANIFESTS_READ` | Hierarchical > Flat > TOML > Default | `50000` | `val >= 1`<br>(Err: "must be at least 1") | `ManifestReferenceLimits.max_manifests_read` |
| **`max_total_references`**<br>`usize` | `max_total_references` | `REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_TOTAL_REFERENCES` | `REGISTRY_BLOB_GC_DISCOVERY_MAX_TOTAL_REFERENCES` | Hierarchical > Flat > TOML > Default | `250000` | `val >= 1`<br>(Err: "must be at least 1") | `ManifestReferenceLimits.max_total_references` |
| **`max_retained_logical_bytes`**<br>`usize` | `max_retained_logical_bytes` | `REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_RETAINED_LOGICAL_BYTES` | `REGISTRY_BLOB_GC_DISCOVERY_MAX_RETAINED_LOGICAL_BYTES` | Hierarchical > Flat > TOML > Default | `33554432`<br>(32 MiB) | `val >= 131072`<br>(Err: "must be at least 131072 bytes") | `ManifestReferenceLimits.max_retained_logical_bytes` |
| **`max_manifest_payload_bytes`**<br>`Option<u64>` | `max_manifest_payload_bytes` | `REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_MANIFEST_PAYLOAD_BYTES` | `REGISTRY_BLOB_GC_DISCOVERY_MAX_MANIFEST_PAYLOAD_BYTES` | Hierarchical > Flat > TOML > Default | `None`<br>(Unresolved Decision) | If `Some(v)`: `v >= 1024` AND `v < u64::MAX`<br>(Err: "must be at least 1024 and less than u64::MAX") | `ManifestReferenceLimits.max_manifest_payload_bytes` |

### 5.5 Configuration Loading, Serde Deserialization & Error Semantics

The configuration contract directly integrates into [`src/config.rs`](file:///home/dietmar/devel/rust/registry-rust/src/config.rs) using its actual existing mechanisms:

1. **Serde File Configuration:**
   - In `src/config.rs:1004`, `FileStorageFs` contains a sub-struct:
     ```rust
     #[derive(Clone, Debug, Default, Deserialize)]
     struct FileStorageFs {
         #[serde(default)]
         root: Option<String>,
         #[serde(default)]
         manifest_listing_max_entries: Option<usize>,
         #[serde(default)]
         manifest_listing_max_name_bytes: Option<usize>,
         #[serde(default)]
         gc: FileStorageFsGc,
     }

     #[derive(Clone, Debug, Default, Deserialize)]
     struct FileStorageFsGc {
         #[serde(default)]
         discovery: FileStorageFsGcDiscovery,
     }

     #[derive(Clone, Debug, Default, Deserialize)]
     struct FileStorageFsGcDiscovery {
         #[serde(default)]
         max_depth: Option<usize>,
         #[serde(default)]
         max_dir_enumerations: Option<usize>,
         #[serde(default)]
         max_total_discovery_entries: Option<usize>,
         #[serde(default)]
         max_manifest_dirs: Option<usize>,
         #[serde(default)]
         max_discovery_retained_path_bytes: Option<usize>,
         #[serde(default)]
         intermediate_dir_max_entries: Option<usize>,
         #[serde(default)]
         intermediate_dir_max_name_bytes: Option<usize>,
         #[serde(default)]
         max_terminal_dir_enumerations: Option<usize>,
         #[serde(default)]
         terminal_dir_max_entries: Option<usize>,
         #[serde(default)]
         terminal_dir_max_name_bytes: Option<usize>,
         #[serde(default)]
         max_manifests_read: Option<usize>,
         #[serde(default)]
         max_total_manifest_entries: Option<usize>,
         #[serde(default)]
         max_total_references: Option<usize>,
         #[serde(default)]
         max_retained_logical_bytes: Option<usize>,
         #[serde(default)]
         max_manifest_payload_bytes: Option<u64>,
     }
     ```
   - In [`src/config.rs:2482-2512`](file:///home/dietmar/devel/rust/registry-rust/src/config.rs#L2482-L2512), `load_config_files` parses TOML documents into a temporary `toml::Value` purely to merge overlays via `merge_toml_value`, and then deserializes the merged table into `FileConfig` via `merged.try_into()`. The application does **not** perform dynamic key lookups on `toml::Value`.
2. **Environment Variable Parsing Helpers:**
   - In [`src/config.rs:2997-3008`](file:///home/dietmar/devel/rust/registry-rust/src/config.rs#L2997-L3008), `env_usize_opt(&[hierarchical, flat])?` and `env_u64_opt(&[hierarchical, flat])?` resolve environment variables:
     - They check the slice of keys in order (hierarchical alias first, flat alias second).
     - If set, the string is trimmed (`v.trim()`) and parsed via `.parse::<usize>()` or `.parse::<u64>()`.
3. **Numeric Parsing Failures & Integer Overflow:**
   - If an environment variable cannot be parsed as an unsigned integer (e.g. non-numeric characters, negative numbers, empty strings, or values exceeding `usize::MAX` / `u64::MAX`), `env_usize_opt` returns `Err(ConfigError::InvalidEnvValue { key, expected: "unsigned integer" })`.
   - If the TOML configuration file contains an invalid type or an integer that overflows the target integer type, `load_config_files` immediately fails with `ConfigError::Deserialize` or `ConfigError::Toml`.
4. **Validation Failures:**
   - If any limit falls below its minimum validation threshold (e.g. `max_depth < 1` or `max_discovery_retained_path_bytes < 65536`), `Config::from_env_and_files` returns `Err(ConfigError::InvalidValue { field: "storage.fs.gc.discovery.<field>", message })`.
5. **Optional Payload-Ceiling Semantics & Checked Sentinel Overflow:**
   - `max_manifest_payload_bytes` is an `Option<u64>`.
   - If unset (`None`), streams are read to EOF without any payload length ceiling.
   - If set to `Some(limit)`:
     - Configuration validation enforces `limit >= 1024` AND `limit < u64::MAX`.
     - Runtime reading in `read_payload_stream_bounded` performs checked sentinel addition:
       ```rust
       let sentinel_limit = limit
           .checked_add(1)
           .ok_or_else(|| StorageError::backend("payload ceiling limit arithmetic overflow"))?;
       ```
     - If `limit == u64::MAX`, configuration validation rejects the value with `ConfigError::InvalidValue`, preventing arithmetic overflow when computing `sentinel_limit`.
     - Applying `stream.take(sentinel_limit).read_to_end(&mut buf)` bounds the bytes consumed through the reader adapter, and reading up to `limit + 1` bytes allows detecting an oversized payload stream using a sentinel byte.
     - **I/O and Memory Reality:** Calling `take(limit + 1).read_to_end(...)` can perform multiple reads/syscalls depending on chunking and buffer sizes; it does **not** guarantee a single syscall. Furthermore, while it limits the bytes consumed through the adapter, it does **not** guarantee a bound on total process heap memory across concurrent operations or prevent heap allocations during JSON AST parsing.
   - **Status:** Setting a payload ceiling remains an **unresolved production decision**. Enforcing a ceiling halts GC on corrupted giant files, but risks rejecting valid massive index manifests unless explicitly configured.

### 5.6 Constructor Signatures & Wiring Enforcement

To ensure direct callers (such as unit tests or standalone tools) cannot bypass limit validation, limits are validated inside the `FsStorage` constructor itself:

```rust
// Proposed FsStorage struct in src/storage/fs.rs
pub struct FsStorage {
    root: PathBuf,
    max_upload_bytes: u64,
    upload_hashes: Vec<Mutex<std::collections::HashMap<String, SerializableSha256>>>,
    referrer_locks: Vec<Mutex<()>>,
    repo_locks: std::sync::Mutex<std::collections::HashMap<String, std::fs::File>>,
    reader: std::sync::Arc<storage_fs::FsMetadataReader>,
    read_adapter: std::sync::Arc<read_adapter::FsBlobCasReadAdapter<storage_fs::FsMetadataReader>>,
    manifest_listing_limits: storage_fs::DirEnumerationLimits,
    // Proposed additions:
    gc_discovery_limits: repo_discovery::DiscoveryLimits,
    gc_ref_limits: manifest_refs::ManifestReferenceLimits,
}

impl FsStorage {
    /// Crate-visible constructor enforcing complete limit validation.
    ///
    /// Accepts internal limit types `DiscoveryLimits` and `ManifestReferenceLimits`.
    /// External integration tests configure limits via the public `Config` and
    /// `StorageWiring` APIs or use `try_new_with_limits` with compiled safe defaults.
    pub(crate) fn try_new_with_gc_limits(
        root: PathBuf,
        max_upload_bytes: u64,
        manifest_listing_limits: storage_fs::DirEnumerationLimits,
        gc_discovery_limits: repo_discovery::DiscoveryLimits,
        gc_ref_limits: manifest_refs::ManifestReferenceLimits,
    ) -> Result<Self, StorageError> {
        // 1. Validate manifest listing limits
        if manifest_listing_limits.max_entries() < manifest_listing::MIN_MANIFEST_LISTING_ENTRIES {
            return Err(StorageError::configuration("manifest_listing_max_entries must be at least 1"));
        }
        if manifest_listing_limits.max_total_name_bytes() < manifest_listing::MIN_MANIFEST_LISTING_NAME_BYTES {
            return Err(StorageError::configuration("manifest_listing_max_name_bytes must be at least 128"));
        }

        // 2. Validate GC discovery limits
        if gc_discovery_limits.max_depth < 1 {
            return Err(StorageError::configuration("gc discovery max_depth must be at least 1"));
        }
        if gc_discovery_limits.max_dir_enumerations < 1 {
            return Err(StorageError::configuration("gc discovery max_dir_enumerations must be at least 1"));
        }
        if gc_discovery_limits.max_total_entries < 1 {
            return Err(StorageError::configuration("gc discovery max_total_entries must be at least 1"));
        }
        if gc_discovery_limits.max_manifest_dirs < 1 {
            return Err(StorageError::configuration("gc discovery max_manifest_dirs must be at least 1"));
        }
        if gc_discovery_limits.max_retained_path_bytes < 65_536 {
            return Err(StorageError::configuration("gc discovery max_retained_path_bytes must be at least 65536"));
        }
        if gc_discovery_limits.per_dir_limits.max_entries() < 1 {
            return Err(StorageError::configuration("gc discovery intermediate_dir_max_entries must be at least 1"));
        }
        if gc_discovery_limits.per_dir_limits.max_total_name_bytes() < 128 {
            return Err(StorageError::configuration("gc discovery intermediate_dir_max_name_bytes must be at least 128"));
        }

        // 3. Validate GC reference collection limits
        if gc_ref_limits.max_terminal_dir_enumerations < 1 {
            return Err(StorageError::configuration("gc ref max_terminal_dir_enumerations must be at least 1"));
        }
        if gc_ref_limits.per_dir_limits.max_entries() < 1 {
            return Err(StorageError::configuration("gc ref terminal_dir_max_entries must be at least 1"));
        }
        if gc_ref_limits.per_dir_limits.max_total_name_bytes() < 128 {
            return Err(StorageError::configuration("gc ref terminal_dir_max_name_bytes must be at least 128"));
        }
        if gc_ref_limits.max_total_manifest_entries < 1 {
            return Err(StorageError::configuration("gc ref max_total_manifest_entries must be at least 1"));
        }
        if gc_ref_limits.max_manifests_read < 1 {
            return Err(StorageError::configuration("gc ref max_manifests_read must be at least 1"));
        }
        if gc_ref_limits.max_total_references < 1 {
            return Err(StorageError::configuration("gc ref max_total_references must be at least 1"));
        }
        if gc_ref_limits.max_retained_logical_bytes < 131_072 {
            return Err(StorageError::configuration("gc ref max_retained_logical_bytes must be at least 131072"));
        }
        if let Some(ceiling) = gc_ref_limits.max_manifest_payload_bytes {
            if ceiling < 1024 || ceiling == u64::MAX {
                return Err(StorageError::configuration("gc ref max_manifest_payload_bytes must be >= 1024 and < u64::MAX"));
            }
        }

        ensure_dir(&root)?;
        let reader = storage_fs::FsMetadataReader::open(&root)
            .map_err(read_adapter::map_fs_startup_error)?;
        reader
            .probe_capability()
            .map_err(read_adapter::map_fs_startup_error)?;
        let reader = std::sync::Arc::new(reader);
        let read_adapter = std::sync::Arc::new(read_adapter::FsBlobCasReadAdapter::new(
            std::sync::Arc::clone(&reader),
        ));

        let mut upload_hashes = Vec::with_capacity(HASH_SHARDS);
        for _ in 0..HASH_SHARDS {
            upload_hashes.push(Mutex::new(std::collections::HashMap::new()));
        }
        let mut referrer_locks = Vec::with_capacity(REFERRER_SHARDS);
        for _ in 0..REFERRER_SHARDS {
            referrer_locks.push(Mutex::new(()));
        }

        Ok(Self {
            root,
            max_upload_bytes,
            upload_hashes,
            referrer_locks,
            repo_locks: std::sync::Mutex::new(std::collections::HashMap::new()),
            reader,
            read_adapter,
            manifest_listing_limits,
            gc_discovery_limits,
            gc_ref_limits,
        })
    }

    /// Existing constructor: forwards to try_new_with_gc_limits with compiled defaults.
    pub fn try_new_with_limits(
        root: PathBuf,
        max_upload_bytes: u64,
        limits: storage_fs::DirEnumerationLimits,
    ) -> Result<Self, StorageError> {
        Self::try_new_with_gc_limits(
            root,
            max_upload_bytes,
            limits,
            repo_discovery::DiscoveryLimits::default(),
            manifest_refs::ManifestReferenceLimits::default(),
        )
    }

    /// Existing convenience constructor: forwards to try_new_with_limits.
    pub fn try_new(root: PathBuf, max_upload_bytes: u64) -> Result<Self, StorageError> {
        Self::try_new_with_limits(
            root,
            max_upload_bytes,
            manifest_listing::default_manifest_dir_limits(),
        )
    }

    /// Existing panicking constructor for tests.
    pub fn new(root: PathBuf, max_upload_bytes: u64) -> Self {
        Self::try_new(root, max_upload_bytes).unwrap_or_else(|err| {
            panic!("failed to initialize FsStorage: {err}");
        })
    }
}
```

#### 5.6.1 Production Type Visibility & External Integration Testing Configuration

1. **Crate-Visible Constructor Policy (Recommended):**
   - `DiscoveryLimits` and `ManifestReferenceLimits` remain `pub(crate)` within `src/storage/fs/` to encapsulate GC traversal budgeting details within the crate, avoiding exposing provisional limit structs in the public library interface.
   - `try_new_with_gc_limits` is defined as `pub(crate)` on `FsStorage`.
   - Production wiring (`storage_wiring_try_from_config` in `src/storage/mod.rs`) resides within the crate and directly invokes `FsStorage::try_new_with_gc_limits`, passing limits parsed and validated from `Config`.
2. **How External Integration Tests Configure Limits:**
   - External integration tests located in `tests/*.rs` configure custom GC limits through the supported public configuration API:
     - By constructing a `Config` (or setting environment variable overrides such as `REGISTRY__STORAGE__FS__GC__DISCOVERY__*` / `REGISTRY_BLOB_GC_DISCOVERY_*`) and invoking `registry_rust::storage::storage_wiring_try_from_config(&config)` or `registry_rust::storage::storage_wiring_from_config(&config)`.
     - Or by using the public constructors `FsStorage::try_new_with_limits(root, max_upload_bytes, limits)` and `FsStorage::try_new(root, max_upload_bytes)`, which remain public and provide compiled safe default GC limits.
3. **Public Visibility Alternative (Rejected for this Slice):**
   - If `try_new_with_gc_limits` were made `pub fn`, both `DiscoveryLimits` and `ManifestReferenceLimits` would need to be `pub struct` and re-exported from `crate::storage::fs::*`.
   - However, `pub(crate)` constructor visibility is recommended to avoid premature stabilization of internal traversal limit types before production benchmarking.
4. **Test Helpers Remain Strictly Test-Only:**
   - Test helpers such as `DiscoveryLimits::test_default()` and `ManifestReferenceLimits::test_default()` remain `#[cfg(test)] pub(crate)` inside unit test modules and are never exposed in production signatures.
   - Production defaults remain explicitly proposed, documented, and unvalidated until production benchmarking.

### 5.7 Separate Primary and Proxy Construction Tracing

Construction of primary storage and proxy cache storage are strictly separated:

1. **Primary Storage Construction ([`src/storage/mod.rs:829-842`](file:///home/dietmar/devel/rust/registry-rust/src/storage/mod.rs#L829-L842)):**
   - In `storage_wiring_try_from_config(config: &Config)`:
   - For `StorageBackend::Filesystem`, `FsStorage` is constructed via `try_new_with_gc_limits`, passing:
     - `config.fs_root.clone()`
     - `config.max_upload_bytes`
     - Validated listing limits (`fs_manifest_listing_max_entries`, `fs_manifest_listing_max_name_bytes`)
     - Validated `DiscoveryLimits` populated from `config.fs_gc_discovery_*`
     - Validated `ManifestReferenceLimits` populated from `config.fs_gc_discovery_*`
   - The resulting `FsStorage` is wrapped in `Arc::new` and converted into `StorageWiring::from_backend`, exposing both `gc_port` and `gc_service_port`.
2. **Proxy Cache Storage Construction ([`src/storage/mod.rs:880-905`](file:///home/dietmar/devel/rust/registry-rust/src/storage/mod.rs#L880-L905)):**
   - In `proxy_cache_storage_try_from_config(config: &Config, upstream: Option<&ProxyUpstreamRoute>)`:
   - For `StorageBackend::Filesystem`, proxy cache storage constructs `FsStorage` via `try_new_with_limits(root, config.max_upload_bytes, limits)`.
   - This forwards to `try_new_with_gc_limits` with compiled safe defaults (`DiscoveryLimits::default()` and `ManifestReferenceLimits::default()`).
   - The resulting `FsStorage` is returned as `Arc<dyn ports::ProxyStoragePort>`.
   - **Policy:** Although `ProxyStoragePort` does not expose `GcStoragePort` and never participates in GC sweeps, its underlying `FsStorage` instance maintains defined, validated limits without requiring operators to configure unused GC settings for proxy caches.

### 5.8 Asynchronous Startup `spawn_blocking` Boundary

`FsStorage` construction performs blocking filesystem operations synchronously (`ensure_dir`, `FsMetadataReader::open(&root)`, and `reader.probe_capability()`).

The asynchronous startup sequence **must explicitly preserve** the `spawn_blocking` boundary:
1. In [`src/storage/mod.rs:939-959`](file:///home/dietmar/devel/rust/registry-rust/src/storage/mod.rs#L939-L959), `storage_wiring_try_from_config_async_with_factory` executes:
   ```rust
   match config.storage_backend {
       StorageBackend::Filesystem => {
           let config_clone = config.clone();
           tokio::task::spawn_blocking(move || storage_factory(&config_clone))
               .await
               .map_err(|join_err| {
                   StorageError::backend(format!(
                       "filesystem storage initialization task failed: {join_err}"
                   ))
               })?
       }
       StorageBackend::S3 => storage_factory(config),
   }
   ```
2. In [`src/runtime.rs:249-275`](file:///home/dietmar/devel/rust/registry-rust/src/runtime.rs#L249-L275), `init_server_storage_wiring` invokes `storage_wiring_try_from_config_async_with_factory`, ensuring the primary server runtime offloads root descriptor opening and capability probing to a dedicated worker thread.
3. In [`src/cli/runtime.rs:63`](file:///home/dietmar/devel/rust/registry-rust/src/cli/runtime.rs#L63), the CLI runtime composition similarly delegates through `storage_wiring_try_from_config_async_with_factory`.
4. In [`src/storage/mod.rs:961-980`](file:///home/dietmar/devel/rust/registry-rust/src/storage/mod.rs#L961-L980), `proxy_cache_storage_try_from_config_async_with_factory` offloads proxy cache constructor execution to `tokio::task::spawn_blocking`.

This explicit offloading prevents blocking the Tokio reactor thread pool during initial storage initialization.

---

## 6. GC Safety, Execution Lifecycle & Mutation Realities

### 6.1 Placement in the GC Lifecycle

Manifest discovery executes at three distinct points in the GC lifecycle:

1. **Pre-Planning ([`blob_gc_plan`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/mod.rs#L129-L194)):**
   - Executed **once** at line 137 before CAS candidate iteration begins.
   - Entirely read-only. Populates `PolicyContext.manifest_protected`.
   - If discovery fails, planning aborts immediately; 0 candidate blobs are scanned or reported as eligible.
2. **Candidate Quarantine ([`blob_gc_quarantine_with_authority`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/mod.rs#L226-L320)):**
   - Executed **per candidate** inside the iteration loop at line 278.
   - Sequence for candidate $N$:
     1. Verify candidate age: `check_candidate_age` (L263).
     2. Verify active authority permit: `authority.gc_mutation_permit()` (L274).
     3. Acquire revalidation lock: `_reval_guard = consistency.acquire_gc_revalidation().await` (L276).
     4. **Execute contained manifest discovery** via `PolicyContext::build` (L278).
     5. Check pins (`policy_ctx.is_pinned`) and references (`policy_ctx.is_referenced`).
     6. If unreferenced: execute quarantine mutation (`storage.quarantine_blob`) (L290).
     7. Drop `_reval_guard`.
3. **Quarantine Deletion ([`blob_gc_delete_fs_with_authority`](file:///home/dietmar/devel/rust/registry-rust/src/blob_gc/mod.rs#L395-L586)):**
   - Executed **per candidate** inside the quarantine inspection loop.
   - Sequence for quarantine candidate $N$:
     1. **Metadata Initialization (Disk Mutation!):** Lines 486–491 read `read_quarantine_time(cfg, &digest)`. If missing, it executes `write_quarantine_time(cfg, &digest, now)` and continues. This writes a timestamp file to disk **before** age evaluation and **before** discovery!
     2. Check quarantine delay: `check_candidate_age(q_at, now, quarantine_delay)` (L494).
     3. Acquire revalidation lock: `reval_guard = consistency.acquire_gc_revalidation().await` (L508).
     4. **Execute contained manifest discovery** via `PolicyContext::build` (L510).
     5. Check pins (`policy_ctx.is_pinned`) and references (`policy_ctx.is_referenced`).
     6. If referenced: execute restoration mutation (`storage.restore_quarantined_blob`) (L518).
     7. If unreferenced: call `execute_guarded_gc_deletion` (L552), which calls `storage.delete_blob_conditional(...)`.
     8. Drop `reval_guard`.

### 6.2 Mutation Stopping vs. Non-Rollback Realities

1. **Discovery Failure Halts Destructive Mutation:**
   - If manifest discovery fails for candidate $N$ (due to I/O error, corrupt JSON, or budget exhaustion), `PolicyContext::build` returns `Err(GcPolicyError::ManifestDiscovery)`.
   - The loop immediately terminates. Candidate $N$ is **not** quarantined, restored, or deleted.
2. **Prior Mutations Remain Committed (No Global Rollback):**
   - Standard Linux filesystems do not provide transactional rollback.
   - If candidate $N = 50$ fails discovery:
     - Candidates $1 \dots 49$ that were already quarantined in `quarantine_with_authority` remain in the quarantine directory.
     - Candidates $1 \dots 49$ that were already unlinked in `delete_fs_with_authority` remain permanently deleted.
     - Any quarantine timestamp files written in step 1 remain written on disk.
   - A discovery error halts subsequent destructive work; it **does not and cannot** roll back earlier committed mutations.

### 6.3 Retention of Per-Candidate Revalidation

- **No Caching in this Slice:** Per-candidate revalidation under `consistency.acquire_gc_revalidation()` is explicitly **retained**. Introducing timestamp-based `PolicyContext` caching is rejected because it requires a separately designed, reviewed, and verified cache invalidation and concurrency protocol.
- **Operational Cost:** In large registries, re-running discovery per candidate scales as $O(C \times (D + M))$. Operators must account for this I/O and CPU cost, tuning GC batch sizes (`max_blobs`) accordingly.
- **Visibility Qualifications:** Iterative directory traversal is **not a point-in-time snapshot**. Directory entries are observed sequentially. Furthermore, `consistency.acquire_gc_revalidation()` only synchronizes against writers that participate in consistency coordination; out-of-band filesystem operations do not respect this lock.

### 6.4 Multi-Layered Defense-in-Depth Safeguards

Omission of a manifest from the discovered protected set weakens reachability protection, but **does not autonomously trigger physical deletion**. Five separate safeguards remain active:
1. **Persistent Directed Acyclic Graph (`BlobRefIndex` in sled):** An unreferenced manifest in storage cannot cause deletion if the blob is recorded as reachable from an active tag or manifest in sled.
2. **Active Upload Pins (`is_pinned`):** In-flight uploads and reservations are protected by pin leases.
3. **Repository Memberships:** Manifest-blob linkages tracked via `RepositoryBlobMembershipStorage` require explicit membership unlink sweeps.
4. **Minimum Candidate Age:** Candidates younger than `blob_gc_default_min_age_secs` are skipped before discovery is reached.
5. **Quarantine Delay Window:** Quarantined blobs must age beyond `blob_gc_default_quarantine_delay_secs` before permanent unlinking.

---

## 7. Promotion Plan & Realistic Integration Verification

### 7.1 Promotion without Divergent Implementations

The test seams ([`repo_discovery.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/repo_discovery.rs) and [`manifest_refs_seam.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/manifest_refs_seam.rs)) were authored with production-ready error handling and checked arithmetic. They will be promoted directly:
1. Remove `#[cfg(test)]` guards in `src/storage/fs.rs`.
2. Move `manifest_refs_seam.rs` to `src/storage/fs/manifest_refs.rs`.
3. Retain unit and fault-injection tests in `mod tests` within those files.
4. Wire `FsStorage::discover_manifest_references` to invoke the promoted modules using configured limits.

### 7.2 Realistic Caller Integration Tests

Integration tests must exercise real GC callers with observable state counters:

```rust
// tests/gc_contained_discovery_integration_tests.rs

/// 1. Planning Caller Test: Verifies contained discovery populates protected set
#[tokio::test]
async fn test_gc_service_plan_populates_protected_set_via_pinned_reader() {
    // Setup FsStorage, BlobRefIndex, and GcService with 1 repository and 1 manifest.
    // Assert plan returns BlobGcStats with zero errors.
    // Assert that referenced blobs are not reported as eligible.
}

/// 2. Quarantine Caller Test: Verifies limit exhaustion halts mutation
#[tokio::test]
async fn test_gc_service_quarantine_halts_mutation_on_discovery_limit() {
    // Configure tight max_manifests_read = 1 with 2 manifests present.
    // Run gc_service.quarantine().
    // Assert Err returned containing GcPolicyError::ManifestDiscovery.
    // Assert stats.quarantined_blobs == 0.
    // Assert candidate blob is NOT moved to quarantine/ directory.
}

/// 3. Deletion Caller Test: Verifies discovery failure prevents unlinking
#[tokio::test]
async fn test_gc_service_delete_halts_unlinking_on_discovery_error() {
    // Populate quarantine with an eligible blob.
    // Inject corrupt JSON into an unrelated manifest file.
    // Run gc_service.delete().
    // Assert Err returned containing GcPolicyError::ManifestDiscovery.
    // Assert candidate blob remains present in quarantine/ directory.
}

/// 4. Capability Forwarding Test: Verifies wrappers preserve discovery
#[tokio::test]
async fn test_storage_wiring_and_hooked_storage_forward_discovery() {
    // Wrap FsStorage in HookedStorage, LifecycleFaultStorage, and StorageWiring.
    // Assert discover_manifest_references returns Ok(Some(digests)).
    // Wrap S3Storage in StorageWiring.
    // Assert discover_manifest_references returns Ok(None).
}

/// 5. Separated Pinned Reader Test: Asserts reader containment
#[tokio::test]
async fn test_pinned_reader_discovery_isolated_from_pathname_swap() {
    // Verify contained reader continues reading original inode after root symlink swap.
    // Note: Do not execute mutations through swapped pathname without separate writer handling.
}
```

---

## 8. Operational Limits, Platform Constraints & Safe Rollback

### 8.1 Operational Constraints

1. **Linux `/proc/self/fd` Requirement:**
   - Contained payload opening requires reopening descriptors via `/proc/self/fd/<raw_fd>`.
   - Requires a genuine, accessible, stable `procfs` mount. Container environments with masked `/proc` will fail closed.
2. **Mount and Hard-Link Limitations:**
   - Linux `openat2` with `RESOLVE_BENEATH` prevents path resolution from escaping the root descriptor, but does not isolate hard links within the same filesystem mount, nor does it isolate sub-mounts without `RESOLVE_NO_XDEV`.
3. **Non-Linux Platforms Explicitly Unverified:**
   - Linux descriptor containment (`openat2`) is unavailable on macOS, FreeBSD, and Windows. Non-Linux compilation and execution are **explicitly unverified** and unsupported for contained discovery.
4. **Divergence Between Pinned Reads and Pathname Mutations:**
   - `FsStorage` reads via the pinned `reader: Arc<FsMetadataReader>`, but writes files via pathname strings (`self.root.join(...)`). If the root pathname is swapped externally while the server runs, readers and writers will access divergent physical directories.

### 8.2 Safe Rollback Strategy

The previous suggestion of returning `Ok(None)` from `FsStorage` when a feature flag is disabled **is rejected as unsafe**, because returning `None` routes GC through public catalog discovery, which silently omits root-adjacent and reserved-ancestor manifests.

The safe operational rollback strategy is:
1. **Revert Deployment:** Revert the application binary to the prior release.
2. **Explicitly Disable GC:** If contained discovery fails in production, administrators should immediately disable GC operations via existing configuration:
   - `REGISTRY_BLOB_GC_ENABLED=false` or `REGISTRY_BLOB_GC_ENABLE_DELETE=false`.
3. **No Retained Legacy Bypass:** Retaining the uncontained walker (`build_manifest_protected_set_fs`) as an active runtime fallback toggle is **not approved** and would require a separate architectural design and compatibility justification.

---

## 9. Mechanically Extracted Source Evidence

### Excerpt 1: Production GC Bypass Routing
- **File:** `src/blob_gc/policy.rs` | **Lines:** 166–176 | **SHA-256:** `b5fb40df976f8cb9d3ad062a2497b58dcfe2612584aa42a13465704d9ac19094`

```rust
pub async fn build_manifest_protected_set(
    cfg: &crate::config::Config,
    storage: &(impl storage::GcServiceStoragePort + ?Sized),
) -> Result<HashSet<String>, GcPolicyError> {
    if storage.kind() == "fs"
        && tokio::fs::metadata(&cfg.fs_root.join("repos"))
            .await
            .is_ok()
    {
        return build_manifest_protected_set_fs(&cfg.fs_root).await;
    }
```

### Excerpt 2: Quarantine Timestamp Initialization Before Discovery
- **File:** `src/blob_gc/mod.rs` | **Lines:** 486–492 | **SHA-256:** `92023ddf7765bdaf0fd2ed2f541d356530f795291661c59ae29b6cd522da0c44`

```rust
            let q_at = match read_quarantine_time(cfg, &digest).await? {
                Some(t) => t,
                None => {
                    let _ = write_quarantine_time(cfg, &digest, now).await;
                    continue;
                }
            };
```

### Excerpt 3: Per-Candidate Revalidation in Quarantine Loop
- **File:** `src/blob_gc/mod.rs` | **Lines:** 276–294 | **SHA-256:** `92023ddf7765bdaf0fd2ed2f541d356530f795291661c59ae29b6cd522da0c44`

```rust
            let _reval_guard = consistency.acquire_gc_revalidation().await;

            let mut policy_ctx = PolicyContext::build(cfg, storage, idx, policy).await?;

            if policy_ctx.is_pinned(&candidate.digest, now)? {
                drop(_reval_guard);
                continue;
            }

            if policy_ctx.is_referenced(&candidate.digest).await? {
                drop(_reval_guard);
                continue;
            }

            let q_res = storage
                .quarantine_blob(&permit, &candidate.digest, &candidate.version)
                .await;

            drop(_reval_guard);
```

### Excerpt 4: Existing GcStoragePort Arc Blanket Implementation
- **File:** `src/storage/ports/mod.rs` | **Lines:** 973–980 | **SHA-256:** `6d495636aad3c1da533474652941b5a6bb783f224421ffdae602a208b891a1e3`

```rust
#[async_trait]
impl<T: ?Sized + GcStoragePort + Send + Sync> GcStoragePort for Arc<T> {
    fn kind(&self) -> &'static str {
        (**self).kind()
    }
    fn gc_strategy(&self) -> GcStorageStrategy {
        (**self).gc_strategy()
    }
```

### Excerpt 5: Payload Ceiling Sentinel Overflow Protection
- **File:** `src/storage/fs/manifest_refs_seam.rs` | **Lines:** 323–326 | **SHA-256:** `c610a8cc499e144873f1e87699131a5375bfa8b3ae46fce8c8c55641894ee303`

```rust
    let sentinel_limit = limit
        .checked_add(1)
        .ok_or_else(|| StorageError::backend("payload ceiling limit arithmetic overflow"))?;
```

### Excerpt 6: Asynchronous Storage Wiring spawn_blocking Boundary
- **File:** `src/storage/mod.rs` | **Lines:** 939–959 | **SHA-256:** `6a086b45fcbeeeef8cffc1e9e7fa6fe4ae06b4904feeb54e0ddfa0f5ce252199`

```rust
pub(crate) async fn storage_wiring_try_from_config_async_with_factory<F>(
    config: &Config,
    storage_factory: F,
) -> Result<StorageWiring, StorageError>
where
    F: FnOnce(&Config) -> Result<StorageWiring, StorageError> + Send + 'static,
{
    match config.storage_backend {
        StorageBackend::Filesystem => {
            let config_clone = config.clone();
            tokio::task::spawn_blocking(move || storage_factory(&config_clone))
                .await
                .map_err(|join_err| {
                    StorageError::backend(format!(
                        "filesystem storage initialization task failed: {join_err}"
                    ))
                })?
        }
        StorageBackend::S3 => storage_factory(config),
    }
}
```

### Excerpt 7: LifecycleFaultStorage Wrapping FsStorage
- **File:** `tests/manifest_lifecycle_tests.rs` | **Lines:** 3545–3547 | **SHA-256:** `5db488f58b093374ba274ca8bcfbb7cb9cb3e2008f51a2c340d8281176b668d2`

```rust
#[derive(Clone)]
struct LifecycleFaultStorage {
    inner: Arc<FsStorage>,
```

---

## 10. Preserved Document Evidence & Baseline Register

### `registry-rust` Baselines (HEAD: `d51ea1ac69921dfdbd583b6b197b80b1f149e232`)
- `docs/architecture/adr-001-first-refactoring-boundary.md`: `fafc8a82cc571ec622b21dc9c32518aef35e89a383f4d7719365f13a5dfaefbe`
- `docs/architecture/adr-002-application-service-boundary.md`: `37f3294b2214893aa9d633b30afe45c9884f02f8ed6d8e97ebab7780b20a4c56`
- `docs/architecture/adr-003-storage-capability-ports.md`: `c3f755fa056bb10b01243df7082ec39e2cccf59dc88f46185c092c27aaab43f1`
- `docs/architecture/adr-004-application-read-services.md`: `104c728c49d5f507dde0c3df9b2b00c25bb7554ec045617e609e66ef3572d503`
- `docs/architecture/adr-005-server-runtime-composition-root.md`: `ab935125574b042c94f26d5f9bf54879a7cd6567f1235a7009ade4760a1070aa`
- `docs/architecture/adr-006-cli-runtime-composition.md`: `44abab2c2d5458581113d72d13b32870715129ee3c8a344194bea3a7c8d68a19`
- `docs/architecture/adr-007-manifest-compatibility-consolidation.md`: `670bab979b6c3abe9a6eb56697f39273df49960cc0484e8b6783f5cc93a7d03b`
- `docs/architecture/adr-008-http-transport-test-topology.md`: `71ec04ef7d6f04a063e45fd4dbd902ea76ea2d48429b5dec0e1857835a714ad3`
- `docs/architecture/adr-009-structured-storage-error-taxonomy.md`: `e3bc75f8d53c3341b01883d6452532da571025cff89376f7349761086d278648`
- `docs/architecture/current-code-assessment.md`: `e92bf0ce653c8bec3e2e27b5d30215e3608c08012f3e97b019afa7da054fe980`
- `docs/architecture/filesystem-cas-listing-characterization.md`: `d9cf61dd7e6274db40c6b46ccec1b138bb52a8b948f838422aa1bb36f350e875`
- `docs/architecture/filesystem-cas-listing-integration-assessment.md`: `d2cab0aba1282b8fd54eb8c34c7d5d3cb207261e645fd4244ba4abbb65f25529`
- `docs/architecture/filesystem-cas-listing-production-cutover.md`: `a38b5c49c2ad0c0a49fdfac001f02888880f36d9e4968e2ce2c81e7da6651d29`
- `docs/architecture/filesystem-cas-listing-production-integration-design.md`: `4e9a93b3e607201bf1d003103c56c663b7c8a562d61d7e599ef7046e32ad8d3c`
- `docs/architecture/filesystem-gc-contained-metadata-design.md`: `3e154b597099a409875bf1b63794964b4c169f7136f44d9f490fad22b0414c47`
- `docs/architecture/filesystem-gc-directory-discovery-seam.md`: `56a72959c4d816ee82a0b90304183aa7a399a80ca76c67959e51f26748868129`
- `docs/architecture/filesystem-gc-manifest-discovery-characterization.md`: `4ab9d3491c09911db5fc66629c6b1079c7ada01c31a59bdbf2a731d75c749833`
- `docs/architecture/filesystem-gc-manifest-discovery-integration-design.md`: `8218ed8bb76e0d39e54893ed50f0b7311c81919943fa0c5b3eece1005d3567ac`
- `docs/architecture/filesystem-gc-manifest-reference-seam-design.md`: `d2f2d76857f72c36021a966ef32a66f9382f834f448ef315e35005972a2275e5`
- `docs/architecture/filesystem-gc-manifest-reference-seam.md`: `49457b99bc62a4126fd7cd317a5a07146cc5aa33647c87ab25427b00cecb3dd6`
- `docs/architecture/filesystem-gc-repository-discovery-decisions.md`: `9e63d7e20a50c4a9494b32905fd2db3061144fda493060e0f808f169557b780c`
- `docs/architecture/filesystem-manifest-listing-characterization.md`: `578bb7ace87dafafe161acf778fbad475d8ecf20915752feb1f129f5f44a78b1`
- `docs/architecture/filesystem-manifest-listing-contained-integration-design.md`: `6bb88f989249cb068c67d285e154b10206bf1fba88064b1ba6f4113f7af4447b`
- `docs/architecture/filesystem-manifest-listing-production-cutover.md`: `a73d51f18f4c8be5fc92d3dae032c604f42c5d3714907e986d8d93d2db21de7e`
- `docs/architecture/filesystem-manifest-listing-production-decisions.md`: `5dcb53845bedecec299c5ec8ee7026abf48b60692058bd28af3f3564e78df30d`
- `docs/architecture/filesystem-manifest-listing-production-readiness-assessment.md`: `e0999defc75a56f9b7b14a22bc0f295c598d7ce4d048fbe10cabd6fc9d98ddaa`
- `docs/architecture/filesystem-manifest-read-characterization.md`: `13fe826d9fbc365ecc1c2350b16bad6ed253415f3a1f248e1ec786f33cb273d1`
- `docs/architecture/filesystem-manifest-read-integration-assessment.md`: `28297f6d774a3ad447cd4a3fb8d570c3e64989d072c588174f6dba7fc9eb2d4d`
- `docs/architecture/filesystem-manifest-read-production-cutover.md`: `0846fce8c12dc73ec08faf33a24d60d4e5f9ab98ff45f5a0fbe39af627d66b75`
- `docs/architecture/filesystem-manifest-read-production-integration-design.md`: `4f77379730e6044744a7ca755b25a376cc22145d3ce6a15b3774d2fd4e18801f`
- `docs/architecture/filesystem-production-read-cutover.md`: `d0fb1eb8490a58eace1ba8fa7b292664e9c73f9ab9f1acb72e51d02f5948309d`
- `docs/architecture/filesystem-reference-index-sync-hardening-design.md`: `95d2ed55cc8daf98ea9595e2a2f0ef6c52aba92567ed8f5ed35a10b62dd0bc6a`
- `docs/architecture/filesystem-repository-discovery-characterization.md`: `3d48160a1bf039203d1cc5cef1699c6951a68cd044ac2c857682df668ae6a411`
- `docs/architecture/o-05-filesystem-metadata-containment.md`: `2ff4a8750a1878a80c71f86226454997d9334967f65e2dfede9581ea15018f05`
- `docs/architecture/o-05-linux-descriptor-metadata-experiment.md`: `aab7b3e3413def869bfdc79f37589ff2c419623d7a0ccbaa64f56d947bc36f25`
- `docs/architecture/storage-fs-metadata-integration-assessment.md`: `15f9f5ac7214644d3d20ab14527460f5f676930905bd4eff0666d56a4e35c275`

### `storage-layer-rust` Baselines (HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`)
- `docs/architecture/filesystem-payload-acquisition-experiment.md`: `68a408052f4f583778b3c5a05f0335e7e818bfb5fdd11ba7fe069c47d85fa8eb`
- `docs/architecture/filesystem-payload-read-design.md`: `a4667cfb949eac37922834f3d14fa56d64d07b8aef10828046ff15a5183ed2c8`
