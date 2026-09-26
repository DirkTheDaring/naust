> **Note (2026-09-19):** this file hosts one of the fullest historical gate tables; gate STATUS is tracked only in [acceptance-gates.md](../../technical-debt.md).

# Architecture Assessment: Refreshed Filesystem Read-Containment Gaps

> **Historical snapshot.** Tag listing later sits on `tag_domain` (`32c42c6`). Current residual inventory: [`current-state.md`](current-state.md).

- **Document:** `docs/architecture/filesystem-read-containment-post-tag-listing-assessment.md`
- **Status:** Read-Only Architectural Gap Assessment & Next Slice Proposal (Corrected Record)
- **Primary Repository Baseline:** `~/devel/rust/registry-rust` at HEAD [`f1d6d9c128a8a713f2d929b03c3a66ed06bf30fe`](registry-rust) (`master`)
- **Dependency Repository Baseline:** `~/devel/rust/storage-layer-rust` at HEAD [`0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`](storage-layer-rust) (`main`, strictly read-only)
- **Latest Reviewed Cutover Package:** `~/devel/rust/manifest-read-review-evidence/session-20260912-2300/filesystem-tag-listing-production-cutover.tar.gz` (Size: 197,747 bytes; SHA-256: `521ece9d608ae555d4e53a5ff1fc640fa8ba40cbff75b2f3672cd949b5eef8af`)
- **Recorded Cutover Verification:** 122 passing unit/integration tests and 3 ignored tests (unprivileged permissions tests).
- **Canonical Quality Gates:** `O-03`, `O-04`, `O-05`, `O-06`, `O-13`, `O-15`, `O-16`, and `D-06` remain explicitly **OPEN**.

---

## 1. Executive Summary & Assessment Scope

Following the local commit of bounded filesystem tag-listing production cutover in commit [`f1d6d9c128a8a713f2d929b03c3a66ed06bf30fe`](registry-rust), both `FsStorage::list_tags` and `FsStorage::list_tags_page` operate through contained descriptor-relative enumeration beneath a pinned storage root descriptor via [`storage_fs::FsMetadataReader`](storage-layer-rust/crates/storage-fs/src/reader.rs).

Promoting contained tag listing into production fulfills the tag-listing milestones characterized in commit `6d13cd9`, validated in seam commit `8dd1799`, and hardened across callers in lifecycle commit `96c0729`, migration commit `5779f7f`, and supervisor commit `02cfa07`.

However, completing this cutover does **not** close canonical Quality Gate **O-05 (Broader filesystem read containment)**. Multiple production filesystem read paths in [`FsStorage`](src/storage/fs.rs) remain uncontained, continuing to execute ambient path-based operations via `tokio::fs` or `std::fs`.

This document provides a refreshed, comprehensive gap assessment against current production source:
1. Reconciles prior assessments (`filesystem-read-containment-remaining-gaps.md`, baseline `2fc21aa`) with active source, removing obsolete labels and recording the exact operational contracts of committed cutovers.
2. Delivers an exhaustive mechanical inventory of all remaining uncontained filesystem read paths in [`src/storage/fs.rs`](src/storage/fs.rs) and surrounding modules, specifying symbols, line references, callers, operation types, root ownership, containment mechanisms, resource bounds, error handling, and mutation relationships.
3. Traces caller consequences precisely for remaining high-priority gaps. In particular, it separates direct-read reachability (`list_referrers`) from paged error suppression (`list_referrers_page`), tracing actual reference sites and establishing that `list_referrers_page` has no active production caller.
4. Corrects validation terminology, distinguishing structural path validation from canonical repository grammar.
5. Accurately classifies read-modify-write operations (such as `clear_membership_candidate`) as ambient reads embedded in mutation workflows rather than standalone observation reads.
6. Evaluates remaining gaps against six architectural criteria and recommends exactly one immediate next bounded slice: **OCI Referrers Read Characterization (`list_referrers` & `list_referrers_page`)**, freezing existing production behavior in controlled test fixtures without changing production routing or prematurely enforcing unapproved containment policies.

---

## 2. Baselines, Environment & Evidence Verification

### 2.1 Repository Baselines & Working Tree States

The baselines and working tree states were inspected prior to analysis:

| Property | Primary Repository (`registry-rust`) | Dependency Repository (`storage-layer-rust`) |
| :--- | :--- | :--- |
| **Path** | `~/devel/rust/registry-rust` | `~/devel/rust/storage-layer-rust` |
| **Branch** | `master` | `main` |
| **HEAD Commit** | `f1d6d9c128a8a713f2d929b03c3a66ed06bf30fe` | `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` |
| **Latest Commit Message** | `fix(storage): use contained filesystem tag listing in production` | `chore: sync documentation and architecture records` |
| **Tracked Index State** | Clean (0 staged tracked files) | Clean (0 staged tracked files) |
| **Index Listing SHA-256** | `84ed2dd089d2aed312d262914979ba5fa612dc1d82d938730e303ee8991de9e3` | `44132dfe678626f7d7e49fe69fda67046c07bb800667b74092e15dd47e767e68` |
| **Working Tree Diff vs HEAD** | Clean (`git diff-index --quiet HEAD --` exits 0) | Clean (`git diff-index --quiet HEAD --` exits 0) |
| **Working Tree Content SHA-256** | `3054e900996e297d419fcd64f7b6fe1e7e24d1cb261e027cdb7f1ade0e261579` | `cd97629e41d854087743d2e8ce258df26daf504674282f1555709e64f4686006` |
| **Mode** | Read-Only Analysis & Documentation | Strictly Read-Only |

### 2.2 Latest Cutover Package Verification

The reviewed cutover package for the preceding milestone was verified:
- **Archive Path:** `~/devel/rust/manifest-read-review-evidence/session-20260912-2300/filesystem-tag-listing-production-cutover.tar.gz`
- **Verified Size:** 197,747 bytes
- **Verified SHA-256:** `521ece9d608ae555d4e53a5ff1fc640fa8ba40cbff75b2f3672cd949b5eef8af`
- **Verification Records:** Contains 42 payload files plus `MANIFEST.sha256`. Recorded verification includes 122 passing tests and 3 ignored tests. None of these tests were re-executed during this read-only assessment.

### 2.3 Committed Tag-Listing Cutover Contract

The cutover committed in `f1d6d9c` established the following production contracts:
- **Active Production Routing:** Both `FsStorage::list_tags` ([`src/storage/fs.rs:1020-1028`](src/storage/fs.rs#L1020-L1028)) and `FsStorage::list_tags_page` ([`src/storage/fs.rs:1213-1230`](src/storage/fs.rs#L1213-L1230)) route through contained seam functions (`tag_listing::contained_list_tags_seam` and `tag_listing::contained_list_tags_page_seam`) using `self.reader: Arc<FsMetadataReader>`.
- **Approved Operational Limits:**
  - `tag_listing_max_entries`: 10,000 (minimum 1, maximum `usize::MAX`).
  - `tag_listing_max_name_bytes`: 1,500,000 (minimum 128, maximum `usize::MAX`).
  - `tag_listing_repo_probe_max_entries`: 64 (minimum 1, maximum `usize::MAX`).
  - `tag_listing_repo_probe_max_name_bytes`: 4,096 (minimum 64, maximum `usize::MAX`).
  - `tag_listing_max_payload_bytes`: 1,024 (minimum 256, strictly `< u64::MAX`).
- **Preserved Boundaries & Invariants:**
  1. `resolve_tag` ([`src/storage/fs.rs:1010-1018`](src/storage/fs.rs#L1010-L1018)) and `get_tag_with_version` ([`src/storage/fs.rs:1263-1275`](src/storage/fs.rs#L1263-L1275)) retain their default `TagReadLimits` (`max_payload_bytes: None`), unconstrained by listing payload limits.
  2. `list_tag_files` ([`src/storage/fs.rs:716-739`](src/storage/fs.rs#L716-L739)) remains preserved byte-for-byte and retains its active use by `delete_manifest` ([`src/storage/fs.rs:1812`](src/storage/fs.rs#L1812)) for referencing tag removal.
  3. Caller error policies were not altered by listing promotion; caller hardening was committed prior to cutover in commits `96c0729`, `5779f7f`, and `02cfa07`.
  4. Nonzero pages process all retained candidates in the directory before sorting and slicing; successive pages repeat directory enumeration and candidate payload reads.
  5. No snapshot isolation or global concurrency/memory budget was established.
  6. Earlier legitimate caller mutations can precede an error; no atomic rollback or hardware durability guarantee follows.

---

## 3. Reconciliation of Prior Assessments with Authoritative Source

Prior architectural records (`docs/architecture/filesystem-read-containment-remaining-gaps.md` authored at baseline `2fc21aa`, and `docs/architecture/storage-fs-metadata-integration-assessment.md`) analyzed read-containment before tag-read and tag-listing cutovers.

Between baseline `2fc21aa` and current HEAD `f1d6d9c`, nine commits advanced read containment and caller resilience:
1. `7d6649d`: `test(storage): characterize filesystem tag read semantics`
2. `20f9390`: `test(storage): validate contained filesystem tag reads`
3. `5a0b424`: `refactor(storage): route filesystem tag reads through contained reader`
4. `6d13cd9`: `test(storage): characterize filesystem tag listing semantics`
5. `8dd1799`: `test(storage): validate contained filesystem tag listing`
6. `96c0729`: `fix(lifecycle): propagate tag listing failures before destructive cleanup`
7. `5779f7f`: `fix(migration): propagate tag listing failures before readiness`
8. `02cfa07`: `fix(supervisor): propagate proxy tag listing failures`
9. `f1d6d9c`: `fix(storage): use contained filesystem tag listing in production`

### 3.1 Status Classification Reconciliation

To eliminate obsolete "test-only" or "legacy production" labels, all operations are classified under four distinct architectural categories:

| Component / Path | Prior Label (`2fc21aa`) | Current State (`f1d6d9c`) | Architectural Category | Source Evidence |
| :--- | :--- | :--- | :--- | :--- |
| **CAS Blob Reads** (`head_blob`, `open_blob`) | Contained | Contained | **Active Production Behavior** | [`src/storage/fs.rs:997-1008`](src/storage/fs.rs#L997-L1008) |
| **CAS Blob Listing** (`list_cas_blobs_page`) | Contained | Contained | **Active Production Behavior** | [`src/storage/fs.rs:3467-3479`](src/storage/fs.rs#L3467-L3479) |
| **Manifest Reads** (`head_manifest`, `get_manifest`) | Contained | Contained | **Active Production Behavior** | [`src/storage/fs.rs:1030-1044`](src/storage/fs.rs#L1030-L1044) |
| **Manifest Listing** (`list_manifest_digests_page`) | Contained | Contained | **Active Production Behavior** | [`src/storage/fs.rs:1197-1211`](src/storage/fs.rs#L1197-L1211) |
| **GC Manifest Ref Discovery** (`discover_manifest_references`) | Contained | Contained | **Active Production Behavior** | [`src/storage/fs.rs:3672-3682`](src/storage/fs.rs#L3672-L3682) |
| **Tag Direct Reads** (`resolve_tag`, `get_tag_with_version`) | Uncontained Gap | Contained (Cutover in `5a0b424`) | **Active Production Behavior** | [`src/storage/fs.rs:1010-1018`](src/storage/fs.rs#L1010-L1018), [`1263-1275`](src/storage/fs.rs#L1263-L1275) |
| **Tag Listing** (`list_tags`, `list_tags_page`) | Uncontained Gap | Contained (Cutover in `f1d6d9c`) | **Active Production Behavior** | [`src/storage/fs.rs:1020-1028`](src/storage/fs.rs#L1020-L1028), [`1213-1230`](src/storage/fs.rs#L1213-L1230) |
| **Metadata / Payload Seams** (`contained_metadata`, `metadata_seam`, `payload_seam`) | Test-Only Seams | Test-Only Seams | **Helpers / Seams Still Test-Only** | [`src/storage/fs.rs:3689-3703`](src/storage/fs.rs#L3689-L3703) |
| **Contained Referrer Reads** | Unimplemented | Unimplemented | **Proposed Work Not Implemented** | Does not exist; uses ambient `tokio::fs::read` |
| **Contained Catalog Discovery** | Unimplemented | Unimplemented | **Proposed Work Not Implemented** | Does not exist; uses ambient `tokio::fs::read_dir` |
| **Contained Membership Reads** | Unimplemented | Unimplemented | **Proposed Work Not Implemented** | Does not exist; uses ambient `tokio::fs::read` |
| **Supervisor `resolve_tag` Swallowing** | Deferred Risk | Deferred Risk | **Residual Risk Intentionally Deferred** | [`src/supervisor.rs:958`](src/supervisor.rs#L958) swallows non-fatal resolve errors |
| **Ref-Index Rebuild Partial State** | Deferred Risk | Deferred Risk | **Residual Risk Intentionally Deferred** | [`src/blob_ref_index.rs:724`](src/blob_ref_index.rs#L724) leaves `Building` state on abort |
| **Nonzero Page Candidate Repetition** | Deferred Risk | Deferred Risk | **Residual Risk Intentionally Deferred** | [`src/storage/fs/tag_listing.rs`](src/storage/fs/tag_listing.rs) re-scans all retained candidates |

---

## 4. Comprehensive Inventory of Remaining Filesystem Read Paths

This section traces all remaining production-reachable filesystem read paths in [`src/storage/fs.rs`](src/storage/fs.rs) and callers.

### 4.1 Master Inventory Table

The inventory explicitly separates standalone observation reads from reads embedded in mutation or read-modify-write workflows:

| Index | Area / Feature | Exact Source Symbol | Line Range | Operation Type | Root Ownership | Containment Status | Resource Limits | Mutation / Caller Impact |
|---|---|---|---|---|---|---|---|---|
| **R-1** | OCI Referrers Direct | `Storage::list_referrers` | [`fs.rs:1722-1735`](src/storage/fs.rs#L1722-L1735) | Payload read & JSON parse | Ambient pathname | **Uncontained** | None (unbounded `Vec<u8>`) | Production public route & mutation helper |
| **R-2** | OCI Referrers Paged | `Storage::list_referrers_page` | [`fs.rs:1232-1261`](src/storage/fs.rs#L1232-L1261) | In-memory sort & page slice | Ambient pathname | **Uncontained** | None (`unwrap_or_default`) | **No active production caller** |
| **R-3** | Catalog Discovery | `Storage::list_repositories` / `list_repo_names` | [`fs.rs:741-816`](src/storage/fs.rs#L741-L816), [`948-950`](src/storage/fs.rs#L948-L950) | Iterative dir walk + 4x stat | Ambient pathname | **Uncontained** | None (unbounded stack/RAM) | Observation input to GC/Safety |
| **R-4** | Catalog Timestamps | `Storage::repo_timestamps` / `max_mtime_in_dir` | [`fs.rs:818-844`](src/storage/fs.rs#L818-L844), [`952-972`](src/storage/fs.rs#L952-L972) | Dir enumeration & mtime | Ambient pathname | **Uncontained** | None | Pure observation |
| **R-5** | Storage Emptiness | `Storage::is_storage_empty` / `fs_dir_has_any_entry` | [`fs.rs:905-930`](src/storage/fs.rs#L905-L930), [`974-995`](src/storage/fs.rs#L974-L995) | Recursive dir entry probe | Ambient pathname | **Uncontained** | None | Startup & test readiness |
| **R-6** | Manifest Delete Discovery | `FsStorage::list_tag_files` | [`fs.rs:716-739`](src/storage/fs.rs#L716-L739), [`1812`](src/storage/fs.rs#L1812) | Dir enumeration & string read | Ambient pathname | **Uncontained** | None (reads all tag files) | **Embedded in Mutation** (`delete_manifest`) |
| **R-7** | Repo-Blob Membership | `get_repo_blob_membership` | [`fs.rs:2873-2898`](src/storage/fs.rs#L2873-L2898) | JSON payload read | Ambient pathname | **Uncontained** | None | Observation & verification |
| **R-8** | Repo-Blob Membership | `list_repo_blob_memberships_page` | [`fs.rs:2983-3109`](src/storage/fs.rs#L2983-L3109) | Dir walk & JSON reads | Ambient pathname | **Uncontained** | Max limit 1000 in heap | Observation input to sweep |
| **R-9** | Repo-Blob Membership | `list_all_repo_blob_memberships_page` | [`fs.rs:3111-3263`](src/storage/fs.rs#L3111-L3263) | 3-level dir walk & JSON reads | Ambient pathname | **Uncontained** | Max limit 1000 in heap | Observation input to sweep |
| **R-10** | Repo-Blob Membership | `count_repo_blob_memberships` | [`fs.rs:3265-3284`](src/storage/fs.rs#L3265-L3284) | Dir enumeration & metadata | Ambient pathname | **Uncontained** | None | Read-only count check |
| **R-11** | Repo-Blob Membership | `is_membership_ready` / `get_migration_checkpoint` | [`fs.rs:3286-3297`](src/storage/fs.rs#L3286-L3297), [`3343-3361`](src/storage/fs.rs#L3343-L3361) | Metadata probe & JSON read | Ambient pathname | **Uncontained** | None | Readiness gating |
| **R-12** | Lifecycle Recovery | `Storage::read_lifecycle_journal` | [`fs.rs:1343-1354`](src/storage/fs.rs#L1343-L1354) | JSON payload read | Ambient pathname | **Uncontained** | None | Mutation recovery check |
| **R-13** | Quarantine Inspection | `quarantined_blob_version` / `compute_fs_blob_version` | [`fs.rs:3437-3463`](src/storage/fs.rs#L3437-L3463), [`3597-3612`](src/storage/fs.rs#L3597-L3612) | Stat & streaming payload hash | Ambient pathname | **Uncontained** | 64 KB buffer; unbounded file | Precondition for GC delete |
| **R-14** | Quarantine Inspection | `read_quarantine_timestamp` | [`fs.rs:3402-3422`](src/storage/fs.rs#L3402-L3422) | Text payload read | Ambient pathname | **Uncontained** | None | GC age evaluation |
| **R-15** | Upload Inspection | `get_finalized_receipt` / `reap_expired_sessions` | [`fs.rs:2783-2801`](src/storage/fs.rs#L2783-L2801), [`2803-2868`](src/storage/fs.rs#L2803-L2868) | Dir enumeration & JSON reads | Ambient pathname | **Uncontained** | None | Upload cleanup & recovery |
| **R-16** | Membership Candidate Clear | `clear_membership_candidate` | [`fs.rs:2942-2970`](src/storage/fs.rs#L2942-L2970) | Read-modify-write | Ambient pathname | **Uncontained** | None | **Embedded in Mutation** (atomic write) |
| **R-17** | Tag Mutation Reads | `mutate_tag` / `delete_tag_conditional` | [`fs.rs:1101`](src/storage/fs.rs#L1101), [`1310`](src/storage/fs.rs#L1310) | Sync file read under lock | Ambient pathname | **Uncontained** | None | **Embedded in Mutation** |
| **R-18** | Manifest Delete Subject Read | `delete_manifest` (subject extract) | [`fs.rs:1788`](src/storage/fs.rs#L1788) | Async manifest payload read | Ambient pathname | **Uncontained** | None | **Embedded in Mutation** |

---

### 4.2 Detailed Analysis by Area

#### Area 1: OCI Referrer Reads and Listing (`list_referrers`, `list_referrers_page`)
- **Source Files & Symbols:**
  - `Storage::list_referrers` ([`src/storage/fs.rs:1722-1735`](src/storage/fs.rs#L1722-L1735))
  - `Storage::list_referrers_page` ([`src/storage/fs.rs:1232-1261`](src/storage/fs.rs#L1232-L1261))
  - Private helper `referrers_path` ([`src/storage/fs.rs:671-678`](src/storage/fs.rs#L671-L678))
- **Production Callers & Real Invocation Sites:**
  1. `ReferrersQueryService::query_referrers` ([`src/application/referrers.rs:51`](src/application/referrers.rs#L51)), serving the public OCI HTTP route `GET /v2/<name>/referrers/<digest>` ([`src/http_api/referrers.rs:52`](src/http_api/referrers.rs#L52)). **Calls `list_referrers` directly**, not `list_referrers_page`.
  2. `FsStorage::add_referrer` ([`src/storage/fs.rs:1747`](src/storage/fs.rs#L1747)) and `FsStorage::remove_referrer` ([`src/storage/fs.rs:1766`](src/storage/fs.rs#L1766)) during manifest mutations.
  3. `FsStorage::delete_manifest` ([`src/storage/fs.rs:1825`](src/storage/fs.rs#L1825)) via `remove_referrer`.
  4. Forwarding adapters: `SupervisorStorageWrapper::list_referrers` ([`src/supervisor.rs:1701`](src/supervisor.rs#L1701)), `LifecycleStorageWrapper::list_referrers` ([`src/manifest_lifecycle.rs:1902`](src/manifest_lifecycle.rs#L1902)).
- **Status of `list_referrers_page`:**
  - Auditing all references across `src/` and `tests/` establishes that **no production caller invokes `list_referrers_page`**.
  - Trait declarations exist on `ReferrersReader` ([`src/storage/ports/mod.rs:123`](src/storage/ports/mod.rs#L123)), implementations exist on `FsStorage` and `S3Storage`, and forwarding methods exist in `supervisor.rs:1705` and `manifest_lifecycle.rs:1904`. However, neither supervisor nor lifecycle call `list_referrers_page`.
  - All test references are mock storage definitions or live S3 harness calls (`tests/s3_live_integration.rs:1150`).
- **Operation Type:**
  - `list_referrers`: Payload read and JSON deserialization (`tokio::fs::read(&path)` -> `serde_json::from_slice::<Vec<ReferrerDescriptor>>(&bytes)`).
  - `list_referrers_page`: Delegates to `list_referrers`, performs in-memory sorting, binary searches continuation token, and slices the requested page.
- **Root Ownership:** Ambient pathname: `self.root.join("repos").join(name).join("referrers").join(format!("{}.json", subject.hex()))`. Does **not** use pinned `self.reader: Arc<FsMetadataReader>`.
- **Containment Mechanism & Fallback:** **Completely Uncontained**. Path resolution traverses the filesystem using ambient OS pathnames without directory restriction flags (`RESOLVE_BENEATH`), without symlink rejection (`RESOLVE_NO_SYMLINKS`), and without magiclink rejection.
- **Resource Limits:** None. Reads the entire JSON file into heap memory via `tokio::fs::read` without an upper byte bound or entry count ceiling.
- **Error Propagation:**
  - `list_referrers`: Missing file (`NotFound`) returns `Ok(Vec::new())`. I/O errors and JSON deserialization failures are mapped to `StorageError::io(...)`.
  - `list_referrers_page`: Invokes `.unwrap_or_default()`, silently suppressing **all** errors (permission denials, disk read errors, JSON corruptions) into an empty vector.
- **Relationship to Mutation:** `list_referrers` is called during mutations (`add_referrer`, `remove_referrer`, `delete_manifest`). `list_referrers_page` is pure observation.
- **Test Evidence & Gaps:** Basic happy-path test in `src/storage/fs/tests.rs:40-126` (`referrers_add_list_remove_and_delete_manifest`). **Missing Evidence:** No tests for symlink escapes, path traversal in repository name, corrupted JSON handling, file size bounds, duplicate digest cursors, or `list_referrers_page` pagination edge cases.

---

#### Area 2: Repository Catalog Discovery (`list_repositories`, `list_repo_names`)
- **Source Files & Symbols:**
  - `Storage::list_repositories` ([`src/storage/fs.rs:948-950`](src/storage/fs.rs#L948-L950))
  - Private helper `list_repo_names` ([`src/storage/fs.rs:741-816`](src/storage/fs.rs#L741-L816))
- **Production Callers & Entry Points:**
  1. OCI catalog route `GET /v2/_catalog` ([`src/http_api/catalog.rs:136`](src/http_api/catalog.rs#L136)).
  2. `Storage::is_storage_empty` ([`src/storage/fs.rs:975`](src/storage/fs.rs#L975)).
  3. `BlobDeleteSafety::check_blob_unreferenced_any_repo` ([`src/blob_delete_safety.rs:93`](src/blob_delete_safety.rs#L93)).
  4. `BlobRefIndex::rebuild` ([`src/blob_ref_index.rs:722`](src/blob_ref_index.rs#L722)).
  5. `MembershipMigration::plan_membership_migration` ([`src/membership_migration.rs:15`](src/membership_migration.rs#L15)).
  6. `RuntimeSupervisor::check_storage_health` ([`src/supervisor.rs:915`](src/supervisor.rs#L915)).
  7. GC policy fallback in `blob_gc::policy::build_manifest_protected_set` ([`src/blob_gc/policy.rs:355`](src/blob_gc/policy.rs#L355)).
- **Operation Type:** Iterative directory traversal via `tokio::fs::read_dir` on `repos/` with in-memory stack, followed by four ambient `tokio::fs::metadata` stat calls (`tags/`, `manifests/`, `blobs/`, `meta/`) per directory visited.
- **Root Ownership:** Ambient pathname: `self.root.join("repos")`. Does not use pinned reader.
- **Containment Mechanism & Fallback:** **Completely Uncontained**. Symlinks in directory ancestors are resolved by ambient OS pathname resolution. Intermediate directory symlinks are skipped via dirent `file_type.is_dir()`, but leaf directory recognition (`tags/`, `manifests/`, etc.) executes ambient `stat()` which follows symlinks.
- **Resource Limits:** No directory depth bound, no directory enumeration count bound, no total entry count bound, no name byte bound.
- **Error Propagation:** Missing `repos/` returns `Ok(vec![])`. Unreadable intermediate directory returns `StorageError::io`. Non-UTF-8 entries and non-directories are skipped.
- **Relationship to Mutation:** Observation-only fallback for GC validation, blob delete authorization, and index rebuild.
- **Test Evidence & Gaps:** Characterized in `src/storage/fs/tests.rs:5545-6140` under commit `12533ae`.

---

#### Area 3: Repository Timestamps and Storage Emptiness
- **Source Files & Symbols:**
  - `Storage::repo_timestamps` ([`src/storage/fs.rs:952-972`](src/storage/fs.rs#L952-L972))
  - `FsStorage::max_mtime_in_dir` ([`src/storage/fs.rs:818-844`](src/storage/fs.rs#L818-L844))
  - `Storage::is_storage_empty` ([`src/storage/fs.rs:974-995`](src/storage/fs.rs#L974-L995))
  - Private helper `fs_dir_has_any_entry` ([`src/storage/fs.rs:905-930`](src/storage/fs.rs#L905-L930))
- **Production Callers & Entry Points:**
  - `repo_timestamps`: Query handlers, proxy caching freshness evaluation.
  - `is_storage_empty`: Startup bootstrap, readiness probe, test suites.
- **Operation Type:** Directory enumeration (`tokio::fs::read_dir`) and entry metadata query (`entry.metadata().await` -> `modified()`). Emptiness check recursively probes entries across seven subdirectories (`blobs`, `uploads`, `quarantine`, `repo-blobs`, `repo-memberships`, `repos`, `journals`).
- **Root Ownership:** Ambient pathname: `self.root.join("repos").join(name)`, `self.root.join(sub)`.
- **Containment & Limits:** Uncontained ambient path operations. No entry count limits or depth bounds.
- **Error Propagation:** Missing directory returns `NotFound` or `false`. I/O failures propagate as `StorageError::io`.

---

#### Area 4: Mutation-Associated Tag File Discovery (`list_tag_files`)
- **Source Files & Symbols:**
  - `FsStorage::list_tag_files` ([`src/storage/fs.rs:716-739`](src/storage/fs.rs#L716-L739))
  - Call site: `FsStorage::delete_manifest` ([`src/storage/fs.rs:1812-1821`](src/storage/fs.rs#L1812-L1821))
- **Production Callers & Entry Points:**
  - Invoked exclusively inside `FsStorage::delete_manifest` to find and unlink tag files referencing the deleted manifest. Preserved byte-for-byte during tag-listing cutover (`f1d6d9c`).
- **Operation Type:** Directory enumeration (`tokio::fs::read_dir`) on `repos/<name>/tags` followed by point file read (`tokio::fs::read_to_string(&path)`) for every tag file found.
- **Root Ownership:** Ambient pathname: `self.root.join("repos").join(name).join("tags")`.
- **Containment & Limits:** Uncontained ambient path operations. Filters dotfiles; no entry limit or size bound.
- **Relationship to Mutation:** **Embedded in Mutation Workflow**. Not a standalone observation read.
- **Architectural Distinction:** Modifying `list_tag_files` would alter active mutation and deletion semantics under Quality Gate **O-04 (Filesystem write durability and containment)**.

---

#### Area 5: Repository-Blob Membership Reads (`RepositoryBlobMembershipStorage`)
- **Source Files & Symbols:**
  - `get_repo_blob_membership` ([`src/storage/fs.rs:2873-2898`](src/storage/fs.rs#L2873-L2898))
  - `list_repo_blob_memberships_page` ([`src/storage/fs.rs:2983-3109`](src/storage/fs.rs#L2983-L3109))
  - `list_all_repo_blob_memberships_page` ([`src/storage/fs.rs:3111-3263`](src/storage/fs.rs#L3111-L3263))
  - `count_repo_blob_memberships` ([`src/storage/fs.rs:3265-3284`](src/storage/fs.rs#L3265-L3284))
  - `is_membership_ready` ([`src/storage/fs.rs:3286-3297`](src/storage/fs.rs#L3286-L3297))
  - `get_migration_checkpoint` ([`src/storage/fs.rs:3343-3361`](src/storage/fs.rs#L3343-L3361))
- **Production Callers & Entry Points:**
  - `src/membership_migration.rs` (planning, application, verification)
  - `src/gc_service.rs` (membership-aware GC sweeps, reference checks, candidate unlinking)
- **Operation Type:** File payload reading (`tokio::fs::read`), multi-level directory enumeration (`tokio::fs::read_dir`), existence checking (`tokio::fs::metadata`).
- **Root Ownership:** Ambient pathname: `self.root.join("repo-memberships")...`, `self.root.join("meta")...`.
- **Containment & Limits:** Uncontained ambient path operations. Output page capped to `min(page_limit, 1000)` using in-memory `BinaryHeap`, but directory traversal evaluates all directory entries without enumeration or name byte bounds.
- **Error Propagation:** Missing directories return empty pages. Malformed JSON records propagate `StorageError::corrupt_data`.
- **Classification Note on `clear_membership_candidate`:**
  - [`FsStorage::clear_membership_candidate`](src/storage/fs.rs#L2942-L2970) reads a membership record, updates its state in memory, serializes it, and writes it back via `write_atomic_file`.
  - It is classified under **Area 8 (Mutation-Embedded Reads)** as a read-modify-write operation, not a standalone membership read.

---

#### Area 6: Durable Lifecycle Journal Reads (`read_lifecycle_journal`)
- **Source Files & Symbols:**
  - `Storage::read_lifecycle_journal` ([`src/storage/fs.rs:1343-1354`](src/storage/fs.rs#L1343-L1354))
- **Production Callers & Entry Points:**
  - `ManifestLifecycleManager::recover_pending_journal_under_lock` ([`src/manifest_lifecycle.rs:555`](src/manifest_lifecycle.rs#L555))
  - `ManifestLifecycleManager::check_journal_state` ([`src/manifest_lifecycle.rs:512`](src/manifest_lifecycle.rs#L512))
- **Operation Type:** Single-file payload read (`tokio::fs::read(&path)` -> `Bytes`).
- **Root Ownership:** Ambient pathname: `fs_repo_dir(&self.root, &canonical)?.join("meta").join("lifecycle_journal.json")`.
- **Containment & Limits:** Uncontained ambient read. No file size bound.
- **Error Propagation:** Missing file returns `Ok(None)`. I/O errors return `StorageError::io(...)`.
- **Relationship to Mutation:** Evaluated during crash recovery before executing pending journaled deletions.

---

#### Area 7: Upload Session and Quarantine Inspection
- **Source Files & Symbols:**
  - `quarantined_blob_version` ([`src/storage/fs.rs:3597-3612`](src/storage/fs.rs#L3597-L3612))
  - `compute_fs_blob_version` ([`src/storage/fs.rs:3437-3463`](src/storage/fs.rs#L3437-L3463))
  - `read_quarantine_timestamp` ([`src/storage/fs.rs:3402-3422`](src/storage/fs.rs#L3402-L3422))
  - `get_finalized_receipt` ([`src/storage/fs.rs:2783-2801`](src/storage/fs.rs#L2783-L2801))
  - `reap_expired_sessions` ([`src/storage/fs.rs:2803-2868`](src/storage/fs.rs#L2803-L2868))
- **Production Callers & Entry Points:**
  - `blob_gc::policy` (evaluating quarantined blobs, computing versions, conditional deletion).
  - `upload_coordinator.rs` (checking finalized receipts, session recovery).
- **Operation Type:** Metadata queries (`tokio::fs::metadata`), streaming payload reads (`compute_fs_blob_version` streams 64 KB buffers to calculate SHA-256), JSON payload reads (`tokio::fs::read`).
- **Root Ownership:** Ambient pathname: `self.root.join("quarantine")...`, `self.root.join("uploads")...`.
- **Relationship to Mutation:** Deeply embedded in mutation lifecycles (upload commits, GC quarantine, and conditional unlinking).

---

#### Area 8: Mutation-Embedded Reads
- **Source Files & Symbols:**
  - `mutate_tag` ([`src/storage/fs.rs:1101`](src/storage/fs.rs#L1101)): Synchronous `std::fs::read(&path)` under `.lock.{tag}` to read existing tag digest before overwriting.
  - `delete_tag_conditional` ([`src/storage/fs.rs:1310`](src/storage/fs.rs#L1310)): Synchronous `std::fs::read(&path)` under `.lock.{tag}` to hash existing bytes for expected version comparison before unlinking.
  - `delete_manifest` ([`src/storage/fs.rs:1788`](src/storage/fs.rs#L1788)): Asynchronous `tokio::fs::read(&manifest_path)` to extract subject digest before unlinking manifest.
  - `clear_membership_candidate` ([`src/storage/fs.rs:2942-2970`](src/storage/fs.rs#L2942-L2970)): Read-modify-write operation reading record via `tokio::fs::read` and committing modification via `write_atomic_file`.
- **Classification:** Standalone read containment slices must **not** modify mutation paths. Write containment, locking, and atomic deletion are governed by Quality Gate **O-04**.

---

## 5. Detailed Caller Trace & Consequence Analysis

### 5.1 OCI Referrers Trace: Direct Reads vs. Paged Error Suppression

To ensure strict precision, the direct read call chain must be clearly separated from the paged read method:

```
[Public OCI HTTP Request: GET /v2/<name>/referrers/<digest>]
      │
      ▼
src/http_api/referrers.rs:52 (ReferrersQueryService::query_referrers)
      │
      ▼
src/application/referrers.rs:51 (reader.list_referrers(repo, subject))
      │
      ├── StorageError::NotFound ────────► Returns empty page Ok(vec![]) -> HTTP 200 {"manifests":[]}
      ├── StorageError::Io / Corrupt ────► Propagates Err(ReferrersQueryError::Storage)
      │                                     └── HTTP API maps to HTTP 500 (errors::internal_error)
      └── StorageError::InvalidRepoName ─► Returns HTTP 400 (NAME_INVALID)
```

#### Established Facts on Direct vs. Paged Referrers:
1. **Public Route Reachability:**
   - The public HTTP route handler [`referrers_get_or_head`](src/http_api/referrers.rs#L30-L75) invokes `ReferrersQueryService::query_referrers`.
   - `ReferrersQueryService` invokes `reader.list_referrers(repo, subject).await`. It does **not** call `list_referrers_page`.
   - In `ReferrersQueryService`, any non-`NotFound` storage error from `list_referrers` is propagated as `ReferrersQueryError::Storage(e)`, which the HTTP route handler maps to HTTP 500 (`errors::internal_error()`).
   - Therefore, the public OCI route does **not** swallow errors into empty results; it fails closed with HTTP 500 on I/O or JSON corruption errors.
2. **Status of `list_referrers_page` in Production:**
   - `list_referrers_page` ([`src/storage/fs.rs:1232-1261`](src/storage/fs.rs#L1232-L1261)) delegates to `self.list_referrers(repo, subject).await.unwrap_or_default()`, suppressing errors into an empty vector.
   - However, exhaustive codebase inspection demonstrates that **no production caller invokes `list_referrers_page`**.
   - Forwarding implementations in `supervisor.rs:1705` and `manifest_lifecycle.rs:1904` exist only to satisfy trait requirements; neither supervisor nor lifecycle call them.
   - All references to `list_referrers_page` are in test mocks or live S3 test harnesses.
   - Consequently, while `list_referrers_page` contains a severe error-suppression flaw, this flaw does **not** currently affect the public route. It exists as a latent defect and compatibility liability on the storage port.
3. **Risks in the Direct Read Path (`list_referrers`):**
   - Uses ambient OS path resolution without descriptor pinning.
   - Constructs path via `referrers_path(name, subject)` without validating `name` against directory traversal (`..`) or control characters.
   - Loads the entire referrers JSON payload into memory via `tokio::fs::read` without a size ceiling.
   - Maps JSON deserialization failures to `StorageError::io` rather than `StorageError::corrupt_data`.
4. **Manifest Deletion Cleanup (`delete_manifest`):**
   - In `delete_manifest` ([`src/storage/fs.rs:1825`](src/storage/fs.rs#L1825)), cleanup of referrers uses `let _ = self.remove_referrer(...)`.
   - If `remove_referrer` fails due to unreadable or corrupt JSON, the error is ignored, leaving stale referrers. However, this does not cause premature physical deletion of blobs or manifests.

---

### 5.2 Repository Catalog Trace: `list_repositories` / `list_repo_names`

```
[Storage::list_repositories]
      │
      ├── GET /v2/_catalog (src/http_api/catalog.rs:136)
      │     └── Error: Returns HTTP 500 (errors::internal_error). Read-only; 0 mutations.
      │
      ├── BlobDeleteSafety::check_blob_unreferenced_any_repo (src/blob_delete_safety.rs:93)
      │     └── Error: Propagates Err. Caller aborts blob deletion. Fails closed (safe).
      │     └── Omission: If repo omitted from discovery, its tags are NOT checked!
      │           Creates vulnerability to premature blob deletion if other checks pass.
      │
      ├── BlobRefIndex::rebuild (src/blob_ref_index.rs:722)
      │     └── Error: Aborts rebuild loop. Leaves index in Building state and partial trees.
      │
      ├── MembershipMigration::plan_membership_migration (src/membership_migration.rs:15)
      │     └── Error: Propagates Err. Dry-run aborts immediately. 0 mutations.
      │
      └── RuntimeSupervisor::check_storage_health (src/supervisor.rs:915)
            └── Error: Supervisor records storage probe failure, retries on next tick.
```

#### Detailed Consequence Tracing:
1. **Omitted Discovery Result:**
   - `list_repo_names` considers a directory a repository only if it contains a `tags/`, `manifests/`, `blobs/`, or `meta/` subdirectory.
   - If a directory contains only `referrers/`, it is omitted.
   - If an ancestor path contains a non-UTF-8 directory name, the entire subtree is omitted.
   - In `BlobDeleteSafety`: If a repository is omitted from catalog discovery, `check_blob_unreferenced_any_repo` skips scanning that repository's tags. However, blob deletion still requires absence of references in `BlobRefIndex` and valid mutation permits. Omission increases vulnerability to inconsistency, but does not autonomously execute deletion.
2. **Intermediate Directory Resolution Failure:**
   - If an intermediate directory cannot be read (`EACCES`), `list_repo_names` aborts and returns `StorageError::io`.
   - `blob_delete_safety` aborts, preventing blob deletion.
   - Catalog route returns HTTP 500.
3. **Symlink Traversal Risk:**
   - `tokio::fs::metadata(&tags_dir)` follows symlinks. A symlinked `tags` leaf pointing anywhere outside the storage root is recognized as a valid repository, allowing external directory trees to be indexed or reported in `_catalog`.

---

## 6. Evidence-Backed Priority Ranking of Remaining Gaps

Remaining read-containment gaps are ranked using six architectural criteria:
1. **Demonstrated Production Reachability:** Exposure via public network endpoints or active production background loops.
2. **Consequence of Incomplete / Misleading Results:** Blast radius of errors, silent swallowing, or corrupt data.
3. **Containment & Resource Exposure:** Exposure to path traversal, symlink escapes, or unbounded memory/IO consumption.
4. **Compatibility Uncertainty:** Semantic divergence, unapproved breaking changes, or missing contracts.
5. **Available Reusable Primitives:** Availability of tested `storage-fs` components (`openat2`, `FsMetadataReader`).
6. **Scope & Verification Cost:** Compactness of changes and feasibility of verification without broad regression risk.

### 6.1 Priority Ranking Matrix

| Rank | Functional Area | Candidate Symbols | Reachability | Consequence | Containment Exposure | Compatibility Uncertainty | Available Primitives | Scope & Cost | Overall Recommendation |
|:---:|---|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **1** | **OCI Referrers Point Reads & Listing** | `list_referrers`, `list_referrers_page` | **High** for direct read (`GET /v2/...`); **None** for paged read | **Medium** (Direct read propagates 500; paged read swallows errors) | **High** (Unbounded JSON, ambient read, unvalidated repo name) | **Low** (Deterministic JSON document) | **High** (`open_payload` / `get_metadata`) | **Low** (Single document, compact module) | **RECOMMENDED NEXT SLICE** |
| **2** | **Repository Catalog Discovery** | `list_repositories`, `list_repo_names` | **High** (Public OCI API & Safety) | **High** (Safety check input) | **High** (Unbounded tree walk, stat symlinks) | **Very High** (GC vs Catalog semantic divergence) | **Medium** (`repo_discovery` exists for GC) | **High** (Complex recursive tree walk) | Next candidate after Referrers |
| **3** | **Repository Timestamps & Emptiness** | `repo_timestamps`, `is_storage_empty` | **Medium** (Readiness & Cache) | **Low** (Informational) | **Medium** (Ambient dir walk) | **Low** (Standard mtime) | **Medium** (Contained dir listing) | **Medium** (Small helper functions) | Subordinate to Catalog |
| **4** | **Repository-Blob Membership Reads** | `get_repo_blob_membership`, listing | **Medium** (Migration & Sweep) | **Medium** (Migration halts) | **High** (3-level dir walk) | **Medium** (Sled vs FS duality) | **Medium** (Directory enumeration) | **High** (6 standalone methods) | Defer to Membership phase |
| **5** | **Durable Lifecycle Journal Reads** | `read_lifecycle_journal` | **Low** (Crash recovery only) | **Medium** (Recovery gating) | **Low** (Single JSON file) | **Low** (Single document) | **High** (`open_payload`) | **Low** (1 method) | Package with Lifecycle write audit |
| **6** | **Quarantine & Upload Inspection** | `quarantined_blob_version`, timestamps | **Medium** (GC Sweep & Uploads) | **Medium** (Conditional unlinking) | **Medium** (Ambient paths) | **Medium** (GC state machine) | **Medium** (Hash & stat) | **High** (Interleaved with mutations) | Defer to O-04 write containment |
| **7** | **Mutation-Embedded Reads** | `list_tag_files`, `clear_membership_candidate`, `mutate_tag` | **High** (Mutations & Cleanup) | **Critical** (Direct deletion/mutation) | **High** (Ambient write/unlink) | **High** (Durability & locking) | **Low** (Mutation primitives needed) | **High** (Governed by O-04) | **Strictly Excluded from Read Slices** |

---

## 7. Recommended Next Bounded Slice: OCI Referrers Read Characterization

We recommend that the single immediate next bounded slice be:
**OCI Referrers Read Characterization (`list_referrers` & `list_referrers_page`)**.

This slice must be **characterization and documentation only**, freezing and verifying existing production behavior in controlled test fixtures before designing containment primitives or proposing cutover.

### 7.1 Concrete Objective and Scope
- **Objective:** Empirically characterize and freeze existing production behavior of `FsStorage::list_referrers` and `FsStorage::list_referrers_page` across all valid, missing, corrupted, pagination, and edge-case inputs.
- **Scope:**
  - Create dedicated characterization test suite in `src/storage/fs/tests.rs` (or dedicated sub-module).
  - Test missing repository and missing referrers file behavior.
  - Test valid single and multiple `ReferrerDescriptor` records with and without optional fields (`artifact_type`, `annotations`).
  - Test corrupted, truncated, and malformed JSON payloads.
  - Test invalid UTF-8 byte sequences.
  - Test ambient symlink behavior and path traversal handling.
  - Characterize full pagination mechanics in `list_referrers_page` (sorting, cursors, duplicates, zero-limit, large limits, arithmetic overflow).
  - Characterize and document the error-swallowing behavior of `list_referrers_page` (`unwrap_or_default()`).
  - Characterize caller interaction with `ReferrersQueryService::query_referrers` and `delete_manifest`.

### 7.2 Expected Files and Symbols
- **Test File:** [`src/storage/fs/tests.rs`](src/storage/fs/tests.rs) (new characterization tests).
- **Target Symbols Under Characterization:**
  - `FsStorage::list_referrers` ([`src/storage/fs.rs:1722-1735`](src/storage/fs.rs#L1722-L1735))
  - `FsStorage::list_referrers_page` ([`src/storage/fs.rs:1232-1261`](src/storage/fs.rs#L1232-L1261))
  - `FsStorage::referrers_path` ([`src/storage/fs.rs:671-678`](src/storage/fs.rs#L671-L678))
  - `ReferrerDescriptor` ([`src/storage/mod.rs`](src/storage/mod.rs))

### 7.3 Explicit Exclusions
- **No Production Code Modifications:** `src/storage/fs.rs`, `src/application/referrers.rs`, and `src/http_api/referrers.rs` remain completely untouched.
- **No Referrer Mutation Changes:** `add_referrer`, `remove_referrer`, and `delete_manifest` remain untouched.
- **No Contained Reader Routing:** Do not route referrers through `self.reader` during characterization.
- **No Premature Limit Enforcement:** Do not enforce new arbitrary file size limits or entry limits during characterization.
- **No Lockfile, Dependency, or Configuration Changes:** Strictly zero changes to `Cargo.toml`, `Cargo.lock`, or `Config`.

### 7.4 Detailed Proposed Pagination Characterization Plan
Characterization tests must systematically observe and freeze current `list_referrers_page` semantics:
1. **Sorting and Page Boundaries:**
   - Verify that descriptors are sorted lexicographically by `digest` string.
   - Verify page slicing for `page_limit` smaller than total count, returning first `page_limit` descriptors and `next_token` equal to the last descriptor's digest.
2. **Continuation Token Lookup:**
   - Existing token: Token matching an existing digest starts at `idx + 1`.
   - Absent token: Token not matching an existing digest uses binary search insertion point `idx`.
3. **Duplicate Descriptor Digests:**
   - When multiple descriptors have identical `digest` strings, `binary_search_by` returns an arbitrary match, and `idx + 1` can split or skip duplicates across page boundaries. Characterize this behavior.
4. **Zero-Sized Pages (`page_limit == 0`):**
   - Verify that `list_referrers` is still called (reading and parsing the entire JSON payload from disk) before producing an empty slice `Ok((vec![], None))`.
5. **Terminal Pages:**
   - Verify that when `end_idx == refs.len()`, `next_token` is `None`.
6. **Large `page_limit` and Unchecked Addition Arithmetic:**
   - Current code executes `let end_idx = (start_idx + page_limit).min(refs.len());`.
   - When `start_idx == 0`, `0 + usize::MAX` does not overflow.
   - However, when `start_idx > 0` (e.g. following a continuation token to position 1) and `page_limit == usize::MAX`, `start_idx + page_limit` is subject to integer overflow.
   - Under debug builds or overflow-checking profiles, this causes a panic; under unchecked release profiles, it wraps around.
   - The characterization test plan specifies a deterministic fixture with `start_idx > 0` to observe and record this exact arithmetic boundary.

### 7.5 Unresolved Compatibility Decisions (Requiring Future Design / User Approval)
The characterization slice will surface and record the following architectural decisions without pre-approving them:
1. **Operational Payload Size Limit:** Determine whether to introduce an upper byte ceiling for referrers JSON files, and if so, what value should be proposed.
2. **Operational Entry Count Limit:** Determine whether to introduce an upper ceiling on the number of descriptors parsed per subject.
3. **Error Taxonomy for Corrupted Payloads:** Determine whether JSON parsing failures should map to `StorageErrorKind::CorruptData` rather than legacy `StorageErrorKind::Io`.
4. **Correction of Error Swallowing:** Determine whether `list_referrers_page` should stop swallowing errors via `unwrap_or_default()` and propagate errors fail-closed to callers.
5. **Path Validation Contract:**
   - Current code uses raw string interpolation in `referrers_path`.
   - Future design must choose between:
     * **Structural Path Validation (`validate_path_component`):** Rejects empty names, leading/trailing slashes, backslashes, NUL/control bytes, repeated slashes, and `.` or `..` segments, while permitting uppercase and broader character sets.
     * **Strict Canonical Repository Grammar (`CanonicalRepoName::parse`):** Enforces lowercase alphanumeric OCI Distribution Spec regex format (`[a-z0-9]+...`).
     * **Other Explicit Compatibility Policy:** Retaining permissive legacy path behavior with internal sanitization.
   - This decision must be explicitly framed for user approval before implementation.

### 7.6 What Completion Would and Would Not Establish
- **Would Establish:**
  - An empirical, regression-locked baseline of current production referrers behavior under `FsStorage`.
  - Precise documentation of error mapping and pagination liabilities.
  - Concrete foundation for subsequent contained test-seam design.
- **Would NOT Establish:**
  - Contained descriptor-relative resolution for referrers.
  - Enforcement of resource bounds or repository path validation.
  - Resolution of Quality Gate **O-05**.
  - Production code modifications or cutover authorization.

---

## 8. Canonical Quality Gate Status

All eight canonical quality gates remain explicitly **OPEN**:

| Gate | Canonical Gate Title | Status | Scope & Current Condition |
|:---:|---|:---:|---|
| **O-03** | Key and continuation-token contracts | **OPEN** | Contained tag listing enforces pagination and token ordering; referrers pagination has no active production caller and unchecked arithmetic; catalog pagination contracts remain open. |
| **O-04** | Filesystem write durability and containment | **OPEN** | Focus has remained exclusively on read containment. Write durability, atomic rename containment, and directory locking remain unaddressed. |
| **O-05** | Broader filesystem read containment | **OPEN** | CAS blobs, manifests, GC discovery, tag reads, and tag listing are contained; referrers, catalog discovery, membership, and quarantine reads remain uncontained ambient path operations. |
| **O-06** | Typed AWS mapping and pinned-MinIO evidence | **OPEN** | Standalone MinIO CI harness evidence and typed AWS error mapping remain pending. |
| **O-13** | Hosting, distribution, and release strategy | **OPEN** | Permanent crate publishing, hosting, and downstream dependency release strategy for `storage-layer-rust` remain open. |
| **O-15** | Non-Linux verification | **OPEN** | Non-Linux platform fallback behavior and cross-platform CI verification remain unverified. |
| **O-16** | Earlier Slice 11 audit/test-inventory evidence | **OPEN** | Audit evidence and test-inventory accounting for Slice 11 remain open. |
| **D-06** | Broader extraction, cutover, compatibility, and distribution acceptance | **OPEN** | End-to-end downstream distribution validation and overarching architectural acceptance remain open. |

---

## 9. Summary Conclusion

Production filesystem reads in `registry-rust` have been reassessed against authoritative source following tag-listing production cutover (`f1d6d9c`). Tag point reads and tag listings now operate under descriptor containment. However, multiple uncontained read paths remain across OCI referrers, repository catalog discovery, repository-blob membership, lifecycle journals, and quarantine inspection. Quality Gate **O-05** remains explicitly **OPEN**.

The single recommended next bounded slice is **OCI Referrers Read Characterization (`list_referrers` & `list_referrers_page`)**, freezing existing production behavior in controlled test fixtures without changing production routing or prematurely enforcing unapproved containment policies.
