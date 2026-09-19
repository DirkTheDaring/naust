> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Landed as `02cfa07`; the self-corrected implementation record is supervisor-tag-listing-error-hardening.md.
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Supervisor Tag-Listing Failure Policy Assessment (Finalized Record)

- **Document:** `docs/architecture/supervisor-tag-listing-failure-policy-assessment.md`
- **Target Repository:** `registry-rust` (HEAD: `5779f7f97f1109bcbb15bb6ad47e845bd4686bbf`)
- **Dependency Repository:** `storage-layer-rust` (HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`, strictly read-only)
- **Status:** **ARCHITECTURAL ASSESSMENT & POLICY SPECIFICATION ONLY — PENDING APPROVAL — PRODUCTION UNCHANGED — NOT COMMITTED**
- **Canonical Quality Gates:** All eight quality gates remain explicitly **OPEN** (`O-03, O-04, O-05, O-06, O-13, O-15, O-16, D-06`).

---

## 1. Executive Summary & Purpose

This architectural assessment investigates supervisor-driven tag listing failure handling, candidate retention, and cleanup policies across `registry-rust`.

The repository is preparing for the production promotion of descriptor-relative contained filesystem tag listing (`contained_list_tags_seam` and `contained_list_tags_page_seam`), which replaces uncontained, ambient filesystem traversals with Linux `openat2` containment beneath a pinned directory descriptor (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS`).

### 1.1 Context & Preceding Hardening Slices
Caller-side error handling was previously hardened in two subsystems before storage cutover:
1. **Lifecycle Hardening (Commit `96c0729bcd02c5f44fbfa13c5b0d4ea77bff6a86`):**
   Hardened three call sites in [`src/manifest_lifecycle.rs`](file:///home/dietmar/devel/rust/registry-rust/src/manifest_lifecycle.rs) (`DeleteManifest` recovery, `ProxyEvict` recovery, and active proxy eviction). Wildcard error suppression (`Err(_) => (Vec::new(), None)`) was replaced: `StorageError::NotFound` returns an empty page, while any other error propagates immediately via `ManifestLifecycleError::Storage(e)`, preventing destructive manifest deletion or membership unlinking.
2. **Membership-Migration Hardening (Commit `5779f7f97f1109bcbb15bb6ad47e845bd4686bbf`):**
   Hardened planning, application, and verification in [`src/membership_migration.rs`](file:///home/dietmar/devel/rust/registry-rust/src/membership_migration.rs). Wildcard error suppression (`unwrap_or_default()`) was replaced:
   - In planning (`plan_membership_migration`), `StorageError::NotFound` returns empty tags, and non-`NotFound` errors return `Err(e)` with zero checkpoint writes.
   - In application (`apply_membership_migration`), `StorageError::NotFound` returns empty tags; non-`NotFound` errors set `checkpoint.phase = MigrationPhase::Failed`, preserve continuation cursors, record bounded UTF-8 diagnostics, await checkpoint persistence (handling secondary save errors), and return `Err(e)`.
   - In verification (`verify_membership_migration`, `src/membership_migration.rs:243`), `StorageError::NotFound` returns empty tags; non-`NotFound` errors return `Err(e)`. When called from application after the Verifying checkpoint save, verification failure leaves that checkpoint in place. Direct verification does not itself create or transition a checkpoint.

### 1.2 Core Finding: The Supervisor Gap
Production filesystem tag-listing routing currently remains legacy (`FsStorage::list_tags` and `FsStorage::list_tags_page`).
Crucially, **neither the lifecycle nor migration hardening decisions established supervisor or background garbage collection policy**.

Inspection of [`src/supervisor.rs`](file:///home/dietmar/devel/rust/registry-rust/src/supervisor.rs) and supervisor-invoked maintenance loops reveals critical architectural boundaries:
1. **Separation of Name-Only Listing from Paginated Listing:**
   Supervisor callers (`compute_protected_blobs` and `find_repo_blob_reference`) invoke name-only `storage.list_tags(&repo).await`, **not** paginated `list_tags_page`. Name-only listing inspects directory entries without opening tag payloads. A corrupted or unreadable tag payload does **not** cause name-only listing to fail. Subsequent tag resolution (`storage.resolve_tag`) and manifest reading occur on distinct branches with their own separate error handling.
2. **`proxy_gc_once` Executes Zero Storage Mutations:**
   In [`src/supervisor.rs:823-907`](file:///home/dietmar/devel/rust/registry-rust/src/supervisor.rs#L823-L907), `proxy_gc_once` scans candidate blob files in `fs_root`, filters out those present in `protected_blobs`, tallies a local `evicted_records` counter, and logs an informational message. It executes **zero storage mutations, zero file deletions, and zero CAS unlinks**. Physical CAS reclamation is managed exclusively by `BlobGcService`. However, in `compute_protected_blobs`, `if let Ok(tags) = storage.list_tags(&repo).await` silently swallows listing errors, and `if let Ok(digest) = storage.resolve_tag(&repo, &tag).await` separately swallows resolution errors, resulting in incomplete `protected_blobs` calculations.
3. **Dual Membership Reference-Check Sites in GC Sweep:**
   In [`src/gc_service.rs:590-680`](file:///home/dietmar/devel/rust/registry-rust/src/gc_service.rs#L590-L680) (`sweep_repository_memberships_with_guard`), `find_repo_blob_reference` is invoked at two distinct sites:
   - *Site 1 (Initial Reference Check, line 611):* Any error returned by `find_repo_blob_reference` propagates through `?`, **aborting the entire membership sweep immediately**. Mutations committed on earlier records ($1 \dots k-1$) remain in storage, but downstream CAS deletion (`blob_gc_delete`) is bypassed for this scheduled tick.
   - *Site 2 (Pre-Unlink Revalidation, line 654):* Handled via `Err(_) => true`. An error during revalidation treats the blob as still referenced, skips `ledger.unlink`, increments `stats.skipped`, and **continues sweeping subsequent records**.
4. **Index Mutation Boundaries:**
   In `sync_repo_manifests_and_tags`: Discovery errors prevent application-phase index writes within that sync call. The subsequent application phase is not established as transactional or atomic. Furthermore, `rebuild` clears `tag_to_root`, `root_counts`, `rev_edges`, and `repo_memberships` while retaining metadata, including Building state. Discovery failure on a subsequent repository leaves those trees cleared, partially populated by earlier repositories, and the index unready.

---

## 2. Trace of Actual Production Callers

Starting with [`src/supervisor.rs`](file:///home/dietmar/devel/rust/registry-rust/src/supervisor.rs), direct and indirect invocations of `list_tags` and `list_tags_page` were traced across supervisor-spawned maintenance tasks, startup validation, and background services.

```
run_server_supervisor (src/supervisor.rs:309)
├─> build_server_runtime (src/runtime.rs:265)
│     └─> idx.ensure_healthy_or_rebuild [PATH 3: STARTUP REBUILD]
├─> spawn_proxy_gc (src/supervisor.rs:527)
│     └─> proxy_gc_once -> compute_protected_blobs [PATH 1: PROXY GC]
└─> spawn_blob_gc_scheduler (src/supervisor.rs:1300)
      └─> scheduled_cleanup_once (src/gc_service.rs:693)
            ├─> ensure_ref_index_ready -> rebuild [PATH 3: GC REBUILD]
            └─> sweep_repository_memberships_with_guard [PATH 2: MEMBERSHIP SWEEP]
                  └─> find_repo_blob_reference (src/blob_delete_safety.rs:127)
```

### 2.1 Path 1: `proxy_gc` (Direct Supervisor Worker)

- **Source Locations:**
  - Invocation Loop: [`src/supervisor.rs:527-621`](file:///home/dietmar/devel/rust/registry-rust/src/supervisor.rs#L527-L621) (`spawn_proxy_gc`)
  - Once Execution: [`src/supervisor.rs:823-907`](file:///home/dietmar/devel/rust/registry-rust/src/supervisor.rs#L823-L907) (`proxy_gc_once`)
  - Tag Listing Call Site: [`src/supervisor.rs:909-968`](file:///home/dietmar/devel/rust/registry-rust/src/supervisor.rs#L909-L968) (`compute_protected_blobs`, line 940)
  - Helper Functions: [`pick_latest_semver_tag`](file:///home/dietmar/devel/rust/registry-rust/src/supervisor.rs#L1012-L1038) (lines 1012–1038), [`collect_protected_blobs_for_manifest`](file:///home/dietmar/devel/rust/registry-rust/src/supervisor.rs#L970-L1010) (lines 970–1010).
- **Scheduling & Entry Point:**
  Spawned inside `run_server_supervisor` at line 367 via `spawn_proxy_gc(&supervisor, state.clone()).await;`.
  Requires `state.config.proxy.enabled == true`.
  Uses `supervisor.spawn_loop` under `TaskClassification::MaintenanceScheduler` with interval `Duration::from_secs(state.config.proxy.gc_interval_secs.max(1))`.
  Tasks are named `"proxy_gc"` or `format!("proxy_gc_upstream_{i}")`.
- **Storage Instance & Backend:**
  Operates on the proxy cache storage instance: `ctx.cache_storage` or `state.proxy_cache: Option<Arc<dyn ProxyStoragePort>>`.
  Currently restricted exclusively to `StorageBackend::Filesystem` (`src/supervisor.rs:533, 576`).
- **Repository Discovery & Tag Listing Call:**
  - Repositories are enumerated via `storage.list_repositories().await?`.
  - For each repo matching a `ProxyRepoRule` with `EvictionPolicy::KeepLatestCachedSemver { tag_regex, allow_prerelease }`:
    Calls `storage.list_tags(&repo).await` at line 940.
  - **This call is name-only `list_tags`, returning `Vec<String>`. No tag payloads are opened or parsed during this call.**
- **Subsequent Resolution & Manifest Traversal:**
  - `pick_latest_semver_tag` inspects tag names, parses SemVer, and returns the highest SemVer tag string.
  - Line 951: `if let Ok(digest) = storage.resolve_tag(&repo, &tag).await` resolves the tag to its root manifest digest. **Failures during tag resolution are separately suppressed.**
  - If `resolve_tag` succeeds, `collect_protected_blobs_for_manifest` recursively reads manifests (`storage.get_manifest`) and parses references to populate `protected_blobs: HashSet<String>`. Manifest read or parse errors propagate via `?`.
- **Error Branches in Listing:**
  Line 940:
  ```rust
  if let Ok(tags) = storage.list_tags(&repo).await
      && let Some(latest) =
          pick_latest_semver_tag(tags, tag_regex.as_deref(), *allow_prerelease)
  {
      pinned_tags.push(latest);
  }
  ```
  On `StorageError::NotFound` or any `StorageError::Internal` (e.g. `StorageErrorKind::Io`, `StorageErrorKind::Backend`), the error is silently discarded. `pinned_tags` is not populated for that repository.
- **Actual Behavior of `proxy_gc_once`:**
  Lines 888–906:
  ```rust
  if total <= max_cache_bytes {
      return Ok(());
  }

  entries.sort_by_key(|(_, _, last, mtime)| (*last, *mtime));

  let mut evicted_records: u64 = 0;
  for (_digest, _size, _, _) in entries {
      evicted_records += 1;
  }

  if evicted_records > 0 {
      tracing::info!(
          evicted_records,
          max_cache_bytes,
          "proxy eviction: evicted old proxy cache metadata; physical CAS reclamation managed exclusively by BlobGcService"
      );
  }
  Ok(())
  ```
  `proxy_gc_once` performs **no file unlinking, no cache directory deletion, and no database writes**. It computes non-protected blobs against `max_cache_bytes`, tallies `evicted_records`, and logs.
  Therefore, incomplete protection calculations result in inaccurate counter logging and degraded protection data, but do **not** trigger immediate persistent cache deletion in current source.
- **Coordination & Outer Propagation:**
  No cluster lock or lease is held. If an error is returned by `compute_protected_blobs`, `spawn_loop` logs `proxy gc failed: {e}` and retries on the next interval. Because listing and resolution errors are swallowed, `compute_protected_blobs` returns `Ok(protected_blobs)` despite failures.

---

### 2.2 Path 2: `blob_gc_scheduler` (Scheduled Background Blob GC & Membership Sweep)

- **Source Locations:**
  - Scheduler Loop: [`src/supervisor.rs:1300-1370`](file:///home/dietmar/devel/rust/registry-rust/src/supervisor.rs#L1300-L1370) (`spawn_blob_gc_scheduler`)
  - Once Execution: [`src/gc_service.rs:693-800`](file:///home/dietmar/devel/rust/registry-rust/src/gc_service.rs#L693-L800) (`scheduled_cleanup_once`)
  - Membership Sweep: [`src/gc_service.rs:590-680`](file:///home/dietmar/devel/rust/registry-rust/src/gc_service.rs#L590-L680) (`sweep_repository_memberships_with_guard`)
  - Reference Safety Check: [`src/blob_delete_safety.rs:127-158`](file:///home/dietmar/devel/rust/registry-rust/src/blob_delete_safety.rs#L127-L158) (`find_repo_blob_reference`)
- **Scheduling & Locks:**
  Spawned at line 366 of `src/supervisor.rs`. Requires `config.blob_gc_schedule_enabled == true`.
  Guarded by `self.run_lock.try_lock()` (in-process concurrency control), `self.try_acquire_fs_gc_lock()` (`quarantine/gc.lock` on filesystem backend), and validates active `mutation_authority`.
- **Membership Sweep Structure:**
  - `sweep_repository_memberships_with_guard` paginates memberships via `self.storage.list_all_repo_blob_memberships_page(continuation, 256)`.
  - For each `RepoBlobMembershipRecord` (`rec`), it queries `find_repo_blob_reference(self.storage.as_ref(), rec.repo.as_str(), &rec.digest)`.
- **Internal Stages of `find_repo_blob_reference`:**
  In [`src/blob_delete_safety.rs:132-158`](file:///home/dietmar/devel/rust/registry-rust/src/blob_delete_safety.rs#L132-L158):
  1. *Tag Listing (Name-Only):*
     ```rust
     let tags = match storage.list_tags(repo).await {
         Ok(t) => t,
         Err(StorageError::NotFound) => return Ok(None),
         Err(e) => return Err(e),
     };
     ```
     `StorageError::NotFound` returns `Ok(None)` (0 references). Any other error returns `Err(e)`.
  2. *Tag Resolution (Payload Inspection):*
     ```rust
     for tag in tags {
         let root = match storage.resolve_tag(repo, &tag).await {
             Ok(d) => d,
             Err(StorageError::NotFound) => continue,
             Err(e) => return Err(e),
         };
         roots.entry(root).or_insert(tag);
     }
     ```
     `StorageError::NotFound` (e.g. concurrent deletion) is skipped via `continue`. **Non-NotFound errors during resolution propagate immediately as `Err(e)`.**
  3. *Manifest Traversal:*
     `scan_repo_for_blob` fetches and parses manifests for each root, propagating errors via `?`.
  Therefore, errors from `find_repo_blob_reference` can originate from listing, tag resolution, or manifest reading.
- **The Two Distinct Reference-Check Sites in `sweep_repository_memberships_with_guard`:**
  - **Site 1: Initial Reference Check ([`src/gc_service.rs:606-612`](file:///home/dietmar/devel/rust/registry-rust/src/gc_service.rs#L606-L612)):**
    ```rust
    let is_referenced = crate::blob_delete_safety::find_repo_blob_reference(
        self.storage.as_ref(),
        rec.repo.as_str(),
        &rec.digest,
    )
    .await?
    .is_some();
    ```
    If `find_repo_blob_reference` returns `Err(e)`, the `?` operator **immediately aborts the sweep**.
    - Mutations performed on earlier records ($1 \dots k-1$) via `RepositoryMembershipLedger` remain in storage.
    - Subsequent records are not evaluated.
    - `scheduled_cleanup_once` halts before reaching Phase 2 (`blob_gc_delete`), so no blobs are deleted from CAS during this run.
  - **Site 2: Pre-Unlink Revalidation ([`src/gc_service.rs:645-655`](file:///home/dietmar/devel/rust/registry-rust/src/gc_service.rs#L645-L655)):**
    ```rust
    let still_referenced =
        match crate::blob_delete_safety::find_repo_blob_reference(
            self.storage.as_ref(),
            rec.repo.as_str(),
            &rec.digest,
        )
        .await
        {
            Ok(r) => r.is_some(),
            Err(_) => true,
        };

    if !still_referenced {
        if ledger.unlink(rec.repo.as_str(), &rec.digest).await? {
            stats.unlinked += 1;
        }
    }
    ```
    If `find_repo_blob_reference` errors during revalidation, it evaluates to `true` (still referenced).
    - `ledger.unlink` is **not** called.
    - The candidate record is preserved.
    - The loop increments `stats.skipped` and **continues to subsequent records**.
- **`RepositoryMembershipLedger` Delegation and Reverse-Index Updates:**
  Authoritative membership state is persisted directly in `self.storage` (`RepositoryBlobMembershipStorage`), **not** in Sled. When the ledger runs in `LedgerIndexMode::Indexed`, the secondary reverse index (`BlobRefIndex`, Sled) is updated at three points in [`src/repository_membership_ledger.rs`](file:///home/dietmar/devel/rust/registry-rust/src/repository_membership_ledger.rs):
  - `link_with_guard` (lines 103–127): `mark_dirty`, storage `link_repo_blob`, then `record_membership`, `flush`, `mark_ready`.
  - `unlink_with_guard` (lines 136–163): `mark_dirty`, storage `unlink_repo_blob`, then `remove_membership` (only if the storage marker was removed), `flush`, `mark_ready`.
  - `reactivate_with_guard` (lines 200–215): storage `clear_membership_candidate`; when it reports a state change, `idx.record_membership(digest, repo)` is called with its result discarded (`let _ =`), without `mark_dirty`/`flush`/`mark_ready`.
  `set_candidate_with_guard` (lines 178–191) mutates storage only; per its source comment, candidates remain in the reverse index so GC will not collect them prematurely.

---

### 2.3 Path 3: Ref-Index Rebuild & Sync (`BlobRefIndex`)

- **Source Locations:**
  - Startup Invocation: [`src/runtime.rs:411-430`](file:///home/dietmar/devel/rust/registry-rust/src/runtime.rs#L411-L430)
  - Preflight Invocation: [`src/gc_service.rs:715`](file:///home/dietmar/devel/rust/registry-rust/src/gc_service.rs#L715) (`ensure_ref_index_ready`)
  - Rebuild Loop: [`src/blob_ref_index.rs:708-746`](file:///home/dietmar/devel/rust/registry-rust/src/blob_ref_index.rs#L708-L746) (`rebuild`)
  - Sync Function: [`src/blob_ref_index.rs:493-531`](file:///home/dietmar/devel/rust/registry-rust/src/blob_ref_index.rs#L493-L531) (`sync_repo_manifests_and_tags`)
  - Paginated Call Site: [`src/blob_ref_index.rs:628-656`](file:///home/dietmar/devel/rust/registry-rust/src/blob_ref_index.rs#L628-L656) (`discover_repo_manifests_and_tags`, line 634)
- **Staging vs Global Rebuild Boundaries:**
  - In `sync_repo_manifests_and_tags`: Discovery errors prevent application-phase index writes within that sync call. The subsequent application phase is not established as transactional or atomic. Zero index writes occur if discovery fails.
  - However, `rebuild` is **not globally mutation-free**:
    1. Sets `META_STATE = META_STATE_BUILDING` and flushes.
    2. `rebuild` clears `tag_to_root`, `root_counts`, `rev_edges`, and `repo_memberships` while retaining metadata, including Building state.
    3. Loops through repositories. If repo 1 succeeds, its manifests and tags are written to Sled. If repo 2 encounters an error during `list_tags_page`, `rebuild` aborts.
    4. The four cleared trees are left with partial entries from repo 1, and `meta` still holds `META_STATE_BUILDING`.
    5. `check_health()` fails closed with `RefIndexError::Corrupt("index not ready (previous rebuild incomplete?)")`.
    6. Server startup or GC execution aborts fail-closed before any client requests or GC deletions can proceed.

---

### 2.4 Path 4: Tag-Rooted GC Refresh (`refresh_tag_rooted_conservative`)

- **Source Locations:** [`src/gc_service.rs:172-198`](file:///home/dietmar/devel/rust/registry-rust/src/gc_service.rs#L172-L198) -> [`src/blob_ref_index.rs:758-800`](file:///home/dietmar/devel/rust/registry-rust/src/blob_ref_index.rs#L758-L800).
- **Listing Type:** Unpaginated `storage.list_tags(&repo).await` (line 768).
- **Execution Dynamic:**
  Iterates over repositories and tags.
  `StorageError::NotFound` skips the repository (`continue`).
  Any other error returns `Err(e.into())`, aborting the refresh.
  For each successfully resolved tag, it performs inline mutations: `self.ingest_root(...)`, `self.tag_to_root.insert(...)`, and `self.inc_root_count(...)`.
  If an error occurs on repository $j$, index entries already written for repositories $1 \dots j-1$ remain in the Sled trees.

---

### 2.5 Summary Matrix of Production Call Sites

| Call Path | Source Location | Storage Method | Listing Scope | Subsequent Error Branches | Effect of Listing Error |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1. Proxy GC** | `supervisor.rs:940` | `list_tags` | Names only | `resolve_tag` separately suppressed | Silent suppression; latest SemVer unpinned; incomplete protection calculation |
| **2. Membership Sweep (Site 1)** | `gc_service.rs:611` | `list_tags` (via helper) | Names only | `resolve_tag` & manifest errors propagate | Aborts entire sweep via `?`; prior ledger mutations remain; CAS delete skipped |
| **2. Membership Sweep (Site 2)** | `gc_service.rs:654` | `list_tags` (via helper) | Names only | `resolve_tag` & manifest errors propagate | `Err(_) => true`; preserves candidate; sweep continues to next record |
| **3. Ref-Index Rebuild** | `blob_ref_index.rs:634` | `list_tags_page` | Names & digests (staged) | Paginated continuation loop with cycle detection | Aborts rebuild; index left cleared/partial in `Building` state; fail-closed |
| **4. Tag-Rooted Refresh** | `blob_ref_index.rs:768` | `list_tags` | Names only | `resolve_tag` errors propagate | Aborts refresh; prior repo index updates remain in Sled; GC operation fails |

---

## 3. Failure Consequence Characterization from Source

### 3.1 Scenario Analysis

#### Scenario A: First-Page vs Later-Page Listing Failure
- In `list_tags` (Paths 1, 2, 4): Unpaginated call. The concept of pagination failure is inapplicable.
- In `list_tags_page` (Path 3): In `discover_repo_manifests_and_tags`, if page 1 fails, discovery returns `Err` immediately. If page 1 succeeds and page 2 fails, discovery returns `Err` immediately. In both cases, Phase 2 index application is bypassed for that repository.

#### Scenario B: Failure Before Versus After Candidate Mutations
- In `sweep_repository_memberships_with_guard`:
  - If failure occurs at Site 1 on record $k$, mutations on records $1 \dots k-1$ remain committed in `self.storage`. However, the sweep aborts before Phase 2 (`blob_gc_delete`), so no blobs are deleted from CAS during this run.
  - If failure occurs at Site 2, the candidate is preserved, and the sweep continues.

#### Scenario C: Legacy Silent Omission vs Contained Seam
- Legacy `list_tags_page` silently drops tag files whose payload cannot be read or parsed as a valid digest (`if let Ok(...)`).
- Contained `contained_list_tags_page_seam` validates payloads and propagates read errors as `StorageError::Internal { kind: StorageErrorKind::Io, .. }` or `StorageErrorKind::Backend`.
- In contrast, name-only `list_tags` (both legacy and contained) inspects directory entries without opening payloads. A corrupt payload does **not** fail name-only listing. Corrupt payloads are encountered subsequently during `resolve_tag`.

#### Scenario D: Actual `StorageError` Types and Limit Mappings
`StorageError` does not contain `Io` or `ContainedLimitsExceeded` enum variants.
- System I/O errors are constructed via `StorageError::io(e)` as `StorageError::Internal { kind: StorageErrorKind::Io, message }`.
- In `contained_list_tags_seam` and `contained_list_tags_page_seam`, directory enumeration limit exceedance maps via `FsDirError::LimitExceeded { reason } => StorageError::backend(...)`, yielding `StorageError::Internal { kind: StorageErrorKind::Backend, message }`.

#### Scenario E: Missing Repository Versus Missing `tags/` Directory
- Entire repository directory missing (`repos/{repo}` absent): Both legacy and contained `list_tags` return `StorageError::NotFound`.
- Repository exists but `tags/` subdirectory missing: Both legacy and contained `list_tags` return `Ok(vec![])`.
- In `find_repo_blob_reference`, both cases evaluate to zero tag roots (`Ok(None)`).

#### Scenario F: Bounded Safety and Concurrency
- `scheduled_cleanup_once` passes `min_age = Duration::from_secs(self.config.blob_gc_default_min_age_secs)`. In `src/config.rs:1968`, this defaults to **7 days** (`7 * 24 * 3600` seconds), not 24 hours.
- A grace period with discrete rechecks does not establish continuous observation. Concurrent publications during sweep execution may race with candidate evaluation.
- `StorageError::NotFound` is a point-in-time observation, not proof of stable absence.

---

### 3.2 Epistemic Classification

#### Facts Established by Source
1. `compute_protected_blobs` at line 940 swallows `storage.list_tags` errors, and at line 951 separately swallows `storage.resolve_tag` errors.
2. `proxy_gc_once` performs zero persistent storage mutations or file deletions; it computes candidates and logs.
3. `find_repo_blob_reference` errors at Site 1 abort the entire sweep via `?`, whereas errors at Site 2 preserve the candidate via `Err(_) => true` and continue the sweep.
4. `RepositoryMembershipLedger` delegates authoritative state to `RepositoryBlobMembershipStorage`, not Sled; in `Indexed` mode the reverse index is updated on link, unlink, and reactivation (`reactivate_with_guard` calls `idx.record_membership` when the candidate flag was cleared).
5. In `rebuild`, `rebuild` clears `tag_to_root`, `root_counts`, `rev_edges`, and `repo_memberships` while retaining metadata, including Building state.
6. `StorageError` uses `StorageErrorKind` categories; no `StorageError::Io` or `ContainedLimitsExceeded` variants exist.
7. `blob_gc_default_min_age_secs` defaults to 7 days (604,800 seconds).

#### Existing Test Evidence
1. Lifecycle tests in `tests/manifest_lifecycle_tests.rs` verify fail-closed manifest preservation on listing errors.
2. Membership and index tests (in `tests/repository_membership_tests.rs` and `src/blob_ref_index.rs`) provide regression coverage of existing behavior: `apply_membership_migration` failures persist `Failed` checkpoints, `plan_membership_migration` returns errors with zero writes, and when called from application after the Verifying checkpoint save, verification failure leaves that checkpoint in place. Direct verification does not itself create or transition a checkpoint.

#### Inferences
1. Promoting contained listing without updating `compute_protected_blobs` leaves SemVer cache protection vulnerable to directory limit exhaustion.
2. Promoting contained listing without changing sweep abort behavior means any persistent listing fault on a single repository halts registry-wide GC.

#### Proposed Behavior (Pending Approval)
1. In `compute_protected_blobs`, propagate non-`NotFound` listing errors to `proxy_gc_once`.
2. Maintain the existing sweep-abort policy as the safe, fail-closed default pending formal redesign of multi-repository continuation.

#### Unresolved Decisions
1. Whether supervisor routines should retain `StorageError::NotFound => empty` compatibility or require explicit repository existence probes.
2. Whether `compute_protected_blobs` should also propagate `resolve_tag` errors or retain resolution suppression.
3. Designing safe, multi-record per-repository isolation for membership sweeps.

---

## 4. Proposed Bounded Supervisor Policy (Pending Approval)

### 4.1 Policy for `proxy_gc` (`compute_protected_blobs`)
1. **Explicit Error Branching:**
   In `compute_protected_blobs`:
   - `StorageError::NotFound`: treated as empty tags (`Vec::new()`) pending supervisor policy approval.
   - Any non-`NotFound` error: propagate `Err(format!("failed to list tags for repository '{repo}': {e}"))`.
2. **Execution Boundary:**
   If `compute_protected_blobs` returns an error, `proxy_gc_once` aborts and returns `Err(e)` before scanning `fs_root`; candidate counting and its informational log are skipped. `proxy_gc_once` itself emits no log on this path. The closure in `spawn_proxy_gc` maps the error to `proxy gc failed: {e}` (`src/supervisor.rs:568`), and `TaskSupervisor::spawn_loop` logs it at `warn` level as `periodic task iteration error` (`src/task_supervisor.rs:272`) before the next interval tick.
3. **Retained Limitation:**
   `resolve_tag` suppression at line 951 is retained unchanged in this slice, preserving existing tag resolution error behavior.

### 4.2 Policy for `blob_gc_scheduler` (`sweep_repository_memberships_with_guard`)
1. **Preserve Existing Sweep-Abort Policy as Bounded Default:**
   The `?` operator at Site 1 (`gc_service.rs:611`) must be **preserved**.
   - An error from `find_repo_blob_reference` aborts the membership sweep immediately.
   - Prior committed ledger mutations remain.
   - Because `scheduled_cleanup_once` awaits the sweep with `?` (`src/gc_service.rs:720–722`) before reaching `blob_gc_delete`, the CAS deletion phase is not reached in that tick. This is source-order reasoning about the enclosing caller; it is not a property observable from `sweep_repository_memberships_with_guard` alone.
2. **Rejection of Premature Per-Repository Continuation:**
   Replacing `?` with `continue` at Site 1 would skip an individual record rather than isolating an entire repository. It would allow `sweep_repository_memberships_with_guard` to return `Ok`, enabling downstream CAS deletion to execute against potentially incomplete reference data. Any alternative continuation policy is deferred for a dedicated design.
3. **Preserve Site 2 Revalidation Behavior:**
   Site 2 (`Err(_) => true`) is retained: errors during pre-unlink revalidation preserve candidate status and skip unlinking.

### 4.3 Policy for Ref-Index Rebuild
Retain current fail-closed behavior: two-phase staging protects individual repo syncs, while `rebuild` clears `tag_to_root`, `root_counts`, `rev_edges`, and `repo_memberships` while retaining metadata, including Building state, halting server startup or GC runs safely.

---

## 5. Smallest Follow-Up Implementation and Test Specification

### 5.1 Source Code Changes
1. In `src/supervisor.rs` (`compute_protected_blobs`):
   Update lines 940–946:
   ```rust
   let tags = match storage.list_tags(&repo).await {
       Ok(t) => t,
       Err(StorageError::NotFound) => Vec::new(),
       Err(e) => {
           return Err(format!(
               "failed to list tags for repository '{repo}' during proxy gc: {e}"
           ));
       }
   };
   if let Some(latest) = pick_latest_semver_tag(tags, tag_regex.as_deref(), *allow_prerelease) {
       pinned_tags.push(latest);
   }
   ```
2. In `src/gc_service.rs`: Zero source changes in the narrow slice; preserve the existing sweep-abort policy.

### 5.2 Narrow Test Specification (Observable Boundaries)

Each case names the function under test and asserts only what that function can observe. Log lines emitted by callers (`TaskSupervisor::spawn_loop`) are outside a direct `proxy_gc_once` test; downstream behavior of `scheduled_cleanup_once` is outside a direct sweep test.

#### 5.2.1 New coverage for the proposed `compute_protected_blobs` change
1. **Representative non-`NotFound` listing failures abort proxy GC.**
   For each of `StorageError::internal(StorageErrorKind::Io, "disk error")`, `StorageError::backend("directory entry limit exceeded")` (the contained-seam `FsDirError::LimitExceeded` mapping), and `StorageError::internal(StorageErrorKind::PermissionDenied, "EACCES")`, inject the error on `list_tags(repo)` for a repository matched by a `KeepLatestCachedSemver` rule.
   - `compute_protected_blobs` returns `Err(msg)` where `msg` names the repository.
   - `proxy_gc_once` returns `Err(msg)`. To show the abort precedes candidate scanning, pass an `fs_root` whose `blobs/sha256` directory does not exist: had scanning been reached, `proxy_gc_once` would have returned `Ok(())` via the `NotFound` branch at `src/supervisor.rs:838`.
   - Direct proxy_gc_once tests assert its returned error, not an error log from that function. The outer supervisor loop logs errors (tested through `spawn_proxy_gc`/`TaskSupervisor::spawn_loop`).
2. **`NotFound` compatibility (pending approval).**
   Inject `StorageError::NotFound` on `list_tags("missing-repo")`. `compute_protected_blobs` returns `Ok(set)` with zero pinned tags for that repository and continues with the remaining repositories. This case encodes the proposed `NotFound => Vec::new()` policy and must not be merged until Unresolved Decision 1 (§3.2) is approved.
3. **Successful SemVer selection and protection.**
   With tags `["v1.0.0", "v1.2.0", "v1.3.0-rc1"]`, `tag_regex = None`, `allow_prerelease = false`: `pick_latest_semver_tag` yields `v1.2.0`; `resolve_tag` returns its root digest; `compute_protected_blobs` returns `Ok(set)` containing the root manifest digest and every blob digest referenced by that manifest (via `collect_protected_blobs_for_manifest`). With `allow_prerelease = true` the selection is `v1.3.0-rc1`.
4. **Subsequent invocation succeeds after fault clearance.**
   First call with the injected `Io` failure returns `Err`; clear the fault in the fake storage; the second call returns `Ok(set)` with the protection from case 3. This models the supervisor loop's per-tick retry without executing the loop.
5. **Retained limitation: `resolve_tag` error suppression (characterization).**
   Inject `StorageError::internal(StorageErrorKind::Io, ..)` on `resolve_tag(repo, "v1.2.0")` while `list_tags` succeeds. `compute_protected_blobs` returns `Ok(set)` and `set` does **not** contain that manifest's blobs. This documents the limitation retained by §4.1 item 3 and must be updated when Unresolved Decision 2 is settled.

#### 5.2.2 Existing-behavior regression coverage (no production change)
The following cases pin current behavior of code this slice does not modify. They are regression coverage, not new production changes, and any discrepancy found is a documentation finding first.
6. **Membership sweep initial-check failure aborts the sweep.**
   Under test: `sweep_repository_memberships_with_guard` directly. Inject a non-`NotFound` storage error on `list_tags` for the repository of Record 2 of three. Assert `Err(..)`, that Record 1's ledger mutation is persisted in storage, and that Record 3 was never evaluated (its state unchanged, no `find_repo_blob_reference` call recorded). Demonstrating skipped downstream CAS work requires testing scheduled_cleanup_once, or explicitly identifying the conclusion as source-order reasoning (since `scheduled_cleanup_once` halts at the `?` before calling Phase 2 CAS deletion when sweep returns an error). To observe it, drive `scheduled_cleanup_once` with an enabled configuration, an active mutation authority, and a ready index, and assert that no `blob_gc_delete` call reaches storage.
7. **Membership revalidation failure preserves the candidate.**
   Inject the storage error only on the second `find_repo_blob_reference` evaluation (Site 2) for an aged candidate. Assert the record is not unlinked, `stats.skipped` increments, and the following record is processed.
8. **Direct sync staging versus rebuild failure.**
   `sync_repo_manifests_and_tags`: inject a `list_tags_page` failure; assert no Sled tree changed. `rebuild`: inject the same failure on the second repository; assert `rebuild` clears `tag_to_root`, `root_counts`, `rev_edges`, and `repo_memberships` while retaining metadata, including Building state, leaves the cleared trees with only the first repository's entries, and `check_health()` returns `RefIndexError::Corrupt(..)`.

## 6. Canonical Quality Gate Status

All eight canonical quality gates remain explicitly **OPEN**:
- `O-03`: Key and continuation-token contracts.
- `O-04`: Filesystem write durability and containment.
- `O-05`: Broader filesystem read containment.
- `O-06`: Typed AWS mapping and pinned-MinIO evidence.
- `O-13`: Hosting, distribution, and release strategy.
- `O-15`: Non-Linux verification.
- `O-16`: Earlier Slice 11 audit/test-inventory evidence.
- `D-06`: Broader extraction, cutover, compatibility, and distribution acceptance.
