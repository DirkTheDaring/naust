> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Landed as `96c0729`. Provenance (reconstructed 2026-09-19): the original carries no status/date/baseline stamps; it was added in commit `96c0729` (2026-09-12), the implementing commit itself.
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Lifecycle Tag-Listing Error Hardening Architecture Design

## 1. Executive Summary & Purpose

This document details the bounded error-hardening implementation for tag-listing calls within `ManifestLifecycleService` in `registry-rust`.

Previously, three lifecycle recovery and eviction call sites suppressed every listing failure from `self.storage.list_tags_page(...)` via a wildcard match:
```rust
Err(_) => (Vec::new(), None)
```
This wildcard suppression created a failure mode: if tag listing returned an error (such as transient or permanent I/O errors, storage backend unavailability, or OS permission restrictions), the caller interpreted the result as zero tags referencing the target manifest. Under that interpretation, the lifecycle routine proceeded to subsequent steps that could delete manifests, remove referrers, or unlink proxy blob memberships.

This slice hardens all three call sites by replacing wildcard error suppression with an explicit, fail-closed policy:
1. `StorageError::NotFound => (Vec::new(), None)`: Retained as an explicitly authorized narrow compatibility policy.
2. Every other error: Immediately returned and propagated up through `ManifestLifecycleError::Storage(e)` (fail-closed).

A non-NotFound listing error stops subsequent processing at the three hardened sites. Earlier mutations remain, legacy payload omission remains, and NotFound compatibility remains.

This modification is strictly bounded to the three caller sites in `src/manifest_lifecycle.rs` and does **not** alter the production storage drivers, does **not** promote the test seam to production, and leaves all 8 canonical quality gates OPEN.

---

## 2. Authorized Call Sites & Replacement Policy

### 2.1 The Exact Replacement Policy

The wildcard pattern `Err(_) => (Vec::new(), None)` was replaced at each authorized call site with:

```rust
Err(StorageError::NotFound) => (Vec::new(), None),
Err(e) => return Err(ManifestLifecycleError::Storage(e)),
```

### 2.2 Rationale & Why `NotFound` Retains Empty-Page Interpretation

- **Fail-Closed Principle:** Storage errors indicating infrastructure failures, system I/O errors (`StorageErrorKind::Io`), permission denials (`StorageErrorKind::PermissionDenied`), corrupt entries (`StorageErrorKind::CorruptData`), or backend RPC errors (`StorageErrorKind::Backend`) must not be suppressed into zero observed tags. Treating unexpected storage errors as zero tags allows lifecycle routines to infer that a manifest has zero references when reference discovery was not successfully completed.
- **Narrow `NotFound` Compatibility Policy:** The three caller sites retain the explicitly authorized compatibility policy of treating StorageError::NotFound as an empty terminal page. This policy does not establish snapshot completeness or prove that a repository has no tags. Filesystem paged listing already returns empty success for a missing tags directory.
- **Fail-Fast Propagation:** When any non-`NotFound` error occurs, the pagination loop is terminated immediately. No subsequent pagination requests are issued, no subsequent destructive steps are initiated, and the enclosing operation returns an error.

### 2.3 Exact Call Sites in `src/manifest_lifecycle.rs`

#### Site 1: `recover_pending_journal_under_lock` — `LifecycleOpKind::DeleteManifest`
- **Location:** `src/manifest_lifecycle.rs` (lines 585–593).
- **Context:** During recovery of an interrupted `DeleteManifest` operation at phase `LifecyclePhase::TagsSnapshotted`, the service paginates through repository tags using `POLICY_B_TAG_PAGE_SIZE` (64) to discover and delete any remaining tags pointing to `journal.target_digest`.
- **Hardened Source:**
```rust
let (page, next_tok) = match self
    .storage
    .list_tags_page(repo, proof_token.as_deref(), POLICY_B_TAG_PAGE_SIZE)
    .await
{
    Ok(res) => res,
    Err(StorageError::NotFound) => (Vec::new(), None),
    Err(e) => return Err(ManifestLifecycleError::Storage(e)),
};
```
- **Failure Behavior:** If listing fails on any page (first page or later page), the function immediately aborts and returns `Err(ManifestLifecycleError::Storage(e))`. The subsequent destructive steps—`remove_referrer`, `delete_manifest`, index deletion reconciliation (`on_manifest_deleted`), and `delete_journal`—are completely bypassed.

#### Site 2: `recover_pending_journal_under_lock` — `LifecycleOpKind::ProxyEvict`
- **Location:** `src/manifest_lifecycle.rs` (lines 666–674).
- **Context:** During recovery of an interrupted `ProxyEvict` operation at phase `LifecyclePhase::ProxyTagDeleted`, the service checks if any other tags in the repository resolve to `journal.target_digest`.
- **Hardened Source:**
```rust
let (page, next_tok) = match self
    .storage
    .list_tags_page(repo, page_tok.as_deref(), POLICY_B_TAG_PAGE_SIZE)
    .await
{
    Ok(p) => p,
    Err(StorageError::NotFound) => (Vec::new(), None),
    Err(e) => return Err(ManifestLifecycleError::Storage(e)),
};
```
- **Failure Behavior:** If listing fails, recovery returns `Err(ManifestLifecycleError::Storage(e))` immediately. Step 3 (manifest deletion, ref index updates, and unlinking proxy blob memberships) and step 4 (journal cleanup) are skipped.

#### Site 3: `evict_proxy_cached_entry`
- **Location:** `src/manifest_lifecycle.rs` (lines 1220–1228).
- **Context:** In step 5 of active proxy cache eviction, after conditionally deleting the target tag alias and updating the durable journal to `LifecyclePhase::ProxyTagDeleted`, the service paginates repository tags to ensure no other tag points to `target_digest` before proceeding to delete the manifest.
- **Hardened Source:**
```rust
let (page, next_tok) = match self
    .storage
    .list_tags_page(repo, page_tok.as_deref(), POLICY_B_TAG_PAGE_SIZE)
    .await
{
    Ok(p) => p,
    Err(StorageError::NotFound) => (Vec::new(), None),
    Err(e) => return Err(ManifestLifecycleError::Storage(e)),
};
```
- **Failure Behavior:** If listing fails on page 1 or on subsequent pages, `evict_proxy_cached_entry` returns `Err(ManifestLifecycleError::Storage(e))`. Step 6 (manifest deletion, blob unlinking, index cleanup, and journal deletion) is never entered.

---

## 3. Invariants & Bounded Safety Guarantees

### 3.1 Destructive Actions Avoided on Listing Errors
When a non-`NotFound` listing error occurs at any of the three hardened call sites:
1. **No Manifest Deletion:** `self.storage.delete_manifest(...)` is never called.
2. **No Referrer Removal:** `self.storage.remove_referrer(...)` is never called.
3. **No Blob Unlinking:** `self.storage.unlink_repo_blob(...)` is never called.
4. **No Journal Deletion:** `self.delete_journal(...)` is never called.
5. **No False Index Reconciliation:** `idx.on_manifest_deleted(...)` is never called.

### 3.2 Bounded Nature of the Safety Guarantees
A non-NotFound listing error stops subsequent processing at the three hardened sites. Earlier mutations remain, legacy payload omission remains, and NotFound compatibility remains.

Specifically:
- **Site-Specific Enforcement:** The guarantee is bounded strictly to the three call sites in `src/manifest_lifecycle.rs`. It does **not** prove that all storage discovery failures prevent deletion across the entire registry codebase.
- **Legacy Storage Behavior Unchanged:** This change modifies callers of `list_tags_page`, not the underlying storage driver. The legacy `FsStorage::list_tags_page` implementation continues to silently omit unreadable payloads during directory scans.
- **`NotFound` Compatibility:** `StorageError::NotFound` explicitly does **not** fail closed; it preserves its narrow compatibility interpretation as an empty terminal page `(Vec::new(), None)`.
- **Preceding Mutation State:** This slice guarantees that subsequent destructive actions are avoided after a listing error. It does **not** prove that earlier deletion attempts succeeded.
- **Tested Durability Scope:** Tested journal retention confirms that the journal remains readable in storage across the simulated faults and permits successful resumption upon recovery retry within the test environment. It does not constitute an independent guarantee of hardware power-loss durability.

### 3.3 Ordering, Coordination, and Preceding Mutations
The lifecycle control flow exhibits specific sequencing that must be understood precisely:
1. **Coordination/Lease Activity Precedes Recovery:** Mutating operations acquire coordination before invoking recovery. Specifically:
   - In publication paths, `publish` and `publish_manifest` delegate to `publish_internal`. `publish_proxy_cached_manifest` also delegates to `publish_internal`. It is `publish_internal` that executes `let mut guard = self.acquire_coordination(&repo).await?;` and subsequently calls `self.recover_and_ensure_index_healthy(&repo).await?;`.
   - `evict_proxy_cached_entry` calls `self.acquire_coordination(repo).await?;` before `self.recover_and_ensure_index_healthy(repo).await?;`.
   - `delete_manifest`, `delete_tag`, and `mutate_tag` each call `self.acquire_coordination(repo).await?;` before `self.recover_and_ensure_index_healthy(repo).await?;`.
   Therefore, lease and coordination acquisition consistently precede recovery.
2. **Recovery Itself Performs Preceding Legitimate Mutations:**
   - In `recover_pending_journal_under_lock` for `DeleteManifest`, step 1 executes conditional deletions (`self.storage.delete_tag_conditional`) for un-deleted tags in `journal.relevant_tags` before initiating `list_tags_page`.
   - In `recover_pending_journal_under_lock` for `ProxyEvict`, step 1 conditionally deletes the target tag alias before initiating `list_tags_page`.
   - In active `evict_proxy_cached_entry`, step 4 conditionally deletes the target tag alias and updates the journal to `LifecyclePhase::ProxyTagDeleted` before step 5 initiates `list_tags_page`.
3. **Propagation Without Rollback:** When listing fails, error propagation terminates the remainder of the routine immediately. Preceding legitimate mutations remain in effect; this change does not roll back or undo preceding work.
4. **Resumption from Durable Journal:** Because the journal was not deleted and reflects the current phase (`TagsSnapshotted` or `ProxyTagDeleted`), subsequent retries under `recover_pending_journal_under_lock` can resume and complete the remaining stages once the listing fault is resolved.

---

## 4. Control Flow & Service-Boundary Tracing

### 4.1 Internal Lifecycle Propagation
The control flow across callers within `ManifestLifecycleService` was systematically traced:

1. **`recover_pending_journal_under_lock(repo, journal)`:**
   - When `list_tags_page` returns `Err(e)` (where `e != StorageError::NotFound`), the function immediately returns `Err(ManifestLifecycleError::Storage(e))`.
2. **`recover_and_ensure_index_healthy(repo)`:**
   - Invokes `self.recover_pending_journal_under_lock(repo, &journal).await?`.
   - Because of the `?` operator, the error is not swallowed, suppressed, or logged-and-ignored; it propagates immediately to the caller.
3. **Mutating Entry Points (`publish_internal`, `evict_proxy_cached_entry`, `delete_manifest`, `delete_tag`, `mutate_tag`):**
   - Each entry point executes `self.recover_and_ensure_index_healthy(repo).await?;` after acquiring coordination.
   - If recovery fails due to a listing fault, the entry point aborts immediately with `Err(ManifestLifecycleError::Storage(e))`. No subsequent operation journal is created, no manifest payload is written, and no additional mutations occur.

### 4.2 Application Service Boundary
At the application layer:
- **`ManifestMutationService::evict_proxy_manifest_and_memberships` (`src/application/manifest.rs`):**
  - Calls `self.lifecycle.evict_proxy_cached_entry(...)` and maps the result via `.map_err(ManifestMutationError::from)`.
  - The error propagates cleanly as `ManifestMutationError::Lifecycle(ManifestLifecycleError::Storage(...))`.
- **Scope of Claims:** Conclusions are strictly restricted to this verified service boundary. We do not make unverified claims regarding downstream HTTP status codes (such as HTTP 500) or unverified background supervisor sweeps, as no supervisor sweep calls manifest recovery directly.

---

## 5. Relationship to Contained Tag-Listing Production Promotion

The contained filesystem tag-listing test seam (`contained_list_tags_page_seam` in `src/storage/fs/tag_listing.rs`) provides path-traversal containment, robust parsing, and Linux `openat2` resolution restrictions under `cfg(test)`.

Prior to this slice, promoting the contained seam to production would have been problematic: any contained-listing error (e.g. permission denied on a malformed child entry or I/O failure) encountered by lifecycle callers would have been masked as an empty page, allowing subsequent deletion steps to proceed.

By hardening the lifecycle callers to fail closed on all non-`NotFound` listing errors, the system now guarantees that:
1. Storage listing failures safely block subsequent cleanup steps rather than triggering deletion.
2. A prerequisite for production promotion is satisfied.

However, **production promotion itself is not part of this slice.** Production storage drivers continue to use legacy listing routines.

---

## 6. Status of Canonical Quality Gates

All 8 canonical quality gates remain strictly **OPEN**:

| Gate | Status | Canonical Definition |
|:---|:---:|:---|
| **O-03** | **OPEN** | Key and continuation-token contracts. |
| **O-04** | **OPEN** | Filesystem write durability and containment. |
| **O-05** | **OPEN** | Broader filesystem read containment. |
| **O-06** | **OPEN** | Typed AWS mapping and pinned-MinIO evidence. |
| **O-13** | **OPEN** | Hosting, distribution, and release strategy. |
| **O-15** | **OPEN** | Non-Linux verification. |
| **O-16** | **OPEN** | Earlier Slice 11 audit/test-inventory evidence. |
| **D-06** | **OPEN** | Broader extraction, cutover, compatibility, and distribution acceptance. |

---

## 7. Verification Summary

Comprehensive automated verification was performed in `registry-rust`:
- **Unit & Integration Tests:** 18 new focused tests in `tests/manifest_lifecycle_tests.rs` verifying first-page and later-page failures across `Backend`, `Io`, and `PermissionDenied` categories, operation recording, mutation absence, journal durability, retry success, `NotFound` compatibility, and other-tag reference discovery.
- **Full Test Suite:** All 97 lifecycle tests in `tests/manifest_lifecycle_tests.rs` pass cleanly (`0 failed`).
- **Test Execution Accounting:** 97 distinct passed tests; 116 total passing executions across packaged verification runs, with 19 repeated executions.
- **Linter & Typecheck:**
  - `cargo fmt --check`: Clean (0 diffs).
  - `cargo check --locked --all-targets --all-features`: Clean (0 warnings/errors).
  - `cargo clippy --locked --all-targets --all-features -- -D warnings`: Clean (0 warnings).
