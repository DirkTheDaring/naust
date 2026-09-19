> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Landed as `5779f7f`.
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Membership-Migration Tag-Listing Error Hardening Architecture Document

- **Status:** **CONTRACT ASSERTIONS COMPLETED — READY FOR REVIEW — NOT COMMITTED**
- **Target Repository:** `registry-rust` (HEAD: `96c0729bcd02c5f44fbfa13c5b0d4ea77bff6a86`)
- **Dependency Repository:** `storage-layer-rust` (HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`, strictly read-only)
- **Canonical Quality Gates:** All eight quality gates remain explicitly **OPEN** (`O-03, O-04, O-05, O-06, O-13, O-15, O-16, D-06`).
- **Baseline Document Reference:** `docs/architecture/membership-migration-tag-listing-hardening-design.md` (SHA-256: `2bc5a9bc6890264d29b857ec1d240445734c9a34870642fee79f9f87a9009431`)

---

## 1. Executive Summary & Purpose

This document specifies and records the implementation of tag-listing error hardening across the repository-blob membership migration subsystem in `registry-rust`.

Previously, three primary functions in `src/membership_migration.rs` suppressed all tag-listing errors:
```rust
let tags = storage.list_tags(repo).await.unwrap_or_default();
```
This wildcard error suppression caused:
1. **Planning (`plan_membership_migration`):** Under-reporting migration work (reporting 0 manifests/memberships for unreadable repositories).
2. **Application (`apply_membership_migration`):** Skipping backfill for a repository on listing errors and advancing `source_continuation_token` past it, causing unmigrated repositories to be permanently bypassed on subsequent passes.
3. **Verification (`verify_membership_migration`):** Omitting unreadable repositories from verification, allowing `apply_membership_migration` to falsely mark storage as `Ready`.

This slice hardens these three paths with an explicitly authorized compatibility and failure-handling contract, implements UTF-8 safe diagnostic bounding, and verifies all failure dynamics through deterministic test doubles and hardened contract assertions.

---

## 2. Authorized Design Decisions

Following the review of `docs/architecture/membership-migration-tag-listing-hardening-design.md`, the pending architectural decisions have been resolved as follows:

### 2.1 NotFound Compatibility Policy
- **Planning (`plan_membership_migration`):** `StorageError::NotFound` returns empty tags (`Vec::new()`). Zero writes attempted.
- **Application (`apply_membership_migration`):** `StorageError::NotFound` returns empty tags (`Vec::new()`).
- **Verification (`verify_membership_migration`):** `StorageError::NotFound` returns empty tags (`Vec::new()`).
- **All Other Errors:** Propagate immediately as `Err(StorageError)`.
- **Policy Grounding:** This is an explicitly authorized compatibility policy. `NotFound` is not proof of snapshot completeness or absence of concurrent changes. On filesystem storage, a missing `tags/` directory within an existing repository directory already returns `Ok(Vec::new())`; `StorageError::NotFound` indicates the repository directory itself is absent from disk.

### 2.2 Application Failure Contract (`apply_membership_migration`)
When `storage.list_tags(repo)` returns a non-`NotFound` error:
1. **Transition Phase:** Set `checkpoint.phase = MigrationPhase::Failed`.
2. **Preserve Continuation Cursor:** Do NOT advance `source_continuation_token`. It remains set to the last successfully completed repository (`Some(last_completed)` or `None`).
3. **Retain Current Repository:** Keep `checkpoint.current_repository = Some(canonical_repo)`.
4. **Preserve Completed Work:** Prior created memberships and cumulative `checkpoint.stats` are preserved.
5. **Preserve Lease Expiry & Owner:** Preserve `checkpoint.owner_id` and `checkpoint.lease_expiry_unix_secs` exactly as renewed at the start of the repository attempt.
6. **No Lease Renewal or Clearance:** Do not renew or clear the lease on failure.
7. **UTF-8 Safe Diagnostic Bounding:** Set `checkpoint.failure_info` with repository and listing error context, bounded to at most 512 UTF-8 bytes without splitting a character boundary (`bound_utf8_diagnostic`).
8. **Await Failure Checkpoint Persistence:** Call and await `storage.save_migration_checkpoint(&checkpoint).await`.
9. **Return Original Error:** Return the original listing error `Err(e)`.
10. **Secondary Checkpoint Persistence Failure Policy:** If `save_migration_checkpoint` fails:
    - Emit a structured warning diagnostic containing repository name, original listing error, and checkpoint-save error.
    - Return the original listing error `Err(e)`.
    - Do not claim a `Failed` checkpoint was persisted. Storage retains the last successfully saved checkpoint (`Applying`).

### 2.3 Verification Failure Contract (`verify_membership_migration`)
When `storage.list_tags(repo)` fails during verification:
1. `StorageError::NotFound` returns empty tags (`Vec::new()`).
2. Any other error returns `Err(e)` immediately.
3. In `apply_membership_migration`, caller's `?` propagates `Err(e)` immediately:
   - `storage.mark_membership_ready().await?` is NOT called.
   - The `Ready` checkpoint transition is NOT performed.
   - The persisted checkpoint remains in `MigrationPhase::Verifying` with `source_continuation_token` pointing to the final repository.
   - Retry after lease expiry skips the application loop (since all repositories `<= source_continuation_token`) and directly repeats verification.

---

## 3. Implementation Details

### 3.1 UTF-8 Character Boundary Bounding
In `src/membership_migration.rs`:
```rust
fn bound_utf8_diagnostic(err_msg: &mut String, max_bytes: usize) {
    if err_msg.len() > max_bytes {
        let mut boundary = max_bytes;
        while !err_msg.is_char_boundary(boundary) {
            boundary -= 1;
        }
        err_msg.truncate(boundary);
    }
}
```
- If `err_msg.len() > 512`, `boundary` decrements from 512 until `is_char_boundary(boundary)` is true.
- Since index 0 is always a character boundary in valid UTF-8, the search terminates panic-free with `boundary <= 512`.
- `err_msg.truncate(boundary)` slices cleanly on a character boundary, preventing panics and invalid UTF-8 strings.

### 3.2 Error Handling Changes
In `src/membership_migration.rs`:
1. `plan_membership_migration`:
   ```rust
   let tags = match storage.list_tags(repo).await {
       Ok(tags) => tags,
       Err(StorageError::NotFound) => Vec::new(),
       Err(e) => return Err(e),
   };
   ```
2. `apply_membership_migration`:
   ```rust
   let tags = match storage.list_tags(repo).await {
       Ok(tags) => tags,
       Err(StorageError::NotFound) => Vec::new(),
       Err(e) => {
           checkpoint.phase = MigrationPhase::Failed;
           let mut err_msg = format!("tag listing failed for repo {repo}: {e}");
           bound_utf8_diagnostic(&mut err_msg, 512);
           checkpoint.failure_info = Some(err_msg);
           if let Err(save_err) = storage.save_migration_checkpoint(&checkpoint).await {
               tracing::warn!(
                   repo = %repo,
                   listing_error = %e,
                   checkpoint_save_error = %save_err,
                   "failed to persist migration failure checkpoint; returning original listing error"
               );
           }
           return Err(e);
       }
   };
   ```
3. `verify_membership_migration`:
   ```rust
   let tags = match storage.list_tags(repo).await {
       Ok(tags) => tags,
       Err(StorageError::NotFound) => Vec::new(),
       Err(e) => return Err(e),
   };
   ```

---

## 4. Known Adjacent Limitations & Deferrals

The following known issues in `src/membership_migration.rs` are documented and intentionally deferred to subsequent slices:
1. **`resolve_tag` Suppression:** `resolve_tag` failures in `plan_membership_migration` (line 35) and `apply_membership_migration` (line 159) continue to use `if let Ok(...)`, suppressing unresolvable tags.
2. **Verification Read/Parse Suppression:** `verify_membership_migration` continues to suppress `resolve_tag`, `get_manifest`, and `parse_manifest_refs` failures using `if let Ok(...)`.
3. **Parse-Error Un-Awaited Checkpoint Save:** In `apply_membership_migration` (line 167), manifest parse error handling discards an unawaited `save_migration_checkpoint` future (`let _ = ...;`).
4. **Parse-Error Character Truncation:** In `apply_membership_migration` (line 165), parse error message truncation uses `err_msg.truncate(512)` directly, which may split a UTF-8 multibyte boundary if triggered.

---

## 5. Checkpoint Storage & Locking Limitations

- **Check-Then-Save Mutual Exclusion:** The mutual exclusion in `apply_membership_migration` reads the existing checkpoint, compares the lease timestamp in memory, and writes a new checkpoint. This pattern is not an atomic compare-and-swap on storage backends.
- **Backend Durability Contracts:**
  - Filesystem backend uses POSIX rename for replacement without optimistic version checks.
  - S3 backend uses `put_object_conditional` with `None, None` (unconditional PUT).
- **Guarantees Not Claimed:** Power-loss durability, atomic lease acquisition, and exactly-once counters are explicitly not claimed. Test assertions verify deterministic fixture states.

---

## 6. Verification and Acceptance Test Coverage

Deterministic scripted test doubles in `tests/support/gc_coordination.rs` (`LifecycleFaultStorage`) and tests in `tests/repository_membership_tests.rs` cover:
1. **Planning Failure:** Propagation of original error kind/message and verification of zero attempted writes.
2. **Application Failure Exact Lease & Cursor Preservation:**
   - Repository A succeeds and links memberships; repository B fails listing.
   - Asserts A's memberships are preserved, cursor remains at A, B remains current repository, and checkpoint is marked `Failed`.
   - Compares the `Failed` checkpoint directly against the last successfully saved `Applying` checkpoint for repository B: proves exact equality of `owner_id`, `lease_expiry_unix_secs`, `source_continuation_token`, and `current_repository` using recorded checkpoint values rather than wall-clock assumptions.
3. **Failure Checkpoint Persistence:** Verifies failure checkpoint save is awaited on disk.
4. **Secondary Checkpoint Save Failure Diagnostic Capture:**
   - Injects failure-before-write on saving `Failed` checkpoint.
   - Captures tracing events via isolated `BufferWriter` and `tracing::subscriber::set_default` under `#[tokio::test(flavor = "current_thread")]`, preventing subscriber interference with parallel tests.
   - Asserts warning contains repository (`repo-b`), original listing error (`tag listing connection reset`), and secondary save error (`simulated failure-before-write saving checkpoint`).
   - Asserts original error is returned and storage retains the last successfully persisted `Applying` checkpoint.
5. **Retained Lease Rejection:** Verifies immediate reinvocation generates a new UUID owner and is rejected with `StorageError::Conflict`.
6. **Controlled Expiry, Retry, and Phase-Aware Application Work Skipping:**
   - Manipulates checkpoint expiry timestamp in fixture (without sleeping 60 seconds).
   - Proves via `TagListingRecord { repo, phase }` that on retry after application failure, completed repo A is NOT listed during `Applying`, failed repo B IS listed during `Applying`, and verification subsequently lists both repos.
7. **Verification Listing Failure & Zero Application-Stage Listing on Retry:**
   - Injects listing failure during verification.
   - Asserts `Ready` is prevented, `mark_membership_ready` is not called, checkpoint remains `Verifying`.
   - Proves via `TagListingRecord { repo, phase }` that subsequent retry with unchanged inventory makes ZERO `Applying`-stage tag listing calls and repeats verification for all repositories to completion.
8. **NotFound Compatibility:** Independent verification that `StorageError::NotFound` returns empty tags for planning, application, and verification.
9. **Multibyte UTF-8 Bounding:** Verifies panic-free bounding of multibyte characters crossing byte 512.
10. **Representative Storage Error Variants:** Validates propagation of `Backend`, `Io`, and `PermissionDenied` errors.
11. **Shared Test-Support Backward Compatibility:** Verifies all 97 tests in `manifest_lifecycle_tests` pass without regression with the enhanced `LifecycleFaultStorage`.

---

## 7. Canonical Quality Gate Status

All 8 canonical quality gates remain explicitly **OPEN**:

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
