> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Landed as `5779f7f`. Companion implementation record (same baseline): membership-migration-tag-listing-error-hardening.md.
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Membership-Migration Tag-Listing Error Hardening Architecture Design

- **Status:** **DESIGN ONLY — READ-ONLY INSPECTION — IMPLEMENTATION DEFERRED — NOT COMMITTED**
- **Target Repository:** `registry-rust` (HEAD: `96c0729bcd02c5f44fbfa13c5b0d4ea77bff6a86`)
- **Dependency Repository:** `storage-layer-rust` (HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`, strictly read-only)
- **Canonical Quality Gates:** All eight quality gates remain explicitly **OPEN** (`O-03, O-04, O-05, O-06, O-13, O-15, O-16, D-06`).

---

## 1. Executive Summary & Purpose

This design document provides a source-grounded architectural specification for hardening tag-listing error handling across the repository-blob membership migration subsystem in `registry-rust`.

Currently, three distinct functions in `src/membership_migration.rs` invoke tag listing via an unchecked wildcard fallback:
```rust
let tags = storage.list_tags(repo).await.unwrap_or_default();
```
This pattern suppresses every error variant—including transient I/O errors (`StorageErrorKind::Io`), permission denials (`StorageErrorKind::PermissionDenied`), corrupt entries (`StorageErrorKind::CorruptData`), and backend RPC failures (`StorageErrorKind::Backend`)—into an empty tag vector (`Vec::new()`).

In the membership migration subsystem, this silent suppression leads to the following failure modes:
1. **Planning (`plan_membership_migration`):** Produces false dry-run statistics reporting 0 manifests and 0 memberships to create, misleading operators into believing unmigrated repositories require no action.
2. **Application (`apply_membership_migration`):** When tag listing fails for a repository, the loop body executes 0 times, creating 0 memberships for that repository. Immediately afterward, the checkpoint cursor (`source_continuation_token`) advances past the un-backfilled repository. A subsequent invocation skips previously completed names, omitting backfills for the failed repository on subsequent application passes.
3. **Verification (`verify_membership_migration`):** Suppressing listing errors results in 0 tags being checked for the failed repository. If other repositories pass, verification returns `Ok(true)`, allowing `apply_membership_migration` to mark the storage layer `Ready` despite unmigrated repositories.

This document establishes the precise failure dynamics of the existing code, proposes a narrow fail-closed hardening contract, specifies lease and retry mechanics, documents check-then-save locking limitations, addresses UTF-8 safe diagnostic bounding, documents secondary checkpoint failure handling, analyzes `NotFound` semantics, specifies concrete acceptance tests, and defines the smallest implementation slice.

---

## 2. Complete Migration Lifecycle Tracing

### 2.1 Enclosing Functions & Mechanical Source Excerpts

The migration lifecycle consists of three primary entry points in `src/membership_migration.rs`:

#### 1. `plan_membership_migration` (`src/membership_migration.rs:11-40`)
```rust
/// Plan repository blob membership migration (dry-run). Performs ZERO writes.
pub async fn plan_membership_migration(
    storage: &(impl BlobRefIndexStoragePort + ?Sized),
) -> Result<MigrationStats, StorageError> {
    let mut stats = MigrationStats::default();
    let repos = storage.list_repositories().await?;
    stats.repositories_scanned = repos.len();

    for repo in &repos {
        let tags = storage.list_tags(repo).await.unwrap_or_default();
        for tag in tags {
            if let Ok(manifest_digest) = storage.resolve_tag(repo, &tag).await {
                stats.manifests_scanned += 1;
                let (_meta, bytes) = storage.get_manifest(repo, &manifest_digest).await?;
                let refs = parse_manifest_refs(&bytes).map_err(|e| {
                    StorageError::corrupt_data(format!(
                        "corrupt manifest {manifest_digest} in repo {repo}: {e}"
                    ))
                })?;
                for blob_d in refs.blob_references() {
                    match storage.get_repo_blob_membership(repo, blob_d).await? {
                        Some(_) => stats.memberships_already_present += 1,
                        None => stats.memberships_created += 1,
                    }
                }
            }
        }
    }

    Ok(stats)
}
```

#### 2. `apply_membership_migration` (`src/membership_migration.rs:44-208`)
```rust
/// Apply repository blob membership backfill from authoritative tagged manifests.
/// Resumes from previous checkpoint if interrupted.
pub async fn apply_membership_migration(
    storage: &(impl BlobUploadCoordinatorStoragePort + ?Sized),
) -> Result<MigrationStats, StorageError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let my_owner_id = uuid::Uuid::new_v4().to_string();

    let mut checkpoint = match storage.get_migration_checkpoint().await? {
        Some(existing) => {
            if existing.phase == MigrationPhase::Ready {
                return Ok(existing.stats);
            }
            // Check lease
            if let (Some(owner), Some(expiry)) =
                (&existing.owner_id, existing.lease_expiry_unix_secs)
            {
                if now < expiry && owner != &my_owner_id {
                    return Err(StorageError::conflict(format!(
                        "concurrent migrator {owner} holds active lease until {expiry}"
                    )));
                }
            }
            MigrationCheckpointRecord {
                schema_version: 1,
                phase: MigrationPhase::Applying,
                owner_id: Some(my_owner_id.clone()),
                lease_expiry_unix_secs: Some(now + LEASE_DURATION_SECS),
                source_continuation_token: existing.source_continuation_token,
                current_repository: None,
                current_cursor: None,
                stats: existing.stats,
                started_unix_secs: existing.started_unix_secs,
                last_updated_unix_secs: now,
                failure_info: None,
                verification_result: None,
            }
        }
        None => MigrationCheckpointRecord {
            schema_version: 1,
            phase: MigrationPhase::Applying,
            owner_id: Some(my_owner_id.clone()),
            lease_expiry_unix_secs: Some(now + LEASE_DURATION_SECS),
            source_continuation_token: None,
            current_repository: None,
            current_cursor: None,
            stats: MigrationStats::default(),
            started_unix_secs: now,
            last_updated_unix_secs: now,
            failure_info: None,
            verification_result: None,
        },
    };

    // Save initial Applying checkpoint
    storage.save_migration_checkpoint(&checkpoint).await?;

    let mut repos = storage.list_repositories().await?;
    repos.sort();
    checkpoint.stats.repositories_scanned = repos.len();

    for repo in &repos {
        // Skip already completed repositories based on deterministic sorted continuation cursor
        if let Some(ref last_completed) = checkpoint.source_continuation_token {
            if repo <= last_completed {
                continue;
            }
        }

        // Set current repository cursor
        let canonical_repo = crate::registry::canonical_name::CanonicalRepoName::parse(repo)
            .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
        checkpoint.current_repository = Some(canonical_repo);
        let cur_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        checkpoint.last_updated_unix_secs = cur_time;
        checkpoint.lease_expiry_unix_secs = Some(cur_time + LEASE_DURATION_SECS);
        storage.save_migration_checkpoint(&checkpoint).await?;

        let tags = storage.list_tags(repo).await.unwrap_or_default();
        for tag in tags {
            if let Ok(manifest_digest) = storage.resolve_tag(repo, &tag).await {
                checkpoint.stats.manifests_scanned += 1;
                let (_meta, bytes) = storage.get_manifest(repo, &manifest_digest).await?;
                let refs = parse_manifest_refs(&bytes).map_err(|e| {
                    checkpoint.phase = MigrationPhase::Failed;
                    let mut err_msg = format!("corrupt manifest {manifest_digest} in {repo}: {e}");
                    err_msg.truncate(512);
                    checkpoint.failure_info = Some(err_msg);
                    let _ = storage.save_migration_checkpoint(&checkpoint);
                    StorageError::corrupt_data(format!(
                        "corrupt manifest {manifest_digest} in repo {repo}: {e}"
                    ))
                })?;
                for blob_d in refs.blob_references() {
                    match storage.get_repo_blob_membership(repo, blob_d).await? {
                        Some(_) => {
                            checkpoint.stats.memberships_already_present += 1;
                        }
                        None => {
                            let canonical_repo =
                                crate::registry::canonical_name::CanonicalRepoName::parse(&repo)
                                    .map_err(|e| StorageError::InvalidRepoName(e.to_string()))?;
                            let record = RepoBlobMembershipRecord::new_migration(
                                canonical_repo,
                                blob_d.clone(),
                            );
                            storage.link_repo_blob(&record).await?;
                            checkpoint.stats.memberships_created += 1;
                        }
                    }
                }
            }
        }

        // Advance cursor and clear current repository
        checkpoint.source_continuation_token = Some(repo.clone());
        checkpoint.current_repository = None;
        let cur_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        checkpoint.last_updated_unix_secs = cur_time;
        checkpoint.lease_expiry_unix_secs = Some(cur_time + LEASE_DURATION_SECS);
        storage.save_migration_checkpoint(&checkpoint).await?;
    }

    // Phase: Verifying
    checkpoint.phase = MigrationPhase::Verifying;
    storage.save_migration_checkpoint(&checkpoint).await?;

    // Verify all memberships exist and point to valid CAS objects before marking Ready!
    let is_valid = verify_membership_migration(storage).await?;
    if !is_valid {
        checkpoint.phase = MigrationPhase::Failed;
        checkpoint.failure_info = Some(
            "membership verification failed: unlinked or missing CAS blobs detected".to_string(),
        );
        checkpoint.verification_result = Some(false);
        storage.save_migration_checkpoint(&checkpoint).await?;
        return Err(StorageError::corrupt_data(
            "membership verification failed after apply; not all referenced blobs have durable records",
        ));
    }

    // Mark ready only after full verification passes
    storage.mark_membership_ready().await?;
    checkpoint.phase = MigrationPhase::Ready;
    checkpoint.verification_result = Some(true);
    checkpoint.owner_id = None;
    checkpoint.lease_expiry_unix_secs = None;
    checkpoint.current_repository = None;
    checkpoint.current_cursor = None;
    checkpoint.last_updated_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    storage.save_migration_checkpoint(&checkpoint).await?;

    Ok(checkpoint.stats)
}
```

#### 3. `verify_membership_migration` (`src/membership_migration.rs:211-241`)
```rust
/// Verify that all repository-referenced blobs have durable membership records and exist in CAS.
pub async fn verify_membership_migration(
    storage: &(impl BlobUploadCoordinatorStoragePort + ?Sized),
) -> Result<bool, StorageError> {
    let repos = storage.list_repositories().await?;
    for repo in &repos {
        let tags = storage.list_tags(repo).await.unwrap_or_default();
        for tag in tags {
            if let Ok(manifest_digest) = storage.resolve_tag(repo, &tag).await {
                if let Ok((_meta, bytes)) = storage.get_manifest(repo, &manifest_digest).await {
                    if let Ok(refs) = parse_manifest_refs(&bytes) {
                        for blob_d in refs.blob_references() {
                            let membership = storage.get_repo_blob_membership(repo, blob_d).await?;
                            let Some(record) = membership else {
                                return Ok(false);
                            };
                            // Verify repository and digest match
                            if record.repo != *repo || record.digest != *blob_d {
                                return Ok(false);
                            }
                            // Verify CAS blob exists globally
                            if storage.head_blob(blob_d).await.is_err() {
                                return Ok(false);
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(true)
}
```

### 2.2 Checkpoint Model & Storage Persistence

`MigrationPhase` and `MigrationCheckpointRecord` are defined in `src/storage/repo_membership.rs:228-267`:
```rust
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MigrationPhase {
    Uninitialized,
    Planning,
    Applying,
    Verifying,
    Ready,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationCheckpointRecord {
    pub schema_version: u32,
    pub phase: MigrationPhase,
    pub owner_id: Option<String>,
    pub lease_expiry_unix_secs: Option<u64>,
    pub source_continuation_token: Option<String>,
    pub current_repository: Option<CanonicalRepoName>,
    pub current_cursor: Option<String>,
    pub stats: MigrationStats,
    pub started_unix_secs: u64,
    pub last_updated_unix_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_info: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_result: Option<bool>,
}
```

Durable persistence is backend-specific:
- **Filesystem (`src/storage/fs.rs:3327-3360`):**
  - Read: `self.root.join("meta").join("migration_checkpoint.json")`. Returns `Ok(None)` if `NotFound`.
  - Write: `write_atomic_file(&path, &bytes).await?` into `meta/migration_checkpoint.json`.
- **S3 (`src/storage/s3.rs:3863-3895`):**
  - Read: `driver.get_object(bucket, "meta/migration_checkpoint.json")`. Returns `Ok(None)` if missing.
  - Write: `driver.put_object_conditional(bucket, "meta/migration_checkpoint.json", ...).await?`.

### 2.3 Lease Acquisition, Mutual Exclusion, and Check-Then-Save Limitations

1. **Duration & Owner Identity:**
   `const LEASE_DURATION_SECS: u64 = 60;` (`src/membership_migration.rs:8`).
   Every invocation of `apply_membership_migration` generates a completely new random UUID:
   `let my_owner_id = uuid::Uuid::new_v4().to_string();` (line 52).
   There is **no API parameter** or mechanism to pass an existing owner ID into `apply_membership_migration`.
2. **Mutual Exclusion Check:**
   ```rust
   if let (Some(owner), Some(expiry)) = (&existing.owner_id, existing.lease_expiry_unix_secs) {
       if now < expiry && owner != &my_owner_id {
           return Err(StorageError::conflict(format!(
               "concurrent migrator {owner} holds active lease until {expiry}"
           )));
       }
   }
   ```
3. **Non-Atomic Check-Then-Save Architecture:**
   The current locking model is **not** an atomic distributed compare-and-swap (CAS):
   - In `FsStorage`: `write_atomic_file(&path, &bytes)` creates a temporary file and renames it over `meta/migration_checkpoint.json`. While POSIX rename guarantees atomic directory entry replacement (readers never see partial writes), it does **not** check the prior file content or version.
   - In `S3Storage`: `save_migration_checkpoint` calls:
     ```rust
     self.driver.put_object_conditional(bucket, &key, Bytes::from(bytes), None, None).await?;
     ```
     Arguments 4 and 5 (`if_match` and `if_none_match`) are both `None`. This is an **unconditional S3 PUT**.
   - **Limitation:** Two competing migrators executing concurrently against an uninitialized or expired checkpoint can both read the expired checkpoint, both evaluate `now >= expiry`, and both write their own checkpoint. The last write silently wins.
   - Redesigning distributed mutual exclusion (e.g. conditional CAS etag writes) is outside the scope of this bounded error-hardening slice; this limitation is recorded as current reality.

### 2.4 Application & CLI Callers

The migration functions are invoked through `MaintenanceRuntime` in `src/cli/runtime.rs:394-419`:
```rust
pub async fn migrate_membership_plan(&self) -> Result<MigrationStats, CliError> {
    plan_membership_migration(self.storage_wiring.blob_ref_index().as_ref())
        .await
        .map_err(CliError::Storage)
}

pub async fn migrate_membership_apply(&self) -> Result<MigrationStats, CliError> {
    let stats = apply_membership_migration(self.storage_wiring.blob_mutation().as_ref())
        .await
        .map_err(CliError::Storage)?;

    self.storage_wiring
        .membership_reader()
        .mark_membership_ready()
        .await
        .map_err(CliError::Storage)?;

    Ok(stats)
}

pub async fn migrate_membership_verify(&self) -> Result<bool, CliError> {
    verify_membership_migration(self.storage_wiring.blob_mutation().as_ref())
        .await
        .map_err(CliError::Storage)
}
```

In `src/cli/mod.rs:494-539`, `MigrateMembershipCommand::{Plan, Apply, Verify}` call these methods and execute `runtime.finalize_with_result(res).await?`, which releases distributed mutation authority and local filesystem locks before process termination.

---

## 3. Current Behavior & Failure Dynamics

### 3.1 Tag-Listing Call Sites & Error Handling

There are three calls to `storage.list_tags(repo)` in `src/membership_migration.rs`:
- Line 19 in `plan_membership_migration`
- Line 127 in `apply_membership_migration`
- Line 216 in `verify_membership_migration`

All three call sites execute `.unwrap_or_default()`.

#### Behavior Under Errors:
1. **`StorageError::NotFound`:** Swallowed into `Vec::new()`.
2. **Non-`NotFound` Errors (`Io`, `PermissionDenied`, `Backend`, `CorruptData`):** Swallowed into `Vec::new()`.
3. **Discovery Treated as Empty:** Yes, every error is silently treated as an empty tag vector for that repository.

### 3.2 State and Mutation Progression in `apply_membership_migration`

When `apply_membership_migration` processes repositories:
1. **Prior Repositories:** Any repository lexicographically preceding the current repository has had its tags scanned and its memberships linked in storage via `storage.link_repo_blob(&record).await?`.
2. **Current Repository Before Listing:**
   - The checkpoint is saved with:
     - `current_repository = Some(canonical_repo)`
     - `source_continuation_token = Some(last_completed_repo)` (unchanged)
     - `phase = MigrationPhase::Applying`
3. **Current Repository During Listing Failure:**
   - `storage.list_tags(repo)` fails (e.g. `Io` or `Backend`).
   - `.unwrap_or_default()` returns `vec![]`.
   - The inner loop `for tag in tags` does not execute.
   - Zero manifests are resolved; zero blob memberships are linked for `repo`.
   - **Membership Invariant:** Skipping backfill does **not** unlink previously linked memberships. Existing memberships for earlier repositories remain intact.
4. **Continuation Token Advance:**
   - Lines 164-172 execute immediately after the tag loop:
     ```rust
     checkpoint.source_continuation_token = Some(repo.clone());
     checkpoint.current_repository = None;
     checkpoint.last_updated_unix_secs = cur_time;
     checkpoint.lease_expiry_unix_secs = Some(cur_time + LEASE_DURATION_SECS);
     storage.save_migration_checkpoint(&checkpoint).await?;
     ```
   - **The continuation token advances past `repo` despite the listing failure.**

### 3.3 Subsequent Invocation & Cursor Behavior

If `apply_membership_migration` runs again after an interruption or error:
1. **Cursor Skipping:** The loop checks:
   ```rust
   if let Some(ref last_completed) = checkpoint.source_continuation_token {
       if repo <= last_completed {
           continue;
       }
   }
   ```
   The application cursor skips previously completed names on subsequent application passes.
2. **Restoration Boundaries:**
   - The skip on subsequent application passes does **not** mean memberships are permanently unrecoverable by other means. Verification still visits all repositories reported by `storage.list_repositories()`, and an explicit administrative reset/repair of the continuation token can restore missing memberships.
3. **Statistics & Idempotency:**
   - Re-evaluating a repository does not duplicate membership records because `link_repo_blob` is idempotent.
   - However, counters in `MigrationStats` (`manifests_scanned`, `memberships_already_present`) are additive and cumulative. They are **not** exactly-once counters.

---

## 4. Proposed Narrow Hardening Contract

### 4.1 Planning Contract (`plan_membership_migration`)

```rust
let tags = match storage.list_tags(repo).await {
    Ok(t) => t,
    Err(StorageError::NotFound) => Vec::new(),
    Err(e) => return Err(e),
};
```
- **Policy (Approval Pending):**
  - `StorageError::NotFound`: Treated as empty tags (`Vec::new()`).
  - Non-`NotFound` Errors: Propagate immediately as `Err(StorageError)`.
  - *Rationale:* Planning performs zero writes. Halting on I/O or backend errors prevents false zero-tag reports.

### 4.2 Application Contract (`apply_membership_migration`)

When `storage.list_tags(repo)` returns an error:
1. **`StorageError::NotFound` Policy (Approval Pending):**
   - Treated as empty tags (`Vec::new()`).
   - *Note:* Missing `tags/` in filesystem storage already returns `Ok(Vec::new())`. A returned `StorageError::NotFound` from `list_tags` occurs only when the repository directory itself is absent from disk (`tokio::fs::metadata` error). This is a distinct policy proposal, not automatically approved by the manifest lifecycle compatibility decision.
2. **Non-`NotFound` Error Handling:**
   - **Do NOT advance continuation cursor:** `checkpoint.source_continuation_token` remains set to `last_completed` (the previously completed repository).
   - **Record Failure State:**
     - `checkpoint.phase = MigrationPhase::Failed;`
     - `checkpoint.current_repository = Some(canonical_repo);`
   - **UTF-8 Safe Diagnostic Bounding (Max 512 Bytes):**
     - Bounding must operate on UTF-8 byte boundaries, not arbitrary character truncation. Rust's `String::truncate` panics if the index falls inside a multibyte sequence.
     - Specification:
       ```rust
       let mut err_msg = format!("tag listing failed for repo {repo}: {e}");
       if err_msg.len() > 512 {
           let mut boundary = 512;
           while !err_msg.is_char_boundary(boundary) {
               boundary -= 1;
           }
           err_msg.truncate(boundary);
       }
       checkpoint.failure_info = Some(err_msg);
       ```
     - Preserves valid UTF-8 and guarantees `err_msg.len() <= 512` bytes.
   - **Secondary Checkpoint-Save Failure Handling Policy:**
     - The failure checkpoint save must be explicitly awaited:
       ```rust
       if let Err(save_err) = storage.save_migration_checkpoint(&checkpoint).await {
           tracing::warn!(
               repo = %repo,
               listing_error = %e,
               checkpoint_save_error = %save_err,
               "failed to persist migration failure checkpoint; returning original listing error"
           );
       }
       ```
     - *Concrete Behavior:*
       1. Await the failure checkpoint save.
       2. Return the original listing error `Err(e)` to the caller.
       3. Emit a diagnostic containing **both** the original listing error and the secondary checkpoint-save error.
       4. Do **not** claim `Failed` was persisted if the save failed.
       5. If saving the failure checkpoint fails, storage retains the last successfully persisted checkpoint: `phase: MigrationPhase::Applying`, `current_repository: Some(canonical_repo)`, `source_continuation_token: Some(last_completed)`.
   - **Lease & Expiry Decision (Approval Pending):**
     - *Proposal:* Preserve existing lease expiry (`checkpoint.lease_expiry_unix_secs` left unchanged from the last renewal before failure).
     - *Mechanics:* The lease is **not** renewed upon failure. It expires naturally at `last_updated_unix_secs + LEASE_DURATION_SECS`.
     - *Immediate Retry Consequence:* Because every `apply_membership_migration` generates a fresh UUID, any immediate retry before expiry will be rejected with `StorageError::Conflict`. Retry can only proceed once the lease expires.
   - **Completed Work Preserved:**
     - Memberships created for prior repositories remain durable.
   - **Return Error:** Propagate `Err(e)`.

### 4.3 Verification Contract (`verify_membership_migration`)

```rust
let tags = match storage.list_tags(repo).await {
    Ok(t) => t,
    Err(StorageError::NotFound) => Vec::new(),
    Err(e) => return Err(e),
};
```
- **Policy (Approval Pending):**
  - `StorageError::NotFound`: Treated as empty tags (`Vec::new()`).
  - Non-`NotFound` Errors: Immediately returned as `Err(StorageError)`.

### 4.4 Distinct Failure Behavior: Application vs Verification Failure

It is critical to distinguish an error during application from an error during verification:

1. **Failure During Application (`apply_membership_migration`):**
   - Tag listing error halts repository traversal.
   - `source_continuation_token` is **not** advanced past the failed repository.
   - On retry after lease expiry, the migrator resumes at the failed repository.
2. **Failure During Verification (`verify_membership_migration`):**
   - In `apply_membership_migration`:
     ```rust
     checkpoint.phase = MigrationPhase::Verifying;
     storage.save_migration_checkpoint(&checkpoint).await?;

     let is_valid = verify_membership_migration(storage).await?;
     ```
   - If `verify_membership_migration` returns `Err(StorageError)`:
     - The `?` operator returns immediately with `Err`.
     - `storage.mark_membership_ready().await?` is **not** called.
     - The failure block (lines 182-191) setting `phase = Failed` is **not** executed.
     - On storage, the checkpoint remains `MigrationPhase::Verifying`.
     - Crucially: `source_continuation_token` was **already advanced** to the final repository during the application loop!
     - On a subsequent invocation after lease expiry, the migrator loads the checkpoint, sees `source_continuation_token` matching the final repository, skips the application loop entirely, and re-enters verification directly.
     - **Verification failure does NOT re-run backfills for any repository** unless an explicit repair resets the continuation token.

---

## 5. Adjacent Limitations & Scope Boundaries

### 5.1 Documented Adjacent Suppression Sites

Inspection of `src/membership_migration.rs` reveals adjacent error suppression patterns:

1. **`resolve_tag` in `plan_membership_migration` (`line 21`):**
   `if let Ok(manifest_digest) = storage.resolve_tag(repo, &tag).await` silently skips unresolvable tags.
2. **`resolve_tag` in `apply_membership_migration` (`line 129`):**
   `if let Ok(manifest_digest) = storage.resolve_tag(repo, &tag).await` silently skips unresolvable tags without backfilling their memberships.
3. **Triple Suppression in `verify_membership_migration` (`lines 218-220`):**
   `resolve_tag`, `get_manifest`, and `parse_manifest_refs` all use `if let Ok(...)`. Unreadable or corrupt manifests are silently ignored during verification, potentially yielding `Ok(true)`.
4. **Un-Awaited Checkpoint Save (`line 137`):**
   `let _ = storage.save_migration_checkpoint(&checkpoint);` discards the async Future without `.await`.
5. **Byte-Unsafe Truncation in Parse Error (`line 135`):**
   `err_msg.truncate(512);` truncates arbitrary characters, risking a panic if byte 512 falls within a UTF-8 multibyte sequence.

### 5.2 Independence & Scope Boundaries

- **Why Tag-Listing Hardening is Independently Valuable:** Tag listing is the root directory traversal. If listing fails, an entire repository's tags are omitted at once. Hardening tag listing prevents whole-repository omission and prevents false continuation token advancement.
- **Boundary:** Modifying `resolve_tag`, `get_manifest`, parse suppression, or locking architecture remains outside this bounded slice.

---

## 6. Concrete Acceptance Tests

### 6.1 Existing Tests (Exact Names)
- `test_s3_migration_plan_performs_zero_writes` (`src/storage/s3/tests.rs:3323`)
- `test_s3_migration_conditional_state_acquisition_and_lease_renewal` (`src/storage/s3/tests.rs:3341`)
- `test_s3_migration_concurrent_owner_rejection` (`src/storage/s3/tests.rs:3357`)
- `test_s3_migration_interrupted_apply_and_cursor_resume` (`src/storage/s3/tests.rs:3390`)
- `test_membership_migration_lifecycle_and_fail_closed_startup` (`tests/repository_membership_tests.rs:600`)
- `test_11_membership_migration_and_rebuild` (`tests/canonical_repo_grammar_tests.rs:898`)

### 6.2 Proposed Acceptance Tests

1. **`test_apply_immediate_retry_rejected_by_retained_active_lease`:**
   - Injects listing fault on repository during apply.
   - Apply fails and records failure, preserving active lease.
   - An immediate reinvocation generates a fresh UUID, observes active lease, and returns `Err(StorageError::Conflict)`.
2. **`test_apply_retry_resumes_after_controlled_lease_expiry`:**
   - Following failure, test fixture manipulates the persisted checkpoint's `lease_expiry_unix_secs` to a past timestamp (deterministic test control without sleeping 60 seconds).
   - Re-invocation acquires lease, skips completed repositories via `source_continuation_token`, processes the failed repository, and finishes.
3. **`test_apply_failure_after_completed_repository_preserves_cursor`:**
   - `repo-a` completes; `repo-b` fails listing.
   - Asserts `source_continuation_token == Some("repo-a")` (cursor NOT advanced to `repo-b`).
   - Asserts memberships for `repo-a` remain linked.
4. **`test_apply_secondary_checkpoint_save_failure_handling`:**
   - Tag listing fails; `save_migration_checkpoint` also fails when saving the failure record.
   - Initial and per-repo checkpoint saves succeed; only the failure-save is faulted.
   - Asserts function returns original listing error without panic.
   - Asserts storage retains last successfully persisted checkpoint (`Applying`).
5. **`test_verify_storage_error_preserves_verifying_phase_and_prevents_ready`:**
   - Verification encounters listing failure and returns `Err(StorageError)`.
   - Asserts `mark_membership_ready` is not called.
   - Asserts checkpoint remains `MigrationPhase::Verifying`.
6. **`test_diagnostic_multibyte_utf8_bounding`:**
   - Repository/error text containing multibyte UTF-8 characters crossing byte index 512.
   - Asserts sliced string is valid UTF-8, `<= 512` bytes, and does not panic.
7. **`test_not_found_policy_evaluations`:**
   - Tests `NotFound` handling separately across planning, application, and verification against proposed policies.

---

## 7. Recommended Smallest Implementation Slice

### 7.1 Files & Symbols to Modify
- **File:** `src/membership_migration.rs`
  - Function: `plan_membership_migration` (lines 18–20)
  - Function: `apply_membership_migration` (lines 127–130)
  - Function: `verify_membership_migration` (lines 216–218)
- **Tests:** Add integration tests in `tests/repository_membership_tests.rs`.

### 7.2 Compatibility Effects
- Forward compatible: Existing valid storage with uncorrupted tags experiences zero behavior change.
- Fail-closed: Corrupt or unreadable tag directories halt migration rather than advancing continuation tokens past unmigrated repositories.

### 7.3 Unresolved Decisions for Future Approval
1. **Lease Handling on Failure:** Whether to preserve existing lease expiry, renew it, or clear ownership upon failure.
2. **`NotFound` Policy:** Explicit approval needed for `NotFound` policies across planning, application, and verification.
3. **Adjacent Suppressions:** Whether adjacent `resolve_tag` and manifest retrieval error suppression should be addressed in this slice or a subsequent slice.

### 7.4 Verification Commands
- `cargo fmt --check`
- `cargo check --locked --all-targets --all-features`
- `cargo clippy --locked --all-targets --all-features -- -D warnings`
- `cargo test --locked --test repository_membership_tests`

---

## 8. Canonical Quality Gate Status

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
