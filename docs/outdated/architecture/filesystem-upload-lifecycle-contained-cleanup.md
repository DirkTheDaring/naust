# Contained Filesystem Upload Lifecycle & Coherent Cleanup

- **Document**: `docs/architecture/filesystem-upload-lifecycle-contained-cleanup.md`
- **Repository**: `registry-rust` (with coordinated additive primitives in `storage-layer-rust`)
- **Date**: 2026-09-13
- **Status**: IMPLEMENTED on `master` (`UploadAuthorities` / contained reaper). The original “NOT COMMITTED — NOT PUSHED” stamp is obsolete. Broader FS mutation cutover is `f555e5f`; this note is the upload-lifecycle behavior record.
- **Aligned HEAD:** `9405991` (2026-09-18). See [`current-state.md`](current-state.md).
- **Baseline HEADs (when this note was written):**
  - `registry-rust`: `00676c721fde2687196eececbc2cdb097bb1fd9c`
  - `storage-layer-rust`: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`
- **Canonical Quality Gates** (remain explicitly **OPEN** as acceptance criteria): `O-03`, `O-04`, `O-05`,
  `O-06`, `O-13`, `O-15`, `O-16`, `D-06`.

This document describes the **actual behavior** of the shipped implementation, not a
proposal. It reflects the corrected implementation: shared, stable per-subtree
authority; a supervised streaming owner under which no mutation survives lock
release; a real fault-injection test matrix; and honest accounting of the genuine
residual costs (which are limited to a stable-lock file that is never unlinked).

---

## 1. Summary

Every filesystem upload-lifecycle participant — session `create` / `status` /
`append` / `begin_finalize` / `commit_finalize` / `recover` / `abort`, the expiry
reaper, and the receipt / CAS / membership helpers they call — resolves its targets
beneath **one shared authority per lifecycle subtree**, held for the process
lifetime behind `Arc<UploadAuthorities>`. The storage **root** descriptor is pinned
once at construction; each subtree (`uploads`, `uploads/.finalized`, `blobs`,
`repo-memberships`) is pinned **exactly once, lazily, and then shared** across every
participant through a `tokio::sync::OnceCell`.

All lifecycle mutations run under the dependency's **owned locking boundary**
(`ContainedDir::run_locked`), which holds a stable `flock` on a per-session
`.lock.{uuid}` file across inspection, decision, and mutation, releasing it *inside*
the closure so the lock survives caller cancellation. Streaming operations
(`append_if_offset`, `begin_finalize`) run their **entire** inspect → recover →
append → commit/verify body inside `run_locked` on a `spawn_blocking` worker that
owns the lock; the async byte stream is fed to that worker over a bounded channel.
Cancelling the request future therefore never abandons an in-flight mutation with
the lock released: the worker either commits or rolls back to the committed offset
**while still holding the lock**, and only then releases it.

The implementation lives in `src/storage/fs.rs` (registry) on top of the additive
`storage_fs::mutate` primitives (`ContainedDir`, `BlockingDir`,
`ContainedLockGuard`, `OwnedFdHandle`, `FileName`, `run_locked`, `authority_id`, …)
in the dependency.

---

## 2. Shared, stable per-subtree authority

### 2.1 What is pinned, and when

`UploadAuthorities` holds the pinned storage **root** (`ContainedDir`) plus one
`OnceCell<ContainedDir>` per lifecycle subtree (`uploads`, `finalized`, `blobs`,
`memberships`). The root is captured synchronously in the `FsStorage` constructor
via `FsMetadataReader::open_contained_dir_sync("")` (no async runtime required), so
both the primary and proxy-cache synchronous construction sites in
`src/storage/mod.rs` (`try_new_with_all_limits` on each path) are wired identically.

No subtree directory is materialized at construction. The first operation that needs
a subtree runs `ensure_subdir` — beneath the pinned root, or, for `.finalized`,
beneath the shared `uploads` authority — and installs the resulting pinned
descriptor in the cell. This keeps construction **total**: a symlinked or occupied
storage area, or a read-only path that must remain free to reject, does not cause
construction to fail, and no directory is created until a real lifecycle operation
demands it.

### 2.2 Once-init and sharing contract

`OnceCell::get_or_try_init` gives four properties the lifecycle depends on:

1. **Shared success.** Once a subtree is pinned, every later call — from any task,
   on any `FsStorage` clone that shares this `Arc<UploadAuthorities>` — receives the
   *same cached descriptor*. Every access, lock, read, write, recovery, abort,
   receipt, and publication step flows through that one cached inode.
2. **No competing authorities under concurrency.** When many callers race the first
   use, exactly one initializer runs to success and every racer observes that one
   result. Concurrent initialization cannot install two different authorities.
3. **No silent reopen.** Because no accessor ever re-resolves the pathname after the
   cell is populated, a later rename/replacement of a subtree's *pathname* on disk
   cannot redirect any operation. The pinned descriptor follows the inode it was
   opened on.
4. **Explicit init failure, retryable.** An initialization failure (a symlink
   squatting the subtree name, a permission error, a missing parent) is surfaced to
   the caller and leaves the cell empty, so a subsequent attempt may retry. Failure
   is never cached as success.

`ContainedDir::authority_id()` exposes the stable per-authority identity used by the
tests in §6 to assert that separate lifecycle calls observe the same authority and
that a concurrent first-use race collapses to a single authority.

### 2.3 Guarantee scope

The sharing is **intra-instance**. All clones of one `FsStorage` (held behind an
`Arc`) share these cells, so every task in one process operating through one
`FsStorage` observes one coherent authority per subtree. Two independent `FsStorage`
instances — or two OS processes — each pin their own root and their own cells and do
**not** share cached descriptors. Cross-instance / cross-process mutual exclusion
rests solely on the stable on-disk `.lock.{uuid}` `flock` domain (a kernel lock
keyed by inode), not on any in-memory sharing.

### 2.4 Coherence under root/subtree replacement (the reaper)

Because inspection and every destructive action resolve through the same pinned
inode, a detached tree's expiry can never drive deletion of a same-name record in a
fresh replacement tree. The expiry reaper enumerates `{uuid}.meta.json` under the
shared `uploads` authority and processes each candidate as **one continuously-locked
owned operation**: it acquires the candidate's `.lock.{uuid}` **once** via `try_lock`
(a busy or held lock means a live participant owns the session — it is skipped), then
runs the fresh meta read, the expiry decision, and the per-state action inside a
single `run_locked` closure that holds that same lock throughout. An expired
`Finalizing` session is handed to recovery — it rolls forward if its CAS blob is
fully published and is otherwise left intact for later completion, **never aborted**;
any other expired state is aborted. The lock is **never dropped and reacquired**
between the check and the action, so no cooperating update can slip in between them.
The inner steps use locked helpers (`recover_session_locked` / `abort_session_locked`)
that receive the already-held authority rather than the public `recover_session` /
`abort_session` entry points (which would re-lock). All steps resolve through the
*same* shared authorities, so if the ambient `uploads` (or the storage root) is
renamed away and a fresh tree appears at the same pathname, the reaper continues to
resolve the **original, now-detached** inode for both inspection and any deletion, and
the fresh replacement's records survive.

The receipt-cleanup pass is likewise locked: it enumerates `{uuid}.json` under the
pinned `.finalized` authority and unlinks a past-TTL receipt only under the matching
`.lock.{uuid}`, **re-reading** the receipt under the lock. A busy lock (a live
same-UUID session), a receipt republished fresh, a receipt whose stored identity no
longer matches its leaf name, and a vanished receipt are each respected rather than
blindly deleted from a stale listing snapshot.

The reaper's returned `Result<usize, StorageError>` counts only *confirmed* cleanups
and distinguishes not-expired / absent / changed / busy outcomes from failures;
per-candidate failures are logged and skipped, and a fatal listing failure is
surfaced as `Err`.

Honoring a pathname replacement therefore requires re-deriving the authorities — a
process restart. This is not a weakened guarantee; it is the mechanism that makes
mid-flight replacement *impossible to adopt silently* (§7).

---

## 3. Supervised streaming owner

### 3.1 The problem it solves

An earlier design held the lock guard across the streaming `await` points and
relied on the *next* operation's recovery-on-entry to repair a tail left by a
cancelled stream. That left a window in which the request future could be cancelled,
the lock released, and an in-flight write only reconciled later. The corrected
design removes that window: **every outstanding mutation completes while the matching
lock is held, or is definitively rolled back before that lock is released.**

### 3.2 How it works

`append_if_offset` and `begin_finalize` run their entire inspect → recover-on-entry
→ append → (verify/persist) body inside `uploads.run_locked(guard, worker)` on a
`spawn_blocking` thread that owns the session lock. The async byte stream is not
moved into the worker; instead:

- A bounded `tokio::sync::mpsc` channel (`STREAM_CHANNEL_CAP = 16`) connects an async
  **feeder** (`feed_stream`) to the synchronous worker.
- The worker drains the channel with `blocking_recv()` inside `drain_append_blocking`,
  writing and hashing each chunk under the held lock. Terminal messages
  (`Finish` / `StreamError`) and channel closure (`None`) map to `DrainOutcome`
  variants: `Finished`, `StreamAborted`, and `Cancelled`.
- The request future awaits `tokio::join!(feed_stream(...), worker)`.

`run_locked` runs the worker on a detached `spawn_blocking` task: dropping the
`.await` on the request side does **not** abort the worker. So when the request
future is cancelled, the feeder is dropped, the channel closes without a terminal
message, and `drain_append_blocking` observes `None` and **rolls the data file back
to `committed_offset` via `set_len` while still holding the lock**, then returns.
The lock is released only after that rollback. No detached mutation and no released
lock ever coexist.

Both operations share this owner: `append_if_offset` always drains a stream;
`begin_finalize` drains only when a trailing stream is present, and additionally
performs digest verification and the `Finalizing` persist inside the same held-lock
body.

### 3.3 What a competing operation sees

Because the worker owns the lock for the full duration of the (possibly cancelled)
mutation, a competing append / finalize / abort / reaper action on the same session
is excluded until the original worker commits or completes its rollback. After a
cancelled append, the session's committed offset, on-disk length, and meta are
mutually consistent, and a fresh append resumes from the rolled-back offset. §6
lists the deterministic tests that pause a real operation at an in-flight boundary,
cancel it, and assert this exclusion and consistency for both append and finalize.

---

## 4. Lifecycle correctness decisions

- **Membership before receipt** (Issue A). `commit_finalize` and the `Finalizing`
  roll-forward write the `repo-memberships` record *before* the finalized receipt.
  At the instant a commit returns success both records are present. This ordering is
  a write sequence, **not** a standing invariant that an observed receipt always
  implies a durable membership: a receipt-authoritative replay does not rewrite the
  membership index, and a membership record may be reclaimed independently
  afterwards. A missing membership under a present receipt is tolerated and
  re-asserted idempotently by `recover_session`.
- **Unified receipt-lookup authority.** The public `get_finalized_receipt` resolves
  the receipt through the **same** pinned `.finalized` authority the writers
  (`commit_finalize`, the `Finalizing` roll-forward) publish through, rather than
  re-resolving `uploads/.finalized` from the root on each call. Reader and writer
  therefore stay in agreement after a `.finalized` or `uploads` pathname replacement;
  repository/UUID validation, missing-file (`Ok(None)`), and corrupt/error semantics
  are preserved.
- **Real roll-forward, not truncation** (Issue B). A `Finalizing` session is rolled
  *forward* only when the CAS blob is already fully published at the expected size,
  in which case membership + receipt are (re)written. It is never silently truncated
  back to a pre-finalize state.
- **Honest abort failure boundary, meta removed LAST** (Issue C). `abort_session`
  unlinks the data leaf, then every residual hash generation, and removes
  `{uuid}.meta.json` **last**. A genuine missing leaf is treated as idempotent
  absence; any *other* unlink failure (e.g. an EIO) is **propagated** to the caller
  and the meta is **preserved**, so an interrupted cleanup never reports success and
  the session stays recoverable — a later retry re-derives the same work and
  completes it. The reaper likewise counts only a fully successful abort: an
  incomplete abort is logged and the session is left intact, not counted
  (`test_fs_abort_data_unlink_failure_preserves_meta_and_retries`,
  `test_fs_abort_hash_unlink_failure_after_earlier_deletion_preserves_meta`,
  `test_fs_abort_tolerates_genuinely_missing_leaves`,
  `test_fs_reaper_does_not_count_incomplete_abort`). The residual hash generations to
  remove are **enumerated directly from disk** — a contained `BlockingDir::list`
  filtered to this session's own `{uuid}.hash.{n}` prefix
  (`session_hash_generations_by_scan`), run under the held `.lock.{uuid}` so no
  concurrent append can add a generation mid-abort. This replaces both the former
  fixed `0..100` range **and** the later `{G-1, G, G+1}` window derived from
  `meta.hash_generation`: **neither window is a sound bound**. The generation-advancing
  paths (`append_if_offset` and the `begin_finalize` trailing stream) write generation
  `G+1`, persist the meta at `G+1`, then clean the old `G` leaf with a *suppressed*,
  best-effort unlink (`let _ = dir.unlink(...)`). A real non-`ENOENT` failure there
  orphans `G` while the recorded generation keeps advancing on later appends, so a
  residual can sit arbitrarily far below `hash_generation - 1`. Only a direct scan is
  complete, and the same scan runs for every meta state — `Present`, `Corrupt`, **and
  `Absent`** (the absent-meta path previously removed only the data leaf and returned,
  orphaning any residual hash leaf forever). A later, sparse, or out-of-window
  generation is therefore removed rather than orphaned while the meta is deleted
  (`test_fs_abort_removes_sparse_later_hash_generations`,
  `test_fs_abort_removes_residual_generation_from_real_append_unlink_failure`,
  `test_fs_abort_removes_residual_generation_from_real_finalize_unlink_failure`,
  `test_fs_abort_absent_meta_still_removes_residual_hash`). A directory too large to
  enumerate under the list limits surfaces as an error rather than a silently
  truncated (incomplete) listing, so it is treated as a required-cleanup failure and
  propagated. This ordering-plus-error-boundary property holds within a live
  process; it is **not** a claim of guaranteed durability or recovery across
  arbitrary hardware crashes or power loss — no fsync/write-barrier ordering between
  the unlinks is asserted. The stable session lock file is intentionally never
  unlinked. (The suppressed old-hash unlink in the append/finalize commit paths, and
  the analogous single-generation cleanup in `commit_finalize`, are left as-is: they
  are best-effort space reclamation whose residuals this scan-based abort now fully
  reclaims; the on-disk generation counter and recovery remain correct regardless.)
- **Non-destructive expiry for `Finalizing` sessions** (Issue C, baseline preserved).
  The reaper *attempts recovery* for an expired `Finalizing` session but **never
  aborts it**. A fully-published CAS blob rolls forward (a completed finalization) and
  counts as a cleanup; a not-yet-published, size-mismatched, or invalidly-described
  finalization stays intact and available for later completion — it is diagnosed, not
  counted, and its staging data + meta survive. A corrupt/failed recovery is likewise
  diagnosed and never authorizes deletion. Containment did not introduce a new
  destructive expiry policy; any future "expire and delete incomplete finalizing
  sessions" behaviour would be a separate proposal. Asserted by
  `test_fs_reaper_leaves_pending_finalizing_with_cas_absent`,
  `test_fs_reaper_leaves_finalizing_with_cas_size_mismatch`,
  `test_fs_reaper_leaves_finalizing_with_invalid_finalizing_info`,
  `test_fs_reaper_finalizing_publication_then_retry_rolls_forward`, and
  `test_fs_reaper_rolls_forward_published_finalizing_with_accurate_count`.
- **Receipt-authoritative replay vs. recovery roll-forward.** A `commit_finalize`
  replay that finds an existing receipt reports the finalization from the receipt
  and does **not** rewrite the membership index; re-asserting a missing membership is
  the job of `recover_session` (which sees the `Finalizing` meta + published CAS
  blob). This split is deliberate and is asserted by the matrix in §6.
- **Explicit confirmed-cleanup counting**. The reaper's returned
  `Result<usize, StorageError>` counts only *confirmed* cleanups (a recovery or abort
  that returned `Ok`, or a confirmed receipt unlink), skips fresh and live-locked
  candidates, continues across per-candidate errors, and surfaces a fatal
  directory-listing failure as `Err`.
- **Idempotent partial-publication retry**. If `commit_finalize` finds the staging
  meta gone but the CAS blob already published at the expected size, it reports
  `AlreadyFinalized` and (re)asserts the membership and receipt rather than a
  spurious `NotFound`. A wrong-size CAS destination with a missing source is a
  genuine error, not a spurious success.
- **O_EXCL temp creation / leaf-type validation**. Atomic writes create their temp
  with `O_EXCL` and clean up honestly on failure; leaf reads reject non-regular
  files (FIFOs, directories, symlinks) rather than following or blocking on them.

---

## 5. The retained session lock file (honest accounting)

The `.lock.{uuid}` file is **never unlinked** — not by `abort_session`, not by the
reaper, not by `commit_finalize`. This is what makes the lock domain *stable*: a
racing acquirer can never observe a recreated lock inode, so the `flock` exclusion is
sound for the process lifetime and across processes.

The honest consequence is **unbounded accumulation**: one zero-byte `.lock.{uuid}`
file persists per session ever created, indefinitely. This is a genuine, accepted
tradeoff of the stable-lock design — the one place in this change where a real cost
is paid — not an oversight. A future out-of-band sweep (safe only when it can prove
no live authority references a given lock inode) could reclaim them; the lifecycle
code deliberately does not, because in-band unlinking would reintroduce the
recreated-inode race the stable domain exists to eliminate.

`test_fs_session_lock_file_retained_through_abort_and_reaper` asserts the lock file
survives both `abort_session` and the reaper with its inode unchanged.

---

## 6. Test coverage

Dependency primitives are covered by `crates/storage-fs/tests/mutate_contained.rs`
(containment, bounded reads, inspection, stable locking with retained lock file, the
owned blocking boundary, and a real Tokio caller-cancellation proof).

Registry lifecycle coverage lives in `src/storage/fs/tests.rs` and
`src/storage/fs/upload_quarantine_read.rs`. Beyond the pre-existing crash-recovery,
reaper, digest-mismatch, restart-resume, and malformed-record suites:

**Shared-authority (§2).**

- `test_fs_authority_shared_across_separate_lifecycle_calls` — separate calls observe
  the same `authority_id` per subtree; `uploads` ≠ `finalized`.
- `test_fs_authority_concurrent_lazy_init_single_authority` — 32 racing tasks
  (multi-threaded runtime) collapse to one authority.
- `test_fs_authority_uploads_replacement_beneath_unchanged_root` — after `uploads` is
  renamed away and recreated, the operation lands in the detached (pinned) inode, not
  the fresh ambient one.
- `test_fs_authority_finalized_replacement_beneath_unchanged_root` — a replay finds
  the receipt through the pinned `.finalized` after its pathname is replaced.
- `test_fs_authority_uploads_replacement_between_lock_and_mutation` — replacing
  `uploads` *after* lock acquisition but *before* the commit still lands in the pinned
  inode.
- `test_fs_upload_authority_wired_through_primary_and_proxy_cache_construction` — both
  production factories construct, and a concrete `FsStorage` runs a full lifecycle.
- `test_real_reaper_root_replacement_acts_only_on_current_tree` — pinned-authority
  coherence under **root** replacement (existing regression).

**Supervised streaming owner (§3).**

- `test_fs_append_cancellation_excludes_competing_until_rolled_back` — a paused append
  is cancelled; a competing append is excluded until the owner rolls back, then
  completes consistently (offset/on-disk length/finalize digest all agree).
- `test_fs_finalize_cancellation_excludes_competing_until_rolled_back` — same, for a
  `begin_finalize` trailing stream.
- `test_fs_append_cancellation_rolls_back_under_lock` — with no competing consumer,
  the data file is deterministically observed back at the committed offset and the
  session remains `Active`, resumable by a fresh append.

**Fault-injection matrix (§4).**

- `test_fs_reaper_counts_only_confirmed_cleanups` — 2 aborted expired sessions + 2
  past-TTL receipt unlinks = count 4; fresh session, fresh receipt, and a live-locked
  expired session are skipped.
- `test_fs_commit_finalize_cas_mismatch_is_error` — source gone + wrong-size CAS
  destination fails the publish and writes no receipt.
- `test_fs_commit_finalize_tolerates_prior_cas_publication` — correct-size CAS + gone
  source rolls forward to `Published` with membership + receipt.
- `test_fs_commit_finalize_retry_heals_membership_and_receipt` — CAS present, all
  staging/index records gone: retry reports `AlreadyFinalized` and heals both.
- `test_fs_commit_finalize_retry_restores_lost_receipt` — a lost receipt is restored
  on retry (membership already present).
- `test_fs_commit_finalize_replay_is_receipt_authoritative` — a receipt-present replay
  does **not** rewrite a dropped membership (documents the split with recovery).
- `test_fs_recover_session_rolls_forward_membership_and_receipt` — recovery re-asserts
  both from a `Finalizing` meta + published CAS blob.
- `test_fs_abort_session_retry_after_partial_interruption` — an abort interrupted
  after the data unlink but before meta removal (modelled by removing the data leaf
  while leaving the meta) is completed idempotently on retry. This establishes that
  the *meta-last* ordering leaves a still-recoverable session at that specific
  boundary; it does **not** claim guaranteed recovery across arbitrary hardware
  crashes or power loss (no fsync/write-barrier ordering is asserted — see §4).

**Abort failure boundary (honest cleanup, §4).** Faults injected at the dependency's
`Unlink` fault point via the same opt-in `fault-injection` dev seam.

- `test_fs_abort_data_unlink_failure_preserves_meta_and_retries` — an EIO on the
  staging-data unlink surfaces through the public abort, preserves the meta + data +
  hash leaves (session stays recoverable), and a retry after clearing the fault
  completes the abort.
- `test_fs_abort_hash_unlink_failure_after_earlier_deletion_preserves_meta` — with
  stray hash generations around the recorded generation, the abort deletes the data
  leaf and the earlier generations, then fails on a later generation's unlink: the
  meta is preserved and only the failed generation remains; a retry heals it.
- `test_fs_abort_tolerates_genuinely_missing_leaves` — already-absent data + hash
  leaves are idempotent absence, not a failure, and the abort still removes the meta.
- `test_fs_abort_removes_sparse_later_hash_generations` — a session whose recorded
  generation is far above the former fixed `0..100` bound has its residual leaves
  removed, not orphaned while the meta is deleted.
- `test_fs_abort_removes_residual_generation_from_real_append_unlink_failure` — the
  **production out-of-window boundary**: `FaultPoint::Unlink` makes an append's
  suppressed old-hash cleanup fail while the append still commits; a further append
  advances the recorded generation to 2, leaving `hash.0` orphaned at a generation
  *below* `hash_generation - 1` (outside any `{G-1, G, G+1}` window). The scan-based
  abort removes every data/hash/meta leaf for the UUID, proving cleanup after a real
  old-hash unlink failure.
- `test_fs_abort_removes_residual_generation_from_real_finalize_unlink_failure` — the
  same suppressed-cleanup mechanism on the `begin_finalize` trailing-stream path:
  begin_finalize commits (digest matches) but its old-hash unlink fails, orphaning a
  residual that the scan-based abort of the (unpublished) `Finalizing` session removes.
- `test_fs_abort_absent_meta_still_removes_residual_hash` — with the meta already
  gone, the abort still scans and removes the residual `hash.0` (and the data leaf),
  staying contained to the UUID — the old absent-meta path removed only the data leaf.
- `test_fs_reaper_does_not_count_incomplete_abort` — a reaper abort whose data unlink
  fails is logged and skipped: the count stays 0 and the session survives, so an
  incomplete abort is never a confirmed cleanup.

**Non-destructive `Finalizing` expiry policy (§4, baseline preserved).** Driven
through the real `reap_expired_sessions`.

- `test_fs_reaper_leaves_pending_finalizing_with_cas_absent` — an expired finalizing
  session with staging data present and CAS absent is left intact (count 0, meta +
  data survive, no receipt fabricated).
- `test_fs_reaper_leaves_finalizing_with_cas_size_mismatch` — a CAS blob present at
  the wrong size does not roll forward; the session survives, count 0.
- `test_fs_reaper_leaves_finalizing_with_invalid_finalizing_info` — invalid finalizing
  information does not authorize cleanup; the session survives, count 0.
- `test_fs_reaper_finalizing_publication_then_retry_rolls_forward` — a session left
  pending on one pass rolls forward on the next once its CAS blob is published (count
  transitions 0 → 1; receipt appears), proving the pending state stays completable.
- `test_fs_reaper_rolls_forward_published_finalizing_with_accurate_count` — a
  published + expired finalizing session is counted exactly once while a fresh
  finalizing session is skipped.

**Reaper lock-gap and receipt-locking regressions (§2, §3).**

- `test_fs_reaper_locked_boundary_excludes_cooperating_update` — a cooperating update
  attempted at the former inspect/act boundary blocks on the continuously-held session
  lock and, once released, observes the session as already reaped (it cannot slip
  between the locked expiry check and the destructive action).
- `test_fs_reaper_honors_update_completed_before_locked_check` — an update that
  completed before the reaper's locked read refreshes `last_active`; the reaper reads
  it freshly under the lock and leaves the session in place.
- `test_fs_reaper_receipt_busy_lock_is_skipped`,
  `test_fs_reaper_receipt_concurrent_publication_is_respected`,
  `test_fs_reaper_receipt_changed_identity_is_preserved`,
  `test_fs_reaper_receipt_disappearance_is_absent`,
  `test_fs_reaper_receipt_fresh_same_uuid_is_preserved` — receipt cleanup happens only
  under the matching `.lock.{uuid}`, re-reading the receipt under the lock so a busy
  lock, a concurrent republication, a changed stored identity, a disappearance, and a
  fresh same-UUID receipt are each respected.
- `test_fs_get_finalized_receipt_after_finalized_replacement_uses_pin`,
  `test_fs_get_finalized_receipt_after_uploads_replacement_uses_pin` — the public
  `get_finalized_receipt` resolves through the same pinned `.finalized` authority the
  writers publish through, agreeing with commit and recovery after a `.finalized` or
  `uploads` pathname replacement.

**Deterministic atomic-write failure semantics (§4).** Injected through the opt-in
`storage_fs::mutate::fault` seam, which is compiled into the dependency **only** via
the registry's dev-dependency feature (`fault-injection`); production builds never
contain it.

- `test_fs_atomic_primary_write_failure_surfaces_and_leaves_no_destination` — an
  `ENOSPC` on the temp write surfaces an error and publishes no destination.
- `test_fs_atomic_rename_failure_preserves_prior_destination` — an `EIO` on the
  publish rename surfaces an error, preserves the prior destination byte-for-byte, and
  leaves no temp residual.
- `test_fs_atomic_secondary_cleanup_failure_surfaces` — a rename failure whose temp
  cleanup also fails surfaces an error (the `CleanupFailed` combination is not
  swallowed) with the prior destination intact.
- `test_fs_atomic_write_failure_retry_heals` — once the transient fault clears, a
  retry completes and publishes the record.
- `test_fs_commit_finalize_membership_write_failure_is_surfaced` — a write fault on
  the membership record during `commit_finalize` fails the finalize and publishes no
  receipt, demonstrating end-to-end honest propagation rather than silent success.

The full `registry-rust` library test suite passes; the dependency suite passes in
full. Exact commands and counts are recorded in the evidence package.

---

## 7. Intended consequence: restart to honor a replacement

Because the authorities follow their pinned inodes, an operator who *intends* to
replace the storage tree at the same pathname (e.g. swapping in a restored volume by
rename) will find the running process still operating on the old, detached inode.
The replacement is honored only after the authorities are re-derived — a **process
restart**.

This is stated as an operational fact, not a limitation dressed up as a feature: the
same property that requires a restart to adopt a replacement is exactly the property
that prevents a detached tree's expiry from driving deletion in a fresh tree (§2.4),
and prevents a mid-flight pathname swap from silently redirecting an in-progress
lock/read/mutation. Adopting a replacement is therefore always a deliberate, explicit
act, never an implicit reopen.

---

## 8. Scope boundary: membership candidate helpers

`set_membership_candidate`, `clear_membership_candidate`, and `unlink_repo_blob`
remain on the ambient `repo_blob_path` layout using `tokio::fs` / `write_atomic_file`.
They resolve to the **same on-disk records** as the pinned `repo-memberships`
authority used by `link_repo_blob` (`repo-memberships/by-repo/{key}/{algo}/{hex}.json`),
so this is a routing boundary, not a layout divergence. Bringing them onto the pinned
authority is a mechanical follow-up outside the coherence-critical path and is left
explicitly out of scope for this change.
