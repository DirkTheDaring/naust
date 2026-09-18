# Filesystem Upload-Lifecycle Contained Cleanup — Design and Prototype

> **Historical design package.** Production now runs the contained reaper / `UploadAuthorities` path documented in [`filesystem-upload-lifecycle-contained-cleanup.md`](filesystem-upload-lifecycle-contained-cleanup.md). Current residual inventory: [`current-state.md`](current-state.md).

**Status (as written):** DESIGN AND PROTOTYPE READY — PRODUCTION UNCHANGED — DECISIONS
IDENTIFIED — **NOT PRODUCTION-READY** — NOT COMMITTED.

**Status at `master` `9405991`:** design landed; this file is not current remaining work.

This document is the consolidated decision package for making the filesystem
upload-session and receipt reaper (`FsStorage::reap_expired_sessions`) coherent:
binding inspection, locking, recovery/abort, and deletion into **one operation
boundary that never escapes its declared authority or synchronization boundary**,
so the deferred reaper cutover can be unblocked.

It does not change production. It proposes a design, proves the mechanism with an
executable test-only prototype
(`tests/upload_lifecycle_contained_cleanup_prototype.rs`), and specifies the exact
additive dependency APIs required. **The reaper gap remains OPEN**; earlier
assessments are unchanged.

This revision closes six mechanism gaps that a previous draft left as "accepted
ceilings," and then hardens four of them that a review found still unsound:

* **Lock reclamation** — the earlier "online sweep" (acquire the lock, confirm no
  session artifacts, unlink the lock file) is **withdrawn as unsafe**: acquisition
  plus artifact-absence does not exclude a pre-existing *unacquired waiter* on the
  lock inode, so the sweep can still split lock identity (proven by a deterministic
  regression). Lock files are **retained**; reclamation is permitted **only under
  global quiescence**. Accumulation is therefore **unbounded in the historical
  session count** (§3.1).
* **Cancellation** — expressed through an **owned operation boundary** that carries
  the directory authority *and* the lock guard into the blocking work, resolving the
  `&ContainedDir`/ownership mismatch, and demonstrated with a **real Tokio
  cancellation test** (§3.4, §5).
* **Recovery dispatch** — the reaper's Finalizing branch is routed through the
  **roll-forward** (not truncate-to-zero), exercised **through the reaper** across
  the full matrix, with **contained parent creation** and **atomic leaf
  replacement** specified and their **crash-durability boundary** stated (§3.3, §6).
* **Authority/deployment** — the "every supported deployment" claim is **removed**;
  **Option A (a single process-lifetime shared authority) is the one recommended
  path**, fully scoped; Option B is retained only as an **operator-accepted
  restriction with detection-only enforcement** and a stated residual window (§3A).

Two of the gaps (the cooperative lock-identity defect; writer/reaper coherence
during ancestor replacement) turn out **not** to be reaper-only fixes: making the
system coherent expands production scope, and this document states that concretely
(§3.1, §3A, §7) rather than promising a reaper-only change. **This does not approve
the dependency API change or the production rollout**; both remain separately
gated.

Baselines: primary `registry-rust @ 00676c721fde2687196eececbc2cdb097bb1fd9c`;
dependency `storage-layer-rust @ 0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`
(read-only, unchanged).

---

## 1. Why this is a write-boundary decision, not another read slice

The predecessor batch contained standalone **point reads** through the shared
pinned `storage_fs::FsMetadataReader`. It **deferred** the reaper because the
reaper does not merely read — it *acts* (locks, recovers, aborts, unlinks) on what
it reads. Routing only its inspection through the pinned reader while its mutations
resolve fresh ambient pathnames is an **inspection-to-action mismatch**: after a
storage root is renamed aside and a fresh same-named tree is created, the pinned
descriptor inspects the *detached* tree while ambient mutations delete a same-UUID
record in the *replacement* tree — deleting a fresh, non-expired record on the
strength of a different tree's expiry. The committed regression
`test_real_reaper_root_replacement_acts_only_on_current_tree`
(`src/storage/fs/upload_quarantine_read.rs`) is the permanent guard against that
mismatch and must remain green until a complete replacement contract is approved.

The fix is therefore not "contain one more read." It is: make inspection and every
mutation of a lifecycle resolve through **the same pinned directory authority and
the same lock**, so a lifecycle operates on exactly one tree — never a mix — and no
step escapes its authority or lock. As §3 shows, that scope reaches beyond the
reaper into the writers, because a reaper cannot be coherent against ancestor
replacement while the writers it coordinates with still resolve ambient paths.

---

## 2. Current call / state / lock trace (as-built at `00676c72`)

All paths hang off `uploads_dir() = <root>/uploads` (`src/storage/fs.rs:680-710`):

| Purpose | Relative path |
|---|---|
| staging data | `uploads/{uuid}.data` (`session_data_path`/`upload_path`) |
| session meta | `uploads/{uuid}.meta.json` (`session_meta_path`) |
| resumable hash | `uploads/{uuid}.hash.{generation}` (`session_hash_path`) |
| session lock | `uploads/.lock.{uuid}` (`session_lock_path`) |
| finalized receipt | `uploads/.finalized/{uuid}.json` (`finalized_receipt_path`) |
| legacy hash sidecar | `uploads/{uuid}.sha256state` (`upload_hash_path`, migration only) |
| CAS blob | `blobs/{algo}/{prefix2}/{hex}` (post-finalize rename target) |
| repo membership | `<repo-blobs-dir>/…` (`link_repo_blob`, `src/storage/fs.rs:2736-2747`) |

**Everything on the write side resolves ambient `tokio::fs` pathnames.** The
pinned `self.reader` (an `Arc<storage_fs::FsMetadataReader>` built once at
construction, `src/storage/fs.rs:351-360`) is used only by read impls. No mutation
touches it.

### 2.1 Session state machine

`FsSessionMetaRecord.state` (`src/storage/fs.rs:1682`) is written only as `Active`
(create, `:1824`) or `Finalizing` (begin_finalize, `:2352`). **`Appending` is
never persisted by this backend** — so the reaper's `Appending` arm is effectively
dead for fs-written meta, and an expired `Active` session falls to the abort-only
arm.

### 2.2 Lock domain (shared by writers and reaper)

The lock is an advisory `fs2`/`flock(2)` exclusive lock on the ambient file
`uploads/.lock.{uuid}` (`FsSessionLockGuard`, `src/storage/fs.rs:1702-1714`;
`acquire_fs_session_lock`/`try_acquire_fs_session_lock`, `:1716-1767`). It is
acquired at entry and held for the whole body by: `create_session` (`:1804`),
`session_status` (`:1844`), `append_if_offset` (`:1986`), `begin_finalize`
(`:2159`), `commit_finalize` (`:2381`), `abort_session` (`:2511`),
`recover_session` (`:2536`). The reaper uses the non-blocking variant (`:2676`).
**This shared domain is a hard constraint: any coherent reaper must lock the same
`.lock.{uuid}` file the writers lock — not a reaper-only lock.**

Two kernel facts about `flock` govern this design and are proven directly in the
prototype (§4):

* **`flock` is per-open-file-description.** Two separate `open()` calls of the
  *same inode* are mutually excluded (the second `LOCK_EX | LOCK_NB` returns
  `EWOULDBLOCK`), even within one process/thread. Holding the guard and then
  calling a method that re-opens `.lock.{uuid}` and re-locks it **self-deadlocks**.
* **`unlink` + recreate at the same name yields a *different inode*.** Two openers
  that straddle the unlink then hold exclusive locks on two different inodes — a
  **split lock domain**: both "succeed," neither excludes the other. Identical lock
  *filenames* do not imply an identical lock *domain*.
* **Acquiring the lock does not exclude a pre-existing *unacquired waiter*.**
  `open()` and `flock()` are two steps. A process can hold an fd on the lock inode
  (having `open`ed it) while it is still *blocked in* — or has not yet reached —
  `flock(LOCK_EX)`. An actor that successfully acquires the lock and then observes
  "no session artifacts" has learned **nothing** about such a waiter: the waiter's fd
  predates the acquisition and is invisible to it. If that actor then unlinks and the
  name is recreated, the waiter (inode X) and the next opener (inode Y) both proceed
  under exclusive locks on **different inodes**. This is why "acquire, then confirm
  absence, then unlink" is **not** a safe reclamation protocol (§3.1), and is proven
  by `online_reclamation_sweep_splits_lock_identity_despite_acquisition_and_artifact_absence`.

### 2.3 The lock-identity defect (design requirement 1 — a cooperative defect)

`abort_session` **unlinks the lock file** `uploads/.lock.{uuid}` at `:2526` — the
only unlink site for the lock. Combined with §2.2, this is a **cooperative
concurrency defect**, not merely a limitation against hostile actors:

```
Waiter W:  open(.lock.uuid) → inode X          (blocked, or about to lock)
Abort A:   … unlink(.lock.uuid) …              (removes name → inode X)
Opener O:  open(.lock.uuid, O_CREAT) → inode Y  (fresh inode at same name)
W: flock(X) OK        O: flock(Y) OK           → BOTH hold "the session lock"
```

Two well-behaved actors that both take `.lock.{uuid}` — exactly as the protocol
demands — can end up holding exclusive locks on different inodes across an abort
boundary. The prototype proves this split with real `flock`
(`unlinking_lock_on_abort_splits_identity_and_breaks_exclusion`) and proves that
**retaining** the lock file preserves exclusion
(`stable_lock_identity_retained_file_preserves_mutual_exclusion_across_abort`).

### 2.4 The drop-before-act window (the second core defect)

Because `recover_session` and `abort_session` each **re-acquire** the same lock
(`:2536`, `:2511`), the reaper cannot hold the lock across them. It explicitly
`drop(_guard)` **before** dispatch (`src/storage/fs.rs:2686`):

```
try_lock .lock.{uuid}  ── inspect meta ── age check     [lock held]
drop(_guard)                                            [lock released]  ← TOCTOU window
recover_session / abort_session  (each re-locks)        [lock re-acquired inside]
```

Today this is *masked* because inspection and mutation both use ambient paths on
the same tree (internally coherent), and the returned `count` tallies **attempts,
not confirmed successes**. The moment inspection is pinned but mutation stays
ambient, the window becomes the root-replacement mismatch the regression catches.

### 2.5 Persistence discipline (independent of containment)

* `atomic_write_file` (`:776-806`) — **strict**: file `sync_all` and parent
  `fsync_dir` both propagated.
* `write_atomic_file` (`:1769-1792`) — **loose**: both fsyncs `let _ =` **ignored**.

Session meta, hash, and receipt writes all use the **loose** helper.
`commit_finalize`'s CAS `rename` precedes two `let _ =` `fsync_dir` calls
(`:2465-2466`). Crash durability is therefore already weaker than a naïve reading
suggests; it is a *separate* axis (§6). This proposal does not claim to fix it and,
crucially, does not let "separate axis" imply crash recovery is guaranteed (§6).

### 2.6 Callers, result type, construction

`reap_expired_sessions -> Result<usize, StorageError>`
(`src/storage/upload_session.rs:185-191`, default `Err(Unsupported)`; `Arc<T>`
forwarder `:265-273`). Caller chain: `supervisor.rs:1253` →
`UploadCoordinator::reap_expired_uploads`
(`src/upload_coordinator.rs:856-869`) → `storage.reap_expired_sessions`. The
`usize` is opaque to callers (logged), which gives room to redefine its meaning
(§6, Decision D-3).

Both production sites build the pinned reader identically via
`try_new_with_all_limits` → `FsMetadataReader::open(&root)` (`:351-360`): primary
`storage_wiring_try_from_config` (`src/storage/mod.rs:879-887`) and proxy-cache
`proxy_cache_storage_try_from_config` (`:961-969`). Each pins its own root once;
any authority the reaper needs must be reachable from that already-pinned root at
both sites (identical in shape).

---

## 3. Operation-by-operation authority map (design requirement 3)

Every operation reachable from `reap_expired_sessions` — including the operations
**inside** `recover_session`/`abort_session` it dispatches to — is listed with the
authority it needs and the subtree it touches. "Ambient" = resolves a fresh
pathname today; "pinned-X" = must resolve beneath descriptor X for coherence.

| # | Operation | Reached from | Reads/Writes | Subtree | Authority required |
|---|---|---|---|---|---|
| 1 | enumerate `uploads/` `*.meta.json` | reaper | read (dir) | uploads | pinned-uploads |
| 2 | acquire `.lock.{uuid}` | reaper + every writer | create+flock | uploads | pinned-uploads, **stable inode** (§3.1) |
| 3 | read `{uuid}.meta.json` | reaper, recover, abort | read | uploads | pinned-uploads |
| 4 | capture meta identity `(dev,ino)` | reaper | fstat | uploads | pinned-uploads |
| 5 | revalidate meta name→inode | reaper | openat+fstat | uploads | pinned-uploads + held lock |
| 6 | unlink `{uuid}.data` | abort | unlink | uploads | pinned-uploads + held lock |
| 7 | unlink `{uuid}.hash.{g}` (0..N) | abort | unlink | uploads | pinned-uploads + held lock |
| 8 | unlink `{uuid}.meta.json` (**LAST**, §5) | abort | unlink | uploads | pinned-uploads + held lock |
| 9 | truncate `{uuid}.data` to offset | recover (Active/torn tail) | ftruncate | uploads | pinned-uploads + held lock |
| 10 | parse finalizing digest+size from meta | recover (Finalizing) | read | uploads | pinned-uploads |
| 11 | **inspect CAS blob** `blobs/{algo}/{prefix2}/{hex}` | recover (Finalizing) | metadata (size) | **blobs** | **pinned-blobs** |
| 12 | **link repo membership** | recover (Finalizing) | write | **repo-membership** | **pinned-membership** |
| 13 | write finalized receipt `{uuid}.json` | recover (Finalizing) | write | uploads/.finalized | pinned-finalized |
| 14 | leave meta in place (idempotent roll-forward) | recover (Finalizing) | — | uploads | (no unlink) |
| 15 | enumerate `.finalized/` `*.json` | reaper | read (dir) | uploads/.finalized | pinned-finalized |
| 16 | inspect receipt + identity | reaper | read+fstat | uploads/.finalized | pinned-finalized + held lock |
| 17 | revalidate receipt name→inode | reaper | openat+fstat | uploads/.finalized | pinned-finalized + held lock |
| 18 | unlink expired receipt | reaper | unlink | uploads/.finalized | pinned-finalized + held lock |
| 19 | release `.lock.{uuid}` (explicit `LOCK_UN`) | reaper + writers | flock | uploads | held-guard drop **inside** op (§3.4) |

**Three subtrees, not one.** Rows 11–13 show that Finalizing recovery is NOT a
truncation and does NOT live under `uploads/` alone: it inspects CAS under
`blobs/`, writes membership under the repo-membership subtree, and writes the
receipt under `.finalized/`. A design that pins only `uploads/`/`.finalized/`
cannot contain the Finalizing branch. This is the central correction over the
previous draft, which modelled recovery as truncation-to-zero and therefore stood
as evidence only for row 9, never rows 10–14.

**Contained parent creation and atomic leaf replacement (rows 12–13).** Membership
and receipt writes may target a per-repo subdirectory (`membership/{repo}/`) that
does not yet exist. Each such write therefore (a) **creates missing parents beneath
the pinned membership descriptor** with `mkdirat` (idempotent on `EEXIST`) and
re-opens the created dir with a containment-checked `openat2` (`O_DIRECTORY`), so the
parent is itself a pinned authority; then (b) **replaces the leaf atomically** —
write a temporary sibling `.tmp.{name}.{seq}` beneath the same pinned dir, then
`renameat` it over the final name. `renameat` gives atomic *visibility* (a reader
sees either the old or the new leaf, never a partial one); on any pre-rename failure
the temporary is unlinked so no `.tmp.*` residue leaks. This is atomic **visibility**,
not crash **durability** — see §6. Roll-forward writes **membership before receipt**
so that a crash between them leaves the receipt absent and the session re-enters
recovery on the next pass (idempotent retry), never a receipt without membership.

### 3.1 Stable lock identity, resource accumulation, reclamation (requirement 1)

**Protocol.** The `.lock.{uuid}` file is **retained** across abort/cleanup: no
operation unlinks it. Exclusion is then always evaluated against a single inode
(row 2), so a waiter that opened the lock before an abort and an opener after it
resolve the same inode and remain mutually excluded (proven:
`stable_lock_identity_retained_file_preserves_mutual_exclusion_across_abort`).
The lock file is acquired beneath the **pinned uploads descriptor** so its inode is
also stable against `uploads/` replacement; an ambient lock pathname splits the
domain across replacement (proven:
`ambient_lock_path_splits_domain_across_uploads_replacement`).

**Resource accumulation and safe reclamation.** Retaining lock files means one
`.lock.{uuid}` zero-length inode accumulates per session **ever created**. This
accumulation is **unbounded in the historical session count** — it grows without
limit for the life of the storage tree unless reclaimed. "Zero-length" is not "free":
each retained lock consumes a directory entry and an inode, and (on most filesystems)
at least one block of directory capacity; a long-lived busy registry accretes them
indefinitely. We describe this honestly as unbounded, **not** as a bounded ceiling.

The previous draft proposed an **online out-of-band sweep** — acquire `.lock.{uuid}`,
confirm no `{uuid}.meta.json`/`{uuid}.data`/`{uuid}.hash.*`/`.finalized/{uuid}.json`
exists, then unlink the lock. **That protocol is withdrawn as unsafe.** Per §2.2,
acquiring the lock and finding no artifacts does not exclude a **pre-existing
unacquired waiter** holding an fd on the same inode. The sweep unlinks, the name is
recreated by the next `create_session`, and the straddling waiter (inode X) plus the
new holder (inode Y) both hold exclusive locks on different inodes — the very split
retention was meant to prevent. This is demonstrated deterministically by
`online_reclamation_sweep_splits_lock_identity_despite_acquisition_and_artifact_absence`,
which runs the exact acquire→absence→unlink→recreate sequence and shows two "holders"
coexisting.

Reclamation is therefore permitted **only under global quiescence**: a maintenance
window in which the reclaimer can establish that there are **no lock holders, no
unacquired waiters, and no new openers** for the duration of the removal — e.g. the
registry (and any peer sharing the root) is stopped, or a top-level exclusive
"maintenance" lock excludes *all* lifecycle entry points (every writer and the reaper
must acquire that outer lock before opening any `.lock.{uuid}`). Under global
quiescence there are provably no straddling fds, so unlink+absence is safe. **No
correct online (concurrent-with-traffic) reclamation protocol is claimed**; if one is
later found, it must come with a proof and a regression, not an "acquire-then-check".
The prototype models retention and the exclusion it preserves, and the *failure* of
the online sweep; it does not model a quiescence sweep (that is an operational
procedure, §7).

**Rollout / rollback consequence (removes the old interop claim).** An
old-binary that still unlinks `.lock.{uuid}` on abort will, if run concurrently
against the same tree as a new-binary that relies on retention, **reintroduce the
split** — a new-binary waiter on inode X and an old-binary-induced opener on inode
Y are not mutually excluded. Therefore: **old lock-unlinking binaries must be
stopped before a new-binary rollout on a shared root** (no mixed-binary operation
on one storage tree during the lock-identity change). Rollback to an old binary is
safe for *data* (formats unchanged) but **reintroduces the lock-identity defect**;
it does not undo any deletion already performed. We explicitly **withdraw** the
prior blanket claim that "old and new binaries can share a storage root."

### 3.2 One pinned directory authority per lifecycle

Anchor on **directory descriptors captured once** and resolve every subsequent
inspection, lock, and mutation relative to them with `*at` syscalls
(`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`):

* `root` — `O_PATH | O_DIRECTORY` (as `FsMetadataReader::open` already does).
* `uploads/` — `openat2` beneath `root`.
* `uploads/.finalized/` — `openat2` beneath `uploads` (**best-effort**: absent
  `.finalized/` yields `None` and must not block session processing — §6, proven:
  `absent_finalized_dir_does_not_block_session_processing`).
* `blobs/` and the repo-membership dir — `openat2` beneath `root`, required for the
  Finalizing branch (rows 11–12).

Once open, replacing the pathname of any pinned ancestor cannot redirect the pass
(proven for root/`uploads/`/`.finalized/`). The pass then operates on **one tree**.

### 3.3 Locked-inner variants that RECEIVE and RETAIN authority + lock (req. 3)

The reaper holds `.lock.{uuid}` across the destructive action. To avoid the
self-deadlock of §2.2, the destructive bodies become **locked-inner variants that
take both the pinned authority and a lock token as parameters** and use contained
primitives — they do **not** re-lock, and they do **not** call ambient helpers such
as `self.link_repo_blob`. Merely deleting the lock-acquisition line is insufficient
(the body would still resolve ambient pathnames and still escape the authority);
the body must *receive* the pinned descriptors and the guard:

```
recover_finalizing_locked(&Authority, &LockToken, uuid) -> FinalizingOutcome
recover_truncate_locked  (&Authority, &LockToken, uuid, offset)
abort_locked             (&Authority, &LockToken, uuid)
unlink_receipt_locked    (&Authority, &LockToken, uuid)
```

The prototype implements exactly this shape: each locked-inner method takes
`&SessionLock` and asserts it matches the uuid (`debug_assert_eq!(lock.uuid(),
uuid)`), and resolves every leaf beneath the pinned descriptors. `abort_locked`,
`recover_truncate_locked`, `recover_finalizing_locked`, and `unlink_receipt_locked`
never re-acquire the lock (`locked_inner_ops_do_not_reacquire_lock`).

**Finalizing recovery is routed through the reaper and is representative, not
production-integrated.** The prototype reaper (`prototype_reap`) dispatches an expired
`Finalizing` session to `recover_finalizing_locked` — the **roll-forward** — and maps
its `FinalizingOutcome` to the report: `RolledForward ⇒ success`,
`AlreadyFinalized ⇒ skipped_already_final`, `CasUnavailable ⇒ skipped_incomplete`,
`Err ⇒ errors`. The truncate-to-zero primitive is **no longer** on the Finalizing
path; it is the *torn-tail* (Active) branch, exercised directly by
`truncation_recovery_is_the_torn_tail_branch_distinct_from_rollforward`. The
roll-forward performs the row 10–14 flow across three pinned subtrees — read
finalizing meta (uploads), inspect CAS size (blobs), create the membership parent and
write the membership leaf atomically (membership), write the receipt leaf atomically
(finalized), leave meta — with a *simplified* flat CAS layout and marker membership
record.

The recovery matrix is exercised **through the reaper**, not by calling the inner body
directly: CAS absent/mismatched (`reaper_finalizing_without_cas_is_skipped_incomplete`),
an existing receipt (`reaper_finalizing_existing_receipt_is_already_final`), a
membership success followed by an injected receipt-write failure and then a retry
after the fault clears (`reaper_finalizing_membership_then_receipt_failure_then_retry`,
which also asserts no `.tmp.*` residue and that membership is present while the receipt
is absent between attempts), a missing required membership parent that recovery must
create (`reaper_finalizing_creates_missing_membership_parent`), and accurate
outcome/count mapping across a mixed batch
(`reaper_mixed_finalizing_outcomes_map_to_accurate_counts`). It is evidence that the
**authority flow** is containable (no step escapes a pinned descriptor) and that
recovery **publishes nothing** when CAS is absent/mismatched
(`representative_finalizing_recovery_without_cas_match_does_not_publish`).

**Comparison with production and intentional compatibility changes.** Production
`recover_session`'s Finalizing branch (`src/storage/fs.rs:2571-2608`) links repo
membership via the ambient `self.link_repo_blob` (`:2736-2747`) and writes the receipt
via the loose `write_atomic_file`, then **leaves** the meta — the model matches that
*shape* (inspect CAS, link membership, write receipt, leave meta, idempotent). Two
intentional changes: (1) membership is written **before** the receipt (production's
exact interleaving is incidental; the model fixes the order so a crash cannot leave a
receipt without membership); (2) every leaf/parent resolves beneath a pinned
descriptor rather than an ambient path. These are the compatibility-relevant deltas;
on-disk **formats** are unchanged (§6, §8). The model is **not** evidence for the
production CAS sharding, record byte-formats, or `link_repo_blob` internals; those
require production integration (§7) and the dependency API of §5.

### 3.4 Cancellation and in-flight mutations (design requirement 5)

Production mutations run inside `spawn_blocking`. **Cancelling the awaiting future
does not abort the already-running blocking closure.** The synchronization
guarantee must therefore be **structural, expressed as an owned operation boundary**,
not a property of the async wrapper:

* **Owned operation boundary.** A lifecycle mutation is a value that **owns** both the
  directory authority (an `Arc<OwnedFd>`, clonable into blocking work) *and* the lock
  guard, and carries them together through inspection, the expiry/identity decision,
  and every resulting blocking mutation. Because the boundary owns the guard, no
  intermediate step can drop it early. This resolves the mismatch in the earlier
  sketch, where async methods took only `&ContainedDir`: a shared borrow cannot move
  the guard into `spawn_blocking`, so the guard would have to drop on the async side
  (at the `.await`), i.e. exactly at the cancellation point. The API therefore exposes
  an **owned** entry (`ContainedDir::run_locked`, §5) that takes the guard **by value**
  and hands it, with a cloned directory `Arc`, into the blocking closure.
* **`LOCK_UN` happens inside the blocking closure, after the mutation.** The guard is
  moved into the closure and dropped there; its `Drop` issues the explicit
  `flock(LOCK_UN)`. So release happens *after* the mutation finishes, never on caller
  cancellation. Aborting or dropping the awaiting `JoinHandle` detaches the blocking
  task but does **not** abort it — the closure runs to completion and only then
  releases.
* **Directory-fd liveness ≠ lock-hold.** Keeping an `Arc<OwnedFd>` for the directory
  alive does not keep the flock held — the flock lives on the lock file's open file
  description, released by `LOCK_UN` on guard drop. The design does not conflate them.

**Two kinds of evidence, kept separate.** (1) A **real Tokio cancellation test**,
`tokio_cancellation_retains_lock_until_blocking_completion` (multi-thread runtime):
a `tokio::spawn`ed future awaits a `spawn_blocking` mutation that owns the guard; the
test `abort()`s that future mid-operation (deterministic barriers, no sleeps), then —
via a *separate* `spawn_blocking` probe — observes the lock is **still held** and the
mutation **not** yet visible; only after the worker completes does the guard drop,
the lock become acquirable, and the effect appear. This is actual async-cancellation
evidence. (2) A **synchronous ownership model**,
`blocking_ownership_model_retains_lock_until_completion`, exercises the same
move-guard-into-blocking-work invariant with threads and channel barriers; it is
retained as a clear illustration of the ownership property but is **explicitly labelled
as modeling, not async-cancellation evidence**. We do not present the thread test as
if it were the Tokio result.

### 3.5 Receipt inspect→delete boundary under one continuous lock (requirement 2)

The receipt loop carries `.lock.{uuid}` — the **same** lock identity/authority the
receipt *writers* use (row 2; `commit_finalize`/`recover_session` write receipts
under that lock) — continuously across **inspect → expiry decision → revalidation →
unlink**:

* Busy lock ⇒ non-error skip (`receipt_busy_lock_is_skipped_not_deleted`): a
  concurrent finalize/recover holding the session lock is never raced.
* Receipt replaced between inspection and action ⇒ identity revalidation rejects
  the delete; a fresh same-UUID receipt survives
  (`receipt_replacement_between_inspect_and_action_is_rejected`).
* Receipt vanished ⇒ idempotent no-op (`receipt_disappearance_is_idempotent_noop`).

**`unlinkat` removes a NAME, not the inspected inode** (§3.6). We do **not** claim
`unlinkat` deletes the previously-inspected receipt inode; the held lock plus
identity revalidation is what prevents deleting a replacement. The explicit
limitation against a **non-cooperating** actor that rebinds the leaf without taking
the lock is preserved (§3.7).

### 3.6 `unlinkat` removes a name, not an inode

`unlinkat(dir_fd, "{name}", 0)` removes the **directory entry** in the pinned dir
inode; it does not guarantee removal of the *same file inode* inspection opened.
The pinned dir fd fixes *which directory*; tying the *name* to the *inspected
inode* requires the shared lock (excluding cooperative actors) plus
revalidation-under-lock. We do not offer pathname-metadata-check-then-ambient-unlink
as a solution: the unlink is itself descriptor-relative and lock-guarded.

### 3.7 Documented limitation (the honest ceiling)

Revalidation narrows but cannot close the race against a **non-cooperative** actor
that mutates a leaf without taking `.lock.{uuid}`, because `openat2`+`unlinkat` is
not one atomic transaction. This is the ceiling of the achievable guarantee on a
shared POSIX tree; we state it rather than broadening "cooperative locking" into
"protection against arbitrary hostile changes" (Decision D-4). It is **distinct
from** the lock-identity defect of §3.1, which was a *cooperative* defect and **is**
fixed by retention.

---

## 3A. Writer/reaper coherence during ancestor replacement (design requirement 4)

The reaper can be made coherent against ancestor replacement by pinning (§3.2). But
the **writers** (`create_session`, `append_if_offset`, `begin_finalize`,
`commit_finalize`, `session_status`) still resolve ambient pathnames. Trace a
writer that acquires `.lock.{uuid}` **before** `uploads/` (or `root`) is replaced
and performs later pathname operations **after**:

```
Writer: acquire .lock.uuid (ambient path → old uploads inode)   [holds lock on X]
Someone: rename uploads → uploads.old ; mkdir uploads (new inode)
Writer: write {uuid}.data / rename data→blob / write receipt     [now resolves NEW uploads]
```

The writer's later operations land in the **new** `uploads/`, while its lock is on
the **old** `.lock.{uuid}` inode — and a second actor taking `.lock.{uuid}` under
the *new* `uploads/` is **not excluded** from it. Identical lock filenames, two lock
domains (`ambient_lock_path_splits_domain_across_uploads_replacement`). `.finalized`
replacement has the same character for receipt writers. A reaper-only fix cannot
repair this: the incoherence is in the writers' own path resolution and lock
domain, independent of the reaper.

**Two coherent contracts** (this is the substantive decision — D-7):

* **Option A — one process-lifetime shared authority carried through all lifecycle
  participants (RECOMMENDED).** A single `ContainedDir` authority set (`root`,
  `uploads/`, `blobs/`, `membership/`, `.finalized/`) is captured **once** at storage
  construction and shared (via `Arc`) by *every* lifecycle participant —
  `create_session`, `append_if_offset`, `begin_finalize`, `commit_finalize`,
  `session_status`, `recover`, `abort`, and the reaper. Every one of them resolves its
  leaves **and** `.lock.{uuid}` beneath those same captured descriptors.

  *Authority lifetime.* The authority lives for the **process lifetime**: it is not
  re-derived per operation. This is precisely why "every operation pins something" is
  insufficient on its own — if each operation opened its *own* fresh descriptor by
  pathname, two operations straddling an ancestor replacement would pin two different
  inodes and split both the tree and the lock domain again. Compatible identities
  across participants come from *sharing one long-lived pin*, not from each
  independently pinning. Because all participants hold the *same* `uploads/` fd, they
  compute `.lock.{uuid}` on the *same* inode and share **one** lock domain and **one**
  tree for the whole process lifetime, regardless of any ancestor pathname
  replacement.

  *Consequence / cost.* If an operator legitimately rotates `uploads/` aside, the
  running process keeps operating on the **pinned (now-detached)** tree until it is
  restarted — replacement is *ignored*, not honored, for the process's lifetime. That
  is the deliberate tradeoff. Scope: this is a **large expansion of O-04 write-side
  containment** — the entire fs upload write path moves onto the additive dependency
  mutation API (§5, which for this reason must include append/rename/ensure-dir, not
  only the reaper's read/write/unlink), and both construction sites (§2.6) supply the
  shared authority. It is the **only** option that makes the writers coherent against
  replacement, which is why it is recommended as the single correct path.

* **Option B — an operator-accepted operational restriction (interim only, NOT a
  correctness substitute).** The operator accepts, as an explicit deployment
  precondition, that the registry is the **exclusive owner** of its storage root and
  that nothing renames `root`/`uploads/`/`.finalized/` out from under a running
  registry. Under that precondition ancestor replacement does not occur concurrently
  with active operations, so ambient writers stay coherent.

  This is a restriction, and **documentation alone is not enforcement**. The concrete
  enforcement/verification boundary is: the registry can *detect* (not *prevent*)
  violation — e.g. at startup and periodically, `fstat` its pinned root/`uploads/`
  descriptors and compare `(dev,ino)` against a fresh pathname lookup of the same
  paths; a mismatch means the tree was rotated under it and the registry can refuse to
  act / alarm. Prevention would require an OS-level guarantee the registry cannot make
  from userspace (it cannot stop a privileged co-tenant from renaming a directory).
  **Residual risk:** between two detection points there is a window in which an ambient
  writer can straddle a replacement undetected; detection reduces but does not close
  it. Option B is therefore acceptable only as an **operator-accepted interim
  restriction with a stated residual window**, not as a claim of writer coherence.

**Recommendation: Option A, as the one correct path.** It is the only contract under
which the writers are actually replacement-coherent; Option B trades that correctness
for a smaller change and an operational promise the registry can only *detect*, not
*enforce*. If the cutover must ship before the full write-path move, it ships under
Option B **as an explicitly operator-accepted restriction** (D-7), with the residual
window recorded and Option A as the committed target — but this document does not
present B as making the writers coherent, and does **not** implicitly approve either
the dependency API change or the production rollout (both separately gated).

The prototype demonstrates the mechanism underlying the recommendation: pinned-
descriptor lock resolution preserves one domain
(`representative`/single-tree group); ambient lock paths split it
(`ambient_lock_path_splits_domain_across_uploads_replacement`); and the unsafe online
reclamation that would undermine even a shared pin is caught
(`online_reclamation_sweep_splits_lock_identity_despite_acquisition_and_artifact_absence`).
It does not (and cannot) *enforce* Option B's operational invariant — that is a
deployment property, and its detection boundary is production scope (§7).

---

## 4. Prototype: results and claim boundary

`tests/upload_lifecycle_contained_cleanup_prototype.rs` (Linux-gated, integration
test — a separate crate, **not** wired into any production module). It implements
the §3 mechanism with real `*at` syscalls. **All 32 scenarios pass**
(`logs/04_prototype_tests.log`).

Grouped by the requirement they close:

| Group | Tests | Demonstrates |
|---|---|---|
| Single-tree coherence | `root_replacement_acts_only_on_detached_tree`, `uploads_dir_ancestor_replacement_stays_on_detached_inode`, `finalized_dir_ancestor_replacement_stays_on_detached_inode`, `expired_old_receipt_vs_fresh_same_uuid`, `expired_old_session_vs_active_replacement_and_age_selectivity` | Pinned pass never mixes trees; age gate. |
| **Req 1 — lock identity** | `stable_lock_identity_retained_file_preserves_mutual_exclusion_across_abort`, `unlinking_lock_on_abort_splits_identity_and_breaks_exclusion`, `ambient_lock_path_splits_domain_across_uploads_replacement`, `online_reclamation_sweep_splits_lock_identity_despite_acquisition_and_artifact_absence` | Retention preserves exclusion; unlink and ambient paths split the domain; **the online acquire→absence→unlink sweep splits it despite a successful acquire and full artifact absence** (why only global quiescence is safe, §3.1). |
| Lock/act coherence | `busy_lock_is_skipped_not_deleted`, `locked_inner_ops_do_not_reacquire_lock`, `candidate_change_between_inspect_and_action_is_rejected`, `inspection_failure_performs_no_destructive_action` | Busy-skip; no re-lock; revalidation rejects; fail-closed. |
| **Req 2 — receipt boundary** | `receipt_busy_lock_is_skipped_not_deleted`, `receipt_replacement_between_inspect_and_action_is_rejected`, `receipt_disappearance_is_idempotent_noop` | Continuous shared lock across inspect→unlink. |
| **Req 3 — recovery through the reaper** | `representative_finalizing_recovery_rolls_forward_under_pinned_authority`, `representative_finalizing_recovery_without_cas_match_does_not_publish`, `reaper_routes_expired_finalizing_through_rollforward`, `reaper_finalizing_without_cas_is_skipped_incomplete`, `reaper_finalizing_existing_receipt_is_already_final`, `reaper_finalizing_membership_then_receipt_failure_then_retry`, `reaper_finalizing_creates_missing_membership_parent`, `reaper_mixed_finalizing_outcomes_map_to_accurate_counts`, `truncation_recovery_is_the_torn_tail_branch_distinct_from_rollforward` | Reaper dispatches Finalizing to roll-forward (not truncate); CAS absent/mismatch/existing-receipt/parent-creation/retry-after-receipt-fault; accurate outcome→count map; truncate is the torn-tail branch only. |
| **Req 4 — ancestor replacement** | `ambient_lock_path_splits_domain_across_uploads_replacement` (+ single-tree group) | Ambient lock domain splits; shared pin preserves. |
| **Req 5 — cancellation** | `tokio_cancellation_retains_lock_until_blocking_completion` (real async), `blocking_ownership_model_retains_lock_until_completion` (ownership model) | Lock retained until the op completes despite caller cancellation — proven under a real Tokio abort, and separately modeled synchronously. |
| **Req 6 — outcomes/dirs/durability** | `earlier_success_then_later_error_is_reported_honestly`, `fatal_enumeration_failure_maps_to_err`, `absent_finalized_dir_does_not_block_session_processing`, `partial_abort_is_rediscovered_and_completed`, `valid_cleanup_then_idempotent_retry` | Honest `Result` mapping; missing-dir tolerance; partial-crash rediscovery; idempotency. |

### Claim boundary (stated in the file header and honoured here)

* **Real filesystem evidence**: coherence, lock-identity, fail-closed, busy-skip,
  rejection, cancellation-retention, and idempotency assertions are real
  `openat2`/`unlinkat`/`renameat`/`flock`/`fstat`/`ftruncate`/`fdopendir` results
  against real tmpdir trees on this Linux host. Not claimed for non-Linux; not
  hardware durability (no power-loss testing).
* **Real async cancellation vs ownership modeling**: `tokio_cancellation_retains_
  lock_until_blocking_completion` is **actual async-cancellation evidence** — a real
  multi-thread Tokio runtime, a real `abort()` of the awaiting future, a real
  `spawn_blocking` that owns the guard. `blocking_ownership_model_retains_lock_until_
  completion` is a **synchronous ownership model** (threads + channel barriers),
  retained only to illustrate the move-guard-into-blocking-work invariant; it is **not**
  presented as async-cancellation evidence.
* **Prototype behaviour**: `PinnedUploadsAuthority`/`prototype_reap`/`ReapReport`
  model the *proposal*; they are not the production reaper and are structured
  differently (locked-inner variants taking a lock token; a success/skip/error
  report; a public-result mapping).
* **Representative modeling**: `recover_finalizing_locked` models the row 10–14
  authority flow with a simplified CAS/membership layout — evidence for
  containability of the flow, **not** for production formats/CAS sharding.
* **Simulated capability contract**: the `RootRelativeMutator` trait models the
  additive dependency primitives that **do not yet exist**; the libc impl proves the
  *kernel mechanism*, **not** the dependency's future in-crate implementation
  (which needs its own tests, §5).

### One real bug the prototype caught (development evidence)

The first `list_dir` used `dup(uploads_fd)` for `fdopendir`. `dup` shares the
directory read offset with the pinned fd, so a second pass started at EOF and
enumerated nothing. Fixed by opening a **fresh** enumeration descriptor via
`openat2(".")` per call — which is also how the dependency's `enumerate_dir`
behaves (fresh fd per call). Concrete argument for putting enumeration in the
dependency rather than re-implementing it ad hoc.

---

## 5. Dependency capability requirements (exact, additive)

`storage_fs @ 0a628fd0` is **read-only**: `FsMetadataReader` exposes `head`,
`open_payload`, `enumerate_dir`, `inspect_file_metadata`, `probe_capability`,
`root_path`. There is **no** fd-relative mutation, **no** `flock`, **no** raw-fd
accessor, and **no payload acquisition relative to a captured dir handle**. The
previous draft's proposed API was **incomplete for the real recovery branch**: it
lacked (a) CAS payload/size acquisition relative to a captured `blobs/` handle, (b)
a membership-write authority (nested directory acquisition under the repo-membership
subtree), and (c) the receipt/membership **writes** recovery performs. The
completed additive surface below covers rows 1–19.

```rust
// New module `storage_fs::mutate` (Linux-gated), additive; existing API unchanged.

pub struct ContainedDir { /* Arc<OwnedFd> dir + PathBuf for diagnostics */ }

impl FsMetadataReader {
    /// Open a directory beneath the pinned root as a mutation/enumeration anchor.
    pub async fn open_contained_dir(&self, subdir: &ObjectKey)
        -> Result<ContainedDir, FsMutateError>;
}

impl ContainedDir {
    /// Open a subdirectory beneath THIS dir (nested authority acquisition — needed
    /// for blobs/{algo}/{prefix2} and the repo-membership subtree, rows 11–12).
    pub async fn open_subdir(&self, name: &FileName) -> Result<ContainedDir, FsMutateError>;

    /// Create `name` as a subdirectory beneath THIS dir if absent (idempotent on
    /// EEXIST) and return it as a pinned authority — contained parent creation for
    /// `membership/{repo}/` (§3, rows 12–13). `mkdirat` then containment-checked
    /// `openat2(O_DIRECTORY)`.
    pub async fn ensure_subdir(&self, name: &FileName) -> Result<ContainedDir, FsMutateError>;

    /// Enumerate immediate entries (fresh fd per call; bounded like enumerate_dir).
    pub async fn list(&self, limits: DirEnumerationLimits) -> Result<Vec<DirEntry>, FsDirError>;

    /// Inspect a leaf's (size, mtime, dev, ino). `Ok(None)` is ENOENT only.
    pub async fn inspect(&self, name: &FileName) -> Result<Option<FsFileIdentity>, FsMutateError>;

    /// Read a payload leaf beneath this dir (CAS acquisition relative to a captured
    /// handle, row 11) — bounded; returns size-checked bytes or a streaming handle.
    pub async fn read_leaf(&self, name: &FileName, limit: u64) -> Result<Vec<u8>, FsMutateError>;

    /// Acquire an advisory exclusive lock on a lock-file leaf beneath this dir,
    /// created if absent and RETAINED (never unlinked by lock ops — §3.1).
    /// `Ok(None)` = busy (EWOULDBLOCK); `Ok(Some)` = held; guard releases on drop.
    pub async fn try_lock(&self, name: &FileName) -> Result<Option<ContainedLockGuard>, FsMutateError>;

    /// OWNED operation boundary (§3.4). Consume a held lock guard and run `body` on a
    /// blocking thread with a cloned directory `Arc`; the guard is MOVED INTO the
    /// closure and dropped there (LOCK_UN after the mutation), so caller cancellation
    /// cannot release early. Returns `body`'s result.
    pub async fn run_locked<T, F>(&self, guard: ContainedLockGuard, body: F) -> Result<T, FsMutateError>
    where
        F: FnOnce(BlockingDir, ContainedLockGuard) -> Result<T, FsMutateError> + Send + 'static,
        T: Send + 'static;

    /// Atomically replace a leaf beneath this dir: write `.tmp.{name}.{seq}`, then
    /// `renameat` over `name` (atomic VISIBILITY; not crash durability — §6).
    /// The temporary is unlinked on any pre-rename failure (no `.tmp.*` residue).
    /// Loose or strict fsync per `durable` (§6, D-5). Used for receipt/membership
    /// writes (rows 12–13).
    pub async fn write_leaf_atomic(&self, name: &FileName, bytes: &[u8], durable: bool)
        -> Result<(), FsMutateError>;

    /// Remove a directory entry beneath this dir. `missing_ok` maps ENOENT→Ok.
    pub async fn unlink(&self, name: &FileName, missing_ok: bool) -> Result<(), FsMutateError>;

    /// Truncate a regular-file leaf beneath this dir to `len` (recover torn tail).
    pub async fn truncate(&self, name: &FileName, len: u64) -> Result<(), FsMutateError>;

    // --- Additional write-path primitives required ONLY by Option A (§3A/§7) ---
    // Without these, "Option A support" would be an unbacked claim, so they are
    // specified here as part of the recommended path's full scope.

    /// Append bytes to a data leaf beneath this dir at a checked offset
    /// (`append_if_offset`). Fails if the current size != `expected_offset`.
    pub async fn append_at(&self, name: &FileName, expected_offset: u64, bytes: &[u8])
        -> Result<(), FsMutateError>;

    /// Rename a leaf beneath this dir to a leaf beneath `dst` dir (CAS publish:
    /// `uploads/{uuid}.data` → `blobs/{algo}/{prefix2}/{hex}`). Both ends are pinned.
    pub async fn rename_leaf(&self, name: &FileName, dst: &ContainedDir, dst_name: &FileName)
        -> Result<(), FsMutateError>;
}

pub struct FsFileIdentity { /* size:u64, mtime:Option<SystemTime>, dev:u64, ino:u64 */ }
pub struct ContainedLockGuard { /* Arc<OwnedFd>; Drop => flock(LOCK_UN) */ }
pub struct BlockingDir { /* cloned Arc<OwnedFd> handed to a run_locked body */ }

#[non_exhaustive]
pub enum FsMutateError {
    NotFound, NotADirectory, PermissionDenied,
    ResolutionRejected { raw_os_error: i32 },   // ELOOP/EXDEV — never "absence"
    Busy,                                       // EWOULDBLOCK on a blocking lock
    Io(std::io::Error),
    PlatformUnsupported, RuntimeMissing, TaskJoinFailed,
}
```

**Ownership / lifetime / cancellation (design requirement 5).** `ContainedDir` owns
an `Arc<OwnedFd>` cloned into each `spawn_blocking`, so the directory outlives
in-flight work even if the handle is dropped. The **owned operation boundary** is
`run_locked`: it takes the `ContainedLockGuard` **by value** and moves it (with a
cloned directory `Arc`, surfaced to `body` as a `BlockingDir`) into the blocking
closure, where the guard is dropped **after** the mutation — so `flock(LOCK_UN)` runs
on completion, never on caller cancellation. This is why the mutation entry is
`run_locked(guard, body)` and **not** an `&ContainedDir` method that borrows the guard:
a shared borrow could not move the guard past the `.await`, forcing release exactly at
the cancellation point (§3.4). Keeping the directory `Arc<OwnedFd>` alive is explicitly
**not** a substitute for holding the lock. Leaf names are a validated `FileName`
newtype (no `/`, no NUL, not `.`/`..`).

**Lock retention and reclamation.** `try_lock` creates the lock file if absent and
**never unlinks it**; there is **no lock-file removal primitive** in the mutation API.
Reclamation is **not** an online operation: per §3.1 the only safe removal is under
**global quiescence** (all holders, waiters, and new openers excluded — a maintenance
window or an outer maintenance lock gating every lifecycle entry point). The
previously-proposed online `list`+`inspect`+`try_lock`+`unlink` sweep is **withdrawn**
(it splits identity despite a successful acquire — proven regression, §3.1). A
quiescence reclaimer, if built, is an operational procedure driven outside normal
traffic, not a registry primitive.

**Option A is the recommended path, so its write primitives are specified here.**
`create/append/begin_finalize/commit_finalize/session_status` resolve every leaf,
rename, and the lock through `ContainedDir` — hence `append_at`, `rename_leaf`,
`ensure_subdir`, and `run_locked` above. Specifying them is required: this document
does **not** claim Option A support while leaving its append/rename/parent-creation
operations unspecified (§3A/§7). Under an interim Option B cutover these write
primitives are unused (writers stay ambient), but the reaper/recovery subset
(`open_contained_dir`/`open_subdir`/`ensure_subdir`/`list`/`inspect`/`read_leaf`/
`try_lock`/`run_locked`/`write_leaf_atomic`/`unlink`/`truncate`) is still needed.

**Required dependency tests** (the prototype does **not** substitute): per-primitive
containment (`RESOLVE_BENEATH`/symlink/`..` rejection with real symlinks), `ENOENT`
vs `ELOOP`/`EXDEV` classification, root/ancestor-replacement coherence per op, lock
busy/acquired/**retained-across-unlink**/released, `write_leaf_atomic` temp+rename
atomic visibility with **temp cleanup on injected pre-rename failure** and durable vs
loose fsync, `ensure_subdir` idempotent parent creation + containment, `read_leaf`
bounds, nested `open_subdir` containment, `append_at` offset check, `rename_leaf`
cross-pinned-dir publish, truncate correctness, and `run_locked` **releasing the lock
only after the blocking body completes under a real Tokio abort** — as
`#[tokio::test]`s inside `storage-fs`.

---

## 6. Outcomes, missing directories, durability (design requirement 6)

**Public result contract.** `reap_expired_sessions -> Result<usize, StorageError>`:

* **Confirmed success** counts an operation whose intended terminal effect was
  achieved, **including** an idempotent `ENOENT` on re-unlink (deletion success) and
  a Finalizing **roll-forward that leaves session meta** (success = roll-forward
  completed, not deletion). A busy-skip and a safe rejection are **not** successes.
* **Per-item error** (one candidate's inspect/act failed) does **not** erase earlier
  confirmed successes: the pass returns `Ok(successes)` and surfaces per-item errors
  out of band (logs/metrics). Proven:
  `earlier_success_then_later_error_is_reported_honestly` → `Ok(1)` with a recorded
  error and the failed candidate untouched.
* **Fatal top-level failure** (the `uploads/` enumeration itself failed — the pass
  cannot be performed) returns `Err`; the scheduler retries next pass. Proven:
  `fatal_enumeration_failure_maps_to_err`. `report_to_public_result` encodes exactly
  this mapping (`fatal ⇒ Err`, else `Ok(successes)`).

This is Decision D-3, option (a): keep `Result<usize, …>`, redefine `usize` as
confirmed successes. Source-compatible for the opaque callers (§2.6).

**Missing directories.** An absent `.finalized/` must **not** prevent eligible
session processing: the pinned handle is `Option`, sessions are reaped, receipts are
simply skipped, and the pass is not an error
(`absent_finalized_dir_does_not_block_session_processing`). An absent `uploads/` is
an empty pass (nothing to enumerate), not an error. Absent `blobs/`/membership dirs
during a Finalizing recovery yield `CasUnavailable` (publish nothing), not a crash.

**Partial deletion ordering and rediscovery.** `abort_locked` removes `{uuid}.data`
and `{uuid}.hash.*` **before** `{uuid}.meta.json` (meta LAST). A crash mid-abort
therefore leaves the `.meta.json` present, so the next pass re-enumerates the
session (the loop is keyed on `*.meta.json`) and re-aborts idempotently (`ENOENT` on
the already-removed data/hash is success). Proven:
`partial_abort_is_rediscovered_and_completed`. **This corrects production, which
removes data first then meta** (`abort_session` `:2513`→`:2519`): a crash between
those leaves an orphan `.data`/`.hash` that the meta-keyed enumeration never
rediscovers — a leak. The blanket statement "any crash mid-reap simply leaves work
for the next pass" is therefore **withdrawn**: rediscovery depends on ordering, and
only the meta-LAST ordering makes an interrupted abort discoverable.

**Durability boundary — separation must not imply guaranteed crash recovery.**
Session meta/hash/receipt writes use the **loose** `write_atomic_file` (fsyncs
ignored); `commit_finalize` ignores post-rename `fsync_dir`. The roll-forward's
membership/receipt writes go through `write_leaf_atomic` (§5), whose `renameat` gives
atomic **visibility** — a concurrent reader sees the whole old or whole new leaf,
never a torn one, and a pre-rename crash leaves only a `.tmp.*` that no reader
consults — but with `durable=false` this is **not** crash **durability**: after a
power loss the rename or the bytes may not have reached stable storage. Atomic
visibility and crash durability are distinct axes, and this design provides the
former unconditionally and the latter only under `durable=true` (D-5). Consequently
the meta-LAST ordering guarantees rediscovery **only for effects that reached stable
storage**. With loose fsync, the *order of durability* is not guaranteed to match the
*order of syscalls*: a crash could make the `.data` unlink durable while the
still-present `.meta.json` was itself never durable, or reorder the two unlinks.
So the ordering discipline gives **best-effort** rediscovery, not a crash-recovery
guarantee. Fixing that requires strict fsync (Decision D-5), which is a separate
axis; we **do not** claim the separation delivers guaranteed crash recovery. Format
compatibility is preserved (same `FsSessionMetaRecord`/`FinalizedReceipt`,
`format_version: 1`).

---

## 7. Production implementation plan (all affected paths)

When the cutover is approved, the eventual change set is:

1. **Dependency (`storage-fs`)** — add the §5 `mutate` module + tests (now including
   `open_subdir`, `ensure_subdir`, `read_leaf`, `write_leaf_atomic`, `run_locked`,
   `append_at`/`rename_leaf`, lock **retention**, and the `run_locked` cancellation
   lock-release-after-op). Publish; bump the registry pin. *(Separate authorized
   change; this proposal only specifies it.)*
2. **`src/storage/fs.rs`**
   * **Stop unlinking the lock file** in `abort_session` (`:2526`) — retention
     (§3.1). Do **not** add an online reclamation sweep; lock-file reclamation, if
     performed at all, is an **out-of-traffic quiescence procedure** (§3.1), not a
     concurrent primitive.
   * Split `abort_session`/`recover_session` into public (lock-acquiring) wrappers +
     `*_locked` inner bodies that **receive** the pinned authority + lock token and
     use contained primitives (not ambient `link_repo_blob`/`write_atomic_file`).
     The Finalizing `*_locked` body performs rows 10–14 through pinned
     `uploads`/`blobs`/membership/`.finalized` handles, **creating the membership
     parent with `ensure_subdir` and replacing the membership/receipt leaves with
     `write_leaf_atomic`** (membership before receipt, §3/§3.3).
   * Change `abort_session` deletion order to **meta LAST** (§6).
   * Rewrite `reap_expired_sessions` to: open `ContainedDir` for `uploads/`,
     `.finalized/` (optional), `blobs/`, membership once per pass; enumerate; per
     candidate `try_lock` → `inspect` → age-gate → revalidate → `*_locked` dispatch
     via the owned `run_locked` boundary (moving the guard into the blocking op,
     §3.4) → confirmed-success accounting; receipts under the `.finalized/` handle
     with the continuous lock (§3.5). Route an expired `Finalizing` session to the
     **roll-forward**, mapping `RolledForward`/`AlreadyFinalized`/`CasUnavailable`/`Err`
     as in §6 — **not** truncate-to-zero (truncate stays the torn-tail branch).
3. **Writers (Option A — recommended, D-7)** — route
   `create/append/begin_finalize/commit_finalize/session_status` through the **single
   process-lifetime shared authority** + retained lock (§3A), using `append_at`,
   `rename_leaf`, `ensure_subdir`, and `run_locked` (§5). This is the write-path move
   that makes writers replacement-coherent. If an **interim Option B** cutover is
   explicitly operator-accepted, writers may stay ambient **provided** the
   exclusive-root restriction is recorded as a hard precondition **and** the registry
   implements the startup/periodic `(dev,ino)` **detection** check (§3A); the residual
   window is accepted in writing.
4. **Construction** — `storage_wiring_try_from_config` (`:879-887`) and
   `proxy_cache_storage_try_from_config` (`:961-969`): supply a reader capable of the
   new API (identical shape for both). No signature change if handles are opened on
   demand from the already-held `reader`.
5. **Callers** — `UploadCoordinator::reap_expired_uploads` (`:856-869`),
   `supervisor.rs:1253`: unchanged under D-3(a).
6. **Regression** — `test_real_reaper_root_replacement_acts_only_on_current_tree`:
   **migrate, not delete** (D-2). Under the pinned design the pass acts on the
   *detached* tree; the "fresh replacement survives" assertion still holds; the
   old-tree/`count` assertions encode today's ambient semantics and must be
   re-specified to the new contract.

**Scope honesty.** Rows 11–12 (CAS inspect, membership write) mean even the
**reaper-only** cutover reaches beyond `uploads/` into `blobs/` and the membership
subtree, requiring the §5 additions (`open_subdir`/`ensure_subdir`/`read_leaf`/
`write_leaf_atomic`/`run_locked`). Full writer coherence against ancestor replacement
(§3A Option A, **the recommended path**) is a **larger O-04 expansion** — the entire
fs upload write path onto the contained API, including `append_at`/`rename_leaf`. A
reaper-only change makes the *reaper* coherent and the *Finalizing recovery* contained,
but does **not** make `create/append/finalize` replacement-coherent; only Option A
does. An interim Option B cutover leaves the writers ambient and rests on an
operator-accepted, detection-only exclusive-root restriction with a residual window.
We state this rather than promise a reaper-only fix.

**Out of scope** (no shared dependency forces them in): quarantine GC mutation
containment, journal write durability, unrelated tag/manifest mutations. The shared
touchpoint is the additive `storage_fs` `mutate` module.

---

## 8. Compatibility, rollout, and rollback

* **On-disk formats** are unchanged (`format_version: 1`); recovery/receipt
  idempotency semantics preserved.
* **Lock protocol changes** (retention, §3.1). Therefore, on a shared storage root,
  **old lock-unlinking binaries must be stopped before rolling out a
  retention-relying binary** — mixed-binary operation on one tree reintroduces the
  split (§3.1). The prior blanket "old and new binaries can share a storage root"
  claim is **withdrawn**; it holds only for binaries that agree on lock retention.
* **Rollout**: land the dependency primitive + tests; then the reaper cutover behind
  existing reaper scheduling. Under the recommended **Option A**, also land the
  write-path move onto the shared authority. Under an interim **Option B**, assert the
  exclusive-root restriction in deployment **and** enable the registry's `(dev,ino)`
  detection check (§3A); the residual window is accepted in writing. No data migration.
  Lock-file reclamation, if done, is a separate **quiescence** procedure, never an
  online sweep (§3.1).
* **Rollback**: reverting the registry binary restores the ambient reaper and the
  lock-unlinking behaviour — which **reintroduces the lock-identity defect** (§3.1)
  and the meta-first abort ordering (§6). Data remains format-valid. **Rollback does
  not reverse any deletion already performed** — reaping is destructive and
  forward-only. This is a containment/correctness change, not a transactional one.

---

## 9. Decisions requiring user approval

Routine engineering choices (helper names, module layout, error-enum spelling) are
**not** listed. The substantive decisions:

* **D-1 — Adopt locked-inner `recover_session_locked`/`abort_session_locked` that
  receive pinned authority + a lock token and use contained primitives; hold one
  lock across inspect→act.** *Recommend:* yes. *Consequence:* eliminates the
  drop-before-act window; public methods keep their contract.
* **D-2 — Migrate the root-replacement regression to the pinned contract** (act on
  the detached tree; replacement survives) instead of deleting it. *Recommend:*
  migrate.
* **D-3 — Redefine `count` as confirmed successes, keeping `Result<usize, …>`**
  (option a) with the fatal-vs-per-item mapping of §6. *Recommend:* (a).
* **D-4 — Accept the documented ceiling: cooperative-lock + revalidation, not
  protection against a lock-ignoring/hostile actor** (§3.7). *Recommend:* accept.
  *Note:* distinct from the now-**fixed** cooperative lock-identity defect (§3.1).
* **D-5 — Treat crash durability (loose `write_atomic_file`, ignored `fsync_dir`) as
  a separate effort**, but **do not** treat that separation as a crash-recovery
  guarantee (§6). *Recommend:* yes.
* **D-6 — Add the mutation primitives (now incl. `open_subdir`/`ensure_subdir`/
  `read_leaf`/`write_leaf_atomic`/`run_locked`, plus `append_at`/`rename_leaf` for the
  recommended Option A write path) to `storage-fs`**, not duplicate a backend in
  registry-rust. *Recommend:* add to the dependency (separate authorized change).
* **D-7 — Writer/reaper ancestor-replacement contract (§3A): Option A (one
  process-lifetime shared authority carried through all lifecycle participants — the
  large O-04 write-path expansion) vs Option B (operator-accepted, detection-only
  exclusive-root restriction — small code, residual window, not writer-coherent).**
  *Recommend:* **Option A as the one correct path.** *Consequence:* the full fs upload
  write path moves onto the shared authority (§5/§7); it is the only contract that
  makes writers replacement-coherent. If the cutover must precede that move, it ships
  under Option B **only as an explicitly operator-accepted interim restriction** with
  the residual window recorded and the `(dev,ino)` detection check enabled — Option B
  is not presented as writer coherence.
* **D-8 — Lock-file retention with reclamation ONLY under global quiescence (§3.1)**,
  replacing `abort_session`'s inline lock unlink; the previously-proposed online
  acquire→absence→unlink sweep is **withdrawn as unsafe** (proven regression).
  *Recommend:* yes. *Consequence:* a single stable lock domain; **unbounded**
  accumulation of empty lock files (∝ historical session count) reclaimable only
  out-of-traffic; a rollout constraint (D-7/§8) that old lock-unlinking binaries not
  run concurrently.

**The reaper gap remains OPEN.** This document is a proposal and an executable
demonstration of the mechanism; it does not resolve the gap or change production.

---

## 10. Status and canonical gates

**NOT PRODUCTION-READY.** As shipped here, production still runs the ambient reaper:
its Finalizing recovery escapes any single authority (rows 11–12 touch `blobs/` and
the membership subtree ambiently), its abort unlinks the lock file (cooperative
split, §3.1) and removes meta first (non-rediscoverable partial abort, §6), and its
writers are not replacement-coherent (§3A). This document specifies the coherent
design and proves the mechanism; it does not make any production operation stay
within its declared authority or synchronization boundary. It must not be labelled
production-ready until the D-1…D-8 decisions are taken and implemented (with the
dependency API of §5 landed and tested).

All canonical gates remain **OPEN**: O-03, O-04, O-05, O-06, O-13, O-15, O-16,
D-06. O-04 (write-side containment) is the gate this proposal is scoped to unblock,
pending §9 and the dependency primitives in §5 — and, per §3A/§7, full O-04 closure
for the *writers* is Option A, larger than the reaper cutover.
