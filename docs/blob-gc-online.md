# Online-safe blob GC architecture (in-process)

This document defines a **reviewable architecture** for reclaiming disk space from unreferenced blobs **while the registry is running**, without causing transient pull failures or risking ref-index corruption.

Scope:
- Filesystem backend (`fs_root`) only (initially).
- GC runs **inside the server process** (single writer for sled/ref-index).
- Deletion is **two-phase** (quarantine, then delayed delete with re-check).

Relationship to `docs/blob-gc.md`:
- `docs/blob-gc.md` defines the general GC concept, policies, and the current offline maintenance CLI.
- This document tightens the design to be safe **while the server is running** by adding pins/leases, quarantine readability, and a single-writer execution model.

Non-goals (for this phase):
- S3 online GC.
- Perfect “global snapshot” of all references at a single instant.
- Immediate space reclamation (safety wins over speed).

## Safety invariants

The system is considered safe if these invariants hold:

- **I1: No permanent deletion of reachable blobs** under the configured policy.
- **I2: Reads must not break due to GC**: a pull should not 404 because GC moved a blob.
- **I3: Crash-safe and idempotent**: crashes may delay reclamation but must not lose data.
- **I4: No cross-process corruption of sled**: only the server process mutates sled/ref-index state.
- **I5: GC is monotone-safe**: the system may delay reclamation, but must not create new unavailability or corruption while making progress.

Practical interpretation:
- The design prefers **false negatives** (keep blobs) over **false positives** (delete blobs).

## Race classes and how they are avoided

### Race A: Writer makes a blob reachable after GC decided it was unreferenced
Example: client finalized blobs, then later uploads manifest + sets tag.

Mitigation:
- **Pins/leases** protect recently finalized blobs and in-flight pushes.
- **Quarantine + delayed delete**: final delete requires a **fresh reachability re-check**.

### Race B: Reader pulls while GC moved/deleted a blob
Mitigation:
- **Quarantine must remain readable**.
- Blob read path must check:
  1) `LIVE` (`blobs/...`)
  2) if not found: `QUARANTINED` (`quarantine/blobs/...`)

This neutralizes transient 404 windows from `rename()`.

### Race C: GC and server concurrently mutate the ref-index / sled
Mitigation:
- Run GC **in-process**.
- Ensure a **single-writer rule** for sled (no external CLI opens the DB while the server runs).

## Core data model

### Blob locations

A blob digest `sha256:<hex>` maps to paths:

- LIVE: `fs_root/blobs/sha256/<prefix2>/<hex>`
- QUARANTINED: `fs_root/quarantine/blobs/sha256/<prefix2>/<hex>`

Quarantine metadata:
- `fs_root/quarantine/meta/sha256/<prefix2>/<hex>.ts` contains an epoch timestamp for `quarantine_at`.

Filesystem constraints (must be true for the atomicity guarantees):
- `quarantine/` must be on the **same mounted filesystem** as `blobs/` so that `rename()` is atomic. If operators place them on different devices, the system must refuse online quarantine (or fall back to copy+fsync, which is significantly more complex).
- The implementation must never follow symlinks when resolving blob/quarantine paths (defense-in-depth).

### Pins / leases (in-flight protection)

A pin is a short-lived protection record keyed by digest.

Design options:
- Preferred: persisted in sled as a tree (e.g. `pins`) owned by the server process.
- Minimal fallback: small files under `fs_root/quarantine/pins/...` (works, but less structured).

Pin semantics:
- `pin(digest, pinned_until, reason)`
- `is_pinned(digest, now)` returns true if `now < pinned_until`.

Pins are used to protect:
- **recently finalized blobs** (covers finalized-but-not-yet-tagged window)
- optionally, blobs associated with active upload sessions (if mapping exists)

Recommended initial default:
- `finalize_grace`: 72h (given observed pushes up to ~36h)

Time semantics (best practice):
- Pins should use **wall clock** timestamps for persistence (so they survive restart), but logic should tolerate clock steps.
- Treat “time went backwards” conservatively: if `now` is earlier than stored timestamps, prefer keeping data (do not delete).

## State machine

Every blob is conceptually in one of these states:

- `LIVE`: present at LIVE path
- `QUARANTINED`: present at quarantine path and has `quarantine_at`
- `DELETED`: absent from both

Transitions:

1) `LIVE -> QUARANTINED` (phase 1)
- Preconditions: blob is not reachable (policy) AND not pinned AND older than `min_age`.
- Action: atomic `rename(LIVE, QUARANTINED)`.
- Persist `quarantine_at` metadata.

2) `QUARANTINED -> LIVE` (restore)
- Condition: blob becomes reachable OR pinned before final deletion.
- Action: atomic `rename(QUARANTINED, LIVE)`.

3) `QUARANTINED -> DELETED` (phase 2)
- Preconditions: `now - quarantine_at >= quarantine_delay` AND still not reachable AND not pinned.
- Action: delete/unlink quarantined file.

Crash safety:
- Crash between rename and metadata write: blob remains in quarantine and must remain readable.
- Re-running GC is safe; operations must tolerate `NotFound` and “already moved”.

Durability notes (industry best practice):
- If you want strong power-loss durability: after creating/updating quarantine metadata, `fsync()` the file and parent directory; after `rename()`, `fsync()` the destination directory. Many systems accept “eventual correctness after reboot” here; document the chosen durability level explicitly.

## Reachability policy (what counts as “in use”)

The system supports both policies already defined in `docs/blob-gc.md`:

- **Tag-rooted**: reachable from any current tag root.
- **Manifest-rooted (conservative default)**: reachable from any stored manifest.

In online mode, the **effective protection** is:

A blob is protected if **any** of the following is true:

1) Reachable by policy at check time.
2) Pinned/leased at check time.
3) Newer than `min_age` (defense-in-depth).

The snapshot is used for efficiency; correctness depends on quarantine readability + re-check.

Ref-index health best practice:
- GC should only trust the reachability oracle if the ref-index is **healthy**.
- If the index is not healthy, either:
  - refuse to run online GC (recommended), or
  - rebuild the index in-process and only proceed once it reaches a `ready` state.

The key safety rule: never delete based on an index that is known to be incomplete.

## Execution model (in-process service)

### Why in-process
- `sled` is not a general-purpose multi-process transactional database.
- In-process GC enforces the single-writer rule and avoids index corruption and subtle interleavings.

### Service boundary

Introduce an internal `GcService` with:
- a global `Mutex<()>` to allow only one GC run at a time
- methods: `plan`, `quarantine`, `delete`
- dependencies:
  - storage (filesystem)
  - ref-index (reachability)
  - pin store

Additional best-practice behaviors:
- Run GC in a **low-priority/background** task pool (avoid starving request handling).
- Enforce **rate limits** / budgets per run (max blobs, max bytes, max wall-clock time) to keep tail latency predictable.
- Make each phase **restartable** without manual cleanup.

### Triggering GC

Expose an authenticated admin trigger (HTTP) to invoke `plan/quarantine/delete`.

Rationale:
- preserves safe invariants
- keeps CLI UX possible (CLI can call admin endpoint) without opening sled externally

Important: a standalone CLI must not open sled/ref-index while the server is live.
If a CLI workflow is desired for operators, it should act as a thin client that calls the in-process admin trigger.

Security best practices:
- Keep admin GC endpoints off the public registry surface by default (bind to loopback, behind admin auth, or require an explicit config flag).
- Require a strong permission such as “admin/maintenance”; do not reuse broad “push” scopes unless they are already tightly controlled.
- Log all GC actions with a unique run id and include the authenticated subject.

## Operational defaults and rollout

Recommended rollout sequence:

1) Implement quarantine-read fallback in blob read path.
2) Add pin-on-finalize (lease) to cover slow/in-flight pushes.
3) Enable online **quarantine-only** runs; observe.
4) Enable online **delete** after quarantine delay + recheck.

Recommended conservative defaults:
- `min_age`: 7 days
- `quarantine_delay`: 24h
- `finalize_grace` pin: 72h

Operational safety toggles (strongly recommended):
- A global kill switch: `gc.enabled=false` stops quarantine/delete immediately.
- Phase gating: allow `plan` and `quarantine` separately from `delete`.
- “Dry-run first” should remain the standard workflow.

## Open questions for review

- Pin persistence location: sled tree vs filesystem metadata files.
- Authentication model for admin GC trigger (reuse existing push auth, or separate admin token).
- Multi-instance deployments sharing `fs_root`: leader-only GC vs coordination.

## Well-known failure patterns (and mitigations)

These are common ways online deletion systems break; this design addresses them explicitly.

1) TOCTOU: “unreferenced at scan time, referenced at delete time”
- Mitigation: quarantine + delay + **re-check** before delete.

2) In-flight pushes: finalized blobs are not yet referenced by any tag
- Mitigation: pin-on-finalize (`finalize_grace`) and conservative `min_age`.

3) Pull races: GC moves content while a reader is opening it
- Mitigation: **quarantine is readable** (LIVE first, then QUARANTINED).

4) Unbounded work: GC starves the server or triggers thundering herds
- Mitigation: budgets per run (max blobs/bytes/time), low-priority execution, and explicit operator scheduling.

5) Clock issues: NTP steps, clock skew after reboot, bad mtimes
- Mitigation: treat clock anomalies conservatively; prefer pins and explicit timestamps over mtime-only decisions.

6) Partial metadata / crash during phase transitions
- Mitigation: design is correct even if metadata is missing; worst case is “quarantine but keep readable”, and retry later.

7) Multi-instance coordination failures
- Mitigation: single in-process writer per instance; for shared `fs_root`, require leader-only GC or a shared coordination mechanism.

8) Index health / corruption leading to false deletion
- Mitigation: refuse online delete unless ref-index reports healthy; rebuild in-process if configured.
