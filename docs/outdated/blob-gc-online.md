> **Historical design document — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Implemented with significant divergences: `consistency_gate` became `ConsistencyCoordinator` guards; the publication machine is seven steps (heal step added); S3 online GC exists (the "non-goal" was exceeded); the thin-client CLI was not adopted (REQ-014); the 72 h finalize grace is not wired — actual pin TTL is 1 h (REQ-012 / KI-03); EXDEV cross-device refusal is not implemented (REQ-013); a background scheduler and membership sweep exist and are undocumented here. Current reference: [gc-operations.md](../operations.md) · [requirements.md](../requirements.md) · [known-issues.md](../technical-debt.md).

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

## Backend Strategies: Filesystem vs. S3

The garbage collector supports two production storage backends with distinct physical storage mechanics:

### 1. Filesystem Backend (`FsStorage`)
- **Quarantine-and-Delay Strategy:** Candidates are first quarantined via atomic `rename()` from `blobs/sha256/<p2>/<hex>` to `quarantine/blobs/sha256/<p2>/<hex>`. An epoch timestamp metadata file is written to `quarantine/meta/sha256/<p2>/<hex>.ts`.
- **Read Fallback:** While quarantined, the blob remains readable on pull requests (`LIVE` checked first, then `QUARANTINED`).
- **Delayed Final Delete:** Candidates are only permanently deleted after `quarantine_delay_secs` has elapsed AND a fresh reachability revalidation succeeds under the shared `consistency_gate`.
- **Restoration:** If a quarantined blob is re-referenced before deletion, it is atomically restored back to `LIVE`.

### 2. S3 Backend (`S3Storage`)
- **Direct Conditional-Delete Strategy:** S3 does not support atomic cross-directory renames or quarantine fallbacks. S3 GC operates via direct conditional deletion using S3 entity tags (`If-Match: "<etag>"`).
- **Quarantine Gating:** Invoking quarantine on S3 returns HTTP `422 Unprocessable Entity` with typed error `StrategyUnsupported`. Quarantine is rejected fail-closed on S3.
- **Bucket Versioning Requirement:** Physical S3 GC requires a verified **Unversioned** bucket (`S3BucketVersioningState::Unversioned`).
- **Fail-Closed on Versioned / Unknown Buckets:**
  - `Enabled` or `Suspended` versioning: S3 GC refuses deletion and fails closed with `StrategyUnsupported`. S3 delete markers do *not* constitute physical GC, and physical GC is disabled to prevent silent storage leaks.
  - `UnknownOrDenied` (e.g. 403 Forbidden on `GetBucketVersioning`): Fails closed with `StrategyUnsupported`.
  - Zero conditional delete requests are transmitted to S3 when in any non-unversioned state.
  - On registry startup, a preflight check logs an actionable warning if S3 versioning is enabled or unreadable.

---

## Writer Authority, Coordination, and Publication Invariants

### 1. Storage Mutation Authority (`RuntimeMutationAuthority`)
- All GC operations and lifecycle mutations require holding a cluster-wide mutation authority lease (storage lock).
- Offline CLI and online supervisor processes mutually exclude each other via storage lock acquisition.
- If authority renewal fails, workers terminate and GC fails closed.

### 2. Consistency Gate (`consistency_gate`)
- An in-process mutex (`Arc<Mutex<()>>`) serializes GC reachability revalidation against active mutations (manifest publications, tag updates, membership migrations, and blob finalization).
- Candidate reachability is evaluated once during discovery, and revalidated under the gate immediately prior to deletion.

### 3. Unified Proxy & Upload Blob-Publication Invariant
Both upload finalization and proxy cache writes follow the exact 6-step state machine in `BlobUploadCoordinator`:
1. Stream and digest-verify payload.
2. Acquire durable `PinLeaseGuard` in `BlobRefIndex` with active heartbeat renewal.
3. Publish payload into CAS store (`blobs/sha256/<p2>/<hex>`).
4. Under `consistency_gate`: link repository membership in storage, record in `BlobRefIndex`, flush index, and mark ready.
5. Release `consistency_gate`.
6. Stop heartbeat and release pin *only after* membership link is durable.

### 4. Crash Recovery
- If the process crashes between CAS publication and membership linking, the pin expires after its TTL, allowing GC to reclaim the orphan.
- If the process crashes after membership linking, the blob is protected by durable repository membership even after pin expiration.

---

## Operator Testing & Qualification

### Running MinIO Integration Tests
To qualify against local MinIO (e.g. `docker.io/minio/minio:RELEASE.2025-09-07T16-13-09Z` on `http://127.0.0.1:9000`):
```bash
cargo test --locked --all-features --test s3_live_integration -- --nocapture
```

### Running Real AWS GC Contract Runner
To run destructive contract tests against a live AWS S3 account, invoke the opt-in runner with explicit account confirmation:
```bash
EXPECTED_AWS_ACCOUNT_ID=441104250201 \
AWS_REGION=us-east-1 \
./scripts/run_aws_gc_contract.sh
```

### Destructive Test Safety Model
1. **Endpoint Safety Guardrails:** Non-local endpoints require explicit `ALLOW_NON_LOCAL_S3_DESTRUCTIVE_TESTS=1`.
2. **Account Guardrails:** STS caller account must match `EXPECTED_AWS_ACCOUNT_ID` before any bucket operation.
3. **Prefix Isolation:** Live test harnesses generate unique UUID prefixes (`live-test-<uuid>/`) and purge only matching test objects.
4. **Bucket Cleanup:** The AWS runner generates an ephemeral bucket (`registry-rust-gc-contract-<uuid>`), applies public access block, runs tests, empties objects/markers, deletes the bucket, and verifies 404 deletion status.

---

## Security Advisories (`cargo audit`)
The project baseline permits three non-exploitable advisory warnings:
1. `fxhash` 0.2.1: `RUSTSEC-2025-0057` (unmaintained upstream).
2. `instant` 0.1.13: `RUSTSEC-2024-0384` (unmaintained upstream).
3. `lru` 0.16.4: `RUSTSEC-2026-0253` (potential lack of panic safety in `LruCache::pop()`, unused in panic-unwind paths).
