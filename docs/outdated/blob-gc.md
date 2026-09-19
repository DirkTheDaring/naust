> **Historical design document — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> This is the original GC concept (pre-refactor). The current operational reference is [gc-operations.md](../operations.md); the safety model introduced here is tracked as REQ-010 in [requirements.md](../requirements.md). Known divergences from this text: [known-issues.md](../technical-debt.md) KI-03, KI-05 and requirements REQ-012/REQ-013/REQ-014. Do not use the CLI/flag descriptions below; they do not match the shipped CLI.

# Safe dangling-blob GC concept

This document describes the **general GC safety model** and the current **offline maintenance** approach.

For the fully online (while-running, in-process) design, see `docs/blob-gc-online.md`.

This document defines a **safe** approach to reclaim storage from **dangling (unreferenced) blobs** without risking deletion of blobs needed by:

- existing images/tags
- digest-based pulls
- in-flight (not yet tagged) pushes
- concurrent writes/reads

It is written for the **filesystem backend** first (where we can enumerate blobs by walking `fs_root/blobs/sha256/**`).

## Definitions

- **Blob**: content-addressed object stored at `blobs/sha256/<prefix2>/<hex>`.
- **Root**: an object that makes content “in use”. The strictest/most common definition is **any tag** (`repos/<name>/tags/<tag>`) pointing at a manifest digest.
- **Reachable**: a blob is reachable if it is referenced by a root manifest/tag directly or through a chain of manifests/indexes/artifacts.
- **Dangling blob**: a blob present on disk but not reachable from any root.
- **In-flight push window**: time where a client has already finalized some blobs, but has not yet uploaded the manifest / applied the tag.
- **Pin / lease**: a short-lived “treat as in-use” record for a digest (used to protect in-flight pushes and recently finalized blobs in online mode).

## What can create dangling blobs

Dangling blobs are normal in OCI/Docker flows:

1. Client uploads and finalizes layer blobs.
2. Client uploads manifest.
3. Client sets/updates tag.

If the client aborts after step (1), the registry has valid blobs on disk that are not referenced by any tag.

## Safety goals / invariants

A safe GC must maintain these invariants:

1. **Never delete reachable blobs** (by the chosen “root” policy).
2. **Never delete blobs that might become reachable soon due to an in-flight push**, unless they are older than a conservative grace period.
3. Be robust to **concurrency**: while GC is running, clients may upload manifests/tags, delete manifests, or pull blobs.
4. Provide an operator-friendly recovery story:
   - dry-run first
   - observable logs/metrics
   - idempotent
   - does not require downtime
5. If the blob reference index is enabled, the GC must either:
   - require the index to be healthy, or
   - rebuild it before proceeding.

## Policy choices (must be explicit)

There are two common policies for what counts as “in use”. We should support both and make the default conservative.

### Policy A: tag-rooted reachability

A blob is protected if it is reachable from any current tag root.

- Pros: best space reclamation; matches “delete by tag removes content eventually” expectation.
- Cons: breaks workflows that rely on pulling by digest long after a tag was removed.

### Policy B (conservative default): manifest-rooted reachability

A blob is protected if it is reachable from any stored manifest, whether or not that manifest is tagged.

- Pros: safer for digest-pull workflows.
- Cons: much less space reclaimed unless manifests are also GC’d.

Important nuance: this preserves digest-based pulls **of manifests** (and their referenced blobs) as long as the manifest object remains stored. It does not guarantee that an arbitrary blob digest remains available forever if it is not referenced by any manifest.

Why this is a good default: it still reclaims the storage you care about most (blobs created by incomplete pushes where the manifest was never uploaded), while reducing the chance of breaking digest-based workflows.

## Core approach: two-phase GC with quarantine + grace period

A simple “delete everything unreferenced” is unsafe due to in-flight push windows.

The safe concept is:

1. **Grace period**: only consider blobs older than `min_age` (e.g. days). This prevents deleting blobs from slow/in-flight pushes.
2. **Quarantine phase**: move candidates out of the live blob store first (atomic rename). Do not permanently delete immediately.
3. **Recheck before final delete**: if a blob becomes reachable after quarantine, restore it.

For a fully online system (no downtime), this is typically combined with **pins/leases** and a **read fallback to quarantine**; see `docs/blob-gc-online.md`.

This converts a risky destructive operation into a reversible, observable process.

### Why quarantine helps

- It allows a second reachability check after a delay.
- It reduces blast radius if the reachability logic is wrong.
- On filesystem, `rename()` is atomic; no partial state.

## Proposed filesystem layout

Add a quarantine area under the same `fs_root`:

- `quarantine/blobs/sha256/<prefix2>/<hex>`

(Keeping it inside the same filesystem makes `rename()` cheap and atomic.)

## Algorithm (policy-selectable)

Inputs:

- `min_age` (required for safety; default should be conservative, e.g. 7 days)
- `quarantine_delay` (time a blob must remain quarantined before permanent deletion, e.g. 24h)
- `max_per_run` (limit work per run)
- mode: `dry-run | quarantine | delete`

Recommended starting defaults given that pushes can take up to ~36 hours in degraded environments:

- `min_age`: 7 days (or at least 72 hours if you need faster reclamation)
- `quarantine_delay`: 24 hours

Steps:

### Step 0: Preconditions

- Only supported for filesystem backend initially.

Offline maintenance mode (current implementation):
- **Must not run concurrently with a live registry process** that uses the same `fs_root`.
  - Enforced by an exclusive lock file: `fs_root/.locks/registry-rust.lock`.
  - The registry server holds this lock for its whole runtime (filesystem backend).
  - `registry-rust blob-gc ...` and `registry-rust ref-index ...` refuse to run if the lock is held.

Online mode:
- Do not use the external CLI to mutate storage while the server is live.
- Use an in-process GC service and make quarantine readable; see `docs/blob-gc-online.md`.
- If ref-index is enabled:
  - run `ref-index ensure` behavior: check health; rebuild if corrupted/forced.
- If ref-index is disabled:
  - either refuse to run (recommended), or fall back to a full scan-based reachability check (slow).

### Step 1: Enumerate blob candidates

- Walk `fs_root/blobs/sha256/**/<hex>`.
- For each blob file:
  - parse digest from filename
  - read file mtime (or metadata modified time)
  - if `now - mtime < min_age`: **skip**
  - else: candidate

### Step 2: Reachability check

For each candidate digest:

- Policy A (tag-rooted): if `is_blob_referenced(digest)` is true: **skip**
- Policy B (manifest-rooted): **skip** if either:
  - `is_blob_referenced(digest)` is true (reachable from any tag root), or
  - the blob is reachable from any stored manifest object (treat all stored manifests as additional roots for this GC run)

Otherwise: eligible

### Step 3: Quarantine (reversible)

For each eligible blob (up to `max_per_run`):

- `rename(blobs/…/<hex> -> quarantine/blobs/…/<hex>)`
- record an entry in a small “gc state” log (optional but recommended):
  - digest
  - original path
  - quarantine time

If `rename` fails because the file is gone, ignore.

### Step 4: Final deletion (after delay + recheck)

For blobs already in quarantine:

- if `now - quarantine_time < quarantine_delay`: skip
- re-run reachability check:
  - if referenced now: restore with `rename(quarantine -> blobs)`
  - if still unreferenced: delete file

This ensures that a push which completes after quarantine can still recover content.

## Handling in-flight pushes explicitly (extra belt-and-suspenders)

Grace period is usually enough for offline runs, but online designs typically need a stronger primitive:

- **Pins/leases**: on `finalize_upload`, record `digest -> pinned_until` (e.g. `finalize_grace`) so GC treats the blob as in-use even if no tag exists yet.
- GC must skip pinned blobs even if reachability says “unreferenced”.

This avoids relying solely on filesystem timestamps and better covers the finalized-but-not-yet-tagged window.

## Concurrency considerations

- Reads: deleting a blob currently being served can break clients. Quarantine-first reduces risk:
  - If you quarantine by rename, a currently-open file descriptor can still be read to completion on Unix, but new opens will fail.
  - Therefore, run final deletion only after a delay.
  - For true online operation, the server must also check quarantine on reads to avoid transient 404s (see `docs/blob-gc-online.md`).
- Writes: a blob can become referenced at any time after its layers exist.
  - Quarantine + recheck ensures we don’t permanently delete blobs that became referenced after the first check.
- Multiple registry instances:
  - If multiple processes share the same `fs_root`, run GC from only one instance (leader election / operator cron on one node), or use a lock file in `fs_root`.

## Failure modes and mitigation

- **Ref-index corruption**: refuse to proceed unless `ref-index ensure` succeeds.
- **Partial GC run**: safe; quarantine and delete phases are idempotent.
- **Clock skew / bad mtime**: prefer persisted `finalized_at` timestamps if implemented.
- **Disk full during quarantine**: quarantine is rename, so it doesn’t require extra space (within same filesystem).

## Operator UX (recommended)

Offline maintenance UX (current):

- `blob-gc plan --min-age-secs ...` (dry-run summary)
- `blob-gc quarantine --min-age-secs ... --max-per-run ...`
- `blob-gc delete --quarantine-delay-secs ... --max-per-run ...`

Online UX (preferred long-term):
- An authenticated admin trigger that runs the same phases **in-process**, to keep a single writer for sled/ref-index and avoid live read races.

Defaults should be conservative:

- dry-run by default
- require explicit `--delete`
- `min_age` large enough to cover worst-case push durations in your environment

## Metrics / logging

Track:

- blobs scanned / eligible / quarantined / restored / deleted
- bytes eligible / quarantined / restored / deleted
- ref-index health and rebuild events

## Non-filesystem backends (S3)

For S3-backed deployments, GC operates via direct conditional deletion (`If-Match: "<etag>"`) on verified unversioned buckets. See `docs/blob-gc-online.md` for the complete S3 GC architecture, bucket versioning requirements, and operator qualification run instructions.
