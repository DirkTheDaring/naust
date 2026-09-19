> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Landed (`634da23`); timestamps later moved onto `repo_timestamp_domain` (`84dbe13`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Contained Filesystem Repository Timestamps and Storage-Emptiness Inspection

- **Document:** `docs/architecture/filesystem-timestamps-and-emptiness-containment.md`
- **Status:** Implementation & Compatibility Record (working tree, not committed)
- **Primary Repository Baseline:** `/home/dietmar/devel/rust/registry-rust` at HEAD `3b647132f867accd7551ddb0ca297064be89abb9` (`master`), changes applied in the working tree only.
- **Dependency Repository:** `/home/dietmar/devel/rust/storage-layer-rust` at HEAD `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (strictly read-only; unchanged — the required primitive, `FsMetadataReader::inspect_file_metadata`, already existed).
- **Scope:** Gap-inventory items R-4 (`repo_timestamps` / `max_mtime_in_dir`) and R-5 (`is_storage_empty`'s `fs_dir_has_any_entry` probes). The `list_repositories` leg of `is_storage_empty` was already contained by the committed catalog batch.

---

## 1. Source and Caller Traces (as verified against current source)

### 1.1 `FsStorage::repo_timestamps` (ambient, replaced)

Legacy behavior: symlink-following `tokio::fs::metadata` existence probe of
`repos/<name>` (any object type passed; missing → `NotFound`); then
`max_mtime_in_dir` over `tags/` and `manifests/`: ambient `read_dir` (missing
dir → `None`; other open errors → `Io`), **silent truncation** on
mid-iteration errors (`while let Ok(Some(..))`), per-entry symlink-following
`entry.metadata()` with **silent skip** of metadata/`modified()` failures,
`is_file()` filtering (so symlinks-to-files contributed the target's mtime),
direct children only, hidden files included, maximum selected.

Consumers (actual invocation sites):
- `CatalogQueryService::repo_timestamps` → HTTP `_meta` handlers
  (`src/http_api/catalog.rs`): `meta_orgs_repos` (per-repo errors suppressed
  to default timestamps — unchanged, documented), `meta_repo` (previously
  **all** errors → 404 `NAME_UNKNOWN`; corrected, see §4), `meta_repos`
  (already distinguishes `NotFound` from other errors → 500).
- Forwarding adapters in `supervisor.rs` and `manifest_lifecycle.rs` (trait
  plumbing, no policy).
Consequence of a wrong/missing timestamp: informational staleness in `_meta`
responses only; no mutation consumes these values.

### 1.2 `FsStorage::is_storage_empty` / `fs_dir_has_any_entry` (ambient probes, replaced)

Legacy behavior: after the (already contained) `list_repositories` check, an
ambient recursive `read_dir` walk over seven areas in order — `blobs`,
`uploads`, `quarantine`, `repo-blobs`, `repo-memberships`, `repos`,
`journals` — returning "not empty" on the first **non-directory** dirent,
descending directory entries, `NotFound → false`, propagating iteration and
`file_type` errors as `Io`. Symlinked area roots and ancestors were followed.

Safety-relevant consumer (`src/runtime.rs` startup): when
`is_membership_ready()` is false, `is_storage_empty()` == `Ok(true)` causes
**`mark_membership_ready()` to be written** (fresh-install path); `Ok(false)`
fails startup with `MembershipBackfillRequired`; `Err` fails startup with
`MembershipInspection`. A **falsely empty** result would therefore mark
membership ready over a populated registry and skip required backfill — the
exact incomplete-observation hazard this batch removes. Error paths were
already fail-closed; no caller change was needed there.

## 2. What Was Implemented

New module `src/storage/fs/timestamps_emptiness.rs`:
- `RepoMetaInspector` seam (contained `enumerate_dir` + `inspect_file_metadata`)
  implemented by the shared pinned `FsMetadataReader`; no per-operation reader
  opening, no ambient fallback. Blocking syscalls run inside the dependency's
  `spawn_blocking` offload.
- `repo_timestamps_impl`: structural name validation → contained repository
  existence probe (one enumeration; missing → `NotFound`) → contained
  enumeration of `tags/` and `manifests/` (missing/vanished → `None`) →
  per-regular-entry `inspect_file_metadata` (`openat2 O_PATH` + `fstat`,
  `S_IFREG`-enforced, checked nanosecond timestamp conversion; pre-epoch
  representable) → maximum selection.
- `contained_subtree_has_any_entry`: breadth-first contained walk; first
  non-directory dirent concludes "not empty" without further enumeration (the
  legacy short-circuit — a qualifying observed entry justifies the early
  conclusion); directory entries descend; missing/vanished directories are
  empty contributions; uninspectable areas fail closed.
- Shared error mapping reused: `catalog_discovery::map_contained_dir_error`
  (made `pub(crate)`, renamed from the catalog-local name, with a generalized
  per-directory-limit message) and `read_adapter::translate_metadata_read_error`.

Production routing (`src/storage/fs.rs`): `repo_timestamps` and the emptiness
subtree loop now delegate to the module over `self.reader.as_ref()`; the
ambient `max_mtime_in_dir` method and `fs_dir_has_any_entry` function were
removed. Area list and short-circuit order are unchanged. Primary and
proxy-cache filesystem storages share this single `FsStorage` code path.

Narrow caller correction (`src/http_api/catalog.rs`, `meta_repo`): storage
`NotFound` and `InvalidRepoName` still present as 404 `NAME_UNKNOWN`
(preserving the visible behavior for missing and traversal names, which
previously produced ambient `NotFound`); every other error now returns 500
instead of masquerading as 404. No other caller changes; checkpoint,
readiness, cursor, lease, and retry contracts untouched.

## 3. Preserved Semantics vs. Intentional Changes

Preserved (test-frozen): missing repository → `NotFound`; missing/vanished
`tags`/`manifests` → `None`; direct-children-only scan; only regular files
contribute; hidden regular files (e.g. persisted `.lock.*`) still contribute;
empty directory → `None`; maximum selection; wrong-type `tags`/`manifests`
and unreadable directories → legacy `Io`; emptiness `NotFound → false`,
non-directory-entry short-circuit, descent through directories,
empty-directory trees count as empty, area order.

Intentional containment changes (each test-frozen):
1. **Pinned-root resolution** — root pathname replacement no longer redirects
   either operation to the replacement tree.
2. **Symlinked path components rejected** (`Io`): symlinked repository dir,
   `tags/`/`manifests/`, or emptiness area root (e.g. `blobs → elsewhere`) —
   previously followed. For emptiness this also means an area hidden behind a
   symlink can never contribute a false empty result.
3. **Symlink entries no longer contribute timestamps** (dirent-type policy;
   previously a symlink-to-file contributed the target's mtime). Symlink
   entries in the emptiness walk still count as entries (non-directory dirent
   → not empty), unchanged.
4. **No maximum over an incomplete relevant set**: mid-iteration enumeration
   failures and genuine per-entry inspection failures (permission denial,
   stat failure, invalid metadata, containment resolution rejection,
   unsupported environment) now propagate instead of silently
   truncating/skipping; no successful partial timestamp result is returned
   after such a failure. Entries that are genuinely absent (`NotFound`) or
   confirmed non-regular objects at inspection time (`UnsupportedObjectType`,
   established by `fstat` on the acquired leaf descriptor) are excluded as
   non-qualifying — absence is distinguished from failure, matching the
   legacy filtering; observed symlink dirents are filtered during
   enumeration. A containment `ResolutionRejected` (`ELOOP`/`EXDEV`) is
   **not** suppressed: the kernel rejects the whole path resolution, which
   does not establish the leaf's object type and can stem from an ancestor
   substitution (e.g. the `tags/` directory replaced by a symlink after
   enumeration), so it propagates through the existing
   `translate_metadata_read_error` mapping (legacy `Io`). Containment does
   not provide snapshot isolation; propagation, not suppression, is what
   prevents a substituted ancestor from yielding `Ok(None)` or a maximum
   over only earlier-inspected files.
5. **Unaddressable names fail closed** (`CorruptData`): non-UTF-8 or
   key-unformable (backslash/control) regular-file names in
   `tags`/`manifests` (legacy included them in the maximum without needing
   the name; contained inspection cannot address them, and silent exclusion
   would produce an incomplete maximum); non-descendable directory names in
   the emptiness walk (could hide entries → false empty). Non-directory
   entries in the emptiness walk never need a name.
6. **Traversal repository names** → `InvalidRepoName` at the storage boundary
   (previously ambient resolution, in practice `NotFound`); the `meta_repo`
   route still presents 404 for them.

## 4. Error Handling and Caller Consequences

- `repo_timestamps` new error classes reach: `meta_repo` → 500 for genuine
  failures (was 404 for everything; absence still 404), `meta_orgs_repos` →
  per-repo default timestamps (pre-existing suppression, informational,
  unchanged and documented), `meta_repos` → 500 (already correct).
- `is_storage_empty` failures reach runtime startup as
  `RuntimeBuildError::MembershipInspection` (fail-closed, pre-existing); a
  falsely-empty conclusion from an uninspectable area is now impossible by
  construction. S3 backend behavior untouched.

## 5. Resource Costs and Limits

No numeric ceilings were introduced (none decided; no approved limit's scope
covers these operations; no production configuration path exists). Per-call
costs: `repo_timestamps` = 1 probe enumeration + ≤2 directory enumerations +
one `openat2`+`fstat` pair per regular entry; memory = one entry batch per
enumerated directory plus transient key strings. Emptiness = breadth-first
enumeration until the first non-directory entry; a pathological
all-directory tree is walked completely; memory = pending-key queue + one
batch. These are per-call costs only — no global memory or concurrency
budget, and descriptor containment provides neither snapshot isolation,
hard-link isolation, mount isolation, global memory protection, nor hardware
durability.

## 6. Verification Summary

See the evidence package for raw logs. Executed for this batch: fmt-check,
`cargo check`/`clippy --locked --all-targets --all-features -D warnings`,
full library suite (952 passed / 0 failed / 18 ignored; +24 module tests over
the prior 928), focused `timestamps_emptiness` tests (24: 16 fake-driven
contract/fault-injection + 8 Linux real-FsStorage production-routing tests),
and caller/regression suites (application_read 49, ports_wiring 10,
supervisor_and_command 42, manifest_lifecycle 97, gc_contained_discovery 13,
slow_connection 5). `git diff --check` and explicit whitespace checks clean.
No live-S3 or non-Linux execution is claimed; permission-denial fault paths
are covered by deterministic fakes (no new ignored privileged tests were
added).

## 7. Deployment / Rollback Considerations

The change is a read-path cutover with no storage-format, configuration, or
dependency change; deploying it alters observation and error surfaces only.
Rolling back to the previous binary restores the ambient behaviors (including
the silent-truncation and symlink-following hazards) but does not and cannot
reverse any mutations that occurred while the new binary ran — e.g. a
`mark_membership_ready` marker legitimately written after a genuine empty
check, or any `Applying` migration checkpoints; no reversal of intervening
mutations is claimed. Operators should expect: `_meta` endpoints returning
500 (instead of 404/stale data) on genuine inspection failures, and startup
failing closed (instead of potentially marking membership ready) when an
emptiness area is uninspectable.

## 8. Remaining Limitations

- Timestamp/emptiness walks are unbounded per call (ambient baseline);
  numeric budgets remain an operational decision with no configuration path.
- `meta_orgs_repos` continues to render default timestamps on per-repo
  failures (pre-existing, informational).
- Membership, journal, upload/quarantine, and mutation-embedded reads remain
  as previously assessed; write containment is Gate O-04; non-Linux is O-15.

All canonical gates — O-03, O-04, O-05, O-06, O-13, O-15, O-16, D-06 —
remain **OPEN**. This batch narrows O-05 by resolving R-4 and R-5.
