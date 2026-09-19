> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Landed (`ef50360`); membership point ops later moved onto `membership_domain` (`6ed8b3e`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Contained Filesystem Repository-Blob Membership, Readiness, and Checkpoint Reads

- **Document:** `docs/architecture/filesystem-membership-read-containment.md`
- **Status:** Implementation & Compatibility Record (working tree, not committed)
- **Primary Repository Baseline:** `/home/dietmar/devel/rust/registry-rust` at HEAD `634da23b1f4f015fb38fa0dd9baea95236583a62` (`master`), changes applied in the working tree only.
- **Dependency Repository:** `/home/dietmar/devel/rust/storage-layer-rust` at HEAD `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (strictly read-only; unchanged — all required primitives existed: `enumerate_dir`, `open_payload`, `inspect_file_metadata`).
- **Scope:** Gap items R-7–R-11: `get_repo_blob_membership` (R-7), `list_repo_blob_memberships_page` (R-8), `list_all_repo_blob_memberships_page` (R-9), `count_repo_blob_memberships` (R-10), `is_membership_ready` + `get_migration_checkpoint` (R-11).

---

## 1. Read and Caller Traces (verified against current source)

On-disk layout (unchanged): records at
`repo-memberships/by-repo/<base64url(repo)>/<algo>/<hex>.json`
(JSON `RepoBlobMembershipRecord`; repo names encoded via URL-safe unpadded
base64, decoded and grammar-checked by `decode_canonical_repo_key`); readiness
marker `meta/membership_ready.json`; checkpoint `meta/migration_checkpoint.json`.

Ambient behaviors replaced:
- `get_repo_blob_membership`: `CanonicalRepoName` grammar; ambient `read`;
  missing → `None`; malformed → `CorruptData`; other → `Io`.
- Both page listings: **`metadata(root).is_err()` → empty page** (any failure,
  not just absence); `if let Ok(read_dir)` and `while let Ok(Some(..))` walks
  silently dropping unreadable directories and truncating on iteration errors;
  lossy name decoding; candidates = `*.json` minus `.tmp.`; bounded
  `limit+1` min-heap over sort keys with `<= token` skip; record loads
  fail-closed (`Io`/`CorruptData`); limit clamp `[1,1000]`.
- `count_repo_blob_memberships`: **every failure silently counted as zero**
  (`if let Ok` on the root walk; `metadata(marker).is_ok()` as existence).
- `is_membership_ready`: checkpoint read (errors propagate) AND marker
  `metadata().is_ok()` — any marker-probe failure silently meant "not ready",
  and any object type (directory!) counted as a present marker.
- `get_migration_checkpoint`: ambient read; missing → `None` (permits fresh
  migration); malformed → `CorruptData`; other → `Io`.

Safety-relevant consumers (all invocation-verified, not name-inferred):
- **`blob_gc/validation.rs` pre-delete validation and `blob_gc/mod.rs`
  eligibility:** `count == 0` is a precondition toward blob deletion; both
  callers propagate `Err` (they `map_err` into typed GC errors) — the silent
  zero inside the function was the hazard.
- **`RepositoryMembershipLedger`**: `has_any_membership` (StorageOnly mode:
  `count > 0`) gates deletion safety; `reconcile_memberships` pages
  `list_all_...` to rebuild the reverse index and then `mark_ready` — silently
  empty pages would mark an incomplete index ready.
- **`gc_service` membership sweep**: pages `list_all_...`; empty pages skip
  sweeping (fail-safe direction) but incomplete pages would desynchronize
  candidate state.
- **Migration (`membership_migration.rs`)**: `plan`/`apply`/`verify` use point
  reads and `get_migration_checkpoint`; a checkpoint read failure presenting
  as `None` would permit a fresh migration to stampede an interrupted one.
- **Runtime startup**: `is_membership_ready` gates `mark_membership_ready` /
  `MembershipBackfillRequired`; its `Err` arm fails startup
  (`MembershipInspection`).
- **Non-destructive suppressors (unchanged, documented):**
  `upload_coordinator` treats `Ok(Some(_))` pattern-matches on point reads as
  "membership present" and any error as absence in its already-finalized
  fast path — the fail-open direction performs a redundant upload, not a
  destructive action. `manifest_lifecycle`/`proxy`/`application/blob_read`
  point-read consumers propagate or degrade non-destructively.

## 2. What Was Implemented

New module `src/storage/fs/membership_read.rs`:
- `MembershipReadOps` seam (contained `enumerate_dir` + `open_payload` +
  `inspect_file_metadata`) implemented by the shared pinned
  `FsMetadataReader`; no per-operation reader opening; no ambient fallback;
  blocking syscalls stay on the dependency's `spawn_blocking` offload.
- `get_repo_blob_membership_impl`, `list_repo_blob_memberships_page_impl`,
  `list_all_repo_blob_memberships_page_impl`,
  `count_repo_blob_memberships_impl`, `membership_ready_marker_present`,
  `get_migration_checkpoint_impl`. Keys are composed from the existing
  canonical helpers (`canonical_repo_membership_relpath`,
  `canonical_all_memberships_prefix`, `encode/decode_canonical_repo_key`);
  error mapping reuses `catalog_discovery::map_contained_dir_error` and
  `read_adapter::translate_*`.

Production routing (`src/storage/fs.rs`): the six read paths delegate to the
module over `self.reader.as_ref()`. All membership WRITE and read-modify-write
paths are untouched: `link_repo_blob`, `set_membership_candidate`,
`clear_membership_candidate` (still ambient read-modify-write, R-16),
`unlink_repo_blob`, `mark_membership_ready`, `save_migration_checkpoint`.

## 3. Preserved Contracts vs. Intentional Changes

Preserved (test-frozen): record schema and identity; strict repo grammar on
point reads; missing record/root/checkpoint/marker absence semantics; sort
keys (`<algo>:<hex>` per-repo, `<encoded>/<algo>/<hex>` global), `<= token`
continuation skip, `limit+1` heap selection, ascending order, `next_token`
semantics, `[1,1000]` clamp (pre-existing embedded cap — not a new ceiling);
`.json`/`.tmp.` name filters; undecodable repo directory → `CorruptData`;
non-digest `.json` names skipped in the global walk (they cannot be
membership records); fail-closed record loads (vanished → `Io`, malformed →
`CorruptData`, no partial page); checkpoint fields (lease, continuation
token, current repository) round-trip unchanged; readiness conjunction
(checkpoint phase `Ready` AND marker) unchanged; counting a present marker
object of any observed type (over-counting protects the blob).

Intentional changes (each test-frozen):
1. **Pinned-root resolution** for all six reads.
2. **Symlinked path components rejected** (`Io`) instead of followed
   (membership roots, repo/algo dirs, record files, `meta/`, marker,
   checkpoint). Symlinked dirents at the repository/algorithm levels remain
   skipped. **Name-qualifying record candidates whose observed dirent type is
   not a regular file fail the page closed with `CorruptData`** — they are
   never silently omitted from authoritative pages, and they are rejected
   from dirent evidence alone (symlinks are not followed; no potentially
   blocking special-file open is attempted). Rejection occurs during
   candidate scanning, deliberately before continuation-token filtering and
   heap selection — earlier than the legacy read-time failure, which only
   surfaced for candidates selected into the current page and silently
   followed symlinked records. Counting is deliberately NOT aligned with this
   rejection: `count_repo_blob_memberships` still counts a present marker
   object of any observed type (over-counting protects the blob), while
   authoritative listings fail closed on the same object.
3. **No unsafe incomplete success**: unreadable membership roots or
   directories, mid-iteration failures, probe failures, and nonregular
   name-qualifying candidates now propagate instead of yielding empty or
   silently smaller pages or a **zero count**. Consequences: a failed count
   can no longer permit blob deletion (GC pre-delete validation and
   eligibility, ledger `has_any_membership`); failed or under-reported
   `list_all` pages can no longer let `reconcile_memberships` mark an
   incomplete reverse index ready. Reconciliation is additive and reaches
   `mark_ready` only after complete discovery: a failed run leaves a
   previously-ready index in its earlier legitimately built state (earlier
   inserts of valid records are legitimate work, not rolled back) and never
   establishes readiness on a never-ready index (regression-verified against
   the real `BlobRefIndex`). Containment still does not establish snapshot
   isolation.
4. **Checkpoint/readiness failures are not absence**: containment rejection,
   permission, or I/O failures on the checkpoint no longer present as a
   missing checkpoint (which would permit a fresh migration); marker-probe
   failures no longer silently report "not ready"; a non-regular object at
   the marker path is `CorruptData` instead of counting as a present marker.
5. **Containment resolution rejection always propagates** (`ELOOP`/`EXDEV`
   prove neither absence nor a leaf-only substitution).
6. **Non-UTF-8 entry names fail closed** (`CorruptData`) instead of being
   lossy-decoded into identifiers that cannot resolve to real records.
7. Corrupt point-read message now reports the storage-relative key instead of
   an ambient absolute path (kind unchanged: `CorruptData`).

## 4. Indirect Mutation-Caller Effects

Routing these shared public reads through containment changes reads consumed
inside otherwise-untouched mutation flows:
- `mark_membership_ready` reads the checkpoint (now contained) AFTER writing
  the marker: a contained read failure aborts between the marker write and
  the checkpoint save. The marker write is an earlier legitimate write and is
  not rolled back; readiness stays unestablished when the checkpoint phase is
  not `Ready` — no rollback or atomic check-and-save is claimed.
- Migration application/verification consume the contained checkpoint and
  point reads; lease ownership/expiry, continuation tokens, retry behavior,
  and the persisted `Applying` checkpoint ordering are unchanged (regression:
  full `repository_membership_tests` suite, including deterministic
  lease-expiry fixtures, passes).
- `set_membership_candidate` / `clear_membership_candidate` perform their own
  ambient reads and are NOT routed through this module (excluded
  read-modify-write, remains R-16 under O-04).
- `upload_coordinator`'s fail-open `Ok(Some(_))` suppression now also
  swallows the new containment errors in its fast path; the consequence
  remains a redundant upload attempt, not a destructive action (documented,
  unchanged).

## 5. Resource Costs and Open Decisions

No new numeric ceilings; per-call enumeration limits are effectively
unbounded (ambient baseline), and no approved limit's scope covers these
reads. Costs per call: point reads buffer one payload plus its parsed record;
page listings enumerate the relevant subtree per page (one entry batch per
directory, `limit+1` candidate heap, `limit` record payloads parsed);
successive pages repeat enumeration work; counting performs one
`openat2`+`fstat` probe per repository directory. No global memory or
concurrency budget follows; containment provides neither snapshot isolation
nor hard-link/mount isolation. Numeric budget policy for membership walks
joins the consolidated open resource decisions (no configuration path
exists; adoption requires wiring plus the decision).

## 6. Verification Summary

Executed (raw logs in the evidence package): fmt-check, `cargo check`/`clippy
--locked --all-targets --all-features -D warnings`, full library suite
(**969 passed / 0 failed / 18 ignored**; +17 over 952: the 17 new module
tests; 2 legacy tests were re-frozen in place), focused membership tests (17
module tests: 11 fake-driven + 6 Linux real-fs, including the actual
`RepositoryMembershipLedger::reconcile_memberships` regression against the
real `BlobRefIndex`), and caller regression suites: repository_membership (56,
including migration apply/resume/lease-expiry), gc_adversarial_coordination
(22), online_gc (3), gc_contained_discovery (13), ports_wiring (10),
application_read (49), manifest_lifecycle (97), production_upload_cutover
(25), supervisor_and_command (42). Legacy re-frozen tests:
`test_get_repo_blob_membership_malformed_json_is_corrupt_data` (relative-key
message) and `test_list_repo_blob_memberships_page_nonregular_json_candidate_fails_closed`
(renamed; a directory named as a qualifying record now fails the page closed
with `CorruptData` from dirent evidence, without being opened). No sleeps, no new
ignored tests; permission/containment failure paths use deterministic fakes;
no live-S3 or non-Linux claims.

## 7. Deployment / Rollback Considerations

Read-path cutover only; no storage format, configuration, or dependency
change. Operators should expect: GC runs and ledger operations erroring
(instead of silently under-counting) when the membership area is
uninspectable; startup failing closed on marker/checkpoint inspection
failures; `_meta`-unrelated. Rolling back the binary restores the ambient
suppression behaviors but cannot reverse mutations performed while the new
binary ran (e.g. checkpoints saved, memberships linked/unlinked after
legitimately successful reads); no reversal of intervening mutations is
claimed.

## 8. Remaining Limitations

- `set_membership_candidate`/`clear_membership_candidate` reads remain
  ambient inside read-modify-write flows (R-16, O-04 scope), as do lifecycle
  journal (R-12), upload/quarantine readers (R-13–R-15), and
  `delete_manifest`-embedded reads (R-6, R-17–R-18).
- Membership walk budgets are unbounded pending an operational decision.
- `upload_coordinator` fast-path suppression is fail-open by design and
  unchanged.

All canonical gates — O-03, O-04, O-05, O-06, O-13, O-15, O-16, D-06 —
remain **OPEN**. This batch narrows O-05 by resolving the standalone read
paths of R-7–R-11.
