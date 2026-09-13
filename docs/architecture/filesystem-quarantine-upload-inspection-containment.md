# Contained Filesystem Quarantine and Upload Inspection Point Reads (Partial — Reaper Cutover Deferred)

- **Document:** `docs/architecture/filesystem-quarantine-upload-inspection-containment.md`
- **Status:** Implementation & Compatibility Record with Mutation-Path Audit and a DEFERRED-path blocker record (working tree, not committed)
- **Primary Repository Baseline:** `/home/dietmar/devel/rust/registry-rust` at HEAD `0cd6a734475555ffe315ba6db8775031b248ee36` (`master`), changes applied in the working tree only.
- **Dependency Repository:** `/home/dietmar/devel/rust/storage-layer-rust` at HEAD `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (strictly read-only; unchanged — the required capability, including nanosecond mtime via `inspect_file_metadata`, exists; no gap).
- **Scope:** Gap items R-13–R-15. This batch contains the three standalone
  inspection **point reads** (`quarantined_blob_version`,
  `read_quarantine_timestamp`, `get_finalized_receipt`) and applies a narrow
  error-handling correction to the actively-used GC quarantine-age check
  (`blob_gc::read_quarantine_time`). **The reaper's inspection reads inside
  `reap_expired_sessions` are DEFERRED, not contained** (see §0); a rejected
  intermediate cutover is preserved as a regression.

---

## 0. Deferred: `reap_expired_sessions` inspection reads (blocking finding)

`reap_expired_sessions` is an inspect-then-act loop: it INSPECTS session-meta
and receipt records and then, on the strength of that inspection, ACTS on them
— acquires the session lock, calls `recover_session`/`abort_session` (which
unlink the session meta and `.data` by ambient pathname), and unlinks receipts
with `remove_file`. All of these mutations remain ambient (write-side
containment is O-04, out of scope).

An intermediate implementation in this batch routed only the reaper's
INSPECTION reads through the pinned `FsMetadataReader` while leaving the
mutations ambient. That is an **inspection-to-action mismatch** and was
rejected:

- The pinned reader resolves against a root **descriptor** captured at
  `FsStorage` construction. After the storage root directory is replaced
  (renamed aside and a fresh directory put in its place at the same pathname),
  the pinned descriptor keeps the ORIGINAL (now detached) tree readable, while
  the ambient mutation pathnames resolve the REPLACEMENT tree.
- A reaper that reads an EXPIRED record from the detached tree and then unlinks
  by ambient pathname deletes a **same-UUID replacement** record in the current
  tree — a fresh, non-expired session or receipt — on the strength of a
  different tree's expiry.

Binding inspection to the mutations safely requires a write-side identity check
or write containment (O-04), which is out of this batch's scope. The forbidden
shortcuts (one-time root/path equality check; canonicalize/metadata-then-ambient
delete; inode-check-then-independently-resolved unlink; ambient read fallback
after contained resolution) were **not** substituted.

**Resolution:** the reaper stays on its **pre-batch, internally coherent
ambient** implementation — it reads AND acts through the same ambient
pathnames, so it can only delete a record it actually observed in the current
tree. Only the reaper-routing changes were surgically reverted; the reaper-only
module helpers (`list_reap_*`, `read_*_for_reap`, candidate/qualification
helpers) were removed rather than left as unused production code.

**Regression (development evidence + permanent guard):**
`upload_quarantine_read::tests::real_fs_tests::test_real_reaper_root_replacement_acts_only_on_current_tree`
constructs the root-replacement scenario with an expired record in the detached
tree and a fresh, same-UUID record in the current tree, then asserts on the
files themselves in BOTH trees (distinguishing an actual deletion from the
returned attempt counter). Against the rejected pinned-inspection reaper it
FAILS (the fresh replacement session meta and receipt are deleted; raw log
`PROMOTED-reaper-root-replacement-FAILS.log` in the evidence package). Against
the shipped ambient reaper it PASSES (both fresh records survive; the detached
tree is never resolved by the action paths).

The same mismatch applies uniformly to every read-driven step the reaper takes
— session lock acquisition, session-meta inspection, `recover_session` /
`abort_session`, and receipt removal — because each acts through an ambient
pathname while the rejected cutover would have inspected through the pinned
descriptor; the regression exercises the session-abort and receipt-removal
paths, which are the ones that unlink a specific file.

## 1. Source and Caller Traces (verified against current source)

Confirmed source-observed risks:
- `quarantined_blob_version`: `metadata(path).is_err() → Ok(None)` — **any**
  metadata failure presented as "no quarantined object"; then
  `compute_fs_blob_version` (ambient stat + streaming SHA-256; token
  `fs:{len}:{mtime_nanos}:{sha256hex}`, pre-epoch/unavailable mtime → 0).
- `FsStorage::read_quarantine_timestamp`: read/parse failures → `None`;
  **unchecked `UNIX_EPOCH + Duration::from_secs(secs)`** (panic on
  unrepresentable stored values). **Zero production callers** (the API is
  public but unused) — the actively-used quarantine-age check is
  `blob_gc::read_quarantine_time`, a parallel cfg-rooted ambient helper with
  the same suppress-to-None + unchecked-addition shape, whose `None` result
  causes the sweep to **overwrite the timestamp with `now`** — so a corrupt or
  unreadable stored timestamp was silently destroyed and the deletion clock
  restarted.
- `get_finalized_receipt`: ambient read; parse → `CorruptData`; identity check
  (`receipt.repo == session.repo && receipt.uuid == session.uuid`), mismatch →
  `Ok(None)`. Traversal-capable session ids were joined into ambient paths.
- `reap_expired_sessions`: `if let Ok(read_dir)` / `while let Ok(Some(..))`
  suppression of directory, iteration, read, and parse failures; lossy names;
  lock `Ok(None)` (busy) and `Err` conflated into silent skips; interleaves
  inspection with `recover_session`/`abort_session`/receipt unlink whose
  results are ignored while the counter increments per attempt. **These
  suppression behaviors are unchanged in this batch** — the reaper is deferred
  (§0); its correction awaits write-side containment so inspection and action
  share a tree.

Callers: GC quarantine sweep (`blob_gc/mod.rs`) consumes
`quarantined_blob_version` with an existing fail-closed `Err` arm
(`BlobGcError::QuarantineVersionQuery`) and `Ok(None) → skip`;
`delete_blob_conditional` (unchanged mutation) recomputes the version
ambiently via `compute_fs_blob_version` and compares. `get_finalized_receipt`
is consumed by `upload_coordinator` finalize/dedupe paths (errors propagate;
its separate `Ok(Some(_))` fast-path suppression is fail-open/non-destructive
and unchanged). `reap_expired_sessions` is invoked by
`UploadCoordinator::reap_expired_uploads` (errors propagate).

## 2. What Was Implemented

New module `src/storage/fs/upload_quarantine_read.rs` over the shared pinned
`FsMetadataReader` via the existing `membership_read::MembershipReadOps` seam
(no new seam trait; no per-operation reader opening; no ambient fallback;
blocking work on the dependency's offload). It implements the **three
standalone point reads only**:
- `quarantined_blob_version_impl`: contained `inspect_file_metadata`
  (len + mtime) + streamed `open_payload` hashing in 64 KiB chunks. **Token
  semantics byte-identical to the ambient `compute_fs_blob_version`** still
  used by the unchanged `delete_blob_conditional` comparison (pre-epoch or
  unavailable mtime → 0, exactly as before); equality is test-verified
  against the ambient computation for the same object. Only genuine
  `NotFound` → `None`; vanish-between-inspect-and-hash is a read failure
  (legacy open-after-stat behavior); everything else propagates.
- `read_quarantine_timestamp_impl`: contained payload read; whitespace
  trimming and second precision preserved; future values returned as stored;
  genuine absence → `None`; non-UTF-8/unparseable/unrepresentable stored
  values → descriptive `CorruptData` with **checked** `UNIX_EPOCH.checked_add`
  (no panic path); acquisition failures → `Io`.
- `get_finalized_receipt_impl`: structural session-id validation before key
  composition (traversal ids rejected; previously joined ambiently); contained
  read; parse → `CorruptData` (preserved); **identity semantics preserved** —
  a receipt whose repo/uuid do not match the requested session remains
  `Ok(None)` (its established safe meaning: "no receipt for THIS session",
  triggering normal upload retry; a foreign receipt is never accepted).

`FsStorage` routing: the three point reads delegate to the module.
`reap_expired_sessions` is **unchanged from its pre-batch ambient body** (§0):
ambient `read_dir` enumeration with `while let Ok(Some(..))`, ambient
`tokio::fs::read` + `serde_json` parse of session-meta/receipt records, ambient
lock/recover/abort/unlink — reads and acts through the same pathnames. A code
comment at the function records why it is deferred and names the regression.

Narrow caller correction (`blob_gc::read_quarantine_time`, the actively-used
quarantine-age check): genuine absence still returns `None` (the sweep then
initializes a fresh timestamp — unchanged write path); read failures and
corrupt/unrepresentable stored values now fail closed
(`BlobGcError::FsReadMeta`) with checked arithmetic instead of being
conflated with absence and **overwriting the stored evidence**. This is a
HARDENING of error handling, **not containment**: the helper remains an
ambient cfg-rooted read (it does not use the pinned reader), as does the
sweep's own quarantine directory walk — see §4. "Hardened" ≠ "contained".

## 3. Failure-Propagation Consequences (regression-verified)

- A failed version query can no longer present as "no quarantined object":
  the GC sweep's existing `Err` arm (`QuarantineVersionQuery`) fails the run
  instead of skipping, and `Ok(None)` is reserved for genuine absence
  (production-entry test with a symlinked quarantine root).
- A corrupt/unreadable quarantine timestamp can no longer fabricate a fresh
  age: `FsStorage` API → `CorruptData`; GC sweep helper → `FsReadMeta` error
  (previously: silently `None` → timestamp overwritten → deletion clock
  restarted and evidence destroyed).
- A failed receipt read can no longer present as a valid receipt or as
  absence; identity mismatches still mean "no receipt for this session".
- **Not changed this batch (deferred):** the reaper still suppresses directory/
  iteration/read/parse failures and still conflates busy locks with lock
  acquisition failures; its returned count still tallies attempts, not
  confirmed successes. These pre-existing behaviors are internally coherent
  with its ambient mutations and are recorded, not corrected, here (§0).

## 4. Mutation-Path Audit (unchanged, ambient — O-04 remains)

- `quarantine_blob` / `restore_quarantined_blob` / `delete_blob_conditional`:
  ambient metadata probes, `create_dir_all`, `rename`, `remove_file`; no
  fsync of moved data or directories; conditional delete recomputes the
  version ambiently and unlinks by pathname. **Read/write root divergence:**
  the contained version query observes the pinned root, while these mutations
  resolve ambient pathnames — under a replaced root pathname, a version
  observed through the pinned root does NOT describe the object the ambient
  conditional delete would remove. This divergence predates and survives this
  batch; the conditional delete's own ambient recomputation-and-compare is
  what protects it today. Closing it requires a write-side identity check or
  write containment (O-04) — deliberately not attempted here, and no ambient
  read fallback compensates.
- Reaper (fully ambient, deferred — §0): ambient `read_dir` inspection,
  `try_acquire_fs_session_lock` (ambient lock file + `fs2` flock),
  `recover_session`, `abort_session` (ambient unlinks with ignored results,
  best-effort hash-file cleanup), receipt `remove_file` (ignored result). The
  inspection-to-action mismatch that blocks contained inspection is described
  in §0 and frozen by the root-replacement regression.
- Quarantine timestamp write (`write_quarantine_timestamp` /
  `blob_gc::write_quarantine_time`) and the GC sweep's quarantine directory
  walk (`blob_gc/mod.rs` `read_dir` with `while let Ok(Some(..))` truncation,
  fail-safe direction: skipped candidates are not deleted) remain ambient —
  the quarantine/upload area is NOT fully contained by this batch and is
  recorded as such in the gap assessment.

## 5. Resource Costs and Open Decisions

No numeric ceilings introduced (no approved limit covers these reads; ambient
baselines were unbounded). Version hashing streams the blob once in 64 KiB
chunks (no whole-blob buffering); timestamp/receipt reads buffer one small
payload plus its parsed form. Per-call costs only — no global memory or
concurrency budget, no snapshot isolation (objects may change between
inspection and any later action). Any inspection-read ceilings join the
consolidated open resource decisions.

## 6. Verification Summary

Executed (raw logs in the evidence package): fmt-check, `cargo check`/`clippy
--locked --all-targets --all-features -D warnings`, full library suite, and the
relevant regression suites (production_upload_cutover,
gc_adversarial_coordination, online_gc, manifest_lifecycle, application_read,
slow_connection, gc_contained_discovery). Exact counts, commands, and exit
statuses are recorded in the evidence logs. The `upload_quarantine_read` module
carries: 3 fake-driven point-read fault/contract tests; 4 Linux real-fs
point-read tests (version equality with the ambient conditional-delete
computation, symlinked quarantine root + pinned-root replacement,
timestamp roundtrip/corruption, receipt roundtrip + foreign-session + symlink
rejection); and 1 Linux real-fs reaper **root-replacement regression** that
flips from FAIL against the rejected pinned-inspection reaper (raw log
preserved) to PASS against the shipped ambient reaper. Plus 1
`blob_gc::read_quarantine_time` correction test. No sleeps, no new ignored
tests, no live-S3 or non-Linux claims.

## 7. Deployment / Rollback Considerations

Read-path point-read cutover plus one GC error-handling correction; the reaper
is unchanged; no storage-format, configuration, or dependency change.
Operators should expect: GC sweeps erroring (instead of silently skipping or
overwriting state) while quarantine areas hold corrupt or uninspectable
entries — the offending file is named in the error and preserved. Reaper
behavior is unchanged from before this batch. Rolling back the binary restores
the ambient version/timestamp/receipt suppression behaviors but cannot reverse
mutations performed while the new binary ran (blobs deleted after legitimately
successful inspections); no reversal of intervening mutations, snapshot
isolation, or hardware durability is claimed.

All canonical gates — O-03, O-04, O-05, O-06, O-13, O-15, O-16, D-06 —
remain **OPEN**.
