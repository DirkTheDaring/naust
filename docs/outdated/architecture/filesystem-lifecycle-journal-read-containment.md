> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Landed (`0cd6a73`). The "journal writes/deletion remain ambient and under-durable" finding below was subsequently addressed: journal writes were contained and made durable (`f5f9bf7`, `8c0ac64`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Contained Filesystem Lifecycle-Journal Reads and Journal Write/Delete Audit

- **Document:** `docs/architecture/filesystem-lifecycle-journal-read-containment.md`
- **Status:** Implementation & Compatibility Record with Write/Delete Audit (working tree, not committed)
- **Primary Repository Baseline:** `/home/dietmar/devel/rust/registry-rust` at HEAD `ef50360da7ac7640a628e2f808c242d914ca4c14` (`master`), changes applied in the working tree only.
- **Dependency Repository:** `/home/dietmar/devel/rust/storage-layer-rust` at HEAD `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (strictly read-only; unchanged).
- **Scope:** Gap item R-12 (`read_lifecycle_journal`) read containment, narrow recovery-boundary caller corrections, and an audit (documentation only) of the associated journal write/delete ordering.

---

## 1. Source and Caller Traces (verified against current source)

- **Read (replaced):** `FsStorage::read_lifecycle_journal(repo)` — strict
  `CanonicalRepoName` grammar, `fs_repo_dir` traversal guard, ambient
  `tokio::fs::read` of `repos/<repo>/meta/lifecycle_journal.json`;
  missing → `Ok(None)`; other errors → `Io`. Returns raw `Bytes` —
  deserialization lives with callers. **No separate journal-discovery method
  exists**: GC pre-delete validation enumerates repositories via the
  already-contained catalog discovery and point-reads each journal.
- **Record:** `LifecycleJournalRecord { op_id, repo, op_kind
  (Publish/DeleteManifest/DeleteTag/ProxyEvict), target_digest,
  target_reference, phase, owner_id, lease_expiry_unix_secs, …,
  relevant_tags, subject_digest, … }` (schema unchanged).
- **Callers:**
  1. `ManifestLifecycleManager::read_journal` → parse (`CorruptJournal` on
     malformed) → consumed by `recover_and_ensure_index_healthy`, which every
     outer mutation (`publish_manifest`, manifest deletion, tag deletion,
     proxy eviction paths) invokes **after `acquire_coordination` (repository
     lease at `repos/<repo>/.repo_lock`) and before any recovery or new
     journal write**. Storage read errors already propagated via `?`,
     aborting the outer mutation.
  2. `is_lifecycle_active` **swallowed read errors** (`.ok().flatten()` →
     `false`, i.e. "no pending operation"). Verified to have **zero
     invocation sites** in `src/` and `tests/` — a latent defect, hardened in
     this batch (Section 3).
  3. `blob_gc/validation.rs` journal check during candidate deletion:
     read/parse failures propagate as typed GC errors (deletion aborted);
     journals matching the candidate protect it. Unchanged.
- **Writes/deletes (audited, unchanged):** `write_lifecycle_journal`,
  `delete_lifecycle_journal` (Section 4); phase updates during recovery and
  the delete-journal-on-completion flow live in `manifest_lifecycle.rs` and
  are unchanged. There is no startup/background journal recovery loop;
  recovery happens at the mutation boundary per repository.

## 2. What Was Implemented

New module `src/storage/fs/journal_read.rs`:
`read_lifecycle_journal_impl(reader, repo)` over the shared pinned
`FsMetadataReader` as an `ObjectPayloadReader` (no per-operation reader
opening, no ambient fallback; blocking work stays on the dependency's
`spawn_blocking` offload). Key `repos/<repo>/meta/lifecycle_journal.json`
after the preserved `CanonicalRepoName` validation; acquisition errors map
through the shared `read_adapter::translate_payload_read_error`; the payload
is drained without a seam-imposed ceiling (ambient baseline; no approved
journal limit exists — any ceiling joins the consolidated open resource
decisions) and returned as raw `Bytes`.
`FsStorage::read_lifecycle_journal` now delegates to it.

**Preserved contracts:** raw-bytes return with caller-side parsing (all
operation kinds, phases, and any tolerated older record shapes unaffected);
genuine absence (missing repo/`meta/`/file) → `Ok(None)`; existing empty or
malformed payloads still reach callers as bytes and fail at their parse step
(empty file ≠ absence); legacy `Io` taxonomy; recovery ordering and
idempotency untouched; lease/retry contracts untouched.

**Intentional changes (test-frozen):** pinned-root resolution; symlinked
repository/`meta/`/journal path components rejected (`Io`) instead of
followed; non-regular journal objects rejected via `S_IFREG` evidence without
potentially blocking opens; containment resolution rejection (`ELOOP`/`EXDEV`)
never treated as absence (ancestor rejection included); unsupported
environments → `Configuration`. No failure class becomes "no pending
operation".

## 3. Narrow Caller Corrections (each justified by its consequence)

1. **`read_journal` repository-identity validation:** recovery applies the
   journal's digests, tags, and referrers to the repository the journal was
   read FROM; a journal recording a different repository (misplaced or
   mis-written) would redirect those recovery mutations. After parsing, a
   `record.repo != repo` mismatch now fails closed
   (`Storage(CorruptData)` with both names). Valid historical journals always
   record the repository they are stored under (`write_journal` builds the
   record from the same `repo`), so no compatible record is rejected.
2. **`is_lifecycle_active` returns `Result<bool, _>`:** read failures
   propagate instead of presenting as "inactive"; genuine absence and expired
   leases remain `Ok(false)`. Signature change is safe: zero callers existed
   (verified); the previous `.ok().flatten()` would have let an unreadable
   journal report "no pending operation" to any future consumer.

No other caller needed hardening: the outer-mutation boundary already
propagates (`?`), and GC validation already fails closed.

## 4. Journal Write/Delete Audit (documentation only — O-04 remains)

Exact current sequences (unchanged by this batch):
- **Write:** `CanonicalRepoName` parse → ambient `ensure_dir(meta)` →
  `tokio::fs::write` to `.tmp.journal.<uuid>` (**no fsync of file data before
  rename**) → ambient `rename` onto `lifecycle_journal.json` → directory
  fsync attempted with the result **ignored** (`let _ = fsync_dir(..)`).
  File durability before rename is assumed, not established; directory-entry
  durability is best-effort. (Contrast: `atomic_write_file` used elsewhere
  fsyncs file data before rename and propagates the directory fsync result.)
- **Delete:** ambient `remove_file` (missing → `Ok`) → ignored best-effort
  directory fsync.
- **Root divergence:** writes/deletes resolve ambient pathnames while the
  read resolves the pinned root. If the root pathname or journal ancestors
  are replaced at runtime, mutations act on the replacement tree while reads
  observe the originally opened root — the pre-existing divergence shared by
  every contained read path. **No ambient read fallback compensates**; the
  read fails closed on rejection instead. Closing the divergence and the
  durability gaps requires write containment (Gate O-04), deliberately out of
  scope. The journal read being contained does NOT make the journal lifecycle
  contained.
- **Indirect mutation effect of the read cutover:** only the recovery
  boundary — an unreadable journal now aborts the outer mutation after lease
  acquisition (legitimate earlier work, released by the coordination guard;
  no rollback claimed) and before recovery, journal deletion/overwrite, or a
  conflicting new operation.

## 5. Recovery Failure Behavior (regression-verified through actual callers)

Real `ManifestLifecycleService` over real `FsStorage`
(`journal_read::tests::real_fs_tests`):
- **Unreadable journal (symlinked path):** `delete_tag` aborts with the
  propagated storage error; the unreadable journal is neither deleted nor
  overwritten (symlink and target byte-verified intact); the tag survives; no
  conflicting operation starts. After the deterministic fault is removed, the
  same mutation succeeds, deletes the tag, and clears its own journal —
  retry/idempotency preserved (the retry may traverse the coordination
  layer's built-in bounded lease backoff; the tests add no sleeps).
- **Corrupt journal:** aborts with `CorruptJournal`; journal preserved
  byte-for-byte; tag intact.
- **Repository-identity mismatch:** aborts with `Storage(CorruptData)`;
  mismatched journal preserved; tag intact.
- **`is_lifecycle_active`:** absent → `Ok(false)`; unexpired → `Ok(true)`;
  expired → `Ok(false)`; unreadable → `Err` (was silently `false`).
- Journal-read handling does not branch on operation kind or phase (parse
  precedes recovery branching); representative recovery branches themselves
  are covered by the existing `manifest_lifecycle_tests` suite (97 tests,
  re-run green), which exercises Publish/Delete phase recovery with valid
  journals.

## 6. Verification Summary

Executed (raw logs in the evidence package): fmt-check, `cargo check`/`clippy
--locked --all-targets --all-features -D warnings`, full library suite
(**978 passed / 0 failed / 18 ignored**; +9 new module tests over 969: 3
fake-driven + 6 Linux real-fs/actual-caller), focused journal tests, and
regression suites: manifest_lifecycle (97), gc_adversarial_coordination (22),
online_gc (3), application_service (9), application_read (49),
oci_conformance_regression (4), oci_1_1 (7) = 191. Startup/supervisor suites
were not re-run: runtime startup does not consume journal reads (verified by
trace), and the lifecycle wrapper forwarding is exercised by the lifecycle
suite. No sleeps, no new ignored tests, no live-S3 or non-Linux claims;
injected failure evidence is labeled as scripted seams.

## 7. Deployment / Rollback Considerations

Read-path cutover plus two narrow caller corrections; no storage-format,
configuration, or dependency change. Operators should expect outer mutations
to fail (instead of silently attempting recovery or proceeding) while a
repository's journal is unreadable, and GC to keep failing closed as before.
Rolling back the binary restores the ambient read and the
`is_lifecycle_active` suppression but cannot reverse mutations performed
while the new binary ran (journals written/cleared, tags/manifests mutated by
legitimately successful operations); no reversal of intervening mutations is
claimed. Containment provides no snapshot isolation and the journal
write/delete durability gaps documented in Section 4 remain.

All canonical gates — O-03, O-04, O-05, O-06, O-13, O-15, O-16, D-06 —
remain **OPEN**. This batch narrows O-05 by resolving R-12's standalone read;
journal writes/deletion remain ambient under O-04.
