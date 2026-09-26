# Architecture Assessment Update: Filesystem Read-Containment Gaps After Quarantine/Upload Inspection Point-Read Containment (Reaper Deferred)

> **Historical snapshot — not HEAD remaining work.** Recorded against `0cd6a73` plus an uncommitted working tree. Later `master` commits contained the reaper and mutation paths (`f555e5f` and following). Current residual inventory: [`current-state.md`](current-state.md). Index: [`README.md`](README.md).

- **Document:** `docs/architecture/filesystem-read-containment-post-upload-quarantine-assessment.md`
- **Status:** Historical gap delta (superseded as current inventory by `current-state.md`). Originally superseded the R-13–R-15 rows of `filesystem-read-containment-post-journal-assessment.md`.
- **Primary Repository Baseline:** `~/devel/rust/registry-rust` at HEAD `0cd6a734475555ffe315ba6db8775031b248ee36` (`master`), with the quarantine/upload inspection point-read batch applied in the working tree (uncommitted).
- **Dependency Repository:** `~/devel/rust/storage-layer-rust` at HEAD `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (read-only, unchanged).
- **Implementation Record:** `docs/architecture/filesystem-quarantine-upload-inspection-containment.md`

## 1. Resolved / Changed Items

| Index | Area | Prior Status | Current Status |
|---|---|---|---|
| R-1–R-5, R-7–R-12 | All previously contained standalone reads | Contained (committed through `0cd6a73`) | Unchanged. |
| **R-13** | Quarantine version/inspection (`quarantined_blob_version`, `compute_fs_blob_version` consumers) | Any metadata failure silently reported "no quarantined object" | **Contained point read.** Pinned-reader inspect + streamed hash; token byte-identical to the ambient `compute_fs_blob_version` still used by the unchanged `delete_blob_conditional` comparison (equality test-verified); only genuine `NotFound` is absence; all other failures propagate to the GC sweep's existing fail-closed arm. |
| **R-14** | Quarantine timestamps (`read_quarantine_timestamp`; actively-used `blob_gc::read_quarantine_time`) | Read/parse failures → `None` with unchecked epoch addition; the GC sweep overwrote the timestamp on `None`, destroying corrupt evidence and restarting the deletion clock | **`FsStorage` point read contained** (absence vs. `CorruptData` vs. `Io`, checked representability; zero production callers, latent API). **GC helper narrowly HARDENED** (absence-only `None`; corrupt/unreadable/unrepresentable → `FsReadMeta` error; checked arithmetic) but **remains an ambient cfg-rooted read** — hardened ≠ contained, and it is not marked contained. |
| **R-15 (receipt point read)** | `get_finalized_receipt` | Ambient read; receipt path composable from unvalidated session ids | **Contained point read.** Receipt reads validate session ids structurally, preserve parse taxonomy and the identity-mismatch→`None` semantics. |
| **R-15 (reaper reads) — DEFERRED** | `reap_expired_sessions` directory enumeration + session-meta/receipt reads | Ambient reads; suppressed directory/iteration/read/parse/lock errors; counts ignored attempts | **NOT contained — deferred.** Routing only the reaper's inspection through the pinned reader while its `recover_session`/`abort_session`/receipt-unlink mutations stay ambient is an inspection-to-action mismatch: after a root replacement the pinned descriptor reads the detached tree while the ambient mutations delete a same-UUID replacement record in the current tree. Binding inspection to the mutations requires write-side containment (O-04, out of scope). The reaper stays on its pre-batch, internally coherent ambient implementation (reads AND acts through the same ambient pathnames); its pre-existing error suppression and attempt-count semantics are unchanged. Frozen by the real-fs regression `test_real_reaper_root_replacement_acts_only_on_current_tree` (FAILs against the rejected cutover, PASSes against the shipped ambient reaper). |

**Kept distinct (NOT resolved):** quarantine/restore/conditional-delete
mutations, the entire reaper (its ambient inspection reads AND its
locking/recovery/abort/unlink), quarantine-timestamp writes, and the GC
sweep's own ambient quarantine directory walk (with its `while let Ok`
truncation, fail-safe direction) remain ambient under O-04, including the
documented pinned-read/ambient-write root divergence: a version observed
through the pinned root does not authorize deleting an object reached through a
replaced ambient pathname (today the conditional delete's own ambient
recomputation is the operative guard).

## 2. Remaining Uncontained Read Areas

| Index | Area | Symbols | Notes |
|---|---|---|---|
| R-6 | Manifest delete tag discovery | `list_tag_files` | Embedded in `delete_manifest` (O-04 scope). |
| **R-15 (reaper)** | Upload reaper inspection reads | `reap_expired_sessions` directory enumeration + session-meta/receipt reads | **Deferred (this batch).** Ambient; contained inspection blocked by the inspection-to-action mismatch until write-side containment (O-04) lets inspection and mutation share a tree. Regression preserved. |
| — | GC quarantine sweep walk & timestamp helper | `blob_gc/mod.rs` directory scan, `read_quarantine_time`/`write_quarantine_time` | Ambient cfg-rooted; error handling HARDENED this batch (not contained); containment would require routing the sweep's enumeration through the pinned reader (candidate follow-up, GC-internal). |
| R-16–R-18 | Mutation-embedded reads | `set/clear_membership_candidate`, `mutate_tag`, `delete_tag_conditional`, `delete_manifest` subject read | Excluded from standalone read slices; governed by O-04. |

The three standalone quarantine/upload **point reads** (R-13, R-14 API, R-15
receipt) are contained by this batch. The reaper's inspection reads remain the
one standalone-read item deferred pending write containment, and are kept
explicitly in the inventory above (not reclassified out of it). Mutation-
embedded reads and the GC-internal ambient sweep walk are unchanged.

## 3. Open Resource-Policy Decisions (consolidated, carried)

1. Catalog discovery numeric budgets (no configuration path).
2. Timestamp/emptiness walk budgets; membership walk budgets; lifecycle-journal payload ceiling (all unbounded per call).
3. **Quarantine/upload inspection ceilings (this batch: unbounded, matching the ambient baselines; version hashing streams in 64 KiB chunks).**
4. Referrers read numeric limits; `list_referrers_page` policy; referrers parse taxonomy (carried).

## 4. Canonical Quality Gates

All gates remain explicitly **OPEN**:

| Gate | Status | Delta from this batch |
|:---:|:---:|---|
| O-03 | OPEN | No token/pagination contracts touched. |
| O-04 | OPEN | Quarantine/reaper mutation paths and the pinned-read/ambient-write divergence explicitly audited (implementation record §0, §4); the reaper's contained-inspection cutover is blocked on write containment and deferred with a regression; write containment and durability remain. |
| O-05 | OPEN | Narrowed but NOT closed to standalone reads: the three quarantine/upload point reads (R-13, R-14 API, R-15 receipt) are contained; the reaper's inspection reads (R-15) are deferred and remain ambient; mutation-embedded reads (R-6, R-16–R-18) and the GC-internal ambient quarantine sweep walk remain. |
| O-06 | OPEN | Untouched. |
| O-13 | OPEN | Untouched. |
| O-15 | OPEN | Untouched; non-Linux unverified. |
| O-16 | OPEN | Untouched. |
| D-06 | OPEN | Untouched. |
