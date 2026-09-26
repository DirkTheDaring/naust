# Architecture Assessment Update: Filesystem Read-Containment Gaps After Lifecycle-Journal Read Containment

> **Historical snapshot.** Journal *reads* described here landed; later commits also contained journal *writes* via the shared domain (`f5f9bf7`, `8c0ac64`). Current residual inventory: [`current-state.md`](current-state.md).

- **Document:** `docs/architecture/filesystem-read-containment-post-journal-assessment.md`
- **Status:** Gap Assessment Delta (supersedes the R-12 row of `filesystem-read-containment-post-membership-assessment.md`; earlier assessments preserved unchanged as historical records)
- **Primary Repository Baseline:** `~/devel/rust/registry-rust` at HEAD `ef50360da7ac7640a628e2f808c242d914ca4c14` (`master`), with the lifecycle-journal read-containment batch applied in the working tree (uncommitted).
- **Dependency Repository:** `~/devel/rust/storage-layer-rust` at HEAD `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (read-only, unchanged).
- **Implementation Record:** `docs/architecture/filesystem-lifecycle-journal-read-containment.md`

## 1. Resolved / Changed Items

| Index | Area | Prior Status | Current Status |
|---|---|---|---|
| R-1–R-5, R-7–R-11 | Referrers, catalog, timestamps, emptiness, membership/readiness/checkpoint reads | Contained (committed `906ef89`, `3b64713`, `634da23`, `ef50360`) | Unchanged. |
| **R-12** | Lifecycle journal read (`read_lifecycle_journal`) | Uncontained ambient read; `is_lifecycle_active` swallowed read errors into "inactive"; no journal repo-identity validation | **Read contained** via `journal_read::read_lifecycle_journal_impl` over the shared pinned reader. Preserved: raw-bytes contract with caller-side parsing (operation kinds/phases/older records unaffected), genuine absence → `None`, empty/malformed payloads still fail at the caller's parse (never absence), legacy `Io` taxonomy, recovery ordering, lease/retry contracts. Intentional: pinned-root resolution; symlinked/non-regular journal paths rejected; containment rejection (incl. ancestor) never treated as absence. Narrow caller corrections: `read_journal` fails closed on a repository-identity mismatch (a foreign journal would redirect recovery mutations to the location repository); `is_lifecycle_active` now returns `Result` and propagates read failures (zero prior callers). Recovery boundary regression-verified through the actual `ManifestLifecycleService`: an unreadable/corrupt/mismatched journal aborts the outer mutation without deleting or overwriting the journal or starting a conflicting operation, and the mutation succeeds after the fault is cleared. |

**Kept distinct (NOT resolved):** journal **writes and deletion remain
ambient and under-durable** (temp write without file fsync before rename;
ignored best-effort directory fsyncs; ambient pathname resolution diverging
from the pinned read root under root replacement) — audited in the
implementation record §4 and governed by Gate O-04. There is no separate
journal-discovery read (GC validation iterates the contained catalog and
point-reads journals), so no ambient journal discovery remains.

## 2. Remaining Uncontained Read Areas

| Index | Area | Symbols | Notes |
|---|---|---|---|
| R-6 | Manifest delete tag discovery | `list_tag_files` | Embedded in `delete_manifest` (O-04 scope). |
| R-13–R-15 | Quarantine & upload inspection | `quarantined_blob_version`, `read_quarantine_timestamp`, `get_finalized_receipt`, `reap_expired_sessions` | Embedded in GC/upload mutation lifecycles (O-04 adjacency). Now the last remaining read area outside mutation-embedded reads. |
| R-16–R-18 | Mutation-embedded reads | `set/clear_membership_candidate`, `mutate_tag`, `delete_tag_conditional`, `delete_manifest` subject read | Excluded from standalone read slices; governed by O-04. |

## 3. Open Resource-Policy Decisions (consolidated, carried)

1. Catalog discovery numeric budgets (no configuration path).
2. Timestamp/emptiness walk budgets (unbounded per call).
3. Membership walk budgets (unbounded per call).
4. **Lifecycle-journal payload ceiling (this batch: unbounded, matching the ambient baseline; no approved journal limit exists).**
5. Referrers read numeric limits; `list_referrers_page` policy; referrers parse taxonomy (carried).

## 4. Canonical Quality Gates

All gates remain explicitly **OPEN**:

| Gate | Status | Delta from this batch |
|:---:|:---:|---|
| O-03 | OPEN | No token/pagination contracts touched. |
| O-04 | OPEN | Journal write/delete durability and root-divergence gaps now explicitly audited (implementation record §4); membership/tag/manifest mutation reads and write containment remain. |
| O-05 | OPEN | Narrowed further: all standalone read paths except upload/quarantine inspection (R-13–R-15) are now contained; mutation-embedded reads (R-6, R-16–R-18) remain by design under O-04. |
| O-06 | OPEN | Untouched. |
| O-13 | OPEN | Untouched. |
| O-15 | OPEN | Untouched; non-Linux unverified. |
| O-16 | OPEN | Untouched. |
| D-06 | OPEN | Untouched. |
