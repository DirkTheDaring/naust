# Architecture Assessment Update: Filesystem Read-Containment Gaps After Membership Read Containment

> **Historical snapshot.** Membership **point** ops later moved onto `membership_domain` (`6ed8b3e`). Listing/count/readiness **reads** stay on the contained `membership_read` seam (pinned reader). Marker/checkpoint **writes** stay pathname `atomic_write_file`. Current residual inventory: [`current-state.md`](current-state.md).

- **Document:** `docs/architecture/filesystem-read-containment-post-membership-assessment.md`
- **Status:** Gap Assessment Delta (supersedes the R-7–R-11 rows of `filesystem-read-containment-post-timestamps-assessment.md`; earlier assessments preserved unchanged as historical records)
- **Primary Repository Baseline:** `~/devel/rust/registry-rust` at HEAD `634da23b1f4f015fb38fa0dd9baea95236583a62` (`master`), with the membership read-containment batch applied in the working tree (uncommitted).
- **Dependency Repository:** `~/devel/rust/storage-layer-rust` at HEAD `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (read-only, unchanged).
- **Implementation Record:** `docs/architecture/filesystem-membership-read-containment.md`

## 1. Resolved / Changed Items

| Index | Area | Prior Status | Current Status |
|---|---|---|---|
| R-1–R-5 | Referrers, catalog discovery, timestamps, emptiness | Contained (committed `906ef89`, `3b64713`, `634da23`) | Unchanged. |
| **R-7** | Membership point read (`get_repo_blob_membership`) | Uncontained ambient read | **Contained** (pinned reader payload acquisition; missing → `None`; `CorruptData`/`Io` taxonomy preserved; relative-key diagnostics; symlinks/containment rejections propagate). |
| **R-8** | Per-repo membership listing (`list_repo_blob_memberships_page`) | Uncontained; every root/walk failure silently produced an empty page | **Contained.** Pagination contracts preserved (sort keys, `<= token` skip, `limit+1` heap, `[1,1000]` clamp, fail-closed record loads). Unreadable roots/directories, mid-iteration failures, non-UTF-8 names, and **name-qualifying record candidates with nonregular dirent types** now fail closed (`CorruptData` for the latter, rejected from dirent evidence before token filtering, without following symlinks or opening special files); only genuine `NotFound` yields an empty page. |
| **R-9** | Global membership listing (`list_all_repo_blob_memberships_page`) | Same silent-empty pattern | **Contained**, same policy; undecodable repo directories still `CorruptData`; non-digest `.json` names still skipped; nonregular name-qualifying candidates fail the page closed. Silently empty or under-reported pages can no longer let `RepositoryMembershipLedger::reconcile_memberships` mark an incomplete reverse index ready (regression-verified against the real `BlobRefIndex`: a failed reconciliation preserves a previously-ready index's earlier legitimate state and never establishes readiness on a never-ready index). Counting remains conservative (present nonregular markers still count, protecting the blob). |
| **R-10** | Membership counting (`count_repo_blob_memberships`) | **Every failure silently counted as zero** — a zero count is a precondition toward blob deletion in `blob_gc` validation/eligibility and `has_any_membership` | **Contained and fail-closed.** Genuinely missing root → 0; genuinely absent markers not counted; confirmed non-regular marker objects still counted (protective legacy semantics); all probe/enumeration failures — including containment rejection — propagate. A failed count can no longer permit deletion. |
| **R-11** | Readiness + checkpoint reads (`is_membership_ready`, `get_migration_checkpoint`) | Marker probe swallowed all failures as "absent"; any object type counted as marker; checkpoint contained-adjacent failures could present as missing | **Contained.** Genuine absence semantics preserved (missing checkpoint → `None`; missing marker → not ready); all other failures propagate; a non-regular marker object is `CorruptData` and cannot establish readiness; a checkpoint read failure can no longer present as absence and permit a fresh migration. Checkpoint lease/token/retry contracts and all write paths (`mark_membership_ready`, `save_migration_checkpoint`) unchanged. |

Not resolved within this area (kept honest): the membership **read-modify-write** paths `set_membership_candidate` and `clear_membership_candidate` still perform ambient reads inside their mutation flows (R-16, O-04 scope); `link_repo_blob`/`unlink_repo_blob` writes remain ambient (O-04).

## 2. Remaining Uncontained Read Areas

| Index | Area | Symbols | Notes |
|---|---|---|---|
| R-6 | Manifest delete tag discovery | `list_tag_files` | Embedded in `delete_manifest` (O-04 scope). |
| R-12 | Lifecycle journal read | `read_lifecycle_journal` | Crash-recovery gating; package with lifecycle write audit. Now the largest remaining standalone read. |
| R-13–R-15 | Quarantine & upload inspection | `quarantined_blob_version`, `read_quarantine_timestamp`, `get_finalized_receipt`, `reap_expired_sessions` | Embedded in GC/upload mutation lifecycles (O-04 adjacency). |
| R-16–R-18 | Mutation-embedded reads | `set/clear_membership_candidate`, `mutate_tag`, `delete_tag_conditional`, `delete_manifest` subject read | Excluded from standalone read slices; governed by O-04. |

## 3. Open Resource-Policy Decisions (consolidated, carried)

1. Catalog discovery numeric budgets (recommendation in the catalog cutover record; no configuration path).
2. Timestamp/emptiness walk budgets (unbounded per call).
3. **Membership walk budgets (this batch: unbounded per call, matching the ambient baseline; costs documented in the implementation record §5).**
4. Referrers read numeric limits; `list_referrers_page` policy; referrers parse taxonomy (carried).

## 4. Canonical Quality Gates

All gates remain explicitly **OPEN**:

| Gate | Status | Delta from this batch |
|:---:|:---:|---|
| O-03 | OPEN | Membership pagination/token contracts preserved unchanged; no new token schemes. |
| O-04 | OPEN | Untouched; membership writes, candidate read-modify-write, and durability remain. |
| O-05 | OPEN | Narrowed further: CAS blobs, manifests, GC discovery, tag reads/listing, referrers, catalog discovery, timestamps, emptiness, and now membership/readiness/checkpoint standalone reads are contained. Journal (R-12), upload/quarantine (R-13–R-15), and mutation-embedded reads (R-6, R-16–R-18) remain. |
| O-06 | OPEN | Untouched. |
| O-13 | OPEN | Untouched. |
| O-15 | OPEN | Untouched; non-Linux unverified. |
| O-16 | OPEN | Untouched. |
| D-06 | OPEN | Untouched. |
