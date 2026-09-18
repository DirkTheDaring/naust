# Architecture Assessment Update: Filesystem Read-Containment Gaps After Timestamps/Emptiness Containment

> **Historical snapshot.** Timestamps later also sit on `repo_timestamp_domain` (`84dbe13`). Current residual inventory: [`current-state.md`](current-state.md).

- **Document:** `docs/architecture/filesystem-read-containment-post-timestamps-assessment.md`
- **Status:** Gap Assessment Delta (supersedes the R-4/R-5 rows of `filesystem-read-containment-post-catalog-assessment.md`; earlier assessments preserved unchanged as historical records)
- **Primary Repository Baseline:** `/home/dietmar/devel/rust/registry-rust` at HEAD `3b647132f867accd7551ddb0ca297064be89abb9` (`master`), with the timestamps/emptiness containment batch applied in the working tree (uncommitted).
- **Dependency Repository:** `/home/dietmar/devel/rust/storage-layer-rust` at HEAD `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (read-only, unchanged).
- **Implementation Record:** `docs/architecture/filesystem-timestamps-and-emptiness-containment.md`

## 1. Resolved / Changed Items

| Index | Area | Prior Status | Current Status |
|---|---|---|---|
| R-1/R-2 | Referrers reads / paged | Contained (committed `906ef89`) / latent paged defects, zero callers | Unchanged. |
| R-3 | Catalog discovery | Contained (committed `3b64713`) | Unchanged. |
| **R-4** | Repository timestamps (`repo_timestamps` / `max_mtime_in_dir`) | Uncontained (ambient probe + symlink-following mtime scan with silent truncation/skip) | **Contained.** Routes through `timestamps_emptiness::repo_timestamps_impl` over the shared pinned `FsMetadataReader` (`enumerate_dir` + `inspect_file_metadata`). Ordinary contracts preserved (NotFound/None cases, direct children, regular files incl. hidden, max selection, legacy Io taxonomy). Intentional changes: pinned-root resolution; symlinked components rejected; symlink entries no longer contribute; no maximum over an incomplete relevant set (failures — including containment resolution rejection, which does not establish the leaf's object type and can stem from an ancestor substitution — propagate; genuine absence and `fstat`-confirmed non-regular objects are excluded; observed symlink dirents are filtered during enumeration); unaddressable names fail closed with CorruptData; traversal names → InvalidRepoName. The ambient `max_mtime_in_dir` was removed. Narrow caller correction: `meta_repo` now returns 500 for genuine failures instead of 404 (absence and invalid names still 404); `meta_orgs_repos` per-repo default-timestamp suppression is unchanged and documented. |
| **R-5** | Storage emptiness (`is_storage_empty` / `fs_dir_has_any_entry`) | `list_repositories` leg contained; recursive area probes ambient | **Contained.** The seven area probes route through `timestamps_emptiness::contained_subtree_has_any_entry` (same area list and short-circuit order; first non-directory dirent concludes non-empty; empty-directory trees remain empty; missing/vanished directories are empty contributions). Intentional changes: symlinked area roots/ancestors rejected instead of followed; non-descendable directory names fail closed — **an uninspectable area can no longer produce a false empty-storage success**, protecting the runtime startup path that writes `mark_membership_ready()` on `Ok(true)`. The ambient `fs_dir_has_any_entry` was removed. |

## 2. Remaining Uncontained Read Areas

| Index | Area | Symbols | Notes |
|---|---|---|---|
| R-6 | Manifest delete tag discovery | `list_tag_files` | Embedded in `delete_manifest` (O-04 scope). |
| R-7–R-11 | Repo-blob membership reads | `get_repo_blob_membership`, listing/count/checkpoint | Standalone reads interleaved with migration/sweep state transitions; checkpoint/cursor/lease contracts must be preserved. Now the largest remaining standalone-read area. |
| R-12 | Lifecycle journal read | `read_lifecycle_journal` | Crash-recovery gating; package with lifecycle write audit. |
| R-13–R-15 | Quarantine & upload inspection | `quarantined_blob_version`, `read_quarantine_timestamp`, `get_finalized_receipt`, `reap_expired_sessions` | Embedded in GC/upload mutation lifecycles (O-04 adjacency). |
| R-16–R-18 | Mutation-embedded reads | `clear_membership_candidate`, `mutate_tag`, `delete_tag_conditional`, `delete_manifest` subject read | Excluded from standalone read slices; governed by O-04. |

## 3. Open Resource-Policy Decisions (consolidated, carried)

1. Catalog discovery numeric budgets (recommendation in the catalog cutover record §5; no production configuration path yet).
2. Timestamp/emptiness walk budgets (this batch: unbounded per call, matching the ambient baseline; costs documented in the implementation record §5; adoption would require both a decision and production wiring).
3. Referrers read numeric limits (`max_payload_bytes` / `max_descriptors`).
4. `list_referrers_page` policy (deprecate vs. fail-closed + saturating arithmetic; zero production callers).
5. Referrers JSON parse taxonomy (`Io` → `CorruptData`).

## 4. Canonical Quality Gates

All gates remain explicitly **OPEN**:

| Gate | Status | Delta from this batch |
|:---:|:---:|---|
| O-03 | OPEN | No token/pagination contracts touched. |
| O-04 | OPEN | Untouched; mutation-embedded reads and write durability remain. |
| O-05 | OPEN | Narrowed further: CAS blobs, manifests, GC discovery, tag reads/listing, referrers, catalog discovery, and now repository timestamps and storage-emptiness probes are contained. Membership (R-7–R-11), journal (R-12), upload/quarantine (R-13–R-15), and mutation-embedded reads (R-6, R-16–R-18) remain. |
| O-06 | OPEN | Untouched. |
| O-13 | OPEN | Untouched. |
| O-15 | OPEN | Untouched; non-Linux unverified. |
| O-16 | OPEN | Untouched. |
| D-06 | OPEN | Untouched. |
