# Architecture Assessment Update: Filesystem Read-Containment Gaps After Referrers Cutover

> **Historical snapshot.** Referrers later sit on `referrer_domain` (`b1e607c`). Current residual inventory: [`current-state.md`](current-state.md).

- **Document:** `docs/architecture/filesystem-read-containment-post-referrers-assessment.md`
- **Status:** Gap Assessment Delta (supersedes the referrers rows of `filesystem-read-containment-post-tag-listing-assessment.md`)
- **Primary Repository Baseline:** `/home/dietmar/devel/rust/registry-rust` at HEAD `2419d46e89d88151c18972210ba79826bf776b59` (`master`), with the contained referrers read cutover applied in the working tree (uncommitted).
- **Dependency Repository:** `/home/dietmar/devel/rust/storage-layer-rust` at HEAD `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (read-only, unchanged).
- **Cutover Record:** `docs/architecture/filesystem-referrers-read-production-cutover.md`

This document updates the inventory of `filesystem-read-containment-post-tag-listing-assessment.md`
(Section 4.1, items R-1 … R-18) against the current working tree. Only the referrers rows changed;
all other rows were re-checked for continued accuracy of their symbols and classifications and
remain as previously assessed.

## 1. Resolved / Changed Items

| Index | Area | Prior Status | Current Status |
|---|---|---|---|
| **R-1** | OCI Referrers Direct (`list_referrers`) | Uncontained (ambient `tokio::fs::read`, unbounded, unvalidated name) | **Contained.** Routes through `referrers_read::read_referrers_contained` over the shared pinned `FsMetadataReader` (`openat2` `RESOLVE_BENEATH`/`NO_SYMLINKS`/`NO_MAGICLINKS`, `S_IFREG`). Structural repository-name validation rejects traversal with `InvalidRepoName` before reader calls. Default limits are `None`/`None` (unbounded baseline preserved; operational ceilings remain an open product decision). Mutation consumers (`add_referrer`, `remove_referrer`, `delete_manifest`) now consume the contained read; compatibility is test-frozen. |
| **R-2** | OCI Referrers Paged (`list_referrers_page`) | Uncontained + error suppression + unchecked arithmetic; no active production caller | **Partially mitigated, code unchanged.** Underlying read is now contained via delegation to `list_referrers`; a traversal name can no longer leak data through the paged path (suppressed to empty page). The `.unwrap_or_default()` suppression and unchecked `start_idx + page_limit` arithmetic remain byte-for-byte. Re-verified 2026-09-12: all non-test references are trait declarations or forwarding bodies (`supervisor.rs:1705`, `manifest_lifecycle.rs:1905`, `storage/mod.rs:696`, `ports/mod.rs` macro + blanket impl) with **zero call sites**; remaining references are mocks and the live-S3 harness. Deprecation/fail-closed remains a follow-up slice. |

## 2. Remaining Uncontained Read Areas (unchanged from prior assessment)

| Index | Area | Symbols | Notes |
|---|---|---|---|
| R-3 | Catalog discovery | `list_repositories` / `list_repo_names` | Highest-consequence remaining gap (safety-check input, `_catalog` route, index rebuild, migration planning). Catalog recognition semantics differ from GC discovery traversal; do not substitute one for the other without caller-by-caller tracing. |
| R-4 | Catalog timestamps | `repo_timestamps` / `max_mtime_in_dir` | Observation only. |
| R-5 | Storage emptiness | `is_storage_empty` / `fs_dir_has_any_entry` | Startup/readiness probing. |
| R-6 | Manifest delete tag discovery | `list_tag_files` | Embedded in `delete_manifest` mutation; intentionally preserved (O-04 scope). |
| R-7–R-11 | Repo-blob membership reads | `get_repo_blob_membership`, listing/count/checkpoint | Standalone reads exist, but callers interleave with migration/sweep state transitions; checkpoint/cursor/lease contracts must be preserved. |
| R-12 | Lifecycle journal read | `read_lifecycle_journal` | Crash-recovery gating; best packaged with a lifecycle write audit. |
| R-13–R-15 | Quarantine & upload inspection | `quarantined_blob_version`, `read_quarantine_timestamp`, `get_finalized_receipt`, `reap_expired_sessions` | Deeply embedded in GC/upload mutation lifecycles (O-04 adjacency). |
| R-16–R-18 | Mutation-embedded reads | `clear_membership_candidate`, `mutate_tag`, `delete_tag_conditional`, `delete_manifest` subject read | Read-modify-write operations; excluded from standalone read slices, governed by O-04. |

## 3. Recommended Next Slice

**Repository catalog discovery (R-3)** is now the highest-priority remaining standalone read gap,
per the prior ranking matrix (rank 2 behind referrers). Its compatibility uncertainty is high:
catalog recognition (leaf-subdirectory heuristics: `tags/`, `manifests/`, `blobs/`, `meta/`) and
GC discovery traversal have different established semantics, and error-handling changes must be
traced through the `_catalog` API, `BlobDeleteSafety`, `BlobRefIndex::rebuild`,
`MembershipMigration`, and the supervisor health probe before altering behavior. A characterization
slice for `list_repo_names` symlink-leaf recognition and error propagation should precede any
containment routing.

## 4. Canonical Quality Gates

All gates remain explicitly **OPEN**:

| Gate | Status | Delta from this batch |
|:---:|:---:|---|
| O-03 | OPEN | Referrers pagination contracts unchanged (no active caller); catalog pagination open. |
| O-04 | OPEN | Untouched; referrers writes (`ensure_dir`, `atomic_write_file`, `remove_file`) remain ambient. |
| O-05 | OPEN | Narrowed: CAS blobs, manifests, GC discovery, tag reads, tag listing, and now referrers reads are contained. Catalog, timestamps/emptiness, membership, journal, upload/quarantine, and mutation-embedded reads remain. |
| O-06 | OPEN | Untouched. |
| O-13 | OPEN | Untouched. |
| O-15 | OPEN | Untouched; non-Linux verification unperformed. |
| O-16 | OPEN | Untouched. |
| D-06 | OPEN | Untouched. |
