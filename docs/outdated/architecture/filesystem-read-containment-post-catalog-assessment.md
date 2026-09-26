# Architecture Assessment Update: Filesystem Read-Containment Gaps After Catalog Discovery Cutover

> **Historical snapshot.** Catalog discovery containment described here landed; remaining-work tables below are not HEAD. Current residual inventory: [`current-state.md`](current-state.md).

- **Document:** `docs/architecture/filesystem-read-containment-post-catalog-assessment.md`
- **Status:** Gap Assessment Delta (supersedes the catalog rows of `filesystem-read-containment-post-referrers-assessment.md`)
- **Primary Repository Baseline:** `~/devel/rust/registry-rust` at HEAD `906ef891baab47780856132df6e25492f3cd3499` (`master`), with the contained catalog-discovery cutover applied in the working tree (uncommitted).
- **Dependency Repository:** `~/devel/rust/storage-layer-rust` at HEAD `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (read-only, unchanged).
- **Cutover Record:** `docs/architecture/filesystem-catalog-discovery-production-cutover.md`

This delta updates the inventory carried by the post-tag-listing assessment (R-1…R-18) and the
post-referrers delta. Only the catalog rows changed; other rows were re-checked and remain as
previously assessed.

## 1. Resolved / Changed Items

| Index | Area | Prior Status | Current Status |
|---|---|---|---|
| **R-1** | Referrers direct reads (`list_referrers`) | Contained (committed `906ef89`) | Unchanged: **Contained**. |
| **R-2** | Referrers paged (`list_referrers_page`) | Latent suppression/arithmetic defects, zero production callers | Unchanged. |
| **R-3** | Catalog discovery (`list_repositories` / `list_repo_names`) | Uncontained (ambient DFS + 4 symlink-following stats per dir; silent truncation; unbounded) | **Contained.** Routes through `catalog_discovery::discover_catalog_repositories_impl` over the shared pinned `FsMetadataReader`. Recognition semantics preserved; intentional changes: symlinked path components rejected, symlinked markers not recognized, no silent truncation (incomplete enumeration is an error), **UTF-8 names unaddressable by contained keys fail closed with `CorruptData`** (silent omission was rejected because membership migration application/verification could otherwise establish readiness over an incomplete repository set; migration persists its initial `Applying` checkpoint before discovery — that write is not rolled back and readiness is not established), pinned-root resolution. Whole-walk budget mechanics implemented with retained-byte accounting matching exactly what the walk stores (pending queue keys + output names); **production default unbounded** — numeric ceilings are a consolidated open decision with **no production configuration path yet** (cutover record §5). |

## 2. Remaining Uncontained Read Areas

| Index | Area | Symbols | Notes |
|---|---|---|---|
| R-4 | Catalog timestamps | `repo_timestamps` / `max_mtime_in_dir` | Ambient dir walk + mtime stats; observation only. Now the natural next small slice (shares `enumerate_dir` mechanics; needs an entry-metadata primitive or per-entry `head` calls). |
| R-5 | Storage emptiness | `is_storage_empty` / `fs_dir_has_any_entry` | `list_repositories` leg now contained; the recursive `fs_dir_has_any_entry` probe over seven subtrees remains ambient. |
| R-6 | Manifest delete tag discovery | `list_tag_files` | Embedded in `delete_manifest` (O-04 scope). |
| R-7–R-11 | Repo-blob membership reads | `get_repo_blob_membership`, listing/count/checkpoint | Standalone reads interleaved with migration/sweep state transitions; checkpoint/cursor/lease contracts must be preserved. |
| R-12 | Lifecycle journal read | `read_lifecycle_journal` | Crash-recovery gating; package with lifecycle write audit. |
| R-13–R-15 | Quarantine & upload inspection | `quarantined_blob_version`, `read_quarantine_timestamp`, `get_finalized_receipt`, `reap_expired_sessions` | Embedded in GC/upload mutation lifecycles (O-04 adjacency). |
| R-16–R-18 | Mutation-embedded reads | `clear_membership_candidate`, `mutate_tag`, `delete_tag_conditional`, `delete_manifest` subject read | Excluded from standalone read slices; governed by O-04. |

## 3. Open Resource-Policy Decisions (consolidated)

1. **Catalog discovery numeric budgets** — recommendation with defaults/rationale/affected
   callers in the cutover record §5; unbounded default preserves the ambient baseline until
   decided. There is currently no production configuration path for non-default limits;
   adoption requires wiring (constructor/limit plumbing and validation) in addition to the
   decision.
2. **Referrers read numeric limits** (`max_payload_bytes`/`max_descriptors`) — carried from the
   referrers cutover; seam accepts limits without code changes.
3. **`list_referrers_page` policy** (deprecate vs. fail-closed + saturating arithmetic) — zero
   production callers.
4. **Referrers JSON parse taxonomy** (`Io` → `CorruptData`) — carried.

## 4. Canonical Quality Gates

All gates remain explicitly **OPEN**:

| Gate | Status | Delta from this batch |
|:---:|:---:|---|
| O-03 | OPEN | Catalog pagination contract unchanged (application-level in-memory slicing over full walks); storage-level catalog tokens not introduced. |
| O-04 | OPEN | Untouched. |
| O-05 | OPEN | Narrowed further: CAS blobs, manifests, GC discovery, tag reads/listing, referrers, and now catalog discovery are contained. R-4/R-5 fragments, membership, journal, upload/quarantine, and mutation-embedded reads remain. |
| O-06 | OPEN | Untouched. |
| O-13 | OPEN | Untouched. |
| O-15 | OPEN | Untouched. |
| O-16 | OPEN | Untouched. |
| D-06 | OPEN | Untouched. |
