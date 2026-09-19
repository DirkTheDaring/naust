> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Landed (`3b64713`); repository catalog discovery is contained at HEAD.
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Contained Filesystem Catalog Discovery: Production Cutover Record

- **Document:** `docs/architecture/filesystem-catalog-discovery-production-cutover.md`
- **Status:** Implementation & Compatibility Record (working tree, not committed)
- **Primary Repository Baseline:** `/home/dietmar/devel/rust/registry-rust` at HEAD `906ef891baab47780856132df6e25492f3cd3499` (`master`), changes applied in the working tree only.
- **Dependency Repository:** `/home/dietmar/devel/rust/storage-layer-rust` at HEAD `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (strictly read-only; unchanged — all required primitives already existed).
- **Predecessor Records:**
  - `docs/architecture/filesystem-gc-repository-discovery-decisions.md` (GC-vs-catalog semantic separation; committed).
  - `docs/architecture/filesystem-read-containment-post-referrers-assessment.md` (gap inventory; committed in `906ef89`).

---

## 1. What Was Implemented

### 1.1 New Module: `src/storage/fs/catalog_discovery.rs`

A production contained repository-catalog walk:

- `CatalogDiscoveryLimits { max_depth, max_dir_enumerations, max_total_entries, max_repositories, max_retained_path_bytes, per_dir_limits }` with
  `Default = CatalogDiscoveryLimits::unbounded()` (all `usize::MAX`), preserving the ambient
  implementation's unbounded traversal baseline. **No numeric production limits were invented**
  (Section 5).
- `discover_catalog_repositories_impl(enumerator, limits)`: bounded breadth-first walk of
  `repos/` via `storage_fs::FsMetadataReader::enumerate_dir` (`openat2` with
  `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`), returning sorted,
  deduplicated repository-relative names.
- **Reused mechanics, separate policy:** the module reuses GC discovery's
  `DiscoveryDirEnumerator` seam, `RetainedPathBytesTracker`, and checked budget helpers from
  `repo_discovery.rs`, while keeping the catalog's own recognition policy. Catalog discovery
  and GC manifest discovery remain distinct components with different completeness contracts
  (public `/v2/_catalog` presentation vs. GC reachability safety); neither was substituted for
  the other, per the committed decisions record (F-01).

### 1.2 Production Routing Change: `FsStorage::list_repo_names`

`FsStorage::list_repo_names` (the sole body behind `Storage::list_repositories`) now delegates
to `catalog_discovery::discover_catalog_repositories_impl(self.reader.as_ref(), &CatalogDiscoveryLimits::default())`,
replacing the ambient `tokio::fs::read_dir` stack walk with four symlink-following
`tokio::fs::metadata` marker stats per visited directory. The shared pinned
`Arc<FsMetadataReader>` is reused; no constructor, trait, configuration, or storage-format
change. Both primary and proxy-cache filesystem storages are `FsStorage` instances and route
through the same path (test-verified).

### 1.3 Preserved Catalog Recognition Semantics (test-frozen)

- Repository recognition: a directory is a repository iff it directly contains a `tags/`,
  `manifests/`, `blobs/`, or `meta/` **directory**; `referrers/`-only directories are not
  repositories; markers must be directories (regular files named `tags` are not markers).
- Reserved layout names (`tags`, `manifests`, `referrers`, `blobs`, `meta`) are never listed
  and never descended into, at any depth; the `repos/` root itself is never a repository.
- Nested repositories and namespace parents both listed; sorted lexicographically; deduplicated.
- Missing `repos/` root and empty root: `Ok(vec![])`. A directory observed in its parent but
  removed before its own enumeration is skipped (legacy `NotFound => continue`).
- Symlinked child entries skipped (dirent type gate — unchanged).
- Non-UTF-8 entry names skipped with their subtrees (legacy catalog behavior; GC discovery
  instead fails closed — preserved divergence).
- Hidden (dot-prefixed) directories not filtered.
- Wrong-type `repos/` root and unreadable/permission-denied directories keep the legacy
  `StorageErrorKind::Io` classification (GC discovery's `CorruptData`/`PermissionDenied`
  mappings were intentionally not adopted here to avoid caller-visible kind changes).

### 1.4 Test Changes

- New unit/acceptance tests in `catalog_discovery.rs`: 16 fake-enumerator tests (recognition,
  reserved pruning, marker dirent-type policy, unaddressable-name fail-closed, non-UTF-8 skip,
  mid-walk failure atomicity, full error-taxonomy mapping, exact budget boundaries with
  one-over cases — including retained-path-byte tests for multiple simultaneously pending
  entries and for simultaneously retained output names plus pending entries) and
  5 Linux real-filesystem tests (recognition/nesting/hidden dirs, symlinked root rejection,
  symlinked marker/child, pinned-root replacement, wide layout with per-directory budget).
- New `catalog_discovery_contained_integration` module in `src/storage/fs/tests.rs` (6 tests):
  catalog service pagination through production wiring, catalog service fail-closed on a
  symlinked `repos/` root, second-instance (proxy-cache role) containment,
  `is_storage_empty` propagation, `plan_membership_migration` propagation, and a real-FsStorage
  regression exercising actual `apply_membership_migration` and `verify_membership_migration`
  against an unaddressable directory name (explicit failure, `Applying` checkpoint accounted
  for, readiness not established).
- Re-frozen characterization tests for the intentional changes:
  - `test_repo_discovery_symlink_semantics` (symlinked `repos/` root now rejected; symlinked
    marker no longer recognizes; symlinked child still skipped).
  - `test_repo_discovery_root_replacement_vs_repos_replacement` (Part 1 now observes the
    pinned original root after pathname replacement, aligning catalog discovery with the
    already-contained manifest/tag reads; Part 2 — `repos/` replaced beneath the same root —
    unchanged: the replacement is observed because resolution is beneath the pinned root).
  - `test_repo_discovery_unaddressable_repo_name_fails_closed` (renamed from
    `test_repo_discovery_invalid_repo_name_aborts_downstream_manifest_listing`; asserts the
    explicit `CorruptData` discovery failure and still verifies downstream `InvalidRepoName`
    rejection).
  - `test_repository_enumeration_io_failure_is_io` (same `Io` kind; message now the contained
    "target path is not a directory: repos" wording).
- Unchanged and passing: all other repo-discovery characterization tests (missing/empty root,
  marker recognition rules, nested hierarchy/sorting/dedup, reserved-name exclusion with GC
  divergence, non-UTF-8 skip vs. GC fail-closed) and the ignored-but-explicitly-executed
  permission test (unprivileged uid, effective chmod 0o000 denial verified in-test).

---

## 2. Observable Compatibility Changes

| Input class | Legacy ambient behavior | Contained behavior (now) |
|---|---|---|
| Ordinary valid layouts (markers, nesting, hidden dirs, sorting) | Listed | **Identical** |
| Missing/empty `repos/`; concurrently removed observed child | Empty / skipped | **Identical** |
| Symlinked child repo entry | Skipped | **Identical** |
| Non-UTF-8 entry names | Skipped with subtree | **Identical** |
| Wrong-type root, unreadable directory | `Err(Internal(Io))` | **Identical kind** (message wording changed) |
| **Symlinked `repos/` root or path component** | Followed silently (could catalog external trees) | **`Err(Internal(Io))`** (kernel resolution rejection) |
| **Symlinked recognition marker** (`tags` → elsewhere) | Recognized the repository | **Not recognized** (dirent-type policy) |
| **Mid-iteration `readdir` failure** | Silent truncation returned as success | **Error propagates; no partial catalog** |
| **Entry type inspection failure** | Entry silently skipped | **Error propagates** (`EntryDisappeared` → `Io`) |
| **UTF-8 names unaddressable by contained keys** (backslash, control chars) | Listed (all downstream contained operations rejected them with `InvalidRepoName`) | **Walk fails closed with `Err(Internal(CorruptData))`** carrying the parent key and offending name. Silently skipping such names was rejected: it would convert previously visible downstream failures into successful incomplete discovery, which membership migration application/verification could mistake for completion and establish readiness over an incomplete repository set. |
| Root pathname replaced at runtime | Followed the replacement tree | **Pinned to the originally opened root** (consistent with contained blob/manifest/tag/referrers reads, eliminating the prior divergence where catalog names from a replacement tree failed all subsequent pinned reads) |
| Budget exhaustion | n/a (unbounded) | `Err(Internal(Backend))` — reachable only with configured limits; production default is unbounded |

Caller impact: every production consumer already propagates `list_repositories` errors
fail-closed (catalog HTTP → 500; `BlobRefIndex::rebuild` aborts; `blob_gc` policy/validation
error out; membership migration aborts before further writes; supervisor probe records failure
and retries; `is_storage_empty` propagates). **No caller hardening was required**; no call site
suppresses discovery errors (re-verified across `src/`; `blob_delete_safety::find_blob_reference`
is `#[allow(dead_code)]` with no production call sites). The changes strictly convert
silent-omission/truncation cases into explicit errors or preserve existing results.

**Membership migration checkpoint compatibility (test-frozen):**
`apply_membership_migration` persists its initial `Applying` checkpoint (including an owner
lease) *before* catalog discovery runs. When discovery fails, the error propagates directly
(`?`) without a `Failed` checkpoint transition for this specific failure point: the persisted
checkpoint remains in phase `Applying`, that earlier legitimate write is **not rolled back**,
no subsequent per-repository processing occurs, and readiness is not established
(`is_membership_ready` requires phase `Ready` plus the ready marker). Verification likewise
fails explicitly instead of computing a verdict from an incomplete repository set
(`test_membership_migration_apply_and_verify_fail_closed_on_unaddressable_name`).

## 3. Resource Model (as implemented)

- `CatalogDiscoveryLimits` accounts for the complete walk: per-directory entry/name-byte
  ceilings, traversal depth, total enumerations, cumulative entries inspected (including
  skipped entries), retained repository names, and cumulative logical path bytes across the
  pending queue plus output. A per-directory ceiling is not a global traversal bound; the
  whole-walk counters provide that separately.
- All bounds fail the whole walk closed with `Backend`; no truncated catalog is ever returned
  as success. Checked arithmetic throughout (reusing the GC helpers).
- The retained-path-byte tracker counts exactly the logical bytes of what the walk stores:
  the `ObjectKey` strings held in the pending traversal queue (charged on enqueue, debited on
  dequeue) plus the repository-name strings held in the output list (charged on recognition,
  never debited). The pending queue stores only the key per entry; the relative repository
  name is derived from the key at recognition time, so nothing retained is unaccounted and
  nothing is double-charged across the enqueue → dequeue → output transition (boundary- and
  one-over-tested for multiple simultaneous pending entries and for output names coexisting
  with pending entries).
- These are logical accounting bounds, **not** exact allocation-capacity or peak-memory
  bounds: temporary strings, per-directory enumeration batch vectors, queue/output capacity
  growth, allocator overhead, and concurrent requests add memory beyond the accounted bytes.
  No global memory or concurrency budget follows.
- Paged catalog requests (`/v2/_catalog?n=…&last=…`) re-run the full walk per page; pagination
  remains in-memory in `CatalogQueryService` (sort, cursor position, slice) — unchanged.
- Descriptor containment does not establish snapshot isolation, hard-link isolation, or mount
  isolation; successive walks may observe different filesystem states. No cache or snapshot
  mechanism was added.

## 4. Why Catalog Discovery Remains Distinct from GC Discovery

Recognition rules and completeness requirements differ and are both load-bearing:
catalog omits reserved-name subtrees, root-adjacent `manifests`, and non-UTF-8 subtrees —
acceptable for `/v2/_catalog` presentation, but each omission would be a
reachability hole for GC. (UTF-8 names that cannot form contained keys now fail closed with
`CorruptData` in both walks.) Conversely,
GC returns manifest-directory keys (not repository
names), descends through reserved names, and fails closed on non-UTF-8. The two walks share
traversal mechanics (`repo_discovery` helpers) but keep separate policy functions; GC behavior
is regression-verified unchanged (`test_repo_discovery_reserved_leaf_names_excluded_and_gc_divergence`,
`test_repo_discovery_non_utf8_ancestors_skipped_vs_gc_walker`, GC integration suites).

## 5. Consolidated Open Decision: Operational Catalog Budgets

The production default is unbounded, matching the ambient baseline. **There is currently no
production configuration path for non-default limits**: `FsStorage::list_repo_names` passes
`CatalogDiscoveryLimits::default()` inline, and no constructor, config key, or CLI surface
exposes catalog limits. Adopting finite ceilings therefore requires both the operational
decision below and actual production wiring (constructor/limit plumbing plus minimum-value
validation mirroring the existing `try_new_with_all_limits` checks). One consolidated
recommendation for review:

| Limit | Recommended default | Rationale |
|---|---|---|
| `max_depth` | 32 | Matches approved GC discovery default; repository namespaces deeper than 32 segments exceed OCI-practical naming. |
| `max_dir_enumerations` | 10,000 | Matches GC default; bounds walk fan-out. |
| `max_total_entries` | 250,000 | Matches GC default; bounds cumulative inspection work. |
| `max_repositories` | 10,000 | Aligns with GC `max_manifest_dirs`; a catalog beyond this needs an operational review anyway. |
| `max_retained_path_bytes` | 10 MiB | Matches GC default; bounds queue+output name bytes. |
| `per_dir_limits` | 1,000 entries / 100,000 name bytes | Matches GC default. |

Affected callers on adoption: `_catalog` route (oversized catalogs become HTTP 500 instead of
unbounded work), `BlobRefIndex::rebuild`, GC policy/validation fallbacks, membership migration,
supervisor probe, `is_storage_empty` — all already fail closed on `Backend` errors. This is an
operational compatibility decision (a legitimately large registry would begin failing) and is
therefore **not** enacted in this batch.

## 6. Remaining Limitations

- `repo_timestamps` / `max_mtime_in_dir` and `is_storage_empty`'s `fs_dir_has_any_entry`
  remain ambient (inventory items R-4/R-5); membership, journal, upload/quarantine, and
  mutation-embedded reads remain as previously assessed.
- Write containment, durability, and rollback remain Gate O-04.
- Non-Linux behavior unverified (O-15); `FsMetadataReader` startup already requires Linux
  `openat2` for all contained reads.
- The committed decisions record references the former test name
  `test_repo_discovery_invalid_repo_name_aborts_downstream_manifest_listing`; that test is now
  `test_repo_discovery_unaddressable_repo_name_fails_closed` with the contained fail-closed
  contract.
- A discovery failure during migration application leaves the persisted `Applying` checkpoint
  (with its owner lease) in place; retry semantics are governed by the existing lease-expiry
  contract, which this batch does not change.

## 7. Canonical Quality Gates

`O-03`, `O-04`, `O-05`, `O-06`, `O-13`, `O-15`, `O-16`, and `D-06` all remain **OPEN**. This
cutover narrows O-05 (catalog discovery contained) but catalog budget policy (Section 5),
R-4/R-5, membership/journal/upload/quarantine reads, and mutation-embedded reads remain.
