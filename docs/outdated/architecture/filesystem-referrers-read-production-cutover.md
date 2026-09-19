> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Landed (`906ef89`); referrers later moved onto `referrer_domain` (`b1e607c`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Contained Filesystem Referrers Read: Production Cutover Record

- **Document:** `docs/architecture/filesystem-referrers-read-production-cutover.md`
- **Status:** Implementation & Compatibility Record (working tree, not committed)
- **Primary Repository Baseline:** `/home/dietmar/devel/rust/registry-rust` at HEAD `2419d46e89d88151c18972210ba79826bf776b59` (`master`), changes applied in the working tree only.
- **Dependency Repository:** `/home/dietmar/devel/rust/storage-layer-rust` at HEAD `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (strictly read-only; unchanged).
- **Predecessor Records:**
  - Characterization: `docs/architecture/filesystem-referrers-read-characterization.md` (committed in `2419d46`).
  - Integration design: `docs/architecture/filesystem-referrers-read-contained-integration-design.md` (untracked; errata corrected alongside this cutover).

---

## 1. What Was Implemented

### 1.1 New Module: `src/storage/fs/referrers_read.rs`

A production (not test-gated) contained referrers read helper, following the established
`tag_read.rs` conventions:

- `ReferrersReadLimits { max_payload_bytes: Option<u64>, max_descriptors: Option<usize> }`
  with `Default = { None, None }` (no seam-imposed ceilings).
- `referrers_key(repo, subject)` composing `repos/<repo>/referrers/<subject.hex()>.json`
  after structural path validation via the shared `tag_read::validate_path_component`
  (made `pub(crate)`; no duplicate validator was introduced).
- `read_referrers_contained(reader, repo, subject, limits)` performing descriptor-relative
  acquisition through `storage_core::ObjectPayloadReader` (`openat2` with
  `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS` plus `S_IFREG` validation in
  `storage_fs::FsMetadataReader`), bounded stream draining, JSON deserialization, and
  post-parse descriptor count checking. Acquisition errors are translated by the existing
  `read_adapter::translate_payload_read_error`; no new error adapter was added.

### 1.2 Production Routing Change: `FsStorage::list_referrers`

`FsStorage::list_referrers` now routes through
`referrers_read::read_referrers_contained(self.reader.as_ref(), name, subject, &ReferrersReadLimits::default())`,
replacing the ambient `tokio::fs::read(referrers_path(..))` + `serde_json::from_slice` body.

- The shared pinned-root reader `self.reader: Arc<FsMetadataReader>` is reused; no new reader,
  descriptor, or configuration was introduced.
- `referrers_path` is retained for the write/unlink legs of `add_referrer` and `remove_referrer`.
- `list_referrers_page` is **unchanged** (see Section 3.3).

### 1.3 Test Changes

- New unit/acceptance tests in `src/storage/fs/referrers_read.rs` (mock-reader and Linux
  real-filesystem categories; 20 tests, 1 of which is the `#[ignore]` unprivileged permission test).
- Four characterization tests in `src/storage/fs/tests.rs` (Group 3, formerly "Ambient path
  behavior") re-frozen to the contained production contract:
  - `test_contained_symlink_inside_root_rejected` (was `test_ambient_symlink_inside_fixture`)
  - `test_contained_symlink_outside_storage_root_rejected` (was `test_ambient_symlink_outside_storage_root`)
  - `test_contained_directory_symlink_rejected` (was `test_ambient_directory_symlink`)
  - `test_repo_name_path_traversal_rejected_at_storage_boundary` (was `test_repo_name_path_traversal_storage_boundary`)
- New module `referrers_contained_mutation_compat` in `src/storage/fs/tests.rs` (7 tests)
  covering mutation-read compatibility and production wiring (Section 3.2).
- All other referrers characterization tests (missing paths, corrupt payloads, EISDIR, ordering,
  pagination, service propagation) pass **unchanged**, and the pre-existing mutation regression
  `referrers_add_list_remove_and_delete_manifest` passes unchanged.

### 1.4 Documentation Changes

- Corrected two inaccurate statements in
  `filesystem-referrers-read-contained-integration-design.md`:
  1. The claim that a failed `ensure_dir` leaves "no directories created"
     (`std::fs::create_dir_all` can partially succeed before an error and can reuse existing
     directories; earlier creation is not rolled back).
  2. The claim that "peak allocation protection is provided solely by `max_payload_bytes`"
     (the limit bounds collected stream bytes at `N + 1`; vector capacity, allocator overhead,
     and simultaneous raw/parsed retention exceed it, and the descriptor-count check runs after
     deserialization).
- Added an implementation-status addendum to that design document.

---

## 2. Decision Resolutions (DEC-01 … DEC-09)

The design document's decision table was resolved as follows. These are routine engineering
decisions within the established contracts; none introduces an unapproved numeric production
default or changes public-route success behavior for canonical repository names.

| ID | Decision | Resolution |
|---|---|---|
| DEC-01 | Repository validation contract | **Alt A implemented**: structural path safety via the shared `tag_read::validate_path_component` (same contract already active for tag reads/listing). Not `CanonicalRepoName` grammar; no silent normalization. |
| DEC-02 | Missing paths & probes | **Alt A implemented**: `ReadError::NotFound` → `Ok(Vec::new())`, no repository probing. Matches OCI referrers semantics and the legacy contract. |
| DEC-03 | Error taxonomy mapping | **Alt A implemented**: acquisition errors translate through the existing `read_adapter::translate_payload_read_error` (NotFound→NotFound handled locally; resolution/symlink/non-regular/permission→`Io`; `openat2` unavailable→`Configuration`; runtime/join→`Backend`). |
| DEC-04 | Parsing error taxonomy | **Alt A implemented**: JSON deserialization failures keep the legacy `StorageError::io(serde_message)` mapping, preserving characterized messages (e.g. "EOF while parsing a value"). A future move to `CorruptData` remains a separately reviewable option. |
| DEC-05 | Resource ceilings | **Alt A implemented**: `ReferrersReadLimits` defaults to `None`/`None`. Production passes the default, preserving the exact unbounded characterization baseline. **No numeric production defaults were invented**; adopting operational ceilings is a consolidated open decision (Section 5). |
| DEC-06 | Direct read ordering | **Alt A implemented**: stored physical array order preserved; no sorting, deduplication, or normalization in storage. |
| DEC-07 | Paged error suppression | **Alt A retained**: `list_referrers_page` remains byte-for-byte unchanged (zero active production callers). Deprecation/fail-closed remains a separately identifiable follow-up slice. |
| DEC-08 | Pagination arithmetic | **Alt A retained**: unchecked `start_idx + page_limit` and duplicate-digest token ambiguity remain, still covered by characterization tests (including the complete-method panic observation). |
| DEC-09 | Mutation seam integration | **Superseded by combined slice**: rather than deferring, `list_referrers` was promoted **with** mutation-read compatibility analysis and tests in the same batch (Section 3.2), per the current working instruction. |

---

## 3. Observable Compatibility Changes

### 3.1 Standalone Read (`list_referrers`)

| Input class | Legacy ambient behavior | Contained behavior (now) |
|---|---|---|
| Canonical repo names, regular files, valid JSON | `Ok(descriptors)` in stored order | **Identical** |
| Missing repo / referrers dir / subject file | `Ok(vec![])` | **Identical** (`NotFound` at acquisition) |
| Corrupt/truncated/invalid-UTF-8/wrong-type JSON | `Err(Internal(Io))` with serde message | **Identical** |
| Directory at `<hex>.json` | `Err(Internal(Io))` (EISDIR) | **Identical kind** (`Io`; message now names the unsupported object type) |
| Permission denied | `Err(Internal(Io))` | **Identical kind** |
| **Symlinked file or directory component** | Followed silently (including escapes outside the root) | **`Err(Internal(Io))`** (kernel resolution rejection) |
| **Structurally invalid repo name** (`..`, `//`, leading/trailing `/`, `\`, NUL/control) | Ambient traversal; could read outside `repos/` or return `Ok(vec![])` on ENOENT | **`Err(StorageError::InvalidRepoName)`** with zero reader calls |
| Root pathname replaced at runtime | Followed the replacement tree | **Pinned to the originally opened root descriptor** (consistent with all other contained reads) |

Public-route impact: `GET /v2/<name>/referrers/<digest>` already rejects non-canonical names via
`CanonicalRepoName::parse` before storage, so the `InvalidRepoName` change is unreachable there.
The symlink rejection changes previously-served symlinked content into a fail-closed HTTP 500 —
the same policy adopted for contained tag/manifest/CAS reads.

### 3.2 Mutation-Read Compatibility (verified by tests)

Because `add_referrer`, `remove_referrer`, and `delete_manifest` (via `remove_referrer`) call
`self.list_referrers`, the cutover changes reads embedded in those mutations:

1. **`add_referrer` with a traversal name** now fails closed with `InvalidRepoName` — but only
   *after* `ensure_dir` (uncontained `std::fs::create_dir_all`) has run. Directory creation can
   fully or partially succeed and is **not rolled back**
   (`test_add_referrer_traversal_rejected_after_ensure_dir_side_effect`).
2. **`add_referrer` over a symlinked referrers file** now aborts with `Io` before serialization
   and writeback; the symlink and its target remain untouched
   (`test_add_referrer_symlinked_file_fails_closed_without_overwrite`). Under ambient reads the
   symlink target's parsed content would have been read and the link path atomically replaced.
3. **`add_referrer` over a corrupt file** keeps the pre-existing fail-closed contract: the
   corrupted file is not overwritten (`test_add_referrer_corrupt_existing_file_fails_closed`).
4. **`remove_referrer` over a symlinked referrers file** aborts with `Io`; no unlink or writeback
   occurs (`test_remove_referrer_symlinked_file_fails_closed`).
5. **`delete_manifest`** continues to ignore `remove_referrer` failures: with a symlinked subject
   referrers file, the manifest is removed, cleanup fails silently, and the rejected file remains
   as stale residue — the manifest does **not** survive
   (`test_delete_manifest_swallows_contained_referrer_cleanup_failure`). Referrer cleanup failure
   therefore still does not imply the manifest exists.
6. **Pinned-root wiring** is proven by `test_list_referrers_pinned_root_divergence`: after root
   pathname replacement, production `list_referrers` observes the originally pinned tree while
   pathname mutations would operate on the replacement tree (pre-existing divergence, unchanged).

Read containment does **not** contain the writes in these mutations (`ensure_dir`,
`atomic_write_file`, `remove_file` remain ambient) and provides no rollback; that remains Gate O-04.

### 3.3 `list_referrers_page` (unchanged code, inherited read change)

The method body is unchanged. Because it delegates to `list_referrers`, its underlying read is now
contained, but `.unwrap_or_default()` still suppresses **all** errors — including the new
`InvalidRepoName` and symlink rejections — into `Ok((vec![], None))`. Notably, a traversal
repository name that previously could *leak data* through the paged path now yields an empty page.
Zero active production callers exist (re-verified: only trait declarations, forwarding adapters
with no call sites, mocks, and live-S3 harness references). The suppression and unchecked
`start_idx + page_limit` arithmetic remain documented, characterization-covered latent defects.

---

## 4. Resource Contract (as implemented)

- A representable finite ceiling `N` permits collecting at most `N + 1` bytes to detect overflow;
  this is a collected-byte ceiling, not a peak-memory or peak-allocation bound.
- `max_descriptors` is checked after deserialization; it limits accepted results, not parsing
  allocation. No global memory/concurrency budget follows. No decompression stage exists.
- Edge cases (all test-covered):
  - Missing file → empty success at acquisition, before any limit evaluation.
  - Existing 0-byte file with `Some(0)` → byte check passes; JSON parse fails with legacy `Io`.
  - Existing nonempty file with `Some(0)` → `CorruptData` overflow before parsing.
  - `[]` (2 bytes) with `max_descriptors = Some(0)` → valid empty array accepted.
  - `Some(u64::MAX)` → `CorruptData` (checked `limit + 1` unrepresentable), evaluated **after**
    acquisition, so `NotFound` (empty success) and acquisition errors take precedence.
- Concurrent modification can yield truncated bytes (parse failure) or changed-but-valid JSON;
  parsing does not establish snapshot consistency.

---

## 5. Remaining Open Items (consolidated, not blockers for this batch)

1. **Operational numeric limits for referrers reads** (`max_payload_bytes` / `max_descriptors`
   production values, config plumbing analogous to `tag_listing_*`): requires a product decision;
   the seam accepts limits without further code changes.
2. **`list_referrers_page` policy** (deprecate/remove vs. fail-closed + saturating arithmetic):
   zero production callers; separately identifiable follow-up.
3. **JSON parse taxonomy migration** to `CorruptData` (DEC-04 Alt B): observable-message change on
   the public 500 path; kept legacy for this cutover.
4. **Write containment for referrers mutations** (`ensure_dir`, `atomic_write_file`,
   `remove_file`): Gate O-04.

## 6. Canonical Quality Gates

`O-03`, `O-04`, `O-05`, `O-06`, `O-13`, `O-15`, `O-16`, and `D-06` all remain **OPEN**. This
cutover narrows the O-05 gap (referrers point reads/listing now contained) but does not close it:
catalog discovery, timestamps/emptiness, membership, journal, upload/quarantine, and
mutation-embedded reads remain uncontained (see the updated gap assessment).
