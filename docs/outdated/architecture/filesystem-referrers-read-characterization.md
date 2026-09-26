> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Contained referrers reads landed (`906ef89`); referrers later moved onto `referrer_domain` (`b1e607c`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Filesystem OCI Referrers Read Characterization

## Executive Summary & Baseline

This document records the empirical baseline and architectural characterization of the OCI referrers read implementation in `registry-rust`. This slice is strictly confined to **characterization tests and documentation**; the production implementation remains 100% byte-for-byte unchanged.

### Verified Repository Baselines

- **Primary Repository**: `~/devel/rust/registry-rust`
  - Current HEAD: `f1d6d9c128a8a713f2d929b03c3a66ed06bf30fe`
  - Active Branch: `master`
  - Prior Commit: `fix(storage): use contained filesystem tag listing in production`
- **Dependency Repository**: `~/devel/rust/storage-layer-rust`
  - Current HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`
  - Active Branch: `main`
  - Strictly read-only throughout this work.
- **Accepted Prior Assessment**:
  - Archive: `~/devel/rust/manifest-read-review-evidence/session-20260912-2345/filesystem-read-containment-post-tag-listing-assessment.tar.gz`
  - SHA-256: `d99b526451e8b8c8c7cbfc37dfaa16afdcc68ce983262da1e3bde1d5b63dda05`
  - Size: 39,799 bytes (8 payload files + `MANIFEST.sha256`).

### Scope & Constraints

- **Authorized Scope**: Test-only additions under `#[cfg(test)]` in [src/storage/fs/tests.rs](src/storage/fs/tests.rs) (28 characterization tests in `referrers_read_characterization`) and this documentation artifact.
- **Production Preservation**: Zero production code changes. [src/storage/fs.rs](src/storage/fs.rs), [src/application/referrers.rs](src/application/referrers.rs), [src/http_api/referrers.rs](src/http_api/referrers.rs), storage traits, mutation methods, and configuration files are preserved byte-for-byte.
- **Canonical Quality Gates**: All canonical gates remain explicitly **OPEN**:
  - `O-03`: Key and continuation-token contracts.
  - `O-04`: Filesystem write durability and containment.
  - `O-05`: Broader filesystem read containment.
  - `O-06`: Typed AWS mapping and pinned-MinIO evidence.
  - `O-13`: Hosting, distribution, and release strategy.
  - `O-15`: Non-Linux verification.
  - `O-16`: Earlier Slice 11 audit/test-inventory evidence.
  - `D-06`: Broader extraction, cutover, compatibility, and distribution acceptance.

---

## Source Symbols, Call Chains, and Reachability

### 1. Storage Symbols

- **`FsStorage::referrers_path`** ([src/storage/fs.rs:671-678](src/storage/fs.rs#L671-L678)):
  ```rust
  fn referrers_path(&self, name: &str, subject: &Digest) -> PathBuf {
      self.root
          .join("repos")
          .join(name)
          .join("referrers")
          .join(format!("{}.json", subject.hex()))
  }
  ```
  Constructs an uncontained ambient pathname. Does not check repository boundaries or validate against directory traversal.

- **`FsStorage::list_referrers`** ([src/storage/fs.rs:1721-1735](src/storage/fs.rs#L1721-L1735)):
  ```rust
  async fn list_referrers(
      &self,
      name: &str,
      subject: &Digest,
  ) -> Result<Vec<ReferrerDescriptor>, StorageError> {
      let path = self.referrers_path(name, subject);
      let bytes = match tokio::fs::read(&path).await {
          Ok(b) => b,
          Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
          Err(err) => return Err(StorageError::io(err.to_string())),
      };
      serde_json::from_slice::<Vec<ReferrerDescriptor>>(&bytes)
          .map_err(|err| StorageError::io(err.to_string()))
  }
  ```
  Performs an uncontained direct read using `tokio::fs::read`. Treats `NotFound` as terminal empty success (`Ok(Vec::new())`). All other I/O errors and JSON deserialization errors map to `StorageError::Internal { kind: StorageErrorKind::Io, .. }`. Preserves file storage order without sorting.

- **`FsStorage::list_referrers_page`** ([src/storage/fs.rs:1232-1261](src/storage/fs.rs#L1232-L1261)):
  ```rust
  async fn list_referrers_page(
      &self,
      repo: &str,
      subject: &Digest,
      continuation_token: Option<&str>,
      page_limit: usize,
  ) -> Result<(Vec<ReferrerDescriptor>, Option<String>), StorageError> {
      let mut refs = self.list_referrers(repo, subject).await.unwrap_or_default();
      refs.sort_by(|a, b| a.digest.cmp(&b.digest));

      let start_idx = if let Some(token) = continuation_token {
          match refs.binary_search_by(|r| r.digest.as_str().cmp(token)) {
              Ok(idx) => idx + 1,
              Err(idx) => idx,
          }
      } else {
          0
      };

      let end_idx = (start_idx + page_limit).min(refs.len());
      let page_slice = &refs[start_idx..end_idx];

      let next_token = if end_idx < refs.len() {
          page_slice.last().map(|r| r.digest.clone())
      } else {
          None
      };

      Ok((page_slice.to_vec(), next_token))
  }
  ```
  Calls `self.list_referrers` and immediately invokes `.unwrap_or_default()`. This silently suppresses **all** disk read errors, permission denials, corrupted JSON, and invalid types into an empty vector (`Ok((vec![], None))`). Sorts descriptors in-memory lexicographically by digest.

### 2. Caller Reachability Audit

- **Public OCI Route Caller**:
  - `GET /v2/<name>/referrers/<digest>` is handled by `http_api::referrers::get_referrers` ([src/http_api/referrers.rs:25-102](src/http_api/referrers.rs#L25-L102)).
  - Handler delegates to `ReferrersQueryService::query_referrers` ([src/application/referrers.rs:36-95](src/application/referrers.rs#L36-L95)).
  - `query_referrers` calls `reader.list_referrers(repo, subject).await` **directly**.
  - **The public query service does NOT call `list_referrers_page`**.
  - Service-level error handling:
    - `StorageError::NotFound` becomes an empty vector.
    - All other storage errors (`StorageError::Internal { kind: Io, .. }`) are returned as `ReferrersQueryError::Storage(e)`, which the HTTP handler maps to HTTP 500 (`errors::internal_error()`).
    - Repository name validation occurs at the service entry point via `CanonicalRepoName::parse(repo)`. Traversal syntax like `../` is rejected as `ReferrersQueryError::InvalidRepoName` (HTTP 400 `NAME_INVALID`) before storage is invoked.

- **Status of `list_referrers_page`**:
  - Exhaustive codebase inspection confirms that **there is no active production caller of `list_referrers_page`**.
  - All existing references are:
    1. Trait declarations on `Storage` ([src/storage/mod.rs:431](src/storage/mod.rs#L431)) and `ReferrersReader` ([src/storage/ports/mod.rs:123](src/storage/ports/mod.rs#L123)).
    2. Forwarding adapter implementations in `Arc<dyn Storage>`, `Arc<dyn ReferrersReader>`, and macro `impl_referrers_reader!`.
    3. Unused delegating methods in `supervisor.rs:1705`, `manifest_lifecycle.rs:1904`, `blob_ref_index.rs:1268`.
    4. Mock implementations in `tests/ports_wiring_tests.rs` and `tests/support/gc_coordination.rs`.
    5. Live S3 integration harness in `tests/s3_live_integration.rs`.
  - Therefore, the error-swallowing flaw (`unwrap_or_default()`) in `list_referrers_page` is a latent defect on the storage capability port, but does not affect the current public HTTP route.

---

## Empirical Behavior Matrix

The following matrix records the empirically observed behavior of `FsStorage::list_referrers`, `FsStorage::list_referrers_page`, and caller boundaries across all tested categories:

| # | Fixture / Condition | Direct Read (`list_referrers`) | Paged Read (`list_referrers_page`) | Error Kind / Code | Service Caller Behavior | Evidence Type |
|---|---------------------|--------------------------------|------------------------------------|-------------------|-------------------------|---------------|
| 1 | Missing repository directory | `Ok(vec![])` | `Ok((vec![], None))` | N/A (NotFound -> empty) | Returns empty `ReferrersPage` | Executed Test |
| 2 | Existing repo, missing `referrers/` dir | `Ok(vec![])` | `Ok((vec![], None))` | N/A (NotFound -> empty) | Returns empty `ReferrersPage` | Executed Test |
| 3 | Existing `referrers/` dir, missing `<hex>.json` | `Ok(vec![])` | `Ok((vec![], None))` | N/A (NotFound -> empty) | Returns empty `ReferrersPage` | Executed Test |
| 4 | Empty JSON array file `[]` | `Ok(vec![])` | `Ok((vec![], None))` | N/A | Returns empty `ReferrersPage` | Executed Test |
| 5 | Multi-descriptor JSON array `[C, A, B]` | `Ok(vec![C, A, B])` (preserves file order) | `Ok(vec![A, B, C])` (lexically sorted by digest) | N/A | Slices sorted page, applies filter | Executed Test |
| 6 | SHA-256 subject path (`<64hex>.json`) | `Ok(descriptors)` from `repos/<repo>/referrers/<64hex>.json` | `Ok((descriptors, token))` | N/A | Processes page | Executed Test |
| 7 | SHA-512 subject path (`<128hex>.json`) | `Ok(descriptors)` from `repos/<repo>/referrers/<128hex>.json` | `Ok((descriptors, token))` | N/A | Processes page | Executed Test |
| 8 | Empty file (0 bytes) | `Err(StorageError::Internal { kind: Io, .. })` ("EOF while parsing a value") | `Ok((vec![], None))` (suppressed via `unwrap_or_default`) | `StorageErrorKind::Io` | Returns `ReferrersQueryError::Storage` (HTTP 500) | Executed Test |
| 9 | Whitespace-only file (`"   \n\t  "`) | `Err(StorageError::Internal { kind: Io, .. })` (EOF) | `Ok((vec![], None))` (suppressed) | `StorageErrorKind::Io` | Returns `ReferrersQueryError::Storage` (HTTP 500) | Executed Test |
| 10 | Truncated JSON | `Err(StorageError::Internal { kind: Io, .. })` (syntax error) | `Ok((vec![], None))` (suppressed) | `StorageErrorKind::Io` | Returns `ReferrersQueryError::Storage` (HTTP 500) | Executed Test |
| 11 | Invalid JSON syntax | `Err(StorageError::Internal { kind: Io, .. })` (syntax error) | `Ok((vec![], None))` (suppressed) | `StorageErrorKind::Io` | Returns `ReferrersQueryError::Storage` (HTTP 500) | Executed Test |
| 12 | Wrong top-level JSON type (object `{}`) | `Err(StorageError::Internal { kind: Io, .. })` ("invalid type: map, expected a sequence") | `Ok((vec![], None))` (suppressed) | `StorageErrorKind::Io` | Returns `ReferrersQueryError::Storage` (HTTP 500) | Executed Test |
| 13 | Invalid UTF-8 byte sequence | `Err(StorageError::Internal { kind: Io, .. })` | `Ok((vec![], None))` (suppressed) | `StorageErrorKind::Io` | Returns `ReferrersQueryError::Storage` (HTTP 500) | Executed Test |
| 14 | Valid JSON with whitespace formatting | `Ok(descriptors)` (deserializes formatted JSON) | `Ok((descriptors, None))` | N/A | Deserializes formatted JSON | Executed Test |
| 15 | Moderate payload (100 descriptors, ~20 KB) | `Ok(descriptors)` (absence of configured ceiling) | `Ok((descriptors, None))` | N/A | Deserializes all 100 descriptors | Executed Test |
| 16 | Symlink to file inside storage root | Followed ambiently; returns target descriptors | Followed ambiently | N/A | Followed ambiently | Executed Test |
| 17 | Symlink to file outside storage root | Followed ambiently; escapes root without containment error | Followed ambiently | N/A | Followed ambiently | Executed Test |
| 18 | Directory symlink on `referrers/` | Followed ambiently | Followed ambiently | N/A | Followed ambiently | Executed Test |
| 19 | Path traversal in repo name (`../`) | Traversed ambiently at storage boundary if intermediate dir exists | Traversed ambiently | N/A | Blocked at service boundary: `InvalidRepoName` (HTTP 400) | Executed Test |
| 20 | Directory in place of JSON file | `Err(StorageError::Internal { kind: Io, .. })` (EISDIR) | `Ok((vec![], None))` (suppressed) | `StorageErrorKind::Io` | Returns `ReferrersQueryError::Storage` (HTTP 500) | Executed Test |
| 21 | Permission denied (`chmod 0o000`) | `Err(StorageError::Internal { kind: Io, .. })` (`PermissionDenied`) | `Ok((vec![], None))` (suppressed) | `StorageErrorKind::Io` | Returns `ReferrersQueryError::Storage` (HTTP 500) | Executed Test (Ignored by default; verified) |
| 22 | Pagination: 3 items, limit 1 | N/A | Page 1: `([A], Some(A))` | N/A | Public service performs its own pagination | Executed Test |
| 23 | Pagination: middle page | N/A | Page 2: `([B], Some(B))` | N/A | Public service performs its own pagination | Executed Test |
| 24 | Pagination: terminal page | N/A | Page 3: `([C], None)` | N/A | Public service performs its own pagination | Executed Test |
| 25 | Pagination: beyond terminal page | N/A | Page 4: `([], None)` | N/A | Public service performs its own pagination | Executed Test |
| 26 | Pagination: absent token before elements | N/A | Starts at index 0: `([A], Some(A))` | N/A | Public service defaults absent token to 0 | Executed Test |
| 27 | Pagination: absent token between elements | N/A | Starts at insertion index 1: `([C], None)` | N/A | Public service defaults absent token to 0 | Executed Test |
| 28 | Pagination: absent token after elements | N/A | Starts at `refs.len()`: `([], None)` | N/A | Public service defaults absent token to 0 | Executed Test |
| 29 | Pagination: duplicate descriptor digests | N/A | Non-deterministic binary search index; returns <= 1 item | N/A | Public service uses linear `.position()` | Executed Test |
| 30 | Pagination: limit = 0 (empty terminal success) | N/A | `Ok(([], None))` (valid and corrupt files yield empty success) | N/A | Public service returns `([], None, has_more)` | Executed Test (read-before-slicing is Source Evidence) |
| 31 | Pagination: limit = `usize::MAX`, `start_idx = 0` | N/A | `Ok((all_refs, None))`; addition does not overflow | N/A | Evaluates without overflow | Executed Test |
| 32 | Pagination: limit = `usize::MAX`, `start_idx > 0` | N/A | Panics: addition overflow in debug; slice bounds in release | Panic | N/A | Executed Test |

---

## Detailed Architectural Findings

### 1. Direct-Read vs Paged-Read Divergence

- **Ordering**:
  - `list_referrers` preserves the exact physical order of entries as stored in the JSON file.
  - `list_referrers_page` sorts all entries in-memory using `refs.sort_by(|a, b| a.digest.cmp(&b.digest))`.
- **Error Suppression (`unwrap_or_default`)**:
  - `list_referrers` returns `StorageError::Internal { kind: StorageErrorKind::Io, .. }` for any filesystem error (permission denied, EISDIR) or deserialization error (truncated JSON, invalid UTF-8, wrong type).
  - `list_referrers_page` masks every error into an empty `Vec<ReferrerDescriptor>`. To any paged caller, a corrupted index, an I/O timeout, an unreadable symlink, or an unprivileged permission denial appears identical to a valid subject with zero referrers.
- **Memory Consumption**:
  - Both methods read the entire `<hex>.json` file into memory at once. `list_referrers_page` does not stream or parse incrementally; pagination is purely in-memory post-processing.

### 2. Pagination Mechanics & Complete-Method Panic Observations

- **Current Slicing Arithmetic**:
  ```rust
  let end_idx = (start_idx + page_limit).min(refs.len());
  let page_slice = &refs[start_idx..end_idx];
  ```
- **Arithmetic Behavior Across Build Profiles**:
  - **Case A: `start_idx == 0` with `page_limit == usize::MAX`**:
    `0 + usize::MAX` equals `usize::MAX`. `usize::MAX.min(refs.len())` evaluates cleanly to `refs.len()`. The method succeeds and returns all descriptors with `next_token = None`.
  - **Case B: `start_idx > 0` with `page_limit == usize::MAX`**:
    - **Under Debug / Test Profile (`overflow-checks = true`)**: `start_idx + page_limit` panics with `attempt to add with overflow`.
    - **Under Release Profile without Overflow Checks**: `start_idx + page_limit` wraps (e.g. `1 + usize::MAX == 0`). Then `end_idx = 0.min(refs.len()) == 0`. The subsequent slice `&refs[start_idx..end_idx]` evaluates to `&refs[1..0]`, which panics with `slice index starts at 1 but ends at 0`.
    - **Empirical Confirmation**: Observed safely using an awaited spawned Tokio task (`tokio::spawn`), catching the panic cleanly and asserting `join_err.is_panic()`.
- **Continuation Token Edge Cases**:
  - When `continuation_token` matches an existing digest, `binary_search_by` yields `Ok(idx)`, advancing `start_idx = idx + 1`.
  - When the token is absent:
    - Token smaller than all digests yields `Err(0)` -> `start_idx = 0` (starts from beginning).
    - Token between digests yields `Err(idx)` -> `start_idx = idx` (starts from first element greater than token).
    - Token greater than all digests yields `Err(refs.len())` -> `start_idx = refs.len()`, slicing to empty.
  - When multiple descriptors share the same digest: Rust's standard library `binary_search_by` does not guarantee which matching element is returned. In `list_referrers_page`, if index `0` is returned, `start_idx` becomes `1`; if index `1` is returned, `start_idx` becomes `2` (skipping the second duplicate entirely).
- **Zero Page Limit Behavior & Read Precedence**:
  - When `page_limit == 0`, `end_idx = start_idx`. `end_idx < refs.len()` may be true (e.g. `0 < 2`), but `page_slice` has length 0, so `page_slice.last()` returns `None`.
  - Both valid JSON files and corrupted JSON files yield empty terminal success `Ok((vec![], None))`.
  - **Source vs. Behavioral Evidence**: Behavioral observation alone does not distinguish whether `page_limit == 0` short-circuited or whether the read/parse occurred and errors were swallowed by `unwrap_or_default()`. Source code inspection of [src/storage/fs.rs:1239](src/storage/fs.rs#L1239) confirms that `self.list_referrers(repo, subject).await` is invoked unconditionally on line 1239 before `page_limit` is inspected on line 1251.

### 3. Descriptor Schema & Field Deserialization

- The in-storage JSON document is deserialized into `Vec<ReferrerDescriptor>` ([src/storage/mod.rs:341-351](src/storage/mod.rs#L341-L351)):
  ```rust
  pub struct ReferrerDescriptor {
      pub media_type: String,
      pub digest: String,
      pub size: u64,
      #[serde(skip_serializing_if = "Option::is_none")]
      pub artifact_type: Option<String>,
      #[serde(skip_serializing_if = "Option::is_none")]
      pub annotations: Option<HashMap<String, String>>,
  }
  ```
- **Field Naming**: The struct does not declare `#[serde(rename_all = "camelCase")]`. Consequently, the stored JSON keys are snake_case (`media_type`, `artifact_type`). Deserialization strictly rejects `mediaType` with `"missing field media_type"`.
- **Validation**: Serde enforces standard JSON typing (string, string, integer, optional map). No semantic validation is performed on the digest format or media type during storage deserialization.

### 4. Ambient Path Traversal & Containment Status

- `FsStorage` performs no pathname canonicalization, `openat2` resolution, or root boundary verification for referrers:
  - Final file symlinks pointing inside or outside the storage root are followed ambiently by `tokio::fs::read`.
  - Directory symlinks on `referrers/` are followed ambiently.
  - If a caller supplies a repository name with traversal segments (e.g. `../escaped_repo`), `FsStorage::referrers_path` concatenates it into `<root>/repos/../escaped_repo/referrers/<hex>.json`. If the intermediate directory exists, the kernel resolves the path outside `repos/`.
- However, the public query service (`ReferrersQueryService::query_referrers`) enforces `CanonicalRepoName::parse(repo)` before invoking storage. Traversal sequences are rejected with `InvalidRepoName` at the application layer.

### 5. Relationship to Mutation Boundaries

- `list_referrers` is called during mutations:
  - **`FsStorage::add_referrer`** ([src/storage/fs.rs:1736-1755](src/storage/fs.rs#L1736-L1755)): Calls `self.list_referrers(name, subject).await?`, appends the new descriptor (or updates if digest already exists), serializes the array, and writes to disk via `tokio::fs::write`.
  - **`FsStorage::remove_referrer`** ([src/storage/fs.rs:1757-1785](src/storage/fs.rs#L1757-L1785)): Calls `self.list_referrers(name, subject).await?`, filters out the matching digest, and writes back or deletes the file if the vector becomes empty.
  - **`delete_manifest`** ([src/storage/fs.rs:1140-1200](src/storage/fs.rs#L1140-L1200)): Removes the referrers file `<hex>.json` when the subject manifest is deleted.
- These mutations rely on `list_referrers` failing fail-closed on corrupted data (`?` propagates `StorageError::io`). If `list_referrers` were to swallow errors (like `list_referrers_page` does), `add_referrer` on a corrupted file would overwrite existing referrers with an array containing only the newly added referrer, destroying the corrupted data without notice.

---

## Limitations of Characterization

1. **Build Profile**: Panic observation on `start_idx + page_limit` was tested under the standard `test` profile (`unoptimized + debuginfo`), which has `overflow-checks = true`. Behavior under release profiles with overflow checks disabled was analyzed via code inspection of wrapping arithmetic semantics.
2. **Permissions Environment**: Permission-denied testing requires an unprivileged OS user where `chmod 0o000` prevents file access. The test is marked `#[ignore = "requires unprivileged user environment where chmod 0o000 denies filesystem access"]` for CI environments running as root UID 0, and was verified independently with `--locked -- --ignored`.
3. **Non-Regular Files**: Testing FIFOs with `tokio::fs::read` blocks the thread pool indefinitely because standard open without `O_NONBLOCK` waits for a writer. Reads are strictly confined to test-owned fixtures; host device access (such as `/dev/null`) is excluded. Non-regular object behavior is characterized safely using directory objects (returning `EISDIR`).
4. **No Resource Limit**: Tests verified the absence of a configured payload ceiling using a 100-descriptor fixture (~20 KB). A finite test cannot prove infinite capacity, but establishes that current production code enforces no budget checks.

---

## Remaining Compatibility Decisions & Future Design Scope

The following architectural and design decisions are intentionally deferred to future slices:

1. **Containment Seam Selection**: Whether to introduce contained reading via `open_payload` / `get_metadata` or extend openat2-based containment to `referrers_path`.
2. **Payload Size & Count Ceilings**: Whether to configure explicit size ceilings (e.g. `ReferrersReadLimits` analogous to `TagReadLimits` and `ManifestListingLimits`).
3. **Error Policy for `list_referrers_page`**:
   - Whether to retain `unwrap_or_default()` for backward compatibility with external storage callers, or align it with `list_referrers` to fail closed on corrupted documents.
4. **Pagination Arithmetic Hardening**:
   - Replacing `(start_idx + page_limit).min(refs.len())` with `start_idx.saturating_add(page_limit).min(refs.len())` to eliminate the complete-method panic.
5. **Duplicate Digest Token Determinism**:
   - Defining whether continuation tokens on duplicate digests should use linear scanning or stable cursor indexing.

---

## Verification Summary

### 1. Pre-Verification and Post-Verification Source Identity

To guarantee that the exact source tested during verification matches the final delivered files without drift:

| File | Pre-Verification SHA-256 | Post-Verification SHA-256 | Identity Status |
|------|--------------------------|---------------------------|-----------------|
| `src/storage/fs/tests.rs` | `02156e6eb7648719ced2f61d3d143de8387676ece9c670584f0a66a510034763` | `02156e6eb7648719ced2f61d3d143de8387676ece9c670584f0a66a510034763` | **Identical** |
| `docs/architecture/filesystem-referrers-read-characterization.md` | Recorded in `preservation_hashes.md` | Recorded in `preservation_hashes.md` | **Identical** |

Production preservation against committed baseline (`f1d6d9c128a8a713f2d929b03c3a66ed06bf30fe`):

| Production File | Baseline SHA-256 | Post-Verification SHA-256 | Status |
|-----------------|------------------|---------------------------|--------|
| `src/storage/fs.rs` | `03e118a0f46d5b94b1ad6f5df33a466f42d6381737dd67f4ddbacee8877be8d8` | `03e118a0f46d5b94b1ad6f5df33a466f42d6381737dd67f4ddbacee8877be8d8` | **Identical** |
| `src/application/referrers.rs` | `1070dc4a6602fe1a772905f8134dabbb416edb2090969b6879e8d8c9f9747bdb` | `1070dc4a6602fe1a772905f8134dabbb416edb2090969b6879e8d8c9f9747bdb` | **Identical** |
| `src/http_api/referrers.rs` | `99fe84fffad06297bbb2d686d55afc109676db1c42d346c147b563a6dfa03916` | `99fe84fffad06297bbb2d686d55afc109676db1c42d346c147b563a6dfa03916` | **Identical** |

### 2. Test Suite Results (`--locked`)

```text
running 28 tests
test storage::fs::tests::referrers_read_characterization::test_ambient_symlink_inside_fixture ... ok
test storage::fs::tests::referrers_read_characterization::test_ambient_directory_symlink ... ok
test storage::fs::tests::referrers_read_characterization::test_directory_in_place_of_json_file ... ok
test storage::fs::tests::referrers_read_characterization::test_empty_file_serde_eof_error ... ok
test storage::fs::tests::referrers_read_characterization::test_ambient_symlink_outside_storage_root ... ok
test storage::fs::tests::referrers_read_characterization::test_invalid_json_syntax ... ok
test storage::fs::tests::referrers_read_characterization::test_missing_repository ... ok
test storage::fs::tests::referrers_read_characterization::test_invalid_utf8_payload ... ok
test storage::fs::tests::referrers_read_characterization::test_missing_referrers_dir ... ok
test storage::fs::tests::referrers_read_characterization::test_pagination_large_limit_start_idx_zero ... ok
test storage::fs::tests::referrers_read_characterization::test_missing_subject_file ... ok
test storage::fs::tests::referrers_read_characterization::test_empty_json_array ... ok
test storage::fs::tests::referrers_read_characterization::test_permission_denied_file ... ignored, requires unprivileged user environment where chmod 0o000 denies filesystem access
test storage::fs::tests::referrers_read_characterization::test_repo_name_path_traversal_storage_boundary ... ok
test storage::fs::tests::referrers_read_characterization::test_wrong_toplevel_json_type ... ok
test storage::fs::tests::referrers_read_characterization::test_valid_json_with_whitespace_formatting ... ok
test storage::fs::tests::referrers_read_characterization::test_pagination_zero_page_limit_behavior ... ok
test storage::fs::tests::referrers_read_characterization::test_whitespace_only_file ... ok
test storage::fs::tests::referrers_read_characterization::test_truncated_json ... ok
test storage::fs::tests::referrers_read_characterization::test_moderate_payload_absence_of_read_ceiling ... ok
test storage::fs::tests::referrers_read_characterization::test_pagination_duplicate_descriptor_digests ... ok
test storage::fs::tests::referrers_read_characterization::test_valid_sha256_and_sha512_subject_paths ... ok
test storage::fs::tests::referrers_read_characterization::test_pagination_continuation_tokens_absent ... ok
test storage::fs::tests::referrers_read_characterization::test_pagination_lexical_sorting_and_pages ... ok
test storage::fs::tests::referrers_read_characterization::test_service_caller_rejects_path_traversal_repo_name ... ok
test storage::fs::tests::referrers_read_characterization::test_service_caller_referrers_query_corrupt_json_error_propagation ... ok
test storage::fs::tests::referrers_read_characterization::test_valid_multi_descriptor_ordering_and_fields ... ok
test storage::fs::tests::referrers_read_characterization::test_pagination_large_limit_start_idx_gt_zero_complete_method_panic ... ok

test result: ok. 27 passed; 0 failed; 1 ignored; 0 measured; 865 filtered out; finished in 0.22s
```

Explicit execution of the ignored permission-denied test:
```text
running 1 test
test storage::fs::tests::referrers_read_characterization::test_permission_denied_file ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 892 filtered out; finished in 0.00s
```

Existing mutation regression test:
```text
running 1 test
test storage::fs::tests::referrers_add_list_remove_and_delete_manifest ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 892 filtered out; finished in 0.03s
```

### 3. Verification Commands and Statuses

| Command | Exit Code | Purpose |
|---------|-----------|---------|
| `cargo fmt --check` | 0 | Code formatting validation |
| `cargo check --locked --all-targets --all-features` | 0 | Compilation and type checking |
| `cargo clippy --locked --all-targets --all-features -- -D warnings` | 0 | Lint and warning validation |
| `git diff --check` | 0 | Whitespace and syntax diff checks |
| `cargo test --locked --lib storage::fs::tests::referrers_read_characterization` | 0 | 27 characterization tests executed |
| `cargo test --locked --lib storage::fs::tests::referrers_read_characterization::test_permission_denied_file -- --ignored` | 0 | Unprivileged permission test executed |
| `cargo test --locked --lib storage::fs::tests::referrers_add_list_remove_and_delete_manifest` | 0 | Mutation regression test executed |
