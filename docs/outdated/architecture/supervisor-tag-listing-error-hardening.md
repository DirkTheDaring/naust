# Supervisor Tag-Listing Error Hardening Design & Verification

- **Document:** `docs/architecture/supervisor-tag-listing-error-hardening.md`
- **Target Repository:** `registry-rust` (HEAD: `5779f7f97f1109bcbb15bb6ad47e845bd4686bbf`)
- **Dependency Repository:** `storage-layer-rust` (HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`, strictly read-only)
- **Status:** **IMPLEMENTED** on `master` (error propagation in `compute_protected_blobs`; `NotFound` → empty tag list, other errors fail the proxy-GC path). The original “NOT COMMITTED / before contained tag listing” framing is obsolete. Production tag listing is `tag_domain`, not `contained_list_tags_seam`. Current inventory: [`current-state.md`](current-state.md).
- **Canonical Quality Gates:** All eight quality gates remain explicitly **OPEN** as acceptance criteria (`O-03, O-04, O-05, O-06, O-13, O-15, O-16, D-06`).

---

## 1. Executive Summary & Purpose

This implementation slice hardens supervisor tag-listing error handling in [`src/supervisor.rs`](src/supervisor.rs) before any production promotion of contained filesystem tag listing (`contained_list_tags_seam`).

Previously, within `compute_protected_blobs` under the `KeepLatestCachedSemver` eviction policy, wildcard error suppression (`if let Ok(tags) = storage.list_tags(&repo).await`) silently swallowed all tag listing errors. Any transient I/O failure, permission fault, or contained directory-entry limit exceedance silently produced zero pinned tags for that repository.

This slice implements the authorized error propagation policy reviewed in `session-20260912-2045/supervisor-tag-listing-failure-policy-assessment.tar.gz`.

---

## 2. Authorized Policy and Production Code Changes

### 2.1 Selected `NotFound` Compatibility Policy
In `compute_protected_blobs`, `StorageError::NotFound` is explicitly treated as empty tags (`Vec::new()`).
- **Compatibility Choice:** In legacy filesystem storage, an existing repository lacking a `tags/` directory already returns an empty successful listing (`Ok(Vec::new())`). The injected `NotFound` test uses an existing catalogued repository whose listing is overridden via mock injection; it is not a real missing-directory experiment.
- **Policy Definition:** `NotFound`-as-empty is the explicitly selected compatibility policy (aligning with lifecycle and migration policy choices), not proof of stable repository absence.
- **Scope Limit:** This compatibility choice is explicitly selected for this slice.

### 2.2 Error Propagation
Every non-`NotFound` listing error returns `Err(String)` containing the repository context and the original error text:
```rust
failed to list tags for repository '{repo}' during proxy gc: {e}
```

### 2.3 Exact Production Code Modification
In [`src/supervisor.rs`](src/supervisor.rs#L936-L953), lines 936–953:

```rust
                crate::config::EvictionPolicy::KeepLatestCachedSemver {
                    tag_regex,
                    allow_prerelease,
                } => {
                    let tags = match storage.list_tags(&repo).await {
                        Ok(tags) => tags,
                        Err(storage::StorageError::NotFound) => Vec::new(),
                        Err(e) => {
                            return Err(format!(
                                "failed to list tags for repository '{repo}' during proxy gc: {e}"
                            ));
                        }
                    };
                    if let Some(latest) =
                        pick_latest_semver_tag(tags, tag_regex.as_deref(), *allow_prerelease)
                    {
                        pinned_tags.push(latest);
                    }
                }
```

### 2.4 Function Signature and Outer Error Propagation
- **Signature Preserved:** `pub async fn compute_protected_blobs(storage: &(impl storage::BlobIndexStoragePort + ?Sized), repo_rules: &[crate::config::ProxyRepoRule], proxy: &crate::proxy::Proxy) -> Result<HashSet<String>, String>` is preserved unchanged.
- **Outer Caller (`proxy_gc_once`):** In `proxy_gc_once` ([`src/supervisor.rs:830`](src/supervisor.rs#L830)), `compute_protected_blobs(storage, repo_rules, proxy).await?` propagates the error immediately via `?`.
- **Pre-Scan Abort:** Because `proxy_gc_once` aborts at line 830, candidate scanning over `fs_root/blobs/sha256`, candidate sorting, and `tracing::info!` logging are entirely skipped.
- **Supervisor Task Loop:** `spawn_proxy_gc` maps the error to `proxy gc failed: {e}`, and `TaskSupervisor::spawn_loop` logs the periodic iteration error at `warn` level before the next interval tick.

### 2.5 Clarification on Current `proxy_gc_once` Behavior
`proxy_gc_once` currently scans candidate blob files, calculates non-protected entries against `max_cache_bytes`, tallies `evicted_records`, and emits an informational log. It does **not** persistently unlink files, delete directories, or modify databases; physical CAS reclamation is managed exclusively by `BlobGcService`. This change prevents inaccurate candidate counting and faulty protection sets, but does **not** claim to prevent an existing physical deletion operation.

### 2.6 Retained Limitation: `resolve_tag` Error Suppression
The subsequent tag resolution loop:
```rust
            for tag in pinned_tags {
                if let Ok(digest) = storage.resolve_tag(&repo, &tag).await {
                    collect_protected_blobs_for_manifest(
```
remains unchanged. Failures during `resolve_tag` continue to be silently swallowed in this slice and are documented as a remaining limitation.

### 2.7 Invariant Preservations
The following subsystems and configurations remain completely unmodified:
1. **Membership Sweep:** Reference-check behavior at both Site 1 (`gc_service.rs:611`, `?` abort) and Site 2 (`gc_service.rs:654`, `Err(_) => true` continuation) is unchanged.
2. **Lifecycle & Migration:** Lifecycle error propagation and membership migration planning/application/verification are unchanged.
3. **Index Rebuilding & Refresh:** `rebuild`, `sync_repo_manifests_and_tags`, and `refresh_tag_rooted_conservative` are unchanged.
4. **Storage Routing:** Production filesystem storage routing remains legacy (`FsStorage::list_tags`); descriptor-relative `contained_list_tags_seam` remains test-only.
5. **Configuration & APIs:** Zero public API, dependency, or configuration changes.

---

## 3. Test Verification & Coverage

### 3.1 Focused Test Suite
Six focused test cases were added to `mod tests` in [`src/supervisor.rs`](src/supervisor.rs):

1. **`test_compute_protected_blobs_propagates_representative_listing_errors`:**
   Injects representative `StorageErrorKind::Io`, `StorageErrorKind::Backend` (the contained directory limit mapping), and `StorageErrorKind::PermissionDenied` failures on `list_tags`. Verifies that `compute_protected_blobs` returns `Err(msg)` containing the repository context and exact error details.
2. **`test_compute_protected_blobs_not_found_treated_as_empty_tags`:**
   Injects `StorageError::NotFound` via mock override on a catalogued repository while a valid repository succeeds. Verifies that `compute_protected_blobs` returns `Ok(...)`. Asserts that the valid repository's referenced layer blob digest is protected and that the root manifest digest itself is excluded from `protected_blobs`. The test checks the layer digest and excludes the root manifest digest; it does not independently assert config-digest inclusion or distinguish contributions from two repositories sharing the same manifest.
3. **`test_compute_protected_blobs_successful_semver_selection_and_contents`:**
   Sets up repositories with SemVer tags `v1.0.0`, `v1.2.0`, and `v1.3.0-rc1`. Verifies that without prerelease `v1.2.0` is selected, and with prerelease `v1.3.0-rc1` is selected, correctly protecting the respective blob digests.
4. **`test_compute_protected_blobs_succeeds_after_fault_clearance`:**
   Injects an I/O fault on the first invocation (asserting `Err`), clears the fault, and verifies that the second invocation returns `Ok(...)` with full protection.
5. **`test_compute_protected_blobs_resolve_tag_failure_suppression_characterized`:**
   Succeeds on `list_tags` but fails on `resolve_tag`. Verifies that `compute_protected_blobs` returns `Ok(...)` with an empty protected set, pinning and characterizing the retained limitation.
6. **`test_proxy_gc_once_propagates_listing_error_before_candidate_scan`:**
   Injects a listing error and invokes `proxy_gc_once` against a nonexistent `blobs/sha256` path. Because `proxy_gc_once` aborts at line 830, it returns `Err(msg)` before scanning (had scanning been reached, line 838 would have returned `Ok(())`). Direct assertions verify the returned `Result::Err`, with zero assertions on outer supervisor logging and zero sleeps.

### 3.2 Regression & Total Final Verification Coverage
Executed the final verification suite yielding 64 distinct passing tests:
- Supervisor library test filter (`cargo test --lib supervisor::tests`): 21 passed (including all 6 new focused tests).
- Supervisor and command integration suite (`cargo test --test supervisor_and_command_tests`): 42 passed, 0 failed.
- Proxy GC lifecycle regression (`cargo test --test manifest_lifecycle_tests test_shared_content_same_repo_client_and_proxy_gc_safety`): 1 passed, 0 failed.
- Total: 64 distinct passing tests across these packaged final runs.
Preserved development failures remain recorded separately in `logs/dev/`.

---

## 4. Canonical Quality Gate Status

All eight canonical quality gates remain explicitly **OPEN**:
- `O-03`: Key and continuation-token contracts.
- `O-04`: Filesystem write durability and containment.
- `O-05`: Broader filesystem read containment.
- `O-06`: Typed AWS mapping and pinned-MinIO evidence.
- `O-13`: Hosting, distribution, and release strategy.
- `O-15`: Non-Linux verification.
- `O-16`: Earlier Slice 11 audit/test-inventory evidence.
- `D-06`: Broader extraction, cutover, compatibility, and distribution acceptance.
