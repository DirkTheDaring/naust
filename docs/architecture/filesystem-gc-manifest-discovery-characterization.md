# Filesystem GC Manifest Discovery Characterization

## Status
- **Date**: 2026-09-12
- **State**: Characterization Complete — Production Code Unchanged — Ready for Review — Not Committed
- **Target Subsystem**: Garbage Collection Manifest-Rooted Reachability Discovery (`src/blob_gc/policy.rs`)

---

## 1. Production Call Chain Trace & Caller Excerpts

### 1.1 Overview & Call Chain Architecture
Garbage collection protects blobs from premature deletion using two primary policy modes:
1. `BlobGcPolicy::TagRooted`: Evaluates reachability exclusively through tagged manifests and DAG edges stored in `BlobRefIndex`.
2. `BlobGcPolicy::ManifestRooted` (Default): Evaluates reachability from both tagged and untagged manifests stored in the repository namespace.

Under `BlobGcPolicy::ManifestRooted`, `PolicyContext::build` constructs an in-memory set of protected digest strings (`HashSet<String>`) by invoking `build_manifest_protected_set(cfg, storage)`:

```rust
// File: src/blob_gc/policy.rs, lines 116-137
impl PolicyContext {
    pub async fn build(
        cfg: &crate::config::Config,
        storage: &(impl storage::GcServiceStoragePort + ?Sized),
        idx: &BlobRefIndex,
        policy: BlobGcPolicy,
    ) -> Result<Self, GcPolicyError> {
        idx.check_health()?;

        let idx = Arc::new(idx.clone());

        let manifest_protected = match policy {
            BlobGcPolicy::TagRooted => None,
            BlobGcPolicy::ManifestRooted => Some(build_manifest_protected_set(cfg, storage).await?),
        };

        Ok(Self {
            policy,
            idx,
            manifest_protected,
        })
    }
```

---

### 1.2 Consumer Excerpts and Scoping of Mutation Halts

#### A. Planning Caller: `blob_gc_plan`
In `blob_gc_plan`, `PolicyContext::build` is executed immediately before candidate enumeration:

```rust
// File: src/blob_gc/mod.rs, lines 130-145
pub async fn blob_gc_plan(
    cfg: &crate::config::Config,
    storage: &Arc<dyn storage::GcServiceStoragePort>,
    idx: &BlobRefIndex,
    policy: BlobGcPolicy,
    min_age: Duration,
    limits: BlobGcLimits,
) -> Result<BlobGcStats, BlobGcError> {
    let mut policy_ctx = PolicyContext::build(cfg, storage, idx, policy).await?;
    let mut stats = BlobGcStats::default();

    let t0 = Instant::now();
    let now = SystemTime::now();

    let mut traverser = CasBlobTraverser::new(storage, 100);
```
- **Scope**: In `blob_gc_plan`, a discovery error returns `Err(BlobGcError::Policy(err))` before any CAS candidate is evaluated or scanned. Zero mutations occur because planning is strictly read-only.

#### B. Quarantine Caller: `blob_gc_quarantine_with_authority`
In `blob_gc_quarantine_with_authority`, `PolicyContext::build` is called **inside the candidate loop** under revalidation locking:

```rust
// File: src/blob_gc/mod.rs, lines 257-295
            match check_candidate_age(candidate.last_modified, now, min_age) {
                AgeEligibility::Eligible => {}
                _ => continue,
            }

            stats.scanned_blobs += 1;
            stats.scanned_bytes = stats.scanned_bytes.saturating_add(candidate.size);

            if !authority.is_active() {
                return Err(BlobGcError::AuthorityReleased);
            }
            let permit = authority.gc_mutation_permit();

            let _reval_guard = consistency.acquire_gc_revalidation().await;

            let mut policy_ctx = PolicyContext::build(cfg, storage, idx, policy).await?;

            if policy_ctx.is_pinned(&candidate.digest, now)? {
                drop(_reval_guard);
                continue;
            }

            if policy_ctx.is_referenced(&candidate.digest).await? {
                drop(_reval_guard);
                continue;
            }

            let q_res = storage
                .quarantine_blob(&permit, &candidate.digest, &candidate.version)
                .await;
```
- **Preceding Work**: Before `PolicyContext::build` is evaluated for candidate $N$:
  * Authority status is checked (`authority.is_active()`).
  * Candidate eligibility age is validated (`check_candidate_age`).
  * `stats.scanned_blobs` and `scanned_bytes` are incremented.
  * The revalidation guard `consistency.acquire_gc_revalidation()` is acquired.
  * Candidates $1 \dots (N-1)$ from earlier iterations or batches may have **already been quarantined** in storage.
- **Scope of Halt**: A failure in `PolicyContext::build` returns `Err(BlobGcError::Policy(err))` and drops `_reval_guard`. It prevents candidate $N$ and subsequent candidates from being quarantined, but does not revert previous quarantine operations.

#### C. Deletion Caller: `blob_gc_delete_with_authority`
In `blob_gc_delete_with_authority`, `PolicyContext::build` is also called **inside the candidate loop**:

```rust
// File: src/blob_gc/mod.rs, lines 490-530
            let q_at = match read_quarantine_time(cfg, &digest).await? {
                Some(t) => t,
                None => {
                    let _ = write_quarantine_time(cfg, &digest, now).await;
                    continue;
                }
            };

            match check_candidate_age(q_at, now, quarantine_delay) {
                AgeEligibility::Eligible => {}
                _ => continue,
            }

            if stats.deleted_bytes >= limits.max_bytes {
                continue;
            }

            if !authority.is_active() {
                return Err(BlobGcError::AuthorityReleased);
            }
            let permit = authority.gc_mutation_permit();

            let reval_guard = consistency.acquire_gc_revalidation().await;

            let mut policy_ctx = PolicyContext::build(cfg, storage, idx, policy).await?;
```
- **Preceding Work**: Before `PolicyContext::build` is evaluated:
  * If `read_quarantine_time` returns `None`, `write_quarantine_time` **executes a metadata write** to record the quarantine timestamp.
  * Candidate quarantine aging (`check_candidate_age`) and authority validity are checked.
  * `reval_guard` is acquired.
  * Any previous candidates in the loop may have already been deleted or restored.
- **Scope of Halt**: A discovery error halts deletion for candidate $N$ and drops `reval_guard`, returning `Err(BlobGcError::Policy(err))`. Prior deletions and prior quarantine timestamp writes remain in effect.

---

### 1.3 Verbatim Branch Selection & Root Ownership
In `build_manifest_protected_set`, the branch condition determines whether to use the filesystem bypass walker or the storage capability port:

```rust
// File: src/blob_gc/policy.rs, lines 166-178
pub async fn build_manifest_protected_set(
    cfg: &crate::config::Config,
    storage: &(impl storage::GcServiceStoragePort + ?Sized),
) -> Result<HashSet<String>, GcPolicyError> {
    if storage.kind() == "fs"
        && tokio::fs::metadata(&cfg.fs_root.join("repos"))
            .await
            .is_ok()
    {
        return build_manifest_protected_set_fs(&cfg.fs_root).await;
    }

    let repos = storage
        .list_repositories()
        .await
        .map_err(GcPolicyError::ListRepositories)?;
```

#### Exact Branch Semantics
1. **`storage.kind() == "fs"`**: Verifies that the storage instance identifies as filesystem storage. For S3 storage (`kind() == "s3"`), this check is false and the bypass is never taken.
2. **`metadata(&cfg.fs_root.join("repos")).await.is_ok()`**:
   * Calls `tokio::fs::metadata` on the path `cfg.fs_root.join("repos")`.
   * **Not simply "path exists"**: If `metadata` returns `Ok(_)`, `is_ok()` is true. However, `metadata` succeeding does not verify that `repos` is actually a directory (it could be a regular file, FIFO, socket, or symlink).
   * **Fallback on Any Metadata Error**: If `metadata` returns `Err(_)` (including `NotFound`, `PermissionDenied`, or I/O failure), `is_ok()` is false, causing silent fallback to the storage-port path (`storage.list_repositories()`).
3. **Root Ownership Divergence**: `storage` encapsulates its own storage root (e.g. `FsStorage.root`). However, `build_manifest_protected_set` inspects `cfg.fs_root` directly from configuration. If `cfg.fs_root` diverges from `storage.root`, discovery evaluates `cfg.fs_root`.

---

### 1.4 Verbatim Filesystem Traversal & Manifest Recognition
```rust
// File: src/blob_gc/policy.rs, lines 227-285
pub async fn build_manifest_protected_set_fs(
    fs_root: &Path,
) -> Result<HashSet<String>, GcPolicyError> {
    let repos_root = fs_root.join("repos");
    let mut stack: Vec<PathBuf> = vec![repos_root];
    let mut protected: HashSet<String> = HashSet::new();

    while let Some(dir) = stack.pop() {
        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(GcPolicyError::FsReadDir { path: dir, source }),
        };

        while let Ok(Some(ent)) = rd.next_entry().await {
            let ft = match ent.file_type().await {
                Ok(t) => t,
                Err(_) => continue,
            };
            let path = ent.path();

            if ft.is_dir() {
                let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if name == "manifests" {
                    let mut md = match tokio::fs::read_dir(&path).await {
                        Ok(d) => d,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(source) => {
                            return Err(GcPolicyError::FsReadDir { path, source });
                        }
                    };
                    while let Ok(Some(m)) = md.next_entry().await {
                        let mft = match m.file_type().await {
                            Ok(t) => t,
                            Err(_) => continue,
                        };
                        if !mft.is_file() {
                            continue;
                        }
                        let mp = m.path();
                        let hex = match mp.file_name().and_then(|s| s.to_str()) {
                            Some(s) => s,
                            None => continue,
                        };
                        if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                            continue;
                        }
                        let digest = format!("sha256:{hex}");
                        protected.insert(digest.clone());

                        let bytes = match tokio::fs::read(&mp).await {
                            Ok(b) => b,
                            Err(source) => {
                                return Err(GcPolicyError::FsReadManifest { path: mp, source });
                            }
                        };
                        let refs = parse_manifest_refs(&bytes).map_err(|source| {
                            GcPolicyError::ParseManifest {
                                repository: "fs".to_string(),
                                digest: digest.clone(),
                                source,
                            }
                        })?;
                        for r in refs.all_references() {
                            protected.insert(r.as_str().to_string());
                        }
                    }
                } else {
                    stack.push(path);
                }
                continue;
            }
        }
    }

    Ok(protected)
}
```

#### Traversal Mechanics & Partial Results Risk
- **Silent Loop Termination**: Both `while let Ok(Some(ent)) = rd.next_entry().await` and `while let Ok(Some(m)) = md.next_entry().await` evaluate `next_entry().await`. If `next_entry()` returns `Err(_)`, the `while let Ok(Some(...))` pattern fails to match, and the loop terminates **silently without returning an error**. If an I/O error occurs mid-directory, the walker may return a partial `protected` set rather than failing closed.
- **Symlinks**:
  * Directory entries: `ent.file_type().await` returns symlink file types without following them. `ft.is_dir()` returns `false` for symlinked directories, so symlinked repository subdirectories are skipped.
  * Manifest entries: `m.file_type().await` reports `mft.is_file() == false` for symlinked manifest files, so symlinked manifest files are skipped.
  * Root path: `tokio::fs::read_dir(&repos_root)` resolves symlinks at the path target, so an initial symlinked `repos` root is traversed.
- **SHA-512 Omission**: `hex.len() != 64` silently skips 128-hex SHA-512 manifest filenames (`continue`). Neither the manifest digest nor its referenced config/layer blobs are added to `protected`.
- **Case Handling**: `c.is_ascii_hexdigit()` accepts uppercase hex (`A`-`F`). Uppercase names are inserted as uppercase strings (`"sha256:AAAA..."`), diverging from canonical lowercase CAS digests.
- **Error Granularity**: Parse errors record `"fs"` as the repository name (`repository: "fs".to_string()`), obscuring the actual repository name.

---

### 1.5 Trace of `FsStorage::list_repositories` and `list_repo_names`

When the bypass is not taken, `build_manifest_protected_set` calls `storage.list_repositories()`. In `FsStorage`, this delegates to `list_repo_names`:

```rust
// File: src/storage/fs.rs, lines 773-775
    async fn list_repositories(&self) -> Result<Vec<String>, StorageError> {
        self.list_repo_names().await
    }
```

#### Verbatim `list_repo_names` Implementation
```rust
// File: src/storage/fs.rs, lines 566-641
    async fn list_repo_names(&self) -> Result<Vec<String>, StorageError> {
        let repos_root = self.root.join("repos");
        let mut repos = Vec::new();

        let mut stack: Vec<(PathBuf, String)> = vec![(repos_root.clone(), String::new())];
        while let Some((dir_path, rel)) = stack.pop() {
            let mut dir = match tokio::fs::read_dir(&dir_path).await {
                Ok(d) => d,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(StorageError::io(err.to_string())),
            };

            while let Ok(Some(entry)) = dir.next_entry().await {
                let file_type = match entry.file_type().await {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                if !file_type.is_dir() {
                    continue;
                }

                let name = match entry.file_name().to_str() {
                    Some(s) => s.to_string(),
                    None => continue,
                };

                // Do not descend into internal leaf dirs.
                if name == "tags"
                    || name == "manifests"
                    || name == "referrers"
                    || name == "blobs"
                    || name == "meta"
                {
                    continue;
                }

                let child_path = entry.path();
                let child_rel = if rel.is_empty() {
                    name
                } else {
                    format!("{rel}/{name}")
                };

                // Consider this a repo if it has tags/, manifests/, blobs/, or meta/ directories.
                let tags_dir = child_path.join("tags");
                let manifests_dir = child_path.join("manifests");
                let blobs_dir = child_path.join("blobs");
                let meta_dir = child_path.join("meta");
                let has_tags = tokio::fs::metadata(&tags_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                let has_manifests = tokio::fs::metadata(&manifests_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                let has_blobs = tokio::fs::metadata(&blobs_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                let has_meta = tokio::fs::metadata(&meta_dir)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                if has_tags || has_manifests || has_blobs || has_meta {
                    repos.push(child_rel.clone());
                }

                stack.push((child_path, child_rel));
            }
        }

        repos.sort();
        repos.dedup();
        Ok(repos)
    }
```

#### Repository Discovery Analysis
1. **Path Ownership**: `list_repo_names` uses raw pathname concatenation (`self.root.join("repos")`) and `tokio::fs::read_dir(&dir_path)`. It does **not** use pinned directory descriptors (`dirfd`).
2. **Filtering & Recognition**: Subdirectories named `tags`, `manifests`, `referrers`, `blobs`, or `meta` are excluded from recursion. A directory is recognized as a repository if and only if `tags`, `manifests`, `blobs`, or `meta` subdirectories exist within it.
3. **Error Suppression**: `NotFound` on subdirectories is silently ignored (`continue`). If `dir.next_entry().await` returns an error, the `while let Ok(Some(entry))` loop silently terminates, risking partial repository discovery.
4. **Architectural Gap**: Routing `build_manifest_protected_set` through the storage-port path calls `storage.list_repositories()`, which invokes `list_repo_names`. Because `list_repo_names` is itself an uncontained pathname walker, **switching to the storage port does NOT place repository discovery under a pinned descriptor**. Only the subsequent per-repository call `storage.list_manifest_digests_page(repo)` uses the contained reader.

---

### 1.6 Deletion Safeguards Independent of Manifest Discovery (Scoped Observations)

> [!IMPORTANT]
> **Omission from `manifest_protected` does not equate to physical deletion.**
> The characterization establishes protected-set observations, not universal GC safety. Within the inspected execution paths in `src/blob_gc/mod.rs` and `src/blob_gc/policy.rs`, multiple distinct, layered safety checks are evaluated before any candidate blob is purged:

1. **Ref-Index Reachability (`BlobRefIndex::is_blob_referenced`)**:
   Under the inspected `policy_ctx.is_referenced(&candidate.digest)` check, reachability is queried against `BlobRefIndex`. If this returns `true`, candidate quarantine and deletion are skipped.
   * **Scoped observation on untagged SHA-512 manifests**: `is_blob_referenced` evaluates root counts and DAG edges stored in sled. In the inspected index synchronization path (`sync_repo_manifests_and_tags`), all manifests (including untagged manifests) are ingested as roots and edges. Therefore, an untagged SHA-512 manifest may already be indexed as a root, protecting its referenced blobs via `BlobRefIndex` even if omitted from `manifest_protected`.
2. **Active Operation Pins (`BlobRefIndex::is_blob_pinned`)**:
   Within the inspected quarantine and deletion execution paths, pins are evaluated under the revalidation lock via `policy_ctx.is_pinned(&candidate.digest, now)?`. If this check returns `true`, the candidate is skipped for that cycle.
3. **Repository Blob Membership Ledger (`count_repo_blob_memberships`)**:
   In `blob_gc_plan`, the repository-scoped membership ledger is checked (`storage.count_repo_blob_memberships(&candidate.digest)`). If `mem_count > 0`, the candidate is skipped.
4. **Age and Grace Period Validation (`check_candidate_age`)**:
   In both `blob_gc_quarantine_with_authority` and `blob_gc_delete_with_authority`, blobs whose elapsed age is less than the configured minimum age or quarantine delay are categorized as ineligible and skipped.
5. **Two-Stage Quarantine Delay (`blob_gc_default_quarantine_delay_secs`)**:
   In the inspected filesystem GC flow, candidate blobs are never deleted directly from CAS; they are moved to quarantine and evaluated again after the quarantine delay. Quarantined blobs that become referenced before deletion are restored to CAS (`restore_quarantined_blob`).
6. **Revalidation Under Consistency Guard (`acquire_gc_revalidation`)**:
   In the inspected quarantine and deletion paths, mutations acquire an exclusive revalidation guard on `ConsistencyCoordinator`, ensuring pins, references, and journals are evaluated under the guard.
7. **Runtime Mutation Authority (`RuntimeMutationAuthority`)**:
   In the inspected mutation paths, `authority.is_active()` and `authority.gc_mutation_permit()` are verified; if authority is inactive, mutations abort immediately with `Err(BlobGcError::AuthorityReleased)`.

---

## 2. Characterization Test Results

The behavior was characterized through 14 focused tests in `src/blob_gc/policy.rs`:
- **12 normally enabled tests** (5 portable, 3 Unix-gated, 4 Linux-gated).
- **2 explicitly ignored permission tests** (Unix-gated, requiring effective unprivileged execution where `mode 000` denies access).

| Test Name | Platform Gating | Tested Behavior / Condition | Observed Result | Status |
| :--- | :--- | :--- | :--- | :--- |
| `test_gc_manifest_discovery_branch_selection_fs_vs_storage_port` | `target_os = "linux"` | Evaluates branch condition for `FsStorage` with existing repos, missing repos fallback, and S3 mock backend. | Selects fs bypass when `kind == "fs"` and `repos` exists; falls back to `storage.list_repositories()` when `repos` missing; routes through storage port for S3 backend. | **PASS** |
| `test_gc_manifest_discovery_missing_and_empty_repository_trees` | Portable | Tests missing `repos/`, empty `repos/`, repo without `manifests/`, and empty `manifests/`. | Returns `Ok(empty_set)` across all 4 missing/empty variations without error. | **PASS** |
| `test_gc_manifest_discovery_sha256_canonical_vs_sha512_skipped` | Portable | Tests 64-hex SHA-256 vs 128-hex SHA-512 manifest filenames. | SHA-256 manifest and referenced config/layer blobs protected. SHA-512 manifest **skipped completely** (`hex.len() != 64`). | **PASS** |
| `test_gc_manifest_discovery_name_filtering` | `unix` | Tests prefixed (`sha256:<hex>`), uppercase hex, temporary (`.tmp`), malformed (`zz`), and non-UTF-8 filenames. | Prefixed, temporary, malformed, and non-UTF-8 files skipped. Uppercase hex accepted as-is (diverges from canonical lowercase digest). | **PASS** |
| `test_gc_manifest_discovery_nested_repository_paths` | Portable | Tests deeply nested repository (`repos/a/b/c/manifests`) and root-adjacent repository (`repos/manifests`). | Traversal resolves `"manifests"` directory at arbitrary depths and protects manifests/blobs. | **PASS** |
| `test_gc_manifest_discovery_symlink_entries_skipped` | `unix` | Tests symlinked directory entry under `repos/` and symlinked manifest file under `manifests/`. | Both directory and file symlinks skipped (`ft.is_dir()` and `mft.is_file()` are false). | **PASS** |
| `test_gc_manifest_discovery_symlinked_initial_root_traversed` | `unix` | Tests `cfg.fs_root.join("repos")` where the root itself is a symlink. | Root symlink resolved by `read_dir`, traversing target directory cleanly. | **PASS** |
| `test_gc_manifest_discovery_entry_type_filtering` | Portable | Tests regular file directly in `repos/` and subdirectory inside `manifests/`. | Non-directory in `repos/` and non-file in `manifests/` skipped without error. | **PASS** |
| `test_gc_manifest_discovery_read_parse_errors_fail_closed` | Portable | Tests corrupted JSON and invalid descriptor digest structure (portable parse tests). | Corrupt JSON and invalid digest return `GcPolicyError::ParseManifest`. Both fail closed without permission dependence. | **PASS** |
| `test_gc_manifest_discovery_unreadable_manifest_permission_denied_ignored` | `unix` (Ignored) | Dedicated unreadable manifest file test (`mode 000`) capturing original `Permissions`, with explicit restoration returning `Result` and fallback Drop. | Asserted `GcPolicyError::FsReadManifest` with underlying `ErrorKind::PermissionDenied`. Guard restored permissions cleanly. | **PASS (Ignored)** |
| `test_gc_manifest_discovery_unreadable_directory_permission_denied_ignored` | `unix` (Ignored) | Dedicated unreadable directory test (`mode 000`) capturing original `Permissions`, with explicit restoration returning `Result` and fallback Drop. | Asserted `GcPolicyError::FsReadDir` with underlying `ErrorKind::PermissionDenied`. Guard restored permissions cleanly. | **PASS (Ignored)** |
| `test_gc_manifest_discovery_ignores_configured_manifest_listing_budgets` | `target_os = "linux"` | Compares `FsStorage` with restrictive `DirEnumerationLimits::new(1, 100_000)` against fs bypass. | Contained reader fails with `enumeration resource limit exceeded: MaxEntries(1)`; fs bypass walker completely ignores limits and returns all 15 entries. | **PASS** |
| `test_gc_manifest_discovery_pathname_divergence_from_pinned_storage` | `target_os = "linux"` | Storage initialized with `fs_root_a`; `cfg.fs_root` configured with distinct root `fs_root_b`. | Manifests discovered from `cfg.fs_root` (`fs_root_b`), not `storage` (`fs_root_a`). When `fs_root_b/repos` removed, falls back to `storage` (`fs_root_a`). | **PASS** |
| `test_gc_manifest_discovery_policy_context_fails_closed_on_discovery_error` | `target_os = "linux"` | `PolicyContext::build` with corrupt manifest under `ManifestRooted` vs `TagRooted`. | `ManifestRooted` fails closed (`Err(ParseManifest)`); `TagRooted` succeeds because it does not invoke manifest discovery. | **PASS** |

---

### 3. Separation of Observations from Proposed Changes

| Dimension | Source Location & Actual Condition | Observed Result (Characterization) | Effect on Protected Set | Caller Error Behavior | Remaining Uncertainty |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Branch Selection** | `src/blob_gc/policy.rs:168-175`<br>`storage.kind() == "fs" && metadata(&cfg.fs_root.join("repos")).is_ok()` | Direct filesystem bypass selected based on `cfg.fs_root` path check on host, not `storage`. | Bypasses `storage.list_repositories()` entirely. | Fails closed if bypass errors; falls back to storage port if `repos` metadata fails. | Whether any production environments intentionally configure `cfg.fs_root` differently from storage root. |
| **SHA-512 Support** | `src/blob_gc/policy.rs:260`<br>`if hex.len() != 64 \|\| !hex.chars().all(...) { continue; }` | 128-hex SHA-512 manifest filenames are silently skipped. | Untagged SHA-512 manifests and referenced blobs are **omitted** from protected set. | Silent skip (`continue`). Caller receives `Ok(protected)` missing SHA-512 data. | Untagged SHA-512 manifests may still have DAG roots and edges in `BlobRefIndex` if ingested during sync/rebuild. |
| **Resource Limits & Budgets** | `src/blob_gc/policy.rs:232-285`<br>Unbounded `read_dir` loop with no limits. | Configured `DirEnumerationLimits` on `FsStorage` are completely ignored. | All manifests discovered in a single unbounded pass regardless of size or depth. | Silent bypass; no limit check performed. | Risk of unbounded memory allocation or threadpool starvation on massive registries. |
| **Loop Error Handling** | `src/blob_gc/policy.rs:238, 253`<br>`while let Ok(Some(...)) = rd/md.next_entry().await` | Directory iteration silently terminates if `next_entry()` returns `Err`. | Loop terminates early; partial results returned as `Ok(protected)`. | Caller proceeds with truncated protected set. | Likelihood of transient filesystem errors causing partial protected sets. |
| **Error Granularity** | `src/blob_gc/policy.rs:271-277`<br>`repository: "fs".to_string()` | Hardcoded `"fs"` string emitted in `GcPolicyError::ParseManifest`. | Entire protected set construction fails closed. | Aborts `blob_gc_plan`, `quarantine`, and `delete`. | Logs lack repository identity, complicating operator troubleshooting. |

---

### 4. Assessment of Next Integration Step

#### Comparison: Existing Walker vs. Promoted Contained Reader
| Attribute | Existing GC FS Bypass (`build_manifest_protected_set_fs`) | Promoted Contained Reader (`storage::fs::manifest_listing`) |
| :--- | :--- | :--- |
| **Manifest Listing Containment** | Uncontained directory walk via raw `tokio::fs` paths. | Pinned to repository directory descriptor with `O_NOFOLLOW`. |
| **Repository Discovery** | Unbounded recursive search for `"manifests"` directories. | N/A (listing requires repository name; discovery uses `list_repo_names`). |
| **Manifest Formats** | SHA-256 only (64 hex characters). SHA-512 silently skipped. | Both SHA-256 (64 hex) and SHA-512 (128 hex) supported. |
| **Resource Budgets** | None. Unbounded directory entries and memory allocation. | Configured `DirEnumerationLimits` (entries and filename bytes). |
| **Pagination** | Single-pass in-memory accumulation into `HashSet<String>`. | Paginated cursor with continuation tokens. |
| **Error Context** | Hardcoded `"fs"` repository label in parse errors. | Accurate repository name preserved in all error variants. |

#### Concrete Compatibility Decisions & Open Repository-Discovery Gaps
1. **Repository Discovery Gap**: Replacing the bypass with the storage-port path (`storage.list_repositories()`) delegates to `FsStorage::list_repo_names`, which is **also an uncontained, raw pathname walker**. A contained GC manifest listing solution requires first defining a contained repository enumeration interface or evaluating repository names through an authoritative registry catalog.
2. **SHA-512 Discovery Parity**: Routing manifest listing through `storage.list_manifest_digests_page` will enable SHA-512 manifest discovery for GC, closing the bypass walker's blind spot.
3. **Budget Exhaustion & Error Policy**: If a repository's manifests directory exceeds configured enumeration limits, `FsStorage` returns `StorageError::Internal { kind: Backend, message: "enumeration resource limit exceeded: ..." }`. In GC, this will correctly fail closed, aborting the GC run rather than operating on a truncated protected set.
4. **Memory Footprint**: The existing walker loads all manifest references into a single massive `HashSet<String>`. For large registries, streaming or batching reachability checks against `BlobRefIndex` should be considered for long-term scalability.

#### Bounded Follow-Up Recommendation (Unapproved Design Input)
- **Status**: **Unapproved**. Must be submitted as a separate design slice.
- **Recommended Focus**:
  1. Define a contained repository discovery interface or integrate with authoritative catalog storage.
  2. Route manifest enumeration through `storage.list_manifest_digests_page(repo)` to gain contained descriptor traversal and SHA-512 support.
  3. Retire `build_manifest_protected_set_fs`.

---

## 5. Canonical Quality Gates (All Remain OPEN)

All eight canonical quality gates remain explicitly **OPEN**:
- **O-03**: Key and continuation-token contracts. (**OPEN**)
- **O-04**: Filesystem write durability and containment. (**OPEN**)
- **O-05**: Broader filesystem read containment. (**OPEN**)
- **O-06**: Typed AWS mapping and pinned-MinIO evidence. (**OPEN**)
- **O-13**: Hosting, distribution, and release strategy. (**OPEN**)
- **O-15**: Non-Linux verification. (**OPEN**)
- **O-16**: Earlier Slice 11 audit/test-inventory evidence. (**OPEN**)
- **D-06**: Broader extraction, cutover, compatibility, and distribution acceptance. (**OPEN**)
