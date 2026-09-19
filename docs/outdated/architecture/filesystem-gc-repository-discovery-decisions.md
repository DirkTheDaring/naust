> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. The decisions recorded PENDING here were implemented via the contained-discovery cutover (`2fc21aa` lineage).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Architecture Decision Record: Filesystem GC Discovery Test-Seam Contract

**Repository:** `registry-rust`
**Target Document:** `docs/architecture/filesystem-gc-repository-discovery-decisions.md`
**Authoritative Baselines:**
- `registry-rust` HEAD: `12533aedd00d5f2f6b76942a80a1dc40fbaf344d` (committed repository discovery characterization)
- `storage-layer-rust` HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`

**Scope:** Concrete, implementable test-only discovery contract for filesystem garbage collection manifest reachability.
**Status:** **DECISION PROPOSAL ONLY — NOT AUTHORIZED FOR PRODUCTION CUTOVER — NO COMMITS OR PUSHES — PRODUCTION CODE UNCHANGED**.
**Quality Gates:** **O-03, O-04, O-05, O-06, O-13, O-15, O-16, and D-06 remain explicitly OPEN**.

---

## 1. Executive Summary & Experimental Boundary

### 1.1 Incremental Extraction Trajectory
The filesystem storage extraction initiative decomposes storage operations into `storage-fs` primitives and integrates them into `registry-rust` through reviewed slices: characterization, architectural design, test-only seam integration, production cutover, and explicit user-authorized local commit.

Completed milestones prior to this proposal include:
1. **Descriptor-Relative Payload Reads:** Linux `openat2` resolution beneath pinned storage roots in `storage-fs` and integrated into `registry-rust`.
2. **Bounded Single-Directory Enumeration:** `storage_fs::DirEnumerationLimits` and `FsMetadataReader::enumerate_dir` enforcing descriptor containment (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`) over individual directories.
3. **Contained File Metadata Inspection:** `FsFileMetadata` inspection via descriptor-relative `fstat`.
4. **Registry Filesystem Cutover:** Blob payload reads, CAS listing, manifest reads, and manifest listing (`FsStorage::list_manifest_digests_page`, commit `acddfe7779b867de7c27c13ee3c77afedd36d11f`) routed through contained components.
5. **Lifecycle Reference Discovery Hardening:** Propagation of typed storage errors during lifecycle reference checks.
6. **Reference-Index Synchronization Hardening:** Staging storage discovery before applying `BlobRefIndex` mutations (commit `2f7fcc8b46c30c0008fc60ba15e15b0c1bf3eea3`).
7. **GC Manifest Discovery Characterization:** Empirical characterization of `build_manifest_protected_set` and `build_manifest_protected_set_fs` (commit `bfbcfc1d35ea12e96dcf5347cf0c4667a490b3b3`).
8. **Filesystem Repository Discovery Characterization:** Empirical characterization of `FsStorage::list_repositories` / `list_repo_names` vs. direct GC walker (commit `12533aedd00d5f2f6b76942a80a1dc40fbaf344d`).

### 1.2 The Production Dilemma & Scope of this Seam
In `registry-rust`, garbage collection reachability discovery currently has two divergent code paths in `src/blob_gc/policy.rs`:
- **Direct Filesystem Bypass Walker (`build_manifest_protected_set_fs`):** Activated whenever `storage.kind() == "fs"` and `tokio::fs::metadata(&cfg.fs_root.join("repos")).await.is_ok()`. This path traverses raw pathnames beneath `cfg.fs_root/repos` using uncontained `tokio::fs::read_dir`, scanning any directory named `manifests` as a non-recursive leaf. Removing this bypass is **NOT approved**.
- **Storage-Port Path:** Falls back to `storage.list_repositories().await` (implemented by `FsStorage::list_repo_names`) followed by per-repository manifest pagination via `storage.list_manifest_digests_page`. Contained repository discovery is **NOT implemented or approved**.

Characterization established that `FsStorage::list_repositories` and `build_manifest_protected_set_fs` discover materially different manifest sets:
- Direct GC scans root-adjacent manifests (`repos/manifests/<hex>`), whereas the catalog omits them.
- Direct GC discovers manifests beneath reserved directory names (`repos/tags/...`, `repos/blobs/...`, `repos/meta/...`, `repos/referrers/...`), whereas the catalog omits them.
- Direct GC traverses non-UTF-8 directory ancestors, whereas the catalog omits their entire subtrees.
- Direct GC ignores directory enumeration budgets, while contained manifest listing enforces per-directory limits.
- The catalog suppresses leaf recognition errors with `.unwrap_or(false)`, whereas contained readers return typed errors.
- The catalog can yield repository names with backslashes, which downstream contained manifest listing rejects with `StorageError::InvalidRepoName`.

### 1.3 Recommended Experimental Scope: Discovering Manifest-Directory ObjectKeys
Rather than forcing discovery into the public repository catalog contract (`Vec<String>` repository names with an ad-hoc boolean for root manifests), this proposal defines a single, implementable test-only discovery contract:
- **Core Output:** The seam discovers and returns a collection of representable **manifest-directory `ObjectKey`s** beneath the pinned root descriptor:
  - `repos/library/ubuntu/manifests`
  - `repos/tags/sub1/sub2/manifests`
  - `repos/manifests`
- **Strictly Bounded Seam:** This seam discovers manifest directories only. Manifest payload reading, canonical digest validation, protected-set construction, and production routing cutover are **explicitly deferred** to subsequent slices.

---

## 2. Established Source Realities & Caller Architecture

### 2.1 GC Call Chains and Mutation Halting Scopes

Discovery occurs in `PolicyContext::build` (`src/blob_gc/policy.rs:116-137`) under `BlobGcPolicy::ManifestRooted`. The impact of discovery errors varies by caller:

```
[GC Service Entry Points]
  ├── blob_gc_plan (src/blob_gc/mod.rs:129-150)
  │     └── PolicyContext::build (Once before candidate iteration)
  │           └── Discovery error: Aborts immediately; read-only; 0 mutations.
  │
  ├── blob_gc_quarantine_with_authority (src/blob_gc/mod.rs:257-295)
  │     └── Loop over CAS blob candidates:
  │           ├── check_candidate_age -> candidate N eligible
  │           ├── stats.scanned_blobs += 1
  │           ├── authority.gc_mutation_permit()
  │           ├── _reval_guard = consistency.acquire_gc_revalidation()
  │           ├── PolicyContext::build (Evaluated per candidate N)
  │           │     └── Discovery error: Halts candidate N and subsequent candidates;
  │           │           drops _reval_guard; returns Err.
  │           │           DOES NOT rollback earlier quarantined candidates 1..(N-1)!
  │           └── storage.quarantine_blob(...)
  │
  └── blob_gc_delete_with_authority (src/blob_gc/mod.rs:490-532)
        └── Loop over quarantine candidates:
              ├── read_quarantine_time -> if None: write_quarantine_time(now) [MUTATION!]
              ├── check_candidate_age -> candidate N eligible
              ├── authority.gc_mutation_permit()
              ├── reval_guard = consistency.acquire_gc_revalidation()
              ├── PolicyContext::build (Evaluated per candidate N)
              │     └── Discovery error: Halts candidate N; drops reval_guard; returns Err.
              │           DOES NOT undo prior candidate deletions, restorations, or
              │           written quarantine timestamps!
              └── storage.delete_quarantined_blob(...)
```

#### Key Caller Findings:
1. **Per-Candidate Revalidation:** In `blob_gc_quarantine_with_authority` and `blob_gc_delete_with_authority`, `PolicyContext::build` is invoked **inside the candidate loop** under the revalidation lock. A run with 1,000 candidates executes discovery up to 1,000 times to observe point-in-time state.
2. **No Global Transactional Rollback:** An error in repository discovery aborts work on candidate $N$, but mutations from earlier iterations (quarantined blobs, deleted blobs, restored blobs, written quarantine timestamp files) remain committed to storage.
3. **Index Health Pre-Check:** `PolicyContext::build` (`src/blob_gc/policy.rs:28`) executes `idx.check_health()?` before constructing the protected set. If the index is marked corrupt or unhealthy, discovery is not reached and GC aborts immediately.
4. **Multi-Layered Safeguards:** Omission of a manifest from `manifest_protected` does not directly cause physical deletion of referenced blobs. Physical deletion requires:
   - The candidate must not be referenced in `BlobRefIndex` (`policy_ctx.is_referenced`).
   - The candidate must not be pinned (`policy_ctx.is_pinned`).
   - The candidate must be older than `min_age` (for quarantine) or `quarantine_delay` (for deletion).
   - An active mutation permit must be held from `authority`.
   Omission from `manifest_protected` creates a *vulnerability to premature reclamation* if the reference index is unsynchronized or incomplete, but does not autonomously execute deletion.

### 2.2 Direct GC Walker (`build_manifest_protected_set_fs`)
```rust
// Mechanically Quoted Source: src/blob_gc/policy.rs:227-285
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

### 2.3 Storage-Port Traversal in `build_manifest_protected_set`
```rust
// Mechanically Quoted Source: src/blob_gc/policy.rs:166-225
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

    let mut protected = HashSet::new();
    for repo in repos {
        let mut cursor = None;
        loop {
            let (digests, next_cursor) = storage
                .list_manifest_digests_page(&repo, cursor.as_deref(), 100)
                .await
                .map_err(|source| GcPolicyError::ListManifests {
                    repository: repo.clone(),
                    source,
                })?;

            for digest in digests {
                protected.insert(digest.as_str().to_string());
                let (_meta, bytes) =
                    storage
                        .get_manifest(&repo, &digest)
                        .await
                        .map_err(|source| GcPolicyError::ReadManifest {
                            repository: repo.clone(),
                            digest: digest.to_string(),
                            source,
                        })?;
                let refs =
                    parse_manifest_refs(&bytes).map_err(|source| GcPolicyError::ParseManifest {
                        repository: repo.clone(),
                        digest: digest.to_string(),
                        source,
                    })?;
                for r in refs.all_references() {
                    protected.insert(r.as_str().to_string());
                }
            }

            if next_cursor.is_none() {
                break;
            }
            cursor = next_cursor;
        }
    }

    Ok(protected)
}
```

### 2.4 Repository Catalog Discovery (`list_repo_names`)
```rust
// Mechanically Quoted Source: src/storage/fs.rs:566-641
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

---

## 3. Concrete Decision Proposal: Section A — Discovery Ownership

### 3.1 Architectural Comparison

| Dimension | Option 1: Public Repository Catalog (`RepositoryCatalogReader::list_repositories`) | Option 2: Dedicated Registry-Owned GC Directory Discovery |
| :--- | :--- | :--- |
| **Primary Responsibility** | Serving external client queries for OCI Distribution `/v2/_catalog`. | Discovering reachable filesystem manifest directories to prevent destructive blob deletion. |
| **Visibility vs Reachability Contract** | Only enumerates canonical, client-visible repositories with recognized leaves (`tags`, `manifests`, `blobs`, `meta`). | Discovers all on-disk terminal `manifests/` directories beneath `repos/` to guarantee reachability completeness. |
| **Output Type** | `Result<Vec<String>, StorageError>` (user-facing repository names). Cannot represent root-adjacent manifests. | `Result<Vec<ObjectKey>, StorageError>` (exact manifest directory storage keys). Representable for all valid locations. |
| **Containment Guarantee** | Legacy uncontained `tokio::fs::read_dir` over host pathnames (`self.root.join("repos")`). Subject to root-replacement divergence against pinned reader descriptors. | Descriptor-relative single-directory enumeration via `storage_fs::FsMetadataReader` and Linux `openat2` (`RESOLVE_BENEATH \| RESOLVE_NO_SYMLINKS \| RESOLVE_NO_MAGICLINKS`). |
| **Domain Logic Location** | Coupled directly into generic storage port trait (`RepositoryCatalogReader`). | Encapsulated in `registry-rust` (`src/storage/fs/repo_discovery.rs`), preserving `storage-fs` as a domain-free, single-directory primitive. |
| **Downstream Validation Divergence** | The catalog does NOT validate all returned repository names. In test `test_repo_discovery_invalid_repo_name_aborts_downstream_manifest_listing`, downstream listing was exercised directly; GC storage-port propagation is a **source-only trace** (`src/blob_gc/policy.rs:187-193`). | Discovery validates path components up-front during `ObjectKey` composition, failing closed before returning. |

### 3.2 Analysis: Why Catalog Visibility Must Not Govern GC Reachability
1. **Presentation vs. Integrity:** `/v2/_catalog` is an external API. Repositories may legitimately exist on disk without being exposed in `/v2/_catalog` (e.g. referrers-only repositories containing artifact indices, or legacy unnamespaced roots). Using the catalog for GC reachability means any repository omitted from `/v2/_catalog` has its manifests excluded from `manifest_protected`.
2. **Root-Replacement Vulnerability:** Test `test_repo_discovery_root_replacement_vs_repos_replacement` demonstrated that if the filesystem directory backing `active_root` is replaced on disk:
   - `list_repositories()` (using `tokio::fs::read_dir`) reads the *new* filesystem directory path.
   - `FsMetadataReader` (pinned via root file descriptor) continues reading the *old* filesystem descriptor.
   - Using catalog discovery results in querying the pinned reader for repository names discovered from the new tree, which fail with `NotFound`, returning empty manifest lists.
3. **Decoupled Evolution:** Changing catalog discovery to discover non-standard paths would corrupt `/v2/_catalog` client responses. Conversely, tightening catalog validation would cause GC to omit unnamespaced directories.

### 3.3 Ownership Decision & Recommendation
- **Decision:** **Reject** Option 1 (relying on the public repository catalog as the sole source of GC discovery).
- **Recommendation:** **Adopt Option 2**: Implement a dedicated, registry-owned GC directory discovery component in `registry-rust` that drives recursive hierarchy traversal using existing descriptor-relative `storage-fs::FsMetadataReader::enumerate_dir` primitives, returning discovered manifest-directory `ObjectKey`s. Keep `storage-fs` strictly domain-free and single-directory.

---

## 4. Concrete Decision Proposal: Section B — Compatibility & Traversal Policy

### 4.1 Storage Layouts and Writer-Created Structures
To evaluate traversal rules accurately, actual storage layouts must be distinguished:
- **Global CAS Store (`fs_root/blobs/<algo>/<prefix>/<hex>`):** Stored directly beneath the storage root (`src/storage/fs.rs:447-454`), completely outside `repos/`.
- **Repository-Internal Subdirectories (`repos/<repo>/...`):** For an ordinary repository (e.g. `repos/library/ubuntu`), legitimate writer-created subdirectories include:
  - `repos/<repo>/tags/` (containing tag files or locks, `src/storage/fs.rs:932-940`)
  - `repos/<repo>/manifests/` (containing canonical manifest files, `src/storage/fs.rs:906-912`)
  - `repos/<repo>/blobs/` (repository-level blob link/membership markers, `src/storage/fs.rs:3380-3410`)
  - `repos/<repo>/meta/` (repository metadata or timestamps)
  - `repos/<repo>/referrers/` (OCI artifact referrer links)
- **Nested Hierarchies:** Multi-segment repositories (e.g. `org/team/project`) create intermediate path segments (`repos/org/team/project/manifests`).

### 4.2 Selected Traversal Strategy: Bounded Traversal
Characterization test `test_repo_discovery_reserved_leaf_names_excluded_and_gc_divergence` proved that the direct GC walker (`build_manifest_protected_set_fs`) descended through any directory whose name was not `"manifests"`, successfully discovering manifests inside nested subtrees such as `repos/tags/subrepo/manifests/<hex>`.

#### Removal of Alternative B.2 (Indiscriminate Subdirectory Rejection):
The alternative of rejecting any directory named `tags`, `blobs`, `meta`, or `referrers` that contains subdirectories is **rejected**. Writer-created structures, tag mutation locks, nested namespaces, or future sub-index directories can legitimately place subdirectories within repository trees. Arbitrarily rejecting them would cause false-positive GC failures on valid repositories.

#### Selected Seam Algorithm:
1. **Starting Point:** Begin enumeration at `repos/`.
2. **Directory Traversal:** When examining directory entries returned by `enumerate_dir`:
   - If an entry has name `"manifests"` and is a directory:
     - Record its composed `ObjectKey` (e.g. `repos/library/ubuntu/manifests`, `repos/tags/sub1/sub2/manifests`, `repos/manifests`) in the discovered manifest directories collection.
     - **Do NOT recurse inside it**. The directory named `manifests` is a terminal leaf.
   - If an entry is a directory and its name is **not** `"manifests"`:
     - Descend into the directory, pushing its path to the traversal queue/stack, regardless of whether its name is `tags`, `blobs`, `meta`, `referrers`, or an ordinary namespace segment.
3. **Stopping Conditions:**
   - Terminate when all queued directories have been enumerated.
   - Fail closed with `StorageError::backend(...)` if any provisional whole-walk limit (max depth, max directory enumerations, max total entries) is exceeded.

### 4.3 Path Representation vs. Repository Naming (Root-Adjacent Manifests)
- **Observed Divergence:** Direct GC scans `repos/manifests/<hex>` and protects manifest `...c1` and layer `...d1`. The catalog skips `manifests` at the root.
- **Path Representation Reality:**
  - `manifest_dir_key(repo)` (`src/storage/fs/manifest_listing.rs:96-101`) enforces `repo != ""` and rejects empty repository strings with `StorageError::InvalidRepoName`.
  - `ManifestReader::list_manifest_digests_page` takes `repo: &str`.
  - However, `storage_core::ObjectKey::parse("repos/manifests")` **is completely valid and representable**.
  - `FsMetadataReader::enumerate_dir(Some(&key), limits)` can directly enumerate `ObjectKey("repos/manifests")` beneath the pinned root descriptor without any public API change.
  - Similarly, payload reads can directly open `ObjectKey("repos/manifests/<hex>")`.
- **Trade-Off Analysis:**
  - *Preserving via Direct Contained Enumeration (Selected for Test Seam):* Enumerate `ObjectKey("repos/manifests")` directly. Preserves reachability for legacy root-level manifests without modifying public OCI repository APIs.
  - *Explicit Rejection (Alternative):* Check `ObjectKey("repos/manifests")`. If regular files exist, fail GC closed with `StorageError::corrupt_data("root-adjacent manifests detected without repository name")`. Prevents silent blob deletion and forces administrative intervention. Any migration tooling is a separate proposal, not an established prerequisite.
  - *Silent Omission:* **REJECTED** (creates vulnerability to premature blob deletion).

---

## 5. Concrete Decision Proposal: Section C — Failure Policy

The proposed test seam enforces a single, unambiguous policy for every failure condition:

| Condition | Proposed Test-Seam Policy | Exact Error Constructor / Return |
| :--- | :--- | :--- |
| **Initial `repos/` Missing** | Clean return of empty manifest directory set. | `Ok(Vec::new())` |
| **Directory Observed, Missing When Opened (TOCTOU)** | Explicit failure. If a directory entry was observed in a parent listing but fails with `NotFound` when opened, fail the entire walk. | `StorageError::io(format!("observed directory disappeared before enumeration: {key}"))` |
| **Symlink Directory Entries** | Skip `DirEntryType::Symlink` entries encountered during directory iteration, matching the direct GC walker. | (Entry skipped during iteration) |
| **Symlinks in Path Being Resolved** | Kernel `openat2` resolution failure under `RESOLVE_NO_SYMLINKS` (`ELOOP`) maps to typed I/O error. | `StorageError::io(source.to_string())` |
| **Regular / Non-Directory Entries** | Skip regular files, FIFOs, and sockets during hierarchy navigation. | (Entry skipped during iteration) |
| **Non-UTF-8 Directory Entry Names** | Explicit failure. Raw `OsString` names failing UTF-8 conversion fail discovery closed. | `StorageError::corrupt_data(format!("unrepresentable non-UTF-8 directory name: {name:?}"))` |
| **Unrepresentable Path Characters** | Directory names containing NUL bytes, leading/trailing slashes, or empty segments failing `ObjectKey::parse`. | `StorageError::corrupt_data(format!("directory name cannot form valid ObjectKey: {name:?}"))` |
| **Wrong-Type Initial `repos/`** | If `repos/` is a regular file instead of a directory, `enumerate_dir` returns `FsDirError::NotADirectory`. | `StorageError::corrupt_data("target path is not a directory: repos")` |
| **Enumeration / I/O Error** | Any underlying filesystem I/O error during directory reading aborts the walk immediately. | `StorageError::io(source.to_string())` |
| **Permission Denied** | EACCES/EPERM encountered during traversal aborts the walk immediately. | `StorageError::permission_denied(source.to_string())` |
| **Budget Limit Exhaustion** | Any single-directory or whole-walk budget exceeded aborts immediately without partial results. | `StorageError::backend(format!("discovery limit exceeded: {reason}"))` |

### 5.1 Disappearance After Observation vs. Missing Root
- A missing `repos/` root directory at the start of discovery is a normal empty-registry state and returns `Ok(Vec::new())`.
- In contrast, if a subdirectory is observed in a parent listing and disappears before it can be enumerated, this reflects concurrent mutation. The test seam treats this as an error (`StorageError::io`), ensuring that partial or inconsistent directory trees do not silently succeed.
- **Safety Limitation:** This strict error propagation halts on explicit filesystem errors, but does not establish point-in-time snapshot isolation across distinct system calls. Concurrent renames or unlinks during traversal remain an unavoidable characteristic of host filesystem iteration.

---

## 6. Concrete Decision Proposal: Section D — Resource Policy & Budget Accounting

### 6.1 Caller-Supplied, Test-Only Limits
Single-directory limits (`DEFAULT_MANIFEST_LISTING_MAX_ENTRIES = 10_000` and `DEFAULT_MANIFEST_LISTING_MAX_NAME_BYTES = 1_500_000`) are approved defaults for **manifest listing** (`repos/<repo>/manifests`), not approved whole-tree discovery settings.

The discovery test seam uses explicit, caller-supplied limits without production defaults:
```rust
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct DiscoveryTestLimits {
    /// Maximum directory depth relative to `repos/` (root `repos/` is depth 0).
    pub max_depth: usize,
    /// Maximum number of directory enumerations performed during the walk.
    pub max_dir_enumerations: usize,
    /// Maximum cumulative directory entries inspected across all enumerations.
    pub max_total_entries: usize,
    /// Maximum number of terminal manifest directory ObjectKeys retained.
    pub max_manifest_dirs: usize,
    /// Per-directory limits passed to each `enumerate_dir` call.
    pub per_dir_limits: storage_fs::DirEnumerationLimits,
}
```

### 6.2 Precise Accounting Rules & Boundary Checks
1. **Depth Accounting:**
   - Root `repos/` is depth 0. Each child pushed to the traversal stack has depth $D + 1$.
   - **Boundary Check:** Checked immediately before enqueueing a directory. If $D + 1 > 	ext{max\_depth}$, traversal fails with `StorageError::backend("max traversal depth exceeded")`.
   - **Inclusive Boundary:** A directory at depth equal to `max_depth` may be enumerated, but its subdirectories cannot be enqueued.
2. **Directory Enumeration Count:**
   - Incremented by 1 immediately prior to each `reader.enumerate_dir` call.
   - **Boundary Check:** If `dir_enumerations >= max_dir_enumerations`, fails with `StorageError::backend("max directory enumerations exceeded")`.
3. **Total Entries Examined:**
   - Incremented for each `DirEntry` returned in an enumeration batch.
   - **Boundary Check:** If `total_entries > max_total_entries`, fails with `StorageError::backend("max total entries examined exceeded")`.
4. **Retained Output Accounting:**
   - Incremented when a terminal `"manifests"` directory `ObjectKey` is recorded.
   - **Boundary Check:** If `retained.len() >= max_manifest_dirs`, fails with `StorageError::backend("max manifest directories limit exceeded")`.
5. **Terminal Directory Existence Verification:**
   - In the proposed seam, when a directory entry named `"manifests"` is observed, its `ObjectKey` is recorded directly from the parent directory listing without opening it.
   - **Residual Race:** Opening the terminal directory during discovery would consume extra directory enumerations without eliminating TOCTOU races: the directory can still be removed or unlinked before downstream manifest listing opens it.

### 6.3 Concurrency & Payload Deferrals
- **Revalidation Loop Deferral:** In `blob_gc_quarantine_with_authority` and `blob_gc_delete_with_authority`, `PolicyContext::build` is invoked inside the candidate loop under revalidation lock. Moving it outside changes revalidation freshness and is explicitly deferred.
- **Manifest Read Payload Ceiling:** Bounding manifest payload buffer sizes (`max_manifest_bytes`) in `get_manifest_impl` is an open issue that belongs in a dedicated manifest-read proposal, not in directory discovery.

---

## 7. Concrete Decision Proposal: Section E — Recommended Next Slice: Test Seam Contract

### 7.1 Component Architecture & Public Interface
```rust
// Proposed test seam: src/storage/fs/repo_discovery.rs

use async_trait::async_trait;
use storage_core::ObjectKey;
use storage_fs::{DirEntry, DirEnumerationLimits, FsDirError, FsMetadataReader};
use crate::storage::StorageError;

/// Narrow test abstraction for descriptor-relative directory enumeration,
/// enabling deterministic failure injection without private storage-fs hooks.
#[async_trait]
pub(crate) trait DiscoveryDirEnumerator: Send + Sync {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError>;
}

#[async_trait]
impl DiscoveryDirEnumerator for FsMetadataReader {
    async fn enumerate_dir(
        &self,
        target: Option<&ObjectKey>,
        limits: DirEnumerationLimits,
    ) -> Result<Vec<DirEntry>, FsDirError> {
        self.enumerate_dir(target, limits).await
    }
}

/// Discovers terminal manifest directory ObjectKeys beneath `repos/`.
pub(crate) async fn discover_manifest_dirs_impl(
    enumerator: &(impl DiscoveryDirEnumerator + ?Sized),
    limits: DiscoveryTestLimits,
) -> Result<Vec<ObjectKey>, StorageError> {
    // 1. Check repos/ existence via enumerate_dir(Some(&ObjectKey::parse("repos")?), ...).
    // 2. If NotFound, return Ok(Vec::new()).
    // 3. Perform bounded traversal using queue: Vec<(ObjectKey, usize)>.
    // 4. Enforce strict budget accounting and single-policy failure rules.
    // 5. Return Ok(retained_keys).
    todo!()
}
```

### 7.2 Required Test Matrix for the Seam
The test seam must empirically verify the following scenarios under `#[cfg(test)]`:
1. **Deep Reserved Ancestors:** Verify that `repos/tags/sub1/sub2/manifests` is discovered as a terminal `ObjectKey`.
2. **Root-Adjacent Manifests:** Verify that `repos/manifests` is discovered as a terminal `ObjectKey`.
3. **Terminal Leaf Invariance:** Verify that `repos/app/manifests/nested_dir` is NOT traversed.
4. **Ordinary Writer-Created Layouts:** Verify that standard repository structures (`tags/`, `manifests/`, `blobs/`, `meta/`, `referrers/`) are traversed cleanly without false-positive errors.
5. **Missing Root Directory:** Verify that missing `repos/` returns `Ok([])` with zero enumerations.
6. **Observed Disappearance (TOCTOU):** Inject `FsDirError::NotFound` on a child directory after parent observation; verify it returns `StorageError::Internal { kind: StorageErrorKind::Io, .. }`.
7. **Symlink Dirent Entries vs Path Symlinks:** Verify that symlink dirents inside `repos/` are skipped, while path symlinks fail with `StorageError::io(...)`.
8. **Non-UTF-8 Path Rejection:** Verify that non-UTF-8 directory entries fail closed with `StorageError::Internal { kind: StorageErrorKind::CorruptData, .. }`.
9. **Budget Boundaries & Atomic Failure:** Verify that exceeding `max_depth`, `max_dir_enumerations`, or `max_total_entries` fails closed without returning partial results from earlier discoveries.

---

## 8. Concrete Decision Proposal: Section F — Approval Table

| Change ID | Dimension | Current Production Behavior | Proposed Test-Seam Behavior | Recommendation & Alternatives | Supporting Evidence | Compatibility Impact | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **F-01** | **Discovery Ownership** | Direct GC bypass walker (`build_manifest_protected_set_fs`) uses uncontained `tokio::fs::read_dir`; fallback uses catalog (`list_repo_names`). | Dedicated registry-owned GC discovery component using contained `FsMetadataReader`. | **Adopt dedicated GC component**. Alternative (reusing public catalog) rejected due to semantic divergence and path replacement risks. | `src/blob_gc/policy.rs:166-285`; `src/storage/fs.rs:566-641`; `test_repo_discovery_root_replacement_vs_repos_replacement`. | Decouples public API presentation from storage reachability safety. | **EXPLICITLY UNAPPROVED** |
| **F-02** | **Output Representation** | Catalog returns `Vec<String>` (repository names). Direct GC walker populates `HashSet<String>` (digests). | Returns `Vec<ObjectKey>` representing terminal manifest directories (e.g. `repos/a/b/manifests`, `repos/manifests`). | **Adopt ObjectKey collection**. Replaces repository string splitting and boolean root flags with explicit storage paths. | `src/storage/fs/manifest_listing.rs:86-138`; `storage_core::ObjectKey`. | Provides usable paths for subsequent manifest listing without inventing public API changes. | **EXPLICITLY UNAPPROVED** |
| **F-03** | **Traversal Strategy** | Direct GC descends into non-`manifests` directories regardless of name; catalog skips `tags`, `blobs`, `meta`, `referrers`, `manifests`. | Bounded traversal starting at `repos/`. Treats `manifests` as a non-recursive terminal leaf. Traverses other directories up to depth/entry limits. | **Adopt bounded traversal**. Rejects Option B.2 (indiscriminate rejection of subdirectories in internal folders). | `src/blob_gc/policy.rs:249-263`; `test_repo_discovery_reserved_leaf_names_excluded_and_gc_divergence`. | Preserves reachability for legacy nested manifests while bounding traversal resources. | **EXPLICITLY UNAPPROVED** |
| **F-04** | **Root-Adjacent Manifests** | Direct GC discovers `repos/manifests/<hex>`; catalog skips `manifests` at root; `manifest_dir_key("")` rejects empty repo. | Contained enumeration of `ObjectKey("repos/manifests")` as a terminal manifest directory. | **Preserve via direct contained ObjectKey**. Does not require public repository API changes or migration tooling. | `src/storage/fs/manifest_listing.rs:96-101`; `src/blob_gc/policy.rs:249-278`; test fixture L5647. | Prevents silent omission of legacy root manifests. | **EXPLICITLY UNAPPROVED** |
| **F-05** | **Symlink Entry Policy** | Direct GC walker and catalog dirent checks skip symlink entries because `file_type.is_dir()` is false. | Skip `DirEntryType::Symlink` entries during traversal. Path symlinks rejected via Linux `openat2` `ResolutionRejected`. | **Adopt skip for dirents, propagate for path resolution**. | `crates/storage-fs/src/dir.rs:93-118`; `src/storage/fs/tests.rs:5925-5969`. | Preserves existing dirent skip behavior while enforcing strict descriptor containment on paths. | **EXPLICITLY UNAPPROVED** |
| **F-06** | **Disappearance After Observation** | Direct GC walker silently terminates if `next_entry()` fails. Catalog suppresses probe errors with `.unwrap_or(false)`. | If a directory was observed in parent listing but returns `NotFound` when opened, fail closed with `StorageError::io`. | **Adopt explicit failure**. Rejects silent continuation on observed disappearance. | `src/storage/fs.rs:574-578`; `src/blob_gc/policy.rs:202-206`. | Guarantees that inconsistent or concurrently mutated directory trees do not silently succeed. | **EXPLICITLY UNAPPROVED** |
| **F-07** | **Non-UTF-8 Path Handling** | Direct GC traverses non-UTF-8 ancestors (mapping to `""`); catalog skips them. | Explicit fail-closed rejection with `StorageError::corrupt_data` on encountering non-UTF-8 directory names. | **Adopt fail-closed rejection**. Prevents silent omission of reachable manifests beneath non-UTF-8 directories. | `test_repo_discovery_non_utf8_ancestors_skipped_vs_gc_walker` (`src/storage/fs/tests.rs:5836-5894`). | Halts GC safely when unrepresentable filesystem paths exist; alerts administrator. | **EXPLICITLY UNAPPROVED** |
| **F-08** | **Wrong-Type `repos` Error Kind** | If `repos` is a file, `tokio::fs::read_dir` returns `ENOTDIR`, producing `StorageError::io`. | Contained `enumerate_dir` returns `FsDirError::NotADirectory`, translating to `StorageError::corrupt_data`. | **Acknowledge as a behavior change**. Alternative: map `NotADirectory` to `StorageError::io` to preserve current kind. | `src/storage/fs.rs:575`; `src/storage/fs/manifest_listing.rs:144-146`; `src/storage/fs/tests.rs:5971-5995`. | Changes returned error kind from `Io` to `CorruptData` on wrong-type repos root. | **EXPLICITLY UNAPPROVED** |
| **F-09** | **Iteration Error Propagation** | `while let Ok(Some(entry)) = dir.next_entry().await` silently terminates on error, returning a partial result. | Iteration errors fail closed immediately, returning `StorageError::io`. Zero partial successful results. | **Adopt strict error propagation**. Alternative: silent break (unacceptable risk of data loss). | `src/storage/fs.rs:578`; `src/blob_gc/policy.rs:206, 223`; `crates/storage-fs/src/dir.rs:335-375`. | Prevents executing GC against a partial observation of the repository tree. | **EXPLICITLY UNAPPROVED** |
| **F-10** | **Provisional Test Budgets** | Whole-walk traversal depth, total directories, and total repository count are unbounded. | Introduce caller-supplied, test-only limits (`max_depth`, `max_dir_enumerations`, `max_total_entries`, `max_manifest_dirs`). | **Establish provisional test limits; defer production defaults**. | `src/storage/fs.rs:566-641`; `src/blob_gc/policy.rs:227-285`. | Protects against stack exhaustion and unbounded memory growth in massive hierarchies. | **EXPLICITLY UNAPPROVED** |

---

## 9. Canonical Quality Gates

All canonical quality gates remain explicitly **OPEN**:
- `O-03`: Key and continuation-token contracts. (**OPEN**)
- `O-04`: Filesystem write durability and containment. (**OPEN**)
- `O-05`: Broader filesystem read containment. (**OPEN**)
- `O-06`: Typed AWS mapping and pinned-MinIO evidence. (**OPEN**)
- `O-13`: Hosting, distribution, and release strategy. (**OPEN**)
- `O-15`: Non-Linux verification. (**OPEN**)
- `O-16`: Earlier Slice 11 audit/test-inventory evidence. (**OPEN**)
- `D-06`: Broader extraction, cutover, compatibility, and distribution acceptance. (**OPEN**)
