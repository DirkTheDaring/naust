> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Contained repository/GC discovery landed (`2fc21aa`, `3b64713` lineage).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Filesystem Repository Discovery Characterization

## 1. Executive Summary & Verification Scope
This document provides an empirical characterization of the production filesystem repository discovery mechanism in `registry-rust` (`FsStorage::list_repositories`, backed by `list_repo_names` in `src/storage/fs.rs:566–641`).

Repository discovery is evaluated against its current callers, its directory recognition rules, its behavior under filesystem mutations and non-standard filesystem entries, and its architectural differences from the uncontained garbage collection walker (`build_manifest_protected_set_fs` in `src/blob_gc/policy.rs:227–285`).

### Operational Scope & Constraints
- **Production Code Status**: Unchanged. All characterization is established via Linux-gated tests in `src/storage/fs/tests.rs`.
- **Garbage Collection Safekeeping**: Characterization tests evaluate differences in discovered manifest sets. These tests do not exercise physical blob deletion or sweeping; blob reference indexing (`BlobRefIndex`) and journal safeguards remain separate.
- **Canonical Quality Gates**: All eight quality gates (`O-03`, `O-04`, `O-05`, `O-06`, `O-13`, `O-15`, `O-16`, `D-06`) remain explicitly **OPEN**.

---

## 2. Production Source Architecture

### 2.1 Mechanically Quoted Source: `list_repo_names` (`src/storage/fs.rs:566–641`)
The production implementation in `src/storage/fs.rs` discovers repositories recursively using asynchronous filesystem operations:

```rust
// Mechanically Quoted Source: src/storage/fs.rs:566–641
async fn list_repo_names(repos_root: &Path) -> Result<Vec<String>, StorageError> {
    let mut repos = Vec::new();
    let mut stack = vec![(repos_root.to_path_buf(), String::new())];

    while let Some((dir, prefix)) = stack.pop() {
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(StorageError::io(e)),
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let file_type = match entry.file_type().await {
                Ok(ft) => ft,
                Err(_) => continue,
            };

            if file_type.is_dir() {
                let name = match entry.file_name().to_str() {
                    Some(s) => s.to_string(),
                    None => continue,
                };

                if name == "tags"
                    || name == "manifests"
                    || name == "referrers"
                    || name == "blobs"
                    || name == "meta"
                {
                    continue;
                }

                let repo_name = if prefix.is_empty() {
                    name.clone()
                } else {
                    format!("{prefix}/{name}")
                };

                let path = entry.path();
                let tags_dir = path.join("tags");
                let manifests_dir = path.join("manifests");
                let blobs_dir = path.join("blobs");
                let meta_dir = path.join("meta");

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
                    repos.push(repo_name.clone());
                }

                stack.push((path, repo_name));
            }
        }
    }

    repos.sort();
    repos.dedup();
    Ok(repos)
}
```

### 2.2 Mechanically Quoted Source: `build_manifest_protected_set_fs` (`src/blob_gc/policy.rs:227–285`)
In contrast, the filesystem GC walker in `src/blob_gc/policy.rs` directly traverses `repos/` without repository name validation or recognition probes:

```rust
// Mechanically Quoted Source: src/blob_gc/policy.rs:227–285
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

---

## 3. Characterization Analysis Across Core Dimensions

### 3.1 Missing and Empty `repos/` Directory
- **Missing Directory**: When `repos_root` does not exist on disk, `tokio::fs::read_dir(&dir)` returns `io::ErrorKind::NotFound`. The match arm `Err(e) if e.kind() == NotFound => continue` consumes the error and pops the next stack item. Because the initial `repos_root` was the only item on the stack, the loop terminates immediately, returning `Ok(Vec::new())`.
- **Empty Directory**: When `repos_root` exists but contains zero directory entries, `entries.next_entry().await` yields `Ok(None)`. The inner loop finishes, the stack is empty, and `list_repo_names` returns `Ok(Vec::new())`.

### 3.2 Leaf Directory Recognition Rules
To determine whether a candidate directory is a repository, `list_repo_names` checks for the presence of four specific subdirectories via `tokio::fs::metadata`:
1. `tags/` (`has_tags`)
2. `manifests/` (`has_manifests`)
3. `blobs/` (`has_blobs`)
4. `meta/` (`has_meta`)

A candidate is included if `has_tags || has_manifests || has_blobs || has_meta` evaluates to `true`.
- **Referrers Leaf Exclusion**: The directory name `"referrers"` is omitted from the recognition condition. A candidate directory that contains only a `referrers/` subdirectory is **not recognized** as a repository.
- **Arbitrary Files and Subdirectories**: Directories containing arbitrary subdirectories (e.g. `arbitrary_subdir/`) or loose files without at least one of the four recognized leaf directories are not recognized.

### 3.3 Nested Hierarchies, Sorting, and Deduplication
- **Multi-Level Parent-Child Discovery**: The traversal descends into child directories regardless of whether the parent was recognized as a repository (unless the directory name matches one of the five reserved names). For example, in a hierarchy with `org/meta`, `org/team/tags`, and `org/team/project/manifests`, all three levels (`org`, `org/team`, `org/team/project`) are discovered.
- **Traversal Order and Determinism**: Traversal uses a LIFO stack (`stack.pop()`), which produces non-lexicographical traversal order. However, before returning, `list_repo_names` executes `repos.sort()` and `repos.dedup()`, guaranteeing a unique, deterministic, lexicographically sorted ascending vector.

### 3.4 Reserved Directory Segment Exclusion & GC Walker Divergence
`list_repo_names` explicitly skips descending into directory entries whose component name matches any of five reserved words:
- `"tags"`
- `"manifests"`
- `"referrers"`
- `"blobs"`
- `"meta"`

#### Discovered-Set Divergence with `build_manifest_protected_set_fs`
To characterize differences between repository discovery and the direct GC walker, test fixture `test_repo_discovery_reserved_leaf_names_excluded_and_gc_divergence` evaluates six independent locations, each populated with a distinct valid OCI manifest, distinct config digest, and distinct layer digest:

| Location | Reserved Segment Role | Distinct Digests | `list_repositories()` Status | `build_manifest_protected_set_fs` Status |
| :--- | :--- | :--- | :--- | :--- |
| `repos/manifests/<hex>` | Direct `manifests/` leaf under `repos/` | Manifest `...c1`, Layer `...d1` | **Omitted** (`"manifests"` skipped at top level) | **Included** (scanned as `name == "manifests"`) |
| `repos/tags/subrepo/manifests/<hex>` | Reserved `"tags"` ancestor | Manifest `...c2`, Layer `...d2` | **Omitted** (`"tags"` ancestor skipped) | **Included** (`"tags"` pushed to stack, subrepo scanned) |
| `repos/referrers/subrepo/manifests/<hex>` | Reserved `"referrers"` ancestor | Manifest `...c3`, Layer `...d3` | **Omitted** (`"referrers"` ancestor skipped) | **Included** (`"referrers"` pushed to stack, subrepo scanned) |
| `repos/blobs/subrepo/manifests/<hex>` | Reserved `"blobs"` ancestor | Manifest `...c4`, Layer `...d4` | **Omitted** (`"blobs"` ancestor skipped) | **Included** (`"blobs"` pushed to stack, subrepo scanned) |
| `repos/meta/subrepo/manifests/<hex>` | Reserved `"meta"` ancestor | Manifest `...c5`, Layer `...d5` | **Omitted** (`"meta"` ancestor skipped) | **Included** (`"meta"` pushed to stack, subrepo scanned) |
| `repos/manifests/nested_repo/manifests/<hex>` | Repository nested beneath `"manifests"` | Manifest `...c6`, Layer `...d6` | **Omitted** (`"manifests"` ancestor skipped) | **Omitted** (GC walker treats `manifests` as a non-recursive leaf scan) |

#### What These Assertions Establish
1. **Repository Discovery Omission**: `list_repositories()` skips all five reserved directory names at any depth, returning an empty list for all six fixture locations.
2. **GC Walker Leaf Scan Semantics**: In `build_manifest_protected_set_fs`, when a directory is named `"manifests"`, it scans regular files inside that directory and does **not** push subdirectories to the traversal stack. Consequently, `repos/manifests/<hex>` is included, but `repos/manifests/nested_repo/manifests/<hex>` is omitted from the protected set by both mechanisms.
3. **Protected Set Scope**: If the direct GC walker were replaced with repository-driven manifest listing without reconciling reserved segment handling, manifests located under reserved directory names (or at the root of `repos/`) would be **omitted from this manifest-protected set**. Blob reference indexing and other GC safeguards remain separate; these tests do not exercise physical blob deletion or sweeping.

### 3.5 Non-UTF-8 Ancestors & Downstream Name Validation
- **Non-UTF-8 Directory Entries**: `entry.file_name().to_str()` evaluates to `None` for directory names containing invalid UTF-8 byte sequences. `list_repo_names` skips these entries via `None => continue`, omitting the entire subtree beneath them. In contrast, `build_manifest_protected_set_fs` maps non-UTF-8 directory names to empty string `""` via `.unwrap_or("")`, which does not equal `"manifests"`, causing the walker to push the path to `stack` and discover manifests inside valid UTF-8 subdirectories beneath non-UTF-8 ancestors.
- **Filesystem Directory Names vs. Repository Name Constraints**: POSIX `readdir` returns individual directory entry names. It does not return path separators (`/`), empty segments (`//`), or path traversal prefixes (`..`) as repository directory names. However, single entry names containing characters rejected by OCI/distribution naming rules can exist on disk.
- **Tested Fixture**: Test fixture `test_repo_discovery_invalid_repo_name_aborts_downstream_manifest_listing` uses a directory named `"invalid\\backslash"`.
  * `list_repo_names` returns `"invalid\\backslash"` as a discovered repository name.
  * Downstream, calling `storage.list_manifest_digests_page("invalid\\backslash", ...)` invokes `manifest_dir_key("invalid\\backslash")`, which validates repository naming and rejects the string with `StorageError::InvalidRepoName("repository name cannot contain backslashes")`.
  * In `build_manifest_protected_set`, this error propagates via `?` as `GcPolicyError::ListManifests`, causing the entire GC discovery run to **fail closed**, rather than omitting manifests silently.

### 3.6 Symlink Semantics & Directory Recognition
- **Top-Level `repos/` Symlink**: `tokio::fs::read_dir(&repos_root)` follows symlinks. If `repos/` is a symlink pointing to an external directory tree, `list_repo_names` enumerates and discovers repositories within the target tree.
- **Symlinked Repository Entries**: During enumeration, `entry.file_type().await` inspects the dirent without dereferencing symlinks. For a symlink pointing to a directory, `file_type.is_dir()` returns `false`. Consequently, symlinked repository directories are **skipped**.
- **Symlinked Recognition Leaf**: When probing for recognition leaves, `tokio::fs::metadata(&tags_dir)` dereferences symlinks. A real repository directory containing a symlink `tags -> /external/tags` evaluates `m.is_dir() == true`, successfully recognizing the repository.

### 3.7 Wrong-Type Paths & Permission Failures
- **Wrong-Type `repos` Path**: If `repos` is a regular file instead of a directory, `tokio::fs::read_dir` fails with `ENOTDIR`. `list_repo_names` returns `StorageError::io(e)` with internal kind `StorageErrorKind::Io`.
- **Regular File Inside `repos/`**: Regular files inside `repos/` fail `file_type.is_dir()` and are skipped.
- **Permission Denied on `repos/`**: In unprivileged environments where mode `0o000` denies directory traversal, `tokio::fs::read_dir(&dir)` returns `std::io::ErrorKind::PermissionDenied`. `list_repo_names` maps this to `StorageError::io(e)` with internal kind `StorageErrorKind::Io`.
- **Permission Test Execution**: Test `test_repo_discovery_permission_denied_ignored` is ignored by default to prevent false negatives in privileged/root environments where `CAP_DAC_OVERRIDE` bypasses mode `0o000`. The test checks effective permissions before asserting, runs under `-- --ignored`, and uses `ScopedPermReset` to guarantee permission restoration during normal completion or panic unwinding.

### 3.8 Storage-Root Pathname Replacement vs. In-Root `repos/` Replacement
The interaction between uncontained path traversal and descriptor-pinned storage was characterized in `test_repo_discovery_root_replacement_vs_repos_replacement`:
1. **Storage-Root Pathname Replacement**: When the host directory backing `active_root` is renamed or replaced on disk, `list_repositories()` (which opens paths relative to the current pathname) discovers repositories in the *new* filesystem directory. However, `FsMetadataReader` remains pinned to the initial `root_fd`. Subsequent contained manifest listings for newly discovered repositories resolve relative to `root_fd`, fail to find the new directory, and translate `NotFound` into an empty page without error.
2. **In-Root `repos/` Replacement**: When `repos/` is replaced beneath the *same* storage root, `openat2` relative to `root_fd` dynamically resolves `repos/<repo>/manifests` at call time, successfully observing the replacement directory.

---

## 4. Resource Bounds & Containment Analysis

### 4.1 Unbounded Whole-Walk Memory Allocations
- In `storage-layer-rust` (`storage-fs/src/dir.rs`), single-directory reads enforce strict bounds via `DirEnumerationLimits`:
  * `max_entries`: Maximum directory entries retained.
  * `max_total_name_bytes`: Maximum cumulative bytes across retained entry names.
- In contrast, `list_repo_names` accumulates all discovered repository names into an unbounded heap vector `Vec<String>`. In installations with millions of repositories, this vector grows without upper bound.

### 4.2 Unbounded Traversal Depth
- `list_repo_names` traverses directory trees recursively via `stack.push((path, repo_name))`.
- There is no maximum traversal depth limit. While recursive directory traversal performs asynchronous I/O across deep directory trees, no performance benchmarks or worker threadpool saturation experiments were conducted in these characterization tests. The architectural observation is that traversal work is unbounded by caller limits.

### 4.3 Absence of Namespace Isolation & Snapshot Isolation
- Directory enumeration operates directly on host filesystem paths without filesystem mount isolation or directory file descriptor constraints.
- Enumeration lacks point-in-time snapshot isolation: concurrent directory creations, renames, or deletions during traversal can result in partial or interleaved views.

### 4.4 Architectural Design Choices for Future Contained Discovery
Whether future contained directory discovery adopts a tokenized pagination interface, batch streaming, or another mechanism remains an open design choice, not an established prerequisite.

---

## 5. Unexercised-Path Inventory & Source-Only Behaviors

The following behaviors and edge conditions in `list_repo_names` are established from source analysis:
1. **Source-Only Recognition-Probe Error Suppression (`.unwrap_or(false)`)**:
   In `list_repo_names`, each recognition probe evaluates:
   ```rust
   let has_tags = tokio::fs::metadata(&tags_dir).await.map(|m| m.is_dir()).unwrap_or(false);
   ```
   If `tokio::fs::metadata()` encounters an error (such as `EACCES`, `EIO`, or `ELOOP`), the `.unwrap_or(false)` call suppresses the error and treats the leaf as absent. The error is neither logged nor propagated, silently causing the repository to be omitted from discovery.
2. **Subtree Permission Failure During Traversal**:
   If permissions deny reading a subdirectory deeper in the tree, `tokio::fs::read_dir(&dir)` returns `StorageError::io(e)`, failing the entire discovery operation, even if previous repositories were successfully enumerated.
3. **Symlink Cycle Handling**:
   `tokio::fs::metadata` follows symlinks. A symlink cycle in a leaf probe (`tags -> tags`) fails with `ELOOP`, which `.unwrap_or(false)` suppresses, treating the leaf as absent.

---

## 6. Canonical Quality Gates
All eight quality gates remain explicitly **OPEN**:
- `O-03`: Key and continuation-token contracts. (**OPEN**)
- `O-04`: Filesystem write durability and containment. (**OPEN**)
- `O-05`: Broader filesystem read containment. (**OPEN**)
- `O-06`: Typed AWS mapping and pinned-MinIO evidence. (**OPEN**)
- `O-13`: Hosting, distribution, and release strategy. (**OPEN**)
- `O-15`: Non-Linux verification. (**OPEN**)
- `O-16`: Earlier Slice 11 audit/test-inventory evidence. (**OPEN**)
- `D-06`: Broader extraction, cutover, compatibility, and distribution acceptance. (**OPEN**)
