# Filesystem Manifest Listing Characterization

This document records the observed runtime behavior and verified source semantics of existing filesystem manifest listing in `registry-rust` (`FsStorage::list_manifest_digests_page` and its forwarding via `ManifestReader::list_manifest_digests_page`).

This slice establishes actual behavior prior to designing contained manifest listing. It characterizes current behavior without prescribing a new pagination contract, error policy, resource limit, or implementation.

---

## 1. Call Chain and Ownership

### 1.1 Interface and Trait Forwarding

Manifest listing is defined across three layers in `registry-rust`:

1. **Concrete Implementation** (`src/storage/fs.rs:1000-1046`):
   ```rust
   async fn list_manifest_digests_page(
       &self,
       repo: &str,
       continuation_token: Option<&str>,
       page_limit: usize,
   ) -> Result<(Vec<Digest>, Option<String>), StorageError>
   ```
2. **Port Trait** (`src/storage/ports/mod.rs:56-62`):
   ```rust
   #[async_trait]
   pub trait ManifestReader: Send + Sync {
       // ...
       async fn list_manifest_digests_page(
           &self,
           repo: &str,
           continuation_token: Option<&str>,
           page_limit: usize,
       ) -> Result<(Vec<Digest>, Option<String>), StorageError>;
   }
   ```
   Forwarded on `FsStorage` at `src/storage/ports/mod.rs:448-456`:
   ```rust
   async fn list_manifest_digests_page(
       &self,
       repo: &str,
       continuation_token: Option<&str>,
       page_limit: usize,
   ) -> Result<(Vec<$crate::registry::digest::Digest>, Option<String>), $crate::storage::StorageError> {
       $crate::storage::Storage::list_manifest_digests_page(self, repo, continuation_token, page_limit).await
   }
   ```
3. **Omnibus Storage Trait** (`src/storage/mod.rs:411-416` and forwarded for `&T` at `src/storage/ports/mod.rs:768-777`):
   ```rust
   async fn list_manifest_digests_page(
       &self,
       repo: &str,
       continuation_token: Option<&str>,
       page_limit: usize,
   ) -> Result<(Vec<Digest>, Option<String>), StorageError>;
   ```

### 1.2 Actual Control Flow and Call Chain

```
Caller (GC / RefIndex / ManifestLifecycle)
  │
  ▼
ManifestReader::list_manifest_digests_page (src/storage/ports/mod.rs:56-62)
  │
  ▼
Storage::list_manifest_digests_page (src/storage/ports/mod.rs:448-456)
  │
  ▼
FsStorage::list_manifest_digests_page (src/storage/fs.rs:1000-1046)
  │
  ├─ Path Construction (line 1006):
  │    let manifests_dir = self.root.join("repos").join(repo).join("manifests");
  │
  ├─ Initial Existence Check (lines 1007-1009):
  │    if !manifests_dir.exists() { return Ok((Vec::new(), None)); }
  │
  ├─ Directory Traversal (lines 1011-1024):
  │    if let Ok(mut entries) = tokio::fs::read_dir(&manifests_dir).await {
  │        while let Ok(Some(entry)) = entries.next_entry().await { ... }
  │    }
  │
  ├─ Entry Parsing:
  │    ├─ Filter: .tmp. and .lock. skipped
  │    ├─ Try 1: Digest::parse(&format!("sha256:{file_name}"))
  │    └─ Try 2: Digest::parse(&file_name)
  │
  ├─ In-Memory Sorting (line 1025):
  │    all_digests.sort_by(|a, b| a.hex().cmp(b.hex()));
  │
  ├─ Binary Search Lookup (lines 1027-1034):
  │    match all_digests.binary_search_by(|d| d.as_str().as_str().cmp(token)) {
  │        Ok(idx) => idx + 1,
  │        Err(idx) => idx,
  │    }
  │
  ├─ Page Slicing (lines 1036-1037):
  │    let end_idx = (start_idx + page_limit).min(all_digests.len());
  │    let page_slice = &all_digests[start_idx..end_idx];
  │
  └─ Continuation Token Generation (lines 1039-1043):
       if end_idx < all_digests.len() {
           page_slice.last().map(|d| d.as_str().to_string())
       } else {
           None
       }
```

---

## 2. Source-Inspected vs. Executed Behavior Matrix

| Behavior Domain | Concrete Observed Semantic | Evidence Type | Source Reference | Characterization Test |
| :--- | :--- | :--- | :--- | :--- |
| **Missing repo dir** | Returns `Ok(([], None))` | Executed & Source | `fs.rs:1007-1009` | `test_manifest_listing_missing_and_empty_paths` |
| **Missing manifests/ dir** | Returns `Ok(([], None))` | Executed & Source | `fs.rs:1007-1009` | `test_manifest_listing_missing_and_empty_paths` |
| **Empty manifests/ dir** | Returns `Ok(([], None))` | Executed & Source | `fs.rs:1012` | `test_manifest_listing_missing_and_empty_paths` |
| **Creation order** | Ignored; outputs sorted ascending by `hex()` | Executed & Source | `fs.rs:1025` | `test_manifest_listing_ordering_independent_of_creation_order` |
| **Page limit = 0** | Returns `Ok(([], None))`; `next_token = None` (no progress) | Executed & Source | `fs.rs:1036-1043` | `test_manifest_listing_page_limits_zero_and_oversized` |
| **Page limit >= total** | Returns all entries; `next_token = None` | Executed & Source | `fs.rs:1036-1043` | `test_manifest_listing_page_limits_zero_and_oversized` |
| **Continuation token** | Returns `Some(d.as_str())` when items remain; `None` on final page | Executed & Source | `fs.rs:1040` | `test_manifest_listing_complete_traversal_and_continuation_tokens` |
| **Arbitrary token before** | Starts at index 0 (all items returned) | Executed & Source | `fs.rs:1030` | `test_manifest_listing_arbitrary_tokens_boundary_cases` |
| **Arbitrary token between** | Starts at insertion point `Ok(i)+1` or `Err(i)` | Executed & Source | `fs.rs:1029-1030` | `test_manifest_listing_arbitrary_tokens_boundary_cases` |
| **Arbitrary token after** | Returns `Ok(([], None))` | Executed & Source | `fs.rs:1036-1045` | `test_manifest_listing_arbitrary_tokens_boundary_cases` |
| **Raw SHA-256 hex name** | Parsed as `sha256:<hex>` | Executed & Source | `fs.rs:1018` | `test_manifest_listing_filename_interpretation_variants` |
| **Raw SHA-512 hex name** | **Ignored** (omitted from listing) | Executed & Source | `fs.rs:1018-1022` | `test_manifest_listing_filename_interpretation_variants` |
| **Prefixed `sha256:<hex>`** | Parsed via second branch `Digest::parse(&file_name)` | Executed & Source | `fs.rs:1020` | `test_manifest_listing_filename_interpretation_variants` |
| **Prefixed `sha512:<hex>`** | Parsed via second branch `Digest::parse(&file_name)` | Executed & Source | `fs.rs:1020` | `test_manifest_listing_filename_interpretation_variants` |
| **Uppercase hex name** | Parsed and normalized to lowercase `Digest` | Executed & Source | `digest.rs:36` | `test_manifest_listing_filename_interpretation_variants` |
| **Temporary/lock files** | Skipped (`.tmp.` and `.lock.` prefixes) | Executed & Source | `fs.rs:1015` | `test_manifest_listing_filename_interpretation_variants` |
| **Malformed file names** | Skipped silently (`Digest::parse` fails) | Executed & Source | `fs.rs:1022` | `test_manifest_listing_filename_interpretation_variants` |
| **Non-UTF-8 file names** | `to_string_lossy` introduces `U+FFFD`, parse fails, skipped | Executed & Source | `fs.rs:1014` | `test_manifest_listing_non_utf8_filename_ignored` |
| **Duplicate digest names** | **Not deduplicated**; duplicate entries returned | Executed & Source | `fs.rs:1019, 1021` | `test_manifest_listing_duplicate_digest_filenames_not_deduplicated` |
| **Mixed-algorithm ordering** | **Sorting vs. cursor mismatch**: slice sorted by `hex()`, searched by `as_str()`; binary search comparator disagrees with partition order | Executed & Source | `fs.rs:1025, 1028` | `test_manifest_listing_mixed_algorithm_sorting_and_cursor_mismatch` |
| **Entry file types** | **Unchecked**: directories, valid symlinks, dangling symlinks listed if name parses | Executed & Source | `fs.rs:1013-1023` | `test_manifest_listing_entry_types_unfiltered` |
| **Symlinked manifests/** | Followed; lists target directory | Executed & Source | `fs.rs:1006` | `test_manifest_listing_symlinked_manifests_and_ancestors` |
| **Symlinked repo ancestor** | Followed; lists target repository manifests | Executed & Source | `fs.rs:1006` | `test_manifest_listing_symlinked_manifests_and_ancestors` |
| **Path traversal (`../`)** | **Uncontained**: escapes `root/repos` via `Path::join` | Executed & Source | `fs.rs:1006` | `test_manifest_listing_path_traversal_and_absolute_paths` |
| **Absolute repo name** | **Uncontained**: redirects to `<abs_path>/manifests` | Executed & Source | `fs.rs:1006` | `test_manifest_listing_path_traversal_and_absolute_paths` |
| **`ENOTDIR` error** | Swallowed; returns `Ok(([], None))` | Executed & Source | `fs.rs:1007, 1012` | `test_manifest_listing_component_wrong_type_suppressed` |
| **`EACCES` on `read_dir`** | Swallowed; returns `Ok(([], None))` | Executed & Source | `fs.rs:1007, 1012` | `test_manifest_listing_permission_denied_ignored` |
| **Mid-stream iteration error** | Loop terminates early; returns partial slice without error | Source-Inspected | `fs.rs:1013` | Documented coverage gap (see Section 5.3) |
| **Inter-page mutation** | Insertion between completed page calls is observed on subsequent page | Executed & Source | `fs.rs:1005-1045` | `test_manifest_listing_inter_page_mutation_lacks_snapshot_isolation` |

---

## 3. Detailed Semantic Analysis

### 3.1 Filename Parsing and the Raw SHA-512 Ingestion Gap

The entry parsing loop in `src/storage/fs.rs:1012-1024` executes:
```rust
if let Ok(mut entries) = tokio::fs::read_dir(&manifests_dir).await {
    while let Ok(Some(entry)) = entries.next_entry().await {
        let file_name = entry.file_name().to_string_lossy().to_string();
        if file_name.starts_with(".tmp.") || file_name.starts_with(".lock.") {
            continue;
        }
        if let Ok(d) = Digest::parse(&format!("sha256:{file_name}")) {
            all_digests.push(d);
        } else if let Ok(d) = Digest::parse(&file_name) {
            all_digests.push(d);
        }
    }
}
```

#### Observations:
1. **Raw SHA-256 hex**: A 64-hex filename is prefixed with `"sha256:"`. `Digest::parse` succeeds because the algorithm is `"sha256"` and hex length is exactly 64 (`src/registry/digest.rs:48`).
2. **Raw SHA-512 hex**: A 128-hex filename is first prefixed with `"sha256:"`. `Digest::parse` fails because the `"sha256"` branch enforces `hex.len() == 64`. The fallback branch `Digest::parse(&file_name)` fails because `Digest::parse` requires a colon (`:`) separator (`src/registry/digest.rs:25`). **Result**: Raw SHA-512 files (which are valid payload filenames produced by `digest.hex()`) are silently skipped and never returned by manifest listing.
3. **Prefixed filenames**: Filenames containing `sha256:<64hex>` or `sha512:<128hex>` fail the first branch (`sha256:sha256:...`) but succeed in the second branch.
4. **Uppercase hex**: `Digest::parse` parses uppercase hex characters and converts them to lowercase via `hex.to_ascii_lowercase()` (`src/registry/digest.rs:36`).
5. **Deduplication**: If both `aaaaaaaa...` (raw) and `sha256:aaaaaaaa...` (prefixed) exist in the directory, both are parsed into identical `Digest` values and both are added to `all_digests`. Listing does **not** deduplicate.

### 3.2 Sorting vs. Cursor Binary Search Mismatch

In `src/storage/fs.rs`:
- Line 1025: `all_digests.sort_by(|a, b| a.hex().cmp(b.hex()));`
- Line 1028: `match all_digests.binary_search_by(|d| d.as_str().as_str().cmp(token))`

#### Deterministic Ordering Mismatch:
- `d.hex()` returns only the raw hexadecimal string (`src/registry/digest.rs:60-61`).
- `d.as_str()` returns `format!("{}:{}", self.algorithm, self.hex)` (`src/registry/digest.rs:56-58`).
- The continuation token returned by `list_manifest_digests_page` at line 1040 is `d.as_str().to_string()`.
- In a homogeneous collection (e.g. all SHA-256), sorting by `a.hex().cmp(b.hex())` produces the same relative ordering as sorting by `a.as_str().cmp(b.as_str())`.
- However, when mixed digest algorithms are present (e.g. SHA-256 and SHA-512), the slice is sorted by `hex()` but searched by `as_str()`. Because `as_str()` includes the algorithm prefix (`"sha256:"` vs `"sha512:"`), the slice is **not partitioned** according to the binary search comparator.
- Characterization test `test_manifest_listing_mixed_algorithm_sorting_and_cursor_mismatch` proves deterministically that `hex()` order disagrees with canonical string order (`all[0].hex() < all[1].hex()`, but `all[0].as_str() > all[1].as_str()`).

#### Observed Toolchain Pagination Outcome:
- In the bounded observation run against the unpaginated 3-item collection (`sha512:1111...`, `sha256:2222...`, `sha256:8888...`), pagination executed 2 steps and terminated upon receiving an empty page.
- Collected: 1 digest (`sha512:1111...`).
- Omitted: 2 digests (`sha256:2222...`, `sha256:8888...`).
- This failure demonstrates that binary search on an unpartitioned slice does not guarantee progressing cursor lookup. This specific probe sequence and outcome are recorded as observed facts on the tested toolchain, not as a portable contract or desirable behavior.

### 3.3 Page Limit, Arithmetic Bounds, and usize Overflow

Lines 1036-1045:
```rust
let end_idx = (start_idx + page_limit).min(all_digests.len());
let page_slice = &all_digests[start_idx..end_idx];

let next_token = if end_idx < all_digests.len() {
    page_slice.last().map(|d| d.as_str().to_string())
} else {
    None
};

Ok((page_slice.to_vec(), next_token))
```

#### Observations:
1. **Zero page limit (`page_limit == 0`)**:
   `start_idx + 0 == start_idx`. `end_idx = start_idx.min(len) = start_idx`.
   `page_slice = &all_digests[start_idx..start_idx]` is empty.
   `next_token` evaluates to `None` because `page_slice.last()` is `None` (or if `end_idx == all_digests.len()`, `next_token` is `None`).
   The function returns `Ok(([], None))`. The caller receives an empty page and no continuation token, stopping iteration even when unvisited items remain.
2. **Arithmetic overflow of `start_idx + page_limit`**:
   `start_idx` and `page_limit` are both of type `usize`.
   The expression `start_idx + page_limit` uses standard integer addition (`+`).
   - In debug builds or builds with `overflow-checks = true`, if `start_idx + page_limit > usize::MAX`, the process panics with an overflow error.
   - In release builds with two's complement wrapping (`overflow-checks = false`), `start_idx + page_limit` wraps around. If it wraps to a value `w` such that `w < all_digests.len()` and `w < start_idx`, then `end_idx = w.min(len) = w`. The slice expression `&all_digests[start_idx..end_idx]` will then attempt `start_idx..w` where `start_idx > end_idx`, causing a panic (`slice index starts after end`).
   - This overflow does **not** depend on large directory size: even with a small directory of 1 item, a caller supplying `page_limit = usize::MAX` causes `1 + usize::MAX` to overflow `usize`.
3. **Cursor out-of-bounds**:
   If `token` sorts after all items, `binary_search_by` returns `Err(len)`. `start_idx = len`.
   `end_idx = (len + page_limit).min(len) = len`.
   `page_slice = &all_digests[len..len] = []`.
   `end_idx < len` is `false`. `next_token = None`.
   Returns `Ok(([], None))` without requiring a separate `start_idx >= len` early return.

### 3.4 Entry Types and Containment

- `tokio::fs::read_dir` returns `DirEntry` items.
- `FsStorage::list_manifest_digests_page` inspects only `entry.file_name()`. It does not call `entry.file_type()`, `entry.metadata()`, or `tokio::fs::symlink_metadata()`.
- **Observed Consequences**:
  1. If a directory named with 64 hex digits exists in `manifests/`, it is returned as a manifest digest.
  2. If a symlink (pointing inside or outside the storage root) has a 64-hex name, it is returned as a manifest digest.
  3. If a dangling symlink has a 64-hex name, it is returned as a manifest digest.
- **Path Escape**:
  The directory path is built via `self.root.join("repos").join(repo).join("manifests")`.
  No path canonicalization or containment check is performed. A caller passing `repo = "../../other"` escapes `root/repos`. An absolute path replaces the path prefix completely.

### 3.5 Error Handling and Silent Suppression

Error suppression occurs at two distinct layers in `list_manifest_digests_page`:
1. **Initial existence check (lines 1007-1009)**:
   ```rust
   if !manifests_dir.exists() {
       return Ok((Vec::new(), None));
   }
   ```
   `Path::exists()` calls `std::fs::metadata()`. It returns `false` not only when the target path does not exist (`ENOENT`), but also when metadata lookup fails due to permissions (`EACCES` on an ancestor component), broken symlinks, or non-directory ancestor components (`ENOTDIR`). Any such metadata failure produces `Ok(([], None))`.
2. **Directory opening (line 1012)**:
   ```rust
   if let Ok(mut entries) = tokio::fs::read_dir(&manifests_dir).await {
   ```
   If `manifests_dir.exists()` evaluates to `true` (e.g. `manifests` exists as a regular file, or is a symlink to an unreadable directory), `tokio::fs::read_dir(&manifests_dir).await` returns an error (`ENOTDIR` or `EACCES`). The `if let Ok` guard evaluates to `false`, and the method returns `Ok((Vec::new(), None))`.
3. **Mid-stream iteration errors (line 1013)**:
   ```rust
   while let Ok(Some(entry)) = entries.next_entry().await {
   ```
   If an error occurs while iterating entries after the directory has been opened, the `while let Ok(Some(entry))` loop terminates immediately without propagating the error, returning a partial list of manifests collected up to that point.

---

## 4. Production Caller Impact Assessment

Inspection of actual production call sites in `registry-rust` reveals how callers interact with `list_manifest_digests_page`:

### 4.1 `src/manifest_lifecycle.rs:773-801` (`is_blob_referenced_in_repo`)

```rust
async fn is_blob_referenced_in_repo(&self, repo: &str, target_blob: &Digest) -> bool {
    let mut tok: Option<String> = None;
    loop {
        let (page, next_tok) = match self
            .storage
            .list_manifest_digests_page(repo, tok.as_deref(), 100)
            .await
        {
            Ok(p) => p,
            Err(_) => return false,
        };
        for m_d in page {
            if let Ok((_meta, bytes)) = self.storage.get_manifest(repo, &m_d).await {
                if let Ok(refs) = crate::manifest_refs::parse_manifest_refs(&bytes) {
                    for b in refs.blob_references() {
                        if b == target_blob {
                            return true;
                        }
                    }
                }
            }
        }
        match next_tok {
            Some(t) => tok = Some(t),
            None => break,
        }
    }
    false
}
```
- **Control Flow & Nontermination**:
  - The loop terminates when a matching blob reference is found (`return true`), when `next_tok` is `None` (`break; false`), or when listing returns an `Err(_)` (`return false`).
  - **No Progress Guard**: If `next_tok` returns `Some(t)` with the same token repeatedly and `page` is nonempty, the loop does **not** break. It will repeat the same page indefinitely (nontermination and CPU burn).
- **Incomplete Reference Discovery**:
  - If a raw SHA-512 manifest exists, it is omitted by listing; `is_blob_referenced_in_repo` fails to inspect that manifest, returning `false` if no other manifest references the blob.
- **Subsequent Readability Mismatch**:
  - If a manifest was stored under an uppercase filename or prefixed filename (e.g. `sha256:<hex>`), listing accepts the name and normalizes it to lowercase `Digest`.
  - However, `get_manifest` resolves `repos/<repo>/manifests/<digest.hex()>`. On case-sensitive filesystems, if the file exists only as `sha256:<hex>` or with uppercase characters, `get_manifest` returns `NotFound`. The inner `if let Ok((_meta, bytes))` fails silently and skips reference inspection for that manifest.

### 4.2 `src/blob_gc/policy.rs:170-225` (`build_manifest_protected_set`)

```rust
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
```
- **Backend Dispatch**:
  - When `storage.kind() == "fs"` and `repos/` exists, `build_manifest_protected_set` routes directly to `build_manifest_protected_set_fs(&cfg.fs_root)`, which performs its own recursive directory walk of `repos/*/manifests/*` rather than calling `list_manifest_digests_page`.
  - When invoked against abstract storage (or when `repos/` is absent), the `for repo in repos` loop calls `list_manifest_digests_page`.
- **Accumulation and Scope of Omission**:
  - `protected` is initialized once (`let mut protected = HashSet::new()`) and accumulates across all repositories in `repos`.
  - An omitted repository (e.g. returning `Ok(([], None))` due to `EACCES` or missing `manifests/`) omits manifests for that single repository. It does **not** make the entire protected set empty, because other repositories continue to contribute.
  - Potential consequence: If a repository's manifest is omitted (such as raw SHA-512), references from that manifest are not added to `protected`. If GC policy later relies on `protected` to determine candidate deletion, unreferenced blob identification could be inaccurate for blobs referenced only by that manifest. Actual blob deletion requires that GC sweep is subsequently executed, candidate grace periods elapse, and no secondary references protect the blob.

### 4.3 `src/blob_ref_index.rs:503-517` (`reconcile_repo`)

```rust
let mut manifest_token: Option<String> = None;
loop {
    let (manifests, next_tok) = storage
        .list_manifest_digests_page(repo, manifest_token.as_deref(), 128)
        .await?;
    for digest in manifests {
        self.inc_root_count(digest.as_str().as_bytes())?;
        self.ingest_root(storage, repo, &digest).await?;
    }
    match next_tok {
        Some(tok) => manifest_token = Some(tok),
        None => break,
    }
}
```
- **No Progress Guard**: Like `is_blob_referenced_in_repo`, if `next_tok` returns the same token repeatedly and `manifests` is nonempty, the loop will run indefinitely.
- **Silent Suppression**: If `list_manifest_digests_page` returns `Ok(([], None))` due to permission errors on `manifests/`, `reconcile_repo` breaks immediately without error, skipping root ingestion for that repository while proceeding to tag reconciliation.

---

## 5. Compatibility Assessment with `storage-fs` Directory Enumeration

### 5.1 Committed `storage-fs` Directory Enumeration Primitive

The committed `storage-fs` directory enumeration API (`storage-layer-rust/crates/storage-fs/src/reader.rs:291-313` and `storage-layer-rust/crates/storage-fs/src/dir.rs:67-221`) provides:

```rust
pub async fn enumerate_dir(
    &self,
    target: Option<&ObjectKey>,
    limits: crate::dir::DirEnumerationLimits,
) -> Result<Vec<crate::dir::DirEntry>, crate::dir::FsDirError>
```

#### Architectural Characteristics:
1. **Descriptor-Relative Resolution**:
   Resolves `target` relative to the pinned root directory descriptor using Linux `openat2` with flags:
   `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`.
   Symlinks and directory escapes are rejected at the kernel resolution boundary.
2. **Materialized Bounded Result**:
   Returns a single materialized `Vec<DirEntry>`. It does not expose a streaming iterator, batch cursor, or `getdents64` interface across the public API.
3. **Resource Accounting (`DirEnumerationLimits`)**:
   Enforces caller-specified limits on:
   - `max_entries`: Maximum number of directory entries retained.
   - `max_total_name_bytes`: Maximum cumulative bytes across retained entry names.
   Exceeding either budget fails immediately with a typed error (`FsDirError::LimitExceeded { reason }`) without returning partial success.
4. **Point-in-Time Observations (`DirEntryType`)**:
   Reports `DirEntryType` (`Regular`, `Directory`, `Symlink`, `Other`).
   These types are point-in-time observations made during iteration (`readdir`), **not capabilities** authorizing subsequent pathname access.
5. **Strongly Typed Errors (`FsDirError`)**:
   Returns strongly typed errors defining the full operational failure surface:
   - `FsDirError::NotFound { path }`
   - `FsDirError::NotADirectory { path }`
   - `FsDirError::PermissionDenied { path, source }`
   - `FsDirError::ResolutionRejected { raw_os_error, source }`
   - `FsDirError::SyscallUnsupported(source)`
   - `FsDirError::LimitExceeded { reason }`
   - `FsDirError::EntryDisappeared { name }`
   - `FsDirError::Io { source }`
   - `FsDirError::RuntimeMissing(source)`
   - `FsDirError::TaskJoinFailed(source)`
   - `FsDirError::PlatformUnsupported`
6. **Descriptor Ownership and Safety**:
   The descriptor opened via `openat2` is transferred to a `DIR*` stream managed with `libc::fdopendir` and closed via `libc::closedir` in RAII guards.
7. **Explicit Non-Guarantees**:
   - `RESOLVE_BENEATH` prevents escaping the pinned root descriptor, but does not provide mount isolation for child mounts attached beneath the root.
   - Directory iteration does not provide snapshot isolation; entries added or removed concurrently may be partially observed.
   - It does not track or isolate hard-link aliases.

### 5.2 Compatibility Decisions for a Later Contained-Listing Design

| Semantic Dimension | Existing `FsStorage::list_manifest_digests_page` | Contained `storage-fs` Primitive | Decision Required for Future Contained Listing |
| :--- | :--- | :--- | :--- |
| **Path Containment** | Unvalidated `Path::join`; permits traversal and absolute paths | Contained `openat2` resolution beneath pinned root | Adopt contained resolution for `repos/<repo>/manifests` |
| **Entry Type Filtering** | Unchecked; directories and symlinks are listed | Reports observed `DirEntryType` | Decide whether to filter for `DirEntryType::Regular` only or support symlinks |
| **Error Surfacing** | Silently swallows `ENOENT`, `ENOTDIR`, and `EACCES` | Returns typed `FsDirError` (e.g. `NotFound`, `NotADirectory`, `PermissionDenied`, `ResolutionRejected`) | Differentiate expected missing repository/manifests directory from unexpected access denial |
| **Filename Parsing** | Omits raw SHA-512; accepts uppercase and prefixed names; no deduplication | Domain-free `OsString` names | Reconcile filename parsing: fix raw SHA-512 gap, decide on deduplication and normalization |
| **Sorting & Cursor** | In-memory sort by `hex()`, binary search by `as_str()` | Unordered directory entries | Reconcile sorting key with cursor comparator (sort and search by canonical `Digest::as_str()`) |
| **Pagination Model** | Reads entire directory into memory, sorts, slices | Bounded single-directory enumeration | Decide whether pagination remains registry-owned (buffered in-memory) or moves to a contained cursor model |

### 5.3 Documented Coverage Gap: Mid-Stream Directory Iteration Errors

In `FsStorage::list_manifest_digests_page`, `while let Ok(Some(entry)) = entries.next_entry().await` silently terminates on an I/O error during directory reading. Because deterministic fault injection during directory entry iteration is not deterministically exercised in this slice without external mocks or hooks (and no production hooks may be added per charter), this specific mid-stream termination branch was verified by source inspection rather than unit test execution.

---

## 6. Limitations, Rollback Scope, and Canonical Gates

### 6.1 Limitations

- This slice is **characterization only**. No production behavior has been modified.
- The defects identified in current behavior (raw SHA-512 omission, sorting vs. cursor mismatch on mixed algorithms, silent error suppression of `EACCES`/`ENOTDIR`, and lack of file type checking) are characterized as observed facts; they are not codified as permanent requirements.
- Tests were executed on Linux (`x86_64`). Non-Linux behavior was not executed.

### 6.2 Rollback Scope

Because no production code, Cargo manifests, or dependencies were modified, rollback requires only reverting the appended test code in `src/storage/fs/tests.rs` and deleting this documentation file. No database migrations, data layout conversions, or operational steps are required.

### 6.3 Canonical Quality Gates

All canonical quality gates remain explicitly **OPEN**:
- **O-03**: Key and continuation-token contracts.
- **O-04**: Filesystem write durability and containment.
- **O-05**: Broader filesystem read containment.
- **O-06**: Typed AWS mapping and pinned-MinIO evidence.
- **O-13**: Hosting, distribution, and release strategy.
- **O-15**: Non-Linux verification.
- **O-16**: Earlier Slice 11 audit/test-inventory evidence.
- **D-06**: Broader extraction, cutover, compatibility, and distribution acceptance.
