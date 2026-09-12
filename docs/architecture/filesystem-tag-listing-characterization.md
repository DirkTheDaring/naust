# Architecture Assessment: Filesystem Tag-Listing Characterization (Corrected)

- **Document**: `docs/architecture/filesystem-tag-listing-characterization.md`
- **Repository**: `registry-rust`
- **Date**: 2026-09-12
- **Status**: CHARACTERIZATION & ARCHITECTURAL ASSESSMENT ONLY — PRODUCTION UNCHANGED — NOT COMMITTED
- **Authoritative Baseline HEADs**:
  - `registry-rust`: `5a0b4246e4c24e8db56d84f047eadae5387e7330`
  - `storage-layer-rust`: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (strictly read-only)
- **Canonical Quality Gates**: All eight gates remain explicitly **OPEN**:
  - `O-03`: Key and continuation-token contracts.
  - `O-04`: Filesystem write durability and containment.
  - `O-05`: Broader filesystem read containment.
  - `O-06`: Typed AWS mapping and pinned-MinIO evidence.
  - `O-13`: Hosting, distribution, and release strategy.
  - `O-15`: Non-Linux verification.
  - `O-16`: Earlier Slice 11 audit/test-inventory evidence.
  - `D-06`: Broader extraction, cutover, compatibility, and distribution acceptance.

---

## 1. Executive Summary & Review Baseline

This assessment characterizes the current production implementation and behavior of filesystem tag listing in `registry-rust`, specifically evaluating:
1. [`FsStorage::list_tags`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L950-L984) (`TagReader::list_tags` port).
2. [`FsStorage::list_tags_page`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1169-L1214) (`TagReader::list_tags_page` port).
3. The internal traversal helper [`FsStorage::list_tag_files`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L646-L669).
4. Interactions with the contained tag-read operations ([`FsStorage::resolve_tag`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L939-L948) and [`FsStorage::get_tag_with_version`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1250-L1262)) cut over in commit `5a0b4246e4c24e8db56d84f047eadae5387e7330`.

### 1.1 Core Invariants & Key Findings

1. **Independent Implementations & Zero Delegation**:
   Neither `list_tags` nor `list_tags_page` delegates to the other, nor does either delegate to `resolve_tag` or `get_tag_with_version`. They are independent implementations in [`src/storage/fs.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs).
2. **Complete Bypass of Contained Reader**:
   Both `list_tags` and `list_tags_page` bypass the shared `Arc<storage_fs::FsMetadataReader>` (`self.reader`) and its pinned `root_fd`. Both execute raw ambient pathname operations anchored to `self.root: PathBuf` (`tokio::fs::metadata`, `tokio::fs::read_dir`, and `tokio::fs::read_to_string`).
3. **Contrasting Directory & Error Contracts**:
   - `list_tags` verifies repository directory existence via `tokio::fs::metadata(&repo_dir)`. If the repository directory is missing, it returns `Err(StorageError::NotFound)`. If the `tags/` subfolder within an existing repository is missing, it returns `Ok(vec![])`. It inspects directory entry names only, does not open or parse tag files, and treats subdirectories as ordinary tags.
   - `list_tags_page` delegates to `list_tag_files`, which queries `self.root.join("repos").join(repo).join("tags")` directly via `tokio::fs::read_dir`. When the repository does not exist, `read_dir` returns `ErrorKind::NotFound`, causing `list_tag_files` to return `Ok(vec![])`. Thus, `list_tags_page("nonexistent-repo", ...)` returns `Ok((vec![], None))`—**silently suppressing `StorageError::NotFound` for missing repositories**.
4. **Silent Drop of Malformed, Empty, and Corrupted Content in `list_tags_page`**:
   `list_tags_page` sequentially reads each tag file via uncontained `tokio::fs::read_to_string(&path)` and parses with `Digest::parse(content.trim())`. Any file read error (I/O error, non-UTF-8 bytes, directory `EISDIR`, permission denial, dangling symlinks) or digest parse error (empty file, invalid length, malformed hex) is **silently ignored** (`if let Ok(...)`), omitting the tag from the returned page without warning or error.
5. **Symlink Traversal Escape in `list_tags_page`**:
   Because `list_tags_page` uses ambient `tokio::fs::read_to_string`, the OS kernel follows symlinks without containment. External symlinks pointing outside the repository storage root are followed; if the target file contains valid digest text, it is returned as an ordinary tag.
6. **No Resource Limits & Full-Scan In-Memory Slicing**:
   Configured limits (`self.manifest_listing_limits`) apply exclusively to manifest listing. Neither `list_tags` nor `list_tags_page` consults or enforces them. `list_tags_page` reads, opens, and parses **every single tag file in the repository into a memory buffer** on every page request, sorts the buffer, and only then applies continuation tokens and page limits.
7. **Split-Brain Namespace Divergence Under Root Replacement**:
   Contained operations (`resolve_tag`, `get_tag_with_version`) resolve relative to the pinned `root_fd` (`FsMetadataReader`). Listing operations (`list_tags`, `list_tags_page`) and tag mutations (`set_tag`, `mutate_tag`, `delete_tag_conditional`) resolve ambiently via `self.root: PathBuf`. Atomically replacing the root directory pathname causes contained reads to observe the pinned original directory (Tree A), while tag listings and mutations immediately observe and modify the replacement directory (Tree B).

---

## 2. Source-Grounded Call Chains & Ownership Boundaries

### 2.1 Trait and Architecture Map

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│ Port Declarations (src/storage/ports/mod.rs)                                            │
│                                                                                         │
│ pub trait TagReader: Send + Sync {                                                      │
│     async fn resolve_tag(&self, name: &str, tag: &str) -> Result<Digest, StorageError>;  │
│     async fn get_tag_with_version(&self, repo: &str, tag: &str)                         │
│         -> Result<Option<(Digest, String)>, StorageError>;                              │
│     async fn list_tags(&self, name: &str) -> Result<Vec<String>, StorageError>;         │
│     async fn list_tags_page(&self, repo: &str, continuation_token: Option<&str>,        │
│                            page_limit: usize)                                           │
│         -> Result<(Vec<(String, Digest)>, Option<String>), StorageError>;               │
│ }                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│ Concrete Implementation: FsStorage (src/storage/fs.rs)                                 │
│                                                                                         │
│  [CONTAINED - VIA self.reader: Arc<storage_fs::FsMetadataReader>]                       │
│  ├─> resolve_tag(name, tag)                                                             │
│  │     └─> tag_read::resolve_tag_impl(self.reader.as_ref(), name, tag, &limits)         │
│  │           └─> openat2(root_fd, RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS)                │
│  └─> get_tag_with_version(repo, tag)                                                    │
│        └─> tag_read::get_tag_with_version_impl(self.reader.as_ref(), repo, tag, &limits)│
│              └─> openat2(root_fd, RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS)                │
│                                                                                         │
│  [UNCONTAINED - VIA self.root: PathBuf AND AMBIENT TOKIO FS]                            │
│  ├─> list_tags(name)                                                                    │
│  │     ├─> tokio::fs::metadata(self.root.join("repos").join(name))                      │
│  │     └─> tokio::fs::read_dir(self.root.join("repos").join(name).join("tags"))         │
│  │           └─> iterates dir entries, filters !name.starts_with('.'), sorts            │
│  ├─> list_tags_page(repo, continuation_token, page_limit)                               │
│  │     ├─> list_tag_files(repo)                                                         │
│  │     │     └─> tokio::fs::read_dir(self.root.join("repos").join(repo).join("tags"))    │
│  │     │           └─> returns Vec<PathBuf> of entries where !name.starts_with('.')     │
│  │     ├─> for path in tag_files:                                                       │
│  │     │     └─> tokio::fs::read_to_string(&path) [AMBIENT, FOLLOWS SYMLINKS]           │
│  │     │           └─> Digest::parse(content.trim())                                    │
│  │     │                 └─> if let Ok(...) => push (rel, digest) [ERRORS DROPPED]      │
│  │     ├─> sorts full vector by tag name                                                │
│  │     └─> binary_search_by continuation_token -> slices page                           │
│  └─> delete_manifest_by_digest(name, digest, maybe_subject)                             │
│        └─> list_tag_files(name)                                                         │
│              └─> tokio::fs::read_to_string(&path)                                       │
│                    └─> if content.trim() == digest => tokio::fs::remove_file(&path)     │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Ownership & Resource Limits

| Component | Descriptor / Root Ownership | Configured Resource Limits | Payload Reading |
| :--- | :--- | :--- | :--- |
| **`resolve_tag`** | `self.reader` (`root_fd` pinned) | `TagReadLimits { max_payload_bytes: None }` | Contained chunk stream via `storage_fs` |
| **`get_tag_with_version`** | `self.reader` (`root_fd` pinned) | `TagReadLimits { max_payload_bytes: None }` | Contained chunk stream via `storage_fs` |
| **`list_tags`** | Ambient path `self.root: PathBuf` | **None** (unbounded directory enumeration) | **None** (metadata / names only) |
| **`list_tags_page`** | Ambient path `self.root: PathBuf` | **None** (`manifest_listing_limits` does NOT apply) | Ambient uncontained `read_to_string` |
| **`list_tag_files`** | Ambient path `self.root: PathBuf` | **None** (unbounded directory enumeration) | **None** (filenames only) |

---

## 3. Behavior Comparison: `list_tags` vs `list_tags_page`

The table below contrasts the observable behaviors of `list_tags` and `list_tags_page` across diverse input and filesystem conditions, verified by characterization tests in [`src/storage/fs/tests.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/tests.rs#L7094-L8048).

| Scenario | `FsStorage::list_tags` Behavior | `FsStorage::list_tags_page` Behavior | Supporting Test Name |
| :--- | :--- | :--- | :--- |
| **Missing repository directory** | Returns `Err(StorageError::NotFound)` | Returns `Ok((vec![], None))` (**NotFound suppressed**) | `test_tag_listing_missing_and_empty_directories` |
| **Existing repo, missing `tags/` dir** | Returns `Ok(vec![])` | Returns `Ok((vec![], None))` | `test_tag_listing_missing_and_empty_directories` |
| **Existing repo, empty `tags/` dir** | Returns `Ok(vec![])` | Returns `Ok((vec![], None))` | `test_tag_listing_missing_and_empty_directories` |
| **Valid SHA-256 target** | Returns tag name in sorted list | Returns `(tag, digest)` in page | `test_tag_listing_valid_sha256_and_sha512_formats` |
| **Valid SHA-512 target** | Returns tag name in sorted list | Returns `(tag, digest)` in page | `test_tag_listing_valid_sha256_and_sha512_formats` |
| **Content whitespace / CRLF / tabs** | Returns tag name (content not read) | Returns `(tag, digest)` (`Digest::parse` trims) | `test_tag_listing_whitespace_tabs_crlf_and_substantial_padding` |
| **Substantial padding (64KB whitespace)**| Returns tag name (content not read) | Returns `(tag, digest)` (`Digest::parse` trims) | `test_tag_listing_whitespace_tabs_crlf_and_substantial_padding` |
| **Empty tag file (0 bytes)** | Returns tag name | **Silently dropped** (omitted from page) | `test_tag_listing_empty_malformed_and_invalid_utf8_taxonomy` |
| **Malformed digest string** | Returns tag name | **Silently dropped** (omitted from page) | `test_tag_listing_empty_malformed_and_invalid_utf8_taxonomy` |
| **Invalid UTF-8 content** | Returns tag name | **Silently dropped** (`read_to_string` fails) | `test_tag_listing_empty_malformed_and_invalid_utf8_taxonomy` |
| **Dotfiles (`.hidden`, `.lock.v1`, `.tmp.v1`)** | **Filtered out** (`!file_name.starts_with('.')`) | **Filtered out** (at directory scan) | `test_tag_listing_dotfiles_locks_temps_and_nested_directories` |
| **Nested directory in `tags/`** | **Returned as tag name** (no file type check) | **Silently dropped** (`read_to_string` gives `EISDIR`) | `test_tag_listing_dotfiles_locks_temps_and_nested_directories` |
| **Non-UTF-8 filename** | **Silently skipped** (`to_str()` returns `None`) | **Silently skipped** (`to_str()` returns `None`) | `test_tag_listing_non_utf8_filenames` |
| **Path traversal input (`dummy/../target`)** | Resolves ambiently to `target` tags | Resolves ambiently to `target` tags | `test_tag_listing_path_traversal_and_structural_inputs` |
| **Nested repo input (`org/team/repo`)** | Resolves ambiently to nested tags | Resolves ambiently to nested tags | `test_tag_listing_path_traversal_and_structural_inputs` |
| **Absolute path input (`/tmp/abs_repo`)** | `Path::join` replaces `self.root` completely | `Path::join` replaces `self.root` completely | `test_tag_listing_path_traversal_and_structural_inputs` |
| **Empty repo name input (`""`)** | Resolves to `repos/tags` -> returns `Ok([])` | Resolves to `repos/tags` -> returns `Ok(([], None))` | `test_tag_listing_path_traversal_and_structural_inputs` |
| **Internal symlink to valid tag file** | Returns symlink filename | Returns target digest (**symlink dereferenced**) | `test_tag_listing_controlled_symlinks` |
| **External symlink pointing outside root** | Returns symlink filename | Returns target digest (**uncontained read leak**) | `test_tag_listing_controlled_symlinks` |
| **Dangling symlink** | Returns symlink filename | **Silently dropped** (`read_to_string` fails) | `test_tag_listing_controlled_symlinks` |
| **Ancestor directory symlink** | Follows symlink into target directory | Follows symlink into target directory | `test_tag_listing_controlled_symlinks` |
| **Directory entry in `tags/`** | Returns directory entry filename | **Silently dropped** (`read_to_string` gives `EISDIR`) | `test_tag_listing_non_regular_objects` |
| **Permission denied on individual tag file**| Returns tag name (file not opened) | **Silently dropped** (`read_to_string` fails) | `test_tag_listing_permission_denied` (unprivileged) |
| **Permission denied on `tags/` dir** | Returns `Err(StorageError::Io)` | Returns `Err(StorageError::Io)` | `test_tag_listing_permission_denied` (unprivileged) |
| **Continuation token: exact match** | N/A | Starts at `idx + 1` | `test_tag_listing_pagination_boundaries_cursors_and_zero_limit`|
| **Continuation token: missing / between**| N/A | Starts at insertion point `Err(idx)` | `test_tag_listing_pagination_boundaries_cursors_and_zero_limit`|
| **Continuation token: before all tags** | N/A | Starts at index `0` | `test_tag_listing_pagination_boundaries_cursors_and_zero_limit`|
| **Continuation token: after all tags** | N/A | Returns `Ok((vec![], None))` | `test_tag_listing_pagination_boundaries_cursors_and_zero_limit`|
| **Continuation token: terminal tag** | N/A | Returns `Ok((vec![], None))` | `test_tag_listing_pagination_boundaries_cursors_and_zero_limit`|
| **Structurally unusual tokens (`""`, `t/slash`, emoji, `\0`, 10KB)** | N/A | Evaluated via raw lexical byte comparison (`cmp`) | `test_tag_listing_pagination_boundaries_cursors_and_zero_limit`|
| **Page limit = 0** | N/A | Returns `Ok((vec![], None))` | `test_tag_listing_pagination_boundaries_cursors_and_zero_limit`|
| **Configured manifest listing limits** | Unbounded; limits ignored | Unbounded; limits ignored | `test_tag_listing_ignores_configured_manifest_enumeration_limits`|
| **Mutations between page requests** | N/A | Lacks snapshot isolation; reflects live changes | `test_tag_listing_deterministic_mutations_between_pages` |
| **Root directory pathname replacement** | Observes replacement tree immediately | Observes replacement tree immediately | `test_tag_listing_root_replacement_divergence` |

---

## 4. Caller Analysis: Error Propagation, Suppressions, and Mutation Side Effects

Every production caller of `list_tags` and `list_tags_page` was inspected across the codebase.

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Caller Impact Matrix                                                                                                │
├────────────────────────────────┬──────────────────────────┬──────────────────────┬──────────────────────────────────┤
│ Caller & Source File           │ Port Method Used         │ Error Handling       │ Consequence of Listing Failure   │
├────────────────────────────────┼──────────────────────────┼──────────────────────┼──────────────────────────────────┤
│ 1. TagQueryService::query_tags │ TagReader::list_tags     │ Propagated           │ Translates NotFound to HTTP 404; │
│    src/application/tags.rs:49  │                          │ (StorageError ->     │ other errors to HTTP 500.        │
│                                │                          │  AppError)           │ In-memory pagination of all tags.│
├────────────────────────────────┼──────────────────────────┼──────────────────────┼──────────────────────────────────┤
│ 2. TagQueryService::list_tags  │ TagReader::list_tags     │ Propagated           │ Used by /v2/_catalog to fetch    │
│    src/application/tags.rs:97  │                          │                      │ platforms. Errors propagate.     │
├────────────────────────────────┼──────────────────────────┼──────────────────────┼──────────────────────────────────┤
│ 3. BlobRefIndex::discover_     │ TagReader::list_tags_page│ Propagated           │ Aborts discovery via `?` before  │
│    repository_data             │                          │                      │ any sled mutation. Silent tag    │
│    src/blob_ref_index.rs:634   │                          │                      │ drops lead to root deletion!     │
├────────────────────────────────┼──────────────────────────┼──────────────────────┼──────────────────────────────────┤
│ 4. BlobRefIndex::refresh_tag_  │ TagReader::list_tags     │ Propagated           │ NotFound continues; other errors │
│    rooted_conservative         │                          │ (NotFound continues, │ abort via `return Err(e.into())`.│
│    src/blob_ref_index.rs:768   │                          │  others abort)       │ Prior tag ingestions remain.     │
├────────────────────────────────┼──────────────────────────┼──────────────────────┼──────────────────────────────────┤
│ 5. blob_delete_safety::        │ TagReader::list_tags     │ Propagated           │ NotFound skips repo; Io error    │
│    find_any_blob_reference     │                          │ (NotFound continues, │ aborts scan, preventing unsafe   │
│    src/blob_delete_safety.rs:96│                          │  others return Err)  │ blob deletion.                   │
├────────────────────────────────┼──────────────────────────┼──────────────────────┼──────────────────────────────────┤
│ 6. blob_delete_safety::        │ TagReader::list_tags     │ Propagated           │ NotFound returns Ok(None);       │
│    find_repo_blob_reference    │                          │ (NotFound -> Ok(None)│ others abort with Err.           │
│    src/blob_delete_safety.rs:132                          │  others return Err)  │                                  │
├────────────────────────────────┼──────────────────────────┼──────────────────────┼──────────────────────────────────┤
│ 7. recover_pending_journal_    │ TagReader::list_tags_page│ SUPPRESSED           │ Returns (vec![], None). Tags     │
│    under_lock (TagsSnapshotted)│                          │ (Err(_) ->           │ matching target digest are NOT   │
│    src/manifest_lifecycle:587  │                          │  (vec![], None))     │ deleted; proceeds to manifest del│
├────────────────────────────────┼──────────────────────────┼──────────────────────┼──────────────────────────────────┤
│ 8. recover_pending_journal_    │ TagReader::list_tags_page│ SUPPRESSED           │ Returns (vec![], None).          │
│    under_lock (ProxyTagDeleted)│                          │ (Err(_) ->           │ `has_other_tags` stays false!    │
│    src/manifest_lifecycle:668  │                          │  (vec![], None))     │ Proceeds to manifest delete!     │
├────────────────────────────────┼──────────────────────────┼──────────────────────┼──────────────────────────────────┤
│ 9. evict_proxy_cached_entry    │ TagReader::list_tags_page│ SUPPRESSED           │ Returns (vec![], None).          │
│    src/manifest_lifecycle:1222 │                          │ (Err(_) ->           │ `has_other_tags` stays false!    │
│                                │                          │  (vec![], None))     │ Deletes if get_manifest succeeds!│
├────────────────────────────────┼──────────────────────────┼──────────────────────┼──────────────────────────────────┤
│ 10. delete_manifest_policy_b   │ TagReader::list_tags_page│ Mixed                │ NotFound -> empty page;          │
│     src/manifest_lifecycle:1390│                          │ (NotFound -> empty,  │ other errors abort via `?`.      │
│     and line 1486              │                          │  others propagate)   │ Pre-delete proof propagates `?`. │
├────────────────────────────────┼──────────────────────────┼──────────────────────┼──────────────────────────────────┤
│ 11. delete_manifest_by_digest  │ list_tag_files           │ Propagated           │ Lists tag files; reads each.     │
│     src/storage/fs.rs:1796     │                          │                      │ Mutates: calls remove_file if    │
│                                │                          │                      │ target matches!                  │
├────────────────────────────────┼──────────────────────────┼──────────────────────┼──────────────────────────────────┤
│ 12. Supervisor::evaluate_      │ TagReader::list_tags     │ SUPPRESSED           │ Fails silently. Semver tag not   │
│     cache_eviction             │                          │ (if let Ok(tags) = ..│ pinned; cached blobs may be      │
│     src/supervisor.rs:940      │                          │  else ignored)       │ prematurely evicted!             │
├────────────────────────────────┼──────────────────────────┼──────────────────────┼──────────────────────────────────┤
│ 13. membership_migration       │ TagReader::list_tags     │ SUPPRESSED           │ Error mapped to empty vec. Repo  │
│     src/membership_migration   │                          │ (.unwrap_or_default()│ skipped; memberships unpopulated.│
│     lines 19, 127, 216         │                          │ )                    │                                  │
└────────────────────────────────┴──────────────────────────┴──────────────────────┴──────────────────────────────────┘
```

### 4.1 Detailed Analysis of Critical Callers & Precision of Consequences

To avoid overstating downstream consequences, we distinguish:
- **Directly Asserted Behavior**: Behavior verified by unit and integration tests.
- **Source-Derived Consequence**: Direct programmatic logic in the immediate caller.
- **Potential Downstream Risk**: Subsequent effect that requires additional conditions and may be mitigated by other layers.

#### 4.1.1 Reference-Index Discovery & Synchronization (`src/blob_ref_index.rs:634`)
- **Directly Asserted Behavior**: `list_tags_page` silently omits corrupt, empty, unreadable, and non-regular tag files (`test_tag_listing_empty_malformed_and_invalid_utf8_taxonomy`).
- **Source-Derived Consequence**: In `discover_repo_manifests_and_tags`, any omitted tag is absent from `DiscoveredRepoData::tags`. During phase 2 application (`sync_repo_manifests_and_tags`, lines 504–512), all existing tags for the repository are removed from `tag_to_root`, and only the discovered tags are inserted. Consequently, during that synchronization, the omitted tag mapping is removed from the index's `tag_to_root` tree. This is not permanently irreversible: a subsequent successful synchronization (e.g., following permission restoration or corruption repair) can restore the tag mapping.
- **Potential Downstream Risk vs. Conditional Protections**:
  - *Risk Characterization*: Does omitting a tag root cause GC to immediately delete the referenced manifest and blobs?
  - *Source-Derived, Conditional Protections*: **No, not automatically.** This characterization does not prove end-to-end GC deletion or safety, but identifies relevant source-derived conditional protections. The repository's manifests are independently enumerated via `list_manifest_digests_page` (lines 547–553) and inserted into `roots` and `root_counts`. As long as the manifest file exists in `repos/<repo>/manifests/`, its root count remains incremented, and its DAG edges to blobs are preserved. Furthermore, `RepositoryBlobMembershipStorage` records blob memberships independently, and GC enforces multi-repo reference checks and candidate grace periods.
  - *Vulnerability Conditions*: Downstream blob reclamation would require additional conditions: the manifest must be unreferenced by any other tag, omitted or deleted from `repos/<repo>/manifests/`, unreferenced across other repositories, and have zero active leases before a subsequent GC pass reclaims it.

#### 4.1.2 Reference-Index Conservative Tag Root Refresh (`src/blob_ref_index.rs:768`)
- **Directly Asserted Behavior**: `list_tags` returns all non-dotfile entry names without reading content.
- **Source-Derived Consequence**: `refresh_tag_rooted_conservative` iterates `repos` and calls `storage.list_tags(&repo).await`. If `StorageError::NotFound` is returned, the repo is skipped via `continue`. If any other error occurs, `return Err(e.into())` aborts the function.
- **Iterative Sled Mutations & Error Propagation**: The function executes `self.ingest_root(storage, &repo, &root).await?` (which writes DAG edges into Sled), and performs `self.tag_to_root.insert(&key, new_val)?` and `self.inc_root_count(...)` during iteration. These mutations are applied directly to Sled trees as each tag is encountered. If an error occurs midway through repository enumeration, the error is immediately propagated via `return Err(e.into())`. This aborts further iteration without rolling back prior successful mutations. The final `self.db.flush()?` (line 798) concerns persistence and disk durability; it is not the point at which in-memory Sled tree mutations become logically applied.

#### 4.1.3 Manifest Lifecycle Proxy Eviction (`src/manifest_lifecycle.rs:1222`)
- **Directly Asserted Behavior**: `list_tags_page` suppresses missing repo errors (`test_tag_listing_missing_and_empty_directories`).
- **Source-Derived Consequence**: In `evict_proxy_cached_entry`, line 1226 explicitly maps any error from `list_tags_page` to `(Vec::new(), None)`. If an error occurs, `has_other_tags` remains `false`.
- **Preceding Mutations**: At Step 4 (lines 1200–1213), tag alias deletion has **already been attempted** via `delete_tag_conditional`, and if successful, `on_tag_deleted` was called and `LifecyclePhase::ProxyTagDeleted` journaled.
- **Guarded Repository Manifest Deletion & Storage Terminology**:
  - At Step 6 (lines 1246–1258), repository-manifest deletion is guarded by two nested conditions:
    1. `if !has_other_tags`: evaluates to true if no remaining tags point to `target_digest`, or if `list_tags_page` errored and returned `(Vec::new(), None)`; and
    2. `if let Ok((_meta, bytes)) = self.storage.get_manifest(repo, target_digest).await`: a successful manifest read.
  - If both conditions are satisfied, the service calls `let _ = self.storage.delete_manifest(repo, target_digest).await;`. Because the return value is ignored (`let _ = ...`), neither the call attempt nor the subsequent `manifest_removed = true;` flag proves that physical deletion on the filesystem succeeded.
  - **Storage Terminology Precision**: `delete_manifest` deletes the **repository-scoped manifest file** (`repos/<repo>/manifests/<target_digest>`), NOT the global CAS content-addressed blob (`cas/blobs/<shard>/<hex>`). The global CAS blob remains untouched by this call.
  - Subsequently, lines 1260–1264 call `idx.on_manifest_deleted(repo, target_digest)`, decrementing root counts for that repository.
  - At Step 7 (line 1280), if `is_blob_referenced_in_repo` confirms no remaining references, proxy blob memberships are unlinked via `self.storage.unlink_repo_blob(repo, blob_d)`. Physical blob deletion in CAS occurs only if a subsequent GC pass finds zero remaining references across all repositories.

#### 4.1.4 Manifest Lifecycle Recovery Replay (`src/manifest_lifecycle.rs:587, 668`)
- **Source-Derived Consequence**: In `recover_pending_journal_under_lock`:
  - `TagsSnapshotted` replay (line 591): `Err(_) => (Vec::new(), None)` maps any listing error to an empty terminal page, ending the tag-enumeration loop without deleting remaining tags. However, recovery itself does not terminate: execution immediately continues to remove referrers (if present), calls `self.storage.delete_manifest(repo, &journal.target_digest)` to delete the repository manifest, reconciles the reference index (`idx.on_manifest_deleted`), flushes the index, and deletes the journal (`self.delete_journal(repo)`).
  - `ProxyTagDeleted` replay (line 672): `Err(_) => (Vec::new(), None)` leaves `has_other_tags` as `false`. Under `if !has_other_tags` (line 690), `get_manifest` is invoked; if it fails, `refs` is set to `None`. Execution then proceeds directly to call `self.storage.delete_manifest(repo, &journal.target_digest)`. Therefore, unlike normal proxy eviction, repository-manifest deletion in this recovery branch does NOT require a successful `get_manifest` or parsed references. Subsequent reference-dependent proxy membership processing is guarded separately by `if let Some(refs) = refs`. In all cases, `delete_manifest` result is ignored with `let _ = ...`, so the call attempt does not prove that physical deletion succeeded.

---

## 5. Platform Support & Contained Reader Architecture

### 5.1 Accurate Non-Linux Analysis in `storage-layer-rust`

A rigorous source inspection of `storage-layer-rust` was conducted to determine how non-Linux platforms are handled:

In `storage-layer-rust/crates/storage-fs/src/reader.rs`:
- Lines 134–164 implement `FsMetadataReader::new` for `#[cfg(target_os = "linux")]` using `libc::open` with `libc::O_DIRECTORY | libc::O_PATH | libc::O_CLOEXEC` to acquire `root_fd`.
- Lines 165–170 define the non-Linux implementation:
  ```rust
  #[cfg(not(target_os = "linux"))]
  {
      let _ = root_path;
      Err(FsMetadataError::PlatformUnsupported)
  }
  ```
- Lines 234–242 and 308–312 repeat this pattern for `from_owned_fd` and payload inspection operations:
  ```rust
  #[cfg(not(target_os = "linux"))]
  {
      let _ = key;
      Err(FsMetadataError::PlatformUnsupported)
  }
  ```

**Conclusion on Platform Claims**:
- There is **no fallback pathname resolution** implemented in `storage-layer-rust` for contained readers or metadata inspection.
- On non-Linux platforms, `FsMetadataReader::new` unconditionally returns `Err(FsMetadataError::PlatformUnsupported)`.
- Consequently, routing `resolve_tag` or future listing operations through `FsMetadataReader` on non-Linux platforms will fail at storage initialization unless a genuine platform containment implementation (or approved fallback) is engineered.
- Non-Linux compilation and execution remain unverified (Quality Gate `O-15` remains **OPEN**).

---

## 6. Pagination Mechanics & Resource Consumption

### 6.1 Source Analysis vs. Experimental Measurements

1. **`list_tags`**:
   - **Source Analysis**: Traverses `repos/<repo>/tags` directory using `tokio::fs::read_dir`. Collects all non-dotfile entry names into `Vec<String>` and calls `tags.sort()`.
   - **Resource Footprint**: Memory consumption is $O(N)$ where $N$ is the total number of entries in `tags/`. Zero file descriptor retention beyond the directory iteration. Zero file reads or digest parsing.
2. **`list_tags_page`**:
   - **Source Analysis**:
     1. Calls `list_tag_files(repo)`: Enumerates all directory entries where `!file_name.starts_with('.')` into `Vec<PathBuf>`.
     2. Sequentially executes `tokio::fs::read_to_string(&path)` for every entry in `Vec<PathBuf>`.
     3. Parses each read content with `Digest::parse(content.trim())`.
     4. Collects valid pairs into `Vec<(String, Digest)>`.
     5. Executes `tags_with_digest.sort_by(...)`.
     6. Performs binary search on `continuation_token` to locate `start_idx`.
     7. Slices `[start_idx..end_idx]` and extracts `next_token = page_slice.last()`.
   - **Resource Footprint**:
     - Memory: $O(N)$ allocations for `PathBuf`, file contents, and `Digest` structs.
     - I/O: $N$ file opens, reads, and closes on **every single page request**. Fetching a repository with 5,000 tags in pages of 100 requires $50 \times 5,000 = 250,000$ file opens and reads.
     - CPU: $N$ SHA-256 string parses and vector sorts per page request.
3. **Continuation Token Contracts**:
   - The token is the plain UTF-8 tag name string of the last entry in the returned page (`next_token = page_slice.last().map(|(t, _)| t.clone())`).
   - Binary search uses raw Rust string slice comparison (`t.as_str().cmp(token)`).
   - If `continuation_token` is missing or between anchors, `binary_search_by` returns `Err(idx)`, and pagination resumes from `idx` (insertion point).
   - Structurally unusual tokens (`""`, slashes, spaces, emoji, embedded null bytes, 10KB strings) undergo **zero schema or structural validation**; they are simply evaluated as lexical probe points in the sorted list.
4. **Limits**:
   - `self.manifest_listing_limits` is **not** consulted or enforced (`test_tag_listing_ignores_configured_manifest_enumeration_limits`).
   - If `page_limit == 0`, `start_idx = 0`, `end_idx = 0`, returning `([], None)` immediately.

---

## 7. Remaining Uncertainties & Missing Evidence

1. **Gate O-15 (Non-Linux Verification)**:
   All characterization tests and openat2 containment guarantees were executed and validated on Linux. Non-Linux platforms return `PlatformUnsupported` in `storage-layer-rust` and remain unverified.
2. **Permission Denied Verification Under Unprivileged Execution**:
   `test_tag_listing_permission_denied` is explicitly marked `#[ignore]` because `chmod 0o000` cannot deny file access in privileged or root test containers. When executed in an unprivileged environment via `-- --ignored`, both file-level and directory-level permission denials succeed.
3. **Special Files and Non-Regular Objects**:
   FIFOs, character devices, and domain sockets created in `tags/` cause `list_tags` to return their filenames. In `list_tags_page`, `read_to_string` on a FIFO may block depending on opening flags and the presence of writers, but this has not been experimentally verified in an isolated test fixture.
4. **Lack of Snapshot Isolation Across Mutating Callers**:
   Neither listing method provides snapshot isolation. Concurrent tag writes or deletions during pagination can cause duplicate, omitted, or inconsistent tag observations.

---

## 8. Proposed Follow-Up Design Slice: Contained Tag Listing

### 8.1 Scope of the Smallest Safe Slice

The smallest subsequent engineering slice should introduce a contained directory reader seam for tags in `registry-rust`, backed by `storage-layer-rust` contained directory iteration:
1. **Directory Containment**: Enumerate `repos/<repo>/tags/` beneath `root_fd` using contained directory descriptor resolution (`openat2` with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS`).
2. **Payload Containment**: When resolving `(tag, digest)` pairs for `list_tags_page`, read tag contents through `self.reader` rather than ambient `tokio::fs::read_to_string`.
3. **Strict Path Validation**: Enforce canonical repository and tag name validation prior to descriptor acquisition.

### 8.2 Explicit Compatibility Decisions Required

Promoting contained tag listing requires explicit architectural decisions from registry maintainers:
1. **Repository NotFound Parity**:
   Should `list_tags_page` be aligned with `list_tags` to return `StorageError::NotFound` when the repository does not exist, or must it preserve legacy `Ok((vec![], None))` for backward compatibility with existing lifecycle recovery callers?
2. **Malformed and Corrupt Tag Handling in `list_tags_page`**:
   Should `list_tags_page` continue silently dropping corrupt/empty/unreadable tag files (matching current behavior), or should it return `StorageError::CorruptData` / propagate I/O errors (which would fail caller discovery closed, but could abort lifecycle recovery loops)?
3. **Subdirectories in `tags/`**:
   Should `list_tags` continue returning subdirectories as tags, or should it filter for regular files only (matching `list_tags_page`'s effective behavior)?
4. **Symlink Rejection**:
   Should tag listing reject symlinks inside `tags/` (aligning with `resolve_tag`), or must symlinks remain visible in `list_tags`?
5. **Pagination Resource Limits**:
   Should tag listing introduce a configurable ceiling on maximum tags scanned per repository to avoid unbounded in-memory allocations and repeated full directory re-reads?

---

## 9. Conclusion

The production tag listing methods (`list_tags` and `list_tags_page`) currently operate entirely outside the contained reader architecture. They bypass root pinning, follow symlinks ambiently, lack snapshot isolation, and exhibit conflicting error and contract semantics.

All 13 active characterization tests and 1 ignored permission test verify these current behaviors without altering production code. All 8 canonical quality gates remain explicitly **OPEN**.
