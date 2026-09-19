> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. References `list_tag_files`, which no longer exists; contained tag listing landed (`f1d6d9c`) and later moved onto `tag_domain` (`32c42c6`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Architecture Design: Contained Filesystem Tag Listing Integration (Corrected)

**Repository:** `registry-rust`
**Target Path:** `docs/architecture/filesystem-tag-listing-contained-integration-design.md`
**Authoritative Baselines:**
- `registry-rust` HEAD: `6d13cd983e10c15fa806d2d23385ae5752fe6362`
  - Parent: `5a0b4246e4c24e8db56d84f047eadae5387e7330`
  - Latest Commit: `test(storage): characterize filesystem tag listing semantics`
- `storage-layer-rust` HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (strictly read-only)

**Scope:** Architectural design for an isolated, test-only integration seam and subsequent production prerequisites for descriptor-relative contained filesystem tag listing beneath the pinned root descriptor via `storage_fs::FsMetadataReader`.
**Status:** **DESIGN ONLY — NOT IMPLEMENTED — NOT COMMITTED**.
**Canonical Quality Gates:** All eight quality gates remain explicitly **OPEN**:
- `O-03`: Key and continuation-token contracts.
- `O-04`: Filesystem write durability and containment.
- `O-05`: Broader filesystem read containment.
- `O-06`: Typed AWS mapping and pinned-MinIO evidence.
- `O-13`: Hosting, distribution, and release strategy.
- `O-15`: Non-Linux verification.
- `O-16`: Earlier Slice 11 audit/test-inventory evidence.
- `D-06`: Broader extraction, cutover, compatibility, and distribution acceptance.

---

## 1. Executive Summary & Authorization Boundary

In prior storage extraction milestones, descriptor-relative containment via Linux `openat2` flags (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`) was incrementally designed, tested, and integrated for CAS blobs and manifests. Point-in-time tag resolution (`resolve_tag`) and optimistic-concurrency versioned tag retrieval (`get_tag_with_version`) were cut over in [`src/storage/fs/tag_read.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/tag_read.rs) (commit `5a0b4246e4c24e8db56d84f047eadae5387e7330`), followed by characterization of legacy listing semantics in commit `6d13cd983e10c15fa806d2d23385ae5752fe6362` ([`docs/architecture/filesystem-tag-listing-characterization.md`](file:///home/dietmar/devel/rust/registry-rust/docs/architecture/filesystem-tag-listing-characterization.md)).

### 1.1 Authorization Status: Design Only
This document establishes an architectural design, probe analysis, and caller matrix for contained tag listing. **This task authorizes a design, not new production policies**:
- **No Production Policy Approval**: Approval of earlier tag-read changes does not automatically approve tag-listing changes.
- **Explicit Labeling**: All design choices are strictly categorized as either:
  - **Existing behavior proposed for preservation**; or
  - **Proposed change — requires approval**.
- **No Production Modifications**: Production `FsStorage::list_tags` and `FsStorage::list_tags_page` remain completely unchanged. No code is modified, staged, committed, or pushed in this slice.

### 1.2 The Problem: Uncontained Pathname Enumeration
While single-tag reads operate beneath the pinned root directory descriptor:
1. **Bypass of Contained Reader**: [`FsStorage::list_tags`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L950-L984) and [`FsStorage::list_tags_page`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1169-L1214) bypass `self.reader: Arc<storage_fs::FsMetadataReader>` and execute uncontained ambient pathname operations starting from `self.root: PathBuf`.
2. **Path Traversal & Symlink Follow**: Raw path composition does not validate repository strings. Intermediate `..` segments escape the storage root, and symlinks inside the repository are traversed transparently by the OS kernel.
3. **Split-Brain Namespace Divergence**: Pinned reads (`resolve_tag`, `get_tag_with_version`) remain attached to the original directory descriptor (Tree A), while ambient listing and mutation operations observe replacement directories (Tree B) after external renames.
4. **Asymmetric Error and Omission Semantics**:
   - `list_tags` verifies repository directory metadata, returning `StorageError::NotFound` if missing, but returns names of subdirectories, dangling symlinks, and FIFOs without reading them.
   - `list_tags_page` delegates to `list_tag_files`, which queries `tags/` directly without checking repository directory existence, silently returning `Ok((vec![], None))` for nonexistent repositories. Furthermore, `list_tags_page` silently drops any unreadable, empty, malformed, or non-UTF-8 tag file without error.

---

## 2. Source-Grounded Architectural Inspection & Current Baseline

### 2.1 Production Implementations in `src/storage/fs.rs`

#### 2.1.1 `FsStorage::list_tags` (`src/storage/fs.rs:950-984`)
```rust
// src/storage/fs.rs:950-984
    async fn list_tags(&self, name: &str) -> Result<Vec<String>, StorageError> {
        let repo_dir = self.root.join("repos").join(name);
        match tokio::fs::metadata(&repo_dir).await {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(err) => return Err(StorageError::io(err.to_string())),
        }

        let tags_dir = repo_dir.join("tags");
        let mut dir = match tokio::fs::read_dir(&tags_dir).await {
            Ok(d) => d,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(StorageError::io(err.to_string())),
        };

        let mut tags = Vec::new();
        loop {
            match dir.next_entry().await {
                Ok(Some(entry)) => {
                    if let Some(file_name) = entry.file_name().to_str() {
                        if !file_name.starts_with('.') {
                            tags.push(file_name.to_string());
                        }
                    }
                }
                Ok(None) => break,
                Err(err) => return Err(StorageError::io(err.to_string())),
            }
        }

        tags.sort();
        Ok(tags)
    }
```
**Key Baseline Facts**:
- Checks repository directory existence via `tokio::fs::metadata(&repo_dir)`. If nonexistent, returns `Err(StorageError::NotFound)`.
- If `tags/` subfolder is missing, returns `Ok(Vec::new())`.
- Does not inspect entry file types; subdirectories, symlinks, and FIFOs are included if their name does not start with `.`.
- Does not read file content or parse digests.
- Does not delegate to `list_tag_files`.

#### 2.1.2 `FsStorage::list_tag_files` (`src/storage/fs.rs:646-669`) and `list_tags_page` (`src/storage/fs.rs:1169-1214`)
```rust
// src/storage/fs.rs:646-669
    async fn list_tag_files(&self, name: &str) -> Result<Vec<PathBuf>, StorageError> {
        let tags_dir = self.root.join("repos").join(name).join("tags");
        let mut dir = match tokio::fs::read_dir(&tags_dir).await {
            Ok(d) => d,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(StorageError::io(err.to_string())),
        };

        let mut files = Vec::new();
        loop {
            match dir.next_entry().await {
                Ok(Some(entry)) => {
                    if let Some(file_name) = entry.file_name().to_str() {
                        if !file_name.starts_with('.') {
                            files.push(entry.path());
                        }
                    }
                }
                Ok(None) => break,
                Err(err) => return Err(StorageError::io(err.to_string())),
            }
        }
        Ok(files)
    }

// src/storage/fs.rs:1169-1214
    async fn list_tags_page(
        &self,
        repo: &str,
        continuation_token: Option<&str>,
        page_limit: usize,
    ) -> Result<(Vec<(String, Digest)>, Option<String>), StorageError> {
        let tag_files = self.list_tag_files(repo).await?;
        let tags_dir = self.root.join("repos").join(repo).join("tags");

        let mut tags_with_digest: Vec<(String, Digest)> = Vec::new();
        for path in tag_files {
            let rel = match path.strip_prefix(&tags_dir) {
                Ok(r) => r.to_string_lossy().to_string(),
                Err(_) => continue,
            };
            if rel.starts_with(".tmp.") || rel.starts_with(".lock.") {
                continue;
            }
            if let Ok(content) = tokio::fs::read_to_string(&path).await
                && let Ok(digest) = Digest::parse(content.trim())
            {
                tags_with_digest.push((rel, digest));
            }
        }
        tags_with_digest.sort_by(|a, b| a.0.cmp(&b.0));

        let start_idx = if let Some(token) = continuation_token {
            match tags_with_digest.binary_search_by(|(t, _)| t.as_str().cmp(token)) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            }
        } else {
            0
        };

        let end_idx = (start_idx + page_limit).min(tags_with_digest.len());
        let page_slice = &tags_with_digest[start_idx..end_idx];

        let next_token = if end_idx < tags_with_digest.len() {
            page_slice.last().map(|(t, _)| t.clone())
        } else {
            None
        };

        Ok((page_slice.to_vec(), next_token))
    }
```
**Key Baseline Facts**:
- `list_tag_files` probes `self.root.join("repos").join(name).join("tags")` directly without checking repository directory existence. If `repos/<repo>` is missing, `tokio::fs::read_dir` returns `NotFound`, which is swallowed as `Ok(Vec::new())`. Hence, `list_tags_page` returns `Ok((vec![], None))` for nonexistent repositories, suppressing `StorageError::NotFound`.
- For each path in `tag_files`, `list_tags_page` attempts `tokio::fs::read_to_string(&path)` and `Digest::parse(content.trim())`. Read errors (`EISDIR` on subdirectories, permission denials) and parse errors (empty file, invalid length, non-hex) are silently omitted via `if let Ok(...)`.
- Full-scan in memory: every tag file is read into memory and sorted before applying continuation tokens or page slicing.
- `start_idx + page_limit` uses unchecked addition on `usize`.

### 2.2 Retention of `list_tag_files` for Mutation Path (`delete_manifest`)
A critical dependency in `src/storage/fs.rs` is that **`list_tag_files` cannot be deleted during listing cutover**.
In [`src/storage/fs.rs:1796-1805`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs#L1796-L1805), `FsStorage::delete_manifest` calls `self.list_tag_files`:
```rust
// src/storage/fs.rs:1794-1805
        // Remove any tags pointing to this digest.
        let digest_str = digest.as_str();
        for path in self.list_tag_files(name).await? {
            let content = match tokio::fs::read_to_string(&path).await {
                Ok(s) => s,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(StorageError::io(err.to_string())),
            };
            if content.trim() == digest_str {
                let _ = tokio::fs::remove_file(&path).await;
            }
        }
```
**Requirement**: `list_tag_files` must be retained for the `delete_manifest` mutation path. Refactoring mutation paths is strictly outside this read-containment slice.

### 2.3 Contained Tag Read Helpers & Scope in `src/storage/fs/tag_read.rs`
1. **Helper Visibility**:
   In `src/storage/fs/tag_read.rs`, `validate_path_component` is a private function:
   ```rust
   // src/storage/fs/tag_read.rs:94
   fn validate_path_component(component: &str, field_name: &str) -> Result<(), StorageError>
   ```
   A sibling module cannot invoke `tag_read::validate_path_component` as written without modifying production source code.
2. **Validation Rules**:
   `validate_path_component` checks:
   - Empty string.
   - Leading or trailing `/`.
   - Backslashes `\\`.
   - NUL bytes and ASCII control characters: `c == '\0' || c.is_ascii_control()`. It does **not** check non-ASCII control characters.
   - Repeated slashes `//`.
   - Segments equal to `.` or `..`.
3. **Payload Stream Draining & Limit Mapping**:
   In `src/storage/fs/tag_read.rs:143-182` (`drain_tag_stream`):
   - Stream read failures map to `StorageError::io(...)` (`StorageErrorKind::Io`).
   - Payload limit overflow/excess maps to `StorageError::corrupt_data(...)` (`StorageErrorKind::CorruptData`).
   - `limits.max_payload_bytes == None` means no seam-imposed payload ceiling (the production default).
4. **Existing Payload Error Translation**:
   In [`src/storage/fs/read_adapter.rs:93-135`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/read_adapter.rs#L93-L135), `translate_read_error` translates:
   - `storage_core::ReadError::NotFound` -> `StorageError::NotFound`.
   - `storage_core::ReadError::PermissionDenied` -> `StorageError::io(...)`.
   - `storage_core::ReadError::Backend` wrapping `FsMetadataError::ResolutionRejected` -> `StorageError::io(...)`.
   - `storage_core::ReadError::Backend` wrapping `FsMetadataError::UnsupportedObjectType` -> `StorageError::io(...)`.
   - `storage_core::ReadError::Backend` wrapping `FsMetadataError::SyscallUnsupported` -> `StorageError::configuration(...)`.

### 2.4 Contained Directory Enumeration Primitives in `storage_fs`
In `crates/storage-fs/src/dir.rs:527-533`:
```rust
        if name_bytes == b"." || name_bytes == b".." {
            continue;
        }

        let name_len = name_bytes.len();
        let new_total = account_entry(entries.len(), total_name_bytes, name_len, &limits)?;
```
**Budget Accounting**:
`b"."` and `b".."` are explicitly skipped before `account_entry` is evaluated. Therefore, `.` and `..` **do not count against `max_entries` or `max_total_name_bytes`**.

---

## 3. Integration Seam Architecture & Probe Analysis

### 3.1 Module Placement & Test-Local Path Validation
- **Placement**: Gated under `#[cfg(test)]` in `src/storage/fs/tag_listing.rs`.
- **Helper Approach Without Modifying Production Source**:
  Because `tag_read::validate_path_component` is private, the test-seam module defines a test-local validator matching the same validation contract within its `#[cfg(test)]` scope. Extracting `validate_path_component` into a shared `pub(crate)` module (e.g. `src/storage/fs/path_safety.rs`) is a separate proposed production source change requiring future approval.

### 3.2 Distinguishing Symlink Path Containment from Symlink Child Entries
- **Symlinks in Directory Path**:
  `openat2` resolves `repos/<repo>/tags` with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`. If `repos`, `<repo>`, or `tags` is a symlink, `openat2` immediately rejects path resolution with `ELOOP` or `EXDEV` (`FsDirError::ResolutionRejected`), failing the entire call with `StorageError::io`.
- **Symlinks Returned as Entries During Enumeration**:
  Once `tags/` is acquired, `readdir` yields child entries. A child entry may have point-in-time observed type `DirEntryType::Symlink`.
  - The presence of a symlink entry does not fail directory enumeration.
  - Candidate filtering skips non-regular entries (`DirEntryType::Symlink`).

### 3.3 Contained Repository-Existence Probe Analysis
`enumerate_dir` is a full bounded enumeration operation, not an open-only existence probe. Setting limits to `DirEnumerationLimits::new(1, 128)` will fail with `FsDirError::LimitExceeded` whenever an existing repository contains more than one entry (`manifests/`, `tags/`, `revisions/`, etc.).

#### Approach A: Bounded Enumeration Probe (Using Available APIs)
- The caller supplies an explicit probe budget: `repo_probe_limits: DirEnumerationLimits`.
- Probe executes `reader.enumerate_dir(Some(&repo_key), repo_probe_limits).await`.
- **Cost Accounting**: Acquires `repos/<repo>` via `openat2`, converts to `DIR*` with `fdopendir`, performs iterative `readdir`, executes descriptor-relative `fstatat` type inspections only for entries requiring type fallback (such as `DT_UNKNOWN`), rather than necessarily every entry, and allocates `Vec<DirEntry>`.
- **Error Mapping**:
  - `FsDirError::NotFound`: Represents an acquisition-time observation that `repos/<repo>` does not exist at the instant of directory resolution, rather than permanent proof of nonexistence. `list_tags` returns `StorageError::NotFound`.
  - `FsDirError::LimitExceeded`: Does **not** swallow or assume existence. Propagates as `StorageError::backend(...)` (`StorageErrorKind::Backend`).
    - *Compatibility Change*: Legacy `list_tags` used `tokio::fs::metadata(&repo_dir)` (`stat()`), which never failed due to entry counts. Under an enumeration probe, an existing repository with more entries than `repo_probe_limits` fails closed with a backend error (*Proposed change — requires approval*).
  - `FsDirError::PermissionDenied` / `ResolutionRejected` / `Io`: Propagates according to standard directory error mapping.
- **TOCTOU Race**: The existence of `repos/<repo>` and the state of `repos/<repo>/tags` are separate observations. An unavoidable race remains: a repository could be created or removed concurrently between separate observations (e.g. concurrent `rm -rf repos/<repo>` or `mkdir repos/<repo>/tags` between the probe and opening `tags/`).

#### Approach B: Proposed Future Capability: Open-Only Directory Probe
As a future enhancement to `storage-layer-rust`:
- An open-only descriptor capability on `FsMetadataReader` (e.g. `probe_dir_exists(target: Option<&ObjectKey>) -> Result<bool, FsDirError>`).
- Executes `openat2` with `O_DIRECTORY | O_PATH` without `fdopendir`, `readdir`, or type inspections.
- *Status*: **Proposed prerequisite for future cutover — not currently available in storage-layer-rust**.

---

## 4. One Explicit Recommended Test-Seam Contract

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Explicit Recommended Test-Seam Contract Flow                                                     │
├──────────────────────────────────────────────────────────────────────────────────────────────────┤
│ 1. Repository Validation:                                                                        │
│    - Test-local validator rejects empty strings, leading/trailing '/', '\\', NUL bytes,          │
│      ASCII controls, empty segments, and '.' / '..' traversals with StorageError::InvalidRepoName.│
│                                                                                                  │
│ 2. Contained Directory Acquisition (repos/<repo>/tags):                                          │
│    - openat2 on repos/<repo>/tags with RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS.                    │
│    - Symlinks in path fail closed (ResolutionRejected -> StorageError::io).                      │
│    - If ENOENT (NotFound): probe repos/<repo> via contained enumerate_dir(repo_probe_limits):     │
│      * If repo missing: list_tags returns StorageError::NotFound.                                │
│        list_tags_page returns Ok((Vec::new(), None)) (preserving legacy behavior;              │
│        aligning list_tags_page to NotFound is a proposed change requiring approval).            │
│      * If repo exists but tags/ missing: returns Ok(Vec::new()) / Ok((Vec::new(), None)).       │
│      * If probe returns LimitExceeded: propagates StorageError::backend (no silent swallow).     │
│    - PermissionDenied / SyscallUnsupported / PlatformUnsupported fail closed.                    │
│    - Zero-page policy: Directory acquisition and enumeration ARE performed when page_limit == 0, │
│      surfacing directory permission and missing-repo errors. Payload reads are skipped.          │
│                                                                                                  │
│ 3. Candidate Filtering:                                                                          │
│    - Skip entries starting with '.'.                                                             │
│    - Skip invalid UTF-8 entry names.                                                             │
│    - Filter for point-in-time observed DirEntryType::Regular.                                    │
│    - Non-regular entries (DirEntryType::Directory, Symlink, Other) are skipped from the         │
│      candidate list during iteration.                                                            │
│                                                                                                  │
│ 4. Payload Acquisition (Digest-Bearing Listing Only):                                            │
│    - When page_limit == 0: skip payload acquisition entirely (returns Ok((Vec::new(), None))).   │
│    - For regular candidates: open payload via reader.open_payload(&tag_key).                     │
│    - If NotFound (entry concurrently deleted between readdir and open): omit candidate.          │
│    - If PermissionDenied: fail closed with StorageError::io (via translate_payload_read_error).   │
│    - If ResolutionRejected (entry replaced with symlink): fail closed with StorageError::io.     │
│    - If UnsupportedObjectType (entry replaced with dir/FIFO/socket): fail closed with            │
│      StorageError::io("unsupported object type (mode: ...)").                                    │
│                                                                                                  │
│ 5. Stream Draining & Parsing:                                                                    │
│    - Drain payload stream up to caller-supplied TagReadLimits::max_payload_bytes.                │
│    - Stream read error -> StorageError::io (via drain_tag_stream).                               │
│    - Stream limit overflow -> StorageError::corrupt_data (via drain_tag_stream).                 │
│    - Invalid UTF-8 bytes -> StorageError::io (fail closed; proposed change requiring approval).  │
│    - Parse digest via Digest::parse(content.trim()).                                             │
│      * If corrupt or empty -> omit candidate from page (preserving legacy tolerance).            │
│                                                                                                  │
│ 6. Sorting & Pagination:                                                                         │
│    - In-place lexical sort: tags_with_digest.sort_unstable_by(|a, b| a.0.cmp(&b.0)).             │
│    - Binary search on continuation_token using raw whole-string lexical comparison.              │
│    - Slicing using saturating arithmetic: start_idx.saturating_add(page_limit).min(len).         │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 4.1 Proposed Seam Function Signatures

To isolate contained listing logic, decouple dependencies, and enable thorough test-double injection without modifying production code, the test seam defines two entry-point functions accepting explicit directory enumeration budgets and payload acquisition dependencies:

```rust
/// Proposed test seam for contained name-only tag listing (`list_tags`).
pub(crate) async fn contained_list_tags_seam<D>(
    dir_enumerator: &D,
    repo_name: &str,
    repo_probe_limits: storage_fs::DirEnumerationLimits,
    tags_dir_limits: storage_fs::DirEnumerationLimits,
) -> Result<Vec<String>, StorageError>
where
    D: TagDirEnumerator + ?Sized;

/// Proposed test seam for contained digest-bearing paginated tag listing (`list_tags_page`).
pub(crate) async fn contained_list_tags_page_seam<D, P>(
    dir_enumerator: &D,
    payload_reader: &P,
    repo_name: &str,
    cursor: Option<&str>,
    page_limit: usize,
    repo_probe_limits: storage_fs::DirEnumerationLimits,
    tags_dir_limits: storage_fs::DirEnumerationLimits,
    payload_limits: TagReadLimits,
) -> Result<(Vec<TagSummary>, Option<String>), StorageError>
where
    D: TagDirEnumerator + ?Sized,
    P: TagPayloadReader + ?Sized;
```

- **`repo_probe_limits`**: Caller-supplied budget enforced when probing `repos/<repo>` on absent `tags/` directory.
- **`tags_dir_limits`**: Caller-supplied budget enforced when enumerating `repos/<repo>/tags`.
- **`payload_limits`**: Caller-supplied byte limit for draining payload streams (`max_payload_bytes`).
- **`payload_reader`**: Abstracted payload reader dependency, omitted entirely from `contained_list_tags_seam` to guarantee that name-only listing never acquires or reads tag payloads.

---

## 5. Explicit Compatibility Decision Table

| # | Case | Current Legacy Behavior | Proposed Seam Behavior | Eventual Production Target | Decision Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | **Missing Repository** | `list_tags`: `NotFound`<br>`list_tags_page`: `Ok(([], None))` | `list_tags`: `NotFound`<br>`list_tags_page`: `Ok(([], None))` | Align `list_tags_page` to `NotFound` once callers hardened | **Proposed change — requires approval** |
| 2 | **Missing `tags/` in Existing Repo** | `list_tags`: `Ok([])`<br>`list_tags_page`: `Ok(([], None))` | Returns `Ok([])` / `Ok(([], None))` | Preserve empty success | **Existing behavior proposed for preservation** |
| 3 | **Empty `tags/` Directory** | Returns `Ok([])` / `Ok(([], None))` | Returns `Ok([])` / `Ok(([], None))` | Preserve empty success | **Existing behavior proposed for preservation** |
| 4 | **Structural Path Validation** | Accepts `..`, leading slashes, backslashes | Rejects `..`, `/`, `\`, NUL, ASCII controls | Adopt structural validation | **Proposed change — requires approval** |
| 5 | **Dotfiles & Lock Files** | Filters names starting with `.` | Filters names starting with `.` | Preserve filtering | **Existing behavior proposed for preservation** |
| 6 | **Nested Subdirectories** | `list_tags`: returns name<br>`list_tags_page`: omitted (`EISDIR`) | Filters out `DirEntryType::Directory` | Exclude non-regular entries | **Proposed change — requires approval** |
| 7 | **Non-UTF-8 Entry Names** | `list_tags` & `list_tags_page`: omitted | Skips non-UTF-8 entry names | Preserve omission | **Existing behavior proposed for preservation** |
| 8 | **Symlinks in Path to Directory** | Followed by OS kernel | `openat2` fails with `ResolutionRejected` | Fail closed on symlink path components | **Proposed change — requires approval** |
| 9 | **Symlink Child Entries in `tags/`** | `list_tags`: returns name<br>`list_tags_page`: followed | Seam skips `DirEntryType::Symlink` | Exclude non-regular entries from listing | **Proposed change — requires approval** |
| 10 | **Non-Regular Objects (FIFOs)** | `list_tags`: returns name<br>`list_tags_page`: unverified | Seam skips `DirEntryType::Other` | Exclude non-regular objects | **Proposed change — requires approval** |
| 11 | **Unreadable Tag Files (`EACCES`)** | `list_tags`: returns name<br>`list_tags_page`: omitted | `list_tags` returns name; `list_tags_page` fails closed (`StorageError::io`) | Fail closed once callers hardened | **Proposed change — requires approval** |
| 12 | **Invalid UTF-8 in Tag Payload** | `list_tags_page`: silently omitted | `list_tags_page` fails closed (`StorageError::io`) | Fail closed on invalid UTF-8 | **Proposed change — requires approval** |
| 13 | **Corrupt/Empty Tag Payloads** | `list_tags`: returns name<br>`list_tags_page`: omitted | `list_tags` returns name; `list_tags_page` omits candidate | Determine omission vs fail-closed | **Existing behavior proposed for preservation** |
| 14 | **Continuation Tokens** | Raw lexical string comparison | Retains raw whole-string lexical comparison | Preserve continuation token semantics | **Existing behavior proposed for preservation** |
| 15 | **Page Limit = 0 Handling** | `list_tags_page` reads all files, slices to 0 | Enumerates directory, skips payload reads, slices to 0 | Enforce directory checks while saving I/O | **Proposed change — requires approval** |
| 16 | **Pagination Arithmetic Overflow** | Unchecked addition `start_idx + page_limit` | Saturating addition `saturating_add` | Prevent overflow | **Proposed change — requires approval** |
| 17 | **Concurrent Disappearance** | `next_entry()` error returns `io` | Payload `open` missing -> omit candidate | Safe omission on disappearance | **Proposed change — requires approval** |
| 18 | **Acquisition-Time Replacement** | `read_to_string` error omitted | `open_payload` fails closed (`StorageError::io`) | Fail closed on concurrent type replacement | **Proposed change — requires approval** |
| 19 | **Probe Budget Exhaustion** | N/A (`stat()` does not count entries) | Propagates `StorageError::backend` | Propagate budget exhaustion | **Proposed change — requires approval** |
| 20 | **Root Replacement Divergence** | Pinned reads observe Tree A; listing observes Tree B | Contained listing observes pinned Tree A | Align all reads with pinned Tree A | **Proposed change — requires approval** |

---

## 6. Precise Resource Bounds & Operating Boundaries

1. **Production Tag Read Limits**:
   In production, `FsStorage::resolve_tag` and `get_tag_with_version` use `TagReadLimits { max_payload_bytes: None }`. Any finite payload limit in the test seam is an **experimental test parameter**, not an approved production limit.
2. **Raw Filename Budgets Exclude**:
   `DirEnumerationLimits::max_total_name_bytes` accounts **only** the byte lengths of entry names passed to `account_entry`. It explicitly excludes:
   - In-memory heap allocations for `DirEntry` or `Vec<DirEntry>`.
   - String and `Digest` object allocations.
   - Slicing and result vector copies.
   - Payload stream buffers.
   - Concurrent listing requests.
3. **Handling of `.` and `..`**:
   As verified in `crates/storage-fs/src/dir.rs:527`, `b"."` and `b".."` are skipped before `account_entry`. They do **not** consume enumeration limits.
4. **Point-in-Time Observations**:
   Entry types observed during `readdir` are point-in-time observations. An entry observed as `Regular` may be replaced with a directory or symlink before payload acquisition.
5. **Digest Syntax Bounds**:
   Valid OCI digest syntax consists of an algorithm identifier (`sha256:`, `sha512:`) followed by hex characters, with optional leading and trailing whitespace stripped by `trim()`. Comments are not valid digest syntax. SHA-512 with whitespace padding can exceed 130 bytes.
6. **Snapshot & Mount Boundaries**:
   Root descriptor pinning provides neither mount isolation nor namespace snapshot consistency. Iterative `readdir` calls observe concurrent mutations.

---

## 7. Caller Error Handling as a Strict Production Prerequisite

Every caller of `list_tags` and `list_tags_page` was inspected in `registry-rust`.

### 7.1 Conditional Caller Behavior Analysis

#### 7.1.1 Application Tag Service (`src/application/tags.rs:49-52`)
The service uses [`TagQueryError`](file:///home/dietmar/devel/rust/registry-rust/src/application/errors.rs#L232-L245) with variants `InvalidRepoName`, `NotFound`, `Storage`, and `Internal`. It maps `StorageError::NotFound` to `TagQueryError::NotFound` (HTTP 404), and other errors to `TagQueryError::Storage` (HTTP 500).

#### 7.1.2 Reference Index Sync (`src/blob_ref_index.rs:501-530, 632-652`)
- **Staged Discovery**: `list_tags_page` errors abort discovery via `?` before Phase 2.
- **Sled Deletion on Omission**: In Phase 2, all existing tags for `repo` are removed from `tag_to_root` before inserting staged tags. If discovery succeeds while omitting an unreadable tag, that tag is **deleted from the reference index**. A subsequent sync could restore it if readable.

#### 7.1.3 Conservative Tag Refresh (`src/blob_ref_index.rs:768-795`)
Mutations (`tag_to_root.insert`, `inc_root_count`) occur **directly inside the loop**. If an error occurs on tag $N$, earlier tags $1..N-1$ remain modified in sled memory. `self.db.flush()?` guarantees fsync durability, not transactional rollback.

#### 7.1.4 Lifecycle `TagsSnapshotted` Recovery (`src/manifest_lifecycle.rs:585-605`)
Listing errors are swallowed into an empty page `(Vec::new(), None)`. This ends the tag deletion loop. Recovery continues to attempt downstream mutations (such as calling `self.storage.delete_manifest` and `idx.on_manifest_deleted`), though ignored results mean physical filesystem deletion is not proven.

#### 7.1.5 Lifecycle `ProxyEvict` Recovery (`src/manifest_lifecycle.rs:666-705`)
Listing errors are swallowed, causing `has_other_tags` to remain `false`. Execution proceeds to attempt `delete_manifest` under `!has_other_tags`, even if `get_manifest` fails and `refs` becomes `None`.

#### 7.1.6 Normal Proxy Eviction (`src/manifest_lifecycle.rs:1247-1254`)
In normal eviction, `delete_manifest` is attempted only if **both** `!has_other_tags` is true and `get_manifest` succeeds.

#### 7.1.7 Supervisor Cache Eviction Protection (`src/supervisor.rs:940-960`)
If `storage.list_tags(&repo)` fails, `pinned_tags` is not populated for that rule. However, the entry is not necessarily evicted: other rules (e.g. `KeepTags`) or independent reference index protections may still protect the underlying blobs.

#### 7.1.8 Membership Migration (`src/membership_migration.rs:127, 164-173, 214-226`)
Listing errors are swallowed via `.unwrap_or_default()`. Tags in that repository are skipped, and the repository continuation token advances. Final transition to `MigrationPhase::Ready` depends on `verify_membership_migration`, which also swallows listing errors; discrepancies in unlisted tags would be missed, but non-empty verification results on other repositories or ledger state could still fail the migration.

### 7.2 Ordered Sequence of Follow-Up Engineering Slices
1. **Slice 1: Test-Only Contained Tag Listing Seam** *(Design Target)*:
   Isolated under `#[cfg(test)]` in `src/storage/fs/tag_listing.rs`. Validates contained enumeration, candidate filtering, payload streaming, and budget boundaries. Zero production impact.
2. **Slice 2: Caller Error Hardening (Strict Production Prerequisite)**:
   - Harden `TagsSnapshotted` and `ProxyEvict` recovery to abort on listing errors rather than proceeding with manifest deletion.
   - Harden `membership_migration.rs` to fail closed on storage errors rather than skipping tags and advancing checkpoints.
   - Harden `supervisor.rs` cache eviction against listing failure.
3. **Slice 3: Policy Alignment on Corrupt/Missing Tag Semantics**:
   - Formal decision on missing repository error vs empty page.
   - Formal decision on corrupt tag payload omission vs fail-closed error.
   - Proposal to extract `validate_path_component` into a shared `pub(crate)` path safety helper.
4. **Slice 4: Production Wiring & Cutover**:
   - Wire `FsStorage::list_tags` and `list_tags_page` to contained implementations.
   - Retain `list_tag_files` for `delete_manifest`.
   - Validate performance and heap consumption under load.

---

## 8. Actionable Verification Plan (Planned Test Seam Tests)

The following tests are planned for the test-seam implementation slice (`src/storage/fs/tests/tag_listing_seam_tests.rs` or `src/storage/fs/tests.rs`). **No tests have been executed in this documentation-only slice**.

### 8.1 Proposed Test Doubles & Injection Signatures

```rust
// Proposed test-seam directory enumeration abstraction
#[async_trait::async_trait]
pub(crate) trait TagDirEnumerator: Send + Sync {
    async fn enumerate_dir(
        &self,
        target: Option<&storage_core::ObjectKey>,
        limits: storage_fs::DirEnumerationLimits,
    ) -> Result<Vec<storage_fs::DirEntry>, storage_fs::FsDirError>;
}

// Proposed test double for directory enumeration recording both targets and supplied limits
#[derive(Default)]
pub(crate) struct MockTagDirEnumerator {
    /// Records every enumeration invocation: (target_key, supplied_limits).
    pub recorded_invocations: std::sync::Mutex<
        Vec<(Option<storage_core::ObjectKey>, storage_fs::DirEnumerationLimits)>,
    >,
    /// Scripted responses keyed by target object key.
    pub scripted_responses: std::sync::Mutex<
        std::collections::HashMap<
            Option<storage_core::ObjectKey>,
            std::collections::VecDeque<Result<Vec<storage_fs::DirEntry>, storage_fs::FsDirError>>,
        >,
    >,
}

// Simulated payload stream body that can produce complete bytes or partial data followed by an I/O error
#[derive(Clone, Debug)]
pub(crate) enum MockPayloadBody {
    /// Complete data that finishes cleanly at EOF.
    Complete(Vec<u8>),
    /// Returns initial data bytes, followed by an explicit `std::io::Error`.
    PartialThenError {
        data: Vec<u8>,
        error_kind: std::io::ErrorKind,
        error_message: String,
    },
    /// Fails immediately on first read with `std::io::Error`.
    ImmediateIoError {
        error_kind: std::io::ErrorKind,
        error_message: String,
    },
}

// Proposed test-seam payload reader abstraction
#[async_trait::async_trait]
pub(crate) trait TagPayloadReader: Send + Sync {
    async fn open_payload(
        &self,
        key: &storage_core::ObjectKey,
    ) -> Result<storage_core::ObjectPayload, storage_core::ReadError>;
}

// Proposed test double for injecting payload open outcomes and partial stream read failures
#[derive(Default)]
pub(crate) struct MockTagPayloadReader {
    /// Open-time errors (e.g. ReadError::NotFound, ReadError::PermissionDenied, ReadError::Backend(...)).
    pub inject_open_error: std::sync::Mutex<
        std::collections::HashMap<String, storage_core::ReadError>,
    >,
    /// Configured payload body outcomes (complete or partial-then-error).
    pub inject_payload_bodies: std::sync::Mutex<
        std::collections::HashMap<String, MockPayloadBody>,
    >,
    /// Recorded payload open requests by tag key string.
    pub recorded_opens: std::sync::Mutex<Vec<String>>,
}
```

### 8.2 Error Translation Taxonomy & Assertion Strategy

The test seam maintains strict separation between directory-error translation and payload-error translation, using existing translators rather than inventing ad-hoc mappings:

1. **Directory-Error Translation** (`storage_fs::FsDirError` -> `StorageError`):
   - Handled via `translate_terminal_dir_error` (or test-seam directory translator):
     - `FsDirError::EntryDisappeared { .. }` -> `StorageErrorKind::Io`.
     - `FsDirError::LimitExceeded { .. }` -> `StorageErrorKind::Backend`.
     - `FsDirError::PermissionDenied { .. }` -> `StorageErrorKind::PermissionDenied`.
     - `FsDirError::ResolutionRejected { .. }` -> `StorageErrorKind::Io`.
     - `FsDirError::NotADirectory { .. }` -> `StorageErrorKind::CorruptData`.
     - `FsDirError::SyscallUnsupported(..)` / `PlatformUnsupported` -> `StorageErrorKind::Configuration`.
     - `FsDirError::NotFound { .. }`: When probing repository or checking `tags/`, mapped to `StorageError::NotFound` or empty page per operation contract.
2. **Payload-Error Translation** (`storage_core::ReadError` -> `StorageError`):
   - Handled via `read_adapter::translate_read_error`:
     - `ReadError::NotFound` -> `StorageError::NotFound` (omitted from page under proposed seam policy).
     - `ReadError::PermissionDenied` -> `StorageErrorKind::Io`.
     - `ReadError::Backend` with `FsMetadataError::ResolutionRejected` -> `StorageErrorKind::Io`.
     - `ReadError::Backend` with `FsMetadataError::UnsupportedObjectType` -> `StorageErrorKind::Io`.
     - `ReadError::Backend` with `FsMetadataError::SyscallUnsupported` -> `StorageErrorKind::Configuration`.
3. **Stream Draining & Parsing Translation** (`tag_read::drain_tag_stream`):
   - Explicit `std::io::Error` during stream read -> `StorageErrorKind::Io`.
   - Byte limit overflow (`buffer.len() as u64 > limit`) -> `StorageErrorKind::CorruptData`.
   - Limit arithmetic overflow (`limit.checked_add(1)` fails for `u64::MAX`) -> `StorageErrorKind::CorruptData`.
   - Invalid UTF-8 bytes during payload decode -> `StorageErrorKind::Io` (proposed fail-closed policy).
4. **Strongly-Typed Assertion Strategy**:
   - Planned tests assert on top-level enum variants (`matches!(err, StorageError::NotFound)`, `matches!(err, StorageError::InvalidRepoName(_))`) or inner error kinds via `err.internal_kind() == Some(StorageErrorKind::...)`.
   - Avoids brittle assertions against message text strings or nonexistent variants (`StorageError::Backend` / `StorageError::CorruptData`).

### 8.3 Concrete Planned Test Inventory

1. **`test_seam_name_only_returns_names_without_opening_payloads`** *(Planned)*:
   - Construct repository with 3 tag candidates.
   - Call `contained_list_tags_seam`.
   - Verify sorted names returned; verify no payload reader is required or called.
2. **`test_seam_digest_bearing_parses_payloads_and_slices`** *(Planned)*:
   - Construct 3 tag files with valid `sha256:` digests.
   - Call `contained_list_tags_page_seam(..., cursor: None, page_limit: 2, ...)`.
   - Verify 2 sorted summaries returned with next continuation token matching second tag name.
3. **`test_seam_repo_probe_budget_exhaustion`** *(Planned)*:
   - Existing repository contains `manifests/`, `tags/`, `metadata.json` (3 entries).
   - Configure `repo_probe_limits` with `max_entries = 1`.
   - Simulate absent `tags/` so existence probe is executed.
   - Assert call fails closed with `err.internal_kind() == Some(StorageErrorKind::Backend)`.
   - Assert `MockTagDirEnumerator::recorded_invocations` contains `(Some(repo_key), repo_probe_limits)`.
4. **`test_seam_missing_repo_vs_missing_tags_directory`** *(Planned)*:
   - Nonexistent repository: probe returns `FsDirError::NotFound`. Assert `contained_list_tags_seam` returns `matches!(err, StorageError::NotFound)` and `contained_list_tags_page_seam` returns `Ok((Vec::new(), None))`.
   - Existing repository with absent `tags/`: probe returns `Ok(_)`, `tags/` returns `FsDirError::NotFound`. Assert both return empty success `Ok(Vec::new())` / `Ok((Vec::new(), None))`.
5. **`test_seam_exact_directory_and_payload_boundaries`** *(Planned)*:
   - Directory entry limit: $N$ entries with limit $N$ succeeds; limit $N-1$ fails closed with `err.internal_kind() == Some(StorageErrorKind::Backend)`.
   - Directory filename bytes: limit matching cumulative bytes succeeds; limit - 1 fails with `err.internal_kind() == Some(StorageErrorKind::Backend)`.
   - Payload byte limit: 71-byte digest file with `max_payload_bytes: Some(71)` succeeds; `max_payload_bytes: Some(70)` fails with `err.internal_kind() == Some(StorageErrorKind::CorruptData)`.
6. **`test_seam_zero_page_behavior_and_error_precedence`** *(Planned)*:
   - `page_limit == 0` with invalid repo name fails closed with `matches!(err, StorageError::InvalidRepoName(_))`.
   - `page_limit == 0` on unreadable `tags/` directory (`FsDirError::PermissionDenied`) fails closed with `err.internal_kind() == Some(StorageErrorKind::PermissionDenied)`.
   - `page_limit == 0` on symlink directory path (`FsDirError::ResolutionRejected`) fails closed with `err.internal_kind() == Some(StorageErrorKind::Io)`.
   - `page_limit == 0` with unreadable tag file (`ReadError::PermissionDenied`) or corrupt payload inside accessible directory returns `Ok((Vec::new(), None))`, verifying payload opening is skipped.
7. **`test_seam_candidate_observed_regular_replaced_before_payload_open`** *(Planned)*:
   - Candidate observed as `DirEntryType::Regular` during enumeration is replaced before `open_payload`:
     - Replaced with directory: `open_payload` returns `ReadError::Backend(FsMetadataError::UnsupportedObjectType)`, mapped to `err.internal_kind() == Some(StorageErrorKind::Io)`.
     - Replaced with symlink: `open_payload` returns `ReadError::Backend(FsMetadataError::ResolutionRejected)`, mapped to `err.internal_kind() == Some(StorageErrorKind::Io)`.
     - Permission denied: `open_payload` returns `ReadError::PermissionDenied`, mapped to `err.internal_kind() == Some(StorageErrorKind::Io)`.
8. **`test_seam_payload_not_found_after_enumeration_omitted`** *(Planned)*:
   - Candidate observed as `DirEntryType::Regular` during enumeration is deleted before `open_payload`.
   - `open_payload` returns `ReadError::NotFound`.
   - Candidate is omitted from page results, preserving legacy tolerance for concurrent deletions.
9. **`test_seam_dir_entry_disappeared_propagates_directory_error`** *(Planned)*:
   - Entry vanishes during directory enumeration inspection, yielding `FsDirError::EntryDisappeared`.
   - Mapped via directory translation to `err.internal_kind() == Some(StorageErrorKind::Io)`.
   - Directory error is propagated immediately without swallowing.
10. **`test_seam_stream_partial_read_followed_by_io_error`** *(Planned)*:
    - Stream yields partial data bytes followed by an explicit `std::io::Error` (via `MockPayloadBody::PartialThenError`).
    - Assert `drain_tag_stream` fails closed with `err.internal_kind() == Some(StorageErrorKind::Io)`.
11. **`test_seam_stream_normal_eof_parsed_using_bytes_received`** *(Planned)*:
    - Stream ends normally (EOF).
    - Valid digest text succeeds; empty or malformed digest text follows proposed omission policy.
    - Metadata content length is not enforced as an expected stream length (e.g. metadata claims 500 bytes, stream yields 71 valid bytes $\to$ parses successfully).
12. **`test_seam_payload_sha512_whitespace_none_limit_and_checked_arithmetic`** *(Planned)*:
    - SHA-512 digest (128 hex characters, 135 bytes total) parses successfully.
    - Surrounding whitespace and trailing `\r\n` / `\n` trimmed without error.
    - `max_payload_bytes: None` drains stream unbounded.
    - Checked limit arithmetic: `max_payload_bytes: Some(u64::MAX)` triggers `limit.checked_add(1)` overflow check, failing with `err.internal_kind() == Some(StorageErrorKind::CorruptData)`.
13. **`test_seam_inter_page_deterministic_insertion_deletion_and_modification`** *(Planned)*:
    - Page 1 returns up to token `t2`.
    - Insertion between pages: `t2.5` added. Page 2 with cursor `t2` returns `t2.5` then `t3` without duplicates.
    - Deletion between pages: `t4` deleted. Page 2 with cursor `t2` returns remaining tags without observing `t4`.
    - Target modification between pages: `t3` rewritten to new digest. Page 2 returns `t3` with updated digest.
14. **`test_seam_payload_invalid_utf8_fails_closed`** *(Planned)*:
    - Tag payload contains invalid UTF-8 bytes (e.g. `[0xff, 0xfe]`).
    - Fails closed with `err.internal_kind() == Some(StorageErrorKind::Io)` (proposed change requiring approval).
15. **`test_seam_shared_reader_identity_and_root_replacement`** *(Planned)*:
    - Contained seam initialized with real `storage_fs::FsMetadataReader`.
    - Root directory renamed to `root.old`; new `root.new` created at path.
    - Contained listing continues observing Tree A via pinned descriptor.
16. **`test_seam_pagination_cursors_unusual_tokens_and_saturating_arithmetic`** *(Planned)*:
    - Continuation tokens: `""`, `"t2.5"`, `"t2 🏷️"`, 10 KiB token.
    - Slicing with `page_limit == usize::MAX` does not panic or overflow due to `saturating_add`.
17. **`test_seam_independent_budget_tracking`** *(Planned)*:
    - Verifies `MockTagDirEnumerator::recorded_invocations` records independent budgets: `repo_probe_limits` for repository probe, and `tags_dir_limits` for tags directory enumeration.

---

## 9. Operating Limits & Rollout Boundaries

- **Linux Kernel `openat2`**: Descriptor-relative directory and payload containment strictly requires the Linux `openat2` syscall with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`.
- **Procfs Mount Requirements**: Phase 2 readable payload reopening (`open_payload`) requires an accessible, genuine, and stable `/proc` filesystem mounted at `/proc/self/fd/N`.
- **No Mount or Hard-Link Isolation**: Descriptor containment restricts traversal beneath the opened root descriptor, but nested mount points or hard links within the tree are not isolated by the kernel flags.
- **Platform Fail-Closed Boundary**: Non-Linux operating systems fail closed with `FsDirError::PlatformUnsupported`. No uncontained pathname fallback is introduced.
- **Rollback Contract**: If a regression occurs during subsequent production rollout, rolling back the binary and restarting the process restores legacy behavior. Rollback does not undo mutations written to sled or disk.

---

## 10. Quality Gate Status & Audit

All eight canonical quality gates remain explicitly **OPEN**:
- `O-03`: Key and continuation-token contracts.
- `O-04`: Filesystem write durability and containment.
- `O-05`: Broader filesystem read containment.
- `O-06`: Typed AWS mapping and pinned-MinIO evidence.
- `O-13`: Hosting, distribution, and release strategy.
- `O-15`: Non-Linux verification.
- `O-16`: Earlier Slice 11 audit/test-inventory evidence.
- `D-06`: Broader extraction, cutover, compatibility, and distribution acceptance.

---

## 11. Conclusion

This corrected design resolves the repository-existence probe budget issue, specifies exact payload and zero-page error mappings, restores a comprehensive and actionable verification plan, and conditionally phrases caller impact. By establishing a disciplined multi-slice roadmap, `registry-rust` can proceed toward contained tag listing without compromising architectural integrity.
