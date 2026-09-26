> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. The decisions recorded PENDING here were implemented (`acddfe7`). Near-duplicate readiness assessment: filesystem-manifest-listing-production-readiness-assessment.md.
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Architecture Decision Record: Contained Filesystem Manifest Listing Production Promotion

**Repository:** `registry-rust`
**Target Document:** `docs/architecture/filesystem-manifest-listing-production-decisions.md`
**Authoritative Baselines:**
- `registry-rust` HEAD: `005cfc21a1d78832672bc557954d415c70d71e78` (committed test seam)
- `storage-layer-rust` HEAD: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e`

**Scope:** Production-promotion decision package for contained filesystem manifest listing (`FsStorage::list_manifest_digests_page`).
**Status:** **DECISION ONLY — NOT AUTHORIZED FOR PRODUCTION CUTOVER — NO COMMITS OR PUSHES**.
**Quality Gates:** **O-03, O-04, O-05, O-06, O-13, O-15, O-16, and D-06 remain OPEN**.

---

## 1. Executive Summary & Accepted Baseline Context

In milestone session-20260911-2140 (commit `005cfc21a1d78832672bc557954d415c70d71e78`), the contained filesystem manifest-listing **test-only seam** was committed to `registry-rust`. That test seam established the following verified architectural behaviors:
1. **Pre-Composition Repository Validation:** Enforces strict validation on repository path strings (`manifest_dir_key`) before key composition and before zero-limit checks, failing closed with `StorageError::InvalidRepoName` on path traversal (`..`), absolute prefixes (`/`), backslashes (`\\`), control characters, and empty segments.
2. **Descriptor Containment:** Enforces single-directory enumeration beneath the pinned storage root descriptor via `storage_fs::FsMetadataReader` and Linux `openat2` (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`).
3. **Canonical Filename Discovery & Filtering:** Discovers canonical lowercase raw 64-hex SHA-256 and 128-hex SHA-512 regular files. Filters out non-regular entries (subdirectories, symlinks, FIFOs) and non-canonical filenames (uppercase hex, algorithm-prefixed `sha256:`, temporary upload files `.tmp.*`, and lock files `.lock.*`).
4. **Consistent Sorting & Whole-String Token Comparison:** Sorts manifests via `Digest::cmp` (algorithm ascending, then hex ascending), deduplicates entries, and evaluates continuation tokens by full-string lexical comparison (`d.as_str().as_str().cmp(token)`) rather than tuple-splitting.
5. **Typed Error Taxonomy:** Propagates typed errors (`StorageError::NotFound`, `StorageError::Internal` with `StorageErrorKind::CorruptData`, `StorageErrorKind::PermissionDenied`, `StorageErrorKind::Io`, `StorageErrorKind::Configuration`, or `StorageErrorKind::Backend`) rather than suppressing directory read failures.

**Current Production State:**
Production `FsStorage::list_manifest_digests_page` (`src/storage/fs.rs:1000-1046`) remains **unmodified** on legacy uncontained `tokio::fs::read_dir`. The 10,000-entry and 1,500,000-name-byte constants in `manifest_listing.rs` are **provisional test fixtures** and are **not approved production defaults**.

This document presents the complete, source-grounded decision package for promoting contained manifest listing to production, including a detailed trace of caller control flow and partial-progress dynamics.

---

## 2. Exact Promotion Scope

Promoting contained manifest listing to production requires coordinated modifications across a strictly bounded set of files and declarations.

### 2.1 File and Declaration Changes

```
registry-rust/
├── src/
│   ├── storage/
│   │   ├── fs.rs                           [MODIFY: Module declaration, FsStorage struct/limits, list_manifest_digests_page]
│   │   ├── fs/
│   │   │   ├── manifest_listing.rs         [MODIFY: Remove top-level #![cfg(test)], expose default limit constructor]
│   │   │   └── tests.rs                    [MODIFY: Update 8 characterization assertions to match contained behavior]
│   │   ├── mod.rs                          [MODIFY: Pass configured limits to FsStorage constructor]
│   └── config.rs                           [MODIFY: Add FileStorageFs config fields, validation, and env vars]
```

#### 1. `src/storage/fs.rs`
- **Module Declaration:** Remove the `#[cfg(test)]` attribute guarding the module declaration at lines 3568-3570:
  ```rust
  // Current (test-only):
  #[cfg(test)]
  #[path = "fs/manifest_listing.rs"]
  pub(crate) mod manifest_listing;

  // Promoted (production):
  #[path = "fs/manifest_listing.rs"]
  pub(crate) mod manifest_listing;
  ```
- **Method Implementation:** Replace the legacy implementation of `list_manifest_digests_page` at lines 1000-1046 with delegation to the contained implementation:
  ```rust
  async fn list_manifest_digests_page(
      &self,
      repo: &str,
      continuation_token: Option<&str>,
      page_limit: usize,
  ) -> Result<(Vec<Digest>, Option<String>), StorageError> {
      manifest_listing::list_manifest_digests_page_impl(
          self.reader.as_ref(),
          repo,
          continuation_token,
          page_limit,
          self.manifest_listing_limits,
      )
      .await
  }
  ```
- **Storage Struct & Constructor:** Add `manifest_listing_limits: storage_fs::DirEnumerationLimits` to `FsStorage`. In `FsStorage::try_new`, initialize this field with validated defaults or pass it explicitly via `FsStorage::try_new_with_limits`. Ensure direct constructors validate the supplied limits as well.

#### 2. `src/storage/fs/manifest_listing.rs`
- **Top-Level Attribute:** Remove the file-level `#![cfg(test)]` at line 33.
- **Production Limit Helper:** Because `storage_fs::DirEnumerationLimits` has no `Default` trait implementation, expose a crate-owned helper function using `DirEnumerationLimits::new`:
  ```rust
  pub const PROVISIONAL_DEFAULT_MANIFEST_MAX_ENTRIES: usize = 10_000;
  pub const PROVISIONAL_DEFAULT_MANIFEST_MAX_NAME_BYTES: usize = 1_500_000;

  pub fn default_manifest_dir_limits() -> DirEnumerationLimits {
      DirEnumerationLimits::new(
          PROVISIONAL_DEFAULT_MANIFEST_MAX_ENTRIES,
          PROVISIONAL_DEFAULT_MANIFEST_MAX_NAME_BYTES,
      )
  }
  ```
- **Test Gating Preservation:** The unit test suite starting at line 246 is declared as:
  ```rust
  #[cfg(test)]
  mod tests { ... }
  ```
  Removing `#![cfg(test)]` at the file level makes `list_manifest_digests_page_impl` and its support helpers available to `pub(crate)` production code, while the entire test suite (`mod tests`) remains strictly gated under `#[cfg(test)]`, preventing any test harness code from compiling into production binaries.

#### 3. Shared Reader, Startup Offload, and Capability Probe
`FsStorage` already owns an `Arc<storage_fs::FsMetadataReader>` initialized during asynchronous startup offload:
- In `src/storage/fs.rs:203-207`, `FsStorage::try_new` opens the root directory descriptor with `FsMetadataReader::open(&root)` and verifies kernel containment flags via `reader.probe_capability()`.
- In `src/storage/mod.rs:934-946`, `storage_wiring_try_from_config_async_with_factory` offloads this blocking initialization to `tokio::task::spawn_blocking`.
- Promotion reuses the existing `self.reader.as_ref()`. It does not introduce new root directory descriptor openings or repeated capability probes.

### 2.2 Characterization Test Impact in `src/storage/fs/tests.rs`

Milestone session-20260911-2055 characterized legacy `list_manifest_digests_page` across 16 tests in `src/storage/fs/tests.rs:4578-5314`. Upon promotion:
- **8 Tests Remain Valid Unchanged:**
  1. `test_manifest_listing_missing_and_empty_paths` (lines 4580-4613): Absent repos and empty manifest directories return `Ok(([], None))`.
  2. `test_manifest_listing_ordering_independent_of_creation_order` (lines 4615-4652): Pure SHA-256 entries sort lexicographically ascending.
  3. `test_manifest_listing_complete_traversal_and_continuation_tokens` (lines 4653-4708): Standard pagination across multiple pages with next tokens.
  4. `test_manifest_listing_non_utf8_filename_ignored` (lines 4796-4821): Invalid UTF-8 filenames are skipped.
  5. `test_manifest_listing_page_limits_zero_and_oversized` (lines 4822-4849): Page limit 0 returns empty slice; oversized limit returns all items.
  6. `test_manifest_listing_arbitrary_tokens_boundary_cases` (lines 4850-4894): Out-of-set tokens search correctly across SHA-256 items.
  7. `test_manifest_listing_inter_page_mutation_lacks_snapshot_isolation` (lines 5251-5289): Sequential inter-page additions observe new entries.
  8. `test_manifest_listing_manifest_reader_port_forwarding` (lines 5290-5314): Invocation via `<FsStorage as ManifestReader>` trait functions properly.
- **8 Tests Require Assertion Updates Due to Contained Listing Rules:**
  1. `test_manifest_listing_filename_interpretation_variants` (lines 4709-4771): Legacy ignored raw SHA-512 files, while including prefixed `sha256:`, `sha512:`, and uppercase hex. Contained listing **discovers raw SHA-512** files and **skips prefixed and uppercase** files.
  2. `test_manifest_listing_duplicate_digest_filenames_not_deduplicated` (lines 4772-4795): Legacy returned duplicate digests; contained listing **deduplicates**.
  3. `test_manifest_listing_mixed_algorithm_sorting_and_cursor_mismatch` (lines 4895-5002): Legacy sorted by `hex()` only, causing binary search cursor mismatch. Contained listing sorts by `Digest::cmp` (algorithm then hex), **resolving the cursor mismatch**.
  4. `test_manifest_listing_entry_types_unfiltered` (lines 5003-5054): Legacy listed subdirectories and symlinks as manifests; contained listing **filters out non-regular files**.
  5. `test_manifest_listing_symlinked_manifests_and_ancestors` (lines 5055-5101): Legacy followed symlinks outside storage root; contained listing **rejects symlinks with `StorageError::Internal` (`StorageErrorKind::Io`)**.
  6. `test_manifest_listing_path_traversal_and_absolute_paths` (lines 5102-5144): Legacy allowed `../../escaped_repo` and absolute paths; contained listing **rejects them with `StorageError::InvalidRepoName`**.
  7. `test_manifest_listing_component_wrong_type_suppressed` (lines 5145-5175): Legacy silently swallowed `ENOTDIR` when `manifests` was a regular file; contained listing **fails closed with `StorageError::Internal` (`StorageErrorKind::CorruptData`)**.
  8. `test_manifest_listing_permission_denied_ignored` (lines 5176-5250): Legacy silently swallowed `EACCES` as an empty page; contained listing **fails closed with `StorageError::Internal` (`StorageErrorKind::PermissionDenied`)**.

---

## 3. Production Budget Options

Directory enumeration in `storage-fs` requires finite bounds (`DirEnumerationLimits`) to prevent unbounded memory allocation and kernel thread exhaustion during `readdir`.

### 3.1 Comparison of Budget Options

| Dimension | Option A: Hardcoded Production Limits | Option B: Configurable Limits with Validated Defaults (Recommended) |
| :--- | :--- | :--- |
| **Configuration Surface** | No configuration additions; constants hardcoded in `fs/manifest_listing.rs`. | Add TOML fields in `[storage.fs]` and environment variable overrides. |
| **Implementation Complexity** | Minimal (no changes to `config.rs` or `storage/mod.rs`). | Moderate (plumbs configuration through `Config` and `FsStorage`). |
| **Operational Flexibility** | **Zero.** If a repository exceeds the hardcoded limit, listing fails permanently until binary recompile/deploy. | **High.** Operators can tune limits dynamically per deployment or emergency-raise limits via ENV. |
| **Operational Recovery** | Requires offline file deletion or binary replacement. | Operator can adjust environment variable without code change. |
| **Defensive Safety** | Guarantees hard ceiling on all deployments. | Defaults guarantee safe ceiling; validation prevents misconfiguration (e.g. 0). |

**Recommendation:** **Option B (Configurable with Validated Defaults)**.
*Justification:* Hardcoding limits creates an operational cliff. When a legitimate registry repository accumulates manifests beyond a hardcoded limit, all manifest listing, reference index synchronization, and manifest lifecycle deletions fail. Making limits configurable with validated defaults protects against unbounded directory attacks while providing an operational recovery mechanism.

### 3.2 Concrete Configuration Architecture (Option B)

1. **Configuration Entry Point:**
   Configuration loading enters through `Config::from_env_with_files(config_paths: &[PathBuf])` in `src/config.rs:1132`.
2. **TOML Configuration (`[storage.fs]`):**
   ```toml
   [storage.fs]
   root = "./data"
   manifest_listing_max_entries = 10000
   manifest_listing_max_name_bytes = 1500000
   ```
   Add optional fields to `FileStorageFs` in `src/config.rs:1001-1005`:
   ```rust
   #[derive(Clone, Debug, Default, Deserialize)]
   struct FileStorageFs {
       #[serde(default)]
       root: Option<String>,
       #[serde(default)]
       manifest_listing_max_entries: Option<usize>,
       #[serde(default)]
       manifest_listing_max_name_bytes: Option<usize>,
   }
   ```
3. **Environment Variable Names and Precedence:**
   - Precedence: Environment Variable > File Configuration > Default.
   - For entries: `env_usize_opt(&["REGISTRY__STORAGE__FS__MANIFEST_LISTING_MAX_ENTRIES", "STORAGE_FS_MANIFEST_LISTING_MAX_ENTRIES"])?`
   - For name bytes: `env_usize_opt(&["REGISTRY__STORAGE__FS__MANIFEST_LISTING_MAX_NAME_BYTES", "STORAGE_FS_MANIFEST_LISTING_MAX_NAME_BYTES"])?`
   - Hierarchical alias (`REGISTRY__STORAGE__FS__*`) precedes the flat alias (`STORAGE_FS_*`). Numeric parsing uses `env_usize_opt`, which trims strings and returns `ConfigError::InvalidEnvValue` on malformed input.
4. **Validation Rules in `src/config.rs` and Direct Constructors:**
   - Configuration validation alone is insufficient; direct constructors (`FsStorage::try_new_with_limits`) must enforce the same invariant checks:
     - `manifest_listing_max_entries`: Must be `>= 1`. (Rejects 0 to prevent immediate enumeration failure).
     - `manifest_listing_max_name_bytes`: Must be `>= 130`.
   - **Rationale for 130-byte minimum:** A SHA-512 manifest filename is 128 raw hex ASCII characters. If `max_name_bytes` is less than 128 bytes, a single SHA-512 manifest could never be enumerated. Setting the floor at 130 bytes guarantees that at least one 128-byte SHA-512 filename can be accommodated.
   - **Independent Binding Limits:** `DirEnumerationLimits` intentionally supports independent entry and byte limits. Entry count and cumulative byte length do not enforce an artificial `name_bytes >= entries * 64` constraint; either limit can bind first depending on directory contents. If an upper bound is established, it should be sized based on host memory policies rather than arbitrary multiples.
5. **Constructor Behavior:**
   - Direct constructor `FsStorage::try_new(root, max_upload_bytes)` uses `manifest_listing::default_manifest_dir_limits()`.
   - `FsStorage::try_new_with_limits(root, max_upload_bytes, limits)` validates limits before storing them in `self.manifest_listing_limits`.

### 3.3 Resource Accounting, Sizing, and Failure Dynamics

All proposed default figures below are **PROVISIONAL** until benchmarked under production workloads.

#### 1. Joint Limit Sizing (Entries vs. Name Bytes)
- In `storage-fs/src/dir.rs:249-267`, `account_entry` is evaluated per entry:
  ```rust
  pub(crate) fn account_entry(
      current_count: usize,
      current_bytes: usize,
      next_name_len: usize,
      limits: &DirEnumerationLimits,
  ) -> Result<usize, FsDirError> {
      if current_count >= limits.max_entries() {
          return Err(FsDirError::LimitExceeded {
              reason: LimitExceededReason::MaxEntries(limits.max_entries()),
          });
      }

      match current_bytes.checked_add(next_name_len) {
          Some(new_total) if new_total <= limits.max_total_name_bytes() => Ok(new_total),
          _ => Err(FsDirError::LimitExceeded {
              reason: LimitExceededReason::MaxTotalNameBytes(limits.max_total_name_bytes()),
          }),
      }
  }
  ```
- **SHA-256 Entries:** 64 hex characters = 64 bytes. For 10,000 entries: `10,000 * 64 = 640,000` name bytes.
- **SHA-512 Entries:** 128 hex characters = 128 bytes. For 10,000 entries: `10,000 * 128 = 1,280,000` name bytes.
- **Ignored & Non-Regular Entries Counted BEFORE Filtering:**
  In `storage-fs/src/dir.rs:524-533`, `readdir` increments entry count and accumulates name bytes **before** checking file type or filename format:
  ```rust
  let name_bytes = c_name.to_bytes();
  if name_bytes == b"." || name_bytes == b".." { continue; }
  let new_total = account_entry(entries.len(), total_name_bytes, name_len, &limits)?;
  ```
  Consequently, temporary upload files (`.tmp.upload-xyz`), lock files (`.lock.exclusive`), subdirectories, and non-canonical files **all consume budget**. If a directory contains 4,000 temporary files and 7,000 manifest files, total entries evaluated is 11,000, exceeding a 10,000 entry limit even though only 7,000 are valid manifests.
- **Byte Budget Ratio:** To accommodate 10,000 entries where all or some are SHA-512, plus temporary file name overhead (~30 bytes each), `max_total_name_bytes` must be at least `1,500,000 bytes` (~1.43 MiB).

#### 2. Total Heap and Syscall Overhead
- `DirEnumerationLimits` accounts *only* for the raw `OsString` bytes of retained entries. It does *not* prove safe total memory usage:
  - Each retained entry creates a `DirEntry` struct containing `OsString` (heap allocation + 24-byte pointer/len/cap metadata) and `DirEntryType` (enum). 10,000 entries require ~1.5–2.5 MiB heap in `Vec<DirEntry>`.
  - In `manifest_listing.rs`, valid entries are parsed into `Vec<Digest>`. 10,000 digests require ~320 KiB heap.
  - Syscall duration: Directory traversal is executed synchronously inside `tokio::task::spawn_blocking`. Traversal of 10,000 dentries on local NVMe takes ~1–5 ms; on remote/network filesystems (NFS/EBS) or fragmented ext4 directories, it may take 50–200 ms, holding a Tokio blocking worker thread for that duration.

#### 3. Stateless Pagination Amplification
- Manifest listing is **stateless**: every page request enumerates the *entire* directory from disk, sorts all entries, and slices the requested window `[start..end]`.
- If a client paginates through 10,000 manifests requesting 100 entries per page:
  - The client makes **100 HTTP requests**.
  - The server executes **100 full directory enumerations** (`100 * 10,000 = 1,000,000` readdir visits).
  - The server performs **100 vector allocations, sorts, and deduplications**.
- Under concurrent requests, memory and CPU scale as `O(N_concurrent * Total_Entries)`.

#### 4. Recovery Under Limit Exceeded
When `LimitExceeded` is reached, `FsStorage::list_manifest_digests_page` returns `StorageError::Internal` (`StorageErrorKind::Backend`).
- Raising only the entry limit may leave the byte limit binding if name bytes are exhausted; both limits must be appropriately adjusted by the operator.
- Under Option B (configurable), raising limits via environment variable or TOML configuration enables operational recovery without requiring source code modifications.

---

## 4. Comprehensive Caller Risk & Control Flow Analysis

Source inspection of callers of `list_manifest_digests_page` reveals critical architectural risks that must be understood prior to promotion.

### 4.1 Lifecycle Control Flow & Call Sites

In `registry-rust/src/manifest_lifecycle.rs`, `list_manifest_digests_page` is invoked exclusively through the private helper `is_blob_referenced_in_repo`.

#### 1. Verbatim Helper: `is_blob_referenced_in_repo` (`src/manifest_lifecycle.rs:773-801`)
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

#### 2. Enclosing Caller 1: `evict_proxy_cached_entry` (`src/manifest_lifecycle.rs:1130-1290`)
*(Note: Standard policy B manifest deletion in `delete_manifest:1313-1520` deletes tags and manifest bytes, but does not unlink blob memberships. Operational proxy blob unlinking occurs exclusively in `evict_proxy_cached_entry`):*
```rust
    pub async fn evict_proxy_cached_entry(
        &self,
        repo: &str,
        tag: Option<&str>,
        target_digest: &Digest,
    ) -> Result<ProxyEvictionResult, ManifestLifecycleError> {
        if CanonicalRepoName::parse(repo).is_err() {
            return Err(ManifestLifecycleError::InvalidRepoName);
        }

        let mut guard = self.acquire_coordination(repo).await?;
        self.recover_and_ensure_index_healthy(repo).await?;

        // 1. Snapshot target tag if provided
        let mut relevant_tags = Vec::new();
        if let Some(t) = tag {
            if let Ok(Some((target, version))) = self.storage.get_tag_with_version(repo, t).await {
                if target == *target_digest {
                    relevant_tags.push(TagSnapshot {
                        tag: t.to_string(),
                        observed_version: version,
                        target_digest: target,
                        deleted: false,
                    });
                }
            }
        }

        // 2. Durably mark index dirty before authoritative mutations
        guard.check_lease().await?;
        if let Some(idx) = self.ref_index.as_ref() {
            idx.mark_dirty()?;
        }

        // 3. Write initial lifecycle journal
        let canonical_repo =
            CanonicalRepoName::parse(repo).map_err(|_| ManifestLifecycleError::InvalidRepoName)?;
        let mut journal = LifecycleJournalRecord {
            op_id: uuid::Uuid::new_v4().to_string(),
            repo: canonical_repo,
            op_kind: LifecycleOpKind::ProxyEvict,
            target_digest: target_digest.clone(),
            target_reference: tag.map(|s| s.to_string()),
            phase: LifecyclePhase::ProxyEvictInitiated,
            owner_id: guard.owner_id.clone(),
            lease_expiry_unix_secs: now_unix_secs() + REPO_LEASE_TTL_SECS,
            started_unix_secs: now_unix_secs(),
            updated_unix_secs: now_unix_secs(),
            relevant_tags: relevant_tags.clone(),
            subject_digest: None,
            artifact_type: None,
            annotations: None,
            media_type: None,
            manifest_size: None,
        };
        self.write_journal(repo, &journal).await?;

        // 4. Conditionally remove tag alias
        let mut tag_removed = None;
        if let Some(tag_snap) = relevant_tags.first() {
            let res = self
                .storage
                .delete_tag_conditional(repo, &tag_snap.tag, Some(&tag_snap.observed_version))
                .await;
            if matches!(res, Ok(crate::storage::ConditionalDeleteResult::Deleted)) {
                if let Some(idx) = self.ref_index.as_ref() {
                    let _ = idx.on_tag_deleted(repo, &tag_snap.tag);
                }
                tag_removed = Some(tag_snap.tag.clone());

                journal.phase = LifecyclePhase::ProxyTagDeleted;
                journal.updated_unix_secs = now_unix_secs();
                let _ = self.write_journal(repo, &journal).await;
            }
        }

        // 5. Check if any other tags in repo resolve to target_digest
        let mut has_other_tags = false;
        let mut page_tok: Option<String> = None;
        loop {
            let (page, next_tok) = match self
                .storage
                .list_tags_page(repo, page_tok.as_deref(), POLICY_B_TAG_PAGE_SIZE)
                .await
            {
                Ok(p) => p,
                Err(_) => (Vec::new(), None),
            };
            for (_t_name, t_d) in page {
                if t_d.hex() == target_digest.hex() {
                    has_other_tags = true;
                    break;
                }
            }
            if has_other_tags {
                break;
            }
            match next_tok {
                Some(tok) => page_tok = Some(tok),
                None => break,
            }
        }

        let mut manifest_removed = false;
        let mut memberships_unlinked = 0;

        // 6. If no tags point to target_digest, remove manifest root and unneeded proxy memberships
        if !has_other_tags {
            if let Ok((_meta, bytes)) = self.storage.get_manifest(repo, target_digest).await {
                let refs = crate::manifest_refs::parse_manifest_refs(&bytes).ok();

                // Delete manifest from storage
                let _ = self.storage.delete_manifest(repo, target_digest).await;
                manifest_removed = true;

                journal.phase = LifecyclePhase::ProxyManifestDeleted;
                journal.updated_unix_secs = now_unix_secs();
                let _ = self.write_journal(repo, &journal).await;

                // Reconcile index
                if let Some(idx) = self.ref_index.as_ref() {
                    let _ = idx.on_manifest_deleted(repo, target_digest);
                    idx.flush()?;
                    idx.mark_ready()?;
                }

                // If refs were parsed, check if remaining manifests in repo reference each blob
                if let Some(refs) = refs {
                    for blob_d in refs.blob_references() {
                        let still_referenced = self.is_blob_referenced_in_repo(repo, blob_d).await;

                        if !still_referenced {
                            // Check if blob membership is of Proxy provenance
                            if let Ok(Some(record)) =
                                self.storage.get_repo_blob_membership(repo, blob_d).await
                            {
                                if record.provenance
                                    == crate::storage::repo_membership::MembershipProvenance::Proxy
                                {
                                    let _ = self.storage.unlink_repo_blob(repo, blob_d).await;
                                    memberships_unlinked += 1;
                                }
                            }
                        }
                    }
                }

                journal.phase = LifecyclePhase::ProxyMembershipsUnlinked;
                journal.updated_unix_secs = now_unix_secs();
                let _ = self.write_journal(repo, &journal).await;
            }
        }

        if let Some(idx) = self.ref_index.as_ref() {
            idx.flush()?;
            idx.mark_ready()?;
        }

        self.delete_journal(repo).await?;
        guard.release().await?;

        Ok(ProxyEvictionResult {
            tag_removed,
            manifest_removed,
            memberships_unlinked,
        })
    }
```

#### 3. Enclosing Caller 2: Recovery Dispatcher & `recover_pending_journal_under_lock` (`src/manifest_lifecycle.rs:494-510, 644-770`)
```rust
    pub async fn recover_and_ensure_index_healthy(
        &self,
        repo: &str,
    ) -> Result<(), ManifestLifecycleError> {
        if let Some(journal) = self.read_journal(repo).await? {
            self.recover_pending_journal_under_lock(repo, &journal)
                .await?;
        }

        if let Some(idx) = self.ref_index.as_ref() {
            if idx.check_health().is_err() {
                idx.ensure_healthy_or_rebuild(&self.storage, true, false)
                    .await?;
            }
        }
        Ok(())
    }
```
And inside `recover_pending_journal_under_lock` under `LifecycleOpKind::ProxyEvict`:
```rust
            LifecycleOpKind::ProxyEvict => {
                // 1. If tag specified in journal, finish conditionally deleting tag alias
                if let Some(ref tag) = journal.target_reference {
                    if let Ok(Some((target, version))) =
                        self.storage.get_tag_with_version(repo, tag).await
                    {
                        if target == journal.target_digest {
                            let _ = self
                                .storage
                                .delete_tag_conditional(repo, tag, Some(&version))
                                .await;
                            if let Some(idx) = self.ref_index.as_ref() {
                                let _ = idx.on_tag_deleted(repo, tag);
                            }
                        }
                    }
                }

                // 2. Check if any other tags in the repo resolve to target_digest
                let mut has_other_tags = false;
                let mut page_tok: Option<String> = None;
                loop {
                    let (page, next_tok) = match self
                        .storage
                        .list_tags_page(repo, page_tok.as_deref(), POLICY_B_TAG_PAGE_SIZE)
                        .await
                    {
                        Ok(p) => p,
                        Err(_) => (Vec::new(), None),
                    };
                    for (_t_name, t_d) in page {
                        if t_d == journal.target_digest {
                            has_other_tags = true;
                            break;
                        }
                    }
                    if has_other_tags {
                        break;
                    }
                    match next_tok {
                        Some(tok) => page_tok = Some(tok),
                        None => break,
                    }
                }

                // 3. If no remaining tags resolve to target_digest, finish removing manifest root & proxy memberships
                if !has_other_tags {
                    let refs = match self
                        .storage
                        .get_manifest(repo, &journal.target_digest)
                        .await
                    {
                        Ok((_meta, bytes)) => {
                            crate::manifest_refs::parse_manifest_refs(&bytes).ok()
                        }
                        Err(_) => None,
                    };

                    let _ = self
                        .storage
                        .delete_manifest(repo, &journal.target_digest)
                        .await;

                    if let Some(idx) = self.ref_index.as_ref() {
                        let _ = idx.on_manifest_deleted(repo, &journal.target_digest);
                        idx.flush()?;
                        idx.mark_ready()?;
                    }

                    if let Some(refs) = refs {
                        for blob_d in refs.blob_references() {
                            let still_referenced =
                                self.is_blob_referenced_in_repo(repo, blob_d).await;

                            if !still_referenced {
                                if let Ok(Some(record)) =
                                    self.storage.get_repo_blob_membership(repo, blob_d).await
                                {
                                    if record.provenance
                                        == crate::storage::repo_membership::MembershipProvenance::Proxy
                                    {
                                        let _ = self.storage.unlink_repo_blob(repo, blob_d).await;
                                    }
                                }
                            }
                        }
                    } else {
                        // Manifest was already deleted prior to recovery; check all proxy memberships in this repo
                        let mut page_tok: Option<String> = None;
                        loop {
                            let (page, next_tok) = match self
                                .storage
                                .list_repo_blob_memberships_page(repo, page_tok.as_deref(), 100)
                                .await
                            {
                                Ok(p) => p,
                                Err(_) => break,
                            };
                            for rec in page {
                                if rec.provenance
                                    == crate::storage::repo_membership::MembershipProvenance::Proxy
                                {
                                    let still_referenced =
                                        self.is_blob_referenced_in_repo(repo, &rec.digest).await;
                                    if !still_referenced {
                                        let _ =
                                            self.storage.unlink_repo_blob(repo, &rec.digest).await;
                                    }
                                }
                            }
                            match next_tok {
                                Some(tok) => page_tok = Some(tok),
                                None => break,
                            }
                        }
                    }
                }

                if let Some(idx) = self.ref_index.as_ref() {
                    idx.flush()?;
                    idx.mark_ready()?;
                }

                self.delete_journal(repo).await?;
            }
```

#### 4. Source-Established Architectural Facts
1. **Operations Preceding Reference Discovery:**
   - In `evict_proxy_cached_entry`: Coordination lock acquired, prior recovery executed, index marked dirty, initial journal written (`ProxyEvictInitiated`), conditional tag delete executed and persisted (`ProxyTagDeleted`), full tag listing executed to ensure zero remaining tags resolve to target digest, manifest bytes read, manifest deleted from storage, index updated, and phase persisted (`ProxyManifestDeleted`).
   - In `recover_pending_journal_under_lock`: Index marked dirty, conditional tag delete executed if recorded in journal, full tag listing checked.
2. **Phase Persistence:**
   - In `evict_proxy_cached_entry`, phases are persisted sequentially to disk via `self.write_journal`: `ProxyEvictInitiated` -> `ProxyTagDeleted` -> `ProxyManifestDeleted` -> `ProxyMembershipsUnlinked` -> `delete_journal`.
   - In `recover_pending_journal_under_lock`, intermediate phases are **not persisted**; recovery executes steps and then calls `self.delete_journal(repo).await?`.
3. **Error Impact on Journal Advancement & Removal:**
   - Currently, `is_blob_referenced_in_repo` returns `bool` (converting errors to `false`). It never returns an error. Consequently, `evict_proxy_cached_entry` unconditionally writes `ProxyMembershipsUnlinked` and deletes the journal.
   - If `is_blob_referenced_in_repo` is hardened to return `Result<bool, StorageError>`, an error halts execution before writing `ProxyMembershipsUnlinked`. The journal remains on disk at `ProxyManifestDeleted`, and `delete_journal` is not called.
4. **How Recovery Obtains References After Manifest Deletion:**
   - Lines 691-700 & 730-760: If the manifest was deleted prior to recovery, `self.storage.get_manifest` fails with `NotFound`, so `refs` evaluates to `None`.
   - Because `refs` is `None`, recovery falls back to `self.storage.list_repo_blob_memberships_page(repo, ...)` across the entire repository. For **every proxy membership in the repository**, recovery executes `is_blob_referenced_in_repo(repo, &rec.digest)`.
   - If `is_blob_referenced_in_repo` fails open (returning `false` on listing error), recovery attempts to unlink every proxy membership in the entire repository.
5. **Recovery Retry Trigger & Caller Propagation:**
   - Recovery is triggered **on-demand** by `recover_and_ensure_index_healthy(repo)`.
   - Callers of `recover_and_ensure_index_healthy`: `publish_manifest` (line 964), `evict_proxy_cached_entry` (line 1141), `delete_manifest` (line 1323), `delete_tag` (line 1537), and `reconcile_repo` (line 1637).
   - If `recover_pending_journal_under_lock` returns an error, all callers propagate `Err(ManifestLifecycleError)` via `?`.
   - **There is no background daemon or recurring task polling pending journals.** Retaining a journal does not guarantee automatic background retries; retries occur only when subsequent requests target that repository.

### 4.2 Partial-Progress Behavior Analysis

In `evict_proxy_cached_entry`, candidate blobs from `refs.blob_references()` are processed sequentially:
```rust
for blob_d in refs.blob_references() {
    let still_referenced = self.is_blob_referenced_in_repo(repo, blob_d).await?;
    if !still_referenced {
        if let Ok(Some(record)) = self.storage.get_repo_blob_membership(repo, blob_d).await {
            if record.provenance == MembershipProvenance::Proxy {
                let _ = self.storage.unlink_repo_blob(repo, blob_d).await;
            }
        }
    }
}
```

#### Distinctions Under Failure
Suppose a manifest references blobs `[A, B, C]`:
1. **Blob A succeeds:** `is_blob_referenced_in_repo` returns `Ok(false)` -> membership for `A` is unlinked.
2. **Blob B fails:** `is_blob_referenced_in_repo` returns `Err(StorageError::Internal(...))` (e.g. `LimitExceeded`).
   - **No unlink for Blob B:** Because discovery returned an error, membership `B` is not unlinked.
   - **No further cleanup after that error:** The loop exits via `?`. Blob `C` is never evaluated or unlinked.
   - **Earlier successful unlinks are NOT rolled back:** Membership `A` was already removed from `repo-memberships/by-repo/<encoded_repo>/`. There is no rollback mechanism to restore membership `A`.

#### Safety of Retry & Information Availability
- **Why retry is safe:**
  Because membership `A` was verified as genuinely unreferenced (`Ok(false)`), its removal was valid and permanent.
  When recovery runs later, the manifest was already deleted, so recovery scans all remaining proxy memberships via `list_repo_blob_memberships_page`. Because membership `A` was unlinked, it is not present; recovery proceeds to evaluate `B` and `C`.
  Idempotency holds: unlinking an already-unlinked membership returns `Ok(false)` without error.

#### Alternative: Two-Phase Discovery Before Mutation ("All-or-Nothing Cleanup")
If the intended contract is instead "no membership unlinks unless all reference checks succeed":
- **Phase 1 (Discovery):** Evaluate `is_blob_referenced_in_repo` for all candidate blobs in memory (`Vec<&Digest>`). If any check returns `Err(e)`, abort immediately before unlinking anything.
- **Phase 2 (Mutation):** Unlink all candidate blobs identified in Phase 1.
- **Costs & Concurrency Limitations:**
  - Buffering candidate digests requires minimal memory (`Vec<&Digest>` for a single manifest).
  - **No True Filesystem Atomicity:** Filesystem storage does not support multi-file atomic batch unlinks. Phase 2 executes individual `remove_file` calls non-atomically. A crash during Phase 2 still leaves partial unlinks.
  - **Time-of-Check to Time-of-Use Window:** Between Phase 1 and Phase 2, concurrent operations could write a new manifest referencing a candidate blob (mitigated by the coordination lease).
  - Therefore, Two-Phase Discovery prevents partial mutations on discovery error, but does not provide multi-file transactional atomicity.

### 4.3 Pagination-Cycle Handling

In `is_blob_referenced_in_repo`, continuation tokens are strings returned by the storage backend.

#### Cycle Detection Requirement
A broken or corrupt listing implementation could emit repeated tokens:
- **Immediate Repetition:** Token `A` followed by token `A`.
- **Multi-Token Cycle:** Token `A` -> Token `B` -> Token `A`.

To detect arbitrary cycles across the traversal, tokens must be treated as opaque strings and tracked across the entire session:
```rust
let mut tok: Option<String> = None;
let mut seen_tokens: std::collections::HashSet<String> = std::collections::HashSet::new();

loop {
    let (page, next_tok) = self
        .storage
        .list_manifest_digests_page(repo, tok.as_deref(), 100)
        .await?;

    // Process page...

    match next_tok {
        Some(t) => {
            if !seen_tokens.insert(t.clone()) {
                return Err(StorageError::backend(format!(
                    "pagination cycle detected on continuation token: {t}"
                )));
            }
            tok = Some(t);
        }
        None => break,
    }
}
```

#### Boundary & Limitations
- `HashSet<String>` detects all cycles within finite token sets (both immediate repeats and arbitrary loops `A -> B -> C -> A`).
- **Limitation:** Cycle detection does **not** bound an endless sequence of *distinct* tokens (`"token_1"`, `"token_2"`, `"token_3"`, ...). Bounding distinct token sequences requires an explicit pagination step counter (e.g. `const MAX_PAGINATION_STEPS = 10_000`) or underlying directory limits.

### 4.4 Reference Index & Garbage Collection Context

#### Reference Index Synchronization (`src/blob_ref_index.rs:486-537`)
- Step 1 removes existing tags for a repository from `self.tag_to_root`. Removals are attempted and individual errors ignored (`let _ = self.tag_to_root.remove(k)`).
- Step 2 lists manifests via `?`. If listing fails (e.g. `LimitExceeded`), the function aborts immediately without rollback.
- Storage tags on disk (`repos/<repo>/tags/`) are untouched.
- A later successful synchronization will re-read tags from disk and re-insert them into `tag_to_root`, repairing the index. In the interim, operations querying `tag_to_root` observe zero tags for that repository.

#### Garbage Collection Bypass (`src/blob_gc/policy.rs:166-176, 227-303`)
- Requires **BOTH** conditions:
  1. `storage.kind() == "fs"`
  2. `tokio::fs::metadata(&cfg.fs_root.join("repos")).await.is_ok()`
- Walker checks entry file types (`ft.is_dir()`, `mft.is_file()`) before descending or reading; it does not simply follow all symlinks. Symlinked initial/ancestor paths are followed by `tokio::fs::read_dir`, but symlink entries within directories are filtered by `!mft.is_file()`. Non-contained reads remain subject to replacement races.
- Raw 128-hex SHA-512 manifest files are omitted by the explicit 64-character length filter (`hex.len() != 64`).
- GC discovery refactoring is an independent unresolved issue, not a strict prerequisite for listing promotion, because GC already bypasses `list_manifest_digests_page`.

---

## 5. Implementation-Ready Narrow Slice: Lifecycle Reference-Discovery Hardening

Rather than combining multiple architectural changes, work should proceed with a single, self-contained next slice.

**Recommended Slice:** **Lifecycle Reference-Discovery Hardening**.

### 5.1 Proposed Result Contract & Implementation Details
- Target file: [`registry-rust/src/manifest_lifecycle.rs`](src/manifest_lifecycle.rs)
- Update `is_blob_referenced_in_repo` to return `Result<bool, StorageError>`:
  - Propagate `list_manifest_digests_page` errors via `?`.
  - Propagate `get_manifest` errors via `?`.
  - Map `parse_manifest_refs` errors to `StorageError::corrupt_data(...)`.
  - Track `seen_tokens: HashSet<String>` and return `StorageError::backend(...)` on duplicate token insertion.
- Update `evict_proxy_cached_entry` (lines 1256-1275):
  - Propagate `is_blob_referenced_in_repo` error with `?`.
  - On error, proxy memberships are not unlinked, journal is retained at `ProxyManifestDeleted`, and error is returned to caller.
- Update `recover_pending_journal_under_lock` (lines 713-755):
  - Propagate `is_blob_referenced_in_repo` error with `?`.
  - On error, proxy memberships are not unlinked, journal is retained on disk, and error is returned to caller.

### 5.2 Required Test Coverage
Target file: [`registry-rust/tests/manifest_lifecycle_tests.rs`](tests/manifest_lifecycle_tests.rs)
1. **Listing Failure Propagation:** Verify that a simulated listing error in `list_manifest_digests_page` causes `is_blob_referenced_in_repo` to return `Err` and prevents proxy blob unlinking.
2. **Manifest Read Failure Propagation:** Verify that a simulated I/O error in `get_manifest` causes `is_blob_referenced_in_repo` to return `Err`.
3. **Corrupt Manifest Reference Failure:** Verify that unparseable manifest bytes return `StorageErrorKind::CorruptData`.
4. **Pagination Cycle Detection:**
   - Test immediate token repetition (`A -> A`) fails with cycle detected.
   - Test multi-token cycle (`A -> B -> A`) fails with cycle detected.
5. **Deletion Error Propagation & Journal State:** Verify that eviction failure leaves journal at `ProxyManifestDeleted` on disk without advancing to `ProxyMembershipsUnlinked`.
6. **Recovery Error Propagation & Journal Retention:** Verify that recovery encountering an error aborts and preserves the journal on disk for subsequent retry.
7. **Partial-Progress and Subsequent Retry:** Verify that if blob A is unlinked before blob B fails, a subsequent recovery retry successfully finishes unlinking B once storage is restored.
8. **Success Paths:**
   - Verify genuinely unreferenced proxy blob is unlinked.
   - Verify genuinely referenced proxy blob is preserved.

### 5.3 Deferred Scope
The following areas remain strictly deferred to subsequent milestones:
- Reference-index transaction/staging hardening (`sync_repo_manifests_and_tags`).
- Garbage collection bypass refactoring (`build_manifest_protected_set_fs`).
- Production manifest listing promotion in `FsStorage`.
- Resource budget configuration in `Config`.

---

## 6. Compatibility Decisions

| Area | Legacy Production Behavior | Contained Seam Behavior | Decision for Production Promotion |
| :--- | :--- | :--- | :--- |
| **Repository Validation** | Unvalidated string concatenation; permitted path traversal (`../../`) and absolute paths. | Validates name strictly via `manifest_dir_key` before any I/O or zero-limit checks. Fails closed with `StorageError::InvalidRepoName`. | **Approve for Production.** Prevents directory escape and traversal vulnerabilities. |
| **Missing Paths vs. Unexpected Errors** | Swallowed missing directory, `ENOTDIR` (file in place of dir), and `EACCES` (permission denied) into `Ok(([], None))`. | Returns `Ok(([], None))` *only* on `NotFound`. Returns `CorruptData` on `NotADirectory`, `PermissionDenied` on `EACCES`, `Io` on resolution failures. | **Approve for Production.** Prevents silent data omission and masks of underlying filesystem corruption. |
| **SHA-512 Discovery** | Raw 128-hex files were silently ignored (failed `sha256:` length check and lacked colon). | Discovers both canonical raw 64-hex SHA-256 and raw 128-hex SHA-512 files. | **Approve for Production.** Aligns manifest listing with multi-algorithm digest support. |
| **Non-Canonical Filenames & Entry Types** | Listed prefixed (`sha256:`), uppercase hex, subdirectories, symlinks, and dangling symlinks. | Strictly filters to lowercase raw hex regular files (`DirEntryType::RegularFile`). Skips prefixed, uppercase, and non-regular entries. | **Approve for Production.** Storage writes manifests only as lowercase raw hex regular files; non-regular entries are invalid. |
| **Deduplication** | No deduplication; duplicate entries returned if multiple filenames resolved to same digest. | Runs `all_digests.dedup()` after sorting. | **Approve for Production.** Guarantees unique digest entries per page. |
| **Sort Order & In-Flight Legacy Tokens** | Sorted by `hex()` only. Binary search used `as_str()`, failing on mixed algorithms. | Sorts by `Digest::cmp` (algorithm ascending, then hex ascending). Token comparison uses whole strings. | **Approve for Production.** In pure SHA-256 repos, sort order is identical (zero disruption). In mixed repos, legacy in-flight tokens should be reset during deployment. |
| **Arbitrary-Token Acceptance** | Evaluated continuation token as arbitrary string via `as_str()`. | Evaluates arbitrary tokens as whole string via `d.as_str().as_str().cmp(token)` without splitting. | **Approve for Production.** Correctly handles arbitrary client continuation tokens. |
| **Zero-Limit Behavior** | Read directory, sorted entries, returned empty slice. | Validates repo first; if `page_limit == 0`, immediately returns `Ok(([], None))` with zero filesystem I/O. | **Approve for Production.** Avoids unnecessary disk I/O while preserving fail-closed validation. |
| **Production Resource Limits** | Unbounded memory; read arbitrary directory sizes. | Enforces `DirEnumerationLimits`. Fails closed with `StorageError::Internal` (`StorageErrorKind::Backend`). | **Approve with Option B Configuration.** Protects host from exhaustion while allowing operational tuning. |

### Operational Boundaries and Explicit Non-Guarantees
- **No Subsequent Readability Guarantee:** Canonical filename discovery does *not* guarantee that `get_manifest` will succeed. Files may be unlinked, corrupt, or unreadable when subsequently opened.
- **No Transactional Snapshot Isolation:** Point-in-time directory observation does *not* provide snapshot consistency across pagination requests or concurrent file additions/deletions.
- **No Mount Isolation Beneath Root:** Linux `RESOLVE_BENEATH` prevents escaping the root descriptor, but does not isolate submounts mounted inside the root.
- **No Storage Mutation Rollback:** Directory listing is strictly read-only; mutations in caller layers have no automatic storage rollback.

---

## 7. Separate Pending Decisions Requiring User Review

The following decisions are presented for separate review:

1. **Approval of the Narrowly Defined First Caller-Hardening Slice:**
   - Patching `is_blob_referenced_in_repo` in `src/manifest_lifecycle.rs` to return `Result<bool, StorageError>` and fail closed at deletion and recovery call sites before introducing budget errors.
2. **Later Reference-Index Hardening:**
   - Implementing staging or transactional semantics for tag removals in `sync_repo_manifests_and_tags` before manifest listing pagination.
3. **Later Production Configuration Policy and Listing Promotion:**
   - Approving Option B (configurable limits with provisional defaults of 10,000 entries and 1,500,000 name bytes) and cutting over `FsStorage::list_manifest_digests_page`.
4. **Separate Garbage Collection Discovery Harmonization:**
   - Refactoring `build_manifest_protected_set_fs` in `src/blob_gc/policy.rs` to use descriptor-relative manifest listing and discover SHA-512 manifests.

---

**ALL QUALITY GATES REMAIN EXPLICITLY OPEN:**
`O-03, O-04, O-05, O-06, O-13, O-15, O-16, D-06`.
