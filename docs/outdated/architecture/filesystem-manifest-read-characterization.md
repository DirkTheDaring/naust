> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. Contained manifest reads landed (`1924225`); manifests later moved onto the shared `manifest_domain` ObjectStore family (`76209a9`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Filesystem Manifest Read Characterization (Slice 2c)

## Baselines and Scope

### Repository Baselines
- **registry-rust**: `008726451fc1104e9872b75e342f57ddf9df10f4`
- **storage-layer-rust**: `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (read-only baseline)

### Source References
- Manifest read operations: `src/storage/fs.rs` (`FsStorage::head_manifest`, `FsStorage::get_manifest`)
- Filename and path generation: `src/storage/fs.rs` (`FsStorage::manifest_path`)
- Media type detection: `src/storage/fs.rs` (`FsStorage::detect_manifest_media_type`)
- Manifest read port: `src/storage/ports/mod.rs` (`ManifestReader::head_manifest`, `ManifestReader::get_manifest`)
- Application callers: `src/application/manifest_read.rs` (`ManifestReadService`), `src/manifest_lifecycle.rs` (`ManifestLifecycleService`)
- HTTP API route handlers: `src/http_api/handlers.rs` (`manifest_by_reference`)
- Focused characterization tests: `src/storage/fs/tests.rs`

### Characterization Scope and Exclusions
- **In Scope**: Source-grounded characterization of `FsStorage::head_manifest` and `FsStorage::get_manifest`, including file layout, media-type detection, error classifications, repository/digest-to-key mapping, and path containment/symlink behaviors.
- **Explicit Exclusions**:
  - Manifest listing (`list_manifest_digests_page`) is strictly excluded (its error suppression, pagination, sorting, and cursor semantics require a separate slice).
  - Unapproved listing limits (such as 10,000 entries / 1 MiB) are unapproved and not implemented or endorsed.
  - Tag reads and tag mutations (`head_tag`, `get_tag`, `mutate_tag`, `delete_tag`).
  - Manifest mutations (`put_manifest`, `delete_manifest`).
  - Production cutover, router changes, or new configuration flags.

---

## Observed Behavior: `head_manifest` and `get_manifest`

The table below summarizes observed behaviors derived directly from source inspection and verified by focused characterization tests in `src/storage/fs/tests.rs`.

| Scenario | Input Condition | `head_manifest` Result | `get_manifest` Result | Supporting Test Name |
| :--- | :--- | :--- | :--- | :--- |
| **Valid OCI Manifest** | Valid JSON with explicit `mediaType: "application/vnd.oci.image.manifest.v1+json"` | `Ok(ManifestMeta { size: len, media_type })` | `Ok((ManifestMeta, Bytes))` where `Bytes` matches payload byte-for-byte and length matches `size` | `test_manifest_read_representative_valid_oci_manifest` |
| **Media Type: Explicit** | Valid JSON object with custom `"mediaType": "application/vnd.custom.v1+json"` | `Ok(ManifestMeta { media_type: "application/vnd.custom.v1+json", size })` | `Ok((ManifestMeta, Bytes))` | `test_manifest_read_media_type_detection_variants` |
| **Media Type: Missing** | Valid JSON object without `mediaType` field | `Ok(ManifestMeta { media_type: "application/vnd.oci.image.manifest.v1+json", size })` (default fallback) | `Ok((ManifestMeta, Bytes))` with default fallback | `test_manifest_read_media_type_detection_variants` |
| **Media Type: Non-String** | Valid JSON object where `"mediaType": 42` (integer) | `Ok(ManifestMeta { media_type: "application/vnd.oci.image.manifest.v1+json", size })` (falls back via `v.as_str()`) | `Ok((ManifestMeta, Bytes))` with default fallback | `test_manifest_read_media_type_detection_variants` |
| **Media Type: Scalar JSON** | JSON payload is a JSON scalar (e.g. `"just a string"`, `123`) | `Ok(ManifestMeta { media_type: "application/vnd.oci.image.manifest.v1+json", size })` (no `mediaType` key, fallback) | `Ok((ManifestMeta, Bytes))` with default fallback | `test_manifest_read_media_type_detection_variants` |
| **Empty File** | 0-byte file (`bytes.len() == 0`) | `Err(StorageError::Internal { kind: CorruptData, .. })` (`serde_json::from_slice` EOF) | `Err(StorageError::Internal { kind: CorruptData, .. })` | `test_manifest_read_empty_and_malformed_payloads_classify_as_corrupt_data` |
| **Malformed Non-JSON** | Bytes cannot be parsed as JSON (`not valid json`) | `Err(StorageError::Internal { kind: CorruptData, .. })` | `Err(StorageError::Internal { kind: CorruptData, .. })` | `test_manifest_read_empty_and_malformed_payloads_classify_as_corrupt_data` |
| **Missing Repo Dir** | `repos/<name>` does not exist | `Err(StorageError::NotFound)` | `Err(StorageError::NotFound)` | `test_manifest_read_missing_paths_return_not_found` |
| **Missing Manifests Dir** | `repos/<name>/manifests` does not exist | `Err(StorageError::NotFound)` | `Err(StorageError::NotFound)` | `test_manifest_read_missing_paths_return_not_found` |
| **Missing Manifest File** | `repos/<name>/manifests/<hex>` does not exist | `Err(StorageError::NotFound)` | `Err(StorageError::NotFound)` | `test_manifest_read_missing_paths_return_not_found` |
| **Non-Directory Repo** | `repos/<name>` is a regular file | `Err(StorageError::Internal { kind: Io, .. })` (`ENOTDIR`) | `Err(StorageError::Internal { kind: Io, .. })` (`ENOTDIR`) | `test_manifest_read_nondirectory_components_return_io` |
| **Non-Directory Manifests** | `repos/<name>/manifests` is a regular file | `Err(StorageError::Internal { kind: Io, .. })` (`ENOTDIR`) | `Err(StorageError::Internal { kind: Io, .. })` (`ENOTDIR`) | `test_manifest_read_nondirectory_components_return_io` |
| **Directory Manifest File** | Target `<hex>` is a directory instead of a regular file | `Err(StorageError::Internal { kind: Io, .. })` (`EISDIR`) | `Err(StorageError::Internal { kind: Io, .. })` (`EISDIR`) | `test_manifest_read_nondirectory_components_return_io` |
| **Permission Denied** | Target `<hex>` has permissions `0o000` (unreadable) | `Err(StorageError::Internal { kind: Io, .. })` (`EACCES`, never `NotFound`) | `Err(StorageError::Internal { kind: Io, .. })` (`EACCES`, never `NotFound`) | `test_manifest_read_permission_denied_ignored` |
| **Symlink Outside Root** | Target file or intermediate directory is a symlink pointing outside root | `Ok(...)` (follows symlink out of root; legacy containment gap) | `Ok(...)` (follows symlink out of root; legacy containment gap) | `test_manifest_read_containment_symlink_traversal` |
| **Dangling Symlink** | Symlink pointing to nonexistent file | `Err(StorageError::NotFound)` (kernel returns `ENOENT`) | `Err(StorageError::NotFound)` (kernel returns `ENOENT`) | `test_manifest_read_containment_symlink_traversal` |
| **Path Traversal Escape** | Repo string contains `../../` segments | Follows traversal if target exists (legacy `manifest_path` gap) | Follows traversal if target exists (legacy `manifest_path` gap) | `test_manifest_read_unvalidated_caller_path_traversal_gap` |

---

## Contract Analysis: Why `head_manifest` Requires Payload Acquisition

Under the current production contract in `src/storage/fs.rs`:

```rust
async fn head_manifest(&self, name: &str, digest: &Digest) -> Result<ManifestMeta, StorageError> {
    let path = self.manifest_path(name, digest);
    let bytes = match tokio::fs::read(&path).await {
        Ok(b) => b,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(StorageError::NotFound);
        }
        Err(err) => return Err(StorageError::io(err.to_string())),
    };
    let media_type = self.detect_manifest_media_type(&bytes).await?;
    Ok(ManifestMeta {
        size: bytes.len() as u64,
        media_type,
    })
}
```

Key contractual implications:
1. **Media Type Derivation**: Manifests do not store media type in file names or filesystem metadata. Media type is parsed from the JSON body (`detect_manifest_media_type`) looking for top-level `"mediaType"`. If the field is missing, non-string, or the JSON is a scalar, it defaults to `"application/vnd.oci.image.manifest.v1+json"`.
2. **Size Derivation**: `ManifestMeta.size` is derived directly from `bytes.len()` of the bytes actually read into memory.
3. **Payload Integrity Validation**: Because `detect_manifest_media_type` parses JSON via `serde_json::from_slice`, empty files (0 bytes) or corrupted non-JSON payloads return `StorageErrorKind::CorruptData`.
4. **Conclusion**: Filesystem metadata inspection alone (`fstat`/`stat`) cannot replace `head_manifest` while preserving existing behavior:
   - Metadata inspection cannot determine `media_type` without payload acquisition.
   - Metadata inspection cannot detect empty or corrupt manifest payloads and would incorrectly return `Ok` for 0-byte or malformed files, violating the contract.
   - Separate reads do not guarantee snapshot consistency; reading metadata followed by reading payload can observe concurrent mutations.
   - Files are not inherently immutable merely because their filename is a digest.

---

## Repository and Digest Key Construction

### File Layout
Manifest files are stored at:
```text
<storage-root>/repos/<repository-name>/manifests/<digest-hex>
```

Differences from Blob CAS layout:
- **Blobs**: Sharded and algorithm-prefixed: `<storage-root>/blobs/<algorithm>/<prefix2>/<digest-hex>`.
- **Manifests**: Unsharded flat directory per repository: `<storage-root>/repos/<repository-name>/manifests/<digest-hex>`.
- Supported digest algorithms: `Digest::parse` supports `sha256` (64-character hex) and `sha512` (128-character hex). The filename is strictly `<digest.hex()>` without algorithm prefix (e.g. `sha256:`).

### Validation Guarantees and Caller Categorization
Inspection of caller paths across `registry-rust` reveals distinct validation tiers:

1. **HTTP Boundary Validation (`src/http_api/handlers.rs`)**:
   - In route handler `manifest_by_reference` (lines 422–435), HTTP requests check repository syntax before routing:
     ```rust
     if !is_valid_repo_name(name) {
         return errors::name_invalid().into_response();
     }
     ```
   - Helper `is_valid_repo_name` in `src/registry/validation.rs:7-9` delegates directly to `CanonicalRepoName::parse(name).is_ok()`.
   - Incoming HTTP callers through this handler cannot supply arbitrary paths containing `../` or uppercase/invalid characters.

2. **Application Service Validation (`src/application/manifest_read.rs`, `src/manifest_lifecycle.rs`)**:
   - `ManifestReadService` is an application-level query service (not an HTTP handler).
   - In `ManifestReadService::head_manifest` (line 393) and `ManifestReadService::get_manifest` (line 498), execution begins with `self.resolve_reference_digest(repo, reference, ...)` (line 271).
   - In `resolve_reference_digest` (lines 278–282), repository syntax is validated explicitly:
     ```rust
     CanonicalRepoName::parse(repo).map_err(|source| ManifestReadError::InvalidRepoName {
         name: repo.to_string(),
         source,
     })?;
     ```
   - Similarly, in `src/manifest_lifecycle.rs:869`, `ManifestLifecycleService::publish_internal` validates `CanonicalRepoName::parse(&repo)`.

3. **Other Subsystem and Direct Storage Calls**:
   - Not every internal subsystem repeats repository validation at each storage callsite. Subsystems such as `src/supervisor.rs:989-1000` (`collect_protected_blobs_for_manifest`), `src/blob_ref_index.rs:710`, `src/blob_delete_safety.rs:42`, and `src/membership_migration.rs:23` pass repository strings directly to storage operations, relying on prior invariants or persisted state.
   - At the storage boundary, `FsStorage::head_manifest` and `FsStorage::get_manifest` (`src/storage/fs.rs:839, 859`) accept raw `name: &str` without validation.
   - The internal path constructor `FsStorage::manifest_path` (`src/storage/fs.rs:416-423`) performs raw `Path::join`:
     ```rust
     fn manifest_path(&self, name: &str, digest: &Digest) -> PathBuf {
         self.root.join("repos").join(name).join("manifests").join(digest.hex())
     }
     ```
   - **Containment Gap**: Direct callers supplying a repository string containing `../` segments can traverse outside `<storage-root>/repos` if the target path exists on disk.

### Contract Separation: `ObjectKey` vs Canonical OCI Repository Name
- **Canonical OCI Repository Name (`CanonicalRepoName`)**: Validates adherence to the normative OCI distribution specification repository grammar (lowercase alphanumeric characters, valid separators `.` `_` `-` `__` `---`, component segment bounds, maximum total length of 255).
- **Generic Object Key (`storage_core::ObjectKey`)**: Validates generic storage-layer relative path syntax (rejects leading slashes, empty segments, absolute paths, and `..` via `ObjectKeyError::DotDotSegment`).
- These are distinct contracts serving different architectural purposes.
- Focused characterization tests demonstrating that representative single-segment (`testrepo`) and multi-segment (`library/ubuntu`) names map cleanly to `ObjectKey` verify compatibility for those specific cases, but do not prove that every valid OCI repository name necessarily satisfies all constraints of every future `ObjectKey` mapping without explicit conversion rules.

---

## Proposed Future Contained-Read Boundary (Proposal Only)

> [!NOTE]
> This section is strictly a proposal for future work. Production code remains completely unchanged in this slice.

To eliminate path traversal and symlink escape vulnerabilities in manifest reads:
1. **Relative `ObjectKey` Construction**:
   - Construct manifest keys as `ObjectKey` instances: `repos/<valid-repo-path>/manifests/<hex>`.
   - Rejection of invalid characters and `..` at key construction time.
2. **Contained Filesystem Resolution**:
   - Route manifest file opens through `storage-fs` contained reader facilities (e.g. `openat2` with `RESOLVE_BENEATH`/`RESOLVE_NO_SYMLINKS` on Linux or verified component-by-component open).
3. **Behavior Changes Requiring Separate Acceptance**:
   - **Symlink Rejection**: Currently, symlinks pointing outside the storage root (or inside the root) are transparently followed by `tokio::fs::read`. Rejecting symlinks alters this legacy behavior and requires explicit acceptance.
   - **Direct Caller Name Validation**: Rejecting dot-dot segments in storage layer entrypoints changes behavior for non-canonical test or internal callers.
   - **Payload Buffering vs Streaming**: Since `head_manifest` must inspect the JSON payload to extract `mediaType`, any bounded reader must read the manifest payload safely (e.g. up to a defined maximum manifest size, rather than unbounded memory allocation).

---

## Canonical Gate Status

All canonical quality gates remain explicitly **OPEN**:
- **O-03**: Key and continuation-token contracts.
- **O-04**: Filesystem write durability and containment.
- **O-05**: Broader filesystem read containment.
- **O-06**: Typed AWS mapping and pinned-MinIO evidence.
- **O-13**: Hosting, distribution, and release strategy.
- **O-15**: Non-Linux verification.
- **O-16**: Earlier Slice 11 audit/test-inventory evidence.
- **D-06**: Broader extraction, cutover, compatibility, and distribution acceptance.
