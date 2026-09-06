# ADR-009: Structured Storage Error Taxonomy and Stringly-Typed Tunneling Resolution

* **Status:** Accepted (Implemented in commit 49d405483b6ea729fc6a4e9a0a99177ec37c2bfd; Release boundary: 0.9.0; not yet tagged, published, or distributed)
* **Date:** 2026-09-02
* **Authors:** Senior Software Architect (OCI Distribution & Storage Systems)
* **Scope:** Storage Error Taxonomy, Error Control Flow, Internal Error Structuring, Elimination of String-Matching Control Flow, D-06 Resolution
* **Refines:** `docs/architecture/adr-001-first-refactoring-boundary.md`, `docs/architecture/adr-002-application-service-boundary.md`, `docs/architecture/adr-003-storage-capability-ports.md`, `docs/architecture/adr-004-application-read-services.md`, `docs/architecture/adr-005-server-runtime-composition-root.md`, `docs/architecture/adr-006-cli-runtime-composition.md`, `docs/architecture/adr-007-manifest-compatibility-consolidation.md`, `docs/architecture/adr-008-http-transport-test-topology.md`, `docs/architecture/current-code-assessment.md`

---

## 1. Context & Problem Statement

Prior to this architectural change, `StorageError::Internal(String)` was used across the repository (233+ occurrences across 13 files) as a catch-all variant for every failure mode that did not match one of the early dedicated domain errors (`NotFound`, `DigestMismatch`, `Unsupported`, `TooLarge`, `InsufficientStorage`, `TagAlreadyExists`, `ExclusiveWriterLocked`, `InvalidRepoName`, `MigrationRequired`).

This created technical debt item **D-06 ("Stringly-Typed Internal Error Tunneling")**:
1. **Loss of Semantic Error Category:** Low-level OS I/O errors (`tokio::fs::*`), remote S3 transport/SDK errors (`aws_sdk_s3::error::SdkError`), HTTP 403 access denied errors, corrupt disk data / invalid JSON payloads, memory serialization failures, configuration deficiencies, and state-machine invariant violations were all tunneled into untyped `String` payloads.
2. **Fragile String-Based Error Control Flow:** Upstream logic and storage adapters were forced to perform substring checks on error messages to guide control flow. Specifically, in `src/storage/s3.rs`, conditional mutations (such as `set_membership_candidate`, `clear_membership_candidate`, `put_object_conditional`, and `delete_object_conditional`) inspected error strings with checks like `err.contains("412") || err.contains("PreconditionFailed") || err.contains("AtLeastOnePreconditionFailed")`.
3. **Impaired Observability & Diagnostic Fidelity:** Higher-level callers (such as `UploadCoordinator`, `RuntimeMutationAuthority`, and CLI maintenance commands) could not distinguish recoverable transient backend/network errors from unrecoverable data corruption without regex/string heuristics.

---

## 2. Decision: Structured Storage Error Taxonomy (`StorageErrorKind`)

We resolve D-06 comprehensively by introducing an evidence-based, 8-variant taxonomy enum `StorageErrorKind` and transforming `StorageError::Internal` from a tuple variant into a structured variant containing both `kind: StorageErrorKind` and `message: String`.

### 2.1 Taxonomy Enum Definition

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageErrorKind {
    /// Filesystem or OS-level I/O failure (e.g. read/write/rename/create_dir, file locks).
    Io,
    /// Remote storage service, network transport, or SDK communication failure.
    Backend,
    /// Permission or access denied by OS or backend (e.g. HTTP 403, EACCES).
    PermissionDenied,
    /// Corrupt, unparseable, or malformed data stored in the repository, index, or metadata.
    CorruptData,
    /// Serialization failure when preparing in-memory structures for storage.
    Serialization,
    /// Invalid storage configuration or unsupported backend capability.
    Configuration,
    /// Concurrency or precondition conflict on storage resources (e.g. CAS ETag mismatch, active lease).
    Conflict,
    /// Internal invariant violation or inconsistent state-machine transition.
    InternalInvariant,
}
```

### 2.2 Preserved Display Formatting and Outward Stability

To maintain exact outward compatibility with log processors, metrics, and existing assertions, `StorageError::Internal` preserves the exact Display format `internal error: {message}`:

```rust
#[derive(Clone, Debug, Error)]
pub enum StorageError {
    #[error("not found")]
    NotFound,
    #[error("digest mismatch")]
    DigestMismatch,
    #[error("unsupported")]
    Unsupported,
    #[error("too large")]
    TooLarge,
    #[error("insufficient storage")]
    InsufficientStorage,
    #[error("tag already exists")]
    TagAlreadyExists,
    #[error("exclusive writer lock held by another deployment/instance: {0}")]
    ExclusiveWriterLocked(String),
    #[error("invalid repository name: {0}")]
    InvalidRepoName(String),
    #[error("migration required: {0}")]
    MigrationRequired(String),

    #[error("internal error: {message}")]
    Internal {
        kind: StorageErrorKind,
        message: String,
    },
}
```

### 2.3 Ergonomic Constructor Helpers and Accessors

To ensure clear, readable code and prevent accidental re-introduction of string-only errors, `StorageError` provides typed constructors and accessors:

* `StorageError::internal(kind, message)` — General structured internal error constructor.
* `StorageError::io(err)` — Filesystem / OS I/O error constructor (`StorageErrorKind::Io`).
* `StorageError::backend(err)` — Remote storage / SDK error constructor (`StorageErrorKind::Backend`).
* `StorageError::permission_denied(err)` — Permission denied constructor (`StorageErrorKind::PermissionDenied`).
* `StorageError::corrupt_data(err)` — Malformed or unparseable stored data constructor (`StorageErrorKind::CorruptData`).
* `StorageError::serialization(err)` — In-memory serialization error constructor (`StorageErrorKind::Serialization`).
* `StorageError::configuration(err)` — Configuration deficiency constructor (`StorageErrorKind::Configuration`).
* `StorageError::conflict(err)` — CAS / lease concurrency conflict constructor (`StorageErrorKind::Conflict`).
* `StorageError::internal_invariant(err)` — Invariant violation constructor (`StorageErrorKind::InternalInvariant`).
* `err.internal_kind() -> Option<StorageErrorKind>` — Accessor returning `Some(kind)` for `Internal` and `None` for dedicated variants.
* `err.message() -> Option<&str>` — Accessor returning the inner message across variants.

---

## 3. Elimination of String-Matching Control Flow

All semantic string parsing for error recovery was eliminated:

1. **S3 Precondition & ETag Handling:** In `src/storage/s3.rs`, `put_object_conditional` and `delete_object_conditional` directly inspect the underlying `aws_sdk_s3::error::SdkError::ServiceError` status code (412) and error code (`"PreconditionFailed"`, `"AtLeastOnePreconditionFailed"`).
2. **CAS Candidate Conflict Transitions:** In `set_membership_candidate` and `clear_membership_candidate`, concurrent modification handling matches `Err(StorageError::Internal { kind: StorageErrorKind::Conflict, .. })` or `Err(StorageError::TagAlreadyExists)` directly without string inspection.
3. **Deterministic Testing:** In unit and integration test suites, tests assert specific `StorageErrorKind` variants (e.g. `err.internal_kind() == Some(StorageErrorKind::CorruptData)` for malformed hex filenames or corrupted JSON records).

---

## 4. Migration Scope & Verification

Every single constructor and match site across the repository was classified:

| File | Scope & Classifications |
| :--- | :--- |
| [`src/storage/mod.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/mod.rs) | `StorageErrorKind` taxonomy definition, constructor helpers, configuration & I/O errors |
| [`src/storage/fs.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs) | 119 call sites classified into `Io`, `CorruptData`, `Serialization`, `InternalInvariant`, `PermissionDenied` |
| [`src/storage/s3.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/s3.rs) | 65 call sites classified into `Backend`, `PermissionDenied`, `Configuration`, `Serialization`, `CorruptData`, `Conflict` |
| [`src/storage/tests.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/tests.rs) | Dedicated unit test suite verifying taxonomy display, serde, accessors, and variant stability |
| [`src/storage/fs/tests.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/tests.rs) | Typed pattern assertions on `StorageErrorKind::CorruptData` and `StorageErrorKind::Io` |
| [`src/storage/s3/tests.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/s3/tests.rs) | Typed error injections for `Backend`, `PermissionDenied`, `Conflict`, `InternalInvariant` |
| [`src/blob_delete_safety.rs`](file:///home/dietmar/devel/rust/registry-rust/src/blob_delete_safety.rs) | Unparseable manifest handling mapped to `CorruptData`, ledger invariants |
| [`src/membership_migration.rs`](file:///home/dietmar/devel/rust/registry-rust/src/membership_migration.rs) | Corrupt manifest mapped to `CorruptData`, concurrent migrator lease conflict mapped to `Conflict` |
| [`src/storage/mutation_authority.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/mutation_authority.rs) | Invalid confirmation tokens mapped to `PermissionDenied` |
| [`src/runtime.rs`](file:///home/dietmar/devel/rust/registry-rust/src/runtime.rs) | Unwind release failure mapped to `Backend` |
| [`src/upload_coordinator.rs`](file:///home/dietmar/devel/rust/registry-rust/src/upload_coordinator.rs) | Pin acquisition/renewal I/O mapped to `Io`, missing CAS blobs mapped to `CorruptData` |
| [`tests/application_read_tests.rs`](file:///home/dietmar/devel/rust/registry-rust/tests/application_read_tests.rs) | Mock upload transitions mapped to `Io` and `Backend` |
| [`tests/supervisor_and_command_tests.rs`](file:///home/dietmar/devel/rust/registry-rust/tests/supervisor_and_command_tests.rs) | Teardown release failures mapped to `Backend` |
| [`tests/support/s3_mock.rs`](file:///home/dietmar/devel/rust/registry-rust/tests/support/s3_mock.rs) | Missing part invariant mapped to `InternalInvariant` |

---

## 5. Public-API, SemVer, and Serialization Compatibility Analysis

### 5.1 Rust Source-Level Breaking Change (SemVer 0.9.0 Release Boundary)

The transformation of `StorageError::Internal` from a tuple variant `StorageError::Internal(String)` to a struct variant `StorageError::Internal { kind: StorageErrorKind, message: String }` is a **source-breaking change** for downstream Rust callers that construct or pattern-match the public variant:

* **Source Incompatibility:** Preserving `Display` output and public function return signatures does **not** preserve Rust source compatibility. Downstream code that previously matched `StorageError::Internal(msg)` or constructed `StorageError::Internal(msg)` will fail compilation.
* **`#[non_exhaustive]` Limitation:** Adding `#[non_exhaustive]` to `StorageError` (or its variants) prevents future breakage from newly added variants, but it does **not** retroactively preserve callers that constructed or matched the legacy tuple variant.
* **Pattern Matching Migration:**
  ```rust
  // Legacy (source-broken):
  match err {
      StorageError::Internal(msg) => { ... }
  }

  // Migrated:
  match err {
      StorageError::Internal { kind, message } => { ... }
      // Or to match message while ignoring kind:
      StorageError::Internal { ref message, .. } => { ... }
  }
  ```
* **Construction Migration:**
  ```rust
  // Legacy (source-broken):
  StorageError::Internal("disk error".to_string())

  // Migrated:
  StorageError::io("disk error")
  // Or:
  StorageError::internal(StorageErrorKind::Io, "disk error")
  ```
* **SemVer Assessment:** Under Cargo/Semantic Versioning rules for pre-1.0 releases (`0.y.z`), breaking API changes must increment the minor version number (i.e. from `0.8.18` to `0.9.0`, not `0.8.19`). The implementation was completed in Slice 11 at commit `49d405483b6ea729fc6a4e9a0a99177ec37c2bfd`, and this accepted architectural decision establishes the `0.9.0` release boundary in `Cargo.toml`. The crate is now versioned at `0.9.0` as the required release boundary; the release has not been tagged, published, or distributed. Downstream consumers must migrate to the constructor helpers (`StorageError::io`, `backend`, `corrupt_data`, etc.) and the accessor methods (`err.internal_kind()`, `err.message()`).

### 5.2 Outward Wire, Display, and Protocol Compatibility

While Rust source compatibility is broken for direct variant matchers, outward protocol and display behavior are strictly preserved:
1. **Display Formatting:** `StorageError` implements `Display` producing `"internal error: {message}"`, ensuring log parsers, metrics exporters, and status messages observe identical output.
2. **Golden Assertions for Pre-existing Variants:** Dedicated unit tests verify that all pre-existing variants retain exact outward display text (`"not found"`, `"digest mismatch"`, `"unsupported"`, `"too large"`, `"insufficient storage"`, `"tag already exists"`, `"exclusive writer lock held by another deployment/instance: {s}"`, `"invalid repository name: {s}"`, `"migration required: {s}"`).
3. **HTTP Transport Status Mapping:** In `src/http_api/handlers.rs`, `StorageError::Internal` continues to map to HTTP 500 Internal Server Error, ensuring zero behavioral deviation for external OCI HTTP clients.
4. **Serialization Infallibility Contract:** Production domain entities (`RepoBlobMembershipRecord`, `MigrationCheckpointRecord`, `UploadSessionDoc`) contain only primitive string and numeric fields, making `serde_json::to_vec` serialization infallible during normal operation except in the event of heap allocation exhaustion. The `StorageErrorKind::Serialization` category exists to handle any serializer failure injected or encountered in format encoders.
5. **Operation-Aware S3 Classification:** S3 error mapping enforces operation-aware semantics:
   - **GET / HEAD:** Missing objects (HTTP 404, `NoSuchKey`, `NotFound`) map to `StorageError::NotFound`; HTTP 403 / `AccessDenied` maps to `StorageErrorKind::PermissionDenied`; transport/5xx map to `StorageErrorKind::Backend`.
   - **Ordinary PUT:** HTTP 403 / `AccessDenied` maps to `StorageErrorKind::PermissionDenied`; all other errors map to `StorageErrorKind::Backend` (preserving baseline behavior where PUT 404/412 are not mapped to `NotFound` or `Conflict`).
   - **Conditional Publication:** Precondition failure (HTTP 412) preserves dedicated variant `StorageError::TagAlreadyExists` or `StorageErrorKind::Conflict`.
   - **Conditional Deletion:** Preserves `ConditionalDeleteResult::NotFound` for 404 and `ConditionalDeleteResult::PreconditionFailed` for 412.
6. **Serialization Representation:** `StorageError` itself does not derive `Serialize` or `Deserialize` and is not persisted in database or S3 metadata. Persisted formats store only domain entities (`FinalizedReceipt`, `RepoBlobMembershipRecord`, `MigrationCheckpointRecord`). The newly introduced `StorageErrorKind` derives `Serialize`/`Deserialize` as an available representation for future structured telemetry and logging, and is not currently persisted into storage schemas.

---

## 6. Consequences

* **Positive:** Complete elimination of string-matching control flow; strongly typed errors across filesystem and S3 storage adapters; structured error classification enabling deterministic error handling, retries, and telemetry.
* **Breaking Change:** Rust callers pattern-matching on `StorageError::Internal` must update match arms to `{ kind, message }` or use the `err.internal_kind()` / `err.message()` accessors. Requires `0.9.0` SemVer release boundary.
* **Outward Compatibility:** Internal-error `Display` format (`"internal error: {message}"`) and HTTP 500 status mapping are preserved.
* **Operation Awareness:** Operation-specific error semantics for GET, HEAD, PUT, conditional publication, and conditional deletion are strictly preserved.
