# ADR-007: Manifest Compatibility Re-Export Deprecation and Test Consolidation

* **Status:** Accepted
* **Date:** 2026-09-01
* **Authors:** Senior Software Architect (OCI Distribution & Storage Systems)
* **Scope:** Manifest Lifecycle Domain Consolidation, Ghost Compatibility Shim Deprecation, Public Rust API Compatibility, Assertion-Level Test Migration
* **Refines:** `docs/architecture/adr-001-first-refactoring-boundary.md`, `docs/architecture/adr-002-application-service-boundary.md`, `docs/architecture/adr-003-storage-capability-ports.md`, `docs/architecture/adr-004-application-read-services.md`, `docs/architecture/adr-005-server-runtime-composition-root.md`, `docs/architecture/adr-006-cli-runtime-composition.md`, `docs/architecture/current-code-assessment.md`

---

## 1. Context & Problem Statement

Prior to this architectural slice, manifest operations and reference extraction had evolved through multiple stages:
1. Manifest parsing and reference extraction were consolidated in `src/manifest_refs.rs`.
2. The durable lifecycle journal, crash recovery, and CAS manifest publication engine were established in `src/manifest_lifecycle.rs` (`ManifestLifecycleService`).
3. Transport routing and high-level publication coordination were extracted to `src/application/manifest.rs` (`ManifestMutationService`).

Despite these developments, **`src/manifest_publication.rs` remained in `src/` as a 993-line ghost module**:
* **Technical Debt & Redundancy:** 963 lines of `src/manifest_publication.rs` were duplicate inline unit tests dating from before the centralized integration test suite in `tests/manifest_lifecycle_tests.rs`.
* **Internal Coupling:** Production handlers (`src/http_api/handlers.rs`) retained a legacy import referencing `crate::manifest_publication::MAX_MANIFEST_SIZE`.
* **Public SemVer Risk:** Because `registry-rust` is compiled both as a binary and a library (`crate_types = ["lib", "bin"]`), the top-level declaration `pub mod manifest_publication;` in `src/lib.rs` and its public aliases (`ManifestPublisher`, `PublishManifestError`, etc.) constituted an exposed public API surface. Unconditionally deleting the module would break external consumers relying on those paths.

---

## 2. Decision: Deprecated Re-Export Shim & Assertion-Level Test Migration

### 2.1 Preserving Public API Compatibility (`src/manifest_publication.rs` & `src/lib.rs`)

We retain `src/manifest_publication.rs` as a minimal, 10-line deprecated compatibility module re-exporting all nine original public items:

```rust
//! Backwards-compatibility re-exports and aliases for manifest lifecycle types.
//!
//! This module provides compatibility aliases for external callers.
//! Internal codebase callers should use [`crate::manifest_lifecycle`] directly.

pub use crate::manifest_lifecycle::{
    is_supported_manifest_media_type, ManifestLifecycleError as PublishManifestError,
    ManifestLifecycleService, ManifestLifecycleService as ManifestPublisher, ProxyEvictionResult,
    ProxyPublicationEvidence, PublishManifestRequest, PublishedManifest, MAX_MANIFEST_SIZE,
};
```

In `src/lib.rs`, the module is marked with the standard Rust deprecation attribute:
```rust
pub mod manifest_lifecycle;
#[deprecated(note = "use crate::manifest_lifecycle instead")]
pub mod manifest_publication;
pub mod manifest_refs;
```

### 2.2 Routing All Internal Production Callers to `src/manifest_lifecycle.rs`

All internal production files are updated to reference `crate::manifest_lifecycle` directly. `src/http_api/handlers.rs` now imports `crate::manifest_lifecycle::MAX_MANIFEST_SIZE`. Zero internal production callers depend on `manifest_publication`.

### 2.3 Assertion-Level Test Migration & Deduplication

All 11 legacy tests previously embedded in `src/manifest_publication.rs` were audited at the assertion level against `tests/manifest_lifecycle_tests.rs` and `tests/gc_adversarial_coordination_tests.rs`:

1. **Unique Scenario Migration:** Eight unique behavioral scenarios were migrated into `tests/manifest_lifecycle_tests.rs`:
   * `test_migrated_stage_1_reference_parsing_fails`
   * `test_migrated_reference_kind_validation_blobs_vs_child_manifests`
   * `test_migrated_concurrent_immutable_tag_race_safety`
   * `test_migrated_immutable_tag_idempotent_republish`
   * `test_migrated_dirty_index_state_rebuild_after_crash`
   * `test_migrated_concurrent_overwrite_publications_converge`
   * `test_migrated_same_digest_retry_after_dirty_rebuilds_and_succeeds`
   * `test_migrated_immutable_conflict_retains_content_addressed_manifest_and_referrer`
2. **Public API Compatibility Proof:** Added `test_public_api_manifest_publication_compatibility` to compile-time verify all nine public compatibility names and type shapes.
3. **Exact Duplicates Retired:** Legacy test `test_gc_exclusion_with_shared_coordinator` and failpoint tests with direct equivalents in `tests/gc_adversarial_coordination_tests.rs` and `tests/manifest_lifecycle_tests.rs` were retired.

---

## 3. Consequences & Inventory Impact

### Positive
1. **Dramatic Code Reduction:** `src/manifest_publication.rs` line count dropped from 993 lines to 10 lines (98.9% reduction in technical debt).
2. **Production / Test Separation:** 963 lines of test fixtures were removed from production compilation units (`src/lib.rs`), improving compilation performance.
3. **Zero SemVer Breakage:** All legacy public paths remain functional with deprecation notices.
4. **Canonical Single Source of Truth:** `src/manifest_lifecycle.rs` is now the sole authoritative source for lifecycle publication and journal recovery.

### Future Removal Policy
The deprecated `registry_rust::manifest_publication` module will remain in place across all `0.8.x` patch releases and will only be completely removed in a future explicitly authorized major release.
