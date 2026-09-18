# Architecture Assessment: Current-State Analysis & Strategic Technical Roadmap

**Repository:** `registry-rust`  
**Baseline Commit:** `efdae2ee77ee622c81b6b157da593659769301ce`  
**Assessment Date:** 2026-08-26  
**Auditor:** Senior Software Architect (OCI Distribution & Storage Systems)

**Living inventory (supersedes leftover “current remaining work” in this file):** [`current-state.md`](current-state.md) at `master` `9405991` (2026-09-18). Document index: [`README.md`](README.md).

---

## Addendum: Code-aligned status at `9405991` (2026-09-18)

This assessment’s baseline sections (§1–§8 diagrams and traces, §11 original slice numbering) describe **2026-08-26** structure. They are not a description of HEAD. The §9 register **was updated** in this addendum pass (D-01/D-02 are no longer “Planned”).

At `9405991`:

- **Wave 1 (ADRs 001–009, slices 1–11) landed for production consumers.** OCI blob/manifest/catalog/tag/referrer routes go through seven application services. `StorageWiring::from_backend` takes **port** trait bounds, not `dyn Storage`. `ConsistencyCoordinator`, CLI `CommandPolicy`, test sidecars, and `StorageErrorKind` are in tree. Admin GC and token mint still read `AppState` config/`gc_service`.
- **Wave 2 filesystem/ObjectStore cutovers landed after this assessment’s remaining-gap notes.** Contained mutation authorities (`f555e5f`), durability barriers (`8c0ac64`), shared domains phases 3–8, streaming CAS listing (`9405991`). The upload reaper is not ambient `read_dir`.
- **Residual coupling (code):** omnibus `impl Storage` remains on adapters (tests + same concrete type as the ports); `AppState` still a process bag (services + proxy/GC/semaphores); pathname `atomic_write_file` for `meta/` membership **writes**; pathname repo `flock`; repository-existence probe not on `ObjectStore`.
- **Quality gates** in the later FS notes remain **OPEN** as acceptance criteria. That is not the same as “cutovers did not happen.” Assessment D-06 (stringly `StorageError`) is **Resolved (ADR-009)**; filesystem-doc D-06 is a different ID.

Updated debt-register statuses are in §9 below.

---

## Post-Slice-10 Implementation Status (Slice 10 Implemented)

> **Implementation Note (Post-Slice-10 Verification State):**
> Following Slices 1 through 9, **Slice 10 (Filesystem Storage Private Test Extraction)** was implemented and verified under the governing policy established in ADR-008.
>
> * **Test Topology Standard (`src/storage/fs/tests.rs`):** Extracted the private inline test module (baseline `src/storage/fs.rs` 5,103 lines; `#[cfg(test)]` attribute line 3676, declaration `mod tests {` line 3677, closing brace line 5103; inclusive span 1,428 lines; test body lines 3678–5102 comprising 1,425 lines) into the dedicated file-backed sidecar `src/storage/fs/tests.rs` (1,409 lines, formatted under `rustfmt --edition 2024`).
> * **Production Module Declaration & Visibility:** Declared in `src/storage/fs.rs` via `#[cfg(test)] #[path = "fs/tests.rs"] mod tests;` (2 insertions, 1,427 deletions per `git diff --numstat`). Visibility remains strictly private (`mod tests;`), identical to baseline.
> * **Production Content & Size Reduction:** Production filesystem storage adapter logic (lines 1–3676) is 100% byte-for-byte identical to baseline. Tracked `src/storage/fs.rs` reduced from 5,103 to 3,678 lines (27.9% reduction). Total combined source across tracked file and sidecar is 5,087 lines.
> * **Production Test Isolation:** Gated strictly behind `#[cfg(test)]`. No production code or signatures widened; zero test fixtures compiled into release builds (`cargo build --release --locked`).
> * **Unchanged Test Identity & Coverage:** Exactly 28 compiled filesystem unit tests (`storage::fs::tests::*`) verified identical via `diff -u` before and after. Total workspace test count remains exactly **708 tests across 18 binaries**.
> * **Rustfmt Reinlining Equivalence:** Normalized reinlining comparison of `src/storage/fs.rs` with `src/storage/fs/tests.rs` confirmed 100% byte-identical against baseline (`0 diffs`).
> * **Technical Debt Register:** Item **D-04** marked as **Resolved (Slices 8–10)** (in-source test bloat eliminated across `src/http_api/handlers.rs`, `src/storage/s3.rs`, and `src/storage/fs.rs`).

---

## Post-Slice-9 Implementation Status (Slice 9 Implemented)

> **Implementation Note (Post-Slice-9 Verification State):**
> Following Slices 1 through 8, **Slice 9 (S3 Storage Private Test Extraction and Production Test Isolation)** was implemented and verified under the governing policy established in ADR-008.
>
> * **Test Topology Standard (`src/storage/s3/tests.rs`):** Extracted 3,506 lines of inline unit test code (lines 3773–7278) from `src/storage/s3.rs` into the file-backed private unit test module `src/storage/s3/tests.rs` (3,492 lines) declared via `#[cfg(test)] #[path = "s3/tests.rs"] pub(crate) mod tests;`.
> * **Production Test Isolation & Visibility Boundary:** Fully eliminated public production test bloat. The production module declaration is strictly gated behind `#[cfg(test)]`. Normal release compilation (`cargo build --release`) compiles zero test helpers, zero mock drivers (`MockS3Driver`), and zero test log entries (`S3CallLogEntry`).
> * **Integration Test Support Migration (`tests/support/s3_mock.rs`):** Relocated reusable integration mock infrastructure (`MockS3Driver`, `S3CallLogEntry`, and `create_mock_storage()`) into `tests/support/s3_mock.rs`. Integration tests (`tests/supervisor_and_command_tests.rs`, `tests/manifest_lifecycle_tests.rs`, `tests/gc_adversarial_coordination_tests.rs`) import from `tests/support/s3_mock` using public crate interfaces, completely decoupling integration tests from crate internals.
> * **Unchanged Test Identity & Coverage:** Exactly 61 compiled S3 unit tests verified identical via `diff -u` before and after. Total workspace test count remains exactly **708 tests across 18 binaries**.
> * **Production File Size Reduction:** `src/storage/s3.rs` reduced from 7,278 to 3,776 lines (48.1% physical line count reduction), leaving purely production S3 backend driver logic.
> * **Technical Debt Register:** Item **D-04** advanced (S3 storage test bloat extracted, isolated under `#[cfg(test)]`, integration mock infrastructure relocated to `tests/support/`, and production-visible test footprint eliminated; residual filesystem test bloat in `src/storage/fs.rs` deferred to Slice 10).

---

## Post-Slice-8 Implementation Status (ADR-008 Accepted)

> **Implementation Note (Post-Slice-8 Verification State):**
> Following Slices 1 through 7, **Slice 8 (ADR-008: HTTP Transport Test Topology and Production Visibility Preservation)** was implemented and verified.
>
> * **ADR-008 Status:** Accepted.
> * **Test Topology Standard (`src/http_api/handlers/tests.rs`):** Extracted 1,514 lines of inline unit test code (lines 596–2110) from `src/http_api/handlers.rs` into the file-backed child module `src/http_api/handlers/tests.rs` (1,495 lines) declared via `#[cfg(test)] #[path = "handlers/tests.rs"] mod tests;`.
> * **Production Visibility Invariant Preserved:** Zero production functions, structs, enums, or constructors widened in visibility. `AppState::new_test` remains strictly gated under `#[cfg(test)]` without leaking into release builds.
> * **Unchanged Test Identity & Coverage:** Exactly 46 compiled handler unit tests verified identical via `diff -u` before and after. Total workspace test count remains exactly **708 tests across 18 binaries**.
> * **Production File Size Reduction:** `src/http_api/handlers.rs` reduced from 3,050 to 1,538 lines, leaving purely production routing and handler dispatch logic.
> * **Technical Debt Register:** Item **D-04** partially resolved (HTTP handler test bloat eliminated).

---

## Post-Slice-7 Implementation Status (ADR-007 Accepted)

> **Implementation Note (Post-Slice-7 Verification State):**
> Following Slices 1 through 6, **Slice 7 (ADR-007: Manifest Compatibility Re-Export Deprecation and Test Consolidation)** was implemented and verified.
>
> * **ADR-007 Status:** Accepted.
> * **Preserved Public API Compatibility (`src/manifest_publication.rs` & `src/lib.rs`):** Converted `src/manifest_publication.rs` into a minimal, 10-line backwards-compatibility re-export shim containing only documentation, the nine original public exported items (`ManifestPublisher`, `PublishManifestError`, `ManifestLifecycleService`, `PublishManifestRequest`, `PublishedManifest`, `ProxyEvictionResult`, `ProxyPublicationEvidence`, `MAX_MANIFEST_SIZE`, `is_supported_manifest_media_type`), and marked the module deprecated in `src/lib.rs` with `#[deprecated(note = "use crate::manifest_lifecycle instead")]`.
> * **Internal Caller Migration:** All internal production callers (`src/http_api/handlers.rs`) updated to reference `crate::manifest_lifecycle` directly. Zero internal production callers depend on `manifest_publication`.
> * **Assertion-Level Test Migration:** Audited all 11 legacy inline unit tests from `src/manifest_publication.rs`, migrating 8 unique behavioral scenarios and 1 compile-time public API compatibility verification test into `tests/manifest_lifecycle_tests.rs`. Retired redundant duplicate tests whose coverage is fully provided in `tests/gc_adversarial_coordination_tests.rs`.
> * **Massive Ghost Code Reduction:** Reduced `src/manifest_publication.rs` from 993 lines to 10 lines (98.9% technical debt reduction), removing 963 lines of test fixtures from production compilation units.
> * **Verified Test Inventory:** 708 total listed tests across 18 test binaries. **708 passed, 0 failed, 0 ignored** with local MinIO backend healthy.
> * **Technical Debt Register:** Item **D-05** marked as **Resolved (ADR-007)**.

---

## Post-Slice-6 Implementation Status (ADR-006 Accepted)

> **Implementation Note (Post-Slice-6 Verification State):**
> Following Slices 1 through 5, **Slice 6 (ADR-006: CLI Runtime Composition, Command Safety Policies, and Typed Errors)** was implemented and verified.
>
> * **ADR-006 Status:** Accepted.
> * **Command Safety Policy Taxonomy (`src/cli/policy.rs`):** Introduced crate-private `CommandPolicy` classifying all 15 commands and subcommands into `Pure`, `ReadOnly`, `ExclusiveInspection`, `ExclusiveMutation`, `Migration`, and `BreakGlass`.
> * **Non-Mutating Read-Only Invariants:**
>   * `BlobRefIndex::check_path_health` and `BlobRefIndex::open_existing` guarantee that checking a missing or corrupted index creates zero database files, directories, WAL entries, or lock metadata on disk.
>   * `blob-gc plan` and `ref-index check` acquire zero locks (`FsRootLock` or `RuntimeMutationAuthority`) and perform zero auto-repair or writes.
> * **Bounded Maintenance Runtime (`src/cli/runtime.rs`):** Introduced `MaintenanceRuntime` as the bounded composition root for CLI commands, consuming narrow capability ports from `StorageWiring`, enforcing strict lock ordering (`FsRootLock` -> `RuntimeMutationAuthority`), and maintaining single ownership of `RuntimeMutationAuthority` with non-owning delegation to `GcService`.
> * **Typed CLI Errors (`src/cli/errors.rs`):** Introduced `CliError` enum replacing all untyped strings and internal `process::exit` calls, with preserved process exit codes (0 = success, 1 = operational failure, 2 = usage/configuration error) and compound error preservation (`ExecutionAndTeardownFailed`).
> * **Deterministic Unit & Integration Tests:** Added 19 new tests in `tests/supervisor_and_command_tests.rs` covering command policies, pure commands, non-mutating index checks and GC plan, readiness rules, authority unwinding, lock contention on FS and S3, delete failure unwinding, compound teardown failures, break-glass recovery isolation, and live S3 MinIO backend operations.
> * **Verified Test Inventory:** 710 total listed tests across 18 test binaries. **710 passed, 0 failed, 0 ignored** with local MinIO backend healthy.

---

## Post-Slice-5 Implementation Status (ADR-005 Accepted)

> **Implementation Note (Post-Slice-5 Verification State):**
> Following Slices 1 through 4, **Slice 5 (ADR-005: Server Runtime Composition Root and Supervisor Boundary)** was implemented and verified.
>
> * **ADR-005 Status:** Accepted.
> * **Server Runtime Composition Root (`src/runtime.rs`):** Introduced crate-private `ServerRuntime` encapsulating `AppState`, `StorageWiring`, `BlobRefIndex`, `ConsistencyCoordinator`, and `RuntimeMutationAuthority`. Exposes durability flush (`flush_for_shutdown()`), and clean authority release (`release_mutation_authority()`).
> * **Centralized Application Graph Assembly:** Centralized all 7 pure application services (`BlobMutationService`, `ManifestMutationService`, `BlobReadService`, `ManifestReadService`, `CatalogQueryService`, `TagQueryService`, `ReferrersQueryService`) in `assemble_application_services` within `src/runtime.rs`. Test helpers (`AppState::new_test`, `AppState::new_test_with_proxy`) delegate directly to `crate::runtime::build_test_app_state`.
> * **Strict Startup Pipeline, Storage Emptiness Capability & Failure Unwinding:** `build_server_runtime` orchestrates an 8-phase deterministic startup pipeline: storage initialization, mutation authority lease acquisition, fail-closed repository membership readiness preflight using dedicated narrow port `StorageReadinessInspector` (accessed via `StorageWiring::readiness_inspector()`), index opening and health recovery, proxy & proxy-cache storage initialization, consistency coordinator & GC service creation, application service graph assembly, and `AppState` construction. If any error occurs after acquiring mutation authority, the lease is cleanly released before returning `Err(RuntimeBuildError)`.
> * **Storage Factory Placement:** Concrete storage constructors (`proxy_cache_storage_try_from_config`, `storage_wiring_try_from_config`, `storage_from_config`) are centralized in `src/storage/mod.rs`, preserving `src/storage/ports/mod.rs` as pure capability interfaces.
> * **Supervisor Narrowing (`src/supervisor.rs`):** Removed all direct references to `FsStorage`, `S3Storage`, and low-level storage traversal helpers (`is_store_completely_empty`, `has_any_file_or_dir`). Supervisor focuses purely on process lifecycle orchestration, worker task supervision, flush hook execution, and graceful authority release.
> * **Deterministic Unit & Integration Tests:** Added comprehensive unit tests in `src/runtime.rs` and lifecycle contract tests in `tests/supervisor_and_command_tests.rs` verifying filesystem graph assembly, coordinator identity sharing, phase ordering, fail-closed membership rejection, multi-phase failure unwinding, proxy configurations, required live S3 mode behavior, and live S3 MinIO backend graph construction and teardown.
> * **Verified Test Inventory:** 691 total listed tests across 18 test binaries. **691 passed, 0 failed, 0 ignored** with local MinIO backend healthy.
> * **Deferred Slice 6 Work:** Remaining CLI command composition duplication (`src/cli/`) deferred for unification in Slice 6.

---

## Post-Slice-4 Implementation Status (ADR-004 Accepted)

> **Implementation Note (Post-Slice-4 Verification State):**
> Following Slices 1 through 3, **Slice 4 (ADR-004: Complete Application Read Services, Query Services, and Proxy Encapsulation)** was implemented and verified.
>
> * **ADR-004 Status:** Accepted.
> * **Application Read & Query Services (`src/application/`):** Established `BlobReadService`, `ManifestReadService`, `CatalogQueryService`, `TagQueryService`, and `ReferrersQueryService`.
> * **Deterministic Query Policy in Application Layer:** Application query services own deterministic sorting, bounds checking, `last` cursor navigation, and `has_more` calculation. HTTP handlers retain purely transport concerns (query string parsing, RFC 5988 Link header creation, JSON formatting).
> * **Proxy Encapsulation & Boundary Isolation:** Replaced transport-coupled proxy references with transport-neutral `ProxyTarget` (`src/application/proxy.rs`). Removed `.coordinator()` and `.lifecycle()` engine leakage from mutation services. Upstream proxy fetching now delegates publication exclusively through accepted application mutation services.
> * **Completely Thinned Handlers & Clean `AppState`:** Removed all raw reader fields from `AppState`. HTTP handlers perform only authentication, parameter parsing, application service calls, and response mapping.
> * **Zero HTTP Coupling in Application:** Zero Axum/HTTP types (`StatusCode`, `HeaderMap`, `Response`, `IntoResponse`) or `StorageWiring` / `dyn Storage` in `src/application/`.
> * **Deterministic Unit & Integration Tests:** Added `tests/application_read_tests.rs` verifying read services, deterministic query pagination, tenant isolation, and live S3 MinIO backend operations.
> * **Verified Test Inventory:** 620 total listed tests across 18 test binaries (614 Slice 3 baseline + 6 new application read tests). **620 passed, 0 failed, 0 ignored** with local MinIO backend healthy.

---

## Post-Slice-3 Implementation Status (ADR-003 Accepted)

> **Implementation Note (Post-Slice-3 Verification State):**
> Following Slice 1 and Slice 2, **Slice 3 (ADR-003: Storage Capability Port Segregation & Production Migration)** was implemented and verified.
>
> * **ADR-003 Status:** Accepted.
> * **Granular Capability Ports (`src/storage/ports/`):** Defined cohesive capability traits (`BlobCasReader`, `BlobCasWriter`, `RepositoryCatalogReader`, `ManifestReader`, `ManifestStore`, `TagReader`, `TagStore`, `ReferrersReader`, `ReferrersStore`, `LifecycleJournalStore`, `RepositoryLeaseStore`, `ClusterLockStore`, `GcStoragePort`).
> * **Cohesive Composite Service Ports:** Defined composite trait bounds for domain engines (`BlobRefIndexStoragePort`, `BlobUploadCoordinatorStoragePort`, `ManifestLifecycleStoragePort`, `BlobIndexStoragePort`, `GcServiceStoragePort`).
> * **Shared Backend Wiring (`StorageWiring`):** Implemented `StorageWiring::from_backend` ensuring that a single concrete storage instance (`Arc<FsStorage>` or `Arc<S3Storage>`) is shared across all segregated port views without split-brain risk or duplicate state.
> * **Zero Production Consumers on Omnibus `Storage`:** Every production consumer (`BlobMutationService`, `ManifestMutationService`, `GcService`, `RuntimeMutationAuthority`, `BlobRefIndex`, `BlobDeleteService`, `RepositoryMembershipLedger`, `ProxyService`) was migrated to its minimum required capability interface.
> * **Segregated `AppState`:** Removed `storage: Arc<dyn Storage>` from `AppState`, exposing only read-only capability views (`blob_reader`, `membership_reader`, `manifest_reader`, `tag_reader`, `catalog_reader`, `referrers_reader`).
> * **Deterministic Unit & Integration Tests:** Added `tests/ports_wiring_tests.rs` verifying shared backend mutation visibility across port views and isolated capability fakes without omnibus storage.
> * **Verified Test Inventory:** 614 total listed tests across 17 test binaries (611 Slice 2 baseline + 3 new port wiring tests). **614 passed, 0 failed, 0 ignored** with local MinIO backend healthy.

---

## Post-Slice-2 Implementation Status (ADR-002 Accepted)

> **Implementation Note (Post-Slice-2 Verification State):**
> Following Slice 1, **Slice 2 (ADR-002: Application Service Layer & HTTP Transport Thinning)** was implemented and verified.
>
> * **ADR-002 Status:** Accepted.
> * **Cohesive Application Layer (`src/application/`):** Pure application service module created with `BlobMutationService` (`src/application/blob.rs`) and `ManifestMutationService` (`src/application/manifest.rs`).
> * **Zero HTTP Coupling in Application:** `src/application/` contains zero references to HTTP types (`axum`, `http`, `StatusCode`, `HeaderMap`, `Response`).
> * **Thinned Transport Layer:** HTTP handlers in `src/http_api/handlers.rs` and `src/http_api/tags.rs` now exclusively parse transport parameters, check authentication challenges, and delegate directly to application services with centralized error mappers (`blob_mutation_error_to_response`, `manifest_mutation_error_to_response`).
> * **Encapsulated AppState:** Raw coordinator handles (`BlobUploadCoordinator`, `BlobDeleteService`, `ManifestLifecycleService`, `RepositoryMembershipLedger`, `ConsistencyCoordinator`) have been removed from `AppState` and are accessed exclusively through `blob_service` and `manifest_service`.
> * **Remaining Direct Storage Access (Read-Only):** Transport handlers retain direct access only for read-only catalog/tag/referrer listing and content streaming; full storage-port segregation remains explicitly deferred to **Slice 3**.
> * **Deterministic Unit & Integration Tests:** 9 comprehensive application service integration tests added in `tests/application_service_tests.rs` (including 2 typed error source chain verification tests).
> * **Verified Test Inventory:** 611 total listed tests across 16 test binaries (602 Slice 1 baseline + 9 new application tests). **611 passed, 0 failed, 0 ignored** with local MinIO backend healthy.

---

## Post-Slice-1 Implementation Status (ADR-001 Accepted)

> **Implementation Note (Post-Slice-1 Verification State):**
> Following the baseline assessment, **Slice 1 (ADR-001: Encapsulating Concurrency Synchronization via `ConsistencyCoordinator`)** was designed, implemented, and verified.
> 
> * **ADR-001 Status:** Accepted.
> * **Raw Synchronization Removal:** The legacy, unencapsulated `consistency_gate: Arc<tokio::sync::Mutex<()>>` field has been completely removed from `AppState` and all domain service constructors.
> * **Encapsulated Ownership:** `ConsistencyCoordinator` (`src/consistency.rs`) now privately owns and encapsulates the synchronization primitive.
> * **Typed Serialization:** Typed `MutationGuard` and `GcRevalidationGuard` tokens serialize current application mutation paths (`BlobUploadCoordinator`, `ManifestLifecycleService`, `RepositoryMembershipLedger`, `BlobDeleteService`, `Proxy`) and GC reachability revalidation paths.
> * **Application-Level Guarded Deletion:** Authoritative GC candidate deletion (`execute_guarded_gc_deletion`) strictly requires dual independent proofs: `&GcMutationPermit` (proving active deployment mutation authority) and `&GcRevalidationGuard` (proving reachability mutation exclusion).
> * **Storage Trait Scope / Limitation:** The low-level `Storage` trait (`src/storage/mod.rs`) and storage adapters (`FsStorage`, `S3Storage`) still require only mutation authority (`&GcMutationPermit`); full storage trait segregation and port decoupling remain deferred to subsequent architectural slices.
> * **Verified Test Inventory:** The verified test suite expanded to **602 passed, 0 failed, 0 ignored across 15 test binaries**.


---

## 1. Executive Verdict

### Overall Verdict: **Operationally Correct, Structurally Coupled (Incremental Refactoring Required)**

**Baseline Finding (Pre-Slice-1):**
At the baseline assessment commit (`efdae2ee7`), the `registry-rust` codebase passed 583 automated unit, integration, and concurrency tests, including genuine MinIO qualification and live AWS S3 contract runs. Critical OCI/Docker distribution semantics—such as atomic upload finalization, repository-scoped blob membership, manifest lifecycle journaling, cross-repository mounts, and fail-closed S3 garbage collection—were enforced with high rigor. (Post-Slice-1, the verified test suite stands at 602 passed tests across 15 binaries).

However, the baseline architecture exhibited **structural coupling, abstraction leakage, and boundary erosion** accumulated from iterative correctness hardening:

1. **God-Trait Overload in Storage:** The `Storage` trait (`src/storage/mod.rs`) aggregates 29 direct methods and inherits 18 methods across three supertraits (`UploadSessionStorage`, `RepositoryBlobMembershipStorage`, `GcStorage`) for a total of **47 methods** spanning raw CAS blob streaming, manifest parsing, OCI referrers indexing, tag mutation policies, deployment-level cluster locks, lifecycle journals, repository leases, and GC quarantine primitives.
2. **Fat Handlers & Flat `AppState`:** HTTP handlers in `src/http_api/handlers.rs` orchestrate multi-step domain workflows (e.g. proxy cache hit membership linking, digest fallbacks, stream guard telemetry) by reaching directly into **22 flat fields on `AppState`**, bypassing application service encapsulation.
3. **Conventional Synchronization (Pre-Slice-1 Baseline):** Concurrency safety between GC sweeps and client mutations originally relied on convention: callers manually remembered to acquire a shared, unencapsulated `Arc<tokio::sync::Mutex<()>>` (`consistency_gate`). *(Resolved in Slice 1 via `ConsistencyCoordinator`).*
4. **Significant In-Source Test Footprint:** Across `src/`, inline `#[cfg(test)] mod tests` account for **15,548 out of 43,537 total lines (35.7%)**, with critical hotspot files exceeding 50% to 79% test code (e.g. `src/http_api/handlers.rs` is 68.2% tests with 2,672 test lines; `src/storage/s3.rs` has 3,505 test lines; `src/manifest_publication.rs` has 943 test lines for a 28-line re-export shim).
5. **Fragmented Error Taxonomies:** Domain errors are repeatedly wrapped into `StorageError::Internal(String)` and unpacked via string matching across HTTP, CLI, and supervisor boundaries.

**Conclusion:** A complete rewrite is **unnecessary, risky, and strongly discouraged**. The foundational domain invariants (e.g. 6-step pinned blob publication, WAL lifecycle journals, S3 ETag conditional mutation) are sound. An **incremental refactoring roadmap** (guided by ADR-001) will encapsulate synchronization boundaries, extract application services, decouple storage ports, and segregate test fixtures without altering OCI-visible behavior.

---

## 2. Current Architecture Map

> **HEAD (`9405991`):** delivery → application services → domain engines → `StorageWiring` ports → `FsStorage` / `S3Storage` (pinned `FsMetadataReader` + ObjectStore domains). See [`current-state.md`](current-state.md). The diagram immediately below is the **baseline** map from 2026-08-26 and is retained as historical evidence of coupling that slices 1–11 and later storage cutovers addressed.

### 2.1 System Component Overview (baseline 2026-08-26)

```
                               ┌───────────────────────────┐
                               │        CLI / Main         │
                               │   (`src/main.rs`, `cli`)  │
                               └─────────────┬─────────────┘
                                             │
                                             ▼
                               ┌───────────────────────────┐
                               │     Server Supervisor     │
                               │   (`src/supervisor.rs`)   │
                               └─────────────┬─────────────┘
                                             │
                        ┌────────────────────┴────────────────────┐
                        ▼                                         ▼
         ┌─────────────────────────────┐           ┌─────────────────────────────┐
         │     HTTP Transport Layer    │           │    Background Orchestrator  │
         │  (`src/http_api/mod.rs`,    │           │  (`gc_service`, `task_sup`, │
         │   `handlers`, `routing`,    │           │   `token_rate_limit`)       │
         │   `catalog`, `tags`, etc.)  │           └──────────────┬──────────────┘
         └──────────────┬──────────────┘                          │
                        │                                         │
                        └────────────────────┬────────────────────┘
                                             │
                                             ▼
                        ┌────────────────────────────────────────┐
                        │          Flat Composition Root         │
                        │        (`AppState`, 22 fields)         │
                        └────────────────────┬───────────────────┘
                                             │
        ┌────────────────────────────────────┼────────────────────────────────────┐
        ▼                                    ▼                                    ▼
┌──────────────────────────┐   ┌──────────────────────────┐   ┌──────────────────────────┐
│ UploadCoordinator        │   │ ManifestLifecycleService │   │ GcService / BlobGc       │
│ - UploadState token HMAC │   │ - Write-ahead journal    │   │ - 3-phase sweep          │
│ - Chunk stream guards    │   │ - Tag mutation policies  │   │ - Ref validation gate    │
│ - RefIndex pin heartbeats│   │ - Referrers DAG indexing │   │ - Storage deletion permit│
└────────────┬─────────────┘   └─────────────┬────────────┘   └────────────┬─────────────┘
             │                               │                             │
             └───────────────────────────────┼─────────────────────────────┘
                                             │
                                             ▼
                        ┌────────────────────────────────────────┐
                        │ Storage Port (God Trait: 47 methods)   │
                        │       (`src/storage/mod.rs`)           │
                        └────────────────────┬───────────────────┘
                                             │
                        ┌────────────────────┴────────────────────┐
                        ▼                                         ▼
         ┌─────────────────────────────┐           ┌─────────────────────────────┐
         │      FsStorage Adapter      │           │      S3Storage Adapter      │
         │  - POSIX directory layout   │           │  - S3Driver (AWS SDK v1)    │
         │  - Atomic file renames      │           │  - MockS3Driver in-memory   │
         │  - Quarantine metadata (.ts)│           │  - Conditional ETag deletes │
         │  - File locks (`.lock`)     │           │  - JSON lease objects       │
         └─────────────────────────────┘           └─────────────────────────────┘
```

### 2.2 Actual Dependency Direction Matrix

**HEAD (`9405991`):** HTTP depends on application services, not `dyn Storage`. Application modules do not import Axum. Supervisor calls `build_server_runtime` and does not construct `FsStorage`/`S3Storage`. Remaining leaks: `AppState` still exposes config/proxy/GC/semaphores to transport; adapters still implement omnibus `Storage`; `Config` is still passed wholesale into assembly.

**Baseline 2026-08-26 (historical):**

| Consumer Layer | Permitted Dependencies | Actual Dependencies (Violations Flagged) | Status |
|---|---|---|---|
| **Delivery / HTTP (`http_api`)** | Application Services, Domain Types | `AppState` (all 22 fields), `Storage` direct methods, `BlobRefIndex`, `RepositoryMembershipLedger`, `Proxy` | ⚠️ Leaky |
| **Application Services (`upload_coordinator`, `manifest_lifecycle`, `gc_service`)** | Domain Types, Storage Ports | `Storage` (god trait), `BlobRefIndex`, `RuntimeMutationAuthority`, `Config` (raw struct) | ⚠️ Mixed |
| **Domain Logic (`registry`, `manifest_refs`, `blob_gc/policy`)** | Pure domain models | Pure (no external dependencies) | ✅ Clean |
| **Storage Adapters (`fs`, `s3`)** | Storage Ports, Infrastructure SDKs | `StorageError`, `RepoBlobMembershipRecord`, `LifecycleJournalRecord`, `Config` | ⚠️ Broad |
| **Supervisor (`supervisor`)** | Composition Root, App Services | Direct Axum route mounting, S3 capability checks, ACME TLS generation | ⚠️ Fat |

---

## 3. Critical Use-Case Traces

> Baseline traces (2026-08-26). At HEAD, handlers delegate mutations/reads to application services; proxy publication goes through those services; the FS upload reaper and GC quarantine mutations use contained authorities. See [`current-state.md`](current-state.md).

### Trace 1: Resumable Blob Upload and Finalization
```
Client                      HTTP Handler              BlobUploadCoordinator       BlobRefIndex        Storage Backend
  │                              │                              │                      │                     │
  │── POST /v2/<r>/blobs/uploads ┼─────────────────────────────>│                      │                     │
  │                              │                              │── create_session() ──┼────────────────────>│
  │   202 Accepted + Location    │                              │                      │                     │
  │   (State Token Signed HMAC)  │<─────────────────────────────│                      │                     │
  │<─────────────────────────────│                              │                      │                     │
  │                              │                              │                      │                     │
  │── PATCH (Chunk 1..N) ────────┼─────────────────────────────>│                      │                     │
  │                              │                              │── append_chunk() ────┼────────────────────>│
  │   202 Accepted + State Token │<─────────────────────────────│                      │                     │
  │<─────────────────────────────│                              │                      │                     │
  │                              │                              │                      │                     │
  │── PUT ?digest=sha256:... ────┼─────────────────────────────>│                      │                     │
  │                              │                              │── acquire_pin() ────>│ (Heartbeat spawned) │
  │                              │                              │── commit_cas_blob() ─┼────────────────────>│
  │                              │                              │── acquire_mutation()>│                     │
  │                              │                              │── link_membership() ─┼────────────────────>│
  │                              │                              │── record_membership >│                     │
  │                              │                              │── flush() & ready ──>│                     │
  │                              │                              │── drop(MutationGuard)│                     │
  │                              │                              │── release_pin() ────>│ (Heartbeat stopped) │
  │   201 Created                │<─────────────────────────────│                      │                     │
  │<─────────────────────────────│                              │                      │                     │
```
- **Invariants:** Pin is held continuously from CAS write through membership link; membership is established before pin release.
- **Hotspot:** `UploadCoordinator` directly invokes `BlobRefIndex` methods while holding the mutation guard.

### Trace 2: Proxy Blob Fetch and Caching
- **Initiating Boundary:** `handlers::blob_by_digest` / `handlers::blob_by_digest_proxy_only`
- **Application Flow:**
  1. Handler checks `ctx.cache.open_blob(&digest)`. If cache hit, handler calls `state.membership_ledger.link(...)` inline.
  2. If cache miss, handler invokes `ctx.proxy.fetch_blob_into_storage(&decision, &digest, &state.upload_coordinator)`.
  3. `Proxy` streams from upstream, validates SHA256, and delegates to `coordinator.publish_proxy_blob`.
  4. `publish_proxy_blob` executes the 6-step state machine: Acquire `PinLeaseGuard` -> Commit to CAS -> Acquire `MutationGuard` (historically raw `consistency_gate`) -> Link membership in storage & `BlobRefIndex` -> Flush index -> Release pin.
- **Architectural Smell:** The cache-hit path performs membership linking directly in `handlers.rs:468-480` instead of routing through an application service.

### Trace 3: S3 Garbage Collection Delete Sweep
- **Initiating Boundary:** `GcService::delete` / `GcService::scheduled_cleanup_once`
- **Application Flow:**
  1. `GcService` checks `storage.check_bucket_versioning_for_gc()`. If versioning is `Enabled`, `Suspended`, or `UnknownOrDenied`, fails closed with `StrategyUnsupported`.
  2. `GcService` inspects its `Arc<Mutex<Option<RuntimeMutationAuthority>>>` and mints a short-lived `GcMutationPermit`.
  3. Traverses repository tags and manifests to build live root digest set.
  4. Paginates CAS blobs via `storage.list_cas_blobs_page`.
  5. Evaluates candidates against policy: `manifest_reachability`, `min_age_secs`, and `BlobRefIndex::is_blob_pinned`.
  6. For each unreferenced candidate, acquires `GcRevalidationGuard` (historically raw `consistency_gate.lock().await`) and revalidates reachability against journals and tags.
  7. If still unreferenced, calls `storage.delete_blob_conditional(&permit, &digest, Some(&version))` using S3 ETag conditional delete.
- **Architectural Smell:** Permitting is dynamically tied to a mutex holding an Option of authority, requiring defensive lock acquisition inside loop iterations.

---

## 4. Dependency and Ownership Findings

```
[Delivery Layer: Axum Handlers / Routing]
       │
       ▼ (Direct field access)
[AppState: 17 Unencapsulated Shared Fields]
   ├── config: Arc<Config>
   ├── storage: Arc<dyn Storage>
   ├── ref_index: Option<Arc<BlobRefIndex>>
   ├── gc_service: Option<Arc<GcService>>
   ├── proxy / proxy_cache / proxy_upstreams
   ├── semaphores (buffered_body, request, upload_request)
   ├── telemetry counters (active_upload_requests, etc.)
   ├── consistency_gate: Arc<Mutex<()>> (Pre-Slice-1 baseline; replaced by ConsistencyCoordinator in Slice 1)
   ├── membership_ledger: Arc<RepositoryMembershipLedger>
   ├── upload_coordinator: Arc<BlobUploadCoordinator>
   ├── delete_service: Arc<BlobDeleteService>
   └── manifest_lifecycle: Arc<ManifestLifecycleService>
```

### Key Violations Identified
1. **No Application Service Gateway:** Handlers in `src/http_api/handlers.rs` interact with `Storage`, `BlobRefIndex`, `RepositoryMembershipLedger`, `ManifestLifecycleService`, and `BlobUploadCoordinator` ad-hoc across 1,245 lines of dispatch code.
2. **Leaky Storage Trait (`src/storage/mod.rs`):** `Storage` combines CAS blob primitives with high-level application orchestration (deployment lock JSON document serialization, journal payloads, OCI referrers JSON manipulation).
3. **Ghost Modules (`src/manifest_publication.rs`):** 972-line file retained purely as a backwards-compatibility re-export for `src/manifest_lifecycle.rs`, carrying 944 lines of tests that duplicate integration assertions.
4. **Configuration Struct Proliferation:** Raw `Config` struct (3,632 lines) is passed into domain services, coupling domain components to TOML configuration representations.

---

## 5. State, Concurrency, and Recovery Model

| Synchronization Mechanism | Invariant Protected | Owner | Structural Enforcement | Crash Behavior |
|---|---|---|---|---|
| **`ConsistencyCoordinator`** (pre-Slice-1 raw `consistency_gate`) | Mutual exclusion between GC candidate revalidation and client mutations | In-process coordinator (`src/consistency.rs`) | ✅ Enforced via `MutationGuard` and `GcRevalidationGuard` | Memory-only; released on process exit |
| **`RuntimeMutationAuthority`** | Single-writer deployment guarantee across instances | `supervisor` / `GcService` | ✅ Enforced via `GcMutationPermit` | Lock expires after lease TTL on S3 (`locks/deployment_writer.json`) or released via FS lockfile |
| **`BlobRefIndex` Pins** (`pins` tree) | In-flight upload and proxy caching protection against GC | `BlobUploadCoordinator` (`PinLeaseGuard`) | ✅ Scoped heartbeat task with TTL | Pins expire automatically after TTL in sled index |
| **`LifecycleJournalRecord`** | Atomic two-phase manifest publication and tag mutation | `ManifestLifecycleService` | ✅ WAL file / S3 journal record | Replayed or rolled back during startup recovery (`recover_all_journals`) |
| **`S3BucketVersioningState`** | Physical deletion safety on S3 | `S3Storage` / `GcService` | ✅ Preflight capability check | Read dynamically from S3 bucket configuration |

---

## 6. Storage Abstraction Assessment

### 6.1 The God-Trait Problem in `Storage`
The `Storage` trait currently acts as an omnibus interface:

```rust
// Proposed Segregation of Storage Capabilities
pub trait BlobCasStorage: Send + Sync {
    async fn head_blob(&self, digest: &Digest) -> Result<BlobMeta, StorageError>;
    async fn open_blob(&self, digest: &Digest) -> Result<(BlobMeta, Pin<Box<dyn AsyncRead + Send>>), StorageError>;
}

pub trait ManifestStorage: Send + Sync {
    async fn get_manifest(&self, repo: &str, digest: &Digest) -> Result<(ManifestMeta, Bytes), StorageError>;
    async fn put_manifest(&self, repo: &str, digest: &Digest, bytes: Bytes) -> Result<ManifestMeta, StorageError>;
    async fn delete_manifest(&self, repo: &str, digest: &Digest) -> Result<(), StorageError>;
}

pub trait TagStorage: Send + Sync {
    async fn resolve_tag(&self, repo: &str, tag: &str) -> Result<Digest, StorageError>;
    async fn mutate_tag(&self, repo: &str, tag: &str, digest: &Digest, policy: TagMutationPolicy) -> Result<TagMutation, StorageError>;
    async fn delete_tag_conditional(&self, repo: &str, tag: &str, expected_version: Option<&str>) -> Result<ConditionalDeleteResult, StorageError>;
}

pub trait ClusterLockStorage: Send + Sync {
    async fn acquire_deployment_writer_lock(&self, doc: &DeploymentWriterLockDoc) -> Result<(bool, Option<String>), StorageError>;
    async fn release_deployment_writer_lock(&self, doc: &DeploymentWriterLockDoc, expected_etag: Option<&str>) -> Result<bool, StorageError>;
}
```

### 6.2 Mock Fidelity vs. Production Adapters
- `MockS3Driver` (`src/storage/s3.rs:4100-4350`) provides in-memory simulation of S3 conditional headers (`If-Match`, `If-None-Match`), versioning states, and continuation tokens.
- **Fidelity Assessment:** High. Mock correctly replicates AWS S3 412 Precondition Failed, 404 Not Found, and cyclic pagination behavior.
- **Limitation:** In-memory mock structures live in `src/storage/s3.rs` instead of a dedicated test-support fixture module.

---

## 7. Error Architecture Assessment

### Current Error Taxonomy
- **`StorageError` (`src/storage/mod.rs:26-56`):** Contains 10 variants (`NotFound`, `DigestMismatch`, `Unsupported`, `TooLarge`, `InsufficientStorage`, `TagAlreadyExists`, `ExclusiveWriterLocked`, `InvalidRepoName`, `MigrationRequired`, `Internal(String)`).
- **`ManifestLifecycleError` (`src/manifest_lifecycle.rs:100-150`):** Granular domain errors (`MissingBlob`, `InvalidManifest`, `TagAlreadyExists`, etc.).
- **`CoordinatorError` (`src/upload_coordinator.rs:75-110`):** Upload session token errors, rate errors, and storage wraps.
- **`GcServiceError` (`src/gc_service.rs:60-95`):** Strategy errors, budget exhaustion, lock timeouts.

### Identified Deficiencies
1. **Stringly Typed Internal Errors:** `StorageError::Internal(String)` is overused to tunnel AWS SDK errors, sled I/O errors, and JSON serialization failures.
2. **Duplicated OCI Error Mapping:** HTTP handlers manually map `StorageError`, `ManifestLifecycleError`, and `CoordinatorError` into Axum `Response` using `http_api/errors.rs` helpers across multiple branches.

---

## 8. Test Architecture Assessment

### 8.1 Test Inventory Breakdown (602 Tests Total)
```
Total Test Suite: 602 Tests
├── Inline Unit Tests (340 tests in src/)
│   ├── src/storage/s3.rs tests (72 tests)
│   ├── src/storage/fs.rs tests (48 tests)
│   ├── src/http_api/handlers.rs tests (64 tests)
│   ├── src/config.rs tests (35 tests)
│   ├── src/upload_coordinator.rs tests (28 tests)
│   ├── src/consistency.rs tests (9 tests)
│   └── other unit tests (84 tests)
└── Integration Tests (262 tests in tests/*.rs)
    ├── tests/manifest_lifecycle_tests.rs (56 tests)
    ├── tests/repository_membership_tests.rs (49 tests)
    ├── tests/s3_live_integration.rs (29 tests)
    ├── tests/production_upload_cutover_tests.rs (25 tests)
    ├── tests/supervisor_and_command_tests.rs (22 tests)
    ├── tests/gc_adversarial_coordination_tests.rs (22 tests)
    ├── tests/canonical_repo_grammar_tests.rs (18 tests)
    ├── tests/authorization_regression_and_storage_golden_tests.rs (11 tests)
    ├── tests/cli_config_tests.rs (11 tests)
    ├── tests/oci_1_1_tests.rs (7 tests)
    ├── tests/slow_connection_tests.rs (5 tests)
    ├── tests/oci_conformance_regression_tests.rs (4 tests)
    └── tests/online_gc_integration.rs (3 tests)
```

### 8.2 Strengths & Weaknesses
- **Strengths:** Outstanding adversarial concurrency coverage (barriers, crash boundaries, race injections) and rigorous live S3 qualification.
- **Weaknesses:** Excessive test code co-located in production files inflates module complexity and creates noise during architectural reviews.

---

## 9. Prioritized Technical Debt Register

| ID | Severity | Architectural Smell | Root Cause | Consequence | Affected Use Cases | Target Boundary | Recommended Correction | Status |
|---|---|---|---|---|---|---|---|:---:|
| **D-01** | **P1** | `Storage` God-Trait Overload | Rapid feature addition to single trait | Leaky abstraction; monolithic mock requirements | All storage access | `src/storage/ports/` | Segregate `Storage` into capability ports | **Mostly resolved (ADR-003).** `StorageWiring::from_backend` requires port traits, not `Storage`. Residual: adapters still `impl Storage` (tests / same concrete type). |
| **D-02** | **P1** | Unencapsulated `AppState` in Handlers | Direct handler access to 17 internal fields | Business logic leakage into transport layer | HTTP dispatch, Proxy caching | `src/application/` (not `src/services/`) | Introduce application services; handlers parse/auth/map only | **Mostly resolved (ADR-002/004).** Mutations/reads go through seven services. Residual: `AppState` still holds proxy, GC, semaphores, config. |
| **D-03** | **P2** | Conventional `consistency_gate` Synchronization | Unwrapped `Arc<Mutex<()>>` | Risk of future mutators bypassing gate | Manifest publish, GC sweep, Upload finalize | `src/consistency.rs` | Encapsulate gate inside transactional coordinator guards | **Resolved (ADR-001)** |
| **D-04** | **P2** | In-Source Test Footprint Bloat | Historical co-location of extensive mocks | 55% of `src/` is test code; hinders maintainability | Development & Review | `tests/` or `src/fixtures/` | Extract mock drivers and unit tests into dedicated submodules or integration tests | **Resolved (Slices 8–10)** |
| **D-05** | **P3** | Ghost Re-export Module | Partial refactoring of `manifest_publication` | Redundant 972-line file with duplicate tests | Build / Navigation | `src/manifest_lifecycle.rs` | Deprecate `manifest_publication.rs` and migrate residual test cases to `manifest_lifecycle_tests.rs` | **Resolved (ADR-007)** |
| **D-06** | **P3** | Stringly-Typed Internal Error Tunneling | Generic `StorageError::Internal(String)` | Loss of error root cause and retry semantics | Error reporting, CLI exit codes | `src/storage/` | Introduce structured `StorageErrorKind` taxonomy with typed causes and eliminate string control flow | **Resolved (ADR-009 / Slice 11)** |

> **D-06 Architectural & Compatibility Assessment Note (Resolved by ADR-009 / Slice 11):**
> * **Public API Reachability:** StorageError is publicly reachable as registry_rust::storage::StorageError because src/lib.rs declares pub mod storage and src/storage/mod.rs declares pub enum StorageError.
> * **Source-Breaking Representation Change:** Transforming `StorageError::Internal(String)` into `StorageError::Internal { kind: StorageErrorKind, message: String }` breaks downstream code that constructs `StorageError::Internal(...)` or matches tuple patterns `StorageError::Internal(...)`.
> * **Downstream Migration:** Downstream consumers must migrate to struct patterns (`StorageError::Internal { kind, message }` / `{ ref message, .. }`) and helper constructors (`StorageError::io`, `backend`, `corrupt_data`, etc., or `StorageError::internal(kind, msg)`).
> * **Display vs. Source Compatibility:** Preserving the `Display` format `internal error: {message}` maintains log and metric output stability but does **not** preserve Rust source compatibility.
> * **Compatibility Precedent & SemVer Release Boundary:** Prior to this boundary, the crate package was version `0.8.18`. While no standalone repository-wide compatibility policy was found, ADR-007 provides an existing compatibility precedent where public Rust paths and type shapes are treated as compatibility surfaces. Here, Slice 11 implemented the structured variant at commit `49d405483b6ea729fc6a4e9a0a99177ec37c2bfd`, establishing the required breaking `0.9.0` release boundary in `Cargo.toml`. Under standard Cargo pre-1.0 (`0.y.z`) Semantic Versioning conventions, this breaking change requires releasing under a `0.9.0` minor version boundary rather than a `0.8.19` patch bump.
> * **Direct Serialization Status:** `StorageError` implements neither `Serialize` nor `Deserialize` and possesses no direct serialized wire representation. `StorageErrorKind` derives `Serialize`/`Deserialize` for future structured telemetry and logging, but this does not serialize `StorageError` itself. Transport DTOs (`OciErrorResponse`) and persisted storage entities (`FinalizedReceipt`, `RepoBlobMembershipRecord`, etc.) remain strictly separate domain records.
> * **Status:** Technical Debt item D-06 is **Resolved** by ADR-009 in Slice 11 (commit `49d405483b6ea729fc6a4e9a0a99177ec37c2bfd`). The crate is now versioned at `0.9.0` as the required release boundary; the release has not been tagged, published, or distributed.



---

## 10. Target Architecture

### 10.1 Target Layering & Boundaries

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                 DELIVERY LAYER                                         │
│   - CLI (`src/cli/`)                                                                   │
│   - OCI HTTP Transport (`src/http_api/` - thin controllers, protocol serialization)    │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                            APPLICATION SERVICE FACADE                                  │
│   - `BlobService`: upload streaming, state verification, publication coordination       │
│   - `ManifestService`: lifecycle journaling, tag mutations, referrers DAG              │
│   - `GarbageCollectionService`: planning, sweep execution, permit gating               │
│   - `ProxyService`: upstream routing, caching, background eviction                     │
└─────────────────────┬────────────────────────────────────────────┬─────────────────────┘
                      │                                            │
                      ▼                                            ▼
┌──────────────────────────────────────────┐  ┌──────────────────────────────────────────┐
│              DOMAIN LAYER                │  │            COORDINATION LAYER            │
│  - `CanonicalRepoName`, `Digest`         │  │  - `ConsistencyGate` (encapsulated guard)│
│  - `ManifestRefs`, `ReferrerDescriptor`  │  │  - `MutationAuthority` (lease manager)   │
│  - GC Policies & Reachability Traversers │  │  - `RefIndex` (metadata & pin manager)   │
└─────────────────────┬────────────────────┘  └────────────────────┬─────────────────────┘
                      │                                            │
                      └─────────────────────┬──────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                STORAGE PORTS (TRAITS)                                  │
│   - `BlobCasStore`, `ManifestStore`, `TagStore`, `ClusterLockStore`, `GcStoragePort`   │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                      ┌─────────────────────┴─────────────────────┐
                      ▼                                           ▼
┌──────────────────────────────────────────┐  ┌──────────────────────────────────────────┐
│          FsStorage Adapter               │  │          S3Storage Adapter               │
│  - Local POSIX filesystem mechanics      │  │  - AWS SDK S3 Driver                     │
│  - Atomic rename & quarantine directory  │  │  - Direct conditional ETag deletes       │
└──────────────────────────────────────────┘  └──────────────────────────────────────────┘
```

### 10.2 What Remains Unchanged
- **Zero OCI Wire Changes:** All HTTP routes, response headers, error JSON bodies, and status codes remain 100% identical.
- **Zero Storage Layout Changes:** Filesystem directory hierarchies (`blobs/sha256/`, `repos/`, `quarantine/`) and S3 key schemes (`blobs/sha256/`, `repos/`, `membership/by-repo/`) remain strictly preserved.
- **Zero State Machine Alterations:** The 6-step pinned upload invariant, WAL lifecycle journals, and S3 unversioned capability gating remain intact.

---

## 11. Incremental Refactoring Roadmap

```
  [Slice 1: Storage Trait Segregation] ──> [Slice 2: Application Service Facades]
                    │                                         │
                    ▼                                         ▼
  [Slice 3: Gate Encapsulation]        ──> [Slice 4: Test & Mock Decoupling]
                    │                                         │
                    ▼                                         ▼
  [Slice 5: Ghost Module Cleanup]      ──> [Slice 6: Typed Error Architecture]
```

### Slice 1: Storage Trait Segregation (High Leverage)
- **Objective:** Split the monolithic `Storage` trait into segregated, cohesive capability traits (`BlobCasStore`, `ManifestStore`, `TagStore`, `ClusterLockStore`, `GcStoragePort`).
- **Files Involved:** `src/storage/mod.rs`, `src/storage/fs.rs`, `src/storage/s3.rs`.
- **Target Invariant:** Storage consumers only depend on the narrow capabilities they require.
- **Rollback Criteria:** Blanket `impl<T: ...> Storage for T` composite trait guarantees zero breakage for existing callers during transition.

### Slice 2: Extract Application Services from Handlers
- **Objective:** Remove business logic from `src/http_api/handlers.rs` into `src/services/` (`BlobService`, `ManifestService`, `ProxyService`).
- **Files Involved:** `src/http_api/handlers.rs`, `src/app_state.rs`, `src/services/`.
- **Target Invariant:** HTTP handlers only parse requests, extract auth contexts, invoke one service method, and format OCI responses.

### Slice 3: Encapsulate Concurrency & Transactional Gates
- **Objective:** Replace raw `Arc<Mutex<()>>` with a typed `ConsistencyGate` offering scoped transaction guards (`ConsistencyGate::run_with_mutation_guard(...)`).
- **Files Involved:** `src/upload_coordinator.rs`, `src/manifest_lifecycle.rs`, `src/gc_service.rs`, `src/app_state.rs`.
- **Target Invariant:** Unsynchronized mutation becomes structurally unrepresentable at compile time.

### Slice 4: Test & Mock Segregation
- **Objective:** Move large inline `mod tests` (>3,000 lines) from `src/storage/s3.rs` and `src/http_api/handlers.rs` into dedicated test modules under `tests/` or `src/test_fixtures/`.
- **Files Involved:** `src/storage/s3.rs`, `src/http_api/handlers.rs`, `tests/`.
- **Target Invariant:** Production files focus strictly on runtime logic; line count reduced by >50%.

### Slice 5: Deprecate Ghost Compatibility Shims
- **Objective:** Remove `src/manifest_publication.rs` and consolidate remaining unique unit tests into `tests/manifest_lifecycle_tests.rs`.
- **Files Involved:** `src/manifest_publication.rs`, `src/lib.rs`, `tests/manifest_lifecycle_tests.rs`.
- **Target Invariant:** One clear source of truth for manifest lifecycle management.

### Slice 6: Typed Storage Error Hierarchy
- **Objective:** Replace `StorageError::Internal(String)` with structured error enums (`StorageIoError`, `DriverError`, `SerializationError`) while preserving OCI HTTP error mapping.
- **Files Involved:** `src/storage/mod.rs`, `src/http_api/errors.rs`.
- **Target Invariant:** Preserve error context and enable granular retry classification.

---

## 12. Non-Goals & Intentionally Retained Tradeoffs

1. **Retaining In-Memory Sled Ref-Index:** Embedded `sled` remains the reference index for filesystem and single-instance deployments. Replacing it with an external relational database is out of scope.
2. **Retaining Direct S3 Conditional Deletes:** Physical S3 deletion will continue to rely on object-level ETag preconditions (`If-Match`) rather than complex out-of-band inventory processing.
3. **Preserving Synchronous GC Lock Windows:** The in-process synchronization coordinator (`ConsistencyCoordinator`) mutex is retained for reachability revalidation to avoid complex multi-version distributed concurrency protocols.

---

## 13. Open Questions for Maintainer Input

1. **Storage Capability Segregation:** Should `Storage` remain available as a blanket composite trait for convenience, or should consumers be forced to take only specific sub-traits (e.g. `Arc<dyn BlobCasStore>`)?
2. **Multi-Instance S3 Coordination:** For multi-instance clustered deployments, should `BlobRefIndex` synchronization evolve toward distributed leasing or remain scoped to single-writer leader instances?
3. **Test File Organization:** Is moving unit test suites from inline `mod tests` into dedicated sibling files under `tests/unit/` preferred for long-term code navigation?

---

*End of Assessment Document.*
