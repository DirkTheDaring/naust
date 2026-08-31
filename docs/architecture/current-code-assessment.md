# Architecture Assessment: Current-State Analysis & Strategic Technical Roadmap

**Repository:** `registry-rust`  
**Baseline Commit:** `efdae2ee77ee622c81b6b157da593659769301ce`  
**Assessment Date:** 2026-08-26  
**Auditor:** Senior Software Architect (OCI Distribution & Storage Systems)

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

### 2.1 System Component Overview

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

| Consumer Layer | Permitted Dependencies | Actual Dependencies (Violations Flagged) | Status |
|---|---|---|---|
| **Delivery / HTTP (`http_api`)** | Application Services, Domain Types | `AppState` (all 22 fields), `Storage` direct methods, `BlobRefIndex`, `RepositoryMembershipLedger`, `Proxy` | ⚠️ Leaky |
| **Application Services (`upload_coordinator`, `manifest_lifecycle`, `gc_service`)** | Domain Types, Storage Ports | `Storage` (god trait), `BlobRefIndex`, `RuntimeMutationAuthority`, `Config` (raw struct) | ⚠️ Mixed |
| **Domain Logic (`registry`, `manifest_refs`, `blob_gc/policy`)** | Pure domain models | Pure (no external dependencies) | ✅ Clean |
| **Storage Adapters (`fs`, `s3`)** | Storage Ports, Infrastructure SDKs | `StorageError`, `RepoBlobMembershipRecord`, `LifecycleJournalRecord`, `Config` | ⚠️ Broad |
| **Supervisor (`supervisor`)** | Composition Root, App Services | Direct Axum route mounting, S3 capability checks, ACME TLS generation | ⚠️ Fat |

---

## 3. Critical Use-Case Traces

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
| **D-01** | **P1** | `Storage` God-Trait Overload | Rapid feature addition to single trait | Leaky abstraction; monolithic mock requirements | All storage access | `src/storage/ports/` | Segregate `Storage` into `BlobCasStorage`, `ManifestStorage`, `TagStorage`, `LockStorage` | Planned (Slice 3) |
| **D-02** | **P1** | Unencapsulated `AppState` in Handlers | Direct handler access to 17 internal fields | Business logic leakage into transport layer | HTTP dispatch, Proxy caching | `src/services/` | Introduce `RegistryApplicationService` facade; pass focused contexts to handlers | Planned (Slice 2) |
| **D-03** | **P2** | Conventional `consistency_gate` Synchronization | Unwrapped `Arc<Mutex<()>>` | Risk of future mutators bypassing gate | Manifest publish, GC sweep, Upload finalize | `src/consistency.rs` | Encapsulate gate inside transactional coordinator guards | **Resolved (ADR-001)** |
| **D-04** | **P2** | In-Source Test Footprint Bloat | Historical co-location of extensive mocks | 55% of `src/` is test code; hinders maintainability | Development & Review | `tests/` or `src/fixtures/` | Extract mock drivers and unit tests into dedicated submodules or integration tests | Planned |
| **D-05** | **P3** | Ghost Re-export Module | Partial refactoring of `manifest_publication` | Redundant 972-line file with duplicate tests | Build / Navigation | `src/manifest_lifecycle.rs` | Deprecate `manifest_publication.rs` and migrate residual test cases to `manifest_lifecycle_tests.rs` | Planned |
| **D-06** | **P3** | Stringly-Typed Internal Error Tunneling | Generic `StorageError::Internal(String)` | Loss of error root cause and retry semantics | Error reporting, CLI exit codes | `src/errors.rs` | Introduce structured `StorageErrorKind` with typed underlying causes | Planned |

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
