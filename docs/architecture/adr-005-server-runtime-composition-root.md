# ADR-005: Server Runtime Composition Root and Supervisor Boundary

* **Status:** Accepted
* **Date:** 2026-08-31
* **Authors:** Senior Software Architect (OCI Distribution & Storage Systems)
* **Scope:** Server Runtime Composition Root, Supervisor Boundary Narrowing, Centralized Graph Assembly, Startup Preflight & Failure Unwinding
* **Refines:** `docs/architecture/adr-001-first-refactoring-boundary.md`, `docs/architecture/adr-002-application-service-boundary.md`, `docs/architecture/adr-003-storage-capability-ports.md`, `docs/architecture/adr-004-application-read-services.md`, `docs/architecture/current-code-assessment.md`

---

## 1. Context & Problem Statement

Prior to Slice 5, the codebase had established clean consistency coordination (ADR-001), application mutation services (ADR-002), segregated storage capability ports (ADR-003), and application read/query services (ADR-004). However, server initialization and dependency assembly suffered from significant structural issues:

1. **Supervisor Conflation:** `src/supervisor.rs` mixed supervisor task lifecycle management (signal handling, task supervision, graceful worker draining) with concrete backend instantiation (`FsStorage::new`, `S3Storage::new`), storage emptiness probes, multi-step readiness preflights, index opening, and multi-service graph wiring.
2. **Duplicated Service Graph Construction:** Application services (`BlobMutationService`, `ManifestMutationService`, `BlobReadService`, `ManifestReadService`, `CatalogQueryService`, `TagQueryService`, `ReferrersQueryService`) and proxy targets were constructed separately in `src/supervisor.rs` and in test helpers (`AppState::new_test`, `AppState::new_test_with_proxy`, and test modules).
3. **Scattered Storage Selection:** Concrete storage construction helpers and proxy-cache storage instantiation logic were directly embedded in `src/supervisor.rs` rather than being centralized in the storage module.
4. **Fragile Partial-Build Cleanup:** When startup failed midway (e.g. during repository membership validation, index initialization, proxy cache creation, or phase hook execution), mutation authority lease release and resource unwinding required bespoke error handling inside the supervisor.

---

## 2. Decision: Authoritative Server Runtime Composition Root (`src/runtime.rs`)

We introduce `src/runtime.rs` as the single authoritative composition boundary for the server runtime.

### 2.1. `ServerRuntime` Encapsulation

```rust
pub struct ServerRuntime {
    app_state: Arc<AppState>,
    storage_wiring: StorageWiring,
    ref_index: Option<Arc<BlobRefIndex>>,
    consistency_coordinator: Arc<ConsistencyCoordinator>,
    mutation_authority: RuntimeMutationAuthority,
}
```

`ServerRuntime` encapsulates all long-lived server components, exposing read-only accessors and controlled lifecycle operations:
* `app_state(&self) -> &Arc<AppState>`: Exposes the configured application state to HTTP routers.
* `storage_wiring(&self) -> &StorageWiring`: Exposes the storage capability port wiring.
* `ref_index(&self) -> Option<&Arc<BlobRefIndex>>`: Exposes the optional blob reference index.
* `consistency_coordinator(&self) -> &Arc<ConsistencyCoordinator>`: Exposes the shared consistency coordinator.
* `flush_for_shutdown(&self) -> Result<(), String>`: Flushes dirty ref-index entries and membership state prior to teardown.
* `release_mutation_authority(self) -> Result<(), String>`: Consumes the runtime to cleanly release the deployment mutation authority lease.

### 2.2. Strict Ordered Startup Pipeline & Readiness Preflight

`build_server_runtime` orchestrates the deterministic multi-phase startup sequence:

```
[1. Storage Wiring Construction]
         │ (storage::storage_wiring_try_from_config)
         ▼
[2. Mutation Authority Lease Acquisition]
         │ (RuntimeMutationAuthority::acquire)
         ▼
[3. Repository Membership Readiness Preflight]
         │ (Fail-closed check; auto-init on empty store, error on unmigrated data)
         ▼
[4. BlobRefIndex Opening & Health Recovery]
         │ (BlobRefIndex::open, ensure_healthy_or_rebuild)
         ▼
[5. Proxy & Proxy-Cache Storage Construction]
         │ (storage::proxy_cache_storage_try_from_config)
         ▼
[6. Consistency Coordinator & GC Service Instantiation]
         │ (Single ConsistencyCoordinator::new, GcService::with_coordinator_and_authority)
         ▼
[7. Centralized Application Service Assembly]
         │ (assemble_application_services)
         ▼
[8. AppState & Phase Hook Execution]
         │ (AppState constructed, final Phase::AppStateConstructed verified)
         ▼
    ServerRuntime
```

### 2.3. Typed Build Errors & Automatic Failure Unwinding

All construction failures are categorized into a typed enumeration `RuntimeBuildError`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum RuntimeBuildError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("storage initialization error: {0}")]
    Storage(#[from] StorageError),
    #[error("mutation authority acquisition failed: {0}")]
    Authority(String),
    #[error("repository membership backfill required: ...")]
    MembershipBackfillRequired,
    #[error("blob ref index initialization error: {0}")]
    IndexInit(String),
    #[error("blob ref index open error: {0}")]
    IndexOpen(String),
    #[error("proxy upstream configuration error: {0}")]
    ProxyUpstreamInit(String),
    #[error("proxy initialization error: {0}")]
    ProxyInit(String),
    #[error("proxy cache storage initialization error: {0}")]
    ProxyCache(String),
    #[error("startup phase hook failed at {phase:?}: {message}")]
    PhaseHook { phase: StartupPhase, message: String },
}
```

**Unwinding Guarantee:** If an error occurs after mutation authority is acquired (phases 3–8), `build_server_runtime` explicitly releases `mutation_authority.release().await` before returning `Err(RuntimeBuildError)`, ensuring zero orphaned locks or dirty cluster leases.

---

## 3. Storage Factory Centralization (`src/storage/mod.rs`)

Concrete storage backend instantiation is removed from `src/supervisor.rs`.
* `src/storage/ports/mod.rs` remains strictly focused on capability traits, blanket implementations, and `StorageWiring`.
* Concrete selection helpers (`storage_from_config`, `storage_wiring_try_from_config`, `proxy_cache_storage_try_from_config`) reside in `src/storage/mod.rs`.

---

## 4. Single Source of Truth for Application Service Graph Assembly

All 7 application services are instantiated in exactly one place: `assemble_application_services` in `src/runtime.rs`:
* `BlobMutationService`
* `ManifestMutationService`
* `BlobReadService`
* `ManifestReadService`
* `CatalogQueryService`
* `TagQueryService`
* `ReferrersQueryService`

Test harnesses (`AppState::new_test`, `AppState::new_test_with_proxy`) delegate directly to `crate::runtime::build_test_app_state`, ensuring test runtime configurations traverse the identical service graph construction path as production.

---

## 5. Supervisor Narrowing (`src/supervisor.rs`)

`src/supervisor.rs` is reduced to pure orchestration:
1. Calls `crate::runtime::build_server_runtime(config, injector).await`.
2. Registers `runtime.flush_for_shutdown()` as the supervisor's durability flush hook.
3. Spawns supervised worker tasks (`spawn_upload_reaper`, `spawn_blob_gc_scheduler`, `spawn_proxy_gc`, `spawn_proxy_scrub`, `spawn_public_server`).
4. On graceful shutdown or failure exit, cleanly releases authority via `runtime.release_mutation_authority().await`.
5. Contains zero imports or usages of concrete backends (`FsStorage`, `S3Storage`) or low-level storage traversal helpers.

---

## 6. Verification & Guarantees

* **Zero Concrete Storage in Supervisor:** Verified 0 occurrences of `FsStorage` and `S3Storage` in `src/supervisor.rs`.
* **Zero Duplicated Service Constructors:** Verified `BlobMutationService::new`, `ManifestMutationService::new`, `BlobReadService::new`, `ManifestReadService::new`, `CatalogQueryService::new`, `TagQueryService::new`, `ReferrersQueryService::new` exist solely in `src/runtime.rs`.
* **Pure Storage Ports:** Verified `src/storage/ports/mod.rs` contains no concrete storage construction helpers; defines dedicated `StorageReadinessInspector` port, keeping `RepositoryCatalogReader` purely repository-scoped.
* **Fail-Closed Readiness & Unwinding:** Verified across all startup phases, fail-closed multi-page S3 pagination, loop detection, and unmigrated storage rejection scenarios in `src/runtime.rs` unit tests.
* **Live S3/MinIO Integration:** Verified full `ServerRuntime` construction, service execution, clean flush, authority release, and resource teardown against live MinIO S3 backend.
* **Full Test Suite:** 691 tests passing across 18 test suites.
