# ADR-003: Storage Capability Port Segregation and Production Migration

* **Status:** Accepted
* **Date:** 2026-08-31
* **Authors:** Senior Software Architect (OCI Distribution & Storage Systems)
* **Scope:** Storage Capability Ports, Interface Segregation Principle (ISP), Production Consumer Migration, Shared Backend Wiring
* **Refines:** `docs/architecture/adr-001-first-refactoring-boundary.md`, `docs/architecture/adr-002-application-service-boundary.md`, `docs/architecture/current-code-assessment.md`

---

## 1. Context & Problem Statement

Following Slice 1 (ADR-001: `ConsistencyCoordinator` encapsulation) and Slice 2 (ADR-002: Application Service Layer & thin HTTP mutation handlers), the registry system still had an omnibus storage coupling:
1. **Interface Segregation Principle (ISP) Violation:** The monolithic `Storage` trait combined 20+ unrelated storage capabilities (CAS blob reads, multi-part chunked uploads, tag lookups, tag mutations, manifest indexing, WAL journal writes, repository lease locks, cluster deployment locks, referrers graph mutations, and GC scanning) into a single omnibus interface.
2. **Overprivileged Consumers:** Read-only transport routes (e.g. `catalog.rs`, `tags.rs`, `referrers.rs`, blob/manifest read streams in `handlers.rs`) and internal domain engines (e.g. `BlobRefIndex`, `RuntimeMutationAuthority`, `BlobUploadCoordinator`, `ManifestLifecycleService`) took `Arc<dyn Storage>`. This made it impossible to enforce at compile time that read-only transport paths cannot execute mutations or delete data.
3. **Omnibus Test Fakes:** Testing specialized components required implementing or mocking the entire 20+ method `Storage` trait, rather than providing focused capability fakes for the exact operation under test.
4. **`AppState` Omnibus Exposure:** `AppState` held `pub storage: Arc<dyn Storage>`, allowing any subsystem to reach into raw storage indiscriminately.

---

## 2. Decision: Segregated Capability Ports (`src/storage/ports/`)

We introduce cohesive capability ports and composite service ports in `src/storage/ports/mod.rs` and migrate every production consumer to its minimum required capability interface.

### 2.1. Granular Capability Ports

| Port Trait | Responsibility & Methods | Primary Consumers |
|---|---|---|
| `BlobCasReader` | Read-only CAS blob access: `head_blob`, `open_blob` | HTTP blob downloads, Blob CAS checks, Policy evaluation |
| `BlobCasWriter` | Blob uploads & chunk assembly: `create_upload`, `upload_status`, `append_upload`, `finalize_upload`, `abort_upload` | `BlobUploadCoordinator`, `BlobMutationService` |
| `RepositoryCatalogReader` | Repository catalog queries: `list_repositories`, `repo_timestamps` | `src/http_api/catalog.rs`, Catalog handlers |
| `ManifestReader` | Read-only manifest retrieval: `head_manifest`, `get_manifest`, `list_manifest_digests_page` | HTTP manifest GET/HEAD, GC traversers |
| `ManifestStore` | Manifest mutations: `put_manifest`, `delete_manifest` (super-trait: `ManifestReader`) | `ManifestLifecycleService`, `ManifestMutationService` |
| `TagReader` | Read-only tag resolution: `resolve_tag`, `list_tags`, `list_tags_page`, `get_tag_with_version` | `src/http_api/tags.rs`, Tag GET/HEAD handlers |
| `TagStore` | Tag mutations: `set_tag`, `mutate_tag`, `delete_tag`, `delete_tag_conditional` (super-trait: `TagReader`) | `ManifestLifecycleService`, `ManifestMutationService` |
| `ReferrersReader` | Read-only artifact referrers: `list_referrers`, `list_referrers_page` | `src/http_api/referrers.rs` |
| `ReferrersStore` | Referrers graph mutations: `add_referrer`, `remove_referrer` (super-trait: `ReferrersReader`) | `ManifestLifecycleService` |
| `LifecycleJournalStore` | Write-Ahead-Log journals: `read_lifecycle_journal`, `write_lifecycle_journal`, `delete_lifecycle_journal` | `ManifestLifecycleService` |
| `RepositoryLeaseStore` | Repository mutation leases: `acquire_repo_lease`, `renew_repo_lease`, `release_repo_lease` | `ManifestLifecycleService` |
| `ClusterLockStore` | Cluster deployment locks: `acquire_deployment_writer_lock`, `renew_deployment_writer_lock`, `release_deployment_writer_lock`, `inspect_deployment_writer_lock`, `force_unlock_deployment_writer` | `RuntimeMutationAuthority`, `Supervisor` |
| `GcStoragePort` | GC sweeping and quarantine: `gc_strategy`, `check_bucket_versioning_for_gc`, `list_cas_blobs_page`, `quarantine_blob`, `restore_quarantined_blob`, `quarantined_blob_version`, `delete_blob_conditional` | `GcService` |

### 2.2. Cohesive Composite Service Ports

For internal domain engines requiring a cohesive bundle of capabilities, explicit composite traits are defined:
* `BlobRefIndexStoragePort`: `RepositoryCatalogReader + ManifestReader + TagReader` (used by `BlobRefIndex`).
* `BlobUploadCoordinatorStoragePort`: `UploadSessionStorage + RepositoryBlobMembershipStorage + BlobCasReader + BlobCasWriter + BlobRefIndexStoragePort` (used by `BlobUploadCoordinator`).
* `ManifestLifecycleStoragePort`: `TagStore + ManifestStore + ReferrersStore + LifecycleJournalStore + RepositoryLeaseStore + BlobRefIndexStoragePort` (used by `ManifestLifecycleService`).
* `BlobIndexStoragePort`: `BlobCasReader + RepositoryBlobMembershipStorage` (used by `BlobDeleteService`).
* `GcServiceStoragePort`: `GcStoragePort + RepositoryCatalogReader + ManifestReader` (used by `GcService`).

### 2.3. Shared Backend Wiring (`StorageWiring`)

To prevent split-brain states and redundant allocations, `StorageWiring` encapsulates a single underlying backend instance (`Arc<FsStorage>` or `Arc<S3Storage>`):
```rust
pub struct StorageWiring {
    backend_kind: &'static str,
    blob_mutation: Arc<dyn BlobUploadCoordinatorStoragePort>,
    manifest_lifecycle: Arc<dyn ManifestLifecycleStoragePort>,
    blob_reader: Arc<dyn BlobCasReader>,
    membership_reader: Arc<dyn RepositoryBlobMembershipStorage>,
    manifest_reader: Arc<dyn ManifestReader>,
    tag_reader: Arc<dyn TagReader>,
    catalog_reader: Arc<dyn RepositoryCatalogReader>,
    referrers_reader: Arc<dyn ReferrersReader>,
    gc_port: Arc<dyn GcStoragePort>,
    gc_service_port: Arc<dyn GcServiceStoragePort>,
    cluster_lock: Arc<dyn ClusterLockStore>,
    blob_index: Arc<dyn BlobIndexStoragePort>,
    blob_ref_index: Arc<dyn BlobRefIndexStoragePort>,
}
```
`StorageWiring::from_backend(backend: Arc<S>)` constructs individual trait object views from a single `backend.clone()`, ensuring all ports share identical mutexes, caches, and storage state.

---

## 3. Production Consumer Migrations

1. **`AppState` (`src/app_state.rs`):**
   - Removed `pub storage: Arc<dyn Storage>`.
   - Exposes only narrow, read-only capability views:
     - `blob_reader: Arc<dyn BlobCasReader>`
     - `membership_reader: Arc<dyn RepositoryBlobMembershipStorage>`
     - `manifest_reader: Arc<dyn ManifestReader>`
     - `tag_reader: Arc<dyn TagReader>`
     - `catalog_reader: Arc<dyn RepositoryCatalogReader>`
     - `referrers_reader: Arc<dyn ReferrersReader>`
2. **HTTP Transport Layer (`src/http_api/`):**
   - `catalog.rs`: uses `state.catalog_reader` (`RepositoryCatalogReader`).
   - `tags.rs`: uses `state.tag_reader` (`TagReader`).
   - `referrers.rs`: uses `state.referrers_reader` (`ReferrersReader`).
   - `handlers.rs`: read routes use `state.blob_reader` (`BlobCasReader`), `state.manifest_reader` (`ManifestReader`), and `state.membership_reader` (`RepositoryBlobMembershipStorage`). All mutation routes continue to delegate exclusively to `state.blob_service` and `state.manifest_service`.
3. **Application & Domain Services:**
   - `BlobMutationService`: uses `Arc<dyn BlobUploadCoordinatorStoragePort>`.
   - `ManifestMutationService`: uses `Arc<dyn ManifestLifecycleStoragePort>`.
   - `BlobRefIndex`: methods take `&(impl BlobRefIndexStoragePort + ?Sized)`.
   - `BlobDeleteService`: accepts `&(impl BlobIndexStoragePort + ?Sized)`.
   - `GcService`: uses `Arc<dyn GcServiceStoragePort>` and `Arc<dyn BlobRefIndexStoragePort>`.
   - `RuntimeMutationAuthority`: accepts `Arc<dyn ClusterLockStore>`.
   - `PolicyContext`: accepts `&(impl GcServiceStoragePort + ?Sized)`.
   - `ProxyService`: accepts `&(impl BlobUploadCoordinatorStoragePort + ?Sized)`.

---

## 4. Invariants Preserved

* **Slice 1 Invariants:** Single `ConsistencyCoordinator` instance, dual-proof GC deletion (`GcMutationPermit` + `GcRevalidationGuard`), pin-before-CAS race prevention, WAL journal transitions, and typed domain error preservation.
* **Slice 2 Invariants:** Protocol-agnostic application service boundary (`BlobMutationService`, `ManifestMutationService`), thin HTTP handlers, single-acquisition mutation guards within domain engines, and strongly typed application errors.
* **Slice 3 Invariants:** Zero production consumers receive or store `Arc<dyn Storage>`. Read-only transport paths cannot execute mutations at compile time. Filesystem and S3 deployments share exactly one underlying storage instance across all port views.

---

## 5. Verification & Test Inventory

* **New Structural & Behavioral Tests:**
  - Added `tests/ports_wiring_tests.rs`:
    - `test_storage_wiring_shared_backend_state_across_port_views`: Deterministically verifies that mutations executed through `blob_mutation()` and `manifest_lifecycle()` port views are immediately reflected in `blob_reader()`, `membership_reader()`, and `tag_reader()` views on the same wiring.
    - `test_isolated_blob_cas_reader_consumer_without_omnibus_storage`: Verifies that a consumer requiring only `BlobCasReader` functions with a minimal fake without implementing omnibus storage.
    - `test_isolated_tag_reader_consumer_without_omnibus_storage`: Verifies that a consumer requiring only `TagReader` functions with a minimal fake without implementing omnibus storage.
* **Full Test Suite:** 614 passed (611 baseline + 3 new tests), 0 failed, 0 ignored across all test binaries.
* **Live S3 MinIO Suite:** Validated using the repository's pinned MinIO container (`docker.io/minio/minio:RELEASE.2025-09-07T16-13-09Z`), passing all S3 integration tests.
* **Quality Gates:** `cargo fmt --check`, `cargo check --locked --all-targets --all-features`, `cargo clippy --locked --all-targets --all-features -- -D warnings`, and `cargo build --release --locked` all pass with zero errors and zero warnings.
