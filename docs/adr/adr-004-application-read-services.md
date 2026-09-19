# ADR-004: Complete Application Read Services, Query Services, and Proxy Encapsulation

* **Status:** Accepted
* **Date:** 2026-08-31
* **Authors:** Senior Software Architect (OCI Distribution & Storage Systems)
* **Scope:** Application Read Services, Query Services, Query Policy Segregation, Proxy Encapsulation, Handlers Thinning
* **Refines:** `docs/architecture/adr-001-first-refactoring-boundary.md`, `docs/architecture/adr-002-application-service-boundary.md`, `docs/architecture/adr-003-storage-capability-ports.md`, `docs/architecture/current-code-assessment.md`

---

## 1. Context & Problem Statement

Following Slice 1 (ADR-001: `ConsistencyCoordinator` encapsulation), Slice 2 (ADR-002: Application Mutation Services), and Slice 3 (ADR-003: Storage Capability Port Segregation), the read side of the system and proxy publication still presented architectural gaps:
1. **Query Policy in Transport Handlers:** Deterministic sorting, pagination slicing (`n` and `last` cursors), `has_more` computation, and artifact-type filtering were implemented ad-hoc across `src/http_api/catalog.rs`, `src/http_api/tags.rs`, and `src/http_api/referrers.rs`.
2. **Read Workflow Logic in Handlers:** Blob read flows (repository membership check, local CAS lookup, proxy cache lookup, upstream fetch, membership record generation) and manifest read flows (tag-to-digest resolution, tag freshness TTL/AlwaysRevalidate checking, local lookup, proxy cache lookup, upstream fetch, OCI content negotiation) were embedded inside `src/http_api/handlers.rs`.
3. **Mutation Engine Handle Leakage:** `BlobMutationService` and `ManifestMutationService` leaked inner engine handles via `pub fn coordinator()` and `pub fn lifecycle()`. The proxy subsystem bypassed application services to publish fetched blobs and manifests directly via these raw engine handles.
4. **Transport-Bound Proxy Context:** Handlers and proxy methods relied on Axum-coupled `ProxyContext` structures rather than a transport-neutral proxy execution target.
5. **Direct Storage Reader Access on `AppState`:** `AppState` directly exposed raw reader ports (`blob_reader`, `membership_reader`, `manifest_reader`, `tag_reader`, `catalog_reader`, `referrers_reader`), bypassing application service encapsulation.

---

## 2. Decision: Dedicated Application Read & Query Services (`src/application/`)

We introduce five cohesive, transport-neutral application read and query services in `src/application/`, completely decoupling transport handlers from storage reader ports and business query logic.

### 2.1. Application Read Services

| Service | Location | Responsibility & Methods |
|---|---|---|
| `BlobReadService` | `src/application/blob_read.rs` | Head and stream blobs with repository-scoped membership verification, local CAS lookup, proxy cache lookup, upstream fallback, and automatic membership record creation. Exposes `head_blob`, `get_blob`. |
| `ManifestReadService` | `src/application/manifest_read.rs` | Head and get manifests with reference resolution, canonical name validation, tag freshness revalidation, local lookup, proxy cache lookup, upstream fetch, payload size limits, and `OCI-Subject` extraction. Exposes `head_manifest`, `get_manifest`, `resolve_reference_digest`. |
| `CatalogQueryService` | `src/application/catalog.rs` | Deterministic repository listing, cursor-based pagination (`n`, `last`), `has_more` calculation, next-cursor generation, and repo timestamps. Exposes `query_catalog`, `list_repositories`, `repo_timestamps`. |
| `TagQueryService` | `src/application/tags.rs` | Deterministic lexicographical tag listing, cursor-based pagination (`n`, `last`), `has_more` calculation, next-cursor generation, and tag resolution. Exposes `query_tags`, `list_tags`, `resolve_tag`. |
| `ReferrersQueryService` | `src/application/referrers.rs` | Deterministic artifact referrers discovery sorted by digest hex, cursor-based pagination (`n`, `last`), `has_more` calculation, next-cursor generation, and `artifact_type` filtering. Exposes `query_referrers`. |

### 2.2. Transport-Neutral Proxy Boundary (`src/application/proxy.rs`)

To isolate the application layer from HTTP transport concerns, `src/application/proxy.rs` defines `ProxyTarget`:
```rust
pub struct ProxyTarget {
    pub proxy: Arc<Proxy>,
    pub cache_storage: Arc<dyn ProxyStoragePort>,
}
```
Application read services accept `Option<&ProxyTarget>`. Handlers pass the proxy target without leaking Axum/HTTP types into `src/application/`.

### 2.3. Encapsulated High-Level Proxy Publication

We removed `pub fn coordinator()` from `BlobMutationService` and `pub fn lifecycle()` from `ManifestMutationService`.
In `src/proxy.rs`, upstream blob and manifest caching now publish exclusively through high-level unified application mutation services:
* `fetch_blob_into_storage` calls `blob_service.publish_verified_proxy_blob(&decision.local_repo, digest, pinned_stream)`.
* `fetch_manifest_and_cache` calls `manifest_service.publish_verified_proxy_manifest(...)`.
Neither `src/proxy.rs` nor `src/application/blob_read.rs` nor `src/application/manifest_read.rs` orchestrates low-level upload steps (`create_upload`, `append_upload`, `finalize_upload`, `link_repo_blob`).

---

## 3. Thin HTTP Transport Handlers & Rewired `AppState`

1. **`AppState` (`src/app_state.rs`):**
   - Removed all raw reader fields (`blob_reader`, `membership_reader`, `manifest_reader`, `tag_reader`, `catalog_reader`, `referrers_reader`).
   - Holds only application services:
     - `blob_mutation_service: Arc<BlobMutationService>`
     - `manifest_mutation_service: Arc<ManifestMutationService>`
     - `blob_read_service: Arc<BlobReadService>`
     - `manifest_read_service: Arc<ManifestReadService>`
     - `catalog_query_service: Arc<CatalogQueryService>`
     - `tag_query_service: Arc<TagQueryService>`
     - `referrers_query_service: Arc<ReferrersQueryService>`
2. **Handlers (`src/http_api/`):**
   - `catalog.rs`: Parses query parameters (`n`, `last`), calls `catalog_query_service.query_catalog()`, formats RFC 5988 `Link` headers, and serializes JSON response.
   - `tags.rs`: Parses query parameters (`n`, `last`), calls `tag_query_service.query_tags()`, formats RFC 5988 `Link` headers, and serializes JSON response.
   - `referrers.rs`: Parses query parameters (`n`, `last`, `artifactType`), calls `referrers_query_service.query_referrers()`, formats RFC 5988 `Link` headers, and serializes OCI index JSON response.
   - `handlers.rs`: `blob_by_digest` and `manifest_by_reference` handle auth, parse parameters, delegate to `BlobReadService` and `ManifestReadService`, and map results to HTTP status codes, ranges, digests, content-lengths, and `OCI-Subject` headers.

---

## 4. Verification & Guarantees

* **Zero HTTP Leaks:** `src/application/` contains zero imports or usages of Axum/HTTP types (`StatusCode`, `HeaderMap`, `Response`, `IntoResponse`).
* **Zero Storage Wiring Leaks:** `src/application/` does not depend on `StorageWiring` or omnibus `dyn Storage`.
* **Zero Mutation Engine Leaks:** Neither `BlobMutationService` nor `ManifestMutationService` exposes raw coordinator/lifecycle engines.
* **Deterministic Behavior:** Deterministic sorting and cursor math verified across all query endpoints.
* **Backend Parity:** Verified across both local filesystem storage and live S3/MinIO in `tests/application_read_tests.rs`.

---

## Reconciliation addendum (2026-09-19, `master` `2718bc16`)

The decision stands as implemented. Naming map and residuals, recorded without altering the original rationale: the mutation-service fields named `blob_mutation_service`/`manifest_mutation_service` in §3.1 are `blob_service`/`manifest_service` in code (`src/app_state.rs:66-67`). Beyond §2.3's removals of `coordinator()`/`lifecycle()`, the services still expose `membership_ledger()`, `delete_service()`, `from_lifecycle()`, and raw reader-port getters.
