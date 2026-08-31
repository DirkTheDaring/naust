# ADR-002: Establishment of the Application Service Layer and Thinning of Mutation HTTP Handlers

* **Status:** Accepted
* **Date:** 2026-08-30
* **Authors:** Senior Software Architect (OCI Distribution & Storage Systems)
* **Scope:** Application Service Boundary, HTTP Transport Layer Thinning, Domain Model Isolation
* **Refines:** `docs/architecture/adr-001-first-refactoring-boundary.md`, `docs/architecture/current-code-assessment.md`

---

## 1. Context & Problem Statement

Following the successful execution of ADR-001 (Slice 1: `ConsistencyCoordinator` encapsulation), the registry codebase still suffered from an unencapsulated HTTP transport boundary:
1. `AppState` exposed raw fields, directly exposing internal domain primitives (`BlobUploadCoordinator`, `BlobDeleteService`, `ManifestLifecycleService`, `RepositoryMembershipLedger`) to the HTTP transport layer (`src/http_api/`).
2. Transport handlers in `src/http_api/handlers.rs` and `src/http_api/tags.rs` were "fat handlers"—directly orchestrating business workflows, parsing protocol specifics while simultaneously manipulating internal mutation coordinators, managing WAL journal states, and performing ad-hoc error mapping.
3. No pure, protocol-agnostic application layer existed to execute use cases independently of HTTP headers, Axum status codes, or OCI error representations.
4. Testing mutation business logic required booting full Axum HTTP routers and constructing synthetic HTTP requests with HeaderMaps and query parameters.

---

## 2. Decision: Cohesive Application Service Layer (`src/application/`)

We introduce a dedicated, pure application layer in `src/application/` composed of two primary application services:

### 2.1. `BlobMutationService` (`src/application/blob.rs`)
Encapsulates all blob ingestion, session management, cross-repository mounting, proxy ingestion, safe unlinking, and expired session reaping:
* `start_upload(repo: &str) -> Result<StartUploadResult, BlobMutationError>`
* `get_upload_status(repo: &str, uuid: &str, state_token: Option<&str>) -> Result<UploadStatusResult, BlobMutationError>`
* `append_chunk(repo: &str, uuid: &str, state_param: &str, range: Option<(u64, u64)>, content_len: Option<u64>, stream: UploadByteStream) -> Result<AppendResult, BlobMutationError>`
* `abort_upload(repo: &str, uuid: &str, state_token: Option<&str>) -> Result<(), BlobMutationError>`
* `finalize_upload(repo: &str, uuid: &str, state_token: Option<&str>, range: Option<(u64, u64)>, trailing_stream: Option<UploadByteStream>, digest: &Digest) -> Result<FinalizeResult, BlobMutationError>`
* `monolithic_upload(repo: &str, digest: &Digest, stream: Option<UploadByteStream>) -> Result<MonolithicUploadResult, BlobMutationError>`
* `cross_mount(target_repo: &str, source_repo: Option<&str>, digest: &Digest) -> Result<CrossMountResult, BlobMutationError>`
* `link_proxy_blob_membership(repo: &str, digest: &Digest) -> Result<(), BlobMutationError>`
* `publish_proxy_blob(repo: &CanonicalRepoName, digest: &Digest, stream: UploadByteStream) -> Result<(), BlobMutationError>`
* `delete_repo_blob(repo: &str, digest: &Digest) -> Result<BlobDeleteResult, BlobMutationError>`
* `reap_expired_uploads(max_age_secs: u64, receipt_ttl_secs: u64) -> Result<usize, BlobMutationError>`

### 2.2. `ManifestMutationService` (`src/application/manifest.rs`)
Encapsulates all manifest publication, tag lifecycle mutations, proxy-cached entry promotion, and proxy eviction:
* `publish_manifest(req: PublishManifestRequest) -> Result<PublishedManifest, ManifestMutationError>`
* `delete_manifest(repo: &str, digest: &Digest) -> Result<ManifestDeleteResult, ManifestMutationError>`
* `delete_tag(repo: &str, tag: &str, allow_tag_overwrite: bool) -> Result<TagDeleteResult, ManifestMutationError>`
* `publish_manifest_from_proxy(evidence: ProxyPublicationEvidence) -> Result<PublishedManifest, ManifestMutationError>`
* `evict_proxy_manifest_and_memberships(repo: &str, target_digest: &Digest, tag: Option<&str>) -> Result<ProxyEvictionResult, ManifestMutationError>`

---

## 3. Strict Boundary & Concurrency Rules

1. **Protocol Independence:** `src/application/` contains zero references to `axum`, `http`, `StatusCode`, `HeaderMap`, or `Response`. All application signatures use pure Rust types (`Bytes`, `Digest`, `&str`, streams, and strongly typed request/response structs).
2. **Typed Application Errors:** Application services return strongly typed domain errors (`BlobMutationError`, `ManifestMutationError`) preserving error identity and typed source chains (`std::error::Error::source`) without generic strings or status code embeddings.
3. **Transport Layer Thinning:** HTTP handlers in `src/http_api/handlers.rs` and `src/http_api/tags.rs` are strictly reduced to:
   - Transport parameter parsing (URL paths, headers, query parameters).
   - Authentication/authorization challenge generation.
   - Forwarding to `state.blob_service` or `state.manifest_service`.
   - Pure mapping of application outcomes to standard OCI HTTP status codes and headers via centralized mapper functions (`blob_mutation_error_to_response`, `manifest_mutation_error_to_response`).
4. **Single Source of Truth & Guard Ownership:**
   - Raw coordinator handles (`BlobUploadCoordinator`, `BlobDeleteService`, `ManifestLifecycleService`, `RepositoryMembershipLedger`) are removed from `AppState` and accessed exclusively through `blob_service` and `manifest_service`.
   - Mutation guards (`MutationGuard`) are acquired internally within the domain engines (`BlobUploadCoordinator`, `ManifestLifecycleService`, `RepositoryMembershipLedger`, `BlobDeleteService`) exactly once per mutating operation. Handlers never touch concurrency coordinators.
5. **Remaining Direct Storage Access (Read-Only):**
   - Direct storage access in `src/http_api/` is strictly constrained to read-only queries: `list_repositories`, `repo_timestamps`, `list_tags` (`catalog.rs`, `tags.rs`), `list_referrers` (`referrers.rs`), and `open_blob` / `get_manifest` streaming (`handlers.rs`).
   - Full storage-port segregation (decoupling read ports from omnibus storage) remains explicitly deferred to **Slice 3**.

---

## 4. Verification & Test Inventory

### Verification Results
* **Test Suite Expansion:** Total listed tests expanded from **602** (Slice 1 baseline) to **611** (Slice 2), with exactly **+9 new deterministic application-service tests** in `tests/application_service_tests.rs` (7 lifecycle/mutation workflows + 2 typed error source preservation tests) and 0 tests removed.
* **Execution Results:** **611 listed, 611 passed, 0 failed, 0 ignored** across all 16 test binaries executed with `set -o pipefail`.
* **MinIO S3 Live Contract:** Validated using the repository's pinned MinIO container (`docker.io/minio/minio:RELEASE.2025-09-07T16-13-09Z`), passing all 29 S3 integration tests in `tests/s3_live_integration.rs`.
* **Structural Audits:** All ripgrep structural checks confirmed zero HTTP leakage into `src/application/`, zero unencapsulated mutation coordinator access in `src/http_api/`, and zero stringly typed errors.
* **Full Toolchain:** `cargo fmt --check`, `cargo check --locked --all-targets --all-features`, `cargo clippy --locked --all-targets --all-features -- -D warnings`, and `cargo build --release --locked` passed with zero errors and zero warnings.
