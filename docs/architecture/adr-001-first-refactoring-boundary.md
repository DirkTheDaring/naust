# ADR-001: Selection and Design of the First Architectural Refactoring Boundary

* **Status:** Accepted
* **Date:** 2026-08-26
* **Authors:** Senior Software Architect (OCI Distribution & Storage Systems)
* **Scope:** Concurrency, Application Layering, and Storage Coupling
* **Supersedes / Refines:** `docs/architecture/current-code-assessment.md`

---

## 1. Context & Problem Statement

The architecture assessment (`docs/architecture/current-code-assessment.md`) identified several structural deficiencies in `registry-rust`:
1. The `Storage` trait has grown into a 47-method omnibus interface (29 direct methods + 18 inherited across 3 supertraits).
2. HTTP handlers in `src/http_api/handlers.rs` orchestrate domain operations directly across 22 unencapsulated fields in `AppState`.
3. Mutual exclusion between client mutations and garbage collection reachability revalidation previously relied on convention: callers had to manually remember to acquire an unencapsulated in-memory lock (`consistency_gate: Arc<tokio::sync::Mutex<()>>`).
4. Inline `#[cfg(test)] mod tests` make up 35.7% of the total lines in `src/`, with individual files exceeding 50% to 79% test code.
5. `src/manifest_publication.rs` is retained as a 972-line file containing 943 lines of fault-injection unit tests for a 28-line re-export shim.

The assessment initially proposed **Storage Trait Segregation (Candidate A)** as the first slice with a blanket composite trait `Storage`. However, deeper architectural review revealed a fundamental flaw with that proposal:

> **The Blanket Trait Trap:** Simply splitting `Storage` into sub-traits while retaining a blanket `impl<T> Storage for T` and leaving application services taking `Arc<dyn Storage>` achieves zero dependency decoupling. In Rust with `#[async_trait]`, trait object upcasting is not automatic; consumers cannot seamlessly convert `Arc<dyn Storage>` into `Arc<dyn BlobCasStorage>` without composition-root proliferation or accessor boilerplate. Furthermore, `Storage` bloat is an *effect*, not the *cause*, of fat handlers and uncoordinated domain access.

We select and design the genuinely highest-leverage first architectural refactoring slice that establishes an enforceable dependency boundary.

---

## 2. Storage Consumer / Capability Matrix

To understand true storage coupling, we audit every production consumer against actual required storage operations:

| Consumer | Primary Responsibilities | Direct Storage Capabilities Called | Minimal Cohesive Interface |
|---|---|---|---|
| **`BlobUploadCoordinator`** (`src/upload_coordinator.rs`) | Chunk streaming, HMAC session tokens, CAS commit, ref-index pinning | `create_session`, `get_session`, `update_session`, `append_chunk`, `commit_blob`, `abort_session`, `link_repo_blob`, `get_repo_blob_membership`, `head_blob` | `UploadSessionStore` + `RepoMembershipStore` + `BlobCasReader` |
| **`ManifestLifecycleService`** (`src/manifest_lifecycle.rs`) | Manifest validation, WAL journal, tag mutations, referrers DAG | `read/write/delete_lifecycle_journal`, `acquire/renew/release_repo_lease`, `get/put/delete_manifest`, `resolve/set/mutate/delete_tag`, `list_tags_page`, `list_manifest_digests_page`, `add/remove/list_referrers`, `get_repo_blob_membership`, `head_blob` | `ManifestStore` + `TagStore` + `ReferrersStore` + `LifecycleJournalStore` + `RepoLeaseStore` |
| **`RepositoryMembershipLedger`** (`src/repository_membership_ledger.rs`) | Authoritative repo-scoped blob membership index | `get_repo_blob_membership`, `link_repo_blob`, `unlink_repo_blob`, `list_repo_blob_memberships_page`, `list_repositories`, `head_blob` | `RepoMembershipStore` + `BlobCasReader` + `RepoCatalogReader` |
| **`BlobDeleteService`** (`src/blob_delete_safety.rs`) | Safe repo-scoped blob unlinking | `list_tags`, `resolve_tag`, `get_manifest`, `unlink_repo_blob` | `RepoMembershipStore` + `TagStore` + `ManifestStore` |
| **`Proxy`** (`src/proxy.rs`) | Upstream proxy caching & TTL eviction | `open_blob`, `head_blob`, `get/put/delete_manifest`, `resolve/set/mutate/delete_tag`, `get/link/unlink_repo_blob` | `BlobCasStore` + `ManifestStore` + `TagStore` + `RepoMembershipStore` |
| **`GcService` & `blob_gc`** (`src/gc_service.rs`, `src/blob_gc/`) | 3-phase sweep, quarantine, ETag delete | `list_cas_blobs_page`, `quarantine_blob`, `restore_quarantined_blob`, `delete_blob_conditional`, `check_bucket_versioning_for_gc`, `list_repositories`, `list_tags_page`, `list_manifest_digests_page`, `get_manifest`, `read_lifecycle_journal` | `GcStoragePort` + `RepoCatalogReader` + `ManifestReader` + `TagReader` |
| **`RuntimeMutationAuthority`** (`src/storage/mutation_authority.rs`) | Deployment writer lease ownership | `acquire/release/inspect/admin_clear_deployment_writer_lock` | `ClusterLockStore` |
| **`MembershipMigration`** (`src/membership_migration.rs`) | Legacy global-to-canonical backfill | `list_repositories`, `list_manifest_digests_page`, `get_manifest`, `list_tags_page`, `get/link_repo_blob` | `RepoCatalogReader` + `ManifestReader` + `TagReader` + `RepoMembershipStore` |
| **`Supervisor`** (`src/supervisor.rs`) | Process boot & readiness check | `list_repositories`, `inspect_deployment_writer_lock`, `check_bucket_versioning_for_gc` | `RepoCatalogReader` + `ClusterLockStore` + `GcPreflight` |
| **`HTTP Handlers`** (`src/http_api/handlers.rs`) | OCI protocol dispatch & streaming | `open_blob`, `head_blob`, `get/head_manifest`, `list_repositories`, `repo_timestamps`, `list_tags_page`, `resolve_tag`, `list_referrers_page`, `get_repo_blob_membership` | ⚠️ Bypasses application services; calls raw storage |

### Consumer Conclusions
1. **Accidental Coupling in Handlers:** HTTP handlers directly invoke 9 different storage methods across `state.storage` and `ctx.cache` because application service boundaries are incomplete.
2. **True Cohesive Sub-Domains:**
   - **Upload & Blob Ingestion:** `UploadSessionStore` + `RepoMembershipStore`
   - **Manifest & Tag Lifecycle:** `ManifestStore` + `TagStore` + `ReferrersStore` + `LifecycleJournalStore`
   - **Cluster Coordination:** `ClusterLockStore` + `RepoLeaseStore`
   - **Storage Reclaim:** `GcStoragePort`
3. **No Component Genuinely Needs all 47 Methods:** The only reason `Storage` exists as a monolith is historical convenience.

---

## 3. Considered Options for First Refactoring Slice

We evaluate four candidate initial refactoring slices:

* **Candidate A:** Storage-port segregation with consumer constructor migration.
* **Candidate B:** Encapsulating `consistency_gate` behind a structurally enforced `ConsistencyCoordinator`.
* **Candidate C:** Extracting dedicated `BlobService` / `CatalogService` / `TagService` facades from fat HTTP handlers.
* **Candidate D:** Removing the `src/manifest_publication.rs` compatibility shim and relocating its 11 unit tests.

### Candidate Evaluation & Scoring (1 = Poor, 5 = Excellent)

| Evaluation Criterion | Candidate A (Storage Split) | Candidate B (Consistency Gate) | Candidate C (Handler Extraction) | Candidate D (Shim Removal) |
|---|:---:|:---:|:---:|:---:|
| **Architectural Leverage** | 2 | **5** | 4 | 1 |
| **Reduction of Invalid States** | 2 | **5** | 4 | 1 |
| **Reduction of Dependency Fan-Out** | 3 | **4** | **5** | 2 |
| **Behavior / Regression Risk** | 2 (High Churn) | **4** (Focused Concurrency Risk) | 4 (Medium) | 5 (Zero) |
| **Migration Radius** | 1 (Massive: 25+ files) | **4** (Focused: 18 modified files) | 3 (Moderate: 6 files) | 5 (Trivial: 2 files) |
| **Testability** | 3 | **5** | 4 | 4 |
| **Reversibility** | 2 | **5** | 4 | 5 |
| **Zero Parallel Sources of Truth** | 2 (Blanket Trait Trap) | **5** | 4 | 5 |
| **TOTAL SCORE (out of 40)** | **17** | **37** | **33** | **28** |

### Detailed Candidate Analysis

- **Why Candidate A is Rejected as First Slice:**
  Candidate A has massive blast radius (changing constructors across every service, test harness, and supervisor setup), but provides low immediate leverage because handlers and services still retain their underlying logic and concurrency assumptions. Without trait upcasting, `AppState` would either need multiple storage Arcs or a permanent blanket trait.
- **Why Candidate D is Inadequate as First Slice:**
  Candidate D is trivial cleanup (removing a 28-line production re-export shim and moving tests). It has no architectural leverage and establishes no significant dependency rule.
- **Why Candidate B is the Selected Slice:**
  Candidate B targets the single most critical concurrency invariant in the repository: **preventing unsynchronized storage mutations during GC reachability sweeps**. Replacing raw `Arc<Mutex<()>>` with a typed `ConsistencyCoordinator` and RAII `MutationGuard` / `GcRevalidationGuard` makes unsynchronized mutation structurally explicit across all mutating services. Concurrency refactoring inherently carries medium risk, which is fully mitigated by 10 deterministic integration tests and zero asynchronous sleeps.

---

## 4. Decision: Select Candidate B (Encapsulated Concurrency Boundary) Followed by Candidate C

We select **Candidate B (Encapsulating Consistency Coordination via RAII Guards)** as Architectural Slice 1, to be followed by **Candidate C (Application Service Extraction)** as Slice 2.

### Exact Dependency Rules Established by Slice 1:

> **Architecture Rule 1 (Consistency & Mutation Invariant):**
> 1. Raw storage mutation operations that alter blob reachability (manifest publication, tag creation/replacement/deletion, repository blob membership linking/unlinking, and proxy blob ingestion) MUST execute under a typed `MutationGuard`.
> 2. Garbage collection reachability revalidation MUST execute under a typed `GcRevalidationGuard` passed explicitly to candidate revalidation.
> 3. The underlying synchronization primitive (`tokio::sync::Mutex`) MUST NOT be public, MUST NOT be exposed on `AppState`, and MUST NOT be acquirable without producing a typed guard token.
> 4. Handlers never touch the consistency coordinator directly; they interact exclusively with application services (`BlobUploadCoordinator`, `ManifestLifecycleService`, `RepositoryMembershipLedger`, `BlobDeleteService`).
> 5. **Explicit Construction Rule:** `ConsistencyCoordinator` derives NO `Default`. Every production service constructor (`ManifestLifecycleService`, `BlobUploadCoordinator`, `RepositoryMembershipLedger`, `BlobDeleteService`, `GcService`) MUST require an explicit `ConsistencyCoordinator` parameter. No service may silently construct a private fallback coordinator.
> 6. **Single Composition Root Instance:** Exactly one coordinator instance is created by the server supervisor composition root and cloned to dependent services. Standalone CLI commands create one coordinator per isolated command execution.
> 7. **Application-Level Enforcement Guarantee vs. Low-Level Limitation:** All current application-level GC deletion flows (`execute_guarded_gc_deletion`) strictly require both independent proofs (`&GcMutationPermit` and `&GcRevalidationGuard`). The low-level `Storage` trait adapter still enforces mutation authority only; structural segregation of storage traits is deferred to subsequent architectural slices.

---

## 5. Design of Implemented Slice (`ConsistencyCoordinator`)

### 5.1 Production Module: `src/consistency.rs`

```rust
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard};

/// Coordination primitive providing mutual exclusion between
/// reachability-altering mutations and garbage collection reachability revalidation.
///
/// # Composition Policy
/// `ConsistencyCoordinator` is a library-level composition primitive.
/// Each application runtime composition root (e.g. server supervisor) or isolated
/// maintenance CLI execution MUST construct exactly one instance and inject clones
/// into all coordinator-dependent services. Independent coordinators do NOT synchronize.
#[derive(Clone, Debug)]
pub struct ConsistencyCoordinator {
    gate: Arc<Mutex<()>>,
}

/// An unforgeable RAII guard proving that a reachability-altering mutation is actively in progress.
#[derive(Debug)]
pub struct MutationGuard {
    _guard: OwnedMutexGuard<()>,
}

/// An unforgeable RAII guard proving that a GC reachability revalidation check is actively in progress.
#[derive(Debug)]
pub struct GcRevalidationGuard {
    _guard: OwnedMutexGuard<()>,
}

impl ConsistencyCoordinator {
    /// Creates a new, unlocked `ConsistencyCoordinator`.
    pub fn new() -> Self {
        Self {
            gate: Arc::new(Mutex::new(())),
        }
    }

    /// Asynchronously acquires exclusive execution for a reachability mutation.
    pub async fn acquire_mutation(&self) -> MutationGuard {
        let guard = self.gate.clone().lock_owned().await;
        MutationGuard { _guard: guard }
    }

    /// Asynchronously acquires exclusive execution for GC reachability revalidation.
    pub async fn acquire_gc_revalidation(&self) -> GcRevalidationGuard {
        let guard = self.gate.clone().lock_owned().await;
        GcRevalidationGuard { _guard: guard }
    }
}
```

### 5.2 Typed GC Outcome and Error Model (`src/blob_gc/validation.rs`)

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GcProtectionReason {
    ActiveUploadPin,
    RepositoryMembership { count: usize },
    ManifestOrTagReachable,
    ActiveLifecycleJournal { repository: String, op_id: String },
    PolicyAgeProtected,
}

#[derive(Debug, thiserror::Error)]
pub enum GcCandidateDeletionError {
    #[error("durable reference index health check failed: {0}")]
    IndexHealth(#[from] crate::blob_ref_index::RefIndexError),
    #[error("failed to query repository memberships for blob {digest}: {source}")]
    RepositoryMembershipQuery { digest: Digest, #[source] source: crate::storage::StorageError },
    #[error("repository enumeration failed during lifecycle journal pre-delete check: {0}")]
    RepositoryEnumeration(#[source] crate::storage::StorageError),
    #[error("failed to read lifecycle journal for repository '{repository}': {source}")]
    LifecycleJournalRead { repository: String, #[source] source: crate::storage::StorageError },
    #[error("corrupt lifecycle journal record in repository '{repository}': {source}")]
    LifecycleJournalCorrupt { repository: String, #[source] source: serde_json::Error },
    #[error("storage conditional deletion failed for blob {digest}: {source}")]
    StorageDelete { digest: Digest, #[source] source: crate::storage::StorageError },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GcCandidateDeletionOutcome {
    Deleted { size: u64 },
    Protected(GcProtectionReason),
    NotFound,
    PreconditionFailed { current_version: Option<storage::BlobObjectVersion> },
}
```

### 5.3 Lock Ordering Hierarchy

1. **Outer Leases & Locks:**
   - Per-repository lease on S3 (`acquire_repo_lease`) / POSIX flock on FS (`RepoCoordinationGuard` in `manifest_lifecycle.rs`).
   - S3 deployment writer lock / FS root lock (`RuntimeMutationAuthority`).
   - Service-level sequential run lock (`GcService.run_lock`).
2. **Short-Lived Consistency Coordinator (`src/consistency.rs`):**
   - `ConsistencyCoordinator.acquire_mutation()` -> `MutationGuard`.
   - `ConsistencyCoordinator.acquire_gc_revalidation()` -> `GcRevalidationGuard`.
3. **Storage Mutation / Precondition Execution:**
   - Conditional S3 ETag delete (`If-Match`), atomic rename, sled index flush.
   - Guard dropped via RAII before next candidate in GC or immediately after upload/manifest commit.

---

## 6. Architecture Fitness Tests

Compile-time and runtime architectural rules enforcing this boundary:

1. **Private Mutex Invariant:** `ConsistencyCoordinator.gate` is private to `consistency.rs`. No handler or outside service can directly call `.lock().await`.
2. **Dual-Proof Guarded Deletion:** `execute_guarded_gc_deletion(&storage, &idx, &candidate, now, &mut policy_ctx, &permit, &reval_guard)` requires both `&GcMutationPermit` and `&GcRevalidationGuard`, making un-coordinated or unauthorized GC candidate deletion structurally impossible at the application layer.
3. **Zero Raw Gate in `AppState`:** `AppState` completely removes the legacy `consistency_gate` field.
4. **Nested Guard Support:** `RepositoryMembershipLedger` provides `_with_guard(&MutationGuard, ...)` variants preventing double-locking / deadlocks.

---

## 7. Migration Sequence and Verification

### Acceptance Criteria Checklist
- [x] `cargo fmt --check` passes cleanly.
- [x] `cargo check --locked --all-targets --all-features` passes with 0 warnings.
- [x] `cargo clippy --locked --all-targets --all-features -- -D warnings` passes with 0 warnings.
- [x] All 602 automated unit, integration, and adversarial tests pass (`cargo test`).
- [x] `cargo build --release --locked` completes successfully.
- [x] Raw `Arc<tokio::sync::Mutex<()>>` consistency gate is completely removed from `src/` and `tests/`.
- [x] Zero changes to OCI HTTP protocol wire behavior or storage file layouts.

---

*End of ADR-001.*

