# ADR-010: `registry-core` Crate Boundary — Registry Primitives as a Separate Crate

* **Status:** Accepted (execution starting at `master` @ `be40792`)
* **Date:** 2026-09-26
* **Authors:** Senior Software Architect
* **Scope:** Crate topology, core/server module partition, proxy seam, core-owned policy types, backend feature disposition, dependency posture, stability posture, explicit non-goals
* **Refines:** ADR-002 (application service boundary), ADR-003 (storage capability ports), ADR-004 (application read services)
* **Plan:** [`../../plans/registry-core-extraction-plan.md`](../../plans/registry-core-extraction-plan.md) (phases, sizing, risk register, verified coupling inventory)

---

## 1. Context & Problem Statement

The Wave-1/Wave-2 refactorings produced a de-facto layered architecture inside one crate: transport-free application services (`src/application/`), domain engines (upload coordinator, manifest lifecycle, membership ledger, GC, ref-index), capability ports (`src/storage/ports/`), and two backends. The goal is to make that layer a real, separately consumable crate — `registry-core` — so that any transport/auth/config stack can implement a registry on top of it, with the existing binary as the first consumer.

A verified coupling inventory (plan appendix, at `be40792`) shows the boundary is crossed at seven production surfaces plus test fixtures, and that `src/proxy.rs` and `src/application/` are mutually dependent — a cycle that cannot cross a crate boundary.

## 2. Decisions

### 2.1 Topology

Cargo workspace in this repository: `crates/registry-core` (library) + `crates/registry-rust` (server/CLI binary). Sibling path-deps: `storage-core`/`storage-fs`/`storage-s3` become core dependencies; `acmecert-core` stays server-only. The module partition (every `lib.rs` module assigned) is recorded in the plan, Phase 2.

### 2.2 Proxy seam

Core defines an `UpstreamFetcher` trait plus core-owned `ProxyError`, `RepoDecision`, `FetchManifestResult`, and `TagMeta` types, carrying exactly the surface `application/{manifest_read,proxy,errors}.rs` uses today. The reqwest engine (`src/proxy.rs`) stays in the server and implements the trait; its back-calls into `application::{Blob,Manifest}MutationService` become legal server→core edges.

**Recorded fallback** (decided explicitly at plan gate G2, never silently): if the trait exceeds ~8 methods or must leak reqwest/config types, the proxy-aware read paths move to the server instead — a smaller core, not a stall.

### 2.3 Core-owned policy types

A `policy` module in core owns the configuration types core logic consumes: `TagPolicy`, a new `GcPolicy` (replacing `Arc<Config>` in `GcService` and the 13 `&Config` signatures in `blob_gc/`), `LegacyMultipartCleanupPolicy`, and an upstream-route type. The server's `Config` maps into them via tested `From` impls. The GC deletion-strategy branch on `config::StorageBackend` becomes a storage-port **capability query** (quarantine support); core never encodes backend identity.

### 2.4 Backend disposition

Both backends (`storage/fs.rs`, `storage/s3.rs`) move into core **unconditionally**. The `storage-fs`/`storage-s3` cargo features are vestigial — they gate zero code in `src/`, `tests/`, Makefile, or Dockerfile — and are **deleted**, not carried across. Real feature gating, or split `registry-storage-*` crates, are possible later work, out of scope.

### 2.5 Accepted core dependencies

sled (`BlobRefIndex`), tokio, the storage-layer sibling crates, and the always-linked AWS SDK are accepted core dependencies, documented here consciously (plan risk R8). Revisited only when a second consumer exists; the ports keep later extraction possible.

### 2.6 Stability posture

`registry-core` is 0.x and explicitly unstable until a second consumer exists. The deprecated `manifest_publication` shim (ADR-007) is dropped during the split, not moved.

### 2.7 Boundary enforcement

An **allowlist** gate (Makefile/CI) enforces that files in core-listed modules import only core-listed modules, **including inside `#[cfg(test)]`** — test fixtures constructing the server `Config` are boundary violations too. The gate is built before any signature work (plan Phase 1a) and stays green permanently.

## 3. Non-Goals (scope fence)

* No KI-26 handler cleanup (the 58 `state.config` reads in `http_api/handlers.rs` stay as-is).
* No `registry-storage-*` crate split.
* No new feature engineering (replacing the deleted vestigial features with real gating is future work).
* No proxy-engine rewrite; the reqwest implementation moves behind the trait untouched.

## 4. Consequences

* KI-07 closes (the `upload_state` module moves to the core side).
* KI-26's blast radius narrows: core is enforced clean without touching handler internals.
* Embedders inherit sled/tokio/AWS-SDK (accepted, 2.5).
* Two crates mean per-crate test invocation and a workspace-aware build/packaging pipeline (plan Phase 2, gate G3).
* Stopping after plan Phase 1 (boundary enforced, no crate split yet) is a legitimate, consistent end state.
