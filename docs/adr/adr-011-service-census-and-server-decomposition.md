# ADR-011: Service Census and Server Decomposition Closure (KI-26)

* **Status:** Accepted (2026-09-26; remediation plan R4, `plans/technical-debt-remediation-plan.md`)
* **Authors:** Senior Software Architect
* **Scope:** Final application-service census, `/token` and admin-GC facade closure, handler-family decomposition, recorded residues of the Wave-1 coupling item KI-26
* **Refines:** ADR-002/ADR-004 (application services), ADR-010 (crate boundary)

---

## 1. Context

KI-26 collected the Wave-1 residual couplings: the monolithic `handlers.rs` dispatcher with ~58 direct `state.config` reads, the `/_admin/gc/*` and `/token` endpoints bypassing every service layer, the never-built assessment-§10.1 `GarbageCollectionService`/`ProxyService` facades (a re-scoping that was never recorded as a decision), `AppState` as a process bag, and the omnibus `Storage` trait as the internal port-implementation vehicle.

## 2. Decision: the service census

The registry's use-case surface consists of **seven core application services** (in `registry-core`, ADR-010): `BlobMutationService`, `ManifestMutationService`, `BlobReadService`, `ManifestReadService`, `CatalogQueryService`, `TagQueryService`, `ReferrersQueryService` — plus **two server services**: `GcAdminService` (`src/gc_admin.rs`: owns the admin run-id sequence and the `GcAdminPolicy` defaults snapshot; the four admin handlers are parse/auth/delegate/format) and `TokenService` (`src/token_service.rs`: validation, scope decision, push-allowlist enforcement, signing, observability; the `/token` handler is parse/delegate/format).

**Recorded re-scoping decisions** (closing the "never recorded" sub-item):

* The assessment's `GarbageCollectionService` facade is realized as core `GcService` (engine) + server `GcAdminService` (admin facade).
* The assessment's `ProxyService` facade is deliberately **not** built as such: `upstream::UpstreamFetcher` + `application::ProxyTarget` (ADR-010) are its replacement — the application layer consumes exactly that seam.

## 3. Decision: handler decomposition and policy snapshots

`http_api/handlers.rs` is a dispatcher only; the family handlers live in `blobs.rs`, `manifests.rs`, `uploads.rs`. Mechanical transfer knobs are read from the `HttpTransferPolicy` snapshot (`http_api/policy.rs`, `From<&Config>` with an exhaustive-destructuring mapping test — the ADR-010 `GcPolicy` pattern), never from `state.config`.

## 4. Recorded residues (accepted architecture, not deferred defects)

1. **Token/auth decision functions stay parameterized by `&Config`** (`decide_token_scopes_for_request` and helpers). They are pure policy functions over config sub-structures, unit-tested as such; mirroring ~14 config fields into a parallel context struct would duplicate structure without removing the coupling. Revisit only if the server config itself is decomposed.
2. **Auth-flavored reads inside upload/manifest handlers** (bearer verification against the key ring, auth-strategy branches, private-repo checks) remain reads of `state.config`: they are the HTTP auth boundary's own concern (ADR-002's middleware + challenge logic), not transfer policy.
3. **`AppState` keeps its flat field list** (config, services, proxy runtime, semaphores, IP limiter). With the config-read and bypass defects gone, regrouping fields into nested context structs is naming churn without a seam change; not worth the ripple through supervisor/middleware call sites.
4. **Omnibus `Storage` remains the internal port-implementation vehicle** in `registry-core` (assessment Q1) — reaffirmed: the ports are the contract; the macro expansion is an implementation detail invisible to consumers.

## 5. Consequences

KI-26 closes with residues 1–4 as recorded accepted architecture (its criterion: "each residual either scheduled or recorded as accepted architecture"). `gc_run_seq` left `AppState`; `/token` and admin GC are service-mediated; the black-box conformance and integration suites froze behavior across the decomposition.
