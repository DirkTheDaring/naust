# Plan: extract `registry-core` — a registry-primitives crate

- **Status:** in execution (progress log at end of file)
- **Date:** 2026-09-26
- **Analyzed code revision:** `master` @ `be40792`
- **Provenance:** three review passes over the initial proposal; all module assignments and coupling counts below were verified against the source at the revision above, not taken from `docs/architecture/README.md`.

**Goal.** A separate crate containing all primitives for handling registry objects (blobs, manifests, tags, referrers, uploads, membership, GC, consistency) — transport-free, auth-free, config-free — so anyone can implement a registry on top. The existing binary becomes its first consumer.

**Topology.** Cargo workspace in this repo: `crates/registry-core` (lib) + `crates/registry-rust` (server/CLI binary). Sibling path-deps unchanged; `storage-core`/`storage-fs`/`storage-s3` become core deps, `acmecert-core` stays server-only.

## Phase 0 — ADR-010: boundary decisions

1. **Proxy seam (load-bearing).** `proxy.rs` and `application/` are mutually dependent (`application/manifest_read.rs:82-187`, `application/proxy.rs:7`, `application/errors.rs:182,214` consume the engine; `proxy.rs:582,662` calls back into the mutation services) — a cycle that cannot cross a crate boundary. **Decision: core defines an `UpstreamFetcher` trait plus core-owned `ProxyError`/`RepoDecision`/`FetchManifestResult`/`TagMeta`; the reqwest engine stays in the server and implements it.** Recorded fallback: move the proxy-aware read paths to the server — decided explicitly at the 1d checkpoint, not silently.
2. **Core-owned policy types** (`registry_core::policy`): `TagPolicy` (`application/manifest_read.rs:5`), new `GcPolicy` (replaces `Arc<Config>` at `gc_service.rs:80` and the 13 `&Config` signatures in `blob_gc/`), `LegacyMultipartCleanupPolicy` (`storage/s3.rs:740-1075`), an upstream-route type (`storage/mod.rs:943,1043`). Server `Config` maps into them via tested `From` impls. The GC backend branch (`gc_service.rs:201`) becomes a port **capability query** (quarantine support), not a copied `StorageBackend` enum.
3. **Backends move into core unconditionally.** The `storage-fs`/`storage-s3` cargo features are vestigial (they gate zero code in `src/`, `tests/`, Makefile, or Dockerfile); delete them (recommended) or defer real gating to follow-up work — decided in the ADR, not cargo-culted across.
4. **Accepted core deps:** sled (`BlobRefIndex`), tokio, the storage-layer siblings, always-linked AWS SDK. Documented consciously (see risk R8).
5. **Stability:** `registry-core` at 0.x, explicitly unstable until a second consumer exists. Drop the deprecated `manifest_publication` shim during the split.
6. **Non-goals (scope fence, see R6):** no KI-26 handler cleanup (58 `state.config` reads in `http_api/handlers.rs` stay as-is), no `registry-storage-*` crate split, no new feature engineering, no proxy-engine rewrite.

## Phase 1 — In-place decoupling (single crate; the real refactoring)

- **1a. Boundary gate first, as an allowlist covering test code.** Makefile/CI check: files in core-listed modules may import only core-listed modules, *including inside `#[cfg(test)]`*. Its first run freezes the authoritative violation list and **re-baselines this plan's scope** (go/no-go gate G1).
- **1b. KI-07:** move `http_api/upload_state.rs` (verified: zero crate-internal imports) to the core side; update `upload_coordinator.rs:2` and `application/errors.rs`; temporary re-export shim.
- **1c. Policy types**, two parts: (i) production — narrow the 13 `blob_gc` signatures, swap `GcService`'s config field, replace the s3/proxy-route config types, add the capability port method with an equivalence test against the old backend branch; (ii) tests — rewrite the `gc_service` (lines 831-943, including the `security::TokenSigningKey` fixture) and `blob_gc` sidecar fixtures to build `GcPolicy` directly instead of a full server `Config`.
- **1d. Proxy seam:** extract shared types from `proxy.rs`, define `UpstreamFetcher` with exactly the methods used today (manifest fetch incl. not-modified/ETag flow, `now_unix`, `ttl_expires_at`, tag-meta publication), rewrite the 24 application-side reference sites (20 of them in `manifest_read.rs`) against `Arc<dyn UpstreamFetcher>`, implement the trait on `Proxy`. Ends at go/no-go gate G2: confirm the trait or invoke the ADR fallback.
- **1e. Gate green = boundary proven.** 1b–1d each compile and pass `cargo test --lib` independently — three checkpoint commits on master.

Phase 1 is worthwhile even if the split stops here: it closes KI-07 and narrows KI-26's blast radius.

## Phase 2 — Workspace conversion and the move

`git mv` per this complete partition (every `lib.rs` module assigned):

| `registry-core` | `registry-rust` (server) |
|---|---|
| `registry/`, `storage/` (ports, domains, facade, fs, s3, upload_session, repo_membership, mutation_authority), `upload_coordinator`, `manifest_lifecycle`, `manifest_refs`, `repository_membership_ledger`, `membership_migration`, `blob_ref_index`, `blob_delete_safety`, `gc_service`, `blob_gc/`, `consistency`, `fs_root_lock`, `application/`, moved `upload_state`, new `policy` + proxy-seam modules, `test_support` | `http_api/`, `auth`, `rbac`, `security`, `config`, `audit`, `proxy` (engine impl), `supervisor`, `task_supervisor`, `runtime`, `cli/`, `app_state`, `request_routing`, `ip_concurrency`, `token_rate_limit`, `robot_secrets`, `glob`, `main` |
| *dropped:* `manifest_publication`, vestigial storage features | |

Sidecar unit tests move with their modules (compilable thanks to 1c-ii). The 20 black-box integration suites, `tests/compliance`, and `tests/s3_live_integration.rs` stay with the server. Sibling dev-only features (`fault-injection`, `mock-client`) move to core's dev-dependencies (same edition-2024 resolver guarantee as documented in `Cargo.toml` today). Build plumbing (Makefile, Dockerfile, `packaging/`, `vendor/` + `target.offline`) is verified by a dry run **before** the move commit (gate G3). Lands as **one commit**.

## Phase 3 — Curate the public API

Tighten `pub` → `pub(crate)`: public surface = ports (~20 traits) + engines + application services + errors + policy types + prelude; the omnibus `Storage` trait stays a private implementation vehicle. **Acceptance includes the full server integration suite** (tightening can break tests that reach into core; `test_support` remains the sanctioned `#[doc(hidden)]` window). Rustdoc the load-bearing contracts in core itself: seven-step upload finalization (heal-before-pin), five GC protection axes, membership readiness gate, coordinator-per-composition-root. Add a minimal example: a toy registry over core with no auth/config stack — the proof of the crate's purpose.

## Phase 4 — Re-verify and reconcile

Full matrices: fmt/clippy/`cargo test --lib` per crate, stress and grammar suites, all four conformance matrices in `tests/compliance`, live-MinIO qualification rerun (existing R5/R6 procedure). Docs: rewrite `docs/architecture/README.md` with a fresh audited-revision stamp, record ADR-010, update `technical-debt.md` (KI-07 closes, KI-26 narrows, feature-flag disposition recorded), `data-model.md` untouched.

## Sizing

Estimates in focused working sessions, grounded in measured touch points (24 proxy reference sites; 13 `&Config` signatures in `blob_gc`; ~20 port traits; 20 integration suites; ~5,500 lines in application+blob_gc+registry; ~30k lines moving to core).

| Work item | Estimate | Confidence | Basis |
|---|---|---|---|
| Phase 0 (ADR) | 0.5 session | High | Decisions already analyzed; writing only |
| 1a gate | 0.5 session | High | Script + Makefile target; allowlist is a fixed module list |
| 1b KI-07 | hours | High | One file move, two import sites, zero internal deps |
| 1c policy types | 1–2 sessions | Medium-high | 13 signatures + 3 type replacements + fixture rewrites; plumbing, no logic |
| 1d proxy seam | 1–2 sessions | **Medium** | 24 sites, 20 concentrated in one file; trait design is the only creative work |
| Phase 2 move | 1–2 sessions | Medium-high | ~30k lines moved mechanically; effort is build plumbing, not code |
| Phase 3 curation | 2–3 sessions | **Low-medium** | API design iterates; hardest to bound |
| Phase 4 verification | 1 session | High | All procedures exist and are documented (R5/R6) |
| **Total** | **7–11 sessions** | | Critical path: 1d; widest variance: Phase 3 |

Phase 3 can be deliberately thin on the first pass (tighten visibility, minimal docs, defer the example) if the goal is "boundary exists and is enforced" rather than "publishable crate" — trims 1–2 sessions with no architectural cost, since 0.x-unstable is the declared posture anyway.

## Risk register

| # | Risk | P | I | Mitigation / trigger | Residual |
|---|---|---|---|---|---|
| R1 | Proxy trait balloons — `manifest_read`'s 20 sites are interleaved with local read logic and the trait ends up mirroring the engine | Med | High (schedule) | Timebox 1d; **fallback trigger: trait exceeds ~8 methods or must leak reqwest/config types.** Fallback (proxy-aware services move to server) is pre-approved in ADR-010, so the failure mode is a smaller core, not a stall | Low |
| R2 | More hidden couplings — each review pass of this plan found some; assume the pattern continues | Med | Med | Gate 1a (allowlist, test code included) runs **before** any signature work and converts unknowns into a compile-time list; G1 re-baselines scope. Budget +1 session contingency | Low |
| R3 | Behavioral regression in GC while narrowing signatures — GC deletes data, highest blast radius in the codebase | Low | **Very high** | 1c changes plumbing only, never logic; field-by-field `From` tests; capability-query equivalence test against the old backend branch; the existing five-axis protection and adversarial GC suites must stay green at every checkpoint | Low, monitored at every commit |
| R4 | Workspace conversion breaks offline/vendored/packaging builds | Med | Med (blocks release, not correctness) | G3 dry run against `vendor/` before the move commit; Dockerfile/packaging updated in the same commit; Phase 2 is one commit → rollback is a single revert | Low |
| R5 | Phase 3 tightening breaks integration tests or freezes the API too early | Med | Low-med | Server suite in Phase 3 acceptance; 0.x-unstable posture; `test_support` as the only sanctioned internals window | Low |
| R6 | Scope creep — fixing KI-26's 58 handler config-reads, splitting backend crates, or adding real feature gating "while we're here" | **High** | Med (schedule, review quality) | Explicit non-goals in ADR-010 (Phase 0 item 6); the gate makes core clean without touching handler internals | Med — needs discipline, not tooling |
| R7 | Long-lived branch divergence | Low | Med | Every phase lands on master at a consistent state; stopping points after Phase 1 and Phase 2 are first-class outcomes | Low |
| R8 | Forced deps (sled, tokio, AWS SDK) undercut the crate's premise — embedders must take the index and SDK choices | Certain | Low now, med later | Accepted consciously in ADR decision 4; ports keep later extraction possible; revisit only when a second consumer actually exists | Accepted |

**Go/no-go gates:** G1 after 1a (re-baseline scope from the gate's violation list; abort costs half a session). G2 after 1d (confirm trait or invoke fallback). G3 before the Phase 2 commit (offline-build dry run green). Phase 3 acceptance = full server integration suite green.

**Rollback:** Phases 1b–1d are independent commits, individually revertible; Phase 2 is one commit, one revert; stopping after Phase 1 permanently is a legitimate end state (KI-07 closed, boundary enforced, no crate split).

## First executable task

Phase 0 + 1a: the ADR and the allowlist gate — half a day, self-contained; G1 hardens every estimate above before any signature changes.

## Appendix: verified coupling inventory (at `be40792`)

Production imports of server-side types from would-be-core modules:

| Site | Coupling |
|---|---|
| `application/errors.rs:182,214` | `proxy::ProxyError` |
| `application/manifest_read.rs:5,82-187` | `config::TagPolicy`; proxy engine types throughout |
| `application/proxy.rs:7` | `Arc<proxy::Proxy>` |
| `application/errors.rs` + `upload_coordinator.rs:2` | `http_api::upload_state` (KI-07; the module itself is HTTP-free) |
| `gc_service.rs:80,201` | `Arc<config::Config>`; `config::StorageBackend` branch |
| `blob_gc/mod.rs`, `blob_gc/policy.rs` | 13 `&config::Config` production signatures |
| `storage/s3.rs:740-1075` | `config::LegacyMultipartCleanupPolicy` |
| `storage/mod.rs:943,1043` | `config::ProxyUpstreamRoute` |

Test-only (breaks core compilation after split if unfixed): `gc_service.rs:831-943` and `blob_gc/mod.rs:985+` construct the full server `Config` (incl. `security::TokenSigningKey`).

### G1 re-baseline (2026-09-26, gate first run — learnings)

The 1a gate's first run found **100 violation lines across 12 files**, confirming risk R2. Additions beyond the inventory above:

* `test_support.rs:45` → `auth::push_repository_allowed` — **new production coupling.** Fix: the pure allowlist-matching logic moves into `registry::access_pattern` (core); `auth` delegates. (Done in 1b′.)
* `storage/fs/tests.rs` (27 sites, incl. `proxy` ×2 and `supervisor` ×1 around lines 11661-11713) and `storage/s3/tests.rs` (9 sites) — sidecar fixtures build the server `Config`; added to 1c-ii scope. Fixtures that are genuinely end-to-end (the supervisor-using one) relocate to the server side instead of being rewritten.
* `gc_service.rs:110,125,141` — additional `Arc<Config>` constructor parameters (same 1c fix).
* `manifest_lifecycle.rs` "→ proxy" hits were **doc comments only** (`ProxyPublicationEvidence` lives in core; the server constructs it — direction is fine). Gate now skips comment lines.
* Core `#[macro_export]` macros (`impl_storage_ports!`, `impl_gc_storage_port!`) resolve as `crate::<name>` and needed gate allowlisting.

Gate: `scripts/check-core-boundary.sh` / `make core-boundary`.

### Phase 1c learnings (2026-09-26, completed)

* **The capability query already existed**: `GcServiceStoragePort::gc_strategy()` (`FilesystemQuarantine`/`S3DirectConditional`) was already how `blob_gc` branches; only `gc_service.rs:201` duplicated it via `config::StorageBackend`. Replaced + equivalence test added.
* **`blob_gc`'s Config use was `fs_root`-only**, and after the fix several path params turned out dead: `build_manifest_protected_set`/`PolicyContext::build` had an *already-unused* `_cfg` parameter (removed entirely), and `blob_gc_plan`/`blob_gc_quarantine{,_with_authority}`/`blob_gc_delete_s3_with_authority` lost their path params — the core API shrank versus plan.
* **Relocation beat abstraction for the wiring**: no core upstream-route type was needed. The 7 `Config`-consuming factory functions (218 lines) moved from `storage/mod.rs` to a new server-side `src/storage_wiring/` module, together with 8 wiring/composition tests (848 lines) that lived in `storage/fs/tests.rs`. `storage/mod.rs` no longer imports `config` at all.
* **Gate limitation found**: multi-line `use crate::{ config::{…} }` trees evade the line-based regex (storage/mod.rs's import was invisible to the gate). Acceptable interim: the Phase 2 crate split makes the compiler the real gate.
* `GcPolicy` lives in new core `src/policy/` with `TagPolicy` and `LegacyMultipartCleanupPolicy` (config re-exports for compat); `impl From<&Config> for GcPolicy` in config.rs with an exhaustive-destructuring mapping test (R3).
* **Live-S3 suite is environment-gated**: `tests/s3_live_integration.rs` fails without a running MinIO (base-reproduced at the pre-1c commit — not a regression). Live qualification remains a Phase 4 item, as planned.
* Verification at completion: fmt clean; lib 1127/0/13; all non-live integration suites green; gate violations reduced to the 24 proxy-seam lines in `application/` (= exactly the 1d scope).

### Phase 1d/1e learnings and G2 decision (2026-09-26, completed — Phase 1 DONE)

* **G2 verdict: trait CONFIRMED, fallback not invoked.** `UpstreamFetcher` landed with 12 methods — over the ~8 heuristic numerically, but the second fallback trigger (leaking reqwest/config types) never fired: `ProxyError` is message-only, and the publication back-calls take core types (`BlobMutationService`, `ManifestMutationService`, `&dyn ProxyStoragePort`, `&dyn BlobUploadCoordinatorStoragePort`). The trait does not mirror the engine: auth/token caching, singleflight, egress policy, GC/scrub/eviction all stay engine-internal.
* **Gate blind spot #2 found**: `application/blob_read.rs` consumes the engine through `ProxyTarget` *method calls* (no `crate::proxy` path anywhere), so the line-regex never saw it. Real coupling surface was larger than the token count suggested; the trait covers it (`decision_for_repo`, `head_blob_upstream`, `fetch_blob_into_storage`, `note_blob_access`).
* **`RepoDecision` dragged `EvictionPolicy`** — moved to core `policy` alongside `TagPolicy` (config re-exports for compat).
* Moved to new core `src/upstream/`: `ProxyError`, `RepoDecision`, `TagMeta`, `FetchManifestResult`, `now_unix`/`ttl_expires_at` helpers, the trait. `proxy.rs` re-exports the moved types and implements the trait by delegation to its inherent methods.
* **Server workers need the concrete engine**: proxy GC/scrub in `supervisor.rs` call engine methods outside the seam (correctly — eviction is not a core concern). `AppState` gained `proxy_upstream_engines: Vec<Arc<Proxy>>` parallel to the (now trait-object) `proxy_upstreams: Vec<ProxyTarget>`.
* **1e: gate GREEN** (`core-boundary gate: clean`). Verification: fmt clean; lib 1127/0/13; full workspace test run 1490 passed / 31 failed, with all 31 failures isolated to the environment-gated `s3_live_integration` suite (base-reproduced pre-change; Phase 4 item).

### Phase 2 learnings (2026-09-26, completed)

* **Layout adjustment (G3-driven, ADR-010 addendum):** the root package stays `registry-rust` at the repo root; the workspace gains one member, `crates/registry-core`. Moving the server to `crates/registry-rust` would have churned Dockerfile/packaging/conformance paths for zero architectural gain — the release binary still lands at `target/release/registry-rust`.
* **Server `lib.rs` re-exports core modules under their old paths** (`pub use registry_core::storage;` etc., plus the two `#[macro_export]` macros), so `registry_rust::…` paths in all 20 integration suites kept compiling unchanged.
* **Cross-crate test seams solved with a core `test-mocks` feature**: the deterministic S3 mock (extracted from `s3/tests.rs` into `storage/s3/mock.rs`), FS test helpers (`storage/fs/test_helpers.rs`), and two white-box reader accessors are `#[cfg(any(test, feature = "test-mocks"))]`; the server enables the feature via its dev-dependency on registry-core — dev-only, same unification guarantee as the sibling crates' `fault-injection`/`mock-client`.
* **Visibility widenings** required by the server (now `pub`, to be curated in Phase 3): `storage::facade`, `fs::{manifest_listing, tag_listing, repo_discovery, manifest_refs, read_adapter}`, `FsStorage::try_new_with_all_limits`.
* **Operator-visible change**: three `tracing` targets renamed `registry_rust::storage::fs` → `registry_core::storage::fs` (log-filter configs may reference them) — Phase 4 docs note.
* `manifest_publication` shim deleted per ADR-010 §2.6, together with its compatibility test (`test_public_api_manifest_publication_compatibility`).
* **Test invocation changed**: `cargo test` at the root no longer runs core's unit tests — use `cargo test --workspace`. Core standalone: 909 lib tests; workspace totals 1489 passed / 31 env-gated / 14 ignored (−1 vs pre-split = the deleted shim test).
* Gate script repointed: forbidden-crate grep over `crates/registry-core` + `cargo check -p registry-core` — the compiler is now the primary boundary. Its first post-split run caught three stale `registry_rust::…` tracing target strings.
* Verification: gate clean; fmt clean; release build produces `target/release/registry-rust` (G3); conformance matrices fs/basic/token green post-split (s3 + live suite need MinIO → Phase 4).

Reverse edge (server → core, legal after split): `proxy.rs:582,662` → `application::{Blob,Manifest}MutationService`.

Verified clean: `membership_migration` (imports only `manifest_refs` + `storage`), `upload_state.rs` (zero crate imports), `consistency`, `fs_root_lock`, `task_supervisor`, `glob` (used only by `request_routing`/`rbac`/`config` — server side); no core production code touches `auth`, `rbac`, `security`, `audit`, or `request_routing`; storage layer imports nothing above it except the listed `config` sites.
