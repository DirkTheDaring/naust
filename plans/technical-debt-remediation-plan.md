# Technical-debt remediation — architecture solution and plan

> **Naming note (2026-09-26, ADR-012):** after this plan closed, the product was renamed `registry-rust`→`naust` and `registry-core`→`naust-core`. Names and paths below are historical.

- **Status:** proposed (not started)
- **Date:** 2026-09-26 (rev 2 — corrected after senior-architect review, see "Review provenance" below)
- **Baseline:** `master` @ `c049d7d` (ADR-010 split complete); debt register = [`../docs/technical-debt.md`](../docs/technical-debt.md) (KI-01…KI-27, GATE-O03…O16, FSD06)
- **Companion:** [`registry-core-extraction-plan.md`](registry-core-extraction-plan.md) (closed) — this plan reuses its proven mechanics: policy snapshots, capability ports, explicit go/no-go gates, per-phase test acceptance, learnings appended in place.
- **Review provenance:** rev 1 was reviewed against the code and vendored deps; four corrections are integrated: (1) A2 now owns a runtime ACME renewal loop — no renewal event exists to hook (startup-only `generate_pem_dir`; verified `supervisor.rs`); (2) A3/R2 gained an explicit `ProxyStoragePort` enumeration extension — the port has no CAS-listing capability today (verified `ports/mod.rs:290-299`) — plus the access-time input seam; (3) KI-04/REQ-006 added to A4/D3 (was orphaned in R3); (4) A1's service census now records the `ProxyService` disposition explicitly. Verified clean in the same review: `RustlsConfig::reload_from_pem_file` exists (axum-server 0.8.0, `tls_rustls/mod.rs:287`); `blob_gc_sweep` has zero external callers; `_storage` at `proxy.rs:614` is unused; Dockerfile staging paths are consistent for both crates.

**Scope statement.** This plan addresses every *open* register line: each item is either (a) fixed by a designed change, (b) resolved by a recorded decision, or (c) explicitly re-parked with an owner-approved criterion. It does not silently drop anything.

---

## Part 1 — Architecture solution

Seven design decisions, one per debt cluster. The unifying principle is the one ADR-010 validated: **behavior is parameterized by narrow policy/capability types derived once at composition time — never by reaching into `Config` or backend identity at the point of use.**

### A1. Server context decomposition (KI-26, enables KI-27a later)

`AppState` stops being a process bag and becomes a thin composition of cohesive contexts, each built once in `runtime.rs`:

```
AppState
├── ApplicationServices            (exists — core services, unchanged)
├── AuthContext                    { strategy, key ring, RBAC policy, token TTL, rate-limit knobs }
├── GcContext                      { Arc<GcService>, gc_run_seq, admin policy snapshot }
├── ProxyRuntime                   { engines: Vec<Arc<Proxy>>, targets: Vec<ProxyTarget>, routing }
└── RequestLimits                  { semaphores, IP limiter, body/timeout policy snapshots }
```

- **Policy snapshots, not `Config`:** each handler family gets a small server-side struct (`BlobHandlerPolicy`, `ManifestHandlerPolicy`, `TokenPolicy`, …) derived from `Config` via tested `From` impls — the exact `GcPolicy` pattern from ADR-010 Phase 1c. The ~58 `state.config.*` reads in `handlers.rs` become field reads on the snapshot owned by the family.
- **Handler split:** `http_api/handlers.rs` (1538 lines) splits by resource family (`blobs.rs`, `uploads.rs`, `manifests.rs`, `tags_refs.rs` …), each a parse/delegate/format module against one service + one snapshot. Routing (`routing.rs`) is already separate and stays.
- **Bypass closure:** two new *server-side* services complete the facade story: `GcAdminService` (wraps `GcService` + `gc_run_seq` + admin policy; `/_admin/gc/*` handlers delegate to it) and `TokenService` (mint/introspect against `AuthContext`; `/token` handler delegates). They live in the server crate — auth and admin surface are not registry primitives, so core is untouched.
- **Service census ADR** closes the KI-26 sub-item "re-scoping to 7 services never recorded" by recording the final census — 7 core application services + 2 server services — **and explicitly recording the `ProxyService` disposition: the assessment-§10.1 `ProxyService` facade is deliberately not built as such; `upstream::UpstreamFetcher` + `application::ProxyTarget` (delivered by ADR-010) are its replacement.** Without this sentence the "never recorded" defect would survive its own remediation.

### A2. TLS lifecycle manager (KI-01) — *corrected: owns renewal, not just reload*

**Fact base:** ACME provisioning (`acmecert_core::simple::generate_pem_dir`) runs **only at startup**; `renewal_window_secs` merely tells that one-shot call to renew when inside the window. There is no runtime renewal loop, so there is no "renewal event" to subscribe to — a pure reload-watcher would never fire for ACME-managed certs.

A supervisor-owned `TlsManager` task therefore has two responsibilities:

- **Renewal loop (ACME-managed certs):** periodically re-invoke `generate_pem_dir` (config interval, default ~12 h; the call is already window-idempotent — it renews only when inside `renewal_window`). On successful renewal, proceed to reload.
- **Hot reload (both cert sources):** keep the existing `axum_server::tls_rustls::RustlsConfig` (shared handle; `reload_from_pem_file` swaps certs without rebinding — verified present in axum-server 0.8.0). For externally managed certs (`tls_cert_path` deployments where an external agent renews), an mtime poll (default ~5 min) triggers the reload.
- **SAN preflight:** parse the certificate at load/reload (new parse-only dependency `x509-parser`) and compare SANs to `acme.names`/configured hostnames. Startup: fail closed on mismatch (break-glass escape hatch `tls.allow_san_mismatch=true`). Renewal/reload: refuse the swap + log loudly, keep serving the old cert — never degrade a running server on bad input.
- Log actual SANs + `notAfter` at every load — makes the archived rollout playbook followable again.

### A3. Backend-neutral, contained proxy-cache eviction (KI-02) — *corrected: port extension + access-time seam*

Today `proxy_gc_once` ambiently walks `fs_root/blobs/sha256` with `read_dir` — FS-only *and* an uncontained filesystem walk. The fix removes both defects, with two explicitly named work items the port surface does not provide today:

- **Port extension (new work item):** `ProxyStoragePort` currently exposes no CAS enumeration (its supertraits are `BlobUploadCoordinatorStoragePort + BlobIndexStoragePort + ReferrersReader` — verified). Add a cache-enumeration capability (either a `GcStoragePort`-style listing supertrait or an `as_cas_enumerator()` view) to `ProxyStoragePort`. Both backends already implement the underlying streaming listing for GC, so this is surface plumbing, not new listing logic.
- **Access-time seam (new work item):** eviction ranks candidates by last-access, which lives in the **proxy engine's sled index** (server-side), not in storage. The core planner therefore takes fully materialized candidate rows — `{digest, size, last_modified, last_access: Option<u64>}` — assembled by the server worker (enumeration via the extended port + access lookup via the engine). Core never touches sled or the engine.
- **Planner in core:** a `cache_eviction` module computing the deletion set from candidate rows + protected set + `policy::EvictionPolicy` (already core since ADR-010) + `max_cache_bytes`. Execution (deletes) goes back through the port: FS unlink or S3 conditional delete. The supervisor worker shrinks to: enumerate → look up access times → build protected set → run planner → apply deletions → report.
- Scrub (`proxy_scrub_once`) follows the same pattern in a second step; if S3 scrub is not wanted, that residual is *recorded* with a rationale instead of a `warn!` and silent return.

### A4. Config and knob hygiene (KI-03, KI-04, KI-05, KI-09, KI-17, KI-18, KI-06)

One rule: **every knob is either effective, or deleted; every hardcoded behavior is either a documented config default, or removed.** Concretely (recommendations, each confirmed at gate D3):

- KI-03 `blob_gc_finalize_grace_secs`: **wire it** — it is the natural source for the finalize-time pin extension (`gc_pin_duration_secs` stays as the general pin knob). If the 72×-gap intent is dead, delete the knob instead; either way REQ-012 gets resolved.
- KI-04 / REQ-006 token observability: **adopt REQ-006 (rec)** — emit the `token_denied`/`token_granted` tracing events in `auth_token.rs`, wire the existing `AuthMetrics` increments, assert both in a test; alternative: record REQ-006 as dropped and delete `AuthMetrics`. Either way the dead counters go.
- KI-05 CLI kill-switch override: **CLI respects `blob_gc.enabled=false`** and fails with a clear message; a new explicit `--force-gc` flag reproduces today's behavior. Covered by tests both ways.
- KI-09 token rate limit: move env knobs into `Config` (strict-validation visible), keep env as documented override; add unit tests to `token_rate_limit.rs`.
- KI-17 private-name heuristic / KI-18 `*`-grant catalog scope: **promote both to explicit config** (`auth.private_name_patterns` with today's list as default; `rbac.star_grants_catalog` default `true`), documented in operations.md — behavior preserved, surprise removed.
- KI-06: delete the unused `CommandIntent` classification; `CommandPolicy` is the single source (ADR-006 addendum).

### A5. Packaging and supply chain (KI-10, KI-11, KI-21, KI-15/16)

- KI-10 container build: the Dockerfile must stage **all** path deps, not just acmecert. Solution: extend the existing `vendor/` convention — `vendor/storage-layer-rust/crates/{storage-core,storage-fs,storage-s3}` staged to `/storage-layer-rust` the same way acmecert is staged to `/acmecert` (paths verified consistent for both the root crate's `../storage-layer-rust` and core's `../../../storage-layer-rust`), refreshed by a `make vendor-sync` target; build verified in CI (`podman build`). (Alternative recorded: parent-dir build context — rejected: breaks `COPY . .` isolation.)
- KI-11 secrets/config hygiene: packaged `etc/` configs become **neutral templates** (placeholders, no live hostnames/ACME strings, `debug=false`, remove the unknown `[auth.push] mode` key so strict validation passes); `tls/`, `tls/old*`, `certs/` PEM key material leaves the tree (gitignored `local/` dev fixtures + README note). History scrub is explicitly out of scope (no remotes exist yet — do this *before* first push, see A6).
- KI-21: align DEB unit to the RPM unit's `LimitNOFILE`/`ReadWritePaths`/`ReadOnlyPaths` (keep `ProtectSystem=strict` in both).
- KI-15/16 (multi-arch visibility question, `imagetools` note): fold into the GATE-O13 release decision as documented notes; they are not code debt.

### A6. Evidence, CI, and release (KI-19, KI-20, GATE-O13, GATE-O06/O16)

- **Evidence convention:** a committed `evidence/` directory (or the existing archive convention) holding per-revision conformance + live-MinIO run reports (JUnit/HTML digests, not gitignored) — closes KI-20's criterion mechanically after every qualification run, starting with a re-archive of the Phase-4 ADR-010 runs.
- **CI:** make `.github/workflows/ci.yml` real: fmt --all --check → `make core-boundary` → `cargo test --workspace --locked` → conformance fs/basic/token (s3 + live-MinIO as a service-container job). Precondition: **GATE-O13 hosting decision** (remote, visibility, release channel) — a user decision, sequenced before the KI-11 tree scrub lands anywhere public.
- GATE-O06/O16 evidence staleness closes as a side effect of the evidence convention.

### A7. Containment residues — record or finish (KI-12, KI-22; KI-23/24/25 re-parked)

The ObjectStore campaign closed at its natural boundary (register history, verdict B). Policy here:

- KI-12/KI-22 repo lease: the *missing rationale* is the defect. Decision D7 picks one: (i) implement TTL-enforcing `renew` over the existing flock, or (ii) record the no-op as accepted design (single-writer authority already serializes mutation; the per-repo lease is belt-and-braces). Recommendation: **(ii) record**, because `RuntimeMutationAuthority` is the real exclusivity mechanism and a second lease TTL adds failure modes without adding safety.
- KI-23/24/25 stay deferred **with their existing criteria confirmed by the owner** (this plan's D7 sign-off makes the parking explicit rather than implicit).
- KI-27a (core visibility curation) stays parked on its recorded criterion: revisit at the second core consumer.

---

## Part 2 — Decision docket

**Resolved 2026-09-26 (execution authorized by owner; recommendations adopted):** D1 = renewal loop + reload + SAN preflight; D2 = implement S3 eviction per A3; D3 = A4 batch incl. **adopt REQ-006**; D4 = `vendor/` staging; D5 = key material out of tree pre-push; **D6 = DEFERRED — hosting/remote/visibility/release channel is an owner-only choice; R6 executes its local parts (evidence convention, CI definition) and leaves KI-19/GATE-O13 open**; D7 = record repo-lease no-op as accepted design; KI-23/24/25 parking confirmed.

| # | Decision | Options (recommendation first) |
|---|---|---|
| D1 | TLS scope | Renewal loop + reload + SAN preflight per A2 **(rec)** · reload only (externally-renewed certs; ACME stays startup-only, documented) · accept KI-01 as documented limitation |
| D2 | S3 cache eviction | Implement per A3 **(rec)** · formally accept FS-only (document + config warning on s3+max_cache_bytes) |
| D3 | Knob semantics batch | A4 recommendations as listed, **including adopt-vs-drop REQ-006 (KI-04)** **(rec: adopt)** · per-knob overrides |
| D4 | Vendoring strategy | `vendor/` staging for storage-layer **(rec)** · parent build context · publish siblings to a registry |
| D5 | Key-material disposition | Move out of tree pre-push **(rec)** · accept as dev fixtures (recorded) |
| D6 | GATE-O13 hosting/release | Needs owner: remote + visibility + release channel; blocks CI + publication, and sequences the KI-11 scrub |
| D7 | Repo-lease semantics + residue parking | Record no-op as accepted design **(rec)** · implement TTL renew; plus sign-off on KI-23/24/25 criteria |

## Part 3 — Phased plan

Phases are independent where possible; each ends with: full `cargo test --workspace --locked`, fmt, `make core-boundary`, conformance fs/basic/token (plus s3/live where the phase touches storage or proxy), register update closing its KI lines, learnings appended here.

### R0 — Quick wins (no decisions needed; ~0.5–1 session)
KI-08 comment fix · KI-13 remnant (drop the unused `_storage` parameter from `UpstreamFetcher::fetch_manifest_and_cache`, the `Proxy` inherent method, and call sites — **before** anyone builds against the trait) · KI-14 (delete dead `blob_gc_sweep`, zero external callers verified) · KI-27b/c (operations.md log-target note; README/CI `cargo test --workspace` note) · KI-06 (`CommandIntent` removal + ADR-006 addendum) · A7 rationale recording for the repo lease if D7 = record. Closes ~6 register lines.

### R1 — TLS lifecycle (after D1; **2–3 sessions** — grew from rev 1: it now owns the renewal loop, not just a reload hook)
`TlsManager` per corrected A2: renewal loop (ACME) + reload path + mtime trigger (external certs) + SAN preflight + `x509-parser` dep + tests. Acceptance = the KI-01 criterion verbatim: renewed cert served without restart (proven by a repeated-reload soak test and a renewal-loop test against a short window); mismatch fails closed at startup, refuses swap at runtime.

### R2 — Contained, backend-neutral cache eviction (after D2; 2–3 sessions)
Corrected A3, in two commits mirroring the ADR-010 discipline: **(1) enumerate-only** — `ProxyStoragePort` enumeration extension + candidate-row assembly (port listing × engine access-times) + core planner, verified against the old walker's candidate set on FS (parity test), zero deletions; **(2) delete** — planner output applied via the port (FS unlink / S3 conditional delete) + live-MinIO bounded-cache acceptance test. Scrub follow-up or recorded residual. Deletion-class change: protected-set tests green at every step.

### R3 — Config & policy hygiene (after D3; 1–2 sessions)
A4 batch: each knob wired-or-removed with a test; KI-17/18 promoted to config; token rate limit into `Config`; KI-04 per the D3 REQ-006 decision (emit + test the token events, or delete `AuthMetrics`); REQ-006/REQ-012 acceptance columns resolved. Closes KI-03/04/05/09/17/18.

### R4 — Server decomposition (KI-26; 3–5 sessions; independently stageable, do last)
A1 in three commits: (1) policy snapshots + handler family split (mechanical, behavior-frozen by the black-box suites), (2) `GcAdminService` + `TokenService` (bypass closure), (3) `AppState` context regrouping + service-census ADR **including the recorded `ProxyService` disposition**. Conformance all-matrices green after each commit. KI-26 then closes or shrinks to explicitly-accepted residue (omnibus-`Storage`-as-vehicle stays a recorded fact, per assessment Q1).

### R5 — Packaging & supply chain (after D4/D5; 1–2 sessions)
`make vendor-sync` + Dockerfile staging + verified container build · neutral config templates + key-material relocation · DEB/RPM unit alignment. Closes KI-10/11/21; KI-15/16 folded into the D6 record.

### R6 — Evidence, CI, release (after D6; 1–2 sessions)
Evidence convention + re-archive of the ADR-010 Phase-4 runs (closes KI-20, GATE-O06/O16 evidence rows) · CI workflow live on the chosen host (closes KI-19) · GATE-O13 record. **Sequencing rule: R5's scrub lands before anything is pushed.**

### Sizing and order

| Phase | Effort | Value | Prereq |
|---|---|---|---|
| R0 | 0.5–1 session | 6 register lines | none |
| R1 | 2–3 | prod-outage class fix (incl. runtime renewal) | D1 |
| R2 | 2–3 | unbounded-growth fix + containment | D2 |
| R3 | 1–2 | operator-surprise removal | D3 |
| R4 | 3–5 | architecture completion | none (do last) |
| R5 | 1–2 | shippable artifacts | D4, D5 |
| R6 | 1–2 | evidence + CI | D6, after R5 |
| **Total** | **11–17 sessions** | | |

Recommended order: **R0 now; then R1 → R2 (the risk-reduction arc); R3; R5 → R6 once D6 is decided; R4 last** (largest, least urgent, fully protected by the black-box suites whenever it runs). Every phase leaves master consistent; stopping after any phase is a valid end state.

## Part 4 — Risk register

| # | Risk | P | I | Mitigation |
|---|---|---|---|---|
| S1 | R2 touches deletion (cache eviction) — data-loss class | Low | Very high | Two-commit structure (enumerate-only with FS parity test first, delete second); plumbing/logic separation; protected-set tests; live-MinIO acceptance |
| S2 | R4 behavior drift while splitting handlers | Med | High | Behavior frozen by conformance + 20 black-box suites after every commit; snapshots are `From`-tested like `GcPolicy` |
| S3 | TLS renewal/reload introduces a serving gap or panic path | Low | High | Reload refuses bad input and keeps old cert; startup-only fail-closed; repeated-reload soak test; renewal loop failure = log + retry, never exit |
| S4 | Decision drift — D-items answered ad hoc mid-phase | Med | Med | Docket answered before its phase starts; answers recorded in this file + register |
| S5 | Secret handling (KI-11) — accidental publication | Low | Very high | Hard sequencing rule R5-before-any-push; pre-push checklist includes a tree scan for PEM keys |
| S6 | Scope creep into core API changes | Med | Med | Core seam changes are named and bounded: R0's trait-param removal, R2's port extension + `cache_eviction` planner. Everything else is server/composition side; core-boundary gate in every phase's acceptance |

---

## Execution log

### R0 learnings (2026-09-26, completed, closes 6 register lines)

* KI-08, KI-13 (both `_storage` seam removal — trait+impl+7 call sites — and the stale branch comments), KI-14, KI-06 (+ ADR-006 addendum), KI-27b/c (operations.md §6 + README), KI-12 recorded per D7 (rationale doc-comment at `renew_repo_lease` + register closure inheriting into GATE-O04's lease row).
* Learning (CORRECTED during R1): `blob_gc_sweep` had no production callers, but its removal also deleted `test_blob_gc_plan_and_sweep_s3`, which bundled valuable plan/pin assertions on the S3 mock. The R1 verification's test-count reconciliation (1493 ≠ expected 1494) caught the loss; the coverage was restored as `test_blob_gc_plan_and_delete_s3` (plan + delete legs, no sweep). Lesson: when deleting a dead entry point, split its tests, don't delete them. Removing `_storage` also surfaced and removed a second dead thread: `ensure_tag_fresh`'s unused `cache` parameter.
* Acceptance: fmt/gate clean; workspace 1488 passed (−1 = deleted CommandIntent matrix test) / 31 env-gated / 14 ignored; conformance fs/basic/token green.

### R1 learnings (2026-09-26, completed — KI-01 CLOSED)

* Delivered per corrected A2: `src/tls_manager.rs` (inspect/SAN-coverage/preflight + a testable `tick` and a `TlsWatcher` for `spawn_loop`), `try_acme_renewal` extracted from the startup-only provisioning (non-exiting; `Config` errors stay fatal at startup with the original exit codes, provisioning errors keep the existing-cert fallback), supervisor wiring (fail-closed ACME SAN preflight; log-only inspection for external certs; supervised `tls_manager` task at `renew_check_interval_secs`/`tls_reload_poll_secs`).
* New knobs: `server.tls.acme.renew_check_interval_secs` (43200), `server.tls.acme.allow_san_mismatch` (false), `server.tls.reload_poll_secs` (300) — TOML + env + README table + operations.md §3 rewritten.
* Deps added: `x509-parser` (runtime, parse-only), `rcgen` (dev).
* **Criterion proven end-to-end**: `tests/tls_reload_tests.rs` binds ONCE, then verifies via real rustls handshakes that five successive renewals are served without restart and that a SAN-mismatched renewal is refused (old cert keeps serving). Unit matrix covers wildcard/multi-label/case SAN rules, fingerprint dedup, renewal-failure survival, garbage-PEM refusal.
* Behavioral note: renewal now also happens per-tick at runtime; a *renewal* failure is log-and-retry (never exits), preserving the startup contract exactly.
* Acceptance: fmt/gate clean; workspace 1494 passed (+5 unit, +1 e2e, +1 restored S3 GC test, −1 sweep-bundled test) / 31 env-gated / 14 ignored; conformance fs/basic/token green.

### R2 learnings (2026-09-26, completed — KI-02 CLOSED)

* **The premise was worse than the register said**: the historical `proxy_gc_once` never deleted anything on ANY backend — it counted candidates and logged that reclamation belongs to `BlobGcService`, which never runs over cache roots. `max_cache_bytes` was inert everywhere. R2 therefore *implemented* enforcement, not merely ported it to S3; the plan's "FS parity" applies to the enumeration candidate set only, and the budget semantics are new (total-bytes budget, LRU with never-accessed first, protected content never evicted, residual warned).
* **Strictness contract surfaced by the parity test**: the contained streaming listing FAILS CLOSED on any malformed cache entry (root files, non-hex prefixes, non-digest leaves) where the old walker silently skipped junk. Accepted and test-pinned as intentional: the cache tree is process-owned; corruption should wedge eviction loudly, and the recovery is deleting the cache dir.
* Delivered: permit-free `CacheEvictionPort` (+ blanket, + `impl_cache_eviction_port!` for test doubles, + minimal impl for the supervisor's injected double); FS contained unlink with optional version validation; S3 ETag-conditional delete; core `cache_eviction` LRU planner; `proxy_gc_once` rewritten onto port+planner with per-blob conditional deletes (NotFound/PreconditionFailed = concurrently refreshed content is left alone); FS-only spawn guards removed; scrub S3 residual recorded at the guard.
* Two-commit S1 discipline held: enumerate-only leg (4ee6522) landed with parity + planner tests before any delete code ran.
* Acceptance: fmt/gate clean; workspace green (non-live); conformance fs/basic/token + s3 green; **live-MinIO 33/0/1 ×2** including the new port acceptance test (real ETag conditional refusal + bounding); MinIO restored to stopped.

### R3 learnings (2026-09-26, completed — KI-03/04/05/09/17/18 CLOSED; REQ-006 adopted, REQ-012 wired)

* KI-03/REQ-012: wired as an auto-expiring `finalize-grace` pin (dedicated pin id, placed at STEP 7 for `Published` outcomes only — cross-mounts skip it since membership already protects them). One historical test asserted "unpinned after publish"; that assertion now holds only with grace=0 — updated, plus a dedicated grace test.
* KI-04/REQ-006 adopted: `token_issued`/`token_denied`(reason)/`token_error` events + wired `AuthMetrics` (accessors added; endpoint test asserts both counters).
* KI-05: CLI quarantine/delete now respect the kill switches; `--force-gc` restores the old behavior. Learning: `blob_gc.enabled` defaults to **false**, so the shared CLI-test fixture needed explicit enablement — proof the old behavior silently bypassed the operator default.
* KI-09: rate-limit knobs in `Config` (`[token]`), env kept as override; limiter unit-tested (its API is `retry_after()`, not an acquire bool).
* KI-17: `auth.private_name_prefixes` (default = historical list; empty list disables). Deliberate scope cut: the angle-bracket/percent-encoding checks stay hardcoded — they are injection detection, not naming policy.
* KI-18: `auth.star_grants_catalog` (default true) via a new `grant_scopes_by_prefix_with_options`; the old signature delegates with the historical behavior, so RBAC tests stay valid.
* Register/requirements/operations/README all reconciled in the same commit.
* Acceptance: fmt/gate clean; workspace 1511 passed / env-gated live suite only; conformance fs/basic/token green.

### R5 learnings (2026-09-26, completed — KI-10/11/21 CLOSED)

* **KI-11 was misdescribed in two directions**: (worse) packaging read config from the **untracked, environment-specific** local `etc/` tree — package contents were machine-dependent, not merely sensitive; (better) `tls/`, `certs/`, `etc/` were already gitignored/untracked, so there was no git-history exposure to scrub. Fix: neutral tracked templates in `packaging/config/` (placeholders, ACME off, `debug=false`, no unknown keys; **validated with `check-config`**), Makefile/DEB staging repointed.
* KI-21's row was partially stale too (DEB already had `ReadWritePaths`); actual gaps were `LimitNOFILE`, `ReadOnlyPaths`, `StateDirectoryMode` — added.
* KI-10: `make vendor-sync` + Dockerfile staging for `/storage-layer-rust` (workspace manifest included — the crates use `workspace = true` field inheritance). **Container build verified end-to-end: `podman build` exit 0, binary executes in the image** (first verified container build in the register's history).
* Vendored storage-layer sources are committed under `vendor/`, matching the existing acmecert convention (self-contained container builds).

### R6 learnings (2026-09-26, completed for its LOCAL scope — KI-20 CLOSED; KI-19/GATE-O13 remain owner-blocked per D6)

* Evidence convention: committed `evidence/` directory (convention in `evidence/README.md`); first record `evidence/2026-09-26-debt-remediation/RECORD.md` + JUnit/exit artifacts for all four conformance matrices — closes KI-20's criterion at stated revisions.
* CI definition rewritten to mirror the real pipeline (fmt → `make core-boundary` → `cargo test --workspace --locked` → conformance fs/basic/token; separate live-S3 job with a MinIO **bitnami** service container — the official image needs a `server` command GH services cannot pass). Vendored path-dep staging included. Honest caveat kept in the file: never executed on a hosted runner (no remote).
* D6 remains the only open decision: hosting/remote/visibility/release channel. The R5 scrub concern proved moot for git history (key material was never tracked); the pre-push checklist still applies to any future remote (`git ls-files | grep -iE '\\.pem|secret'` should stay empty — verified empty today).

### R4 learnings (2026-09-26, completed — KI-26 CLOSED with recorded residues; PLAN COMPLETE except D6-blocked KI-19/GATE-O13)

* Commit 1 (`5863691`): dispatcher split into `blobs.rs`/`manifests.rs`/`uploads.rs` with re-exports (sidecar tests untouched); `HttpTransferPolicy` snapshot removed the mechanical config reads. Commit 2 (`17a6dc6`): `GcAdminService` (owns run-id sequence — `gc_run_seq` left `AppState`) + `TokenService` (decision/allowlist/signing/observability); both bypass endpoints are parse/delegate/format now. Commit 3: ADR-011 records the census (7 core + 2 server), the `GarbageCollectionService`→`GcService`+`GcAdminService` and `ProxyService`→`UpstreamFetcher`+`ProxyTarget` re-scopings, and four accepted residues (config-parameterized token decision fns; auth-boundary reads in handlers; flat `AppState`; omnibus-`Storage`-as-vehicle).
* Judgment call recorded rather than executed: `AppState` context regrouping (A1's nested-struct sketch) — with the config-read and bypass defects gone it is naming churn without a seam change; ADR-011 §4.3 records it as accepted architecture, satisfying KI-26's criterion verbatim.
* Behavior frozen throughout by the black-box suites: workspace 1514 passed / env-gated live only; conformance fs/basic/token green after every commit; boundary gate clean; zero warnings.

## FINAL STATUS (2026-09-26)

**Closed this campaign:** KI-01, KI-02, KI-03, KI-04, KI-05, KI-06, KI-08, KI-09, KI-10, KI-11, KI-12 (accepted design), KI-13, KI-14, KI-17, KI-18, KI-20, KI-21, KI-26 (recorded residues), KI-27b/c — plus REQ-006 adopted and REQ-012 wired.
**Remaining open:** KI-19 + GATE-O13 (owner hosting decision, D6), KI-27a (parked until a second core consumer), KI-15/16 (folded into the D6 record), KI-22–25 (containment residues re-parked with owner-confirmed criteria per D7), and the historical GATE rows whose closure requires human acceptance acts.

### Launch addendum (2026-09-26 — D6 executed, KI-19 CLOSED)

D6 resolved by the owner: public GitHub under `DirkTheDaring`, MIT, product renamed **Naust** (ADR-012), gate authority + all GATE rows closed (ADR-013). Published all three repos; first hosted CI run green end-to-end (run 36238807454; record in `evidence/2026-09-26-naust-launch/`). Launch-hardening learnings: ext4 inode reuse broke four swap fixtures (inode-keeper fix); MinIO official images no longer pull anonymously (bitnamilegacy archive, digest-pinned); live suite restored to opt-in `--ignored` gating. **Remaining open across the whole register: KI-27a (second core consumer) and KI-22…25 (re-parked containment residues) only.**
