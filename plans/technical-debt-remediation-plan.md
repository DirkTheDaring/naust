# Technical-debt remediation — architecture solution and plan

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
