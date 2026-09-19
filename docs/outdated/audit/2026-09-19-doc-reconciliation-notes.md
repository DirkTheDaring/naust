> **ARCHIVED audit evidence (moved from `docs/audit/` on 2026-09-19).** Cited by the canonical set as the evidence trail; not a status source. The registers it references (known-issues.md, acceptance-gates.md) were merged into [`../../technical-debt.md`](../../technical-debt.md); document paths it names predate the consolidation (see [`../README.md`](../README.md) for the path map).

# Documentation reconciliation audit — working notes

- **Status:** COMPLETE — Phase 4 (approved reconciliation) executed 2026-09-19; changes uncommitted, awaiting final review. See §9 for the execution record.
- **Started:** 2026-09-19
- **Audited code revision (unchanged throughout):** `master` @ `2718bc16`; working tree contained no code changes before or after — only documentation files were modified.
- **Purpose:** Establish an evidence-backed picture of the current system and reconcile documentation around it. No application-code changes. Resumable: a new session continues from these notes.
- **Method:** broad map → bounded per-subsystem verification passes (parallel read-only agents) → reconciliation proposal → STOP for human review before editing existing docs.

---

## 1. Audited revision (Phase 1)

| Item | Value |
|---|---|
| Branch | `master` |
| HEAD | `2718bc16bb3bb5fb68523efc9120de4855c803d0` — `docs(architecture): align living inventory with current master` (2026-09-18) |
| Working tree | clean (no uncommitted/staged changes, no stashes) |
| Crate | `registry-rust` 0.9.0, edition 2024, single crate (no workspace) |
| Path dep: storage-layer-rust | `../storage-layer-rust` @ `74974af5` (main, clean; contains earlier pinned `7ea62160` as ancestor); crates used: storage-core, storage-fs, storage-s3. Dev-only features: `fault-injection` (storage-fs), `mock-client` (storage-s3) — resolver keeps them out of production builds per Cargo.toml comment |
| Path dep: acmecert | `../acmecert` @ `0e985c8` (v0.5.0, clean); crate acmecert-core |

Note: HEAD itself is a documentation-alignment commit; `docs/architecture/current-state.md` records state at its parent `9405991` and is the self-declared living inventory (see `docs/architecture/README.md` reading order).

## 2. Repository map (Phase 1)

- **Entry point:** `src/main.rs` → clap subcommands; server composition `src/runtime.rs` + `src/supervisor.rs`; CLI composition `src/cli/runtime.rs` + `src/cli/policy.rs`.
- **Subsystems (src/):** `http_api/` (transport), `application/` (seven services), `registry/` , `storage/` (FsStorage/S3Storage + contained authorities + ObjectStore domain adapters), `blob_gc/` + `gc_service.rs` + `blob_ref_index.rs` (GC & ref index), `membership_migration.rs` + `repository_membership_ledger.rs` (membership), `upload_coordinator.rs`, `manifest_lifecycle.rs`/`manifest_publication.rs`/`manifest_refs.rs`, auth stack (`auth.rs`, `rbac.rs`, `robot_secrets.rs`, `token_rate_limit.rs`, `security.rs`, `audit.rs`), `proxy.rs` (pull-through cache), `config.rs`, misc (`ip_concurrency.rs`, `fs_root_lock.rs`, `consistency.rs`, `task_supervisor.rs`).
- **Tests:** 21 files under `tests/` + `tests/support/`; inline unit tests extracted to sidecars per ADR-008 (e.g. `src/storage/fs/tests.rs`, `src/storage/s3/tests.rs`).
- **Deployment:** Dockerfile, docker-compose{,.config,.minio,.tls}.yml, Makefile, `dist/`, `packaging/`, `scripts/`, `configs/`, `.github/`.
- **Data/ops leftovers in tree:** `data/`, `conformance-results/`, `certs/`, `tls/`, `target.offline/`, `vendor/`.

## 3. Documentation inventory (Phase 1 — top level)

- `docs/architecture/README.md` — reading-order + supersession key (updated at HEAD). **Candidate canonical index.**
- `docs/architecture/current-state.md` — living current-state inventory @ 9405991, 2026-09-18. **Candidate canonical current-state.**
- `docs/architecture/current-code-assessment.md` — 2026-08-26 baseline + slice addenda + updated §9 debt register (addendum @ 9405991). Historical baseline + living register hybrid.
- `docs/architecture/adr-001..009` — accepted ADRs (layering, services, ports, read services, composition roots, CLI, manifest compat, test topology, error taxonomy).
- ~65 `filesystem-*`/`o-05-*`/membership/supervisor/lifecycle notes — per-slice characterization/design/cutover snapshots, many with stale "NOT COMMITTED"-style stamps (per README these are historical).
- `docs/*.md` (blob-gc, blob-gc-online, rbac, harbor-lite-phase2, traefik-configuration, container-testing-guide) — feature/ops docs, alignment unverified (Phase 2).
- Root: `PLAN.md` (2026-01-02 MVP plan — earliest requirements doc), `README.md` (user-facing, 26 KB), `BUG0.txt`, `ISSUES.txt`, `ideas.txt` (informal trackers; **ideas.txt** carries the TLS/cert FIXMEs — corrected 2026-09-19, earlier note wrongly said ISSUES.txt; ISSUES.txt is about multi-platform image listing).

Known ID hazard (from README): two distinct "D-06" labels; FS acceptance gates O-03/04/05/06/13/15/16 + fs-doc D-06 remain OPEN as *acceptance*, though code work landed.

## 4. Phase 2 verification passes (agents launched 2026-09-19)

| Pass | Scope | Status |
|---|---|---|
| A | Storage subsystem (claims of current-state.md §3/§4) | **complete — findings in §5.1** |
| B | Application/HTTP/runtime layering + ADR 001–009 vs code | **complete — findings in §5.2** |
| C | GC/ref-index/membership vs docs | **complete — findings in §5.6** |
| D | Auth/RBAC/proxy/CLI/config/deployment vs docs | **complete — findings in §5.5** |
| E | Per-document inventory (status stamps, supersession, duplicates) | **complete — findings in §5.3** |
| F | Per-gate investigation of OPEN acceptance gates (O-03/04/05/06/13/15/16, fs-doc D-06) | **complete — findings in §5.8 + proposal §5** |

## 4b. Refactoring timeline reconstructed from git history (194 commits total)

- **Era 1 — MVP & features (2026-01 … 2026-04):** initial build per `PLAN.md`; online GC phases (`71c451d`…), v0.6.x, traefik config, auth fix (`313a8af`).
- **Era 2 — OCI 1.1 compliance & hardening (2026-08-24 … 08-26):** OCI 1.1 support (`5b48992`), long v0.7.x–v0.8.18 compliance-fix series, pre-refactor hardening slices (handler modularization `62b4580`, membership ledger `3f6ace8`, supervisor extraction `ad746e6`). Baseline assessment commit = `efdae2e` (matches `current-code-assessment.md` "Baseline Commit").
- **Era 3a — Wave 1 refactor (2026-08-27 … 09-06):** ADR-driven slices, each commit tagged with its ADR: ADR-001 `7adb489`, ADR-002 `07974ec`, ADR-003 `8b44972`, ADR-004 `bfb1369`, ADR-005 `07daf31`, ADR-006 `72fcf37`, ADR-007 `8662f04`, ADR-008 `04c6588`/`ebfcc9b`/`8731d08`, ADR-009 `49d4054`; v0.9.0 boundary `1680f68`.
- **Era 3b — Wave 2 storage containment / ObjectStore migration (2026-09-08 … 09-18):** StorageWiringFacade seam `f3d8c96`; per-family characterize→validate→cutover triples (metadata, CAS listing, manifest read/list, GC discovery, tag read/list, referrers, catalog, timestamps, membership, journal, quarantine); contained mutation cutover `f555e5f`; durability barriers `8c0ac64`; ObjectStore shared-domain phases 3–8 (`32c42c6`…`84dbe13`); ref-index fixes (`be34b2e`); test-hardening (`00b41d6`, `a080ec7`, live S3 `2e6bb3b`); streaming CAS listing / FsListingBudgets removal (`1772f0a`…`9405991`); doc alignment `2718bc1` (HEAD).

**HYPOTHESIS (UNCONFIRMED — do not treat as a conclusion):** the commit history shows each *slice* completing its own characterize→validate→cutover cadence, which suggests the campaign advanced by finished increments rather than being interrupted mid-slice. That does NOT establish that the overall intended architecture was completed. The audit must separately establish:
1. **Originally intended** end-state (sources: PLAN.md, current-code-assessment.md strategic roadmap, ADRs, gate register in the filesystem notes).
2. **Implemented and connected to production entry points** (passes A–D evidence).
3. **Explicitly deferred or abandoned** (in-code prose deferrals found by pass A; doc-level deferral/abandonment statements; §5.4 gate investigation).
4. **Incomplete or undeterminable** (recorded per item; absence of evidence stated as such).

## 5. Findings

### 5.0 Execution check (verified by execution, 2026-09-19)

`cargo test --lib --locked` at HEAD `2718bc1`: **1125 passed, 0 failed, 13 ignored** (2.8 s). Composition of the 13 ignored tests was not analyzed. Integration test binaries not run by this audit.

Additional execution checks (2026-09-19):
- `git tag`: v0.5.0, v0.6.0, v0.6.1, **v0.9.0** (annotated, created 2026-09-06 19:18, points at `1680f68`). **v0.9.0 IS tagged locally.** ADR-009's "not yet tagged" line (dated 2026-09-02) was accurate when written — the tag came four days later; the 2026-09-18 repetitions in `current-code-assessment.md:483` and `current-state.md:103` were stale when written.
- `git remote -v`: **empty** in both registry-rust and ../storage-layer-rust — no remotes; nothing can have been pushed/published *from these clones*; existence of external hosting is unknowable from local evidence.
- `conformance-results/` (gitignored, local-only): four matrices (basic/fs/s3/token), all `exit_code.txt` = 0, `result.yaml` spec v1.1.1 (fs: 75/80 passed, rest skipped-or-not-run per derived summary), files dated **2026-08-26 14:29–14:32** — i.e. baseline-assessment era, ~90 commits before HEAD. **No conformance run artifact exists for any post-refactor revision locally; CI runs it but artifacts are ephemeral uploads.** Current conformance at HEAD: UNVERIFIED.

### 5.8 Pass F — acceptance-gate register (complete 2026-09-19)

Full per-gate detail is in the reconciliation proposal §5 (canonical location). Key meta-findings:

- **Criterion origin unresolved:** no doc defines the O-numbering; IDs O-01/02/07–12/14 never occur anywhere (repo + sibling). Earliest in-repo appearance: `o-05-filesystem-metadata-containment.md` (commit `f7ad9f9`, 2026-09-08, six gates "retain their existing meanings") and `5f504ac` (adds O-04/O-15). Fullest in-repo definitional tables: `filesystem-read-containment-remaining-gaps.md:576-585`, `filesystem-production-read-cutover.md:234-242`, `post-tag-listing-assessment.md:484-493`, `tag-read-production-readiness-assessment.md:605-614`.
- **Hard ID collision:** `filesystem-gc-contained-discovery-production-cutover.md:14-24` redefines ALL EIGHT gate IDs to unrelated subjects; `filesystem-tag-read-contained-seam.md:25,101,258` redefines O-03. Highest-priority doc defect.
- No doc anywhere declares any gate closed; D-06(fs) has never been given partial credit.
- Closure types: O-13, D-06(fs) = human decision; O-06, O-15, O-16 = verification run + doc act; O-03, O-05 = doc act + decision; O-04 = code change (repo lease + 2 meta/ writes) or accepted exception. **No gate is "just stale docs".**
- Premise corrections: live S3 integration suite is NOT ignore-gated (only the AccessDenied test, `s3_live_integration.rs:2326`); it runs in CI against pinned MinIO (`ci.yml:30-39`, image pinned in `scripts/start-minio.sh:4`; but `docker-compose.minio.yml` uses `:latest` — unpinned).
- Evidence-artifact gap pattern: `dist/` and `conformance-results/` are gitignored — the missing "evidence" for O-06/O-16/D-06 exists transiently in CI but has no committed form.
- Pending TAG-DEC-01…06 (tag-read readiness doc) and D7 (metadata assessment) sign-offs: cutovers shipped while these remain PENDING/DEFERRED on paper — input to D-06(fs).

### 5.1 Pass A — storage subsystem (observed in source; complete 2026-09-19)

Verdict: **current-state.md §3/§4 is accurate** at HEAD, with two wording nuances.

Confirmed (evidence file:line in pass A report, key anchors here):
- `FsStorage` fields as documented plus retained `gc_discovery_limits`/`gc_ref_limits`, `root`, `repo_locks`; phase 3–8 domain fields literally labeled (`src/storage/fs.rs:415-487`).
- Pathname `meta/` writes: `mark_membership_ready` (`fs.rs:3650-3692`, `tokio::fs::create_dir_all` + `atomic_write_file`) and `save_migration_checkpoint` (`fs.rs:3701-3717` — uses `ensure_dir` (`storage/mod.rs:1070`), **not** `create_dir_all` as the doc says; substance identical: ambient pathname writes).
- Repo lease pathname flock `repos/<repo>/.repo_lock` (`fs.rs:1341-1373`); note: `renew` is a no-op `Ok(true)` (`fs.rs:1375-1383`) — worth documenting.
- Tag-listing repo-existence probe contained via `FsMetadataReader`, explicitly deferred "until that family migrates in a later phase" (`src/storage/fs/tag_listing.rs:13-17`, `fs.rs:669-671`, `tag_domain.rs:112`).
- No `list_tag_files` / `FsListingBudgets` symbols in src/ or tests/ (one historical comment `src/storage/fs/tests.rs:15561`); only historical docs mention them.
- Reaper `reap_expired_sessions` (`fs.rs:3296+`) fully contained: `upload_authorities` streams + `try_lock`/`run_locked`; zero `tokio::fs::read_dir` in `fs.rs`. Production caller `UploadCoordinator::reap_expired_uploads` via `UploadSessionStorage` port.
- CAS listing: `stream_dir` + bounded `BinaryHeap` top-K, O(limit) memory, page limit clamped [1,1000] (`src/storage/fs/listing.rs:311-477`).
- Membership enumeration on per-backend `FsMetadataReader` seam (`fs.rs:3605-3698`, `membership_read.rs`); rationale: `ObjectStore::list_page` has no common-prefix rows (`membership_domain.rs:22-30`).
- `compute_fs_blob_version` is `#[cfg(test)]`-only ambient reference impl (`fs.rs:3890+`).
- Six shared ObjectStore domains (tags/manifests/referrers/membership-point/journal/repo-timestamps) backend-neutral over `storage_core::ObjectStore` with FS (`FsObjectStore`, per-family instance) and S3 (`S3ObjectStore`, shared `OnceCell`) adapters.

**Nuance 1 (doc phrasing risk):** "production wiring uses port bounds, not `dyn Storage`" is literally true (all `dyn Storage`/`use ...::Storage` sites are test-only), BUT the omnibus `Storage` trait is still the production *implementation vehicle*: `impl_storage_ports!`/`impl_gc_storage_port!` macros expand port impls that call `crate::storage::Storage::<method>` and are applied unconditionally to both backends (`src/storage/ports/mod.rs:386-714`). Docs should say: no production consumer holds `Storage`; ports delegate to it internally.
**Nuance 2:** deferred work is recorded in code as prose comments, not TODO/FIXME (zero TODO/FIXME/todo!/unimplemented! in production storage code). Deferral list: repo-existence family, membership tree enumeration, `meta/` readiness+checkpoint writes, `fs::write_membership_sync`, reaper inspection-read containment follow-up (`upload_quarantine_read.rs:14,330`).

Storage-relevant tests mapped (names + coverage captured in pass A report; notably `tests/upload_lifecycle_contained_cleanup_prototype.rs` is an explicitly TEST-ONLY prototype — a real "proposed but not productionized" artifact).

### 5.2 Pass B — application/HTTP/runtime layering (observed in source; complete 2026-09-19)

Verdict: **current-state.md §1/§2 accurate on every checked claim.** ADRs 002–005/008 show recorded drift (inventory below). New findings not in any doc:

- **Dead code:** `AppState.auth_metrics`/`AuthMetrics` constructed (`runtime.rs:623,695`) but `inc_token_issued/denied/internal_error` have zero call sites in src/ or tests/. Candidate cleanup decision for humans.
- **Inverted module coupling:** `src/application/errors.rs:1` imports `crate::http_api::upload_state::StateTokenError`. `upload_state.rs` itself is HTTP-free (HMAC/serde only), so no HTTP types leak, but the application→http_api module-path dependency points the wrong way per ADR-002's direction rule.
- **ConsistencyCoordinator is per-composition-root, not process-global:** server builds one (`runtime.rs:577`); CLI mints one per operation (`cli/runtime.rs:265,314,363`); `task_supervisor.rs:680` another. Docs should state scope precisely.

Confirmed with anchors:
- `src/application/` HTTP-free (zero axum/StatusCode/HeaderMap/http:: matches); 7 services + errors + ProxyTarget modules.
- Exactly seven services on `AppState` (`app_state.rs:66-72`), built only in `assemble_application_services` (`runtime.rs:93-157`).
- `StorageWiring::from_backend<S>` bounded by 8 port traits, stores 15 port views (`ports/mod.rs:1047-1067`); no `dyn Storage`.
- `AppState` residual bag confirmed (config, auth_metrics, ref_index, gc_service+gc_run_seq, proxy trio, 3 semaphores, counters, ip_limiter, is_high_pressure); no `storage` field; only narrow `proxy_cache: Arc<dyn ProxyStoragePort>`.
- `ServerRuntime` = 3 fields (`runtime.rs:161-165`); ADR-005 §2.1 sketch already disclaimed by its own implementation note; but ADR-005 §2.3 `RuntimeBuildError` sketch (10 variants, String payloads) vs code (12 variants, structured, no `Config`) is NOT disclaimed.
- Startup chain: main → cli::run_cli → CommandPolicy::ExclusiveMutation → supervisor::run_server_supervisor (TLS gen, FsRootLock, build_server_runtime, build_router `supervisor.rs:206-307`, TaskSupervisor + reaper/GC/proxy workers, shutdown flush→authority release). `supervisor.rs` has zero FsStorage/S3Storage references (ADR-005 §6 holds).
- `handlers.rs` = 1538-line dispatcher; 59 `state.config` policy reads; inline auth on upload paths + auth middleware; all OCI route families delegate to the seven services; admin GC (`http_api/admin.rs`) and token mint (`http_api/auth_token.rs`) bypass services and read AppState/config directly (matches current-state.md residual-coupling bullet).
- Route surface: /v2 tree via `OciRoute::parse` (`http_api/routing.rs:47`; includes non-standard `TagDelete` `/v2/<name>/tags/reference/<tag>` and `_oci/ext/discover`), /_meta/{catalog,orgs,orgs/:org/repos,repos/*}, /token, /_admin/gc/{health,plan,quarantine,delete} gated by `config.admin_api.enabled`. NB `src/request_routing.rs` is host-based proxy routing + client-IP resolution, not the URL router.
- ADR-007 exact: `manifest_publication.rs` 10-line deprecated shim, zero production callers.

ADR↔code drift inventory (for the Phase 3 ADR table; record-only, no supersession implied):
| ADR | Drift |
|---|---|
| ADR-002 §2 | Method sets exist; code adds ADR-004-named proxy methods coexisting with ADR-002 names (`publish_verified_proxy_blob` + `publish_proxy_blob`, etc.) |
| ADR-002 §3.5 | "direct storage access in handlers" superseded by ADR-004; not annotated |
| ADR-003 §3.1/§3.2 | Six AppState reader fields removed by ADR-004; ADR-003 text not annotated |
| ADR-003 §2.3 | StorageWiring sketch 13 fields vs 15 (missing proxy_storage, readiness_inspector) |
| ADR-004 §3.1 | Field names `blob_mutation_service`/`manifest_mutation_service` vs code `blob_service`/`manifest_service`; residual accessors (`membership_ledger()`, `delete_service()`, `from_lifecycle()`, raw reader-port getters) beyond ADR |
| ADR-005 §2.1 | Sketch disclaimed in-file (OK); §2.3 RuntimeBuildError drift NOT disclaimed |
| ADR-008 §2.2 | handlers/tests.rs 1525 lines vs documented 1495 (drift, cosmetic) |

Tests covering the layer mapped in pass B report (application_service_tests, application_read_tests, ports_wiring_tests incl. two compile-isolation tests proving consumers build without omnibus `Storage`, oci_1_1/conformance black-box suites, supervisor_and_command_tests 42 composition-root contracts, handlers sidecar).

### 5.3 Pass E — full documentation inventory (complete 2026-09-19)

Count: **76** files in docs/architecture/ (all git-tracked) + 6 in docs/. Structure by group:

- **G0 canonical:** README.md (index/key), current-state.md (living inventory @9405991), current-code-assessment.md (2026-08-26 baseline @efdae2e + 10 slice addenda + 2026-09-18 addendum; hybrid baseline/register).
- **G1 ADRs 001–009:** all `Status: Accepted`. ADR-005 has an in-file implementation note disclaiming its §2.1 sketch (the model to copy); ADR-009's Status line itself records "0.9.0 not yet tagged/published" (= O-13). None declared superseded.
- **G2–G9 slice chains** (CAS listing, manifest read, manifest listing, tag read, tag listing [longest, +3 caller-hardening docs], tag mutation [chain incomplete: characterization+design only, landed via f555e5f/32c42c6], referrers, GC discovery [10 files]): characterization → seam/assessment → design/decisions → readiness → cutover per family. Nearly all carry authoring-time stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY — NOT AUTHORIZED", "blocked") that are false at HEAD.
- **G10:** 7 post-* read-containment assessments + remaining-gaps — ALL already carry corrective "Historical snapshot" banners. Redundant by construction (each restates the R-1..R-18 table with 1–2 rows changed).
- **G11 implementation records:** production-read-cutover, catalog/timestamps/membership/journal containment records — still stamped "working tree, not committed" (stale); journal doc additionally claims "journal writes remain ambient" (falsified by f5f9bf7/8c0ac64). quarantine-upload-inspection doc has banner but §0 body still reads "DEFERRED".
- **G12 upload lifecycle pair:** -design.md (bannered historical) + cleanup.md (rehabilitated, Status IMPLEMENTED, aligned @9405991 — canonical behavior record).
- **G13 O-05 metadata docs:** dated 2026-09-08, gates OPEN, accurate as characterization-era records.
- **docs/ top level:** blob-gc.md & blob-gc-online.md (NO status/date markers; pre-containment framing; not referenced by architecture README), rbac.md (inline "Implemented" comments, phases 1–4 roadmap partially stamped), harbor-lite-phase2.md ("implemented"), traefik/container-testing (ops guides, no claims to go stale).

**Un-bannered contradictions of README's "do not treat as open work" list (priority fixes):**
- `FsListingBudgets` specified as live contract: `filesystem-cas-listing-production-integration-design.md` (no banner).
- `list_tag_files` as live symbol, no banner: tag-listing characterization / contained-integration-design / contained-seam / production-readiness-assessment ("promotion is blocked") / production-cutover-readiness.
- Cutover records with zero provenance (no status, date, or commit): `filesystem-manifest-read-production-cutover.md`, `filesystem-tag-read-production-cutover.md`, `lifecycle-tag-listing-error-hardening.md` — and all three describe call paths since moved to ObjectStore domains.

**Self-corrected already (banner model exists):** cas-listing-production-cutover, tag-listing-production-cutover, supervisor-tag-listing-error-hardening, referrers-read-contained-integration-design (addendum), all G10 files, G12 pair.

**Near-duplicate/consolidation candidates:** (1) tag-listing production-readiness-assessment vs production-cutover-readiness (same promotion, two baselines); (2) membership-migration hardening design vs error-hardening (same baseline 96c0729); (3) supervisor-tag-listing assessment vs error-hardening (same baseline 5779f7f); (4) gc-manifest-discovery-integration-design vs gc-contained-discovery-production-integration-design ("Corrected"); (5) manifest-listing production-decisions vs production-readiness-assessment; (6) cas-listing-integration-assessment vs gc-contained-metadata-design (same gap); (7) the 7 post-* assessments among themselves. Verified NOT duplicates: gc-manifest-reference-seam-design vs -seam (planned vs executed tests); upload-lifecycle -design vs record.

**Canonical candidates:** README.md, current-state.md, ADRs 001–009, current-code-assessment.md (baseline+§9 register), filesystem-upload-lifecycle-contained-cleanup.md, supervisor-tag-listing-error-hardening.md, rbac.md + harbor-lite-phase2.md, blob-gc-online.md (with blob-gc.md as predecessor), traefik/container-testing guides.

### 5.5 Pass D — auth/RBAC/proxy/CLI/config/deployment (complete 2026-09-19; observed in source unless noted)

**Auth model (confirmed):** single middleware gate `require_auth_middleware` (`auth.rs:279`); anonymous pull default-on (`config.rs:1478-1485`) unless repo "private" — which is a hardcoded name heuristic (`config.rs:2833-2848`), not config; Basic auth (robots → users → legacy push creds, robots shadow users); `/token` mints HMAC-SHA256 JWT-shaped tokens (`security.rs:323-361`), key ring `token_signing_keys`, verifier accepts 2- and 3-part legacy-compat forms. Robots + users/groups (harbor-lite phase 2) both exist (`config.rs:299-332`); Argon2id secrets.

**rbac.md vs code:** core invariants HOLD (deny-by-default, granted⊆requested⊆policy, prefix boundary, no arbitrary wildcards — `rbac.rs:200-252`, covered by unit + truth-table tests). Mismatches: (1) **Phase 4 observability events (`token_issued`/`token_denied`/`token_error`) DO NOT EXIST** — zero tracing calls in auth_token.rs; correlates with dead `AuthMetrics` (§5.2). Same stale claim in PLAN.md. (2) `*` grant silently confers catalog scope (`rbac.rs:212-220`), undocumented. (3) harbor-lite-phase2.md wrong that `repo_prefix` must end in `/` (exact names and bare `*` accepted, `rbac.rs:39-57`); (4) its "exact precedence flag" doesn't exist — robots-first hardcoded. (5) Token rate limit: global fixed window, env-only (`supervisor.rs:238-248`), OUTSIDE Config — invisible to check-config; **no tests** for token_rate_limit.rs.

**Proxy (pull-through cache):** implementation matches README broadly; two omissions: cache eviction/scrub is **FS-only** (`supervisor.rs:533-536,575-578` log-and-return for S3 — unbounded S3 cache growth, `max_cache_bytes` inert), and `max_cache_bytes` is hard-required when proxy enabled (`config.rs:2631-2634`).

**CLI:** 15 subcommands, `CommandPolicy` six-variant gate enforced via `MaintenanceRuntime::acquire`; S3 destructive GC needs `--confirm-all-writers-stopped`. **Inconsistency:** parallel `CommandIntent` classification (`cli/mod.rs:225-291`, unused by execute_cli) classifies `migrate-membership verify` as `ReadOnly` while `CommandPolicy` says `ExclusiveInspection`.

**Config:** TOML surface `FileConfig` (`config.rs:537-569`); unknown keys → hard error in strict/best_practice via serde_ignored; explicit legacy: `s3_legacy_multipart_cleanup_policy`, single `token.signing_key`, `push_username/password`, env aliases. All 8 `configs/*.toml` examples schema-valid. **BUT packaged `etc/registry-rust/registry.auth.toml` does NOT parse cleanly:** `[auth.push] mode = "token_only"` is an unknown key (hard failure under strict); references nonexistent `challenge_mode`; wrong `hash-secret` usage in comments. **Packaged `registry.core.toml` ships developer-specific values** (environment-specific hostnames, ACME against a private endpoint with an authorization string, `debug=true`, placeholder signing keys — paths only; values not reproduced here) and auth.toml ships a live push-`*` user account with a real Argon2id hash. Security/ops-relevant human decision.

**Deployment:** compose files consistent with env names; Dockerfile stages only `vendor/acmecert` — **does not stage `../storage-layer-rust` path deps** (container build likely broken unless those crates reachable some other way; UNRESOLVED — not build-tested by this audit). Two divergent systemd units (RPM has LimitNOFILE/ReadWritePaths; DEB lacks them — README:87 claim true only for RPM). `dist/` contains checked-in... correction: `dist/` is **gitignored** (local artifacts only: .debs 0.6.1 + 0.9.0). CI: `.github/workflows/ci.yml` **defines** build+test+smokes+MinIO live S3+podman smoke+OCI conformance jobs, but **with no git remote configured there is no local evidence the workflow has ever executed** (see §5.0 execution checks); no clippy/fmt/audit/release jobs defined. `certs/` and `tls/` (incl. `old*/`) contain PEM private-key files — paths recorded, values not reproduced, liveness not verified.

**README stale claims (12 findings, key ones):** `PUSH_AUTH_MODE`/`auth.push.mode`/`deny_if_no_basic`/`basic_or_token` DO NOT EXIST in code (real knob: `auth.strategy`, absent from README table); `anonymous_pull` undocumented; "GET /v2/ may 401" false (V2Ping unconditionally passed, `auth.rs:301-307`); "blob-gc FS-only" false (S3 supported); 3 CLI subcommands omitted; empty "Environment variables" section (heading at :221, content at :563); catalog-auth semantics understated; `[limits]`/`[timeouts]` security knobs undocumented; config table covers ~half of FileConfig.

**ideas.txt TLS FIXMEs CONFIRMED in code:** no SAN/altname validation on startup (cert generated/fallback-used without parsing SANs; name-set change doesn't trigger regen — renewal is expiry-only in acmecert, sibling-repo evidence); NO cert reload thread (single `RustlsConfig::from_pem_file` at `supervisor.rs:384-392`, zero reload calls — long-running server never picks up renewed cert; `renewal_window_secs` only effective at process start). Configured-names startup logging partial (`supervisor.rs:72-80`). No tests for ACME/TLS path or fs_root_lock.rs.

### 5.6 Pass C — GC/ref-index/membership (complete 2026-09-19; observed in source unless noted)

**Architecture (confirmed):** one GC engine (`src/blob_gc/`), three entry points (CLI offline w/ FsRootLock+authority; `/_admin/gc/*` gated by `admin_api.enabled`; background scheduler `spawn_blob_gc_scheduler` gated by `blob_gc_schedule_enabled`). FS = two-phase quarantine+delayed delete; S3 = direct conditional ETag delete, fail-closed on bucket versioning. Reachability via sled `BlobRefIndex` (7 trees, SCHEMA_VERSION=2, states ready/building/dirty). **Five** protection axes enforced (pin, membership count>0, policy reachability, active WAL journal, min-age) — docs describe only three. Separate membership sweep (Active→Candidate→Unlink) runs first in scheduled cleanup; no CLI/admin route for it. `be34b2e` fixes both verified in source + covered by tests (heal-before-pin-gate at `upload_coordinator.rs:609-621,793-802`; `rebuild_gate` serialization/coalescing + tests). Contained GC discovery IS the only production FS path (`policy.rs:169-228` fail-closed; old `build_manifest_protected_set_fs` deleted — grep zero). Membership migration = one-shot operator backfill with fail-closed boot gate (`runtime.rs:346-393`; auto-ready only for empty storage), resumable checkpoint w/ 60s lease, phases Applying/Verifying/Ready/Failed; covered end-to-end by `repository_membership_tests.rs:603+`.

**docs/blob-gc.md + blob-gc-online.md = STALE DESIGN DOCS** (pre-refactor). Specific falsified claims: FS-only scope (S3 online GC works); `consistency_gate` symbol gone (now ConsistencyCoordinator guards); 6-step upload machine now 7 steps with heal step undocumented; "CLI as thin client of admin API" never adopted (CLI opens sled directly; safety via FsRootLock+authority instead); pin TTL doc says 72h `finalize_grace`, actual `gc_pin_duration_secs` default **1h**; "refuse cross-device quarantine" (EXDEV) NOT implemented; background scheduler + membership sweep undocumented; "dry-run by default/--delete flag" model never built.

**Code findings (new, not in any doc):**
1. **`blob_gc_finalize_grace_secs` (default 72h) is parsed+validated but NEVER read by production logic** — dead config knob; real pin TTL is 1h. 72× intent-vs-behavior gap for slow pushes (blob-gc-online.md rationale: pushes up to ~36h).
2. **CLI force-overrides kill switches**: `MaintenanceRuntime` sets `blob_gc_enabled=true; blob_gc_enable_delete=true` (`cli/runtime.rs:261-262,310-312,359-361`) — `blob_gc.enabled=false` does not stop offline destructive GC. Untested either way.
3. `GcService::plan` not kill-switch gated (read-only; expectation issue).
4. `blob_gc_sweep` (`blob_gc/mod.rs:766`) has no production caller (unit-test only).
5. Lock-order comments name `quarantine/.lock`; code creates `quarantine/gc.lock` (`gc_service.rs:205,295,427`).
6. Stale test comments in `blob_gc/policy.rs:314-387` describe deleted branch logic; `_cfg` param vestigial.
7. Legacy-but-reachable: `LedgerIndexMode::StorageOnly` (constructor choice, prod always Indexed), `find_blob_reference` `#[allow(dead_code)]`, legacy single-key pin compat branch.
8. `filesystem-reference-index-sync-hardening-design.md` §5.3 deferred-work list is now fully implemented (repo_roots contribution accounting, DiscoveryLimits, contained promotion) and its "SCHEMA_VERSION = 1" claim is stale (code = 2).

### 5.7 Original-intent inventory (sources of intended requirements/architecture)

| Source | What it declares | Notes for traceability |
|---|---|---|
| `PLAN.md` (2026-01-02) | Product requirements: OCI/Docker v2 API surface, anonymous pull + authenticated push, FS default + S3 optional storage, media types, error format, RBAC/robots, hardening defaults, GC concept, acceptance checks | Earliest requirement source; several items marked "Implemented" inline (RBAC, harbor-lite phase 2) |
| root `README.md` | User-facing feature claims | Verified in pass D |
| `current-code-assessment.md` §10 (Target Architecture) | Intended Wave-1 end-state: delivery → application service facade → domain/coordination → storage ports → adapters; **§10.1 names four facade services: `BlobService`, `ManifestService`, `GarbageCollectionService`, `ProxyService`** | Implemented: blob/manifest mutation + 5 read/query services (7 total, different decomposition than sketched). **NOT implemented as sketched: no `GarbageCollectionService` or `ProxyService` application facade** — `GcService` remains a domain engine reached directly by admin handlers (`http_api/admin.rs`), proxy remains `Option<Arc<Proxy>>` on AppState with only `ProxyTarget` neutralized. §10.2 "zero wire/layout/state-machine changes" = explicit non-goals honored. Confirmed vs pass B evidence |
| `current-code-assessment.md` §11 (Roadmap) | 6 planned slices (trait segregation, app services, gate encapsulation, test/mock decoupling, ghost-module cleanup, typed errors); planned module name `src/services/` | Executed as 11 slices under ADRs 001–009 in different order and naming (`src/application/`); all 6 themes landed per §9 register (D-01/D-02 "Mostly resolved" with named residuals, D-03..D-06 "Resolved") |
| `current-code-assessment.md` §13 (Open Questions for Maintainer Input) | Q1 keep omnibus `Storage` composite vs force sub-traits; Q2 multi-instance ref-index strategy; Q3 test file organization | **No recorded answers found in-doc.** De facto outcomes: Q1 — omnibus kept as implementation vehicle behind ports (pass A nuance 1) — no ADR records this as a decision; Q3 — ADR-008 chose file-backed sidecars (answers it in substance); Q2 — unresolved (assessment §12.1 retains single-writer sled; no later decision doc found yet). Q1/Q2 go to "decisions requiring human input" |
| `current-code-assessment.md` §12 (Non-Goals) | Retain sled ref-index, S3 ETag conditional deletes, in-process ConsistencyCoordinator | Intentionally retained trade-offs — must not be mistaken for unfinished work |
| ADRs 001–009 | Accepted decisions per slice | Conformance matrix from pass B (§5.2) |
| Wave-2 filesystem notes (gate register, per-family designs, "later phase" markers) | Intended containment/ObjectStore end-state + acceptance gates | Gate criteria under investigation in pass F; explicit deferrals (repo-existence family, membership enumeration, `meta/` writes, reaper inspection reads) recorded in code prose (pass A) |
| `docs/blob-gc.md` / `blob-gc-online.md`, `docs/rbac.md` phases | GC design intent; RBAC phase roadmap (phases 1–4) | Alignment checked in passes C/D |

## 6. Coverage / not inspected

- Sibling repos not audited beyond HEAD/cleanliness (`storage-layer-rust` @74974af, `acmecert` @0e985c8). Any claim depending on their internals (e.g. `openat2` flags inside `storage_fs::FsMetadataReader`, `ObjectStore` contract semantics) is **qualified: relies on unaudited sibling sources** and on this repo's call sites only.
- **Execution evidence so far:** exactly one execution artifact — `cargo test --lib --locked` at HEAD (§5.0, 1125/0/13). This is *unit-test execution*, not integration-binary execution and not a run of the deployed application. No server process was started, no container/compose deployment exercised, no live S3 endpoint contacted by this audit. All other claims are "observed in source" or "covered by a test (not run by this audit)".
- `vendor/`, `target*/`, `data/`, `conformance-results/` contents not inspected (build/runtime artifacts).

## 7. Unresolved questions for the human

(to be filled — consolidated in the Phase 3 proposal)

## 8. Next steps

1. DONE: all six passes complete and integrated (§5.0–§5.8).
2. DONE: reconciliation proposal finalized and approved (options A–E, recommended defaults).
3. DONE: Phase 4 executed — see §9.
4. Remaining: human review of the uncommitted diff; the non-blocking decisions listed in the proposal §9 and known-issues.md; gate closures per acceptance-gates.md.

## 9. Phase 4 execution record (2026-09-19)

Approved scope executed; **no application code, test, or config file changed** (verified: `git status` shows only `.md`/`.txt` files; `ideas.txt` is gitignored so its annotation is local-only).

- **New canonical documents (5):** `docs/README.md` (entry point), `docs/requirements.md` (REQ-001…023), `docs/architecture/acceptance-gates.md` (GATE-O03…GATE-FSD06 + carried TAG-DEC-01…06 and D7; authority recorded UNRESOLVED; original wording vs inferred interpretation vs PROPOSED closure criteria kept apart), `docs/known-issues.md` (KI-01…21; paths of sensitive files recorded without values), `docs/gc-operations.md` (current GC behavior).
- **Banners:** 46 B-HIST/B-PROV/B-GATE banners on architecture snapshot docs (guarded, idempotent); 4 pointer notes on keep-files (production-read-cutover, remaining-gaps, o-05-metadata, post-tag-listing); inline §0 pointer in quarantine-upload doc; historical banners on blob-gc.md, blob-gc-online.md, PLAN.md; migration notes on BUG0.txt, ISSUES.txt, ideas.txt; review stamps on traefik/container-testing guides. The two gate-ID-collision docs carry explicit GATE WARNING banners; their original tables are preserved.
- **Canonical corrections:** current-state.md (6 dated precision fixes incl. v0.9.0 tag correction, omnibus-vehicle nuance, KI-07 edge, per-root coordinator, lease no-op, ensure_dir); architecture/README.md (register links, 3 new "not open work" items, gate-hazard section); current-code-assessment.md (dated correction: tag + Q1/Q2 unanswered); dated reconciliation addenda appended to ADR-001…006 and ADR-009 (originals untouched; ADR-007/008 unchanged).
- **Root README:** all 12 confirmed defects fixed (PUSH_AUTH_MODE family removed; `auth.strategy` default `token` + `anonymous_pull` documented — values verified in `src/config.rs:1458-1485`; /v2/ ping-401 claim corrected; blob-gc S3 support + flags; migrate-membership/inspect-lock/admin-clear-lock added; empty env-vars heading removed and orphan list retitled; catalog-auth semantics; limits/timeouts protection table (env names verified `src/config.rs:2160-2228`); S3 cache-eviction + max_cache_bytes notes; RPM/DEB systemd divergence; TOKEN_SIGNING_KEY keyring caveat; docs entry-point link).
- **rbac.md / harbor-lite-phase2.md:** dated status notes (Phase 4 observability NOT implemented → REQ-006/KI-04; `*`-grant catalog scope → KI-18; token-log caveat) and two dated corrections (no trailing-`/` enforcement; no precedence flag) — original text preserved in all cases.
- **Validation performed:** (a) all relative links in the 76 changed tracked files resolve (pre-existing `file://` URIs in ADR-008/009 left as-is — they resolve locally, not introduced by this work); (b) every KI-/REQ-/GATE- reference in changed files resolves to its register (21 KI, 23 REQ, 8 GATE); (c) sensitive-value scan of changed files: only pre-existing placeholder hashes and one pre-existing hostname line in BUG0.txt; nothing added; (d) keep-files verified byte-unchanged (10 spot-checked incl. ADR-007/008, self-corrected docs, post-* assessments); (e) banner idempotence guards in place.
- **Not done (by design):** no gate closed; no retrospective ADR created (remain proposals in the reconciliation proposal §3); no code/packaging/release change; nothing committed.

## 10. Final validation pass (2026-09-19, post-execution)

**Coverage:** every git-visible changed/added file (78 = 71 tracked modified + 7 untracked new: the five canonical docs plus these notes and the proposal) plus the gitignored-but-annotated `ideas.txt`. Nothing is staged.

Results:
1. **Inventory reconciled:** 0 staged, 71 unstaged tracked modifications, 7 untracked additions; `ideas.txt` ignored (annotation local-only, content preserved in tracked registers). Earlier "77" count = 76 status lines + the `docs/audit/` collapse; the per-file count is 78. Non-md/txt changes: none — documentation-only confirmed (`git diff --numstat`: 328 insertions / 21 deletions across tracked files; deletions confined to the four intentionally rewritten files: README.md 10, architecture/README.md 1, current-state.md 8, rbac.md 2; all other changes are pure prepends/appends).
2. **Links (anchor-aware, GitHub-style slugs):** zero introduced failures; zero pre-existing relative-link failures in the changed files. Pre-existing absolute `file://` URIs in ADR-008/009 were out of scope (machine-local, untouched).
3. **Registers:** 21 KI, 23 REQ, 8 GATE definitions; zero duplicates; every KI/REQ/GATE mention across all changed/added files resolves to a definition row (KI/REQ) or `###` heading (GATE). Carried obligations present: TAG-DEC ×10 mentions and D7 ×3 in acceptance-gates.md; REQ-006/012/013/014 rows exist; KI-15/16 hold the tracker migrations.
4. **Diff/content review:** no gate/requirement/issue STATUS asserted outside the registers in canonical docs (current-state §5 retains historical prose under an explicit "register wins" pointer; proposal tables are marked frozen snapshots). Sensitive scan of added lines + full new-file contents: one developer hostname found in these notes and replaced with a path-only description; remaining matches are pre-existing lines (placeholder hashes in rbac/harbor-lite examples, BUG0.txt's original command). No key material, credentials, or token values copied anywhere.

**Limitations:** anchor validation used GitHub-style slugification (approximation); `file://` URIs and content of unchanged documents were not validated; no execution beyond the checks recorded in §5.0; sibling repositories remain unaudited; register content correctness rests on the pass A–F evidence, not on re-verification in this pass.
