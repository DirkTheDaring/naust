# Technical debt — canonical register

- **Role:** the single canonical register of remaining work: implementation gaps, architectural debt, missing verification/acceptance evidence, unresolved requirements and decisions, and operational/packaging concerns. Other documents link here by ID; status lives only here.
- **Audited code revision:** `master` @ `2718bc16` (2026-09-19 documentation audit; evidence trail: [`outdated/audit/`](outdated/audit/2026-09-19-doc-reconciliation-notes.md)).
- **ID continuity:** `KI-01…KI-21` and `GATE-…` IDs are unchanged from the prior `known-issues.md` / `architecture/acceptance-gates.md` registers (now merged here). New items added by this consolidation continue the KI series (KI-22…KI-26). `REQ-…` acceptance status lives in [`requirements.md`](requirements.md); ADRs live in [`adr/`](adr/README.md).
- **Semantics:** entries document current state and evidence; nothing here is an approved priority, owner, or decision. "Completion criterion" rows marked *PROPOSED* are this register's suggestion, not an accepted definition of done. Fixes to code, packaging, or release are separate unapproved decisions.

Contents: §1 acceptance gates · §2 carried open decisions · §3 implementation gaps & defects · §4 architectural debt · §5 missing verification/acceptance evidence · §6 unresolved requirements & design decisions · §7 operational & packaging concerns.

---

## 1. Acceptance gates (GATE-O03…GATE-FSD06)

**Authority: UNRESOLVED.** No in-repo document defines these gates or who may close them. The numbering (only O-03/04/05/06/13/15/16 and one "D-06" ever occur; O-01/02/07–12/14 occur nowhere in this repo or `../storage-layer-rust`) is inherited from an out-of-tree review process; the earliest in-repo docs assert the gates "retain their existing meanings" without stating them (`outdated/architecture/o-05-filesystem-metadata-containment.md`, commit `f7ad9f9`, 2026-09-08; O-04/O-15 added at `5f504ac`). **No gate is closed, and this register closes none.**

| Tracking ID | Historical ID | Status | Closure requires |
|---|---|---|---|
| GATE-O03 | O-03 | OPEN | documentation act + human decision |
| GATE-O04 | O-04 | OPEN | code change **or** accepted exception + decision record |
| GATE-O05 | O-05 | OPEN | verification/doc act + human acceptance |
| GATE-O06 | O-06 | OPEN | verification run + documentation act |
| GATE-O13 | O-13 | OPEN | human decision first (least-advanced gate) |
| GATE-O15 | O-15 | OPEN | non-Linux verification run **or** Linux-only ADR |
| GATE-O16 | O-16 | OPEN | verification run + documentation act (cheapest) |
| GATE-FSD06 | fs-doc "D-06" | OPEN | human acceptance, per clause (master gate) |

**ID-collision warning (historical evidence, preserved):** `outdated/architecture/filesystem-gc-contained-discovery-production-cutover.md` (lines ~14–24 of the original) redefines **all eight** IDs to unrelated subjects; `outdated/architecture/filesystem-tag-read-contained-seam.md` redefines O-03. Both carry corrective banners — never read gate meanings from those tables. "D-06" also collides with the **assessment** debt item D-06 (stringly `StorageError` — Resolved by ADR-009); GATE-FSD06 refers only to the filesystem-doc acceptance gate.

Each gate below keeps three tiers apart: **Original wording** (verbatim, fullest in-repo source — the originating definition is out of tree and unrecovered), **Inferred interpretation** (the 2026-09-19 audit's reading, not authoritative), and **Proposed closure criteria** (PROPOSED only).

### GATE-O03 — key and continuation-token contracts

- **Original wording:** "Exact validated key and continuation-token contract." (`outdated/architecture/filesystem-production-read-cutover.md:235`); "Broader key validation and cross-backend token standardization remain pending." (`outdated/architecture/filesystem-cas-listing-production-cutover.md:156`). *Criterion source unresolved beyond these quotes.*
- **Inferred interpretation:** the storage layer's object-key grammar and listing continuation semantics must be an explicit, validated, accepted contract.
- **Implementation evidence [confirmed]:** a written contract exists in the sibling crate — `ObjectKey::parse` grammar, `PageToken` strictly-after semantics, `list_page` ordering/exactly-once rules *(sibling: `storage-core/src/object_store.rs:87-97,414-441`, `key.rs:34`)*; registry-side streaming cursor `src/storage/fs/listing.rs:311-477`. Cross-backend token standardization is **deliberately withheld** by a sibling design note — never accepted registry-side.
- **Validation evidence:** adapter contract suites (FS/S3/in-memory) *(sibling tests)*; in-repo `tests/storage_limits_adversarial_tests.rs`, `tests/canonical_repo_grammar_tests.rs`. **Missing:** a registry-side acceptance record; a decision on the withheld standardization.
- **Proposed closure criteria (PROPOSED):** an ADR ratifying the `ObjectKey`/`PageToken` contract as the registry contract, plus an explicit accept-or-overturn decision on cross-backend token standardization.

### GATE-O04 — filesystem write durability and containment

- **Original wording:** "Filesystem write durability, descriptor-relative containment, symlink safety, and crash outcomes." (`…filesystem-production-read-cutover.md:236`); "Write durability, synchronization barriers, and directory lock containment remain open." (`…filesystem-read-containment-remaining-gaps.md:579`).
- **Inferred interpretation:** every production filesystem *write* path must be descriptor-contained with explicit durability/crash behavior.
- **Implementation evidence [confirmed]:** contained mutation cutover (`f555e5f`) + durability barriers (`8c0ac64`). Residues: pathname `meta/` writes (`src/storage/fs.rs:3651`, `:3705`), pathname repo-lease flock with no-op `renew` (`fs.rs:1341-1383`, → KI-12), and `8c0ac64`'s explicitly chosen best-effort crash persistence for deletions (commit message; no accepting document). See KI-22.
- **Validation evidence:** fault-injected barrier tests (`src/storage/fs/tests.rs:17256+`, dev-only `fault-injection` feature). **Missing:** containment/crash tests for the residues; an accepted record of the deletion-durability carve-out.
- **Proposed closure criteria (PROPOSED):** migrate the residual surfaces onto contained authorities **or** record them as accepted permanent exceptions; separately record acceptance of the deletion-durability carve-out.

### GATE-O05 — broader filesystem read containment

- **Original wording:** "Broader filesystem read containment … governs filesystem read operations … does not dictate filesystem write operations (which are governed by O-04)." (`…filesystem-read-containment-remaining-gaps.md:495-496`); "requires establishing strict path containment and symlink safety" (`…o-05-filesystem-metadata-containment.md:12`).
- **Inferred interpretation:** closes when the enumerated uncontained-read list (`remaining-gaps` §508-516) is empty and the symlink-safety behavior change is signed off.
- **Implementation evidence [confirmed]:** at `2718bc16` every family on that list is contained; the only surviving ambient calls in `src/storage/fs*` production code are writes (GATE-O04) or `#[cfg(test)]`. Mechanism: `openat2` `RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS` *(sibling)*. The repository-existence probe is contained but backend-specific (KI-23 — a seam, not a containment violation).
- **Validation evidence:** per-family Linux-gated containment tests; traversal-attack tests; `cargo test --lib` green at HEAD [executed 2026-09-19]. **Missing:** a recorded re-enumeration finding the list empty; sign-off on the symlink 404→500 behavior change (**TAG-DEC-03**, §2).
- **Proposed closure criteria (PROPOSED):** publish the HEAD re-enumeration (audit notes contain the evidence), resolve TAG-DEC-03, obtain acceptance from whoever holds gate authority.

### GATE-O06 — typed AWS mapping and pinned-MinIO evidence

- **Original wording:** "Typed AWS mapping and genuine pinned-MinIO evidence." (`…filesystem-production-read-cutover.md:238`).
- **Inferred interpretation:** S3 error handling structurally typed, plus a *recorded* live-MinIO run against a pinned image.
- **Implementation evidence [confirmed]:** typed AWS error classification implemented (`src/storage/s3.rs:300-304,516-518` + classifier functions) *(plus sibling client)*. MinIO pinned in `scripts/start-minio.sh:4`; `docker-compose.minio.yml` uses `:latest` (unpinned).
- **Validation evidence:** `tests/s3_live_integration.rs` (~35 live tests; only the AccessDenied test is `#[ignore]`-gated, `:2326`). A CI **definition** would run the suite, but no git remote exists, so no execution evidence (KI-19). Local `conformance-results/` artifacts date 2026-08-26, baseline era, gitignored (KI-20). **Missing:** any recorded pinned-MinIO run at a post-refactor revision; a supervised AccessDenied run (procedure at `s3_live_integration.rs:2318-2327`).
- **Proposed closure criteria (PROPOSED):** run and archive a pinned-MinIO live-suite report at HEAD plus one supervised AccessDenied run; decide compose-file pinning.

### GATE-O13 — hosting, distribution, and release strategy

- **Original wording:** "Permanent repository hosting and release/distribution strategy." (`…filesystem-production-read-cutover.md:239`); "Permanent hosting, crate publishing, and release packaging strategy for `storage-layer-rust` pending." (`…remaining-gaps.md:582`).
- **Implementation evidence [executed 2026-09-19]:** **no git remotes on either repository**; sibling crates declare `publish = false` *(sibling)*; consumed as path dependencies (build not reproducible off this machine, KI-10); `dist/` gitignored; no release workflow. Local annotated tag `v0.9.0` exists (2026-09-06, at `1680f68`); publication/distribution beyond this machine unknowable from local evidence (see the ADR-009 addendum).
- **Validation evidence:** none exists; the criterion is strategic.
- **Proposed closure criteria (PROPOSED):** decisions on hosting, publish-vs-vendor-vs-submodule, and path-dependency strategy, then execution. **Human decision first; least-advanced gate.**

### GATE-O15 — non-Linux verification

- **Original wording:** "Non-Linux filesystem support." / "non-Linux execution is unverified" (`…filesystem-production-read-cutover.md:240,213-214`).
- **Implementation evidence [confirmed]:** Linux `openat2` is a hard init requirement (`src/storage/fs.rs:655-659`; `PlatformUnsupported` mapping `:385-389`); non-Linux stubs fail closed, written for unix-like targets; Windows compilation undemonstrated *(sibling cfg arms)*.
- **Validation evidence:** none — every containment test is `#[cfg(target_os = "linux")]`; CI definition is ubuntu-only. **Missing:** any non-Linux artifact at all.
- **Proposed closure criteria (PROPOSED):** a recorded non-Linux run demonstrating fail-closed init, **or** an accepted Linux-only ADR.

### GATE-O16 — Slice 11 audit / test-inventory / MinIO-report completeness

- **Original wording:** "Earlier Slice 11 audit, test-inventory, and MinIO-report completeness." (`…filesystem-production-read-cutover.md:241`).
- **Inferred interpretation:** Slice 11 (ADR-009, commit `49d4054`) lacks the "Verified Test Inventory" record that slices 1–10 have in `outdated/architecture/current-code-assessment.md`; produce it.
- **Implementation evidence [confirmed]:** slices 1–10 each have an inventory line (last: 708 tests / 18 binaries); none exists for Slice 11. HEAD reality: 21 test files; `cargo test --lib` = 1125/0/13 [executed]; full-suite count unreconciled.
- **Proposed closure criteria (PROPOSED):** run the full suite (lib + all integration binaries) with pinned MinIO healthy; record counts in the established addendum format; reconcile the 18→21 binary delta. **Cheapest gate to close.**

### GATE-FSD06 — overarching extraction/cutover/compatibility/distribution acceptance

- **Original wording:** "Remains unresolved until extracted implementations, cutover evidence, compatibility assessment, and distribution strategy are accepted." (`…filesystem-production-read-cutover.md:242`).
- **Inferred interpretation:** a four-clause human acceptance; the verb is "are accepted", so no code or test can close it.
- **Implementation evidence [confirmed], clause by clause:** extraction — done (three sibling crates consumed); cutover evidence — abundant but recorded in historical snapshots (bannered, archived); compatibility — partially assessed (ADR-007 precedent, ADR-009 SemVer analysis) with **TAG-DEC-01…06 and D7 still pending** (§2); distribution — not started (= GATE-O13).
- **Validation evidence:** no partial-credit statement exists in any document; this register creates none.
- **Proposed closure criteria (PROPOSED):** GATE-O13 closed; TAG-DEC items and D7 resolved; a citable evidence set; a signed acceptance record. **Master OPEN gate.**

## 2. Carried open decision items

Recorded in now-archived slice documents; obligations preserved here (canonical status HERE, original wording in the archived sources).

| Item | Original source (archived) | Subject | Status |
|---|---|---|---|
| TAG-DEC-01 | `outdated/architecture/filesystem-tag-read-production-readiness-assessment.md` | Tag-read cutover decision (recorded DEFERRED) | OPEN — cutover shipped while pending |
| TAG-DEC-02 | same | (recorded PENDING APPROVAL) | OPEN |
| TAG-DEC-03 | same | Symlink rejection behavior change: dangling-symlink tag reads 404→500 | OPEN — blocks GATE-O05 sign-off |
| TAG-DEC-04 | same | (recorded PENDING REVIEW) | OPEN |
| TAG-DEC-05 | same | (recorded DEFERRED) | OPEN |
| TAG-DEC-06 | same | (recorded PENDING REVIEW) | OPEN |
| D7 (metadata assessment) | `outdated/architecture/storage-fs-metadata-integration-assessment.md` (§ "Recommendation (Awaiting Acceptance)") | Metadata-seam recommendation acceptance | OPEN |

## 3. Implementation gaps and defects

| ID | Item | State / impact / evidence / related / next / completion criterion |
|---|---|---|
| KI-01 | **RESOLVED (2026-09-26, remediation R1).** ~~No certificate reload, no SAN validation.~~ A supervised `tls_manager` task now owns runtime ACME renewal (`renew_check_interval_secs`, default 12 h) and hot reload via `RustlsConfig::reload_from_pem_file` (no rebind); externally managed certs are watched (`tls.reload_poll_secs`). SAN preflight fails closed at startup (`allow_san_mismatch` break-glass) and refuses bad runtime reloads while keeping the old cert. Criterion met and test-covered: renewed cert served without restart (`tests/tls_reload_tests.rs` incl. 5-swap soak + mismatch refusal), unit matrix in `src/tls_manager/tests.rs`. | Closed. |
| KI-02 | **Proxy cache eviction/scrub is filesystem-only.** | *State:* confirmed (`src/supervisor.rs:533-536,575-578,624` log-and-return on S3). *Impact:* with `storage.backend="s3"`, `max_cache_bytes` is inert and the cache grows unbounded. *Related:* REQ-016; documented in root README and [`operations.md`](operations.md). *Next:* implement S3 eviction or formally document/accept the limitation. *Criterion (PROPOSED):* S3 cache bounded, or limitation accepted in requirements. |
| KI-03 | **Dead GC config knob `blob_gc_finalize_grace_secs`.** | *State:* parsed+validated, never read (`src/config.rs:170,2039-2044`); effective pin TTL is `gc_pin_duration_secs` = 3600 s (`:1968-1973`; `upload_coordinator.rs:630,811`). *Impact:* 72× gap vs historical design intent for slow pushes; operators setting the knob get nothing. *Related:* REQ-012 (acceptance Unknown). *Next:* decide intent (wire it, remove it, or accept 1 h). *Criterion (PROPOSED):* knob either effective or removed, with REQ-012 resolved. |
| KI-04 | **Token observability absent; dead counters.** | *State:* zero tracing events in `src/http_api/auth_token.rs`; `AuthMetrics` (`app_state.rs:25-29`) constructed, increments never called. *Impact:* the staged-rollout playbook in archived `rbac.md` cannot be followed (no `token_denied` logs to review). *Related:* REQ-006 (acceptance Unknown). *Next:* adopt/drop REQ-006. *Criterion (PROPOSED):* events emitted and asserted by a test, or REQ-006 recorded as dropped and `AuthMetrics` removed. |
| KI-05 | **CLI overrides GC kill switches.** | *State:* `MaintenanceRuntime` force-sets `blob_gc_enabled=true; blob_gc_enable_delete=true` (`src/cli/runtime.rs:261-262,310-312,359-361`); untested either way. *Impact:* `blob_gc.enabled=false` does not stop offline destructive GC — surprising for operators. *Related:* [`operations.md`](operations.md) documents it. *Next:* decide intended semantics. *Criterion (PROPOSED):* behavior decided, documented, and covered by a test. |
| KI-06 | **RESOLVED (2026-09-26, remediation R0).** ~~`CommandIntent` diverged from `CommandPolicy`.~~ Unused classification + exhaustive test deleted; `CommandPolicy` is the single source (ADR-006 addendum). | Closed. |
| KI-08 | **RESOLVED (2026-09-26, remediation R0).** Comments now say `quarantine/gc.lock`, matching the code. | Closed. |
| KI-12 | **RESOLVED as accepted design (2026-09-26, remediation D7/R0).** The no-op `renew_repo_lease` is now a *recorded* design: `RuntimeMutationAuthority` provides cross-process mutation exclusivity; the per-repo flock is belt-and-braces scoping within it, and a second TTL would add failure modes without safety (rationale doc-comment at the impl in `crates/registry-core/src/storage/fs.rs`). GATE-O04's lease row inherits this rationale. | Closed (criterion: "recorded accepted design" met). |
| KI-13 | **RESOLVED (2026-09-26, remediation R0; partially by ADR-010 Phase 1c).** The vestigial `_cfg` param was deleted in Phase 1c; the unused `_storage` parameter was removed from `fetch_manifest_and_cache` (inherent method, `UpstreamFetcher` trait, all call sites) before any external trait consumer exists; the stale branch-logic test comments in `blob_gc/policy.rs` were rewritten to describe contained discovery. | Closed. |
| KI-14 | **RESOLVED (2026-09-26, remediation R0).** Dead `blob_gc_sweep` entry point and its unit tests deleted (zero external callers verified). Scheduled cleanup continues to use `blob_gc_quarantine`/`blob_gc_delete` directly. | Closed. |

## 4. Architectural debt

| ID | Item | Details |
|---|---|---|
| KI-07 | **RESOLVED (2026-09-26, ADR-010 Phase 1b, commit `fe9d8bd`).** ~~Inverted module edge — `application/errors.rs` imported `http_api::upload_state`.~~ `upload_state` moved to core (`crates/registry-core/src/upload_lifecycle/state.rs`); the criterion ("no `http_api` import under `application/`") now holds by construction — `application/` lives in the `registry-core` crate, which cannot reference server modules (enforced by the compiler and `make core-boundary`). | Closed. |
| KI-22 | **GATE-O04 write residues (deferred by design, partially unrecorded).** `meta/membership_ready.json` + `meta/migration_checkpoint.json` pathname writes (`src/storage/fs.rs:3650-3717`), blocking `fs::write_membership_sync`, repo-lease flock (KI-12). | Deferral recorded for the `meta/` writes and sync writer (`src/storage/membership_domain.rs:22-32` out-of-scope list); **no deferral rationale exists for the repo lease**. *Next:* contain or record exceptions (GATE-O04). *Criterion:* GATE-O04's proposed criteria. |
| KI-23 | **Repository-existence family not on ObjectStore.** Contained but backend-specific probe (`src/storage/fs/tag_listing.rs:13-17`; "later phase" in `tag_domain.rs:112`, `fs.rs:669-671`). | Explicitly deferred in code prose. *Next:* migrate when a repository-family design exists. *Criterion:* probe served by a backend-neutral family, or deferral converted to accepted design. |
| KI-24 | **Membership tree enumeration on per-backend seam** (`src/storage/fs/membership_read.rs`), not `ObjectStore::list_page` — the accepted contract has no common-prefix rows (`membership_domain.rs:22-30`). | Explicitly deferred; blocked on the listing-contract limitation (relates to GATE-O03). *Criterion:* enumeration backend-neutral, or limitation accepted. |
| KI-25 | **Reaper inspection-read containment follow-up** noted in `src/storage/fs/upload_quarantine_read.rs:14,330` (reaper *mutations* are contained; an inspection-read follow-up remains). A test-only prototype of a single-tree cleanup exists (`tests/upload_lifecycle_contained_cleanup_prototype.rs`) and was never productionized. | *Criterion:* follow-up implemented or recorded as unnecessary. |
| KI-26 | **Wave-1 residual coupling** *(paths updated for the ADR-010 crate split; scope narrowed 2026-09-26: `GcService` now takes core `GcPolicy` instead of `Arc<Config>`, and the application layer consumes the proxy engine only through `upstream::UpstreamFetcher`)*. `AppState` remains a process bag (config, GC, proxy, semaphores, IP limiter — `src/app_state.rs`); whole `Config` passed into assembly; `src/http_api/handlers.rs` is a 1538-line dispatcher with ~58 `state.config` policy reads; admin GC (`http_api/admin.rs`) and `/token` (`http_api/auth_token.rs`) bypass the application services; the assessment §10.1 `GarbageCollectionService`/`ProxyService` facades were never built (re-scoping to 7 services never recorded as a decision — §6). Omnibus `Storage` remains the internal port implementation vehicle (`crates/registry-core/src/storage/ports/mod.rs`). | *Evidence:* audit passes A/B; ADR-010 execution. *Next:* per-item decisions; none is a defect by itself. Handler-side cleanup is an explicit ADR-010 non-goal. *Criterion (PROPOSED):* each residual either scheduled or recorded as accepted architecture. |
| KI-27 | **ADR-010 split residues (2026-09-26).** (a) Core visibility widened for server wiring (`storage::facade`, `fs::{manifest_listing, tag_listing, repo_discovery, manifest_refs, read_adapter}`, two `#[doc(hidden)]` reader accessors) pending deliberate Phase 3 curation — deferred while registry-core is 0.x-unstable with one consumer. (b) **resolved 2026-09-26**: tracing-target rename documented in `operations.md` §6. (c) **resolved 2026-09-26**: `cargo test --workspace` documented in `operations.md` §6 and the root README. | *Criterion:* (a) revisited when a second consumer exists; (b)/(c) closed. |

## 5. Missing verification / acceptance evidence

| ID | Item | Details |
|---|---|---|
| KI-19 | **No git remotes on either repository**; `.github/workflows/ci.yml` is a definition with no evidence of ever executing; nothing pushed/published from these clones. | [executed check 2026-09-19]. Feeds GATE-O13. *Criterion:* remotes/hosting decided (GATE-O13). |
| KI-20 | **Run-evidence artifacts gitignored and stale.** `conformance-results/` local artifacts date 2026-08-26 (baseline era; all matrices exit 0, spec v1.1.1); `dist/` gitignored. No post-refactor conformance or live-MinIO run is recorded anywhere; **current conformance at HEAD is unverified**. (Runner has since moved to `tests/compliance/run.sh`; new results land in `tests/compliance/results/`, also gitignored.) | Feeds GATE-O06/O16 and REQ-001/REQ-021 verification columns. *Next:* run + archive evidence at HEAD. *Criterion:* committed/archived run reports at a stated revision. |
| — | Gates GATE-O05/O06/O15/O16 verification specifics | see §1. |
| — | TAG-DEC-01…06, D7 | see §2. |

## 6. Unresolved requirements and design decisions

Acceptance status is canonical in [`requirements.md`](requirements.md); this section tracks the *decision work items*.

| Item | Question | Evidence / notes |
|---|---|---|
| REQ-006 | Adopt, defer, or drop token observability events? | KI-04; plan text preserved in `outdated/rbac.md` Phase 4. |
| REQ-012 | Was abandoning the 72 h finalize grace intentional? | KI-03; design rationale in `outdated/blob-gc-online.md:87-88`. |
| REQ-013 | Adopt or drop EXDEV cross-device quarantine refusal? | Not implemented at `2718bc16`; no record of dropping it (`outdated/blob-gc-online.md:68`). |
| REQ-014 | Ratify the shipped CLI-safety design (FsRootLock + mutation authority) that displaced the thin-client CLI idea? | Shipped design tested (`tests/supervisor_and_command_tests.rs`); candidate retrospective ADR — **proposal only**. |
| REQ-020 | Adopt the private-repo name heuristic as a requirement, or replace with config? | KI-17. |
| Assessment §13 Q1 | Keep omnibus `Storage` as internal composite vehicle, or force sub-traits? | Fact recorded in ADR-003 addendum; never decided. |
| Assessment §13 Q2 | Multi-instance ref-index strategy (distributed leasing vs single-writer)? | Never decided; §12 of the archived assessment retains single-writer sled as a non-goal-era trade-off. |
| Re-scoping record | The 7-service decomposition (vs assessment §10.1 four facades) and the sibling `PageToken` standardization-withholding were implemented/asserted without decision records. | Candidate retrospective ADRs — **proposals only**; must not be presented as historical decisions. |
| KI-17 | **Undocumented private-repo name heuristic**: repos named `private*`/`secret*`/`protected*`/`restricted*` (or containing angle-bracket encodings) require auth regardless of `anonymous_pull` (`src/config.rs:2833-2848`). | Behavior documented, not thereby approved. |
| KI-18 | **`*` grant silently confers registry catalog scope** (`src/rbac.rs:212-220`) — absent from the archived rbac.md model. | Behavior documented, not thereby approved. |

## 7. Operational and packaging concerns

| ID | Item | Details |
|---|---|---|
| KI-09 | **Token rate limit env-only and untested.** Global fixed window configured from raw env only (`src/supervisor.rs:238-248`), outside `Config` — invisible to `check-config`/strict validation; `src/token_rate_limit.rs` has no tests. | *Criterion (PROPOSED):* knob in `Config` + tests, or documented exception. |
| KI-10 | **Dockerfile does not stage `../storage-layer-rust` path dependencies** (only `vendor/acmecert`); container image build viability unverified. | Feeds GATE-O13 (path-dep strategy). *Criterion:* image builds reproducibly, or documented builder prerequisite. |
| KI-11 | **Packaged configs carry environment-specific and sensitive material.** `etc/registry-rust/registry.core.toml` + `registry.auth.toml` (RPM/DEB conffiles) contain environment-specific hostnames/endpoints, an ACME authorization string, `debug=true`, an unknown key `[auth.push] mode` (hard `UnknownKeys` failure under strict/best-practice), a wrong `hash-secret` usage comment, and an Argon2id hash for a `*`-push-grant user. `tls/` (incl. `old/`, `old2/`) and `certs/` hold PEM private-key files in the working tree. **Paths only; values not reproduced; liveness not verified.** | Packaging/security decision. *Criterion (PROPOSED):* packaged configs are neutral templates; key material out of the tree or accepted as dev fixtures. |
| KI-15 | Multi-platform image visibility question (see all platform variants, not just amd64). | Migrated from `outdated/root/ISSUES.txt`. Open product question. |
| KI-16 | `docker buildx imagetools create` usage note for mirroring multi-arch images. | Migrated from `outdated/root/BUG0.txt`. Note only. |
| KI-21 | **RPM/DEB systemd unit divergence.** DEB unit lacks `LimitNOFILE`, `ReadWritePaths`, `ReadOnlyPaths` present in the RPM unit while setting `ProtectSystem=strict`. | *Criterion (PROPOSED):* units aligned or divergence documented per package. |

Resolved-by-observation notes from the migrated trackers: harbor-style groups exist (REQ-007); cache separation exists (`proxy.cache.fs_root`/`s3_prefix`); "do I need the harbor api?" is an open product question, not tracked as debt.
