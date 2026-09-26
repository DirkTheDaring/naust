# Technical debt — canonical register

- **Role:** the single canonical register of remaining work: implementation gaps, architectural debt, missing verification/acceptance evidence, unresolved requirements and decisions, and operational/packaging concerns. Other documents link here by ID; status lives only here.
- **Audited code revision:** `master` @ `2718bc16` (2026-09-19 documentation audit; evidence trail: [`outdated/audit/`](outdated/audit/2026-09-19-doc-reconciliation-notes.md)).
- **ID continuity:** `KI-01…KI-21` and `GATE-…` IDs are unchanged from the prior `known-issues.md` / `architecture/acceptance-gates.md` registers (now merged here). New items added by this consolidation continue the KI series (KI-22…KI-26). `REQ-…` acceptance status lives in [`requirements.md`](requirements.md); ADRs live in [`adr/`](adr/README.md).
- **Semantics:** entries document current state and evidence; nothing here is an approved priority, owner, or decision. "Completion criterion" rows marked *PROPOSED* are this register's suggestion, not an accepted definition of done. Fixes to code, packaging, or release are separate unapproved decisions.

Contents: §1 acceptance gates · §2 carried open decisions · §3 implementation gaps & defects · §4 architectural debt · §5 missing verification/acceptance evidence · §6 unresolved requirements & design decisions · §7 operational & packaging concerns.

---

## 1. Acceptance gates (GATE-O03…GATE-FSD06)

**Authority: RESOLVED (2026-09-26, ADR-013)** — the repository maintainer is the gate/release authority; closures are recorded decisions, never silent. Historical note: no in-repo document previously defined these gates or who may close them. The numbering (only O-03/04/05/06/13/15/16 and one "D-06" ever occur; O-01/02/07–12/14 occur nowhere in this repo or `../storage-layer-rust`) is inherited from an out-of-tree review process; the earliest in-repo docs assert the gates "retain their existing meanings" without stating them (`outdated/architecture/o-05-filesystem-metadata-containment.md`, commit `f7ad9f9`, 2026-09-08; O-04/O-15 added at `5f504ac`). **All eight gates were CLOSED by ADR-013 (2026-09-26)**; the per-gate sections below are preserved as the historical record of their criteria and evidence.

| Tracking ID | Historical ID | Status | Closure requires |
|---|---|---|---|
| GATE-O03 | O-03 | **CLOSED (ADR-013)** | contract ratified; token standardization accepted as withheld |
| GATE-O04 | O-04 | **CLOSED (ADR-013)** | residues accepted as permanent exceptions (incl. deletion-durability carve-out) |
| GATE-O05 | O-05 | **CLOSED (ADR-013)** | audit re-enumeration accepted; TAG-DEC-03 ratified (fail-closed 500) |
| GATE-O06 | O-06 | **CLOSED (ADR-013)** | committed evidence (`evidence/2026-09-26-debt-remediation/`) |
| GATE-O13 | O-13 | **CLOSED (D6 2026-09-26)** | public GitHub (`DirkTheDaring/naust`), MIT, self-hosted deployments; release records follow the first push |
| GATE-O15 | O-15 | **CLOSED (ADR-013)** | Linux-only declared (FsStorage requires `openat2`) |
| GATE-O16 | O-16 | **CLOSED (ADR-013)** | committed evidence (see O-06) |
| GATE-FSD06 | fs-doc "D-06" | **CLOSED (ADR-013)** | clauses covered by ADR-009…013 closures |

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
| TAG-DEC-01 | `outdated/architecture/filesystem-tag-read-production-readiness-assessment.md` | Tag-read cutover decision (recorded DEFERRED) | **RATIFIED (ADR-013)** |
| TAG-DEC-02 | same | (recorded PENDING APPROVAL) | **RATIFIED (ADR-013)** |
| TAG-DEC-03 | same | Symlink rejection behavior change: dangling-symlink tag reads 404→500 | **RATIFIED (ADR-013)** — fail-closed 500 is the contract |
| TAG-DEC-04 | same | (recorded PENDING REVIEW) | **RATIFIED (ADR-013)** |
| TAG-DEC-05 | same | (recorded DEFERRED) | **RATIFIED (ADR-013)** |
| TAG-DEC-06 | same | (recorded PENDING REVIEW) | **RATIFIED (ADR-013)** |
| D7 (metadata assessment) | `outdated/architecture/storage-fs-metadata-integration-assessment.md` (§ "Recommendation (Awaiting Acceptance)") | Metadata-seam recommendation acceptance | **ACCEPTED (ADR-013)** |

## 3. Implementation gaps and defects

| ID | Item | State / impact / evidence / related / next / completion criterion |
|---|---|---|
| KI-01 | **RESOLVED (2026-09-26, remediation R1).** ~~No certificate reload, no SAN validation.~~ A supervised `tls_manager` task now owns runtime ACME renewal (`renew_check_interval_secs`, default 12 h) and hot reload via `RustlsConfig::reload_from_pem_file` (no rebind); externally managed certs are watched (`tls.reload_poll_secs`). SAN preflight fails closed at startup (`allow_san_mismatch` break-glass) and refuses bad runtime reloads while keeping the old cert. Criterion met and test-covered: renewed cert served without restart (`tests/tls_reload_tests.rs` incl. 5-swap soak + mismatch refusal), unit matrix in `src/tls_manager/tests.rs`. | Closed. |
| KI-02 | **RESOLVED (2026-09-26, remediation R2).** ~~Proxy cache eviction/scrub is filesystem-only.~~ Eviction is now backend-neutral AND contained: enumeration + physical deletes go through the new permit-free `CacheEvictionPort` (streaming CAS listing; contained unlink on FS, ETag-conditional delete on S3), planning is the core `cache_eviction` LRU planner. Note the historical walker never deleted anything on ANY backend — `max_cache_bytes` was inert everywhere; it is now enforced (total-bytes budget, protected content never evicted, residual logged). Scrub remains FS-only as a *recorded* design (S3 payload integrity is enforced by the object store; rationale at the guard in `supervisor.rs`). Covered by planner unit tests, FS/S3-mock worker e2e tests, FS parity + fail-closed listing tests, and a live-MinIO port acceptance test. | Closed. |
| KI-03 | **RESOLVED (2026-09-26, remediation R3).** `blob_gc_finalize_grace_secs` is now effective: freshly published blobs receive an auto-expiring `finalize-grace` pin (REQ-012 wired; dedicated test `test_finalize_grace_pin_protects_fresh_publication`). | Closed. |
| KI-04 | **RESOLVED (2026-09-26, remediation R3, REQ-006 adopted).** `/token` emits `token_issued`/`token_denied`/`token_error` tracing events with reasons, and the `AuthMetrics` counters are wired (endpoint test asserts both). | Closed. |
| KI-05 | **RESOLVED (2026-09-26, remediation R3).** The CLI now respects `blob_gc.enabled=false` (and `enable_delete=false` for delete) and refuses with a clear error; `--force-gc` reproduces the historical override. Test-covered both ways (`test_cli_respects_blob_gc_kill_switch`). | Closed. |
| KI-06 | **RESOLVED (2026-09-26, remediation R0).** ~~`CommandIntent` diverged from `CommandPolicy`.~~ Unused classification + exhaustive test deleted; `CommandPolicy` is the single source (ADR-006 addendum). | Closed. |
| KI-08 | **RESOLVED (2026-09-26, remediation R0).** Comments now say `quarantine/gc.lock`, matching the code. | Closed. |
| KI-12 | **RESOLVED as accepted design (2026-09-26, remediation D7/R0).** The no-op `renew_repo_lease` is now a *recorded* design: `RuntimeMutationAuthority` provides cross-process mutation exclusivity; the per-repo flock is belt-and-braces scoping within it, and a second TTL would add failure modes without safety (rationale doc-comment at the impl in `crates/naust-core/src/storage/fs.rs`). GATE-O04's lease row inherits this rationale. | Closed (criterion: "recorded accepted design" met). |
| KI-13 | **RESOLVED (2026-09-26, remediation R0; partially by ADR-010 Phase 1c).** The vestigial `_cfg` param was deleted in Phase 1c; the unused `_storage` parameter was removed from `fetch_manifest_and_cache` (inherent method, `UpstreamFetcher` trait, all call sites) before any external trait consumer exists; the stale branch-logic test comments in `blob_gc/policy.rs` were rewritten to describe contained discovery. | Closed. |
| KI-14 | **RESOLVED (2026-09-26, remediation R0).** Dead `blob_gc_sweep` entry point and its unit tests deleted (zero external callers verified). Scheduled cleanup continues to use `blob_gc_quarantine`/`blob_gc_delete` directly. | Closed. |

## 4. Architectural debt

| ID | Item | Details |
|---|---|---|
| KI-07 | **RESOLVED (2026-09-26, ADR-010 Phase 1b, commit `fe9d8bd`).** ~~Inverted module edge — `application/errors.rs` imported `http_api::upload_state`.~~ `upload_state` moved to core (`crates/naust-core/src/upload_lifecycle/state.rs`); the criterion ("no `http_api` import under `application/`") now holds by construction — `application/` lives in the `naust-core` crate, which cannot reference server modules (enforced by the compiler and `make core-boundary`). | Closed. |
| KI-22 | **GATE-O04 write residues (deferred by design, partially unrecorded).** `meta/membership_ready.json` + `meta/migration_checkpoint.json` pathname writes (`src/storage/fs.rs:3650-3717`), blocking `fs::write_membership_sync`, repo-lease flock (KI-12). | Deferral recorded for the `meta/` writes and sync writer (`src/storage/membership_domain.rs:22-32` out-of-scope list); **no deferral rationale exists for the repo lease**. *Next:* contain or record exceptions (GATE-O04). *Criterion:* GATE-O04's proposed criteria. |
| KI-23 | **Repository-existence family not on ObjectStore.** Contained but backend-specific probe (`src/storage/fs/tag_listing.rs:13-17`; "later phase" in `tag_domain.rs:112`, `fs.rs:669-671`). | Explicitly deferred in code prose. *Next:* migrate when a repository-family design exists. *Criterion:* probe served by a backend-neutral family, or deferral converted to accepted design. |
| KI-24 | **Membership tree enumeration on per-backend seam** (`src/storage/fs/membership_read.rs`), not `ObjectStore::list_page` — the accepted contract has no common-prefix rows (`membership_domain.rs:22-30`). | Explicitly deferred; blocked on the listing-contract limitation (relates to GATE-O03). *Criterion:* enumeration backend-neutral, or limitation accepted. |
| KI-25 | **Reaper inspection-read containment follow-up** noted in `src/storage/fs/upload_quarantine_read.rs:14,330` (reaper *mutations* are contained; an inspection-read follow-up remains). A test-only prototype of a single-tree cleanup exists (`tests/upload_lifecycle_contained_cleanup_prototype.rs`) and was never productionized. | *Criterion:* follow-up implemented or recorded as unnecessary. |
| KI-26 | **RESOLVED with recorded residues (2026-09-26, remediation R4 — see ADR-011).** The 1538-line dispatcher is split by resource family; mechanical config reads go through the `HttpTransferPolicy` snapshot; `/_admin/gc/*` delegates to `GcAdminService` (owns run-id sequence + defaults; `gc_run_seq` removed from `AppState`); `/token` delegates to `TokenService` (decision, allowlist, signing, observability). The service census (7 core + 2 server) and the `GarbageCollectionService`/`ProxyService` re-scoping are recorded in ADR-011, which also records four accepted residues (config-parameterized token decision fns; auth-boundary reads in handlers; flat `AppState`; omnibus-`Storage`-as-vehicle). | Closed (criterion met: every residual recorded as accepted architecture). |
| KI-27 | **ADR-010 split residues (2026-09-26).** (a) Core visibility widened for server wiring (`storage::facade`, `fs::{manifest_listing, tag_listing, repo_discovery, manifest_refs, read_adapter}`, two `#[doc(hidden)]` reader accessors) pending deliberate Phase 3 curation — deferred while naust-core is 0.x-unstable with one consumer. (b) **resolved 2026-09-26**: tracing-target rename documented in `operations.md` §6. (c) **resolved 2026-09-26**: `cargo test --workspace` documented in `operations.md` §6 and the root README. | *Criterion:* (a) revisited when a second consumer exists; (b)/(c) closed. |

## 5. Missing verification / acceptance evidence

| ID | Item | Details |
|---|---|---|
| KI-19 | **No git remotes on either repository** — unchanged; blocked on the GATE-O13 hosting decision (owner). *Progress 2026-09-26 (R6):* `.github/workflows/ci.yml` rewritten to mirror the real local pipeline (fmt → boundary gate → workspace tests → conformance; separate live-S3 job with a MinIO service), including vendored-path-dep staging — but it has still never executed on a hosted runner. *Criterion:* remotes/hosting decided (GATE-O13), then one green hosted run. |
| KI-20 | **RESOLVED (2026-09-26, remediation R6).** Committed evidence convention established: `evidence/` holds per-revision qualification records + JUnit/exit artifacts (see `evidence/README.md`; first record: `evidence/2026-09-26-debt-remediation/` — all four conformance matrices exit 0, live suite 33/0/1 ×2, container build verified, at stated revisions). Raw HTML/logs remain gitignored by design. GATE-O06/O16 evidence rows can now cite committed artifacts. | Closed. |
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
| KI-17 | **RESOLVED (2026-09-26, remediation R3).** The private-name heuristic is explicit configuration: `auth.private_name_prefixes` (default = historical list; empty disables prefix privacy). Injection-smell checks (angle brackets/encodings) remain hardcoded by design. Test-covered. | Closed. |
| KI-18 | **RESOLVED (2026-09-26, remediation R3).** `auth.star_grants_catalog` (default `true` = historical behavior) controls whether a `*` grant confers registry catalog scope. Test-covered both ways. | Closed. |

## 7. Operational and packaging concerns

| ID | Item | Details |
|---|---|---|
| KI-09 | **RESOLVED (2026-09-26, remediation R3).** Token rate-limit knobs live in `Config` (`[token].rate_limit_rpm`/`rate_limit_window_secs`, env kept as override) — visible to strict validation/check-config; `token_rate_limit.rs` now has unit tests (disabled, window cap, reset). | Closed. |
| KI-10 | **RESOLVED (2026-09-26, remediation R5).** `make vendor-sync` stages both sibling path-deps into `vendor/` (acmecert + storage-layer-rust crates with their workspace manifest — the crates use workspace field inheritance); the Dockerfile stages them at `/acmecert` and `/storage-layer-rust`. **Verified: `podman build` completed (exit 0) and the binary runs in the image.** | Closed. |
| KI-11 | **RESOLVED (2026-09-26, remediation R5).** Packages now ship NEUTRAL tracked templates (`packaging/config/registry.{core,auth}.toml` — placeholders only, ACME disabled, `debug=false`, no unknown keys, no credential material; validated by `check-config`). The Makefile/RPM/DEB staging no longer reads the untracked local `etc/` tree (which had made package contents machine-dependent). Local `etc/`, `tls/`, `certs/` were verified **gitignored and untracked** (the register's "in the working tree" concern carried no git-history risk); they remain local dev fixtures. Pre-push tree scan stays on the R6 checklist. | Closed. |
| KI-15 | Multi-platform image visibility question (see all platform variants, not just amd64). | Migrated from `outdated/root/ISSUES.txt`. Open product question. |
| KI-16 | `docker buildx imagetools create` usage note for mirroring multi-arch images. | Migrated from `outdated/root/BUG0.txt`. Note only. |
| KI-21 | **RESOLVED (2026-09-26, remediation R5).** DEB unit aligned with RPM: `LimitNOFILE=65536`, `ReadOnlyPaths=/etc/naust`, `StateDirectoryMode=0750` added (it already had `ReadWritePaths` — the register row was partially stale). `ProtectSystem=strict` retained in both. | Closed. |

Resolved-by-observation notes from the migrated trackers: harbor-style groups exist (REQ-007); cache separation exists (`proxy.cache.fs_root`/`s3_prefix`); "do I need the harbor api?" is an open product question, not tracked as debt.
