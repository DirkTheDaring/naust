> **ARCHIVED audit artifact (moved from `docs/audit/` on 2026-09-19).** Frozen proposal snapshot; superseded by the executed consolidation. Canonical set: [`../../README.md`](../../README.md). Paths named below predate the consolidation.

# Reconciliation proposal — registry-rust documentation vs implementation (v2)

- **Status:** APPROVED (options A–E, recommended defaults) and **EXECUTED 2026-09-19**. Execution record and validation results: `2026-09-19-doc-reconciliation-notes.md` §9. Changes are uncommitted, awaiting final review. The registers proposed in §2/§5/§6 now exist as `docs/requirements.md`, `docs/architecture/acceptance-gates.md`, `docs/known-issues.md`, `docs/gc-operations.md`, `docs/README.md` — canonical status lives there; the tables below are the frozen proposal snapshot.
- No application code was or will be changed by this work. Gate closures, retrospective ADRs, product-behavior and packaging/release decisions remain explicitly unresolved.
- **Audited revision:** `master` @ `2718bc16` (clean tree, no stashes). Path deps: `../storage-layer-rust` @ `74974af5`, `../acmecert` @ `0e985c8` (both clean). **Sibling repos were not audited internally**; every claim that rests on their sources is marked *(sibling)*.
- **Evidence base:** `docs/audit/2026-09-19-doc-reconciliation-notes.md` §5.0–§5.8 (file:line anchors live there and in the per-pass reports).
- **Labels:** **[confirmed]** observed in source at HEAD · **[test]** asserted by an in-tree test (not executed by this audit unless stated) · **[executed]** run during this audit · **[inferred]** reasonable reading of evidence, labeled as such · **[unresolved]** evidence insufficient or conflicting.

**Everything this audit executed:** `cargo test --lib --locked` (1125 passed / 0 failed / 13 ignored; ignored-set composition not analyzed); `git tag` / tag-date inspection; `git remote -v` on both repos (both **empty**); inspection of local `conformance-results/` artifacts. Nothing else was run: no integration binaries, no server, no container build, no deployment, no live S3.

A consequence of the empty remotes that qualifies several claims below: `.github/workflows/ci.yml` is a workflow **definition**; with no remote configured there is **no local evidence it has ever executed** on a hosted runner. Statements formerly phrased "CI runs X" are downgraded to "a CI definition would run X".

---

## 1. Current-state architecture overview

`docs/architecture/current-state.md` (aligned at HEAD itself) survived adversarial verification on every checked claim and should remain the canonical current-state document **[confirmed — passes A/B]**, with the precision fixes below.

```
CLI / HTTP (axum router: supervisor.rs:206-307; OCI path parser: http_api/routing.rs)
  → 7 application services on AppState (src/application/)
    → domain engines (upload coordinator, manifest lifecycle, membership ledger, GcService, proxy)
      → StorageWiring: 15 narrow port views (src/storage/ports/mod.rs:1047-1162)
        → FsStorage | S3Storage
          → contained authorities (storage-fs FsMetadataReader/openat2 — sibling)
          → 6 shared ObjectStore domains (tags, manifests, referrers, membership point ops,
            lifecycle journal, repo timestamps) — backend-neutral, FS + S3
```

Precision fixes the canonical doc should absorb (all **[confirmed]**, anchors in notes §5.1–§5.6):

1. **"HTTP-free application layer" — two distinct properties, one holds fully, one does not.** *Framework independence* holds: zero `axum`/`http::`/`StatusCode`/`HeaderMap` references under `src/application/`. *Dependency direction* is violated once: `src/application/errors.rs:1` imports `crate::http_api::upload_state::StateTokenError` (that module is itself HTTP-type-free, so no framework types leak — but the application→http_api module edge points against ADR-002's direction rule). Docs should state both halves, not the blanket claim.
2. **Filesystem containment — never state unqualified.** All enumerated production **read** families are descriptor-contained, mutations go through contained authorities, and GC discovery + the reaper are contained. Residual **ambient pathname** surfaces remain: `mark_membership_ready` and `save_migration_checkpoint` writes under `meta/` (`fs.rs:3650-3717`; the latter via `ensure_dir`, not `create_dir_all` as currently worded), the repo lease flock `repos/<repo>/.repo_lock` (`fs.rs:1341-1373`; `renew` is a no-op `Ok(true)`), the deferred reaper-inspection read follow-up (`upload_quarantine_read.rs:14,330`), and contained-but-backend-specific seams (repo-existence probe, membership enumeration).
3. **Omnibus `Storage` trait:** no production consumer holds `Storage`/`dyn Storage` (all such sites test-only), but the trait is the production **implementation vehicle** — the port macros expand to `Storage::<method>` calls, applied unconditionally to both backends (`ports/mod.rs:386-714`).
4. **`ConsistencyCoordinator` is per-composition-root**, not process-global (server: one; CLI: one per operation; `task_supervisor.rs:680`: another).
5. Route surface includes non-standard `TagDelete` (`/v2/<name>/tags/reference/<tag>`) and `_oci/ext/discover` (`http_api/routing.rs:5-44`).
6. **Release status (split by what local evidence establishes):** annotated tag `v0.9.0` exists locally (created 2026-09-06, at `1680f68`) **[executed]**. ADR-009's "not yet tagged" line (authored 2026-09-02) was **true when written**; its repetition in the 2026-09-18 addenda of `current-code-assessment.md:483` and `current-state.md:103` was stale when written. Publishing/distribution: sibling crates declare `publish = false` *(sibling)*; no remotes exist on either repo, so nothing was pushed from these clones; whether any external hosting/publication exists is **[unresolved — unknowable locally]**.

## 2. Canonical requirements register (proposed `docs/requirements.md`)

No existing document is suitable as a requirements register (`PLAN.md` is a dated plan; `README.md` is user documentation), so a new `docs/requirements.md` is proposed, seeded with the table below. Columns are deliberately independent: **Acceptance** = is this requirement currently wanted? (an old requirement's current acceptance may be Unknown); **Implementation** = what the code does; **Verification** = what evidence exists. Current behavior is never auto-promoted to an accepted requirement — behavior-derived rows carry Acceptance = *Proposed*.

Acceptance values: **Accepted** (stated in a requirement-bearing doc, not contradicted) · **Assumed** (long-implemented, never formally stated) · **Unknown** (source stale/contradicted; current intent unclear) · **Proposed** (inferred from code/tests; needs adoption decision).

| ID | Requirement | Source (verbatim origin) | Acceptance | Implementation at HEAD | Verification evidence | Unresolved |
|---|---|---|---|---|---|---|
| REQ-001 | OCI/Docker v2 API surface (ping, pull, push, tags, catalog) | PLAN.md §MVP | Accepted | Implemented [confirmed] | Conformance + oci_1_1 + read/mutation suites [test]; **current conformance at HEAD unverified** — only local run artifacts date 2026-08-26 (baseline era, all matrices exit 0, spec v1.1.1) [executed observation]; CI definition exists, execution evidence absent | Re-run conformance at HEAD |
| REQ-002 | Anonymous pull; authenticated push | PLAN.md §Policy | Accepted | Implemented; overridden per-repo by a hardcoded name heuristic (`config.rs:2833-2848`) [confirmed] | auth matrix tests, grammar test 08 [test] | Is the name heuristic an accepted requirement? (Proposed REQ-020) |
| REQ-003 | FS default + S3 optional pluggable storage | PLAN.md §Storage | Accepted | Implemented [confirmed] | ports_wiring FS+MinIO [test]; live suite requires MinIO endpoint | — |
| REQ-004 | Push auth via Basic; TLS in front | PLAN.md §Auth | Accepted (exceeded) | Basic + Bearer/token + robots + users/groups [confirmed] | security.rs + sidecar suites [test] | — |
| REQ-005 | RBAC invariants: deny-by-default; granted ⊆ requested ∩ policy | docs/rbac.md §Invariants; PLAN.md §RBAC | Accepted | Implemented (`rbac.rs:200-252`) [confirmed] | truth-table + regression tests [test] | `*` grant silently implies catalog scope — undocumented behavior (known-issues item) |
| REQ-006 | Token observability events (token_issued/denied/error) | docs/rbac.md Phase 4; PLAN.md §Review | Unknown (planned; never revisited) | **Not implemented at audited revision**: zero tracing calls in auth_token.rs; `AuthMetrics` counters never called [confirmed] | none | Adopt, defer, or drop — human decision |
| REQ-007 | Users + groups config-only RBAC (harbor-lite ph. 2) | docs/harbor-lite-phase2.md | Accepted ("implemented") | Implemented [confirmed]; two doc details false (trailing-`/` rule; precedence flag) | user/group sidecar tests [test] | — |
| REQ-008 | Registry error format + status codes | PLAN.md §Errors | Accepted | Implemented [confirmed] | conformance regression [test] | — |
| REQ-009 | Hardening: limits, timeouts, traversal safety, tracing | PLAN.md §Hardening | Accepted (exceeded) | Implemented + slowloris/IP-concurrency [confirmed] | slow_connection, ip_concurrency [test] | — |
| REQ-010 | GC never deletes referenced/pinned/young blobs | docs/blob-gc.md safety model | Accepted (model), doc stale | Implemented with **5** protection axes (docs list 3) [confirmed] | 23 adversarial coordination tests + online GC integration [test] | — |
| REQ-011 | Online GC: quarantine + delayed delete, admin-triggered | docs/blob-gc-online.md | Accepted (scope since exceeded) | Implemented; also S3 conditional-delete online GC and a background scheduler the doc doesn't cover [confirmed] | online_gc_integration [test] | Docs must be rebased on actual behavior (disposition) |
| REQ-012 | 72 h finalize grace protecting slow pushes | blob-gc-online.md:87-88 | Unknown | **Divergent**: `blob_gc_finalize_grace_secs` (72 h) parsed, never read; actual pin TTL `gc_pin_duration_secs` = 1 h [confirmed] | no test covers the gap | Intentional abandonment or defect? [unresolved] |
| REQ-013 | Refuse cross-device (EXDEV) online quarantine | blob-gc-online.md:68 | Unknown | Not implemented at audited revision (no EXDEV handling on quarantine path) [confirmed]; no record of dropping it | none | Adopt or drop |
| REQ-014 | GC CLI as thin client of admin API when server live | blob-gc-online.md:178-179 | Unknown (superseded in practice) | Different design shipped: CLI opens sled directly under FsRootLock + mutation authority [confirmed] | supervisor_and_command_tests cover the shipped design [test] | Ratify shipped design (retrospective ADR?) |
| REQ-015 | Membership backfill gate before serving pre-existing data | membership-migration docs | Accepted | Implemented: fail-closed boot + resumable checkpointed migration [confirmed] | full-lifecycle membership tests [test] | — |
| REQ-016 | Proxy cache bounded by max_cache_bytes | README | Accepted (as documented) | FS-only; **S3 cache eviction not implemented** (log-and-return, `supervisor.rs:533-578`) [confirmed] | proxy tests exist; no test of the S3 gap | Document limitation (blocking doc fix); code fix separate |
| REQ-017 | ACME TLS provisioning + renewal | README/configs; ideas.txt FIXMEs | Accepted (partially met) | Startup-only provisioning; **no reload thread; no SAN validation** [confirmed; renewal-window logic is *(sibling)*] | none (TLS path untested) | Known-issue register entry |
| REQ-018 | Wave-1 target architecture (services, ports, roots, typed errors) | current-code-assessment.md §10–11; ADRs | Accepted (via ADRs, which re-scoped it) | Landed **as re-scoped**: 7-service decomposition instead of §10.1's four facades; GC/proxy application facades **not implemented at audited revision**; AppState narrowing partial (still holds config/GC/proxy/semaphores); admin-GC and /token bypass services [confirmed] | application/ports/supervisor suites [test] | The §10.1→ADR re-scoping was never recorded as a decision |
| REQ-019 | Wave-2 containment/ObjectStore end-state | filesystem-* notes | Accepted (per-slice), acceptance gates OPEN | Landed through streaming CAS listing; six explicitly deferred frontier items (§4b) [confirmed] | storage adversarial + contained-discovery suites [test]; lib suite [executed] | Gate closure (§5) |
| REQ-020 *(Proposed — behavior, not accepted requirement)* | Private-repo name heuristic (`private*`/`secret*`/…) forces auth | code only (`config.rs:2833-2848`) | Proposed | Implemented [confirmed] | covered indirectly [test] | Adopt as requirement or replace with config |
| REQ-021 *(Proposed)* | Zero OCI wire / storage-layout / state-machine change during refactor | current-code-assessment.md §10.2 (stated as constraint) | Assumed | Honored as far as audited [inferred] | golden storage + conformance regression [test]; **not re-verified at HEAD by execution** | — |
| REQ-022 *(Proposed)* | Single-writer deployment safety (FsRootLock / S3 lease) | code + ADR-006 | Assumed | Implemented [confirmed] | lock-contention matrix [test] | Lease `renew` no-op — intended? |
| REQ-023 | v0.9.0 release tagged, published, distributed | ADR-009 status line (O-13/D-06 context) | Accepted (as an open act) | Tagged locally [executed]; publish blocked (`publish = false` siblings *(sibling)*; no remotes) | n/a | Hosting/publication strategy = gate O-13 |

## 3. ADR matrix

Recorded decision status preserved exactly; conformance separate; proposed action is always a dated addendum (ADR-005's existing implementation note is the in-repo model), never a rewrite. Proposed retrospective ADRs are **proposals only** until accepted.

| ADR | Recorded status | Implementation conformance at HEAD [confirmed] | Proposed addendum |
|---|---|---|---|
| 001 | Accepted | Conforms | Optional: coordinator is per-composition-root |
| 002 | Accepted | Conforms; §3.5 later superseded by ADR-004 (unannotated); proxy methods exist under both ADR-002 and ADR-004 names | Note §3.5 supersession + method-name map |
| 003 | Accepted | Conforms in substance; §3.1/3.2 reader-field sketch removed by ADR-004 (unannotated); wiring sketch missing 2 of 15 fields; omnibus trait retained as internal vehicle — assessment §13 Q1 never answered by any decision record | Note supersession; record omnibus-as-vehicle as **fact**, flag Q1 as undecided |
| 004 | Accepted | Conforms; field-name drift (`blob_service` vs `blob_mutation_service`); residual accessors beyond ADR | Naming map |
| 005 | Accepted | Conforms; §2.1 self-disclaimed already; §2.3 `RuntimeBuildError` sketch (10 variants/String) vs code (12/structured) not disclaimed | Extend existing note to §2.3 |
| 006 | Accepted | Conforms; unused parallel `CommandIntent` disagrees with `CommandPolicy` on `migrate-membership verify` (ReadOnly vs ExclusiveInspection) | Record divergence as open code question |
| 007 | Accepted | Conforms exactly | none |
| 008 | Accepted | Conforms; cosmetic sidecar line-count drift | none |
| 009 | Accepted ("0.9.0 … not yet tagged, published, or distributed") | Taxonomy conforms. Status line was accurate at authoring (2026-09-02); tag created 2026-09-06 [executed]. Published/distributed: still not, per local evidence; external state unknowable | Dated addendum: "tag v0.9.0 created 2026-09-06 (1680f68); publication/distribution remain open (O-13)" — do not edit the original line |

**Unresolved intent (no decision recorded anywhere) — candidates for clearly-labeled retrospective ADRs, if the maintainer wants them on record:** (a) omnibus `Storage` retained as internal implementation vehicle (assessment §13 Q1); (b) multi-instance ref-index strategy (§13 Q2); (c) CLI safety design (FsRootLock+authority) that displaced blob-gc-online.md's thin-client idea; (d) 7-service decomposition displacing the §10.1 four-facade sketch; (e) cross-backend continuation-token standardization deliberately withheld (`PageToken` design note, *(sibling)*) — see gate O-03. None of these may be presented as historical decisions.

## 4. Refactoring ledger

### 4a. Wave 1 — landed as re-scoped by the ADRs; residuals named

The assessment §11 roadmap (6 slices, `src/services/`, four facades) was **re-scoped in flight** by ADRs 001–009 (11 slices, `src/application/`, 7 services). Every ADR-committed slice landed [confirmed]. What did **not** land is the part of assessment §10.1 the ADRs never adopted — and no document records that re-scoping as a decision:

| Intended (source) | Shipped [confirmed] | Residual / not implemented at audited revision |
|---|---|---|
| Storage god-trait → ports (§11 S1; ADR-003) | ports + StorageWiring | Omnibus trait as internal macro vehicle (fact, undecided as policy) |
| Thin handlers via app services (§11 S2; ADR-002/004) | 7 services; all OCI routes delegate | `handlers.rs` 1538-line dispatcher, 59 `state.config` policy reads; **admin GC + /token bypass services** |
| `GarbageCollectionService`, `ProxyService` facades (§10.1) | — | **Not implemented at audited revision**; GcService reached directly by `http_api/admin.rs`; proxy on AppState with only `ProxyTarget` neutralized |
| Gate encapsulation (§11 S3; ADR-001) | ConsistencyCoordinator | Per-root instances (scope narrower than "process-wide" phrasing) |
| Test/mock segregation (§11 S4; ADR-008) | sidecars + tests/support | — |
| Ghost-module cleanup (§11 S5; ADR-007) | 10-line deprecated shim | shim retained deliberately (compat) |
| Typed errors (§11 S6; ADR-009) | StorageErrorKind | publication/distribution open (O-13) |
| AppState narrowing (§10.1 implied) | raw engines removed | AppState still a process bag; whole `Config` into assembly |

### 4b. Wave 2 — landed slices, and the live frontier

Landed [confirmed; commits in notes §4b]: metadata seam → contained reads per family → contained mutation cutover (`f555e5f`) → durability barriers (`8c0ac64`, with an explicitly chosen best-effort carve-out for deletion crash-persistence) → ObjectStore domains phases 3–8 → streaming CAS listing (`FsListingBudgets` removed). Ref-index BUILDING-wedge fix + rebuild serialization (`be34b2e`) verified in source and by tests.

**Three distinct categories — do not conflate:**

**(i) Explicitly deferred (a written deferral exists, in code prose):**

| Item | Deferral record |
|---|---|
| Repository-existence family onto ObjectStore | `fs/tag_listing.rs:13-17`, `tag_domain.rs:112` ("later phase") |
| Membership tree enumeration onto ObjectStore | `membership_domain.rs:22-32` (list_page contract can't express it) |
| `meta/` ready-marker + checkpoint writes; `fs::write_membership_sync` | `membership_domain.rs` out-of-scope list |
| Reaper inspection-read containment follow-up | `upload_quarantine_read.rs:14,330` |

**(ii) Declared non-goals (intentionally retained; not unfinished work):** sled ref-index, S3 ETag conditional deletes, in-process coordinator (assessment §12); zero wire/layout changes (§10.2).

**(iii) Not implemented at the audited revision, with no recorded decision either way [unresolved intent]:** repo-lease containment + no-op `renew` (listed in current-state §4 as residue, but no deferral rationale exists anywhere); EXDEV refusal (REQ-013); 72 h grace wiring (REQ-012); token observability (REQ-006); cert reload/SAN validation (REQ-017); GC/proxy facades (REQ-018). *History note:* pass evidence establishes absence at HEAD; commit history was not exhaustively searched for prior implementations of these — "never implemented" is claimed for none of them.

**Test-only prototype never promoted:** `tests/upload_lifecycle_contained_cleanup_prototype.rs` [confirmed].

### 4c. Superseded proposals vs unresolved intent

**Superseded (replacement landed; supersession evidenced):**

| Proposal | Superseded by | Evidence |
|---|---|---|
| `FsListingBudgets` fixed budgets ("Option A") | streaming top-K listing | removed at `9405991`; cutover doc carries banner |
| `src/services/`, four-facade sketch | `src/application/`, 7 services | ADR-002/004 + code (re-scoping itself unrecorded — §3) |
| Ambient reaper / `list_tag_files` / `AppState.storage` | contained reaper / tag_domain / port views | README "not open work" list + passes A/B |

**Intent unresolved (no landed replacement, no recorded abandonment):** thin-client GC CLI (a *different* safety design shipped; ratification pending), REQ-006/012/013/017 items above, cross-backend token standardization (declined in sibling design note only).

## 5. Acceptance-gate register (from pass F; proposed to become `docs/architecture/acceptance-gates.md`)

**Criterion source unresolved (global):** no in-repo document *defines* the gate numbering. IDs O-01/02/07–12/14 occur nowhere (repo + sibling). The earliest docs assert the gates "retain their existing meanings" (`o-05-filesystem-metadata-containment.md:6`, commit `f7ad9f9` 2026-09-08; O-04/O-15 added at `5f504ac`). The originating register is out of tree. Fullest in-repo definitional tables: `filesystem-read-containment-remaining-gaps.md:576-585`, `filesystem-production-read-cutover.md:234-242`, `post-tag-listing-assessment.md:484-493`, `tag-read-production-readiness-assessment.md:605-614`. **No document anywhere declares any gate closed.** One doc (`filesystem-gc-contained-discovery-production-cutover.md:14-24`) redefines **all eight IDs** to unrelated subjects (hard collision); `filesystem-tag-read-contained-seam.md:25,101,258` redefines O-03. Both need corrective banners before the register below is safe to consume.

| Gate | Criterion (fullest in-repo wording + source) | Implementation evidence at HEAD | Validation evidence / what's missing | Closure requires |
|---|---|---|---|---|
| **O-03** | "Exact validated key and continuation-token contract" (`filesystem-production-read-cutover.md:235`); "cross-backend token standardization … pending" (`cas-listing-production-cutover.md:156`) | Generic listing/token contract now exists: `ObjectKey::parse` grammar, `PageToken` strictly-after semantics, written `list_page` ordering contract *(sibling: storage-core/src/object_store.rs:87-97, 414-441; key.rs:34)*; registry streaming cursor `fs/listing.rs:311-477` [confirmed]. Cross-backend token standardization **deliberately withheld** by design note *(sibling)* — never accepted registry-side | Adapter contract suites run FS/S3/in-memory *(sibling tests)*; in-repo adversarial pagination + grammar suites [test]. Missing: a registry-side acceptance of the grammar/token contract; a recorded decision on the withheld standardization | **Doc act + human decision** (ratify contract; accept or overturn the withholding); fix the two colliding redefinitions |
| **O-04** | "Filesystem write durability, descriptor-relative containment, symlink safety, and crash outcomes" (`production-read-cutover.md:236`); "directory lock containment remain open" (`remaining-gaps.md:579`) | Mutation cutover `f555e5f` + durability barriers `8c0ac64` [confirmed]. Residues [confirmed]: pathname `meta/` writes (`fs.rs:3651`, `:3705`), pathname repo-lease flock (`fs.rs:1341-1373`, no-op renew), `8c0ac64`'s explicitly chosen best-effort deletion crash-persistence (commit message; no accepting document) | Fault-injected barrier tests (`fs/tests.rs:17256+`, dev-only `fault-injection` feature) [test]. Missing: containment/crash tests for `mark_membership_ready`/repo lease; an accepted record of the best-effort carve-out | **Code change** (lease + 2 meta/ writes) **or accepted permanent exception**; plus decision recording the deletion-durability carve-out |
| **O-05** | "Broader filesystem read containment"; closes when the enumerated uncontained-read list is empty; "strict path containment and symlink safety" (`remaining-gaps.md:495-517`, `o-05-…md:12`) | The gate's own 2026-09 uncontained list is now empty at HEAD: every listed family contained (per-family modules + ObjectStore phases) [confirmed]; only surviving ambient calls in `fs/` prod code are writes (O-04) or `#[cfg(test)]`; mechanism = `openat2` RESOLVE_BENEATH/NO_SYMLINKS *(sibling)*. Repo-existence probe contained but backend-specific (open seam, not a containment violation) | Per-family Linux-gated containment tests; traversal-attack tests [test]; lib suite [executed]. Missing: a re-enumeration of the `remaining-gaps.md` list at HEAD finding it empty (no such doc exists — `current-state.md:7` explicitly declines closure); TAG-DEC-03 symlink 404→500 behavior-change sign-off still PENDING | **Verification/doc act + human acceptance** (incl. resolving TAG-DEC-03). Closest to closable; still not "stale docs" — the acceptance artifact does not exist |
| **O-06** | "Typed AWS mapping and genuine pinned-MinIO evidence" (`production-read-cutover.md:238`) | Typed AWS error mapping implemented (`s3.rs:300-304,516-518`; classifier fns) [confirmed] + *(sibling client.rs)*. MinIO pinned in `scripts/start-minio.sh:4`; **compose file uses `:latest` (unpinned)** [confirmed] | Live suite `tests/s3_live_integration.rs` (~35 tests) is **not** ignore-gated except the AccessDenied test (`:2326`, needs scoped creds, mutates process-global env). CI **definition** would run it; with no remote, no execution evidence exists. `conformance-results/` gitignored; only local artifacts are 2026-08-26 (baseline era). **No recorded pinned-MinIO run at any post-refactor revision exists in-repo** | **Verification run + doc act** (record a pinned-MinIO run at HEAD; supervised AccessDenied run per `:2318-2327`); small decision on compose pinning |
| **O-13** | "Permanent repository hosting and release/distribution strategy" (`production-read-cutover.md:239`), incl. "crate publishing … for storage-layer-rust" (`remaining-gaps.md:582`) | **No remotes on either repo [executed]**; siblings `publish = false` *(sibling)*; consumed as path deps (build not reproducible off this machine); `dist/` artifacts gitignored; no release workflow. Local tag v0.9.0 exists [executed] — correcting the stale "not tagged" repetitions (§3) | No strategy document exists anywhere. Missing: everything the criterion names | **Human decision first** (hosting, publish-vs-vendor, path-dep strategy), then infrastructure. Least-advanced gate |
| **O-15** | "Non-Linux filesystem support"; "non-Linux execution is unverified" (`production-read-cutover.md:240,213-214`) | Linux `openat2` is a hard init requirement (`fs.rs:655-659`; `PlatformUnsupported` mapping `fs.rs:385-389`); non-Linux stubs fail closed, written for unix-like targets *(sibling dir.rs/reader.rs cfg arms)*; Windows compilation undemonstrated | CI definition is ubuntu-only; every containment test is `#[cfg(target_os="linux")]` — zero non-Linux coverage by construction. Missing: any non-Linux artifact at all | **Verification run on a non-Linux target** (record fail-closed init) **or an accepted Linux-only ADR** |
| **O-16** | "Earlier Slice 11 audit, test-inventory, and MinIO-report completeness" (`production-read-cutover.md:241`) | Slice 11 = ADR-009 (`49d4054`). `current-code-assessment.md` has "Verified Test Inventory" lines for slices 1–10 (last: 708 tests / 18 binaries) — **none for Slice 11** [confirmed]. HEAD reality: 21 test files; lib 1125/0/13 [executed]; full-suite count unreconciled | Missing exactly what it names: a Slice-11-format inventory + MinIO report. Cheapest gate to close; still requires a run + a written record | **Verification run + doc act** (full suite w/ MinIO, record in the slice-addendum format, reconcile 18→21 binaries) |
| **fs-doc D-06** | "Remains unresolved until extracted implementations, cutover evidence, compatibility assessment, and distribution strategy **are accepted**" (`production-read-cutover.md:242`). Distinct from resolved assessment-D-06 (README.md:14-17 flags the collision) | Clause-by-clause: extraction done; cutover evidence abundant but recorded in stale-stamped docs (3 with zero provenance); compatibility partially assessed with **six TAG-DEC items + decision D7 still PENDING/DEFERRED while the cutovers shipped**; distribution clause = O-13, not started | No partial-credit statement exists in any doc (verified across 25 occurrences). Cannot close before O-13 | **Human acceptance decision, per clause**; master OPEN gate |

## 6. Canonical documentation structure

One home per category; shared items referenced by stable ID (REQ-nnn, gate IDs, ADR numbers), not duplicated status lists. Audit notes remain evidence, not a tracker.

| Category | Canonical home | Status |
|---|---|---|
| Requirements & constraints | `docs/requirements.md` (**new**, seeded from §2) | proposed |
| Current architecture | `docs/architecture/current-state.md` | exists; absorb §1 fixes |
| ADRs & decision history | `docs/architecture/adr-*.md` (+ dated addenda; retrospective ADRs only if accepted) | exists |
| Remaining refactor work + acceptance gates | `docs/architecture/acceptance-gates.md` (**new**, from §5; includes the §4b frontier by reference) | proposed |
| Operational instructions | root `README.md` (user-facing), `docs/gc-operations.md` (**new**), `docs/rbac.md`, `docs/traefik-configuration.md`, `docs/container-testing-guide.md` | mixed |
| Known issues | `docs/known-issues.md` (**new**; absorbs confirmed FIXMEs from ideas.txt/ISSUES.txt/BUG0.txt + audit code findings D-list) | proposed |
| Historical evidence | `docs/architecture/` slice-note corpus (bannered, in place), `current-code-assessment.md` (baseline), `PLAN.md` (bannered), superseded blob-gc pair | exists |
| Entry point / reading order | `docs/README.md` (**new**, one page): requirements → current-state → ADRs → gates/active work → ops → known issues → history | proposed |

Recommended defaults (per approval item A below): historical docs stay **in place** with dated banners (matches the repo's existing self-correction convention; no link breakage); banners follow the ADR-005/G10 model: date, audited HEAD, one-line "what landed where", pointer to current-state/gates by ID.

## 7. Document disposition table (one row per file)

Banner templates: **B-HIST** = dated historical-snapshot banner + landed-commit pointer + "not remaining work; see current-state.md / acceptance-gates.md". **B-PROV** = B-HIST + reconstructed provenance (this file has no date/commit stamps; range recoverable from `git log --follow`). **B-GATE** = corrective banner: "gate table below uses non-canonical redefinitions of O-03…D-06; canonical register: acceptance-gates.md".

| Path | Disposition | Specific change / destination of active content |
|---|---|---|
| docs/architecture/README.md | update | Add category map (§6) + link to requirements/gates/known-issues; extend "not open work" list (consistency_gate symbol, FS-only-GC claim); keep as architecture-index |
| docs/architecture/current-state.md | update | Absorb §1 fixes 1–6; link gates by ID instead of restating §5 gate prose |
| docs/architecture/current-code-assessment.md | update (minimal) | Dated addendum: v0.9.0 tag correction; note §13 Q1/Q2 unanswered; add Slice-11 inventory line only after O-16 run |
| docs/architecture/adr-001…adr-009 (9 files) | keep + addenda | Per §3 column only; ADR-009 addendum re tag date; no rewrites |
| docs/architecture/filesystem-cas-listing-characterization.md | B-HIST | → gates O-03 |
| docs/architecture/filesystem-cas-listing-integration-assessment.md | B-HIST | "not cut over" = authoring-time |
| docs/architecture/filesystem-cas-listing-production-integration-design.md | B-HIST | FsListingBudgets superseded at 9405991 |
| docs/architecture/filesystem-cas-listing-production-cutover.md | keep | banner already present |
| docs/architecture/filesystem-catalog-discovery-production-cutover.md | B-HIST | landed lineage 3b64713/906ef89 |
| docs/architecture/filesystem-gc-contained-discovery-production-cutover.md | **B-GATE + B-HIST** | highest priority: colliding gate table |
| docs/architecture/filesystem-gc-contained-discovery-production-integration-design.md | B-HIST | landed d51ea1a→2fc21aa lineage |
| docs/architecture/filesystem-gc-contained-metadata-design.md | B-HIST | design landed via later slices |
| docs/architecture/filesystem-gc-directory-discovery-seam.md | B-HIST | |
| docs/architecture/filesystem-gc-manifest-discovery-characterization.md | B-HIST | |
| docs/architecture/filesystem-gc-manifest-discovery-integration-design.md | B-HIST | |
| docs/architecture/filesystem-gc-manifest-reference-seam-design.md | B-HIST | |
| docs/architecture/filesystem-gc-manifest-reference-seam.md | B-HIST | |
| docs/architecture/filesystem-gc-repository-discovery-decisions.md | B-HIST | decision rows were authoring-time PENDING; landed via 2fc21aa |
| docs/architecture/filesystem-lifecycle-journal-read-containment.md | B-HIST | "journal writes remain ambient" falsified by f5f9bf7/8c0ac64 |
| docs/architecture/filesystem-manifest-listing-characterization.md | B-HIST | |
| docs/architecture/filesystem-manifest-listing-contained-integration-design.md | B-HIST | |
| docs/architecture/filesystem-manifest-listing-production-cutover.md | B-PROV | no status/date; describes pre-ObjectStore path |
| docs/architecture/filesystem-manifest-listing-production-decisions.md | B-HIST | near-duplicate of readiness-assessment; cross-link |
| docs/architecture/filesystem-manifest-listing-production-readiness-assessment.md | B-HIST | cross-link decisions doc |
| docs/architecture/filesystem-manifest-read-characterization.md | B-HIST | |
| docs/architecture/filesystem-manifest-read-integration-assessment.md | B-HIST | |
| docs/architecture/filesystem-manifest-read-production-cutover.md | B-PROV | no status/date/commit at all |
| docs/architecture/filesystem-manifest-read-production-integration-design.md | B-HIST | |
| docs/architecture/filesystem-membership-read-containment.md | B-HIST | point ops later on membership_domain (6ed8b3e) |
| docs/architecture/filesystem-production-read-cutover.md | keep + note | canonical gate *wording* source; add pointer to acceptance-gates.md |
| docs/architecture/filesystem-quarantine-upload-inspection-containment.md | update (banner exists) | §0 "DEFERRED, not contained" body needs inline pointer; original text preserved as quote |
| docs/architecture/filesystem-read-containment-post-catalog-assessment.md | keep | bannered |
| docs/architecture/filesystem-read-containment-post-journal-assessment.md | keep | bannered |
| docs/architecture/filesystem-read-containment-post-membership-assessment.md | keep | bannered |
| docs/architecture/filesystem-read-containment-post-referrers-assessment.md | keep | bannered |
| docs/architecture/filesystem-read-containment-post-tag-listing-assessment.md | keep | bannered; hosts a fullest gate table — cross-link register |
| docs/architecture/filesystem-read-containment-post-timestamps-assessment.md | keep | bannered |
| docs/architecture/filesystem-read-containment-post-upload-quarantine-assessment.md | keep | bannered |
| docs/architecture/filesystem-read-containment-remaining-gaps.md | keep + note | bannered; canonical O-05 definition + gate table — cross-link register; its §508-516 list is the O-05 closure checklist (active content → acceptance-gates.md) |
| docs/architecture/filesystem-reference-index-sync-hardening-design.md | B-HIST | §5.3 deferrals since implemented; SCHEMA_VERSION=1 claim stale (code=2) |
| docs/architecture/filesystem-referrers-read-characterization.md | B-HIST | |
| docs/architecture/filesystem-referrers-read-contained-integration-design.md | keep | addendum already present |
| docs/architecture/filesystem-referrers-read-production-cutover.md | B-HIST | later on referrer_domain (b1e607c) |
| docs/architecture/filesystem-repository-discovery-characterization.md | B-HIST | |
| docs/architecture/filesystem-tag-listing-characterization.md | B-HIST | references list_tag_files (gone) |
| docs/architecture/filesystem-tag-listing-contained-integration-design.md | B-HIST | same |
| docs/architecture/filesystem-tag-listing-contained-seam.md | B-HIST | same |
| docs/architecture/filesystem-tag-listing-production-cutover-readiness.md | B-HIST | same; supersedes sibling readiness doc in substance — cross-link |
| docs/architecture/filesystem-tag-listing-production-cutover.md | keep | banner already present |
| docs/architecture/filesystem-tag-listing-production-readiness-assessment.md | B-HIST | "promotion blocked" = authoring-time |
| docs/architecture/filesystem-tag-mutation-contained-integration-design.md | B-HIST | chain ended without own cutover; landed via f555e5f + 32c42c6 |
| docs/architecture/filesystem-tag-mutation-write-characterization.md | B-HIST | same |
| docs/architecture/filesystem-tag-read-characterization.md | B-HIST | |
| docs/architecture/filesystem-tag-read-contained-integration-design.md | B-HIST | |
| docs/architecture/filesystem-tag-read-contained-seam.md | **B-GATE + B-HIST** | redefines O-03 (secondary collision) |
| docs/architecture/filesystem-tag-read-production-cutover.md | B-PROV | no status/date/commit |
| docs/architecture/filesystem-tag-read-production-readiness-assessment.md | B-HIST + note | **active content: TAG-DEC-01…06 pending sign-offs → move to acceptance-gates.md (feeds O-05/D-06) before marking historical** |
| docs/architecture/filesystem-timestamps-and-emptiness-containment.md | B-HIST | later on repo_timestamp_domain (84dbe13) |
| docs/architecture/filesystem-upload-lifecycle-contained-cleanup-design.md | keep | bannered |
| docs/architecture/filesystem-upload-lifecycle-contained-cleanup.md | keep | canonical shipped-behavior record |
| docs/architecture/lifecycle-tag-listing-error-hardening.md | B-PROV | no status/date/baseline; landed as 96c0729 |
| docs/architecture/membership-migration-tag-listing-error-hardening.md | B-HIST | landed 5779f7f |
| docs/architecture/membership-migration-tag-listing-hardening-design.md | B-HIST | landed 5779f7f; near-duplicate pair — cross-link |
| docs/architecture/o-05-filesystem-metadata-containment.md | keep + note | earliest gate assertion; cross-link register |
| docs/architecture/o-05-linux-descriptor-metadata-experiment.md | keep | accurate dated record |
| docs/architecture/storage-fs-metadata-integration-assessment.md | B-HIST + note | **active content: decision D7 "Awaiting Acceptance" → acceptance-gates.md (feeds D-06)** |
| docs/architecture/supervisor-tag-listing-error-hardening.md | keep | self-corrected model doc |
| docs/architecture/supervisor-tag-listing-failure-policy-assessment.md | B-HIST | landed 02cfa07 |
| docs/blob-gc.md | B-HIST | historical design; **active safety-model content → new docs/gc-operations.md** |
| docs/blob-gc-online.md | B-HIST | historical design (consistency_gate, 6-step machine, thin-client CLI, 72h grace, EXDEV all diverged); **active items REQ-012/013/014 → requirements.md as Unknown-acceptance rows** |
| docs/gc-operations.md | **create** | Actual behavior: 3 entry points, 5 protection axes, scheduler + membership sweep, kill-switch semantics incl. CLI override, S3 conditional delete, pin TTL reality, `quarantine/gc.lock` name — all with code anchors |
| docs/rbac.md | update | Phase 4: mark "not implemented at audited revision (REQ-006, acceptance unknown)" keeping plan text; document `*`-grant catalog implication |
| docs/harbor-lite-phase2.md | update | Fix two falsified details (trailing-`/`, precedence flag); keep "implemented" status |
| docs/container-testing-guide.md | keep | add date stamp |
| docs/traefik-configuration.md | keep | add date stamp |
| docs/requirements.md | **create** | §2 register |
| docs/architecture/acceptance-gates.md | **create** | §5 register + §4b frontier by reference + TAG-DEC/D7 carried items |
| docs/known-issues.md | **create** | Confirmed items with anchors: TLS no-reload/no-SAN-check; S3 proxy-cache eviction gap; dead `blob_gc_finalize_grace_secs`; dead `AuthMetrics`; CLI kill-switch override; `CommandIntent` divergence; application→http_api import edge; `quarantine/.lock` comment vs `gc.lock`; token rate limit env-only/untested; Dockerfile missing storage-layer staging; packaged etc/ config issues (see approval note on secrets); README defects list |
| docs/README.md | **create** | one-page entry point (§6 order) |
| PLAN.md | B-HIST | banner: 2026-01 MVP plan, historical requirement source; **active requirement content → requirements.md (REQ-001…009 rows cite it)** |
| README.md (root) | update | Fix 12 confirmed defects (notes §5.5): remove PUSH_AUTH_MODE family, add `auth.strategy` + `anonymous_pull`, fix /v2/ 401 claim, fix blob-gc-S3 claim, add 3 missing subcommands, repair empty env section, document limits/timeouts knobs, note S3 cache-eviction + systemd divergence |
| BUG0.txt | merge-candidate → known-issues.md | content: one buildx note |
| ISSUES.txt | merge-candidate → known-issues.md | multi-platform listing question |
| ideas.txt | merge-candidate → known-issues.md | TLS FIXMEs (confirmed) + open questions; "groups like harbor" item is done — note that |
| VERSION | keep | (not documentation; Makefile sync target uses it) |

**Recorded but NOT blocking documentation work (no approval needed now):** `etc/registry-rust/registry.core.toml` and `registry.auth.toml` contain environment-specific endpoints, an ACME authorization string, `debug=true`, an unknown `[auth.push] mode` key (hard error under strict mode), and an Argon2id password hash for a `*`-push-grant user — packaged as conffiles. `tls/` (incl. `old/`, `old2/`) and `certs/` contain PEM private-key files. **Paths recorded; values not reproduced; liveness/validity not verified.** These are packaging/security decisions, tracked in known-issues.md, independent of doc reconciliation.

## 8. Refactoring status report — what remains unfinished and why

There is no single "stopping point." The evidence supports this composite:

- **Completed and production-wired [confirmed]:** Wave 1 as re-scoped by ADRs 001–009; Wave 2 through streaming CAS listing. All OCI routes flow through the seven services and port views; enumerated FS read families, mutations, GC discovery, and the reaper are descriptor-contained; six storage families are backend-neutral. Lib suite green at HEAD [executed].
- **The live frontier (explicitly deferred, §4b-i):** the ObjectStore listing contract cannot express hierarchical enumeration, so the repository-existence and membership-enumeration families stopped at contained-but-backend-specific seams; `meta/` writes, the blocking membership writer, and a reaper read follow-up are recorded as out of scope of the landed phases. This is the most precise "where it stands."
- **Divergence with no recorded decision (§4b-iii):** repo lease, EXDEV, 72 h grace, token observability, cert reload/SAN, GC/proxy facades — each is *not implemented at the audited revision*; whether each was dropped or merely not reached is [unresolved].
- **Acceptance debt:** all eight gates OPEN by design of the review process; several are now closable by runs + records rather than code (§5), but none has its closure artifact.
- **Evidence debt:** run artifacts (`conformance-results/`, `dist/`) are gitignored and stale (2026-08-26); no remote means no CI execution evidence; the only at-HEAD execution evidence is this audit's lib run.
- **Documentation debt:** ~35 un-bannered snapshot docs assert authoring-time state as current; one doc redefines all gate IDs; three cutover records have no provenance; two root-level GC design docs describe a superseded design.

The "refactoring stopped partway" impression = frontier (real) + divergences (real, undecided) + acceptance/evidence/doc debt (large, fixable without code).

## 9. Approval choices

**Blocking documentation reconciliation (answer these):**

- **A. Historical-banner mechanics** — Recommended default: in-place dated banners (B-HIST/B-PROV/B-GATE per §7), no file moves. Alternative: relocate to `docs/architecture/history/`. *(Default requires no further input.)*
- **B. New canonical docs** — approve creating `docs/requirements.md`, `docs/architecture/acceptance-gates.md`, `docs/known-issues.md`, `docs/gc-operations.md`, `docs/README.md` per §6. Recommended: yes, all five; they are the homes the disposition table routes active content into.
- **C. Canonical-doc corrections** — approve the §1 precision fixes to `current-state.md`/`architecture/README.md` and the dated ADR addenda of §3 (including the ADR-009 tag-date addendum). Recommended: yes.
- **D. Root README fixes** — the 12 confirmed defects are user-facing behavior descriptions; approve fixing now, or defer to a separate pass. Recommended: fix now (they are factual, code-anchored).
- **E. blob-gc pair + PLAN.md + .txt trackers** — approve B-HIST banners + content migration into gc-operations.md / requirements.md / known-issues.md, keeping originals in place. Recommended: yes.

**Explicitly NOT blocking — recorded in known-issues.md / requirements.md as unresolved, requiring no decision today:** REQ-006/012/013/014/020 acceptance; code cleanups (dead knob/counters, CommandIntent, import edge); packaging/secrets items; container-build strategy; gate closures themselves (O-13/D-06 etc.); retrospective ADRs (remain proposals); whether to commit `docs/audit/*`.
