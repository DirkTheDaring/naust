# Archive of superseded documentation (`docs/outdated/`)

**This directory is historical evidence. Nothing in it is authoritative for current behavior, current status, or remaining work.** The canonical set is: [`../README.md`](../README.md) (entry point), [`../requirements.md`](../requirements.md), [`../architecture/README.md`](../architecture/README.md) + [`../architecture/data-model.md`](../architecture/data-model.md), [`../adr/`](../adr/README.md), [`../technical-debt.md`](../technical-debt.md), [`../operations.md`](../operations.md). Nothing was deleted; unique active content (open decisions TAG-DEC-01…06 and D7, deferred work, requirement sources, GC safety model, RBAC playbooks) was transferred to the canonical set before archiving — the technical-debt register preserves all KI-/GATE- IDs.

Most files carry dated banners (2026-09-19) stating what landed where; the two files that redefine gate IDs carry explicit GATE WARNING banners. Documents here retain their authoring-time status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "blocked", pending decisions) — those reflect the moment of writing, not the tree.

## Path map (old → archived) and canonical replacement

| Old path | Archived at | Canonical replacement |
|---|---|---|
| `docs/architecture/<any file except adr-*>` | `outdated/architecture/<same name>` | see groups below |
| `docs/architecture/adr-00N-*.md` | **not archived** — moved to `docs/adr/` (still active) | [`../adr/README.md`](../adr/README.md) |
| `docs/blob-gc.md`, `docs/blob-gc-online.md` | `outdated/` (same names) | [`../operations.md`](../operations.md) §1 + REQ-010/011/012/013/014 |
| `docs/rbac.md` | `outdated/rbac.md` | invariants → REQ-005; schema + playbooks → [`../operations.md`](../operations.md) §2; unbuilt Phase 4 → REQ-006/KI-04 |
| `docs/harbor-lite-phase2.md` | `outdated/harbor-lite-phase2.md` | REQ-007 + [`../operations.md`](../operations.md) §2 (two corrected claims noted in-file) |
| `PLAN.md` (repo root) | `outdated/root/PLAN.md` | [`../requirements.md`](../requirements.md) (REQ-001…009 cite it) |
| `BUG0.txt`, `ISSUES.txt` (repo root) | `outdated/root/` | KI-16, KI-15 in [`../technical-debt.md`](../technical-debt.md) |
| `docs/audit/*` (2026-09-19 audit notes + reconciliation proposal) | `outdated/audit/` | evidence base cited by the canonical set; not a status source |
| `docs/known-issues.md`, `docs/architecture/acceptance-gates.md`, `docs/gc-operations.md` | **not archived** — transient 2026-09-19 intermediates (never committed), fully merged into [`../technical-debt.md`](../technical-debt.md) (all KI/GATE IDs preserved) and [`../operations.md`](../operations.md) §1 | those two files |
| `ideas.txt` (repo root, gitignored) | left in place (untracked by design) | KI-01 + resolved notes in technical-debt §7 |

## Groups within `outdated/architecture/` (67 files)

| Group | Files | Canonical replacement |
|---|---|---|
| Living inventory (superseded) | `current-state.md` | [`../architecture/README.md`](../architecture/README.md) |
| Baseline assessment + slice addenda | `current-code-assessment.md` | architecture README (state), technical-debt §4/§6 (residuals, Q1/Q2), ADR index (slice history) |
| Old doc index / staleness key | `README.md` | this index + [`../README.md`](../README.md) |
| Gate wording sources | `filesystem-production-read-cutover.md`, `filesystem-read-containment-remaining-gaps.md` | technical-debt §1 quotes them verbatim; status only there |
| CAS listing chain (4) | `filesystem-cas-listing-{characterization,integration-assessment,production-integration-design,production-cutover}.md` | architecture README §4; GATE-O03 |
| Manifest read chain (4) | `filesystem-manifest-read-{characterization,integration-assessment,production-integration-design,production-cutover}.md` | architecture README §4 |
| Manifest listing chain (5) | `filesystem-manifest-listing-{characterization,contained-integration-design,production-decisions,production-readiness-assessment,production-cutover}.md` | architecture README §4 |
| Tag read chain (5) | `filesystem-tag-read-{characterization,contained-integration-design,contained-seam,production-readiness-assessment,production-cutover}.md` | architecture README §4; TAG-DEC items → technical-debt §2 |
| Tag listing chain (5+4) | `filesystem-tag-listing-*` (5), `lifecycle-tag-listing-error-hardening.md`, `membership-migration-tag-listing-{hardening-design,error-hardening}.md`, `supervisor-tag-listing-{failure-policy-assessment,error-hardening}.md` | architecture README §4 |
| Tag mutation pair (2) | `filesystem-tag-mutation-{write-characterization,contained-integration-design}.md` | architecture README §4 |
| Referrers chain (3) | `filesystem-referrers-read-{characterization,contained-integration-design,production-cutover}.md` | architecture README §4 |
| GC discovery chain (10) | `filesystem-gc-*.md`, `filesystem-repository-discovery-characterization.md` | operations §1, architecture README §5.2; GATE WARNING on the contained-discovery cutover file |
| Read-containment records & assessments (13) | `filesystem-{catalog-discovery,timestamps-and-emptiness,membership-read,lifecycle-journal-read,quarantine-upload-inspection,production-read}-*.md`, `filesystem-read-containment-post-*-assessment.md` (7) | architecture README §4; GATE-O05 |
| Upload lifecycle pair (2) | `filesystem-upload-lifecycle-contained-cleanup{,-design}.md` | architecture README §5.1; KI-25 |
| O-05 / metadata era (3) | `o-05-filesystem-metadata-containment.md`, `o-05-linux-descriptor-metadata-experiment.md`, `storage-fs-metadata-integration-assessment.md` | GATE-O05; D7 → technical-debt §2 |
| Ref-index hardening (1) | `filesystem-reference-index-sync-hardening-design.md` | architecture README §5.1 (its deferrals since implemented) |

Provenance: all files were moved with `git mv` from their original paths at revision `2718bc16`; per-file authoring provenance (baseline commits, dates) is inside each file or its banner.
