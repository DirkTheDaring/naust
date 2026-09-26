# Documentation

Entry point for `naust` documentation. **Audited code revision for all canonical documents: `master` @ `2718bc16`**, with a targeted 2026-09-26 amendment for the ADR-010 `naust-core` crate split (architecture README §1–§3/§6, ADR-010, technical-debt KI-07/KI-26/KI-27 — other documents were not re-audited). One canonical home per concern; everything links by stable ID (`REQ-nnn`, `KI-nn`, `GATE-…`, ADR number) — status is stated only in the owning register.

| Read this | For |
|---|---|
| [`requirements.md`](requirements.md) | Canonical requirements & constraints (REQ-001…023): statement, original source, acceptance / implementation / verification tracked separately |
| [`architecture/README.md`](architecture/README.md) | Current architecture: components, boundaries, layering state, storage-cutover state, key runtime flows (with diagrams) |
| [`architecture/data-model.md`](architecture/data-model.md) | Logical data entities, ownership, and consistency mechanisms |
| [`adr/README.md`](adr/README.md) | Accepted decisions ADR-001…009 (index + records with dated addenda; rationale never rewritten) |
| [`technical-debt.md`](technical-debt.md) | Canonical remaining work: acceptance gates (GATE-…), open decisions (TAG-DEC, D7), implementation gaps & defects, architectural debt, missing verification evidence, unresolved requirements, packaging concerns (KI-01…KI-26) |
| [`operations.md`](operations.md) | Operating the registry: GC, identity/RBAC & token playbooks, TLS/ACME caveats, proxy cache, deployment cautions. Companion guides: [`traefik-configuration.md`](traefik-configuration.md), [`container-testing-guide.md`](container-testing-guide.md) |
| root [`../README.md`](../README.md) | Running locally, compose/packaging recipes, configuration & env-var reference |

**Suggested reading order for a new developer or AI:** architecture README → data model → requirements → technical debt → operations; consult ADRs when you need the *why* behind a boundary. **[`technical-debt.md`](technical-debt.md) is the single authoritative source for unfinished work** — every open gate, issue, deferred item, and undecided question lives there (or in `requirements.md` for acceptance status); no other document states such status.

[`outdated/`](outdated/README.md) holds archived historical material (superseded plans, per-slice refactoring notes, old assessments, audit transcripts) with an index mapping old paths to canonical replacements. **It is historical evidence only — never read current status, remaining work, or behavior from it.** The main reading path above never requires it.
