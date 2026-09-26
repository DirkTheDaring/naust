# Architecture decision records

Accepted decisions, in order. **Naming note (2026-09-26, ADR-012):** the product was renamed `registry-rust`→`naust` and `registry-core`→`naust-core`; ADR-001…011 retain the pre-rename names and paths as historical records. **All nine ADRs were claim-verified against the source at `2718bc16` (2026-09-19):** decision end-states hold throughout; per-file "Claim-verification note" sections on ADR-001/003/005/008/009 record the handful of literal drifts found (a constructor signature, two renamed/removed helper names, a never-named `ProxyService` symbol, authoring-time line counts, one post-ADR error variant). IDs and historical rationale are preserved; each file may carry dated *reconciliation addenda* (2026-09-19) noting where the implementation has since moved — addenda never rewrite the original decision. Implementation status does not retire a decision record. No ADR is currently superseded; candidate *retrospective* ADRs (unratified) are listed in [`../technical-debt.md`](../technical-debt.md) §6 and must not be treated as accepted decisions.

| ADR | Decision (one line) | Status | Addendum |
|---|---|---|---|
| [001](adr-001-first-refactoring-boundary.md) | First refactoring slice = `ConsistencyCoordinator` encapsulation, not storage-trait segregation | Accepted | scope note (per-root coordinator) |
| [002](adr-002-application-service-boundary.md) | Application mutation services; thin HTTP mutation handlers | Accepted | §3.5 superseded by ADR-004; method-name coexistence; KI-07 |
| [003](adr-003-storage-capability-ports.md) | Segregate omnibus `Storage` into capability ports; migrate production consumers | Accepted | AppState reader views later removed by ADR-004; omnibus-as-vehicle fact; Q1 open |
| [004](adr-004-application-read-services.md) | Five application read/query services; proxy encapsulation | Accepted | field-name map; residual accessors |
| [005](adr-005-server-runtime-composition-root.md) | `src/runtime.rs` as server composition root; narrow supervisor | Accepted | §2.1 self-corrected in-file; §2.3 error-enum drift noted |
| [006](adr-006-cli-runtime-composition.md) | `MaintenanceRuntime` + `CommandPolicy` safety taxonomy | Accepted | KI-06 (`CommandIntent` divergence) |
| [007](adr-007-manifest-compatibility-consolidation.md) | 10-line deprecated `manifest_publication` re-export shim | Accepted | — (conforms exactly) |
| [008](adr-008-http-transport-test-topology.md) | File-backed `#[cfg(test)]` sidecar test modules | Accepted | — |
| [009](adr-009-structured-storage-error-taxonomy.md) | Structured `StorageErrorKind`; 0.9.0 release boundary | Accepted | tag `v0.9.0` created 2026-09-06 (after authoring); publication open → GATE-O13 |
| [010](adr-010-naust-core-crate-boundary.md) | `naust-core` crate boundary: workspace split, proxy `UpstreamFetcher` seam, core-owned policy types, allowlist gate | Accepted | §2.1 layout addendum (root package stays at repo root) |
| [011](adr-011-service-census-and-server-decomposition.md) | Service census (7 core + 2 server); `/token` + admin-GC facades; handler decomposition; KI-26 residues recorded | Accepted | — |
| [012](adr-012-product-naming-naust.md) | Product renamed **Naust**; crates `naust`/`naust-core`; MIT license; historical docs keep old names | Accepted | — |
| [013](adr-013-gate-authority-and-closures.md) | Gate authority = maintainer; GATE-O03/04/05/06/15/16/FSD06 closed; TAG-DEC-01…06 + D7 ratified; REQ-013 dropped, REQ-014 ratified | Accepted | — |

Context: current architecture [`../architecture/README.md`](../architecture/README.md) · requirements [`../requirements.md`](../requirements.md) · remaining work [`../technical-debt.md`](../technical-debt.md).
