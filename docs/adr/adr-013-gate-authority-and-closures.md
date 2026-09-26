# ADR-013: Gate Authority, Gate Closures, and Retroactive Ratifications

* **Status:** Accepted (2026-09-26)
* **Scope:** Acceptance-gate governance; closure of GATE-O03/O04/O05/O06/O15/O16/FSD06; ratification of TAG-DEC-01…06 and the D7 metadata recommendation; disposition of REQ-013/REQ-014

## 1. Gate authority (good-practice assessment)

The gates were inherited from an out-of-tree review process with no recorded authority. Good practice for a single-maintainer open-source project is explicit, lightweight governance: **the repository maintainer (`DirkTheDaring`) is the gate, release, and decision authority**, exercising it through recorded decisions in this ADR series; nothing closes silently. If the project gains co-maintainers, authority questions move to PR review and this ADR gains an addendum. This replaces the register's "Authority: UNRESOLVED" state.

## 2. Gate closures (each per its proposed criteria)

| Gate | Decision |
|---|---|
| GATE-O03 | **CLOSED.** The `ObjectKey` grammar / `PageToken` strictly-after semantics / `list_page` ordering rules (sibling `storage-core`) are hereby ratified as *the* registry listing contract. The withheld cross-backend token standardization is **accepted as withheld**: tokens remain backend-scoped by design. |
| GATE-O04 | **CLOSED with accepted permanent exceptions:** the `meta/` pathname writes and blocking membership sync writer (deferral recorded in `membership_domain.rs`), the repo-lease flock with no-op `renew` (rationale recorded at the impl, KI-12), and the `8c0ac64` best-effort crash persistence for deletions (accepted: deletion durability is bounded by GC's re-scan; a lost deletion re-quarantines on the next run). |
| GATE-O05 | **CLOSED.** The 2026-09-19 audit re-enumeration (empty uncontained-read list) is accepted as the record; TAG-DEC-03 is decided below. |
| GATE-O06 / GATE-O16 | **CLOSED.** Satisfied by the committed evidence convention: `evidence/2026-09-26-debt-remediation/` (all four conformance matrices exit 0, live-MinIO suite green, JUnit artifacts committed at stated revisions). |
| GATE-O15 | **CLOSED as Linux-only.** `FsStorage` requires Linux `openat2(RESOLVE_BENEATH…)` and fails closed elsewhere; non-Linux hosts are explicitly unsupported for the filesystem backend (S3 backend portability is untested and unclaimed). |
| GATE-FSD06 | **CLOSED.** Its clauses are covered by the closures above plus ADR-009/010/011; distribution/compatibility clauses by ADR-012 and the release records to follow. |

## 3. Retroactive ratifications

* **TAG-DEC-01…06:** the tag-read containment cutover shipped and has been continuously verified since (conformance, live-MinIO, adversarial suites). All six pending markers are **ratified as shipped**. Specifically **TAG-DEC-03**: a dangling-symlink tag read returns **500 (fail-closed), not 404** — storage corruption must not masquerade as absence. Accepted as the intended contract (regression-tested).
* **D7 metadata-seam recommendation:** the shipped `storage-fs` metadata-seam design is **accepted**.
* **REQ-013** (refuse cross-device EXDEV online quarantine): **dropped** — the shipped two-phase quarantine keeps the quarantine tree inside the same filesystem root, so the guarded scenario does not arise.
* **REQ-014** (GC CLI as thin admin-API client): the **shipped alternative is ratified** — the CLI opens storage directly under `FsRootLock` + `RuntimeMutationAuthority` (ADR-006), which is strictly safer offline than depending on a live server.
