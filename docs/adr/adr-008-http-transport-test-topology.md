# ADR-008: HTTP Transport Test Topology and Production Visibility Preservation

* **Status:** Accepted
* **Date:** 2026-09-01
* **Authors:** Senior Software Architect (OCI Distribution & Storage Systems)
* **Scope:** Delivery Layer Test Topology, File-Backed Sidecar Unit Modules, Production Visibility Preservation Invariant, Separation of White-Box Unit vs. Black-Box Integration Tests
* **Refines:** `docs/architecture/adr-001-first-refactoring-boundary.md`, `docs/architecture/adr-002-application-service-boundary.md`, `docs/architecture/adr-003-storage-capability-ports.md`, `docs/architecture/adr-004-application-read-services.md`, `docs/architecture/adr-005-server-runtime-composition-root.md`, `docs/architecture/adr-006-cli-runtime-composition.md`, `docs/architecture/adr-007-manifest-compatibility-consolidation.md`, `docs/architecture/current-code-assessment.md`

---

## 1. Context & Problem Statement

Following the thinning of HTTP handlers (ADR-002) and read service encapsulation (ADR-004), [`src/http_api/handlers.rs`](file:///home/dietmar/devel/rust/registry-rust/src/http_api/handlers.rs) contained an inline test block (`mod tests { ... }` at lines 596–2110, totaling 1,515 lines) containing 46 unit test functions and extensive mock fixtures, embedded in the middle of production handler functions (which continued after line 2110).

When addressing technical debt item **D-04 ("In-Source Test Footprint Bloat")**, a critical architectural choice arises:
1. **The Anti-Pattern (External Integration Migration):** Blindly moving all inline tests to an external integration binary under `tests/` (e.g. `tests/http_transport_tests.rs`). Because downstream integration binaries compile the crate as an external dependency, white-box tests that exercise internal helper methods (token scope parsing, HMAC upload state tokens, repo validation) or construct test states (`AppState::new_test`) would require **widening production visibility** (changing `private` or `pub(crate)` items to `pub`, or leaking `#[cfg(test)]` constructors into release builds).
2. **The Correct Architectural Pattern (File-Backed Sidecar Unit Modules):** Keeping white-box unit tests within their parent module's privacy scope by extracting them into a dedicated file-backed child module (`src/http_api/handlers/tests.rs`) declared via `#[path = "handlers/tests.rs"] mod tests;`.

---

## 2. Decision: File-Backed Sidecar Unit Modules & Visibility Invariants

### 2.1 Repository Test Topology Standard

We establish the following permanent repository test architecture policy:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                 TEST TOPOLOGY RULES                                    │
│                                                                                        │
│  1. White-Box Unit Tests:                                                              │
│     - Tests that exercise private helpers, internal state, or `#[cfg(test)]` fixtures  │
│     - MUST remain unit tests within the module's legitimate privacy scope              │
│     - Extracted into file-backed sidecars (`src/<module>/tests.rs` or                  │
│       `src/<module>/<submodule>/tests.rs`) using `#[path = "..."] mod tests;`         │
│                                                                                        │
│  2. Black-Box Protocol Tests:                                                          │
│     - Tests that exercise public HTTP boundaries, wire protocols, and CLI binaries      │
│     - Live in workspace integration binaries under `tests/*.rs`                        │
│                                                                                        │
│  3. Production Visibility Preservation Invariant:                                      │
│     - Production visibility (private, pub(crate), pub) MUST NEVER be widened           │
│       merely to satisfy external integration tests                                     │
│     - `#[cfg(test)]` constructors MUST NOT leak into production release builds          │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Implementation in `src/http_api/handlers.rs`

The monolithic inline `mod tests { ... }` block (lines 596–2110) in `src/http_api/handlers.rs` is replaced with exactly one file-backed declaration:

```rust
#[cfg(test)]
#[path = "handlers/tests.rs"]
mod tests;
```

The 46 unit tests and their supporting fixtures (`with_admin_creds`, `admin_headers_ok`, `test_app_state`, etc.) are placed in [`src/http_api/handlers/tests.rs`](file:///home/dietmar/devel/rust/registry-rust/src/http_api/handlers/tests.rs) (1,495 lines). Production handlers continue uninterrupted from line 599 through line 1538 in [`src/http_api/handlers.rs`](file:///home/dietmar/devel/rust/registry-rust/src/http_api/handlers.rs).

Because `src/http_api/handlers/tests.rs` is declared as `mod tests` within `http_api::handlers`, the compiled test namespace remains completely identical:
```text
http_api::handlers::tests::<test_name>
```

---

## 3. Consequences & Verification Impact

### Positive
1. **Source File Separation:** Extracted 1,514 lines from `src/http_api/handlers.rs` (3,050 lines -> 1,538 lines of pure production routing and handler dispatch logic), creating `src/http_api/handlers/tests.rs` (1,495 lines).
2. **Zero Visibility Leaks:** Zero functions, structs, enums, or constructors had their visibility widened.
3. **Zero Test Identity / Count Delta:** Exactly 46 compiled handler unit tests remain in `unittests src/lib.rs`. Total workspace test count remains unchanged at **708 tests across 18 binaries**.
4. **Clean Production Separation:** Release builds (`cargo build --release`) completely exclude the test sidecar file without any macro leakage.

---

**Claim-verification note (2026-09-19, source-checked at `2718bc16`):** the sidecar declaration, privacy scope, and test identity hold exactly — still precisely 46 handler unit tests, and `src/http_api/handlers.rs` is still 1,538 lines as §3.1 states. The sidecar has since grown from the 1,495 lines recorded here to 1,525 lines (later tests edited in place; count unchanged). The "708 tests across 18 binaries" totals are authoring-time figures.
