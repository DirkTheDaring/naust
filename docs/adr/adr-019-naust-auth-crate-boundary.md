# ADR-019: `naust-auth` Crate Boundary — Pure Authentication & RBAC Primitives

* **Status:** Accepted
* **Date:** 2026-09-27
* **Authors:** Senior Software Architect
* **Scope:** Crate topology, security domain model, credential verification, RBAC evaluation, token issuance & verification, transport-free invariant, dependency posture, stability posture.
* **Refines:** ADR-010 (`naust-core` boundary), ADR-014 (auth trust boundaries), ADR-015 (catalog credential matching).

---

## 1. Context & Problem Statement

Following ADR-010, `naust-core` is established as transport-, auth-, and config-free. As a result, all authentication, password hashing, token issuance, and RBAC matrix evaluations remained inside the binary crate `naust` (`src/`).

Within `src/`, authentication functions (`verify_direct_basic_access`, `decide_token_scopes_for_request`, `is_authenticated`) took references to the monolithic server `Config` (which includes storage, S3, TLS, and network options) and mixed Axum HTTP extractors (`HeaderMap`, `Authorization<Basic>`, `StatusCode`) with core cryptographic operations and policy decisions.

## 2. Decisions

### 2.1 Topology
Cargo workspace member: `vendor/naust-auth` (library) consumed by `naust` (server/CLI binary).
`naust-auth` depends on `naust-core` exclusively for canonical repository naming types (`CanonicalRepoName`).

### 2.2 Transport-Free and Pure Compute Invariant
`naust-auth` contains **zero HTTP dependencies** (`axum`, `http`, `hyper`), **zero network I/O** (`reqwest`), and **zero filesystem access**.
All authentication decisions, token signing/verification, Argon2id password hashing, and RBAC evaluations are deterministic CPU-bound computations.

### 2.3 Policy Configuration Types
`naust-auth` defines its own domain configuration structures:
- `AuthConfig`: Contains authentication strategy, anonymous pull policies, push credentials, and RBAC definitions.
- `TokenConfig`: Contains token service names, issuer, audience, TTL, and HMAC signing keyrings.
- `RobotAccountsConfig`, `UserAccountsConfig`, `GroupConfig`: Contain credential hashes and grant rules.

The server's `Config` maps into `AuthConfig` via `From<&Config>` implementations.

### 2.4 Module Partition
- **`naust-auth`:** Owns `rbac`, `token` (claims, signer, verifier, keyring, rate limit), `credentials` (Argon2id password hashing, sentinel hash timing defense, basic auth match), and `policy` (pure decision engine).
- **`naust` (Server):** Owns Axum middlewares, header extraction, `WWW-Authenticate` formatting, and HTTP error response mapping.

## 3. Non-Goals
- No changes to token claims wire format or OCI RBAC specification.
- No changes to storage or manifest lifecycle code.

## 4. Consequences
- Cryptographic code and security logic are hermetically isolated for independent auditing.
- Auth tests execute in milliseconds without spinning up HTTP servers or temp directories.
- Third-party tools and CLIs can embed `naust-auth` without server dependencies.
