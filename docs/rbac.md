# RBAC / Harbor-style robots, users & groups

This document captures a security-first, reviewable approach to Harbor-style authorization for `registry-rust`.

## Why this exists

Harbor’s user/group concept is fundamentally **multi-tenant RBAC** for registries:
- identity (human/robot)
- membership (group/project)
- authorization (role -> actions on a namespace)
- token issuance (mint scoped Bearer tokens)

In an OCI distribution registry, the most security-critical part is **token minting**: once a token is issued, it authorizes all subsequent `/v2/...` requests until expiry.

Goal: add the **security value** (least privilege, revocation, auditability) without introducing a sprawling identity system that becomes a larger attack surface than the registry.

## Non‑negotiable security invariants

1. **Deny by default**
   - If a request cannot be attributed to a subject (robot/user), mint no token.
   - If a scope cannot be evaluated, mint no token.

2. **Never grant more than requested**
   - Granted scopes MUST be a subset of requested scopes.

3. **Never grant more than policy allows**
   - Granted scopes MUST be a subset of what the policy allows for that subject.

4. **Deterministic matching**
   - Authorization matching MUST be deterministic and reviewable.
   - Prefer prefix matching (`repo.starts_with(prefix)`) over regex/globs.

5. **No implicit wildcards**
   - `*` in policy should be explicit (and discouraged).
   - If a repo prefix is empty or malformed, treat it as invalid and deny.

6. **Canonicalize before authorize**
   - Normalize repo names and actions before comparison (trim, lowercase where applicable).
   - Reject invalid repo names early.

7. **Token binding**
   - Tokens MUST be bound to the correct `service`/audience.
   - Tokens MUST have short TTL with an enforced maximum.

8. **No secrets in logs**
   - Never log passwords, robot secrets, or full tokens.
   - Logging should capture subject id + requested scopes + granted scopes + deny reason.

9. **Small trusted computing base (TCB)**
   - The authorization decision logic should be in a small pure module with heavy unit tests.
   - HTTP handlers should be thin wrappers that authenticate + call the policy engine.

## Scope model (recommended v1)

Start with **robot accounts + repo-prefix ACL**. This yields most of Harbor’s security value with minimal new surface area.

Entities:
- **Subject**: `robot:<name>`
- **Grant**: `{ repo_prefix, actions }` where actions ⊆ {`pull`,`push`,`delete` (optional)}

Notes:
- Treat `delete` as a separate feature gate. If delete endpoints are not supported, do not include `delete` in the model.
- Prefer one robot per workload (CI pipeline, deployer, mirror).

## TOML configuration schema (proposal)

This is intentionally flat and reviewable.

```toml
# Implemented: robot accounts and scoped grants.
#
# [auth.robots]
# enabled = true
#
# [[auth.robots.accounts]]
# name = "ci"
# # Hash (Argon2id) of the robot secret (never store plaintext in config).
# secret_hash = "$argon2id$v=19$m=19456,t=2,p=1$..."
#
# # Scopes allowed for this robot.
# # Matching rule: repository name must start with repo_prefix.
# grants = [
#   { repo_prefix = "org1/", actions = ["pull","push"] },
#   { repo_prefix = "library/", actions = ["pull"] },
# ]
#
# # Optional: cap token TTL for this robot.
# max_ttl_secs = 600
```

Implemented Phase 2 (users + groups) uses the same grant format, but assigns grants via group membership:

```toml
[auth.users]
enabled = true

[[auth.users.accounts]]
name = "alice"
secret_hash = "$argon2id$v=19$m=19456,t=2,p=1$..."
groups = ["devs"]

[[auth.groups]]
name = "devs"
grants = [
   { repo_prefix = "org1/", actions = ["pull","push"] },
]
```

Design constraints:
- avoid regex in v1 (prefix is enough for most org layouts)
- explicit actions only
- `repo_prefix` should typically end with `/` (enforceable)

## Implementation outline (historical)

### Phase 1 — Pure policy engine (reviewability first)

Create `src/authz.rs` (or `src/rbac.rs`) with pure functions:
- `parse_requested_scopes(str) -> Vec<ScopeRequest>` (normalize + validate)
- `allowed_scopes(subject, requested, policy) -> Vec<GrantedScope>`

Properties:
- returned scopes are always subsets of requested
- returned scopes are always subsets of policy
- stable ordering and deterministic decisions

### Phase 2 — Robot authentication for token endpoint

Add authentication to the token endpoint:
- Basic auth on the token endpoint, mapping username to robot name.
- Verify provided secret against `secret_hash` using Argon2id.

Hardening:
- rate limit token endpoint
- reject empty/oversized auth headers

### Phase 3 — Token minting hardening

- enforce `service` binding
- enforce maximum TTL
- include `sub`, `iat`, `exp`, `aud/service`
- keep signing and verification centralized in `src/security.rs`

### Phase 4 — Observability and rollout

- log decisions (subject, requested, granted, deny reason)
- staged rollout:
  1) enable robots for pull-only namespaces
  2) enable push for narrow prefixes
  3) expand after validating logs/metrics

## Reviewer & rollout checklist

### Security invariants

- Granted scopes are the intersection of requested and allowed policy.
- Policy evaluation is deny-by-default (invalid policy or unknown action => no grant).
- Prefix matching boundaries are correct (no `org/` -> `org2/...` bleed).
- Tokens are service-bound (`aud == token_service`) and TTL-bounded (`exp - iat <= token_ttl_secs`).
- Tokens contain `iss`, `aud`, `iat`, `exp` and `jti` for correlation.

### Config + operational safety

- Strict config parsing enabled in production (`BEST_PRACTICE=1` or `[config].strict=true`).
- Robot secrets are stored only as Argon2id hashes (no plaintext in config or env vars).
- `token_signing_key` is long, random, and treated as a secret (rotate on compromise).
- `token_ttl_secs` is short (minutes, not hours) to bound blast radius.

### Logging hygiene

- No secrets in logs: no Basic credentials, no secret hashes, no full bearer tokens.
- Token flow emits structured events:
   - `token_issued` (info)
   - `token_denied` (warn)
   - `token_error` (error)
   - `token_decision` (debug; includes requested/granted scopes)

### Staged rollout plan

1) Deploy with robots enabled but only granting pull (or only for a narrow prefix).
2) Validate logs for `token_denied` reasons and expected subjects (`robot:<name>`).
3) Enable push grants for a single repo prefix used by CI.
4) Expand prefixes gradually after verifying clients behave as expected.

## Operational playbook

### Create / rotate a robot secret

1) Generate a new Argon2id hash:
    - Run `registry-rust hash-secret` and paste the secret on stdin.
2) Update `secret_hash` in TOML for the robot account.
3) Restart the registry process.

Notes:
- Old secrets stop working immediately after restart.
- Prefer one robot per workload; rotate secrets periodically.

### Token signing key rotation (overlap, recommended)

Current implementation supports overlap rotation using multiple HMAC signing keys with a `kid` hint.

Rotation procedure:
1) Add a new primary key as the FIRST entry in `[[token.signing_keys]]`.
2) Keep the previous key(s) listed after it for an overlap window.
3) Restart the registry.
4) After the overlap window (>= max token TTL), remove the old key(s) and restart again.

Notes:
- The FIRST `token.signing_keys` entry is used to mint new tokens.
- All `token.signing_keys` entries are accepted for verification.
- Env vars (`TOKEN_SIGNING_KEY` / `REGISTRY__TOKEN__SIGNING_KEY`) are ignored when `token.signing_keys` is present.

Legacy rotation procedure (single key):
- If you use `token.signing_key` / `TOKEN_SIGNING_KEY` only, rotation is still a hard cutover.

### Incident response (suspected credential leakage)

- If a robot secret is suspected leaked: rotate that robot’s `secret_hash` and restart.
- If a signing key is suspected leaked: rotate by removing the compromised key from `token.signing_keys` (or changing `token.signing_key`) and restart.
- Review `token_denied` and `token_issued` logs for unexpected subjects, repos, or push activity.

### Limitations / planned improvements

- Harbor-style users + groups are implemented (config-only). See `docs/harbor-lite-phase2.md`.
- Projects/tenants, external IdP integration (OIDC/LDAP), and UI/CRUD flows are out of scope.
- No persistent token revocation list (by design); rely on short TTL + key removal/rotation.
- Token endpoint rate limiting is global (not per-IP). Consider adding per-IP limiting if exposed to untrusted networks.

- Policy engine is small/pure and heavily tested
- No scope escalation possible (subset checks)
- Prefix matching has no edge-case bypass
- Token TTL is bounded and service-bound
- Secrets never logged
- Strict config parsing catches typos in production
