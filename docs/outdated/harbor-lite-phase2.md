# Harbor-lite Phase 2 (users + groups)

This phase is **implemented**: it extends the robots-only RBAC into a Harbor-like mental model (human users + groups), while keeping the same security posture:

- deny-by-default
- deterministic matching (prefix-based)
- no scope escalation (grants ⊆ requested ⊆ policy)
- small trusted computing base (TCB)
- no secrets in logs

## What exists today (Phase 1)

- Robot accounts authenticated via Basic auth on `/token`
- Robot secrets stored as Argon2id hashes
- Repo-prefix grants (deterministic) mint Bearer token scopes

This provides most of the security value for CI/CD and automation.

## Phase 2 goal

Add **human users** and **groups** without building an identity system.

Non-goals (explicitly out of scope for Phase 2):
- UI, database, CRUD APIs
- password reset flows, email, MFA
- SSO/OIDC/LDAP integration
- regex/glob authorization boundaries

## Proposed model

### Identities

- `robot:<name>` — already exists
- `user:<name>` — new

Both authenticate to `/token` with Basic auth.

### Authorization policy

- A **group** is a named set of grants.
- A **user** has membership in zero or more groups.
- Allowed scopes are computed as:

$$
\text{granted} = \text{requested} \cap \left(\bigcup_{g \in user.groups} policy[g]\right)
$$

Optionally, users may also have a local grant list (same format as robots) if you want a "break glass" escape hatch, but the simplest v2 keeps grants only in groups.

### Deterministic matching

Keep the current `repo_prefix` string prefix matching.

- Require `repo_prefix` to end in `/` (same validation as today)
- No regex in policy

> *(Correction 2026-09-19, at `master` `2718bc16`: no trailing-`/` requirement is enforced in code — `RbacRepoPattern` also accepts an exact repository name with no trailing slash and a bare `*` (`src/rbac.rs:39-57`); the packaged example config even ships `repo_prefix = "*"`. The design text above is preserved as written.)*

### Tokens

Unchanged token format and security requirements:
- `aud` bound to `token.service`
- bounded TTL
- overlap key rotation using `[[token.signing_keys]]`

## Proposed TOML schema

This is TOML-only to keep it reviewable and avoid env var explosion.

```toml
[auth.users]
enabled = true

[[auth.users.accounts]]
name = "alice"
secret_hash = "$argon2id$v=19$m=19456,t=2,p=1$..."
# optional: cap TTL for this user
max_ttl_secs = 600
# group membership
groups = ["devs", "release"]

[[auth.groups]]
name = "devs"
grants = [
  { repo_prefix = "org1/", actions = ["pull", "push"] },
]

[[auth.groups]]
name = "release"
grants = [
  { repo_prefix = "org1/", actions = ["push"] },
]
```

## Request handling rules

### Authentication on `/token`

- If robots are enabled and credentials match a robot, authenticate as `robot:<name>`.
- Else if user credentials match a user, authenticate as `user:<name>`.
- Else deny.

(Exact precedence can be a configuration flag; simplest is "robots-first" to preserve existing behavior.)

> *(Correction 2026-09-19: no precedence configuration flag exists — robots-first is hardcoded (`src/http_api/auth_token.rs`, `src/auth.rs`), and `audit-permissions` warns on robot/user name collisions (`src/audit.rs:59-74`).)*

### Scope minting

- Parse requested scopes
- Normalize actions/types
- Deny unknown scope types
- Apply policy intersection
- Mint token with granted scopes only

## Test plan

1) **Authorization invariants**
- granted ⊆ requested
- granted ⊆ policy
- deny-by-default on unknown subject / invalid policy

2) **Prefix boundary tests**
- `org/` does not match `org2/repo`

3) **User/group evaluation**
- union of multiple groups
- empty groups list grants nothing

4) **Token flow**
- `sub=user:<name>`
- `aud` binding
- TTL caps

## Rollout plan

1) Add users/groups support but keep it disabled by default.
2) Enable for pull-only first (or on a narrow repo prefix).
3) Expand once logs match expectations.

## Future extensions (later phases)

- OIDC integration mapping external groups → internal groups
- per-IP token rate limiting
- optional allowlist of user names to reduce typo risk
