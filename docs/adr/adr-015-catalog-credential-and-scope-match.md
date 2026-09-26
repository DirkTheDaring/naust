# ADR-015: Catalog predicate, credential hosts, and exact repository scopes

* **Status:** Accepted (2026-09-26)
* **Amends:** [ADR-014](adr-014-auth-and-proxy-trust-boundaries.md) clauses 2, 3, and 5
* **Scope:** The three residuals left after the ADR-014 implementation. Does not decide multi-instance sled, the `read`→`pull` alias, or length-leaking secret compares.

## Problem

ADR-014 closed the anonymous `repository:*:pull` bypass, but three weaker copies of the same mistakes remain.

1. **Two catalog predicates.** `authorize_catalog` requires a non-empty token subject before a bearer may see private names. The `/v2/_catalog` middleware allows any bearer for which `token_allows_catalog_action` is true, including a subject-less token. Both live catalog routes call the handler check, so private names stay hidden today. A later catalog response built on the middleware's answer would not.
2. **Content hosts are credential hosts.** `is_token_realm_host_allowed` is true for every entry of `allowed_upstream_hosts`, not only the upstream host plus `token_realm_hosts`. A CDN listed so blob redirects can be fetched can be presented as the `WWW-Authenticate` realm and receive `upstream_username` / `upstream_password`.
3. **`*` still means every repository at check time.** `matches_repo_name` returns true for scope name `*` and for a `…/*` prefix. `token_allows_catalog_action` treats `repository:*` with action `*` as catalog access. Current issuers cannot mint those names (`CanonicalRepoName` rejects them, and the anonymous path rejects repository names that contain `*`). A future issuer that copies the client scope through would reopen ADR-014's bug. Grant wildcards and token scope names are still different languages.

## Decision

One predicate per decision, and repository scope names are exact.

### 1. Catalog authorization is only `authorize_catalog`

`require_auth_middleware` for `/v2/_catalog` calls `authorize_catalog` and does not keep a second bearer or Basic test.

| Result | Middleware | Handler (`/v2/_catalog` and `/_meta/*`) |
|---|---|---|
| `Denied` | 401 challenge | 401 challenge |
| `PublicOnly` | continue | list or read only names that fail `is_repo_private` |
| `Full` | continue | list or read every name |

`Full` stays what ADR-014 defined: a non-empty bearer subject and `token_allows_catalog_action`, or Basic that passes the same grant intersection as token minting (`star_grants_catalog` included). An empty subject is never `Full`.

The anonymous mint path refuses any scope whose name contains `*`, including `registry:catalog:*` and `registry:*`. A subject-less catalog token is not issued. Exact public repository scopes are unchanged.

### 2. Upstream Basic uses a credential host set

Two sets, stored separately on `Proxy`:

| Set | Source | Used for |
|---|---|---|
| Content hosts | `allowed_upstream_hosts`, or the upstream host when that list is empty | blob and manifest fetches, redirect targets |
| Credential hosts | the host of `upstream_base_url`, plus `token_realm_hosts` | the token realm that may receive upstream Basic |

`is_token_realm_host_allowed` consults credential hosts only. `ensure_upstream_allowed` and redirect checks consult content hosts only. Per-upstream routes use the same split against that route's base URL and its own `token_realm_hosts`.

Upstream Basic is attached only inside the token exchange, after the realm host is accepted. A redirect that changes host keeps dropping `Authorization`. A CDN listed in `allowed_upstream_hosts` can be fetched. It cannot be the realm that receives the password.

Operators who put `auth.docker.io` only in `allowed_upstream_hosts` move it to `token_realm_hosts`. The registry host itself remains a credential host because it is the upstream host.

### 3. A repository scope authorizes one repository

`matches_repo_name` is exact equality, plus the existing `library/` alias on both sides. Scope name `*` authorizes nothing. A name ending in `/*` authorizes nothing.

`token_allows_catalog_action` recognizes only a `registry` scope whose name is `catalog` or `*` and whose actions include a catalog action (`*`, `pull`, `push`, `read`). A `repository` scope named `*` does not confer catalog access.

Namespace wildcards stay on RBAC grants. The token carries the canonical repository that was requested. `CanonicalRepoName` remains the filter that drops `*` and `/*` on the authenticated mint path. Check time does not trust that the issuer remembered.

Still-valid tokens issued before this change that contain `repository:*` stop matching every repository at the end of their TTL. That is the intended cutover. There is no overlap window for wildcard scope names.

## Consequences

* `/v2/_catalog` and `/_meta` cannot drift: both answers come from `authorize_catalog`.
* Docker Hub and any other split registry/auth host must list the auth host in `token_realm_hosts`. Listing it only as a content host no longer sends Basic there.
* Tests that expect `matches_repo_name("*", …)` or a `foo/*` scope to allow a repository, and tests that expect `repository:*` to allow the catalog, change with the predicate.
* Multi-instance use of the sled ref-index is still undecided and is not part of this decision.
