# ADR-014: Auth and proxy trust boundaries

* **Status:** Accepted (2026-09-26). Clauses 2, 3, and 5 are amended by [ADR-015](adr-015-catalog-credential-and-scope-match.md) (2026-09-26): catalog has one predicate, upstream Basic is not sent to content-allowlist hosts, and `*` is not a repository match.
* **Scope:** Anonymous token minting, private-name matching, catalog visibility, proxy egress, upstream credential hosts, redirect default, token signing-key lifetime, constant-time comparison of configured secrets

## Problem

Anonymous pull and the private-name list were enforced by different predicates. `/token` classified a scope by the scope string (`*` is not a private name) and then signed that scope. Repository authorization treated `*` as every repository, so `repository:*:pull` pulled `private/…` with no credentials.

Catalog listing used a third predicate: any valid Basic password, and `/_meta` used “any authenticated caller” while an open catalog returned every name.

The pull-through proxy checked DNS, then connected with a second lookup, and its blocklist did not fold IPv4-mapped IPv6 back to IPv4. Upstream Basic credentials were sent to any host sharing the last two DNS labels of the upstream. Redirects defaulted to any public HTTPS host. A missing signing key became a process-local UUID. Admin and push secrets were compared with `==`.

## Decision

One privacy predicate, one catalog decision, and one connect-time egress policy.

1. **Private names match on a path boundary.** After the existing `library/` strip, prefix `private` matches `private` and `private/…`, and does not match `private-repo`, `privately-owned/…`, or `team/private/…`. A configured prefix may itself contain `/`; the same boundary rule applies.
2. **Anonymous tokens are exact and public.** A repository scope whose name contains `*` requires authentication. The anonymous success path refuses the request if any requested repository scope is expansive or private. Authenticated grants are unchanged: `matches_repo_name` still honors `*` and `…/*` on tokens that passed RBAC.
3. **Catalog visibility is one decision**, shared by `/v2/_catalog` and `/_meta/*`. A caller with catalog scope (Bearer `token_allows_catalog_action` plus a non-empty subject, or Basic passed through the same grant intersection as token minting, including `auth.star_grants_catalog`) sees every name. Otherwise, if catalog auth is required (the existing middleware predicate), the answer is 401. Otherwise the listing contains only names that fail `is_repo_private`. A direct `/_meta` read of a hidden name is `NAME_UNKNOWN`.
4. **Proxy egress is enforced by the resolver reqwest uses.** When `block_private_networks` is on, that resolver drops blocked addresses, including IPv4-mapped and IPv4-compatible IPv6. The pre-connect lookup remains a fast failure; it is not the only check.
5. **Upstream Basic goes only to named hosts.** The upstream host is allowed. Additional token-realm hosts come from `proxy.safety.token_realm_hosts` (and the per-upstream override). Last-two-label inference is removed. Docker Hub needs an explicit `auth.docker.io`.
6. **Redirects default to `same_host`.** `any_public` stays available and is what Docker Hub blob CDNs need.
7. **The server requires an explicit signing key.** A missing key is still generated so offline CLI commands can load config, but it is marked ephemeral. `serve` and `check-config` refuse it unless `token.allow_ephemeral_signing_key` is set. `best_practice` still fails at load time with no fallback. The upload-state HMAC uses that same keyring and does not fall back to a static string when the keyring is empty.
8. **Configured admin and push secrets compare in constant time** when the byte lengths match (`subtle`). Robot and user secrets stay on Argon2.

## Consequences

* `private-repo` is no longer private under the default prefix `private`. Operators who relied on the substring match should use a boundary prefix (`private` matches `private/…`) or set the full name.
* Pull-through of Docker Hub no longer follows cross-host redirects or exchanges tokens with `auth.docker.io` until `redirect_policy = "any_public"` and `token_realm_hosts = ["auth.docker.io"]` are set.
* Deployments that booted without `token.signing_key` now fail `check-config` and `serve` until a key or the ephemeral opt-in is set.
