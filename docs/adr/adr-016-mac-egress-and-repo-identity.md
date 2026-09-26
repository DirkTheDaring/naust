# ADR-016: MAC domains, egress special ranges, and repository identity

* **Status:** Accepted (2026-09-26)
* **Amends:** [ADR-015](adr-015-catalog-credential-and-scope-match.md) clause 3 (the `library/` alias)
* **Scope:** The three medium findings from the security-architecture pass. Does not decide multi-instance sled.

## Problem

Three boundaries still describe one thing with two rules.

1. **One key, two MACs.** Bearer verification accepts a two-part token and, for a three-part token, an HMAC over the payload alone. Upload state is that same construction: HMAC-SHA256 over the payload base64, keyed with the token signing key (`assemble_application_services`). Upload-state JSON has no `exp`, so it does not parse as a bearer today. The separation is the JSON shape.
2. **The egress blocklist is a subset of the addresses that reach a private network.** `is_blocked_ip` folds IPv4-mapped IPv6, then blocks loopback, RFC1918, link-local, and IPv6 unique-local. The well-known NAT64 prefix `64:ff9b::/96` and carrier-grade NAT `100.64.0.0/10` are connected. The resolver drops only what that function drops.
3. **`library/` is one grant and two repositories.** `matches_repo_name` strips a leading `library/`. `CanonicalRepoName` does not, so storage keeps `ubuntu` and `library/ubuntu` as different names. A token for one authorizes the other. Upload-state binding strips the same prefix, so a state token for one name verifies against the other.

## Decision

### 1. Upload state uses a derived key; bearers are three-part only

The upload coordinator is keyed with `HMAC-SHA256(token signing key, "naust.upload-state.v1")`, not the token signing key. Bearer verification accepts only a three-part token whose signature is over `header.payload`. A two-part token is a format error. A signature over the payload alone does not verify.

In-flight upload `_state` values from before this change fail. The client starts the upload again. Bearer tokens this registry mints are already three-part and keep verifying.

### 2. NAT64 to a blocked IPv4 is blocked, and CGNAT is blocked

When `block_private_networks` is on, an address in `64:ff9b::/96` is judged as the IPv4 embedded in the last 32 bits. `100.64.0.0/10` is blocked as its own range, including when it is that embedded address. A NAT64 form of a public IPv4 stays allowed. IPv4-mapped addresses stay folded as they are today.

### 3. A repository grant names the stored repository

`matches_repo_name` is exact equality of the trimmed names. It does not strip `library/`. Upload-state repository binding compares the same way: trim slashes, then exact equality.

`is_repo_private` still strips one leading `library/` before the prefix check. `library/secret/app` stays private when `secret` is a private prefix. Pulling it requires a token whose scope name is `library/secret/app`.

## Test cases

1. A three-part bearer issued by this registry verifies. A two-part `payload.sig` is `InvalidFormat`. A three-part token whose MAC covers only the payload is `InvalidSignature`.
2. An upload-state token signed with the token key does not verify as a bearer and does not verify as upload state under the derived key. The same payload signed with `upload_state_signing_key` verifies as upload state and does not verify as a bearer. The derived key bytes differ from the token key.
3. `64:ff9b::7f00:1` (127.0.0.1) and `64:ff9b::c0a8:1` (192.168.0.1) are blocked. `64:ff9b::101:101` (1.1.1.1) is not. `100.64.0.1` and `100.127.255.1` are blocked. `100.63.255.255` and `100.128.0.1` are not. `::ffff:127.0.0.1` stays blocked.
4. A scope named `library/ubuntu` authorizes `library/ubuntu` and does not authorize `ubuntu`. A scope named `ubuntu` does not authorize `library/ubuntu`. `is_repo_private("library/secret/app")` stays true. An upload-state token for `library/repo-a` does not verify against `repo-a`.

## Consequences

* Resuming an upload across this deploy fails until the client opens a new session.
* Operators who relied on a token for `ubuntu` to pull `library/ubuntu` issue the token for the stored name.
* Private-name coverage of `library/…` is unchanged.
