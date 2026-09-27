# ADR-017: Ship the registry the way CI can prove

- **Status:** Accepted (2026-09-27)
- **Date:** 2026-09-27
- **Relates to:** the DevOps review of packaging, the container quickstart, and probes

## Context

Hosted CI compiles `vendor/` and runs tests. It did not build the RPM, the DEB, or the container. `make rpm` and `make deb` copied `naust.*` packaging inputs that were still named `registry-rust.*`, and `docs/blob-gc.md`, which is not in the tree. `docker compose up` published `0.0.0.0:5000` with `demo`/`demo` and no `TOKEN_SIGNING_KEY`, so `serve` exited on the ephemeral-key refusal. `GET /v2/` returns 401 when auth is configured, and token counters never left the process.

`#![allow(clippy::all)]` is crate-wide. Turning clippy into a merge gate is a separate cleanup, not a substitute for a packaging check.

## Decision

1. Packaging inputs use the product name `naust`. Both systemd units keep `LimitNOFILE=65536`, stop within 30 seconds (the process drains for 15), and give up after five restarts in 60 seconds. Packages install `docs/operations.md` instead of the missing GC note. The RPM spec identifies the project as MIT at `https://github.com/DirkTheDaring/naust`. `make packaging-check` fails when those inputs or the compose contract are absent. CI runs that target on every push.

2. The image does not embed a signing key. The compose file binds `127.0.0.1:5000`, sets a loopback-only dev key, and leaves push credentials unset. A config-file overlay still clears `TOKEN_SIGNING_KEY` so the mounted TOML is the source of the key. A version tag pushes `ghcr.io/<repository>:<tag>` and `:latest`. That image still requires `TOKEN_SIGNING_KEY` at run time.

3. `GET /healthz` is unauthenticated and returns 200 once the process is listening. Listening already means startup, including membership migration, has finished. `GET /metrics` is unauthenticated Prometheus text for token counters and in-flight request gauges, with no repository names. `/v2/` stays the OCI ping and may still be 401. Probes and metrics skip the per-IP concurrency limit.

4. `vendor/` remains the snapshot CI and the image compile. `make vendor-sync` is how a tested sibling commit becomes that snapshot. CI does not fetch `naust-core` `master` on its own.

## Consequences

- Operators restore a filesystem registry by stopping the single writer and copying the storage root and the sled ref-index directory together.
- A published GHCR image will not boot until the operator supplies a signing key.
- Metrics on the registry port are visible to anyone who can open that port. Bind the port to loopback, or put a proxy in front, when that is too open.
