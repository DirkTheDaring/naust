# registry-rust

Minimal Docker/OCI registry (Distribution v2-ish) in Rust.

## Run locally (cargo)

Using one config file:

```sh
cargo run -- server --config ./configs/registry.example.toml
```

Layer multiple config files (later files override earlier ones):

```sh
cargo run -- server \
  --config ./configs/registry.core.toml \
  --config ./configs/registry.auth.toml
```

Merge semantics:
- TOML tables deep-merge
- arrays/lists are replaced wholesale (last file wins)
- scalar values are overridden

Note: the configuration format is TOML (YAML is not supported).

```sh
export REGISTRY_USERNAME=demo REGISTRY_PASSWORD=demo
export REGISTRY_PUSH_ALLOW_REPOS=myrepo
CARGO_TARGET_DIR=target2 cargo run -- server
```

Smoke test:

```sh
CARGO_TARGET_DIR=target2 REGISTRY_USERNAME=demo REGISTRY_PASSWORD=demo scripts/smoke.sh
```

Pull-through cache smoke test (offline; starts a local upstream registry):

```sh
chmod +x scripts/pullthrough-smoke.sh
scripts/pullthrough-smoke.sh
```

Podman (real client) smoke test:

```sh
# Generates a temporary self-signed TLS cert if needed.
CARGO_TARGET_DIR=target2 REGISTRY_USERNAME=demo REGISTRY_PASSWORD=demo scripts/podman-smoke.sh
```

## OCI Distribution conformance (integration test)

This repo runs the OCI Distribution Spec conformance suite as a black-box CI job.
CI is configured to run Pull + Push + Content Discovery + Content Management workflows.

To run it locally (starts a local registry and writes reports to `./conformance-results`):

```sh
chmod +x scripts/oci-conformance.sh
scripts/oci-conformance.sh
```

To run with Content Discovery + Content Management enabled locally:

```sh
OCI_TEST_CONTENT_DISCOVERY=1 OCI_TEST_CONTENT_MANAGEMENT=1 scripts/oci-conformance.sh
```

## Run with Docker

```sh
docker compose up --build
```

This listens on `127.0.0.1:5000` (host) and stores data in a named volume (`registry-data`).

### File descriptor limits (important)

Registry workloads can open many concurrent sockets (clients, reverse proxies) and many blob files.
If the process hits the OS file descriptor limit, you may see logs like:

`ERROR axum::serve: accept error: Too many open files (os error 24)`

- **systemd**: set `LimitNOFILE` in the service unit (the packaged unit sets `65536`).
- **docker-compose**: set `ulimits.nofile` (the example compose sets it).

To inspect at runtime:

```sh
systemctl show registry-rust -p LimitNOFILE
cat /proc/$(pidof registry-rust)/limits | grep -i "open files"
ls /proc/$(pidof registry-rust)/fd | wc -l
```

## Build RPM (containerized, Fedora)

The default `make rpm` builds on the host.

To build the RPM inside a specific Fedora release (useful for targeting different Fedora versions), use the container build helper:

```sh
packaging/docker/build-rpm-in-fedora.sh 40
```

If rootless Podman fails (e.g. because your home directory is mounted `noexec`), use:

```sh
packaging/docker/build-rpm-in-fedora.sh --rootful 40
```

Artifacts are written to `./dist/rpmbuild/RPMS/...` as usual.

## Build DEB (containerized, Debian)

The `make deb` target produces a Debian `.deb` package under `./dist/`.

If you are not on a Debian-based host (or you don't want to install `dpkg-deb` locally), build inside the latest Debian stable container (currently `trixie`):

```sh
chmod +x packaging/docker/build-deb-in-debian.sh
packaging/docker/build-deb-in-debian.sh trixie
```

If rootless Podman fails (e.g. because your home directory is mounted `noexec`), use:

```sh
packaging/docker/build-deb-in-debian.sh --rootful trixie
```

Artifacts are written to `./dist/registry-rust_<version>_<arch>.deb`.

### Config file (optional)

To run with a TOML config file via compose, use the overlay and set `REGISTRY_TOML_PATH`:

```sh
REGISTRY_TOML_PATH=./configs/registry.example.toml \
  docker compose -f docker-compose.yml -f docker-compose.config.yml up --build
```

### S3 (MinIO) backend (optional)

Run the registry backed by a local MinIO instance:

```sh
docker compose -f docker-compose.yml -f docker-compose.minio.yml up --build
```

End-to-end smoke test (starts/stops the compose stack):

```sh
chmod +x scripts/minio-smoke.sh
REGISTRY_USERNAME=demo REGISTRY_PASSWORD=demo scripts/minio-smoke.sh
```

To run the MinIO smoke using a TOML config file:

```sh
REGISTRY_TOML_PATH=./configs/registry.minio.toml scripts/minio-smoke.sh
```

### HTTPS (optional)

Generate a local self-signed cert (SAN for `127.0.0.1` and `localhost`):

```sh
mkdir -p certs
openssl req -x509 -nodes -newkey rsa:2048 \
  -keyout certs/key.pem \
  -out certs/cert.pem \
  -days 365 \
  -subj "/CN=127.0.0.1" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
```

Run with TLS enabled:

```sh
docker compose -f docker-compose.yml -f docker-compose.tls.yml up --build
```

## Docker CLI test

Docker usually requires either TLS or marking the registry as insecure.

Note: when auth is configured, `GET /v2/` may return `401` with `WWW-Authenticate: Bearer ...`.
This is expected: Docker/Podman use it to discover the token endpoint.

- For local dev, configure Docker daemon with an insecure registry entry for `127.0.0.1:5000`.
- Then:

```sh
docker login 127.0.0.1:5000
# use demo/demo from compose

docker tag alpine:latest 127.0.0.1:5000/myrepo:latest

docker push 127.0.0.1:5000/myrepo:latest

docker logout 127.0.0.1:5000
# anonymous pull
docker pull 127.0.0.1:5000/myrepo:latest
```

## Podman test

If you're using Podman, pushes work via the Bearer token flow.

```sh
podman login --tls-verify=false 127.0.0.1:5000  # demo/demo from compose
podman tag alpine:latest 127.0.0.1:5000/myrepo:latest
podman push --tls-verify=false 127.0.0.1:5000/myrepo:latest
podman pull --tls-verify=false 127.0.0.1:5000/myrepo:latest
```

## Environment variables

## CLI helpers

- `registry-rust server [--config <PATH>]`: run the registry server
- `registry-rust check-config [--config <PATH>]`: parse/validate config and exit
- `registry-rust audit-permissions [--config <PATH>]`: print effective RBAC permissions and exit
- `registry-rust hash-secret`: read a secret from stdin and print an Argon2id hash (for robots/users)
- `registry-rust ref-index check [--config <PATH>]`: verify the blob reference index is healthy
- `registry-rust ref-index rebuild [--config <PATH>]`: rebuild the blob reference index from storage
- `registry-rust ref-index ensure [--config <PATH>]`: check and rebuild the blob reference index if needed
- `registry-rust blob-gc plan|quarantine|delete [--config <PATH>]`: reclaim storage by quarantining/deleting unreferenced blobs (filesystem backend only; refuses to run while the server is active on the same `fs_root`)

## Online blob GC (admin API)

This repo also implements an *in-process* online-safe blob GC (see `docs/blob-gc-online.md`).
It is driven via admin-only HTTP endpoints and is disabled by default.

To enable it in TOML:

```toml
[admin_api]
enabled = true
username = "admin"
password = "change-me"

[blob_gc]
enabled = true         # allows quarantine
enable_delete = false  # keep false until you've validated quarantine behavior

# Optional: periodic background cleanup (disabled by default)
# When enabled, the server runs an automatic quarantine pass every interval and (if enable_delete=true)
# a delete pass immediately after, using the default policy/budgets from this section.
# Note: the first scheduled run happens after one full interval.
# schedule_enabled = false
# schedule_interval_secs = 604800 # 7d
```

Endpoints:
- `GET /_admin/gc/health` (checks ref-index readiness; no side effects)
- `POST /_admin/gc/plan`
- `POST /_admin/gc/quarantine` (requires `blob_gc.enabled=true`)
- `POST /_admin/gc/delete` (requires `blob_gc.enabled=true` and `blob_gc.enable_delete=true`)

Minimal example (defaults are taken from `[blob_gc]` when fields are omitted):

```sh
curl -u admin:change-me http://127.0.0.1:5000/_admin/gc/health
curl -u admin:change-me -X POST http://127.0.0.1:5000/_admin/gc/plan -H 'content-type: application/json' -d '{}'
curl -u admin:change-me -X POST http://127.0.0.1:5000/_admin/gc/quarantine -H 'content-type: application/json' -d '{}'
```

Staged rollout example (explicit request fields; safe budgets):

```sh
# 1) Plan (no side effects). Policy defaults to "manifest_rooted" if omitted.
curl -u admin:change-me -X POST http://127.0.0.1:5000/_admin/gc/plan \
  -H 'content-type: application/json' \
  -d '{
    "policy": "manifest_rooted",
    "min_age_secs": 604800,
    "budgets": { "max_blobs": 2000, "max_bytes": 10737418240, "max_seconds": 30 }
  }'

# 2) Quarantine eligible blobs (requires: blob_gc.enabled=true)
curl -u admin:change-me -X POST http://127.0.0.1:5000/_admin/gc/quarantine \
  -H 'content-type: application/json' \
  -d '{
    "policy": "manifest_rooted",
    "min_age_secs": 604800,
    "budgets": { "max_blobs": 2000, "max_bytes": 10737418240, "max_seconds": 30 }
  }'

# 3) Observe for at least `quarantine_delay_secs` (default: 24h).
#    Pulls should continue to work while blobs are quarantined.

# 4) Enable delete only after you are satisfied with quarantine behavior:
#    [blob_gc]
#    enable_delete = true

# 5) Delete old quarantined blobs (requires: blob_gc.enabled=true AND blob_gc.enable_delete=true)
curl -u admin:change-me -X POST http://127.0.0.1:5000/_admin/gc/delete \
  -H 'content-type: application/json' \
  -d '{
    "policy": "manifest_rooted",
    "quarantine_delay_secs": 86400,
    "budgets": { "max_blobs": 2000, "max_bytes": 10737418240, "max_seconds": 30 }
  }'
```

Policy values:
- `manifest_rooted` (default): keep any blob reachable from any manifest
- `tag_rooted`: keep only blobs reachable from current tags

### Config file (TOML)

You can optionally load configuration from a TOML file and still override any value via environment variables.

By default, unknown TOML keys are ignored with a warning printed to stderr.

To fail fast on unknown keys (recommended), enable strict parsing via one of:

- `STRICT_CONFIG=1` (or `REGISTRY__CONFIG__STRICT=1`)
- `[config].strict = true` in the TOML
- best-practice profile (`BEST_PRACTICE=1` or `[profile].name="best_practice"`)

## Pull-through cache (proxy)

This registry can act as a pull-through cache for selected upstream repositories (useful for Docker Hub rate limits).

Important: cached pull-through content is stored in a separate storage root/prefix (filesystem default: `./data/cache`).
This prevents pushed images from being mixed into the cache and makes cache cleanup as simple as removing the cache directory.

Minimal working config: `configs/registry.simple.toml`.

Users/groups example (Harbor-lite Phase 2): `configs/registry.users_groups.toml`.

Full reference config (all settings, annotated): `configs/registry.example.toml`.

Multi-upstream example (Docker Hub + GHCR, one process): `configs/registry.proxy.multi.toml`.

### Unambiguous local vs cache behavior (route by Host)

By default, the registry serves reads from local storage first and may fall back to the proxy cache/upstream.
If you want this to be unambiguous, you can configure a separate hostname that is **proxy-only**.

In proxy-only mode:
- reads never consult local storage (cache/upstream only)
- writes (push/delete/upload) are rejected with `405`

Example (TOML):

```toml
[proxy]
enabled = true

[proxy.routing]
proxy_hosts = ["cache.example.com"]
trust_x_forwarded_host = true
```

If you run behind a reverse proxy (e.g. Traefik), you have two options:

- Prefer: keep `trust_x_forwarded_host=false` (default) if your proxy preserves the original `Host` header when forwarding.
- Use `trust_x_forwarded_host=true` only when the registry is reachable *only* via that trusted proxy (so clients cannot spoof `X-Forwarded-Host`). Make sure the proxy overwrites/normalizes any incoming forwarded headers.

### Docker Hub credentials (optional)

To raise Docker Hub pull rate limits, configure upstream credentials. Prefer a Docker Hub Personal Access Token (PAT).

TOML:

```toml
[proxy.upstream]
base_url = "https://registry-1.docker.io"
username = "my-docker-id"
password = "my-dockerhub-pat"
```

Or env vars:

```sh
export REGISTRY__PROXY__UPSTREAM__BASE_URL="https://registry-1.docker.io"
export REGISTRY__PROXY__UPSTREAM__USERNAME="my-docker-id"
export REGISTRY__PROXY__UPSTREAM__PASSWORD="my-dockerhub-pat"
```

### Multiple upstream registries (and separate caches)

One `registry-rust` process can proxy multiple upstream registries using `[[proxy.upstreams]]` (TOML-only).

Each upstream route has:
- `hosts`: host patterns (minimal `*` glob) that select the upstream
- `base_url`: upstream base URL (+ optional `username`/`password`)
- `max_cache_bytes`: per-upstream cache limit (plus optional cache location overrides)

When the request Host matches an upstream route, the registry runs in proxy-only mode for that request (unambiguous reads; writes rejected).

Example (single process, Docker Hub + GHCR, filesystem caches):

```toml
[proxy]
enabled = true

[[proxy.upstreams]]
hosts = ["dockerhub-cache.example.com"]
base_url = "https://registry-1.docker.io"
username = "my-docker-id"         # optional
password = "my-dockerhub-pat"     # optional
# fs_root and index_path are optional; by default they are derived from upstream.base_url,
# e.g. ./data/cache/registry-1.docker.io/ (filesystem backend)
max_cache_bytes = 10737418240

[[proxy.upstreams]]
hosts = ["ghcr-cache.example.com"]
base_url = "https://ghcr.io"
# fs_root and index_path are optional; defaults are derived from upstream.base_url.
max_cache_bytes = 10737418240
```

Full config example: `configs/registry.proxy.multi.toml`.

- Enable: set `CONFIG_PATH` to a TOML file (see `configs/registry.example.toml` and `configs/registry.best_practice.toml`).
- Precedence: defaults < config file < env vars
- Best-practice profile:
  - Set `BEST_PRACTICE=1`, or set `[profile].name = "best_practice"` in the TOML.
  - In best-practice mode, `TOKEN_SIGNING_KEY` (or `token.signing_key` / `token.signing_keys` in the TOML) is required; the process fails fast if missing.

Example:

```sh
CONFIG_PATH=./configs/registry.best_practice.toml \
TOKEN_SIGNING_KEY='replace-me-with-a-long-random-secret' \
CARGO_TARGET_DIR=target2 cargo run -- server
```

### Canonical env var namespace

All options also support a canonical `REGISTRY__...` env var namespace (double-underscore separators), for example:

- `REGISTRY__SERVER__LISTEN_ADDR` (alias for `LISTEN_ADDR`)
- `REGISTRY__TOKEN__SIGNING_KEY` (alias for `TOKEN_SIGNING_KEY`)
- `REGISTRY__UPLOADS__DISALLOW_MONOLITHIC_UPLOADS` (alias for `DISALLOW_MONOLITHIC_UPLOADS`)

Existing env var names continue to work.

### Token endpoint rate limiting

To reduce brute-force and protect expensive password/hash verification, `/token` is rate limited (global, per-process).

- Disable: `TOKEN_RATE_LIMIT_RPM=0`
- Tune:
  - `TOKEN_RATE_LIMIT_RPM` / `REGISTRY__TOKEN__RATE_LIMIT_RPM` (default: `1200`)
  - `TOKEN_RATE_LIMIT_WINDOW_SECS` / `REGISTRY__TOKEN__RATE_LIMIT_WINDOW_SECS` (default: `60`)

### Harbor-lite RBAC (robots + users/groups)

Push tokens are authorized via deterministic repo-prefix grants (deny-by-default):

- **Robots**: `[auth.robots]` + `[[auth.robots.accounts]]` (config-only)
- **Users + groups**: `[auth.users]` + `[[auth.users.accounts]]` + `[[auth.groups]]` (config-only)

These settings are TOML-only to keep reviewable policy in source control.
See `docs/rbac.md` and `docs/harbor-lite-phase2.md`.

### Config option inventory

Precedence: defaults < config file < env vars

Best-practice profile (`BEST_PRACTICE=1` or `[profile].name="best_practice"`) changes these defaults:

- `features.allow_tag_overwrite`: default `false` (instead of `true`)
- `uploads.disallow_monolithic_uploads`: default `true` (instead of `false`)
- `catalog.requires_auth`: default `true` (instead of `false`)
- `timeouts.request_timeout_secs`: default `60` (instead of `300`)
- `timeouts.upload_request_timeout_secs`: default `7200` (instead of `3600`)
- `token.signing_key` / `token.signing_keys`: required (no random fallback)

### Token signing key rotation (overlap)

For safe signing-key rotation without breaking in-flight tokens, configure a keyring in TOML via `[[token.signing_keys]]`.

- The FIRST entry is the primary key used to mint new tokens.
- All entries are accepted for verification (overlap window).
- `TOKEN_SIGNING_KEY` / `REGISTRY__TOKEN__SIGNING_KEY` are ignored when `token.signing_keys` is present (TOML-only feature).

Example:

```toml
[token]
service = "registry-rust"

[[token.signing_keys]]
kid = "k2026_01"
key = "<new-long-random-secret>"

[[token.signing_keys]]
kid = "k2025_12"
key = "<old-long-random-secret>"
```

| Purpose | TOML key | Canonical env | Legacy env | Default |
| --- | --- | --- | --- | --- |
| Config file path | (n/a) | `REGISTRY__CONFIG_PATH` | `CONFIG_PATH` | unset |
| Best-practice profile | `profile.name` | (n/a) | `BEST_PRACTICE` | off |
| Listen addr | `server.listen_addr` | `REGISTRY__SERVER__LISTEN_ADDR` | `LISTEN_ADDR` | `127.0.0.1:5000` |
| Public URL | `server.public_url` | `REGISTRY__SERVER__PUBLIC_URL` | `PUBLIC_URL` | unset |
| TLS cert path | `server.tls.cert_path` | `REGISTRY__SERVER__TLS__CERT_PATH` | `TLS_CERT_PATH` | unset |
| TLS key path | `server.tls.key_path` | `REGISTRY__SERVER__TLS__KEY_PATH` | `TLS_KEY_PATH` | unset |
| TLS ACME enabled | `server.tls.acme.enabled` | `REGISTRY__SERVER__TLS__ACME__ENABLED` | `TLS_ACME_ENABLED` | off |
| TLS ACME provider | `server.tls.acme.provider` | `REGISTRY__SERVER__TLS__ACME__PROVIDER` | `TLS_ACME_PROVIDER` | `ispone` |
| TLS ACME email | `server.tls.acme.email` | `REGISTRY__SERVER__TLS__ACME__EMAIL` | `TLS_ACME_EMAIL` | unset |
| TLS ACME names | `server.tls.acme.names` | `REGISTRY__SERVER__TLS__ACME__NAMES` | `TLS_ACME_NAMES` | unset |
| TLS ACME output dir | `server.tls.acme.output_dir` | `REGISTRY__SERVER__TLS__ACME__OUTPUT_DIR` | `TLS_ACME_OUTPUT_DIR` | unset |
| TLS ACME renewal window | `server.tls.acme.renewal_window_secs` | `REGISTRY__SERVER__TLS__ACME__RENEWAL_WINDOW_SECS` | `TLS_ACME_RENEWAL_WINDOW_SECS` | `2592000` |
| TLS ACME debug | `server.tls.acme.debug` | `REGISTRY__SERVER__TLS__ACME__DEBUG` | `TLS_ACME_DEBUG` | off |
| TLS ACME proxy | `server.tls.acme.proxy` | `REGISTRY__SERVER__TLS__ACME__PROXY` | `TLS_ACME_PROXY` | unset |
| TLS ACME ispone base URL | `server.tls.acme.ispone.base_url` | `REGISTRY__SERVER__TLS__ACME__ISPONE__BASE_URL` | `TLS_ACME_ISPONE_BASE_URL` | unset |
| TLS ACME ispone auth | `server.tls.acme.ispone.authorization` | `REGISTRY__SERVER__TLS__ACME__ISPONE__AUTHORIZATION` | `TLS_ACME_ISPONE_AUTHORIZATION` | unset |
| TLS ACME exec hook path | `server.tls.acme.exec_path.exec_path` | `REGISTRY__SERVER__TLS__ACME__EXEC_PATH__EXEC_PATH` | `TLS_ACME_EXEC_PATH` | unset |
| Push auth mode | `auth.push.mode` | `REGISTRY__AUTH__PUSH__MODE` | `PUSH_AUTH_MODE` | `token_only` |
| Push username | `auth.push.username` | `REGISTRY__AUTH__PUSH__USERNAME` | `REGISTRY_USERNAME` | unset |
| Push password | `auth.push.password` | `REGISTRY__AUTH__PUSH__PASSWORD` | `REGISTRY_PASSWORD` | unset |
| Push allowlist | `auth.push.allow_repos` | `REGISTRY__AUTH__PUSH__ALLOW_REPOS` | `REGISTRY_PUSH_ALLOW_REPOS` | unset |
| Robot accounts (RBAC) | `auth.robots` | (n/a) | (n/a) | disabled |
| Users/groups (RBAC) | `auth.users` / `auth.groups` | (n/a) | (n/a) | disabled |
| Storage backend | `storage.backend` | `REGISTRY__STORAGE__BACKEND` | `STORAGE_BACKEND` | `fs` |
| FS root | `storage.fs.root` | `REGISTRY__STORAGE__FS__ROOT` | `STORAGE_FS_ROOT` | `./data` |
| Blob ref index enabled | `storage.ref_index.enabled` | `REGISTRY__STORAGE__REF_INDEX__ENABLED` | `STORAGE_REF_INDEX_ENABLED` | `true` |
| Blob ref index path | `storage.ref_index.path` | `REGISTRY__STORAGE__REF_INDEX__PATH` | `STORAGE_REF_INDEX_PATH` | `<fs_root>/ref-index` |
| Blob ref index rebuild on start | `storage.ref_index.rebuild_on_start` | `REGISTRY__STORAGE__REF_INDEX__REBUILD_ON_START` | `STORAGE_REF_INDEX_REBUILD_ON_START` | `false` |
| Blob ref index auto rebuild | `storage.ref_index.auto_rebuild_on_corruption` | `REGISTRY__STORAGE__REF_INDEX__AUTO_REBUILD_ON_CORRUPTION` | `STORAGE_REF_INDEX_AUTO_REBUILD_ON_CORRUPTION` | `true` |
| S3 endpoint | `storage.s3.endpoint` | `REGISTRY__STORAGE__S3__ENDPOINT` | `STORAGE_S3_ENDPOINT` | unset |
| S3 region | `storage.s3.region` | `REGISTRY__STORAGE__S3__REGION` | `STORAGE_S3_REGION` | unset |
| S3 bucket | `storage.s3.bucket` | `REGISTRY__STORAGE__S3__BUCKET` | `STORAGE_S3_BUCKET` | unset |
| S3 prefix | `storage.s3.prefix` | `REGISTRY__STORAGE__S3__PREFIX` | `STORAGE_S3_PREFIX` | `registry` |
| Allow tag overwrite | `features.allow_tag_overwrite` | `REGISTRY__FEATURES__ALLOW_TAG_OVERWRITE` | `ALLOW_TAG_OVERWRITE` | `true` (best-practice: `false`) |
| Automatic crossmount | `features.automatic_crossmount` | `REGISTRY__FEATURES__AUTOMATIC_CROSSMOUNT` | `REGISTRY_AUTOMATIC_CROSSMOUNT` | `false` |
| Upload GC enabled | `uploads.gc_enabled` | `REGISTRY__UPLOADS__GC_ENABLED` | `UPLOAD_GC_ENABLED` | `true` |
| Upload GC interval | `uploads.gc_interval_secs` | `REGISTRY__UPLOADS__GC_INTERVAL_SECS` | `UPLOAD_GC_INTERVAL_SECS` | `3600` |
| Upload GC max age | `uploads.gc_max_age_secs` | `REGISTRY__UPLOADS__GC_MAX_AGE_SECS` | `UPLOAD_GC_MAX_AGE_SECS` | `86400` |
| Max upload bytes | `limits.max_upload_bytes` | `REGISTRY__LIMITS__MAX_UPLOAD_BYTES` | `MAX_UPLOAD_BYTES` | `5368709120` |
| Max request body bytes | `limits.max_request_body_bytes` | `REGISTRY__LIMITS__MAX_REQUEST_BODY_BYTES` | `MAX_REQUEST_BODY_BYTES` | `33554432` |
| Max buffered requests | `limits.max_concurrent_buffered_requests` | `REGISTRY__LIMITS__MAX_CONCURRENT_BUFFERED_REQUESTS` | `MAX_CONCURRENT_BUFFERED_REQUESTS` | `8` (best-practice: `4`) |
| Max in-flight requests | `limits.max_concurrent_requests` | `REGISTRY__LIMITS__MAX_CONCURRENT_REQUESTS` | `MAX_CONCURRENT_REQUESTS` | `256` (best-practice: `64`) |
| Request timeout | `timeouts.request_timeout_secs` | `REGISTRY__TIMEOUTS__REQUEST_TIMEOUT_SECS` | `REQUEST_TIMEOUT_SECS` | `300` (best-practice: `60`) |
| Upload request timeout | `timeouts.upload_request_timeout_secs` | `REGISTRY__TIMEOUTS__UPLOAD_REQUEST_TIMEOUT_SECS` | `UPLOAD_REQUEST_TIMEOUT_SECS` | `3600` (best-practice: `7200`) |
| Disallow monolithic uploads | `uploads.disallow_monolithic_uploads` | `REGISTRY__UPLOADS__DISALLOW_MONOLITHIC_UPLOADS` | `DISALLOW_MONOLITHIC_UPLOADS` | `false` (best-practice: `true`) |
| Catalog requires auth | `catalog.requires_auth` | `REGISTRY__CATALOG__REQUIRES_AUTH` | `CATALOG_REQUIRES_AUTH` | `false` (best-practice: `true`) |
| Token service | `token.service` | `REGISTRY__TOKEN__SERVICE` | `TOKEN_SERVICE` | `registry-rust` |
| Token signing key (legacy single key) | `token.signing_key` | `REGISTRY__TOKEN__SIGNING_KEY` | `TOKEN_SIGNING_KEY` | random per-process (best-practice: required) |
| Token signing keys (overlap rotation) | `token.signing_keys` | (n/a) | (n/a) | unset |
| Token TTL | `token.ttl_secs` | `REGISTRY__TOKEN__TTL_SECS` | `TOKEN_TTL_SECS` | `600` |

Proxy cache maintenance:

| Purpose | TOML key | Canonical env | Legacy env | Default |
| --- | --- | --- | --- | --- |
| Cache scrub enabled | `proxy.cache.scrub_enabled` | `REGISTRY__PROXY__CACHE__SCRUB_ENABLED` | `PROXY_SCRUB_ENABLED` | `false` |
| Cache scrub interval | `proxy.cache.scrub_interval_secs` | `REGISTRY__PROXY__CACHE__SCRUB_INTERVAL_SECS` | `PROXY_SCRUB_INTERVAL_SECS` | `3600` |
| Cache scrub max files | `proxy.cache.scrub_max_files_per_run` | `REGISTRY__PROXY__CACHE__SCRUB_MAX_FILES_PER_RUN` | `PROXY_SCRUB_MAX_FILES_PER_RUN` | `2000` |

- `LISTEN_ADDR` (default `127.0.0.1:5000`)
- `PUSH_AUTH_MODE` / `REGISTRY__AUTH__PUSH__MODE` (`deny_if_no_basic` | `basic_or_token` | `token_only`)
- `REGISTRY_USERNAME`, `REGISTRY_PASSWORD` (required unless `PUSH_AUTH_MODE=token_only`)
- `REGISTRY_PUSH_ALLOW_REPOS` (optional, comma-separated; supports `org/*` prefixes and `*`)
- `STORAGE_BACKEND` (`fs` or `s3`)
- `STORAGE_FS_ROOT` (default `./data`)
- `STORAGE_REF_INDEX_ENABLED` (`1`/`0`; default `1`) — if disabled, safe blob delete falls back to scanning manifests
- `STORAGE_REF_INDEX_PATH` (default `<fs_root>/ref-index`)
- `ALLOW_TAG_OVERWRITE` (`1`/`0`)
- `REGISTRY_AUTOMATIC_CROSSMOUNT` (`1`/`0`; default `0`)
- `MAX_UPLOAD_BYTES` (default `5368709120`)
- `MAX_REQUEST_BODY_BYTES` (default `33554432`)
- `REQUEST_TIMEOUT_SECS` (default `300`)
- `UPLOAD_REQUEST_TIMEOUT_SECS` (default `3600`) — request timeout for blob upload endpoints only
- `DISALLOW_MONOLITHIC_UPLOADS` (`1`/`0`; default `0`) — if enabled, rejects monolithic uploads (body on `POST ?digest` or `PUT .../uploads/<uuid>?digest=`) and forces PATCH-based chunked upload

Inventory/listing endpoints:

- `CATALOG_REQUIRES_AUTH` (`1`/`0`; default `0`) — if enabled, `/v2/_catalog` and `/_meta/*` require Basic or Bearer auth.

Endpoints:

- `GET /v2/_catalog?n=<N>&last=<repo>` — standard registry catalog listing (best-effort).
- `GET /_meta/catalog?n=<N>&last=<repo>[&org=<org>]` — one-call repo listing with timestamp metadata.
- `GET /_meta/orgs?n=<N>&last=<org>` — list org names (derived from `org/repo`).
- `GET /_meta/orgs/<org>/repos?n=<N>&last=<repo>` — list repos under an org with timestamps.
- `GET /_meta/repos/<org>/<repo>` — timestamp metadata for a single repo.

Long-running robustness (filesystem backend only):

- `UPLOAD_GC_ENABLED` (`1`/`0`; default `1`)
- `UPLOAD_GC_INTERVAL_SECS` (default `3600`)
- `UPLOAD_GC_MAX_AGE_SECS` (default `86400`)

The server performs graceful shutdown on SIGTERM/SIGINT.

Auth/token (for Docker/Podman clients):

- `PUBLIC_URL` (recommended; e.g. `http://127.0.0.1:5000` or `https://127.0.0.1:5000`)
- `TOKEN_SERVICE` (default `registry-rust`)
- `TOKEN_SIGNING_KEY` (default: random per process; set a fixed secret for stable long-running deployments)
- `TOKEN_TTL_SECS` (default `600`)

TLS:

- `TLS_CERT_PATH`, `TLS_KEY_PATH` (if both set, the server listens with HTTPS)

ACME TLS provisioning (optional):

- Configure `[server.tls.acme]` in the TOML to generate/renew `cert.pem` + `key.pem` during server start.
- Providers:
  - `ispone` (HTTP bridge): `TLS_ACME_ISPONE_BASE_URL`, `TLS_ACME_ISPONE_AUTHORIZATION`
  - `exec_path` (external hook): `TLS_ACME_EXEC_PATH`

S3 backend:

- `STORAGE_S3_ENDPOINT` (optional; for S3-compatible endpoints like MinIO)
- `STORAGE_S3_REGION`, `STORAGE_S3_BUCKET`, `STORAGE_S3_PREFIX`
- Credentials are read from standard AWS env vars (e.g. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).
