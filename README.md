# registry-rust

Minimal Docker/OCI registry (Distribution v2-ish) in Rust.

## Run locally (cargo)

```sh
export REGISTRY_USERNAME=demo REGISTRY_PASSWORD=demo
export REGISTRY_PUSH_ALLOW_REPOS=myrepo
CARGO_TARGET_DIR=target2 cargo run
```

Smoke test:

```sh
CARGO_TARGET_DIR=target2 REGISTRY_USERNAME=demo REGISTRY_PASSWORD=demo scripts/smoke.sh
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

### Config file (TOML)

You can optionally load configuration from a TOML file and still override any value via environment variables.

- Enable: set `CONFIG_PATH` to a TOML file (see `configs/registry.example.toml` and `configs/registry.best_practice.toml`).
- Precedence: defaults < config file < env vars
- Best-practice profile:
  - Set `BEST_PRACTICE=1`, or set `[profile].name = "best_practice"` in the TOML.
  - In best-practice mode, `TOKEN_SIGNING_KEY` (or `token.signing_key` in the TOML) is required; the process fails fast if missing.

Example:

```sh
CONFIG_PATH=./configs/registry.best_practice.toml \
TOKEN_SIGNING_KEY='replace-me-with-a-long-random-secret' \
CARGO_TARGET_DIR=target2 cargo run
```

### Canonical env var namespace

All options also support a canonical `REGISTRY__...` env var namespace (double-underscore separators), for example:

- `REGISTRY__SERVER__LISTEN_ADDR` (alias for `LISTEN_ADDR`)
- `REGISTRY__TOKEN__SIGNING_KEY` (alias for `TOKEN_SIGNING_KEY`)
- `REGISTRY__UPLOADS__DISALLOW_MONOLITHIC_UPLOADS` (alias for `DISALLOW_MONOLITHIC_UPLOADS`)

Existing env var names continue to work.

### Config option inventory

Precedence: defaults < config file < env vars

Best-practice profile (`BEST_PRACTICE=1` or `[profile].name="best_practice"`) changes these defaults:

- `features.allow_tag_overwrite`: default `false` (instead of `true`)
- `uploads.disallow_monolithic_uploads`: default `true` (instead of `false`)
- `catalog.requires_auth`: default `true` (instead of `false`)
- `timeouts.request_timeout_secs`: default `60` (instead of `300`)
- `timeouts.upload_request_timeout_secs`: default `7200` (instead of `3600`)
- `token.signing_key`: required (no random fallback)

| Purpose | TOML key | Canonical env | Legacy env | Default |
| --- | --- | --- | --- | --- |
| Config file path | (n/a) | `REGISTRY__CONFIG_PATH` | `CONFIG_PATH` | unset |
| Best-practice profile | `profile.name` | (n/a) | `BEST_PRACTICE` | off |
| Listen addr | `server.listen_addr` | `REGISTRY__SERVER__LISTEN_ADDR` | `LISTEN_ADDR` | `127.0.0.1:5000` |
| Public URL | `server.public_url` | `REGISTRY__SERVER__PUBLIC_URL` | `PUBLIC_URL` | unset |
| TLS cert path | `server.tls.cert_path` | `REGISTRY__SERVER__TLS__CERT_PATH` | `TLS_CERT_PATH` | unset |
| TLS key path | `server.tls.key_path` | `REGISTRY__SERVER__TLS__KEY_PATH` | `TLS_KEY_PATH` | unset |
| Push username | `auth.push.username` | `REGISTRY__AUTH__PUSH__USERNAME` | `REGISTRY_USERNAME` | unset |
| Push password | `auth.push.password` | `REGISTRY__AUTH__PUSH__PASSWORD` | `REGISTRY_PASSWORD` | unset |
| Push allowlist | `auth.push.allow_repos` | `REGISTRY__AUTH__PUSH__ALLOW_REPOS` | `REGISTRY_PUSH_ALLOW_REPOS` | unset |
| Storage backend | `storage.backend` | `REGISTRY__STORAGE__BACKEND` | `STORAGE_BACKEND` | `fs` |
| FS root | `storage.fs.root` | `REGISTRY__STORAGE__FS__ROOT` | `STORAGE_FS_ROOT` | `./data` |
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
| Request timeout | `timeouts.request_timeout_secs` | `REGISTRY__TIMEOUTS__REQUEST_TIMEOUT_SECS` | `REQUEST_TIMEOUT_SECS` | `300` (best-practice: `60`) |
| Upload request timeout | `timeouts.upload_request_timeout_secs` | `REGISTRY__TIMEOUTS__UPLOAD_REQUEST_TIMEOUT_SECS` | `UPLOAD_REQUEST_TIMEOUT_SECS` | `3600` (best-practice: `7200`) |
| Disallow monolithic uploads | `uploads.disallow_monolithic_uploads` | `REGISTRY__UPLOADS__DISALLOW_MONOLITHIC_UPLOADS` | `DISALLOW_MONOLITHIC_UPLOADS` | `false` (best-practice: `true`) |
| Catalog requires auth | `catalog.requires_auth` | `REGISTRY__CATALOG__REQUIRES_AUTH` | `CATALOG_REQUIRES_AUTH` | `false` (best-practice: `true`) |
| Token service | `token.service` | `REGISTRY__TOKEN__SERVICE` | `TOKEN_SERVICE` | `registry-rust` |
| Token signing key | `token.signing_key` | `REGISTRY__TOKEN__SIGNING_KEY` | `TOKEN_SIGNING_KEY` | random per-process (best-practice: required) |
| Token TTL | `token.ttl_secs` | `REGISTRY__TOKEN__TTL_SECS` | `TOKEN_TTL_SECS` | `600` |

- `LISTEN_ADDR` (default `127.0.0.1:5000`)
- `REGISTRY_USERNAME`, `REGISTRY_PASSWORD` (if unset, pushes are rejected)
- `REGISTRY_PUSH_ALLOW_REPOS` (optional, comma-separated; supports `org/*` prefixes and `*`)
- `STORAGE_BACKEND` (`fs` or `s3`)
- `STORAGE_FS_ROOT` (default `./data`)
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

S3 backend:

- `STORAGE_S3_ENDPOINT` (optional; for S3-compatible endpoints like MinIO)
- `STORAGE_S3_REGION`, `STORAGE_S3_BUCKET`, `STORAGE_S3_PREFIX`
- Credentials are read from standard AWS env vars (e.g. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).
