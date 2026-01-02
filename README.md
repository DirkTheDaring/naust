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
