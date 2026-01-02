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

## Run with Docker

```sh
docker compose up --build
```

This listens on `127.0.0.1:5000` (host) and stores data in `./data`.

## Docker CLI test

Docker usually requires either TLS or marking the registry as insecure.

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

## Environment variables

- `LISTEN_ADDR` (default `127.0.0.1:5000`)
- `REGISTRY_USERNAME`, `REGISTRY_PASSWORD` (if unset, pushes are rejected)
- `REGISTRY_PUSH_ALLOW_REPOS` (optional, comma-separated; supports `org/*` prefixes and `*`)
- `STORAGE_BACKEND` (`fs` or `s3` — S3 is stubbed)
- `STORAGE_FS_ROOT` (default `./data`)
- `ALLOW_TAG_OVERWRITE` (`1`/`0`)
- `MAX_UPLOAD_BYTES` (default `5368709120`)
- `MAX_REQUEST_BODY_BYTES` (default `33554432`)
- `REQUEST_TIMEOUT_SECS` (default `300`)
