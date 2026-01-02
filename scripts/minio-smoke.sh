#!/usr/bin/env bash
set -euo pipefail

# End-to-end smoke test using MinIO (S3 backend) via docker compose.
# Requires: docker compose (or podman compose), curl

ADDR="${ADDR:-127.0.0.1:5000}"
USER="${REGISTRY_USERNAME:-demo}"
PASS="${REGISTRY_PASSWORD:-demo}"
REPO="${REPO:-myrepo}"
TAG="${TAG:-latest}"

log() { printf '%s\n' "$*"; }

compose() {
  if command -v docker >/dev/null 2>&1; then
    if docker compose version >/dev/null 2>&1; then
      docker compose "$@"
      return
    fi
  fi
  if command -v podman >/dev/null 2>&1; then
    if podman compose version >/dev/null 2>&1; then
      podman compose "$@"
      return
    fi
  fi
  if command -v podman-compose >/dev/null 2>&1; then
    podman-compose "$@"
    return
  fi
  echo "No compose implementation found (need 'docker compose', 'podman compose', or 'podman-compose')." >&2
  exit 127
}

log "Starting MinIO + registry (S3 backend)"

# Optional: run registry configured via TOML.
# Enable by setting REGISTRY_TOML_PATH (or CONFIG_PATH for compatibility).
USE_TOML=0
if [[ -n "${REGISTRY_TOML_PATH:-}" || -n "${CONFIG_PATH:-}" ]]; then
  USE_TOML=1
fi

if [[ -n "${CONFIG_PATH:-}" && -z "${REGISTRY_TOML_PATH:-}" ]]; then
  # In compose, CONFIG_PATH is the in-container path. Use REGISTRY_TOML_PATH for the host file.
  # If the user provided CONFIG_PATH anyway, default to a sensible MinIO example.
  export REGISTRY_TOML_PATH="${REGISTRY_TOML_PATH:-./configs/registry.minio.toml}"
fi

if [[ $USE_TOML -eq 1 ]]; then
  export REGISTRY_TOML_PATH="${REGISTRY_TOML_PATH:-./configs/registry.minio.toml}"
  log "Using TOML config: REGISTRY_TOML_PATH=$REGISTRY_TOML_PATH"
  COMPOSE_FILES=( -f docker-compose.yml -f docker-compose.minio.yml -f docker-compose.config.yml )
else
  COMPOSE_FILES=( -f docker-compose.yml -f docker-compose.minio.yml )
fi

compose "${COMPOSE_FILES[@]}" down -v >/dev/null 2>&1 || true
compose "${COMPOSE_FILES[@]}" up -d --build

cleanup() {
  compose "${COMPOSE_FILES[@]}" down -v >/dev/null 2>&1 || true
}

interrupted=0

on_exit() {
  code=$?
  if [[ $code -ne 0 && $interrupted -eq 0 ]]; then
    log "FAILED (exit=$code). Dumping compose logs:"
    compose "${COMPOSE_FILES[@]}" logs registry minio minio-init 2>/dev/null || true
  fi
  cleanup
}
trap on_exit EXIT
trap 'interrupted=1; exit 130' INT TERM

log "Waiting for registry to respond"
for _ in {1..60}; do
  code=$(curl -sS -o /dev/null -w '%{http_code}' "http://$ADDR/v2/" || true)
  if [[ "$code" == "200" || "$code" == "401" ]]; then
    break
  fi
  sleep 0.5
done

log "Push denied without auth"
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "http://$ADDR/v2/$REPO/blobs/uploads/")
[[ "$code" == "401" ]]

log "Upload blob"
resp=""
for _ in {1..60}; do
  resp=$(curl -sS -u "$USER:$PASS" -D - -o /dev/null -X POST "http://$ADDR/v2/$REPO/blobs/uploads/" || true)
  status=$(printf '%s' "$resp" | awk 'NR==1{print $2}')
  if [[ "$status" == "202" ]]; then
    break
  fi
  if [[ "$status" == "401" ]]; then
    echo "upload create unauthorized (check REGISTRY_USERNAME/REGISTRY_PASSWORD)" >&2
    echo "$resp" >&2
    exit 2
  fi
  sleep 1
done

status=$(printf '%s' "$resp" | awk 'NR==1{print $2}')
if [[ "$status" != "202" ]]; then
  echo "upload create did not succeed (status=${status:-<none>})" >&2
  echo "$resp" >&2
  exit 2
fi

loc=$(printf '%s' "$resp" | awk -F': ' 'tolower($1)=="location"{gsub("\r","",$2); print $2}')

if [[ -z "$loc" ]]; then
  echo "missing Location header from upload create" >&2
  echo "$resp" >&2
  exit 2
fi

upload_url="$loc"
if [[ "$upload_url" != http://* && "$upload_url" != https://* ]]; then
  upload_url="http://$ADDR$upload_url"
fi

blob_data='hello-layer-s3'
_=$(curl -fsS -u "$USER:$PASS" -X PATCH --data-binary "$blob_data" "$upload_url")
blob_digest=$(printf '%s' "$blob_data" | sha256sum | awk '{print $1}')
_=$(curl -fsS -u "$USER:$PASS" -X PUT "$upload_url?digest=sha256:$blob_digest")

log "Push manifest (tag)"
manifest=$(printf '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:%s","size":0},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"sha256:%s","size":%d}]}' \
  "0000000000000000000000000000000000000000000000000000000000000000" \
  "$blob_digest" \
  ${#blob_data})
_=$(curl -fsS -u "$USER:$PASS" -X PUT -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' --data-binary "$manifest" "http://$ADDR/v2/$REPO/manifests/$TAG")

log "Anonymous pull manifest"
_=$(curl -fsS "http://$ADDR/v2/$REPO/manifests/$TAG" >/dev/null)

log "Anonymous pull blob"
out=$(curl -fsS "http://$ADDR/v2/$REPO/blobs/sha256:$blob_digest")
[[ "$out" == "$blob_data" ]]

log "OK"
