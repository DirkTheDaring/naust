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

log "Starting MinIO + registry (S3 backend)"
docker compose -f docker-compose.yml -f docker-compose.minio.yml down -v >/dev/null 2>&1 || true
docker compose -f docker-compose.yml -f docker-compose.minio.yml up -d --build

cleanup() {
  docker compose -f docker-compose.yml -f docker-compose.minio.yml down -v >/dev/null 2>&1 || true
}

interrupted=0

on_exit() {
  code=$?
  if [[ $code -ne 0 && $interrupted -eq 0 ]]; then
    log "FAILED (exit=$code). Dumping compose logs:"
    docker compose -f docker-compose.yml -f docker-compose.minio.yml logs --no-color registry minio minio-init 2>/dev/null || true
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
resp=$(curl -isS -u "$USER:$PASS" -X POST "http://$ADDR/v2/$REPO/blobs/uploads/")
loc=$(printf '%s' "$resp" | awk -F': ' 'tolower($1)=="location"{gsub("\r","",$2); print $2}')

blob_data='hello-layer-s3'
_=$(curl -fsS -u "$USER:$PASS" -X PATCH --data-binary "$blob_data" "http://$ADDR$loc")
blob_digest=$(printf '%s' "$blob_data" | sha256sum | awk '{print $1}')
_=$(curl -fsS -u "$USER:$PASS" -X PUT "http://$ADDR$loc?digest=sha256:$blob_digest")

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
