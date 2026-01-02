#!/usr/bin/env bash
set -euo pipefail

# Minimal smoke test for the MVP registry implementation.
# Requires: curl, sha256sum

ADDR="${ADDR:-127.0.0.1:5000}"
USER="${REGISTRY_USERNAME:-demo}"
PASS="${REGISTRY_PASSWORD:-demo}"
REPO="${REPO:-myrepo}"
TAG="${TAG:-latest}"

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target2}"

export REGISTRY_USERNAME="$USER"
export REGISTRY_PASSWORD="$PASS"

# Restrict pushes to this repo only for smoke test coverage.
export REGISTRY_PUSH_ALLOW_REPOS="$REPO"

log() { printf '%s\n' "$*"; }

log "Starting registry on $ADDR (repo=$REPO tag=$TAG)"
RUST_LOG=warn "./$CARGO_TARGET_DIR/debug/registry-rust" >/tmp/registry-rust.log 2>&1 &
PID=$!
cleanup() {
  kill "$PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

sleep 0.3

log "Ping /v2/"
curl -fsS "http://$ADDR/v2/" >/dev/null

log "Push denied without auth"
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "http://$ADDR/v2/$REPO/blobs/uploads/")
[[ "$code" == "401" ]]

log "Push denied by allowlist (wrong repo)"
code=$(curl -sS -o /dev/null -w '%{http_code}' -u "$USER:$PASS" -X POST "http://$ADDR/v2/notallowed/blobs/uploads/")
[[ "$code" == "403" ]]

log "Upload blob"
resp=$(curl -isS -u "$USER:$PASS" -X POST "http://$ADDR/v2/$REPO/blobs/uploads/")
loc=$(printf '%s' "$resp" | awk -F': ' 'tolower($1)=="location"{gsub("\r","",$2); print $2}')

blob_data='hello-layer'
_=$(curl -fsS -u "$USER:$PASS" -X PATCH --data-binary "$blob_data" "http://$ADDR$loc")
blob_digest=$(printf '%s' "$blob_data" | sha256sum | awk '{print $1}')
_=$(curl -fsS -u "$USER:$PASS" -X PUT "http://$ADDR$loc?digest=sha256:$blob_digest")

log "Push manifest (tag)"
manifest=$(printf '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:%s","size":0},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"sha256:%s","size":%d}]}' \
  "0000000000000000000000000000000000000000000000000000000000000000" \
  "$blob_digest" \
  ${#blob_data})
resp2=$(curl -isS -u "$USER:$PASS" -X PUT -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' --data-binary "$manifest" "http://$ADDR/v2/$REPO/manifests/$TAG")

log "Anonymous pull manifest"
_=$(curl -fsS "http://$ADDR/v2/$REPO/manifests/$TAG" >/dev/null)

log "Anonymous pull blob"
out=$(curl -fsS "http://$ADDR/v2/$REPO/blobs/sha256:$blob_digest")
[[ "$out" == "$blob_data" ]]

log "Anonymous tags list"
tags=$(curl -fsS "http://$ADDR/v2/$REPO/tags/list")
printf '%s' "$tags" | grep -q "\"$TAG\""

log "OK"
