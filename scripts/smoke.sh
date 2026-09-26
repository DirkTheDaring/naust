#!/usr/bin/env bash
set -euo pipefail

# Minimal smoke test for the MVP registry implementation.
# Requires: curl, sha256sum

ADDR="${ADDR:-127.0.0.1:5000}"
USER="${REGISTRY_USERNAME:-demo}"
PASS="${REGISTRY_PASSWORD:-demo}"
REPO="${REPO:-myrepo}"
TAG="${TAG:-latest}"

log() { printf '%s\n' "$*"; }

# Optional: load registry configuration from a TOML file.
# In TOML mode, we avoid forcing push auth / allowlist via env vars,
# so the TOML can control them. Curl still uses USER/PASS for auth.
CONFIG_PATH="${CONFIG_PATH:-}"
USE_TOML=0
if [[ -n "$CONFIG_PATH" ]]; then
  USE_TOML=1
  export CONFIG_PATH
  log "Using CONFIG_PATH=$CONFIG_PATH"
fi

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target}"
BIN="./$CARGO_TARGET_DIR/debug/naust"
if [[ -f "./$CARGO_TARGET_DIR/release/naust" ]]; then
  BIN="./$CARGO_TARGET_DIR/release/naust"
fi

export REGISTRY_USERNAME="$USER"
export REGISTRY_PASSWORD="$PASS"
export REGISTRY_AUTH_STRATEGY="${REGISTRY_AUTH_STRATEGY:-both}"
export STORAGE_FS_ROOT="${STORAGE_FS_ROOT:-$(mktemp -d)}"

# Make the smoke test deterministic: force plain HTTP.
unset TLS_CERT_PATH TLS_KEY_PATH
if [[ $USE_TOML -eq 0 ]]; then
  export PUBLIC_URL="http://$ADDR"
fi

# Restrict pushes to this repo only for smoke test coverage.
if [[ $USE_TOML -eq 0 ]]; then
  export REGISTRY_PUSH_ALLOW_REPOS="$REPO"
fi

log "Starting registry on $ADDR (repo=$REPO tag=$TAG)"
RUST_LOG=warn "$BIN" server >/tmp/naust.log 2>&1 &
PID=$!
cleanup() {
  kill "$PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

sleep 0.5

log "Ping /v2/"
code=$(curl -sS -o /dev/null -w '%{http_code}' "http://$ADDR/v2/")
[[ "$code" == "200" || "$code" == "401" ]]

log "Push denied without auth"
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "http://$ADDR/v2/$REPO/blobs/uploads/")
[[ "$code" == "401" ]]

log "Push denied by allowlist (wrong repo)"
code=$(curl -sS -o /dev/null -w '%{http_code}' -u "$USER:$PASS" -X POST "http://$ADDR/v2/notallowed/blobs/uploads/")
[[ "$code" == "401" || "$code" == "403" ]]

log "Upload blob"
resp=$(curl -isS -u "$USER:$PASS" -X POST "http://$ADDR/v2/$REPO/blobs/uploads/")
loc=$(printf '%s' "$resp" | awk -F': ' 'tolower($1)=="location"{gsub("\r","",$2); print $2}')

blob_data='hello-layer'
patch_resp=$(curl -isS -u "$USER:$PASS" -X PATCH --data-binary "$blob_data" "http://$ADDR$loc")
loc2=$(printf '%s' "$patch_resp" | awk -F': ' 'tolower($1)=="location"{gsub("\r","",$2); print $2}')
if [[ -n "$loc2" ]]; then loc="$loc2"; fi
blob_digest=$(printf '%s' "$blob_data" | sha256sum | awk '{print $1}')
sep="?"
[[ "$loc" == *"?"* ]] && sep="&"
_=$(curl -fsS -u "$USER:$PASS" -X PUT "http://$ADDR${loc}${sep}digest=sha256:$blob_digest")

log "Push manifest (tag)"
manifest=$(printf '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:%s","size":%d},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"sha256:%s","size":%d}]}' \
  "$blob_digest" \
  ${#blob_data} \
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
