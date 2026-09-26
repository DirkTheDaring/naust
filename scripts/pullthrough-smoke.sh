#!/usr/bin/env bash
set -euo pipefail

# Pull-through cache smoke test (offline).
#
# Starts two local naust instances:
#  1) Upstream registry (no proxy) on UPSTREAM_ADDR
#  2) Proxy registry (proxy enabled) on PROXY_ADDR pointing at the upstream
#
# Validates:
#  - proxy pulls populate the cache root (default: ./data/cache)
#  - proxy primary store stays empty (default: ./data/primary)
#  - second pull doesn't increase cached blob count
#
# Requires: curl, python3, sha256sum

PROXY_ADDR="${PROXY_ADDR:-127.0.0.1:5000}"
UPSTREAM_ADDR="${UPSTREAM_ADDR:-127.0.0.1:5001}"
REPO="${REPO:-library/alpine}"
TAG="${TAG:-latest}"

USER="${REGISTRY_USERNAME:-demo}"
PASS="${REGISTRY_PASSWORD:-demo}"

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target2}"
BIN="./$CARGO_TARGET_DIR/debug/naust"

DATA_ROOT="${DATA_ROOT:-$PWD/data}"
PRIMARY_ROOT="${PRIMARY_ROOT:-$DATA_ROOT/primary}"
CACHE_ROOT="${CACHE_ROOT:-$DATA_ROOT/cache}"
UPSTREAM_ROOT="${UPSTREAM_ROOT:-$DATA_ROOT/upstream}"

log() { printf '%s\n' "$*"; }

die() {
  echo "ERROR: $*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || die "$1 not found"
}

need curl
need python3
need sha256sum

log "Building registry binary ($BIN)"
CARGO_TARGET_DIR="$CARGO_TARGET_DIR" cargo build -q

TMP_UP="$(mktemp)"
TMP_PROXY="$(mktemp)"
cleanup_cfg() {
  rm -f "$TMP_UP" "$TMP_PROXY" >/dev/null 2>&1 || true
}

PID_UP=""
PID_PROXY=""
cleanup() {
  [[ -n "$PID_PROXY" ]] && kill "$PID_PROXY" >/dev/null 2>&1 || true
  [[ -n "$PID_UP" ]] && kill "$PID_UP" >/dev/null 2>&1 || true
  cleanup_cfg
}
trap cleanup EXIT

mkdir -p "$DATA_ROOT" "$PRIMARY_ROOT" "$CACHE_ROOT" "$UPSTREAM_ROOT"

# Clear any old content for deterministic assertions.
rm -rf "$PRIMARY_ROOT/blobs" "$PRIMARY_ROOT/repos" >/dev/null 2>&1 || true
rm -rf "$CACHE_ROOT/blobs" "$CACHE_ROOT/repos" "$CACHE_ROOT/proxy-index" >/dev/null 2>&1 || true
rm -rf "$UPSTREAM_ROOT/blobs" "$UPSTREAM_ROOT/repos" >/dev/null 2>&1 || true

cat >"$TMP_UP" <<EOF
[server]
listen_addr = "$UPSTREAM_ADDR"
public_url = "http://$UPSTREAM_ADDR"

[storage]
backend = "fs"

[storage.fs]
root = "$UPSTREAM_ROOT"

[auth.push]
username = "$USER"
password = "$PASS"
allow_repos = ["*"]
EOF

cat >"$TMP_PROXY" <<EOF
[server]
listen_addr = "$PROXY_ADDR"
public_url = "http://$PROXY_ADDR"

[storage]
backend = "fs"

[storage.fs]
root = "$PRIMARY_ROOT"

[auth.push]
# Proxy instance doesn't need pushes for this smoke.
username = ""
password = ""

[proxy]
enabled = true
mode = "allowlist"

[proxy.upstream]
base_url = "http://$UPSTREAM_ADDR"

[proxy.safety]
allowed_upstream_hosts = ["127.0.0.1"]
allowed_repo_prefixes = ["library/"]
# Allow loopback for this offline smoke.
block_private_networks = false
max_concurrent_upstream = 8

[proxy.cache]
fs_root = "$CACHE_ROOT"
index_path = "$CACHE_ROOT/proxy-index"
max_cache_bytes = 2147483648
gc_interval_secs = 3600

[[proxy.repos]]
match = "$REPO"
tag_policy = "digest_only"
EOF

unset TLS_CERT_PATH TLS_KEY_PATH

log "Starting upstream registry ($UPSTREAM_ADDR)"
CONFIG_PATH="$TMP_UP" RUST_LOG=warn "$BIN" server >/tmp/naust-upstream.log 2>&1 &
PID_UP=$!
sleep 0.5

log "Seeding upstream with $REPO:$TAG"
resp=$(curl -isS -u "$USER:$PASS" -X POST "http://$UPSTREAM_ADDR/v2/$REPO/blobs/uploads/")
loc=$(printf '%s' "$resp" | awk -F': ' 'tolower($1)=="location"{gsub("\r","",$2); print $2}')
[[ -n "$loc" ]] || die "missing Location for upload (upstream)"

config_data='{"architecture":"amd64","os":"linux"}'
_=$(curl -fsS -u "$USER:$PASS" -X PATCH --data-binary "$config_data" "http://$UPSTREAM_ADDR$loc")
config_digest=$(printf '%s' "$config_data" | sha256sum | awk '{print $1}')
_=$(curl -fsS -u "$USER:$PASS" -X PUT "http://$UPSTREAM_ADDR$loc?digest=sha256:$config_digest")
config_size=${#config_data}

resp=$(curl -isS -u "$USER:$PASS" -X POST "http://$UPSTREAM_ADDR/v2/$REPO/blobs/uploads/")
loc=$(printf '%s' "$resp" | awk -F': ' 'tolower($1)=="location"{gsub("\r","",$2); print $2}')
[[ -n "$loc" ]] || die "missing Location for upload (upstream layer)"

layer_data='hello-layer'
_=$(curl -fsS -u "$USER:$PASS" -X PATCH --data-binary "$layer_data" "http://$UPSTREAM_ADDR$loc")
layer_digest=$(printf '%s' "$layer_data" | sha256sum | awk '{print $1}')
_=$(curl -fsS -u "$USER:$PASS" -X PUT "http://$UPSTREAM_ADDR$loc?digest=sha256:$layer_digest")
layer_size=${#layer_data}

manifest=$(printf '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:%s","size":%d},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"sha256:%s","size":%d}]}' \
  "$config_digest" \
  "$config_size" \
  "$layer_digest" \
  "$layer_size")
_=$(curl -fsS -u "$USER:$PASS" -X PUT -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' --data-binary "$manifest" \
  "http://$UPSTREAM_ADDR/v2/$REPO/manifests/$TAG")

log "Starting proxy registry ($PROXY_ADDR)"
CONFIG_PATH="$TMP_PROXY" RUST_LOG=warn "$BIN" server >/tmp/naust-proxy.log 2>&1 &
PID_PROXY=$!
sleep 0.6

log "Fetching manifest via proxy (should populate cache)"
tmp_manifest="$(mktemp)"
tmp_headers="$(mktemp)"
http_code="$(curl -sS -H 'Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
  -D "$tmp_headers" \
  -o "$tmp_manifest" \
  -w '%{http_code}' \
  "http://$PROXY_ADDR/v2/$REPO/manifests/$TAG" || true)"
if [[ "$http_code" != "200" ]]; then
  log "Manifest fetch HTTP $http_code; headers/body/logs follow"
  cat "$tmp_headers" || true
  head -c 1000 "$tmp_manifest" || true
  echo
  tail -n 160 /tmp/naust-proxy.log || true
  tail -n 160 /tmp/naust-upstream.log || true
  die "manifest fetch failed"
fi
if [[ ! -s "$tmp_manifest" ]]; then
  log "Empty manifest body; showing response and logs"
  cat "$tmp_headers" || true
  tail -n 160 /tmp/naust-proxy.log || true
  tail -n 160 /tmp/naust-upstream.log || true
  die "empty manifest body"
fi
if ! python3 - "$tmp_manifest" >/dev/null 2>&1 <<'PY'; then
import json,sys
with open(sys.argv[1],'rb') as f:
  json.load(f)
PY
  log "Manifest body is not valid JSON; headers/body/logs follow"
  cat "$tmp_headers" || true
  head -c 1000 "$tmp_manifest" || true
  echo
  tail -n 160 /tmp/naust-proxy.log || true
  tail -n 160 /tmp/naust-upstream.log || true
  die "invalid manifest JSON"
fi

mapfile -t digests < <(python3 - "$tmp_manifest" <<'PY'
import json,sys
with open(sys.argv[1],'rb') as f:
  m=json.load(f)
outs=[]
if isinstance(m,dict):
  cfg=m.get('config')
  if isinstance(cfg,dict) and isinstance(cfg.get('digest'),str):
    outs.append(cfg['digest'])
  layers=m.get('layers')
  if isinstance(layers,list):
    for l in layers:
      if isinstance(l,dict) and isinstance(l.get('digest'),str):
        outs.append(l['digest'])
for d in outs:
  print(d)
PY
)

rm -f "$tmp_manifest" "$tmp_headers" >/dev/null 2>&1 || true

[[ ${#digests[@]} -gt 0 ]] || die "no blob digests found in manifest"

log "Fetching ${#digests[@]} blobs via proxy"
for d in "${digests[@]}"; do
  curl -fsS "http://$PROXY_ADDR/v2/$REPO/blobs/$d" >/dev/null
done

cache_blob_count="$( (find "$CACHE_ROOT/blobs/sha256" -type f 2>/dev/null || true) | wc -l | awk '{print $1}' )"
primary_blob_count="$( (find "$PRIMARY_ROOT/blobs/sha256" -type f 2>/dev/null || true) | wc -l | awk '{print $1}' )"

log "Cache blobs: $cache_blob_count"
log "Primary blobs: $primary_blob_count"

[[ "$cache_blob_count" -gt 0 ]] || die "expected cached blobs under $CACHE_ROOT"
[[ "$primary_blob_count" -eq 0 ]] || die "expected no blobs under primary store ($PRIMARY_ROOT)"

log "Second fetch should be served from cache (no additional files expected)"
cache_before="$cache_blob_count"
for d in "${digests[@]}"; do
  curl -fsS "http://$PROXY_ADDR/v2/$REPO/blobs/$d" >/dev/null
done
cache_after="$( (find "$CACHE_ROOT/blobs/sha256" -type f 2>/dev/null || true) | wc -l | awk '{print $1}' )"
[[ "$cache_after" -eq "$cache_before" ]] || die "cache blob count changed unexpectedly ($cache_before -> $cache_after)"

log "OK"
