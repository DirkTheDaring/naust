#!/usr/bin/env bash
set -euo pipefail

# Smoke test for catalog/org listing + timestamp metadata.
# Requires: curl, sha256sum

ADDR="${ADDR:-127.0.0.1:5000}"
USER="${REGISTRY_USERNAME:-demo}"
PASS="${REGISTRY_PASSWORD:-demo}"

# Optional: load registry configuration from a TOML file.
# Note: this script intentionally toggles CATALOG_REQUIRES_AUTH via env between phases.
if [[ -n "${CONFIG_PATH:-}" ]]; then
  export CONFIG_PATH
  log "Using CONFIG_PATH=$CONFIG_PATH"
fi

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target2}"
BIN="./$CARGO_TARGET_DIR/debug/naust"

log() { printf '%s\n' "$*"; }

die() {
  echo "ERROR: $*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || die "$1 not found"
}

need curl
need sha256sum

log "Building registry binary ($BIN)"
CARGO_TARGET_DIR="$CARGO_TARGET_DIR" cargo build -q

# Deterministic HTTP for this smoke test.
unset TLS_CERT_PATH TLS_KEY_PATH
export PUBLIC_URL="http://$ADDR"

export REGISTRY_USERNAME="$USER"
export REGISTRY_PASSWORD="$PASS"
# Allow pushing multiple repos.
export REGISTRY_PUSH_ALLOW_REPOS="*"

push_one() {
  local repo="$1"
  local tag="$2"
  local blob_data="$3"

  log "Pushing $repo:$tag"

  local resp loc blob_digest manifest

  resp=$(curl -isS -u "$USER:$PASS" -X POST "http://$ADDR/v2/$repo/blobs/uploads/")
  loc=$(printf '%s' "$resp" | awk -F': ' 'tolower($1)=="location"{gsub("\r","",$2); print $2}')
  [[ -n "$loc" ]] || die "missing Location for upload (repo=$repo)"

  _=$(curl -fsS -u "$USER:$PASS" -X PATCH --data-binary "$blob_data" "http://$ADDR$loc")
  blob_digest=$(printf '%s' "$blob_data" | sha256sum | awk '{print $1}')
  _=$(curl -fsS -u "$USER:$PASS" -X PUT "http://$ADDR$loc?digest=sha256:$blob_digest")

  manifest=$(printf '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:%s","size":0},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"sha256:%s","size":%d}]}' \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "$blob_digest" \
    ${#blob_data})

  _=$(curl -fsS -u "$USER:$PASS" -X PUT -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' --data-binary "$manifest" "http://$ADDR/v2/$repo/manifests/$tag")
}

request_expect_code() {
  local url="$1"
  local expected="$2"
  local extra_args=()
  shift 2 || true
  extra_args+=("$@")

  local code
  code=$(curl -sS -o /dev/null -w '%{http_code}' "${extra_args[@]}" "$url")
  [[ "$code" == "$expected" ]] || die "expected $expected, got $code for $url"
}

start_registry() {
  local require_auth="$1"
  export CATALOG_REQUIRES_AUTH="$require_auth"

  log "Starting registry on $ADDR (CATALOG_REQUIRES_AUTH=$require_auth)"
  RUST_LOG=warn "$BIN" server >/tmp/naust-catalog.log 2>&1 &
  PID=$!
  export PID
}

stop_registry() {
  if [[ -n "${PID:-}" ]]; then
    kill "$PID" >/dev/null 2>&1 || true
    unset PID
  fi
}

cleanup() {
  stop_registry
}
trap cleanup EXIT

# --------- Phase 1: public listing ---------
start_registry 0
sleep 0.4

# Push a few repos across two orgs.
push_one "org1/repoa" "latest" "hello-layer-a"
push_one "org1/repob" "v1" "hello-layer-b"
push_one "org2/repoc" "latest" "hello-layer-c"

log "Check /v2/_catalog pagination"
# Page 1
hdr1=$(mktemp)
body1=$(mktemp)
curl -fsS -D "$hdr1" -o "$body1" "http://$ADDR/v2/_catalog?n=2"
grep -q '"repositories"' "$body1"
# Must contain org1/repoa and org1/repob in some order (sorted by backend).
grep -q 'org1/repoa' "$body1" || true

link=$(awk -F': ' 'tolower($1)=="link"{gsub("\r","",$2); print $2}' "$hdr1" | tail -n1)
[[ -n "$link" ]] || die "missing Link header for catalog page 1"

next_path=$(printf '%s' "$link" | sed -n 's/^<\([^>]*\)>.*/\1/p')
[[ -n "$next_path" ]] || die "failed to parse next link path"

# Page 2
curl -fsS -o "$body1" "http://$ADDR$next_path"
grep -q 'org2/repoc' "$body1" || die "expected org2/repoc on catalog page 2"

log "Check org listing + repo listing"
resp_orgs=$(curl -fsS "http://$ADDR/_meta/orgs?n=1")
grep -q '"orgs"' <<<"$resp_orgs"

resp_org1=$(curl -fsS "http://$ADDR/_meta/orgs/org1/repos")
grep -q 'org1/repoa' <<<"$resp_org1"
grep -q '"last_change"' <<<"$resp_org1"

auth_filtered=$(curl -fsS "http://$ADDR/_meta/catalog?org=org1")
grep -q 'org1/repoa' <<<"$auth_filtered"
! grep -q 'org2/repoc' <<<"$auth_filtered" || die "org filter failed"

rm -f "$hdr1" "$body1"

# --------- Phase 2: auth-gated listing ---------
stop_registry
sleep 0.2
start_registry 1
sleep 0.4

log "Unauthenticated listing is denied"
request_expect_code "http://$ADDR/v2/_catalog" 401
request_expect_code "http://$ADDR/_meta/catalog" 401

log "Authenticated listing works (Basic)"
request_expect_code "http://$ADDR/v2/_catalog" 200 -u "$USER:$PASS"
request_expect_code "http://$ADDR/_meta/catalog" 200 -u "$USER:$PASS"

log "OK"
