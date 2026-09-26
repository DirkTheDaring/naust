#!/usr/bin/env bash
set -euo pipefail

# Podman-based smoke test (real client) for the registry.
# Requires: podman
# Does NOT require network access: builds a tiny local image (FROM scratch) with a small layer.

ADDR="${ADDR:-127.0.0.1:5000}"
USER="${REGISTRY_USERNAME:-demo}"
PASS="${REGISTRY_PASSWORD:-demo}"
REPO="${REPO:-myrepo}"
TAG="${TAG:-latest}"

# Optional: load registry configuration from a TOML file.
# When set, the registry uses: defaults < config file < env vars.
# This smoke script will still set TLS env vars (to use a temporary cert), but will not
# force push auth/allowlist env vars in TOML mode.
CONFIG_PATH="${CONFIG_PATH:-}"
USE_TOML=0
if [[ -n "$CONFIG_PATH" ]]; then
  USE_TOML=1
  export CONFIG_PATH
fi

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target2}"

# Registry TLS certs (self-signed is fine; podman will use --tls-verify=false).
CERT_PATH="${TLS_CERT_PATH:-$PWD/certs/cert.pem}"
KEY_PATH="${TLS_KEY_PATH:-$PWD/certs/key.pem}"

log() { printf '%s\n' "$*"; }

if ! command -v podman >/dev/null 2>&1; then
  echo "podman not found" >&2
  exit 1
fi

if [[ ! -f "$CERT_PATH" || ! -f "$KEY_PATH" ]]; then
  if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl not found (needed to generate a temporary TLS cert)" >&2
    echo "Either install openssl or set TLS_CERT_PATH/TLS_KEY_PATH." >&2
    exit 1
  fi

  TLS_TMP_DIR="$(mktemp -d)"
  CERT_PATH="$TLS_TMP_DIR/cert.pem"
  KEY_PATH="$TLS_TMP_DIR/key.pem"

  log "Generating temporary self-signed TLS cert"
  openssl req -x509 -nodes -newkey rsa:2048 \
    -keyout "$KEY_PATH" \
    -out "$CERT_PATH" \
    -days 1 \
    -subj "/CN=127.0.0.1" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
    >/dev/null 2>&1
fi

# Build a tiny image locally (no network): scratch + one layer.
TMP_DIR="$(mktemp -d)"
cleanup_tmp() { rm -rf "$TMP_DIR"; }
trap cleanup_tmp EXIT

printf 'hello registry\n' >"$TMP_DIR/hello.txt"
cat >"$TMP_DIR/Dockerfile" <<'EOF'
FROM scratch
ADD hello.txt /hello.txt
LABEL org.opencontainers.image.title="naust-podman-smoke"
EOF

LOCAL_IMG="naust-smoke:local"
log "Building local test image ($LOCAL_IMG)"
podman build -t "$LOCAL_IMG" "$TMP_DIR" >/dev/null

export REGISTRY_USERNAME="$USER"
export REGISTRY_PASSWORD="$PASS"

if [[ $USE_TOML -eq 0 ]]; then
  export REGISTRY_PUSH_ALLOW_REPOS="$REPO"
fi

# Always run this smoke under TLS (podman uses --tls-verify=false for self-signed).
export PUBLIC_URL="https://$ADDR"
export TLS_CERT_PATH="$CERT_PATH"
export TLS_KEY_PATH="$KEY_PATH"

log "Starting registry on $ADDR (TLS enabled)"
RUST_LOG=warn "./$CARGO_TARGET_DIR/debug/naust" server >/tmp/naust-podman.log 2>&1 &
PID=$!
cleanup() {
  kill "$PID" >/dev/null 2>&1 || true
  podman logout --tls-verify=false "$ADDR" >/dev/null 2>&1 || true
  if [[ -n "${TLS_TMP_DIR:-}" ]]; then
    rm -rf "$TLS_TMP_DIR" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

sleep 0.5

log "Logging in"
podman login --tls-verify=false -u "$USER" -p "$PASS" "$ADDR" >/dev/null

DEST_IMG="$ADDR/$REPO:$TAG"
log "Tagging $LOCAL_IMG -> $DEST_IMG"
podman tag "$LOCAL_IMG" "$DEST_IMG"

log "Pushing"
podman push --tls-verify=false "$DEST_IMG" >/dev/null

log "Removing local dest tag and pulling back"
podman rmi "$DEST_IMG" >/dev/null 2>&1 || true
podman pull --tls-verify=false "$DEST_IMG" >/dev/null

log "OK"
