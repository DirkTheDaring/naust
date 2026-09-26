#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PORT="${PORT:-5000}"
ADDR="127.0.0.1:${PORT}"

CONFIG_PATH="${CONFIG_PATH:-$ROOT_DIR/configs/registry.example.toml}"

export LISTEN_ADDR="$ADDR"
export CONFIG_PATH

# Keep pushes enabled for the smoke.
export REGISTRY_USERNAME="${REGISTRY_USERNAME:-demo}"
export REGISTRY_PASSWORD="${REGISTRY_PASSWORD:-demo}"
export REGISTRY_PUSH_ALLOW_REPOS="${REGISTRY_PUSH_ALLOW_REPOS:-myrepo}"

# Ensure tokens survive process restarts during the smoke.
export TOKEN_SIGNING_KEY="${TOKEN_SIGNING_KEY:-smoke-signing-key-please-change}"

echo "[config-smoke] Using CONFIG_PATH=$CONFIG_PATH"

# Run a throwaway registry instance.
BIN="${BIN:-$ROOT_DIR/target2/debug/naust}"
if [[ ! -x "$BIN" ]]; then
  echo "[config-smoke] building (debug) into target2 ..."
  (cd "$ROOT_DIR" && CARGO_TARGET_DIR=target2 cargo build -q)
fi

"$BIN" server &
PID=$!

cleanup() {
  kill "$PID" >/dev/null 2>&1 || true
  wait "$PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Wait until the registry is ready.
for i in {1..50}; do
  code=$(curl -s -o /dev/null -w "%{http_code}" "http://$ADDR/v2/" || true)
  if [[ "$code" == "200" || "$code" == "401" ]]; then
    break
  fi
  sleep 0.1
  if [[ $i -eq 50 ]]; then
    echo "[config-smoke] registry did not become ready (code=$code)"
    exit 1
  fi
done

# Basic sanity: catalog should be reachable (may be 401 if configured that way).
code=$(curl -s -o /dev/null -w "%{http_code}" "http://$ADDR/v2/_catalog?n=1" || true)
if [[ "$code" != "200" && "$code" != "401" ]]; then
  echo "[config-smoke] unexpected /v2/_catalog status: $code"
  exit 1
fi

echo "[config-smoke] OK"
