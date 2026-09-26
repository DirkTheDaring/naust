#!/usr/bin/env bash
set -euo pipefail

# Demonstrate resumable chunked uploads:
# 1) Start registry (fs backend)
# 2) Create upload session
# 3) PATCH first chunk
# 4) Restart registry
# 5) GET upload status (Range)
# 6) PATCH second chunk from resumed offset
# 7) Finalize with PUT ?digest=
# 8) HEAD/GET verify

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${PORT:-5010}"
ADDR="127.0.0.1:${PORT}"
BASE="http://${ADDR}"
USER="${REGISTRY_USERNAME:-demo}"
PASS="${REGISTRY_PASSWORD:-demo}"
REPO="${REPO:-demoorg/demorepo}"

# Optional: load registry configuration from a TOML file.
# This script still forces a temp fs backend/root + LISTEN_ADDR via env for determinism.
if [[ -n "${CONFIG_PATH:-}" ]]; then
  export CONFIG_PATH
  echo "using CONFIG_PATH=$CONFIG_PATH"
fi

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 1; }
}

require curl
require sha256sum

TMP="$(mktemp -d)"
DATA_DIR="$TMP/data"
mkdir -p "$DATA_DIR"

cleanup() {
  if [[ -n "${REG_PID:-}" ]] && kill -0 "$REG_PID" 2>/dev/null; then
    kill "$REG_PID" 2>/dev/null || true
    wait "$REG_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

start_registry() {
  (cd "$ROOT_DIR" && cargo build -q)

  REG_LOG="$TMP/registry.log"
  : >"$REG_LOG"

  echo "starting registry on $BASE"
  STORAGE_BACKEND=fs \
  STORAGE_FS_ROOT="$DATA_DIR" \
  LISTEN_ADDR="$ADDR" \
  REGISTRY_USERNAME="$USER" \
  REGISTRY_PASSWORD="$PASS" \
  REQUEST_TIMEOUT_SECS="${REQUEST_TIMEOUT_SECS:-30}" \
  UPLOAD_REQUEST_TIMEOUT_SECS="${UPLOAD_REQUEST_TIMEOUT_SECS:-300}" \
  RUST_LOG="${RUST_LOG:-info}" \
  "$ROOT_DIR/target/debug/naust" server >>"$REG_LOG" 2>&1 &
  REG_PID=$!

  # wait until ready
  for _ in $(seq 1 200); do
    if ! kill -0 "$REG_PID" 2>/dev/null; then
      echo "registry process exited early" >&2
      tail -n 200 "$REG_LOG" >&2 || true
      exit 1
    fi
    code=$(curl -sS -o /dev/null -w '%{http_code}' "$BASE/v2/" 2>/dev/null || true)
    if [[ "$code" == "200" || "$code" == "401" ]]; then
      return 0
    fi
    sleep 0.1
  done

  echo "registry did not become ready" >&2
  tail -n 200 "$REG_LOG" >&2 || true
  exit 1
}

stop_registry() {
  echo "stopping registry (pid=$REG_PID)"
  kill "$REG_PID" 2>/dev/null || true
  wait "$REG_PID" 2>/dev/null || true
  unset REG_PID
}

hdr() {
  # Read a header value from a raw header block.
  # Usage: hdr "$headers" "Location"
  local headers="$1"
  local name="$2"
  echo "$headers" | awk -v n="$name" 'BEGIN{IGNORECASE=1} $1 ~ ("^"n":$") {sub(/\r$/, "", $2); print $2}'
}

hdr_line() {
  # Return full header line (minus CR) by header name.
  local headers="$1"
  local name="$2"
  echo "$headers" | awk -v n="$name" 'BEGIN{IGNORECASE=1} tolower($1)==tolower(n":") {sub(/\r$/, "", $0); print $0}'
}

# Create test data
CHUNK1="$TMP/chunk1.bin"
CHUNK2="$TMP/chunk2.bin"
BLOB="$TMP/blob.bin"

dd if=/dev/urandom of="$CHUNK1" bs=1k count=64 status=none
# second chunk intentionally different size
(dd if=/dev/urandom of="$CHUNK2" bs=1k count=32 status=none)
cat "$CHUNK1" "$CHUNK2" >"$BLOB"
TOTAL_SIZE=$(wc -c <"$BLOB" | tr -d ' ')
C1_SIZE=$(wc -c <"$CHUNK1" | tr -d ' ')
C2_SIZE=$(wc -c <"$CHUNK2" | tr -d ' ')
SHA=$(sha256sum "$BLOB" | awk '{print $1}')
DIGEST="sha256:$SHA"

echo "blob digest: $DIGEST"
echo "blob size:   $TOTAL_SIZE bytes (chunk1=$C1_SIZE, chunk2=$C2_SIZE)"

start_registry

# 1) Create upload session
CREATE_HEADERS=$(curl -sS -D - -o /dev/null -u "$USER:$PASS" -X POST "$BASE/v2/$REPO/blobs/uploads/")
LOCATION=$(hdr "$CREATE_HEADERS" "Location")
UUID=$(hdr "$CREATE_HEADERS" "Docker-Upload-UUID")

if [[ -z "$LOCATION" || -z "$UUID" ]]; then
  echo "failed to create upload session" >&2
  echo "$CREATE_HEADERS" >&2
  exit 1
fi

echo "upload uuid: $UUID"
echo "upload url:  $BASE$LOCATION"

# 2) PATCH first chunk
END1=$((C1_SIZE - 1))
PATCH1_HEADERS=$(curl -sS -D - -o /dev/null -u "$USER:$PASS" \
  -X PATCH "$BASE$LOCATION" \
  -H "Content-Type: application/octet-stream" \
  -H "Content-Range: 0-$END1" \
  --data-binary "@$CHUNK1")

echo "after chunk1: $(hdr_line "$PATCH1_HEADERS" "Range")"

# 3) Restart registry, then resume
stop_registry
start_registry

STATUS_HEADERS=$(curl -sS -D - -o /dev/null -u "$USER:$PASS" -X GET "$BASE$LOCATION")
RANGE=$(echo "$(hdr "$STATUS_HEADERS" "Range")" | tr -d '\r')

if [[ -z "$RANGE" ]]; then
  echo "failed to query upload status" >&2
  echo "$STATUS_HEADERS" >&2
  exit 1
fi

# RANGE format is "0-<last_byte>"
LAST_BYTE=$(echo "$RANGE" | awk -F- '{print $2}')
OFFSET=$((LAST_BYTE + 1))

if [[ "$OFFSET" -ne "$C1_SIZE" ]]; then
  echo "unexpected resume offset: got $OFFSET, expected $C1_SIZE" >&2
  exit 1
fi

echo "resuming at offset=$OFFSET"

# 4) PATCH second chunk from resumed offset
END2=$((OFFSET + C2_SIZE - 1))
PATCH2_HEADERS=$(curl -sS -D - -o /dev/null -u "$USER:$PASS" \
  -X PATCH "$BASE$LOCATION" \
  -H "Content-Type: application/octet-stream" \
  -H "Content-Range: $OFFSET-$END2" \
  --data-binary "@$CHUNK2")

echo "after chunk2: $(hdr_line "$PATCH2_HEADERS" "Range")"

# 5) Finalize
FINAL_HEADERS=$(curl -sS -D - -o /dev/null -u "$USER:$PASS" \
  -X PUT "$BASE$LOCATION?digest=$DIGEST")

echo "finalize: $(echo "$FINAL_HEADERS" | head -n 1 | tr -d '\r')"

# 6) Verify blob exists and size matches
HEAD_HEADERS=$(curl -sS -D - -o /dev/null -u "$USER:$PASS" -I "$BASE/v2/$REPO/blobs/$DIGEST")
LEN=$(hdr "$HEAD_HEADERS" "Content-Length")

if [[ "$LEN" != "$TOTAL_SIZE" ]]; then
  echo "unexpected blob length: got $LEN, expected $TOTAL_SIZE" >&2
  exit 1
fi

DOWNLOADED="$TMP/download.bin"
curl -fsS -u "$USER:$PASS" "$BASE/v2/$REPO/blobs/$DIGEST" -o "$DOWNLOADED"

if ! cmp -s "$BLOB" "$DOWNLOADED"; then
  echo "downloaded blob does not match uploaded content" >&2
  exit 1
fi

echo "OK: resumable upload succeeded (restart + resume + finalize)"
