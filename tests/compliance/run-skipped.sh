#!/usr/bin/env bash
set -euo pipefail

# Runs the OCI distribution-spec conformance suite in modes that exercise the
# historically-skipped specs in junit.xml (env-only setup branches + cross-mount).
#
# Requirements:
# - git, go (1.20+), curl
#
# Notes:
# - This script runs multiple focused conformance invocations and writes results
#   under tests/compliance/results/skipped/...
# - Cross-mount "automatic content discovery" is a registry behavior; we toggle it
#   via REGISTRY_AUTOMATIC_CROSSMOUNT=1/0.
# - The upstream suite is cached under tests/compliance/.cache (pinned commit).

# shellcheck source=tests/compliance/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

ADDR="${ADDR:-127.0.0.1:5000}"
OCI_ROOT_URL="${OCI_ROOT_URL:-http://${ADDR}}"
OCI_NAMESPACE="${OCI_NAMESPACE:-conformance/myrepo}"
OCI_CROSSMOUNT_NAMESPACE="${OCI_CROSSMOUNT_NAMESPACE:-conformance/other}"
OCI_USERNAME="${OCI_USERNAME:-${REGISTRY_USERNAME:-demo}}"
OCI_PASSWORD="${OCI_PASSWORD:-${REGISTRY_PASSWORD:-demo}}"

RESULTS_BASE="${RESULTS_BASE:-${COMPLIANCE_DIR}/results/skipped}"

need git
need go
need curl

cd "${REPO_ROOT}"

workdir="$(mktemp -d)"
registry_data_dir=""
registry_pid=""
registry_log=""

cleanup() {
  if [[ -n "${registry_pid}" ]]; then
    kill "${registry_pid}" 2>/dev/null || true
    wait "${registry_pid}" 2>/dev/null || true
  fi
  if [[ -n "${registry_data_dir}" ]]; then
    rm -rf "${registry_data_dir}" || true
  fi
  rm -rf "${workdir}" || true
}
trap cleanup EXIT

start_registry() {
  local auto_crossmount="$1" # 1 or 0
  registry_data_dir="$(mktemp -d)"
  registry_log="${workdir}/registry-${auto_crossmount}.log"

  log "Starting registry on ${ADDR} (REGISTRY_AUTOMATIC_CROSSMOUNT=${auto_crossmount})"
  env -u TLS_CERT_PATH -u TLS_KEY_PATH \
    REGISTRY_USERNAME="${OCI_USERNAME}" \
    REGISTRY_PASSWORD="${OCI_PASSWORD}" \
    REGISTRY_PUSH_ALLOW_REPOS='*' \
    REGISTRY_AUTOMATIC_CROSSMOUNT="${auto_crossmount}" \
    LISTEN_ADDR="${ADDR}" \
    STORAGE_BACKEND=fs \
    STORAGE_FS_ROOT="${registry_data_dir}" \
    ALLOW_TAG_OVERWRITE=1 \
    PUBLIC_URL="${OCI_ROOT_URL}" \
    RUST_LOG=info \
    ./target/debug/registry-rust server >"${registry_log}" 2>&1 &
  registry_pid="$!"

  log "Waiting for registry to respond"
  for _ in {1..60}; do
    if ! kill -0 "${registry_pid}" 2>/dev/null; then
      echo "registry process exited during startup" >&2
      echo "--- registry log ---" >&2
      tail -200 "${registry_log}" >&2 || true
      exit 1
    fi
    code=$(curl -s -o /dev/null -w '%{http_code}' "${OCI_ROOT_URL}/v2/" 2>/dev/null || true)
    if [[ "${code}" == "200" || "${code}" == "401" ]]; then
      return 0
    fi
    sleep 0.5
  done

  echo "registry did not become ready" >&2
  echo "--- registry log ---" >&2
  tail -200 "${registry_log}" >&2 || true
  exit 1
}

stop_registry() {
  if [[ -n "${registry_pid}" ]]; then
    kill "${registry_pid}" 2>/dev/null || true
    wait "${registry_pid}" 2>/dev/null || true
    registry_pid=""
  fi
  if [[ -n "${registry_data_dir}" ]]; then
    rm -rf "${registry_data_dir}" || true
    registry_data_dir=""
  fi
}

log "Building registry"
cargo build

ensure_harness

mkdir -p "${RESULTS_BASE}"

run_conformance() {
  local outdir="$1"
  shift

  mkdir -p "${outdir}"
  (
    cd "${outdir}"

    export OCI_ROOT_URL
    export OCI_NAMESPACE
    export OCI_CROSSMOUNT_NAMESPACE
    export OCI_USERNAME
    export OCI_PASSWORD

    # Hide disabled workflows; we focus on a small set per run.
    export OCI_HIDE_SKIPPED_WORKFLOWS=1
    export OCI_DEBUG=0
    export OCI_DELETE_MANIFEST_BEFORE_BLOBS=1

    "${HARNESS_BIN}" "$@"
  )
}

# 1) Pull: execute the env-only tag-name branch.
# This spec only runs when OCI_TAG_NAME + OCI_MANIFEST_DIGEST + OCI_BLOB_DIGEST are set.
stop_registry
start_registry 0
log "Running: Pull env-only tag name"
(
  export OCI_TEST_PULL=1
  export OCI_TEST_PUSH=0
  export OCI_TEST_CONTENT_DISCOVERY=0
  export OCI_TEST_CONTENT_MANAGEMENT=0

  export OCI_TAG_NAME="envtag"
  export OCI_MANIFEST_DIGEST="sha256:0000000000000000000000000000000000000000000000000000000000000000"
  export OCI_BLOB_DIGEST="sha256:0000000000000000000000000000000000000000000000000000000000000000"

  run_conformance "${RESULTS_BASE}/pull-env" \
    --ginkgo.focus "Get tag name from environment"
)

# 2) Content Discovery: execute the env-only tag list branch.
stop_registry
start_registry 0
log "Running: Content Discovery env-only tag list"
(
  export OCI_TEST_PULL=0
  export OCI_TEST_PUSH=0
  export OCI_TEST_CONTENT_DISCOVERY=1
  export OCI_TEST_CONTENT_MANAGEMENT=0

  export OCI_TAG_LIST="test0,test1,test2"

  run_conformance "${RESULTS_BASE}/discovery-env" \
    --ginkgo.focus "Populate registry with test tags \(no push\)"
)

# 3) Content Management: ensure blob-delete GET-after-delete runs (requires DELETE not 405).
stop_registry
start_registry 0
log "Running: Content Management blob delete (DELETE + GET-after-delete)"
(
  export OCI_TEST_PULL=0
  export OCI_TEST_PUSH=0
  export OCI_TEST_CONTENT_DISCOVERY=0
  export OCI_TEST_CONTENT_MANAGEMENT=1

  run_conformance "${RESULTS_BASE}/management-blob-delete" \
    --ginkgo.focus "Blob delete"
)

# 4) Push: run cross-mount tests twice (registry auto-crossmount enabled vs disabled).
# The conformance suite gates the two mutually-exclusive specs by OCI_AUTOMATIC_CROSSMOUNT.

stop_registry
start_registry 1
log "Running: Push cross-mount (automatic enabled)"
(
  export OCI_TEST_PULL=0
  export OCI_TEST_PUSH=1
  export OCI_TEST_CONTENT_DISCOVERY=0
  export OCI_TEST_CONTENT_MANAGEMENT=0

  export OCI_AUTOMATIC_CROSSMOUNT=true

  run_conformance "${RESULTS_BASE}/push-crossmount-enabled" \
    --ginkgo.focus "OCI Distribution Conformance Tests Push"
)

stop_registry
start_registry 0
log "Running: Push cross-mount (automatic disabled)"
(
  export OCI_TEST_PULL=0
  export OCI_TEST_PUSH=1
  export OCI_TEST_CONTENT_DISCOVERY=0
  export OCI_TEST_CONTENT_MANAGEMENT=0

  export OCI_AUTOMATIC_CROSSMOUNT=false

  run_conformance "${RESULTS_BASE}/push-crossmount-disabled" \
    --ginkgo.focus "OCI Distribution Conformance Tests Push"
)

log "OK (results in ${RESULTS_BASE})"
