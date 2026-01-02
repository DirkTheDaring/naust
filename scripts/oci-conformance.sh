#!/usr/bin/env bash
set -euo pipefail

# Runs the OCI Distribution Spec conformance suite (black-box) against a live registry.
# By default, this script starts a local instance of this registry (fs backend) on ADDR,
# runs the upstream tests, writes reports, and tears down.
#
# Requirements:
# - git
# - go (1.17+; recommended 1.22+)
# - curl
#
# Notes:
# - The upstream suite tolerates DELETE being unimplemented (405) for non-management workflows.
# - This registry is configured to allow pushes to any repo during the run.

ADDR="${ADDR:-127.0.0.1:5000}"
OCI_ROOT_URL="${OCI_ROOT_URL:-http://${ADDR}}"
OCI_NAMESPACE="${OCI_NAMESPACE:-conformance/myrepo}"
OCI_CROSSMOUNT_NAMESPACE="${OCI_CROSSMOUNT_NAMESPACE:-conformance/other}"
OCI_USERNAME="${OCI_USERNAME:-${REGISTRY_USERNAME:-demo}}"
OCI_PASSWORD="${OCI_PASSWORD:-${REGISTRY_PASSWORD:-demo}}"

# Upstream conformance repo/tag/commit.
DISTRIBUTION_SPEC_REF="${DISTRIBUTION_SPEC_REF:-v1.1.0}"

# Output directory for junit.xml/report.html.
RESULTS_DIR="${RESULTS_DIR:-${PWD}/conformance-results}"

# If set to 1, do not start/stop the local registry (assumes OCI_ROOT_URL points to a live one).
SKIP_START_REGISTRY="${SKIP_START_REGISTRY:-0}"

log() { printf '%s\n' "$*"; }
need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required tool: $1" >&2
    exit 127
  fi
}

need git
need go
need curl

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

if [[ "${SKIP_START_REGISTRY}" != "1" ]]; then
  log "Building registry"
  cargo build

  registry_data_dir="$(mktemp -d)"
  registry_log="${workdir}/registry.log"

  log "Starting registry on ${ADDR} (fs backend)"
  env -u TLS_CERT_PATH -u TLS_KEY_PATH \
    REGISTRY_USERNAME="${OCI_USERNAME}" \
    REGISTRY_PASSWORD="${OCI_PASSWORD}" \
    REGISTRY_PUSH_ALLOW_REPOS='*' \
    LISTEN_ADDR="${ADDR}" \
    STORAGE_BACKEND=fs \
    STORAGE_FS_ROOT="${registry_data_dir}" \
    ALLOW_TAG_OVERWRITE=1 \
    PUBLIC_URL="${OCI_ROOT_URL}" \
    RUST_LOG=info \
    ./target/debug/registry-rust >"${registry_log}" 2>&1 &
  registry_pid="$!"

  log "Waiting for registry to respond"
  for _ in {1..60}; do
    if ! kill -0 "${registry_pid}" 2>/dev/null; then
      echo "registry process exited during startup" >&2
      if [[ -n "${registry_log}" && -f "${registry_log}" ]]; then
        echo "--- registry log ---" >&2
        tail -200 "${registry_log}" >&2 || true
      fi
      exit 1
    fi
    code=$(curl -s -o /dev/null -w '%{http_code}' "${OCI_ROOT_URL}/v2/" 2>/dev/null || true)
    if [[ "${code}" == "200" || "${code}" == "401" ]]; then
      break
    fi
    sleep 0.5
  done

  code=$(curl -s -o /dev/null -w '%{http_code}' "${OCI_ROOT_URL}/v2/" 2>/dev/null || true)
  if [[ "${code}" != "200" && "${code}" != "401" ]]; then
    echo "registry did not become ready (last code=${code:-<none>})" >&2
    if [[ -n "${registry_log}" && -f "${registry_log}" ]]; then
      echo "--- registry log ---" >&2
      tail -200 "${registry_log}" >&2 || true
    fi
    exit 1
  fi
fi

log "Fetching OCI distribution-spec conformance suite (${DISTRIBUTION_SPEC_REF})"
# Shallow clone when possible; falls back if ref is not a branch/tag.
if git clone --depth 1 --branch "${DISTRIBUTION_SPEC_REF}" https://github.com/opencontainers/distribution-spec "${workdir}/distribution-spec" >/dev/null 2>&1; then
  :
else
  git clone https://github.com/opencontainers/distribution-spec "${workdir}/distribution-spec" >/dev/null
  (cd "${workdir}/distribution-spec" && git checkout "${DISTRIBUTION_SPEC_REF}" >/dev/null)
fi

log "Building conformance binary"
(
  cd "${workdir}/distribution-spec/conformance"
  go test -c
)

mkdir -p "${RESULTS_DIR}"

log "Running conformance suite"
(
  cd "${RESULTS_DIR}"
  export OCI_ROOT_URL
  export OCI_NAMESPACE
  export OCI_CROSSMOUNT_NAMESPACE
  export OCI_USERNAME
  export OCI_PASSWORD

  # Default to pull+push; allow callers to enable additional workflows.
  : "${OCI_TEST_PULL:=1}"
  : "${OCI_TEST_PUSH:=1}"
  export OCI_TEST_PULL
  export OCI_TEST_PUSH

  if [[ -n "${OCI_TEST_CONTENT_DISCOVERY:-}" ]]; then
    export OCI_TEST_CONTENT_DISCOVERY
  fi
  if [[ -n "${OCI_TEST_CONTENT_MANAGEMENT:-}" ]]; then
    export OCI_TEST_CONTENT_MANAGEMENT
  fi

  export OCI_HIDE_SKIPPED_WORKFLOWS=1
  export OCI_DEBUG=0
  export OCI_DELETE_MANIFEST_BEFORE_BLOBS=1

  "${workdir}/distribution-spec/conformance/conformance.test"
)

log "OK (results in ${RESULTS_DIR})"
