#!/usr/bin/env bash
set -euo pipefail

# Runs the official OCI Distribution Spec conformance suite (v1.1.1, commit a139cc423184af6078077b9b7ee336eddbd03f8f)
# against Filesystem, S3 (MinIO), Basic Authentication, and Bearer Token Authentication.
#
# Requirements:
# - git
# - go (1.20+)
# - curl
# - python3 (for YAML report generation)

DISTRIBUTION_SPEC_REF="${DISTRIBUTION_SPEC_REF:-v1.1.1}"
DISTRIBUTION_SPEC_COMMIT="a139cc423184af6078077b9b7ee336eddbd03f8f"

RESULTS_BASE="${RESULTS_BASE:-${PWD}/conformance-results}"
WORK_DIR="$(mktemp -d)"

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
need python3

cleanup() {
  rm -rf "${WORK_DIR}" || true
}
trap cleanup EXIT

log "Fetching OCI distribution-spec conformance suite (commit ${DISTRIBUTION_SPEC_COMMIT})"
git clone https://github.com/opencontainers/distribution-spec "${WORK_DIR}/distribution-spec" >/dev/null 2>&1
(
  cd "${WORK_DIR}/distribution-spec"
  git checkout "${DISTRIBUTION_SPEC_COMMIT}" >/dev/null 2>&1
)

log "Building official conformance test harness binary..."
(
  cd "${WORK_DIR}/distribution-spec/conformance"
  go test -c
)
HARNESS_BIN="${WORK_DIR}/distribution-spec/conformance/conformance.test"

log "Building registry binary..."
cargo build

run_matrix() {
  local matrix_name="$1"
  local backend="$2"
  local auth_strategy="$3"
  local port="$4"
  local results_dir="${RESULTS_BASE}/${matrix_name}"
  local data_dir=""
  local prefix=""
  local s3_endpoint="${TEST_S3_ENDPOINT:-http://127.0.0.1:9000}"
  local s3_bucket="${TEST_S3_BUCKET:-registry-live-test}"
  local reg_pid=""
  local reg_log="${results_dir}/registry.log"

  mkdir -p "${results_dir}"

  log "=== Starting registry for matrix '${matrix_name}' (backend=${backend}, auth=${auth_strategy}) on port ${port} ==="
  if [[ "${backend}" == "fs" ]]; then
    data_dir="$(mktemp -d)"
    env -u TLS_CERT_PATH -u TLS_KEY_PATH \
      REGISTRY_USERNAME=demo \
      REGISTRY_PASSWORD=demo \
      REGISTRY_PUSH_ALLOW_REPOS='*' \
      REGISTRY_PUSH_ACTIONS='pull,push,delete' \
      REGISTRY_AUTH_STRATEGY="${auth_strategy}" \
      LISTEN_ADDR="127.0.0.1:${port}" \
      PUBLIC_URL="http://127.0.0.1:${port}" \
      STORAGE_BACKEND=fs \
      STORAGE_FS_ROOT="${data_dir}" \
      ALLOW_TAG_OVERWRITE=1 \
      RUST_LOG=debug \
      ./target/debug/registry-rust server > "${reg_log}" 2>&1 &
    reg_pid=$!
  else
    prefix="conformance-${matrix_name}-s3-$(date +%s)"
    env -u TLS_CERT_PATH -u TLS_KEY_PATH \
      REGISTRY_USERNAME=demo \
      REGISTRY_PASSWORD=demo \
      REGISTRY_PUSH_ALLOW_REPOS='*' \
      REGISTRY_PUSH_ACTIONS='pull,push,delete' \
      REGISTRY_AUTH_STRATEGY="${auth_strategy}" \
      LISTEN_ADDR="127.0.0.1:${port}" \
      PUBLIC_URL="http://127.0.0.1:${port}" \
      STORAGE_BACKEND=s3 \
      STORAGE_S3_ENDPOINT="${s3_endpoint}" \
      STORAGE_S3_BUCKET="${s3_bucket}" \
      STORAGE_S3_PREFIX="${prefix}" \
      STORAGE_S3_REGION="${TEST_S3_REGION:-us-east-1}" \
      AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}" \
      AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}" \
      ALLOW_TAG_OVERWRITE=1 \
      RUST_LOG=debug \
      ./target/debug/registry-rust server > "${reg_log}" 2>&1 &
    reg_pid=$!
  fi

  log "Waiting for registry to respond on port ${port}..."
  for _ in {1..60}; do
    if ! kill -0 "${reg_pid}" 2>/dev/null; then
      echo "Registry exited prematurely for matrix ${matrix_name}" >&2
      tail -n 100 "${reg_log}" >&2
      exit 1
    fi
    code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${port}/v2/" || true)
    if [[ "$code" == "200" || "$code" == "401" ]]; then
      break
    fi
    sleep 0.5
  done

  log "Running official OCI Distribution conformance suite against matrix '${matrix_name}'..."
  set +e
  (
    cd "${results_dir}"
    export OCI_ROOT_URL="http://127.0.0.1:${port}"
    export OCI_NAMESPACE="conformance/${matrix_name}repo"
    export OCI_CROSSMOUNT_NAMESPACE="conformance/${matrix_name}cross"
    export OCI_USERNAME="demo"
    export OCI_PASSWORD="demo"
    export OCI_VERSION="1.1"
    export OCI_TEST_PULL=1
    export OCI_TEST_PUSH=1
    export OCI_TEST_CONTENT_DISCOVERY=1
    export OCI_TEST_CONTENT_MANAGEMENT=1
    export OCI_DELETE_MANIFEST_BEFORE_BLOBS=1
    export OCI_HIDE_SKIPPED_WORKFLOWS=0
    export OCI_DEBUG=0
    export OCI_REPORT_DIR="${results_dir}"

    "${HARNESS_BIN}" --ginkgo.v
  )
  local exit_code=$?
  set -e

  kill -SIGTERM "${reg_pid}" 2>/dev/null || true
  wait "${reg_pid}" 2>/dev/null || true
  if [[ -n "${data_dir}" ]]; then
    rm -rf "${data_dir}" || true
  fi

  echo "${exit_code}" > "${results_dir}/exit_code.txt"

  # Convert JUnit to result.yaml
  python3 - <<PY
import xml.etree.ElementTree as ET
import yaml

tree = ET.parse("${results_dir}/junit.xml")
root = tree.getroot()
suites = root.findall("testsuite") if root.tag == "testsuites" else [root]
total = int(root.attrib.get("tests", 0))
failures = int(root.attrib.get("failures", 0))
errors = int(root.attrib.get("errors", 0))
skipped = int(root.attrib.get("skipped", 0))
time_val = float(root.attrib.get("time", 0.0))

tests = []
for s in suites:
    for tc in s.findall("testcase"):
        f = tc.find("failure")
        e = tc.find("error")
        sk = tc.find("skipped")
        st = "failed" if f is not None else ("error" if e is not None else ("skipped" if sk is not None else "passed"))
        tests.append({
            "name": tc.attrib.get("name", ""),
            "classname": tc.attrib.get("classname", ""),
            "status": st,
            "time": float(tc.attrib.get("time", 0.0)),
            "message": (f or e or sk).attrib.get("message", "") if (f or e or sk) is not None else "",
            "details": ((f or e or sk).text or "").strip() if (f or e or sk) is not None else ""
        })

data = {
    "spec_version": "v1.1.1",
    "harness_commit": "${DISTRIBUTION_SPEC_COMMIT}",
    "matrix": "${matrix_name}",
    "backend": "${backend}",
    "auth_strategy": "${auth_strategy}",
    "total": total,
    "passed": total - (failures + errors + skipped),
    "failed": failures + errors,
    "skipped": skipped,
    "time_seconds": time_val,
    "tests": tests
}

with open("${results_dir}/result.yaml", "w") as out:
    yaml.dump(data, out, sort_keys=False)
PY

  log "Matrix '${matrix_name}' conformance finished with exit code ${exit_code}"
  if [[ "${exit_code}" -ne 0 ]]; then
    echo "Matrix '${matrix_name}' FAILED!" >&2
    return "${exit_code}"
  fi
}

mkdir -p "${RESULTS_BASE}"

log "Starting Matrix 1: Filesystem (unauthenticated / both)"
run_matrix "fs" "fs" "both" 5081

log "Starting Matrix 2: S3 MinIO (both)"
run_matrix "s3" "s3" "both" 5082

log "Starting Matrix 3: Basic Authentication (basic)"
run_matrix "basic" "fs" "basic" 5083

log "Starting Matrix 4: Bearer Token Authentication (token)"
run_matrix "token" "fs" "token" 5084

log "All 4 conformance matrices completed successfully with 0 failures!"
