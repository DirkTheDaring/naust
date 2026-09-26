#!/usr/bin/env bash
set -euo pipefail

# Runs the official OCI Distribution Spec conformance suite (v1.1.1) against
# the current registry implementation.
#
# Matrices:
#   fs     - Filesystem backend, auth strategy "both"   (port 5081)
#   s3     - S3/MinIO backend,  auth strategy "both"    (port 5082)
#   basic  - Filesystem backend, Basic authentication   (port 5083)
#   token  - Filesystem backend, Bearer token auth      (port 5084)
#
# Usage:
#   tests/compliance/run.sh              # all matrices (s3 skipped if MinIO is unreachable)
#   tests/compliance/run.sh fs token     # selected matrices only (explicit s3 hard-fails without MinIO)
#
# Requirements:
# - git, go (1.20+), curl, python3 (+ pyyaml, for the derived YAML report)
# - for the s3 matrix: MinIO at TEST_S3_ENDPOINT (default http://127.0.0.1:9000),
#   e.g. via scripts/start-minio.sh
#
# The upstream suite is cached under tests/compliance/.cache (pinned commit);
# results are written under tests/compliance/results/<matrix>/.

# shellcheck source=tests/compliance/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

RESULTS_BASE="${RESULTS_BASE:-${COMPLIANCE_DIR}/results}"

need git
need go
need curl
need python3

S3_ENDPOINT="${TEST_S3_ENDPOINT:-http://127.0.0.1:9000}"
S3_BUCKET="${TEST_S3_BUCKET:-registry-live-test}"

ALL_MATRICES=(fs s3 basic token)
EXPLICIT_SELECTION=0
if [[ $# -gt 0 ]]; then
  EXPLICIT_SELECTION=1
  MATRICES=("$@")
  for m in "${MATRICES[@]}"; do
    case "${m}" in
      fs|s3|basic|token) ;;
      *)
        echo "Unknown matrix '${m}' (expected: fs, s3, basic, token)" >&2
        exit 2
        ;;
    esac
  done
else
  MATRICES=("${ALL_MATRICES[@]}")
fi

cd "${REPO_ROOT}"

ensure_harness

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
      STORAGE_S3_ENDPOINT="${S3_ENDPOINT}" \
      STORAGE_S3_BUCKET="${S3_BUCKET}" \
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
    export OCI_AUTOMATIC_CROSSMOUNT=0
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

  # Convert JUnit to derived summary result.yaml (clearly labeled as derived)
  python3 - <<PY
import xml.etree.ElementTree as ET
import yaml

tree = ET.parse("${results_dir}/junit.xml")
root = tree.getroot()
testcases = root.findall(".//testcase")
failures = len(root.findall(".//testcase[failure]"))
errors = len(root.findall(".//testcase[error]"))
skipped = len(root.findall(".//testcase[skipped]"))
total = len(testcases)
passed = total - (failures + errors + skipped)
time_val = float(root.attrib.get("time", 0.0))

tests = []
for tc in testcases:
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
    "spec_version": "${DISTRIBUTION_SPEC_REF}",
    "harness_commit": "${DISTRIBUTION_SPEC_COMMIT}",
    "artifact_nature": "derived_summary_from_official_junit_xml",
    "matrix": "${matrix_name}",
    "backend": "${backend}",
    "auth_strategy": "${auth_strategy}",
    "total": total,
    "passed": passed,
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

s3_reachable() {
  curl -s -o /dev/null --connect-timeout 2 "${S3_ENDPOINT}"
}

mkdir -p "${RESULTS_BASE}"

RAN=()
SKIPPED_S3=0
for m in "${MATRICES[@]}"; do
  case "${m}" in
    fs)
      log "Starting matrix: Filesystem (unauthenticated / both)"
      run_matrix "fs" "fs" "both" 5081
      ;;
    s3)
      if ! s3_reachable; then
        if [[ "${EXPLICIT_SELECTION}" == "1" ]]; then
          echo "Matrix 's3' requested but no S3 endpoint reachable at ${S3_ENDPOINT}." >&2
          echo "Start MinIO first, e.g.: scripts/start-minio.sh" >&2
          exit 1
        fi
        log "WARNING: skipping matrix 's3' - no S3 endpoint reachable at ${S3_ENDPOINT} (start MinIO via scripts/start-minio.sh to include it)"
        SKIPPED_S3=1
        continue
      fi
      log "Starting matrix: S3 MinIO (both)"
      run_matrix "s3" "s3" "both" 5082
      ;;
    basic)
      log "Starting matrix: Basic Authentication (basic)"
      run_matrix "basic" "fs" "basic" 5083
      ;;
    token)
      log "Starting matrix: Bearer Token Authentication (token)"
      run_matrix "token" "fs" "token" 5084
      ;;
  esac
  RAN+=("${m}")
done

if [[ "${SKIPPED_S3}" == "1" ]]; then
  log "Conformance matrices completed with 0 failures: ${RAN[*]} (s3 SKIPPED - MinIO unreachable)"
else
  log "All conformance matrices completed successfully with 0 failures: ${RAN[*]}"
fi
