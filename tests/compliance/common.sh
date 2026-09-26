#!/usr/bin/env bash
# Shared helpers for the OCI distribution-spec conformance runners.
# Sourced by run.sh and run-skipped.sh; not meant to be executed directly.
#
# Provides:
# - DISTRIBUTION_SPEC_REF / DISTRIBUTION_SPEC_COMMIT: the pinned upstream suite
# - ensure_harness: fetch + build the official conformance.test binary,
#   cached under tests/compliance/.cache so repeat runs are fast and offline
# - log / need helpers

COMPLIANCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC2034  # used by the sourcing scripts
REPO_ROOT="$(cd "${COMPLIANCE_DIR}/../.." && pwd)"

DISTRIBUTION_SPEC_REF="${DISTRIBUTION_SPEC_REF:-v1.1.1}"
DISTRIBUTION_SPEC_COMMIT="${DISTRIBUTION_SPEC_COMMIT:-a139cc423184af6078077b9b7ee336eddbd03f8f}"

CACHE_DIR="${COMPLIANCE_DIR}/.cache"
HARNESS_BIN="${CACHE_DIR}/conformance.test"

log() { printf '%s\n' "$*"; }

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required tool: $1" >&2
    exit 127
  fi
}

# Builds (or reuses) the official conformance harness at the pinned commit.
# Set CONFORMANCE_FORCE_REFRESH=1 to discard the cache and re-fetch.
ensure_harness() {
  if [[ "${CONFORMANCE_FORCE_REFRESH:-0}" != "1" && -x "${HARNESS_BIN}" && -f "${CACHE_DIR}/commit.txt" ]] \
    && [[ "$(cat "${CACHE_DIR}/commit.txt")" == "${DISTRIBUTION_SPEC_COMMIT}" ]]; then
    log "Using cached conformance harness (${DISTRIBUTION_SPEC_REF} @ ${DISTRIBUTION_SPEC_COMMIT})"
    return 0
  fi

  need git
  need go

  log "Fetching OCI distribution-spec conformance suite (${DISTRIBUTION_SPEC_REF} @ ${DISTRIBUTION_SPEC_COMMIT})"
  rm -rf "${CACHE_DIR}"
  mkdir -p "${CACHE_DIR}"
  git clone https://github.com/opencontainers/distribution-spec "${CACHE_DIR}/distribution-spec" >/dev/null 2>&1
  (
    cd "${CACHE_DIR}/distribution-spec" || exit 1
    git checkout "${DISTRIBUTION_SPEC_COMMIT}" >/dev/null 2>&1
  )

  log "Building official conformance test harness binary..."
  (
    cd "${CACHE_DIR}/distribution-spec/conformance" || exit 1
    go test -c
  )
  cp "${CACHE_DIR}/distribution-spec/conformance/conformance.test" "${HARNESS_BIN}"
  printf '%s\n' "${DISTRIBUTION_SPEC_COMMIT}" > "${CACHE_DIR}/commit.txt"
}
