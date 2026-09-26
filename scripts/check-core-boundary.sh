#!/usr/bin/env bash
# Core-boundary gate (ADR-010, plan Phase 1a; repointed after the Phase 2 split).
#
# Since the crate split, the compiler is the primary boundary: registry-core
# cannot reference server modules at all. This script remains as a fast CI
# sanity check that (a) core never grows a dependency on the server crate or
# transport/auth libraries, and (b) core still compiles standalone.

set -u
cd "$(dirname "$0")/.."

violations=$(grep -rnE 'registry_rust::|axum::|reqwest::|acmecert' crates/registry-core/src 2>/dev/null \
  | grep -vE '^\s*//')

if [ -n "$violations" ]; then
  echo "core-boundary gate: FORBIDDEN references in registry-core:"
  echo "$violations"
  exit 1
fi

if ! cargo check -p registry-core --locked --quiet; then
  echo "core-boundary gate: registry-core does not compile standalone"
  exit 1
fi

echo "core-boundary gate: clean"
