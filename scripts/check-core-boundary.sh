#!/usr/bin/env bash
# Core-boundary gate (ADR-010, plan Phase 1a; repointed after the Phase 2 split).
#
# Since the crate split, the compiler is the primary boundary: naust-core
# cannot reference server modules at all. This script remains as a fast CI
# sanity check that (a) core never grows a dependency on the server crate or
# transport/auth libraries, and (b) core still compiles standalone.

set -u
cd "$(dirname "$0")/.."

violations=$(grep -rnE 'naust::|axum::|reqwest::|acmecert' ../naust-core/src 2>/dev/null \
  | grep -vE '^\s*//')

if [ -n "$violations" ]; then
  echo "core-boundary gate: FORBIDDEN references in naust-core:"
  echo "$violations"
  exit 1
fi

if ! cargo check -p naust-core --locked --quiet; then
  echo "core-boundary gate: naust-core does not compile standalone"
  exit 1
fi

echo "core-boundary gate: clean"
