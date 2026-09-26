#!/usr/bin/env bash
# Core-boundary allowlist gate (ADR-010, plan Phase 1a).
#
# Files belonging to the future `registry-core` crate may reference, via `crate::…`,
# ONLY modules that are themselves on the core list. The scan covers ALL code in
# those files, including `#[cfg(test)]` fixtures — test-only violations break the
# core crate's compilation after the split just as production ones do.
#
# Exit 0 = boundary clean; exit 1 = violations printed as file:line:match.

set -u
cd "$(dirname "$0")/.."

# Core module list (ADR-010 §2.1 / plan Phase 2 partition). Extend when new
# core-side modules are created (e.g. policy, upstream seam, upload_lifecycle).
CORE_MODULES=(
  registry
  storage
  upload_coordinator
  manifest_lifecycle
  manifest_refs
  repository_membership_ledger
  membership_migration
  blob_ref_index
  blob_delete_safety
  gc_service
  blob_gc
  consistency
  fs_root_lock
  application
  test_support
  policy
  upstream
  upload_lifecycle
)

# Files/dirs that make up the core side today (module roots and directories).
CORE_PATHS=(
  src/registry
  src/storage
  src/application
  src/blob_gc
  src/upload_coordinator.rs
  src/manifest_lifecycle.rs
  src/manifest_refs.rs
  src/repository_membership_ledger.rs
  src/membership_migration.rs
  src/blob_ref_index.rs
  src/blob_delete_safety.rs
  src/gc_service.rs
  src/consistency.rs
  src/fs_root_lock.rs
  src/test_support.rs
)
for extra in src/policy src/policy.rs src/upstream src/upstream.rs src/upload_lifecycle src/upload_lifecycle.rs; do
  [ -e "$extra" ] && CORE_PATHS+=("$extra")
done

allow_alt=$(IFS='|'; echo "${CORE_MODULES[*]}")
# #[macro_export] macros defined in core modules resolve as crate::<name>.
CORE_MACROS='impl_storage_ports|impl_gc_storage_port'

# Comment lines are skipped: doc prose may legitimately mention server modules.
violations=$(perl -ne '
  next if m{^\s*(//|\*)};
  while (/crate::([a-z_]+)/g) {
    print "$ARGV:$.:crate::$1\n" unless $1 =~ /^('"$allow_alt"'|'"$CORE_MACROS"')$/;
  }
  close ARGV if eof;
' $(find "${CORE_PATHS[@]}" -name '*.rs' 2>/dev/null) | sort -u)

if [ -n "$violations" ]; then
  echo "core-boundary gate: FORBIDDEN imports in core modules (allowlist: ${allow_alt}):"
  echo "$violations"
  exit 1
fi
echo "core-boundary gate: clean"
