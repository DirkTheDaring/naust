#!/usr/bin/env bash
set -euo pipefail

# Build the RPM inside a Fedora container of a chosen version.
#
# Examples:
#   packaging/docker/build-rpm-in-fedora.sh 40
#   packaging/docker/build-rpm-in-fedora.sh 41 -- make clean-rpm rpm
#
# If rootless Podman fails with errors like:
#   "cannot apply additional memory protection after relocation: Permission denied"
# your container storage may be on a `noexec` filesystem (often /home). Re-run with:
#   packaging/docker/build-rpm-in-fedora.sh --rootful 40
# which uses `sudo podman`.
#
# By default this runs: make clean-rpm rpm

rootful=0
if [[ "${1:-}" == "--rootful" ]]; then
  rootful=1
  shift
fi

fedora_version="${1:-}"
if [[ -z "${fedora_version}" ]]; then
  echo "usage: $0 [--rootful] <fedora_version> [-- <make args...>]" >&2
  exit 2
fi
shift || true

make_args=("clean-rpm" "rpm")
if [[ "${1:-}" == "--" ]]; then
  shift
  if [[ "$#" -gt 0 ]]; then
    make_args=("$@")
  fi
fi

engine="podman"
if ! command -v podman >/dev/null 2>&1; then
  engine="docker"
fi
if ! command -v "${engine}" >/dev/null 2>&1; then
  echo "error: neither podman nor docker found" >&2
  exit 1
fi

engine_cmd=("${engine}")
if [[ "${engine}" == "podman" && "${rootful}" -eq 1 ]]; then
  engine_cmd=(sudo podman)
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
image="registry-rust-rpmbuild:${fedora_version}"

# Optional local path dependency used by this workspace:
#   acmecert-core = { path = "../acmecert/crates/acmecert-core" }
# When building inside a container, we need to mount that sibling repository.
acmecert_host_dir="${repo_root}/../acmecert"
acmecert_mount=()
if [[ -f "${acmecert_host_dir}/crates/acmecert-core/Cargo.toml" ]]; then
  acmecert_mount_target="/acmecert"
  if [[ "${engine}" == "podman" ]]; then
    acmecert_mount=(-v "${acmecert_host_dir}:${acmecert_mount_target}:ro,Z")
  else
    acmecert_mount=(-v "${acmecert_host_dir}:${acmecert_mount_target}:ro")
  fi
fi

build_args=()
if [[ "${engine}" == "podman" ]]; then
  # Better Dockerfile feature parity (e.g. SHELL) than OCI format.
  build_args+=(--format docker)
fi

"${engine_cmd[@]}" build "${build_args[@]}" \
  -f "${repo_root}/packaging/docker/Dockerfile.fedora-rpmbuild" \
  --build-arg "FEDORA_VERSION=${fedora_version}" \
  -t "${image}" \
  "${repo_root}"

# Run as the calling user so artifacts in ./dist aren't owned by root.
uid="$(id -u)"
gid="$(id -g)"

repo_mount=("${repo_root}:/workspace")
if [[ "${engine}" == "podman" ]]; then
  repo_mount=("${repo_root}:/workspace:Z")
fi

"${engine_cmd[@]}" run --rm \
  --user "${uid}:${gid}" \
  -e HOME=/tmp \
  -e CARGO_HOME=/tmp/cargo \
  -v "${repo_mount[0]}" \
  "${acmecert_mount[@]}" \
  -w /workspace \
  "${image}" \
  make "${make_args[@]}"
