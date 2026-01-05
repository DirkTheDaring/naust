#!/usr/bin/env bash
set -euo pipefail

# Build the .deb inside a Debian container of a chosen suite.
#
# Examples:
#   packaging/docker/build-deb-in-debian.sh trixie
#   packaging/docker/build-deb-in-debian.sh bookworm -- deb
#
# If rootless Podman fails with mmap/noexec errors, rerun with:
#   packaging/docker/build-deb-in-debian.sh --rootful trixie

rootful=0
if [[ "${1:-}" == "--rootful" ]]; then
  rootful=1
  shift
fi

debian_suite="${1:-}"
if [[ -z "${debian_suite}" ]]; then
  echo "usage: $0 [--rootful] <debian_suite> [-- <make args...>]" >&2
  exit 2
fi
shift || true

make_args=("clean-deb" "deb")
if [[ "${1:-}" == "--" ]]; then
  shift
  if [[ "$#" -gt 0 ]]; then
    make_args=("$@");
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
image="registry-rust-debbuild:${debian_suite}"

build_args=()
if [[ "${engine}" == "podman" ]]; then
  build_args+=(--format docker)
fi

"${engine_cmd[@]}" build "${build_args[@]}" \
  -f "${repo_root}/packaging/docker/Dockerfile.debian-debbuild" \
  --build-arg "DEBIAN_SUITE=${debian_suite}" \
  -t "${image}" \
  "${repo_root}"

uid="$(id -u)"
gid="$(id -g)"

repo_mount=("${repo_root}:/workspace")
if [[ "${engine}" == "podman" ]]; then
  repo_mount=("${repo_root}:/workspace:Z")
fi

# Mount optional sibling path dependency (acmecert) if present.
acmecert_host_dir="${repo_root}/../acmecert"
acmecert_mount=()
if [[ -f "${acmecert_host_dir}/crates/acmecert-core/Cargo.toml" ]]; then
  if [[ "${engine}" == "podman" ]]; then
    acmecert_mount=(-v "${acmecert_host_dir}:/acmecert:ro,Z")
  else
    acmecert_mount=(-v "${acmecert_host_dir}:/acmecert:ro")
  fi
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
