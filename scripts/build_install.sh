#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/build_install.sh [--pg-config PATH] [--build-dir PATH]

Build pg_yaap with Meson in release mode and install it into the PostgreSQL
installation selected by pg_config. PG_CONFIG and BUILD_DIR may also be set
through the environment.
EOF
}

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)
pg_config_bin=${PG_CONFIG:-pg_config}
build_dir=${BUILD_DIR:-}

while (($#)); do
  case "$1" in
    --pg-config)
      (($# >= 2)) || { usage >&2; exit 2; }
      pg_config_bin=$2
      shift 2
      ;;
    --build-dir)
      (($# >= 2)) || { usage >&2; exit 2; }
      build_dir=$2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

command -v "$pg_config_bin" >/dev/null || {
  echo "Cannot find pg_config: $pg_config_bin (set PG_CONFIG or pass --pg-config)" >&2
  exit 1
}
command -v meson >/dev/null || { echo "meson is required" >&2; exit 1; }
command -v ninja >/dev/null || { echo "ninja is required" >&2; exit 1; }

pg_config_bin=$(command -v "$pg_config_bin")
pg_version=$($pg_config_bin --version)
pg_server_include=$($pg_config_bin --includedir-server)
pg_module_dir=$($pg_config_bin --pkglibdir)
pg_share_dir=$($pg_config_bin --sharedir)
pg_header="$pg_server_include/pg_config.h"

if [[ ! -f "$pg_header" ]]; then
  echo "Missing $pg_header. Install the server development headers matching $pg_version." >&2
  exit 1
fi

if grep -qE '^[[:space:]]*#define[[:space:]]+USE_LLVM[[:space:]]+1([[:space:]]|$)' "$pg_header"; then
  llvm_config=${LLVM_CONFIG:-}
  if [[ -z "$llvm_config" ]]; then
    for candidate in llvm-config llvm-config-19 llvm-config-18; do
      if command -v "$candidate" >/dev/null; then
        llvm_config=$candidate
        break
      fi
    done
  fi
  if [[ -z "$llvm_config" ]] || ! command -v "$llvm_config" >/dev/null; then
    echo "$pg_version was built with LLVM, but llvm-config was not found. Install the matching LLVM development package or set LLVM_CONFIG." >&2
    exit 1
  fi
  llvm_version=$($llvm_config --version)
  echo "PostgreSQL LLVM support: enabled (llvm-config $llvm_version)"
else
  llvm_config=
  echo "PostgreSQL LLVM support: disabled"
fi

if [[ -z "$build_dir" ]]; then
  major=$($pg_config_bin --version | sed -E 's/.* ([0-9]+)(\..*)?$/\1/')
  build_dir="$repo_root/build-release-pg$major"
fi
if [[ "$build_dir" != /* ]]; then
  build_dir="$repo_root/$build_dir"
fi

meson_args=("-Dpg_config=$pg_config_bin" "-Dbuildtype=release")
if [[ -n "$llvm_config" ]]; then
  meson_args+=("-Dllvm_config=$llvm_config")
fi

echo "Target PostgreSQL: $pg_version"
echo "pg_config: $pg_config_bin"
echo "Module directory: $pg_module_dir"
echo "Extension SQL directory: $pg_share_dir/extension"
echo "Build directory: $build_dir"

if [[ -f "$build_dir/meson-private/coredata.dat" ]]; then
  meson setup --reconfigure "$build_dir" "$repo_root" "${meson_args[@]}"
else
  meson setup "$build_dir" "$repo_root" "${meson_args[@]}"
fi
meson compile -C "$build_dir"

install_cmd=(meson install -C "$build_dir")
if [[ ! -w "$pg_module_dir" || ! -w "$pg_share_dir/extension" ]]; then
  command -v sudo >/dev/null || {
    echo "Install directories are not writable and sudo is unavailable. Run as their owner or root." >&2
    exit 1
  }
  sudo "${install_cmd[@]}"
else
  "${install_cmd[@]}"
fi

echo "Installed pg_yaap for $pg_version."
echo "In psql, verify with: LOAD 'pg_yaap'; SHOW pg_yaap.enabled;"
