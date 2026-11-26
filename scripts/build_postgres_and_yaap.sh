#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/build_postgres_and_yaap.sh [--prefix PATH] [--jobs N]

Run from an unpacked pg_yaap-offline bundle containing postgres/ and pg_yaap/.
Environment: PREFIX, JOBS, ENABLE_LLVM (default: 1), BUILD_ROOT.
EOF
}

bundle_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
prefix=${PREFIX:-"$HOME/pgsql-yaap"}
jobs=${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)}
enable_llvm=${ENABLE_LLVM:-1}
build_root=${BUILD_ROOT:-"$bundle_dir/build"}

while (($#)); do
  case "$1" in
    --prefix) (($# >= 2)) || { usage >&2; exit 2; }; prefix=$2; shift 2 ;;
    --jobs) (($# >= 2)) || { usage >&2; exit 2; }; jobs=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

for tool in meson ninja cc c++ grep; do
  command -v "$tool" >/dev/null || { echo "Required tool not found: $tool" >&2; exit 1; }
done
[[ -f "$bundle_dir/postgres/meson.build" ]] || { echo "Missing bundled PostgreSQL source at $bundle_dir/postgres" >&2; exit 1; }
[[ -f "$bundle_dir/pg_yaap/meson.build" ]] || { echo "Missing bundled pg_yaap source at $bundle_dir/pg_yaap" >&2; exit 1; }

pg_args=("--prefix=$prefix" -Dbuildtype=release -Dicu=enabled -Dreadline=enabled)
if [[ "$enable_llvm" == 1 ]]; then
  command -v llvm-config >/dev/null || { echo "ENABLE_LLVM=1 but llvm-config is missing; install LLVM dev files offline or set ENABLE_LLVM=0" >&2; exit 1; }
  llvm_config_bin=$(command -v llvm-config)
  pg_args+=(-Dllvm=enabled)
elif [[ "$enable_llvm" == 0 ]]; then
  pg_args+=(-Dllvm=disabled)
else
  echo "ENABLE_LLVM must be 0 or 1" >&2
  exit 2
fi

mkdir -p "$build_root"
pg_build="$build_root/postgres"
ext_build="$build_root/pg_yaap"

echo "Building PostgreSQL from $bundle_dir/postgres"
meson setup "$pg_build" "$bundle_dir/postgres" "${pg_args[@]}"
meson compile -C "$pg_build" -j "$jobs"
meson install -C "$pg_build"

export PATH="$prefix/bin:$PATH"
pg_config_bin="$prefix/bin/pg_config"
[[ -x "$pg_config_bin" ]] || { echo "PostgreSQL install did not provide $pg_config_bin" >&2; exit 1; }

echo "Building pg_yaap for $($pg_config_bin --version)"
ext_args=("-Dpg_config=$pg_config_bin" -Dbuildtype=release)
if [[ -n "${llvm_config_bin:-}" ]]; then
  ext_args+=("-Dllvm_config=$llvm_config_bin")
fi
meson setup "$ext_build" "$bundle_dir/pg_yaap" "${ext_args[@]}"
meson compile -C "$ext_build" -j "$jobs"
meson install -C "$ext_build"

echo "PostgreSQL and pg_yaap installed under $prefix"
echo "Initialize a data directory, then start PostgreSQL with:"
echo "  $prefix/bin/initdb -D /path/to/pgdata"
echo "  $prefix/bin/pg_ctl -D /path/to/pgdata -l /path/to/pg.log start"
echo "Test loading with: $prefix/bin/psql -d postgres -c \"LOAD 'pg_yaap'; SHOW pg_yaap.enabled;\""
