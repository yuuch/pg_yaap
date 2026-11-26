#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/make_offline_bundle.sh [--postgres-src PATH] [--output PATH]

Create a source-only tar.gz bundle. PostgreSQL source defaults to the parent
checkout when pg_yaap is at postgres/contrib/pg_yaap.
EOF
}

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
yaap_src=$(cd -- "$script_dir/.." && pwd)
pg_src=$(cd -- "$yaap_src/../.." && pwd)
output=${OUTPUT:-"$PWD/pg_yaap-offline.tar.gz"}

while (($#)); do
  case "$1" in
    --postgres-src) (($# >= 2)) || { usage >&2; exit 2; }; pg_src=$2; shift 2 ;;
    --output) (($# >= 2)) || { usage >&2; exit 2; }; output=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

pg_src=$(cd -- "$pg_src" && pwd)
[[ -f "$pg_src/meson.build" && -f "$pg_src/configure.ac" ]] || {
  echo "Not a PostgreSQL source tree: $pg_src" >&2
  exit 1
}
[[ -f "$yaap_src/meson.build" ]] || { echo "Not a pg_yaap source tree: $yaap_src" >&2; exit 1; }

output_dir=$(cd -- "$(dirname -- "$output")" && pwd)
output="$output_dir/$(basename -- "$output")"
stage=$(mktemp -d "${TMPDIR:-/tmp}/pg-yaap-bundle.XXXXXX")
trap 'rm -rf "$stage"' EXIT
mkdir -p "$stage/pg_yaap-offline/postgres" "$stage/pg_yaap-offline/pg_yaap"

# Copy source files only. Exclusions keep repository metadata, build products,
# benchmark artifacts, and generated data out of the offline archive.
copy_source_tree() {
  local source_dir=$1 destination_dir=$2
  (
    cd "$source_dir"
    find . \
      \( -type d \( -name .git -o -name .cache -o -name .omx \
        -o -name artifacts -o -name '*.trace' -o -name '.build*' -o -name 'build*' -o -name 'installed*' \
        -o -name bench-instance -o -name data -o -name pgdata \
        -o -name patches -o -path './contrib/pg_yaap' -o -path './contrib/pg_duckdb' \) -prune \) -o \
      \( -type f ! -name '*.trace' ! -name '._*' ! -name '.DS_Store' ! -name 'a.txt' \) -print
  ) | tar -C "$source_dir" -cf - -T - | tar -C "$destination_dir" -xf -
}

copy_source_tree "$pg_src" "$stage/pg_yaap-offline/postgres"
copy_source_tree "$yaap_src" "$stage/pg_yaap-offline/pg_yaap"

cat > "$stage/pg_yaap-offline/BUILD-OFFLINE.md" <<'EOF'
# Offline build bundle

This archive contains PostgreSQL source and pg_yaap source, without Git history
or build/data artifacts. On the target Linux machine, install the required
compiler, Meson/Ninja, PostgreSQL build dependencies, and (by default) LLVM
development tools from offline packages, then run:

    ./scripts/build_postgres_and_yaap.sh --prefix "$HOME/pgsql-yaap"

Set ENABLE_LLVM=0 if LLVM is intentionally unavailable. The extension will
then build without PostgreSQL LLVM/JIT support. See pg_yaap/docs/offline-linux-build.md
for dependency and runtime configuration details.
EOF

tar -C "$stage" -czf "$output" pg_yaap-offline
echo "Created source-only bundle: $output"
pg_version=$(sed -n 's/^AC_INIT(\[PostgreSQL\], \[\([^]]*\)\].*/\1/p' "$pg_src/configure.ac" | head -n 1)
echo "PostgreSQL source version: ${pg_version:-unknown}"
