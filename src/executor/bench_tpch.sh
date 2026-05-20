#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

PSQL="${PSQL:-$ROOT/installed/bin/psql}"
QUERY_DIR="${QUERY_DIR:-$ROOT/contrib/pg_carbon/tests/tpch}"
DB_NAME="${DB_NAME:-tpch}"
HOST="${HOST:-/tmp}"
PORT="${PORT:-5432}"
RUNS="${RUNS:-1}"
WORKERS="${WORKERS:-12}"
TIMEOUT="${TIMEOUT:-60s}"
TRACE_HOOKS="${TRACE_HOOKS:-off}"
TRACE_EXECUTION_PATH="${TRACE_EXECUTION_PATH:-off}"

usage() {
	printf 'usage: %s <query_num> [runs]\n' "$0" >&2
	printf 'env: WORKERS=<n> TIMEOUT=<60s> TRACE_HOOKS=<on|off> TRACE_EXECUTION_PATH=<on|off>\n' >&2
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
	usage
	exit 1
fi

QNUM="${1#q}"
if [[ ! "$QNUM" =~ ^[0-9]+$ ]]; then
	printf 'invalid query number: %s\n' "$1" >&2
	exit 1
fi

if [[ $# -eq 2 ]]; then
	RUNS="$2"
fi

if [[ ! "$RUNS" =~ ^[0-9]+$ || "$RUNS" -le 0 ]]; then
	printf 'invalid runs: %s\n' "$RUNS" >&2
	exit 1
fi

if [[ ! "$WORKERS" =~ ^[0-9]+$ || "$WORKERS" -le 0 ]]; then
	printf 'invalid workers: %s\n' "$WORKERS" >&2
	exit 1
fi

if [[ "$TRACE_HOOKS" != "on" && "$TRACE_HOOKS" != "off" ]]; then
	printf 'invalid TRACE_HOOKS: %s\n' "$TRACE_HOOKS" >&2
	exit 1
fi

if [[ "$TRACE_EXECUTION_PATH" != "on" && "$TRACE_EXECUTION_PATH" != "off" ]]; then
	printf 'invalid TRACE_EXECUTION_PATH: %s\n' "$TRACE_EXECUTION_PATH" >&2
	exit 1
fi

SQL_FILE="$QUERY_DIR/q${QNUM}.sql"
if [[ ! -f "$SQL_FILE" ]]; then
	printf 'SQL file not found: %s\n' "$SQL_FILE" >&2
	exit 1
fi

if [[ ! -x "$PSQL" ]]; then
	printf 'psql not found or not executable: %s\n' "$PSQL" >&2
	exit 1
fi

TMP_SQL="$(mktemp -t pg_yaap_bench_tpch.XXXXXX.sql)"
trap 'rm -f "$TMP_SQL"' EXIT

{
	printf "LOAD 'pg_yaap';\n"
	printf 'SET pg_yaap.enabled = on;\n'
	printf 'SET pg_yaap.parallel = on;\n'
	printf 'SET pg_yaap.parallel_max_workers = %s;\n' "$WORKERS"
	printf "SET statement_timeout = '%s';\n" "$TIMEOUT"
	printf 'SET pg_yaap.trace_hooks = %s;\n' "$TRACE_HOOKS"
	printf 'SET pg_yaap.trace_execution_path = %s;\n' "$TRACE_EXECUTION_PATH"
	printf '%s\n' '\timing on'
	for ((i = 1; i <= RUNS; ++i)); do
		printf '%s %d\n' '\echo __PG_YAAP_BENCH_RUN_BEGIN__' "$i"
		cat "$SQL_FILE"
		printf '\n'
		printf '%s %d\n' '\echo __PG_YAAP_BENCH_RUN_END__' "$i"
	done
} > "$TMP_SQL"

printf 'benchmarking q%s (%s), runs=%s, workers=%s, timeout=%s, trace_hooks=%s, trace_execution_path=%s\n' \
	"$QNUM" "$SQL_FILE" "$RUNS" "$WORKERS" "$TIMEOUT" "$TRACE_HOOKS" "$TRACE_EXECUTION_PATH"
"$PSQL" -X -P pager=off -h "$HOST" -p "$PORT" -d "$DB_NAME" -v ON_ERROR_STOP=1 -f "$TMP_SQL" 2>&1 |
	awk '
		/^__PG_YAAP_BENCH_RUN_BEGIN__ [0-9]+$/ {
			run = $2
			in_run = 1
			run_sum = 0
			next
		}
		/^Time:/ {
			if (!in_run)
				next
			run_sum += $2
			next
		}
		/^__PG_YAAP_BENCH_RUN_END__ [0-9]+$/ {
			if (!in_run || $2 != run)
				next
			++n
			printf("run%d: %.3f ms\n", run, run_sum)
			sum += run_sum
			if (min == 0 || run_sum < min) min = run_sum
			if (run_sum > max) max = run_sum
			in_run = 0
			run = 0
			run_sum = 0
			next
		}
		END {
			if (n == 0) {
				exit 1
			}
			printf("avg: %.3f ms\n", sum / n)
			printf("min: %.3f ms\n", min)
			printf("max: %.3f ms\n", max)
		}
	'
