# Testing Guide

`pg_yaap` includes Meson C++ unit tests, PostgreSQL `pg_regress` fixtures, and standalone Python integration suites under `tests/`.

## 1. C++ Unit Tests (Meson)

Defined in [`meson.build`](../meson.build):

```bash
meson test -C build --print-errorlogs
```

Included targets:
- `pg_yaap_execution_trace_unit` (`tests/execution_trace_unit.cpp`): shared-memory trace buffers, overflow accounting, abnormal exit handling, and JSON export.
- `pg_yaap_pipeline_task_queue` (`tests/pipeline_task_queue.cpp`): cross-process `DsmTaskQueue` correctness using PostgreSQL atomics in `mmap` memory.
- `pg_yaap_pipeline_error_protocol` (`tests/pipeline_error_protocol.cpp`): first-writer-wins cross-process error publication protocol (`worker_error.hpp`).
- `pg_yaap_pipeline_event_dag` (`tests/pipeline_event_dag.cpp`): `TaskScheduler` Event DAG validation (cycles, bounds, duplicate-edge coalescing, shared-sink guard, multi-process fan-in/fan-out, and abort safety).

## 2. Python Integration Suites

All core Python suites use only the Python 3 standard library and `psql` subprocesses against a running PostgreSQL instance.

### Environment Variables

| Variable | Default / Example | Description |
|---|---|---|
| `PSQL` | `/path/to/psql` | Path to the `psql` binary matching the target PostgreSQL install. |
| `YAAP_LIBRARY` | `/path/to/pg_yaap.so` (or `.dylib`) | Shared module path passed to `LOAD '...'`. |
| `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE` | `/tmp`, `55432`, `postgres`, `postgres` | Standard `libpq` connection parameters read by `psql`. |
| `YAAP_TEST_OUTPUT` | `artifacts/...` | Output directory for trace JSONs and regression artifacts. |
| `YAAP_TEST_MEMORY` | `64MB` | Overrides the tight shared-memory budget used by `tests/memory_runtime.py`. |

### Required Server Settings

Because several suites run `pg_yaap.parallel_max_workers = 12` (and `pipeline_scheduling.py --queue-pressure` requests 65 workers), start the PostgreSQL server with sufficient worker slots:

```bash
pg_ctl -D /path/to/data -l server.log -w start -o \
  "-p 55432 -k /tmp -c listen_addresses='' -c shared_buffers=256MB -c max_worker_processes=32 -c max_parallel_workers=12"
```

### Suites and Coverage

| Script | Coverage | Notes / Flags |
|---|---|---|
| [`tests/hybrid_hash_join.py`](../tests/hybrid_hash_join.py) | Resumable INNER/SEMI/ANTI/LEFT Hybrid Hash Join across 1/4/12 workers, all/partial/no-resident batches, compound & string keys, chained hybrids, cancel-reuse | Uses `YAAP_LIBRARY`, `PSQL`, `PG*`. |
| [`tests/right_outer_hash_join.py`](../tests/right_outer_hash_join.py) | RIGHT / swapped LEFT outer joins, NULL extensions, spilled build segments, cancel-reuse | Uses `YAAP_LIBRARY`, `PSQL`, `PG*`. |
| [`tests/memory_runtime.py`](../tests/memory_runtime.py) | Aggregate, join, order-strings, residual joins, and independent branches under tight `64MB` vs `8GB` budgets across 4/12 workers | `YAAP_TEST_MEMORY` overrides budget. |
| [`tests/pipeline_scheduling.py`](../tests/pipeline_scheduling.py) | Logical task identities and exactly-once completion across 1 and 4 workers | Pass `--queue-pressure` to exercise 65 workers against the 64-slot queue (`max_worker_processes >= 65` required). |
| [`tests/pipeline_stage_plans.py`](../tests/pipeline_stage_plans.py) | Operator stage plans (`COMBINE`/`FINALIZE`) for hash agg, perfect-hash agg, join, cross-product, and independent branches (`hash_join_partition_probe` on/off) | Compares results against PostgreSQL. |
| [`tests/execution_trace.py`](../tests/execution_trace.py) | Trace off/single/multi-worker, multi-pipeline deps, fan-in, 8 MB buffer, security/permission errors, concurrent sessions, overhead check | Pass `--abnormal` for query cancel & `pg_terminate_backend`; pass `--tpch` when `public.lineitem` exists. |
| [`tests/pipeline_stage3_4.py`](../tests/pipeline_stage3_4.py) | Concurrent `PhysicalTopN` & `CrossProduct`, Materialize spill/reload, `EXPLAIN`/`EXPLAIN ANALYZE`/cursor boundaries, worker shortage degradation, leak checks | Standalone Stage 3–4 checks. |
| [`tests/delim_reuse.py`](../tests/delim_reuse.py) | Correlated aggregate result equivalence vs PostgreSQL | Uses isolated schema. |
| [`tests/read_stream.py`](../tests/read_stream.py) | `ReadStream` heap scan with deleted tuples vs PostgreSQL | Uses isolated schema. |
| [`tests/test_ssd_read_bench.py`](../tests/test_ssd_read_bench.py) | CLI / correctness unit checks for standalone `ssd_read_bench` helper | Set `SSD_READ_BENCH` to binary path. |
| [`tests/tpch22_worker12.py`](../tests/tpch22_worker12.py) | Runs 22 TPC-H queries against a loaded TPC-H database | Requires external `QUERY_DIR` (`q1.sql`..`q22.sql`) and preloaded TPC-H tables (`PGDATABASE=tpch`). |

### Example Full Run

```bash
export PSQL=/path/to/psql
export YAAP_LIBRARY=/path/to/pg_yaap.so
export PGHOST=/tmp PGPORT=55432 PGUSER=postgres PGDATABASE=postgres

for t in \
  tests/hybrid_hash_join.py \
  tests/right_outer_hash_join.py \
  tests/memory_runtime.py \
  tests/pipeline_scheduling.py \
  tests/pipeline_stage_plans.py
do
  python3 "$t"
done
python3 tests/execution_trace.py --abnormal
```

## 3. GitHub Actions CI

The workflow at [`.github/workflows/pg_yaap-ci.yml`](../.github/workflows/pg_yaap-ci.yml) compiles PostgreSQL `REL_18_3` with `-Dllvm=enabled`, builds and installs `pg_yaap` in release mode, runs `meson test -C build --print-errorlogs`, initializes a temporary PostgreSQL cluster (`max_worker_processes=32`, `max_parallel_workers=12`), and executes the 6 core Python suites (`execution_trace.py --abnormal` without `--tpch`).
