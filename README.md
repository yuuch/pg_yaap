<p align="center">
  <img src="imgs/logo.png" alt="pg_yaap logo" width="200">
</p>

<h1 align="center">pg_yaap</h1>

<p align="center"><strong>A modern analytical engine for PostgreSQL</strong></p>
<p align="center">Modern OLAP. Native PostgreSQL storage.</p>
<p align="center">PostgreSQL extension · C++17 · Apache 2.0 · Experimental</p>

<p align="center">
  English · <a href="README.zh.md">中文</a> ·
  <a href="#benchmarks">Benchmarks</a> · <a href="#architecture">Architecture</a> ·
  <a href="#quick-start">Quick Start</a>
</p>

## Overview

pg_yaap gives supported analytical queries a dedicated optimizer and a vectorized C++ execution engine inside PostgreSQL. It reads native heap tables through PostgreSQL's storage and buffer management APIs.

PostgreSQL handles SQL parsing, catalogs, transactions, and client protocol. YAAP plans admitted queries and runs them through parallel pipelines. The project is experimental and intended for development and evaluation.

## Benchmarks

On an **Apple M4 Pro with 24 GB RAM**, YAAP recorded **20.5 s at SF10** and **414.5 s at SF100** in total SQL time. At SF100, it completed all 22 queries against a PostgreSQL database occupying about **125 GB on disk**.

| TPC-H scale | PostgreSQL | pg_duckdb | pg_yaap |
| --- | ---: | ---: | ---: |
| SF10 | 447.9 s | 95.8 s | **20.5 s** |
| SF100 | 3,302.0 s | 3,346.6 s | **414.5 s** |

At SF10, the recorded PostgreSQL and pg_duckdb totals were 21.8× and 4.7× the YAAP total. These are ratios of historical runs with different test settings.

| SF100 execution outcome | PostgreSQL | pg_duckdb | pg_yaap |
| --- | ---: | ---: | ---: |
| Queries completed | 18/22 | 18/22 | 22/22 |
| Timeouts | 4 | 4 | 0 |

**SF100 correctness:** YAAP returned a negative `sum_charge` for the Q1 `N` / `O` group. Both baselines returned a positive value. Each baseline timeout adds 600 s to its total, and the baselines used 2 GB of `shared_buffers` versus 4 GB for YAAP. The table reports execution time and completion. Result equivalence has not been established.

See the [benchmark conditions, per-query charts, and reproduction entry points](docs/benchmark.md).

## Key Features

| Feature | Implementation |
| --- | --- |
| Independent optimizer | YAAP logical and physical planning, including decorrelation, predicate pushdown, and cost-based join ordering. |
| Vectorized execution | Columnar `DataChunk` batches of up to 2,048 rows and batch-oriented operators. |
| Parallel pipelines | A dependency DAG schedules `RUN`, `COMBINE`, and `FINALIZE` stages. |
| Multi-process runtime | PostgreSQL background workers claim tasks from a shared ready queue, with shared state in DSM and DSA. |
| Larger-than-memory processing | Memory-aware execution with spillable intermediates, including partitioned hash joins. |
| Native PostgreSQL storage | Heap scans use PostgreSQL's buffer manager and snapshots. |

## Architecture

![System architecture showing PostgreSQL parsing and storage around YAAP planning and execution](imgs/arch.png)

The planner hook builds a YAAP physical plan. The executor lowers it into pipelines and returns rows through PostgreSQL's tuple interface. Once YAAP claims a query, planning or execution failures surface as errors.

Within a process, operators pass columnar batches directly along a pipeline. A pipeline breaker such as a hash build or aggregation publishes shared state for downstream pipelines. The leader builds the event graph and starts workers. Workers activate ready stages and claim tasks across pipelines from the query-wide queue. See the [execution engine architecture and scheduler](docs/pipeline_runtime.md#pipelines-and-event-dependencies).

The runtime exports scheduler events and per-process timings as Chrome Trace JSON for Perfetto. See [Execution Trace](docs/execution_trace.md).

## Quick Start

### Requirements

- **PostgreSQL 18.3** is the CI target. Use the target server's `pg_config` and server development headers.
- A C and C++17 toolchain, Meson, and Ninja.
- LLVM development files and a compatible `llvm-config` when PostgreSQL was built with LLVM. Pass `-Dllvm_config=/path/to/llvm-config` if needed. A PostgreSQL build without LLVM uses the non-JIT path.

### Build and install

From the repository root, using an account that can write to the target PostgreSQL installation:

```sh
meson setup build -Dpg_config=/path/to/postgresql/bin/pg_config -Dbuildtype=release
meson compile -C build
meson install -C build
```

For a build from PostgreSQL source or an offline installation, see the [offline build guide (中文)](docs/offline-linux-build.md).

### Run and verify

Connect to a test database with `psql` as a superuser and load YAAP for the session.

```sql
LOAD 'pg_yaap';
SHOW pg_yaap.enabled;
```

Run plain `EXPLAIN` on an analytical query to check for `Planner: YAAP optimizer` and `YAAP Physical Plan`. To confirm execution, enable `pg_yaap.trace_execution_path` and look for `pg_yaap_path: path=pipeline detail=yaap_pipeline` in the server log after running the query.

## Design Philosophy

| Design choice | PostgreSQL | pg_duckdb | pg_yaap |
| --- | --- | --- | --- |
| Query optimizer and execution engine | PostgreSQL | Embedded DuckDB | YAAP |
| Execution model | Tuple-oriented | Vectorized DuckDB | Vectorized YAAP |
| Parallel execution | PostgreSQL processes | DuckDB threads, with PostgreSQL workers available for heap scans | PostgreSQL background workers |
| Query native PostgreSQL tables | Yes | Yes | Yes |

YAAP is designed around PostgreSQL's multi-process architecture, with its own optimizer, operators, and scheduler. The pg_duckdb column describes the upstream design. [pg_duckdb project and settings](https://github.com/duckdb/pg_duckdb/blob/main/docs/settings.md)

## Current Status and Roadmap

- **Optimizer** · Logical and physical planning for supported analytical SQL.
- **Executor** · Parallel sequential scan, hash join, hash and perfect hash aggregation, Top-N, and disk spill.
- **Runtime** · Per-query background workers, event DAG scheduling, a shared task queue, and execution tracing.
- **Current limits** · SQL coverage is partial. Sort uses single-process PostgreSQL tuplesort. YAAP's scaled-integer `NUMERIC` can differ from PostgreSQL's exact decimal results.

Potential roadmap items include TPC-H result validation and correctness fixes, parallel sort, worker pooling, broader SQL coverage, and memory and I/O optimization.

## Documentation

| Topic | Reference |
| --- | --- |
| Pipeline scheduling and worker lifecycle | [Pipeline runtime](docs/pipeline_runtime.md) |
| Spill and partitioned probe | [Hybrid hash join](docs/hybrid_hash_join.md), [Partitioned hash join probe](docs/hash_join_partition_probe.md) |
| Memory budgeting | [DSA memory limits](docs/dsa_memory_limits.md) |
| Trace setup and event types | [Execution Trace](docs/execution_trace.md) |
| Benchmark methodology | [TPC-H snapshots](docs/benchmark.md) |
| Building PostgreSQL and YAAP offline | [Offline Linux build (中文)](docs/offline-linux-build.md) |

## Acknowledgements and License

Thanks to PostgreSQL for the extension infrastructure and to DuckDB for inspiring YAAP's optimizer and execution design.

pg_yaap is licensed under the [Apache License 2.0](LICENSE).
