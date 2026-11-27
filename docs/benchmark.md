# TPC-H Benchmark Methodology & Records

This document records the controlled environment, shared PostgreSQL server configuration, engine parameters, known correctness/stability caveats, and full per-query results for the TPC-H SF10 and SF100 benchmarks referenced in the README.

---

## 1. Hardware & Software Environment

| Component | Value |
|---|---|
| **Host** | MacBook Pro (`Mac16,8`), Apple M4 Pro (14 cores: 10 Performance + 4 Efficiency) |
| **System RAM** | 24 GB unified memory |
| **Storage format** | Native PostgreSQL UTF8 heap relations (`tpch` SF10 ~13 GB, `lineitem` 59,986,052 rows; `tpch100g` SF100 ~125 GB, `lineitem` 600,037,902 rows / 86 GB, `ANALYZE`d, no secondary indexes) |
| **PostgreSQL version** | `18.3` (compiled with Meson `-Dbuildtype=release -Dllvm=enabled`) |
| **`pg_yaap` build** | Meson `-Dbuildtype=release` (`-O3`), commit `8ea2c59` (merged as `1d4bf59` on `main`) |
| **`pg_duckdb` build** | `pg_duckdb` tag **`v1.2.2`** (commit `4e921ff`, bundled **DuckDB `v2.0.1`**), compiled cleanly against PG `18.3` with `-O3` |

---

## 2. Controlled Configuration & Methodology

All three engines (`pg_yaap`, `pg_duckdb v1.2.2`, and `PostgreSQL 18.3`) ran on the **same PostgreSQL `18.3` instance and database** with identical server parameters:

```text
-c shared_buffers=4GB
-c max_parallel_workers=12
-c max_parallel_workers_per_gather=12
-c max_worker_processes=64
-c io_method=worker
-c io_workers=32
-c effective_io_concurrency=64
-c io_combine_limit=128kB
-c shared_preload_libraries=pg_duckdb
```

### Per-Engine Session Settings

- **`PostgreSQL 18.3` (native parallel):**
  ```sql
  SET pg_yaap.enabled = off;
  SET duckdb.force_execution = false;
  SET max_parallel_workers_per_gather = 12;
  SET jit = on;
  ```
- **`pg_duckdb v1.2.2` (DuckDB `v2.0.1`):**
  ```sql
  SET pg_yaap.enabled = off;
  SET duckdb.force_execution = true;
  SET duckdb.max_memory = 8192;                 -- 8 GB, aligned with pg_yaap.memory_limit
  SET duckdb.threads = -1;                      -- all 14 CPU cores (tuned)
  SET duckdb.max_workers_per_postgres_scan = 8; -- decoupled from threads_for_postgres_scan
  SET duckdb.threads_for_postgres_scan = 2;     -- keeps GlobalProcessLock contention low
  ```
  *(Note: Raising `threads_for_postgres_scan` above `2` regresses multi-table join queries due to `GlobalProcessLock` contention every 32 tuples; raising `max_workers_per_postgres_scan` to `8` while keeping `threads_for_postgres_scan = 2` improves SF10 total runtime from `73.46 s` at default `2/2` down to `63.28 s`.)*
- **`pg_yaap`:**
  ```sql
  LOAD 'pg_yaap';
  SET duckdb.force_execution = false;
  SET pg_yaap.enabled = on;
  SET pg_yaap.parallel_max_workers = 12;
  SET pg_yaap.memory_limit = '8GB';
  SET pg_yaap.memory_soft_limit = -1;
  SET pg_yaap.trace_enabled = off;
  ```

> **Memory limit semantics:** `pg_yaap.memory_limit = 8GB` caps **shared pipeline-breaker payloads** in DSA (and `pg_yaap.dsa_memory_limit = -1` permits up to `payload + min(payload / 2, 4 GiB)` in DSA segments). It is **not** a whole-query RSS cap — `shared_buffers` (4 GB), PostgreSQL backend/worker private `MemoryContext`s, LLVM JIT state, and OS file caches are outside `pg_yaap.memory_limit`. See [docs/dsa_memory_limits.md](dsa_memory_limits.md).

### Execution & Timeout Policy

- **SF10 (`tpch`, UTF8)**: 1 unmeasured 22-query warmup pass, followed by an interleaved sweep (`yaap` → `duckdb` → `pg` per query) with **`30 s` `statement_timeout`** (`+8 s` client termination watchdog).
- **SF100 (`tpch100g`, UTF8, 125 GB)**: Interleaved sweep (`yaap` → `duckdb` → `pg` per query) with **`100 s` `statement_timeout`** (without OS cache purge; exceeds 24 GB RAM and 4 GB `shared_buffers`). PostgreSQL `Q09` initially hit `pgsql_tmp` disk exhaustion when only 35 GiB remained free before artifact cleanup and was re-run cleanly with 166 GiB free, hitting `statement_timeout` at `100 s` (`101.78 s` wall).

---

## 3. Correctness Verification & Known Issues

- **SF10**: `pg_yaap` completed `22/22` with `0` timeouts and matched `pg_duckdb v1.2.2` (`22/22`) and `PostgreSQL 18.3` (`19/19` completed queries) row-for-row.
- **SF100**: `pg_yaap` completed `22/22` with `0` timeouts and matched `pg_duckdb v1.2.2` (`22/22`) and `PostgreSQL 18.3` (`16/16` completed queries) row-for-row.
  - *Resolved during SF100 verification*: (1) `SUM_NUMERIC`/`AVG_NUMERIC` accumulators were upgraded to 128-bit `NumericWideInt` with runtime overflow promotion on 64-bit projection arithmetic, eliminating the SF100 Q1 `N|O` `sum_charge` overflow (`11016932248183.655467` exact match to all 20 digits in `15.14 s`); (2) an earlier `SIGBUS` in `dsa_free` on SF100 Q21 caused by PostgreSQL's `dsa.c::make_new_segment()` odd-sized segment pagemap overflow (upstream PG commit `4da2afd01f938af35c1a52bbf6bc40baa52462f6`) was mitigated extension-side via `YaapDsaSafeAllocBytes` + a `512 MiB` single-array cap in `HashJoinBatch` (`61.45 s`, `100/100` rows match `pg_duckdb`).

---

## 4. Controlled Summary Results

### SF10 (`tpch`, `30 s` timeout cap)

| Query | `pg_yaap` SQL (`ms`) | `pg_yaap` Wall (`ms`) | `pg_duckdb v1.2.2` SQL (`ms`) | `PostgreSQL 18.3` SQL (`ms`) | Output Match (`YAAP vs DuckDB / PG`) |
|---:|---:|---:|---:|---:|:---:|
| Q01 | 658.66 | 677.17 | 5907.60 | 2777.15 | MATCH / MATCH |
| Q02 | 660.95 | 680.19 | 1223.41 | 30000.00 (30 s timeout) | MATCH / TIMEOUT |
| Q03 | 1265.84 | 1284.95 | 3110.27 | 1894.64 | MATCH / MATCH |
| Q04 | 782.00 | 801.58 | 2857.68 | 1927.72 | MATCH / MATCH |
| Q05 | 898.39 | 917.49 | 4239.14 | 984.45 | MATCH / MATCH |
| Q06 | 585.83 | 604.71 | 723.94 | 588.99 | MATCH / MATCH |
| Q07 | 868.11 | 887.07 | 2335.16 | 1005.42 | MATCH / MATCH |
| Q08 | 968.29 | 987.59 | 4419.35 | 2751.04 | MATCH / MATCH |
| Q09 | 2482.07 | 2501.93 | 5623.62 | 13479.51 | MATCH / MATCH |
| Q10 | 1974.00 | 2019.44 | 1694.32 | 1860.77 | MATCH / MATCH |
| Q11 | 311.58 | 330.70 | 1013.30 | 345.89 | MATCH / MATCH |
| Q12 | 884.33 | 903.05 | 1523.20 | 1582.47 | MATCH / MATCH |
| Q13 | 844.78 | 863.64 | 1020.14 | 5273.99 | MATCH / MATCH |
| Q14 | 837.89 | 856.93 | 773.20 | 816.56 | MATCH / MATCH |
| Q15 | 601.43 | 621.36 | 1458.56 | 1819.98 | MATCH / MATCH |
| Q16 | 364.73 | 413.84 | 457.00 | 672.83 | MATCH / MATCH |
| Q17 | 608.04 | 627.20 | 6629.09 | 30000.00 (30 s timeout) | MATCH / TIMEOUT |
| Q18 | 1302.57 | 1324.49 | 7050.59 | 12481.03 | MATCH / MATCH |
| Q19 | 635.45 | 676.04 | 1089.36 | 822.79 | MATCH / MATCH |
| Q20 | 839.12 | 860.11 | 1313.15 | 30000.00 (30 s timeout) | MATCH / TIMEOUT |
| Q21 | 2071.62 | 2091.29 | 8002.62 | 4506.55 | MATCH / MATCH |
| Q22 | 365.31 | 387.26 | 811.33 | 403.94 | MATCH / MATCH |
| **Total (Completed SQL)** | **20810.99** (`22/22`) | **21318.03** | **63276.03** (`22/22`) | **55995.72** (`19/22`) | `22/22` / `19/19` |
| **Total (timeout = `30 s`)** | **20810.99** (`20.81 s`) | — | **63276.03** (`63.28 s`) | **145995.72+** (`146.00 s+`) | `3 × 30 s` PG timeouts (`Q02, Q17, Q20`) |

### SF100 (`tpch100g`, UTF8, `100 s` timeout cap)

| Query | `pg_yaap` SQL (`ms`) | `pg_yaap` Wall (`ms`) | `pg_duckdb v1.2.2` SQL (`ms`) | `PostgreSQL 18.3` SQL (`ms`) | Notes |
|---:|---:|---:|---:|---:|---|
| Q01 | 15138.38 | 15160.00 | 57623.46 | 25375.80 | Exact match (`128-bit` accumulator) |
| Q02 | 5795.96 | 5819.32 | 9764.70 | 100000.00 (100 s timeout) |  |
| Q03 | 20272.13 | 20390.19 | 28650.60 | 41686.89 |  |
| Q04 | 21263.33 | 21290.00 | 39081.07 | 44138.88 |  |
| Q05 | 20702.53 | 20728.04 | 43512.65 | 36595.09 |  |
| Q06 | 15969.85 | 15995.68 | 15658.20 | 15807.24 |  |
| Q07 | 22221.47 | 22244.25 | 23946.10 | 29010.25 |  |
| Q08 | 21237.16 | 21261.51 | 48841.61 | 22802.48 |  |
| Q09 | 40369.79 | 40398.20 | 57898.25 | 100000.00 (100 s timeout) |  |
| Q10 | 34943.70 | 34978.43 | 23093.17 | 37245.43 |  |
| Q11 | 2966.33 | 3023.80 | 9205.58 | 5689.96 |  |
| Q12 | 19039.17 | 19062.39 | 24001.05 | 33884.58 |  |
| Q13 | 10671.31 | 10701.62 | 8513.74 | 57559.12 |  |
| Q14 | 17561.97 | 17586.47 | 17329.96 | 18080.71 |  |
| Q15 | 15854.80 | 15879.74 | 30697.64 | 55684.14 |  |
| Q16 | 4877.69 | 4937.89 | 4421.45 | 29330.27 |  |
| Q17 | 15709.98 | 15732.44 | 66678.21 | 100000.00 (100 s timeout) |  |
| Q18 | 25928.83 | 26044.57 | 70196.38 | 100000.00 (100 s timeout) |  |
| Q19 | 16282.36 | 16418.19 | 16420.02 | 27094.08 |  |
| Q20 | 19073.81 | 19144.61 | 19694.47 | 100000.00 (100 s timeout) |  |
| Q21 | 61445.65 | 61470.00 | 85322.46 | 100000.00 (100 s timeout) | `100/100` rows match `pg_duckdb` |
| Q22 | 5267.73 | 5291.71 | 9604.27 | 21412.84 |  |
| **Total (Completed SQL)** | **432593.93** (`22/22`) | **433403.77** | **710155.04** (`22/22`) | **501397.76** (`16/22`) | `22/22` exact match; `1.64×` vs `pg_duckdb v1.2.2`; `1.90×` vs PG on PG's 16 completed (`264.27 s` vs `501.40 s`) |
| **Total (timeout = `100 s`)** | **432593.93** (`432.59 s`) | — | **710155.04** (`710.16 s`) | **1101397.76+** (`1,101.40 s+`) | `2.55×+` vs PG (`6 × 100 s` timeouts: `Q02, Q09, Q17, Q18, Q20, Q21`) |

*(For historical Q18 Execution Trace details, see [docs/execution_trace.md](execution_trace.md) and [docs/tpch_sf10_q18_trace.json](tpch_sf10_q18_trace.json).)*
