# pg_yaap — Yet Another Analytic Processing ⚡

<p align="center">
  <img src="imgs/logo.png" alt="pg_yaap logo" width="480" height="480"/>
</p>

<p align="center">
  <strong>Modern OLAP inside PostgreSQL: a YAAP-owned optimizer plus a C++ columnar execution engine.</strong>
</p>

<p align="center">
  <a href="README.zh.md">🇨🇳 中文</a>
  ·
  <a href="LICENSE">📄 License</a>
  ·
  <a href="#quick-start-">🚀 Quick Start</a>
  ·
  <a href="#architecture-">🏗️ Architecture</a>
  ·
  <a href="#tpc-h-snapshot-">📊 Benchmark</a>
</p>


---

## What is pg_yaap? 🚀

`pg_yaap` is a PostgreSQL extension that replaces PostgreSQL's planner and executor path for supported analytical queries with a YAAP-owned optimizer and a C++ columnar execution engine.

The key point: **YAAP does not replace PostgreSQL itself.**

PostgreSQL still owns the boring-but-critical database stuff:

* 🧾 SQL parsing
* 🗂️ catalog access
* 💾 storage
* 🔒 transactions
* 🌐 protocol and result delivery
* 🧩 the rest of the server runtime

YAAP takes over the analytical hot path:

* 🧠 logical planning
* 🛠️ physical planning
* 📦 vectorized / columnar execution
* 🧵 parallel pipeline execution

In short: PostgreSQL remains the system of record. YAAP gives selected OLAP queries a much more modern execution path.

---

## Why pg_yaap? ⚡

PostgreSQL is great as a general-purpose database, but analytical workloads often want a very different execution model from the classic tuple-at-a-time executor.

`pg_yaap` explores that direction without turning PostgreSQL into a fork:

* 🐘 keep PostgreSQL parser, catalog, storage, and transaction semantics
* 🧠 use a YAAP-owned optimizer for supported analytical queries
* 📦 process data in columnar batches instead of one tuple at a time
* 🏗️ organize execution around pipelines and explicit pipeline breakers
* 🔥 fail loudly after YAAP claims a query instead of silently falling back

No magic. No hidden fallback. If YAAP owns the query, YAAP owns the result.

---

## Core ideas 🧠

* **YAAP-owned optimizer**
  Supported analytical queries are routed into YAAP's logical and physical planners.

* **Columnar execution engine**
  Runtime execution is based on batches instead of PostgreSQL's traditional per-tuple executor model.

* **Pipeline-based execution**
  Operators are organized into execution pipelines, with joins, aggregation, and sort treated as explicit pipeline breakers.

* **PostgreSQL-native integration**
  YAAP enters through PostgreSQL hooks and returns results through PostgreSQL's normal tuple interfaces.

* **No silent fallback**
  Once YAAP admits a query, it should either run it or fail clearly. This keeps development and benchmarking honest.

---

## Architecture 🏗️

![YAAP architecture](imgs/arch.png)

At a high level:

1. `planner_hook` routes supported queries into YAAP's logical and physical planners.
2. `ExecutorStart` and `ExecutorRun` hand admitted queries to YAAP's runtime.
3. YAAP executes columnar batches through parallel pipelines.
4. Results are converted back to PostgreSQL's normal tuple interface.

See [Pipeline runtime and process communication](docs/pipeline_runtime.md) for task scheduling, shared-state ownership, synchronization, and current limits.

---

## TPC-H snapshot 📊

![TPC-H median runtime chart](imgs/comparison.svg)

> Updated `pg_yaap` SF10 snapshot (Apple M4 Pro / 24GB, `shared_buffers=4GB`, `pg_yaap.memory_limit=8GB`, 12 workers): **20.5s** SQL time (**21.4s** wall time) across all 22/22 queries (down from 30.8s; vs PostgreSQL `447.9s` and `pg_duckdb` `95.8s` from the baseline run). Benchmark numbers depend on hardware, PostgreSQL configuration, dataset scale factor, query coverage, and whether the query is fully supported by YAAP. Treat this chart as a snapshot, not a universal claim.

### SF100 (100GB)

![TPC-H SF100 comparison](imgs/comparison-sf100.svg)

This snapshot uses the `tpch100g` database (about 125GB on disk) on a 24GB M4 Pro. In the updated `pg_yaap` run (`shared_buffers=4GB`, 12 workers, `pg_yaap.memory_limit=8GB`), all **22/22** queries complete in **414.5s** SQL time (**425.3s** wall time, 0 timeouts — down from `619.5s`). PostgreSQL native (`3,302.0s`, 18/22 OK, 4 timeouts) and `pg_duckdb` (`3,346.6s`, 18/22 OK, 4 timeouts, default 4GiB memory and scan parallelism) are from the `shared_buffers=2GB` baseline run; each timeout counts as 600 seconds in the total.

**Correctness note:** YAAP's SF100 Q1 `N|O` `sum_charge` differs from both PostgreSQL and pg_duckdb: YAAP returns a negative value while the other two return a positive value. The chart compares runtime; it does not imply matching results.

---

## Build 🔧

### Meson

```bash
meson setup build -Dpg_config=/path/to/pg_config
meson compile -C build
meson install -C build
```

### PGXS / Makefile

```bash
make PG_CONFIG=/path/to/pg_config
make PG_CONFIG=/path/to/pg_config install
```

---

## Quick start 🧪

```sql
LOAD 'pg_yaap';
SET pg_yaap.enabled = on;
SET pg_yaap.parallel = on;
```

Then run supported analytical SQL queries as usual.

```sql
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price
FROM lineitem
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

---

## Current scope 🚧

`pg_yaap` is focused on analytical query processing. It is not trying to replace PostgreSQL as a database system.

Good fit:

* ✅ scan-heavy OLAP queries
* ✅ TPC-H-style analytical workloads
* ✅ modern optimizer / executor experiments inside PostgreSQL
* ✅ columnar and vectorized execution research
* ✅ pipeline and parallel execution research

Not the goal:

* ❌ replacing PostgreSQL storage
* ❌ replacing PostgreSQL parser or catalog
* ❌ becoming a general-purpose PostgreSQL fork
* ❌ silently masking unsupported behavior with fallback execution
* ❌ general OLTP acceleration

---

## Acknowledgements 🙏

Thanks to the PostgreSQL project for its hook architecture, extension ecosystem, and decades of database engineering.

Thanks to DuckDB for inspiring many modern ideas around optimizer structure, vectorized execution, and columnar analytical processing.

---

## 中文版 🇨🇳

For the Chinese version, see [README.zh.md](README.zh.md).

---

## License 📄

Apache License 2.0 — see [LICENSE](LICENSE).

