# pg_yaap — Agent & Developer Skeleton

PostgreSQL extension that intercepts analytical `SELECT` queries and runs them through a DuckDB-inspired C++ optimizer and parallel vectorized pipeline engine (`shared_preload_libraries` or `LOAD 'pg_yaap'`).

---

## Execution Path

```text
PostgreSQL Parser -> pg_yaap_planner_hook
  -> YaapAdapter (Query -> LogicalOperator)
  -> LogicalPlanner::Normalize (decorrelation)
  -> LogicalOptimizer::Optimize (predicate pushdown, DP join order, CSE)
  -> PhysicalPlanner::CreatePlan (YAAP PhysicalOperator tree -> OptimizerPlanBundle)
  -> pg_yaap_ExecutorStart -> BuildPipelineFromOptimizerPlan (yaap_opt_translator)
  -> pg_yaap_ExecutorRun   -> pipeline::PgYaapPipelineRun (Event DAG + bgworkers)
```

The optimizer-owned `PhysicalOperator` tree (`OptimizerPlanBundle`) lowered via `yaap_opt_translator` is the **only supported path**. Legacy PG-plan `translator.cpp` is not maintained for new features.

---

## Core Invariants (Must Follow)

1. **No silent fallback:** Non-`SELECT`, DDL, unsupported cursors (`SCROLL`/`HOLD`/`BACKWARD`/`REWIND`/`MARK`), `pg_yaap.enabled = off`, and unsupported plan shapes are rejected **before** registration and stay on PostgreSQL. Once a plan bundle/state is registered, any failure raises `ereport(ERROR)` immediately — never fall back silently to PostgreSQL.
2. **Optimizer alignment (`src/optimizer/`):** Keep optimizer structure, query-graph/DP join ordering, and rule responsibilities aligned with DuckDB. Fix optimizer discrepancies in `src/optimizer/`, not via workarounds in `yaap_opt_translator` or `EXPLAIN`.
3. **NUMERIC semantics:** Scan and expression hot paths encode `NUMERIC` as scale-2 `int64_t` (`DEFAULT_NUMERIC_SCALE = 2`), promoting automatically via runtime overflow checks to 128-bit (`__int128`) in arithmetic expressions; `SUM`/`AVG` accumulators use 128-bit integers internally. Wider decimals fall back to `Wide128` (interpreter-only, no JIT). Minor rounding differences vs PG exact `NUMERIC` are intentional.
4. **C/C++ & hook boundary (`src/bridge/`):**
   - Keep pure C hook files in `src/bridge/` only; `src/optimizer/` and `src/executor/` are C++17.
   - `ExecutorStart_hook` runs `prev_ExecutorStart` / `standard_ExecutorStart` first (unless optimizer-only `EState` is built by `pg_yaap_start_optimizer_executor`) and **must early-out when `IsParallelWorker()` is true** (workers enter via `pipeline_worker_main.cpp`).
   - Catch or guard C++ scopes with `PG_TRY`/`PG_CATCH` before `ereport(ERROR)` so DSA handles, LLVM JIT contexts, and `MemoryContext`-backed C++ objects do not leak on `longjmp`.
5. **Memory & cross-process state:**
   - Use `state->context` (per-query `MemoryContext`) and `PgMemoryContextAllocator` / `PgVector` / `PgUnorderedMap` — never raw `new`/`malloc` for query-lifetime state.
   - Processes never share C++ objects, virtual tables, or `palloc` pointers. Cross-process payloads live in query DSM/DSA (`dsa_pointer` offsets registered on descriptors in `pipeline_descriptor.cpp`, `one dsa_allocate0 per payload`).

---

## Repository Skeleton

| Component | Path | Responsibility |
|---|---|---|
| Bridge | `src/bridge/` | `_PG_init`, 12 GUCs, planner/EXPLAIN/executor hooks, `OptimizerPlanBundle` registry, `QueryDesc` state HTAB |
| Optimizer | `src/optimizer/` | PG `Query` adapter, subquery decorrelation, rule-based optimizer, DP join ordering, physical planner |
| Core & Expr | `src/executor/engine/` | `DataChunk<2048>`, tuple deform + LLVM JIT, vectorized `VecExprProgram` + JIT, PG allocator wrappers |
| Pipeline Runtime | `src/executor/engine/parallel/pipeline/` | `yaap_opt_translator*`, `MetaPipeline` / Event DAG (`TaskScheduler`), `DsmTaskQueue`, `PipelineLeader`, bgworker `main`, parallel operators (`SeqScan`/`ReadStream`, Hybrid `HashJoin`, `HashAgg`, `TopN`, `Order`, `OutputSink`) |
| Tests & SQL | `tests/`, `sql/`, `expected/` | Meson C++ unit tests (`meson.build`), `pg_regress` SQL fixtures, Python integration suites |

Detailed runtime & IPC design: `docs/pipeline_runtime.md` · GUC reference: `docs/configuration.md` · Testing guide: `docs/testing.md`.

---

## Build & Test

```bash
# Configure / compile / install (from PG build or standalone meson dir)
meson compile -C build
meson install -C build --only-changed
meson test -C build --print-errorlogs
```
