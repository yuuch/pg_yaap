# pg_yaap — Yet Another Analytic Processing

**PostgreSQL extension that replaces the optimizer and executor for OLAP queries with a high-performance C++ columnar engine.**

---

## Overview

pg_yaap is a PostgreSQL extension (loaded via `shared_preload_libraries` or `CREATE EXTENSION`) that hooks into PostgreSQL's planner and executor to intercept `SELECT` queries and run them through its own C++ optimizer and columnar execution engine. The goal is to deliver modern OLAP performance within PostgreSQL without forking the server — it runs in-process, uses PG's storage layer (buffer manager, heap scan), and returns results through standard PG tuple slots.

### Supported execution path

| Path | Optimizer | Executor | Status |
|------|-----------|----------|--------|
| **Optimizer → Executor** | YAAP's own `LogicalPlanner` → `LogicalOptimizer` → `PhysicalPlanner` | `yaap_opt_translator` → YAAP pipeline runtime | Active |

The supported path is the optimizer-owned `PhysicalOperator` tree registered in `OptimizerPlanBundle`. PostgreSQL plan translation may still exist as legacy code, but it is no longer the intended architecture or a supported target for new fixes.

---

## Failure Philosophy

**No silent fallback.** This is a deliberate design choice throughout the codebase:

- **Optimizer failures**: If any optimizer pass returns null, encounters an unsupported expression node, or throws an exception — `ereport(ERROR)` is raised immediately. There is no attempt to fall back to the PG optimizer.
- **Executor failures**: If plan lowering produces null, or pipeline execution fails — `ereport(ERROR)` with a diagnostic message. The bridge's `pg_yaap_execute_query` returns `true` on success only; `false` is reserved for the "no state registered" case, not for partial failure.

The rationale: silent fallbacks conceal correctness bugs and make performance regressions invisible. If YAAP claims a query, it runs it completely or fails loudly. Queries that are genuinely unsupported should be caught early — either the admission filter in `state.c` rejects them before state is allocated, or the optimizer build step in the planner hook returns null (so no bundle is registered, and the query never reaches the executor hooks).

---

## NUMERIC: Scale-2 int64 by Design

To avoid the extreme overhead of PostgreSQL's arbitrary-precision NUMERIC in the hot path, YAAP stores `NUMERIC(15,2)` values as **scaled int64** — the numeric value multiplied by 100 and stored as a 64-bit integer.

- `DEFAULT_NUMERIC_SCALE = 2` (defined in `core/types.hpp`)
- Scaled int64 covers values from approximately −9.2 × 10^16 to +9.2 × 10^16 ÷ 100, i.e. ±9.2 × 10^14 in decimal — sufficient for TPC-H money columns.
- Wider numerics (precision > 18 decimal digits) fall back to `Wide128` (emulated 128-bit integer), which uses the interpreter path only (no JIT for Wide128).
- Aggregation accumulators use `NumericWideInt` (128-bit) internally to avoid overflow, then narrow back to int64 at output time.

**Precision differences vs PostgreSQL NUMERIC are expected and accepted.** The deform path in `data_chunk_deform.hpp` shows the conversion: `(int64_t)(DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow, val)) * 100.0 + 0.5)`. This rounds to the nearest cent. PG's NUMERIC is exact decimal; YAAP trades that exactness for performance. The TPC-H specification allows minor precision differences, and this is a documented design trade-off.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        PostgreSQL Core                          │
│  planner_hook → pg_yaap_planner_hook                            │
│  ExecutorStart/Run/Finish/End hooks → pg_yaap wrappers          │
└──────────────────────┬──────────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────────┐
│  src/bridge/          C boundary + C++ glue                     │
│  ┌──────────────┐  ┌──────────┐  ┌──────────────────────────┐   │
│  │ pg_yaap.c    │  │ state.c  │  │ execute.cpp               │   │
│  │ _PG_init     │  │ HTAB     │  │ initialize_plan           │   │
│  │ GUCs         │  │ admission│  │ execute_query → pipeline   │   │
│  │ hooks        │  │ lifecycle│  │ delete_plan               │   │
│  └──────────────┘  └──────────┘  └──────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ optimizer_registry.cpp  —  PlannedStmt → Bundle mapping   │   │
│  └──────────────────────────────────────────────────────────┘   │
└──────────────────────┬──────────────────────────────────────────┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
┌───────▼────────┐           ┌────────▼───────────────────────────┐
│ src/optimizer/ │           │ src/executor/engine/               │
│                │           │                                    │
│ adapter/       │           │ core/    DataChunk, types, memory  │
│  yaap_adapter  │           │ expr/    VecExprProgram + JIT      │
│  PG Query →    │           │ llvmjit  deform + expr JIT         │
│  LogicalOp     │           │ parallel/pipeline/                 │
│                │           │   Translator PG Plan → PhysicalOp  │
│ planner/       │           │   physical_* operator impls        │
│  logical_plan  │           │   pipeline_leader  execution       │
│  normalizer    │           │   task_scheduler    parallel       │
│  decorrelation │           │   output_sink  chunk → slot        │
│                │           │   yaap_opt_translator              │
│ optimizer/     │           │     YAAP PhysicalOp → pipeline     │
│  filter_push   │           │     PhysicalOp                     │
│  predicate_prop│           │                                    │
│  join_order    │           │                                    │
│  cardinality   │           │                                    │
│                │           │                                    │
│ physical/      │           │                                    │
│  physical_plan │           │                                    │
│  LogicalOp →   │           │                                    │
│  PhysicalOp    │           │                                    │
└────────────────┘           └────────────────────────────────────┘
```

### Bridge Layer (`src/bridge/`)

The only C files in the project live here. This layer is the outer boundary between PostgreSQL and the C++ engine.

| File | Purpose |
|------|---------|
| `pg_yaap.c` | `_PG_init`/`_PG_fini`, 10 GUCs, hook chaining (planner + 4 executor hooks), debug pretty-printer |
| `state.h` / `state.c` | Per-query state HTAB keyed by `QueryDesc *`, admission filter (`plan_uses_supported_relations`), lifecycle |
| `execute.h` / `execute.cpp` | C++ shim: initialize from optimizer bundle, dispatch into pipeline runtime, teardown |
| `optimizer_registry.h` / `.cpp` / `.hpp` | Registers/lookup/discards `OptimizerPlanBundle` mapped to `PlannedStmt *` |

**Hook flow:**
1. `planner_hook` (before PG plans): Runs YAAP optimizer. On success, registers bundle and forces PG planner GUCs to disable parallel workers (YAAP manages its own parallelism). On failure → `ereport(ERROR)`.
2. `ExecutorStart_hook` (after PG's `standard_ExecutorStart`): Builds per-query state. Calls `initialize_plan` to lower the optimizer bundle. Worker processes (`IsParallelWorker()`) are bypassed.
3. `ExecutorRun_hook`: Looks up state. If present, calls `execute_query` → `PgYaapPipelineRun`. On success, returns `true` (PG executor is skipped). On failure → `ereport(ERROR)`.
4. `ExecutorEnd_hook`: Cleans up per-query state and optimizer bundle.

### Optimizer (`src/optimizer/`)

A DuckDB-inspired optimizer pipeline that converts PostgreSQL's `Query` parse tree into a YAAP physical plan.

**Project constraint:** keep the optimizer architecture, join-order/query-graph machinery, and rule responsibilities as close to DuckDB as practical. If a YAAP plan diverges materially from DuckDB, fix the optimizer/planner modules first rather than compensating in `yaap_opt_translator` or other explain-only code.

**Pipeline:**
```
PG Query → YaapAdapter::TranslatePGQuery → LogicalOperator tree
  → LogicalPlanner::Normalize (decorrelation, mark join, delim join)
  → LogicalOptimizer::Optimize:
      1. JoinPredicateExtraction  (pull join conditions out of filters)
      2. FilterPushdown           (push filters toward leaf scans)
      3. PredicatePropagation     (propagate equality constraints)
      4. ScanFilterFolding        (fold filters into LogicalGet)
      5. CardinalityEstimator     (estimate row counts)
      6. JoinOrderOptimizer       (DP-based join ordering)
  → PhysicalPlanner::CreatePlan → PhysicalOperator tree
```

**Key types (in `yaap_adapter.hpp`):**
- `LogicalOperator` base with 16 operator types (`LOGICAL_GET`, `LOGICAL_FILTER`, `LOGICAL_AGGREGATE_AND_GROUP_BY`, `LOGICAL_COMPARISON_JOIN`, `LOGICAL_DEPENDENT_JOIN`, etc.)
- `Expression` hierarchy: `BoundColumnRef`, `BoundConstant`, `BoundFunction`, `BoundAggregate`, `BoundConjunction`, `BoundSubquery`
- `TableIndex` / `ColumnBinding` — logical column references across operator boundaries
- `JoinType` — maps PG join types including `MARK` and `SINGLE` for subquery decorrelation

**Physical plan (in `physical_plan.hpp`):**
- `PhysicalTableScan`, `PhysicalHashJoin`, `PhysicalHashAggregate`, `PhysicalProjection`, `PhysicalFilter`, `PhysicalOrderBy`, `PhysicalLimit`, `PhysicalDistinct`, `PhysicalWindow`, `PhysicalSetOperation`, `PhysicalCrossProduct`, `PhysicalDelimGet`

### Executor (`src/executor/engine/`)

The columnar execution engine with JIT compilation support and a parallel pipeline runtime.

**Core data structures:**
- `DataChunk<2048>` — Fixed-size columnar batch with 16 slots each for `double`, `int64_t`, `int32_t`, `VecStringRef`, plus nulls bitmap, selection vector, dictionary encoding, and a string arena
- `VecExprProgram` — Linear IR for vectorized expression evaluation. 64 registers (int32/int64/float8/nulls). Interpreted execution with optional LLVM JIT compilation.
- `DeformProgram` — Maps PG tuple attributes to DataChunk column slots. Supports `kInt32`, `kInt64`, `kDate32`, `kFloat8`, `kNumeric` (scaled int64), `kStringRef` (prefix + arena), `kBpchar1` decode kinds.

**Pipeline runtime (`parallel/pipeline/`):**

The parallel executor is structured as a DAG of pipelines connected at pipeline-breaking operators (hash join build, aggregate, sort).

| Component | Purpose |
|-----------|---------|
| `PhysicalOperator` | Base class with Source/Operator/Sink role flags. `BuildPipelines()` slices the tree into pipelines. |
| `Translator` | Recursively converts PG `Plan` nodes into `PhysicalOperator` tree. Shape-matched: supports `SeqScan`, `Agg`, `Sort`, `Limit`, `HashJoin`, `SubqueryScan`, `Material`, `Hash`, `Append`. |
| `MetaPipeline` | Top-level DAG of pipelines with dependency edges. Built from the `PhysicalOperator` tree. |
| `Pipeline` | Single straight-line chain of operators, executed by one or more workers reading from a shared source. |
| `PipelineLeader` | Orchestrates execution: builds MetaPipeline, schedules tasks, waits for completion. |
| `TaskScheduler` | Manages PG background workers via DSM task queues. Workers call `pipeline_worker_main.cpp`. |
| `OutputSink` | Materializes final `DataChunk` → `TupleTableSlot` → `DestReceiver`. |
| `yaap_opt_translator` | Translates YAAP `PhysicalOperator` (from optimizer) into pipeline `PhysicalOperator` for the parallel runtime. |

**Operator implementations:**
- `PhysicalSeqScan` — Parallel table scan with atomic block pool. Workers claim blocks via atomic counter, read through PG's `ReadStream` API.
- `PhysicalHashJoin` — Radix-partitioned (256-way) parallel hash join with bloom filter pre-filtering and linear-probe hash table.
- `PhysicalHashAggregate` — Hash-based aggregation with `AggregateHashTable`.
- `PhysicalPerfectHashAggregate` — Optimized path when group count is small enough to fit in a direct-mapped array.
- `PhysicalOrder` — Parallel sort using PG's `tuplesort` via `TuplesortInstrumentation`.
- `PhysicalProjection` — Column projection (select list). No data movement; just remaps which slots are considered output.
- `OutputSink` — Single-threaded final sink. Encodes DataChunk columns → Datum arrays → TupleTableSlot. Handles NUMERIC scale-2 encoding back to PG Datum.

**Memory management:**
- `PgMemoryContextAllocator` — Routes C++ STL container allocations through `palloc`/`pfree` tied to PG `MemoryContext`. Ensures containers are freed on context teardown, even across `ereport(ERROR)` longjmps.
- `PgVector<T>`, `PgUnorderedMap<K,V>` — STL container aliases using PG allocator.
- `AllocatePgShared<T>` — `allocate_shared` wrapper for PG-context-tied `shared_ptr`.

**NUMERIC encoding detail:**
- Deform: `(int64_t)(numeric_float8_no_overflow(val) * 100.0 + 0.5)` — rounds to nearest cent.
- Output: `Int64GetDatum(scaled_value / 100)` + build NUMERIC with scale 2 via `DirectFunctionCall2(numeric_int64_opt, Datum, Int32GetDatum(typmod))`.
- Aggregation accumulator: `NumericWideInt` (128-bit) to avoid overflow during SUM/AVG.
- AVG output: Reconstructs `(sum / count, remainder)` as a NUMERIC pair.

---

## GUCs

All defined in `pg_yaap.c::_PG_init`. All are `PGC_USERSET`.

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pg_yaap.enabled` | bool | `on` | Master switch. Off → all hooks short-circuit. |
| `pg_yaap.trace_hooks` | bool | `off` | LOG-level dump of every hook entry/exit. |
| `pg_yaap.trace_execution_path` | bool | `off` | One LOG line per query with dispatch path. |
| `pg_yaap.jit_deform` | bool | `on` | Enable LLVM JIT for tuple deform. |
| `pg_yaap.parallel` | bool | `on` | **Must be on** for pipeline execution. Off → `initialize_plan` returns false. |
| `pg_yaap.parallel_max_workers` | int | 4 (0–1024) | Max bgworkers per query. |
| `pg_yaap.parallel_min_relation_blocks` | int | 1024 (0–INT_MAX) | Min relation size before parallel lowering. |
| `pg_yaap.parallel_leader_participation` | bool | `on` | Leader runs pipeline tasks alongside workers. |
| `pg_yaap.parallel_experimental_hash_pipeline` | bool | `off` | Legacy GUC — registered but has no effect. |
| `pg_yaap.profile` | bool | `off` | Emit per-stage pipeline timing after each query. |

---

## Directory Map

```
pg_yaap/
├── meson.build                    # Top-level Meson: subdirs, module target, install, tests
├── pg_yaap.control                # Extension metadata
├── pg_yaap--1.0.sql               # LOAD 'MODULE_PATHNAME'
├── README                         # High-level project description
├── sql/select_star.sql            # Regression test: basic SeqScan
├── expected/select_star.out       # Expected output for regression test
├── src/
│   ├── bridge/                    # PG hook boundary (C + C++ glue)
│   │   ├── pg_yaap.c              # _PG_init, GUCs, hook wrappers
│   │   ├── state.h / state.c      # Query state HTAB, admission filter
│   │   ├── execute.h / execute.cpp # Plan init → Translator → pipeline dispatch
│   │   ├── optimizer_registry.h / .cpp / .hpp  # Optimizer bundle ↔ PlannedStmt map
│   │   └── AGENTS.md              # Bridge-level documentation
│   ├── optimizer/                 # YAAP optimizer (C++)
│   │   ├── adapter/               # PG Query → LogicalOperator translation
│   │   │   ├── yaap_adapter.hpp   # LogicalOperator + Expression type definitions
│   │   │   └── yaap_adapter.cpp   # Full PG parse tree walker
│   │   ├── catalog/               # PG catalog statistics access
│   │   │   └── pg_external_catalog.hpp / .cpp
│   │   ├── logical/               # Logical operator utilities
│   │   │   ├── expression_rewriters.hpp / .cpp
│   │   │   ├── filter_rewrite_utils.hpp / .cpp
│   │   │   ├── logical_utils.hpp / .cpp
│   │   │   └── mark_join_normalization.hpp / .cpp
│   │   ├── optimizer/             # Rule-based logical optimizer
│   │   │   ├── optimizer_core.hpp / .cpp        # LogicalOptimizer pipeline
│   │   │   ├── filter_pushdown.cpp
│   │   │   ├── predicate_propagation.cpp
│   │   │   ├── scan_filter_folding.cpp
│   │   │   ├── join_order_optimizer.cpp           # DP join enumerator
│   │   │   ├── join_order_cost_model.cpp
│   │   │   ├── join_order_query_graph.cpp
│   │   │   ├── join_order_plan_enumerator.cpp
│   │   │   ├── join_order_reconstruct.cpp
│   │   │   ├── join_predicate_extraction.cpp
│   │   │   └── optimizer_stats.cpp
│   │   ├── physical/              # Logical → Physical plan conversion
│   │   │   ├── physical_plan.hpp / physical_plan_generator.hpp/.cpp
│   │   │   └── physical_planner.hpp / .cpp
│   │   └── planner/               # Planner entry + normalization
│   │       ├── logical_planner.hpp / .cpp
│   │       ├── planner_normalizer.hpp / .cpp
│   │       ├── decorrelate_dependent_join.hpp / .cpp
│   │       ├── decorrelate_rewrite_utils.hpp / .cpp
│   │       ├── delim_join_cleanup.hpp / .cpp
│   │       └── mark_join_cleanup.hpp / .cpp
│   └── executor/engine/           # Columnar executor + parallel pipeline (C++)
│       ├── core/                  # Fundamental types and data structures
│       │   ├── types.hpp          # Numeric helpers, Wide128, scale handling
│       │   ├── data_chunk.hpp     # DataChunk<2048> template + DeformProgram
│       │   ├── data_chunk_deform.hpp/.cpp  # Tuple deform: PG HeapTuple → DataChunk
│       │   ├── memory.hpp         # PgMemoryContextAllocator, PgVector, PgUnorderedMap
│       │   ├── hash_table_defs.hpp # Hash join partition structures
│       │   ├── parallel_dsa_bridge.hpp/.cpp  # DSA/DSM helpers
│       │   ├── robin_hood_pg_adapter.hpp
│       │   ├── pg_mcxt_guard.hpp
│       │   └── status.hpp
│       ├── expr/                  # Vectorized expression evaluation
│       │   ├── expr.hpp           # VecExprProgram, VecExprStep, JIT interface
│       │   ├── expr.cpp           # Interpreter
│       │   └── vector_operations.hpp
│       ├── hash_table.hpp         # Bloom filter + linear probe HT (volvec)
│       ├── llvmjit_deform_datachunk.cpp/.h  # LLVM JIT tuple deform
│       ├── llvmjit_expr.cpp       # LLVM JIT expression compilation
│       ├── yaap_engine.hpp        # Central engine header (includes core + expr)
│       └── parallel/pipeline/     # Parallel pipeline runtime
│           ├── physical_operator.hpp/.cpp   # PhysicalOperator base class
│           ├── translator.hpp/.cpp          # Legacy PG-plan translator (not the supported path)
│           ├── translator_shape.cpp         # Shape matching
│           ├── translator_expr.cpp          # Expression lowering
│           ├── translator_filter.cpp        # Filter lowering
│           ├── translator_layout.cpp        # Column layout assignment
│           ├── translator_internal.hpp
│           ├── yaap_pipeline_init.hpp/.cpp  # YAAP physical plan -> pipeline init/lowering entry
│           ├── pipeline.hpp / pipeline_leader.hpp/.cpp
│           ├── task_scheduler.hpp/.cpp / task.hpp/.cpp
│           ├── task.cpp
│           ├── pipeline_worker_main.cpp     # BG worker entry point
│           ├── pipeline_descriptor.hpp/.cpp
│           ├── pipeline_run_event.hpp/.cpp
│           ├── pipeline_combine_event.hpp/.cpp
│           ├── pipeline_finalize_event.hpp/.cpp
│           ├── event.hpp/.cpp
│           ├── meta_pipeline.hpp/.cpp
│           ├── operator.hpp / source.hpp / sink.hpp
│           ├── types.hpp / cancel.hpp
│           ├── physical_seq_scan.hpp/.cpp
│           ├── physical_hash_join.hpp/.cpp
│           ├── physical_hash_aggregate.hpp/.cpp
│           ├── physical_perfect_hash_aggregate.hpp/.cpp
│           ├── physical_order.hpp/.cpp
│           ├── physical_projection.hpp/.cpp
│           ├── aggregate_hash_table.hpp/.cpp
│           ├── columnar_filter.hpp/.cpp
│           ├── output_sink.hpp/.cpp
│           ├── query_state.hpp              # PgYaapQueryState C++ definition
│           ├── runtime_dsm.hpp/.cpp
│           ├── dsm_control.hpp / dsm_task_queue.hpp/.cpp
│           ├── pipeline_dsm_lookup.hpp
│           ├── tuple_data_layout.hpp/.cpp
│           ├── tuple_data_collection.hpp/.cpp
│           ├── tuple_data_ops.hpp/.cpp
│           ├── pipeline_profile.hpp/.cpp
│           └── AGENTS.md
```

---

## Build & Test

```bash
# From PostgreSQL source root
meson setup build --prefix=/path/to/installed --reconfigure
meson compile -C build contrib/pg_yaap
meson install -C build --only-changed

# Run regression test
meson test -C build pg_yaap
```

Build requires C++17, `-O3 -march=native -ftree-vectorize -funroll-loops -ffast-math`. LLVM is optional; the `llvmjit_*.cpp` files are only compiled when `llvm.found()` is true.

---

## Development Rules

1. **No silent fallback.** If the optimizer or executor can't handle a query, raise `ereport(ERROR)`. Do not add fallback-to-PG logic in new code paths.
2. **NUMERIC is scale-2 int64 by default.** Accept that results may differ from PG's exact decimal arithmetic. This is intentional.
3. **PG MemoryContext discipline.** All per-query allocations must go through `state->context` or its children. Use `PgVector`/`PgUnorderedMap`/`AllocatePgShared` for C++ containers. Never use bare `new`/`malloc` for query-lifetime objects.
4. **C/C++ boundary.** C files live only in `src/bridge/`. All engine code is C++ (`.cpp`/`.hpp`). The bridge uses `extern "C"` wrappers to expose entry points.
5. **Hook ordering.** `ExecutorStart_hook` calls previous first (PG state must exist). `ExecutorRun_hook` tries YAAP first, falls through to previous only when no state is registered.
6. **Worker bypass.** `ExecutorStart_hook` must check `IsParallelWorker()` and return early — workers participate via the pipeline runtime, not PG's executor hooks.
7. **Avoid `elog(ERROR)` inside engine calls without `PG_TRY/PG_CATCH` around the C++ destructor scope.** DSA/JIT contexts and MemoryContext-owned objects leak otherwise.
