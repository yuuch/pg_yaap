# bridge/ — PostgreSQL Hook Integration & Plan Dispatch

**Refreshed:** 2026-05-12.

6 files (5 source + this AGENTS.md). This is the **only** bridge layer: PostgreSQL `ExecutorStart/Run/Finish/End` hooks → `pg_yaap` admission → executor/optimizer C++ entrypoints → `PhysicalOperator` tree → `pipeline::PgYaapPipelineRun`. `src/executor` and `src/optimizer` are implementation subtrees; C files stay here at the outer boundary.

## OVERVIEW

`pg_yaap` does NOT replace PostgreSQL's planner. The outer bridge owns hook chaining, admission, per-query state, and planner/executor glue. Executor and optimizer internals stay below this layer as C++ implementation code. Per-query state lives in a backend-local HTAB keyed by `QueryDesc *`.

The bridge does **NO slot materialization** as of step 2. Result emission from `DataChunk` to `TupleTableSlot` to `DestReceiver` lives inside the pipeline runtime's `OutputSink` (see `parallel/pipeline/output_sink.{hpp,cpp}`). The bridge owns admission, plan ownership transfer, and dispatch — nothing else.

## STRUCTURE

| File | Lang | Purpose |
|------|------|---------|
| `pg_yaap.c` | C | `_PG_init`/`_PG_fini`, GUC registration, hook chaining (`ExecutorStart/Run/Finish/End`), pretty-printer for `pg_yaap_plan_node_name`. |
| `state.h` | C / C++ | Public C interface for the per-query state HTAB and the `pg_yaap_try_build_query_state` / `pg_yaap_close_query_state` helpers. Forward-declares opaque `PgYaapQueryState`. |
| `state.c` | C | Defines `struct PgYaapQueryState { MemoryContext context; void *parallel_plan; void *parallel_scheduler; }`, the HTAB, `plan_uses_supported_relations` admission filter, query-state alloc/dispose. |
| `execute.h` | C / C++ | Declarations for `pg_yaap_initialize_plan`, `pg_yaap_delete_plan`, `pg_yaap_execute_query`. |
| `execute.cpp` | C++ | Bridge from `OptimizerPlanBundle::physical_plan` into runtime lowering/dispatch, owns the resulting pipeline root via `state->parallel_plan`, and dispatches `pg_yaap_execute_query` to `pipeline::PgYaapPipelineRun`. |
| `AGENTS.md` | doc | This file. |

## WHERE TO LOOK

| Task | File | Notes |
|------|------|-------|
| `_PG_init`, hook registration, GUC table | `pg_yaap.c` | All registered GUCs defined here. |
| Admission filter | `state.c::plan_uses_supported_relations` | Recursive walk; rejects when no `RTE_RELATION` user table is touched. SubqueryScan supported via recursion. |
| Per-query state HTAB | `state.c` (impl), `state.h` (API) | HTAB keyed by `QueryDesc *`; entries hold `PgYaapQueryState *`. |
| Query state lifecycle | `state.c::pg_yaap_try_build_query_state`, `pg_yaap_close_query_state` | Allocates child `MemoryContext` named `"pg_yaap query context"`. |
| Plan init / dispatch | `execute.cpp::pg_yaap_initialize_plan`, `pg_yaap_execute_query` | Lowers the optimizer bundle into the YAAP runtime. `parallel_scheduler` is set to a static char sentinel iff `pg_yaap.parallel=on`. |
| Plan teardown | `execute.cpp::pg_yaap_delete_plan` | `static_cast<PhysicalOperator*>(state->parallel_plan)` → `delete`. Called from `pg_yaap_close_query_state` in `state.c`. |
| Pipeline dispatch | `execute.cpp::pg_yaap_execute_query` | Calls `pg_yaap::pipeline::PgYaapPipelineRun(queryDesc, state, &failure_reason)`. On failure/unsupported shape it falls through to `standard_ExecutorRun`. |

## CONVENTIONS

- **C/C++ split**: `pg_yaap.c` and `state.c` are pure C and live only in `src/bridge`. `execute.cpp` is the outer C++ glue that talks to executor/optimizer C++ code. `src/executor` / `src/optimizer` keep implementation code, not PG hook C entrypoints.
- **Naming**: all C-visible symbols use `pg_yaap_` prefix. C++ entry uses `namespace pg_yaap` (and `pg_yaap::pipeline` for runtime types).
- **Hook discipline**: each hook saves the previous handler at `_PG_init` time. The chain order in this codebase is **previous-first** for `ExecutorStart` (PG state must exist before we admit), and **previous-as-fallback** for `ExecutorRun` (we try `pg_yaap` first, fall through to previous on `false`). `_PG_fini` restores all four pointers.
- **Worker bypass**: `ExecutorStart_hook` early-outs via `IsParallelWorker()`. Workers participate in the pipeline runtime via the worker bgworker entry (`pipeline_worker_main.cpp`), not via PG's executor hooks.
- **Per-query state**: created in `ExecutorStart_hook` after `IsParallelWorker()` check, looked up by `QueryDesc *` pointer in `ExecutorRun_hook`, destroyed in `ExecutorEnd_hook`. The HTAB lives in `TopMemoryContext`; per-query data lives in a `state->context` child `MemoryContext` (`"pg_yaap query context"`) that is destroyed by `pg_yaap_close_query_state`.
- **Admission**: `plan_uses_supported_relations` walks the plan tree before init. If it rejects, `pg_yaap_try_build_query_state` returns `NULL`, no state is registered, and `ExecutorRun_hook` finds no entry → fallthrough to standard PG executor.
- **Plan-init failure**: if `pg_yaap_initialize_plan` returns `false` (Translator returned `nullptr`, or `pg_yaap.parallel=off`, or `parallel_plan==NULL`), the just-allocated state is closed via `pg_yaap_close_query_state` and is never registered — same fallthrough path.
- **Parallel sentinel**: `state->parallel_plan` holds a real `PhysicalOperator *` cast to `void *`. `state->parallel_scheduler` is set to the address of a static char (`pgyaap_parallel_scheduler_sentinel` in `execute.cpp`) iff `pg_yaap.parallel=on`. Both being non-null gates the dispatch path. The real `TaskScheduler` lives inside `parallel/pipeline/` (not behind this sentinel) and is constructed per-query from `PgYaapPipelineRun`.
- **`MemoryContextSwitchTo` discipline**: `pg_yaap_initialize_plan` switches into `state->context` before invoking optimizer-path lowering so any palloc-backed allocations the engine makes are anchored to the per-query lifetime.

## GUCs (defined in `pg_yaap.c::_PG_init`)

9 GUCs are registered. All are `PGC_USERSET`.

| GUC | Default | Range | Purpose |
|-----|---------|-------|---------|
| `pg_yaap.enabled` | `true` | bool | Master switch; off → all hooks short-circuit before admission. |
| `pg_yaap.trace_hooks` | `false` | bool | `LOG`-level dump of every hook entry/exit and admission decision. |
| `pg_yaap.trace_execution_path` | `false` | bool | One `LOG` line per query describing dispatch path (`pipeline` vs `native_pg`) and reason. |
| `pg_yaap.jit_deform` | `true` | bool | Enable LLVM tuple-deform JIT. Consumed inside the engine, not the bridge. |
| `pg_yaap.parallel` | `false` | bool | Enable pipeline runtime dispatch. **Required `on` for any pg_yaap execution today** (see `pg_yaap_initialize_plan`: returns `false` when off). |
| `pg_yaap.parallel_max_workers` | `4` | 0–1024 | Max bgworkers per query. |
| `pg_yaap.parallel_min_relation_blocks` | `1024` | 0–`INT_MAX` | Min relation size before parallel lowering is considered. |
| `pg_yaap.parallel_leader_participation` | `true` | bool | Leader runs pipeline tasks alongside workers. |
| `pg_yaap.parallel_experimental_hash_pipeline` | `false` | bool | Reserved; **no effect in greenfield** (HashJoin removed). Kept registered to avoid breaking existing config files. |
| `pg_yaap.profile` | `false` | bool | Emit per-stage pipeline timing after each offloaded query. Fine-grained scan stages add visible overhead; use for diagnosis, not benchmark timing. |

`pg_yaap.parallel_morsel_nblocks` is no longer registered. The active SeqScan
path uses an atomic block pool consumed by each worker's local read stream
callback, not a tunable block-range morsel scheduler.

### Dead-code GUC variable

`pg_yaap.c:29` declares `bool pg_yaap_disable_jit_for_parallel_worker = false;` but **never registers it** with `DefineCustom*Variable` and nothing reads it. This is a leftover from the pre-greenfield era. Slated for removal at the next bridge cleanup pass.

## ANTI-PATTERNS

- **Do NOT call standard executor + then `pg_yaap` for the same `QueryDesc`.** Dispatch is exclusive: either the bridge owns execution for this `QueryDesc` (hook returns after `pg_yaap_execute_query` returns `true`) or it passes through (hook calls `prev_*` / `standard_*`).
- **Do NOT allocate per-query memory in `TopMemoryContext`.** Use `state->context` (the per-query child context); it is destroyed on `pg_yaap_close_query_state`.
- **Do NOT skip `IsParallelWorker()` early-out in `ExecutorStart_hook`.** PG bgworkers reach hooks too; double-dispatch into the pipeline runtime would corrupt state.
- **Do NOT `elog(ERROR)` from inside a hook without `PG_TRY/PG_CATCH` around the engine call.** C++ destructors must run; otherwise DSA/JIT contexts and the per-query MemoryContext leak.
- **Do NOT touch `parallel_plan` / `parallel_scheduler` outside `bridge/execute.cpp` and `parallel/pipeline/`.** They are opaque from every other site.
- **Do NOT add new GUCs without `DefineCustom*Variable` in `_PG_init`** and without documenting them in this file.
- **Do NOT chain hooks in the wrong order.** Standard pattern: save previous in `_PG_init`, install ours, call previous (or `standard_*` if previous is `NULL`) at the established point. See existing wrappers as the reference.
- **Do NOT reintroduce slot materialization here.** `OutputSink` owns chunk → slot → `DestReceiver` materialization. Bridge stays minimal.
- **Do NOT widen the admission filter to non-relation plans** without a corresponding Translator path, or every fallthrough query starts paying admission overhead for nothing.

## EXECUTION FLOW

1. **`_PG_init`** (`pg_yaap.c`):
   - Defines all 10 registered GUCs.
   - `pg_yaap_init_state_table()` creates the `QueryDesc *`-keyed HTAB.
   - Saves the previous four hook pointers and installs `pg_yaap_ExecutorStart/Run/Finish/End`.
2. **`pg_yaap_ExecutorStart`**:
   - **Calls previous (or `standard_ExecutorStart`) FIRST** so PG `EState`/snapshot is built.
   - If `!pg_yaap_enabled` → return.
   - Optional `LOG` dump of pid / parallel-worker state / planTree node name.
   - If `IsParallelWorker()` → return.
   - `pg_yaap_try_build_query_state(qd, eflags)` → if `NULL`, return.
   - `pg_yaap_initialize_plan(qd, state)`:
     - `MemoryContextSwitchTo(state->context)`.
     - Lower `OptimizerPlanBundle::physical_plan` through the YAAP optimizer-to-executor path.
     - On non-null: `state->parallel_plan = root.release()`; if `pg_yaap_parallel`, `state->parallel_scheduler = &sentinel`.
     - On `parallel_plan==NULL`: emit `WARNING: pg_yaap: unsupported plan shape, falling back to standard PostgreSQL executor` and return `false`.
     - On `pg_yaap_parallel==false`: emit `WARNING: pg_yaap: pg_yaap.parallel=off, falling back to standard PostgreSQL executor` and return `parallel_scheduler != NULL` (i.e. `false`).
   - On `true`: `pg_yaap_register_state(qd, state)`. On `false`: `pg_yaap_close_query_state(state)` (which calls `pg_yaap_delete_plan` and destroys `state->context`).
3. **`pg_yaap_ExecutorRun`**:
   - `pg_yaap_lookup_state(qd)` → if non-`NULL`:
     - Patch `estate->es_snapshot = GetActiveSnapshot()` if missing.
      - `pg_yaap_execute_query(qd, state, direction, count)`:
        - Gate on `state->parallel_plan && state->parallel_scheduler` (both non-null).
        - `pg_yaap::pipeline::PgYaapPipelineRun(qd, state, &failure_reason)`.
       - On `true` → log `pg_yaap_path: path=pipeline detail=yaap_pipeline` (when traced) and return `true`.
       - On `false` → log skip reason (when traced) and return `false`.
     - On `true` from `pg_yaap_execute_query` → return (we own this query).
     - On `false` → log fallback path and fall through.
   - Call previous (or `standard_ExecutorRun`).
4. **`pg_yaap_ExecutorFinish`**: call previous (or `standard_ExecutorFinish`). No bridge-side work today.
5. **`pg_yaap_ExecutorEnd`**:
   - `pg_yaap_lookup_state(qd)` → if non-`NULL`, `pg_yaap_close_query_state(state)`.
   - `pg_yaap_unregister_state(qd)`.
   - Call previous (or `standard_ExecutorEnd`).

## NOTES

- `PgYaapQueryState` (defined in `state.c`, opaque elsewhere) carries exactly three fields: `MemoryContext context`, `void *parallel_plan` (real `PhysicalOperator *`), `void *parallel_scheduler` (currently a static-char sentinel). Real `TaskScheduler` is owned per-query inside `parallel/pipeline/PgYaapPipelineRun` and is not exposed through this sentinel.
- HTAB key is the `QueryDesc *` pointer itself (`HASH_BLOBS`). Relies on PG not reusing the pointer across overlapping queries in the same backend; nested executors get distinct `QueryDesc`, so this is safe.
- `pg_yaap_initialize_plan` always emits a `WARNING` on the fallthrough path. This is intentional (M-FRAME-MIN sanity test relies on the WARNING text). Once the runtime lands, demote to `LOG`-when-traced.
- Admission is **broad on purpose**: the bridge does not gatekeep on plan shape — the Translator does. Today the Translator returns `nullptr` for everything, so `parallel_plan==NULL` is the universal fallthrough trigger.
- `pg_yaap_plan_node_name` in `pg_yaap.c` is a debug helper for `trace_hooks`. It still names `HashJoin`/`MergeJoin`/etc. that have no Translator support; harmless because the strings are only used for logging.
- The `parallel_experimental_hash_pipeline` GUC is a leftover from the pre-greenfield era; HashJoin is removed and this GUC currently has no effect. Kept registered to avoid breaking existing config files.
- All admission failures must produce a clean fallback to PostgreSQL execution — **never** `ereport(ERROR)` for "unsupported plan".
