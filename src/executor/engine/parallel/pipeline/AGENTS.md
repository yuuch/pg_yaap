# pipeline/ — DuckDB-Style PhysicalOperator + MetaPipeline Runtime

**Refreshed:** 2026-11-26.

The **only** parallel runtime in the codebase. Replaces the deleted legacy `parallel_runtime.cpp` + `runtime_*.cpp/.inc` family. The current execution, synchronization and limitation contract is documented in `docs/pipeline_runtime.md` at the extension root. Historical milestones such as `q10_milestone.md` do not describe all current operators.

## OVERVIEW

DuckDB-faithful `PhysicalOperator` tree (unified `Source/Operator/Sink` base, `physical_operator.hpp`) is built by the YAAP optimizer, then lowered by `yaap_opt_translator` into the pipeline runtime. The tree is sliced at blocking operators (`HashAggregate`, `Order`) into `MetaPipeline` chains via `PhysicalOperator::BuildPipelines`. Each pipeline has RUN, COMBINE and FINALIZE events. The elected scheduling Worker expands them into descriptors in one global DSM ready queue. Any idle PostgreSQL bgworker may claim a whole task and execute it to completion. Logical `task_index` identifies task-owned partial state independently of physical `worker_index`. Cross-process state lives in DSA, published via `PipelineSharedControl.event_states_root` and `pipelines_root`, addressed by id through `pipeline_descriptor.cpp` Store/LoadSharedPayload + `pipeline_dsm_lookup.hpp`.

**Status:** the runtime executes optimizer-owned plans with cross-process descriptors and shared operator payloads. Logical RUN tasks own their partial-result slots. The leader drains materialized output after all events complete and workers stop.

## MODULE STATUS

### Shipped — runtime infrastructure

| Module | Notes |
|--------|-------|
| `types.hpp` | `PipelineChunk = DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>` (2048 rows), `LEADER_WORKER_INDEX = -1`, `ExecCtx`. |
| `dsm_control.hpp` | `PipelineSharedControl` (DSM-resident), `PIPELINE_DSM_KEY_{CONTROL=…0001,DSA=…0008,TASK_QUEUE=…0009,…000A}`, magic `0x56505043`, `worker_error` (NONE/WRITING/READY) and a 512-byte diagnostic buffer. |
| `pipeline.hpp` | Pipeline metadata struct (id, source, sink, depends_on). |
| `execution_slots.hpp` | Logical RUN partial slots come from `ExecCtx.task_index`; physical `worker_index` remains independent for process roles/profiling. Cohort storage is sized from the ready worker count. |
| `operator.hpp`, `source.hpp`, `sink.hpp` | Result enums (`SourceResultType`, `OperatorResultType`, `SinkResultType`, `SinkCombineResultType`, `SinkFinalizeType`). Do not reorder. |
| `dsm_task_queue.{hpp,cpp}` | One global bounded ready queue at `PIPELINE_DSM_KEY_TASK_QUEUE`; arbitrary idle workers claim with cursor CAS, protected by 64-bit cell sequences and publication/reclamation barriers. Dynamic PID/Latch registry. `TaskKind { RUN=1, COMBINE=2, FINALIZE=3 }`. |
| `worker_error.hpp` | CAS claims WRITING; complete diagnostics publish READY with a barrier; readers acquire READY before accessing text. |
| `physical_operator.{hpp,cpp}` | `BuildPipelines` slices the tree at sinks; `FinishLocalSink` seals/publishes private sink state before a whole RUN completes. `GetCombineTaskPlan`/`GetFinalizeTaskPlan` expose operator-owned stage partitioning and concurrency. |
| `meta_pipeline.{hpp,cpp}` | `MetaPipeline` chain. Q1 produces 3 pipelines: P0=[Order→Output], P1=[HashAgg→Order], P2=[SeqScan→HashAgg]. |
| `pipeline_descriptor.{hpp,cpp}` | Leader serialize + Worker reconstruct, `Store/LoadSharedPayloadOnDescriptor` (one `dsa_allocate0` per payload — never share allocations across payloads). Operator-id keyed lookup is the **only safe cross-process publish channel** for sinks; per-process operator fields cannot serve as the source of truth. |
| `pipeline_dsm_lookup.hpp` | Template id → object resolver against the descriptor. |
| `query_state.hpp` | Opaque `void* parallel_plan` / `parallel_scheduler` layout shared with `bridge/state.c`. |
| `task_scheduler.{hpp,cpp}` | Validates and stores the Event DAG in DSA. Workers elect a scheduling pass owner to activate phases, publish tasks and release successor edges. Per-event `tasks_in_flight`/`max_concurrency` constrain publication without process binding. |
| `pipeline_leader.cpp` | Leader: builds DSM/DSA/descriptors, allocates event state, registers PIDs, launches and monitors workers, and drains finalized materialized output. |
| `pipeline_worker_main.cpp` | bgworker entry: DSM/DSA attach, snapshot restore, descriptor reconstruction, scheduling election, global task claim, execution, completion counts and error publication. |
| `task.cpp` | A claimed RUN owns fresh local state and drives source→operators→sink through completion and `FinishLocalSink`. Separately claimed combine/finalize tasks resolve shared state even when the claimant never ran that pipeline. |
| `translator.cpp` (+ `translator_{expr,filter,layout,shape}.cpp`) | Legacy PostgreSQL-plan translation. The supported optimizer-owned path uses `yaap_pipeline_init` and `yaap_opt_translator*`. |

### Shipped — operators

| Module | Notes |
|--------|-------|
| `physical_seq_scan.{cpp,hpp}` | Page-wise scan with shared state initialized at RUN activation; `heap_prepare_pagescan` removed (Bug C-pre). `AppendProjectedTupleToChunk` real body. |
| `physical_hash_aggregate.{cpp,hpp}` | Real `Sink/Combine/Finalize/GetGlobalSinkState/GetGlobalSourceState/GetData`. Uses DSA-authoritative shared state; `GetCombineTaskPlan` schedules partition owners that consume all published producer partials. |
| `physical_order.{cpp,hpp}` | Single-executor sort (`MaxThreads=1`) using PostgreSQL tuplesort with existing spill support; load shared payload before allocating. |
| `physical_projection.{cpp,hpp}` | Pure projection operator (no sink). |
| `output_sink.{hpp,cpp}` | DSA TDC sink. `EmitGlobalTdcToDest` drains finalized global TDC into `DestReceiver`. |

Additional operator:

| Module | Notes |
|--------|-------|
| `physical_hash_join.{cpp,hpp}` | Partitioned/spillable hash join. Segmented producer writers seal/publish in RUN-tail `FinishLocalSink`, so their COMBINE stage has zero tasks; flat partitioned builds retain shared-registry COMBINE after the RUN barrier (`LWLock` + `PG_TRY`). Probe may emit multiple chunks per input. |
| `physical_top_n.{cpp,hpp}` | Task-local Top-N candidate tracking for `max_rows <= 4096` merged once in `FinishLocalSink` under `LWLock` + `PG_TRY`; larger bounds use `LWLock`-guarded global updates. |
| `physical_cross_product.{cpp,hpp}` | `LWLock` + `PG_TRY`-guarded shared build combine in `FinishLocalSink` (both segmented and flat paths). |
| `physical_delim_scan.{cpp,hpp}` | `PhysicalMaterialize` spills/reloads under soft-limit pressure and retires chains once `remaining_consumers` reaches zero at consumer RUN completion. |

### Shared tuple storage and hash table

| Module | Notes |
|--------|-------|
| `tuple_data_layout.{cpp,hpp}` | Row layout: column offsets, NUMERIC scale, aggregate metadata. |
| `tuple_data_collection.{cpp,hpp}` | DSA-allocated row collection (`finalized` flag is authoritative, not the per-process `*GlobalSourceState::finalized` snapshot). |
| `tuple_data_ops.{cpp,hpp}` | Row pack / unpack / hash. |
| `aggregate_hash_table.{cpp,hpp}` | Row-store aggregate hash-table support; current hash aggregation schedules partition-owned combines. Historical deviations are recorded in `docs/ARCHITECTURE_DEVIATIONS.md`. |

## EXECUTION FLOW

1. **Bridge** (`bridge/execute.cpp`): lower `OptimizerPlanBundle::physical_plan` through `yaap_opt_translator` → pipeline root (stored as opaque `void*` in query state). On `ExecutorRun`, dispatch to `pipeline::PgYaapPipelineRun(qd, qstate, &reason)`.
2. **Leader** (`pipeline_leader.cpp`):
   - Allocate DSM with `PIPELINE_DSM_MAGIC`; register keys `_CONTROL`, `_DSA`, `_TASK_QUEUE`.
   - Initialize `PipelineSharedControl`; `dsa_create_in_place` at `_DSA`.
   - `BuildPipelines` to slice operator tree into `MetaPipeline` (Q1 → 3 pipelines).
   - `pipeline_descriptor.cpp` serializes per-pipeline payloads (schema, layouts, qual) into DSA, publishes `pipelines_root`.
   - Build and validate the shared Event DAG before worker launch.
   - Launch up to `pg_yaap.parallel_max_workers` workers, then publish the actual ready cohort and graph start flag.
   - Monitor for errors and query completion. Join workers before draining the shared result through `OutputSink::EmitGlobalTdcToDest`.
   - Single cleanup path on PG_CATCH; PostmasterDeath shutdown via `BGWH_STOPPED` poll + `WaitLatch`.
3. **Worker** (`pipeline_worker_main.cpp`):
   - `dsm_attach` + `shm_toc_lookup` for `_CONTROL`, `_DSA`, `_TASK_QUEUE`; `dsa_attach` to leader area.
   - Reconstruct descriptor IR and register local Pipeline objects before reporting ready.
   - Loop: claim an arbitrary whole task with `DsmTaskQueue::TryPop`, execute to completion, then release in-flight quota and decrement remaining tasks. RUN-tail `FinishLocalSink` publishes private state before completion; COMBINE and FINALIZE resolve shared state independently of the RUN process.
   - On `ereport(ERROR)`, claim WRITING, format task/error details, publish READY with a barrier, then request shutdown and wake the leader before re-raising.
   - The elected scheduler initializes runnable stages, processes completed RUN cleanup and releases successor dependencies. It wakes the leader when no events remain.
   - An empty queue is not query completion. Wait for new work, and exit on explicit shutdown/error/leader or postmaster death.

## CROSS-PROCESS PAYLOAD INVARIANT

Resolve shared payload handles through the current descriptor. The elected
scheduler sets `ExecCtx::initialize_shared_state` during activation. Only
that caller may initialize a missing handle. Publish and clear through all
descriptor aliases for the operator. After the Event publication barrier a
task attaches the payload using its own process-local wrapper.

## CONVENTIONS

 - **Supported shapes:** the runtime only needs to support shapes emitted by the YAAP optimizer and admitted by `yaap_opt_translator`. Legacy PG-plan translation is not the supported architecture.
 - **Column identity:** optimizer-path lowering should treat `ColumnBinding` plus operator output dictionaries as authoritative. `varno/attno` may remain as compatibility metadata, but must not drive semantic resolution on the optimizer path.
- **Process/task identity**: physical `worker_index` is `-1` for the leader or `0..N-1` for bgworkers. Logical `task_index` identifies a task and its RUN partial slot; it is never a destination worker.
- **Chunk size**: `PipelineChunk = DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>` (2048 rows).
- **DSM keys**: `PIPELINE_DSM_KEY_*` in `0xD8…` range. Always go through `dsm_control.hpp` constants.
- **DSA allocation**: one `dsa_allocate0` per payload. Never share an allocation across payloads. Never publish palloc'd pointers.
- **Atomics**: use queue barriers for descriptor publication and reclamation. The elected Worker acquires task completion before releasing resources and dependencies.
- **Whole-task scheduling**: one global shared ready queue; any idle executor claims any published task and runs it to completion. Never preassign task indices to processes or recreate per-process queues. Fresh RUN local state must prevent one process's successive tasks from overwriting each other's published partials.
- **Publication quotas**: `tasks_in_flight` includes queued, claimed and executing tasks. `max_concurrency` gates dispatch for the event, independently of worker identity.
- **Hybrid cohorts**: hybrid participant indices are logical RUN task indices. Preserve fixed cohort and admission barriers. The scheduling pass releases its election before the claimant runs a task. Other workers can continue publishing the cohort.
- **Runtime limits**: workers spawn per query; non-parallel sources cap RUN at one; final results are materialized in DSA and drained by the leader. No worker pool or streaming delivery.
- **EventId convention**: `pid*3 + {0,1,2}` (one slot per event kind per pid).
- **Sink finishing and combine**: finish process-local writers/contributions in RUN-tail `FinishLocalSink` before publishing completion. Segmented hash join and cross product then have zero-task COMBINE stages. Post-barrier hash aggregation/perfect-hash/flat-partitioned-join COMBINE tasks consume all shared task partials and may run in any process.
- **`*GlobalSourceState::finalized` is a stale snapshot.** Read `global_tdc->finalized` from DSA (Bug E).
- **MemoryContext discipline**: each worker creates its own per-query `MemoryContext`; pipeline operators run under `ExecCtx::mcxt`.
- **Error propagation**: use `worker_error.hpp`; CAS NONE→WRITING claims the diagnostic buffer and a barrier publishes READY. Read text only through an acquire of READY; never inspect WRITING contents.
- **Sort scope (current)**: `MaxThreads=1`; existing tuplesort/spill behavior remains in scope. No parallel sort; do not widen sort execution without a separate design.
- **Cleanup discipline**: single PG_CATCH-owned cleanup path in `pipeline_leader.cpp`. `SignalShutdownAndWait` only on success; `ShutdownAndDestroy` everywhere else (Bug C).

## ANTI-PATTERNS

- **Do NOT change existing `PhysicalOperator` execution virtual signatures.** Use the current `SinkTaskPlan` hooks for stage planning.
- **Do NOT add `AttachGlobal*State` virtuals** or an `ExecutionAffinity` enum. Both unnecessary.
 - **Do NOT add query-specific hacks.** Prefer reusable operator/type support; keep admission conservative and explicit.
- **Do NOT extend Sort beyond its current single-executor (`MaxThreads=1`) implementation.** Existing tuplesort/spill support does not imply parallel sort.
- Do not wait for queue capacity while holding the scheduling election. Leave the logical task index pending in its event.
- **Do NOT publish palloc'd pointers via `PipelineSharedControl` or DSA.**
- **Do NOT cache `shared_payload_dp_` as cross-process state.** Always Load from descriptor first (Bug H).
- **Do NOT publish a COMBINE task that depends on another process's private local sink.** Put private finishing in `FinishLocalSink`; later stages resolve task-owned partials from shared registries.
- **Do NOT trust `*GlobalSourceState::finalized`.** Read DSA-resident `global_tdc->finalized` (Bug E).
- **Do NOT skip populating `worker_error{,_msg}` before `ereport(ERROR)`** in a worker.
- **Do NOT reorder result enums** (`SourceResultType`, `OperatorResultType`, `SinkResultType`, `SinkCombineResultType`, `SinkFinalizeType`).
- **Do NOT introduce a `BLOCKED` task state** in this milestone.
- **Do NOT return `BLOCKED` from any path**; assert + `ereport(ERROR)` if observed.
- **Do NOT implement design doc `§8.7` as written** (per `GLOBAL_LOCAL_STATE_DESIGN.md` line 1428).
- **Do NOT introduce `shm_mq` / `TupleQueueReader` for result delivery.** DSA TDC drain via `EmitGlobalTdcToDest` is the design.
- **Do NOT reintroduce deleted modules:** `pipeline_lowering.{hpp,cpp}`, `executor.{hpp,cpp}` (the pipeline-driver one), `seq_scan_source.*`, `filter_op.*`, `partial_agg_op.*`, `agg_sink.*`, `pipeline_worker_state.*`, `pipeline_worker_context.*`, `worker_context.hpp`, `LoweredPipeline`, `WorkerPipelineExecutor`, `q1_translator.*`, `ParallelAggPartialState`, `ParallelPipelineRole/Desc/Driver/Sink`.

## NOTES

 - **Resumable output contract:** `task.cpp` drains an operator suffix for each source chunk; operators that return `HAVE_MORE_OUTPUT` must be re-enterable on the same logical input until they return `NEED_MORE_INPUT`.
- **Deviations from `pg_duckdb_architecture.md`** are recorded in `contrib/pg_yaap/docs/ARCHITECTURE_DEVIATIONS.md`. Read it before designing new sinks.
- **bundle.pipelines order:** P0=[Order→Output], P1=[HashAgg→Order], P2=[SeqScan→HashAgg].
- **`task_scheduler.cpp`** expands RUN/COMBINE/FINALIZE plans using the event phase; sink-specific partitioning and budget formulas belong to operators.
- LSP may report `'utils/errcodes.h' file not found` on `dsm_control.hpp` — workspace include-path quirk, not a real error.
- Trace via `pg_yaap.trace_execution_path=on`.
