# pipeline/ — DuckDB-Style PhysicalOperator + MetaPipeline Runtime

**Refreshed:** 2026-05-09.

**~52 files, 22 active translation units.** The **only** parallel runtime in the codebase. Replaces the deleted legacy `parallel_runtime.cpp` + `runtime_*.cpp/.inc` family. Validated slice includes Q1/Q6 smoke plus the join-heavy Q10 path (see `contrib/pg_yaap/q10_milestone.md`).

## OVERVIEW

DuckDB-faithful `PhysicalOperator` tree (unified `Source/Operator/Sink` base, `physical_operator.hpp`) is built by the YAAP optimizer, then lowered by `yaap_opt_translator` into the pipeline runtime. The tree is sliced at blocking operators (`HashAggregate`, `Order`) into `MetaPipeline` chains via `PhysicalOperator::BuildPipelines`. Each pipeline becomes one or more `Task`s (`PipelineRunEvent`, `PipelineCombineEvent`, `PipelineFinalizeEvent`) and is dispatched onto a DSM-resident MPMC `DsmTaskQueue` (Vyukov bounded queue) by `TaskScheduler::EnqueueTasks`. PostgreSQL parallel bgworkers (and the leader) pop tasks and execute them. Cross-process state lives in DSA, published via `PipelineSharedControl.event_states_root` and `pipelines_root`, addressed by id through `pipeline_descriptor.cpp` Store/LoadSharedPayload + `pipeline_dsm_lookup.hpp`.

**Status:** runtime end-to-end **plumbed**; SeqScan → HashAgg → Order → OutputSink runs in leader+workers; descriptor publish/load works cross-process; leader drains the global TDC after FINALIZE.

## MODULE STATUS

### Shipped — runtime infrastructure

| Module | Notes |
|--------|-------|
| `types.hpp` | `PipelineChunk = DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>` (1024 rows), `LEADER_WORKER_INDEX = -1`, `ExecCtx`. |
| `dsm_control.hpp` | `PipelineSharedControl` (DSM-resident), `PIPELINE_DSM_KEY_{CONTROL=…0001,DSA=…0008,TASK_QUEUE=…0009,…000A}`, magic `0x56505043`, `worker_error{,_msg[256]}`. |
| `pipeline.hpp` | Pipeline metadata struct (id, source, sink, depends_on). |
| `operator.hpp`, `source.hpp`, `sink.hpp` | Result enums (`SourceResultType`, `OperatorResultType`, `SinkResultType`, `SinkCombineResultType`, `SinkFinalizeType`). Do not reorder. |
| `dsm_task_queue.{hpp,cpp}` | Vyukov MPMC bounded queue at `PIPELINE_DSM_KEY_TASK_QUEUE`. `TaskKind { RUN=1, COMBINE=2, FINALIZE=3 }`. `RegisterWorkerLatches` for wakeup. |
| `event.{hpp,cpp}` + `pipeline_run_event.{hpp,cpp}`, `pipeline_combine_event.{hpp,cpp}`, `pipeline_finalize_event.{hpp,cpp}` | Atomic dependency machinery. Workers atomic-decrement `EventShmState.tasks_remaining` and `SetLatch` on the leader; only the leader calls `FinishEvent`. `EventId = pid*3 + {0,1,2}`. |
| `physical_operator.{hpp,cpp}` | Base virtual signatures **locked** in `eb7901b022a` (do not change). `BuildPipelines` slices the tree at sinks. |
| `meta_pipeline.{hpp,cpp}` | `MetaPipeline` chain. Q1 produces 3 pipelines: P0=[Order→Output], P1=[HashAgg→Order], P2=[SeqScan→HashAgg]. |
| `pipeline_descriptor.{hpp,cpp}` | Leader serialize + Worker reconstruct, `Store/LoadSharedPayloadOnDescriptor` (one `dsa_allocate0` per payload — never share allocations across payloads). Operator-id keyed lookup is the **only safe cross-process publish channel** for sinks; per-process operator fields cannot serve as the source of truth. |
| `pipeline_dsm_lookup.hpp` | Template id → object resolver against the descriptor. |
| `query_state.hpp` | Opaque `void* parallel_plan` / `parallel_scheduler` layout shared with `bridge/state.c`. |
| `task_scheduler.{hpp,cpp}` | `SchedulerState`, `BindRuntime`, `AllocateEventShmStates` (DSA array via `control->event_states_root`), `EnqueueTasks` dispatching on `Event::kind()`. |
| `pipeline_leader.cpp` | Leader: builds DSM/DSA + descriptor, allocates `EventShmState`, registers latches, launches bgworkers, leader-participates, drives event loop, finalizes, drains global TDC via `EmitGlobalTdcToDest`. Bug A/C landed; race fix at `6c344eb036d`. |
| `pipeline_worker_main.cpp` | bgworker entry: DSM/DSA attach, descriptor reconstruct, `DsmTaskQueue` drain, `Task::Execute`, populate `worker_error{,_msg}` on ereport, atomic-dec + SetLatch. |
| `task.cpp` | `PipelineRunEvent::Execute` drives source→operators→sink loop; `PipelineCombineEvent::Execute` runs sink combine; `PipelineFinalizeEvent::Execute` finalizes. Diagnostic `RUN.GetData ENTER` fprintf still in. |
| `translator.cpp` (+ `translator_{expr,filter,layout,shape}.cpp`) | Plan-to-`PhysicalOperator` translation for the currently admitted shapes. Builds descriptors/layouts and allocates payloads via DSA before operator ctors. |

### Shipped — operators

| Module | Notes |
|--------|-------|
| `physical_seq_scan.{cpp,hpp}` | Page-wise scan with leader self-alloc (Bug B); descriptor fallback in workers; `heap_prepare_pagescan` removed (Bug C-pre). `AppendProjectedTupleToChunk` real body. |
| `physical_hash_aggregate.{cpp,hpp}` | Real `Sink/Combine/Finalize/GetGlobalSinkState/GetGlobalSourceState/GetData`. Uses DSA-authoritative `global_tdc->finalized` gating; Combine runs in every worker (not leader-only). |
| `physical_order.{cpp,hpp}` | Sort `MaxThreads=1`, in-memory single-run. Bug H (Load-before-alloc invariant) landed. Diagnostic `Order.GetGlobalSinkState ENTER`, `Order LEADER ALLOC`, `Order LOAD`, `Order NEITHER-BRANCH`, `Order.Finalize ENTER/EXIT` fprintfs still in. |
| `physical_projection.{cpp,hpp}` | NEW. Pure projection operator (no sink). |
| `output_sink.{hpp,cpp}` | DSA TDC sink. `EmitGlobalTdcToDest` drains finalized global TDC into `DestReceiver`. |

Additional operators used by Q10:

| Module | Notes |
|--------|-------|
| `physical_hash_join.{cpp,hpp}` | Inner equi-hash join. Build side materializes into TDC + bucket/chain directory; probe side can resume mid-bucket and may emit multiple output chunks per input chunk (`HAVE_MORE_OUTPUT`). |

### Shipped — TupleData / hash table (NEW this cycle, ~1561 LoC)

| Module | Notes |
|--------|-------|
| `tuple_data_layout.{cpp,hpp}` | Row layout: column offsets, NUMERIC scale, aggregate metadata. |
| `tuple_data_collection.{cpp,hpp}` | DSA-allocated row collection (`finalized` flag is authoritative, not the per-process `*GlobalSourceState::finalized` snapshot). |
| `tuple_data_ops.{cpp,hpp}` | Row pack / unpack / hash. |
| `aggregate_hash_table.{cpp,hpp}` | Robin-hood-style row-store hash table with `mutex` for cross-worker `Combine` serialization (deviation from pg_duckdb partition-lane design — see `docs/ARCHITECTURE_DEVIATIONS.md`). |

## EXECUTION FLOW

1. **Bridge** (`bridge/execute.cpp`): lower `OptimizerPlanBundle::physical_plan` through `yaap_opt_translator` → pipeline root (stored as opaque `void*` in query state). On `ExecutorRun`, dispatch to `pipeline::PgYaapPipelineRun(qd, qstate, &reason)`.
2. **Leader** (`pipeline_leader.cpp`):
   - Allocate DSM with `PIPELINE_DSM_MAGIC`; register keys `_CONTROL`, `_DSA`, `_TASK_QUEUE`.
   - Initialize `PipelineSharedControl`; `dsa_create_in_place` at `_DSA`.
   - `BuildPipelines` to slice operator tree into `MetaPipeline` (Q1 → 3 pipelines).
   - `pipeline_descriptor.cpp` serializes per-pipeline payloads (schema, layouts, qual) into DSA, publishes `pipelines_root`.
   - Pre-init source+sink for each pipeline in `PipelineRunEvent::Schedule` (deviation: pg_duckdb leader does not participate in RUN — see deviations doc).
   - `TaskScheduler::AllocateEventShmStates` for the per-event `EventShmState { pg_atomic_uint32 tasks_remaining; pg_atomic_uint32 saw_error; }` array, publishes `event_states_root`.
   - `TaskScheduler::EnqueueTasks` dispatches on `Event::kind()` into `DsmTaskQueue`.
   - Launches up to `pg_yaap.parallel_max_workers` bgworkers via `RegisterDynamicBackgroundWorker`.
   - Leader-participates (worker_index `LEADER_WORKER_INDEX = -1`).
   - Drives event loop: `WaitLatch`, on completion atomic-dec triggers, leader calls `FinishEvent` to fan in next event.
   - On all events done: leader calls `final_output->EmitGlobalTdcToDest(leader_rt.exec_ctx)` to drain the global TDC into the `DestReceiver`.
   - Single cleanup path on PG_CATCH; PostmasterDeath shutdown via `BGWH_STOPPED` poll + `WaitLatch`.
3. **Worker** (`pipeline_worker_main.cpp`):
   - `dsm_attach` + `shm_toc_lookup` for `_CONTROL`, `_DSA`, `_TASK_QUEUE`; `dsa_attach` to leader area.
   - Reconstruct descriptor IR via `pipeline_descriptor.cpp` Worker side.
   - Loop: `DsmTaskQueue::TryPopForWorker` → `Task::Execute`.
   - On `ereport(ERROR)`, populate `worker_error{,_msg}` before re-raising.
   - On task completion, atomic-decrement `EventShmState.tasks_remaining`; `SetLatch(leader_latch)` if zero. Workers **never** call `FinishEvent`.
   - Exit cleanly when queue is exhausted and event signals done.

## CROSS-PROCESS PAYLOAD INVARIANT (Bug H lesson, GENERAL RULE)

`PhysicalOperator::shared_payload_dp_` is a **per-process instance field**. Each backend reconstructs its own operator instance via `pipeline_descriptor.cpp`. The field cannot serve as cross-process state.

**Invariant for every Sink (`HashAggregate`, `Order`, future sinks):**

```cpp
SinkXxxGlobalState *
PhysicalXxx::GetGlobalSinkState(ExecCtx &ctx) {
    auto state = ...;

    // 1. ALWAYS Load from descriptor first.
    state->shared_payload_dp = DsaPointerIsValid(shared_payload_dp_) ? shared_payload_dp_
                              : LoadSharedPayloadFromDescriptor(this);

    // 2. Only the leader allocs IF Load returned invalid.
    if (ctx.worker_index == LEADER_WORKER_INDEX
        && !DsaPointerIsValid(state->shared_payload_dp))
    {
        state->shared_payload_dp = dsa_allocate0(ctx.dsa, ...);
        StoreSharedPayloadOnDescriptor(this, state->shared_payload_dp);
    }
    else
    {
        // 3. Worker fallback: Load again after waiting if needed.
        if (!DsaPointerIsValid(state->shared_payload_dp))
            state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this);
    }
    state->global_xxx = static_cast<XxxType *>(dsa_get_address(ctx.dsa, state->shared_payload_dp));
}
```

Same invariant for `GetGlobalSourceState`. **Never** read `shared_payload_dp_` directly without Load fallback.

## CONVENTIONS

 - **Supported shapes:** the runtime only needs to support shapes emitted by the YAAP optimizer and admitted by `yaap_opt_translator`. Legacy PG-plan translation is not the supported architecture.
 - **Column identity:** optimizer-path lowering should treat `ColumnBinding` plus operator output dictionaries as authoritative. `varno/attno` may remain as compatibility metadata, but must not drive semantic resolution on the optimizer path.
- **Worker indexing**: leader is `LEADER_WORKER_INDEX = -1`. Bgworkers are `0..N-1`. Leader participation gated by `pg_yaap.parallel_leader_participation`.
- **Chunk size**: `PipelineChunk = DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>` (1024 rows).
- **DSM keys**: `PIPELINE_DSM_KEY_*` in `0xD8…` range. Always go through `dsm_control.hpp` constants.
- **DSA allocation**: one `dsa_allocate0` per payload. Never share an allocation across payloads. Never publish palloc'd pointers.
- **Atomics**: `EventShmState.tasks_remaining` + `SetLatch(leader_latch)`. Workers **never** call `FinishEvent` — only the leader does.
- **EventId convention**: `pid*3 + {0,1,2}` (one slot per event kind per pid).
- **Combine discipline**: every worker runs its own `Combine` against the GlobalSinkState (Bug F). `local_tdc` lives in backend-private memory; a leader-only Combine drops every worker's partials.
- **`*GlobalSourceState::finalized` is a stale snapshot.** Read `global_tdc->finalized` from DSA (Bug E).
- **MemoryContext discipline**: each worker creates its own per-query `MemoryContext`; pipeline operators run under `ExecCtx::mcxt`.
- **Error propagation**: workers populate `PipelineSharedControl.worker_error` (atomic uint32) **and** `worker_error_msg[PIPELINE_WORKER_ERROR_MSG_LEN]` before erroring.
- **Sort scope (current)**: `MaxThreads=1`, in-memory single-run. No external sort, no parallel sort.
- **Cleanup discipline**: single PG_CATCH-owned cleanup path in `pipeline_leader.cpp`. `SignalShutdownAndWait` only on success; `ShutdownAndDestroy` everywhere else (Bug C).

## ANTI-PATTERNS

- **Do NOT change `PhysicalOperator` base virtual signatures.** Locked in `eb7901b022a`.
- **Do NOT add `AttachGlobal*State` virtuals** or an `ExecutionAffinity` enum. Both unnecessary.
 - **Do NOT add query-specific hacks.** Prefer reusable operator/type support; keep admission conservative and explicit.
- **Do NOT extend Sort beyond MaxThreads=1, in-memory single-run.**
- **Do NOT have workers call `FinishEvent`.** Atomic-dec + `SetLatch` only.
- **Do NOT publish palloc'd pointers via `PipelineSharedControl` or DSA.**
- **Do NOT cache `shared_payload_dp_` as cross-process state.** Always Load from descriptor first (Bug H).
- **Do NOT make `Combine` leader-only.** Per-worker Combine is mandatory (Bug F).
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
- **`task_scheduler.cpp`** dispatches RUN/COMBINE/FINALIZE on `Event::kind()`.
- LSP may report `'utils/errcodes.h' file not found` on `dsm_control.hpp` — workspace include-path quirk, not a real error.
- Trace via `pg_yaap.trace_execution_path=on`.
