# pg_yaap Execution Trace (Layer 1)

`pg_yaap` includes a lightweight, in-process **Execution Trace** subsystem designed to observe macro-level multi-process query execution in [Perfetto UI](https://ui.perfetto.dev/) without OS-level profiling privileges (such as `dtrace`/`xctrace` on macOS or `perf` on Linux).

---

## Architecture

```text
Leader                Worker 0          Worker 1          ...
  |                      |                 |
  v                      v                 v
Trace Buffer          Trace Buffer      Trace Buffer   (DSA, cache-line isolated)
  |                      |                 |
  +----------------------+-----------------+
                         |  (collected after all workers reach BGWH_STOPPED)
                         v
                  Trace Collector
                         |  (64 KiB streaming writer, O(1) memory)
                         v
                Chrome Trace JSON
                         |
                         v
                    Perfetto UI
```

### Key Design Decisions

1. **Per-Process, Single-Writer, Lock-Free Append-Only Buffers**:
   - The leader allocates one `YaapTraceBufferHeader` (cache-line aligned) and one fixed-capacity `YaapTraceEvent[]` array per participating process (Slot `0` = Leader, Slots `1..N` = Workers `0..N-1`) inside the query's existing DSA area (`PipelineSharedControl::trace_root`).
   - Allocation uses `DSA_ALLOC_ZERO | DSA_ALLOC_NO_OOM` with a hard 64 MB / half-DSA-limit safety cap. If DSA memory is insufficient, tracing degrades gracefully (`WARNING` + disabled) without failing the SQL query.
   - Each process resolves its buffer pointers once on attach (`YaapTraceAttachProcess`) into process-local storage (`g_yaap_trace_local`). Hot-path writes perform zero locks, zero CAS, zero heap allocations, zero string formatting, and zero file I/O.
   - When a buffer reaches capacity, new events are dropped and `dropped_count` is incremented; the collector emits a `WARNING` and marks `"complete": false`.

2. **Cross-Process Time & Memory Synchronization**:
   - Time is captured exclusively via `YaapTraceNow()` / `YaapTraceCaptureTime()`, backed by `clock_gettime(CLOCK_MONOTONIC, &ts)` in `uint64_t` nanoseconds (supported on both macOS and Linux).
   - The leader captures `origin_ns` once during `YaapTraceInitQuery` and publishes it before launching workers; every recorded event stores `timestamp_ns` relative to `origin_ns`.
   - Every event append executes a release memory barrier (`pg_write_barrier()`) before advancing `header->event_count`, guaranteeing that even if a worker crashes or is terminated abruptly, the leader only reads completely written event records.
   - The leader collects buffers only **after** `SignalShutdownAndWait` (`WaitForBackgroundWorkerShutdown`) completes for every worker and issues `pg_read_barrier()`, before `DestroyRuntimeDsm` releases shared memory.

3. **Compact POD Event Model (`40 bytes`)**:
   - `YaapTraceEvent` stores only fixed-width integers (`timestamp_ns`, `duration_ns`, `event_type`, `category`, `pipeline_id`, `task_id`, `arg`) — no strings and no process-private pointers.
   - Exported traces never contain SQL text, relation names, constants, or tuple payloads.

4. **Near-Zero Disabled Overhead**:
   - `YaapTraceIsEnabled()` checks `g_yaap_trace_local != nullptr` (one branch-predicted pointer test).
   - When `pg_yaap.trace_enabled = off` (default), no trace buffer is allocated, no clock is read, no events are recorded, and no file I/O occurs.

5. **Layer 2 Extensibility (Resource Accounting)**:
   - `YaapTraceTimePoint`, `YaapTraceScope`, `YaapTraceBegin`/`YaapTraceEnd`, `YaapTraceCategory`, and `YaapTraceBufferHeader::format_version` are structured so future Layer 2 metrics (`cpu_time_ns`, `off_cpu_time_ns`, I/O bytes, context switches) can be captured inside `YaapTraceBegin`/`YaapTraceEnd` without rewriting executor instrumentation call sites.

---

## GUC Configuration

| GUC | Type | Default | Context | Description |
|---|---|---|---|---|
| `pg_yaap.trace_enabled` | `bool` | `off` | `PGC_USERSET` | Enable or disable Execution Trace for the session. |
| `pg_yaap.trace_buffer_mb` | `int` (MB) | `1MB` (`1`–`16`) | `PGC_USERSET` | Per-process trace buffer size (each worker and the leader gets its own buffer; total per-query trace memory is bounded at 64 MB and <= 50% of `dsa_limit_bytes`). |
| `pg_yaap.trace_output_dir` | `string` | `""` | `PGC_SUSET` | Absolute server-side directory where trace JSON files are written. Must not contain `..`. Export also checks `superuser()` or `pg_write_server_files` membership at runtime. |

---

## Usage

### 1. Enable Tracing in a PostgreSQL Session

```sql
-- Superuser configures the server output directory (or set in postgresql.conf)
SET pg_yaap.trace_output_dir = '/tmp/yaap_traces';

-- Enable execution trace for this session
SET pg_yaap.trace_enabled = on;
SET pg_yaap.trace_buffer_mb = 2;

-- Run any OLAP query
SELECT l_returnflag, l_linestatus, sum(l_extendedprice * (1 - l_discount)), count(*)
FROM lineitem
WHERE l_shipdate <= date '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

After the query completes, `pg_yaap` prints a `NOTICE` with the generated file path and separate query vs. export timings:

```text
NOTICE:  pg_yaap[trace] file=/tmp/yaap_traces/yaap_trace_41384_177742926577665_51465639397000.json status=complete events=173 dropped=0 query_ms=51.556 export_ms=0.117
```

Files are created exclusively (`O_CREAT | O_EXCL`, mode `0600`) with unique names (`yaap_trace_<leader_pid>_<execution_id>_<origin_ns>.json`) so concurrent sessions never collide.

### 2. Import into Perfetto UI

1. Open [https://ui.perfetto.dev/](https://ui.perfetto.dev/) in your browser.
2. Click **"Open trace file"** in the left sidebar (or drag-and-drop the `.json` file onto the page).
3. Inspect the tracks in deterministic order:
   - **YAAP Pipeline Scheduler**: One track per pipeline (`Pipeline N: <SOURCE> -> <SINK>`) showing `Pipeline Ready`, `Pipeline Schedule`, `Event Schedule`, `Event Finish`, and `Pipeline Finish` markers.
   - **YAAP Leader (pid ...)**:
     - `Query`: `Query Execute` and `Output Emit` intervals (excluding JSON export time).
     - `Leader Wait`: `Leader Wait Workers Ready` and `Leader Wait Event DAG` intervals.
   - **YAAP Worker 0..N-1 (pid ...)**:
     - `Execution`: `Worker Start`, `Pipeline P / RUN|COMBINE|FINALIZE / Task T` (`ph: "X"`), and `Worker Exit`.
     - `Idle`: `Worker Idle` intervals spent waiting for ready tasks.
     - `Scheduler`: `Enqueue` and `Dequeue` instant markers.

---

## Recorded Event Types

| Event | Phase | Track | Description |
|---|---|---|---|
| `QUERY_EXECUTE` | `X` | Leader / Query | Overall query execution (excludes JSON export). |
| `OUTPUT_EMIT` | `X` | Leader / Query | Final `OutputSink` tuple emission to client. |
| `LEADER_WAIT` | `X` | Leader / Leader Wait | Leader waiting for worker startup or Event DAG completion. |
| `WORKER_START` | `i` | Worker / Execution | Worker attached and initialized. |
| `WORKER_EXIT` | `i` | Worker / Execution | Worker exiting loop (`abnormal: true/false`). |
| `WORKER_IDLE` | `X` | Worker / Idle | Worker blocked in `WaitLatch` waiting for tasks. |
| `PIPELINE_READY` | `i` | Scheduler / Pipeline N | Pipeline dependencies satisfied. |
| `PIPELINE_SCHEDULE` | `i` | Scheduler / Pipeline N | Pipeline `RUN` task cohort scheduled. |
| `PIPELINE_FINISH` | `i` | Scheduler / Pipeline N | Pipeline `FINALIZE` completed. |
| `EVENT_SCHEDULE` | `i` | Scheduler / Pipeline N | Event DAG node (`RUN`/`COMBINE`/`FINALIZE`) activated. |
| `EVENT_FINISH` | `i` | Scheduler / Pipeline N | Event DAG node finished. |
| `TASK_ENQUEUE` | `i` | Worker / Scheduler | `TaskDescriptor` pushed to `DsmTaskQueue`. |
| `TASK_DEQUEUE` | `i` | Worker / Scheduler | `TaskDescriptor` popped from `DsmTaskQueue`. |
| `TASK_EXECUTE` | `X` | Worker / Execution | Actual execution interval of one task (`RUN`/`COMBINE`/`FINALIZE`). |

---

## Error & Abnormal Exit Semantics

- **Query cancellation / SQL error**: Leader shuts down workers, collects whatever events were published before the abort, exports a trace marked `"complete": false`, and re-throws the original PostgreSQL error untouched.
- **Abrupt worker termination (`SIGKILL` / `pg_terminate_backend`)**: Because workers publish events with a write barrier before advancing `event_count`, all events recorded prior to termination remain intact. The leader detects the non-`FINALIZED` buffer state, increments `abnormal_processes`, and marks `"complete": false`.
- **Trace export failure**: Caught internally and downgraded to a `WARNING`; never fails a completed SQL query or masks a query error.
