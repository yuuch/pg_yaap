# Pipeline × Worker Timeline

The existing `pg_yaap.profile` boolean retains its stage-counter behavior. Timeline profiling is independent:

```sql
SET pg_yaap.profile = off;
SET pg_yaap.profile_timeline = basic; -- off (default), basic, trace
```

Basic reports Pipeline × Worker start, end, active time and execution slice counts. Trace additionally records bounded task intervals in DSM. It does not record tuple or chunk events. `active_ns` is elapsed time inside RUN and COMBINE task execution, including waits; it is not process CPU time. Finalize is recorded separately and excluded from task active time. Summed worker active time is not query wall time.

`task_count` counts actual Execute calls (including repeated calls of a yielded task), plus direct trivial Finalize calls in the separate Finalize counters. RUN and COMBINE contribute to the main pipeline start/end, active time and task count; Finalize never contributes to those fields. The per-worker summary also provides `run_active_ns` and `combine_active_ns`. Rows are intentionally not recounted: the legacy profiler remains the source for operator row counters. A pipeline with no execution has zero timestamps and counters. Query timeline zero is captured just before worker launch, after the pipeline DAG and descriptors are built; `total_ms` excludes SQL planning, earlier initialization, report/export and later DSM teardown, so it differs from client SQL timing.

All timestamps use the PostgreSQL monotonic clock shared by processes (Linux monotonic clock, macOS monotonic clock implementation). One query origin is captured by the leader; exported intervals use that origin. Each lane identifies a worker slot and records its operating system PID separately. The current runtime executes RUN tasks in background workers; leader work can appear as its own lane. A yielded task is immediately re-entered by the current execution loop: repeated task IDs represent separate actual Execute calls, not an assertion that the task was requeued.

The Trace Event `pid` identifies the query leader, grouping one query in Perfetto; `tid` identifies the slot (`0` is leader, `1..N` are workers). `args.os_pid` and the lane name carry the actual OS PID. Worker summaries use `worker_id=-1` for the leader and `0..N-1` for background workers. Logical task IDs are `(event_id << 32) | partition_id`, or the owner slot when there is no partition. They are strings in JSON to avoid JavaScript integer precision loss. Events are sorted by their common relative timestamp at export.

```sql
SET pg_yaap.profile_timeline = trace;
SET pg_yaap.profile_trace_capacity = 8192; -- 1..65536 events per worker
SET pg_yaap.profile_trace_file = '/absolute/existing/directory/q18';
-- Run the query normally.
```

The server writes `q18.<leader_pid>.<query_origin_ns>.json`. The directory must already exist and be writable by the server OS account. The file is created exclusively with mode 0600; existing files are never overwritten. The path setting is SUSET: a superuser or a role granted SET privilege on that parameter can configure it. Export separately requires superuser or membership in `pg_write_server_files`. A failed export issues a warning without changing query results.

Open [Perfetto](https://ui.perfetto.dev), select **Open trace file**, and choose the local JSON. Each PostgreSQL worker appears as a lane. RUN, COMBINE and Finalize are separate intervals; Ready/Finished markers and dependency IDs connect the pipeline lifecycle. Standard Trace Event `ts` and `dur` are microseconds. The additional `yaap` object retains nanosecond summaries. Capacity exhaustion increments `dropped_events`; summaries continue, but detailed events no longer represent all active time. Incomplete task records are explicitly counted after cancellation/error; missing events must not be interpreted as idle time.

Example Trace Event (illustrative):

```json
{"traceEvents":[{"name":"Pipeline 0 / run","cat":"pipeline","ph":"X","pid":1234,"tid":1,"ts":10000,"dur":780000,"args":{"pipeline_id":0,"worker_id":0,"task_id":"1","event_id":0,"partition_id":4294967295,"result":"finished","os_pid":1235}}]}
```

Run integration checks against a dedicated instance, using the same connection environment as other YAAP tests:

```sh
PGHOST=/tmp PGPORT=55432 PGUSER=postgres PGDATABASE=tpch python3 tests/pipeline_timeline.py
# Optional disruptive checks on a dedicated server:
PGHOST=/tmp PGPORT=55432 PGUSER=postgres PGDATABASE=tpch python3 tests/pipeline_timeline.py --abnormal
```

The script compares query results with native PG and checks aggregate timestamps, repeated task slices, dependencies, Finalize separation, capacity overflow, disabled mode, and legacy compatibility. `--abnormal` additionally cancels a query, terminates one YAAP worker, checks available partial exports, and runs a subsequent query. It uses an isolated fixture schema and preserves unrelated DuckDB DDL event trigger state. Artifacts default to `artifacts/pipeline-timeline-tests`; `PSQL`, `YAAP_LIBRARY`, and `YAAP_TEST_OUTPUT` can override paths.

Trace capacity is per process and preallocated. A query-wide 16MiB metadata/trace data budget additionally limits the effective capacity (reported in the output); if metadata alone exceeds it, timeline collection is disabled with a warning. JSON serialization uses temporary leader memory after workers stop. Task intervals include blocking inside Execute and cannot distinguish CPU execution from I/O waits. Scheduler ready time and actual first execution time are distinct; their difference includes scheduling and initialization. Worker crashes may leave incomplete intervals: their unknown durations are not invented or included in active time. If a process dies during a statistics update, that slot's statistics are omitted and the report is partial. Unexpected reuse of a slot by a different PID disables collection for the replacement and marks the report partial; the current runtime does not restart workers. A hard leader/server crash cannot reliably export a report.

## Implementation and lifecycle

- `src/bridge/pg_yaap.c`: three new GUCs, preserving the legacy boolean.
- `pipeline_timeline.hpp/.cpp`: POD DSA storage, common clock, worker-owned counters and bounded traces, report/export.
- `pipeline_profile.hpp/.cpp`, `dsm_control.hpp`, `runtime_dsm.cpp`, engine `meson.build`: extend existing profiler allocation/registration and shared-control publication.
- `task.cpp`: each RUN, COMBINE and FINALIZE Execute call; the existing 32-chunk yield is an independent fragment.
- `task_scheduler.cpp`: also instrument direct trivial Finalize calls.
- `pipeline_run_event.cpp`, `event.cpp`: Ready before initialization/enqueue and Finished after the Finalize event.
- `pipeline_worker_main.cpp`, `pipeline_leader.cpp`: explicit closure on PG errors; join workers before summary, including startup failure and query cancellation. Saved query errors survive diagnostic export failures.
- `tests/pipeline_timeline.py`: integration checks using the existing psql/transactional fixture pattern.

No queue entries, task affinities, chunk budgets, scheduling decisions, optimizers or operators are changed. Output sinks currently cap their own pipeline to one worker even when more workers are configured; the profiler reports the observed participation rather than the configured count. The parallel scan/aggregate pipeline is tested with multiple workers, and hash join's partition Finalize path is tested on workers separately from leader Finalize.

## Measured overhead and example

macOS, optimized `-O3` build, TPC-H SF10, 12 YAAP workers, 8GB query memory, 8GB shared buffers, 6 I/O workers, legacy `pg_yaap.profile=off`. Q1 modes were interleaved in rotating order after two warmups; each mode has six samples:

| Mode | Median Q1 time | Difference from off |
|---|---:|---:|
| off | 917.81ms | — |
| basic | 905.16ms | -1.38% |
| trace, including export | 906.28ms | -1.26% |

These small differences are within run-to-run noise (individual Q1 samples span roughly 884–958ms); the negative differences do not establish a speedup from profiling. No stable slowdown was resolved by this measurement. A single Q18 comparison measured off 18.53s, basic 18.94s, trace 18.38s; these are single-run observations, not precise overhead estimates. Every mode returned the same output. Measurements and raw samples are in `artifacts/pipeline-timeline-benchmark`.

The saved `q18-trace.json` imports into Perfetto v58.2: 3,670 pipeline slices/instants, 12 worker lanes, no parser error/data-loss statistics. `q18-worker-timeline.svg` is a static overview; use Perfetto for interval zoom and task arguments. For example, Q18 Pipeline 5 (INNER probe → SEMI probe input) starts at 9058ms, ends at 14624ms, spans 5566ms with 12 workers and 932 execution fragments. Worker 0's span is about 5559ms but its accumulated active time is 3929ms; intervening time is not incorrectly charged to this pipeline.

Validation includes result comparisons, one pipeline/one worker, configured worker counts versus actual output-sink participation, parallel pipelines, dependencies, repeated logical task IDs, leader and worker Finalize, empty inputs, no-task startup failures, trace overflow, both profilers together, cancellation, worker termination, query reuse, permissions, and export failure. Linux runtime execution and a hard-killed leader have not been tested on this macOS host.

## Partition-pair admission waits

Hash join probe workers distinguish exhausted input from a pending partition
that temporarily does not fit the admission budget. Waiting workers retain their
Task and retry outside the partition lock, checking cancellation each time.
Their timeline active time includes this wait; it is not CPU time. With the
legacy `pg_yaap.profile=on`, `hash_join_pair_wait` reports the accumulated sleep
intervals separately. Increasing participation alone does not prove more useful
parallel work; compare this wait stage and completed query time as well.

## Hash join stage detail

The legacy stage profiler separates `hash_join_build_append` from
`task_run_total`; build sink time is included in the outer Task duration but
is no longer accumulated into the same stage twice. For paired probe Finalize,
`hash_join_partition_spill` measures the build-segment traversal and Spill path,
and `hash_join_partition_working_set` measures WorkingBytes estimation.
These are nested inside Finalize and must not be added to its total.

## Hybrid hash join

With partition probe enabled, the executor now streams resident partitions and
drains deferred rows at source EOF in the same RUN. See [Hybrid Hash Join](hybrid_hash_join.md)
for the lifecycle and counters. Initial hash construction and EOF waits appear
inside RUN, not as separate pipeline FINALIZE events. The historical overhead
measurements above predate this execution change.
