# PostgreSQL scan cost attribution

`pg_yaap.profile_scan` extends the existing Pipeline/Worker profiler. It does
not change execution, scheduling, PG visibility rules, or the optimizer.

```sql
SET pg_yaap.profile = off;
SET pg_yaap.profile_scan = sample;       -- off (default), sample, exact
SET pg_yaap.profile_timeline = basic;    -- task/worker intervals
SELECT ...;
```

Scan profiling also works with both older profilers off. With
`pg_yaap.profile_scan=off`, the existing `pg_yaap.profile` behavior is unchanged.
When both are enabled, the new scan measurements replace the old fine-grained
scan timers; other legacy stage measurements remain enabled.

## Read the report

`pg_yaap[scan]` prints worker, pipeline and query aggregates. Pipeline labels
identify the source table. `(pipeline, operator, relation)` distinguishes scans
of the same table; operator is the descriptor index within that pipeline.
Worker records include the OS PID. Unknown operator indices use UINT32_MAX.

| Field | Meaning |
|---|---|
| `source_ms` | Source elapsed work: local scan initialization plus completed GetData calls |
| `pg_access_ms` | Buffer API calls, page/MVCC preparation, HeapTuple iteration |
| `row_to_column_ms` | Qual-column and output-column decoding, including type conversion and string copying |
| `predicate_ms` | Scan predicate computation; this is query processing |
| `setup_ms` | Local relation/heap scan/ReadStream initialization, descriptor bindings, deformer/JIT setup |
| `pg_input_ms` | PG access + row-to-column + setup |
| `other_ms` | Positive residual within the measured source; an estimate in Sample mode |
| `overestimate_ms` | Amount by which classified estimates exceed source; never silently hidden |
| `pages` | Valid buffer pages obtained, including repeated scans; not physical disk reads |
| `visible_rows` | Exact number of visible tuples visited |
| `output_rows` | Exact number of rows emitted after the scan filter |
| `qual_rows`, `proj_rows` | Actual decoding passes; these are not distinct input-row counts |
| `samples`, `qual_samples`, `proj_samples` | Observed sample counts; zero means unknown cost, not free work |

`load_ms`, `prepare_ms`, `tuple_ms`, `qual_ms`, and `proj_ms` expose the
corresponding lower-level components. `pg_access_ms` is not purely disk I/O.
`load_ms` includes PostgreSQL's public `read_stream_next_buffer` call, which
may return a shared-buffer hit or wait inside the native ReadStream. Its
internal phases are not visible to the extension, and I/O worker background
execution is not all charged to this scanner.

Primary scan times are **elapsed worker-time sums, not CPU time or query wall time**.
The native ReadStream path does not expose separate submit/wait CPU or elapsed
timers to YAAP. See `docs/yaap_read_stream.md` for its integration boundary.
Multiple workers and pipelines overlap. In particular, never calculate a
hypothetical columnar query latency by subtracting aggregate PG-input time from
query wall time. The source is a parent of its categories: do not add it to
those categories again.

With Timeline enabled, `pg_yaap[scan-task]` also prints RUN active time and
`outside_scan_ms`. The latter is RUN minus measured source work and still
includes initialization outside the source, waits, profiling overhead, and
other runtime work. It is not a pure compute counter. Combine and Finalize
remain separate existing timeline phases. Internal hash/aggregate/sort spill
and reload remain engine work, not PG table input.

## Sample versus Exact

Sample measures all GetData calls, pages and counts, but times each tuple with
probability 1/64 using a per-worker xorshift sequence. Timed tuple components are
expanded by 64 after subtracting the worker's median block-average empty-clock-bracket cost.
The calibration is only an approximation: cache effects, branch effects,
clock resolution, descheduling and rare slow rows can still distort estimates.
`sample_clock_overhead_ns` is meaningful only in Worker lines, not aggregate lines.
Small inputs can have no samples. The report explicitly labels Sample as
estimated and exposes overestimation rather than rescaling categories to fit.

Exact times each tuple stage. It is useful for diagnosis and can substantially
slow scanning; its own timer overhead remains part of the observed execution.
Profiler overhead is measured with Off/Sample/Exact query benchmarks, not
inferred by subtracting stage counters. Metadata lookup is cached per local
scanner; calibration and final publication costs are outside `source_ms`.
Global source initialization before task dispatch and final scan destruction
are not separately attributed to the input categories.

## Perfetto / Chrome trace

```sql
SET pg_yaap.profile_timeline = trace;
SET pg_yaap.profile_scan = sample;
SET pg_yaap.profile_trace_file = '/absolute/writable/path/query';
SELECT ...;
```

The existing exporter produces a uniquely named JSON file with server-side
permission checks. Open it at `ui.perfetto.dev`. Worker summary JSON objects and
Task Execute events have an optional `scan_cost` object with nanosecond values,
row/sample counts, operator/relation IDs, mode and `tuple_times_estimated`.
These are costs accumulated inside the task fragment, **not contiguous child
intervals**. Worker RUN totals match sums of retained task fragments when trace
capacity is not exhausted. No tuple or DataChunk trace events are generated.
Existing JSON fields and schema version remain compatible.

## Memory and abnormal execution

Scan counters use independent worker-exclusive DSA slots, aligned and strided
to PG_CACHE_LINE_SIZE. They are published once per completed GetData, not on
each tuple; no per-tuple lock or shared allocator is used. Storage is capped at
16MiB independently of Timeline's 16MiB cap. Allocation failure disables scan
profiling with a warning instead of failing the query. Trace capacity is still
bounded, with existing dropped-event reporting.

A changed PID in an occupied counter slot is rejected and reported incomplete.
On query cancellation/worker failure, the existing shutdown synchronization
precedes reporting. A PostgreSQL ERROR longjmp can skip the last GetData's
publication; reports say `complete=no`, and partially published values must not
be treated as a full-query cost. Report errors do not replace the query error.

## Verification

`tests/scan_profile.py` reuses the timeline harness for modes, legacy
compatibility, exact row counts, independent repeated scans, strings/NUMERIC,
JIT toggles, empty/rejected input, fragment-to-worker JSON conservation, trace
overflow, cancellation and worker termination. It compares supported fixture
results with native PostgreSQL. Existing NUMERIC NULL-aggregate differences
are outside this profiler change; the numeric aggregate fixture uses non-null
values. Do not interpret the profiler tests as a fix for those semantics.

Benchmark records and exported traces are under
`artifacts/scan-profiler-20261023/`; the verification report contains measured
optimized-build overhead and actual example output.
