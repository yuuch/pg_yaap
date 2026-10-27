# Extension-only Scan I/O profiler

This profiler observes the existing PG and YAAP read paths. It does not change
block allocation, prefetch policy, request limits, Heap visibility, PG Buffer
Manager, I/O workers, or the planner. The tested server is PostgreSQL 18.3 on
macOS. No PG core source changes, private-layout mirrors, symbol interception,
extra Buffer pins or extra Wait calls are used.

## Run

```sql
SET pg_yaap.scan_read_stream = yaap;
SET pg_yaap.profile = off;
SET pg_yaap.profile_scan = off;
SET pg_yaap.profile_scan_io = summary; -- off (default), summary, detail
SELECT ...;
```

`summary` reports one query summary and ranked hypotheses. `detail` additionally
reports each Worker/Pipeline, configurations, API timing components and finite
histograms. The old profilers remain independent. IO-only mode does not enable
per-tuple timing.

To export structured observations and diagnoses through the existing secure
Timeline exporter:

```sql
SET pg_yaap.profile_scan_io = detail;
SET pg_yaap.profile_timeline = trace;
SET pg_yaap.profile_trace_file = '/absolute/writable/prefix';
SELECT ...;
```

The JSON adds `scan_io` objects to Worker, Pipeline and Query summaries.
Existing fields/schema are retained. It stores histogram buckets, not page
traces. Open the generated JSON in Perfetto. File permissions/path privileges
are still checked by the existing exporter.

Rules expose `pg_yaap.profile_scan_io_wait_pct` (default 20) and
`pg_yaap.profile_scan_io_min_pages` (default 128). Other thresholds are explicit
in `ScanIODiagnosisConfig`. Thresholds only surface hypotheses; they never
classify a page as ready, a call as blocking or a fallback as queue-full.

## Metrics and boundaries

| Quantity | Availability | Source and semantics |
|---|---|---|
| Consumed pages | EXACT | One per valid Next return; EOF is not a page |
| Initial cache ready/miss/already-loading split | UNAVAILABLE | PG exposes no safe observer of all initial lookup outcomes here |
| PG-accounted hit rate | EXACT counter ratio | Backend `pgBufferUsage` deltas inside GetData; not initial lookup classification |
| Known-ready at need | EXACT lower-bound population | At Next entry, before refill, its submitted operation has no pending Wait: Start returned ready earlier or our prior successful Wait validated it |
| Readiness unknown | EXACT population | Pending or not yet accepted at need; never relabelled as not-ready |
| Strict Prefetch Ready / Cold Ready | UNAVAILABLE | Unknown state and initial cache origin cannot be resolved through this contract |
| Start-return-to-need lead | EXACT API-clock observation | Per consumed page, associated with accepted operation; not actual internal enqueue time |
| Need-to-Start-return | EXACT API-clock observation | Separate population where that API return followed need; not a physical late-I/O count |
| Validated-ready observation | OBSERVED upper bound after API return | Successful consumption Wait return minus Start return; not true Submit→Complete latency |
| True completion / queue / service latency | UNAVAILABLE | No true completion/enqueue/dequeue timestamps in these public interfaces |
| Submitted lookahead | EXACT queue count | Accepted, unconsumed pages including head; no array scan needed |
| Reserved depth | EXACT reservation count | Can include unsubmitted/forwarded suffixes; not actual PG pin ownership |
| UnWaited request count | EXACT own references | Requests requiring consumption Wait; may already have completed; not disk concurrency |
| Configured ring/pin/share caps | EXACT snapshots | Distinct configuration values; derived ring equality does not establish ring pressure |
| Pin binding | EXACT branch events | Positive wanted pages constrained by public additional-pin allowance |
| Window-full observations | EXACT co-binding observations | Refilling stopped with reservation at page capacity; can coexist with slot/quota stop and is not independent loss time |
| Own request/slot/burst stops | EXACT branch events | Explicit local reasons; not PG AIO queue capacity |
| Stream creation | EXACT | Per instrumented source object; task re-entry preserves it |
| Reset count | 0, no reset API | Current custom stream has no reset entry point; separate creations are not reset events |
| Accepted-operation size | EXACT logical distribution | Includes cached Start calls and split continuations; not physical device request sizes |
| Need-Wait accepted-operation size | EXACT logical distribution | Removes immediate-ready cohort, but still does not prove physical request count/bytes |
| Start / Wait API times | EXACT elapsed envelopes | Include PG-private lookup, pin, synchronization, retry and possible synchronous execution |
| Actual IO wait/stall and sync read time/count | UNAVAILABLE | Cannot split those private intervals safely from an API envelope |
| PG Queue Full / forced-sync cause counts | UNAVAILABLE | Never inferred from `f_sync`, high latency, cache misses or thread CPU |
| Repeated page loads / buffer pressure | UNAVAILABLE without scoped evidence | No unbounded relation/block history is retained |
| SSD bandwidth limit | UNAVAILABLE without external reference | PG read counters may hit OS cache; no device-limit inference from logical GB/s |

Known-ready plus unknown equals consumed. First pages of combined operations
can have unknown readiness and carry a large Wait envelope, while the remaining
pages are known ready after validation. Consequently even a high ready lower
bound does **not** mean little waiting. Cold readiness remains unavailable.

The validation observation is measured after Start returns. A PG request might
already have executed synchronously inside Start; the observation is therefore
not even a bound on total internal Submit→Complete latency. It is a safe-ready
observation interval relative to the API return. Independent page-lead and
request-validation P95s cannot establish causality or compute Stall.

All main costs are cumulative elapsed Worker times, including descheduling,
not CPU or Query Wall Time. `wait_api_envelope_pct` divides consumption-Wait
elapsed by the same source scope's local-init/GetData elapsed. It is explicitly
not `io_stall_ratio`. Existing sampled thread CPU remains a distinct optional
observation, never a measure of device service time.

The mutually exclusive Next accounting is:

```text
Next total
  StartReadBuffers API envelope
  WaitReadBuffers API envelope
  Refill/lookahead exclusive of Start
  Other exclusive residual
```

Buffer lookup, actual AIO submission, true wait and synchronous execution inside
PG APIs cannot be split further. They remain unclassified within those API
envelopes, with unavailable strict fields. Do not add overlapping metadata or
unclassified API explanations to the four categories again.

## Histograms and publication

Fixed 64-bin logarithmic histograms retain bounded memory. Quantiles are bucket
upper bounds clipped to the observed maximum, not exact percentiles. Zero
observations/saturation return unavailable quantiles. Worker buckets are merged
before calculating query quantiles; percentiles are never averaged.

Only an enabled custom stream allocates queue-lifetime operation observations.
They associate accepted pages with one stable request index and timestamp;
metadata is reused when slots wrap. No long-lived per-page event log exists.
Counters/histograms publish deltas once per GetData using the existing
`read_stream_published` pattern and a separate IO snapshot. Shared records are
Worker-exclusive/cache-line aligned and capped at 16MiB independently of the
existing profilers. A publishing guard rejects torn records after termination.
PID reuse is marked incomplete rather than combined.

PostgreSQL ERROR can skip the final publication. Error reports mark partial
scope and do not claim complete-query ratios. No metric labelled unavailable
is rendered as a numeric zero. Normal early LIMIT counts only consumed pages;
accepted future operations may exceed consumption. End cleanup Wait is outside
the consumption validation population and not reported as actual Stall.

PG native ReadStream supports only the common source/PG counter observations;
custom queue metrics are explicitly unavailable for that mode.

## Diagnostic rules

`ScanIODiagnose` is a pure bounded evaluator, independent of PG and scanning.
It returns `CONFIRMED`, `SUSPECTED`, or `INSUFFICIENT_DATA`, with evidence, numeric
names, an experiment and an explicit sorting-time semantic. An API envelope
used as priority is marked estimated; it is not attributed lost Worker time.

Supported codes cover prefetch submission timing, pin/share policy, own request
quota, reset overhead, sync/queue-full, buffer pressure, latency, request size,
storage bandwidth, Buffer Manager overhead and insufficient/inconsistent data.
Some require trusted future/external evidence and intentionally cannot fire as
confirmed from current extension-only signals.

A static 8192→1306 clamp is configuration evidence, not automatically a pin
bottleneck. A backend-share hypothesis also requires frequent full-window
observations, near-cap depth, meaningful Wait envelope and incomplete proven
readiness; it remains suspected and names the YAAP policy. Slot/window/quota
conditions can coexist. Low cache hit, a reused ring, a high independent P95,
`io_method=sync`, or many cached single-page calls alone never prove the
corresponding bottleneck.

See [interface audit](scan_io_observability.md) and
[rule contract](scan_io_diagnosis_rules.md) for PG18 source references and tests.
For device evidence, optionally collect macOS `iostat -d -w 1` outside the hot
path; discard the since-boot first row, note other workloads, and establish a
separate sustained-bandwidth reference. `pg_aios` is a privileged transient
snapshot; its `f_sync` hint is not a queue-full reason or reliable fallback
count. This profiler does not query it per page.

## Validation

`meson test -C build-bench pg_yaap_scan_io_rules` runs portable rule/histogram
tests. `tests/scan_io_profile.py` reuses the existing timeline harness for
availability, 1/4/8/12-worker conservation, combined IO, empty/filtered/early
stop, legacy/native modes, overflow, cancellation and termination.

`artifacts/scan-io-profiler-20261023/` contains hot/cold-Shared Buffer, demand-only,
low backend-share, sync-config and IO-worker-count scenarios, Q1 benchmarks,
Perfetto JSON, raw iostat and the final measurement report. Cold here means PG
Shared Buffers cold; OS cache was not purged. Exact Cold Ready, true Stall and
kernel-level sync injection cannot be validated without additional supported
observability and are explicitly not claimed.

## Detailed buffer-path evidence (2026-10-24)

`profile_scan_io=detail` additionally exports:

- `start_api_ns` and `wait_api_ns` fixed logarithmic histograms. Their
  percentiles are bucket upper bounds for API elapsed times, not storage latency.
- `block_assignment_ns`: elapsed time assembling contiguous batches via the
  existing parallel block allocator. `pin_budget_check_ns`: elapsed time querying
  the backend's additional pin allowance. Both are **subsets** of
  `refill_exclusive_ns`; do not add them to it. Timing is per batch, not tuple.
- `submit_cpu_samples`, `submit_sample_wall_ns`, `submit_sample_cpu_ns`, and
  corresponding `wait_*` fields: independently sampled (~1/64 API calls) thread
  CPU and elapsed time. No extrapolated sync-read count or total CPU time is
  claimed. `cpu_clock_failures` and zero samples mean no usable CPU evidence.
  Sample elapsed minus CPU includes descheduling, locks and IO; it is not pure
  IO stall. Summary mode does not emit the detail report lines.

Human output uses `[scan-io-cpu]`, `[scan-io-refill-detail]` and
`[scan-io-hist]`. Worker, pipeline and query JSON merge additive sample sums and
histogram bins. Profiling off keeps clocks disabled. No scan behavior changes.

For external stack evidence, see [scan_io_stacks.md](scan_io_stacks.md). This
separate opt-in tool can establish that a YAAP worker was sampled beneath
`pgaio_worker_submit -> pgaio_io_perform_synchronously -> pread/preadv`, or that
an IO worker was executing `preadv` while a compute worker waited. It preserves
raw stacks and checks ancestry rather than unrelated symbol presence. Missing
samples do not prove absence; sampled reads may be served by the OS cache.
PG internal queue-full counts and exact fallback durations remain unavailable.

### Ring interpretation

BAS_BULKREAD is a reuse strategy over shared_buffers, not separate page memory.
`GetAccessStrategyWithSize` caps ring buffers to NBuffers/8. BAS_BULKREAD's
strategy pin limit equals its ring size. YAAP chooses its window using a
per-query/per-source shared-buffer share and backend pin share first, creates a
ring of that size, then clamps the queue to the strategy's pin allowance.
Consequently ring and window can have equal reported capacities because YAAP
selected them together; equality alone does not identify an undersized ring.
Already pinned buffers are protected against ordinary eviction. Normal ring
reuse and `GetVictimBuffer` stacks do not prove harmful replacement pressure.
Reliable internal buffer lookup/replacement exclusive times require kernel
instrumentation and are intentionally UNAVAILABLE here.


## Scoped PostgreSQL shared-read time

The IO profiler snapshots public `pgBufferUsage.shared_blk_read_time` around
`StartReadBuffers` and `WaitReadBuffers`, without adding a PG wait or probing
private AIO structures. `pg_start_read_ns` and `pg_wait_read_ns` are exact **PG
counter deltas within those API calls**, not CPU samples or pure IO stall.
`[scan-io-pg-time]` displays them in milliseconds in summary and detail modes.
JSON metric source is `POSTGRES_STATISTICS`, semantic
`PG_ACCOUNTED_READ_TIME_WITHIN_API`, counter `shared_blk_read_time`.

The corresponding `pg_start_timed_calls` / `pg_wait_timed_calls` count calls for
which `track_io_timing` was enabled on a non-temporary relation. A metric is
`EXACT` only for a complete profile with at least one corresponding API call
and timing enabled for **every** call. Disabled timing, no calls, temporary
relations, incomplete publication or mixed coverage yields `UNAVAILABLE` /
`null`, never an apparently measured zero. Additive raw counters remain in
`observed`; partial sums must be interpreted alongside the timed-call counts.

In PG18 `WaitReadBuffers` records the `pgaio_wref_wait` bracket into this counter
when the operation is not yet known complete (`bufmgr.c` around 1692–1704).
However, Wait can also retry `AsyncReadBuffers`, whose submission/synchronous
execution may add read time. `pg_wait_new_read_pages` is the scoped delta of
`pgBufferUsage.shared_blks_read` within Wait, exposing new read operations during
that call (e.g. retry/partial-read continuation). It is **not** a queue-full or
physical SSD counter. It is collected regardless of `track_io_timing`.
Do not rename the scoped time to `actual_io_wait_ns` or subtract it from query
wall time as though it were independent work. It can include scheduling delays
inside PG's bracket, and unavailable internal reasons remain unavailable.
Shared and local buffer counters are distinct; these time metrics intentionally
exclude temporary-relation local-buffer time.

The integration validator checks counter/time bounds, availability and additive
Worker-to-Pipeline-to-Query merging. On a dedicated server started with
`track_io_timing=off`, `YAAP_TEST_EXPECT_PG_TIMING=off` additionally verifies that
all scoped-time metrics remain unavailable; changing only the leader session's
GUC does not establish the configuration of independently launched workers.


## Optional patched-PG request timing

A compatible diagnostic PG build exports `YAAP_PG_AIO_TIMING_VERSION`,
`READ_BUFFERS_PROFILE_TIMING`, and `ReadBuffersGetTiming()`. YAAP opts into that
interface only with `profile_scan_io=detail`. Stock PG headers still build:
`kernel_timing` is explicitly `UNAVAILABLE`, and no private AIO handle is read.
Patched PG and this extension must be rebuilt against matching installed headers;
this is not an ABI-compatible plug-in for an unpatched server binary.

After a successful logical `WaitReadBuffers`, YAAP copies the stable operation
summary exactly once into the existing worker-local counters. The core summary
accumulates all physical reads, including partial-read retries. Fields prefixed
`kernel_` report request counts, requested/completed syscall bytes, successful
queue entries, worker execution, exact queue-full synchronous fallback and other
forced synchronous paths, plus cumulative queue/reopen/read/shared-completion
and enqueue-to-shared-ready durations. Syscall bytes and time include OS page
cache behavior and must not be described as physical SSD bytes or latency.
`kernel_read_ns` measures the actual read syscall envelope; `kernel_queue_ns`
uses the core monotonic enqueue/dequeue boundaries, without verbose logging.
These are sums of request durations, not query wall time.

Human detail output adds `[scan-io-kernel]`. JSON `metrics.kernel_timing` carries
availability, source, feature version and operation coverage. It is `EXACT` only
for a complete detail profile with every completed Wait operation profiled,
every physical request timed, and no failed/unclassified operations. Otherwise
its value is `null`, even though raw partial counters remain in `observed`.
No requests is a valid measured zero in a supported complete detail scope;
means and throughput still require nonzero bytes/request counts. Summary mode
and disabled profiling do not enable the kernel instrumentation.

The fixed histograms `kernel_operation_queue_ns` and
`kernel_operation_read_ns` have **one observation per completed logical read
operation containing physical reads**. An operation with retries contributes
its sum across those requests. They are not per-physical-request latency
histograms. Physical-request means can be calculated from cumulative sums and
physical counts; neither histogram quantiles nor individual-worker rates should
be blindly multiplied by concurrency to predict query throughput.

### Page consumer interval

Detail mode also records `page_consumer_gap_ns` and `page_consumer_gap_count`:
the elapsed interval between returning a valid Buffer from `YaapReadStreamNext`
and entering `YaapReadStreamNext` again. Existing entry/exit timestamps are
reused; there is no extra per-page clock or allocation. The matching bounded
histogram is `page_consumer_gap_distribution_ns`, and human output is
`[scan-io-consumer]`.

This interval includes caller-side heap/MVCC/tuple processing, row-to-column
conversion, downstream work between batches, locks, scheduling and any task
reentry gap. It is **not pure CPU or exclusive columnar-engine time**. The first
Next call has no preceding interval; an EOF call can close the last page's
interval. If LIMIT/early teardown prevents another Next call, the final page is
not counted. Divide the sum by the interval count for observed elapsed time per
page interval, and compare with core request sizes/service times while retaining
worker concurrency, cache state and scheduling context. It cannot by itself
prove that compute throughput exceeds device throughput.

## Optional PG shared-buffer allocation profile

A server built with `YAAP_PG_BUFFER_PROFILE_VERSION` can expose the work before
AIO submission through `ReadBuffersGetBufferProfile`. YAAP opts in only for
shared-buffer reads in `profile_scan_io=detail`, using
`READ_BUFFERS_PROFILE_BUFFER`. Stock-header builds and summary/off modes leave
these observations unavailable. This is independent of the AIO completion
handle: the result is owner-local and is collected immediately after every
successful Start call, including cache-ready returns.

`metrics.buffer_profile` reports whether exact count coverage is complete.
`observed.buffer_*` counters merge using the existing incremental worker slots:

- `alloc_calls`, `lookup_hits`, `lookup_misses`: initial shared BufferAlloc calls
  and initial tag lookup outcome. A lookup hit means that a mapping exists; the
  page can still be loading. It is **not** the strict ready-buffer hit metric.
- `ring_attempts`, `ring_hits`: attempted strategy-ring reuse and successful
  reuse; `freelist_candidates`, `freelist_hits`: popped candidates and usable
  free-list buffers; `clock_candidates`, `clock_usage_decrements`,
  `clock_pinned_skips`: Clock Sweep work. A free-list pop is not a linear search
  of the entire buffer array; Clock Sweep is a distinct fallback.
- `victim_retries`, `dirty_writebacks`: replacement retries and FlushBuffer
  attempts; the latter is not a physical-device write count.
- `profiled_start_calls`, `sampled_start_calls`, `sampled_pages`: count coverage
  and timing-sample coverage. Counts cover all opted-in calls; clocks cover
  independently selected approximately 1/64 Start-call cohorts, not every page.

The raw sampled elapsed sums have the same `buffer_` prefix:
`alloc_total_ns`, `lookup_ns`, `mapping_lock_ns`, `victim_ns`, `strategy_ns`,
`ring_ns`, `freelist_ns`, `clock_ns`, `dirty_write_ns`, `invalidate_ns`, `pin_ns`.
They are **sample sums**, not whole-query totals. `metrics.buffer_alloc_sampled_ns`
uses availability `ESTIMATED`, semantic `SAMPLED_COHORT_SUM_NOT_FULL_QUERY_TOTAL`,
and includes the sample rate and counts. A missing sample reports unavailable.
Do not compare a raw sample sum directly with the full Start API envelope.
Offline expansion by the sampling factor is only an estimate and must be
labelled as such; cohorts can have different accepted page counts.

These phases are nested/inclusive. In particular, victim time includes strategy,
pin, dirty writeback and invalidation; strategy time includes ring/free-list/
Clock Sweep selection. Allocation total is a parent envelope. **Do not add
parents to their children**. Mapping-lock time measures acquisition elapsed for initial/new mappings and
old-tag invalidation, not an isolated spin-only CPU cost or lock hold time.
Its old-tag component is also inside invalidation elapsed. All clocks include measurement overhead,
which can be material for sub-microsecond individual phases. Histograms are not
added for these counters, and no per-page log or allocation is introduced.

Human detail output adds `[scan-buffer-alloc]` (exact counts) and
`[scan-buffer-sample]` (raw sampled phase milliseconds and coverage). These
measurements belong to the issuing YAAP worker before it queues the I/O; the
I/O worker already receives selected, pinned destination buffer addresses.
