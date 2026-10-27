# YAAP's bounded PG-buffer read queue

The optional queue replaces PG ReadStream's look-ahead policy, not PG storage.
It calls `StartReadBuffers`, `WaitReadBuffers` and `ReleaseBuffer`. With the
server's `io_method=worker`, PG's existing I/O workers still read into shared
buffers. No PostgreSQL core changes or additional I/O processes are required.
With `io_method=sync`, the same path is correct but consumption may perform the
read synchronously; queueing cannot turn a synchronous backend into AIO.

## Configuration

```sql
SET pg_yaap.scan_read_stream = yaap;     -- yaap (default) or pg
SET pg_yaap.scan_prefetch_buffers = 1024; -- requested window in pages
SET pg_yaap.scan_prefetch_requests = 1;   -- 0 retains the old window-only policy
SET pg_yaap.profile_scan = sample;
SET pg_yaap.profile_timeline = basic;
SELECT ...;
```

`scan_read_stream=pg` selects the original PG ReadStream implementation,
including PG's BAS_BULKREAD policy for sufficiently large relations. The
default YAAP stream submits asynchronous reads directly to PG's Buffer Manager
without a BAS ring. There is no automatic fallback when the YAAP path is
selected and fails. Both GUCs are captured in the query DSM before background
workers launch.

The requested window is 16–8192 pages, default 1024. Actual windows can be as
small as one page. At 8KiB/block, the requested default is 8MiB. See the actual
`window` and `batch` in `pg_yaap[read-stream] worker` output instead of assuming
the requested value was granted.

## Policy and ownership

- Preserve the existing parallel heap block allocator and its continuous runs.
- Preallocate fixed batch slots with stable Buffer arrays and
  ReadBuffersOperation objects. No request storage is allocated in Next.
- When below the page low-water mark, refill within both the page budget and
  the outstanding-request limit. With a nonzero request limit, each refill
  starts at most two new operations requiring a Wait. Cached-ready operations
  do not consume that I/O quota. Batch size remains at most `io_combine_limit`
  and half the granted page window (at least one page).
- Only join consecutive block numbers in one batch. Preserve a discontinuous
  next block for the following batch; PG handles relation segment boundaries.
- PG may shorten a request and leave forwarded pins. Continue with those exact
  array entries and never move an outstanding operation or its Buffer array.
- Wait at the first buffer of each outstanding operation before consumption.
  A false StartReadBuffers return means its accepted pages are already ready.
- Transfer exactly one pin to the heap scanner for each returned Buffer, clear
  its queue entry, and retain the existing page visibility/deform handling.
- On normal completion or early LIMIT, wait for all pending requests before
	  releasing all unconsumed pins.

The YAAP queue passes a NULL BufferAccessStrategy to `ReadBuffers`; misses use
the ordinary shared-buffer replacement path and can occupy any available
buffer instead of cycling through a per-scan BAS ring. This is intended for
large-memory TPC-H runs where retaining scanned pages in `shared_buffers` is
useful. Pages are still unpinned after consumption and can be evicted under
normal buffer pressure; YAAP does not create a second cache or pin the full
relation. Each active source window is limited by a quarter of shared_buffers
across query participants and the backend GetPinLimit minus a small reserve. It is
not divided by every Scan descriptor in the query DAG: current workers run a
RUN task to completion, including yielded fragments, and destroy its source
before taking another task. If scheduling changes to interleave RUN tasks,
re-audit this assumption. The window does not reserve pins up front; filling
still checks PG's actual additional-pin and strategy limits. If the additional-
pin estimate is zero and this queue has no data, the existing one-page progress
guarantee is preserved. Real small windows use a batch at most half the window
(minimum one) so the validated head and a prefetched batch can coexist. See
[scan_window_budget.md](scan_window_budget.md).

Request memory is deliberately transaction-owned in TopTransactionContext:
PostgreSQL ERROR may skip C++ destructors, so request descriptors and result
storage must outlive ResourceOwner's AIO/pin cleanup. Successful End explicitly
frees the allocations; transaction abort reclaims the abnormal path. No bare
malloc or heap-owning STL containers are used for request storage.

## Diagnostic counters

Existing `scan_load_page` / `load_ms` now measures the selected Next API. The
scan profiler additionally prints worker, pipeline and query aggregates:

| Counter | Meaning |
|---|---|
| `submit_ms` | Time inside StartReadBuffers, including any waits within that API |
| `wait_api_ms` | Time inside WaitReadBuffers during consumption; not pure device service time |
| `refill_exclusive_ms` | Queue filling/block allocation time excluding submitted API intervals |
| `requests`, `need_wait` | Start calls and returns requiring a later Wait; the latter may already be complete when consumed |
| `requested_pages` | Sum of API request sizes, including repeated suffixes after splitting; not unique bytes read |
| `accepted_pages` | Pages accepted across completed submissions, counting cache hits as well as reads |
| `window`, `batch` | Granted per-source limits, meaningful in Worker records; zero in aggregates |

Submit, consumption-wait and refill-exclusive times are subcomponents of Next,
not additional costs to add on top of `load_ms`. Waits during End on an early
stop are outside these consumption counters, though they remain in task/wall
time. Peak pins are a per-queue diagnostic, excluding transferred current-page
pins and other queues. Profiling off does not read clocks in the queue.

## Validation and limits

`tests/read_stream.py` compares native PostgreSQL results and both YAAP read
paths for checksums (including deleted tuples), predicates, strings, multiple
sources, empty input, early LIMIT/reuse, cancellation and worker termination.
Each test run checks 38 cases. The same suite passes with worker AIO/2GiB
shared buffers and with sync I/O/8MiB shared buffers, exercising one-page
windows and the low-pin progress path. Existing scan, Join and memory tests
also support `YAAP_TEST_READ_STREAM=yaap`.

This is a fixed-window/request-cap prototype, not an adaptive scheduler. Short runs can
fill the slot count before the page window. Cached scans may pay extra queue
and initial-fill overhead. PG's pin limits can prevent a larger requested
window from taking effect. Storage bandwidth, Buffer Manager contention and
I/O pool limits remain PG concerns below this queue. The API is tested against
the current local PG branch, not promised as a stable cross-major-version ABI.

Q1 A/B results, raw outputs, diagnostics and test evidence are in
`artifacts/yaap-read-stream-20261023/`. The default remains PG because the
initial window-only SF100 test did not demonstrate a speedup; request-cap
measurements are recorded separately in the throttle experiment report.

## Page assignment and the PG I/O worker handoff

The parallel allocator uses a shared atomic fetch-and-add to claim contiguous
page-number chunks. Within a claimed chunk, a worker advances a private cursor.
It does not perform an atomic operation for every consumed page or assign a
permanent half-table to each worker. Q1 currently starts with 16384-page
chunks; the allocator ramps chunk size down near the tail. Startblock can wrap.
Prefetch advances the page-number generator, so claimed pages are not the same
as processed pages. The queue reads only after obtaining ownership and keeps
its saved block at a discontinuity. Page-number allocation is independent of
which shared-buffer slot PG uses for that page.

The PG native stream still uses its access-strategy ring when `scan_read_stream`
is explicitly set to `pg`. The YAAP queue has no such ring; its bounded request
queue and pin window control concurrent I/O, not total scan cache residency.
The separate PostgreSQL AIO submission queue described below is unrelated to
the BufferAccessStrategy ring.

In the current PG branch, method_worker.c submits request IDs through a global
64-slot ring (one slot reserved), then uses SetLatch to wake an idle I/O worker.
Workers consume the queue and wake peers. If insertion fails because the queue
is full, pgaio_worker_submit_internal executes those requests synchronously in
the submitting process. This is not a notification-timeout fallback. Requests
referencing private memory or files that cannot be reopened also require
synchronous handling through separate paths. No core source was modified to
observe these mechanisms.

## Detailed plugin-side profiling

`pg_yaap[read-stream-cpu]` samples approximately 1/64 Submit/Wait API calls using
CLOCK_THREAD_CPUTIME_ID nested within elapsed-time brackets. It reports raw
sample counts, sampled wall and sampled CPU sums, without extrapolating CPU
into an exact whole-query number. Clock failures are explicit and never fall
back to process CPU time. CPU/wall differences include scheduling and lock
sleep, not just disk I/O. Clock quantization is clamped to avoid negative
non-CPU intervals; sample results remain diagnostic estimates. I/O worker CPU
is not included in the submitting thread's CPU samples.

`pg_yaap[read-stream-queue]` adds refill counts, pin/slot limit stops, request
splits, single/multi-page accepted requests and maximum queue requests not yet
Waited. That peak is per queue, can include already completed requests, and is
not the global submission depth or current physical disk concurrency.

`pg_yaap[scan-buffer]` uses PG's own shared_blks_hit/shared_blks_read deltas
inside GetData to report shared-buffer hit rates for either stream. These are
PG accounting counters, not a measure of OS-cache hits or physical device
traffic. Init/catalog work outside GetData is excluded.

The 2/4/8GiB SF100 Q1 comparison, CPU/elapsed samples and external worker stack
samples are saved in `artifacts/yaap-read-stream-20261023/`. Larger windows moved
blocking time into Submit rather than consistently reducing query time. Worker
stacks confirm pgaio_worker_submit -> pgaio_io_perform_synchronously -> preadv,
which is the current PG worker provider's queue-full path for these heap reads.
The measurements do not directly time enqueue-to-wakeup delay, and this change
does not claim to fix that latency or eliminate synchronous execution.

## Request throttling

`pg_yaap.scan_prefetch_requests` is an integer GUC (0–64, default 1), captured
in query DSM for workers. It applies only to the explicitly selected YAAP
stream; `scan_read_stream=yaap` is now the default. Zero preserves
the old full-page-window submission policy for A/B tests. Positive values
strictly cap operations for which Start returned need_wait and consumption
Wait has not completed. This is per source, includes already completed reads
not yet Waited, and is not a reservation of PG's global I/O queue slots.
Other sources and queries can still compete for that global queue.

A batch tracks its first block, reserved page count, submitted prefix and
consumed prefix. If PG splits a read, quota/burst exhaustion leaves the
remaining suffix and forwarded pins in the same stable array. Refill resumes
that suffix before creating a later batch, even after the block generator
reaches EOF. Next never exposes an unsubmitted page. If head progress requires
quota, it completes the oldest pending operation and retries. End only Waits
submitted operations, then releases all remaining pins, including forwarded
pins for an abandoned suffix. Page budgeting conservatively includes reserved
pages that have not actually been pinned yet.

When quota is full, Next avoids repeatedly attempting a no-op refill for each
page. Completion during consumption releases the quota; the next page can
trigger another small refill. No new scheduler state, global spinlock, busy
polling or PG core changes were introduced. Counters include request-limit
stops and two-I/O burst stops, while max_queue_inflight can be checked against
the configured per-source limit.

`tests/read_stream.py` now covers limits 0/1/2/4, with an optional
`YAAP_TEST_STRESS_SPLIT=1` relation larger than shared_buffers and warmed
alternating page islands. It checks mixed-cache split continuations, quota-one
EOF, unfinished suffixes on LIMIT, checksum conservation and backend reuse.
The throttle experiments are under `artifacts/yaap-read-throttle-20261023/`.

### Per-request page cap

`pg_yaap.scan_prefetch_batch_pages` limits each YAAP logical read batch (0–128,
default 0 follows PostgreSQL `io_combine_limit`). The effective cap is also bounded
by `io_combine_limit` and the granted queue window. PostgreSQL may accept fewer
pages. This setting is copied to query DSM and does not change the PG stream.
For example, `SET pg_yaap.scan_prefetch_batch_pages=16` caps batches at 16 pages.
Keeping `scan_prefetch_requests=4` also reduces the pages covered by those requests;
this is not a constant-in-flight-byte comparison.

### One-request default

The default is now 1 outstanding, not-yet-validated Need-Wait operation per
local Scan Source. A completed operation releases its quota when consumption
Wait validates it; the following Next call can issue another operation while
the already validated current batch is consumed. Cached/validated buffers may
still be held up to the pin/window budget. This does not mean synchronous IO.
For Q1 each worker has one Scan Source, so twelve scan workers can keep at most
twelve such operations, rather than forty-eight with quota four. Multi-source
queries retain a separate quota per Source, not a backend-global token.
Existing explicit settings remain valid; no global PG queue size changed.
