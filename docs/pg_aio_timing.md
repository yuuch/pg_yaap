# Optional PostgreSQL 18 AIO timing patch

The patch in `patches/postgresql-18-aio-timing.patch` was developed against
PostgreSQL 18.3 source commit62d6c7d3df6287f1bd83199c1a746e50d31571a0.
It adds an opt-in `READ_BUFFERS_PROFILE_TIMING` flag and owner-local
`ReadBuffersGetTiming()` result. No new queue, buffer manager or IO worker is
introduced. Normal reads do not take the new clocks. Fixed per-handle/result
storage is larger even with timing disabled.

## Build and ABI

Apply from the PostgreSQL source root, then rebuild/install PG and rebuild YAAP
against the newly installed server headers. Restart before loading the rebuilt
extension. `PgAioReturn` and `ReadBuffersOperation` layouts changed: a plugin
built with patched headers must be paired with the patched server; toggling a
GUC cannot fix an ABI mismatch. Other modules using these structures must also
be rebuilt. The YAAP source conditionally compiles the adapter using
`YAAP_PG_AIO_TIMING_VERSION`, so compiling against stock headers leaves kernel
metrics unavailable.

This workspace uses PostgreSQL `build-bench -> installed-bench` followed by
YAAP's `contrib/pg_yaap/build-bench`. The separate `installed` prefix is not
changed. Reverting requires restoring/rebuilding the matching PG headers/runtime
and rebuilding YAAP, not just swapping the postgres executable.

## Enable

```sql
SET pg_yaap.scan_read_stream=yaap;
SET pg_yaap.profile_scan_io=detail;
-- Optional bounded prefix with executor PID and per-operation timings:
SET pg_yaap.profile_scan_blocks=4096;
```

Summary/off modes do not set the kernel timing flag. AIO queue timing is recorded
inside the existing queue lock at successful insertion/consumption. Worker
reopen, actual read syscall, shared completion and enqueue-to-shared-ready are
separate elapsed intervals. The executor PID is carried back; operations with
partial reads executed by different processes are marked mixed instead of
incorrectly attributed to one worker. No shared-handle lookup occurs after
Wait: completion copies the result into the issuing backend before recycling.
Repeated physical reads are accumulated by `ProcessReadBuffersResult()`.

`kernel_queue_full_sync_requests` is set only in the actual failed queue-insert
branch. Forced-sync and unclassified executors remain separate. Successful read
bytes are bytes returned by the syscall, not proof of physical SSD traffic.
`read_ns` includes OS page cache, kernel scheduling and syscall blocking. The
IO_uring path is unclassified for this syscall instrumentation; it must not be
reported as zero-latency IO. Failed reads/reopen errors are not used as complete
speed samples. Clocks are cross-process monotonic nanoseconds; errno is saved
before reading a completion timestamp.

Worker queue/read sums overlap across outstanding requests and processes. They
are not query wall time and must not be added to consumer processing time.
Kernel histograms in YAAP contain physical-request sums per logical read
operation, so retries are explicitly labelled; they are not raw per-physical-
request latency distributions.

## YAAP page consumption

`page_consumer_gap_ns` measures the interval between a successful Next Buffer
return and the next Next entry. It reuses existing clocks and includes tuple
visibility/deform, downstream operators, locks and descheduling. It is not pure
CPU or columnar-only time. No first-page interval exists; a last page contributes
only if another Next entry is observed (including EOF). LIMIT/cancellation may
leave that final interval unobserved. The mean can guide experiments but does
not by itself predict optimal request size or scale linearly with workers.

For per-IO-worker prefix samples, collect `[scan-block] event=WAIT` records with
`kernel_available=1`, nonmixed executors and `kernel_worker_requests` equal to
`kernel_requests`, group by `kernel_executor_pid`, and divide summed read time
by summed requests/bytes. This is bounded-prefix evidence, not whole-query
per-worker accounting. Whole-query aggregate counters remain available in
`[scan-io-kernel]` and the timeline JSON.

## Buffer allocation extension

The same combined patch now also exposes `READ_BUFFERS_PROFILE_BUFFER`,
`YAAP_PG_BUFFER_PROFILE_VERSION` and `ReadBuffersGetBufferProfile()`. YAAP detail
mode sets the flag and consumes the owner-local result immediately after Start,
including cache-ready returns. The patch adds `freelist.c` and
`buf_internals.h` instrumentation; rebuild/install both PG and YAAP again.

Counts cover initial mapping hits/misses, Ring selection, Free List candidates,
Clock Sweep visits/usage decrements/pinned skips, retries and FlushBuffer
attempts. A mapping hit is not necessarily a valid/ready page. Candidate
selection is not a guarantee that later invalidation succeeded.

Elapsed phases are sampled using independent 1/64 Start-call cohorts. Raw sums
include profiler clock/counter overhead. Allocation, victim selection, strategy
and invalidation are inclusive/nested; mapping-lock acquisition includes both
old- and new-tag acquisition envelopes but not the entire lock hold. Do not add
all phase times. Multiplying a nested sampled total by64 can exceed the exact
Start envelope because it also extrapolates nested clock overhead. It is not a
precise whole-query allocation total. For rarely visited phases, a zero raw
sample may mean no sample was selected, not zero real cost. Exact path counts
remain independent of timing sampling. Temp/local buffers are unavailable.
