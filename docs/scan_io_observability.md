# Extension-only Scan I/O observability audit

Audit target: the local PostgreSQL 18.3 binary and matching source under
`/Users/chenyunwen/proj/postgres`. No PG source changes, SQL experiments or
builds were performed for this audit. Conclusions concern the current API,
not a stable promise across PG major releases.

## Observable boundary

| Metric | Availability | Source | Exact meaning / limitation | Cost |
|---|---|---|---|---|
| Pages accepted by StartReadBuffers | EXACT | YAAP API arguments/results | Accepted logical pages, including cache-ready pages; not device-read bytes | O(1)/call |
| Pages guaranteed ready at Start return | EXACT | StartReadBuffers false return | Safe to access, but may have become ready during the call; not necessarily an initial cache hit | O(1)/call |
| Consumed pages | EXACT | YaapReadStreamNext pin transfer | Count once on transfer to scanner; no initial lookup recount | O(1)/page |
| Initial ready/miss/already-loading three-way classification | UNAVAILABLE | No observational public API | Public read API hides BufferAlloc/IO-in-progress decisions | None |
| PG-accounted hit/read deltas | EXACT statistic | pgBufferUsage | Standard PG counters within the chosen call boundary, not a full initial-page census | O(1)/GetData |
| Requested buffer_hit_rate from a complete initial census | UNAVAILABLE | Above boundary | hit/(hit+read) does not include every already-loading lookup | None |
| Strict page-ready-at-consume rate | UNAVAILABLE | No pure public Buffer readiness probe | Wait-required request must be completed/validated before access | None |
| Known-ready-at-consume pages | EXACT lower bound | YAAP request metadata | Start(false), or successful earlier Wait for the same request; remaining pages are UNKNOWN, not NOT_READY | O(1)/page |
| Cold prefetch ready rate | UNAVAILABLE | Initial cache origin is unknown | Do not infer cold from need_wait=true | None |
| Wait API elapsed time/count | EXACT API interval | YAAP instrumentation | Includes validation, retries and possibly synchronous reads; not actual blocked I/O | Two timestamps/Wait |
| actual_io_wait_ns / strict I/O Stall Ratio | UNAVAILABLE | Public Wait wrapper lacks an exclusive wait-only hook | Neither total Next, Wait API elapsed, nor elapsed-minus-CPU is a pure I/O wait counter | None |
| Sampled non-CPU fraction of Submit/Wait | ESTIMATED | Thread CPU + monotonic clock | Includes descheduling and lock sleeps; not I/O-specific | Bounded sampling |
| Start API entry/return and t_need | EXACT boundary timestamps | YAAP instrumentation | Real submission is somewhere inside opaque Start; name metrics after their actual boundary | Per request/page sample |
| True t_ready / true I/O completion latency | UNAVAILABLE | No timestamp in read API or pg_aios | Completion may precede first observation; validation may retry | None |
| Time to observe validated readiness | EXACT observed upper bound | Successful Wait return | Includes delayed observation and may dwarf true I/O latency; not a completion timestamp | Request metadata + histogram |
| Configured window / ring / backend pin cap | EXACT configuration | YAAP + public strategy/pin accessors | A cap is not proof it constrained actual progress | O(1)/Begin |
| Submitted lookahead | EXACT logical count | accepted minus consumed | Counts accepted future pages, not reserved unsubmitted suffixes | O(1)/Start/Next |
| Reserved page budget | EXACT logical count | YAAP queue state | Existing s->pinned includes unsubmitted reservations; it is not actual physical pin count | O(1) |
| Current actual queue pin count | UNAVAILABLE at O(1) | Forwarded buffers are hidden in result array | submitted pages <= actual pins <= reserved pages in normal complete state | No scan by default |
| Request cap / pin-limit / slot-limit events | EXACT branch counts | YAAP code | Names must describe the concrete branch, not causally assert a bottleneck | O(1) |
| Actual disk ios_in_progress | UNAVAILABLE | Completion occurs independently | Existing inflight is requests not yet Waited, possibly already completed | None |
| Stream creation/reset counts | EXACT | YAAP lifecycle | Count actual create/reset sites, not task re-entry | O(1)/lifecycle |
| I/O request size histogram | EXACT logical API distribution | Start accepted count | Distinguish calls returning need_wait from ready calls; still not exact OS/device operation distribution | Fixed histogram |
| Queue depth/full/fallback count and ns | UNAVAILABLE | PG-private worker provider | Do not access static queue objects or infer from latency/f_sync | None |
| Kernel synchronous execution reason | UNAVAILABLE | Opaque PG provider | External stacks/logs may support a hypothesis, not an exact per-scan counter | Optional external |
| Explicit extension-controlled sync path | EXACT only if present | YAAP branch | Current queue does not choose a separate read-the-file sync path; io_method=sync is PG configuration | O(1)/branch |
| PG sync mode configured | EXACT configuration | io_method | Does not imply every cache-ready page performs synchronous I/O | O(1)/Begin |
| Repeated physical page loads | UNAVAILABLE | No cheap per-scan physical-load event | Multiple scans or low hit rate are not proof of repeated device reads | Optional labeled bounded sample |
| SSD physical throughput/utilization | UNAVAILABLE in extension | OS tools such as iostat | PG read bytes may be served from OS cache | Optional external |

## Why an AIO wait reference is not a page-ready API

`storage/bufmgr.h:121–141` documents the communication fields after strategy
as **private**, initialized only for reads. This includes `io_wref`,
`io_return` and `nblocks_done`. The public supported read contract is
StartReadBuffers/WaitReadBuffers (`bufmgr.c:1465–1486`): a false Start return
guarantees accepted pages are ready; a true return requires Wait on the same
operation and stable arrays before those pages are accessed.

`storage/aio.h:324–329` exposes `pgaio_wref_check_done` for caller-owned wait
references. It is not entirely observational: the implementation can reclaim
an owner's completed handle (`aio.c:1002–1040`, notably 1030–1031). Accessing
StartReadBuffers' private reference would add a version-sensitive dependency.
A completed handle can represent a partial read or error. Wait processes the
result (`bufmgr.c:1592–1628`), reports errors, and can issue more I/O until the
whole operation is complete (`1632–1741`, notably 1723–1738).

Therefore, a public handle-level completed observation alone must not be
exported as safe-page-ready, actual t_ready, completed full-operation latency,
or an exact stall classification. This implementation should not add polling,
early Wait calls, callbacks or changes to prefetch ordering merely to obtain
more accurate telemetry. A future version-specific optional adapter would
need a separate contract review and explicit precision labels.

`BufferIsValid` only tests a nonzero Buffer number (`bufmgr.h:348–374`), not
BM_VALID. `ReadRecentBuffer` is not a pure alternative: it enlarges the
ResourceOwner, pins on success and increments shared_blks_hit
(`bufmgr.c:682–750`, especially 692–693 and 736–740). Calling it per page would
perturb pins and the very cache statistics being measured.

## Statistics cannot repair the missing classifications

PG increments shared_blks_hit when BufferAlloc reports a ready hit
(`bufmgr.c:1154–1157`). It increments shared_blks_read when the backend actually
issues an smgr read (`1953–1963`). A lookup finding another backend's load can
fall outside that simple hit+own-read census. Consequently, preserve these as
PG-accounted statistics and do not manufacture the third category by
subtracting them from page consumption.

`pgBufferUsage.shared_blk_read_time` is also not a pure wait timer. PG adds to
it through pgstat_count_io_op_time (`pgstat_io.c:122–148`) both around actual
AIO waits (`bufmgr.c:1692–1704`) and around smgrstartreadv (`1953–1958`), where
sync fallback may execute. Delta collection restricted to Wait still has
partial-retry/sync caveats. A clearly named PG-reported read-time proxy is
possible; exact actual_io_wait_ns remains unavailable.

## pg_aios and synchronous evidence

`pg_aios` is a live handle snapshot, with owner PID, handle ID/generation,
state, operation, offset/length, result and flags (`aio_funcs.c:47–224`). It
has no submission/dequeue/completion timestamps or historical queue-full
counter. Short completed requests may disappear between snapshots; handles
are recycled. It requires pg_read_all_stats or equivalent access
(`system_views.sql:1404–1409`), so unavailable permissions must not fail the
query being profiled.

`f_sync` renders PGAIO_HF_SYNCHRONOUS (`aio_funcs.c:221`), which is documented
as a hint, not a reason (`aio.h:63–70`). It can request the generic synchronous
path (`aio.c:482–500`). Conversely, queue-full fallback directly calls
pgaio_io_perform_synchronously without necessarily setting that hint.

The current worker provider has a private 64-slot submission ring
(`method_worker.c:97–100`). Failed insertion (`181–192`) collects requests
for synchronous execution (`244–289`). DEBUG3 can log queue full, DEBUG4 can
log worker choice, and external stacks can show the provider's synchronous
call path. Those observations can justify a separately sourced diagnosis,
with scope, sample coverage and uncertainty, but not populate exact extension
fallback counts. Generic synchronous execution has additional causes:
private memory, non-postmaster operation, or inability to reopen the file
(`235–240`).

## Safe time and histogram names

Recommended request metadata are fixed-size per already-preallocated request
slot, not a historical per-page log. Store Start API entry/return and the
accepted page range. On consumption, measure `start_return_to_need` and retain
the request/page association. After a required successful Wait, record
`validated_ready_observation_upper_bound`, explicitly including observation
lag. A fixed log histogram may merge count/sum/buckets across workers.
Do not compare independent lead and observation percentiles to infer a stall,
and do not call the observation distribution I/O completion latency.

If an operation was known ready at Start or by an earlier successful Wait,
its consumed pages contribute to a known-ready lower bound. For the first
page of a required Wait, readiness remains unknown at the extension boundary.
No arbitrary duration threshold converts UNKNOWN into NOT_READY.

## Diagnosis constraints

A confirmed limiter event establishes that YAAP reached that limiter, not
that increasing it will improve latency. Record requested, granted, ring,
backend-pin and fair-share caps separately; a configured window clamp is
not evidence that actual submitted lookahead was persistently pin-limited.
IO limits can dominate well below the pin cap.

Low cache hit rate alone cannot establish shared-buffer pressure, nor can
normal BAS_BULKREAD reuse in the native PG stream mode. The YAAP stream mode
uses no BAS ring. GetAccessStrategyWithSize caps a ring at NBuffers/8
(`freelist.c:626–654`), while BAS_BULKREAD allows pins up to the full ring
(`689–703`). Backend pin limits and current additional-pin limits are separate
public APIs (`bufmgr.h:303–308`). Ring capacity, backend pins and total shared
buffer pressure require distinct diagnoses.

Exact QUEUE_FULL_FALLBACK, strict PREFETCH_TOO_LATE, actual I/O Stall and
STORAGE_BANDWIDTH_LIMITED should default to INSUFFICIENT_DATA when only the
above extension metrics are available. High API elapsed with low sampled CPU
can justify a low-confidence waiting hypothesis, not a specific device or
queue cause. Device saturation requires optional independently sourced OS
observations and a credible sustained-bandwidth baseline.
