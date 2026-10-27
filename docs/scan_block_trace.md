# Bounded scan Block trace

The aggregate Scan I/O profile identifies expensive API boundaries. This optional
trace associates those boundaries with actual relation Block numbers, so an
investigator can follow one worker's assigned range, read requests and consumption.
It does not classify a cache miss or real device I/O from a Wait call alone.

## Enable

```sql
SET pg_yaap.scan_read_stream = yaap;
SET pg_yaap.profile_scan_blocks = 4096;
-- Optional aggregate profile, independent of Block tracing:
SET pg_yaap.profile_scan_io = detail;
```

`profile_scan_blocks` is an event capacity **per local Scan Source**, 0–65536;
0 disables it. This is a short diagnostic capture, not an unbounded SF100 page
log. Each source owns a fixed-capacity local buffer. When full, later events are
dropped and counted. Recorded events are emitted at `LOG` level to the PostgreSQL server log at stream
teardown, rather than
performing log I/O for each page in the scan hot path. Trace collection, timestamp
reads and final output have overhead; use a separate run for baseline timing.
A capacity of 4096 does not mean 4096 pages: assignment, submission and Wait events
also consume entries. Multi-source joins multiply the configured per-source cap.

## Event association

Background-worker LOG output is not forwarded through the leader's client
connection. Read the server log; a successful client query with no Block lines
on stderr does not mean tracing was disabled.

Each `pg_yaap[scan-block]` line includes PID, Worker slot, relation OID, a
`stream` origin timestamp identifying the local source, sequence,
monotonic `time_ns` / `end_ns`, request ID, Block range and Buffer information.
Request IDs are local to a source, not globally unique. Worker slot alone is not
a process identity; retain PID, `stream` and relation context when joining records. `rel` is the
relation OID, not its name.

| Event | Meaning |
|---|---|
| `ASSIGN` | Worker claims a contiguous ownership range from the PG parallel Block allocator. Assignment is work ownership, not an I/O request. |
| `START` | One `StartReadBuffers` call, with requested and accepted page counts, accepted first Block, API entry/return times and whether PG requires a later Wait. |
| `NEED` | A particular Block is needed for consumption; associated with the read request that covers it. Its timestamp is taken before any refill needed to obtain that page. |
| `WAIT` | `WaitReadBuffers` API entry/return for that request, recorded as an interval. |
| `CLEANUP_WAIT` | Outstanding operation waited at teardown, for example after a LIMIT. Keep separate from waits incurred to consume pages. |

The `pg_yaap[scan-block-summary]` line declares recorded events, dropped events,
capacity and successful teardown status (`complete=yes`). A successful teardown
can still have dropped events, so both fields must be checked. Missing records after overflow are not evidence
that a page was never submitted or waited. Query cancellation or Worker failure
can also prevent normal teardown output; never treat absent summaries as a
successful complete capture.

A `NEED` timestamp can precede its corresponding `START`, because a refill can be
triggered only when that Block is needed. Event sequence is recording order;
sort by timestamp when inspecting actual intervals. Block ownership may wrap at
the end of a relation; `last_block` is modulo the relation page count.

## What the evidence supports

For a request's accepted Block range, compare:

1. Its `START` API entry/return and `need_wait` result.
2. When the corresponding Block first appears in `NEED`.
3. Its `WAIT` interval, if any, and the request ID linking the events.

A `START` result that does not need Wait means the accepted buffers are ready.
`NEED.need_wait=-1` means the page had not been submitted at the original
Next entry; `0` means already validated or Start-ready; `1` means validation is
still pending in this queue. For `1`, PG may already have completed the read. A long `WAIT` is real API elapsed cost,
including blocking and other API work, not an exact physical I/O duration.
`START` API return is not the kernel's actual enqueue timestamp. No true I/O
completion timestamp is invented.

A Block with `NEED` before `START` was not submitted by this queue before demand,
which is direct evidence of missing submission lead for that Block. It does not
establish the cause of every subsequent wait. If requests were submitted earlier
but Wait remains costly, the trace alone cannot separate backend scheduling,
I/O-worker queueing, service latency or device throughput.

Buffer IDs refer to PostgreSQL's Buffer Pool; they are reusable identifiers, not
persistent addresses or evidence of an SSD read. Pair this trace with
[Scan I/O profile](scan_io_profiler.md) and optional
[stack evidence](scan_io_stacks.md). No kernel modification is required.

## Integration check

Against a dedicated PostgreSQL instance with the rebuilt library:

```sh
PGHOST=/tmp PGPORT=55432 PGUSER=postgres PGDATABASE=tpch \
YAAP_TEST_OUTPUT=artifacts/scan-block-regression \
YAAP_BLOCK_TRACE_LOG=/absolute/path/to/postgresql-server.log \
python3 tests/scan_block_trace.py
```

The test reads a fresh server-log byte range around each query and saves
`<case>.blocks.log`. Use a dedicated instance with a nonrotating readable server
log and no concurrent YAAP queries. The test uses the existing YAAP fixture/DDL
helpers and checks unchanged query
results, disabled mode, complete single-/multi-worker Block coverage,
Start/Need/Wait request association, independent detailed I/O profiling,
capacity overflow, empty input and LIMIT output/teardown. The hot fixture does
not guarantee an outstanding I/O operation at LIMIT teardown. It does not assert that every
Wait blocked or that every read accessed the physical device.
