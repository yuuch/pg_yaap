# Scan I/O profiler

YAAP's physical scan now uses PostgreSQL's native ReadStream. The extension
does not keep a parallel YAAP read queue and cannot time the native stream's
internal buffer lookups, `StartReadBuffers`, `WaitReadBuffers`, or AIO queue
handoff.

## Use

```sql
SET pg_yaap.profile = off;
SET pg_yaap.profile_scan = sample;       -- off, sample, exact
SET pg_yaap.profile_scan_io = summary;    -- off, summary, detail
SELECT ...;
```

`profile_scan` attributes elapsed work in YAAP's scan source to page loading,
heap page preparation, tuple processing, qualification, projection, and setup.
It also reports PostgreSQL's `shared_blks_hit` / `shared_blks_read` deltas
within the scan's `GetData` boundary. These counters do not reveal whether a
page was ready before the native ReadStream consumed it, nor do they identify
physical SSD reads.

`profile_scan_io` retains only observations available at the extension
boundary. It reports source elapsed time and PG-accounted buffer counters;
native ReadStream timing, per-page readiness, I/O worker queue depth, and
queue-full fallback cause are marked unavailable. It does not produce YAAP
queue metrics, request-size histograms, or custom-buffer-allocation timings.

To export scan observations with the existing secure timeline exporter:

```sql
SET pg_yaap.profile_scan_io = detail;
SET pg_yaap.profile_timeline = trace;
SET pg_yaap.profile_trace_file = '/absolute/writable/prefix';
SELECT ...;
```

The exported JSON retains availability and source labels. Open the resulting
trace in Perfetto. No per-page trace is collected by normal scan profiling.

## Buffer strategy

YAAP creates heap scans without `SO_ALLOW_STRAT` and passes a null
`BufferAccessStrategy` to PostgreSQL's parallel ReadStream. YAAP therefore does
not use a BAS_BULKREAD ring. PG's normal shared-buffer replacement policy still
applies; removing the ring does not pin a relation in memory or prevent normal
eviction under pressure.

For external stack samples, see [scan_io_stacks.md](scan_io_stacks.md). A
sample can show which code path was active, but it is not an event count, wait
duration, physical I/O measurement, or proof of queue-full fallback.
