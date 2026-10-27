# Scan I/O observability

YAAP SeqScan uses PostgreSQL's native `ReadStream` and Buffer Manager. The
extension does not own the individual read operations, so it cannot observe
their internal Start/Wait timing or AIO queue state without changing PG core.

| Metric | Availability | Source | Meaning |
|---|---|---|---|
| Scan source elapsed time | Exact elapsed interval | YAAP `GetData` profiler | Worker elapsed time, including scheduling and time in PG Buffer APIs |
| Shared buffer hits and reads | Exact PG counter delta | `pgBufferUsage` around YAAP `GetData` | PG-accounted hits/reads, not a complete per-page initial lookup classification |
| Buffer ready at consumption | Unavailable | Native ReadStream hides per-page pre-Wait state | A valid returned Buffer is ready, but whether it waited is not exposed |
| `StartReadBuffers` / `WaitReadBuffers` duration | Unavailable | ReadStream owns these calls | `scan_load_page` includes its public `read_stream_next_buffer` interval, not a separated wait duration |
| AIO submission queue depth/full fallback | Unavailable | PG worker provider internals | Do not infer queue-full from elapsed time or `f_sync` |
| BAS ring use by YAAP SeqScan | Disabled | YAAP scan initialization | Heap scan is created without `SO_ALLOW_STRAT`; ReadStream receives a null strategy |
| Physical device bytes / latency | Unavailable in extension | OS/device tools | PG reads may be served by the OS page cache |

Use `pg_yaap.profile_scan=sample` or `exact` for scan stage timing and PG
buffer counters. `pg_yaap.profile_scan_io` may report source elapsed time and
PG-accounted buffer deltas, but native ReadStream internals remain explicitly
unavailable. See [scan_io_profiler.md](scan_io_profiler.md).
