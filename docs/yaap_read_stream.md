# Physical SeqScan ReadStream

YAAP uses PostgreSQL's `ReadStream` and Buffer Manager for heap scans. The
extension no longer maintains a separate read queue or submits its own
`StartReadBuffers` / `WaitReadBuffers` operations.

For a YAAP sequential scan, `heap_beginscan` is called without
`SO_ALLOW_STRAT`. This prevents PostgreSQL from creating a `BAS_BULKREAD`
`BufferAccessStrategy` for the scan. YAAP then initializes its parallel scan
ReadStream with a null strategy, so its reads use PostgreSQL's normal shared
buffer replacement policy rather than a small bulk-read ring. The scan still
uses PG's normal buffer pins, visibility checks, and I/O implementation.

The callback passed to `read_stream_begin_relation` only obtains the next
block from PostgreSQL's parallel block allocator. YAAP's allocation chunk is
work assignment to a scan worker; it is not an I/O request size. PostgreSQL
ReadStream controls lookahead, I/O batching, and pinning.

## Configuration

There is no YAAP-specific ReadStream mode or prefetch-window GUC. Configure
PostgreSQL's I/O behavior with its normal settings, such as `io_method`,
`effective_io_concurrency`, `io_combine_limit`, and `io_max_concurrency`.
`pg_yaap.profile_scan` can still report scan-stage timings and PostgreSQL's
shared-buffer hit/read counters. `pg_yaap.profile_scan_io` reports extension
observations where available and explicitly marks PostgreSQL ReadStream
internals as unavailable.

The ReadStream's own lookahead pins a bounded set of buffers. Removing the
BAS ring allows consumed pages to remain in the shared buffer pool under the
normal replacement policy; it does not pin a whole relation or guarantee that
pages stay resident under memory pressure.

The PG ReadStream is created per parallel scan source and is ended before the
heap scan descriptor releases its resources. Both the coordinator and
background workers use the same path, including when the coordinator
participates in scan work.
