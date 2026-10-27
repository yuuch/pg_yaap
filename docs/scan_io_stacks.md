# Scan I/O stack evidence on macOS

YAAP uses PostgreSQL's native ReadStream, so the extension cannot time its
internal `StartReadBuffers` and `WaitReadBuffers` calls. Use the optional
external helper to collect bounded stack evidence while a representative query
runs. No PostgreSQL kernel changes, symbol interception, or executor changes
are required.

## Collect

Start the query. In another terminal, select
explicit YAAP worker PIDs belonging to that query. In a dedicated instance,
`SELECT pid, backend_type FROM pg_stat_activity WHERE backend_type='pg_yaap worker'`
is useful; in a shared instance, that predicate alone does **not** establish query
ownership. The helper deliberately does not guess ownership or launch a query.

```sh
python3 tests/profile_scan_io_stacks.py --target-role yaap \
  --pid 12345 --pid 12346 --duration 1 \
  --output artifacts/q1-scan-stacks
```

Use actual PIDs. Each PID is checked with `ps` and must be a live PostgreSQL process.
`--target-role yaap|io` records the role selected by the caller; it does not
verify database or query ownership. To sample PG I/O workers, select their
explicit PIDs and use `--target-role io` with a separate output directory. Up to eight distinct PIDs are
allowed, each sampled sequentially for 1–5 seconds at a 10 ms sample interval.
Each sampler has a duration + 10 second timeout. The raw output and sampler log
are retained; existing raw files are never overwritten. A worker may exit or a
PID may be reused between verification and attachment; inspect raw process
metadata when interpreting a short or failed sample. Permissions to attach to
the process are required by macOS.

`stack-evidence.json` includes up to three bounded ancestry examples per
category. All flags mean **sampled presence**, not event counts or elapsed-time
attribution:

- `submitter_sync_read`: the same YAAP scan stack contains
  `pgaio_worker_submit → pgaio_io_perform_synchronously → pread/preadv`.
- `buffer_io_wait`: the same YAAP scan stack contains
  `read_stream_next_buffer → pgaio_io_wait →` a wait primitive.
- `io_worker_read`: `IoWorkerMain → pgaio_io_perform_synchronously → pread/preadv`
  occurs within one stack. This is expected execution inside an I/O worker and
  is kept separate from compute-worker synchronous execution.
- `buffer_replacement`: `GetVictimBuffer` appears below a YAAP scan frame.
  Replacement is normal under the shared-buffer policy; its presence does not
  establish Shared Buffer pressure.

The parser follows indentation in the `sample` call graph. Unrelated branches,
threads, flat symbol summaries, and binary image names cannot jointly establish
one of these paths.

A synchronous syscall can hit the OS page cache; it does not prove a physical
SSD read. A missing path does not prove it never ran. The queue-full count and
physical-disk I/O remain `UNAVAILABLE`; sampled symbol ancestry does not provide
precise fallback frequency, reason counts, or duration. Sampling perturbs the
process and must be run separately from baseline performance measurements.

## Parse existing captures and check the parser

```sh
python3 tests/profile_scan_io_stacks.py --self-test
python3 tests/profile_scan_io_stacks.py \
  --parse-files artifacts/q1-scan-stacks/worker-12345.sample.txt \
  --output artifacts/q1-scan-stacks-parsed
```

Parsing does not require PostgreSQL or macOS. Up to 64 input files are accepted per run, each bounded to 20 MiB
and evidence paths to 32 frames. Eight synthetic ancestry checks cover a positive
sync path, sibling branches, missing call graph, missing YAAP ancestry, a wait
path, cross-thread separation, and I/O-worker execution separated from YAAP execution.
