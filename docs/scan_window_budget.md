# Scan window budget and batch geometry

The YAAP read queue window is a ceiling on reserved pages, not a reservation of
that many PostgreSQL pins at source initialization. Actual pins are acquired
when `StartReadBuffers` runs. The queue checks `GetAdditionalPinLimit()` before
acquiring each batch and retains the existing one-page empty-queue progress
path when the conservative additional-pin allowance is exhausted.

Previously, source initialization divided the backend pin budget by every
SeqScan descriptor in the query DAG. That count did not represent concurrent
scan activity. Background workers immediately continue a yielded RUN task
until it finishes; RUN completion destroys its local source before another
task starts. For the current runtime's Heap scans, queries with many sequential
scan pipelines therefore received unnecessarily small windows. This does not
justify deleting dynamic pin accounting or changing task scheduling.

The corrected ceiling applies the backend pin allowance and per-query-worker
shared-buffer share to the active source without dividing by all query sources.
The actual additional-pin guard remains authoritative across other pin
users in the same backend. No new budget manager, global lock, scheduler policy or optimizer rule
is introduced. The one-request default remains unchanged.

Batch size is also bounded by half the effective window, with a minimum of one
page. This allows a validated current batch and the next requested batch to
coexist when the real budget permits it. Explicit `scan_prefetch_buffers` and
`scan_prefetch_batch_pages` upper bounds are preserved. A one-page effective
window cannot hold two batches and continues using the existing progress path.

For example, the old Q2 geometry was an 88-page window with an 88-page batch.
After returning the first page, 87 pages remained reserved, so a refill could
submit only one more page. A nominal large request thus alternated with a tiny
one. Geometry, accepted page counts and query elapsed must all be checked;
increasing a reported window alone is not proof of faster reads.

`tests/scan_window.py` reuses the existing fixture, native-result and timeline
validators. A six-alias join creates several scan sources and regresses the
old all-source division. It checks one/four workers, normal geometry, a
17-page window, an explicit 16-page batch cap, same-backend LIMIT/reuse and
empty input. The cached fixture verifies configuration and correctness, not
physical IO throughput. SF100 query profiles provide the actual request-size
and elapsed-time evidence.

Run against the dedicated test server configured with `io_combine_limit=128` and the normal 2GiB Shared Buffers budget:

```sh
PGHOST=/tmp PGPORT=55432 PGUSER=postgres PGDATABASE=tpch \
YAAP_TEST_OUTPUT=/absolute/path/to/scan-window-tests \
python3 tests/scan_window.py
```
