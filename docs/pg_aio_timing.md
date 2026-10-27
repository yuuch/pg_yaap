# PostgreSQL AIO timing patch (research artifact)

`patches/postgresql-18-aio-timing.patch` is an optional PostgreSQL 18
instrumentation experiment. It is not required to build or run pg_yaap.

YAAP SeqScan now consumes PostgreSQL's native `ReadStream`; the extension does
not own its `ReadBuffersOperation` handles. As a result, this patch's
owner-local operation timings are not currently connected to YAAP's query
profile. `pg_yaap.profile_scan_io` marks native ReadStream internals as
unavailable rather than reporting those measurements as zero.

Applying the patch changes PostgreSQL internal structure layouts. PostgreSQL
and YAAP must both be rebuilt against matching patched headers and binaries;
the patch is not ABI compatible with an unmodified server. No PostgreSQL core
changes are needed for the normal YAAP ReadStream or ringless scan path.
