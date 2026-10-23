# DSA large allocations and memory-limit semantics

YAAP has two separate budgets. `pg_yaap.memory_limit` controls charged shared
breaker storage. `pg_yaap.dsa_memory_limit` controls the total capacity of the
query's active PostgreSQL DSA segments. Neither is a whole-query RSS limit.

## Configuration

```sql
SET pg_yaap.memory_limit = '8GB';
SET pg_yaap.dsa_memory_limit = '20GB';
```

The explicit DSA ceiling is exactly 21,474,836,480 bytes; no metadata allowance
is added on top. It must be at least `memory_limit`. Validation happens before
creating DSM, so invalid settings do not leave a partially created runtime.

The default `dsa_memory_limit=-1` preserves the previous formula:

```
payload budget + min(payload budget / 2, 4 GiB) - external control bytes
```

Consequently, setting only `memory_limit=20GB` still uses approximately 24GiB
of DSA capacity. Set the new DSA GUC explicitly when an exact ceiling is wanted.
The limit is a ceiling, not a startup allocation; DSA initially maps about 4MiB
and grows on demand. Existing soft-limit/spill behavior remains unchanged.

## Individual allocations above 1GiB

PG's ordinary `dsa_allocate`/`dsa_allocate0` reject sizes above `MaxAllocSize`
(1GiB minus 1 byte). PG already supports larger requests through
`DSA_ALLOC_HUGE` in `dsa_allocate_extended`; no PG-core patch is needed.

`core/dsa_allocation.hpp` selects HUGE for valid large requests while preserving
ZERO and NO_OOM flags. Join directories, links, salts, compact references and
aggregate hash arrays use this helper. TDC allocations reuse the helper while
retaining their row-width/heap/offset structural checks. HUGE does not bypass
the DSA area ceiling or make a large array spillable.

## Why capacity differs from actual memory use

The final query log prints `resident`, `peak`, `dsa_total`, `payload_limit`, and
`dsa_limit` separately. `dsa_total` is active segment capacity, including unused
space, allocator metadata and fragmentation. It is not live bytes or RSS.
Freed segments may also remain mapped briefly by another attached worker.

Some allocations are outside the payload counter:

- Legacy Join/aggregate row-reference pages allocate 16 bytes per row, plus
  page directories. 600 million rows require about 8.94GiB for one table.
  Hybrid radix Join no longer builds these full-input tables: it traverses
  independent key/payload segment chains to populate only the current batch's
  compact references.
- Aggregate hash-table arrays and flat TDCs are not completely charged.
- COUNT(DISTINCT) currently uses flat state. Growth holds old/new TDC and
  old/new hash arrays concurrently until copy and rehash finish.

Hybrid resident-batch directories **are charged**; do not classify every Join
array as missing accounting. Worker-private MemoryContexts, JIT memory,
shared_buffers and OS caches are additional to DSA. Thus 20GiB DSA capacity
cannot guarantee 20GiB whole-query memory or machine usage.

## Verification and follow-up

The segment self-test requests more than 1GiB with NO_OOM under a 64MiB area
ceiling, proving HUGE dispatch without allocating a giant mapping. Integration
tests check exact 20GiB configuration, equal payload/DSA limits, invalid limits
and same-backend reuse. SF100 Q4/Q22 provide actual large-allocation coverage.

The SF100 experiment uses 12 workers, 8GiB payload, 20GiB DSA ceiling and 2GiB
shared_buffers on a 24GiB machine, with durable per-query logs and pressure
cancellation. It is not a same-configuration speed comparison to the earlier
8GiB shared_buffers run. Artifacts: `artifacts/sf100-huge-20gb/`.

Probe tails now flush at their per-worker quota or near the hard allocation
limit rather than whenever unrelated resident storage exceeds 75%. Full
segments still spill, and atomic charging still enforces the hard limit.

The current optimized build completed all 22 SF100 queries with profiling off
in 595.561372 seconds using the settings above. Q18 took 103.415615 seconds and
Q21 77.830657 seconds. Output matched previous successful runs; this is not a
full SF100 correctness comparison against PostgreSQL. The server was restarted
before the sweep, without clearing OS caches. Durable results are under
`artifacts/sf100-full-current-20261023/`.

Next priorities are category-level live-byte accounting for remaining row
references, AHTs and flat TDCs, including resize overlap, and spillable distinct
state. Raising the area ceiling alone is not a fix for those lifetime problems.
