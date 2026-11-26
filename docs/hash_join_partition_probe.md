# Hash join partition-pair probe

`pg_yaap.hash_join_partition_probe` defaults to `on`. With multiple configured
workers, each hash join has a probe-input pipeline breaker. `off` retains the
streaming executor for controlled comparisons; this does not select PostgreSQL
as a fallback.

## Data flow

1. Build workers publish local radix partition chains.
2. Build COMBINE publishes descriptors. Direct segmented builds defer hash
   directory construction until their partition is admitted for probing.
3. The entire probe input is collected into matching radix partitions, using
   worker-local segmented TDC writers. Completed blocks and final tails have
   immutable shared spill backing before consumers start.
4. The paired source claims Build Pi / Probe Pi. It reserves a conservative
   complete working-set estimate, builds that partition's directory, pins the
   required Build segments, and sequentially reads Probe Pi.
5. The existing resumable INNER/SEMI/ANTI/LEFT join implementation produces
   output. Only after all output for the pair has drained are input segments,
   Build segments, row references and the hash directory released.

Independent paired-probe source RUN events are serialized query-wide on the
leader. Within the active source, workers own independent partition pairs.
Other producer/build pipelines remain parallel. This avoids two operators each
admitting half of the same query memory budget.

## Memory and lifecycle

The normal pair budget is at most half the hard query limit and at most the
soft limit. Admission subtracts currently non-evictable memory and leaves a
quarter-limit reserve for other execution state. Build keys/payload pages and
the directory are included in the estimate. Probe page size is chosen from the
worker/partition count and query budget; writable tails also have an LRU bound.

A partition larger than its admission budget is processed alone with bounded
segment read windows. It remains a matching partition pair, but may incur
segment eviction. This is explicitly counted by
`hash_join_oversized_partition_pairs`; recursive repartitioning is not currently
implemented. SF100 has not been validated by this change.

A permanent partition retirement atomically claims the segment before freeing
its resident payload, rather than retaining charged FREE buffers. Shared
cancellation is checked during collection, source processing, and read retries.
RUN completion releases source leases before leader consumer cleanup. Small
source metadata survives until query DSA teardown because FINALIZE may
reconstruct source GlobalState after RUN.

Raw TDC spill images include the reserved row area when a string heap is in
use: the heap begins after `row_capacity`, not after `row_count`. A partial
string-tail roundtrip is covered by the segment self-test.

## Diagnostics and validation

Distinct stages record probe partition append/finalize, pair prepare, sequential
partition input read, pair reload/eviction, and the original join execution.
Their timings are nested and accumulated across workers; they must not be
added as independent wall-clock costs.

`tests/memory_runtime.py` compares native PostgreSQL results for resumable,
scalar and compound-key joins, residual SEMI/ANTI/LEFT joins, independent join
branches, forced oversized pairs, and cancellation followed by backend reuse.
The current 12-worker fixture needs 256MB rather than its historical 64MB setting.
SF10 Q21 output was additionally compared row-for-row with a fresh native
PostgreSQL execution. Results and timings are under `artifacts/q21-pair-*`.
