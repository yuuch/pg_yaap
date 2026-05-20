# core/ — Low-Level Primitives & Shared Infrastructure

**Refreshed:** 2026-04-30 (HEAD `6c344eb036d` + uncommitted M-FRAME-MIN step-3g.2-final runtime work; the core/ layer itself is unchanged this cycle).

7 active headers + 1 source file. Foundation layer used by the (in-flight)
PhysicalOperator + MetaPipeline runtime under `parallel/pipeline/`. The
historical legacy serial executor under `src/engine/exec/` has been **deleted
in full** — disregard any pre-greenfield AGENTS note that mentioned it.

## OVERVIEW

Shared data structures, type system, memory management, and parallel
coordination primitives. `DataChunk<N>` (columnar batch), hash table metadata,
DSA bridge for worker↔leader communication, and a robin-hood hash adapter
binding to PostgreSQL allocators. Hash-table-related defs are still present
even though HashJoin has been removed; they remain available for a future
shared-aggregate sink and may be pruned later if unused.

## WHERE TO LOOK

| Task | File | Notes |
|------|------|-------|
| DataChunk definition | `data_chunk.hpp` | Columnar batch: vectors + selection, `DEFAULT_CHUNK_SIZE = 1024` |
| Tuple deform API | `data_chunk_deform.hpp` | JIT and interpreter deform entry points |
| Type system | `types.hpp` | `VecType`, NUMERIC(15,2) as scaled int64, Wide128 handling. Closes its own `namespace pg_yaap` (no namespace leak) |
| Memory allocators | `memory.hpp` | `PgMemoryContextAllocator`, `PgMemoryContextObject`, `VolVecVector` alias |
| Hash table metadata | `hash_table_defs.hpp` | `HashTableInfo`, serialization formats, partition state |
| DSA shared bridge | `parallel_dsa_bridge.{hpp,cpp}` | DSA-backed shared hash bridge, `QueryHashBridgePack` lifecycle. Single `.cpp` in this dir |
| Robin-hood adapter | `robin_hood_pg_adapter.hpp` | Wraps robin-hood-hashing with PostgreSQL palloc/pfree |

## CONVENTIONS

- **DataChunk**: Fixed-size columnar batch (1024 rows default). Each column is
  a typed vector (int64*, double*, etc.) with optional selection mask.
- **Memory**: All allocations use PostgreSQL `MemoryContext`. STL containers
  must use `PgMemoryContextAllocator`. Never raw malloc in hot paths.
- **Type semantics**: NUMERIC(15,2) → scaled int64 with widened int128
  accumulators for aggregation. Wide128 for exact numerics — interpreter-only,
  no JIT (correctness rule).
- **Namespace**: every header in this directory opens `namespace pg_yaap { … }`
  explicitly. The historical "namespace leak via `core/types.hpp`" pattern is
  closed and FORBIDDEN — `types.hpp` closes its own namespace and every
  consumer reopens.
- **DSA lifecycle**: Bridge packs allocated in DSA, published via pointer
  offset. Leader owns pack creation; workers attach read-only. Known issue:
  published packs lack an explicit release point (leak).

## ANTI-PATTERNS

- **Do NOT allocate DataChunk with raw malloc.** Use `MemoryContext` or
  `PgMemoryContextAllocator` for STL.
- **Do NOT assume DSA pointers are stable across detach/reattach.** Convert
  to offsets before storing in shared memory.
- **Do NOT free DSA memory without coordination.** Multiple workers may
  reference the same DSA pack.
- **Do NOT re-open the namespace leak via `types.hpp`.** Every header that
  historically depended on the leak (`memory.hpp`, `hash_table_defs.hpp`,
  `data_chunk.hpp`, `data_chunk_deform.hpp`, `robin_hood_pg_adapter.hpp`,
  `parallel_dsa_bridge.{hpp,cpp}`) must open its own `namespace pg_yaap`
  block.
- **Do NOT reference `src/engine/exec/`.** That directory has been deleted in
  full as part of the greenfield demolition (step 1, `53ac06adcb7`).

## NOTES

- A second copy of `data_chunk_deform.hpp` lives at
  `src/engine/data_chunk_deform.hpp` (one level up). Prefer the `core/` copy
  for new includes; the parent copy will be removed during JIT-wiring
  cleanup.
- Robin-hood hash adapter overrides allocate/deallocate to use palloc/pfree;
  required for `MemoryContext` reclamation.
- DSA bridge packs were originally designed for the (now deleted) parallel
  HashJoin path. They remain in case the pipeline runtime grows a shared
  hash sink; today's runtime stubs do not use them.
- `PipelineSharedControl` lives in `parallel/pipeline/dsm_control.hpp` (DSM-
  resident control block); it is **not** a DSA bridge pack and does not flow
  through `parallel_dsa_bridge.{hpp,cpp}`.
