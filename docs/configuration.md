# Configuration Reference

All `pg_yaap` runtime GUCs are registered in [`src/bridge/pg_yaap.c`](../src/bridge/pg_yaap.c) inside `_PG_init()`.

## Complete GUC Reference

| Name | Type | Default | Range / Constraint | Unit | Context | Description |
|---|---|---|---|---|---|---|
| `pg_yaap.enabled` | `bool` | `on` | `on` / `off` | — | `PGC_USERSET` | Master switch for YAAP planner, EXPLAIN, and executor hooks. |
| `pg_yaap.parallel` | `bool` | `on` | `on` / `off` | — | `PGC_USERSET` | Enables parallel pipeline lowering (`false` prevents pipeline plan initialization). |
| `pg_yaap.parallel_max_workers` | `int` | `4` | `0`–`1024` | workers | `PGC_USERSET` | Maximum dynamic PostgreSQL background workers requested per query. |
| `pg_yaap.parallel_min_relation_blocks` | `int` | `1024` | `0`–`INT_MAX` | blocks | `PGC_USERSET` | Minimum relation size in 8 KiB blocks before parallel lowering is considered. |
| `pg_yaap.jit_deform` | `bool` | `on` | `on` / `off` | — | `PGC_USERSET` | Enables LLVM JIT heap tuple deformation into `DataChunk` columns. |
| `pg_yaap.hash_join_partition_probe` | `bool` | `on` | `on` / `off` | — | `PGC_USERSET` | Uses radix-partitioned Hybrid Hash Join when running with multiple workers; `off` uses the non-hybrid streaming probe path. |
| `pg_yaap.memory_limit` | `int` | `256` (`256MB`) | `64`–`INT_MAX / 1024` | MB | `PGC_USERSET` | Hard shared budget for operator breaker payloads (not whole-query RSS). |
| `pg_yaap.memory_soft_limit` | `int` | `-1` | `-1`–`INT_MAX / 1024` | MB | `PGC_USERSET` | Soft threshold that triggers operator spilling; `-1` uses 75% of `pg_yaap.memory_limit`. |
| `pg_yaap.dsa_memory_limit` | `int` | `-1` | `-1`–`INT_MAX / 1024` | MB | `PGC_USERSET` | Ceiling on active PostgreSQL DSA segment capacity; `-1` uses `payload + min(payload / 2, 4 GiB) - external_control`. |
| `pg_yaap.trace_enabled` | `bool` | `off` | `on` / `off` | — | `PGC_USERSET` | Enables lightweight per-process Layer 1 Execution Trace recording. |
| `pg_yaap.trace_buffer_mb` | `int` | `1` (`1MB`) | `1`–`16` | MB | `PGC_USERSET` | Per-process trace ring capacity (allocated once for leader and each worker; query total capped at 64 MB and <= 50% of `dsa_limit`). |
| `pg_yaap.trace_output_dir` | `string` | `""` | Absolute server path, no `..`, `< MAXPGPATH - 96` | — | `PGC_SUSET` | Server-side directory for exported Perfetto Chrome Trace JSON files. Requires superuser or `pg_write_server_files` at export time. |
| `pg_yaap.trace_hooks` | `bool` | `off` | `on` / `off` | — | `PGC_USERSET` | Emits `LOG`-level diagnostic traces on hook entry, admission, and plan lowering. |
| `pg_yaap.trace_execution_path` | `bool` | `off` | `on` / `off` | — | `PGC_USERSET` | Emits one low-noise `LOG` line per executed query summarizing dispatch path. |
| `pg_yaap.parallel_experimental_hash_pipeline` | `bool` | `false` (`off`) | `on` / `off` | — | `PGC_USERSET` | Legacy compatibility GUC; registered with no effect in the current runtime. |

## PostgreSQL Server Settings Interaction

- **`max_worker_processes` / `max_parallel_workers`**: Dynamic background workers requested via `pg_yaap.parallel_max_workers` require available PostgreSQL background worker slots. When testing with `pg_yaap.parallel_max_workers = 12`, configure `max_worker_processes` (e.g., `32` or `64`) high enough to accommodate bgworkers plus autovacuum/I/O workers.
- **`shared_buffers` & `io_method`**: Table scans read through PostgreSQL's Buffer Manager and `ReadStream` API. See [docs/yaap_read_stream.md](yaap_read_stream.md).
- **`pg_yaap.memory_limit` vs. `pg_yaap.dsa_memory_limit` vs. RSS**: See [docs/dsa_memory_limits.md](dsa_memory_limits.md) for the exact accounting boundaries between breaker payloads, DSA segment capacity, and worker-private memory contexts.
