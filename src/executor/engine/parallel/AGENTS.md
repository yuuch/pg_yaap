# parallel/ — Pipeline Container

This directory is a **container only**. The greenfield rewrite (commits `53ac06adcb7`, `fd9a8aaf326`) deleted the legacy morsel runtime in full and the new MetaPipeline runtime lives entirely under `pipeline/`.

## STRUCTURE

- `pipeline/` — DuckDB-style `PhysicalOperator` IR + `MetaPipeline` runtime over PostgreSQL DSM/DSA + parallel bgworkers. **The only runtime in the codebase.** See `pipeline/AGENTS.md`.

No source files live at this level. No design docs, no headers, no `.cpp/.hpp`. All built sources are under `pipeline/`.

## WHERE TO LOOK

| Task | Location |
|------|----------|
| Add a new operator / source / sink / event / task | `pipeline/` (see `pipeline/AGENTS.md`) |
| Bridge entry into the runtime | `bridge/execute.cpp` will call `pipeline::PgYaapPipelineRun(...)` (currently a stub returning `false`) |
| Plan → `PhysicalOperator` translation | `pipeline/translator.cpp` (currently returns `nullptr` for every nodeTag) |
| DSM/DSA layout, control block, task queue keys | `pipeline/dsm_control.hpp` (`PIPELINE_DSM_KEY_*`, magic `PIPELINE_DSM_MAGIC = 0x56505043`) |
| Cross-process descriptor IR | `pipeline/pipeline_descriptor.{hpp,cpp}` (Store/LoadSharedPayload, Leader serialize, Worker reconstruct) |
| Worker bgworker `main` | `pipeline/pipeline_worker_main.cpp` (currently `elog(ERROR)` stub) |

## CONVENTIONS

- **Add no `.cpp/.hpp` here.** New code goes under `pipeline/`. This directory exists purely as a namespace anchor and an aggregation point for the per-pipeline AGENTS.md.
- The `pipeline/` runtime is the **only** parallel path. The bridge calls it directly via `bridge/execute.cpp`; there is no other consumer.

## ANTI-PATTERNS

- **Do NOT recreate `parallel_runtime.cpp` or any `runtime_*.{cpp,hpp,inc}`.** Intentionally deleted in `53ac06adcb7`/`fd9a8aaf326`. The MetaPipeline runtime under `pipeline/` is the replacement.
- **Do NOT add a parallel runtime parallel to `pipeline/`.** One runtime only.
- **Do NOT add design docs at this level.** The current authoritative plans live in `.sisyphus/plans/` (`pipeline-port-plan.md`, `3g2-final-delta-map.md`) and the design doc `contrib/pg_yaap/docs/GLOBAL_LOCAL_STATE_DESIGN.md`.
- **Do NOT reintroduce `ParallelAggPartialState`, `ParallelPipelineRole/Desc/Driver/Sink`, `LoweredPipeline`, `WorkerPipelineExecutor`, or any pre-greenfield runtime symbol.** Per-operator shared state in the new world is descriptor-resident in DSA via `pipeline/pipeline_descriptor.cpp` payloads.
