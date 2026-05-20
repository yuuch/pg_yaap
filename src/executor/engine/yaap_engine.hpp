#pragma once

/*
 * Central engine header. Aggregates the small set of types that JIT/expr/core
 * survivors and the pipeline runtime need without dragging in operator state.
 *
 * Intentionally does NOT include any plan-state / query-state / worker-context
 * headers: those types are scoped to their owning subsystem (executor for
 * VecPlanState/VecAggState/VecSeqScanState once M-FRAME-MIN lands, and
 * parallel/pipeline/query_state.hpp for the opaque PgYaapQueryState).
 */

#include "core/types.hpp"
#include "core/memory.hpp"
#include "core/hash_table_defs.hpp"
#include "core/data_chunk.hpp"
#include "core/data_chunk_deform.hpp"
#include "expr/expr.hpp"
