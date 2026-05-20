#pragma once

// Reserved for M-Q1-PERF B.2 (batched columnar predicate path); not wired in B.1.
// B.1 evaluates the qual inline per-tuple inside PhysicalSeqScan; this batched
// API is kept so B.2 can flip on bitmap/dense selvec without re-introducing the
// type-dispatch table.

#include <cstdint>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

/* Vectorized predicate over a deformed qual-only chunk.
 * Replaces per-tuple heap_getattr+EvalQualOnTuple (Q1 SF=10 nocachegetattr 15.4%).
 * NULL row -> false (matches deleted EvalQualOnTuple). NONE -> identity selvec.
 * Returns surviving row count written to selvec_out (dense uint16 indices). */
uint16_t EvalColumnarPredicate(
    const QualDescriptor                            *qual,
    const DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>    &qual_chunk,
    const uint16_t                                  *qual_dst_cols,
    uint16_t                                         count,
    uint16_t                                        *selvec_out);

}
}
