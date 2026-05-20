#pragma once

/*
 * pipeline/pipeline.hpp
 *
 * Pipeline aggregate (M-FRAME-MIN step 3b). A Pipeline is a maximal chain of
 * PhysicalOperators executable without a blocking boundary: exactly one source,
 * zero or more streaming operators, exactly one sink. Pipelines are produced by
 * MetaPipeline::Build by slicing the PhysicalOperator tree at IsPipelineBreaker
 * boundaries; dependencies (`depends_on`) form the inter-pipeline DAG.
 *
 * All PhysicalOperator* references here are non-owning views into the tree
 * owned by the MetaPipelineBundle (see meta_pipeline.hpp).
 *
 * Spec: PIPELINE_PORT_PLAN.md §15.3.2; GLOBAL_LOCAL_STATE_DESIGN.md §8.3.
 */

#include <cstdint>

#include "core/memory.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

class PhysicalOperator;

struct Pipeline : public PgMemoryContextObject {
	PipelineId                       id          = INVALID_PIPELINE_ID;
	PhysicalOperator                *source      = nullptr;
	PgVector<PhysicalOperator *>     ops;
	PhysicalOperator                *sink        = nullptr;
	PgVector<PipelineId>             depends_on;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
