#pragma once

#include <memory>
#include <string>

extern "C" {
#include "executor/execdesc.h"
}

#include "optimizer_registry.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/query_state.hpp"

namespace pg_yaap {

struct OptimizerPlanSupportStatus {
	bool supported = false;
	std::string path;
	std::string detail;
};

/*
 * Authoritative optimizer->pipeline init/lowering entry. The optimizer-owned
 * physical plan is the only supported execution input on the YAAP path.
 */
std::unique_ptr<pg_yaap::pipeline::PhysicalOperator>
BuildPipelineFromOptimizerPlan(QueryDesc *queryDesc,
							   pg_yaap::PgYaapQueryState *state,
							   const OptimizerPlanBundle &bundle);

OptimizerPlanSupportStatus
AnalyzeOptimizerPlanSupport(const OptimizerPlanBundle &bundle);

std::string
DescribeOptimizerPlan(const OptimizerPlanBundle &bundle);

TupleDesc
BuildOptimizerOutputTupleDesc(const OptimizerPlanBundle &bundle);

}  // namespace pg_yaap
