#pragma once

#include <memory>

extern "C" {
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
}

#include "adapter/yaap_adapter.hpp"
#include "optimizer_registry.h"
#include "physical/physical_plan.hpp"

namespace pg_yaap {

struct OptimizerPlanBundle {
	std::unique_ptr<yaap::LogicalOperator> logical_plan;
	std::unique_ptr<yaap::PhysicalOperator> physical_plan;
	List *output_targetlist = nullptr;
};

OptimizerPlanBundle *LookupOptimizerPlanBundle(PlannedStmt *plannedstmt);

}  // namespace pg_yaap
