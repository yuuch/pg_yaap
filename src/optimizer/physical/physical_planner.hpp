#pragma once

#include "physical_plan.hpp"

namespace yaap {

class LogicalOperator;

class PhysicalPlanner {
public:
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalOperator& op);
};

} // namespace yaap
