#pragma once

#include <map>
#include <vector>

#include "physical_plan.hpp"

namespace yaap {

class PhysicalPlanGenerator {
public:
    std::unique_ptr<PhysicalOperator> Plan(LogicalOperator& op);

private:
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalOperator& op);
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalGet& op);
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalProjection& op);
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalFilter& op);
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalDistinct& op);
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalSetOperation& op);
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalLimit& op);
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalWindow& op);
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalComparisonJoin& op);
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalDependentJoin& op);
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalDelimGet& op);
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalMaterializedCTE& op);
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalCTERef& op);
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalCrossProduct& op);
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalAggregate& op);
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalOrder& op);

    std::vector<Expression*> BorrowExpressions(const std::vector<std::unique_ptr<Expression>>& expressions);
    size_t EstimateCardinality(LogicalOperator& op);

    std::map<size_t, std::vector<PhysicalOperator::OutputColumn>> cte_outputs_;
};

} // namespace yaap
