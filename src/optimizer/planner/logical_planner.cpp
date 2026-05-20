#include "logical_planner.hpp"

#include "../adapter/yaap_adapter.hpp"
#include "planner_normalizer.hpp"

namespace yaap {

std::unique_ptr<LogicalOperator> LogicalPlanner::CreateInitialPlan(::Query* pg_query) {
    YaapAdapter adapter;
    return adapter.TranslatePGQuery(pg_query);
}

std::unique_ptr<LogicalOperator> LogicalPlanner::Normalize(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) {
        return nullptr;
    }

    PlannerNormalizer normalizer;
    return normalizer.Normalize(std::move(plan));
}

std::unique_ptr<LogicalOperator> LogicalPlanner::Plan(::Query* pg_query) {
    return Normalize(CreateInitialPlan(pg_query));
}

} // namespace yaap
