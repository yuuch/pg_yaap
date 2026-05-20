#pragma once

#include <memory>

extern "C" {
struct Query;
}

namespace yaap {

class LogicalOperator;

class LogicalPlanner {
public:
    std::unique_ptr<LogicalOperator> Plan(::Query* pg_query);

private:
    std::unique_ptr<LogicalOperator> CreateInitialPlan(::Query* pg_query);
    std::unique_ptr<LogicalOperator> Normalize(std::unique_ptr<LogicalOperator> plan);
};

} // namespace yaap
