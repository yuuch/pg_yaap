#pragma once

#include <memory>

namespace yaap {

class LogicalOperator;

enum class PlannerPass {
    DECORRELATE_DEPENDENT_JOIN,
    EXISTS_STYLE_MARK_CLEANUP,
    REDUNDANT_DELIM_CLEANUP
};

class PlannerNormalizer {
public:
    std::unique_ptr<LogicalOperator> Normalize(std::unique_ptr<LogicalOperator> plan);

private:
    void RunPass(PlannerPass pass, std::unique_ptr<LogicalOperator>& plan, bool& changed);
    void RunSubqueryCleanupLoop(std::unique_ptr<LogicalOperator>& plan, bool& changed);
};

} // namespace yaap
