#include "delim_join_cleanup.hpp"

#include "../adapter/yaap_adapter.hpp"

namespace yaap {

namespace {

void MarkChanged(bool* changed) {
    if (changed) {
        *changed = true;
    }
}

bool ContainsDelimGet(LogicalOperator* op) {
    if (!op) {
        return false;
    }
    if (op->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
        return true;
    }
    for (auto& child : op->children) {
        if (ContainsDelimGet(child.get())) {
            return true;
        }
    }
    return false;
}

} // namespace

std::unique_ptr<LogicalOperator> CleanupRedundantDelimJoins(std::unique_ptr<LogicalOperator> plan,
                                                            bool* changed) {
    if (!plan) {
        return nullptr;
    }

    for (auto& child : plan->children) {
        child = CleanupRedundantDelimJoins(std::move(child), changed);
    }

    if (plan->type != LogicalOperatorType::LOGICAL_DELIM_JOIN ||
        plan->children.size() != 2) {
        return plan;
    }

    auto* join = static_cast<LogicalComparisonJoin*>(plan.get());
    if (join->dependent || ContainsDelimGet(plan->children[1].get())) {
        return plan;
    }
    if (join->join_type == JOIN_SEMI || join->join_type == JOIN_ANTI) {
        return plan;
    }

    auto* dependent_join = static_cast<LogicalDependentJoin*>(plan.get());
    if (!dependent_join->correlated_columns.empty() || dependent_join->perform_delim) {
        return plan;
    }
    dependent_join->correlated_columns.clear();
    dependent_join->perform_delim = false;
    dependent_join->any_join = false;
    dependent_join->propagate_null_values = false;
    plan->type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
    MarkChanged(changed);
    return plan;
}

} // namespace yaap
