#include "optimizer_core.hpp"

#include "../logical/logical_utils.hpp"

namespace yaap {

OptimizerPass ScanFilterFolding::Pass() const {
    return OptimizerPass::SCAN_FILTER_FOLDING;
}

std::unique_ptr<LogicalOperator> ScanFilterFolding::FoldIntoGet(std::unique_ptr<LogicalOperator> filter_plan) {
    auto* filter = static_cast<LogicalFilter*>(filter_plan.get());
    auto child = std::move(filter_plan->children[0]);
    if (child->type != LogicalOperatorType::LOGICAL_GET) {
        filter_plan->children[0] = std::move(child);
        return filter_plan;
    }

    auto* get = static_cast<LogicalGet*>(child.get());
    for (auto& expression : filter->expressions) {
        AppendUniqueFilter(get->filters, std::move(expression));
    }
    return child;
}

std::unique_ptr<LogicalOperator> ScanFilterFolding::Rewrite(std::unique_ptr<LogicalOperator> plan) {
    for (auto& child : plan->children) {
        child = Rewrite(std::move(child));
    }

    if (plan->type == LogicalOperatorType::LOGICAL_FILTER &&
        plan->children.size() == 1 &&
        plan->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
        return FoldIntoGet(std::move(plan));
    }

    return plan;
}

std::unique_ptr<LogicalOperator> ScanFilterFolding::Optimize(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) {
        return nullptr;
    }
    return Rewrite(std::move(plan));
}

} // namespace yaap
