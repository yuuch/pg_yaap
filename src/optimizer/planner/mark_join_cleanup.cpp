#include "mark_join_cleanup.hpp"

#include "../adapter/yaap_adapter.hpp"
#include "../logical/expression_rewriters.hpp"
#include "../logical/filter_rewrite_utils.hpp"
#include "../logical/logical_utils.hpp"
#include "../logical/mark_join_normalization.hpp"

namespace yaap {

namespace {

void MarkChanged(bool* changed) {
    if (changed) {
        *changed = true;
    }
}

struct JoinShape {
    LogicalOperatorType type;
    int join_type;
    bool has_mark_index;
    size_t mark_index;
    bool invert_result;
    bool any_join;
};

JoinShape CaptureJoinShape(LogicalOperator* plan) {
    auto* join = static_cast<LogicalComparisonJoin*>(plan);
    JoinShape shape{plan->type,
                    join->join_type,
                    join->has_mark_index,
                    join->mark_index.index,
                    join->invert_result,
                    false};
    if (plan->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
        plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        shape.any_join = static_cast<LogicalDependentJoin*>(plan)->any_join;
    }
    return shape;
}

bool JoinShapeChanged(const JoinShape& before, LogicalOperator* after) {
    auto current = CaptureJoinShape(after);
    return before.type != current.type ||
           before.join_type != current.join_type ||
           before.has_mark_index != current.has_mark_index ||
           before.mark_index != current.mark_index ||
           before.invert_result != current.invert_result ||
           before.any_join != current.any_join;
}

std::unique_ptr<LogicalOperator> NormalizeExistsStyleMarkFilters(std::unique_ptr<LogicalOperator> plan,
                                                                 bool* changed);
std::unique_ptr<LogicalOperator> SimplifyExistsStyleMarkJoinsRecursively(std::unique_ptr<LogicalOperator> plan,
                                                                         bool* changed);

std::unique_ptr<LogicalOperator> PushExistsStyleMarkFilterIntoProjection(
    std::unique_ptr<LogicalOperator> filter_plan,
    bool* changed) {
    auto* filter = static_cast<LogicalFilter*>(filter_plan.get());
    SplitConjunctionList(filter->expressions);

    auto projection_plan = std::move(filter_plan->children[0]);
    auto* projection = static_cast<LogicalProjection*>(projection_plan.get());
    if (projection->children.size() != 1 || !projection->children[0]) {
        return MakeFilterNode(std::move(projection_plan), std::move(filter->expressions));
    }

    std::map<std::pair<size_t, size_t>, Expression*> replacements;
    for (size_t i = 0; i < projection->expressions.size(); ++i) {
        replacements.emplace(std::make_pair(projection->table_index.index, i), projection->expressions[i].get());
    }

    std::vector<std::unique_ptr<Expression>> keep_filters;
    std::vector<std::unique_ptr<Expression>> push_filters;
    for (auto& expression : filter->expressions) {
        if (CanRewriteProjectionFilter(expression.get(), replacements)) {
            push_filters.push_back(CloneExpressionWithExpressionReplacements(expression.get(), replacements));
        } else {
            keep_filters.push_back(std::move(expression));
        }
    }

    if (!push_filters.empty()) {
        projection->children[0] = NormalizeExistsStyleMarkFilters(
            MakeFilterNode(std::move(projection->children[0]), std::move(push_filters)), changed);
        projection_plan->estimated_cardinality = projection->children[0]->estimated_cardinality;
        MarkChanged(changed);
    }

    if (keep_filters.empty()) {
        MarkChanged(changed);
        return projection_plan;
    }
    return MakeFilterNode(std::move(projection_plan), std::move(keep_filters));
}

std::unique_ptr<LogicalOperator> PushExistsStyleMarkFilterThroughWrapper(
    std::unique_ptr<LogicalOperator> filter_plan,
    bool* changed) {
    auto* filter = static_cast<LogicalFilter*>(filter_plan.get());
    SplitConjunctionList(filter->expressions);

    auto wrapper_plan = std::move(filter_plan->children[0]);
    if (wrapper_plan->children.size() != 1 || !wrapper_plan->children[0]) {
        return MakeFilterNode(std::move(wrapper_plan), std::move(filter->expressions));
    }

    std::set<size_t> child_tables;
    CollectOutputTables(wrapper_plan->children[0].get(), child_tables);

    std::vector<std::unique_ptr<Expression>> keep_filters;
    std::vector<std::unique_ptr<Expression>> push_filters;
    for (auto& expression : filter->expressions) {
        std::set<size_t> referenced_tables;
        CollectReferencedTables(expression.get(), referenced_tables);
        if (!referenced_tables.empty() && IsSubset(referenced_tables, child_tables)) {
            push_filters.push_back(std::move(expression));
        } else {
            keep_filters.push_back(std::move(expression));
        }
    }

    if (!push_filters.empty()) {
        wrapper_plan->children[0] = NormalizeExistsStyleMarkFilters(
            MakeFilterNode(std::move(wrapper_plan->children[0]), std::move(push_filters)), changed);
        wrapper_plan->estimated_cardinality = wrapper_plan->children[0]->estimated_cardinality;
        MarkChanged(changed);
    }

    if (keep_filters.empty()) {
        MarkChanged(changed);
        return wrapper_plan;
    }
    return MakeFilterNode(std::move(wrapper_plan), std::move(keep_filters));
}

std::unique_ptr<LogicalOperator> NormalizeExistsStyleMarkFilters(std::unique_ptr<LogicalOperator> plan,
                                                                 bool* changed) {
    if (!plan) {
        return nullptr;
    }

    for (auto& child : plan->children) {
        child = NormalizeExistsStyleMarkFilters(std::move(child), changed);
    }

    if (plan->type != LogicalOperatorType::LOGICAL_FILTER ||
        plan->children.size() != 1 ||
        !plan->children[0]) {
        return plan;
    }

    auto* filter = static_cast<LogicalFilter*>(plan.get());
    SplitConjunctionList(filter->expressions);

    auto& child = plan->children[0];
    if (child->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
        child->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
        child->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        auto join_plan = std::move(child);
        auto before_shape = CaptureJoinShape(join_plan.get());
        auto original_filter_count = filter->expressions.size();
        auto keep_filters = std::move(filter->expressions);
        SimplifyExistsStyleMarkJoinFilter(join_plan, keep_filters);
        if (JoinShapeChanged(before_shape, join_plan.get()) ||
            keep_filters.size() != original_filter_count) {
            MarkChanged(changed);
        }
        if (keep_filters.empty()) {
            return join_plan;
        }
        return MakeFilterNode(std::move(join_plan), std::move(keep_filters));
    }

    if (child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
        return PushExistsStyleMarkFilterIntoProjection(std::move(plan), changed);
    }

    if (child->type == LogicalOperatorType::LOGICAL_ORDER ||
        child->type == LogicalOperatorType::LOGICAL_DISTINCT ||
        child->type == LogicalOperatorType::LOGICAL_LIMIT ||
        child->type == LogicalOperatorType::LOGICAL_WINDOW) {
        return PushExistsStyleMarkFilterThroughWrapper(std::move(plan), changed);
    }

    return plan;
}

std::unique_ptr<LogicalOperator> SimplifyExistsStyleMarkJoinsRecursively(std::unique_ptr<LogicalOperator> plan,
                                                                         bool* changed) {
    if (!plan) {
        return nullptr;
    }

    for (auto& child : plan->children) {
        child = SimplifyExistsStyleMarkJoinsRecursively(std::move(child), changed);
    }

    if (plan->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
        plan->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
        plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        auto before_shape = CaptureJoinShape(plan.get());
        SimplifyExistsStyleMarkJoin(plan);
        if (JoinShapeChanged(before_shape, plan.get())) {
            MarkChanged(changed);
        }
    }
    return plan;
}

} // namespace

std::unique_ptr<LogicalOperator> NormalizeExistsStyleMarkJoins(std::unique_ptr<LogicalOperator> plan,
                                                               bool* changed) {
    if (!plan) {
        return nullptr;
    }
    plan = NormalizeExistsStyleMarkFilters(std::move(plan), changed);
    DisableMarkJoinConversionForConsumedMarkers(plan.get());
    return SimplifyExistsStyleMarkJoinsRecursively(std::move(plan), changed);
}

} // namespace yaap
