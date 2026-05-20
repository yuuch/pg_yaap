#include "mark_join_normalization.hpp"

#include "logical_utils.hpp"

#include "../adapter/yaap_adapter.hpp"

#include <optional>

namespace yaap {

namespace {

LogicalOperator* SkipTransparentOperators(LogicalOperator* input) {
    while (input && (
               (input->type == LogicalOperatorType::LOGICAL_FILTER && input->children.size() == 1) ||
               (input->type == LogicalOperatorType::LOGICAL_DISTINCT && input->children.size() == 1) ||
               (input->type == LogicalOperatorType::LOGICAL_ORDER && input->children.size() == 1) ||
               (input->type == LogicalOperatorType::LOGICAL_LIMIT && input->children.size() == 1))) {
        input = input->children[0].get();
    }
    return input;
}

bool IsExistsStyleMarkJoinCandidate(LogicalOperator* plan) {
    if (!plan ||
        (plan->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
         plan->type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN &&
         plan->type != LogicalOperatorType::LOGICAL_DELIM_JOIN)) {
        return false;
    }

    auto* join = static_cast<LogicalComparisonJoin*>(plan);
    if (join->join_type != JOIN_MARK || !join->convert_mark_to_semi || plan->children.size() != 2) {
        return false;
    }
    return true;
}

void ConvertMarkJoinToSemiOrAnti(std::unique_ptr<LogicalOperator>& plan, bool positive) {
    auto* join = static_cast<LogicalComparisonJoin*>(plan.get());
    join->join_type = positive ? JOIN_SEMI : JOIN_ANTI;
    join->invert_result = false;
    join->has_mark_index = false;
    join->mark_index = TableIndex{static_cast<size_t>(-1)};

    if (plan->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
        plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        auto* dependent_join = static_cast<LogicalDependentJoin*>(plan.get());
        dependent_join->any_join = false;
    }
}

} // namespace

void DisableMarkJoinConversionForConsumedMarkers(LogicalOperator* plan, std::set<size_t> table_bindings) {
    if (!plan) {
        return;
    }

    switch (plan->type) {
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
            auto* join = static_cast<LogicalComparisonJoin*>(plan);
            if (join->join_type == JOIN_MARK &&
                join->has_mark_index &&
                table_bindings.find(join->mark_index.index) != table_bindings.end()) {
                join->convert_mark_to_semi = false;
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_PROJECTION: {
            auto* projection = static_cast<LogicalProjection*>(plan);
            std::set<size_t> new_bindings;
            for (auto& expression : projection->expressions) {
                CollectReferencedTables(expression.get(), new_bindings);
            }
            table_bindings = std::move(new_bindings);
            break;
        }
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
            auto* aggregate = static_cast<LogicalAggregate*>(plan);
            std::set<size_t> new_bindings;
            for (auto& expression : aggregate->groups) {
                CollectReferencedTables(expression.get(), new_bindings);
            }
            for (auto& expression : aggregate->expressions) {
                CollectReferencedTables(expression.get(), new_bindings);
            }
            table_bindings = std::move(new_bindings);
            break;
        }
        default:
            break;
    }

    for (auto& child : plan->children) {
        DisableMarkJoinConversionForConsumedMarkers(child.get(), table_bindings);
    }
}

bool IsExistsStyleMarkJoinRHS(LogicalOperator* rhs) {
    rhs = SkipTransparentOperators(rhs);
    if (!rhs || rhs->type != LogicalOperatorType::LOGICAL_PROJECTION) {
        return false;
    }

    auto* projection = static_cast<LogicalProjection*>(rhs);
    if (projection->expressions.empty()) {
        return false;
    }
    auto* marker = projection->expressions[0].get();
    return marker && marker->type == ExpressionType::BOUND_CONSTANT;
}

bool ExtractMarkerFilterPolarity(Expression* expression, TableIndex mark_index, bool& positive) {
    if (!expression) {
        return false;
    }

    if (expression->type == ExpressionType::BOUND_COLUMN_REF) {
        auto* column = static_cast<BoundColumnRefExpression*>(expression);
        if (column->binding.table_index.index != mark_index.index) {
            return false;
        }
        positive = true;
        return true;
    }

    if (expression->type == ExpressionType::BOUND_CONJUNCTION) {
        auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
        if (conjunction->bool_expr_type != 2 || conjunction->children.size() != 1) {
            return false;
        }
        if (!ExtractMarkerFilterPolarity(conjunction->children[0].get(), mark_index, positive)) {
            return false;
        }
        positive = !positive;
        return true;
    }

    if (expression->type != ExpressionType::BOUND_FUNCTION) {
        return false;
    }

    auto* function = static_cast<BoundFunctionExpression*>(expression);
    if (function->children.size() != 1 ||
        !ExtractMarkerFilterPolarity(function->children[0].get(), mark_index, positive)) {
        return false;
    }

    if (function->function_name == "is_true" || function->function_name == "is_not_false") {
        return true;
    }
    if (function->function_name == "is_false" || function->function_name == "is_not_true") {
        positive = !positive;
        return true;
    }
    return false;
}

void SimplifyExistsStyleMarkJoinFilter(std::unique_ptr<LogicalOperator>& join_plan,
                                       std::vector<std::unique_ptr<Expression>>& filters) {
    if (!IsExistsStyleMarkJoinCandidate(join_plan.get())) {
        return;
    }

    auto* join = static_cast<LogicalComparisonJoin*>(join_plan.get());
    std::optional<bool> marker_polarity;
    for (const auto& filter : filters) {
        bool positive = false;
        if (!ExtractMarkerFilterPolarity(filter.get(), join->mark_index, positive)) {
            continue;
        }
        if (!marker_polarity.has_value()) {
            marker_polarity = positive;
        } else if (marker_polarity.value() != positive) {
            return;
        }
    }

    if (!marker_polarity.has_value()) {
        return;
    }

    std::vector<std::unique_ptr<Expression>> keep_filters;
    for (auto& filter : filters) {
        bool positive = false;
        if (ExtractMarkerFilterPolarity(filter.get(), join->mark_index, positive)) {
            continue;
        }
        keep_filters.push_back(std::move(filter));
    }
    filters = std::move(keep_filters);

    ConvertMarkJoinToSemiOrAnti(join_plan, marker_polarity.value());
}

void SimplifyExistsStyleMarkJoin(std::unique_ptr<LogicalOperator>& plan) {
    if (!IsExistsStyleMarkJoinCandidate(plan.get())) {
        return;
    }

    auto* join = static_cast<LogicalComparisonJoin*>(plan.get());
    std::set<size_t> left_tables;
    std::set<size_t> right_tables;
    CollectOutputTables(plan->children[0].get(), left_tables);
    CollectOutputTables(plan->children[1].get(), right_tables);
    for (auto& condition : join->conditions) {
        std::set<size_t> referenced_tables;
        CollectReferencedTables(condition.get(), referenced_tables);
        if (referenced_tables.empty() ||
            IsSubset(referenced_tables, left_tables) ||
            IsSubset(referenced_tables, right_tables)) {
            return;
        }
    }

    ConvertMarkJoinToSemiOrAnti(plan, !join->invert_result);
}

} // namespace yaap
