#include "filter_rewrite_utils.hpp"

#include "../adapter/yaap_adapter.hpp"
#include "../optimizer/optimizer_stats.hpp"

namespace yaap {

namespace {

constexpr int kPgAndExpr = 0;

bool IsProjectionExpressionPushdownSafe(Expression* expression) {
    if (!expression) {
        return false;
    }

    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF:
        case ExpressionType::BOUND_CONSTANT:
            return true;
        default:
            return false;
    }
}

void AppendConjuncts(std::unique_ptr<Expression> expression,
                     std::vector<std::unique_ptr<Expression>>& output) {
    if (!expression) {
        return;
    }
    if (expression->type != ExpressionType::BOUND_CONJUNCTION) {
        output.push_back(std::move(expression));
        return;
    }

    auto* conjunction = static_cast<BoundConjunctionExpression*>(expression.get());
    if (conjunction->bool_expr_type != kPgAndExpr) {
        output.push_back(std::move(expression));
        return;
    }

    for (auto& child : conjunction->children) {
        AppendConjuncts(std::move(child), output);
    }
}

} // namespace

void SplitConjunctionList(std::vector<std::unique_ptr<Expression>>& expressions) {
    std::vector<std::unique_ptr<Expression>> split;
    for (auto& expression : expressions) {
        AppendConjuncts(std::move(expression), split);
    }
    expressions = std::move(split);
}

std::unique_ptr<LogicalOperator> MakeFilterNode(
    std::unique_ptr<LogicalOperator> child,
    std::vector<std::unique_ptr<Expression>> expressions) {
    if (!child || expressions.empty()) {
        return child;
    }

    auto filter = std::make_unique<LogicalFilter>();
    filter->expressions = std::move(expressions);
    filter->children.push_back(std::move(child));
    RelationStatisticsHelper statistics_helper;
    filter->estimated_cardinality = statistics_helper.EstimateFilterCardinality(
        statistics_helper.Extract(*filter->children[0]), filter->expressions);
    return filter;
}

bool CanRewriteProjectionFilter(
    Expression* expression,
    const std::map<std::pair<size_t, size_t>, Expression*>& replacements) {
    if (!expression) {
        return false;
    }

    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* column = static_cast<BoundColumnRefExpression*>(expression);
            auto key = std::make_pair(column->binding.table_index.index, column->binding.column_index.index);
            auto it = replacements.find(key);
            if (it == replacements.end()) {
                return true;
            }
            return IsProjectionExpressionPushdownSafe(it->second);
        }
        case ExpressionType::BOUND_CONSTANT:
            return true;
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            for (auto& child : function->children) {
                if (!CanRewriteProjectionFilter(child.get(), replacements)) {
                    return false;
                }
            }
            return true;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            for (auto& child : conjunction->children) {
                if (!CanRewriteProjectionFilter(child.get(), replacements)) {
                    return false;
                }
            }
            return true;
        }
        default:
            return false;
    }
}

} // namespace yaap
