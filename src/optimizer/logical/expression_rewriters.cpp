#include "expression_rewriters.hpp"

#include "logical_utils.hpp"

#include "../adapter/yaap_adapter.hpp"

namespace yaap {

std::unique_ptr<Expression> CloneExpressionWithExpressionReplacements(
    Expression* expression,
    const std::map<std::pair<size_t, size_t>, Expression*>& replacements) {
    if (!expression) {
        return nullptr;
    }

    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* column = static_cast<BoundColumnRefExpression*>(expression);
            auto key = std::make_pair(column->binding.table_index.index, column->binding.column_index.index);
            auto it = replacements.find(key);
            if (it != replacements.end()) {
                return CloneExpressionTree(it->second);
            }
            return std::make_unique<BoundColumnRefExpression>(column->binding, column->table_name, column->column_name);
        }
        case ExpressionType::BOUND_CONSTANT: {
            auto* constant = static_cast<BoundConstantExpression*>(expression);
            return std::make_unique<BoundConstantExpression>(constant->value, constant->is_null);
        }
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            auto clone = std::make_unique<BoundFunctionExpression>(function->function_name, function->op_oid);
            for (auto& child : function->children) {
                clone->children.push_back(CloneExpressionWithExpressionReplacements(child.get(), replacements));
            }
            return clone;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression);
            auto clone = std::make_unique<BoundAggregateExpression>(
                aggregate->function_name, aggregate->agg_oid, aggregate->is_distinct);
            for (auto& child : aggregate->children) {
                clone->children.push_back(CloneExpressionWithExpressionReplacements(child.get(), replacements));
            }
            return clone;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            auto clone = std::make_unique<BoundConjunctionExpression>(conjunction->bool_expr_type);
            for (auto& child : conjunction->children) {
                clone->children.push_back(CloneExpressionWithExpressionReplacements(child.get(), replacements));
            }
            return clone;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subquery = static_cast<BoundSubqueryExpression*>(expression);
            auto clone = std::make_unique<BoundSubqueryExpression>(subquery->sublink_type, subquery->sublink_name);
            clone->subquery_plan = nullptr;
            for (auto& child : subquery->children) {
                clone->children.push_back(CloneExpressionWithExpressionReplacements(child.get(), replacements));
            }
            return clone;
        }
        default:
            return nullptr;
    }
}

} // namespace yaap
