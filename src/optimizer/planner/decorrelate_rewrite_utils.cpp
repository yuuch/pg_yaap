extern "C" {
#include "postgres.h"
}

#include "decorrelate_rewrite_utils.hpp"

#include "../adapter/yaap_adapter.hpp"
#include "../logical/logical_utils.hpp"

#include <map>
#include <sstream>

namespace yaap {

namespace {

std::string ExpressionFingerprint(Expression* expression) {
    if (!expression) {
        return "<null>";
    }

    std::stringstream ss;
    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* column = static_cast<BoundColumnRefExpression*>(expression);
            ss << "col:" << column->binding.table_index.index << "."
               << column->binding.column_index.index;
            break;
        }
        case ExpressionType::BOUND_CONSTANT: {
            auto* constant = static_cast<BoundConstantExpression*>(expression);
            ss << "const:" << (constant->is_null ? "NULL" : constant->value);
            break;
        }
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            ss << "fn:" << function->function_name << "(";
            for (size_t i = 0; i < function->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << ExpressionFingerprint(function->children[i].get());
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression);
            ss << "agg:" << aggregate->function_name << "(";
            for (size_t i = 0; i < aggregate->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << ExpressionFingerprint(aggregate->children[i].get());
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            ss << "conj:" << conjunction->bool_expr_type << "(";
            for (size_t i = 0; i < conjunction->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << ExpressionFingerprint(conjunction->children[i].get());
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subquery = static_cast<BoundSubqueryExpression*>(expression);
            ss << "subquery:" << subquery->sublink_name << "(";
            for (size_t i = 0; i < subquery->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << ExpressionFingerprint(subquery->children[i].get());
            }
            ss << ")";
            break;
        }
        default:
            ss << "opaque";
            break;
    }
    return ss.str();
}

bool IsEqualityFunction(Expression* expression) {
    if (!expression || expression->type != ExpressionType::BOUND_FUNCTION) {
        return false;
    }
    auto* function = static_cast<BoundFunctionExpression*>(expression);
    return function->function_name == "=" && function->children.size() == 2;
}

} // namespace

bool ExpressionReferencesOnlyAllowedTables(Expression* expression, const std::set<size_t>& allowed_tables) {
    if (!expression) {
        return true;
    }

    std::set<size_t> referenced_tables;
    CollectReferencedTables(expression, referenced_tables);
    for (auto table_index : referenced_tables) {
        if (allowed_tables.find(table_index) == allowed_tables.end()) {
            return false;
        }
    }
    return true;
}

std::string DecorrelatedExpressionFingerprint(Expression* expression) {
    return ExpressionFingerprint(expression);
}

std::string DerivedDecorrelatedOutputName(Expression* expression, size_t index) {
    if (!expression) {
        return "group" + std::to_string(index + 1);
    }
    if (expression->type == ExpressionType::BOUND_COLUMN_REF) {
        return static_cast<BoundColumnRefExpression*>(expression)->column_name;
    }
    if (expression->type == ExpressionType::BOUND_AGGREGATE) {
        return static_cast<BoundAggregateExpression*>(expression)->function_name;
    }
    return "group" + std::to_string(index + 1);
}

bool ExtractDecorrelatedEquality(Expression* expression,
                                 const std::set<size_t>& allowed_tables,
                                 std::unique_ptr<Expression>& inner_expr,
                                 std::unique_ptr<Expression>& outer_expr) {
    if (!IsEqualityFunction(expression)) {
        return false;
    }

    auto* function = static_cast<BoundFunctionExpression*>(expression);
    auto* left = function->children[0].get();
    auto* right = function->children[1].get();
    bool left_allowed = ExpressionReferencesOnlyAllowedTables(left, allowed_tables);
    bool right_allowed = ExpressionReferencesOnlyAllowedTables(right, allowed_tables);
    if (left_allowed == right_allowed) {
        return false;
    }

    if (left_allowed) {
        inner_expr = CloneExpressionTree(left);
        outer_expr = CloneExpressionTree(right);
    } else {
        inner_expr = CloneExpressionTree(right);
        outer_expr = CloneExpressionTree(left);
    }
    return inner_expr != nullptr && outer_expr != nullptr;
}

std::vector<OrderByNode> CloneDecorrelatedOrders(const std::vector<OrderByNode>& orders) {
    std::vector<OrderByNode> result;
    result.reserve(orders.size());
    for (const auto& order : orders) {
        OrderByNode clone;
        clone.expression = CloneExpressionTree(order.expression.get());
        result.push_back(std::move(clone));
    }
    return result;
}

std::unique_ptr<Expression> BuildDecorrelatedLimitRowNumberFilter(Expression* limit_count,
                                                                  Expression* limit_offset,
                                                                  TableIndex window_table_index) {
    auto make_rownum_ref = [&]() {
        return std::make_unique<BoundColumnRefExpression>(
            ColumnBinding{window_table_index, ProjectionIndex{0}},
            "window",
            "limit_rownum");
    };

    std::unique_ptr<Expression> condition;
    if (limit_count) {
        std::unique_ptr<Expression> upper_bound;
        if (limit_offset) {
            auto add = std::make_unique<BoundFunctionExpression>("+", InvalidOid);
            add->children.push_back(CloneExpressionTree(limit_count));
            add->children.push_back(CloneExpressionTree(limit_offset));
            upper_bound = std::move(add);
        } else {
            upper_bound = CloneExpressionTree(limit_count);
        }

        auto upper = std::make_unique<BoundFunctionExpression>("<=", InvalidOid);
        upper->children.push_back(make_rownum_ref());
        upper->children.push_back(std::move(upper_bound));
        condition = std::move(upper);
    }

    if (limit_offset) {
        auto lower = std::make_unique<BoundFunctionExpression>(">", InvalidOid);
        lower->children.push_back(make_rownum_ref());
        lower->children.push_back(CloneExpressionTree(limit_offset));
        if (condition) {
            auto conjunction = std::make_unique<BoundConjunctionExpression>(0);
            conjunction->children.push_back(std::move(lower));
            conjunction->children.push_back(std::move(condition));
            condition = std::move(conjunction);
        } else {
            condition = std::move(lower);
        }
    }

    return condition;
}

bool RewriteLiftedFiltersThroughProjection(LogicalProjection& projection,
                                           const std::set<size_t>& allowed_tables,
                                           std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    std::map<std::string, size_t> materialized_indexes;
    for (size_t i = 0; i < projection.expressions.size(); ++i) {
        materialized_indexes.emplace(ExpressionFingerprint(projection.expressions[i].get()), i);
    }

    auto materialize_expression = [&](std::unique_ptr<Expression> inner_expr) -> std::unique_ptr<Expression> {
        auto fingerprint = ExpressionFingerprint(inner_expr.get());
        auto it = materialized_indexes.find(fingerprint);
        size_t projection_index;
        std::string output_name;
        if (it != materialized_indexes.end()) {
            projection_index = it->second;
            output_name = projection_index < projection.output_names.size()
                ? projection.output_names[projection_index]
                : DerivedDecorrelatedOutputName(inner_expr.get(), projection_index);
        } else {
            projection_index = projection.expressions.size();
            output_name = DerivedDecorrelatedOutputName(inner_expr.get(), projection_index);
            projection.expressions.push_back(std::move(inner_expr));
            projection.output_names.push_back(output_name);
            materialized_indexes.emplace(fingerprint, projection_index);
        }
        return std::make_unique<BoundColumnRefExpression>(
            ColumnBinding{projection.table_index, ProjectionIndex{projection_index}},
            "proj",
            output_name);
    };

    std::vector<std::unique_ptr<Expression>> rewritten_filters;
    for (auto& expression : lifted_filters) {
        std::unique_ptr<Expression> inner_expr;
        std::unique_ptr<Expression> outer_expr;
        if (ExtractDecorrelatedEquality(expression.get(), allowed_tables, inner_expr, outer_expr)) {
            auto equality = std::make_unique<BoundFunctionExpression>("=", InvalidOid);
            equality->children.push_back(materialize_expression(std::move(inner_expr)));
            equality->children.push_back(std::move(outer_expr));
            rewritten_filters.push_back(std::move(equality));
            continue;
        }
        if (ExpressionReferencesOnlyAllowedTables(expression.get(), allowed_tables)) {
            return false;
        }
        rewritten_filters.push_back(std::move(expression));
    }
    lifted_filters = std::move(rewritten_filters);
    return true;
}

bool RewriteLiftedFiltersThroughAggregate(LogicalAggregate& aggregate,
                                          const std::set<size_t>& allowed_tables,
                                          std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    std::map<std::string, size_t> group_indexes;
    for (size_t i = 0; i < aggregate.groups.size(); ++i) {
        group_indexes.emplace(ExpressionFingerprint(aggregate.groups[i].get()), i);
    }

    auto materialize_group = [&](std::unique_ptr<Expression> inner_expr) -> std::unique_ptr<Expression> {
        auto fingerprint = ExpressionFingerprint(inner_expr.get());
        auto it = group_indexes.find(fingerprint);
        size_t group_index;
        std::string output_name;
        if (it != group_indexes.end()) {
            group_index = it->second;
            output_name = group_index < aggregate.group_names.size()
                ? aggregate.group_names[group_index]
                : DerivedDecorrelatedOutputName(inner_expr.get(), group_index);
        } else {
            group_index = aggregate.groups.size();
            output_name = DerivedDecorrelatedOutputName(inner_expr.get(), group_index);
            aggregate.groups.push_back(std::move(inner_expr));
            aggregate.group_names.push_back(output_name);
            group_indexes.emplace(fingerprint, group_index);
        }
        return std::make_unique<BoundColumnRefExpression>(
            ColumnBinding{aggregate.group_index, ProjectionIndex{group_index}},
            "agg",
            output_name);
    };

    std::vector<std::unique_ptr<Expression>> rewritten_filters;
    for (auto& expression : lifted_filters) {
        std::unique_ptr<Expression> inner_expr;
        std::unique_ptr<Expression> outer_expr;
        if (ExtractDecorrelatedEquality(expression.get(), allowed_tables, inner_expr, outer_expr)) {
            auto equality = std::make_unique<BoundFunctionExpression>("=", InvalidOid);
            equality->children.push_back(materialize_group(std::move(inner_expr)));
            equality->children.push_back(std::move(outer_expr));
            rewritten_filters.push_back(std::move(equality));
            continue;
        }
        if (ExpressionReferencesOnlyAllowedTables(expression.get(), allowed_tables)) {
            return false;
        }
        rewritten_filters.push_back(std::move(expression));
    }
    lifted_filters = std::move(rewritten_filters);
    return true;
}

bool AddCorrelatedKeysToDistinct(LogicalDistinct& distinct,
                                 const std::set<size_t>& allowed_tables,
                                 const std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    std::set<std::string> distinct_fingerprints;
    for (const auto& expression : distinct.expressions) {
        distinct_fingerprints.insert(ExpressionFingerprint(expression.get()));
    }

    for (const auto& expression : lifted_filters) {
        std::unique_ptr<Expression> inner_expr;
        std::unique_ptr<Expression> outer_expr;
        if (ExtractDecorrelatedEquality(expression.get(), allowed_tables, inner_expr, outer_expr)) {
            auto fingerprint = ExpressionFingerprint(inner_expr.get());
            if (distinct_fingerprints.insert(fingerprint).second) {
                distinct.expressions.push_back(std::move(inner_expr));
            }
            continue;
        }
        if (ExpressionReferencesOnlyAllowedTables(expression.get(), allowed_tables)) {
            return false;
        }
    }
    return true;
}

} // namespace yaap
