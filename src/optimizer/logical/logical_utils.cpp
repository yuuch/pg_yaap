#include "logical_utils.hpp"

#include "../adapter/yaap_adapter.hpp"

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

} // namespace

void CollectReferencedTables(Expression* expression, std::set<size_t>& table_indexes) {
    if (!expression) {
        return;
    }

    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* column_ref = static_cast<BoundColumnRefExpression*>(expression);
            table_indexes.insert(column_ref->binding.table_index.index);
            break;
        }
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            for (auto& child : function->children) {
                CollectReferencedTables(child.get(), table_indexes);
            }
            break;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression);
            for (auto& child : aggregate->children) {
                CollectReferencedTables(child.get(), table_indexes);
            }
            break;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            for (auto& child : conjunction->children) {
                CollectReferencedTables(child.get(), table_indexes);
            }
            break;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subquery = static_cast<BoundSubqueryExpression*>(expression);
            for (auto& child : subquery->children) {
                CollectReferencedTables(child.get(), table_indexes);
            }
            break;
        }
        default:
            break;
    }
}

void CollectOutputTables(LogicalOperator* op, std::set<size_t>& table_indexes) {
    if (!op) {
        return;
    }

    switch (op->type) {
        case LogicalOperatorType::LOGICAL_GET: {
            auto* get = static_cast<LogicalGet*>(op);
            table_indexes.insert(get->table_index.index);
            break;
        }
        case LogicalOperatorType::LOGICAL_PROJECTION: {
            auto* projection = static_cast<LogicalProjection*>(op);
            table_indexes.insert(projection->table_index.index);
            for (auto& child : op->children) {
                CollectOutputTables(child.get(), table_indexes);
            }
            return;
        }
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
            auto* aggregate = static_cast<LogicalAggregate*>(op);
            table_indexes.insert(aggregate->group_index.index);
            table_indexes.insert(aggregate->aggregate_index.index);
            for (auto& child : op->children) {
                CollectOutputTables(child.get(), table_indexes);
            }
            return;
        }
        case LogicalOperatorType::LOGICAL_SET_OPERATION: {
            auto* setop = static_cast<LogicalSetOperation*>(op);
            table_indexes.insert(setop->table_index.index);
            for (auto& child : op->children) {
                CollectOutputTables(child.get(), table_indexes);
            }
            return;
        }
        case LogicalOperatorType::LOGICAL_DISTINCT:
        case LogicalOperatorType::LOGICAL_ORDER:
        case LogicalOperatorType::LOGICAL_WINDOW:
        case LogicalOperatorType::LOGICAL_LIMIT: {
            for (auto& child : op->children) {
                CollectOutputTables(child.get(), table_indexes);
            }
            if (op->type == LogicalOperatorType::LOGICAL_WINDOW) {
                auto* window = static_cast<LogicalWindow*>(op);
                table_indexes.insert(window->table_index.index);
            }
            return;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN:
            for (auto& child : op->children) {
                CollectOutputTables(child.get(), table_indexes);
            }
            return;
        case LogicalOperatorType::LOGICAL_DELIM_GET: {
            auto* delim_get = static_cast<LogicalDelimGet*>(op);
            table_indexes.insert(delim_get->table_index.index);
            break;
        }
        default:
            for (auto& child : op->children) {
                CollectOutputTables(child.get(), table_indexes);
            }
            break;
    }
}

bool IsSubset(const std::set<size_t>& candidate, const std::set<size_t>& container) {
    for (auto value : candidate) {
        if (container.find(value) == container.end()) {
            return false;
        }
    }
    return true;
}

std::unique_ptr<Expression> CloneExpressionTree(Expression* expression) {
    if (!expression) {
        return nullptr;
    }

    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* column = static_cast<BoundColumnRefExpression*>(expression);
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
                clone->children.push_back(CloneExpressionTree(child.get()));
            }
            return clone;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression);
            auto clone = std::make_unique<BoundAggregateExpression>(aggregate->function_name, aggregate->agg_oid,
                                                                    aggregate->is_distinct);
            for (auto& child : aggregate->children) {
                clone->children.push_back(CloneExpressionTree(child.get()));
            }
            return clone;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            auto clone = std::make_unique<BoundConjunctionExpression>(conjunction->bool_expr_type);
            for (auto& child : conjunction->children) {
                clone->children.push_back(CloneExpressionTree(child.get()));
            }
            return clone;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subquery = static_cast<BoundSubqueryExpression*>(expression);
            auto clone = std::make_unique<BoundSubqueryExpression>(subquery->sublink_type, subquery->sublink_name);
            for (auto& child : subquery->children) {
                clone->children.push_back(CloneExpressionTree(child.get()));
            }
            return clone;
        }
        default:
            return nullptr;
    }
}

void AppendUniqueFilter(std::vector<std::unique_ptr<Expression>>& filters, std::unique_ptr<Expression> expression) {
    if (!expression) {
        return;
    }

    auto fingerprint = ExpressionFingerprint(expression.get());
    for (const auto& existing : filters) {
        if (ExpressionFingerprint(existing.get()) == fingerprint) {
            return;
        }
    }

    filters.push_back(std::move(expression));
}

} // namespace yaap
