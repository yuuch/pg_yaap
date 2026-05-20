#include "optimizer_core.hpp"

#include "../logical/filter_rewrite_utils.hpp"
#include "../logical/logical_utils.hpp"

#include <map>
#include <sstream>

namespace yaap {

namespace {

constexpr int kPgAndExpr = 0;
constexpr int kPgOrExpr = 1;

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
        default:
            ss << "opaque";
            break;
    }
    return ss.str();
}

void ExtractAndConjuncts(std::unique_ptr<Expression> expression,
                         std::vector<std::unique_ptr<Expression>>& conjuncts) {
    if (!expression) {
        return;
    }
    if (expression->type != ExpressionType::BOUND_CONJUNCTION) {
        conjuncts.push_back(std::move(expression));
        return;
    }

    auto* conjunction = static_cast<BoundConjunctionExpression*>(expression.get());
    if (conjunction->bool_expr_type != kPgAndExpr) {
        conjuncts.push_back(std::move(expression));
        return;
    }

    for (auto& child : conjunction->children) {
        ExtractAndConjuncts(std::move(child), conjuncts);
    }
}

std::unique_ptr<Expression> MakeConjunction(int bool_expr_type,
                                            std::vector<std::unique_ptr<Expression>> expressions) {
    if (expressions.empty()) {
        return nullptr;
    }
    if (expressions.size() == 1) {
        return std::move(expressions[0]);
    }

    auto conjunction = std::make_unique<BoundConjunctionExpression>(bool_expr_type);
    conjunction->children = std::move(expressions);
    return conjunction;
}

std::unique_ptr<Expression> FactorCommonDisjunctConjuncts(std::unique_ptr<Expression> expression) {
    if (!expression || expression->type != ExpressionType::BOUND_CONJUNCTION) {
        return expression;
    }

    auto* conjunction = static_cast<BoundConjunctionExpression*>(expression.get());
    for (auto& child : conjunction->children) {
        child = FactorCommonDisjunctConjuncts(std::move(child));
    }

    if (conjunction->bool_expr_type != kPgOrExpr || conjunction->children.size() < 2) {
        return expression;
    }

    std::vector<std::vector<std::unique_ptr<Expression>>> branch_conjuncts;
    branch_conjuncts.reserve(conjunction->children.size());
    for (auto& child : conjunction->children) {
        std::vector<std::unique_ptr<Expression>> conjuncts;
        ExtractAndConjuncts(std::move(child), conjuncts);
        if (conjuncts.empty()) {
            return expression;
        }
        branch_conjuncts.push_back(std::move(conjuncts));
    }

    std::map<std::string, size_t> candidate_counts;
    for (const auto& conjunct : branch_conjuncts[0]) {
        candidate_counts.emplace(ExpressionFingerprint(conjunct.get()), 1);
    }

    for (size_t branch_idx = 1; branch_idx < branch_conjuncts.size(); ++branch_idx) {
        std::set<std::string> branch_fingerprints;
        for (const auto& conjunct : branch_conjuncts[branch_idx]) {
            branch_fingerprints.insert(ExpressionFingerprint(conjunct.get()));
        }
        for (auto it = candidate_counts.begin(); it != candidate_counts.end();) {
            if (branch_fingerprints.find(it->first) == branch_fingerprints.end()) {
                it = candidate_counts.erase(it);
            } else {
                ++it;
            }
        }
    }

    if (candidate_counts.empty()) {
        std::vector<std::unique_ptr<Expression>> rebuilt_children;
        rebuilt_children.reserve(branch_conjuncts.size());
        for (auto& conjuncts : branch_conjuncts) {
            rebuilt_children.push_back(MakeConjunction(kPgAndExpr, std::move(conjuncts)));
        }
        return MakeConjunction(kPgOrExpr, std::move(rebuilt_children));
    }

    std::vector<std::unique_ptr<Expression>> common_conjuncts;
    for (auto& branch : branch_conjuncts) {
        std::vector<std::unique_ptr<Expression>> remaining;
        for (auto& conjunct : branch) {
            auto fingerprint = ExpressionFingerprint(conjunct.get());
            if (candidate_counts.find(fingerprint) != candidate_counts.end()) {
                if (common_conjuncts.size() < candidate_counts.size()) {
                    common_conjuncts.push_back(std::move(conjunct));
                }
                continue;
            }
            remaining.push_back(std::move(conjunct));
        }
        branch = std::move(remaining);
        if (branch.empty()) {
            return MakeConjunction(kPgAndExpr, std::move(common_conjuncts));
        }
    }

    std::vector<std::unique_ptr<Expression>> rebuilt_or_children;
    rebuilt_or_children.reserve(branch_conjuncts.size());
    for (auto& branch : branch_conjuncts) {
        rebuilt_or_children.push_back(MakeConjunction(kPgAndExpr, std::move(branch)));
    }

    common_conjuncts.push_back(MakeConjunction(kPgOrExpr, std::move(rebuilt_or_children)));
    return MakeConjunction(kPgAndExpr, std::move(common_conjuncts));
}

} // namespace

OptimizerPass JoinPredicateExtraction::Pass() const {
    return OptimizerPass::JOIN_PREDICATE_EXTRACTION;
}

bool JoinPredicateExtraction::IsEquiJoinPredicate(Expression* expression,
                                                  const std::set<size_t>& left_tables,
                                                  const std::set<size_t>& right_tables) {
    if (!expression || expression->type != ExpressionType::BOUND_FUNCTION) {
        return false;
    }

    auto* function = static_cast<BoundFunctionExpression*>(expression);
    if (function->function_name != "=" || function->children.size() != 2) {
        return false;
    }

    std::set<size_t> left_expression_tables;
    std::set<size_t> right_expression_tables;
    CollectReferencedTables(function->children[0].get(), left_expression_tables);
    CollectReferencedTables(function->children[1].get(), right_expression_tables);

    bool left_to_right = !left_expression_tables.empty() && !right_expression_tables.empty() &&
                         IsSubset(left_expression_tables, left_tables) &&
                         IsSubset(right_expression_tables, right_tables);
    bool right_to_left = !left_expression_tables.empty() && !right_expression_tables.empty() &&
                         IsSubset(left_expression_tables, right_tables) &&
                         IsSubset(right_expression_tables, left_tables);
    return left_to_right || right_to_left;
}

std::unique_ptr<LogicalOperator> JoinPredicateExtraction::ExtractFromFilter(std::unique_ptr<LogicalOperator> filter_plan) {
    auto* filter = static_cast<LogicalFilter*>(filter_plan.get());
    auto cross_product = std::move(filter_plan->children[0]);
    auto* cross = static_cast<LogicalCrossProduct*>(cross_product.get());

    std::set<size_t> left_tables;
    std::set<size_t> right_tables;
    CollectOutputTables(cross->children[0].get(), left_tables);
    CollectOutputTables(cross->children[1].get(), right_tables);

    std::vector<std::unique_ptr<Expression>> join_conditions;
    std::vector<std::unique_ptr<Expression>> remaining_filters;

    for (auto& expression : filter->expressions) {
        expression = FactorCommonDisjunctConjuncts(std::move(expression));
    }
    SplitConjunctionList(filter->expressions);

    for (auto& expression : filter->expressions) {
        if (IsEquiJoinPredicate(expression.get(), left_tables, right_tables)) {
            join_conditions.push_back(std::move(expression));
        } else {
            remaining_filters.push_back(std::move(expression));
        }
    }

    if (join_conditions.empty()) {
        filter_plan->children[0] = std::move(cross_product);
        return filter_plan;
    }

    auto join = std::make_unique<LogicalComparisonJoin>(0);
    join->conditions = std::move(join_conditions);
    join->children.push_back(std::move(cross->children[0]));
    join->children.push_back(std::move(cross->children[1]));
    join->estimated_cardinality = cross->estimated_cardinality;

    if (!remaining_filters.empty()) {
        auto remaining_filter = std::make_unique<LogicalFilter>();
        remaining_filter->expressions = std::move(remaining_filters);
        remaining_filter->children.push_back(std::move(join));
        remaining_filter->estimated_cardinality = remaining_filter->children[0]->estimated_cardinality;
        return remaining_filter;
    }

    return join;
}

std::unique_ptr<LogicalOperator> JoinPredicateExtraction::Rewrite(std::unique_ptr<LogicalOperator> plan) {
    for (auto& child : plan->children) {
        child = Rewrite(std::move(child));
    }

    if (plan->type == LogicalOperatorType::LOGICAL_FILTER &&
        plan->children.size() == 1 &&
        plan->children[0]->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
        return ExtractFromFilter(std::move(plan));
    }

    return plan;
}

std::unique_ptr<LogicalOperator> JoinPredicateExtraction::Optimize(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) {
        return nullptr;
    }
    return Rewrite(std::move(plan));
}

} // namespace yaap
