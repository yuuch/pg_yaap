extern "C" {
#include "postgres.h"
}

#include "decorrelate_dependent_join.hpp"
#include "decorrelate_rewrite_utils.hpp"

#include "../adapter/yaap_adapter.hpp"
#include "../logical/filter_rewrite_utils.hpp"
#include "../logical/logical_utils.hpp"
#include "../optimizer/optimizer_stats.hpp"

#include <algorithm>
#include <map>
#include <cmath>

namespace yaap {

namespace {

void MarkChanged(bool* changed) {
    if (changed) {
        *changed = true;
    }
}

bool ExpressionsReferenceOnlyAllowedTables(const std::vector<std::unique_ptr<Expression>>& expressions,
                                           const std::set<size_t>& allowed_tables) {
    for (const auto& expression : expressions) {
        if (!ExpressionReferencesOnlyAllowedTables(expression.get(), allowed_tables)) {
            return false;
        }
    }
    return true;
}

bool OrdersReferenceOnlyAllowedTables(const LogicalOrder& order, const std::set<size_t>& allowed_tables) {
    for (const auto& entry : order.orders) {
        if (!ExpressionReferencesOnlyAllowedTables(entry.expression.get(), allowed_tables)) {
            return false;
        }
    }
    return true;
}

size_t FindBindingDistinctUpperBound(LogicalOperator& op,
                                     ColumnBinding binding,
                                     const RelationStatisticsHelper& statistics_helper) {
    size_t best = 0;

    auto stats = statistics_helper.Extract(op);
    auto column = statistics_helper.LookupColumnStats(stats, binding);
    if (column.has_stats && column.distinct.distinct_count > 0) {
        best = std::max<size_t>(
            1,
            std::min(column.distinct.distinct_count, std::max<size_t>(1, stats.cardinality)));
    }

    for (const auto& child : op.children) {
        if (!child) {
            continue;
        }
        auto child_best = FindBindingDistinctUpperBound(*child, binding, statistics_helper);
        if (child_best == 0) {
            continue;
        }
        best = best == 0 ? child_best : std::min(best, child_best);
    }

    return best;
}

std::unique_ptr<Expression> CloneExpressionWithBindingReplacements(
    Expression* expression,
    const std::map<std::pair<size_t, size_t>, ColumnBinding>& replacements) {
    if (!expression) {
        return nullptr;
    }

    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* column = static_cast<BoundColumnRefExpression*>(expression);
            auto key = std::make_pair(column->binding.table_index.index, column->binding.column_index.index);
            auto it = replacements.find(key);
            if (it != replacements.end()) {
                return std::make_unique<BoundColumnRefExpression>(it->second, column->table_name, column->column_name);
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
                clone->children.push_back(CloneExpressionWithBindingReplacements(child.get(), replacements));
            }
            return clone;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression);
            auto clone = std::make_unique<BoundAggregateExpression>(
                aggregate->function_name, aggregate->agg_oid, aggregate->is_distinct);
            for (auto& child : aggregate->children) {
                clone->children.push_back(CloneExpressionWithBindingReplacements(child.get(), replacements));
            }
            return clone;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            auto clone = std::make_unique<BoundConjunctionExpression>(conjunction->bool_expr_type);
            for (auto& child : conjunction->children) {
                clone->children.push_back(CloneExpressionWithBindingReplacements(child.get(), replacements));
            }
            return clone;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subquery = static_cast<BoundSubqueryExpression*>(expression);
            auto clone = std::make_unique<BoundSubqueryExpression>(subquery->sublink_type, subquery->sublink_name);
            clone->subquery_plan = nullptr;
            for (auto& child : subquery->children) {
                clone->children.push_back(CloneExpressionWithBindingReplacements(child.get(), replacements));
            }
            return clone;
        }
        default:
            return nullptr;
    }
}

bool SubtreeCoversTables(LogicalOperator* op, const std::set<size_t>& required_tables) {
    if (!op) {
        return false;
    }
    std::set<size_t> output_tables;
    CollectOutputTables(op, output_tables);
    for (auto table_index : required_tables) {
        if (output_tables.find(table_index) == output_tables.end()) {
            return false;
        }
    }
    return true;
}

bool IsEqualityColumnComparison(Expression* expression,
                                BoundColumnRefExpression*& left,
                                BoundColumnRefExpression*& right) {
    left = nullptr;
    right = nullptr;
    if (!expression || expression->type != ExpressionType::BOUND_FUNCTION) {
        return false;
    }
    auto* function = static_cast<BoundFunctionExpression*>(expression);
    if (function->function_name != "=" || function->children.size() != 2) {
        return false;
    }
    left = dynamic_cast<BoundColumnRefExpression*>(function->children[0].get());
    right = dynamic_cast<BoundColumnRefExpression*>(function->children[1].get());
    return left != nullptr && right != nullptr;
}

void CollectSubtreeReferencedTables(LogicalOperator* op, std::set<size_t>& out) {
    if (!op) {
        return;
    }
    CollectOutputTables(op, out);
    switch (op->type) {
        case LogicalOperatorType::LOGICAL_GET: {
            auto* get = static_cast<LogicalGet*>(op);
            for (auto& expression : get->filters) {
                CollectReferencedTables(expression.get(), out);
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_FILTER: {
            auto* filter = static_cast<LogicalFilter*>(op);
            for (auto& expression : filter->expressions) {
                CollectReferencedTables(expression.get(), out);
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_PROJECTION: {
            auto* projection = static_cast<LogicalProjection*>(op);
            for (auto& expression : projection->expressions) {
                CollectReferencedTables(expression.get(), out);
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
            auto* aggregate = static_cast<LogicalAggregate*>(op);
            for (auto& expression : aggregate->groups) {
                CollectReferencedTables(expression.get(), out);
            }
            for (auto& expression : aggregate->expressions) {
                CollectReferencedTables(expression.get(), out);
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
            auto* join = static_cast<LogicalComparisonJoin*>(op);
            for (auto& expression : join->conditions) {
                CollectReferencedTables(expression.get(), out);
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_DISTINCT: {
            auto* distinct = static_cast<LogicalDistinct*>(op);
            for (auto& expression : distinct->expressions) {
                CollectReferencedTables(expression.get(), out);
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_ORDER: {
            auto* order = static_cast<LogicalOrder*>(op);
            for (auto& entry : order->orders) {
                CollectReferencedTables(entry.expression.get(), out);
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_WINDOW: {
            auto* window = static_cast<LogicalWindow*>(op);
            for (auto& expression : window->partitions) {
                CollectReferencedTables(expression.get(), out);
            }
            for (auto& entry : window->orders) {
                CollectReferencedTables(entry.expression.get(), out);
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_LIMIT: {
            auto* limit = static_cast<LogicalLimit*>(op);
            CollectReferencedTables(limit->limit_count.get(), out);
            CollectReferencedTables(limit->limit_offset.get(), out);
            break;
        }
        default:
            break;
    }
    for (auto& child : op->children) {
        CollectSubtreeReferencedTables(child.get(), out);
    }
}

bool BuildSemiJoinVisibleBindingReplacements(
    const LogicalComparisonJoin& join,
    std::set<size_t>& hidden_tables,
    std::map<std::pair<size_t, size_t>, ColumnBinding>& replacements) {
    hidden_tables.clear();
    replacements.clear();
    if ((join.join_type != JOIN_SEMI && join.join_type != JOIN_ANTI) || join.children.size() != 2 ||
        !join.children[0] || !join.children[1]) {
        return true;
    }

    std::set<size_t> visible_tables;
    CollectOutputTables(join.children[0].get(), visible_tables);
    CollectOutputTables(join.children[1].get(), hidden_tables);

    for (const auto& condition : join.conditions) {
        BoundColumnRefExpression* left = nullptr;
        BoundColumnRefExpression* right = nullptr;
        if (!IsEqualityColumnComparison(condition.get(), left, right)) {
            continue;
        }
        const bool left_visible = visible_tables.find(left->binding.table_index.index) != visible_tables.end();
        const bool right_visible = visible_tables.find(right->binding.table_index.index) != visible_tables.end();
        if (left_visible && !right_visible) {
            replacements[std::make_pair(right->binding.table_index.index, right->binding.column_index.index)] =
                left->binding;
        } else if (right_visible && !left_visible) {
            replacements[std::make_pair(left->binding.table_index.index, left->binding.column_index.index)] =
                right->binding;
        }
    }
    return true;
}

bool RewriteSemiJoinExpressionsToVisibleOutputs(
    const std::set<size_t>& hidden_tables,
    const std::map<std::pair<size_t, size_t>, ColumnBinding>& replacements,
    std::vector<std::unique_ptr<Expression>>& expressions,
    bool drop_tautologies) {
    if (expressions.empty() || replacements.empty()) {
        return true;
    }

    std::vector<std::unique_ptr<Expression>> rewritten_filters;
    rewritten_filters.reserve(expressions.size());
    for (auto& expression : expressions) {
        auto rewritten = CloneExpressionWithBindingReplacements(expression.get(), replacements);
        if (!rewritten) {
            return false;
        }
        BoundColumnRefExpression* left = nullptr;
        BoundColumnRefExpression* right = nullptr;
        if (drop_tautologies &&
            IsEqualityColumnComparison(rewritten.get(), left, right) &&
            left->binding.table_index.index == right->binding.table_index.index &&
            left->binding.column_index.index == right->binding.column_index.index) {
            continue;
        }
        std::set<size_t> referenced_tables;
        CollectReferencedTables(rewritten.get(), referenced_tables);
        for (auto table_index : referenced_tables) {
            if (hidden_tables.find(table_index) != hidden_tables.end()) {
                return false;
            }
        }
        rewritten_filters.push_back(std::move(rewritten));
    }
    expressions = std::move(rewritten_filters);
    return true;
}

void KeepSemiJoinHiddenLiftedFiltersOnRight(
    const std::set<size_t>& allowed_tables,
    LogicalComparisonJoin& join,
    std::unique_ptr<LogicalOperator>& right_child,
    std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    if ((join.join_type != JOIN_SEMI && join.join_type != JOIN_ANTI) || !right_child || lifted_filters.empty()) {
        return;
    }

    std::set<size_t> right_subtree_tables;
    CollectSubtreeReferencedTables(right_child.get(), right_subtree_tables);

    std::vector<std::unique_ptr<Expression>> hidden_filters;
    std::vector<std::unique_ptr<Expression>> keep_filters;
    hidden_filters.reserve(lifted_filters.size());
    keep_filters.reserve(lifted_filters.size());
    for (auto& expression : lifted_filters) {
        std::set<size_t> referenced_tables;
        CollectReferencedTables(expression.get(), referenced_tables);
        if (!referenced_tables.empty() && IsSubset(referenced_tables, right_subtree_tables)) {
            hidden_filters.push_back(std::move(expression));
        } else {
            keep_filters.push_back(std::move(expression));
        }
    }
    if (!hidden_filters.empty()) {
        right_child = MakeFilterNode(std::move(right_child), std::move(hidden_filters));
    }
    lifted_filters = std::move(keep_filters);
}

std::unique_ptr<LogicalOperator>* FindDeepestCoveringSubtree(
    std::unique_ptr<LogicalOperator>& plan,
    const std::set<size_t>& required_tables) {
    if (!plan || !SubtreeCoversTables(plan.get(), required_tables)) {
        return nullptr;
    }
    for (auto& child : plan->children) {
        if (auto* result = FindDeepestCoveringSubtree(child, required_tables)) {
            return result;
        }
    }
    return &plan;
}

struct ParsedLiftedEquality {
    std::string key;
    std::string inner_fingerprint;
    std::unique_ptr<Expression> inner_expr;
    std::unique_ptr<Expression> outer_expr;
};

ssize_t FindProjectionExpressionIndex(LogicalProjection& projection, const std::string& fingerprint);
bool ParseLiftedEquality(Expression* expression,
                         const std::set<size_t>& allowed_tables,
                         ParsedLiftedEquality& parsed);

bool CollectUnaryBranchEqualities(LogicalProjection& projection,
                                  LogicalOperator* op,
                                  const std::set<size_t>& branch_allowed_tables,
                                  std::map<std::string, size_t>& equalities,
                                  std::map<std::string, ParsedLiftedEquality>* parsed_equalities) {
    bool found = false;
    while (op) {
        if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
            auto* filter = static_cast<LogicalFilter*>(op);
            for (const auto& expression : filter->expressions) {
                if (ExpressionReferencesOnlyAllowedTables(expression.get(), branch_allowed_tables)) {
                    return false;
                }
                ParsedLiftedEquality parsed;
                if (!ParseLiftedEquality(expression.get(), branch_allowed_tables, parsed)) {
                    return false;
                }
                ssize_t projection_index = FindProjectionExpressionIndex(projection, parsed.inner_fingerprint);
                if (projection_index < 0) {
                    return false;
                }
                auto key = std::to_string(projection_index) + "==" +
                    DecorrelatedExpressionFingerprint(parsed.outer_expr.get());
                if (equalities.find(key) != equalities.end()) {
                    return false;
                }
                equalities.emplace(key, static_cast<size_t>(projection_index));
                if (parsed_equalities) {
                    parsed_equalities->emplace(std::move(key), std::move(parsed));
                }
                found = true;
            }
        } else if (op->type == LogicalOperatorType::LOGICAL_GET) {
            auto* get = static_cast<LogicalGet*>(op);
            for (const auto& expression : get->filters) {
                if (ExpressionReferencesOnlyAllowedTables(expression.get(), branch_allowed_tables)) {
                    return false;
                }
                ParsedLiftedEquality parsed;
                if (!ParseLiftedEquality(expression.get(), branch_allowed_tables, parsed)) {
                    return false;
                }
                ssize_t projection_index = FindProjectionExpressionIndex(projection, parsed.inner_fingerprint);
                if (projection_index < 0) {
                    return false;
                }
                auto key = std::to_string(projection_index) + "==" +
                    DecorrelatedExpressionFingerprint(parsed.outer_expr.get());
                if (equalities.find(key) != equalities.end()) {
                    return false;
                }
                equalities.emplace(key, static_cast<size_t>(projection_index));
                if (parsed_equalities) {
                    parsed_equalities->emplace(std::move(key), std::move(parsed));
                }
                found = true;
            }
            return found;
        }

        if (op->children.size() != 1) {
            return false;
        }
        op = op->children[0].get();
    }
    return false;
}

LogicalOperator* FindUnaryLeafRoot(LogicalOperator* op) {
    while (op && op->type != LogicalOperatorType::LOGICAL_GET) {
        if (op->children.size() != 1) {
            return nullptr;
        }
        op = op->children[0].get();
    }
    return op;
}

bool ParseLiftedEquality(Expression* expression,
                         const std::set<size_t>& allowed_tables,
                         ParsedLiftedEquality& parsed) {
    std::unique_ptr<Expression> inner_expr;
    std::unique_ptr<Expression> outer_expr;
    if (!ExtractDecorrelatedEquality(expression, allowed_tables, inner_expr, outer_expr)) {
        return false;
    }

    parsed.inner_fingerprint = DecorrelatedExpressionFingerprint(inner_expr.get());
    auto outer_fingerprint = DecorrelatedExpressionFingerprint(outer_expr.get());
    parsed.key = parsed.inner_fingerprint + "==" + outer_fingerprint;
    parsed.inner_expr = std::move(inner_expr);
    parsed.outer_expr = std::move(outer_expr);
    return true;
}

ssize_t FindProjectionExpressionIndex(LogicalProjection& projection, const std::string& fingerprint) {
    for (size_t idx = 0; idx < projection.expressions.size(); ++idx) {
        if (DecorrelatedExpressionFingerprint(projection.expressions[idx].get()) == fingerprint) {
            return static_cast<ssize_t>(idx);
        }
    }
    return -1;
}

} // namespace

bool DecorrelateDependentJoin::DecorrelateScalarAggregateJoin(std::unique_ptr<LogicalOperator>& plan) {
    if (!plan || (plan->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
                  plan->type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN &&
                  plan->type != LogicalOperatorType::LOGICAL_DELIM_JOIN)) {
        return false;
    }

    auto* join = static_cast<LogicalComparisonJoin*>(plan.get());
    auto* dependent_join = static_cast<LogicalDependentJoin*>(plan.get());
    if (!join->dependent || join->join_type != JOIN_SINGLE || plan->children.size() != 2) {
        return false;
    }

    if (!plan->children[1]) {
        return false;
    }

    auto* aggregate_root = plan->children[1].get();
    while (aggregate_root &&
           aggregate_root->type == LogicalOperatorType::LOGICAL_PROJECTION &&
           aggregate_root->children.size() == 1) {
        aggregate_root = aggregate_root->children[0].get();
    }

    if (!aggregate_root || aggregate_root->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
        return false;
    }

    auto* aggregate = static_cast<LogicalAggregate*>(aggregate_root);
    if (aggregate->children.size() != 1) {
        return false;
    }

    if (ContainsBlockingOperator(aggregate->children[0].get())) {
        return false;
    }

    std::set<size_t> allowed_tables;
    CollectOutputTables(aggregate->children[0].get(), allowed_tables);

    struct Correlation {
        std::unique_ptr<Expression> inner;
        std::unique_ptr<Expression> outer;
    };
    std::vector<Correlation> correlations;

    auto analyze_expression_list =
        [&](std::vector<std::unique_ptr<Expression>>& expressions) -> bool {
            SplitConjunctions(expressions);
            for (auto& expression : expressions) {
                if (ExpressionReferencesAllowedTables(expression.get(), allowed_tables)) {
                    continue;
                }

                std::unique_ptr<Expression> inner_expr;
                std::unique_ptr<Expression> outer_expr;
                if (!ExtractDecorrelatedEquality(expression.get(), allowed_tables, inner_expr, outer_expr)) {
                    return false;
                }
                correlations.push_back({std::move(inner_expr), std::move(outer_expr)});
            }
            return true;
        };

    auto analyze = [&](auto&& self, LogicalOperator* op) -> bool {
        if (!op) {
            return true;
        }

        for (auto& child : op->children) {
            if (!self(self, child.get())) {
                return false;
            }
        }

        if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
            auto* filter = static_cast<LogicalFilter*>(op);
            return analyze_expression_list(filter->expressions);
        }

        if (op->type == LogicalOperatorType::LOGICAL_GET) {
            auto* get = static_cast<LogicalGet*>(op);
            return analyze_expression_list(get->filters);
        }

        if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
            op->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
            op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
            auto* nested_join = static_cast<LogicalComparisonJoin*>(op);
            return analyze_expression_list(nested_join->conditions);
        }

        if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
            auto* projection = static_cast<LogicalProjection*>(op);
            for (auto& expression : projection->expressions) {
                if (!ExpressionReferencesAllowedTables(expression.get(), allowed_tables)) {
                    return false;
                }
            }
            return true;
        }

        return true;
    };

    if (!analyze(analyze, aggregate->children[0].get())) {
        return false;
    }

    if (correlations.empty()) {
        return false;
    }

    std::set<size_t> correlation_inner_tables;
    for (const auto& correlation : correlations) {
        CollectReferencedTables(correlation.inner.get(), correlation_inner_tables);
    }
    if (correlation_inner_tables.empty()) {
        return false;
    }

    std::vector<std::unique_ptr<Expression>> lifted_filters;
    if (!PushDownCorrelatedNode(aggregate->children[0], allowed_tables, lifted_filters)) {
        return false;
    }

    RelationStatisticsHelper statistics_helper;
    auto outer_stats = plan->children[0] ? statistics_helper.Extract(*plan->children[0]) : RelationStats{};
    size_t delim_cardinality = std::max<size_t>(1, outer_stats.cardinality);
    double correlated_distinct = 1.0;
    bool used_outer_stats = false;
    for (const auto& correlation : correlations) {
        if (!correlation.outer || correlation.outer->type != ExpressionType::BOUND_COLUMN_REF) {
            continue;
        }
        auto binding = static_cast<BoundColumnRefExpression*>(correlation.outer.get())->binding;
        auto distinct_upper_bound = plan->children[0]
            ? FindBindingDistinctUpperBound(*plan->children[0], binding, statistics_helper)
            : 0;
        if (distinct_upper_bound == 0) {
            auto column = statistics_helper.LookupColumnStats(outer_stats, binding);
            if (!column.has_stats || column.distinct.distinct_count == 0) {
                continue;
            }
            distinct_upper_bound = column.distinct.distinct_count;
        }
        correlated_distinct *= static_cast<double>(distinct_upper_bound);
        used_outer_stats = true;
    }
    if (used_outer_stats) {
        delim_cardinality = std::min(
            delim_cardinality,
            std::max<size_t>(1, static_cast<size_t>(std::ceil(correlated_distinct))));
    }

    auto delim_table_index = AllocateTableIndex();
    auto delim_get = std::make_unique<LogicalDelimGet>(delim_table_index);
    delim_get->estimated_cardinality = delim_cardinality;
    std::vector<ColumnBinding> delim_bindings;
    delim_bindings.reserve(correlations.size());
    std::vector<std::string> delim_output_names;
    delim_output_names.reserve(correlations.size());
    std::map<std::pair<size_t, size_t>, ColumnBinding> outer_binding_replacements;
    for (size_t idx = 0; idx < correlations.size(); ++idx) {
        auto output_name = DerivedDecorrelatedOutputName(correlations[idx].outer.get(), idx);
        ColumnBinding delim_binding{delim_table_index, ProjectionIndex{idx}};
        delim_get->correlated_columns.push_back(delim_binding);
        delim_get->output_names.push_back(output_name);
        delim_bindings.push_back(delim_binding);
        delim_output_names.push_back(output_name);
        if (correlations[idx].outer && correlations[idx].outer->type == ExpressionType::BOUND_COLUMN_REF) {
            auto outer_binding = static_cast<BoundColumnRefExpression*>(correlations[idx].outer.get())->binding;
            outer_binding_replacements[std::make_pair(
            outer_binding.table_index.index,
            outer_binding.column_index.index)] = delim_binding;
        }
    }

    auto* target_subtree = FindDeepestCoveringSubtree(aggregate->children[0], correlation_inner_tables);
    if (!target_subtree) {
        return false;
    }
    const size_t target_cardinality = *target_subtree ? (*target_subtree)->estimated_cardinality : 0;
    auto delim_cross = std::make_unique<LogicalCrossProduct>();
    delim_cross->estimated_cardinality = target_cardinality > 0
        ? target_cardinality * delim_cardinality
        : delim_cardinality;
    delim_cross->children.push_back(std::move(*target_subtree));
    delim_cross->children.push_back(std::move(delim_get));
    if (!lifted_filters.empty()) {
        auto delim_filter = std::make_unique<LogicalFilter>();
        delim_filter->estimated_cardinality = target_cardinality > 0 ? target_cardinality : delim_cardinality;
        for (const auto& lifted : lifted_filters) {
            auto rewritten = CloneExpressionWithBindingReplacements(lifted.get(), outer_binding_replacements);
            if (rewritten) {
                delim_filter->expressions.push_back(std::move(rewritten));
            }
        }
        delim_filter->children.push_back(std::move(delim_cross));
        *target_subtree = std::move(delim_filter);
    } else {
        *target_subtree = std::move(delim_cross);
    }

    auto add_group_key = [&](std::unique_ptr<Expression> inner_expr) -> size_t {
        auto fingerprint = DecorrelatedExpressionFingerprint(inner_expr.get());
        for (size_t idx = 0; idx < aggregate->groups.size(); ++idx) {
            if (DecorrelatedExpressionFingerprint(aggregate->groups[idx].get()) == fingerprint) {
                return idx;
            }
        }

        size_t group_idx = aggregate->groups.size();
        aggregate->group_names.push_back(DerivedDecorrelatedOutputName(inner_expr.get(), group_idx));
        aggregate->groups.push_back(std::move(inner_expr));
        return group_idx;
    };

    for (size_t idx = 0; idx < correlations.size(); ++idx) {
        auto delim_ref = std::make_unique<BoundColumnRefExpression>(
            delim_bindings[idx],
            "delim",
            idx < delim_output_names.size()
                ? delim_output_names[idx]
                : DerivedDecorrelatedOutputName(correlations[idx].outer.get(), idx));
        size_t group_idx = add_group_key(std::move(delim_ref));
        std::string group_name = group_idx < aggregate->group_names.size()
            ? aggregate->group_names[group_idx]
            : DerivedDecorrelatedOutputName(nullptr, group_idx);
        auto group_ref = std::make_unique<BoundColumnRefExpression>(
            ColumnBinding{aggregate->group_index, ProjectionIndex{group_idx}},
            "agg",
            group_name);
        auto condition = std::make_unique<BoundFunctionExpression>("=", InvalidOid);
        condition->children.push_back(std::move(correlations[idx].outer));
        condition->children.push_back(std::move(group_ref));
        AppendUniqueFilter(join->conditions, std::move(condition));
    }

    join->dependent = false;
    if (plan->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
        plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        auto decorrelated_type =
            (join->join_type == JOIN_MARK ||
             join->join_type == JOIN_SEMI ||
             join->join_type == JOIN_ANTI ||
             join->join_type == JOIN_SINGLE)
                ? LogicalOperatorType::LOGICAL_DELIM_JOIN
                : (dependent_join->correlated_columns.empty() || !dependent_join->perform_delim
                       ? LogicalOperatorType::LOGICAL_COMPARISON_JOIN
                       : LogicalOperatorType::LOGICAL_DELIM_JOIN);
        plan->type = decorrelated_type;
        if (decorrelated_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
            dependent_join->correlated_columns.clear();
            dependent_join->perform_delim = false;
        }
    }
    return true;
}

bool DecorrelateDependentJoin::ExpressionReferencesAllowedTables(
    Expression* expression,
    const std::set<size_t>& allowed_tables) const {
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

bool DecorrelateDependentJoin::SubtreeHasCorrelatedReferences(
    LogicalOperator* op,
    const std::set<size_t>& allowed_tables) const {
    if (!op) {
        return false;
    }

    switch (op->type) {
        case LogicalOperatorType::LOGICAL_GET: {
            auto* get = static_cast<LogicalGet*>(op);
            if (!ExpressionsReferenceOnlyAllowedTables(get->filters, allowed_tables)) {
                return true;
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_FILTER: {
            auto* filter = static_cast<LogicalFilter*>(op);
            if (!ExpressionsReferenceOnlyAllowedTables(filter->expressions, allowed_tables)) {
                return true;
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_PROJECTION: {
            auto* projection = static_cast<LogicalProjection*>(op);
            if (!ExpressionsReferenceOnlyAllowedTables(projection->expressions, allowed_tables)) {
                return true;
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
            auto* aggregate = static_cast<LogicalAggregate*>(op);
            if (!ExpressionsReferenceOnlyAllowedTables(aggregate->groups, allowed_tables) ||
                !ExpressionsReferenceOnlyAllowedTables(aggregate->expressions, allowed_tables)) {
                return true;
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_SET_OPERATION:
            break;
        case LogicalOperatorType::LOGICAL_DISTINCT: {
            auto* distinct = static_cast<LogicalDistinct*>(op);
            if (!ExpressionsReferenceOnlyAllowedTables(distinct->expressions, allowed_tables)) {
                return true;
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_ORDER: {
            auto* order = static_cast<LogicalOrder*>(op);
            if (!OrdersReferenceOnlyAllowedTables(*order, allowed_tables)) {
                return true;
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_WINDOW: {
            auto* window = static_cast<LogicalWindow*>(op);
            if (!ExpressionsReferenceOnlyAllowedTables(window->partitions, allowed_tables)) {
                return true;
            }
            for (const auto& order : window->orders) {
                if (!ExpressionReferencesAllowedTables(order.expression.get(), allowed_tables)) {
                    return true;
                }
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_LIMIT: {
            auto* limit = static_cast<LogicalLimit*>(op);
            if (!ExpressionReferencesAllowedTables(limit->limit_count.get(), allowed_tables) ||
                !ExpressionReferencesAllowedTables(limit->limit_offset.get(), allowed_tables)) {
                return true;
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
            auto* join = static_cast<LogicalComparisonJoin*>(op);
            if (!ExpressionsReferenceOnlyAllowedTables(join->conditions, allowed_tables)) {
                return true;
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
        case LogicalOperatorType::LOGICAL_DELIM_GET:
            break;
        default:
            return true;
    }

    for (auto& child : op->children) {
        if (SubtreeHasCorrelatedReferences(child.get(), allowed_tables)) {
            return true;
        }
    }
    return false;
}

void DecorrelateDependentJoin::InitializeTableIndexAllocator(LogicalOperator* plan) {
    next_table_index_ = 0;
    std::set<size_t> table_indexes;
    CollectOutputTables(plan, table_indexes);
    for (auto table_index : table_indexes) {
        next_table_index_ = std::max(next_table_index_, table_index + 1);
    }
}

TableIndex DecorrelateDependentJoin::AllocateTableIndex() {
    return TableIndex{next_table_index_++};
}

void DecorrelateDependentJoin::SplitConjunctions(std::vector<std::unique_ptr<Expression>>& expressions) {
    std::vector<std::unique_ptr<Expression>> split;
    for (auto& expression : expressions) {
        if (expression && expression->type == ExpressionType::BOUND_CONJUNCTION) {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression.get());
            if (conjunction->bool_expr_type == 0) {
                for (auto& child : conjunction->children) {
                    split.push_back(std::move(child));
                }
                continue;
            }
        }
        split.push_back(std::move(expression));
    }
    expressions = std::move(split);
}

bool DecorrelateDependentJoin::ContainsBlockingOperator(LogicalOperator* op) const {
    if (!op) {
        return false;
    }

    switch (op->type) {
        case LogicalOperatorType::LOGICAL_GET:
        case LogicalOperatorType::LOGICAL_FILTER:
        case LogicalOperatorType::LOGICAL_PROJECTION:
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN:
        case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
        case LogicalOperatorType::LOGICAL_DELIM_GET:
            break;
        default:
            return true;
    }

    for (auto& child : op->children) {
        if (ContainsBlockingOperator(child.get())) {
            return true;
        }
    }
    return false;
}

void DecorrelateDependentJoin::ExtractCorrelatedExpressions(
    std::vector<std::unique_ptr<Expression>>& expressions,
    const std::set<size_t>& allowed_tables,
    std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    SplitConjunctions(expressions);

    std::vector<std::unique_ptr<Expression>> keep_filters;
    for (auto& expression : expressions) {
        if (ExpressionReferencesAllowedTables(expression.get(), allowed_tables)) {
            keep_filters.push_back(std::move(expression));
        } else {
            lifted_filters.push_back(std::move(expression));
        }
    }
    expressions = std::move(keep_filters);
}

bool DecorrelateDependentJoin::PushDownGet(
    std::unique_ptr<LogicalOperator>& plan,
    const std::set<size_t>& allowed_tables,
    std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    auto* get = static_cast<LogicalGet*>(plan.get());
    ExtractCorrelatedExpressions(get->filters, allowed_tables, lifted_filters);
    return true;
}

bool DecorrelateDependentJoin::PushDownFilter(
    std::unique_ptr<LogicalOperator>& plan,
    const std::set<size_t>& allowed_tables,
    std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    if (plan->children.size() != 1 || !PushDownCorrelatedNode(plan->children[0], allowed_tables, lifted_filters)) {
        return false;
    }

    auto* filter = static_cast<LogicalFilter*>(plan.get());
    ExtractCorrelatedExpressions(filter->expressions, allowed_tables, lifted_filters);
    if (filter->expressions.empty()) {
        auto child = std::move(plan->children[0]);
        plan = std::move(child);
    }
    return true;
}

bool DecorrelateDependentJoin::PushDownProjection(
    std::unique_ptr<LogicalOperator>& plan,
    const std::set<size_t>& allowed_tables,
    std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    auto* projection = static_cast<LogicalProjection*>(plan.get());
    if (!ExpressionsReferenceOnlyAllowedTables(projection->expressions, allowed_tables) ||
        plan->children.size() != 1) {
        return false;
    }
    if (!PushDownCorrelatedNode(plan->children[0], allowed_tables, lifted_filters)) {
        return false;
    }
    return RewriteLiftedFiltersThroughProjection(*projection, allowed_tables, lifted_filters);
}

bool DecorrelateDependentJoin::PushDownAggregate(
    std::unique_ptr<LogicalOperator>& plan,
    const std::set<size_t>& allowed_tables,
    std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    auto* aggregate = static_cast<LogicalAggregate*>(plan.get());
    if (!ExpressionsReferenceOnlyAllowedTables(aggregate->groups, allowed_tables) ||
        !ExpressionsReferenceOnlyAllowedTables(aggregate->expressions, allowed_tables) ||
        plan->children.size() != 1) {
        return false;
    }
    if (!PushDownCorrelatedNode(plan->children[0], allowed_tables, lifted_filters)) {
        return false;
    }
    return RewriteLiftedFiltersThroughAggregate(*aggregate, allowed_tables, lifted_filters);
}

bool DecorrelateDependentJoin::PushDownDistinct(
    std::unique_ptr<LogicalOperator>& plan,
    const std::set<size_t>& allowed_tables,
    std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    auto* distinct = static_cast<LogicalDistinct*>(plan.get());
    if (!ExpressionsReferenceOnlyAllowedTables(distinct->expressions, allowed_tables) ||
        plan->children.size() != 1) {
        return false;
    }
    if (!PushDownCorrelatedNode(plan->children[0], allowed_tables, lifted_filters)) {
        return false;
    }
    return AddCorrelatedKeysToDistinct(*distinct, allowed_tables, lifted_filters);
}

bool DecorrelateDependentJoin::PushDownSetOperation(
    std::unique_ptr<LogicalOperator>& plan,
    const std::set<size_t>& allowed_tables,
    std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    auto* setop = static_cast<LogicalSetOperation*>(plan.get());
    if (setop->setop_type != SetOperationType::UNION || !setop->all || plan->children.size() != 2) {
        return false;
    }

    auto* left_projection = plan->children[0] && plan->children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION
        ? static_cast<LogicalProjection*>(plan->children[0].get())
        : nullptr;
    auto* right_projection = plan->children[1] && plan->children[1]->type == LogicalOperatorType::LOGICAL_PROJECTION
        ? static_cast<LogicalProjection*>(plan->children[1].get())
        : nullptr;
    if (!left_projection || !right_projection ||
        !ExpressionsReferenceOnlyAllowedTables(left_projection->expressions, allowed_tables) ||
        !ExpressionsReferenceOnlyAllowedTables(right_projection->expressions, allowed_tables) ||
        plan->children[0]->children.size() != 1 ||
        plan->children[1]->children.size() != 1) {
        return false;
    }

    std::set<size_t> left_allowed_tables;
    std::set<size_t> right_allowed_tables;
    CollectOutputTables(plan->children[0]->children[0].get(), left_allowed_tables);
    CollectOutputTables(plan->children[1]->children[0].get(), right_allowed_tables);

    if (!FindUnaryLeafRoot(plan->children[0]->children[0].get()) ||
        !FindUnaryLeafRoot(plan->children[1]->children[0].get())) {
        return false;
    }

    std::map<std::string, size_t> left_equalities;
    std::map<std::string, size_t> right_equalities;
    std::map<std::string, ParsedLiftedEquality> rewritten_equalities;
    if (!CollectUnaryBranchEqualities(*left_projection, plan->children[0]->children[0].get(), left_allowed_tables,
                                      left_equalities, &rewritten_equalities) ||
        !CollectUnaryBranchEqualities(*right_projection, plan->children[1]->children[0].get(), right_allowed_tables,
                                      right_equalities, nullptr)) {
        return false;
    }

    if (left_equalities.size() != right_equalities.size()) {
        return false;
    }
    for (const auto& entry : left_equalities) {
        auto right_it = right_equalities.find(entry.first);
        if (right_it == right_equalities.end() || right_it->second != entry.second ||
            entry.second >= setop->output_names.size()) {
            return false;
        }
    }

    std::vector<std::unique_ptr<Expression>> left_lifted_filters;
    std::vector<std::unique_ptr<Expression>> right_lifted_filters;
    if (!PushDownCorrelatedNode(plan->children[0]->children[0], left_allowed_tables, left_lifted_filters) ||
        !PushDownCorrelatedNode(plan->children[1]->children[0], right_allowed_tables, right_lifted_filters) ||
        left_lifted_filters.size() != rewritten_equalities.size() ||
        right_lifted_filters.size() != rewritten_equalities.size()) {
        return false;
    }

    std::vector<std::unique_ptr<Expression>> rewritten_filters;
    for (auto& entry : rewritten_equalities) {
        auto left_it = left_equalities.find(entry.first);
        if (left_it == left_equalities.end() || left_it->second >= setop->output_names.size()) {
            return false;
        }

        auto equality = std::make_unique<BoundFunctionExpression>("=", InvalidOid);
        equality->children.push_back(std::make_unique<BoundColumnRefExpression>(
            ColumnBinding{setop->table_index, ProjectionIndex{left_it->second}},
            "setop",
            setop->output_names[left_it->second]));
        equality->children.push_back(std::move(entry.second.outer_expr));
        rewritten_filters.push_back(std::move(equality));
    }

    lifted_filters = std::move(rewritten_filters);
    return true;
}

bool DecorrelateDependentJoin::PushDownOrder(
    std::unique_ptr<LogicalOperator>& plan,
    const std::set<size_t>& allowed_tables,
    std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    auto* order = static_cast<LogicalOrder*>(plan.get());
    if (!OrdersReferenceOnlyAllowedTables(*order, allowed_tables) || plan->children.size() != 1) {
        return false;
    }
    return PushDownCorrelatedNode(plan->children[0], allowed_tables, lifted_filters);
}

bool DecorrelateDependentJoin::PushDownWindow(
    std::unique_ptr<LogicalOperator>& plan,
    const std::set<size_t>& allowed_tables,
    std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    auto* window = static_cast<LogicalWindow*>(plan.get());
    if (!ExpressionsReferenceOnlyAllowedTables(window->partitions, allowed_tables) ||
        plan->children.size() != 1) {
        return false;
    }
    for (const auto& order : window->orders) {
        if (!ExpressionReferencesAllowedTables(order.expression.get(), allowed_tables)) {
            return false;
        }
    }
    return PushDownCorrelatedNode(plan->children[0], allowed_tables, lifted_filters);
}

bool DecorrelateDependentJoin::PushDownLimit(
    std::unique_ptr<LogicalOperator>& plan,
    const std::set<size_t>& allowed_tables,
    std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    auto* limit = static_cast<LogicalLimit*>(plan.get());
    if (!ExpressionReferencesAllowedTables(limit->limit_count.get(), allowed_tables) ||
        !ExpressionReferencesAllowedTables(limit->limit_offset.get(), allowed_tables) ||
        plan->children.size() != 1) {
        return false;
    }

    LogicalOperator* base_child = plan->children[0].get();
    if (base_child && base_child->type == LogicalOperatorType::LOGICAL_ORDER) {
        auto* order = static_cast<LogicalOrder*>(base_child);
        if (!OrdersReferenceOnlyAllowedTables(*order, allowed_tables) || order->children.size() != 1) {
            return false;
        }
        base_child = order->children[0].get();
    }

    if (!SubtreeHasCorrelatedReferences(base_child, allowed_tables)) {
        return PushDownCorrelatedNode(plan->children[0], allowed_tables, lifted_filters);
    }

    std::vector<OrderByNode> orders;
    std::unique_ptr<LogicalOperator> child = std::move(plan->children[0]);
    if (child && child->type == LogicalOperatorType::LOGICAL_ORDER) {
        auto* order = static_cast<LogicalOrder*>(child.get());
        orders = CloneDecorrelatedOrders(order->orders);
        child = std::move(order->children[0]);
    }

    std::vector<std::unique_ptr<Expression>> child_lifted_filters;
    if (!PushDownCorrelatedNode(child, allowed_tables, child_lifted_filters)) {
        return false;
    }

    std::vector<std::unique_ptr<Expression>> partitions;
    std::vector<std::unique_ptr<Expression>> remaining_lifted;
    for (auto& expression : child_lifted_filters) {
        std::set<size_t> referenced_tables;
        CollectReferencedTables(expression.get(), referenced_tables);

        bool has_allowed_ref = false;
        for (auto table_index : referenced_tables) {
            if (allowed_tables.find(table_index) != allowed_tables.end()) {
                has_allowed_ref = true;
                break;
            }
        }

        std::unique_ptr<Expression> inner_expr;
        std::unique_ptr<Expression> outer_expr;
        if (ExtractDecorrelatedEquality(expression.get(), allowed_tables, inner_expr, outer_expr)) {
            partitions.push_back(std::move(inner_expr));
            remaining_lifted.push_back(std::move(expression));
            continue;
        }
        if (!has_allowed_ref) {
            remaining_lifted.push_back(std::move(expression));
            continue;
        }
        return false;
    }

    if (partitions.empty()) {
        return false;
    }

    auto window = std::make_unique<LogicalWindow>(AllocateTableIndex());
    window->estimated_cardinality = child->estimated_cardinality;
    window->function_names.push_back("row_number");
    window->output_names.push_back("limit_rownum");
    window->partitions = std::move(partitions);
    window->orders = std::move(orders);
    window->children.push_back(std::move(child));

    auto filter = std::make_unique<LogicalFilter>();
    filter->estimated_cardinality = plan->estimated_cardinality;
    auto rownum_filter = BuildDecorrelatedLimitRowNumberFilter(
        limit->limit_count.get(),
        limit->limit_offset.get(),
        window->table_index);
    if (!rownum_filter) {
        return false;
    }
    filter->expressions.push_back(std::move(rownum_filter));
    filter->children.push_back(std::move(window));
    plan = std::move(filter);

    for (auto& expression : remaining_lifted) {
        lifted_filters.push_back(std::move(expression));
    }
    return true;
}

bool DecorrelateDependentJoin::PushDownJoin(
    std::unique_ptr<LogicalOperator>& plan,
    const std::set<size_t>& allowed_tables,
    std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    auto* join = static_cast<LogicalComparisonJoin*>(plan.get());
    if (join->dependent || plan->children.size() != 2) {
        return false;
    }

    std::set<size_t> semi_join_hidden_tables;
    std::map<std::pair<size_t, size_t>, ColumnBinding> semi_join_replacements;
    if (!BuildSemiJoinVisibleBindingReplacements(*join, semi_join_hidden_tables, semi_join_replacements) ||
        !RewriteSemiJoinExpressionsToVisibleOutputs(
            semi_join_hidden_tables, semi_join_replacements, join->conditions, true)) {
        return false;
    }

    bool left_has_correlation = SubtreeHasCorrelatedReferences(plan->children[0].get(), allowed_tables);
    bool right_has_correlation = SubtreeHasCorrelatedReferences(plan->children[1].get(), allowed_tables);
    bool join_conditions_are_local = ExpressionsReferenceOnlyAllowedTables(join->conditions, allowed_tables);

    // Correlated predicates inside an outer join ON clause are not safe to lift into
    // the parent dependent-join condition set in this simplified rewrite model.
    if (IsOuterJoinType(join->join_type) && !join_conditions_are_local) {
        return false;
    }

    std::vector<std::unique_ptr<Expression>> local_lifted_filters;
    switch (join->join_type) {
        case JOIN_INNER:
            if ((left_has_correlation &&
                 !PushDownCorrelatedNode(plan->children[0], allowed_tables, local_lifted_filters)) ||
                (right_has_correlation &&
                 !PushDownCorrelatedNode(plan->children[1], allowed_tables, local_lifted_filters))) {
                return false;
            }
            break;
        case JOIN_LEFT:
            if (right_has_correlation) {
                return false;
            }
            if (left_has_correlation &&
                !PushDownCorrelatedNode(plan->children[0], allowed_tables, local_lifted_filters)) {
                return false;
            }
            break;
        case JOIN_RIGHT:
            if (left_has_correlation) {
                return false;
            }
            if (right_has_correlation &&
                !PushDownCorrelatedNode(plan->children[1], allowed_tables, local_lifted_filters)) {
                return false;
            }
            break;
        case JOIN_FULL:
            if (left_has_correlation || right_has_correlation) {
                return false;
            }
            break;
        default:
            if (!PushDownCorrelatedNode(plan->children[0], allowed_tables, local_lifted_filters) ||
                !PushDownCorrelatedNode(plan->children[1], allowed_tables, local_lifted_filters)) {
                return false;
            }
            break;
    }

    ExtractCorrelatedExpressions(join->conditions, allowed_tables, local_lifted_filters);
    KeepSemiJoinHiddenLiftedFiltersOnRight(allowed_tables, *join, plan->children[1], local_lifted_filters);
    if (!RewriteSemiJoinExpressionsToVisibleOutputs(
            semi_join_hidden_tables, semi_join_replacements, local_lifted_filters, false)) {
        return false;
    }
    for (auto& expression : local_lifted_filters) {
        lifted_filters.push_back(std::move(expression));
    }
    if (plan->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
        join->join_type == JOIN_INNER &&
        join->conditions.empty()) {
        auto cross = std::make_unique<LogicalCrossProduct>();
        cross->estimated_cardinality = plan->estimated_cardinality;
        cross->children.push_back(std::move(plan->children[0]));
        cross->children.push_back(std::move(plan->children[1]));
        plan = std::move(cross);
    }
    return true;
}

bool DecorrelateDependentJoin::PushDownCrossProduct(
    std::unique_ptr<LogicalOperator>& plan,
    const std::set<size_t>& allowed_tables,
    std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    if (plan->children.size() != 2) {
        return false;
    }

    bool left_has_correlation = SubtreeHasCorrelatedReferences(plan->children[0].get(), allowed_tables);
    bool right_has_correlation = SubtreeHasCorrelatedReferences(plan->children[1].get(), allowed_tables);

    std::vector<std::unique_ptr<Expression>> local_lifted_filters;
    if ((left_has_correlation &&
         !PushDownCorrelatedNode(plan->children[0], allowed_tables, local_lifted_filters)) ||
        (right_has_correlation &&
         !PushDownCorrelatedNode(plan->children[1], allowed_tables, local_lifted_filters))) {
        return false;
    }

    for (auto& expression : local_lifted_filters) {
        lifted_filters.push_back(std::move(expression));
    }
    return true;
}

bool DecorrelateDependentJoin::PushDownCorrelatedNode(
    std::unique_ptr<LogicalOperator>& plan,
    const std::set<size_t>& allowed_tables,
    std::vector<std::unique_ptr<Expression>>& lifted_filters) {
    if (!plan) {
        return true;
    }

    switch (plan->type) {
        case LogicalOperatorType::LOGICAL_FILTER:
            return PushDownFilter(plan, allowed_tables, lifted_filters);
        case LogicalOperatorType::LOGICAL_PROJECTION:
            return PushDownProjection(plan, allowed_tables, lifted_filters);
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
            return PushDownAggregate(plan, allowed_tables, lifted_filters);
        case LogicalOperatorType::LOGICAL_DISTINCT:
            return PushDownDistinct(plan, allowed_tables, lifted_filters);
        case LogicalOperatorType::LOGICAL_SET_OPERATION:
            return PushDownSetOperation(plan, allowed_tables, lifted_filters);
        case LogicalOperatorType::LOGICAL_ORDER:
            return PushDownOrder(plan, allowed_tables, lifted_filters);
        case LogicalOperatorType::LOGICAL_WINDOW:
            return PushDownWindow(plan, allowed_tables, lifted_filters);
        case LogicalOperatorType::LOGICAL_LIMIT:
            return PushDownLimit(plan, allowed_tables, lifted_filters);
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN:
            return PushDownJoin(plan, allowed_tables, lifted_filters);
        case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
            return PushDownCrossProduct(plan, allowed_tables, lifted_filters);
        case LogicalOperatorType::LOGICAL_GET:
            return PushDownGet(plan, allowed_tables, lifted_filters);
        case LogicalOperatorType::LOGICAL_DELIM_GET:
            return true;
        default:
            return false;
    }
}

std::unique_ptr<LogicalOperator> DecorrelateDependentJoin::Rewrite(std::unique_ptr<LogicalOperator> plan,
                                                                   bool* changed) {
    for (auto& child : plan->children) {
        child = Rewrite(std::move(child), changed);
    }

    if (plan->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
        plan->type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN &&
        plan->type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        return plan;
    }

    auto* join = static_cast<LogicalComparisonJoin*>(plan.get());
    auto* dependent_join = static_cast<LogicalDependentJoin*>(plan.get());
    if (!join->dependent || plan->children.size() != 2) {
        return plan;
    }

    if (DecorrelateScalarAggregateJoin(plan)) {
        MarkChanged(changed);
        return plan;
    }

    std::set<size_t> allowed_tables;
    CollectOutputTables(plan->children[1].get(), allowed_tables);
    std::vector<std::unique_ptr<Expression>> lifted_filters;
    auto right = std::move(plan->children[1]);
    if (!PushDownCorrelatedNode(right, allowed_tables, lifted_filters)) {
        plan->children[1] = std::move(right);
        return plan;
    }

    plan->children[1] = std::move(right);
    if (!lifted_filters.empty()) {
        for (auto& lifted : lifted_filters) {
            AppendUniqueFilter(join->conditions, std::move(lifted));
        }
    }

    join->dependent = false;
    if (plan->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
        plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        auto decorrelated_type =
            (join->join_type == JOIN_MARK ||
             join->join_type == JOIN_SEMI ||
             join->join_type == JOIN_ANTI ||
             join->join_type == JOIN_SINGLE)
                ? LogicalOperatorType::LOGICAL_DELIM_JOIN
                : (dependent_join->correlated_columns.empty() || !dependent_join->perform_delim
                       ? LogicalOperatorType::LOGICAL_COMPARISON_JOIN
                       : LogicalOperatorType::LOGICAL_DELIM_JOIN);
        plan->type = decorrelated_type;
        if (decorrelated_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
            dependent_join->correlated_columns.clear();
            dependent_join->perform_delim = false;
        }
    }

    MarkChanged(changed);
    return plan;
}

std::unique_ptr<LogicalOperator> DecorrelateDependentJoin::Optimize(std::unique_ptr<LogicalOperator> plan,
                                                                    bool* changed) {
    if (!plan) {
        return nullptr;
    }
    InitializeTableIndexAllocator(plan.get());
    return Rewrite(std::move(plan), changed);
}

} // namespace yaap
