#include "optimizer_core.hpp"

#include <algorithm>
#include <cmath>

namespace yaap {

namespace {

void CapDelimGetCardinality(LogicalOperator* plan, size_t upper_bound) {
    if (!plan) {
        return;
    }
    if (plan->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
        plan->estimated_cardinality = std::max<size_t>(
            1,
            std::min(std::max<size_t>(1, plan->estimated_cardinality), std::max<size_t>(1, upper_bound)));
    }
    for (auto& child : plan->children) {
        CapDelimGetCardinality(child.get(), upper_bound);
    }
}

size_t FindMinDelimGetCardinality(LogicalOperator* plan) {
    if (!plan) {
        return 0;
    }
    size_t best = 0;
    if (plan->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
        best = std::max<size_t>(1, plan->estimated_cardinality);
    }
    for (auto& child : plan->children) {
        auto child_best = FindMinDelimGetCardinality(child.get());
        if (child_best == 0) {
            continue;
        }
        best = best == 0 ? child_best : std::min(best, child_best);
    }
    return best;
}

} // namespace

OptimizerPass CardinalityEstimator::Pass() const {
    return OptimizerPass::CARDINALITY_ESTIMATOR;
}

RelationStats CardinalityEstimator::Rewrite(LogicalOperator& plan) {
    for (auto& child : plan.children) {
        Rewrite(*child);
    }

    switch (plan.type) {
        case LogicalOperatorType::LOGICAL_GET: {
            plan.estimated_cardinality = statistics_helper_.Extract(plan).cardinality;
            break;
        }
        case LogicalOperatorType::LOGICAL_FILTER: {
            auto& filter = static_cast<LogicalFilter&>(plan);
            auto input_stats = statistics_helper_.Extract(*filter.children[0]);
            filter.estimated_cardinality = statistics_helper_.EstimateFilterCardinality(
                input_stats,
                filter.expressions);
            break;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
            auto& join = static_cast<LogicalComparisonJoin&>(plan);
            auto left_stats = statistics_helper_.Extract(*join.children[0]);
            if (join.join_type == JOIN_SINGLE || plan.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
                CapDelimGetCardinality(join.children[1].get(), left_stats.cardinality);
            }
            auto right_stats = statistics_helper_.Extract(*join.children[1]);
            if (join.dependent || join.join_type != JOIN_INNER) {
                size_t estimated_cardinality = left_stats.cardinality;
                if (join.join_type == JOIN_MARK) {
                    estimated_cardinality = left_stats.cardinality;
                } else if (IsSemiOrAntiJoinType(join.join_type)) {
                    if (!join.conditions.empty()) {
                        std::vector<Expression*> conditions;
                        conditions.reserve(join.conditions.size());
                        for (auto& condition : join.conditions) {
                            conditions.push_back(condition.get());
                        }
                        estimated_cardinality = join.join_type == JOIN_ANTI
                            ? statistics_helper_.EstimateSemiOrAntiJoinCardinality(left_stats, right_stats, conditions, true)
                            : std::min(
                                  left_stats.cardinality,
                                  statistics_helper_.EstimateJoinCardinality(left_stats, right_stats, conditions));
                    } else {
                        estimated_cardinality = std::max<size_t>(
                            1,
                            static_cast<size_t>(std::ceil(
                                static_cast<double>(left_stats.cardinality) *
                                RelationStatisticsHelper::DEFAULT_SELECTIVITY)));
                    }
                }
                if (join.join_type == JOIN_LEFT) {
                    estimated_cardinality = std::max(estimated_cardinality, left_stats.cardinality);
                } else if (join.join_type == JOIN_RIGHT) {
                    estimated_cardinality = std::max(estimated_cardinality, right_stats.cardinality);
                } else if (join.join_type == JOIN_FULL) {
                    estimated_cardinality = std::max({estimated_cardinality, left_stats.cardinality, right_stats.cardinality});
                }
                join.estimated_cardinality = estimated_cardinality;
                break;
            }
            std::vector<Expression*> conditions;
            conditions.reserve(join.conditions.size());
            for (auto& condition : join.conditions) {
                conditions.push_back(condition.get());
            }
            join.estimated_cardinality = statistics_helper_.EstimateJoinCardinality(
                left_stats,
                right_stats,
                conditions);
            break;
        }
        case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
            if (plan.children.size() == 2) {
                size_t left = plan.children[0]->estimated_cardinality;
                size_t right = plan.children[1]->estimated_cardinality;
                plan.estimated_cardinality = (left == 0 || right == 0) ? 0 : left * right;
            }
            break;
        case LogicalOperatorType::LOGICAL_PROJECTION:
        case LogicalOperatorType::LOGICAL_ORDER:
            if (plan.children.size() == 1) {
                plan.estimated_cardinality = plan.children[0]->estimated_cardinality;
            }
            break;
        case LogicalOperatorType::LOGICAL_DISTINCT:
            if (plan.children.size() == 1) {
                auto input_stats = statistics_helper_.Extract(*plan.children[0]);
                auto& distinct = static_cast<LogicalDistinct&>(plan);
                plan.estimated_cardinality = statistics_helper_.EstimateDistinctCardinality(
                    input_stats,
                    distinct.expressions);
            }
            break;
        case LogicalOperatorType::LOGICAL_SET_OPERATION:
            if (plan.children.size() == 2) {
                plan.estimated_cardinality =
                    plan.children[0]->estimated_cardinality + plan.children[1]->estimated_cardinality;
            }
            break;
        case LogicalOperatorType::LOGICAL_LIMIT:
            if (plan.children.size() == 1) {
                auto input_stats = statistics_helper_.Extract(*plan.children[0]);
                auto& limit = static_cast<LogicalLimit&>(plan);
                plan.estimated_cardinality = statistics_helper_.EstimateLimitCardinality(
                    input_stats,
                    limit.limit_count.get(),
                    limit.limit_offset.get());
            }
            break;
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
            if (plan.children.size() == 1) {
                auto input_stats = statistics_helper_.Extract(*plan.children[0]);
                auto& aggregate = static_cast<LogicalAggregate&>(plan);
                if (aggregate.groups.empty()) {
                    plan.estimated_cardinality = 1;
                } else {
                    plan.estimated_cardinality = statistics_helper_.EstimateDistinctCardinality(
                        input_stats,
                        aggregate.groups);
                    auto delim_upper_bound = FindMinDelimGetCardinality(plan.children[0].get());
                    if (delim_upper_bound > 0) {
                        plan.estimated_cardinality = std::min(plan.estimated_cardinality, delim_upper_bound);
                    }
                }
            }
            break;
        default:
            break;
    }

    return statistics_helper_.Extract(plan);
}

static std::unique_ptr<Expression> CloneExpressionWithBindingReplacements(
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

std::unique_ptr<LogicalOperator> CardinalityEstimator::Optimize(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) {
        return nullptr;
    }
    Rewrite(*plan);
    return plan;
}

void LogicalOptimizer::RunOptimizer(OptimizerPass pass, OptimizerRule& rule, std::unique_ptr<LogicalOperator>& plan) {
    if (!plan || rule.Pass() != pass) {
        return;
    }
    plan = rule.Optimize(std::move(plan));
}

std::unique_ptr<LogicalOperator> LogicalOptimizer::Optimize(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) return nullptr;

    JoinPredicateExtraction join_predicate_extraction;
    RunOptimizer(OptimizerPass::JOIN_PREDICATE_EXTRACTION, join_predicate_extraction, plan);

    FilterPushdown filter_pushdown;
    RunOptimizer(OptimizerPass::FILTER_PUSHDOWN, filter_pushdown, plan);
    RunOptimizer(OptimizerPass::JOIN_PREDICATE_EXTRACTION, join_predicate_extraction, plan);

    PredicatePropagation predicate_propagation;
    RunOptimizer(OptimizerPass::PREDICATE_PROPAGATION, predicate_propagation, plan);

    ScanFilterFolding scan_filter_folding;
    RunOptimizer(OptimizerPass::SCAN_FILTER_FOLDING, scan_filter_folding, plan);

    RemoveUnusedColumns remove_unused_columns;
    RunOptimizer(OptimizerPass::REMOVE_UNUSED_COLUMNS, remove_unused_columns, plan);

    // Column pruning can expose new FILTER -> GET shapes after the initial pushdown/folding pass.
    // Fold once more so later cardinality/join-order work does not double-count scan-local predicates.
    RunOptimizer(OptimizerPass::SCAN_FILTER_FOLDING, scan_filter_folding, plan);

    CardinalityEstimator cardinality_estimator;
    RunOptimizer(OptimizerPass::CARDINALITY_ESTIMATOR, cardinality_estimator, plan);

    JoinOrderOptimizer join_order_optimizer;
    RunOptimizer(OptimizerPass::JOIN_ORDER, join_order_optimizer, plan);

    // Join-order reconstruction can surface residual filters above rebuilt
    // special joins; push them back to the owning child before physical planning.
    RunOptimizer(OptimizerPass::FILTER_PUSHDOWN, filter_pushdown, plan);

    RunOptimizer(OptimizerPass::CARDINALITY_ESTIMATOR, cardinality_estimator, plan);

    return plan;
}

} // namespace yaap
