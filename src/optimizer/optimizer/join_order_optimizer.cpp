#include "optimizer_core.hpp"
#include "join_order_plan_enumerator.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>

#include "../adapter/yaap_adapter.hpp"

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

uint64_t TableMaskToRelationMask(uint64_t table_mask, const std::vector<JoinOrderJoinRelation>& relations) {
    uint64_t relation_mask = 0;
    for (size_t relation_idx = 0; relation_idx < relations.size(); ++relation_idx) {
        for (auto table_idx : relations[relation_idx].output_tables) {
            if ((table_mask & (uint64_t{1} << table_idx)) != 0) {
                relation_mask |= (uint64_t{1} << relation_idx);
                break;
            }
        }
    }
    return relation_mask;
}

bool NeedsPreCapDelimGet(LogicalOperator* plan) {
    if (!plan || plan->children.size() != 2) {
        return false;
    }
    if (plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        return true;
    }
    if (plan->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
        plan->type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
        return false;
    }
    return static_cast<LogicalComparisonJoin*>(plan)->join_type == JOIN_SINGLE;
}

void NormalizeJoinCardinality(LogicalOperator* plan) {
	if (!plan) {
		return;
	}
	for (auto& child : plan->children) {
		NormalizeJoinCardinality(child.get());
	}
	if ((plan->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		 plan->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
		 plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) &&
		plan->children.size() == 2 && plan->children[0]) {
		auto* join = static_cast<LogicalComparisonJoin*>(plan);
		if (!join->dependent && IsSemiOrAntiJoinType(join->join_type)) {
			plan->estimated_cardinality =
				std::min(plan->estimated_cardinality, plan->children[0]->estimated_cardinality);
		}
	}
}

void RecomputeNonReorderableBinaryCardinality(LogicalOperator* plan) {
	if (!plan || plan->children.size() != 2 || !plan->children[0] || !plan->children[1]) {
		return;
	}

	RelationStatisticsHelper statistics_helper;
	auto left_stats = statistics_helper.Extract(*plan->children[0]);
	if (plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
		(plan->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
		 static_cast<LogicalComparisonJoin*>(plan)->join_type == JOIN_SINGLE) ||
		(plan->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN &&
		 static_cast<LogicalComparisonJoin*>(plan)->join_type == JOIN_SINGLE)) {
		CapDelimGetCardinality(plan->children[1].get(), left_stats.cardinality);
	}
	auto right_stats = statistics_helper.Extract(*plan->children[1]);

	switch (plan->type) {
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
		case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
			auto* join = static_cast<LogicalComparisonJoin*>(plan);
			if (join->dependent || join->join_type != JOIN_INNER) {
				size_t estimated_cardinality = left_stats.cardinality;
				if (join->join_type == JOIN_MARK) {
					estimated_cardinality = left_stats.cardinality;
        } else if (IsSemiOrAntiJoinType(join->join_type)) {
            if (!join->conditions.empty()) {
                std::vector<Expression*> conditions;
                conditions.reserve(join->conditions.size());
                for (auto& condition : join->conditions) {
                    conditions.push_back(condition.get());
                }
                estimated_cardinality = join->join_type == JOIN_ANTI
                    ? statistics_helper.EstimateSemiOrAntiJoinCardinality(left_stats, right_stats, conditions, true)
                    : std::min(
                          left_stats.cardinality,
                          statistics_helper.EstimateJoinCardinality(left_stats, right_stats, conditions));
            } else {
                estimated_cardinality = std::max<size_t>(
                    1,
                    static_cast<size_t>(std::ceil(
                        static_cast<double>(left_stats.cardinality) *
								RelationStatisticsHelper::DEFAULT_SELECTIVITY)));
					}
				}
				if (join->join_type == JOIN_LEFT) {
					estimated_cardinality = std::max(estimated_cardinality, left_stats.cardinality);
				} else if (join->join_type == JOIN_RIGHT) {
					estimated_cardinality = std::max(estimated_cardinality, right_stats.cardinality);
				} else if (join->join_type == JOIN_FULL) {
					estimated_cardinality =
						std::max({estimated_cardinality, left_stats.cardinality, right_stats.cardinality});
				}
				plan->estimated_cardinality = estimated_cardinality;
				return;
			}

			std::vector<Expression*> conditions;
			conditions.reserve(join->conditions.size());
			for (auto& condition : join->conditions) {
				conditions.push_back(condition.get());
			}
			plan->estimated_cardinality =
				statistics_helper.EstimateJoinCardinality(left_stats, right_stats, conditions);
			return;
		}
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
			size_t left = left_stats.cardinality;
			size_t right = right_stats.cardinality;
			plan->estimated_cardinality = (left == 0 || right == 0) ? 0 : left * right;
			return;
		}
		default:
			return;
	}
}

void RecomputeUnaryCardinality(LogicalOperator* plan) {
	if (!plan || plan->children.size() != 1 || !plan->children[0]) {
		return;
	}

	RelationStatisticsHelper statistics_helper;
	switch (plan->type) {
		case LogicalOperatorType::LOGICAL_FILTER: {
			auto* filter = static_cast<LogicalFilter*>(plan);
			plan->estimated_cardinality = statistics_helper.EstimateFilterCardinality(
				statistics_helper.Extract(*plan->children[0]),
				filter->expressions);
			return;
		}
		case LogicalOperatorType::LOGICAL_PROJECTION:
		case LogicalOperatorType::LOGICAL_ORDER:
		case LogicalOperatorType::LOGICAL_WINDOW:
		case LogicalOperatorType::LOGICAL_LIMIT: {
			plan->estimated_cardinality = plan->children[0]->estimated_cardinality;
			return;
		}
		case LogicalOperatorType::LOGICAL_DISTINCT: {
			auto* distinct = static_cast<LogicalDistinct*>(plan);
			plan->estimated_cardinality = statistics_helper.EstimateDistinctCardinality(
				statistics_helper.Extract(*plan->children[0]),
				distinct->expressions);
			return;
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto* aggregate = static_cast<LogicalAggregate*>(plan);
			auto input_stats = statistics_helper.Extract(*plan->children[0]);
			if (aggregate->groups.empty()) {
				plan->estimated_cardinality = 1;
				return;
			}
			plan->estimated_cardinality = statistics_helper.EstimateDistinctCardinality(
				input_stats,
				aggregate->groups);
			auto delim_upper_bound = FindMinDelimGetCardinality(plan->children[0].get());
			if (delim_upper_bound > 0) {
				plan->estimated_cardinality = std::min(plan->estimated_cardinality, delim_upper_bound);
			}
			return;
		}
		default:
			return;
	}
}

} // namespace

std::unique_ptr<LogicalOperator> JoinOrderOptimizer::OptimizeJoinSubset(
	uint64_t subset_mask,
	std::vector<JoinRelation>& relations,
	const std::vector<JoinCondition>& conditions) {
	JoinOrderPlanEnumerator enumerator(*this, subset_mask, relations, conditions);
	return enumerator.Solve();
}

std::unique_ptr<LogicalOperator> JoinOrderOptimizer::Rewrite(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) {
        return nullptr;
    }

    if (!IsSafeJoinOrderTree(plan.get())) {
        if (NeedsPreCapDelimGet(plan.get()) && plan->children[0] && plan->children[1]) {
            plan->children[0] = Rewrite(std::move(plan->children[0]));
            if (!plan->children[0]) {
                return nullptr;
            }
            RelationStatisticsHelper statistics_helper;
            auto left_stats = statistics_helper.Extract(*plan->children[0]);
            CapDelimGetCardinality(plan->children[1].get(), left_stats.cardinality);
            plan->children[1] = Rewrite(std::move(plan->children[1]));
            if (!plan->children[1]) {
                return nullptr;
            }
        } else {
        for (auto& child : plan->children) {
            if (child) {
                child = Rewrite(std::move(child));
            }
        }
        }
        for (auto& child : plan->children) {
            if (!child) {
                return nullptr;
            }
        }
        if (plan->children.size() == 1 && plan->children[0]) {
            RecomputeUnaryCardinality(plan.get());
        } else {
            RecomputeNonReorderableBinaryCardinality(plan.get());
        }
        return plan;
    }

    if (!IsReorderableJoinTree(plan.get())) {
        if (NeedsPreCapDelimGet(plan.get()) && plan->children[0] && plan->children[1]) {
            plan->children[0] = Rewrite(std::move(plan->children[0]));
            if (!plan->children[0]) {
                return nullptr;
            }
            RelationStatisticsHelper statistics_helper;
            auto left_stats = statistics_helper.Extract(*plan->children[0]);
            CapDelimGetCardinality(plan->children[1].get(), left_stats.cardinality);
            plan->children[1] = Rewrite(std::move(plan->children[1]));
            if (!plan->children[1]) {
                return nullptr;
            }
        } else {
        for (auto& child : plan->children) {
            if (child) {
                child = Rewrite(std::move(child));
            }
        }
        }
        for (auto& child : plan->children) {
            if (!child) {
                return nullptr;
            }
        }
        if (plan->children.size() == 1 && plan->children[0]) {
            RecomputeUnaryCardinality(plan.get());
        } else {
            RecomputeNonReorderableBinaryCardinality(plan.get());
        }
        return plan;
    }

    std::vector<JoinRelation> relations;
    std::vector<JoinCondition> conditions;
    dp_stats_.clear();
    ExtractJoinGraph(std::move(plan), relations, conditions);

    elog(LOG, "pg_yaap: join-order extracted relations=%zu conditions=%zu",
         relations.size(), conditions.size());

    if (relations.size() < 2) {
        if (relations.empty()) {
            return nullptr;
        }
        auto result = std::move(relations[0].plan);
        std::vector<std::unique_ptr<Expression>> residual_filters;
        for (auto& condition : conditions) {
            if (__builtin_popcountll(condition.relation_mask) < 2) {
                residual_filters.push_back(std::move(condition.expression));
            }
        }
        if (!residual_filters.empty()) {
            auto filter = std::make_unique<LogicalFilter>();
            filter->expressions = std::move(residual_filters);
            filter->children.push_back(std::move(result));
            RelationStatisticsHelper statistics_helper;
            filter->estimated_cardinality = statistics_helper.EstimateFilterCardinality(
                statistics_helper.Extract(*filter->children[0]), filter->expressions);
            result = std::move(filter);
        }
        return result;
    }

    if (relations.size() > 20) {
        throw std::runtime_error("Join order DP currently supports up to 20 relations");
    }

    std::vector<JoinCondition> join_conditions;
    std::vector<std::unique_ptr<Expression>> residual_filters;
    for (auto& condition : conditions) {
        condition.relation_mask = ReferencedRelationMask(condition.expression.get(), relations);
        if (__builtin_popcountll(condition.relation_mask) < 2) {
            residual_filters.push_back(std::move(condition.expression));
            continue;
        }
        uint64_t left_table_mask = 0;
        uint64_t right_table_mask = 0;
        if (TryExtractJoinSides(condition.expression.get(), left_table_mask, right_table_mask)) {
            condition.left_table_mask = left_table_mask;
            condition.right_table_mask = right_table_mask;
            condition.left_relation_mask = TableMaskToRelationMask(left_table_mask, relations);
            condition.right_relation_mask = TableMaskToRelationMask(right_table_mask, relations);
        }
		join_conditions.push_back(
			{std::move(condition.expression),
             condition.relation_mask,
             condition.left_table_mask,
             condition.right_table_mask,
             condition.left_relation_mask,
             condition.right_relation_mask,
             condition.join_type,
             condition.invert_result,
             condition.from_residual_predicate});
    }

    auto components = FindJoinComponents(relations, join_conditions);
    if (components.empty()) {
        return nullptr;
    }

    elog(LOG, "pg_yaap: join-order components=%zu", components.size());

    std::vector<std::unique_ptr<LogicalOperator>> component_plans;
    component_plans.reserve(components.size());
    for (auto component_mask : components) {
		auto component_plan = OptimizeJoinSubset(component_mask, relations, join_conditions);
        if (!component_plan) {
            return nullptr;
        }
        component_plans.push_back(std::move(component_plan));
    }

    if (component_plans.size() == 1) {
        auto result = std::move(component_plans[0]);
        if (!residual_filters.empty()) {
            auto filter = std::make_unique<LogicalFilter>();
            filter->expressions = std::move(residual_filters);
            filter->children.push_back(std::move(result));
            RelationStatisticsHelper statistics_helper;
            filter->estimated_cardinality = statistics_helper.EstimateFilterCardinality(
                statistics_helper.Extract(*filter->children[0]), filter->expressions);
            result = std::move(filter);
        }
        return result;
    }

    std::sort(component_plans.begin(), component_plans.end(),
              [](const std::unique_ptr<LogicalOperator>& left, const std::unique_ptr<LogicalOperator>& right) {
                  return left->estimated_cardinality < right->estimated_cardinality;
              });

    auto result = std::move(component_plans[0]);
    for (size_t idx = 1; idx < component_plans.size(); ++idx) {
        auto cross_product = std::make_unique<LogicalCrossProduct>();
        cross_product->children.push_back(std::move(result));
        cross_product->children.push_back(std::move(component_plans[idx]));
        cross_product->estimated_cardinality =
            cross_product->children[0]->estimated_cardinality * cross_product->children[1]->estimated_cardinality;
        result = std::move(cross_product);
    }

    if (!residual_filters.empty()) {
        auto filter = std::make_unique<LogicalFilter>();
        filter->expressions = std::move(residual_filters);
        filter->children.push_back(std::move(result));
        RelationStatisticsHelper statistics_helper;
        filter->estimated_cardinality = statistics_helper.EstimateFilterCardinality(
            statistics_helper.Extract(*filter->children[0]), filter->expressions);
        result = std::move(filter);
    }
    return result;
}

std::unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(std::unique_ptr<LogicalOperator> plan) {
	if (!plan) {
		return nullptr;
	}
	auto rewritten = Rewrite(std::move(plan));
	NormalizeJoinCardinality(rewritten.get());
	return rewritten;
}

} // namespace yaap
