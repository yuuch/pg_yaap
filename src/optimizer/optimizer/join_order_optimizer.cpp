#include "optimizer_core.hpp"
#include "join_order_plan_enumerator.hpp"

#include <algorithm>
#include <cmath>
#include <set>
#include <stdexcept>

#include "../adapter/yaap_adapter.hpp"
#include "../logical/logical_utils.hpp"

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

bool RemapSemiProjectionReferences(Expression *expr, const LogicalProjection &projection) {
	if (expr == nullptr)
		return true;
	switch (expr->type) {
		case ExpressionType::BOUND_COLUMN_REF: {
			auto *ref = static_cast<BoundColumnRefExpression *>(expr);
			if (ref->binding.table_index.index != projection.table_index.index)
				return true;
			const size_t idx = ref->binding.column_index.index;
			if (idx >= projection.expressions.size() ||
				projection.expressions[idx] == nullptr ||
				projection.expressions[idx]->type != ExpressionType::BOUND_COLUMN_REF)
				return false;
			const auto *input = static_cast<const BoundColumnRefExpression *>(
				projection.expressions[idx].get());
			ref->binding = input->binding;
			ref->table_name = input->table_name;
			ref->column_name = input->column_name;
			return true;
		}
		case ExpressionType::BOUND_FUNCTION: {
			auto *function = static_cast<BoundFunctionExpression *>(expr);
			for (auto &child : function->children)
				if (!RemapSemiProjectionReferences(child.get(), projection))
					return false;
			return true;
		}
		case ExpressionType::BOUND_CONJUNCTION: {
			auto *conjunction = static_cast<BoundConjunctionExpression *>(expr);
			for (auto &child : conjunction->children)
				if (!RemapSemiProjectionReferences(child.get(), projection))
					return false;
			return true;
		}
		case ExpressionType::BOUND_AGGREGATE: {
			auto *aggregate = static_cast<BoundAggregateExpression *>(expr);
			for (auto &child : aggregate->children)
				if (!RemapSemiProjectionReferences(child.get(), projection))
					return false;
			return true;
		}
		case ExpressionType::BOUND_SUBQUERY: {
			auto *subquery = static_cast<BoundSubqueryExpression *>(expr);
			for (auto &child : subquery->children)
				if (!RemapSemiProjectionReferences(child.get(), projection))
					return false;
			return true;
		}
		case ExpressionType::BOUND_CONSTANT:
			return true;
		case ExpressionType::BOUND_COMPARISON:
		case ExpressionType::OPAQUE:
			return false;
		default:
			return false;
	}
}

bool CollectSemiBindings(Expression *expr, std::set<ColumnBindingKey> &bindings) {
	if (expr == nullptr)
		return true;
	switch (expr->type) {
		case ExpressionType::BOUND_COLUMN_REF: {
			const auto *ref = static_cast<const BoundColumnRefExpression *>(expr);
			bindings.insert(ColumnBindingKey{ref->binding.table_index.index, ref->binding.column_index.index});
			return true;
		}
		case ExpressionType::BOUND_FUNCTION: {
			const auto *function = static_cast<const BoundFunctionExpression *>(expr);
			for (const auto &child : function->children)
				if (!CollectSemiBindings(child.get(), bindings)) return false;
			return true;
		}
		case ExpressionType::BOUND_CONJUNCTION: {
			const auto *conjunction = static_cast<const BoundConjunctionExpression *>(expr);
			for (const auto &child : conjunction->children)
				if (!CollectSemiBindings(child.get(), bindings)) return false;
			return true;
		}
		case ExpressionType::BOUND_AGGREGATE: {
			const auto *aggregate = static_cast<const BoundAggregateExpression *>(expr);
			for (const auto &child : aggregate->children)
				if (!CollectSemiBindings(child.get(), bindings)) return false;
			return true;
		}
		case ExpressionType::BOUND_SUBQUERY: {
			const auto *subquery = static_cast<const BoundSubqueryExpression *>(expr);
			for (const auto &child : subquery->children)
				if (!CollectSemiBindings(child.get(), bindings)) return false;
			return true;
		}
		case ExpressionType::BOUND_CONSTANT:
			return true;
		case ExpressionType::BOUND_COMPARISON:
		case ExpressionType::OPAQUE:
			return false;
		default:
			return false;
	}
}

bool FindSemiPushChild(const LogicalComparisonJoin &semi,
			       const LogicalOperator &inner,
			       const LogicalProjection *projection,
			       std::set<size_t> &left_refs,
			       size_t &push_child) {
	std::set<size_t> semi_left_tables;
	std::set<size_t> semi_right_tables;
	std::set<size_t> inner_left_tables;
	std::set<size_t> inner_right_tables;
	CollectOutputTables(semi.children[0].get(), semi_left_tables);
	CollectOutputTables(semi.children[1].get(), semi_right_tables);
	CollectOutputTables(inner.children[0].get(), inner_left_tables);
	CollectOutputTables(inner.children[1].get(), inner_right_tables);
	std::set<size_t> right_refs;
	for (const auto &condition : semi.conditions) {
		std::set<ColumnBindingKey> refs;
		if (!CollectSemiBindings(condition.get(), refs))
			return false;
		for (const ColumnBindingKey &binding : refs) {
			size_t table = binding.table_index;
			if (projection != nullptr && table == projection->table_index.index) {
				if (binding.column_index >= projection->expressions.size() ||
					projection->expressions[binding.column_index] == nullptr ||
					projection->expressions[binding.column_index]->type != ExpressionType::BOUND_COLUMN_REF)
					return false;
				const auto *input = static_cast<const BoundColumnRefExpression *>(
					projection->expressions[binding.column_index].get());
				table = input->binding.table_index.index;
			}
			const bool is_left = semi_left_tables.count(table) != 0;
			const bool is_right = semi_right_tables.count(table) != 0;
			if (is_left == is_right)
				return false;
			if (is_left)
				left_refs.insert(table);
			else
				right_refs.insert(table);
		}
	}
	if (left_refs.empty() || right_refs.empty())
		return false;
	const auto subset = [](const std::set<size_t> &needles, const std::set<size_t> &haystack) {
		return std::all_of(needles.begin(), needles.end(), [&](size_t value) {
			return haystack.count(value) != 0;
		});
	};
	const bool in_left = subset(left_refs, inner_left_tables);
	const bool in_right = subset(left_refs, inner_right_tables);
	if (in_left == in_right)
		return false;
	push_child = in_left ? 0 : 1;
	return true;
}

void FlattenAndExpression(std::unique_ptr<Expression> expression,
			  std::vector<std::unique_ptr<Expression>> &conjuncts) {
	if (expression && expression->type == ExpressionType::BOUND_CONJUNCTION) {
		auto *conjunction = static_cast<BoundConjunctionExpression *>(expression.get());
		if (conjunction->bool_expr_type == 0) {
			for (auto &child : conjunction->children)
				FlattenAndExpression(std::move(child), conjuncts);
			return;
		}
	}
	conjuncts.push_back(std::move(expression));
}

void RemoveFiltersReferencing(std::vector<std::unique_ptr<Expression>> &expressions,
				     const std::set<size_t> &hidden_tables) {
	std::vector<std::unique_ptr<Expression>> conjuncts;
	for (auto &expression : expressions)
		FlattenAndExpression(std::move(expression), conjuncts);
	std::vector<std::unique_ptr<Expression>> kept;
	for (auto &expression : conjuncts) {
		std::set<size_t> references;
		CollectReferencedTables(expression.get(), references);
		const bool references_hidden = std::any_of(
			references.begin(), references.end(), [&](size_t table) {
				return hidden_tables.count(table) != 0;
			});
		if (!references_hidden)
			kept.push_back(std::move(expression));
	}
	expressions = std::move(kept);
}

std::set<size_t> PruneDanglingSemiReferences(
	std::unique_ptr<LogicalOperator> &plan,
	const std::set<const LogicalComparisonJoin *> &pushed_semis) {
	if (!plan)
		return {};

	std::set<size_t> hidden_tables;
	for (auto &child : plan->children) {
		auto child_hidden = PruneDanglingSemiReferences(child, pushed_semis);
		hidden_tables.insert(child_hidden.begin(), child_hidden.end());
	}

	if (plan->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *join = static_cast<LogicalComparisonJoin *>(plan.get());
		if (pushed_semis.count(join) != 0) {
			std::set<size_t> semi_right_tables;
			CollectOutputTables(join->children[1].get(), semi_right_tables);
			hidden_tables.insert(semi_right_tables.begin(), semi_right_tables.end());
			return hidden_tables;
		}
		// Predicate propagation can leave equivalent predicates that mention the
		// SEMI right input above this join. That input is no longer visible there;
		// the pushed SEMI still enforces its predicate on the owning left input.
		if (join->join_type == JOIN_INNER)
			RemoveFiltersReferencing(join->conditions, hidden_tables);
		if (join->join_type == JOIN_INNER && join->conditions.empty()) {
			auto cross_product = std::make_unique<LogicalCrossProduct>();
			cross_product->children = std::move(plan->children);
			cross_product->estimated_cardinality =
				cross_product->children[0]->estimated_cardinality *
				cross_product->children[1]->estimated_cardinality;
			plan = std::move(cross_product);
		}
	} else if (plan->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto *filter = static_cast<LogicalFilter *>(plan.get());
		RemoveFiltersReferencing(filter->expressions, hidden_tables);
		if (filter->expressions.empty() && plan->children.size() == 1)
			plan = std::move(plan->children[0]);
	}
	return hidden_tables;
}

std::unique_ptr<LogicalOperator> PushSemiJoinIntoInnerChild(
	std::unique_ptr<LogicalOperator> semi_plan,
	std::unique_ptr<LogicalOperator> inner_plan,
	std::unique_ptr<LogicalOperator> semi_right,
	std::vector<std::unique_ptr<Expression>> conditions,
	size_t push_child) {
	auto *inner = inner_plan.get();
	auto *semi = static_cast<LogicalComparisonJoin *>(semi_plan.get());
	const size_t semi_cardinality = semi->estimated_cardinality;
	semi->conditions = std::move(conditions);
	semi->children.clear();
	semi->children.push_back(std::move(inner->children[push_child]));
	semi->children.push_back(std::move(semi_right));
	semi->estimated_cardinality = std::min(
		semi->children[0]->estimated_cardinality, semi_cardinality);
	inner->children[push_child] = std::move(semi_plan);
	RecomputeNonReorderableBinaryCardinality(inner);
	return inner_plan;
}

std::unique_ptr<LogicalOperator> PushSelectiveSemiJoinsDown(
	std::unique_ptr<LogicalOperator> plan,
	std::set<const LogicalComparisonJoin *> &pushed_semis) {
	if (!plan)
		return nullptr;
	for (auto &child : plan->children)
		child = PushSelectiveSemiJoinsDown(std::move(child), pushed_semis);
	if (plan->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		plan->children.size() != 2)
		return plan;
	auto *semi = static_cast<LogicalComparisonJoin *>(plan.get());
	if (semi->join_type != JOIN_SEMI || semi->dependent || semi->has_mark_index ||
		semi->children[0] == nullptr || semi->children[1] == nullptr)
		return plan;

	// For an INNER join, a SEMI predicate that references only one input can be
	// applied to that input before the join. This is the useful shape for queries
	// such as Q18: filter orders by qualifying order keys before joining customer
	// and lineitem. Keep the rewrite conservative when bindings cannot be mapped
	// through a projection or span both INNER inputs.

	LogicalProjection *projection = nullptr;
	LogicalOperator *inner_op = semi->children[0].get();
	if (inner_op->type == LogicalOperatorType::LOGICAL_PROJECTION &&
		inner_op->children.size() == 1 && inner_op->children[0] != nullptr) {
		projection = static_cast<LogicalProjection *>(inner_op);
		inner_op = projection->children[0].get();
	}
	if ((inner_op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
		 inner_op->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) ||
		inner_op->children.size() != 2)
		return plan;
	if (inner_op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *inner = static_cast<LogicalComparisonJoin *>(inner_op);
		if (inner->join_type != JOIN_INNER || inner->dependent)
			return plan;
	}
	if (inner_op->children[0] == nullptr || inner_op->children[1] == nullptr)
		return plan;

	std::set<size_t> left_refs;
	size_t push_child = 0;
	if (!FindSemiPushChild(*semi, *inner_op, projection, left_refs, push_child))
		return plan;

	std::vector<std::unique_ptr<Expression>> rewritten_conditions;
	rewritten_conditions.reserve(semi->conditions.size());
	for (const auto &condition : semi->conditions) {
		auto rewritten = CloneExpressionTree(condition.get());
		if (!rewritten || (projection != nullptr &&
			!RemapSemiProjectionReferences(rewritten.get(), *projection)))
			return plan;
		rewritten_conditions.push_back(std::move(rewritten));
	}
	semi->conditions = std::move(rewritten_conditions);

	auto conditions = std::move(semi->conditions);
	auto semi_right = std::move(semi->children[1]);
	pushed_semis.insert(semi);
	std::unique_ptr<LogicalOperator> rewritten_inner;
	if (projection != nullptr) {
		auto projection_plan = std::move(plan->children[0]);
		auto inner_plan = std::move(projection_plan->children[0]);
		rewritten_inner = PushSemiJoinIntoInnerChild(std::move(plan), std::move(inner_plan),
			std::move(semi_right), std::move(conditions), push_child);
		projection_plan->children[0] = PushSelectiveSemiJoinsDown(std::move(rewritten_inner), pushed_semis);
		RecomputeUnaryCardinality(projection_plan.get());
		return projection_plan;
	}
	auto inner_plan = std::move(plan->children[0]);
	return PushSelectiveSemiJoinsDown(PushSemiJoinIntoInnerChild(std::move(plan), std::move(inner_plan),
		std::move(semi_right), std::move(conditions), push_child), pushed_semis);
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
	std::set<const LogicalComparisonJoin *> pushed_semis;
	auto rewritten = PushSelectiveSemiJoinsDown(Rewrite(std::move(plan)), pushed_semis);
	if (!pushed_semis.empty())
		PruneDanglingSemiReferences(rewritten, pushed_semis);
	NormalizeJoinCardinality(rewritten.get());
	return rewritten;
}

} // namespace yaap
