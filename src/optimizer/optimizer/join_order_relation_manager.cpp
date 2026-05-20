#include "optimizer_core.hpp"

#include "../logical/logical_utils.hpp"
#include "join_order_plan_node.hpp"

#include "../adapter/yaap_adapter.hpp"

#include <map>
#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace yaap {

OptimizerPass JoinOrderOptimizer::Pass() const {
    return OptimizerPass::JOIN_ORDER;
}

namespace {

bool IsReorderableJoinNode(LogicalOperator* op) {
    if (!op) {
        return false;
    }
    if (op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
        return true;
    }
    if (op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        return false;
    }
    auto* join = static_cast<LogicalComparisonJoin*>(op);
    return !join->dependent && (join->join_type == JOIN_INNER || IsSemiOrAntiJoinType(join->join_type));
}

void RecomputeBinaryOperatorCardinality(LogicalOperator* plan) {
    if (!plan || plan->children.size() != 2 || !plan->children[0] || !plan->children[1]) {
        return;
    }

    RelationStatisticsHelper statistics_helper;
    auto left_stats = statistics_helper.Extract(*plan->children[0]);
    auto right_stats = statistics_helper.Extract(*plan->children[1]);

    if (plan->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
        size_t left = left_stats.cardinality;
        size_t right = right_stats.cardinality;
        plan->estimated_cardinality = (left == 0 || right == 0) ? 0 : left * right;
        return;
    }

    if (plan->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
        plan->type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN &&
        plan->type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        return;
    }

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
                estimated_cardinality = std::min(
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
            estimated_cardinality = std::max(estimated_cardinality, std::max(left_stats.cardinality, right_stats.cardinality));
        }
        plan->estimated_cardinality = estimated_cardinality;
        return;
    }

    std::vector<Expression*> conditions;
    conditions.reserve(join->conditions.size());
    for (auto& condition : join->conditions) {
        conditions.push_back(condition.get());
    }
    plan->estimated_cardinality = statistics_helper.EstimateJoinCardinality(left_stats, right_stats, conditions);
}

} // namespace

bool JoinOrderOptimizer::IsReorderableJoinTree(LogicalOperator* op) {
    if (!op) {
        return false;
    }
    if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
        return op->children.size() == 1 && op->children[0] && IsReorderableJoinTree(op->children[0].get());
    }
    return IsReorderableJoinNode(op);
}

bool JoinOrderOptimizer::IsSafeJoinOrderTree(LogicalOperator* op) {
    if (!op) {
        return false;
    }

    switch (op->type) {
        case LogicalOperatorType::LOGICAL_GET:
        case LogicalOperatorType::LOGICAL_DELIM_GET:
            return true;
        case LogicalOperatorType::LOGICAL_FILTER:
        case LogicalOperatorType::LOGICAL_PROJECTION:
        case LogicalOperatorType::LOGICAL_ORDER:
        case LogicalOperatorType::LOGICAL_LIMIT:
            return op->children.size() == 1 && op->children[0] && IsSafeJoinOrderTree(op->children[0].get());
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN:
        case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
            return op->children.size() == 2 && op->children[0] && op->children[1] &&
                   IsSafeJoinOrderTree(op->children[0].get()) &&
                   IsSafeJoinOrderTree(op->children[1].get());
        default:
            return false;
    }
}

void JoinOrderOptimizer::ExtractJoinGraph(std::unique_ptr<LogicalOperator> plan,
                                          std::vector<JoinRelation>& relations,
                                          std::vector<JoinCondition>& conditions) {
    if (!plan) {
        return;
    }

	if (plan->type == LogicalOperatorType::LOGICAL_FILTER && plan->children.size() == 1 && plan->children[0]) {
		auto* filter = static_cast<LogicalFilter*>(plan.get());
		for (auto& expression : filter->expressions) {
			if (expression && expression->type == ExpressionType::BOUND_CONJUNCTION) {
				auto* conjunction = static_cast<BoundConjunctionExpression*>(expression.get());
				if (conjunction->bool_expr_type == 0) {
					for (auto& child : conjunction->children) {
						conditions.push_back({std::move(child), 0, 0, false, false});
					}
					continue;
				}
			}
			conditions.push_back({std::move(expression), 0, 0, false, false});
		}
        ExtractJoinGraph(std::move(plan->children[0]), relations, conditions);
        return;
    }

    if (plan->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
        plan->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
        plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
        plan->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
        if (plan->children.size() != 2 || !plan->children[0] || !plan->children[1]) {
            std::set<size_t> output_tables;
            CollectOutputTables(plan.get(), output_tables);
            RelationStatisticsHelper statistics_helper;
            auto relation_stats = statistics_helper.Extract(*plan);
            relations.push_back({std::move(plan), std::move(output_tables), 0, std::move(relation_stats)});
            relations.back().estimated_cardinality = relations.back().plan->estimated_cardinality;
            relations.back().stats.cardinality = relations.back().estimated_cardinality;
            return;
        }

        if (IsReorderableJoinNode(plan.get())) {
            if (plan->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
                plan->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
                plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
                auto* join = static_cast<LogicalComparisonJoin*>(plan.get());
                for (auto& condition : join->conditions) {
					conditions.push_back({std::move(condition), 0, 0, 0, 0, 0, join->join_type, join->invert_result, false});
    			}
    		}

    		ExtractJoinGraph(std::move(plan->children[0]), relations, conditions);
    		ExtractJoinGraph(std::move(plan->children[1]), relations, conditions);
    		return;
    	}

        for (auto& child : plan->children) {
            child = Rewrite(std::move(child));
            if (!child) {
                return;
            }
        }
        RecomputeBinaryOperatorCardinality(plan.get());
        std::set<size_t> output_tables;
        CollectOutputTables(plan.get(), output_tables);
        RelationStatisticsHelper statistics_helper;
        auto relation_stats = statistics_helper.Extract(*plan);
        relations.push_back({std::move(plan), std::move(output_tables), 0, std::move(relation_stats)});
        relations.back().estimated_cardinality = relations.back().plan->estimated_cardinality;
        relations.back().stats.cardinality = relations.back().estimated_cardinality;
        return;
	}

	if (plan->children.size() == 1 && plan->children[0] && IsSafeJoinOrderTree(plan->children[0].get())) {
		plan->children[0] = Rewrite(std::move(plan->children[0]));
		if (!plan->children[0]) {
			return;
		}
		RelationStatisticsHelper statistics_helper;
		auto wrapper_stats = statistics_helper.Extract(*plan);
		plan->estimated_cardinality = wrapper_stats.cardinality;
		plan->children[0]->estimated_cardinality = statistics_helper.Extract(*plan->children[0]).cardinality;
		std::set<size_t> output_tables;
		CollectOutputTables(plan.get(), output_tables);
		relations.push_back({std::move(plan), std::move(output_tables), wrapper_stats.cardinality, std::move(wrapper_stats)});
		relations.back().estimated_cardinality = relations.back().plan->estimated_cardinality;
		relations.back().stats.cardinality = relations.back().estimated_cardinality;
		return;
	}

    std::set<size_t> output_tables;
    CollectOutputTables(plan.get(), output_tables);
    RelationStatisticsHelper statistics_helper;
    auto relation_stats = statistics_helper.Extract(*plan);
    relations.push_back({std::move(plan), std::move(output_tables), 0, std::move(relation_stats)});
    relations.back().estimated_cardinality = relations.back().plan->estimated_cardinality;
    relations.back().stats.cardinality = relations.back().estimated_cardinality;
}

std::vector<uint64_t> JoinOrderOptimizer::FindJoinComponents(const std::vector<JoinRelation>& relations,
                                                             const std::vector<JoinCondition>& conditions) {
    if (relations.empty()) {
        return {};
    }

    std::vector<size_t> parent(relations.size());
    std::vector<size_t> rank(relations.size(), 0);
    for (size_t idx = 0; idx < relations.size(); ++idx) {
        parent[idx] = idx;
    }

    auto find_root = [&](auto&& self, size_t idx) -> size_t {
        if (parent[idx] == idx) {
            return idx;
        }
        parent[idx] = self(self, parent[idx]);
        return parent[idx];
    };

    auto unite = [&](size_t left, size_t right) {
        auto left_root = find_root(find_root, left);
        auto right_root = find_root(find_root, right);
        if (left_root == right_root) {
            return;
        }
        if (rank[left_root] < rank[right_root]) {
            std::swap(left_root, right_root);
        }
        parent[right_root] = left_root;
        if (rank[left_root] == rank[right_root]) {
            rank[left_root]++;
        }
    };

    for (const auto& condition : conditions) {
        if (condition.from_residual_predicate) {
            continue;
        }
        if (__builtin_popcountll(condition.relation_mask) < 2) {
            continue;
        }

        std::vector<size_t> members;
        for (size_t idx = 0; idx < relations.size(); ++idx) {
            if ((condition.relation_mask & (uint64_t{1} << idx)) != 0) {
                members.push_back(idx);
            }
        }

        for (size_t idx = 1; idx < members.size(); ++idx) {
            unite(members[0], members[idx]);
        }
    }

    std::map<size_t, uint64_t> components;
    for (size_t idx = 0; idx < relations.size(); ++idx) {
        auto root = find_root(find_root, idx);
        components[root] |= (uint64_t{1} << idx);
    }

    std::vector<uint64_t> result;
    result.reserve(components.size());
    for (auto& entry : components) {
        result.push_back(entry.second);
    }
    return result;
}

uint64_t JoinOrderOptimizer::ReferencedRelationMask(Expression* expression, const std::vector<JoinRelation>& relations) {
    std::set<size_t> referenced_tables;
    CollectReferencedTables(expression, referenced_tables);

    uint64_t mask = 0;
    for (size_t relation_idx = 0; relation_idx < relations.size(); ++relation_idx) {
        for (auto table_idx : referenced_tables) {
            if (relations[relation_idx].output_tables.find(table_idx) != relations[relation_idx].output_tables.end()) {
                mask |= (uint64_t{1} << relation_idx);
                break;
            }
        }
    }
    return mask;
}

RelationStats JoinOrderOptimizer::GetDPStats(
    uint64_t mask,
    const std::map<uint64_t, std::unique_ptr<JoinOrderDPJoinNode>>& plans,
    const std::vector<JoinRelation>& relations) const {
    if ((mask & (mask - 1)) == 0) {
        size_t relation_idx = 0;
        while (((uint64_t{1} << relation_idx) & mask) == 0) {
            relation_idx++;
        }
        return relations[relation_idx].stats;
    }

    auto stats_entry = dp_stats_.find(mask);
    if (stats_entry != dp_stats_.end()) {
        return stats_entry->second;
    }

    auto plan_entry = plans.find(mask);
    if (plan_entry == plans.end()) {
        return RelationStats{};
    }

    const auto& node = *plan_entry->second;
    if (node.is_leaf) {
        return RelationStats{};
    }

    auto left_stats = GetDPStats(node.left_set.mask, plans, relations);
    auto right_stats = GetDPStats(node.right_set.mask, plans, relations);
    RelationStatisticsHelper statistics_helper;
    return statistics_helper.CombineReorderableStats(left_stats, right_stats, node.cardinality);
}

} // namespace yaap
