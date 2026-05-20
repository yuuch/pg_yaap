#include "join_order_reconstruct.hpp"

#include "../logical/logical_utils.hpp"

#include "join_order_plan_node.hpp"

#include <algorithm>
#include <map>
#include <set>
#include <stdexcept>
#include <string>

namespace yaap {

namespace {

std::unique_ptr<LogicalOperator> PushResidualFilters(std::unique_ptr<LogicalOperator> node,
													 std::vector<std::unique_ptr<Expression>> filters) {
	if (filters.empty()) {
		return node;
	}
	auto filter = std::make_unique<LogicalFilter>();
	filter->expressions = std::move(filters);
	filter->children.push_back(std::move(node));
	return filter;
}

using BindingKey = ColumnBindingKey;

BindingKey ToBindingKey(ColumnBinding binding) {
	return BindingKey{binding.table_index.index, binding.column_index.index};
}

bool TryExtractColumnEquality(Expression* expression, ColumnBinding& left, ColumnBinding& right) {
	if (!expression || expression->type != ExpressionType::BOUND_FUNCTION) {
		return false;
	}
	auto* function = static_cast<BoundFunctionExpression*>(expression);
	if (function->function_name != "=" || function->children.size() != 2) {
		return false;
	}
	if (function->children[0]->type != ExpressionType::BOUND_COLUMN_REF ||
		function->children[1]->type != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	left = static_cast<BoundColumnRefExpression*>(function->children[0].get())->binding;
	right = static_cast<BoundColumnRefExpression*>(function->children[1].get())->binding;
	return true;
}

struct EqualitySet {
	std::map<BindingKey, BindingKey> parent;

	void Ensure(BindingKey key) {
		if (parent.find(key) == parent.end()) {
			parent[key] = key;
		}
	}

	BindingKey Find(BindingKey key) {
		Ensure(key);
		auto it = parent.find(key);
		if (it->second.table_index == key.table_index && it->second.column_index == key.column_index) {
			return key;
		}
		it->second = Find(it->second);
		return it->second;
	}

	bool Union(BindingKey left, BindingKey right) {
		left = Find(left);
		right = Find(right);
		if (left.table_index == right.table_index && left.column_index == right.column_index) {
			return false;
		}
		parent[right] = left;
		return true;
	}
};

void CollectSubtreeEqualities(LogicalOperator* op, EqualitySet& equalities) {
	if (!op) {
		return;
	}

	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto* filter = static_cast<LogicalFilter*>(op);
		for (const auto& expression : filter->expressions) {
			ColumnBinding left;
			ColumnBinding right;
			if (TryExtractColumnEquality(expression.get(), left, right)) {
				equalities.Union(ToBindingKey(left), ToBindingKey(right));
			}
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
			   op->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
			   op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto* join = static_cast<LogicalComparisonJoin*>(op);
		if (!join->dependent && join->join_type == JOIN_INNER) {
			for (const auto& expression : join->conditions) {
				ColumnBinding left;
				ColumnBinding right;
				if (TryExtractColumnEquality(expression.get(), left, right)) {
					equalities.Union(ToBindingKey(left), ToBindingKey(right));
				}
			}
		}
	}

	for (const auto& child : op->children) {
		CollectSubtreeEqualities(child.get(), equalities);
	}
}

std::vector<const JoinOrderFilterInfo*> PruneRedundantInnerJoinFilters(
	const std::vector<const JoinOrderFilterInfo*>& join_filters,
	LogicalOperator* left,
	LogicalOperator* right) {
	EqualitySet equalities;
	CollectSubtreeEqualities(left, equalities);
	CollectSubtreeEqualities(right, equalities);

	std::vector<const JoinOrderFilterInfo*> pruned;
	pruned.reserve(join_filters.size());
	for (const auto* filter : join_filters) {
		ColumnBinding left_binding;
		ColumnBinding right_binding;
		if (!TryExtractColumnEquality(filter->condition.expression.get(), left_binding, right_binding)) {
			pruned.push_back(filter);
			continue;
		}
		if (equalities.Union(ToBindingKey(left_binding), ToBindingKey(right_binding))) {
			pruned.push_back(filter);
		}
	}
	return pruned;
}

} // namespace

JoinOrderReconstructor::JoinOrderReconstructor(const JoinOrderQueryGraphManager& query_graph_manager)
    : query_graph_manager_(query_graph_manager) {
}

std::unique_ptr<LogicalOperator> JoinOrderReconstructor::Reconstruct(
    uint64_t mask,
    const std::map<uint64_t, std::unique_ptr<JoinOrderDPJoinNode>>& plans,
    std::vector<JoinOrderJoinRelation>& relations,
    const std::vector<JoinOrderJoinCondition>& conditions) const {
    auto result = GenerateJoins(mask, plans, relations, conditions);
    return std::move(result.op);
}

JoinOrderGenerateRelation JoinOrderReconstructor::GenerateJoins(
    uint64_t mask,
    const std::map<uint64_t, std::unique_ptr<JoinOrderDPJoinNode>>& plans,
    std::vector<JoinOrderJoinRelation>& relations,
    const std::vector<JoinOrderJoinCondition>& conditions) const {
    if ((mask & (mask - 1)) == 0) {
        size_t relation_idx = 0;
        while (((uint64_t{1} << relation_idx) & mask) == 0) {
            relation_idx++;
        }
        const auto entry = plans.find(mask);
        if (entry == plans.end()) {
            throw std::runtime_error("Join order reconstruction missing leaf DP node");
        }
        return {&entry->second->set, std::move(relations[relation_idx].plan)};
    }

    const auto entry = plans.find(mask);
    if (entry == plans.end()) {
        throw std::runtime_error("Join order reconstruction missing DP node");
    }

    const auto& node = *entry->second;
    if (node.is_leaf) {
        throw std::runtime_error("Leaf join node should have been handled by mask cardinality check");
    }

    auto left = GenerateJoins(node.left_set.mask, plans, relations, conditions);
    auto right = GenerateJoins(node.right_set.mask, plans, relations, conditions);

    std::vector<const JoinOrderFilterInfo*> join_filters;
    std::vector<const JoinOrderFilterInfo*> residual_filters;
    CollectNodeFilters(node, mask, join_filters, residual_filters);
    const auto* primary_filter = ChoosePrimaryJoinFilter(join_filters);
    int join_type = primary_filter ? primary_filter->join_type : 0;
    bool invert_result = primary_filter ? primary_filter->invert_result : false;
    auto result_op = MakeJoinNode(node, std::move(left), std::move(right),
                                  std::move(join_filters), std::move(residual_filters), join_type);
    if (result_op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        static_cast<LogicalComparisonJoin*>(result_op.get())->invert_result = invert_result;
    }
    return {&node.set, std::move(result_op)};
}

void JoinOrderReconstructor::CollectNodeFilters(
    const JoinOrderDPJoinNode& node,
    uint64_t mask,
    std::vector<const JoinOrderFilterInfo*>& join_filters,
    std::vector<const JoinOrderFilterInfo*>& residual_filters) const {
    std::set<size_t> used_filter_indexes;
    std::set<const JoinOrderJoinCondition*> used_conditions;
    if (node.info) {
        for (const auto* filter : node.info->filters) {
            used_filter_indexes.insert(filter->filter_index);
            used_conditions.insert(&filter->condition);
            if (!FilterBelongsToNode(*filter, node, mask)) {
                continue;
            }
            if (filter->from_residual_predicate) {
                residual_filters.push_back(filter);
            } else {
                join_filters.push_back(filter);
            }
        }
    }

    for (const auto& filter_info_ptr : query_graph_manager_.GetFilterInfos()) {
        const auto& filter = *filter_info_ptr;
        if (used_filter_indexes.find(filter.filter_index) != used_filter_indexes.end()) {
            continue;
        }
        if (used_conditions.find(&filter.condition) != used_conditions.end()) {
            continue;
        }
        if (!FilterBelongsToNode(filter, node, mask)) {
            continue;
        }
        if (filter.from_residual_predicate) {
            residual_filters.push_back(&filter);
        } else {
            join_filters.push_back(&filter);
        }
    }
}

bool JoinOrderReconstructor::FilterBelongsToNode(const JoinOrderFilterInfo& filter,
                                                 const JoinOrderDPJoinNode& node,
                                                 uint64_t mask) const {
    uint64_t filter_mask = filter.left_set.mask | filter.right_set.mask;
    if (!IsMaskSubset(filter_mask, mask)) {
        return false;
    }
    if (IsMaskSubset(filter_mask, node.left_set.mask) ||
        IsMaskSubset(filter_mask, node.right_set.mask)) {
        return false;
    }
    return true;
}

std::unique_ptr<LogicalOperator> JoinOrderReconstructor::MakeJoinNode(
    const JoinOrderDPJoinNode& node,
    JoinOrderGenerateRelation left,
    JoinOrderGenerateRelation right,
    std::vector<const JoinOrderFilterInfo*> join_filters,
    std::vector<const JoinOrderFilterInfo*> residual_filters,
    int join_type) const {
    const auto* primary_filter = ChoosePrimaryJoinFilter(join_filters);
    bool swap_children = false;
    if (primary_filter) {
        join_type = primary_filter->join_type;
    }
    if (primary_filter && IsSemiOrAntiJoinType(join_type)) {
        swap_children = IsInvertedFilter(*primary_filter, *left.set, *right.set);
    }
    if (swap_children) {
        std::swap(left, right);
    }
    if (join_type == JOIN_INNER) {
        join_filters = PruneRedundantInnerJoinFilters(join_filters, left.op.get(), right.op.get());
    }

    std::vector<std::unique_ptr<Expression>> join_conditions;
    join_conditions.reserve(join_filters.size() + residual_filters.size());
    for (const auto* filter : join_filters) {
        if (join_type == JOIN_INNER) {
            join_conditions.push_back(CloneExpressionTree(filter->condition.expression.get()));
        } else {
            join_conditions.push_back(CloneOrientedFilter(*filter, *left.set, *right.set));
        }
    }

    if (!join_conditions.empty()) {
        auto join = std::make_unique<LogicalComparisonJoin>(join_type);
        join->conditions = std::move(join_conditions);
        join->children.push_back(std::move(left.op));
        join->children.push_back(std::move(right.op));
        join->estimated_cardinality = node.cardinality;
        if (IsSemiOrAntiJoinType(join_type)) {
            join->estimated_cardinality = std::min(join->estimated_cardinality, join->children[0]->estimated_cardinality);
        }
        std::vector<std::unique_ptr<Expression>> residual_conditions;
        residual_conditions.reserve(residual_filters.size());
        for (const auto* residual : residual_filters) {
            residual_conditions.push_back(CloneExpressionTree(residual->condition.expression.get()));
        }
        return PushResidualFilters(std::move(join), std::move(residual_conditions));
    }

    auto cross_product = std::make_unique<LogicalCrossProduct>();
    cross_product->children.push_back(std::move(left.op));
    cross_product->children.push_back(std::move(right.op));
    cross_product->estimated_cardinality = node.cardinality;
    std::vector<std::unique_ptr<Expression>> residual_conditions;
    residual_conditions.reserve(residual_filters.size());
    for (const auto* residual : residual_filters) {
        residual_conditions.push_back(CloneExpressionTree(residual->condition.expression.get()));
    }
    return PushResidualFilters(std::move(cross_product), std::move(residual_conditions));
}

const JoinOrderFilterInfo* JoinOrderReconstructor::ChoosePrimaryJoinFilter(
    const std::vector<const JoinOrderFilterInfo*>& join_filters) const {
    if (join_filters.empty()) {
        return nullptr;
    }
    for (const auto* filter : join_filters) {
        if (filter->join_type == JOIN_INNER) {
            return filter;
        }
    }
    return join_filters.front();
}

bool JoinOrderReconstructor::IsInvertedFilter(const JoinOrderFilterInfo& filter,
                                              const JoinOrderRelationSet& left_set,
                                              const JoinOrderRelationSet& right_set) const {
    return !(JoinOrderRelationSet::IsSubset(left_set, filter.left_set) &&
             JoinOrderRelationSet::IsSubset(right_set, filter.right_set));
}

std::unique_ptr<Expression> JoinOrderReconstructor::CloneOrientedFilter(
    const JoinOrderFilterInfo& filter,
    const JoinOrderRelationSet& left_set,
    const JoinOrderRelationSet& right_set) const {
    bool invert = IsInvertedFilter(filter, left_set, right_set);
    auto clone = CloneExpressionTree(filter.condition.expression.get());
    if (!invert || !clone || clone->type != ExpressionType::BOUND_FUNCTION) {
        return clone;
    }
    auto* function = static_cast<BoundFunctionExpression*>(clone.get());
    if (function->children.size() != 2) {
        return clone;
    }
    std::swap(function->children[0], function->children[1]);
    return clone;
}

} // namespace yaap
