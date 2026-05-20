#include "join_order_query_graph.hpp"

namespace yaap {

JoinOrderQueryGraphManager::JoinOrderQueryGraphManager(uint64_t subset_mask)
    : subset_mask_(subset_mask) {
}

void JoinOrderQueryGraphManager::Build(const std::vector<JoinOrderJoinCondition>& conditions) {
	filter_infos_.clear();
	size_t filter_index = 0;
	for (const auto& condition : conditions) {
		auto condition_mask = condition.relation_mask & subset_mask_;
		if (__builtin_popcountll(condition_mask) < 2) {
			continue;
		}
		uint64_t left = condition.left_relation_mask & subset_mask_;
		uint64_t right = condition.right_relation_mask & subset_mask_;
		bool derived_sides = left != 0 && right != 0 && (left & right) == 0;

		auto emit_filter = [&](uint64_t left_mask, uint64_t right_mask, bool residual, bool create_edge) {
			if (left_mask == 0 || right_mask == 0 || (left_mask & right_mask) != 0) {
				return;
			}
			auto& left_set = set_manager_.GetJoinRelation(left_mask);
			auto& right_set = set_manager_.GetJoinRelation(right_mask);
			auto filter_info = std::make_unique<JoinOrderFilterInfo>(
				condition, left_set, right_set, filter_index++, condition.join_type);
			filter_info->invert_result = condition.invert_result;
			filter_info->from_residual_predicate = residual;
			CollectColumnBindingsByMask(condition.expression.get(),
                                        condition.left_table_mask,
                                        condition.right_table_mask,
                                        *filter_info);
			auto* filter_ptr = filter_info.get();
			filter_infos_.push_back(std::move(filter_info));
			if (create_edge) {
				query_graph_.CreateEdge(left_set, right_set, filter_ptr);
				query_graph_.CreateEdge(right_set, left_set, filter_ptr);
			}
		};

		bool residual_only = condition.from_residual_predicate || !derived_sides;
		if (residual_only) {
			uint64_t left_mask = left;
			uint64_t right_mask = right;
			if (left_mask == 0 || right_mask == 0 || (left_mask & right_mask) != 0) {
				left_mask = condition_mask & (~condition_mask + 1);
				right_mask = condition_mask ^ left_mask;
			}
			emit_filter(left_mask, right_mask, true, false);
			continue;
		}

		if (derived_sides && left != 0 && right != 0 &&
			__builtin_popcountll(left) == 1 && __builtin_popcountll(right) == 1) {
			emit_filter(left, right, false, true);
			continue;
		}

		uint64_t subset = condition_mask;
		while (subset != 0) {
			subset = (subset - 1) & condition_mask;
			if (subset == 0 || subset == condition_mask) {
				continue;
			}
			auto other = condition_mask ^ subset;
			if (subset > other) {
				continue;
			}
			emit_filter(subset, other, false, true);
		}
	}
}

const JoinOrderQueryGraphEdges& JoinOrderQueryGraphManager::GetQueryGraphEdges() const {
	return query_graph_;
}

const std::vector<std::unique_ptr<JoinOrderFilterInfo>>& JoinOrderQueryGraphManager::GetFilterInfos() const {
	return filter_infos_;
}

const JoinOrderRelationSetManager& JoinOrderQueryGraphManager::GetSetManager() const {
	return set_manager_;
}

JoinOrderRelationSetManager& JoinOrderQueryGraphManager::GetSetManager() {
	return set_manager_;
}

} // namespace yaap
