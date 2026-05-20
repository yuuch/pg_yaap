#include "join_order_plan_enumerator.hpp"

#include <algorithm>

namespace yaap {

namespace {

int PrimaryJoinType(const std::vector<const JoinOrderNeighborInfo*>& connections) {
    int primary_join_type = JOIN_INNER;
    for (const auto* connection : connections) {
        for (const auto* filter : connection->filters) {
            if (filter->from_residual_predicate) {
                continue;
            }
            if (filter->join_type == JOIN_INNER) {
                return JOIN_INNER;
            }
            if (primary_join_type == JOIN_INNER) {
                primary_join_type = filter->join_type;
            }
        }
    }
    return primary_join_type;
}

size_t ConnectionFilterCount(const std::vector<const JoinOrderNeighborInfo*>& connections) {
    size_t count = 0;
    for (const auto* connection : connections) {
        count += connection->filters.size();
    }
    return count;
}

} // namespace

JoinOrderPlanEnumerator::JoinOrderPlanEnumerator(JoinOrderOptimizer& optimizer,
                                                 uint64_t subset_mask,
                                                 std::vector<JoinOrderJoinRelation>& relations,
                                                 const std::vector<JoinOrderJoinCondition>& conditions)
    : optimizer_(optimizer),
      subset_mask_(subset_mask),
      relations_(relations),
      conditions_(conditions),
      component_relation_count_(__builtin_popcountll(subset_mask)),
      query_graph_manager_(subset_mask),
      cost_model_(optimizer) {
}

std::unique_ptr<LogicalOperator> JoinOrderPlanEnumerator::Solve() {
    if ((subset_mask_ & (subset_mask_ - 1)) == 0) {
        size_t relation_idx = 0;
        while (((uint64_t{1} << relation_idx) & subset_mask_) == 0) {
            relation_idx++;
        }
        return std::move(relations_[relation_idx].plan);
    }

    InitializeLeafPlans();
    if (!SolveJoinOrderExactly()) {
        return nullptr;
    }

    auto full_plan = plans_.find(subset_mask_);
    if (full_plan == plans_.end()) {
        return nullptr;
    }

    return query_graph_manager_.Reconstruct(subset_mask_, plans_, relations_, conditions_);
}

void JoinOrderPlanEnumerator::InitializeLeafPlans() {
    optimizer_.dp_stats_.clear();
    for (size_t idx = 0; idx < relations_.size(); ++idx) {
        uint64_t mask = uint64_t{1} << idx;
        if ((mask & subset_mask_) == 0) {
            continue;
        }
        auto& set = query_graph_manager_.GetSetManager().GetJoinRelation(mask);
        auto node = std::make_unique<JoinOrderDPJoinNode>(set);
        node->cardinality = relations_[idx].estimated_cardinality;
        plans_[mask] = std::move(node);
        optimizer_.dp_stats_[mask] = relations_[idx].stats;
    }
}

std::vector<uint64_t> JoinOrderPlanEnumerator::AddSuperSets(const std::vector<uint64_t>& current,
                                                            const std::vector<size_t>& all_neighbors) const {
    std::vector<uint64_t> result;
    for (auto neighbor_set : current) {
        auto max_neighbor = HighestRelationIndex(neighbor_set);
        for (auto neighbor : all_neighbors) {
            if (max_neighbor >= neighbor) {
                continue;
            }
            auto neighbor_bit = uint64_t{1} << neighbor;
            if ((neighbor_set & neighbor_bit) == 0) {
                result.push_back(neighbor_set | neighbor_bit);
            }
        }
    }
    return result;
}

std::vector<uint64_t> JoinOrderPlanEnumerator::GetAllNeighborSets(std::vector<size_t> neighbors) const {
    std::vector<uint64_t> subsets;
    std::sort(neighbors.begin(), neighbors.end());
    if (neighbors.empty()) {
        return subsets;
    }

    std::vector<uint64_t> added;
    for (auto neighbor : neighbors) {
        auto singleton = uint64_t{1} << neighbor;
        added.push_back(singleton);
        subsets.push_back(singleton);
    }
    do {
        added = AddSuperSets(added, neighbors);
        for (auto subset : added) {
            subsets.push_back(subset);
        }
    } while (!added.empty());
    return subsets;
}

void JoinOrderPlanEnumerator::TryEmitPair(uint64_t left, uint64_t right) {
    if (left == 0 || right == 0 || (left & right) != 0) {
        return;
    }
    auto left_state = plans_.find(left);
    auto right_state = plans_.find(right);
    if (left_state == plans_.end() || right_state == plans_.end()) {
        return;
    }
    auto& left_set = query_graph_manager_.GetSetManager().GetJoinRelation(left);
    auto& right_set = query_graph_manager_.GetSetManager().GetJoinRelation(right);
    auto connections = query_graph_manager_.GetQueryGraphEdges().GetConnections(left_set, right_set);
    if (connections.empty()) {
        return;
    }
    auto try_record = [&](uint64_t candidate_left,
                          uint64_t candidate_right,
                          const std::vector<const JoinOrderNeighborInfo*>& candidate_connections) {
        size_t cardinality = cost_model_.EstimateCardinality(
            candidate_left, candidate_right, plans_, relations_, candidate_connections);
        double cost = cost_model_.ComputeCost(candidate_left, candidate_right, cardinality, plans_);
        uint64_t union_mask = candidate_left | candidate_right;
        auto existing = plans_.find(union_mask);
        bool replace = existing == plans_.end() || cost < existing->second->cost;

        if (!replace && existing != plans_.end() && cost == existing->second->cost) {
            int candidate_join_type = PrimaryJoinType(candidate_connections);
            if (IsSemiOrAntiJoinType(candidate_join_type)) {
                auto candidate_left_stats =
                    optimizer_.GetDPStats(candidate_left, plans_, relations_);
                auto existing_left_stats =
                    optimizer_.GetDPStats(existing->second->left_set.mask, plans_, relations_);
                replace = candidate_left_stats.cardinality < existing_left_stats.cardinality;
            } else {
                const size_t candidate_filter_count = ConnectionFilterCount(candidate_connections);
                const size_t existing_filter_count =
                    existing->second->info ? existing->second->info->filters.size() : 0;
                if (candidate_filter_count != existing_filter_count) {
                    replace = candidate_filter_count > existing_filter_count;
                } else {
                    auto candidate_right_stats =
                        optimizer_.GetDPStats(candidate_right, plans_, relations_);
                    auto existing_right_stats =
                        optimizer_.GetDPStats(existing->second->right_set.mask, plans_, relations_);
                    if (candidate_right_stats.cardinality != existing_right_stats.cardinality) {
                        replace = candidate_right_stats.cardinality < existing_right_stats.cardinality;
                    } else {
                        auto candidate_left_stats =
                            optimizer_.GetDPStats(candidate_left, plans_, relations_);
                        auto existing_left_stats =
                            optimizer_.GetDPStats(existing->second->left_set.mask, plans_, relations_);
                        if (candidate_left_stats.cardinality != existing_left_stats.cardinality) {
                            replace = candidate_left_stats.cardinality < existing_left_stats.cardinality;
                        } else {
                            replace = cardinality < existing->second->cardinality;
                        }
                    }
                }
            }
        }

        if (!replace) {
            return;
        }

        auto& candidate_left_set = query_graph_manager_.GetSetManager().GetJoinRelation(candidate_left);
        auto& candidate_right_set = query_graph_manager_.GetSetManager().GetJoinRelation(candidate_right);
        auto& union_set = query_graph_manager_.GetSetManager().Union(candidate_left_set, candidate_right_set);
        auto node = std::make_unique<JoinOrderDPJoinNode>(
            union_set, candidate_connections.front(), candidate_left_set, candidate_right_set, cost);
        node->cardinality = cardinality;
        plans_[union_mask] = std::move(node);
        optimizer_.dp_stats_[union_mask] =
            cost_model_.CombineStats(candidate_left, candidate_right, cardinality, plans_, relations_, candidate_connections);
    };

    try_record(left, right, connections);

    int primary_join_type = PrimaryJoinType(connections);
    if (IsSemiOrAntiJoinType(primary_join_type)) {
        auto reverse_connections = query_graph_manager_.GetQueryGraphEdges().GetConnections(right_set, left_set);
        if (!reverse_connections.empty()) {
            try_record(right, left, reverse_connections);
        }
    }
}

bool JoinOrderPlanEnumerator::EnumerateCmp(uint64_t left, uint64_t right, uint64_t exclusion) {
    auto& right_set = query_graph_manager_.GetSetManager().GetJoinRelation(right);
    auto neighbors = query_graph_manager_.GetQueryGraphEdges().GetNeighbors(right_set, exclusion);
    if (neighbors.empty()) {
        return true;
    }

    auto subsets = GetAllNeighborSets(neighbors);
    std::vector<uint64_t> union_sets;
    union_sets.reserve(subsets.size());
    for (auto subset : subsets) {
        uint64_t combined = right | subset;
        if (combined == right) {
            return false;
        }
        auto combined_state = plans_.find(combined);
        auto& left_set = query_graph_manager_.GetSetManager().GetJoinRelation(left);
        auto& combined_set = query_graph_manager_.GetSetManager().GetJoinRelation(combined);
        if (combined_state != plans_.end() &&
            !query_graph_manager_.GetQueryGraphEdges().GetConnections(left_set, combined_set).empty()) {
            TryEmitPair(left, combined);
        }
        union_sets.push_back(combined);
    }

    uint64_t next_exclusion = exclusion | MaskFromRelations(neighbors);
    for (auto union_set : union_sets) {
        if (!EnumerateCmp(left, union_set, next_exclusion)) {
            return false;
        }
    }
    return true;
}

bool JoinOrderPlanEnumerator::EmitCSG(uint64_t node) {
    if (__builtin_popcountll(node) == component_relation_count_) {
        return true;
    }
    uint64_t exclusion = LowerRelationsMask(LowestRelationIndex(node)) | node;
    auto& node_set = query_graph_manager_.GetSetManager().GetJoinRelation(node);
    auto neighbors = query_graph_manager_.GetQueryGraphEdges().GetNeighbors(node_set, exclusion);
    if (neighbors.empty()) {
        return true;
    }
    std::sort(neighbors.begin(), neighbors.end(), std::greater<size_t>());

    uint64_t next_exclusion = exclusion;
    for (auto neighbor : neighbors) {
        next_exclusion |= (uint64_t{1} << neighbor);
    }

    for (auto neighbor : neighbors) {
        auto neighbor_relation = uint64_t{1} << neighbor;
        auto& neighbor_set = query_graph_manager_.GetSetManager().GetJoinRelation(neighbor_relation);
        if (!query_graph_manager_.GetQueryGraphEdges().GetConnections(node_set, neighbor_set).empty()) {
            TryEmitPair(node, neighbor_relation);
        }
        if (!EnumerateCmp(node, neighbor_relation, next_exclusion)) {
            return false;
        }
        next_exclusion &= ~neighbor_relation;
    }
    return true;
}

bool JoinOrderPlanEnumerator::EnumerateCSGRecursive(uint64_t node, uint64_t exclusion) {
    auto& node_set = query_graph_manager_.GetSetManager().GetJoinRelation(node);
    auto neighbors = query_graph_manager_.GetQueryGraphEdges().GetNeighbors(node_set, exclusion);
    if (neighbors.empty()) {
        return true;
    }

    auto subsets = GetAllNeighborSets(neighbors);
    std::vector<uint64_t> union_sets;
    union_sets.reserve(subsets.size());
    for (auto subset : subsets) {
        auto new_set = node | subset;
        if (new_set == node) {
            return false;
        }
        auto new_state = plans_.find(new_set);
        if (new_state != plans_.end()) {
            if (!EmitCSG(new_set)) {
                return false;
            }
        }
        union_sets.push_back(new_set);
    }

    uint64_t next_exclusion = exclusion | MaskFromRelations(neighbors);
    for (auto union_set : union_sets) {
        if (!EnumerateCSGRecursive(union_set, next_exclusion)) {
            return false;
        }
    }
    return true;
}

bool JoinOrderPlanEnumerator::SolveJoinOrderExactly() {
    query_graph_manager_.Build(conditions_);
    for (size_t idx = relations_.size(); idx > 0; --idx) {
        uint64_t seed = uint64_t{1} << (idx - 1);
        if ((subset_mask_ & seed) == 0) {
            continue;
        }
        if (!EmitCSG(seed)) {
            return false;
        }
        uint64_t exclusion = LowerRelationsMask(idx);
        if (!EnumerateCSGRecursive(seed, exclusion)) {
            return false;
        }
    }
    return true;
}

size_t JoinOrderPlanEnumerator::HighestRelationIndex(uint64_t mask) const {
    for (size_t idx = relations_.size(); idx > 0; --idx) {
        if ((mask & (uint64_t{1} << (idx - 1))) != 0) {
            return idx - 1;
        }
    }
    return 0;
}

uint64_t JoinOrderPlanEnumerator::LowerRelationsMask(size_t upper_bound) const {
    uint64_t result = 0;
    for (size_t idx = 0; idx < upper_bound; ++idx) {
        uint64_t bit = uint64_t{1} << idx;
        if ((subset_mask_ & bit) != 0) {
            result |= bit;
        }
    }
    return result;
}

uint64_t JoinOrderPlanEnumerator::MaskFromRelations(const std::vector<size_t>& relations) const {
    uint64_t result = 0;
    for (auto relation : relations) {
        result |= uint64_t{1} << relation;
    }
    return result;
}

} // namespace yaap
