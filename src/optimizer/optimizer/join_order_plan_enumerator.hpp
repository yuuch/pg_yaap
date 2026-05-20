#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "join_order_cost_model.hpp"
#include "join_order_plan_node.hpp"
#include "join_order_query_graph.hpp"

namespace yaap {

class JoinOrderPlanEnumerator {
public:
    JoinOrderPlanEnumerator(JoinOrderOptimizer& optimizer,
                            uint64_t subset_mask,
                            std::vector<JoinOrderJoinRelation>& relations,
                            const std::vector<JoinOrderJoinCondition>& conditions);

    std::unique_ptr<LogicalOperator> Solve();

private:
    void InitializeLeafPlans();
    std::vector<uint64_t> AddSuperSets(const std::vector<uint64_t>& current,
                                       const std::vector<size_t>& all_neighbors) const;
    std::vector<uint64_t> GetAllNeighborSets(std::vector<size_t> neighbors) const;
    void TryEmitPair(uint64_t left, uint64_t right);
    bool EnumerateCmp(uint64_t left, uint64_t right, uint64_t exclusion);
    bool EmitCSG(uint64_t node);
    bool EnumerateCSGRecursive(uint64_t node, uint64_t exclusion);
    bool SolveJoinOrderExactly();
    size_t HighestRelationIndex(uint64_t mask) const;
    uint64_t LowerRelationsMask(size_t upper_bound) const;
    uint64_t MaskFromRelations(const std::vector<size_t>& relations) const;

    JoinOrderOptimizer& optimizer_;
    uint64_t subset_mask_;
    std::vector<JoinOrderJoinRelation>& relations_;
    const std::vector<JoinOrderJoinCondition>& conditions_;
    size_t component_relation_count_;
    JoinOrderQueryGraphManager query_graph_manager_;
    JoinOrderCostModel cost_model_;
    std::map<uint64_t, std::unique_ptr<JoinOrderDPJoinNode>> plans_;
};

} // namespace yaap
