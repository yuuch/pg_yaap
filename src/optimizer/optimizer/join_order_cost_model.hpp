#pragma once

#include <cstdint>
#include <map>
#include <vector>

#include "join_order_query_graph.hpp"
#include "optimizer_core.hpp"

namespace yaap {

class JoinOrderCostModel {
public:
    explicit JoinOrderCostModel(JoinOrderOptimizer& optimizer);

    size_t EstimateCardinality(uint64_t left,
                               uint64_t right,
                               const std::map<uint64_t, std::unique_ptr<JoinOrderDPJoinNode>>& plans,
                               const std::vector<JoinOrderJoinRelation>& relations,
                               const std::vector<const JoinOrderNeighborInfo*>& connections) const;

    double ComputeCost(uint64_t left,
                       uint64_t right,
                       size_t join_cardinality,
                       const std::map<uint64_t, std::unique_ptr<JoinOrderDPJoinNode>>& plans) const;

    RelationStats CombineStats(uint64_t left,
                               uint64_t right,
                               size_t join_cardinality,
                               const std::map<uint64_t, std::unique_ptr<JoinOrderDPJoinNode>>& plans,
                               const std::vector<JoinOrderJoinRelation>& relations,
                               const std::vector<const JoinOrderNeighborInfo*>& connections) const;

private:
    JoinOrderOptimizer& optimizer_;
};

} // namespace yaap
