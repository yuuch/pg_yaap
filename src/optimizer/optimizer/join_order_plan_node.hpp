#pragma once

#include <cstddef>
#include <cstdint>

#include "join_order_query_graph.hpp"
#include "join_order_relation_set.hpp"

namespace yaap {

class JoinOrderDPJoinNode {
public:
    explicit JoinOrderDPJoinNode(JoinOrderRelationSet& set);
    JoinOrderDPJoinNode(JoinOrderRelationSet& set,
                        const JoinOrderNeighborInfo* info,
                        JoinOrderRelationSet& left_set,
                        JoinOrderRelationSet& right_set,
                        double cost);

    JoinOrderRelationSet& set;
    const JoinOrderNeighborInfo* info;
    bool is_leaf;
    JoinOrderRelationSet& left_set;
    JoinOrderRelationSet& right_set;
    double cost = 0;
    size_t cardinality = 0;
};

} // namespace yaap

