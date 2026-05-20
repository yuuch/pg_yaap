#include "join_order_plan_node.hpp"

namespace yaap {

JoinOrderDPJoinNode::JoinOrderDPJoinNode(JoinOrderRelationSet& set)
    : set(set),
      info(nullptr),
      is_leaf(true),
      left_set(set),
      right_set(set) {
}

JoinOrderDPJoinNode::JoinOrderDPJoinNode(JoinOrderRelationSet& set,
                                         const JoinOrderNeighborInfo* info,
                                         JoinOrderRelationSet& left_set,
                                         JoinOrderRelationSet& right_set,
                                         double cost)
    : set(set),
      info(info),
      is_leaf(false),
      left_set(left_set),
      right_set(right_set),
      cost(cost) {
}

} // namespace yaap

