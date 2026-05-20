#include "join_order_query_graph.hpp"

#include "join_order_reconstruct.hpp"

namespace yaap {

std::unique_ptr<LogicalOperator> JoinOrderQueryGraphManager::Reconstruct(
    uint64_t mask,
    const std::map<uint64_t, std::unique_ptr<JoinOrderDPJoinNode>>& plans,
    std::vector<JoinOrderJoinRelation>& relations,
    const std::vector<JoinOrderJoinCondition>& conditions) const {
	JoinOrderReconstructor reconstructor(*this);
	return reconstructor.Reconstruct(mask, plans, relations, conditions);
}

} // namespace yaap
