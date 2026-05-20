#pragma once

#include <map>
#include <memory>
#include <vector>

#include "join_order_query_graph.hpp"

namespace yaap {

struct JoinOrderGenerateRelation {
    JoinOrderGenerateRelation(JoinOrderRelationSet* set, std::unique_ptr<LogicalOperator> op)
        : set(set), op(std::move(op)) {
    }

    JoinOrderRelationSet* set;
    std::unique_ptr<LogicalOperator> op;
};

class JoinOrderReconstructor {
public:
    explicit JoinOrderReconstructor(const JoinOrderQueryGraphManager& query_graph_manager);

    std::unique_ptr<LogicalOperator> Reconstruct(
        uint64_t mask,
        const std::map<uint64_t, std::unique_ptr<JoinOrderDPJoinNode>>& plans,
        std::vector<JoinOrderJoinRelation>& relations,
        const std::vector<JoinOrderJoinCondition>& conditions) const;

private:
    JoinOrderGenerateRelation GenerateJoins(
        uint64_t mask,
        const std::map<uint64_t, std::unique_ptr<JoinOrderDPJoinNode>>& plans,
        std::vector<JoinOrderJoinRelation>& relations,
        const std::vector<JoinOrderJoinCondition>& conditions) const;
    void CollectNodeFilters(const JoinOrderDPJoinNode& node,
                            uint64_t mask,
                            std::vector<const JoinOrderFilterInfo*>& join_filters,
                            std::vector<const JoinOrderFilterInfo*>& residual_filters) const;
    const JoinOrderFilterInfo* ChoosePrimaryJoinFilter(
        const std::vector<const JoinOrderFilterInfo*>& join_filters) const;
    bool FilterBelongsToNode(const JoinOrderFilterInfo& filter,
                             const JoinOrderDPJoinNode& node,
                             uint64_t mask) const;
    std::unique_ptr<LogicalOperator> MakeJoinNode(
        const JoinOrderDPJoinNode& node,
        JoinOrderGenerateRelation left,
        JoinOrderGenerateRelation right,
        std::vector<const JoinOrderFilterInfo*> join_filters,
        std::vector<const JoinOrderFilterInfo*> residual_filters,
        int join_type) const;
    bool IsInvertedFilter(const JoinOrderFilterInfo& filter,
                          const JoinOrderRelationSet& left_set,
                          const JoinOrderRelationSet& right_set) const;
    std::unique_ptr<Expression> CloneOrientedFilter(const JoinOrderFilterInfo& filter,
                                                    const JoinOrderRelationSet& left_set,
                                                    const JoinOrderRelationSet& right_set) const;

    const JoinOrderQueryGraphManager& query_graph_manager_;
};

} // namespace yaap
