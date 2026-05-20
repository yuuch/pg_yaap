#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <vector>

#include "join_order_filter_info.hpp"
#include "join_order_relation_set.hpp"
#include "optimizer_core.hpp"

namespace yaap {

struct JoinOrderNeighborInfo {
    explicit JoinOrderNeighborInfo(JoinOrderRelationSet& neighbor) : neighbor(neighbor) {
    }

    JoinOrderRelationSet& neighbor;
    std::vector<const JoinOrderFilterInfo*> filters;
};

class JoinOrderQueryGraphEdges {
public:
    void CreateEdge(JoinOrderRelationSet& left, JoinOrderRelationSet& right, const JoinOrderFilterInfo* filter_info);
    std::vector<size_t> GetNeighbors(const JoinOrderRelationSet& node, uint64_t exclusion) const;
    std::vector<const JoinOrderNeighborInfo*> GetConnections(const JoinOrderRelationSet& node,
                                                             const JoinOrderRelationSet& other) const;

private:
    struct QueryEdge {
        std::vector<JoinOrderNeighborInfo> neighbors;
        std::map<size_t, std::unique_ptr<QueryEdge>> children;
    };

    QueryEdge* GetQueryEdge(const JoinOrderRelationSet& left);
    void EnumerateNeighbors(const JoinOrderRelationSet& node,
                            const std::function<bool(const JoinOrderNeighborInfo&)>& callback) const;
    bool EnumerateNeighborsDFS(const std::vector<size_t>& node_relations,
                               const QueryEdge& edge,
                               size_t index,
                               const std::function<bool(const JoinOrderNeighborInfo&)>& callback) const;

    QueryEdge root_;
};

class JoinOrderQueryGraphManager {
public:
    explicit JoinOrderQueryGraphManager(uint64_t subset_mask);

    void Build(const std::vector<JoinOrderJoinCondition>& conditions);
    std::unique_ptr<LogicalOperator> Reconstruct(
        uint64_t mask,
        const std::map<uint64_t, std::unique_ptr<JoinOrderDPJoinNode>>& plans,
        std::vector<JoinOrderJoinRelation>& relations,
        const std::vector<JoinOrderJoinCondition>& conditions) const;
    const JoinOrderQueryGraphEdges& GetQueryGraphEdges() const;
    const std::vector<std::unique_ptr<JoinOrderFilterInfo>>& GetFilterInfos() const;
    const JoinOrderRelationSetManager& GetSetManager() const;
    JoinOrderRelationSetManager& GetSetManager();

private:
    uint64_t subset_mask_;
    JoinOrderRelationSetManager set_manager_;
    std::vector<std::unique_ptr<JoinOrderFilterInfo>> filter_infos_;
    JoinOrderQueryGraphEdges query_graph_;
};

size_t LowestRelationIndex(uint64_t mask);
std::vector<size_t> RelationIndexes(uint64_t mask);
bool IsMaskSubset(uint64_t candidate, uint64_t container);

} // namespace yaap
