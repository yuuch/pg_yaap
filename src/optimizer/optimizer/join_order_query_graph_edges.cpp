#include "join_order_query_graph.hpp"

namespace yaap {

size_t LowestRelationIndex(uint64_t mask) {
    for (size_t idx = 0; idx < 64; ++idx) {
        if ((mask & (uint64_t{1} << idx)) != 0) {
            return idx;
        }
    }
    return 64;
}

std::vector<size_t> RelationIndexes(uint64_t mask) {
    std::vector<size_t> indexes;
    for (size_t idx = 0; idx < 64; ++idx) {
        if ((mask & (uint64_t{1} << idx)) != 0) {
            indexes.push_back(idx);
        }
    }
    return indexes;
}

bool IsMaskSubset(uint64_t candidate, uint64_t container) {
    return (candidate & container) == candidate;
}

void JoinOrderQueryGraphEdges::CreateEdge(JoinOrderRelationSet& left,
                                          JoinOrderRelationSet& right,
                                          const JoinOrderFilterInfo* filter_info) {
    auto* edge = GetQueryEdge(left);
    for (auto& neighbor : edge->neighbors) {
        if (neighbor.neighbor.mask == right.mask) {
            if (filter_info) {
                neighbor.filters.push_back(filter_info);
            }
            return;
        }
    }
    edge->neighbors.emplace_back(right);
    if (filter_info) {
        edge->neighbors.back().filters.push_back(filter_info);
    }
}

std::vector<size_t> JoinOrderQueryGraphEdges::GetNeighbors(const JoinOrderRelationSet& node, uint64_t exclusion) const {
    uint64_t result = 0;
    EnumerateNeighbors(node, [&](const JoinOrderNeighborInfo& info) {
        auto neighbor_idx = LowestRelationIndex(info.neighbor.mask);
        if ((exclusion & (uint64_t{1} << neighbor_idx)) == 0) {
            result |= uint64_t{1} << neighbor_idx;
        }
        return false;
    });
    return RelationIndexes(result);
}

std::vector<const JoinOrderNeighborInfo*> JoinOrderQueryGraphEdges::GetConnections(
    const JoinOrderRelationSet& node,
    const JoinOrderRelationSet& other) const {
    std::vector<const JoinOrderNeighborInfo*> connections;
    EnumerateNeighbors(node, [&](const JoinOrderNeighborInfo& info) {
        if (JoinOrderRelationSet::IsSubset(other, info.neighbor)) {
            connections.push_back(&info);
        }
        return false;
    });
    return connections;
}

JoinOrderQueryGraphEdges::QueryEdge* JoinOrderQueryGraphEdges::GetQueryEdge(const JoinOrderRelationSet& left) {
    auto* edge = &root_;
    for (auto relation : left.relations) {
        auto entry = edge->children.find(relation);
        if (entry == edge->children.end()) {
            entry = edge->children.emplace(relation, std::make_unique<QueryEdge>()).first;
        }
        edge = entry->second.get();
    }
    return edge;
}

void JoinOrderQueryGraphEdges::EnumerateNeighbors(
    const JoinOrderRelationSet& node,
    const std::function<bool(const JoinOrderNeighborInfo&)>& callback) const {
    const auto& node_relations = node.relations;
    for (size_t idx = 0; idx < node_relations.size(); ++idx) {
        auto iter = root_.children.find(node_relations[idx]);
        if (iter != root_.children.end()) {
            if (EnumerateNeighborsDFS(node_relations, *iter->second, idx + 1, callback)) {
                return;
            }
        }
    }
}

bool JoinOrderQueryGraphEdges::EnumerateNeighborsDFS(
    const std::vector<size_t>& node_relations,
    const QueryEdge& edge,
    size_t index,
    const std::function<bool(const JoinOrderNeighborInfo&)>& callback) const {
    for (auto& neighbor : edge.neighbors) {
        if (callback(neighbor)) {
            return true;
        }
    }

    for (size_t node_idx = index; node_idx < node_relations.size(); ++node_idx) {
        auto iter = edge.children.find(node_relations[node_idx]);
        if (iter != edge.children.end()) {
            if (EnumerateNeighborsDFS(node_relations, *iter->second, node_idx + 1, callback)) {
                return true;
            }
        }
    }
    return false;
}

} // namespace yaap
