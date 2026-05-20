#include "join_order_relation_set.hpp"

namespace yaap {

JoinOrderRelationSet::JoinOrderRelationSet(uint64_t mask)
    : mask(mask) {
    for (size_t idx = 0; idx < 64; ++idx) {
        if ((mask & (uint64_t{1} << idx)) != 0) {
            relations.push_back(idx);
        }
    }
}

bool JoinOrderRelationSet::IsSubset(const JoinOrderRelationSet& super_set, const JoinOrderRelationSet& sub_set) {
    return (sub_set.mask & super_set.mask) == sub_set.mask;
}

JoinOrderRelationSet& JoinOrderRelationSetManager::GetJoinRelation(uint64_t mask) {
    auto entry = relation_sets_.find(mask);
    if (entry == relation_sets_.end()) {
        entry = relation_sets_.emplace(mask, std::make_unique<JoinOrderRelationSet>(mask)).first;
    }
    return *entry->second;
}

JoinOrderRelationSet& JoinOrderRelationSetManager::Union(const JoinOrderRelationSet& left,
                                                         const JoinOrderRelationSet& right) {
    return GetJoinRelation(left.mask | right.mask);
}

} // namespace yaap

