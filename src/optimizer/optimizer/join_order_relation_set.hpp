#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <vector>

namespace yaap {

struct JoinOrderRelationSet {
    explicit JoinOrderRelationSet(uint64_t mask);

    uint64_t mask = 0;
    std::vector<size_t> relations;

    static bool IsSubset(const JoinOrderRelationSet& super_set, const JoinOrderRelationSet& sub_set);
};

class JoinOrderRelationSetManager {
public:
    JoinOrderRelationSet& GetJoinRelation(uint64_t mask);
    JoinOrderRelationSet& Union(const JoinOrderRelationSet& left, const JoinOrderRelationSet& right);

private:
    std::map<uint64_t, std::unique_ptr<JoinOrderRelationSet>> relation_sets_;
};

} // namespace yaap

