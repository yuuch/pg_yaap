#pragma once

#include <cstdint>

#include "join_order_relation_set.hpp"
#include "optimizer_core.hpp"

namespace yaap {

struct JoinOrderFilterInfo {
    JoinOrderFilterInfo(const JoinOrderJoinCondition& condition,
                        JoinOrderRelationSet& left_set,
                        JoinOrderRelationSet& right_set,
                        size_t filter_index,
                        int join_type);

    const JoinOrderJoinCondition& condition;
    JoinOrderRelationSet& left_set;
    JoinOrderRelationSet& right_set;
    size_t filter_index = 0;
    int join_type = 0;
    bool invert_result = false;
    ColumnBinding left_binding{};
    ColumnBinding right_binding{};
    bool has_left_binding = false;
    bool has_right_binding = false;
    bool from_residual_predicate = false;
};

void CollectColumnBindingsByMask(Expression* expression,
                                 uint64_t left_mask,
                                 uint64_t right_mask,
                                 JoinOrderFilterInfo& filter_info);

uint64_t ExtractExpressionMask(Expression* expression);
bool TryExtractJoinSides(Expression* expression, uint64_t& left_mask, uint64_t& right_mask);

} // namespace yaap
