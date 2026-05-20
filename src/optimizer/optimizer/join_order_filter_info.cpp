#include "join_order_filter_info.hpp"

namespace yaap {

JoinOrderFilterInfo::JoinOrderFilterInfo(const JoinOrderJoinCondition& condition,
                                         JoinOrderRelationSet& left_set,
                                         JoinOrderRelationSet& right_set,
                                         size_t filter_index,
                                         int join_type)
    : condition(condition),
      left_set(left_set),
      right_set(right_set),
      filter_index(filter_index),
      join_type(join_type) {
}

void CollectColumnBindingsByMask(Expression* expression,
                                 uint64_t left_mask,
                                 uint64_t right_mask,
                                 JoinOrderFilterInfo& filter_info) {
    if (!expression) {
        return;
    }

    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* column_ref = static_cast<BoundColumnRefExpression*>(expression);
            auto table_mask = uint64_t{1} << column_ref->binding.table_index.index;
            if (!filter_info.has_left_binding && (left_mask & table_mask) != 0) {
                filter_info.left_binding = column_ref->binding;
                filter_info.has_left_binding = true;
            } else if (!filter_info.has_right_binding && (right_mask & table_mask) != 0) {
                filter_info.right_binding = column_ref->binding;
                filter_info.has_right_binding = true;
            }
            return;
        }
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            for (auto& child : function->children) {
                CollectColumnBindingsByMask(child.get(), left_mask, right_mask, filter_info);
            }
            return;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            for (auto& child : conjunction->children) {
                CollectColumnBindingsByMask(child.get(), left_mask, right_mask, filter_info);
            }
            return;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression);
            for (auto& child : aggregate->children) {
                CollectColumnBindingsByMask(child.get(), left_mask, right_mask, filter_info);
            }
            return;
        }
        case ExpressionType::BOUND_SUBQUERY:
        case ExpressionType::BOUND_CONSTANT:
        case ExpressionType::BOUND_COMPARISON:
        case ExpressionType::OPAQUE:
            return;
    }
}

uint64_t ExtractExpressionMask(Expression* expression) {
    if (!expression) {
        return 0;
    }

    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* column_ref = static_cast<BoundColumnRefExpression*>(expression);
            return uint64_t{1} << column_ref->binding.table_index.index;
        }
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            uint64_t mask = 0;
            for (auto& child : function->children) {
                mask |= ExtractExpressionMask(child.get());
            }
            return mask;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            uint64_t mask = 0;
            for (auto& child : conjunction->children) {
                mask |= ExtractExpressionMask(child.get());
            }
            return mask;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression);
            uint64_t mask = 0;
            for (auto& child : aggregate->children) {
                mask |= ExtractExpressionMask(child.get());
            }
            return mask;
        }
        case ExpressionType::BOUND_SUBQUERY:
        case ExpressionType::BOUND_CONSTANT:
        case ExpressionType::BOUND_COMPARISON:
        case ExpressionType::OPAQUE:
            return 0;
    }
}

bool TryExtractJoinSides(Expression* expression, uint64_t& left_mask, uint64_t& right_mask) {
    if (!expression) {
        return false;
    }

    switch (expression->type) {
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            if (function->children.size() != 2) {
                return false;
            }
            auto first_mask = ExtractExpressionMask(function->children[0].get());
            auto second_mask = ExtractExpressionMask(function->children[1].get());
            if (first_mask == 0 || second_mask == 0 || (first_mask & second_mask) != 0) {
                return false;
            }
            left_mask = first_mask;
            right_mask = second_mask;
            return true;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            if (conjunction->bool_expr_type != 0 || conjunction->children.empty()) {
                return false;
            }
            uint64_t aggregate_left = 0;
            uint64_t aggregate_right = 0;
            bool initialized = false;
            for (auto& child : conjunction->children) {
                uint64_t child_left = 0;
                uint64_t child_right = 0;
                if (!TryExtractJoinSides(child.get(), child_left, child_right)) {
                    return false;
                }
                if (!initialized) {
                    aggregate_left = child_left;
                    aggregate_right = child_right;
                    initialized = true;
                    continue;
                }
                bool same_orientation = (child_left & aggregate_right) == 0 && (child_right & aggregate_left) == 0;
                bool reverse_orientation = (child_left & aggregate_left) == 0 && (child_right & aggregate_right) == 0;
                if (same_orientation) {
                    aggregate_left |= child_left;
                    aggregate_right |= child_right;
                } else if (reverse_orientation) {
                    aggregate_left |= child_right;
                    aggregate_right |= child_left;
                } else {
                    return false;
                }
            }
            if (!initialized || aggregate_left == 0 || aggregate_right == 0 || (aggregate_left & aggregate_right) != 0) {
                return false;
            }
            left_mask = aggregate_left;
            right_mask = aggregate_right;
            return true;
        }
        case ExpressionType::BOUND_COLUMN_REF:
        case ExpressionType::BOUND_CONSTANT:
        case ExpressionType::BOUND_AGGREGATE:
        case ExpressionType::BOUND_SUBQUERY:
        case ExpressionType::BOUND_COMPARISON:
        case ExpressionType::OPAQUE:
            return false;
    }
}

} // namespace yaap

