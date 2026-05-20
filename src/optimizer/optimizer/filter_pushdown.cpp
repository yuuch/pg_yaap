#include "optimizer_core.hpp"

#include "../logical/expression_rewriters.hpp"
#include "../logical/filter_rewrite_utils.hpp"
#include "../logical/logical_utils.hpp"
#include "../logical/mark_join_normalization.hpp"

namespace yaap {

namespace {

void SplitFiltersByChildTables(std::vector<std::unique_ptr<Expression>>& expressions,
                               const std::set<size_t>& left_tables,
                               const std::set<size_t>& right_tables,
                               bool route_non_left_to_right,
                               std::vector<std::unique_ptr<Expression>>& keep_filters,
                               std::vector<std::unique_ptr<Expression>>& left_filters,
                               std::vector<std::unique_ptr<Expression>>& right_filters) {
    for (auto& expression : expressions) {
        std::set<size_t> referenced_tables;
        CollectReferencedTables(expression.get(), referenced_tables);

        if (!referenced_tables.empty() && IsSubset(referenced_tables, left_tables)) {
            left_filters.push_back(std::move(expression));
        } else if (!referenced_tables.empty() && IsSubset(referenced_tables, right_tables)) {
            right_filters.push_back(std::move(expression));
        } else if (route_non_left_to_right && !referenced_tables.empty()) {
            bool references_left = false;
            for (auto table_index : referenced_tables) {
                if (left_tables.find(table_index) != left_tables.end()) {
                    references_left = true;
                    break;
                }
            }
            if (!references_left) {
                right_filters.push_back(std::move(expression));
            } else {
                keep_filters.push_back(std::move(expression));
            }
        } else {
            keep_filters.push_back(std::move(expression));
        }
    }
}

std::unique_ptr<LogicalOperator> MakeCrossProductNode(std::unique_ptr<LogicalOperator> left,
                                                      std::unique_ptr<LogicalOperator> right) {
    auto cross_product = std::make_unique<LogicalCrossProduct>();
    cross_product->children.push_back(std::move(left));
    cross_product->children.push_back(std::move(right));
    cross_product->estimated_cardinality =
        cross_product->children[0]->estimated_cardinality * cross_product->children[1]->estimated_cardinality;
    return cross_product;
}

struct JoinConditionPushPolicy {
    bool push_left = false;
    bool push_right = false;
    bool convert_empty_inner_join = false;
};

JoinConditionPushPolicy GetJoinConditionPushPolicy(const LogicalOperator& join_plan) {
    if (join_plan.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
        join_plan.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        return {false, true, false};
    }
    if (join_plan.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        return {};
    }

    auto& join = static_cast<const LogicalComparisonJoin&>(join_plan);
    switch (join.join_type) {
        case JOIN_INNER:
            return {true, true, true};
        case JOIN_SEMI:
            return {true, true, false};
        case JOIN_ANTI:
            return {false, true, false};
        case JOIN_MARK:
        case JOIN_SINGLE:
            return {false, true, false};
        default:
            return {};
    }
}

std::unique_ptr<LogicalOperator> PushJoinConditionFilters(FilterPushdown& rewriter,
                                                          std::unique_ptr<LogicalOperator> join_plan) {
    if (!join_plan ||
        (join_plan->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
         join_plan->type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN &&
         join_plan->type != LogicalOperatorType::LOGICAL_DELIM_JOIN)) {
        return join_plan;
    }

    auto* join = static_cast<LogicalComparisonJoin*>(join_plan.get());
    SplitConjunctionList(join->conditions);

    std::set<size_t> left_tables;
    std::set<size_t> right_tables;
    CollectOutputTables(join->children[0].get(), left_tables);
    CollectOutputTables(join->children[1].get(), right_tables);

    std::vector<std::unique_ptr<Expression>> keep_conditions;
    std::vector<std::unique_ptr<Expression>> left_filters;
    std::vector<std::unique_ptr<Expression>> right_filters;

    auto push_policy = GetJoinConditionPushPolicy(*join_plan);
    auto* comparison_join = static_cast<LogicalComparisonJoin*>(join_plan.get());
    const bool route_non_left_to_right =
        push_policy.push_right &&
        (comparison_join->join_type == JOIN_SEMI || comparison_join->join_type == JOIN_ANTI);
    SplitFiltersByChildTables(join->conditions, left_tables, right_tables, route_non_left_to_right,
                              keep_conditions, left_filters, right_filters);

    if (push_policy.push_left && !left_filters.empty()) {
        join->children[0] = rewriter.Optimize(MakeFilterNode(std::move(join->children[0]), std::move(left_filters)));
    } else {
        for (auto& expression : left_filters) {
            keep_conditions.push_back(std::move(expression));
        }
    }

    if (push_policy.push_right && !right_filters.empty()) {
        join->children[1] = rewriter.Optimize(MakeFilterNode(std::move(join->children[1]), std::move(right_filters)));
    } else {
        for (auto& expression : right_filters) {
            keep_conditions.push_back(std::move(expression));
        }
    }

    join->conditions = std::move(keep_conditions);
    if (push_policy.convert_empty_inner_join && join->conditions.empty()) {
        return MakeCrossProductNode(std::move(join->children[0]), std::move(join->children[1]));
    }
    return join_plan;
}

} // namespace

OptimizerPass FilterPushdown::Pass() const {
    return OptimizerPass::FILTER_PUSHDOWN;
}

void FilterPushdown::SplitConjunctions(std::vector<std::unique_ptr<Expression>>& expressions) {
    SplitConjunctionList(expressions);
}

void FilterPushdown::CheckMarkToSemi(LogicalOperator* plan, std::set<size_t> table_bindings) {
    DisableMarkJoinConversionForConsumedMarkers(plan, std::move(table_bindings));
}

std::unique_ptr<LogicalOperator> FilterPushdown::PushIntoCrossProduct(std::unique_ptr<LogicalOperator> filter_plan) {
    auto* filter = static_cast<LogicalFilter*>(filter_plan.get());
    SplitConjunctions(filter->expressions);

    auto cross_product = std::move(filter_plan->children[0]);
    auto* cross = static_cast<LogicalCrossProduct*>(cross_product.get());

    std::set<size_t> left_tables;
    std::set<size_t> right_tables;
    CollectOutputTables(cross->children[0].get(), left_tables);
    CollectOutputTables(cross->children[1].get(), right_tables);

    std::vector<std::unique_ptr<Expression>> keep_filters;
    std::vector<std::unique_ptr<Expression>> left_filters;
    std::vector<std::unique_ptr<Expression>> right_filters;
    SplitFiltersByChildTables(filter->expressions, left_tables, right_tables, false,
                              keep_filters, left_filters, right_filters);

    if (!left_filters.empty()) {
        cross->children[0] = Rewrite(MakeFilterNode(std::move(cross->children[0]), std::move(left_filters)));
    }
    if (!right_filters.empty()) {
        cross->children[1] = Rewrite(MakeFilterNode(std::move(cross->children[1]), std::move(right_filters)));
    }

    if (keep_filters.empty()) {
        cross_product->estimated_cardinality =
            cross_product->children[0]->estimated_cardinality * cross_product->children[1]->estimated_cardinality;
        return cross_product;
    }
    return MakeFilterNode(std::move(cross_product), std::move(keep_filters));
}

std::unique_ptr<LogicalOperator> FilterPushdown::PushIntoProjection(std::unique_ptr<LogicalOperator> filter_plan) {
    auto* filter = static_cast<LogicalFilter*>(filter_plan.get());
    SplitConjunctions(filter->expressions);

    auto projection_plan = std::move(filter_plan->children[0]);
    auto* projection = static_cast<LogicalProjection*>(projection_plan.get());
    if (projection->children.size() != 1 || !projection->children[0]) {
        return MakeFilterNode(std::move(projection_plan), std::move(filter->expressions));
    }

    std::map<std::pair<size_t, size_t>, Expression*> replacements;
    for (size_t i = 0; i < projection->expressions.size(); ++i) {
        replacements.emplace(std::make_pair(projection->table_index.index, i), projection->expressions[i].get());
    }

    std::vector<std::unique_ptr<Expression>> keep_filters;
    std::vector<std::unique_ptr<Expression>> push_filters;
    for (auto& expression : filter->expressions) {
        if (CanRewriteProjectionFilter(expression.get(), replacements)) {
            push_filters.push_back(CloneExpressionWithExpressionReplacements(expression.get(), replacements));
        } else {
            keep_filters.push_back(std::move(expression));
        }
    }

    if (!push_filters.empty()) {
        projection->children[0] = Rewrite(MakeFilterNode(std::move(projection->children[0]), std::move(push_filters)));
        projection_plan->estimated_cardinality = projection->children[0]->estimated_cardinality;
    }

    if (keep_filters.empty()) {
        return projection_plan;
    }
    return MakeFilterNode(std::move(projection_plan), std::move(keep_filters));
}

std::unique_ptr<LogicalOperator> FilterPushdown::PushIntoAggregate(std::unique_ptr<LogicalOperator> filter_plan) {
    auto* filter = static_cast<LogicalFilter*>(filter_plan.get());
    SplitConjunctions(filter->expressions);

    auto aggregate_plan = std::move(filter_plan->children[0]);
    auto* aggregate = static_cast<LogicalAggregate*>(aggregate_plan.get());
    if (aggregate->children.size() != 1 || !aggregate->children[0] || aggregate->groups.empty()) {
        return MakeFilterNode(std::move(aggregate_plan), std::move(filter->expressions));
    }

    std::map<std::pair<size_t, size_t>, Expression*> replacements;
    for (size_t i = 0; i < aggregate->groups.size(); ++i) {
        replacements.emplace(std::make_pair(aggregate->group_index.index, i), aggregate->groups[i].get());
    }

    std::vector<std::unique_ptr<Expression>> keep_filters;
    std::vector<std::unique_ptr<Expression>> push_filters;
    for (auto& expression : filter->expressions) {
        std::set<size_t> referenced_tables;
        CollectReferencedTables(expression.get(), referenced_tables);
        if (referenced_tables.empty() ||
            referenced_tables.find(aggregate->aggregate_index.index) != referenced_tables.end() ||
            !IsSubset(referenced_tables, std::set<size_t>{aggregate->group_index.index})) {
            keep_filters.push_back(std::move(expression));
            continue;
        }
        push_filters.push_back(CloneExpressionWithExpressionReplacements(expression.get(), replacements));
    }

    if (!push_filters.empty()) {
        aggregate->children[0] = Rewrite(MakeFilterNode(std::move(aggregate->children[0]), std::move(push_filters)));
    }

    if (keep_filters.empty()) {
        return aggregate_plan;
    }
    return MakeFilterNode(std::move(aggregate_plan), std::move(keep_filters));
}

std::unique_ptr<LogicalOperator> FilterPushdown::Rewrite(std::unique_ptr<LogicalOperator> plan) {
    for (auto& child : plan->children) {
        child = Rewrite(std::move(child));
    }

    if (plan->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
        plan->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
        plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        plan = PushJoinConditionFilters(*this, std::move(plan));
    }

    if (plan->type != LogicalOperatorType::LOGICAL_FILTER ||
        plan->children.size() != 1 ||
        !plan->children[0]) {
        return plan;
    }

    auto* filter = static_cast<LogicalFilter*>(plan.get());
    SplitConjunctions(filter->expressions);

    auto& child = plan->children[0];
    if (child->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
        return PushIntoCrossProduct(std::move(plan));
    }

    if (child->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
        child->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
        child->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        auto join_plan = std::move(child);
        auto* join = static_cast<LogicalComparisonJoin*>(join_plan.get());
        auto push_policy = GetJoinConditionPushPolicy(*join_plan);

        std::set<size_t> left_tables;
        std::set<size_t> right_tables;
        CollectOutputTables(join->children[0].get(), left_tables);
        CollectOutputTables(join->children[1].get(), right_tables);

        std::vector<std::unique_ptr<Expression>> keep_filters;
        std::vector<std::unique_ptr<Expression>> left_filters;
        std::vector<std::unique_ptr<Expression>> right_filters;
        const bool route_non_left_to_right =
            push_policy.push_right &&
            (join->join_type == JOIN_SEMI || join->join_type == JOIN_ANTI);
        SplitFiltersByChildTables(filter->expressions, left_tables, right_tables, route_non_left_to_right,
                                  keep_filters, left_filters, right_filters);

        if (!left_filters.empty()) {
            join->children[0] = Rewrite(MakeFilterNode(std::move(join->children[0]), std::move(left_filters)));
        }
        if (!right_filters.empty()) {
            if (push_policy.push_right) {
                join->children[1] = Rewrite(MakeFilterNode(std::move(join->children[1]), std::move(right_filters)));
            } else {
                for (auto& expression : right_filters) {
                    keep_filters.push_back(std::move(expression));
                }
            }
        }

		if (keep_filters.empty()) {
			return join_plan;
		}
		SimplifyExistsStyleMarkJoinFilter(join_plan, keep_filters);
		if (keep_filters.empty()) {
			return join_plan;
		}
		for (auto& expression : keep_filters) {
			join->conditions.push_back(std::move(expression));
		}
		return join_plan;
	}

    if (child->type == LogicalOperatorType::LOGICAL_PROJECTION &&
        child->children.size() == 1 &&
        child->children[0]) {
        return PushIntoProjection(std::move(plan));
    }

    if (child->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY &&
        child->children.size() == 1 &&
        child->children[0]) {
        return PushIntoAggregate(std::move(plan));
    }

    if ((child->type == LogicalOperatorType::LOGICAL_ORDER ||
         child->type == LogicalOperatorType::LOGICAL_DISTINCT) &&
        child->children.size() == 1 &&
        child->children[0]) {
        auto wrapper_plan = std::move(child);
        std::set<size_t> child_tables;
        CollectOutputTables(wrapper_plan->children[0].get(), child_tables);

        std::vector<std::unique_ptr<Expression>> keep_filters;
        std::vector<std::unique_ptr<Expression>> push_filters;
        for (auto& expression : filter->expressions) {
            std::set<size_t> referenced_tables;
            CollectReferencedTables(expression.get(), referenced_tables);
            if (!referenced_tables.empty() && IsSubset(referenced_tables, child_tables)) {
                push_filters.push_back(std::move(expression));
            } else {
                keep_filters.push_back(std::move(expression));
            }
        }

        if (!push_filters.empty()) {
            wrapper_plan->children[0] = Rewrite(MakeFilterNode(std::move(wrapper_plan->children[0]), std::move(push_filters)));
            wrapper_plan->estimated_cardinality = wrapper_plan->children[0]->estimated_cardinality;
        }
        if (keep_filters.empty()) {
            return wrapper_plan;
        }
        return MakeFilterNode(std::move(wrapper_plan), std::move(keep_filters));
    }

    return plan;
}

std::unique_ptr<LogicalOperator> FilterPushdown::Optimize(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) {
        return nullptr;
    }
    CheckMarkToSemi(plan.get(), {});
    return Rewrite(std::move(plan));
}

} // namespace yaap
