#include "optimizer_core.hpp"

#include "../adapter/yaap_adapter.hpp"
#include "../logical/logical_utils.hpp"

#include <algorithm>
#include <functional>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace yaap {

namespace {

using BindingKey = std::pair<size_t, size_t>;

BindingKey ToBindingKey(const ColumnBinding& binding) {
    return {binding.table_index.index, binding.column_index.index};
}

size_t FindMaxTableIndex(const LogicalOperator* op) {
    if (!op) {
        return 0;
    }
    size_t max_idx = 0;
    auto bump = [&](TableIndex tidx) {
        if (tidx.index > max_idx) {
            max_idx = tidx.index;
        }
    };
    switch (op->type) {
        case LogicalOperatorType::LOGICAL_GET:
            bump(static_cast<const LogicalGet*>(op)->table_index);
            break;
        case LogicalOperatorType::LOGICAL_PROJECTION:
            bump(static_cast<const LogicalProjection*>(op)->table_index);
            break;
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
            const auto* agg = static_cast<const LogicalAggregate*>(op);
            bump(agg->group_index);
            bump(agg->aggregate_index);
            break;
        }
        case LogicalOperatorType::LOGICAL_SET_OPERATION:
            bump(static_cast<const LogicalSetOperation*>(op)->table_index);
            break;
        case LogicalOperatorType::LOGICAL_WINDOW:
            bump(static_cast<const LogicalWindow*>(op)->table_index);
            break;
        case LogicalOperatorType::LOGICAL_DELIM_GET:
            bump(static_cast<const LogicalDelimGet*>(op)->table_index);
            break;
        case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
            bump(static_cast<const LogicalMaterializedCTE*>(op)->table_index);
            break;
        case LogicalOperatorType::LOGICAL_CTE_REF: {
            const auto* ref = static_cast<const LogicalCTERef*>(op);
            bump(ref->table_index);
            bump(ref->cte_index);
            break;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
            const auto* join = static_cast<const LogicalComparisonJoin*>(op);
            if (join->has_mark_index) {
                bump(join->mark_index);
            }
            break;
        }
        default:
            break;
    }
    for (const auto& child : op->children) {
        max_idx = std::max(max_idx, FindMaxTableIndex(child.get()));
    }
    return max_idx;
}

bool ContainsDelimGet(const LogicalOperator* op) {
    if (!op) {
        return false;
    }
    if (op->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
        return true;
    }
    for (const auto& child : op->children) {
        if (ContainsDelimGet(child.get())) {
            return true;
        }
    }
    return false;
}

void RewriteBindingsInExpr(Expression* expr, const std::map<BindingKey, ColumnBinding>& map) {
    if (!expr || map.empty()) {
        return;
    }
    switch (expr->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* col = static_cast<BoundColumnRefExpression*>(expr);
            auto it = map.find(ToBindingKey(col->binding));
            if (it != map.end()) {
                col->binding = it->second;
            }
            break;
        }
        case ExpressionType::BOUND_FUNCTION: {
            auto* fn = static_cast<BoundFunctionExpression*>(expr);
            for (auto& child : fn->children) {
                RewriteBindingsInExpr(child.get(), map);
            }
            break;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* agg = static_cast<BoundAggregateExpression*>(expr);
            for (auto& child : agg->children) {
                RewriteBindingsInExpr(child.get(), map);
            }
            break;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conj = static_cast<BoundConjunctionExpression*>(expr);
            for (auto& child : conj->children) {
                RewriteBindingsInExpr(child.get(), map);
            }
            break;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subq = static_cast<BoundSubqueryExpression*>(expr);
            for (auto& child : subq->children) {
                RewriteBindingsInExpr(child.get(), map);
            }
            break;
        }
        default:
            break;
    }
}

void RewriteBindingsInSubtree(LogicalOperator* op,
                              const std::map<BindingKey, ColumnBinding>& map,
                              const LogicalOperator* skip_subtree = nullptr,
                              const LogicalOperator* skip_cte_output_bindings = nullptr) {
    if (!op || op == skip_subtree || map.empty()) {
        return;
    }
    auto rewrite_vec = [&](std::vector<std::unique_ptr<Expression>>& exprs) {
        for (auto& expr : exprs) {
            RewriteBindingsInExpr(expr.get(), map);
        }
    };
    auto rewrite_binding_vec = [&](std::vector<ColumnBinding>& bindings) {
        for (auto& binding : bindings) {
            auto it = map.find(ToBindingKey(binding));
            if (it != map.end()) {
                binding = it->second;
            }
        }
    };
    switch (op->type) {
        case LogicalOperatorType::LOGICAL_GET:
            rewrite_vec(static_cast<LogicalGet*>(op)->filters);
            break;
        case LogicalOperatorType::LOGICAL_FILTER:
            rewrite_vec(static_cast<LogicalFilter*>(op)->expressions);
            break;
        case LogicalOperatorType::LOGICAL_PROJECTION:
            rewrite_vec(static_cast<LogicalProjection*>(op)->expressions);
            break;
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
            auto* agg = static_cast<LogicalAggregate*>(op);
            rewrite_vec(agg->groups);
            rewrite_vec(agg->expressions);
            break;
        }
        case LogicalOperatorType::LOGICAL_DISTINCT:
            rewrite_vec(static_cast<LogicalDistinct*>(op)->expressions);
            break;
        case LogicalOperatorType::LOGICAL_ORDER: {
            auto* ord = static_cast<LogicalOrder*>(op);
            for (auto& node : ord->orders) {
                RewriteBindingsInExpr(node.expression.get(), map);
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_WINDOW: {
            auto* win = static_cast<LogicalWindow*>(op);
            rewrite_vec(win->partitions);
            for (auto& node : win->orders) {
                RewriteBindingsInExpr(node.expression.get(), map);
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
            rewrite_vec(static_cast<LogicalComparisonJoin*>(op)->conditions);
            break;
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
            auto* dep = static_cast<LogicalDependentJoin*>(op);
            rewrite_vec(dep->conditions);
            rewrite_binding_vec(dep->correlated_columns);
            break;
        }
        case LogicalOperatorType::LOGICAL_DELIM_GET:
            rewrite_binding_vec(static_cast<LogicalDelimGet*>(op)->correlated_columns);
            break;
        case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
            if (op != skip_cte_output_bindings) {
                rewrite_binding_vec(static_cast<LogicalMaterializedCTE*>(op)->output_bindings);
            }
            break;
        default:
            break;
    }
    for (auto& child : op->children) {
        RewriteBindingsInSubtree(child.get(), map, skip_subtree, skip_cte_output_bindings);
    }
}

void CollectBindingsInExpr(const Expression* expr, std::set<BindingKey>& out) {
    if (!expr) {
        return;
    }
    switch (expr->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            const auto* col = static_cast<const BoundColumnRefExpression*>(expr);
            out.insert(ToBindingKey(col->binding));
            break;
        }
        case ExpressionType::BOUND_FUNCTION: {
            const auto* fn = static_cast<const BoundFunctionExpression*>(expr);
            for (const auto& child : fn->children) {
                CollectBindingsInExpr(child.get(), out);
            }
            break;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            const auto* agg = static_cast<const BoundAggregateExpression*>(expr);
            for (const auto& child : agg->children) {
                CollectBindingsInExpr(child.get(), out);
            }
            break;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            const auto* conj = static_cast<const BoundConjunctionExpression*>(expr);
            for (const auto& child : conj->children) {
                CollectBindingsInExpr(child.get(), out);
            }
            break;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            const auto* subq = static_cast<const BoundSubqueryExpression*>(expr);
            for (const auto& child : subq->children) {
                CollectBindingsInExpr(child.get(), out);
            }
            break;
        }
        default:
            break;
    }
}

void CollectReferencedBindingsOutside(const LogicalOperator* op,
                                      const std::set<const LogicalOperator*>& skip_nodes,
                                      std::set<BindingKey>& out) {
    if (!op || skip_nodes.count(op) > 0) {
        return;
    }
    auto collect_vec = [&](const std::vector<std::unique_ptr<Expression>>& exprs) {
        for (const auto& expr : exprs) {
            CollectBindingsInExpr(expr.get(), out);
        }
    };
    switch (op->type) {
        case LogicalOperatorType::LOGICAL_GET:
            collect_vec(static_cast<const LogicalGet*>(op)->filters);
            break;
        case LogicalOperatorType::LOGICAL_FILTER:
            collect_vec(static_cast<const LogicalFilter*>(op)->expressions);
            break;
        case LogicalOperatorType::LOGICAL_PROJECTION:
            collect_vec(static_cast<const LogicalProjection*>(op)->expressions);
            break;
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
            const auto* agg = static_cast<const LogicalAggregate*>(op);
            collect_vec(agg->groups);
            collect_vec(agg->expressions);
            break;
        }
        case LogicalOperatorType::LOGICAL_DISTINCT:
            collect_vec(static_cast<const LogicalDistinct*>(op)->expressions);
            break;
        case LogicalOperatorType::LOGICAL_ORDER: {
            const auto* ord = static_cast<const LogicalOrder*>(op);
            for (const auto& node : ord->orders) {
                CollectBindingsInExpr(node.expression.get(), out);
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_WINDOW: {
            const auto* win = static_cast<const LogicalWindow*>(op);
            collect_vec(win->partitions);
            for (const auto& node : win->orders) {
                CollectBindingsInExpr(node.expression.get(), out);
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
            collect_vec(static_cast<const LogicalComparisonJoin*>(op)->conditions);
            break;
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
            const auto* dep = static_cast<const LogicalDependentJoin*>(op);
            collect_vec(dep->conditions);
            for (const auto& binding : dep->correlated_columns) {
                out.insert(ToBindingKey(binding));
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_DELIM_GET: {
            const auto* delim = static_cast<const LogicalDelimGet*>(op);
            for (const auto& binding : delim->correlated_columns) {
                out.insert(ToBindingKey(binding));
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: {
            const auto* cte = static_cast<const LogicalMaterializedCTE*>(op);
            for (const auto& binding : cte->output_bindings) {
                out.insert(ToBindingKey(binding));
            }
            break;
        }
        default:
            break;
    }
    for (const auto& child : op->children) {
        CollectReferencedBindingsOutside(child.get(), skip_nodes, out);
    }
}

void EnsureProjectedColumn(LogicalGet& get, ProjectionIndex col_idx) {
    for (const auto existing : get.projected_columns) {
        if (existing.index == col_idx.index) {
            return;
        }
    }
    get.projected_columns.push_back(col_idx);
}

bool IsTransparentCast(const std::string& fn) {
    return fn == "text" || fn == "varchar" || fn == "bpchar" || fn == "char" ||
           fn == "int8" || fn == "int4" || fn == "int2" || fn == "numeric" ||
           fn == "float8" || fn == "float4";
}

const Expression* UnwrapCast(const Expression* expr) {
    while (expr != nullptr && expr->type == ExpressionType::BOUND_FUNCTION) {
        const auto* fn = static_cast<const BoundFunctionExpression*>(expr);
        if (fn->children.size() == 1 && IsTransparentCast(fn->function_name)) {
            expr = fn->children[0].get();
            continue;
        }
        break;
    }
    return expr;
}

bool ExtractSimpleEquality(const Expression* expr,
                           const BoundColumnRefExpression*& lhs,
                           const BoundColumnRefExpression*& rhs) {
    if (!expr || expr->type != ExpressionType::BOUND_FUNCTION) {
        return false;
    }
    const auto* fn = static_cast<const BoundFunctionExpression*>(expr);
    if (fn->function_name != "=" || fn->children.size() != 2) {
        return false;
    }
    const Expression* left_u = UnwrapCast(fn->children[0].get());
    const Expression* right_u = UnwrapCast(fn->children[1].get());
    if (!left_u || !right_u ||
        left_u->type != ExpressionType::BOUND_COLUMN_REF ||
        right_u->type != ExpressionType::BOUND_COLUMN_REF) {
        return false;
    }
    lhs = static_cast<const BoundColumnRefExpression*>(left_u);
    rhs = static_cast<const BoundColumnRefExpression*>(right_u);
    return true;
}

std::string CanonicalSingleGetFilterKey(const Expression* expr, size_t get_table_index, bool& ok) {
    if (!expr) {
        ok = false;
        return {};
    }
    std::stringstream ss;
    switch (expr->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            const auto* col = static_cast<const BoundColumnRefExpression*>(expr);
            if (col->binding.table_index.index != get_table_index) {
                ok = false;
                return {};
            }
            ss << "C:" << col->binding.column_index.index;
            break;
        }
        case ExpressionType::BOUND_CONSTANT: {
            const auto* c = static_cast<const BoundConstantExpression*>(expr);
            ss << "K:" << (c->is_null ? "NULL" : c->value);
            break;
        }
        case ExpressionType::BOUND_FUNCTION: {
            const auto* fn = static_cast<const BoundFunctionExpression*>(expr);
            ss << "F:" << fn->function_name << ":" << fn->op_oid << "(";
            for (size_t i = 0; i < fn->children.size(); ++i) {
                if (i > 0) ss << ",";
                ss << CanonicalSingleGetFilterKey(fn->children[i].get(), get_table_index, ok);
                if (!ok) return {};
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            const auto* conj = static_cast<const BoundConjunctionExpression*>(expr);
            ss << "B:" << conj->bool_expr_type << "(";
            std::vector<std::string> parts;
            for (const auto& ch : conj->children) {
                parts.push_back(CanonicalSingleGetFilterKey(ch.get(), get_table_index, ok));
                if (!ok) return {};
            }
            if (conj->bool_expr_type == 0 || conj->bool_expr_type == 1) {
                std::sort(parts.begin(), parts.end());
            }
            for (size_t i = 0; i < parts.size(); ++i) {
                if (i > 0) ss << ",";
                ss << parts[i];
            }
            ss << ")";
            break;
        }
        default:
            ok = false;
            return {};
    }
    return ss.str();
}

bool SameLogicalGetFilters(const LogicalGet& a, const LogicalGet& b) {
    if (a.relid != b.relid || a.filters.size() != b.filters.size()) {
        return false;
    }
    std::vector<std::string> fa;
    std::vector<std::string> fb;
    for (const auto& f : a.filters) {
        bool ok = true;
        fa.push_back(CanonicalSingleGetFilterKey(f.get(), a.table_index.index, ok));
        if (!ok) return false;
    }
    for (const auto& f : b.filters) {
        bool ok = true;
        fb.push_back(CanonicalSingleGetFilterKey(f.get(), b.table_index.index, ok));
        if (!ok) return false;
    }
    std::sort(fa.begin(), fa.end());
    std::sort(fb.begin(), fb.end());
    return fa == fb;
}

// Resolve a ColumnBinding through a chain of simple pass-through LogicalProjections.
bool ResolveThroughProjection(const LogicalProjection* proj,
                              const ColumnBinding& binding,
                              ColumnBinding& resolved) {
    if (!proj || binding.table_index.index != proj->table_index.index) {
        resolved = binding;
        return true;
    }
    if (binding.column_index.index >= proj->expressions.size()) {
        return false;
    }
    const Expression* expr = UnwrapCast(proj->expressions[binding.column_index.index].get());
    if (!expr || expr->type != ExpressionType::BOUND_COLUMN_REF) {
        return false;
    }
    resolved = static_cast<const BoundColumnRefExpression*>(expr)->binding;
    return true;
}

// ============================================================================
// Pass 1: Correlated Scale-Invariant Delim-Aggregate Reuse (e.g. TPC-H Q17)
//
// Shape:
//   DelimJoin(left, right, corr=[c])
//     left  = [Proj] -> InnerJoin(T_outer, S, T_outer.k = S.k)
//     right = [Proj] -> Agg(groups=[T_inner.k or delim.0], aggs=[avg/min/max(T_inner.x)])
//                     -> [Proj] -> InnerJoin(T_inner, DelimGet(corr=[c]), T_inner.k = delim.0)
//   where T_outer and T_inner are identical LogicalGets (same relid & filters),
//   and c resolves to S.k (or T_outer.k).
//
// Because every row of T_outer with key k is multiplied by the same multiplicity
// m(k) >= 1 of matching rows in S, AVG/MIN/MAX over T_outer ⋈ S grouped by k is
// mathematically identical to AVG/MIN/MAX over T_inner ⋈ DISTINCT(k).
// Hoisting `left` into a LogicalMaterializedCTE eliminates the duplicate full
// scan of T_inner and converts the DelimJoin into a plain HashJoin!
// ============================================================================
bool TryRewriteDelimScaleInvariantAgg(std::unique_ptr<LogicalOperator>& root,
                                      std::unique_ptr<LogicalOperator>& node,
                                      size_t& next_table_index) {
    if (!node) {
        return false;
    }
    for (auto& child : node->children) {
        if (TryRewriteDelimScaleInvariantAgg(root, child, next_table_index)) {
            return true;
        }
    }

    if ((node->type != LogicalOperatorType::LOGICAL_DELIM_JOIN &&
         node->type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) ||
        node->children.size() != 2 || !node->children[0] || !node->children[1]) {
        return false;
    }

    auto* delim = static_cast<LogicalDependentJoin*>(node.get());
    if (delim->correlated_columns.size() != 1) {
        return false;
    }
    if (ContainsDelimGet(delim->children[0].get())) {
        return false;
    }

    // Unwrap optional LogicalProjection on left child.
    LogicalProjection* left_proj = nullptr;
    LogicalOperator* left_body = delim->children[0].get();
    if (left_body->type == LogicalOperatorType::LOGICAL_PROJECTION && left_body->children.size() == 1) {
        left_proj = static_cast<LogicalProjection*>(left_body);
        left_body = left_proj->children[0].get();
    }
    if (!left_body || left_body->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
        left_body->children.size() != 2) {
        return false;
    }
    auto* outer_join = static_cast<LogicalComparisonJoin*>(left_body);
    if (outer_join->join_type != JOIN_INNER || outer_join->conditions.size() != 1) {
        return false;
    }
    const BoundColumnRefExpression* outer_lhs = nullptr;
    const BoundColumnRefExpression* outer_rhs = nullptr;
    if (!ExtractSimpleEquality(outer_join->conditions[0].get(), outer_lhs, outer_rhs)) {
        return false;
    }

    // Resolve correlated column through optional left_proj.
    ColumnBinding corr_resolved{};
    if (!ResolveThroughProjection(left_proj, delim->correlated_columns[0], corr_resolved)) {
        return false;
    }

    // Identify outer_get (the base table being re-scanned on the right) and the other side S.
    LogicalGet* outer_get = nullptr;
    ColumnBinding outer_get_key{};
    ColumnBinding other_side_key{};
    auto check_outer_side = [&](LogicalOperator* cand, LogicalOperator* other) -> bool {
        if (!cand || cand->type != LogicalOperatorType::LOGICAL_GET) {
            return false;
        }
        auto* get = static_cast<LogicalGet*>(cand);
        std::set<size_t> other_tables;
        CollectOutputTables(other, other_tables);
        if (outer_lhs->binding.table_index.index == get->table_index.index &&
            other_tables.count(outer_rhs->binding.table_index.index) > 0) {
            outer_get = get;
            outer_get_key = outer_lhs->binding;
            other_side_key = outer_rhs->binding;
            return true;
        }
        if (outer_rhs->binding.table_index.index == get->table_index.index &&
            other_tables.count(outer_lhs->binding.table_index.index) > 0) {
            outer_get = get;
            outer_get_key = outer_rhs->binding;
            other_side_key = outer_lhs->binding;
            return true;
        }
        return false;
    };

    // Prefer the larger LogicalGet if both sides are LogicalGets; check which side matches corr_resolved.
    auto matches_corr = [&](const ColumnBinding& k1, const ColumnBinding& k2) {
        return ToBindingKey(corr_resolved) == ToBindingKey(k1) ||
               ToBindingKey(corr_resolved) == ToBindingKey(k2);
    };

    // Unwrap right child: [Projection]* -> Aggregate -> [Projection] -> InnerJoin(inner_get, delim_get).
    std::unique_ptr<LogicalOperator>* right_agg_slot = &delim->children[1];
    while (*right_agg_slot && (*right_agg_slot)->type == LogicalOperatorType::LOGICAL_PROJECTION &&
           (*right_agg_slot)->children.size() == 1) {
        right_agg_slot = &(*right_agg_slot)->children[0];
    }
    if (!*right_agg_slot || (*right_agg_slot)->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY ||
        (*right_agg_slot)->children.size() != 1) {
        return false;
    }
    auto* inner_agg = static_cast<LogicalAggregate*>(right_agg_slot->get());
    if (inner_agg->groups.size() != 1 || inner_agg->expressions.empty()) {
        return false;
    }
    for (const auto& agg_expr : inner_agg->expressions) {
        if (!agg_expr || agg_expr->type != ExpressionType::BOUND_AGGREGATE) {
            return false;
        }
        const auto* agg_fn = static_cast<const BoundAggregateExpression*>(agg_expr.get());
        if (agg_fn->is_distinct ||
            (agg_fn->function_name != "avg" &&
             agg_fn->function_name != "min" &&
             agg_fn->function_name != "max")) {
            return false;
        }
    }

    std::unique_ptr<LogicalOperator>* inner_join_slot = &inner_agg->children[0];
    LogicalProjection* inner_pre_proj = nullptr;
    if (*inner_join_slot && (*inner_join_slot)->type == LogicalOperatorType::LOGICAL_PROJECTION &&
        (*inner_join_slot)->children.size() == 1) {
        inner_pre_proj = static_cast<LogicalProjection*>(inner_join_slot->get());
        inner_join_slot = &inner_pre_proj->children[0];
    }
    if (!*inner_join_slot || (*inner_join_slot)->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
        (*inner_join_slot)->children.size() != 2) {
        return false;
    }
    auto* inner_join = static_cast<LogicalComparisonJoin*>(inner_join_slot->get());
    if (inner_join->join_type != JOIN_INNER || inner_join->conditions.size() != 1) {
        return false;
    }

    LogicalGet* inner_get = nullptr;
    LogicalDelimGet* delim_get = nullptr;
    if (inner_join->children[0]->type == LogicalOperatorType::LOGICAL_GET &&
        inner_join->children[1]->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
        inner_get = static_cast<LogicalGet*>(inner_join->children[0].get());
        delim_get = static_cast<LogicalDelimGet*>(inner_join->children[1].get());
    } else if (inner_join->children[1]->type == LogicalOperatorType::LOGICAL_GET &&
               inner_join->children[0]->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
        inner_get = static_cast<LogicalGet*>(inner_join->children[1].get());
        delim_get = static_cast<LogicalDelimGet*>(inner_join->children[0].get());
    } else {
        return false;
    }
    if (delim_get->correlated_columns.size() != 1) {
        return false;
    }

    // Match outer_get against inner_get (try child 0 then child 1 of outer_join).
    bool matched_outer = false;
    if (check_outer_side(outer_join->children[0].get(), outer_join->children[1].get()) &&
        SameLogicalGetFilters(*outer_get, *inner_get) &&
        matches_corr(outer_get_key, other_side_key)) {
        matched_outer = true;
    } else if (check_outer_side(outer_join->children[1].get(), outer_join->children[0].get()) &&
               SameLogicalGetFilters(*outer_get, *inner_get) &&
               matches_corr(outer_get_key, other_side_key)) {
        matched_outer = true;
    }
    if (!matched_outer) {
        return false;
    }

    // Verify inner_join condition equates inner_get.<outer_get_key.col> = delim_get.0.
    const BoundColumnRefExpression* inner_lhs = nullptr;
    const BoundColumnRefExpression* inner_rhs = nullptr;
    if (!ExtractSimpleEquality(inner_join->conditions[0].get(), inner_lhs, inner_rhs)) {
        return false;
    }
    bool inner_cond_ok = false;
    if (inner_lhs->binding.table_index.index == inner_get->table_index.index &&
        inner_lhs->binding.column_index.index == outer_get_key.column_index.index &&
        inner_rhs->binding.table_index.index == delim_get->table_index.index &&
        inner_rhs->binding.column_index.index == 0) {
        inner_cond_ok = true;
    } else if (inner_rhs->binding.table_index.index == inner_get->table_index.index &&
               inner_rhs->binding.column_index.index == outer_get_key.column_index.index &&
               inner_lhs->binding.table_index.index == delim_get->table_index.index &&
               inner_lhs->binding.column_index.index == 0) {
        inner_cond_ok = true;
    }
    if (!inner_cond_ok) {
        return false;
    }

    // Verify inner_agg group resolves to inner_get.<outer_get_key.col> or delim_get.0.
    const Expression* grp_u = UnwrapCast(inner_agg->groups[0].get());
    if (!grp_u || grp_u->type != ExpressionType::BOUND_COLUMN_REF) {
        return false;
    }
    ColumnBinding grp_b = static_cast<const BoundColumnRefExpression*>(grp_u)->binding;
    if (!ResolveThroughProjection(inner_pre_proj, grp_b, grp_b)) {
        return false;
    }
    const bool grp_is_key =
        (grp_b.table_index.index == inner_get->table_index.index &&
         grp_b.column_index.index == outer_get_key.column_index.index) ||
        (grp_b.table_index.index == delim_get->table_index.index &&
         grp_b.column_index.index == 0);
    if (!grp_is_key) {
        return false;
    }

    // Collect all columns of inner_get referenced by inner_agg (through optional inner_pre_proj).
    std::set<BindingKey> inner_refs;
    for (const auto& g : inner_agg->groups) CollectBindingsInExpr(g.get(), inner_refs);
    for (const auto& e : inner_agg->expressions) CollectBindingsInExpr(e.get(), inner_refs);

    std::set<size_t> needed_inner_get_cols;
    needed_inner_get_cols.insert(outer_get_key.column_index.index);
    for (const auto& ref : inner_refs) {
        ColumnBinding b{TableIndex{ref.first}, ProjectionIndex{ref.second}};
        if (!ResolveThroughProjection(inner_pre_proj, b, b)) {
            return false;
        }
        if (b.table_index.index == inner_get->table_index.index) {
            needed_inner_get_cols.insert(b.column_index.index);
        } else if (b.table_index.index == delim_get->table_index.index && b.column_index.index == 0) {
            // Maps to corr_resolved / outer_get_key.
        } else {
            return false;
        }
    }

    // Ensure outer_get projects all columns needed by inner_agg.
    for (size_t col_idx : needed_inner_get_cols) {
        EnsureProjectedColumn(*outer_get, ProjectionIndex{col_idx});
    }

    // If left has a LogicalProjection, ensure each needed outer_get column is exposed by left_proj.
    std::map<size_t, ColumnBinding> inner_col_to_left_binding;
    for (size_t col_idx : needed_inner_get_cols) {
        ColumnBinding target{outer_get->table_index, ProjectionIndex{col_idx}};
        if (left_proj != nullptr) {
            bool found = false;
            for (size_t i = 0; i < left_proj->expressions.size(); ++i) {
                const Expression* e = UnwrapCast(left_proj->expressions[i].get());
                if (e && e->type == ExpressionType::BOUND_COLUMN_REF) {
                    const auto* cr = static_cast<const BoundColumnRefExpression*>(e);
                    if (cr->binding.table_index.index == target.table_index.index &&
                        cr->binding.column_index.index == target.column_index.index) {
                        inner_col_to_left_binding[col_idx] = ColumnBinding{left_proj->table_index, ProjectionIndex{i}};
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                size_t new_pos = left_proj->expressions.size();
                std::string cname = col_idx < outer_get->output_names.size()
                    ? outer_get->output_names[col_idx]
                    : "col" + std::to_string(col_idx + 1);
                left_proj->expressions.push_back(std::make_unique<BoundColumnRefExpression>(
                    target, outer_get->table_name, cname));
                left_proj->output_names.push_back(cname);
                inner_col_to_left_binding[col_idx] = ColumnBinding{left_proj->table_index, ProjectionIndex{new_pos}};
            }
        } else {
            inner_col_to_left_binding[col_idx] = target;
        }
    }

    // Collect all bindings produced by left that are referenced anywhere outside left (excluding right's inner_join).
    std::set<size_t> left_tables;
    CollectOutputTables(delim->children[0].get(), left_tables);
    std::set<BindingKey> ext_refs;
    CollectReferencedBindingsOutside(root.get(), {delim->children[0].get()}, ext_refs);

    std::vector<ColumnBinding> cte_output_bindings;
    std::set<BindingKey> cte_output_seen;
    auto add_cte_output = [&](const ColumnBinding& b) -> size_t {
        BindingKey k = ToBindingKey(b);
        for (size_t i = 0; i < cte_output_bindings.size(); ++i) {
            if (ToBindingKey(cte_output_bindings[i]) == k) {
                return i;
            }
        }
        size_t pos = cte_output_bindings.size();
        cte_output_bindings.push_back(b);
        cte_output_seen.insert(k);
        return pos;
    };

    for (const auto& ref : ext_refs) {
        if (left_tables.count(ref.first) > 0) {
            add_cte_output(ColumnBinding{TableIndex{ref.first}, ProjectionIndex{ref.second}});
        }
    }
    for (const auto& kv : inner_col_to_left_binding) {
        add_cte_output(kv.second);
    }
    if (cte_output_bindings.empty()) {
        return false;
    }

    const TableIndex cte_idx{++next_table_index};
    const TableIndex scan1_idx{++next_table_index};
    const TableIndex scan2_idx{++next_table_index};
    const std::string cte_name = "cte_delim_" + std::to_string(cte_idx.index);

    std::vector<std::string> cte_output_names;
    cte_output_names.reserve(cte_output_bindings.size());
    for (size_t i = 0; i < cte_output_bindings.size(); ++i) {
        cte_output_names.push_back("c" + std::to_string(i));
    }

    // Build binding rewrite maps for scan1 (outer side) and scan2 (inner agg side).
    std::map<BindingKey, ColumnBinding> scan1_map;
    for (size_t i = 0; i < cte_output_bindings.size(); ++i) {
        scan1_map[ToBindingKey(cte_output_bindings[i])] = ColumnBinding{scan1_idx, ProjectionIndex{i}};
    }

    std::map<BindingKey, ColumnBinding> scan2_map;
    for (const auto& kv : inner_col_to_left_binding) {
        size_t pos = add_cte_output(kv.second);
        scan2_map[{inner_get->table_index.index, kv.first}] = ColumnBinding{scan2_idx, ProjectionIndex{pos}};
    }
    // Map delim_get.0 to scan2's key column (prefer corr binding if in cte_output, else outer_get_key).
    size_t delim_pos = add_cte_output(inner_col_to_left_binding[outer_get_key.column_index.index]);
    scan2_map[{delim_get->table_index.index, 0}] = ColumnBinding{scan2_idx, ProjectionIndex{delim_pos}};

    const size_t producer_card = delim->children[0]->estimated_cardinality;

    auto scan1 = std::make_unique<LogicalCTERef>(scan1_idx, cte_idx, cte_name);
    scan1->output_names = cte_output_names;
    scan1->estimated_cardinality = producer_card;

    auto scan2 = std::make_unique<LogicalCTERef>(scan2_idx, cte_idx, cte_name);
    scan2->output_names = cte_output_names;
    scan2->estimated_cardinality = producer_card;

    auto cte = std::make_unique<LogicalMaterializedCTE>(cte_idx, cte_name);
    cte->output_bindings = cte_output_bindings;
    cte->output_names = cte_output_names;
    cte->estimated_cardinality = delim->estimated_cardinality;
    cte->children.push_back(std::move(delim->children[0]));

    // Replace inner_join with scan2 and rewrite bindings inside right branch.
    *inner_join_slot = std::move(scan2);
    RewriteBindingsInSubtree(delim->children[1].get(), scan2_map);

    // Replace delim with a plain LogicalComparisonJoin (no longer dependent/delim!).
    // JOIN_SINGLE with correlated equality + filter lowers as INNER join.
    const int plain_join_type = (delim->join_type == JOIN_SINGLE) ? JOIN_INNER : delim->join_type;
    auto plain_join = std::make_unique<LogicalComparisonJoin>(plain_join_type);
    plain_join->dependent = false;
    plain_join->mark_index = delim->mark_index;
    plain_join->has_mark_index = delim->has_mark_index;
    plain_join->invert_result = delim->invert_result;
    plain_join->estimated_cardinality = delim->estimated_cardinality;
    plain_join->conditions = std::move(delim->conditions);
    plain_join->children.push_back(std::move(scan1));
    plain_join->children.push_back(std::move(delim->children[1]));

    LogicalOperator* producer_raw = cte->children[0].get();
    LogicalOperator* cte_raw = cte.get();
    cte->children.push_back(std::move(plain_join));
    node = std::move(cte);

    // Rewrite references to left's old bindings everywhere outside the CTE producer
    // (and without rewriting this CTE's own output_bindings).
    RewriteBindingsInSubtree(root.get(), scan1_map, producer_raw, cte_raw);
    return true;
}

// ============================================================================
// Pass 2: Self-Join Filtered-Aggregate Reuse via Eager Aggregation (e.g. TPC-H Q18)
//
// Shape:
//   OuterAgg(groups=G, aggs=[SUM/MIN/MAX(B.x), ...])
//     -> [Proj] -> InnerJoin(A, B, A.k = B.k)
//   where B is LogicalGet(R, F) (no other columns of B used above the join),
//   and A contains SemiJoin(X, Y, X.k = Y.k) with X.k transitively equated to A.k,
//   and Y = [Distinct/Proj]* -> [Filter(P)] -> InnerAgg(group=[R.k], aggs=[..., SUM/MIN/MAX(R.x)])
//           -> [Proj] -> LogicalGet(R, F).
//
// Hoists `[Filter(P)] -> InnerAgg` into a LogicalMaterializedCTE shared between
// the SemiJoin's build side and the outer InnerJoin's B side, rewriting OuterAgg's
// SUM/MIN/MAX(B.x) to aggregate the pre-aggregated CTE column!
// ============================================================================
void CollectTransitiveEqualities(const LogicalOperator* op,
                                 std::vector<std::pair<BindingKey, BindingKey>>& eqs) {
    if (!op) {
        return;
    }
    if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        const auto* join = static_cast<const LogicalComparisonJoin*>(op);
        if (join->join_type == JOIN_INNER || join->join_type == JOIN_SEMI) {
            for (const auto& cond : join->conditions) {
                const BoundColumnRefExpression* l = nullptr;
                const BoundColumnRefExpression* r = nullptr;
                if (ExtractSimpleEquality(cond.get(), l, r)) {
                    eqs.push_back({ToBindingKey(l->binding), ToBindingKey(r->binding)});
                }
            }
        }
    } else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
        const auto* proj = static_cast<const LogicalProjection*>(op);
        for (size_t i = 0; i < proj->expressions.size(); ++i) {
            const Expression* u = UnwrapCast(proj->expressions[i].get());
            if (u && u->type == ExpressionType::BOUND_COLUMN_REF) {
                const auto* cr = static_cast<const BoundColumnRefExpression*>(u);
                eqs.push_back({{proj->table_index.index, i}, ToBindingKey(cr->binding)});
            }
        }
    }
    for (const auto& child : op->children) {
        CollectTransitiveEqualities(child.get(), eqs);
    }
}

std::set<BindingKey> ComputeEqualityClass(const BindingKey& seed,
                                          const std::vector<std::pair<BindingKey, BindingKey>>& eqs) {
    std::set<BindingKey> cls{seed};
    bool changed = true;
    while (changed) {
        changed = false;
        for (const auto& e : eqs) {
            if (cls.count(e.first) > 0 && cls.count(e.second) == 0) {
                cls.insert(e.second);
                changed = true;
            } else if (cls.count(e.second) > 0 && cls.count(e.first) == 0) {
                cls.insert(e.first);
                changed = true;
            }
        }
    }
    return cls;
}

struct SemiAggMatch {
    std::unique_ptr<LogicalOperator>* producer_slot = nullptr;
    LogicalAggregate* inner_agg = nullptr;
    LogicalGet* inner_get = nullptr;
    std::vector<size_t> matched_inner_agg_indices;
};

bool FindMatchingSemiFilteredAgg(std::unique_ptr<LogicalOperator>& node,
                                 const std::set<BindingKey>& a_key_class,
                                 const LogicalGet& outer_get,
                                 size_t outer_key_col,
                                 const std::vector<std::pair<std::string, size_t>>& needed_aggs,
                                 SemiAggMatch& out) {
    if (!node) {
        return false;
    }
    if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN && node->children.size() == 2) {
        auto* join = static_cast<LogicalComparisonJoin*>(node.get());
        if (join->join_type == JOIN_SEMI && join->conditions.size() == 1) {
            const BoundColumnRefExpression* lhs = nullptr;
            const BoundColumnRefExpression* rhs = nullptr;
            if (ExtractSimpleEquality(join->conditions[0].get(), lhs, rhs)) {
                std::set<size_t> left_tables;
                std::set<size_t> right_tables;
                CollectOutputTables(join->children[0].get(), left_tables);
                CollectOutputTables(join->children[1].get(), right_tables);

                const BoundColumnRefExpression* x_ref = nullptr;
                const BoundColumnRefExpression* y_ref = nullptr;
                if (left_tables.count(lhs->binding.table_index.index) > 0 &&
                    right_tables.count(rhs->binding.table_index.index) > 0) {
                    x_ref = lhs;
                    y_ref = rhs;
                } else if (left_tables.count(rhs->binding.table_index.index) > 0 &&
                           right_tables.count(lhs->binding.table_index.index) > 0) {
                    x_ref = rhs;
                    y_ref = lhs;
                }

                if (x_ref != nullptr && y_ref != nullptr &&
                    a_key_class.count(ToBindingKey(x_ref->binding)) > 0 &&
                    !ContainsDelimGet(join->children[1].get())) {
                    // Walk down Y through optional Distinct / Projection to Filter or Aggregate.
                    std::unique_ptr<LogicalOperator>* cursor = &join->children[1];
                    ColumnBinding y_binding = y_ref->binding;
                    bool unwrap_ok = true;
                    while (*cursor &&
                           ((*cursor)->type == LogicalOperatorType::LOGICAL_PROJECTION ||
                            (*cursor)->type == LogicalOperatorType::LOGICAL_DISTINCT) &&
                           (*cursor)->children.size() == 1) {
                        if ((*cursor)->type == LogicalOperatorType::LOGICAL_PROJECTION) {
                            auto* proj = static_cast<LogicalProjection*>(cursor->get());
                            if (!ResolveThroughProjection(proj, y_binding, y_binding)) {
                                unwrap_ok = false;
                                break;
                            }
                        }
                        cursor = &(*cursor)->children[0];
                    }

                    if (unwrap_ok && *cursor) {
                        std::unique_ptr<LogicalOperator>* producer_slot = cursor;
                        LogicalOperator* agg_cand = cursor->get();
                        if (agg_cand->type == LogicalOperatorType::LOGICAL_FILTER &&
                            agg_cand->children.size() == 1) {
                            agg_cand = agg_cand->children[0].get();
                        }
                        if (agg_cand &&
                            agg_cand->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY &&
                            agg_cand->children.size() == 1) {
                            auto* inner_agg = static_cast<LogicalAggregate*>(agg_cand);
                            if (inner_agg->groups.size() == 1 &&
                                y_binding.table_index.index == inner_agg->group_index.index &&
                                y_binding.column_index.index == 0) {
                                LogicalProjection* inner_proj = nullptr;
                                LogicalOperator* get_cand = inner_agg->children[0].get();
                                if (get_cand && get_cand->type == LogicalOperatorType::LOGICAL_PROJECTION &&
                                    get_cand->children.size() == 1) {
                                    inner_proj = static_cast<LogicalProjection*>(get_cand);
                                    get_cand = inner_proj->children[0].get();
                                }
                                if (get_cand && get_cand->type == LogicalOperatorType::LOGICAL_GET) {
                                    auto* inner_get = static_cast<LogicalGet*>(get_cand);
                                    if (SameLogicalGetFilters(outer_get, *inner_get)) {
                                        const Expression* grp_u = UnwrapCast(inner_agg->groups[0].get());
                                        ColumnBinding grp_b{};
                                        if (grp_u && grp_u->type == ExpressionType::BOUND_COLUMN_REF &&
                                            ResolveThroughProjection(
                                                inner_proj,
                                                static_cast<const BoundColumnRefExpression*>(grp_u)->binding,
                                                grp_b) &&
                                            grp_b.table_index.index == inner_get->table_index.index &&
                                            grp_b.column_index.index == outer_key_col) {
                                            std::vector<size_t> matched_indices;
                                            bool all_aggs_found = true;
                                            for (const auto& need : needed_aggs) {
                                                bool found = false;
                                                for (size_t j = 0; j < inner_agg->expressions.size(); ++j) {
                                                    const auto* ae = dynamic_cast<const BoundAggregateExpression*>(
                                                        inner_agg->expressions[j].get());
                                                    if (!ae || ae->is_distinct ||
                                                        ae->function_name != need.first ||
                                                        ae->children.size() != 1) {
                                                        continue;
                                                    }
                                                    const Expression* arg_u = UnwrapCast(ae->children[0].get());
                                                    if (!arg_u || arg_u->type != ExpressionType::BOUND_COLUMN_REF) {
                                                        continue;
                                                    }
                                                    ColumnBinding arg_b{};
                                                    if (!ResolveThroughProjection(
                                                            inner_proj,
                                                            static_cast<const BoundColumnRefExpression*>(arg_u)->binding,
                                                            arg_b)) {
                                                        continue;
                                                    }
                                                    if (arg_b.table_index.index == inner_get->table_index.index &&
                                                        arg_b.column_index.index == need.second) {
                                                        matched_indices.push_back(j);
                                                        found = true;
                                                        break;
                                                    }
                                                }
                                                if (!found) {
                                                    all_aggs_found = false;
                                                    break;
                                                }
                                            }
                                            if (all_aggs_found) {
                                                out.producer_slot = producer_slot;
                                                out.inner_agg = inner_agg;
                                                out.inner_get = inner_get;
                                                out.matched_inner_agg_indices = std::move(matched_indices);
                                                return true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    for (auto& child : node->children) {
        if (FindMatchingSemiFilteredAgg(child, a_key_class, outer_get, outer_key_col, needed_aggs, out)) {
            return true;
        }
    }
    return false;
}

bool TryRewriteSelfJoinFilteredAgg(std::unique_ptr<LogicalOperator>& node, size_t& next_table_index) {
    if (!node) {
        return false;
    }
    for (auto& child : node->children) {
        if (TryRewriteSelfJoinFilteredAgg(child, next_table_index)) {
            return true;
        }
    }

    if (node->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY ||
        node->children.size() != 1 || !node->children[0]) {
        return false;
    }
    auto* outer_agg = static_cast<LogicalAggregate*>(node.get());
    if (outer_agg->expressions.empty()) {
        return false;
    }

    LogicalProjection* mid_proj = nullptr;
    LogicalOperator* join_cand = outer_agg->children[0].get();
    if (join_cand->type == LogicalOperatorType::LOGICAL_PROJECTION && join_cand->children.size() == 1) {
        mid_proj = static_cast<LogicalProjection*>(join_cand);
        join_cand = mid_proj->children[0].get();
    }
    if (!join_cand || join_cand->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
        join_cand->children.size() != 2) {
        return false;
    }
    auto* outer_join = static_cast<LogicalComparisonJoin*>(join_cand);
    if (outer_join->join_type != JOIN_INNER || outer_join->conditions.size() != 1) {
        return false;
    }
    const BoundColumnRefExpression* jlhs = nullptr;
    const BoundColumnRefExpression* jrhs = nullptr;
    if (!ExtractSimpleEquality(outer_join->conditions[0].get(), jlhs, jrhs)) {
        return false;
    }

    for (size_t b_side = 0; b_side < 2; ++b_side) {
        const size_t a_side = 1 - b_side;
        if (!outer_join->children[b_side] ||
            outer_join->children[b_side]->type != LogicalOperatorType::LOGICAL_GET) {
            continue;
        }
        auto* outer_get = static_cast<LogicalGet*>(outer_join->children[b_side].get());
        std::set<size_t> a_tables;
        CollectOutputTables(outer_join->children[a_side].get(), a_tables);

        ColumnBinding a_key{};
        ColumnBinding b_key{};
        if (a_tables.count(jlhs->binding.table_index.index) > 0 &&
            jrhs->binding.table_index.index == outer_get->table_index.index) {
            a_key = jlhs->binding;
            b_key = jrhs->binding;
        } else if (a_tables.count(jrhs->binding.table_index.index) > 0 &&
                   jlhs->binding.table_index.index == outer_get->table_index.index) {
            a_key = jrhs->binding;
            b_key = jlhs->binding;
        } else {
            continue;
        }

        // Verify outer_agg groups do not reference B (except optionally b_key).
        bool groups_ok = true;
        for (const auto& g : outer_agg->groups) {
            std::set<BindingKey> grefs;
            CollectBindingsInExpr(g.get(), grefs);
            for (const auto& r : grefs) {
                ColumnBinding b{TableIndex{r.first}, ProjectionIndex{r.second}};
                if (!ResolveThroughProjection(mid_proj, b, b)) {
                    groups_ok = false;
                    break;
                }
                if (b.table_index.index == outer_get->table_index.index &&
                    b.column_index.index != b_key.column_index.index) {
                    groups_ok = false;
                    break;
                }
            }
            if (!groups_ok) break;
        }
        if (!groups_ok) {
            continue;
        }

        // Verify every aggregate in outer_agg referencing B is non-distinct sum/min/max(B.col).
        std::vector<std::pair<std::string, size_t>> needed_aggs;
        std::vector<size_t> outer_agg_positions;
        bool aggs_ok = true;
        for (size_t i = 0; i < outer_agg->expressions.size(); ++i) {
            const auto* ae = dynamic_cast<const BoundAggregateExpression*>(outer_agg->expressions[i].get());
            if (!ae) {
                aggs_ok = false;
                break;
            }
            std::set<BindingKey> arefs;
            CollectBindingsInExpr(ae, arefs);
            bool touches_b = false;
            for (const auto& r : arefs) {
                ColumnBinding b{TableIndex{r.first}, ProjectionIndex{r.second}};
                if (ResolveThroughProjection(mid_proj, b, b) &&
                    b.table_index.index == outer_get->table_index.index) {
                    touches_b = true;
                    break;
                }
            }
            if (!touches_b) {
                // COUNT(*) or an aggregate on A would change multiplicity if B is pre-aggregated!
                aggs_ok = false;
                break;
            }
            if (ae->is_distinct || ae->children.size() != 1 ||
                (ae->function_name != "sum" &&
                 ae->function_name != "min" &&
                 ae->function_name != "max")) {
                aggs_ok = false;
                break;
            }
            const Expression* arg_u = UnwrapCast(ae->children[0].get());
            if (!arg_u || arg_u->type != ExpressionType::BOUND_COLUMN_REF) {
                aggs_ok = false;
                break;
            }
            ColumnBinding arg_b{};
            if (!ResolveThroughProjection(
                    mid_proj,
                    static_cast<const BoundColumnRefExpression*>(arg_u)->binding,
                    arg_b) ||
                arg_b.table_index.index != outer_get->table_index.index) {
                aggs_ok = false;
                break;
            }
            needed_aggs.push_back({ae->function_name, arg_b.column_index.index});
            outer_agg_positions.push_back(i);
        }
        if (!aggs_ok || needed_aggs.empty()) {
            continue;
        }

        // Compute transitive equality class of a_key within A and find matching SemiJoin.
        std::vector<std::pair<BindingKey, BindingKey>> a_eqs;
        CollectTransitiveEqualities(outer_join->children[a_side].get(), a_eqs);
        std::set<BindingKey> a_key_class = ComputeEqualityClass(ToBindingKey(a_key), a_eqs);

        SemiAggMatch match;
        if (!FindMatchingSemiFilteredAgg(
                outer_join->children[a_side],
                a_key_class,
                *outer_get,
                b_key.column_index.index,
                needed_aggs,
                match)) {
            continue;
        }

        // Hoist *match.producer_slot into a LogicalMaterializedCTE above outer_agg!
        const TableIndex cte_idx{++next_table_index};
        const TableIndex scan1_idx{++next_table_index};
        const TableIndex scan2_idx{++next_table_index};
        const std::string cte_name = "cte_agg_reuse_" + std::to_string(cte_idx.index);

        std::vector<ColumnBinding> cte_output_bindings;
        std::vector<std::string> cte_output_names;
        cte_output_bindings.push_back(ColumnBinding{match.inner_agg->group_index, ProjectionIndex{0}});
        cte_output_names.push_back(
            !match.inner_agg->group_names.empty() ? match.inner_agg->group_names[0] : "grp0");

        // Deduplicate matched inner aggregate indices in cte_output_bindings.
        std::map<size_t, size_t> inner_agg_to_cte_pos;
        for (size_t idx : match.matched_inner_agg_indices) {
            if (inner_agg_to_cte_pos.count(idx) == 0) {
                size_t pos = cte_output_bindings.size();
                inner_agg_to_cte_pos[idx] = pos;
                cte_output_bindings.push_back(ColumnBinding{match.inner_agg->aggregate_index, ProjectionIndex{idx}});
                cte_output_names.push_back(
                    idx < match.inner_agg->aggregate_names.size()
                        ? match.inner_agg->aggregate_names[idx]
                        : "agg" + std::to_string(idx));
            }
        }

        const size_t producer_card = (*match.producer_slot)->estimated_cardinality;

        auto scan1 = std::make_unique<LogicalCTERef>(scan1_idx, cte_idx, cte_name);
        scan1->output_names = cte_output_names;
        scan1->estimated_cardinality = producer_card;

        auto scan2 = std::make_unique<LogicalCTERef>(scan2_idx, cte_idx, cte_name);
        scan2->output_names = cte_output_names;
        scan2->estimated_cardinality = producer_card;

        auto cte = std::make_unique<LogicalMaterializedCTE>(cte_idx, cte_name);
        cte->output_bindings = cte_output_bindings;
        cte->output_names = cte_output_names;
        cte->estimated_cardinality = outer_agg->estimated_cardinality;
        cte->children.push_back(std::move(*match.producer_slot));

        // Replace producer slot inside A with scan1 and rewrite A's bindings to scan1.
        *match.producer_slot = std::move(scan1);
        std::map<BindingKey, ColumnBinding> scan1_map;
        for (size_t i = 0; i < cte_output_bindings.size(); ++i) {
            scan1_map[ToBindingKey(cte_output_bindings[i])] = ColumnBinding{scan1_idx, ProjectionIndex{i}};
        }
        RewriteBindingsInSubtree(outer_join->children[a_side].get(), scan1_map);

        // Replace B (outer_get) with scan2 and rewrite outer_join/mid_proj/outer_agg references!
        std::map<BindingKey, ColumnBinding> scan2_map;
        scan2_map[ToBindingKey(b_key)] = ColumnBinding{scan2_idx, ProjectionIndex{0}};
        for (size_t i = 0; i < needed_aggs.size(); ++i) {
            const size_t cte_pos = inner_agg_to_cte_pos[match.matched_inner_agg_indices[i]];
            scan2_map[{outer_get->table_index.index, needed_aggs[i].second}] =
                ColumnBinding{scan2_idx, ProjectionIndex{cte_pos}};
        }

        outer_join->children[b_side] = std::move(scan2);
        for (auto& cond : outer_join->conditions) {
            RewriteBindingsInExpr(cond.get(), scan2_map);
        }
        if (mid_proj != nullptr) {
            for (auto& expr : mid_proj->expressions) {
                RewriteBindingsInExpr(expr.get(), scan2_map);
            }
        }
        for (auto& g : outer_agg->groups) {
            RewriteBindingsInExpr(g.get(), scan2_map);
        }
        for (auto& e : outer_agg->expressions) {
            RewriteBindingsInExpr(e.get(), scan2_map);
        }

        // Update cardinality estimates on outer_join / mid_proj / outer_agg so HashJoin build side picks cleanly.
        const size_t new_card = std::min(outer_join->estimated_cardinality,
                                         std::max<size_t>(1, producer_card));
        outer_join->estimated_cardinality = new_card;
        if (mid_proj != nullptr) {
            mid_proj->estimated_cardinality = new_card;
        }
        outer_agg->estimated_cardinality = std::min(outer_agg->estimated_cardinality, new_card);

        cte->children.push_back(std::move(node));
        node = std::move(cte);
        return true;
    }

    return false;
}

// ============================================================================
// Pass 3: General Common Subplan Elimination (e.g. TPC-H Q15, Q11, Q02, views/CTEs)
// ============================================================================
struct SubtreeSignature {
    bool eligible = false;
    bool has_join_or_agg = false;
    bool has_agg = false;
    size_t sum_base_scan_rows = 0;
    std::string key;
    // Canonical local table index assignment: actual_table_index -> canonical_id
    std::map<size_t, size_t> table_to_canon;
    // Reverse map: canonical_id -> actual_table_index
    std::vector<size_t> canon_to_table;
    // For each canonical_id that corresponds to a LogicalGet: pointer to that LogicalGet
    std::map<size_t, LogicalGet*> canon_to_get;
};

std::string CanonicalizeExpr(const Expression* expr,
                             const std::map<size_t, size_t>& table_to_canon,
                             bool& ok) {
    if (!expr) {
        ok = false;
        return {};
    }
    std::stringstream ss;
    switch (expr->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            const auto* col = static_cast<const BoundColumnRefExpression*>(expr);
            auto it = table_to_canon.find(col->binding.table_index.index);
            if (it == table_to_canon.end()) {
                ok = false;
                return {};
            }
            ss << "C:" << it->second << "." << col->binding.column_index.index;
            break;
        }
        case ExpressionType::BOUND_CONSTANT: {
            const auto* c = static_cast<const BoundConstantExpression*>(expr);
            ss << "K:" << (c->is_null ? "NULL" : c->value);
            break;
        }
        case ExpressionType::BOUND_FUNCTION: {
            const auto* fn = static_cast<const BoundFunctionExpression*>(expr);
            std::vector<std::string> args;
            args.reserve(fn->children.size());
            for (const auto& ch : fn->children) {
                args.push_back(CanonicalizeExpr(ch.get(), table_to_canon, ok));
                if (!ok) return {};
            }
            if ((fn->function_name == "=" || fn->function_name == "<>" ||
                 fn->function_name == "+" || fn->function_name == "*") &&
                args.size() == 2) {
                if (args[1] < args[0]) {
                    std::swap(args[0], args[1]);
                }
            }
            ss << "F:" << fn->function_name << ":" << fn->op_oid << "(";
            for (size_t i = 0; i < args.size(); ++i) {
                if (i > 0) ss << ",";
                ss << args[i];
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            const auto* agg = static_cast<const BoundAggregateExpression*>(expr);
            ss << "A:" << agg->function_name << ":" << agg->agg_oid << ":"
               << (agg->is_distinct ? "D" : "A") << "(";
            for (size_t i = 0; i < agg->children.size(); ++i) {
                if (i > 0) ss << ",";
                ss << CanonicalizeExpr(agg->children[i].get(), table_to_canon, ok);
                if (!ok) return {};
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            const auto* conj = static_cast<const BoundConjunctionExpression*>(expr);
            std::vector<std::string> parts;
            for (const auto& ch : conj->children) {
                parts.push_back(CanonicalizeExpr(ch.get(), table_to_canon, ok));
                if (!ok) return {};
            }
            if (conj->bool_expr_type == 0 || conj->bool_expr_type == 1) {
                std::sort(parts.begin(), parts.end());
            }
            ss << "B:" << conj->bool_expr_type << "(";
            for (size_t i = 0; i < parts.size(); ++i) {
                if (i > 0) ss << ",";
                ss << parts[i];
            }
            ss << ")";
            break;
        }
        default:
            ok = false;
            return {};
    }
    return ss.str();
}

void AppendChildCanonTables(SubtreeSignature& parent, const SubtreeSignature& child) {
    const size_t offset = parent.canon_to_table.size();
    for (size_t i = 0; i < child.canon_to_table.size(); ++i) {
        size_t actual = child.canon_to_table[i];
        parent.table_to_canon[actual] = offset + i;
        parent.canon_to_table.push_back(actual);
    }
    for (const auto& kv : child.canon_to_get) {
        parent.canon_to_get[offset + kv.first] = kv.second;
    }
}

void ComputeSignatures(LogicalOperator* op,
                       std::map<LogicalOperator*, SubtreeSignature>& sigs) {
    if (!op) {
        return;
    }
    for (auto& child : op->children) {
        ComputeSignatures(child.get(), sigs);
    }

    SubtreeSignature sig;
    auto add_table = [&](size_t actual_tidx, LogicalGet* get_ptr = nullptr) -> size_t {
        size_t cid = sig.canon_to_table.size();
        sig.table_to_canon[actual_tidx] = cid;
        sig.canon_to_table.push_back(actual_tidx);
        if (get_ptr != nullptr) {
            sig.canon_to_get[cid] = get_ptr;
        }
        return cid;
    };

    switch (op->type) {
        case LogicalOperatorType::LOGICAL_GET: {
            auto* get = static_cast<LogicalGet*>(op);
            add_table(get->table_index.index, get);
            std::vector<std::string> fkeys;
            bool ok = true;
            for (const auto& f : get->filters) {
                fkeys.push_back(CanonicalizeExpr(f.get(), sig.table_to_canon, ok));
                if (!ok) break;
            }
            if (!ok) {
                break;
            }
            std::sort(fkeys.begin(), fkeys.end());
            std::stringstream ss;
            ss << "GET(" << get->relid << ";";
            for (size_t i = 0; i < fkeys.size(); ++i) {
                if (i > 0) ss << "&";
                ss << fkeys[i];
            }
            ss << ")";
            sig.eligible = true;
            sig.has_join_or_agg = false;
            sig.sum_base_scan_rows = std::max<size_t>(1, get->estimated_cardinality);
            sig.key = ss.str();
            break;
        }
        case LogicalOperatorType::LOGICAL_FILTER: {
            if (op->children.size() != 1 || !op->children[0]) break;
            const auto& csig = sigs[op->children[0].get()];
            if (!csig.eligible) break;
            AppendChildCanonTables(sig, csig);
            auto* filter = static_cast<LogicalFilter*>(op);
            std::vector<std::string> pkeys;
            bool ok = true;
            for (const auto& p : filter->expressions) {
                pkeys.push_back(CanonicalizeExpr(p.get(), sig.table_to_canon, ok));
                if (!ok) break;
            }
            if (!ok) break;
            std::sort(pkeys.begin(), pkeys.end());
            std::stringstream ss;
            ss << "FILTER(" << csig.key << ";";
            for (size_t i = 0; i < pkeys.size(); ++i) {
                if (i > 0) ss << "&";
                ss << pkeys[i];
            }
            ss << ")";
            sig.eligible = true;
            sig.has_join_or_agg = csig.has_join_or_agg;
            sig.has_agg = csig.has_agg;
            sig.sum_base_scan_rows = csig.sum_base_scan_rows;
            sig.key = ss.str();
            break;
        }
        case LogicalOperatorType::LOGICAL_PROJECTION: {
            if (op->children.size() != 1 || !op->children[0]) break;
            const auto& csig = sigs[op->children[0].get()];
            if (!csig.eligible) break;
            AppendChildCanonTables(sig, csig);
            auto* proj = static_cast<LogicalProjection*>(op);
            std::vector<std::string> ekeys;
            bool ok = true;
            for (const auto& e : proj->expressions) {
                ekeys.push_back(CanonicalizeExpr(e.get(), sig.table_to_canon, ok));
                if (!ok) break;
            }
            if (!ok) break;
            add_table(proj->table_index.index);
            std::stringstream ss;
            ss << "PROJ(" << csig.key << ";";
            for (size_t i = 0; i < ekeys.size(); ++i) {
                if (i > 0) ss << ",";
                ss << ekeys[i];
            }
            ss << ")";
            sig.eligible = true;
            sig.has_join_or_agg = csig.has_join_or_agg;
            sig.has_agg = csig.has_agg;
            sig.sum_base_scan_rows = csig.sum_base_scan_rows;
            sig.key = ss.str();
            break;
        }
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
            if (op->children.size() != 1 || !op->children[0]) break;
            const auto& csig = sigs[op->children[0].get()];
            if (!csig.eligible) break;
            AppendChildCanonTables(sig, csig);
            auto* agg = static_cast<LogicalAggregate*>(op);
            bool ok = true;
            std::vector<std::string> gkeys;
            for (const auto& g : agg->groups) {
                gkeys.push_back(CanonicalizeExpr(g.get(), sig.table_to_canon, ok));
                if (!ok) break;
            }
            std::vector<std::string> akeys;
            if (ok) {
                for (const auto& a : agg->expressions) {
                    akeys.push_back(CanonicalizeExpr(a.get(), sig.table_to_canon, ok));
                    if (!ok) break;
                }
            }
            if (!ok) break;
            add_table(agg->group_index.index);
            add_table(agg->aggregate_index.index);
            std::stringstream ss;
            ss << "AGG(" << csig.key << ";G=[";
            for (size_t i = 0; i < gkeys.size(); ++i) {
                if (i > 0) ss << ",";
                ss << gkeys[i];
            }
            ss << "];A=[";
            for (size_t i = 0; i < akeys.size(); ++i) {
                if (i > 0) ss << ",";
                ss << akeys[i];
            }
            ss << "])";
            sig.eligible = true;
            sig.has_join_or_agg = true;
            sig.has_agg = true;
            sig.sum_base_scan_rows = csig.sum_base_scan_rows;
            sig.key = ss.str();
            break;
        }
        case LogicalOperatorType::LOGICAL_DISTINCT: {
            if (op->children.size() != 1 || !op->children[0]) break;
            const auto& csig = sigs[op->children[0].get()];
            if (!csig.eligible) break;
            AppendChildCanonTables(sig, csig);
            auto* dist = static_cast<LogicalDistinct*>(op);
            bool ok = true;
            std::vector<std::string> dkeys;
            for (const auto& e : dist->expressions) {
                dkeys.push_back(CanonicalizeExpr(e.get(), sig.table_to_canon, ok));
                if (!ok) break;
            }
            if (!ok) break;
            std::stringstream ss;
            ss << "DIST(" << csig.key << ";";
            for (size_t i = 0; i < dkeys.size(); ++i) {
                if (i > 0) ss << ",";
                ss << dkeys[i];
            }
            ss << ")";
            sig.eligible = true;
            sig.has_join_or_agg = csig.has_join_or_agg;
            sig.has_agg = csig.has_agg;
            sig.sum_base_scan_rows = csig.sum_base_scan_rows;
            sig.key = ss.str();
            break;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
            if (op->children.size() != 2 || !op->children[0] || !op->children[1]) break;
            auto* join = static_cast<LogicalComparisonJoin*>(op);
            if (join->dependent || join->has_mark_index) break;
            const auto& lsig = sigs[op->children[0].get()];
            const auto& rsig = sigs[op->children[1].get()];
            if (!lsig.eligible || !rsig.eligible) break;

            // For INNER join, canonicalize child order by child signature so (A ⋈ B) == (B ⋈ A).
            const bool swap_order = (join->join_type == JOIN_INNER) && (rsig.key < lsig.key);
            const auto& first_sig = swap_order ? rsig : lsig;
            const auto& second_sig = swap_order ? lsig : rsig;
            AppendChildCanonTables(sig, first_sig);
            AppendChildCanonTables(sig, second_sig);

            bool ok = true;
            std::vector<std::string> ckeys;
            for (const auto& c : join->conditions) {
                ckeys.push_back(CanonicalizeExpr(c.get(), sig.table_to_canon, ok));
                if (!ok) break;
            }
            if (!ok) break;
            std::sort(ckeys.begin(), ckeys.end());
            std::stringstream ss;
            ss << "JOIN(" << join->join_type << ";" << first_sig.key << ";" << second_sig.key << ";";
            for (size_t i = 0; i < ckeys.size(); ++i) {
                if (i > 0) ss << "&";
                ss << ckeys[i];
            }
            ss << ")";
            sig.eligible = true;
            sig.has_join_or_agg = true;
            sig.has_agg = lsig.has_agg || rsig.has_agg;
            sig.sum_base_scan_rows = lsig.sum_base_scan_rows + rsig.sum_base_scan_rows;
            sig.key = ss.str();
            break;
        }
        case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
            if (op->children.size() != 2 || !op->children[0] || !op->children[1]) break;
            const auto& lsig = sigs[op->children[0].get()];
            const auto& rsig = sigs[op->children[1].get()];
            if (!lsig.eligible || !rsig.eligible) break;
            const bool swap_order = rsig.key < lsig.key;
            const auto& first_sig = swap_order ? rsig : lsig;
            const auto& second_sig = swap_order ? lsig : rsig;
            AppendChildCanonTables(sig, first_sig);
            AppendChildCanonTables(sig, second_sig);
            sig.eligible = true;
            sig.has_join_or_agg = true;
            sig.has_agg = lsig.has_agg || rsig.has_agg;
            sig.sum_base_scan_rows = lsig.sum_base_scan_rows + rsig.sum_base_scan_rows;
            sig.key = "CROSS(" + first_sig.key + ";" + second_sig.key + ")";
            break;
        }
        default:
            break;
    }

    sigs[op] = std::move(sig);
}

bool IsAncestorOf(const LogicalOperator* anc, const LogicalOperator* target) {
    if (!anc) return false;
    if (anc == target) return true;
    for (const auto& ch : anc->children) {
        if (IsAncestorOf(ch.get(), target)) return true;
    }
    return false;
}

std::unique_ptr<LogicalOperator>* FindSlotForPointer(std::unique_ptr<LogicalOperator>& cur,
                                                     const LogicalOperator* target) {
    if (cur.get() == target) {
        return &cur;
    }
    if (!cur) {
        return nullptr;
    }
    for (auto& ch : cur->children) {
        if (auto* found = FindSlotForPointer(ch, target)) {
            return found;
        }
    }
    return nullptr;
}

// Find the lowest common ancestor slot containing all target pointers.
std::unique_ptr<LogicalOperator>* FindLCASlot(std::unique_ptr<LogicalOperator>& cur,
                                              const std::vector<LogicalOperator*>& targets) {
    if (!cur) return nullptr;
    for (auto* t : targets) {
        if (!IsAncestorOf(cur.get(), t)) {
            return nullptr;
        }
    }
    for (auto& ch : cur->children) {
        if (auto* deeper = FindLCASlot(ch, targets)) {
            return deeper;
        }
    }
    return &cur;
}

bool RunOneCseRound(std::unique_ptr<LogicalOperator>& root, size_t& next_table_index) {
    std::map<LogicalOperator*, SubtreeSignature> sigs;
    ComputeSignatures(root.get(), sigs);

    // Group eligible nodes with join or agg by canonical key.
    std::map<std::string, std::vector<LogicalOperator*>> groups;
    for (auto& kv : sigs) {
        LogicalOperator* op = kv.first;
        const SubtreeSignature& sig = kv.second;
        if (!sig.eligible || !sig.has_join_or_agg) {
            continue;
        }
        // Guard against materializing a huge blowing-up join.
        if (op->estimated_cardinality > 2 * std::max<size_t>(1, sig.sum_base_scan_rows)) {
            continue;
        }
        groups[sig.key].push_back(op);
    }

    // Filter to groups with >= 2 non-overlapping occurrences, pick highest savings.
    std::string best_key;
    std::vector<LogicalOperator*> best_nodes;
    size_t best_score = 0;

    for (auto& kv : groups) {
        auto& nodes = kv.second;
        if (nodes.size() < 2) {
            continue;
        }
        // Remove any node that is a descendant of another node in the same group.
        std::vector<LogicalOperator*> disjoint;
        for (auto* n : nodes) {
            bool inside_another = false;
            for (auto* m : nodes) {
                if (m != n && IsAncestorOf(m, n)) {
                    inside_another = true;
                    break;
                }
            }
            if (!inside_another) {
                disjoint.push_back(n);
            }
        }
        if (disjoint.size() < 2) {
            continue;
        }
        const auto& sig = sigs[disjoint[0]];
        // Cost guard for join-only CSE subplans (no aggregate reduction):
        // Avoid materializing multi-hundred-MB wide join spools (e.g. TPC-H Q02
        // at SF100 where partsupp ⋈ supplier ⋈ nation ⋈ region is ~16M wide rows
        // with 5 text columns, which spills several GB and blocks downstream
        // selective Bloom filters from pushing into partsupp). Narrow/selective
        // join subplans (e.g. TPC-H Q11, ~3.2M x 3 numeric cols) stay well
        // below the 256 MB threshold.
        if (!sig.has_agg) {
            std::set<const LogicalOperator*> skip_cand(disjoint.begin(), disjoint.end());
            std::set<BindingKey> cand_ext_refs;
            CollectReferencedBindingsOutside(root.get(), skip_cand, cand_ext_refs);
            std::set<std::pair<size_t, size_t>> cand_outputs;
            for (auto* occ : disjoint) {
                const auto& osig = sigs[occ];
                for (const auto& ref : cand_ext_refs) {
                    auto it = osig.table_to_canon.find(ref.first);
                    if (it != osig.table_to_canon.end()) {
                        cand_outputs.insert({it->second, ref.second});
                    }
                }
            }
            uint64_t est_row_bytes = 16;
            for (const auto& co : cand_outputs) {
                uint64_t col_bytes = 8;
                auto git = sig.canon_to_get.find(co.first);
                if (git != sig.canon_to_get.end() && git->second != nullptr &&
                    co.second < git->second->output_types.size()) {
                    const uint32_t oid = git->second->output_types[co.second].type_oid;
                    if (oid == 25u || oid == 1042u || oid == 1043u) {
                        col_bytes = 64;
                    }
                }
                est_row_bytes += col_bytes;
            }
            const uint64_t est_mat_bytes =
                static_cast<uint64_t>(disjoint[0]->estimated_cardinality) * est_row_bytes;
            static constexpr uint64_t kMaxJoinOnlyCseMaterializeBytes = 1024ULL * 1024ULL * 1024ULL;
            if (est_mat_bytes > kMaxJoinOnlyCseMaterializeBytes) {
                continue;
            }
        }
        // Prefer maximal subtrees (higher sum_base_scan_rows, then longer signature key).
        const size_t score = sig.sum_base_scan_rows * (disjoint.size() - 1) * 1000 + sig.key.size();
        if (score > best_score) {
            best_score = score;
            best_key = kv.first;
            best_nodes = std::move(disjoint);
        }
    }

    if (best_nodes.size() < 2) {
        return false;
    }

    // Check LCA: never hoist strictly inside a LOGICAL_MATERIALIZED_CTE's producer/consumer boundary oddly.
    auto* lca_slot = FindLCASlot(root, best_nodes);
    if (!lca_slot || !*lca_slot) {
        return false;
    }
    // Verify no occurrence IS the LCA itself.
    for (auto* n : best_nodes) {
        if (n == lca_slot->get()) {
            return false;
        }
    }

    // Collect all externally referenced bindings for each occurrence, mapped to canonical (canon_tid, col_idx).
    std::set<const LogicalOperator*> skip_all(best_nodes.begin(), best_nodes.end());
    std::set<BindingKey> ext_refs;
    CollectReferencedBindingsOutside(root.get(), skip_all, ext_refs);

    std::vector<std::pair<size_t, size_t>> canonical_outputs;  // (canon_tid, col_idx)
    std::set<std::pair<size_t, size_t>> canonical_seen;
    for (auto* occ : best_nodes) {
        const auto& sig = sigs[occ];
        for (const auto& ref : ext_refs) {
            auto it = sig.table_to_canon.find(ref.first);
            if (it != sig.table_to_canon.end()) {
                std::pair<size_t, size_t> ckey{it->second, ref.second};
                if (canonical_seen.insert(ckey).second) {
                    canonical_outputs.push_back(ckey);
                }
            }
        }
    }
    if (canonical_outputs.empty()) {
        return false;
    }

    // Choose best_nodes[0] as the producer and union projected_columns from all other copies' LogicalGets.
    LogicalOperator* producer_node = best_nodes[0];
    const auto& prod_sig = sigs[producer_node];
    for (size_t k = 1; k < best_nodes.size(); ++k) {
        const auto& other_sig = sigs[best_nodes[k]];
        for (const auto& kv : other_sig.canon_to_get) {
            auto pit = prod_sig.canon_to_get.find(kv.first);
            if (pit != prod_sig.canon_to_get.end() && pit->second != nullptr && kv.second != nullptr) {
                for (const auto proj_col : kv.second->projected_columns) {
                    EnsureProjectedColumn(*pit->second, proj_col);
                }
            }
        }
    }
    // Also ensure any canonical output coming from a LogicalGet is projected in producer_node.
    for (const auto& co : canonical_outputs) {
        auto pit = prod_sig.canon_to_get.find(co.first);
        if (pit != prod_sig.canon_to_get.end() && pit->second != nullptr) {
            EnsureProjectedColumn(*pit->second, ProjectionIndex{co.second});
        }
    }

    const TableIndex cte_idx{++next_table_index};
    const std::string cte_name = "cte_cse_" + std::to_string(cte_idx.index);

    std::vector<ColumnBinding> cte_output_bindings;
    std::vector<std::string> cte_output_names;
    cte_output_bindings.reserve(canonical_outputs.size());
    cte_output_names.reserve(canonical_outputs.size());
    for (size_t i = 0; i < canonical_outputs.size(); ++i) {
        const size_t actual_tidx = prod_sig.canon_to_table[canonical_outputs[i].first];
        cte_output_bindings.push_back(ColumnBinding{TableIndex{actual_tidx}, ProjectionIndex{canonical_outputs[i].second}});
        cte_output_names.push_back("c" + std::to_string(i));
    }

    const size_t producer_card = producer_node->estimated_cardinality;

    // Locate all slots BEFORE mutating any pointer in the tree.
    std::vector<std::unique_ptr<LogicalOperator>*> occ_slots;
    occ_slots.reserve(best_nodes.size());
    for (auto* occ : best_nodes) {
        auto* slot = FindSlotForPointer(*lca_slot, occ);
        if (!slot) {
            return false;
        }
        occ_slots.push_back(slot);
    }

    // Extract producer from occ_slots[0].
    std::unique_ptr<LogicalOperator> producer_owned = std::move(*occ_slots[0]);

    // Build rewrite map across all occurrences and replace each slot with a distinct LogicalCTERef.
    std::map<BindingKey, ColumnBinding> combined_rewrite_map;
    for (size_t k = 0; k < best_nodes.size(); ++k) {
        const TableIndex scan_idx{++next_table_index};
        const auto& occ_sig = sigs[best_nodes[k]];
        for (size_t pos = 0; pos < canonical_outputs.size(); ++pos) {
            const size_t actual_tidx = occ_sig.canon_to_table[canonical_outputs[pos].first];
            combined_rewrite_map[{actual_tidx, canonical_outputs[pos].second}] =
                ColumnBinding{scan_idx, ProjectionIndex{pos}};
        }
        auto scan = std::make_unique<LogicalCTERef>(scan_idx, cte_idx, cte_name);
        scan->output_names = cte_output_names;
        scan->estimated_cardinality = producer_card;
        *occ_slots[k] = std::move(scan);
    }

    auto cte = std::make_unique<LogicalMaterializedCTE>(cte_idx, cte_name);
    cte->output_bindings = std::move(cte_output_bindings);
    cte->output_names = std::move(cte_output_names);
    cte->estimated_cardinality = (*lca_slot)->estimated_cardinality;
    LogicalOperator* producer_raw = producer_owned.get();
    LogicalOperator* cte_raw = cte.get();
    cte->children.push_back(std::move(producer_owned));
    cte->children.push_back(std::move(*lca_slot));
    *lca_slot = std::move(cte);

    // Rewrite references to all replaced subtrees everywhere outside the CTE producer
    // (and without rewriting this CTE's own output_bindings).
    RewriteBindingsInSubtree(root.get(), combined_rewrite_map, producer_raw, cte_raw);
    return true;
}

} // namespace

OptimizerPass CommonSubplanElimination::Pass() const {
    return OptimizerPass::COMMON_SUBPLAN_ELIMINATION;
}

std::unique_ptr<LogicalOperator> CommonSubplanElimination::Optimize(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) {
        return nullptr;
    }

    size_t next_table_index = FindMaxTableIndex(plan.get()) + 100;

    // Pass 1: Correlated scale-invariant delim-aggregate reuse (e.g. TPC-H Q17).
    for (int round = 0; round < 4; ++round) {
        if (!TryRewriteDelimScaleInvariantAgg(plan, plan, next_table_index)) {
            break;
        }
    }

    // Pass 2: Self-join filtered-aggregate reuse via eager aggregation (e.g. TPC-H Q18).
    for (int round = 0; round < 4; ++round) {
        if (!TryRewriteSelfJoinFilteredAgg(plan, next_table_index)) {
            break;
        }
    }

    // Pass 3: General common subplan elimination (e.g. TPC-H Q15, Q11, Q02, views/CTEs).
    for (int round = 0; round < 4; ++round) {
        if (!RunOneCseRound(plan, next_table_index)) {
            break;
        }
    }

    return plan;
}

} // namespace yaap
