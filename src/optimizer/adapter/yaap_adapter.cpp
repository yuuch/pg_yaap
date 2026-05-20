extern "C" {
#include "postgres.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_class.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "access/relation.h"
#include "access/htup_details.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/value.h"
#include "optimizer/optimizer.h"
#include "optimizer/plancat.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
}

#include "yaap_adapter.hpp"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <iterator>
#include <limits>
#include <string>
#include <sstream>
#include <set>
#include <stdexcept>

namespace yaap {

namespace {

bool TryParsePositiveIntConstant(Expression *expr, int &out_value) {
    const auto *constant = dynamic_cast<const BoundConstantExpression *>(expr);
    if (constant == nullptr || constant->is_null) {
        return false;
    }
    char *end = nullptr;
    errno = 0;
    long parsed = std::strtol(constant->value.c_str(), &end, 10);
    if (errno != 0 || end == constant->value.c_str()) {
        return false;
    }
    while (*end != '\0' && std::isspace(static_cast<unsigned char>(*end))) {
        ++end;
    }
    if (*end != '\0' || parsed <= 0 || parsed > std::numeric_limits<int>::max()) {
        return false;
    }
    out_value = static_cast<int>(parsed);
    return true;
}

const BoundColumnRefExpression *UnwrapPrefixBaseColumn(const Expression *expr) {
    if (expr == nullptr) {
        return nullptr;
    }
    if (const auto *column = dynamic_cast<const BoundColumnRefExpression *>(expr)) {
        return column;
    }
    const auto *func = dynamic_cast<const BoundFunctionExpression *>(expr);
    if (func == nullptr || func->children.size() != 1) {
        return nullptr;
    }
    if (func->function_name != "text" &&
        func->function_name != "varchar" &&
        func->function_name != "bpchar" &&
        func->function_name != "char") {
        return nullptr;
    }
    return UnwrapPrefixBaseColumn(func->children[0].get());
}

bool IsPrefixSliceExpr(const Expression *expr,
                      const BoundColumnRefExpression *&out_base,
                      int &out_len) {
    const auto *func = dynamic_cast<const BoundFunctionExpression *>(expr);
    if (func == nullptr || func->function_name != "prefix_slice" || func->children.size() != 2) {
        return false;
    }
    out_base = UnwrapPrefixBaseColumn(func->children[0].get());
    if (out_base == nullptr) {
        return false;
    }
    return TryParsePositiveIntConstant(func->children[1].get(), out_len);
}

std::unique_ptr<Expression> CloneColumnRef(const BoundColumnRefExpression &column) {
    return std::make_unique<BoundColumnRefExpression>(column.binding, column.table_name, column.column_name);
}

std::unique_ptr<Expression> CloneExpression(const Expression &expr) {
    switch (expr.type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            const auto &column = static_cast<const BoundColumnRefExpression &>(expr);
            return CloneColumnRef(column);
        }
        case ExpressionType::BOUND_CONSTANT: {
            const auto &constant = static_cast<const BoundConstantExpression &>(expr);
            return std::make_unique<BoundConstantExpression>(constant.value, constant.is_null);
        }
        case ExpressionType::BOUND_FUNCTION: {
            const auto &function = static_cast<const BoundFunctionExpression &>(expr);
            auto copy = std::make_unique<BoundFunctionExpression>(function.function_name, function.op_oid);
            for (const auto &child : function.children) {
                if (child == nullptr) {
                    return nullptr;
                }
                auto child_copy = CloneExpression(*child);
                if (!child_copy) {
                    return nullptr;
                }
                copy->children.push_back(std::move(child_copy));
            }
            return copy;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            const auto &conjunction = static_cast<const BoundConjunctionExpression &>(expr);
            auto copy = std::make_unique<BoundConjunctionExpression>(conjunction.bool_expr_type);
            for (const auto &child : conjunction.children) {
                if (child == nullptr) {
                    return nullptr;
                }
                auto child_copy = CloneExpression(*child);
                if (!child_copy) {
                    return nullptr;
                }
                copy->children.push_back(std::move(child_copy));
            }
            return copy;
        }
        default:
            return nullptr;
    }
}

std::unique_ptr<Expression> BuildPrefixLikePredicate(const BoundColumnRefExpression &column,
                                                     const std::string &prefix) {
    auto like = std::make_unique<BoundFunctionExpression>("~~", InvalidOid);
    like->children.push_back(CloneColumnRef(column));
    like->children.push_back(std::make_unique<BoundConstantExpression>(prefix + "%", false));
    return like;
}

std::unique_ptr<Expression> TryRewriteScalarArrayMembership(bool use_or,
                                                            Oid compare_opno,
                                                            const Expression &lhs,
                                                            const Expression &rhs) {
    if (compare_opno == InvalidOid) {
        return nullptr;
    }
    char *opname = get_opname(compare_opno);
    if (opname == nullptr) {
        return nullptr;
    }

    const BoundColumnRefExpression *base = nullptr;
    int prefix_len = 0;
    const auto *array = dynamic_cast<const BoundFunctionExpression *>(&rhs);
    if (array == nullptr || array->function_name != "array" || array->children.empty()) {
        return nullptr;
    }

    if (use_or && strcmp(opname, "=") == 0 && IsPrefixSliceExpr(&lhs, base, prefix_len)) {
        auto disjunction = std::make_unique<BoundConjunctionExpression>(OR_EXPR);
        for (const auto &child : array->children) {
            const auto *constant = dynamic_cast<const BoundConstantExpression *>(child.get());
            if (constant == nullptr || constant->is_null) {
                return nullptr;
            }
            if (static_cast<int>(constant->value.size()) != prefix_len) {
                return nullptr;
            }
            disjunction->children.push_back(BuildPrefixLikePredicate(*base, constant->value));
        }
        if (disjunction->children.empty()) {
            return nullptr;
        }
        if (disjunction->children.size() == 1) {
            return std::move(disjunction->children[0]);
        }
        return disjunction;
    }

    auto conjunction = std::make_unique<BoundConjunctionExpression>(use_or ? OR_EXPR : AND_EXPR);
    for (const auto &child : array->children) {
        const auto *constant = dynamic_cast<const BoundConstantExpression *>(child.get());
        if (constant == nullptr || constant->is_null) {
            return nullptr;
        }

        auto lhs_copy = CloneExpression(lhs);
        auto rhs_copy = CloneExpression(*constant);
        if (!lhs_copy || !rhs_copy) {
            return nullptr;
        }

        auto compare = std::make_unique<BoundFunctionExpression>(opname, compare_opno);
        compare->children.push_back(std::move(lhs_copy));
        compare->children.push_back(std::move(rhs_copy));
        conjunction->children.push_back(std::move(compare));
    }
    if (conjunction->children.empty()) {
        return nullptr;
    }
    if (conjunction->children.size() == 1) {
        return std::move(conjunction->children[0]);
    }
    return conjunction;
}

bool IsPrefixSliceFunc(const std::string &function_name, ::List *args) {
    if (function_name != "substring" && function_name != "substr") {
        return false;
    }
    if (args == nullptr || list_length(args) != 3) {
        return false;
    }
    ::Node *start_arg = (::Node *) list_nth(args, 1);
    ::Node *len_arg = (::Node *) list_nth(args, 2);
    if (!IsA(start_arg, Const) || !IsA(len_arg, Const)) {
        return false;
    }
    Const *start_const = (Const *) start_arg;
    Const *len_const = (Const *) len_arg;
    if (start_const->constisnull || len_const->constisnull) {
        return false;
    }
    return DatumGetInt32(start_const->constvalue) == 1 && DatumGetInt32(len_const->constvalue) > 0;
}

int MapPGJoinType(::JoinType join_type) {
    switch (join_type) {
        case ::JOIN_INNER:
            return JOIN_INNER;
        case ::JOIN_LEFT:
            return JOIN_LEFT;
        case ::JOIN_FULL:
            return JOIN_FULL;
        case ::JOIN_RIGHT:
            return JOIN_RIGHT;
        case ::JOIN_SEMI:
            return JOIN_SEMI;
        case ::JOIN_ANTI:
            return JOIN_ANTI;
        default:
            throw std::runtime_error("Unsupported PostgreSQL join type");
    }
}

struct ProducedOutput {
	ColumnBinding binding;
	std::string name;
	std::string semantic_key;
	std::string table_name;
	bool has_source_binding = false;
	ColumnBinding source_binding{TableIndex{0}, ProjectionIndex{0}};
};

std::vector<ColumnBinding> CollectColumnBindings(Expression* expression,
                                                 const std::set<size_t>& allowed_tables);
void CollectColumnBindingsInternal(Expression* expression,
                                   const std::set<size_t>& allowed_tables,
                                   std::vector<ColumnBinding>& bindings,
                                   std::set<std::pair<size_t, size_t>>& seen);

bool IsSupportedWindowFunctionName(const std::string& function_name) {
    return function_name == "row_number" ||
           function_name == "rank" ||
           function_name == "dense_rank" ||
           function_name == "percent_rank" ||
           function_name == "cume_dist";
}

std::string ExpressionSemanticKey(Expression* expression) {
    if (!expression) {
        return "<null>";
    }

    std::stringstream ss;
    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* column = static_cast<BoundColumnRefExpression*>(expression);
            ss << "col:" << column->table_name << "." << column->column_name;
            break;
        }
        case ExpressionType::BOUND_CONSTANT: {
            auto* constant = static_cast<BoundConstantExpression*>(expression);
            ss << "const:" << (constant->is_null ? "NULL" : constant->value);
            break;
        }
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            ss << "fn:" << function->function_name << "(";
            for (size_t i = 0; i < function->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << ExpressionSemanticKey(function->children[i].get());
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression);
            ss << "agg:" << aggregate->function_name << "(";
            for (size_t i = 0; i < aggregate->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << ExpressionSemanticKey(aggregate->children[i].get());
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            ss << "conj:" << conjunction->bool_expr_type << "(";
            for (size_t i = 0; i < conjunction->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << ExpressionSemanticKey(conjunction->children[i].get());
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subquery = static_cast<BoundSubqueryExpression*>(expression);
            ss << "subquery:" << subquery->sublink_name << "(";
            for (size_t i = 0; i < subquery->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << ExpressionSemanticKey(subquery->children[i].get());
            }
            ss << ")";
            break;
        }
        default:
            ss << "opaque";
            break;
    }
    return ss.str();
}

std::string DerivedColumnName(Expression* expression, size_t index) {
    if (!expression) {
        return "col" + std::to_string(index + 1);
    }
    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF:
            return static_cast<BoundColumnRefExpression*>(expression)->column_name;
        case ExpressionType::BOUND_AGGREGATE:
            return static_cast<BoundAggregateExpression*>(expression)->function_name;
        case ExpressionType::BOUND_CONSTANT:
            return "const";
        default:
            return "col" + std::to_string(index + 1);
    }
}

std::string DerivedTableName(Expression* expression, const std::string& fallback) {
	if (expression != nullptr && expression->type == ExpressionType::BOUND_COLUMN_REF) {
		return static_cast<BoundColumnRefExpression*>(expression)->table_name;
	}
	return fallback;
}

ProducedOutput MakeProducedOutput(ColumnBinding binding,
								  std::string table_name,
								  std::string name,
								  Expression* source_expression = nullptr) {
	ProducedOutput output;
	output.binding = binding;
	output.name = std::move(name);
	output.semantic_key = source_expression != nullptr ? ExpressionSemanticKey(source_expression) : std::string();
	output.table_name = std::move(table_name);
	if (source_expression != nullptr && source_expression->type == ExpressionType::BOUND_COLUMN_REF) {
		output.has_source_binding = true;
		output.source_binding = static_cast<BoundColumnRefExpression*>(source_expression)->binding;
	}
	return output;
}

bool SameBinding(ColumnBinding left, ColumnBinding right) {
	return left.table_index.index == right.table_index.index &&
		   left.column_index.index == right.column_index.index;
}

LogicalOperator* SkipTransparentOperators(LogicalOperator* input);

bool CollectAggrefNodesWalker(::Node *node, void *context) {
    if (node == nullptr) {
        return false;
    }
    if (IsA(node, Aggref)) {
        auto &aggrefs = *static_cast<std::vector<Aggref *> *>(context);
        auto *aggref = reinterpret_cast<Aggref *>(node);
        if (std::find(aggrefs.begin(), aggrefs.end(), aggref) == aggrefs.end()) {
            aggrefs.push_back(aggref);
        }
        return false;
    }
	if (IsA(node, SubLink)) {
		return false;
	}
    return expression_tree_walker(node, CollectAggrefNodesWalker, context);
}

std::vector<Aggref *> CollectQueryAggregates(::Query *pg_query) {
    std::vector<Aggref *> aggrefs;
    if (pg_query == nullptr) {
        return aggrefs;
    }

    ListCell *lc = nullptr;
    foreach(lc, pg_query->targetList) {
        auto *te = reinterpret_cast<TargetEntry *>(lfirst(lc));
        if (te == nullptr || te->resjunk) {
            continue;
        }
        CollectAggrefNodesWalker(reinterpret_cast<Node *>(te->expr), &aggrefs);
    }
    if (pg_query->havingQual != nullptr) {
        CollectAggrefNodesWalker(pg_query->havingQual, &aggrefs);
    }
    return aggrefs;
}

bool TryResolveTargetListBinding(::Query *pg_query,
                                 TargetEntry *target_entry,
                                 LogicalOperator *input,
                                 std::unique_ptr<Expression> &out_expression) {
    if (pg_query == nullptr || target_entry == nullptr || input == nullptr) {
        return false;
    }

    auto *producer = SkipTransparentOperators(input);
    if (producer == nullptr || producer->type != LogicalOperatorType::LOGICAL_PROJECTION) {
        return false;
    }

    auto *projection = static_cast<LogicalProjection *>(producer);
    size_t projected_idx = 0;
    ListCell *lc = nullptr;
    foreach(lc, pg_query->targetList) {
        auto *candidate = reinterpret_cast<TargetEntry *>(lfirst(lc));
        if (candidate == nullptr || candidate->resjunk) {
            continue;
        }
        if (candidate == target_entry) {
            if (projected_idx >= projection->output_names.size()) {
                return false;
            }
            out_expression = std::make_unique<BoundColumnRefExpression>(
                ColumnBinding{projection->table_index, ProjectionIndex{projected_idx}},
                "proj",
                projection->output_names[projected_idx]);
            return true;
        }
        ++projected_idx;
    }
    return false;
}

LogicalOperator* SkipTransparentOperators(LogicalOperator* input) {
    while (input && (
               (input->type == LogicalOperatorType::LOGICAL_FILTER && input->children.size() == 1) ||
               (input->type == LogicalOperatorType::LOGICAL_DISTINCT && input->children.size() == 1) ||
               (input->type == LogicalOperatorType::LOGICAL_ORDER && input->children.size() == 1) ||
               (input->type == LogicalOperatorType::LOGICAL_LIMIT && input->children.size() == 1))) {
        input = input->children[0].get();
    }
    return input;
}

std::vector<ProducedOutput> CollectProducedOutputs(LogicalOperator* input) {
    std::vector<ProducedOutput> outputs;
    input = SkipTransparentOperators(input);
    if (!input) {
        return outputs;
    }

    switch (input->type) {
        case LogicalOperatorType::LOGICAL_PROJECTION: {
            auto* projection = static_cast<LogicalProjection*>(input);
            for (size_t idx = 0; idx < projection->expressions.size(); ++idx) {
				auto* source = projection->expressions[idx].get();
				std::string name = idx < projection->output_names.size()
					? projection->output_names[idx]
					: "col" + std::to_string(idx + 1);
				outputs.push_back(MakeProducedOutput(
                    ColumnBinding{projection->table_index, ProjectionIndex{idx}},
					DerivedTableName(source, "proj"),
					std::move(name),
					source));
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
            auto* aggregate = static_cast<LogicalAggregate*>(input);
            for (size_t idx = 0; idx < aggregate->groups.size(); ++idx) {
				auto* source = aggregate->groups[idx].get();
				std::string name = idx < aggregate->group_names.size()
					? aggregate->group_names[idx]
					: "group" + std::to_string(idx + 1);
				outputs.push_back(MakeProducedOutput(
                    ColumnBinding{aggregate->group_index, ProjectionIndex{idx}},
					DerivedTableName(source, "group"),
					std::move(name),
					source));
            }
            for (size_t idx = 0; idx < aggregate->expressions.size(); ++idx) {
				auto* source = aggregate->expressions[idx].get();
				std::string name = idx < aggregate->aggregate_names.size()
					? aggregate->aggregate_names[idx]
					: "agg" + std::to_string(idx + 1);
				outputs.push_back(MakeProducedOutput(
                    ColumnBinding{aggregate->aggregate_index, ProjectionIndex{idx}},
					"agg",
					std::move(name),
					source));
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_SET_OPERATION: {
            auto* setop = static_cast<LogicalSetOperation*>(input);
            for (size_t idx = 0; idx < setop->output_names.size(); ++idx) {
				outputs.push_back(MakeProducedOutput(
                    ColumnBinding{setop->table_index, ProjectionIndex{idx}},
					"setop",
					setop->output_names[idx]));
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_DELIM_GET: {
            auto* delim_get = static_cast<LogicalDelimGet*>(input);
            for (size_t idx = 0; idx < delim_get->correlated_columns.size(); ++idx) {
				std::string name = idx < delim_get->output_names.size()
					? delim_get->output_names[idx]
					: "delim" + std::to_string(idx + 1);
				outputs.push_back(MakeProducedOutput(
                    ColumnBinding{delim_get->table_index, ProjectionIndex{idx}},
					"delim",
					std::move(name)));
            }
            break;
        }
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
		case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
			for (const auto& child : input->children) {
				auto child_outputs = CollectProducedOutputs(child.get());
				outputs.insert(outputs.end(),
							   std::make_move_iterator(child_outputs.begin()),
							   std::make_move_iterator(child_outputs.end()));
			}
			auto* join = dynamic_cast<LogicalComparisonJoin*>(input);
			if (join != nullptr && join->has_mark_index) {
				outputs.push_back(MakeProducedOutput(
					ColumnBinding{join->mark_index, ProjectionIndex{0}},
					"subquery",
					"mark"));
			}
			break;
		}
        case LogicalOperatorType::LOGICAL_GET: {
            break;
        }
        default:
            break;
    }

    return outputs;
}

void CollectCorrelatedColumnsFromPlan(LogicalOperator* op,
                                      const std::set<size_t>& allowed_tables,
                                      std::vector<ColumnBinding>& bindings,
                                      std::set<std::pair<size_t, size_t>>& seen) {
    if (!op) {
        return;
    }

    auto collect_expression_list = [&](std::vector<std::unique_ptr<Expression>>& expressions) {
        for (auto& expression : expressions) {
            auto expr_bindings = CollectColumnBindings(expression.get(), allowed_tables);
            for (const auto& binding : expr_bindings) {
                auto key = std::make_pair(binding.table_index.index, binding.column_index.index);
                if (seen.insert(key).second) {
                    bindings.push_back(binding);
                }
            }
        }
    };

    switch (op->type) {
        case LogicalOperatorType::LOGICAL_FILTER:
            collect_expression_list(static_cast<LogicalFilter*>(op)->expressions);
            break;
        case LogicalOperatorType::LOGICAL_PROJECTION:
            collect_expression_list(static_cast<LogicalProjection*>(op)->expressions);
            break;
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
            auto* aggregate = static_cast<LogicalAggregate*>(op);
            collect_expression_list(aggregate->groups);
            collect_expression_list(aggregate->expressions);
            break;
        }
        case LogicalOperatorType::LOGICAL_SET_OPERATION:
            break;
        case LogicalOperatorType::LOGICAL_DISTINCT:
            collect_expression_list(static_cast<LogicalDistinct*>(op)->expressions);
            break;
        case LogicalOperatorType::LOGICAL_ORDER: {
            auto* order = static_cast<LogicalOrder*>(op);
            for (auto& entry : order->orders) {
                CollectColumnBindingsInternal(entry.expression.get(), allowed_tables, bindings, seen);
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_WINDOW: {
            auto* window = static_cast<LogicalWindow*>(op);
            collect_expression_list(window->partitions);
            for (auto& entry : window->orders) {
                CollectColumnBindingsInternal(entry.expression.get(), allowed_tables, bindings, seen);
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_LIMIT: {
            auto* limit = static_cast<LogicalLimit*>(op);
            CollectColumnBindingsInternal(limit->limit_count.get(), allowed_tables, bindings, seen);
            CollectColumnBindingsInternal(limit->limit_offset.get(), allowed_tables, bindings, seen);
            break;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
            auto* join = static_cast<LogicalComparisonJoin*>(op);
            collect_expression_list(join->conditions);
            break;
        }
        default:
            break;
    }

    for (auto& child : op->children) {
        CollectCorrelatedColumnsFromPlan(child.get(), allowed_tables, bindings, seen);
    }
}

std::vector<ColumnBinding> CollectCorrelatedColumnsFromPlan(LogicalOperator* op,
                                                            const std::set<size_t>& allowed_tables) {
    std::vector<ColumnBinding> bindings;
    std::set<std::pair<size_t, size_t>> seen;
    CollectCorrelatedColumnsFromPlan(op, allowed_tables, bindings, seen);
    return bindings;
}

std::string SubLinkName(int sublink_type) {
    switch (sublink_type) {
        case 0: return "EXISTS";
        case 1: return "ALL";
        case 2: return "ANY";
        case 3: return "ROWCOMPARE";
        case 4: return "EXPR";
        case 5: return "MULTIEXPR";
        case 6: return "ARRAY";
        case 7: return "CTE";
        default: return "SUBLINK";
    }
}

std::vector<ProducedOutput> ProducedOutputsFromProjection(LogicalProjection* projection) {
    std::vector<ProducedOutput> outputs;
    for (size_t idx = 0; idx < projection->expressions.size(); ++idx) {
        outputs.push_back({
            ColumnBinding{projection->table_index, ProjectionIndex{idx}},
            idx < projection->output_names.size() ? projection->output_names[idx] : "col" + std::to_string(idx + 1)
        });
    }
    return outputs;
}

std::vector<ProducedOutput> ProducedOutputsFromAggregate(LogicalAggregate* aggregate) {
    std::vector<ProducedOutput> outputs;
    for (size_t idx = 0; idx < aggregate->groups.size(); ++idx) {
        outputs.push_back({
            ColumnBinding{aggregate->group_index, ProjectionIndex{idx}},
            idx < aggregate->group_names.size() ? aggregate->group_names[idx] : "group" + std::to_string(idx + 1)
        });
    }
    for (size_t idx = 0; idx < aggregate->expressions.size(); ++idx) {
        outputs.push_back({
            ColumnBinding{aggregate->aggregate_index, ProjectionIndex{idx}},
            idx < aggregate->aggregate_names.size() ? aggregate->aggregate_names[idx] : "agg" + std::to_string(idx + 1)
        });
    }
    return outputs;
}

std::vector<ProducedOutput> ProducedOutputsFromWindow(LogicalWindow* window) {
    std::vector<ProducedOutput> outputs;
    for (size_t idx = 0; idx < window->function_names.size(); ++idx) {
        outputs.push_back({
            ColumnBinding{window->table_index, ProjectionIndex{idx}},
            idx < window->output_names.size() ? window->output_names[idx] : "window" + std::to_string(idx + 1)
        });
    }
    return outputs;
}

WindowClause* FindWindowClause(::Query* pg_query, Index winref) {
    if (!pg_query) {
        return nullptr;
    }

    ListCell* lc;
    foreach(lc, pg_query->windowClause) {
        auto* window_clause = (WindowClause*)lfirst(lc);
        if (window_clause->winref == winref) {
            return window_clause;
        }
    }
    return nullptr;
}

void CollectColumnBindingsInternal(Expression* expression,
                                   const std::set<size_t>& allowed_tables,
                                   std::vector<ColumnBinding>& bindings,
                                   std::set<std::pair<size_t, size_t>>& seen) {
    if (!expression) {
        return;
    }

    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* column = static_cast<BoundColumnRefExpression*>(expression);
            if (allowed_tables.empty() || allowed_tables.find(column->binding.table_index.index) != allowed_tables.end()) {
                auto key = std::make_pair(column->binding.table_index.index, column->binding.column_index.index);
                if (seen.insert(key).second) {
                    bindings.push_back(column->binding);
                }
            }
            break;
        }
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            for (auto& child : function->children) {
                CollectColumnBindingsInternal(child.get(), allowed_tables, bindings, seen);
            }
            break;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression);
            for (auto& child : aggregate->children) {
                CollectColumnBindingsInternal(child.get(), allowed_tables, bindings, seen);
            }
            break;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            for (auto& child : conjunction->children) {
                CollectColumnBindingsInternal(child.get(), allowed_tables, bindings, seen);
            }
            break;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subquery = static_cast<BoundSubqueryExpression*>(expression);
            for (auto& child : subquery->children) {
                CollectColumnBindingsInternal(child.get(), allowed_tables, bindings, seen);
            }
            break;
        }
        default:
            break;
    }
}

std::vector<ColumnBinding> CollectColumnBindings(Expression* expression, const std::set<size_t>& allowed_tables) {
    std::vector<ColumnBinding> bindings;
    std::set<std::pair<size_t, size_t>> seen;
    CollectColumnBindingsInternal(expression, allowed_tables, bindings, seen);
    return bindings;
}

void CollectOutputTables(LogicalOperator* op, std::set<size_t>& table_indexes) {
    if (!op) {
        return;
    }

    switch (op->type) {
        case LogicalOperatorType::LOGICAL_GET: {
            auto* get = static_cast<LogicalGet*>(op);
            table_indexes.insert(get->table_index.index);
            break;
        }
        case LogicalOperatorType::LOGICAL_PROJECTION: {
            auto* projection = static_cast<LogicalProjection*>(op);
            table_indexes.insert(projection->table_index.index);
            for (auto& child : op->children) {
                CollectOutputTables(child.get(), table_indexes);
            }
            return;
        }
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
            auto* aggregate = static_cast<LogicalAggregate*>(op);
            table_indexes.insert(aggregate->group_index.index);
            table_indexes.insert(aggregate->aggregate_index.index);
            for (auto& child : op->children) {
                CollectOutputTables(child.get(), table_indexes);
            }
            return;
        }
        case LogicalOperatorType::LOGICAL_SET_OPERATION: {
            auto* setop = static_cast<LogicalSetOperation*>(op);
            table_indexes.insert(setop->table_index.index);
            for (auto& child : op->children) {
                CollectOutputTables(child.get(), table_indexes);
            }
            return;
        }
        case LogicalOperatorType::LOGICAL_WINDOW: {
            auto* window = static_cast<LogicalWindow*>(op);
            table_indexes.insert(window->table_index.index);
            for (auto& child : op->children) {
                CollectOutputTables(child.get(), table_indexes);
            }
            return;
        }
        case LogicalOperatorType::LOGICAL_DISTINCT:
        case LogicalOperatorType::LOGICAL_ORDER:
        case LogicalOperatorType::LOGICAL_LIMIT: {
            for (auto& child : op->children) {
                CollectOutputTables(child.get(), table_indexes);
            }
            return;
        }
        case LogicalOperatorType::LOGICAL_DELIM_GET: {
            auto* delim_get = static_cast<LogicalDelimGet*>(op);
            table_indexes.insert(delim_get->table_index.index);
            break;
        }
        default:
            for (auto& child : op->children) {
                CollectOutputTables(child.get(), table_indexes);
            }
            break;
    }
}

bool PromoteScalarSubqueryExpression(std::unique_ptr<Expression>& expression,
                                     std::unique_ptr<LogicalOperator>& plan,
                                     YaapAdapter& adapter);
std::unique_ptr<LogicalOperator> BuildDependentJoin(YaapAdapter& adapter,
                                                    std::unique_ptr<LogicalOperator> left,
                                                    BoundSubqueryExpression* subquery,
                                                    bool negated);

bool PromoteScalarSubqueryExpression(std::unique_ptr<Expression>& expression,
                                     std::unique_ptr<LogicalOperator>& plan,
                                     YaapAdapter& adapter) {
    if (!expression) {
        return false;
    }

    switch (expression->type) {
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression.get());
            bool promoted = false;
            for (auto& child : function->children) {
                promoted = PromoteScalarSubqueryExpression(child, plan, adapter) || promoted;
            }
            return promoted;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression.get());
            bool promoted = false;
            for (auto& child : aggregate->children) {
                promoted = PromoteScalarSubqueryExpression(child, plan, adapter) || promoted;
            }
            return promoted;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression.get());
            bool promoted = false;
            for (auto& child : conjunction->children) {
                promoted = PromoteScalarSubqueryExpression(child, plan, adapter) || promoted;
            }
            return promoted;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subquery = static_cast<BoundSubqueryExpression*>(expression.get());
            if (!subquery->subquery_plan) {
                return false;
            }
            if (subquery->sublink_type == 0 || subquery->sublink_type == 2 || subquery->sublink_type == 6) {
                auto join_plan = BuildDependentJoin(adapter, std::move(plan), subquery, false);
                auto* join = static_cast<LogicalComparisonJoin*>(join_plan.get());
                if (!join->has_mark_index) {
                    return false;
                }
                expression = std::make_unique<BoundColumnRefExpression>(
                    ColumnBinding{join->mark_index, ProjectionIndex{0}},
                    "subquery",
                    "mark");
                plan = std::move(join_plan);
                return true;
            }
            if (subquery->sublink_type != 3 && subquery->sublink_type != 4 && subquery->sublink_type != 5) {
                return false;
            }

            auto outputs = CollectProducedOutputs(subquery->subquery_plan.get());
            if (outputs.size() != 1) {
                return false;
            }

            auto subquery_plan = std::move(subquery->subquery_plan);
            auto output = outputs.front();
            expression = std::make_unique<BoundColumnRefExpression>(
                output.binding,
                "subquery",
                output.name);

            auto join = std::make_unique<LogicalDependentJoin>(JOIN_SINGLE);
            join->dependent = true;
            join->children.push_back(std::move(plan));
            join->children.push_back(std::move(subquery_plan));
            join->estimated_cardinality = join->children[0]->estimated_cardinality;
            std::set<size_t> left_tables;
            CollectOutputTables(join->children[0].get(), left_tables);
            join->correlated_columns = CollectCorrelatedColumnsFromPlan(join->children[1].get(), left_tables);
            if (!join->correlated_columns.empty()) {
                join->perform_delim = true;
            }
            plan = std::move(join);
            return true;
        }
        default:
            return false;
    }
}

bool IsNotConjunction(Expression* expression) {
    if (!expression || expression->type != ExpressionType::BOUND_CONJUNCTION) {
        return false;
    }
    auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
    return conjunction->bool_expr_type == 2 && conjunction->children.size() == 1;
}

BoundSubqueryExpression* ExtractTopLevelSubquery(Expression* expression) {
    if (!expression) {
        return nullptr;
    }
    if (expression->type == ExpressionType::BOUND_SUBQUERY) {
        return static_cast<BoundSubqueryExpression*>(expression);
    }
    if (IsNotConjunction(expression)) {
        auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
        if (conjunction->children[0] && conjunction->children[0]->type == ExpressionType::BOUND_SUBQUERY) {
            return static_cast<BoundSubqueryExpression*>(conjunction->children[0].get());
        }
    }
    return nullptr;
}

std::unique_ptr<Expression> RewriteSubqueryParams(std::unique_ptr<Expression> expression,
                                                  const std::vector<ProducedOutput>& outputs,
                                                  size_t& output_index) {
    if (!expression) {
        return nullptr;
    }

    switch (expression->type) {
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression.get());
            for (auto& child : function->children) {
                child = RewriteSubqueryParams(std::move(child), outputs, output_index);
            }
            return expression;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression.get());
            for (auto& child : aggregate->children) {
                child = RewriteSubqueryParams(std::move(child), outputs, output_index);
            }
            return expression;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression.get());
            for (auto& child : conjunction->children) {
                child = RewriteSubqueryParams(std::move(child), outputs, output_index);
            }
            return expression;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subquery = static_cast<BoundSubqueryExpression*>(expression.get());
            for (auto& child : subquery->children) {
                child = RewriteSubqueryParams(std::move(child), outputs, output_index);
            }
            return expression;
        }
        case ExpressionType::BOUND_CONSTANT: {
            auto* constant = static_cast<BoundConstantExpression*>(expression.get());
            if (!constant->is_null &&
                constant->value.rfind("param$", 0) == 0 &&
                output_index < outputs.size()) {
                auto& output = outputs[output_index++];
                return std::make_unique<BoundColumnRefExpression>(
                    output.binding,
                    "subquery",
                    output.name);
            }
            return expression;
        }
        default:
            return expression;
    }
}

std::unique_ptr<Expression> TryRewriteToProducedOutput(LogicalOperator* input,
													   Expression* expression,
													   const std::string& fingerprint) {
	if (input == nullptr || expression == nullptr) {
		return nullptr;
	}

	auto outputs = CollectProducedOutputs(input);
	auto find_unique = [&](auto predicate) -> const ProducedOutput* {
		const ProducedOutput* matched_output = nullptr;
		for (const auto& output : outputs) {
			if (!predicate(output)) {
				continue;
			}
			if (matched_output != nullptr) {
				return nullptr;
			}
			matched_output = &output;
		}
		return matched_output;
	};

	const ProducedOutput* matched_output = nullptr;
	if (expression->type == ExpressionType::BOUND_COLUMN_REF) {
		auto* column = static_cast<BoundColumnRefExpression*>(expression);
		matched_output = find_unique([&](const ProducedOutput& output) {
			return SameBinding(output.binding, column->binding);
		});
		if (matched_output == nullptr) {
			matched_output = find_unique([&](const ProducedOutput& output) {
				return output.has_source_binding && SameBinding(output.source_binding, column->binding);
			});
		}
	}
	if (matched_output == nullptr) {
		matched_output = find_unique([&](const ProducedOutput& output) {
			return !output.semantic_key.empty() && output.semantic_key == fingerprint;
		});
	}

	if (matched_output == nullptr) {
		return nullptr;
	}

	return std::make_unique<BoundColumnRefExpression>(
		matched_output->binding,
		matched_output->table_name.empty() ? "output" : matched_output->table_name,
		matched_output->name);
}

std::unique_ptr<LogicalOperator> BuildDependentJoin(YaapAdapter& adapter,
                                                    std::unique_ptr<LogicalOperator> left,
                                                    BoundSubqueryExpression* subquery,
                                                    bool negated) {
    if (!subquery || !subquery->subquery_plan) {
        throw std::runtime_error("Unsupported subquery predicate");
    }

    std::unique_ptr<LogicalOperator> right = std::move(subquery->subquery_plan);
    int join_type = JOIN_SEMI;
    if (subquery->sublink_name == "EXISTS") {
        join_type = JOIN_MARK;
    } else if (subquery->sublink_name == "ANY" || subquery->sublink_name == "ARRAY") {
        join_type = JOIN_MARK;
    } else if (subquery->sublink_name == "ALL") {
        join_type = negated ? JOIN_SEMI : JOIN_ANTI;
    } else if (subquery->sublink_name == "ROWCOMPARE" || subquery->sublink_name == "EXPR" ||
               subquery->sublink_name == "MULTIEXPR") {
        join_type = JOIN_SINGLE;
    }

    auto join = std::make_unique<LogicalDependentJoin>(join_type);
    join->dependent = true;
    join->invert_result = negated;
    if (join_type == JOIN_MARK) {
        join->mark_index = adapter.GenerateTableIndex();
        join->has_mark_index = true;
    }
    join->children.push_back(std::move(left));
    join->children.push_back(std::move(right));

    if (!subquery->children.empty()) {
        auto outputs = CollectProducedOutputs(join->children[1].get());
        if (!outputs.empty()) {
            size_t output_index = 0;
            auto condition = RewriteSubqueryParams(std::move(subquery->children[0]), outputs, output_index);
            if (condition) {
                join->conditions.push_back(std::move(condition));
            }
        }
    }

    if (!join->children.empty()) {
        join->estimated_cardinality = join->children[0]->estimated_cardinality;
    }

    std::set<size_t> left_tables;
    CollectOutputTables(join->children[0].get(), left_tables);
    join->correlated_columns = CollectCorrelatedColumnsFromPlan(join->children[1].get(), left_tables);
    if (!join->correlated_columns.empty()) {
        join->perform_delim = true;
        join->any_join = (join_type == JOIN_MARK);
    } else {
        join->dependent = false;
        join->perform_delim = false;
        join->any_join = false;
        join->type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
    }

    return join;
}

std::unique_ptr<Expression> RewriteToProducedBinding(LogicalOperator* input, std::unique_ptr<Expression> expression) {
    if (!input || !expression) {
        return expression;
    }

    if (expression->type == ExpressionType::BOUND_FUNCTION) {
        auto* function = static_cast<BoundFunctionExpression*>(expression.get());
        for (auto& child : function->children) {
            child = RewriteToProducedBinding(input, std::move(child));
        }
    } else if (expression->type == ExpressionType::BOUND_AGGREGATE) {
        auto* aggregate = static_cast<BoundAggregateExpression*>(expression.get());
        for (auto& child : aggregate->children) {
            child = RewriteToProducedBinding(input, std::move(child));
        }
    } else if (expression->type == ExpressionType::BOUND_CONJUNCTION) {
        auto* conjunction = static_cast<BoundConjunctionExpression*>(expression.get());
        for (auto& child : conjunction->children) {
            child = RewriteToProducedBinding(input, std::move(child));
        }
    } else if (expression->type == ExpressionType::BOUND_SUBQUERY) {
        auto* subquery = static_cast<BoundSubqueryExpression*>(expression.get());
        for (auto& child : subquery->children) {
            child = RewriteToProducedBinding(input, std::move(child));
        }
    }

    input = SkipTransparentOperators(input);

    auto fingerprint = ExpressionSemanticKey(expression.get());

    if (input->type == LogicalOperatorType::LOGICAL_PROJECTION) {
        auto* projection = static_cast<LogicalProjection*>(input);
        for (size_t idx = 0; idx < projection->expressions.size(); ++idx) {
            if (ExpressionSemanticKey(projection->expressions[idx].get()) == fingerprint) {
                auto* source = projection->expressions[idx].get();
                return std::make_unique<BoundColumnRefExpression>(
                    ColumnBinding{projection->table_index, ProjectionIndex{idx}},
                    "proj",
                    idx < projection->output_names.size() ? projection->output_names[idx] : DerivedColumnName(source, idx));
            }
        }
    }

    if (input->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
        auto* aggregate = static_cast<LogicalAggregate*>(input);
        for (size_t idx = 0; idx < aggregate->groups.size(); ++idx) {
            if (ExpressionSemanticKey(aggregate->groups[idx].get()) == fingerprint) {
                auto* source = aggregate->groups[idx].get();
                auto* source_ref = static_cast<BoundColumnRefExpression*>(source);
                std::string table_name = source_ref ? source_ref->table_name : "group";
                std::string column_name = idx < aggregate->group_names.size()
                    ? aggregate->group_names[idx]
                    : (source_ref ? source_ref->column_name : DerivedColumnName(source, idx));
                return std::make_unique<BoundColumnRefExpression>(
                    ColumnBinding{aggregate->group_index, ProjectionIndex{idx}},
                    table_name,
                    column_name);
            }
        }
        for (size_t idx = 0; idx < aggregate->expressions.size(); ++idx) {
            if (ExpressionSemanticKey(aggregate->expressions[idx].get()) == fingerprint) {
                auto* source = aggregate->expressions[idx].get();
                std::string table_name = "agg";
                std::string column_name = idx < aggregate->aggregate_names.size()
                    ? aggregate->aggregate_names[idx]
                    : DerivedColumnName(source, idx);
                return std::make_unique<BoundColumnRefExpression>(
                    ColumnBinding{aggregate->aggregate_index, ProjectionIndex{idx}},
                    table_name,
                    column_name);
            }
        }
    }

    if (input->type == LogicalOperatorType::LOGICAL_WINDOW) {
        auto* window = static_cast<LogicalWindow*>(input);
        if (expression->type == ExpressionType::BOUND_FUNCTION) {
            auto* function = static_cast<BoundFunctionExpression*>(expression.get());
            for (size_t idx = 0; idx < window->function_names.size(); ++idx) {
                if (function->children.empty() && window->function_names[idx] == function->function_name) {
                    return std::make_unique<BoundColumnRefExpression>(
                        ColumnBinding{window->table_index, ProjectionIndex{idx}},
                        "window",
                        idx < window->output_names.size() ? window->output_names[idx] : "window" + std::to_string(idx + 1));
                }
            }
        }
    }

    if (input->type == LogicalOperatorType::LOGICAL_SET_OPERATION) {
        auto* setop = static_cast<LogicalSetOperation*>(input);
        if (input->children.size() == 2) {
            auto left_outputs = CollectProducedOutputs(input->children[0].get());
            auto right_outputs = CollectProducedOutputs(input->children[1].get());
            if (expression->type == ExpressionType::BOUND_COLUMN_REF) {
                auto* column = static_cast<BoundColumnRefExpression*>(expression.get());
                for (size_t idx = 0; idx < setop->output_names.size(); ++idx) {
                    bool matches_left = idx < left_outputs.size() &&
                        left_outputs[idx].binding.table_index.index == column->binding.table_index.index &&
                        left_outputs[idx].binding.column_index.index == column->binding.column_index.index;
                    bool matches_right = idx < right_outputs.size() &&
                        right_outputs[idx].binding.table_index.index == column->binding.table_index.index &&
                        right_outputs[idx].binding.column_index.index == column->binding.column_index.index;
                    if (matches_left || matches_right) {
                        return std::make_unique<BoundColumnRefExpression>(
                            ColumnBinding{setop->table_index, ProjectionIndex{idx}},
                            "setop",
                            setop->output_names[idx]);
                    }
                }
            }
            for (size_t idx = 0; idx < left_outputs.size() && idx < setop->output_names.size(); ++idx) {
                auto left_expr = std::make_unique<BoundColumnRefExpression>(
                    left_outputs[idx].binding,
                    "setop_child",
                    left_outputs[idx].name);
                if (ExpressionSemanticKey(left_expr.get()) == fingerprint) {
                    return std::make_unique<BoundColumnRefExpression>(
                        ColumnBinding{setop->table_index, ProjectionIndex{idx}},
                        "setop",
                        setop->output_names[idx]);
                }
                if (idx < right_outputs.size()) {
                    auto right_expr = std::make_unique<BoundColumnRefExpression>(
                        right_outputs[idx].binding,
                        "setop_child",
                        right_outputs[idx].name);
                    if (ExpressionSemanticKey(right_expr.get()) == fingerprint) {
                        return std::make_unique<BoundColumnRefExpression>(
                            ColumnBinding{setop->table_index, ProjectionIndex{idx}},
                            "setop",
                            setop->output_names[idx]);
                    }
                }
            }
        }
    }

	if (auto produced = TryRewriteToProducedOutput(input, expression.get(), fingerprint)) {
		return produced;
	}

    return expression;
}

} // namespace

YaapAdapter::YaapAdapter(YaapAdapter* parent)
    : parent_(parent) {}

::RangeTblEntry* YaapAdapter::GetRte(int rtindex) {
    if (rtindex <= 0 || rtable_ == nullptr || rtindex > list_length(rtable_)) {
        throw std::runtime_error("Var references invalid range table index");
    }
    return (::RangeTblEntry*)list_nth(rtable_, rtindex - 1);
}

::RangeTblEntry* YaapAdapter::GetRte(::Var* var) {
    if (!var) {
        throw std::runtime_error("Null Var in GetRte");
    }
    if (var->varlevelsup == 0) {
        return GetRte(var->varno);
    }
    if (!parent_) {
        throw std::runtime_error("Outer query reference without parent adapter");
    }
    Var outer = *var;
    outer.varlevelsup -= 1;
    return parent_->GetRte(&outer);
}

std::string YaapAdapter::GetRteName(int rtindex) {
    RangeTblEntry* rte = GetRte(rtindex);
    if (rte->eref && rte->eref->aliasname) {
        return rte->eref->aliasname;
    }
    return "rt" + std::to_string(rtindex);
}

std::string YaapAdapter::GetRteName(::Var* var) {
    RangeTblEntry* rte = GetRte(var);
    if (rte->eref && rte->eref->aliasname) {
        return rte->eref->aliasname;
    }
    return "rt" + std::to_string(var->varno);
}

std::string YaapAdapter::GetColumnName(::Var* var) {
    if (var->varattno <= 0) {
        return "ctid";
    }

    RangeTblEntry* rte = GetRte(var);
    if (rte->eref && rte->eref->colnames && var->varattno <= list_length(rte->eref->colnames)) {
        Node* column_name = (Node*)list_nth(rte->eref->colnames, var->varattno - 1);
        if (column_name != nullptr) {
            return strVal(column_name);
        }
    }

    return "col" + std::to_string(var->varattno);
}

TableIndex YaapAdapter::GenerateTableIndex() {
    return TableIndex{next_table_index_++};
}

ColumnBinding YaapAdapter::BindBaseColumn(::Var* var) {
    PGBindingKey key{static_cast<int>(var->varlevelsup), var->varno, var->varattno};
    for (const auto& entry : pg_bindings_) {
        const auto& existing_key = entry.first;
        if (existing_key.varlevelsup == key.varlevelsup &&
            existing_key.varno == key.varno &&
            existing_key.varattno == key.varattno) {
            return entry.second;
        }
    }

    if (var->varlevelsup > 0 && parent_) {
        Var outer = *var;
        outer.varlevelsup -= 1;
        return parent_->BindBaseColumn(&outer);
    }

    throw std::runtime_error("Var references a range table entry before it is bound");
}

ColumnBinding YaapAdapter::GetBaseColumnBinding(::Var* var) {
    return BindBaseColumn(var);
}

size_t YaapAdapter::EstimateBaseCardinality(::RangeTblEntry* rte) {
    if (rte->rtekind != RTE_RELATION || !OidIsValid(rte->relid)) {
        return 0;
    }

    Relation rel = relation_open(rte->relid, NoLock);
    if (rel == nullptr) {
        return 0;
    }

    BlockNumber pages = 0;
    double reltuples = 0;
    double allvisfrac = 0;
    estimate_rel_size(rel, nullptr, &pages, &reltuples, &allvisfrac);
    relation_close(rel, NoLock);

    if (reltuples <= 0) {
        return 0;
    }
    return std::max<size_t>(1, static_cast<size_t>(std::ceil(reltuples)));
}

void YaapAdapter::RegisterOutputBindings(int rtindex, ::RangeTblEntry* rte, LogicalOperator* plan) {
    if (!rte || !plan) {
        return;
    }

    auto outputs = CollectProducedOutputs(plan);
    if (outputs.empty()) {
        if (plan->type == LogicalOperatorType::LOGICAL_PROJECTION) {
            outputs = ProducedOutputsFromProjection(static_cast<LogicalProjection*>(SkipTransparentOperators(plan)));
        } else if (plan->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
            outputs = ProducedOutputsFromAggregate(static_cast<LogicalAggregate*>(SkipTransparentOperators(plan)));
        } else if (plan->type == LogicalOperatorType::LOGICAL_WINDOW) {
            outputs = ProducedOutputsFromWindow(static_cast<LogicalWindow*>(SkipTransparentOperators(plan)));
        }
    }

    if (outputs.empty() && rte->eref && rte->eref->colnames) {
        LogicalOperator* producer = SkipTransparentOperators(plan);
        if (producer && producer->type == LogicalOperatorType::LOGICAL_GET) {
            auto* get = static_cast<LogicalGet*>(producer);
            size_t column_count = list_length(rte->eref->colnames);
            for (size_t idx = 0; idx < column_count; ++idx) {
                Node* column_name = (Node*)list_nth(rte->eref->colnames, idx);
                std::string name = column_name ? strVal(column_name) : "col" + std::to_string(idx + 1);
                pg_bindings_.push_back({
                    PGBindingKey{0, rtindex, static_cast<int>(idx + 1)},
                    ColumnBinding{get->table_index, ProjectionIndex{idx}}
                });
            }
            return;
        }
    }

    if (!outputs.empty()) {
        for (size_t idx = 0; idx < outputs.size(); ++idx) {
            pg_bindings_.push_back({
                PGBindingKey{0, rtindex, static_cast<int>(idx + 1)},
                outputs[idx].binding
            });
        }
    }
}

void YaapAdapter::PropagateUnaryCardinality(LogicalOperator& op) {
    if (op.children.size() == 1) {
        op.estimated_cardinality = op.children[0]->estimated_cardinality;
    }
}

void YaapAdapter::PropagateCrossProductCardinality(LogicalCrossProduct& op) {
    if (op.children.size() != 2) {
        return;
    }
    size_t left = op.children[0]->estimated_cardinality;
    size_t right = op.children[1]->estimated_cardinality;
    if (left == 0 || right == 0) {
        op.estimated_cardinality = 0;
        return;
    }
    op.estimated_cardinality = left * right;
}

std::unique_ptr<LogicalOperator> YaapAdapter::TranslateFromExprList(::List* fromlist) {
    if (fromlist == nullptr || list_length(fromlist) == 0) return nullptr;
    
    // Bottom-up: if multiple tables, we build a left-deep cross product tree
    ListCell* lc;
    std::unique_ptr<LogicalOperator> left_child = nullptr;

    foreach(lc, fromlist) {
        ::Node* node = (::Node*)lfirst(lc);
        auto child = TranslateFromNode(node);
        if (!child) return nullptr;

        if (!left_child) {
            left_child = std::move(child);
        } else {
            auto cross_product = std::make_unique<LogicalCrossProduct>();
            cross_product->children.push_back(std::move(left_child));
            cross_product->children.push_back(std::move(child));
            PropagateCrossProductCardinality(*cross_product);
            left_child = std::move(cross_product);
        }
    }
    return left_child;
}

std::unique_ptr<LogicalOperator> YaapAdapter::TranslateFromNode(::Node* pg_node) {
    if (!pg_node) return nullptr;

    if (IsA(pg_node, RangeTblRef)) {
        RangeTblRef* rtref = (RangeTblRef*)pg_node;
        RangeTblEntry* rte = GetRte(rtref->rtindex);
        if (rte->rtekind == RTE_RELATION) {
            TableIndex table_index = GenerateTableIndex();
            auto get = std::make_unique<LogicalGet>(
                table_index,
                rtref->rtindex,
                static_cast<unsigned int>(rte->relid),
                GetRteName(rtref->rtindex));
            if (rte->eref && rte->eref->colnames) {
                const size_t column_count = list_length(rte->eref->colnames);
                get->output_names.reserve(column_count);
				get->output_types.reserve(column_count);
				Relation rel = OidIsValid(rte->relid) ? relation_open(rte->relid, NoLock) : nullptr;
				TupleDesc tupdesc = rel != nullptr ? RelationGetDescr(rel) : nullptr;
                for (size_t idx = 0; idx < column_count; ++idx) {
                    Node *column_name = (Node *) list_nth(rte->eref->colnames, idx);
                    get->output_names.push_back(column_name ? strVal(column_name) : "col" + std::to_string(idx + 1));
					ColumnTypeInfo type_info{};
					if (tupdesc != nullptr && idx < static_cast<size_t>(tupdesc->natts))
					{
						Form_pg_attribute attr = TupleDescAttr(tupdesc, static_cast<int>(idx));
						if (!attr->attisdropped)
						{
							type_info.type_oid = static_cast<uint32_t>(attr->atttypid);
							type_info.typmod = attr->atttypmod;
						}
					}
					get->output_types.push_back(type_info);
                }
				if (rel != nullptr)
					relation_close(rel, NoLock);
            }
            get->estimated_cardinality = EstimateBaseCardinality(rte);
            RegisterOutputBindings(rtref->rtindex, rte, get.get());
            return get;
        }

        if (rte->rtekind == RTE_SUBQUERY) {
            if (!rte->subquery || !IsA(rte->subquery, Query)) {
                throw std::runtime_error("Unsupported subquery RTE");
            }
            YaapAdapter child(this);
            child.next_table_index_ = next_table_index_;
            child.cte_list_ = cte_list_;
            auto subquery = child.TranslatePGQuery((::Query*)rte->subquery);
            if (!subquery) {
                throw std::runtime_error("Failed to translate subquery RTE");
            }
            next_table_index_ = child.next_table_index_;
            RegisterOutputBindings(rtref->rtindex, rte, subquery.get());
            return subquery;
        }

        if (rte->rtekind == RTE_CTE) {
            CommonTableExpr* cte = nullptr;
            for (auto* entry : cte_list_) {
                if (entry && entry->ctename && rte->ctename && strcmp(entry->ctename, rte->ctename) == 0) {
                    cte = entry;
                    break;
                }
            }
            if (!cte || !cte->ctequery || !IsA(cte->ctequery, Query)) {
                throw std::runtime_error("Unsupported CTE RTE");
            }
            YaapAdapter child(this);
            child.next_table_index_ = next_table_index_;
            child.cte_list_ = cte_list_;
            auto cte_plan = child.TranslatePGQuery((::Query*)cte->ctequery);
            if (!cte_plan) {
                throw std::runtime_error("Failed to translate CTE RTE");
            }
            next_table_index_ = child.next_table_index_;
            RegisterOutputBindings(rtref->rtindex, rte, cte_plan.get());
            return cte_plan;
        }

        throw std::runtime_error("Unsupported range table entry kind");
    }
    
    if (IsA(pg_node, JoinExpr)) {
        JoinExpr* jexpr = (JoinExpr*)pg_node;
        
        auto left_child = TranslateFromNode(jexpr->larg);
        auto right_child = TranslateFromNode(jexpr->rarg);

        if (!left_child || !right_child) {
            throw std::runtime_error("Failed to translate children of JoinExpr");
        }

        auto join_op = std::make_unique<LogicalComparisonJoin>(MapPGJoinType(jexpr->jointype));
        join_op->children.push_back(std::move(left_child));
        join_op->children.push_back(std::move(right_child));
        if (join_op->children[0]->estimated_cardinality != 0 &&
            join_op->children[1]->estimated_cardinality != 0) {
            join_op->estimated_cardinality =
                join_op->children[0]->estimated_cardinality * join_op->children[1]->estimated_cardinality;
        }

        // Translate the ON condition
        if (jexpr->quals != nullptr) {
            auto join_cond = TranslateExpression(jexpr->quals);
            if (!join_cond) {
                throw std::runtime_error("Failed to translate JoinExpr quals");
            }
            join_op->conditions.push_back(std::move(join_cond));
        }

        return std::move(join_op);
    }

    throw std::runtime_error(std::string("Unsupported FROM node type: ") + std::to_string(nodeTag(pg_node)));
}

std::unique_ptr<LogicalOperator> YaapAdapter::TranslateSetOperation(::Query* pg_query, ::Node* setop_node) {
    if (!setop_node) {
        throw std::runtime_error("Set operation query missing setOperations tree");
    }

    if (IsA(setop_node, RangeTblRef)) {
        return TranslateFromNode(setop_node);
    }

    if (!IsA(setop_node, SetOperationStmt)) {
        throw std::runtime_error("Unsupported set operation node type");
    }

    auto* setop_stmt = (SetOperationStmt*)setop_node;
    if (setop_stmt->op != SETOP_UNION || !setop_stmt->all) {
        throw std::runtime_error("Only UNION ALL set operations are supported");
    }

    auto left = TranslateSetOperation(pg_query, setop_stmt->larg);
    auto right = TranslateSetOperation(pg_query, setop_stmt->rarg);
    if (!left || !right) {
        throw std::runtime_error("Failed to translate UNION ALL children");
    }

    auto left_outputs = CollectProducedOutputs(left.get());
    auto right_outputs = CollectProducedOutputs(right.get());
    if (left_outputs.empty() || right_outputs.empty() || left_outputs.size() != right_outputs.size()) {
        throw std::runtime_error("UNION ALL children must expose the same output width");
    }

    auto setop = std::make_unique<LogicalSetOperation>(GenerateTableIndex(), SetOperationType::UNION, true);
    setop->output_names.reserve(left_outputs.size());
    for (const auto& output : left_outputs) {
        setop->output_names.push_back(output.name);
    }
    setop->children.push_back(std::move(left));
    setop->children.push_back(std::move(right));
    size_t left_cardinality = setop->children[0] ? setop->children[0]->estimated_cardinality : 0;
    size_t right_cardinality = setop->children[1] ? setop->children[1]->estimated_cardinality : 0;
    setop->estimated_cardinality = left_cardinality + right_cardinality;
    return setop;
}

std::unique_ptr<Expression> YaapAdapter::TranslateExpression(::Node* pg_expr) {
    if (!pg_expr) return nullptr;

    if (IsA(pg_expr, Var)) {
        Var* var = (Var*)pg_expr;
        RangeTblEntry* rte = GetRte(var);
        if (rte->rtekind == RTE_GROUP && rte->groupexprs != nullptr &&
            var->varattno > 0 && var->varattno <= list_length(rte->groupexprs)) {
            return TranslateExpression((::Node*)list_nth(rte->groupexprs, var->varattno - 1));
        }

        return std::make_unique<BoundColumnRefExpression>(
            GetBaseColumnBinding(var),
            GetRteName(var),
            GetColumnName(var));
    }

    if (IsA(pg_expr, FuncExpr)) {
        FuncExpr* func = (FuncExpr*)pg_expr;
        std::string function_name = get_func_name(func->funcid);
        if (IsPrefixSliceFunc(function_name, func->args)) {
            auto prefix_expr = std::make_unique<BoundFunctionExpression>("prefix_slice", func->funcid);
            auto base_expr = TranslateExpression((::Node *) list_nth(func->args, 0));
            auto len_expr = TranslateExpression((::Node *) list_nth(func->args, 2));
            if (!base_expr || !len_expr) {
                throw std::runtime_error("Failed to translate substring prefix arguments");
            }
            prefix_expr->children.push_back(std::move(base_expr));
            prefix_expr->children.push_back(std::move(len_expr));
            return prefix_expr;
        }
        std::vector<std::unique_ptr<Expression>> translated_args;
        ListCell* lc;
        foreach(lc, func->args) {
            ::Node* arg = (::Node*)lfirst(lc);
            auto child_expr = TranslateExpression(arg);
            if (!child_expr) {
                throw std::runtime_error("Failed to translate FuncExpr argument");
            }
            translated_args.push_back(std::move(child_expr));
        }

        if (translated_args.size() == 1 &&
            translated_args[0]->type == ExpressionType::BOUND_CONSTANT &&
            (function_name == "int8" || function_name == "int4" || function_name == "int2" ||
             function_name == "numeric" || function_name == "float8" || function_name == "float4")) {
            return std::move(translated_args[0]);
        }

        auto duck_expr = std::make_unique<BoundFunctionExpression>(function_name, func->funcid);
        duck_expr->children = std::move(translated_args);
        return duck_expr;
    }

    if (IsA(pg_expr, OpExpr)) {
        OpExpr* op = (OpExpr*)pg_expr;
        auto duck_expr = std::make_unique<BoundFunctionExpression>(get_opname(op->opno), op->opno);
        
        ListCell* lc;
        foreach(lc, op->args) {
            ::Node* arg = (::Node*)lfirst(lc);
            auto child_expr = TranslateExpression(arg);
            if (!child_expr) {
                throw std::runtime_error("Failed to translate OpExpr argument");
            }
            duck_expr->children.push_back(std::move(child_expr));
        }
        return duck_expr;
    }

    if (IsA(pg_expr, DistinctExpr)) {
        DistinctExpr* op = (DistinctExpr*)pg_expr;
        auto duck_expr = std::make_unique<BoundFunctionExpression>("distinct", op->opno);

        ListCell* lc;
        foreach(lc, op->args) {
            ::Node* arg = (::Node*)lfirst(lc);
            auto child_expr = TranslateExpression(arg);
            if (!child_expr) {
                throw std::runtime_error("Failed to translate DistinctExpr argument");
            }
            duck_expr->children.push_back(std::move(child_expr));
        }
        return duck_expr;
    }

    if (IsA(pg_expr, NullIfExpr)) {
        NullIfExpr* op = (NullIfExpr*)pg_expr;
        auto duck_expr = std::make_unique<BoundFunctionExpression>("nullif", op->opno);

        ListCell* lc;
        foreach(lc, op->args) {
            ::Node* arg = (::Node*)lfirst(lc);
            auto child_expr = TranslateExpression(arg);
            if (!child_expr) {
                throw std::runtime_error("Failed to translate NullIfExpr argument");
            }
            duck_expr->children.push_back(std::move(child_expr));
        }
        return duck_expr;
    }

    if (IsA(pg_expr, BoolExpr)) {
        BoolExpr* b = (BoolExpr*)pg_expr;
        auto bool_expr = std::make_unique<BoundConjunctionExpression>(b->boolop);
        
        ListCell* lc;
        foreach(lc, b->args) {
            ::Node* arg = (::Node*)lfirst(lc);
            auto child_expr = TranslateExpression(arg);
            if (!child_expr) {
                throw std::runtime_error("Failed to translate BoolExpr argument");
            }
            bool_expr->children.push_back(std::move(child_expr));
        }
        return bool_expr;
    }

    if (IsA(pg_expr, ScalarArrayOpExpr)) {
        ScalarArrayOpExpr* saop = (ScalarArrayOpExpr*)pg_expr;
        std::vector<std::unique_ptr<Expression>> translated_args;
        ListCell* lc;
        foreach(lc, saop->args) {
            ::Node* arg = (::Node*)lfirst(lc);
            auto child_expr = TranslateExpression(arg);
            if (!child_expr) {
                throw std::runtime_error("Failed to translate ScalarArrayOpExpr argument");
            }
            translated_args.push_back(std::move(child_expr));
        }
        if (translated_args.size() == 2) {
            auto rewritten = TryRewriteScalarArrayMembership(saop->useOr,
                                                             saop->opno,
                                                             *translated_args[0],
                                                             *translated_args[1]);
            if (rewritten) {
                return rewritten;
            }
        }
        auto sa_expr = std::make_unique<BoundFunctionExpression>(
            saop->useOr ? "any" : "all",
            saop->opno);
        for (auto &arg : translated_args) {
            sa_expr->children.push_back(std::move(arg));
        }
        return sa_expr;
    }

    if (IsA(pg_expr, NullTest)) {
        NullTest* test = (NullTest*)pg_expr;
        auto null_expr = std::make_unique<BoundFunctionExpression>(
            test->nulltesttype == IS_NULL ? "is_null" : "is_not_null",
            InvalidOid);
        auto child_expr = TranslateExpression((::Node*)test->arg);
        if (!child_expr) {
            throw std::runtime_error("Failed to translate NullTest argument");
        }
        null_expr->children.push_back(std::move(child_expr));
        return null_expr;
    }

    if (IsA(pg_expr, BooleanTest)) {
        BooleanTest* test = (BooleanTest*)pg_expr;
        static const char* names[] = {
            "is_true", "is_not_true", "is_false", "is_not_false", "is_unknown", "is_not_unknown"
        };
        auto bool_test = std::make_unique<BoundFunctionExpression>(
            names[test->booltesttype],
            InvalidOid);
        auto child_expr = TranslateExpression((::Node*)test->arg);
        if (!child_expr) {
            throw std::runtime_error("Failed to translate BooleanTest argument");
        }
        bool_test->children.push_back(std::move(child_expr));
        return bool_test;
    }

    if (IsA(pg_expr, ArrayExpr)) {
        ArrayExpr* array = (ArrayExpr*)pg_expr;
        auto array_expr = std::make_unique<BoundFunctionExpression>("array", InvalidOid);
        ListCell* lc;
        foreach(lc, array->elements) {
            ::Node* element = (::Node*)lfirst(lc);
            auto child_expr = TranslateExpression(element);
            if (!child_expr) {
                throw std::runtime_error("Failed to translate ArrayExpr element");
            }
            array_expr->children.push_back(std::move(child_expr));
        }
        return array_expr;
    }

    if (IsA(pg_expr, RowCompareExpr)) {
        RowCompareExpr* row = (RowCompareExpr*)pg_expr;
        auto row_expr = std::make_unique<BoundFunctionExpression>("row_compare", InvalidOid);
        ListCell* lc;
        foreach(lc, row->largs) {
            auto child_expr = TranslateExpression((::Node*)lfirst(lc));
            if (!child_expr) {
                throw std::runtime_error("Failed to translate RowCompareExpr lhs");
            }
            row_expr->children.push_back(std::move(child_expr));
        }
        foreach(lc, row->rargs) {
            auto child_expr = TranslateExpression((::Node*)lfirst(lc));
            if (!child_expr) {
                throw std::runtime_error("Failed to translate RowCompareExpr rhs");
            }
            row_expr->children.push_back(std::move(child_expr));
        }
        return row_expr;
    }

    if (IsA(pg_expr, RelabelType)) {
        RelabelType* relabel = (RelabelType*)pg_expr;
        return TranslateExpression((::Node*)relabel->arg);
    }

    if (IsA(pg_expr, CoerceViaIO)) {
        CoerceViaIO* coercion = (CoerceViaIO*)pg_expr;
        return TranslateExpression((::Node*)coercion->arg);
    }

    if (IsA(pg_expr, CollateExpr)) {
        CollateExpr* collate = (CollateExpr*)pg_expr;
        return TranslateExpression((::Node*)collate->arg);
    }

    if (IsA(pg_expr, CaseExpr)) {
        CaseExpr* case_expr = (CaseExpr*)pg_expr;
        auto duck_expr = std::make_unique<BoundFunctionExpression>("case", InvalidOid);

        if (case_expr->arg != nullptr) {
            auto arg_expr = TranslateExpression((::Node*)case_expr->arg);
            if (!arg_expr) {
                throw std::runtime_error("Failed to translate CaseExpr base argument");
            }
            duck_expr->children.push_back(std::move(arg_expr));
        }

        ListCell* lc;
        foreach(lc, case_expr->args) {
            CaseWhen* when = (CaseWhen*)lfirst(lc);
            auto when_expr = std::make_unique<BoundFunctionExpression>("when", InvalidOid);

            auto cond_expr = TranslateExpression((::Node*)when->expr);
            if (!cond_expr) {
                throw std::runtime_error("Failed to translate CaseExpr condition");
            }
            when_expr->children.push_back(std::move(cond_expr));

            auto result_expr = TranslateExpression((::Node*)when->result);
            if (!result_expr) {
                throw std::runtime_error("Failed to translate CaseExpr result");
            }
            when_expr->children.push_back(std::move(result_expr));
            duck_expr->children.push_back(std::move(when_expr));
        }

        if (case_expr->defresult != nullptr) {
            auto else_expr = TranslateExpression((::Node*)case_expr->defresult);
            if (!else_expr) {
                throw std::runtime_error("Failed to translate CaseExpr default");
            }
            duck_expr->children.push_back(std::move(else_expr));
        }
        return duck_expr;
    }

    if (IsA(pg_expr, Aggref)) {
        Aggref* agg = (Aggref*)pg_expr;
        auto aggregate = std::make_unique<BoundAggregateExpression>(
            get_func_name(agg->aggfnoid),
            agg->aggfnoid,
            agg->aggdistinct != nullptr);

        ListCell* lc;
        foreach(lc, agg->args) {
            TargetEntry* arg_te = (TargetEntry*)lfirst(lc);
            if (arg_te->resjunk) {
                continue;
            }
            auto child_expr = TranslateExpression((::Node*)arg_te->expr);
            if (!child_expr) {
                throw std::runtime_error("Failed to translate Aggref argument");
            }
            aggregate->children.push_back(std::move(child_expr));
        }

        return aggregate;
    }

    if (IsA(pg_expr, WindowFunc)) {
        WindowFunc* window = (WindowFunc*)pg_expr;
        std::string function_name = get_func_name(window->winfnoid);
        if (!IsSupportedWindowFunctionName(function_name)) {
            throw std::runtime_error("Unsupported window function: " + function_name);
        }
        if (window->args != nullptr && list_length(window->args) != 0) {
            throw std::runtime_error("Window function arguments are not supported");
        }
        return std::make_unique<BoundFunctionExpression>(function_name, window->winfnoid);
    }

    if (IsA(pg_expr, SubLink)) {
        SubLink* sublink = (SubLink*)pg_expr;
        auto subquery_expr = std::make_unique<BoundSubqueryExpression>(
            static_cast<int>(sublink->subLinkType),
            SubLinkName(static_cast<int>(sublink->subLinkType)));

        if (sublink->subselect != nullptr && IsA(sublink->subselect, Query)) {
            YaapAdapter child(this);
            child.next_table_index_ = next_table_index_;
            child.cte_list_ = cte_list_;
            auto plan = child.TranslatePGQuery((::Query*)sublink->subselect);
            if (!plan) {
                throw std::runtime_error("Failed to translate SubLink subquery");
            }
            next_table_index_ = child.next_table_index_;
            subquery_expr->subquery_plan = std::move(plan);
        }

        if (sublink->testexpr != nullptr) {
            auto testexpr = TranslateExpression(sublink->testexpr);
            if (!testexpr) {
                throw std::runtime_error("Failed to translate SubLink test expression");
            }
            subquery_expr->children.push_back(std::move(testexpr));
        }
        return subquery_expr;
    }

    if (IsA(pg_expr, Const)) {
        Const* con = (Const*)pg_expr;
        if (con->constisnull) {
            return std::make_unique<BoundConstantExpression>("NULL", true);
        }

        Oid output_func;
        bool is_varlena;
        getTypeOutputInfo(con->consttype, &output_func, &is_varlena);
        char* value = OidOutputFunctionCall(output_func, con->constvalue);
        return std::make_unique<BoundConstantExpression>(value, false);
    }

    if (IsA(pg_expr, Param)) {
        Param* param = (Param*)pg_expr;
        return std::make_unique<BoundConstantExpression>(
            "param$" + std::to_string(param->paramid), false);
    }

    if (IsA(pg_expr, Integer)) {
        return std::make_unique<BoundConstantExpression>(std::to_string(intVal(pg_expr)), false);
    }

    if (IsA(pg_expr, Float)) {
        return std::make_unique<BoundConstantExpression>(std::to_string(floatVal(pg_expr)), false);
    }

    if (IsA(pg_expr, String)) {
        return std::make_unique<BoundConstantExpression>(strVal(pg_expr), false);
    }

    if (IsA(pg_expr, Boolean)) {
        return std::make_unique<BoundConstantExpression>(boolVal(pg_expr) ? "true" : "false", false);
    }

    if (IsA(pg_expr, TypeCast)) {
        TypeCast* cast = (TypeCast*)pg_expr;
        return TranslateExpression(cast->arg);
    }

    if (IsA(pg_expr, A_Const)) {
        A_Const* con = (A_Const*)pg_expr;
        if (con->isnull) {
            return std::make_unique<BoundConstantExpression>("NULL", true);
        }

        if (IsA(&con->val.node, Integer)) {
            return std::make_unique<BoundConstantExpression>(std::to_string(intVal(&con->val.node)), false);
        }
        if (IsA(&con->val.node, Float)) {
            return std::make_unique<BoundConstantExpression>(strVal(&con->val.node), false);
        }
        if (IsA(&con->val.node, String)) {
            return std::make_unique<BoundConstantExpression>(strVal(&con->val.node), false);
        }
        if (IsA(&con->val.node, Boolean)) {
            return std::make_unique<BoundConstantExpression>(boolVal(&con->val.node) ? "true" : "false", false);
        }
    }

    throw std::runtime_error(std::string("Unsupported expression node type: ") + std::to_string(nodeTag(pg_expr)));
}

std::unique_ptr<LogicalOperator> YaapAdapter::TranslateAggregate(::Query* pg_query,
                                                                   std::unique_ptr<LogicalOperator> input) {
    if (!pg_query->hasAggs && pg_query->groupClause == nullptr) {
        return input;
    }

    auto aggregate = std::make_unique<LogicalAggregate>(GenerateTableIndex(), GenerateTableIndex());

    ListCell* lc;
    foreach(lc, pg_query->groupClause) {
        SortGroupClause* sgc = (SortGroupClause*)lfirst(lc);
        TargetEntry* tle = get_sortgroupref_tle(sgc->tleSortGroupRef, pg_query->targetList);
        if (!tle) {
            throw std::runtime_error("Group reference not found in targetList");
        }
        aggregate->groups.push_back(TranslateExpression((::Node*)tle->expr));
        aggregate->group_names.push_back(
            tle->resname ? tle->resname : DerivedColumnName(aggregate->groups.back().get(), aggregate->group_names.size()));
    }

    auto aggrefs = CollectQueryAggregates(pg_query);
	std::set<std::string> aggregate_fingerprints;
    for (auto *aggref : aggrefs) {
		auto aggregate_expr = TranslateExpression(reinterpret_cast<::Node *>(aggref));
		const std::string fingerprint = ExpressionSemanticKey(aggregate_expr.get());
		if (!aggregate_fingerprints.insert(fingerprint).second) {
			continue;
		}
		aggregate->expressions.push_back(std::move(aggregate_expr));
        aggregate->aggregate_names.push_back(
            DerivedColumnName(aggregate->expressions.back().get(), aggregate->aggregate_names.size()));
    }

    aggregate->children.push_back(std::move(input));
    if (aggregate->groups.empty()) {
        aggregate->estimated_cardinality = 1;
    } else {
        PropagateUnaryCardinality(*aggregate);
    }
    return aggregate;
}

std::unique_ptr<LogicalOperator> YaapAdapter::TranslateHaving(::Query* pg_query,
                                                                std::unique_ptr<LogicalOperator> input) {
    if (pg_query->havingQual == nullptr) {
        return input;
    }

    auto having_filter = std::make_unique<LogicalFilter>();
    auto having_expr = TranslateExpression(pg_query->havingQual);
    if (!having_expr) {
        throw std::runtime_error("Failed to translate HAVING condition");
    }
    PromoteScalarSubqueryExpression(having_expr, input, *this);
    having_expr = RewriteToProducedBinding(input.get(), std::move(having_expr));
    having_filter->expressions.push_back(std::move(having_expr));
    having_filter->children.push_back(std::move(input));
    PropagateUnaryCardinality(*having_filter);
    return having_filter;
}

std::unique_ptr<LogicalOperator> YaapAdapter::TranslateWindow(::Query* pg_query,
                                                                std::unique_ptr<LogicalOperator> input) {
    if (!pg_query->hasWindowFuncs) {
        return input;
    }

    auto window = std::make_unique<LogicalWindow>(GenerateTableIndex());
    Index winref = 0;
    ListCell* lc;

    foreach(lc, pg_query->targetList) {
        TargetEntry* te = (TargetEntry*)lfirst(lc);
        if (te->resjunk || !IsA(te->expr, WindowFunc)) {
            continue;
        }

        auto* window_func = (WindowFunc*)te->expr;
        std::string function_name = get_func_name(window_func->winfnoid);
        if (!IsSupportedWindowFunctionName(function_name)) {
            throw std::runtime_error("Unsupported window function: " + function_name);
        }
        if (window_func->args != nullptr && list_length(window_func->args) != 0) {
            throw std::runtime_error("Window function arguments are not supported");
        }

        if (winref == 0) {
            winref = window_func->winref;
        } else if (winref != window_func->winref) {
            throw std::runtime_error("Multiple window specifications are not supported");
        }

        window->function_names.push_back(function_name);
        window->output_names.push_back(
            te->resname ? te->resname : ("window" + std::to_string(window->output_names.size() + 1)));
    }

    if (window->function_names.empty()) {
        return input;
    }

    auto* window_clause = FindWindowClause(pg_query, winref);
    if (!window_clause) {
        throw std::runtime_error("Window clause not found");
    }

    foreach(lc, window_clause->partitionClause) {
        auto* sgc = (SortGroupClause*)lfirst(lc);
        auto* tle = get_sortgroupref_tle(sgc->tleSortGroupRef, pg_query->targetList);
        if (!tle) {
            throw std::runtime_error("Window partition reference not found in targetList");
        }
        auto partition_expr = TranslateExpression((::Node*)tle->expr);
        if (!partition_expr) {
            throw std::runtime_error("Failed to translate window partition expression");
        }
        PromoteScalarSubqueryExpression(partition_expr, input, *this);
        partition_expr = RewriteToProducedBinding(input.get(), std::move(partition_expr));
        window->partitions.push_back(std::move(partition_expr));
    }

    foreach(lc, window_clause->orderClause) {
        auto* sgc = (SortGroupClause*)lfirst(lc);
        auto* tle = get_sortgroupref_tle(sgc->tleSortGroupRef, pg_query->targetList);
        if (!tle) {
            throw std::runtime_error("Window order reference not found in targetList");
        }
        OrderByNode order_node;
        order_node.expression = TranslateExpression((::Node*)tle->expr);
        if (!order_node.expression) {
            throw std::runtime_error("Failed to translate window order expression");
        }
        PromoteScalarSubqueryExpression(order_node.expression, input, *this);
        order_node.expression = RewriteToProducedBinding(input.get(), std::move(order_node.expression));
        window->orders.push_back(std::move(order_node));
    }

    window->children.push_back(std::move(input));
    PropagateUnaryCardinality(*window);
    return window;
}

std::unique_ptr<LogicalOperator> YaapAdapter::TranslateProjection(::Query* pg_query,
                                                                    std::unique_ptr<LogicalOperator> input) {
    if (pg_query->targetList == nullptr) {
        return input;
    }

    auto projection = std::make_unique<LogicalProjection>(GenerateTableIndex());
    ListCell* lc;
    foreach(lc, pg_query->targetList) {
        TargetEntry* te = (TargetEntry*)lfirst(lc);
        if (te->resjunk) {
            continue;
        }

        auto proj_expr = TranslateExpression((::Node*)te->expr);
        if (!proj_expr) {
            throw std::runtime_error("Failed to translate projection expression");
        }
        PromoteScalarSubqueryExpression(proj_expr, input, *this);
        proj_expr = RewriteToProducedBinding(input.get(), std::move(proj_expr));
        projection->expressions.push_back(std::move(proj_expr));
        projection->output_names.push_back(
            te->resname ? te->resname : DerivedColumnName(projection->expressions.back().get(), projection->output_names.size()));
    }

    projection->children.push_back(std::move(input));
    PropagateUnaryCardinality(*projection);
    return projection;
}

std::unique_ptr<LogicalOperator> YaapAdapter::TranslateDistinct(::Query* pg_query,
                                                                  std::unique_ptr<LogicalOperator> input) {
    if (pg_query->distinctClause == nullptr) {
        return input;
    }
    if (pg_query->hasDistinctOn) {
        throw std::runtime_error("DISTINCT ON is not supported");
    }

    auto distinct = std::make_unique<LogicalDistinct>();
    ListCell* lc;
    foreach(lc, pg_query->distinctClause) {
        SortGroupClause* sgc = (SortGroupClause*)lfirst(lc);
        TargetEntry* tle = get_sortgroupref_tle(sgc->tleSortGroupRef, pg_query->targetList);
        if (!tle) {
            throw std::runtime_error("Distinct reference not found in targetList");
        }

        auto distinct_expr = TranslateExpression((::Node*)tle->expr);
        if (!distinct_expr) {
            throw std::runtime_error("Failed to translate DISTINCT expression");
        }
        PromoteScalarSubqueryExpression(distinct_expr, input, *this);
        distinct_expr = RewriteToProducedBinding(input.get(), std::move(distinct_expr));
        distinct->expressions.push_back(std::move(distinct_expr));
    }

    distinct->children.push_back(std::move(input));
    PropagateUnaryCardinality(*distinct);
    return distinct;
}

std::unique_ptr<LogicalOperator> YaapAdapter::TranslateOrder(::Query* pg_query,
                                                               std::unique_ptr<LogicalOperator> input) {
    if (pg_query->sortClause == nullptr) {
        return input;
    }

    auto order_op = std::make_unique<LogicalOrder>();

    ListCell* lc;
    foreach(lc, pg_query->sortClause) {
        SortGroupClause* sgc = (SortGroupClause*)lfirst(lc);
        TargetEntry* tle = get_sortgroupref_tle(sgc->tleSortGroupRef, pg_query->targetList);
        if (!tle) {
            throw std::runtime_error("Sort reference not found in targetList");
        }

        OrderByNode order_node;
        if (!TryResolveTargetListBinding(pg_query, tle, input.get(), order_node.expression)) {
            order_node.expression = TranslateExpression((::Node*)tle->expr);
            if (!order_node.expression) {
                throw std::runtime_error("Failed to translate order by expression");
            }
            PromoteScalarSubqueryExpression(order_node.expression, input, *this);
            order_node.expression = RewriteToProducedBinding(input.get(), std::move(order_node.expression));
        }
        order_op->orders.push_back(std::move(order_node));
    }

    order_op->children.push_back(std::move(input));
    PropagateUnaryCardinality(*order_op);
    return order_op;
}

std::unique_ptr<LogicalOperator> YaapAdapter::TranslateLimit(::Query* pg_query,
                                                               std::unique_ptr<LogicalOperator> input) {
    if (pg_query->limitCount == nullptr && pg_query->limitOffset == nullptr) {
        return input;
    }

    auto limit = std::make_unique<LogicalLimit>();
    if (pg_query->limitCount != nullptr) {
        limit->limit_count = TranslateExpression((::Node*)pg_query->limitCount);
        if (!limit->limit_count) {
            throw std::runtime_error("Failed to translate LIMIT count");
        }
    }
    if (pg_query->limitOffset != nullptr) {
        limit->limit_offset = TranslateExpression((::Node*)pg_query->limitOffset);
        if (!limit->limit_offset) {
            throw std::runtime_error("Failed to translate LIMIT offset");
        }
    }

    limit->children.push_back(std::move(input));
    PropagateUnaryCardinality(*limit);
    return limit;
}

std::unique_ptr<LogicalOperator> YaapAdapter::TranslatePGQuery(::Query* pg_query) {
    rtable_ = pg_query->rtable;
    cte_list_.clear();
    ListCell* cte_lc;
    foreach(cte_lc, pg_query->cteList) {
        cte_list_.push_back((CommonTableExpr*)lfirst(cte_lc));
    }

    if (pg_query->commandType != CMD_SELECT) {
        throw std::runtime_error("Only SELECT is supported");
    }
    if (!pg_query->jointree) {
        throw std::runtime_error("Query missing jointree");
    }
    
    std::unique_ptr<LogicalOperator> plan;
    if (pg_query->setOperations != nullptr) {
        plan = TranslateSetOperation(pg_query, pg_query->setOperations);
    } else {
        plan = TranslateFromExprList(pg_query->jointree->fromlist);
        if (!plan) {
             throw std::runtime_error("Failed to translate FROM list (e.g., empty FROM)");
        }
    }

    // 2. Translate WHERE
    if (pg_query->jointree->quals != nullptr) {
        auto filter_expr = TranslateExpression(pg_query->jointree->quals);
        if (!filter_expr) {
             throw std::runtime_error("Failed to translate WHERE condition");
        }
        std::vector<std::unique_ptr<Expression>> where_filters;
        if (filter_expr->type == ExpressionType::BOUND_CONJUNCTION &&
            static_cast<BoundConjunctionExpression*>(filter_expr.get())->bool_expr_type == 0) {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(filter_expr.get());
            for (auto& child : conjunction->children) {
                where_filters.push_back(std::move(child));
            }
        } else {
            where_filters.push_back(std::move(filter_expr));
        }

        std::vector<std::unique_ptr<Expression>> remaining_filters;
        for (auto& where_expr : where_filters) {
            auto* top_level_subquery = ExtractTopLevelSubquery(where_expr.get());
            if (top_level_subquery) {
                bool negated = IsNotConjunction(where_expr.get());
                plan = BuildDependentJoin(*this, std::move(plan), top_level_subquery, negated);
                continue;
            }
            PromoteScalarSubqueryExpression(where_expr, plan, *this);
            remaining_filters.push_back(std::move(where_expr));
        }

        if (!remaining_filters.empty()) {
            auto where_filter = std::make_unique<LogicalFilter>();
            where_filter->expressions = std::move(remaining_filters);
            where_filter->children.push_back(std::move(plan));
            PropagateUnaryCardinality(*where_filter);
            plan = std::move(where_filter);
        }
    }

    plan = TranslateAggregate(pg_query, std::move(plan));
    plan = TranslateHaving(pg_query, std::move(plan));
    plan = TranslateWindow(pg_query, std::move(plan));
    plan = TranslateProjection(pg_query, std::move(plan));
    plan = TranslateDistinct(pg_query, std::move(plan));
    plan = TranslateOrder(pg_query, std::move(plan));
    plan = TranslateLimit(pg_query, std::move(plan));

    return plan;
}

} // namespace yaap
