#include "physical_plan_generator.hpp"

#include <stdexcept>
#include <sstream>
#include <string_view>

namespace yaap {

namespace {

using OutputColumn = PhysicalOperator::OutputColumn;

struct AggregateRewriteCandidate {
	std::string semantic_key;
	std::unique_ptr<Expression> expression;
};

std::string ExprTableName(Expression *expr, const std::string &fallback) {
    auto *col = dynamic_cast<BoundColumnRefExpression *>(expr);
    return (col != nullptr && !col->table_name.empty()) ? col->table_name : fallback;
}

std::string ExprColumnName(Expression *expr, const std::string &fallback) {
    auto *col = dynamic_cast<BoundColumnRefExpression *>(expr);
    return (col != nullptr && !col->column_name.empty()) ? col->column_name : fallback;
}

void CopyOutputs(PhysicalOperator &target, const PhysicalOperator &source) {
    target.outputs = source.outputs;
}

void AppendOutputs(PhysicalOperator &target, const PhysicalOperator &source) {
    target.outputs.insert(target.outputs.end(), source.outputs.begin(), source.outputs.end());
}

bool OutputBindingVisible(const ColumnBinding& binding,
						  const std::vector<OutputColumn>& visible_outputs) {
	for (const auto& output : visible_outputs) {
		if (output.binding.table_index.index == binding.table_index.index &&
			output.binding.column_index.index == binding.column_index.index) {
			return true;
		}
	}
	return false;
}

std::string_view BaseColumnName(const std::string& name) {
	std::string_view view{name};
	const size_t dot = view.rfind('.');
	if (dot != std::string_view::npos && dot + 1 < view.size()) {
		view.remove_prefix(dot + 1);
	}
	return view;
}

bool SameVisibleColumnName(const BoundColumnRefExpression& left, const BoundColumnRefExpression& right) {
	const auto left_name = BaseColumnName(left.column_name);
	const auto right_name = BaseColumnName(right.column_name);
	if (!left_name.empty() && !right_name.empty()) {
		return left_name == right_name;
	}
	return left.binding.column_index.index == right.binding.column_index.index;
}

bool IsTransparentCastFunctionName(const std::string& function_name) {
	return function_name == "text" ||
		   function_name == "varchar" ||
		   function_name == "bpchar" ||
		   function_name == "char" ||
		   function_name == "int8" ||
		   function_name == "int4" ||
		   function_name == "int2" ||
		   function_name == "numeric" ||
		   function_name == "float8" ||
		   function_name == "float4";
}

const Expression* UnwrapTransparentCastExpr(const Expression* expression) {
	while (true) {
		auto* function = dynamic_cast<const BoundFunctionExpression*>(expression);
		if (function == nullptr || function->children.size() != 1 ||
			!IsTransparentCastFunctionName(function->function_name)) {
			return expression;
		}
		expression = function->children[0].get();
	}
}

bool BindingEquals(const ColumnBinding& left, const ColumnBinding& right) {
	return left.table_index.index == right.table_index.index &&
		   left.column_index.index == right.column_index.index;
}

bool IsCorrelatedBinding(const ColumnBinding& binding, const std::vector<ColumnBinding>& correlated_columns) {
	for (const auto& correlated : correlated_columns) {
		if (BindingEquals(binding, correlated)) {
			return true;
		}
	}
	return false;
}

bool IsRedundantPhysicalDelimCorrelationEquality(const Expression* expression,
												 const PhysicalOperator& left,
												 const PhysicalOperator& right,
												 const std::vector<ColumnBinding>& correlated_columns) {
	if (expression == nullptr || expression->type != ExpressionType::BOUND_FUNCTION) {
		return false;
	}
	auto* function = static_cast<const BoundFunctionExpression*>(expression);
	if (function->function_name != "=" || function->children.size() != 2) {
		return false;
	}
	auto* left_column = dynamic_cast<const BoundColumnRefExpression*>(
		UnwrapTransparentCastExpr(function->children[0].get()));
	auto* right_column = dynamic_cast<const BoundColumnRefExpression*>(
		UnwrapTransparentCastExpr(function->children[1].get()));
	if (left_column == nullptr || right_column == nullptr) {
		return false;
	}

	auto hidden_to_delim = [&](const BoundColumnRefExpression& hidden,
							  const BoundColumnRefExpression& visible) {
		return !OutputBindingVisible(hidden.binding, left.outputs) &&
			   !OutputBindingVisible(hidden.binding, right.outputs) &&
			   !IsCorrelatedBinding(hidden.binding, correlated_columns) &&
			   OutputBindingVisible(visible.binding, right.outputs) &&
			   SameVisibleColumnName(hidden, visible);
	};

	const bool left_visible =
		OutputBindingVisible(left_column->binding, left.outputs) ||
		OutputBindingVisible(left_column->binding, right.outputs);
	const bool right_visible =
		OutputBindingVisible(right_column->binding, left.outputs) ||
		OutputBindingVisible(right_column->binding, right.outputs);
	if (!left_visible && !right_visible &&
		!IsCorrelatedBinding(left_column->binding, correlated_columns) &&
		!IsCorrelatedBinding(right_column->binding, correlated_columns)) {
		return true;
	}

	return hidden_to_delim(*left_column, *right_column) ||
		   hidden_to_delim(*right_column, *left_column);
}

std::vector<Expression*> BorrowJoinConditionsForPhysical(
	const LogicalComparisonJoin& logical_join,
	const PhysicalOperator& left,
	const PhysicalOperator& right) {
	const bool may_have_delim_correlation =
		logical_join.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
		logical_join.type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
		logical_join.dependent;
	std::vector<Expression*> result;
	result.reserve(logical_join.conditions.size());
	std::vector<ColumnBinding> correlated_columns;
	if (logical_join.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
		logical_join.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		const auto& dependent_join = static_cast<const LogicalDependentJoin&>(logical_join);
		correlated_columns = dependent_join.correlated_columns;
	}
	for (size_t idx = 0; idx < logical_join.conditions.size(); ++idx) {
		const auto& condition = logical_join.conditions[idx];
		if (may_have_delim_correlation &&
			IsRedundantPhysicalDelimCorrelationEquality(condition.get(), left, right, correlated_columns)) {
			continue;
		}
		result.push_back(condition.get());
	}
	return result;
}

void BindDelimScansToOuter(PhysicalOperator* op,
						   const PhysicalOperator* outer_child,
						   const std::vector<ColumnBinding>& outer_bindings) {
	if (op == nullptr) {
		return;
	}
	if (op->type == PhysicalOperatorType::DELIM_SCAN) {
		auto* scan = static_cast<PhysicalDelimScan*>(op);
		scan->delim_outer_child = outer_child;
		scan->delim_outer_bindings = outer_bindings;
	}
	for (auto& child : op->children) {
		BindDelimScansToOuter(child.get(), outer_child, outer_bindings);
	}
}

std::string ExpressionSemanticKey(const Expression* expression) {
	if (expression == nullptr) {
		return "<null>";
	}

	std::stringstream ss;
	switch (expression->type) {
		case ExpressionType::BOUND_COLUMN_REF: {
			auto* column = static_cast<const BoundColumnRefExpression*>(expression);
			ss << "col:" << column->binding.table_index.index << "." << column->binding.column_index.index;
			break;
		}
		case ExpressionType::BOUND_CONSTANT: {
			auto* constant = static_cast<const BoundConstantExpression*>(expression);
			ss << "const:" << (constant->is_null ? "NULL" : constant->value);
			break;
		}
		case ExpressionType::BOUND_FUNCTION: {
			auto* function = static_cast<const BoundFunctionExpression*>(expression);
			ss << "fn:" << function->function_name << ":" << function->op_oid << "(";
			for (size_t idx = 0; idx < function->children.size(); ++idx) {
				if (idx > 0) {
					ss << ",";
				}
				ss << ExpressionSemanticKey(function->children[idx].get());
			}
			ss << ")";
			break;
		}
		case ExpressionType::BOUND_AGGREGATE: {
			auto* aggregate = static_cast<const BoundAggregateExpression*>(expression);
			ss << "agg:" << aggregate->function_name << ":" << aggregate->agg_oid << ":"
			   << (aggregate->is_distinct ? "distinct" : "all") << "(";
			for (size_t idx = 0; idx < aggregate->children.size(); ++idx) {
				if (idx > 0) {
					ss << ",";
				}
				ss << ExpressionSemanticKey(aggregate->children[idx].get());
			}
			ss << ")";
			break;
		}
		case ExpressionType::BOUND_CONJUNCTION: {
			auto* conjunction = static_cast<const BoundConjunctionExpression*>(expression);
			ss << "conj:" << conjunction->bool_expr_type << "(";
			for (size_t idx = 0; idx < conjunction->children.size(); ++idx) {
				if (idx > 0) {
					ss << ",";
				}
				ss << ExpressionSemanticKey(conjunction->children[idx].get());
			}
			ss << ")";
			break;
		}
		case ExpressionType::BOUND_SUBQUERY: {
			auto* subquery = static_cast<const BoundSubqueryExpression*>(expression);
			ss << "subquery:" << subquery->sublink_name << "(";
			for (size_t idx = 0; idx < subquery->children.size(); ++idx) {
				if (idx > 0) {
					ss << ",";
				}
				ss << ExpressionSemanticKey(subquery->children[idx].get());
			}
			ss << ")";
			break;
		}
		default:
			ss << "opaque:" << static_cast<int>(expression->type);
			break;
	}
	return ss.str();
}

std::unique_ptr<Expression> CloneExpression(const Expression* expression) {
	if (expression == nullptr) {
		return nullptr;
	}
	switch (expression->type) {
		case ExpressionType::BOUND_COLUMN_REF: {
			auto* column = static_cast<const BoundColumnRefExpression*>(expression);
			return std::make_unique<BoundColumnRefExpression>(column->binding, column->table_name, column->column_name);
		}
		case ExpressionType::BOUND_CONSTANT: {
			auto* constant = static_cast<const BoundConstantExpression*>(expression);
			return std::make_unique<BoundConstantExpression>(constant->value, constant->is_null);
		}
		case ExpressionType::BOUND_FUNCTION: {
			auto* function = static_cast<const BoundFunctionExpression*>(expression);
			auto clone = std::make_unique<BoundFunctionExpression>(function->function_name, function->op_oid);
			for (const auto& child : function->children) {
				clone->children.push_back(CloneExpression(child.get()));
			}
			return clone;
		}
		case ExpressionType::BOUND_AGGREGATE: {
			auto* aggregate = static_cast<const BoundAggregateExpression*>(expression);
			auto clone = std::make_unique<BoundAggregateExpression>(
				aggregate->function_name, aggregate->agg_oid, aggregate->is_distinct);
			for (const auto& child : aggregate->children) {
				clone->children.push_back(CloneExpression(child.get()));
			}
			return clone;
		}
		case ExpressionType::BOUND_CONJUNCTION: {
			auto* conjunction = static_cast<const BoundConjunctionExpression*>(expression);
			auto clone = std::make_unique<BoundConjunctionExpression>(conjunction->bool_expr_type);
			for (const auto& child : conjunction->children) {
				clone->children.push_back(CloneExpression(child.get()));
			}
			return clone;
		}
		case ExpressionType::BOUND_SUBQUERY: {
			auto* subquery = static_cast<const BoundSubqueryExpression*>(expression);
			auto clone = std::make_unique<BoundSubqueryExpression>(subquery->sublink_type, subquery->sublink_name);
			for (const auto& child : subquery->children) {
				clone->children.push_back(CloneExpression(child.get()));
			}
			return clone;
		}
		default:
			throw std::runtime_error("CloneExpression missing case for expression type " +
				std::to_string(static_cast<int>(expression->type)));
	}
}

void CollectAggregateProducedColumns(
	const BoundAggregateExpression* aggregate_expr,
	const PhysicalOperator* source_op,
	const std::vector<OutputColumn>& visible_outputs,
	std::vector<std::unique_ptr<Expression>>& matches) {
	if (aggregate_expr == nullptr || source_op == nullptr) {
		return;
	}
	if ((source_op->type == PhysicalOperatorType::FILTER ||
		 source_op->type == PhysicalOperatorType::PROJECTION ||
		 source_op->type == PhysicalOperatorType::ORDER_BY ||
		 source_op->type == PhysicalOperatorType::LIMIT) &&
		source_op->children.size() == 1 &&
		source_op->children[0] != nullptr) {
		CollectAggregateProducedColumns(aggregate_expr, source_op->children[0].get(), visible_outputs, matches);
		return;
	}
	if (source_op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		auto* aggregate = static_cast<const PhysicalHashAggregate*>(source_op);
		const std::string fingerprint = ExpressionSemanticKey(aggregate_expr);
		for (size_t idx = 0; idx < aggregate->expressions.size(); ++idx) {
			if (ExpressionSemanticKey(aggregate->expressions[idx]) != fingerprint) {
				continue;
			}
			const ColumnBinding binding{aggregate->aggregate_index, ProjectionIndex{idx}};
			if (!OutputBindingVisible(binding, visible_outputs)) {
				continue;
			}
			std::string column_name = idx < aggregate->aggregate_names.size()
				? aggregate->aggregate_names[idx]
				: aggregate_expr->function_name;
			matches.push_back(std::make_unique<BoundColumnRefExpression>(
				binding,
				"agg",
				std::move(column_name)));
		}
		return;
	}
	for (const auto& child : source_op->children) {
		CollectAggregateProducedColumns(aggregate_expr, child.get(), visible_outputs, matches);
	}
}

void CollectAggregateProducedColumnsByFunction(
	const BoundAggregateExpression* aggregate_expr,
	const PhysicalOperator* source_op,
	const std::vector<OutputColumn>& visible_outputs,
	std::vector<AggregateRewriteCandidate>& matches) {
	if (aggregate_expr == nullptr || source_op == nullptr) {
		return;
	}
	if ((source_op->type == PhysicalOperatorType::FILTER ||
		 source_op->type == PhysicalOperatorType::PROJECTION ||
		 source_op->type == PhysicalOperatorType::ORDER_BY ||
		 source_op->type == PhysicalOperatorType::LIMIT) &&
		source_op->children.size() == 1 &&
		source_op->children[0] != nullptr) {
		CollectAggregateProducedColumnsByFunction(aggregate_expr, source_op->children[0].get(), visible_outputs, matches);
		return;
	}
	if (source_op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		auto* aggregate = static_cast<const PhysicalHashAggregate*>(source_op);
		for (size_t idx = 0; idx < aggregate->expressions.size(); ++idx) {
			auto* produced = dynamic_cast<const BoundAggregateExpression*>(aggregate->expressions[idx]);
			if (produced == nullptr || produced->function_name != aggregate_expr->function_name) {
				continue;
			}
			const ColumnBinding binding{aggregate->aggregate_index, ProjectionIndex{idx}};
			if (!OutputBindingVisible(binding, visible_outputs)) {
				continue;
			}
			std::string column_name = idx < aggregate->aggregate_names.size()
				? aggregate->aggregate_names[idx]
				: aggregate_expr->function_name;
			matches.push_back(AggregateRewriteCandidate{
				ExpressionSemanticKey(produced),
				std::make_unique<BoundColumnRefExpression>(
					binding,
					"agg",
					std::move(column_name))});
		}
		return;
	}
	for (const auto& child : source_op->children) {
		CollectAggregateProducedColumnsByFunction(aggregate_expr, child.get(), visible_outputs, matches);
	}
}

std::unique_ptr<Expression> RewriteAggregateExprToProducedColumn(
	const BoundAggregateExpression* aggregate_expr,
	const std::vector<const PhysicalOperator*>& sources) {
	std::vector<std::unique_ptr<Expression>> matches;
	for (const PhysicalOperator* source : sources) {
		CollectAggregateProducedColumns(aggregate_expr, source, source->outputs, matches);
	}
	if (matches.size() == 1) {
		return std::move(matches[0]);
	}
	if (!matches.empty()) {
		return nullptr;
	}

	std::vector<AggregateRewriteCandidate> function_matches;
	for (const PhysicalOperator* source : sources) {
		CollectAggregateProducedColumnsByFunction(aggregate_expr, source, source->outputs, function_matches);
	}
	if (function_matches.empty()) {
		return nullptr;
	}
	const std::string& candidate_key = function_matches.front().semantic_key;
	for (const auto& candidate : function_matches) {
		if (candidate.semantic_key != candidate_key) {
			return nullptr;
		}
	}
	return std::move(function_matches.front().expression);
}

std::unique_ptr<Expression> RewriteAggregateRefs(
	const Expression* expression,
	const std::vector<const PhysicalOperator*>& sources,
	bool& changed) {
	if (expression == nullptr) {
		return nullptr;
	}
	switch (expression->type) {
		case ExpressionType::BOUND_AGGREGATE: {
			auto* aggregate = static_cast<const BoundAggregateExpression*>(expression);
			if (auto rewritten = RewriteAggregateExprToProducedColumn(aggregate, sources)) {
				changed = true;
				return rewritten;
			}
			auto clone = std::make_unique<BoundAggregateExpression>(
				aggregate->function_name, aggregate->agg_oid, aggregate->is_distinct);
			bool child_changed = false;
			for (const auto& child : aggregate->children) {
				clone->children.push_back(RewriteAggregateRefs(child.get(), sources, child_changed));
			}
			if (child_changed) {
				changed = true;
				return clone;
			}
			return CloneExpression(expression);
		}
		case ExpressionType::BOUND_FUNCTION: {
			auto* function = static_cast<const BoundFunctionExpression*>(expression);
			auto clone = std::make_unique<BoundFunctionExpression>(function->function_name, function->op_oid);
			bool child_changed = false;
			for (const auto& child : function->children) {
				clone->children.push_back(RewriteAggregateRefs(child.get(), sources, child_changed));
			}
			if (child_changed) {
				changed = true;
				return clone;
			}
			return CloneExpression(expression);
		}
		case ExpressionType::BOUND_CONJUNCTION: {
			auto* conjunction = static_cast<const BoundConjunctionExpression*>(expression);
			auto clone = std::make_unique<BoundConjunctionExpression>(conjunction->bool_expr_type);
			bool child_changed = false;
			for (const auto& child : conjunction->children) {
				clone->children.push_back(RewriteAggregateRefs(child.get(), sources, child_changed));
			}
			if (child_changed) {
				changed = true;
				return clone;
			}
			return CloneExpression(expression);
		}
		default:
			return CloneExpression(expression);
	}
}

void CanonicalizeAggregateRefs(
	PhysicalOperator& owner,
	std::vector<Expression*>& expressions,
	const std::vector<const PhysicalOperator*>& sources) {
	owner.expression_storage.reserve(owner.expression_storage.size() + expressions.size());
	for (Expression*& expression : expressions) {
		bool changed = false;
		auto rewritten = RewriteAggregateRefs(expression, sources, changed);
		if (!changed || !rewritten) {
			continue;
		}
		owner.expression_storage.push_back(std::move(rewritten));
		expression = owner.expression_storage.back().get();
	}
}

bool ExpressionContainsBoundAggregate(const Expression* expression, bool allow_root_aggregate = false) {
	if (expression == nullptr) {
		return false;
	}
	if (expression->type == ExpressionType::BOUND_AGGREGATE) {
		auto* aggregate = static_cast<const BoundAggregateExpression*>(expression);
		for (const auto& child : aggregate->children) {
			if (ExpressionContainsBoundAggregate(child.get(), false)) {
				return true;
			}
		}
		return !allow_root_aggregate;
	}
	if (expression->type == ExpressionType::BOUND_FUNCTION) {
		auto* function = static_cast<const BoundFunctionExpression*>(expression);
		for (const auto& child : function->children) {
			if (ExpressionContainsBoundAggregate(child.get(), false)) {
				return true;
			}
		}
		return false;
	}
	if (expression->type == ExpressionType::BOUND_CONJUNCTION) {
		auto* conjunction = static_cast<const BoundConjunctionExpression*>(expression);
		for (const auto& child : conjunction->children) {
			if (ExpressionContainsBoundAggregate(child.get(), false)) {
				return true;
			}
		}
		return false;
	}
	if (expression->type == ExpressionType::BOUND_SUBQUERY) {
		auto* subquery = static_cast<const BoundSubqueryExpression*>(expression);
		for (const auto& child : subquery->children) {
			if (ExpressionContainsBoundAggregate(child.get(), false)) {
				return true;
			}
		}
	}
	return false;
}

void VerifyNoBoundAggregateExpressions(const std::vector<Expression*>& expressions,
									   const char* context,
									   bool allow_root_aggregate = false) {
	for (const Expression* expression : expressions) {
		if (ExpressionContainsBoundAggregate(expression, allow_root_aggregate)) {
			throw std::runtime_error(std::string("Uncanonicalized aggregate expression remains in ") + context);
		}
	}
}

void FinalizeAggregateCanonicalization(PhysicalOperator& op) {
	for (auto& child : op.children) {
		if (child != nullptr) {
			FinalizeAggregateCanonicalization(*child);
		}
	}

	switch (op.type) {
		case PhysicalOperatorType::TABLE_SCAN: {
			auto& scan = static_cast<PhysicalTableScan&>(op);
			VerifyNoBoundAggregateExpressions(scan.filters, "PhysicalTableScan.filters");
			return;
		}
		case PhysicalOperatorType::PROJECTION: {
			auto& projection = static_cast<PhysicalProjection&>(op);
			if (projection.children.size() == 1 && projection.children[0] != nullptr) {
				CanonicalizeAggregateRefs(op, projection.select_list, {projection.children[0].get()});
			}
			VerifyNoBoundAggregateExpressions(projection.select_list, "PhysicalProjection.select_list");
			return;
		}
		case PhysicalOperatorType::FILTER: {
			auto& filter = static_cast<PhysicalFilter&>(op);
			if (filter.children.size() == 1 && filter.children[0] != nullptr) {
				CanonicalizeAggregateRefs(op, filter.expressions, {filter.children[0].get()});
			}
			VerifyNoBoundAggregateExpressions(filter.expressions, "PhysicalFilter.expressions");
			return;
		}
		case PhysicalOperatorType::DISTINCT: {
			auto& distinct = static_cast<PhysicalDistinct&>(op);
			if (distinct.children.size() == 1 && distinct.children[0] != nullptr) {
				CanonicalizeAggregateRefs(op, distinct.expressions, {distinct.children[0].get()});
			}
			VerifyNoBoundAggregateExpressions(distinct.expressions, "PhysicalDistinct.expressions");
			return;
		}
		case PhysicalOperatorType::LIMIT: {
			auto& limit = static_cast<PhysicalLimit&>(op);
			std::vector<Expression*> limit_exprs;
			if (limit.limit_count != nullptr) {
				limit_exprs.push_back(limit.limit_count);
			}
			if (limit.limit_offset != nullptr) {
				limit_exprs.push_back(limit.limit_offset);
			}
			VerifyNoBoundAggregateExpressions(limit_exprs, "PhysicalLimit");
			return;
		}
		case PhysicalOperatorType::WINDOW: {
			auto& window = static_cast<PhysicalWindow&>(op);
			if (window.children.size() == 1 && window.children[0] != nullptr) {
				CanonicalizeAggregateRefs(op, window.partitions, {window.children[0].get()});
				CanonicalizeAggregateRefs(op, window.orders, {window.children[0].get()});
			}
			VerifyNoBoundAggregateExpressions(window.partitions, "PhysicalWindow.partitions");
			VerifyNoBoundAggregateExpressions(window.orders, "PhysicalWindow.orders");
			return;
		}
		case PhysicalOperatorType::HASH_JOIN: {
			auto& join = static_cast<PhysicalHashJoin&>(op);
			if (join.children.size() == 2 && join.children[0] != nullptr && join.children[1] != nullptr) {
				CanonicalizeAggregateRefs(op, join.conditions, {join.children[0].get(), join.children[1].get()});
			}
			VerifyNoBoundAggregateExpressions(join.conditions, "PhysicalHashJoin.conditions");
			return;
		}
		case PhysicalOperatorType::HASH_GROUP_BY: {
			auto& aggregate = static_cast<PhysicalHashAggregate&>(op);
			VerifyNoBoundAggregateExpressions(aggregate.groups, "PhysicalHashAggregate.groups");
			VerifyNoBoundAggregateExpressions(aggregate.expressions, "PhysicalHashAggregate.expressions", true);
			return;
		}
		case PhysicalOperatorType::ORDER_BY: {
			auto& order = static_cast<PhysicalOrderBy&>(op);
			if (order.children.size() == 1 && order.children[0] != nullptr) {
				CanonicalizeAggregateRefs(op, order.orders, {order.children[0].get()});
			}
			VerifyNoBoundAggregateExpressions(order.orders, "PhysicalOrderBy.orders");
			return;
		}
		case PhysicalOperatorType::SET_OPERATION:
		case PhysicalOperatorType::DELIM_SCAN:
		case PhysicalOperatorType::CROSS_PRODUCT:
			return;
	}
}

bool IsScalarPhysicalNode(const PhysicalOperator* op) {
	if (op == nullptr) {
		return false;
	}
	if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		auto* aggregate = static_cast<const PhysicalHashAggregate*>(op);
		return aggregate->groups.empty();
	}
	if ((op->type == PhysicalOperatorType::FILTER ||
		 op->type == PhysicalOperatorType::PROJECTION ||
		 op->type == PhysicalOperatorType::ORDER_BY ||
		 op->type == PhysicalOperatorType::LIMIT) &&
		op->children.size() == 1) {
		return IsScalarPhysicalNode(op->children[0].get());
	}
	return false;
}

std::unique_ptr<PhysicalHashJoin> MakeInnerJoin(
	std::unique_ptr<PhysicalOperator> left,
	std::unique_ptr<PhysicalOperator> right,
	std::vector<Expression*> conditions,
	size_t estimated_cardinality) {
	auto join = std::make_unique<PhysicalHashJoin>(
		JOIN_INNER,
		std::move(conditions),
		estimated_cardinality);
	join->children.push_back(std::move(left));
	join->children.push_back(std::move(right));
	AppendOutputs(*join, *join->children[0]);
	AppendOutputs(*join, *join->children[1]);
	CanonicalizeAggregateRefs(*join, join->conditions, {join->children[0].get(), join->children[1].get()});
	return join;
}

} // namespace

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::Plan(LogicalOperator& op) {
	auto plan = CreatePlan(op);
	if (plan != nullptr) {
		FinalizeAggregateCanonicalization(*plan);
	}
	return plan;
}

std::vector<Expression*> PhysicalPlanGenerator::BorrowExpressions(
    const std::vector<std::unique_ptr<Expression>>& expressions) {
    std::vector<Expression*> result;
    result.reserve(expressions.size());
    for (const auto& expression : expressions) {
        result.push_back(expression.get());
    }
    return result;
}

size_t PhysicalPlanGenerator::EstimateCardinality(LogicalOperator& op) {
    return op.estimated_cardinality;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOperator& op) {
    switch (op.type) {
        case LogicalOperatorType::LOGICAL_GET:
            return CreatePlan(static_cast<LogicalGet&>(op));
        case LogicalOperatorType::LOGICAL_FILTER:
            return CreatePlan(static_cast<LogicalFilter&>(op));
        case LogicalOperatorType::LOGICAL_LIMIT:
            return CreatePlan(static_cast<LogicalLimit&>(op));
        case LogicalOperatorType::LOGICAL_WINDOW:
            return CreatePlan(static_cast<LogicalWindow&>(op));
        case LogicalOperatorType::LOGICAL_PROJECTION:
            return CreatePlan(static_cast<LogicalProjection&>(op));
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
            return CreatePlan(static_cast<LogicalAggregate&>(op));
        case LogicalOperatorType::LOGICAL_DISTINCT:
            return CreatePlan(static_cast<LogicalDistinct&>(op));
        case LogicalOperatorType::LOGICAL_SET_OPERATION:
            return CreatePlan(static_cast<LogicalSetOperation&>(op));
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
            return CreatePlan(static_cast<LogicalComparisonJoin&>(op));
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
            return CreatePlan(static_cast<LogicalDependentJoin&>(op));
        case LogicalOperatorType::LOGICAL_DELIM_JOIN:
            return CreatePlan(static_cast<LogicalDependentJoin&>(op));
        case LogicalOperatorType::LOGICAL_DELIM_GET:
            return CreatePlan(static_cast<LogicalDelimGet&>(op));
        case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
            return CreatePlan(static_cast<LogicalCrossProduct&>(op));
        case LogicalOperatorType::LOGICAL_ORDER:
            return CreatePlan(static_cast<LogicalOrder&>(op));
        default:
            throw std::runtime_error("Unsupported logical operator for physical planning");
    }
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalGet& op) {
    auto scan = std::make_unique<PhysicalTableScan>(
        op.table_index,
        op.pg_rtindex,
        op.relid,
        op.table_name,
		op.output_types,
        op.projected_columns,
		BorrowExpressions(op.filters),
        EstimateCardinality(op));
    for (auto proj_idx : op.projected_columns) {
        std::string column_name =
            proj_idx.index < op.output_names.size() ? op.output_names[proj_idx.index]
                                                    : "col" + std::to_string(proj_idx.index + 1);
        scan->outputs.push_back(OutputColumn{
            ColumnBinding{op.table_index, proj_idx},
            op.table_name,
			std::move(column_name),
			proj_idx.index < op.output_types.size() ? op.output_types[proj_idx.index].type_oid : 0,
			proj_idx.index < op.output_types.size() ? op.output_types[proj_idx.index].typmod : -1});
    }
	return scan;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalProjection& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalProjection expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
    auto projection = std::make_unique<PhysicalProjection>(op.table_index,
                                                            BorrowExpressions(op.expressions), op.output_names,
                                                            EstimateCardinality(op));
    projection->outputs.reserve(op.expressions.size());
    for (size_t idx = 0; idx < op.expressions.size(); ++idx) {
        std::string column_name =
            idx < op.output_names.size() ? op.output_names[idx]
                                         : ExprColumnName(op.expressions[idx].get(), "col" + std::to_string(idx + 1));
        projection->outputs.push_back(OutputColumn{
            ColumnBinding{op.table_index, ProjectionIndex{idx}},
            ExprTableName(op.expressions[idx].get(), "proj"),
            std::move(column_name)});
    }
    projection->children.push_back(std::move(child));
	CanonicalizeAggregateRefs(*projection, projection->select_list, {projection->children[0].get()});
    return projection;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalFilter& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalFilter expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
	if (child->type == PhysicalOperatorType::CROSS_PRODUCT &&
		child->children.size() == 2 &&
		child->children[0] != nullptr &&
		child->children[1] != nullptr) {
		auto left = std::move(child->children[0]);
		auto right = std::move(child->children[1]);
		return MakeInnerJoin(std::move(left), std::move(right), BorrowExpressions(op.expressions), EstimateCardinality(op));
	}
	if (child->type == PhysicalOperatorType::HASH_JOIN) {
		auto* join = static_cast<PhysicalHashJoin*>(child.get());
		std::vector<Expression*> filters = BorrowExpressions(op.expressions);
		join->conditions.insert(join->conditions.end(), filters.begin(), filters.end());
		CanonicalizeAggregateRefs(*join, join->conditions, {join->children[0].get(), join->children[1].get()});
		join->estimated_cardinality = EstimateCardinality(op);
		return child;
	}
    auto filter = std::make_unique<PhysicalFilter>(BorrowExpressions(op.expressions), EstimateCardinality(op));
    filter->children.push_back(std::move(child));
    CopyOutputs(*filter, *filter->children[0]);
	CanonicalizeAggregateRefs(*filter, filter->expressions, {filter->children[0].get()});
    return filter;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDistinct& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalDistinct expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
    auto distinct = std::make_unique<PhysicalDistinct>(BorrowExpressions(op.expressions), EstimateCardinality(op));
    distinct->children.push_back(std::move(child));
    CopyOutputs(*distinct, *distinct->children[0]);
	CanonicalizeAggregateRefs(*distinct, distinct->expressions, {distinct->children[0].get()});
    return distinct;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSetOperation& op) {
    if (op.children.size() != 2) {
        throw std::runtime_error("LogicalSetOperation expects two children");
    }
    auto left = CreatePlan(*op.children[0]);
    auto right = CreatePlan(*op.children[1]);
    auto setop = std::make_unique<PhysicalSetOperation>(
        op.table_index,
        op.setop_type,
        op.all,
        op.output_names,
        EstimateCardinality(op));
    setop->outputs.reserve(op.output_names.size());
    for (size_t idx = 0; idx < op.output_names.size(); ++idx) {
        setop->outputs.push_back(OutputColumn{
            ColumnBinding{op.table_index, ProjectionIndex{idx}},
            "setop",
            op.output_names[idx]});
    }
    setop->children.push_back(std::move(left));
    setop->children.push_back(std::move(right));
    return setop;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalLimit& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalLimit expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
    auto limit = std::make_unique<PhysicalLimit>(op.limit_count.get(), op.limit_offset.get(), EstimateCardinality(op));
    limit->children.push_back(std::move(child));
    CopyOutputs(*limit, *limit->children[0]);
    return limit;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalWindow& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalWindow expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
    std::vector<Expression*> orders;
    orders.reserve(op.orders.size());
    for (const auto& order : op.orders) {
        orders.push_back(order.expression.get());
    }
    auto window = std::make_unique<PhysicalWindow>(
        op.table_index,
        op.function_names,
        op.output_names,
        BorrowExpressions(op.partitions),
        std::move(orders),
        EstimateCardinality(op));
    if (child) {
        CopyOutputs(*window, *child);
    }
    for (size_t idx = 0; idx < op.function_names.size(); ++idx) {
        std::string column_name =
            idx < op.output_names.size() ? op.output_names[idx] : "window" + std::to_string(idx + 1);
        window->outputs.push_back(OutputColumn{
            ColumnBinding{op.table_index, ProjectionIndex{idx}},
            "window",
            std::move(column_name)});
    }
    window->children.push_back(std::move(child));
    return window;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalComparisonJoin& op) {
    if (op.children.size() != 2) {
        throw std::runtime_error("LogicalComparisonJoin expects two children");
    }
    auto left = CreatePlan(*op.children[0]);
    auto right = CreatePlan(*op.children[1]);
    auto hash_join = std::make_unique<PhysicalHashJoin>(
        op.join_type,
        BorrowJoinConditionsForPhysical(op, *left, *right),
        EstimateCardinality(op));
    hash_join->dependent = op.dependent;
    hash_join->mark_index = op.mark_index;
    hash_join->has_mark_index = op.has_mark_index;
    hash_join->invert_result = op.invert_result;
    hash_join->children.push_back(std::move(left));
    hash_join->children.push_back(std::move(right));
    const bool semi_or_anti = op.join_type == JOIN_SEMI || op.join_type == JOIN_ANTI;
    AppendOutputs(*hash_join, *hash_join->children[0]);
    if (!semi_or_anti) {
        AppendOutputs(*hash_join, *hash_join->children[1]);
    }
    if (op.has_mark_index) {
        hash_join->outputs.push_back(OutputColumn{
            ColumnBinding{op.mark_index, ProjectionIndex{0}},
            "mark",
            "mark"});
    }
	CanonicalizeAggregateRefs(*hash_join, hash_join->conditions, {hash_join->children[0].get(), hash_join->children[1].get()});
    return hash_join;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDependentJoin& op) {
    auto physical = CreatePlan(static_cast<LogicalComparisonJoin&>(op));
    auto* hash_join = static_cast<PhysicalHashJoin*>(physical.get());
    hash_join->correlated_columns = op.correlated_columns;
    hash_join->delim_join =
        (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) ||
        (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN &&
         (op.join_type == JOIN_SEMI || op.join_type == JOIN_ANTI || op.join_type == JOIN_SINGLE));
    hash_join->mark_index = op.mark_index;
    hash_join->has_mark_index = op.has_mark_index;
    hash_join->invert_result = op.invert_result;
	if (hash_join->delim_join &&
		hash_join->children.size() == 2 &&
		hash_join->children[0] != nullptr &&
		hash_join->children[1] != nullptr &&
		!hash_join->correlated_columns.empty()) {
		BindDelimScansToOuter(hash_join->children[1].get(),
							  hash_join->children[0].get(),
							  hash_join->correlated_columns);
	}
    return physical;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDelimGet& op) {
    auto physical = std::make_unique<PhysicalDelimScan>(op.table_index, EstimateCardinality(op));
    physical->correlated_columns = op.correlated_columns;
    physical->output_names = op.output_names;
    physical->outputs.reserve(op.correlated_columns.size());
    for (size_t idx = 0; idx < op.correlated_columns.size(); ++idx) {
        std::string column_name =
            idx < op.output_names.size() ? op.output_names[idx] : "delim" + std::to_string(idx + 1);
        physical->outputs.push_back(OutputColumn{
            ColumnBinding{op.table_index, ProjectionIndex{idx}},
            "delim",
            std::move(column_name)});
    }
    return physical;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCrossProduct& op) {
    if (op.children.size() != 2) {
        throw std::runtime_error("LogicalCrossProduct expects two children");
    }
    auto left = CreatePlan(*op.children[0]);
    auto right = CreatePlan(*op.children[1]);
	if (IsScalarPhysicalNode(left.get()) || IsScalarPhysicalNode(right.get())) {
		return MakeInnerJoin(std::move(left), std::move(right), {}, EstimateCardinality(op));
	}
    auto cross_product = std::make_unique<PhysicalCrossProduct>(EstimateCardinality(op));
    cross_product->children.push_back(std::move(left));
    cross_product->children.push_back(std::move(right));
    AppendOutputs(*cross_product, *cross_product->children[0]);
    AppendOutputs(*cross_product, *cross_product->children[1]);
    return cross_product;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalAggregate& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalAggregate expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
    auto hash_aggregate = std::make_unique<PhysicalHashAggregate>(
        op.group_index,
        op.aggregate_index,
        BorrowExpressions(op.groups),
        BorrowExpressions(op.expressions),
        op.group_names,
        op.aggregate_names,
        EstimateCardinality(op));
    hash_aggregate->outputs.reserve(op.groups.size() + op.expressions.size());
    for (size_t idx = 0; idx < op.groups.size(); ++idx) {
        std::string column_name =
            idx < op.group_names.size() ? op.group_names[idx]
                                        : ExprColumnName(op.groups[idx].get(), "group" + std::to_string(idx + 1));
        hash_aggregate->outputs.push_back(OutputColumn{
            ColumnBinding{op.group_index, ProjectionIndex{idx}},
            ExprTableName(op.groups[idx].get(), "group"),
            std::move(column_name)});
    }
    for (size_t idx = 0; idx < op.expressions.size(); ++idx) {
        std::string column_name =
            idx < op.aggregate_names.size() ? op.aggregate_names[idx] : "agg" + std::to_string(idx + 1);
        hash_aggregate->outputs.push_back(OutputColumn{
            ColumnBinding{op.aggregate_index, ProjectionIndex{idx}},
            "agg",
            std::move(column_name)});
    }
    hash_aggregate->children.push_back(std::move(child));
    return hash_aggregate;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOrder& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalOrder expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
    std::vector<Expression*> orders;
    orders.reserve(op.orders.size());
    for (const auto& order : op.orders) {
        orders.push_back(order.expression.get());
    }
    auto physical_order = std::make_unique<PhysicalOrderBy>(std::move(orders), EstimateCardinality(op));
    physical_order->children.push_back(std::move(child));
    CopyOutputs(*physical_order, *physical_order->children[0]);
	CanonicalizeAggregateRefs(*physical_order, physical_order->orders, {physical_order->children[0].get()});
    return physical_order;
}

} // namespace yaap
