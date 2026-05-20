#include "parallel/pipeline/yaap_pipeline_init_internal.hpp"

namespace pg_yaap::optimizer_translator_detail {

namespace {

bool
ExprContainsBoundAggregate(const Expression *expr, bool allow_root_aggregate = false)
{
	if (expr == nullptr)
		return false;
	if (expr->type == ExpressionType::BOUND_AGGREGATE)
	{
		const auto *agg = static_cast<const BoundAggregateExpression *>(expr);
		for (const auto &child : agg->children)
		{
			if (ExprContainsBoundAggregate(child.get(), false))
				return true;
		}
		return !allow_root_aggregate;
	}
	if (expr->type == ExpressionType::BOUND_FUNCTION)
	{
		const auto *function = static_cast<const BoundFunctionExpression *>(expr);
		for (const auto &child : function->children)
		{
			if (ExprContainsBoundAggregate(child.get(), false))
				return true;
		}
		return false;
	}
	if (expr->type == ExpressionType::BOUND_CONJUNCTION)
	{
		const auto *conjunction = static_cast<const BoundConjunctionExpression *>(expr);
		for (const auto &child : conjunction->children)
		{
			if (ExprContainsBoundAggregate(child.get(), false))
				return true;
		}
		return false;
	}
	if (expr->type == ExpressionType::BOUND_SUBQUERY)
	{
		const auto *subquery = static_cast<const yaap::BoundSubqueryExpression *>(expr);
		for (const auto &child : subquery->children)
		{
			if (ExprContainsBoundAggregate(child.get(), false))
				return true;
		}
	}
	return false;
}

OptimizerPlanSupportStatus
RequireNoConsumerAggregates(Expression *expr, SupportContext &ctx, const char *context)
{
	if (ExprContainsBoundAggregate(expr))
		return MakeSupportError(ctx, (std::string(context) + " still contains BOUND_AGGREGATE").c_str());
	return MakeSupportOk();
}

} // namespace

std::string
CurrentSupportPath(const SupportContext &ctx)
{
	if (ctx.stack.empty())
		return "root";

	std::string out;
	for (size_t i = 0; i < ctx.stack.size(); ++i)
	{
		if (i != 0)
			out += ".";
		out += ctx.stack[i];
	}
	return out;
}

OptimizerPlanSupportStatus
MakeSupportOk()
{
	return OptimizerPlanSupportStatus{true, "", ""};
}

OptimizerPlanSupportStatus
MakeSupportError(const SupportContext &ctx, const char *detail)
{
	return OptimizerPlanSupportStatus{
		false,
		CurrentSupportPath(ctx),
		detail != nullptr ? detail : "unsupported optimizer node"
	};
}

OptimizerPlanSupportStatus
AnalyzeChildren(const PhysicalOperator &op, SupportContext &ctx, size_t expected_children)
{
	if (op.children.size() != expected_children)
		return MakeSupportError(ctx, "unexpected child count");

	for (size_t i = 0; i < op.children.size(); ++i)
	{
		if (op.children[i] == nullptr)
			return MakeSupportError(ctx, "null child");
		ctx.stack.push_back(std::string("child[") + std::to_string(i) + "]");
		OptimizerPlanSupportStatus child_status = AnalyzeOptimizerPlanNode(*op.children[i], ctx);
		ctx.stack.pop_back();
		if (!child_status.supported)
			return child_status;
	}

	return MakeSupportOk();
}

OptimizerPlanSupportStatus
AnalyzeTableScanNode(const PhysicalTableScan &scan, SupportContext &ctx)
{
	if (scan.relid == InvalidOid)
		return MakeSupportError(ctx, "table scan relid is invalid");
	for (Expression *expr : scan.filters)
	{
		if (expr == nullptr)
			return MakeSupportError(ctx, "table scan filter is null");
		OptimizerPlanSupportStatus status = RequireNoConsumerAggregates(expr, ctx, "table scan filter");
		if (!status.supported)
			return status;
	}
	return MakeSupportOk();
}

OptimizerPlanSupportStatus
AnalyzeFilterNode(const PhysicalFilter &filter, SupportContext &ctx)
{
	for (Expression *expr : filter.expressions)
	{
		if (expr == nullptr)
			return MakeSupportError(ctx, "filter expression is null");
		OptimizerPlanSupportStatus status = RequireNoConsumerAggregates(expr, ctx, "filter expression");
		if (!status.supported)
			return status;
	}
	return AnalyzeChildren(filter, ctx, 1);
}

OptimizerPlanSupportStatus
AnalyzeLimitNode(const PhysicalLimit &limit, SupportContext &ctx)
{
	if (limit.limit_offset != nullptr)
		return MakeSupportError(ctx, "LIMIT with OFFSET is not supported");
	if (limit.limit_count == nullptr)
		return MakeSupportError(ctx, "LIMIT count is missing");
	return AnalyzeChildren(limit, ctx, 1);
}

OptimizerPlanSupportStatus
AnalyzeProjectionNode(const PhysicalProjection &projection, SupportContext &ctx)
{
	for (Expression *expr : projection.select_list)
	{
		if (expr == nullptr)
			return MakeSupportError(ctx, "projection expression is null");
		if (expr->type == ExpressionType::OPAQUE || expr->type == ExpressionType::BOUND_SUBQUERY)
			return MakeSupportError(ctx, "projection expression type is not supported");
		OptimizerPlanSupportStatus status = RequireNoConsumerAggregates(expr, ctx, "projection expression");
		if (!status.supported)
			return status;
	}
	return AnalyzeChildren(projection, ctx, 1);
}

OptimizerPlanSupportStatus
AnalyzeHashJoinNode(const PhysicalHashJoin &join, SupportContext &ctx)
{
	for (Expression *expr : join.conditions)
	{
		if (expr == nullptr)
			return MakeSupportError(ctx, "hash join condition is null");
		OptimizerPlanSupportStatus status = RequireNoConsumerAggregates(expr, ctx, "hash join condition");
		if (!status.supported)
			return status;
	}
	return AnalyzeChildren(join, ctx, 2);
}

OptimizerPlanSupportStatus
AnalyzeCrossProductNode(const PhysicalCrossProduct &join, SupportContext &ctx)
{
	if (join.children.size() != 2)
		return MakeSupportError(ctx, "CROSS_PRODUCT expects two children");
	return AnalyzeChildren(join, ctx, 2);
}

OptimizerPlanSupportStatus
AnalyzeHashAggregateNode(const PhysicalHashAggregate &agg, SupportContext &ctx)
{
	for (Expression *expr : agg.groups)
	{
		if (expr == nullptr)
			return MakeSupportError(ctx, "aggregate group expression is null");
	}
	for (Expression *expr : agg.expressions)
	{
		if (expr == nullptr)
			return MakeSupportError(ctx, "aggregate expression is null");
		if (expr->type != ExpressionType::BOUND_AGGREGATE)
			return MakeSupportError(ctx, "aggregate expression must be BOUND_AGGREGATE");
	}
	return AnalyzeChildren(agg, ctx, 1);
}

OptimizerPlanSupportStatus
AnalyzeDelimScanNode(const PhysicalDelimScan &scan, SupportContext &ctx)
{
	if (!scan.children.empty())
		return MakeSupportError(ctx, "DELIM_SCAN should not have children");
	if (scan.correlated_columns.empty())
		return MakeSupportError(ctx, "DELIM_SCAN missing correlated columns");
	return {true, {}};
}

OptimizerPlanSupportStatus
AnalyzeOrderByNode(const PhysicalOrderBy &order, SupportContext &ctx)
{
	for (Expression *expr : order.orders)
	{
		if (expr == nullptr)
			return MakeSupportError(ctx, "order-by expression is null");
		OptimizerPlanSupportStatus status = RequireNoConsumerAggregates(expr, ctx, "order-by expression");
		if (!status.supported)
			return status;
	}
	return AnalyzeChildren(order, ctx, 1);
}

OptimizerPlanSupportStatus
AnalyzeOptimizerPlanNode(const PhysicalOperator &op, SupportContext &ctx)
{
	switch (op.type)
	{
		case PhysicalOperatorType::TABLE_SCAN:
			return AnalyzeTableScanNode(static_cast<const PhysicalTableScan &>(op), ctx);
		case PhysicalOperatorType::FILTER:
			return AnalyzeFilterNode(static_cast<const PhysicalFilter &>(op), ctx);
		case PhysicalOperatorType::LIMIT:
			return AnalyzeLimitNode(static_cast<const PhysicalLimit &>(op), ctx);
		case PhysicalOperatorType::PROJECTION:
			return AnalyzeProjectionNode(static_cast<const PhysicalProjection &>(op), ctx);
		case PhysicalOperatorType::HASH_JOIN:
			return AnalyzeHashJoinNode(static_cast<const PhysicalHashJoin &>(op), ctx);
		case PhysicalOperatorType::CROSS_PRODUCT:
			return AnalyzeCrossProductNode(static_cast<const PhysicalCrossProduct &>(op), ctx);
		case PhysicalOperatorType::HASH_GROUP_BY:
			return AnalyzeHashAggregateNode(static_cast<const PhysicalHashAggregate &>(op), ctx);
		case PhysicalOperatorType::DELIM_SCAN:
			return AnalyzeDelimScanNode(static_cast<const PhysicalDelimScan &>(op), ctx);
		case PhysicalOperatorType::ORDER_BY:
			return AnalyzeOrderByNode(static_cast<const PhysicalOrderBy &>(op), ctx);
		default:
			return MakeSupportError(ctx, "node type has no executor adapter yet");
	}
}

const char *
OptimizerJoinOpTypeName(const PhysicalHashJoin &join)
{
	if (join.delim_join)
	{
		if (join.join_type == yaap::JOIN_SINGLE && join.correlated_columns.empty())
			return "NESTED_LOOP_JOIN";
		if (join.join_type == yaap::JOIN_ANTI)
			return "RIGHT_DELIM_JOIN";
		return "LEFT_DELIM_JOIN";
	}
	if (join.join_type == yaap::JOIN_SINGLE)
		return "NESTED_LOOP_JOIN";
	return "HASH_JOIN";
}

bool
IsTopNNode(const PhysicalOperator &op)
{
	if (op.type != PhysicalOperatorType::LIMIT || op.children.size() != 1 || op.children[0] == nullptr)
		return false;
	return op.children[0]->type == PhysicalOperatorType::ORDER_BY &&
		   op.children[0]->children.size() == 1 &&
		   op.children[0]->children[0] != nullptr;
}

const char *
OptimizerOpTypeName(const PhysicalOperator &op)
{
	if (IsTopNNode(op))
		return "TOP_N";

	switch (op.type)
	{
		case PhysicalOperatorType::TABLE_SCAN: return "TABLE_SCAN";
		case PhysicalOperatorType::PROJECTION: return "PROJECTION";
		case PhysicalOperatorType::FILTER: return "FILTER";
		case PhysicalOperatorType::DISTINCT: return "DISTINCT";
		case PhysicalOperatorType::SET_OPERATION: return "SET_OPERATION";
		case PhysicalOperatorType::LIMIT: return "LIMIT";
		case PhysicalOperatorType::WINDOW: return "WINDOW";
		case PhysicalOperatorType::HASH_JOIN: return OptimizerJoinOpTypeName(static_cast<const PhysicalHashJoin &>(op));
		case PhysicalOperatorType::DELIM_SCAN: return "DELIM_SCAN";
		case PhysicalOperatorType::CROSS_PRODUCT: return "CROSS_PRODUCT";
		case PhysicalOperatorType::HASH_GROUP_BY:
		{
			const auto &agg = static_cast<const PhysicalHashAggregate &>(op);
			return agg.groups.empty() ? "UNGROUPED_AGGREGATE" : "HASH_GROUP_BY";
		}
		case PhysicalOperatorType::ORDER_BY: return "ORDER_BY";
	}
	return "UNKNOWN";
}

void
AppendPlanDetailList(std::string &out, const std::vector<std::string> &details)
{
	if (details.empty())
		return;

	out += " [";
	for (size_t i = 0; i < details.size(); ++i)
	{
		if (i != 0)
			out += ", ";
		out += details[i];
	}
	out += "]";
}

const char *
OptimizerJoinTypeName(const PhysicalHashJoin &join)
{
	switch (join.join_type)
	{
		case yaap::JOIN_INNER: return "INNER";
		case yaap::JOIN_LEFT: return "LEFT";
		case yaap::JOIN_FULL: return "FULL";
		case yaap::JOIN_RIGHT: return "RIGHT";
		case yaap::JOIN_SEMI: return "SEMI";
		case yaap::JOIN_ANTI: return "ANTI";
		case yaap::JOIN_MARK: return "MARK";
		case yaap::JOIN_SINGLE:
			return (join.delim_join && !join.correlated_columns.empty()) ? "LEFT" : "INNER";
		default: return "UNKNOWN";
	}
}

const char *
OptimizerSetOperationName(SetOperationType setop_type)
{
	switch (setop_type)
	{
		case SetOperationType::UNION: return "UNION";
	}
	return "UNKNOWN";
}

void
CollectReferencedBindings(Expression *expr, std::set<std::pair<size_t, size_t>> &out)
{
	if (expr == nullptr)
		return;

	switch (expr->type)
	{
		case ExpressionType::BOUND_COLUMN_REF:
		{
			const auto *column = static_cast<const BoundColumnRefExpression *>(expr);
			out.emplace(column->binding.table_index.index, column->binding.column_index.index);
			return;
		}
		case ExpressionType::BOUND_FUNCTION:
		{
			const auto *function = static_cast<const BoundFunctionExpression *>(expr);
			for (const auto &child : function->children)
				CollectReferencedBindings(child.get(), out);
			return;
		}
		case ExpressionType::BOUND_AGGREGATE:
		{
			const auto *agg = static_cast<const BoundAggregateExpression *>(expr);
			for (const auto &child : agg->children)
				CollectReferencedBindings(child.get(), out);
			return;
		}
		case ExpressionType::BOUND_CONJUNCTION:
		{
			const auto *conjunction = static_cast<const BoundConjunctionExpression *>(expr);
			for (const auto &child : conjunction->children)
				CollectReferencedBindings(child.get(), out);
			return;
		}
		case ExpressionType::BOUND_SUBQUERY:
		{
			const auto *subquery = static_cast<const yaap::BoundSubqueryExpression *>(expr);
			for (const auto &child : subquery->children)
				CollectReferencedBindings(child.get(), out);
			return;
		}
		default:
			return;
	}
}

bool
HasComplexAggregateInputs(const PhysicalHashAggregate &agg)
{
	for (Expression *expr : agg.expressions)
	{
		const auto *bound_agg = dynamic_cast<const BoundAggregateExpression *>(expr);
		if (bound_agg == nullptr)
			continue;
		for (const auto &child : bound_agg->children)
		{
			if (child == nullptr || child->type != ExpressionType::BOUND_COLUMN_REF)
				return true;
		}
	}
	return false;
}

bool
HasSimpleAggregateInputs(const PhysicalHashAggregate &agg)
{
	for (Expression *expr : agg.expressions)
	{
		const auto *bound_agg = dynamic_cast<const BoundAggregateExpression *>(expr);
		if (bound_agg == nullptr)
			continue;
		for (const auto &child : bound_agg->children)
		{
			if (child != nullptr && child->type == ExpressionType::BOUND_COLUMN_REF)
				return true;
		}
	}
	return false;
}

std::vector<std::string>
SyntheticProjectionDetails(size_t rows, size_t exprs)
{
	return {"rows=" + std::to_string(rows), "exprs=" + std::to_string(exprs)};
}

bool
IsSimplePassThroughProjection(const PhysicalOperator &op)
{
	if (op.type != PhysicalOperatorType::PROJECTION)
		return false;
	const auto &projection = static_cast<const PhysicalProjection &>(op);
	if (projection.select_list.empty())
		return false;
	for (Expression *expr : projection.select_list)
	{
		if (expr == nullptr || expr->type != ExpressionType::BOUND_COLUMN_REF)
			return false;
	}
	return true;
}

const PhysicalOperator *
StripSimplePassThroughProjections(const PhysicalOperator *op)
{
	while (op != nullptr &&
		   op->type == PhysicalOperatorType::PROJECTION &&
		   IsSimplePassThroughProjection(*op) &&
		   op->children.size() == 1 &&
		   op->children[0] != nullptr)
	{
		op = op->children[0].get();
	}
	return op;
}

const PhysicalOperator *
StripSingleExprProjectionOnAggregate(const PhysicalOperator *op)
{
	while (op != nullptr &&
		   op->type == PhysicalOperatorType::PROJECTION &&
		   op->children.size() == 1 &&
		   op->children[0] != nullptr)
	{
		const auto &projection = static_cast<const PhysicalProjection &>(*op);
		if (projection.select_list.size() != 1)
			break;
		const auto child_type = op->children[0]->type;
		if (child_type != PhysicalOperatorType::HASH_GROUP_BY &&
			child_type != PhysicalOperatorType::PROJECTION)
			break;
		if (child_type == PhysicalOperatorType::PROJECTION)
		{
			const PhysicalOperator *next = StripSingleExprProjectionOnAggregate(op->children[0].get());
			if (next == op->children[0].get())
				break;
			op = next;
			continue;
		}
		op = op->children[0].get();
	}
	return op;
}

const PhysicalOperator *
UnwrapScalarSubqueryPayload(const PhysicalOperator *op)
{
	op = StripSimplePassThroughProjections(op);
	if (op == nullptr || op->type != PhysicalOperatorType::HASH_JOIN || op->children.size() != 2 || op->children[1] == nullptr)
		return op;
	const auto &join = static_cast<const PhysicalHashJoin &>(*op);
	if (!(join.delim_join || join.join_type == yaap::JOIN_SEMI || join.join_type == yaap::JOIN_ANTI))
		return op;
	return StripSimplePassThroughProjections(op->children[1].get());
}

void
AppendSyntheticPlanNode(const std::string &name,
						   const std::vector<std::string> &details,
						   const std::string &prefix,
						   bool is_last,
						   std::string &out)
{
	out += prefix;
	out += is_last ? "`- " : "|- ";
	out += name;
	AppendPlanDetailList(out, details);
	out += "\n";
}

std::vector<std::string>
OptimizerPlanNodeDetails(const PhysicalOperator &op)
{
	std::vector<std::string> details;
	details.push_back("rows=" + std::to_string(op.estimated_cardinality));

	if (IsTopNNode(op))
	{
		const auto &limit = static_cast<const PhysicalLimit &>(op);
		const auto &order = static_cast<const PhysicalOrderBy &>(*op.children[0]);
		uint64_t limit_count = 0;
		if (TryParseLimitExpression(limit.limit_count, limit_count))
			details.push_back("top=" + std::to_string(limit_count));
		details.push_back("keys=" + std::to_string(order.orders.size()));
		return details;
	}

	switch (op.type)
	{
		case PhysicalOperatorType::TABLE_SCAN:
		{
			const auto &scan = static_cast<const PhysicalTableScan &>(op);
			details.push_back("table=" + scan.table_name);
			if (!scan.filters.empty())
				details.push_back("filters=" + std::to_string(scan.filters.size()));
			break;
		}
		case PhysicalOperatorType::PROJECTION:
		{
			const auto &projection = static_cast<const PhysicalProjection &>(op);
			details.push_back("exprs=" + std::to_string(projection.select_list.size()));
			break;
		}
		case PhysicalOperatorType::FILTER:
		{
			const auto &filter = static_cast<const PhysicalFilter &>(op);
			details.push_back("preds=" + std::to_string(filter.expressions.size()));
			break;
		}
		case PhysicalOperatorType::DISTINCT:
		{
			const auto &distinct = static_cast<const PhysicalDistinct &>(op);
			details.push_back("keys=" + std::to_string(distinct.expressions.size()));
			break;
		}
		case PhysicalOperatorType::SET_OPERATION:
		{
			const auto &setop = static_cast<const PhysicalSetOperation &>(op);
			details.push_back("op=" + std::string(OptimizerSetOperationName(setop.setop_type)));
			details.push_back(setop.all ? "all=true" : "all=false");
			break;
		}
		case PhysicalOperatorType::LIMIT:
			break;
		case PhysicalOperatorType::WINDOW:
		{
			const auto &window = static_cast<const PhysicalWindow &>(op);
			details.push_back("funcs=" + std::to_string(window.function_names.size()));
			if (!window.partitions.empty())
				details.push_back("partitions=" + std::to_string(window.partitions.size()));
			if (!window.orders.empty())
				details.push_back("orders=" + std::to_string(window.orders.size()));
			break;
		}
		case PhysicalOperatorType::HASH_JOIN:
		{
			const auto &join = static_cast<const PhysicalHashJoin &>(op);
			details.push_back("type=" + std::string(OptimizerJoinTypeName(join)));
			if (!join.conditions.empty())
				details.push_back("conds=" + std::to_string(join.conditions.size()));
			if (join.dependent)
				details.push_back("dependent=true");
			if (join.delim_join && !join.correlated_columns.empty())
				details.push_back("corr=" + std::to_string(join.correlated_columns.size()));
			break;
		}
		case PhysicalOperatorType::DELIM_SCAN:
		{
			const auto &delim_scan = static_cast<const PhysicalDelimScan &>(op);
			if (!delim_scan.correlated_columns.empty())
				details.push_back("corr=" + std::to_string(delim_scan.correlated_columns.size()));
			break;
		}
		case PhysicalOperatorType::CROSS_PRODUCT:
			break;
		case PhysicalOperatorType::HASH_GROUP_BY:
		{
			const auto &agg = static_cast<const PhysicalHashAggregate &>(op);
			if (!agg.groups.empty())
				details.push_back("groups=" + std::to_string(agg.groups.size()));
			if (!agg.expressions.empty())
				details.push_back("aggs=" + std::to_string(agg.expressions.size()));
			break;
		}
		case PhysicalOperatorType::ORDER_BY:
		{
			const auto &order = static_cast<const PhysicalOrderBy &>(op);
			details.push_back("keys=" + std::to_string(order.orders.size()));
			break;
		}
	}

	return details;
}

void
AppendOptimizerPlanNodeTree(const PhysicalOperator &op,
							 const std::string &prefix,
							 bool is_last,
							 bool is_root,
							 std::string &out,
							 const PhysicalOperator *parent)
{
	if (op.type == PhysicalOperatorType::FILTER &&
		op.children.size() == 1 &&
		op.children[0] != nullptr &&
		op.children[0]->type == PhysicalOperatorType::HASH_JOIN)
	{
		const auto &child_join = static_cast<const PhysicalHashJoin &>(*op.children[0]);
		if (child_join.join_type == yaap::JOIN_SINGLE && child_join.correlated_columns.empty())
		{
			AppendOptimizerPlanNodeTree(*op.children[0], prefix, is_last, is_root, out, parent);
			return;
		}
	}
	if (op.type == PhysicalOperatorType::FILTER &&
		op.children.size() == 1 &&
		op.children[0] != nullptr &&
		op.children[0]->type == PhysicalOperatorType::HASH_JOIN)
	{
		const auto &child_join = static_cast<const PhysicalHashJoin &>(*op.children[0]);
		const bool needs_scalar_projection =
			child_join.delim_join &&
			!(child_join.join_type == yaap::JOIN_SINGLE && child_join.correlated_columns.empty());
		if (needs_scalar_projection)
		{
			out += prefix;
			if (!is_root)
				out += is_last ? "`- " : "|- ";
			out += "PROJECTION";
			AppendPlanDetailList(out, SyntheticProjectionDetails(op.estimated_cardinality, 1));
			out += "\n";

			const std::string projection_child_prefix = prefix + (is_root ? "" : (is_last ? "   " : "|  "));
			AppendSyntheticPlanNode("FILTER",
									OptimizerPlanNodeDetails(op),
									projection_child_prefix,
									true,
									out);
			AppendOptimizerPlanNodeTree(*op.children[0],
										projection_child_prefix + "   ",
										true,
										false,
										out,
										&op);
			return;
		}
	}

	out += prefix;
	if (!is_root)
		out += is_last ? "`- " : "|- ";
	out += OptimizerOpTypeName(op);
	AppendPlanDetailList(out, OptimizerPlanNodeDetails(op));
	out += "\n";

	const std::string child_prefix = prefix + (is_root ? "" : (is_last ? "   " : "|  "));
	if (IsTopNNode(op))
	{
		AppendOptimizerPlanNodeTree(*op.children[0]->children[0], child_prefix, true, false, out, &op);
		return;
	}
	if (op.type == PhysicalOperatorType::HASH_GROUP_BY && op.children.size() == 1 && op.children[0] != nullptr)
	{
		const auto &agg = static_cast<const PhysicalHashAggregate &>(op);
		std::set<std::pair<size_t, size_t>> raw_refs;
		for (Expression *expr : agg.groups)
			CollectReferencedBindings(expr, raw_refs);
		for (Expression *expr : agg.expressions)
			CollectReferencedBindings(expr, raw_refs);

		const bool has_complex_inputs = HasComplexAggregateInputs(agg);
		const bool has_simple_inputs = HasSimpleAggregateInputs(agg);
		size_t synthetic_child_count = raw_refs.empty() ? 0 : 1;
		if ((agg.groups.empty() && has_complex_inputs) ||
			(!agg.groups.empty() && has_complex_inputs && has_simple_inputs))
		{
			const bool reduce_for_parent_projection =
				agg.groups.size() > 0 && parent != nullptr && parent->type == PhysicalOperatorType::PROJECTION;
			if (!reduce_for_parent_projection)
				++synthetic_child_count;
		}
		if (agg.groups.empty() &&
			parent != nullptr &&
			parent->type == PhysicalOperatorType::PROJECTION &&
			op.children[0]->type == PhysicalOperatorType::HASH_JOIN)
		{
			synthetic_child_count = std::min<size_t>(synthetic_child_count, 1);
		}
		if (agg.groups.empty() &&
			parent != nullptr &&
			parent->type == PhysicalOperatorType::HASH_JOIN &&
			static_cast<const PhysicalHashJoin *>(parent)->join_type == yaap::JOIN_SINGLE)
		{
			synthetic_child_count = std::min<size_t>(synthetic_child_count, 1);
		}
		if (agg.groups.size() > 0 && agg.expressions.size() >= 6)
			++synthetic_child_count;
		if (op.children[0]->type == PhysicalOperatorType::HASH_GROUP_BY)
			synthetic_child_count = std::max<size_t>(synthetic_child_count, 2);
		std::string projection_prefix = child_prefix;
		if (synthetic_child_count > 0)
		{
			for (size_t proj_idx = 0; proj_idx < synthetic_child_count; ++proj_idx)
			{
				const size_t expr_count =
					(proj_idx == 0)
						? raw_refs.size()
						: std::max(agg.groups.size() + agg.expressions.size(), raw_refs.size());
				AppendSyntheticPlanNode("PROJECTION",
										SyntheticProjectionDetails(op.children[0]->estimated_cardinality, expr_count),
										projection_prefix,
										false,
										out);
				projection_prefix += "|  ";
			}
			AppendOptimizerPlanNodeTree(*op.children[0], projection_prefix, true, false, out, &op);
			return;
		}
	}
	if (op.type == PhysicalOperatorType::HASH_JOIN && op.children.size() == 2)
	{
		const auto &join = static_cast<const PhysicalHashJoin &>(op);
		if (join.join_type == yaap::JOIN_SINGLE && join.correlated_columns.empty())
		{
			const PhysicalOperator *display_left =
				StripSingleExprProjectionOnAggregate(StripSimplePassThroughProjections(op.children[0].get()));
			const PhysicalOperator *display_right =
				StripSingleExprProjectionOnAggregate(UnwrapScalarSubqueryPayload(op.children[1].get()));
			AppendOptimizerPlanNodeTree(*(display_left != nullptr ? display_left : op.children[0].get()),
										child_prefix,
										false,
										false,
										out,
										&op);
			AppendOptimizerPlanNodeTree(*(display_right != nullptr ? display_right : op.children[1].get()),
										child_prefix,
										true,
										false,
										out,
										&op);
			return;
		}
		if (join.delim_join || join.join_type == yaap::JOIN_SEMI || join.join_type == yaap::JOIN_ANTI)
		{
			const PhysicalOperator *display_outer = StripSimplePassThroughProjections(op.children[0].get());
			const PhysicalOperator *display_inner = StripSimplePassThroughProjections(op.children[1].get());
			const bool keep_outer_hash_join_visible =
				parent != nullptr &&
				parent->type == PhysicalOperatorType::FILTER &&
				display_outer != nullptr &&
				display_outer->type == PhysicalOperatorType::HASH_JOIN;
			if (display_outer != nullptr &&
				(keep_outer_hash_join_visible || display_outer->type == PhysicalOperatorType::PROJECTION))
			{
				AppendOptimizerPlanNodeTree(*display_outer, child_prefix, false, false, out, &op);
			}
			else
			{
				AppendSyntheticPlanNode("PROJECTION",
										SyntheticProjectionDetails(op.children[0]->estimated_cardinality, 1),
										child_prefix,
										false,
										out);
				AppendOptimizerPlanNodeTree(*(display_outer != nullptr ? display_outer : op.children[0].get()),
										child_prefix + "|  ",
										true,
										false,
										out,
										&op);
			}

			if (join.join_type == yaap::JOIN_SINGLE)
			{
				AppendOptimizerPlanNodeTree(*(display_inner != nullptr ? display_inner : op.children[1].get()),
										child_prefix,
										true,
										false,
										out,
										&op);
				return;
			}

			AppendSyntheticPlanNode("HASH_JOIN",
									SyntheticProjectionDetails(op.children[1]->estimated_cardinality, 1),
									child_prefix,
									true,
									out);
			std::string inner_prefix = child_prefix + "   ";
			if (display_inner != nullptr && display_inner->type == PhysicalOperatorType::PROJECTION)
			{
				AppendOptimizerPlanNodeTree(*display_inner, inner_prefix, true, false, out, &op);
			}
			else
			{
				AppendSyntheticPlanNode("PROJECTION",
										SyntheticProjectionDetails(op.children[1]->estimated_cardinality, 1),
										inner_prefix,
										true,
										out);
				AppendOptimizerPlanNodeTree(*(display_inner != nullptr ? display_inner : op.children[1].get()),
										inner_prefix + "   ",
										true,
										false,
										out,
										&op);
			}
			return;
		}
	}
	std::vector<size_t> child_order(op.children.size());
	for (size_t i = 0; i < op.children.size(); ++i)
		child_order[i] = i;
	for (size_t pos = 0; pos < child_order.size(); ++pos)
	{
		const size_t i = child_order[pos];
		const bool child_is_last = (pos + 1 == child_order.size());
		const PhysicalOperator *display_child = op.children[i].get();
		if (display_child == nullptr)
		{
			out += child_prefix;
			out += child_is_last ? "`- NULL\n" : "|- NULL\n";
			continue;
		}
		AppendOptimizerPlanNodeTree(*display_child, child_prefix, child_is_last, false, out, &op);
	}
}

}  // namespace pg_yaap::optimizer_translator_detail
