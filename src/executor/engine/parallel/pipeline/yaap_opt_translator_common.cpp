#include "parallel/pipeline/yaap_pipeline_init_internal.hpp"

#include <cerrno>
#include <cctype>
#include <cstdlib>

namespace pg_yaap::optimizer_translator_detail {

namespace {

bool
LooksLikeNumericLiteral(const std::string &value)
{
	const char *ptr = value.c_str();
	char *end = nullptr;
	errno = 0;
	(void) std::strtod(ptr, &end);
	if (errno != 0 || end == ptr)
		return false;
	while (*end != '\0' && std::isspace(static_cast<unsigned char>(*end)))
		++end;
	return *end == '\0';
}

}  // namespace

bool
BuildOrderedOutputBindingsForRefs(const std::vector<ColumnRef> &requested_refs,
								  const std::vector<ColumnRef> &raw_refs,
								  const std::vector<yaap::PhysicalOperator::OutputColumn> &raw_outputs,
								  std::vector<yaap::PhysicalOperator::OutputColumn> &out_bindings)
{
	if (raw_refs.size() != raw_outputs.size())
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: output binding alignment size mismatch requested=%zu raw_refs=%zu raw_outputs=%zu",
				 requested_refs.size(),
				 raw_refs.size(),
				 raw_outputs.size());
		return false;
	}

	if (requested_refs.size() == raw_refs.size())
	{
		bool identical = true;
		for (size_t i = 0; i < requested_refs.size(); ++i)
		{
			if (!SameColumnRef(requested_refs[i], raw_refs[i]))
			{
				if (pg_yaap_trace_hooks)
					elog(LOG,
						 "pg_yaap: output binding alignment order mismatch idx=%zu requested=(%u,%d) raw=(%u,%d)",
						 i,
						 requested_refs[i].varno,
						 requested_refs[i].attno,
						 raw_refs[i].varno,
						 raw_refs[i].attno);
				identical = false;
				break;
			}
		}
		if (identical)
		{
			out_bindings = raw_outputs;
			return true;
		}
	}

	out_bindings.clear();
	out_bindings.reserve(requested_refs.size());
	std::vector<bool> used(raw_refs.size(), false);
	for (const ColumnRef &ref : requested_refs)
	{
		bool matched = false;
		for (size_t i = 0; i < raw_refs.size(); ++i)
		{
			if (used[i] || !SameColumnRef(ref, raw_refs[i]))
				continue;
			out_bindings.push_back(raw_outputs[i]);
			used[i] = true;
			matched = true;
			break;
		}
		if (!matched)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG,
					 "pg_yaap: output binding alignment missing requested=(%u,%d) raw_refs=%zu",
					 ref.varno,
					 ref.attno,
					 raw_refs.size());
			return false;
		}
	}
	return true;
}

namespace {

std::unique_ptr<Expression>
RewritePipelineFilterExpr(const Expression *expr,
						 const PhysicalOperator *source_op)
{
	if (expr == nullptr)
		return nullptr;

	switch (expr->type)
	{
		case ExpressionType::BOUND_COLUMN_REF:
		{
			const auto *column = static_cast<const BoundColumnRefExpression *>(expr);
			if (source_op != nullptr)
			{
				if ((source_op->type == PhysicalOperatorType::FILTER ||
					 source_op->type == PhysicalOperatorType::ORDER_BY ||
					 source_op->type == PhysicalOperatorType::LIMIT) &&
					source_op->children.size() == 1 &&
					source_op->children[0] != nullptr)
					return RewritePipelineFilterExpr(expr, source_op->children[0].get());
				if (source_op->type == PhysicalOperatorType::PROJECTION)
				{
					const auto *projection = static_cast<const PhysicalProjection *>(source_op);
					const bool refers_to_projection_output =
						column->binding.table_index.index == projection->table_index.index;
					const size_t ordinal = column->binding.column_index.index;
					if (refers_to_projection_output &&
						ordinal < projection->select_list.size() &&
						projection->children.size() == 1 &&
						projection->children[0] != nullptr)
						return RewritePipelineFilterExpr(projection->select_list[ordinal], projection->children[0].get());
				}
			}
			return std::make_unique<BoundColumnRefExpression>(column->binding, column->table_name, column->column_name);
		}
		case ExpressionType::BOUND_CONSTANT:
		{
			const auto *constant = static_cast<const BoundConstantExpression *>(expr);
			return std::make_unique<BoundConstantExpression>(constant->value, constant->is_null);
		}
		case ExpressionType::BOUND_AGGREGATE:
		{
			const auto *aggregate = static_cast<const BoundAggregateExpression *>(expr);
			elog(ERROR,
				 "pg_yaap: optimizer filter expression still contains aggregate %s; physical planner must canonicalize it",
				 aggregate->function_name.c_str());
			return nullptr;
		}
		case ExpressionType::BOUND_FUNCTION:
		{
			const auto *function = static_cast<const BoundFunctionExpression *>(expr);
			auto clone = std::make_unique<BoundFunctionExpression>(function->function_name, function->op_oid);
			for (const auto &child : function->children)
				clone->children.push_back(RewritePipelineFilterExpr(child.get(), source_op));
			return clone;
		}
		case ExpressionType::BOUND_CONJUNCTION:
		{
			const auto *conjunction = static_cast<const BoundConjunctionExpression *>(expr);
			auto clone = std::make_unique<BoundConjunctionExpression>(conjunction->bool_expr_type);
			for (const auto &child : conjunction->children)
				clone->children.push_back(RewritePipelineFilterExpr(child.get(), source_op));
			return clone;
		}
		default:
			return nullptr;
	}
}

} // namespace

bool
UseInt32CharDecodeForType(Oid type_oid, int32 typmod)
{
	if (type_oid == CHAROID)
		return true;
	if (type_oid != BPCHAROID || typmod < VARHDRSZ)
		return false;
	return (typmod - VARHDRSZ) == 1;
}

dsa_pointer
BuildFilterArray(dsa_area *dsa, const void *data, size_t elem_size, size_t count)
{
	if (count == 0)
		return InvalidDsaPointer;
	dsa_pointer dp = dsa_allocate0(dsa, elem_size * count);
	if (!DsaPointerIsValid(dp))
		return InvalidDsaPointer;
	std::memcpy(dsa_get_address(dsa, dp), data, elem_size * count);
	return dp;
}

dsa_pointer
BuildCharArray(dsa_area *dsa, const std::vector<char> &bytes)
{
	if (bytes.empty())
		return InvalidDsaPointer;
	dsa_pointer dp = dsa_allocate0(dsa, bytes.size());
	if (!DsaPointerIsValid(dp))
		return InvalidDsaPointer;
	std::memcpy(dsa_get_address(dsa, dp), bytes.data(), bytes.size());
	return dp;
}

dsa_pointer
BuildOutputTdc(dsa_area *dsa,
			   dsa_pointer layout_dp,
			   const TupleDataLayout &layout,
			   uint32_t row_capacity)
{
	const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(&layout, row_capacity);
	dsa_pointer payload_dp = pg_yaap::pipeline::TupleDataCollectionAllocate(
		dsa,
		row_capacity,
		layout.row_width,
		heap_capacity);
	if (!DsaPointerIsValid(payload_dp))
		return InvalidDsaPointer;
	auto *tdc = static_cast<TupleDataCollection *>(dsa_get_address(dsa, payload_dp));
	TupleDataCollectionInit(tdc, row_capacity, layout.row_width, layout_dp, heap_capacity);
	return payload_dp;
}

uint32_t
EstimateOutputRows(QueryDesc *qd)
{
	double plan_rows = 1024.0;
	if (qd != nullptr && qd->plannedstmt != nullptr && qd->plannedstmt->planTree != nullptr)
		plan_rows = qd->plannedstmt->planTree->plan_rows;
	const double cap_d = std::max(1024.0, std::min(static_cast<double>(1u << 20), plan_rows * 1.5));
	return static_cast<uint32_t>(cap_d);
}

uint32_t
EstimateResultRows(QueryDesc *qd, uint32_t estimated_groups)
{
	if (estimated_groups > 0)
		return std::max<uint32_t>(1024u, std::min<uint32_t>(1u << 20, estimated_groups));
	return EstimateOutputRows(qd);
}

uint32_t
EstimateInitialResultRows(QueryDesc *qd, uint32_t estimated_groups)
{
	return std::min<uint32_t>(8192u, EstimateResultRows(qd, estimated_groups));
}

uint32_t
EstimateHashJoinBuildRows(size_t estimated_rows)
{
	const double rows = estimated_rows > 0 ? static_cast<double>(estimated_rows) : 1024.0;
	const double with_margin = std::max(1024.0, rows * 1.25);
	return static_cast<uint32_t>(std::min(static_cast<double>(1u << 26), with_margin));
}

ColumnRef
BindingToColumnRef(const yaap::ColumnBinding &binding)
{
	return ColumnRef{
		static_cast<Index>(binding.table_index.index + 1),
		static_cast<AttrNumber>(binding.column_index.index + 1)
	};
}

bool
SameColumnRef(const ColumnRef &lhs, const ColumnRef &rhs)
{
	return lhs.varno == rhs.varno && lhs.attno == rhs.attno;
}

void
AppendUniqueColumnRef(const ColumnRef &ref, std::vector<ColumnRef> &out)
{
	for (const ColumnRef &existing : out)
	{
		if (SameColumnRef(existing, ref))
			return;
	}
	out.push_back(ref);
}

void
CollectReferencedColumns(const Expression *expr, std::vector<ColumnRef> &out)
{
	if (expr == nullptr)
		return;
	switch (expr->type)
	{
		case ExpressionType::BOUND_COLUMN_REF:
			AppendUniqueColumnRef(BindingToColumnRef(static_cast<const BoundColumnRefExpression *>(expr)->binding), out);
			return;
		case ExpressionType::BOUND_FUNCTION:
		{
			const auto *func = static_cast<const BoundFunctionExpression *>(expr);
			for (const auto &child : func->children)
				CollectReferencedColumns(child.get(), out);
			return;
		}
		case ExpressionType::BOUND_AGGREGATE:
		{
			const auto *agg = static_cast<const BoundAggregateExpression *>(expr);
			for (const auto &child : agg->children)
				CollectReferencedColumns(child.get(), out);
			return;
		}
		case ExpressionType::BOUND_CONJUNCTION:
		{
			const auto *conj = static_cast<const BoundConjunctionExpression *>(expr);
			for (const auto &child : conj->children)
				CollectReferencedColumns(child.get(), out);
			return;
		}
		default:
			return;
	}
}

void
CollectReferencedSourceColumns(const Expression *expr,
							   const PhysicalOperator *source_op,
							   std::vector<ColumnRef> &out)
{
	if (expr == nullptr)
		return;
	switch (expr->type)
	{
		case ExpressionType::BOUND_COLUMN_REF:
		{
			const auto *column = static_cast<const BoundColumnRefExpression *>(expr);
			AppendUniqueColumnRef(BindingToColumnRef(column->binding), out);
			return;
		}
		case ExpressionType::BOUND_FUNCTION:
		{
			const auto *func = static_cast<const BoundFunctionExpression *>(expr);
			for (const auto &child : func->children)
				CollectReferencedSourceColumns(child.get(), source_op, out);
			return;
		}
		case ExpressionType::BOUND_AGGREGATE:
		{
			const auto *agg = static_cast<const BoundAggregateExpression *>(expr);
			for (const auto &child : agg->children)
				CollectReferencedSourceColumns(child.get(), source_op, out);
			return;
		}
		case ExpressionType::BOUND_CONJUNCTION:
		{
			const auto *conj = static_cast<const BoundConjunctionExpression *>(expr);
			for (const auto &child : conj->children)
				CollectReferencedSourceColumns(child.get(), source_op, out);
			return;
		}
		default:
			return;
	}
}

void
FilterRequestedColumns(const std::vector<ColumnRef> &available,
					   const std::vector<ColumnRef> *required,
					   std::vector<ColumnRef> &out)
{
	out.clear();
	if (required == nullptr || required->empty())
	{
		out = available;
		return;
	}
	for (const ColumnRef &ref : *required)
	{
		for (const ColumnRef &candidate : available)
		{
			if (SameColumnRef(ref, candidate))
			{
				AppendUniqueColumnRef(candidate, out);
				break;
			}
		}
	}
}

bool
InferSyntheticParentVarno(const std::vector<ColumnRef> *required,
						  Index &out_varno)
{
	if (required == nullptr || required->empty())
		return false;

	const Index varno = required->front().varno;
	if (varno == 0)
		return false;

	for (const ColumnRef &ref : *required)
	{
		if (ref.varno != varno)
			return false;
	}
	for (size_t i = 0; i < required->size(); ++i)
	{
		if ((*required)[i].attno != static_cast<AttrNumber>(i + 1))
			return false;
	}

	out_varno = varno;
	return true;
}

std::vector<ColumnRef>
BuildParentFacingOutputCols(const std::vector<ColumnRef> &selected_raw_output_cols,
							const std::vector<ColumnRef> *required_output_cols)
{
	Index synthetic_varno = 0;
	/*
	 * Only retag outputs when the parent requested the full projected shape.
	 * If the parent asked for a strict subset, preserving the raw child
	 * bindings keeps downstream join/filter expressions aligned with the
	 * projection's own table_index.
	 */
	if (required_output_cols == nullptr ||
		required_output_cols->size() != selected_raw_output_cols.size() ||
		!InferSyntheticParentVarno(required_output_cols, synthetic_varno))
		return selected_raw_output_cols;

	std::vector<ColumnRef> out_cols;
	out_cols.reserve(selected_raw_output_cols.size());
	for (size_t i = 0; i < selected_raw_output_cols.size(); ++i)
	{
		out_cols.push_back(ColumnRef{
			synthetic_varno,
			static_cast<AttrNumber>(i + 1)});
	}
	return out_cols;
}

bool
LookupBindingColumn(const yaap::ColumnBinding &binding,
					const std::vector<ColumnRef> &cols,
					const std::vector<ColumnSchema> &schema,
					ColumnRef &out_ref,
					const ColumnSchema *&out_col)
{
	out_ref = BindingToColumnRef(binding);
	return LookupRawColumn(out_ref, cols, schema, out_col);
}

bool
LookupNamedExprInputColumn(const BoundColumnRefExpression *expr,
						   const std::vector<yaap::PhysicalOperator::OutputColumn> *outputs,
						   const std::vector<ColumnRef> &cols,
						   const std::vector<ColumnSchema> &schema,
						   ColumnRef &out_ref,
						   const ColumnSchema *&out_col)
{
	if (expr == nullptr)
		return false;

	if (outputs != nullptr)
	{
		for (size_t i = 0; i < outputs->size(); ++i)
		{
			const auto &output = (*outputs)[i];
			const bool binding_match =
				output.binding.table_index.index == expr->binding.table_index.index &&
				output.binding.column_index.index == expr->binding.column_index.index;
			if (!binding_match || i >= cols.size() || i >= schema.size())
				continue;
			out_ref = cols[i];
			out_col = &schema[i];
			return true;
		}
		return false;
	}
	return LookupBindingColumn(expr->binding, cols, schema, out_ref, out_col) && out_col != nullptr;
}

bool
LookupPassthroughColumn(const ColumnRef &ref,
						const std::vector<ColumnRef> &cols,
						const std::vector<ColumnSchema> &schema,
						const ColumnSchema *&out_col)
{
	if (LookupRawColumn(ref, cols, schema, out_col))
		return true;
	if (cols.size() != schema.size())
		return false;
	if (ref.attno <= 0)
		return false;
	const size_t ordinal = static_cast<size_t>(ref.attno - 1);
	if (ordinal >= schema.size())
		return false;
	out_col = &schema[ordinal];
	return true;
}

bool
ApplyPostAggregateFilters(OptimizerNodeTranslation node,
						  const PhysicalHashAggregate &source_agg,
						  const std::vector<Expression *> &filters,
						  PgYaapQueryState *state,
						  OptimizerNodeTranslation &out)
{
	if (filters.empty())
	{
		out = std::move(node);
		return true;
	}
	if (state == nullptr || state->runtime_dsa == nullptr || node.op == nullptr)
		return false;
	(void) source_agg;

	std::vector<ColumnSchema> filter_schema = node.schema;
	for (ColumnSchema &col : filter_schema)
	{
		if (col.src_attno <= 0)
			col.src_attno = static_cast<int16_t>(col.chunk_slot + 1);
	}

	dsa_pointer input_schema_dp = BuildSchemaDescriptorFromColumns(node.schema, state->runtime_dsa);
	if (!DsaPointerIsValid(input_schema_dp))
		return false;

	std::vector<FilterInputDesc> filter_inputs;
	std::vector<FilterExprDesc> filter_exprs;
	std::vector<FilterStep> filter_steps;
	std::vector<char> filter_string_consts;
	if (!LowerScanFilters(filters,
						  &node.outputs,
						  node.cols,
						  filter_schema,
						  filter_inputs,
						  filter_exprs,
						  filter_steps,
						  filter_string_consts))
		return false;

	auto filter_op = std::make_unique<PipelineFilter>(
		input_schema_dp,
		PgVector<FilterInputDesc>(filter_inputs.begin(), filter_inputs.end()),
		PgVector<FilterExprDesc>(filter_exprs.begin(), filter_exprs.end()),
		PgVector<FilterStep>(filter_steps.begin(), filter_steps.end()),
		PgVector<char>(filter_string_consts.begin(), filter_string_consts.end()));
	filter_op->AddChild(std::move(node.op));
	node.op = std::move(filter_op);
	out = std::move(node);
	return true;
}

bool
ApplyPipelineFilters(OptimizerNodeTranslation node,
					const PhysicalOperator *source_op,
					const std::vector<Expression *> &filters,
					PgYaapQueryState *state,
					OptimizerNodeTranslation &out)
{
	if (filters.empty())
	{
		out = std::move(node);
		return true;
	}
	if (state == nullptr || state->runtime_dsa == nullptr || node.op == nullptr)
		return false;

	std::vector<ColumnSchema> filter_schema = node.schema;
	for (ColumnSchema &col : filter_schema)
	{
		if (col.src_attno <= 0)
			col.src_attno = static_cast<int16_t>(col.chunk_slot + 1);
	}

	std::vector<std::unique_ptr<Expression>> rewritten_storage;
	std::vector<Expression *> rewritten_filters;
	rewritten_storage.reserve(filters.size());
	rewritten_filters.reserve(filters.size());
	for (Expression *filter_expr : filters)
	{
		auto rewritten = RewritePipelineFilterExpr(filter_expr, source_op);
		if (rewritten == nullptr)
			return false;
		rewritten_filters.push_back(rewritten.get());
		rewritten_storage.push_back(std::move(rewritten));
	}

	dsa_pointer input_schema_dp = BuildSchemaDescriptorFromColumns(node.schema, state->runtime_dsa);
	if (!DsaPointerIsValid(input_schema_dp))
		return false;

	std::vector<FilterInputDesc> filter_inputs;
	std::vector<FilterExprDesc> filter_exprs;
	std::vector<FilterStep> filter_steps;
	std::vector<char> filter_string_consts;
	if (!LowerScanFilters(rewritten_filters,
						  &node.outputs,
						  node.cols,
						  filter_schema,
						  filter_inputs,
						  filter_exprs,
						  filter_steps,
						  filter_string_consts))
		return false;

	auto filter_op = std::make_unique<PipelineFilter>(
		input_schema_dp,
		PgVector<FilterInputDesc>(filter_inputs.begin(), filter_inputs.end()),
		PgVector<FilterExprDesc>(filter_exprs.begin(), filter_exprs.end()),
		PgVector<FilterStep>(filter_steps.begin(), filter_steps.end()),
		PgVector<char>(filter_string_consts.begin(), filter_string_consts.end()));
	filter_op->AddChild(std::move(node.op));
	node.op = std::move(filter_op);
	out = std::move(node);
	return true;
}

bool
IsComparisonName(const std::string &name)
{
	return name == "<" || name == "<=" || name == "=" || name == ">=" || name == ">" || name == "<>" ||
		   name == "!=" || name == "~~";
}

bool
MapComparisonNameToQualOp(const std::string &name, QualOp &out)
{
	if (name == "<=") out = QualOp::LE;
	else if (name == "<") out = QualOp::LT;
	else if (name == "=") out = QualOp::EQ;
	else if (name == ">=") out = QualOp::GE;
	else if (name == ">") out = QualOp::GT;
	else if (name == "<>" || name == "!=") out = QualOp::NE;
	else return false;
	return true;
}

bool
TryParseUInt64(const std::string &text, uint64_t &out)
{
	const char *ptr = text.c_str();
	char *end = nullptr;
	errno = 0;
	unsigned long long value = std::strtoull(ptr, &end, 10);
	if (errno != 0 || end == ptr)
		return false;
	while (*end != '\0' && std::isspace(static_cast<unsigned char>(*end)))
		++end;
	if (*end != '\0')
		return false;
	out = static_cast<uint64_t>(value);
	return true;
}

bool
IsLimitCoercionFunction(const std::string &name)
{
	return name == "int8" || name == "int4" || name == "int2" || name == "numeric" ||
		   name == "float8" || name == "float4";
}

bool
TryParseLimitExpression(const Expression *expr, uint64_t &out)
{
	if (expr == nullptr)
		return false;
	if (expr->type == ExpressionType::BOUND_CONSTANT)
		return TryParseUInt64(static_cast<const BoundConstantExpression *>(expr)->value, out);
	if (expr->type == ExpressionType::BOUND_FUNCTION)
	{
		const auto *func = static_cast<const BoundFunctionExpression *>(expr);
		if (func->children.size() == 1 && IsLimitCoercionFunction(func->function_name))
			return TryParseLimitExpression(func->children[0].get(), out);
	}
	return false;
}

bool
ConvertConstantToDatum(const BoundConstantExpression *constant, Oid target_type, int32 typmod, Datum &out)
{
	if (constant == nullptr || constant->is_null || !OidIsValid(target_type))
		return false;
	Oid input_func = InvalidOid;
	Oid ioparam = InvalidOid;
	getTypeInputInfo(target_type, &input_func, &ioparam);
	out = OidInputFunctionCall(input_func, const_cast<char *>(constant->value.c_str()), ioparam, typmod);
	return true;
}

bool
ScaleNumericDatumToTargetScale(Datum numeric_datum, int8_t target_scale, int64_t &out_value)
{
	int64_t factor = 1;
	if (!Pow10Int64(target_scale, factor))
		return false;
	Datum factor_numeric = NumericGetDatum(int64_to_numeric(factor));
	Datum scaled = DirectFunctionCall2(numeric_mul, numeric_datum, factor_numeric);
	out_value = DatumGetInt64(DirectFunctionCall1(numeric_int8, scaled));
	return true;
}

bool
ScaleNumericConstantToInt64(const BoundConstantExpression *constant, int8_t &out_scale, int64_t &out_value)
{
	if (constant == nullptr || constant->is_null)
		return false;
	const std::string &value = constant->value;
	if (!LooksLikeNumericLiteral(value))
		return false;
	size_t dot = value.find('.');
	out_scale = (dot == std::string::npos) ? 0 : static_cast<int8_t>(value.size() - dot - 1);
	Datum numeric_datum = 0;
	if (!ConvertConstantToDatum(constant, NUMERICOID, -1, numeric_datum))
		return false;
	return ScaleNumericDatumToTargetScale(numeric_datum, out_scale, out_value);
}

bool
ScaleNumericConstantToTargetScale(const BoundConstantExpression *constant, int8_t target_scale, int64_t &out_value)
{
	if (constant == nullptr || constant->is_null)
		return false;
	if (!LooksLikeNumericLiteral(constant->value))
		return false;
	Datum numeric_datum = 0;
	if (!ConvertConstantToDatum(constant, NUMERICOID, -1, numeric_datum))
		return false;
	return ScaleNumericDatumToTargetScale(numeric_datum, target_scale, out_value);
}

bool
ScaleIntegralConstantToTargetScale(const BoundConstantExpression *constant, int8_t target_scale, int64_t &out_value)
{
	if (constant == nullptr || constant->is_null || target_scale < 0)
		return false;
	int64_t base_value = 0;
	if (pg_strcasecmp(constant->value.c_str(), "true") == 0)
		base_value = 1;
	else if (pg_strcasecmp(constant->value.c_str(), "false") == 0)
		base_value = 0;
	else
	{
		const char *ptr = constant->value.c_str();
		char *end = nullptr;
		errno = 0;
		long long value = std::strtoll(ptr, &end, 10);
		if (errno != 0 || end == ptr)
			return false;
		while (*end != '\0' && std::isspace(static_cast<unsigned char>(*end)))
			++end;
		if (*end != '\0')
			return false;
		base_value = static_cast<int64_t>(value);
	}
	return RescaleInt64Constant(base_value, 0, target_scale, out_value);
}

bool
ScaleConstantToTargetScale(const BoundConstantExpression *constant, int8_t target_scale, int64_t &out_value)
{
	if (constant == nullptr || constant->is_null)
		return false;
	if (constant->value.find('.') != std::string::npos)
		return ScaleNumericConstantToTargetScale(constant, target_scale, out_value);
	return ScaleIntegralConstantToTargetScale(constant, target_scale, out_value);
}

bool
TryExtractShortStringConst(const BoundConstantExpression *constant, uint8_t &out_len, int64_t &out_value)
{
	if (constant == nullptr || constant->is_null)
		return false;
	const size_t len = constant->value.size();
	if (len > 8)
		return false;
	out_len = static_cast<uint8_t>(len);
	out_value = 0;
	if (len > 0)
		std::memcpy(&out_value, constant->value.data(), len);
	return true;
}

bool
StoreStringConstBytes(const BoundConstantExpression *constant,
					  Oid type_oid,
					  int32 typmod,
					  std::vector<char> &pool,
					  uint32_t &out_offset,
					  uint32_t &out_len,
					  uint64_t &out_inline_value)
{
	if (constant == nullptr || constant->is_null)
		return false;
	Datum datum = 0;
	if (!ConvertConstantToDatum(constant, type_oid, typmod, datum))
		return false;
	const char *ptr = nullptr;
	size_t len = 0;
	if (type_oid == BPCHAROID || type_oid == TEXTOID || type_oid == VARCHAROID)
	{
		text *txt = DatumGetTextPP(datum);
		ptr = VARDATA_ANY(txt);
		len = VARSIZE_ANY_EXHDR(txt);
	}
	else
	{
		ptr = constant->value.data();
		len = constant->value.size();
	}
	out_len = static_cast<uint32_t>(len);
	if (len <= sizeof(out_inline_value))
	{
		out_offset = UINT32_MAX;
		out_inline_value = 0;
		if (len > 0)
			std::memcpy(&out_inline_value, ptr, len);
		return true;
	}
	out_offset = static_cast<uint32_t>(pool.size());
	out_inline_value = 0;
	pool.insert(pool.end(), ptr, ptr + len);
	return true;
}

bool
ExtractCharFilterConst(const BoundConstantExpression *constant, int32_t &out_value)
{
	if (constant == nullptr || constant->is_null)
		return false;
	if (constant->value.empty())
		return false;
	out_value = static_cast<unsigned char>(constant->value[0]);
	return true;
}

bool
TryExtractLikePattern(const BoundConstantExpression *constant,
					  bool &out_prefix,
					  std::string &out_match)
{
	if (constant == nullptr || constant->is_null)
		return false;
	const std::string &pattern = constant->value;
	const size_t first_pct = pattern.find('%');
	const size_t first_us = pattern.find('_');
	if (first_us != std::string::npos)
		return false;
	if (first_pct == std::string::npos)
	{
		out_prefix = true;
		out_match = pattern;
		return true;
	}
	if (pattern.find('%', first_pct + 1) == std::string::npos)
	{
		if (first_pct == pattern.size() - 1)
		{
			out_prefix = true;
			out_match = pattern.substr(0, pattern.size() - 1);
			return true;
		}
		if (first_pct == 0)
		{
			out_prefix = false;
			out_match = pattern.substr(1);
			return !out_match.empty() && out_match.find('%') == std::string::npos;
		}
	}
	if (pattern.size() >= 2 && pattern.front() == '%' && pattern.back() == '%' &&
		pattern.substr(1, pattern.size() - 2).find('%') == std::string::npos)
	{
		out_prefix = false;
		out_match = pattern.substr(1, pattern.size() - 2);
		return !out_match.empty();
	}
	return false;
}

bool
EvaluateDateExpression(const Expression *expr, DateADT &out)
{
	if (expr == nullptr)
		return false;
	if (expr->type == ExpressionType::BOUND_CONSTANT)
	{
		Datum datum = 0;
		if (!ConvertConstantToDatum(static_cast<const BoundConstantExpression *>(expr), DATEOID, -1, datum))
			return false;
		out = DatumGetDateADT(datum);
		return true;
	}
	if (expr->type != ExpressionType::BOUND_FUNCTION)
		return false;
	const auto *func = static_cast<const BoundFunctionExpression *>(expr);
	if (func->children.size() != 2 || (func->function_name != "+" && func->function_name != "-"))
		return false;
	DateADT base = 0;
	if (!EvaluateDateExpression(func->children[0].get(), base))
		return false;
	const auto *interval_const = dynamic_cast<const BoundConstantExpression *>(func->children[1].get());
	if (interval_const == nullptr || interval_const->is_null)
		return false;
	Datum interval_datum = 0;
	if (!ConvertConstantToDatum(interval_const, INTERVALOID, -1, interval_datum))
		return false;
	Datum result = (func->function_name == "+")
		? DirectFunctionCall2(date_pl_interval, DateADTGetDatum(base), interval_datum)
		: DirectFunctionCall2(date_mi_interval, DateADTGetDatum(base), interval_datum);
	Timestamp ts = DatumGetTimestamp(result);
	int64 days = ts / USECS_PER_DAY;
	int64 rem = ts % USECS_PER_DAY;
	if (rem < 0)
	{
		days -= 1;
		rem += USECS_PER_DAY;
	}
	if (rem != 0)
		return false;
	out = static_cast<DateADT>(days);
	return true;
}

std::vector<bool>
ParseOrderDirections(const char *source_text, size_t nkeys)
{
	std::vector<bool> out(nkeys, true);
	if (source_text == nullptr || nkeys == 0)
		return out;
	std::string sql(source_text);
	std::string upper(sql);
	std::transform(upper.begin(), upper.end(), upper.begin(),
				   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
	size_t order_pos = upper.find("ORDER BY");
	if (order_pos == std::string::npos)
		return out;
	size_t end_pos = upper.find("LIMIT", order_pos);
	if (end_pos == std::string::npos)
		end_pos = sql.find(';', order_pos);
	if (end_pos == std::string::npos)
		end_pos = sql.size();
	std::string clause = sql.substr(order_pos + 8, end_pos - (order_pos + 8));
	int depth = 0;
	size_t item_idx = 0;
	size_t start = 0;
	for (size_t i = 0; i <= clause.size() && item_idx < nkeys; ++i)
	{
		const char ch = (i < clause.size()) ? clause[i] : ',';
		if (ch == '(')
			++depth;
		else if (ch == ')')
			--depth;
		else if (ch == ',' && depth == 0)
		{
			std::string item = clause.substr(start, i - start);
			std::string item_upper(item);
			std::transform(item_upper.begin(), item_upper.end(), item_upper.begin(),
						   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
			if (item_upper.find(" DESC") != std::string::npos)
				out[item_idx] = false;
			start = i + 1;
			++item_idx;
		}
	}
	return out;
}

bool
BuildAllTableColumnRefs(Oid relid, Index varno, std::vector<ColumnRef> &out_cols)
{
	Relation rel = relation_open(relid, AccessShareLock);
	TupleDesc tupdesc = RelationGetDescr(rel);
	out_cols.clear();
	for (int attidx = 0; attidx < tupdesc->natts; ++attidx)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attidx);
		if (attr->attisdropped)
			continue;
		out_cols.push_back(ColumnRef{varno, static_cast<AttrNumber>(attidx + 1)});
	}
	relation_close(rel, AccessShareLock);
	return !out_cols.empty();
}

bool
BuildProjectedTableColumnRefs(Oid relid,
							  Index varno,
							  const std::vector<yaap::ProjectionIndex> &projected_columns,
							  std::vector<ColumnRef> &out_cols)
{
	if (projected_columns.empty())
		return BuildAllTableColumnRefs(relid, varno, out_cols);

	Relation rel = relation_open(relid, AccessShareLock);
	TupleDesc tupdesc = RelationGetDescr(rel);
	out_cols.clear();
	for (const auto &projected : projected_columns)
	{
		const size_t attidx = projected.index;
		if (attidx >= static_cast<size_t>(tupdesc->natts))
		{
			relation_close(rel, AccessShareLock);
			out_cols.clear();
			return false;
		}
		Form_pg_attribute attr = TupleDescAttr(tupdesc, static_cast<int>(attidx));
		if (attr->attisdropped)
		{
			relation_close(rel, AccessShareLock);
			out_cols.clear();
			return false;
		}
		out_cols.push_back(ColumnRef{varno, static_cast<AttrNumber>(attidx + 1)});
	}
	relation_close(rel, AccessShareLock);
	return !out_cols.empty();
}

bool
LookupOrAddScanFilterInput(const ColumnSchema &col,
						   std::vector<FilterInputDesc> &inputs,
						   uint16_t &out_idx)
{
	for (size_t i = 0; i < inputs.size(); ++i)
	{
		if (inputs[i].attno == static_cast<uint16_t>(col.src_attno) &&
			inputs[i].decode_kind == col.decode_kind &&
			inputs[i].source_decode_kind == col.decode_kind)
		{
			out_idx = static_cast<uint16_t>(i);
			return true;
		}
	}
	if (inputs.size() >= pg_yaap::pipeline::FILTER_MAX_INPUTS || col.src_attno <= 0)
		return false;
	uint8_t numeric_scale = 0;
	if (col.decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
	{
		int8_t scale = 0;
		if (!ColumnNumericScale(col, scale))
			return false;
		numeric_scale = static_cast<uint8_t>(std::max<int>(0, scale));
	}
	out_idx = static_cast<uint16_t>(inputs.size());
	inputs.push_back(FilterInputDesc{
		static_cast<uint16_t>(col.src_attno),
		static_cast<uint8_t>(out_idx),
		col.decode_kind,
		col.decode_kind,
		numeric_scale,
		0});
	return true;
}

bool
LookupOrAddScanFilterInputAs(const ColumnSchema &col,
							 ColumnDecodeKind target_decode_kind,
							 uint8_t target_numeric_scale,
							 std::vector<FilterInputDesc> &inputs,
							 uint16_t &out_idx)
{
	if (col.src_attno <= 0 || inputs.size() >= pg_yaap::pipeline::FILTER_MAX_INPUTS)
		return false;

	FilterInputDesc desc{
		static_cast<uint16_t>(col.src_attno),
		0,
		target_decode_kind,
		col.decode_kind,
		target_numeric_scale,
		0};
	if (target_decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED &&
		col.decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
	{
		int8_t scale = 0;
		if (!ColumnNumericScale(col, scale))
			return false;
		desc.numeric_scale = static_cast<uint8_t>(std::max<int>(0, scale));
	}

	for (size_t i = 0; i < inputs.size(); ++i)
	{
		const auto &existing = inputs[i];
		if (existing.attno == desc.attno &&
			existing.decode_kind == desc.decode_kind &&
			existing.source_decode_kind == desc.source_decode_kind &&
			existing.numeric_scale == desc.numeric_scale)
		{
			out_idx = static_cast<uint16_t>(i);
			return true;
		}
	}
	desc.dst_col = static_cast<uint8_t>(inputs.size());
	out_idx = static_cast<uint16_t>(inputs.size());
	inputs.push_back(desc);
	return true;
}

bool
LookupOrAddJoinFilterInput(const ColumnRef &ref,
						   const std::vector<ColumnRef> &left_cols,
						   const std::vector<ColumnSchema> &left_schema,
						   const std::vector<ColumnRef> &right_cols,
						   const std::vector<ColumnSchema> &right_schema,
						   std::vector<HashJoinFilterInputDesc> &inputs,
						   uint16_t &out_idx,
						   const ColumnSchema *&out_col)
{
	HashJoinFilterInputDesc desc{};
	if (LookupRawColumn(ref, left_cols, left_schema, out_col))
	{
		desc.side = HashJoinOutputSide::LEFT;
		desc.input_chunk_slot = out_col->chunk_slot;
		desc.decode_kind = out_col->decode_kind;
		desc.source_decode_kind = out_col->decode_kind;
		desc.numeric_scale = 0;
	}
	else if (LookupRawColumn(ref, right_cols, right_schema, out_col))
	{
		desc.side = HashJoinOutputSide::RIGHT;
		desc.input_chunk_slot = out_col->chunk_slot;
		desc.decode_kind = out_col->decode_kind;
		desc.source_decode_kind = out_col->decode_kind;
		desc.numeric_scale = 0;
	}
	else
		return false;

	if (desc.decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
	{
		int8_t scale = 0;
		if (!ColumnNumericScale(*out_col, scale))
			return false;
		desc.numeric_scale = static_cast<uint8_t>(std::max<int>(0, scale));
	}

	for (size_t i = 0; i < inputs.size(); ++i)
	{
		const auto &existing = inputs[i];
		if (existing.side == desc.side &&
			existing.input_chunk_slot == desc.input_chunk_slot &&
			existing.decode_kind == desc.decode_kind &&
			existing.source_decode_kind == desc.source_decode_kind &&
			existing.numeric_scale == desc.numeric_scale)
		{
			out_idx = static_cast<uint16_t>(i);
			return true;
		}
	}
	if (inputs.size() >= pg_yaap::pipeline::FILTER_MAX_INPUTS)
		return false;
	out_idx = static_cast<uint16_t>(inputs.size());
	inputs.push_back(desc);
	return true;
}

bool
LookupOrAddJoinFilterInputAs(const ColumnRef &ref,
							 const std::vector<ColumnRef> &left_cols,
							 const std::vector<ColumnSchema> &left_schema,
							 const std::vector<ColumnRef> &right_cols,
							 const std::vector<ColumnSchema> &right_schema,
							 ColumnDecodeKind target_decode_kind,
							 uint8_t target_numeric_scale,
							 std::vector<HashJoinFilterInputDesc> &inputs,
							 uint16_t &out_idx,
							 const ColumnSchema *&out_col)
{
	HashJoinFilterInputDesc desc{};
	if (LookupRawColumn(ref, left_cols, left_schema, out_col))
		desc.side = HashJoinOutputSide::LEFT;
	else if (LookupRawColumn(ref, right_cols, right_schema, out_col))
		desc.side = HashJoinOutputSide::RIGHT;
	else
		return false;

	desc.input_chunk_slot = out_col->chunk_slot;
	desc.decode_kind = target_decode_kind;
	desc.source_decode_kind = out_col->decode_kind;
	desc.numeric_scale = target_numeric_scale;
	if (target_decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED &&
		desc.source_decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
	{
		int8_t scale = 0;
		if (!ColumnNumericScale(*out_col, scale))
			return false;
		desc.numeric_scale = static_cast<uint8_t>(std::max<int>(0, scale));
	}

	for (size_t i = 0; i < inputs.size(); ++i)
	{
		const auto &existing = inputs[i];
		if (existing.side == desc.side &&
			existing.input_chunk_slot == desc.input_chunk_slot &&
			existing.decode_kind == desc.decode_kind &&
			existing.source_decode_kind == desc.source_decode_kind &&
			existing.numeric_scale == desc.numeric_scale)
		{
			out_idx = static_cast<uint16_t>(i);
			return true;
		}
	}
	if (inputs.size() >= pg_yaap::pipeline::FILTER_MAX_INPUTS)
		return false;
	out_idx = static_cast<uint16_t>(inputs.size());
	inputs.push_back(desc);
	return true;
}

}  // namespace pg_yaap::optimizer_translator_detail
