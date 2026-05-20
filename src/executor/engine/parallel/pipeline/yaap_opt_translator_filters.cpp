#include "parallel/pipeline/yaap_pipeline_init_internal.hpp"

#include <iomanip>
#include <sstream>

namespace pg_yaap::optimizer_translator_detail {

static int
ExprTypeForLog(const Expression *expr)
{
	return expr != nullptr ? static_cast<int>(expr->type) : -1;
}

static bool
TryParsePositiveIntConstant(const Expression *expr, int &out_value)
{
	const auto *constant = dynamic_cast<const BoundConstantExpression *>(expr);
	if (constant == nullptr || constant->is_null)
		return false;
	char *end = nullptr;
	errno = 0;
	long parsed = std::strtol(constant->value.c_str(), &end, 10);
	if (errno != 0 || end == constant->value.c_str())
		return false;
	while (*end != '\0' && std::isspace(static_cast<unsigned char>(*end)))
		++end;
	if (*end != '\0' || parsed <= 0 || parsed > std::numeric_limits<int>::max())
		return false;
	out_value = static_cast<int>(parsed);
	return true;
}

static std::string
ExpressionSemanticKey(const Expression *expression)
{
	if (expression == nullptr)
		return "<null>";

	std::stringstream ss;
	switch (expression->type)
	{
		case ExpressionType::BOUND_COLUMN_REF:
		{
			const auto *column = static_cast<const BoundColumnRefExpression *>(expression);
			ss << "col:" << column->binding.table_index.index << "." << column->binding.column_index.index;
			break;
		}
		case ExpressionType::BOUND_CONSTANT:
		{
			const auto *constant = static_cast<const BoundConstantExpression *>(expression);
			ss << "const:" << (constant->is_null ? "NULL" : constant->value);
			break;
		}
		case ExpressionType::BOUND_FUNCTION:
		{
			const auto *function = static_cast<const BoundFunctionExpression *>(expression);
			ss << "fn:" << function->function_name << "(";
			for (size_t i = 0; i < function->children.size(); ++i)
			{
				if (i > 0)
					ss << ",";
				ss << ExpressionSemanticKey(function->children[i].get());
			}
			ss << ")";
			break;
		}
		case ExpressionType::BOUND_AGGREGATE:
		{
			const auto *aggregate = static_cast<const BoundAggregateExpression *>(expression);
			ss << "agg:" << aggregate->function_name << "(";
			for (size_t i = 0; i < aggregate->children.size(); ++i)
			{
				if (i > 0)
					ss << ",";
				ss << ExpressionSemanticKey(aggregate->children[i].get());
			}
			ss << ")";
			break;
		}
		case ExpressionType::BOUND_CONJUNCTION:
		{
			const auto *conjunction = static_cast<const BoundConjunctionExpression *>(expression);
			ss << "conj:" << conjunction->bool_expr_type << "(";
			for (size_t i = 0; i < conjunction->children.size(); ++i)
			{
				if (i > 0)
					ss << ",";
				ss << ExpressionSemanticKey(conjunction->children[i].get());
			}
			ss << ")";
			break;
		}
		case ExpressionType::BOUND_SUBQUERY:
		{
			const auto *subquery = static_cast<const yaap::BoundSubqueryExpression *>(expression);
			ss << "subquery:" << subquery->sublink_name << "(";
			for (size_t i = 0; i < subquery->children.size(); ++i)
			{
				if (i > 0)
					ss << ",";
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

static bool
PatternUsesOnlyPercentWildcards(const BoundConstantExpression *constant)
{
	return constant != nullptr &&
		!constant->is_null &&
		constant->value.find('_') == std::string::npos;
}

static bool
LookupAggregateOutputColumn(const Expression *expr,
							const PhysicalOperator *source_op,
							const std::vector<ColumnRef> &cols,
							const std::vector<ColumnSchema> &schema,
							ColumnRef &out_ref,
							const ColumnSchema *&out_col)
{
	expr = UnwrapTransparentCastExpr(expr);
	if (expr == nullptr || expr->type != ExpressionType::BOUND_AGGREGATE || source_op == nullptr)
		return false;
	if (source_op->type != PhysicalOperatorType::HASH_GROUP_BY)
		return false;

	const auto *aggregate = static_cast<const PhysicalHashAggregate *>(source_op);
	const std::string fingerprint = ExpressionSemanticKey(expr);
	for (size_t idx = 0; idx < aggregate->expressions.size(); ++idx)
	{
		if (ExpressionSemanticKey(aggregate->expressions[idx]) != fingerprint)
			continue;
		out_ref = ColumnRef{
			static_cast<Index>(aggregate->aggregate_index.index + 1),
			static_cast<AttrNumber>(idx + 1)};
		return LookupRawColumn(out_ref, cols, schema, out_col);
	}
	return false;
}

static const BoundColumnRefExpression *
UnwrapPrefixBaseColumn(const Expression *expr)
{
	if (expr == nullptr)
		return nullptr;
	if (const auto *column = dynamic_cast<const BoundColumnRefExpression *>(expr))
		return column;
	const auto *func = dynamic_cast<const BoundFunctionExpression *>(expr);
	if (func == nullptr || func->children.size() != 1)
		return nullptr;
	if (func->function_name != "text" &&
		func->function_name != "varchar" &&
		func->function_name != "bpchar" &&
		func->function_name != "char")
		return nullptr;
	return UnwrapPrefixBaseColumn(func->children[0].get());
}

static bool
IsPrefixSliceExpr(const Expression *expr,
				  const BoundColumnRefExpression *&out_base,
				  int &out_len)
{
	const auto *func = dynamic_cast<const BoundFunctionExpression *>(expr);
	if (func == nullptr || func->function_name != "prefix_slice" || func->children.size() != 2)
		return false;
	out_base = UnwrapPrefixBaseColumn(func->children[0].get());
	if (out_base == nullptr)
		return false;
	return TryParsePositiveIntConstant(func->children[1].get(), out_len);
}

bool
AppendFilterExpr(std::vector<FilterExprDesc> &exprs,
				 const std::vector<FilterStep> &steps,
				 size_t first_step_idx,
				 uint16_t out_bool_reg)
{
	if (steps.size() < first_step_idx || exprs.size() >= pg_yaap::pipeline::FILTER_MAX_INPUTS)
		return false;
	exprs.push_back(FilterExprDesc{
		static_cast<uint16_t>(first_step_idx),
		static_cast<uint16_t>(steps.size() - first_step_idx),
		out_bool_reg,
		0});
	return true;
}

bool
LowerScanFilterBoolExpr(const Expression *expr,
						const std::vector<yaap::PhysicalOperator::OutputColumn> *outputs,
						const std::vector<ColumnRef> &cols,
						const std::vector<ColumnSchema> &schema,
						std::vector<FilterInputDesc> &inputs,
						std::vector<FilterStep> &steps,
						std::vector<char> &string_consts,
						uint16_t &next_bool_reg,
						uint16_t &out_bool_reg);

static bool
ResolveScanFilterColumn(const BoundColumnRefExpression *expr,
						const std::vector<yaap::PhysicalOperator::OutputColumn> *outputs,
						const std::vector<ColumnRef> &cols,
						const std::vector<ColumnSchema> &schema,
						ColumnRef &out_ref,
						const ColumnSchema *&out_col)
{
	if (outputs != nullptr)
	{
		if (LookupNamedExprInputColumn(expr, outputs, cols, schema, out_ref, out_col))
			return true;
		return false;
	}
	return LookupBindingColumn(expr->binding, cols, schema, out_ref, out_col) && out_col != nullptr;
}

bool
LowerJoinFilterBoolExpr(const Expression *expr,
						const PhysicalOperator *left_source_op,
						const PhysicalOperator *right_source_op,
						const std::vector<yaap::PhysicalOperator::OutputColumn> *left_outputs,
						const std::vector<ColumnRef> &left_cols,
						const std::vector<ColumnSchema> &left_schema,
						const std::vector<yaap::PhysicalOperator::OutputColumn> *right_outputs,
						const std::vector<ColumnRef> &right_cols,
						const std::vector<ColumnSchema> &right_schema,
						std::vector<HashJoinFilterInputDesc> &inputs,
						std::vector<FilterStep> &steps,
						std::vector<char> &string_consts,
						uint16_t &next_bool_reg,
						uint16_t &out_bool_reg);

static const BoundConstantExpression *
UnwrapTransparentConstant(const Expression *expr)
{
	expr = UnwrapTransparentCastExpr(expr);
	if (const auto *constant = dynamic_cast<const BoundConstantExpression *>(expr))
		return constant;
	const auto *func = dynamic_cast<const BoundFunctionExpression *>(expr);
	if (func == nullptr || !IsTransparentCastFunctionName(func->function_name) || func->children.empty())
		return nullptr;
	return UnwrapTransparentConstant(func->children[0].get());
}

static bool
ParseNumericConstantValue(const BoundConstantExpression *constant, long double &out)
{
	if (constant == nullptr || constant->is_null)
		return false;
	char *end = nullptr;
	errno = 0;
	const long double value = strtold(constant->value.c_str(), &end);
	if (errno != 0 || end == constant->value.c_str() || (end != nullptr && *end != '\0'))
		return false;
	out = value;
	return true;
}

static std::unique_ptr<BoundConstantExpression>
FoldNumericConstantExpression(const Expression *expr)
{
	if (const auto *constant = UnwrapTransparentConstant(expr))
		return std::make_unique<BoundConstantExpression>(constant->value, constant->is_null);

	const auto *func = dynamic_cast<const BoundFunctionExpression *>(UnwrapTransparentCastExpr(expr));
	if (func == nullptr || func->children.size() != 2)
		return nullptr;

	auto left = FoldNumericConstantExpression(func->children[0].get());
	auto right = FoldNumericConstantExpression(func->children[1].get());
	long double left_value = 0;
	long double right_value = 0;
	if (!ParseNumericConstantValue(left.get(), left_value) ||
		!ParseNumericConstantValue(right.get(), right_value))
		return nullptr;

	long double result = 0;
	if (func->function_name == "+")
		result = left_value + right_value;
	else if (func->function_name == "-")
		result = left_value - right_value;
	else if (func->function_name == "*")
		result = left_value * right_value;
	else
		return nullptr;

	std::ostringstream ss;
	ss << std::setprecision(24) << result;
	return std::make_unique<BoundConstantExpression>(ss.str(), false);
}

bool
LowerScanFilterCompare(const BoundFunctionExpression *func,
					   const std::vector<yaap::PhysicalOperator::OutputColumn> *outputs,
					   const std::vector<ColumnRef> &cols,
					   const std::vector<ColumnSchema> &schema,
					   std::vector<FilterInputDesc> &inputs,
					   std::vector<FilterStep> &steps,
					   std::vector<char> &string_consts,
					   uint16_t &next_bool_reg,
					   uint16_t &out_bool_reg)
{
	const bool negate_like = func != nullptr && func->function_name == "!~~";
	if (func == nullptr || func->children.size() != 2 ||
		(!IsComparisonName(func->function_name) && !negate_like))
		return false;
	const auto *left_col = UnwrapTransparentCastColumn(func->children[0].get());
	const auto *right_col = UnwrapTransparentCastColumn(func->children[1].get());

	FilterStep step{};
	step._pad0 = 0;
	step.const_offset = UINT32_MAX;
	step.const_len = 0;
	step.const_value = 0;
	step.right_idx = 0;
	step.out_bool_reg = next_bool_reg++;

	auto finish_step = [&]() -> bool
	{
		out_bool_reg = step.out_bool_reg;
		steps.push_back(step);
		if (!negate_like)
			return true;
		if (next_bool_reg >= pg_yaap::pipeline::FILTER_MAX_BOOL_REGS)
			return false;
		out_bool_reg = next_bool_reg++;
		steps.push_back(FilterStep{FilterStepOp::BOOL_NOT, QualOp::EQ, step.out_bool_reg, 0, out_bool_reg, 0, UINT32_MAX, 0, 0});
		return true;
	};

	auto build_const = [&](const BoundColumnRefExpression *col_expr,
						   const Expression *const_expr,
						   bool constant_on_right) -> bool
	{
		const_expr = UnwrapTransparentCastExpr(const_expr);
		auto folded_constant = FoldNumericConstantExpression(const_expr);
		const auto *constant = folded_constant.get();
		ColumnRef ref{};
		const ColumnSchema *col = nullptr;
		if (!ResolveScanFilterColumn(col_expr, outputs, cols, schema, ref, col) || col == nullptr)
			return false;
		if (!LookupOrAddScanFilterInput(*col, inputs, step.left_idx))
			return false;

		if (col->decode_kind == ColumnDecodeKind::INT32_DATE)
		{
			DateADT date_const = 0;
			if (!EvaluateDateExpression(const_expr, date_const) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT32_CMP_CONST;
			step.const_value = static_cast<uint64_t>(DateADTGetDatum(date_const));
			return true;
		}
		if (constant == nullptr)
			return false;
		if (col->decode_kind == ColumnDecodeKind::INT32_INT4)
		{
			Datum datum = 0;
			if (!ConvertConstantToDatum(constant, INT4OID, -1, datum) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT32_CMP_CONST;
			step.const_value = static_cast<uint64_t>(DatumGetInt32(datum));
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::INT32_CHAR)
		{
			int32_t ch = 0;
			if (!ExtractCharFilterConst(constant, ch) || !MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT32_CMP_CONST;
			step.const_value = static_cast<uint64_t>(static_cast<uint32_t>(ch));
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::INT64_INT8)
		{
			Datum datum = 0;
			if (!ConvertConstantToDatum(constant, INT8OID, -1, datum) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT64_CMP_CONST;
			step.const_value = static_cast<uint64_t>(DatumGetInt64(datum));
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
		{
			int64_t value = 0;
			const int8_t scale = static_cast<int8_t>(ExtractNumericTypmodScale(col->typmod));
			if (!ScaleNumericConstantToTargetScale(constant, scale, value) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT64_CMP_CONST;
			step.const_value = static_cast<uint64_t>(value);
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::STRING_REF)
		{
			if (func->function_name == "=")
				step.op = FilterStepOp::STRING_EQ_CONST;
			else if (func->function_name == "<>" || func->function_name == "!=")
				step.op = FilterStepOp::STRING_NE_CONST;
			else if (func->function_name == "~~" || func->function_name == "!~~")
			{
				bool prefix = false;
				std::string match;
				if (TryExtractLikePattern(constant, prefix, match))
				{
					auto pattern_const = BoundConstantExpression(match, false);
					if (!StoreStringConstBytes(&pattern_const,
											   TEXTOID,
											   -1,
											   string_consts,
											   step.const_offset,
											   step.const_len,
											   step.const_value))
						return false;
					step.op = prefix ? FilterStepOp::STRING_PREFIX_LIKE : FilterStepOp::STRING_CONTAINS_LIKE;
				}
				else
				{
					if (!PatternUsesOnlyPercentWildcards(constant) ||
						!StoreStringConstBytes(constant,
											  TEXTOID,
											  -1,
											  string_consts,
											  step.const_offset,
											  step.const_len,
											  step.const_value))
						return false;
					step.op = FilterStepOp::STRING_SQL_LIKE;
				}
				step.cmp_op = QualOp::EQ;
				return true;
			}
			else
				return false;
			if (!StoreStringConstBytes(constant,
									  col->type_oid,
									  col->typmod,
									  string_consts,
									  step.const_offset,
									  step.const_len,
									  step.const_value))
				return false;
			step.cmp_op = QualOp::EQ;
			return true;
		}
		return false;
	};

	auto build_prefix_slice_const = [&](const Expression *prefix_expr,
										const Expression *const_expr) -> bool
	{
		const BoundColumnRefExpression *base_col = nullptr;
		int prefix_len = 0;
		const auto *constant = dynamic_cast<const BoundConstantExpression *>(const_expr);
		ColumnRef ref{};
		const ColumnSchema *col = nullptr;
		if (func->function_name != "=")
			return false;
		if (!IsPrefixSliceExpr(prefix_expr, base_col, prefix_len))
		{
			if (pg_yaap_trace_hooks)
			{
				const auto *prefix_func = dynamic_cast<const BoundFunctionExpression *>(prefix_expr);
				elog(LOG, "pg_yaap: prefix compare rejected IsPrefixSliceExpr fn=%s arg0_type=%d arg1_type=%d",
					 prefix_func != nullptr ? prefix_func->function_name.c_str() : "<non-fn>",
					 prefix_func != nullptr && !prefix_func->children.empty() ? ExprTypeForLog(prefix_func->children[0].get()) : -1,
					 prefix_func != nullptr && prefix_func->children.size() > 1 ? ExprTypeForLog(prefix_func->children[1].get()) : -1);
			}
			return false;
		}
		if (constant == nullptr || constant->is_null)
			return false;
		if (!ResolveScanFilterColumn(base_col, outputs, cols, schema, ref, col) ||
			col == nullptr || col->decode_kind != ColumnDecodeKind::STRING_REF)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: prefix compare rejected binding attno=%d found=%d decode_kind=%d",
					 static_cast<int>(ref.attno),
					 col != nullptr ? 1 : 0,
					 col != nullptr ? static_cast<int>(col->decode_kind) : -1);
			return false;
		}

		std::string prefix = constant->value;
		if (static_cast<int>(prefix.size()) < prefix_len)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: prefix compare rejected const_len=%zu prefix_len=%d value=%s",
					 prefix.size(), prefix_len, prefix.c_str());
			return false;
		}
		prefix.resize(prefix_len);

		if (!LookupOrAddScanFilterInput(*col, inputs, step.left_idx))
			return false;

		auto pattern_const = BoundConstantExpression(prefix, false);
		if (!StoreStringConstBytes(&pattern_const,
								   TEXTOID,
								   -1,
								   string_consts,
								   step.const_offset,
								   step.const_len,
								   step.const_value))
			return false;
		step.op = FilterStepOp::STRING_PREFIX_LIKE;
		step.cmp_op = QualOp::EQ;
		return true;
	};

	if (left_col != nullptr && right_col == nullptr && build_const(left_col, func->children[1].get(), true))
		return finish_step();
	if (build_prefix_slice_const(func->children[0].get(), func->children[1].get()))
		return finish_step();
	if (right_col != nullptr && left_col == nullptr && build_const(right_col, func->children[0].get(), false))
	{
		if (func->function_name == "<")
			step.cmp_op = QualOp::GT;
		else if (func->function_name == "<=")
			step.cmp_op = QualOp::GE;
		else if (func->function_name == ">")
			step.cmp_op = QualOp::LT;
		else if (func->function_name == ">=")
			step.cmp_op = QualOp::LE;
		return finish_step();
	}
	if (build_prefix_slice_const(func->children[1].get(), func->children[0].get()))
		return finish_step();
	auto log_compare_failure = [&]()
	{
		if (!pg_yaap_trace_hooks)
			return;
		ColumnRef lhs_ref{};
		ColumnRef rhs_ref{};
		const ColumnSchema *lhs_schema = nullptr;
		const ColumnSchema *rhs_schema = nullptr;
		bool lhs_found = left_col != nullptr &&
			ResolveScanFilterColumn(left_col, outputs, cols, schema, lhs_ref, lhs_schema) &&
			lhs_schema != nullptr;
		bool rhs_found = right_col != nullptr &&
			ResolveScanFilterColumn(right_col, outputs, cols, schema, rhs_ref, rhs_schema) &&
			rhs_schema != nullptr;
		const auto *lhs_func = dynamic_cast<const BoundFunctionExpression *>(func->children[0].get());
		const auto *rhs_func = dynamic_cast<const BoundFunctionExpression *>(func->children[1].get());
		elog(LOG,
			 "pg_yaap: scan compare lowering failed fn=%s lhs_type=%d lhs_fn=%s rhs_type=%d rhs_fn=%s lhs_found=%d lhs_binding=(%zu,%zu) lhs_name=%s.%s lhs_ref=(%u,%d) lhs_decode=%d rhs_found=%d rhs_binding=(%zu,%zu) rhs_name=%s.%s rhs_ref=(%u,%d) rhs_decode=%d cols=%zu schema=%zu outputs=%zu",
			 func->function_name.c_str(),
			 ExprTypeForLog(func->children[0].get()),
			 lhs_func != nullptr ? lhs_func->function_name.c_str() : "<non-fn>",
			 ExprTypeForLog(func->children[1].get()),
			 rhs_func != nullptr ? rhs_func->function_name.c_str() : "<non-fn>",
			 lhs_found ? 1 : 0,
			 left_col != nullptr ? left_col->binding.table_index.index : 0,
			 left_col != nullptr ? left_col->binding.column_index.index : 0,
			 left_col != nullptr ? left_col->table_name.c_str() : "",
			 left_col != nullptr ? left_col->column_name.c_str() : "",
			 lhs_ref.varno,
			 lhs_ref.attno,
			 lhs_schema != nullptr ? static_cast<int>(lhs_schema->decode_kind) : -1,
			 rhs_found ? 1 : 0,
			 right_col != nullptr ? right_col->binding.table_index.index : 0,
			 right_col != nullptr ? right_col->binding.column_index.index : 0,
			 right_col != nullptr ? right_col->table_name.c_str() : "",
			 right_col != nullptr ? right_col->column_name.c_str() : "",
			 rhs_ref.varno,
			 rhs_ref.attno,
			 rhs_schema != nullptr ? static_cast<int>(rhs_schema->decode_kind) : -1,
			 cols.size(),
			 schema.size(),
			 outputs != nullptr ? outputs->size() : 0);
		if (outputs != nullptr)
		{
			for (size_t i = 0; i < outputs->size() && i < cols.size(); ++i)
				elog(LOG,
					 "pg_yaap: scan compare output[%zu] binding=(%zu,%zu) name=%s.%s ref=(%u,%d)",
					 i,
					 (*outputs)[i].binding.table_index.index,
					 (*outputs)[i].binding.column_index.index,
					 (*outputs)[i].table_name.c_str(),
					 (*outputs)[i].column_name.c_str(),
					 cols[i].varno,
					 cols[i].attno);
		}
	};

	if (left_col == nullptr || right_col == nullptr)
	{
		log_compare_failure();
		return false;
	}

	ColumnRef left_ref{};
	ColumnRef right_ref{};
	const ColumnSchema *left_schema = nullptr;
	const ColumnSchema *right_schema = nullptr;
	bool left_found = ResolveScanFilterColumn(left_col, outputs, cols, schema, left_ref, left_schema) &&
		left_schema != nullptr;
	bool right_found = ResolveScanFilterColumn(right_col, outputs, cols, schema, right_ref, right_schema) &&
		right_schema != nullptr;
	if (!left_found || !right_found ||
		left_schema == nullptr || right_schema == nullptr ||
		!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
	{
		log_compare_failure();
		return false;
	}
	auto is_integral_numeric = [](ColumnDecodeKind kind)
	{
		return kind == ColumnDecodeKind::INT32_INT4 || kind == ColumnDecodeKind::INT64_INT8;
	};
	auto is_numeric_family = [&](ColumnDecodeKind kind)
	{
		return is_integral_numeric(kind) || kind == ColumnDecodeKind::INT64_NUMERIC_SCALED;
	};
	if (left_schema->decode_kind != right_schema->decode_kind)
	{
		if (!(is_numeric_family(left_schema->decode_kind) && is_numeric_family(right_schema->decode_kind)))
		{
			log_compare_failure();
			return false;
		}
		int8_t left_scale = 0;
		int8_t right_scale = 0;
		if (left_schema->decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED &&
			!ColumnNumericScale(*left_schema, left_scale))
		{
			log_compare_failure();
			return false;
		}
		if (right_schema->decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED &&
			!ColumnNumericScale(*right_schema, right_scale))
		{
			log_compare_failure();
			return false;
		}
		const uint8_t target_scale = static_cast<uint8_t>(std::max<int>(left_scale, right_scale));
		if (!LookupOrAddScanFilterInputAs(*left_schema,
										 ColumnDecodeKind::INT64_NUMERIC_SCALED,
										 target_scale,
										 inputs,
										 step.left_idx) ||
			!LookupOrAddScanFilterInputAs(*right_schema,
										 ColumnDecodeKind::INT64_NUMERIC_SCALED,
										 target_scale,
										 inputs,
										 step.right_idx))
		{
			log_compare_failure();
			return false;
		}
		step.op = FilterStepOp::INT64_CMP_VAR;
		return finish_step();
	}
	if (left_schema->decode_kind == ColumnDecodeKind::INT32_DATE ||
		left_schema->decode_kind == ColumnDecodeKind::INT32_INT4 ||
		left_schema->decode_kind == ColumnDecodeKind::INT32_CHAR)
		step.op = FilterStepOp::INT32_CMP_VAR;
	else if (left_schema->decode_kind == ColumnDecodeKind::INT64_INT8 ||
			 left_schema->decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
		step.op = FilterStepOp::INT64_CMP_VAR;
	else
	{
		log_compare_failure();
		return false;
	}
	if (!LookupOrAddScanFilterInput(*left_schema, inputs, step.left_idx) ||
		!LookupOrAddScanFilterInput(*right_schema, inputs, step.right_idx))
	{
		log_compare_failure();
		return false;
	}
	return finish_step();
}

static bool
LowerScanFilterAnyOrAll(const BoundFunctionExpression *func,
						const std::vector<yaap::PhysicalOperator::OutputColumn> *outputs,
						const std::vector<ColumnRef> &cols,
						const std::vector<ColumnSchema> &schema,
						std::vector<FilterInputDesc> &inputs,
						std::vector<FilterStep> &steps,
						std::vector<char> &string_consts,
						uint16_t &next_bool_reg,
						uint16_t &out_bool_reg)
{
	if (func == nullptr || func->children.size() != 2 ||
		(func->function_name != "any" && func->function_name != "all"))
		return false;
	if (func->children[0] == nullptr || func->children[1] == nullptr)
		return false;

	const auto *array = dynamic_cast<const BoundFunctionExpression *>(func->children[1].get());
	if (array == nullptr || array->function_name != "array" || array->children.empty())
	{
		if (pg_yaap_trace_hooks && array != nullptr)
			elog(LOG, "pg_yaap: any/all lowering unsupported rhs fn=%s children=%zu",
				 array->function_name.c_str(), array->children.size());
		return false;
	}

	const BoundColumnRefExpression *prefix_base = nullptr;
	int prefix_len = 0;
	const auto *lhs_col = dynamic_cast<const BoundColumnRefExpression *>(func->children[0].get());
	const bool lhs_is_prefix = IsPrefixSliceExpr(func->children[0].get(), prefix_base, prefix_len);
	if (lhs_col == nullptr && !lhs_is_prefix)
	{
		if (pg_yaap_trace_hooks)
		{
			const auto *lhs_func = dynamic_cast<const BoundFunctionExpression *>(func->children[0].get());
			elog(LOG, "pg_yaap: any/all lowering unsupported lhs type=%d fn=%s",
				 static_cast<int>(func->children[0]->type),
				 lhs_func != nullptr ? lhs_func->function_name.c_str() : "<non-fn>");
		}
		return false;
	}

	char *compare_name_cstr = func->op_oid == InvalidOid ? nullptr : get_opname(func->op_oid);
	if (compare_name_cstr == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: any/all lowering missing compare op oid=%d", func->op_oid);
		return false;
	}
	const std::string compare_name(compare_name_cstr);

	auto lower_member = [&](const BoundConstantExpression *constant, uint16_t &member_reg) -> bool
	{
		if (constant == nullptr || constant->is_null)
			return false;
		if (lhs_is_prefix)
		{
			if (compare_name != "=" || static_cast<int>(constant->value.size()) != prefix_len)
			{
				if (pg_yaap_trace_hooks)
					elog(LOG, "pg_yaap: any/all prefix lowering rejected compare=%s const_len=%zu prefix_len=%d",
						 compare_name.c_str(), constant->value.size(), prefix_len);
				return false;
			}
			BoundFunctionExpression like_expr("~~", InvalidOid);
			like_expr.children.push_back(
				std::make_unique<BoundColumnRefExpression>(prefix_base->binding,
														   prefix_base->table_name,
														   prefix_base->column_name));
			like_expr.children.push_back(
				std::make_unique<BoundConstantExpression>(constant->value + "%", false));
			return LowerScanFilterCompare(&like_expr,
										  outputs,
										  cols, schema, inputs, steps, string_consts,
										  next_bool_reg, member_reg);
		}

		BoundFunctionExpression compare_expr(compare_name, func->op_oid);
		compare_expr.children.push_back(
			std::make_unique<BoundColumnRefExpression>(lhs_col->binding,
													   lhs_col->table_name,
													   lhs_col->column_name));
		compare_expr.children.push_back(
			std::make_unique<BoundConstantExpression>(constant->value, constant->is_null));
		return LowerScanFilterCompare(&compare_expr,
									  outputs,
									  cols, schema, inputs, steps, string_consts,
									  next_bool_reg, member_reg);
	};

	uint16_t accum_reg = 0;
	bool first = true;
	for (size_t member_idx = 0; member_idx < array->children.size(); ++member_idx)
	{
		const auto &member = array->children[member_idx];
		const auto *constant = dynamic_cast<const BoundConstantExpression *>(member.get());
		uint16_t member_reg = 0;
		if (!lower_member(constant, member_reg))
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: any/all lowering failed member idx=%zu", member_idx);
			return false;
		}
		if (first)
		{
			accum_reg = member_reg;
			first = false;
			continue;
		}
		if (next_bool_reg >= pg_yaap::pipeline::FILTER_MAX_BOOL_REGS)
			return false;
		const uint16_t combined_reg = next_bool_reg++;
		steps.push_back(FilterStep{
			func->function_name == "all" ? FilterStepOp::BOOL_AND : FilterStepOp::BOOL_OR,
			QualOp::EQ,
			accum_reg,
			member_reg,
			combined_reg,
			0,
			UINT32_MAX,
			0,
			0});
		accum_reg = combined_reg;
	}

	if (first)
		return false;
	out_bool_reg = accum_reg;
	return true;
}

bool
LowerScanFilterBoolExpr(const Expression *expr,
						const std::vector<yaap::PhysicalOperator::OutputColumn> *outputs,
						const std::vector<ColumnRef> &cols,
						const std::vector<ColumnSchema> &schema,
						std::vector<FilterInputDesc> &inputs,
						std::vector<FilterStep> &steps,
						std::vector<char> &string_consts,
						uint16_t &next_bool_reg,
						uint16_t &out_bool_reg)
{
	if (expr == nullptr)
		return false;
	if (expr->type == ExpressionType::BOUND_FUNCTION)
	{
		const auto *func = static_cast<const BoundFunctionExpression *>(expr);
		return LowerScanFilterCompare(func, outputs, cols, schema, inputs, steps, string_consts, next_bool_reg, out_bool_reg) ||
			   LowerScanFilterAnyOrAll(func, outputs, cols, schema, inputs, steps, string_consts, next_bool_reg, out_bool_reg);
	}
	if (expr->type != ExpressionType::BOUND_CONJUNCTION)
		return false;
	const auto *conj = static_cast<const BoundConjunctionExpression *>(expr);
	if (conj->children.empty())
		return false;
	if (conj->bool_expr_type == NOT_EXPR)
	{
		if (conj->children.size() != 1 || next_bool_reg >= pg_yaap::pipeline::FILTER_MAX_BOOL_REGS)
			return false;
		uint16_t child_reg = 0;
		if (!LowerScanFilterBoolExpr(conj->children[0].get(), outputs, cols, schema, inputs, steps, string_consts, next_bool_reg, child_reg))
		{
			if (pg_yaap_trace_hooks && conj->children[0] != nullptr)
			{
				const auto *func = dynamic_cast<const BoundFunctionExpression *>(conj->children[0].get());
				elog(LOG, "pg_yaap: scan NOT child lowering failed type=%d fn=%s",
					 static_cast<int>(conj->children[0]->type),
					 func != nullptr ? func->function_name.c_str() : "<non-fn>");
			}
			return false;
		}
		out_bool_reg = next_bool_reg++;
		steps.push_back(FilterStep{FilterStepOp::BOOL_NOT, QualOp::EQ, child_reg, 0, out_bool_reg, 0, UINT32_MAX, 0, 0});
		return true;
	}
	uint16_t left_reg = 0;
	if (!LowerScanFilterBoolExpr(conj->children[0].get(), outputs, cols, schema, inputs, steps, string_consts, next_bool_reg, left_reg))
	{
		if (pg_yaap_trace_hooks && conj->children[0] != nullptr)
		{
			const auto *func = dynamic_cast<const BoundFunctionExpression *>(conj->children[0].get());
			elog(LOG, "pg_yaap: scan conjunction first child failed bool_type=%d child_type=%d fn=%s",
				 conj->bool_expr_type,
				 static_cast<int>(conj->children[0]->type),
				 func != nullptr ? func->function_name.c_str() : "<non-fn>");
		}
		return false;
	}
	for (size_t i = 1; i < conj->children.size(); ++i)
	{
		if (next_bool_reg >= pg_yaap::pipeline::FILTER_MAX_BOOL_REGS)
			return false;
		uint16_t right_reg = 0;
		if (!LowerScanFilterBoolExpr(conj->children[i].get(), outputs, cols, schema, inputs, steps, string_consts, next_bool_reg, right_reg))
		{
			if (pg_yaap_trace_hooks && conj->children[i] != nullptr)
			{
				const auto *func = dynamic_cast<const BoundFunctionExpression *>(conj->children[i].get());
				elog(LOG, "pg_yaap: scan conjunction child[%zu] failed bool_type=%d child_type=%d fn=%s",
					 i,
					 conj->bool_expr_type,
					 static_cast<int>(conj->children[i]->type),
					 func != nullptr ? func->function_name.c_str() : "<non-fn>");
			}
			return false;
		}
		out_bool_reg = next_bool_reg++;
		steps.push_back(FilterStep{
			conj->bool_expr_type == AND_EXPR ? FilterStepOp::BOOL_AND : FilterStepOp::BOOL_OR,
			QualOp::EQ,
			left_reg,
			right_reg,
			out_bool_reg,
			0,
			UINT32_MAX,
			0,
			0});
		left_reg = out_bool_reg;
	}
	out_bool_reg = left_reg;
	return true;
}

bool
LowerJoinFilterCompare(const BoundFunctionExpression *func,
					   const PhysicalOperator *left_source_op,
					   const PhysicalOperator *right_source_op,
					   const std::vector<yaap::PhysicalOperator::OutputColumn> *left_outputs,
					   const std::vector<ColumnRef> &left_cols,
					   const std::vector<ColumnSchema> &left_schema,
					   const std::vector<yaap::PhysicalOperator::OutputColumn> *right_outputs,
					   const std::vector<ColumnRef> &right_cols,
					   const std::vector<ColumnSchema> &right_schema,
					   std::vector<HashJoinFilterInputDesc> &inputs,
					   std::vector<FilterStep> &steps,
					   std::vector<char> &string_consts,
					   uint16_t &next_bool_reg,
					   uint16_t &out_bool_reg)
{
	const bool negate_like = func != nullptr && func->function_name == "!~~";
	if (func == nullptr || func->children.size() != 2 ||
		(!IsComparisonName(func->function_name) && !negate_like))
		return false;
	const Expression *left_expr = UnwrapTransparentCastExpr(func->children[0].get());
	const Expression *right_expr = UnwrapTransparentCastExpr(func->children[1].get());
	const auto *left_col = dynamic_cast<const BoundColumnRefExpression *>(left_expr);
	const auto *right_col = dynamic_cast<const BoundColumnRefExpression *>(right_expr);
	auto folded_left_const = FoldNumericConstantExpression(left_expr);
	auto folded_right_const = FoldNumericConstantExpression(right_expr);
	const auto *left_const = folded_left_const.get();
	const auto *right_const = folded_right_const.get();

	FilterStep step{};
	step._pad0 = 0;
	step.const_offset = UINT32_MAX;
	step.const_len = 0;
	step.const_value = 0;
	step.right_idx = 0;
	step.out_bool_reg = next_bool_reg++;

	auto finish_step = [&]() -> bool
	{
		out_bool_reg = step.out_bool_reg;
		steps.push_back(step);
		if (!negate_like)
			return true;
		if (next_bool_reg >= pg_yaap::pipeline::FILTER_MAX_BOOL_REGS)
			return false;
		out_bool_reg = next_bool_reg++;
		steps.push_back(FilterStep{FilterStepOp::BOOL_NOT, QualOp::EQ, step.out_bool_reg, 0, out_bool_reg, 0, UINT32_MAX, 0, 0});
		return true;
	};

	auto locate = [&](const Expression *side_expr,
					  const BoundColumnRefExpression *col_expr,
					  const PhysicalOperator *source_op,
					  uint16_t &idx,
					  const ColumnSchema *&col) -> bool
	{
		if (col_expr == nullptr)
		{
			ColumnRef aggregate_ref{};
			if (!LookupAggregateOutputColumn(side_expr, source_op, left_cols, left_schema, aggregate_ref, col) &&
				!LookupAggregateOutputColumn(side_expr, source_op, right_cols, right_schema, aggregate_ref, col))
				return false;
			return LookupOrAddJoinFilterInput(aggregate_ref,
											 left_cols, left_schema,
											 right_cols, right_schema,
											 inputs, idx, col);
		}
		ColumnRef named_ref{};
		if (LookupNamedExprInputColumn(col_expr,
									   source_op == left_source_op ? left_outputs : right_outputs,
									   source_op == left_source_op ? left_cols : right_cols,
									   source_op == left_source_op ? left_schema : right_schema,
									   named_ref,
									   col) &&
			col != nullptr &&
			LookupOrAddJoinFilterInput(named_ref,
									  left_cols, left_schema,
									  right_cols, right_schema,
									  inputs, idx, col))
			return true;
		if (LookupOrAddJoinFilterInput(BindingToColumnRef(col_expr->binding),
									  left_cols, left_schema,
									  right_cols, right_schema,
									  inputs, idx, col))
			return true;

		return false;
	};

	auto resolve_ref = [&](const Expression *side_expr,
						   const BoundColumnRefExpression *col_expr,
						   const PhysicalOperator *source_op,
						   ColumnRef &out_ref,
						   const ColumnSchema *&out_col) -> bool
	{
		if (col_expr == nullptr)
			return LookupAggregateOutputColumn(side_expr, left_source_op, left_cols, left_schema, out_ref, out_col) ||
				   LookupAggregateOutputColumn(side_expr, right_source_op, right_cols, right_schema, out_ref, out_col);
		if (LookupNamedExprInputColumn(col_expr,
									   source_op == left_source_op ? left_outputs : right_outputs,
									   source_op == left_source_op ? left_cols : right_cols,
									   source_op == left_source_op ? left_schema : right_schema,
									   out_ref,
									   out_col))
			return true;
		out_ref = BindingToColumnRef(col_expr->binding);
		if (LookupRawColumn(out_ref, left_cols, left_schema, out_col) ||
			LookupRawColumn(out_ref, right_cols, right_schema, out_col))
			return true;
		return false;
	};

	auto log_join_compare_failure = [&]()
	{
		if (!pg_yaap_trace_hooks)
			return;
		const auto *lhs_func = dynamic_cast<const BoundFunctionExpression *>(func->children[0].get());
		const auto *rhs_func = dynamic_cast<const BoundFunctionExpression *>(func->children[1].get());
		const ColumnSchema *lhs_loc_schema = nullptr;
		const ColumnSchema *rhs_loc_schema = nullptr;
		const bool lhs_found = left_col != nullptr &&
			(LookupRawColumn(BindingToColumnRef(left_col->binding), left_cols, left_schema, lhs_loc_schema) ||
			 LookupRawColumn(BindingToColumnRef(left_col->binding), right_cols, right_schema, lhs_loc_schema));
		const bool rhs_found = right_col != nullptr &&
			(LookupRawColumn(BindingToColumnRef(right_col->binding), left_cols, left_schema, rhs_loc_schema) ||
			 LookupRawColumn(BindingToColumnRef(right_col->binding), right_cols, right_schema, rhs_loc_schema));
		elog(LOG,
			 "pg_yaap: join compare lowering failed fn=%s lhs_type=%d lhs_fn=%s rhs_type=%d rhs_fn=%s lhs_binding=(%zu,%zu) rhs_binding=(%zu,%zu) lhs_found=%d rhs_found=%d lhs_decode=%d rhs_decode=%d",
			 func->function_name.c_str(),
			 ExprTypeForLog(func->children[0].get()),
			 lhs_func != nullptr ? lhs_func->function_name.c_str() : "<non-fn>",
			 ExprTypeForLog(func->children[1].get()),
			 rhs_func != nullptr ? rhs_func->function_name.c_str() : "<non-fn>",
			 left_col != nullptr ? left_col->binding.table_index.index : static_cast<size_t>(-1),
			 left_col != nullptr ? left_col->binding.column_index.index : static_cast<size_t>(-1),
			 right_col != nullptr ? right_col->binding.table_index.index : static_cast<size_t>(-1),
			 right_col != nullptr ? right_col->binding.column_index.index : static_cast<size_t>(-1),
			 lhs_found ? 1 : 0,
			 rhs_found ? 1 : 0,
			 lhs_loc_schema != nullptr ? static_cast<int>(lhs_loc_schema->decode_kind) : -1,
			 rhs_loc_schema != nullptr ? static_cast<int>(rhs_loc_schema->decode_kind) : -1);
	};

	auto build_const = [&](const Expression *side_expr,
						   const BoundColumnRefExpression *col_expr,
						   const PhysicalOperator *source_op,
						   const BoundConstantExpression *constant) -> bool
	{
		const ColumnSchema *col = nullptr;
		if (!locate(side_expr, col_expr, source_op, step.left_idx, col) || col == nullptr)
			return false;
		if (col->decode_kind == ColumnDecodeKind::INT32_DATE)
		{
			DateADT date_const = 0;
			if (!EvaluateDateExpression(constant, date_const) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT32_CMP_CONST;
			step.const_value = static_cast<uint64_t>(DateADTGetDatum(date_const));
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::INT32_INT4)
		{
			Datum datum = 0;
			if (!ConvertConstantToDatum(constant, INT4OID, -1, datum) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT32_CMP_CONST;
			step.const_value = static_cast<uint64_t>(DatumGetInt32(datum));
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::INT32_CHAR)
		{
			int32_t ch = 0;
			if (!ExtractCharFilterConst(constant, ch) || !MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT32_CMP_CONST;
			step.const_value = static_cast<uint64_t>(static_cast<uint32_t>(ch));
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::INT64_INT8)
		{
			Datum datum = 0;
			if (!ConvertConstantToDatum(constant, INT8OID, -1, datum) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT64_CMP_CONST;
			step.const_value = static_cast<uint64_t>(DatumGetInt64(datum));
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
		{
			int64_t value = 0;
			const int8_t scale = static_cast<int8_t>(ExtractNumericTypmodScale(col->typmod));
			if (!ScaleNumericConstantToTargetScale(constant, scale, value) ||
				!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
				return false;
			step.op = FilterStepOp::INT64_CMP_CONST;
			step.const_value = static_cast<uint64_t>(value);
			return true;
		}
		if (col->decode_kind == ColumnDecodeKind::STRING_REF)
		{
			if (func->function_name == "=")
				step.op = FilterStepOp::STRING_EQ_CONST;
			else if (func->function_name == "<>" || func->function_name == "!=")
				step.op = FilterStepOp::STRING_NE_CONST;
			else if (func->function_name == "~~" || func->function_name == "!~~")
			{
				bool prefix = false;
				std::string match;
				if (TryExtractLikePattern(constant, prefix, match))
				{
					auto pattern_const = BoundConstantExpression(match, false);
					if (!StoreStringConstBytes(&pattern_const,
											   TEXTOID,
											   -1,
											   string_consts,
											   step.const_offset,
											   step.const_len,
											   step.const_value))
						return false;
					step.op = prefix ? FilterStepOp::STRING_PREFIX_LIKE : FilterStepOp::STRING_CONTAINS_LIKE;
				}
				else
				{
					if (!PatternUsesOnlyPercentWildcards(constant) ||
						!StoreStringConstBytes(constant,
											  TEXTOID,
											  -1,
											  string_consts,
											  step.const_offset,
											  step.const_len,
											  step.const_value))
						return false;
					step.op = FilterStepOp::STRING_SQL_LIKE;
				}
				step.cmp_op = QualOp::EQ;
				return true;
			}
			else
				return false;
			if (!StoreStringConstBytes(constant,
									  col->type_oid,
									  col->typmod,
									  string_consts,
									  step.const_offset,
									  step.const_len,
									  step.const_value))
				return false;
			step.cmp_op = QualOp::EQ;
			return true;
		}
		return false;
	};

	auto source_for_column = [&](const BoundColumnRefExpression *col_expr) -> const PhysicalOperator *
	{
		if (col_expr == nullptr)
			return nullptr;
		const ColumnRef ref = BindingToColumnRef(col_expr->binding);
		const ColumnSchema *col = nullptr;
		if (LookupRawColumn(ref, left_cols, left_schema, col) && col != nullptr)
			return left_source_op;
		if (LookupRawColumn(ref, right_cols, right_schema, col) && col != nullptr)
			return right_source_op;
		return nullptr;
	};

	auto build_const_for_column = [&](const Expression *side_expr,
									  const BoundColumnRefExpression *col_expr,
									  const BoundConstantExpression *constant) -> bool
	{
		if (const PhysicalOperator *source_op = source_for_column(col_expr))
			return build_const(side_expr, col_expr, source_op, constant);
		return build_const(side_expr, col_expr, left_source_op, constant) ||
			   build_const(side_expr, col_expr, right_source_op, constant);
	};

	if (left_col != nullptr && right_const != nullptr && build_const_for_column(left_expr, left_col, right_const))
		return finish_step();
	if (right_col != nullptr && left_const != nullptr && build_const_for_column(right_expr, right_col, left_const))
	{
		if (func->function_name == "<")
			step.cmp_op = QualOp::GT;
		else if (func->function_name == "<=")
			step.cmp_op = QualOp::GE;
		else if (func->function_name == ">")
			step.cmp_op = QualOp::LT;
		else if (func->function_name == ">=")
			step.cmp_op = QualOp::LE;
		return finish_step();
	}
	if ((left_col == nullptr && left_expr->type != ExpressionType::BOUND_AGGREGATE) ||
		(right_col == nullptr && right_expr->type != ExpressionType::BOUND_AGGREGATE))
	{
		log_join_compare_failure();
		return false;
	}

	const ColumnSchema *left_col_schema = nullptr;
	const ColumnSchema *right_col_schema = nullptr;
	if (!locate(left_expr, left_col, left_source_op, step.left_idx, left_col_schema) ||
		!locate(right_expr, right_col, right_source_op, step.right_idx, right_col_schema) ||
		left_col_schema == nullptr || right_col_schema == nullptr ||
		!MapComparisonNameToQualOp(func->function_name, step.cmp_op))
	{
		log_join_compare_failure();
		return false;
	}
	auto is_integral_numeric = [](ColumnDecodeKind kind)
	{
		return kind == ColumnDecodeKind::INT32_INT4 || kind == ColumnDecodeKind::INT64_INT8;
	};
	auto is_numeric_family = [&](ColumnDecodeKind kind)
	{
		return is_integral_numeric(kind) || kind == ColumnDecodeKind::INT64_NUMERIC_SCALED;
	};
	if (left_col_schema->decode_kind != right_col_schema->decode_kind)
	{
		if (!(is_numeric_family(left_col_schema->decode_kind) && is_numeric_family(right_col_schema->decode_kind)))
		{
			log_join_compare_failure();
			return false;
		}

		ColumnRef left_ref{};
		ColumnRef right_ref{};
		if (!resolve_ref(left_expr, left_col, left_source_op, left_ref, left_col_schema) ||
			!resolve_ref(right_expr, right_col, right_source_op, right_ref, right_col_schema))
		{
			log_join_compare_failure();
			return false;
		}
		if (!(is_numeric_family(left_col_schema->decode_kind) && is_numeric_family(right_col_schema->decode_kind)))
		{
			log_join_compare_failure();
			return false;
		}
		if (!(left_col_schema->decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED ||
			  left_col_schema->decode_kind == ColumnDecodeKind::INT32_INT4 ||
			  left_col_schema->decode_kind == ColumnDecodeKind::INT64_INT8) ||
			!(right_col_schema->decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED ||
			  right_col_schema->decode_kind == ColumnDecodeKind::INT32_INT4 ||
			  right_col_schema->decode_kind == ColumnDecodeKind::INT64_INT8))
		{
			log_join_compare_failure();
			return false;
		}
		if (!LookupOrAddJoinFilterInputAs(left_ref,
										 left_cols, left_schema,
										 right_cols, right_schema,
										 ColumnDecodeKind::INT64_NUMERIC_SCALED,
										 0,
										 inputs, step.left_idx, left_col_schema) ||
			!LookupOrAddJoinFilterInputAs(right_ref,
										 left_cols, left_schema,
										 right_cols, right_schema,
										 ColumnDecodeKind::INT64_NUMERIC_SCALED,
										 0,
										 inputs, step.right_idx, right_col_schema))
		{
			log_join_compare_failure();
			return false;
		}
		step.op = FilterStepOp::INT64_CMP_VAR;
		return finish_step();
	}
	if (left_col_schema->decode_kind == ColumnDecodeKind::INT32_DATE ||
		left_col_schema->decode_kind == ColumnDecodeKind::INT32_INT4 ||
		left_col_schema->decode_kind == ColumnDecodeKind::INT32_CHAR)
		step.op = FilterStepOp::INT32_CMP_VAR;
	else if (left_col_schema->decode_kind == ColumnDecodeKind::INT64_INT8 ||
			 left_col_schema->decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
		step.op = FilterStepOp::INT64_CMP_VAR;
	else
	{
		log_join_compare_failure();
		return false;
	}
	return finish_step();
}

bool
LowerJoinFilterBoolExpr(const Expression *expr,
						const PhysicalOperator *left_source_op,
						const PhysicalOperator *right_source_op,
						const std::vector<yaap::PhysicalOperator::OutputColumn> *left_outputs,
						const std::vector<ColumnRef> &left_cols,
						const std::vector<ColumnSchema> &left_schema,
						const std::vector<yaap::PhysicalOperator::OutputColumn> *right_outputs,
						const std::vector<ColumnRef> &right_cols,
						const std::vector<ColumnSchema> &right_schema,
						std::vector<HashJoinFilterInputDesc> &inputs,
						std::vector<FilterStep> &steps,
						std::vector<char> &string_consts,
						uint16_t &next_bool_reg,
						uint16_t &out_bool_reg)
{
	if (expr == nullptr)
		return false;
	if (expr->type == ExpressionType::BOUND_FUNCTION)
		return LowerJoinFilterCompare(static_cast<const BoundFunctionExpression *>(expr),
									  left_source_op,
									  right_source_op,
									  left_outputs,
									  left_cols, left_schema,
									  right_outputs,
									  right_cols, right_schema,
									  inputs, steps, string_consts, next_bool_reg, out_bool_reg);
	if (expr->type != ExpressionType::BOUND_CONJUNCTION)
		return false;
	const auto *conj = static_cast<const BoundConjunctionExpression *>(expr);
	if (conj->children.empty())
		return false;
	if (conj->bool_expr_type == NOT_EXPR)
	{
		if (conj->children.size() != 1 || next_bool_reg >= pg_yaap::pipeline::FILTER_MAX_BOOL_REGS)
			return false;
		uint16_t child_reg = 0;
		if (!LowerJoinFilterBoolExpr(conj->children[0].get(),
									 left_source_op,
									 right_source_op,
									 left_outputs,
									 left_cols, left_schema,
									 right_outputs,
									 right_cols, right_schema,
									 inputs, steps, string_consts, next_bool_reg, child_reg))
			return false;
		out_bool_reg = next_bool_reg++;
		steps.push_back(FilterStep{FilterStepOp::BOOL_NOT, QualOp::EQ, child_reg, 0, out_bool_reg, 0, UINT32_MAX, 0, 0});
		return true;
	}
	uint16_t left_reg = 0;
	if (!LowerJoinFilterBoolExpr(conj->children[0].get(),
								 left_source_op,
								 right_source_op,
								 left_outputs,
								 left_cols, left_schema,
								 right_outputs,
								 right_cols, right_schema,
								 inputs, steps, string_consts, next_bool_reg, left_reg))
		return false;
	for (size_t i = 1; i < conj->children.size(); ++i)
	{
		if (next_bool_reg >= pg_yaap::pipeline::FILTER_MAX_BOOL_REGS)
			return false;
		uint16_t right_reg = 0;
		if (!LowerJoinFilterBoolExpr(conj->children[i].get(),
									 left_source_op,
									 right_source_op,
									 left_outputs,
									 left_cols, left_schema,
									 right_outputs,
									 right_cols, right_schema,
									 inputs, steps, string_consts, next_bool_reg, right_reg))
			return false;
		out_bool_reg = next_bool_reg++;
		steps.push_back(FilterStep{
			conj->bool_expr_type == AND_EXPR ? FilterStepOp::BOOL_AND : FilterStepOp::BOOL_OR,
			QualOp::EQ,
			left_reg,
			right_reg,
			out_bool_reg,
			0,
			UINT32_MAX,
			0,
			0});
		left_reg = out_bool_reg;
	}
	out_bool_reg = left_reg;
	return true;
}

bool
LowerScanFilters(const std::vector<Expression *> &filters,
				 const std::vector<yaap::PhysicalOperator::OutputColumn> *outputs,
				 const std::vector<ColumnRef> &cols,
				 const std::vector<ColumnSchema> &schema,
				 std::vector<FilterInputDesc> &inputs,
				 std::vector<FilterExprDesc> &exprs,
				 std::vector<FilterStep> &steps,
				 std::vector<char> &string_consts)
{
	uint16_t next_bool_reg = 0;
	for (Expression *expr : filters)
	{
		if (expr == nullptr)
			return false;
		const size_t first_step_idx = steps.size();
		uint16_t out_bool_reg = 0;
		if (!LowerScanFilterBoolExpr(expr, outputs, cols, schema, inputs, steps, string_consts, next_bool_reg, out_bool_reg) ||
			!AppendFilterExpr(exprs, steps, first_step_idx, out_bool_reg))
		{
			if (pg_yaap_trace_hooks)
			{
				if (expr->type == ExpressionType::BOUND_FUNCTION)
				{
					const auto *func = static_cast<BoundFunctionExpression *>(expr);
					if (func->function_name == "any" || func->function_name == "all")
					{
						const auto *rhs_func = func->children.size() > 1
							? dynamic_cast<const BoundFunctionExpression *>(func->children[1].get())
							: nullptr;
						elog(LOG,
							 "pg_yaap: scan filter lowering failed on function %s op_oid=%d nchildren=%zu lhs_type=%d rhs_type=%d rhs_fn=%s",
							 func->function_name.c_str(),
							 func->op_oid,
							 func->children.size(),
							 func->children.empty() ? -1 : ExprTypeForLog(func->children[0].get()),
							 func->children.size() > 1 ? ExprTypeForLog(func->children[1].get()) : -1,
							 rhs_func != nullptr ? rhs_func->function_name.c_str() : "<non-fn>");
					}
					else
					{
						elog(LOG, "pg_yaap: scan filter lowering failed on function %s",
							 func->function_name.c_str());
					}
				}
				else if (expr->type == ExpressionType::BOUND_CONJUNCTION)
					elog(LOG, "pg_yaap: scan filter lowering failed on conjunction type=%d",
						 static_cast<BoundConjunctionExpression *>(expr)->bool_expr_type);
				else
					elog(LOG, "pg_yaap: scan filter lowering failed on expr type=%d", static_cast<int>(expr->type));
			}
			return false;
		}
	}
	return true;
}

bool
LowerJoinFilters(const std::vector<Expression *> &filters,
				 const PhysicalOperator *left_source_op,
				 const PhysicalOperator *right_source_op,
				 const std::vector<yaap::PhysicalOperator::OutputColumn> *left_outputs,
				 const std::vector<ColumnRef> &left_cols,
				 const std::vector<ColumnSchema> &left_schema,
				 const std::vector<yaap::PhysicalOperator::OutputColumn> *right_outputs,
				 const std::vector<ColumnRef> &right_cols,
				 const std::vector<ColumnSchema> &right_schema,
				 std::vector<HashJoinFilterInputDesc> &inputs,
				 std::vector<FilterExprDesc> &exprs,
				 std::vector<FilterStep> &steps,
				 std::vector<char> &string_consts,
				 uint16_t &out_bool_regs)
{
	uint16_t next_bool_reg = 0;
	for (Expression *expr : filters)
	{
		if (expr == nullptr)
			return false;
		const size_t first_step_idx = steps.size();
		uint16_t out_bool_reg = 0;
		if (!LowerJoinFilterBoolExpr(expr,
									 left_source_op,
									 right_source_op,
									 left_outputs,
									 left_cols, left_schema,
									 right_outputs,
									 right_cols, right_schema,
									 inputs, steps, string_consts, next_bool_reg, out_bool_reg) ||
			!AppendFilterExpr(exprs, steps, first_step_idx, out_bool_reg))
			return false;
	}
	out_bool_regs = next_bool_reg;
	return true;
}

}  // namespace pg_yaap::optimizer_translator_detail
