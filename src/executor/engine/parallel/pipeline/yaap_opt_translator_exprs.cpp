#include "parallel/pipeline/yaap_pipeline_init_internal.hpp"

extern "C" {
#include "storage/dsm.h"
#include "storage/shm_toc.h"
}

#include "parallel/pipeline/dsm_control.hpp"

#include <cerrno>
#include <cstdlib>

namespace pg_yaap::optimizer_translator_detail {

bool
LookupCachedOptimizerExpr(const Expression *expr,
						  const std::vector<MaterializedOptExpr> *cache,
						  int8_t &out_scale,
						  uint8_t &out_slot)
{
	if (cache == nullptr)
		return false;
	for (const auto &entry : *cache)
	{
		if (entry.expr == expr)
		{
			out_scale = entry.scale;
			out_slot = entry.slot;
			return true;
		}
	}
	return false;
}

bool
AppendScaleProjectStep(uint8_t input_slot,
					   int8_t input_scale,
					   int8_t target_scale,
					   std::vector<ProjectStep> &steps,
					   uint8_t &next_int64_slot,
					   uint8_t &out_slot)
{
	if (input_scale == target_scale)
	{
		out_slot = input_slot;
		return true;
	}
	int64_t factor = 0;
	const int delta = static_cast<int>(target_scale) - static_cast<int>(input_scale);
	if (!Pow10Int64(std::abs(delta), factor) || next_int64_slot >= kDataChunkMaxSlots)
		return false;
	out_slot = next_int64_slot++;
	steps.push_back(ProjectStep{
		ProjectOp::NUMERIC_SCALE_VAR_CONST,
		input_slot,
		0,
		out_slot,
		delta >= 0 ? factor : -factor});
	return true;
}

static bool
LowerProjectionNumericCompare(const BoundFunctionExpression *func,
							  std::vector<ProjectStep> &steps,
							  uint8_t &next_int64_slot,
							  const std::vector<ColumnRef> &cols,
							  const std::vector<ColumnSchema> &schema,
							  uint8_t &out_slot)
{
	if (func == nullptr || func->children.size() != 2 || next_int64_slot >= kDataChunkMaxSlots)
		return false;
	ProjectOp op = ProjectOp::COPY_VAR;
	if (func->function_name == "<") op = ProjectOp::INT64_LT_VAR_CONST;
	else if (func->function_name == "<=") op = ProjectOp::INT64_LE_VAR_CONST;
	else if (func->function_name == "=") op = ProjectOp::INT64_EQ_VAR_CONST;
	else if (func->function_name == ">=") op = ProjectOp::INT64_GE_VAR_CONST;
	else if (func->function_name == ">") op = ProjectOp::INT64_GT_VAR_CONST;
	else if (func->function_name == "<>" || func->function_name == "!=") op = ProjectOp::INT64_NE_VAR_CONST;
	else return false;

	const auto *lhs = dynamic_cast<const BoundColumnRefExpression *>(func->children[0].get());
	const auto *rhs = dynamic_cast<const BoundConstantExpression *>(func->children[1].get());
	if (lhs == nullptr || rhs == nullptr || rhs->is_null)
		return false;

	ColumnRef ref{};
	const ColumnSchema *col = nullptr;
	if (!LookupBindingColumn(lhs->binding, cols, schema, ref, col) || col == nullptr)
		return false;
	if (col->decode_kind != ColumnDecodeKind::INT32_INT4 &&
		col->decode_kind != ColumnDecodeKind::INT64_INT8 &&
		col->decode_kind != ColumnDecodeKind::INT64_NUMERIC_SCALED)
		return false;

	int64_t const_value = 0;
	if (col->decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
	{
		int8_t scale = 0;
		if (!ColumnNumericScale(*col, scale) || !ScaleConstantToTargetScale(rhs, scale, const_value))
			return false;
	}
	else
	{
		char *end = nullptr;
		errno = 0;
		long long parsed = std::strtoll(rhs->value.c_str(), &end, 10);
		if (errno != 0 || end == rhs->value.c_str())
			return false;
		while (*end != '\0' && std::isspace(static_cast<unsigned char>(*end)))
			++end;
		if (*end != '\0')
			return false;
		const_value = static_cast<int64_t>(parsed);
	}

	uint8_t input_slot = col->chunk_slot;
	if (col->decode_kind == ColumnDecodeKind::INT32_INT4)
	{
		if (next_int64_slot >= kDataChunkMaxSlots)
			return false;
		input_slot = next_int64_slot++;
		steps.push_back(ProjectStep{ProjectOp::INT32_TO_INT64_VAR, col->chunk_slot, 0, input_slot, 0});
	}

	out_slot = next_int64_slot++;
	steps.push_back(ProjectStep{op, input_slot, 0, out_slot, const_value});
	return true;
}

bool
LowerOptimizerBoolExpr(const Expression *expr,
					   std::vector<ProjectStep> &steps,
					   uint8_t &next_int64_slot,
					   const std::vector<ColumnRef> &cols,
					   const std::vector<ColumnSchema> &schema,
					   uint8_t &out_slot);

bool
LowerOptimizerExpr(const Expression *expr,
				   std::vector<ProjectStep> &steps,
				   uint8_t &next_int64_slot,
				   const std::vector<ColumnRef> &cols,
				   const std::vector<ColumnSchema> &schema,
				   const std::vector<MaterializedOptExpr> *cache,
				   int8_t &out_scale,
				   uint8_t &out_slot,
				   bool require_int64_slot);

static const BoundColumnRefExpression *
UnwrapPrefixBaseColumn(const Expression *expr)
{
	return UnwrapTransparentCastColumn(expr);
}

bool
LowerProjectionConstant(const BoundConstantExpression *constant,
						std::vector<ProjectStep> &steps,
						uint8_t &next_int64_slot,
						int8_t &out_scale,
						uint8_t &out_slot)
{
	if (constant == nullptr || constant->is_null || next_int64_slot >= kDataChunkMaxSlots)
		return false;
	int64_t value = 0;
	int8_t scale = 0;
	if (pg_strcasecmp(constant->value.c_str(), "true") == 0)
		value = 1;
	else if (pg_strcasecmp(constant->value.c_str(), "false") == 0)
		value = 0;
	else if (constant->value.find('.') != std::string::npos)
	{
		if (!ScaleNumericConstantToInt64(constant, scale, value))
			return false;
	}
	else
	{
		const char *ptr = constant->value.c_str();
		char *end = nullptr;
		errno = 0;
		long long parsed = std::strtoll(ptr, &end, 10);
		if (errno != 0 || end == ptr)
			return false;
		while (*end != '\0' && std::isspace(static_cast<unsigned char>(*end)))
			++end;
		if (*end != '\0')
			return false;
		value = static_cast<int64_t>(parsed);
	}
	out_slot = next_int64_slot++;
	out_scale = scale;
	steps.push_back(ProjectStep{ProjectOp::CONST_INT64, 0, 0, out_slot, value});
	return true;
}

bool
LowerProjectionStringCompare(const BoundFunctionExpression *func,
							 std::vector<ProjectStep> &steps,
							 uint8_t &next_int64_slot,
							 const std::vector<ColumnRef> &cols,
							 const std::vector<ColumnSchema> &schema,
							 uint8_t &out_slot)
{
	if (func == nullptr || func->children.size() != 2)
		return false;
	const auto *lhs = dynamic_cast<const BoundColumnRefExpression *>(func->children[0].get());
	const auto *rhs = dynamic_cast<const BoundConstantExpression *>(func->children[1].get());
	if (lhs == nullptr || rhs == nullptr)
		return false;
	ColumnRef ref{};
	const ColumnSchema *col = nullptr;
	if (!LookupBindingColumn(lhs->binding, cols, schema, ref, col) || col == nullptr || col->decode_kind != ColumnDecodeKind::STRING_REF)
		return false;
	uint8_t const_len = 0;
	int64_t const_value = 0;
	if (!TryExtractShortStringConst(rhs, const_len, const_value) || next_int64_slot >= kDataChunkMaxSlots)
		return false;
	uint8_t encoded_len = const_len;
	if (col->type_oid == BPCHAROID)
		encoded_len |= 0x80;
	out_slot = next_int64_slot++;
	if (func->function_name == "=")
		steps.push_back(ProjectStep{ProjectOp::STRING_EQ_VAR_CONST, col->chunk_slot, encoded_len, out_slot, const_value});
	else if (func->function_name == "<>" || func->function_name == "!=")
		steps.push_back(ProjectStep{ProjectOp::STRING_NE_VAR_CONST, col->chunk_slot, encoded_len, out_slot, const_value});
	else
		return false;
	return true;
}

bool
LowerProjectionStringLike(const BoundFunctionExpression *func,
						  std::vector<ProjectStep> &steps,
						  uint8_t &next_int64_slot,
						  const std::vector<ColumnRef> &cols,
						  const std::vector<ColumnSchema> &schema,
						  uint8_t &out_slot)
{
	if (func == nullptr || func->children.size() != 2 || func->function_name != "~~")
		return false;
	const auto *lhs = dynamic_cast<const BoundColumnRefExpression *>(func->children[0].get());
	const auto *rhs = dynamic_cast<const BoundConstantExpression *>(func->children[1].get());
	if (lhs == nullptr || rhs == nullptr)
		return false;
	ColumnRef ref{};
	const ColumnSchema *col = nullptr;
	if (!LookupBindingColumn(lhs->binding, cols, schema, ref, col) || col == nullptr || col->decode_kind != ColumnDecodeKind::STRING_REF)
		return false;
	bool prefix = false;
	std::string match;
	if (!TryExtractLikePattern(rhs, prefix, match) || !prefix || match.size() > 8 || next_int64_slot >= kDataChunkMaxSlots)
		return false;
	int64_t packed = 0;
	if (!match.empty())
		std::memcpy(&packed, match.data(), match.size());
	out_slot = next_int64_slot++;
	steps.push_back(ProjectStep{
		ProjectOp::STRING_PREFIX_LIKE,
		col->chunk_slot,
		static_cast<uint8_t>(match.size()),
		out_slot,
		packed});
	return true;
}

bool
LowerProjectionStringPrefixSlice(const BoundFunctionExpression *func,
								 std::vector<ProjectStep> &steps,
								 uint8_t &next_string_slot,
								 const std::vector<ColumnRef> &cols,
								 const std::vector<ColumnSchema> &schema,
								 uint8_t &out_slot)
{
	if (func == nullptr || func->function_name != "prefix_slice" || func->children.size() != 2 || next_string_slot >= kDataChunkMaxSlots)
		return false;
	const auto *lhs = UnwrapPrefixBaseColumn(func->children[0].get());
	const auto *len = dynamic_cast<const BoundConstantExpression *>(func->children[1].get());
	if (lhs == nullptr || len == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: projection prefix_slice rejected lhs_type=%d len_type=%d",
				 func->children[0] != nullptr ? static_cast<int>(func->children[0]->type) : -1,
				 func->children[1] != nullptr ? static_cast<int>(func->children[1]->type) : -1);
		return false;
	}
	ColumnRef ref{};
	const ColumnSchema *col = nullptr;
	if (!LookupBindingColumn(lhs->binding, cols, schema, ref, col) || col == nullptr || col->decode_kind != ColumnDecodeKind::STRING_REF)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: projection prefix_slice lookup rejected binding=(%zu,%zu) found=%d decode=%d",
				 lhs->binding.table_index.index,
				 lhs->binding.column_index.index,
				 col != nullptr ? 1 : 0,
				 col != nullptr ? static_cast<int>(col->decode_kind) : -1);
		return false;
	}
	char *end = nullptr;
	errno = 0;
	long parsed = std::strtol(len->value.c_str(), &end, 10);
	if (errno != 0 || end == len->value.c_str() || parsed <= 0 || parsed > 255)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: projection prefix_slice len parse rejected value=%s", len->value.c_str());
		return false;
	}
	while (*end != '\0' && std::isspace(static_cast<unsigned char>(*end)))
		++end;
	if (*end != '\0')
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: projection prefix_slice len trailing rejected value=%s", len->value.c_str());
		return false;
	}
	out_slot = next_string_slot++;
	steps.push_back(ProjectStep{
		ProjectOp::STRING_PREFIX_SLICE,
		col->chunk_slot,
		static_cast<uint8_t>(parsed),
		out_slot,
		0});
	return true;
}

bool
LowerOptimizerBoolExpr(const Expression *expr,
					   std::vector<ProjectStep> &steps,
					   uint8_t &next_int64_slot,
					   const std::vector<ColumnRef> &cols,
					   const std::vector<ColumnSchema> &schema,
					   uint8_t &out_slot)
{
	if (expr == nullptr)
		return false;
	if (expr->type == ExpressionType::BOUND_FUNCTION)
	{
		const auto *func = static_cast<const BoundFunctionExpression *>(expr);
		if (LowerProjectionStringLike(func, steps, next_int64_slot, cols, schema, out_slot) ||
			LowerProjectionStringCompare(func, steps, next_int64_slot, cols, schema, out_slot) ||
			LowerProjectionNumericCompare(func, steps, next_int64_slot, cols, schema, out_slot))
			return true;
	}
	if (expr->type != ExpressionType::BOUND_CONJUNCTION)
		return false;
	const auto *conj = static_cast<const BoundConjunctionExpression *>(expr);
	if (conj->children.empty())
		return false;
	if (conj->bool_expr_type == NOT_EXPR)
	{
		if (conj->children.size() != 1 || next_int64_slot >= kDataChunkMaxSlots)
			return false;
		uint8_t child_slot = 0;
		if (!LowerOptimizerBoolExpr(conj->children[0].get(), steps, next_int64_slot, cols, schema, child_slot))
			return false;
		out_slot = next_int64_slot++;
		steps.push_back(ProjectStep{ProjectOp::BOOL_NOT_VAR, child_slot, 0, out_slot, 0});
		return true;
	}
	uint8_t left_slot = 0;
	if (!LowerOptimizerBoolExpr(conj->children[0].get(), steps, next_int64_slot, cols, schema, left_slot))
		return false;
	for (size_t i = 1; i < conj->children.size(); ++i)
	{
		if (next_int64_slot >= kDataChunkMaxSlots)
			return false;
		uint8_t right_slot = 0;
		if (!LowerOptimizerBoolExpr(conj->children[i].get(), steps, next_int64_slot, cols, schema, right_slot))
			return false;
		out_slot = next_int64_slot++;
		steps.push_back(ProjectStep{
			conj->bool_expr_type == AND_EXPR ? ProjectOp::BOOL_AND_VAR_VAR : ProjectOp::BOOL_OR_VAR_VAR,
			left_slot,
			right_slot,
			out_slot,
			0});
		left_slot = out_slot;
	}
	out_slot = left_slot;
	return true;
}

bool
LowerProjectionStringExpr(const Expression *expr,
						  std::vector<ProjectStep> &steps,
						  std::vector<char> &string_consts,
						  uint8_t &next_int64_slot,
						  uint8_t &next_string_slot,
						  const std::vector<ColumnRef> &cols,
						  const std::vector<ColumnSchema> &schema,
						  uint8_t &out_slot)
{
	if (expr == nullptr)
		return false;
	const Expression *unwrapped = UnwrapTransparentCastExpr(expr);
	if (unwrapped == nullptr || unwrapped->type != ExpressionType::BOUND_FUNCTION)
		return false;
	const auto *func = static_cast<const BoundFunctionExpression *>(unwrapped);
	if (func->function_name == "prefix_slice")
		return LowerProjectionStringPrefixSlice(func, steps, next_string_slot, cols, schema, out_slot);

	if (pg_strcasecmp(func->function_name.c_str(), "regexp_replace") == 0)
	{
		if (func->children.size() != 3 || next_string_slot >= kDataChunkMaxSlots)
			return false;
		const auto *src_col_expr = UnwrapTransparentCastColumn(func->children[0].get());
		const auto *pat_const = dynamic_cast<const BoundConstantExpression *>(UnwrapTransparentCastExpr(func->children[1].get()));
		const auto *repl_const = dynamic_cast<const BoundConstantExpression *>(UnwrapTransparentCastExpr(func->children[2].get()));
		if (src_col_expr == nullptr || pat_const == nullptr || repl_const == nullptr ||
			pat_const->is_null || repl_const->is_null)
			return false;
		ColumnRef ref{};
		const ColumnSchema *src_col = nullptr;
		if (!LookupBindingColumn(src_col_expr->binding, cols, schema, ref, src_col) ||
			src_col == nullptr || src_col->decode_kind != ColumnDecodeKind::STRING_REF)
			return false;
		const size_t pat_off = string_consts.size();
		const size_t pat_len = pat_const->value.size();
		string_consts.insert(string_consts.end(), pat_const->value.begin(), pat_const->value.end());
		const size_t repl_off = string_consts.size();
		const size_t repl_len = repl_const->value.size();
		string_consts.insert(string_consts.end(), repl_const->value.begin(), repl_const->value.end());
		if (pat_off > 0xFFFFu || pat_len > 0xFFFFu || repl_off > 0xFFFFu || repl_len > 0xFFFFu)
			return false;
		const uint64_t packed =
			(static_cast<uint64_t>(pat_off) & 0xFFFFu) |
			((static_cast<uint64_t>(pat_len) & 0xFFFFu) << 16) |
			((static_cast<uint64_t>(repl_off) & 0xFFFFu) << 32) |
			((static_cast<uint64_t>(repl_len) & 0xFFFFu) << 48);
		out_slot = next_string_slot++;
		steps.push_back(ProjectStep{
			ProjectOp::STRING_REGEXP_REPLACE,
			src_col->chunk_slot,
			0,
			out_slot,
			static_cast<int64_t>(packed)});
		return true;
	}

	if (func->function_name == "case" && func->children.size() == 2)
	{
		const auto *when_func = dynamic_cast<const BoundFunctionExpression *>(func->children[0].get());
		if (when_func == nullptr || when_func->function_name != "when" || when_func->children.size() != 2)
			return false;
		uint8_t then_slot = 0;
		bool then_is_str = false;
		const Expression *then_expr = UnwrapTransparentCastExpr(when_func->children[1].get());
		if (const auto *then_col_expr = dynamic_cast<const BoundColumnRefExpression *>(then_expr))
		{
			ColumnRef ref{};
			const ColumnSchema *col = nullptr;
			if (LookupBindingColumn(then_col_expr->binding, cols, schema, ref, col) &&
				col != nullptr && col->decode_kind == ColumnDecodeKind::STRING_REF)
			{
				then_slot = col->chunk_slot;
				then_is_str = true;
			}
		}
		else if (LowerProjectionStringExpr(then_expr, steps, string_consts, next_int64_slot, next_string_slot, cols, schema, then_slot))
		{
			then_is_str = true;
		}
		if (!then_is_str || next_string_slot >= kDataChunkMaxSlots)
			return false;

		uint8_t cond_slot = 0;
		if (!LowerOptimizerBoolExpr(when_func->children[0].get(), steps, next_int64_slot, cols, schema, cond_slot))
			return false;

		const Expression *else_expr = UnwrapTransparentCastExpr(func->children[1].get());
		if (const auto *else_const = dynamic_cast<const BoundConstantExpression *>(else_expr))
		{
			if (else_const->is_null)
				return false;
			const uint32_t else_off = static_cast<uint32_t>(string_consts.size());
			const uint32_t else_len = static_cast<uint32_t>(else_const->value.size());
			if (else_len > 0)
				string_consts.insert(string_consts.end(), else_const->value.begin(), else_const->value.end());
			const uint64_t packed = static_cast<uint64_t>(else_off) | (static_cast<uint64_t>(else_len) << 32);
			out_slot = next_string_slot++;
			steps.push_back(ProjectStep{
				ProjectOp::STRING_CASE_VAR_CONST,
				cond_slot,
				then_slot,
				out_slot,
				static_cast<int64_t>(packed)});
			return true;
		}
		uint8_t else_slot = 0;
		bool else_is_str = false;
		if (const auto *else_col_expr = dynamic_cast<const BoundColumnRefExpression *>(else_expr))
		{
			ColumnRef ref{};
			const ColumnSchema *col = nullptr;
			if (LookupBindingColumn(else_col_expr->binding, cols, schema, ref, col) &&
				col != nullptr && col->decode_kind == ColumnDecodeKind::STRING_REF)
			{
				else_slot = col->chunk_slot;
				else_is_str = true;
			}
		}
		else if (LowerProjectionStringExpr(else_expr, steps, string_consts, next_int64_slot, next_string_slot, cols, schema, else_slot))
		{
			else_is_str = true;
		}
		if (!else_is_str || next_string_slot >= kDataChunkMaxSlots)
			return false;
		out_slot = next_string_slot++;
		steps.push_back(ProjectStep{
			ProjectOp::STRING_CASE_ELSE_VAR,
			cond_slot,
			then_slot,
			out_slot,
			static_cast<int64_t>(else_slot)});
		return true;
	}

	return false;
}

bool
LowerExtractYearExpr(const BoundFunctionExpression *func,
					 std::vector<ProjectStep> &steps,
					 uint8_t &next_int64_slot,
					 const std::vector<ColumnRef> &cols,
					 const std::vector<ColumnSchema> &schema,
					 int8_t &out_scale,
					 uint8_t &out_slot)
{
	if (func == nullptr || func->children.size() != 2 || next_int64_slot >= kDataChunkMaxSlots)
		return false;
	if (pg_strcasecmp(func->function_name.c_str(), "date_trunc") == 0)
	{
		const auto *field = dynamic_cast<const BoundConstantExpression *>(UnwrapTransparentCastExpr(func->children[0].get()));
		const auto *value = UnwrapTransparentCastColumn(func->children[1].get());
		if (field == nullptr || value == nullptr || pg_strcasecmp(field->value.c_str(), "minute") != 0)
			return false;
		ColumnRef ref{};
		const ColumnSchema *col = nullptr;
		if (!LookupBindingColumn(value->binding, cols, schema, ref, col) ||
			col == nullptr || col->type_oid != TIMESTAMPOID || col->decode_kind != ColumnDecodeKind::INT64_INT8)
			return false;
		out_slot = next_int64_slot++;
		out_scale = 0;
		steps.push_back(ProjectStep{ProjectOp::DATE_TRUNC_MINUTE_TIMESTAMP, col->chunk_slot, 0, out_slot, 0});
		return true;
	}
	if (func->function_name != "extract" && func->function_name != "date_part")
		return false;
	const auto *field = dynamic_cast<const BoundConstantExpression *>(UnwrapTransparentCastExpr(func->children[0].get()));
	const auto *value = UnwrapTransparentCastColumn(func->children[1].get());
	if (field == nullptr || value == nullptr)
		return false;
	ColumnRef ref{};
	const ColumnSchema *col = nullptr;
	if (!LookupBindingColumn(value->binding, cols, schema, ref, col) || col == nullptr)
		return false;
	if (pg_strcasecmp(field->value.c_str(), "year") == 0 && col->decode_kind == ColumnDecodeKind::INT32_DATE)
	{
		out_slot = next_int64_slot++;
		out_scale = 0;
		steps.push_back(ProjectStep{ProjectOp::EXTRACT_YEAR_FROM_DATE, col->chunk_slot, 0, out_slot, 0});
		return true;
	}
	if (pg_strcasecmp(field->value.c_str(), "minute") == 0 &&
		col->type_oid == TIMESTAMPOID && col->decode_kind == ColumnDecodeKind::INT64_INT8)
	{
		out_slot = next_int64_slot++;
		out_scale = 0;
		steps.push_back(ProjectStep{ProjectOp::EXTRACT_MINUTE_FROM_TIMESTAMP, col->chunk_slot, 0, out_slot, 0});
		return true;
	}
	return false;
}

static bool
LowerStringLengthExpr(const BoundFunctionExpression *func,
					  std::vector<ProjectStep> &steps,
					  uint8_t &next_int64_slot,
					  const std::vector<ColumnRef> &cols,
					  const std::vector<ColumnSchema> &schema,
					  int8_t &out_scale,
					  uint8_t &out_slot)
{
	if (func == nullptr || func->children.size() != 1 || next_int64_slot >= kDataChunkMaxSlots)
		return false;
	if (pg_strcasecmp(func->function_name.c_str(), "length") != 0 &&
		pg_strcasecmp(func->function_name.c_str(), "char_length") != 0 &&
		pg_strcasecmp(func->function_name.c_str(), "character_length") != 0)
		return false;
	const auto *arg = UnwrapTransparentCastColumn(func->children[0].get());
	if (arg == nullptr)
		return false;
	ColumnRef ref{};
	const ColumnSchema *col = nullptr;
	if (!LookupBindingColumn(arg->binding, cols, schema, ref, col) ||
		col == nullptr || col->decode_kind != ColumnDecodeKind::STRING_REF)
		return false;
	out_slot = next_int64_slot++;
	out_scale = 0;
	steps.push_back(ProjectStep{ProjectOp::STRING_LENGTH, col->chunk_slot, 0, out_slot, 0});
	return true;
}

bool
LowerNumericBinaryExpr(const BoundFunctionExpression *func,
					   std::vector<ProjectStep> &steps,
					   uint8_t &next_int64_slot,
					   const std::vector<ColumnRef> &cols,
					   const std::vector<ColumnSchema> &schema,
					   const std::vector<MaterializedOptExpr> *cache,
					   int8_t &out_scale,
					   uint8_t &out_slot)
{
	if (func == nullptr || func->children.size() != 2)
		return false;
	const bool is_mul = func->function_name == "*";
	const bool is_add = func->function_name == "+";
	const bool is_sub = func->function_name == "-";
	const bool is_div = func->function_name == "/";
	if (!is_mul && !is_add && !is_sub && !is_div)
		return false;

	const Expression *lhs = func->children[0].get();
	const Expression *rhs = func->children[1].get();
	const auto *lhs_const = dynamic_cast<const BoundConstantExpression *>(lhs);
	const auto *rhs_const = dynamic_cast<const BoundConstantExpression *>(rhs);

	if (is_div)
	{
		if (next_int64_slot >= kDataChunkMaxSlots)
			return false;
		int8_t lhs_scale = 0;
		int8_t rhs_scale = 0;
		uint8_t lhs_slot = 0;
		uint8_t rhs_slot = 0;
		if (!LowerOptimizerExpr(lhs, steps, next_int64_slot, cols, schema, cache, lhs_scale, lhs_slot, true) ||
			!LowerOptimizerExpr(rhs, steps, next_int64_slot, cols, schema, cache, rhs_scale, rhs_slot, true))
			return false;
		int64_t factor = 0;
		out_scale = (lhs_const != nullptr || rhs_const != nullptr)
			? std::max<int8_t>(kProjectionConstDivisionScale, std::max(lhs_scale, rhs_scale))
			: kProjectionDivisionScale;
		if (!Pow10Int64(static_cast<int>(out_scale) + static_cast<int>(rhs_scale) - static_cast<int>(lhs_scale), factor))
			return false;
		out_slot = next_int64_slot++;
		steps.push_back(ProjectStep{ProjectOp::NUMERIC_DIV_VAR_VAR, lhs_slot, rhs_slot, out_slot, factor});
		return true;
	}

	if (is_mul)
	{
		if (lhs_const != nullptr && rhs_const != nullptr)
			return false;
		if (lhs_const != nullptr || rhs_const != nullptr)
		{
			const Expression *var_expr = lhs_const != nullptr ? rhs : lhs;
			const auto *const_expr = lhs_const != nullptr ? lhs_const : rhs_const;
			int8_t var_scale = 0;
			uint8_t var_slot = 0;
			if (!LowerOptimizerExpr(var_expr, steps, next_int64_slot, cols, schema, cache, var_scale, var_slot, true))
				return false;
			int8_t const_scale = 0;
			int64_t const_value = 0;
			if (!ScaleNumericConstantToInt64(const_expr, const_scale, const_value) || next_int64_slot >= kDataChunkMaxSlots)
				return false;
			out_slot = next_int64_slot++;
			out_scale = static_cast<int8_t>(var_scale + const_scale);
			steps.push_back(ProjectStep{ProjectOp::NUMERIC_MUL_VAR_CONST, var_slot, 0, out_slot, const_value});
			return true;
		}
		int8_t lhs_scale = 0;
		int8_t rhs_scale = 0;
		uint8_t lhs_slot = 0;
		uint8_t rhs_slot = 0;
		if (!LowerOptimizerExpr(lhs, steps, next_int64_slot, cols, schema, cache, lhs_scale, lhs_slot, true) ||
			!LowerOptimizerExpr(rhs, steps, next_int64_slot, cols, schema, cache, rhs_scale, rhs_slot, true) ||
			next_int64_slot >= kDataChunkMaxSlots)
			return false;
		out_slot = next_int64_slot++;
		out_scale = static_cast<int8_t>(lhs_scale + rhs_scale);
		steps.push_back(ProjectStep{ProjectOp::NUMERIC_MUL_VAR_VAR, lhs_slot, rhs_slot, out_slot, 0});
		return true;
	}

	if (lhs_const != nullptr && rhs_const != nullptr)
		return false;
	if (lhs_const != nullptr)
	{
		int8_t rhs_scale = 0;
		uint8_t rhs_slot = 0;
		if (!LowerOptimizerExpr(rhs, steps, next_int64_slot, cols, schema, cache, rhs_scale, rhs_slot, true))
			return false;
		int8_t lhs_scale = 0;
		int64_t lhs_value = 0;
		if (!ScaleNumericConstantToInt64(lhs_const, lhs_scale, lhs_value))
			return false;
		out_scale = static_cast<int8_t>(Max(lhs_scale, rhs_scale));
		if (!RescaleInt64Constant(lhs_value, lhs_scale, out_scale, lhs_value) || next_int64_slot >= kDataChunkMaxSlots)
			return false;
		out_slot = next_int64_slot++;
		steps.push_back(ProjectStep{
			is_sub ? ProjectOp::NUMERIC_SUB_CONST_VAR : ProjectOp::NUMERIC_ADD_CONST_VAR,
			0,
			rhs_slot,
			out_slot,
			lhs_value});
		return true;
	}
	if (rhs_const != nullptr)
	{
		int8_t lhs_scale = 0;
		uint8_t lhs_slot = 0;
		if (!LowerOptimizerExpr(lhs, steps, next_int64_slot, cols, schema, cache, lhs_scale, lhs_slot, true))
			return false;
		int8_t rhs_scale = 0;
		int64_t rhs_value = 0;
		if (!ScaleNumericConstantToInt64(rhs_const, rhs_scale, rhs_value))
			return false;
		out_scale = static_cast<int8_t>(Max(lhs_scale, rhs_scale));
		uint8_t lhs_aligned = lhs_slot;
		if (!AppendScaleProjectStep(lhs_slot, lhs_scale, out_scale, steps, next_int64_slot, lhs_aligned) ||
			!RescaleInt64Constant(rhs_value, rhs_scale, out_scale, rhs_value) ||
			next_int64_slot >= kDataChunkMaxSlots)
			return false;
		out_slot = next_int64_slot++;
		steps.push_back(ProjectStep{
			is_sub ? ProjectOp::NUMERIC_SUB_VAR_CONST : ProjectOp::NUMERIC_ADD_VAR_CONST,
			lhs_aligned,
			0,
			out_slot,
			rhs_value});
		return true;
	}
	int8_t lhs_scale = 0;
	int8_t rhs_scale = 0;
	uint8_t lhs_slot = 0;
	uint8_t rhs_slot = 0;
	if (!LowerOptimizerExpr(lhs, steps, next_int64_slot, cols, schema, cache, lhs_scale, lhs_slot, true) ||
		!LowerOptimizerExpr(rhs, steps, next_int64_slot, cols, schema, cache, rhs_scale, rhs_slot, true))
		return false;
	out_scale = static_cast<int8_t>(Max(lhs_scale, rhs_scale));
	uint8_t lhs_aligned = lhs_slot;
	uint8_t rhs_aligned = rhs_slot;
	if (!AppendScaleProjectStep(lhs_slot, lhs_scale, out_scale, steps, next_int64_slot, lhs_aligned) ||
		!AppendScaleProjectStep(rhs_slot, rhs_scale, out_scale, steps, next_int64_slot, rhs_aligned) ||
		next_int64_slot >= kDataChunkMaxSlots)
		return false;
	out_slot = next_int64_slot++;
	steps.push_back(ProjectStep{
		is_sub ? ProjectOp::NUMERIC_SUB_VAR_VAR : ProjectOp::NUMERIC_ADD_VAR_VAR,
		lhs_aligned,
		rhs_aligned,
		out_slot,
		0});
	return true;
}

bool
LowerCaseExpr(const BoundFunctionExpression *func,
			  std::vector<ProjectStep> &steps,
			  uint8_t &next_int64_slot,
			  const std::vector<ColumnRef> &cols,
			  const std::vector<ColumnSchema> &schema,
			  const std::vector<MaterializedOptExpr> *cache,
			  int8_t &out_scale,
			  uint8_t &out_slot)
{
	if (func == nullptr || func->function_name != "case" || func->children.size() < 2)
		return false;
	const auto *when_func = dynamic_cast<const BoundFunctionExpression *>(func->children[0].get());
	if (when_func == nullptr || when_func->function_name != "when" || when_func->children.size() != 2)
		return false;
	uint8_t cond_slot = 0;
	if (!LowerOptimizerBoolExpr(when_func->children[0].get(), steps, next_int64_slot, cols, schema, cond_slot))
		return false;
	int8_t then_scale = 0;
	uint8_t then_slot = 0;
	if (!LowerOptimizerExpr(when_func->children[1].get(), steps, next_int64_slot, cols, schema, cache, then_scale, then_slot, true))
		return false;
	const Expression *else_expr = func->children.back().get();
	const auto *else_const = dynamic_cast<const BoundConstantExpression *>(else_expr);
	if (else_const != nullptr)
	{
		int64_t else_value = 0;
		if (!ScaleConstantToTargetScale(else_const, then_scale, else_value) || next_int64_slot >= kDataChunkMaxSlots)
			return false;
		out_slot = next_int64_slot++;
		out_scale = then_scale;
		steps.push_back(ProjectStep{
			ProjectOp::NUMERIC_CASE_VAR_CONST,
			cond_slot,
			then_slot,
			out_slot,
			else_value});
		return true;
	}
	int8_t else_scale = 0;
	uint8_t else_slot = 0;
	if (!LowerOptimizerExpr(else_expr, steps, next_int64_slot, cols, schema, cache, else_scale, else_slot, true))
		return false;
	out_scale = static_cast<int8_t>(Max(then_scale, else_scale));
	uint8_t aligned_then = then_slot;
	uint8_t aligned_else = else_slot;
	if (!AppendScaleProjectStep(then_slot, then_scale, out_scale, steps, next_int64_slot, aligned_then) ||
		!AppendScaleProjectStep(else_slot, else_scale, out_scale, steps, next_int64_slot, aligned_else) ||
		next_int64_slot >= kDataChunkMaxSlots)
		return false;
	out_slot = next_int64_slot++;
	steps.push_back(ProjectStep{
		ProjectOp::NUMERIC_CASE_ELSE_VAR,
		cond_slot,
		aligned_then,
		out_slot,
		aligned_else});
	return true;
}

bool
LowerOptimizerExpr(const Expression *expr,
				   std::vector<ProjectStep> &steps,
				   uint8_t &next_int64_slot,
				   const std::vector<ColumnRef> &cols,
				   const std::vector<ColumnSchema> &schema,
				   const std::vector<MaterializedOptExpr> *cache,
				   int8_t &out_scale,
				   uint8_t &out_slot,
				   bool require_int64_slot)
{
	if (expr == nullptr)
		return false;
	if (LookupCachedOptimizerExpr(expr, cache, out_scale, out_slot))
		return true;
	if (expr->type == ExpressionType::BOUND_CONSTANT)
		return LowerProjectionConstant(static_cast<const BoundConstantExpression *>(expr), steps, next_int64_slot, out_scale, out_slot);
	if (expr->type == ExpressionType::BOUND_COLUMN_REF)
	{
		ColumnRef ref{};
		const ColumnSchema *col = nullptr;
		const auto *bound = static_cast<const BoundColumnRefExpression *>(expr);
		if (!LookupBindingColumn(bound->binding, cols, schema, ref, col) || col == nullptr || !ColumnNumericScale(*col, out_scale))
			return false;
		if (require_int64_slot && col->decode_kind == ColumnDecodeKind::INT32_INT4)
		{
			for (const ProjectStep &existing : steps)
			{
				if (existing.op == ProjectOp::INT32_TO_INT64_VAR && existing.in_a_chunk_slot == col->chunk_slot)
				{
					out_slot = existing.out_chunk_slot;
					return true;
				}
			}
			if (next_int64_slot >= kDataChunkMaxSlots)
				return false;
			out_slot = next_int64_slot++;
			steps.push_back(ProjectStep{ProjectOp::INT32_TO_INT64_VAR, col->chunk_slot, 0, out_slot, 0});
			return true;
		}
		out_slot = col->chunk_slot;
		return true;
	}
	if (expr->type != ExpressionType::BOUND_FUNCTION)
		return false;
	const auto *func = static_cast<const BoundFunctionExpression *>(expr);
	if (func->children.size() == 1 && IsTransparentCastFunctionName(func->function_name))
	{
		const bool cast_to_int64 = require_int64_slot ||
			func->function_name == "int8" || func->function_name == "numeric";
		return LowerOptimizerExpr(func->children[0].get(), steps, next_int64_slot, cols, schema, cache, out_scale, out_slot, cast_to_int64);
	}
	if (LowerExtractYearExpr(func, steps, next_int64_slot, cols, schema, out_scale, out_slot) ||
		LowerStringLengthExpr(func, steps, next_int64_slot, cols, schema, out_scale, out_slot) ||
		LowerCaseExpr(func, steps, next_int64_slot, cols, schema, cache, out_scale, out_slot) ||
		LowerNumericBinaryExpr(func, steps, next_int64_slot, cols, schema, cache, out_scale, out_slot))
		return true;
	return false;
}

bool
InferProjectionExprSchema(const Expression *expr,
						  const std::vector<ColumnRef> &cols,
						  const std::vector<ColumnSchema> &schema,
						  Oid &out_type_oid,
						  int32 &out_typmod,
						  int8_t &out_scale)
{
	if (expr == nullptr)
		return false;
	if (expr->type == ExpressionType::BOUND_COLUMN_REF)
	{
		ColumnRef ref{};
		const ColumnSchema *col = nullptr;
		if (!LookupBindingColumn(static_cast<const BoundColumnRefExpression *>(expr)->binding, cols, schema, ref, col) || col == nullptr)
			return false;
		out_type_oid = col->type_oid;
		out_typmod = col->typmod;
		return ColumnNumericScale(*col, out_scale);
	}
	if (expr->type == ExpressionType::BOUND_CONSTANT)
	{
		const auto *constant = static_cast<const BoundConstantExpression *>(expr);
		if (pg_strcasecmp(constant->value.c_str(), "true") == 0 || pg_strcasecmp(constant->value.c_str(), "false") == 0)
		{
			out_type_oid = BOOLOID;
			out_typmod = -1;
			out_scale = 0;
			return true;
		}
		if (constant->value.empty())
		{
			out_type_oid = TEXTOID;
			out_typmod = -1;
			out_scale = 0;
			return true;
		}
		{
			const char *p = constant->value.c_str();
			char *end = nullptr;
			errno = 0;
			(void) std::strtoll(p, &end, 10);
			if (errno == 0 && end != p && *end == '\0')
			{
				out_type_oid = INT8OID;
				out_typmod = -1;
				out_scale = 0;
				return true;
			}
		}
		if (constant->value.find('.') != std::string::npos)
		{
			int8_t num_scale = 0;
			int64_t num_val = 0;
			if (ScaleNumericConstantToInt64(constant, num_scale, num_val))
			{
				out_type_oid = NUMERICOID;
				out_typmod = -1;
				out_scale = num_scale;
				return true;
			}
		}
		out_type_oid = TEXTOID;
		out_typmod = -1;
		out_scale = 0;
		return true;
	}
	if (expr->type != ExpressionType::BOUND_FUNCTION)
		return false;
	const auto *func = static_cast<const BoundFunctionExpression *>(expr);
	if (func->children.size() == 1 && IsTransparentCastFunctionName(func->function_name))
		return InferProjectionExprSchema(func->children[0].get(), cols, schema, out_type_oid, out_typmod, out_scale);
	if (func->function_name == "case")
	{
		if (func->children.size() < 2)
			return false;
		size_t when_idx = 0;
		if (dynamic_cast<const BoundFunctionExpression *>(func->children[0].get()) == nullptr ||
			static_cast<const BoundFunctionExpression *>(func->children[0].get())->function_name != "when")
			when_idx = 1;
		if (when_idx >= func->children.size())
			return false;
		const auto *when_func = dynamic_cast<const BoundFunctionExpression *>(func->children[when_idx].get());
		if (when_func == nullptr || when_func->function_name != "when" || when_func->children.size() != 2)
			return false;
		Oid then_type = InvalidOid;
		Oid else_type = InvalidOid;
		int32 then_typmod = -1;
		int32 else_typmod = -1;
		int8_t then_scale = 0;
		int8_t else_scale = 0;
		if (!InferProjectionExprSchema(when_func->children[1].get(), cols, schema,
									   then_type, then_typmod, then_scale) ||
			!InferProjectionExprSchema(func->children.back().get(), cols, schema,
									   else_type, else_typmod, else_scale))
			return false;
		const bool then_str = then_type == TEXTOID || then_type == VARCHAROID || then_type == BPCHAROID;
		const bool else_str = else_type == TEXTOID || else_type == VARCHAROID || else_type == BPCHAROID;
		if (then_str && (else_str || func->children.back()->type == ExpressionType::BOUND_CONSTANT))
		{
			out_type_oid = TEXTOID;
			out_typmod = -1;
			out_scale = 0;
			return true;
		}
		const bool then_integral = then_type == BOOLOID || then_type == INT2OID || then_type == INT4OID || then_type == INT8OID;
		const bool else_integral = else_type == BOOLOID || else_type == INT2OID || else_type == INT4OID || else_type == INT8OID;
		if (then_integral && else_integral)
		{
			out_type_oid = INT8OID;
			out_typmod = -1;
			out_scale = 0;
			return true;
		}
		if (then_type == NUMERICOID || else_type == NUMERICOID)
		{
			out_type_oid = NUMERICOID;
			out_typmod = -1;
			out_scale = static_cast<int8_t>(Max(then_scale, else_scale));
			return true;
		}
		if (then_type == else_type)
		{
			out_type_oid = then_type;
			out_typmod = then_typmod;
			out_scale = then_scale;
			return true;
		}
		return false;
	}
	if (func->function_name == "extract" || func->function_name == "date_part")
	{
		out_type_oid = NUMERICOID;
		out_typmod = -1;
		out_scale = 0;
		return true;
	}
	if (pg_strcasecmp(func->function_name.c_str(), "date_trunc") == 0)
	{
		out_type_oid = TIMESTAMPOID;
		out_typmod = -1;
		out_scale = 0;
		return true;
	}
	if (pg_strcasecmp(func->function_name.c_str(), "length") == 0 ||
		pg_strcasecmp(func->function_name.c_str(), "char_length") == 0 ||
		pg_strcasecmp(func->function_name.c_str(), "character_length") == 0)
	{
		out_type_oid = INT4OID;
		out_typmod = -1;
		out_scale = 0;
		return true;
	}
	if (pg_strcasecmp(func->function_name.c_str(), "regexp_replace") == 0)
	{
		out_type_oid = TEXTOID;
		out_typmod = -1;
		out_scale = 0;
		return true;
	}
	if (func->function_name == "+" || func->function_name == "-" ||
		func->function_name == "*" || func->function_name == "/")
	{
		if (func->children.size() != 2)
			return false;
		Oid lhs_type = InvalidOid;
		Oid rhs_type = InvalidOid;
		int32 lhs_typmod = -1;
		int32 rhs_typmod = -1;
		int8_t lhs_scale = 0;
		int8_t rhs_scale = 0;
		if (!InferProjectionExprSchema(func->children[0].get(), cols, schema, lhs_type, lhs_typmod, lhs_scale) ||
			!InferProjectionExprSchema(func->children[1].get(), cols, schema, rhs_type, rhs_typmod, rhs_scale))
			return false;
		const Oid op_rettype = OidIsValid(func->op_oid) ? get_op_rettype(func->op_oid) : InvalidOid;
		if (op_rettype == INT2OID || op_rettype == INT4OID || op_rettype == INT8OID)
		{
			out_type_oid = op_rettype;
			out_typmod = -1;
			out_scale = 0;
			return true;
		}
		out_type_oid = NUMERICOID;
		out_typmod = -1;
		if (func->function_name == "*")
			out_scale = static_cast<int8_t>(lhs_scale + rhs_scale);
		else if (func->function_name == "/")
			out_scale = kProjectionDivisionScale;
		else
			out_scale = static_cast<int8_t>(Max(lhs_scale, rhs_scale));
		return true;
	}
	if (func->function_name == "=" || func->function_name == "<>" || func->function_name == "!=" ||
		func->function_name == "~~")
	{
		out_type_oid = BOOLOID;
		out_typmod = -1;
		out_scale = 0;
		return true;
	}
	if (func->function_name == "prefix_slice")
	{
		out_type_oid = TEXTOID;
		out_typmod = -1;
		out_scale = 0;
		return true;
	}
	return false;
}

uint8_t
NextFreeInt64Slot(const std::vector<ColumnSchema> &schema)
{
	uint8_t next_slot = 0;
	for (const ColumnSchema &col : schema)
	{
		if (col.decode_kind != ColumnDecodeKind::INT64_INT8 &&
			col.decode_kind != ColumnDecodeKind::INT64_NUMERIC_SCALED)
			continue;
		const uint8_t candidate = static_cast<uint8_t>(col.chunk_slot + 1);
		if (candidate > next_slot)
			next_slot = candidate;
	}
	return next_slot;
}

uint8_t
NextFreeStringSlot(const std::vector<ColumnSchema> &schema)
{
	uint8_t next_slot = 0;
	for (const ColumnSchema &col : schema)
	{
		if (col.decode_kind != ColumnDecodeKind::STRING_REF)
			continue;
		const uint8_t candidate = static_cast<uint8_t>(col.chunk_slot + 1);
		if (candidate > next_slot)
			next_slot = candidate;
	}
	return next_slot;
}

bool
ClassifyOptimizerAggregate(const BoundAggregateExpression *agg,
						   const std::vector<yaap::PhysicalOperator::OutputColumn> *input_outputs,
						   const std::vector<ColumnRef> &cols,
						   const std::vector<ColumnSchema> &schema,
						   std::vector<ProjectStep> &project_steps,
						   std::vector<ProjectExprDesc> &project_exprs,
						   std::vector<MaterializedOptExpr> &materialized_exprs,
						   uint8_t &next_int64_slot,
						   AggFuncDesc &out_desc,
						   TdcAggKind &out_kind,
						   int16_t &out_numeric_scale,
						   Oid &out_type_oid,
						   bool &out_input_is_string)
{
	if (agg == nullptr)
		return false;
	out_desc = AggFuncDesc{static_cast<Oid>(agg->agg_oid), InvalidOid, InvalidOid, 0, 0};
	out_numeric_scale = 0;
	out_type_oid = InvalidOid;
	out_input_is_string = false;

	if (agg->children.empty())
	{
		if (pg_strcasecmp(agg->function_name.c_str(), "count") == 0)
		{
			out_kind = TdcAggKind::COUNT_STAR;
			return true;
		}
		return false;
	}
	if (agg->children.size() != 1)
		return false;

	const Expression *arg = agg->children[0].get();
	const auto *bare_arg = UnwrapTransparentCastColumn(arg);
	ColumnRef bare_ref{};
	const ColumnSchema *bare_col = nullptr;
	bool is_bare_ref = false;
	if (bare_arg != nullptr)
	{
		is_bare_ref = LookupNamedExprInputColumn(bare_arg, input_outputs, cols, schema, bare_ref, bare_col);
		if (pg_yaap_trace_hooks && agg->is_distinct)
		{
			elog(LOG,
				 "pg_yaap: count distinct lookup expr=%s.%s binding=(%zu,%zu) matched=%d decode=%d slot=%u typmod=%d input_outputs=%zu",
				 bare_arg->table_name.c_str(),
				 bare_arg->column_name.c_str(),
				 bare_arg->binding.table_index.index,
				 bare_arg->binding.column_index.index,
				 is_bare_ref ? 1 : 0,
				 bare_col != nullptr ? static_cast<int>(bare_col->decode_kind) : -1,
				 bare_col != nullptr ? static_cast<unsigned>(bare_col->chunk_slot) : 0,
				 bare_col != nullptr ? bare_col->typmod : -1,
				 input_outputs != nullptr ? input_outputs->size() : 0);
			if (input_outputs != nullptr)
			{
				for (size_t i = 0; i < input_outputs->size(); ++i)
					elog(LOG,
						 "pg_yaap: count distinct available_output[%zu]=%s.%s binding=(%zu,%zu)",
						 i,
						 (*input_outputs)[i].table_name.c_str(),
						 (*input_outputs)[i].column_name.c_str(),
						 (*input_outputs)[i].binding.table_index.index,
						 (*input_outputs)[i].binding.column_index.index);
			}
		}
	}

	if (pg_strcasecmp(agg->function_name.c_str(), "count") == 0)
	{
		if (!is_bare_ref || bare_col == nullptr)
		{
			if (pg_yaap_trace_hooks && bare_arg != nullptr && agg->is_distinct)
			{
				elog(LOG,
					 "pg_yaap: count distinct classify miss input_outputs=%zu expr=%s.%s binding=(%zu,%zu) cols=%zu schema=%zu",
					 input_outputs != nullptr ? input_outputs->size() : 0,
					 bare_arg->table_name.c_str(),
					 bare_arg->column_name.c_str(),
					 bare_arg->binding.table_index.index,
					 bare_arg->binding.column_index.index,
					 cols.size(),
					 schema.size());
				if (input_outputs != nullptr)
				{
					for (size_t i = 0; i < input_outputs->size(); ++i)
						elog(LOG,
							 "pg_yaap: count distinct input_output[%zu]=%s.%s binding=(%zu,%zu)",
							 i,
							 (*input_outputs)[i].table_name.c_str(),
							 (*input_outputs)[i].column_name.c_str(),
							 (*input_outputs)[i].binding.table_index.index,
							 (*input_outputs)[i].binding.column_index.index);
				}
			}
			return false;
		}
		if (bare_col->decode_kind == ColumnDecodeKind::INT32_INT4)
		{
			if (next_int64_slot >= kDataChunkMaxSlots)
				return false;
			const uint16_t first = static_cast<uint16_t>(project_steps.size());
			const uint8_t cast_slot = next_int64_slot++;
			project_steps.push_back(ProjectStep{ProjectOp::INT32_TO_INT64_VAR, bare_col->chunk_slot, 0, cast_slot, 0});
			project_exprs.push_back(ProjectExprDesc{first, 1, cast_slot, 0, 0});
			materialized_exprs.push_back(MaterializedOptExpr{arg, 0, cast_slot});
			out_desc.input_col_idx = cast_slot;
		}
		else if (bare_col->decode_kind == ColumnDecodeKind::INT64_INT8 ||
				 bare_col->decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
		{
			out_desc.input_col_idx = bare_col->chunk_slot;
		}
		else if (bare_col->decode_kind == ColumnDecodeKind::STRING_REF)
		{
			out_desc.input_col_idx = bare_col->chunk_slot;
			out_input_is_string = true;
		}
		else
		{
			if (pg_yaap_trace_hooks && bare_arg != nullptr)
				elog(LOG,
					 "pg_yaap: count distinct unsupported decode=%d expr=%s.%s binding=(%zu,%zu) slot=%u typmod=%d",
					 static_cast<int>(bare_col->decode_kind),
					 bare_arg->table_name.c_str(),
					 bare_arg->column_name.c_str(),
					 bare_arg->binding.table_index.index,
					 bare_arg->binding.column_index.index,
					 static_cast<unsigned>(bare_col->chunk_slot),
					 bare_col->typmod);
			return false;
		}
		out_kind = agg->is_distinct ? TdcAggKind::COUNT_DISTINCT_NONNULL : TdcAggKind::COUNT_NONNULL;
		return true;
	}

	if (pg_strcasecmp(agg->function_name.c_str(), "sum") == 0)
	{
		Oid arg_type_oid = InvalidOid;
		int32 arg_typmod = -1;
		int8_t arg_scale = 0;
		const bool intlike_expr =
			InferProjectionExprSchema(arg, cols, schema, arg_type_oid, arg_typmod, arg_scale) &&
			arg_scale == 0 &&
			(arg_type_oid == BOOLOID || arg_type_oid == INT2OID || arg_type_oid == INT4OID || arg_type_oid == INT8OID);
		if (is_bare_ref && bare_col != nullptr &&
			(bare_col->decode_kind == ColumnDecodeKind::INT32_INT4 || bare_col->decode_kind == ColumnDecodeKind::INT64_INT8))
		{
			if (bare_col->decode_kind == ColumnDecodeKind::INT32_INT4)
			{
				if (next_int64_slot >= kDataChunkMaxSlots)
					return false;
				const uint16_t first = static_cast<uint16_t>(project_steps.size());
				const uint8_t cast_slot = next_int64_slot++;
				project_steps.push_back(ProjectStep{ProjectOp::INT32_TO_INT64_VAR, bare_col->chunk_slot, 0, cast_slot, 0});
				project_exprs.push_back(ProjectExprDesc{first, 1, cast_slot, 0, 0});
				materialized_exprs.push_back(MaterializedOptExpr{arg, 0, cast_slot});
				out_desc.input_col_idx = cast_slot;
			}
			else
				out_desc.input_col_idx = bare_col->chunk_slot;
			out_kind = TdcAggKind::SUM_INT64;
			return true;
		}
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: classify sum fallback bare_ref=%d decode=%d typmod=%d arg_type=%d",
				 is_bare_ref ? 1 : 0,
				 bare_col != nullptr ? static_cast<int>(bare_col->decode_kind) : -1,
				 bare_col != nullptr ? bare_col->typmod : -1,
				 arg != nullptr ? static_cast<int>(arg->type) : -1);
		out_kind = intlike_expr ? TdcAggKind::SUM_INT64 : TdcAggKind::SUM_NUMERIC;
	}
	else if (pg_strcasecmp(agg->function_name.c_str(), "avg") == 0)
	{
		out_kind = TdcAggKind::AVG_NUMERIC;
		if (is_bare_ref && bare_col != nullptr && bare_col->decode_kind == ColumnDecodeKind::INT32_INT4)
		{
			if (next_int64_slot >= kDataChunkMaxSlots)
				return false;
			const uint16_t first = static_cast<uint16_t>(project_steps.size());
			const uint8_t cast_slot = next_int64_slot++;
			project_steps.push_back(ProjectStep{ProjectOp::INT32_TO_INT64_VAR, bare_col->chunk_slot, 0, cast_slot, 0});
			project_exprs.push_back(ProjectExprDesc{first, 1, cast_slot, 0, 0});
			materialized_exprs.push_back(MaterializedOptExpr{arg, 0, cast_slot});
			out_desc.input_col_idx = cast_slot;
			out_numeric_scale = static_cast<int16_t>(kAvgNumericExtraScale);
			return true;
		}
	}
	else if (pg_strcasecmp(agg->function_name.c_str(), "min") == 0)
	{
		Oid arg_type_oid = InvalidOid;
		int32 arg_typmod = -1;
		int8_t arg_scale = 0;
		const bool intlike_expr =
			InferProjectionExprSchema(arg, cols, schema, arg_type_oid, arg_typmod, arg_scale) &&
			arg_scale == 0 &&
			(arg_type_oid == BOOLOID || arg_type_oid == INT2OID || arg_type_oid == INT4OID ||
			 arg_type_oid == INT8OID || arg_type_oid == DATEOID || arg_type_oid == TIMESTAMPOID);
		if (is_bare_ref && bare_col != nullptr &&
			(bare_col->decode_kind == ColumnDecodeKind::INT32_INT4 ||
			 bare_col->decode_kind == ColumnDecodeKind::INT32_DATE ||
			 bare_col->decode_kind == ColumnDecodeKind::INT64_INT8))
		{
			if (bare_col->decode_kind == ColumnDecodeKind::INT32_INT4 ||
				bare_col->decode_kind == ColumnDecodeKind::INT32_DATE)
			{
				if (next_int64_slot >= kDataChunkMaxSlots)
					return false;
				const uint16_t first = static_cast<uint16_t>(project_steps.size());
				const uint8_t cast_slot = next_int64_slot++;
				project_steps.push_back(ProjectStep{ProjectOp::INT32_TO_INT64_VAR, bare_col->chunk_slot, 0, cast_slot, 0});
				project_exprs.push_back(ProjectExprDesc{first, 1, cast_slot, 0, 0});
				materialized_exprs.push_back(MaterializedOptExpr{arg, 0, cast_slot});
				out_desc.input_col_idx = cast_slot;
			}
			else
				out_desc.input_col_idx = bare_col->chunk_slot;
			out_kind = TdcAggKind::MIN_INT64;
			out_type_oid = bare_col->type_oid;
			return true;
		}
		if (is_bare_ref && bare_col != nullptr && bare_col->decode_kind == ColumnDecodeKind::STRING_REF)
		{
			out_desc.input_col_idx = bare_col->chunk_slot;
			out_kind = TdcAggKind::MIN_STRING;
			out_type_oid = bare_col->type_oid;
			out_input_is_string = true;
			return true;
		}
		out_kind = intlike_expr ? TdcAggKind::MIN_INT64 : TdcAggKind::MIN_NUMERIC;
		if (intlike_expr)
			out_type_oid = arg_type_oid;
	}
	else if (pg_strcasecmp(agg->function_name.c_str(), "max") == 0)
	{
		Oid arg_type_oid = InvalidOid;
		int32 arg_typmod = -1;
		int8_t arg_scale = 0;
		const bool intlike_expr =
			InferProjectionExprSchema(arg, cols, schema, arg_type_oid, arg_typmod, arg_scale) &&
			arg_scale == 0 &&
			(arg_type_oid == BOOLOID || arg_type_oid == INT2OID || arg_type_oid == INT4OID ||
			 arg_type_oid == INT8OID || arg_type_oid == DATEOID || arg_type_oid == TIMESTAMPOID);
		if (is_bare_ref && bare_col != nullptr &&
			(bare_col->decode_kind == ColumnDecodeKind::INT32_INT4 ||
			 bare_col->decode_kind == ColumnDecodeKind::INT32_DATE ||
			 bare_col->decode_kind == ColumnDecodeKind::INT64_INT8))
		{
			if (bare_col->decode_kind == ColumnDecodeKind::INT32_INT4 ||
				bare_col->decode_kind == ColumnDecodeKind::INT32_DATE)
			{
				if (next_int64_slot >= kDataChunkMaxSlots)
					return false;
				const uint16_t first = static_cast<uint16_t>(project_steps.size());
				const uint8_t cast_slot = next_int64_slot++;
				project_steps.push_back(ProjectStep{ProjectOp::INT32_TO_INT64_VAR, bare_col->chunk_slot, 0, cast_slot, 0});
				project_exprs.push_back(ProjectExprDesc{first, 1, cast_slot, 0, 0});
				materialized_exprs.push_back(MaterializedOptExpr{arg, 0, cast_slot});
				out_desc.input_col_idx = cast_slot;
			}
			else
				out_desc.input_col_idx = bare_col->chunk_slot;
			out_kind = TdcAggKind::MAX_INT64;
			out_type_oid = bare_col->type_oid;
			return true;
		}
		if (is_bare_ref && bare_col != nullptr && bare_col->decode_kind == ColumnDecodeKind::STRING_REF)
		{
			out_desc.input_col_idx = bare_col->chunk_slot;
			out_kind = TdcAggKind::MAX_STRING;
			out_type_oid = bare_col->type_oid;
			out_input_is_string = true;
			return true;
		}
		out_kind = intlike_expr ? TdcAggKind::MAX_INT64 : TdcAggKind::MAX_NUMERIC;
		if (intlike_expr)
			out_type_oid = arg_type_oid;
	}
	else
		return false;

	if (is_bare_ref && bare_col != nullptr)
	{
		int8_t scale = 0;
		if (!ColumnNumericScale(*bare_col, scale))
		{
			if (pg_yaap_trace_hooks)
				elog(LOG,
					 "pg_yaap: classify aggregate bare ref scale lookup failed fn=%s decode=%d typmod=%d",
					 agg->function_name.c_str(),
					 static_cast<int>(bare_col->decode_kind),
					 bare_col->typmod);
			return false;
		}
		out_desc.input_col_idx = bare_col->chunk_slot;
		out_numeric_scale = (out_kind == TdcAggKind::AVG_NUMERIC) ? static_cast<int16_t>(scale + kAvgNumericExtraScale) : scale;
		return true;
	}

	int8_t lowered_scale = 0;
	uint8_t lowered_slot = 0;
	if (LookupCachedOptimizerExpr(arg, &materialized_exprs, lowered_scale, lowered_slot))
	{
		out_desc.input_col_idx = lowered_slot;
		out_numeric_scale = (out_kind == TdcAggKind::AVG_NUMERIC) ? static_cast<int16_t>(lowered_scale + kAvgNumericExtraScale) : lowered_scale;
		return true;
	}

	const uint16_t first_step_idx = static_cast<uint16_t>(project_steps.size());
	if (!LowerOptimizerExpr(arg, project_steps, next_int64_slot, cols, schema, &materialized_exprs, lowered_scale, lowered_slot) ||
		project_steps.size() == first_step_idx)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: classify aggregate expr lowering failed fn=%s arg_type=%d steps_before=%u steps_after=%zu",
				 agg->function_name.c_str(),
				 arg != nullptr ? static_cast<int>(arg->type) : -1,
				 first_step_idx,
				 project_steps.size());
		return false;
	}
	project_exprs.push_back(ProjectExprDesc{
		first_step_idx,
		static_cast<uint16_t>(project_steps.size() - first_step_idx),
		lowered_slot,
		lowered_scale,
		0});
	materialized_exprs.push_back(MaterializedOptExpr{arg, lowered_scale, lowered_slot});
	out_desc.input_col_idx = lowered_slot;
	out_numeric_scale = (out_kind == TdcAggKind::AVG_NUMERIC) ? static_cast<int16_t>(lowered_scale + kAvgNumericExtraScale) : lowered_scale;
	return true;
}

bool
BuildOptimizerAggOutput(const PhysicalHashAggregate &agg,
						const std::vector<yaap::PhysicalOperator::OutputColumn> *input_outputs,
						const std::vector<ColumnRef> &input_cols,
						const std::vector<ColumnSchema> &input_schema,
						const AggBuildState &agg_state,
						std::vector<ColumnRef> &out_cols,
						std::vector<ColumnSchema> &out_schema)
{
	out_cols.clear();
	out_schema.clear();
	if (agg.groups.size() + agg.expressions.size() > kDataChunkMaxSlots)
		return false;

	for (size_t i = 0; i < agg.groups.size(); ++i)
	{
		ColumnSchema cs{};
		if (i < agg_state.group_schemas.size())
		{
			cs = agg_state.group_schemas[i];
		}
		else
		{
			const auto *col_expr = dynamic_cast<const BoundColumnRefExpression *>(agg.groups[i]);
			if (col_expr == nullptr)
				return false;
			ColumnRef ref{};
			const ColumnSchema *src = nullptr;
			if ((!LookupNamedExprInputColumn(col_expr, input_outputs, input_cols, input_schema, ref, src) ||
				 src == nullptr) &&
				(!LookupBindingColumn(col_expr->binding, input_cols, input_schema, ref, src) ||
				 src == nullptr))
				return false;
			cs = *src;
		}
		cs.chunk_slot = static_cast<uint8_t>(i);
		cs.src_attno = 0;
		out_schema.push_back(cs);
		out_cols.push_back(BindingToColumnRef(yaap::ColumnBinding{
			agg.group_index,
			yaap::ProjectionIndex{i}}));
	}

	for (size_t i = 0; i < agg_state.agg_kinds.size(); ++i)
	{
		ColumnSchema cs{};
		cs.chunk_slot = static_cast<uint8_t>(agg.groups.size() + i);
		cs.src_attno = 0;
		cs._pad0 = 0;
		switch (agg_state.agg_kinds[i])
		{
			case TdcAggKind::COUNT_STAR:
			case TdcAggKind::COUNT_NONNULL:
			case TdcAggKind::COUNT_DISTINCT_NONNULL:
			case TdcAggKind::SUM_INT64:
				cs.type_oid = INT8OID;
				cs.typmod = -1;
				cs.typlen = 8;
				cs.typbyval = true;
				cs.decode_kind = ColumnDecodeKind::INT64_INT8;
				break;
			case TdcAggKind::MIN_INT64:
			case TdcAggKind::MAX_INT64:
			{
				const Oid oid = (i < agg_state.agg_out_type_oids.size() && OidIsValid(agg_state.agg_out_type_oids[i]))
					? agg_state.agg_out_type_oids[i] : INT8OID;
				cs.type_oid = oid;
				cs.typmod = -1;
				cs.typlen = (oid == INT2OID) ? 2 : ((oid == INT4OID || oid == DATEOID) ? 4 : 8);
				cs.typbyval = true;
				cs.decode_kind = ColumnDecodeKind::INT64_INT8;
				break;
			}
			case TdcAggKind::SUM_NUMERIC:
			case TdcAggKind::AVG_NUMERIC:
				cs.type_oid = NUMERICOID;
				cs.typmod = MakeNumericTypmod(38, agg_state.agg_numeric_scales[i]);
				cs.typlen = -1;
				cs.typbyval = false;
				cs.decode_kind = ColumnDecodeKind::INT64_NUMERIC_SCALED;
				break;
			case TdcAggKind::MIN_NUMERIC:
			case TdcAggKind::MAX_NUMERIC:
				cs.type_oid = NUMERICOID;
				cs.typmod = MakeNumericTypmod(18, agg_state.agg_numeric_scales[i]);
				cs.typlen = -1;
				cs.typbyval = false;
				cs.decode_kind = ColumnDecodeKind::INT64_NUMERIC_SCALED;
				break;
			case TdcAggKind::MIN_STRING:
			case TdcAggKind::MAX_STRING:
			{
				const Oid oid = (i < agg_state.agg_out_type_oids.size() && OidIsValid(agg_state.agg_out_type_oids[i]))
					? agg_state.agg_out_type_oids[i] : TEXTOID;
				cs.type_oid = oid;
				cs.typmod = -1;
				cs.typlen = -1;
				cs.typbyval = false;
				cs.decode_kind = ColumnDecodeKind::STRING_REF;
				break;
			}
			default:
				return false;
		}
		out_schema.push_back(cs);
		out_cols.push_back(ColumnRef{
			static_cast<Index>(agg.aggregate_index.index + 1),
			static_cast<AttrNumber>(i + 1)});
	}

	return out_cols.size() == out_schema.size();
}

bool
CollectJoinKeys(const Expression *expr,
				const std::vector<yaap::PhysicalOperator::OutputColumn> *left_outputs,
				const std::vector<ColumnRef> &left_cols,
				const std::vector<ColumnSchema> &left_schema,
				const std::vector<yaap::PhysicalOperator::OutputColumn> *right_outputs,
				const std::vector<ColumnRef> &right_cols,
				const std::vector<ColumnSchema> &right_schema,
				std::vector<ColumnRef> &left_keys,
				std::vector<ColumnRef> &right_keys,
				std::vector<Expression *> &residuals)
{
	if (expr == nullptr)
		return false;
	if (expr->type == ExpressionType::BOUND_CONJUNCTION)
	{
		const auto *conj = static_cast<const BoundConjunctionExpression *>(expr);
		if (conj->bool_expr_type != AND_EXPR)
		{
			residuals.push_back(const_cast<Expression *>(expr));
			return true;
		}
		for (const auto &child : conj->children)
		{
			if (!CollectJoinKeys(child.get(),
								 left_outputs,
								 left_cols,
								 left_schema,
								 right_outputs,
								 right_cols,
								 right_schema,
								 left_keys,
								 right_keys,
								 residuals))
				return false;
		}
		return true;
	}
	if (expr->type != ExpressionType::BOUND_FUNCTION)
	{
		residuals.push_back(const_cast<Expression *>(expr));
		return true;
	}
	const auto *func = static_cast<const BoundFunctionExpression *>(expr);
	if (func->function_name != "=" || func->children.size() != 2)
	{
		residuals.push_back(const_cast<Expression *>(expr));
		return true;
	}
	const auto *lhs = dynamic_cast<const BoundColumnRefExpression *>(func->children[0].get());
	const auto *rhs = dynamic_cast<const BoundColumnRefExpression *>(func->children[1].get());
	if (lhs == nullptr || rhs == nullptr)
	{
		residuals.push_back(const_cast<Expression *>(expr));
		return true;
	}

	ColumnRef lhs_left_ref{};
	ColumnRef lhs_right_ref{};
	ColumnRef rhs_left_ref{};
	ColumnRef rhs_right_ref{};
	const ColumnSchema *lhs_left_schema = nullptr;
	const ColumnSchema *lhs_right_schema = nullptr;
	const ColumnSchema *rhs_left_schema = nullptr;
	const ColumnSchema *rhs_right_schema = nullptr;
	const bool lhs_left = LookupNamedExprInputColumn(lhs, left_outputs, left_cols, left_schema, lhs_left_ref, lhs_left_schema);
	const bool lhs_right = LookupNamedExprInputColumn(lhs, right_outputs, right_cols, right_schema, lhs_right_ref, lhs_right_schema);
	const bool rhs_left = LookupNamedExprInputColumn(rhs, left_outputs, left_cols, left_schema, rhs_left_ref, rhs_left_schema);
	const bool rhs_right = LookupNamedExprInputColumn(rhs, right_outputs, right_cols, right_schema, rhs_right_ref, rhs_right_schema);
	if (lhs_left && rhs_right &&
		lhs_left_schema != nullptr && rhs_right_schema != nullptr &&
		lhs_left_schema->decode_kind == rhs_right_schema->decode_kind)
	{
		left_keys.push_back(lhs_left_ref);
		right_keys.push_back(rhs_right_ref);
		return true;
	}
	if (lhs_right && rhs_left &&
		lhs_right_schema != nullptr && rhs_left_schema != nullptr &&
		lhs_right_schema->decode_kind == rhs_left_schema->decode_kind)
	{
		left_keys.push_back(rhs_left_ref);
		right_keys.push_back(lhs_right_ref);
		return true;
	}
	if (pg_yaap_trace_hooks &&
		((lhs_left || lhs_right) && (rhs_left || rhs_right)))
		elog(LOG,
			 "pg_yaap: collect join keys schema mismatch lhs=(%zu,%zu left=%d right=%d) rhs=(%zu,%zu left=%d right=%d) lhs_left_decode=%d lhs_right_decode=%d rhs_left_decode=%d rhs_right_decode=%d",
			 lhs->binding.table_index.index,
			 lhs->binding.column_index.index,
			 lhs_left ? 1 : 0,
			 lhs_right ? 1 : 0,
			 rhs->binding.table_index.index,
			 rhs->binding.column_index.index,
			 rhs_left ? 1 : 0,
			 rhs_right ? 1 : 0,
			 lhs_left_schema != nullptr ? static_cast<int>(lhs_left_schema->decode_kind) : -1,
			 lhs_right_schema != nullptr ? static_cast<int>(lhs_right_schema->decode_kind) : -1,
			 rhs_left_schema != nullptr ? static_cast<int>(rhs_left_schema->decode_kind) : -1,
			 rhs_right_schema != nullptr ? static_cast<int>(rhs_right_schema->decode_kind) : -1);
	if (pg_yaap_trace_hooks)
		elog(LOG,
			 "pg_yaap: collect join keys side miss lhs=(%zu,%zu left=%d right=%d) rhs=(%zu,%zu left=%d right=%d)",
			 lhs->binding.table_index.index,
			 lhs->binding.column_index.index,
			 lhs_left ? 1 : 0,
			 lhs_right ? 1 : 0,
			 rhs->binding.table_index.index,
			 rhs->binding.column_index.index,
			 rhs_left ? 1 : 0,
			 rhs_right ? 1 : 0);
	residuals.push_back(const_cast<Expression *>(expr));
	return true;
}

static bool
HasOuterHashJoin(const PipelineOperator *op)
{
	if (op == nullptr)
		return false;
	if (op->type() == pg_yaap::pipeline::PhysicalOperatorType::HASH_JOIN) {
		const auto *join = static_cast<const PipelineHashJoin *>(op);
		if (join->join_mode() == pg_yaap::pipeline::HashJoinMatchMode::LEFT ||
			join->join_mode() == pg_yaap::pipeline::HashJoinMatchMode::RIGHT)
			return true;
	}
	for (const auto &child : op->children())
		if (HasOuterHashJoin(child.get()))
			return true;
	return false;
}

std::unique_ptr<PipelineOperator>
BuildOutputContract(OptimizerNodeTranslation &node,
					QueryDesc *queryDesc,
					PgYaapQueryState *state)
{
	if (node.op == nullptr || state == nullptr || state->runtime_dsa == nullptr || node.schema.empty())
		return nullptr;
	TupleDataLayout output_layout;
	if (!BuildColumnOnlyLayout(node.schema, output_layout))
		return nullptr;
	if (HasOuterHashJoin(node.op.get()))
		TupleDataLayoutEnableValidity(&output_layout);
	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(node.schema, state->runtime_dsa);
	dsa_pointer output_layout_dp = SerializeTupleDataLayout(output_layout, state->runtime_dsa);
	if (!DsaPointerIsValid(output_schema_dp) || !DsaPointerIsValid(output_layout_dp))
		return nullptr;
	const bool topn_output =
		!node.final_sort_keys.empty() &&
		node.limit_count > 0;
	const bool unsorted_limit_output =
		node.final_sort_keys.empty() &&
		node.limit_count > 0;
	const uint64_t keep_rows_u64 = (node.limit_count > 0)
		? (node.limit_offset <= UINT64_MAX - node.limit_count
			? node.limit_offset + node.limit_count
			: UINT64_MAX)
		: 0;
	uint32_t row_capacity = EstimateInitialResultRows(queryDesc, node.estimated_groups);
	if (topn_output)
		row_capacity = static_cast<uint32_t>(std::max<uint64_t>(1, std::min<uint64_t>(row_capacity, node.limit_count)));
	else if (unsorted_limit_output && keep_rows_u64 <= 4096)
		row_capacity = static_cast<uint32_t>(std::max<uint64_t>(1, keep_rows_u64));
	dsa_pointer segment_arena_dp = InvalidDsaPointer;
	dsa_pointer buffer_state_dp = InvalidDsaPointer;
	if (state->runtime_dsm != nullptr)
	{
		shm_toc *toc = shm_toc_attach(pipeline::PIPELINE_DSM_MAGIC,
			dsm_segment_address(state->runtime_dsm));
		if (toc != nullptr)
		{
			auto *control = static_cast<pipeline::PipelineSharedControl *>(
				shm_toc_lookup(toc, pipeline::PIPELINE_DSM_KEY_CONTROL, true));
			if (control != nullptr)
			{
				segment_arena_dp = control->shared_segment_arena_root;
				buffer_state_dp = control->buffer_state_root;
			}
		}
	}
	/* Small Top-N retains a bounded replacement table; unsorted small LIMIT
	 * stops accepting rows after offset + limit; larger ordered output streams
	 * segments into PostgreSQL external sort on the leader. */
	if ((topn_output && node.limit_count <= 4096) ||
		(unsorted_limit_output && keep_rows_u64 <= 4096))
	{
		segment_arena_dp = InvalidDsaPointer;
		buffer_state_dp = InvalidDsaPointer;
	}
	const dsa_pointer output_payload_dp = BuildOutputTdc(state->runtime_dsa,
		output_layout_dp, output_layout, row_capacity, segment_arena_dp, buffer_state_dp);
	if (!DsaPointerIsValid(output_payload_dp))
		return nullptr;
	if (auto *output_payload = static_cast<pipeline::OutputSharedPayload *>(
			dsa_get_address(state->runtime_dsa, output_payload_dp)))
		output_payload->skip_emit_rows = node.limit_offset;
	const dsa_pointer final_sort_keys_dp = BuildFilterArray(state->runtime_dsa,
		node.final_sort_keys.data(),
		sizeof(SortKeyDesc),
		node.final_sort_keys.size());
	if (!node.final_sort_keys.empty() && !DsaPointerIsValid(final_sort_keys_dp))
		return nullptr;

	auto output_op = std::make_unique<OutputSink>(
		queryDesc->dest,
		queryDesc->tupDesc,
		static_cast<int>(queryDesc->operation),
		output_schema_dp,
		output_layout_dp,
		final_sort_keys_dp,
		static_cast<uint16_t>(node.final_sort_keys.size()),
		output_payload_dp,
		row_capacity,
		node.limit_count,
		nullptr);
	output_op->SetSkipEmitRows(node.limit_offset);
	output_op->AddChild(std::move(node.op));
	return output_op;
}

}  // namespace pg_yaap::optimizer_translator_detail
