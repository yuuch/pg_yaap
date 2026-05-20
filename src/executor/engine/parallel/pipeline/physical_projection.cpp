#include "parallel/pipeline/physical_projection.hpp"

extern "C" {
#include "postgres.h"
#include "datatype/timestamp.h"
#include "utils/datetime.h"
#include "utils/elog.h"
}

#include <algorithm>
#include <cstring>

namespace pg_yaap {
namespace pipeline {

namespace {

static inline bool
ColumnHasNulls(const uint8_t *nulls, uint16_t count)
{
	return count != 0 && std::memchr(nulls, 1, count) != nullptr;
}

static inline uint32_t
TrimBpcharLength(const char *data, uint32_t len)
{
	while (len > 0 && data[len - 1] == ' ')
		--len;
	return len;
}

static void
ExecuteStep(const ProjectStep &step, PipelineChunk &out)
{
	const uint16_t count = out.count;
	uint8_t *const out_nulls = out.nulls[step.out_chunk_slot];

	switch (step.op)
	{
		case ProjectOp::NUMERIC_SCALE_VAR_CONST:
		{
			const uint8_t *const in_nulls = out.nulls[step.in_a_chunk_slot];
			const int64_t *const input = out.int64_columns[step.in_a_chunk_slot];
			int64_t *const output = out.int64_columns[step.out_chunk_slot];
			if (!ColumnHasNulls(in_nulls, count))
			{
				std::memset(out_nulls, 0, count);
				if (step.const_value >= 0)
				{
					for (uint16_t row = 0; row < count; ++row)
						output[row] = input[row] * step.const_value;
				}
				else
				{
					for (uint16_t row = 0; row < count; ++row)
						output[row] = input[row] / (-step.const_value);
				}
				return;
			}
			for (uint16_t row = 0; row < count; ++row)
			{
				if (in_nulls[row] != 0)
				{
					out_nulls[row] = 1;
					continue;
				}
				output[row] = (step.const_value >= 0) ?
					input[row] * step.const_value :
					input[row] / (-step.const_value);
				out_nulls[row] = 0;
			}
			return;
		}

		case ProjectOp::NUMERIC_MUL_VAR_VAR:
		case ProjectOp::NUMERIC_ADD_VAR_VAR:
		case ProjectOp::NUMERIC_SUB_VAR_VAR:
		{
			const uint8_t *const left_nulls = out.nulls[step.in_a_chunk_slot];
			const uint8_t *const right_nulls = out.nulls[step.in_b_chunk_slot];
			const int64_t *const left = out.int64_columns[step.in_a_chunk_slot];
			const int64_t *const right = out.int64_columns[step.in_b_chunk_slot];
			int64_t *const output = out.int64_columns[step.out_chunk_slot];
			if (!ColumnHasNulls(left_nulls, count) && !ColumnHasNulls(right_nulls, count))
			{
				std::memset(out_nulls, 0, count);
				switch (step.op)
				{
					case ProjectOp::NUMERIC_MUL_VAR_VAR:
						for (uint16_t row = 0; row < count; ++row)
							output[row] = left[row] * right[row];
						break;
					case ProjectOp::NUMERIC_ADD_VAR_VAR:
						for (uint16_t row = 0; row < count; ++row)
							output[row] = left[row] + right[row];
						break;
					case ProjectOp::NUMERIC_SUB_VAR_VAR:
						for (uint16_t row = 0; row < count; ++row)
							output[row] = left[row] - right[row];
						break;
					default:
						break;
				}
				return;
			}
			for (uint16_t row = 0; row < count; ++row)
			{
				if (left_nulls[row] != 0 || right_nulls[row] != 0)
				{
					out_nulls[row] = 1;
					continue;
				}
				switch (step.op)
				{
					case ProjectOp::NUMERIC_MUL_VAR_VAR:
						output[row] = left[row] * right[row];
						break;
					case ProjectOp::NUMERIC_ADD_VAR_VAR:
						output[row] = left[row] + right[row];
						break;
					case ProjectOp::NUMERIC_SUB_VAR_VAR:
						output[row] = left[row] - right[row];
						break;
					default:
						break;
				}
				out_nulls[row] = 0;
			}
			return;
		}

		case ProjectOp::NUMERIC_MUL_VAR_CONST:
		case ProjectOp::NUMERIC_ADD_VAR_CONST:
		case ProjectOp::NUMERIC_SUB_VAR_CONST:
		case ProjectOp::COPY_VAR:
		{
			const uint8_t *const in_nulls = out.nulls[step.in_a_chunk_slot];
			const int64_t *const input = out.int64_columns[step.in_a_chunk_slot];
			int64_t *const output = out.int64_columns[step.out_chunk_slot];
			if (!ColumnHasNulls(in_nulls, count))
			{
				std::memset(out_nulls, 0, count);
				switch (step.op)
				{
					case ProjectOp::NUMERIC_MUL_VAR_CONST:
						for (uint16_t row = 0; row < count; ++row)
							output[row] = input[row] * step.const_value;
						break;
					case ProjectOp::NUMERIC_ADD_VAR_CONST:
						for (uint16_t row = 0; row < count; ++row)
							output[row] = input[row] + step.const_value;
						break;
					case ProjectOp::NUMERIC_SUB_VAR_CONST:
						for (uint16_t row = 0; row < count; ++row)
							output[row] = input[row] - step.const_value;
						break;
					case ProjectOp::COPY_VAR:
						for (uint16_t row = 0; row < count; ++row)
							output[row] = input[row];
						break;
					default:
						break;
				}
				return;
			}
			for (uint16_t row = 0; row < count; ++row)
			{
				if (in_nulls[row] != 0)
				{
					out_nulls[row] = 1;
					continue;
				}
				switch (step.op)
				{
					case ProjectOp::NUMERIC_MUL_VAR_CONST:
						output[row] = input[row] * step.const_value;
						break;
					case ProjectOp::NUMERIC_ADD_VAR_CONST:
						output[row] = input[row] + step.const_value;
						break;
					case ProjectOp::NUMERIC_SUB_VAR_CONST:
						output[row] = input[row] - step.const_value;
						break;
					case ProjectOp::COPY_VAR:
						output[row] = input[row];
						break;
					default:
						break;
				}
				out_nulls[row] = 0;
			}
			return;
		}

		case ProjectOp::NUMERIC_SUB_CONST_VAR:
		case ProjectOp::NUMERIC_ADD_CONST_VAR:
		{
			const uint8_t *const in_nulls = out.nulls[step.in_b_chunk_slot];
			const int64_t *const input = out.int64_columns[step.in_b_chunk_slot];
			int64_t *const output = out.int64_columns[step.out_chunk_slot];
			if (!ColumnHasNulls(in_nulls, count))
			{
				std::memset(out_nulls, 0, count);
				if (step.op == ProjectOp::NUMERIC_SUB_CONST_VAR)
				{
					for (uint16_t row = 0; row < count; ++row)
						output[row] = step.const_value - input[row];
				}
				else
				{
					for (uint16_t row = 0; row < count; ++row)
						output[row] = step.const_value + input[row];
				}
				return;
			}
			for (uint16_t row = 0; row < count; ++row)
			{
				if (in_nulls[row] != 0)
				{
					out_nulls[row] = 1;
					continue;
				}
				output[row] = (step.op == ProjectOp::NUMERIC_SUB_CONST_VAR) ?
					step.const_value - input[row] :
					step.const_value + input[row];
				out_nulls[row] = 0;
			}
			return;
		}

		case ProjectOp::NUMERIC_DIV_VAR_VAR:
		{
			const uint8_t *const left_nulls = out.nulls[step.in_a_chunk_slot];
			const uint8_t *const right_nulls = out.nulls[step.in_b_chunk_slot];
			const int64_t *const left = out.int64_columns[step.in_a_chunk_slot];
			const int64_t *const right = out.int64_columns[step.in_b_chunk_slot];
			int64_t *const output = out.int64_columns[step.out_chunk_slot];
			if (!ColumnHasNulls(left_nulls, count) && !ColumnHasNulls(right_nulls, count))
				std::memset(out_nulls, 0, count);
			for (uint16_t row = 0; row < count; ++row)
			{
				if ((left_nulls[row] != 0) || (right_nulls[row] != 0))
				{
					out_nulls[row] = 1;
					continue;
				}
				const int64_t denom = right[row];
				if (denom == 0)
				{
					out_nulls[row] = 1;
					continue;
				}
				const NumericWideInt numerator =
					WideIntFromInt64(left[row]) * WideIntFromInt64(step.const_value);
				output[row] =
					WideIntToInt64Checked(numerator / WideIntFromInt64(denom),
						"projection numeric division");
				out_nulls[row] = 0;
			}
			return;
		}

		case ProjectOp::STRING_PREFIX_LIKE:
		{
			const uint8_t *const in_nulls = out.nulls[step.in_a_chunk_slot];
			const VecStringRef *const input = out.string_columns[step.in_a_chunk_slot];
			int64_t *const output = out.int64_columns[step.out_chunk_slot];
			const uint32_t prefix_len = step.in_b_chunk_slot;
			const char *const rhs = reinterpret_cast<const char *>(&step.const_value);
			const bool use_prefix_only = prefix_len <= sizeof(step.const_value);
			if (!ColumnHasNulls(in_nulls, count))
				std::memset(out_nulls, 0, count);
			for (uint16_t row = 0; row < count; ++row)
			{
				if (in_nulls[row] != 0)
				{
					out_nulls[row] = 1;
					continue;
				}
				const VecStringRef &ref = input[row];
				bool match = ref.len >= prefix_len;
				if (match && prefix_len != 0)
				{
					if (use_prefix_only)
						match = std::memcmp(&ref.prefix, rhs, prefix_len) == 0;
					else
					{
						const char *const lhs = out.get_string_ptr(ref);
						match = lhs != nullptr && std::memcmp(lhs, rhs, prefix_len) == 0;
					}
				}
				output[row] = match ? 1 : 0;
				out_nulls[row] = 0;
			}
			return;
		}

		case ProjectOp::NUMERIC_CASE_VAR_CONST:
		{
			const uint8_t *const cond_nulls = out.nulls[step.in_a_chunk_slot];
			const uint8_t *const value_nulls = out.nulls[step.in_b_chunk_slot];
			const int64_t *const cond = out.int64_columns[step.in_a_chunk_slot];
			const int64_t *const value = out.int64_columns[step.in_b_chunk_slot];
			int64_t *const output = out.int64_columns[step.out_chunk_slot];
			if (!ColumnHasNulls(cond_nulls, count) && !ColumnHasNulls(value_nulls, count))
			{
				std::memset(out_nulls, 0, count);
				for (uint16_t row = 0; row < count; ++row)
					output[row] = (cond[row] != 0) ? value[row] : step.const_value;
				return;
			}
			for (uint16_t row = 0; row < count; ++row)
			{
				if (cond_nulls[row] == 0 && cond[row] != 0)
				{
					if (value_nulls[row] != 0)
					{
						out_nulls[row] = 1;
						continue;
					}
					output[row] = value[row];
				}
				else
					output[row] = step.const_value;
				out_nulls[row] = 0;
			}
			return;
		}

		case ProjectOp::EXTRACT_YEAR_FROM_DATE:
		{
			const uint8_t *const in_nulls = out.nulls[step.in_a_chunk_slot];
			const int32_t *const input = out.int32_columns[step.in_a_chunk_slot];
			int64_t *const output = out.int64_columns[step.out_chunk_slot];
			for (uint16_t row = 0; row < count; ++row)
			{
				if (in_nulls[row] != 0)
				{
					out_nulls[row] = 1;
					continue;
				}
				int year = 0, month = 0, day = 0;
				j2date(static_cast<int>(input[row]) + POSTGRES_EPOCH_JDATE, &year, &month, &day);
				output[row] = year;
				out_nulls[row] = 0;
			}
			return;
		}

		case ProjectOp::STRING_EQ_VAR_CONST:
		case ProjectOp::STRING_NE_VAR_CONST:
		{
			const uint8_t *const in_nulls = out.nulls[step.in_a_chunk_slot];
			const VecStringRef *const input = out.string_columns[step.in_a_chunk_slot];
			int64_t *const output = out.int64_columns[step.out_chunk_slot];
			const bool bpchar_semantics = (step.in_b_chunk_slot & 0x80) != 0;
			const uint32_t rhs_len = static_cast<uint32_t>(step.in_b_chunk_slot & 0x7f);
			const char *const rhs = reinterpret_cast<const char *>(&step.const_value);
			for (uint16_t row = 0; row < count; ++row)
			{
				if (in_nulls[row] != 0)
				{
					out_nulls[row] = 1;
					continue;
				}
				const VecStringRef &ref = input[row];
				const char *lhs = out.get_string_ptr(ref);
				bool eq = false;
				if (lhs != nullptr)
				{
					if (bpchar_semantics)
					{
						const uint32_t lhs_len = TrimBpcharLength(lhs, ref.len);
						const uint32_t trimmed_rhs_len = TrimBpcharLength(rhs, rhs_len);
						eq = lhs_len == trimmed_rhs_len &&
							(lhs_len == 0 || std::memcmp(lhs, rhs, lhs_len) == 0);
					}
					else
						eq = ref.len == rhs_len && (rhs_len == 0 || std::memcmp(lhs, rhs, rhs_len) == 0);
				}
				output[row] = (step.op == ProjectOp::STRING_EQ_VAR_CONST ? eq : !eq) ? 1 : 0;
				out_nulls[row] = 0;
			}
			return;
		}

		case ProjectOp::STRING_PREFIX_SLICE:
		{
			const uint8_t *const in_nulls = out.nulls[step.in_a_chunk_slot];
			const VecStringRef *const input = out.string_columns[step.in_a_chunk_slot];
			VecStringRef *const output = out.string_columns[step.out_chunk_slot];
			const uint32_t prefix_len = step.in_b_chunk_slot;
			for (uint16_t row = 0; row < count; ++row)
			{
				if (in_nulls[row] != 0)
				{
					out_nulls[row] = 1;
					continue;
				}
				const VecStringRef &ref = input[row];
				const char *ptr = out.get_string_ptr(ref);
				if (ptr == nullptr && ref.len != 0)
				{
					out_nulls[row] = 1;
					continue;
				}
				output[row] = out.store_string_bytes(ptr, std::min<uint32_t>(ref.len, prefix_len));
				out_nulls[row] = 0;
			}
			return;
		}

		case ProjectOp::NUMERIC_CASE_ELSE_VAR:
		{
			const uint8_t *const cond_nulls = out.nulls[step.in_a_chunk_slot];
			const uint8_t *const then_nulls = out.nulls[step.in_b_chunk_slot];
			const uint8_t *const else_nulls = out.nulls[static_cast<uint8_t>(step.const_value)];
			const int64_t *const cond = out.int64_columns[step.in_a_chunk_slot];
			const int64_t *const then_vals = out.int64_columns[step.in_b_chunk_slot];
			const int64_t *const else_vals = out.int64_columns[static_cast<uint8_t>(step.const_value)];
			int64_t *const output = out.int64_columns[step.out_chunk_slot];
			for (uint16_t row = 0; row < count; ++row)
			{
				const bool take_then = cond_nulls[row] == 0 && cond[row] != 0;
				const uint8_t *src_nulls = take_then ? then_nulls : else_nulls;
				const int64_t *src_vals = take_then ? then_vals : else_vals;
				if (src_nulls[row] != 0)
				{
					out_nulls[row] = 1;
					continue;
				}
				output[row] = src_vals[row];
				out_nulls[row] = 0;
			}
			return;
		}

		case ProjectOp::BOOL_AND_VAR_VAR:
		case ProjectOp::BOOL_OR_VAR_VAR:
		{
			const uint8_t *const left_nulls = out.nulls[step.in_a_chunk_slot];
			const uint8_t *const right_nulls = out.nulls[step.in_b_chunk_slot];
			const int64_t *const left = out.int64_columns[step.in_a_chunk_slot];
			const int64_t *const right = out.int64_columns[step.in_b_chunk_slot];
			int64_t *const output = out.int64_columns[step.out_chunk_slot];
			for (uint16_t row = 0; row < count; ++row)
			{
				if (left_nulls[row] != 0 || right_nulls[row] != 0)
				{
					out_nulls[row] = 1;
					continue;
				}
				output[row] = (step.op == ProjectOp::BOOL_AND_VAR_VAR) ? ((left[row] != 0 && right[row] != 0) ? 1 : 0)
					: ((left[row] != 0 || right[row] != 0) ? 1 : 0);
				out_nulls[row] = 0;
			}
			return;
		}

		case ProjectOp::BOOL_NOT_VAR:
		{
			const uint8_t *const in_nulls = out.nulls[step.in_a_chunk_slot];
			const int64_t *const input = out.int64_columns[step.in_a_chunk_slot];
			int64_t *const output = out.int64_columns[step.out_chunk_slot];
			for (uint16_t row = 0; row < count; ++row)
			{
				if (in_nulls[row] != 0)
				{
					out_nulls[row] = 1;
					continue;
				}
				output[row] = input[row] == 0 ? 1 : 0;
				out_nulls[row] = 0;
			}
			return;
		}

		case ProjectOp::CONST_INT64:
		{
			int64_t *const output = out.int64_columns[step.out_chunk_slot];
			std::memset(out_nulls, 0, count);
			for (uint16_t row = 0; row < count; ++row)
				output[row] = step.const_value;
			return;
		}

		case ProjectOp::INT32_TO_INT64_VAR:
		{
			const uint8_t *const in_nulls = out.nulls[step.in_a_chunk_slot];
			const int32_t *const input = out.int32_columns[step.in_a_chunk_slot];
			int64_t *const output = out.int64_columns[step.out_chunk_slot];
			for (uint16_t row = 0; row < count; ++row)
			{
				if (in_nulls[row] != 0)
				{
					out_nulls[row] = 1;
					continue;
				}
				output[row] = input[row];
				out_nulls[row] = 0;
			}
			return;
		}

		case ProjectOp::INT64_LT_VAR_CONST:
		case ProjectOp::INT64_LE_VAR_CONST:
		case ProjectOp::INT64_EQ_VAR_CONST:
		case ProjectOp::INT64_GE_VAR_CONST:
		case ProjectOp::INT64_GT_VAR_CONST:
		case ProjectOp::INT64_NE_VAR_CONST:
		{
			const uint8_t *const in_nulls = out.nulls[step.in_a_chunk_slot];
			const int64_t *const input = out.int64_columns[step.in_a_chunk_slot];
			int64_t *const output = out.int64_columns[step.out_chunk_slot];
			for (uint16_t row = 0; row < count; ++row)
			{
				if (in_nulls[row] != 0)
				{
					out_nulls[row] = 1;
					continue;
				}
				const int64_t value = input[row];
				switch (step.op)
				{
					case ProjectOp::INT64_LT_VAR_CONST: output[row] = value < step.const_value; break;
					case ProjectOp::INT64_LE_VAR_CONST: output[row] = value <= step.const_value; break;
					case ProjectOp::INT64_EQ_VAR_CONST: output[row] = value == step.const_value; break;
					case ProjectOp::INT64_GE_VAR_CONST: output[row] = value >= step.const_value; break;
					case ProjectOp::INT64_GT_VAR_CONST: output[row] = value > step.const_value; break;
					case ProjectOp::INT64_NE_VAR_CONST: output[row] = value != step.const_value; break;
					default: break;
				}
				out_nulls[row] = 0;
			}
			return;
		}
	}

	elog(ERROR, "pg_yaap: unsupported projection opcode %u", static_cast<unsigned>(step.op));
}

} // namespace

std::unique_ptr<OperatorState>
PhysicalProjection::GetOperatorState(ExecCtx &ctx)
{
	(void) ctx;
	return std::make_unique<ProjectionOperatorState>();
}

OperatorResultType
PhysicalProjection::Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state)
{
	(void) ctx;
	auto &op_state = static_cast<ProjectionOperatorState &>(state);
	if (op_state.current_input_drained)
	{
		op_state.current_input_drained = false;
		out.reset();
		return OperatorResultType::NEED_MORE_INPUT;
	}
	out = in;
	if (out.has_any_dictionary())
		out.flatten();

	for (const ProjectExprDesc &expr : expr_descs_)
	{
		if (static_cast<size_t>(expr.first_step_idx) + static_cast<size_t>(expr.n_steps) > steps_.size())
			elog(ERROR, "pg_yaap: projection step range exceeds step tape");
		if (expr.n_steps == 0)
			continue;

		for (uint16_t step_idx = expr.first_step_idx;
		     step_idx < static_cast<uint16_t>(expr.first_step_idx + expr.n_steps);
		     ++step_idx)
		{
			const ProjectStep &step = steps_[step_idx];
			ExecuteStep(step, out);
		}

		if (expr.output_chunk_slot != steps_[expr.first_step_idx + expr.n_steps - 1].out_chunk_slot)
			elog(ERROR, "pg_yaap: projection output slot descriptor mismatch");
	}
	op_state.current_input_drained = out.count > 0;
	return out.count > 0 ? OperatorResultType::HAVE_MORE_OUTPUT
	                     : OperatorResultType::NEED_MORE_INPUT;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
