#include "parallel/pipeline/physical_seq_scan.hpp"
#include "parallel/pipeline/pipeline_profile.hpp"

extern "C" {
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "catalog/pg_type_d.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "storage/read_stream.h"
#include "utils/date.h"
#include "utils/elog.h"
#include "utils/numeric.h"
#include "utils/snapmgr.h"
#include "varatt.h"

extern int  pg_yaap_parallel_max_workers;
extern bool pg_yaap_jit_deform;
extern bool pg_yaap_disable_jit_for_parallel_worker;
extern bool pg_yaap_trace_hooks;
extern bool pg_yaap_trace_execution_path;

extern Datum int8_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_mul(PG_FUNCTION_ARGS);
extern Datum numeric_int8(PG_FUNCTION_ARGS);
}

#include <algorithm>
#include <cmath>
#include <string_view>

#include "parallel/pipeline/cancel.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/physical_seq_scan_page_prepare.hpp"
#include "parallel/pipeline/physical_seq_scan_read_stream.hpp"
#ifdef USE_LLVM
#include "llvmjit_deform_datachunk.h"
#endif

namespace pg_yaap {
namespace pipeline {

namespace {

static SeqScanSharedPayload *
ResolveSeqScanPayload(ExecCtx &ctx, dsa_pointer dp)
{
	if (!DsaPointerIsValid(dp))
		return nullptr;
	return static_cast<SeqScanSharedPayload *>(dsa_get_address(ctx.dsa, dp));
}

static inline bool
SeqScanTraceEnabled(const ExecCtx &ctx)
{
	return ctx.control != nullptr &&
		pg_atomic_read_u32(&ctx.control->trace_execution_path) != 0;
}

static void
LogSeqScanDeformerJitDecision(const ExecCtx &ctx,
							 const char *kind,
							 const DeformProgram &program,
							 bool compiled,
							 const char *reason)
{
	if (!pg_yaap_trace_hooks && !SeqScanTraceEnabled(ctx))
		return;
	elog(LOG,
		 "pg_yaap: SeqScan %s deformer worker=%d targets=%d last_att=%d jit=%s%s%s",
		 kind,
		 ctx.worker_index,
		 program.ntargets,
		 program.last_att_index,
		 compiled ? "on" : "off",
		 (reason != nullptr && reason[0] != '\0') ? " reason=" : "",
		 (reason != nullptr && reason[0] != '\0') ? reason : "");
}

static bool
MatchPercentLikePattern(const char *lhs, uint32_t lhs_len,
						  const char *pattern, uint32_t pattern_len)
{
	if (pattern == nullptr)
		return false;
	std::string_view text(lhs != nullptr ? lhs : "", lhs_len);
	std::string_view spec(pattern, pattern_len);
	if (spec.empty())
		return text.empty();
	if (spec == "%")
		return true;

	size_t text_pos = 0;
	size_t pat_pos = 0;
	const bool anchored_start = spec.front() != '%';
	const bool anchored_end = spec.back() != '%';
	bool first_token = true;

	while (pat_pos < spec.size())
	{
		while (pat_pos < spec.size() && spec[pat_pos] == '%')
			++pat_pos;
		if (pat_pos >= spec.size())
			return true;

		const size_t next_pct = spec.find('%', pat_pos);
		const std::string_view token = spec.substr(
			pat_pos,
			next_pct == std::string_view::npos ? spec.size() - pat_pos : next_pct - pat_pos);
		if (token.empty())
		{
			pat_pos = next_pct;
			continue;
		}

		if (first_token && anchored_start)
		{
			if (text.size() < token.size() || text.substr(0, token.size()) != token)
				return false;
			text_pos = token.size();
		}
		else
		{
			const size_t found = text.find(token, text_pos);
			if (found == std::string_view::npos)
				return false;
			text_pos = found + token.size();
		}
		first_token = false;

		if (next_pct == std::string_view::npos)
			return !anchored_end || text_pos == text.size();
		pat_pos = next_pct;
	}

	return !anchored_end || text_pos == text.size();
}

static SchemaDescriptor *
ResolveSchemaDescriptor(dsa_area *dsa, dsa_pointer dp)
{
	if (!DsaPointerIsValid(dp))
		return nullptr;
	return static_cast<SchemaDescriptor *>(dsa_get_address(dsa, dp));
}

static FilterInputDesc *
ResolveFilterInputs(dsa_area *dsa, dsa_pointer dp)
{
	if (!DsaPointerIsValid(dp))
		return nullptr;
	return static_cast<FilterInputDesc *>(dsa_get_address(dsa, dp));
}

static FilterExprDesc *
ResolveFilterExprs(dsa_area *dsa, dsa_pointer dp)
{
	if (!DsaPointerIsValid(dp))
		return nullptr;
	return static_cast<FilterExprDesc *>(dsa_get_address(dsa, dp));
}

static FilterStep *
ResolveFilterSteps(dsa_area *dsa, dsa_pointer dp)
{
	if (!DsaPointerIsValid(dp))
		return nullptr;
	return static_cast<FilterStep *>(dsa_get_address(dsa, dp));
}

static uint32
ComputeMaxThreadsFromPayload(const SeqScanSharedPayload *shared)
{
	if (shared == nullptr || shared->total_blocks == 0)
		return 1;

	uint32 want = (uint32) std::min<uint64>(shared->total_blocks, (uint64) UINT32_MAX);
	return (uint32) std::max(1, std::min(pg_yaap_parallel_max_workers, (int) want));
}

/*
 * Inline single-row predicate evaluator. Reads from qual_chunk at row 0
 * (the qual deformer always writes there) using the dst_col resolved at
 * build time. NULL → false; QualKind::NONE → true short-circuits before
 * any deform happens (caller skips the qual deform entirely in that
 * case). Type dispatch is the same set as EvalTypedCompare's by-value
 * Datum path (DATEOID/INT4/INT8 × 6 ops); we read pre-decoded typed
 * values from the chunk so no Datum unpacking is needed here.
 */
static inline const char *
ResolveFilterConstPtr(const FilterStep &step, const char *string_consts)
{
	if (step.const_len == 0)
		return "";
	if (step.const_offset == UINT32_MAX)
		return reinterpret_cast<const char *>(&step.const_value);
	if (string_consts == nullptr)
		return nullptr;
	return string_consts + step.const_offset;
}

static inline uint16_t
RequiredFilterBoolRegs(const FilterExprDesc *exprs, uint16_t n_exprs)
{
	uint16_t max_reg = 0;
	for (uint16_t expr_idx = 0; expr_idx < n_exprs; ++expr_idx)
		max_reg = std::max<uint16_t>(max_reg, static_cast<uint16_t>(exprs[expr_idx].output_bool_reg + 1));
	return max_reg;
}

static inline bool
EvalFilterStep(const FilterStep &step,
	       const DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE> &filter_chunk,
	       const char *string_consts,
	       uint8_t *bool_values)


{
	bool result = false;
		switch (step.op)
		{
			case FilterStepOp::INT32_CMP_CONST:
			{
				if (!filter_chunk.nulls[step.left_idx][0])
				{
					const int32_t l = filter_chunk.get_int32(step.left_idx, 0);
					const int32_t r = static_cast<int32_t>(step.const_value);
				switch (step.cmp_op)
				{
					case QualOp::LE: result = l <= r; break;
					case QualOp::LT: result = l <  r; break;
					case QualOp::EQ: result = l == r; break;
					case QualOp::GE: result = l >= r; break;
					case QualOp::GT: result = l >  r; break;
					case QualOp::NE: result = l != r; break;
				}
			}
			break;
		}
			case FilterStepOp::INT64_CMP_CONST:
			{
				if (!filter_chunk.nulls[step.left_idx][0])
				{
					const int64_t l = filter_chunk.get_int64(step.left_idx, 0);
					const int64_t r = static_cast<int64_t>(step.const_value);
				switch (step.cmp_op)
				{
					case QualOp::LE: result = l <= r; break;
					case QualOp::LT: result = l <  r; break;
					case QualOp::EQ: result = l == r; break;
					case QualOp::GE: result = l >= r; break;
					case QualOp::GT: result = l >  r; break;
					case QualOp::NE: result = l != r; break;
				}
			}
			break;
		}
		case FilterStepOp::INT32_CMP_VAR:
		{
			if (!filter_chunk.nulls[step.left_idx][0] && !filter_chunk.nulls[step.right_idx][0])
			{
				const int32_t l = filter_chunk.get_int32(step.left_idx, 0);
				const int32_t r = filter_chunk.get_int32(step.right_idx, 0);
				switch (step.cmp_op)
				{
					case QualOp::LE: result = l <= r; break;
					case QualOp::LT: result = l <  r; break;
					case QualOp::EQ: result = l == r; break;
					case QualOp::GE: result = l >= r; break;
					case QualOp::GT: result = l >  r; break;
					case QualOp::NE: result = l != r; break;
				}
			}
			break;
		}
		case FilterStepOp::INT64_CMP_VAR:
		{
			if (!filter_chunk.nulls[step.left_idx][0] && !filter_chunk.nulls[step.right_idx][0])
			{
				const int64_t l = filter_chunk.get_int64(step.left_idx, 0);
				const int64_t r = filter_chunk.get_int64(step.right_idx, 0);
				switch (step.cmp_op)
				{
					case QualOp::LE: result = l <= r; break;
					case QualOp::LT: result = l <  r; break;
					case QualOp::EQ: result = l == r; break;
					case QualOp::GE: result = l >= r; break;
					case QualOp::GT: result = l >  r; break;
					case QualOp::NE: result = l != r; break;
				}
			}
			break;
		}
		case FilterStepOp::STRING_EQ_CONST:
			case FilterStepOp::STRING_NE_CONST:
			{
				if (!filter_chunk.nulls[step.left_idx][0])
				{
					const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, 0);
					const char *lhs = filter_chunk.get_string_ptr(step.left_idx, 0);
					const char *rhs = ResolveFilterConstPtr(step, string_consts);
				const bool eq = (lhs != nullptr || ref.len == 0) && rhs != nullptr &&
					ref.len == step.const_len &&
					(step.const_len == 0 || std::memcmp(lhs, rhs, step.const_len) == 0);
				result = (step.op == FilterStepOp::STRING_EQ_CONST) ? eq : !eq;
			}
			break;
		}
			case FilterStepOp::STRING_PREFIX_LIKE:
			{
				if (!filter_chunk.nulls[step.left_idx][0])
				{
					const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, 0);
					const char *rhs = ResolveFilterConstPtr(step, string_consts);
				result = rhs != nullptr && ref.len >= step.const_len;
					if (result && step.const_len != 0)
					{
						if (step.const_len <= sizeof(ref.prefix))
							result = std::memcmp(&ref.prefix, rhs, step.const_len) == 0;
						else
						{
							const char *lhs = filter_chunk.get_string_ptr(step.left_idx, 0);
							result = lhs != nullptr && std::memcmp(lhs, rhs, step.const_len) == 0;
						}
				}
			}
			break;
		}
			case FilterStepOp::STRING_CONTAINS_LIKE:
			{
				if (!filter_chunk.nulls[step.left_idx][0])
				{
					const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, 0);
					const char *rhs = ResolveFilterConstPtr(step, string_consts);
					const char *lhs = filter_chunk.get_string_ptr(step.left_idx, 0);
					result = rhs != nullptr && lhs != nullptr && step.const_len <= ref.len;
					if (result)
					{
						for (uint32_t start = 0; start + step.const_len <= ref.len; ++start)
						{
							if (std::memcmp(lhs + start, rhs, step.const_len) == 0)
							{
								result = true;
								break;
							}
							result = false;
						}
					}
				}
				break;
			}
			case FilterStepOp::STRING_SQL_LIKE:
			{
				if (!filter_chunk.nulls[step.left_idx][0])
				{
					const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, 0);
					const char *rhs = ResolveFilterConstPtr(step, string_consts);
					const char *lhs = filter_chunk.get_string_ptr(step.left_idx, 0);
					result = MatchPercentLikePattern(lhs, ref.len, rhs, step.const_len);
				}
				break;
			}
		case FilterStepOp::BOOL_AND:
			result = bool_values[step.left_idx] && bool_values[step.right_idx];
			break;
		case FilterStepOp::BOOL_OR:
			result = bool_values[step.left_idx] || bool_values[step.right_idx];
			break;
		case FilterStepOp::BOOL_NOT:
			result = !bool_values[step.left_idx];
			break;
		default:
			elog(ERROR, "pg_yaap: unsupported filter step op %u", static_cast<unsigned>(step.op));
	}
	bool_values[step.out_bool_reg] = result ? 1 : 0;
	return result;
}

static inline bool
IsSimpleFilterStep(const FilterExprDesc *exprs,
		   uint16_t n_exprs,
		   const FilterStep *steps,
		   uint16_t n_steps,
		   const FilterStep **simple_step)
{
	if (simple_step != nullptr)
		*simple_step = nullptr;
	if (exprs == nullptr || steps == nullptr || n_exprs != 1)
		return false;
	const FilterExprDesc &expr = exprs[0];
	if (expr.n_steps != 1 || expr.first_step_idx >= n_steps)
		return false;
	const FilterStep &step = steps[expr.first_step_idx];
	if (expr.output_bool_reg != step.out_bool_reg)
		return false;
	switch (step.op)
	{
		case FilterStepOp::INT32_CMP_CONST:
		case FilterStepOp::INT64_CMP_CONST:
		case FilterStepOp::INT32_CMP_VAR:
		case FilterStepOp::INT64_CMP_VAR:
		case FilterStepOp::STRING_EQ_CONST:
		case FilterStepOp::STRING_NE_CONST:
		case FilterStepOp::STRING_PREFIX_LIKE:
		case FilterStepOp::STRING_CONTAINS_LIKE:
		case FilterStepOp::STRING_SQL_LIKE:
			if (simple_step != nullptr)
				*simple_step = &step;
			return true;
		case FilterStepOp::BOOL_AND:
		case FilterStepOp::BOOL_OR:
		case FilterStepOp::BOOL_NOT:
			return false;
	}
	return false;
}

static inline bool
EvalSimpleFilterStep(const FilterStep &step,
		     const DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE> &filter_chunk,
		     const char *string_consts)
{
	switch (step.op)
	{
		case FilterStepOp::INT32_CMP_CONST:
			if (!filter_chunk.nulls[step.left_idx][0])
			{
				const int32_t l = filter_chunk.get_int32(step.left_idx, 0);
				const int32_t r = static_cast<int32_t>(step.const_value);
				switch (step.cmp_op)
				{
					case QualOp::LE: return l <= r;
					case QualOp::LT: return l < r;
					case QualOp::EQ: return l == r;
					case QualOp::GE: return l >= r;
					case QualOp::GT: return l > r;
					case QualOp::NE: return l != r;
				}
			}
			return false;
		case FilterStepOp::INT64_CMP_CONST:
			if (!filter_chunk.nulls[step.left_idx][0])
			{
				const int64_t l = filter_chunk.get_int64(step.left_idx, 0);
				const int64_t r = static_cast<int64_t>(step.const_value);
				switch (step.cmp_op)
				{
					case QualOp::LE: return l <= r;
					case QualOp::LT: return l < r;
					case QualOp::EQ: return l == r;
					case QualOp::GE: return l >= r;
					case QualOp::GT: return l > r;
					case QualOp::NE: return l != r;
				}
			}
			return false;
		case FilterStepOp::INT32_CMP_VAR:
			if (!filter_chunk.nulls[step.left_idx][0] && !filter_chunk.nulls[step.right_idx][0])
			{
				const int32_t l = filter_chunk.get_int32(step.left_idx, 0);
				const int32_t r = filter_chunk.get_int32(step.right_idx, 0);
				switch (step.cmp_op)
				{
					case QualOp::LE: return l <= r;
					case QualOp::LT: return l < r;
					case QualOp::EQ: return l == r;
					case QualOp::GE: return l >= r;
					case QualOp::GT: return l > r;
					case QualOp::NE: return l != r;
				}
			}
			return false;
		case FilterStepOp::INT64_CMP_VAR:
			if (!filter_chunk.nulls[step.left_idx][0] && !filter_chunk.nulls[step.right_idx][0])
			{
				const int64_t l = filter_chunk.get_int64(step.left_idx, 0);
				const int64_t r = filter_chunk.get_int64(step.right_idx, 0);
				switch (step.cmp_op)
				{
					case QualOp::LE: return l <= r;
					case QualOp::LT: return l < r;
					case QualOp::EQ: return l == r;
					case QualOp::GE: return l >= r;
					case QualOp::GT: return l > r;
					case QualOp::NE: return l != r;
				}
			}
			return false;
		case FilterStepOp::STRING_EQ_CONST:
		case FilterStepOp::STRING_NE_CONST:
			if (!filter_chunk.nulls[step.left_idx][0])
			{
				const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, 0);
				const char *lhs = filter_chunk.get_string_ptr(step.left_idx, 0);
				const char *rhs = ResolveFilterConstPtr(step, string_consts);
				const bool eq = (lhs != nullptr || ref.len == 0) && rhs != nullptr &&
					ref.len == step.const_len &&
					(step.const_len == 0 || std::memcmp(lhs, rhs, step.const_len) == 0);
				return step.op == FilterStepOp::STRING_EQ_CONST ? eq : !eq;
			}
			return false;
		case FilterStepOp::STRING_PREFIX_LIKE:
			if (!filter_chunk.nulls[step.left_idx][0])
			{
				const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, 0);
				const char *rhs = ResolveFilterConstPtr(step, string_consts);
				if (rhs == nullptr || ref.len < step.const_len)
					return false;
				if (step.const_len == 0)
					return true;
				if (step.const_len <= sizeof(ref.prefix))
					return std::memcmp(&ref.prefix, rhs, step.const_len) == 0;
				const char *lhs = filter_chunk.get_string_ptr(step.left_idx, 0);
				return lhs != nullptr && std::memcmp(lhs, rhs, step.const_len) == 0;
			}
			return false;
		case FilterStepOp::STRING_CONTAINS_LIKE:
			if (!filter_chunk.nulls[step.left_idx][0])
			{
				const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, 0);
				const char *rhs = ResolveFilterConstPtr(step, string_consts);
				const char *lhs = filter_chunk.get_string_ptr(step.left_idx, 0);
				if (rhs == nullptr || lhs == nullptr || step.const_len > ref.len)
					return false;
				for (uint32_t start = 0; start + step.const_len <= ref.len; ++start)
				{
					if (std::memcmp(lhs + start, rhs, step.const_len) == 0)
						return true;
				}
			}
			return false;
		case FilterStepOp::STRING_SQL_LIKE:
			if (!filter_chunk.nulls[step.left_idx][0])
			{
				const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, 0);
				const char *rhs = ResolveFilterConstPtr(step, string_consts);
				const char *lhs = filter_chunk.get_string_ptr(step.left_idx, 0);
				return MatchPercentLikePattern(lhs, ref.len, rhs, step.const_len);
			}
			return false;
		case FilterStepOp::BOOL_AND:
		case FilterStepOp::BOOL_OR:
		case FilterStepOp::BOOL_NOT:
			break;
	}
	return false;
}

/*
 * Map planner-published per-column decode kind onto the deformer's
 * physical decode kind. The two enums are intentionally distinct:
 * ColumnDecodeKind is the projection-layer contract (lives in the
 * descriptor IR, must stay stable across processes); DeformDecodeKind
 * is the physical decoder's per-target tag and may add new kinds
 * (e.g. kBpchar1) without touching the descriptor format. Returns
 * false for ColumnDecodeKind::NONE so caller can hard-error.
 */
static inline bool
MapColumnToDeformKind(const ColumnSchema &col, DeformDecodeKind &out_kind)
{
	switch (col.decode_kind)
	{
		case ColumnDecodeKind::INT32_CHAR:
			out_kind = (col.type_oid == BPCHAROID)
				? DeformDecodeKind::kBpchar1
				: DeformDecodeKind::kInt32;
			return true;
		case ColumnDecodeKind::INT32_DATE:
			out_kind = DeformDecodeKind::kDate32;
			return true;
		case ColumnDecodeKind::INT32_INT4:
			out_kind = DeformDecodeKind::kInt32;
			return true;
		case ColumnDecodeKind::INT64_INT8:
			out_kind = DeformDecodeKind::kInt64;
			return true;
		case ColumnDecodeKind::INT64_NUMERIC_SCALED:
			out_kind = DeformDecodeKind::kNumeric;
			return true;
		case ColumnDecodeKind::DOUBLE_FLOAT8:
			out_kind = DeformDecodeKind::kFloat8;
			return true;
		case ColumnDecodeKind::STRING_REF:
			out_kind = DeformDecodeKind::kStringRef;
			return true;
		case ColumnDecodeKind::NONE:
		default:
			return false;
	}
}

/*
 * Build per-schema-column DeformBindings for the given output chunk.
 * Invariant: BuildDeformProgramFromSchema stamps target.dst_col with the
 * source schema column index (NOT chunk_slot), and the deformer writes
 * bindings.columns_data[dst_col]. We therefore index bindings by schema
 * column index s in [0, n_columns), and route each schema column to its
 * per-storage chunk array via columns[s].chunk_slot + decode_kind.
 *
 * Heads point at row 0; the deformer offsets by row_idx.
 */
static inline void
BuildDeformBindings(const SchemaDescriptor *out_schema,
                    PipelineChunk &out,
                    DeformBindings &bindings)
{
	const uint16_t n = out_schema->n_columns;
	if (n > kMaxDeformTargets)
		elog(ERROR, "pg_yaap: SeqScan projection schema has too many columns (%u)",
		     static_cast<unsigned>(n));
	bindings.ncolumns = n;
	bindings.owner_chunk = &out;
	for (uint16_t s = 0; s < n; ++s)
	{
		const ColumnSchema &col = out_schema->columns[s];
		const uint8_t       slot_idx = col.chunk_slot;
		if (slot_idx >= 16)
			elog(ERROR,
			     "pg_yaap: SeqScan projection column %u has invalid chunk slot %u",
			     static_cast<unsigned>(s),
			     static_cast<unsigned>(slot_idx));
		void *data_head;
		switch (col.decode_kind)
		{
			case ColumnDecodeKind::INT32_CHAR:
			case ColumnDecodeKind::INT32_DATE:
			case ColumnDecodeKind::INT32_INT4:
				data_head = static_cast<void *>(out.int32_columns[slot_idx]);
				break;
			case ColumnDecodeKind::INT64_INT8:
			case ColumnDecodeKind::INT64_NUMERIC_SCALED:
				data_head = static_cast<void *>(out.int64_columns[slot_idx]);
				break;
			case ColumnDecodeKind::DOUBLE_FLOAT8:
				data_head = static_cast<void *>(out.double_columns[slot_idx]);
				break;
			case ColumnDecodeKind::STRING_REF:
				data_head = static_cast<void *>(out.string_columns[slot_idx]);
				break;
			default:
				elog(ERROR, "pg_yaap: unsupported ColumnDecodeKind=%u in SeqScan deform binding",
				     (unsigned) col.decode_kind);
		}
		bindings.columns_data[s]  = data_head;
		bindings.columns_nulls[s] = out.nulls[slot_idx];
	}
}

/*
 * Native + JIT deform path. Replaces the per-column heap_getattr loop
 * (which called nocachegetattr — Q1's dominant hot leaf at 16.7K samples
 * pre-B.1) with a single offset-walking deform that emits all targets
 * in one pass. The deformer dispatches to the JIT'd function if
 * proj_jit_func is set, else interprets. Caller is responsible for
 * having qual already evaluated and survived; out.count is incremented
 * here on append.
 */
static inline void
AppendProjectedTupleViaDeformer(PipelineChunk &out,
                                  HeapTuple tuple,
                                  const DeformBindings &bindings,
                                  SeqScanLocalState &local)
{
	if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
		elog(ERROR, "pg_yaap: SeqScan output chunk overflow before append");
	uint16_t row = out.count++;

	local.proj_deformer->deform_tuple_header(tuple->t_data, row, bindings);
	if (out.count > PIPELINE_DEFAULT_CHUNK_SIZE)
		elog(ERROR, "pg_yaap: SeqScan output chunk overflow after append");
}

/*
 * Build DeformProgram from SchemaDescriptor (projection side). Stamps
 * target.dst_col with the SCHEMA column index s, NOT chunk_slot —
 * matches the contract used by BuildDeformBindings above and consumed
 * by data_chunk_deform.cpp's `bindings.columns_data[t.dst_col]` writes.
 * att_index uses 0-based convention (PG's attnum-1) to match
 * TupleDescCompactAttr indexing inside DataChunkDeformer::deform_tuple_header.
 */
static bool
BuildProjDeformProgramFromSchema(const SchemaDescriptor *out_schema,
                                  DeformProgram &program)
{
	program.reset();
	const uint16_t n = out_schema->n_columns;
	if (n > kMaxDeformTargets)
		return false;
	for (uint16_t s = 0; s < n; ++s)
	{
		const ColumnSchema &col = out_schema->columns[s];
		DeformDecodeKind    kind;
		if (col.src_attno <= 0)
			return false;
		if (!MapColumnToDeformKind(col, kind))
			return false;
		program.add_target((int) col.src_attno - 1, (int) s, kind);
	}
	program.finalize();
	return true;
}

/* Build a filter-side DeformProgram from the published input vector.
 * Repeated predicates on the same column are deduplicated by the translator,
 * so each dst_col here corresponds to one decoded scratch slot. */
static bool
BuildFilterDeformProgram(const FilterInputDesc *inputs,
			        uint16_t n_inputs,
			        DeformProgram &program)
{
	program.reset();
	if (inputs == nullptr || n_inputs == 0)
		return false;
	if (n_inputs > FILTER_MAX_INPUTS)
		return false;

	for (uint16_t i = 0; i < n_inputs; ++i)
	{
		const FilterInputDesc &input = inputs[i];
		if (input.attno <= 0)
			return false;

		DeformDecodeKind kind;
		switch (input.source_decode_kind)
		{
			case ColumnDecodeKind::INT32_CHAR: kind = DeformDecodeKind::kBpchar1; break;
			case ColumnDecodeKind::INT32_DATE: kind = DeformDecodeKind::kDate32; break;
			case ColumnDecodeKind::INT32_INT4: kind = DeformDecodeKind::kInt32; break;
			case ColumnDecodeKind::INT64_INT8:
				kind = DeformDecodeKind::kInt64;
				break;
			case ColumnDecodeKind::INT64_NUMERIC_SCALED:
				kind = DeformDecodeKind::kNumeric;
				break;
			case ColumnDecodeKind::STRING_REF:
				kind = DeformDecodeKind::kStringRef;
				break;
			default:
				return false;
		}
		program.add_target((int) input.attno - 1, (int) input.dst_col, kind);
	}
	program.finalize();
	return true;
}

static inline void
ReleaseSeqScanPage(SeqScanLocalState &local)
{
	if (local.scan_desc != nullptr && BufferIsValid(local.scan_desc->rs_cbuf))
	{
		ReleaseBuffer(local.scan_desc->rs_cbuf);
		local.scan_desc->rs_cbuf = InvalidBuffer;
	}
	if (local.scan_desc != nullptr)
	{
		local.scan_desc->rs_cblock = InvalidBlockNumber;
		local.scan_desc->rs_ntuples = 0;
	}
	local.page_visible_index = 0;
}

static bool
LoadNextSeqScanPage(SeqScanLocalState &local, ExecCtx &ctx)
{
	Assert(local.scan_desc != nullptr);
	Assert(local.read_stream != nullptr);

	ReleaseSeqScanPage(local);
	if (PipelineCancelRequested(ctx))
		return false;
	local.scan_desc->rs_dir = ForwardScanDirection;

	const bool profile_on = PipelineProfileEnabled(ctx);
	instr_time load_start;
	if (profile_on)
		INSTR_TIME_SET_CURRENT(load_start);
	local.scan_desc->rs_cbuf = read_stream_next_buffer(local.read_stream, NULL);
	if (profile_on)
	{
		instr_time load_end;
		INSTR_TIME_SET_CURRENT(load_end);
		PipelineProfileAddDiff(ctx,
			PipelineProfileStage::SCAN_LOAD_PAGE,
			load_end,
			load_start);
	}

	if (!BufferIsValid(local.scan_desc->rs_cbuf))
	{
		local.scan_desc->rs_cblock = InvalidBlockNumber;
		local.scan_desc->rs_ntuples = 0;
		local.page_visible_index = 0;
		return false;
	}

	local.scan_desc->rs_cblock = BufferGetBlockNumber(local.scan_desc->rs_cbuf);

	instr_time prepare_start;
	if (profile_on)
		INSTR_TIME_SET_CURRENT(prepare_start);
	{
		HeapScanDesc scan = local.scan_desc;
		Buffer buffer = scan->rs_cbuf;
		Snapshot snapshot = scan->rs_base.rs_snapshot;
		Page page;
		int lines;
		bool all_visible;
		const bool check_serializable = local.check_serializable;

		/*
		 * pg_yaap AP/Q1 path only needs page-at-a-time visibility state
		 * (rs_vistuples/rs_ntuples). PostgreSQL's heap_prepare_pagescan() also
		 * does opportunistic heap_page_prune_opt(), which shows up as hot work in
		 * xctrace but is maintenance, not scan correctness. Keep the pagemode
		 * visible-tuple contract and skip the prune step.
		 */
		Assert(scan->rs_base.rs_flags & SO_ALLOW_PAGEMODE);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);
		lines = PageGetMaxOffsetNumber(page);
		all_visible = PageIsAllVisible(page) && !snapshot->takenDuringRecovery;
		if (likely(all_visible))
		{
			if (likely(!check_serializable))
				scan->rs_ntuples = PgYaapCollectPageTuples<true, false>(scan,
																	 snapshot,
																	 page,
																	 buffer,
																	 scan->rs_cblock,
																	 lines);
			else
				scan->rs_ntuples = PgYaapCollectPageTuples<true, true>(scan,
																	snapshot,
																	page,
																	buffer,
																	scan->rs_cblock,
																	lines);
		}
		else
		{
			if (likely(!check_serializable))
				scan->rs_ntuples = PgYaapCollectPageTuples<false, false>(scan,
																  snapshot,
																  page,
																  buffer,
																  scan->rs_cblock,
																  lines);
			else
				scan->rs_ntuples = PgYaapCollectPageTuples<false, true>(scan,
																 snapshot,
																 page,
																 buffer,
																 scan->rs_cblock,
																 lines);
		}
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	}
	if (profile_on)
	{
		instr_time prepare_end;
		INSTR_TIME_SET_CURRENT(prepare_end);
		PipelineProfileAddDiff(ctx,
			PipelineProfileStage::SCAN_PREPARE_PAGE,
			prepare_end,
			prepare_start,
			local.scan_desc->rs_ntuples);
	}
	local.page_visible_index = 0;
	return true;
}

static bool
EnsureSeqScanPageLoaded(SeqScanLocalState &local, ExecCtx &ctx)
{
	while (true)
	{
		if (BufferIsValid(local.scan_desc->rs_cbuf) &&
			local.page_visible_index < local.scan_desc->rs_ntuples)
			return true;

		if (!LoadNextSeqScanPage(local, ctx))
			return false;
	}
}

/* Build filter-side DeformBindings against per-input scratch slots. The
 * filter deformer writes one row at index 0; duplicate-column predicates share
 * the same dst_col so each published input maps to one binding slot. */
static inline void
BuildFilterDeformBindings(const FilterInputDesc *inputs,
			         uint16_t n_inputs,
			         DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE> &qchunk,
			         DeformBindings &bindings)
{
	bindings.ncolumns = n_inputs;
	bindings.owner_chunk = &qchunk;
	for (uint8_t i = 0; i < bindings.ncolumns; ++i)
	{
		void *data_head = nullptr;
		switch (inputs[i].decode_kind)
		{
			case ColumnDecodeKind::INT32_CHAR:
			case ColumnDecodeKind::INT32_DATE:
			case ColumnDecodeKind::INT32_INT4:
				data_head = static_cast<void *>(qchunk.int32_columns[i]);
				break;
			case ColumnDecodeKind::INT64_INT8:
			case ColumnDecodeKind::INT64_NUMERIC_SCALED:
				data_head = static_cast<void *>(qchunk.int64_columns[i]);
				break;
			case ColumnDecodeKind::STRING_REF:
				data_head = static_cast<void *>(qchunk.string_columns[i]);
				break;
			default:
				elog(ERROR, "pg_yaap: qual decode kind=%u unsupported",
				     (unsigned) inputs[i].decode_kind);
		}
		bindings.columns_data[i]  = data_head;
		bindings.columns_nulls[i] = qchunk.nulls[i];
	}
}

} // namespace

std::unique_ptr<GlobalSourceState>
PhysicalSeqScan::GetGlobalSourceState(ExecCtx &ctx)
{
	auto state = std::make_unique<SeqScanGlobalState>();
	state->dsa = ctx.dsa;
	state->desc = desc_;

	/*
	 * Bug B fix — mirror HashAgg pattern (physical_hash_aggregate.cpp:94-129).
	 *
	 * SeqScan ctor receives shared_payload_dp = InvalidDsaPointer from the
	 * translator. The leader must self-allocate the SeqScanSharedPayload in
	 * DSA (one per relation, contains the morsel cursor) and publish it back
	 * through StoreSharedPayloadOnDescriptor so worker pipelines can resolve
	 * it via LoadSharedPayloadFromDescriptor. PipelineRunEvent::Schedule
	 * pre-invokes this on the leader before EnqueueTasks (DuckDB-faithful
	 * Pipeline::ResetSource), so when workers later call this method the
	 * descriptor is already populated.
	 */
	state->shared_payload_dp = DsaPointerIsValid(shared_payload_dp_) ? shared_payload_dp_ :
		LoadSharedPayloadFromDescriptor(this);

	if (ctx.worker_index == LEADER_WORKER_INDEX && !DsaPointerIsValid(state->shared_payload_dp))
	{
		Relation rel = relation_open(relid_, AccessShareLock);
		BlockNumber total = RelationGetNumberOfBlocks(rel);

		state->shared_payload_dp = dsa_allocate0(ctx.dsa, sizeof(SeqScanSharedPayload));
		auto *payload = static_cast<SeqScanSharedPayload *>(
			dsa_get_address(ctx.dsa, state->shared_payload_dp));
		table_block_parallelscan_initialize(rel, (ParallelTableScanDesc) &payload->pbscan);
		payload->total_blocks = total;
		relation_close(rel, AccessShareLock);
		StoreSharedPayloadOnDescriptor(this, state->shared_payload_dp);
	}

	state->shared = ResolveSeqScanPayload(ctx, state->shared_payload_dp);
	state->max_threads = ComputeMaxThreadsFromPayload(state->shared);
	return state;
}

std::unique_ptr<LocalSourceState>
PhysicalSeqScan::GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate)
{
	auto &global = static_cast<SeqScanGlobalState &>(gstate);
	std::unique_ptr<SeqScanLocalState> local = std::make_unique<SeqScanLocalState>();

	(void) ctx;

	local->rel = relation_open(relid_, AccessShareLock);
	local->scan_tupdesc = RelationGetDescr(local->rel);
	local->scan_desc = (HeapScanDesc) heap_beginscan(local->rel,
		GetActiveSnapshot(),
		0,
		nullptr,
		global.shared != nullptr ? (ParallelTableScanDesc) &global.shared->pbscan : nullptr,
		SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_PAGEMODE);
	if (local->scan_desc->rs_base.rs_parallel != nullptr)
		InstallAggressiveParallelSeqScanReadStream(local->scan_desc,
		                                           local->rel,
		                                           local->read_stream_state,
		                                           ctx.worker_index,
		                                           SeqScanTraceEnabled(ctx));
	local->read_stream = local->scan_desc->rs_read_stream;
	if (local->read_stream == nullptr)
		elog(ERROR, "pg_yaap: SeqScan failed to initialize read stream");
	local->check_serializable =
		CheckForSerializableConflictOutNeeded(local->rel, GetActiveSnapshot());
	local->exhausted = (global.shared == nullptr || global.shared->total_blocks == 0);

	return std::unique_ptr<LocalSourceState>(std::move(local));
}

SourceResultType
PhysicalSeqScan::GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input)
{
	auto &global = static_cast<SeqScanGlobalState &>(input.global_state);
	auto &local = static_cast<SeqScanLocalState &>(input.local_state);
	const bool first_call = !local.diag_first_call_logged;

	if (first_call)
		local.diag_first_call_logged = true;

	out.reset_lightweight();

	if (local.exhausted || global.shared == nullptr)
		return SourceResultType::FINISHED;

	if (!local.descriptor_cache_ready)
	{
		local.out_schema_cache = ResolveSchemaDescriptor(global.dsa, output_schema_dp_);
		if (local.out_schema_cache == nullptr)
			elog(ERROR, "pg_yaap: PhysicalSeqScan output_schema_dp not published");
		local.filter_inputs_cache = ResolveFilterInputs(global.dsa, filter_inputs_dp_);
		local.filter_exprs_cache = ResolveFilterExprs(global.dsa, filter_exprs_dp_);
		local.filter_steps_cache = ResolveFilterSteps(global.dsa, filter_steps_dp_);
		local.filter_string_consts_cache = DsaPointerIsValid(filter_string_consts_dp_)
			? static_cast<const char *>(dsa_get_address(global.dsa, filter_string_consts_dp_))
			: nullptr;
		local.required_bool_regs_cache = local.filter_exprs_cache != nullptr ?
			RequiredFilterBoolRegs(local.filter_exprs_cache, n_filter_exprs_) : 0;
		if (n_filter_inputs_ > 0 && local.filter_inputs_cache == nullptr)
			elog(ERROR, "pg_yaap: PhysicalSeqScan filter_inputs_dp not published");
		if (n_filter_exprs_ > 0 && local.filter_exprs_cache == nullptr)
			elog(ERROR, "pg_yaap: PhysicalSeqScan filter_exprs_dp not published");
		if (n_filter_steps_ > 0 && local.filter_steps_cache == nullptr)
			elog(ERROR, "pg_yaap: PhysicalSeqScan filter_steps_dp not published");
		if (filter_bool_regs_ > FILTER_MAX_BOOL_REGS)
			elog(ERROR, "pg_yaap: PhysicalSeqScan filter bool register overflow (%u)",
			     static_cast<unsigned>(filter_bool_regs_));
		local.simple_filter_step_cache = nullptr;
		if (n_filter_exprs_ > 0 && local.filter_exprs_cache != nullptr &&
			local.filter_steps_cache != nullptr)
		{
			IsSimpleFilterStep(local.filter_exprs_cache,
			                   n_filter_exprs_,
			                   local.filter_steps_cache,
			                   n_filter_steps_,
			                   &local.simple_filter_step_cache);
		}
		local.descriptor_cache_ready = true;
	}

	SchemaDescriptor *out_schema = local.out_schema_cache;
	FilterInputDesc *filter_inputs = local.filter_inputs_cache;
	FilterExprDesc *filter_exprs = local.filter_exprs_cache;
	FilterStep *filter_steps = local.filter_steps_cache;
	const char *filter_string_consts = local.filter_string_consts_cache;
	const uint16_t required_bool_regs = local.required_bool_regs_cache;
	const FilterStep *simple_filter_step = local.simple_filter_step_cache;

	(void) ctx;

	/*
	 * First-call deformer build. Why here (not in GetLocalSourceState):
	 * out_schema is resolved from a DSA pointer that is only valid after
	 * descriptor publish; the local-state ctor runs before that for
	 * leader-self-allocate paths. Builds BOTH projection and filter programs
	 * (filter only when published), allocates the filter scratch chunk lazily,
	 * and JITs each independently. JIT compile is opportunistic and silently
	 * falls back to native interpreter on failure. The filter deformer uses the
	 * same factory/dispatch path as projection; only the target list differs.
	 */
	if (!local.deform_programs_built)
	{
		if (BuildProjDeformProgramFromSchema(out_schema, local.proj_deform_program))
		{
			local.proj_deformer = std::make_unique<DataChunkDeformer>(local.scan_tupdesc,
			                                                           &local.proj_deform_program);
			bool proj_jit_compiled = false;
			const char *proj_jit_reason = nullptr;
#ifdef USE_LLVM
			const bool allow_proj_worker_jit =
				!pg_yaap_disable_jit_for_parallel_worker ||
				ctx.worker_index == LEADER_WORKER_INDEX;
			if (pg_yaap_jit_deform && allow_proj_worker_jit)
			{
				JitDeformFunc fn = nullptr;
				JitContext   *jc = nullptr;
				const char   *err = nullptr;
				if (pg_yaap_try_compile_jit_deform_to_datachunk(local.scan_tupdesc,
				                                                    &local.proj_deform_program,
				                                                    &fn, &jc, &err) &&
				    fn != nullptr)
				{
					proj_jit_compiled    = true;
					local.proj_jit_func    = fn;
					local.proj_jit_context = jc;
					local.proj_deformer->set_jit_func(fn);
					if (jc != nullptr)
						pg_yaap_register_llvm_jit_context(jc);
				}
				else
					proj_jit_reason =
						(err != nullptr && err[0] != '\0') ?
						err : "compile returned false without failure reason";
			}
			else if (!pg_yaap_jit_deform)
				proj_jit_reason = "disabled by pg_yaap.jit_deform";
			else
				proj_jit_reason = "disabled for parallel worker";
#endif
			LogSeqScanDeformerJitDecision(ctx,
										 "projection",
										 local.proj_deform_program,
										 proj_jit_compiled,
										 proj_jit_reason);
		}

		/* Filter side: only built if a real predicate exists. Empty filter tapes
		 * skip this entire block; the inner loop calls projection directly. */
		if (n_filter_inputs_ > 0 && n_filter_exprs_ > 0 && n_filter_steps_ > 0)
		{
			if (!BuildFilterDeformProgram(filter_inputs,
					n_filter_inputs_,
					local.filter_deform_program))
				ereport(ERROR,
				        (errmsg("pg_yaap: failed to build SeqScan filter deformer")));
			local.filter_uses_string_arena = false;
			for (int target_idx = 0; target_idx < local.filter_deform_program.ntargets; ++target_idx)
			{
				if (local.filter_deform_program.targets[target_idx].decode_kind == DeformDecodeKind::kStringRef)
				{
					local.filter_uses_string_arena = true;
					break;
				}
			}
			local.filter_chunk = std::unique_ptr<DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>>(
				new DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>());
			local.filter_deformer = std::make_unique<DataChunkDeformer>(local.scan_tupdesc,
								    &local.filter_deform_program);
			bool filter_jit_compiled = false;
			const char *filter_jit_reason = nullptr;
#ifdef USE_LLVM
			const bool allow_filter_worker_jit =
				!pg_yaap_disable_jit_for_parallel_worker ||
				ctx.worker_index == LEADER_WORKER_INDEX;
			if (pg_yaap_jit_deform && allow_filter_worker_jit)
			{
				JitDeformFunc fn = nullptr;
				JitContext   *jc = nullptr;
				const char   *err = nullptr;
				if (pg_yaap_try_compile_jit_deform_to_datachunk(local.scan_tupdesc,
				                                                    &local.filter_deform_program,
				                                                    &fn, &jc, &err) &&
				    fn != nullptr)
				{
					filter_jit_compiled    = true;
					local.filter_jit_func    = fn;
					local.filter_jit_context = jc;
					local.filter_deformer->set_jit_func(fn);
					if (jc != nullptr)
						pg_yaap_register_llvm_jit_context(jc);
				}
				else
					filter_jit_reason =
						(err != nullptr && err[0] != '\0') ?
						err : "compile returned false without failure reason";
			}
			else if (!pg_yaap_jit_deform)
				filter_jit_reason = "disabled by pg_yaap.jit_deform";
			else
				filter_jit_reason = "disabled for parallel worker";
#endif
			LogSeqScanDeformerJitDecision(ctx,
										 "filter",
										 local.filter_deform_program,
										 filter_jit_compiled,
										 filter_jit_reason);
		}
		local.deform_programs_built = true;
	}

	/* Pre-build filter bindings once per GetData call (heads point at row 0 of
	 * the persistent filter_chunk; the deformer always writes there). */
	DeformBindings filter_bindings;
	const bool has_filter = (n_filter_inputs_ > 0 && n_filter_exprs_ > 0 &&
				      n_filter_steps_ > 0 && local.filter_deformer != nullptr &&
				      local.filter_chunk != nullptr);
	if (has_filter)
		BuildFilterDeformBindings(filter_inputs, n_filter_inputs_, *local.filter_chunk, filter_bindings);
	DeformBindings proj_bindings;
	BuildDeformBindings(out_schema, out, proj_bindings);
	const bool use_simple_filter = has_filter && simple_filter_step != nullptr;
	const bool profile_on = PipelineProfileEnabled(ctx);

	uint32_t tuple_checks = 0;
	while (out.count < PIPELINE_DEFAULT_CHUNK_SIZE)
	{
		if (PipelineCancelRequestedEvery(ctx, tuple_checks++, 1023u))
		{
			ReleaseSeqScanPage(local);
			break;
		}
		if (!EnsureSeqScanPageLoaded(local, ctx))
		{
			local.exhausted = true;
			break;
		}

		instr_time tuple_iter_start;
		if (profile_on)
			INSTR_TIME_SET_CURRENT(tuple_iter_start);

		OffsetNumber offnum = local.scan_desc->rs_vistuples[local.page_visible_index++];
		Page page = BufferGetPage(local.scan_desc->rs_cbuf);
		ItemId lpp = PageGetItemId(page, offnum);
		HeapTupleData tuple;
		tuple.t_data = (HeapTupleHeader) PageGetItem(page, lpp);
		tuple.t_len = ItemIdGetLength(lpp);
		tuple.t_tableOid = RelationGetRelid(local.scan_desc->rs_base.rs_rd);
		ItemPointerSet(&(tuple.t_self), local.scan_desc->rs_cblock, offnum);

		if (profile_on)
		{
			instr_time tuple_iter_end;
			INSTR_TIME_SET_CURRENT(tuple_iter_end);
			PipelineProfileAddDiff(ctx,
				PipelineProfileStage::SCAN_VISIBLE_TUPLE,
				tuple_iter_end,
				tuple_iter_start,
				1);
		}

		if (tuple.t_data == nullptr)
			continue;

		/* B.1 Option C: deform filter cols first (1-row scratch at row 0),
		 * evaluate predicate inline; only on survival do we deform the
		 * full projection at out.count and increment. Q1 rejects ~96.6%
		 * of rows so the projection deform is short-circuited for the
		 * vast majority of tuples. */
		if (has_filter)
		{
			instr_time qual_deform_start;
			if (profile_on)
				INSTR_TIME_SET_CURRENT(qual_deform_start);
			if (local.filter_uses_string_arena)
				local.filter_chunk->string_arena.clear();
			local.filter_deformer->deform_tuple_header(tuple.t_data, 0, filter_bindings);
			if (profile_on)
			{
				instr_time qual_deform_end;
				INSTR_TIME_SET_CURRENT(qual_deform_end);
				PipelineProfileAddDiff(ctx,
					PipelineProfileStage::SCAN_QUAL_DEFORM,
					qual_deform_end,
					qual_deform_start,
					1);
			}

			instr_time filter_start;
			if (profile_on)
				INSTR_TIME_SET_CURRENT(filter_start);
			bool pass;
			if (use_simple_filter)
			{
				pass = EvalSimpleFilterStep(*simple_filter_step,
					*local.filter_chunk,
					filter_string_consts);
			}
			else
			{
				pass = true;
				if (required_bool_regs > 0)
					std::memset(local.filter_bool_values, 0, required_bool_regs * sizeof(local.filter_bool_values[0]));
				for (uint16_t expr_idx = 0; expr_idx < n_filter_exprs_; ++expr_idx)
				{
					const FilterExprDesc &expr = filter_exprs[expr_idx];
					const uint16_t expr_end = expr.first_step_idx + expr.n_steps;
					if (expr_end > n_filter_steps_)
						elog(ERROR, "pg_yaap: filter expression step range overflow");
					for (uint16_t step_idx = expr.first_step_idx; step_idx < expr_end; ++step_idx)
						EvalFilterStep(filter_steps[step_idx],
							*local.filter_chunk,
							filter_string_consts,
							local.filter_bool_values);
					if (expr.output_bool_reg >= filter_bool_regs_ ||
					    !local.filter_bool_values[expr.output_bool_reg])
					{
						pass = false;
						break;
					}
				}
			}
			if (profile_on)
			{
				instr_time filter_end;
				INSTR_TIME_SET_CURRENT(filter_end);
				PipelineProfileAddDiff(ctx,
					PipelineProfileStage::SCAN_FILTER,
					filter_end,
					filter_start,
					1);
			}
			if (!pass)
				continue;
		}
		instr_time proj_start;
		if (profile_on)
			INSTR_TIME_SET_CURRENT(proj_start);
		AppendProjectedTupleViaDeformer(out, &tuple, proj_bindings, local);
		if (profile_on)
		{
			instr_time proj_end;
			INSTR_TIME_SET_CURRENT(proj_end);
			PipelineProfileAddDiff(ctx,
				PipelineProfileStage::SCAN_PROJ_DEFORM,
				proj_end,
				proj_start,
				1);
		}
	}

	if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
	{
		if (first_call && pg_yaap_trace_execution_path)
		{
			elog(LOG,
			     "pg_yaap: SeqScan.GetData first_call full_chunk=%u exhausted=%d has_filter=%d total_blocks=%u",
			     out.count,
			     local.exhausted ? 1 : 0,
			     has_filter ? 1 : 0,
			     global.shared != nullptr ? global.shared->total_blocks : 0);
		}
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	ReleaseSeqScanPage(local);
	if (first_call && pg_yaap_trace_execution_path)
		elog(LOG,
		     "pg_yaap: SeqScan.GetData first_call out_count=%u exhausted=%d has_filter=%d total_blocks=%u",
		     out.count,
		     local.exhausted ? 1 : 0,
		     has_filter ? 1 : 0,
		     global.shared != nullptr ? global.shared->total_blocks : 0);
	return out.count > 0 ? SourceResultType::HAVE_MORE_OUTPUT
	                    : SourceResultType::FINISHED;
}

int
PhysicalSeqScan::MaxThreads(ExecCtx &ctx) const
{
	/* Bug N: shared_payload_dp_ ctor field is InvalidDsaPointer until the
	 * leader self-allocates inside GetGlobalSourceState; reading it here
	 * collapsed RUN fan-out to 1 and serialized the entire scan onto
	 * worker 0 (loop_wait == active_w_task across w in {1,2,4,8}).
	 * Load-from-descriptor is the canonical cross-process channel
	 * (Bug H invariant). PipelineRunEvent::Schedule pre-invokes
	 * GetGlobalSourceState on the leader before EnqueueTasks calls
	 * MaxThreads, so the descriptor slot is populated by this point. */
	dsa_pointer dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	SeqScanSharedPayload *shared = ResolveSeqScanPayload(ctx, dp);
	return (int) ComputeMaxThreadsFromPayload(shared);
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
