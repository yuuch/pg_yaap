#include "parallel/pipeline/physical_hash_join.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"

extern int pg_yaap_parallel_max_workers;
extern bool pg_yaap_trace_execution_path;
extern bool pg_yaap_trace_hooks;
}

#include <algorithm>
#include <cstring>
#include <string_view>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/cancel.hpp"
#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/physical_hash_join_combine.hpp"
#include "parallel/pipeline/physical_hash_join_fast_probe.hpp"
#include "parallel/pipeline/physical_seq_scan.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

static constexpr uint32_t HASH_JOIN_INVALID_ROW = UINT32_MAX;
static constexpr uint32_t HASH_JOIN_MAX_INITIAL_ROWS = 1u << 20;

static uint32_t
EffectiveWorkerCount(const ExecCtx &ctx)
{
	if (ctx.control != nullptr && ctx.control->num_workers > 0)
		return static_cast<uint32_t>(ctx.control->num_workers);
	return static_cast<uint32_t>(std::max(1, pg_yaap_parallel_max_workers));
}

static inline uint16_t
HashJoinSalt(uint64_t hash)
{
	return static_cast<uint16_t>(hash >> 48);
}

static uint32_t
HashJoinInitialRows(uint32_t estimated_rows)
{
	return std::max<uint32_t>(1024u, std::min<uint32_t>(estimated_rows, HASH_JOIN_MAX_INITIAL_ROWS));
}

static HashJoinSharedPayload *
ResolvePayload(dsa_area *dsa, dsa_pointer payload_dp)
{
	if (!DsaPointerIsValid(payload_dp))
		return nullptr;
	return static_cast<HashJoinSharedPayload *>(dsa_get_address(dsa, payload_dp));
}

static const TupleDataLayout *
ResolveLayout(dsa_area *dsa, dsa_pointer layout_dp)
{
	if (!DsaPointerIsValid(layout_dp))
		return nullptr;
	return static_cast<const TupleDataLayout *>(dsa_get_address(dsa, layout_dp));
}

static TupleDataCollection *
ResolveTdc(dsa_area *dsa, dsa_pointer tdc_dp)
{
	if (!DsaPointerIsValid(tdc_dp))
		return nullptr;
	return static_cast<TupleDataCollection *>(dsa_get_address(dsa, tdc_dp));
}

static HashJoinLocalBuildRegistryEntry *
ResolveLocalBuildRegistry(dsa_area *dsa, HashJoinSharedPayload *payload)
{
	if (payload == nullptr || !DsaPointerIsValid(payload->local_build_registry_dp))
		return nullptr;
	return static_cast<HashJoinLocalBuildRegistryEntry *>(
		dsa_get_address(dsa, payload->local_build_registry_dp));
}

static void
FreeDsaPointerIfValid(dsa_area *dsa, dsa_pointer *dp)
{
	if (dp != nullptr && DsaPointerIsValid(*dp))
	{
		dsa_free(dsa, *dp);
		*dp = InvalidDsaPointer;
	}
}

static uint64_t
EstimateTdcAllocBytes(dsa_area *dsa, dsa_pointer tdc_dp)
{
	TupleDataCollection *tdc = ResolveTdc(dsa, tdc_dp);
	if (tdc == nullptr)
		return 0;
	return static_cast<uint64_t>(TupleDataCollectionAllocSize(tdc->row_capacity,
		tdc->row_width,
		tdc->heap_capacity));
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

static void
CopyBuildRow(const TupleDataLayout *layout,
	         const TupleDataCollection *src_tdc,
	         const uint8_t *src_row,
	         TupleDataCollection *dst_tdc,
	         uint8_t *dst_row)
{
	for (uint16_t col_idx = 0; col_idx < layout->column_count; ++col_idx)
	{
		const TdcColumnDesc &col = layout->columns[col_idx];
		if (col.kind != TdcColumnKind::STRING_REF)
		{
			std::memcpy(dst_row + col.offset, src_row + col.offset, col.width);
			continue;
		}

		VecStringRef src_ref;
		std::memcpy(&src_ref, src_row + col.offset, sizeof(src_ref));
		const char *src_ptr = VecStringRefDataPtr(src_ref,
			src_tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(src_tdc)) : nullptr);
		VecStringRef dst_ref;
		if (!TupleDataCollectionStoreStringBytes(dst_tdc, src_ptr, src_ref.len, &dst_ref))
			elog(ERROR, "pg_yaap: hash join build-row copy ran out of heap");
		std::memcpy(dst_row + col.offset, &dst_ref, sizeof(dst_ref));
	}
}

static inline bool
TdcNeedsGrowForChunkRow(const TupleDataLayout *layout,
	                  const TupleDataCollection *tdc,
	                  const PipelineChunk &chunk,
	                  uint16_t row_idx)
{
	return !TupleDataCollectionHasSpaceForAppend(
		tdc,
		TupleDataCollectionRequiredHeapBytesForChunkRow(layout, chunk, row_idx));
}

static inline bool
TdcNeedsGrowForStoredRow(const TupleDataLayout *layout,
	                    const TupleDataCollection *tdc,
	                    const TupleDataCollection *src_tdc,
	                    const uint8_t *src_row)
{
	return !TupleDataCollectionHasSpaceForAppend(
		tdc,
		TupleDataCollectionRequiredHeapBytesForRow(layout, src_tdc, src_row));
}

static bool
LayoutHasStringRef(const TupleDataLayout *layout)
{
	if (layout == nullptr)
		return false;
	for (uint16_t col_idx = 0; col_idx < layout->column_count; ++col_idx)
	{
		if (layout->columns[col_idx].kind == TdcColumnKind::STRING_REF)
			return true;
	}
	return false;
}

static bool
LayoutsEqual(const TupleDataLayout *lhs, const TupleDataLayout *rhs)
{
	if (lhs == rhs)
		return true;
	if (lhs == nullptr || rhs == nullptr)
		return false;
	if (lhs->column_count != rhs->column_count ||
		lhs->aggregate_count != rhs->aggregate_count ||
		lhs->row_width != rhs->row_width ||
		lhs->validity_width != rhs->validity_width)
		return false;
	return std::memcmp(lhs->columns,
		rhs->columns,
		sizeof(TdcColumnDesc) * lhs->column_count) == 0 &&
		std::memcmp(lhs->aggregates,
			rhs->aggregates,
			sizeof(TdcAggregateDesc) * lhs->aggregate_count) == 0;
}

static bool
OutputColumnsReferenceRight(const HashJoinOutputColumnDesc *output_columns,
					   uint16_t output_column_count)
{
	if (output_columns == nullptr)
		return false;
	for (uint16_t i = 0; i < output_column_count; ++i)
	{
		if (output_columns[i].side == HashJoinOutputSide::RIGHT &&
			output_columns[i].decode_kind != ColumnDecodeKind::NONE)
			return true;
	}
	return false;
}

static bool
TryHashSingleJoinKeyRow(const TupleDataLayout *layout,
	                    const TupleDataCollection *tdc,
	                    const uint8_t *row_ptr,
	                    uint64_t &out_hash)
{
	if (layout == nullptr || row_ptr == nullptr || layout->column_count != 1)
		return false;
	const TdcColumnDesc &col = layout->columns[0];
	switch (col.kind)
	{
		case TdcColumnKind::INT32:
		{
			int32_t v;
			std::memcpy(&v, row_ptr + col.offset, sizeof(v));
			out_hash = HashSingleGroupInt32Value(v);
			return true;
		}
		case TdcColumnKind::INT64:
		{
			int64_t v;
			std::memcpy(&v, row_ptr + col.offset, sizeof(v));
			out_hash = HashSingleGroupInt64Value(v);
			return true;
		}
		case TdcColumnKind::DOUBLE:
		case TdcColumnKind::STRING_REF:
			return false;
	}
	return false;
}

static bool
TryHashSingleJoinKeyChunkRow(const TupleDataLayout *layout,
	                         const PipelineChunk &chunk,
	                         uint16_t row_idx,
	                         uint64_t &out_hash)
{
	if (layout == nullptr || layout->column_count != 1)
		return false;
	const TdcColumnDesc &col = layout->columns[0];
	if (col.src_col_idx >= 16)
		return false;
	switch (col.kind)
	{
		case TdcColumnKind::INT32:
			out_hash = HashSingleGroupInt32Value(chunk.get_int32(col.src_col_idx, row_idx));
			return true;
		case TdcColumnKind::INT64:
			out_hash = HashSingleGroupInt64Value(chunk.get_int64(col.src_col_idx, row_idx));
			return true;
		case TdcColumnKind::DOUBLE:
		case TdcColumnKind::STRING_REF:
			return false;
	}
	return false;
}

static const uint8_t *
ResolveSharedLocalBuildRowCached(dsa_area *dsa,
						 const HashJoinSharedPayload *payload,
						 const HashJoinLocalBuildRegistryEntry *registry,
						 const uint32_t *slots,
						 const uint32_t *local_idxs,
						 uint32_t build_row_idx,
						 const TupleDataCollection **out_rows_tdc)
{
	if (payload == nullptr || !payload->build_rows_shared_local ||
		registry == nullptr || slots == nullptr || local_idxs == nullptr)
		elog(ERROR, "pg_yaap: shared-local hash join payload mappings missing");
	const uint32_t slot = slots[build_row_idx];
	if (slot >= payload->local_state_slot_count)
		elog(ERROR, "pg_yaap: shared-local hash join registry slot out of range");
	const TupleDataCollection *rows_tdc = ResolveTdc(dsa, registry[slot].build_rows_dp);
	if (rows_tdc == nullptr)
		elog(ERROR, "pg_yaap: shared-local hash join row store missing");
	const uint32_t local_row_idx = local_idxs[build_row_idx];
	if (local_row_idx >= pg_atomic_read_u32(const_cast<pg_atomic_uint32 *>(&rows_tdc->row_count)))
		elog(ERROR, "pg_yaap: shared-local hash join local row index out of range");
	if (out_rows_tdc != nullptr)
		*out_rows_tdc = rows_tdc;
	return TupleDataCollectionGetRowConst(rows_tdc, local_row_idx);
}

static void
GrowJoinTdc(ExecCtx &ctx,
	       const TupleDataLayout *layout,
	       dsa_pointer layout_dp,
	       dsa_pointer *tdc_dp,
	       uint32_t required_heap_bytes)
{
	dsa_pointer old_tdc_dp = *tdc_dp;
	TupleDataCollection *old_tdc = ResolveTdc(ctx.dsa, *tdc_dp);
	if (old_tdc == nullptr)
		elog(ERROR, "pg_yaap: hash join TDC missing during grow");

	const uint32_t old_count = pg_atomic_read_u32(&old_tdc->row_count);
	uint32_t new_capacity = std::max(old_tdc->row_capacity * 2u, old_count + 1u);
	new_capacity = TupleDataCollectionClampRowCapacity(new_capacity,
		layout->row_width,
		required_heap_bytes,
		old_count + 1u);
	const uint32_t heap_capacity = TupleDataCollectionGrowHeapCapacity(layout,
		old_tdc,
		new_capacity,
		required_heap_bytes);
	if (pg_yaap_trace_execution_path)
		ereport(LOG,
			(errmsg("pg_yaap hashjoin tdc grow old_count=%u old_capacity=%u new_capacity=%u row_width=%u heap_capacity=%u required_heap=%u",
				old_count,
				old_tdc->row_capacity,
				new_capacity,
				layout->row_width,
				heap_capacity,
				required_heap_bytes)));
	dsa_pointer new_tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
		new_capacity,
		layout->row_width,
		heap_capacity);
	auto *new_tdc = ResolveTdc(ctx.dsa, new_tdc_dp);
	TupleDataCollectionInit(new_tdc,
		new_capacity,
		layout->row_width,
		layout_dp,
		heap_capacity);

	for (uint32_t row_idx = 0; row_idx < old_count; ++row_idx)
	{
		uint8_t *dst = nullptr;
		const uint32_t copied_idx = TupleDataCollectionAppendRow(new_tdc, &dst);
		if (copied_idx == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: hash join TDC grow overflow");
		const uint8_t *src = TupleDataCollectionGetRowConst(old_tdc, row_idx);
		CopyBuildRow(layout, old_tdc, src, new_tdc, dst);
	}

	*tdc_dp = new_tdc_dp;
	dsa_free(ctx.dsa, old_tdc_dp);
}

static void
UpdateHashJoinLocalBuildRegistry(ExecCtx &ctx,
	                            HashJoinSharedPayload *payload,
	                            HashJoinLocalSinkState &local)
{
	if (ctx.worker_index < 0)
		return;
	auto *registry = ResolveLocalBuildRegistry(ctx.dsa, payload);
	if (registry == nullptr || static_cast<uint32_t>(ctx.worker_index) >= payload->local_state_slot_count)
		elog(ERROR, "pg_yaap: hash join local build registry missing during grow");
	registry[ctx.worker_index].build_keys_dp = local.build_keys_dp;
	registry[ctx.worker_index].build_rows_dp = local.build_rows_dp;
}

static void
EnsureJoinLocalCapacity(ExecCtx &ctx,
	                  HashJoinSharedPayload *payload,
	                  dsa_pointer key_layout_dp,
	                  const TupleDataLayout *key_layout,
	                  dsa_pointer row_layout_dp,
	                  const TupleDataLayout *row_layout,
	                  HashJoinLocalSinkState &local,
	                  const PipelineChunk &chunk,
	                  uint16_t row_idx)
{
	while (TdcNeedsGrowForChunkRow(key_layout, local.build_keys, chunk, row_idx) ||
	       (!payload->build_rows_use_keys &&
	        TdcNeedsGrowForChunkRow(row_layout, local.build_rows, chunk, row_idx)))
	{
		if (TdcNeedsGrowForChunkRow(key_layout, local.build_keys, chunk, row_idx))
			GrowJoinTdc(ctx,
				key_layout,
				key_layout_dp,
				&local.build_keys_dp,
				TupleDataCollectionRequiredHeapBytesForChunkRow(key_layout, chunk, row_idx));
		if (!payload->build_rows_use_keys &&
			TdcNeedsGrowForChunkRow(row_layout, local.build_rows, chunk, row_idx))
			GrowJoinTdc(ctx,
				row_layout,
				row_layout_dp,
				&local.build_rows_dp,
				TupleDataCollectionRequiredHeapBytesForChunkRow(row_layout, chunk, row_idx));
		if (payload->build_rows_use_keys)
			local.build_rows_dp = local.build_keys_dp;
		local.build_keys = ResolveTdc(ctx.dsa, local.build_keys_dp);
		local.build_rows = payload->build_rows_use_keys
			? local.build_keys
			: ResolveTdc(ctx.dsa, local.build_rows_dp);
		if (local.build_keys == nullptr || local.build_rows == nullptr)
			elog(ERROR, "pg_yaap: hash join local build rows missing after grow");
		UpdateHashJoinLocalBuildRegistry(ctx, payload, local);
	}
}

static void
EnsureJoinGlobalCapacity(ExecCtx &ctx,
	                   HashJoinGlobalSinkState &global,
	                   const uint8_t *src_key_row,
	                   const TupleDataCollection *src_key_tdc,
	                   const uint8_t *src_row,
	                   const TupleDataCollection *src_row_tdc)
{
	global.build_keys = ResolveTdc(ctx.dsa, global.payload->build_keys_dp);
	global.build_rows = global.payload->build_rows_use_keys
		? global.build_keys
		: ResolveTdc(ctx.dsa, global.payload->build_rows_dp);
	if (global.build_keys == nullptr || global.build_rows == nullptr)
		elog(ERROR, "pg_yaap: hash join global build rows missing before grow");
	while (TdcNeedsGrowForStoredRow(global.build_key_layout,
			global.build_keys,
			src_key_tdc,
			src_key_row) ||
	       (!global.payload->build_rows_use_keys &&
	        TdcNeedsGrowForStoredRow(global.build_layout,
			global.build_rows,
			src_row_tdc,
			src_row)))
	{
		if (TdcNeedsGrowForStoredRow(global.build_key_layout,
				global.build_keys,
				src_key_tdc,
				src_key_row))
			GrowJoinTdc(ctx,
				global.build_key_layout,
				global.build_key_layout_dp,
				&global.payload->build_keys_dp,
				TupleDataCollectionRequiredHeapBytesForRow(global.build_key_layout,
					src_key_tdc,
					src_key_row));
		if (!global.payload->build_rows_use_keys &&
			TdcNeedsGrowForStoredRow(global.build_layout,
				global.build_rows,
				src_row_tdc,
				src_row))
			GrowJoinTdc(ctx,
				global.build_layout,
				global.build_layout_dp,
				&global.payload->build_rows_dp,
				TupleDataCollectionRequiredHeapBytesForRow(global.build_layout,
					src_row_tdc,
					src_row));
		if (global.payload->build_rows_use_keys)
			global.payload->build_rows_dp = global.payload->build_keys_dp;
		global.build_keys = ResolveTdc(ctx.dsa, global.payload->build_keys_dp);
		global.build_rows = global.payload->build_rows_use_keys
			? global.build_keys
			: ResolveTdc(ctx.dsa, global.payload->build_rows_dp);
		if (global.build_keys == nullptr || global.build_rows == nullptr)
			elog(ERROR, "pg_yaap: hash join global build rows missing after grow");
	}
}

static bool
ColumnDecodeKindToTdcKind(ColumnDecodeKind decode_kind, TdcColumnKind &out)
{
	switch (decode_kind)
	{
		case ColumnDecodeKind::INT32_CHAR:
		case ColumnDecodeKind::INT32_DATE:
		case ColumnDecodeKind::INT32_INT4:
			out = TdcColumnKind::INT32;
			return true;
		case ColumnDecodeKind::INT64_INT8:
		case ColumnDecodeKind::INT64_NUMERIC_SCALED:
			out = TdcColumnKind::INT64;
			return true;
		case ColumnDecodeKind::DOUBLE_FLOAT8:
			out = TdcColumnKind::DOUBLE;
			return true;
		case ColumnDecodeKind::STRING_REF:
			out = TdcColumnKind::STRING_REF;
			return true;
		case ColumnDecodeKind::NONE:
			return false;
	}
	return false;
}

static const TdcColumnDesc *
FindRightColumnBySlotAndKind(const TupleDataLayout *right_layout,
					 uint8_t input_chunk_slot,
					 ColumnDecodeKind decode_kind)
{
	TdcColumnKind expected_kind;
	if (!ColumnDecodeKindToTdcKind(decode_kind, expected_kind))
		elog(ERROR, "pg_yaap: invalid hash join right payload decode kind %u",
		     static_cast<unsigned>(decode_kind));
	for (uint16_t col_idx = 0; col_idx < right_layout->column_count; ++col_idx)
	{
		const TdcColumnDesc &col = right_layout->columns[col_idx];
		if (col.src_col_idx == input_chunk_slot && col.kind == expected_kind)
			return &col;
	}
	return nullptr;
}

static bool
ReadPromotedFilterInt64FromProbe(const HashJoinFilterInputDesc &input,
								 const PipelineChunk &probe_chunk,
								 uint16_t probe_row_idx,
								 int64_t &out_value)
{
	switch (input.source_decode_kind)
	{
		case ColumnDecodeKind::INT32_CHAR:
		case ColumnDecodeKind::INT32_DATE:
		case ColumnDecodeKind::INT32_INT4:
			out_value = static_cast<int64_t>(probe_chunk.get_int32(input.input_chunk_slot, probe_row_idx));
			return true;
		case ColumnDecodeKind::INT64_INT8:
		case ColumnDecodeKind::INT64_NUMERIC_SCALED:
			out_value = probe_chunk.get_int64(input.input_chunk_slot, probe_row_idx);
			return true;
		default:
			return false;
	}
}

static bool
ReadPromotedFilterInt64FromBuild(const HashJoinFilterInputDesc &input,
								 const TdcColumnDesc *right_col,
								 const uint8_t *build_row,
								 int64_t &out_value)
{
	switch (input.source_decode_kind)
	{
		case ColumnDecodeKind::INT32_CHAR:
		case ColumnDecodeKind::INT32_DATE:
		case ColumnDecodeKind::INT32_INT4:
		{
			int32_t value = 0;
			std::memcpy(&value, build_row + right_col->offset, sizeof(value));
			out_value = static_cast<int64_t>(value);
			return true;
		}
		case ColumnDecodeKind::INT64_INT8:
		case ColumnDecodeKind::INT64_NUMERIC_SCALED:
			std::memcpy(&out_value, build_row + right_col->offset, sizeof(out_value));
			return true;
		default:
			return false;
	}
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

static HashJoinFilterInputDesc *
ResolveFilterInputs(dsa_area *dsa, dsa_pointer dp)
{
	if (!DsaPointerIsValid(dp))
		return nullptr;
	return static_cast<HashJoinFilterInputDesc *>(dsa_get_address(dsa, dp));
}

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
Pow10Int64Local(uint8_t exp, int64_t &out)
{
	int64_t value = 1;
	for (uint8_t i = 0; i < exp; ++i)
	{
		if (value > PG_INT64_MAX / 10)
			return false;
		value *= 10;
	}
	out = value;
	return true;
}

static inline bool
RescaleNumericForCompare(int64_t value, uint8_t from_scale, uint8_t to_scale, int64_t &out)
{
	if (from_scale == to_scale)
	{
		out = value;
		return true;
	}
	if (from_scale > to_scale)
		return false;
	int64_t factor = 0;
	if (!Pow10Int64Local(static_cast<uint8_t>(to_scale - from_scale), factor))
		return false;
	NumericWideInt widened = WideIntFromInt64(value) * WideIntFromInt64(factor);
	if (!WideIntFitsInt64(widened))
		return false;
	out = WideIntToInt64Checked(widened, "pg_yaap: numeric compare rescale overflow");
	return true;
}

struct SimpleJoinFilterInputRef {
	const HashJoinFilterInputDesc *input = nullptr;
	const TdcColumnDesc *right_col = nullptr;
};

struct SimpleJoinFilterSpec {
	bool valid = false;
	FilterStepOp op = FilterStepOp::BOOL_NOT;
	QualOp cmp_op = QualOp::EQ;
	SimpleJoinFilterInputRef left;
	SimpleJoinFilterInputRef right;
};

static inline bool
IsSimpleInt32JoinFilterInput(const HashJoinFilterInputDesc &input)
{
	return input.decode_kind == input.source_decode_kind &&
		(input.decode_kind == ColumnDecodeKind::INT32_CHAR ||
		 input.decode_kind == ColumnDecodeKind::INT32_DATE ||
		 input.decode_kind == ColumnDecodeKind::INT32_INT4);
}

static inline bool
IsSimpleInt64JoinFilterInput(const HashJoinFilterInputDesc &input)
{
	return input.decode_kind == input.source_decode_kind &&
		(input.decode_kind == ColumnDecodeKind::INT64_INT8 ||
		 input.decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED);
}

static bool
TryBuildSimpleJoinFilterSpec(const HashJoinFilterInputDesc *inputs,
							   uint16_t n_inputs,
							   const FilterExprDesc *exprs,
							   uint16_t n_exprs,
							   const FilterStep *steps,
							   uint16_t n_steps,
							   const TupleDataLayout *build_layout,
							   SimpleJoinFilterSpec &out)
{
	out = {};
	if (inputs == nullptr || exprs == nullptr || steps == nullptr || build_layout == nullptr ||
		n_exprs != 1 || n_steps != 1)
		return false;

	const FilterExprDesc &expr = exprs[0];
	const FilterStep &step = steps[0];
	if (expr.first_step_idx != 0 || expr.n_steps != 1 ||
		step.out_bool_reg != expr.output_bool_reg ||
		step.left_idx >= n_inputs || step.right_idx >= n_inputs ||
		inputs[step.left_idx].side == inputs[step.right_idx].side)
		return false;

	const HashJoinFilterInputDesc &left_input = inputs[step.left_idx];
	const HashJoinFilterInputDesc &right_input = inputs[step.right_idx];
	if (step.op == FilterStepOp::INT32_CMP_VAR)
	{
		if (!IsSimpleInt32JoinFilterInput(left_input) ||
			!IsSimpleInt32JoinFilterInput(right_input))
			return false;
	}
	else if (step.op == FilterStepOp::INT64_CMP_VAR)
	{
		if (!IsSimpleInt64JoinFilterInput(left_input) ||
			!IsSimpleInt64JoinFilterInput(right_input))
			return false;
		if (left_input.decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED &&
			right_input.decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED &&
			left_input.numeric_scale != right_input.numeric_scale)
			return false;
	}
	else
		return false;

	out.op = step.op;
	out.cmp_op = step.cmp_op;
	out.left.input = &left_input;
	out.right.input = &right_input;
	if (left_input.side == HashJoinOutputSide::RIGHT)
	{
		out.left.right_col = FindRightColumnBySlotAndKind(build_layout,
			left_input.input_chunk_slot,
			left_input.source_decode_kind);
		if (out.left.right_col == nullptr)
			return false;
	}
	if (right_input.side == HashJoinOutputSide::RIGHT)
	{
		out.right.right_col = FindRightColumnBySlotAndKind(build_layout,
			right_input.input_chunk_slot,
			right_input.source_decode_kind);
		if (out.right.right_col == nullptr)
			return false;
	}
	out.valid = true;
	return true;
}

static inline int32_t
ReadSimpleJoinFilterInt32(const SimpleJoinFilterInputRef &input,
						  const PipelineChunk &probe_chunk,
						  uint16_t probe_row_idx,
						  const uint8_t *build_row)
{
	if (input.input == nullptr)
		elog(ERROR, "pg_yaap: simple hash join filter input missing");
	if (input.input->side == HashJoinOutputSide::LEFT)
		return probe_chunk.get_int32(input.input->input_chunk_slot, probe_row_idx);
	if (build_row == nullptr || input.right_col == nullptr)
		elog(ERROR, "pg_yaap: simple hash join filter build input missing");
	int32_t value = 0;
	std::memcpy(&value, build_row + input.right_col->offset, sizeof(value));
	return value;
}

static inline int64_t
ReadSimpleJoinFilterInt64(const SimpleJoinFilterInputRef &input,
						  const PipelineChunk &probe_chunk,
						  uint16_t probe_row_idx,
						  const uint8_t *build_row)
{
	if (input.input == nullptr)
		elog(ERROR, "pg_yaap: simple hash join filter input missing");
	if (input.input->side == HashJoinOutputSide::LEFT)
		return probe_chunk.get_int64(input.input->input_chunk_slot, probe_row_idx);
	if (build_row == nullptr || input.right_col == nullptr)
		elog(ERROR, "pg_yaap: simple hash join filter build input missing");
	int64_t value = 0;
	std::memcpy(&value, build_row + input.right_col->offset, sizeof(value));
	return value;
}

static inline bool
EvaluateSimpleJoinFilter(const SimpleJoinFilterSpec &spec,
						 const PipelineChunk &probe_chunk,
						 uint16_t probe_row_idx,
						 const uint8_t *build_row)
{
	if (!spec.valid)
		return false;
	if (spec.op == FilterStepOp::INT32_CMP_VAR)
	{
		const int32_t l = ReadSimpleJoinFilterInt32(spec.left, probe_chunk, probe_row_idx, build_row);
		const int32_t r = ReadSimpleJoinFilterInt32(spec.right, probe_chunk, probe_row_idx, build_row);
		switch (spec.cmp_op)
		{
			case QualOp::LE: return l <= r;
			case QualOp::LT: return l <  r;
			case QualOp::EQ: return l == r;
			case QualOp::GE: return l >= r;
			case QualOp::GT: return l >  r;
			case QualOp::NE: return l != r;
		}
	}
	const int64_t l = ReadSimpleJoinFilterInt64(spec.left, probe_chunk, probe_row_idx, build_row);
	const int64_t r = ReadSimpleJoinFilterInt64(spec.right, probe_chunk, probe_row_idx, build_row);
	switch (spec.cmp_op)
	{
		case QualOp::LE: return l <= r;
		case QualOp::LT: return l <  r;
		case QualOp::EQ: return l == r;
		case QualOp::GE: return l >= r;
		case QualOp::GT: return l >  r;
		case QualOp::NE: return l != r;
	}
	return false;
}

static inline bool
EvalFilterStep(const FilterStep &step,
	       const HashJoinFilterInputDesc *inputs,
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
				int64_t l = filter_chunk.get_int64(step.left_idx, 0);
				int64_t r = filter_chunk.get_int64(step.right_idx, 0);
				if (inputs != nullptr &&
					inputs[step.left_idx].decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED &&
					inputs[step.right_idx].decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
				{
					static int numeric_compare_debug_budget = 8;
					const int64_t orig_l = l;
					const int64_t orig_r = r;
					const uint8_t target_scale = std::max(inputs[step.left_idx].numeric_scale,
						inputs[step.right_idx].numeric_scale);
					if (!RescaleNumericForCompare(l, inputs[step.left_idx].numeric_scale, target_scale, l) ||
						!RescaleNumericForCompare(r, inputs[step.right_idx].numeric_scale, target_scale, r))
						elog(ERROR, "pg_yaap: hash join numeric filter rescale failed");
					if (pg_yaap_trace_hooks && numeric_compare_debug_budget-- > 0)
						elog(LOG,
							 "pg_yaap: hash join numeric compare left=%lld(scale=%u)->%lld right=%lld(scale=%u)->%lld target=%u op=%u",
							 (long long) orig_l,
							 (unsigned) inputs[step.left_idx].numeric_scale,
							 (long long) l,
							 (long long) orig_r,
							 (unsigned) inputs[step.right_idx].numeric_scale,
							 (long long) r,
							 (unsigned) target_scale,
							 (unsigned) step.cmp_op);
				}
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
			elog(ERROR, "pg_yaap: unsupported hash join filter step op %u", static_cast<unsigned>(step.op));
	}
	bool_values[step.out_bool_reg] = result ? 1 : 0;
	return result;
}

static inline void
PopulateJoinFilterChunk(const HashJoinFilterInputDesc *inputs,
			       uint16_t n_inputs,
			       const PipelineChunk &probe_chunk,
			       uint16_t probe_row_idx,
			       const TupleDataLayout *build_layout,
			       const TupleDataCollection *build_rows,
			       const uint8_t *build_row,
			       DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE> &filter_chunk)
{
	filter_chunk.reset_lightweight();
	filter_chunk.count = 1;
	for (uint16_t i = 0; i < n_inputs; ++i)
	{
		const HashJoinFilterInputDesc &input = inputs[i];
		filter_chunk.nulls[i][0] = 0;
		if (input.side == HashJoinOutputSide::LEFT)
		{
			switch (input.decode_kind)
			{
				case ColumnDecodeKind::INT32_CHAR:
				case ColumnDecodeKind::INT32_DATE:
				case ColumnDecodeKind::INT32_INT4:
					filter_chunk.int32_columns[i][0] = probe_chunk.get_int32(input.input_chunk_slot, probe_row_idx);
					break;
				case ColumnDecodeKind::INT64_INT8:
				case ColumnDecodeKind::INT64_NUMERIC_SCALED:
					if (!ReadPromotedFilterInt64FromProbe(input, probe_chunk, probe_row_idx, filter_chunk.int64_columns[i][0]))
						elog(ERROR, "pg_yaap: unsupported LEFT promoted hash join filter decode kind %u->%u",
							 static_cast<unsigned>(input.source_decode_kind),
							 static_cast<unsigned>(input.decode_kind));
					break;
				case ColumnDecodeKind::DOUBLE_FLOAT8:
					filter_chunk.double_columns[i][0] = probe_chunk.get_double(input.input_chunk_slot, probe_row_idx);
					break;
				case ColumnDecodeKind::STRING_REF:
				{
					const VecStringRef src = probe_chunk.get_string_ref(input.input_chunk_slot, probe_row_idx);
					const char *ptr = probe_chunk.get_string_ptr(input.input_chunk_slot, probe_row_idx);
					filter_chunk.string_columns[i][0] = filter_chunk.store_string_bytes(ptr, src.len);
					break;
				}
				case ColumnDecodeKind::NONE:
					elog(ERROR, "pg_yaap: invalid LEFT hash join filter decode kind NONE");
			}
			continue;
		}

		const TdcColumnDesc *right_col = FindRightColumnBySlotAndKind(build_layout,
			input.input_chunk_slot,
			input.source_decode_kind);
		if (right_col == nullptr)
			elog(ERROR, "pg_yaap: hash join filter mapping missing right payload column");
		switch (input.decode_kind)
		{
			case ColumnDecodeKind::INT32_CHAR:
			case ColumnDecodeKind::INT32_DATE:
			case ColumnDecodeKind::INT32_INT4:
				std::memcpy(&filter_chunk.int32_columns[i][0], build_row + right_col->offset, sizeof(int32_t));
				break;
			case ColumnDecodeKind::INT64_INT8:
			case ColumnDecodeKind::INT64_NUMERIC_SCALED:
				if (!ReadPromotedFilterInt64FromBuild(input, right_col, build_row, filter_chunk.int64_columns[i][0]))
					elog(ERROR, "pg_yaap: unsupported RIGHT promoted hash join filter decode kind %u->%u",
						 static_cast<unsigned>(input.source_decode_kind),
						 static_cast<unsigned>(input.decode_kind));
				break;
			case ColumnDecodeKind::DOUBLE_FLOAT8:
				std::memcpy(&filter_chunk.double_columns[i][0], build_row + right_col->offset, sizeof(double));
				break;
			case ColumnDecodeKind::STRING_REF:
			{
				VecStringRef ref;
				std::memcpy(&ref, build_row + right_col->offset, sizeof(ref));
				const char *ptr = VecStringRefDataPtr(ref,
					build_rows != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(build_rows)) : nullptr);
				filter_chunk.string_columns[i][0] = filter_chunk.store_string_bytes(ptr, ref.len);
				break;
			}
			case ColumnDecodeKind::NONE:
				elog(ERROR, "pg_yaap: invalid RIGHT hash join filter decode kind NONE");
		}
	}
}

static inline bool
EvaluateJoinFilter(const HashJoinFilterInputDesc *inputs,
			   uint16_t n_inputs,
			   const FilterExprDesc *exprs,
			   uint16_t n_exprs,
			   const FilterStep *steps,
			   uint16_t n_steps,
			   const char *string_consts,
			   uint16_t required_bool_regs,
			   const PipelineChunk &probe_chunk,
			   uint16_t probe_row_idx,
			   const TupleDataLayout *build_layout,
			   const TupleDataCollection *build_rows,
			   const uint8_t *build_row,
			   DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE> &filter_chunk,
			   uint8_t *bool_values)
{
	if (n_inputs == 0 || n_exprs == 0 || n_steps == 0)
		return true;
	PopulateJoinFilterChunk(inputs,
		n_inputs,
		probe_chunk,
		probe_row_idx,
		build_layout,
		build_rows,
		build_row,
		filter_chunk);
	if (required_bool_regs > 0)
		std::memset(bool_values, 0, required_bool_regs * sizeof(bool_values[0]));
	for (uint16_t expr_idx = 0; expr_idx < n_exprs; ++expr_idx)
	{
		const FilterExprDesc &expr = exprs[expr_idx];
		const uint16_t end_step = static_cast<uint16_t>(expr.first_step_idx + expr.n_steps);
		if (end_step > n_steps)
			elog(ERROR, "pg_yaap: hash join filter expr step range out of bounds");
		for (uint16_t step_idx = expr.first_step_idx; step_idx < end_step; ++step_idx)
			(void) EvalFilterStep(steps[step_idx], inputs, filter_chunk, string_consts, bool_values);
		if (expr.output_bool_reg >= required_bool_regs || bool_values[expr.output_bool_reg] == 0)
			return false;
	}
	return true;
}

class HashJoinOperatorState final : public OperatorState {
public:
	bool initialized = false;
	HashJoinProbeCursorState cursor;
	HashJoinFastProbeState fast_probe;
};

} // namespace

int
PhysicalHashJoin::MaxThreads(ExecCtx &ctx) const
{
	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	HashJoinSharedPayload *payload = ResolvePayload(ctx.dsa, payload_dp);
	if (payload == nullptr)
		return 1;
	return std::max(1, static_cast<int>(payload->local_state_slot_count));
}

void
PhysicalHashJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta)
{
	Assert(children().size() == 2);

	/*
	 * HashJoin is the first binary operator in this runtime. We keep the probe
	 * side streaming in the current pipeline and create a child producer
	 * pipeline for the build side whose sink is this same operator instance.
	 * Current convention is left=probe, right=build.
	 */
	Pipeline &build_pipeline = meta.CreateChildPipeline(current, *this);
	meta.SetSink(build_pipeline, *this);
	build_pipeline.source = nullptr;
	children()[1]->BuildPipelines(build_pipeline, meta);
	children()[0]->BuildPipelines(current, meta);
	meta.AddOperator(current, *this);
}

std::unique_ptr<GlobalSinkState>
PhysicalHashJoin::GetGlobalSinkState(ExecCtx &ctx)
{
	auto state = std::make_unique<HashJoinGlobalSinkState>();
	state->dsa = ctx.dsa;
	state->desc = desc_;
	state->probe_key_layout_dp = left_key_layout_dp_;
	state->build_key_layout_dp = right_key_layout_dp_;
	state->output_columns_dp = output_columns_dp_;
	state->build_layout_dp = right_payload_layout_dp_;
	state->shared_payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	if (!DsaPointerIsValid(state->probe_key_layout_dp) && desc_ != nullptr)
		state->probe_key_layout_dp = desc_->body.hash_join.left_key_layout;
	if (!DsaPointerIsValid(state->build_key_layout_dp) && desc_ != nullptr)
		state->build_key_layout_dp = desc_->body.hash_join.right_key_layout;
	if (!DsaPointerIsValid(state->build_layout_dp) && desc_ != nullptr)
		state->build_layout_dp = desc_->body.hash_join.right_payload_layout;
	if (!DsaPointerIsValid(state->output_columns_dp) && desc_ != nullptr)
		state->output_columns_dp = desc_->body.hash_join.output_columns;
	state->probe_key_layout = ResolveLayout(ctx.dsa, state->probe_key_layout_dp);
	state->build_key_layout = ResolveLayout(ctx.dsa, state->build_key_layout_dp);
	state->build_layout = ResolveLayout(ctx.dsa, state->build_layout_dp);
	state->output_column_count = output_column_count_ > 0 ? output_column_count_ :
		(desc_ != nullptr ? desc_->body.hash_join.output_column_count : 0);
	if (DsaPointerIsValid(state->output_columns_dp) && state->output_column_count > 0)
		state->output_columns = static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, state->output_columns_dp));
	if (state->probe_key_layout == nullptr)
		elog(ERROR, "pg_yaap: hash join missing probe-side key layout");
	if (state->build_key_layout == nullptr)
		elog(ERROR, "pg_yaap: hash join missing build-side key layout");
	if (state->build_layout == nullptr)
		elog(ERROR, "pg_yaap: hash join missing build-side payload layout");

	if (ctx.worker_index == LEADER_WORKER_INDEX && !DsaPointerIsValid(state->shared_payload_dp))
	{
		state->shared_payload_dp = dsa_allocate0(ctx.dsa, sizeof(HashJoinSharedPayload));
		state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);
		state->payload->local_state_slot_count = EffectiveWorkerCount(ctx);
		state->payload->build_partition_count = 1;
		state->payload->hash_table_capacity = 0;
		state->payload->radix_bits = 0;
		state->payload->combined = false;
		const HashJoinMatchMode join_mode = desc_ != nullptr ? desc_->body.hash_join.join_mode : join_mode_;
		const auto *output_columns = DsaPointerIsValid(output_columns_dp_)
			? static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, output_columns_dp_))
			: (desc_ != nullptr && DsaPointerIsValid(desc_->body.hash_join.output_columns)
				? static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, desc_->body.hash_join.output_columns))
				: nullptr);
		const uint16_t output_column_count = output_column_count_ > 0 ? output_column_count_ :
			(desc_ != nullptr ? desc_->body.hash_join.output_column_count : 0);
		const uint16_t n_filter_inputs = n_filter_inputs_ > 0 ? n_filter_inputs_ :
			(desc_ != nullptr ? desc_->body.hash_join.n_filter_inputs : 0);
		const uint16_t n_filter_exprs = n_filter_exprs_ > 0 ? n_filter_exprs_ :
			(desc_ != nullptr ? desc_->body.hash_join.n_filter_exprs : 0);
		const uint16_t n_filter_steps = n_filter_steps_ > 0 ? n_filter_steps_ :
			(desc_ != nullptr ? desc_->body.hash_join.n_filter_steps : 0);
		const bool can_share_local_payload =
			(join_mode == HashJoinMatchMode::ANTI ||
			 join_mode == HashJoinMatchMode::SEMI) &&
			!OutputColumnsReferenceRight(output_columns, output_column_count);
		const bool payload_rows_unused =
			can_share_local_payload &&
			n_filter_inputs == 0 &&
			n_filter_exprs == 0 &&
			n_filter_steps == 0;
		state->payload->build_rows_use_keys =
			LayoutsEqual(state->build_key_layout, state->build_layout) ||
			payload_rows_unused;
		state->payload->build_rows_shared_local =
			!payload_rows_unused &&
			(LayoutHasStringRef(state->build_layout) ||
			 can_share_local_payload);
		state->payload->finalized = false;
		pg_atomic_init_u32(&state->payload->release_state, 0);
		pg_atomic_init_u32(&state->payload->combine_prepare_state, 0);
		state->payload->combined_row_count = 0;
		state->payload->combined_key_heap_used = 0;
		state->payload->combined_row_heap_used = 0;
		SpinLockInit(&state->payload->mutex);
		state->payload->local_build_registry_dp = dsa_allocate0(ctx.dsa,
			static_cast<size_t>(state->payload->local_state_slot_count) * sizeof(HashJoinLocalBuildRegistryEntry));
		const uint32_t initial_rows = HashJoinInitialRows(max_rows_);
		const uint64_t per_worker_rows =
			(initial_rows + state->payload->local_state_slot_count - 1) / state->payload->local_state_slot_count;
		const uint64_t initial_global_rows = per_worker_rows * state->payload->local_state_slot_count;
		const uint32_t global_row_capacity = static_cast<uint32_t>(std::max<uint64_t>(1024u, initial_global_rows));
		const uint32_t key_heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->build_key_layout,
			global_row_capacity);
		const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->build_layout,
			global_row_capacity);
		state->payload->build_keys_dp = TupleDataCollectionAllocate(ctx.dsa,
			global_row_capacity,
			state->build_key_layout->row_width,
			key_heap_capacity);
		if (state->payload->build_rows_use_keys)
			state->payload->build_rows_dp = state->payload->build_keys_dp;
		else
			state->payload->build_rows_dp = TupleDataCollectionAllocate(ctx.dsa,
				global_row_capacity,
				state->build_layout->row_width,
				heap_capacity);
		state->build_keys = ResolveTdc(ctx.dsa, state->payload->build_keys_dp);
		state->build_rows = state->payload->build_rows_use_keys
			? state->build_keys
			: ResolveTdc(ctx.dsa, state->payload->build_rows_dp);
		TupleDataCollectionInit(state->build_keys,
			global_row_capacity,
			state->build_key_layout->row_width,
			state->build_key_layout_dp,
			key_heap_capacity);
		if (!state->payload->build_rows_use_keys)
			TupleDataCollectionInit(state->build_rows,
				global_row_capacity,
				state->build_layout->row_width,
				state->build_layout_dp,
				heap_capacity);
		StoreSharedPayloadOnDescriptor(this, state->shared_payload_dp);
	}
	else
	{
		if (!DsaPointerIsValid(state->shared_payload_dp))
			state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this);
		state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);
	}

	if (state->payload == nullptr)
		elog(ERROR, "pg_yaap: hash join shared payload not initialized");

	state->build_keys = ResolveTdc(ctx.dsa, state->payload->build_keys_dp);
	state->build_rows = state->payload->build_rows_use_keys
		? state->build_keys
		: ResolveTdc(ctx.dsa, state->payload->build_rows_dp);
	if (state->build_keys == nullptr)
		elog(ERROR, "pg_yaap: hash join global build keys not initialized");
	if (state->build_rows == nullptr)
		elog(ERROR, "pg_yaap: hash join global build rows not initialized");

	state->local_state_slot_count = state->payload->local_state_slot_count;
	state->finalized = state->payload->finalized;
	return state;
}

std::unique_ptr<LocalSinkState>
PhysicalHashJoin::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<HashJoinGlobalSinkState &>(gstate);
	auto state = std::make_unique<HashJoinLocalSinkState>();
	state->build_key_layout = global.build_key_layout;
	state->build_layout = global.build_layout;
	const uint32_t initial_rows = HashJoinInitialRows(max_rows_);
	uint32_t local_capacity = initial_rows;
	if (global.local_state_slot_count > 0)
		local_capacity = (initial_rows + global.local_state_slot_count - 1) / global.local_state_slot_count;
	local_capacity = std::max<uint32_t>(1024u, local_capacity);
	const uint32_t key_heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->build_key_layout,
		local_capacity);
	const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->build_layout,
		local_capacity);
	state->build_keys_dp = TupleDataCollectionAllocate(ctx.dsa,
		local_capacity,
		state->build_key_layout->row_width,
		key_heap_capacity);
	if (global.payload->build_rows_use_keys)
		state->build_rows_dp = state->build_keys_dp;
	else
		state->build_rows_dp = TupleDataCollectionAllocate(ctx.dsa,
			local_capacity,
			state->build_layout->row_width,
			heap_capacity);
	state->build_keys = ResolveTdc(ctx.dsa, state->build_keys_dp);
	state->build_rows = global.payload->build_rows_use_keys
		? state->build_keys
		: ResolveTdc(ctx.dsa, state->build_rows_dp);
	TupleDataCollectionInit(state->build_keys,
		local_capacity,
		state->build_key_layout->row_width,
		global.build_key_layout_dp,
		key_heap_capacity);
	if (!global.payload->build_rows_use_keys)
		TupleDataCollectionInit(state->build_rows,
			local_capacity,
			state->build_layout->row_width,
			global.build_layout_dp,
			heap_capacity);

	if (ctx.worker_index >= 0)
	{
		auto *registry = ResolveLocalBuildRegistry(ctx.dsa, global.payload);
		if (registry == nullptr || static_cast<uint32_t>(ctx.worker_index) >= global.payload->local_state_slot_count)
			elog(ERROR, "pg_yaap: hash join local build registry missing");
		registry[ctx.worker_index].build_keys_dp = state->build_keys_dp;
		registry[ctx.worker_index].build_rows_dp = state->build_rows_dp;
		registry[ctx.worker_index].row_count = 0;
		registry[ctx.worker_index].key_heap_used = 0;
		registry[ctx.worker_index].row_heap_used = 0;
		registry[ctx.worker_index].global_row_offset = 0;
		registry[ctx.worker_index].global_key_heap_offset = 0;
		registry[ctx.worker_index].global_row_heap_offset = 0;
		if (pg_yaap_trace_execution_path)
			ereport(LOG,
				(errmsg("pg_yaap hashjoin registry slot=%d keys=" UINT64_FORMAT " rows=" UINT64_FORMAT,
					ctx.worker_index,
					(uint64) state->build_keys_dp,
					(uint64) state->build_rows_dp)));
	}

	return state;
}

SinkResultType
PhysicalHashJoin::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input)
{
	auto &global = static_cast<HashJoinGlobalSinkState &>(input.global_state);
	auto &local = static_cast<HashJoinLocalSinkState &>(input.local_state);
	if (local.build_keys == nullptr || local.build_rows == nullptr ||
		local.build_key_layout == nullptr || local.build_layout == nullptr)
		elog(ERROR, "pg_yaap: hash join local build rows not initialized");

	for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
	{
		if (PipelineCancelRequestedEvery(ctx, row_idx))
			return SinkResultType::FINISHED;
		EnsureJoinLocalCapacity(ctx,
			global.payload,
			global.build_key_layout_dp,
			local.build_key_layout,
			global.build_layout_dp,
			local.build_layout,
			local,
			in,
			row_idx);
		uint8_t *key_row_ptr = nullptr;
		uint8_t *row_ptr = nullptr;
		const uint32_t key_appended = TupleDataCollectionAppendRow(local.build_keys, &key_row_ptr);
		const uint32_t appended = global.payload->build_rows_use_keys
			? key_appended
			: TupleDataCollectionAppendRow(local.build_rows, &row_ptr);
		if (key_appended == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: hash join local build key row capacity exceeded");
		if (appended == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: hash join local build row capacity exceeded");
		if (global.payload->build_rows_use_keys)
			row_ptr = key_row_ptr;
		ScatterGroupOnly(local.build_key_layout, local.build_keys, key_row_ptr, in, row_idx);
		if (!global.payload->build_rows_use_keys)
			ScatterGroupOnly(local.build_layout, local.build_rows, row_ptr, in, row_idx);
	}

	(void) ctx;
	local.build_input_rows += in.count;
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
PhysicalHashJoin::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input)
{
	auto &global = static_cast<HashJoinGlobalSinkState &>(input.global_state);
	if (input.local_state == nullptr)
		elog(ERROR, "pg_yaap: hash join combine local state missing");
	auto &local = static_cast<HashJoinLocalSinkState &>(*input.local_state);
	if (local.build_keys == nullptr || local.build_rows == nullptr)
		elog(ERROR, "pg_yaap: hash join combine local build rows missing");

	if (global.payload != nullptr && global.payload->build_rows_shared_local)
		return ExecuteHashJoinSharedPayloadCombine(ctx, global, local);

	if (LayoutHasStringRef(global.build_key_layout))
	{
		const uint32_t local_row_count = pg_atomic_read_u32(&local.build_keys->row_count);
		if (local_row_count != pg_atomic_read_u32(&local.build_rows->row_count))
			elog(ERROR, "pg_yaap: hash join local build key/payload row counts diverged");
		uint32_t row_idx = 0;
		while (row_idx < local_row_count)
		{
			if (PipelineCancelRequested(ctx))
				return SinkCombineResultType::FINISHED;
			const uint32_t batch_end = std::min(row_idx + 64u, local_row_count);
			SpinLockAcquire(&global.payload->mutex);
			for (; row_idx < batch_end; ++row_idx)
			{
				const uint8_t *src_key_row = TupleDataCollectionGetRowConst(local.build_keys, row_idx);
				const uint8_t *src_row = TupleDataCollectionGetRowConst(local.build_rows, row_idx);
				EnsureJoinGlobalCapacity(ctx,
					global,
					src_key_row,
					local.build_keys,
					src_row,
					local.build_rows);
				uint8_t *dst_key_row = nullptr;
				uint8_t *dst_row = nullptr;
				const uint32_t key_appended = TupleDataCollectionAppendRow(global.build_keys, &dst_key_row);
				const uint32_t appended = TupleDataCollectionAppendRow(global.build_rows, &dst_row);
				if (key_appended == TDC_INVALID_ROW_INDEX)
					elog(ERROR, "pg_yaap: hash join global build key row capacity exceeded during combine");
				if (appended == TDC_INVALID_ROW_INDEX)
					elog(ERROR, "pg_yaap: hash join global build row capacity exceeded during combine");
				CopyBuildRow(global.build_key_layout, local.build_keys, src_key_row, global.build_keys, dst_key_row);
				CopyBuildRow(global.build_layout, local.build_rows, src_row, global.build_rows, dst_row);
			}
			SpinLockRelease(&global.payload->mutex);
		}

		global.payload->combined = true;
		return SinkCombineResultType::FINISHED;
	}

	return ExecuteHashJoinCombine(ctx, global, local);
}

SinkFinalizeType
PhysicalHashJoin::Finalize(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<HashJoinGlobalSinkState &>(gstate);
	if (global.payload != nullptr)
	{
		PublishHashJoinCombinedRows(global);
		global.build_keys = ResolveTdc(ctx.dsa, global.payload->build_keys_dp);
		global.build_rows = ResolveTdc(ctx.dsa, global.payload->build_rows_dp);
	}
	if (global.build_keys == nullptr ||
		(!global.payload->build_rows_shared_local && global.build_rows == nullptr))
		elog(ERROR, "pg_yaap: hash join finalize missing global build rows");
	const uint32_t row_count = pg_atomic_read_u32(&global.build_keys->row_count);
	if (!global.payload->build_rows_shared_local &&
		!global.payload->build_rows_use_keys &&
		row_count != pg_atomic_read_u32(&global.build_rows->row_count))
		elog(ERROR, "pg_yaap: hash join finalize key/payload row counts diverged");
	if (pg_yaap_trace_execution_path)
		ereport(LOG,
			(errmsg("pg_yaap hashjoin finalize build_rows=%u key_width=%u payload_width=%u hash_capacity_pre=%u",
				row_count,
				global.build_key_layout != nullptr ? global.build_key_layout->row_width : 0,
				global.build_layout != nullptr ? global.build_layout->row_width : 0,
				row_count > 0 ? 1u : 0u)));
	if (row_count > 0)
	{
		uint32_t hash_capacity = global.payload->hash_table_capacity;
		if (!DsaPointerIsValid(global.payload->hash_table_dp) ||
			!DsaPointerIsValid(global.payload->hash_links_dp) ||
			!DsaPointerIsValid(global.payload->hash_salts_dp))
		{
			hash_capacity = global.payload->hash_table_capacity;
			if (hash_capacity == 0)
			{
				hash_capacity = 1;
				while (hash_capacity < row_count * 2u)
				{
					if (PipelineCancelRequested(ctx))
						return SinkFinalizeType::READY;
					hash_capacity <<= 1;
				}
			}
			global.payload->hash_table_capacity = hash_capacity;
			global.payload->hash_table_dp = dsa_allocate(ctx.dsa,
				static_cast<size_t>(hash_capacity) * sizeof(uint32_t));
			global.payload->hash_links_dp = dsa_allocate(ctx.dsa,
				static_cast<size_t>(row_count) * sizeof(uint32_t));
			global.payload->hash_salts_dp = dsa_allocate(ctx.dsa,
				static_cast<size_t>(row_count) * sizeof(uint16_t));
			auto *bucket_heads = static_cast<uint32_t *>(dsa_get_address(ctx.dsa, global.payload->hash_table_dp));
			auto *buckets = DsaPointerIsValid(global.payload->hash_buckets_dp)
				? static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, global.payload->hash_buckets_dp))
				: nullptr;
			auto *links = static_cast<uint32_t *>(dsa_get_address(ctx.dsa, global.payload->hash_links_dp));
			auto *salts = static_cast<uint16_t *>(dsa_get_address(ctx.dsa, global.payload->hash_salts_dp));
			std::memset(bucket_heads, 0xFF, static_cast<size_t>(hash_capacity) * sizeof(uint32_t));

			for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
			{
				if (PipelineCancelRequestedEvery(ctx, row_idx, 1023u))
					return SinkFinalizeType::READY;
				uint32_t bucket = 0;
				if (buckets != nullptr)
				{
					bucket = buckets[row_idx];
				}
				else
				{
					const uint8_t *row_ptr = TupleDataCollectionGetRowConst(global.build_keys, row_idx);
					uint64_t hash = 0;
					if (!TryHashSingleJoinKeyRow(global.build_key_layout, global.build_keys, row_ptr, hash))
						hash = HashGroupRow(global.build_key_layout, global.build_keys, row_ptr);
					salts[row_idx] = HashJoinSalt(hash);
					bucket = static_cast<uint32_t>(hash) & (hash_capacity - 1u);
				}
				links[row_idx] = bucket_heads[bucket];
				bucket_heads[bucket] = row_idx;
			}
		}
	}
	else
	{
		global.payload->hash_table_capacity = 0;
		global.payload->hash_table_dp = InvalidDsaPointer;
		global.payload->hash_buckets_dp = InvalidDsaPointer;
		global.payload->hash_links_dp = InvalidDsaPointer;
		global.payload->hash_salts_dp = InvalidDsaPointer;
	}
	TupleDataCollectionResetScan(global.build_keys);
	if (global.build_rows != nullptr)
		TupleDataCollectionResetScan(global.build_rows);
	global.build_keys->finalized = true;
	if (global.build_rows != nullptr)
		global.build_rows->finalized = true;
	global.payload->finalized = true;
	global.finalized = true;
	return SinkFinalizeType::READY;
}

std::unique_ptr<OperatorState>
PhysicalHashJoin::GetOperatorState(ExecCtx &ctx)
{
	(void) ctx;
	return std::make_unique<HashJoinOperatorState>();
}

void
PhysicalHashJoin::ReleaseBuildPayloadAfterConsumerRun(ExecCtx &ctx)
{
	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	HashJoinSharedPayload *payload = ResolvePayload(ctx.dsa, payload_dp);
	if (payload == nullptr)
		return;

	uint32 expected = 0;
	if (!pg_atomic_compare_exchange_u32(&payload->release_state, &expected, 1))
		return;

	const uint64_t global_keys_bytes = EstimateTdcAllocBytes(ctx.dsa, payload->build_keys_dp);
	const uint64_t global_rows_bytes = EstimateTdcAllocBytes(ctx.dsa, payload->build_rows_dp);
	uint64_t local_keys_bytes = 0;
	uint64_t local_rows_bytes = 0;
	auto *registry = ResolveLocalBuildRegistry(ctx.dsa, payload);
	if (registry != nullptr)
	{
		for (uint32_t i = 0; i < payload->local_state_slot_count; ++i)
		{
			local_keys_bytes += EstimateTdcAllocBytes(ctx.dsa, registry[i].build_keys_dp);
			local_rows_bytes += EstimateTdcAllocBytes(ctx.dsa, registry[i].build_rows_dp);
		}
	}
	const uint64_t hash_bytes = DsaPointerIsValid(payload->hash_table_dp)
		? static_cast<uint64_t>(payload->hash_table_capacity) * sizeof(uint32_t)
		: 0;
	TupleDataCollection *build_keys = ResolveTdc(ctx.dsa, payload->build_keys_dp);
	const uint64_t bucket_bytes = DsaPointerIsValid(payload->hash_buckets_dp) && build_keys != nullptr
		? static_cast<uint64_t>(pg_atomic_read_u32(&build_keys->row_count)) * sizeof(uint32_t)
		: 0;
	const uint64_t link_bytes = DsaPointerIsValid(payload->hash_links_dp) && build_keys != nullptr
		? static_cast<uint64_t>(pg_atomic_read_u32(&build_keys->row_count)) * sizeof(uint32_t)
		: 0;
	const uint64_t salt_bytes = DsaPointerIsValid(payload->hash_salts_dp) && build_keys != nullptr
		? static_cast<uint64_t>(pg_atomic_read_u32(&build_keys->row_count)) * sizeof(uint16_t)
		: 0;
	(void) salt_bytes;
	(void) bucket_bytes;

	FreeDsaPointerIfValid(ctx.dsa, &payload->hash_table_dp);
	FreeDsaPointerIfValid(ctx.dsa, &payload->hash_buckets_dp);
	FreeDsaPointerIfValid(ctx.dsa, &payload->hash_links_dp);
	FreeDsaPointerIfValid(ctx.dsa, &payload->hash_salts_dp);
	FreeDsaPointerIfValid(ctx.dsa, &payload->build_row_slots_dp);
	FreeDsaPointerIfValid(ctx.dsa, &payload->build_row_local_idxs_dp);
	FreeDsaPointerIfValid(ctx.dsa, &payload->build_keys_dp);
	if (!payload->build_rows_use_keys)
		FreeDsaPointerIfValid(ctx.dsa, &payload->build_rows_dp);
	if (registry != nullptr)
	{
		for (uint32_t i = 0; i < payload->local_state_slot_count; ++i)
		{
			FreeDsaPointerIfValid(ctx.dsa, &registry[i].build_keys_dp);
			if (!payload->build_rows_use_keys)
				FreeDsaPointerIfValid(ctx.dsa, &registry[i].build_rows_dp);
		}
	}
	FreeDsaPointerIfValid(ctx.dsa, &payload->local_build_registry_dp);

	if (pg_yaap_trace_execution_path)
		ereport(LOG,
			(errmsg("pg_yaap hashjoin release op=%p global_keys=" UINT64_FORMAT " global_rows=" UINT64_FORMAT " local_keys=" UINT64_FORMAT " local_rows=" UINT64_FORMAT " hash=" UINT64_FORMAT " links=" UINT64_FORMAT,
				static_cast<void *>(this),
				(uint64) global_keys_bytes,
				(uint64) global_rows_bytes,
				(uint64) local_keys_bytes,
				(uint64) local_rows_bytes,
				(uint64) hash_bytes,
				(uint64) link_bytes)));

	ClearSharedPayloadOnDescriptor(this);
	pg_atomic_write_u32(&payload->release_state, 2);
	dsa_free(ctx.dsa, payload_dp);
}

OperatorResultType
PhysicalHashJoin::Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state)
{
	auto &op_state = static_cast<HashJoinOperatorState &>(state);
	if (pg_yaap_trace_execution_path)
		elog(LOG,
			 "pg_yaap hashjoin enter op=%p in_count=%u current_drained=%d join_mode=%u",
			 static_cast<void *>(this),
			 in.count,
			 op_state.cursor.current_input_drained ? 1 : 0,
			 static_cast<unsigned>(desc_ != nullptr ? desc_->body.hash_join.join_mode : join_mode_));
	if (op_state.cursor.current_input_drained)
	{
		op_state.cursor.current_input_drained = false;
		out.reset();
		return OperatorResultType::NEED_MORE_INPUT;
	}
	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	HashJoinSharedPayload *payload = ResolvePayload(ctx.dsa, payload_dp);
	if (payload == nullptr || !payload->finalized)
		elog(ERROR, "pg_yaap: hash join probe ran before build/finalize completed");
	if (pg_atomic_read_u32(&payload->release_state) != 0)
		elog(ERROR, "pg_yaap: hash join payload used after release");
	if (pg_yaap_trace_execution_path)
		elog(LOG,
			 "pg_yaap hashjoin payload ready op=%p build_keys_dp=%llu build_rows_dp=%llu finalized=%d",
			 static_cast<void *>(this),
			 static_cast<unsigned long long>(payload->build_keys_dp),
			 static_cast<unsigned long long>(payload->build_rows_dp),
			 payload->finalized ? 1 : 0);
	TupleDataCollection *build_keys = ResolveTdc(ctx.dsa, payload->build_keys_dp);
	const bool shared_local_payload = payload->build_rows_shared_local;
	TupleDataCollection *build_rows = shared_local_payload ? nullptr : ResolveTdc(ctx.dsa, payload->build_rows_dp);
	if (build_keys == nullptr || (!shared_local_payload && build_rows == nullptr))
		elog(ERROR, "pg_yaap: hash join probe missing finalized build-side rows");
	const HashJoinLocalBuildRegistryEntry *shared_local_registry = nullptr;
	const uint32_t *shared_local_slots = nullptr;
	const uint32_t *shared_local_idxs = nullptr;
	if (shared_local_payload)
	{
		shared_local_registry = ResolveLocalBuildRegistry(ctx.dsa, payload);
		if (shared_local_registry == nullptr ||
			!DsaPointerIsValid(payload->build_row_slots_dp) ||
			!DsaPointerIsValid(payload->build_row_local_idxs_dp))
			elog(ERROR, "pg_yaap: shared-local hash join payload mappings missing");
		shared_local_slots = static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, payload->build_row_slots_dp));
		shared_local_idxs = static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, payload->build_row_local_idxs_dp));
		if (shared_local_slots == nullptr || shared_local_idxs == nullptr)
			elog(ERROR, "pg_yaap: shared-local hash join payload mappings unresolved");
	}
	const TupleDataLayout *probe_layout = ResolveLayout(ctx.dsa,
		DsaPointerIsValid(left_key_layout_dp_) ? left_key_layout_dp_ :
		(desc_ != nullptr ? desc_->body.hash_join.left_key_layout : InvalidDsaPointer));
	const TupleDataLayout *build_key_layout = ResolveLayout(ctx.dsa,
		DsaPointerIsValid(right_key_layout_dp_) ? right_key_layout_dp_ :
		(desc_ != nullptr ? desc_->body.hash_join.right_key_layout : InvalidDsaPointer));
	const TupleDataLayout *build_row_layout = ResolveLayout(ctx.dsa,
		DsaPointerIsValid(right_payload_layout_dp_) ? right_payload_layout_dp_ :
		(desc_ != nullptr ? desc_->body.hash_join.right_payload_layout : InvalidDsaPointer));
	if (probe_layout == nullptr || build_key_layout == nullptr || build_row_layout == nullptr)
		elog(ERROR, "pg_yaap: hash join probe missing payload layouts");
	const auto *output_columns = DsaPointerIsValid(output_columns_dp_)
		? static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, output_columns_dp_))
		: (desc_ != nullptr && DsaPointerIsValid(desc_->body.hash_join.output_columns)
			? static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, desc_->body.hash_join.output_columns))
			: nullptr);
	const auto *filter_inputs = DsaPointerIsValid(filter_inputs_dp_)
		? ResolveFilterInputs(ctx.dsa, filter_inputs_dp_)
		: (desc_ != nullptr ? ResolveFilterInputs(ctx.dsa, desc_->body.hash_join.filter_inputs) : nullptr);
	const auto *filter_exprs = DsaPointerIsValid(filter_exprs_dp_)
		? ResolveFilterExprs(ctx.dsa, filter_exprs_dp_)
		: (desc_ != nullptr ? ResolveFilterExprs(ctx.dsa, desc_->body.hash_join.filter_exprs) : nullptr);
	const auto *filter_steps = DsaPointerIsValid(filter_steps_dp_)
		? ResolveFilterSteps(ctx.dsa, filter_steps_dp_)
		: (desc_ != nullptr ? ResolveFilterSteps(ctx.dsa, desc_->body.hash_join.filter_steps) : nullptr);
	const char *filter_string_consts = DsaPointerIsValid(filter_string_consts_dp_)
		? static_cast<const char *>(dsa_get_address(ctx.dsa, filter_string_consts_dp_))
		: (desc_ != nullptr && DsaPointerIsValid(desc_->body.hash_join.filter_string_consts)
			? static_cast<const char *>(dsa_get_address(ctx.dsa, desc_->body.hash_join.filter_string_consts))
			: nullptr);
	uint16_t output_column_count = output_column_count_ > 0 ? output_column_count_ :
		(desc_ != nullptr ? desc_->body.hash_join.output_column_count : 0);
	const uint16_t n_filter_inputs = n_filter_inputs_ > 0 ? n_filter_inputs_ :
		(desc_ != nullptr ? desc_->body.hash_join.n_filter_inputs : 0);
	const uint16_t n_filter_exprs = n_filter_exprs_ > 0 ? n_filter_exprs_ :
		(desc_ != nullptr ? desc_->body.hash_join.n_filter_exprs : 0);
	const uint16_t n_filter_steps = n_filter_steps_ > 0 ? n_filter_steps_ :
		(desc_ != nullptr ? desc_->body.hash_join.n_filter_steps : 0);
	const HashJoinMatchMode join_mode = desc_ != nullptr ? desc_->body.hash_join.join_mode : join_mode_;
	if (output_columns == nullptr || output_column_count == 0)
		elog(ERROR, "pg_yaap: hash join probe missing output column mapping");
	if ((n_filter_inputs > 0 && filter_inputs == nullptr) ||
		(n_filter_exprs > 0 && filter_exprs == nullptr) ||
		(n_filter_steps > 0 && filter_steps == nullptr))
		elog(ERROR, "pg_yaap: hash join probe missing residual join filter metadata");
	if (!DsaPointerIsValid(payload->hash_table_dp) ||
		!DsaPointerIsValid(payload->hash_links_dp) ||
		!DsaPointerIsValid(payload->hash_salts_dp))
		return OperatorResultType::NEED_MORE_INPUT;

	auto *bucket_heads = static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, payload->hash_table_dp));
	auto *links = static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, payload->hash_links_dp));
	auto *salts = static_cast<const uint16_t *>(dsa_get_address(ctx.dsa, payload->hash_salts_dp));
	InitializeHashJoinFastProbeState(op_state.fast_probe,
		output_columns,
		output_column_count,
		build_row_layout,
		probe_layout,
		build_key_layout,
		join_mode,
		n_filter_inputs,
		n_filter_exprs,
		n_filter_steps);
	uint16_t matched_probe_rows[PIPELINE_DEFAULT_CHUNK_SIZE];
	const uint8_t *matched_build_rows[PIPELINE_DEFAULT_CHUNK_SIZE];
	const TupleDataCollection *matched_build_row_tdcs[PIPELINE_DEFAULT_CHUNK_SIZE];
	std::unique_ptr<DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>> filter_chunk_holder;
	uint8_t filter_bool_values[FILTER_MAX_BOOL_REGS];
	const bool has_join_filter = n_filter_inputs > 0 && n_filter_exprs > 0 && n_filter_steps > 0;
	SimpleJoinFilterSpec simple_join_filter;
	const bool has_simple_join_filter =
		has_join_filter &&
		TryBuildSimpleJoinFilterSpec(filter_inputs,
			n_filter_inputs,
			filter_exprs,
			n_filter_exprs,
			filter_steps,
			n_filter_steps,
			build_row_layout,
			simple_join_filter);
	const uint16_t required_bool_regs =
		(has_join_filter && !has_simple_join_filter && filter_exprs != nullptr) ?
			RequiredFilterBoolRegs(filter_exprs, n_filter_exprs) : 0;
	const bool needs_build_row =
		has_join_filter || OutputColumnsReferenceRight(output_columns, output_column_count);
	if (has_join_filter && !has_simple_join_filter)
		filter_chunk_holder = std::make_unique<DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>>();
	out.reset();
	uint32_t matched_rows = 0;

	if (!shared_local_payload &&
		TryFastInnerJoinProbe(ctx,
			op_state.fast_probe,
			payload->hash_table_capacity,
			bucket_heads,
			links,
			salts,
			build_keys,
			build_rows,
			in,
			out,
			op_state.cursor,
			matched_rows))
	{
		op_state.initialized = true;
		return out.count > 0 ? OperatorResultType::HAVE_MORE_OUTPUT : OperatorResultType::NEED_MORE_INPUT;
	}

	if (!op_state.cursor.active_probe)
	{
		op_state.cursor.active_probe = true;
		op_state.cursor.probe_row_idx = 0;
		op_state.cursor.build_row_idx = HASH_JOIN_INVALID_ROW;
		op_state.cursor.have_build_cursor = false;
		op_state.cursor.probe_salt = 0;
		op_state.cursor.probe_matched = false;
	}

	while (op_state.cursor.probe_row_idx < in.count)
	{
		uint16_t batch_count = 0;
		if (PipelineCancelRequestedEvery(ctx, op_state.cursor.probe_row_idx))
			break;
		if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
			break;
		if (!op_state.cursor.have_build_cursor)
		{
			uint64_t hash = 0;
			if (!TryHashSingleJoinKeyChunkRow(probe_layout, in, op_state.cursor.probe_row_idx, hash))
				hash = HashGroup(probe_layout, in, op_state.cursor.probe_row_idx);
			op_state.cursor.probe_salt = HashJoinSalt(hash);
			const uint32_t bucket = static_cast<uint32_t>(hash) & (payload->hash_table_capacity - 1u);
			op_state.cursor.build_row_idx = bucket_heads[bucket];
			op_state.cursor.have_build_cursor = true;
			op_state.cursor.probe_matched = false;
			while (op_state.cursor.build_row_idx != HASH_JOIN_INVALID_ROW &&
				salts[op_state.cursor.build_row_idx] != op_state.cursor.probe_salt)
				op_state.cursor.build_row_idx = links[op_state.cursor.build_row_idx];
		}
		while (op_state.cursor.build_row_idx != HASH_JOIN_INVALID_ROW)
		{
			const uint32_t build_row_idx = op_state.cursor.build_row_idx;
			op_state.cursor.build_row_idx = links[build_row_idx];
			while (op_state.cursor.build_row_idx != HASH_JOIN_INVALID_ROW &&
				salts[op_state.cursor.build_row_idx] != op_state.cursor.probe_salt)
				op_state.cursor.build_row_idx = links[op_state.cursor.build_row_idx];
			const uint8_t *build_key_row = TupleDataCollectionGetRowConst(build_keys, build_row_idx);
			if (MatchGroupLayouts(build_key_layout, build_keys, build_key_row, probe_layout, in, op_state.cursor.probe_row_idx))
			{
				const TupleDataCollection *build_row_tdc = nullptr;
				const uint8_t *build_row = nullptr;
				if (needs_build_row)
				{
					build_row_tdc = build_rows;
					build_row = shared_local_payload
						? ResolveSharedLocalBuildRowCached(ctx.dsa,
							payload,
							shared_local_registry,
							shared_local_slots,
							shared_local_idxs,
							build_row_idx,
							&build_row_tdc)
						: TupleDataCollectionGetRowConst(build_rows, build_row_idx);
				}
				if (has_join_filter)
				{
					if (has_simple_join_filter)
					{
						if (!EvaluateSimpleJoinFilter(simple_join_filter,
								in,
								op_state.cursor.probe_row_idx,
								build_row))
							continue;
					}
					else if (!EvaluateJoinFilter(filter_inputs,
							 n_filter_inputs,
							 filter_exprs,
							 n_filter_exprs,
							 filter_steps,
							 n_filter_steps,
							 filter_string_consts,
							 required_bool_regs,
							 in,
							 op_state.cursor.probe_row_idx,
							 build_row_layout,
							 build_row_tdc,
							 build_row,
							 *filter_chunk_holder,
							 filter_bool_values))
					{
						continue;
					}
				}
				if (join_mode == HashJoinMatchMode::ANTI)
				{
					op_state.cursor.probe_matched = true;
					batch_count = 0;
					op_state.cursor.build_row_idx = HASH_JOIN_INVALID_ROW;
					break;
				}
				op_state.cursor.probe_matched = true;
				++matched_rows;
				matched_probe_rows[batch_count] = op_state.cursor.probe_row_idx;
				matched_build_rows[batch_count] = build_row;
				matched_build_row_tdcs[batch_count] = build_row_tdc;
				++batch_count;
				if (join_mode == HashJoinMatchMode::SEMI)
				{
					op_state.cursor.build_row_idx = HASH_JOIN_INVALID_ROW;
					break;
				}
				if (out.count + batch_count >= PIPELINE_DEFAULT_CHUNK_SIZE)
					break;
			}
		}
		const bool probe_complete = (op_state.cursor.build_row_idx == HASH_JOIN_INVALID_ROW);
		if ((join_mode == HashJoinMatchMode::ANTI || join_mode == HashJoinMatchMode::LEFT) &&
			probe_complete && batch_count == 0 && !op_state.cursor.probe_matched)
		{
			matched_probe_rows[0] = op_state.cursor.probe_row_idx;
			matched_build_rows[0] = nullptr;
			matched_build_row_tdcs[0] = nullptr;
			batch_count = 1;
		}
		if (batch_count > 0)
		{
			CopyRowsByResolvedMappingBatch(in,
				matched_probe_rows,
				matched_build_row_tdcs,
				matched_build_rows,
				op_state.fast_probe.resolved_output_columns.data(),
				op_state.fast_probe.output_column_count,
				out,
				out.count,
				batch_count);
			out.count += batch_count;
		}
		if (probe_complete)
		{
			matched_rows = 0;
			op_state.cursor.probe_matched = false;
			op_state.cursor.have_build_cursor = false;
			++op_state.cursor.probe_row_idx;
			if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
				break;
			continue;
		}
		if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
			break;
	}
	if (op_state.cursor.probe_row_idx >= in.count)
	{
		op_state.cursor.active_probe = false;
		op_state.cursor.current_input_drained = out.count > 0;
		op_state.cursor.build_row_idx = HASH_JOIN_INVALID_ROW;
		op_state.cursor.have_build_cursor = false;
		op_state.cursor.probe_salt = 0;
		op_state.cursor.probe_matched = false;
	}
	if (out.count > 0 && pg_yaap_trace_execution_path)
		ereport(LOG,
			(errmsg("pg_yaap hashjoin execute worker=%d probe_rows=%u out_rows=%u matched_rows=%u build_rows=%u key_width=%u payload_width=%u",
				ctx.worker_index,
				in.count,
				out.count,
				matched_rows,
				pg_atomic_read_u32(&build_keys->row_count),
				build_key_layout->row_width,
				build_row_layout->row_width)));
	op_state.initialized = true;

	return out.count > 0 ? OperatorResultType::HAVE_MORE_OUTPUT : OperatorResultType::NEED_MORE_INPUT;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
