#include "parallel/pipeline/physical_hash_join_fast_probe.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <cstring>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/cancel.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

static inline uint16_t
HashJoinSalt(uint64_t hash)
{
	return static_cast<uint16_t>(hash >> 48);
}

static inline bool
ColumnDecodeKindToTdcKind(ColumnDecodeKind decode_kind, TdcColumnKind &out_kind)
{
	switch (decode_kind)
	{
		case ColumnDecodeKind::INT32_CHAR:
		case ColumnDecodeKind::INT32_DATE:
		case ColumnDecodeKind::INT32_INT4:
			out_kind = TdcColumnKind::INT32;
			return true;
		case ColumnDecodeKind::INT64_INT8:
		case ColumnDecodeKind::INT64_NUMERIC_SCALED:
			out_kind = TdcColumnKind::INT64;
			return true;
		case ColumnDecodeKind::DOUBLE_FLOAT8:
			out_kind = TdcColumnKind::DOUBLE;
			return true;
		case ColumnDecodeKind::STRING_REF:
			out_kind = TdcColumnKind::STRING_REF;
			return true;
		case ColumnDecodeKind::NONE:
			return false;
	}
	return false;
}

static inline const TdcColumnDesc *
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

static inline void
CopyResolvedJoinRow(const PipelineChunk &left_chunk,
		    uint16_t left_row_idx,
		    const TupleDataCollection *right_tdc,
		    const uint8_t *right_row,
		    const HashJoinResolvedOutputColumn *resolved_columns,
		    uint16_t output_column_count,
		    PipelineChunk &out,
		    uint16_t out_row_idx)
{
	for (uint16_t i = 0; i < output_column_count; ++i)
	{
		const HashJoinResolvedOutputColumn &desc = resolved_columns[i];
		switch (desc.decode_kind)
		{
			case ColumnDecodeKind::INT32_CHAR:
			case ColumnDecodeKind::INT32_DATE:
			case ColumnDecodeKind::INT32_INT4:
				if (desc.side == HashJoinOutputSide::LEFT)
				{
					out.int32_columns[desc.output_chunk_slot][out_row_idx] =
						left_chunk.get_int32(desc.input_chunk_slot, left_row_idx);
					out.nulls[desc.output_chunk_slot][out_row_idx] =
						left_chunk.nulls[desc.input_chunk_slot][left_row_idx];
				}
				else if (right_row == nullptr)
				{
					out.int32_columns[desc.output_chunk_slot][out_row_idx] = 0;
					out.nulls[desc.output_chunk_slot][out_row_idx] = 1;
				}
				else
				{
					int32_t value;
					std::memcpy(&value, right_row + desc.right_col->offset, sizeof(value));
					out.int32_columns[desc.output_chunk_slot][out_row_idx] = value;
					out.nulls[desc.output_chunk_slot][out_row_idx] = 0;
				}
				break;
			case ColumnDecodeKind::INT64_INT8:
			case ColumnDecodeKind::INT64_NUMERIC_SCALED:
				if (desc.side == HashJoinOutputSide::LEFT)
				{
					out.int64_columns[desc.output_chunk_slot][out_row_idx] =
						left_chunk.get_int64(desc.input_chunk_slot, left_row_idx);
					out.nulls[desc.output_chunk_slot][out_row_idx] =
						left_chunk.nulls[desc.input_chunk_slot][left_row_idx];
				}
				else if (right_row == nullptr)
				{
					out.int64_columns[desc.output_chunk_slot][out_row_idx] = 0;
					out.nulls[desc.output_chunk_slot][out_row_idx] = 1;
				}
				else
				{
					int64_t value;
					std::memcpy(&value, right_row + desc.right_col->offset, sizeof(value));
					out.int64_columns[desc.output_chunk_slot][out_row_idx] = value;
					out.nulls[desc.output_chunk_slot][out_row_idx] = 0;
				}
				break;
			case ColumnDecodeKind::DOUBLE_FLOAT8:
				if (desc.side == HashJoinOutputSide::LEFT)
				{
					out.double_columns[desc.output_chunk_slot][out_row_idx] =
						left_chunk.get_double(desc.input_chunk_slot, left_row_idx);
					out.nulls[desc.output_chunk_slot][out_row_idx] =
						left_chunk.nulls[desc.input_chunk_slot][left_row_idx];
				}
				else if (right_row == nullptr)
				{
					out.double_columns[desc.output_chunk_slot][out_row_idx] = 0;
					out.nulls[desc.output_chunk_slot][out_row_idx] = 1;
				}
				else
				{
					double value;
					std::memcpy(&value, right_row + desc.right_col->offset, sizeof(value));
					out.double_columns[desc.output_chunk_slot][out_row_idx] = value;
					out.nulls[desc.output_chunk_slot][out_row_idx] = 0;
				}
				break;
			case ColumnDecodeKind::STRING_REF:
				if (desc.side == HashJoinOutputSide::LEFT)
				{
					const VecStringRef src = left_chunk.get_string_ref(desc.input_chunk_slot, left_row_idx);
					const char *ptr = left_chunk.get_string_ptr(desc.input_chunk_slot, left_row_idx);
					out.string_columns[desc.output_chunk_slot][out_row_idx] =
						out.store_string_bytes(ptr, src.len);
					out.nulls[desc.output_chunk_slot][out_row_idx] =
						left_chunk.nulls[desc.input_chunk_slot][left_row_idx];
				}
				else if (right_row == nullptr)
				{
					out.nulls[desc.output_chunk_slot][out_row_idx] = 1;
					out.string_columns[desc.output_chunk_slot][out_row_idx] = VecStringRef{0, 0, 0};
				}
				else
				{
					VecStringRef ref;
					std::memcpy(&ref, right_row + desc.right_col->offset, sizeof(ref));
					const char *ptr = VecStringRefDataPtr(ref,
						right_tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(right_tdc)) : nullptr);
					if (ptr == nullptr && ref.len != 0)
						elog(ERROR, "pg_yaap: hash join output string missing backing storage");
					out.string_columns[desc.output_chunk_slot][out_row_idx] =
						out.store_string_bytes(ptr, ref.len);
					out.nulls[desc.output_chunk_slot][out_row_idx] = 0;
				}
				break;
			case ColumnDecodeKind::NONE:
				out.nulls[desc.output_chunk_slot][out_row_idx] = 1;
				break;
		}
	}
}

}  // namespace

void
InitializeHashJoinFastProbeState(HashJoinFastProbeState &state,
				 const HashJoinOutputColumnDesc *output_columns,
				 uint16_t output_column_count,
				 const TupleDataLayout *build_row_layout,
				 const TupleDataLayout *probe_layout,
				 const TupleDataLayout *build_key_layout,
				 HashJoinMatchMode join_mode,
				 uint16_t n_filter_inputs,
				 uint16_t n_filter_exprs,
				 uint16_t n_filter_steps)
{
	if (state.initialized)
		return;
	state.initialized = true;
	state.output_column_count = output_column_count;
	state.resolved_output_columns.clear();
	state.resolved_output_columns.resize(output_column_count);
	bool has_none_output = false;
	for (uint16_t i = 0; i < output_column_count; ++i)
	{
		state.resolved_output_columns[i].side = output_columns[i].side;
		state.resolved_output_columns[i].input_chunk_slot = output_columns[i].input_chunk_slot;
		state.resolved_output_columns[i].decode_kind = output_columns[i].decode_kind;
		state.resolved_output_columns[i].output_chunk_slot = output_columns[i].output_chunk_slot;
		if (output_columns[i].decode_kind == ColumnDecodeKind::NONE)
		{
			has_none_output = true;
			continue;
		}
		if (output_columns[i].side == HashJoinOutputSide::RIGHT)
		{
			state.resolved_output_columns[i].right_col =
				FindRightColumnBySlotAndKind(build_row_layout,
					output_columns[i].input_chunk_slot,
					output_columns[i].decode_kind);
			if (state.resolved_output_columns[i].right_col == nullptr)
				elog(ERROR, "pg_yaap: hash join output column mapping missing right payload column");
		}
	}

	if (join_mode != HashJoinMatchMode::INNER ||
	    has_none_output ||
	    n_filter_inputs != 0 || n_filter_exprs != 0 || n_filter_steps != 0 ||
	    probe_layout == nullptr || build_key_layout == nullptr ||
	    probe_layout->column_count != 1 || build_key_layout->column_count != 1)
		return;

	const TdcColumnDesc &probe_col = probe_layout->columns[0];
	const TdcColumnDesc &build_col = build_key_layout->columns[0];
	if (probe_col.kind != build_col.kind || probe_col.width != build_col.width)
		return;
	if (probe_col.src_col_idx >= 16)
		return;
	if (probe_col.kind != TdcColumnKind::INT32 && probe_col.kind != TdcColumnKind::INT64)
		return;

	state.fast_inner_enabled = true;
	state.probe_key_is_int32 = (probe_col.kind == TdcColumnKind::INT32);
	state.probe_chunk_slot = static_cast<uint8_t>(probe_col.src_col_idx);
	state.build_key_offset = build_col.offset;
}

void
CopyRowsByResolvedMappingBatch(const PipelineChunk &left_chunk,
			       const uint16_t *left_row_indices,
			       const TupleDataCollection *const *right_tdcs,
			       const uint8_t *const *right_rows,
			       const HashJoinResolvedOutputColumn *resolved_columns,
			       uint16_t output_column_count,
			       PipelineChunk &out,
			       uint16_t out_row_base,
			       uint16_t batch_count)
{
	for (uint16_t batch_idx = 0; batch_idx < batch_count; ++batch_idx)
	{
		CopyResolvedJoinRow(left_chunk,
			left_row_indices[batch_idx],
			right_tdcs != nullptr ? right_tdcs[batch_idx] : nullptr,
			right_rows[batch_idx],
			resolved_columns,
			output_column_count,
			out,
			static_cast<uint16_t>(out_row_base + batch_idx));
	}
}

bool
TryFastInnerJoinProbe(ExecCtx &ctx,
		      const HashJoinFastProbeState &state,
		      uint32_t hash_table_capacity,
		      const uint32_t *bucket_heads,
		      const uint32_t *links,
		      const uint16_t *salts,
		      const TupleDataCollection *build_keys,
		      const TupleDataCollection *build_rows,
		      const PipelineChunk &in,
		      PipelineChunk &out,
		      HashJoinProbeCursorState &cursor,
		      uint32_t &matched_rows)
{
	if (!state.fast_inner_enabled)
		return false;

	if (!cursor.active_probe)
	{
		cursor.active_probe = true;
		cursor.probe_row_idx = 0;
		cursor.build_row_idx = UINT32_MAX;
		cursor.have_build_cursor = false;
		cursor.probe_salt = 0;
		cursor.probe_matched = false;
	}

	while (cursor.probe_row_idx < in.count)
	{
		if (PipelineCancelRequestedEvery(ctx, cursor.probe_row_idx))
			break;
		if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
			break;

		if (!cursor.have_build_cursor)
		{
			if (in.nulls[state.probe_chunk_slot][cursor.probe_row_idx] != 0)
			{
				++cursor.probe_row_idx;
				continue;
			}
			const uint64_t hash = state.probe_key_is_int32
				? HashSingleGroupInt32Value(in.get_int32(state.probe_chunk_slot, cursor.probe_row_idx))
				: HashSingleGroupInt64Value(in.get_int64(state.probe_chunk_slot, cursor.probe_row_idx));
			cursor.probe_salt = HashJoinSalt(hash);
			cursor.build_row_idx = bucket_heads[static_cast<uint32_t>(hash) & (hash_table_capacity - 1u)];
			cursor.have_build_cursor = true;
			while (cursor.build_row_idx != UINT32_MAX &&
			       salts[cursor.build_row_idx] != cursor.probe_salt)
				cursor.build_row_idx = links[cursor.build_row_idx];
		}

		while (cursor.build_row_idx != UINT32_MAX)
		{
			const uint32_t build_row_idx = cursor.build_row_idx;
			cursor.build_row_idx = links[build_row_idx];
			while (cursor.build_row_idx != UINT32_MAX &&
			       salts[cursor.build_row_idx] != cursor.probe_salt)
				cursor.build_row_idx = links[cursor.build_row_idx];

			const uint8_t *build_key_row = TupleDataCollectionGetRowConst(build_keys, build_row_idx);
			const bool match = state.probe_key_is_int32
				? ([&]() {
					int32_t build_v;
					std::memcpy(&build_v, build_key_row + state.build_key_offset, sizeof(build_v));
					return build_v == in.get_int32(state.probe_chunk_slot, cursor.probe_row_idx);
				})()
				: ([&]() {
					int64_t build_v;
					std::memcpy(&build_v, build_key_row + state.build_key_offset, sizeof(build_v));
					return build_v == in.get_int64(state.probe_chunk_slot, cursor.probe_row_idx);
				})();
			if (!match)
				continue;

			const uint8_t *build_row = TupleDataCollectionGetRowConst(build_rows, build_row_idx);
			CopyResolvedJoinRow(in,
				cursor.probe_row_idx,
				build_rows,
				build_row,
				state.resolved_output_columns.data(),
				state.output_column_count,
				out,
				out.count);
			++out.count;
			++matched_rows;
			if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
				break;
		}

		if (cursor.build_row_idx == UINT32_MAX)
		{
			cursor.have_build_cursor = false;
			++cursor.probe_row_idx;
			continue;
		}
		if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
			break;
	}

	if (cursor.probe_row_idx >= in.count)
	{
		cursor.active_probe = false;
		cursor.current_input_drained = out.count > 0;
		cursor.build_row_idx = UINT32_MAX;
		cursor.have_build_cursor = false;
		cursor.probe_salt = 0;
		cursor.probe_matched = false;
	}
	return true;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
