#pragma once

#include <cstdint>
#include <vector>

#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_layout.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

struct HashJoinResolvedOutputColumn {
	HashJoinOutputSide side = HashJoinOutputSide::LEFT;
	uint8_t input_chunk_slot = 0;
	ColumnDecodeKind decode_kind = ColumnDecodeKind::NONE;
	uint8_t output_chunk_slot = 0;
	const TdcColumnDesc *right_col = nullptr;
};

struct HashJoinFastProbeState {
	bool initialized = false;
	bool fast_inner_enabled = false;
	bool probe_key_is_int32 = false;
	uint8_t probe_chunk_slot = 0;
	uint16_t build_key_offset = 0;
	uint16_t output_column_count = 0;
	std::vector<HashJoinResolvedOutputColumn> resolved_output_columns;
};

struct HashJoinProbeCursorState {
	bool active_probe = false;
	bool current_input_drained = false;
	uint16_t probe_row_idx = 0;
	uint32_t build_row_idx = UINT32_MAX;
	bool have_build_cursor = false;
	uint16_t probe_salt = 0;
	bool probe_matched = false;
};

void InitializeHashJoinFastProbeState(HashJoinFastProbeState &state,
				      const HashJoinOutputColumnDesc *output_columns,
				      uint16_t output_column_count,
				      const TupleDataLayout *build_row_layout,
				      const TupleDataLayout *probe_layout,
				      const TupleDataLayout *build_key_layout,
				      HashJoinMatchMode join_mode,
				      uint16_t n_filter_inputs,
				      uint16_t n_filter_exprs,
				      uint16_t n_filter_steps);

void CopyRowsByResolvedMappingBatch(const PipelineChunk &left_chunk,
				    const uint16_t *left_row_indices,
				    const TupleDataCollection *const *right_tdcs,
				    const uint8_t *const *right_rows,
				    const HashJoinResolvedOutputColumn *resolved_columns,
				    uint16_t output_column_count,
				    PipelineChunk &out,
				    uint16_t out_row_base,
				    uint16_t batch_count);

bool TryFastInnerJoinProbe(ExecCtx &ctx,
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
			   uint32_t &matched_rows);

}  /* namespace pipeline */
}  /* namespace pg_yaap */
