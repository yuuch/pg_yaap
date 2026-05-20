#pragma once

#include "core/types.hpp"
#include "core/memory.hpp"

namespace pg_yaap
{

struct VecHashPartition {
	uint64_t *keys;       /* join key values */
	uint64_t *payloads;   /* payload columns compact storage */
	uint32_t  count;
	uint32_t  capacity;
	bool      is_external; /* data in DSM (not local memory) */
};

struct VecHashPartitionTable {
	VecHashPartition partitions[VOLVEC_RADIX_FANOUT];
	uint32_t partition_counts[VOLVEC_RADIX_FANOUT];  /* histogram / running offsets */

	VecHashPartitionTable() {
		memset(this, 0, sizeof(*this));
	}
};

struct VecLinearProbeHT {
	uint64_t *keys;      /* key array, size = capacity */
	uint64_t *payloads;  /* payload data, compact */
	uint32_t *values;    /* index into payloads */
	uint8_t  *states;    /* 0=empty, 1=occupied */
	uint32_t capacity;
	uint32_t count;

	VecLinearProbeHT() : keys(nullptr), payloads(nullptr), values(nullptr), states(nullptr), capacity(0), count(0) {}
};

struct VecBloomFilter {
	uint64_t *bits;
	uint32_t bit_count;
	uint32_t hash_funcs;
	uint64_t seeds[4];

	VecBloomFilter() : bits(nullptr), bit_count(0), hash_funcs(0) { memset(seeds, 0, sizeof(seeds)); }
};

static constexpr uint32_t VOLVEC_ROW_VARLEN_INLINE_OFFSET = UINT32_MAX;

struct ParallelHashRowVarlenRef {
	uint64_t prefix;
	uint32_t offset;
	uint32_t length;

	ParallelHashRowVarlenRef()
		: prefix(0), offset(0), length(0)
	{
	}
};

struct ParallelHashRowBlockDesc {
	uint64_t block_offset;
	uint32_t start_row_ordinal;
	uint32_t row_count;
	uint32_t row_width;
	uint32_t reserved;

	ParallelHashRowBlockDesc()
		: block_offset(0), start_row_ordinal(0), row_count(0), row_width(0), reserved(0)
	{
	}
};

struct ParallelHashBuildTaskPartitionRows {
	uint64_t row_blocks_offset;
	uint64_t row_blocks_size;
	uint64_t varlen_offset;
	uint64_t varlen_size;
	uint32_t row_count;
	uint32_t row_width;

	ParallelHashBuildTaskPartitionRows()
		: row_blocks_offset(0), row_blocks_size(0), varlen_offset(0), varlen_size(0),
		  row_count(0), row_width(0)
	{
	}
};

struct ParallelHashBuildTaskFragmentRows {
	uint32_t task_id;
	uint32_t num_partitions;
	uint64_t partitions_offset;
	uint64_t partitions_size;

	ParallelHashBuildTaskFragmentRows()
		: task_id(0), num_partitions(0), partitions_offset(0), partitions_size(0)
	{
	}
};

struct ParallelHashBuildPartitionRows {
	uint64_t row_blocks_offset;
	uint64_t row_blocks_size;
	uint64_t row_block_descs_offset;
	uint64_t row_block_descs_size;
	uint64_t varlen_offset;
	uint64_t varlen_size;
	uint64_t bucket_heads_offset;
	uint64_t bucket_heads_size;
	uint64_t chain_next_offset;
	uint64_t chain_next_size;
	uint32_t row_count;
	uint32_t row_width;
	uint32_t bucket_count;
	uint32_t bucket_mask;

	ParallelHashBuildPartitionRows()
		: row_blocks_offset(0), row_blocks_size(0), row_block_descs_offset(0), row_block_descs_size(0),
		  varlen_offset(0), varlen_size(0), bucket_heads_offset(0), bucket_heads_size(0),
		  chain_next_offset(0), chain_next_size(0), row_count(0), row_width(0),
		  bucket_count(0), bucket_mask(0)
	{
	}
};

struct ParallelHashGlobalPartitionState {
	uint64_t row_blocks_offset;
	uint64_t row_blocks_size;
	uint64_t varlen_offset;
	uint64_t varlen_size;
	uint64_t bucket_heads_offset;
	uint64_t bucket_heads_size;
	uint64_t chain_next_offset;
	uint64_t chain_next_size;
	uint32_t merged_row_count;
	uint32_t expected_fragments;
	uint32_t merged_fragments;
	uint8_t finalize_ready;
	uint8_t probe_ready;
	uint8_t reserved[2];

	ParallelHashGlobalPartitionState()
		: row_blocks_offset(0), row_blocks_size(0), varlen_offset(0), varlen_size(0),
		  bucket_heads_offset(0), bucket_heads_size(0), chain_next_offset(0), chain_next_size(0),
		  merged_row_count(0), expected_fragments(0), merged_fragments(0),
		  finalize_ready(0), probe_ready(0), reserved{0, 0}
	{
	}
};

/* Serialized partition metadata for the shared hash bridge. */
struct ParallelHashBuildPartition {
	uint64_t entries_offset;
	uint64_t entries_size;
	uint64_t bucket_heads_offset;
	uint64_t bucket_heads_size;
	uint64_t chunks_offset;
	uint64_t chunks_size;
	uint32_t bucket_count;
	uint32_t bucket_mask;
	uint32_t entry_count;
	uint32_t chunk_count;
};

/* Serialized build payload chunk metadata for the shared hash bridge. */
struct ParallelHashBuildChunk {
	uint32_t row_count;
	uint32_t string_arena_size;
	uint64_t string_arena_offset;
	uint64_t value_offsets[16];
	uint64_t aux_value_offsets[16];
	uint64_t nulls_offsets[16];
};

/* Serialized header for the shared hash bridge. */
struct ParallelHashBuildState {
	uint32_t magic;
	uint32_t version;
	uint32_t num_partitions;
	uint32_t total_entries;
	uint32_t num_chunks;
	uint32_t num_payload_cols;
	uint32_t row_layout_version;
	uint32_t row_width;
	uint64_t partitions_offset;
	uint64_t partitions_size;
	uint64_t row_partitions_offset;
	uint64_t row_partitions_size;
	uint64_t payload_cols_offset;
	uint64_t payload_cols_size;
	uint64_t bucket_heads_offset;
	uint64_t bucket_heads_size;
	uint32_t bucket_count;
	uint32_t bucket_mask;
	uint64_t entries_offset;
	uint64_t entries_size;
	uint64_t chunks_offset;
	uint64_t chunks_size;
	uint8_t build_complete;
	uint8_t reserved[7];
};

/* Serialized header for a worker-local hash build fragment. */
struct ParallelHashBuildFragmentState {
	uint32_t total_entries;
	uint32_t num_chunks;
	uint32_t num_payload_cols;
	uint32_t reserved;
	uint64_t entries_offset;
	uint64_t entries_size;
	uint64_t chunks_offset;
	uint64_t chunks_size;
};

} // namespace pg_yaap
