#include "core/parallel_dsa_bridge.hpp"

namespace pg_yaap
{

struct ParallelRowChunkHeader
{
	uint32 row_count = 0;
	uint32 string_arena_size = 0;
};

DsaDataChunkBridge::DsaDataChunkBridge(dsa_area *area, size_t mem_limit)
	: dsa_(area), limit_(mem_limit), total_allocated_(0), fallback_triggered_(false)
{
}

DsaDataChunkBridge::~DsaDataChunkBridge()
{
}

dsa_pointer DsaDataChunkBridge::append_data_chunk(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk, bool &spilled_to_disk, BufFile *spill_file)
{
	uint32 row_count = chunk.has_selection ? chunk.sel.count : chunk.count;
	uint32 string_arena_size = (uint32) chunk.string_arena.size();

	size_t req_size = sizeof(ParallelRowChunkHeader) +
					  16 * row_count * (sizeof(uint8_t) + sizeof(int32_t) + sizeof(int64_t) + sizeof(double) + sizeof(VecStringRef)) +
					  string_arena_size;

	if (fallback_triggered_ || total_allocated_ + req_size > limit_)
	{
		fallback_triggered_ = true;
		spilled_to_disk = true;

		elog(LOG, "pg_yaap: DSA bridge FALLBACK to BufFile (row_count=%u, req_size=%zu, total_allocated=%zu, limit=%zu)",
			 row_count, req_size, total_allocated_, limit_);

		ParallelRowChunkHeader header;
		header.row_count = row_count;
		header.string_arena_size = string_arena_size;

		BufFileWrite(spill_file, &header, sizeof(header));

		/* Column-major batch write for BufFile fallback */
		for (int col = 0; col < 16; col++)
		{
			if (chunk.has_selection)
			{
				/* Compact selection into temp buffers then batch write */
				uint8_t nulls_buf[DEFAULT_CHUNK_SIZE];
				int32_t int32_buf[DEFAULT_CHUNK_SIZE];
				int64_t int64_buf[DEFAULT_CHUNK_SIZE];
				double double_buf[DEFAULT_CHUNK_SIZE];
				VecStringRef string_buf[DEFAULT_CHUNK_SIZE];

				for (uint32 r = 0; r < row_count; r++)
				{
					int src_row = chunk.sel.row_ids[r];
					nulls_buf[r] = chunk.nulls[col][src_row];
					int32_buf[r] = chunk.int32_columns[col][src_row];
					int64_buf[r] = chunk.int64_columns[col][src_row];
					double_buf[r] = chunk.double_columns[col][src_row];
					string_buf[r] = chunk.string_columns[col][src_row];
				}

				BufFileWrite(spill_file, nulls_buf, row_count * sizeof(uint8_t));
				BufFileWrite(spill_file, int32_buf, row_count * sizeof(int32_t));
				BufFileWrite(spill_file, int64_buf, row_count * sizeof(int64_t));
				BufFileWrite(spill_file, double_buf, row_count * sizeof(double));
				BufFileWrite(spill_file, string_buf, row_count * sizeof(VecStringRef));
			}
			else
			{
				/* No selection - batch write entire columns */
				BufFileWrite(spill_file, chunk.nulls[col], row_count * sizeof(uint8_t));
				BufFileWrite(spill_file, chunk.int32_columns[col], row_count * sizeof(int32_t));
				BufFileWrite(spill_file, chunk.int64_columns[col], row_count * sizeof(int64_t));
				BufFileWrite(spill_file, chunk.double_columns[col], row_count * sizeof(double));
				BufFileWrite(spill_file, chunk.string_columns[col], row_count * sizeof(VecStringRef));
			}
		}
		if (header.string_arena_size > 0)
			BufFileWrite(spill_file, chunk.string_arena.data(), header.string_arena_size);
		return 0;
	}

	spilled_to_disk = false;
	dsa_pointer ptr = dsa_allocate(dsa_, req_size);
	total_allocated_ += req_size;
	memory_pointers_.push_back(ptr);

	static int log_counter = 0;
	if ((log_counter++ % 1000) == 0)
		elog(LOG, "pg_yaap: DSA bridge using DSA memory (row_count=%u, req_size=%zu, total_allocated=%zu, limit=%zu, chunks=%zu)",
			 row_count, req_size, total_allocated_, limit_, memory_pointers_.size());

	char *local_addr = (char *)dsa_get_address(dsa_, ptr);

	ParallelRowChunkHeader header;
	header.row_count = row_count;
	header.string_arena_size = string_arena_size;
	memcpy(local_addr, &header, sizeof(header));
	local_addr += sizeof(header);

	/* Column-major copy for DSA - batch memcpy per column */
	for (int col = 0; col < 16; col++)
	{
		if (chunk.has_selection)
		{
			/* Compact selection into temp buffers then batch memcpy */
			uint8_t nulls_buf[DEFAULT_CHUNK_SIZE];
			int32_t int32_buf[DEFAULT_CHUNK_SIZE];
			int64_t int64_buf[DEFAULT_CHUNK_SIZE];
			double double_buf[DEFAULT_CHUNK_SIZE];
			VecStringRef string_buf[DEFAULT_CHUNK_SIZE];

			for (uint32 r = 0; r < row_count; r++)
			{
				int src_row = chunk.sel.row_ids[r];
				nulls_buf[r] = chunk.nulls[col][src_row];
				int32_buf[r] = chunk.int32_columns[col][src_row];
				int64_buf[r] = chunk.int64_columns[col][src_row];
				double_buf[r] = chunk.double_columns[col][src_row];
				string_buf[r] = chunk.string_columns[col][src_row];
			}

			memcpy(local_addr, nulls_buf, row_count * sizeof(uint8_t));
			local_addr += row_count * sizeof(uint8_t);
			memcpy(local_addr, int32_buf, row_count * sizeof(int32_t));
			local_addr += row_count * sizeof(int32_t);
			memcpy(local_addr, int64_buf, row_count * sizeof(int64_t));
			local_addr += row_count * sizeof(int64_t);
			memcpy(local_addr, double_buf, row_count * sizeof(double));
			local_addr += row_count * sizeof(double);
			memcpy(local_addr, string_buf, row_count * sizeof(VecStringRef));
			local_addr += row_count * sizeof(VecStringRef);
		}
		else
		{
			/* No selection - batch memcpy entire columns */
			memcpy(local_addr, chunk.nulls[col], row_count * sizeof(uint8_t));
			local_addr += row_count * sizeof(uint8_t);
			memcpy(local_addr, chunk.int32_columns[col], row_count * sizeof(int32_t));
			local_addr += row_count * sizeof(int32_t);
			memcpy(local_addr, chunk.int64_columns[col], row_count * sizeof(int64_t));
			local_addr += row_count * sizeof(int64_t);
			memcpy(local_addr, chunk.double_columns[col], row_count * sizeof(double));
			local_addr += row_count * sizeof(double);
			memcpy(local_addr, chunk.string_columns[col], row_count * sizeof(VecStringRef));
			local_addr += row_count * sizeof(VecStringRef);
		}
	}
	if (header.string_arena_size > 0)
	{
		memcpy(local_addr, chunk.string_arena.data(), header.string_arena_size);
	}

	return ptr;
}

DsaDataChunkBridgeReader::DsaDataChunkBridgeReader(dsa_area *area, const dsa_pointer *ptrs, size_t num_ptrs, BufFile *fallback_file)
	: dsa_(area), ptrs_(ptrs), num_ptrs_(num_ptrs), current_ptr_idx_(0), fallback_file_(fallback_file)
{
}

DsaDataChunkBridgeReader::~DsaDataChunkBridgeReader()
{
}

bool DsaDataChunkBridgeReader::read_next(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	if (current_ptr_idx_ < num_ptrs_)
	{
		char *local_addr = (char *)dsa_get_address(dsa_, ptrs_[current_ptr_idx_]);

		ParallelRowChunkHeader header;
		memcpy(&header, local_addr, sizeof(header));
		local_addr += sizeof(header);

		chunk.reset();
		chunk.count = header.row_count;
		chunk.has_selection = false;

		/* Column-major read from DSA - batch memcpy per column */
		for (int col = 0; col < 16; col++)
		{
			memcpy(chunk.nulls[col], local_addr, header.row_count * sizeof(uint8_t));
			local_addr += header.row_count * sizeof(uint8_t);
			memcpy(chunk.int32_columns[col], local_addr, header.row_count * sizeof(int32_t));
			local_addr += header.row_count * sizeof(int32_t);
			memcpy(chunk.int64_columns[col], local_addr, header.row_count * sizeof(int64_t));
			local_addr += header.row_count * sizeof(int64_t);
			memcpy(chunk.double_columns[col], local_addr, header.row_count * sizeof(double));
			local_addr += header.row_count * sizeof(double);
			memcpy(chunk.string_columns[col], local_addr, header.row_count * sizeof(VecStringRef));
			local_addr += header.row_count * sizeof(VecStringRef);
		}
		if (header.string_arena_size > 0)
		{
			chunk.string_arena.resize(header.string_arena_size);
			memcpy(chunk.string_arena.data(), local_addr, header.string_arena_size);
		}

		current_ptr_idx_++;
		return true;
	}
	else if (fallback_file_ != nullptr)
	{
		ParallelRowChunkHeader header;
		if (BufFileRead(fallback_file_, &header, sizeof(header)) != sizeof(header))
			return false;

		chunk.reset();
		chunk.count = header.row_count;
		chunk.has_selection = false;

		/* Column-major read from BufFile */
		for (int col = 0; col < 16; col++)
		{
			if (BufFileRead(fallback_file_, chunk.nulls[col], header.row_count * sizeof(uint8_t)) != header.row_count * sizeof(uint8_t))
				return false;
			if (BufFileRead(fallback_file_, chunk.int32_columns[col], header.row_count * sizeof(int32_t)) != header.row_count * sizeof(int32_t))
				return false;
			if (BufFileRead(fallback_file_, chunk.int64_columns[col], header.row_count * sizeof(int64_t)) != header.row_count * sizeof(int64_t))
				return false;
			if (BufFileRead(fallback_file_, chunk.double_columns[col], header.row_count * sizeof(double)) != header.row_count * sizeof(double))
				return false;
			if (BufFileRead(fallback_file_, chunk.string_columns[col], header.row_count * sizeof(VecStringRef)) != header.row_count * sizeof(VecStringRef))
				return false;
		}
		if (header.string_arena_size > 0)
		{
			chunk.string_arena.resize(header.string_arena_size);
			if (BufFileRead(fallback_file_, chunk.string_arena.data(), header.string_arena_size) != header.string_arena_size)
				return false;
		}
		return true;
	}

	return false;
}

/* Close the namespace opened by types.hpp */
} // namespace pg_yaap
