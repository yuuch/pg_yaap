#pragma once

#include "core/data_chunk.hpp"

namespace pg_yaap
{

class DsaDataChunkBridge {
public:
	DsaDataChunkBridge(dsa_area *area, size_t mem_limit);
	~DsaDataChunkBridge();

	dsa_pointer append_data_chunk(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk, bool &spilled_to_disk, BufFile *spill_file);

	const std::vector<dsa_pointer>& get_memory_pointers() const { return memory_pointers_; }

	size_t get_total_allocated() const { return total_allocated_; }

private:
	dsa_area *dsa_;
	size_t limit_;
	size_t total_allocated_;
	bool fallback_triggered_;
	std::vector<dsa_pointer> memory_pointers_;
};

class DsaDataChunkBridgeReader {
public:
	DsaDataChunkBridgeReader(dsa_area *area, const dsa_pointer *ptrs, size_t num_ptrs, BufFile *fallback_file);
	~DsaDataChunkBridgeReader();

	bool read_next(DataChunk<DEFAULT_CHUNK_SIZE> &chunk);

private:
	dsa_area *dsa_;
	const dsa_pointer *ptrs_;
	size_t num_ptrs_;
	size_t current_ptr_idx_;
	BufFile *fallback_file_;
};

} // namespace pg_yaap
