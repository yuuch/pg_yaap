#pragma once

#include <cstdint>
#include <memory>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

struct ChunkLease
{
	DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE> *chunk = nullptr;
	uint32_t slot = UINT32_MAX;
	uint32_t generation = 0;

	bool IsValid() const { return chunk != nullptr && slot != UINT32_MAX; }
};

/* Worker-local pool. No object or lease from this class is published. */
class LocalChunkPool
{
public:
	ChunkLease Acquire()
	{
		uint32_t slot;
	if (free_slots_.empty())
		{
			slot = static_cast<uint32_t>(chunks_.size());
			chunks_.push_back(std::make_unique<DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>>());
			generations_.push_back(1);
			leased_.push_back(0);
		}
		else
		{
			slot = free_slots_.back();
			free_slots_.pop_back();
		}
		Assert(slot < chunks_.size());
		Assert(leased_[slot] == 0);
		leased_[slot] = 1;
		return ChunkLease{chunks_[slot].get(), slot, generations_[slot]};
	}

	void Release(ChunkLease *lease)
	{
		if (lease == nullptr || !lease->IsValid())
			return;
		if (lease->slot >= chunks_.size() || leased_[lease->slot] == 0 ||
			generations_[lease->slot] != lease->generation ||
			chunks_[lease->slot].get() != lease->chunk)
			ereport(ERROR, (errmsg("pg_yaap: invalid local chunk lease")));
		chunks_[lease->slot]->reset_lightweight();
		leased_[lease->slot] = 0;
		++generations_[lease->slot];
		if (generations_[lease->slot] == 0)
			generations_[lease->slot] = 1;
		free_slots_.push_back(lease->slot);
		*lease = ChunkLease{};
	}

	void TrimStringArenas(size_t target_bytes)
	{
		for (size_t i = 0; i < chunks_.size(); ++i)
		{
			if (leased_[i] == 0 && chunks_[i]->string_arena.capacity() > target_bytes)
			{
				chunks_[i]->string_arena.clear();
				chunks_[i]->string_arena.shrink_to_fit();
			}
		}
	}

	uint32_t Size() const { return static_cast<uint32_t>(chunks_.size()); }
	uint32_t LeasedCount() const
	{
		return static_cast<uint32_t>(chunks_.size() - free_slots_.size());
	}

private:
	PgVector<std::unique_ptr<DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>>> chunks_;
	PgVector<uint32_t> generations_;
	PgVector<uint32_t> free_slots_;
	PgVector<uint8_t> leased_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
