#pragma once

/*
 * pipeline/pipeline_dsm_lookup.hpp  (M-FRAME-MIN step 3f)
 *
 * Process-local id -> object resolver. Locked by:
 *   docs/PIPELINE_PORT_PLAN.md §15.5
 *   docs/GLOBAL_LOCAL_STATE_DESIGN.md §8.5
 *
 * Why this exists:
 *   TaskDescriptor (dsm_task_queue.hpp) carries scheduler-assigned u32 ids
 *   (event_id, pipeline_id) instead of C++ pointers, because DSM is shared
 *   across processes and pointers are NOT portable. Each process therefore
 *   needs a private map from id -> local C++ object. That map is THIS class.
 *
 * Lifetime:
 *   - One PipelineDsmLookup<Pipeline> and one PipelineDsmLookup<Event> per
 *     query, owned by TaskScheduler (3g).
 *   - Lives in the per-query MemoryContext. NOT in DSM. NOT in DSA.
 *   - Leader populates at Schedule() time; workers populate during
 *     pipeline_worker_main setup after locally reconstructing the
 *     PhysicalOperator tree from the serialized PlannedStmt + id manifest.
 *
 * Non-owning: stored T* are owned by the per-query arena (MetaPipeline /
 * scheduler), never freed by this lookup.
 */

#include <cstdint>
#include <unordered_map>

extern "C" {
#include "postgres.h"
}

#include "core/memory.hpp"

namespace pg_yaap {
namespace pipeline {

template <typename T>
class PipelineDsmLookup
{
public:
	using Map = std::unordered_map<
		uint32_t,
		T *,
		std::hash<uint32_t>,
		std::equal_to<uint32_t>,
		PgMemoryContextAllocator<std::pair<const uint32_t, T *>>>;

	explicit PipelineDsmLookup(MemoryContext mcxt)
	    : map_(0,
	           std::hash<uint32_t>(),
	           std::equal_to<uint32_t>(),
	           PgMemoryContextAllocator<std::pair<const uint32_t, T *>>(mcxt))
	{
	}

	/* Register id -> obj. Caller MUST guarantee id is unique within this
	 * lookup; duplicate ids are a scheduler bug. */
	void
	Register(uint32_t id, T *obj)
	{
		Assert(obj != nullptr);
		Assert(map_.find(id) == map_.end());
		map_.emplace(id, obj);
	}

	/* Returns nullptr when id is unknown (worker received a descriptor
	 * before its local reconstruction caught up — caller decides retry vs
	 * abort). */
	T *
	Resolve(uint32_t id) const
	{
		auto it = map_.find(id);
		return it == map_.end() ? nullptr : it->second;
	}

	size_t Size() const { return map_.size(); }

private:
	Map map_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
