#pragma once

extern "C" {
#include "postgres.h"
#include "port/atomics.h"
}

#include <cstdint>

namespace pg_yaap {
namespace buffer {

/*
 * Query-scoped accounting for shared breaker storage.
 *
 * This is deliberately a small control object.  It lives in DSA, while the
 * actual payload is owned by an arena.  Writers charge at arena/slab
 * granularity; no hot-path row operation should touch these counters.
 */
struct YaapBufferSharedState
{
	uint64_t hard_limit;
	uint64_t soft_limit;
	pg_atomic_uint64 resident_bytes;
	pg_atomic_uint64 committed_bytes;
	pg_atomic_uint64 spill_bytes;
	pg_atomic_uint64 peak_resident_bytes;
	pg_atomic_uint64 allocation_count;
	pg_atomic_uint64 pressure_epoch;
};

void YaapBufferSharedStateInit(YaapBufferSharedState *state,
							   uint64_t hard_limit,
							   uint64_t soft_limit);

bool YaapBufferTryChargeExternalBytes(YaapBufferSharedState *state,
								  uint64_t bytes);

void YaapBufferChargeExternalBytes(YaapBufferSharedState *state,
							   uint64_t bytes,
							   const char *what);

void YaapBufferReleaseExternalBytes(YaapBufferSharedState *state,
								uint64_t bytes);

void YaapBufferRecordSpillBytes(YaapBufferSharedState *state, uint64_t bytes);

uint64_t YaapBufferResidentBytes(const YaapBufferSharedState *state);
bool YaapBufferOverSoftLimit(const YaapBufferSharedState *state);
uint64_t YaapBufferPressureEpoch(const YaapBufferSharedState *state);

}  /* namespace buffer */
}  /* namespace pg_yaap */
