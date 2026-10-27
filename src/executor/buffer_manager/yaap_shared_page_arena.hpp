#pragma once

extern "C" {
#include "postgres.h"
#include "port/atomics.h"
#include "utils/dsa.h"
}

#include <cstdint>

#include "buffer_manager/yaap_buffer_shared.hpp"

namespace pg_yaap {
namespace buffer {

/*
 * A page is mutable only while its owner holds OWNER_WRITE.  Once sealed it
 * becomes immutable and may be read by other processes without a per-access
 * mutex.  The state transition is the concurrency boundary.
 */
enum class YaapSharedPageState : uint32_t {
	OWNER_WRITE = 0,
	SEALED = 1,
	RECLAIMING = 2,
	FREE = 3,
};

struct YaapSharedPage {
	pg_atomic_uint32 state;
	pg_atomic_uint32 readers;
	uint32_t capacity;
	uint32_t used;
	dsa_pointer payload_dp;
	dsa_pointer next_dp;
	dsa_pointer free_next_dp;
	dsa_pointer arena_dp;
};

struct YaapSharedPageArena {
	uint32_t page_size;
	uint32_t page_count;
	uint32_t free_page_count;
	uint32_t _pad0;
	dsa_pointer self_dp;
	dsa_pointer first_page_dp;
	dsa_pointer last_page_dp;
	dsa_pointer free_page_dp;
};

struct YaapSharedPageReadLease {
	dsa_pointer page_dp = InvalidDsaPointer;
	const void *data = nullptr;
	uint32_t used = 0;

	bool IsValid() const
	{
		return DsaPointerIsValid(page_dp) && data != nullptr;
	}
};

dsa_pointer YaapSharedPageArenaCreate(dsa_area *dsa,
                                      YaapBufferSharedState *buffer_state,
                                      uint32_t page_size,
                                      uint32_t page_count = 128);

YaapSharedPageArena *YaapSharedPageArenaResolve(dsa_area *dsa,
                                                dsa_pointer arena_dp);

YaapSharedPage *YaapSharedPageResolve(dsa_area *dsa,
                                      dsa_pointer page_dp);

YaapSharedPage *YaapSharedPageArenaAllocatePage(
	dsa_area *dsa,
	YaapBufferSharedState *buffer_state,
	YaapSharedPageArena *arena);

bool YaapSharedPageArenaTryGrow(dsa_area *dsa,
	                              YaapBufferSharedState *buffer_state,
	                              YaapSharedPageArena *arena,
	                              uint32_t page_count);

uint32_t YaapSharedPageArenaFreePageCount(const YaapSharedPageArena *arena);

void *YaapSharedPageWritableAddress(dsa_area *dsa,
                                    YaapSharedPage *page);

void YaapSharedPageSetUsed(YaapSharedPage *page, uint32_t used);
void YaapSharedPageSeal(YaapSharedPage *page);

bool YaapSharedPageAcquireRead(dsa_area *dsa,
	                           dsa_pointer page_dp,
                               YaapSharedPage *page,
                               YaapSharedPageReadLease *lease);
void YaapSharedPageReleaseRead(dsa_area *dsa,
                               YaapSharedPageReadLease *lease);

bool YaapSharedPageReclaim(dsa_area *dsa,
                           YaapBufferSharedState *buffer_state,
                           dsa_pointer page_dp,
                           YaapSharedPage *page);

void YaapSharedPageArenaDestroy(dsa_area *dsa,
                                YaapBufferSharedState *buffer_state,
                                dsa_pointer arena_dp);

}  /* namespace buffer */
}  /* namespace pg_yaap */
