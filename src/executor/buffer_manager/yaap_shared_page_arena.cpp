extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <algorithm>
#include <limits>

#include "buffer_manager/yaap_shared_page_arena.hpp"

namespace pg_yaap {
namespace buffer {

namespace {

static YaapSharedPage *
ResolvePage(dsa_area *dsa, dsa_pointer page_dp)
{
	if (dsa == nullptr || !DsaPointerIsValid(page_dp))
		return nullptr;
	return static_cast<YaapSharedPage *>(dsa_get_address(dsa, page_dp));
}

static uint64_t
PageAllocationBytes(const YaapSharedPageArena *arena)
{
	return sizeof(YaapSharedPage) + arena->page_size;
}

static YaapSharedPage *
AllocateNewPage(dsa_area *dsa,
				YaapBufferSharedState *buffer_state,
				YaapSharedPageArena *arena,
				dsa_pointer arena_dp,
				bool already_charged = false)
{
	const uint64_t alloc_bytes = PageAllocationBytes(arena);
	if (!already_charged)
		YaapBufferChargeExternalBytes(buffer_state, alloc_bytes, "shared-page-arena");
	volatile dsa_pointer page_dp = InvalidDsaPointer;
	volatile dsa_pointer payload_dp = InvalidDsaPointer;
	PG_TRY();
	{
		page_dp = dsa_allocate0(dsa, sizeof(YaapSharedPage));
		payload_dp = dsa_allocate0(dsa, arena->page_size);
		auto *page = ResolvePage(dsa, page_dp);
		if (page == nullptr || !DsaPointerIsValid(payload_dp))
			ereport(ERROR, (errmsg("pg_yaap: shared page allocation failed")));
		pg_atomic_init_u32(&page->state,
			static_cast<uint32_t>(YaapSharedPageState::FREE));
		pg_atomic_init_u32(&page->readers, 0);
		page->capacity = arena->page_size;
		page->used = 0;
		page->payload_dp = payload_dp;
		page->next_dp = InvalidDsaPointer;
		page->free_next_dp = arena->free_page_dp;
		page->arena_dp = arena_dp;
		if (DsaPointerIsValid(arena->last_page_dp))
		{
			auto *last = ResolvePage(dsa, arena->last_page_dp);
			if (last == nullptr)
				ereport(ERROR, (errmsg("pg_yaap: shared page arena tail is missing")));
			last->next_dp = page_dp;
		}
		else
			arena->first_page_dp = page_dp;
		arena->last_page_dp = page_dp;
		arena->free_page_dp = page_dp;
		++arena->page_count;
		++arena->free_page_count;
	}
	PG_CATCH();
	{
		if (DsaPointerIsValid(payload_dp))
			dsa_free(dsa, payload_dp);
		if (DsaPointerIsValid(page_dp))
			dsa_free(dsa, page_dp);
		if (!already_charged)
			YaapBufferReleaseExternalBytes(buffer_state, alloc_bytes);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ResolvePage(dsa, page_dp);
}

}  /* namespace */

dsa_pointer
YaapSharedPageArenaCreate(dsa_area *dsa,
                          YaapBufferSharedState *buffer_state,
                          uint32_t page_size,
                          uint32_t page_count)
{
	if (dsa == nullptr || buffer_state == nullptr)
		elog(ERROR, "pg_yaap: shared page arena requires DSA and buffer state");
	if (page_size == 0)
		 elog(ERROR, "pg_yaap: shared page arena page size must be non-zero");
	if (page_count == 0)
		 elog(ERROR, "pg_yaap: shared page arena page count must be non-zero");

	dsa_pointer arena_dp = dsa_allocate0(dsa, sizeof(YaapSharedPageArena));
	auto *arena = static_cast<YaapSharedPageArena *>(dsa_get_address(dsa, arena_dp));
	if (arena == nullptr)
		elog(ERROR, "pg_yaap: shared page arena metadata allocation failed");
	arena->page_size = page_size;
	arena->page_count = 0;
	arena->free_page_count = 0;
	arena->self_dp = arena_dp;
	arena->first_page_dp = InvalidDsaPointer;
	arena->last_page_dp = InvalidDsaPointer;
	arena->free_page_dp = InvalidDsaPointer;
	PG_TRY();
	{
		for (uint32_t i = 0; i < page_count; ++i)
			(void) AllocateNewPage(dsa, buffer_state, arena, arena_dp);
	}
	PG_CATCH();
	{
		YaapSharedPageArenaDestroy(dsa, buffer_state, arena_dp);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return arena_dp;
}

YaapSharedPageArena *
YaapSharedPageArenaResolve(dsa_area *dsa, dsa_pointer arena_dp)
{
	if (dsa == nullptr || !DsaPointerIsValid(arena_dp))
		return nullptr;
	return static_cast<YaapSharedPageArena *>(dsa_get_address(dsa, arena_dp));
}

YaapSharedPage *
YaapSharedPageResolve(dsa_area *dsa, dsa_pointer page_dp)
{
	return ResolvePage(dsa, page_dp);
}

YaapSharedPage *
YaapSharedPageArenaAllocatePage(dsa_area *dsa,
                                YaapBufferSharedState *buffer_state,
                                YaapSharedPageArena *arena)
{
	if (dsa == nullptr || buffer_state == nullptr || arena == nullptr)
		elog(ERROR, "pg_yaap: shared page allocation requires arena inputs");
	if (arena->page_size == 0)
		elog(ERROR, "pg_yaap: shared page arena has invalid page size");

	dsa_pointer page_dp = arena->free_page_dp;
	if (!DsaPointerIsValid(page_dp))
		return nullptr;
	auto *page = ResolvePage(dsa, page_dp);
	if (page == nullptr)
		elog(ERROR, "pg_yaap: shared page arena free-list entry is missing");
	arena->free_page_dp = page->free_next_dp;
	if (arena->free_page_count == 0)
		elog(ERROR, "pg_yaap: shared page arena free-list accounting underflow");
	--arena->free_page_count;
	page->free_next_dp = InvalidDsaPointer;
	page->used = 0;
	pg_atomic_write_u32(&page->readers, 0);
	pg_atomic_write_u32(&page->state,
		static_cast<uint32_t>(YaapSharedPageState::OWNER_WRITE));
	return page;
}

bool
YaapSharedPageArenaTryGrow(dsa_area *dsa,
                           YaapBufferSharedState *buffer_state,
                           YaapSharedPageArena *arena,
                           uint32_t page_count)
{
	if (dsa == nullptr || buffer_state == nullptr || arena == nullptr || page_count == 0)
		return false;
	const uint64_t bytes = static_cast<uint64_t>(page_count) * PageAllocationBytes(arena);
	if (!YaapBufferTryChargeExternalBytes(buffer_state, bytes))
		return false;
	volatile uint32_t allocated = 0;
	PG_TRY();
	{
		for (; allocated < page_count; ++allocated)
			(void) AllocateNewPage(dsa, buffer_state, arena, arena->self_dp, true);
	}
	PG_CATCH();
	{
		/* Linked pages remain owned and charged until arena teardown. */
		YaapBufferReleaseExternalBytes(buffer_state,
			static_cast<uint64_t>(page_count - allocated) * PageAllocationBytes(arena));
		PG_RE_THROW();
	}
	PG_END_TRY();
	return true;
}

uint32_t
YaapSharedPageArenaFreePageCount(const YaapSharedPageArena *arena)
{
	return arena == nullptr ? 0u : arena->free_page_count;
}

void *
YaapSharedPageWritableAddress(dsa_area *dsa, YaapSharedPage *page)
{
	if (page == nullptr ||
		pg_atomic_read_u32(&page->state) !=
			static_cast<uint32_t>(YaapSharedPageState::OWNER_WRITE))
		return nullptr;
	return dsa_get_address(dsa, page->payload_dp);
}

void
YaapSharedPageSetUsed(YaapSharedPage *page, uint32_t used)
{
	if (page == nullptr || used > page->capacity)
		elog(ERROR, "pg_yaap: shared page used bytes exceed capacity");
	page->used = used;
}

void
YaapSharedPageSeal(YaapSharedPage *page)
{
	if (page == nullptr ||
		pg_atomic_read_u32(&page->state) !=
			static_cast<uint32_t>(YaapSharedPageState::OWNER_WRITE))
		elog(ERROR, "pg_yaap: shared page seal requires owner-write state");
	pg_atomic_write_u32(&page->state,
		static_cast<uint32_t>(YaapSharedPageState::SEALED));
}

bool
YaapSharedPageAcquireRead(dsa_area *dsa,
                          dsa_pointer page_dp,
                          YaapSharedPage *page,
                          YaapSharedPageReadLease *lease)
{
	if (lease == nullptr)
		return false;
	*lease = YaapSharedPageReadLease{};
	if (dsa == nullptr || page == nullptr)
		return false;
	if (pg_atomic_read_u32(&page->state) !=
		static_cast<uint32_t>(YaapSharedPageState::SEALED))
		return false;

	pg_atomic_add_fetch_u32(&page->readers, 1);
	if (pg_atomic_read_u32(&page->state) !=
		static_cast<uint32_t>(YaapSharedPageState::SEALED))
	{
		pg_atomic_sub_fetch_u32(&page->readers, 1);
		return false;
	}
	void *data = dsa_get_address(dsa, page->payload_dp);
	if (data == nullptr)
	{
		pg_atomic_sub_fetch_u32(&page->readers, 1);
		return false;
	}
	lease->page_dp = page_dp;
	lease->data = data;
	lease->used = page->used;
	return true;
}

void
YaapSharedPageReleaseRead(dsa_area *dsa,
                          YaapSharedPageReadLease *lease)
{
	if (lease == nullptr || !DsaPointerIsValid(lease->page_dp))
		return;
	YaapSharedPage *page = ResolvePage(dsa, lease->page_dp);
	if (page != nullptr)
		pg_atomic_sub_fetch_u32(&page->readers, 1);
	*lease = YaapSharedPageReadLease{};
}

bool
YaapSharedPageReclaim(dsa_area *dsa,
                      YaapBufferSharedState *buffer_state,
                      dsa_pointer page_dp,
                      YaapSharedPage *page)
{
	if (dsa == nullptr || buffer_state == nullptr || page == nullptr)
		return false;
	if (pg_atomic_read_u32(&page->readers) != 0)
		return false;
	uint32_t expected = static_cast<uint32_t>(YaapSharedPageState::SEALED);
	if (!pg_atomic_compare_exchange_u32(&page->state,
		&expected,
		static_cast<uint32_t>(YaapSharedPageState::RECLAIMING)))
		return false;
	YaapSharedPageArena *arena = YaapSharedPageArenaResolve(dsa, page->arena_dp);
	if (arena == nullptr)
		return false;
	page->free_next_dp = arena->free_page_dp;
	arena->free_page_dp = page_dp;
	++arena->free_page_count;
	pg_atomic_write_u32(&page->state,
		static_cast<uint32_t>(YaapSharedPageState::FREE));
	return true;
}

void
YaapSharedPageArenaDestroy(dsa_area *dsa,
                           YaapBufferSharedState *buffer_state,
                           dsa_pointer arena_dp)
{
	YaapSharedPageArena *arena = YaapSharedPageArenaResolve(dsa, arena_dp);
	if (arena == nullptr)
		return;
	for (dsa_pointer page_dp = arena->first_page_dp;
		 DsaPointerIsValid(page_dp);)
	{
		YaapSharedPage *page = ResolvePage(dsa, page_dp);
		if (page == nullptr)
			break;
		const dsa_pointer next_dp = page->next_dp;
		if (pg_atomic_read_u32(&page->readers) != 0 ||
			pg_atomic_read_u32(&page->state) ==
				static_cast<uint32_t>(YaapSharedPageState::OWNER_WRITE))
			ereport(ERROR, (errmsg("pg_yaap: destroying shared page arena with live page")));
		const uint64_t bytes = sizeof(YaapSharedPage) + page->capacity;
		const bool has_payload = DsaPointerIsValid(page->payload_dp);
		if (has_payload)
			dsa_free(dsa, page->payload_dp);
		dsa_free(dsa, page_dp);
		if (has_payload)
			YaapBufferReleaseExternalBytes(buffer_state, bytes);
		page_dp = next_dp;
	}
	dsa_free(dsa, arena_dp);
}

}  /* namespace buffer */
}  /* namespace pg_yaap */
