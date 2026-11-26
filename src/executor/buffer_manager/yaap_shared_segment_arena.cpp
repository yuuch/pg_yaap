extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include "buffer_manager/yaap_shared_segment_arena.hpp"

#include <cstdio>
#include <cstring>

namespace pg_yaap {
namespace buffer {

namespace {

static constexpr uint32_t kSegmentIndexRootEntries = 1024;
static constexpr uint32_t kSegmentIndexLeafEntries = 4096;

static constexpr uint32_t kSegmentSpillMagic = 0x59415350; /* YASP */

static uint64_t
SegmentChecksum(const void *data, uint32_t bytes)
{
	const auto *ptr = static_cast<const unsigned char *>(data);
	uint64_t hash = UINT64_C(1469598103934665603);
	for (uint32_t i = 0; i < bytes; ++i)
	{
		hash ^= ptr[i];
		hash *= UINT64_C(1099511628211);
	}
	return hash;
}

static YaapSharedSegmentDesc *
ResolveDesc(dsa_area *dsa, dsa_pointer dp)
{
	return dsa == nullptr || !DsaPointerIsValid(dp) ? nullptr :
		static_cast<YaapSharedSegmentDesc *>(dsa_get_address(dsa, dp));
}

static uint64_t
SegmentAllocationBytes(const YaapSharedSegmentArena *arena)
{
	return sizeof(YaapSharedSegmentDesc) + arena->segment_size;
}

static YaapSegmentHandle
HandleFor(const YaapSharedSegmentDesc *segment)
{
	return YaapSegmentHandle{segment->arena_id, segment->segment_id, segment->generation};
}

static YaapSharedSegmentDesc *
AllocateNew(dsa_area *dsa, YaapBufferSharedState *state,
				YaapSharedSegmentArena *arena, dsa_pointer arena_dp,
				uint32_t owner_worker, uint64_t owner_tag, bool already_charged)
{
	const uint64_t bytes = SegmentAllocationBytes(arena);
	if (!already_charged)
	{
		char purpose[64];
		std::snprintf(purpose, sizeof(purpose), "shared-segment arena=%u",
			arena->arena_id);
		YaapBufferChargeExternalBytes(state, bytes, purpose);
	}
	volatile dsa_pointer desc_dp = InvalidDsaPointer;
	volatile dsa_pointer payload_dp = InvalidDsaPointer;
	PG_TRY();
	{
		desc_dp = dsa_allocate0(dsa, sizeof(YaapSharedSegmentDesc));
		payload_dp = dsa_allocate0(dsa, arena->segment_size);
		auto *segment = ResolveDesc(dsa, desc_dp);
		if (segment == nullptr || !DsaPointerIsValid(payload_dp))
			ereport(ERROR, (errmsg("pg_yaap: shared segment allocation failed")));
		segment->arena_id = arena->arena_id;
		/* Publish the fully allocated descriptor under the arena lock below. */
		segment->generation = 1;
		segment->owner_worker = owner_worker;
		segment->owner_tag = owner_tag;
		segment->capacity = arena->segment_size;
		segment->used = 0;
		segment->payload_dp = payload_dp;
		segment->self_dp = desc_dp;
		segment->next_dp = InvalidDsaPointer;
		segment->chain_next_dp = InvalidDsaPointer;
		/* A newly allocated segment is returned directly to its owner.  It must
		 * not also be visible on the free-list while it is writable. */
		segment->free_next_dp = InvalidDsaPointer;
		pg_atomic_init_u32(&segment->state,
							static_cast<uint32_t>(YaapSharedSegmentState::FREE));
		pg_atomic_init_u32(&segment->readers, 0);
			for (;;)
			{
				SpinLockAcquire(&arena->mutex);
				const uint32_t id = arena->next_segment_id;
					const uint32_t root_slot = id / kSegmentIndexLeafEntries;
					const uint32_t leaf_slot = id % kSegmentIndexLeafEntries;
				if (root_slot >= kSegmentIndexRootEntries)
				{
					SpinLockRelease(&arena->mutex);
					ereport(ERROR, (errmsg("pg_yaap: shared segment index id space exhausted")));
				}
				auto *root = static_cast<dsa_pointer *>(
					dsa_get_address(dsa, arena->segment_index_root_dp));
				dsa_pointer leaf_dp = root[root_slot];
				if (DsaPointerIsValid(leaf_dp))
				{
					auto *leaf = static_cast<dsa_pointer *>(dsa_get_address(dsa, leaf_dp));
					if (leaf == nullptr)
					{
						SpinLockRelease(&arena->mutex);
						ereport(ERROR, (errmsg("pg_yaap: shared segment index page is unavailable")));
					}
					segment->segment_id = id;
					++arena->next_segment_id;
					leaf[leaf_slot] = desc_dp;
					if (DsaPointerIsValid(arena->last_segment_dp))
					{
						auto *last = ResolveDesc(dsa, arena->last_segment_dp);
						last->next_dp = desc_dp;
					}
					else
						arena->first_segment_dp = desc_dp;
					arena->last_segment_dp = desc_dp;
					++arena->segment_count;
					SpinLockRelease(&arena->mutex);
					break;
				}
				SpinLockRelease(&arena->mutex);

				const size_t leaf_bytes =
					static_cast<size_t>(kSegmentIndexLeafEntries) * sizeof(dsa_pointer);
				YaapBufferChargeExternalBytes(state, leaf_bytes, "shared-segment-index-page");
				volatile dsa_pointer candidate_dp = InvalidDsaPointer;
				PG_TRY();
				{
					candidate_dp = dsa_allocate0(dsa, leaf_bytes);
				}
				PG_CATCH();
				{
					YaapBufferReleaseExternalBytes(state, leaf_bytes);
					PG_RE_THROW();
				}
				PG_END_TRY();

				SpinLockAcquire(&arena->mutex);
				root = static_cast<dsa_pointer *>(
					dsa_get_address(dsa, arena->segment_index_root_dp));
				if (!DsaPointerIsValid(root[root_slot]))
				{
					root[root_slot] = candidate_dp;
					candidate_dp = InvalidDsaPointer;
					arena->segment_index_bytes += leaf_bytes;
				}
				SpinLockRelease(&arena->mutex);
				if (DsaPointerIsValid(candidate_dp))
				{
					dsa_free(dsa, candidate_dp);
					YaapBufferReleaseExternalBytes(state, leaf_bytes);
				}
			}
	}
	PG_CATCH();
	{
		if (DsaPointerIsValid(payload_dp))
			dsa_free(dsa, payload_dp);
		if (DsaPointerIsValid(desc_dp))
			dsa_free(dsa, desc_dp);
		if (!already_charged)
			YaapBufferReleaseExternalBytes(state, bytes);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ResolveDesc(dsa, desc_dp);
}

}  /* namespace */

bool
YaapSharedSegmentSpill(dsa_area *dsa, YaapBufferSharedState *buffer_state,
						   const YaapSegmentHandle &handle, dsa_pointer arena_dp,
						   YaapSpillStore *store,
						   YaapSpillExtent *extent)
{
	if (buffer_state == nullptr || extent == nullptr)
		return false;
	auto *segment = YaapSharedSegmentResolve(dsa, arena_dp, handle);
	if (segment == nullptr || pg_atomic_read_u32(&segment->state) !=
		static_cast<uint32_t>(YaapSharedSegmentState::PUBLISHED))
		return false;
	if (pg_atomic_read_u32(&segment->readers) != 0)
		return false;
	const void *payload = dsa_get_address(dsa, segment->payload_dp);
	if (payload == nullptr)
		return false;
	YaapSegmentSpillImageHeader header{
		kSegmentSpillMagic, 1, 0, handle.arena_id, handle.segment_id,
		handle.generation, segment->used, SegmentChecksum(payload, segment->used)};
	uint32_t expected = static_cast<uint32_t>(YaapSharedSegmentState::PUBLISHED);
	if (!pg_atomic_compare_exchange_u32(&segment->state, &expected,
										 static_cast<uint32_t>(YaapSharedSegmentState::SPILLING)))
		return false;
	if (pg_atomic_read_u32(&segment->readers) != 0)
	{
		pg_atomic_write_u32(&segment->state,
			static_cast<uint32_t>(YaapSharedSegmentState::PUBLISHED));
		return false;
	}
	const YaapSpillExtent previous_extent = segment->spill_extent;
	const bool has_shared_backing = previous_extent.file_name[0] != '\0';
	if (!has_shared_backing && store == nullptr)
	{
		pg_atomic_write_u32(&segment->state,
			static_cast<uint32_t>(YaapSharedSegmentState::PUBLISHED));
		return false;
	}
	if (has_shared_backing)
		*extent = previous_extent;
	else
	{
		uint32_t spill_sequence = ++segment->spill_sequence;
		if (spill_sequence == 0)
			spill_sequence = ++segment->spill_sequence;
		char spill_file_name[64];
		std::snprintf(spill_file_name, sizeof(spill_file_name),
			"pg_yaap_seg_%u_%u_%u_%u", handle.arena_id, handle.segment_id,
			handle.generation, spill_sequence);
		store->SetSharedFileName(spill_file_name);
	}
	PG_TRY();
	{
		if (!has_shared_backing)
		{
			store->WriteBlock(&header, sizeof(header), payload, segment->used, extent);
			segment->spill_extent = *extent;
			if (previous_extent.file_name[0] != '\0' &&
				std::strcmp(previous_extent.file_name, extent->file_name) != 0)
				store->DeleteSharedFile(previous_extent.file_name);
		}
		dsa_free(dsa, segment->payload_dp);
		segment->payload_dp = InvalidDsaPointer;
		YaapBufferReleaseExternalBytes(buffer_state, segment->capacity);
		if (!has_shared_backing)
			YaapBufferRecordSpillBytes(buffer_state, extent->bytes);
		pg_atomic_write_u32(&segment->state,
						static_cast<uint32_t>(YaapSharedSegmentState::SPILLED));
	}
	PG_CATCH();
	{
		pg_atomic_write_u32(&segment->state,
						static_cast<uint32_t>(YaapSharedSegmentState::PUBLISHED));
		PG_RE_THROW();
	}
	PG_END_TRY();
	return true;
}

bool
YaapSharedSegmentReload(dsa_area *dsa, YaapBufferSharedState *buffer_state,
							 const YaapSegmentHandle &handle, dsa_pointer arena_dp,
							 YaapSpillStore *store,
							 const YaapSpillExtent &extent,
							 YaapSharedSegmentReadLease *lease)
{
	if (buffer_state == nullptr || store == nullptr)
		return false;
	auto *segment = YaapSharedSegmentResolve(dsa, arena_dp, handle);
	if (segment == nullptr)
		return false;
	const uint32_t state = pg_atomic_read_u32(&segment->state);
	bool allocated = false;
	volatile bool charged = false;
	if (state != static_cast<uint32_t>(YaapSharedSegmentState::SPILLED))
		return false;
	uint32_t expected = state;
	if (!pg_atomic_compare_exchange_u32(&segment->state, &expected,
		static_cast<uint32_t>(YaapSharedSegmentState::RELOADING)))
		return false;
	allocated = true;
	/* A segment may have been re-spilled between the caller reading its extent
	 * and winning the RELOADING transition. Never load bytes from a stale file
	 * extent into the current segment generation. */
	if (segment->spill_extent.fileno != extent.fileno ||
		segment->spill_extent.offset != extent.offset ||
		segment->spill_extent.bytes != extent.bytes ||
		std::strcmp(segment->spill_extent.file_name, extent.file_name) != 0)
	{
		pg_atomic_write_u32(&segment->state,
			static_cast<uint32_t>(YaapSharedSegmentState::SPILLED));
		return false;
	}
	bool reload_ok = true;
	PG_TRY();
	{
		if (allocated)
		{
			/* Reload is an expected pressure point.  Do not throw from here:
			 * callers already have a retry/failure path, and an ERROR longjmp
			 * from a worker during reload can strand the pipeline event. */
			if (!YaapBufferTryChargeExternalBytes(buffer_state, segment->capacity))
			{
				pg_atomic_write_u32(&segment->state,
					static_cast<uint32_t>(YaapSharedSegmentState::SPILLED));
				reload_ok = false;
			}
			else
			{
				charged = true;
				segment->payload_dp = dsa_allocate_extended(dsa, segment->capacity,
					DSA_ALLOC_ZERO | DSA_ALLOC_NO_OOM);
				if (!DsaPointerIsValid(segment->payload_dp))
				{
					YaapBufferReleaseExternalBytes(buffer_state, segment->capacity);
					charged = false;
					pg_atomic_write_u32(&segment->state,
						static_cast<uint32_t>(YaapSharedSegmentState::SPILLED));
					reload_ok = false;
				}
			}
		}
		if (reload_ok)
		{
			void *payload = dsa_get_address(dsa, segment->payload_dp);
			if (payload == nullptr)
				ereport(ERROR, (errmsg("pg_yaap: shared segment reload payload is missing")));
			YaapSegmentSpillImageHeader header{};
			if (extent.bytes < sizeof(header) || extent.bytes - sizeof(header) > segment->capacity)
				ereport(ERROR, (errmsg("pg_yaap: shared segment spill extent is oversized")));
			store->ReadBlock(extent, &header, sizeof(header), payload,
				static_cast<uint32_t>(extent.bytes - sizeof(header)));
			const uint64_t actual_checksum = header.payload_bytes <= segment->capacity
				? SegmentChecksum(payload, header.payload_bytes) : 0;
			if (header.magic != kSegmentSpillMagic || header.version != 1 ||
				header.arena_id != handle.arena_id || header.segment_id != handle.segment_id ||
				header.generation != handle.generation || header.payload_bytes > segment->capacity ||
				header.payload_bytes != extent.bytes - sizeof(header) ||
				header.checksum != actual_checksum)
				ereport(ERROR,
					(errmsg("pg_yaap: invalid shared segment spill image handle=(%u,%u,%u) header=(magic=%u version=%u arena=%u segment=%u generation=%u payload=%u checksum=%llu actual_checksum=%llu) extent=(file=%s offset=%llu bytes=%llu) segment_capacity=%u",
						handle.arena_id, handle.segment_id, handle.generation,
						header.magic, header.version, header.arena_id,
						header.segment_id, header.generation,
						header.payload_bytes,
						(unsigned long long) header.checksum,
						(unsigned long long) actual_checksum,
						extent.file_name,
						(unsigned long long) extent.offset,
						(unsigned long long) extent.bytes,
						segment->capacity)));
			segment->used = header.payload_bytes;
			if (lease != nullptr)
			{
				if (pg_atomic_read_u32(&segment->readers) != 0)
					elog(ERROR, "pg_yaap: spilled segment reload found existing readers");
				pg_atomic_write_u32(&segment->readers, 1);
				lease->handle = handle;
				lease->data = payload;
				lease->used = segment->used;
			}
			/* Publish only after the payload is fully read and checksum-validated.
			 * Readers that lose the reload CAS must never observe WRITABLE_LOCAL as
			 * permission to seal a segment whose image is still being restored. */
			pg_atomic_write_u32(&segment->state,
				static_cast<uint32_t>(YaapSharedSegmentState::PUBLISHED));
		}
	}
	PG_CATCH();
	{
		if (allocated && DsaPointerIsValid(segment->payload_dp))
		{
			dsa_free(dsa, segment->payload_dp);
			segment->payload_dp = InvalidDsaPointer;
		}
		if (charged)
			YaapBufferReleaseExternalBytes(buffer_state, segment->capacity);
		pg_atomic_write_u32(&segment->state,
			static_cast<uint32_t>(YaapSharedSegmentState::SPILLED));
		PG_RE_THROW();
	}
	PG_END_TRY();
	return reload_ok;
}

bool
YaapSharedSegmentGetSpillExtent(dsa_area *dsa, const YaapSegmentHandle &handle,
								dsa_pointer arena_dp, YaapSpillExtent *extent)
{
	if (extent == nullptr)
		return false;
	auto *segment = YaapSharedSegmentResolve(dsa, arena_dp, handle);
	if (segment == nullptr || pg_atomic_read_u32(&segment->state) !=
		static_cast<uint32_t>(YaapSharedSegmentState::SPILLED))
		return false;
	*extent = segment->spill_extent;
	return true;
}

void
YaapSharedSegmentChainInit(YaapSharedSegmentChain *chain,
							dsa_pointer arena_dp, uint32_t owner_worker)
{
	if (chain == nullptr || !DsaPointerIsValid(arena_dp))
		ereport(ERROR, (errmsg("pg_yaap: invalid shared segment chain")));
	*chain = YaapSharedSegmentChain{};
	chain->arena_dp = arena_dp;
	chain->owner_worker = owner_worker;
}

void *
YaapSharedSegmentChainAcquire(YaapSharedSegmentChain *chain, dsa_area *dsa,
							  YaapBufferSharedState *state, uint32_t required_bytes,
							  uint32_t *available_bytes)
{
	if (chain == nullptr || dsa == nullptr || state == nullptr || required_bytes == 0)
		ereport(ERROR, (errmsg("pg_yaap: invalid shared segment chain acquire")));
	auto *arena = YaapSharedSegmentArenaResolve(dsa, chain->arena_dp);
	if (arena == nullptr || required_bytes > arena->segment_size)
		ereport(ERROR, (errmsg("pg_yaap: shared segment chain request is too large")));
	YaapSharedSegmentDesc *segment = YaapSharedSegmentResolve(dsa, chain->arena_dp,
													 chain->active);
	if (segment == nullptr ||
		pg_atomic_read_u32(&segment->state) !=
		static_cast<uint32_t>(YaapSharedSegmentState::WRITABLE_LOCAL) ||
		segment->capacity - segment->used < required_bytes)
	{
		if (segment != nullptr)
		{
			if (pg_atomic_read_u32(&segment->state) ==
				static_cast<uint32_t>(YaapSharedSegmentState::WRITABLE_LOCAL))
			{
				YaapSharedSegmentSeal(segment);
				YaapSharedSegmentPublish(segment);
			}
		}
		const YaapSegmentHandle previous = chain->active;
		chain->active = YaapSharedSegmentArenaAllocate(dsa, state, chain->arena_dp,
																 chain->owner_worker);
			segment = YaapSharedSegmentResolve(dsa, chain->arena_dp, chain->active);
			if (segment == nullptr)
				ereport(ERROR, (errmsg("pg_yaap: shared segment successor missing")));
			if (previous.IsValid())
			{
				auto *previous_segment = YaapSharedSegmentResolve(dsa, chain->arena_dp, previous);
				if (previous_segment == nullptr)
					ereport(ERROR, (errmsg("pg_yaap: shared segment predecessor missing")));
				previous_segment->chain_next_dp = segment->self_dp;
			}
			else
				chain->first = chain->active;
			++chain->segment_count;
	}
	if (available_bytes != nullptr)
		*available_bytes = segment->capacity - segment->used;
	return static_cast<char *>(dsa_get_address(dsa, segment->payload_dp)) + segment->used;
}

void
YaapSharedSegmentChainCommit(YaapSharedSegmentChain *chain, dsa_area *dsa,
							 uint32_t used_bytes)
{
	if (chain == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: null shared segment chain")));
	auto *segment = YaapSharedSegmentResolve(dsa, chain->arena_dp, chain->active);
	if (segment == nullptr || used_bytes > segment->capacity - segment->used)
		ereport(ERROR, (errmsg("pg_yaap: shared segment chain commit exceeds window")));
	YaapSharedSegmentSetUsed(segment, segment->used + used_bytes);
}

void
YaapSharedSegmentChainSealPublish(YaapSharedSegmentChain *chain, dsa_area *dsa)
{
	if (chain == nullptr || !chain->active.IsValid())
		return;
	auto *segment = YaapSharedSegmentResolve(dsa, chain->arena_dp, chain->active);
	if (segment == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: shared segment chain active segment missing")));
	if (pg_atomic_read_u32(&segment->state) ==
		static_cast<uint32_t>(YaapSharedSegmentState::WRITABLE_LOCAL))
	{
		YaapSharedSegmentSeal(segment);
		YaapSharedSegmentPublish(segment);
	}
}

YaapSharedSegmentDesc *
YaapSharedSegmentChainFirst(dsa_area *dsa, const YaapSharedSegmentChain *chain)
{
	return chain == nullptr ? nullptr :
		YaapSharedSegmentResolve(dsa, chain->arena_dp, chain->first);
}

dsa_pointer
YaapSegmentPublicationDirectoryCreate(dsa_area *dsa, YaapBufferSharedState *state,
									  uint32_t operator_id, uint32_t partition_count,
									  uint32_t producer_count)
{
	if (dsa == nullptr || state == nullptr || operator_id == 0 ||
		partition_count == 0 || producer_count == 0)
		ereport(ERROR, (errmsg("pg_yaap: invalid segment publication directory parameters")));
	const uint64_t slot_count = static_cast<uint64_t>(producer_count) * partition_count;
	if (slot_count > SIZE_MAX / sizeof(YaapSegmentPublicationSlot))
		ereport(ERROR, (errmsg("pg_yaap: segment publication directory is too large")));
	const uint64_t metadata_bytes = sizeof(YaapSegmentPublicationDirectory) +
		slot_count * sizeof(YaapSegmentPublicationSlot);
	YaapBufferChargeExternalBytes(state, metadata_bytes, "segment-publication-metadata");
	volatile dsa_pointer directory_dp = InvalidDsaPointer;
	volatile dsa_pointer slots_dp = InvalidDsaPointer;
	PG_TRY();
	{
		directory_dp = dsa_allocate0(dsa, sizeof(YaapSegmentPublicationDirectory));
		slots_dp = dsa_allocate0(dsa, slot_count * sizeof(YaapSegmentPublicationSlot));
		auto *directory = static_cast<YaapSegmentPublicationDirectory *>(
			dsa_get_address(dsa, directory_dp));
		if (directory == nullptr)
			ereport(ERROR, (errmsg("pg_yaap: segment publication directory allocation failed")));
		directory->operator_id = operator_id;
		directory->partition_count = partition_count;
		directory->producer_count = producer_count;
		pg_atomic_init_u32(&directory->producers_remaining, producer_count);
		directory->slots_dp = slots_dp;
		auto *slots = static_cast<YaapSegmentPublicationSlot *>(dsa_get_address(dsa, slots_dp));
		for (uint64_t i = 0; i < slot_count; ++i)
		{
			pg_atomic_init_u32(&slots[i].state,
							static_cast<uint32_t>(YaapSegmentPublicationState::EMPTY));
			slots[i].owner_worker = UINT32_MAX;
			slots[i].partition_id = UINT32_MAX;
			slots[i].row_count = 0;
			slots[i].handle = YaapSegmentHandle{};
		}
	}
	PG_CATCH();
	{
		if (DsaPointerIsValid(slots_dp))
			dsa_free(dsa, slots_dp);
		if (DsaPointerIsValid(directory_dp))
			dsa_free(dsa, directory_dp);
		YaapBufferReleaseExternalBytes(state, metadata_bytes);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return directory_dp;
}

YaapSegmentPublicationDirectory *
YaapSegmentPublicationDirectoryResolve(dsa_area *dsa, dsa_pointer directory_dp)
{
	return dsa == nullptr || !DsaPointerIsValid(directory_dp) ? nullptr :
		static_cast<YaapSegmentPublicationDirectory *>(dsa_get_address(dsa, directory_dp));
}

static YaapSegmentPublicationSlot *
PublicationSlot(dsa_area *dsa, YaapSegmentPublicationDirectory *directory,
					uint32_t producer_id, uint32_t partition_id)
{
	if (directory == nullptr || producer_id >= directory->producer_count ||
		partition_id >= directory->partition_count)
		return nullptr;
	auto *slots = static_cast<YaapSegmentPublicationSlot *>(dsa_get_address(dsa, directory->slots_dp));
	return slots + static_cast<size_t>(producer_id) * directory->partition_count + partition_id;
}

bool
YaapSegmentPublicationPublish(dsa_area *dsa, dsa_pointer directory_dp,
							  uint32_t producer_id, uint32_t partition_id,
							  const YaapSegmentHandle &handle, uint32_t row_count)
{
	auto *directory = YaapSegmentPublicationDirectoryResolve(dsa, directory_dp);
	auto *slot = PublicationSlot(dsa, directory, producer_id, partition_id);
	if (slot == nullptr || !handle.IsValid())
		return false;
	uint32_t expected = static_cast<uint32_t>(YaapSegmentPublicationState::EMPTY);
	if (!pg_atomic_compare_exchange_u32(&slot->state, &expected,
										 static_cast<uint32_t>(YaapSegmentPublicationState::WRITING)))
		return false;
	slot->owner_worker = producer_id;
	slot->partition_id = partition_id;
	slot->row_count = row_count;
	slot->handle = handle;
	/* Metadata is written before the release-store publication. */
	pg_atomic_write_u32(&slot->state,
						static_cast<uint32_t>(YaapSegmentPublicationState::PUBLISHED));
	return true;
}

bool
YaapSegmentPublicationProducerDone(dsa_area *dsa, dsa_pointer directory_dp)
{
	auto *directory = YaapSegmentPublicationDirectoryResolve(dsa, directory_dp);
	if (directory == nullptr)
		return false;
	uint32_t remaining = pg_atomic_read_u32(&directory->producers_remaining);
	for (;;)
	{
		if (remaining == 0)
			return false;
		uint32_t expected = remaining;
		if (pg_atomic_compare_exchange_u32(&directory->producers_remaining,
											   &expected, remaining - 1))
			return true;
		remaining = expected;
	}
}

const YaapSegmentPublicationSlot *
YaapSegmentPublicationGet(dsa_area *dsa, dsa_pointer directory_dp,
							  uint32_t producer_id, uint32_t partition_id)
{
	auto *directory = YaapSegmentPublicationDirectoryResolve(dsa, directory_dp);
	auto *slot = PublicationSlot(dsa, directory, producer_id, partition_id);
	return slot != nullptr && pg_atomic_read_u32(&slot->state) ==
		static_cast<uint32_t>(YaapSegmentPublicationState::PUBLISHED) ? slot : nullptr;
}

bool
YaapSegmentPublicationConsume(dsa_area *dsa, dsa_pointer directory_dp,
							  uint32_t producer_id, uint32_t partition_id)
{
	auto *directory = YaapSegmentPublicationDirectoryResolve(dsa, directory_dp);
	auto *slot = PublicationSlot(dsa, directory, producer_id, partition_id);
	if (slot == nullptr)
		return false;
	uint32_t expected = static_cast<uint32_t>(YaapSegmentPublicationState::PUBLISHED);
	return pg_atomic_compare_exchange_u32(&slot->state, &expected,
		static_cast<uint32_t>(YaapSegmentPublicationState::CONSUMED));
}

void
YaapSegmentPublicationDirectoryDestroy(dsa_area *dsa, YaapBufferSharedState *state,
										dsa_pointer directory_dp)
{
	auto *directory = YaapSegmentPublicationDirectoryResolve(dsa, directory_dp);
	if (directory == nullptr)
		return;
	const uint64_t bytes = sizeof(YaapSegmentPublicationDirectory) +
		static_cast<uint64_t>(directory->producer_count) * directory->partition_count *
		sizeof(YaapSegmentPublicationSlot);
	if (DsaPointerIsValid(directory->slots_dp))
		dsa_free(dsa, directory->slots_dp);
	dsa_free(dsa, directory_dp);
	YaapBufferReleaseExternalBytes(state, bytes);
}

dsa_pointer
YaapSharedSegmentArenaCreate(dsa_area *dsa, YaapBufferSharedState *state,
							 uint32_t arena_id, uint32_t segment_size)
{
	if (dsa == nullptr || state == nullptr || arena_id == 0 || segment_size == 0)
		ereport(ERROR, (errmsg("pg_yaap: invalid shared segment arena parameters")));
	YaapBufferChargeExternalBytes(state, sizeof(YaapSharedSegmentArena), "segment-arena-metadata");
	dsa_pointer arena_dp = InvalidDsaPointer;
	PG_TRY();
	{
		arena_dp = dsa_allocate0(dsa, sizeof(YaapSharedSegmentArena));
	}
	PG_CATCH();
	{
		YaapBufferReleaseExternalBytes(state, sizeof(YaapSharedSegmentArena));
		PG_RE_THROW();
	}
	PG_END_TRY();
	auto *arena = static_cast<YaapSharedSegmentArena *>(dsa_get_address(dsa, arena_dp));
	if (arena == nullptr)
	{
		YaapBufferReleaseExternalBytes(state, sizeof(YaapSharedSegmentArena));
		ereport(ERROR, (errmsg("pg_yaap: shared segment arena metadata allocation failed")));
	}
	arena->arena_id = arena_id;
	SpinLockInit(&arena->mutex);
	arena->segment_size = segment_size;
	const size_t index_bytes =
		static_cast<size_t>(kSegmentIndexRootEntries) * sizeof(dsa_pointer);
	YaapBufferChargeExternalBytes(state, index_bytes, "shared-segment-index-root");
	volatile dsa_pointer index_dp = InvalidDsaPointer;
	PG_TRY();
	{
		index_dp = dsa_allocate0(dsa, index_bytes);
	}
	PG_CATCH();
	{
		YaapBufferReleaseExternalBytes(state, index_bytes);
		dsa_free(dsa, arena_dp);
		YaapBufferReleaseExternalBytes(state, sizeof(YaapSharedSegmentArena));
		PG_RE_THROW();
	}
	PG_END_TRY();
	arena->segment_index_root_dp = index_dp;
	arena->segment_index_bytes = 0;
	arena->segment_count = 0;
	arena->free_segment_count = 0;
	arena->next_segment_id = 0;
	arena->self_dp = arena_dp;
	arena->first_segment_dp = InvalidDsaPointer;
	arena->last_segment_dp = InvalidDsaPointer;
	arena->eviction_cursor_dp = InvalidDsaPointer;
	arena->free_segment_dp = InvalidDsaPointer;
	return arena_dp;
}

YaapSharedSegmentArena *
YaapSharedSegmentArenaResolve(dsa_area *dsa, dsa_pointer arena_dp)
{
	return dsa == nullptr || !DsaPointerIsValid(arena_dp) ? nullptr :
		static_cast<YaapSharedSegmentArena *>(dsa_get_address(dsa, arena_dp));
}

YaapSharedSegmentDesc *
YaapSharedSegmentResolve(dsa_area *dsa, dsa_pointer arena_dp,
						 const YaapSegmentHandle &handle)
{
	auto *arena = YaapSharedSegmentArenaResolve(dsa, arena_dp);
	if (arena == nullptr || handle.arena_id != arena->arena_id || !handle.IsValid())
		return nullptr;
	const uint32_t root_slot = handle.segment_id / kSegmentIndexLeafEntries;
	const uint32_t leaf_slot = handle.segment_id % kSegmentIndexLeafEntries;
	if (root_slot >= kSegmentIndexRootEntries ||
		!DsaPointerIsValid(arena->segment_index_root_dp))
		return nullptr;
	auto *root = static_cast<const dsa_pointer *>(
		dsa_get_address(dsa, arena->segment_index_root_dp));
	if (root == nullptr || !DsaPointerIsValid(root[root_slot]))
		return nullptr;
	auto *leaf = static_cast<const dsa_pointer *>(
		dsa_get_address(dsa, root[root_slot]));
	if (leaf == nullptr || !DsaPointerIsValid(leaf[leaf_slot]))
		return nullptr;
	auto *segment = ResolveDesc(dsa, leaf[leaf_slot]);
	return segment != nullptr && segment->segment_id == handle.segment_id &&
		segment->generation == handle.generation ? segment : nullptr;
}

YaapSegmentHandle
YaapSharedSegmentArenaAllocate(dsa_area *dsa, YaapBufferSharedState *state,
							   dsa_pointer arena_dp, uint32_t owner_worker,
							   uint64_t owner_tag)
{
	auto *arena = YaapSharedSegmentArenaResolve(dsa, arena_dp);
	if (arena == nullptr || state == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: invalid shared segment allocation state")));
	YaapSharedSegmentDesc *segment = nullptr;
	SpinLockAcquire(&arena->mutex);
	if (DsaPointerIsValid(arena->free_segment_dp))
	{
		segment = ResolveDesc(dsa, arena->free_segment_dp);
		if (segment == nullptr || arena->free_segment_count == 0)
		{
			SpinLockRelease(&arena->mutex);
			ereport(ERROR, (errmsg("pg_yaap: shared segment free-list is corrupt")));
		}
		arena->free_segment_dp = segment->free_next_dp;
		--arena->free_segment_count;
		segment->free_next_dp = InvalidDsaPointer;
		segment->owner_worker = owner_worker;
		segment->owner_tag = owner_tag;
		segment->used = 0;
		segment->spill_sequence = 0;
		segment->chain_next_dp = InvalidDsaPointer;
		std::memset(&segment->spill_extent, 0, sizeof(segment->spill_extent));
		const bool needs_payload = !DsaPointerIsValid(segment->payload_dp);
		pg_atomic_write_u32(&segment->readers, 0);
		pg_atomic_write_u32(&segment->state,
							static_cast<uint32_t>(YaapSharedSegmentState::WRITABLE_LOCAL));
		SpinLockRelease(&arena->mutex);
		if (needs_payload)
		{
			const uint64_t bytes = segment->capacity;
			volatile bool charged = false;
			PG_TRY();
			{
				char purpose[64];
				std::snprintf(purpose, sizeof(purpose),
					"shared-segment-reuse arena=%u", arena->arena_id);
				YaapBufferChargeExternalBytes(state, bytes, purpose);
				charged = true;
				segment->payload_dp = dsa_allocate0(dsa, segment->capacity);
			}
			PG_CATCH();
			{
				if (charged)
					YaapBufferReleaseExternalBytes(state, bytes);
				SpinLockAcquire(&arena->mutex);
				segment->free_next_dp = arena->free_segment_dp;
				arena->free_segment_dp = segment->self_dp;
				++arena->free_segment_count;
				pg_atomic_write_u32(&segment->state,
					static_cast<uint32_t>(YaapSharedSegmentState::FREE));
				SpinLockRelease(&arena->mutex);
				PG_RE_THROW();
			}
			PG_END_TRY();
		}
		return HandleFor(segment);
	}
	SpinLockRelease(&arena->mutex);
	segment = AllocateNew(dsa, state, arena, arena_dp, owner_worker, owner_tag, false);
	pg_atomic_write_u32(&segment->state,
		static_cast<uint32_t>(YaapSharedSegmentState::WRITABLE_LOCAL));
	return HandleFor(segment);
}

void *
YaapSharedSegmentWritableAddress(dsa_area *dsa, YaapSharedSegmentDesc *segment)
{
	if (segment == nullptr || pg_atomic_read_u32(&segment->state) !=
		static_cast<uint32_t>(YaapSharedSegmentState::WRITABLE_LOCAL))
		return nullptr;
	return dsa_get_address(dsa, segment->payload_dp);
}

void
YaapSharedSegmentSetUsed(YaapSharedSegmentDesc *segment, uint32_t used)
{
	if (segment == nullptr || used > segment->capacity)
		ereport(ERROR, (errmsg("pg_yaap: shared segment used bytes exceed capacity")));
	segment->used = used;
}

void
YaapSharedSegmentSeal(YaapSharedSegmentDesc *segment)
{
	if (segment == nullptr || pg_atomic_read_u32(&segment->state) !=
		static_cast<uint32_t>(YaapSharedSegmentState::WRITABLE_LOCAL))
		ereport(ERROR, (errmsg("pg_yaap: shared segment seal requires writable state")));
	pg_atomic_write_u32(&segment->state,
						static_cast<uint32_t>(YaapSharedSegmentState::SEALED_LOCAL));
}

void
YaapSharedSegmentPublish(YaapSharedSegmentDesc *segment)
{
	if (segment == nullptr || pg_atomic_read_u32(&segment->state) !=
		static_cast<uint32_t>(YaapSharedSegmentState::SEALED_LOCAL))
		ereport(ERROR, (errmsg("pg_yaap: shared segment publish requires sealed state")));
	pg_atomic_write_u32(&segment->state,
						static_cast<uint32_t>(YaapSharedSegmentState::PUBLISHED));
}

bool
YaapSharedSegmentAcquireRead(dsa_area *dsa, dsa_pointer arena_dp,
							 const YaapSegmentHandle &handle,
							 YaapSharedSegmentReadLease *lease)
{
	if (lease == nullptr)
		return false;
	*lease = YaapSharedSegmentReadLease{};
	auto *segment = YaapSharedSegmentResolve(dsa, arena_dp, handle);
	if (segment == nullptr || segment->generation != handle.generation ||
		pg_atomic_read_u32(&segment->state) !=
			static_cast<uint32_t>(YaapSharedSegmentState::PUBLISHED))
		return false;
	/* Readers only contend on this atomic refcount, not on an arena-wide
	 * exclusive lock. A spiller/retirer first changes the state, then checks
	 * readers; the second validation below closes the race where that transition
	 * wins just before our increment. */
	pg_atomic_add_fetch_u32(&segment->readers, 1);
	if (segment->generation != handle.generation ||
		pg_atomic_read_u32(&segment->state) !=
			static_cast<uint32_t>(YaapSharedSegmentState::PUBLISHED))
	{
		pg_atomic_sub_fetch_u32(&segment->readers, 1);
		return false;
	}
	lease->handle = handle;
	lease->data = dsa_get_address(dsa, segment->payload_dp);
	lease->used = segment->used;
	if (lease->data == nullptr)
	{
		pg_atomic_sub_fetch_u32(&segment->readers, 1);
		*lease = YaapSharedSegmentReadLease{};
	}
	return lease->data != nullptr;
}

void
YaapSharedSegmentReleaseRead(dsa_area *dsa, dsa_pointer arena_dp,
							 YaapSharedSegmentReadLease *lease)
{
	if (lease == nullptr || !lease->IsValid())
		return;
	auto *segment = YaapSharedSegmentResolve(dsa, arena_dp, lease->handle);
	if (segment != nullptr)
		pg_atomic_sub_fetch_u32(&segment->readers, 1);
	*lease = YaapSharedSegmentReadLease{};
}

bool
YaapSharedSegmentRetire(dsa_area *dsa, YaapBufferSharedState *state,
							dsa_pointer arena_dp, const YaapSegmentHandle &handle,
							bool discard_payload)
{
	auto *segment = YaapSharedSegmentResolve(dsa, arena_dp, handle);
	auto *arena = YaapSharedSegmentArenaResolve(dsa, arena_dp);
	if (segment == nullptr || arena == nullptr || state == nullptr)
		return false;

	SpinLockAcquire(&arena->mutex);
	if (segment->generation != handle.generation || pg_atomic_read_u32(&segment->readers) != 0)
	{
		SpinLockRelease(&arena->mutex);
		return false;
	}
	uint32_t expected = static_cast<uint32_t>(YaapSharedSegmentState::PUBLISHED);
	const bool spilled = pg_atomic_read_u32(&segment->state) ==
		static_cast<uint32_t>(YaapSharedSegmentState::SPILLED);
	if (spilled)
		expected = static_cast<uint32_t>(YaapSharedSegmentState::SPILLED);
	if (!pg_atomic_compare_exchange_u32(&segment->state, &expected,
			static_cast<uint32_t>(YaapSharedSegmentState::RETIRED)))
	{
		SpinLockRelease(&arena->mutex);
		return false;
	}
	/* A spilled segment has no resident payload and its handle may still be
	 * present in a reader's partition directory. Do not recycle its descriptor
	 * within the query: generation alone cannot protect a stale reader that
	 * races reload/retire across phases. Query teardown reclaims it safely. */
	if (spilled)
	{
		SpinLockRelease(&arena->mutex);
		return true;
	}
	if (discard_payload)
	{
		/* Completed partition pairs will never read this image again. Claim
		 * RETIRED before dropping the payload, so a competing evictor cannot
		 * spill or reload it. DSA freeing must happen outside the spinlock. */
		const dsa_pointer payload_dp = segment->payload_dp;
		segment->payload_dp = InvalidDsaPointer;
		SpinLockRelease(&arena->mutex);
		if (DsaPointerIsValid(payload_dp))
		{
			dsa_free(dsa, payload_dp);
			YaapBufferReleaseExternalBytes(state, segment->capacity);
		}
		return true;
	}
	segment->free_next_dp = arena->free_segment_dp;
	arena->free_segment_dp = segment->self_dp;
	++arena->free_segment_count;
	++segment->generation;
	if (segment->generation == 0)
		segment->generation = 1;
	pg_atomic_write_u32(&segment->state,
						 static_cast<uint32_t>(YaapSharedSegmentState::FREE));
	SpinLockRelease(&arena->mutex);
	(void) state;
	return true;
}

void
YaapSharedSegmentArenaDestroy(dsa_area *dsa, YaapBufferSharedState *state,
							  dsa_pointer arena_dp)
{
	auto *arena = YaapSharedSegmentArenaResolve(dsa, arena_dp);
	if (arena == nullptr)
		return;
	for (dsa_pointer dp = arena->first_segment_dp; DsaPointerIsValid(dp);)
	{
		auto *segment = ResolveDesc(dsa, dp);
		if (segment == nullptr)
			break;
		const dsa_pointer next = segment->next_dp;
		const bool payload_resident = DsaPointerIsValid(segment->payload_dp);
		/* DestroyRuntimeDsm is called only after the worker event loop has
		 * stopped.  A cancelled/erroring operator may therefore leave a
		 * published segment behind; teardown is the final owner and must reclaim
		 * it instead of raising a second ERROR while unwinding the original one.
		 * Normal operator paths still use Retire(), which validates readers and
		 * the published state before returning a segment to the free list. */
		if (payload_resident)
		{
			dsa_free(dsa, segment->payload_dp);
			YaapBufferReleaseExternalBytes(state, segment->capacity);
		}
		dsa_free(dsa, dp);
		YaapBufferReleaseExternalBytes(state, sizeof(YaapSharedSegmentDesc));
		dp = next;
	}
	const size_t root_bytes =
		static_cast<size_t>(kSegmentIndexRootEntries) * sizeof(dsa_pointer);
	auto *root = static_cast<dsa_pointer *>(dsa_get_address(dsa, arena->segment_index_root_dp));
	if (root != nullptr)
	{
		for (uint32_t i = 0; i < kSegmentIndexRootEntries; ++i)
			if (DsaPointerIsValid(root[i]))
				dsa_free(dsa, root[i]);
	}
	dsa_free(dsa, arena->segment_index_root_dp);
	dsa_free(dsa, arena_dp);
	YaapBufferReleaseExternalBytes(state, root_bytes + arena->segment_index_bytes);
	YaapBufferReleaseExternalBytes(state, sizeof(YaapSharedSegmentArena));
}

}  /* namespace buffer */
}  /* namespace pg_yaap */
