extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include "buffer_manager/yaap_segmented_tdc.hpp"

#include <cstring>

namespace pg_yaap {
namespace buffer {

using namespace pipeline;

namespace {

static constexpr uint32_t kTdcSpillMagic = 0x59415444; /* YATD */

static uint64_t
TdcChecksum(const void *data, size_t bytes)
{
	const auto *ptr = static_cast<const unsigned char *>(data);
	uint64_t hash = UINT64_C(1469598103934665603);
	for (size_t i = 0; i < bytes; ++i)
	{
		hash ^= ptr[i];
		hash *= UINT64_C(1099511628211);
	}
	return hash;
}

static YaapSharedSegmentDesc *
Resolve(dsa_area *dsa, dsa_pointer arena_dp, const YaapSegmentHandle &handle)
{
	return YaapSharedSegmentResolve(dsa, arena_dp, handle);
}

static TupleDataCollection *
ResolveStore(dsa_area *dsa, dsa_pointer arena_dp, const YaapSegmentHandle &handle)
{
	auto *segment = Resolve(dsa, arena_dp, handle);
	return segment == nullptr ? nullptr : static_cast<TupleDataCollection *>(
		dsa_get_address(dsa, segment->payload_dp));
}

static void
AllocateActive(dsa_area *dsa, YaapBufferSharedState *state, YaapSegmentedTdc *tdc)
{
	const YaapSegmentHandle previous = tdc->active;
	const auto handle = YaapSharedSegmentArenaAllocate(dsa, state, tdc->arena_dp,
		tdc->owner_worker, tdc->owner_tag);
	auto *segment = Resolve(dsa, tdc->arena_dp, handle);
	if (segment == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: segmented TDC successor is missing")));
	const uint64_t required = offsetof(TupleDataCollection, rows) +
		static_cast<uint64_t>(tdc->row_capacity) * tdc->row_width + tdc->heap_capacity;
	if (required > segment->capacity)
		ereport(ERROR, (errmsg("pg_yaap: segmented TDC page layout exceeds segment capacity")));
	auto *store = static_cast<TupleDataCollection *>(dsa_get_address(dsa, segment->payload_dp));
	TupleDataCollectionInit(store, tdc->row_capacity, tdc->row_width,
		tdc->layout_dp, tdc->heap_capacity);
	if (!tdc->first.IsValid())
		tdc->first = handle;
	if (previous.IsValid())
	{
		auto *previous_segment = Resolve(dsa, tdc->arena_dp, previous);
		if (previous_segment == nullptr)
			ereport(ERROR, (errmsg("pg_yaap: segmented TDC predecessor is missing")));
		previous_segment->chain_next_dp = segment->self_dp;
	}
	tdc->active = handle;
	++tdc->segment_count;
}

static void
SealActive(dsa_area *dsa, YaapSegmentedTdc *tdc)
{
	if (tdc == nullptr || !tdc->active.IsValid())
		return;
	auto *segment = Resolve(dsa, tdc->arena_dp, tdc->active);
	if (segment == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: segmented TDC active segment is missing")));
	/* Spill keeps the active handle as the chain predecessor so a later
	 * append can link a fresh successor.  Its payload is intentionally gone,
	 * so never dereference payload_dp for an already sealed/spilled segment. */
	if (pg_atomic_read_u32(&segment->state) !=
		static_cast<uint32_t>(YaapSharedSegmentState::WRITABLE_LOCAL))
		return;
	auto *store = static_cast<TupleDataCollection *>(dsa_get_address(dsa, segment->payload_dp));
	if (store == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: segmented TDC active store is missing")));
	const uint32_t heap_used = pg_atomic_read_u32(&store->heap_used);
	/* The heap begins after the reserved row area, not after row_count.
	 * A partial tail with strings must retain that gap in its raw image. */
	const uint64_t used = offsetof(TupleDataCollection, rows) +
		static_cast<uint64_t>(heap_used > 0 ? store->row_capacity :
			pg_atomic_read_u32(&store->row_count)) * store->row_width + heap_used;
	if (used > segment->capacity)
		ereport(ERROR, (errmsg("pg_yaap: segmented TDC used bytes exceed segment capacity")));
	segment->used = static_cast<uint32_t>(used);
	const uint32_t state = pg_atomic_read_u32(&segment->state);
	if (state == static_cast<uint32_t>(YaapSharedSegmentState::WRITABLE_LOCAL))
	{
		YaapSharedSegmentSeal(segment);
		YaapSharedSegmentPublish(segment);
	}
}

}  /* namespace */

bool
YaapSegmentedTdcSpill(const TupleDataCollection *tdc, YaapSpillStore *store,
						  YaapSpillExtent *extent)
{
	if (tdc == nullptr || store == nullptr || extent == nullptr)
		return false;
	const uint32_t row_count = pg_atomic_read_u32(
		const_cast<pg_atomic_uint32 *>(&tdc->row_count));
	const uint32_t heap_used = pg_atomic_read_u32(
		const_cast<pg_atomic_uint32 *>(&tdc->heap_used));
	const size_t row_bytes = static_cast<size_t>(row_count) * tdc->row_width;
	const size_t payload_bytes = row_bytes + heap_used;
	unsigned char *payload = static_cast<unsigned char *>(
		MemoryContextAlloc(CurrentMemoryContext, payload_bytes > 0 ? payload_bytes : 1));
	if (row_bytes > 0)
		std::memcpy(payload, tdc->rows, row_bytes);
	if (heap_used > 0)
		std::memcpy(payload + row_bytes, TupleDataCollectionHeapConst(tdc), heap_used);
	YaapTdcSpillImageHeader header{
		kTdcSpillMagic, 1, 0, tdc->row_width, tdc->row_capacity,
		tdc->heap_capacity, row_count, heap_used,
		TdcChecksum(payload, payload_bytes)};
	store->WriteBlock(&header, sizeof(header), payload, payload_bytes, extent);
	pfree(payload);
	return true;
}

TupleDataCollection *
YaapSegmentedTdcReload(const YaapSpillExtent &extent, YaapSpillStore *store,
						   MemoryContext context, dsa_pointer layout_dp)
{
	if (store == nullptr || context == nullptr || extent.bytes < sizeof(YaapTdcSpillImageHeader))
		return nullptr;
	MemoryContext old_context = MemoryContextSwitchTo(context);
	YaapTdcSpillImageHeader header{};
	const size_t payload_bytes = extent.bytes - sizeof(header);
	void *payload = MemoryContextAlloc(context, payload_bytes > 0 ? payload_bytes : 1);
	store->ReadBlock(extent, &header, sizeof(header), payload, payload_bytes);
	if (header.magic != kTdcSpillMagic || header.version != 1 ||
		header.row_count > header.row_capacity || header.heap_used > header.heap_capacity ||
		payload_bytes != static_cast<size_t>(header.row_count) * header.row_width + header.heap_used ||
		header.checksum != TdcChecksum(payload, payload_bytes))
	{
		MemoryContextSwitchTo(old_context);
		ereport(ERROR, (errmsg("pg_yaap: invalid TDC spill image")));
	}
	const size_t alloc_size = TupleDataCollectionAllocSize(header.row_capacity,
		header.row_width, header.heap_capacity);
	auto *tdc = static_cast<TupleDataCollection *>(MemoryContextAllocZero(context, alloc_size));
	TupleDataCollectionInit(tdc, header.row_capacity, header.row_width, layout_dp,
		header.heap_capacity);
	const size_t row_bytes = static_cast<size_t>(header.row_count) * header.row_width;
	if (row_bytes > 0)
		std::memcpy(tdc->rows, payload, row_bytes);
	if (header.heap_used > 0)
		std::memcpy(TupleDataCollectionHeap(tdc), static_cast<unsigned char *>(payload) + row_bytes,
			header.heap_used);
	pg_atomic_write_u32(&tdc->row_count, header.row_count);
	pg_atomic_write_u32(&tdc->heap_used, header.heap_used);
	pfree(payload);
	MemoryContextSwitchTo(old_context);
	return tdc;
}

void
YaapSegmentedTdcInit(YaapSegmentedTdc *tdc, dsa_pointer arena_dp,
						dsa_pointer layout_dp, uint32_t owner_worker,
								 uint32_t row_width, uint32_t row_capacity,
								 uint32_t heap_capacity, uint64_t owner_tag,
								 const char *purpose)
{
	if (tdc == nullptr || !DsaPointerIsValid(arena_dp) || row_width == 0 || row_capacity == 0)
		ereport(ERROR, (errmsg("pg_yaap: invalid segmented TDC parameters for %s (arena=%llu row_width=%u row_capacity=%u owner=%u)",
			purpose != nullptr ? purpose : "unspecified",
			(unsigned long long) arena_dp, row_width, row_capacity, owner_worker)));
	*tdc = YaapSegmentedTdc{};
	tdc->arena_dp = arena_dp;
	tdc->layout_dp = layout_dp;
	tdc->owner_worker = owner_worker;
	tdc->owner_tag = owner_tag;
	tdc->row_width = row_width;
	tdc->row_capacity = row_capacity;
	tdc->heap_capacity = heap_capacity;
}

bool
YaapSegmentedTdcAppend(dsa_area *dsa, YaapBufferSharedState *state,
						  YaapSegmentedTdc *tdc, YaapSegmentedRowHandle *handle,
						  TupleDataCollection **row_store, uint8_t **row)

{
	return YaapSegmentedTdcAppendWithHeap(dsa, state, tdc, 0, handle, row_store, row);
}

bool
YaapSegmentedTdcAppendWithHeap(dsa_area *dsa, YaapBufferSharedState *state,
							   YaapSegmentedTdc *tdc, uint32_t required_heap_bytes,
							   YaapSegmentedRowHandle *handle,
							   TupleDataCollection **row_store, uint8_t **row)
{
	if (dsa == nullptr || state == nullptr || tdc == nullptr || handle == nullptr || row == nullptr)
		return false;
	if (!tdc->active.IsValid())
		AllocateActive(dsa, state, tdc);
	else
	{
		auto *active_segment = Resolve(dsa, tdc->arena_dp, tdc->active);
		if (active_segment == nullptr)
			return false;
		/* A reconstructed writer may point at the previously published tail.
		 * Never append into a segment visible to readers; continue the stable
		 * chain with a fresh writable successor instead. */
		if (pg_atomic_read_u32(&active_segment->state) !=
			static_cast<uint32_t>(YaapSharedSegmentState::WRITABLE_LOCAL))
			AllocateActive(dsa, state, tdc);
	}
	auto *store = ResolveStore(dsa, tdc->arena_dp, tdc->active);
	if (store == nullptr)
		return false;
	if (required_heap_bytes > store->heap_capacity)
	{
		/* A single varlena value may be larger than the initial heap grant.
		 * Grow the writer's heap geometry before allocating the successor; the
		 * arena capacity check in AllocateActive remains the hard bound. */
		SealActive(dsa, tdc);
		tdc->heap_capacity = std::max(required_heap_bytes,
			tdc->heap_capacity > UINT32_MAX / 2 ? UINT32_MAX : tdc->heap_capacity * 2u);
		AllocateActive(dsa, state, tdc);
		store = ResolveStore(dsa, tdc->arena_dp, tdc->active);
	}
	if (store == nullptr || required_heap_bytes > store->heap_capacity)
		return false;
	if (pg_atomic_read_u32(&store->row_count) >= store->row_capacity ||
		!TupleDataCollectionHasSpaceForAppend(store, required_heap_bytes))
	{
		SealActive(dsa, tdc);
		AllocateActive(dsa, state, tdc);
		store = ResolveStore(dsa, tdc->arena_dp, tdc->active);
	}
	if (store == nullptr)
		return false;
	uint8_t *out = nullptr;
	const uint32_t row_index = TupleDataCollectionAppendRow(store, &out);
	if (row_index == TDC_INVALID_ROW_INDEX)
		return false;
	*handle = YaapSegmentedRowHandle{tdc->active, row_index};
	if (row_store != nullptr)
		*row_store = store;
	*row = out;
	++tdc->row_count;
	return true;
}

TupleDataCollection *
YaapSegmentedTdcResolveStore(dsa_area *dsa, const YaapSegmentedRowHandle &handle,
							 dsa_pointer arena_dp)
{
	return handle.IsValid() ? ResolveStore(dsa, arena_dp, handle.segment) : nullptr;
}

const uint8_t *
YaapSegmentedTdcResolveRow(dsa_area *dsa, const YaapSegmentedRowHandle &handle,
							   dsa_pointer arena_dp)
{
	auto *store = YaapSegmentedTdcResolveStore(dsa, handle, arena_dp);
	if (store == nullptr || handle.row_index >=
		pg_atomic_read_u32(&store->row_count))
		return nullptr;
	return TupleDataCollectionGetRowConst(store, handle.row_index);
}

void
YaapSegmentedTdcSealPublish(dsa_area *dsa, YaapSegmentedTdc *tdc)
{
	if (tdc == nullptr || !tdc->active.IsValid())
		return;
	SealActive(dsa, tdc);
}

void
YaapSegmentedTdcDestroy(dsa_area *dsa, YaapBufferSharedState *state,
							YaapSegmentedTdc *tdc)
{
	if (dsa == nullptr || state == nullptr || tdc == nullptr ||
		!DsaPointerIsValid(tdc->arena_dp))
		return;
	auto *segment = tdc->first.IsValid()
		? Resolve(dsa, tdc->arena_dp, tdc->first) : nullptr;
	while (segment != nullptr)
	{
		const dsa_pointer next_dp = segment->chain_next_dp;
		const YaapSegmentHandle handle{segment->arena_id, segment->segment_id,
			segment->generation};
		if (pg_atomic_read_u32(&segment->state) ==
			static_cast<uint32_t>(YaapSharedSegmentState::WRITABLE_LOCAL))
		{
			YaapSharedSegmentSeal(segment);
			YaapSharedSegmentPublish(segment);
		}
		if (!YaapSharedSegmentRetire(dsa, state, tdc->arena_dp, handle))
			ereport(ERROR, (errmsg("pg_yaap: segmented TDC destroy failed")));
		segment = DsaPointerIsValid(next_dp)
			? static_cast<YaapSharedSegmentDesc *>(dsa_get_address(dsa, next_dp)) : nullptr;
	}
	*tdc = YaapSegmentedTdc{};
}

dsa_pointer
YaapSegmentedTdcPublishDesc(dsa_area *dsa, const YaapSegmentedTdc &tdc)
{
	if (dsa == nullptr || !tdc.first.IsValid() || !DsaPointerIsValid(tdc.arena_dp))
		return InvalidDsaPointer;
	auto *first = Resolve(dsa, tdc.arena_dp, tdc.first);
	if (first == nullptr)
		return InvalidDsaPointer;
	dsa_pointer desc_dp = dsa_allocate0(dsa, sizeof(YaapSegmentedTdcSharedDesc));
	auto *desc = static_cast<YaapSegmentedTdcSharedDesc *>(dsa_get_address(dsa, desc_dp));
	desc->arena_dp = tdc.arena_dp;
	desc->first_desc_dp = first->self_dp;
	desc->layout_dp = tdc.layout_dp;
	desc->row_width = tdc.row_width;
	desc->row_capacity = tdc.row_capacity;
	desc->heap_capacity = tdc.heap_capacity;
	desc->row_count = tdc.row_count;
	return desc_dp;
}

const YaapSegmentedTdcSharedDesc *
YaapSegmentedTdcResolveDesc(dsa_area *dsa, dsa_pointer desc_dp)
{
	return dsa == nullptr || !DsaPointerIsValid(desc_dp) ? nullptr :
		static_cast<const YaapSegmentedTdcSharedDesc *>(dsa_get_address(dsa, desc_dp));
}

void
YaapSegmentedTdcReaderInitShared(YaapSegmentedTdcReader *reader, dsa_area *dsa,
								 const YaapSegmentedTdcSharedDesc *desc)
{
	if (reader == nullptr)
		return;
	*reader = YaapSegmentedTdcReader{};
	if (desc == nullptr || dsa == nullptr)
		return;
	reader->arena_dp = desc->arena_dp;
	auto *first = static_cast<YaapSharedSegmentDesc *>(
		dsa_get_address(dsa, desc->first_desc_dp));
	if (first == nullptr)
		return;
	reader->current_desc_dp = desc->first_desc_dp;
	reader->current_segment = YaapSegmentHandle{first->arena_id, first->segment_id,
		first->generation};
	if (!YaapSharedSegmentAcquireRead(dsa, reader->arena_dp,
		reader->current_segment, &reader->lease))
		*reader = YaapSegmentedTdcReader{};
}

void
YaapSegmentedTdcReaderInit(YaapSegmentedTdcReader *reader,
						   dsa_area *dsa, dsa_pointer arena_dp, const YaapSegmentedTdc &tdc)
{
	if (reader == nullptr)
		return;
	*reader = YaapSegmentedTdcReader{};
	reader->arena_dp = arena_dp;
	reader->current_segment = tdc.first;
	if (tdc.first.IsValid())
	{
		auto *segment = YaapSharedSegmentResolve(dsa, arena_dp, tdc.first);
		reader->current_desc_dp = segment != nullptr ? segment->self_dp : InvalidDsaPointer;
		if (segment != nullptr && !YaapSharedSegmentAcquireRead(dsa, arena_dp,
			tdc.first, &reader->lease))
			reader->current_desc_dp = InvalidDsaPointer;
	}
}

bool
YaapSegmentedTdcReaderNext(dsa_area *dsa, YaapSegmentedTdcReader *reader,
							YaapSegmentedRowHandle *handle,
							TupleDataCollection **store, const uint8_t **row)
{
	if (reader == nullptr || handle == nullptr || store == nullptr || row == nullptr ||
		!reader->current_segment.IsValid())
		return false;
	auto *segment = reader->current_desc_dp != InvalidDsaPointer
		? static_cast<YaapSharedSegmentDesc *>(dsa_get_address(dsa, reader->current_desc_dp))
		: nullptr;
	if (segment == nullptr)
		return false;
	if (segment->arena_id != reader->current_segment.arena_id ||
		segment->segment_id != reader->current_segment.segment_id ||
		segment->generation != reader->current_segment.generation)
		return false;
	auto *tdc = static_cast<TupleDataCollection *>(dsa_get_address(dsa, segment->payload_dp));
	if (reader->row_index >= pg_atomic_read_u32(&tdc->row_count))
	{
		YaapSharedSegmentReleaseRead(dsa, reader->arena_dp, &reader->lease);
		if (!DsaPointerIsValid(segment->chain_next_dp))
			return false;
		segment = static_cast<YaapSharedSegmentDesc *>(dsa_get_address(dsa,
			segment->chain_next_dp));
		if (segment == nullptr)
			return false;
		reader->current_segment = YaapSegmentHandle{segment->arena_id,
			segment->segment_id, segment->generation};
		reader->current_desc_dp = segment->self_dp;
		if (!YaapSharedSegmentAcquireRead(dsa, reader->arena_dp,
			reader->current_segment, &reader->lease))
			return false;
		reader->row_index = 0;
		tdc = static_cast<TupleDataCollection *>(dsa_get_address(dsa, segment->payload_dp));
	}
	if (tdc == nullptr || reader->row_index >= pg_atomic_read_u32(&tdc->row_count))
		return false;
	*handle = YaapSegmentedRowHandle{reader->current_segment, reader->row_index};
	*store = tdc;
	*row = TupleDataCollectionGetRowConst(tdc, reader->row_index++);
	return true;
}

void
YaapSegmentedTdcReaderClose(dsa_area *dsa, YaapSegmentedTdcReader *reader)
{
	if (reader == nullptr)
		return;
	YaapSharedSegmentReleaseRead(dsa, reader->arena_dp, &reader->lease);
	*reader = YaapSegmentedTdcReader{};
}

}  /* namespace buffer */
}  /* namespace pg_yaap */
