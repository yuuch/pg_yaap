#include "parallel/pipeline/physical_hash_aggregate.hpp"
#include "parallel/pipeline/physical_perfect_hash_aggregate.hpp"

extern "C" {
#include "postgres.h"
#include "catalog/pg_type_d.h"
#include "utils/elog.h"

extern int pg_yaap_parallel_max_workers;
extern bool pg_yaap_trace_hooks;
}

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstring>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/cancel.hpp"
#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"
#include "parallel/pipeline/types.hpp"
#include "buffer_manager/yaap_shared_segment_arena.hpp"
#include "buffer_manager/yaap_segment_row_refs.hpp"
#include "buffer_manager/yaap_segmented_tdc.hpp"
#include "buffer_manager/yaap_spill_store.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

static constexpr int64_t kDenseAggAvgScaleFactor = 1000000000000LL;
static constexpr uint32_t kMaxSegmentBackedRows = 32768;
static constexpr uint32_t kInitialGlobalPartitionRows = 16384;
/* Small local cardinalities are cheaper in the existing vectorized batch
 * AHT.  The segment-backed row-at-a-time probe is reserved for partitions
 * large enough that avoiding flat growth outweighs its control overhead. */
/* Keep medium-cardinality local aggregation on the batched flat-AHT path.
 * The segmented path spills, but its current per-row probe is much slower
 * than the vectorized batch probe until the implementation gains a batched
 * segmented lookup. */
static constexpr uint32_t kMinLocalSegmentBackedRows = 32768;

static bool
GetSingleDistinctCountAgg(const TupleDataLayout *layout, uint16_t *out_idx = nullptr)
{
	if (layout == nullptr)
		return false;
	for (uint16_t agg_idx = 0; agg_idx < layout->aggregate_count; ++agg_idx)
	{
		if (layout->aggregates[agg_idx].kind != TdcAggKind::COUNT_DISTINCT_NONNULL)
			continue;
		if (layout->aggregate_count != 1)
			elog(ERROR, "pg_yaap: distinct aggregates currently require a single COUNT(DISTINCT ...) aggregate");
		if (out_idx != nullptr)
			*out_idx = agg_idx;
		return true;
	}
	return false;
}

static bool
IsSingleStateAggregate(const TupleDataLayout *layout)
{
	return layout != nullptr &&
		layout->column_count == 0 &&
		layout->aggregate_count > 0 &&
		!GetSingleDistinctCountAgg(layout);
}

static void
CopyTdcRow(const TupleDataLayout *layout,
	       const TupleDataCollection *src_tdc,
	       const uint8_t *src_row,
	       TupleDataCollection *dst_tdc,
	       uint8_t *dst_row)
{
	for (uint16_t col_idx = 0; col_idx < layout->column_count; ++col_idx)
	{
		const TdcColumnDesc &col = layout->columns[col_idx];
		if (col.kind != TdcColumnKind::STRING_REF)
		{
			std::memcpy(dst_row + col.offset, src_row + col.offset, col.width);
			continue;
		}

		VecStringRef src_ref;
		std::memcpy(&src_ref, src_row + col.offset, sizeof(src_ref));
		const char *src_ptr = VecStringRefDataPtr(src_ref,
			src_tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(src_tdc)) : nullptr);
		VecStringRef dst_ref;
		if (!TupleDataCollectionStoreStringBytes(dst_tdc, src_ptr, src_ref.len, &dst_ref))
			elog(ERROR, "pg_yaap: hash aggregate TDC copy ran out of heap");
		std::memcpy(dst_row + col.offset, &dst_ref, sizeof(dst_ref));
	}

	for (uint16_t agg_idx = 0; agg_idx < layout->aggregate_count; ++agg_idx)
	{
		const TdcAggregateDesc &agg = layout->aggregates[agg_idx];
		const uint16_t width = agg.kind == TdcAggKind::AVG_NUMERIC ? 16 : 8;
		std::memcpy(dst_row + agg.offset, src_row + agg.offset, width);
	}
}

static const TupleDataLayout *
ResolveLayout(dsa_area *dsa, dsa_pointer layout_dp)
{
	if (!DsaPointerIsValid(layout_dp))
		return nullptr;
	return static_cast<const TupleDataLayout *>(dsa_get_address(dsa, layout_dp));
}

static AggregateHashTable *
ResolveAht(dsa_area *dsa, dsa_pointer aht_dp)
{
	if (!DsaPointerIsValid(aht_dp))
		return nullptr;
	return static_cast<AggregateHashTable *>(dsa_get_address(dsa, aht_dp));
}

static void
DestroyAht(ExecCtx &ctx, dsa_pointer aht_dp)
{
	if (!DsaPointerIsValid(aht_dp))
		return;
	auto *aht = ResolveAht(ctx.dsa, aht_dp);
	if (aht != nullptr && DsaPointerIsValid(aht->row_refs_dp))
		buffer::YaapSegmentRowRefTableDestroy(ctx.dsa, aht->row_refs_dp);
	dsa_free(ctx.dsa, aht_dp);
}

static HashAggSharedPayload *
ResolvePayload(dsa_area *dsa, dsa_pointer payload_dp)
{
	if (!DsaPointerIsValid(payload_dp))
		return nullptr;
	return static_cast<HashAggSharedPayload *>(dsa_get_address(dsa, payload_dp));
}

static HashAggPartition *
ResolvePartitions(dsa_area *dsa, HashAggSharedPayload *payload)
{
	if (payload == nullptr || !DsaPointerIsValid(payload->partitions_dp))
		return nullptr;
	return static_cast<HashAggPartition *>(dsa_get_address(dsa, payload->partitions_dp));
}

static uint32_t
EffectiveWorkerCount(const ExecCtx &ctx)
{
	if (ctx.control != nullptr && ctx.control->num_workers > 0)
		return static_cast<uint32_t>(ctx.control->num_workers);
	return static_cast<uint32_t>(std::max(1, pg_yaap_parallel_max_workers));
}

static dsa_pointer *
ResolveLocalRegistry(dsa_area *dsa, HashAggSharedPayload *payload)
{
	if (payload == nullptr || !DsaPointerIsValid(payload->local_partitions_registry_dp))
		return nullptr;
	return static_cast<dsa_pointer *>(dsa_get_address(dsa, payload->local_partitions_registry_dp));
}

static TupleDataCollection *
ResolveTdc(dsa_area *dsa, dsa_pointer tdc_dp)
{
	if (!DsaPointerIsValid(tdc_dp))
		return nullptr;
	return static_cast<TupleDataCollection *>(dsa_get_address(dsa, tdc_dp));
}

static buffer::YaapBufferSharedState *
ResolveBufferState(const ExecCtx &ctx)
{
	if (ctx.control == nullptr || !DsaPointerIsValid(ctx.control->buffer_state_root))
		return nullptr;
	return static_cast<buffer::YaapBufferSharedState *>(
		dsa_get_address(ctx.dsa, ctx.control->buffer_state_root));
}

static bool
CanUseSegmentBackedLocalTdc(const TupleDataLayout *layout, uint32_t partition_count)
{
	if (layout == nullptr || partition_count == 0)
		return false;
	return true;
}

static bool
LayoutHasStringRef(const TupleDataLayout *layout)
{
	if (layout == nullptr)
		return false;
	for (uint16_t i = 0; i < layout->column_count; ++i)
		if (layout->columns[i].kind == TdcColumnKind::STRING_REF)
			return true;
	return false;
}

static bool
SegmentBackingFits(const ExecCtx &ctx, const TupleDataLayout *layout,
					 uint32_t row_capacity)
{
	auto *buffer_state = ResolveBufferState(ctx);
	if (buffer_state == nullptr)
		return false;
	if (ctx.control == nullptr || layout == nullptr || row_capacity == 0 ||
		row_capacity > kMaxSegmentBackedRows ||
		!DsaPointerIsValid(ctx.control->shared_segment_arena_root))
		return false;
	const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(layout, row_capacity);
	const uint64_t required = offsetof(TupleDataCollection, rows) +
		static_cast<uint64_t>(row_capacity) * layout->row_width + heap_capacity;
	auto *arena = buffer::YaapSharedSegmentArenaResolve(ctx.dsa,
		ctx.control->shared_segment_arena_root);
	if (arena != nullptr && required <= arena->segment_size)
		return true;
	if (!DsaPointerIsValid(ctx.control->large_shared_segment_arena_root))
		return false;
	arena = buffer::YaapSharedSegmentArenaResolve(ctx.dsa,
		ctx.control->large_shared_segment_arena_root);
	return arena != nullptr && required <= arena->segment_size;
}

static bool
TryInitSegmentBackedTdc(ExecCtx &ctx, HashAggPartition &part,
						 dsa_pointer layout_dp, const TupleDataLayout *layout,
							 uint32_t partition_count, uint32_t row_capacity)
{
	if (!CanUseSegmentBackedLocalTdc(layout, partition_count) ||
		ctx.control == nullptr ||
		!DsaPointerIsValid(ctx.control->shared_segment_arena_root))
		return false;
	if (LayoutHasStringRef(layout) && EffectiveWorkerCount(ctx) > 4)
		return false;
	while (row_capacity >= 256u && !SegmentBackingFits(ctx, layout, row_capacity))
		row_capacity /= 2u;
	if (row_capacity < 256u || !SegmentBackingFits(ctx, layout, row_capacity))
		return false;
	auto *state = ResolveBufferState(ctx);
	if (state == nullptr)
		return false;
	dsa_pointer arena_dp = ctx.control->shared_segment_arena_root;
	auto *arena = buffer::YaapSharedSegmentArenaResolve(ctx.dsa, arena_dp);
	if (arena == nullptr || row_capacity == 0)
		return false;
	const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(layout, row_capacity);
	const uint64_t required_bytes = offsetof(TupleDataCollection, rows) +
		static_cast<uint64_t>(row_capacity) * layout->row_width + heap_capacity;
	if (required_bytes > arena->segment_size)
	{
		arena_dp = ctx.control->large_shared_segment_arena_root;
		arena = buffer::YaapSharedSegmentArenaResolve(ctx.dsa, arena_dp);
		if (arena == nullptr || required_bytes > arena->segment_size)
			return false;
	}
	const buffer::YaapSegmentHandle handle = buffer::YaapSharedSegmentArenaAllocate(
		ctx.dsa, state, arena_dp, static_cast<uint32_t>(std::max(ctx.worker_index, 0)));
	auto *segment = buffer::YaapSharedSegmentResolve(ctx.dsa, arena_dp, handle);
	if (segment == nullptr || arena == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: hash aggregate segment allocation failed")));
	part.segment_arena_dp = arena_dp;
	part.segment_arena_id = arena->arena_id;
	part.segment_first_desc_dp = segment->self_dp;
	part.segment_desc_dp = segment->self_dp;
	part.segment_id = handle.segment_id;
	part.segment_generation = handle.generation;
	part.segment_row_count = 0;
	part.segment_backed = true;
	part.tdc_dp = segment->payload_dp;
	auto *tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
	TupleDataCollectionInit(tdc, row_capacity, layout->row_width, layout_dp, heap_capacity);
	return true;
}

static void
PrepareSegmentForRead(ExecCtx &ctx, HashAggPartition &part)
{
	if (!part.segment_backed)
		return;
	auto *state = ResolveBufferState(ctx);
	buffer::YaapSegmentHandle handle{part.segment_arena_id, part.segment_id, part.segment_generation};
	auto *segment = buffer::YaapSharedSegmentResolve(ctx.dsa, part.segment_arena_dp, handle);
	if (segment == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: hash aggregate local segment missing")));
	if (pg_atomic_read_u32(&segment->state) ==
		static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL))
	{
		buffer::YaapSharedSegmentSeal(segment);
		buffer::YaapSharedSegmentPublish(segment);
	}
	(void) state;
}

/* Preserve the shared handle and its accounting while evicting immutable local
 * pages.  The combine reader reloads and retires one page at a time; spilling
 * must never turn an entire partition into an uncharged private allocation. */
static void
MaybeSpillLocalPartition(ExecCtx &ctx, HashAggPartition &part, uint32_t partition_id)
{
	if (!part.segment_backed)
		return;
	auto *state = ResolveBufferState(ctx);
	if (state == nullptr || !buffer::YaapBufferOverSoftLimit(state))
		return;
	if (ctx.control == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: hash aggregate spill control missing")));
	for (dsa_pointer dp = part.segment_first_desc_dp; DsaPointerIsValid(dp);)
	{
		auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, dp));
		if (segment == nullptr)
			ereport(ERROR, (errmsg("pg_yaap: hash aggregate local spill descriptor missing")));
		const dsa_pointer next = segment->chain_next_dp;
		const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id,
			segment->generation};
		char prefix[96];
		std::snprintf(prefix, sizeof(prefix), "pg_yaap_localagg_%u_%u_%u_%u",
			static_cast<unsigned>(MyProcPid), partition_id, segment->segment_id,
			segment->generation);
		buffer::YaapSpillStore store(ctx.mcxt);
		store.AttachSharedFileSet(&ctx.control->spill_fileset, prefix);
		buffer::YaapSpillExtent extent{};
		if (!buffer::YaapSharedSegmentSpill(ctx.dsa, state, handle,
			part.segment_arena_dp, &store, &extent))
			ereport(ERROR, (errmsg("pg_yaap: hash aggregate local segment spill failed")));
		dp = next;
	}
}

static void
SyncSegmentUsed(ExecCtx &ctx, HashAggPartition &part, const TupleDataCollection *tdc)
{
	if (!part.segment_backed || tdc == nullptr)
		return;
	auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
		dsa_get_address(ctx.dsa, part.segment_desc_dp));
	if (segment == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: hash aggregate segment descriptor missing")));
	const uint64_t used = offsetof(TupleDataCollection, rows) +
		static_cast<uint64_t>(pg_atomic_read_u32(const_cast<pg_atomic_uint32 *>(&tdc->row_count))) * tdc->row_width +
		pg_atomic_read_u32(const_cast<pg_atomic_uint32 *>(&tdc->heap_used));
	if (used > segment->capacity)
		ereport(ERROR, (errmsg("pg_yaap: hash aggregate segment payload exceeds capacity")));
	segment->used = static_cast<uint32_t>(used);
}

static void
ReleasePartitionBacking(ExecCtx &ctx, HashAggPartition &part)
{
	if (DsaPointerIsValid(part.aht_dp))
		DestroyAht(ctx, part.aht_dp);
	if (DsaPointerIsValid(part.tdc_dp) && !part.segment_backed)
		dsa_free(ctx.dsa, part.tdc_dp);
	if (part.segment_backed)
	{
		auto *state = ResolveBufferState(ctx);
		for (dsa_pointer desc_dp = part.segment_first_desc_dp;
			 DsaPointerIsValid(desc_dp);)
		{
			auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
				dsa_get_address(ctx.dsa, desc_dp));
			if (segment == nullptr)
				ereport(ERROR, (errmsg("pg_yaap: hash aggregate segment descriptor missing during release")));
			const dsa_pointer next_dp = segment->chain_next_dp;
			const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id,
				segment->generation};
			if (state == nullptr || !buffer::YaapSharedSegmentRetire(
					ctx.dsa, state, part.segment_arena_dp, handle))
				ereport(ERROR, (errmsg("pg_yaap: hash aggregate segment retire failed")));
			desc_dp = next_dp;
		}
	}
	part.aht_dp = InvalidDsaPointer;
	part.tdc_dp = InvalidDsaPointer;
	part.segment_arena_dp = InvalidDsaPointer;
	part.segment_arena_id = 0;
	part.segment_first_desc_dp = InvalidDsaPointer;
	part.segment_desc_dp = InvalidDsaPointer;
	part.segment_row_count = 0;
	part.segment_backed = false;
}

static void
RetireSegmentBackingAfterGrow(ExecCtx &ctx, dsa_pointer arena_dp,
							  uint32_t segment_id, uint32_t generation)
{
	auto *state = ResolveBufferState(ctx);
	/* The arena id is encoded in the partition metadata; grow callers use
	 * the old segment's arena and pass it through the dsa pointer below. */
	auto *arena = buffer::YaapSharedSegmentArenaResolve(ctx.dsa, arena_dp);
	if (arena == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: hash aggregate segment arena missing during grow")));
	buffer::YaapSegmentHandle handle{arena->arena_id, segment_id, generation};
	auto *segment = buffer::YaapSharedSegmentResolve(ctx.dsa, arena_dp, handle);
	if (state == nullptr || segment == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: hash aggregate segment missing during grow")));
	if (pg_atomic_read_u32(&segment->state) ==
		static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL))
	{
		buffer::YaapSharedSegmentSeal(segment);
		buffer::YaapSharedSegmentPublish(segment);
	}
	if (!buffer::YaapSharedSegmentRetire(ctx.dsa, state, arena_dp, handle))
		 ereport(ERROR, (errmsg("pg_yaap: hash aggregate segment retire during grow failed")));
}

/* Append one complete local partition to the stable global segment chain.
 * The writer object is reconstructed from the shared tail on every combine
 * call; only the chain links and row-reference directory are persistent. */
static void
GrowSegmentedLocalAht(ExecCtx &ctx, HashAggPartition &part,
					 uint32_t row_count, const TupleDataLayout *layout);

static void
CombineLocalRowsIntoSegmentedGlobal(ExecCtx &ctx,
								 HashAggPartition &global_part,
								 const TupleDataCollection *local_tdc,
								 dsa_pointer layout_dp,
								 const TupleDataLayout *layout)
{
	if (!global_part.segment_backed || local_tdc == nullptr || layout == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: invalid segmented global hash aggregate state")));
	auto *state = ResolveBufferState(ctx);
	if (state == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: segmented global hash aggregate buffer state missing")));
	auto *tail = static_cast<buffer::YaapSharedSegmentDesc *>(
		dsa_get_address(ctx.dsa, global_part.segment_desc_dp));
	auto *first = static_cast<buffer::YaapSharedSegmentDesc *>(
		dsa_get_address(ctx.dsa, global_part.segment_first_desc_dp));
	if (tail == nullptr || first == nullptr || !DsaPointerIsValid(tail->payload_dp))
		ereport(ERROR, (errmsg("pg_yaap: segmented global hash aggregate chain is incomplete")));
	auto *tail_store = static_cast<TupleDataCollection *>(
		dsa_get_address(ctx.dsa, tail->payload_dp));
	if (tail_store == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: segmented global hash aggregate tail is missing")));

	buffer::YaapSegmentedTdc writer;
	buffer::YaapSegmentedTdcInit(&writer, global_part.segment_arena_dp,
		layout_dp, static_cast<uint32_t>(std::max(ctx.worker_index, 0)),
			tail_store->row_width, tail_store->row_capacity, tail_store->heap_capacity,
			0, "hash aggregate global combine");
	writer.first = buffer::YaapSegmentHandle{first->arena_id, first->segment_id,
		first->generation};
	writer.active = buffer::YaapSegmentHandle{tail->arena_id, tail->segment_id,
		tail->generation};
	writer.segment_count = 1;
	writer.row_count = global_part.segment_row_count;

	const uint32_t row_count = pg_atomic_read_u32(
		const_cast<pg_atomic_uint32 *>(&local_tdc->row_count));
	for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
	{
		if (PipelineCancelRequestedEvery(ctx, row_idx))
			return;
		const uint8_t *src_row = TupleDataCollectionGetRowConst(local_tdc, row_idx);
		const uint64_t hash = HashGroupRow(layout, local_tdc, src_row);
		AggregateHashTable *aht = ResolveAht(ctx.dsa, global_part.aht_dp);
		if (aht == nullptr)
			ereport(ERROR, (errmsg("pg_yaap: segmented global hash aggregate AHT missing")));
		if (static_cast<uint64_t>(global_part.segment_row_count + 1u) * 3u >=
			static_cast<uint64_t>(aht->capacity) * 2u)
		{
			GrowSegmentedLocalAht(ctx, global_part,
				global_part.segment_row_count, layout);
			aht = ResolveAht(ctx.dsa, global_part.aht_dp);
		}
		AggregateHashTableCombineRowSegmented(aht, ctx.dsa, global_part.segment_arena_dp,
			state, &writer, &global_part.segment_row_count, layout, local_tdc, src_row, hash);
	}
	buffer::YaapSegmentedTdcSealPublish(ctx.dsa, &writer);
	global_part.segment_desc_dp = buffer::YaapSharedSegmentResolve(ctx.dsa,
		global_part.segment_arena_dp, writer.active)->self_dp;
	auto *new_tail = static_cast<buffer::YaapSharedSegmentDesc *>(
		dsa_get_address(ctx.dsa, global_part.segment_desc_dp));
	global_part.segment_id = new_tail->segment_id;
	global_part.segment_generation = new_tail->generation;
	global_part.tdc_dp = new_tail->payload_dp;
}

static void
EnsureHashAggGlobalSegmentResident(ExecCtx &ctx,
								   dsa_pointer arena_dp,
								   buffer::YaapSharedSegmentDesc *segment)
{
	if (segment == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: hash aggregate global segment is missing")));
	const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id,
		segment->generation};
	if (pg_atomic_read_u32(&segment->state) ==
		static_cast<uint32_t>(buffer::YaapSharedSegmentState::SPILLED))
	{
		if (ctx.control == nullptr)
			ereport(ERROR, (errmsg("pg_yaap: hash aggregate global spill control missing")));
		buffer::YaapSpillStore spill(ctx.mcxt);
		spill.AttachSharedFileSet(&ctx.control->spill_fileset,
			segment->spill_extent.file_name);
		if (!buffer::YaapSharedSegmentReload(ctx.dsa, ResolveBufferState(ctx), handle,
			arena_dp, &spill, segment->spill_extent))
		{
			for (uint32_t retry = 0; retry < 1000; ++retry)
			{
				const uint32_t state = pg_atomic_read_u32(&segment->state);
				if (state == static_cast<uint32_t>(buffer::YaapSharedSegmentState::PUBLISHED))
					break;
				if (state != static_cast<uint32_t>(buffer::YaapSharedSegmentState::RELOADING) &&
					state != static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL))
					ereport(ERROR, (errmsg("pg_yaap: hash aggregate global spill reload entered invalid state")));
				pg_usleep(100L);
			}
		}
		if (pg_atomic_read_u32(&segment->state) ==
			static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL))
		{
			buffer::YaapSharedSegmentSeal(segment);
			buffer::YaapSharedSegmentPublish(segment);
		}
	}
	if (pg_atomic_read_u32(&segment->state) !=
		static_cast<uint32_t>(buffer::YaapSharedSegmentState::PUBLISHED) ||
		!DsaPointerIsValid(segment->payload_dp))
		ereport(ERROR, (errmsg("pg_yaap: hash aggregate global segment is not readable")));
}

static void
RetireHashAggGlobalPartition(ExecCtx &ctx, HashAggPartition &part)
{
	if (!part.segment_backed)
		return;
	auto *state = ResolveBufferState(ctx);
	if (state == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: hash aggregate source retire state missing")));
	for (dsa_pointer desc_dp = part.segment_first_desc_dp;
		 DsaPointerIsValid(desc_dp);)
	{
		auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
			dsa_get_address(ctx.dsa, desc_dp));
		if (segment == nullptr)
			ereport(ERROR, (errmsg("pg_yaap: hash aggregate source retire descriptor missing")));
		const dsa_pointer next_dp = segment->chain_next_dp;
		const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id,
			segment->generation};
		if (!buffer::YaapSharedSegmentRetire(ctx.dsa, state, part.segment_arena_dp, handle))
			ereport(ERROR, (errmsg("pg_yaap: hash aggregate source segment retire failed")));
		desc_dp = next_dp;
	}
	if (DsaPointerIsValid(part.aht_dp))
		DestroyAht(ctx, part.aht_dp);
	part.aht_dp = InvalidDsaPointer;
	part.tdc_dp = InvalidDsaPointer;
	part.segment_arena_dp = InvalidDsaPointer;
	part.segment_first_desc_dp = InvalidDsaPointer;
	part.segment_desc_dp = InvalidDsaPointer;
	part.segment_backed = false;
}

static void
SpillHashAggGlobalPartition(ExecCtx &ctx, HashAggPartition &part,
							uint32_t partition_id)
{
	if (!part.segment_backed || ctx.control == nullptr)
		return;
	auto *buffer_state = ResolveBufferState(ctx);
	if (buffer_state == nullptr || !buffer::YaapBufferOverSoftLimit(buffer_state))
		return;
	for (dsa_pointer dp = part.segment_first_desc_dp; DsaPointerIsValid(dp);)
	{
		auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
			dsa_get_address(ctx.dsa, dp));
		if (segment == nullptr)
			return;
		const dsa_pointer next = segment->chain_next_dp;
		if (pg_atomic_read_u32(&segment->state) ==
			static_cast<uint32_t>(buffer::YaapSharedSegmentState::PUBLISHED))
		{
			buffer::YaapSpillStore spill(ctx.mcxt);
			char prefix[128];
			std::snprintf(prefix, sizeof(prefix), "pg_yaap_agg_%u_%u_%u",
				partition_id, segment->segment_id, segment->generation);
			spill.AttachSharedFileSet(&ctx.control->spill_fileset, prefix);
			buffer::YaapSpillExtent extent{};
			const buffer::YaapSegmentHandle handle{segment->arena_id,
				segment->segment_id, segment->generation};
			if (!buffer::YaapSharedSegmentSpill(ctx.dsa, buffer_state, handle,
				part.segment_arena_dp, &spill, &extent))
				return;
		}
		dp = next;
	}
}

static void
ReleaseLocalPartitionArrays(ExecCtx &ctx, HashAggSharedPayload *payload)
{
	if (payload == nullptr || !DsaPointerIsValid(payload->local_partitions_registry_dp))
		return;
	auto *registry = ResolveLocalRegistry(ctx.dsa, payload);
	for (uint32_t slot = 0; slot < payload->local_state_slot_count; ++slot)
	{
		if (!DsaPointerIsValid(registry[slot]))
			continue;
		dsa_free(ctx.dsa, registry[slot]);
		registry[slot] = InvalidDsaPointer;
	}
}

static void GrowLocalTdc(ExecCtx &ctx,
                         HashAggLocalSinkState &local,
	                         HashAggPartition &part,
	                         uint32_t required_heap_bytes);

static uint32_t
RequiredHeapBytesForChunkRows(const TupleDataLayout *layout,
							  const PipelineChunk &chunk,
							  const uint16_t *row_indices,
							  uint16_t count);

static bool
PartitionNeedsGrowForChunkBatch(const TupleDataLayout *layout,
								const TupleDataCollection *tdc,
								const PipelineChunk &chunk,
								const uint16_t *row_indices,
								uint16_t count);

static uint32_t
PartitionRowCapacity(uint32_t max_groups, uint32_t partition_count)
{
	const uint32_t n = partition_count < 1 ? 1u : partition_count;
	uint32_t per = (max_groups + n - 1u) / n;
	if (per < 16u)
		per = 16u;
	return per;
}

static bool
CanEncodePerfectHashLayout(const TupleDataLayout *layout)
{
	if (layout == nullptr || layout->column_count == 0 || layout->validity_width != 0)
		return false;
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		if (col.kind != TdcColumnKind::INT32 || col.width != 4 ||
			(col.pg_type_oid != BPCHAROID && col.pg_type_oid != CHAROID))
			return false;
	}
	return true;
}

static inline bool
EncodePerfectHashKey(const TupleDataLayout *layout,
                     const PipelineChunk &chunk,
                     uint16_t row_idx,
                     uint32_t *out_key)
{
	uint32_t key = 0;
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		const int32_t v = chunk.get_int32(col.src_col_idx, row_idx);
		if (v < 0 || v > 255)
			return false;
		key = (key << 8) | static_cast<uint32_t>(v);
	}
	*out_key = key;
	return true;
}

static void
SinkChunkPerfectHash(ExecCtx &ctx,
                     HashAggLocalSinkState &local,
                     PipelineChunk &in)
{
	HashAggPartition &part = local.local_partitions[0];
	TupleDataCollection *tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
	if (tdc == nullptr)
		elog(ERROR, "pg_yaap: perfect hash aggregate TDC missing");

	std::array<uint32_t, PIPELINE_DEFAULT_CHUNK_SIZE> canonical_rows;
	std::array<uint16_t, PIPELINE_DEFAULT_CHUNK_SIZE> update_rows;
	uint16_t update_count = 0;

	for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
	{
		if (PipelineCancelRequestedEvery(ctx, row_idx))
			return;
		uint32_t key = 0;
		if (!EncodePerfectHashKey(local.layout, in, row_idx, &key) ||
			key >= local.perfect_capacity)
			ereport(ERROR,
			        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			         errmsg("pg_yaap: perfect hash key out of domain")));

		uint32_t canonical_idx = local.perfect_row_indices[key];
		if (canonical_idx == TDC_INVALID_ROW_INDEX)
		{
			while (pg_atomic_read_u32(&tdc->row_count) >= tdc->row_capacity)
			{
				GrowLocalTdc(ctx, local, part, 0);
				tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
				if (tdc == nullptr)
					elog(ERROR, "pg_yaap: perfect hash aggregate TDC missing after grow");
			}
			uint8_t *candidate_row = nullptr;
			canonical_idx = TupleDataCollectionAppendRow(tdc, &candidate_row);
			if (canonical_idx == TDC_INVALID_ROW_INDEX)
				elog(ERROR, "pg_yaap: perfect hash aggregate row capacity exceeded");
			ScatterGroupOnly(local.layout, tdc, candidate_row, in, row_idx);
			local.perfect_row_indices[key] = canonical_idx;
		}

		update_rows[update_count] = row_idx;
		canonical_rows[update_count] = canonical_idx;
		++update_count;
	}

	if (update_count > 0)
	{
		tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
		if (tdc == nullptr)
			elog(ERROR, "pg_yaap: perfect hash aggregate TDC missing before update");
		UpdateAggregatesGather(local.layout,
			tdc->rows,
			tdc->row_width,
			canonical_rows.data(),
			in,
			update_rows.data(),
			update_count);
	}
}

static void
SinkChunkSingleState(ExecCtx &ctx,
                     HashAggLocalSinkState &local,
                     PipelineChunk &in)
{
	Assert(IsSingleStateAggregate(local.layout));

	if (in.count == 0)
		return;

	HashAggPartition &part = local.local_partitions[0];
	TupleDataCollection *tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
	if (tdc == nullptr)
		elog(ERROR, "pg_yaap: single-state hash aggregate TDC missing");

	const uint32_t row_count = pg_atomic_read_u32(&tdc->row_count);
	if (row_count > 1)
		elog(ERROR,
			 "pg_yaap: single-state hash aggregate produced %u rows",
			 row_count);

	if (row_count == 0)
	{
		uint8_t *row_ptr = nullptr;
		const uint32_t row_idx = TupleDataCollectionAppendRow(tdc, &row_ptr);
		if (row_idx == TDC_INVALID_ROW_INDEX || row_ptr == nullptr)
			elog(ERROR, "pg_yaap: single-state hash aggregate row allocation failed");
		ScatterGroupOnly(local.layout, tdc, row_ptr, in, 0);
	}

	std::array<uint32_t, PIPELINE_DEFAULT_CHUNK_SIZE> canonical_rows{};
	std::array<uint16_t, PIPELINE_DEFAULT_CHUNK_SIZE> row_indices;
	for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
	{
		if (PipelineCancelRequestedEvery(ctx, row_idx))
			return;
		row_indices[row_idx] = row_idx;
	}

	tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
	if (tdc == nullptr)
		elog(ERROR, "pg_yaap: single-state hash aggregate TDC missing");
	UpdateAggregatesGather(local.layout,
		tdc->rows,
		tdc->row_width,
		canonical_rows.data(),
		in,
		row_indices.data(),
		in.count);
	SyncSegmentUsed(ctx, part, tdc);
}

static void
AllocAhtForTdc(ExecCtx &ctx,
               dsa_pointer tdc_dp,
               uint32_t max_groups,
               dsa_pointer *out_aht_dp,
               AggregateHashTable **out_aht)
{
	const uint32_t capacity = AggregateHashTableChooseCapacity(max_groups);
	*out_aht_dp = dsa_allocate0(ctx.dsa, AggregateHashTableAllocSize(capacity));
	*out_aht = static_cast<AggregateHashTable *>(dsa_get_address(ctx.dsa, *out_aht_dp));
	AggregateHashTableInit(*out_aht, capacity, tdc_dp);
}

static void
GrowSegmentedLocalAht(ExecCtx &ctx, HashAggPartition &part,
					 uint32_t row_count, const TupleDataLayout *layout)
{
	auto *old_aht = ResolveAht(ctx.dsa, part.aht_dp);
	if (old_aht == nullptr || !DsaPointerIsValid(old_aht->row_refs_dp))
		elog(ERROR, "pg_yaap: segmented aggregate AHT cannot grow without row references");
	constexpr uint32_t kMaxAhtCapacity = 1u << 26;
	if (old_aht->capacity >= kMaxAhtCapacity)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("pg_yaap: segmented aggregate exceeds maximum AHT capacity")));
	const uint32_t new_capacity = std::min(kMaxAhtCapacity, old_aht->capacity * 2u);
	const dsa_pointer new_dp = dsa_allocate0(ctx.dsa,
		AggregateHashTableAllocSize(new_capacity));
	auto *new_aht = static_cast<AggregateHashTable *>(dsa_get_address(ctx.dsa, new_dp));
	AggregateHashTableInit(new_aht, new_capacity, InvalidDsaPointer);
	AggregateHashTableCopySegmentRefs(ctx.dsa, old_aht, new_aht);
	TupleDataCollection row_count_header{};
	pg_atomic_init_u32(&row_count_header.row_count, row_count);
	AggregateHashTableRehashSegmented(new_aht, ctx.dsa, part.segment_arena_dp,
		&row_count_header, layout);
	const dsa_pointer old_dp = part.aht_dp;
	part.aht_dp = new_dp;
	DestroyAht(ctx, old_dp);
}

static void
ResizeAhtForTdc(ExecCtx &ctx,
                dsa_pointer tdc_dp,
                TupleDataCollection *tdc,
                const TupleDataLayout *layout,
                dsa_pointer *aht_dp)
{
	dsa_pointer old_aht_dp = *aht_dp;
	AggregateHashTable *old_aht = ResolveAht(ctx.dsa, *aht_dp);
	if (old_aht == nullptr || !AggregateHashTableShouldResize(old_aht, tdc))
		return;

	const uint32_t new_capacity = old_aht->capacity << 1;
	dsa_pointer new_aht_dp = dsa_allocate0(ctx.dsa, AggregateHashTableAllocSize(new_capacity));
	AggregateHashTable *new_aht = static_cast<AggregateHashTable *>(dsa_get_address(ctx.dsa, new_aht_dp));
	AggregateHashTableInit(new_aht, new_capacity, tdc_dp);
	if (old_aht->row_refs_dp != InvalidDsaPointer)
	{
		AggregateHashTableCopySegmentRefs(ctx.dsa, old_aht, new_aht);
		AggregateHashTableRehashSegmented(new_aht, ctx.dsa,
			ctx.control != nullptr ? ctx.control->shared_segment_arena_root : InvalidDsaPointer,
			tdc, layout);
	}
	else
		AggregateHashTableRehash(new_aht, tdc, layout);
	*aht_dp = new_aht_dp;
	DestroyAht(ctx, old_aht_dp);
}

static void
GrowTdcForPartition(ExecCtx &ctx,
                    dsa_pointer layout_dp,
                    const TupleDataLayout *layout,
	                    HashAggPartition &part,
	                    uint32_t required_heap_bytes)
{
	dsa_pointer old_tdc_dp = part.tdc_dp;
	dsa_pointer old_aht_dp = part.aht_dp;
	const bool old_segment_backed = part.segment_backed;
	const dsa_pointer old_segment_arena_dp = part.segment_arena_dp;
	const uint32_t old_segment_id = part.segment_id;
	const uint32_t old_segment_generation = part.segment_generation;
	TupleDataCollection *old_tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
	if (old_tdc == nullptr)
		elog(ERROR, "pg_yaap: hash aggregate partition TDC missing");

	const uint32_t old_count = pg_atomic_read_u32(&old_tdc->row_count);
	uint32_t new_capacity = std::max(old_tdc->row_capacity * 2u, old_count + 1u);
	new_capacity = TupleDataCollectionClampRowCapacity(new_capacity,
		layout->row_width,
		required_heap_bytes,
		old_count + 1u);
	const uint32_t heap_capacity = TupleDataCollectionGrowHeapCapacity(layout,
		old_tdc,
		new_capacity,
		required_heap_bytes);
	dsa_pointer new_tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
		new_capacity,
		layout->row_width,
		heap_capacity);
	auto *new_tdc = ResolveTdc(ctx.dsa, new_tdc_dp);
	TupleDataCollectionInit(new_tdc, new_capacity, layout->row_width, layout_dp, heap_capacity);

	for (uint32_t row_idx = 0; row_idx < old_count; ++row_idx)
	{
		uint8_t *dst = nullptr;
		const uint32_t copied_idx = TupleDataCollectionAppendRow(new_tdc, &dst);
		if (copied_idx == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: hash aggregate TDC grow copy overflow");
		const uint8_t *src = TupleDataCollectionGetRowConst(old_tdc, row_idx);
		CopyTdcRow(layout, old_tdc, src, new_tdc, dst);
	}

	const uint32_t capacity = AggregateHashTableChooseCapacity(new_capacity);
	dsa_pointer new_aht_dp = dsa_allocate0(ctx.dsa, AggregateHashTableAllocSize(capacity));
	AggregateHashTable *new_aht = static_cast<AggregateHashTable *>(dsa_get_address(ctx.dsa, new_aht_dp));
	AggregateHashTableInit(new_aht, capacity, new_tdc_dp);
	AggregateHashTableRehash(new_aht, new_tdc, layout);
	part.tdc_dp = new_tdc_dp;
	part.aht_dp = new_aht_dp;
	DestroyAht(ctx, old_aht_dp);
	if (old_segment_backed)
	{
		RetireSegmentBackingAfterGrow(ctx, old_segment_arena_dp,
			old_segment_id, old_segment_generation);
		part.segment_arena_dp = InvalidDsaPointer;
		part.segment_backed = false;
	}
	else
		dsa_free(ctx.dsa, old_tdc_dp);
}

static void
GrowLocalTdc(ExecCtx &ctx,
             HashAggLocalSinkState &local,
	             HashAggPartition &part,
	             uint32_t required_heap_bytes)
{
	dsa_pointer old_tdc_dp = part.tdc_dp;
	dsa_pointer old_aht_dp = part.aht_dp;
	const bool old_segment_backed = part.segment_backed;
	const dsa_pointer old_segment_arena_dp = part.segment_arena_dp;
	const uint32_t old_segment_id = part.segment_id;
	const uint32_t old_segment_generation = part.segment_generation;
	TupleDataCollection *old_tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
	if (old_tdc == nullptr)
		elog(ERROR, "pg_yaap: local hash aggregate partition TDC missing");
	const uint32_t old_count = pg_atomic_read_u32(&old_tdc->row_count);
	uint32_t new_capacity = std::max(old_tdc->row_capacity * 2u, old_count + 1u);
	new_capacity = TupleDataCollectionClampRowCapacity(new_capacity,
		local.layout->row_width,
		required_heap_bytes,
		old_count + 1u);
	const uint32_t heap_capacity = TupleDataCollectionGrowHeapCapacity(local.layout,
		old_tdc,
		new_capacity,
		required_heap_bytes);
	dsa_pointer new_tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
		new_capacity,
		local.layout->row_width,
		heap_capacity);
	auto *new_tdc = ResolveTdc(ctx.dsa, new_tdc_dp);
	TupleDataCollectionInit(new_tdc,
		new_capacity,
		local.layout->row_width,
		local.layout_dp,
		heap_capacity);

	for (uint32_t row_idx = 0; row_idx < old_count; ++row_idx)
	{
		uint8_t *dst = nullptr;
		const uint32_t copied_idx = TupleDataCollectionAppendRow(new_tdc, &dst);
		if (copied_idx == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: local hash aggregate TDC grow copy overflow");
		const uint8_t *src = TupleDataCollectionGetRowConst(old_tdc, row_idx);
		CopyTdcRow(local.layout, old_tdc, src, new_tdc, dst);
	}

	const uint32_t capacity = AggregateHashTableChooseCapacity(new_capacity);
	dsa_pointer new_aht_dp = dsa_allocate0(ctx.dsa, AggregateHashTableAllocSize(capacity));
	AggregateHashTable *new_aht = static_cast<AggregateHashTable *>(dsa_get_address(ctx.dsa, new_aht_dp));
	AggregateHashTableInit(new_aht, capacity, new_tdc_dp);
	AggregateHashTableRehash(new_aht, new_tdc, local.layout);
	part.tdc_dp = new_tdc_dp;
	part.aht_dp = new_aht_dp;
	DestroyAht(ctx, old_aht_dp);
	if (old_segment_backed)
	{
		RetireSegmentBackingAfterGrow(ctx, old_segment_arena_dp,
			old_segment_id, old_segment_generation);
		part.segment_arena_dp = InvalidDsaPointer;
		part.segment_backed = false;
	}
	else
		dsa_free(ctx.dsa, old_tdc_dp);
}

static void
GrowLocalTdcForLayout(ExecCtx &ctx,
					  const TupleDataLayout *layout,
					  dsa_pointer layout_dp,
					  HashAggPartition &part,
					  uint32_t required_heap_bytes)
{
	dsa_pointer old_tdc_dp = part.tdc_dp;
	dsa_pointer old_aht_dp = part.aht_dp;
	const bool old_segment_backed = part.segment_backed;
	const dsa_pointer old_segment_arena_dp = part.segment_arena_dp;
	const uint32_t old_segment_id = part.segment_id;
	const uint32_t old_segment_generation = part.segment_generation;
	TupleDataCollection *old_tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
	if (old_tdc == nullptr)
		elog(ERROR, "pg_yaap: local distinct aggregate partition TDC missing");
	const uint32_t old_count = pg_atomic_read_u32(&old_tdc->row_count);
	uint32_t new_capacity = std::max(old_tdc->row_capacity * 2u, old_count + 1u);
	new_capacity = TupleDataCollectionClampRowCapacity(new_capacity,
		layout->row_width,
		required_heap_bytes,
		old_count + 1u);
	const uint32_t heap_capacity = TupleDataCollectionGrowHeapCapacity(layout,
		old_tdc,
		new_capacity,
		required_heap_bytes);
	dsa_pointer new_tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
		new_capacity,
		layout->row_width,
		heap_capacity);
	auto *new_tdc = ResolveTdc(ctx.dsa, new_tdc_dp);
	TupleDataCollectionInit(new_tdc,
		new_capacity,
		layout->row_width,
		layout_dp,
		heap_capacity);
	for (uint32_t row_idx = 0; row_idx < old_count; ++row_idx)
	{
		uint8_t *dst = nullptr;
		const uint32_t copied_idx = TupleDataCollectionAppendRow(new_tdc, &dst);
		if (copied_idx == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: local distinct aggregate TDC grow copy overflow");
		const uint8_t *src = TupleDataCollectionGetRowConst(old_tdc, row_idx);
		CopyTdcRow(layout, old_tdc, src, new_tdc, dst);
	}
	const uint32_t capacity = AggregateHashTableChooseCapacity(new_capacity);
	dsa_pointer new_aht_dp = dsa_allocate0(ctx.dsa, AggregateHashTableAllocSize(capacity));
	AggregateHashTable *new_aht = static_cast<AggregateHashTable *>(dsa_get_address(ctx.dsa, new_aht_dp));
	AggregateHashTableInit(new_aht, capacity, new_tdc_dp);
	AggregateHashTableRehash(new_aht, new_tdc, layout);
	part.tdc_dp = new_tdc_dp;
	part.aht_dp = new_aht_dp;
	DestroyAht(ctx, old_aht_dp);
	if (old_segment_backed)
	{
		RetireSegmentBackingAfterGrow(ctx, old_segment_arena_dp,
			old_segment_id, old_segment_generation);
		part.segment_arena_dp = InvalidDsaPointer;
		part.segment_backed = false;
	}
	else
		dsa_free(ctx.dsa, old_tdc_dp);
}

static void
InitDistinctLocalState(ExecCtx &ctx, HashAggLocalSinkState &local, uint32_t per_partition_groups)
{
	const TdcAggregateDesc &distinct_agg = local.layout->aggregates[local.distinct_agg_idx];
	TupleDataLayout distinct_layout{};
	TupleDataLayoutInit(&distinct_layout);
	for (uint16_t col_idx = 0; col_idx < local.layout->column_count; ++col_idx)
	{
		const TdcColumnDesc &col = local.layout->columns[col_idx];
		(void) TupleDataLayoutAppendColumn(&distinct_layout,
			col.kind,
			col.src_col_idx,
			col.pg_type_oid,
			col.numeric_scale);
	}
	(void) TupleDataLayoutAppendColumn(&distinct_layout,
		TdcColumnKind::INT64,
		distinct_agg.src_col_idx,
		distinct_agg.numeric_scale != 0 ? NUMERICOID : INT8OID,
		distinct_agg.numeric_scale);
	TupleDataLayoutSeal(&distinct_layout);
	local.distinct_layout_dp = SerializeTupleDataLayout(distinct_layout, ctx.dsa);
	local.distinct_layout = ResolveLayout(ctx.dsa, local.distinct_layout_dp);
	if (local.distinct_layout == nullptr)
		elog(ERROR, "pg_yaap: failed to publish distinct aggregate layout");

	local.distinct_partitions_dp = dsa_allocate0(ctx.dsa,
		static_cast<size_t>(local.partition_count) * sizeof(HashAggPartition));
	local.distinct_partitions = static_cast<HashAggPartition *>(
		dsa_get_address(ctx.dsa, local.distinct_partitions_dp));
	for (uint32_t part_idx = 0; part_idx < local.partition_count; ++part_idx)
	{
		HashAggPartition &part = local.distinct_partitions[part_idx];
		LWLockInitialize(&part.mutex, LWTRANCHE_PARALLEL_QUERY_DSA);
		const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(local.distinct_layout,
			per_partition_groups);
		part.tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
			per_partition_groups,
			local.distinct_layout->row_width,
			heap_capacity);
		auto *tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
		TupleDataCollectionInit(tdc,
			per_partition_groups,
			local.distinct_layout->row_width,
			local.distinct_layout_dp,
			heap_capacity);
		AggregateHashTable *aht = nullptr;
		AllocAhtForTdc(ctx, part.tdc_dp, per_partition_groups, &part.aht_dp, &aht);
	}
}

static SinkResultType
SinkChunkDistinctCount(ExecCtx &ctx, HashAggLocalSinkState &local, PipelineChunk &in)
{
	std::array<uint64_t, PIPELINE_DEFAULT_CHUNK_SIZE> group_hashes;
	std::array<uint32_t, PIPELINE_DEFAULT_CHUNK_SIZE> partitions;
	std::array<uint16_t, PIPELINE_DEFAULT_CHUNK_SIZE> probe_rows;
	std::array<AggregateHashTableBatchProbeInput, PIPELINE_DEFAULT_CHUNK_SIZE> probe_inputs;
	std::array<AggregateHashTableBatchProbeResult, PIPELINE_DEFAULT_CHUNK_SIZE> probe_results;
	std::array<uint32_t, PIPELINE_DEFAULT_CHUNK_SIZE> main_canonical_rows;
	std::array<uint16_t, PIPELINE_DEFAULT_CHUNK_SIZE> distinct_rows;
	std::array<uint32_t, PIPELINE_DEFAULT_CHUNK_SIZE> distinct_main_canonical_rows;
	uint16_t distinct_input_count = 0;

	const TdcAggregateDesc &distinct_agg = local.layout->aggregates[local.distinct_agg_idx];
	for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
	{
		if (PipelineCancelRequestedEvery(ctx, row_idx))
			return SinkResultType::FINISHED;
		group_hashes[row_idx] = HashGroup(local.layout, in, row_idx);
		partitions[row_idx] = static_cast<uint32_t>(group_hashes[row_idx] >> local.partition_shift) &
			local.partition_mask;
	}

	for (uint32_t part_idx = 0; part_idx < local.partition_count; ++part_idx)
	{
		uint16_t probe_count = 0;
		for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
		{
			if (partitions[row_idx] != part_idx)
				continue;
			probe_rows[probe_count++] = row_idx;
		}
		if (probe_count == 0)
			continue;

		HashAggPartition &main_part = local.local_partitions[part_idx];
		TupleDataCollection *main_tdc = ResolveTdc(ctx.dsa, main_part.tdc_dp);
		if (main_tdc == nullptr)
			elog(ERROR, "pg_yaap: local distinct aggregate main TDC missing");
		while (PartitionNeedsGrowForChunkBatch(local.layout, main_tdc, in, probe_rows.data(), probe_count))
		{
			GrowLocalTdc(ctx, local, main_part,
				RequiredHeapBytesForChunkRows(local.layout, in, probe_rows.data(), probe_count));
			main_tdc = ResolveTdc(ctx.dsa, main_part.tdc_dp);
			if (main_tdc == nullptr)
				elog(ERROR, "pg_yaap: local distinct aggregate main TDC missing after grow");
		}
		AggregateHashTable *main_aht = ResolveAht(ctx.dsa, main_part.aht_dp);
		for (uint16_t i = 0; i < probe_count; ++i)
		{
			probe_inputs[i].row_idx = probe_rows[i];
			probe_inputs[i].hash = group_hashes[probe_rows[i]];
		}
		AggregateHashTableFindOrInsertBatch(main_aht,
			main_tdc,
			local.layout,
			in,
			probe_inputs.data(),
			probe_count,
			probe_results.data());
		if (AggregateHashTableShouldResize(main_aht, main_tdc))
		{
			ResizeAhtForTdc(ctx, main_part.tdc_dp, main_tdc, local.layout, &main_part.aht_dp);
			main_tdc = ResolveTdc(ctx.dsa, main_part.tdc_dp);
			if (main_tdc == nullptr)
				elog(ERROR, "pg_yaap: local distinct aggregate main TDC missing after resize");
		}

		uint16_t distinct_count = 0;
		for (uint16_t i = 0; i < probe_count; ++i)
		{
			main_canonical_rows[i] = probe_results[i].canonical_row_idx;
			const uint16_t row_idx = probe_results[i].row_idx;
			if (in.nulls[distinct_agg.src_col_idx][row_idx] != 0)
				continue;
			distinct_rows[distinct_count] = row_idx;
			distinct_main_canonical_rows[distinct_count] = probe_results[i].canonical_row_idx;
			probe_inputs[distinct_count].row_idx = row_idx;
			probe_inputs[distinct_count].hash = HashGroup(local.distinct_layout, in, row_idx);
			++distinct_count;
		}
		if (distinct_count == 0)
			continue;

		HashAggPartition &distinct_part = local.distinct_partitions[part_idx];
		TupleDataCollection *distinct_tdc = ResolveTdc(ctx.dsa, distinct_part.tdc_dp);
		if (distinct_tdc == nullptr)
			elog(ERROR, "pg_yaap: local distinct aggregate distinct-key TDC missing");
		while (PartitionNeedsGrowForChunkBatch(local.distinct_layout, distinct_tdc, in, distinct_rows.data(), distinct_count))
		{
			GrowLocalTdcForLayout(ctx,
				local.distinct_layout,
				local.distinct_layout_dp,
				distinct_part,
				RequiredHeapBytesForChunkRows(local.distinct_layout, in, distinct_rows.data(), distinct_count));
			distinct_tdc = ResolveTdc(ctx.dsa, distinct_part.tdc_dp);
			if (distinct_tdc == nullptr)
				elog(ERROR, "pg_yaap: local distinct aggregate distinct-key TDC missing after grow");
		}
		AggregateHashTable *distinct_aht = ResolveAht(ctx.dsa, distinct_part.aht_dp);
		AggregateHashTableFindOrInsertBatch(distinct_aht,
			distinct_tdc,
			local.distinct_layout,
			in,
			probe_inputs.data(),
			distinct_count,
			probe_results.data());
		if (AggregateHashTableShouldResize(distinct_aht, distinct_tdc))
		{
			ResizeAhtForTdc(ctx,
				distinct_part.tdc_dp,
				distinct_tdc,
				local.distinct_layout,
				&distinct_part.aht_dp);
		}

		distinct_input_count = 0;
		for (uint16_t i = 0; i < distinct_count; ++i)
		{
			if (!probe_results[i].inserted)
				continue;
			distinct_rows[distinct_input_count] = probe_results[i].row_idx;
			main_canonical_rows[distinct_input_count] = distinct_main_canonical_rows[i];
			++distinct_input_count;
		}
		if (distinct_input_count > 0)
		{
			main_tdc = ResolveTdc(ctx.dsa, main_part.tdc_dp);
			if (main_tdc == nullptr)
				elog(ERROR, "pg_yaap: local distinct aggregate main TDC missing before update");
			UpdateAggregatesGather(local.layout,
				main_tdc->rows,
				main_tdc->row_width,
				main_canonical_rows.data(),
				in,
				distinct_rows.data(),
				distinct_input_count);
		}
	}

	for (uint32_t part_idx = 0; part_idx < local.partition_count; ++part_idx)
		SyncSegmentUsed(ctx, local.local_partitions[part_idx],
			ResolveTdc(ctx.dsa, local.local_partitions[part_idx].tdc_dp));
	return SinkResultType::NEED_MORE_INPUT;
}

static uint32_t
RequiredHeapBytesForChunkRows(const TupleDataLayout *layout,
                              const PipelineChunk &chunk,
                              const uint16_t *row_indices,
                              uint16_t count)
{
	uint64_t required = 0;
	for (uint16_t i = 0; i < count; ++i)
		required += TupleDataCollectionRequiredHeapBytesForChunkRow(layout, chunk, row_indices[i]);
	return required > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(required);
}

static bool
PartitionNeedsGrowForChunkBatch(const TupleDataLayout *layout,
                                const TupleDataCollection *tdc,
                                const PipelineChunk &chunk,
                                const uint16_t *row_indices,
                                uint16_t count)
{
	if (tdc == nullptr)
		return true;
	const uint32_t row_count = pg_atomic_read_u32(
		const_cast<pg_atomic_uint32 *>(&tdc->row_count));
	if (static_cast<uint64_t>(row_count) + count > tdc->row_capacity)
		return true;
	return !TupleDataCollectionHasSpaceForAppend(tdc,
		RequiredHeapBytesForChunkRows(layout, chunk, row_indices, count));
}

static inline bool
PartitionNeedsGrowForRow(const TupleDataLayout *layout,
	                    const TupleDataCollection *tdc,
	                    const uint8_t *row_ptr)
{
	return !TupleDataCollectionHasSpaceForAppend(
		tdc,
		TupleDataCollectionRequiredHeapBytesForRow(layout, tdc, row_ptr));
}

static void
GatherDenseAggregateOutput(const TupleDataLayout *layout,
	                       const TupleDataCollection *tdc,
	                       const uint8_t *row_ptr,
	                       PipelineChunk &chunk,
	                       uint16_t row_idx)
{
	Assert(layout != nullptr);
	Assert(row_ptr != nullptr);

	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		switch (col.kind)
		{
			case TdcColumnKind::INT32:
			{
				int32_t v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				chunk.int32_columns[i][row_idx] = v;
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				chunk.int64_columns[i][row_idx] = v;
				break;
			}
			case TdcColumnKind::DOUBLE:
			{
				double v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				chunk.double_columns[i][row_idx] = v;
				break;
			}
			case TdcColumnKind::STRING_REF:
			{
				VecStringRef ref;
				std::memcpy(&ref, row_ptr + col.offset, sizeof(ref));
				const char *ptr = VecStringRefDataPtr(
					ref,
					tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc)) : nullptr);
				if (ptr == nullptr && ref.len != 0)
					elog(ERROR, "pg_yaap: dense hashagg gather missing string backing");
				chunk.string_columns[i][row_idx] = chunk.store_string_bytes(ptr, ref.len);
				break;
			}
		}
	}

	for (uint16_t a = 0; a < layout->aggregate_count; ++a)
	{
		const TdcAggregateDesc &agg = layout->aggregates[a];
		int64_t v;
		std::memcpy(&v, row_ptr + agg.offset, sizeof(v));
		if (agg.kind == TdcAggKind::AVG_NUMERIC)
		{
			int64_t count;
			std::memcpy(&count, row_ptr + agg.offset + 8, sizeof(count));
			if (count != 0)
			{
				NumericWideInt widened =
					(WideIntFromInt64(v) * WideIntFromInt64(kDenseAggAvgScaleFactor)) /
					WideIntFromInt64(count);
				if (!WideIntFitsInt64(widened))
						ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
							errmsg("pg_yaap: dense hashagg finalized AVG numeric exceeds int64 range (agg=%u source=%u group-cols=%u row-width=%u sum=%lld count=%lld scale=%d)",
								(unsigned) a, (unsigned) agg.src_col_idx,
								(unsigned) layout->column_count, layout->row_width,
								(long long) v, (long long) count, (int) agg.numeric_scale)));
				v = WideIntToInt64Checked(widened, "pg_yaap: dense hashagg finalized AVG numeric");
			}
			else
				v = 0;
		}
		chunk.int64_columns[layout->column_count + a][row_idx] = v;
	}
}

}  /* namespace */

int
PhysicalHashAggregate::MaxThreads(ExecCtx &ctx) const
{
	const TupleDataLayout *layout = ResolveLayout(ctx.dsa, DsaPointerIsValid(layout_dp_) ? layout_dp_ : LayoutDpFromDescriptor());
	if (GetSingleDistinctCountAgg(layout))
		return 1;
	/* String layouts above four workers use the existing flat DSA local-TDC
	 * path (TryInitSegmentBackedTdc declines segment backing).  That storage
	 * choice must not silently cap scan fan-out; honor the query GUC here. */
	return std::max(1, pg_yaap_parallel_max_workers);
}

dsa_pointer
PhysicalHashAggregate::LayoutDpFromDescriptor() const
{
	if (desc_ == nullptr)
		return InvalidDsaPointer;
	return desc_->kind == OpKind::PERFECT_HASH_AGGREGATE
		? desc_->body.perfect_hash_agg.layout
		: desc_->body.hash_agg.layout;
}

dsa_pointer
PhysicalHashAggregate::SharedPayloadDpFromDescriptor() const
{
	if (desc_ == nullptr)
		return InvalidDsaPointer;
	return desc_->kind == OpKind::PERFECT_HASH_AGGREGATE
		? desc_->body.perfect_hash_agg.shared_payload
		: desc_->body.hash_agg.shared_payload;
}

uint32_t
PhysicalHashAggregate::MaxGroupsFromDescriptor() const
{
	if (desc_ == nullptr)
		return 256;
	const uint32_t max_groups = desc_->kind == OpKind::PERFECT_HASH_AGGREGATE
		? desc_->body.perfect_hash_agg.max_groups
		: desc_->body.hash_agg.max_groups;
	return max_groups > 0 ? max_groups : 256;
}

uint32_t
PhysicalHashAggregate::PerfectHashCapacityFromDescriptor() const
{
	if (desc_ == nullptr)
		return 0;
	return desc_->kind == OpKind::PERFECT_HASH_AGGREGATE
		? desc_->body.perfect_hash_agg.perfect_hash_capacity
		: desc_->body.hash_agg.perfect_hash_capacity;
}

std::unique_ptr<GlobalSinkState>
PhysicalHashAggregate::GetGlobalSinkState(ExecCtx &ctx)
{
	auto state = std::make_unique<HashAggGlobalSinkState>();
	state->dsa = ctx.dsa;
	state->desc = desc_;
	state->layout_dp = DsaPointerIsValid(layout_dp_) ? layout_dp_ : LayoutDpFromDescriptor();
	state->layout = ResolveLayout(ctx.dsa, state->layout_dp);
	state->shared_payload_dp = DsaPointerIsValid(shared_payload_dp_) ? shared_payload_dp_ : SharedPayloadDpFromDescriptor();
	state->max_groups = MaxGroupsFromDescriptor();
	state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);

	if (state->layout == nullptr)
		elog(ERROR, "pg_yaap: hash aggregate missing TupleDataLayout");

	const bool needs_init =
		state->payload == nullptr ||
		!DsaPointerIsValid(state->payload->partitions_dp) ||
		state->payload->partition_count == 0;

	if (ctx.worker_index == LEADER_WORKER_INDEX && needs_init)
	{
		const uint32_t perfect_capacity = PerfectHashCapacityFromDescriptor();
		const bool use_perfect_hash = perfect_capacity > 0 &&
			CanEncodePerfectHashLayout(state->layout);
		const bool use_single_state = !use_perfect_hash &&
			IsSingleStateAggregate(state->layout);
		const uint32_t workers = EffectiveWorkerCount(ctx);
		if (use_single_state)
			state->max_groups = 1;
		state->partition_count = (use_perfect_hash || use_single_state) ? 1u :
			HashAggChoosePartitionCount(workers, state->layout->row_width);
		const uint32_t per_partition_groups = PartitionRowCapacity(state->max_groups, state->partition_count);
		const uint32_t initial_global_partition_rows = std::min(per_partition_groups,
			kInitialGlobalPartitionRows);

		if (!DsaPointerIsValid(state->shared_payload_dp))
			state->shared_payload_dp = dsa_allocate0(ctx.dsa, sizeof(HashAggSharedPayload));
		state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);
		state->payload->partition_count = state->partition_count;
		state->payload->partition_mask = state->partition_count - 1u;
		state->payload->max_groups = state->max_groups;
		state->payload->local_state_slot_count = workers;
		state->payload->perfect_hash_capacity = use_perfect_hash ? perfect_capacity : 0;
		state->payload->finalized = false;
		pg_atomic_init_u32(&state->payload->source_partition_next, 0);
		state->payload->partitions_dp = dsa_allocate0(ctx.dsa,
			static_cast<size_t>(state->partition_count) * sizeof(HashAggPartition));
		state->payload->local_partitions_registry_dp = dsa_allocate0(ctx.dsa,
			static_cast<size_t>(state->payload->local_state_slot_count) * sizeof(dsa_pointer));
		state->payload->publication_dp = InvalidDsaPointer;
		pg_atomic_init_u32(&state->payload->combine_partitions_done, 0);
		state->partitions = ResolvePartitions(ctx.dsa, state->payload);
		auto *registry = ResolveLocalRegistry(ctx.dsa, state->payload);
		for (uint32_t slot = 0; slot < state->payload->local_state_slot_count; ++slot)
			registry[slot] = InvalidDsaPointer;

		for (uint32_t part_idx = 0; part_idx < state->partition_count; ++part_idx)
		{
			HashAggPartition &part = state->partitions[part_idx];
			LWLockInitialize(&part.mutex, LWTRANCHE_PARALLEL_QUERY_DSA);
			if (TryInitSegmentBackedTdc(ctx, part, state->layout_dp, state->layout,
								state->partition_count, initial_global_partition_rows))
			{
				AggregateHashTable *aht = nullptr;
				AllocAhtForTdc(ctx, part.tdc_dp, initial_global_partition_rows,
						   &part.aht_dp, &aht);
				AggregateHashTableEnableSegmentRefs(ctx.dsa, aht, initial_global_partition_rows);
				continue;
			}
			const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->layout,
				initial_global_partition_rows);
			part.tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
				initial_global_partition_rows,
				state->layout->row_width,
				heap_capacity);
			auto *tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
			TupleDataCollectionInit(tdc,
				initial_global_partition_rows,
				state->layout->row_width,
				state->layout_dp,
				heap_capacity);
			AggregateHashTable *aht = nullptr;
			AllocAhtForTdc(ctx, part.tdc_dp, initial_global_partition_rows, &part.aht_dp, &aht);
		}
		if (CanUseSegmentBackedLocalTdc(state->layout, state->partition_count) &&
			SegmentBackingFits(ctx, state->layout, per_partition_groups))
		{
			auto *buffer_state = ResolveBufferState(ctx);
			if (buffer_state != nullptr && DsaPointerIsValid(ctx.control->shared_segment_arena_root))
				state->payload->publication_dp = buffer::YaapSegmentPublicationDirectoryCreate(
					ctx.dsa, buffer_state, 1, state->partition_count, workers);
		}
		StoreSharedPayloadOnDescriptor(this, state->shared_payload_dp);
	}
	else
	{
		if (!DsaPointerIsValid(state->shared_payload_dp))
			state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this);
		state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);
		state->partitions = ResolvePartitions(ctx.dsa, state->payload);
		state->partition_count = state->payload != nullptr ? state->payload->partition_count : 0;
	}

	if (state->payload == nullptr || state->partitions == nullptr || state->partition_count == 0)
		elog(ERROR, "pg_yaap: hash aggregate global payload not initialized");

	return state;
}

std::unique_ptr<LocalSinkState>
PhysicalHashAggregate::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<HashAggGlobalSinkState &>(gstate);
	auto state = std::make_unique<HashAggLocalSinkState>();
	state->layout = global.layout;
	state->layout_dp = global.layout_dp;
	state->max_groups = global.max_groups;
	state->partition_count = global.partition_count;
	state->partition_mask = global.payload->partition_mask;
	state->partition_shift = HashAggPartitionShift(state->partition_mask);
	state->perfect_capacity = global.payload->perfect_hash_capacity;
	state->use_perfect_hash = state->perfect_capacity > 0;
	state->has_distinct_count = GetSingleDistinctCountAgg(state->layout, &state->distinct_agg_idx);
	if (state->has_distinct_count)
	{
		state->use_perfect_hash = false;
		state->perfect_capacity = 0;
	}
	if (state->use_perfect_hash)
	{
		state->partition_count = 1;
		state->partition_mask = 0;
		state->partition_shift = 0;
		state->perfect_row_indices.assign(state->perfect_capacity, TDC_INVALID_ROW_INDEX);
	}
	const uint32_t per_partition_groups = PartitionRowCapacity(state->max_groups, state->partition_count);
	/* Cardinality estimates are global upper bounds.  Reserving that capacity
	 * in every worker/partition multiplies a sparse aggregation (Q1 has four
	 * groups) by workers × partitions.  Start bounded and grow on demand. */
	const uint32_t initial_local_partition_rows = std::min(per_partition_groups,
		kInitialGlobalPartitionRows);

	state->local_partitions_dp = dsa_allocate0(ctx.dsa,
		static_cast<size_t>(state->partition_count) * sizeof(HashAggPartition));
	state->local_partitions = static_cast<HashAggPartition *>(
		dsa_get_address(ctx.dsa, state->local_partitions_dp));
	state->segmented_writers.resize(state->partition_count);

	for (uint32_t part_idx = 0; part_idx < state->partition_count; ++part_idx)
	{
		HashAggPartition &part = state->local_partitions[part_idx];
		LWLockInitialize(&part.mutex, LWTRANCHE_PARALLEL_QUERY_DSA);
		if (!state->use_perfect_hash && !state->has_distinct_count &&
			!IsSingleStateAggregate(state->layout) &&
			global.partitions != nullptr && global.partitions[part_idx].segment_backed &&
			initial_local_partition_rows >= kMinLocalSegmentBackedRows &&
			TryInitSegmentBackedTdc(ctx, part, global.layout_dp, state->layout,
									 state->partition_count, initial_local_partition_rows))
		{
			AggregateHashTable *aht = nullptr;
			AllocAhtForTdc(ctx, part.tdc_dp, initial_local_partition_rows,
						   &part.aht_dp, &aht);
			AggregateHashTableEnableSegmentRefs(ctx.dsa, aht, per_partition_groups);
			auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
				dsa_get_address(ctx.dsa, part.segment_desc_dp));
			auto *writer_tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
			buffer::YaapSegmentedTdcInit(&state->segmented_writers[part_idx],
				part.segment_arena_dp, global.layout_dp,
				static_cast<uint32_t>(std::max(ctx.worker_index, 0)),
					writer_tdc->row_width, writer_tdc->row_capacity, writer_tdc->heap_capacity,
					0, "hash aggregate local writer");
			state->segmented_writers[part_idx].first =
				{segment->arena_id, segment->segment_id, segment->generation};
			state->segmented_writers[part_idx].active =
				{segment->arena_id, segment->segment_id, segment->generation};
			state->segmented_writers[part_idx].segment_count = 1;
			continue;
		}
		const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->layout,
			initial_local_partition_rows);
		part.tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
			initial_local_partition_rows,
			state->layout->row_width,
			heap_capacity);
		auto *tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
		TupleDataCollectionInit(tdc,
			initial_local_partition_rows,
			state->layout->row_width,
			global.layout_dp,
			heap_capacity);
		AggregateHashTable *aht = nullptr;
		AllocAhtForTdc(ctx, part.tdc_dp, initial_local_partition_rows, &part.aht_dp, &aht);
	}
	if (state->has_distinct_count)
		InitDistinctLocalState(ctx, *state, per_partition_groups);

	if (ctx.worker_index >= 0)
	{
		auto *registry = ResolveLocalRegistry(ctx.dsa, global.payload);
		if (registry == nullptr || static_cast<uint32_t>(ctx.worker_index) >= global.payload->local_state_slot_count)
			elog(ERROR, "pg_yaap: hash aggregate local registry missing");
		registry[ctx.worker_index] = state->local_partitions_dp;
	}

	return state;
}

SinkResultType
PhysicalHashAggregate::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input)
{
	auto &local = static_cast<HashAggLocalSinkState &>(input.local_state);
	if (local.layout->column_count == 0 && local.layout->aggregate_count > 0)
		elog(LOG,
		     "pg_yaap: HashAgg.SinkChunk in_count=%u partitions=%u perfect=%d",
		     in.count,
		     local.partition_count,
		     local.use_perfect_hash ? 1 : 0);
	if (local.use_perfect_hash)
	{
		SinkChunkPerfectHash(ctx, local, in);
		return SinkResultType::NEED_MORE_INPUT;
	}
	if (local.has_distinct_count)
		return SinkChunkDistinctCount(ctx, local, in);
	if (IsSingleStateAggregate(local.layout))
	{
		SinkChunkSingleState(ctx, local, in);
		return SinkResultType::NEED_MORE_INPUT;
	}

	std::array<uint64_t, PIPELINE_DEFAULT_CHUNK_SIZE> hashes;
	std::array<uint32_t, PIPELINE_DEFAULT_CHUNK_SIZE> partitions;
	std::array<uint16_t, PIPELINE_DEFAULT_CHUNK_SIZE> probe_rows;
	std::array<AggregateHashTableBatchProbeInput, PIPELINE_DEFAULT_CHUNK_SIZE> probe_inputs;
	std::array<AggregateHashTableBatchProbeResult, PIPELINE_DEFAULT_CHUNK_SIZE> probe_results;
	std::array<uint32_t, PIPELINE_DEFAULT_CHUNK_SIZE> update_canonical_rows;
	std::array<uint16_t, PIPELINE_DEFAULT_CHUNK_SIZE> update_row_indices;
	std::array<uint16_t, PIPELINE_DEFAULT_CHUNK_SIZE> update_partitions;
	uint16_t update_count = 0;

	for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
	{
		if (PipelineCancelRequestedEvery(ctx, row_idx))
			return SinkResultType::FINISHED;
		hashes[row_idx] = HashGroup(local.layout, in, row_idx);
		partitions[row_idx] = static_cast<uint32_t>(hashes[row_idx] >> local.partition_shift) &
			local.partition_mask;
	}

	for (uint32_t part_idx = 0; part_idx < local.partition_count; ++part_idx)
	{
		if (PipelineCancelRequestedEvery(ctx, part_idx))
			return SinkResultType::FINISHED;
		uint16_t probe_count = 0;
		for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
		{
			if (partitions[row_idx] != part_idx)
				continue;
			probe_rows[probe_count++] = row_idx;
		}
		if (probe_count == 0)
			continue;

		HashAggPartition &part = local.local_partitions[part_idx];
		if (part.segment_backed)
		{
			auto *state = ResolveBufferState(ctx);
			if (state == nullptr)
				elog(ERROR, "pg_yaap: local segmented hash aggregate buffer state missing");
			buffer::YaapSegmentedTdc &writer = local.segmented_writers[part_idx];
			for (uint16_t i = 0; i < probe_count; ++i)
			{
				const uint16_t row_idx = probe_rows[i];
				TupleDataCollection *row_store = nullptr;
				uint8_t *row = nullptr;
				auto *aht = ResolveAht(ctx.dsa, part.aht_dp);
				if (aht == nullptr)
					elog(ERROR, "pg_yaap: segmented local aggregate AHT missing");
				if (static_cast<uint64_t>(part.segment_row_count + 1u) * 3u >=
					static_cast<uint64_t>(aht->capacity) * 2u)
				{
					GrowSegmentedLocalAht(ctx, part, part.segment_row_count, local.layout);
					aht = ResolveAht(ctx.dsa, part.aht_dp);
				}
				const bool inserted = AggregateHashTableFindOrInsertSegmented(
					aht, ctx.dsa, part.segment_arena_dp,
					state, &writer, &part.segment_row_count, local.layout, in, row_idx,
					hashes[row_idx], &row_store, &row);
				(void) inserted;
				UpdateAggregates(local.layout, row, in, row_idx);
			}
			auto *tail = buffer::YaapSharedSegmentResolve(ctx.dsa,
				part.segment_arena_dp, writer.active);
			if (tail == nullptr)
				elog(ERROR, "pg_yaap: local segmented hash aggregate tail missing");
			part.segment_desc_dp = tail->self_dp;
			part.segment_id = tail->segment_id;
			part.segment_generation = tail->generation;
			part.tdc_dp = tail->payload_dp;
			continue;
		}
		TupleDataCollection *tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
		if (tdc == nullptr)
			elog(ERROR, "pg_yaap: local hash aggregate partition TDC missing");
		while (PartitionNeedsGrowForChunkBatch(local.layout, tdc, in, probe_rows.data(), probe_count))
		{
			GrowLocalTdc(ctx,
				local,
				part,
				RequiredHeapBytesForChunkRows(local.layout, in, probe_rows.data(), probe_count));
			tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
			if (tdc == nullptr)
				elog(ERROR, "pg_yaap: local hash aggregate partition TDC missing after grow");
		}
		AggregateHashTable *aht = ResolveAht(ctx.dsa, part.aht_dp);
		for (uint16_t i = 0; i < probe_count; ++i)
		{
			const uint16_t row_idx = probe_rows[i];
			probe_inputs[i].row_idx = row_idx;
			probe_inputs[i].hash = hashes[row_idx];
		}

		AggregateHashTableFindOrInsertBatch(aht,
			tdc,
			local.layout,
			in,
			probe_inputs.data(),
			probe_count,
			probe_results.data());
		if (AggregateHashTableShouldResize(aht, tdc))
		{
			ResizeAhtForTdc(ctx,
				part.tdc_dp,
				tdc,
				local.layout,
				&part.aht_dp);
			tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
			if (tdc == nullptr)
				elog(ERROR, "pg_yaap: local hash aggregate partition TDC missing");
		}

		for (uint16_t i = 0; i < probe_count; ++i)
		{
			update_row_indices[update_count] = probe_results[i].row_idx;
			update_canonical_rows[update_count] = probe_results[i].canonical_row_idx;
			update_partitions[update_count] = static_cast<uint16_t>(part_idx);
			++update_count;
		}
	}
	uint16_t update_pos = 0;
	while (update_pos < update_count)
	{
		if (PipelineCancelRequestedEvery(ctx, update_pos))
			return SinkResultType::FINISHED;
		const uint16_t part_idx = update_partitions[update_pos];
		uint16_t run_count = 1;
		while (update_pos + run_count < update_count &&
			update_partitions[update_pos + run_count] == part_idx)
			run_count++;

		TupleDataCollection *tdc = ResolveTdc(ctx.dsa, local.local_partitions[part_idx].tdc_dp);
		if (tdc == nullptr)
			elog(ERROR, "pg_yaap: local hash aggregate partition TDC missing");
		UpdateAggregatesGather(local.layout,
			tdc->rows,
			tdc->row_width,
			update_canonical_rows.data() + update_pos,
			in,
			update_row_indices.data() + update_pos,
			run_count);
		update_pos += run_count;
	}

	for (uint32_t part_idx = 0; part_idx < local.partition_count; ++part_idx)
		SyncSegmentUsed(ctx, local.local_partitions[part_idx],
			ResolveTdc(ctx.dsa, local.local_partitions[part_idx].tdc_dp));
	return SinkResultType::NEED_MORE_INPUT;
}

/* The scheduler assigns one COMBINE task per global partition.  The legacy
 * leader-partial path also uses this helper, under the partition LWLock. */
static void
CombineTdcIntoGlobal(ExecCtx &ctx, HashAggGlobalSinkState &global,
	HashAggPartition &global_part, const TupleDataCollection *local_tdc)
{
	if (global_part.segment_backed)
	{
		CombineLocalRowsIntoSegmentedGlobal(ctx, global_part, local_tdc,
			global.layout_dp, global.layout);
		return;
	}
	const uint32_t count = pg_atomic_read_u32(const_cast<pg_atomic_uint32 *>(&local_tdc->row_count));
	for (uint32_t row_idx = 0; row_idx < count; ++row_idx)
	{
		if (PipelineCancelRequestedEvery(ctx, row_idx))
			return;
		const uint8_t *row = TupleDataCollectionGetRowConst(local_tdc, row_idx);
		TupleDataCollection *tdc = ResolveTdc(ctx.dsa, global_part.tdc_dp);
		if (tdc == nullptr)
			elog(ERROR, "pg_yaap: hash aggregate partition TDC missing");
		while (PartitionNeedsGrowForRow(global.layout, tdc, row))
		{
			GrowTdcForPartition(ctx, global.layout_dp, global.layout, global_part,
				TupleDataCollectionRequiredHeapBytesForRow(global.layout, local_tdc, row));
			tdc = ResolveTdc(ctx.dsa, global_part.tdc_dp);
			if (tdc == nullptr)
				elog(ERROR, "pg_yaap: hash aggregate partition TDC missing after grow");
		}
		AggregateHashTableCombineRow(ResolveAht(ctx.dsa, global_part.aht_dp), tdc,
			global.layout, local_tdc, row, HashGroupRow(global.layout, local_tdc, row));
		ResizeAhtForTdc(ctx, global_part.tdc_dp, tdc, global.layout, &global_part.aht_dp);
	}
}

static void
CombineLocalPartition(ExecCtx &ctx, HashAggGlobalSinkState &global,
	HashAggPartition &global_part, HashAggPartition &local_part, uint32_t partition_id)
{
	PrepareSegmentForRead(ctx, local_part);
	if (!local_part.segment_backed)
	{
		const auto *tdc = ResolveTdc(ctx.dsa, local_part.tdc_dp);
		if (tdc != nullptr)
			CombineTdcIntoGlobal(ctx, global, global_part, tdc);
		ReleasePartitionBacking(ctx, local_part);
		return;
	}
	MaybeSpillLocalPartition(ctx, local_part, partition_id);
	/* The local index is no longer used once the immutable chain is handed to
	 * combine.  Release it before reserving memory for a reload/global growth. */
	if (DsaPointerIsValid(local_part.aht_dp))
		DestroyAht(ctx, local_part.aht_dp);
	local_part.aht_dp = InvalidDsaPointer;
	while (DsaPointerIsValid(local_part.segment_first_desc_dp))
	{
		auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
			dsa_get_address(ctx.dsa, local_part.segment_first_desc_dp));
		if (segment == nullptr)
			elog(ERROR, "pg_yaap: local hash aggregate segment chain missing");
		const dsa_pointer next = segment->chain_next_dp;
		EnsureHashAggGlobalSegmentResident(ctx, local_part.segment_arena_dp, segment);
		const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id,
			segment->generation};
		buffer::YaapSharedSegmentReadLease lease;
		if (!buffer::YaapSharedSegmentAcquireRead(ctx.dsa, local_part.segment_arena_dp, handle, &lease))
			elog(ERROR, "pg_yaap: local hash aggregate read lease failed");
		PG_TRY();
		{
			CombineTdcIntoGlobal(ctx, global, global_part,
				static_cast<const TupleDataCollection *>(lease.data));
		}
		PG_CATCH();
		{
			buffer::YaapSharedSegmentReleaseRead(ctx.dsa, local_part.segment_arena_dp, &lease);
			PG_RE_THROW();
		}
		PG_END_TRY();
		buffer::YaapSharedSegmentReleaseRead(ctx.dsa, local_part.segment_arena_dp, &lease);
		if (!buffer::YaapSharedSegmentRetire(ctx.dsa, ResolveBufferState(ctx),
			local_part.segment_arena_dp, handle))
			elog(ERROR, "pg_yaap: local hash aggregate consumed segment retire failed");
		local_part.segment_first_desc_dp = next;
	}
	ReleasePartitionBacking(ctx, local_part);
}

SinkCombineResultType
PhysicalHashAggregate::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input)
{
	auto &global = static_cast<HashAggGlobalSinkState &>(input.global_state);

	if (input.partition_id == UINT32_MAX)
	{
		if (input.local_state == nullptr)
			elog(ERROR, "pg_yaap: hash aggregate combine local state missing");
		auto &local = static_cast<HashAggLocalSinkState &>(*input.local_state);
		for (uint32_t idx = 0; idx < local.partition_count; ++idx)
		{
			if (PipelineCancelRequestedEvery(ctx, idx))
				return SinkCombineResultType::FINISHED;
			auto &part = global.partitions[idx & global.payload->partition_mask];
			/* Allocation, spill I/O and ERROR are legal under LWLock, never
			 * under a spinlock.  This path can overlap worker combines. */
			LWLockAcquire(&part.mutex, LW_EXCLUSIVE);
			PG_TRY();
			{
				CombineLocalPartition(ctx, global, part, local.local_partitions[idx], idx);
			}
			PG_CATCH();
			{
				LWLockRelease(&part.mutex);
				PG_RE_THROW();
			}
			PG_END_TRY();
			LWLockRelease(&part.mutex);
		}
		return SinkCombineResultType::FINISHED;
	}
	if (input.partition_id >= global.partition_count)
		elog(ERROR, "pg_yaap: hash aggregate combine partition %u out of range", input.partition_id);
	dsa_pointer *registry = ResolveLocalRegistry(ctx.dsa, global.payload);
	if (registry == nullptr)
		elog(ERROR, "pg_yaap: hash aggregate local registry missing");
	HashAggPartition &global_part = global.partitions[input.partition_id];
	LWLockAcquire(&global_part.mutex, LW_EXCLUSIVE);
	PG_TRY();
	{
		for (uint32_t slot = 0; slot < global.payload->local_state_slot_count; ++slot)
		{
			if (PipelineCancelRequestedEvery(ctx, slot))
				return SinkCombineResultType::FINISHED;
			if (!DsaPointerIsValid(registry[slot]))
				continue;
			auto *parts = static_cast<HashAggPartition *>(dsa_get_address(ctx.dsa, registry[slot]));
			CombineLocalPartition(ctx, global, global_part, parts[input.partition_id], input.partition_id);
		}

		SpillHashAggGlobalPartition(ctx, global_part, input.partition_id);
	}
	PG_CATCH();
	{
		LWLockRelease(&global_part.mutex);
		PG_RE_THROW();
	}
	PG_END_TRY();
	LWLockRelease(&global_part.mutex);

	const uint32_t completed = pg_atomic_add_fetch_u32(
		&global.payload->combine_partitions_done, 1);
	if (completed == global.partition_count)
	{
		if (DsaPointerIsValid(global.payload->publication_dp))
		{
			auto *buffer_state = ResolveBufferState(ctx);
			if (buffer_state != nullptr)
				buffer::YaapSegmentPublicationDirectoryDestroy(ctx.dsa, buffer_state,
					global.payload->publication_dp);
			global.payload->publication_dp = InvalidDsaPointer;
		}
		ReleaseLocalPartitionArrays(ctx, global.payload);
	}

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType
PhysicalHashAggregate::Finalize(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<HashAggGlobalSinkState &>(gstate);
	for (uint32_t part_idx = 0; part_idx < global.partition_count; ++part_idx)
	{
		if (PipelineCancelRequestedEvery(ctx, part_idx))
			return SinkFinalizeType::READY;
		HashAggPartition &part = global.partitions[part_idx];
		if (part.segment_backed)
		{
			for (dsa_pointer desc_dp = part.segment_first_desc_dp;
				 DsaPointerIsValid(desc_dp);)
			{
				auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
					dsa_get_address(global.dsa, desc_dp));
				if (segment == nullptr)
					elog(ERROR, "pg_yaap: hash aggregate global segment missing during finalize");
				if (pg_atomic_read_u32(&segment->state) ==
					static_cast<uint32_t>(buffer::YaapSharedSegmentState::SPILLED))
				{
					/* Combine has already sealed this image.  Source uses the
					 * shared payload's finalized flag after lazy reload. */
					desc_dp = segment->chain_next_dp;
					continue;
				}
				if (!DsaPointerIsValid(segment->payload_dp))
					elog(ERROR, "pg_yaap: hash aggregate global segment payload missing during finalize");
				auto *segment_tdc = static_cast<TupleDataCollection *>(
					dsa_get_address(global.dsa, segment->payload_dp));
				if (pg_atomic_read_u32(&segment->state) ==
					static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL))
				{
					const uint64_t used = offsetof(TupleDataCollection, rows) +
						static_cast<uint64_t>(pg_atomic_read_u32(&segment_tdc->row_count)) *
						segment_tdc->row_width +
						pg_atomic_read_u32(&segment_tdc->heap_used);
					buffer::YaapSharedSegmentSetUsed(segment, static_cast<uint32_t>(used));
					buffer::YaapSharedSegmentSeal(segment);
					buffer::YaapSharedSegmentPublish(segment);
				}
				segment_tdc->finalized = true;
				desc_dp = segment->chain_next_dp;
			}
			continue;
		}
		TupleDataCollection *tdc = ResolveTdc(global.dsa, part.tdc_dp);
		if (tdc != nullptr)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG,
					 "pg_yaap: HashAgg.Finalize part=%u rows=%u",
					 part_idx,
					 pg_atomic_read_u32(&tdc->row_count));
			tdc->finalized = true;
		}
	}
	/* Once combine has sealed all global segments, pressure can evict them as
	 * direct page images.  The row-reference directory and logical segment
	 * handles remain unchanged; source reloads the same segment on demand. */
	if (auto *buffer_state = ResolveBufferState(ctx);
		buffer_state != nullptr && buffer::YaapBufferOverSoftLimit(buffer_state))
	{
		if (ctx.control == nullptr)
			ereport(ERROR, (errmsg("pg_yaap: hash aggregate global spill control missing")));
		buffer::YaapSpillStore spill(ctx.mcxt);
		spill.AttachSharedFileSet(&ctx.control->spill_fileset, "pg_yaap_hashagg_global");
		for (uint32_t part_idx = 0; part_idx < global.partition_count; ++part_idx)
		{
			HashAggPartition &part = global.partitions[part_idx];
			if (!part.segment_backed)
				continue;
			for (dsa_pointer desc_dp = part.segment_first_desc_dp;
				 DsaPointerIsValid(desc_dp);)
			{
				auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
					dsa_get_address(global.dsa, desc_dp));
				if (segment == nullptr)
					ereport(ERROR, (errmsg("pg_yaap: hash aggregate global spill segment missing")));
				const dsa_pointer next_dp = segment->chain_next_dp;
				if (pg_atomic_read_u32(&segment->state) ==
					static_cast<uint32_t>(buffer::YaapSharedSegmentState::PUBLISHED))
				{
					const buffer::YaapSegmentHandle handle{segment->arena_id,
						segment->segment_id, segment->generation};
					buffer::YaapSpillExtent extent{};
					if (!buffer::YaapSharedSegmentSpill(global.dsa, buffer_state, handle,
						part.segment_arena_dp, &spill, &extent))
						ereport(ERROR, (errmsg("pg_yaap: hash aggregate global segment spill failed")));
				}
				desc_dp = next_dp;
			}
		}
	}
	global.payload->finalized = true;
	global.finalized = true;
	return SinkFinalizeType::READY;
}

std::unique_ptr<GlobalSourceState>
PhysicalHashAggregate::GetGlobalSourceState(ExecCtx &ctx)
{
	auto state = std::make_unique<HashAggGlobalSourceState>();
	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_) ? shared_payload_dp_ :
		LoadSharedPayloadFromDescriptor(this);
	state->payload = ResolvePayload(ctx.dsa, payload_dp);
	state->partitions = ResolvePartitions(ctx.dsa, state->payload);
	state->partition_count = state->payload != nullptr ? state->payload->partition_count : 0;
	if (state->payload == nullptr || state->partitions == nullptr || state->partition_count == 0)
		elog(ERROR, "pg_yaap: hash aggregate source payload not initialized");
	const dsa_pointer source_layout_dp = DsaPointerIsValid(layout_dp_)
		? layout_dp_ : LayoutDpFromDescriptor();
	state->layout = ResolveLayout(ctx.dsa, source_layout_dp);
	if (state->layout == nullptr)
		elog(ERROR, "pg_yaap: hash aggregate source layout missing");
	state->finalized = state->payload->finalized;
	return state;
}

std::unique_ptr<LocalSourceState>
PhysicalHashAggregate::GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate)
{
	(void) ctx;
	(void) gstate;
	return std::make_unique<HashAggLocalSourceState>();
}

SourceResultType
PhysicalHashAggregate::GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input)
{
	auto &global = static_cast<HashAggGlobalSourceState &>(input.global_state);
	auto &local = static_cast<HashAggLocalSourceState &>(input.local_state);
	out.reset();

	/* global.finalized is a stale snapshot from GetGlobalSourceState() time;
	 * only the DSA-resident wrapper flag is authoritative across runtimes. */
	if (!global.payload->finalized)
		return SourceResultType::FINISHED;

	while (out.count < PIPELINE_DEFAULT_CHUNK_SIZE)
	{
		if (PipelineCancelRequestedEvery(ctx, local.source_cursor))
			break;
		if (local.source_partition == UINT32_MAX)
		{
			const uint32_t claimed = pg_atomic_fetch_add_u32(
				&global.payload->source_partition_next, 1);
			if (claimed >= global.partition_count)
				break;
			local.source_partition = claimed;
			local.source_cursor = 0;
			local.source_segment_desc_dp = InvalidDsaPointer;
		}
		HashAggPartition &part = global.partitions[local.source_partition];
		if (part.segment_backed)
		{
			if (!DsaPointerIsValid(local.source_segment_desc_dp))
				local.source_segment_desc_dp = part.segment_first_desc_dp;
			while (DsaPointerIsValid(local.source_segment_desc_dp))
			{
				auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
					dsa_get_address(ctx.dsa, local.source_segment_desc_dp));
				if (segment == nullptr)
					elog(ERROR, "pg_yaap: hash aggregate source segment missing");
				EnsureHashAggGlobalSegmentResident(ctx, part.segment_arena_dp, segment);
				auto *segment_tdc = static_cast<TupleDataCollection *>(
					dsa_get_address(ctx.dsa, segment->payload_dp));
				const uint32_t segment_rows = pg_atomic_read_u32(&segment_tdc->row_count);
				buffer::YaapSharedSegmentReadLease lease;
				const buffer::YaapSegmentHandle handle{segment->arena_id,
					segment->segment_id, segment->generation};
				if (!buffer::YaapSharedSegmentAcquireRead(ctx.dsa, part.segment_arena_dp,
					handle, &lease))
					ereport(ERROR, (errmsg("pg_yaap: hash aggregate source segment read lease failed")));
				/* A single lease covers the output window.  Gather copies string
				 * bytes into the output chunk, so the consumed page can retire. */
				PG_TRY();
				{
					while (local.source_cursor < segment_rows && out.count < PIPELINE_DEFAULT_CHUNK_SIZE)
					{
						const uint8_t *row = TupleDataCollectionGetRowConst(segment_tdc,
							local.source_cursor++);
						GatherDenseAggregateOutput(global.layout, segment_tdc, row, out, out.count++);
					}
				}
				PG_CATCH();
				{
					buffer::YaapSharedSegmentReleaseRead(ctx.dsa, part.segment_arena_dp, &lease);
					PG_RE_THROW();
				}
				PG_END_TRY();
				buffer::YaapSharedSegmentReleaseRead(ctx.dsa, part.segment_arena_dp, &lease);
				if (local.source_cursor >= segment_rows)
				{
					const dsa_pointer next = segment->chain_next_dp;
					if (!buffer::YaapSharedSegmentRetire(ctx.dsa, ResolveBufferState(ctx),
						part.segment_arena_dp, handle))
						ereport(ERROR, (errmsg("pg_yaap: hash aggregate source consumed segment retire failed")));
					part.segment_first_desc_dp = next;
					local.source_segment_desc_dp = next;
					local.source_cursor = 0;
				}
				if (out.count == PIPELINE_DEFAULT_CHUNK_SIZE)
					break;
			}

			if (!DsaPointerIsValid(local.source_segment_desc_dp))
			{
				RetireHashAggGlobalPartition(ctx, part);
				local.source_partition = UINT32_MAX;
				local.source_cursor = 0;
				continue;
			}
			continue;
		}
		TupleDataCollection *tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
		if (tdc == nullptr || !tdc->finalized)
			return SourceResultType::FINISHED;

		const uint32_t row_count = pg_atomic_read_u32(&tdc->row_count);
		if (local.source_cursor >= row_count)
		{
			local.source_partition = UINT32_MAX;
			local.source_cursor = 0;
			local.source_segment_desc_dp = InvalidDsaPointer;
			continue;
		}

		const uint8_t *row = TupleDataCollectionGetRowConst(tdc, local.source_cursor++);
		GatherDenseAggregateOutput(global.layout, tdc, row, out, out.count);
		++out.count;
	}

	return out.count > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

std::unique_ptr<OperatorState>
PhysicalHashAggregate::GetOperatorState(ExecCtx &ctx)
{
	(void) ctx;
	return std::make_unique<HashAggOperatorState>();
}

OperatorResultType
PhysicalHashAggregate::Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state)
{
	(void) ctx;
	auto &op_state = static_cast<HashAggOperatorState &>(state);
	if (op_state.current_input_drained)
	{
		op_state.current_input_drained = false;
		out.reset();
		return OperatorResultType::NEED_MORE_INPUT;
	}
	out = in;
	op_state.current_input_drained = out.count > 0;
	return out.count > 0 ? OperatorResultType::HAVE_MORE_OUTPUT
	                      : OperatorResultType::NEED_MORE_INPUT;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
