#include "core/dsa_allocation.hpp"
#include "parallel/pipeline/physical_hash_join.hpp"
#include "parallel/pipeline/physical_hash_join_probe.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/elog.h"

extern int pg_yaap_parallel_max_workers;
extern bool pg_yaap_trace_execution_path;
extern bool pg_yaap_trace_hooks;
extern bool pg_yaap_hash_join_partition_probe;
}

#include <algorithm>
#include <array>
#include <cstring>
#include <string_view>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/cancel.hpp"
#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/physical_hash_join_combine.hpp"
#include "parallel/pipeline/physical_hash_join_fast_probe.hpp"
#include "parallel/pipeline/physical_seq_scan.hpp"
#include "parallel/pipeline/pipeline_profile.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"
#include "buffer_manager/yaap_segment_row_refs.hpp"
#include "buffer_manager/yaap_segmented_tdc.hpp"
#include "buffer_manager/yaap_spill_store.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

/* Producers publish immutable local segments; Finalize assembles their row
 * directory only after the combine barrier. */
static constexpr bool kEnableDirectSegmentedHashJoin = true;
static constexpr bool kEnablePostCombineSegmentConversion = false;
static constexpr long kHashJoinSegmentWaitPollUs = 100L;

static constexpr uint32_t HASH_JOIN_INVALID_ROW = UINT32_MAX;
/* Wide payload joins (Q10/Q18) must grow on demand instead of reserving a
 * large flat row store before filters have reduced the build side. */
static constexpr uint32_t HASH_JOIN_MAX_INITIAL_ROWS = 1u << 16;

static uint32_t
EffectiveWorkerCount(const ExecCtx &ctx)
{
	if (ctx.control != nullptr && ctx.control->num_workers > 0)
		return static_cast<uint32_t>(ctx.control->num_workers);
	return static_cast<uint32_t>(std::max(1, pg_yaap_parallel_max_workers));
}

static uint32_t
HashJoinChoosePartitionCount(uint32_t workers)
{
	if (workers <= 1)
		return 1;
	/* Two radix partitions per worker keeps parallel finalize work balanced
	 * without eagerly creating thousands of tiny per-worker segment chains. */
	uint32_t target = std::min<uint32_t>(32u, std::max<uint32_t>(8u, workers * 2u));
	uint32_t partitions = 1;
	while (partitions < target)
		partitions <<= 1;
	return partitions;
}

static uint32_t
HashJoinPartitionShift(uint32_t partition_count)
{
	uint32_t bits = 0;
	for (uint32_t n = partition_count; n > 1; n >>= 1)
		++bits;
	return 64u - bits;
}

static uint32_t
HashJoinPartitionIndex(uint64_t hash, const HashJoinSharedPayload *payload)
{
	return payload->build_partition_count == 1 ? 0 :
		static_cast<uint32_t>(hash >> payload->partition_shift) & payload->partition_mask;
}

static HashJoinPartition *
ResolveHashJoinPartitions(dsa_area *dsa, HashJoinSharedPayload *payload)
{
	return payload != nullptr && DsaPointerIsValid(payload->partitions_dp)
		? static_cast<HashJoinPartition *>(dsa_get_address(dsa, payload->partitions_dp)) : nullptr;
}

static inline uint16_t
HashJoinSalt(uint64_t hash)
{
	return static_cast<uint16_t>(hash >> 48);
}

static uint32_t
HashJoinInitialRows(uint32_t estimated_rows)
{
	return std::max<uint32_t>(1024u, std::min<uint32_t>(estimated_rows, HASH_JOIN_MAX_INITIAL_ROWS));
}

static HashJoinSharedPayload *
ResolvePayload(dsa_area *dsa, dsa_pointer payload_dp)
{
	if (!DsaPointerIsValid(payload_dp))
		return nullptr;
	return static_cast<HashJoinSharedPayload *>(dsa_get_address(dsa, payload_dp));
}

static const TupleDataLayout *
ResolveLayout(dsa_area *dsa, dsa_pointer layout_dp)
{
	if (!DsaPointerIsValid(layout_dp))
		return nullptr;
	return static_cast<const TupleDataLayout *>(dsa_get_address(dsa, layout_dp));
}

static TupleDataCollection *
ResolveTdc(dsa_area *dsa, dsa_pointer tdc_dp)
{
	if (!DsaPointerIsValid(tdc_dp))
		return nullptr;
	return static_cast<TupleDataCollection *>(dsa_get_address(dsa, tdc_dp));
}

static HashJoinLocalBuildRegistryEntry *
ResolveLocalBuildRegistry(dsa_area *dsa, HashJoinSharedPayload *payload)
{
	if (payload == nullptr || !DsaPointerIsValid(payload->local_build_registry_dp))
		return nullptr;
	return static_cast<HashJoinLocalBuildRegistryEntry *>(
		dsa_get_address(dsa, payload->local_build_registry_dp));
}

static void
FreeDsaPointerIfValid(dsa_area *dsa, dsa_pointer *dp)
{
	if (dp != nullptr && DsaPointerIsValid(*dp))
	{
		dsa_free(dsa, *dp);
		*dp = InvalidDsaPointer;
	}
}

static uint64_t
EstimateTdcAllocBytes(dsa_area *dsa, dsa_pointer tdc_dp)
{
	TupleDataCollection *tdc = ResolveTdc(dsa, tdc_dp);
	if (tdc == nullptr)
		return 0;
	return static_cast<uint64_t>(TupleDataCollectionAllocSize(tdc->row_capacity,
		tdc->row_width,
		tdc->heap_capacity));
}

static bool
MatchPercentLikePattern(const char *lhs, uint32_t lhs_len,
						  const char *pattern, uint32_t pattern_len)
{
	if (pattern == nullptr)
		return false;
	std::string_view text(lhs != nullptr ? lhs : "", lhs_len);
	std::string_view spec(pattern, pattern_len);
	if (spec.empty())
		return text.empty();
	if (spec == "%")
		return true;

	size_t text_pos = 0;
	size_t pat_pos = 0;
	const bool anchored_start = spec.front() != '%';
	const bool anchored_end = spec.back() != '%';
	bool first_token = true;

	while (pat_pos < spec.size())
	{
		while (pat_pos < spec.size() && spec[pat_pos] == '%')
			++pat_pos;
		if (pat_pos >= spec.size())
			return true;

		const size_t next_pct = spec.find('%', pat_pos);
		const std::string_view token = spec.substr(
			pat_pos,
			next_pct == std::string_view::npos ? spec.size() - pat_pos : next_pct - pat_pos);
		if (token.empty())
		{
			pat_pos = next_pct;
			continue;
		}

		if (first_token && anchored_start)
		{
			if (text.size() < token.size() || text.substr(0, token.size()) != token)
				return false;
			text_pos = token.size();
		}
		else
		{
			const size_t found = text.find(token, text_pos);
			if (found == std::string_view::npos)
				return false;
			text_pos = found + token.size();
		}
		first_token = false;

		if (next_pct == std::string_view::npos)
			return !anchored_end || text_pos == text.size();
		pat_pos = next_pct;
	}

	return !anchored_end || text_pos == text.size();
}

static void
CopyBuildRow(const TupleDataLayout *layout,
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
			elog(ERROR, "pg_yaap: hash join build-row copy ran out of heap");
		std::memcpy(dst_row + col.offset, &dst_ref, sizeof(dst_ref));
	}
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
PartitionedHashJoinShouldSpill(buffer::YaapBufferSharedState *state)
{
	if (state == nullptr)
		return false;
	/* Reserve most of the query budget for finalize directories, probe chunks,
	 * and sibling breakers. The generic 75% soft limit leaves too little DSA
	 * headroom for high-cardinality radix builds. */
	const uint64_t threshold = std::min<uint64_t>(state->soft_limit,
		state->hard_limit / 4u);
	return buffer::YaapBufferResidentBytes(state) > threshold;
}

static bool LayoutHasStringRef(const TupleDataLayout *layout);

static uint32_t
DirectHashJoinSegmentRows(const ExecCtx &ctx,
						  const TupleDataLayout *key_layout,
						  const TupleDataLayout *row_layout)
{
	if (ctx.control == nullptr || key_layout == nullptr || row_layout == nullptr)
		return 0;
	auto *buffer_state = ResolveBufferState(ctx);
	auto *arena = buffer_state != nullptr
		? buffer::YaapSharedSegmentArenaResolve(ctx.dsa,
			ctx.control->large_shared_segment_arena_root) : nullptr;
	if (arena == nullptr)
		return 0;
	/* A narrow TDC must fill its segment, rather than sealing a 1MiB
	 * allocation after only 8192 small rows. Heap reservations are linear
	 * in row count; use the wider of the key and payload geometries so
	 * both writers fit without changing row-reference or spill layouts. */
	const uint64_t key_bytes_per_row = static_cast<uint64_t>(key_layout->row_width) +
		TupleDataCollectionDefaultHeapCapacity(key_layout, 1);
	const uint64_t row_bytes_per_row = static_cast<uint64_t>(row_layout->row_width) +
		TupleDataCollectionDefaultHeapCapacity(row_layout, 1);
	const uint64_t bytes_per_row = std::max(key_bytes_per_row, row_bytes_per_row);
	const uint64_t header_bytes = offsetof(TupleDataCollection, rows);
	if (bytes_per_row == 0 || arena->segment_size <= header_bytes)
		return 0;
	uint32_t rows = (arena->segment_size - header_bytes) / bytes_per_row;
	/* With a small query budget, partial partition tails must fill early
	 * enough for the existing build spill policy to reclaim them. */
	const uint64_t tail_bytes = static_cast<uint64_t>(EffectiveWorkerCount(ctx)) *
		HashJoinChoosePartitionCount(EffectiveWorkerCount(ctx)) * 2u * arena->segment_size;
	if (buffer_state->hard_limit < tail_bytes * 4u)
	{
		rows = std::min<uint32_t>(rows, 8192u);
		uint32_t rounded = 256u;
		while (rounded <= rows / 2u) rounded *= 2u;
		rows = rows >= 256u ? rounded : 0;
	}
	return rows >= 256u ? rows : 0;
}

static void
SpillDirectHashJoinTail(ExecCtx &ctx, buffer::YaapSegmentedTdc &tdc,
						const char *kind, bool force = false)
{
	if (ctx.control == nullptr || !tdc.active.IsValid())
		return;
	auto *buffer_state = ResolveBufferState(ctx);
	if (buffer_state == nullptr ||
		(!force && !buffer::YaapBufferOverSoftLimit(buffer_state)))
		return;
	auto *segment = buffer::YaapSharedSegmentResolve(ctx.dsa, tdc.arena_dp, tdc.active);
	if (segment == nullptr || pg_atomic_read_u32(&segment->state) !=
		static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL))
		return;
	auto *store = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa,
		segment->payload_dp));
	if (store == nullptr || pg_atomic_read_u32(&store->row_count) == 0)
		return;
	/* Spill before the next append can force a successor allocation.  Waiting
	 * for half a segment is unsafe at the hard limit because one input chunk
	 * may consume the remaining space and require two resident segments. */
	buffer::YaapSegmentedTdcSealPublish(ctx.dsa, &tdc);
	buffer::YaapSpillStore spill(ctx.mcxt);
	char prefix[128];
	std::snprintf(prefix, sizeof(prefix), "pg_yaap_hj_%u_%p_%u_%u_%s",
		static_cast<unsigned>(MyProcPid), static_cast<void *>(&tdc),
		segment->segment_id, segment->generation,
		kind != nullptr ? kind : "data");
	spill.AttachSharedFileSet(&ctx.control->spill_fileset, prefix);
	buffer::YaapSpillExtent extent{};
	const buffer::YaapSegmentHandle handle{segment->arena_id,
		segment->segment_id, segment->generation};
	if (!buffer::YaapSharedSegmentSpill(ctx.dsa, buffer_state, handle,
		tdc.arena_dp, &spill, &extent))
		ereport(ERROR, (errmsg("pg_yaap: hash join tail spill failed")));
}

static void
SpillDirectHashJoinPublishedSegments(ExecCtx &ctx, buffer::YaapSegmentedTdc &tdc,
									const char *kind, bool force = false)
{
	if (ctx.control == nullptr || !tdc.first.IsValid())
		return;
	auto *buffer_state = ResolveBufferState(ctx);
	/* Q10 can consume the whole hard limit between soft-limit checks when a
	 * wide build side keeps appending to one direct chain.  Cap resident
	 * published segments as a second safety valve; active writers remain local
	 * while older immutable segments are safely spillable. */
	constexpr uint32_t kMaxResidentPublishedSegments = 32;
	if (buffer_state == nullptr ||
		(!force && !buffer::YaapBufferOverSoftLimit(buffer_state) &&
		 tdc.segment_count <= kMaxResidentPublishedSegments))
		return;
	auto *first = buffer::YaapSharedSegmentResolve(ctx.dsa, tdc.arena_dp, tdc.first);
	if (first == nullptr)
		return;
	for (dsa_pointer dp = first->self_dp; DsaPointerIsValid(dp);)
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
			std::snprintf(prefix, sizeof(prefix), "pg_yaap_hj_%p_%u_%u_%s",
				static_cast<void *>(&tdc), segment->segment_id, segment->generation,
				kind != nullptr ? kind : "data");
			spill.AttachSharedFileSet(&ctx.control->spill_fileset, prefix);
			buffer::YaapSpillExtent extent{};
			const buffer::YaapSegmentHandle handle{segment->arena_id,
				segment->segment_id, segment->generation};
			if (!buffer::YaapSharedSegmentSpill(ctx.dsa, buffer_state, handle,
				tdc.arena_dp, &spill, &extent))
				return;
		}
		dp = next;
	}
}

static bool
EvictOneHashJoinSegment(ExecCtx &ctx, const buffer::YaapSegmentHandle &exclude,
						uint64_t owner_tag)
{
	if (ctx.control == nullptr || !DsaPointerIsValid(ctx.control->large_shared_segment_arena_root))
		return false;
	auto *arena = buffer::YaapSharedSegmentArenaResolve(ctx.dsa,
		ctx.control->large_shared_segment_arena_root);
	auto *buffer_state = ResolveBufferState(ctx);
	if (arena == nullptr || buffer_state == nullptr)
		return false;
	const dsa_pointer first = arena->first_segment_dp;
	if (!DsaPointerIsValid(first))
		return false;
	SpinLockAcquire(&arena->mutex);
	dsa_pointer start = DsaPointerIsValid(arena->eviction_cursor_dp)
		? arena->eviction_cursor_dp : first;
	SpinLockRelease(&arena->mutex);
	if (dsa_get_address(ctx.dsa, start) == nullptr)
		start = first;
	dsa_pointer dp = start;
	do
	{
		auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
			dsa_get_address(ctx.dsa, dp));
		if (segment == nullptr)
			return false;
		const dsa_pointer next = segment->next_dp;
		const buffer::YaapSegmentHandle handle{segment->arena_id,
			segment->segment_id, segment->generation};
		if (handle.segment_id != exclude.segment_id &&
			(owner_tag == 0 || segment->owner_tag == owner_tag) &&
			pg_atomic_read_u32(&segment->state) ==
				static_cast<uint32_t>(buffer::YaapSharedSegmentState::PUBLISHED) &&
			pg_atomic_read_u32(&segment->readers) == 0)
		{
			buffer::YaapSpillExtent extent{};
			bool evicted = false;
			/* Published build rows are immutable.  If the segment already has
			 * a spill image, discard only the resident copy and keep that image
			 * as the reload source; probe-time eviction must not rewrite it. */
			if (segment->spill_extent.file_name[0] != '\0')
				evicted = buffer::YaapSharedSegmentSpill(ctx.dsa, buffer_state, handle,
					ctx.control->large_shared_segment_arena_root, nullptr, &extent);
			else
			{
				/* A never-spilled published segment needs one durable copy before
				 * it can be evicted. Subsequent evictions use the immutable image. */
				buffer::YaapSpillStore spill(ctx.mcxt);
				char prefix[96];
				std::snprintf(prefix, sizeof(prefix), "pg_yaap_hj_evict_%u_%u",
					handle.segment_id, handle.generation);
				spill.AttachSharedFileSet(&ctx.control->spill_fileset, prefix);
				evicted = buffer::YaapSharedSegmentSpill(ctx.dsa, buffer_state, handle,
					ctx.control->large_shared_segment_arena_root, &spill, &extent);
			}
			if (evicted)
			{
				SpinLockAcquire(&arena->mutex);
				arena->eviction_cursor_dp = DsaPointerIsValid(next) ? next : first;
				SpinLockRelease(&arena->mutex);
				return true;
			}
		}
		dp = DsaPointerIsValid(next) ? next : first;
	}
	while (dp != start);
	return false;
}

static bool
CanUseDirectSegmentedHashJoin(const ExecCtx &ctx,
								 const HashJoinGlobalSinkState &global)
{
	if (!kEnableDirectSegmentedHashJoin)
		return false;
	if (ctx.control == nullptr || global.payload == nullptr ||
		global.payload->build_rows_shared_local ||
		global.build_key_layout == nullptr || global.build_layout == nullptr ||
		global.build_key_layout->column_count == 0 || global.build_key_layout->row_width == 0 ||
		global.build_layout->row_width == 0)
		return false;
	return DirectHashJoinSegmentRows(ctx, global.build_key_layout, global.build_layout) != 0;
}

static bool
BuildSegmentedHashJoinPayload(ExecCtx &ctx, HashJoinGlobalSinkState &global)
{
	if (!kEnablePostCombineSegmentConversion)
		return false;
	HashJoinSharedPayload *payload = global.payload;
	if (payload == nullptr || payload->build_rows_shared_local ||
		global.build_keys == nullptr || global.build_rows == nullptr ||
		global.build_key_layout == nullptr || global.build_layout == nullptr ||
		global.build_key_layout->column_count == 0 || global.build_key_layout->row_width == 0 ||
		global.build_layout->row_width == 0 ||
		LayoutHasStringRef(global.build_key_layout) || LayoutHasStringRef(global.build_layout) ||
		!DsaPointerIsValid(ctx.control != nullptr
			? ctx.control->shared_segment_arena_root : InvalidDsaPointer))
		return false;
	if (payload->direct_segmented_build || payload->segmented_build)
		return true;
	auto *buffer_state = ResolveBufferState(ctx);
	if (buffer_state == nullptr)
		return false;
	/* Building the immutable segment copy temporarily overlaps the combine
	 * TDC.  Under a tight query budget, keep the flat representation rather
	 * than exceeding the hard limit before the copy can be reclaimed. */
	if (buffer_state->hard_limit < 512u * 1024u * 1024u)
		return false;
	const uint32_t row_count = pg_atomic_read_u32(&global.build_keys->row_count);
	/* Segment conversion is also the out-of-core escape hatch; do not reject
	 * a large build merely because its flat staging TDC crossed the old
	 * safety threshold. The input is already fully materialized here, so the
	 * segment path must be allowed to take ownership and release it. */
	if (row_count == 0 || row_count > 2000000u)
		return false;
	dsa_pointer arena_dp = ctx.control->large_shared_segment_arena_root;
	auto *arena = buffer::YaapSharedSegmentArenaResolve(ctx.dsa, arena_dp);
	if (arena == nullptr)
		return false;
	uint32_t row_capacity = std::min<uint32_t>(8192u,
		std::max<uint32_t>(256u, row_count));
	uint32_t key_heap = 0;
	uint32_t row_heap = 0;
	for (;;)
	{
		key_heap = TupleDataCollectionDefaultHeapCapacity(global.build_key_layout, row_capacity);
		row_heap = TupleDataCollectionDefaultHeapCapacity(global.build_layout, row_capacity);
		const uint64_t key_bytes = offsetof(TupleDataCollection, rows) +
			static_cast<uint64_t>(row_capacity) * global.build_key_layout->row_width + key_heap;
		const uint64_t row_bytes = offsetof(TupleDataCollection, rows) +
			static_cast<uint64_t>(row_capacity) * global.build_layout->row_width + row_heap;
		if (key_bytes <= arena->segment_size && row_bytes <= arena->segment_size)
			break;
		if (row_capacity <= 256u)
			return false;
		row_capacity = std::max<uint32_t>(256u, row_capacity / 2u);
	}
	buffer::YaapSegmentedTdc key_tdc;
	buffer::YaapSegmentedTdc row_tdc;
	buffer::YaapSegmentedTdcInit(&key_tdc, arena_dp, global.build_key_layout_dp,
			0, global.build_key_layout->row_width, row_capacity, key_heap,
			0, "hash join converted key writer");
	buffer::YaapSegmentedTdcInit(&row_tdc, arena_dp, global.build_layout_dp,
			0, global.build_layout->row_width, row_capacity, row_heap,
			0, "hash join converted payload writer");
	/* Row-count estimates can be wildly conservative. The side table grows
	 * geometrically as rows are actually published; do not reserve from the
	 * plan estimate before the first build tuple arrives. */
	const uint32_t initial_ref_capacity = std::min<uint32_t>(row_count, 8192u);
	payload->build_key_refs_dp = buffer::YaapSegmentRowRefTableCreate(ctx.dsa,
		std::max<uint32_t>(1, initial_ref_capacity));
	payload->build_row_refs_dp = buffer::YaapSegmentRowRefTableCreate(ctx.dsa,
		std::max<uint32_t>(1, initial_ref_capacity));
	auto *key_refs = static_cast<buffer::YaapSegmentRowRefTable *>(
		dsa_get_address(ctx.dsa, payload->build_key_refs_dp));
	auto *row_refs = static_cast<buffer::YaapSegmentRowRefTable *>(
		dsa_get_address(ctx.dsa, payload->build_row_refs_dp));
	for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
	{
		const uint8_t *src_key = TupleDataCollectionGetRowConst(global.build_keys, row_idx);
		const uint8_t *src_row = TupleDataCollectionGetRowConst(global.build_rows, row_idx);
		buffer::YaapSegmentedRowHandle key_handle;
		TupleDataCollection *key_store = nullptr;
		uint8_t *dst_key = nullptr;
		if (!buffer::YaapSegmentedTdcAppendWithHeap(ctx.dsa, buffer_state, &key_tdc,
			TupleDataCollectionRequiredHeapBytesForRow(global.build_key_layout,
				global.build_keys, src_key), &key_handle, &key_store, &dst_key))
			ereport(ERROR, (errmsg("pg_yaap: hash join segmented key append failed")));
		CopyBuildRow(global.build_key_layout, global.build_keys, src_key, key_store, dst_key);
		auto *key_segment = buffer::YaapSharedSegmentResolve(ctx.dsa, arena_dp,
			key_handle.segment);
		if (key_segment == nullptr || buffer::YaapSegmentRowRefAppend(ctx.dsa, key_refs,
			buffer::YaapSegmentRowRef{key_handle.segment, key_handle.row_index}) != row_idx)
			ereport(ERROR, (errmsg("pg_yaap: hash join segmented key reference append failed")));
		buffer::YaapSegmentedRowHandle row_handle;
		TupleDataCollection *row_store = nullptr;
		uint8_t *dst_row = nullptr;
		if (!buffer::YaapSegmentedTdcAppendWithHeap(ctx.dsa, buffer_state, &row_tdc,
			TupleDataCollectionRequiredHeapBytesForRow(global.build_layout,
				global.build_rows, src_row), &row_handle, &row_store, &dst_row))
			ereport(ERROR, (errmsg("pg_yaap: hash join segmented payload append failed")));
		CopyBuildRow(global.build_layout, global.build_rows, src_row, row_store, dst_row);
		auto *row_segment = buffer::YaapSharedSegmentResolve(ctx.dsa, arena_dp,
			row_handle.segment);
		if (row_segment == nullptr || buffer::YaapSegmentRowRefAppend(ctx.dsa, row_refs,
			buffer::YaapSegmentRowRef{row_handle.segment, row_handle.row_index}) != row_idx)
			ereport(ERROR, (errmsg("pg_yaap: hash join segmented payload reference append failed")));
	}
	buffer::YaapSegmentedTdcSealPublish(ctx.dsa, &key_tdc);
	buffer::YaapSegmentedTdcSealPublish(ctx.dsa, &row_tdc);
	if (buffer::YaapBufferOverSoftLimit(buffer_state))
	{
		buffer::YaapSpillStore spill(ctx.mcxt);
		char prefix[96];
		std::snprintf(prefix, sizeof(prefix), "pg_yaap_hashjoin_%u",
			static_cast<unsigned>(MyProcPid));
		spill.AttachSharedFileSet(&ctx.control->spill_fileset, prefix);
		auto spill_chain = [&](const buffer::YaapSegmentedTdc &segmented)
		{
			for (dsa_pointer dp = segmented.first.IsValid()
				? buffer::YaapSharedSegmentResolve(ctx.dsa, segmented.arena_dp,
					segmented.first)->self_dp : InvalidDsaPointer;
				DsaPointerIsValid(dp);)
			{
				auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
					dsa_get_address(ctx.dsa, dp));
				if (segment == nullptr)
					ereport(ERROR, (errmsg("pg_yaap: hash join spill segment descriptor missing")));
				const dsa_pointer next = segment->chain_next_dp;
				const buffer::YaapSegmentHandle handle{segment->arena_id,
					segment->segment_id, segment->generation};
				buffer::YaapSpillExtent extent{};
				if (!buffer::YaapSharedSegmentSpill(ctx.dsa, buffer_state, handle,
					segmented.arena_dp, &spill, &extent))
					ereport(ERROR, (errmsg("pg_yaap: hash join segment spill failed")));
				dp = next;
			}
		};
		spill_chain(key_tdc);
		spill_chain(row_tdc);
	}
	payload->segment_arena_dp = arena_dp;
	payload->build_key_segment_desc_dp = buffer::YaapSegmentedTdcPublishDesc(ctx.dsa, key_tdc);
	payload->build_row_segment_desc_dp = buffer::YaapSegmentedTdcPublishDesc(ctx.dsa, row_tdc);
	payload->segmented_row_count = row_count;
	payload->segmented_build = true;
	return true;
}

/* Small per-chunk lease caches keep nearby probe rows on resident segments.
 * Key and payload leases remain separate because resolving a payload must not
 * evict the key segment while the caller is still comparing it. */
class HashJoinSegmentLeaseCache
{
public:
	static constexpr uint32_t kCapacity = 4;

	buffer::YaapSharedSegmentReadLease *Get(dsa_area *dsa,
											 dsa_pointer arena_dp,
											 const buffer::YaapSegmentHandle &handle)
	{
		for (uint32_t i = 0; i < leases_.size(); ++i)
		{
			auto &lease = leases_[i];
			if (lease.IsValid() && SameHandle(lease.handle, handle))
			{
				last_used_[i] = ++clock_;
				return &lease;
			}
		}
		for (uint32_t i = 0; i < leases_.size(); ++i)
		{
			if (!leases_[i].IsValid())
			{
				last_used_[i] = ++clock_;
				return &leases_[i];
			}
		}
		uint32_t victim = 0;
		for (uint32_t i = 0; i < leases_.size(); ++i)
		{
			if (last_used_[i] < last_used_[victim])
				victim = i;
		}
		buffer::YaapSharedSegmentReleaseRead(dsa, arena_dp, &leases_[victim]);
		last_used_[victim] = 0;
		return &leases_[victim];
	}

	void ReleaseAll(dsa_area *dsa, dsa_pointer arena_dp)
	{
		for (auto &lease : leases_)
			buffer::YaapSharedSegmentReleaseRead(dsa, arena_dp, &lease);
		last_used_.fill(0);
		clock_ = 0;
	}

	void Release(dsa_area *dsa, dsa_pointer arena_dp)
	{
		ReleaseAll(dsa, arena_dp);
	}

private:
	static bool SameHandle(const buffer::YaapSegmentHandle &left,
						   const buffer::YaapSegmentHandle &right)
	{
		return left.arena_id == right.arena_id &&
			left.segment_id == right.segment_id &&
			left.generation == right.generation;
	}

	std::array<buffer::YaapSharedSegmentReadLease, kCapacity> leases_{};
	std::array<uint64_t, kCapacity> last_used_{};
	uint64_t clock_ = 0;
};

struct HashJoinReadWindow
{
	dsa_area *dsa;
	dsa_pointer arena_dp;
	HashJoinSegmentLeaseCache key;
	HashJoinSegmentLeaseCache row;
	buffer::YaapSpillStore spill_store;

	HashJoinReadWindow(ExecCtx &ctx, dsa_pointer segment_arena_dp)
		: dsa(ctx.dsa), arena_dp(segment_arena_dp), spill_store(ctx.mcxt)
	{
		if (ctx.control != nullptr)
			spill_store.AttachSharedFileSet(&ctx.control->spill_fileset, nullptr);
	}

	buffer::YaapSharedSegmentReadLease *Lease(bool key_row,
											 const buffer::YaapSegmentHandle &handle)
	{
		return (key_row ? key : row).Get(dsa, arena_dp, handle);
	}

	void ReleaseLeases()
	{
		key.ReleaseAll(dsa, arena_dp);
		row.ReleaseAll(dsa, arena_dp);
	}

	void Release()
	{
		ReleaseLeases();
		spill_store.Close();
	}
};

static void
ReleaseHashJoinReadWindow(int, Datum arg)
{
	reinterpret_cast<HashJoinReadWindow *>(DatumGetPointer(arg))->Release();
}

static void
AddHashJoinProfileCount(const ExecCtx &ctx,
						bool enabled,
						PipelineProfileStage stage)
{
	if (!enabled)
		return;
	instr_time zero;
	INSTR_TIME_SET_ZERO(zero);
	PipelineProfileAddElapsed(ctx.control, ctx.dsa, ctx.worker_index,
		ctx.profile_event_id, stage, zero);
}

class HashJoinProfileScopeIfEnabled
{
public:
	HashJoinProfileScopeIfEnabled(const ExecCtx &ctx,
								 PipelineProfileStage stage,
								 bool enabled)
		: ctx_(ctx), stage_(stage), active_(enabled)
	{
		if (active_)
			INSTR_TIME_SET_CURRENT(start_);
	}

	~HashJoinProfileScopeIfEnabled()
	{
		if (!active_)
			return;
		instr_time end;
		INSTR_TIME_SET_CURRENT(end);
		PipelineProfileAddDiff(ctx_, stage_, end, start_);
	}

	HashJoinProfileScopeIfEnabled(const HashJoinProfileScopeIfEnabled &) = delete;
	HashJoinProfileScopeIfEnabled &operator=(const HashJoinProfileScopeIfEnabled &) = delete;

private:
	const ExecCtx &ctx_;
	PipelineProfileStage stage_;
	instr_time start_{};
	bool active_;
};

static bool
SameHashJoinSpillExtent(const buffer::YaapSpillExtent &left,
						const buffer::YaapSpillExtent &right)
{
	return left.fileno == right.fileno && left.offset == right.offset &&
		left.bytes == right.bytes &&
		std::strcmp(left.file_name, right.file_name) == 0;
}

static const uint8_t *
AcquireSegmentedHashJoinRowFromRefs(ExecCtx &ctx,
									const HashJoinSharedPayload *payload,
									dsa_pointer refs_dp,
									uint32_t row_idx,
									HashJoinReadWindow *window,
									bool key_row,
									TupleDataCollection **store,
									bool profile_probe = false)
{
	if (payload == nullptr || window == nullptr || store == nullptr ||
		!DsaPointerIsValid(refs_dp) || !DsaPointerIsValid(payload->segment_arena_dp))
		return nullptr;
	const bool profile = profile_probe && PipelineProfileEnabled(ctx);
	const buffer::YaapSegmentRowRef *ref = nullptr;
	if (profile)
	{
		PipelineProfileScope ref_lookup_scope(ctx,
			PipelineProfileStage::HASH_JOIN_SEGMENT_REF_LOOKUP);
		auto *refs = static_cast<const buffer::YaapSegmentRowRefTable *>(
			dsa_get_address(ctx.dsa, refs_dp));
		ref = buffer::YaapSegmentRowRefGet(ctx.dsa, refs, row_idx);
	}
	else
	{
		auto *refs = static_cast<const buffer::YaapSegmentRowRefTable *>(
			dsa_get_address(ctx.dsa, refs_dp));
		ref = buffer::YaapSegmentRowRefGet(ctx.dsa, refs, row_idx);
	}
	if (ref == nullptr)
		return nullptr;
	auto *lease = window->Lease(key_row, ref->segment);
	if (lease == nullptr)
		return nullptr;
	if (lease->IsValid() && lease->handle.arena_id == ref->segment.arena_id &&
		lease->handle.segment_id == ref->segment.segment_id &&
		lease->handle.generation == ref->segment.generation)
	{
		HashJoinProfileScopeIfEnabled hit_scope(ctx,
			PipelineProfileStage::HASH_JOIN_SEGMENT_LEASE_HIT, profile);
		*store = const_cast<TupleDataCollection *>(
			static_cast<const TupleDataCollection *>(lease->data));
		return TupleDataCollectionGetRowConst(*store, ref->row_index);
	}
	HashJoinProfileScopeIfEnabled miss_scope(ctx,
		PipelineProfileStage::HASH_JOIN_SEGMENT_LEASE_MISS, profile);
	buffer::YaapSharedSegmentDesc *segment = nullptr;
	{
		HashJoinProfileScopeIfEnabled resolve_scope(ctx,
			PipelineProfileStage::HASH_JOIN_SEGMENT_RESOLVE, profile);
		buffer::YaapSharedSegmentReleaseRead(ctx.dsa, payload->segment_arena_dp, lease);
		segment = buffer::YaapSharedSegmentResolve(ctx.dsa,
			payload->segment_arena_dp, ref->segment);
	}
	if (segment == nullptr)
		return nullptr;
	for (uint32_t attempt = 0; attempt < 10000; ++attempt)
	{
		const uint32_t state = pg_atomic_read_u32(&segment->state);
		if (state == static_cast<uint32_t>(buffer::YaapSharedSegmentState::PUBLISHED))
		{
			bool acquired = false;
			if (profile)
			{
				PipelineProfileScope lease_acquire_scope(ctx,
					PipelineProfileStage::HASH_JOIN_SEGMENT_LEASE_ACQUIRE);
				acquired = buffer::YaapSharedSegmentAcquireRead(ctx.dsa,
					payload->segment_arena_dp, ref->segment, lease);
			}
			else
				acquired = buffer::YaapSharedSegmentAcquireRead(ctx.dsa,
					payload->segment_arena_dp, ref->segment, lease);
			if (acquired)
			{
				*store = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa,
					segment->payload_dp));
				if (*store == nullptr)
					ereport(ERROR, (errmsg("pg_yaap: hash join segment %u has no resident TDC after lease",
					segment->segment_id)));
				const uint8_t *row = TupleDataCollectionGetRowConst(*store, ref->row_index);
				if (row == nullptr)
					ereport(ERROR, (errmsg("pg_yaap: hash join segment %u ref row %u exceeds row_count %u",
					segment->segment_id, ref->row_index,
					pg_atomic_read_u32(&(*store)->row_count))));
				return row;
			}
			/* A spiller may have claimed the segment just before this reader. */
			AddHashJoinProfileCount(ctx, profile,
				PipelineProfileStage::HASH_JOIN_SEGMENT_LEASE_ACQUIRE_RETRY);
			CHECK_FOR_INTERRUPTS();
			continue;
		}
		if (state == static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL))
		{
			buffer::YaapSharedSegmentSeal(segment);
			buffer::YaapSharedSegmentPublish(segment);
			continue;
		}
		if (state == static_cast<uint32_t>(buffer::YaapSharedSegmentState::SPILLING) ||
			state == static_cast<uint32_t>(buffer::YaapSharedSegmentState::RELOADING))
		{
			CHECK_FOR_INTERRUPTS();
			if (profile)
			{
				PipelineProfileScope wait_scope(ctx,
					PipelineProfileStage::HASH_JOIN_SEGMENT_WAIT);
				pg_usleep(kHashJoinSegmentWaitPollUs);
			}
			else
				pg_usleep(kHashJoinSegmentWaitPollUs);
			continue;
		}
		if (state != static_cast<uint32_t>(buffer::YaapSharedSegmentState::SPILLED))
			return nullptr;
		if (ctx.control == nullptr)
			return nullptr;
		const buffer::YaapSpillExtent spill_extent = segment->spill_extent;
		instr_time reload_start;
		const bool profile_reload = profile_probe && PipelineProfileEnabled(ctx);
		if (profile_reload)
			INSTR_TIME_SET_CURRENT(reload_start);
		const bool reloaded = buffer::YaapSharedSegmentReload(ctx.dsa,
			ResolveBufferState(ctx), ref->segment,
			payload->segment_arena_dp, &window->spill_store, spill_extent,
			profile_probe ? lease : nullptr);
		if (profile_reload)
		{
			instr_time reload_end;
			INSTR_TIME_SET_CURRENT(reload_end);
			PipelineProfileAddDiff(ctx,
				PipelineProfileStage::HASH_JOIN_SEGMENT_RELOAD,
				reload_end, reload_start);
		}
		if (reloaded)
		{
			AddHashJoinProfileCount(ctx, profile,
				PipelineProfileStage::HASH_JOIN_SEGMENT_RELOAD_OK);
			if (profile_probe)
			{
				*store = const_cast<TupleDataCollection *>(
					static_cast<const TupleDataCollection *>(lease->data));
				const uint8_t *row = TupleDataCollectionGetRowConst(*store,
					ref->row_index);
				if (row == nullptr)
					ereport(ERROR, (errmsg("pg_yaap: reloaded hash join segment row is missing")));
				return row;
			}
			continue;
		}
		AddHashJoinProfileCount(ctx, profile,
			PipelineProfileStage::HASH_JOIN_SEGMENT_RELOAD_RETRY);
		const uint32_t retry_state = pg_atomic_read_u32(&segment->state);
		if (retry_state != static_cast<uint32_t>(buffer::YaapSharedSegmentState::SPILLED))
		{
			AddHashJoinProfileCount(ctx, profile,
				PipelineProfileStage::HASH_JOIN_SEGMENT_RELOAD_RACE);
			continue; /* Another reader already owns or completed this reload. */
		}
		if (!SameHashJoinSpillExtent(spill_extent, segment->spill_extent))
			continue; /* The segment was re-spilled to a different extent. */
		/* A reload can fail because other segments are pinned by this chunk's
		 * locality cache. Drop those leases before asking the arena to evict a
		 * victim, otherwise our own cache can make the retry impossible. */
		window->ReleaseLeases();
		bool evicted = false;
		{
			HashJoinProfileScopeIfEnabled evict_scope(ctx,
				PipelineProfileStage::HASH_JOIN_SEGMENT_EVICT, profile);
			evicted = EvictOneHashJoinSegment(ctx, ref->segment, 0);
		}
		AddHashJoinProfileCount(ctx, profile, evicted
			? PipelineProfileStage::HASH_JOIN_SEGMENT_EVICT_OK
			: PipelineProfileStage::HASH_JOIN_SEGMENT_EVICT_NO_VICTIM);
		if (evicted)
			continue; /* Retry now that a resident victim has been spilled. */
		CHECK_FOR_INTERRUPTS();
		if (profile)
		{
			PipelineProfileScope wait_scope(ctx,
				PipelineProfileStage::HASH_JOIN_SEGMENT_WAIT);
			pg_usleep(kHashJoinSegmentWaitPollUs);
		}
		else
			pg_usleep(kHashJoinSegmentWaitPollUs);
	}
	ereport(ERROR, (errmsg("pg_yaap: hash join timed out waiting for segment %u to become readable; state=%u",
		segment->segment_id, pg_atomic_read_u32(&segment->state))));
	return nullptr;
}

static const uint8_t *
AcquireSegmentedHashJoinPartitionRow(ExecCtx &ctx,
									const HashJoinSharedPayload *payload,
									const HashJoinPartition *partition,
									uint32_t partition_id,
									bool key_row,
									uint32_t row_idx,
									HashJoinReadWindow *window,
									TupleDataCollection **store,
									bool profile_probe = false)
{
	if (payload == nullptr || partition == nullptr || row_idx >= partition->row_count ||
		!DsaPointerIsValid(partition->build_row_slots_dp) ||
		!DsaPointerIsValid(partition->build_row_local_idxs_dp))
		return nullptr;
	dsa_pointer refs_dp = InvalidDsaPointer;
	uint32_t local_idx = 0;
	auto resolve_mapping = [&]() {
		auto *registry = ResolveLocalBuildRegistry(ctx.dsa,
			const_cast<HashJoinSharedPayload *>(payload));
		if (registry == nullptr)
			return false;
		auto *slots = static_cast<const uint32_t *>(dsa_get_address(ctx.dsa,
			partition->build_row_slots_dp));
		auto *local_idxs = static_cast<const uint32_t *>(dsa_get_address(ctx.dsa,
			partition->build_row_local_idxs_dp));
		const uint32_t slot = slots[row_idx];
		if (slot >= payload->local_state_slot_count ||
			!DsaPointerIsValid(registry[slot].partition_state_dp) ||
			partition_id >= payload->build_partition_count)
			return false;
		auto *local_parts = static_cast<const HashJoinLocalPartitionState *>(
			dsa_get_address(ctx.dsa, registry[slot].partition_state_dp));
		const auto &local_part = local_parts[partition_id];
		refs_dp = key_row ? local_part.build_key_refs_dp :
			local_part.build_row_refs_dp;
		local_idx = local_idxs[row_idx];
		return DsaPointerIsValid(refs_dp);
	};
	bool mapping_resolved = false;
	if (profile_probe)
	{
		PipelineProfileScope mapping_scope(ctx,
			PipelineProfileStage::HASH_JOIN_PARTITION_ROW_MAP);
		mapping_resolved = resolve_mapping();
	}
	else
		mapping_resolved = resolve_mapping();
	if (!mapping_resolved)
		return nullptr;
	return AcquireSegmentedHashJoinRowFromRefs(ctx, payload, refs_dp,
		local_idx, window, key_row, store, profile_probe);
}

static void
ReleaseSegmentedHashJoinChain(ExecCtx &ctx, dsa_pointer desc_dp,
							 dsa_pointer arena_dp, bool discard_resident = false)
{
	const auto *desc = buffer::YaapSegmentedTdcResolveDesc(ctx.dsa, desc_dp);
	if (desc == nullptr)
		return;
	auto *state = ResolveBufferState(ctx);
	if (state == nullptr)
		return;
	for (dsa_pointer dp = desc->first_desc_dp; DsaPointerIsValid(dp);)
	{
		auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
			dsa_get_address(ctx.dsa, dp));
		if (segment == nullptr)
			ereport(ERROR, (errmsg("pg_yaap: hash join segmented descriptor missing during release")));
		const dsa_pointer next = segment->chain_next_dp;
		const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id,
			segment->generation};
		uint32_t attempts = 0;
		while (!buffer::YaapSharedSegmentRetire(ctx.dsa, state, arena_dp, handle, discard_resident))
		{
			CHECK_FOR_INTERRUPTS();
			if (!discard_resident || ++attempts >= 100000)
				ereport(ERROR, (errmsg("pg_yaap: hash join segmented chain retire failed")));
			pg_usleep(kHashJoinSegmentWaitPollUs);
		}
		dp = next;
	}
	dsa_free(ctx.dsa, desc_dp);
}

static inline bool
TdcNeedsGrowForChunkRow(const TupleDataLayout *layout,
	                  const TupleDataCollection *tdc,
	                  const PipelineChunk &chunk,
	                  uint16_t row_idx)
{
	return !TupleDataCollectionHasSpaceForAppend(
		tdc,
		TupleDataCollectionRequiredHeapBytesForChunkRow(layout, chunk, row_idx));
}

static inline bool
TdcNeedsGrowForStoredRow(const TupleDataLayout *layout,
	                    const TupleDataCollection *tdc,
	                    const TupleDataCollection *src_tdc,
	                    const uint8_t *src_row)
{
	return !TupleDataCollectionHasSpaceForAppend(
		tdc,
		TupleDataCollectionRequiredHeapBytesForRow(layout, src_tdc, src_row));
}

static bool
LayoutHasStringRef(const TupleDataLayout *layout)
{
	if (layout == nullptr)
		return false;
	for (uint16_t col_idx = 0; col_idx < layout->column_count; ++col_idx)
	{
		if (layout->columns[col_idx].kind == TdcColumnKind::STRING_REF)
			return true;
	}
	return false;
}

static bool
LayoutsEqual(const TupleDataLayout *lhs, const TupleDataLayout *rhs)
{
	if (lhs == rhs)
		return true;
	if (lhs == nullptr || rhs == nullptr)
		return false;
	if (lhs->column_count != rhs->column_count ||
		lhs->aggregate_count != rhs->aggregate_count ||
		lhs->row_width != rhs->row_width ||
		lhs->validity_width != rhs->validity_width)
		return false;
	return std::memcmp(lhs->columns,
		rhs->columns,
		sizeof(TdcColumnDesc) * lhs->column_count) == 0 &&
		std::memcmp(lhs->aggregates,
			rhs->aggregates,
			sizeof(TdcAggregateDesc) * lhs->aggregate_count) == 0;
}

static bool
OutputColumnsReferenceRight(const HashJoinOutputColumnDesc *output_columns,
					   uint16_t output_column_count)
{
	if (output_columns == nullptr)
		return false;
	for (uint16_t i = 0; i < output_column_count; ++i)
	{
		if (output_columns[i].side == HashJoinOutputSide::RIGHT &&
			output_columns[i].decode_kind != ColumnDecodeKind::NONE)
			return true;
	}
	return false;
}

static bool
TryHashSingleJoinKeyRow(const TupleDataLayout *layout,
	                    const TupleDataCollection *tdc,
	                    const uint8_t *row_ptr,
	                    uint64_t &out_hash)
{
	if (layout == nullptr || row_ptr == nullptr || layout->column_count != 1)
		return false;
	const TdcColumnDesc &col = layout->columns[0];
	switch (col.kind)
	{
		case TdcColumnKind::INT32:
		{
			int32_t v;
			std::memcpy(&v, row_ptr + col.offset, sizeof(v));
			out_hash = HashSingleGroupInt32Value(v);
			return true;
		}
		case TdcColumnKind::INT64:
		{
			int64_t v;
			std::memcpy(&v, row_ptr + col.offset, sizeof(v));
			out_hash = HashSingleGroupInt64Value(v);
			return true;
		}
		case TdcColumnKind::DOUBLE:
		case TdcColumnKind::STRING_REF:
			return false;
	}
	return false;
}

static bool
TryHashSingleJoinKeyChunkRow(const TupleDataLayout *layout,
	                         const PipelineChunk &chunk,
	                         uint16_t row_idx,
	                         uint64_t &out_hash)
{
	if (layout == nullptr || layout->column_count != 1)
		return false;
	const TdcColumnDesc &col = layout->columns[0];
	if (col.src_col_idx >= 16)
		return false;
	switch (col.kind)
	{
		case TdcColumnKind::INT32:
			out_hash = HashSingleGroupInt32Value(chunk.get_int32(col.src_col_idx, row_idx));
			return true;
		case TdcColumnKind::INT64:
			out_hash = HashSingleGroupInt64Value(chunk.get_int64(col.src_col_idx, row_idx));
			return true;
		case TdcColumnKind::DOUBLE:
		case TdcColumnKind::STRING_REF:
			return false;
	}
	return false;
}

static const uint8_t *
ResolveSharedLocalBuildRowCached(dsa_area *dsa,
						 const HashJoinSharedPayload *payload,
						 const HashJoinLocalBuildRegistryEntry *registry,
						 const uint32_t *slots,
						 const uint32_t *local_idxs,
						 uint32_t build_row_idx,
						 const TupleDataCollection **out_rows_tdc)
{
	if (payload == nullptr || !payload->build_rows_shared_local ||
		registry == nullptr || slots == nullptr || local_idxs == nullptr)
		elog(ERROR, "pg_yaap: shared-local hash join payload mappings missing");
	const uint32_t slot = slots[build_row_idx];
	if (slot >= payload->local_state_slot_count)
		elog(ERROR, "pg_yaap: shared-local hash join registry slot out of range");
	const TupleDataCollection *rows_tdc = ResolveTdc(dsa, registry[slot].build_rows_dp);
	if (rows_tdc == nullptr)
		elog(ERROR, "pg_yaap: shared-local hash join row store missing");
	const uint32_t local_row_idx = local_idxs[build_row_idx];
	if (local_row_idx >= pg_atomic_read_u32(const_cast<pg_atomic_uint32 *>(&rows_tdc->row_count)))
		elog(ERROR, "pg_yaap: shared-local hash join local row index out of range");
	if (out_rows_tdc != nullptr)
		*out_rows_tdc = rows_tdc;
	return TupleDataCollectionGetRowConst(rows_tdc, local_row_idx);
}

static void
GrowJoinTdc(ExecCtx &ctx,
	       const TupleDataLayout *layout,
	       dsa_pointer layout_dp,
	       dsa_pointer *tdc_dp,
	       uint32_t required_heap_bytes)
{
	dsa_pointer old_tdc_dp = *tdc_dp;
	TupleDataCollection *old_tdc = ResolveTdc(ctx.dsa, *tdc_dp);
	if (old_tdc == nullptr)
		elog(ERROR, "pg_yaap: hash join TDC missing during grow");

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
	if (pg_yaap_trace_execution_path)
		ereport(LOG,
			(errmsg("pg_yaap hashjoin tdc grow old_count=%u old_capacity=%u new_capacity=%u row_width=%u heap_capacity=%u required_heap=%u",
				old_count,
				old_tdc->row_capacity,
				new_capacity,
				layout->row_width,
				heap_capacity,
				required_heap_bytes)));
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
			elog(ERROR, "pg_yaap: hash join TDC grow overflow");
		const uint8_t *src = TupleDataCollectionGetRowConst(old_tdc, row_idx);
		CopyBuildRow(layout, old_tdc, src, new_tdc, dst);
	}

	*tdc_dp = new_tdc_dp;
	dsa_free(ctx.dsa, old_tdc_dp);
}

static void
UpdateHashJoinLocalBuildRegistry(ExecCtx &ctx,
	                            HashJoinSharedPayload *payload,
	                            HashJoinLocalSinkState &local)
{
	if (ctx.worker_index < 0)
		return;
	auto *registry = ResolveLocalBuildRegistry(ctx.dsa, payload);
	if (registry == nullptr || static_cast<uint32_t>(ctx.worker_index) >= payload->local_state_slot_count)
		elog(ERROR, "pg_yaap: hash join local build registry missing during grow");
	registry[ctx.worker_index].build_keys_dp = local.build_keys_dp;
	registry[ctx.worker_index].build_rows_dp = local.build_rows_dp;
}

static void
EnsureJoinLocalCapacity(ExecCtx &ctx,
	                  HashJoinSharedPayload *payload,
	                  dsa_pointer key_layout_dp,
	                  const TupleDataLayout *key_layout,
	                  dsa_pointer row_layout_dp,
	                  const TupleDataLayout *row_layout,
	                  HashJoinLocalSinkState &local,
	                  const PipelineChunk &chunk,
	                  uint16_t row_idx)
{
	while (TdcNeedsGrowForChunkRow(key_layout, local.build_keys, chunk, row_idx) ||
	       (!payload->build_rows_use_keys &&
	        TdcNeedsGrowForChunkRow(row_layout, local.build_rows, chunk, row_idx)))
	{
		if (TdcNeedsGrowForChunkRow(key_layout, local.build_keys, chunk, row_idx))
			GrowJoinTdc(ctx,
				key_layout,
				key_layout_dp,
				&local.build_keys_dp,
				TupleDataCollectionRequiredHeapBytesForChunkRow(key_layout, chunk, row_idx));
		if (!payload->build_rows_use_keys &&
			TdcNeedsGrowForChunkRow(row_layout, local.build_rows, chunk, row_idx))
			GrowJoinTdc(ctx,
				row_layout,
				row_layout_dp,
				&local.build_rows_dp,
				TupleDataCollectionRequiredHeapBytesForChunkRow(row_layout, chunk, row_idx));
		if (payload->build_rows_use_keys)
			local.build_rows_dp = local.build_keys_dp;
		local.build_keys = ResolveTdc(ctx.dsa, local.build_keys_dp);
		local.build_rows = payload->build_rows_use_keys
			? local.build_keys
			: ResolveTdc(ctx.dsa, local.build_rows_dp);
		if (local.build_keys == nullptr || local.build_rows == nullptr)
			elog(ERROR, "pg_yaap: hash join local build rows missing after grow");
		UpdateHashJoinLocalBuildRegistry(ctx, payload, local);
	}
}

static void
EnsureJoinGlobalCapacity(ExecCtx &ctx,
	                   HashJoinGlobalSinkState &global,
	                   const uint8_t *src_key_row,
	                   const TupleDataCollection *src_key_tdc,
	                   const uint8_t *src_row,
	                   const TupleDataCollection *src_row_tdc)
{
	global.build_keys = ResolveTdc(ctx.dsa, global.payload->build_keys_dp);
	global.build_rows = global.payload->build_rows_use_keys
		? global.build_keys
		: ResolveTdc(ctx.dsa, global.payload->build_rows_dp);
	if (global.build_keys == nullptr || global.build_rows == nullptr)
		elog(ERROR, "pg_yaap: hash join global build rows missing before grow");
	while (TdcNeedsGrowForStoredRow(global.build_key_layout,
			global.build_keys,
			src_key_tdc,
			src_key_row) ||
	       (!global.payload->build_rows_use_keys &&
	        TdcNeedsGrowForStoredRow(global.build_layout,
			global.build_rows,
			src_row_tdc,
			src_row)))
	{
		if (TdcNeedsGrowForStoredRow(global.build_key_layout,
				global.build_keys,
				src_key_tdc,
				src_key_row))
			GrowJoinTdc(ctx,
				global.build_key_layout,
				global.build_key_layout_dp,
				&global.payload->build_keys_dp,
				TupleDataCollectionRequiredHeapBytesForRow(global.build_key_layout,
					src_key_tdc,
					src_key_row));
		if (!global.payload->build_rows_use_keys &&
			TdcNeedsGrowForStoredRow(global.build_layout,
				global.build_rows,
				src_row_tdc,
				src_row))
			GrowJoinTdc(ctx,
				global.build_layout,
				global.build_layout_dp,
				&global.payload->build_rows_dp,
				TupleDataCollectionRequiredHeapBytesForRow(global.build_layout,
					src_row_tdc,
					src_row));
		if (global.payload->build_rows_use_keys)
			global.payload->build_rows_dp = global.payload->build_keys_dp;
		global.build_keys = ResolveTdc(ctx.dsa, global.payload->build_keys_dp);
		global.build_rows = global.payload->build_rows_use_keys
			? global.build_keys
			: ResolveTdc(ctx.dsa, global.payload->build_rows_dp);
		if (global.build_keys == nullptr || global.build_rows == nullptr)
			elog(ERROR, "pg_yaap: hash join global build rows missing after grow");
	}
}

static void
EnsureJoinPartitionCapacity(ExecCtx &ctx, HashJoinSharedPayload *payload,
		HashJoinPartition &part, const TupleDataLayout *key_layout,
		dsa_pointer key_layout_dp, const TupleDataLayout *row_layout,
		dsa_pointer row_layout_dp, const TupleDataCollection *src_keys,
		const uint8_t *src_key, const TupleDataCollection *src_rows, const uint8_t *src_row)
{
	auto *keys = ResolveTdc(ctx.dsa, part.build_keys_dp);
	auto *rows = payload->build_rows_use_keys ? keys : ResolveTdc(ctx.dsa, part.build_rows_dp);
	if (keys == nullptr || rows == nullptr)
		elog(ERROR, "pg_yaap: hash join partition build store missing");
	while (TdcNeedsGrowForStoredRow(key_layout, keys, src_keys, src_key) ||
		(!payload->build_rows_use_keys && TdcNeedsGrowForStoredRow(row_layout, rows, src_rows, src_row)))
	{
		if (TdcNeedsGrowForStoredRow(key_layout, keys, src_keys, src_key))
			GrowJoinTdc(ctx, key_layout, key_layout_dp, &part.build_keys_dp,
				TupleDataCollectionRequiredHeapBytesForRow(key_layout, src_keys, src_key));
		if (!payload->build_rows_use_keys && TdcNeedsGrowForStoredRow(row_layout, rows, src_rows, src_row))
			GrowJoinTdc(ctx, row_layout, row_layout_dp, &part.build_rows_dp,
				TupleDataCollectionRequiredHeapBytesForRow(row_layout, src_rows, src_row));
		if (payload->build_rows_use_keys)
			part.build_rows_dp = part.build_keys_dp;
		keys = ResolveTdc(ctx.dsa, part.build_keys_dp);
		rows = payload->build_rows_use_keys ? keys : ResolveTdc(ctx.dsa, part.build_rows_dp);
	}
}

static bool
ColumnDecodeKindToTdcKind(ColumnDecodeKind decode_kind, TdcColumnKind &out)
{
	switch (decode_kind)
	{
		case ColumnDecodeKind::INT32_CHAR:
		case ColumnDecodeKind::INT32_DATE:
		case ColumnDecodeKind::INT32_INT4:
			out = TdcColumnKind::INT32;
			return true;
		case ColumnDecodeKind::INT64_INT8:
		case ColumnDecodeKind::INT64_NUMERIC_SCALED:
			out = TdcColumnKind::INT64;
			return true;
		case ColumnDecodeKind::DOUBLE_FLOAT8:
			out = TdcColumnKind::DOUBLE;
			return true;
		case ColumnDecodeKind::STRING_REF:
			out = TdcColumnKind::STRING_REF;
			return true;
		case ColumnDecodeKind::NONE:
			return false;
	}
	return false;
}

static const TdcColumnDesc *
FindRightColumnBySlotAndKind(const TupleDataLayout *right_layout,
					 uint8_t input_chunk_slot,
					 ColumnDecodeKind decode_kind)
{
	TdcColumnKind expected_kind;
	if (!ColumnDecodeKindToTdcKind(decode_kind, expected_kind))
		elog(ERROR, "pg_yaap: invalid hash join right payload decode kind %u",
		     static_cast<unsigned>(decode_kind));
	for (uint16_t col_idx = 0; col_idx < right_layout->column_count; ++col_idx)
	{
		const TdcColumnDesc &col = right_layout->columns[col_idx];
		if (col.src_col_idx == input_chunk_slot && col.kind == expected_kind)
			return &col;
	}
	return nullptr;
}

static const uint8_t *
AcquireSegmentedHashJoinRow(ExecCtx &ctx, const HashJoinSharedPayload *payload,
								uint32_t row_idx, bool key, HashJoinReadWindow *window,
								TupleDataCollection **store)
{
	if (payload == nullptr || row_idx >= payload->segmented_row_count)
		return nullptr;
	return AcquireSegmentedHashJoinRowFromRefs(ctx, payload,
		key ? payload->build_key_refs_dp : payload->build_row_refs_dp,
		row_idx, window, key, store);
}

static bool
ReadPromotedFilterInt64FromProbe(const HashJoinFilterInputDesc &input,
								 const PipelineChunk &probe_chunk,
								 uint16_t probe_row_idx,
								 int64_t &out_value)
{
	switch (input.source_decode_kind)
	{
		case ColumnDecodeKind::INT32_CHAR:
		case ColumnDecodeKind::INT32_DATE:
		case ColumnDecodeKind::INT32_INT4:
			out_value = static_cast<int64_t>(probe_chunk.get_int32(input.input_chunk_slot, probe_row_idx));
			return true;
		case ColumnDecodeKind::INT64_INT8:
		case ColumnDecodeKind::INT64_NUMERIC_SCALED:
			out_value = probe_chunk.get_int64(input.input_chunk_slot, probe_row_idx);
			return true;
		default:
			return false;
	}
}

static bool
ReadPromotedFilterInt64FromBuild(const HashJoinFilterInputDesc &input,
								 const TdcColumnDesc *right_col,
								 const uint8_t *build_row,
								 int64_t &out_value)
{
	switch (input.source_decode_kind)
	{
		case ColumnDecodeKind::INT32_CHAR:
		case ColumnDecodeKind::INT32_DATE:
		case ColumnDecodeKind::INT32_INT4:
		{
			int32_t value = 0;
			std::memcpy(&value, build_row + right_col->offset, sizeof(value));
			out_value = static_cast<int64_t>(value);
			return true;
		}
		case ColumnDecodeKind::INT64_INT8:
		case ColumnDecodeKind::INT64_NUMERIC_SCALED:
			std::memcpy(&out_value, build_row + right_col->offset, sizeof(out_value));
			return true;
		default:
			return false;
	}
}

static FilterExprDesc *
ResolveFilterExprs(dsa_area *dsa, dsa_pointer dp)
{
	if (!DsaPointerIsValid(dp))
		return nullptr;
	return static_cast<FilterExprDesc *>(dsa_get_address(dsa, dp));
}

static FilterStep *
ResolveFilterSteps(dsa_area *dsa, dsa_pointer dp)
{
	if (!DsaPointerIsValid(dp))
		return nullptr;
	return static_cast<FilterStep *>(dsa_get_address(dsa, dp));
}

static HashJoinFilterInputDesc *
ResolveFilterInputs(dsa_area *dsa, dsa_pointer dp)
{
	if (!DsaPointerIsValid(dp))
		return nullptr;
	return static_cast<HashJoinFilterInputDesc *>(dsa_get_address(dsa, dp));
}

static inline const char *
ResolveFilterConstPtr(const FilterStep &step, const char *string_consts)
{
	if (step.const_len == 0)
		return "";
	if (step.const_offset == UINT32_MAX)
		return reinterpret_cast<const char *>(&step.const_value);
	if (string_consts == nullptr)
		return nullptr;
	return string_consts + step.const_offset;
}

static inline uint16_t
RequiredFilterBoolRegs(const FilterExprDesc *exprs, uint16_t n_exprs)
{
	uint16_t max_reg = 0;
	for (uint16_t expr_idx = 0; expr_idx < n_exprs; ++expr_idx)
		max_reg = std::max<uint16_t>(max_reg, static_cast<uint16_t>(exprs[expr_idx].output_bool_reg + 1));
	return max_reg;
}

static inline bool
Pow10Int64Local(uint8_t exp, int64_t &out)
{
	int64_t value = 1;
	for (uint8_t i = 0; i < exp; ++i)
	{
		if (value > PG_INT64_MAX / 10)
			return false;
		value *= 10;
	}
	out = value;
	return true;
}

static inline bool
RescaleNumericForCompare(int64_t value, uint8_t from_scale, uint8_t to_scale, int64_t &out)
{
	if (from_scale == to_scale)
	{
		out = value;
		return true;
	}
	if (from_scale > to_scale)
		return false;
	int64_t factor = 0;
	if (!Pow10Int64Local(static_cast<uint8_t>(to_scale - from_scale), factor))
		return false;
	NumericWideInt widened = WideIntFromInt64(value) * WideIntFromInt64(factor);
	if (!WideIntFitsInt64(widened))
		return false;
	out = WideIntToInt64Checked(widened, "pg_yaap: numeric compare rescale overflow");
	return true;
}

struct SimpleJoinFilterInputRef {
	const HashJoinFilterInputDesc *input = nullptr;
	const TdcColumnDesc *right_col = nullptr;
};

struct SimpleJoinFilterSpec {
	bool valid = false;
	FilterStepOp op = FilterStepOp::BOOL_NOT;
	QualOp cmp_op = QualOp::EQ;
	SimpleJoinFilterInputRef left;
	SimpleJoinFilterInputRef right;
};

static inline bool
IsSimpleInt32JoinFilterInput(const HashJoinFilterInputDesc &input)
{
	return input.decode_kind == input.source_decode_kind &&
		(input.decode_kind == ColumnDecodeKind::INT32_CHAR ||
		 input.decode_kind == ColumnDecodeKind::INT32_DATE ||
		 input.decode_kind == ColumnDecodeKind::INT32_INT4);
}

static inline bool
IsSimpleInt64JoinFilterInput(const HashJoinFilterInputDesc &input)
{
	return input.decode_kind == input.source_decode_kind &&
		(input.decode_kind == ColumnDecodeKind::INT64_INT8 ||
		 input.decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED);
}

static bool
TryBuildSimpleJoinFilterSpec(const HashJoinFilterInputDesc *inputs,
							   uint16_t n_inputs,
							   const FilterExprDesc *exprs,
							   uint16_t n_exprs,
							   const FilterStep *steps,
							   uint16_t n_steps,
							   const TupleDataLayout *build_layout,
							   SimpleJoinFilterSpec &out)
{
	out = {};
	if (inputs == nullptr || exprs == nullptr || steps == nullptr || build_layout == nullptr ||
		n_exprs != 1 || n_steps != 1)
		return false;

	const FilterExprDesc &expr = exprs[0];
	const FilterStep &step = steps[0];
	if (expr.first_step_idx != 0 || expr.n_steps != 1 ||
		step.out_bool_reg != expr.output_bool_reg ||
		step.left_idx >= n_inputs || step.right_idx >= n_inputs ||
		inputs[step.left_idx].side == inputs[step.right_idx].side)
		return false;

	const HashJoinFilterInputDesc &left_input = inputs[step.left_idx];
	const HashJoinFilterInputDesc &right_input = inputs[step.right_idx];
	if (step.op == FilterStepOp::INT32_CMP_VAR)
	{
		if (!IsSimpleInt32JoinFilterInput(left_input) ||
			!IsSimpleInt32JoinFilterInput(right_input))
			return false;
	}
	else if (step.op == FilterStepOp::INT64_CMP_VAR)
	{
		if (!IsSimpleInt64JoinFilterInput(left_input) ||
			!IsSimpleInt64JoinFilterInput(right_input))
			return false;
		if (left_input.decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED &&
			right_input.decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED &&
			left_input.numeric_scale != right_input.numeric_scale)
			return false;
	}
	else
		return false;

	out.op = step.op;
	out.cmp_op = step.cmp_op;
	out.left.input = &left_input;
	out.right.input = &right_input;
	if (left_input.side == HashJoinOutputSide::RIGHT)
	{
		out.left.right_col = FindRightColumnBySlotAndKind(build_layout,
			left_input.input_chunk_slot,
			left_input.source_decode_kind);
		if (out.left.right_col == nullptr)
			return false;
	}
	if (right_input.side == HashJoinOutputSide::RIGHT)
	{
		out.right.right_col = FindRightColumnBySlotAndKind(build_layout,
			right_input.input_chunk_slot,
			right_input.source_decode_kind);
		if (out.right.right_col == nullptr)
			return false;
	}
	out.valid = true;
	return true;
}

static inline int32_t
ReadSimpleJoinFilterInt32(const SimpleJoinFilterInputRef &input,
						  const PipelineChunk &probe_chunk,
						  uint16_t probe_row_idx,
						  const uint8_t *build_row)
{
	if (input.input == nullptr)
		elog(ERROR, "pg_yaap: simple hash join filter input missing");
	if (input.input->side == HashJoinOutputSide::LEFT)
		return probe_chunk.get_int32(input.input->input_chunk_slot, probe_row_idx);
	if (build_row == nullptr || input.right_col == nullptr)
		elog(ERROR, "pg_yaap: simple hash join filter build input missing");
	int32_t value = 0;
	std::memcpy(&value, build_row + input.right_col->offset, sizeof(value));
	return value;
}

static inline int64_t
ReadSimpleJoinFilterInt64(const SimpleJoinFilterInputRef &input,
						  const PipelineChunk &probe_chunk,
						  uint16_t probe_row_idx,
						  const uint8_t *build_row)
{
	if (input.input == nullptr)
		elog(ERROR, "pg_yaap: simple hash join filter input missing");
	if (input.input->side == HashJoinOutputSide::LEFT)
		return probe_chunk.get_int64(input.input->input_chunk_slot, probe_row_idx);
	if (build_row == nullptr || input.right_col == nullptr)
		elog(ERROR, "pg_yaap: simple hash join filter build input missing");
	int64_t value = 0;
	std::memcpy(&value, build_row + input.right_col->offset, sizeof(value));
	return value;
}

static inline bool
EvaluateSimpleJoinFilter(const SimpleJoinFilterSpec &spec,
						 const PipelineChunk &probe_chunk,
						 uint16_t probe_row_idx,
						 const uint8_t *build_row)
{
	if (!spec.valid)
		return false;
	if (spec.op == FilterStepOp::INT32_CMP_VAR)
	{
		const int32_t l = ReadSimpleJoinFilterInt32(spec.left, probe_chunk, probe_row_idx, build_row);
		const int32_t r = ReadSimpleJoinFilterInt32(spec.right, probe_chunk, probe_row_idx, build_row);
		switch (spec.cmp_op)
		{
			case QualOp::LE: return l <= r;
			case QualOp::LT: return l <  r;
			case QualOp::EQ: return l == r;
			case QualOp::GE: return l >= r;
			case QualOp::GT: return l >  r;
			case QualOp::NE: return l != r;
		}
	}
	const int64_t l = ReadSimpleJoinFilterInt64(spec.left, probe_chunk, probe_row_idx, build_row);
	const int64_t r = ReadSimpleJoinFilterInt64(spec.right, probe_chunk, probe_row_idx, build_row);
	switch (spec.cmp_op)
	{
		case QualOp::LE: return l <= r;
		case QualOp::LT: return l <  r;
		case QualOp::EQ: return l == r;
		case QualOp::GE: return l >= r;
		case QualOp::GT: return l >  r;
		case QualOp::NE: return l != r;
	}
	return false;
}

static inline bool
EvalFilterStep(const FilterStep &step,
	       const HashJoinFilterInputDesc *inputs,
	       const DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE> &filter_chunk,
	       const char *string_consts,
	       uint8_t *bool_values)
{
	bool result = false;
	switch (step.op)
	{
		case FilterStepOp::INT32_CMP_CONST:
		{
			if (!filter_chunk.nulls[step.left_idx][0])
			{
				const int32_t l = filter_chunk.get_int32(step.left_idx, 0);
				const int32_t r = static_cast<int32_t>(step.const_value);
				switch (step.cmp_op)
				{
					case QualOp::LE: result = l <= r; break;
					case QualOp::LT: result = l <  r; break;
					case QualOp::EQ: result = l == r; break;
					case QualOp::GE: result = l >= r; break;
					case QualOp::GT: result = l >  r; break;
					case QualOp::NE: result = l != r; break;
				}
			}
			break;
		}
		case FilterStepOp::INT64_CMP_CONST:
		{
			if (!filter_chunk.nulls[step.left_idx][0])
			{
				const int64_t l = filter_chunk.get_int64(step.left_idx, 0);
				const int64_t r = static_cast<int64_t>(step.const_value);
				switch (step.cmp_op)
				{
					case QualOp::LE: result = l <= r; break;
					case QualOp::LT: result = l <  r; break;
					case QualOp::EQ: result = l == r; break;
					case QualOp::GE: result = l >= r; break;
					case QualOp::GT: result = l >  r; break;
					case QualOp::NE: result = l != r; break;
				}
			}
			break;
		}
		case FilterStepOp::INT32_CMP_VAR:
		{
			if (!filter_chunk.nulls[step.left_idx][0] && !filter_chunk.nulls[step.right_idx][0])
			{
				const int32_t l = filter_chunk.get_int32(step.left_idx, 0);
				const int32_t r = filter_chunk.get_int32(step.right_idx, 0);
				switch (step.cmp_op)
				{
					case QualOp::LE: result = l <= r; break;
					case QualOp::LT: result = l <  r; break;
					case QualOp::EQ: result = l == r; break;
					case QualOp::GE: result = l >= r; break;
					case QualOp::GT: result = l >  r; break;
					case QualOp::NE: result = l != r; break;
				}
			}
			break;
		}
		case FilterStepOp::INT64_CMP_VAR:
		{
			if (!filter_chunk.nulls[step.left_idx][0] && !filter_chunk.nulls[step.right_idx][0])
			{
				int64_t l = filter_chunk.get_int64(step.left_idx, 0);
				int64_t r = filter_chunk.get_int64(step.right_idx, 0);
				if (inputs != nullptr &&
					inputs[step.left_idx].decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED &&
					inputs[step.right_idx].decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED)
				{
					static int numeric_compare_debug_budget = 8;
					const int64_t orig_l = l;
					const int64_t orig_r = r;
					const uint8_t target_scale = std::max(inputs[step.left_idx].numeric_scale,
						inputs[step.right_idx].numeric_scale);
					if (!RescaleNumericForCompare(l, inputs[step.left_idx].numeric_scale, target_scale, l) ||
						!RescaleNumericForCompare(r, inputs[step.right_idx].numeric_scale, target_scale, r))
						elog(ERROR, "pg_yaap: hash join numeric filter rescale failed");
					if (pg_yaap_trace_hooks && numeric_compare_debug_budget-- > 0)
						elog(LOG,
							 "pg_yaap: hash join numeric compare left=%lld(scale=%u)->%lld right=%lld(scale=%u)->%lld target=%u op=%u",
							 (long long) orig_l,
							 (unsigned) inputs[step.left_idx].numeric_scale,
							 (long long) l,
							 (long long) orig_r,
							 (unsigned) inputs[step.right_idx].numeric_scale,
							 (long long) r,
							 (unsigned) target_scale,
							 (unsigned) step.cmp_op);
				}
				switch (step.cmp_op)
				{
					case QualOp::LE: result = l <= r; break;
					case QualOp::LT: result = l <  r; break;
					case QualOp::EQ: result = l == r; break;
					case QualOp::GE: result = l >= r; break;
					case QualOp::GT: result = l >  r; break;
					case QualOp::NE: result = l != r; break;
				}
			}
			break;
		}
		case FilterStepOp::STRING_EQ_CONST:
		case FilterStepOp::STRING_NE_CONST:
		{
			if (!filter_chunk.nulls[step.left_idx][0])
			{
				const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, 0);
				const char *lhs = filter_chunk.get_string_ptr(step.left_idx, 0);
				const char *rhs = ResolveFilterConstPtr(step, string_consts);
				const bool eq = (lhs != nullptr || ref.len == 0) && rhs != nullptr &&
					ref.len == step.const_len &&
					(step.const_len == 0 || std::memcmp(lhs, rhs, step.const_len) == 0);
				result = (step.op == FilterStepOp::STRING_EQ_CONST) ? eq : !eq;
			}
			break;
		}
		case FilterStepOp::STRING_PREFIX_LIKE:
		{
			if (!filter_chunk.nulls[step.left_idx][0])
			{
				const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, 0);
				const char *rhs = ResolveFilterConstPtr(step, string_consts);
				result = rhs != nullptr && ref.len >= step.const_len;
				if (result && step.const_len != 0)
				{
					if (step.const_len <= sizeof(ref.prefix))
						result = std::memcmp(&ref.prefix, rhs, step.const_len) == 0;
					else
					{
						const char *lhs = filter_chunk.get_string_ptr(step.left_idx, 0);
						result = lhs != nullptr && std::memcmp(lhs, rhs, step.const_len) == 0;
					}
				}
			}
			break;
		}
		case FilterStepOp::STRING_CONTAINS_LIKE:
		{
			if (!filter_chunk.nulls[step.left_idx][0])
			{
				const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, 0);
				const char *rhs = ResolveFilterConstPtr(step, string_consts);
				const char *lhs = filter_chunk.get_string_ptr(step.left_idx, 0);
				result = rhs != nullptr && lhs != nullptr && step.const_len <= ref.len;
				if (result)
				{
					for (uint32_t start = 0; start + step.const_len <= ref.len; ++start)
					{
						if (std::memcmp(lhs + start, rhs, step.const_len) == 0)
						{
							result = true;
							break;
						}
						result = false;
					}
				}
			}
			break;
		}
		case FilterStepOp::STRING_SQL_LIKE:
		{
			if (!filter_chunk.nulls[step.left_idx][0])
			{
				const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, 0);
				const char *rhs = ResolveFilterConstPtr(step, string_consts);
				const char *lhs = filter_chunk.get_string_ptr(step.left_idx, 0);
				result = MatchPercentLikePattern(lhs, ref.len, rhs, step.const_len);
			}
			break;
		}
		case FilterStepOp::BOOL_AND:
			result = bool_values[step.left_idx] && bool_values[step.right_idx];
			break;
		case FilterStepOp::BOOL_OR:
			result = bool_values[step.left_idx] || bool_values[step.right_idx];
			break;
		case FilterStepOp::BOOL_NOT:
			result = !bool_values[step.left_idx];
			break;
		default:
			elog(ERROR, "pg_yaap: unsupported hash join filter step op %u", static_cast<unsigned>(step.op));
	}
	bool_values[step.out_bool_reg] = result ? 1 : 0;
	return result;
}

static inline void
PopulateJoinFilterChunk(const HashJoinFilterInputDesc *inputs,
			       uint16_t n_inputs,
			       const PipelineChunk &probe_chunk,
			       uint16_t probe_row_idx,
			       const TupleDataLayout *build_layout,
			       const TupleDataCollection *build_rows,
			       const uint8_t *build_row,
			       DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE> &filter_chunk)
{
	filter_chunk.reset_lightweight();
	filter_chunk.count = 1;
	for (uint16_t i = 0; i < n_inputs; ++i)
	{
		const HashJoinFilterInputDesc &input = inputs[i];
		filter_chunk.nulls[i][0] = 0;
		if (input.side == HashJoinOutputSide::LEFT)
		{
			switch (input.decode_kind)
			{
				case ColumnDecodeKind::INT32_CHAR:
				case ColumnDecodeKind::INT32_DATE:
				case ColumnDecodeKind::INT32_INT4:
					filter_chunk.int32_columns[i][0] = probe_chunk.get_int32(input.input_chunk_slot, probe_row_idx);
					break;
				case ColumnDecodeKind::INT64_INT8:
				case ColumnDecodeKind::INT64_NUMERIC_SCALED:
					if (!ReadPromotedFilterInt64FromProbe(input, probe_chunk, probe_row_idx, filter_chunk.int64_columns[i][0]))
						elog(ERROR, "pg_yaap: unsupported LEFT promoted hash join filter decode kind %u->%u",
							 static_cast<unsigned>(input.source_decode_kind),
							 static_cast<unsigned>(input.decode_kind));
					break;
				case ColumnDecodeKind::DOUBLE_FLOAT8:
					filter_chunk.double_columns[i][0] = probe_chunk.get_double(input.input_chunk_slot, probe_row_idx);
					break;
				case ColumnDecodeKind::STRING_REF:
				{
					const VecStringRef src = probe_chunk.get_string_ref(input.input_chunk_slot, probe_row_idx);
					const char *ptr = probe_chunk.get_string_ptr(input.input_chunk_slot, probe_row_idx);
					filter_chunk.string_columns[i][0] = filter_chunk.store_string_bytes(ptr, src.len);
					break;
				}
				case ColumnDecodeKind::NONE:
					elog(ERROR, "pg_yaap: invalid LEFT hash join filter decode kind NONE");
			}
			continue;
		}

		const TdcColumnDesc *right_col = FindRightColumnBySlotAndKind(build_layout,
			input.input_chunk_slot,
			input.source_decode_kind);
		if (right_col == nullptr)
			elog(ERROR, "pg_yaap: hash join filter mapping missing right payload column");
		switch (input.decode_kind)
		{
			case ColumnDecodeKind::INT32_CHAR:
			case ColumnDecodeKind::INT32_DATE:
			case ColumnDecodeKind::INT32_INT4:
				std::memcpy(&filter_chunk.int32_columns[i][0], build_row + right_col->offset, sizeof(int32_t));
				break;
			case ColumnDecodeKind::INT64_INT8:
			case ColumnDecodeKind::INT64_NUMERIC_SCALED:
				if (!ReadPromotedFilterInt64FromBuild(input, right_col, build_row, filter_chunk.int64_columns[i][0]))
					elog(ERROR, "pg_yaap: unsupported RIGHT promoted hash join filter decode kind %u->%u",
						 static_cast<unsigned>(input.source_decode_kind),
						 static_cast<unsigned>(input.decode_kind));
				break;
			case ColumnDecodeKind::DOUBLE_FLOAT8:
				std::memcpy(&filter_chunk.double_columns[i][0], build_row + right_col->offset, sizeof(double));
				break;
			case ColumnDecodeKind::STRING_REF:
			{
				VecStringRef ref;
				std::memcpy(&ref, build_row + right_col->offset, sizeof(ref));
				const char *ptr = VecStringRefDataPtr(ref,
					build_rows != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(build_rows)) : nullptr);
				filter_chunk.string_columns[i][0] = filter_chunk.store_string_bytes(ptr, ref.len);
				break;
			}
			case ColumnDecodeKind::NONE:
				elog(ERROR, "pg_yaap: invalid RIGHT hash join filter decode kind NONE");
		}
	}
}

static inline bool
EvaluateJoinFilter(const HashJoinFilterInputDesc *inputs,
			   uint16_t n_inputs,
			   const FilterExprDesc *exprs,
			   uint16_t n_exprs,
			   const FilterStep *steps,
			   uint16_t n_steps,
			   const char *string_consts,
			   uint16_t required_bool_regs,
			   const PipelineChunk &probe_chunk,
			   uint16_t probe_row_idx,
			   const TupleDataLayout *build_layout,
			   const TupleDataCollection *build_rows,
			   const uint8_t *build_row,
			   DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE> &filter_chunk,
			   uint8_t *bool_values)
{
	if (n_inputs == 0 || n_exprs == 0 || n_steps == 0)
		return true;
	PopulateJoinFilterChunk(inputs,
		n_inputs,
		probe_chunk,
		probe_row_idx,
		build_layout,
		build_rows,
		build_row,
		filter_chunk);
	if (required_bool_regs > 0)
		std::memset(bool_values, 0, required_bool_regs * sizeof(bool_values[0]));
	for (uint16_t expr_idx = 0; expr_idx < n_exprs; ++expr_idx)
	{
		const FilterExprDesc &expr = exprs[expr_idx];
		const uint16_t end_step = static_cast<uint16_t>(expr.first_step_idx + expr.n_steps);
		if (end_step > n_steps)
			elog(ERROR, "pg_yaap: hash join filter expr step range out of bounds");
		for (uint16_t step_idx = expr.first_step_idx; step_idx < end_step; ++step_idx)
			(void) EvalFilterStep(steps[step_idx], inputs, filter_chunk, string_consts, bool_values);
		if (expr.output_bool_reg >= required_bool_regs || bool_values[expr.output_bool_reg] == 0)
			return false;
	}
	return true;
}

class HashJoinOperatorState final : public OperatorState {
public:
	bool initialized = false;
	dsa_pointer batch_dp = InvalidDsaPointer;
	HashJoinProbeCursorState cursor;
	HashJoinFastProbeState fast_probe;
	std::array<uint16_t, PIPELINE_DEFAULT_CHUNK_SIZE> probe_order{};
	std::array<uint64_t, PIPELINE_DEFAULT_CHUNK_SIZE> probe_hashes{};
};

static const uint8_t *
ResolveBatchRow(dsa_area *dsa, const HashJoinBatch *batch,
				const HashJoinBatchRowRef *refs, uint32_t row,
				TupleDataCollection **store)
{
	if (row >= batch->row_count || refs[row].segment_index >= batch->segment_count)
		elog(ERROR, "pg_yaap: batch hash join row reference out of bounds");
	const auto *segments = static_cast<const HashJoinBatchSegment *>(
		dsa_get_address(dsa, batch->segments_dp));
	const auto &segment = segments[refs[row].segment_index];
	*store = static_cast<TupleDataCollection *>(dsa_get_address(dsa, segment.payload_dp));
	if (refs[row].row_offset < offsetof(TupleDataCollection, rows) ||
		refs[row].row_offset > segment.capacity ||
		(*store)->row_width > segment.capacity - refs[row].row_offset)
		elog(ERROR, "pg_yaap: batch hash join row offset out of bounds");
	return reinterpret_cast<const uint8_t *>(*store) + refs[row].row_offset;
}

static void
InitializeHashJoinOutputState(ExecCtx &ctx, PhysicalHashJoin &join,
			      HashJoinOperatorState &state, HashJoinMatchMode join_mode)
{
	const auto *output_columns = DsaPointerIsValid(join.output_columns_dp())
		? static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, join.output_columns_dp()))
		: (join.desc() != nullptr && DsaPointerIsValid(join.desc()->body.hash_join.output_columns)
			? static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa,
				join.desc()->body.hash_join.output_columns)) : nullptr);
	const auto *build_row_layout = ResolveLayout(ctx.dsa, DsaPointerIsValid(join.right_payload_layout_dp())
		? join.right_payload_layout_dp()
		: (join.desc() != nullptr ? join.desc()->body.hash_join.right_payload_layout : InvalidDsaPointer));
	const auto *probe_layout = ResolveLayout(ctx.dsa, DsaPointerIsValid(join.left_key_layout_dp())
		? join.left_key_layout_dp()
		: (join.desc() != nullptr ? join.desc()->body.hash_join.left_key_layout : InvalidDsaPointer));
	const auto *build_key_layout = ResolveLayout(ctx.dsa, DsaPointerIsValid(join.right_key_layout_dp())
		? join.right_key_layout_dp()
		: (join.desc() != nullptr ? join.desc()->body.hash_join.right_key_layout : InvalidDsaPointer));
	const uint16_t output_count = join.output_column_count() > 0 ? join.output_column_count()
		: (join.desc() != nullptr ? join.desc()->body.hash_join.output_column_count : 0);
	if (output_columns == nullptr || build_row_layout == nullptr || probe_layout == nullptr ||
		build_key_layout == nullptr || output_count == 0)
		elog(ERROR, "pg_yaap: hash join output state metadata is incomplete");
	InitializeHashJoinFastProbeState(state.fast_probe, output_columns, output_count,
		build_row_layout, probe_layout, build_key_layout, join_mode,
		join.n_filter_inputs(), join.n_filter_exprs(), join.n_filter_steps());
}

} // namespace

int
PhysicalHashJoin::MaxThreads(ExecCtx &ctx) const
{
	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	HashJoinSharedPayload *payload = ResolvePayload(ctx.dsa, payload_dp);
	if (payload == nullptr)
		return 1;
	return std::max(1, static_cast<int>(payload->local_state_slot_count));
}

void
PhysicalHashJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta)
{
	Assert(children().size() == 2);

	Pipeline &build_pipeline = meta.CreateChildPipeline(current, *this);
	meta.SetSink(build_pipeline, *this);
	build_pipeline.source = nullptr;
	children()[1]->BuildPipelines(build_pipeline, meta);
	/* Radix batches consume segmented equality-key rows. A keyless scalar
	 * comparison (e.g. Q22's balance > AVG) uses flat rows plus residual
	 * predicates; it must remain the ordinary YAAP join operator. */
	const bool force_partition_probe = HashJoinScansBuildResults(join_mode_);
	if (((pg_yaap_parallel_max_workers > 1 && pg_yaap_hash_join_partition_probe) ||
		 force_partition_probe) &&
		n_left_keys_ > 0 && n_right_keys_ > 0)
	{
		probe_stage_ = std::make_unique<PhysicalHashJoinProbe>(*this);
		auto &probe = static_cast<PhysicalHashJoinProbe &>(*probe_stage_);
		children()[0]->BuildPipelines(current, meta);
		meta.AddOperator(current, probe);
	}
	else
	{
		children()[0]->BuildPipelines(current, meta);
		meta.AddOperator(current, *this);
	}
}

std::unique_ptr<GlobalSinkState>
PhysicalHashJoin::GetGlobalSinkState(ExecCtx &ctx)
{
	auto state = std::make_unique<HashJoinGlobalSinkState>();
	state->dsa = ctx.dsa;
	state->desc = desc_;
	state->probe_key_layout_dp = left_key_layout_dp_;
	state->build_key_layout_dp = right_key_layout_dp_;
	state->output_columns_dp = output_columns_dp_;
	state->build_layout_dp = right_payload_layout_dp_;
	state->shared_payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	if (!DsaPointerIsValid(state->probe_key_layout_dp) && desc_ != nullptr)
		state->probe_key_layout_dp = desc_->body.hash_join.left_key_layout;
	if (!DsaPointerIsValid(state->build_key_layout_dp) && desc_ != nullptr)
		state->build_key_layout_dp = desc_->body.hash_join.right_key_layout;
	if (!DsaPointerIsValid(state->build_layout_dp) && desc_ != nullptr)
		state->build_layout_dp = desc_->body.hash_join.right_payload_layout;
	if (!DsaPointerIsValid(state->output_columns_dp) && desc_ != nullptr)
		state->output_columns_dp = desc_->body.hash_join.output_columns;
	state->probe_key_layout = ResolveLayout(ctx.dsa, state->probe_key_layout_dp);
	state->build_key_layout = ResolveLayout(ctx.dsa, state->build_key_layout_dp);
	state->build_layout = ResolveLayout(ctx.dsa, state->build_layout_dp);
	state->output_column_count = output_column_count_ > 0 ? output_column_count_ :
		(desc_ != nullptr ? desc_->body.hash_join.output_column_count : 0);
	if (DsaPointerIsValid(state->output_columns_dp) && state->output_column_count > 0)
		state->output_columns = static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, state->output_columns_dp));
	if (state->probe_key_layout == nullptr)
		elog(ERROR, "pg_yaap: hash join missing probe-side key layout");
	if (state->build_key_layout == nullptr)
		elog(ERROR, "pg_yaap: hash join missing build-side key layout");
	if (state->build_layout == nullptr)
		elog(ERROR, "pg_yaap: hash join missing build-side payload layout");

	if (ctx.worker_index == LEADER_WORKER_INDEX && !DsaPointerIsValid(state->shared_payload_dp))
	{
		state->shared_payload_dp = dsa_allocate0(ctx.dsa, sizeof(HashJoinSharedPayload));
		state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);
		state->payload->local_state_slot_count = EffectiveWorkerCount(ctx);
		state->payload->build_partition_count = HashJoinChoosePartitionCount(
			state->payload->local_state_slot_count);
		const HashJoinMatchMode join_mode = desc_ != nullptr
			? desc_->body.hash_join.join_mode : join_mode_;
		if (HashJoinScansBuildResults(join_mode) && state->payload->build_partition_count < 2)
			state->payload->build_partition_count = 2;
		state->payload->hash_table_capacity = 0;
		state->payload->radix_bits = 64u - HashJoinPartitionShift(
			state->payload->build_partition_count);
		state->payload->partition_mask = state->payload->build_partition_count - 1u;
		state->payload->partition_shift = HashJoinPartitionShift(
			state->payload->build_partition_count);
		state->payload->partitions_dp = InvalidDsaPointer;
		pg_atomic_init_u32(&state->payload->combine_partitions_done, 0);
		pg_atomic_init_u32(&state->payload->finalize_partitions_done, 0);
		state->payload->combined = false;
		const auto *output_columns = DsaPointerIsValid(output_columns_dp_)
			? static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, output_columns_dp_))
			: (desc_ != nullptr && DsaPointerIsValid(desc_->body.hash_join.output_columns)
				? static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, desc_->body.hash_join.output_columns))
				: nullptr);
		const uint16_t output_column_count = output_column_count_ > 0 ? output_column_count_ :
			(desc_ != nullptr ? desc_->body.hash_join.output_column_count : 0);
		const uint16_t n_filter_inputs = n_filter_inputs_ > 0 ? n_filter_inputs_ :
			(desc_ != nullptr ? desc_->body.hash_join.n_filter_inputs : 0);
		const uint16_t n_filter_exprs = n_filter_exprs_ > 0 ? n_filter_exprs_ :
			(desc_ != nullptr ? desc_->body.hash_join.n_filter_exprs : 0);
		const uint16_t n_filter_steps = n_filter_steps_ > 0 ? n_filter_steps_ :
			(desc_ != nullptr ? desc_->body.hash_join.n_filter_steps : 0);
		const bool can_share_local_payload =
			(join_mode == HashJoinMatchMode::ANTI ||
			 join_mode == HashJoinMatchMode::SEMI) &&
			!OutputColumnsReferenceRight(output_columns, output_column_count);
		const bool payload_rows_unused =
			can_share_local_payload &&
			n_filter_inputs == 0 &&
			n_filter_exprs == 0 &&
			n_filter_steps == 0;
		state->payload->build_rows_use_keys =
			LayoutsEqual(state->build_key_layout, state->build_layout) ||
			payload_rows_unused;
		/* String payloads are valid in the segmented path for bounded producer
		 * counts; keeping them in per-worker flat stores defeats the migration. */
		state->payload->build_rows_shared_local =
			state->payload->build_partition_count == 1 &&
			!payload_rows_unused && can_share_local_payload;
		state->payload->finalized = false;
		pg_atomic_init_u32(&state->payload->release_state, 0);
		pg_atomic_init_u32(&state->payload->build_has_null_key, 0);
		pg_atomic_init_u32(&state->payload->combine_prepare_state, 0);
		state->payload->combined_row_count = 0;
		state->payload->combined_key_heap_used = 0;
		state->payload->combined_row_heap_used = 0;
		SpinLockInit(&state->payload->mutex);
		state->payload->direct_segmented_build = false;
		/* The DSA address of the shared payload is stable across attached
		 * processes and uniquely identifies this join operator for eviction. */
		state->payload->owner_tag = static_cast<uint64_t>(state->shared_payload_dp);
		state->payload->local_build_registry_dp = dsa_allocate0(ctx.dsa,
			static_cast<size_t>(state->payload->local_state_slot_count) * sizeof(HashJoinLocalBuildRegistryEntry));
		const uint32_t initial_rows = HashJoinInitialRows(max_rows_);
		const uint64_t per_worker_rows =
			(initial_rows + state->payload->local_state_slot_count - 1) / state->payload->local_state_slot_count;
		const uint64_t initial_global_rows = per_worker_rows * state->payload->local_state_slot_count;
		const uint32_t global_row_capacity = static_cast<uint32_t>(std::max<uint64_t>(1024u, initial_global_rows));
		const uint32_t key_heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->build_key_layout,
			global_row_capacity);
		const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->build_layout,
			global_row_capacity);
		/* Keep radix partitioning when the build uses segmented storage. Each
		 * worker owns one spillable segment chain per partition; the partition
		 * directories are built independently after the producer barrier. */
		state->payload->direct_segmented_build = CanUseDirectSegmentedHashJoin(ctx, *state);
		state->payload->partition_probe_mode = probe_stage_ != nullptr &&
			state->payload->direct_segmented_build && state->payload->build_partition_count > 1;
		if (pg_yaap_trace_execution_path)
			ereport(LOG,
				(errmsg("pg_yaap hashjoin partitions=%u radix_bits=%u workers=%u segmented=%d",
					state->payload->build_partition_count,
					static_cast<unsigned>(state->payload->radix_bits),
					state->payload->local_state_slot_count,
					state->payload->direct_segmented_build ? 1 : 0)));
		if (state->payload->direct_segmented_build)
		state->payload->segment_arena_dp = ctx.control->large_shared_segment_arena_root;
		if (state->payload->build_partition_count > 1)
		{
			const uint32_t partition_capacity = std::max<uint32_t>(1024u,
				global_row_capacity / state->payload->build_partition_count);
			state->payload->partitions_dp = dsa_allocate0(ctx.dsa,
				static_cast<size_t>(state->payload->build_partition_count) * sizeof(HashJoinPartition));
			auto *partitions = ResolveHashJoinPartitions(ctx.dsa, state->payload);
			if (partitions == nullptr)
				elog(ERROR, "pg_yaap: hash join partition payload allocation failed");
			for (uint32_t partition_id = 0; partition_id < state->payload->build_partition_count; ++partition_id)
			{
				HashJoinPartition &part = partitions[partition_id];
				part.build_keys_dp = InvalidDsaPointer;
				part.build_rows_dp = InvalidDsaPointer;
				part.build_key_refs_dp = InvalidDsaPointer;
				part.build_row_refs_dp = InvalidDsaPointer;
				part.build_row_slots_dp = InvalidDsaPointer;
				part.build_row_local_idxs_dp = InvalidDsaPointer;
				part.hash_table_dp = InvalidDsaPointer;
				part.hash_links_dp = InvalidDsaPointer;
				part.hash_salts_dp = InvalidDsaPointer;
				part.hash_table_capacity = 0;
				part.row_count = 0;
				if (state->payload->direct_segmented_build)
					continue;
				part.build_keys_dp = TupleDataCollectionAllocate(ctx.dsa, partition_capacity,
					state->build_key_layout->row_width,
					TupleDataCollectionDefaultHeapCapacity(state->build_key_layout, partition_capacity));
				part.build_rows_dp = state->payload->build_rows_use_keys ? part.build_keys_dp :
					TupleDataCollectionAllocate(ctx.dsa, partition_capacity, state->build_layout->row_width,
						TupleDataCollectionDefaultHeapCapacity(state->build_layout, partition_capacity));
				auto *keys = ResolveTdc(ctx.dsa, part.build_keys_dp);
				auto *rows = state->payload->build_rows_use_keys ? keys : ResolveTdc(ctx.dsa, part.build_rows_dp);
				TupleDataCollectionInit(keys, partition_capacity, state->build_key_layout->row_width,
					state->build_key_layout_dp, TupleDataCollectionDefaultHeapCapacity(state->build_key_layout, partition_capacity));
				if (!state->payload->build_rows_use_keys)
					TupleDataCollectionInit(rows, partition_capacity, state->build_layout->row_width,
						state->build_layout_dp, TupleDataCollectionDefaultHeapCapacity(state->build_layout, partition_capacity));
			}
		}
		else if (!state->payload->direct_segmented_build)
		{
			state->payload->build_keys_dp = TupleDataCollectionAllocate(ctx.dsa,
				global_row_capacity, state->build_key_layout->row_width, key_heap_capacity);
			if (state->payload->build_rows_use_keys)
				state->payload->build_rows_dp = state->payload->build_keys_dp;
			else
				state->payload->build_rows_dp = TupleDataCollectionAllocate(ctx.dsa,
					global_row_capacity, state->build_layout->row_width, heap_capacity);
			state->build_keys = ResolveTdc(ctx.dsa, state->payload->build_keys_dp);
			state->build_rows = state->payload->build_rows_use_keys
				? state->build_keys : ResolveTdc(ctx.dsa, state->payload->build_rows_dp);
			TupleDataCollectionInit(state->build_keys, global_row_capacity,
				state->build_key_layout->row_width, state->build_key_layout_dp, key_heap_capacity);
			if (!state->payload->build_rows_use_keys)
				TupleDataCollectionInit(state->build_rows, global_row_capacity,
					state->build_layout->row_width, state->build_layout_dp, heap_capacity);
		}
		else
		{
			state->payload->build_keys_dp = InvalidDsaPointer;
			state->payload->build_rows_dp = InvalidDsaPointer;
		}
		StoreSharedPayloadOnDescriptor(this, state->shared_payload_dp);
	}
	else
	{
		if (!DsaPointerIsValid(state->shared_payload_dp))
			state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this);
		state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);
	}

	if (state->payload == nullptr)
		elog(ERROR, "pg_yaap: hash join shared payload not initialized");

	state->build_keys = ResolveTdc(ctx.dsa, state->payload->build_keys_dp);
	state->build_rows = state->payload->build_rows_use_keys
		? state->build_keys
		: ResolveTdc(ctx.dsa, state->payload->build_rows_dp);
	if (state->build_keys == nullptr && state->payload->build_partition_count == 1 && !state->payload->segmented_build &&
		!state->payload->direct_segmented_build)
		elog(ERROR, "pg_yaap: hash join global build keys not initialized");
	if (state->build_rows == nullptr && state->payload->build_partition_count == 1 && !state->payload->segmented_build &&
		!state->payload->direct_segmented_build)
		elog(ERROR, "pg_yaap: hash join global build rows not initialized");

	state->local_state_slot_count = state->payload->local_state_slot_count;
	state->finalized = state->payload->finalized;
	return state;
}

std::unique_ptr<LocalSinkState>
PhysicalHashJoin::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<HashJoinGlobalSinkState &>(gstate);
	auto state = std::make_unique<HashJoinLocalSinkState>();
	state->build_key_layout = global.build_key_layout;
	state->build_layout = global.build_layout;
	const uint32_t initial_rows = HashJoinInitialRows(max_rows_);
	uint32_t local_capacity = initial_rows;
	if (global.local_state_slot_count > 0)
		local_capacity = (initial_rows + global.local_state_slot_count - 1) / global.local_state_slot_count;
	local_capacity = std::max<uint32_t>(1024u, local_capacity);
	const uint32_t key_heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->build_key_layout,
		local_capacity);
	const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->build_layout,
		local_capacity);
	if (global.payload->direct_segmented_build &&
		global.payload->build_partition_count > 1)
	{
		if (ctx.worker_index < 0 ||	static_cast<uint32_t>(ctx.worker_index) >=
			global.payload->local_state_slot_count)
			elog(ERROR, "pg_yaap: partitioned segmented hash join requires a worker-local state slot");
		const uint32_t partition_count = global.payload->build_partition_count;
		state->partition_key_segmented.resize(partition_count);
		if (!global.payload->build_rows_use_keys)
			state->partition_row_segmented.resize(partition_count);
		state->partition_state_dp = dsa_allocate0(ctx.dsa,
			static_cast<size_t>(partition_count) * sizeof(HashJoinLocalPartitionState));
		auto *partition_state = static_cast<HashJoinLocalPartitionState *>(
			dsa_get_address(ctx.dsa, state->partition_state_dp));
		if (partition_state == nullptr)
			elog(ERROR, "pg_yaap: segmented hash join local partition state missing");
		for (uint32_t partition_id = 0; partition_id < partition_count; ++partition_id)
		{
			partition_state[partition_id].build_key_segment_desc_dp = InvalidDsaPointer;
			partition_state[partition_id].build_row_segment_desc_dp = InvalidDsaPointer;
			partition_state[partition_id].build_key_refs_dp = InvalidDsaPointer;
			partition_state[partition_id].build_row_refs_dp = InvalidDsaPointer;
			partition_state[partition_id].row_count = 0;
		}
		auto *registry = ResolveLocalBuildRegistry(ctx.dsa, global.payload);
		if (registry == nullptr)
			elog(ERROR, "pg_yaap: segmented hash join local registry missing");
		registry[ctx.worker_index].partition_state_dp = state->partition_state_dp;
		state->segmented_writer_initialized = true;
		return state;
	}
	if (global.payload->direct_segmented_build)
	{
		auto *buffer_state = ResolveBufferState(ctx);
		if (buffer_state == nullptr || ctx.control == nullptr)
			elog(ERROR, "pg_yaap: direct segmented hash join buffer state missing");
		const dsa_pointer arena_dp = global.payload->segment_arena_dp;
		/* A later hash join may need its first segment while an earlier join
		 * already has published build segments.  Reclaim those victims before
		 * the allocation below can hit the hard resident limit. */
			/* Breaker data from sibling join pipelines shares this query-wide
			 * budget. Spill any published large segment with no readers instead
			 * of considering only this operator's own build chain. */
			while (buffer::YaapBufferOverSoftLimit(buffer_state) &&
				EvictOneHashJoinSegment(ctx, buffer::YaapSegmentHandle{}, 0))
			;
		const uint32_t segment_rows = DirectHashJoinSegmentRows(ctx,
			state->build_key_layout, state->build_layout);
		if (segment_rows == 0)
			elog(ERROR, "pg_yaap: direct segmented hash join row capacity unavailable");
		const uint32_t key_heap = TupleDataCollectionDefaultHeapCapacity(state->build_key_layout,
			segment_rows);
		const auto key_handle = buffer::YaapSharedSegmentArenaAllocate(ctx.dsa, buffer_state,
			arena_dp, static_cast<uint32_t>(std::max(ctx.worker_index, 0)),
			global.payload->owner_tag);
		auto *key_segment = buffer::YaapSharedSegmentResolve(ctx.dsa, arena_dp, key_handle);
		if (key_segment == nullptr)
			elog(ERROR, "pg_yaap: direct segmented hash join key segment missing");
		state->build_keys_dp = key_segment->payload_dp;
		state->build_keys = ResolveTdc(ctx.dsa, state->build_keys_dp);
		TupleDataCollectionInit(state->build_keys, segment_rows,
			state->build_key_layout->row_width, global.build_key_layout_dp, key_heap);
		buffer::YaapSegmentedTdcInit(&state->key_segmented, arena_dp, global.build_key_layout_dp,
			static_cast<uint32_t>(std::max(ctx.worker_index, 0)),
				state->build_key_layout->row_width, segment_rows, key_heap,
				0, "hash join local key writer");
		state->key_segmented.owner_tag = global.payload->owner_tag;
		state->key_segmented.first = key_handle;
		state->key_segmented.active = key_handle;
		state->key_segmented.segment_count = 1;
		state->key_refs_dp = buffer::YaapSegmentRowRefTableCreate(ctx.dsa, segment_rows);
		if (global.payload->build_rows_use_keys)
		{
			state->build_rows_dp = state->build_keys_dp;
			state->build_rows = state->build_keys;
			state->row_segmented = state->key_segmented;
			state->row_refs_dp = state->key_refs_dp;
		}
		else
		{
			const uint32_t row_heap = TupleDataCollectionDefaultHeapCapacity(state->build_layout,
				segment_rows);
			const auto row_handle = buffer::YaapSharedSegmentArenaAllocate(ctx.dsa, buffer_state,
				arena_dp, static_cast<uint32_t>(std::max(ctx.worker_index, 0)),
				global.payload->owner_tag);
			auto *row_segment = buffer::YaapSharedSegmentResolve(ctx.dsa, arena_dp, row_handle);
			if (row_segment == nullptr)
				elog(ERROR, "pg_yaap: direct segmented hash join payload segment missing");
			state->build_rows_dp = row_segment->payload_dp;
			state->build_rows = ResolveTdc(ctx.dsa, state->build_rows_dp);
			TupleDataCollectionInit(state->build_rows, segment_rows,
				state->build_layout->row_width, global.build_layout_dp, row_heap);
			buffer::YaapSegmentedTdcInit(&state->row_segmented, arena_dp, global.build_layout_dp,
				static_cast<uint32_t>(std::max(ctx.worker_index, 0)),
					state->build_layout->row_width, segment_rows, row_heap,
					0, "hash join local payload writer");
			state->row_segmented.owner_tag = global.payload->owner_tag;
			state->row_segmented.first = row_handle;
			state->row_segmented.active = row_handle;
			state->row_segmented.segment_count = 1;
			state->row_refs_dp = buffer::YaapSegmentRowRefTableCreate(ctx.dsa, segment_rows);
		}
		state->segmented_writer_initialized = true;
	}
	else
	{
	state->build_keys_dp = TupleDataCollectionAllocate(ctx.dsa,
		local_capacity, state->build_key_layout->row_width, key_heap_capacity);
	if (global.payload->build_rows_use_keys)
		state->build_rows_dp = state->build_keys_dp;
	else
		state->build_rows_dp = TupleDataCollectionAllocate(ctx.dsa,
			local_capacity, state->build_layout->row_width, heap_capacity);
	state->build_keys = ResolveTdc(ctx.dsa, state->build_keys_dp);
	state->build_rows = global.payload->build_rows_use_keys
		? state->build_keys
		: ResolveTdc(ctx.dsa, state->build_rows_dp);
	TupleDataCollectionInit(state->build_keys,
		local_capacity,
		state->build_key_layout->row_width,
		global.build_key_layout_dp,
		key_heap_capacity);
	if (!global.payload->build_rows_use_keys)
		TupleDataCollectionInit(state->build_rows,
			local_capacity,
			state->build_layout->row_width,
			global.build_layout_dp,
			heap_capacity);
	}

	if (ctx.worker_index >= 0)
	{
		auto *registry = ResolveLocalBuildRegistry(ctx.dsa, global.payload);
		if (registry == nullptr || static_cast<uint32_t>(ctx.worker_index) >= global.payload->local_state_slot_count)
			elog(ERROR, "pg_yaap: hash join local build registry missing");
		registry[ctx.worker_index].build_keys_dp = state->build_keys_dp;
		registry[ctx.worker_index].build_rows_dp = state->build_rows_dp;
		registry[ctx.worker_index].build_key_segment_desc_dp = InvalidDsaPointer;
		registry[ctx.worker_index].build_row_segment_desc_dp = InvalidDsaPointer;
		registry[ctx.worker_index].build_key_refs_dp = state->key_refs_dp;
		registry[ctx.worker_index].build_row_refs_dp = state->row_refs_dp;
		registry[ctx.worker_index].row_count = 0;
		registry[ctx.worker_index].key_heap_used = 0;
		registry[ctx.worker_index].row_heap_used = 0;
		registry[ctx.worker_index].global_row_offset = 0;
		registry[ctx.worker_index].global_key_heap_offset = 0;
		registry[ctx.worker_index].global_row_heap_offset = 0;
		if (pg_yaap_trace_execution_path)
			ereport(LOG,
				(errmsg("pg_yaap hashjoin registry slot=%d keys=" UINT64_FORMAT " rows=" UINT64_FORMAT,
					ctx.worker_index,
					(uint64) state->build_keys_dp,
					(uint64) state->build_rows_dp)));
	}

	return state;
}

SinkResultType
PhysicalHashJoin::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input)
{
	auto &global = static_cast<HashJoinGlobalSinkState &>(input.global_state);
	auto &local = static_cast<HashJoinLocalSinkState &>(input.local_state);
	const bool local_partitioned_segmented = global.payload->direct_segmented_build &&
		global.payload->build_partition_count > 1;
	if ((!local_partitioned_segmented &&
		(local.build_keys == nullptr || local.build_rows == nullptr)) ||
		local.build_key_layout == nullptr || local.build_layout == nullptr)
		elog(ERROR, "pg_yaap: hash join local build rows not initialized");
	bool chunk_has_null_key = false;
	for (uint16_t key_idx = 0; key_idx < local.build_key_layout->column_count; ++key_idx)
	{
		const uint16_t slot = local.build_key_layout->columns[key_idx].src_col_idx;
		if (slot >= 16)
			elog(ERROR, "pg_yaap: hash join build key chunk slot out of range");
		for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
			if (in.nulls[slot][row_idx] != 0)
			{
				chunk_has_null_key = true;
				break;
			}
		if (chunk_has_null_key)
			break;
	}
	if (chunk_has_null_key)
		(void) pg_atomic_fetch_or_u32(&global.payload->build_has_null_key, 1u);
	if (global.payload->direct_segmented_build)
	{
		if (global.payload->build_partition_count > 1)
		{
			auto *buffer_state = ResolveBufferState(ctx);
			auto *local_partitions = DsaPointerIsValid(local.partition_state_dp)
				? static_cast<HashJoinLocalPartitionState *>(dsa_get_address(ctx.dsa,
					local.partition_state_dp)) : nullptr;
			if (buffer_state == nullptr || local_partitions == nullptr || ctx.control == nullptr)
				elog(ERROR, "pg_yaap: partitioned segmented hash join local state missing");

			/* Segments are partition-local. Hybrid batches reconstruct row
			 * references from their chains, avoiding permanent per-row directories.
			 * Spill completed blocks periodically;
			 * only spill an active tail when the next input chunk could overflow it. */
			if ((local.spill_scan_ticks++ % 8u) == 0u &&
				PartitionedHashJoinShouldSpill(buffer_state))
			{
				for (uint32_t part_idx = 0; part_idx < global.payload->build_partition_count; ++part_idx)
				{
					auto spill_writer = [&](buffer::YaapSegmentedTdc &writer,
											const char *kind)
					{
						if (!writer.active.IsValid())
							return;
						auto *segment = buffer::YaapSharedSegmentResolve(ctx.dsa,
							writer.arena_dp, writer.active);
						if (segment != nullptr && pg_atomic_read_u32(&segment->state) ==
							static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL))
						{
							auto *store = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa,
								segment->payload_dp));
							if (store != nullptr &&
								pg_atomic_read_u32(&store->row_count) + PIPELINE_DEFAULT_CHUNK_SIZE >=
									store->row_capacity)
								SpillDirectHashJoinTail(ctx, writer, kind, true);
						}
							SpillDirectHashJoinPublishedSegments(ctx, writer, kind, true);
					};
					spill_writer(local.partition_key_segmented[part_idx], "key");
					if (!global.payload->build_rows_use_keys)
						spill_writer(local.partition_row_segmented[part_idx], "row");
				}
			}

			const uint32_t segment_rows = DirectHashJoinSegmentRows(ctx,
				local.build_key_layout, local.build_layout);
			if (segment_rows == 0)
				elog(ERROR, "pg_yaap: partitioned segmented hash join row capacity unavailable");
			auto initialize_writer = [&](buffer::YaapSegmentedTdc &writer,
										const TupleDataLayout *layout,
										dsa_pointer layout_dp,
										const char *name)
			{
				if (DsaPointerIsValid(writer.arena_dp))
					return;
				const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(layout,
					segment_rows);
				buffer::YaapSegmentedTdcInit(&writer, global.payload->segment_arena_dp,
					layout_dp, static_cast<uint32_t>(std::max(ctx.worker_index, 0)),
					layout->row_width, segment_rows, heap_capacity, 0, name);
				writer.owner_tag = global.payload->owner_tag;
			};
			for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
			{
				if (PipelineCancelRequestedEvery(ctx, row_idx))
					return SinkResultType::FINISHED;
				const uint64_t hash = HashGroup(local.build_key_layout, in, row_idx);
				const uint32_t partition_id = HashJoinPartitionIndex(hash, global.payload);
				if (partition_id >= global.payload->build_partition_count)
					elog(ERROR, "pg_yaap: hash join local radix partition out of range");
				auto &part = local_partitions[partition_id];
				if (!global.payload->partition_probe_mode && !DsaPointerIsValid(part.build_key_refs_dp))
					part.build_key_refs_dp = buffer::YaapSegmentRowRefTableCreate(ctx.dsa, 8192u);
				auto &key_writer = local.partition_key_segmented[partition_id];
				initialize_writer(key_writer, local.build_key_layout,
					global.build_key_layout_dp, "hash join local radix key writer");
				buffer::YaapSegmentedRowHandle key_handle;
				TupleDataCollection *key_store = nullptr;
				uint8_t *key_row = nullptr;
			if (!buffer::YaapSegmentedTdcAppendWithHeap(ctx.dsa, buffer_state,
					&key_writer,
					TupleDataCollectionRequiredHeapBytesForChunkRow(local.build_key_layout,
						in, row_idx), &key_handle, &key_store, &key_row))
					ereport(ERROR, (errmsg("pg_yaap: local radix build-key append failed")));
				ScatterGroupOnly(local.build_key_layout, key_store, key_row, in, row_idx);
				if (!global.payload->partition_probe_mode && buffer::YaapSegmentRowRefAppend(ctx.dsa,
					static_cast<buffer::YaapSegmentRowRefTable *>(dsa_get_address(ctx.dsa,
						part.build_key_refs_dp)),
					buffer::YaapSegmentRowRef{key_handle.segment, key_handle.row_index}) ==
					buffer::YAAP_INVALID_SEGMENT_ROW_REF)
					ereport(ERROR, (errmsg("pg_yaap: local radix build-key reference append failed")));
				if (global.payload->build_rows_use_keys)
				{
					part.build_row_refs_dp = part.build_key_refs_dp;
				}
				else
				{
					if (!global.payload->partition_probe_mode && !DsaPointerIsValid(part.build_row_refs_dp))
						part.build_row_refs_dp = buffer::YaapSegmentRowRefTableCreate(ctx.dsa, 8192u);
					auto &row_writer = local.partition_row_segmented[partition_id];
					initialize_writer(row_writer, local.build_layout,
						global.build_layout_dp, "hash join local radix payload writer");
					buffer::YaapSegmentedRowHandle row_handle;
					TupleDataCollection *row_store = nullptr;
					uint8_t *row = nullptr;
					if (!buffer::YaapSegmentedTdcAppendWithHeap(ctx.dsa, buffer_state,
						&row_writer,
						TupleDataCollectionRequiredHeapBytesForChunkRow(local.build_layout,
							in, row_idx), &row_handle, &row_store, &row))
						ereport(ERROR, (errmsg("pg_yaap: local radix build-payload append failed")));
					ScatterGroupOnly(local.build_layout, row_store, row, in, row_idx);
					if (!global.payload->partition_probe_mode && buffer::YaapSegmentRowRefAppend(ctx.dsa,
						static_cast<buffer::YaapSegmentRowRefTable *>(dsa_get_address(ctx.dsa,
							part.build_row_refs_dp)),
						buffer::YaapSegmentRowRef{row_handle.segment, row_handle.row_index}) ==
						buffer::YAAP_INVALID_SEGMENT_ROW_REF)
						ereport(ERROR, (errmsg("pg_yaap: local radix build-payload reference append failed")));
				}
				++part.row_count;
			}
			local.build_input_rows += in.count;
			return SinkResultType::NEED_MORE_INPUT;
		}
		/* Direct hash-join rows are referenced by both finalize and probe.
		 * Keep them resident until the consumer cleanup; there is no safe
		 * eviction victim inside this operator while those references remain
		 * live, so spilling here would make reload fail at the hard limit. */
		auto *buffer_state = ResolveBufferState(ctx);
		if (buffer_state == nullptr)
			elog(ERROR, "pg_yaap: direct segmented hash join buffer state missing");
		SpillDirectHashJoinTail(ctx, local.key_segmented, "key");
		if (!global.payload->build_rows_use_keys)
			SpillDirectHashJoinTail(ctx, local.row_segmented, "row");
		/* Bound the resident build window at one input chunk.  Keeping dozens
		 * of published segments resident defeats eviction for wide Q10 rows. */
		SpillDirectHashJoinPublishedSegments(ctx, local.key_segmented, "key");
		if (!global.payload->build_rows_use_keys)
			SpillDirectHashJoinPublishedSegments(ctx, local.row_segmented, "row");
		for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
		{
			if (PipelineCancelRequestedEvery(ctx, row_idx))
				return SinkResultType::FINISHED;
			buffer::YaapSegmentedRowHandle key_handle;
			TupleDataCollection *key_store = nullptr;
			uint8_t *key_row = nullptr;
				const uint32_t key_heap = TupleDataCollectionRequiredHeapBytesForChunkRow(
					local.build_key_layout, in, row_idx);
			if (!buffer::YaapSegmentedTdcAppendWithHeap(ctx.dsa, buffer_state,
				&local.key_segmented,
					key_heap, &key_handle, &key_store, &key_row))
				{
					auto *active = buffer::YaapSharedSegmentResolve(ctx.dsa,
						local.key_segmented.arena_dp, local.key_segmented.active);
					auto *store = active != nullptr && DsaPointerIsValid(active->payload_dp)
						? static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, active->payload_dp))
						: nullptr;
					ereport(ERROR, (errmsg("pg_yaap: direct segmented hash join key append failed (active=%u generation=%u state=%u row_capacity=%u rows=%u heap_need=%u heap_capacity=%u segment_used=%u segment_capacity=%u resident=%llu hard=%llu)",
						local.key_segmented.active.segment_id,
						local.key_segmented.active.generation,
						active != nullptr ? pg_atomic_read_u32(&active->state) : UINT32_MAX,
						store != nullptr ? store->row_capacity : 0,
						store != nullptr ? pg_atomic_read_u32(&store->row_count) : 0,
						key_heap,
						store != nullptr ? store->heap_capacity : 0,
						active != nullptr ? active->used : 0,
						active != nullptr ? active->capacity : 0,
						(unsigned long long) buffer::YaapBufferResidentBytes(buffer_state),
						(unsigned long long) buffer_state->hard_limit)));
				}
			ScatterGroupOnly(local.build_key_layout, key_store, key_row, in, row_idx);
				auto *key_segment = buffer::YaapSharedSegmentResolve(ctx.dsa,
					local.key_segmented.arena_dp, key_handle.segment);
			if (key_segment == nullptr || buffer::YaapSegmentRowRefAppend(ctx.dsa,
				static_cast<buffer::YaapSegmentRowRefTable *>(dsa_get_address(ctx.dsa,
					local.key_refs_dp)), buffer::YaapSegmentRowRef{key_handle.segment,
						key_handle.row_index}) == buffer::YAAP_INVALID_SEGMENT_ROW_REF)
				ereport(ERROR, (errmsg("pg_yaap: direct segmented hash join key reference failed")));
			if (!global.payload->build_rows_use_keys)
			{
				buffer::YaapSegmentedRowHandle row_handle;
				TupleDataCollection *row_store = nullptr;
				uint8_t *row = nullptr;
				if (!buffer::YaapSegmentedTdcAppendWithHeap(ctx.dsa, buffer_state,
					&local.row_segmented,
					TupleDataCollectionRequiredHeapBytesForChunkRow(local.build_layout,
						in, row_idx), &row_handle, &row_store, &row))
					ereport(ERROR, (errmsg("pg_yaap: direct segmented hash join payload append failed")));
				ScatterGroupOnly(local.build_layout, row_store, row, in, row_idx);
					auto *row_segment = buffer::YaapSharedSegmentResolve(ctx.dsa,
					local.row_segmented.arena_dp, row_handle.segment);
				if (row_segment == nullptr || buffer::YaapSegmentRowRefAppend(ctx.dsa,
					static_cast<buffer::YaapSegmentRowRefTable *>(dsa_get_address(ctx.dsa,
						local.row_refs_dp)), buffer::YaapSegmentRowRef{row_handle.segment,
							row_handle.row_index}) == buffer::YAAP_INVALID_SEGMENT_ROW_REF)
					ereport(ERROR, (errmsg("pg_yaap: direct segmented hash join payload reference failed")));
			}
		}
		local.build_input_rows += in.count;
		local.build_keys_dp = local.key_segmented.active.IsValid()
			? buffer::YaapSharedSegmentResolve(ctx.dsa, local.key_segmented.arena_dp,
				local.key_segmented.active)->payload_dp : InvalidDsaPointer;
		local.build_keys = ResolveTdc(ctx.dsa, local.build_keys_dp);
		if (!global.payload->build_rows_use_keys)
		{
			local.build_rows_dp = buffer::YaapSharedSegmentResolve(ctx.dsa,
				local.row_segmented.arena_dp, local.row_segmented.active)->payload_dp;
			local.build_rows = ResolveTdc(ctx.dsa, local.build_rows_dp);
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

	for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
	{
		if (PipelineCancelRequestedEvery(ctx, row_idx))
			return SinkResultType::FINISHED;
		EnsureJoinLocalCapacity(ctx,
			global.payload,
			global.build_key_layout_dp,
			local.build_key_layout,
			global.build_layout_dp,
			local.build_layout,
			local,
			in,
			row_idx);
		uint8_t *key_row_ptr = nullptr;
		uint8_t *row_ptr = nullptr;
		const uint32_t key_appended = TupleDataCollectionAppendRow(local.build_keys, &key_row_ptr);
		const uint32_t appended = global.payload->build_rows_use_keys
			? key_appended
			: TupleDataCollectionAppendRow(local.build_rows, &row_ptr);
		if (key_appended == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: hash join local build key row capacity exceeded");
		if (appended == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: hash join local build row capacity exceeded");
		if (global.payload->build_rows_use_keys)
			row_ptr = key_row_ptr;
		ScatterGroupOnly(local.build_key_layout, local.build_keys, key_row_ptr, in, row_idx);
		if (!global.payload->build_rows_use_keys)
			ScatterGroupOnly(local.build_layout, local.build_rows, row_ptr, in, row_idx);
	}

	(void) ctx;
	local.build_input_rows += in.count;
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
PhysicalHashJoin::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input)
{
	auto &global = static_cast<HashJoinGlobalSinkState &>(input.global_state);
	if (global.payload != nullptr && global.payload->direct_segmented_build &&
		global.payload->build_partition_count > 1)
	{
		if (input.partition_id != UINT32_MAX || input.local_state == nullptr ||
			ctx.worker_index < 0)
			elog(ERROR, "pg_yaap: partitioned segmented hash join expects one producer Combine task per worker");
		auto &local = static_cast<HashJoinLocalSinkState &>(*input.local_state);
		auto *local_parts = DsaPointerIsValid(local.partition_state_dp)
			? static_cast<HashJoinLocalPartitionState *>(dsa_get_address(ctx.dsa,
				local.partition_state_dp)) : nullptr;
		auto *registry = ResolveLocalBuildRegistry(ctx.dsa, global.payload);
		if (local_parts == nullptr || registry == nullptr ||
			static_cast<uint32_t>(ctx.worker_index) >= global.payload->local_state_slot_count)
			elog(ERROR, "pg_yaap: partitioned segmented hash join Combine state missing");

		for (uint32_t part_idx = 0; part_idx < global.payload->build_partition_count; ++part_idx)
		{
			auto &key_writer = local.partition_key_segmented[part_idx];
			auto &part = local_parts[part_idx];
			if (key_writer.first.IsValid())
			{
				buffer::YaapSegmentedTdcSealPublish(ctx.dsa, &key_writer);
				part.build_key_segment_desc_dp = buffer::YaapSegmentedTdcPublishDesc(
					ctx.dsa, key_writer);
				if (PartitionedHashJoinShouldSpill(ResolveBufferState(ctx)))
					SpillDirectHashJoinPublishedSegments(ctx, key_writer, "radix-key", true);
			}
			if (global.payload->build_rows_use_keys)
			{
				part.build_row_segment_desc_dp = part.build_key_segment_desc_dp;
				part.build_row_refs_dp = part.build_key_refs_dp;
			}
			else
			{
				auto &row_writer = local.partition_row_segmented[part_idx];
				if (row_writer.first.IsValid())
				{
					buffer::YaapSegmentedTdcSealPublish(ctx.dsa, &row_writer);
					part.build_row_segment_desc_dp = buffer::YaapSegmentedTdcPublishDesc(
						ctx.dsa, row_writer);
					if (PartitionedHashJoinShouldSpill(ResolveBufferState(ctx)))
						SpillDirectHashJoinPublishedSegments(ctx, row_writer, "radix-payload", true);
				}
			}
			if (global.payload->partition_probe_mode &&
				(key_writer.row_count != part.row_count ||
				 (!global.payload->build_rows_use_keys &&
				  local.partition_row_segmented[part_idx].row_count != part.row_count)))
				elog(ERROR, "pg_yaap: local radix writer row count mismatch");
			if (DsaPointerIsValid(part.build_key_refs_dp))
				part.row_count = pg_atomic_read_u32(
					&static_cast<buffer::YaapSegmentRowRefTable *>(
						dsa_get_address(ctx.dsa, part.build_key_refs_dp))->count);
		}
		registry[ctx.worker_index].partition_state_dp = local.partition_state_dp;
		return SinkCombineResultType::FINISHED;
	}
	if (global.payload != nullptr && global.payload->build_partition_count > 1)
	{
		/* The leader's optional Run contribution is combined inline during
		 * Finalize, after all partition-owner tasks have completed. */
		if (input.partition_id == UINT32_MAX)
		{
			if (input.local_state == nullptr)
				elog(ERROR, "pg_yaap: hash join leader partition combine state missing");
			auto &leader_local = static_cast<HashJoinLocalSinkState &>(*input.local_state);
			auto *parts = ResolveHashJoinPartitions(ctx.dsa, global.payload);
			if (parts == nullptr || leader_local.build_keys == nullptr || leader_local.build_rows == nullptr)
				elog(ERROR, "pg_yaap: hash join leader partition state missing");
			const uint32_t count = pg_atomic_read_u32(&leader_local.build_keys->row_count);
			for (uint32_t row_idx = 0; row_idx < count; ++row_idx)
			{
				const uint8_t *src_key = TupleDataCollectionGetRowConst(leader_local.build_keys, row_idx);
				const uint8_t *src_row = TupleDataCollectionGetRowConst(leader_local.build_rows, row_idx);
				uint64_t hash = 0;
				if (!TryHashSingleJoinKeyRow(global.build_key_layout, leader_local.build_keys, src_key, hash))
					hash = HashGroupRow(global.build_key_layout, leader_local.build_keys, src_key);
				HashJoinPartition &part = parts[HashJoinPartitionIndex(hash, global.payload)];
				EnsureJoinPartitionCapacity(ctx, global.payload, part,
					global.build_key_layout, global.build_key_layout_dp, global.build_layout,
					global.build_layout_dp, leader_local.build_keys, src_key,
					leader_local.build_rows, src_row);
				auto *keys = ResolveTdc(ctx.dsa, part.build_keys_dp);
				auto *rows = global.payload->build_rows_use_keys ? keys : ResolveTdc(ctx.dsa, part.build_rows_dp);
				uint8_t *dst_key = nullptr;
				uint8_t *dst_row = nullptr;
				if (TupleDataCollectionAppendRow(keys, &dst_key) == TDC_INVALID_ROW_INDEX ||
					(!global.payload->build_rows_use_keys && TupleDataCollectionAppendRow(rows, &dst_row) == TDC_INVALID_ROW_INDEX))
					elog(ERROR, "pg_yaap: hash join leader partition append failed");
				if (global.payload->build_rows_use_keys)
					dst_row = dst_key;
				CopyBuildRow(global.build_key_layout, leader_local.build_keys, src_key, keys, dst_key);
				if (!global.payload->build_rows_use_keys)
					CopyBuildRow(global.build_layout, leader_local.build_rows, src_row, rows, dst_row);
			}
			return SinkCombineResultType::FINISHED;
		}
		if (input.partition_id >= global.payload->build_partition_count)
			elog(ERROR, "pg_yaap: hash join combine partition %u out of range", input.partition_id);
		auto *partitions = ResolveHashJoinPartitions(ctx.dsa, global.payload);
		auto *registry = ResolveLocalBuildRegistry(ctx.dsa, global.payload);
		if (partitions == nullptr || registry == nullptr)
			elog(ERROR, "pg_yaap: hash join partition state missing during combine");
		HashJoinPartition &part = partitions[input.partition_id];
		for (uint32_t slot = 0; slot < global.payload->local_state_slot_count; ++slot)
		{
			auto *local_keys = ResolveTdc(ctx.dsa, registry[slot].build_keys_dp);
			auto *local_rows = global.payload->build_rows_use_keys ? local_keys :
				ResolveTdc(ctx.dsa, registry[slot].build_rows_dp);
			if (local_keys == nullptr || local_rows == nullptr)
				continue;
			const uint32_t local_count = pg_atomic_read_u32(&local_keys->row_count);
			for (uint32_t row_idx = 0; row_idx < local_count; ++row_idx)
			{
				const uint8_t *src_key = TupleDataCollectionGetRowConst(local_keys, row_idx);
				uint64_t hash = 0;
				if (!TryHashSingleJoinKeyRow(global.build_key_layout, local_keys, src_key, hash))
					hash = HashGroupRow(global.build_key_layout, local_keys, src_key);
				if (HashJoinPartitionIndex(hash, global.payload) != input.partition_id)
					continue;
				const uint8_t *src_row = TupleDataCollectionGetRowConst(local_rows, row_idx);
				EnsureJoinPartitionCapacity(ctx, global.payload, part,
					global.build_key_layout, global.build_key_layout_dp,
					global.build_layout, global.build_layout_dp,
					local_keys, src_key, local_rows, src_row);
				auto *keys = ResolveTdc(ctx.dsa, part.build_keys_dp);
				auto *rows = global.payload->build_rows_use_keys ? keys : ResolveTdc(ctx.dsa, part.build_rows_dp);
				uint8_t *dst_key = nullptr;
				uint8_t *dst_row = nullptr;
				if (TupleDataCollectionAppendRow(keys, &dst_key) == TDC_INVALID_ROW_INDEX ||
					(!global.payload->build_rows_use_keys &&
					 TupleDataCollectionAppendRow(rows, &dst_row) == TDC_INVALID_ROW_INDEX))
					elog(ERROR, "pg_yaap: hash join partition append failed");
				if (global.payload->build_rows_use_keys)
					dst_row = dst_key;
				CopyBuildRow(global.build_key_layout, local_keys, src_key, keys, dst_key);
				if (!global.payload->build_rows_use_keys)
					CopyBuildRow(global.build_layout, local_rows, src_row, rows, dst_row);
			}
		}
		part.row_count = pg_atomic_read_u32(&ResolveTdc(ctx.dsa, part.build_keys_dp)->row_count);
		pg_atomic_add_fetch_u32(&global.payload->combine_partitions_done, 1);
		return SinkCombineResultType::FINISHED;
	}
	if (input.local_state == nullptr)
		elog(ERROR, "pg_yaap: hash join combine local state missing");
	auto &local = static_cast<HashJoinLocalSinkState &>(*input.local_state);
	if (local.build_keys == nullptr || local.build_rows == nullptr)
		elog(ERROR, "pg_yaap: hash join combine local build rows missing");
	if (global.payload->direct_segmented_build)
	{
		buffer::YaapSegmentedTdcSealPublish(ctx.dsa, &local.key_segmented);
		if (!global.payload->build_rows_use_keys)
			buffer::YaapSegmentedTdcSealPublish(ctx.dsa, &local.row_segmented);
		auto *registry = ResolveLocalBuildRegistry(ctx.dsa, global.payload);
		if (registry == nullptr || ctx.worker_index < 0)
			elog(ERROR, "pg_yaap: direct segmented hash join registry missing");
		HashJoinLocalBuildRegistryEntry &entry = registry[ctx.worker_index];
		entry.build_key_segment_desc_dp = buffer::YaapSegmentedTdcPublishDesc(ctx.dsa,
			local.key_segmented);
		entry.build_row_segment_desc_dp = global.payload->build_rows_use_keys
			? entry.build_key_segment_desc_dp
			: buffer::YaapSegmentedTdcPublishDesc(ctx.dsa, local.row_segmented);
		entry.build_key_refs_dp = local.key_refs_dp;
		entry.build_row_refs_dp = global.payload->build_rows_use_keys
			? local.key_refs_dp : local.row_refs_dp;
		/* Each producer owns its registry slot. Finalize merges only after all
		 * producers have published, avoiding allocation while holding a spinlock. */
		return SinkCombineResultType::FINISHED;
	}

	if (global.payload != nullptr && global.payload->build_rows_shared_local)
		return ExecuteHashJoinSharedPayloadCombine(ctx, global, local);

	if (LayoutHasStringRef(global.build_key_layout))
	{
		const uint32_t local_row_count = pg_atomic_read_u32(&local.build_keys->row_count);
		if (local_row_count != pg_atomic_read_u32(&local.build_rows->row_count))
			elog(ERROR, "pg_yaap: hash join local build key/payload row counts diverged");
		uint32_t row_idx = 0;
		while (row_idx < local_row_count)
		{
			if (PipelineCancelRequested(ctx))
				return SinkCombineResultType::FINISHED;
			const uint32_t batch_end = std::min(row_idx + 64u, local_row_count);
			SpinLockAcquire(&global.payload->mutex);
			for (; row_idx < batch_end; ++row_idx)
			{
				const uint8_t *src_key_row = TupleDataCollectionGetRowConst(local.build_keys, row_idx);
				const uint8_t *src_row = TupleDataCollectionGetRowConst(local.build_rows, row_idx);
				EnsureJoinGlobalCapacity(ctx,
					global,
					src_key_row,
					local.build_keys,
					src_row,
					local.build_rows);
				uint8_t *dst_key_row = nullptr;
				uint8_t *dst_row = nullptr;
				const uint32_t key_appended = TupleDataCollectionAppendRow(global.build_keys, &dst_key_row);
				const uint32_t appended = TupleDataCollectionAppendRow(global.build_rows, &dst_row);
				if (key_appended == TDC_INVALID_ROW_INDEX)
					elog(ERROR, "pg_yaap: hash join global build key row capacity exceeded during combine");
				if (appended == TDC_INVALID_ROW_INDEX)
					elog(ERROR, "pg_yaap: hash join global build row capacity exceeded during combine");
				CopyBuildRow(global.build_key_layout, local.build_keys, src_key_row, global.build_keys, dst_key_row);
				CopyBuildRow(global.build_layout, local.build_rows, src_row, global.build_rows, dst_row);
			}
			SpinLockRelease(&global.payload->mutex);
		}

		global.payload->combined = true;
		return SinkCombineResultType::FINISHED;
	}

	return ExecuteHashJoinCombine(ctx, global, local);
}

SinkFinalizeType
PhysicalHashJoin::FinalizePartition(ExecCtx &ctx,
									GlobalSinkState &gstate,
									uint32_t partition_id)
{
	auto &global = static_cast<HashJoinGlobalSinkState &>(gstate);
	auto *payload = global.payload;
	if (payload == nullptr || !payload->direct_segmented_build ||
		payload->build_partition_count <= 1 || partition_id >= payload->build_partition_count)
		elog(ERROR, "pg_yaap: invalid partitioned segmented hash join finalize request");
	auto *partitions = ResolveHashJoinPartitions(ctx.dsa, payload);
	auto *registry = ResolveLocalBuildRegistry(ctx.dsa, payload);
	if (partitions == nullptr || registry == nullptr)
		elog(ERROR, "pg_yaap: partitioned segmented hash join finalize state missing");
	HashJoinPartition &part = partitions[partition_id];
	uint64_t total_rows = 0;
	uint64_t all_partition_rows = payload->partition_probe_mode ? payload->segmented_row_count : 0;
	for (uint32_t slot = 0; slot < payload->local_state_slot_count; ++slot)
	{
		if (!DsaPointerIsValid(registry[slot].partition_state_dp))
			continue;
		auto *local_parts = static_cast<HashJoinLocalPartitionState *>(
			dsa_get_address(ctx.dsa, registry[slot].partition_state_dp));
		if (local_parts == nullptr)
			elog(ERROR, "pg_yaap: local radix partition registry is unmapped");
		if (!payload->partition_probe_mode)
			for (uint32_t i = 0; i < payload->build_partition_count; ++i)
				all_partition_rows += local_parts[i].row_count;
		const auto &local_part = local_parts[partition_id];
		if (DsaPointerIsValid(local_part.build_key_refs_dp))
		{
			auto *local_refs = static_cast<const buffer::YaapSegmentRowRefTable *>(
				dsa_get_address(ctx.dsa, local_part.build_key_refs_dp));
			total_rows += pg_atomic_read_u32(
				const_cast<pg_atomic_uint32 *>(&local_refs->count));
		}
	}
	if (total_rows > UINT32_MAX / 2u)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			errmsg("pg_yaap: hash join partition %u exceeds the supported row-index range",
				partition_id)));
	if (total_rows > 0)
	{
		part.row_count = static_cast<uint32_t>(total_rows);
		uint32_t capacity = 1;
		while (capacity < part.row_count * 2u)
			capacity <<= 1;
		part.hash_table_capacity = capacity;
		auto *buffer_state = ResolveBufferState(ctx);
		const auto &key_col = global.build_key_layout->columns[0];
		const bool integer_key = global.build_key_layout->column_count == 1 &&
			(key_col.kind == TdcColumnKind::INT32 || key_col.kind == TdcColumnKind::INT64);
		/* Exact scalar keys replace the salt array, avoiding key-segment I/O
		 * on probe. Bound the extra resident directory bytes for this join;
		 * larger builds retain the ordinary spillable-key path. */
		const bool cache_keys = integer_key && buffer_state != nullptr &&
			all_partition_rows * (key_col.width - sizeof(uint16_t)) <= buffer_state->hard_limit / 16u;
		const uint32_t entry_bytes = cache_keys ? key_col.width : sizeof(uint16_t);
		const uint64_t directory_bytes = static_cast<uint64_t>(capacity) * sizeof(uint32_t) +
			static_cast<uint64_t>(part.row_count) * (sizeof(uint32_t) * 3u + entry_bytes);
		if (buffer_state == nullptr)
			elog(ERROR, "pg_yaap: partitioned segmented hash join buffer state missing during finalize");
		if (directory_bytes > buffer_state->hard_limit)
			ereport(ERROR, (errmsg("pg_yaap: partitioned segmented hash join directory exceeds query memory limit (partition=%u rows=%u bytes=%llu hard=%llu)",
				partition_id, part.row_count, (unsigned long long) directory_bytes,
				(unsigned long long) buffer_state->hard_limit)));
		/* Reserve atomically before allocating maps or arrays. Several
		 * partition builders can reach this point concurrently. */
		uint32_t attempts = 0;
		bool evicted_for_directory = false;
		while (!buffer::YaapBufferTryChargeExternalBytes(buffer_state, directory_bytes))
		{
			CHECK_FOR_INTERRUPTS();
			if (EvictOneHashJoinSegment(ctx, buffer::YaapSegmentHandle{}, 0))
			{
				evicted_for_directory = true;
				attempts = 0;
				continue;
			}
			if (++attempts >= 10000)
				ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
					errmsg("pg_yaap: insufficient memory for radix hash join partition directory after spilling")));
			pg_usleep(kHashJoinSegmentWaitPollUs);
		}
		if (evicted_for_directory)
			dsa_trim(ctx.dsa);
		part.directory_charged_bytes = directory_bytes;
		part.build_row_slots_dp = YaapDsaAllocate(ctx.dsa,
			static_cast<size_t>(part.row_count) * sizeof(uint32_t));
		part.build_row_local_idxs_dp = YaapDsaAllocate(ctx.dsa,
			static_cast<size_t>(part.row_count) * sizeof(uint32_t));
		auto *row_slots = static_cast<uint32_t *>(dsa_get_address(ctx.dsa,
			part.build_row_slots_dp));
		auto *row_local_idxs = static_cast<uint32_t *>(dsa_get_address(ctx.dsa,
			part.build_row_local_idxs_dp));
		uint32_t offset = 0;
		for (uint32_t slot = 0; slot < payload->local_state_slot_count; ++slot)
		{
			if (!DsaPointerIsValid(registry[slot].partition_state_dp))
				continue;
			auto *local_parts = static_cast<HashJoinLocalPartitionState *>(
				dsa_get_address(ctx.dsa, registry[slot].partition_state_dp));
			const auto &local_part = local_parts[partition_id];
			if (!DsaPointerIsValid(local_part.build_key_refs_dp))
				continue;
			const auto *local_keys = static_cast<const buffer::YaapSegmentRowRefTable *>(
				dsa_get_address(ctx.dsa, local_part.build_key_refs_dp));
			const uint32_t count = pg_atomic_read_u32(
				const_cast<pg_atomic_uint32 *>(&local_keys->count));
			for (uint32_t local_idx = 0; local_idx < count; ++local_idx)
			{
				row_slots[offset] = slot;
				row_local_idxs[offset] = local_idx;
				++offset;
			}
		}
		if (offset != part.row_count)
			elog(ERROR, "pg_yaap: radix hash join row mapping count mismatch");
		part.hash_table_dp = YaapDsaAllocate(ctx.dsa,
			static_cast<size_t>(capacity) * sizeof(uint32_t));
		part.hash_links_dp = YaapDsaAllocate(ctx.dsa,
			static_cast<size_t>(part.row_count) * sizeof(uint32_t));
		part.hash_key_values_dp = cache_keys ? YaapDsaAllocate(ctx.dsa,
			static_cast<size_t>(part.row_count) * key_col.width) : InvalidDsaPointer;
		part.hash_salts_dp = cache_keys ? InvalidDsaPointer : YaapDsaAllocate(ctx.dsa,
			static_cast<size_t>(part.row_count) * sizeof(uint16_t));
		auto *heads = static_cast<uint32_t *>(dsa_get_address(ctx.dsa, part.hash_table_dp));
		auto *links = static_cast<uint32_t *>(dsa_get_address(ctx.dsa, part.hash_links_dp));
		auto *salts = cache_keys ? nullptr :
			static_cast<uint16_t *>(dsa_get_address(ctx.dsa, part.hash_salts_dp));
		auto *key_values = cache_keys ?
			static_cast<uint8_t *>(dsa_get_address(ctx.dsa, part.hash_key_values_dp)) : nullptr;
		std::memset(heads, 0xFF, static_cast<size_t>(capacity) * sizeof(uint32_t));

		HashJoinReadWindow window(ctx, payload->segment_arena_dp);
		PG_ENSURE_ERROR_CLEANUP(ReleaseHashJoinReadWindow, PointerGetDatum(&window));
		{
			uint32_t row_idx = 0;
			for (uint32_t slot = 0; slot < payload->local_state_slot_count; ++slot)
			{
				if (!DsaPointerIsValid(registry[slot].partition_state_dp))
					continue;
				const auto *local_parts = static_cast<const HashJoinLocalPartitionState *>(
					dsa_get_address(ctx.dsa, registry[slot].partition_state_dp));
				const auto &local_part = local_parts[partition_id];
				if (!DsaPointerIsValid(local_part.build_key_refs_dp))
					continue;
				/* Same slot-major order as the row mapping above. Resolve the
				 * local block once instead of redoing the mapping for every key. */
				for (uint32_t local_idx = 0; local_idx < local_part.row_count; ++local_idx, ++row_idx)
				{
					if ((local_idx & 1023u) == 0)
						CHECK_FOR_INTERRUPTS();
					TupleDataCollection *key_store = nullptr;
					const uint8_t *key_row = AcquireSegmentedHashJoinRowFromRefs(ctx,
						payload, local_part.build_key_refs_dp, local_idx, &window, true, &key_store);
					if (key_store == nullptr || key_row == nullptr || row_idx >= part.row_count)
						elog(ERROR, "pg_yaap: partitioned segmented hash join key row unavailable");
					uint64_t hash = 0;
					if (!TryHashSingleJoinKeyRow(global.build_key_layout, key_store, key_row, hash))
						hash = HashGroupRow(global.build_key_layout, key_store, key_row);
					if (HashJoinPartitionIndex(hash, payload) != partition_id)
						elog(ERROR, "pg_yaap: build row assigned to the wrong radix partition");
					if (cache_keys)
						std::memcpy(key_values + static_cast<size_t>(row_idx) * key_col.width,
							key_row + key_col.offset, key_col.width);
					else
						salts[row_idx] = HashJoinSalt(hash);
					const uint32_t bucket = static_cast<uint32_t>(hash) & (capacity - 1u);
					links[row_idx] = heads[bucket];
					heads[bucket] = row_idx;
				}
			}
			if (row_idx != part.row_count)
				elog(ERROR, "pg_yaap: hash join finalize row count mismatch");
		}
		PG_END_ENSURE_ERROR_CLEANUP(ReleaseHashJoinReadWindow, PointerGetDatum(&window));
		window.Release();
		/* Finalize has to read spilled build segments to compute hashes. Keep
		 * those reloads transient so the completed radix partition still leaves
		 * room for its non-spillable directory and later hash-join builds. */
		while (!payload->partition_probe_mode && buffer::YaapBufferOverSoftLimit(buffer_state) &&
			EvictOneHashJoinSegment(ctx, buffer::YaapSegmentHandle{}, 0))
		;
	}
	else
	{
		part.row_count = 0;
		part.hash_table_capacity = 0;
		part.hash_table_dp = InvalidDsaPointer;
		part.hash_links_dp = InvalidDsaPointer;
		part.hash_salts_dp = InvalidDsaPointer;
		part.hash_key_values_dp = InvalidDsaPointer;
		part.build_key_refs_dp = InvalidDsaPointer;
		part.build_row_refs_dp = InvalidDsaPointer;
	}

	const uint32_t completed = pg_atomic_add_fetch_u32(&payload->finalize_partitions_done, 1);
	if (completed == payload->build_partition_count && !payload->partition_probe_mode)
	{
		uint64_t total_rows = 0;
		for (uint32_t i = 0; i < payload->build_partition_count; ++i)
			total_rows += partitions[i].row_count;
		if (total_rows > UINT32_MAX)
			elog(ERROR, "pg_yaap: partitioned segmented hash join row count overflow");
		payload->segmented_row_count = static_cast<uint32_t>(total_rows);
		payload->combined = true;
		payload->segmented_build = true;
		payload->finalized = true;
		global.finalized = true;
	}
	return SinkFinalizeType::READY;
}

SinkFinalizeType
PhysicalHashJoin::Finalize(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<HashJoinGlobalSinkState &>(gstate);
	if (global.payload != nullptr && global.payload->direct_segmented_build &&
		global.payload->build_partition_count > 1)
	{
		if (global.payload->partition_probe_mode)
		{
			/* The paired probe source finalizes only its admitted partition. */
			auto *registry = ResolveLocalBuildRegistry(ctx.dsa, global.payload);
			uint64_t rows = 0;
			for (uint32_t slot = 0; slot < global.payload->local_state_slot_count; ++slot)
			{
				if (!DsaPointerIsValid(registry[slot].partition_state_dp)) continue;
				const auto *parts = static_cast<const HashJoinLocalPartitionState *>(
					dsa_get_address(ctx.dsa, registry[slot].partition_state_dp));
				for (uint32_t part = 0; part < global.payload->build_partition_count; ++part)
					rows += parts[part].row_count;
			}
			if (rows > UINT32_MAX) elog(ERROR, "pg_yaap: paired build row count overflow");
			global.payload->segmented_row_count = static_cast<uint32_t>(rows);
			global.payload->segmented_build = true;
			global.payload->combined = true;
			global.payload->finalized = true;
			global.finalized = true;
			return SinkFinalizeType::READY;
		}
		/* Partition directories were built by worker FINALIZE tasks. */
		if (!global.payload->finalized)
			elog(ERROR, "pg_yaap: partitioned segmented hash join finalized on the leader before its workers");
		global.finalized = true;
		return SinkFinalizeType::READY;
	}
	if (global.payload != nullptr && global.payload->build_partition_count > 1)
	{
		auto *partitions = ResolveHashJoinPartitions(ctx.dsa, global.payload);
		if (partitions == nullptr)
			elog(ERROR, "pg_yaap: hash join partition state missing during finalize");
		for (uint32_t partition_id = 0; partition_id < global.payload->build_partition_count; ++partition_id)
		{
			HashJoinPartition &part = partitions[partition_id];
			auto *keys = ResolveTdc(ctx.dsa, part.build_keys_dp);
			if (keys == nullptr)
				elog(ERROR, "pg_yaap: hash join partition keys missing during finalize");
			const uint32_t row_count = pg_atomic_read_u32(&keys->row_count);
			part.row_count = row_count;
			if (row_count == 0)
				continue;
			uint32_t capacity = 1;
			while (capacity < row_count * 2u)
				capacity <<= 1;
			part.hash_table_capacity = capacity;
			const uint64_t directory_bytes = static_cast<uint64_t>(capacity) * sizeof(uint32_t) +
				static_cast<uint64_t>(row_count) * (sizeof(uint32_t) + sizeof(uint16_t));
			buffer::YaapBufferChargeExternalBytes(ResolveBufferState(ctx), directory_bytes,
				"hash join partition directory");
			global.payload->directory_charged_bytes += directory_bytes;
			part.hash_table_dp = YaapDsaAllocate(ctx.dsa, static_cast<size_t>(capacity) * sizeof(uint32_t));
			part.hash_links_dp = YaapDsaAllocate(ctx.dsa, static_cast<size_t>(row_count) * sizeof(uint32_t));
			part.hash_salts_dp = YaapDsaAllocate(ctx.dsa, static_cast<size_t>(row_count) * sizeof(uint16_t));
			auto *heads = static_cast<uint32_t *>(dsa_get_address(ctx.dsa, part.hash_table_dp));
			auto *links = static_cast<uint32_t *>(dsa_get_address(ctx.dsa, part.hash_links_dp));
			auto *salts = static_cast<uint16_t *>(dsa_get_address(ctx.dsa, part.hash_salts_dp));
			std::memset(heads, 0xFF, static_cast<size_t>(capacity) * sizeof(uint32_t));
			for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
			{
				const uint8_t *row = TupleDataCollectionGetRowConst(keys, row_idx);
				uint64_t hash = 0;
				if (!TryHashSingleJoinKeyRow(global.build_key_layout, keys, row, hash))
					hash = HashGroupRow(global.build_key_layout, keys, row);
				if (HashJoinPartitionIndex(hash, global.payload) != partition_id)
					elog(ERROR, "pg_yaap: hash join row placed in wrong partition");
				salts[row_idx] = HashJoinSalt(hash);
				const uint32_t bucket = static_cast<uint32_t>(hash) & (capacity - 1u);
				links[row_idx] = heads[bucket];
				heads[bucket] = row_idx;
			}
			keys->finalized = true;
			auto *rows = global.payload->build_rows_use_keys ? keys : ResolveTdc(ctx.dsa, part.build_rows_dp);
			if (rows != nullptr)
				rows->finalized = true;
		}
		global.payload->combined = true;
		global.payload->finalized = true;
		global.finalized = true;
		return SinkFinalizeType::READY;
	}
	if (global.payload != nullptr && global.payload->direct_segmented_build)
	{
		auto *registry = ResolveLocalBuildRegistry(ctx.dsa, global.payload);
		if (registry == nullptr)
			elog(ERROR, "pg_yaap: direct segmented hash join registry missing during finalize");
		global.payload->segmented_row_count = 0;
		for (uint32_t slot = 0; slot < global.payload->local_state_slot_count; ++slot)
		{
				auto &entry = registry[slot];
			if (!DsaPointerIsValid(entry.build_key_refs_dp))
				continue;
			if (!DsaPointerIsValid(global.payload->build_key_refs_dp))
				global.payload->build_key_refs_dp = buffer::YaapSegmentRowRefTableCreate(ctx.dsa,
					8192u);
			if (!global.payload->build_rows_use_keys &&
				!DsaPointerIsValid(global.payload->build_row_refs_dp))
				global.payload->build_row_refs_dp = buffer::YaapSegmentRowRefTableCreate(ctx.dsa,
					8192u);
			if (global.payload->build_rows_use_keys)
				global.payload->build_row_refs_dp = global.payload->build_key_refs_dp;
			auto *key_src = static_cast<const buffer::YaapSegmentRowRefTable *>(
				dsa_get_address(ctx.dsa, entry.build_key_refs_dp));
			auto *key_dst = static_cast<buffer::YaapSegmentRowRefTable *>(
				dsa_get_address(ctx.dsa, global.payload->build_key_refs_dp));
			const uint32_t local_count = pg_atomic_read_u32(
				const_cast<pg_atomic_uint32 *>(&key_src->count));
			for (uint32_t i = 0; i < local_count; ++i)
			{
				const auto *ref = buffer::YaapSegmentRowRefGet(ctx.dsa, key_src, i);
					if (ref == nullptr)
						ereport(ERROR, (errmsg("pg_yaap: direct hash join key ref %u/%u missing from worker %u (capacity=%u)",
							i, local_count, slot, key_src->capacity)));
					if (buffer::YaapSegmentRowRefAppend(ctx.dsa, key_dst, *ref) ==
						buffer::YAAP_INVALID_SEGMENT_ROW_REF)
						ereport(ERROR, (errmsg("pg_yaap: direct hash join key ref append rejected (row=%u segment=%u generation=%u dst_count=%u dst_capacity=%u)",
							ref->row_index, ref->segment.segment_id, ref->segment.generation,
							pg_atomic_read_u32(&key_dst->count), key_dst->capacity)));
			}
			if (!global.payload->build_rows_use_keys)
			{
				auto *row_src = static_cast<const buffer::YaapSegmentRowRefTable *>(
					dsa_get_address(ctx.dsa, entry.build_row_refs_dp));
				auto *row_dst = static_cast<buffer::YaapSegmentRowRefTable *>(
					dsa_get_address(ctx.dsa, global.payload->build_row_refs_dp));
				const uint32_t row_count = pg_atomic_read_u32(
					const_cast<pg_atomic_uint32 *>(&row_src->count));
				for (uint32_t i = 0; i < row_count; ++i)
				{
					const auto *ref = buffer::YaapSegmentRowRefGet(ctx.dsa, row_src, i);
						if (ref == nullptr)
							ereport(ERROR, (errmsg("pg_yaap: direct hash join payload ref %u/%u missing from worker %u (capacity=%u)",
								i, row_count, slot, row_src->capacity)));
						if (buffer::YaapSegmentRowRefAppend(ctx.dsa, row_dst, *ref) ==
							buffer::YAAP_INVALID_SEGMENT_ROW_REF)
							ereport(ERROR, (errmsg("pg_yaap: direct hash join payload ref append rejected (row=%u segment=%u generation=%u dst_count=%u dst_capacity=%u)",
								ref->row_index, ref->segment.segment_id, ref->segment.generation,
								pg_atomic_read_u32(&row_dst->count), row_dst->capacity)));
				}
			}
			if (local_count > UINT32_MAX - global.payload->segmented_row_count)
				elog(ERROR, "pg_yaap: segmented hash join row count exceeds uint32 range");
			global.payload->segmented_row_count += local_count;
				/* Finalize has copied this producer's immutable row directory.
				 * Retire the producer-side directory now; retaining both copies
				 * across Probe needlessly doubles DSA metadata for large builds. */
				if (DsaPointerIsValid(entry.build_key_refs_dp))
					buffer::YaapSegmentRowRefTableDestroy(ctx.dsa, entry.build_key_refs_dp);
				if (DsaPointerIsValid(entry.build_row_refs_dp) &&
					entry.build_row_refs_dp != entry.build_key_refs_dp)
					buffer::YaapSegmentRowRefTableDestroy(ctx.dsa, entry.build_row_refs_dp);
				entry.build_key_refs_dp = InvalidDsaPointer;
				entry.build_row_refs_dp = InvalidDsaPointer;
		}

		const uint32_t row_count = global.payload->segmented_row_count;
		global.payload->segmented_build = true;
		/* The directory stays resident while payload segments may spill. Its
		 * lifetime ends after the consuming pipeline RUN barrier. */
		if (row_count > 0)
		{
			uint32_t hash_capacity = 1;
			while (hash_capacity < row_count * 2u)
				hash_capacity <<= 1;
			global.payload->hash_table_capacity = hash_capacity;
			const uint64_t directory_bytes = static_cast<uint64_t>(hash_capacity) * sizeof(uint32_t) +
				static_cast<uint64_t>(row_count) * (sizeof(uint32_t) + sizeof(uint16_t));
				auto *buffer_state = ResolveBufferState(ctx);
				if (buffer_state == nullptr || directory_bytes > buffer_state->hard_limit)
					ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("pg_yaap: hash join directory exceeds query memory limit")));
				/* The build-row index is needed while its payload remains
				 * spillable. Free enough published build pages before reserving
				 * this non-spillable directory, then reload pages on probe. */
				while (buffer::YaapBufferResidentBytes(buffer_state) >
					   buffer_state->hard_limit - directory_bytes)
				{
					if (!EvictOneHashJoinSegment(ctx, buffer::YaapSegmentHandle{}, 0))
						break;
				}
				if (buffer::YaapBufferResidentBytes(buffer_state) >
					buffer_state->hard_limit - directory_bytes)
					ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("pg_yaap: insufficient memory for hash join directory after spilling build pages"),
						errdetail("requested %llu bytes; %llu of %llu bytes remain resident",
							(unsigned long long) directory_bytes,
							(unsigned long long) buffer::YaapBufferResidentBytes(buffer_state),
							(unsigned long long) buffer_state->hard_limit)));
				dsa_trim(ctx.dsa);
				buffer::YaapBufferChargeExternalBytes(buffer_state, directory_bytes,
				"hash join directory");
			global.payload->directory_charged_bytes += directory_bytes;
			global.payload->hash_table_dp = YaapDsaAllocate(ctx.dsa,
				static_cast<size_t>(hash_capacity) * sizeof(uint32_t));
			global.payload->hash_links_dp = YaapDsaAllocate(ctx.dsa,
				static_cast<size_t>(row_count) * sizeof(uint32_t));
			global.payload->hash_salts_dp = YaapDsaAllocate(ctx.dsa,
				static_cast<size_t>(row_count) * sizeof(uint16_t));
			auto *heads = static_cast<uint32_t *>(dsa_get_address(ctx.dsa,
				global.payload->hash_table_dp));
			auto *links = static_cast<uint32_t *>(dsa_get_address(ctx.dsa,
				global.payload->hash_links_dp));
			auto *salts = static_cast<uint16_t *>(dsa_get_address(ctx.dsa,
				global.payload->hash_salts_dp));
			std::memset(heads, 0xFF, static_cast<size_t>(hash_capacity) * sizeof(uint32_t));
			auto *refs = static_cast<const buffer::YaapSegmentRowRefTable *>(
				dsa_get_address(ctx.dsa, global.payload->build_key_refs_dp));
			HashJoinReadWindow window(ctx, global.payload->segment_arena_dp);
			PG_ENSURE_ERROR_CLEANUP(ReleaseHashJoinReadWindow, PointerGetDatum(&window));
			{
				for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
				{
					const auto *ref = buffer::YaapSegmentRowRefGet(ctx.dsa, refs, row_idx);
					if (ref == nullptr)
						elog(ERROR, "pg_yaap: direct segmented hash join key ref missing during finalize");
					TupleDataCollection *tdc = nullptr;
					const uint8_t *row = AcquireSegmentedHashJoinRow(ctx, global.payload, row_idx,
						true, &window, &tdc);
					if (tdc == nullptr || row == nullptr)
						elog(ERROR, "pg_yaap: direct segmented hash join key row missing during finalize");
					uint64_t hash = 0;
					if (!TryHashSingleJoinKeyRow(global.build_key_layout, tdc, row, hash))
						hash = HashGroupRow(global.build_key_layout, tdc, row);
					salts[row_idx] = HashJoinSalt(hash);
					const uint32_t bucket = static_cast<uint32_t>(hash) & (hash_capacity - 1u);
					links[row_idx] = heads[bucket];
					heads[bucket] = row_idx;
				}
			}
			PG_END_ENSURE_ERROR_CLEANUP(ReleaseHashJoinReadWindow, PointerGetDatum(&window));
			window.Release();
		}
		global.payload->combined = true;
		global.payload->finalized = true;
		global.finalized = true;
		return SinkFinalizeType::READY;
	}
	if (global.payload != nullptr)
	{
		PublishHashJoinCombinedRows(global);
		global.build_keys = ResolveTdc(ctx.dsa, global.payload->build_keys_dp);
		global.build_rows = ResolveTdc(ctx.dsa, global.payload->build_rows_dp);
	}
	if (global.build_keys == nullptr ||
		(!global.payload->build_rows_shared_local && global.build_rows == nullptr))
		elog(ERROR, "pg_yaap: hash join finalize missing global build rows");
	const uint32_t row_count = pg_atomic_read_u32(&global.build_keys->row_count);
	if (!global.payload->build_rows_shared_local &&
		!global.payload->build_rows_use_keys &&
		row_count != pg_atomic_read_u32(&global.build_rows->row_count))
		elog(ERROR, "pg_yaap: hash join finalize key/payload row counts diverged");
	if (pg_yaap_trace_execution_path)
		ereport(LOG,
			(errmsg("pg_yaap hashjoin finalize build_rows=%u key_width=%u payload_width=%u hash_capacity_pre=%u",
				row_count,
				global.build_key_layout != nullptr ? global.build_key_layout->row_width : 0,
				global.build_layout != nullptr ? global.build_layout->row_width : 0,
				row_count > 0 ? 1u : 0u)));
	if (row_count > 0)
	{
		uint32_t hash_capacity = global.payload->hash_table_capacity;
		if (!DsaPointerIsValid(global.payload->hash_table_dp) ||
			!DsaPointerIsValid(global.payload->hash_links_dp) ||
			!DsaPointerIsValid(global.payload->hash_salts_dp))
		{
			hash_capacity = global.payload->hash_table_capacity;
			if (hash_capacity == 0)
			{
				hash_capacity = 1;
				while (hash_capacity < row_count * 2u)
				{
					if (PipelineCancelRequested(ctx))
						return SinkFinalizeType::READY;
					hash_capacity <<= 1;
				}
			}
			global.payload->hash_table_capacity = hash_capacity;
			const uint64_t directory_bytes = static_cast<uint64_t>(hash_capacity) * sizeof(uint32_t) +
				static_cast<uint64_t>(row_count) * (sizeof(uint32_t) + sizeof(uint16_t));
			buffer::YaapBufferChargeExternalBytes(ResolveBufferState(ctx), directory_bytes,
				"hash join directory");
			global.payload->directory_charged_bytes += directory_bytes;
			global.payload->hash_table_dp = YaapDsaAllocate(ctx.dsa,
				static_cast<size_t>(hash_capacity) * sizeof(uint32_t));
			global.payload->hash_links_dp = YaapDsaAllocate(ctx.dsa,
				static_cast<size_t>(row_count) * sizeof(uint32_t));
			global.payload->hash_salts_dp = YaapDsaAllocate(ctx.dsa,
				static_cast<size_t>(row_count) * sizeof(uint16_t));
			auto *bucket_heads = static_cast<uint32_t *>(dsa_get_address(ctx.dsa, global.payload->hash_table_dp));
			auto *buckets = DsaPointerIsValid(global.payload->hash_buckets_dp)
				? static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, global.payload->hash_buckets_dp))
				: nullptr;
			auto *links = static_cast<uint32_t *>(dsa_get_address(ctx.dsa, global.payload->hash_links_dp));
			auto *salts = static_cast<uint16_t *>(dsa_get_address(ctx.dsa, global.payload->hash_salts_dp));
			std::memset(bucket_heads, 0xFF, static_cast<size_t>(hash_capacity) * sizeof(uint32_t));

			for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
			{
				if (PipelineCancelRequestedEvery(ctx, row_idx, 1023u))
					return SinkFinalizeType::READY;
				uint32_t bucket = 0;
				if (buckets != nullptr)
				{
					bucket = buckets[row_idx];
				}
				else
				{
					const uint8_t *row_ptr = TupleDataCollectionGetRowConst(global.build_keys, row_idx);
					uint64_t hash = 0;
					if (!TryHashSingleJoinKeyRow(global.build_key_layout, global.build_keys, row_ptr, hash))
						hash = HashGroupRow(global.build_key_layout, global.build_keys, row_ptr);
					salts[row_idx] = HashJoinSalt(hash);
					bucket = static_cast<uint32_t>(hash) & (hash_capacity - 1u);
				}
				links[row_idx] = bucket_heads[bucket];
				bucket_heads[bucket] = row_idx;
			}
		}
	}
	else
	{
		global.payload->hash_table_capacity = 0;
		global.payload->hash_table_dp = InvalidDsaPointer;
		global.payload->hash_buckets_dp = InvalidDsaPointer;
		global.payload->hash_links_dp = InvalidDsaPointer;
		global.payload->hash_salts_dp = InvalidDsaPointer;
	}
	if (row_count > 0 && BuildSegmentedHashJoinPayload(ctx, global))
	{
		FreeDsaPointerIfValid(ctx.dsa, &global.payload->build_keys_dp);
		if (!global.payload->build_rows_use_keys)
			FreeDsaPointerIfValid(ctx.dsa, &global.payload->build_rows_dp);
		global.build_keys = nullptr;
		global.build_rows = nullptr;
	}
	if (global.build_keys != nullptr)
		TupleDataCollectionResetScan(global.build_keys);
	if (global.build_rows != nullptr)
		TupleDataCollectionResetScan(global.build_rows);
	if (global.build_keys != nullptr)
		global.build_keys->finalized = true;
	if (global.build_rows != nullptr)
		global.build_rows->finalized = true;
	global.payload->finalized = true;
	global.finalized = true;
	return SinkFinalizeType::READY;
}

std::unique_ptr<OperatorState>
PhysicalHashJoin::GetOperatorState(ExecCtx &ctx)
{
	(void) ctx;
	return std::make_unique<HashJoinOperatorState>();
}

void
PhysicalHashJoin::SetProbeBatch(OperatorState &state, dsa_pointer batch_dp)
{
	auto &probe = static_cast<HashJoinOperatorState &>(state);
	if (probe.cursor.active_probe)
		elog(ERROR, "pg_yaap: cannot replace a hash join batch with active probe output");
	probe.batch_dp = batch_dp;
}

void
PhysicalHashJoin::BuildBatchPartition(ExecCtx &ctx, dsa_pointer batch_dp, uint32_t partition_id)
{
	auto *batch = static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, batch_dp));
	auto *payload = ResolvePayload(ctx.dsa, DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_ : LoadSharedPayloadFromDescriptor(this));
	if (payload == nullptr || !payload->partition_probe_mode || partition_id >= payload->build_partition_count || partition_id >= 64 ||
		((batch->partition_mask >> partition_id) & 1u) == 0 || batch->hash_capacity == 0 ||
		batch->bucket_offsets[partition_id] >= batch->hash_capacity)
		elog(ERROR, "pg_yaap: invalid batch hash join build partition");
	const auto *layout = ResolveLayout(ctx.dsa, DsaPointerIsValid(right_key_layout_dp_)
		? right_key_layout_dp_ : (desc_ != nullptr ? desc_->body.hash_join.right_key_layout : InvalidDsaPointer));
	const auto *registry = ResolveLocalBuildRegistry(ctx.dsa, payload);
	const auto *segments = batch->segment_count == 0 ? nullptr :
		static_cast<const HashJoinBatchSegment *>(dsa_get_address(ctx.dsa, batch->segments_dp));
	const auto *segment_map = batch->segment_map_count == 0 ? nullptr :
		static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, batch->segment_map_dp));
	auto *key_refs = static_cast<HashJoinBatchRowRef *>(dsa_get_address(ctx.dsa, batch->key_refs_dp));
	auto *row_refs = static_cast<HashJoinBatchRowRef *>(dsa_get_address(ctx.dsa, batch->row_refs_dp));
	auto *heads = static_cast<pg_atomic_uint32 *>(dsa_get_address(ctx.dsa, batch->hash_table_dp));
	auto *links = static_cast<uint32_t *>(dsa_get_address(ctx.dsa, batch->hash_links_dp));
	auto *salts = static_cast<uint16_t *>(dsa_get_address(ctx.dsa, batch->hash_salts_dp));
	if (layout == nullptr || registry == nullptr)
		elog(ERROR, "pg_yaap: batch hash join build metadata missing");
	struct SegmentCache {
		uint32_t ordinal = UINT32_MAX;
		TupleDataCollection *tdc = nullptr;
	} key_cache, row_cache;
	auto compact = [&](const buffer::YaapSegmentRowRef *ref, SegmentCache &cache) {
		if (ref == nullptr || ref->segment.segment_id >= batch->segment_map_count)
			elog(ERROR, "pg_yaap: batch hash join segment mapping missing");
		const uint32_t ordinal = segment_map[ref->segment.segment_id];
		if (ordinal >= batch->segment_count)
			elog(ERROR, "pg_yaap: batch hash join segment ordinal out of bounds");
		const auto &segment = segments[ordinal];
		if (segment.arena_id != ref->segment.arena_id || segment.segment_id != ref->segment.segment_id ||
			segment.generation != ref->segment.generation)
			elog(ERROR, "pg_yaap: stale batch hash join row handle");
		if (cache.ordinal != ordinal) {
			const auto *descriptor = static_cast<const buffer::YaapSharedSegmentDesc *>(
				dsa_get_address(ctx.dsa, segment.descriptor_dp));
			if (descriptor->arena_id != segment.arena_id ||
				descriptor->segment_id != segment.segment_id || descriptor->generation != segment.generation ||
				descriptor->payload_dp != segment.payload_dp || !DsaPointerIsValid(segment.payload_dp))
				elog(ERROR, "pg_yaap: stale batch hash join segment reference");
			cache.tdc = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, segment.payload_dp));
			cache.ordinal = ordinal;
		}
		auto *tdc = cache.tdc;
		if (ref->row_index >= pg_atomic_read_u32(&tdc->row_count))
			elog(ERROR, "pg_yaap: batch hash join segment row out of bounds");
		const uint64_t offset = offsetof(TupleDataCollection, rows) + static_cast<uint64_t>(ref->row_index) * tdc->row_width;
		if (offset > UINT32_MAX || offset > segment.capacity || tdc->row_width > segment.capacity - offset)
			elog(ERROR, "pg_yaap: batch hash join compact row offset out of bounds");
		return HashJoinBatchRowRef{ordinal, static_cast<uint32_t>(offset)};
	};
	/* COMBINE publishes append-ordered chains. The batch pins their payloads
	 * before dispatching builders, so chain cursors need neither a second
	 * lease nor a permanent per-row directory. Key and payload chains have
	 * independent boundaries (in particular when string heaps fill). */
	struct ChainCursor {
		dsa_pointer next_dp = InvalidDsaPointer;
		buffer::YaapSharedSegmentDesc *segment = nullptr;
		uint32_t row = 0;
		uint32_t count = 0;
		uint32_t visited = 0;
	};
	auto initialize_cursor = [&](dsa_pointer desc_dp, uint32_t expected_rows) {
		const auto *desc = buffer::YaapSegmentedTdcResolveDesc(ctx.dsa, desc_dp);
		if (desc == nullptr || desc->arena_dp != payload->segment_arena_dp ||
			desc->row_count != expected_rows)
			elog(ERROR, "pg_yaap: batch hash join chain row count mismatch");
		ChainCursor cursor;
		cursor.next_dp = desc->first_desc_dp;
		return cursor;
	};
	auto next_ref = [&](ChainCursor &cursor, buffer::YaapSegmentRowRef *ref) {
		while (cursor.row == cursor.count) {
			if (!DsaPointerIsValid(cursor.next_dp)) return false;
			if (++cursor.visited > batch->segment_count)
				elog(ERROR, "pg_yaap: batch hash join chain contains a cycle");
			cursor.segment = static_cast<buffer::YaapSharedSegmentDesc *>(
				dsa_get_address(ctx.dsa, cursor.next_dp));
			const auto *segment = cursor.segment;
			if (segment->segment_id >= batch->segment_map_count)
				elog(ERROR, "pg_yaap: batch hash join chain segment missing");
			const uint32_t ordinal = segment_map[segment->segment_id];
			if (ordinal >= batch->segment_count || segments[ordinal].descriptor_dp != cursor.next_dp ||
				!DsaPointerIsValid(segments[ordinal].payload_dp) ||
				segment->payload_dp != segments[ordinal].payload_dp ||
				segment->capacity < offsetof(TupleDataCollection, rows))
				elog(ERROR, "pg_yaap: batch hash join chain is not pinned");
			const auto *tdc = static_cast<const TupleDataCollection *>(
				dsa_get_address(ctx.dsa, segments[ordinal].payload_dp));
			cursor.count = pg_atomic_read_u32(const_cast<pg_atomic_uint32 *>(&tdc->row_count));
			if (cursor.count > tdc->row_capacity)
				elog(ERROR, "pg_yaap: batch hash join chain row count out of bounds");
			cursor.row = 0;
			cursor.next_dp = segment->chain_next_dp;
		}
		*ref = buffer::YaapSegmentRowRef{{cursor.segment->arena_id,
			cursor.segment->segment_id, cursor.segment->generation}, cursor.row++};
		return true;
	};
	uint32_t row_idx = batch->partition_offsets[partition_id];
	uint32_t row_end = batch->row_count;
	for (uint32_t next = partition_id + 1; next < payload->build_partition_count && next < 64; ++next)
		if ((batch->partition_mask >> next) & 1u) { row_end = batch->partition_offsets[next]; break; }
	uint64_t expected_rows = 0;
	uint64_t collisions = 0;
	const bool profile_build = PipelineProfileEnabled(ctx);
	for (uint32_t slot = 0; slot < payload->local_state_slot_count; ++slot) {
		if (!DsaPointerIsValid(registry[slot].partition_state_dp)) continue;
		auto *parts = static_cast<HashJoinLocalPartitionState *>(dsa_get_address(ctx.dsa, registry[slot].partition_state_dp));
		auto &part = parts[partition_id];
		expected_rows += part.row_count;
		if (part.row_count == 0) continue;
		ChainCursor keys = initialize_cursor(part.build_key_segment_desc_dp, part.row_count);
		ChainCursor rows;
		if (row_refs != key_refs)
			rows = initialize_cursor(part.build_row_segment_desc_dp, part.row_count);
		for (uint32_t local_idx = 0; local_idx < part.row_count; ++local_idx, ++row_idx) {
			if ((local_idx & 1023u) == 0) CHECK_FOR_INTERRUPTS();
			if (row_idx >= row_end || row_idx >= batch->row_count)
				elog(ERROR, "pg_yaap: batch hash join global row out of bounds");
			/* Sample one row per 1024, staggered by partition/slot. These
			 * counters contain sampled time, never extrapolated time. */
			const bool sample = profile_build && ((local_idx + partition_id * 31u + slot * 17u) & 1023u) == 0;
			instr_time ref_start{}, hash_start{}, insert_start{}, insert_end{};
			if (sample) INSTR_TIME_SET_CURRENT(ref_start);
			buffer::YaapSegmentRowRef key_ref, row_ref;
			if (!next_ref(keys, &key_ref))
				elog(ERROR, "pg_yaap: batch hash join key chain ended early");
			key_refs[row_idx] = compact(&key_ref, key_cache);
			if (row_refs != key_refs) {
				if (!next_ref(rows, &row_ref))
					elog(ERROR, "pg_yaap: batch hash join payload chain ended early");
				row_refs[row_idx] = compact(&row_ref, row_cache);
			}
			TupleDataCollection *key_store = nullptr;
			const uint8_t *key = ResolveBatchRow(ctx.dsa, batch, key_refs, row_idx, &key_store);
			if (sample) INSTR_TIME_SET_CURRENT(hash_start);
			uint64_t hash = 0;
			if (!TryHashSingleJoinKeyRow(layout, key_store, key, hash)) hash = HashGroupRow(layout, key_store, key);
			if (HashJoinPartitionIndex(hash, payload) != partition_id)
				elog(ERROR, "pg_yaap: batch hash join row assigned to wrong radix partition");
			if (sample) INSTR_TIME_SET_CURRENT(insert_start);
			salts[row_idx] = HashJoinSalt(hash);
			const uint32_t bucket = batch->bucket_offsets[partition_id] +
				(static_cast<uint32_t>(hash) & batch->bucket_masks[partition_id]);
			/* One builder owns this entire range. Probe starts after built_workers
			 * publication, so no read-modify-write operation is needed. */
			const uint32_t previous = pg_atomic_read_u32(&heads[bucket]);
			pg_atomic_write_u32(&heads[bucket], row_idx);
			links[row_idx] = previous;
			collisions += previous != HASH_JOIN_INVALID_ROW;
			if (sample) {
				INSTR_TIME_SET_CURRENT(insert_end);
				PipelineProfileAddDiff(ctx, PipelineProfileStage::HASH_JOIN_BATCH_REF_SAMPLE, hash_start, ref_start, 1);
				PipelineProfileAddDiff(ctx, PipelineProfileStage::HASH_JOIN_BATCH_HASH_SAMPLE, insert_start, hash_start, 1);
				PipelineProfileAddDiff(ctx, PipelineProfileStage::HASH_JOIN_BATCH_INSERT_SAMPLE, insert_end, insert_start, 1);
			}
		}
		buffer::YaapSegmentRowRef extra;
		if (next_ref(keys, &extra) || (row_refs != key_refs && next_ref(rows, &extra)))
			elog(ERROR, "pg_yaap: batch hash join chain has excess rows");
	}
	if (static_cast<uint64_t>(batch->partition_offsets[partition_id]) + expected_rows != row_idx || row_idx != row_end)
		elog(ERROR, "pg_yaap: batch hash join partition row count mismatch");
	PipelineProfileScope collision_scope(ctx, PipelineProfileStage::HASH_JOIN_BATCH_COLLISIONS);
	collision_scope.AddRows(collisions);
}

bool
PhysicalHashJoin::IsBatchBuildRowMatched(ExecCtx &ctx, dsa_pointer batch_dp, uint32_t row_idx) const
{
	auto *batch = static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, batch_dp));
	if (batch == nullptr || row_idx >= batch->row_count || !DsaPointerIsValid(batch->matched_bitmap_dp))
		elog(ERROR, "pg_yaap: invalid build-output hash join matched-bit lookup");
	auto *words = static_cast<pg_atomic_uint32 *>(dsa_get_address(ctx.dsa, batch->matched_bitmap_dp));
	const uint32_t word = pg_atomic_read_u32(&words[row_idx >> 5]);
	return (word & (UINT32_C(1) << (row_idx & 31u))) != 0;
}

void
PhysicalHashJoin::EmitBuildResultRow(ExecCtx &ctx, dsa_pointer batch_dp, uint32_t row_idx,
					     OperatorState &state, PipelineChunk &out)
{
	auto *batch = static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, batch_dp));
	if (batch == nullptr || row_idx >= batch->row_count || !DsaPointerIsValid(batch->row_refs_dp))
		elog(ERROR, "pg_yaap: invalid build-output hash join row reference");
	auto *refs = static_cast<HashJoinBatchRowRef *>(dsa_get_address(ctx.dsa, batch->row_refs_dp));
	TupleDataCollection *store = nullptr;
	const uint8_t *row = ResolveBatchRow(ctx.dsa, batch, refs, row_idx, &store);
	auto &join_state = static_cast<HashJoinOperatorState &>(state);
	const HashJoinMatchMode mode = desc_ != nullptr ? desc_->body.hash_join.join_mode : join_mode_;
	InitializeHashJoinOutputState(ctx, *this, join_state, mode);
	CopyUnmatchedBuildRow(store, row, join_state.fast_probe.resolved_output_columns.data(),
			      join_state.fast_probe.output_column_count, out, out.count);
	++out.count;
}

void
PhysicalHashJoin::ReleaseBuildPayloadAfterConsumerRun(ExecCtx &ctx)
{
	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	HashJoinSharedPayload *payload = ResolvePayload(ctx.dsa, payload_dp);
	if (payload == nullptr)
		return;
	const bool direct_segmented_build = payload->direct_segmented_build;
	/* Called after the consumer RUN barrier: all probe windows are closed.
	 * CopyResolvedJoinRow materializes string bytes into its output chunk, so
	 * downstream operators retain no references to the build segments. */

	uint32 expected = 0;
	if (!pg_atomic_compare_exchange_u32(&payload->release_state, &expected, 1))
		return;

	const uint64_t global_keys_bytes = EstimateTdcAllocBytes(ctx.dsa, payload->build_keys_dp);
	const uint64_t global_rows_bytes = EstimateTdcAllocBytes(ctx.dsa, payload->build_rows_dp);
	uint64_t local_keys_bytes = 0;
	uint64_t local_rows_bytes = 0;
	auto *registry = ResolveLocalBuildRegistry(ctx.dsa, payload);
	/* Direct registry TDC pointers are writer snapshots and may point to a
	 * spilled/freed payload. Only stable segment handles own that storage. */
	if (registry != nullptr && !direct_segmented_build)
	{
		for (uint32_t i = 0; i < payload->local_state_slot_count; ++i)
		{
			local_keys_bytes += EstimateTdcAllocBytes(ctx.dsa, registry[i].build_keys_dp);
			local_rows_bytes += EstimateTdcAllocBytes(ctx.dsa, registry[i].build_rows_dp);
		}
	}
	const uint64_t hash_bytes = DsaPointerIsValid(payload->hash_table_dp)
		? static_cast<uint64_t>(payload->hash_table_capacity) * sizeof(uint32_t)
		: 0;
	TupleDataCollection *build_keys = ResolveTdc(ctx.dsa, payload->build_keys_dp);
	const uint64_t bucket_bytes = DsaPointerIsValid(payload->hash_buckets_dp) && build_keys != nullptr
		? static_cast<uint64_t>(pg_atomic_read_u32(&build_keys->row_count)) * sizeof(uint32_t)
		: 0;
	const uint64_t link_bytes = DsaPointerIsValid(payload->hash_links_dp) && build_keys != nullptr
		? static_cast<uint64_t>(pg_atomic_read_u32(&build_keys->row_count)) * sizeof(uint32_t)
		: 0;
	const uint64_t salt_bytes = DsaPointerIsValid(payload->hash_salts_dp) && build_keys != nullptr
		? static_cast<uint64_t>(pg_atomic_read_u32(&build_keys->row_count)) * sizeof(uint16_t)
		: 0;
	(void) salt_bytes;
	(void) bucket_bytes;
	if (payload->build_partition_count > 1)
	{
		auto *partitions = ResolveHashJoinPartitions(ctx.dsa, payload);
		if (partitions != nullptr)
		{
			for (uint32_t partition_id = 0; partition_id < payload->build_partition_count; ++partition_id)
			{
				auto &part = partitions[partition_id];
				FreeDsaPointerIfValid(ctx.dsa, &part.hash_table_dp);
				FreeDsaPointerIfValid(ctx.dsa, &part.hash_links_dp);
				FreeDsaPointerIfValid(ctx.dsa, &part.hash_salts_dp);
				FreeDsaPointerIfValid(ctx.dsa, &part.hash_key_values_dp);
				FreeDsaPointerIfValid(ctx.dsa, &part.build_row_slots_dp);
				FreeDsaPointerIfValid(ctx.dsa, &part.build_row_local_idxs_dp);
				if (direct_segmented_build)
				{
					if (part.directory_charged_bytes > 0)
						buffer::YaapBufferReleaseExternalBytes(ResolveBufferState(ctx),
							part.directory_charged_bytes);
					part.directory_charged_bytes = 0;
					if (DsaPointerIsValid(part.build_key_refs_dp))
						buffer::YaapSegmentRowRefTableDestroy(ctx.dsa, part.build_key_refs_dp);
					if (DsaPointerIsValid(part.build_row_refs_dp) &&
						part.build_row_refs_dp != part.build_key_refs_dp)
						buffer::YaapSegmentRowRefTableDestroy(ctx.dsa, part.build_row_refs_dp);
					part.build_key_refs_dp = InvalidDsaPointer;
					part.build_row_refs_dp = InvalidDsaPointer;
					part.directory_charged_bytes = 0;
				}
				else
				{
					FreeDsaPointerIfValid(ctx.dsa, &part.build_keys_dp);
					if (!payload->build_rows_use_keys)
						FreeDsaPointerIfValid(ctx.dsa, &part.build_rows_dp);
				}
			}
		}
		FreeDsaPointerIfValid(ctx.dsa, &payload->partitions_dp);
	}

	FreeDsaPointerIfValid(ctx.dsa, &payload->hash_table_dp);
	FreeDsaPointerIfValid(ctx.dsa, &payload->hash_buckets_dp);
	FreeDsaPointerIfValid(ctx.dsa, &payload->hash_links_dp);
	FreeDsaPointerIfValid(ctx.dsa, &payload->hash_salts_dp);
	buffer::YaapBufferReleaseExternalBytes(ResolveBufferState(ctx), payload->directory_charged_bytes);
	payload->directory_charged_bytes = 0;
	FreeDsaPointerIfValid(ctx.dsa, &payload->build_row_slots_dp);
	FreeDsaPointerIfValid(ctx.dsa, &payload->build_row_local_idxs_dp);
	if (direct_segmented_build)
	{
		if (registry != nullptr)
		{
			for (uint32_t i = 0; i < payload->local_state_slot_count; ++i)
			{
				if (payload->build_partition_count > 1)
				{
					auto *local_parts = DsaPointerIsValid(registry[i].partition_state_dp)
						? static_cast<HashJoinLocalPartitionState *>(dsa_get_address(ctx.dsa,
							registry[i].partition_state_dp)) : nullptr;
					if (local_parts != nullptr)
					{
						for (uint32_t partition_id = 0;
							partition_id < payload->build_partition_count; ++partition_id)
						{
							auto &local_part = local_parts[partition_id];
							ReleaseSegmentedHashJoinChain(ctx,
								local_part.build_key_segment_desc_dp,
								payload->segment_arena_dp);
							if (local_part.build_row_segment_desc_dp !=
								local_part.build_key_segment_desc_dp)
								ReleaseSegmentedHashJoinChain(ctx,
									local_part.build_row_segment_desc_dp,
									payload->segment_arena_dp);
							if (DsaPointerIsValid(local_part.build_key_refs_dp))
								buffer::YaapSegmentRowRefTableDestroy(ctx.dsa,
									local_part.build_key_refs_dp);
							if (DsaPointerIsValid(local_part.build_row_refs_dp) &&
								local_part.build_row_refs_dp != local_part.build_key_refs_dp)
								buffer::YaapSegmentRowRefTableDestroy(ctx.dsa,
									local_part.build_row_refs_dp);
						}
						FreeDsaPointerIfValid(ctx.dsa, &registry[i].partition_state_dp);
					}
				}
				else
				{
					ReleaseSegmentedHashJoinChain(ctx,
						registry[i].build_key_segment_desc_dp, payload->segment_arena_dp);
					if (registry[i].build_row_segment_desc_dp !=
						registry[i].build_key_segment_desc_dp)
						ReleaseSegmentedHashJoinChain(ctx,
							registry[i].build_row_segment_desc_dp, payload->segment_arena_dp);
					if (DsaPointerIsValid(registry[i].build_key_refs_dp))
						buffer::YaapSegmentRowRefTableDestroy(ctx.dsa, registry[i].build_key_refs_dp);
					if (DsaPointerIsValid(registry[i].build_row_refs_dp) &&
						registry[i].build_row_refs_dp != registry[i].build_key_refs_dp)
						buffer::YaapSegmentRowRefTableDestroy(ctx.dsa, registry[i].build_row_refs_dp);
				}
			}
		}
		if (DsaPointerIsValid(payload->build_key_refs_dp))
			buffer::YaapSegmentRowRefTableDestroy(ctx.dsa, payload->build_key_refs_dp);
		if (DsaPointerIsValid(payload->build_row_refs_dp) &&
			payload->build_row_refs_dp != payload->build_key_refs_dp)
			buffer::YaapSegmentRowRefTableDestroy(ctx.dsa, payload->build_row_refs_dp);
		payload->build_key_refs_dp = InvalidDsaPointer;
		payload->build_row_refs_dp = InvalidDsaPointer;
		payload->direct_segmented_build = false;
		payload->segmented_build = false;
	}
	else if (payload->segmented_build)
	{
		ReleaseSegmentedHashJoinChain(ctx, payload->build_key_segment_desc_dp,
			payload->segment_arena_dp);
		if (payload->build_row_segment_desc_dp != payload->build_key_segment_desc_dp)
			ReleaseSegmentedHashJoinChain(ctx, payload->build_row_segment_desc_dp,
				payload->segment_arena_dp);
		if (DsaPointerIsValid(payload->build_key_refs_dp))
			buffer::YaapSegmentRowRefTableDestroy(ctx.dsa, payload->build_key_refs_dp);
		if (DsaPointerIsValid(payload->build_row_refs_dp) &&
			payload->build_row_refs_dp != payload->build_key_refs_dp)
			buffer::YaapSegmentRowRefTableDestroy(ctx.dsa, payload->build_row_refs_dp);
		payload->build_key_segment_desc_dp = InvalidDsaPointer;
		payload->build_row_segment_desc_dp = InvalidDsaPointer;
		payload->build_key_refs_dp = InvalidDsaPointer;
		payload->build_row_refs_dp = InvalidDsaPointer;
		payload->segmented_build = false;
	}
	if (!direct_segmented_build)
	{
		FreeDsaPointerIfValid(ctx.dsa, &payload->build_keys_dp);
		if (!payload->build_rows_use_keys)
			FreeDsaPointerIfValid(ctx.dsa, &payload->build_rows_dp);
	}
	if (registry != nullptr)
	{
		for (uint32_t i = 0; i < payload->local_state_slot_count; ++i)
		{
			if (!direct_segmented_build)
			{
				FreeDsaPointerIfValid(ctx.dsa, &registry[i].build_keys_dp);
				if (!payload->build_rows_use_keys)
					FreeDsaPointerIfValid(ctx.dsa, &registry[i].build_rows_dp);
			}
		}
	}
	FreeDsaPointerIfValid(ctx.dsa, &payload->local_build_registry_dp);

	if (pg_yaap_trace_execution_path)
		ereport(LOG,
			(errmsg("pg_yaap hashjoin release op=%p global_keys=" UINT64_FORMAT " global_rows=" UINT64_FORMAT " local_keys=" UINT64_FORMAT " local_rows=" UINT64_FORMAT " hash=" UINT64_FORMAT " links=" UINT64_FORMAT,
				static_cast<void *>(this),
				(uint64) global_keys_bytes,
				(uint64) global_rows_bytes,
				(uint64) local_keys_bytes,
				(uint64) local_rows_bytes,
				(uint64) hash_bytes,
				(uint64) link_bytes)));

	ClearSharedPayloadOnDescriptor(this);
	shared_payload_dp_ = InvalidDsaPointer;
	pg_atomic_write_u32(&payload->release_state, 2);
	dsa_free(ctx.dsa, payload_dp);
}

void
PhysicalHashJoin::ReleaseBuildPayloadAtQueryEnd(ExecCtx &ctx)
{
	ReleaseBuildPayloadAfterConsumerRun(ctx);
}

bool
PhysicalHashJoin::EvictUnusedBuildSegment(ExecCtx &ctx,
                                        const buffer::YaapSegmentHandle &exclude)
{
	return EvictOneHashJoinSegment(ctx, exclude, 0);
}

void
PhysicalHashJoin::ReleaseBuildPartitionAfterProbe(ExecCtx &ctx, uint32_t partition_id)
{
	auto *payload = ResolvePayload(ctx.dsa, DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_ : LoadSharedPayloadFromDescriptor(this));
	if (payload == nullptr || !payload->partition_probe_mode)
		return;
	auto *parts = ResolveHashJoinPartitions(ctx.dsa, payload);
	auto *registry = ResolveLocalBuildRegistry(ctx.dsa, payload);
	if (parts == nullptr || registry == nullptr || partition_id >= payload->build_partition_count)
		elog(ERROR, "pg_yaap: paired join partition cleanup state missing");
	for (uint32_t slot = 0; slot < payload->local_state_slot_count; ++slot)
	{
		if (!DsaPointerIsValid(registry[slot].partition_state_dp))
			continue;
		auto *local_parts = static_cast<HashJoinLocalPartitionState *>(
			dsa_get_address(ctx.dsa, registry[slot].partition_state_dp));
		auto &part = local_parts[partition_id];
		ReleaseSegmentedHashJoinChain(ctx, part.build_key_segment_desc_dp, payload->segment_arena_dp, true);
		if (part.build_row_segment_desc_dp != part.build_key_segment_desc_dp)
			ReleaseSegmentedHashJoinChain(ctx, part.build_row_segment_desc_dp, payload->segment_arena_dp, true);
		if (DsaPointerIsValid(part.build_key_refs_dp))
			buffer::YaapSegmentRowRefTableDestroy(ctx.dsa, part.build_key_refs_dp);
		if (DsaPointerIsValid(part.build_row_refs_dp) && part.build_row_refs_dp != part.build_key_refs_dp)
			buffer::YaapSegmentRowRefTableDestroy(ctx.dsa, part.build_row_refs_dp);
		part = HashJoinLocalPartitionState{};
	}
	auto &part = parts[partition_id];
	FreeDsaPointerIfValid(ctx.dsa, &part.hash_table_dp);
	FreeDsaPointerIfValid(ctx.dsa, &part.hash_links_dp);
	FreeDsaPointerIfValid(ctx.dsa, &part.hash_salts_dp);
	FreeDsaPointerIfValid(ctx.dsa, &part.hash_key_values_dp);
	FreeDsaPointerIfValid(ctx.dsa, &part.build_row_slots_dp);
	FreeDsaPointerIfValid(ctx.dsa, &part.build_row_local_idxs_dp);
	buffer::YaapBufferReleaseExternalBytes(ResolveBufferState(ctx), part.directory_charged_bytes);
	part = HashJoinPartition{};
}

OperatorResultType
PhysicalHashJoin::Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state)
{
	auto &op_state = static_cast<HashJoinOperatorState &>(state);
	if (pg_yaap_trace_execution_path)
		elog(LOG,
			 "pg_yaap hashjoin enter op=%p in_count=%u current_drained=%d join_mode=%u",
			 static_cast<void *>(this),
			 in.count,
			 op_state.cursor.current_input_drained ? 1 : 0,
			 static_cast<unsigned>(desc_ != nullptr ? desc_->body.hash_join.join_mode : join_mode_));
	if (op_state.cursor.current_input_drained)
	{
		op_state.cursor.current_input_drained = false;
		out.reset();
		return OperatorResultType::NEED_MORE_INPUT;
	}
	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	HashJoinSharedPayload *payload = ResolvePayload(ctx.dsa, payload_dp);
	if (payload == nullptr || !payload->finalized)
		elog(ERROR, "pg_yaap: hash join probe ran before build/finalize completed");
	if (pg_atomic_read_u32(&payload->release_state) != 0)
		elog(ERROR, "pg_yaap: hash join payload used after release");
	auto *batch = DsaPointerIsValid(op_state.batch_dp)
		? static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, op_state.batch_dp)) : nullptr;
	const bool partitioned_build = batch == nullptr && payload->build_partition_count > 1;
	if (pg_yaap_trace_execution_path)
		elog(LOG,
			 "pg_yaap hashjoin payload ready op=%p build_keys_dp=%llu build_rows_dp=%llu finalized=%d",
			 static_cast<void *>(this),
			 static_cast<unsigned long long>(payload->build_keys_dp),
			 static_cast<unsigned long long>(payload->build_rows_dp),
			 payload->finalized ? 1 : 0);
	TupleDataCollection *build_keys = payload->segmented_build ? nullptr :
		ResolveTdc(ctx.dsa, payload->build_keys_dp);
	const bool shared_local_payload = batch == nullptr && payload->build_rows_shared_local;
	TupleDataCollection *build_rows = shared_local_payload || payload->segmented_build ? nullptr :
		ResolveTdc(ctx.dsa, payload->build_rows_dp);
	dsa_pointer selected_key_refs_dp = payload->build_key_refs_dp;
	dsa_pointer selected_row_refs_dp = payload->build_row_refs_dp;
	HashJoinPartition *selected_partition = nullptr;
	uint32_t selected_partition_id = UINT32_MAX;
	if (batch == nullptr && ((!partitioned_build && !payload->segmented_build && build_keys == nullptr) ||
		(!partitioned_build && !shared_local_payload && !payload->segmented_build && build_rows == nullptr)))
		elog(ERROR, "pg_yaap: hash join probe missing finalized build-side rows");
	const HashJoinLocalBuildRegistryEntry *shared_local_registry = nullptr;
	const uint32_t *shared_local_slots = nullptr;
	const uint32_t *shared_local_idxs = nullptr;
	if (shared_local_payload)
	{
		shared_local_registry = ResolveLocalBuildRegistry(ctx.dsa, payload);
		if (shared_local_registry == nullptr ||
			!DsaPointerIsValid(payload->build_row_slots_dp) ||
			!DsaPointerIsValid(payload->build_row_local_idxs_dp))
			elog(ERROR, "pg_yaap: shared-local hash join payload mappings missing");
		shared_local_slots = static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, payload->build_row_slots_dp));
		shared_local_idxs = static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, payload->build_row_local_idxs_dp));
		if (shared_local_slots == nullptr || shared_local_idxs == nullptr)
			elog(ERROR, "pg_yaap: shared-local hash join payload mappings unresolved");
	}
	const TupleDataLayout *probe_layout = ResolveLayout(ctx.dsa,
		DsaPointerIsValid(left_key_layout_dp_) ? left_key_layout_dp_ :
		(desc_ != nullptr ? desc_->body.hash_join.left_key_layout : InvalidDsaPointer));
	const TupleDataLayout *build_key_layout = ResolveLayout(ctx.dsa,
		DsaPointerIsValid(right_key_layout_dp_) ? right_key_layout_dp_ :
		(desc_ != nullptr ? desc_->body.hash_join.right_key_layout : InvalidDsaPointer));
	const TupleDataLayout *build_row_layout = ResolveLayout(ctx.dsa,
		DsaPointerIsValid(right_payload_layout_dp_) ? right_payload_layout_dp_ :
		(desc_ != nullptr ? desc_->body.hash_join.right_payload_layout : InvalidDsaPointer));
	if (probe_layout == nullptr || build_key_layout == nullptr || build_row_layout == nullptr)
		elog(ERROR, "pg_yaap: hash join probe missing payload layouts");
	const auto *output_columns = DsaPointerIsValid(output_columns_dp_)
		? static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, output_columns_dp_))
		: (desc_ != nullptr && DsaPointerIsValid(desc_->body.hash_join.output_columns)
			? static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, desc_->body.hash_join.output_columns))
			: nullptr);
	const auto *filter_inputs = DsaPointerIsValid(filter_inputs_dp_)
		? ResolveFilterInputs(ctx.dsa, filter_inputs_dp_)
		: (desc_ != nullptr ? ResolveFilterInputs(ctx.dsa, desc_->body.hash_join.filter_inputs) : nullptr);
	const auto *filter_exprs = DsaPointerIsValid(filter_exprs_dp_)
		? ResolveFilterExprs(ctx.dsa, filter_exprs_dp_)
		: (desc_ != nullptr ? ResolveFilterExprs(ctx.dsa, desc_->body.hash_join.filter_exprs) : nullptr);
	const auto *filter_steps = DsaPointerIsValid(filter_steps_dp_)
		? ResolveFilterSteps(ctx.dsa, filter_steps_dp_)
		: (desc_ != nullptr ? ResolveFilterSteps(ctx.dsa, desc_->body.hash_join.filter_steps) : nullptr);
	const char *filter_string_consts = DsaPointerIsValid(filter_string_consts_dp_)
		? static_cast<const char *>(dsa_get_address(ctx.dsa, filter_string_consts_dp_))
		: (desc_ != nullptr && DsaPointerIsValid(desc_->body.hash_join.filter_string_consts)
			? static_cast<const char *>(dsa_get_address(ctx.dsa, desc_->body.hash_join.filter_string_consts))
			: nullptr);
	uint16_t output_column_count = output_column_count_ > 0 ? output_column_count_ :
		(desc_ != nullptr ? desc_->body.hash_join.output_column_count : 0);
	const uint16_t n_filter_inputs = n_filter_inputs_ > 0 ? n_filter_inputs_ :
		(desc_ != nullptr ? desc_->body.hash_join.n_filter_inputs : 0);
	const uint16_t n_filter_exprs = n_filter_exprs_ > 0 ? n_filter_exprs_ :
		(desc_ != nullptr ? desc_->body.hash_join.n_filter_exprs : 0);
	const uint16_t n_filter_steps = n_filter_steps_ > 0 ? n_filter_steps_ :
		(desc_ != nullptr ? desc_->body.hash_join.n_filter_steps : 0);
	const HashJoinMatchMode join_mode = desc_ != nullptr ? desc_->body.hash_join.join_mode : join_mode_;
	if (HashJoinScansBuildResults(join_mode) && batch == nullptr)
		elog(ERROR, "pg_yaap: build-output hash join requires the partitioned probe path");
	if (output_columns == nullptr || output_column_count == 0)
		elog(ERROR, "pg_yaap: hash join probe missing output column mapping");
	if ((n_filter_inputs > 0 && filter_inputs == nullptr) ||
		(n_filter_exprs > 0 && filter_exprs == nullptr) ||
		(n_filter_steps > 0 && filter_steps == nullptr))
		elog(ERROR, "pg_yaap: hash join probe missing residual join filter metadata");
	if (batch == nullptr && !partitioned_build && (!DsaPointerIsValid(payload->hash_table_dp) ||
		!DsaPointerIsValid(payload->hash_links_dp) ||
		!DsaPointerIsValid(payload->hash_salts_dp)))
		return OperatorResultType::NEED_MORE_INPUT;

	const uint32_t *bucket_heads = partitioned_build || batch != nullptr ? nullptr :
		static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, payload->hash_table_dp));
	const uint32_t *links = partitioned_build || batch != nullptr ? nullptr :
		static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, payload->hash_links_dp));
	const uint16_t *salts = partitioned_build || batch != nullptr ? nullptr :
		static_cast<const uint16_t *>(dsa_get_address(ctx.dsa, payload->hash_salts_dp));
	uint32_t hash_table_capacity = payload->hash_table_capacity;
	const pg_atomic_uint32 *batch_heads = nullptr;
	if (batch != nullptr)
	{
		batch_heads = static_cast<const pg_atomic_uint32 *>(dsa_get_address(ctx.dsa, batch->hash_table_dp));
		links = static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, batch->hash_links_dp));
		salts = static_cast<const uint16_t *>(dsa_get_address(ctx.dsa, batch->hash_salts_dp));
		hash_table_capacity = batch->hash_capacity;
	}
	const uint8_t *scalar_key_values = nullptr;
	auto select_partition = [&](uint32_t partition_id) {
		auto *parts = ResolveHashJoinPartitions(ctx.dsa, payload);
		if (parts == nullptr || partition_id >= payload->build_partition_count)
			elog(ERROR, "pg_yaap: hash join probe partition missing");
		auto &part = parts[partition_id];
		if (payload->direct_segmented_build)
		{
			selected_partition = &part;
			selected_partition_id = partition_id;
			build_keys = nullptr;
			build_rows = nullptr;
			selected_key_refs_dp = part.build_key_refs_dp;
			selected_row_refs_dp = part.build_row_refs_dp;
		}
		else
		{
			build_keys = ResolveTdc(ctx.dsa, part.build_keys_dp);
			build_rows = payload->build_rows_use_keys ? build_keys :
				ResolveTdc(ctx.dsa, part.build_rows_dp);
		}
		bucket_heads = DsaPointerIsValid(part.hash_table_dp) ?
			static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, part.hash_table_dp)) : nullptr;
		links = DsaPointerIsValid(part.hash_links_dp) ?
			static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, part.hash_links_dp)) : nullptr;
		salts = DsaPointerIsValid(part.hash_salts_dp) ?
			static_cast<const uint16_t *>(dsa_get_address(ctx.dsa, part.hash_salts_dp)) : nullptr;
		scalar_key_values = DsaPointerIsValid(part.hash_key_values_dp)
			? static_cast<const uint8_t *>(dsa_get_address(ctx.dsa, part.hash_key_values_dp)) : nullptr;
		hash_table_capacity = part.hash_table_capacity;
	};
	auto candidate_matches = [&](uint32_t row, uint16_t probe_row) {
		if (scalar_key_values == nullptr)
			return salts[row] == op_state.cursor.probe_salt;
		const auto &build_col = build_key_layout->columns[0];
		if (probe_layout->column_count != 1 || probe_layout->columns[0].kind != build_col.kind ||
			probe_layout->columns[0].width != build_col.width)
			return false;
		const auto &probe_col = probe_layout->columns[0];
		const uint8_t *value = scalar_key_values + static_cast<size_t>(row) * build_col.width;
		if (build_col.kind == TdcColumnKind::INT32)
		{
			int32_t key;
			std::memcpy(&key, value, sizeof(key));
			return key == in.get_int32(probe_col.src_col_idx, probe_row);
		}
		int64_t key;
		std::memcpy(&key, value, sizeof(key));
		return key == in.get_int64(probe_col.src_col_idx, probe_row);
	};
	InitializeHashJoinOutputState(ctx, *this, op_state, join_mode);
	uint16_t matched_probe_rows[PIPELINE_DEFAULT_CHUNK_SIZE];
	const uint8_t *matched_build_rows[PIPELINE_DEFAULT_CHUNK_SIZE];
	const TupleDataCollection *matched_build_row_tdcs[PIPELINE_DEFAULT_CHUNK_SIZE];
	uint8_t marker_states[PIPELINE_DEFAULT_CHUNK_SIZE]; /* 0=false, 1=true, 2=NULL */
	const bool mark_mode = join_mode == HashJoinMatchMode::MARK ||
		join_mode == HashJoinMatchMode::MARK_FILTER_NOT;
	const bool mark_filter_not = join_mode == HashJoinMatchMode::MARK_FILTER_NOT;
	std::unique_ptr<DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>> filter_chunk_holder;
	uint8_t filter_bool_values[FILTER_MAX_BOOL_REGS];
	const bool has_join_filter = n_filter_inputs > 0 && n_filter_exprs > 0 && n_filter_steps > 0;
	SimpleJoinFilterSpec simple_join_filter;
	const bool has_simple_join_filter =
		has_join_filter &&
		TryBuildSimpleJoinFilterSpec(filter_inputs,
			n_filter_inputs,
			filter_exprs,
			n_filter_exprs,
			filter_steps,
			n_filter_steps,
			build_row_layout,
			simple_join_filter);
	const uint16_t required_bool_regs =
		(has_join_filter && !has_simple_join_filter && filter_exprs != nullptr) ?
			RequiredFilterBoolRegs(filter_exprs, n_filter_exprs) : 0;
	const bool needs_build_row =
		has_join_filter || OutputColumnsReferenceRight(output_columns, output_column_count);
	if (has_join_filter && !has_simple_join_filter)
		filter_chunk_holder = std::make_unique<DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>>();
	out.reset();
	uint32_t matched_rows = 0;

	if (batch == nullptr && !partitioned_build && !shared_local_payload && !payload->segmented_build &&
		!HashJoinScansBuildResults(join_mode) &&
		TryFastInnerJoinProbe(ctx,
			op_state.fast_probe,
			payload->hash_table_capacity,
			bucket_heads,
			links,
			salts,
			build_keys,
			build_rows,
			in,
			out,
			op_state.cursor,
			matched_rows))
	{
		op_state.initialized = true;
		return out.count > 0 ? OperatorResultType::HAVE_MORE_OUTPUT : OperatorResultType::NEED_MORE_INPUT;
	}

	if (!op_state.cursor.active_probe)
	{
		op_state.cursor.active_probe = true;
		op_state.cursor.probe_row_idx = 0;
		op_state.cursor.build_row_idx = HASH_JOIN_INVALID_ROW;
		op_state.cursor.have_build_cursor = false;
		op_state.cursor.probe_salt = 0;
		op_state.cursor.probe_matched = false;
			op_state.cursor.probe_key_null = false;
		op_state.cursor.partition_id = UINT32_MAX;
		/* Count-sort this input chunk by radix partition without moving its
		 * columns. The cursor remains an ordinal in this stable permutation,
		 * so output-full resumptions retain the same input row and hash. */
		if (partitioned_build && payload->segmented_build)
		{
			PgVector<uint16_t> offsets(payload->build_partition_count, 0,
				PgMemoryContextAllocator<uint16_t>(ctx.mcxt));
			for (uint16_t i = 0; i < in.count; ++i)
			{
				uint64_t hash = 0;
				if (!TryHashSingleJoinKeyChunkRow(probe_layout, in, i, hash))
					hash = HashGroup(probe_layout, in, i);
				op_state.probe_hashes[i] = hash;
				++offsets[HashJoinPartitionIndex(hash, payload)];
			}
			uint16_t offset = 0;
			for (auto &count : offsets)
			{
				const uint16_t rows = count;
				count = offset;
				offset += rows;
			}
			for (uint16_t i = 0; i < in.count; ++i)
				op_state.probe_order[offsets[HashJoinPartitionIndex(
					op_state.probe_hashes[i], payload)]++] = i;
		}
		else
		{
			for (uint16_t i = 0; i < in.count; ++i)
				op_state.probe_order[i] = i;
		}
	}
	else if (partitioned_build && op_state.cursor.have_build_cursor)
		select_partition(op_state.cursor.partition_id);

	HashJoinReadWindow window(ctx, payload->segment_arena_dp);
	PG_ENSURE_ERROR_CLEANUP(ReleaseHashJoinReadWindow, PointerGetDatum(&window));
	{
		while (op_state.cursor.probe_row_idx < in.count)
		{
			const uint16_t probe_row_idx = op_state.probe_order[op_state.cursor.probe_row_idx];
			uint16_t batch_count = 0;
			if (PipelineCancelRequestedEvery(ctx, op_state.cursor.probe_row_idx))
				break;
			if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
				break;
			if (!op_state.cursor.have_build_cursor)
			{
				if (mark_mode)
				{
					if (probe_layout->column_count != 1)
						elog(ERROR, "pg_yaap: MARK hash join currently requires one equality key");
					const uint16_t key_slot = probe_layout->columns[0].src_col_idx;
					if (key_slot >= 16)
						elog(ERROR, "pg_yaap: MARK probe key slot out of range");
					op_state.cursor.probe_key_null = in.nulls[key_slot][probe_row_idx] != 0;
					if (op_state.cursor.probe_key_null)
					{
						op_state.cursor.build_row_idx = HASH_JOIN_INVALID_ROW;
						op_state.cursor.have_build_cursor = true;
						op_state.cursor.probe_matched = false;
						continue;
					}
				}
				PipelineProfileScope lookup_scope(ctx,
					PipelineProfileStage::HASH_JOIN_LOOKUP);
				uint64_t hash = 0;
				if (partitioned_build && payload->segmented_build)
					hash = op_state.probe_hashes[probe_row_idx];
				else if (!TryHashSingleJoinKeyChunkRow(probe_layout, in, probe_row_idx, hash))
					hash = HashGroup(probe_layout, in, probe_row_idx);
				if (batch != nullptr && ((batch->partition_mask >> HashJoinPartitionIndex(hash, payload)) & 1u) == 0)
					elog(ERROR, "pg_yaap: probe row belongs to a partition outside the resident hash batch");
				if (partitioned_build)
				{
					op_state.cursor.partition_id = HashJoinPartitionIndex(hash, payload);
					select_partition(op_state.cursor.partition_id);
					if (hash_table_capacity == 0 || bucket_heads == nullptr || links == nullptr ||
						(salts == nullptr && scalar_key_values == nullptr))
					{
						op_state.cursor.build_row_idx = HASH_JOIN_INVALID_ROW;
						op_state.cursor.have_build_cursor = true;
						op_state.cursor.probe_matched = false;
						continue;
					}
				}
				op_state.cursor.probe_salt = HashJoinSalt(hash);
				const uint32_t bucket = batch != nullptr
					? batch->bucket_offsets[HashJoinPartitionIndex(hash, payload)] +
						(static_cast<uint32_t>(hash) & batch->bucket_masks[HashJoinPartitionIndex(hash, payload)])
					: static_cast<uint32_t>(hash) & (hash_table_capacity - 1u);
				op_state.cursor.build_row_idx = batch_heads != nullptr
					? pg_atomic_read_u32(const_cast<pg_atomic_uint32 *>(&batch_heads[bucket])) : bucket_heads[bucket];
				op_state.cursor.have_build_cursor = true;
				op_state.cursor.probe_matched = false;
				op_state.cursor.probe_key_null = false;
				if (!partitioned_build)
					op_state.cursor.partition_id = UINT32_MAX;
				while (op_state.cursor.build_row_idx != HASH_JOIN_INVALID_ROW &&
					!candidate_matches(op_state.cursor.build_row_idx, probe_row_idx))
					op_state.cursor.build_row_idx = links[op_state.cursor.build_row_idx];
			}
			while (op_state.cursor.build_row_idx != HASH_JOIN_INVALID_ROW)
			{
				const uint32_t build_row_idx = op_state.cursor.build_row_idx;
				{
					PipelineProfileScope lookup_scope(ctx,
						PipelineProfileStage::HASH_JOIN_LOOKUP);
					op_state.cursor.build_row_idx = links[build_row_idx];
					while (op_state.cursor.build_row_idx != HASH_JOIN_INVALID_ROW &&
						!candidate_matches(op_state.cursor.build_row_idx, probe_row_idx))
						op_state.cursor.build_row_idx = links[op_state.cursor.build_row_idx];
				}
				bool key_match = scalar_key_values != nullptr && !mark_mode;
				if (!key_match)
				{
					TupleDataCollection *key_store = build_keys;
					const uint8_t *build_key_row = nullptr;
					if (batch != nullptr)
					{
						const auto *refs = static_cast<const HashJoinBatchRowRef *>(dsa_get_address(ctx.dsa, batch->key_refs_dp));
						build_key_row = ResolveBatchRow(ctx.dsa, batch, refs, build_row_idx, &key_store);
					}
					else if (payload->segmented_build)
					{
						PipelineProfileScope segment_read_scope(ctx,
							PipelineProfileStage::HASH_JOIN_SEGMENT_READ);
						build_key_row = payload->direct_segmented_build && partitioned_build
							? AcquireSegmentedHashJoinPartitionRow(ctx, payload,
								selected_partition, selected_partition_id, true,
								build_row_idx, &window, &key_store, true)
							: AcquireSegmentedHashJoinRowFromRefs(ctx, payload,
								selected_key_refs_dp, build_row_idx, &window, true, &key_store, true);
					}
					else
						build_key_row = TupleDataCollectionGetRowConst(build_keys, build_row_idx);
					if (build_key_row == nullptr)
						ereport(ERROR, (errmsg("pg_yaap: hash join segmented key row unavailable")));
					key_match = !(mark_mode && TupleDataLayoutColumnIsNull(build_key_layout,
						build_key_row, 0)) && MatchGroupLayouts(build_key_layout, key_store,
						build_key_row, probe_layout, in, probe_row_idx);
				}
				if (key_match)
				{
					const TupleDataCollection *build_row_tdc = nullptr;
					TupleDataCollection *resolved_row_store = nullptr;
					const uint8_t *build_row = nullptr;
					if (needs_build_row)
					{
						build_row_tdc = build_rows;
						if (batch != nullptr)
						{
							const auto *refs = static_cast<const HashJoinBatchRowRef *>(dsa_get_address(ctx.dsa, batch->row_refs_dp));
							build_row = ResolveBatchRow(ctx.dsa, batch, refs, build_row_idx, &resolved_row_store);
						}
						else if (shared_local_payload)
							build_row = ResolveSharedLocalBuildRowCached(ctx.dsa,
								payload,
								shared_local_registry,
								shared_local_slots,
								shared_local_idxs,
								build_row_idx,
								&build_row_tdc);
						else if (payload->segmented_build)
						{
							PipelineProfileScope segment_read_scope(ctx,
								PipelineProfileStage::HASH_JOIN_SEGMENT_READ);
							build_row = payload->direct_segmented_build && partitioned_build
								? AcquireSegmentedHashJoinPartitionRow(ctx, payload,
									selected_partition, selected_partition_id, false,
									build_row_idx, &window, &resolved_row_store, true)
								: AcquireSegmentedHashJoinRowFromRefs(ctx, payload,
									selected_row_refs_dp, build_row_idx,
									&window, false, &resolved_row_store, true);
						}
						else
							build_row = TupleDataCollectionGetRowConst(build_rows, build_row_idx);
					}
					if (payload->segmented_build && resolved_row_store != nullptr)
						build_row_tdc = resolved_row_store;
					if (needs_build_row && build_row == nullptr)
						ereport(ERROR, (errmsg("pg_yaap: hash join segmented payload row unavailable")));
					if (has_join_filter)
					{
						if (has_simple_join_filter)
						{
							if (!EvaluateSimpleJoinFilter(simple_join_filter,
									in,
									probe_row_idx,
									build_row))
							{
								continue;
							}
						}
						else if (!EvaluateJoinFilter(filter_inputs,
								 n_filter_inputs,
								 filter_exprs,
								 n_filter_exprs,
								 filter_steps,
								 n_filter_steps,
								 filter_string_consts,
								 required_bool_regs,
								 in,
								 probe_row_idx,
								 build_row_layout,
								 build_row_tdc,
								 build_row,
								 *filter_chunk_holder,
								 filter_bool_values))
						{
							continue;
						}
					}
					if (join_mode == HashJoinMatchMode::ANTI)
					{
						op_state.cursor.probe_matched = true;
						batch_count = 0;
						op_state.cursor.build_row_idx = HASH_JOIN_INVALID_ROW;
						break;
					}
					if (mark_mode)
					{
						op_state.cursor.probe_matched = true;
						op_state.cursor.build_row_idx = HASH_JOIN_INVALID_ROW;
						break;
					}
					if (HashJoinTracksBuildMatches(join_mode))
					{
						auto *matched = static_cast<pg_atomic_uint32 *>(
							dsa_get_address(ctx.dsa, batch->matched_bitmap_dp));
						const uint32_t bit = UINT32_C(1) << (build_row_idx & 31u);
						const uint32_t old = pg_atomic_fetch_or_u32(&matched[build_row_idx >> 5], bit);
						if ((old & bit) == 0)
							pg_atomic_fetch_add_u32(&batch->matched_build_rows, 1);
					}
					op_state.cursor.probe_matched = true;
					++matched_rows;
					if (join_mode == HashJoinMatchMode::BUILD_SEMI ||
						join_mode == HashJoinMatchMode::BUILD_ANTI)
						continue;
					if (payload->segmented_build)
					{
						uint16_t probe_row = probe_row_idx;
						if (join_mode != HashJoinMatchMode::ANTI)
							CopyRowsByResolvedMappingBatch(in, &probe_row, &build_row_tdc,
								&build_row, op_state.fast_probe.resolved_output_columns.data(),
								op_state.fast_probe.output_column_count, out, out.count, 1);
						out.count++;
						if (join_mode == HashJoinMatchMode::SEMI)
							op_state.cursor.build_row_idx = HASH_JOIN_INVALID_ROW;
						if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
							break;
						continue;
					}
					matched_probe_rows[batch_count] = probe_row_idx;
					matched_build_rows[batch_count] = build_row;
					matched_build_row_tdcs[batch_count] = build_row_tdc;
					++batch_count;
					if (join_mode == HashJoinMatchMode::SEMI)
					{
						op_state.cursor.build_row_idx = HASH_JOIN_INVALID_ROW;
						break;
					}
					if (out.count + batch_count >= PIPELINE_DEFAULT_CHUNK_SIZE)
						break;
				}
			}
			const bool probe_complete = (op_state.cursor.build_row_idx == HASH_JOIN_INVALID_ROW);
			if (mark_mode && probe_complete)
			{
				matched_probe_rows[0] = probe_row_idx;
				matched_build_rows[0] = nullptr;
				matched_build_row_tdcs[0] = nullptr;
				const uint32_t build_rows_total = payload->segmented_build
					? payload->segmented_row_count : payload->combined_row_count;
				if (op_state.cursor.probe_matched)
					marker_states[0] = 1;
				else if (build_rows_total == 0)
					marker_states[0] = 0;
				else if (op_state.cursor.probe_key_null ||
					pg_atomic_read_u32(&payload->build_has_null_key) != 0)
					marker_states[0] = 2;
				else
					marker_states[0] = 0;
				batch_count = 1;
				if (mark_filter_not && marker_states[0] != 0)
					batch_count = 0;
			}
			if ((join_mode == HashJoinMatchMode::ANTI || join_mode == HashJoinMatchMode::LEFT) &&
				probe_complete && batch_count == 0 && !op_state.cursor.probe_matched)
			{
				matched_probe_rows[0] = probe_row_idx;
				matched_build_rows[0] = nullptr;
				matched_build_row_tdcs[0] = nullptr;
				batch_count = 1;
			}
			if (batch_count > 0)
			{
				CopyRowsByResolvedMappingBatch(in,
					matched_probe_rows,
					matched_build_row_tdcs,
					matched_build_rows,
					op_state.fast_probe.resolved_output_columns.data(),
					op_state.fast_probe.output_column_count,
					out,
					out.count,
					batch_count);
				out.count += batch_count;
				if (join_mode == HashJoinMatchMode::MARK)
				{
					for (uint16_t col_idx = 0; col_idx < output_column_count; ++col_idx)
					{
						if (output_columns[col_idx].side != HashJoinOutputSide::MARK)
							continue;
						const uint8_t slot = output_columns[col_idx].output_chunk_slot;
						for (uint16_t row = 0; row < batch_count; ++row)
						{
							const uint16_t out_row = static_cast<uint16_t>(out.count - batch_count + row);
							out.int32_columns[slot][out_row] = marker_states[row] == 1 ? 1 : 0;
							out.nulls[slot][out_row] = marker_states[row] == 2 ? 1 : 0;
						}
					}
				}
			}
			if (probe_complete)
			{
				matched_rows = 0;
				op_state.cursor.probe_matched = false;
				op_state.cursor.have_build_cursor = false;
				++op_state.cursor.probe_row_idx;
				if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
					break;
				continue;
			}
			if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
				break;
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(ReleaseHashJoinReadWindow, PointerGetDatum(&window));
	window.Release();
	if (op_state.cursor.probe_row_idx >= in.count)
	{
		op_state.cursor.active_probe = false;
		op_state.cursor.current_input_drained = out.count > 0;
		op_state.cursor.build_row_idx = HASH_JOIN_INVALID_ROW;
		op_state.cursor.have_build_cursor = false;
		op_state.cursor.probe_salt = 0;
		op_state.cursor.probe_matched = false;
		op_state.cursor.probe_key_null = false;
	}
	if (out.count > 0 && pg_yaap_trace_execution_path)
		ereport(LOG,
			(errmsg("pg_yaap hashjoin execute worker=%d probe_rows=%u out_rows=%u matched_rows=%u build_rows=%u key_width=%u payload_width=%u",
				ctx.worker_index,
				in.count,
				out.count,
				matched_rows,
				payload->segmented_build ? payload->segmented_row_count :
					pg_atomic_read_u32(&build_keys->row_count),
				build_key_layout->row_width,
				build_row_layout->row_width)));
	op_state.initialized = true;

	return out.count > 0 ? OperatorResultType::HAVE_MORE_OUTPUT : OperatorResultType::NEED_MORE_INPUT;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
