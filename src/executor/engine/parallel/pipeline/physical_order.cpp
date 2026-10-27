#include "parallel/pipeline/physical_order.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "utils/elog.h"
#include "utils/tuplesort.h"
#include "parser/parse_oper.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "executor/tuptable.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_collation_d.h"
}

#include <algorithm>
#include <cstring>
#include <vector>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/cancel.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"
#include "parallel/pipeline/types.hpp"
#include "buffer_manager/yaap_shared_segment_arena.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

static constexpr bool kEnableOrderSegmentBacking = true;

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

static void
SyncOrderSegmentUsed(ExecCtx &ctx, OrderSharedPayload *payload)
{
	if (payload == nullptr || !payload->segment_backed)
		return;
	const buffer::YaapSegmentHandle handle{1, payload->segment_id,
		payload->segment_generation};
	auto *segment = buffer::YaapSharedSegmentResolve(ctx.dsa,
		payload->segment_arena_dp, handle);
	if (segment == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: order segment missing while syncing used bytes")));
	auto *tdc = ResolveTdc(ctx.dsa, payload->tdc_dp);
	if (tdc == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: order TDC missing while syncing segment")));
	const uint64_t used = offsetof(TupleDataCollection, rows) +
		static_cast<uint64_t>(pg_atomic_read_u32(&tdc->row_count)) * tdc->row_width +
		pg_atomic_read_u32(&tdc->heap_used);
	if (used > segment->capacity)
		ereport(ERROR, (errmsg("pg_yaap: order segment used bytes exceed capacity")));
	segment->used = static_cast<uint32_t>(used);
}

static void
EnsureOrderSegmentResident(ExecCtx &ctx, dsa_pointer arena_dp,
							   buffer::YaapSharedSegmentDesc *segment)
{
	if (segment == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: order segment descriptor missing")));
	if (pg_atomic_read_u32(&segment->state) ==
		static_cast<uint32_t>(buffer::YaapSharedSegmentState::SPILLED))
	{
		if (ctx.control == nullptr)
			ereport(ERROR, (errmsg("pg_yaap: order spill control missing")));
		const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id,
			segment->generation};
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
					ereport(ERROR, (errmsg("pg_yaap: order segment reload entered invalid state")));
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
		ereport(ERROR, (errmsg("pg_yaap: order segment is not readable")));
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
			elog(ERROR, "pg_yaap: order TDC grow ran out of heap");
		std::memcpy(dst_row + col.offset, &dst_ref, sizeof(dst_ref));
	}

	for (uint16_t agg_idx = 0; agg_idx < layout->aggregate_count; ++agg_idx)
	{
		const TdcAggregateDesc &agg = layout->aggregates[agg_idx];
		const uint16_t width = agg.kind == TdcAggKind::AVG_NUMERIC ? 16 : 8;
		std::memcpy(dst_row + agg.offset, src_row + agg.offset, width);
	}
	if (layout->validity_width > 0)
		std::memcpy(dst_row + TupleDataLayoutValidityOffset(layout),
			src_row + TupleDataLayoutValidityOffset(layout), layout->validity_width);
}

static dsa_pointer
GrowOrderTdc(ExecCtx &ctx, OrderGlobalState &global, uint32_t required_heap_bytes)
{
	if (global.shared_payload == nullptr)
		elog(ERROR, "pg_yaap: order shared payload wrapper missing during grow");
	dsa_pointer old_tdc_dp = global.shared_payload->tdc_dp;
	const bool old_segment_backed = global.shared_payload->segment_backed;
	TupleDataCollection *old_tdc = global.payload;
	if (old_tdc == nullptr || global.payload_layout == nullptr)
		elog(ERROR, "pg_yaap: order TDC missing during grow");
	const uint32_t old_count = pg_atomic_read_u32(&old_tdc->row_count);
	uint32_t new_capacity = std::max(old_tdc->row_capacity * 2u, old_count + 1u);
	new_capacity = TupleDataCollectionClampRowCapacity(new_capacity,
		old_tdc->row_width,
		required_heap_bytes,
		old_count + 1u);
	const uint32_t heap_capacity = TupleDataCollectionGrowHeapCapacity(global.payload_layout,
		old_tdc,
		new_capacity,
		required_heap_bytes);
	dsa_pointer new_tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
		new_capacity,
		old_tdc->row_width,
		heap_capacity);
	TupleDataCollection *new_tdc = ResolveTdc(ctx.dsa, new_tdc_dp);
	TupleDataCollectionInit(new_tdc,
		new_capacity,
		old_tdc->row_width,
		global.payload_layout_dp,
		heap_capacity);
	for (uint32_t row_idx = 0; row_idx < old_count; ++row_idx)
	{
		uint8_t *dst = nullptr;
		const uint32_t copied_idx = TupleDataCollectionAppendRow(new_tdc, &dst);
		if (copied_idx == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: order TDC grow copy overflow");
		CopyTdcRow(global.payload_layout,
			old_tdc,
			TupleDataCollectionGetRowConst(old_tdc, row_idx),
			new_tdc,
			dst);
	}
	global.shared_payload->tdc_dp = new_tdc_dp;
	if (old_segment_backed)
	{
		auto *state = ResolveBufferState(ctx);
		buffer::YaapSegmentHandle handle{1, global.shared_payload->segment_id,
			global.shared_payload->segment_generation};
		auto *segment = buffer::YaapSharedSegmentResolve(ctx.dsa,
			global.shared_payload->segment_arena_dp, handle);
		if (segment != nullptr && pg_atomic_read_u32(&segment->state) ==
			static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL))
		{
			buffer::YaapSharedSegmentSeal(segment);
			buffer::YaapSharedSegmentPublish(segment);
		}
		if (state == nullptr || !buffer::YaapSharedSegmentRetire(ctx.dsa, state,
				global.shared_payload->segment_arena_dp, handle))
			ereport(ERROR, (errmsg("pg_yaap: order segment retire during grow failed")));
		global.shared_payload->segment_backed = false;
	}
	global.payload = new_tdc;
	global.max_rows = new_capacity;
	if (DsaPointerIsValid(old_tdc_dp) && !old_segment_backed)
		dsa_free(ctx.dsa, old_tdc_dp);
	return global.shared_payload_dp;
}

}  /* namespace */

struct TdcExternalSorter::State {
 ExecCtx *ctx;
 const TupleDataLayout *layout;
 const TupleDataLayout *key_layout;
 const SortKeyDesc *keys;
 uint16_t nkeys;
 bool sequential;
 Tuplesortstate *sort;
 TupleDesc desc;
 TupleTableSlot *slot;
 MemoryContext row_context;
 buffer::YaapBufferSharedState *budget;
 uint64_t reservation;
};

TdcExternalSorter::TdcExternalSorter(ExecCtx &ctx, const TupleDataLayout *payload_layout,
 const TupleDataLayout *key_layout, const SortKeyDesc *keys, uint16_t nkeys,
 bool sequential_keys)
{
 state_ = static_cast<State *>(MemoryContextAllocZero(ctx.mcxt, sizeof(State)));
 State &s = *state_;
 s.ctx = &ctx; s.layout = payload_layout; s.key_layout = key_layout;
 s.keys = keys; s.nkeys = nkeys; s.sequential = sequential_keys;
 s.budget = ResolveBufferState(ctx);
 /* Reserve a bounded share, independent of the configured maximum. PG's
  * tuplesort owns run generation and bounded-fan-in merging. */
 s.reservation = s.budget != nullptr
   ? std::min<uint64_t>(16u * 1024u * 1024u, std::max<uint64_t>(64u * 1024u, s.budget->soft_limit / 8u))
   : 4u * 1024u * 1024u;
 if (s.budget != nullptr)
  buffer::YaapBufferChargeExternalBytes(s.budget, s.reservation, "external sort workspace");
 s.desc = CreateTemplateTupleDesc(nkeys + 1);
 AttrNumber attrs[TUPLE_DATA_MAX_COLUMNS];
 Oid operators[TUPLE_DATA_MAX_COLUMNS], collations[TUPLE_DATA_MAX_COLUMNS];
 bool nulls_first[TUPLE_DATA_MAX_COLUMNS];
 for (uint16_t k = 0; k < nkeys; ++k) {
  const auto &col = key_layout->columns[sequential_keys ? k : keys[k].col_idx];
  Oid type = col.kind == TdcColumnKind::INT32 ? INT4OID :
    col.kind == TdcColumnKind::INT64 ? INT8OID :
    col.kind == TdcColumnKind::DOUBLE ? FLOAT8OID : TEXTOID;
  TupleDescInitEntry(s.desc, k + 1, nullptr, type, -1, 0);
  attrs[k] = k + 1;
  Oid lt, gt;
  get_sort_group_operators(type, true, false, true, &lt, nullptr, &gt, nullptr);
  operators[k] = keys == nullptr || keys[k].asc ? lt : gt;
  collations[k] = type == TEXTOID ? C_COLLATION_OID : InvalidOid;
  nulls_first[k] = keys != nullptr && keys[k].nulls_first;
 }
 TupleDescInitEntry(s.desc, nkeys + 1, nullptr, BYTEAOID, -1, 0);
 s.slot = MakeSingleTupleTableSlot(s.desc, &TTSOpsMinimalTuple);
 s.sort = tuplesort_begin_heap(s.desc, nkeys, attrs, operators, collations,
  nulls_first, static_cast<int>(s.reservation / 1024u), nullptr, TUPLESORT_NONE);
 s.row_context = AllocSetContextCreate(ctx.mcxt, "YAAP sort row", ALLOCSET_SMALL_SIZES);
}

TdcExternalSorter::~TdcExternalSorter()
{
 State &s = *state_;
 tuplesort_end(s.sort);
 ExecDropSingleTupleTableSlot(s.slot);
 FreeTupleDesc(s.desc);
 MemoryContextDelete(s.row_context);
 if (s.budget != nullptr)
  buffer::YaapBufferReleaseExternalBytes(s.budget, s.reservation);
 pfree(state_);
}

void TdcExternalSorter::Put(const TupleDataCollection *tdc, const uint8_t *row)
{
 State &s = *state_;
 MemoryContextReset(s.row_context);
 MemoryContext old = MemoryContextSwitchTo(s.row_context);
 ExecClearTuple(s.slot);
 for (uint16_t k = 0; k < s.nkeys; ++k) {
  const uint16_t key_col = s.sequential ? k : s.keys[k].col_idx;
  const auto &col = s.key_layout->columns[key_col];
  s.slot->tts_isnull[k] = TupleDataLayoutColumnIsNull(s.key_layout, row, key_col);
  if (s.slot->tts_isnull[k]) {
   s.slot->tts_values[k] = (Datum) 0;
   continue;
  }
  const uint8_t *value = row + col.offset;
  switch (col.kind) {
   case TdcColumnKind::INT32: { int32_t v; memcpy(&v, value, sizeof(v)); s.slot->tts_values[k] = Int32GetDatum(v); break; }
   case TdcColumnKind::INT64: { int64_t v; memcpy(&v, value, sizeof(v)); s.slot->tts_values[k] = Int64GetDatum(v); break; }
   case TdcColumnKind::DOUBLE: { double v; memcpy(&v, value, sizeof(v)); s.slot->tts_values[k] = Float8GetDatum(v); break; }
   case TdcColumnKind::STRING_REF: {
    VecStringRef ref; memcpy(&ref, value, sizeof(ref));
    const char *ptr = VecStringRefDataPtr(ref, reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc)));
    s.slot->tts_values[k] = PointerGetDatum(cstring_to_text_with_len(ptr != nullptr ? ptr : "", ref.len)); break;
   }
  }
 }
 const uint32_t heap = TupleDataCollectionRequiredHeapBytesForRow(s.layout, tdc, row);
 const size_t bytes = TupleDataCollectionAllocSize(1, s.layout->row_width, heap);
 auto *image = static_cast<bytea *>(palloc0(VARHDRSZ + bytes));
 SET_VARSIZE(image, VARHDRSZ + bytes);
 auto *packed = static_cast<TupleDataCollection *>(palloc0(bytes));
 TupleDataCollectionInit(packed, 1, s.layout->row_width, InvalidDsaPointer, heap);
 uint8_t *dst;
 TupleDataCollectionAppendRow(packed, &dst);
 CopyTdcRow(s.layout, tdc, row, packed, dst);
 memcpy(VARDATA(image), packed, bytes);
 s.slot->tts_values[s.nkeys] = PointerGetDatum(image);
 s.slot->tts_isnull[s.nkeys] = false;
 ExecStoreVirtualTuple(s.slot);
 tuplesort_puttupleslot(s.sort, s.slot);
 ExecClearTuple(s.slot);
 MemoryContextSwitchTo(old);
 MemoryContextReset(s.row_context);
}

void TdcExternalSorter::Perform() { tuplesort_performsort(state_->sort); }

const TupleDataCollection *TdcExternalSorter::Next()
{
 State &s = *state_;
 if (!tuplesort_gettupleslot(s.sort, true, false, s.slot, nullptr)) return nullptr;
 bool isnull;
 Datum value = slot_getattr(s.slot, s.nkeys + 1, &isnull);
 auto *image = DatumGetByteaPP(value);
 MemoryContextReset(s.row_context);
 auto *packed = static_cast<TupleDataCollection *>(MemoryContextAlloc(s.row_context, VARSIZE_ANY_EXHDR(image)));
 memcpy(packed, VARDATA_ANY(image), VARSIZE_ANY_EXHDR(image));
 return packed;
}

void SpillCompletedTdcSegments(ExecCtx &ctx, dsa_pointer arena_dp,
 dsa_pointer first_dp, const char *prefix)
{
 auto *budget = ResolveBufferState(ctx);
 auto *arena = buffer::YaapSharedSegmentArenaResolve(ctx.dsa, arena_dp);
 if (budget == nullptr || arena == nullptr || ctx.control == nullptr) return;
 const uint64_t target = budget->soft_limit > arena->segment_size
   ? budget->soft_limit - arena->segment_size : 0;
 if (buffer::YaapBufferResidentBytes(budget) <= target) return;
 buffer::YaapSpillStore spill(ctx.mcxt);
 static uint64_t spill_sequence = 0;
 char file_name[64];
 snprintf(file_name, sizeof(file_name), "%.24s_%d_%llu", prefix, MyProcPid,
   static_cast<unsigned long long>(spill_sequence++));
 spill.AttachSharedFileSet(&ctx.control->spill_fileset, file_name);
 for (dsa_pointer dp = first_dp; DsaPointerIsValid(dp);) {
  auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, dp));
  dp = segment->chain_next_dp;
  if (pg_atomic_read_u32(&segment->state) != static_cast<uint32_t>(buffer::YaapSharedSegmentState::PUBLISHED)) continue;
  buffer::YaapSpillExtent extent{};
  if (!buffer::YaapSharedSegmentSpill(ctx.dsa, budget,
    {segment->arena_id, segment->segment_id, segment->generation}, arena_dp, &spill, &extent))
   ereport(ERROR, (errmsg("pg_yaap: completed sort/output segment spill failed")));
  if (buffer::YaapBufferResidentBytes(budget) <= target) break;
 }
}

std::unique_ptr<GlobalSinkState>
PhysicalOrder::GetGlobalSinkState(ExecCtx &ctx)
{
	auto state = std::make_unique<OrderGlobalState>();
	state->dsa = ctx.dsa;
	state->desc = desc_;
	state->sort_keys_dp = sort_keys_dp_;
	state->n_sort_keys = n_sort_keys_;
	state->key_layout_dp = key_layout_dp_;
	state->payload_layout_dp = payload_layout_dp_;
	state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this);
	if (!DsaPointerIsValid(state->shared_payload_dp))
		state->shared_payload_dp = shared_payload_dp_;
	state->max_rows = desc_ != nullptr ? desc_->body.order.max_rows : 256;

	if (!DsaPointerIsValid(state->sort_keys_dp) && desc_ != nullptr)
	{
		state->sort_keys_dp = desc_->body.order.sort_keys;
		state->n_sort_keys = desc_->body.order.n_sort_keys;
	}
	if (!DsaPointerIsValid(state->key_layout_dp) && desc_ != nullptr)
		state->key_layout_dp = desc_->body.order.key_layout;
	if (!DsaPointerIsValid(state->payload_layout_dp) && desc_ != nullptr)
		state->payload_layout_dp = desc_->body.order.payload_layout;
	if (DsaPointerIsValid(state->sort_keys_dp))
		state->sort_keys = static_cast<const SortKeyDesc *>(dsa_get_address(ctx.dsa, state->sort_keys_dp));
	if (DsaPointerIsValid(state->key_layout_dp))
		state->key_layout = static_cast<const TupleDataLayout *>(dsa_get_address(ctx.dsa, state->key_layout_dp));
	if (DsaPointerIsValid(state->payload_layout_dp))
		state->payload_layout = static_cast<const TupleDataLayout *>(dsa_get_address(ctx.dsa, state->payload_layout_dp));

	if (!DsaPointerIsValid(state->shared_payload_dp))
		state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this);

	if (ctx.worker_index == LEADER_WORKER_INDEX && !DsaPointerIsValid(state->shared_payload_dp))
	{
		state->shared_payload_dp = dsa_allocate0(ctx.dsa, sizeof(OrderSharedPayload));
		state->shared_payload = static_cast<OrderSharedPayload *>(
			dsa_get_address(ctx.dsa, state->shared_payload_dp));
		const uint32_t row_width = state->payload_layout != nullptr ? state->payload_layout->row_width : 8;
		uint32_t initial_rows = std::max<uint32_t>(1, std::min<uint32_t>(state->max_rows, 16384u));
		uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->payload_layout,
			initial_rows);
		uint64_t required = offsetof(TupleDataCollection, rows) +
			static_cast<uint64_t>(initial_rows) * row_width + heap_capacity;
		auto *buffer_state = ResolveBufferState(ctx);
		auto *arena = ctx.control != nullptr ? buffer::YaapSharedSegmentArenaResolve(
			ctx.dsa, ctx.control->shared_segment_arena_root) : nullptr;
        while (arena != nullptr && required > arena->segment_size && initial_rows > 1) {
            initial_rows = std::max<uint32_t>(1, initial_rows / 2);
            heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->payload_layout, initial_rows);
            required = offsetof(TupleDataCollection, rows) +
                static_cast<uint64_t>(initial_rows) * row_width + heap_capacity;
        }
		if (kEnableOrderSegmentBacking && buffer_state != nullptr && arena != nullptr && required <= arena->segment_size )
		{
			const auto handle = buffer::YaapSharedSegmentArenaAllocate(ctx.dsa, buffer_state,
				ctx.control->shared_segment_arena_root, static_cast<uint32_t>(std::max(ctx.worker_index, 0)));
			auto *segment = buffer::YaapSharedSegmentResolve(ctx.dsa,
				ctx.control->shared_segment_arena_root, handle);
			state->shared_payload->tdc_dp = segment->payload_dp;
			state->shared_payload->segment_arena_dp = ctx.control->shared_segment_arena_root;
			state->shared_payload->segment_first_desc_dp = segment->self_dp;
			state->shared_payload->segment_id = handle.segment_id;
			state->shared_payload->segment_generation = handle.generation;
			state->shared_payload->segment_row_count = 0;
			state->shared_payload->segment_backed = true;
		}
		else
		{
			const uint32_t flat_heap_capacity = TupleDataCollectionDefaultHeapCapacity(
				state->payload_layout, state->max_rows);
			state->shared_payload->tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
				state->max_rows, row_width, flat_heap_capacity);
			heap_capacity = flat_heap_capacity;
		}
		state->payload = ResolveTdc(ctx.dsa, state->shared_payload->tdc_dp);
		TupleDataCollectionInit(state->payload,
			state->shared_payload->segment_backed ? initial_rows : state->max_rows,
			row_width,
			state->payload_layout_dp,
			heap_capacity);
		if (state->shared_payload->segment_backed)
		{
			auto *segment = buffer::YaapSharedSegmentResolve(ctx.dsa,
				state->shared_payload->segment_arena_dp,
				buffer::YaapSegmentHandle{1, state->shared_payload->segment_id,
					state->shared_payload->segment_generation});
			state->segmented_writer_initialized = segment != nullptr;
			if (segment == nullptr)
				ereport(ERROR, (errmsg("pg_yaap: order segment initialization failed")));
			buffer::YaapSegmentedTdcInit(&state->segmented_writer,
				state->shared_payload->segment_arena_dp, state->payload_layout_dp,
				static_cast<uint32_t>(std::max(ctx.worker_index, 0)), row_width,
					initial_rows, heap_capacity, 0, "order writer");
			state->segmented_writer.first = {1, state->shared_payload->segment_id,
				state->shared_payload->segment_generation};
			state->segmented_writer.active = state->segmented_writer.first;
			state->segmented_writer.segment_count = 1;
		}
		StoreSharedPayloadOnDescriptor(this, state->shared_payload_dp);
	}
	else if (DsaPointerIsValid(state->shared_payload_dp) ||
			 (state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this), DsaPointerIsValid(state->shared_payload_dp)))
	{
		state->shared_payload = static_cast<OrderSharedPayload *>(
			dsa_get_address(ctx.dsa, state->shared_payload_dp));
		state->payload = ResolveTdc(ctx.dsa, state->shared_payload->tdc_dp);
	}
	else
	{
	}

	return state;
}

std::unique_ptr<LocalSinkState>
PhysicalOrder::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate)
{
	(void) ctx;
	(void) gstate;
	return std::make_unique<OrderLocalState>();
}

SinkResultType
PhysicalOrder::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input)
{
	auto &global = static_cast<OrderGlobalState &>(input.global_state);
	(void) input.local_state;

	if (global.payload == nullptr || global.payload_layout == nullptr)
		elog(ERROR, "pg_yaap: order sink payload not initialized");
	if (global.shared_payload != nullptr && global.shared_payload->segment_backed)
	{
		if (!global.segmented_writer_initialized)
			elog(ERROR, "pg_yaap: order segmented writer not initialized");
		auto *buffer_state = ResolveBufferState(ctx);
		for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
		{
			if (PipelineCancelRequestedEvery(ctx, row_idx))
				return SinkResultType::FINISHED;
            if (!TupleDataCollectionHasSpaceForAppend(global.payload,
                TupleDataCollectionRequiredHeapBytesForChunkRow(global.payload_layout, in, row_idx))) {
                buffer::YaapSegmentedTdcSealPublish(ctx.dsa, &global.segmented_writer);
                SpillCompletedTdcSegments(ctx, global.shared_payload->segment_arena_dp,
                    global.shared_payload->segment_first_desc_dp, "pg_yaap_order");
            }
			buffer::YaapSegmentedRowHandle handle;
			TupleDataCollection *store = nullptr;
			uint8_t *row = nullptr;
			if (!buffer::YaapSegmentedTdcAppendWithHeap(ctx.dsa, buffer_state,
				&global.segmented_writer,
				TupleDataCollectionRequiredHeapBytesForChunkRow(global.payload_layout, in, row_idx),
				&handle, &store, &row))
				ereport(ERROR, (errmsg("pg_yaap: order segmented row append failed")));
			Scatter(store != nullptr ? global.payload_layout : global.payload_layout,
				store, row, in, row_idx);
			global.payload = store;
			++global.shared_payload->segment_row_count;
		}
		auto *tail = buffer::YaapSharedSegmentResolve(ctx.dsa,
			global.shared_payload->segment_arena_dp, global.segmented_writer.active);
		if (tail == nullptr)
			ereport(ERROR, (errmsg("pg_yaap: order segmented tail missing")));
		global.shared_payload->segment_id = tail->segment_id;
		global.shared_payload->segment_generation = tail->generation;
		global.shared_payload->tdc_dp = tail->payload_dp;
		global.payload = ResolveTdc(ctx.dsa, global.shared_payload->tdc_dp);
		return SinkResultType::NEED_MORE_INPUT;
	}

	for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
	{
		if (PipelineCancelRequestedEvery(ctx, row_idx))
			return SinkResultType::FINISHED;
		while (!TupleDataCollectionHasSpaceForAppend(global.payload,
			TupleDataCollectionRequiredHeapBytesForChunkRow(global.payload_layout, in, row_idx)))
		{
			GrowOrderTdc(ctx,
				global,
				TupleDataCollectionRequiredHeapBytesForChunkRow(global.payload_layout, in, row_idx));
			StoreSharedPayloadOnDescriptor(this, global.shared_payload_dp);
		}
		uint8_t *row_ptr = nullptr;
		const uint32_t appended = TupleDataCollectionAppendRow(global.payload, &row_ptr);
		if (appended == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: order sink row capacity exceeded");
		Scatter(global.payload_layout, global.payload, row_ptr, in, row_idx);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
PhysicalOrder::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input)
{
	(void) ctx;
	(void) input;
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType
PhysicalOrder::Finalize(ExecCtx &ctx, GlobalSinkState &gstate)
{
 auto &global = static_cast<OrderGlobalState &>(gstate);
 if (global.shared_payload == nullptr) return SinkFinalizeType::READY;
 if (global.shared_payload->segment_backed) {
  // FINALIZE may run in a backend that did not execute RUN; the shared tail
  // is authoritative, not its local writer snapshot.
  auto *segment = buffer::YaapSharedSegmentResolve(ctx.dsa,
    global.shared_payload->segment_arena_dp,
    {1, global.shared_payload->segment_id, global.shared_payload->segment_generation});
  SyncOrderSegmentUsed(ctx, global.shared_payload);
  if (segment != nullptr && pg_atomic_read_u32(&segment->state) ==
    static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL)) {
   buffer::YaapSharedSegmentSeal(segment);
   buffer::YaapSharedSegmentPublish(segment);
  }
  SpillCompletedTdcSegments(ctx, global.shared_payload->segment_arena_dp,
    global.shared_payload->segment_first_desc_dp, "pg_yaap_order");
 } else if (global.payload != nullptr) global.payload->finalized = true;
 global.shared_payload->finalized = true;
 return SinkFinalizeType::READY;
}

std::unique_ptr<GlobalSourceState>
PhysicalOrder::GetGlobalSourceState(ExecCtx &ctx)
{
	auto state = std::make_unique<OrderGlobalState>();
	state->dsa = ctx.dsa;
	state->desc = desc_;
	state->sort_keys_dp = sort_keys_dp_;
	state->n_sort_keys = n_sort_keys_;
	state->key_layout_dp = key_layout_dp_;
	state->payload_layout_dp = payload_layout_dp_;
	state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this);
	if (!DsaPointerIsValid(state->shared_payload_dp))
		state->shared_payload_dp = shared_payload_dp_;

	if (!DsaPointerIsValid(state->sort_keys_dp) && desc_ != nullptr)
	{
		state->sort_keys_dp = desc_->body.order.sort_keys;
		state->n_sort_keys = desc_->body.order.n_sort_keys;
	}
	if (!DsaPointerIsValid(state->key_layout_dp) && desc_ != nullptr)
		state->key_layout_dp = desc_->body.order.key_layout;
	if (!DsaPointerIsValid(state->payload_layout_dp) && desc_ != nullptr)
		state->payload_layout_dp = desc_->body.order.payload_layout;
	if (DsaPointerIsValid(state->sort_keys_dp))
		state->sort_keys = static_cast<const SortKeyDesc *>(dsa_get_address(ctx.dsa, state->sort_keys_dp));
	if (DsaPointerIsValid(state->key_layout_dp))
		state->key_layout = static_cast<const TupleDataLayout *>(dsa_get_address(ctx.dsa, state->key_layout_dp));
	if (DsaPointerIsValid(state->payload_layout_dp))
		state->payload_layout = static_cast<const TupleDataLayout *>(dsa_get_address(ctx.dsa, state->payload_layout_dp));
	if (DsaPointerIsValid(state->shared_payload_dp))
	{
		state->shared_payload = static_cast<OrderSharedPayload *>(
			dsa_get_address(ctx.dsa, state->shared_payload_dp));
		state->payload = ResolveTdc(ctx.dsa, state->shared_payload->tdc_dp);
	}
	if (desc_ != nullptr)
		state->sort_indices_dp = desc_->body.order.sort_indices;
	return state;
}

std::unique_ptr<LocalSourceState>
PhysicalOrder::GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate)
{
	(void) ctx;
	(void) gstate;
	return std::make_unique<OrderLocalState>();
}

SourceResultType
PhysicalOrder::GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input)
{
 auto &global = static_cast<OrderGlobalState &>(input.global_state);
 auto &local = static_cast<OrderLocalState &>(input.local_state);
 out.reset();
 if (global.shared_payload == nullptr || !global.shared_payload->finalized)
  return SourceResultType::FINISHED;
 if (!local.sorter) {
  local.sorter = std::make_unique<TdcExternalSorter>(ctx, global.payload_layout,
    global.key_layout, global.sort_keys, global.key_layout->column_count, true);
  if (global.shared_payload->segment_backed) {
   auto *budget = ResolveBufferState(ctx);
   for (dsa_pointer dp = global.shared_payload->segment_first_desc_dp; DsaPointerIsValid(dp);) {
    auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, dp));
    dp = segment->chain_next_dp;
    EnsureOrderSegmentResident(ctx, global.shared_payload->segment_arena_dp, segment);
    buffer::YaapSharedSegmentReadLease lease;
    const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id, segment->generation};
    if (!buffer::YaapSharedSegmentAcquireRead(ctx.dsa, global.shared_payload->segment_arena_dp, handle, &lease))
     ereport(ERROR, (errmsg("pg_yaap: order sort segment lease failed")));
    auto *tdc = const_cast<TupleDataCollection *>(static_cast<const TupleDataCollection *>(lease.data));
    for (uint32_t r = 0; r < pg_atomic_read_u32(&tdc->row_count); ++r) {
     CHECK_FOR_INTERRUPTS();
     local.sorter->Put(tdc, TupleDataCollectionGetRowConst(tdc, r));
    }
    buffer::YaapSharedSegmentReleaseRead(ctx.dsa, global.shared_payload->segment_arena_dp, &lease);
    if (!buffer::YaapSharedSegmentRetire(ctx.dsa, budget, global.shared_payload->segment_arena_dp, handle))
     ereport(ERROR, (errmsg("pg_yaap: order consumed segment retire failed")));
   }
   global.segment_retired = true;
  } else {
   if (global.payload == nullptr) ereport(ERROR, (errmsg("pg_yaap: order payload missing")));
   for (uint32_t r = 0; r < pg_atomic_read_u32(&global.payload->row_count); ++r)
    local.sorter->Put(global.payload, TupleDataCollectionGetRowConst(global.payload, r));
   dsa_free(ctx.dsa, global.shared_payload->tdc_dp);
   global.shared_payload->tdc_dp = InvalidDsaPointer;
   global.payload = nullptr;
  }
  local.sorter->Perform();
 }
 while (out.count < PIPELINE_DEFAULT_CHUNK_SIZE) {
  CHECK_FOR_INTERRUPTS();
  const auto *row = local.sorter->Next();
  if (row == nullptr) break;
  Gather(global.payload_layout, row, TupleDataCollectionGetRowConst(row, 0), out, out.count++);
 }
 return out.count > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
