#include "parallel/pipeline/physical_perfect_hash_aggregate.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"

extern int pg_yaap_parallel_max_workers;
}

#include <array>
#include <cstring>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

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
			elog(ERROR, "pg_yaap: perfect hash TDC copy ran out of heap");
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

static TupleDataCollection *
ResolveTdc(dsa_area *dsa, dsa_pointer tdc_dp)
{
	if (!DsaPointerIsValid(tdc_dp))
		return nullptr;
	return static_cast<TupleDataCollection *>(dsa_get_address(dsa, tdc_dp));
}

static PerfectHashAggSharedPayload *
ResolvePayload(dsa_area *dsa, dsa_pointer payload_dp)
{
	if (!DsaPointerIsValid(payload_dp))
		return nullptr;
	return static_cast<PerfectHashAggSharedPayload *>(dsa_get_address(dsa, payload_dp));
}

static dsa_pointer *
ResolveLocalRegistry(dsa_area *dsa, PerfectHashAggSharedPayload *payload)
{
	if (payload == nullptr || !DsaPointerIsValid(payload->local_partitions_registry_dp))
		return nullptr;
	return static_cast<dsa_pointer *>(dsa_get_address(dsa, payload->local_partitions_registry_dp));
}

static uint32_t *
ResolveGlobalIndex(dsa_area *dsa, PerfectHashAggSharedPayload *payload)
{
	if (payload == nullptr || !DsaPointerIsValid(payload->global_index_dp))
		return nullptr;
	return static_cast<uint32_t *>(dsa_get_address(dsa, payload->global_index_dp));
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
		const int32_t v = chunk.int32_columns[col.src_col_idx][row_idx];
		if (v < 0 || v > 255)
			return false;
		key = (key << 8) | static_cast<uint32_t>(v);
	}
	*out_key = key;
	return true;
}

static inline bool
EncodePerfectHashRowKey(const TupleDataLayout *layout,
                        const uint8_t *row,
                        uint32_t *out_key)
{
	uint32_t key = 0;
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		if (col.kind != TdcColumnKind::INT32 || col.width != 4)
			return false;
		const int32_t v = *reinterpret_cast<const int32_t *>(row + col.offset);
		if (v < 0 || v > 255)
			return false;
		key = (key << 8) | static_cast<uint32_t>(v);
	}
	*out_key = key;
	return true;
}

static void
GrowLocalTdc(ExecCtx &ctx, PerfectHashAggLocalSinkState &local)
{
	TupleDataCollection *old_tdc = ResolveTdc(ctx.dsa, local.local_tdc_dp);
	if (old_tdc == nullptr)
		elog(ERROR, "pg_yaap: perfect hash local TDC missing");
	const uint32_t old_count = pg_atomic_read_u32(&old_tdc->row_count);
	const uint32_t new_capacity = Max(old_tdc->row_capacity * 2u, old_count + 1u);
	const uint32_t heap_capacity = old_tdc->heap_capacity > 0 ? old_tdc->heap_capacity * 2u :
		TupleDataCollectionDefaultHeapCapacity(local.layout, new_capacity);
	dsa_pointer new_tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
		new_capacity,
		local.layout->row_width,
		heap_capacity);
	auto *new_tdc = ResolveTdc(ctx.dsa, new_tdc_dp);
	TupleDataCollectionInit(new_tdc, new_capacity, local.layout->row_width, local.layout_dp, heap_capacity);
	for (uint32_t row_idx = 0; row_idx < old_count; ++row_idx)
	{
		uint8_t *dst = nullptr;
		const uint32_t copied_idx = TupleDataCollectionAppendRow(new_tdc, &dst);
		if (copied_idx == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: perfect hash local TDC grow overflow");
		const uint8_t *src = TupleDataCollectionGetRowConst(old_tdc, row_idx);
		CopyTdcRow(local.layout, old_tdc, src, new_tdc, dst);
	}
	local.local_tdc_dp = new_tdc_dp;
	local.local_tdc = new_tdc;
}

static void
GrowGlobalTdc(ExecCtx &ctx, PerfectHashAggGlobalSinkState &global)
{
	TupleDataCollection *old_tdc = global.global_tdc;
	if (old_tdc == nullptr)
		elog(ERROR, "pg_yaap: perfect hash global TDC missing");
	const uint32_t old_count = pg_atomic_read_u32(&old_tdc->row_count);
	const uint32_t new_capacity = Max(old_tdc->row_capacity * 2u, old_count + 1u);
	const uint32_t heap_capacity = old_tdc->heap_capacity > 0 ? old_tdc->heap_capacity * 2u :
		TupleDataCollectionDefaultHeapCapacity(global.layout, new_capacity);
	dsa_pointer new_tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
		new_capacity,
		global.layout->row_width,
		heap_capacity);
	auto *new_tdc = ResolveTdc(ctx.dsa, new_tdc_dp);
	TupleDataCollectionInit(new_tdc, new_capacity, global.layout->row_width, global.layout_dp, heap_capacity);
	for (uint32_t row_idx = 0; row_idx < old_count; ++row_idx)
	{
		uint8_t *dst = nullptr;
		const uint32_t copied_idx = TupleDataCollectionAppendRow(new_tdc, &dst);
		if (copied_idx == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: perfect hash global TDC grow overflow");
		const uint8_t *src = TupleDataCollectionGetRowConst(old_tdc, row_idx);
		CopyTdcRow(global.layout, old_tdc, src, new_tdc, dst);
	}
	global.payload->global_tdc_dp = new_tdc_dp;
	global.global_tdc = new_tdc;
}

static void
SinkChunkPerfectHash(ExecCtx &ctx,
                     PerfectHashAggLocalSinkState &local,
                     PipelineChunk &in)
{
	TupleDataCollection *tdc = local.local_tdc;
	if (tdc == nullptr)
		elog(ERROR, "pg_yaap: perfect hash local TDC missing");

	std::array<uint32_t, PIPELINE_DEFAULT_CHUNK_SIZE> canonical_rows;
	std::array<uint16_t, PIPELINE_DEFAULT_CHUNK_SIZE> update_rows;
	uint16_t update_count = 0;

	for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
	{
		uint32_t key = 0;
		if (!EncodePerfectHashKey(local.layout, in, row_idx, &key) ||
			key >= local.perfect_capacity)
		{
			ereport(ERROR,
			        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			         errmsg("pg_yaap: perfect hash key out of domain")));
		}

		uint32_t canonical_idx = local.perfect_row_indices[key];
		if (canonical_idx == TDC_INVALID_ROW_INDEX)
		{
			while (pg_atomic_read_u32(&tdc->row_count) >= tdc->row_capacity)
			{
				GrowLocalTdc(ctx, local);
				tdc = local.local_tdc;
			}
			uint8_t *candidate_row = nullptr;
			canonical_idx = TupleDataCollectionAppendRow(tdc, &candidate_row);
			if (canonical_idx == TDC_INVALID_ROW_INDEX)
				elog(ERROR, "pg_yaap: perfect hash local row capacity exceeded");
			ScatterGroupOnly(local.layout, tdc, candidate_row, in, row_idx);
			local.perfect_row_indices[key] = canonical_idx;
		}

		update_rows[update_count] = row_idx;
		canonical_rows[update_count] = canonical_idx;
		++update_count;
	}

	if (update_count > 0)
	{
		UpdateAggregatesGather(local.layout,
			tdc->rows,
			tdc->row_width,
			canonical_rows.data(),
			in,
			update_rows.data(),
			update_count);
	}
}

}  /* namespace */

std::unique_ptr<GlobalSinkState>
PhysicalPerfectHashAggregate::GetGlobalSinkState(ExecCtx &ctx)
{
	auto state = std::make_unique<PerfectHashAggGlobalSinkState>();
	state->dsa = ctx.dsa;
	state->desc = desc();
	state->layout_dp = DsaPointerIsValid(layout_dp()) ? layout_dp() : LayoutDpFromDescriptor();
	state->layout = ResolveLayout(ctx.dsa, state->layout_dp);
	state->shared_payload_dp = DsaPointerIsValid(shared_payload_dp()) ? shared_payload_dp() : SharedPayloadDpFromDescriptor();
	state->max_groups = MaxGroupsFromDescriptor();
	state->local_state_slot_count = static_cast<uint32_t>(std::max(1, pg_yaap_parallel_max_workers));

	if (state->layout == nullptr)
		elog(ERROR, "pg_yaap: perfect hash aggregate missing TupleDataLayout");

	if (ctx.worker_index == LEADER_WORKER_INDEX && !DsaPointerIsValid(state->shared_payload_dp))
	{
		const uint32_t perfect_capacity = PerfectHashCapacityFromDescriptor();
		if (perfect_capacity == 0)
			elog(ERROR, "pg_yaap: perfect hash aggregate missing perfect capacity");

		state->shared_payload_dp = dsa_allocate0(ctx.dsa, sizeof(PerfectHashAggSharedPayload));
		state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);
		state->payload->max_groups = state->max_groups;
		state->payload->local_state_slot_count = state->local_state_slot_count;
		state->payload->perfect_hash_capacity = perfect_capacity;
		state->payload->finalized = false;
		state->payload->global_index_dp = dsa_allocate0(ctx.dsa,
			static_cast<size_t>(perfect_capacity) * sizeof(uint32_t));
		state->payload->local_partitions_registry_dp = dsa_allocate0(ctx.dsa,
			static_cast<size_t>(state->local_state_slot_count) * sizeof(dsa_pointer));
		const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->layout,
			state->max_groups);
		state->payload->global_tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
			state->max_groups,
			state->layout->row_width,
			heap_capacity);
		state->global_tdc = ResolveTdc(ctx.dsa, state->payload->global_tdc_dp);
		TupleDataCollectionInit(state->global_tdc,
			state->max_groups,
			state->layout->row_width,
			state->layout_dp,
			heap_capacity);
		state->global_index = ResolveGlobalIndex(ctx.dsa, state->payload);
		for (uint32_t i = 0; i < perfect_capacity; ++i)
			state->global_index[i] = TDC_INVALID_ROW_INDEX;
		auto *registry = ResolveLocalRegistry(ctx.dsa, state->payload);
		for (uint32_t slot = 0; slot < state->local_state_slot_count; ++slot)
			registry[slot] = InvalidDsaPointer;
		StoreSharedPayloadOnDescriptor(this, state->shared_payload_dp);
	}
	else
	{
		if (!DsaPointerIsValid(state->shared_payload_dp))
			state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this);
		state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);
	}

	if (state->payload == nullptr)
		elog(ERROR, "pg_yaap: perfect hash aggregate global payload not initialized");
	state->global_tdc = ResolveTdc(ctx.dsa, state->payload->global_tdc_dp);
	state->global_index = ResolveGlobalIndex(ctx.dsa, state->payload);
	if (state->global_tdc == nullptr || state->global_index == nullptr)
		elog(ERROR, "pg_yaap: perfect hash aggregate global state not initialized");

	return state;
}

std::unique_ptr<LocalSinkState>
PhysicalPerfectHashAggregate::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<PerfectHashAggGlobalSinkState &>(gstate);
	auto state = std::make_unique<PerfectHashAggLocalSinkState>();
	state->layout = global.layout;
	state->layout_dp = global.layout_dp;
	state->max_groups = global.max_groups;
	state->perfect_capacity = global.payload->perfect_hash_capacity;
	state->perfect_row_indices.assign(state->perfect_capacity, TDC_INVALID_ROW_INDEX);
	const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->layout,
		state->max_groups);
	state->local_tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
		state->max_groups,
		state->layout->row_width,
		heap_capacity);
	state->local_tdc = ResolveTdc(ctx.dsa, state->local_tdc_dp);
	TupleDataCollectionInit(state->local_tdc,
		state->max_groups,
		state->layout->row_width,
		state->layout_dp,
		heap_capacity);

	if (ctx.worker_index >= 0)
	{
		auto *registry = ResolveLocalRegistry(ctx.dsa, global.payload);
		if (registry == nullptr || static_cast<uint32_t>(ctx.worker_index) >= global.payload->local_state_slot_count)
			elog(ERROR, "pg_yaap: perfect hash local registry missing");
		registry[ctx.worker_index] = state->local_tdc_dp;
	}

	return state;
}

SinkResultType
PhysicalPerfectHashAggregate::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input)
{
	auto &local = static_cast<PerfectHashAggLocalSinkState &>(input.local_state);
	SinkChunkPerfectHash(ctx, local, in);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
PhysicalPerfectHashAggregate::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input)
{
	auto &global = static_cast<PerfectHashAggGlobalSinkState &>(input.global_state);
	if (input.partition_id != UINT32_MAX)
		return SinkCombineResultType::FINISHED;

	if (input.local_state != nullptr)
	{
		auto &local = static_cast<PerfectHashAggLocalSinkState &>(*input.local_state);
		const uint32_t local_row_count = pg_atomic_read_u32(&local.local_tdc->row_count);
		for (uint32_t row_idx = 0; row_idx < local_row_count; ++row_idx)
		{
			const uint8_t *src_row = TupleDataCollectionGetRowConst(local.local_tdc, row_idx);
			uint32_t key = 0;
			if (!EncodePerfectHashRowKey(global.layout, src_row, &key) ||
				key >= global.payload->perfect_hash_capacity)
			{
				elog(ERROR, "pg_yaap: perfect hash combine row key out of domain");
			}

			uint32_t canonical_idx = global.global_index[key];
			if (canonical_idx == TDC_INVALID_ROW_INDEX)
			{
				while (pg_atomic_read_u32(&global.global_tdc->row_count) >= global.global_tdc->row_capacity)
					GrowGlobalTdc(ctx, global);
				uint8_t *dst_row = nullptr;
				canonical_idx = TupleDataCollectionAppendRow(global.global_tdc, &dst_row);
				if (canonical_idx == TDC_INVALID_ROW_INDEX)
					elog(ERROR, "pg_yaap: perfect hash global row capacity exceeded");
				std::memcpy(dst_row, src_row, global.layout->row_width);
				global.global_index[key] = canonical_idx;
			}
			else
			{
				uint8_t *dst_row = TupleDataCollectionGetRow(global.global_tdc, canonical_idx);
				CombineAggregates(global.layout, dst_row, src_row);
			}
		}
		return SinkCombineResultType::FINISHED;
	}

	dsa_pointer *registry = ResolveLocalRegistry(ctx.dsa, global.payload);
	if (registry == nullptr)
		elog(ERROR, "pg_yaap: perfect hash local registry missing");
	for (uint32_t slot = 0; slot < global.payload->local_state_slot_count; ++slot)
	{
		if (!DsaPointerIsValid(registry[slot]))
			continue;
		auto *local_tdc = ResolveTdc(ctx.dsa, registry[slot]);
		if (local_tdc == nullptr)
			continue;
		const uint32_t local_row_count = pg_atomic_read_u32(&local_tdc->row_count);
		for (uint32_t row_idx = 0; row_idx < local_row_count; ++row_idx)
		{
			const uint8_t *src_row = TupleDataCollectionGetRowConst(local_tdc, row_idx);
			uint32_t key = 0;
			if (!EncodePerfectHashRowKey(global.layout, src_row, &key) ||
				key >= global.payload->perfect_hash_capacity)
			{
				elog(ERROR, "pg_yaap: perfect hash combine row key out of domain");
			}

			uint32_t canonical_idx = global.global_index[key];
			if (canonical_idx == TDC_INVALID_ROW_INDEX)
			{
				while (pg_atomic_read_u32(&global.global_tdc->row_count) >= global.global_tdc->row_capacity)
					GrowGlobalTdc(ctx, global);
				uint8_t *dst_row = nullptr;
				canonical_idx = TupleDataCollectionAppendRow(global.global_tdc, &dst_row);
				if (canonical_idx == TDC_INVALID_ROW_INDEX)
					elog(ERROR, "pg_yaap: perfect hash global row capacity exceeded");
				std::memcpy(dst_row, src_row, global.layout->row_width);
				global.global_index[key] = canonical_idx;
			}
			else
			{
				uint8_t *dst_row = TupleDataCollectionGetRow(global.global_tdc, canonical_idx);
				CombineAggregates(global.layout, dst_row, src_row);
			}
		}
	}

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType
PhysicalPerfectHashAggregate::Finalize(ExecCtx &ctx, GlobalSinkState &gstate)
{
	(void) ctx;
	auto &global = static_cast<PerfectHashAggGlobalSinkState &>(gstate);
	if (global.global_tdc != nullptr)
		global.global_tdc->finalized = true;
	global.payload->finalized = true;
	global.finalized = true;
	return SinkFinalizeType::READY;
}

std::unique_ptr<GlobalSourceState>
PhysicalPerfectHashAggregate::GetGlobalSourceState(ExecCtx &ctx)
{
	auto state = std::make_unique<PerfectHashAggGlobalSourceState>();
	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp()) ? shared_payload_dp() :
		LoadSharedPayloadFromDescriptor(this);
	state->payload = ResolvePayload(ctx.dsa, payload_dp);
	if (state->payload == nullptr)
		elog(ERROR, "pg_yaap: perfect hash source payload not initialized");
	state->global_tdc = ResolveTdc(ctx.dsa, state->payload->global_tdc_dp);
	if (state->global_tdc == nullptr)
		elog(ERROR, "pg_yaap: perfect hash source TDC missing");
	state->layout = ResolveLayout(ctx.dsa, state->global_tdc->layout_dp);
	if (state->layout == nullptr)
		elog(ERROR, "pg_yaap: perfect hash source layout missing");
	state->finalized = state->payload->finalized;
	return state;
}

std::unique_ptr<LocalSourceState>
PhysicalPerfectHashAggregate::GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate)
{
	(void) ctx;
	(void) gstate;
	return std::make_unique<PerfectHashAggLocalSourceState>();
}

SourceResultType
PhysicalPerfectHashAggregate::GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input)
{
	auto &global = static_cast<PerfectHashAggGlobalSourceState &>(input.global_state);
	(void) input.local_state;
	(void) ctx;
	out.reset();
	if (!global.payload->finalized)
		return SourceResultType::FINISHED;
	if (global.global_tdc == nullptr || !global.global_tdc->finalized)
		return SourceResultType::FINISHED;

	const uint32_t row_count = pg_atomic_read_u32(&global.global_tdc->row_count);
	while (global.source_cursor < row_count && out.count < PIPELINE_DEFAULT_CHUNK_SIZE)
	{
		const uint8_t *row = TupleDataCollectionGetRowConst(global.global_tdc, global.source_cursor++);
		Gather(global.layout, global.global_tdc, row, out, out.count);
		++out.count;
	}

	return out.count > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

std::unique_ptr<OperatorState>
PhysicalPerfectHashAggregate::GetOperatorState(ExecCtx &ctx)
{
	(void) ctx;
	return std::make_unique<PerfectHashAggOperatorState>();
}

OperatorResultType
PhysicalPerfectHashAggregate::Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state)
{
	(void) ctx;
	auto &op_state = static_cast<PerfectHashAggOperatorState &>(state);
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
