#include "parallel/pipeline/physical_order.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <algorithm>
#include <cstring>
#include <vector>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/cancel.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

static TupleDataCollection *
ResolveTdc(dsa_area *dsa, dsa_pointer tdc_dp)
{
	if (!DsaPointerIsValid(tdc_dp))
		return nullptr;
	return static_cast<TupleDataCollection *>(dsa_get_address(dsa, tdc_dp));
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
}

static dsa_pointer
GrowOrderTdc(ExecCtx &ctx, OrderGlobalState &global, uint32_t required_heap_bytes)
{
	dsa_pointer old_tdc_dp = global.shared_payload_dp;
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
	global.shared_payload_dp = new_tdc_dp;
	global.payload = new_tdc;
	global.max_rows = new_capacity;
	if (DsaPointerIsValid(old_tdc_dp))
		dsa_free(ctx.dsa, old_tdc_dp);
	return new_tdc_dp;
}

static int
CompareKeyColumn(TdcColumnKind kind,
		         const TupleDataCollection *tdc,
		         const uint8_t *a,
		         const uint8_t *b)
{
	switch (kind)
	{
		case TdcColumnKind::INT32:
		{
			int32_t va, vb;
			std::memcpy(&va, a, sizeof(int32_t));
			std::memcpy(&vb, b, sizeof(int32_t));
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
		case TdcColumnKind::INT64:
		{
			int64_t va, vb;
			std::memcpy(&va, a, sizeof(int64_t));
			std::memcpy(&vb, b, sizeof(int64_t));
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
		case TdcColumnKind::DOUBLE:
		{
			double va, vb;
			std::memcpy(&va, a, sizeof(double));
			std::memcpy(&vb, b, sizeof(double));
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
		case TdcColumnKind::STRING_REF:
		{
			VecStringRef ref_a;
			VecStringRef ref_b;
			std::memcpy(&ref_a, a, sizeof(ref_a));
			std::memcpy(&ref_b, b, sizeof(ref_b));
			const char *base = tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc)) : nullptr;
			const char *ptr_a = VecStringRefDataPtr(ref_a, base);
			const char *ptr_b = VecStringRefDataPtr(ref_b, base);
			if (ref_a.len != 0 && (ptr_a == nullptr || ptr_b == nullptr))
				elog(ERROR, "pg_yaap: STRING_REF order key missing heap backing");
			const uint32_t cmp_len = ref_a.len < ref_b.len ? ref_a.len : ref_b.len;
			const int cmp = cmp_len > 0 ? std::memcmp(ptr_a, ptr_b, cmp_len) : 0;
			if (cmp != 0)
				return cmp < 0 ? -1 : 1;
			return (ref_a.len < ref_b.len) ? -1 : (ref_a.len > ref_b.len) ? 1 : 0;
		}
	}
	return 0;
}

}  /* namespace */

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
		const uint32_t row_width = state->payload_layout != nullptr ? state->payload_layout->row_width : 8;
		const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->payload_layout,
			state->max_rows);
		state->shared_payload_dp = TupleDataCollectionAllocate(ctx.dsa,
			state->max_rows,
			row_width,
			heap_capacity);
		state->payload = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, state->shared_payload_dp));
		TupleDataCollectionInit(state->payload,
			state->max_rows,
			row_width,
			state->payload_layout_dp,
			heap_capacity);
		StoreSharedPayloadOnDescriptor(this, state->shared_payload_dp);
	}
	else if (DsaPointerIsValid(state->shared_payload_dp) ||
			 (state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this), DsaPointerIsValid(state->shared_payload_dp)))
	{
		state->payload = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, state->shared_payload_dp));
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

	for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
	{
		if (PipelineCancelRequestedEvery(ctx, row_idx))
			return SinkResultType::FINISHED;
		while (!TupleDataCollectionHasSpaceForAppend(global.payload,
			TupleDataCollectionRequiredHeapBytesForChunkRow(global.payload_layout, in, row_idx)))
		{
			shared_payload_dp_ = GrowOrderTdc(ctx,
				global,
				TupleDataCollectionRequiredHeapBytesForChunkRow(global.payload_layout, in, row_idx));
			StoreSharedPayloadOnDescriptor(this, shared_payload_dp_);
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

	if (global.payload == nullptr)
		return SinkFinalizeType::READY;
	if (global.key_layout == nullptr || global.payload_layout == nullptr)
		elog(ERROR, "pg_yaap: order finalize missing key/payload layouts");

	const uint32_t row_count = pg_atomic_read_u32(&global.payload->row_count);

	const Size indices_bytes = sizeof(OrderSortIndices) +
		static_cast<Size>(row_count) * sizeof(uint32_t);
	global.sort_indices_dp = dsa_allocate0(ctx.dsa, indices_bytes);
	auto *indices_obj = static_cast<OrderSortIndices *>(
		dsa_get_address(ctx.dsa, global.sort_indices_dp));
	indices_obj->count = row_count;
	for (uint32_t i = 0; i < row_count; ++i)
	{
		if (PipelineCancelRequestedEvery(ctx, i, 1023u))
			return SinkFinalizeType::READY;
		indices_obj->indices[i] = i;
	}
	if (global.desc != nullptr)
		global.desc->body.order.sort_indices = global.sort_indices_dp;
	/* Fan-out to all descriptor slots so the consumer-pipeline source-side
	 * resolves the same indices (Order is both P1.sink and P0.source; each
	 * slot is a separate descriptor entry per Fix A2). */
	for (OpDescriptor *d : desc_list_)
		if (d != nullptr)
			d->body.order.sort_indices = global.sort_indices_dp;

	const TupleDataLayout *key_layout = global.key_layout;
	const TupleDataCollection *payload = global.payload;
	const SortKeyDesc *sort_keys = global.sort_keys;
	const uint16_t n_sort_keys = global.n_sort_keys;
	if (PipelineCancelRequested(ctx))
		return SinkFinalizeType::READY;

	std::sort(indices_obj->indices,
	          indices_obj->indices + row_count,
	          [key_layout, payload, sort_keys, n_sort_keys](uint32_t lhs, uint32_t rhs)
	{
		const uint8_t *row_a = TupleDataCollectionGetRowConst(payload, lhs);
		const uint8_t *row_b = TupleDataCollectionGetRowConst(payload, rhs);
		for (uint16_t k = 0; k < key_layout->column_count; ++k)
		{
			const TdcColumnDesc &col = key_layout->columns[k];
			const int cmp = CompareKeyColumn(col.kind,
			                                 payload,
			                                 row_a + col.offset,
			                                 row_b + col.offset);
			if (cmp != 0)
			{
				const bool asc = (sort_keys != nullptr && k < n_sort_keys) ? sort_keys[k].asc : true;
				return asc ? (cmp < 0) : (cmp > 0);
			}
		}
		return false;
	});

	global.payload->finalized = true;
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
		state->payload = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, state->shared_payload_dp));
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

	if (global.payload == nullptr || !global.payload->finalized)
		return SourceResultType::FINISHED;
	if (global.payload_layout == nullptr)
		elog(ERROR, "pg_yaap: order source missing payload layout");
	if (!DsaPointerIsValid(global.sort_indices_dp))
		elog(ERROR, "pg_yaap: order source missing sort indices");

	auto *indices_obj = static_cast<const OrderSortIndices *>(
		dsa_get_address(ctx.dsa, global.sort_indices_dp));

	while (local.source_cursor < indices_obj->count &&
	       out.count < PIPELINE_DEFAULT_CHUNK_SIZE)
	{
		if (PipelineCancelRequestedEvery(ctx, local.source_cursor))
			break;
		const uint32_t row_idx = indices_obj->indices[local.source_cursor++];
		const uint8_t *row_ptr = TupleDataCollectionGetRowConst(global.payload, row_idx);
		Gather(global.payload_layout, global.payload, row_ptr, out, out.count);
		++out.count;
	}

	return out.count > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
