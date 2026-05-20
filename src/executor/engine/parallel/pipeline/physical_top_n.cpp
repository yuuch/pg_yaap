#include "parallel/pipeline/physical_top_n.hpp"

extern "C" {
#include "postgres.h"
#include "utils/dsa.h"
#include "utils/elog.h"
#include "utils/palloc.h"

extern int pg_yaap_parallel_max_workers;
}

#include <algorithm>
#include <cstring>

#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

static TopNSharedPayload *
ResolvePayload(dsa_area *dsa, dsa_pointer payload_dp)
{
	return DsaPointerIsValid(payload_dp)
		? static_cast<TopNSharedPayload *>(dsa_get_address(dsa, payload_dp))
		: nullptr;
}

static const TupleDataLayout *
ResolveLayout(dsa_area *dsa, dsa_pointer layout_dp)
{
	return DsaPointerIsValid(layout_dp)
		? static_cast<const TupleDataLayout *>(dsa_get_address(dsa, layout_dp))
		: nullptr;
}

static TupleDataCollection *
AllocateLocalTdc(const TupleDataLayout *layout,
                 dsa_pointer layout_dp,
                 uint32_t max_rows)
{
	const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(layout, max_rows);
	const size_t alloc_bytes = TupleDataCollectionCheckedAllocSize(max_rows, layout->row_width, heap_capacity);
	auto *tdc = static_cast<TupleDataCollection *>(palloc0(alloc_bytes));
	TupleDataCollectionInit(tdc, max_rows, layout->row_width, layout_dp, heap_capacity);
	return tdc;
}

static int
CompareStoredRows(const TupleDataLayout *layout,
                  const SortKeyDesc *sort_keys,
                  uint16_t n_sort_keys,
                  const TupleDataCollection *lhs_tdc,
                  const uint8_t *lhs_row,
                  const TupleDataCollection *rhs_tdc,
                  const uint8_t *rhs_row)
{
	for (uint16_t key_idx = 0; key_idx < n_sort_keys; ++key_idx)
	{
		const SortKeyDesc &key = sort_keys[key_idx];
		if (key.col_idx >= layout->column_count)
			continue;
		const TdcColumnDesc &col = layout->columns[key.col_idx];
		int cmp = 0;
		switch (col.kind)
		{
			case TdcColumnKind::INT32:
			{
				int32_t lhs = 0;
				int32_t rhs = 0;
				std::memcpy(&lhs, lhs_row + col.offset, sizeof(lhs));
				std::memcpy(&rhs, rhs_row + col.offset, sizeof(rhs));
				cmp = (lhs < rhs) ? -1 : (lhs > rhs) ? 1 : 0;
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t lhs = 0;
				int64_t rhs = 0;
				std::memcpy(&lhs, lhs_row + col.offset, sizeof(lhs));
				std::memcpy(&rhs, rhs_row + col.offset, sizeof(rhs));
				cmp = (lhs < rhs) ? -1 : (lhs > rhs) ? 1 : 0;
				break;
			}
			case TdcColumnKind::DOUBLE:
			{
				double lhs = 0;
				double rhs = 0;
				std::memcpy(&lhs, lhs_row + col.offset, sizeof(lhs));
				std::memcpy(&rhs, rhs_row + col.offset, sizeof(rhs));
				cmp = (lhs < rhs) ? -1 : (lhs > rhs) ? 1 : 0;
				break;
			}
			case TdcColumnKind::STRING_REF:
			{
				VecStringRef lhs_ref;
				VecStringRef rhs_ref;
				std::memcpy(&lhs_ref, lhs_row + col.offset, sizeof(lhs_ref));
				std::memcpy(&rhs_ref, rhs_row + col.offset, sizeof(rhs_ref));
				const char *lhs_ptr = VecStringRefDataPtr(lhs_ref,
					reinterpret_cast<const char *>(TupleDataCollectionHeapConst(lhs_tdc)));
				const char *rhs_ptr = VecStringRefDataPtr(rhs_ref,
					reinterpret_cast<const char *>(TupleDataCollectionHeapConst(rhs_tdc)));
				const uint32_t cmp_len = lhs_ref.len < rhs_ref.len ? lhs_ref.len : rhs_ref.len;
				cmp = cmp_len > 0 ? std::memcmp(lhs_ptr, rhs_ptr, cmp_len) : 0;
				if (cmp != 0)
					cmp = cmp < 0 ? -1 : 1;
				else
					cmp = (lhs_ref.len < rhs_ref.len) ? -1 : (lhs_ref.len > rhs_ref.len) ? 1 : 0;
				break;
			}
		}
		if (cmp != 0)
			return key.asc ? cmp : -cmp;
	}
	return 0;
}

static int
CompareChunkRowToStoredRow(const SchemaDescriptor *input_schema,
                           const TupleDataLayout *layout,
                           const SortKeyDesc *sort_keys,
                           uint16_t n_sort_keys,
                           const PipelineChunk &chunk,
                           uint16_t row_idx,
                           const TupleDataCollection *stored_tdc,
                           const uint8_t *stored_row)
{
	for (uint16_t key_idx = 0; key_idx < n_sort_keys; ++key_idx)
	{
		const SortKeyDesc &key = sort_keys[key_idx];
		if (key.col_idx >= layout->column_count)
			continue;
		const ColumnSchema &schema_col = input_schema->columns[key.col_idx];
		const TdcColumnDesc &col = layout->columns[key.col_idx];
		int cmp = 0;
		switch (col.kind)
		{
			case TdcColumnKind::INT32:
			{
				const int32_t lhs = chunk.int32_columns[schema_col.chunk_slot][row_idx];
				int32_t rhs = 0;
				std::memcpy(&rhs, stored_row + col.offset, sizeof(rhs));
				cmp = (lhs < rhs) ? -1 : (lhs > rhs) ? 1 : 0;
				break;
			}
			case TdcColumnKind::INT64:
			{
				const int64_t lhs = chunk.int64_columns[schema_col.chunk_slot][row_idx];
				int64_t rhs = 0;
				std::memcpy(&rhs, stored_row + col.offset, sizeof(rhs));
				cmp = (lhs < rhs) ? -1 : (lhs > rhs) ? 1 : 0;
				break;
			}
			case TdcColumnKind::DOUBLE:
			{
				const double lhs = chunk.double_columns[schema_col.chunk_slot][row_idx];
				double rhs = 0;
				std::memcpy(&rhs, stored_row + col.offset, sizeof(rhs));
				cmp = (lhs < rhs) ? -1 : (lhs > rhs) ? 1 : 0;
				break;
			}
			case TdcColumnKind::STRING_REF:
			{
				const VecStringRef lhs_ref = chunk.string_columns[schema_col.chunk_slot][row_idx];
				VecStringRef rhs_ref;
				std::memcpy(&rhs_ref, stored_row + col.offset, sizeof(rhs_ref));
				const char *lhs_ptr = VecStringRefDataPtr(lhs_ref,
					chunk.string_arena.empty() ? nullptr : chunk.string_arena.data());
				const char *rhs_ptr = VecStringRefDataPtr(rhs_ref,
					reinterpret_cast<const char *>(TupleDataCollectionHeapConst(stored_tdc)));
				const uint32_t cmp_len = lhs_ref.len < rhs_ref.len ? lhs_ref.len : rhs_ref.len;
				cmp = cmp_len > 0 ? std::memcmp(lhs_ptr, rhs_ptr, cmp_len) : 0;
				if (cmp != 0)
					cmp = cmp < 0 ? -1 : 1;
				else
					cmp = (lhs_ref.len < rhs_ref.len) ? -1 : (lhs_ref.len > rhs_ref.len) ? 1 : 0;
				break;
			}
		}
		if (cmp != 0)
			return key.asc ? cmp : -cmp;
	}
	return 0;
}

static void
CopyStoredRow(const TupleDataLayout *layout,
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
			reinterpret_cast<const char *>(TupleDataCollectionHeapConst(src_tdc)));
		VecStringRef dst_ref;
		if (!TupleDataCollectionStoreStringBytes(dst_tdc, src_ptr, src_ref.len, &dst_ref))
			elog(ERROR, "pg_yaap: top-N TDC ran out of heap");
		std::memcpy(dst_row + col.offset, &dst_ref, sizeof(dst_ref));
	}
}

static TupleDataCollection *
RebuildLocalTdc(const TupleDataLayout *layout,
                dsa_pointer layout_dp,
                uint32_t max_rows,
                const TupleDataCollection *old_tdc,
                const PipelineChunk *append_chunk,
                uint16_t append_row_idx,
                uint32_t replace_row_idx,
                const TupleDataCollection *append_tdc,
                const uint8_t *append_row_ptr)
{
	uint64_t heap_needed = 0;
	const uint32_t row_count = old_tdc != nullptr ? pg_atomic_read_u32(
		const_cast<pg_atomic_uint32 *>(&old_tdc->row_count)) : 0;
	for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
	{
		if (row_idx == replace_row_idx)
			continue;
		heap_needed += TupleDataCollectionRequiredHeapBytesForRow(layout,
			old_tdc,
			TupleDataCollectionGetRowConst(old_tdc, row_idx));
	}
	if (append_chunk != nullptr)
		heap_needed += TupleDataCollectionRequiredHeapBytesForChunkRow(layout, *append_chunk, append_row_idx);
	else if (append_tdc != nullptr && append_row_ptr != nullptr)
		heap_needed += TupleDataCollectionRequiredHeapBytesForRow(layout, append_tdc, append_row_ptr);

	uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(layout, max_rows);
	if (heap_needed > heap_capacity)
		heap_capacity = static_cast<uint32_t>(heap_needed);

	const size_t alloc_bytes = TupleDataCollectionCheckedAllocSize(max_rows, layout->row_width, heap_capacity);
	auto *new_tdc = static_cast<TupleDataCollection *>(palloc0(alloc_bytes));
	TupleDataCollectionInit(new_tdc, max_rows, layout->row_width, layout_dp, heap_capacity);
	for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
	{
		if (row_idx == replace_row_idx)
			continue;
		uint8_t *dst = nullptr;
		if (TupleDataCollectionAppendRow(new_tdc, &dst) == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: local top-N rebuild overflow");
		CopyStoredRow(layout,
			old_tdc,
			TupleDataCollectionGetRowConst(old_tdc, row_idx),
			new_tdc,
			dst);
	}
	uint8_t *dst = nullptr;
	if (TupleDataCollectionAppendRow(new_tdc, &dst) == TDC_INVALID_ROW_INDEX)
		elog(ERROR, "pg_yaap: local top-N append overflow");
	if (append_chunk != nullptr)
		Scatter(layout, new_tdc, dst, *append_chunk, append_row_idx);
	else
		CopyStoredRow(layout, append_tdc, append_row_ptr, new_tdc, dst);
	return new_tdc;
}

static dsa_pointer
RebuildGlobalTdc(ExecCtx &ctx,
                 TopNGlobalState &global,
                 const PipelineChunk *append_chunk,
                 uint16_t append_row_idx,
                 uint32_t replace_row_idx,
                 const TupleDataCollection *append_tdc,
                 const uint8_t *append_row_ptr)
{
	TupleDataCollection *old_tdc = global.global_tdc;
	const uint32_t row_count = old_tdc != nullptr ? pg_atomic_read_u32(&old_tdc->row_count) : 0;
	uint64_t heap_needed = 0;
	for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
	{
		if (row_idx == replace_row_idx)
			continue;
		heap_needed += TupleDataCollectionRequiredHeapBytesForRow(global.layout,
			old_tdc,
			TupleDataCollectionGetRowConst(old_tdc, row_idx));
	}
	if (append_chunk != nullptr)
		heap_needed += TupleDataCollectionRequiredHeapBytesForChunkRow(global.layout, *append_chunk, append_row_idx);
	else if (append_tdc != nullptr && append_row_ptr != nullptr)
		heap_needed += TupleDataCollectionRequiredHeapBytesForRow(global.layout, append_tdc, append_row_ptr);

	uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(global.layout, global.max_rows);
	if (heap_needed > heap_capacity)
		heap_capacity = static_cast<uint32_t>(heap_needed);

	dsa_pointer new_tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
		global.max_rows,
		global.layout->row_width,
		heap_capacity);
	auto *new_tdc = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, new_tdc_dp));
	TupleDataCollectionInit(new_tdc,
		global.max_rows,
		global.layout->row_width,
		global.layout_dp,
		heap_capacity);
	for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
	{
		if (row_idx == replace_row_idx)
			continue;
		uint8_t *dst = nullptr;
		if (TupleDataCollectionAppendRow(new_tdc, &dst) == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: global top-N rebuild overflow");
		CopyStoredRow(global.layout,
			old_tdc,
			TupleDataCollectionGetRowConst(old_tdc, row_idx),
			new_tdc,
			dst);
	}
	uint8_t *dst = nullptr;
	if (TupleDataCollectionAppendRow(new_tdc, &dst) == TDC_INVALID_ROW_INDEX)
		elog(ERROR, "pg_yaap: global top-N append overflow");
	if (append_chunk != nullptr)
		Scatter(global.layout, new_tdc, dst, *append_chunk, append_row_idx);
	else
		CopyStoredRow(global.layout, append_tdc, append_row_ptr, new_tdc, dst);

	global.payload->tdc_dp = new_tdc_dp;
	global.global_tdc = new_tdc;
	return new_tdc_dp;
}

static uint32_t
FindWorstStoredRowIndex(const TupleDataLayout *layout,
                        const SortKeyDesc *sort_keys,
                        uint16_t n_sort_keys,
                        const TupleDataCollection *tdc)
{
	const uint32_t row_count = pg_atomic_read_u32(const_cast<pg_atomic_uint32 *>(&tdc->row_count));
	uint32_t worst_idx = 0;
	for (uint32_t row_idx = 1; row_idx < row_count; ++row_idx)
	{
		if (CompareStoredRows(layout,
			sort_keys,
			n_sort_keys,
			tdc,
			TupleDataCollectionGetRowConst(tdc, row_idx),
			tdc,
			TupleDataCollectionGetRowConst(tdc, worst_idx)) > 0)
			worst_idx = row_idx;
	}
	return worst_idx;
}

}  // namespace

int
PhysicalTopN::MaxThreads(ExecCtx &ctx) const
{
	(void) ctx;
	return std::max(1, pg_yaap_parallel_max_workers);
}

std::unique_ptr<GlobalSinkState>
PhysicalTopN::GetGlobalSinkState(ExecCtx &ctx)
{
	auto state = std::make_unique<TopNGlobalState>();
	state->dsa = ctx.dsa;
	state->desc = desc_;
	state->input_schema_dp = input_schema_dp_;
	state->layout_dp = layout_dp_;
	state->sort_keys_dp = sort_keys_dp_;
	state->n_sort_keys = n_sort_keys_;
	state->shared_payload_dp = DsaPointerIsValid(shared_payload_dp_) ?
		shared_payload_dp_ : LoadSharedPayloadFromDescriptor(this);
	if (!DsaPointerIsValid(state->shared_payload_dp) && desc_ != nullptr)
	{
		state->input_schema_dp = desc_->body.top_n.input_schema;
		state->shared_payload_dp = desc_->body.top_n.shared_payload;
		state->layout_dp = desc_->body.top_n.layout;
		state->sort_keys_dp = desc_->body.top_n.sort_keys;
		state->n_sort_keys = desc_->body.top_n.n_sort_keys;
	}
	state->input_schema = DsaPointerIsValid(state->input_schema_dp)
		? static_cast<const SchemaDescriptor *>(dsa_get_address(ctx.dsa, state->input_schema_dp))
		: nullptr;
	state->layout = ResolveLayout(ctx.dsa, state->layout_dp);
	if (DsaPointerIsValid(state->sort_keys_dp))
		state->sort_keys = static_cast<const SortKeyDesc *>(dsa_get_address(ctx.dsa, state->sort_keys_dp));
	state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);
	state->global_tdc = state->payload != nullptr && DsaPointerIsValid(state->payload->tdc_dp)
		? static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, state->payload->tdc_dp))
		: nullptr;
	state->sort_indices_dp = state->payload != nullptr ? state->payload->sort_indices_dp : InvalidDsaPointer;
	state->max_rows = max_rows_ > 0 ? max_rows_ : (desc_ != nullptr ? desc_->body.top_n.max_rows : 0);
	if (state->input_schema == nullptr || state->layout == nullptr || state->sort_keys == nullptr ||
		state->payload == nullptr || state->global_tdc == nullptr)
		elog(ERROR, "pg_yaap: top-N global state missing layout/sort keys/payload");
	return state;
}

std::unique_ptr<LocalSinkState>
PhysicalTopN::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate)
{
	(void) ctx;
	(void) gstate;
	return std::make_unique<TopNLocalSinkState>();
}

SinkResultType
PhysicalTopN::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input)
{
	(void) ctx;
	auto &global = static_cast<TopNGlobalState &>(input.global_state);
	(void) input.local_state;
	for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
	{
		SpinLockAcquire(&global.payload->mutex);
		global.global_tdc = DsaPointerIsValid(global.payload->tdc_dp)
			? static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, global.payload->tdc_dp))
			: nullptr;
		if (global.global_tdc == nullptr)
			elog(ERROR, "pg_yaap: top-N sink missing global TDC");
		const uint32_t row_count = pg_atomic_read_u32(&global.global_tdc->row_count);
		if (row_count >= global.max_rows)
		{
			const uint32_t worst_idx = FindWorstStoredRowIndex(global.layout,
				global.sort_keys,
				global.n_sort_keys,
				global.global_tdc);
			const uint8_t *worst_row = TupleDataCollectionGetRowConst(global.global_tdc, worst_idx);
			if (CompareChunkRowToStoredRow(global.input_schema,
				global.layout,
				global.sort_keys,
				global.n_sort_keys,
				in,
				row_idx,
				global.global_tdc,
				worst_row) < 0)
			{
				dsa_pointer old_tdc_dp = global.payload->tdc_dp;
				dsa_pointer new_tdc_dp = RebuildGlobalTdc(ctx,
					global,
					&in,
					row_idx,
					worst_idx,
					nullptr,
					nullptr);
				global.payload->tdc_dp = new_tdc_dp;
				global.global_tdc = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, new_tdc_dp));
				if (DsaPointerIsValid(old_tdc_dp))
					dsa_free(ctx.dsa, old_tdc_dp);
			}
			SpinLockRelease(&global.payload->mutex);
			continue;
		}
		if (!TupleDataCollectionHasSpaceForAppend(global.global_tdc,
			TupleDataCollectionRequiredHeapBytesForChunkRow(global.layout, in, row_idx)))
		{
			dsa_pointer old_tdc_dp = global.payload->tdc_dp;
			dsa_pointer new_tdc_dp = RebuildGlobalTdc(ctx,
				global,
				&in,
				row_idx,
				UINT32_MAX,
				nullptr,
				nullptr);
			global.payload->tdc_dp = new_tdc_dp;
			global.global_tdc = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, new_tdc_dp));
			if (DsaPointerIsValid(old_tdc_dp))
				dsa_free(ctx.dsa, old_tdc_dp);
			SpinLockRelease(&global.payload->mutex);
			continue;
		}
		uint8_t *dst = nullptr;
		if (TupleDataCollectionAppendRow(global.global_tdc, &dst) == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: global top-N row capacity exceeded");
		Scatter(global.layout, global.global_tdc, dst, in, row_idx);
		SpinLockRelease(&global.payload->mutex);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
PhysicalTopN::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input)
{
	(void) ctx;
	(void) input;
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType
PhysicalTopN::Finalize(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<TopNGlobalState &>(gstate);
	global.global_tdc = DsaPointerIsValid(global.payload->tdc_dp)
		? static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, global.payload->tdc_dp))
		: nullptr;
	if (global.global_tdc == nullptr)
		elog(ERROR, "pg_yaap: top-N finalize missing global TDC");
	const uint32_t row_count = pg_atomic_read_u32(&global.global_tdc->row_count);
	const Size indices_bytes = sizeof(OrderSortIndices) + static_cast<Size>(row_count) * sizeof(uint32_t);
	global.sort_indices_dp = dsa_allocate0(ctx.dsa, indices_bytes);
	auto *indices_obj = static_cast<OrderSortIndices *>(dsa_get_address(ctx.dsa, global.sort_indices_dp));
	indices_obj->count = row_count;
	for (uint32_t i = 0; i < row_count; ++i)
		indices_obj->indices[i] = i;
	std::sort(indices_obj->indices,
		indices_obj->indices + row_count,
		[&global](uint32_t lhs, uint32_t rhs)
		{
			return CompareStoredRows(global.layout,
				global.sort_keys,
				global.n_sort_keys,
				global.global_tdc,
				TupleDataCollectionGetRowConst(global.global_tdc, lhs),
				global.global_tdc,
				TupleDataCollectionGetRowConst(global.global_tdc, rhs)) < 0;
		});
	global.payload->sort_indices_dp = global.sort_indices_dp;
	global.payload->finalized = true;
	global.global_tdc->finalized = true;
	if (global.desc != nullptr)
		global.desc->body.top_n.sort_indices = global.sort_indices_dp;
	for (OpDescriptor *desc : desc_list_)
		if (desc != nullptr)
			desc->body.top_n.sort_indices = global.sort_indices_dp;
	return SinkFinalizeType::READY;
}

std::unique_ptr<GlobalSourceState>
PhysicalTopN::GetGlobalSourceState(ExecCtx &ctx)
{
	auto sink_state = GetGlobalSinkState(ctx);
	return std::unique_ptr<GlobalSourceState>(
		static_cast<TopNGlobalState *>(sink_state.release()));
}

std::unique_ptr<LocalSourceState>
PhysicalTopN::GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate)
{
	(void) ctx;
	(void) gstate;
	return std::make_unique<TopNLocalSourceState>();
}

SourceResultType
PhysicalTopN::GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input)
{
	(void) ctx;
	auto &global = static_cast<TopNGlobalState &>(input.global_state);
	auto &local = static_cast<TopNLocalSourceState &>(input.local_state);
	out.reset();
	global.global_tdc = DsaPointerIsValid(global.payload->tdc_dp)
		? static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, global.payload->tdc_dp))
		: nullptr;
	if (global.global_tdc == nullptr || !global.payload->finalized || !DsaPointerIsValid(global.payload->sort_indices_dp))
		return SourceResultType::FINISHED;
	auto *indices_obj = static_cast<const OrderSortIndices *>(dsa_get_address(ctx.dsa, global.payload->sort_indices_dp));
	while (local.source_cursor < indices_obj->count && out.count < PIPELINE_DEFAULT_CHUNK_SIZE)
	{
		const uint32_t row_idx = indices_obj->indices[local.source_cursor++];
		const uint8_t *row_ptr = TupleDataCollectionGetRowConst(global.global_tdc, row_idx);
		Gather(global.layout, global.global_tdc, row_ptr, out, out.count);
		++out.count;
	}
	return out.count > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
