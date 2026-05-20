#include "parallel/pipeline/physical_cross_product.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"

extern int pg_yaap_parallel_max_workers;
extern bool pg_yaap_trace_execution_path;
}

#include <algorithm>
#include <cstring>

#include "parallel/pipeline/cancel.hpp"
#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/physical_hash_join_fast_probe.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

static uint32_t
EffectiveWorkerCount(const ExecCtx &ctx)
{
	if (ctx.control != nullptr && ctx.control->num_workers > 0)
		return static_cast<uint32_t>(ctx.control->num_workers);
	return static_cast<uint32_t>(std::max(1, pg_yaap_parallel_max_workers));
}

static CrossProductSharedPayload *
ResolvePayload(dsa_area *dsa, dsa_pointer payload_dp)
{
	if (!DsaPointerIsValid(payload_dp))
		return nullptr;
	return static_cast<CrossProductSharedPayload *>(dsa_get_address(dsa, payload_dp));
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

static CrossProductLocalBuildRegistryEntry *
ResolveLocalBuildRegistry(dsa_area *dsa, CrossProductSharedPayload *payload)
{
	if (payload == nullptr || !DsaPointerIsValid(payload->local_build_registry_dp))
		return nullptr;
	return static_cast<CrossProductLocalBuildRegistryEntry *>(
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
			elog(ERROR, "pg_yaap: cross product build-row copy ran out of heap");
		std::memcpy(dst_row + col.offset, &dst_ref, sizeof(dst_ref));
	}
}

static bool
TdcNeedsGrowForChunkRow(const TupleDataLayout *layout,
	                   const TupleDataCollection *tdc,
	                   const PipelineChunk &chunk,
	                   uint16_t row_idx)
{
	return !TupleDataCollectionHasSpaceForAppend(
		tdc,
		TupleDataCollectionRequiredHeapBytesForChunkRow(layout, chunk, row_idx));
}

static bool
TdcNeedsGrowForStoredRow(const TupleDataLayout *layout,
	                    const TupleDataCollection *tdc,
	                    const TupleDataCollection *src_tdc,
	                    const uint8_t *src_row)
{
	return !TupleDataCollectionHasSpaceForAppend(
		tdc,
		TupleDataCollectionRequiredHeapBytesForRow(layout, src_tdc, src_row));
}

static TupleDataCollection *
GrowTdcCopyRows(ExecCtx &ctx,
	           const TupleDataLayout *layout,
	           dsa_pointer layout_dp,
	           TupleDataCollection *old_tdc,
	           uint32_t minimum_row_capacity)
{
	if (layout == nullptr || old_tdc == nullptr)
		elog(ERROR, "pg_yaap: cross product grow TDC missing layout/state");
	const uint32_t old_capacity = old_tdc->row_capacity;
	const uint32_t new_capacity = std::max<uint32_t>(
		minimum_row_capacity,
		old_capacity < (1u << 30) ? old_capacity * 2u : old_capacity);
	const uint32_t heap_capacity = LayoutHasStringRef(layout)
		? TupleDataCollectionGrowHeapCapacity(layout, old_tdc, new_capacity, 0)
		: 0;
	dsa_pointer new_tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
		new_capacity,
		layout->row_width,
		heap_capacity);
	auto *new_tdc = ResolveTdc(ctx.dsa, new_tdc_dp);
	TupleDataCollectionInit(new_tdc, new_capacity, layout->row_width, layout_dp, heap_capacity);
	const uint32_t row_count = pg_atomic_read_u32(&old_tdc->row_count);
	for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
	{
		const uint8_t *src = TupleDataCollectionGetRowConst(old_tdc, row_idx);
		uint8_t *dst = nullptr;
		if (TupleDataCollectionAppendRow(new_tdc, &dst) == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: cross product grow TDC capacity copy failed");
		CopyBuildRow(layout, old_tdc, src, new_tdc, dst);
	}
	return new_tdc;
}

class CrossProductOperatorState final : public OperatorState {
public:
	bool initialized = false;
	bool current_input_drained = false;
	uint16_t probe_row_idx = 0;
	uint32_t build_row_idx = 0;
	HashJoinFastProbeState output_state;
};

}  // namespace

int
PhysicalCrossProduct::MaxThreads(ExecCtx &ctx) const
{
	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	CrossProductSharedPayload *payload = ResolvePayload(ctx.dsa, payload_dp);
	if (payload == nullptr)
		return 1;
	return std::max(1, static_cast<int>(payload->local_state_slot_count));
}

void
PhysicalCrossProduct::BuildPipelines(Pipeline &current, MetaPipeline &meta)
{
	Assert(children().size() == 2);

	Pipeline &build_pipeline = meta.CreateChildPipeline(current, *this);
	meta.SetSink(build_pipeline, *this);
	build_pipeline.source = nullptr;
	children()[1]->BuildPipelines(build_pipeline, meta);
	children()[0]->BuildPipelines(current, meta);
	meta.AddOperator(current, *this);
}

std::unique_ptr<GlobalSinkState>
PhysicalCrossProduct::GetGlobalSinkState(ExecCtx &ctx)
{
	auto state = std::make_unique<CrossProductGlobalSinkState>();
	state->dsa = ctx.dsa;
	state->desc = desc_;
	state->right_layout_dp = right_payload_layout_dp_;
	state->shared_payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	if (!DsaPointerIsValid(state->right_layout_dp) && desc_ != nullptr)
		state->right_layout_dp = desc_->body.cross_product.right_payload_layout;
	state->right_layout = ResolveLayout(ctx.dsa, state->right_layout_dp);
	if (state->right_layout == nullptr)
		elog(ERROR, "pg_yaap: cross product missing build-side payload layout");

	if (ctx.worker_index == LEADER_WORKER_INDEX && !DsaPointerIsValid(state->shared_payload_dp))
	{
		state->shared_payload_dp = dsa_allocate0(ctx.dsa, sizeof(CrossProductSharedPayload));
		state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);
		state->payload->local_state_slot_count = EffectiveWorkerCount(ctx);
		state->payload->finalized = false;
		pg_atomic_init_u32(&state->payload->release_state, 0);
		state->payload->combined_row_count = 0;
		SpinLockInit(&state->payload->mutex);
		state->payload->local_build_registry_dp = dsa_allocate0(ctx.dsa,
			static_cast<size_t>(state->payload->local_state_slot_count) * sizeof(CrossProductLocalBuildRegistryEntry));
		const uint32_t global_row_capacity = std::max<uint32_t>(1024u, max_rows_);
		const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->right_layout,
			global_row_capacity);
		state->payload->build_rows_dp = TupleDataCollectionAllocate(ctx.dsa,
			global_row_capacity,
			state->right_layout->row_width,
			heap_capacity);
		TupleDataCollectionInit(ResolveTdc(ctx.dsa, state->payload->build_rows_dp),
			global_row_capacity,
			state->right_layout->row_width,
			state->right_layout_dp,
			heap_capacity);
		StoreSharedPayloadOnDescriptor(this, state->shared_payload_dp);
	}

	state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);
	if (state->payload == nullptr)
		elog(ERROR, "pg_yaap: cross product shared payload missing");
	state->build_rows = ResolveTdc(ctx.dsa, state->payload->build_rows_dp);
	if (state->build_rows == nullptr)
		elog(ERROR, "pg_yaap: cross product global build rows missing");
	return state;
}

std::unique_ptr<LocalSinkState>
PhysicalCrossProduct::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<CrossProductGlobalSinkState &>(gstate);
	if (global.payload == nullptr || global.right_layout == nullptr)
		elog(ERROR, "pg_yaap: cross product local sink state missing global payload");

	auto state = std::make_unique<CrossProductLocalSinkState>();
	state->build_layout_dp = global.right_layout_dp;
	state->build_layout = global.right_layout;
	const uint32_t local_slot_count = std::max<uint32_t>(1, global.payload->local_state_slot_count);
	const uint32_t local_row_capacity =
		std::max<uint32_t>(1024u, (max_rows_ + local_slot_count - 1) / local_slot_count);
	const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->build_layout,
		local_row_capacity);
	state->build_rows_dp = TupleDataCollectionAllocate(ctx.dsa,
		local_row_capacity,
		state->build_layout->row_width,
		heap_capacity);
	state->build_rows = ResolveTdc(ctx.dsa, state->build_rows_dp);
	TupleDataCollectionInit(state->build_rows,
		local_row_capacity,
		state->build_layout->row_width,
		state->build_layout_dp,
		heap_capacity);
	if (state->build_rows == nullptr)
		elog(ERROR, "pg_yaap: cross product local build rows allocation failed");

	if (ctx.worker_index >= 0 &&
		static_cast<uint32_t>(ctx.worker_index) < global.payload->local_state_slot_count)
	{
		auto *registry = ResolveLocalBuildRegistry(ctx.dsa, global.payload);
		if (registry != nullptr)
			registry[ctx.worker_index].build_rows_dp = state->build_rows_dp;
	}
	return state;
}

SinkResultType
PhysicalCrossProduct::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input)
{
	(void) ctx;
	auto &local = static_cast<CrossProductLocalSinkState &>(input.local_state);
	if (local.build_rows == nullptr || local.build_layout == nullptr)
		elog(ERROR, "pg_yaap: cross product sink missing build rows/layout");

	for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
	{
		if (TdcNeedsGrowForChunkRow(local.build_layout, local.build_rows, in, row_idx))
			local.build_rows = GrowTdcCopyRows(ctx, local.build_layout, local.build_layout_dp, local.build_rows, local.build_rows->row_capacity + 1u);
		uint8_t *row_ptr = nullptr;
		if (TupleDataCollectionAppendRow(local.build_rows, &row_ptr) == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: cross product local build row capacity exceeded");
		ScatterGroupOnly(local.build_layout, local.build_rows, row_ptr, in, row_idx);
	}

	local.build_input_rows += in.count;
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
PhysicalCrossProduct::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input)
{
	auto &global = static_cast<CrossProductGlobalSinkState &>(input.global_state);
	if (input.local_state == nullptr)
		elog(ERROR, "pg_yaap: cross product combine local state missing");
	auto &local = static_cast<CrossProductLocalSinkState &>(*input.local_state);
	if (local.build_rows == nullptr || global.build_rows == nullptr || global.payload == nullptr)
		elog(ERROR, "pg_yaap: cross product combine build rows missing");

	const uint32_t local_row_count = pg_atomic_read_u32(&local.build_rows->row_count);
	uint32_t row_idx = 0;
	while (row_idx < local_row_count)
	{
		if (PipelineCancelRequested(ctx))
			return SinkCombineResultType::FINISHED;
		const uint32_t batch_end = std::min(row_idx + 64u, local_row_count);
		SpinLockAcquire(&global.payload->mutex);
		for (; row_idx < batch_end; ++row_idx)
		{
			const uint8_t *src_row = TupleDataCollectionGetRowConst(local.build_rows, row_idx);
			while (TdcNeedsGrowForStoredRow(global.right_layout,
					global.build_rows,
					local.build_rows,
					src_row))
				global.build_rows = GrowTdcCopyRows(ctx, global.right_layout, global.right_layout_dp, global.build_rows, global.build_rows->row_capacity + 1u);
			uint8_t *dst_row = nullptr;
			if (TupleDataCollectionAppendRow(global.build_rows, &dst_row) == TDC_INVALID_ROW_INDEX)
				elog(ERROR, "pg_yaap: cross product global build row capacity exceeded during combine");
			CopyBuildRow(global.right_layout, local.build_rows, src_row, global.build_rows, dst_row);
		}
		SpinLockRelease(&global.payload->mutex);
	}
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType
PhysicalCrossProduct::Finalize(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<CrossProductGlobalSinkState &>(gstate);
	if (global.payload == nullptr || global.build_rows == nullptr)
		elog(ERROR, "pg_yaap: cross product finalize missing global state");
	global.payload->combined_row_count = pg_atomic_read_u32(&global.build_rows->row_count);
	if (global.payload->combined_row_count > max_rows_)
		elog(ERROR,
		     "pg_yaap: cross product build side exceeded max rows (%u > %u)",
		     global.payload->combined_row_count,
		     max_rows_);
	TupleDataCollectionResetScan(global.build_rows);
	global.build_rows->finalized = true;
	global.payload->finalized = true;
	if (pg_yaap_trace_execution_path)
		ereport(LOG,
			(errmsg("pg_yaap crossproduct finalize build_rows=%u payload_width=%u",
				global.payload->combined_row_count,
				global.right_layout != nullptr ? global.right_layout->row_width : 0)));
	(void) ctx;
	return SinkFinalizeType::READY;
}

std::unique_ptr<OperatorState>
PhysicalCrossProduct::GetOperatorState(ExecCtx &ctx)
{
	(void) ctx;
	return std::make_unique<CrossProductOperatorState>();
}

void
PhysicalCrossProduct::ReleaseBuildPayloadAfterConsumerRun(ExecCtx &ctx)
{
	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	CrossProductSharedPayload *payload = ResolvePayload(ctx.dsa, payload_dp);
	if (payload == nullptr)
		return;

	uint32 expected = 0;
	if (!pg_atomic_compare_exchange_u32(&payload->release_state, &expected, 1))
		return;

	auto *registry = ResolveLocalBuildRegistry(ctx.dsa, payload);
	if (registry != nullptr)
	{
		for (uint32_t i = 0; i < payload->local_state_slot_count; ++i)
			FreeDsaPointerIfValid(ctx.dsa, &registry[i].build_rows_dp);
	}
	FreeDsaPointerIfValid(ctx.dsa, &payload->local_build_registry_dp);
	FreeDsaPointerIfValid(ctx.dsa, &payload->build_rows_dp);
	ClearSharedPayloadOnDescriptor(this);
	pg_atomic_write_u32(&payload->release_state, 2);
	dsa_free(ctx.dsa, payload_dp);
}

OperatorResultType
PhysicalCrossProduct::Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state)
{
	auto &op_state = static_cast<CrossProductOperatorState &>(state);
	if (op_state.current_input_drained)
	{
		op_state.current_input_drained = false;
		out.reset();
		return OperatorResultType::NEED_MORE_INPUT;
	}

	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	CrossProductSharedPayload *payload = ResolvePayload(ctx.dsa, payload_dp);
	if (payload == nullptr || !payload->finalized)
		elog(ERROR, "pg_yaap: cross product probe ran before build/finalize completed");
	if (pg_atomic_read_u32(&payload->release_state) != 0)
		elog(ERROR, "pg_yaap: cross product payload used after release");
	TupleDataCollection *build_rows = ResolveTdc(ctx.dsa, payload->build_rows_dp);
	if (build_rows == nullptr)
		elog(ERROR, "pg_yaap: cross product probe missing finalized build rows");
	const TupleDataLayout *build_row_layout = ResolveLayout(ctx.dsa,
		DsaPointerIsValid(right_payload_layout_dp_) ? right_payload_layout_dp_ :
		(desc_ != nullptr ? desc_->body.cross_product.right_payload_layout : InvalidDsaPointer));
	if (build_row_layout == nullptr)
		elog(ERROR, "pg_yaap: cross product probe missing build payload layout");
	const auto *output_columns = DsaPointerIsValid(output_columns_dp_)
		? static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, output_columns_dp_))
		: (desc_ != nullptr && DsaPointerIsValid(desc_->body.cross_product.output_columns)
			? static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, desc_->body.cross_product.output_columns))
			: nullptr);
	const uint16_t output_column_count = output_column_count_ > 0 ? output_column_count_ :
		(desc_ != nullptr ? desc_->body.cross_product.output_column_count : 0);
	if (output_columns == nullptr || output_column_count == 0)
		elog(ERROR, "pg_yaap: cross product probe missing output column mapping");

	InitializeHashJoinFastProbeState(op_state.output_state,
		output_columns,
		output_column_count,
		build_row_layout,
		nullptr,
		nullptr,
		HashJoinMatchMode::INNER,
		0,
		0,
		0);

	out.reset();
	const uint32_t build_row_count = pg_atomic_read_u32(&build_rows->row_count);
	while (op_state.probe_row_idx < in.count && out.count < PIPELINE_DEFAULT_CHUNK_SIZE)
	{
		if (PipelineCancelRequestedEvery(ctx, op_state.probe_row_idx))
			break;
		uint16_t batch_count = 0;
		uint16_t matched_probe_rows[PIPELINE_DEFAULT_CHUNK_SIZE];
		const uint8_t *matched_build_rows[PIPELINE_DEFAULT_CHUNK_SIZE];
		const TupleDataCollection *matched_build_row_tdcs[PIPELINE_DEFAULT_CHUNK_SIZE];
		while (op_state.build_row_idx < build_row_count &&
			   out.count + batch_count < PIPELINE_DEFAULT_CHUNK_SIZE)
		{
			matched_probe_rows[batch_count] = op_state.probe_row_idx;
			matched_build_rows[batch_count] = TupleDataCollectionGetRowConst(build_rows, op_state.build_row_idx);
			matched_build_row_tdcs[batch_count] = build_rows;
			++batch_count;
			++op_state.build_row_idx;
		}
		if (batch_count > 0)
		{
			CopyRowsByResolvedMappingBatch(in,
				matched_probe_rows,
				matched_build_row_tdcs,
				matched_build_rows,
				op_state.output_state.resolved_output_columns.data(),
				op_state.output_state.output_column_count,
				out,
				out.count,
				batch_count);
			out.count += batch_count;
		}
		if (op_state.build_row_idx >= build_row_count)
		{
			op_state.build_row_idx = 0;
			++op_state.probe_row_idx;
		}
	}

	if (op_state.probe_row_idx >= in.count)
	{
		op_state.probe_row_idx = 0;
		op_state.build_row_idx = 0;
		op_state.current_input_drained = out.count > 0;
	}

	if (out.count > 0 && pg_yaap_trace_execution_path)
		ereport(LOG,
			(errmsg("pg_yaap crossproduct execute worker=%d probe_rows=%u out_rows=%u build_rows=%u",
				ctx.worker_index,
				in.count,
				out.count,
				build_row_count)));
	return out.count > 0 ? OperatorResultType::HAVE_MORE_OUTPUT : OperatorResultType::NEED_MORE_INPUT;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
