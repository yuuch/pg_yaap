#include "parallel/pipeline/physical_delim_scan.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"

extern int pg_yaap_parallel_max_workers;
extern bool pg_yaap_trace_execution_path;
}

#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"
#include "parallel/pipeline/dsm_control.hpp"
#include "buffer_manager/yaap_shared_segment_arena.hpp"
#include "buffer_manager/yaap_spill_store.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

static HashAggSharedPayload *
ResolvePayload(dsa_area *dsa, dsa_pointer payload_dp)
{
	return DsaPointerIsValid(payload_dp)
		? static_cast<HashAggSharedPayload *>(dsa_get_address(dsa, payload_dp))
		: nullptr;
}

static HashAggPartition *
ResolvePartitions(dsa_area *dsa, HashAggSharedPayload *payload)
{
	if (payload == nullptr || !DsaPointerIsValid(payload->partitions_dp))
		return nullptr;
	return static_cast<HashAggPartition *>(dsa_get_address(dsa, payload->partitions_dp));
}

static TupleDataCollection *
ResolveTdc(dsa_area *dsa, dsa_pointer tdc_dp)
{
	return DsaPointerIsValid(tdc_dp)
		? static_cast<TupleDataCollection *>(dsa_get_address(dsa, tdc_dp))
		: nullptr;
}

static const TupleDataLayout *
ResolveLayout(dsa_area *dsa, dsa_pointer layout_dp)
{
	return DsaPointerIsValid(layout_dp)
		? static_cast<const TupleDataLayout *>(dsa_get_address(dsa, layout_dp))
		: nullptr;
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
EnsureDelimSegmentResident(ExecCtx &ctx, dsa_pointer arena_dp,
							   buffer::YaapSharedSegmentDesc *segment)
{
	if (segment == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: delim segment descriptor missing")));
	if (pg_atomic_read_u32(&segment->state) ==
		static_cast<uint32_t>(buffer::YaapSharedSegmentState::SPILLED))
	{
		if (ctx.control == nullptr)
			ereport(ERROR, (errmsg("pg_yaap: delim spill control missing")));
		buffer::YaapSpillStore spill(ctx.mcxt);
		spill.AttachSharedFileSet(&ctx.control->spill_fileset,
			segment->spill_extent.file_name);
		const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id,
			segment->generation};
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
					ereport(ERROR, (errmsg("pg_yaap: delim segment reload entered invalid state")));
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
		ereport(ERROR, (errmsg("pg_yaap: delim segment is not readable")));
}

static void
RetireDelimSegmentChain(ExecCtx &ctx, HashAggPartition &part)
{
	if (!part.segment_backed)
		return;
	auto *state = ResolveBufferState(ctx);
	if (state == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: delim segment retire state missing")));
	for (dsa_pointer desc_dp = part.segment_first_desc_dp;
		 DsaPointerIsValid(desc_dp);)
	{
		auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
			dsa_get_address(ctx.dsa, desc_dp));
		if (segment == nullptr)
			ereport(ERROR, (errmsg("pg_yaap: delim segment retire descriptor missing")));
		const dsa_pointer next_dp = segment->chain_next_dp;
		const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id,
			segment->generation};
		if (!buffer::YaapSharedSegmentRetire(ctx.dsa, state, part.segment_arena_dp, handle))
			ereport(ERROR, (errmsg("pg_yaap: delim segment retire failed")));
		desc_dp = next_dp;
	}
	part.segment_backed = false;
}

}  // namespace

int
PhysicalDelimScan::MaxThreads(ExecCtx &ctx) const
{
	const dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	HashAggSharedPayload *payload = ResolvePayload(ctx.dsa, payload_dp);
	if (payload == nullptr || payload->partition_count == 0)
		return 1;
	return std::max(1, std::min(pg_yaap_parallel_max_workers,
		static_cast<int>(payload->partition_count)));
}

void
PhysicalDelimScan::BuildPipelines(Pipeline &current, MetaPipeline &meta)
{
	meta.SetSource(current, *this);
	if (!producer_sink_)
		return;

	Pipeline &producer = meta.CreateChildPipeline(current, *producer_sink_);
	meta.SetSink(producer, *producer_sink_);
	producer.source = nullptr;
	Assert(producer_sink_->children().size() == 1);
	producer_sink_->children()[0]->BuildPipelines(producer, meta);
}

std::unique_ptr<GlobalSourceState>
PhysicalDelimScan::GetGlobalSourceState(ExecCtx &ctx)
{
	auto state = std::make_unique<DelimScanGlobalSourceState>();
	const dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	state->payload = ResolvePayload(ctx.dsa, payload_dp);
	state->partitions = ResolvePartitions(ctx.dsa, state->payload);
	state->partition_count = state->payload != nullptr ? state->payload->partition_count : 0;
	if (state->payload == nullptr || state->partitions == nullptr || state->partition_count == 0)
		elog(ERROR, "pg_yaap: delim scan source payload not initialized");

	TupleDataCollection *first_tdc = ResolveTdc(ctx.dsa, state->partitions[0].tdc_dp);
	if (first_tdc != nullptr)
		state->layout = ResolveLayout(ctx.dsa, first_tdc->layout_dp);
	else if (state->partitions[0].segment_backed &&
		DsaPointerIsValid(state->partitions[0].segment_first_desc_dp))
	{
		auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
			dsa_get_address(ctx.dsa, state->partitions[0].segment_first_desc_dp));
		EnsureDelimSegmentResident(ctx, state->partitions[0].segment_arena_dp, segment);
		state->layout = segment != nullptr && DsaPointerIsValid(segment->payload_dp)
			? ResolveLayout(ctx.dsa, static_cast<TupleDataCollection *>(
				dsa_get_address(ctx.dsa, segment->payload_dp))->layout_dp) : nullptr;
	}
	if (state->layout == nullptr)
		elog(ERROR, "pg_yaap: delim scan source layout missing");
	return state;
}

std::unique_ptr<LocalSourceState>
PhysicalDelimScan::GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate)
{
	(void) ctx;
	(void) gstate;
	return std::make_unique<DelimScanLocalSourceState>();
}

SourceResultType
PhysicalDelimScan::GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input)
{
	auto &global = static_cast<DelimScanGlobalSourceState &>(input.global_state);
	auto &local = static_cast<DelimScanLocalSourceState &>(input.local_state);
	out.reset();
	if (pg_yaap_trace_execution_path)
		elog(LOG,
			 "pg_yaap delim scan enter op=%p source_partition=%u partition_count=%u finalized=%d",
			 static_cast<void *>(this),
			 local.source_partition,
			 global.partition_count,
			 global.payload != nullptr && global.payload->finalized ? 1 : 0);

	/* Like DuckDB's delim scan, the producer side must be fully finalized
	 * before consumer workers start claiming partitions. */
	if (!global.payload->finalized)
		return SourceResultType::FINISHED;

	while (out.count < PIPELINE_DEFAULT_CHUNK_SIZE)
	{
		if (local.segment_backed && local.source_partition != UINT32_MAX &&
			!DsaPointerIsValid(local.segment_desc_dp))
		{
			RetireDelimSegmentChain(ctx, global.partitions[local.source_partition]);
			local.segment_backed = false;
		}
		if (local.tdc == nullptr || local.source_cursor >= local.row_count)
		{
			const uint32_t claimed = pg_atomic_fetch_add_u32(&global.payload->source_partition_next, 1);
			if (claimed >= global.partition_count)
				break;
			local.source_partition = claimed;
			local.source_cursor = 0;
			local.tdc = nullptr;
			local.segment_desc_dp = InvalidDsaPointer;
			local.segment_row_cursor = 0;
			local.segment_backed = global.partitions[claimed].segment_backed;
			if (local.segment_backed)
				local.segment_desc_dp = global.partitions[claimed].segment_first_desc_dp;
			else
			{
				local.tdc = ResolveTdc(ctx.dsa, global.partitions[claimed].tdc_dp);
				if (local.tdc == nullptr)
					elog(ERROR, "pg_yaap: delim scan partition payload missing");
				if (!local.tdc->finalized)
					elog(ERROR, "pg_yaap: delim scan partition not finalized before scan");
				local.row_count = pg_atomic_read_u32(&local.tdc->row_count);
			}
		}

		if (DsaPointerIsValid(local.segment_desc_dp))
		{
			auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
				dsa_get_address(ctx.dsa, local.segment_desc_dp));
			EnsureDelimSegmentResident(ctx, global.partitions[local.source_partition].segment_arena_dp,
				segment);
			auto *segment_tdc = static_cast<TupleDataCollection *>(
				dsa_get_address(ctx.dsa, segment->payload_dp));
			const uint32_t segment_rows = pg_atomic_read_u32(&segment_tdc->row_count);
			if (local.segment_row_cursor >= segment_rows)
			{
				local.segment_desc_dp = segment->chain_next_dp;
				local.segment_row_cursor = 0;
				continue;
			}
			const uint8_t *row = TupleDataCollectionGetRowConst(segment_tdc,
				local.segment_row_cursor++);
			Gather(global.layout, segment_tdc, row, out, out.count);
			++out.count;
			continue;
		}

		if (local.source_cursor >= local.row_count)
		{
			local.tdc = nullptr;
			continue;
		}

		const uint8_t *row = TupleDataCollectionGetRowConst(local.tdc, local.source_cursor++);
		Gather(global.layout, local.tdc, row, out, out.count);
		++out.count;
	}

	return out.count > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
