#include "parallel/pipeline/execution_slots.hpp"
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
	return std::max(1, std::min(static_cast<int>(ExecutionWorkerCount(ctx)),
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

#include "buffer_manager/yaap_segmented_tdc.hpp"
#include "buffer_manager/yaap_spill_store.hpp"
#include "parallel/pipeline/cancel.hpp"
namespace pg_yaap::pipeline {
namespace {
/* Immutable publication nodes; chains are retired once the last consumer RUN finishes. */
struct MaterializeChain { dsa_pointer data_desc; dsa_pointer next; };
struct MaterializeSinkState : GlobalSinkState {};
struct MaterializeLocalSink : LocalSinkState { buffer::YaapSegmentedTdc writer; bool initialized = false; };
struct MaterializeSourceState : GlobalSourceState {};
struct MaterializeLocalSource : LocalSourceState {
 explicit MaterializeLocalSource(dsa_area *dsa) : dsa(dsa) {}
 ~MaterializeLocalSource() override { CloseLease(); }
 void CloseLease() {
  if (has_lease) {
   buffer::YaapSharedSegmentReleaseRead(dsa, arena_dp, &lease);
   has_lease = false;
  }
 }
 dsa_area *dsa;
 dsa_pointer arena_dp = InvalidDsaPointer;
 dsa_pointer current_desc_dp = InvalidDsaPointer;
 buffer::YaapSharedSegmentReadLease lease{};
 uint32_t row_index = 0;
 bool has_lease = false;
 bool chain_active = false;
 bool finished = false;
};

void SpillMaterializeSegments(ExecCtx &ctx, dsa_pointer arena_dp,
                              dsa_pointer first_desc_dp, bool force) {
 if (ctx.control == nullptr || !DsaPointerIsValid(arena_dp) || !DsaPointerIsValid(first_desc_dp))
  return;
 auto *budget = ResolveBufferState(ctx);
 if (budget == nullptr || (!force && !buffer::YaapBufferOverSoftLimit(budget)))
  return;
 for (dsa_pointer dp = first_desc_dp; DsaPointerIsValid(dp);) {
  auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, dp));
  if (segment == nullptr) return;
  const dsa_pointer next_dp = segment->chain_next_dp;
  if (pg_atomic_read_u32(&segment->state) ==
      static_cast<uint32_t>(buffer::YaapSharedSegmentState::PUBLISHED)) {
   buffer::YaapSpillStore spill(ctx.mcxt);
   char prefix[96];
   std::snprintf(prefix, sizeof(prefix), "pg_yaap_mat_%u_%u",
    segment->segment_id, segment->generation);
   spill.AttachSharedFileSet(&ctx.control->spill_fileset, prefix);
   const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id, segment->generation};
   buffer::YaapSpillExtent extent{};
   if (!buffer::YaapSharedSegmentSpill(ctx.dsa, budget, handle, arena_dp, &spill, &extent))
    return;
  }
  dp = next_dp;
 }
}

void EnsureMaterializeSegmentResident(ExecCtx &ctx, dsa_pointer arena_dp,
                                      buffer::YaapSharedSegmentDesc *segment) {
 if (segment == nullptr || !DsaPointerIsValid(arena_dp))
  ereport(ERROR, (errmsg("pg_yaap: materialize segment descriptor missing")));
 const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id, segment->generation};
 for (uint32_t attempt = 0; attempt < 10000; ++attempt) {
  const uint32_t state = pg_atomic_read_u32(&segment->state);
  if ((state == static_cast<uint32_t>(buffer::YaapSharedSegmentState::PUBLISHED) ||
       state == static_cast<uint32_t>(buffer::YaapSharedSegmentState::CONSUMING)) &&
      DsaPointerIsValid(segment->payload_dp))
   return;
  if (state == static_cast<uint32_t>(buffer::YaapSharedSegmentState::SPILLED)) {
   if (ctx.control == nullptr)
    ereport(ERROR, (errmsg("pg_yaap: materialize spill control missing")));
   buffer::YaapSpillStore spill(ctx.mcxt);
   spill.AttachSharedFileSet(&ctx.control->spill_fileset, segment->spill_extent.file_name);
   if (buffer::YaapSharedSegmentReload(ctx.dsa, ResolveBufferState(ctx), handle,
       arena_dp, &spill, segment->spill_extent)) {
    if (pg_atomic_read_u32(&segment->state) ==
        static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL)) {
     buffer::YaapSharedSegmentSeal(segment);
     buffer::YaapSharedSegmentPublish(segment);
    }
    return;
   }
  } else if (state == static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL)) {
   buffer::YaapSharedSegmentSeal(segment);
   buffer::YaapSharedSegmentPublish(segment);
   return;
  } else if (state != static_cast<uint32_t>(buffer::YaapSharedSegmentState::RELOADING) &&
             state != static_cast<uint32_t>(buffer::YaapSharedSegmentState::SPILLING)) {
   ereport(ERROR, (errmsg("pg_yaap: materialize segment in unexpected state %u", state)));
  }
  CHECK_FOR_INTERRUPTS();
  pg_usleep(100L);
 }
 ereport(ERROR, (errmsg("pg_yaap: materialize segment reload timed out")));
}
}
dsa_pointer CreateMaterializeShared(dsa_area *dsa) {
 auto dp = dsa_allocate0(dsa, sizeof(MaterializeShared));
 auto *shared = static_cast<MaterializeShared *>(dsa_get_address(dsa, dp));
 pg_atomic_init_u64(&shared->chains_head, InvalidDsaPointer);
 pg_atomic_init_u32(&shared->remaining_consumers, 0);
 shared->arena_dp = InvalidDsaPointer;
 return dp;
}
dsa_pointer CreateMaterializeScan(dsa_area *dsa) {
 auto dp = dsa_allocate0(dsa, sizeof(MaterializeScanShared));
 auto *scan = static_cast<MaterializeScanShared *>(dsa_get_address(dsa, dp));
 pg_atomic_init_u32(&scan->next_chain, 0);
 return dp;
}
void PhysicalMaterialize::BuildPipelines(Pipeline &current, MetaPipeline &meta) {
 meta.SetSource(current, *this);
 auto *owner = producer ? producer : this;
 if (!owner->producer_pipeline) {
  auto &p = meta.CreateChildPipeline(current, *owner);
  owner->producer_pipeline = &p;
  meta.SetSink(p, *owner); p.source = nullptr;
  Assert(owner->children().size() == 1);
  owner->children()[0]->BuildPipelines(p, meta);
 } else current.depends_on.push_back(owner->producer_pipeline->id);
}
std::unique_ptr<GlobalSinkState> PhysicalMaterialize::GetGlobalSinkState(ExecCtx &) { return std::make_unique<MaterializeSinkState>(); }
std::unique_ptr<LocalSinkState> PhysicalMaterialize::GetLocalSinkState(ExecCtx &, GlobalSinkState &) { return std::make_unique<MaterializeLocalSink>(); }
SinkResultType PhysicalMaterialize::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) {
 auto &local = static_cast<MaterializeLocalSink &>(input.local_state);
 const auto *layout = static_cast<const TupleDataLayout *>(dsa_get_address(ctx.dsa, layout_dp));
 auto *shared = static_cast<MaterializeShared *>(dsa_get_address(ctx.dsa, payload_dp));
 if (!local.initialized) {
  if (!ctx.control || !DsaPointerIsValid(ctx.control->shared_segment_arena_root))
   elog(ERROR, "pg_yaap: materialize requires shared segment arena");
  shared->arena_dp = ctx.control->shared_segment_arena_root;
  uint32_t rows = 256;
  const auto *arena = buffer::YaapSharedSegmentArenaResolve(ctx.dsa, ctx.control->shared_segment_arena_root);
  while (rows > 1 && TupleDataCollectionAllocSize(rows, layout->row_width,
          TupleDataCollectionDefaultHeapCapacity(layout, rows)) > arena->segment_size) rows /= 2;
  uint32_t heap_cap = TupleDataCollectionDefaultHeapCapacity(layout, rows);
  if (heap_cap > 0) {
   const size_t fixed = offsetof(TupleDataCollection, rows) +
    static_cast<size_t>(rows) * layout->row_width;
   if (arena->segment_size > fixed)
    heap_cap = static_cast<uint32_t>(arena->segment_size - fixed);
  }
  buffer::YaapSegmentedTdcInit(&local.writer, ctx.control->shared_segment_arena_root,
    layout_dp, ExecutionTaskSlot(ctx), layout->row_width,
    rows, heap_cap, 0, "delim materialize");
  local.initialized = true;
 }
 auto *budget = ResolveBufferState(ctx);
 for (uint16_t i=0; i<in.count; ++i) {
  if (PipelineCancelRequestedEvery(ctx, i))
   return SinkResultType::FINISHED;
  const uint32_t heap = TupleDataCollectionRequiredHeapBytesForChunkRow(layout, in, i);
  if (local.writer.active.IsValid()) {
   auto *active_store = buffer::YaapSegmentedTdcResolveStore(ctx.dsa,
    buffer::YaapSegmentedRowHandle{local.writer.active, 0}, local.writer.arena_dp);
   if (active_store != nullptr && !TupleDataCollectionHasSpaceForAppend(active_store, heap)) {
    buffer::YaapSegmentedTdcSealPublish(ctx.dsa, &local.writer);
    auto *first_seg = buffer::YaapSharedSegmentResolve(ctx.dsa, local.writer.arena_dp, local.writer.first);
    if (first_seg != nullptr)
     SpillMaterializeSegments(ctx, local.writer.arena_dp, first_seg->self_dp, false);
   }
  }
  buffer::YaapSegmentedRowHandle handle; TupleDataCollection *store=nullptr; uint8_t *row=nullptr;
  if (!buffer::YaapSegmentedTdcAppendWithHeap(ctx.dsa, budget, &local.writer, heap, &handle, &store, &row)) {
   buffer::YaapSegmentedTdcSealPublish(ctx.dsa, &local.writer);
   auto *first_seg = local.writer.first.IsValid()
    ? buffer::YaapSharedSegmentResolve(ctx.dsa, local.writer.arena_dp, local.writer.first) : nullptr;
   if (first_seg != nullptr)
    SpillMaterializeSegments(ctx, local.writer.arena_dp, first_seg->self_dp, true);
   if (!buffer::YaapSegmentedTdcAppendWithHeap(ctx.dsa, budget, &local.writer, heap, &handle, &store, &row))
    elog(ERROR,"pg_yaap: materialize memory budget exhausted");
  }
  Scatter(layout,store,row,in,i);
 }
 return SinkResultType::NEED_MORE_INPUT;
}
SinkCombineResultType PhysicalMaterialize::FinishLocalSink(ExecCtx &ctx, OperatorSinkCombineInput &input) {
 if (input.local_state == nullptr)
  elog(ERROR, "pg_yaap: materialize RUN-local publication has no writer state");
 auto &local=static_cast<MaterializeLocalSink &>(*input.local_state);
 auto *shared=static_cast<MaterializeShared *>(dsa_get_address(ctx.dsa,payload_dp));
 if(local.initialized) {
  buffer::YaapSegmentedTdcSealPublish(ctx.dsa,&local.writer);
  auto *first_seg = local.writer.first.IsValid()
   ? buffer::YaapSharedSegmentResolve(ctx.dsa, local.writer.arena_dp, local.writer.first) : nullptr;
  if (first_seg != nullptr)
   SpillMaterializeSegments(ctx, local.writer.arena_dp, first_seg->self_dp, false);
  auto node_dp = dsa_allocate0(ctx.dsa, sizeof(MaterializeChain));
  auto *node = static_cast<MaterializeChain *>(dsa_get_address(ctx.dsa, node_dp));
  node->data_desc = buffer::YaapSegmentedTdcPublishDesc(ctx.dsa,local.writer);
  uint64_t head = pg_atomic_read_u64(&shared->chains_head);
  do { node->next = static_cast<dsa_pointer>(head); }
  while (!pg_atomic_compare_exchange_u64(&shared->chains_head, &head, node_dp));
 }
 return SinkCombineResultType::FINISHED;
}
std::unique_ptr<GlobalSourceState> PhysicalMaterialize::GetGlobalSourceState(ExecCtx &) { return std::make_unique<MaterializeSourceState>(); }
std::unique_ptr<LocalSourceState> PhysicalMaterialize::GetLocalSourceState(ExecCtx &ctx,GlobalSourceState &) { return std::make_unique<MaterializeLocalSource>(ctx.dsa); }
SourceResultType PhysicalMaterialize::GetData(ExecCtx &ctx,PipelineChunk &out,OperatorSourceInput &input) {
 auto &local=static_cast<MaterializeLocalSource &>(input.local_state);
 out.reset();
 if (local.finished) return SourceResultType::FINISHED;
 auto *shared=static_cast<MaterializeShared *>(dsa_get_address(ctx.dsa,payload_dp));
 auto *scan=static_cast<MaterializeScanShared *>(dsa_get_address(ctx.dsa,scan_dp));
 const auto *layout=static_cast<const TupleDataLayout *>(dsa_get_address(ctx.dsa,layout_dp));
 while(out.count<2048) {
  if (!local.chain_active) {
   /* Each source has its own shared cursor; workers within that source divide
    * the immutable writer chains, while other consumers replay all chains. */
   uint32_t index=pg_atomic_fetch_add_u32(&scan->next_chain,1);
   dsa_pointer node_dp=static_cast<dsa_pointer>(pg_atomic_read_u64(&shared->chains_head));
   while (index-- && DsaPointerIsValid(node_dp))
    node_dp=static_cast<const MaterializeChain *>(dsa_get_address(ctx.dsa,node_dp))->next;
   if (!DsaPointerIsValid(node_dp)) { local.finished=true; break; }
   const auto *node=static_cast<const MaterializeChain *>(dsa_get_address(ctx.dsa,node_dp));
   const auto *desc=buffer::YaapSegmentedTdcResolveDesc(ctx.dsa,node->data_desc);
   if (desc == nullptr || !DsaPointerIsValid(desc->first_desc_dp))
    continue;
   local.arena_dp = desc->arena_dp;
   local.current_desc_dp = desc->first_desc_dp;
   local.row_index = 0;
   local.chain_active = true;
  }
  if (!local.has_lease) {
   if (!DsaPointerIsValid(local.current_desc_dp)) {
    local.chain_active = false;
    continue;
   }
   auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
    dsa_get_address(ctx.dsa, local.current_desc_dp));
   EnsureMaterializeSegmentResident(ctx, local.arena_dp, segment);
   const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id, segment->generation};
   if (!buffer::YaapSharedSegmentAcquireRead(ctx.dsa, local.arena_dp, handle, &local.lease))
    ereport(ERROR, (errmsg("pg_yaap: materialize segment read lease failed")));
   local.has_lease = true;
   local.row_index = 0;
  }
  auto *store = const_cast<TupleDataCollection *>(
   static_cast<const TupleDataCollection *>(local.lease.data));
  const uint32_t rows = pg_atomic_read_u32(&store->row_count);
  if (local.row_index >= rows) {
   auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
    dsa_get_address(ctx.dsa, local.current_desc_dp));
   const dsa_pointer next_dp = segment->chain_next_dp;
   local.CloseLease();
   auto *budget = ResolveBufferState(ctx);
   if (budget != nullptr && buffer::YaapBufferOverSoftLimit(budget) &&
       pg_atomic_read_u32(&segment->state) ==
       static_cast<uint32_t>(buffer::YaapSharedSegmentState::PUBLISHED)) {
    buffer::YaapSpillStore spill(ctx.mcxt);
    char prefix[96];
    std::snprintf(prefix, sizeof(prefix), "pg_yaap_mat_%u_%u",
     segment->segment_id, segment->generation);
    spill.AttachSharedFileSet(&ctx.control->spill_fileset, prefix);
    const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id, segment->generation};
    buffer::YaapSpillExtent extent{};
    (void) buffer::YaapSharedSegmentSpill(ctx.dsa, budget, handle, local.arena_dp, &spill, &extent);
   }
   local.current_desc_dp = next_dp;
   if (!DsaPointerIsValid(next_dp))
    local.chain_active = false;
   continue;
  }
  const uint8_t *row = TupleDataCollectionGetRowConst(store, local.row_index++);
  Gather(layout,store,row,out,out.count);
  ++out.count;
 }
 return out.count ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}
void PhysicalMaterialize::ReleaseAfterConsumerRun(ExecCtx &ctx) {
 if (!DsaPointerIsValid(payload_dp))
  return;
 auto *shared = static_cast<MaterializeShared *>(dsa_get_address(ctx.dsa, payload_dp));
 if (shared == nullptr)
  return;
 const uint32_t prev = pg_atomic_fetch_sub_u32(&shared->remaining_consumers, 1);
 Assert(prev > 0);
 if (prev != 1)
  return;
 auto *budget = ResolveBufferState(ctx);
 for (dsa_pointer node_dp = static_cast<dsa_pointer>(pg_atomic_read_u64(&shared->chains_head));
      DsaPointerIsValid(node_dp);) {
  auto *node = static_cast<const MaterializeChain *>(dsa_get_address(ctx.dsa, node_dp));
  const dsa_pointer next_node = node->next;
  const auto *desc = buffer::YaapSegmentedTdcResolveDesc(ctx.dsa, node->data_desc);
  if (desc != nullptr && DsaPointerIsValid(desc->arena_dp)) {
   for (dsa_pointer seg_dp = desc->first_desc_dp; DsaPointerIsValid(seg_dp);) {
    auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, seg_dp));
    const dsa_pointer next_seg = segment->chain_next_dp;
    const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id, segment->generation};
    (void) buffer::YaapSharedSegmentRetire(ctx.dsa, budget, desc->arena_dp, handle);
    seg_dp = next_seg;
   }
  }
  node_dp = next_node;
 }
}
}
