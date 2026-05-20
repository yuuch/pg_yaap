#include "parallel/pipeline/pipeline_profile.hpp"

extern "C" {
#include "utils/dsa.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
}

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/types.hpp"

extern "C" {
extern bool pg_yaap_profile;
}

namespace pg_yaap {
namespace pipeline {

namespace {

static constexpr uint32 kStageCount = static_cast<uint32>(PipelineProfileStage::COUNT);
static constexpr uint64 kEventReportMinNs = UINT64CONST(1000000);

struct StageTotals {
	uint64 elapsed_ns = 0;
	uint64 calls = 0;
	uint64 rows = 0;
};

uint32
ProfileWorkerSlot(int worker_index)
{
	return worker_index == LEADER_WORKER_INDEX ? 0u : static_cast<uint32>(worker_index + 1);
}

const char *
StageName(PipelineProfileStage stage)
{
	switch (stage)
	{
		case PipelineProfileStage::TOTAL: return "total";
		case PipelineProfileStage::TASK_RUN_TOTAL: return "task_run_total";
		case PipelineProfileStage::TASK_COMBINE_TOTAL: return "task_combine_total";
		case PipelineProfileStage::TASK_FINALIZE_TOTAL: return "task_finalize_total";
		case PipelineProfileStage::SOURCE_SEQ_SCAN: return "source_seq_scan";
		case PipelineProfileStage::SOURCE_HASH_AGG: return "source_hashagg_readback";
		case PipelineProfileStage::SOURCE_PERFECT_HASH_AGG: return "source_perfect_hashagg_readback";
		case PipelineProfileStage::SOURCE_ORDER: return "source_order_readback";
		case PipelineProfileStage::OP_FILTER: return "filter_expr";
		case PipelineProfileStage::OP_PROJECTION: return "project_expr";
		case PipelineProfileStage::OP_HASH_JOIN: return "hash_join_probe";
		case PipelineProfileStage::SINK_HASH_AGG_UPDATE: return "agg_update_local";
		case PipelineProfileStage::SINK_PERFECT_HASH_AGG_UPDATE: return "perfect_agg_update_local";
		case PipelineProfileStage::SINK_ORDER_APPEND: return "order_sink_append";
		case PipelineProfileStage::SINK_OUTPUT_APPEND: return "output_sink_append";
		case PipelineProfileStage::COMBINE_HASH_AGG: return "agg_combine_global";
		case PipelineProfileStage::COMBINE_PERFECT_HASH_AGG: return "perfect_agg_combine_global";
		case PipelineProfileStage::FINALIZE_HASH_JOIN: return "hash_join_finalize_global";
		case PipelineProfileStage::FINALIZE_HASH_AGG: return "agg_finalize_global";
		case PipelineProfileStage::FINALIZE_PERFECT_HASH_AGG: return "perfect_agg_finalize_global";
		case PipelineProfileStage::FINALIZE_ORDER: return "order_finalize_sort";
		case PipelineProfileStage::FINALIZE_OUTPUT: return "output_finalize";
		case PipelineProfileStage::OUTPUT_EMIT: return "output_emit_to_dest";
		case PipelineProfileStage::SCAN_BLOCK_FETCH: return "scan_block_fetch";
		case PipelineProfileStage::SCAN_QUAL_DEFORM: return "scan_qual_deform";
		case PipelineProfileStage::SCAN_FILTER: return "scan_filter";
		case PipelineProfileStage::SCAN_PROJ_DEFORM: return "scan_proj_deform";
		case PipelineProfileStage::SCAN_LOAD_PAGE: return "scan_load_page";
		case PipelineProfileStage::SCAN_PREPARE_PAGE: return "scan_prepare_pagescan";
		case PipelineProfileStage::SCAN_VISIBLE_TUPLE: return "scan_visible_tuple_iter";
		case PipelineProfileStage::LEADER_WAIT_READY: return "leader_wait_worker_ready";
		case PipelineProfileStage::LEADER_WAIT_EVENT: return "leader_wait_event";
		case PipelineProfileStage::WORKER_WAIT_TASK: return "worker_wait_task";
		case PipelineProfileStage::COUNT: break;
	}
	return "unknown";
}

PipelineProfileRecord *
ProfileRecords(PipelineSharedControl *control, dsa_area *dsa)
{
	if (control == nullptr || dsa == nullptr ||
		!DsaPointerIsValid(control->profile_records_root))
		return nullptr;
	return static_cast<PipelineProfileRecord *>(
		dsa_get_address(dsa, control->profile_records_root));
}

PipelineProfileRecord *
ProfileRecord(PipelineSharedControl *control,
			  dsa_area *dsa,
			  int worker_index,
			  EventId event_id,
			  PipelineProfileStage stage)
{
	PipelineProfileRecord *records = ProfileRecords(control, dsa);
	if (records == nullptr || event_id >= control->profile_event_count)
		return nullptr;

	const uint32 slot = ProfileWorkerSlot(worker_index);
	if (slot >= control->profile_worker_slots)
		return nullptr;

	const uint32 stage_idx = static_cast<uint32>(stage);
	if (stage_idx >= kStageCount)
		return nullptr;

	const uint64 idx =
		((uint64) slot * control->profile_event_count + event_id) * kStageCount + stage_idx;
	return &records[idx];
}

PipelineProfileSlotInfo *
ProfileSlotInfos(PipelineSharedControl *control, dsa_area *dsa)
{
	if (control == nullptr || dsa == nullptr ||
		!DsaPointerIsValid(control->profile_slot_pids_root))
		return nullptr;
	return static_cast<PipelineProfileSlotInfo *>(
		dsa_get_address(dsa, control->profile_slot_pids_root));
}

const char *
SlotRoleName(int worker_index)
{
	return worker_index == LEADER_WORKER_INDEX ? "leader" : "worker";
}

void
AccumulateStageTotals(StageTotals *totals,
				  uint32 stage_idx,
				  const PipelineProfileRecord &record)
{
	totals[stage_idx].elapsed_ns += record.elapsed_ns;
	totals[stage_idx].calls += record.calls;
	totals[stage_idx].rows += record.rows;
}

const char *
OpKindName(OpKind kind)
{
	switch (kind)
	{
		case OpKind::SEQ_SCAN: return "SEQ_SCAN";
		case OpKind::DELIM_SCAN: return "DELIM_SCAN";
		case OpKind::HASH_AGGREGATE: return "HASH_AGGREGATE";
		case OpKind::PERFECT_HASH_AGGREGATE: return "PERFECT_HASH_AGGREGATE";
		case OpKind::HASH_JOIN: return "HASH_JOIN";
		case OpKind::CROSS_PRODUCT: return "CROSS_PRODUCT";
		case OpKind::ORDER: return "ORDER";
		case OpKind::TOP_N: return "TOP_N";
		case OpKind::OUTPUT: return "OUTPUT";
		case OpKind::FILTER: return "FILTER";
		case OpKind::PROJECTION: return "PROJECTION";
	}
	return "UNKNOWN";
}

const char *
HashJoinModeName(HashJoinMatchMode mode)
{
	switch (mode)
	{
		case HashJoinMatchMode::INNER: return "INNER";
		case HashJoinMatchMode::SEMI: return "SEMI";
		case HashJoinMatchMode::ANTI: return "ANTI";
		case HashJoinMatchMode::LEFT: return "LEFT";
	}
	return "UNKNOWN";
}

const char *
EventKindName(uint32 event_id)
{
	switch (event_id % 3u)
	{
		case 0: return "run";
		case 1: return "combine";
		case 2: return "finalize";
	}
	return "unknown";
}

std::string
DescribeOp(const OpDescriptor &op)
{
	switch (op.kind)
	{
		case OpKind::SEQ_SCAN:
		{
			std::string desc = "SEQ_SCAN";
			char *relname = get_rel_name(op.body.seq_scan.relid);
			if (relname != nullptr)
			{
				desc += "(";
				desc += relname;
				desc += ")";
				pfree(relname);
			}
			return desc;
		}
		case OpKind::HASH_JOIN:
		{
			std::string desc = "HASH_JOIN(";
			desc += HashJoinModeName(op.body.hash_join.join_mode);
			desc += ")";
			return desc;
		}
		case OpKind::CROSS_PRODUCT:
			return "CROSS_PRODUCT";
		default:
			return OpKindName(op.kind);
	}
}

std::string
DescribeEvent(PipelineSharedControl *control, dsa_area *dsa, uint32 event_id)
{
	const uint32 pipeline_id = event_id / 3u;
	if (control == nullptr || dsa == nullptr ||
		!DsaPointerIsValid(control->pipelines_root) ||
		pipeline_id >= static_cast<uint32>(control->num_pipelines))
	{
		return "pipeline=? kind=" + std::string(EventKindName(event_id));
	}

	auto *root = static_cast<PipelineDescriptor *>(dsa_get_address(dsa, control->pipelines_root));
	if (root == nullptr)
		return "pipeline=? kind=" + std::string(EventKindName(event_id));
	const PipelineDescriptor &pd = root[pipeline_id];
	if (!DsaPointerIsValid(pd.ops) || pd.op_count <= 0)
		return "pipeline=" + std::to_string(pipeline_id) + " kind=" + EventKindName(event_id);

	auto *ops = static_cast<OpDescriptor *>(dsa_get_address(dsa, pd.ops));
	if (ops == nullptr)
		return "pipeline=" + std::to_string(pipeline_id) + " kind=" + EventKindName(event_id);

	std::string desc = "pipeline=" + std::to_string(pipeline_id);
	desc += " kind=";
	desc += EventKindName(event_id);
	desc += " source=";
	desc += DescribeOp(ops[0]);
	desc += " sink=";
	desc += DescribeOp(ops[pd.op_count - 1]);
	return desc;
}

}  /* namespace */

bool
PipelineProfileAllocate(PipelineSharedControl *control,
						 dsa_area *dsa,
						 uint32 event_count,
						 uint32 num_workers)
{
	if (control == nullptr || dsa == nullptr)
		return false;

	control->profile_records_root = InvalidDsaPointer;
	control->profile_slot_pids_root = InvalidDsaPointer;
	control->profile_event_count = event_count;
	control->profile_worker_slots = num_workers + 1u;
	pg_atomic_write_u32(&control->profile_enabled, 0u);

	if (!pg_yaap_profile || event_count == 0)
		return true;

	const Size nrecords = static_cast<Size>(control->profile_worker_slots) *
		static_cast<Size>(event_count) * static_cast<Size>(kStageCount);
	const Size bytes = nrecords * sizeof(PipelineProfileRecord);
	const Size slot_info_bytes = static_cast<Size>(control->profile_worker_slots) *
		sizeof(PipelineProfileSlotInfo);
	control->profile_records_root = dsa_allocate0(dsa, bytes);
	control->profile_slot_pids_root = dsa_allocate0(dsa, slot_info_bytes);
	PipelineProfileSlotInfo *slot_infos = ProfileSlotInfos(control, dsa);
	if (slot_infos != nullptr)
	{
		for (uint32 slot = 0; slot < control->profile_worker_slots; ++slot)
		{
			slot_infos[slot].pid = 0;
			slot_infos[slot].worker_index =
				(slot == 0) ? LEADER_WORKER_INDEX : static_cast<int32>(slot - 1);
		}
	}
	pg_atomic_write_u32(&control->profile_enabled, 1u);
	return true;
}

bool
PipelineProfileEnabled(const ExecCtx &ctx)
{
	return ctx.control != nullptr &&
		pg_atomic_read_u32(&ctx.control->profile_enabled) != 0;
}

void
PipelineProfileRegisterProcess(PipelineSharedControl *control,
						 dsa_area *dsa,
						 int worker_index,
						 int pid)
{
	if (control == nullptr || dsa == nullptr ||
		pg_atomic_read_u32(&control->profile_enabled) == 0)
		return;

	const uint32 slot = ProfileWorkerSlot(worker_index);
	if (slot >= control->profile_worker_slots)
		return;

	PipelineProfileSlotInfo *slot_infos = ProfileSlotInfos(control, dsa);
	if (slot_infos == nullptr)
		return;

	slot_infos[slot].pid = pid;
	slot_infos[slot].worker_index = worker_index;
}

void
PipelineProfileAddElapsed(PipelineSharedControl *control,
						   dsa_area *dsa,
						   int worker_index,
						   EventId event_id,
						   PipelineProfileStage stage,
						   instr_time elapsed,
						   uint64 rows)
{
	if (control == nullptr || pg_atomic_read_u32(&control->profile_enabled) == 0)
		return;

	PipelineProfileRecord *record = ProfileRecord(control, dsa, worker_index, event_id, stage);
	if (record == nullptr)
		return;

	record->elapsed_ns += (uint64) INSTR_TIME_GET_NANOSEC(elapsed);
	record->calls += 1;
	record->rows += rows;
}

void
PipelineProfileAddDiff(const ExecCtx &ctx,
						PipelineProfileStage stage,
						instr_time end,
						instr_time start,
						uint64 rows)
{
	instr_time elapsed = end;
	INSTR_TIME_SUBTRACT(elapsed, start);
	PipelineProfileAddElapsed(ctx.control,
						  ctx.dsa,
						  ctx.worker_index,
						  ctx.profile_event_id,
						  stage,
						  elapsed,
						  rows);
}

void
PipelineProfileReport(PipelineSharedControl *control, dsa_area *dsa)
{
	if (control == nullptr || dsa == nullptr ||
		pg_atomic_read_u32(&control->profile_enabled) == 0)
		return;

	PipelineProfileRecord *records = ProfileRecords(control, dsa);
	if (records == nullptr)
		return;

	const TimestampTz report_ts = GetCurrentTimestamp();

	elog(NOTICE,
		 "pg_yaap[timing] report_at=%s workers=%u worker_slots=%u events=%u stages=%u",
		 timestamptz_to_str(report_ts),
		 control->profile_worker_slots > 0 ? control->profile_worker_slots - 1 : 0,
		 control->profile_worker_slots,
		 control->profile_event_count,
		 kStageCount);

	PipelineProfileSlotInfo *slot_infos = ProfileSlotInfos(control, dsa);
	for (uint32 slot = 0; slot < control->profile_worker_slots; ++slot)
	{
		int worker_index = (slot == 0) ? LEADER_WORKER_INDEX : static_cast<int>(slot - 1);
		int pid = 0;
		if (slot_infos != nullptr)
		{
			worker_index = slot_infos[slot].worker_index;
			pid = slot_infos[slot].pid;
		}
		elog(NOTICE,
			 "pg_yaap[timing] slot=%u role=%s worker_index=%d pid=%d",
			 slot,
			 SlotRoleName(worker_index),
			 worker_index,
			 pid);
	}

	StageTotals grand_totals[kStageCount] = {};
	std::vector<StageTotals> event_totals(static_cast<size_t>(control->profile_event_count) * kStageCount);
	std::vector<std::string> event_labels(control->profile_event_count);

	for (uint32 event_id = 0; event_id < control->profile_event_count; ++event_id)
		event_labels[event_id] = DescribeEvent(control, dsa, event_id);

	for (uint32 slot = 0; slot < control->profile_worker_slots; ++slot)
	{
		for (uint32 event_id = 0; event_id < control->profile_event_count; ++event_id)
		{
			for (uint32 stage_idx = 0; stage_idx < kStageCount; ++stage_idx)
			{
				const uint64 idx =
					((uint64) slot * control->profile_event_count + event_id) * kStageCount + stage_idx;
				const PipelineProfileRecord &record = records[idx];
				if (record.calls == 0 && record.elapsed_ns == 0)
					continue;
				StageTotals &total = event_totals[static_cast<size_t>(event_id) * kStageCount + stage_idx];
				total.elapsed_ns += record.elapsed_ns;
				total.calls += record.calls;
				total.rows += record.rows;
			}
		}
	}

	for (uint32 stage_idx = 0; stage_idx < kStageCount; ++stage_idx)
	{
		uint64 sum_ns = 0;
		uint64 max_slot_ns = 0;
		uint64 calls = 0;
		uint64 rows = 0;

		for (uint32 slot = 0; slot < control->profile_worker_slots; ++slot)
		{
			uint64 slot_ns = 0;
			uint64 slot_calls = 0;
			uint64 slot_rows = 0;
			for (uint32 event_id = 0; event_id < control->profile_event_count; ++event_id)
			{
				const uint64 idx =
					((uint64) slot * control->profile_event_count + event_id) * kStageCount + stage_idx;
				const PipelineProfileRecord &record = records[idx];
				slot_ns += record.elapsed_ns;
				slot_calls += record.calls;
				slot_rows += record.rows;
				sum_ns += record.elapsed_ns;
				calls += record.calls;
				rows += record.rows;
				AccumulateStageTotals(grand_totals, stage_idx, record);
			}
			max_slot_ns = std::max(max_slot_ns, slot_ns);

			if (slot_calls == 0 && slot_ns == 0)
				continue;

			int worker_index = (slot == 0) ? LEADER_WORKER_INDEX : static_cast<int>(slot - 1);
			int pid = 0;
			if (slot_infos != nullptr)
			{
				worker_index = slot_infos[slot].worker_index;
				pid = slot_infos[slot].pid;
			}

			elog(NOTICE,
				 "pg_yaap[timing] role=%s worker_index=%d pid=%d stage=%s total_ms=%.3f calls=" UINT64_FORMAT " rows=" UINT64_FORMAT,
				 SlotRoleName(worker_index),
				 worker_index,
				 pid,
				 StageName(static_cast<PipelineProfileStage>(stage_idx)),
				 (double) slot_ns / 1000000.0,
				 slot_calls,
				 slot_rows);
		}

		if (calls == 0 && sum_ns == 0)
			continue;

		elog(NOTICE,
			 "pg_yaap[timing] stage=%s sum_ms=%.3f max_slot_ms=%.3f calls=" UINT64_FORMAT " rows=" UINT64_FORMAT,
			 StageName(static_cast<PipelineProfileStage>(stage_idx)),
			 (double) sum_ns / 1000000.0,
			 (double) max_slot_ns / 1000000.0,
			 calls,
			 rows);
	}

	for (uint32 stage_idx = 0; stage_idx < kStageCount; ++stage_idx)
	{
		const StageTotals &total = grand_totals[stage_idx];
		if (total.calls == 0 && total.elapsed_ns == 0)
			continue;

		elog(NOTICE,
			 "pg_yaap[timing] overall stage=%s total_ms=%.3f calls=" UINT64_FORMAT " rows=" UINT64_FORMAT,
			 StageName(static_cast<PipelineProfileStage>(stage_idx)),
			 (double) total.elapsed_ns / 1000000.0,
			 total.calls,
			 total.rows);
	}

	for (uint32 event_id = 0; event_id < control->profile_event_count; ++event_id)
	{
		for (uint32 stage_idx = 0; stage_idx < kStageCount; ++stage_idx)
		{
			const StageTotals &total =
				event_totals[static_cast<size_t>(event_id) * kStageCount + stage_idx];
			if ((total.calls == 0 && total.elapsed_ns == 0) || total.elapsed_ns < kEventReportMinNs)
				continue;

			elog(NOTICE,
				 "pg_yaap[timing] event=%u %s stage=%s total_ms=%.3f calls=" UINT64_FORMAT " rows=" UINT64_FORMAT,
				 event_id,
				 event_labels[event_id].c_str(),
				 StageName(static_cast<PipelineProfileStage>(stage_idx)),
				 (double) total.elapsed_ns / 1000000.0,
				 total.calls,
				 total.rows);
		}
	}
}

PipelineProfileStage
PipelineProfileSourceStage(PhysicalOperatorType type)
	{
		switch (type)
		{
			case PhysicalOperatorType::SEQ_SCAN: return PipelineProfileStage::SOURCE_SEQ_SCAN;
			case PhysicalOperatorType::HASH_AGGREGATE: return PipelineProfileStage::SOURCE_HASH_AGG;
			case PhysicalOperatorType::PERFECT_HASH_AGGREGATE: return PipelineProfileStage::SOURCE_PERFECT_HASH_AGG;
			case PhysicalOperatorType::ORDER: return PipelineProfileStage::SOURCE_ORDER;
			case PhysicalOperatorType::TOP_N: return PipelineProfileStage::SOURCE_ORDER;
		default: return PipelineProfileStage::TASK_RUN_TOTAL;
	}
}

PipelineProfileStage
PipelineProfileOperatorStage(PhysicalOperatorType type)
{
	switch (type)
	{
		case PhysicalOperatorType::PROJECTION:
			return PipelineProfileStage::OP_PROJECTION;
		case PhysicalOperatorType::FILTER:
			return PipelineProfileStage::OP_FILTER;
		case PhysicalOperatorType::HASH_JOIN:
			return PipelineProfileStage::OP_HASH_JOIN;
		case PhysicalOperatorType::CROSS_PRODUCT:
			return PipelineProfileStage::OP_HASH_JOIN;
		default:
			return PipelineProfileStage::TASK_RUN_TOTAL;
	}
}

PipelineProfileStage
PipelineProfileSinkStage(PhysicalOperatorType type)
	{
		switch (type)
		{
			case PhysicalOperatorType::HASH_AGGREGATE: return PipelineProfileStage::SINK_HASH_AGG_UPDATE;
			case PhysicalOperatorType::PERFECT_HASH_AGGREGATE: return PipelineProfileStage::SINK_PERFECT_HASH_AGG_UPDATE;
			case PhysicalOperatorType::ORDER: return PipelineProfileStage::SINK_ORDER_APPEND;
			case PhysicalOperatorType::TOP_N: return PipelineProfileStage::SINK_ORDER_APPEND;
		case PhysicalOperatorType::OUTPUT: return PipelineProfileStage::SINK_OUTPUT_APPEND;
		default: return PipelineProfileStage::TASK_RUN_TOTAL;
	}
}

PipelineProfileStage
PipelineProfileCombineStage(PhysicalOperatorType type)
{
	if (type == PhysicalOperatorType::HASH_AGGREGATE)
		return PipelineProfileStage::COMBINE_HASH_AGG;
	if (type == PhysicalOperatorType::PERFECT_HASH_AGGREGATE)
		return PipelineProfileStage::COMBINE_PERFECT_HASH_AGG;
	return PipelineProfileStage::TASK_COMBINE_TOTAL;
}

PipelineProfileStage
PipelineProfileFinalizeStage(PhysicalOperatorType type)
	{
		switch (type)
		{
			case PhysicalOperatorType::HASH_JOIN: return PipelineProfileStage::FINALIZE_HASH_JOIN;
			case PhysicalOperatorType::CROSS_PRODUCT: return PipelineProfileStage::FINALIZE_HASH_JOIN;
			case PhysicalOperatorType::HASH_AGGREGATE: return PipelineProfileStage::FINALIZE_HASH_AGG;
			case PhysicalOperatorType::PERFECT_HASH_AGGREGATE: return PipelineProfileStage::FINALIZE_PERFECT_HASH_AGG;
			case PhysicalOperatorType::ORDER: return PipelineProfileStage::FINALIZE_ORDER;
			case PhysicalOperatorType::TOP_N: return PipelineProfileStage::FINALIZE_ORDER;
		case PhysicalOperatorType::OUTPUT: return PipelineProfileStage::FINALIZE_OUTPUT;
		default: return PipelineProfileStage::TASK_FINALIZE_TOTAL;
	}
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
