#pragma once

extern "C" {
#include "postgres.h"
#include "portability/instr_time.h"
}

#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

struct PipelineSharedControl;
enum class PhysicalOperatorType : uint8_t;

struct PipelineProfileSlotInfo {
	int32 pid;
	int32 worker_index;
};

enum class PipelineProfileStage : uint8_t {
	TOTAL = 0,
	TASK_RUN_TOTAL,
	TASK_COMBINE_TOTAL,
	TASK_FINALIZE_TOTAL,
	SOURCE_SEQ_SCAN,
	SOURCE_HASH_AGG,
	SOURCE_PERFECT_HASH_AGG,
	SOURCE_ORDER,
	OP_FILTER,
	OP_PROJECTION,
	OP_HASH_JOIN,
	SINK_HASH_AGG_UPDATE,
	SINK_PERFECT_HASH_AGG_UPDATE,
	SINK_ORDER_APPEND,
	SINK_OUTPUT_APPEND,
	COMBINE_HASH_AGG,
	COMBINE_PERFECT_HASH_AGG,
	FINALIZE_HASH_JOIN,
	FINALIZE_HASH_AGG,
	FINALIZE_PERFECT_HASH_AGG,
	FINALIZE_ORDER,
	FINALIZE_OUTPUT,
	OUTPUT_EMIT,
	SCAN_BLOCK_FETCH,
	SCAN_QUAL_DEFORM,
	SCAN_FILTER,
	SCAN_PROJ_DEFORM,
	SCAN_LOAD_PAGE,
	SCAN_PREPARE_PAGE,
	SCAN_VISIBLE_TUPLE,
	LEADER_WAIT_READY,
	LEADER_WAIT_EVENT,
	WORKER_WAIT_TASK,
	COUNT,
};

struct PipelineProfileRecord {
	uint64 elapsed_ns;
	uint64 calls;
	uint64 rows;
};

bool PipelineProfileAllocate(PipelineSharedControl *control,
							 dsa_area *dsa,
							 uint32 event_count,
							 uint32 num_workers);
bool PipelineProfileEnabled(const ExecCtx &ctx);
void PipelineProfileRegisterProcess(PipelineSharedControl *control,
						 dsa_area *dsa,
						 int worker_index,
						 int pid);
void PipelineProfileAddElapsed(PipelineSharedControl *control,
					   dsa_area *dsa,
					   int worker_index,
						   EventId event_id,
						   PipelineProfileStage stage,
						   instr_time elapsed,
						   uint64 rows = 0);
void PipelineProfileAddDiff(const ExecCtx &ctx,
						PipelineProfileStage stage,
						instr_time end,
						instr_time start,
						uint64 rows = 0);
void PipelineProfileReport(PipelineSharedControl *control, dsa_area *dsa);

class PipelineProfileScope {
public:
	PipelineProfileScope(const ExecCtx &ctx, PipelineProfileStage stage)
		: ctx_(ctx), stage_(stage), active_(PipelineProfileEnabled(ctx))
	{
		if (active_)
			INSTR_TIME_SET_CURRENT(start_);
	}

	~PipelineProfileScope()
	{
		if (!active_)
			return;
		instr_time end;
		INSTR_TIME_SET_CURRENT(end);
		PipelineProfileAddDiff(ctx_, stage_, end, start_, rows_);
	}

	void AddRows(uint64 rows) { rows_ += rows; }

	PipelineProfileScope(const PipelineProfileScope &) = delete;
	PipelineProfileScope &operator=(const PipelineProfileScope &) = delete;

private:
	const ExecCtx &ctx_;
	PipelineProfileStage stage_;
	instr_time start_{};
	uint64 rows_ = 0;
	bool active_ = false;
};

PipelineProfileStage PipelineProfileSourceStage(PhysicalOperatorType type);
PipelineProfileStage PipelineProfileOperatorStage(PhysicalOperatorType type);
PipelineProfileStage PipelineProfileSinkStage(PhysicalOperatorType type);
PipelineProfileStage PipelineProfileCombineStage(PhysicalOperatorType type);
PipelineProfileStage PipelineProfileFinalizeStage(PhysicalOperatorType type);

}  /* namespace pipeline */
}  /* namespace pg_yaap */
