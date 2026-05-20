#include "parallel/pipeline/pipeline_run_event.hpp"

#include "parallel/pipeline/dsm_task_queue.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/task_scheduler.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

PipelineRunEvent::PipelineRunEvent(PipelineId pid, Pipeline *pipeline,
                                   TaskScheduler *scheduler)
    : Event(pid, scheduler), pipeline_(pipeline) {}

void
PipelineRunEvent::Schedule()
{
	Assert(pipeline_ != nullptr);

	/*
	 * Leader pre-init of source + sink GlobalState (DuckDB-faithful;
	 * mirrors duckdb Pipeline::Schedule calling Pipeline::ResetSink and
	 * Pipeline::ResetSource before LaunchScanTasks). Schedule() runs
	 * single-threaded on the leader before tasks fan out, so no atomics
	 * are needed.
	 *
	 * Each Get*State has a leader-only branch (worker_index ==
	 * LEADER_WORKER_INDEX) that dsa_allocate0's the shared payload and
	 * StoreSharedPayloadOnDescriptor()s its dsa pointer. With
	 * pg_yaap.parallel_leader_participation=false, no worker ever takes
	 * that branch, so without this call the payload stays
	 * InvalidDsaPointer and workers ereport "shared payload not
	 * initialized". Returned unique_ptrs are intentionally dropped:
	 * durable state lives in DSA via the descriptor; per-task wrappers
	 * are rebuilt by task.cpp:EnsureGlobalStates.
	 */
	ExecCtx leader_ctx;
	leader_ctx.mcxt         = scheduler_->mcxt();
	leader_ctx.dsa          = scheduler_->dsa();
	leader_ctx.worker_index = LEADER_WORKER_INDEX;
	leader_ctx.control      = scheduler_->control();

	if (pipeline_->source != nullptr)
		(void) pipeline_->source->GetGlobalSourceState(leader_ctx);
	if (pipeline_->sink != nullptr)
		(void) pipeline_->sink->GetGlobalSinkState(leader_ctx);

	scheduler_->EnqueueTasks(*this);
}

TaskKind
PipelineRunEvent::kind() const
{
	return TaskKind::RUN;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
