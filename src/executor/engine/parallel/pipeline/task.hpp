#pragma once

/*
 * pipeline/task.hpp  (M-FRAME-MIN step 3d)
 *
 * Task base + 3 subclasses (Run / Combine / Finalize) for the M-FRAME-MIN
 * pipeline runtime locked by PIPELINE_PORT_PLAN.md §15.1 and
 * GLOBAL_LOCAL_STATE_DESIGN.md §8.5.
 *
 * Contract:
 *   - One Event schedules N Tasks (one per worker for Run/Combine; exactly
 *     one for Finalize, leader-only).
 *   - Task::Execute() returns TASK_FINISHED on success or TASK_ERROR on
 *     failure; the calling worker is responsible for invoking
 *     event->FinishEvent() (success) or event->Abort() (failure) exactly
 *     once after the LAST task of that event has finished. Aggregation
 *     across workers is the TaskScheduler's job (3g).
 *   - TASK_NOT_FINISHED is reserved for cooperative re-yield (used by
 *     PipelineRunTask if morsel queue is empty but more workers may push).
 *   - BLOCKED is intentionally absent (forbidden in M-FRAME-MIN per
 *     pipeline/AGENTS.md ANTI-PATTERNS).
 *
 * Tasks own no DSM/DSA resources directly; they borrow Pipeline + Event
 * pointers whose lifetimes are guaranteed by the TaskScheduler (3g).
 */

#include <cstdint>
#include <memory>

extern "C" {
#include "postgres.h"
#include "executor/execdesc.h"
}

#include "core/memory.hpp"
#include "core/data_chunk.hpp"
#include "parallel/pipeline/dsm_task_queue.hpp"
#include "parallel/pipeline/operator.hpp"
#include "parallel/pipeline/pipeline_dsm_lookup.hpp"
#include "parallel/pipeline/sink.hpp"
#include "parallel/pipeline/source.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

class OutputSink;
struct Pipeline;
struct PipelineSharedControl;
struct EventShmState;

struct ProcessPipelineExecState {
	std::unique_ptr<GlobalSourceState> global_source;
	std::unique_ptr<GlobalSinkState> global_sink;
	PgVector<std::unique_ptr<GlobalOperatorState>> global_ops;
	std::unique_ptr<LocalSourceState> local_source;
	std::unique_ptr<LocalSinkState> local_sink;
	PgVector<std::unique_ptr<OperatorState>> local_ops;
	std::unique_ptr<PipelineChunk> run_src_chunk;
	std::unique_ptr<PipelineChunk> run_scratch_a;
	std::unique_ptr<PipelineChunk> run_scratch_b;
	bool run_initialized = false;
	bool combine_done = false;
	bool leader_partial_pending = false;
};

struct WorkerTaskRuntime {
	ExecCtx exec_ctx;
	PipelineSharedControl *control;
	EventShmState *event_shm;
	PipelineDsmLookup<Pipeline> *pipelines;
	PgVector<std::unique_ptr<ProcessPipelineExecState>> per_pipeline;
	QueryDesc *leader_qd = nullptr;
	OutputSink *final_output = nullptr;

	ProcessPipelineExecState &GetOrCreatePipelineState(PipelineId pipeline_id)
	{
		Assert(pipeline_id != INVALID_PIPELINE_ID);
		const size_t idx = static_cast<size_t>(pipeline_id);
		if (per_pipeline.size() <= idx)
			per_pipeline.resize(idx + 1);
		if (!per_pipeline[idx])
			per_pipeline[idx] = std::make_unique<ProcessPipelineExecState>();
		return *per_pipeline[idx];
	}

	ProcessPipelineExecState &GetPipelineState(PipelineId pipeline_id)
	{
		Assert(pipeline_id != INVALID_PIPELINE_ID);
		const size_t idx = static_cast<size_t>(pipeline_id);
		Assert(idx < per_pipeline.size());
		Assert(per_pipeline[idx] != nullptr);
		return *per_pipeline[idx];
	}
};

enum class TaskExecutionResult : uint8_t {
	TASK_FINISHED,
	TASK_NOT_FINISHED,
	TASK_ERROR,
};

class Task : public PgMemoryContextObject {
public:
	Task(EventId event_id, TaskKind kind, Pipeline *pipeline,
	     WorkerTaskRuntime *runtime, int32_t worker_index,
	     uint32_t partition_id = UINT32_MAX);
	virtual ~Task() = default;

	Task(const Task &)            = delete;
	Task &operator=(const Task &) = delete;

	virtual TaskExecutionResult Execute() = 0;

	EventId event_id() const { return event_id_; }
	TaskKind kind() const { return kind_; }
	Pipeline *pipeline() const { return pipeline_; }
	WorkerTaskRuntime *runtime() const { return runtime_; }
	int32_t worker_index() const { return worker_index_; }
	uint32_t partition_id() const { return partition_id_; }

protected:
	EventId event_id_;
	TaskKind kind_;
	Pipeline *pipeline_;
	WorkerTaskRuntime *runtime_;
	int32_t worker_index_;
	uint32_t partition_id_;
};

class PipelineRunTask final : public Task {
public:
	PipelineRunTask(EventId event_id, Pipeline *pipeline,
	                WorkerTaskRuntime *runtime, int32_t worker_index);

	TaskExecutionResult Execute() override;
};

class PipelineCombineTask final : public Task {
public:
	PipelineCombineTask(EventId event_id, Pipeline *pipeline,
	                    WorkerTaskRuntime *runtime, int32_t worker_index,
	                    uint32_t partition_id);

	TaskExecutionResult Execute() override;
};

class PipelineFinalizeTask final : public Task {
public:
	PipelineFinalizeTask(EventId event_id, Pipeline *pipeline,
	                     WorkerTaskRuntime *runtime, int32_t worker_index);

	TaskExecutionResult Execute() override;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
