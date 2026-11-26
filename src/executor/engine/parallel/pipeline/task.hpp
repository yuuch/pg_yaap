#pragma once

/* Complete tasks are claimed from the shared queue by any idle executor.
 * RUN owns its private state until completion and publishes sink partials before
 * completing the event counter. Subsequent COMBINE tasks resolve shared data;
 * they never depend on the process that executed RUN. */

#include <cstdint>
#include <memory>

extern "C" {
#include "postgres.h"
}

#include "core/memory.hpp"
#include "core/data_chunk.hpp"
#include "core/chunk_pool.hpp"
#include "parallel/pipeline/dsm_task_queue.hpp"
#include "parallel/pipeline/operator.hpp"
#include "parallel/pipeline/sink.hpp"
#include "parallel/pipeline/source.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

struct Pipeline;
struct PipelineSharedControl;

struct ProcessPipelineExecState : PgMemoryContextObject {
	std::unique_ptr<GlobalSourceState> global_source;
	std::unique_ptr<GlobalSinkState> global_sink;
	PgVector<std::unique_ptr<GlobalOperatorState>> global_ops;
	std::unique_ptr<LocalSourceState> local_source;
	std::unique_ptr<LocalSinkState> local_sink;
	PgVector<std::unique_ptr<OperatorState>> local_ops;
	ChunkLease run_src_chunk;
	ChunkLease run_scratch_a;
	ChunkLease run_scratch_b;
	/* Fused resumable joins must not overwrite a prefix operator input. */
	PgVector<ChunkLease> run_op_outputs;
	uint64_t run_op_output_bytes = 0;
	bool run_initialized = false;
	bool source_exhausted = false;
	size_t final_op_index = 0;
};

struct WorkerTaskRuntime {
	ExecCtx exec_ctx;
	PipelineSharedControl *control;
	PgVector<std::unique_ptr<ProcessPipelineExecState>> per_pipeline;
	LocalChunkPool chunk_pool;

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

};

enum class TaskExecutionResult : uint8_t {
	TASK_FINISHED,
	TASK_ERROR,
};

class Task : public PgMemoryContextObject {
public:
	Task(EventId event_id, TaskKind kind, Pipeline *pipeline,
	     WorkerTaskRuntime *runtime, uint32_t task_index,
	     uint32_t partition_id = UINT32_MAX);
	virtual ~Task() = default;

	Task(const Task &)            = delete;
	Task &operator=(const Task &) = delete;

	virtual TaskExecutionResult Execute() = 0;

	EventId event_id() const { return event_id_; }
	TaskKind kind() const { return kind_; }
	Pipeline *pipeline() const { return pipeline_; }
	WorkerTaskRuntime *runtime() const { return runtime_; }
	uint32_t task_index() const { return task_index_; }
	uint32_t partition_id() const { return partition_id_; }

protected:
	EventId event_id_;
	TaskKind kind_;
	Pipeline *pipeline_;
	WorkerTaskRuntime *runtime_;
	uint32_t task_index_;
	uint32_t partition_id_;
};

class PipelineRunTask final : public Task {
public:
	PipelineRunTask(EventId event_id, Pipeline *pipeline,
	                WorkerTaskRuntime *runtime, uint32_t task_index);

	TaskExecutionResult Execute() override;
};

class PipelineCombineTask final : public Task {
public:
	PipelineCombineTask(EventId event_id, Pipeline *pipeline,
	                    WorkerTaskRuntime *runtime, uint32_t task_index,
	                    uint32_t partition_id);

	TaskExecutionResult Execute() override;
};

class PipelineFinalizeTask final : public Task {
public:
	PipelineFinalizeTask(EventId event_id, Pipeline *pipeline,
						 WorkerTaskRuntime *runtime, uint32_t task_index,
						 uint32_t partition_id = UINT32_MAX);

	TaskExecutionResult Execute() override;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
