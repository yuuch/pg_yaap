#include "parallel/pipeline/task.hpp"

#include <cstdint>

extern "C" {
#include "miscadmin.h"
}

#include "core/data_chunk.hpp"
#include "buffer_manager/yaap_buffer_shared.hpp"
#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/physical_hash_join.hpp"
#include "parallel/pipeline/physical_hash_join_probe.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/pipeline_profile.hpp"
#include "parallel/pipeline/pipeline_timeline.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

[[noreturn]] void
RaiseBlockedForbidden()
{
	Assert(false);
	ereport(ERROR,
	        (errmsg("pg_yaap pipeline returned BLOCKED in 3g.2 (forbidden)")));
}

void
EnsureGlobalStates(ProcessPipelineExecState &ps, Pipeline &pipeline, ExecCtx &ctx)
{
	if (ps.global_source == nullptr)
		ps.global_source = pipeline.source->GetGlobalSourceState(ctx);
	if (ps.global_sink == nullptr)
		ps.global_sink = pipeline.sink->GetGlobalSinkState(ctx);
	if (ps.global_ops.empty())
	{
		ps.global_ops.reserve(pipeline.ops.size());
		for (PhysicalOperator *op : pipeline.ops)
			ps.global_ops.push_back(op->GetGlobalOperatorState(ctx));
	}
}

void
EnsureRunLocalStates(ProcessPipelineExecState &ps, Pipeline &pipeline, ExecCtx &ctx)
{
	if (ps.run_initialized)
		return;
	Assert(ps.global_source != nullptr);
	Assert(ps.global_sink != nullptr);
	ps.local_source = pipeline.source->GetLocalSourceState(ctx, *ps.global_source);
	ps.local_sink = pipeline.sink->GetLocalSinkState(ctx, *ps.global_sink);
	ps.local_ops.clear();
	ps.local_ops.reserve(pipeline.ops.size());
	for (PhysicalOperator *op : pipeline.ops)
		ps.local_ops.push_back(op->GetOperatorState(ctx));
	ps.run_initialized = true;
}

void
EnsureRunChunks(WorkerTaskRuntime &rt, ProcessPipelineExecState &ps, Pipeline &pipeline)
{
	if (!ps.run_src_chunk.IsValid())
		ps.run_src_chunk = rt.chunk_pool.Acquire();
	bool hybrid = false;
	for (auto *op : pipeline.ops) hybrid |= op->type() == PhysicalOperatorType::HASH_JOIN_PROBE;
	if (hybrid) {
		/* Two alternating scratch chunks are insufficient when an upstream
		 * join resumes after its suffix has overwritten that join's input.
		 * Each output remains stable until its own producer resumes. */
		if (ps.run_op_outputs.empty()) {
			const uint64_t bytes = pipeline.ops.size() * sizeof(PipelineChunk);
			auto *budget = static_cast<buffer::YaapBufferSharedState *>(
				dsa_get_address(rt.exec_ctx.dsa, rt.control->buffer_state_root));
			if (!buffer::YaapBufferTryChargeExternalBytes(budget, bytes))
				elog(ERROR, "pg_yaap: hybrid pipeline output chunks exceed query memory limit");
			ps.run_op_output_bytes = bytes;
			ps.run_op_outputs.resize(pipeline.ops.size());
		}
		for (auto &lease : ps.run_op_outputs)
			if (!lease.IsValid()) lease = rt.chunk_pool.Acquire();
	} else {
		if (!ps.run_scratch_a.IsValid()) ps.run_scratch_a = rt.chunk_pool.Acquire();
		if (!ps.run_scratch_b.IsValid()) ps.run_scratch_b = rt.chunk_pool.Acquire();
	}
}

void
ReleaseRunChunks(WorkerTaskRuntime &rt, ProcessPipelineExecState &ps)
{
	rt.chunk_pool.Release(&ps.run_src_chunk);
	rt.chunk_pool.Release(&ps.run_scratch_a);
	rt.chunk_pool.Release(&ps.run_scratch_b);
	for (auto &lease : ps.run_op_outputs) rt.chunk_pool.Release(&lease);
	if (ps.run_op_output_bytes != 0) {
		auto *budget = static_cast<buffer::YaapBufferSharedState *>(
			dsa_get_address(rt.exec_ctx.dsa, rt.control->buffer_state_root));
		buffer::YaapBufferReleaseExternalBytes(budget, ps.run_op_output_bytes);
		ps.run_op_output_bytes = 0;
	}
}

void
TrimLocalScratchIfUnderPressure(WorkerTaskRuntime &rt)
{
	if (rt.control == nullptr || !DsaPointerIsValid(rt.control->buffer_state_root))
		return;
	auto *state = static_cast<buffer::YaapBufferSharedState *>(
		dsa_get_address(rt.exec_ctx.dsa, rt.control->buffer_state_root));
	if (buffer::YaapBufferOverSoftLimit(state))
		rt.chunk_pool.TrimStringArenas(256u * 1024u);
}

class TaskProfileEventGuard
{
public:
	TaskProfileEventGuard(ExecCtx &ctx, EventId event_id)
		: ctx_(ctx), previous_(ctx.profile_event_id)
	{
		ctx_.profile_event_id = event_id;
	}

	~TaskProfileEventGuard()
	{
		ctx_.profile_event_id = previous_;
	}

	TaskProfileEventGuard(const TaskProfileEventGuard &) = delete;
	TaskProfileEventGuard &operator=(const TaskProfileEventGuard &) = delete;

private:
	ExecCtx &ctx_;
	EventId previous_;
};

enum class DrainResult : uint8_t {
	NEED_MORE_INPUT,
	FINISHED,
};

DrainResult
DrainOperatorSuffix(ExecCtx &ctx,
                    Pipeline &pipeline,
                    ProcessPipelineExecState &ps,
                    size_t op_idx,
                    PipelineChunk &input,
                    PipelineChunk &scratch_a,
                    PipelineChunk &scratch_b)
{
	if (op_idx >= pipeline.ops.size())
	{
		OperatorSinkInput sink_in{*ps.global_sink, *ps.local_sink};
		SinkResultType kres;
		{
			PipelineProfileScope sink_scope(ctx,
				PipelineProfileSinkStage(pipeline.sink->type()));
			kres = pipeline.sink->SinkChunk(ctx, input, sink_in);
			sink_scope.AddRows(input.count);
		}
		if (kres == SinkResultType::BLOCKED)
			RaiseBlockedForbidden();
		if (kres == SinkResultType::FINISHED)
			return DrainResult::FINISHED;
		return DrainResult::NEED_MORE_INPUT;
	}

	PhysicalOperator *op = pipeline.ops[op_idx];
	OperatorState &op_state = *ps.local_ops[op_idx];
	PipelineChunk *out = ps.run_op_outputs.empty()
		? ((&input == &scratch_a) ? &scratch_b : &scratch_a)
		: ps.run_op_outputs[op_idx].chunk;
	for (;;)
	{
		out->reset();
		OperatorResultType ores;
		{
			PipelineProfileScope op_scope(ctx,
				PipelineProfileOperatorStage(op->type()));
			ores = op->Execute(ctx, input, *out, op_state);
			if (ores == OperatorResultType::HAVE_MORE_OUTPUT)
				op_scope.AddRows(out->count);
		}
		if (ores == OperatorResultType::BLOCKED)
			RaiseBlockedForbidden();
		if (ores == OperatorResultType::FINISHED)
			return DrainResult::FINISHED;
		if (ores == OperatorResultType::NEED_MORE_INPUT)
			return DrainResult::NEED_MORE_INPUT;

		DrainResult downstream = DrainOperatorSuffix(ctx,
			pipeline,
			ps,
			op_idx + 1,
			*out,
			scratch_a,
			scratch_b);
		if (downstream == DrainResult::FINISHED)
			return DrainResult::FINISHED;
	}
}

}  /* namespace */

Task::Task(EventId event_id, TaskKind kind, Pipeline *pipeline,
           WorkerTaskRuntime *runtime, int32_t worker_index,
           uint32_t partition_id)
    : event_id_(event_id)
    , kind_(kind)
    , pipeline_(pipeline)
    , runtime_(runtime)
    , worker_index_(worker_index)
    , partition_id_(partition_id) {}

PipelineRunTask::PipelineRunTask(EventId event_id, Pipeline *pipeline,
                                 WorkerTaskRuntime *runtime,
                                 int32_t worker_index)
    : Task(event_id, TaskKind::RUN, pipeline, runtime, worker_index) {}

TaskExecutionResult
PipelineRunTask::Execute()
{
	auto &rt = *runtime_;
	auto &ctx = rt.exec_ctx;
	TaskProfileEventGuard event_guard(ctx, event_id_);
	PipelineTimelineTaskScope timeline_scope(ctx, event_id_, partition_id_);
	PipelineProfileScope task_scope(ctx, PipelineProfileStage::TASK_RUN_TOTAL);
	TrimLocalScratchIfUnderPressure(rt);
	auto &ps = rt.GetOrCreatePipelineState(pipeline_->id);

	EnsureGlobalStates(ps, *pipeline_, ctx);
	EnsureRunLocalStates(ps, *pipeline_, ctx);
	EnsureRunChunks(rt, ps, *pipeline_);

	const uint32_t chunk_budget = 32;
	uint32_t chunks_done = 0;

	PipelineChunk &src_chunk = *ps.run_src_chunk.chunk;
	PipelineChunk &scratch_a = *(ps.run_op_outputs.empty()
		? ps.run_scratch_a.chunk : ps.run_op_outputs.front().chunk);
	PipelineChunk &scratch_b = *(ps.run_op_outputs.empty()
		? ps.run_scratch_b.chunk : ps.run_op_outputs.back().chunk);
	auto finish_run = [&]() {
		/* Hybrid operator state owns scan participation and resident leases.
		 * Release it on every terminal path, including downstream early stop. */
		ps.local_ops.clear();
		ps.local_source.reset();
		ReleaseRunChunks(rt, ps);
		return TaskExecutionResult::TASK_FINISHED;
	};

	for (;;)
	{
		if (rt.control != nullptr &&
			pg_atomic_read_u32(&rt.control->shutdown_requested) != 0)
		{
			timeline_scope.SetResult(TimelineTaskResult::CANCELLED);
			return finish_run();
		}
		CHECK_FOR_INTERRUPTS();

		if (chunks_done >= chunk_budget)
		{
			timeline_scope.SetResult(TimelineTaskResult::YIELDED);
			return TaskExecutionResult::TASK_NOT_FINISHED;
		}

		if (ps.source_exhausted)
		{
			/* Flush left to right: an earlier join may feed deferred rows into
			 * a later join, so the latter cannot finish its input first. Keep
			 * the same downstream sink state until all drains have completed. */
			while (ps.final_op_index < pipeline_->ops.size() &&
				pipeline_->ops[ps.final_op_index]->type() != PhysicalOperatorType::HASH_JOIN_PROBE)
				++ps.final_op_index;
			if (ps.final_op_index == pipeline_->ops.size())
				return finish_run();
			auto *op = static_cast<PhysicalHashJoinProbe *>(pipeline_->ops[ps.final_op_index]);
			src_chunk.reset();
			OperatorResultType ores;
			{
				PipelineProfileScope op_scope(ctx, PipelineProfileOperatorStage(op->type()));
				ores = op->FinalExecute(ctx, src_chunk, *ps.local_ops[ps.final_op_index]);
				if (ores == OperatorResultType::HAVE_MORE_OUTPUT)
					op_scope.AddRows(src_chunk.count);
			}
			if (ores == OperatorResultType::BLOCKED)
				RaiseBlockedForbidden();
			if (ores == OperatorResultType::FINISHED)
				return finish_run();
			if (ores == OperatorResultType::NEED_MORE_INPUT)
			{
				++ps.final_op_index;
				continue;
			}
			if (DrainOperatorSuffix(ctx, *pipeline_, ps, ps.final_op_index + 1,
				src_chunk, scratch_a, scratch_b) == DrainResult::FINISHED)
				return finish_run();
			++chunks_done;
			continue;
		}

		OperatorSourceInput src_in{*ps.global_source, *ps.local_source};
		SourceResultType sres;
		{
			PipelineProfileScope source_scope(ctx,
				PipelineProfileSourceStage(pipeline_->source->type()));
			sres = pipeline_->source->GetData(ctx, src_chunk, src_in);
			if (sres == SourceResultType::HAVE_MORE_OUTPUT)
				source_scope.AddRows(src_chunk.count);
		}

		if (sres == SourceResultType::BLOCKED)
			RaiseBlockedForbidden();
		if (sres == SourceResultType::FINISHED)
		{
			ps.source_exhausted = true;
			continue;
		}

		if (DrainOperatorSuffix(ctx, *pipeline_, ps, 0, src_chunk, scratch_a, scratch_b) ==
			DrainResult::FINISHED)
		{
			return finish_run();
		}
		++chunks_done;
	}
}

PipelineCombineTask::PipelineCombineTask(EventId event_id,
                                          Pipeline *pipeline,
                                          WorkerTaskRuntime *runtime,
                                          int32_t worker_index,
                                          uint32_t partition_id)
    : Task(event_id, TaskKind::COMBINE, pipeline, runtime, worker_index, partition_id) {}

TaskExecutionResult
PipelineCombineTask::Execute()
{
	auto &rt = *runtime_;
	auto &ctx = rt.exec_ctx;
	TaskProfileEventGuard event_guard(ctx, event_id_);
	PipelineTimelineTaskScope timeline_scope(ctx, event_id_, partition_id_);
	PipelineProfileScope task_scope(ctx, PipelineProfileStage::TASK_COMBINE_TOTAL);
	auto &ps = rt.GetPipelineState(pipeline_->id);
	Assert(ps.global_sink != nullptr);
	if (partition_id_ == UINT32_MAX)
		Assert(ps.local_sink != nullptr);
	Assert(partition_id_ != UINT32_MAX || !ps.combine_done);
	OperatorSinkCombineInput in{ps.local_sink.get(), *ps.global_sink, partition_id_};
	SinkCombineResultType cres;
	{
		PipelineProfileScope combine_scope(ctx,
			PipelineProfileCombineStage(pipeline_->sink->type()));
		cres = pipeline_->sink->Combine(ctx, in);
	}
	if (cres == SinkCombineResultType::BLOCKED)
		RaiseBlockedForbidden();
	if (partition_id_ == UINT32_MAX)
	{
		ps.combine_done = true;
		ps.local_source.reset();
		ps.local_sink.reset();
		ps.local_ops.clear();
	}
	return TaskExecutionResult::TASK_FINISHED;
}

PipelineFinalizeTask::PipelineFinalizeTask(EventId event_id,
                                           Pipeline *pipeline,
                                           WorkerTaskRuntime *runtime,
                                           int32_t worker_index,
                                           uint32_t partition_id)
    : Task(event_id, TaskKind::FINALIZE, pipeline, runtime, worker_index, partition_id) {}

TaskExecutionResult
PipelineFinalizeTask::Execute()
{
	auto &rt = *runtime_;
	auto &ctx = rt.exec_ctx;
	TaskProfileEventGuard event_guard(ctx, event_id_);
	PipelineTimelineTaskScope timeline_scope(ctx, event_id_, partition_id_);
	PipelineProfileScope task_scope(ctx, PipelineProfileStage::TASK_FINALIZE_TOTAL);
	/*
	 * GetOrCreatePipelineState (not strict GetPipelineState):
	 * FINALIZE is allowed to be the first touch on a pipeline for a given
	 * runtime. This is the OUTPUT pipeline topology: workers execute RUN
	 * (writing to the global TDC); the leader skips RUN (leader_participate
	 * is false for OUTPUT) and only sees the pipeline at FINALIZE to drain
	 * the global TDC into the DestReceiver. EnsureGlobalStates() below
	 * initializes global_sink/global_source from the descriptor on a freshly
	 * created empty ProcessPipelineExecState, which is exactly the seed
	 * state we need. The strict GetPipelineState variant traps the empty
	 * vector (libc++ Hardening / __builtin_trap) and is therefore wrong for
	 * leader-only-finalize pipelines.
	 *
	 * COMBINE keeps the strict variant intentionally: if COMBINE arrives
	 * for a runtime that never RAN, that is a real scheduling bug
	 * (local_sink/local_ops would not exist). OUTPUT does not dispatch
	 * COMBINE.
	 */
	auto &ps = rt.GetOrCreatePipelineState(pipeline_->id);
	EnsureGlobalStates(ps, *pipeline_, ctx);

	if (ps.leader_partial_pending && ps.local_sink)
	{
		OperatorSinkCombineInput cin{ps.local_sink.get(), *ps.global_sink, UINT32_MAX};
		auto cres = pipeline_->sink->Combine(ctx, cin);
		if (cres == SinkCombineResultType::BLOCKED)
			RaiseBlockedForbidden();
		ps.leader_partial_pending = false;
		ps.local_source.reset();
		ps.local_sink.reset();
		ps.local_ops.clear();
	}
	SinkFinalizeType fres;
	{
		PipelineProfileScope finalize_scope(ctx,
			PipelineProfileFinalizeStage(pipeline_->sink->type()));
		if (partition_id_ != UINT32_MAX)
		{
			if (pipeline_->sink->type() != PhysicalOperatorType::HASH_JOIN)
				elog(ERROR, "pg_yaap: partitioned FINALIZE task is only valid for hash join");
			fres = static_cast<PhysicalHashJoin *>(pipeline_->sink)->FinalizePartition(
				ctx, *ps.global_sink, partition_id_);
		}
		else
			fres = pipeline_->sink->Finalize(ctx, *ps.global_sink);
	}
	if (fres == SinkFinalizeType::BLOCKED)
		RaiseBlockedForbidden();
	return TaskExecutionResult::TASK_FINISHED;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
