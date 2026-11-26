#include "parallel/pipeline/task_scheduler.hpp"

extern "C" {
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procarray.h"
}

#include <algorithm>
#include <utility>

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/dsm_task_queue.hpp"
#include "parallel/pipeline/execution_slots.hpp"
#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/physical_cross_product.hpp"
#include "parallel/pipeline/physical_delim_scan.hpp"
#include "parallel/pipeline/physical_hash_join.hpp"
#include "parallel/pipeline/physical_hash_join_probe.hpp"
#include "parallel/pipeline/pipeline_dsm_lookup.hpp"
#include "parallel/pipeline/pipeline_timeline.hpp"
#include "parallel/pipeline/worker_error.hpp"

namespace pg_yaap::pipeline {

namespace {

constexpr uint32_t kEventsPerPipeline = 3;

PipelineId EventPipeline(EventId id)
{
	return static_cast<PipelineId>(id / kEventsPerPipeline);
}

TaskKind EventKind(EventId id)
{
	return static_cast<TaskKind>(id % kEventsPerPipeline + 1);
}

bool Stopping(PipelineSharedControl *control)
{
	return pg_atomic_read_u32(&control->shutdown_requested) != 0 ||
		WorkerErrorPending(control);
}

void ReleaseRunInputs(ExecCtx &ctx, Pipeline &pipeline)
{
	if (pipeline.source->type() == PhysicalOperatorType::HASH_JOIN_PROBE)
		static_cast<PhysicalHashJoinProbe *>(pipeline.source)->ReleaseAfterConsumerRun(ctx);
	else if (pipeline.source->type() == PhysicalOperatorType::MATERIALIZE)
		static_cast<PhysicalMaterialize *>(pipeline.source)->ReleaseAfterConsumerRun(ctx);
	for (auto *op : pipeline.ops)
	{
		switch (op->type())
		{
			case PhysicalOperatorType::HASH_JOIN_PROBE:
				static_cast<PhysicalHashJoinProbe *>(op)->ReleaseAfterConsumerRun(ctx);
				break;
			case PhysicalOperatorType::HASH_JOIN:
				static_cast<PhysicalHashJoin *>(op)->ReleaseBuildPayloadAfterConsumerRun(ctx);
				break;
			case PhysicalOperatorType::CROSS_PRODUCT:
				static_cast<PhysicalCrossProduct *>(op)->ReleaseBuildPayloadAfterConsumerRun(ctx);
				break;
			default:
				break;
		}
	}
}

}  /* namespace */

void
TaskScheduler::BuildEvents(const MetaPipelineBundle &bundle, ExecCtx &ctx)
{
	const auto count = static_cast<EventId>(bundle.pipelines.size()) * kEventsPerPipeline;
	Assert(count > 0);
	auto *control = ctx.control;

	PgVector<std::pair<EventId, EventId>> edges;
	PgUnorderedMap<const PhysicalOperator *, PipelineId> sink_owners;
	for (const auto &pipeline : bundle.pipelines)
	{
		if (pipeline->source == nullptr || pipeline->sink == nullptr)
			elog(ERROR, "pg_yaap: pipeline %u is missing source or sink", pipeline->id);
		const auto [it, inserted] = sink_owners.emplace(pipeline->sink, pipeline->id);
		if (!inserted)
			elog(ERROR, "pg_yaap: sink operator shared by pipelines %u and %u without a shared sink-group contract",
				it->second, pipeline->id);
		const EventId run = static_cast<EventId>(pipeline->id) * kEventsPerPipeline;
		edges.emplace_back(run, run + 1);
		edges.emplace_back(run + 1, run + 2);
		for (PipelineId dep : pipeline->depends_on)
		{
			if (dep >= bundle.pipelines.size())
				elog(ERROR, "pg_yaap: invalid pipeline dependency %u", dep);
			edges.emplace_back(static_cast<EventId>(dep) * kEventsPerPipeline + 2, run);
		}
	}
	std::sort(edges.begin(), edges.end());
	edges.erase(std::unique(edges.begin(), edges.end()), edges.end());

	control->event_states_root = dsa_allocate0(ctx.dsa, count * sizeof(EventShmState));
	auto *events = static_cast<EventShmState *>(dsa_get_address(ctx.dsa, control->event_states_root));
	control->event_successors_root = dsa_allocate(ctx.dsa, edges.size() * sizeof(EventId));
	auto *successors = static_cast<EventId *>(dsa_get_address(ctx.dsa, control->event_successors_root));

	for (size_t index = 0; index < edges.size(); ++index)
	{
		const auto [from, to] = edges[index];
		auto &event = events[from];
		if (event.successor_count++ == 0)
			event.successor_offset = static_cast<uint32_t>(index);
		successors[index] = to;
		++events[to].dependencies_remaining;
	}

	PgVector<uint32_t> indegree(count);
	PgVector<EventId> ready;
	for (EventId id = 0; id < count; ++id)
	{
		auto &event = events[id];
		event.phase = SharedEventPhase::WAITING;
		event.activated_by = -1;
		event.completed_by = -1;
		pg_atomic_init_u32(&event.tasks_remaining, 0);
		pg_atomic_init_u32(&event.tasks_in_flight, 0);
		indegree[id] = event.dependencies_remaining;
		if (indegree[id] == 0)
			ready.push_back(id);
	}
	for (size_t visited = 0; visited < ready.size(); ++visited)
	{
		const auto &event = events[ready[visited]];
		for (uint32_t j = 0; j < event.successor_count; ++j)
		{
			const EventId next = successors[event.successor_offset + j];
			if (--indegree[next] == 0)
				ready.push_back(next);
		}
	}
	if (ready.size() != count)
		elog(ERROR, "pg_yaap: pipeline event graph contains a dependency cycle");
	uint32_t terminal_events = 0;
	for (EventId id = 0; id < count; ++id)
		if (events[id].successor_count == 0)
			++terminal_events;
	if (terminal_events == 0)
		elog(ERROR, "pg_yaap: pipeline event graph has no terminal event");

	control->event_count = count;
	control->active_probe_run = INVALID_EVENT_ID;
	pg_atomic_init_u32(&control->events_remaining, count);
	pg_atomic_init_u32(&control->scheduler_started, 0);
	pg_atomic_init_u32(&control->scheduler_busy, 0);
}

TaskScheduler::TaskScheduler(ExecCtx &ctx, DsmTaskQueue &queue,
                             const PipelineDsmLookup<Pipeline> &pipelines)
	: ctx_(ctx), queue_(queue), pipelines_(pipelines),
	  events_(static_cast<EventShmState *>(dsa_get_address(ctx.dsa, ctx.control->event_states_root))),
	  successors_(static_cast<const EventId *>(dsa_get_address(ctx.dsa, ctx.control->event_successors_root)))
{
}

bool
TaskScheduler::Activate(EventId id)
{
	auto &event = events_[id];
	const PipelineId pid = EventPipeline(id);
	auto *pipeline = pipelines_.Resolve(pid);
	Assert(pipeline != nullptr && pipeline->source != nullptr && pipeline->sink != nullptr);
	const TaskKind kind = EventKind(id);
	ExecCtx stage_ctx = ctx_;
	stage_ctx.profile_event_id = id;
	stage_ctx.initialize_shared_state = true;
	SinkTaskPlan plan{0, 0, false};
	switch (kind)
	{
		case TaskKind::RUN:
		{
			const bool hybrid = std::any_of(pipeline->ops.begin(), pipeline->ops.end(),
				[](const PhysicalOperator *op) {
					return op->type() == PhysicalOperatorType::HASH_JOIN_PROBE;
				});
			if (hybrid || pipeline->source->type() == PhysicalOperatorType::HASH_JOIN_PROBE)
			{
				if (ctx_.control->active_probe_run != INVALID_EVENT_ID)
					return false;
				ctx_.control->active_probe_run = id;
			}

			(void) pipeline->source->GetGlobalSourceState(stage_ctx);
			(void) pipeline->sink->GetGlobalSinkState(stage_ctx);
			uint32_t tasks = ExecutionWorkerCount(stage_ctx);
			auto limit = [&](int cap) {
				if (cap > 0)
					tasks = std::min(tasks, static_cast<uint32_t>(cap));
			};
			limit(pipeline->source->ParallelSource() ? pipeline->source->MaxThreads(stage_ctx) : 1);
			for (auto *op : pipeline->ops)
				limit(op->MaxThreads(stage_ctx));
			limit(pipeline->sink->MaxThreads(stage_ctx));
			plan = {tasks, tasks, false};
			if (hybrid)
			{
				for (auto *op : pipeline->ops)
					if (op->type() == PhysicalOperatorType::HASH_JOIN_PROBE)
						static_cast<PhysicalHashJoinProbe *>(op)->InitializeHybrid(stage_ctx, tasks);
			}
			PipelineTimelineReady(ctx_.control, ctx_.dsa, pid);
			break;
		}
		case TaskKind::COMBINE:
			if (!pipeline->sink->CombineIsTrivial())
				plan = pipeline->sink->GetCombineTaskPlan(stage_ctx, events_[id - 1].task_count);
			break;
		case TaskKind::FINALIZE:
			plan = pipeline->sink->GetFinalizeTaskPlan(stage_ctx, events_[id - 2].task_count);
			break;
	}

	if (plan.task_count != 0 &&
		(plan.worker_count == 0 || plan.worker_count > ExecutionWorkerCount(stage_ctx)))
		elog(ERROR, "pg_yaap: operator returned an invalid sink task plan");
	event.task_count = plan.task_count;
	event.max_concurrency = plan.worker_count;
	event.partitioned = plan.partitioned;
	event.phase = SharedEventPhase::RUNNING;
	event.activated_by = ctx_.worker_index;
	/* The complete logical count precedes publication of the first task. */
	pg_atomic_write_u32(&event.tasks_remaining, plan.task_count);
	return true;
}

void
TaskScheduler::FinishEvent(EventId id)
{
	auto &event = events_[id];
	const PipelineId pid = EventPipeline(id);
	if (EventKind(id) == TaskKind::RUN)
	{
		ReleaseRunInputs(ctx_, *pipelines_.Resolve(pid));
		if (ctx_.control->active_probe_run == id)
			ctx_.control->active_probe_run = INVALID_EVENT_ID;
		dsa_trim(ctx_.dsa);
	}
	else if (EventKind(id) == TaskKind::FINALIZE)
		PipelineTimelineFinished(ctx_.control, ctx_.dsa, pid);

	if (Stopping(ctx_.control))
		return;
	event.phase = SharedEventPhase::FINISHED;
	event.completed_by = ctx_.worker_index;
	for (uint32_t j = 0; j < event.successor_count; ++j)
	{
		auto &next = events_[successors_[event.successor_offset + j]];
		Assert(next.dependencies_remaining > 0 && next.phase == SharedEventPhase::WAITING);
		--next.dependencies_remaining;
	}
	if (pg_atomic_sub_fetch_u32(&ctx_.control->events_remaining, 1) == 0)
	{
		PGPROC *leader = BackendPidGetProc(ctx_.control->leader_pid);
		if (leader != nullptr)
			SetLatch(&leader->procLatch);
	}
}

bool
TaskScheduler::DispatchReadyTasks()
{
	bool progress = false;
	for (EventId id = 0; id < ctx_.control->event_count && !Stopping(ctx_.control); ++id)
	{
		auto &event = events_[id];
		if (event.phase != SharedEventPhase::RUNNING)
			continue;
		while (event.next_task < event.task_count &&
		       pg_atomic_read_membarrier_u32(&event.tasks_in_flight) < event.max_concurrency)
		{
			const uint32_t index = event.next_task;
			TaskDescriptor desc{};
			desc.pipeline_id = EventPipeline(id);
			desc.event_id = id;
			desc.partition_id = event.partitioned ? index : UINT32_MAX;
			desc.task_index = index;
			desc.kind = static_cast<uint8_t>(EventKind(id));
			pg_atomic_add_fetch_u32(&event.tasks_in_flight, 1);
			if (!queue_.TryPush(desc))
			{
				pg_atomic_sub_fetch_u32(&event.tasks_in_flight, 1);
				return progress;
			}
			++event.next_task;
			progress = true;
		}
	}
	return progress;
}

bool
TaskScheduler::Advance()
{
	auto *control = ctx_.control;
	if (pg_atomic_read_membarrier_u32(&control->scheduler_started) == 0 ||
		pg_atomic_read_u32(&control->events_remaining) == 0 || Stopping(control))
		return false;
	uint32_t expected = 0;
	if (!pg_atomic_compare_exchange_u32(&control->scheduler_busy, &expected, 1))
		return false;

	/* ERROR aborts the entire query; clear the election token on either exit. */
	volatile bool progress = false;
	PG_TRY();
	{
		bool changed;
		do
		{
			changed = false;
			for (EventId id = 0; id < control->event_count && !Stopping(control); ++id)
			{
				auto &event = events_[id];
				if (event.phase == SharedEventPhase::WAITING && event.dependencies_remaining == 0)
					changed |= Activate(id);
				if (event.phase == SharedEventPhase::RUNNING &&
					pg_atomic_read_membarrier_u32(&event.tasks_remaining) == 0)
				{
					FinishEvent(id);
					changed = true;
				}
			}
			progress |= changed;
		} while (changed && !Stopping(control));
		progress |= DispatchReadyTasks();
	}
	PG_CATCH();
	{
		/* Prevent another worker from advancing a partially activated event. */
		pg_atomic_write_membarrier_u32(&control->shutdown_requested, 1);
		pg_atomic_write_membarrier_u32(&control->scheduler_busy, 0);
		PG_RE_THROW();
	}
	PG_END_TRY();
	pg_atomic_write_membarrier_u32(&control->scheduler_busy, 0);
	return progress;
}

void
TaskScheduler::CompleteTask(EventId id)
{
	auto &event = events_[id];
	pg_atomic_sub_fetch_u32(&event.tasks_in_flight, 1);
	pg_atomic_sub_fetch_u32(&event.tasks_remaining, 1);
	/* A worker already in Advance may have just inspected this phase. */
	queue_.WakeRegisteredWorkers();
}

}  /* namespace pg_yaap::pipeline */
