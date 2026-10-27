/*
 * pipeline/task_scheduler.cpp  (M-FRAME-MIN step 3g.2 / C1)
 *
 * See task_scheduler.hpp banner for the full C1 contract. This TU implements:
 *   - SchedulerState / PerPipelineState (lazy per-pipeline tables)
 *   - BindRuntime (bind already-attached PipelineSharedControl + DsmTaskQueue
 *     + dsa_area pointers; scheduler does not own DSM/DSA)
 *   - AllocateEventShmStates (DSA-allocate the per-event counter array and
 *     publish via PipelineSharedControl::event_states_root + event_count)
 *   - EnqueueTasks (real body; dispatches by Event::kind() into RUN/COMBINE/
 *     FINALIZE fan-out; initializes EventShmState.tasks_remaining BEFORE the
 *     first push; pushes N TaskDescriptors via DsmTaskQueue::TryPush which
 *     auto-wakes registered worker latches)
 *   - EstimateDsmSize (corrected to sizeof(EventShmState); see below)
 *
 * 3g.1 already shipped: ctor, BuildEvents, pipeline-id registration. Those
 * bodies are preserved verbatim.
 *
 * Worker bgworker launch + leader Run() event loop are deferred to C7
 * (pipeline_leader.cpp) per the user-locked C1 scope split.
 */

#include "parallel/pipeline/task_scheduler.hpp"

extern "C" {
#include "port/atomics.h"
#include "utils/dsa.h"
}

#include <algorithm>

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/dsm_task_queue.hpp"
#include "parallel/pipeline/physical_cross_product.hpp"
#include "parallel/pipeline/physical_hash_aggregate.hpp"
#include "parallel/pipeline/physical_hash_join.hpp"
#include "parallel/pipeline/physical_hash_join_probe.hpp"
#include "parallel/pipeline/physical_perfect_hash_aggregate.hpp"
#include "parallel/pipeline/output_sink.hpp"
#include "parallel/pipeline/pipeline_combine_event.hpp"
#include "parallel/pipeline/pipeline_finalize_event.hpp"
#include "parallel/pipeline/pipeline_run_event.hpp"
#include "parallel/pipeline/pipeline_timeline.hpp"
#include "buffer_manager/yaap_shared_segment_arena.hpp"

namespace pg_yaap {
namespace pipeline {

/* ------------------------------------------------------------------------ */
/* PerPipelineState                                                          */
/* ------------------------------------------------------------------------ */

PerPipelineState::PerPipelineState(MemoryContext mcxt)
    : global_ops(PgMemoryContextAllocator<std::unique_ptr<GlobalOperatorState>>(mcxt))
    , local_source_per_worker(PgMemoryContextAllocator<std::unique_ptr<LocalSourceState>>(mcxt))
    , local_sink_per_worker(PgMemoryContextAllocator<std::unique_ptr<LocalSinkState>>(mcxt))
    , local_op_states_per_worker(
          PgMemoryContextAllocator<PgVector<std::unique_ptr<OperatorState>>>(mcxt))
{
}

/* ------------------------------------------------------------------------ */
/* SchedulerState                                                            */
/* ------------------------------------------------------------------------ */

SchedulerState::SchedulerState(MemoryContext mcxt, uint32_t worker_count)
    : mcxt_(mcxt)
    , worker_count_(worker_count)
    , per_pipeline_(PgMemoryContextAllocator<std::unique_ptr<PerPipelineState>>(mcxt))
{
	Assert(mcxt_ != nullptr);
	Assert(worker_count_ >= 1);
}

PerPipelineState &
SchedulerState::GetOrCreate(PipelineId pid)
{
	Assert(pid != INVALID_PIPELINE_ID);
	const size_t idx = static_cast<size_t>(pid);
	if (per_pipeline_.size() <= idx)
		per_pipeline_.resize(idx + 1);
	if (!per_pipeline_[idx])
		per_pipeline_[idx] = std::make_unique<PerPipelineState>(mcxt_);
	return *per_pipeline_[idx];
}

PerPipelineState &
SchedulerState::Get(PipelineId pid)
{
	const size_t idx = static_cast<size_t>(pid);
	Assert(idx < per_pipeline_.size());
	Assert(per_pipeline_[idx] != nullptr);
	return *per_pipeline_[idx];
}

/* ------------------------------------------------------------------------ */
/* TaskScheduler                                                             */
/* ------------------------------------------------------------------------ */

TaskScheduler::TaskScheduler(MemoryContext              mcxt,
                             std::unique_ptr<MetaPipelineBundle> bundle,
                             const TaskSchedulerSizing &sizing)
    : mcxt_(mcxt)
    , bundle_(std::move(bundle))
    , sizing_(sizing)
    , pipelines_(mcxt)
    , events_(mcxt)
    , events_owned_(PgMemoryContextAllocator<std::shared_ptr<Event>>(mcxt))
    , run_task_counts_(PgMemoryContextAllocator<uint32_t>(mcxt))
    , pending_tasks_(PgMemoryContextAllocator<TaskDescriptor>(mcxt))
    , pending_partition_probe_runs_(PgMemoryContextAllocator<Event *>(mcxt))
    , state_(mcxt, sizing.worker_count)
{
	Assert(mcxt_ != nullptr);
	Assert(bundle_ != nullptr);
	Assert(sizing_.worker_count >= 1);

	for (auto &p : bundle_->pipelines)
	{
		Assert(p->id != INVALID_PIPELINE_ID);
		pipelines_.Register(static_cast<uint32_t>(p->id), p.get());
	}
}

void
TaskScheduler::BuildEvents()
{
	Assert(events_owned_.empty());

	const auto &pipelines = bundle_->pipelines;

	PgMemoryContextAllocator<std::shared_ptr<PipelineRunEvent>>      run_alloc(mcxt_);
	PgMemoryContextAllocator<std::shared_ptr<PipelineCombineEvent>>  cmb_alloc(mcxt_);
	PgMemoryContextAllocator<std::shared_ptr<PipelineFinalizeEvent>> fin_alloc(mcxt_);

	std::vector<std::shared_ptr<PipelineRunEvent>,
	            PgMemoryContextAllocator<std::shared_ptr<PipelineRunEvent>>>
	    run_events(run_alloc);
	std::vector<std::shared_ptr<PipelineCombineEvent>,
	            PgMemoryContextAllocator<std::shared_ptr<PipelineCombineEvent>>>
	    combine_events(cmb_alloc);
	std::vector<std::shared_ptr<PipelineFinalizeEvent>,
	            PgMemoryContextAllocator<std::shared_ptr<PipelineFinalizeEvent>>>
	    finalize_events(fin_alloc);
	run_events.reserve(pipelines.size());
	combine_events.reserve(pipelines.size());
	finalize_events.reserve(pipelines.size());

	for (auto &p : pipelines)
	{
		auto run = AllocatePgShared<PipelineRunEvent>(p->id, p.get(), this);
		auto cmb = AllocatePgShared<PipelineCombineEvent>(p->id, p.get(), this);
		auto fin = AllocatePgShared<PipelineFinalizeEvent>(p->id, p.get(), this);

		cmb->AddDependency(run);
		fin->AddDependency(cmb);

		run_events.push_back(run);
		combine_events.push_back(cmb);
		finalize_events.push_back(fin);
	}

	for (size_t i = 0; i < pipelines.size(); i++)
	{
		for (PipelineId dep_pid : pipelines[i]->depends_on)
		{
			Assert(dep_pid < pipelines.size());
			run_events[i]->AddDependency(finalize_events[dep_pid]);
		}
	}

	auto publish = [&](const std::shared_ptr<Event> &ev) {
		EventId id = next_event_id_++;
		events_.Register(id, ev.get());
		events_owned_.push_back(ev);
	};

	for (size_t i = 0; i < pipelines.size(); i++)
	{
		publish(run_events[i]);
		publish(combine_events[i]);
		publish(finalize_events[i]);
	}
}

TaskSchedulerDsmLayout
TaskScheduler::EstimateDsmSize() const
{
	/* C1 fix vs 3g.1: per-event counters are now full EventShmState structs
	 * (tasks_remaining + saw_error), not bare pg_atomic_uint32. The array
	 * lives in DSA (allocated by AllocateEventShmStates) so this byte count
	 * is the DSA budget the leader must reserve, not part of the inline DSM
	 * segment. */
	TaskSchedulerDsmLayout layout{};
	layout.control_bytes        = MAXALIGN(sizeof(PipelineSharedControl));
	layout.task_queue_bytes     = MAXALIGN(
		DsmTaskQueue::EstimateSize(sizing_.task_queue_capacity));
	layout.event_counters_bytes = MAXALIGN(
		(size_t) next_event_id_ * sizeof(EventShmState));
	layout.total_bytes          = layout.control_bytes
	                            + layout.task_queue_bytes
	                            + layout.event_counters_bytes;
	return layout;
}

void
TaskScheduler::BindRuntime(PipelineSharedControl *control,
                           DsmTaskQueue          *task_queue,
                           dsa_area              *dsa)
{
	Assert(control != nullptr);
	Assert(task_queue != nullptr);
	Assert(dsa != nullptr);
	Assert(control_ == nullptr);
	Assert(control->magic == PIPELINE_DSM_MAGIC);

	control_    = control;
	task_queue_ = task_queue;
	dsa_        = dsa;
}

void
TaskScheduler::AllocateEventShmStates()
{
	Assert(control_ != nullptr);
	Assert(dsa_ != nullptr);
	Assert(event_shm_ == nullptr);
	Assert(next_event_id_ > 0);

	const size_t bytes = (size_t) next_event_id_ * sizeof(EventShmState);
	event_shm_dp_      = dsa_allocate0(dsa_, bytes);
	event_shm_         = static_cast<EventShmState *>(
		dsa_get_address(dsa_, event_shm_dp_));

	for (uint32_t i = 0; i < next_event_id_; i++)
	{
		pg_atomic_init_u32(&event_shm_[i].tasks_remaining, 0);
		pg_atomic_init_u32(&event_shm_[i].saw_error, 0);
	}

	control_->event_states_root = event_shm_dp_;
	control_->event_count       = next_event_id_;
}

EventShmState &
TaskScheduler::event_shm_slot(EventId id)
{
	Assert(event_shm_ != nullptr);
	Assert(id < next_event_id_);
	return event_shm_[id];
}

uint32_t
TaskScheduler::DeriveRunTaskCount(Pipeline &pipeline) const
{
	Assert(pipeline.source != nullptr);
	Assert(pipeline.sink != nullptr);
	ExecCtx ctx{mcxt_, dsa_, LEADER_WORKER_INDEX};
	uint32_t bound = sizing_.worker_count;
	auto apply = [&](int v) {
		if (v > 0)
			bound = std::min(bound, static_cast<uint32_t>(v));
	};
	/*
	 * Only sources that explicitly implement parallel morsel ownership may fan
	 * out RUN tasks. Pipeline-breaker sources such as HASH_AGGREGATE expose a
	 * single shared cursor in GetData(); letting multiple workers consume them
	 * duplicates rows. Today SEQ_SCAN is the only parallel source.
	 */
	apply(pipeline.source->ParallelSource() ? pipeline.source->MaxThreads(ctx) : 1);
	for (auto *op : pipeline.ops)
		apply(op->MaxThreads(ctx));
	apply(pipeline.sink->MaxThreads(ctx));
	return bound == 0 ? 1u : bound;
}

void
TaskScheduler::RememberRunTaskCount(PipelineId pid, uint32_t task_count)
{
	Assert(pid != INVALID_PIPELINE_ID);
	const size_t idx = static_cast<size_t>(pid);
	if (run_task_counts_.size() <= idx)
		run_task_counts_.resize(idx + 1);
	run_task_counts_[idx] = std::max<uint32_t>(1u, task_count);
}

uint32_t
TaskScheduler::RememberedRunTaskCount(PipelineId pid) const
{
	Assert(pid != INVALID_PIPELINE_ID);
	const size_t idx = static_cast<size_t>(pid);
	if (idx < run_task_counts_.size() && run_task_counts_[idx] != 0)
		return run_task_counts_[idx];
	return 1u;
}

void
TaskScheduler::EnqueueTasks(Event &event)
{
	/* Preconditions: BindRuntime + AllocateEventShmStates already called by
	 * the leader before any Event::Schedule() runs. EnqueueTasks is invoked
	 * from PipelineRunEvent::Schedule / PipelineCombineEvent::Schedule /
	 * PipelineFinalizeEvent::Schedule (3 cpp files, see C1 dispatch). */
	Assert(control_ != nullptr);
	Assert(task_queue_ != nullptr);
	Assert(event_shm_ != nullptr);

	const PipelineId pid = event.pipeline_id();
	Assert(pid != INVALID_PIPELINE_ID);
	Pipeline *pipeline = pipelines_.Resolve(static_cast<uint32_t>(pid));
	Assert(pipeline != nullptr);
	auto *sink = pipeline->sink;
	Assert(sink != nullptr);

	/* Resolve the EventId scheduler-side. BuildEvents publishes 3 events
	 * per pipeline in the deterministic order (Run, Combine, Finalize)
	 * starting at id = pid*3, so we recover the id by kind without needing
	 * a reverse Resolve(). This stays consistent with PipelineDsmLookup since
	 * Register() was called in the same order. */
	EventId base_id = static_cast<EventId>(pid) * 3u;
	EventId event_id;
	uint32_t task_count;
	bool parallel_hash_join_finalize = false;
	uint32_t hash_join_finalize_workers = 1;
	bool segmented_hash_join_worker_combine = false;
	const TaskKind kind = event.kind();
	const bool has_hybrid_probe = std::any_of(pipeline->ops.begin(), pipeline->ops.end(),
		[](const PhysicalOperator *op) {
			return op->type() == PhysicalOperatorType::HASH_JOIN_PROBE;
		});
	switch (kind)
	{
		case TaskKind::RUN:
			event_id   = base_id + 0;
			if (pipeline->source->type() == PhysicalOperatorType::HASH_JOIN_PROBE || has_hybrid_probe)
			{
				/* Paired sources and streaming hybrid operators reserve partition
				 * working sets. Admit the RUN before initializing those reservations,
				 * rather than parking workers behind another RUN's leases. */
				if (active_partition_probe_run_ != INVALID_EVENT_ID)
				{
					Assert(active_partition_probe_run_ != event_id);
					pg_atomic_write_u32(&event_shm_[event_id].tasks_remaining, 1);
					pending_partition_probe_runs_.push_back(&event);
					return;
				}
				active_partition_probe_run_ = event_id;
			}
			task_count = DeriveRunTaskCount(*pipeline);
			RememberRunTaskCount(pid, task_count);
			if (has_hybrid_probe)
			{
				ExecCtx leader_ctx{mcxt_, dsa_, LEADER_WORKER_INDEX, control_, event_id};
				for (PhysicalOperator *op : pipeline->ops)
					if (op->type() == PhysicalOperatorType::HASH_JOIN_PROBE)
						static_cast<PhysicalHashJoinProbe *>(op)->InitializeHybrid(leader_ctx, task_count);
			}
			break;
		case TaskKind::COMBINE:
			event_id   = base_id + 1;
			if (sink->CombineIsTrivial())
			{
				EventShmState &shm = event_shm_[event_id];
				pg_atomic_write_u32(&shm.tasks_remaining, 0);
				event.FinishEvent();
				return;
			}
			if (pipeline->sink->type() == PhysicalOperatorType::HASH_AGGREGATE)
			{
				auto *hash_agg = static_cast<PhysicalHashAggregate *>(pipeline->sink);
				dsa_pointer payload_dp = LoadSharedPayloadFromDescriptor(hash_agg);
				if (!DsaPointerIsValid(payload_dp))
					elog(ERROR, "pg_yaap: hash aggregate payload missing during COMBINE scheduling");
				auto *payload = static_cast<HashAggSharedPayload *>(dsa_get_address(dsa_, payload_dp));
				task_count = payload->partition_count;
			}
			else if (pipeline->sink->type() == PhysicalOperatorType::PERFECT_HASH_AGGREGATE)
			{
				auto *perfect_hash_agg = static_cast<PhysicalPerfectHashAggregate *>(pipeline->sink);
				dsa_pointer payload_dp = LoadSharedPayloadFromDescriptor(perfect_hash_agg);
				if (!DsaPointerIsValid(payload_dp))
					elog(ERROR, "pg_yaap: perfect hash aggregate payload missing during COMBINE scheduling");
				task_count = 1;
			}
			else if (pipeline->sink->type() == PhysicalOperatorType::HASH_JOIN)
			{
				auto *hash_join = static_cast<PhysicalHashJoin *>(pipeline->sink);
				dsa_pointer payload_dp = LoadSharedPayloadFromDescriptor(hash_join);
				if (!DsaPointerIsValid(payload_dp))
					elog(ERROR, "pg_yaap: hash join payload missing during COMBINE scheduling");
				auto *payload = static_cast<HashJoinSharedPayload *>(dsa_get_address(dsa_, payload_dp));
				segmented_hash_join_worker_combine = payload->direct_segmented_build &&
					payload->build_partition_count > 1;
				task_count = segmented_hash_join_worker_combine
					? RememberedRunTaskCount(pid)
					: (payload->build_partition_count > 1
						? payload->build_partition_count : RememberedRunTaskCount(pid));
			}
			else if (pipeline->sink->type() == PhysicalOperatorType::CROSS_PRODUCT)
			{
				auto *cross_product = static_cast<PhysicalCrossProduct *>(pipeline->sink);
				dsa_pointer payload_dp = LoadSharedPayloadFromDescriptor(cross_product);
				if (!DsaPointerIsValid(payload_dp))
					elog(ERROR, "pg_yaap: cross product payload missing during COMBINE scheduling");
				task_count = RememberedRunTaskCount(pid);
			}
			else
			{
				/* One combine task per worker that participated in Run; matches
				 * the Run fan-out for non-partition-owner sinks. */
				task_count = RememberedRunTaskCount(pid);
			}
			break;
		case TaskKind::FINALIZE:
			event_id   = base_id + 2;
			if (sink->FinalizeIsTrivial())
			{
				EventShmState &shm = event_shm_[event_id];
				pg_atomic_write_u32(&shm.tasks_remaining, 0);
				{
					ExecCtx leader_ctx{mcxt_, dsa_, LEADER_WORKER_INDEX, control_, event_id};
					auto &ps = state_.GetOrCreate(pid);
					if (ps.global_sink == nullptr)
						ps.global_sink = sink->GetGlobalSinkState(leader_ctx);
					if (ps.global_sink != nullptr)
					{
						PipelineTimelineTaskScope timeline_scope(leader_ctx, event_id);
						SinkFinalizeType fres = sink->Finalize(leader_ctx, *ps.global_sink);
						if (fres != SinkFinalizeType::READY)
							elog(ERROR, "pg_yaap: trivial finalize did not finish");
					}
				}
				event.FinishEvent();
				return;
			}
			if (pipeline->sink->type() == PhysicalOperatorType::HASH_JOIN)
			{
				auto *hash_join = static_cast<PhysicalHashJoin *>(pipeline->sink);
				dsa_pointer payload_dp = LoadSharedPayloadFromDescriptor(hash_join);
				if (!DsaPointerIsValid(payload_dp))
					elog(ERROR, "pg_yaap: hash join payload missing during FINALIZE scheduling");
				auto *payload = static_cast<HashJoinSharedPayload *>(dsa_get_address(dsa_, payload_dp));
				parallel_hash_join_finalize = payload->direct_segmented_build &&
					payload->build_partition_count > 1 && !payload->partition_probe_mode;
				if (parallel_hash_join_finalize)
				{
					task_count = payload->build_partition_count;
					/* Each task owns a partition directory and up to four key
					 * segment leases. Use the largest partition to budget concurrent
					 * builders from the hard/soft-limit headroom. */
					uint64_t largest_rows = 0;
					auto *registry = static_cast<const HashJoinLocalBuildRegistryEntry *>(
						dsa_get_address(dsa_, payload->local_build_registry_dp));
					if (registry == nullptr)
						elog(ERROR, "pg_yaap: hash join finalize registry missing");
					for (uint32_t part = 0; part < task_count; ++part)
					{
						uint64_t rows = 0;
						for (uint32_t slot = 0; slot < payload->local_state_slot_count; ++slot)
						{
							if (!DsaPointerIsValid(registry[slot].partition_state_dp))
								continue;
							const auto *parts = static_cast<const HashJoinLocalPartitionState *>(
								dsa_get_address(dsa_, registry[slot].partition_state_dp));
							rows += parts[part].row_count;
						}
						largest_rows = std::max(largest_rows, rows);
					}
					uint64_t buckets = 1;
					while (buckets < largest_rows * 2u)
						buckets <<= 1;
					auto *budget = static_cast<buffer::YaapBufferSharedState *>(
						dsa_get_address(dsa_, control_->buffer_state_root));
					auto *arena = buffer::YaapSharedSegmentArenaResolve(dsa_,
						payload->segment_arena_dp);
					if (budget == nullptr || arena == nullptr)
						elog(ERROR, "pg_yaap: hash join finalize memory budget missing");
					const uint64_t task_bytes = buckets * sizeof(uint32_t) +
						largest_rows * (sizeof(uint32_t) * 3u + sizeof(int64_t)) +
						4u * static_cast<uint64_t>(arena->segment_size);
					const uint64_t headroom = budget->hard_limit > budget->soft_limit
						? budget->hard_limit - budget->soft_limit : budget->hard_limit / 4u;
					hash_join_finalize_workers = static_cast<uint32_t>(
						std::max<uint64_t>(1u, std::min<uint64_t>(
							DeriveRunTaskCount(*pipeline), headroom / task_bytes)));
					break;
				}
			}
			/* Finalize is leader-only for ordinary sinks (Sink::Finalize is a
			 * leader op). Partitioned segmented hash joins dispatch one task per
			 * radix partition so their hash directories can be built in parallel.
			 * leader's worker_index is LEADER_WORKER_INDEX; C7's event loop
			 * runs Finalize inline rather than dispatching to the DSM
			 * queue. We still publish 1 to tasks_remaining so the C7 loop
			 * uses the same count==0 transition signal. */
			task_count = 1;
			break;
		default:
			Assert(false);
			return;
	}

	Assert(events_.Resolve(event_id) == &event);

	EventShmState &shm = event_shm_[event_id];

	/* MUST init tasks_remaining BEFORE the first TryPush. Workers decrement
	 * after each Task::Execute; if even one push happens before the counter
	 * is set to N, a worker may observe tasks_remaining=0 and prematurely
	 * trigger the leader's FinishEvent. */
	pg_atomic_write_u32(&shm.tasks_remaining, task_count);

	if (kind == TaskKind::FINALIZE && !parallel_hash_join_finalize)
		return;

	for (uint32_t i = 0; i < task_count; i++)
	{
		TaskDescriptor desc{};
		desc.pipeline_id  = static_cast<uint32_t>(pid);
		desc.event_id     = event_id;
		desc.partition_id = UINT32_MAX;
		desc.worker_index = static_cast<int32_t>(i);
		desc.kind         = static_cast<uint8_t>(kind);
			if (kind == TaskKind::COMBINE &&
				pipeline->sink->type() == PhysicalOperatorType::HASH_AGGREGATE)
		{
			desc.partition_id = i;
			desc.worker_index = static_cast<int32_t>(i % DeriveRunTaskCount(*pipeline));
			}
			else if (kind == TaskKind::COMBINE &&
				pipeline->sink->type() == PhysicalOperatorType::HASH_JOIN)
			{
				if (segmented_hash_join_worker_combine)
				{
					desc.partition_id = UINT32_MAX;
					desc.worker_index = static_cast<int32_t>(i);
				}
				else
				{
					auto *hash_join = static_cast<PhysicalHashJoin *>(pipeline->sink);
					dsa_pointer payload_dp = LoadSharedPayloadFromDescriptor(hash_join);
					auto *payload = DsaPointerIsValid(payload_dp)
						? static_cast<HashJoinSharedPayload *>(dsa_get_address(dsa_, payload_dp)) : nullptr;
					if (payload != nullptr && payload->build_partition_count > 1)
					{
						desc.partition_id = i;
						desc.worker_index = static_cast<int32_t>(i % DeriveRunTaskCount(*pipeline));
					}
				}
			}
			else if (kind == TaskKind::FINALIZE && parallel_hash_join_finalize)
			{
				desc.partition_id = i;
				desc.worker_index = static_cast<int32_t>(i % hash_join_finalize_workers);
			}

		/* Do not block scheduling while the bounded DSM queue is full.
		 * The leader drains this private backlog as workers free slots. */
		pending_tasks_.push_back(desc);
	}
}

void
TaskScheduler::NotifyPartitionProbeRunFinished(EventId event_id)
{
	if (active_partition_probe_run_ != event_id)
		return;

	active_partition_probe_run_ = INVALID_EVENT_ID;
	while (!pending_partition_probe_runs_.empty())
	{
		Event *next = pending_partition_probe_runs_.front();
		pending_partition_probe_runs_.erase(pending_partition_probe_runs_.begin());
		if (next->aborted())
			continue;
		Assert(next->state() == EventState::SCHEDULED);
		EnqueueTasks(*next);
		break;
	}
}

bool
TaskScheduler::DispatchPendingTasks()
{
	bool progress = false;
	while (pending_cursor_ < pending_tasks_.size())
	{
		if (!task_queue_->TryPush(pending_tasks_[pending_cursor_]))
			break;
		++pending_cursor_;
		progress = true;
	}
	if (pending_cursor_ == pending_tasks_.size())
	{
		pending_tasks_.clear();
		pending_cursor_ = 0;
	}
	return progress;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
