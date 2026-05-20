#pragma once

/*
 * pipeline/task_scheduler.hpp  (M-FRAME-MIN step 3g.2 — C1 extension)
 *
 * Per-query orchestrator for the 3-event pipeline runtime. Locked by:
 *   docs/PIPELINE_PORT_PLAN.md §15.5
 *   docs/GLOBAL_LOCAL_STATE_DESIGN.md §8.5
 *   .sisyphus/plans/3g2-final-delta-map.md (v2)
 *
 * 3g.1 shipped: id assignment, Event construction, DSM size estimation.
 *
 * 3g.2 / C1 (this commit) adds:
 *   - SchedulerState: per-pipeline / per-worker Source/Sink/Operator state
 *     tables, lazy-init on first access. Pipeline struct stays a metadata
 *     view (no state pointers); ALL execution state lives here in the
 *     leader process for the duration of the query.
 *   - BindRuntime: leader hands the scheduler the already-attached
 *     PipelineSharedControl + DsmTaskQueue + dsa_area pointers. The
 *     scheduler does NOT own DSM segments; bridge / leader own them.
 *   - AllocateEventShmStates: dsa_allocate0 the EventShmState[event_count]
 *     array (Oracle C7 protocol) and publish via control->event_states_root
 *     + control->event_count.
 *   - EnqueueTasks: real body. Discriminates RUN / COMBINE / FINALIZE via
 *     Event::kind(); decides task count (RUN/COMBINE = N workers bounded
 *     by source->MaxThreads(); FINALIZE = 1 leader-only); stores N into
 *     EventShmState.tasks_remaining BEFORE pushing; pushes N TaskDescriptors
 *     via DsmTaskQueue::TryPush (which auto-wakes worker latches).
 *
 * 3g.2 / C7 (later, separate file pipeline_leader.cpp) will add:
 *   - bgworker launch via ParallelContext (workers register with the
 *     scheduler-owned DsmTaskQueue latch table BEFORE seeding any task).
 *   - Leader event loop: WaitLatch -> drain tasks_remaining==0 events ->
 *     FinishEvent / Abort -> CHECK_FOR_INTERRUPTS -> repeat until all
 *     events FINISHED or any aborted.
 *
 * 3g.2 / C6 (task.cpp) will add Source -> Operator -> Sink driving inside
 * Task::Execute, reading state via SchedulerState accessors.
 *
 * Lifetime: leader-process-local. Owned by PgYaapQueryState (bridge).
 * The scheduler dies when the query dies; DSM detach is the bridge's job.
 *
 * Q1-narrowed scope today. ParallelSink()/ParallelSource()/MaxThreads()
 * widen at M-Q1-PERF.
 */

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

extern "C" {
#include "postgres.h"
#include "utils/dsa.h"
}

#include "core/memory.hpp"
#include "parallel/pipeline/event.hpp"
#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/operator.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/pipeline_dsm_lookup.hpp"
#include "parallel/pipeline/sink.hpp"
#include "parallel/pipeline/source.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

class DsmTaskQueue;
struct PipelineSharedControl;
struct EventShmState;
struct TaskDescriptor;

/* DSM sizing inputs decided by the leader before dsm_create. */
struct TaskSchedulerSizing {
	uint32_t worker_count;       /* including leader (worker_index = -1) */
	uint32_t task_queue_capacity;/* power of two */
};

/* DSM section sizes returned by EstimateDsmSize, also used by the leader's
 * shm_toc allocator to compute key offsets. */
struct TaskSchedulerDsmLayout {
	size_t control_bytes;        /* PipelineSharedControl */
	size_t task_queue_bytes;     /* DsmTaskQueue + cells */
	size_t event_counters_bytes; /* per-event EventShmState (DSA-resident; this
	                              * field tracks the size pre-allocated in the
	                              * scheduler's reserved DSA region budget) */
	size_t total_bytes;          /* sum of DSM-segment-resident sections only;
	                              * caller adds shm_toc overhead + DSA budget */
};

/*
 * Per-pipeline execution state, owned by SchedulerState. Lazy-init on first
 * access. Per-worker vectors are sized to scheduler.worker_count() + 1 to
 * accommodate the leader's own slot at LEADER_WORKER_INDEX (mapped to slot 0
 * in this table; worker_index 0..N-1 maps to slots 1..N).
 *
 * GlobalSourceState / GlobalSinkState / GlobalOperatorState live once per
 * pipeline (leader-built). LocalSourceState / LocalSinkState / OperatorState
 * are per-worker. C6 (task.cpp) reads/writes these via SchedulerState
 * accessors below.
 */
struct PerPipelineState {
	std::unique_ptr<GlobalSourceState>            global_source;
	std::unique_ptr<GlobalSinkState>              global_sink;
	PgVector<std::unique_ptr<GlobalOperatorState>> global_ops;

	PgVector<std::unique_ptr<LocalSourceState>>   local_source_per_worker;
	PgVector<std::unique_ptr<LocalSinkState>>     local_sink_per_worker;
	PgVector<PgVector<std::unique_ptr<OperatorState>>> local_op_states_per_worker;

	explicit PerPipelineState(MemoryContext mcxt);
};

/*
 * Per-pipeline-table indexed by PipelineId. Entries created lazily by
 * SchedulerState::GetOrCreate. Owned by TaskScheduler.
 */
class SchedulerState {
public:
	SchedulerState(MemoryContext mcxt, uint32_t worker_count);

	/* Lazy-init slot for pipeline `pid`. */
	PerPipelineState &GetOrCreate(PipelineId pid);

	/* Read-side accessor. Asserts the slot exists. */
	PerPipelineState &Get(PipelineId pid);

private:
	MemoryContext                              mcxt_;
	[[maybe_unused]] uint32_t                  worker_count_;  /* C7 will read for per-worker state sizing */
	PgVector<std::unique_ptr<PerPipelineState>> per_pipeline_;  /* indexed by PipelineId */
};

class TaskScheduler {
public:
	TaskScheduler(MemoryContext              mcxt,
	              std::unique_ptr<MetaPipelineBundle> bundle,
	              const TaskSchedulerSizing &sizing);

	~TaskScheduler() = default;

	TaskScheduler(const TaskScheduler &)            = delete;
	TaskScheduler &operator=(const TaskScheduler &) = delete;

	/* Compute the DSM byte budget for the given sizing. event_count is
	 * determined by BuildEvents() and so MUST be called after BuildEvents.
	 * The event_counters_bytes field tracks the DSA budget the leader must
	 * reserve for AllocateEventShmStates; it is NOT part of the inline DSM
	 * segment. */
	TaskSchedulerDsmLayout EstimateDsmSize() const;

	/* Build the 3 Events per Pipeline (Run -> Combine -> Finalize) and the
	 * inter-pipeline dependency edges. Assigns EventIds and registers them
	 * in event_lookup_. Idempotent: callable exactly once per scheduler. */
	void BuildEvents();

	/* C1 NEW: leader hands the scheduler the already-attached shared
	 * structures. Call after dsm_create + DsmTaskQueue::InitInPlace +
	 * dsa_create, before AllocateEventShmStates / Schedule. The scheduler
	 * does NOT take ownership; bridge owns the segment lifecycle. */
	void BindRuntime(PipelineSharedControl *control,
	                 DsmTaskQueue          *task_queue,
	                 dsa_area              *dsa);

	/* C1 NEW: dsa_allocate0 EventShmState[event_count()] in the bound
	 * dsa_area, populate control->event_states_root + control->event_count,
	 * cache the mapped pointer. MUST be called after BuildEvents +
	 * BindRuntime, before any Event::Schedule(). */
	void AllocateEventShmStates();

	/* Concrete *Event::Schedule() implementations call this. C1 NEW body
	 * dispatches by Event::kind() and fans out N TaskDescriptors. */
	void EnqueueTasks(Event &event);

	/* Read-side accessors (used by task bodies in C6 + leader loop in C7). */
	MetaPipelineBundle           &bundle()              { return *bundle_; }
	MemoryContext                 mcxt()          const { return mcxt_; }
	PipelineDsmLookup<Pipeline>  &pipeline_lookup()     { return pipelines_; }
	PipelineDsmLookup<Event>     &event_lookup()        { return events_; }
	uint32_t                      event_count()   const { return next_event_id_; }
	uint32_t                      worker_count()  const { return sizing_.worker_count; }
	const TaskSchedulerSizing    &sizing()        const { return sizing_; }

	/* C1 NEW: shared-state accessors. Null if BindRuntime not yet called. */
	PipelineSharedControl        *control()       const { return control_; }
	DsmTaskQueue                 *task_queue()    const { return task_queue_; }
	dsa_area                     *dsa()           const { return dsa_; }
	EventShmState                *event_shm()     const { return event_shm_; }
	SchedulerState               &state()                { return state_; }

	/* C1 NEW: convenience for C6. Returns the EventShmState slot for an
	 * Event by id; asserts AllocateEventShmStates has been called. */
	EventShmState                &event_shm_slot(EventId id);

private:
	/* Internal helper: derive the per-Run/Combine task fan-out count for
	 * a pipeline. Currently bounded by source->MaxThreads(). */
	uint32_t DeriveRunTaskCount(Pipeline &pipeline) const;
	void RememberRunTaskCount(PipelineId pid, uint32_t task_count);
	uint32_t RememberedRunTaskCount(PipelineId pid) const;

	MemoryContext                                  mcxt_;
	std::unique_ptr<MetaPipelineBundle>            bundle_;
	TaskSchedulerSizing                            sizing_;

	PipelineDsmLookup<Pipeline>                    pipelines_;
	PipelineDsmLookup<Event>                       events_;

	PgVector<std::shared_ptr<Event>>               events_owned_;
	PgVector<uint32_t>                             run_task_counts_;
	uint32_t                                       next_event_id_ = 0;

	/* C1 NEW: shared runtime pointers (bound by BindRuntime; not owned). */
	PipelineSharedControl                         *control_    = nullptr;
	DsmTaskQueue                                  *task_queue_ = nullptr;
	dsa_area                                      *dsa_        = nullptr;
	dsa_pointer                                    event_shm_dp_ = InvalidDsaPointer;
	EventShmState                                 *event_shm_  = nullptr;

	/* C1 NEW: per-pipeline / per-worker execution state. */
	SchedulerState                                 state_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
