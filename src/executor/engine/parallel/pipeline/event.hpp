#pragma once

/*
 * pipeline/event.hpp  (M-FRAME-MIN step 3c)
 *
 * Event base class for the 3-event pipeline lifecycle (Run -> Combine ->
 * Finalize). Mirrors the role of duckdb::Event but specialised for the
 * Q1-only narrowed scope locked by PIPELINE_PORT_PLAN.md §15.1: Initialize
 * and PrepareFinish events from design §8.4 are folded into leader setup
 * and into Finalize respectively.
 *
 * Lifecycle invariants enforced by this class hierarchy:
 *   1. Pipelines may declare upstream Event dependencies; an Event becomes
 *      schedulable only after every dependency has called CompleteDependency.
 *   2. CompleteDependency is wait-free (atomic decrement). When the counter
 *      reaches zero the Event is handed to the scheduler via Schedule().
 *   3. Cross-pipeline edge: if Pipeline B's Pipeline.depends_on contains
 *      Pipeline A, then B.RunEvent depends on A.FinalizeEvent. This is the
 *      ONLY inter-pipeline edge in M-FRAME-MIN.
 *   4. ABORT propagation: a failed RunEvent must skip its CombineEvent and
 *      its FinalizeEvent's user code, but MUST still call CompleteDependency
 *      on every downstream Event to prevent deadlock.
 *
 * BLOCKED is illegal in M-FRAME-MIN — see pipeline/AGENTS.md ANTI-PATTERNS.
 *
 * Spec: PIPELINE_PORT_PLAN.md §15.1, §15.3.2; GLOBAL_LOCAL_STATE_DESIGN.md
 *       §8.4 (5-event design overridden by §15.1 to 3-event subset).
 */

#include <atomic>
#include <cstdint>
#include <memory>

extern "C" {
#include "postgres.h"
}

#include "core/memory.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

class TaskScheduler;

/* Forward decl from dsm_task_queue.hpp (enum class with fixed underlying type
 * is forward-declarable; full definition only needed where kind() is used). */
enum class TaskKind : uint8_t;

enum class EventState : uint8_t {
	PENDING,
	SCHEDULED,
	RUNNING,
	COMBINE_PENDING,
	FINISHED,
	ABORTED,
};

class Event : public PgMemoryContextObject,
              public std::enable_shared_from_this<Event> {
public:
	Event(PipelineId pid, TaskScheduler *scheduler);
	virtual ~Event() = default;

	Event(const Event &)            = delete;
	Event &operator=(const Event &) = delete;

	PipelineId  pipeline_id() const { return pipeline_id_; }
	EventState  state()       const { return state_.load(std::memory_order_acquire); }
	bool        aborted()     const { return state() == EventState::ABORTED; }

	void AddDependency(const std::shared_ptr<Event> &parent);

	/* Called by upstream events on transition to FINISHED or ABORTED. */
	void CompleteDependency(bool upstream_aborted);

	/* Transition PENDING -> SCHEDULED and enqueue this event's tasks. */
	bool TrySchedule();

	/* Mark this event aborted and cascade CompleteDependency to dependents. */
	void Abort();

	/* Mark this event FINISHED (success) and cascade CompleteDependency. */
	void FinishEvent();

	/* Subclass hook: enqueue the event's tasks to the scheduler. */
	virtual void Schedule() = 0;

	/* Subclass hook: report event kind so the scheduler can build the
	 * correct TaskDescriptor without RTTI. Mirrors the 3-event lifecycle. */
	virtual TaskKind kind() const = 0;

protected:
	void NotifyDependents(bool propagate_abort);

	PipelineId                                pipeline_id_;
	TaskScheduler                            *scheduler_;
	std::atomic<EventState>                   state_{EventState::PENDING};
	std::atomic<int32_t>                      pending_dependencies_{0};
	std::atomic<bool>                         saw_aborted_dependency_{false};
	PgVector<std::weak_ptr<Event>>            dependents_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
