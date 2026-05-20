/*
 * pipeline/event.cpp  (M-FRAME-MIN step 3c)
 *
 * See event.hpp for lifecycle invariants and spec backrefs.
 */

#include "parallel/pipeline/event.hpp"

namespace pg_yaap {
namespace pipeline {

Event::Event(PipelineId pid, TaskScheduler *scheduler)
    : pipeline_id_(pid), scheduler_(scheduler) {}

void
Event::AddDependency(const std::shared_ptr<Event> &parent)
{
	if (!parent)
		return;
	pending_dependencies_.fetch_add(1, std::memory_order_relaxed);
	parent->dependents_.emplace_back(weak_from_this());
}

void
Event::CompleteDependency(bool upstream_aborted)
{
	if (upstream_aborted)
		saw_aborted_dependency_.store(true, std::memory_order_release);

	int32_t prev = pending_dependencies_.fetch_sub(1, std::memory_order_acq_rel);
	if (prev != 1)
		return;

	if (saw_aborted_dependency_.load(std::memory_order_acquire))
	{
		Abort();
		return;
	}

	TrySchedule();
}

bool
Event::TrySchedule()
{
	EventState expected = EventState::PENDING;
	if (!state_.compare_exchange_strong(expected, EventState::SCHEDULED,
	                                    std::memory_order_acq_rel))
		return false;

	Schedule();
	return true;
}

void
Event::Abort()
{
	EventState prev = state_.exchange(EventState::ABORTED,
	                                  std::memory_order_acq_rel);
	if (prev == EventState::ABORTED)
		return;
	NotifyDependents(true);
}

void
Event::FinishEvent()
{
	EventState prev = state_.exchange(EventState::FINISHED,
	                                  std::memory_order_acq_rel);
	if (prev == EventState::FINISHED || prev == EventState::ABORTED)
		return;
	NotifyDependents(false);
}

void
Event::NotifyDependents(bool propagate_abort)
{
	for (auto &weak : dependents_)
	{
		if (auto child = weak.lock())
			child->CompleteDependency(propagate_abort);
	}
	dependents_.clear();
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
