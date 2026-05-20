#pragma once

/*
 * pipeline/pipeline_combine_event.hpp  (M-FRAME-MIN step 3c)
 *
 * Phase 2 of the 3-event lifecycle locked by PIPELINE_PORT_PLAN.md §15.1.
 * Schedules N PipelineCombineTasks (one per worker that produced LocalSinkState
 * in PipelineRunEvent) to invoke Sink::Combine, merging local partials into
 * the GlobalSinkState. Skipped entirely if upstream PipelineRunEvent ABORTed
 * (Oracle B2: Combine-only-on-success); the dependency edge handles this via
 * Event::saw_aborted_dependency_ -> Abort() in CompleteDependency.
 */

#include "parallel/pipeline/event.hpp"

namespace pg_yaap {
namespace pipeline {

struct Pipeline;

class PipelineCombineEvent final : public Event {
public:
	PipelineCombineEvent(PipelineId pid, Pipeline *pipeline,
	                     TaskScheduler *scheduler);

	void     Schedule() override;
	TaskKind kind() const override;

private:
	Pipeline *pipeline_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
