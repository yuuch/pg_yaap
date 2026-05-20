#pragma once

/*
 * pipeline/pipeline_run_event.hpp  (M-FRAME-MIN step 3c)
 *
 * Phase 1 of the 3-event lifecycle locked by PIPELINE_PORT_PLAN.md §15.1.
 * Schedules N PipelineRunTasks (one per worker) that drive Source -> Operator
 * chain -> Sink::Sink for the bound Pipeline. On worker completion this event
 * transitions to FINISHED (success path -> CombineEvent eligible) or ABORTED
 * (failure path -> CombineEvent skipped, FinalizeEvent ABORTed downstream).
 *
 * Task instantiation is deferred to step 3d (Task) and step 3g (TaskScheduler).
 */

#include "parallel/pipeline/event.hpp"

namespace pg_yaap {
namespace pipeline {

struct Pipeline;

class PipelineRunEvent final : public Event {
public:
	PipelineRunEvent(PipelineId pid, Pipeline *pipeline, TaskScheduler *scheduler);

	void     Schedule() override;
	TaskKind kind() const override;

private:
	Pipeline *pipeline_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
