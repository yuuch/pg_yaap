#pragma once

/*
 * pipeline/pipeline_finalize_event.hpp  (M-FRAME-MIN step 3c)
 *
 * Phase 3 of the 3-event lifecycle locked by PIPELINE_PORT_PLAN.md §15.1.
 * Leader-only: invokes Sink::Finalize on the GlobalSinkState exactly once.
 * On FINISHED, fans out CompleteDependency to dependent pipelines'
 * RunEvents (the sole inter-pipeline edge in M-FRAME-MIN). On Sink::Finalize
 * returning NO_OUTPUT_POSSIBLE, this event still transitions to FINISHED but
 * downstream pipelines should observe an empty source -- handled in 3g.
 *
 * Initialize and PrepareFinish events from GLOBAL_LOCAL_STATE_DESIGN §8.4
 * are INTENTIONALLY ABSENT here -- folded into leader query setup and into
 * Finalize itself by plan §15.1's narrowed scope.
 */

#include "parallel/pipeline/event.hpp"

namespace pg_yaap {
namespace pipeline {

struct Pipeline;

class PipelineFinalizeEvent final : public Event {
public:
	PipelineFinalizeEvent(PipelineId pid, Pipeline *pipeline,
	                      TaskScheduler *scheduler);

	void     Schedule() override;
	TaskKind kind() const override;

private:
	Pipeline *pipeline_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
