#include "parallel/pipeline/pipeline_finalize_event.hpp"

#include "parallel/pipeline/dsm_task_queue.hpp"
#include "parallel/pipeline/task_scheduler.hpp"

namespace pg_yaap {
namespace pipeline {

PipelineFinalizeEvent::PipelineFinalizeEvent(PipelineId pid, Pipeline *pipeline,
                                             TaskScheduler *scheduler)
    : Event(pid, scheduler), pipeline_(pipeline) {}

void
PipelineFinalizeEvent::Schedule()
{
	Assert(pipeline_ != nullptr);
	scheduler_->EnqueueTasks(*this);
}

TaskKind
PipelineFinalizeEvent::kind() const
{
	return TaskKind::FINALIZE;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
