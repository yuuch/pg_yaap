#include "parallel/pipeline/pipeline_combine_event.hpp"

#include "parallel/pipeline/dsm_task_queue.hpp"
#include "parallel/pipeline/task_scheduler.hpp"

namespace pg_yaap {
namespace pipeline {

PipelineCombineEvent::PipelineCombineEvent(PipelineId pid, Pipeline *pipeline,
                                           TaskScheduler *scheduler)
    : Event(pid, scheduler), pipeline_(pipeline) {}

void
PipelineCombineEvent::Schedule()
{
	Assert(pipeline_ != nullptr);
	scheduler_->EnqueueTasks(*this);
}

TaskKind
PipelineCombineEvent::kind() const
{
	return TaskKind::COMBINE;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
