#pragma once

#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

enum class TimelineTaskResult : uint8_t { FINISHED, YIELDED, FAILED, CANCELLED };

void PipelineTimelineAllocate(PipelineSharedControl *, dsa_area *, uint32 event_count, uint32 workers);
void PipelineTimelineRegisterProcess(PipelineSharedControl *, dsa_area *, int worker_index, int pid);
void PipelineTimelineAbortProcess(PipelineSharedControl *, dsa_area *, int worker_index, bool cancelled);
void PipelineTimelineReady(PipelineSharedControl *, dsa_area *, uint32 pipeline_id);
void PipelineTimelineFinished(PipelineSharedControl *, dsa_area *, uint32 pipeline_id, bool aborted = false);
void PipelineTimelineReport(PipelineSharedControl *, dsa_area *, bool success);

class PipelineTimelineTaskScope {
public:
	PipelineTimelineTaskScope(const ExecCtx &, EventId, uint32 partition_id = UINT32_MAX);
	~PipelineTimelineTaskScope();
	void SetResult(TimelineTaskResult result) { result_ = result; }
	PipelineTimelineTaskScope(const PipelineTimelineTaskScope &) = delete;
	PipelineTimelineTaskScope &operator=(const PipelineTimelineTaskScope &) = delete;
private:
	const ExecCtx &ctx_;
	TimelineTaskResult result_ = TimelineTaskResult::FINISHED;
	bool active_ = false;
};

} // namespace pipeline
} // namespace pg_yaap
