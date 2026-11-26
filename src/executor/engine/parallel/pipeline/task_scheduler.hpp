#pragma once

#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

class DsmTaskQueue;
struct EventShmState;
struct MetaPipelineBundle;
struct Pipeline;
template <class T> class PipelineDsmLookup;

/* Each worker can publish ready work and advance the shared graph. A short
 * scheduling pass has one owner, including operator activation and cleanup.
 * The owner never waits for queue space or runs a pipeline task in the pass. */
class TaskScheduler {
public:
	static void BuildEvents(const MetaPipelineBundle &bundle, ExecCtx &ctx);

	TaskScheduler(ExecCtx &ctx, DsmTaskQueue &queue,
	              const PipelineDsmLookup<Pipeline> &pipelines);

	bool Advance();
	void CompleteTask(EventId id);

private:
	bool Activate(EventId id);
	void FinishEvent(EventId id);
	bool DispatchReadyTasks();

	ExecCtx &ctx_;
	DsmTaskQueue &queue_;
	const PipelineDsmLookup<Pipeline> &pipelines_;
	EventShmState *events_;
	const EventId *successors_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
