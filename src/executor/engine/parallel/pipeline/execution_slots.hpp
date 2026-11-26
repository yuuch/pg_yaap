#pragma once

#include <algorithm>

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

/* Reserve enough partial slots for the maximum concurrent RUN task cohort.
 * RUN identity belongs to the task, regardless of which process claims it. */
inline uint32_t ExecutionWorkerCount(const ExecCtx &ctx)
{
	if (ctx.control == nullptr)
		return 1;
	return static_cast<uint32_t>(ctx.control->num_workers);
}

inline uint32_t ExecutionTaskSlot(const ExecCtx &ctx)
{
	if (ctx.task_index != UINT32_MAX)
		return ctx.task_index;
	return ctx.worker_index == LEADER_WORKER_INDEX && ctx.control != nullptr
		? static_cast<uint32_t>(ctx.control->num_workers)
		: static_cast<uint32_t>(std::max(ctx.worker_index, 0));
}

} // namespace pipeline
} // namespace pg_yaap
