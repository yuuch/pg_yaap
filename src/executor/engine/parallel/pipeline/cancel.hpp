#pragma once

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "storage/procarray.h"
}

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

static inline bool
PipelineCancelRequested(ExecCtx &ctx)
{
	if (ctx.control != nullptr &&
		pg_atomic_read_u32(&ctx.control->shutdown_requested) != 0)
		return true;
	if (ctx.control != nullptr &&
		ctx.control->leader_pid != 0 &&
		ctx.control->leader_pid != MyProcPid &&
		BackendPidGetProc(ctx.control->leader_pid) == nullptr)
		return true;
	CHECK_FOR_INTERRUPTS();
	return false;
}

static inline bool
PipelineCancelRequestedEvery(ExecCtx &ctx, uint32_t counter,
                             uint32_t mask = 63u)
{
	if ((counter & mask) != 0)
		return false;
	return PipelineCancelRequested(ctx);
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
