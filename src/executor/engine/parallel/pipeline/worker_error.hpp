#pragma once

#include <cstdarg>
#include <cstdio>

#include "parallel/pipeline/dsm_control.hpp"

namespace pg_yaap {
namespace pipeline {

/* A reporter owns the buffer before writing it; READY publishes the complete,
 * immutable diagnostic. WRITING is already a failure for cancellation purposes,
 * but readers must not inspect the buffer until they acquire READY. */
enum class WorkerErrorState : uint32
{
	NONE = 0,
	WRITING = 1,
	READY = 2,
};

inline bool
WorkerErrorPending(PipelineSharedControl *control)
{
	return pg_atomic_read_u32(&control->worker_error) !=
		static_cast<uint32>(WorkerErrorState::NONE);
}

inline bool
WorkerErrorReady(PipelineSharedControl *control)
{
	return pg_atomic_read_membarrier_u32(&control->worker_error) ==
		static_cast<uint32>(WorkerErrorState::READY);
}

inline bool
TryBeginWorkerError(PipelineSharedControl *control)
{
	uint32 expected = static_cast<uint32>(WorkerErrorState::NONE);
	return pg_atomic_compare_exchange_u32(&control->worker_error, &expected,
		static_cast<uint32>(WorkerErrorState::WRITING));
}

inline void
FinishWorkerError(PipelineSharedControl *control)
{
	pg_atomic_write_membarrier_u32(&control->worker_error,
		static_cast<uint32>(WorkerErrorState::READY));
}

inline const char *
WorkerErrorMessage(PipelineSharedControl *control)
{
	return WorkerErrorReady(control) ? control->worker_error_msg : nullptr;
}

inline void ReportWorkerError(PipelineSharedControl *control, const char *format, ...)
	pg_attribute_printf(2, 3);

inline void
ReportWorkerError(PipelineSharedControl *control, const char *format, ...)
{
	if (!TryBeginWorkerError(control))
		return;
	va_list args;
	va_start(args, format);
	vsnprintf(control->worker_error_msg, sizeof(control->worker_error_msg), format, args);
	va_end(args);
	FinishWorkerError(control);
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
