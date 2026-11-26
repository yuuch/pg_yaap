#pragma once

/*
 * pipeline/sink.hpp
 *
 * Sink interface (P1: declarations only). See PIPELINE_REFACTOR_DESIGN.md §3.3, §5.
 */

#include <memory>

#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

class GlobalSinkState {
public:
	virtual ~GlobalSinkState() = default;
};

class LocalSinkState {
public:
	virtual ~LocalSinkState() = default;

	/*
	 * Optional zero-copy access to a DSA-published partial owned by another
	 * worker. Default returns nullptr; concrete sinks override only when the
	 * partial layout is safe to read in place (open question #2).
	 */
	virtual void *unsafe_borrow_partial(int /*worker_index*/) { return nullptr; }
};

struct OperatorSinkInput {
	GlobalSinkState &global_state;
	LocalSinkState  &local_state;
};

struct OperatorSinkCombineInput {
	LocalSinkState  *local_state;
	GlobalSinkState &global_state;
	uint32_t         partition_id = UINT32_MAX;
};

/* Leader-side stage expansion. Every task consumes shared state and may be
 * claimed by any executor. A zero-task stage is already finished by RUN-local
 * publication. */
struct SinkTaskPlan {
	uint32_t task_count;
	uint32_t worker_count;
	bool partitioned;
};

class Sink {
public:
	virtual ~Sink() = default;

	virtual std::unique_ptr<GlobalSinkState>
	GetGlobalSinkState(ExecCtx &ctx) = 0;

	virtual std::unique_ptr<LocalSinkState>
	GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate) = 0;

	/* Method name is SinkChunk (not Sink) to avoid C++ ctor name clash. */
	virtual SinkResultType
	SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) = 0;

	virtual SinkCombineResultType
	Combine(ExecCtx &ctx, OperatorSinkCombineInput &input) = 0;

	/* Runs after all COMBINE tasks have published their shared state. */
	virtual SinkFinalizeType
	Finalize(ExecCtx &ctx, GlobalSinkState &gstate) = 0;

	virtual bool ParallelSink() const { return true; }
	virtual bool CombineIsTrivial() const { return false; }
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
