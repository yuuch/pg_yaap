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

	/* Leader-only. Returning NO_OUTPUT_POSSIBLE cancels dependent pipelines. */
	virtual SinkFinalizeType
	Finalize(ExecCtx &ctx, GlobalSinkState &gstate) = 0;

	virtual bool ParallelSink() const { return true; }
	virtual bool CombineIsTrivial() const { return false; }
	virtual bool FinalizeIsTrivial() const { return false; }
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
