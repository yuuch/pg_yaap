#pragma once

/*
 * pipeline/source.hpp
 *
 * Source interface (P1: declarations only, no implementations).
 * See PIPELINE_REFACTOR_DESIGN.md §3.3.
 */

#include <memory>

#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

class GlobalSourceState {
public:
	virtual ~GlobalSourceState() = default;
};

class LocalSourceState {
public:
	virtual ~LocalSourceState() = default;
};

struct OperatorSourceInput {
	GlobalSourceState &global_state;
	LocalSourceState  &local_state;
};

class Source {
public:
	virtual ~Source() = default;

	virtual std::unique_ptr<GlobalSourceState>
	GetGlobalSourceState(ExecCtx &ctx) = 0;

	virtual std::unique_ptr<LocalSourceState>
	GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate) = 0;

	virtual SourceResultType
	GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) = 0;

	virtual bool ParallelSource() const { return false; }
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
