#pragma once

/*
 * pipeline/physical_operator.hpp
 *
 * PhysicalOperator IR (M-IR-MIN). DuckDB-faithful unified operator base that
 * fuses Source / Operator / Sink roles via IsSource()/IsSink() flags.
 *
 * Spec: PIPELINE_PORT_PLAN.md §15.4 (P3X-Q1).
 */

#include <memory>

#include "core/memory.hpp"
#include "parallel/pipeline/operator.hpp"
#include "parallel/pipeline/sink.hpp"
#include "parallel/pipeline/source.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

enum class PhysicalOperatorType : uint8_t {
	SEQ_SCAN,
	DELIM_SCAN,
	MATERIALIZE,
	HASH_AGGREGATE,
	PERFECT_HASH_AGGREGATE,
	HASH_JOIN,
	CROSS_PRODUCT,
	ORDER,
	TOP_N,
	OUTPUT,
	FILTER,
	PROJECTION,
	HASH_JOIN_PROBE,
};

class MetaPipeline;
struct Pipeline;

class PhysicalOperator : public PgMemoryContextObject {
public:
	explicit PhysicalOperator(PhysicalOperatorType type) : type_(type) {}
	virtual ~PhysicalOperator() = default;

	PhysicalOperatorType type() const { return type_; }
	const PgVector<std::unique_ptr<PhysicalOperator>> &children() const { return children_; }

	void AddChild(std::unique_ptr<PhysicalOperator> child) { children_.push_back(std::move(child)); }

	virtual bool IsSource() const { return false; }
	virtual bool IsSink() const { return false; }
	virtual bool ParallelSource() const { return false; }
	virtual bool SupportsInPlace() const { return false; }

	/* Pipeline-breakers split MetaPipeline construction. HashAggregate / Order = true. */
	virtual bool IsPipelineBreaker() const { return IsSink(); }

	virtual std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &ctx) {
		(void) ctx;
		return nullptr;
	}
	virtual std::unique_ptr<LocalSourceState> GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate) {
		(void) ctx; (void) gstate;
		return nullptr;
	}
	virtual SourceResultType GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) {
		(void) ctx; (void) out; (void) input;
		return SourceResultType::FINISHED;
	}

	virtual std::unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ExecCtx &ctx) {
		(void) ctx;
		return nullptr;
	}
	virtual std::unique_ptr<OperatorState> GetOperatorState(ExecCtx &ctx) {
		(void) ctx;
		return nullptr;
	}
	virtual OperatorResultType Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state) {
		(void) ctx; (void) in; (void) out; (void) state;
		return OperatorResultType::FINISHED;
	}
	virtual OperatorResultType ExecuteInPlace(ExecCtx &ctx, PipelineChunk &chunk, OperatorState &state) {
		(void) ctx; (void) chunk; (void) state;
		return OperatorResultType::FINISHED;
	}

	virtual std::unique_ptr<GlobalSinkState> GetGlobalSinkState(ExecCtx &ctx) {
		(void) ctx;
		return nullptr;
	}
	virtual std::unique_ptr<LocalSinkState> GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate) {
		(void) ctx; (void) gstate;
		return nullptr;
	}
	virtual SinkResultType SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) {
		(void) ctx; (void) in; (void) input;
		return SinkResultType::FINISHED;
	}
	virtual SinkCombineResultType Combine(ExecCtx &ctx, OperatorSinkCombineInput &input) {
		(void) ctx; (void) input;
		return SinkCombineResultType::FINISHED;
	}
	/* Complete the RUN's process-private sink writers before publishing task
	 * completion. Later COMBINE tasks must depend only on shared payloads. */
	virtual SinkCombineResultType FinishLocalSink(ExecCtx &ctx, OperatorSinkCombineInput &input) {
		(void) ctx; (void) input;
		return SinkCombineResultType::FINISHED;
	}
	virtual SinkFinalizeType Finalize(ExecCtx &ctx, GlobalSinkState &gstate) {
		(void) ctx; (void) gstate;
		return SinkFinalizeType::READY;
	}
	/* Operators own their stage partitioning and memory-concurrency policy.
	 * The scheduler expands this plan into freely claimable shared tasks. */
	virtual SinkTaskPlan GetCombineTaskPlan(ExecCtx &ctx, uint32_t run_tasks) const {
		(void) ctx;
		return {run_tasks, run_tasks, false};
	}
	virtual SinkTaskPlan GetFinalizeTaskPlan(ExecCtx &ctx, uint32_t run_tasks) const {
		(void) ctx; (void) run_tasks;
		return {1, 1, false};
	}

	virtual bool CombineIsTrivial() const { return false; }

	/* Worker count hint for the scheduler. Single-threaded ops return 1. */
	virtual int MaxThreads(ExecCtx &ctx) const {
		(void) ctx;
		return 0;
	}

	/*
	 * BuildPipelines — DuckDB-faithful default implementation that slices the
	 * PhysicalOperator tree into Pipelines at IsSink() boundaries.
	 *
	 * Behaviour (mirrors duckdb/src/parallel/physical_operator.cpp lines
	 * 285-325 of `BuildPipelines`):
	 *
	 *   - I am a Sink (pipeline-breaker): close the current pipeline using me
	 *     as its sink, open a NEW child pipeline whose source is also me
	 *     (dual-role), and recurse into my single child as the producer of
	 *     that new pipeline's sink.
	 *   - I am a pure Source (no children): set me as the source of the
	 *     current pipeline; recursion stops.
	 *   - I am a streaming Operator (single child, neither Sink nor Source):
	 *     append me to the current pipeline's ops and recurse into my child.
	 *
	 * Concrete operators only override this when they need custom dependency
	 * wiring beyond the standard child-walk (none in M-FRAME-MIN).
	 *
	 * Defined out-of-line in physical_operator.cpp because the body needs the
	 * full MetaPipeline definition.
	 */
	virtual void BuildPipelines(Pipeline &current, MetaPipeline &meta);

protected:
	PhysicalOperatorType                                  type_;

private:
	PgVector<std::unique_ptr<PhysicalOperator>>           children_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
