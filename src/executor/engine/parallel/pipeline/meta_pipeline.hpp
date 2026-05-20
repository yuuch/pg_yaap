#pragma once

/*
 * pipeline/meta_pipeline.hpp  (M-FRAME-MIN step 3b)
 *
 * MetaPipeline: slices a PhysicalOperator tree into a DAG of Pipelines at
 * IsPipelineBreaker() boundaries. DuckDB-faithful, but specialised for the
 * Q1-only operator set in M-FRAME-MIN (SeqScan, HashAggregate, Order,
 * OutputSink). The algorithm itself is generic — Q1 specificity comes only
 * from which PhysicalOperator subclasses exist; encountering an unknown one
 * is the responsibility of Translator (ereport ERROR there, not here).
 *
 * Ownership model:
 *   - MetaPipelineBundle owns the PhysicalOperator tree (root unique_ptr)
 *     AND the unique_ptr<Pipeline> vector. It is the single root of all
 *     M-FRAME-MIN per-query lifetime.
 *   - Pipeline references PhysicalOperator* as non-owning views into the
 *     tree owned by the bundle.
 *   - depends_on[i] is the list of Pipeline ids whose sink must Finalize
 *     before pipeline i may Run. The first build slot (id 0) is always the
 *     leaf-most producer pipeline (deepest sink).
 *
 * Spec: PIPELINE_PORT_PLAN.md §15.3.2; GLOBAL_LOCAL_STATE_DESIGN.md §8.3.
 */

#include <memory>

#include "core/memory.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

class PhysicalOperator;

struct MetaPipelineBundle : public PgMemoryContextObject {
	std::unique_ptr<PhysicalOperator>  root;
	PgVector<std::unique_ptr<Pipeline>> pipelines;
};

class MetaPipeline {
public:
	static std::unique_ptr<MetaPipelineBundle>
	Build(std::unique_ptr<PhysicalOperator> root);

	/*
	 * Mutator API used by PhysicalOperator::BuildPipelines. Public because
	 * subclasses that override the default walker (none in M-FRAME-MIN, but
	 * reserved for future joins) need to call them; pointer semantics keep
	 * the bundle as the single owner.
	 */
	Pipeline &CreatePipeline();
	Pipeline &CreateChildPipeline(Pipeline &parent, PhysicalOperator &sink);
	void      AddOperator(Pipeline &p, PhysicalOperator &op);
	void      SetSource  (Pipeline &p, PhysicalOperator &source);
	void      SetSink    (Pipeline &p, PhysicalOperator &sink);

	MetaPipelineBundle &bundle() { return *bundle_; }

private:
	MetaPipeline() : bundle_(std::make_unique<MetaPipelineBundle>()) {}

	std::unique_ptr<MetaPipelineBundle>  bundle_;
	PipelineId                           next_id_ = 0;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
