#include "parallel/pipeline/physical_operator.hpp"

#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/pipeline.hpp"

namespace pg_yaap {
namespace pipeline {

void
PhysicalOperator::BuildPipelines(Pipeline &current, MetaPipeline &meta)
{
	/*
	 * Terminal sink (e.g. OutputSink): becomes the sink of the current
	 * pipeline; keep walking down with the same pipeline so the upstream
	 * producer fills source/ops on `current` (no new child pipeline).
	 */
	if (IsSink() && !IsPipelineBreaker())
	{
		meta.SetSink(current, *this);
		Assert(children_.size() == 1);
		children_[0]->BuildPipelines(current, meta);
		return;
	}

	/*
	 * Pipeline breaker (HashAggregate, Order, ...): from the consumer side
	 * (current pipeline) it acts as the source we read from. We then spawn
	 * a new producer pipeline where the same operator is the sink, and walk
	 * the children into that producer pipeline.
	 */
	if (IsPipelineBreaker())
	{
		meta.SetSource(current, *this);

		Pipeline &producer = meta.CreateChildPipeline(current, *this);
		meta.SetSink(producer, *this);
		producer.source = nullptr;
		Assert(children_.size() == 1);
		children_[0]->BuildPipelines(producer, meta);
		return;
	}

	if (children_.empty())
	{
		Assert(IsSource());
		meta.SetSource(current, *this);
		return;
	}

	Assert(children_.size() == 1);
	children_[0]->BuildPipelines(current, meta);
	meta.AddOperator(current, *this);
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
