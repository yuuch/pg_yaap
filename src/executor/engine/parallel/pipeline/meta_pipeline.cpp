#include "parallel/pipeline/meta_pipeline.hpp"

#include "parallel/pipeline/physical_operator.hpp"

namespace pg_yaap {
namespace pipeline {

std::unique_ptr<MetaPipelineBundle>
MetaPipeline::Build(std::unique_ptr<PhysicalOperator> root)
{
	if (!root)
	{
		return nullptr;
	}

	std::unique_ptr<MetaPipeline> meta(new MetaPipeline());

	PhysicalOperator *root_ptr = root.get();
	meta->bundle_->root = std::move(root);

	Pipeline &top = meta->CreatePipeline();
	root_ptr->BuildPipelines(top, *meta);

	return std::move(meta->bundle_);
}

Pipeline &
MetaPipeline::CreatePipeline()
{
	auto p = std::make_unique<Pipeline>();
	p->id = next_id_++;
	Pipeline &ref = *p;
	bundle_->pipelines.push_back(std::move(p));
	return ref;
}

Pipeline &
MetaPipeline::CreateChildPipeline(Pipeline &parent, PhysicalOperator &sink)
{
	Pipeline &child = CreatePipeline();
	child.source = &sink;
	/*
	 * Dependency direction: the parent pipeline (consumer) reads from `sink`
	 * which is produced by the child pipeline. Therefore the parent's RUN
	 * event must wait for the child's FINALIZE. depends_on encodes "this
	 * pipeline waits for X", so we push child.id onto parent.depends_on
	 * (NOT the reverse). Previously this was inverted, causing consumer
	 * RUN tasks to be scheduled before producer sinks initialized payload.
	 */
	parent.depends_on.push_back(child.id);
	return child;
}

void
MetaPipeline::AddOperator(Pipeline &p, PhysicalOperator &op)
{
	p.ops.push_back(&op);
}

void
MetaPipeline::SetSource(Pipeline &p, PhysicalOperator &source)
{
	if (p.source == nullptr)
		p.source = &source;
}

void
MetaPipeline::SetSink(Pipeline &p, PhysicalOperator &sink)
{
	p.sink = &sink;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
