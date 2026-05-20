#pragma once

#include "core/data_chunk.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"

namespace pg_yaap {
namespace pipeline {

struct OpDescriptor;

class ProjectionOperatorState final : public OperatorState {
public:
	bool initialized = false;
	bool current_input_drained = false;
};

class PhysicalProjection final : public PhysicalOperator {
public:
	PhysicalProjection(dsa_pointer input_schema_dp,
	                   dsa_pointer output_schema_dp,
	                   PgVector<ProjectExprDesc> expr_descs,
	                   PgVector<ProjectStep> steps,
	                   dsa_pointer expr_descs_dp = InvalidDsaPointer,
	                   dsa_pointer steps_dp = InvalidDsaPointer,
	                   OpDescriptor *desc = nullptr)
		: PhysicalOperator(PhysicalOperatorType::PROJECTION)
		, input_schema_dp_(input_schema_dp)
		, output_schema_dp_(output_schema_dp)
		, expr_descs_(std::move(expr_descs))
		, steps_(std::move(steps))
		, expr_descs_dp_(expr_descs_dp)
		, steps_dp_(steps_dp)
		, desc_(desc)
	{}

	bool IsSource() const override { return false; }
	bool IsSink() const override { return false; }
	bool IsPipelineBreaker() const override { return false; }

	std::unique_ptr<OperatorState> GetOperatorState(ExecCtx &ctx) override;
	OperatorResultType Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state) override;

	dsa_pointer input_schema_dp() const { return input_schema_dp_; }
	dsa_pointer output_schema_dp() const { return output_schema_dp_; }
	const PgVector<ProjectExprDesc> &expr_descs() const { return expr_descs_; }
	const PgVector<ProjectStep> &steps() const { return steps_; }
	dsa_pointer expr_descs_dp() const { return expr_descs_dp_; }
	dsa_pointer steps_dp() const { return steps_dp_; }
	OpDescriptor *desc() const { return desc_; }

private:
	dsa_pointer              input_schema_dp_;
	dsa_pointer              output_schema_dp_;
	PgVector<ProjectExprDesc> expr_descs_;
	PgVector<ProjectStep>     steps_;
	dsa_pointer              expr_descs_dp_;
	dsa_pointer              steps_dp_;
	OpDescriptor            *desc_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
