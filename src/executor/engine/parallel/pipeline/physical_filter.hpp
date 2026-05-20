#pragma once

#include "core/data_chunk.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"

namespace pg_yaap {
namespace pipeline {

struct OpDescriptor;

class FilterOperatorState final : public OperatorState {
public:
	bool initialized = false;
	bool current_input_drained = false;
	std::unique_ptr<PipelineChunk> filter_chunk;
	uint8_t bool_values[FILTER_MAX_BOOL_REGS]{};
};

class PhysicalFilter final : public PhysicalOperator {
public:
	PhysicalFilter(dsa_pointer input_schema_dp,
	               PgVector<FilterInputDesc> filter_inputs,
	               PgVector<FilterExprDesc> filter_exprs,
	               PgVector<FilterStep> filter_steps,
	               PgVector<char> filter_string_consts,
	               dsa_pointer filter_inputs_dp = InvalidDsaPointer,
	               dsa_pointer filter_exprs_dp = InvalidDsaPointer,
	               dsa_pointer filter_steps_dp = InvalidDsaPointer,
	               dsa_pointer filter_string_consts_dp = InvalidDsaPointer,
	               OpDescriptor *desc = nullptr)
		: PhysicalOperator(PhysicalOperatorType::FILTER)
		, input_schema_dp_(input_schema_dp)
		, filter_inputs_(std::move(filter_inputs))
		, filter_exprs_(std::move(filter_exprs))
		, filter_steps_(std::move(filter_steps))
		, filter_string_consts_(std::move(filter_string_consts))
		, filter_inputs_dp_(filter_inputs_dp)
		, filter_exprs_dp_(filter_exprs_dp)
		, filter_steps_dp_(filter_steps_dp)
		, filter_string_consts_dp_(filter_string_consts_dp)
		, desc_(desc)
	{}

	bool IsSource() const override { return false; }
	bool IsSink() const override { return false; }
	bool IsPipelineBreaker() const override { return false; }

	std::unique_ptr<OperatorState> GetOperatorState(ExecCtx &ctx) override;
	OperatorResultType Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state) override;

	dsa_pointer input_schema_dp() const { return input_schema_dp_; }
	const PgVector<FilterInputDesc> &filter_inputs() const { return filter_inputs_; }
	const PgVector<FilterExprDesc> &filter_exprs() const { return filter_exprs_; }
	const PgVector<FilterStep> &filter_steps() const { return filter_steps_; }
	const PgVector<char> &filter_string_consts() const { return filter_string_consts_; }
	dsa_pointer filter_inputs_dp() const { return filter_inputs_dp_; }
	dsa_pointer filter_exprs_dp() const { return filter_exprs_dp_; }
	dsa_pointer filter_steps_dp() const { return filter_steps_dp_; }
	dsa_pointer filter_string_consts_dp() const { return filter_string_consts_dp_; }
	OpDescriptor *desc() const { return desc_; }

private:
	dsa_pointer input_schema_dp_;
	PgVector<FilterInputDesc> filter_inputs_;
	PgVector<FilterExprDesc> filter_exprs_;
	PgVector<FilterStep> filter_steps_;
	PgVector<char> filter_string_consts_;
	dsa_pointer filter_inputs_dp_;
	dsa_pointer filter_exprs_dp_;
	dsa_pointer filter_steps_dp_;
	dsa_pointer filter_string_consts_dp_;
	OpDescriptor *desc_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
