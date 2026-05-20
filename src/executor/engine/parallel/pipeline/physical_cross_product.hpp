#pragma once

extern "C" {
#include "postgres.h"
#include "utils/dsa.h"
}

#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"

namespace pg_yaap {
namespace pipeline {

struct OpDescriptor;

class CrossProductGlobalSinkState final : public GlobalSinkState {
public:
	dsa_area              *dsa = nullptr;
	OpDescriptor          *desc = nullptr;
	dsa_pointer            right_layout_dp = InvalidDsaPointer;
	dsa_pointer            shared_payload_dp = InvalidDsaPointer;
	const TupleDataLayout *right_layout = nullptr;
	CrossProductSharedPayload *payload = nullptr;
	TupleDataCollection   *build_rows = nullptr;
};

class CrossProductLocalSinkState final : public LocalSinkState {
public:
	dsa_pointer            build_layout_dp = InvalidDsaPointer;
	dsa_pointer            build_rows_dp = InvalidDsaPointer;
	const TupleDataLayout *build_layout = nullptr;
	TupleDataCollection   *build_rows = nullptr;
	uint64_t               build_input_rows = 0;
};

class PhysicalCrossProduct final : public PhysicalOperator {
public:
	PhysicalCrossProduct(dsa_pointer left_input_schema_dp,
	                     dsa_pointer right_input_schema_dp,
	                     dsa_pointer output_schema_dp,
	                     dsa_pointer right_payload_layout_dp,
	                     dsa_pointer output_columns_dp,
	                     uint16_t output_column_count,
	                     dsa_pointer shared_payload_dp,
	                     uint32_t max_rows = 1024,
	                     OpDescriptor *desc = nullptr)
		: PhysicalOperator(PhysicalOperatorType::CROSS_PRODUCT)
		, left_input_schema_dp_(left_input_schema_dp)
		, right_input_schema_dp_(right_input_schema_dp)
		, output_schema_dp_(output_schema_dp)
		, right_payload_layout_dp_(right_payload_layout_dp)
		, output_columns_dp_(output_columns_dp)
		, output_column_count_(output_column_count)
		, shared_payload_dp_(shared_payload_dp)
		, max_rows_(max_rows)
		, desc_(desc)
	{}

	bool IsSource() const override { return false; }
	bool IsSink() const override { return true; }
	bool IsPipelineBreaker() const override { return true; }
	int  MaxThreads(ExecCtx &ctx) const override;

	void BuildPipelines(Pipeline &current, MetaPipeline &meta) override;

	std::unique_ptr<GlobalSinkState> GetGlobalSinkState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSinkState> GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate) override;
	SinkResultType                  SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) override;
	SinkCombineResultType           Combine(ExecCtx &ctx, OperatorSinkCombineInput &input) override;
	SinkFinalizeType                Finalize(ExecCtx &ctx, GlobalSinkState &gstate) override;

	std::unique_ptr<OperatorState> GetOperatorState(ExecCtx &ctx) override;
	OperatorResultType Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state) override;
	void ReleaseBuildPayloadAfterConsumerRun(ExecCtx &ctx);

	dsa_pointer left_input_schema_dp() const { return left_input_schema_dp_; }
	dsa_pointer right_input_schema_dp() const { return right_input_schema_dp_; }
	dsa_pointer output_schema_dp() const { return output_schema_dp_; }
	dsa_pointer right_payload_layout_dp() const { return right_payload_layout_dp_; }
	dsa_pointer output_columns_dp() const { return output_columns_dp_; }
	uint16_t output_column_count() const { return output_column_count_; }
	dsa_pointer shared_payload_dp() const { return shared_payload_dp_; }
	uint32_t max_rows() const { return max_rows_; }
	OpDescriptor *desc() const { return desc_; }
	const PgVector<OpDescriptor *> &descs() const { return desc_list_; }
	void AttachDescriptor(OpDescriptor *desc) { desc_ = desc; desc_list_.push_back(desc); }

private:
	dsa_pointer  left_input_schema_dp_;
	dsa_pointer  right_input_schema_dp_;
	dsa_pointer  output_schema_dp_;
	dsa_pointer  right_payload_layout_dp_;
	dsa_pointer  output_columns_dp_;
	uint16_t     output_column_count_;
	dsa_pointer  shared_payload_dp_;
	uint32_t     max_rows_;
	OpDescriptor *desc_;
	PgVector<OpDescriptor *> desc_list_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
