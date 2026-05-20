#pragma once

extern "C" {
#include "postgres.h"
#include "utils/dsa.h"
}

#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_layout.hpp"

namespace pg_yaap {
namespace pipeline {

struct OpDescriptor;

class HashJoinGlobalSinkState final : public GlobalSinkState {
public:
	dsa_area              *dsa = nullptr;
	OpDescriptor          *desc = nullptr;
	dsa_pointer            shared_payload_dp = InvalidDsaPointer;
	dsa_pointer            probe_key_layout_dp = InvalidDsaPointer;
	dsa_pointer            build_key_layout_dp = InvalidDsaPointer;
	dsa_pointer            output_columns_dp = InvalidDsaPointer;
	dsa_pointer            build_layout_dp = InvalidDsaPointer;
	HashJoinSharedPayload *payload = nullptr;
	const TupleDataLayout *probe_key_layout = nullptr;
	const TupleDataLayout *build_key_layout = nullptr;
	const TupleDataLayout *build_layout = nullptr;
	const HashJoinOutputColumnDesc *output_columns = nullptr;
	uint16_t               output_column_count = 0;
	TupleDataCollection   *build_keys = nullptr;
	TupleDataCollection   *build_rows = nullptr;
	uint32_t               local_state_slot_count = 0;
	bool                   finalized = false;
};

class HashJoinLocalSinkState final : public LocalSinkState {
public:
	dsa_pointer            build_keys_dp = InvalidDsaPointer;
	dsa_pointer            build_rows_dp = InvalidDsaPointer;
	const TupleDataLayout *build_key_layout = nullptr;
	const TupleDataLayout *build_layout = nullptr;
	TupleDataCollection   *build_keys = nullptr;
	TupleDataCollection   *build_rows = nullptr;
	uint64_t build_input_rows = 0;
};

class PhysicalHashJoin final : public PhysicalOperator {
public:
	PhysicalHashJoin(dsa_pointer left_input_schema_dp,
	                 dsa_pointer right_input_schema_dp,
	                 dsa_pointer output_schema_dp,
	                 dsa_pointer left_key_layout_dp,
	                 dsa_pointer right_key_layout_dp,
	                 dsa_pointer left_payload_layout_dp,
	                 dsa_pointer right_payload_layout_dp,
	                 dsa_pointer output_columns_dp,
	                 uint16_t output_column_count,
	                 dsa_pointer filter_inputs_dp,
	                 dsa_pointer filter_exprs_dp,
	                 dsa_pointer filter_steps_dp,
	                 dsa_pointer filter_string_consts_dp,
	                 uint16_t n_filter_inputs,
	                 uint16_t n_filter_exprs,
	                 uint16_t n_filter_steps,
	                 uint16_t filter_bool_regs,
	                 uint32_t filter_string_const_bytes,
	                 dsa_pointer shared_payload_dp,
	                 uint16_t n_left_keys,
	                 uint16_t n_right_keys,
	                 uint32_t max_rows = 1024,
	                 OpDescriptor *desc = nullptr)
		: PhysicalHashJoin(left_input_schema_dp,
			right_input_schema_dp,
			output_schema_dp,
			left_key_layout_dp,
			right_key_layout_dp,
			left_payload_layout_dp,
			right_payload_layout_dp,
			output_columns_dp,
			output_column_count,
			filter_inputs_dp,
			filter_exprs_dp,
			filter_steps_dp,
			filter_string_consts_dp,
			n_filter_inputs,
			n_filter_exprs,
			n_filter_steps,
			filter_bool_regs,
			HashJoinMatchMode::INNER,
			filter_string_const_bytes,
			shared_payload_dp,
			n_left_keys,
			n_right_keys,
			max_rows,
			desc)
	{}

	PhysicalHashJoin(dsa_pointer left_input_schema_dp,
	                 dsa_pointer right_input_schema_dp,
	                 dsa_pointer output_schema_dp,
	                 dsa_pointer left_key_layout_dp,
	                 dsa_pointer right_key_layout_dp,
	                 dsa_pointer left_payload_layout_dp,
	                 dsa_pointer right_payload_layout_dp,
	                 dsa_pointer output_columns_dp,
	                 uint16_t output_column_count,
	                 dsa_pointer filter_inputs_dp,
	                 dsa_pointer filter_exprs_dp,
	                 dsa_pointer filter_steps_dp,
	                 dsa_pointer filter_string_consts_dp,
	                 uint16_t n_filter_inputs,
	                 uint16_t n_filter_exprs,
	                 uint16_t n_filter_steps,
	                 uint16_t filter_bool_regs,
	                 HashJoinMatchMode join_mode,
	                 uint32_t filter_string_const_bytes,
	                 dsa_pointer shared_payload_dp,
	                 uint16_t n_left_keys,
	                 uint16_t n_right_keys,
	                 uint32_t max_rows = 1024,
	                 OpDescriptor *desc = nullptr)
		: PhysicalOperator(PhysicalOperatorType::HASH_JOIN)
		, left_input_schema_dp_(left_input_schema_dp)
		, right_input_schema_dp_(right_input_schema_dp)
		, output_schema_dp_(output_schema_dp)
		, left_key_layout_dp_(left_key_layout_dp)
		, right_key_layout_dp_(right_key_layout_dp)
		, left_payload_layout_dp_(left_payload_layout_dp)
		, right_payload_layout_dp_(right_payload_layout_dp)
		, output_columns_dp_(output_columns_dp)
		, output_column_count_(output_column_count)
		, filter_inputs_dp_(filter_inputs_dp)
		, filter_exprs_dp_(filter_exprs_dp)
		, filter_steps_dp_(filter_steps_dp)
		, filter_string_consts_dp_(filter_string_consts_dp)
		, n_filter_inputs_(n_filter_inputs)
		, n_filter_exprs_(n_filter_exprs)
		, n_filter_steps_(n_filter_steps)
		, filter_bool_regs_(filter_bool_regs)
		, join_mode_(join_mode)
		, filter_string_const_bytes_(filter_string_const_bytes)
		, shared_payload_dp_(shared_payload_dp)
		, n_left_keys_(n_left_keys)
		, n_right_keys_(n_right_keys)
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
	dsa_pointer left_key_layout_dp() const { return left_key_layout_dp_; }
	dsa_pointer right_key_layout_dp() const { return right_key_layout_dp_; }
	dsa_pointer left_payload_layout_dp() const { return left_payload_layout_dp_; }
	dsa_pointer right_payload_layout_dp() const { return right_payload_layout_dp_; }
	dsa_pointer output_columns_dp() const { return output_columns_dp_; }
	uint16_t output_column_count() const { return output_column_count_; }
	dsa_pointer filter_inputs_dp() const { return filter_inputs_dp_; }
	dsa_pointer filter_exprs_dp() const { return filter_exprs_dp_; }
	dsa_pointer filter_steps_dp() const { return filter_steps_dp_; }
	dsa_pointer filter_string_consts_dp() const { return filter_string_consts_dp_; }
	uint16_t n_filter_inputs() const { return n_filter_inputs_; }
	uint16_t n_filter_exprs() const { return n_filter_exprs_; }
	uint16_t n_filter_steps() const { return n_filter_steps_; }
	uint16_t filter_bool_regs() const { return filter_bool_regs_; }
	HashJoinMatchMode join_mode() const { return join_mode_; }
	uint32_t filter_string_const_bytes() const { return filter_string_const_bytes_; }
	dsa_pointer shared_payload_dp() const { return shared_payload_dp_; }
	uint16_t n_left_keys() const { return n_left_keys_; }
	uint16_t n_right_keys() const { return n_right_keys_; }
	uint32_t max_rows() const { return max_rows_; }
	OpDescriptor *desc() const { return desc_; }
	const PgVector<OpDescriptor *> &descs() const { return desc_list_; }
	void AttachDescriptor(OpDescriptor *desc) { desc_ = desc; desc_list_.push_back(desc); }

private:
	dsa_pointer  left_input_schema_dp_;
	dsa_pointer  right_input_schema_dp_;
	dsa_pointer  output_schema_dp_;
	dsa_pointer  left_key_layout_dp_;
	dsa_pointer  right_key_layout_dp_;
	dsa_pointer  left_payload_layout_dp_;
	dsa_pointer  right_payload_layout_dp_;
	dsa_pointer  output_columns_dp_;
	uint16_t     output_column_count_;
	dsa_pointer  filter_inputs_dp_;
	dsa_pointer  filter_exprs_dp_;
	dsa_pointer  filter_steps_dp_;
	dsa_pointer  filter_string_consts_dp_;
	uint16_t     n_filter_inputs_;
	uint16_t     n_filter_exprs_;
	uint16_t     n_filter_steps_;
	uint16_t     filter_bool_regs_;
	HashJoinMatchMode join_mode_;
	uint32_t     filter_string_const_bytes_;
	dsa_pointer  shared_payload_dp_;
	uint16_t     n_left_keys_;
	uint16_t     n_right_keys_;
	uint32_t     max_rows_;
	OpDescriptor *desc_;
	PgVector<OpDescriptor *> desc_list_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
