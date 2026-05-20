#pragma once

extern "C" {
#include "postgres.h"
#include "storage/spin.h"
#include "utils/dsa.h"
}

#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/physical_order.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_layout.hpp"

namespace pg_yaap {
namespace pipeline {

struct OpDescriptor;

struct TopNSharedPayload
{
	slock_t     mutex;
	dsa_pointer tdc_dp;
	dsa_pointer sort_indices_dp;
	bool        finalized;
	uint8_t     _pad[7];
};

class TopNGlobalState final : public GlobalSinkState, public GlobalSourceState {
public:
	dsa_area                *dsa = nullptr;
	OpDescriptor            *desc = nullptr;
	dsa_pointer              input_schema_dp = InvalidDsaPointer;
	dsa_pointer              layout_dp = InvalidDsaPointer;
	dsa_pointer              sort_keys_dp = InvalidDsaPointer;
	dsa_pointer              shared_payload_dp = InvalidDsaPointer;
	const SchemaDescriptor  *input_schema = nullptr;
	const TupleDataLayout   *layout = nullptr;
	const SortKeyDesc       *sort_keys = nullptr;
	TopNSharedPayload       *payload = nullptr;
	TupleDataCollection     *global_tdc = nullptr;
	dsa_pointer              sort_indices_dp = InvalidDsaPointer;
	uint16_t                 n_sort_keys = 0;
	uint32_t                 max_rows = 0;
};

class TopNLocalSinkState final : public LocalSinkState {
};

class TopNLocalSourceState final : public LocalSourceState {
public:
	uint32_t source_cursor = 0;
};

class PhysicalTopN final : public PhysicalOperator {
public:
	PhysicalTopN(dsa_pointer input_schema_dp,
	             dsa_pointer layout_dp,
	             dsa_pointer sort_keys_dp,
	             uint16_t n_sort_keys,
	             dsa_pointer shared_payload_dp,
	             uint32_t max_rows,
	             OpDescriptor *desc = nullptr)
		: PhysicalOperator(PhysicalOperatorType::TOP_N)
		, input_schema_dp_(input_schema_dp)
		, layout_dp_(layout_dp)
		, sort_keys_dp_(sort_keys_dp)
		, n_sort_keys_(n_sort_keys)
		, shared_payload_dp_(shared_payload_dp)
		, max_rows_(max_rows)
		, desc_(desc)
	{}

	bool IsSource() const override { return true; }
	bool IsSink() const override { return true; }
	bool IsPipelineBreaker() const override { return true; }
	int  MaxThreads(ExecCtx &ctx) const override;

	std::unique_ptr<GlobalSinkState>   GetGlobalSinkState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSinkState>    GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate) override;
	SinkResultType                     SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) override;
	SinkCombineResultType              Combine(ExecCtx &ctx, OperatorSinkCombineInput &input) override;
	SinkFinalizeType                   Finalize(ExecCtx &ctx, GlobalSinkState &gstate) override;
	bool                               CombineIsTrivial() const override { return true; }

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSourceState>  GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate) override;
	SourceResultType                   GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) override;

	dsa_pointer                        input_schema_dp() const { return input_schema_dp_; }
	dsa_pointer                        layout_dp() const { return layout_dp_; }
	dsa_pointer                        sort_keys_dp() const { return sort_keys_dp_; }
	uint16_t                           n_sort_keys() const { return n_sort_keys_; }
	dsa_pointer                        shared_payload_dp() const { return shared_payload_dp_; }
	uint32_t                           max_rows() const { return max_rows_; }
	OpDescriptor                      *desc() const { return desc_; }
	const PgVector<OpDescriptor *>    &descs() const { return desc_list_; }
	void AttachDescriptor(OpDescriptor *desc) { desc_ = desc; desc_list_.push_back(desc); }

private:
	dsa_pointer   input_schema_dp_;
	dsa_pointer   layout_dp_;
	dsa_pointer   sort_keys_dp_;
	uint16_t      n_sort_keys_;
	dsa_pointer   shared_payload_dp_;
	uint32_t      max_rows_;
	OpDescriptor *desc_;
	PgVector<OpDescriptor *> desc_list_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
