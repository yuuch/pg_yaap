#pragma once

#include "parallel/pipeline/physical_hash_aggregate.hpp"

namespace pg_yaap {
namespace pipeline {

class PerfectHashAggGlobalSinkState final : public GlobalSinkState {
public:
	dsa_area                *dsa = nullptr;
	OpDescriptor            *desc = nullptr;
	dsa_pointer              layout_dp = InvalidDsaPointer;
	const TupleDataLayout   *layout = nullptr;
	dsa_pointer              shared_payload_dp = InvalidDsaPointer;
	PerfectHashAggSharedPayload *payload = nullptr;
	TupleDataCollection     *global_tdc = nullptr;
	uint32_t                *global_index = nullptr;
	uint32_t                 max_groups = 256;
	uint32_t                 local_state_slot_count = 0;
	bool                     finalized = false;
};

class PerfectHashAggGlobalSourceState final : public GlobalSourceState {
public:
	const TupleDataLayout       *layout = nullptr;
	PerfectHashAggSharedPayload *payload = nullptr;
	TupleDataCollection         *global_tdc = nullptr;
	uint32_t                     source_cursor = 0;
	bool                         finalized = false;
};

class PerfectHashAggLocalSinkState final : public LocalSinkState {
public:
	dsa_pointer              local_tdc_dp = InvalidDsaPointer;
	TupleDataCollection     *local_tdc = nullptr;
	const TupleDataLayout   *layout = nullptr;
	dsa_pointer              layout_dp = InvalidDsaPointer;
	uint32_t                 max_groups = 256;
	uint32_t                 perfect_capacity = 0;
	PgVector<uint32_t>       perfect_row_indices;
};

class PerfectHashAggLocalSourceState final : public LocalSourceState {
public:
	bool initialized = false;
};

class PerfectHashAggOperatorState final : public OperatorState {
public:
	bool initialized = false;
	bool current_input_drained = false;
};

class PhysicalPerfectHashAggregate final : public PhysicalHashAggregate {
public:
	PhysicalPerfectHashAggregate(dsa_pointer layout_dp,
	                            PgVector<uint16_t> group_keys,
	                            PgVector<AggFuncDesc> agg_funcs,
	                            dsa_pointer shared_payload_dp,
	                            uint32_t max_groups = 256,
	                            uint32_t perfect_hash_capacity = 0,
	                            OpDescriptor *desc = nullptr)
		: PhysicalHashAggregate(layout_dp,
			std::move(group_keys),
			std::move(agg_funcs),
			shared_payload_dp,
			max_groups,
			perfect_hash_capacity,
			desc)
	{
		type_ = PhysicalOperatorType::PERFECT_HASH_AGGREGATE;
	}

	std::unique_ptr<GlobalSinkState>   GetGlobalSinkState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSinkState>    GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate) override;
	SinkResultType                     SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) override;
	SinkCombineResultType              Combine(ExecCtx &ctx, OperatorSinkCombineInput &input) override;
	SinkFinalizeType                   Finalize(ExecCtx &ctx, GlobalSinkState &gstate) override;

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSourceState>  GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate) override;
	SourceResultType                   GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) override;

	std::unique_ptr<OperatorState>     GetOperatorState(ExecCtx &ctx) override;
	OperatorResultType                 Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state) override;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
