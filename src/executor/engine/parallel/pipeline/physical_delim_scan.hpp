#pragma once

extern "C" {
#include "postgres.h"
#include "utils/dsa.h"
}

#include "parallel/pipeline/aggregate_hash_table.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"

namespace pg_yaap {
namespace pipeline {

struct OpDescriptor;

class DelimScanGlobalSourceState final : public GlobalSourceState {
public:
	HashAggSharedPayload *payload = nullptr;
	HashAggPartition     *partitions = nullptr;
	const TupleDataLayout *layout = nullptr;
	uint32_t              partition_count = 0;
};

class DelimScanLocalSourceState final : public LocalSourceState {
public:
	uint32_t            source_partition = UINT32_MAX;
	uint32_t            source_cursor = 0;
	uint32_t            row_count = 0;
	TupleDataCollection *tdc = nullptr;
};

class PhysicalDelimScan final : public PhysicalOperator {
public:
	PhysicalDelimScan(dsa_pointer input_schema_dp,
	                  dsa_pointer shared_payload_dp,
	                  std::unique_ptr<PhysicalOperator> producer_sink = nullptr,
	                  OpDescriptor *desc = nullptr)
		: PhysicalOperator(PhysicalOperatorType::DELIM_SCAN)
		, input_schema_dp_(input_schema_dp)
		, shared_payload_dp_(shared_payload_dp)
		, producer_sink_(std::move(producer_sink))
		, desc_(desc)
	{}

	bool IsSource() const override { return true; }
	bool ParallelSource() const override { return true; }
	int  MaxThreads(ExecCtx &ctx) const override;
	void BuildPipelines(Pipeline &current, MetaPipeline &meta) override;

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSourceState> GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate) override;
	SourceResultType GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) override;

	dsa_pointer input_schema_dp() const { return input_schema_dp_; }
	dsa_pointer shared_payload_dp() const { return shared_payload_dp_; }
	OpDescriptor *desc() const { return desc_; }
	const PgVector<OpDescriptor *> &descs() const { return desc_list_; }
	void AttachDescriptor(OpDescriptor *desc) { desc_ = desc; desc_list_.push_back(desc); }

private:
	dsa_pointer                       input_schema_dp_;
	dsa_pointer                       shared_payload_dp_;
	std::unique_ptr<PhysicalOperator> producer_sink_;
	OpDescriptor                     *desc_ = nullptr;
	PgVector<OpDescriptor *>          desc_list_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
