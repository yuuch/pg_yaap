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
	dsa_pointer         segment_desc_dp = InvalidDsaPointer;
	uint32_t            segment_row_cursor = 0;
	bool                segment_backed = false;
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

namespace pg_yaap::pipeline {
/* Immutable, budgeted query-lifetime spool. Each source has an independent reader. */
struct MaterializeShared {
 pg_atomic_uint64 chains_head;
 pg_atomic_uint32 remaining_consumers;
 uint32_t _pad0;
 dsa_pointer arena_dp;
};
struct MaterializeScanShared { pg_atomic_uint32 next_chain; };
dsa_pointer CreateMaterializeShared(dsa_area *dsa);
dsa_pointer CreateMaterializeScan(dsa_area *dsa);
class PhysicalMaterialize final : public PhysicalOperator {
public:
 PhysicalMaterialize(dsa_pointer layout, dsa_pointer payload,
                     PhysicalMaterialize *producer = nullptr,
                     dsa_pointer scan = InvalidDsaPointer)
 : PhysicalOperator(PhysicalOperatorType::MATERIALIZE), layout_dp(layout), payload_dp(payload), scan_dp(scan), producer(producer) {}
 bool IsSource() const override { return true; }
 bool IsSink() const override { return producer == nullptr; }
 bool ParallelSource() const override { return true; }
 int MaxThreads(ExecCtx &) const override { return 0; }
 bool CombineIsTrivial() const override { return true; }
 void BuildPipelines(Pipeline &, MetaPipeline &) override;
 std::unique_ptr<GlobalSinkState> GetGlobalSinkState(ExecCtx &) override;
 std::unique_ptr<LocalSinkState> GetLocalSinkState(ExecCtx &, GlobalSinkState &) override;
 SinkResultType SinkChunk(ExecCtx &, PipelineChunk &, OperatorSinkInput &) override;
 SinkCombineResultType FinishLocalSink(ExecCtx &, OperatorSinkCombineInput &) override;
 std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &) override;
 std::unique_ptr<LocalSourceState> GetLocalSourceState(ExecCtx &, GlobalSourceState &) override;
 SourceResultType GetData(ExecCtx &, PipelineChunk &, OperatorSourceInput &) override;
 void ReleaseAfterConsumerRun(ExecCtx &ctx);
 dsa_pointer layout_dp, payload_dp, scan_dp;
 PhysicalMaterialize *producer;
 Pipeline *producer_pipeline = nullptr;
};
}
