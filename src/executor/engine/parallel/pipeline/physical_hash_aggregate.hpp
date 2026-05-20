#pragma once

extern "C" {
#include "postgres.h"
#include "utils/dsa.h"
}

#include "parallel/pipeline/aggregate_hash_table.hpp"
#include "parallel/pipeline/operator.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/sink.hpp"
#include "parallel/pipeline/source.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_layout.hpp"

namespace pg_yaap {
namespace pipeline {

struct OpDescriptor;

class HashAggGlobalSinkState final : public GlobalSinkState {
public:
	/*
	 * Single-instance HashAggregate contract: one operator is both sink and
	 * source, with a COMBINE event between Finalize and GetData. The shared
	 * payload is a DSA wrapper with partition-local AHT/TDC pairs.
	 */
	dsa_area            *dsa = nullptr;
	OpDescriptor        *desc = nullptr;
	dsa_pointer          layout_dp = InvalidDsaPointer;
	const TupleDataLayout *layout = nullptr;
	dsa_pointer          shared_payload_dp = InvalidDsaPointer;
	HashAggSharedPayload *payload = nullptr;
	HashAggPartition    *partitions = nullptr;
	uint32_t             partition_count = 0;
	uint32_t             max_groups = 256;
	bool                 finalized = false;
};

class HashAggGlobalSourceState final : public GlobalSourceState {
public:
	const TupleDataLayout *layout = nullptr;
	HashAggSharedPayload *payload = nullptr;
	HashAggPartition     *partitions = nullptr;
	uint32_t              partition_count = 0;
	uint32_t              source_partition = 0;
	uint32_t              source_cursor = 0;
	bool                  finalized = false;
};

class HashAggLocalSinkState final : public LocalSinkState {
public:
	/* Per-worker local sink state is partition-major; the runtime still owns a
	 * single LocalSinkState object, but HashAgg routes each row to one local
	 * radix partition before dedupe/update. */
	dsa_pointer          local_partitions_dp = InvalidDsaPointer;
	HashAggPartition    *local_partitions = nullptr;
	const TupleDataLayout *layout = nullptr;
	dsa_pointer          layout_dp = InvalidDsaPointer;
	uint32_t             partition_count = 0;
	uint32_t             partition_mask = 0;
	uint32_t             partition_shift = 64;
	uint32_t             max_groups = 256;
	bool                 use_perfect_hash = false;
	uint32_t             perfect_capacity = 0;
	PgVector<uint32_t>   perfect_row_indices;
	bool                 has_distinct_count = false;
	uint16_t             distinct_agg_idx = UINT16_MAX;
	dsa_pointer          distinct_layout_dp = InvalidDsaPointer;
	const TupleDataLayout *distinct_layout = nullptr;
	dsa_pointer          distinct_partitions_dp = InvalidDsaPointer;
	HashAggPartition    *distinct_partitions = nullptr;

};

class HashAggLocalSourceState final : public LocalSourceState {
public:
	bool initialized = false;
};

class HashAggOperatorState final : public OperatorState {
public:
	bool initialized = false;
	bool current_input_drained = false;
};

class PhysicalHashAggregate : public PhysicalOperator {
public:
	/*
	 * Step 6 contract delta: HashAggregate no longer splits PARTIAL/FINAL into
	 * two operator instances. One PhysicalHashAggregate owns the sink, COMBINE,
	 * Finalize, and source phases, so descriptor reconstruction only needs the
	 * serialized layout + aggregate metadata + shared payload pointer.
	 */
	PhysicalHashAggregate(dsa_pointer layout_dp,
	                      PgVector<uint16_t> group_keys,
	                      PgVector<AggFuncDesc> agg_funcs,
	                      dsa_pointer shared_payload_dp,
	                      uint32_t max_groups = 256,
	                      uint32_t perfect_hash_capacity = 0,
	                      OpDescriptor *desc = nullptr)
		: PhysicalOperator(PhysicalOperatorType::HASH_AGGREGATE)
		, layout_dp_(layout_dp)
		, group_keys_(std::move(group_keys))
		, agg_funcs_(std::move(agg_funcs))
		, shared_payload_dp_(shared_payload_dp)
		, max_groups_(max_groups)
		, perfect_hash_capacity_(perfect_hash_capacity)
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

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSourceState>  GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate) override;
	SourceResultType                   GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) override;

	std::unique_ptr<OperatorState>     GetOperatorState(ExecCtx &ctx) override;
	OperatorResultType                 Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state) override;

	dsa_pointer                        layout_dp() const { return layout_dp_; }
	const PgVector<uint16_t>          &group_keys() const { return group_keys_; }
	const PgVector<AggFuncDesc>       &agg_funcs() const { return agg_funcs_; }
	dsa_pointer                        shared_payload_dp() const { return shared_payload_dp_; }
	uint32_t                           max_groups() const { return max_groups_; }
	uint32_t                           perfect_hash_capacity() const { return perfect_hash_capacity_; }
	OpDescriptor                      *desc() const { return desc_; }
	const PgVector<OpDescriptor *>    &descs() const { return desc_list_; }

	/*
	 * Fix A2 (cross-pipeline shared instance): a single PhysicalHashAggregate
	 * C++ instance is referenced by BOTH the producer pipeline (as sink) and
	 * the consumer pipeline (as source) — Translator builds one tree, and
	 * MetaPipeline's slicing reuses the same operator pointer in two Pipeline
	 * objects (see pipeline_descriptor.cpp LeaderSerializePipelines source/
	 * mid/sink switch arms calling AttachDescriptor on this same `this`).
	 *
	 * LeaderSerializePipelines allocates an INDEPENDENT DSA OpDescriptor
	 * array per pipeline, so each AttachDescriptor call here corresponds to
	 * a DIFFERENT DSA slot. We therefore record EVERY attached slot in
	 * desc_list_ (push_back, never overwrite), and StoreSharedPayloadOnDescriptor
	 * iterates desc_list_ to publish the shared payload pointer into ALL
	 * slots — so workers reading from EITHER pipeline's slot see the same
	 * leader-allocated AHT/TDC. desc_ retains the last-attached slot for
	 * single-attach Resolve helpers (worker side attaches once → list size 1).
	 *
	 * Without per-slot fan-out, the leader writes to the LAST attach (P2.sink
	 * slot) but workers running P1.source read from the P1 slot → mismatch →
	 * ERROR "hash aggregate source payload not initialized".
	 */
	void AttachDescriptor(OpDescriptor *desc)
	{
		desc_ = desc;
		desc_list_.push_back(desc);
	}

protected:
	dsa_pointer LayoutDpFromDescriptor() const;
	dsa_pointer SharedPayloadDpFromDescriptor() const;
	uint32_t MaxGroupsFromDescriptor() const;
	uint32_t PerfectHashCapacityFromDescriptor() const;

private:
	dsa_pointer              layout_dp_;
	PgVector<uint16_t>       group_keys_;
	PgVector<AggFuncDesc>    agg_funcs_;
	dsa_pointer              shared_payload_dp_;
	uint32_t                 max_groups_;
	uint32_t                 perfect_hash_capacity_;
	OpDescriptor            *desc_;
	PgVector<OpDescriptor *> desc_list_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
