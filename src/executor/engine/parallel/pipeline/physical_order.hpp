#pragma once

extern "C" {
#include "postgres.h"
#include "utils/dsa.h"
}

#include "parallel/pipeline/operator.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/sink.hpp"
#include "parallel/pipeline/source.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_layout.hpp"
#include "buffer_manager/yaap_segmented_tdc.hpp"

namespace pg_yaap {
namespace pipeline {

struct OpDescriptor;

/* Backend-local PG external sort. Only TDC rows and descriptors cross backends. */
class TdcExternalSorter {
public:
 TdcExternalSorter(ExecCtx &ctx, const TupleDataLayout *payload_layout,
                   const TupleDataLayout *key_layout, const SortKeyDesc *keys,
                   uint16_t nkeys, bool sequential_keys);
 ~TdcExternalSorter();
 void Put(const TupleDataCollection *tdc, const uint8_t *row);
 void Perform();
 const TupleDataCollection *Next();
private:
 struct State;
 State *state_;
};

void SpillCompletedTdcSegments(ExecCtx &ctx, dsa_pointer arena_dp,
                              dsa_pointer first_dp, const char *prefix);


struct OrderSharedPayload
{
	dsa_pointer tdc_dp = InvalidDsaPointer;
	dsa_pointer segment_arena_dp = InvalidDsaPointer;
	dsa_pointer segment_first_desc_dp = InvalidDsaPointer;
	dsa_pointer sorted_refs_dp = InvalidDsaPointer;
	uint32_t segment_id = UINT32_MAX;
	uint32_t segment_generation = 0;
	uint32_t segment_row_count = 0;
	bool segment_backed = false;
	bool finalized = false;
	uint8_t _pad[2] = {0, 0};
};

class OrderGlobalState final : public GlobalSinkState, public GlobalSourceState {
public:
	dsa_area             *dsa = nullptr;
	OpDescriptor         *desc = nullptr;
	dsa_pointer           sort_keys_dp = InvalidDsaPointer;
	dsa_pointer           key_layout_dp = InvalidDsaPointer;
	dsa_pointer           payload_layout_dp = InvalidDsaPointer;
	dsa_pointer           shared_payload_dp = InvalidDsaPointer;
	OrderSharedPayload   *shared_payload = nullptr;
	dsa_pointer           sort_indices_dp = InvalidDsaPointer;
	const SortKeyDesc    *sort_keys = nullptr;
	uint16_t              n_sort_keys = 0;
	const TupleDataLayout *key_layout = nullptr;
	const TupleDataLayout *payload_layout = nullptr;
	TupleDataCollection  *payload = nullptr;
	uint32_t              max_rows = 256;
	bool                  segment_retired = false;
	buffer::YaapSegmentedTdc segmented_writer;
	bool                  segmented_writer_initialized = false;
};

class OrderLocalState final : public LocalSinkState, public LocalSourceState {
public:
	uint32_t source_cursor = 0;
 std::unique_ptr<TdcExternalSorter> sorter;
};

/*
 * Sort indices array stored at the head of the OrderGlobalState's auxiliary
 * DSA allocation. Built in Finalize, consumed in GetData.
 */
struct OrderSortIndices {
	uint32_t count;
	uint32_t _pad;
	uint32_t indices[];
};

struct OrderSortedRowRef {
	dsa_pointer segment_desc_dp;
	uint32_t row_index;
};

class PhysicalOrder final : public PhysicalOperator {
public:
	/*
	 * Step 6 contract delta: Order now receives serialized key/payload layouts
	 * instead of relying on Q1-specific row PODs. The sink/source bodies remain
	 * placeholder stubs for the later real sort implementation.
	 */
	explicit PhysicalOrder(dsa_pointer sort_keys_dp = InvalidDsaPointer,
	                       uint16_t n_sort_keys = 0,
	                       dsa_pointer key_layout_dp = InvalidDsaPointer,
	                       dsa_pointer payload_layout_dp = InvalidDsaPointer,
	                       dsa_pointer shared_payload_dp = InvalidDsaPointer,
	                       uint32_t max_rows = 256,
	                       OpDescriptor *desc = nullptr)
		: PhysicalOperator(PhysicalOperatorType::ORDER)
		, sort_keys_dp_(sort_keys_dp)
		, n_sort_keys_(n_sort_keys)
		, key_layout_dp_(key_layout_dp)
		, payload_layout_dp_(payload_layout_dp)
		, shared_payload_dp_(shared_payload_dp)
		, max_rows_(max_rows)
		, desc_(desc)
	{}

	bool IsSource() const override { return true; }
	bool IsSink() const override { return true; }
	bool IsPipelineBreaker() const override { return true; }
	int  MaxThreads(ExecCtx &ctx) const override { (void) ctx; return 1; }

	std::unique_ptr<GlobalSinkState>   GetGlobalSinkState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSinkState>    GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate) override;
	SinkResultType                     SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) override;
	SinkCombineResultType              Combine(ExecCtx &ctx, OperatorSinkCombineInput &input) override;
	SinkFinalizeType                   Finalize(ExecCtx &ctx, GlobalSinkState &gstate) override;

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSourceState>  GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate) override;
	SourceResultType                   GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) override;

	dsa_pointer                        sort_keys_dp() const { return sort_keys_dp_; }
	uint16_t                           n_sort_keys() const { return n_sort_keys_; }
	dsa_pointer                        key_layout_dp() const { return key_layout_dp_; }
	dsa_pointer                        payload_layout_dp() const { return payload_layout_dp_; }
	dsa_pointer                        shared_payload_dp() const { return shared_payload_dp_; }
	uint32_t                           max_rows() const { return max_rows_; }
	OpDescriptor                      *desc() const { return desc_; }
	const PgVector<OpDescriptor *>    &descs() const { return desc_list_; }
	void                               AttachDescriptor(OpDescriptor *desc) { desc_ = desc; desc_list_.push_back(desc); }  /* see physical_hash_aggregate.hpp Fix A2 */

private:
	dsa_pointer   sort_keys_dp_;
	uint16_t      n_sort_keys_;
	dsa_pointer   key_layout_dp_;
	dsa_pointer   payload_layout_dp_;
	dsa_pointer   shared_payload_dp_;
	uint32_t      max_rows_;
	OpDescriptor *desc_;
	PgVector<OpDescriptor *> desc_list_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
