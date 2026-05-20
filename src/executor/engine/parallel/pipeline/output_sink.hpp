#pragma once

extern "C" {
#include "postgres.h"
#include "access/tupdesc.h"
#include "executor/execdesc.h"
#include "executor/tstoreReceiver.h"
#include "utils/dsa.h"
}

#include <vector>

#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_layout.hpp"

namespace pg_yaap {
namespace pipeline {

struct SchemaDescriptor;
struct OpDescriptor;

class OutputGlobalState final : public GlobalSinkState {
public:
	DestReceiver           *dest = nullptr;
	TupleDesc               tupdesc = nullptr;
	TupleTableSlot         *slot = nullptr;

	const SchemaDescriptor *input_schema = nullptr;
	const TupleDataLayout  *layout = nullptr;
	const SortKeyDesc      *sort_keys = nullptr;
	TupleDataCollection    *global_tdc = nullptr;
	dsa_pointer             shared_payload_dp = InvalidDsaPointer;
	bool                    finalized = false;
	uint16_t                n_sort_keys = 0;
	uint64                  max_emit_rows = 0;
};

class OutputLocalState final : public LocalSinkState {
public:
	uint64 emitted_rows = 0;
};

class OutputSink final : public PhysicalOperator {
public:
	/* Leader ctor (translator path). Owns leader-private dest/tupdesc; the
	 * DSA pointers are mirrored into OpDescriptor by EmitOutput so the worker
	 * ctor below can rebuild on remote backends.
	 *
	 * shared_payload_dp MUST be a valid DSA pointer to a TupleDataCollection
	 * already initialized via TupleDataCollectionInit at translation time.
	 * Reason: OutputSink runs as a non-blocking sink in the OUTPUT pipeline;
	 * unlike PhysicalHashAggregate (whose Combine() is leader-only and thus
	 * gives the leader first-touch on alloc), OutputSink::GetGlobalSinkState
	 * is hit concurrently by every worker on its first RUN task. There is
	 * no "leader-first" gate available, so the global TDC must be published
	 * before any worker can attach. See physical_hash_aggregate.cpp:110-125
	 * for the alloc shape mirrored here. */
	OutputSink(DestReceiver *dest,
	           TupleDesc tupdesc,
	           int operation,
	           dsa_pointer input_schema_dp,
	           dsa_pointer layout_dp,
	           dsa_pointer final_sort_keys_dp,
	           uint16_t n_final_sort_keys,
	           dsa_pointer shared_payload_dp,
	           uint32_t tdc_max_rows,
	           std::vector<SortKeyDesc> final_sort_keys = {},
	           uint64 max_emit_rows = 0,
	           OpDescriptor *desc = nullptr)
		: PhysicalOperator(PhysicalOperatorType::OUTPUT)
		, dest_(dest)
		, tupdesc_(tupdesc)
		, operation_(operation)
		, input_schema_dp_(input_schema_dp)
		, layout_dp_(layout_dp)
		, final_sort_keys_dp_(final_sort_keys_dp)
		, n_final_sort_keys_(n_final_sort_keys)
		, shared_payload_dp_(shared_payload_dp)
		, tdc_max_rows_(tdc_max_rows)
		, final_sort_keys_(std::move(final_sort_keys))
		, max_emit_rows_(max_emit_rows)
		, desc_(desc)
	{}

	/* Worker ctor (descriptor reconstruct path). No dest/tupdesc on workers;
	 * results land in the shared TDC, leader drains via EmitGlobalTdcToDest. */
	OutputSink(dsa_pointer input_schema_dp,
	           dsa_pointer layout_dp,
	           dsa_pointer final_sort_keys_dp,
	           uint16_t n_final_sort_keys,
	           dsa_pointer shared_payload_dp,
	           uint32_t tdc_max_rows,
	           uint64 max_emit_rows = 0,
	           OpDescriptor *desc = nullptr)
		: PhysicalOperator(PhysicalOperatorType::OUTPUT)
		, dest_(nullptr)
		, tupdesc_(nullptr)
		, operation_(0)  /* unused on worker (no DestReceiver) */
		, input_schema_dp_(input_schema_dp)
		, layout_dp_(layout_dp)
		, final_sort_keys_dp_(final_sort_keys_dp)
		, n_final_sort_keys_(n_final_sort_keys)
		, shared_payload_dp_(shared_payload_dp)
		, tdc_max_rows_(tdc_max_rows)
		, final_sort_keys_()
		, max_emit_rows_(max_emit_rows)
		, desc_(desc)
	{}

	bool IsSource() const override { return false; }
	bool IsSink() const override { return true; }
	bool IsPipelineBreaker() const override { return false; }
	int  MaxThreads(ExecCtx &ctx) const override { (void) ctx; return 1; }

	OpDescriptor *desc() const { return desc_; }
	const PgVector<OpDescriptor *> &descs() const { return desc_list_; }
	void          AttachDescriptor(OpDescriptor *desc) { desc_ = desc; desc_list_.push_back(desc); }  /* see physical_hash_aggregate.hpp Fix A2 */
	dsa_pointer   input_schema_dp() const { return input_schema_dp_; }
	dsa_pointer   layout_dp() const { return layout_dp_; }
	dsa_pointer   final_sort_keys_dp() const { return final_sort_keys_dp_; }
	uint16_t      n_final_sort_keys() const { return n_final_sort_keys_; }
	dsa_pointer   shared_payload_dp() const { return shared_payload_dp_; }
	uint32_t      tdc_max_rows() const { return tdc_max_rows_; }
	uint64        max_emit_rows() const { return max_emit_rows_; }

	std::unique_ptr<GlobalSinkState> GetGlobalSinkState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSinkState>  GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate) override;
	SinkResultType                   SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) override;
	SinkCombineResultType            Combine(ExecCtx &ctx, OperatorSinkCombineInput &input) override;
	SinkFinalizeType                 Finalize(ExecCtx &ctx, GlobalSinkState &gstate) override;
	bool                             CombineIsTrivial() const override { return true; }
	bool                             FinalizeIsTrivial() const override { return true; }

	/* Leader-only post-FINALIZE drain. Walks shared TDC → encodes columns
	 * via input_schema → forwards to dest_. No-op when dest_ is nullptr. */
	void EmitGlobalTdcToDest(ExecCtx &ctx);

	/* Refresh DestReceiver/TupleDesc/operation captured at translate-time
	 * (ExecutorStart) with the live values from QueryDesc at ExecutorRun.
	 * PortalRunSelect assigns queryDesc->dest just before ExecutorRun, so
	 * the translate-time qd->dest is always DestNone. Caller must invoke
	 * this on the leader before EmitGlobalTdcToDest. */
	void RefreshDestFromQueryDesc(DestReceiver *dest, TupleDesc tupdesc, int operation)
	{
		dest_ = dest;
		tupdesc_ = tupdesc;
		operation_ = operation;
	}

private:
	DestReceiver *dest_;
	TupleDesc     tupdesc_;
	int           operation_;
	dsa_pointer   input_schema_dp_;
	dsa_pointer   layout_dp_;
	dsa_pointer   final_sort_keys_dp_;
	uint16_t      n_final_sort_keys_;
	dsa_pointer   shared_payload_dp_;
	uint32_t      tdc_max_rows_;
	std::vector<SortKeyDesc> final_sort_keys_;
	uint64        max_emit_rows_;
	OpDescriptor *desc_;
	PgVector<OpDescriptor *> desc_list_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
