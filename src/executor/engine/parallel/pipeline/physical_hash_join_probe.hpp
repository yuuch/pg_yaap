#pragma once

#include "parallel/pipeline/physical_hash_join.hpp"

namespace pg_yaap {
namespace pipeline {

/* Tail quotas bound writable partition buffers independently of unrelated
 * operators. Global pressure only forces a partial flush near the hard limit. */
constexpr bool HashJoinProbeShouldSpillTail(uint32_t active, uint32_t limit,
                                           uint64_t resident, uint64_t hard_limit,
                                           uint64_t allocation_headroom) {
	return active > 0 && (active >= limit || allocation_headroom >= hard_limit ||
		resident > hard_limit - allocation_headroom);
}

/* Hybrid radix probe: resident partitions match while input streams through;
 * FinalExecute drains only deferred partition pairs after the shared EOF barrier. */
class PhysicalHashJoinProbe final : public PhysicalOperator {
public:
	explicit PhysicalHashJoinProbe(PhysicalHashJoin &join)
		: PhysicalOperator(PhysicalOperatorType::HASH_JOIN_PROBE), join_(&join) {}
	PhysicalHashJoinProbe(std::unique_ptr<PhysicalHashJoin> join, OpDescriptor *desc)
		: PhysicalOperator(PhysicalOperatorType::HASH_JOIN_PROBE), owned_join_(std::move(join)),
		  join_(owned_join_.get()), desc_(desc) {}

	bool IsSource() const override { return true; }
	bool IsSink() const override { return true; }
	bool ParallelSource() const override { return true; }
	int MaxThreads(ExecCtx &ctx) const override;
	void InitializeHybrid(ExecCtx &ctx, uint32_t participants);
	std::unique_ptr<OperatorState> GetOperatorState(ExecCtx &ctx) override;
	OperatorResultType Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out,
	                           OperatorState &state) override;
	OperatorResultType FinalExecute(ExecCtx &ctx, PipelineChunk &out, OperatorState &state);
	std::unique_ptr<GlobalSinkState> GetGlobalSinkState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSinkState> GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &global) override;
	SinkResultType SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) override;
	SinkCombineResultType Combine(ExecCtx &ctx, OperatorSinkCombineInput &input) override;
	SinkFinalizeType Finalize(ExecCtx &ctx, GlobalSinkState &state) override;
	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSourceState> GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &state) override;
	SourceResultType GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) override;
	void ReleaseAfterConsumerRun(ExecCtx &ctx);

	PhysicalHashJoin &join() const { return *join_; }
	OpDescriptor *desc() const { return desc_; }
	const PgVector<OpDescriptor *> &descs() const { return descs_; }
	void AttachDescriptor(OpDescriptor *desc) {
		desc_ = desc;
		descs_.push_back(desc);
		join_->AttachDescriptor(desc);
	}
private:
	std::unique_ptr<PhysicalHashJoin> owned_join_;
	PhysicalHashJoin *join_;
	OpDescriptor *desc_ = nullptr;
	PgVector<OpDescriptor *> descs_;
};

} // namespace pipeline
} // namespace pg_yaap
