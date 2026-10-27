#include "core/dsa_allocation.hpp"
#include "parallel/pipeline/physical_hash_join_probe.hpp"

extern "C" {
#include "miscadmin.h"
#include "storage/ipc.h"
}

#include <algorithm>
#include <cstring>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/cancel.hpp"
#include "parallel/pipeline/pipeline_profile.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"

namespace pg_yaap {
namespace pipeline {
namespace {

struct ProbeChain { dsa_pointer first; uint64_t rows; };
struct ProbePair { uint32_t state; uint32_t next_slot; uint32_t active_chains; uint64_t rows; };
struct ProbePayload {
	slock_t mutex;
	dsa_pointer arena_dp;
	dsa_pointer chains_dp;
	dsa_pointer pairs_dp;
	dsa_pointer layout_dp;
	uint32_t slots;
	uint32_t partitions;
	uint32_t null_offset;
	uint64_t budget;
	uint32_t active_pairs;
	uint32_t participants;
	uint64_t initial_mask;
	uint64_t completed_mask;
	dsa_pointer initial_batch_dp;
	dsa_pointer current_batch_dp;
	uint64_t initial_bytes;
	pg_atomic_uint32 prepare_next;
	pg_atomic_uint32 prepared_workers;
	pg_atomic_uint32 scan_done;
	pg_atomic_uint32 initial_unmatched_done;
	pg_atomic_uint32 released_workers;
	pg_atomic_uint32 drain_phase;
	pg_atomic_uint32 abandoned;
	bool hybrid;
	bool finalized;
	bool consumed;
};

struct ProbeSinkGlobal final : GlobalSinkState {
	ProbePayload *payload = nullptr;
	HashJoinSharedPayload *build = nullptr;
	const TupleDataLayout *layout = nullptr;
	const TupleDataLayout *keys = nullptr;
};
struct ProbeSinkLocal final : LocalSinkState {
	PgVector<buffer::YaapSegmentedTdc> writers;
	PgVector<uint64_t> last_used;
	uint64_t clock = 0;
	uint32_t active_tails = 0;
	uint32_t slot = 0;
	bool selected = false;
	uint16_t selection_count = 0;
	uint16_t selection[PIPELINE_DEFAULT_CHUNK_SIZE];
};
struct ProbeSourceGlobal final : GlobalSourceState {
	ProbePayload *payload = nullptr;
	HashJoinSharedPayload *build = nullptr;
	const TupleDataLayout *layout = nullptr;
};
struct ProbeSourceLocal final : LocalSourceState {
	dsa_area *dsa = nullptr;
	dsa_pointer arena = InvalidDsaPointer;
	dsa_pointer probe_arena = InvalidDsaPointer;
	uint32_t partition = UINT32_MAX;
	uint32_t slot = 0;
	uint32_t slot_end = 0;
	dsa_pointer batch_dp = InvalidDsaPointer;
	uint32_t unmatched_cursor = 0;
	uint32_t unmatched_end = 0;
	bool probe_barrier_published = false;
	bool unmatched_scan_initialized = false;
	bool unmatched_barrier_published = false;
	static constexpr uint32_t kPrefetchSegments = 4;
	static constexpr uint64_t kPrefetchBytes = 4u * 1024u * 1024u;
	dsa_pointer prefetch_cursor = InvalidDsaPointer;
	uint32_t prefetch_head = 0;
	uint32_t prefetch_count = 0;
	uint64_t prefetch_bytes = 0;
	uint64_t prefetch_charges[kPrefetchSegments]{};
	uint32_t row = 0;
	dsa_pointer segment = InvalidDsaPointer;
	buffer::YaapSharedSegmentReadLease probe_lease;
	std::unique_ptr<PipelineChunk> chunk;
	std::unique_ptr<OperatorState> join_state;
	std::unique_ptr<OperatorState> unmatched_join_state;
	buffer::YaapSpillStore spill;
	bool have_input = false;

	explicit ProbeSourceLocal(ExecCtx &ctx, dsa_pointer arena_dp, dsa_pointer probe_arena_dp)
		: dsa(ctx.dsa), arena(arena_dp), probe_arena(probe_arena_dp),
		  chunk(std::make_unique<PipelineChunk>()), spill(ctx.mcxt) {
		spill.AttachSharedFileSet(&ctx.control->spill_fileset, nullptr);
	}
	void ReleaseReads() {
		buffer::YaapSharedSegmentReleaseRead(dsa, probe_arena, &probe_lease);
	}
	~ProbeSourceLocal() override { ReleaseReads(); }
};

buffer::YaapBufferSharedState *Budget(ExecCtx &ctx) {
	return static_cast<buffer::YaapBufferSharedState *>(
		dsa_get_address(ctx.dsa, ctx.control->buffer_state_root));
}
HashJoinSharedPayload *BuildPayload(ExecCtx &ctx, PhysicalHashJoin &join) {
	dsa_pointer dp = join.shared_payload_dp();
	if (!DsaPointerIsValid(dp) && join.desc() != nullptr)
		dp = join.desc()->body.hash_join.shared_payload;
	return DsaPointerIsValid(dp) ? static_cast<HashJoinSharedPayload *>(dsa_get_address(ctx.dsa, dp)) : nullptr;
}
ProbePayload *Payload(ExecCtx &ctx, const PhysicalHashJoinProbe &op) {
	dsa_pointer dp = LoadSharedPayloadFromDescriptor(&op);
	return DsaPointerIsValid(dp) ? static_cast<ProbePayload *>(dsa_get_address(ctx.dsa, dp)) : nullptr;
}
ProbeChain *Chains(ExecCtx &ctx, ProbePayload *p) {
	return static_cast<ProbeChain *>(dsa_get_address(ctx.dsa, p->chains_dp));
}
ProbePair *Pairs(ExecCtx &ctx, ProbePayload *p) {
	return static_cast<ProbePair *>(dsa_get_address(ctx.dsa, p->pairs_dp));
}
uint32_t Partition(uint64_t hash, const HashJoinSharedPayload *p) {
	return p->build_partition_count == 1 ? 0 :
		static_cast<uint32_t>(hash >> p->partition_shift) & p->partition_mask;
}

void ErrorRelease(int, Datum arg) {
	auto *local = reinterpret_cast<ProbeSourceLocal *>(DatumGetPointer(arg));
	local->ReleaseReads();
	local->spill.Close();
}

void Acquire(ExecCtx &ctx, PhysicalHashJoin &join, dsa_pointer arena,
             buffer::YaapSharedSegmentDesc *segment, buffer::YaapSpillStore &spill,
             buffer::YaapSharedSegmentReadLease *lease) {
	const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id, segment->generation};
	for (uint32_t attempt = 0; attempt < 100000; ++attempt) {
		CHECK_FOR_INTERRUPTS();
		if (PipelineCancelRequested(ctx))
			ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("pg_yaap: paired probe canceled")));
		if (buffer::YaapSharedSegmentAcquireRead(ctx.dsa, arena, handle, lease))
			return;
		if (pg_atomic_read_u32(&segment->state) == static_cast<uint32_t>(buffer::YaapSharedSegmentState::SPILLED)) {
			bool loaded = false;
			{
				PipelineProfileScope scope(ctx, PipelineProfileStage::HASH_JOIN_PARTITION_RELOAD);
				loaded = buffer::YaapSharedSegmentReload(ctx.dsa, Budget(ctx), handle, arena,
					&spill, segment->spill_extent, lease);
				scope.AddRows(loaded ? 1 : 0);
			}
			if (loaded) return;
			bool evicted = false;
			{
				PipelineProfileScope scope(ctx, PipelineProfileStage::HASH_JOIN_PARTITION_EVICT);
				evicted = join.EvictUnusedBuildSegment(ctx, handle);
				scope.AddRows(evicted ? 1 : 0);
			}
			if (evicted) continue;
		}
		pg_usleep(100L);
	}
	elog(ERROR, "pg_yaap: paired hash join could not acquire segment %u", segment->segment_id);
}

void Retire(ExecCtx &ctx, dsa_pointer arena, const buffer::YaapSegmentHandle &handle) {
	for (uint32_t attempt = 0; attempt < 100000; ++attempt) {
		CHECK_FOR_INTERRUPTS();
		if (buffer::YaapSharedSegmentRetire(ctx.dsa, Budget(ctx), arena, handle, true)) return;
		if (PipelineCancelRequested(ctx))
			ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("pg_yaap: probe retirement canceled")));
		pg_usleep(100L);
	}
	elog(ERROR, "pg_yaap: probe partition retirement failed");
}

void Spill(ExecCtx &ctx, dsa_pointer arena, buffer::YaapSharedSegmentDesc *segment,
           buffer::YaapSpillStore &spill) {
	const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id, segment->generation};
	buffer::YaapSpillExtent extent{};
	if (pg_atomic_read_u32(&segment->state) != static_cast<uint32_t>(buffer::YaapSharedSegmentState::PUBLISHED))
		return;
	PipelineProfileScope scope(ctx, PipelineProfileStage::HASH_JOIN_PROBE_SPILL);
	if (!buffer::YaapSharedSegmentSpill(ctx.dsa, Budget(ctx), handle, arena, &spill, &extent))
		elog(ERROR, "pg_yaap: probe partition spill failed");
	scope.AddRows(1);
}

/* All local chains of one radix partition are stable after Build COMBINE. */
template<class Callback>
void BuildSegments(ExecCtx &ctx, PhysicalHashJoin &join, HashJoinSharedPayload *build,
                   uint32_t partition, bool needed_only, Callback callback) {
	if (!build->direct_segmented_build || build->build_partition_count <= 1)
		return;
	auto *registry = static_cast<HashJoinLocalBuildRegistryEntry *>(
		dsa_get_address(ctx.dsa, build->local_build_registry_dp));
	auto *parts = static_cast<HashJoinPartition *>(dsa_get_address(ctx.dsa, build->partitions_dp));
	bool keys_needed = !DsaPointerIsValid(parts[partition].hash_key_values_dp);
	bool rows_needed = join.n_filter_inputs() > 0;
	auto *outputs = static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, join.output_columns_dp()));
	for (uint16_t i = 0; i < join.output_column_count(); ++i)
		rows_needed |= outputs[i].side == HashJoinOutputSide::RIGHT && outputs[i].decode_kind != ColumnDecodeKind::NONE;
	for (uint32_t slot = 0; slot < build->local_state_slot_count; ++slot) {
		if (!DsaPointerIsValid(registry[slot].partition_state_dp)) continue;
		auto *locals = static_cast<HashJoinLocalPartitionState *>(dsa_get_address(ctx.dsa, registry[slot].partition_state_dp));
		const auto &part = locals[partition];
		dsa_pointer descs[2] = {part.build_key_segment_desc_dp, part.build_row_segment_desc_dp};
		for (uint32_t kind = 0; kind < 2; ++kind) {
			if (!DsaPointerIsValid(descs[kind])) continue;
			if (kind == 1 && descs[1] == descs[0]) continue;
			if (needed_only && kind == 0 && !keys_needed && !(rows_needed && descs[1] == descs[0])) continue;
			if (needed_only && kind == 1 && !rows_needed) continue;
			const auto *desc = buffer::YaapSegmentedTdcResolveDesc(ctx.dsa, descs[kind]);
			for (dsa_pointer dp = desc->first_desc_dp; DsaPointerIsValid(dp);) {
				auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, dp));
				const dsa_pointer next = segment->chain_next_dp;
				callback(segment);
				dp = next;
			}
		}
	}
}

void CompletePair(ExecCtx &ctx, PhysicalHashJoinProbe &op, ProbeSourceGlobal &global, ProbeSourceLocal &local) {
	local.ReleaseReads();
	/* The batch owns its table and Build leases until every worker finishes. */
	auto *pairs = Pairs(ctx, global.payload);
	SpinLockAcquire(&global.payload->mutex);
	--global.payload->active_pairs;
	auto &pair = pairs[local.partition];
	Assert(pair.active_chains > 0);
	--pair.active_chains;
	if (pair.next_slot == global.payload->slots && pair.active_chains == 0) pair.state = 2;
	SpinLockRelease(&global.payload->mutex);
	local.partition = UINT32_MAX;
	local.join_state.reset();
	local.have_input = false;
}

uint64_t AdmissionBudget(ExecCtx &ctx, ProbePayload *p) {
	auto *budget = Budget(ctx);
	uint64_t evictable = 0;
	auto *arena = buffer::YaapSharedSegmentArenaResolve(ctx.dsa, ctx.control->large_shared_segment_arena_root);
	for (dsa_pointer dp = arena->first_segment_dp; DsaPointerIsValid(dp);) {
		auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, dp));
		if (pg_atomic_read_u32(&segment->state) == static_cast<uint32_t>(buffer::YaapSharedSegmentState::PUBLISHED) &&
			pg_atomic_read_u32(&segment->readers) == 0 && DsaPointerIsValid(segment->payload_dp))
			evictable += segment->capacity;
		dp = segment->next_dp;
	}
	const uint64_t resident = buffer::YaapBufferResidentBytes(budget);
	const uint64_t fixed = resident > evictable ? resident - evictable : 0;
	const uint64_t reserve = budget->hard_limit / 4u;
	const uint64_t available = budget->hard_limit > fixed ? budget->hard_limit - fixed : 0;
	return std::min(p->budget, available > reserve ? available - reserve : 0);
}

struct BatchPlan {
	uint64_t rows = 0;
	uint64_t segment_bytes = 0;
	uint64_t directory_bytes = 0;
	uint32_t segments = 0;
	uint32_t map_count = 0;
	uint32_t capacity = 0;
	uint32_t matched_word_count = 0;
	uint32_t offsets[64]{};
	uint32_t bucket_offsets[64]{};
	uint32_t bucket_masks[64]{};
};

uint64_t BuildPartitionRows(ExecCtx &ctx, HashJoinSharedPayload *build, uint32_t part) {
	auto *registry = static_cast<HashJoinLocalBuildRegistryEntry *>(dsa_get_address(ctx.dsa, build->local_build_registry_dp));
	uint64_t rows = 0;
	for (uint32_t slot = 0; slot < build->local_state_slot_count; ++slot) {
		if (!DsaPointerIsValid(registry[slot].partition_state_dp)) continue;
		auto *parts = static_cast<HashJoinLocalPartitionState *>(dsa_get_address(ctx.dsa, registry[slot].partition_state_dp));
		rows += parts[part].row_count;
	}
	return rows;
}

BatchPlan DescribeBatch(ExecCtx &ctx, PhysicalHashJoin &join, HashJoinSharedPayload *build, uint64_t mask) {
	BatchPlan plan;
	for (uint32_t part = 0; part < build->build_partition_count; ++part) {
		plan.offsets[part] = static_cast<uint32_t>(plan.rows);
		if (((mask >> part) & 1u) == 0) continue;
		const uint64_t rows = BuildPartitionRows(ctx, build, part);
		plan.rows += rows;
		if (plan.rows > UINT32_MAX / 2u) {
			plan.directory_bytes = UINT64_MAX;
			return plan;
		}
		uint64_t buckets = 16; /* Ranges are cache-line sized, including empty partitions. */
		while (buckets < rows * 2u) buckets <<= 1;
		if (buckets > UINT32_MAX - plan.capacity) {
			plan.directory_bytes = UINT64_MAX;
			return plan;
		}
		plan.bucket_offsets[part] = plan.capacity;
		plan.bucket_masks[part] = static_cast<uint32_t>(buckets - 1u);
		plan.capacity += static_cast<uint32_t>(buckets);
		BuildSegments(ctx, join, build, part, false, [&](buffer::YaapSharedSegmentDesc *segment) {
			++plan.segments;
			plan.segment_bytes += segment->capacity;
			plan.map_count = std::max(plan.map_count, segment->segment_id + 1);
		});
	}
	plan.capacity = std::max(1u, plan.capacity);
	const uint64_t entries = std::max<uint64_t>(1, plan.rows);
	if (join.join_mode() == HashJoinMatchMode::RIGHT)
		plan.matched_word_count = static_cast<uint32_t>((entries + 31u) / 32u);
	plan.directory_bytes = static_cast<uint64_t>(plan.capacity) * sizeof(pg_atomic_uint32) +
		entries * (sizeof(uint32_t) + sizeof(uint16_t) +
			(build->build_rows_use_keys ? 1u : 2u) * sizeof(HashJoinBatchRowRef)) +
		static_cast<uint64_t>(plan.matched_word_count) * sizeof(pg_atomic_uint32) +
		static_cast<uint64_t>(plan.segments) * sizeof(HashJoinBatchSegment) +
		static_cast<uint64_t>(plan.map_count) * sizeof(uint32_t);
	return plan;
}

uint64_t BatchFootprint(const BatchPlan &plan) {
	return plan.directory_bytes == UINT64_MAX ? UINT64_MAX : plan.segment_bytes + plan.directory_bytes;
}

uint64_t ChooseBatch(ExecCtx &ctx, PhysicalHashJoin &join, HashJoinSharedPayload *build,
                     uint64_t candidates, uint64_t limit) {
	uint64_t mask = 0;
	for (uint32_t part = 0; part < build->build_partition_count; ++part) {
		if (((candidates >> part) & 1u) == 0) continue;
		const uint64_t candidate = mask | (UINT64_C(1) << part);
		if (BatchFootprint(DescribeBatch(ctx, join, build, candidate)) <= limit) mask = candidate;
	}
	return mask;
}

void ChargeBatchDirectory(ExecCtx &ctx, PhysicalHashJoin &join, uint64_t bytes) {
	while (!buffer::YaapBufferTryChargeExternalBytes(Budget(ctx), bytes)) {
		CHECK_FOR_INTERRUPTS();
		if (!join.EvictUnusedBuildSegment(ctx, buffer::YaapSegmentHandle{}))
			elog(ERROR, "pg_yaap: insufficient memory for batch hash directory");
	}
}

dsa_pointer CreateBatch(ExecCtx &ctx, PhysicalHashJoin &join, HashJoinSharedPayload *build, uint64_t mask) {
	if (mask == 0) return InvalidDsaPointer;
	const auto plan = DescribeBatch(ctx, join, build, mask);
	if (plan.directory_bytes == UINT64_MAX)
		elog(ERROR, "pg_yaap: batch hash table exceeds row-index range");
	ChargeBatchDirectory(ctx, join, plan.directory_bytes);
	const dsa_pointer dp = dsa_allocate0(ctx.dsa, sizeof(HashJoinBatch));
	auto *batch = static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, dp));
	batch->partition_mask = mask;
	batch->charged_bytes = plan.directory_bytes;
	batch->row_count = static_cast<uint32_t>(plan.rows);
	batch->hash_capacity = plan.capacity;
	batch->segment_count = plan.segments;
	batch->segment_map_count = plan.map_count;
	if (plan.matched_word_count > 0)
		batch->matched_bitmap_dp = YaapDsaAllocate(ctx.dsa,
			static_cast<size_t>(plan.matched_word_count) * sizeof(pg_atomic_uint32));
	std::memcpy(batch->partition_offsets, plan.offsets, sizeof(plan.offsets));
	std::memcpy(batch->bucket_offsets, plan.bucket_offsets, sizeof(plan.bucket_offsets));
	std::memcpy(batch->bucket_masks, plan.bucket_masks, sizeof(plan.bucket_masks));
	pg_atomic_init_u32(&batch->next_pin, 0);
	pg_atomic_init_u32(&batch->pinned_workers, 0);
	pg_atomic_init_u32(&batch->directory_ready, 0);
	pg_atomic_init_u32(&batch->next_partition, 0);
	pg_atomic_init_u32(&batch->built_workers, 0);
	pg_atomic_init_u32(&batch->probe_done, 0);
	pg_atomic_init_u32(&batch->released_workers, 0);
	pg_atomic_init_u32(&batch->cleanup_state, 0);
	pg_atomic_init_u32(&batch->unmatched_done, 0);
	pg_atomic_init_u32(&batch->matched_build_rows, 0);
	pg_atomic_init_u32(&batch->profile_published, 0);
	if (plan.matched_word_count > 0) {
		auto *matched_words = static_cast<pg_atomic_uint32 *>(dsa_get_address(ctx.dsa,
			batch->matched_bitmap_dp));
		for (uint32_t i = 0; i < plan.matched_word_count; ++i)
			pg_atomic_init_u32(&matched_words[i], 0);
	}
	const size_t entries = std::max<uint32_t>(1, batch->row_count);
	batch->hash_table_dp = YaapDsaAllocate(ctx.dsa, static_cast<size_t>(plan.capacity) * sizeof(pg_atomic_uint32));
	batch->hash_links_dp = YaapDsaAllocate(ctx.dsa, entries * sizeof(uint32_t));
	batch->hash_salts_dp = YaapDsaAllocate(ctx.dsa, entries * sizeof(uint16_t));
	batch->key_refs_dp = YaapDsaAllocate(ctx.dsa, entries * sizeof(HashJoinBatchRowRef));
	batch->row_refs_dp = build->build_rows_use_keys ? batch->key_refs_dp
		: YaapDsaAllocate(ctx.dsa, entries * sizeof(HashJoinBatchRowRef));
	if (plan.segments) batch->segments_dp = YaapDsaAllocate(ctx.dsa, static_cast<size_t>(plan.segments) * sizeof(HashJoinBatchSegment));
	if (plan.map_count) batch->segment_map_dp = YaapDsaAllocate(ctx.dsa, static_cast<size_t>(plan.map_count) * sizeof(uint32_t));
	auto *heads = static_cast<pg_atomic_uint32 *>(dsa_get_address(ctx.dsa, batch->hash_table_dp));
	for (uint32_t i = 0; i < plan.capacity; ++i) pg_atomic_init_u32(&heads[i], UINT32_MAX);
	auto *segments = plan.segments ? static_cast<HashJoinBatchSegment *>(dsa_get_address(ctx.dsa, batch->segments_dp)) : nullptr;
	auto *map = plan.map_count ? static_cast<uint32_t *>(dsa_get_address(ctx.dsa, batch->segment_map_dp)) : nullptr;
	if (map) std::memset(map, 0xFF, static_cast<size_t>(plan.map_count) * sizeof(uint32_t));
	uint32_t index = 0, partitions = 0;
	for (uint32_t part = 0; part < build->build_partition_count; ++part) {
		if (((mask >> part) & 1u) == 0) continue;
		++partitions;
		BuildSegments(ctx, join, build, part, false, [&](buffer::YaapSharedSegmentDesc *segment) {
			if (index >= plan.segments || segment->segment_id >= plan.map_count || map[segment->segment_id] != UINT32_MAX)
				elog(ERROR, "pg_yaap: invalid or duplicate batch segment");
			segments[index] = HashJoinBatchSegment{segment->self_dp, InvalidDsaPointer,
				segment->arena_id, segment->segment_id, segment->generation, segment->capacity};
			map[segment->segment_id] = index++;
		});
	}
	if (index != plan.segments) elog(ERROR, "pg_yaap: batch segment count changed");
	{
		PipelineProfileScope tables(ctx, PipelineProfileStage::HASH_JOIN_BATCH_TABLES); tables.AddRows(1);
		PipelineProfileScope parts(ctx, PipelineProfileStage::HASH_JOIN_BATCH_PARTITIONS); parts.AddRows(partitions);
		PipelineProfileScope refs(ctx, PipelineProfileStage::HASH_JOIN_BATCH_DIRECT_REFS); refs.AddRows(plan.rows);
	}
	return dp;
}

void DestroyBatch(ExecCtx &ctx, dsa_pointer dp) {
	if (!DsaPointerIsValid(dp)) return;
	auto *batch = static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, dp));
	if (batch->charged_bytes == 0) return;
	if (batch->row_refs_dp == batch->key_refs_dp) batch->row_refs_dp = InvalidDsaPointer;
		dsa_pointer *arrays[] = {&batch->matched_bitmap_dp, &batch->segments_dp, &batch->segment_map_dp, &batch->hash_table_dp,
		&batch->hash_links_dp, &batch->hash_salts_dp, &batch->key_refs_dp, &batch->row_refs_dp};
	for (auto *array : arrays) {
		if (DsaPointerIsValid(*array)) dsa_free(ctx.dsa, *array);
		*array = InvalidDsaPointer;
	}
	buffer::YaapBufferReleaseExternalBytes(Budget(ctx), batch->charged_bytes);
	batch->charged_bytes = 0;
	/* Keep the small synchronization object until query teardown: workers
	 * still observing cleanup_state must not dereference freed metadata. */
}

dsa_pointer CreateDeferredBatch(ExecCtx &ctx, PhysicalHashJoinProbe &op, ProbePayload *p) {
	auto *build = BuildPayload(ctx, op.join());
	const uint64_t all = p->partitions == 64 ? UINT64_MAX : (UINT64_C(1) << p->partitions) - 1;
	const uint64_t candidates = all & ~p->completed_mask;
	if (candidates == 0) return InvalidDsaPointer;
	uint64_t mask = ChooseBatch(ctx, op.join(), build, candidates, AdmissionBudget(ctx, p));
	if (mask == 0) {
		/* Soft admission may be smaller than one partition. A single batch
		 * may exceed soft quota, but its pinned rows+directory must fit hard. */
		auto *budget = Budget(ctx);
		const uint64_t saved = p->budget;
		p->budget = budget->hard_limit;
		const uint64_t hard_available = AdmissionBudget(ctx, p);
		p->budget = saved;
		for (uint32_t part = 0; part < p->partitions; ++part) {
			if (((candidates >> part) & 1u) &&
				BatchFootprint(DescribeBatch(ctx, op.join(), build, UINT64_C(1) << part)) <= hard_available) {
				mask = UINT64_C(1) << part;
				break;
			}
		}
		if (mask == 0)
			elog(ERROR, "pg_yaap: no build partition fits a fully resident hash batch; recursive repartitioning required");
		PipelineProfileScope oversized(ctx, PipelineProfileStage::HASH_JOIN_OVERSIZED_PARTITION_PAIR);
		oversized.AddRows(1);
	}
	auto *pairs = Pairs(ctx, p);
	for (uint32_t part = 0; part < p->partitions; ++part)
		if ((mask >> part) & 1u) pairs[part].state = 0;
	return CreateBatch(ctx, op.join(), build, mask);
}

enum class PairClaimResult { CLAIMED, EXHAUSTED };

PairClaimResult ClaimPair(ExecCtx &ctx, PhysicalHashJoinProbe &op, ProbeSourceGlobal &global, ProbeSourceLocal &local) {
	auto *p = global.payload;
	auto *pairs = Pairs(ctx, p);
	if (!DsaPointerIsValid(p->current_batch_dp)) return PairClaimResult::EXHAUSTED;
	auto *batch = static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, p->current_batch_dp));
	local.batch_dp = p->current_batch_dp;
	uint32_t chosen = UINT32_MAX;
	uint32_t chosen_slot = 0;
	bool first_chain = false;
	auto *chains = Chains(ctx, p);
	SpinLockAcquire(&p->mutex);
	for (uint32_t i = 0; i < p->partitions; ++i) {
		if (((batch->partition_mask >> i) & 1u) == 0 || pairs[i].state == 2) continue;
		auto &pair = pairs[i];
		while (pair.next_slot < p->slots &&
			!DsaPointerIsValid(chains[pair.next_slot * p->partitions + i].first)) ++pair.next_slot;
		if (pair.next_slot == p->slots) {
			if (pair.active_chains == 0) pair.state = 2;
			continue;
		}
		chosen = i;
		chosen_slot = pair.next_slot++;
		first_chain = pair.state == 0;
		pair.state = 1;
		++pair.active_chains;
		++p->active_pairs;
		break;
	}
	SpinLockRelease(&p->mutex);
	if (chosen == UINT32_MAX) return PairClaimResult::EXHAUSTED;
	local.partition = chosen;
	/* Producer chains are disjoint immutable probe input. Many workers may
	 * read the same batch table while each owns exactly one probe chain. */
	local.slot = chosen_slot;
	local.slot_end = chosen_slot + 1;
	local.prefetch_cursor = chains[chosen_slot * p->partitions + chosen].first;
	local.prefetch_head = local.prefetch_count = 0;
	local.prefetch_bytes = 0;
	local.segment = InvalidDsaPointer;
	local.row = 0;
	if (first_chain) {
		PipelineProfileScope count(ctx, PipelineProfileStage::HASH_JOIN_PARTITION_PAIR);
		count.AddRows(1);
	}
	{
		PipelineProfileScope count(ctx, PipelineProfileStage::HASH_JOIN_PROBE_CHAINS);
		count.AddRows(1);
	}
	local.join_state = op.join().GetOperatorState(ctx);
	op.join().SetProbeBatch(*local.join_state, p->current_batch_dp);
	return PairClaimResult::CLAIMED;
}

void PrefetchProbeWindow(ExecCtx &ctx, ProbeSourceLocal &local) {
	/* This cursor belongs to one producer chain of a selected batch partition.
	 * Do not prefetch another partition or reload/pin DSM ahead of demand. */
	while (local.prefetch_count < ProbeSourceLocal::kPrefetchSegments &&
		DsaPointerIsValid(local.prefetch_cursor)) {
		auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(
			dsa_get_address(ctx.dsa, local.prefetch_cursor));
		const uint64_t bytes = segment->spill_extent.bytes;
		const bool advise = pg_atomic_read_u32(&segment->state) ==
			static_cast<uint32_t>(buffer::YaapSharedSegmentState::SPILLED) &&
			bytes > 0 && bytes <= ProbeSourceLocal::kPrefetchBytes;
		if (advise && bytes > ProbeSourceLocal::kPrefetchBytes - local.prefetch_bytes) break;
		uint64_t charged = 0;
		if (advise) {
			charged = bytes; /* Bound requests even when the OS rejects a hint. */
			PipelineProfileScope scope(ctx, PipelineProfileStage::HASH_JOIN_PROBE_PREFETCH);
			if (local.spill.PrefetchBlock(segment->spill_extent)) {
				scope.AddRows(1);
			}
		}
		const uint32_t slot = (local.prefetch_head + local.prefetch_count) % ProbeSourceLocal::kPrefetchSegments;
		local.prefetch_charges[slot] = charged;
		local.prefetch_bytes += charged;
		++local.prefetch_count;
		local.prefetch_cursor = segment->chain_next_dp;
	}
}

bool ReadProbeChunk(ExecCtx &ctx, PhysicalHashJoinProbe &op, ProbeSourceGlobal &global, ProbeSourceLocal &local) {
	PipelineProfileScope read(ctx, PipelineProfileStage::HASH_JOIN_PARTITION_INPUT_READ);
	auto *p = global.payload;
	auto *chains = Chains(ctx, p);
	local.chunk->reset();
	while (local.chunk->count < PIPELINE_DEFAULT_CHUNK_SIZE && local.slot < local.slot_end) {
		auto &chain = chains[local.slot * p->partitions + local.partition];
		if (!DsaPointerIsValid(local.segment)) {
			local.segment = chain.first;
			local.row = 0;
			if (!DsaPointerIsValid(local.segment)) { ++local.slot; continue; }
		}
		auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, local.segment));
		if (!local.probe_lease.IsValid()) {
			PrefetchProbeWindow(ctx, local);
			Acquire(ctx, op.join(), global.payload->arena_dp, segment, local.spill, &local.probe_lease);
		}
		auto *tdc = static_cast<const TupleDataCollection *>(local.probe_lease.data);
		if (local.row == pg_atomic_read_u32(const_cast<pg_atomic_uint32 *>(&tdc->row_count))) {
			const dsa_pointer next = segment->chain_next_dp;
			const auto handle = local.probe_lease.handle;
			buffer::YaapSharedSegmentReleaseRead(ctx.dsa, global.payload->arena_dp, &local.probe_lease);
			Retire(ctx, global.payload->arena_dp, handle);
			if (local.prefetch_count != 0) {
				local.prefetch_bytes -= local.prefetch_charges[local.prefetch_head];
				local.prefetch_head = (local.prefetch_head + 1) % ProbeSourceLocal::kPrefetchSegments;
				--local.prefetch_count;
			}
			chain.first = next;
			local.segment = next;
			local.row = 0;
			if (!DsaPointerIsValid(next)) ++local.slot;
			continue;
		}
		const uint8_t *row = TupleDataCollectionGetRowConst(tdc, local.row++);
		const uint16_t out_row = local.chunk->count;
		Gather(global.layout, tdc, row, *local.chunk, out_row);
		for (uint32_t slot = 0; slot < 16; ++slot)
			local.chunk->nulls[slot][out_row] = row[p->null_offset + slot];
		++local.chunk->count;
	}
	return local.chunk->count != 0;
}


/* Process-private state; only the POD ProbePayload is shared across workers. */
struct HybridProbeState final : OperatorState, PgMemoryContextObject {
	ProbePayload *payload;
	dsa_pointer active_batch_dp = InvalidDsaPointer;
	dsa_area *dsa;
	dsa_pointer build_arena;
	buffer::YaapBufferSharedState *buffer_budget;
	buffer::YaapSpillStore initial_spill;
	PgVector<buffer::YaapSharedSegmentReadLease> initial_leases;
	std::unique_ptr<GlobalSinkState> sink_global;
	std::unique_ptr<LocalSinkState> sink_local;
	std::unique_ptr<OperatorState> stream_join;
	std::unique_ptr<OperatorState> initial_unmatched_join_state;
	std::unique_ptr<PipelineChunk> stream_chunk;
	PipelineChunk *stream_input = nullptr;
	uint64_t chunk_charged_bytes = 0;
	std::unique_ptr<GlobalSourceState> drain_global;
	std::unique_ptr<LocalSourceState> drain_local;
	bool have_stream_input = false;
	bool eof_published = false;
	bool initial_scan_barrier_passed = false;
	bool initial_unmatched_initialized = false;
	bool initial_unmatched_worker_published = false;
	uint32_t initial_unmatched_cursor = 0;
	uint32_t initial_unmatched_end = 0;
	bool leases_released = false;
	bool done = false;

	HybridProbeState(ExecCtx &ctx, ProbePayload *p, dsa_pointer arena)
		: payload(p), dsa(ctx.dsa), build_arena(arena), buffer_budget(Budget(ctx)), initial_spill(ctx.mcxt),
		  initial_leases(PgMemoryContextAllocator<buffer::YaapSharedSegmentReadLease>(ctx.mcxt)) {
		const uint64_t all = p->partitions == 64 ? UINT64_MAX : (UINT64_C(1) << p->partitions) - 1;
		if (p->initial_mask != 0 && p->initial_mask != all) {
			if (!buffer::YaapBufferTryChargeExternalBytes(buffer_budget, sizeof(PipelineChunk)))
				elog(ERROR, "pg_yaap: hybrid input chunk exceeds query memory limit");
			chunk_charged_bytes = sizeof(PipelineChunk);
			stream_chunk = std::make_unique<PipelineChunk>();
		}
		initial_spill.AttachSharedFileSet(&ctx.control->spill_fileset, nullptr);
	}
	void ReleaseInitial() {
		for (auto &lease : initial_leases)
			buffer::YaapSharedSegmentReleaseRead(dsa, build_arena, &lease);
		initial_leases.clear();
	}
	~HybridProbeState() override {
		if (!done) pg_atomic_write_u32(&payload->abandoned, 1);
		ReleaseInitial();
		buffer::YaapBufferReleaseExternalBytes(buffer_budget, chunk_charged_bytes);
	}
};

void HybridErrorRelease(int, Datum arg) {
	auto *state = reinterpret_cast<HybridProbeState *>(DatumGetPointer(arg));
	pg_atomic_write_u32(&state->payload->abandoned, 1);
	state->ReleaseInitial();
	if (state->drain_local)
		static_cast<ProbeSourceLocal &>(*state->drain_local).ReleaseReads();
}

bool HybridLive(ExecCtx &ctx, ProbePayload *p) {
	return !PipelineCancelRequested(ctx) && pg_atomic_read_u32(&p->abandoned) == 0;
}

void HybridPause(ExecCtx &ctx, PipelineProfileStage stage) {
	PipelineProfileScope phase(ctx, stage);
	PipelineProfileScope wait(ctx, PipelineProfileStage::HASH_JOIN_HYBRID_WAIT);
	pg_usleep(1000L);
}

void PublishMatchedBuildRows(ExecCtx &ctx, HashJoinBatch *batch) {
	if (batch == nullptr) return;
	uint32_t expected = 0;
	if (pg_atomic_compare_exchange_u32(&batch->profile_published, &expected, 1)) {
		PipelineProfileScope matched(ctx, PipelineProfileStage::HASH_JOIN_MATCHED_BUILD_ROWS);
		matched.AddRows(pg_atomic_read_u32(&batch->matched_build_rows));
	}
}

void PrepareBatch(ExecCtx &ctx, PhysicalHashJoinProbe &op, HybridProbeState &state, dsa_pointer dp) {
	state.active_batch_dp = dp;
	if (!DsaPointerIsValid(dp)) return;
	auto *p = state.payload;
	auto *batch = static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, dp));
	auto *segments = batch->segment_count ? static_cast<HashJoinBatchSegment *>(dsa_get_address(ctx.dsa, batch->segments_dp)) : nullptr;
	/* All key and value segments are pinned before any table entry is built. */
	const uint32_t worker = ctx.worker_index < 0 ? p->participants - 1 : static_cast<uint32_t>(ctx.worker_index);
	const uint32_t start = batch->segment_count ? static_cast<uint64_t>(batch->segment_count) * worker / p->participants : 0;
	{
		PipelineProfileScope pin(ctx, PipelineProfileStage::HASH_JOIN_BATCH_PIN);
		for (uint32_t step = 0; step < batch->segment_count && HybridLive(ctx, p); ++step) {
			/* Each reader owns a lease on the whole batch. A failed owner cannot
			 * unpin memory another process is reading through a direct reference.
			 * Rotate starts so initial reloads are spread across processes. */
			const uint32_t index = (start + step) % batch->segment_count;
			auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, segments[index].descriptor_dp));
			state.initial_leases.emplace_back();
			Acquire(ctx, op.join(), state.build_arena, segment, state.initial_spill, &state.initial_leases.back());
		}
	}
	pg_atomic_fetch_add_u32(&batch->pinned_workers, 1);
	while (pg_atomic_read_u32(&batch->pinned_workers) < p->participants && HybridLive(ctx, p)) HybridPause(ctx, PipelineProfileStage::HASH_JOIN_WAIT_PIN);
	pg_read_barrier();
	if (!HybridLive(ctx, p)) return;
	uint32_t expected = 0;
	if (pg_atomic_compare_exchange_u32(&batch->directory_ready, &expected, 1)) {
		for (uint32_t i = 0; i < batch->segment_count; ++i) {
			auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, segments[i].descriptor_dp));
			if (segment->generation != segments[i].generation || !DsaPointerIsValid(segment->payload_dp) ||
				pg_atomic_read_u32(&segment->readers) == 0)
				elog(ERROR, "pg_yaap: batch segment is not pinned before build");
			segments[i].payload_dp = segment->payload_dp;
		}
		pg_write_barrier();
		pg_atomic_write_u32(&batch->directory_ready, 2);
	}
	while (pg_atomic_read_u32(&batch->directory_ready) != 2 && HybridLive(ctx, p)) HybridPause(ctx, PipelineProfileStage::HASH_JOIN_WAIT_DIRECTORY);
	pg_read_barrier();
	while (HybridLive(ctx, p)) {
		const uint32_t part = pg_atomic_fetch_add_u32(&batch->next_partition, 1);
		if (part >= p->partitions) break;
		if (((batch->partition_mask >> part) & 1u) == 0) continue;
		PipelineProfileScope prepare(ctx, PipelineProfileStage::HASH_JOIN_PAIR_PREPARE);
		PipelineProfileScope build(ctx, PipelineProfileStage::FINALIZE_HASH_JOIN);
		op.join().BuildBatchPartition(ctx, dp, part);
	}
	pg_atomic_fetch_add_u32(&batch->built_workers, 1);
	while (pg_atomic_read_u32(&batch->built_workers) < p->participants && HybridLive(ctx, p)) HybridPause(ctx, PipelineProfileStage::HASH_JOIN_WAIT_BUILD);
	pg_read_barrier();
}

void AdvanceBatch(ExecCtx &ctx, PhysicalHashJoinProbe &op, HybridProbeState &state) {
	auto *p = state.payload;
	auto *batch = static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, state.active_batch_dp));
	/* GetData returned only after this worker consumed every resumable output
	 * for its assigned probe chains. Other workers may still be probing. */
	if (op.join().join_mode() != HashJoinMatchMode::RIGHT) {
		pg_atomic_fetch_add_u32(&batch->probe_done, 1);
		while (pg_atomic_read_u32(&batch->probe_done) < p->participants && HybridLive(ctx, p)) {
			PipelineProfileScope wait(ctx, PipelineProfileStage::HASH_JOIN_PAIR_WAIT);
			HybridPause(ctx, PipelineProfileStage::HASH_JOIN_WAIT_DRAIN);
		}
		pg_read_barrier();
	}
	if (!HybridLive(ctx, p)) return;
	state.drain_local.reset();
	state.ReleaseInitial();
	pg_atomic_fetch_add_u32(&batch->released_workers, 1);
	while (pg_atomic_read_u32(&batch->released_workers) < p->participants && HybridLive(ctx, p)) HybridPause(ctx, PipelineProfileStage::HASH_JOIN_WAIT_RELEASE);
	pg_read_barrier();
	if (!HybridLive(ctx, p)) return;
	uint32_t expected = 0;
	if (pg_atomic_compare_exchange_u32(&batch->cleanup_state, &expected, 1)) {
		PipelineProfileScope cleanup(ctx, PipelineProfileStage::HASH_JOIN_BATCH_CLEANUP);
		const uint64_t mask = batch->partition_mask;
		DestroyBatch(ctx, state.active_batch_dp);
		for (uint32_t part = 0; part < p->partitions; ++part)
			if ((mask >> part) & 1u) op.join().ReleaseBuildPartitionAfterProbe(ctx, part);
		p->completed_mask |= mask;
		p->current_batch_dp = CreateDeferredBatch(ctx, op, p);
		pg_write_barrier();
		pg_atomic_write_u32(&batch->cleanup_state, 2);
	}
	while (pg_atomic_read_u32(&batch->cleanup_state) != 2 && HybridLive(ctx, p)) HybridPause(ctx, PipelineProfileStage::HASH_JOIN_WAIT_CLEANUP);
	pg_read_barrier();
	state.active_batch_dp = p->current_batch_dp;
}

void CopyHybridInputRow(ExecCtx &ctx, PhysicalHashJoin &join, HybridProbeState &state,
                        const TupleDataLayout *layout, const PipelineChunk &in,
                        uint16_t row, PipelineChunk &out) {
	const uint16_t dst = out.count;
	for (uint8_t slot = 0; slot < 16; ++slot) out.nulls[slot][dst] = in.nulls[slot][row];
	for (uint16_t i = 0; i < layout->column_count; ++i) {
		const auto &col = layout->columns[i];
		const uint8_t slot = col.src_col_idx;
		switch (col.kind) {
			case TdcColumnKind::INT32: out.int32_columns[slot][dst] = in.get_int32(slot, row); break;
			case TdcColumnKind::INT64: out.int64_columns[slot][dst] = in.get_int64(slot, row); break;
			case TdcColumnKind::DOUBLE: out.double_columns[slot][dst] = in.get_double(slot, row); break;
			case TdcColumnKind::STRING_REF:
				if (in.nulls[slot][row]) out.string_columns[slot][dst] = out.store_string_bytes("", 0);
				else {
					const auto ref = in.get_string_ref(slot, row);
					if (ref.len > 8) {
						const uint64_t required = out.string_arena.size() + static_cast<uint64_t>(ref.len);
						const uint64_t capacity = out.string_arena.capacity();
						if (required > capacity) {
							const uint64_t growth = std::max<uint64_t>(required, std::max<uint64_t>(256, capacity * 2));
							while (!buffer::YaapBufferTryChargeExternalBytes(state.buffer_budget, growth - capacity)) {
								CHECK_FOR_INTERRUPTS();
								if (!join.EvictUnusedBuildSegment(ctx, buffer::YaapSegmentHandle{}))
									elog(ERROR, "pg_yaap: hybrid string chunk exceeds query memory limit");
							}
							state.chunk_charged_bytes += growth - capacity;
							out.string_arena.reserve(growth);
							if (out.string_arena.capacity() != growth)
								elog(ERROR, "pg_yaap: unexpected hybrid string arena capacity");
						}
					}
					out.string_columns[slot][dst] = out.store_string_bytes(in.get_string_ptr(slot, row), ref.len);
				}
				break;
		}
	}
	++out.count;
}

} // namespace

int PhysicalHashJoinProbe::MaxThreads(ExecCtx &ctx) const { return join_->MaxThreads(ctx); }

std::unique_ptr<GlobalSinkState> PhysicalHashJoinProbe::GetGlobalSinkState(ExecCtx &ctx) {
	auto global = std::make_unique<ProbeSinkGlobal>();
	global->build = BuildPayload(ctx, *join_);
	if (global->build == nullptr || !global->build->finalized)
		elog(ERROR, "pg_yaap: probe partition collection ran before build completion");
	/* Do not interpret flat build stores as an empty local radix registry. */
	if (!global->build->direct_segmented_build || global->build->build_partition_count <= 1)
		elog(ERROR, "pg_yaap: partition probe requires a radix-segmented equality-key build");
	global->payload = Payload(ctx, *this);
	if (global->payload == nullptr && ctx.worker_index == LEADER_WORKER_INDEX) {
		dsa_pointer dp = dsa_allocate0(ctx.dsa, sizeof(ProbePayload));
		auto *p = static_cast<ProbePayload *>(dsa_get_address(ctx.dsa, dp));
		SpinLockInit(&p->mutex);
		p->slots = ctx.control->num_workers + 1;
		p->partitions = global->build->build_partition_count;
		const uint64_t tail_page_budget = Budget(ctx)->hard_limit /
			(static_cast<uint64_t>(p->slots) * p->partitions * 8u);
		uint32_t page_bytes = 1024u * 1024u;
		while (page_bytes > 16u * 1024u && page_bytes > tail_page_budget) page_bytes /= 2;
		const uint32_t arena_id = pg_atomic_fetch_add_u32(&ctx.control->next_partition_probe_arena_id, 1);
		if (arena_id < 3) elog(ERROR, "pg_yaap: probe arena identifier overflow");
		p->arena_dp = buffer::YaapSharedSegmentArenaCreate(ctx.dsa, Budget(ctx), arena_id, page_bytes);
		p->chains_dp = dsa_allocate0(ctx.dsa, sizeof(ProbeChain) * p->slots * p->partitions);
		p->pairs_dp = dsa_allocate0(ctx.dsa, sizeof(ProbePair) * p->partitions);
		const auto *input_layout = static_cast<const TupleDataLayout *>(dsa_get_address(ctx.dsa, join_->left_payload_layout_dp()));
		p->layout_dp = YaapDsaAllocate(ctx.dsa, sizeof(TupleDataLayout));
		auto *layout = static_cast<TupleDataLayout *>(dsa_get_address(ctx.dsa, p->layout_dp));
		*layout = *input_layout;
		p->null_offset = layout->row_width;
		layout->row_width += 16;
		p->budget = std::min(Budget(ctx)->hard_limit / 2u, Budget(ctx)->soft_limit);
		StoreSharedPayloadOnDescriptor(this, dp);
		global->payload = p;
	}
	if (global->payload == nullptr) elog(ERROR, "pg_yaap: probe partition payload missing");
	global->layout = static_cast<const TupleDataLayout *>(dsa_get_address(ctx.dsa, global->payload->layout_dp));
	global->keys = static_cast<const TupleDataLayout *>(dsa_get_address(ctx.dsa, join_->left_key_layout_dp()));
	return global;
}

std::unique_ptr<LocalSinkState> PhysicalHashJoinProbe::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &state) {
	auto &global = static_cast<ProbeSinkGlobal &>(state);
	auto local = std::make_unique<ProbeSinkLocal>();
	local->writers = PgVector<buffer::YaapSegmentedTdc>(global.payload->partitions,
		buffer::YaapSegmentedTdc{}, PgMemoryContextAllocator<buffer::YaapSegmentedTdc>(ctx.mcxt));
	local->last_used = PgVector<uint64_t>(global.payload->partitions, 0, PgMemoryContextAllocator<uint64_t>(ctx.mcxt));
	local->slot = ctx.worker_index < 0 ? global.payload->slots - 1 : ctx.worker_index;
	return local;
}

SinkResultType PhysicalHashJoinProbe::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) {
	auto &global = static_cast<ProbeSinkGlobal &>(input.global_state);
	auto &local = static_cast<ProbeSinkLocal &>(input.local_state);
	auto *arena = buffer::YaapSharedSegmentArenaResolve(ctx.dsa, global.payload->arena_dp);
	buffer::YaapSpillStore spill(ctx.mcxt);
	spill.AttachSharedFileSet(&ctx.control->spill_fileset, nullptr);
	const uint32_t tail_limit = std::max<uint32_t>(1, std::min<uint64_t>(global.payload->partitions,
		Budget(ctx)->hard_limit / (static_cast<uint64_t>(global.payload->slots) * arena->segment_size * 8u)));
	/* Allow one concurrent segment allocation per participant. The existing
	 * per-worker quota bounds writable tails (at least one per worker); do not flush
	 * those tails merely because other operators cross the 75% soft threshold.
	 * The allocator still atomically enforces the hard limit. */
	const uint64_t allocation_headroom = static_cast<uint64_t>(global.payload->slots) *
		(arena->segment_size + sizeof(buffer::YaapSharedSegmentDesc));
	for (uint16_t item = 0; item < (local.selected ? local.selection_count : in.count); ++item) {
		const uint16_t row_idx = local.selected ? local.selection[item] : item;
		if ((item & 1023u) == 0) CHECK_FOR_INTERRUPTS();
		const uint32_t part = Partition(HashGroup(global.keys, in, row_idx), global.build);
		auto &writer = local.writers[part];
		if (!DsaPointerIsValid(writer.arena_dp)) {
			uint32_t capacity = 8192;
			while (capacity > 1 && offsetof(TupleDataCollection, rows) +
				static_cast<uint64_t>(capacity) * global.layout->row_width +
				TupleDataCollectionDefaultHeapCapacity(global.layout, capacity) > arena->segment_size)
				capacity /= 2;
			buffer::YaapSegmentedTdcInit(&writer, global.payload->arena_dp,
				global.payload->layout_dp, local.slot, global.layout->row_width, capacity,
				TupleDataCollectionDefaultHeapCapacity(global.layout, capacity), global.build->owner_tag,
				"hash join radix probe input");
		}
		const uint32_t heap = TupleDataCollectionRequiredHeapBytesForChunkRow(global.layout, in, row_idx);
		if (writer.active.IsValid()) {
			auto *segment = buffer::YaapSharedSegmentResolve(ctx.dsa, writer.arena_dp, writer.active);
			if (pg_atomic_read_u32(&segment->state) == static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL)) {
				auto *tdc = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, segment->payload_dp));
				if (!TupleDataCollectionHasSpaceForAppend(tdc, heap)) {
					buffer::YaapSegmentedTdcSealPublish(ctx.dsa, &writer);
					Spill(ctx, writer.arena_dp, segment, spill);
					--local.active_tails;
				}
			}
		}
		const bool needs_segment = !writer.active.IsValid() ||
			pg_atomic_read_u32(&buffer::YaapSharedSegmentResolve(ctx.dsa, writer.arena_dp, writer.active)->state) !=
				static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL);
		while (needs_segment && HashJoinProbeShouldSpillTail(local.active_tails, tail_limit,
			buffer::YaapBufferResidentBytes(Budget(ctx)), Budget(ctx)->hard_limit, allocation_headroom)) {
			uint32_t oldest = UINT32_MAX;
			for (uint32_t i = 0; i < local.writers.size(); ++i) {
				const auto &candidate = local.writers[i];
				if (!candidate.active.IsValid()) continue;
				auto *segment = buffer::YaapSharedSegmentResolve(ctx.dsa, candidate.arena_dp, candidate.active);
				if (pg_atomic_read_u32(&segment->state) != static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL)) continue;
				if (oldest == UINT32_MAX || local.last_used[i] < local.last_used[oldest]) oldest = i;
			}
			if (oldest == UINT32_MAX) elog(ERROR, "pg_yaap: probe tail accounting mismatch");
			auto &victim = local.writers[oldest];
			auto *segment = buffer::YaapSharedSegmentResolve(ctx.dsa, victim.arena_dp, victim.active);
			buffer::YaapSegmentedTdcSealPublish(ctx.dsa, &victim);
			Spill(ctx, victim.arena_dp, segment, spill);
			--local.active_tails;
		}
		buffer::YaapSegmentedRowHandle handle;
		TupleDataCollection *tdc = nullptr;
		uint8_t *row = nullptr;
		if (!buffer::YaapSegmentedTdcAppendWithHeap(ctx.dsa, Budget(ctx), &writer, heap, &handle, &tdc, &row))
			elog(ERROR, "pg_yaap: probe partition append failed");
		if (needs_segment) ++local.active_tails;
		local.last_used[part] = ++local.clock;
		ScatterGroupOnly(global.layout, tdc, row, in, row_idx);
		for (uint32_t slot = 0; slot < 16; ++slot)
			row[global.payload->null_offset + slot] = in.nulls[slot][row_idx];
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalHashJoinProbe::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input) {
	auto &global = static_cast<ProbeSinkGlobal &>(input.global_state);
	auto &local = static_cast<ProbeSinkLocal &>(*input.local_state);
	buffer::YaapSpillStore spill(ctx.mcxt);
	spill.AttachSharedFileSet(&ctx.control->spill_fileset, nullptr);
	auto *chains = Chains(ctx, global.payload);
	for (uint32_t part = 0; part < global.payload->partitions; ++part) {
		auto &writer = local.writers[part];
		if (!writer.first.IsValid()) continue;
		buffer::YaapSegmentedTdcSealPublish(ctx.dsa, &writer);
		auto *segment = buffer::YaapSharedSegmentResolve(ctx.dsa, writer.arena_dp, writer.active);
		Spill(ctx, writer.arena_dp, segment, spill);
		auto &chain = chains[local.slot * global.payload->partitions + part];
		chain.first = buffer::YaapSharedSegmentResolve(ctx.dsa, writer.arena_dp, writer.first)->self_dp;
		chain.rows = writer.row_count;
	}
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalHashJoinProbe::Finalize(ExecCtx &ctx, GlobalSinkState &state) {
	auto &global = static_cast<ProbeSinkGlobal &>(state);
	auto *pairs = Pairs(ctx, global.payload);
	auto *chains = Chains(ctx, global.payload);
	for (uint32_t part = 0; part < global.payload->partitions; ++part) {
		if ((global.payload->initial_mask >> part) & 1u) continue;
		for (uint32_t slot = 0; slot < global.payload->slots; ++slot)
			pairs[part].rows += chains[slot * global.payload->partitions + part].rows;
	}
	global.payload->finalized = true;
	return SinkFinalizeType::READY;
}

std::unique_ptr<GlobalSourceState> PhysicalHashJoinProbe::GetGlobalSourceState(ExecCtx &ctx) {
	auto global = std::make_unique<ProbeSourceGlobal>();
	global->payload = Payload(ctx, *this);
	global->build = BuildPayload(ctx, *join_);
	if (global->payload == nullptr || !global->payload->finalized ||
		(!global->payload->consumed && global->build == nullptr))
		elog(ERROR, "pg_yaap: paired probe source ran before partition collection completed");
	global->layout = static_cast<const TupleDataLayout *>(dsa_get_address(ctx.dsa, global->payload->layout_dp));
	return global;
}

std::unique_ptr<LocalSourceState> PhysicalHashJoinProbe::GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &state) {
	auto &global = static_cast<ProbeSourceGlobal &>(state);
	auto local = std::make_unique<ProbeSourceLocal>(ctx, global.build->segment_arena_dp, global.payload->arena_dp);
	return local;
}

SourceResultType PhysicalHashJoinProbe::GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) {
	auto &global = static_cast<ProbeSourceGlobal &>(input.global_state);
	auto &local = static_cast<ProbeSourceLocal &>(input.local_state);
	if (global.payload->consumed) return SourceResultType::FINISHED;
	SourceResultType result = SourceResultType::FINISHED;
	PG_ENSURE_ERROR_CLEANUP(ErrorRelease, PointerGetDatum(&local));
	{
		for (;;) {
			CHECK_FOR_INTERRUPTS();
			if (PipelineCancelRequested(ctx) || (global.payload->hybrid &&
				pg_atomic_read_u32(&global.payload->abandoned))) break;
			if (local.partition == UINT32_MAX) {
				const auto claim = ClaimPair(ctx, *this, global, local);
				if (claim == PairClaimResult::EXHAUSTED) break;

				if (Pairs(ctx, global.payload)[local.partition].rows == 0) {
					CompletePair(ctx, *this, global, local);
					continue;
				}
			}
			if (local.have_input) {
				PipelineProfileScope probe(ctx, PipelineProfileStage::OP_HASH_JOIN);
				const auto r = join_->Execute(ctx, *local.chunk, out, *local.join_state);
				if (r == OperatorResultType::HAVE_MORE_OUTPUT) { result = SourceResultType::HAVE_MORE_OUTPUT; break; }
				local.have_input = false;
			}
			if (ReadProbeChunk(ctx, *this, global, local)) { local.have_input = true; continue; }
			CompletePair(ctx, *this, global, local);
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(ErrorRelease, PointerGetDatum(&local));
	if (result != SourceResultType::FINISHED || !HybridLive(ctx, global.payload) ||
		join_->join_mode() != HashJoinMatchMode::RIGHT || !DsaPointerIsValid(local.batch_dp))
		return result;
	auto *batch = static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, local.batch_dp));
	if (!local.probe_barrier_published) {
		pg_atomic_fetch_add_u32(&batch->probe_done, 1);
		local.probe_barrier_published = true;
	}
	while (pg_atomic_read_u32(&batch->probe_done) < global.payload->participants &&
		HybridLive(ctx, global.payload))
		HybridPause(ctx, PipelineProfileStage::HASH_JOIN_WAIT_DRAIN);
	pg_read_barrier();
	if (!HybridLive(ctx, global.payload))
		return SourceResultType::FINISHED;
	if (!local.unmatched_scan_initialized) {
		const uint32_t worker = ctx.worker_index < 0
			? global.payload->participants - 1 : static_cast<uint32_t>(ctx.worker_index);
		local.unmatched_cursor = static_cast<uint64_t>(batch->row_count) * worker /
			global.payload->participants;
		local.unmatched_end = static_cast<uint64_t>(batch->row_count) * (worker + 1) /
			global.payload->participants;
		local.unmatched_scan_initialized = true;
		local.unmatched_join_state = join_->GetOperatorState(ctx);
	}
	{
		PipelineProfileScope scan(ctx, PipelineProfileStage::HASH_JOIN_UNMATCHED_BUILD_SCAN);
		while (local.unmatched_cursor < local.unmatched_end &&
			out.count < PIPELINE_DEFAULT_CHUNK_SIZE) {
			const uint32_t row_idx = local.unmatched_cursor++;
			if (!join_->IsBatchBuildRowMatched(ctx, local.batch_dp, row_idx))
				join_->EmitUnmatchedBuildRow(ctx, local.batch_dp, row_idx,
					*local.unmatched_join_state, out);
		}
		scan.AddRows(out.count);
	}
	if (local.unmatched_cursor == local.unmatched_end && !local.unmatched_barrier_published) {
		pg_atomic_fetch_add_u32(&batch->unmatched_done, 1);
		local.unmatched_barrier_published = true;
	}
	if (local.unmatched_barrier_published) {
		while (pg_atomic_read_u32(&batch->unmatched_done) < global.payload->participants &&
			HybridLive(ctx, global.payload))
			HybridPause(ctx, PipelineProfileStage::HASH_JOIN_WAIT_DRAIN);
		pg_read_barrier();
		PublishMatchedBuildRows(ctx, batch);
	}
	return out.count > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

void PhysicalHashJoinProbe::InitializeHybrid(ExecCtx &ctx, uint32_t participants) {
	auto global_holder = GetGlobalSinkState(ctx);
	auto &global = static_cast<ProbeSinkGlobal &>(*global_holder);
	auto *p = global.payload;
	if (p->hybrid) return;
	if (participants == 0 || p->partitions > 64)
		elog(ERROR, "pg_yaap: invalid hybrid hash join participant/partition count");
	p->hybrid = true;
	p->participants = participants;
	pg_atomic_init_u32(&p->prepare_next, 0);
	pg_atomic_init_u32(&p->prepared_workers, 0);
	pg_atomic_init_u32(&p->scan_done, 0);
	pg_atomic_init_u32(&p->initial_unmatched_done, 0);
	pg_atomic_init_u32(&p->released_workers, 0);
	pg_atomic_init_u32(&p->drain_phase, 0);
	pg_atomic_init_u32(&p->abandoned, 0);
	/* Account for all hybrid operators sharing this query, including a
	 * downstream operator whose resident directories are not built yet. */
	const uint64_t reserved = pg_atomic_read_u64(&ctx.control->hybrid_resident_bytes);
	const uint64_t available = AdmissionBudget(ctx, p);
	const uint64_t limit = available > reserved ? available - reserved : 0;
	const uint64_t all = p->partitions == 64 ? UINT64_MAX : (UINT64_C(1) << p->partitions) - 1;
	p->initial_mask = ChooseBatch(ctx, *join_, global.build, all, limit);
	p->initial_bytes = p->initial_mask ? BatchFootprint(DescribeBatch(ctx, *join_, global.build, p->initial_mask)) : 0;
	p->initial_batch_dp = CreateBatch(ctx, *join_, global.build, p->initial_mask);
	p->current_batch_dp = p->initial_batch_dp;
	auto *pairs = Pairs(ctx, p);
	uint32_t count = 0;
	for (uint32_t part = 0; part < p->partitions; ++part) {
		if ((p->initial_mask >> part) & 1u) { pairs[part].state = 2; ++count; }
	}
	pg_atomic_fetch_add_u64(&ctx.control->hybrid_resident_bytes, p->initial_bytes);
	PipelineProfileScope initial(ctx, PipelineProfileStage::HASH_JOIN_HYBRID_INITIAL_PARTITIONS);
	initial.AddRows(count);
	if (PipelineProfileEnabled(ctx))
		ereport(NOTICE, (errmsg("pg_yaap[hybrid] event=%u build_partitions=" UINT64_FORMAT " resident_partitions=%u total_partitions=%u reserved_bytes=" UINT64_FORMAT " admission_bytes=" UINT64_FORMAT " run_workers=%u",
			ctx.profile_event_id, static_cast<uint64>(global.build->partitions_dp), count,
			p->partitions, p->initial_bytes, limit, participants)));
}

std::unique_ptr<OperatorState> PhysicalHashJoinProbe::GetOperatorState(ExecCtx &ctx) {
	auto *p = Payload(ctx, *this);
	auto *build = BuildPayload(ctx, *join_);
	if (p == nullptr || !p->hybrid || build == nullptr)
		elog(ERROR, "pg_yaap: hybrid hash join was not initialized by the scheduler");
	auto state = std::make_unique<HybridProbeState>(ctx, p, build->segment_arena_dp);
	state->sink_global = GetGlobalSinkState(ctx);
	state->sink_local = GetLocalSinkState(ctx, *state->sink_global);
	state->stream_join = join_->GetOperatorState(ctx);
	PG_ENSURE_ERROR_CLEANUP(HybridErrorRelease, PointerGetDatum(state.get()));
	{
		PrepareBatch(ctx, *this, *state, p->initial_batch_dp);
		if (DsaPointerIsValid(p->initial_batch_dp)) join_->SetProbeBatch(*state->stream_join, p->initial_batch_dp);
		pg_atomic_fetch_add_u32(&p->prepared_workers, 1);
		while (pg_atomic_read_u32(&p->prepared_workers) < p->participants && HybridLive(ctx, p))
			HybridPause(ctx, PipelineProfileStage::HASH_JOIN_WAIT_PREPARED);
		/* Atomic reads alone do not acquire preceding DSA writes on ARM. */
		pg_read_barrier();
	}
	PG_END_ENSURE_ERROR_CLEANUP(HybridErrorRelease, PointerGetDatum(state.get()));
	return state;
}

OperatorResultType PhysicalHashJoinProbe::Execute(ExecCtx &ctx, PipelineChunk &in,
                                                PipelineChunk &out, OperatorState &op_state) {
	auto &state = static_cast<HybridProbeState &>(op_state);
	auto *p = state.payload;
	if (!HybridLive(ctx, p)) return OperatorResultType::FINISHED;
	OperatorResultType result = OperatorResultType::NEED_MORE_INPUT;
	PG_ENSURE_ERROR_CLEANUP(HybridErrorRelease, PointerGetDatum(&state));
	{
		if (!state.have_stream_input) {
			auto &global = static_cast<ProbeSinkGlobal &>(*state.sink_global);
			auto &local = static_cast<ProbeSinkLocal &>(*state.sink_local);
			const uint64_t all = p->partitions == 64 ? UINT64_MAX : (UINT64_C(1) << p->partitions) - 1;
			uint16_t direct_rows = 0;
			local.selection_count = 0;
			if (p->initial_mask == all) {
				/* All partitions fit: probe the original input, without an
				 * extra partition hash or a copy of every column. */
				state.stream_input = &in;
				direct_rows = in.count;
			} else if (p->initial_mask == 0) {
				state.stream_input = nullptr;
				for (uint16_t row = 0; row < in.count; ++row) local.selection[local.selection_count++] = row;
			} else {
				state.stream_chunk->reset();
				for (uint16_t row = 0; row < in.count; ++row) {
					if ((row & 1023u) == 0) CHECK_FOR_INTERRUPTS();
					const uint32_t part = Partition(HashGroup(global.keys, in, row), global.build);
					if ((p->initial_mask >> part) & 1u)
						CopyHybridInputRow(ctx, *join_, state, global.layout, in, row, *state.stream_chunk);
					else local.selection[local.selection_count++] = row;
				}
				state.stream_input = state.stream_chunk.get();
				direct_rows = state.stream_chunk->count;
			}
			if (local.selection_count != 0) {
				PipelineProfileScope append(ctx, PipelineProfileStage::SINK_HASH_JOIN_PARTITION);
				append.AddRows(local.selection_count);
				local.selected = true;
				OperatorSinkInput sink{*state.sink_global, *state.sink_local};
				SinkChunk(ctx, in, sink);
				local.selected = false;
			}
			{
				PipelineProfileScope direct(ctx, PipelineProfileStage::HASH_JOIN_HYBRID_DIRECT_ROWS);
				direct.AddRows(direct_rows);
				PipelineProfileScope deferred(ctx, PipelineProfileStage::HASH_JOIN_HYBRID_DEFERRED_ROWS);
				deferred.AddRows(local.selection_count);
			}
			state.have_stream_input = direct_rows != 0;
		}
		if (state.have_stream_input) {
			PipelineProfileScope probe(ctx, PipelineProfileStage::OP_HASH_JOIN);
			result = join_->Execute(ctx, *state.stream_input, out, *state.stream_join);
			if (result == OperatorResultType::NEED_MORE_INPUT) state.have_stream_input = false;
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(HybridErrorRelease, PointerGetDatum(&state));
	return result;
}

OperatorResultType PhysicalHashJoinProbe::FinalExecute(ExecCtx &ctx, PipelineChunk &out,
                                                     OperatorState &op_state) {
	auto &state = static_cast<HybridProbeState &>(op_state);
	auto *p = state.payload;
	if (state.done) return OperatorResultType::NEED_MORE_INPUT;
	if (!HybridLive(ctx, p)) return OperatorResultType::FINISHED;
	OperatorResultType result = OperatorResultType::NEED_MORE_INPUT;
	PG_ENSURE_ERROR_CLEANUP(HybridErrorRelease, PointerGetDatum(&state));
	{
		if (!state.eof_published) {
			if (state.have_stream_input)
				elog(ERROR, "pg_yaap: hybrid EOF reached with resumable output pending");
			OperatorSinkCombineInput combine{state.sink_local.get(), *state.sink_global, UINT32_MAX};
			Combine(ctx, combine);
			state.stream_join.reset();
			state.stream_chunk.reset();
			state.stream_input = nullptr;
			buffer::YaapBufferReleaseExternalBytes(state.buffer_budget, state.chunk_charged_bytes);
			state.chunk_charged_bytes = 0;
			state.eof_published = true;
			pg_atomic_fetch_add_u32(&p->scan_done, 1);
		}
		if (!state.initial_scan_barrier_passed) {
			while (pg_atomic_read_u32(&p->scan_done) < p->participants && HybridLive(ctx, p))
				HybridPause(ctx, PipelineProfileStage::HASH_JOIN_WAIT_SCAN);
			pg_read_barrier();
			state.initial_scan_barrier_passed = true;
			if (join_->join_mode() == HashJoinMatchMode::RIGHT) {
				auto *batch = DsaPointerIsValid(p->initial_batch_dp)
					? static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, p->initial_batch_dp)) : nullptr;
				const uint32_t worker = ctx.worker_index < 0 ? p->participants - 1
					: static_cast<uint32_t>(ctx.worker_index);
				const uint32_t rows = batch != nullptr ? batch->row_count : 0;
				state.initial_unmatched_cursor = static_cast<uint64_t>(rows) * worker / p->participants;
				state.initial_unmatched_end = static_cast<uint64_t>(rows) * (worker + 1) / p->participants;
				state.initial_unmatched_initialized = true;
				state.initial_unmatched_join_state = join_->GetOperatorState(ctx);
			}
		}
		if (HybridLive(ctx, p) && join_->join_mode() == HashJoinMatchMode::RIGHT) {
			auto *batch = DsaPointerIsValid(p->initial_batch_dp)
				? static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, p->initial_batch_dp)) : nullptr;
			if (batch != nullptr) {
				PipelineProfileScope scan(ctx, PipelineProfileStage::HASH_JOIN_UNMATCHED_BUILD_SCAN);
				while (state.initial_unmatched_cursor < state.initial_unmatched_end &&
					out.count < PIPELINE_DEFAULT_CHUNK_SIZE) {
					const uint32_t row_idx = state.initial_unmatched_cursor++;
					if (!join_->IsBatchBuildRowMatched(ctx, p->initial_batch_dp, row_idx))
						join_->EmitUnmatchedBuildRow(ctx, p->initial_batch_dp, row_idx,
							*state.initial_unmatched_join_state, out);
				}
				scan.AddRows(out.count);
			}
			if (state.initial_unmatched_cursor < state.initial_unmatched_end &&
				out.count >= PIPELINE_DEFAULT_CHUNK_SIZE) {
				result = OperatorResultType::HAVE_MORE_OUTPUT;
			} else {
				if (!state.initial_unmatched_worker_published) {
					pg_atomic_fetch_add_u32(&p->initial_unmatched_done, 1);
					state.initial_unmatched_worker_published = true;
				}
				while (pg_atomic_read_u32(&p->initial_unmatched_done) < p->participants && HybridLive(ctx, p))
					HybridPause(ctx, PipelineProfileStage::HASH_JOIN_WAIT_DRAIN);
				pg_read_barrier();
				PublishMatchedBuildRows(ctx, batch);
				if (out.count > 0)
					result = OperatorResultType::HAVE_MORE_OUTPUT;
			}
		}
		if (result == OperatorResultType::HAVE_MORE_OUTPUT)
			goto hybrid_finalize_done;
		if (HybridLive(ctx, p)) {
			if (!state.leases_released) {
				state.ReleaseInitial();
				state.leases_released = true;
				pg_atomic_fetch_add_u32(&p->released_workers, 1);
			}
			while (pg_atomic_read_u32(&p->released_workers) < p->participants && HybridLive(ctx, p))
				HybridPause(ctx, PipelineProfileStage::HASH_JOIN_WAIT_RELEASE);
			pg_read_barrier();
		}
		if (HybridLive(ctx, p)) {
			uint32_t expected = 0;
			if (pg_atomic_compare_exchange_u32(&p->drain_phase, &expected, 1)) {
				PipelineProfileScope cleanup(ctx, PipelineProfileStage::HASH_JOIN_BATCH_CLEANUP);
				/* Every scanner has published its chains and dropped initial
				 * leases. Only now may the initial hash tables be destroyed. */
				DestroyBatch(ctx, p->initial_batch_dp);
				for (uint32_t part = 0; part < p->partitions; ++part)
					if ((p->initial_mask >> part) & 1u) join_->ReleaseBuildPartitionAfterProbe(ctx, part);
				p->completed_mask |= p->initial_mask;
				pg_atomic_fetch_sub_u64(&ctx.control->hybrid_resident_bytes, p->initial_bytes);
				p->initial_bytes = 0;
				Finalize(ctx, *state.sink_global);
				// Partitions without probe rows need neither a table nor a later batch.
				auto *pairs = Pairs(ctx, p);
				const bool right_outer = join_->join_mode() == HashJoinMatchMode::RIGHT;
				for (uint32_t part = 0; part < p->partitions; ++part) {
					if (!right_outer && ((p->completed_mask >> part) & 1u) == 0 &&
						pairs[part].rows == 0) {
						join_->ReleaseBuildPartitionAfterProbe(ctx, part);
						p->completed_mask |= UINT64_C(1) << part;
					}
				}
				p->current_batch_dp = CreateDeferredBatch(ctx, *this, p);
				pg_write_barrier();
				pg_atomic_write_u32(&p->drain_phase, 2);
			}
			while (pg_atomic_read_u32(&p->drain_phase) != 2 && HybridLive(ctx, p)) HybridPause(ctx, PipelineProfileStage::HASH_JOIN_WAIT_CLEANUP);
			pg_read_barrier();
			while (HybridLive(ctx, p)) {
				if (!state.drain_local) {
					if (!DsaPointerIsValid(p->current_batch_dp)) { state.done = true; break; }
					PrepareBatch(ctx, *this, state, p->current_batch_dp);
					if (!HybridLive(ctx, p)) break;
					state.drain_global = GetGlobalSourceState(ctx);
					state.drain_local = GetLocalSourceState(ctx, *state.drain_global);
				}
				OperatorSourceInput source{*state.drain_global, *state.drain_local};
				if (GetData(ctx, out, source) == SourceResultType::HAVE_MORE_OUTPUT) {
					result = OperatorResultType::HAVE_MORE_OUTPUT;
					break;
				}
				AdvanceBatch(ctx, *this, state);
			}

		}
		if (!HybridLive(ctx, p)) result = OperatorResultType::FINISHED;
	}
		hybrid_finalize_done:;
	PG_END_ENSURE_ERROR_CLEANUP(HybridErrorRelease, PointerGetDatum(&state));
	return result;
}

void PhysicalHashJoinProbe::ReleaseAfterConsumerRun(ExecCtx &ctx) {
	auto *p = Payload(ctx, *this);
	if (p == nullptr || p->consumed) return;
	if (p->hybrid && pg_atomic_read_u32(&p->abandoned) == 0 &&
		(pg_atomic_read_u32(&p->drain_phase) != 2 || p->active_pairs != 0 || p->initial_bytes != 0))
		elog(ERROR, "pg_yaap: hybrid hash join cleanup before complete drain");
	DestroyBatch(ctx, p->current_batch_dp);
	/* RUN task completion releases every source lease first. On an early
	 * downstream stop, retire the remaining probe chains as well. */
	auto *chains = Chains(ctx, p);
	for (uint32_t i = 0; i < p->slots * p->partitions; ++i) {
		for (dsa_pointer dp = chains[i].first; DsaPointerIsValid(dp);) {
			auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, dp));
			const dsa_pointer next = segment->chain_next_dp;
			const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id, segment->generation};
			Retire(ctx, p->arena_dp, handle);
			dp = next;
		}
		chains[i].first = InvalidDsaPointer;
	}
	if (p->initial_bytes != 0) {
		pg_atomic_fetch_sub_u64(&ctx.control->hybrid_resident_bytes, p->initial_bytes);
		p->initial_bytes = 0;
	}
	p->consumed = true;
	buffer::YaapSharedSegmentArenaDestroy(ctx.dsa, Budget(ctx), p->arena_dp);
	p->arena_dp = InvalidDsaPointer;
	join_->ReleaseBuildPayloadAfterConsumerRun(ctx);
	/* Keep the small descriptor/layout metadata until query DSA teardown:
	 * leader FINALIZE can reconstruct this source's GlobalState after RUN. */
}

} // namespace pipeline
} // namespace pg_yaap
