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
#include "parallel/pipeline/execution_slots.hpp"
#include "parallel/pipeline/cancel.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"

namespace pg_yaap {
namespace pipeline {
namespace {

struct ProbeChain { dsa_pointer first; uint64_t rows; };
struct ProbePair { uint32_t state; uint32_t next_slot; uint32_t active_chains; uint64_t rows; };
static constexpr uint32_t kRepartitionFanout = 8;
static constexpr uint32_t kMaxRepartitionDepth = 4;
static constexpr uint32_t kMaxSubPartitionStack = 64;
struct SubPartitionItem {
	uint32_t    root_part;
	uint32_t    depth;
	uint32_t    build_rows;
	uint32_t    _pad0;
	uint64_t    probe_rows;
	dsa_pointer build_key_first_dp;
	dsa_pointer build_row_first_dp;
	dsa_pointer probe_first_dp;
};
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
	uint32_t sub_stack_size;
	SubPartitionItem sub_stack[kMaxSubPartitionStack];
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
	const dsa_pointer dp = join.shared_payload_dp();
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
			const bool loaded = buffer::YaapSharedSegmentReload(ctx.dsa, Budget(ctx), handle, arena,
				&spill, segment->spill_extent, lease);
			if (loaded) return;
			if (join.EvictUnusedBuildSegment(ctx, handle)) continue;
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
	if (!buffer::YaapSharedSegmentSpill(ctx.dsa, Budget(ctx), handle, arena, &spill, &extent))
		elog(ERROR, "pg_yaap: probe partition spill failed");
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
	if (HashJoinTracksBuildMatches(join.join_mode()))
		plan.matched_word_count = static_cast<uint32_t>((entries + 31u) / 32u);
	const uint64_t hash_table_bytes = static_cast<uint64_t>(plan.capacity) * sizeof(pg_atomic_uint32);
	const uint64_t hash_links_bytes = entries * sizeof(uint32_t);
	const uint64_t hash_salts_bytes = entries * sizeof(uint16_t);
	const uint64_t row_refs_bytes = entries * sizeof(HashJoinBatchRowRef);
	const uint64_t matched_bytes = static_cast<uint64_t>(plan.matched_word_count) * sizeof(pg_atomic_uint32);
	const uint64_t segments_bytes = static_cast<uint64_t>(plan.segments) * sizeof(HashJoinBatchSegment);
	const uint64_t map_bytes = static_cast<uint64_t>(plan.map_count) * sizeof(uint32_t);
	if (hash_table_bytes > kYaapMaxSafeDsaSingleAllocBytes ||
		hash_links_bytes > kYaapMaxSafeDsaSingleAllocBytes ||
		hash_salts_bytes > kYaapMaxSafeDsaSingleAllocBytes ||
		row_refs_bytes > kYaapMaxSafeDsaSingleAllocBytes ||
		matched_bytes > kYaapMaxSafeDsaSingleAllocBytes ||
		segments_bytes > kYaapMaxSafeDsaSingleAllocBytes ||
		map_bytes > kYaapMaxSafeDsaSingleAllocBytes) {
		plan.directory_bytes = UINT64_MAX;
		return plan;
	}
	plan.directory_bytes = hash_table_bytes + hash_links_bytes + hash_salts_bytes +
		(build->build_rows_use_keys ? 1u : 2u) * row_refs_bytes +
		matched_bytes + segments_bytes + map_bytes;
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
	(void) partitions;
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

uint32_t RepartitionSubBucket(uint64_t hash, uint32_t depth) {
	uint64_t x = hash ^ (static_cast<uint64_t>(depth) * UINT64_C(0x9e3779b97f4a7c15));
	x ^= x >> 30;
	x *= UINT64_C(0xbf58476d1ce4e5b9);
	x ^= x >> 27;
	x *= UINT64_C(0x94d049bb133111eb);
	x ^= x >> 31;
	return static_cast<uint32_t>(x) & (kRepartitionFanout - 1u);
}

uint32_t RequiredHeapBytesForTdcRow(const TupleDataLayout *layout, const uint8_t *row) {
	uint32_t total = 0;
	for (uint16_t i = 0; i < layout->column_count; ++i) {
		const auto &col = layout->columns[i];
		if (col.kind != TdcColumnKind::STRING_REF) continue;
		VecStringRef ref;
		std::memcpy(&ref, row + col.offset, sizeof(ref));
		if (ref.len > 8) total += ref.len;
	}
	return total;
}

void CopyTdcRow(const TupleDataLayout *layout,
                const TupleDataCollection *src_tdc,
                const uint8_t *src_row,
                TupleDataCollection *dst_tdc,
                uint8_t *dst_row) {
	std::memcpy(dst_row, src_row, layout->row_width);
	for (uint16_t i = 0; i < layout->column_count; ++i) {
		const auto &col = layout->columns[i];
		if (col.kind != TdcColumnKind::STRING_REF)
			continue;
		VecStringRef src_ref;
		std::memcpy(&src_ref, src_row + col.offset, sizeof(src_ref));
		if (src_ref.len <= 8)
			continue;
		const char *src_ptr = VecStringRefDataPtr(src_ref,
			src_tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(src_tdc)) : nullptr);
		VecStringRef dst_ref;
		if (!TupleDataCollectionStoreStringBytes(dst_tdc, src_ptr, src_ref.len, &dst_ref))
			elog(ERROR, "pg_yaap: hash join repartition row copy ran out of heap");
		std::memcpy(dst_row + col.offset, &dst_ref, sizeof(dst_ref));
	}
}

void RetireSegmentChain(ExecCtx &ctx, dsa_pointer arena_dp, dsa_pointer first_dp) {
	for (dsa_pointer dp = first_dp; DsaPointerIsValid(dp);) {
		auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, dp));
		const dsa_pointer next = segment->chain_next_dp;
		const buffer::YaapSegmentHandle handle{segment->arena_id, segment->segment_id, segment->generation};
		Retire(ctx, arena_dp, handle);
		dp = next;
	}
}

void RetireSubPartitionBuild(ExecCtx &ctx, HashJoinSharedPayload *build,
                             dsa_pointer key_first_dp, dsa_pointer row_first_dp) {
	if (DsaPointerIsValid(key_first_dp))
		RetireSegmentChain(ctx, build->segment_arena_dp, key_first_dp);
	if (DsaPointerIsValid(row_first_dp) && row_first_dp != key_first_dp)
		RetireSegmentChain(ctx, build->segment_arena_dp, row_first_dp);
}

BatchPlan DescribeSubPartitionBatch(ExecCtx &ctx, PhysicalHashJoin &join,
                                    HashJoinSharedPayload *build, const SubPartitionItem &item) {
	BatchPlan plan;
	const uint32_t part = item.root_part;
	plan.rows = item.build_rows;
	if (plan.rows > UINT32_MAX / 2u) {
		plan.directory_bytes = UINT64_MAX;
		return plan;
	}
	uint64_t buckets = 16;
	while (buckets < plan.rows * 2u) buckets <<= 1;
	if (buckets > UINT32_MAX) {
		plan.directory_bytes = UINT64_MAX;
		return plan;
	}
	plan.offsets[part] = 0;
	plan.bucket_offsets[part] = 0;
	plan.bucket_masks[part] = static_cast<uint32_t>(buckets - 1u);
	plan.capacity = static_cast<uint32_t>(buckets);
	auto scan_chain = [&](dsa_pointer first_dp) {
		for (dsa_pointer dp = first_dp; DsaPointerIsValid(dp);) {
			auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, dp));
			++plan.segments;
			plan.segment_bytes += segment->capacity;
			plan.map_count = std::max(plan.map_count, segment->segment_id + 1);
			dp = segment->chain_next_dp;
		}
	};
	if (DsaPointerIsValid(item.build_key_first_dp))
		scan_chain(item.build_key_first_dp);
	if (!build->build_rows_use_keys && DsaPointerIsValid(item.build_row_first_dp) &&
		item.build_row_first_dp != item.build_key_first_dp)
		scan_chain(item.build_row_first_dp);
	const uint64_t entries = std::max<uint64_t>(1, plan.rows);
	if (HashJoinTracksBuildMatches(join.join_mode()))
		plan.matched_word_count = static_cast<uint32_t>((entries + 31u) / 32u);
	const uint64_t hash_table_bytes = static_cast<uint64_t>(plan.capacity) * sizeof(pg_atomic_uint32);
	const uint64_t hash_links_bytes = entries * sizeof(uint32_t);
	const uint64_t hash_salts_bytes = entries * sizeof(uint16_t);
	const uint64_t row_refs_bytes = entries * sizeof(HashJoinBatchRowRef);
	const uint64_t matched_bytes = static_cast<uint64_t>(plan.matched_word_count) * sizeof(pg_atomic_uint32);
	const uint64_t segments_bytes = static_cast<uint64_t>(plan.segments) * sizeof(HashJoinBatchSegment);
	const uint64_t map_bytes = static_cast<uint64_t>(plan.map_count) * sizeof(uint32_t);
	if (hash_table_bytes > kYaapMaxSafeDsaSingleAllocBytes ||
		hash_links_bytes > kYaapMaxSafeDsaSingleAllocBytes ||
		hash_salts_bytes > kYaapMaxSafeDsaSingleAllocBytes ||
		row_refs_bytes > kYaapMaxSafeDsaSingleAllocBytes ||
		matched_bytes > kYaapMaxSafeDsaSingleAllocBytes ||
		segments_bytes > kYaapMaxSafeDsaSingleAllocBytes ||
		map_bytes > kYaapMaxSafeDsaSingleAllocBytes) {
		plan.directory_bytes = UINT64_MAX;
		return plan;
	}
	plan.directory_bytes = hash_table_bytes + hash_links_bytes + hash_salts_bytes +
		(build->build_rows_use_keys ? 1u : 2u) * row_refs_bytes +
		matched_bytes + segments_bytes + map_bytes;
	return plan;
}

dsa_pointer CreateSubPartitionBatch(ExecCtx &ctx, PhysicalHashJoin &join,
                                    HashJoinSharedPayload *build, ProbePayload *p,
                                    const SubPartitionItem &item) {
	const uint32_t part = item.root_part;
	const uint64_t mask = UINT64_C(1) << part;
	const auto plan = DescribeSubPartitionBatch(ctx, join, build, item);
	if (plan.directory_bytes == UINT64_MAX)
		elog(ERROR, "pg_yaap: sub-partition batch hash table exceeds row-index range");
	ChargeBatchDirectory(ctx, join, plan.directory_bytes);
	const dsa_pointer dp = dsa_allocate0(ctx.dsa, sizeof(HashJoinBatch));
	auto *batch = static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, dp));
	batch->partition_mask = mask;
	batch->charged_bytes = plan.directory_bytes;
	batch->row_count = static_cast<uint32_t>(plan.rows);
	batch->hash_capacity = plan.capacity;
	batch->segment_count = plan.segments;
	batch->segment_map_count = plan.map_count;
	batch->is_subpartition = 1;
	batch->sub_build_key_first_dp = item.build_key_first_dp;
	batch->sub_build_row_first_dp = build->build_rows_use_keys ? item.build_key_first_dp : item.build_row_first_dp;
	if (plan.matched_word_count > 0) {
		batch->matched_bitmap_dp = YaapDsaAllocate(ctx.dsa,
			static_cast<size_t>(plan.matched_word_count) * sizeof(pg_atomic_uint32));
		auto *matched_words = static_cast<pg_atomic_uint32 *>(dsa_get_address(ctx.dsa, batch->matched_bitmap_dp));
		for (uint32_t i = 0; i < plan.matched_word_count; ++i)
			pg_atomic_init_u32(&matched_words[i], 0);
	}
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
	uint32_t index = 0;
	auto add_chain = [&](dsa_pointer first_dp) {
		for (dsa_pointer sdp = first_dp; DsaPointerIsValid(sdp);) {
			auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, sdp));
			if (index >= plan.segments || segment->segment_id >= plan.map_count || map[segment->segment_id] != UINT32_MAX)
				elog(ERROR, "pg_yaap: invalid or duplicate sub-partition batch segment");
			segments[index] = HashJoinBatchSegment{segment->self_dp, InvalidDsaPointer,
				segment->arena_id, segment->segment_id, segment->generation, segment->capacity};
			map[segment->segment_id] = index++;
			sdp = segment->chain_next_dp;
		}
	};
	if (DsaPointerIsValid(item.build_key_first_dp))
		add_chain(item.build_key_first_dp);
	if (!build->build_rows_use_keys && DsaPointerIsValid(item.build_row_first_dp) &&
		item.build_row_first_dp != item.build_key_first_dp)
		add_chain(item.build_row_first_dp);
	if (index != plan.segments) elog(ERROR, "pg_yaap: sub-partition batch segment count changed");

	/* Distribute the sub-partition's probe segments round-robin across p->slots
	 * so all workers can probe the sub-partition in parallel. */
	auto *chains = Chains(ctx, p);
	for (uint32_t s = 0; s < p->slots; ++s)
		chains[s * p->partitions + part] = ProbeChain{InvalidDsaPointer, 0};
	uint32_t rr = 0;
	for (dsa_pointer pdp = item.probe_first_dp; DsaPointerIsValid(pdp);) {
		auto *seg = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, pdp));
		const dsa_pointer next = seg->chain_next_dp;
		auto &dst_chain = chains[rr * p->partitions + part];
		seg->chain_next_dp = dst_chain.first;
		dst_chain.first = pdp;
		dst_chain.rows += 1;
		rr = (rr + 1u) % p->slots;
		pdp = next;
	}
	auto *pairs = Pairs(ctx, p);
	pairs[part] = ProbePair{0, 0, 0, item.probe_rows};
	return dp;
}

void RepartitionAndPushSubItems(ExecCtx &ctx, PhysicalHashJoinProbe &op,
                                HashJoinSharedPayload *build, ProbePayload *p,
                                uint32_t root_part, uint32_t new_depth,
                                bool from_top_level, const SubPartitionItem *parent_item) {
	if (new_depth > kMaxRepartitionDepth)
		elog(ERROR, "pg_yaap: hash join build partition exceeds memory_limit even after max recursive repartitioning depth (%u)",
			kMaxRepartitionDepth);
	if (p->sub_stack_size + kRepartitionFanout > kMaxSubPartitionStack)
		elog(ERROR, "pg_yaap: hash join recursive repartition stack overflow");

	PhysicalHashJoin &join = op.join();
	const auto *build_key_layout = static_cast<const TupleDataLayout *>(
		dsa_get_address(ctx.dsa, join.right_key_layout_dp()));
	const auto *build_row_layout = static_cast<const TupleDataLayout *>(
		dsa_get_address(ctx.dsa, join.right_payload_layout_dp()));
	const auto *probe_layout = static_cast<const TupleDataLayout *>(
		dsa_get_address(ctx.dsa, p->layout_dp));
	const auto *probe_key_layout = static_cast<const TupleDataLayout *>(
		dsa_get_address(ctx.dsa, join.left_key_layout_dp()));
	const auto *build_arena = buffer::YaapSharedSegmentArenaResolve(ctx.dsa, build->segment_arena_dp);
	const auto *probe_arena = buffer::YaapSharedSegmentArenaResolve(ctx.dsa, p->arena_dp);

	buffer::YaapSpillStore spill(ctx.mcxt);
	spill.AttachSharedFileSet(&ctx.control->spill_fileset, nullptr);

	auto compute_segment_rows = [](const TupleDataLayout *layout, uint32_t seg_size) -> uint32_t {
		uint32_t rows = 8192;
		while (rows > 1 && offsetof(TupleDataCollection, rows) +
			static_cast<uint64_t>(rows) * layout->row_width +
			TupleDataCollectionDefaultHeapCapacity(layout, rows) > seg_size)
			rows /= 2;
		return std::max<uint32_t>(1, rows);
	};

	const uint32_t build_key_seg_rows = compute_segment_rows(build_key_layout, build_arena->segment_size);
	const uint32_t build_row_seg_rows = build->build_rows_use_keys ? build_key_seg_rows
		: compute_segment_rows(build_row_layout, build_arena->segment_size);
	const uint32_t probe_seg_rows = compute_segment_rows(probe_layout, probe_arena->segment_size);

	auto seal_and_spill_writer = [&](buffer::YaapSegmentedTdc &writer) {
		if (!writer.active.IsValid()) return;
		auto *seg = buffer::YaapSharedSegmentResolve(ctx.dsa, writer.arena_dp, writer.active);
		if (seg != nullptr &&
			pg_atomic_read_u32(&seg->state) == static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL)) {
			buffer::YaapSegmentedTdcSealPublish(ctx.dsa, &writer);
			Spill(ctx, writer.arena_dp, seg, spill);
		}
	};

	auto append_with_spill = [&](buffer::YaapSegmentedTdc writers[kRepartitionFanout],
	                             uint32_t sub, dsa_pointer arena_dp, dsa_pointer layout_dp,
	                             const TupleDataLayout *layout, uint32_t seg_rows,
	                             uint32_t heap, const char *label,
	                             TupleDataCollection **out_tdc, uint8_t **out_row) {
		auto &w = writers[sub];
		if (!DsaPointerIsValid(w.arena_dp)) {
			buffer::YaapSegmentedTdcInit(&w, arena_dp, layout_dp, ExecutionTaskSlot(ctx),
				layout->row_width, seg_rows,
				TupleDataCollectionDefaultHeapCapacity(layout, seg_rows),
				build->owner_tag, label);
		}
		if (w.active.IsValid()) {
			auto *seg = buffer::YaapSharedSegmentResolve(ctx.dsa, w.arena_dp, w.active);
			if (seg != nullptr &&
				pg_atomic_read_u32(&seg->state) == static_cast<uint32_t>(buffer::YaapSharedSegmentState::WRITABLE_LOCAL)) {
				auto *tdc = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, seg->payload_dp));
				if (tdc != nullptr && !TupleDataCollectionHasSpaceForAppend(tdc, heap))
					seal_and_spill_writer(w);
			}
		}
		buffer::YaapSegmentedRowHandle handle;
		if (!buffer::YaapSegmentedTdcAppendWithHeap(ctx.dsa, Budget(ctx), &w, heap, &handle, out_tdc, out_row)) {
			for (uint32_t k = 0; k < kRepartitionFanout; ++k)
				seal_and_spill_writer(writers[k]);
			while (join.EvictUnusedBuildSegment(ctx, buffer::YaapSegmentHandle{})) {}
			if (!buffer::YaapSegmentedTdcAppendWithHeap(ctx.dsa, Budget(ctx), &w, heap, &handle, out_tdc, out_row))
				elog(ERROR, "pg_yaap: hash join recursive repartition writer ran out of memory");
		}
	};

	/* 1. Repartition build segments into kRepartitionFanout sub-chains. */
	buffer::YaapSegmentedTdc key_writers[kRepartitionFanout]{};
	buffer::YaapSegmentedTdc row_writers[kRepartitionFanout]{};
	uint32_t sub_build_rows[kRepartitionFanout]{};
	uint64_t total_build_rows = 0;

	struct StreamingChainReader {
		dsa_pointer next_dp = InvalidDsaPointer;
		dsa_pointer current_dp = InvalidDsaPointer;
		buffer::YaapSharedSegmentReadLease lease;
		const TupleDataCollection *tdc = nullptr;
		uint32_t row = 0;
		uint32_t count = 0;
	};
	auto advance_reader = [&](StreamingChainReader &r, dsa_pointer arena_dp) -> bool {
		while (r.row == r.count) {
			if (r.lease.IsValid()) {
				const auto handle = r.lease.handle;
				buffer::YaapSharedSegmentReleaseRead(ctx.dsa, arena_dp, &r.lease);
				Retire(ctx, arena_dp, handle);
				r.tdc = nullptr;
			}
			if (!DsaPointerIsValid(r.next_dp)) return false;
			auto *seg = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, r.next_dp));
			r.current_dp = r.next_dp;
			r.next_dp = seg->chain_next_dp;
			Acquire(ctx, join, arena_dp, seg, spill, &r.lease);
			r.tdc = static_cast<const TupleDataCollection *>(r.lease.data);
			r.count = pg_atomic_read_u32(const_cast<pg_atomic_uint32 *>(&r.tdc->row_count));
			r.row = 0;
		}
		return true;
	};
	auto close_reader = [&](StreamingChainReader &r, dsa_pointer arena_dp) {
		if (r.lease.IsValid()) {
			const auto handle = r.lease.handle;
			buffer::YaapSharedSegmentReleaseRead(ctx.dsa, arena_dp, &r.lease);
			Retire(ctx, arena_dp, handle);
			r.tdc = nullptr;
		}
	};

	auto split_build_pair = [&](dsa_pointer key_first_dp, dsa_pointer row_first_dp) {
		StreamingChainReader kr, rr;
		kr.next_dp = key_first_dp;
		if (!build->build_rows_use_keys) rr.next_dp = row_first_dp;
		uint32_t checks = 0;
		while (advance_reader(kr, build->segment_arena_dp)) {
			if ((checks++ & 1023u) == 0) CHECK_FOR_INTERRUPTS();
			const uint8_t *krow = TupleDataCollectionGetRowConst(kr.tdc, kr.row++);
			const uint8_t *prow = nullptr;
			if (!build->build_rows_use_keys) {
				if (!advance_reader(rr, build->segment_arena_dp))
					elog(ERROR, "pg_yaap: repartition build payload chain ended early");
				prow = TupleDataCollectionGetRowConst(rr.tdc, rr.row++);
			}
			const uint64_t hash = HashGroupRow(build_key_layout, kr.tdc, krow);
			const uint32_t sub = RepartitionSubBucket(hash, new_depth);
			TupleDataCollection *dst_ktdc = nullptr;
			uint8_t *dst_krow = nullptr;
			const uint32_t kheap = RequiredHeapBytesForTdcRow(build_key_layout, krow);
			append_with_spill(key_writers, sub, build->segment_arena_dp, join.right_key_layout_dp(),
				build_key_layout, build_key_seg_rows, kheap, "hash join repartition build keys",
				&dst_ktdc, &dst_krow);
			CopyTdcRow(build_key_layout, kr.tdc, krow, dst_ktdc, dst_krow);
			if (!build->build_rows_use_keys) {
				TupleDataCollection *dst_rtdc = nullptr;
				uint8_t *dst_rrow = nullptr;
				const uint32_t rheap = RequiredHeapBytesForTdcRow(build_row_layout, prow);
				append_with_spill(row_writers, sub, build->segment_arena_dp, join.right_payload_layout_dp(),
					build_row_layout, build_row_seg_rows, rheap, "hash join repartition build rows",
					&dst_rtdc, &dst_rrow);
				CopyTdcRow(build_row_layout, rr.tdc, prow, dst_rtdc, dst_rrow);
			}
			++sub_build_rows[sub];
			++total_build_rows;
		}
		close_reader(kr, build->segment_arena_dp);
		if (!build->build_rows_use_keys)
			close_reader(rr, build->segment_arena_dp);
	};

	if (from_top_level) {
		auto *registry = static_cast<HashJoinLocalBuildRegistryEntry *>(
			dsa_get_address(ctx.dsa, build->local_build_registry_dp));
		for (uint32_t slot = 0; slot < build->local_state_slot_count; ++slot) {
			if (!DsaPointerIsValid(registry[slot].partition_state_dp)) continue;
			auto *locals = static_cast<HashJoinLocalPartitionState *>(
				dsa_get_address(ctx.dsa, registry[slot].partition_state_dp));
			auto &part = locals[root_part];
			if (part.row_count == 0) continue;
			const auto *kdesc = DsaPointerIsValid(part.build_key_segment_desc_dp)
				? buffer::YaapSegmentedTdcResolveDesc(ctx.dsa, part.build_key_segment_desc_dp) : nullptr;
			const auto *rdesc = (!build->build_rows_use_keys && DsaPointerIsValid(part.build_row_segment_desc_dp))
				? buffer::YaapSegmentedTdcResolveDesc(ctx.dsa, part.build_row_segment_desc_dp) : nullptr;
			if (kdesc != nullptr)
				split_build_pair(kdesc->first_desc_dp, rdesc != nullptr ? rdesc->first_desc_dp : InvalidDsaPointer);
			part.build_key_segment_desc_dp = InvalidDsaPointer;
			part.build_row_segment_desc_dp = InvalidDsaPointer;
			part.row_count = 0;
		}
	} else {
		split_build_pair(parent_item->build_key_first_dp, parent_item->build_row_first_dp);
	}

	dsa_pointer sub_build_key_first[kRepartitionFanout]{};
	dsa_pointer sub_build_row_first[kRepartitionFanout]{};
	uint32_t max_sub_build_rows = 0;
	for (uint32_t k = 0; k < kRepartitionFanout; ++k) {
		seal_and_spill_writer(key_writers[k]);
		if (key_writers[k].first.IsValid())
			sub_build_key_first[k] = buffer::YaapSharedSegmentResolve(
				ctx.dsa, key_writers[k].arena_dp, key_writers[k].first)->self_dp;
		else
			sub_build_key_first[k] = InvalidDsaPointer;
		if (!build->build_rows_use_keys) {
			seal_and_spill_writer(row_writers[k]);
			if (row_writers[k].first.IsValid())
				sub_build_row_first[k] = buffer::YaapSharedSegmentResolve(
					ctx.dsa, row_writers[k].arena_dp, row_writers[k].first)->self_dp;
			else
				sub_build_row_first[k] = InvalidDsaPointer;
		} else {
			sub_build_row_first[k] = sub_build_key_first[k];
		}
		if (sub_build_rows[k] > max_sub_build_rows)
			max_sub_build_rows = sub_build_rows[k];
	}

	/* Detect unbreakable skew: if all rows landed in a single sub-bucket,
	 * further hashing cannot shrink the partition. */
	if (total_build_rows > 0 && max_sub_build_rows == total_build_rows) {
		for (uint32_t k = 0; k < kRepartitionFanout; ++k)
			RetireSubPartitionBuild(ctx, build, sub_build_key_first[k], sub_build_row_first[k]);
		elog(ERROR, "pg_yaap: hash join build partition exceeds memory_limit due to extreme key skew (%llu identical-hash rows)",
			(unsigned long long) total_build_rows);
	}

	/* 2. Repartition probe segments into matching kRepartitionFanout sub-chains. */
	buffer::YaapSegmentedTdc probe_writers[kRepartitionFanout]{};
	uint64_t sub_probe_rows[kRepartitionFanout]{};
	auto scratch_chunk = std::make_unique<PipelineChunk>();

	auto split_probe_chain = [&](dsa_pointer probe_first_dp) {
		StreamingChainReader pr;
		pr.next_dp = probe_first_dp;
		uint32_t checks = 0;
		while (advance_reader(pr, p->arena_dp)) {
			if ((checks++ & 1023u) == 0) CHECK_FOR_INTERRUPTS();
			const uint8_t *prow = TupleDataCollectionGetRowConst(pr.tdc, pr.row++);
			scratch_chunk->reset();
			Gather(probe_layout, pr.tdc, prow, *scratch_chunk, 0);
			for (uint32_t s = 0; s < 16; ++s)
				scratch_chunk->nulls[s][0] = prow[p->null_offset + s];
			scratch_chunk->count = 1;
			const uint64_t hash = HashGroup(probe_key_layout, *scratch_chunk, 0);
			const uint32_t sub = RepartitionSubBucket(hash, new_depth);
			TupleDataCollection *dst_ptdc = nullptr;
			uint8_t *dst_prow = nullptr;
			const uint32_t pheap = RequiredHeapBytesForTdcRow(probe_layout, prow);
			append_with_spill(probe_writers, sub, p->arena_dp, p->layout_dp,
				probe_layout, probe_seg_rows, pheap, "hash join repartition probe rows",
				&dst_ptdc, &dst_prow);
			CopyTdcRow(probe_layout, pr.tdc, prow, dst_ptdc, dst_prow);
			std::memcpy(dst_prow + p->null_offset, prow + p->null_offset, 16);
			++sub_probe_rows[sub];
		}
		close_reader(pr, p->arena_dp);
	};

	if (from_top_level) {
		auto *chains = Chains(ctx, p);
		for (uint32_t s = 0; s < p->slots; ++s) {
			auto &chain = chains[s * p->partitions + root_part];
			if (DsaPointerIsValid(chain.first)) {
				split_probe_chain(chain.first);
				chain.first = InvalidDsaPointer;
				chain.rows = 0;
			}
		}
	} else {
		if (DsaPointerIsValid(parent_item->probe_first_dp))
			split_probe_chain(parent_item->probe_first_dp);
	}

	const bool needs_unmatched_build = HashJoinNeedsUnmatchedBuildRows(join.join_mode());
	const bool preserves_unmatched_probe =
		join.join_mode() == HashJoinMatchMode::LEFT ||
		join.join_mode() == HashJoinMatchMode::ANTI ||
		join.join_mode() == HashJoinMatchMode::MARK ||
		join.join_mode() == HashJoinMatchMode::MARK_FILTER_NOT;

	uint32_t pushed = 0;
	for (uint32_t k = 0; k < kRepartitionFanout; ++k) {
		seal_and_spill_writer(probe_writers[k]);
		dsa_pointer probe_first = probe_writers[k].first.IsValid()
			? buffer::YaapSharedSegmentResolve(ctx.dsa, probe_writers[k].arena_dp, probe_writers[k].first)->self_dp
			: InvalidDsaPointer;
		if (sub_build_rows[k] == 0 && !preserves_unmatched_probe) {
			if (DsaPointerIsValid(probe_first))
				RetireSegmentChain(ctx, p->arena_dp, probe_first);
			RetireSubPartitionBuild(ctx, build, sub_build_key_first[k], sub_build_row_first[k]);
			continue;
		}
		if (sub_probe_rows[k] == 0 && !needs_unmatched_build) {
			RetireSubPartitionBuild(ctx, build, sub_build_key_first[k], sub_build_row_first[k]);
			continue;
		}
		p->sub_stack[p->sub_stack_size++] = SubPartitionItem{
			root_part,
			new_depth,
			sub_build_rows[k],
			0,
			sub_probe_rows[k],
			sub_build_key_first[k],
			sub_build_row_first[k],
			probe_first
		};
		++pushed;
	}
	elog(DEBUG1,
		 "pg_yaap: hash join recursive repartition part=%u depth=%u build_rows=%llu -> %u sub-partitions",
		 root_part, new_depth, (unsigned long long) total_build_rows, pushed);
}

dsa_pointer CreateDeferredBatch(ExecCtx &ctx, PhysicalHashJoinProbe &op, ProbePayload *p) {
	auto *build = BuildPayload(ctx, op.join());
	auto hard_admission_budget = [&]() -> uint64_t {
		auto *budget = Budget(ctx);
		const uint64_t saved = p->budget;
		p->budget = budget->hard_limit;
		const uint64_t avail = AdmissionBudget(ctx, p);
		p->budget = saved;
		return avail;
	};
	for (;;) {
		/* First drain any recursively repartitioned sub-partitions (DFS order). */
		if (p->sub_stack_size > 0) {
			SubPartitionItem item = p->sub_stack[--p->sub_stack_size];
			const uint64_t footprint = BatchFootprint(
				DescribeSubPartitionBatch(ctx, op.join(), build, item));
			if (footprint > hard_admission_budget())
				while (op.join().EvictUnusedBuildSegment(ctx, buffer::YaapSegmentHandle{})) {}
			if (footprint <= hard_admission_budget())
				return CreateSubPartitionBatch(ctx, op.join(), build, p, item);
			RepartitionAndPushSubItems(ctx, op, build, p,
				item.root_part, item.depth + 1u, false, &item);
			continue;
		}
		const uint64_t all = p->partitions == 64 ? UINT64_MAX : (UINT64_C(1) << p->partitions) - 1;
		const uint64_t candidates = all & ~p->completed_mask;
		if (candidates == 0) return InvalidDsaPointer;
		uint64_t mask = ChooseBatch(ctx, op.join(), build, candidates, AdmissionBudget(ctx, p));
		if (mask == 0) {
			while (op.join().EvictUnusedBuildSegment(ctx, buffer::YaapSegmentHandle{})) {}
			const uint64_t hard_available = hard_admission_budget();
			uint32_t first_candidate = UINT32_MAX;
			for (uint32_t part = 0; part < p->partitions; ++part) {
				if (((candidates >> part) & 1u) == 0) continue;
				if (first_candidate == UINT32_MAX) first_candidate = part;
				if (BatchFootprint(DescribeBatch(ctx, op.join(), build, UINT64_C(1) << part)) <= hard_available) {
					mask = UINT64_C(1) << part;
					break;
				}
			}
			if (mask == 0) {
				/* No remaining partition fits in memory: recursively repartition
				 * the first oversized partition into kRepartitionFanout sub-partitions. */
				p->completed_mask |= (UINT64_C(1) << first_candidate);
				RepartitionAndPushSubItems(ctx, op, build, p, first_candidate, 1u, true, nullptr);
				continue;
			}
		}
		auto *pairs = Pairs(ctx, p);
		for (uint32_t part = 0; part < p->partitions; ++part)
			if ((mask >> part) & 1u) pairs[part].state = 0;
		return CreateBatch(ctx, op.join(), build, mask);
	}
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
			(void) local.spill.PrefetchBlock(segment->spill_extent);
		}
		const uint32_t slot = (local.prefetch_head + local.prefetch_count) % ProbeSourceLocal::kPrefetchSegments;
		local.prefetch_charges[slot] = charged;
		local.prefetch_bytes += charged;
		++local.prefetch_count;
		local.prefetch_cursor = segment->chain_next_dp;
	}
}

bool ReadProbeChunk(ExecCtx &ctx, PhysicalHashJoinProbe &op, ProbeSourceGlobal &global, ProbeSourceLocal &local) {
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

void HybridPause() {
	pg_usleep(1000L);
}

void PrepareBatch(ExecCtx &ctx, PhysicalHashJoinProbe &op, HybridProbeState &state, dsa_pointer dp) {
	state.active_batch_dp = dp;
	if (!DsaPointerIsValid(dp)) return;
	auto *p = state.payload;
	auto *batch = static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, dp));
	auto *segments = batch->segment_count ? static_cast<HashJoinBatchSegment *>(dsa_get_address(ctx.dsa, batch->segments_dp)) : nullptr;
	/* All key and value segments are pinned before any table entry is built. */
	const uint32_t worker = ctx.task_index;
	const uint32_t start = batch->segment_count ? static_cast<uint64_t>(batch->segment_count) * worker / p->participants : 0;
	for (uint32_t step = 0; step < batch->segment_count && HybridLive(ctx, p); ++step) {
		/* Each reader owns a lease on the whole batch. A failed owner cannot
		 * unpin memory another process is reading through a direct reference.
		 * Rotate starts so initial reloads are spread across processes. */
		const uint32_t index = (start + step) % batch->segment_count;
		auto *segment = static_cast<buffer::YaapSharedSegmentDesc *>(dsa_get_address(ctx.dsa, segments[index].descriptor_dp));
		state.initial_leases.emplace_back();
		Acquire(ctx, op.join(), state.build_arena, segment, state.initial_spill, &state.initial_leases.back());
	}
	pg_atomic_fetch_add_u32(&batch->pinned_workers, 1);
	while (pg_atomic_read_u32(&batch->pinned_workers) < p->participants && HybridLive(ctx, p)) HybridPause();
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
	while (pg_atomic_read_u32(&batch->directory_ready) != 2 && HybridLive(ctx, p)) HybridPause();
	pg_read_barrier();
	while (HybridLive(ctx, p)) {
		const uint32_t part = pg_atomic_fetch_add_u32(&batch->next_partition, 1);
		if (part >= p->partitions) break;
		if (((batch->partition_mask >> part) & 1u) == 0) continue;
		op.join().BuildBatchPartition(ctx, dp, part);
	}
	pg_atomic_fetch_add_u32(&batch->built_workers, 1);
	while (pg_atomic_read_u32(&batch->built_workers) < p->participants && HybridLive(ctx, p)) HybridPause();
	pg_read_barrier();
}

void AdvanceBatch(ExecCtx &ctx, PhysicalHashJoinProbe &op, HybridProbeState &state) {
	auto *p = state.payload;
	auto *batch = static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, state.active_batch_dp));
	/* GetData returned only after this worker consumed every resumable output
	 * for its assigned probe chains. Other workers may still be probing. */
	if (!HashJoinScansBuildResults(op.join().join_mode())) {
		pg_atomic_fetch_add_u32(&batch->probe_done, 1);
		while (pg_atomic_read_u32(&batch->probe_done) < p->participants && HybridLive(ctx, p))
			HybridPause();
		pg_read_barrier();
	}
	if (!HybridLive(ctx, p)) return;
	state.drain_local.reset();
	state.ReleaseInitial();
	pg_atomic_fetch_add_u32(&batch->released_workers, 1);
	while (pg_atomic_read_u32(&batch->released_workers) < p->participants && HybridLive(ctx, p)) HybridPause();
	pg_read_barrier();
	if (!HybridLive(ctx, p)) return;
	uint32_t expected = 0;
	if (pg_atomic_compare_exchange_u32(&batch->cleanup_state, &expected, 1)) {
		const uint64_t mask = batch->partition_mask;
		const bool is_sub = (batch->is_subpartition != 0);
		const dsa_pointer sub_key_first = batch->sub_build_key_first_dp;
		const dsa_pointer sub_row_first = batch->sub_build_row_first_dp;
		DestroyBatch(ctx, state.active_batch_dp);
		if (is_sub) {
			RetireSubPartitionBuild(ctx, BuildPayload(ctx, op.join()), sub_key_first, sub_row_first);
		} else {
			for (uint32_t part = 0; part < p->partitions; ++part)
				if ((mask >> part) & 1u) op.join().ReleaseBuildPartitionAfterProbe(ctx, part);
			p->completed_mask |= mask;
		}
		p->current_batch_dp = CreateDeferredBatch(ctx, op, p);
		pg_write_barrier();
		pg_atomic_write_u32(&batch->cleanup_state, 2);
	}
	while (pg_atomic_read_u32(&batch->cleanup_state) != 2 && HybridLive(ctx, p)) HybridPause();
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
			case TdcColumnKind::INT128: out.set_wide128(slot, dst, in.get_wide128(slot, row)); break;
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
	if (global->payload == nullptr && ctx.initialize_shared_state) {
		dsa_pointer dp = dsa_allocate0(ctx.dsa, sizeof(ProbePayload));
		auto *p = static_cast<ProbePayload *>(dsa_get_address(ctx.dsa, dp));
		SpinLockInit(&p->mutex);
		p->slots = ExecutionWorkerCount(ctx);
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
	local->slot = ExecutionTaskSlot(ctx);
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
		!HashJoinScansBuildResults(join_->join_mode()) || !DsaPointerIsValid(local.batch_dp))
		return result;
	auto *batch = static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, local.batch_dp));
	if (!local.probe_barrier_published) {
		pg_atomic_fetch_add_u32(&batch->probe_done, 1);
		local.probe_barrier_published = true;
	}
	while (pg_atomic_read_u32(&batch->probe_done) < global.payload->participants &&
		HybridLive(ctx, global.payload))
		HybridPause();
	pg_read_barrier();
	if (!HybridLive(ctx, global.payload))
		return SourceResultType::FINISHED;
	if (!local.unmatched_scan_initialized) {
		const uint32_t worker = ctx.task_index;
		local.unmatched_cursor = static_cast<uint64_t>(batch->row_count) * worker /
			global.payload->participants;
		local.unmatched_end = static_cast<uint64_t>(batch->row_count) * (worker + 1) /
			global.payload->participants;
		local.unmatched_scan_initialized = true;
		local.unmatched_join_state = join_->GetOperatorState(ctx);
	}
	while (local.unmatched_cursor < local.unmatched_end &&
		out.count < PIPELINE_DEFAULT_CHUNK_SIZE) {
		const uint32_t row_idx = local.unmatched_cursor++;
		const bool matched = join_->IsBatchBuildRowMatched(ctx, local.batch_dp, row_idx);
		if (HashJoinEmitsBuildMatch(join_->join_mode(), matched))
			join_->EmitBuildResultRow(ctx, local.batch_dp, row_idx,
				*local.unmatched_join_state, out);
	}
	if (local.unmatched_cursor == local.unmatched_end && !local.unmatched_barrier_published) {
		pg_atomic_fetch_add_u32(&batch->unmatched_done, 1);
		local.unmatched_barrier_published = true;
	}
	if (local.unmatched_barrier_published) {
		while (pg_atomic_read_u32(&batch->unmatched_done) < global.payload->participants &&
			HybridLive(ctx, global.payload))
			HybridPause();
		pg_read_barrier();
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
	for (uint32_t part = 0; part < p->partitions; ++part) {
		if ((p->initial_mask >> part) & 1u) pairs[part].state = 2;
	}
	pg_atomic_fetch_add_u64(&ctx.control->hybrid_resident_bytes, p->initial_bytes);
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
			HybridPause();
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
				local.selected = true;
				OperatorSinkInput sink{*state.sink_global, *state.sink_local};
				SinkChunk(ctx, in, sink);
				local.selected = false;
			}
			state.have_stream_input = direct_rows != 0;
		}
		if (state.have_stream_input) {
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
				HybridPause();
			pg_read_barrier();
			state.initial_scan_barrier_passed = true;
			if (HashJoinScansBuildResults(join_->join_mode())) {
				auto *batch = DsaPointerIsValid(p->initial_batch_dp)
					? static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, p->initial_batch_dp)) : nullptr;
				const uint32_t worker = ctx.task_index;
				const uint32_t rows = batch != nullptr ? batch->row_count : 0;
				state.initial_unmatched_cursor = static_cast<uint64_t>(rows) * worker / p->participants;
				state.initial_unmatched_end = static_cast<uint64_t>(rows) * (worker + 1) / p->participants;
				state.initial_unmatched_initialized = true;
				state.initial_unmatched_join_state = join_->GetOperatorState(ctx);
			}
		}
		if (HybridLive(ctx, p) && HashJoinScansBuildResults(join_->join_mode())) {
			auto *batch = DsaPointerIsValid(p->initial_batch_dp)
				? static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, p->initial_batch_dp)) : nullptr;
			if (batch != nullptr) {
				while (state.initial_unmatched_cursor < state.initial_unmatched_end &&
					out.count < PIPELINE_DEFAULT_CHUNK_SIZE) {
					const uint32_t row_idx = state.initial_unmatched_cursor++;
					const bool matched = join_->IsBatchBuildRowMatched(ctx, p->initial_batch_dp, row_idx);
					if (HashJoinEmitsBuildMatch(join_->join_mode(), matched))
						join_->EmitBuildResultRow(ctx, p->initial_batch_dp, row_idx,
							*state.initial_unmatched_join_state, out);
				}
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
					HybridPause();
				pg_read_barrier();
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
				HybridPause();
			pg_read_barrier();
		}
		if (HybridLive(ctx, p)) {
			uint32_t expected = 0;
			if (pg_atomic_compare_exchange_u32(&p->drain_phase, &expected, 1)) {
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
				const bool right_outer = HashJoinNeedsUnmatchedBuildRows(join_->join_mode());
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
			while (pg_atomic_read_u32(&p->drain_phase) != 2 && HybridLive(ctx, p)) HybridPause();
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
	if (DsaPointerIsValid(p->current_batch_dp)) {
		auto *cur_batch = static_cast<HashJoinBatch *>(dsa_get_address(ctx.dsa, p->current_batch_dp));
		if (cur_batch != nullptr && cur_batch->charged_bytes != 0 && cur_batch->is_subpartition != 0) {
			const dsa_pointer sk = cur_batch->sub_build_key_first_dp;
			const dsa_pointer sr = cur_batch->sub_build_row_first_dp;
			DestroyBatch(ctx, p->current_batch_dp);
			if (auto *b = BuildPayload(ctx, *join_); b != nullptr)
				RetireSubPartitionBuild(ctx, b, sk, sr);
		} else {
			DestroyBatch(ctx, p->current_batch_dp);
		}
	}
	if (auto *b = BuildPayload(ctx, *join_); b != nullptr) {
		while (p->sub_stack_size > 0) {
			const auto &item = p->sub_stack[--p->sub_stack_size];
			RetireSubPartitionBuild(ctx, b, item.build_key_first_dp, item.build_row_first_dp);
			if (DsaPointerIsValid(item.probe_first_dp))
				RetireSegmentChain(ctx, p->arena_dp, item.probe_first_dp);
		}
	}
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
	 * FINALIZE can reconstruct this source's GlobalState after RUN. */
}

} // namespace pipeline
} // namespace pg_yaap
