/*
 * pipeline/aggregate_hash_table.cpp
 *
 * Open-addressing hash table over TupleDataCollection. See header for the
 * design contract. Spec: §3.4, §10 step 4 of
 * .sisyphus/plans/3g2-tuple-data-collection-design.md.
 *
 * Probe algorithm:
 *   slot = hash & mask
 *   step = ((hash >> 59) | 1)  # odd, covers power-of-two table
 *   for tries = 0 .. capacity:
 *     entry = entries[slot]
 *     if entry is empty:
 *         claim: write [salt:16 | row_idx:48], return INSERTED
 *     elif entry.salt == our_salt and MatchGroup(our_row, entry.row):
 *         existing match: return EXISTING with entry.row_index
 *     else:
 *         slot = (slot + step) & mask
 *   elog(ERROR, "AHT full")
 *
 * The whole probe is under the mutex in v1 — coarse but trivially correct.
 * Q3+ moves to per-thread local AHTs + merge phase, eliminating the lock.
 */

#include "aggregate_hash_table.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <cstring>

#if defined(__aarch64__)
#include <arm_neon.h>
#endif

#include "tuple_data_ops.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

static void
CopyGroupColumns(const TupleDataLayout *layout,
			 const TupleDataCollection *src_tdc,
			 const uint8_t *src_row,
			 TupleDataCollection *dst_tdc,
			 uint8_t *dst_row)
{
	for (uint16_t col_idx = 0; col_idx < layout->column_count; ++col_idx)
	{
		const TdcColumnDesc &col = layout->columns[col_idx];
		if (col.kind != TdcColumnKind::STRING_REF)
		{
			std::memcpy(dst_row + col.offset, src_row + col.offset, col.width);
			continue;
		}

		VecStringRef src_ref;
		std::memcpy(&src_ref, src_row + col.offset, sizeof(src_ref));
		const char *src_ptr = VecStringRefDataPtr(src_ref,
			src_tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(src_tdc)) : nullptr);
		VecStringRef dst_ref;
		if ((src_ptr == nullptr && src_ref.len != 0) ||
			!TupleDataCollectionStoreStringBytes(dst_tdc, src_ptr, src_ref.len, &dst_ref))
			elog(ERROR, "pg_yaap: aggregate hash table string group copy failed");
		std::memcpy(dst_row + col.offset, &dst_ref, sizeof(dst_ref));
	}
}

static inline uint16_t
HashSalt(uint64_t hash)
{
	return static_cast<uint16_t>(hash >> 48);
}

static inline uint32_t
ProbeStep(uint64_t hash)
{
	return static_cast<uint32_t>((hash >> 59) | UINT64CONST(1));
}

static inline uint64_t
PackEntry(uint16_t salt, uint32_t row_index)
{
	Assert(row_index != AHT_INVALID_ROW_INDEX);
	return (static_cast<uint64_t>(salt) << 48) |
		(static_cast<uint64_t>(row_index) & AHT_ROW_INDEX_MASK);
}

static inline bool
EntryIsEmpty(const AggHtEntry &entry)
{
	return entry.value == AHT_EMPTY_ENTRY_VALUE;
}

static inline uint16_t
EntrySalt(const AggHtEntry &entry)
{
	return static_cast<uint16_t>(entry.value >> 48);
}

static inline uint32_t
EntryRowIndex(const AggHtEntry &entry)
{
	return static_cast<uint32_t>(entry.value & AHT_ROW_INDEX_MASK);
}

struct ProbeMeta
{
	uint16_t salt;
	uint32_t slot;
	uint32_t step;
};

struct ProbeVector
{
	static constexpr uint16_t kWidth = 8;
	uint16_t lane_input[kWidth];
	uint64_t entry_value[kWidth];
	uint16_t count;
	uint16_t empty_mask;
	uint16_t salt_match_mask;
};

static inline ProbeMeta
BuildProbeMeta(const AggregateHashTable *aht, uint64_t hash)
{
	ProbeMeta meta;
	meta.salt = HashSalt(hash);
	meta.slot = static_cast<uint32_t>(hash) & aht->capacity_mask;
	meta.step = ProbeStep(hash);
	return meta;
}

static inline void
PrefetchEntry(const AggregateHashTable *aht, uint32_t slot)
{
#if defined(__GNUC__) || defined(__clang__)
	__builtin_prefetch(&aht->entries[slot], 0, 1);
#else
	(void) aht;
	(void) slot;
#endif
}

static inline bool
EntryValueIsEmpty(uint64_t value)
{
	return value == AHT_EMPTY_ENTRY_VALUE;
}

static inline uint16_t
EntryValueSalt(uint64_t value)
{
	return static_cast<uint16_t>(value >> 48);
}

static inline uint32_t
EntryValueRowIndex(uint64_t value)
{
	return static_cast<uint32_t>(value & AHT_ROW_INDEX_MASK);
}

static inline void
ProbeVectorInit(ProbeVector &vec)
{
	vec.count = 0;
	vec.empty_mask = 0;
	vec.salt_match_mask = 0;
}

static inline void
ProbeVectorAppend(ProbeVector &vec,
                  uint16_t input_idx,
                  uint64_t entry_value,
                  uint16_t wanted_salt)
{
	Assert(vec.count < ProbeVector::kWidth);
	const uint16_t lane = vec.count++;
	vec.lane_input[lane] = input_idx;
	vec.entry_value[lane] = entry_value;
	if (EntryValueIsEmpty(entry_value))
		vec.empty_mask |= static_cast<uint16_t>(1u << lane);
	else if (EntryValueSalt(entry_value) == wanted_salt)
		vec.salt_match_mask |= static_cast<uint16_t>(1u << lane);
}

#if defined(__aarch64__)
static inline uint8_t
NeonMaskEqU64x2(uint64x2_t cmp)
{
	uint8_t mask = 0;
	if (vgetq_lane_u64(cmp, 0) == UINT64_MAX)
		mask |= 1u << 0;
	if (vgetq_lane_u64(cmp, 1) == UINT64_MAX)
		mask |= 1u << 1;
	return mask;
}

static inline void
ProbeVectorFinalizeMasks(ProbeVector &vec, const uint16_t *wanted_salts)
{
	vec.empty_mask = 0;
	vec.salt_match_mask = 0;
	uint16_t lane = 0;
	for (; lane + 2 <= vec.count; lane += 2)
	{
		const uint64_t values[2] = {vec.entry_value[lane], vec.entry_value[lane + 1]};
		const uint64_t wanted[2] = {
			static_cast<uint64_t>(wanted_salts[lane]) << 48,
			static_cast<uint64_t>(wanted_salts[lane + 1]) << 48,
		};
		const uint64x2_t loaded = vld1q_u64(values);
		const uint64x2_t empty_cmp = vceqq_u64(loaded, vdupq_n_u64(AHT_EMPTY_ENTRY_VALUE));
		const uint64x2_t salt_cmp = vceqq_u64(
			vandq_u64(loaded, vdupq_n_u64(UINT64CONST(0xFFFF000000000000))),
			vld1q_u64(wanted));
		vec.empty_mask |= static_cast<uint16_t>(NeonMaskEqU64x2(empty_cmp) << lane);
		vec.salt_match_mask |= static_cast<uint16_t>(NeonMaskEqU64x2(salt_cmp) << lane);
	}
	for (; lane < vec.count; ++lane)
	{
		const uint64_t entry_value = vec.entry_value[lane];
		if (EntryValueIsEmpty(entry_value))
			vec.empty_mask |= static_cast<uint16_t>(1u << lane);
		else if (EntryValueSalt(entry_value) == wanted_salts[lane])
			vec.salt_match_mask |= static_cast<uint16_t>(1u << lane);
	}
}
#endif

static bool FindOrInsertLocked(AggregateHashTable *aht,
                               TupleDataCollection *tdc,
                               const TupleDataLayout *layout,
                               uint32_t group_row_idx,
                               const uint8_t *group_row_ptr,
                               const ProbeMeta &meta,
                               uint32_t *out_existing_row_idx);

static bool FindOrInsertFromSlotLocked(AggregateHashTable *aht,
                                       TupleDataCollection *tdc,
                                       const TupleDataLayout *layout,
                                       uint32_t group_row_idx,
                                       const uint8_t *group_row_ptr,
                                       const ProbeMeta &meta,
                                       uint32_t slot,
                                       uint32_t already_tried,
                                       uint32_t *out_existing_row_idx);

static bool
FindOrInsertLocked(AggregateHashTable *aht,
                   TupleDataCollection *tdc,
                   const TupleDataLayout *layout,
                   uint32_t group_row_idx,
                   const uint8_t *group_row_ptr,
                   uint64_t hash,
                   uint32_t *out_existing_row_idx)
{
	const ProbeMeta meta = BuildProbeMeta(aht, hash);
	PrefetchEntry(aht, meta.slot);
	return FindOrInsertLocked(aht,
		tdc,
		layout,
		group_row_idx,
		group_row_ptr,
		meta,
		out_existing_row_idx);
}

static bool
FindOrInsertLocked(AggregateHashTable *aht,
                   TupleDataCollection *tdc,
                   const TupleDataLayout *layout,
                   uint32_t group_row_idx,
                   const uint8_t *group_row_ptr,
                   const ProbeMeta &meta,
                   uint32_t *out_existing_row_idx)
{
	return FindOrInsertFromSlotLocked(aht,
		tdc,
		layout,
		group_row_idx,
		group_row_ptr,
		meta,
		meta.slot,
		0,
		out_existing_row_idx);
}

static bool
FindOrInsertFromSlotLocked(AggregateHashTable *aht,
                           TupleDataCollection *tdc,
                           const TupleDataLayout *layout,
                           uint32_t group_row_idx,
                           const uint8_t *group_row_ptr,
                           const ProbeMeta &meta,
                           uint32_t slot,
                           uint32_t already_tried,
                           uint32_t *out_existing_row_idx)
{
	const uint16_t our_salt = meta.salt;
	const uint32_t mask = aht->capacity_mask;
	const uint32_t step = meta.step;

	for (uint32_t tries = already_tried; tries < aht->capacity; ++tries)
	{
		AggHtEntry &e = aht->entries[slot];
		if (EntryIsEmpty(e))
		{
			e.value = PackEntry(our_salt, group_row_idx);
			*out_existing_row_idx = group_row_idx;
			return true;
		}
		if (EntrySalt(e) == our_salt)
		{
			const uint32_t existing_idx = EntryRowIndex(e);
			const uint8_t *existing_ptr = TupleDataCollectionGetRowConst(tdc, existing_idx);
				if (MatchGroupRow(layout, tdc, group_row_ptr, tdc, existing_ptr))
			{
				*out_existing_row_idx = existing_idx;
				return false;
			}
		}
		slot = (slot + step) & mask;
	}

	ereport(ERROR,
	        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
	         errmsg("pg_yaap: AggregateHashTable full (capacity=%u)",
	                aht->capacity)));
	pg_unreachable();
}

}  /* namespace */

uint32_t
AggregateHashTableChooseCapacity(uint32_t max_groups)
{
	constexpr uint32_t kMinCapacity = 32;
	constexpr uint32_t kMaxCapacity = 1u << 22;
	uint64_t needed = static_cast<uint64_t>(max_groups < 1 ? 1u : max_groups);
	needed = needed + (needed >> 1) + 1u;
	if (needed < kMinCapacity)
		needed = kMinCapacity;
	if (needed > kMaxCapacity)
		needed = kMaxCapacity;

	uint32_t pow2 = 1;
	while (pow2 < needed)
		pow2 <<= 1;
	return pow2;
}

uint32_t
HashAggChoosePartitionCount(uint32_t worker_count, uint32_t row_width)
{
	uint32_t width_target;
	if (row_width < 32u)
		width_target = 1u << 8;
	else if (row_width < 64u)
		width_target = 1u << 7;
	else
		width_target = 1u << 6;

	uint32_t target = worker_count < 1 ? 1u : worker_count * 4u;
	if (target > width_target)
		target = width_target;
	if (target < 8u)
		target = 8u;
	if (target > 32u)
		target = 32u;

	uint32_t pow2 = 1;
	while (pow2 < target)
		pow2 <<= 1;
	return pow2;
}

uint32_t
HashAggPartitionIndex(uint64_t hash, uint32_t partition_mask)
{
	return static_cast<uint32_t>(hash >> HashAggPartitionShift(partition_mask)) & partition_mask;
}

uint32_t
HashAggPartitionShift(uint32_t partition_mask)
{
	if (partition_mask == 0)
		return 0;
	uint32_t bits = 0;
	uint32_t tmp = partition_mask;
	while (tmp != 0)
	{
		bits++;
		tmp >>= 1;
	}
	return 64u - bits;
}

void
AggregateHashTableInit(AggregateHashTable *aht, uint32_t capacity, dsa_pointer tdc_dp)
{
	Assert(aht != nullptr);
	Assert(capacity > 0 && (capacity & (capacity - 1)) == 0);

	aht->capacity      = capacity;
	aht->capacity_mask = capacity - 1u;
	aht->tdc_dp        = tdc_dp;
	SpinLockInit(&aht->mutex);
	aht->_pad = 0;

	/* dsa_allocate0 gives value=0, a valid packed [salt=0,row=0] entry. */
	for (uint32_t i = 0; i < capacity; ++i)
		aht->entries[i].value = AHT_EMPTY_ENTRY_VALUE;
}

void
AggregateHashTableRehash(AggregateHashTable *aht,
                         TupleDataCollection *tdc,
                         const TupleDataLayout *layout)
{
	Assert(aht != nullptr && tdc != nullptr && layout != nullptr);

	for (uint32_t i = 0; i < aht->capacity; ++i)
		aht->entries[i].value = AHT_EMPTY_ENTRY_VALUE;

	const uint32_t row_count = pg_atomic_read_u32(&tdc->row_count);
	for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
	{
		const uint8_t *row_ptr = TupleDataCollectionGetRowConst(tdc, row_idx);
		const uint64_t hash = HashGroupRow(layout, tdc, row_ptr);
		const uint16_t salt = HashSalt(hash);
		const uint32_t mask = aht->capacity_mask;
		const uint32_t step = ProbeStep(hash);
		uint32_t slot = static_cast<uint32_t>(hash) & mask;

		for (uint32_t tries = 0; tries < aht->capacity; ++tries)
		{
			AggHtEntry &e = aht->entries[slot];
			if (EntryIsEmpty(e))
			{
				e.value = PackEntry(salt, row_idx);
				break;
			}
			slot = (slot + step) & mask;
			if (tries + 1 == aht->capacity)
				ereport(ERROR,
				        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				         errmsg("pg_yaap: AggregateHashTable rehash failed (capacity=%u)",
				                aht->capacity)));
		}
	}
}

bool
AggregateHashTableShouldResize(const AggregateHashTable *aht,
                               TupleDataCollection *tdc)
{
	Assert(aht != nullptr && tdc != nullptr);
	const uint32_t row_count = pg_atomic_read_u32(&tdc->row_count);
	return static_cast<uint64_t>(row_count) * 3u >=
		static_cast<uint64_t>(aht->capacity) * 2u;
}

bool
AggregateHashTableFindOrInsert(AggregateHashTable *aht,
                               TupleDataCollection *tdc,
                               const TupleDataLayout *layout,
                               uint32_t group_row_idx,
                               const uint8_t *group_row_ptr,
                               uint64_t hash,
                               uint32_t *out_existing_row_idx)
{
	Assert(aht != nullptr && tdc != nullptr && layout != nullptr);
	Assert(group_row_ptr != nullptr && out_existing_row_idx != nullptr);
	Assert(group_row_idx != AHT_INVALID_ROW_INDEX);

	uint32_t found_idx = AHT_INVALID_ROW_INDEX;
	SpinLockAcquire(&aht->mutex);
	const bool inserted = FindOrInsertLocked(aht,
		tdc,
		layout,
		group_row_idx,
		group_row_ptr,
		hash,
		&found_idx);
	SpinLockRelease(&aht->mutex);

	*out_existing_row_idx = found_idx;
	return inserted;
}

void
AggregateHashTableFindOrInsertBatch(AggregateHashTable *aht,
                                    TupleDataCollection *tdc,
                                    const TupleDataLayout *layout,
                                    PipelineChunk &chunk,
                                    const AggregateHashTableBatchProbeInput *inputs,
                                    uint16_t count,
                                    AggregateHashTableBatchProbeResult *results)
{
	Assert(aht != nullptr && tdc != nullptr && layout != nullptr);
	Assert(inputs != nullptr && results != nullptr);
	ProbeMeta probe_meta[PIPELINE_DEFAULT_CHUNK_SIZE];
	uint32_t current_slot[PIPELINE_DEFAULT_CHUNK_SIZE];
	uint32_t probe_tries[PIPELINE_DEFAULT_CHUNK_SIZE];
	uint16_t active[PIPELINE_DEFAULT_CHUNK_SIZE];
	uint16_t deferred[PIPELINE_DEFAULT_CHUNK_SIZE];
	const uint8_t *match_rows[ProbeVector::kWidth];
	uint16_t match_input_indices[ProbeVector::kWidth];
	uint16_t match_row_indices[ProbeVector::kWidth];
	uint32_t match_existing_indices[ProbeVector::kWidth];
	bool match_results[ProbeVector::kWidth];
	bool resolved[PIPELINE_DEFAULT_CHUNK_SIZE];
	Assert(count <= PIPELINE_DEFAULT_CHUNK_SIZE);

	for (uint16_t i = 0; i < count; ++i)
	{
		probe_meta[i] = BuildProbeMeta(aht, inputs[i].hash);
		current_slot[i] = probe_meta[i].slot;
		probe_tries[i] = 0;
		active[i] = i;
		resolved[i] = false;
		PrefetchEntry(aht, probe_meta[i].slot);
	}

	SpinLockAcquire(&aht->mutex);
	{
		uint16_t active_count = count;
		while (active_count > 0)
		{
			uint16_t deferred_count = 0;
			uint16_t pos = 0;

			while (pos < active_count)
			{
				ProbeVector vec;
				uint16_t wanted_salts[ProbeVector::kWidth];
				ProbeVectorInit(vec);
				while (pos < active_count && vec.count < ProbeVector::kWidth)
				{
					const uint16_t input_idx = active[pos++];
					if (probe_tries[input_idx] >= aht->capacity)
						ereport(ERROR,
						        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						         errmsg("pg_yaap: AggregateHashTable full (capacity=%u)",
						                aht->capacity)));
					const uint32_t slot = current_slot[input_idx];
					const uint16_t lane = vec.count;
					ProbeVectorAppend(vec,
						input_idx,
						aht->entries[slot].value,
						probe_meta[input_idx].salt);
					wanted_salts[lane] = probe_meta[input_idx].salt;
				}
				#if defined(__aarch64__)
				ProbeVectorFinalizeMasks(vec, wanted_salts);
				#endif

				uint16_t match_count = 0;
				for (uint16_t lane = 0; lane < vec.count; ++lane)
				{
					const uint16_t input_idx = vec.lane_input[lane];
					const uint16_t lane_bit = static_cast<uint16_t>(1u << lane);
					const uint32_t slot = current_slot[input_idx];
					AggHtEntry &entry = aht->entries[slot];

					if ((vec.empty_mask & lane_bit) != 0 && EntryIsEmpty(entry))
					{
						uint8_t *candidate_row = nullptr;
						const uint32_t candidate_idx = TupleDataCollectionAppendRow(tdc, &candidate_row);
						if (candidate_idx == TDC_INVALID_ROW_INDEX)
							elog(ERROR, "pg_yaap: local hash aggregate row capacity exceeded");

						ScatterGroupOnly(layout, tdc, candidate_row, chunk, inputs[input_idx].row_idx);
						entry.value = PackEntry(probe_meta[input_idx].salt, candidate_idx);
						results[input_idx].row_idx = inputs[input_idx].row_idx;
						results[input_idx].canonical_row_idx = candidate_idx;
						results[input_idx].inserted = true;
						resolved[input_idx] = true;
						continue;
					}

					const uint64_t value = entry.value;
					if (!EntryValueIsEmpty(value) &&
						EntryValueSalt(value) == probe_meta[input_idx].salt)
					{
						const uint32_t existing_idx = EntryValueRowIndex(value);
						match_rows[match_count] = TupleDataCollectionGetRowConst(tdc, existing_idx);
						match_input_indices[match_count] = input_idx;
						match_row_indices[match_count] = inputs[input_idx].row_idx;
						match_existing_indices[match_count] = existing_idx;
						++match_count;
						continue;
					}

					current_slot[input_idx] = (slot + probe_meta[input_idx].step) & aht->capacity_mask;
					++probe_tries[input_idx];
					PrefetchEntry(aht, current_slot[input_idx]);
					deferred[deferred_count++] = input_idx;
				}

				if (match_count > 0)
				{
					MatchGroupBatch(layout,
						tdc,
						match_rows,
						chunk,
						match_row_indices,
						match_count,
						match_results);
					for (uint16_t m = 0; m < match_count; ++m)
					{
						const uint16_t input_idx = match_input_indices[m];
						if (match_results[m])
						{
							results[input_idx].row_idx = inputs[input_idx].row_idx;
							results[input_idx].canonical_row_idx = match_existing_indices[m];
							results[input_idx].inserted = false;
							resolved[input_idx] = true;
							continue;
						}

						const uint32_t slot = current_slot[input_idx];
						current_slot[input_idx] = (slot + probe_meta[input_idx].step) & aht->capacity_mask;
						++probe_tries[input_idx];
						PrefetchEntry(aht, current_slot[input_idx]);
						deferred[deferred_count++] = input_idx;
					}
				}
			}

			for (uint16_t i = 0; i < deferred_count; ++i)
				active[i] = deferred[i];
			active_count = deferred_count;
		}
	}
	SpinLockRelease(&aht->mutex);

	for (uint16_t i = 0; i < count; ++i)
		Assert(resolved[i]);
}

void
AggregateHashTableCombineRow(AggregateHashTable *aht,
                             TupleDataCollection *tdc,
                             const TupleDataLayout *layout,
							 const TupleDataCollection *src_tdc,
                             const uint8_t *src_row,
                             uint64_t hash)
{
	Assert(aht != nullptr && tdc != nullptr && layout != nullptr && src_row != nullptr);

	const uint16_t our_salt = HashSalt(hash);
	const uint32_t mask     = aht->capacity_mask;
	uint32_t       slot     = static_cast<uint32_t>(hash) & mask;
	const uint32_t step     = ProbeStep(hash);

	uint8_t *canonical_row = nullptr;
	bool     overflow      = false;

	SpinLockAcquire(&aht->mutex);
	{
		for (uint32_t tries = 0; tries < aht->capacity; ++tries)
		{
			AggHtEntry &e = aht->entries[slot];
			if (EntryIsEmpty(e))
			{
				/* Miss: allocate a fresh canonical row, copy group cols, claim
				 * the slot. The new row's aggregate slots are zero from
				 * dsa_allocate0; CombineAggregates() below merges src into
				 * that zero state, leaving a fully-initialized canonical row
				 * before we drop the mutex. No window for a peer worker to
				 * see a half-built row. */
				uint8_t *new_row = nullptr;
				const uint32_t new_idx = TupleDataCollectionAppendRow(tdc, &new_row);
				if (new_idx == TDC_INVALID_ROW_INDEX)
				{
					overflow = true;
					break;
				}
				CopyGroupColumns(layout, src_tdc, src_row, tdc, new_row);
				for (uint16_t agg_idx = 0; agg_idx < layout->aggregate_count; ++agg_idx)
				{
					const TdcAggregateDesc &agg = layout->aggregates[agg_idx];
					if (agg.kind == TdcAggKind::MIN_INT64 || agg.kind == TdcAggKind::MIN_NUMERIC)
					{
						const int64_t init_min = INT64_MAX;
						std::memcpy(new_row + agg.offset, &init_min, sizeof(int64_t));
					}
					else if (agg.kind == TdcAggKind::MAX_INT64 || agg.kind == TdcAggKind::MAX_NUMERIC)
					{
						const int64_t init_max = INT64_MIN;
						std::memcpy(new_row + agg.offset, &init_max, sizeof(int64_t));
					}
				}
				e.value       = PackEntry(our_salt, new_idx);
				canonical_row = new_row;
				break;
			}
			if (EntrySalt(e) == our_salt)
			{
				const uint32_t existing_idx = EntryRowIndex(e);
				const uint8_t *existing_ptr =
					TupleDataCollectionGetRowConst(tdc, existing_idx);
				if (MatchGroupRow(layout, src_tdc, src_row, tdc, existing_ptr))
				{
					canonical_row = TupleDataCollectionGetRow(tdc, existing_idx);
					break;
				}
			}
			slot = (slot + step) & mask;
		}

		if (canonical_row != nullptr)
			CombineAggregates(layout, canonical_row, src_row);
	}
	SpinLockRelease(&aht->mutex);

	if (overflow)
		ereport(ERROR,
		        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
		         errmsg("pg_yaap: AggregateHashTableCombineRow TDC full")));

	if (canonical_row == nullptr)
		ereport(ERROR,
		        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
		         errmsg("pg_yaap: AggregateHashTableCombineRow AHT full (capacity=%u)",
		                aht->capacity)));
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
