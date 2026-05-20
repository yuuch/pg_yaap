/*
 * pipeline/tuple_data_ops.cpp
 *
 * Stateless row<->chunk codec + hash/match/agg-update primitives. Spec:
 * .sisyphus/plans/3g2-tuple-data-collection-design.md §3.2, §4, §10 step 2.
 *
 * Layout invariant note: TupleDataLayout advances row offsets by each
 * column's aligned physical width. Scatter writes the exact physical value at
 * `col.offset` (including 16-byte VecStringRef values). HashGroup / MatchGroup
 * use typed comparison for STRING_REF and width-bound byte comparison for
 * fixed-width values.
 * This is critical for Q1 group-by (returnflag,linestatus) being two int32
 * cols: hashing the slot would still be deterministic, but matching
 * against a freshly-zeroed dst row would falsely accept any 4-byte int32
 * value — so we keep both consistently width-bound.
 */

#include "tuple_data_ops.hpp"
#include "tuple_data_layout.hpp"
#include "parallel/pipeline/types.hpp"
#include "core/data_chunk.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <cstring>
#include <cstdint>
#include <algorithm>

#if defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace pg_yaap {
namespace pipeline {

namespace {

constexpr int64_t kAvgNumericScaleFactor = 1000000000000LL;

static constexpr uint16_t AGG_DELTA_HASH_SLOTS = PIPELINE_DEFAULT_CHUNK_SIZE * 2;
static constexpr uint16_t AGG_DELTA_EMPTY_SLOT = UINT16_MAX;

static inline uint64_t
Mix64(uint64_t v)
{
	v ^= v >> 30;
	v *= UINT64CONST(0xbf58476d1ce4e5b9);
	v ^= v >> 27;
	v *= UINT64CONST(0x94d049bb133111eb);
	v ^= v >> 31;
	return v;
}

static inline uint64_t
HashStart()
{
	return UINT64CONST(0x9ae16a3b2f90404f);
}

static inline uint64_t
HashCombine(uint64_t hash, uint64_t value, uint16_t column_index)
{
	const uint64_t lane = value +
		(UINT64CONST(0x9e3779b97f4a7c15) * static_cast<uint64_t>(column_index + 1));
	return Mix64(hash ^ Mix64(lane));
}

static inline void
AddInt64At(uint8_t *row_ptr, uint16_t offset, int64_t add)
{
	int64_t acc;
	std::memcpy(&acc, row_ptr + offset, sizeof(acc));
	acc += add;
	std::memcpy(row_ptr + offset, &acc, sizeof(acc));
}

static inline int64_t
ReadInt64At(const uint8_t *row_ptr, uint16_t offset)
{
	int64_t value;
	std::memcpy(&value, row_ptr + offset, sizeof(value));
	return value;
}

static inline bool
LayoutHasStringColumns(const TupleDataLayout *layout)
{
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		if (layout->columns[i].kind == TdcColumnKind::STRING_REF)
			return true;
	}
	return false;
}

static inline uint16_t
LayoutGroupRegionWidth(const TupleDataLayout *layout)
{
	uint16_t width = 0;
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		width = std::max<uint16_t>(width, col.offset + col.width);
	}
	return width;
}

#if defined(__aarch64__)
static inline uint8_t
NeonLaneMaskEqU32(uint32x4_t cmp)
{
	uint8_t mask = 0;
	if (vgetq_lane_u32(cmp, 0) == UINT32_MAX)
		mask |= 1u << 0;
	if (vgetq_lane_u32(cmp, 1) == UINT32_MAX)
		mask |= 1u << 1;
	if (vgetq_lane_u32(cmp, 2) == UINT32_MAX)
		mask |= 1u << 2;
	if (vgetq_lane_u32(cmp, 3) == UINT32_MAX)
		mask |= 1u << 3;
	return mask;
}

static inline uint8_t
NeonLaneMaskEqU64(uint64x2_t cmp)
{
	uint8_t mask = 0;
	if (vgetq_lane_u64(cmp, 0) == UINT64_MAX)
		mask |= 1u << 0;
	if (vgetq_lane_u64(cmp, 1) == UINT64_MAX)
		mask |= 1u << 1;
	return mask;
}
#endif

struct AggDelta
{
	uint8_t *row_ptr = nullptr;
	int64_t values[TUPLE_DATA_MAX_COLUMNS];
	int64_t counts[TUPLE_DATA_MAX_COLUMNS];
};

static inline void
InitAggDelta(AggDelta &delta, uint8_t *row_ptr, uint16_t aggregate_count)
{
	delta.row_ptr = row_ptr;
	std::fill_n(delta.values, aggregate_count, int64_t{0});
	std::fill_n(delta.counts, aggregate_count, int64_t{0});
}

static inline void
AccumulateAggDelta(const TupleDataLayout *layout,
                   AggDelta &delta,
                   const PipelineChunk &chunk,
                   uint16_t row_idx)
{
	for (uint16_t a = 0; a < layout->aggregate_count; ++a)
	{
		const TdcAggregateDesc &agg = layout->aggregates[a];
		switch (agg.kind)
		{
			case TdcAggKind::SUM_INT64:
			case TdcAggKind::SUM_NUMERIC:
				Assert(agg.src_col_idx < 16);
				delta.values[a] += chunk.get_int64(agg.src_col_idx, row_idx);
				break;
			case TdcAggKind::COUNT_STAR:
				delta.values[a] += 1;
				break;
			case TdcAggKind::COUNT_NONNULL:
			case TdcAggKind::COUNT_DISTINCT_NONNULL:
				Assert(agg.src_col_idx < 16);
				if (chunk.nulls[agg.src_col_idx][row_idx] == 0)
					delta.values[a] += 1;
				break;
			case TdcAggKind::AVG_NUMERIC:
				Assert(agg.src_col_idx < 16);
				delta.values[a] += chunk.get_int64(agg.src_col_idx, row_idx);
				delta.counts[a] += 1;
				break;
			case TdcAggKind::MIN_INT64:
			case TdcAggKind::MIN_NUMERIC:
			{
				Assert(agg.src_col_idx < 16);
				const int64_t value = chunk.get_int64(agg.src_col_idx, row_idx);
				if (delta.counts[a] == 0 || value < delta.values[a])
					delta.values[a] = value;
				delta.counts[a] = 1;
				break;
			}
			case TdcAggKind::MAX_INT64:
			case TdcAggKind::MAX_NUMERIC:
			{
				Assert(agg.src_col_idx < 16);
				const int64_t value = chunk.get_int64(agg.src_col_idx, row_idx);
				if (delta.counts[a] == 0 || value > delta.values[a])
					delta.values[a] = value;
				delta.counts[a] = 1;
				break;
			}
		}
	}
}

static inline void
ApplyAggDelta(const TupleDataLayout *layout, const AggDelta &delta)
{
	for (uint16_t a = 0; a < layout->aggregate_count; ++a)
	{
		const TdcAggregateDesc &agg = layout->aggregates[a];
		switch (agg.kind)
		{
			case TdcAggKind::SUM_INT64:
			case TdcAggKind::SUM_NUMERIC:
			case TdcAggKind::COUNT_STAR:
			case TdcAggKind::COUNT_NONNULL:
			case TdcAggKind::COUNT_DISTINCT_NONNULL:
				AddInt64At(delta.row_ptr, agg.offset, delta.values[a]);
				break;
			case TdcAggKind::AVG_NUMERIC:
				AddInt64At(delta.row_ptr, agg.offset, delta.values[a]);
				AddInt64At(delta.row_ptr, agg.offset + 8, delta.counts[a]);
				break;
			case TdcAggKind::MIN_INT64:
			case TdcAggKind::MIN_NUMERIC:
				if (delta.counts[a] != 0)
				{
					int64_t current = ReadInt64At(delta.row_ptr, agg.offset);
					if (current == INT64_MAX || delta.values[a] < current)
						std::memcpy(delta.row_ptr + agg.offset, &delta.values[a], sizeof(int64_t));
				}
				break;
			case TdcAggKind::MAX_INT64:
			case TdcAggKind::MAX_NUMERIC:
				if (delta.counts[a] != 0)
				{
					int64_t current = ReadInt64At(delta.row_ptr, agg.offset);
					if (current == INT64_MIN || delta.values[a] > current)
						std::memcpy(delta.row_ptr + agg.offset, &delta.values[a], sizeof(int64_t));
				}
				break;
		}
	}
}

static void
UpdateAggregatesBatchGroupedGeneric(const TupleDataLayout *layout,
                                    uint8_t **row_ptrs,
                                    const PipelineChunk &chunk,
                                    const uint16_t *row_indices,
                                    uint16_t count)
{
	AggDelta deltas[PIPELINE_DEFAULT_CHUNK_SIZE];
	uint16_t delta_slots[AGG_DELTA_HASH_SLOTS];
	uint16_t delta_count = 0;
	std::fill_n(delta_slots, AGG_DELTA_HASH_SLOTS, AGG_DELTA_EMPTY_SLOT);

	for (uint16_t i = 0; i < count; ++i)
	{
		Assert(row_ptrs[i] != nullptr);
		Assert(row_indices[i] < chunk.count);
		const uintptr_t row_key = reinterpret_cast<uintptr_t>(row_ptrs[i]) >> 3;
		uint16_t delta_idx = AGG_DELTA_EMPTY_SLOT;
		uint16_t slot = static_cast<uint16_t>(row_key & (AGG_DELTA_HASH_SLOTS - 1));
		for (;;)
		{
			const uint16_t existing = delta_slots[slot];
			if (existing == AGG_DELTA_EMPTY_SLOT)
			{
				delta_idx = delta_count;
				InitAggDelta(deltas[delta_idx], row_ptrs[i], layout->aggregate_count);
				delta_slots[slot] = delta_idx;
				++delta_count;
				break;
			}
			if (deltas[existing].row_ptr == row_ptrs[i])
			{
				delta_idx = existing;
				break;
			}
			slot = static_cast<uint16_t>((slot + 1) & (AGG_DELTA_HASH_SLOTS - 1));
		}
		AccumulateAggDelta(layout, deltas[delta_idx], chunk, row_indices[i]);
	}

	for (uint16_t i = 0; i < delta_count; ++i)
		ApplyAggDelta(layout, deltas[i]);
}

static void
UpdateAggregatesGatherGroupedGeneric(const TupleDataLayout *layout,
                                     uint8_t *tdc_base,
                                     uint32_t row_width,
                                     const uint32_t *canonical_row_indices,
                                     const PipelineChunk &chunk,
                                     const uint16_t *row_indices,
                                     uint16_t count)
{
	AggDelta deltas[PIPELINE_DEFAULT_CHUNK_SIZE];
	uint32_t delta_row_indices[PIPELINE_DEFAULT_CHUNK_SIZE];
	uint16_t delta_slots[AGG_DELTA_HASH_SLOTS];
	uint16_t delta_count = 0;
	std::fill_n(delta_slots, AGG_DELTA_HASH_SLOTS, AGG_DELTA_EMPTY_SLOT);

	for (uint16_t i = 0; i < count; ++i)
	{
		Assert(row_indices[i] < chunk.count);
		const uint32_t canonical_idx = canonical_row_indices[i];
		uint16_t delta_idx = AGG_DELTA_EMPTY_SLOT;
		uint16_t slot = static_cast<uint16_t>(canonical_idx & (AGG_DELTA_HASH_SLOTS - 1));
		for (;;)
		{
			const uint16_t existing = delta_slots[slot];
			if (existing == AGG_DELTA_EMPTY_SLOT)
			{
				delta_idx = delta_count;
				delta_row_indices[delta_idx] = canonical_idx;
				InitAggDelta(deltas[delta_idx],
				             tdc_base + static_cast<size_t>(canonical_idx) * row_width,
				             layout->aggregate_count);
				delta_slots[slot] = delta_idx;
				++delta_count;
				break;
			}
			if (delta_row_indices[existing] == canonical_idx)
			{
				delta_idx = existing;
				break;
			}
			slot = static_cast<uint16_t>((slot + 1) & (AGG_DELTA_HASH_SLOTS - 1));
		}
		AccumulateAggDelta(layout, deltas[delta_idx], chunk, row_indices[i]);
	}

	for (uint16_t i = 0; i < delta_count; ++i)
		ApplyAggDelta(layout, deltas[i]);
}

}  /* namespace */

static inline void
ScatterGroupColumns(const TupleDataLayout *layout,
                    TupleDataCollection *tdc,
                    uint8_t *row_ptr,
                    const PipelineChunk &chunk,
                    uint16_t row_idx)
{
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		Assert(col.src_col_idx < 16);

			switch (col.kind)
			{
				case TdcColumnKind::INT32:
			{
				int32_t v = chunk.get_int32(col.src_col_idx, row_idx);
				std::memcpy(row_ptr + col.offset, &v, sizeof(v));
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t v = chunk.get_int64(col.src_col_idx, row_idx);
				std::memcpy(row_ptr + col.offset, &v, sizeof(v));
				break;
			}
				case TdcColumnKind::DOUBLE:
				{
					double v = chunk.get_double(col.src_col_idx, row_idx);
					std::memcpy(row_ptr + col.offset, &v, sizeof(v));
					break;
				}
				case TdcColumnKind::STRING_REF:
				{
					const VecStringRef src = chunk.get_string_ref(col.src_col_idx, row_idx);
					const char *ptr = chunk.get_string_ptr(col.src_col_idx, row_idx);
					VecStringRef dst;
					if ((ptr == nullptr && src.len != 0) || tdc == nullptr ||
						!TupleDataCollectionStoreStringBytes(tdc, ptr, src.len, &dst))
						elog(ERROR,
							"pg_yaap: STRING_REF row-store scatter failed (slot=%u row=%u len=%u offset=%u inline=%d arena=%zu heap_used=%u heap_capacity=%u)",
							static_cast<unsigned>(col.src_col_idx),
							static_cast<unsigned>(row_idx),
							src.len,
							src.offset,
							VecStringRefIsInline(src) ? 1 : 0,
							chunk.string_arena.size(),
							tdc != nullptr ? pg_atomic_read_u32(&tdc->heap_used) : 0,
							tdc != nullptr ? tdc->heap_capacity : 0);
					std::memcpy(row_ptr + col.offset, &dst, sizeof(dst));
					break;
				}
			}
	}
}

void
ScatterGroupOnly(const TupleDataLayout *layout,
                 TupleDataCollection *tdc,
                 uint8_t *row_ptr,
                 const PipelineChunk &chunk,
                 uint16_t row_idx)
{
	Assert(layout != nullptr && row_ptr != nullptr);
	Assert(row_idx < chunk.count);
	ScatterGroupColumns(layout, tdc, row_ptr, chunk, row_idx);
	for (uint16_t a = 0; a < layout->aggregate_count; ++a)
	{
		const TdcAggregateDesc &agg = layout->aggregates[a];
		if (agg.kind == TdcAggKind::MIN_INT64 || agg.kind == TdcAggKind::MIN_NUMERIC)
		{
			const int64_t init_min = INT64_MAX;
			std::memcpy(row_ptr + agg.offset, &init_min, sizeof(int64_t));
		}
		else if (agg.kind == TdcAggKind::MAX_INT64 || agg.kind == TdcAggKind::MAX_NUMERIC)
		{
			const int64_t init_max = INT64_MIN;
			std::memcpy(row_ptr + agg.offset, &init_max, sizeof(int64_t));
		}
	}
}

void
Scatter(const TupleDataLayout *layout,
        TupleDataCollection *tdc,
        uint8_t *row_ptr,
        const PipelineChunk &chunk,
        uint16_t row_idx)
{
	Assert(layout != nullptr && row_ptr != nullptr);
	Assert(row_idx < chunk.count);

	ScatterGroupColumns(layout, tdc, row_ptr, chunk, row_idx);

	const uint16_t agg_chunk_base = layout->column_count;
	for (uint16_t a = 0; a < layout->aggregate_count; ++a)
	{
		const TdcAggregateDesc &agg = layout->aggregates[a];
		int64_t v = chunk.get_int64(agg_chunk_base + a, row_idx);
		std::memcpy(row_ptr + agg.offset, &v, sizeof(v));
		if (agg.kind == TdcAggKind::AVG_NUMERIC)
		{
			const int64_t one = 1;
			std::memcpy(row_ptr + agg.offset + 8, &one, sizeof(one));
		}
	}
}

void
Gather(const TupleDataLayout *layout,
       const TupleDataCollection *tdc,
       const uint8_t *row_ptr,
       PipelineChunk &chunk,
       uint16_t row_idx)
{
	Assert(layout != nullptr && row_ptr != nullptr);
	Assert(layout->column_count + layout->aggregate_count <= 16);

	/* Group/payload columns: chunk col i = row col i. */
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		Assert(col.src_col_idx < 16);

			switch (col.kind)
			{
				case TdcColumnKind::INT32:
			{
				int32_t v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				chunk.int32_columns[col.src_col_idx][row_idx] = v;
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				chunk.int64_columns[col.src_col_idx][row_idx] = v;
				break;
			}
				case TdcColumnKind::DOUBLE:
				{
					double v;
					std::memcpy(&v, row_ptr + col.offset, sizeof(v));
					chunk.double_columns[col.src_col_idx][row_idx] = v;
					break;
				}
				case TdcColumnKind::STRING_REF:
				{
					VecStringRef v;
					std::memcpy(&v, row_ptr + col.offset, sizeof(v));
					const char *ptr = VecStringRefDataPtr(v,
						tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc)) : nullptr);
					if (ptr == nullptr && v.len != 0)
						elog(ERROR, "pg_yaap: STRING_REF gather missing backing storage");
					chunk.string_columns[col.src_col_idx][row_idx] =
						chunk.store_string_bytes(ptr, v.len);
					break;
				}
			}
	}

	/* Aggregate state columns: chunk col N+a = aggregate a (always int64). */
	const uint16_t agg_chunk_base = layout->column_count;
	for (uint16_t a = 0; a < layout->aggregate_count; ++a)
	{
		const TdcAggregateDesc &agg = layout->aggregates[a];
		int64_t v;
		std::memcpy(&v, row_ptr + agg.offset, sizeof(v));

		/* AVG_NUMERIC stores {sum:int64, count:int64} as a 16B pair; the
		 * downstream chunk only has one slot per aggregate, and Scatter only
		 * round-trips the first 8B. Finalize the average here so the count
		 * half is consumed at the producer (HashAgg) where it is still
		 * authoritative, instead of trying to thread a second chunk slot
		 * through Order/Output. AVG descriptors publish an output scale with
		 * extra fractional digits; convert the stored sum to that output scale
		 * here before dividing. Div-by-zero becomes 0 to match SQL's "no rows
		 * produces NULL" only at the EncodeColumn boundary - here we still
		 * emit 0, and OutputSink turns the NULL signal off via tts_isnull
		 * (currently hardcoded false; if we later add NULL tracking the
		 * count==0 case is the place to flip it). */
		if (agg.kind == TdcAggKind::AVG_NUMERIC)
		{
			int64_t count;
			std::memcpy(&count, row_ptr + agg.offset + 8, sizeof(count));
			if (count != 0)
			{
				NumericWideInt widened =
					(WideIntFromInt64(v) * WideIntFromInt64(kAvgNumericScaleFactor)) /
					WideIntFromInt64(count);
				if (!WideIntFitsInt64(widened))
					elog(ERROR,
						 "pg_yaap: finalized AVG numeric exceeds int64 range agg=%u sum=%lld count=%lld scale=%d",
						 static_cast<unsigned>(a),
						 static_cast<long long>(v),
						 static_cast<long long>(count),
						 static_cast<int>(agg.numeric_scale));
				v = WideIntToInt64Checked(widened, "pg_yaap: finalized AVG numeric");
			}
			else
				v = 0;
		}

		chunk.int64_columns[agg_chunk_base + a][row_idx] = v;
	}
}

uint64_t
HashGroup(const TupleDataLayout *layout,
          const PipelineChunk &chunk,
          uint16_t row_idx)
{
	Assert(layout != nullptr);
	Assert(row_idx < chunk.count);

	if (layout->column_count == 1)
	{
		const TdcColumnDesc &col = layout->columns[0];
		Assert(col.src_col_idx < 16);
		switch (col.kind)
		{
			case TdcColumnKind::INT32:
				return HashSingleGroupInt32Value(chunk.get_int32(col.src_col_idx, row_idx));
			case TdcColumnKind::INT64:
				return HashSingleGroupInt64Value(chunk.get_int64(col.src_col_idx, row_idx));
			default:
				break;
		}
	}

	uint64_t h = HashStart();

	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		Assert(col.src_col_idx < 16);

		uint64_t value = 0;
			switch (col.kind)
			{
				case TdcColumnKind::INT32:
			{
				value = static_cast<uint32_t>(chunk.get_int32(col.src_col_idx, row_idx));
				break;
			}
			case TdcColumnKind::INT64:
			{
				value = static_cast<uint64_t>(chunk.get_int64(col.src_col_idx, row_idx));
				break;
			}
			case TdcColumnKind::DOUBLE:
			{
				double v = chunk.get_double(col.src_col_idx, row_idx);
				std::memcpy(&value, &v, sizeof(value));
				break;
			}
				case TdcColumnKind::STRING_REF:
				{
					const VecStringRef ref = chunk.get_string_ref(col.src_col_idx, row_idx);
					const char *ptr = chunk.get_string_ptr(col.src_col_idx, row_idx);
					if (ptr == nullptr && ref.len != 0)
						elog(ERROR, "pg_yaap: STRING_REF chunk hashing missing arena backing");
					value = Mix64(ref.prefix ^ static_cast<uint64_t>(ref.len));
					for (uint32_t byte_idx = 0; byte_idx < ref.len; ++byte_idx)
						value = HashCombine(value,
							static_cast<unsigned char>(ptr[byte_idx]),
							static_cast<uint16_t>(i + byte_idx));
					break;
				}
		}
		h = HashCombine(h, value, i);
	}
	return Mix64(h ^ static_cast<uint64_t>(layout->column_count));
}

uint64_t
HashSingleGroupInt32Value(int32_t value)
{
	uint64_t h = HashStart();
	h = HashCombine(h, static_cast<uint32_t>(value), 0);
	return Mix64(h ^ 1u);
}

uint64_t
HashSingleGroupInt64Value(int64_t value)
{
	uint64_t h = HashStart();
	h = HashCombine(h, static_cast<uint64_t>(value), 0);
	return Mix64(h ^ 1u);
}

uint64_t
HashGroupRow(const TupleDataLayout *layout,
             const TupleDataCollection *tdc,
             const uint8_t *row_ptr)
{
	Assert(layout != nullptr && row_ptr != nullptr);

	if (layout->column_count == 1)
	{
		const TdcColumnDesc &col = layout->columns[0];
		switch (col.kind)
		{
			case TdcColumnKind::INT32:
			{
				int32_t v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				return HashSingleGroupInt32Value(v);
			}
			case TdcColumnKind::INT64:
			{
				int64_t v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				return HashSingleGroupInt64Value(v);
			}
			default:
				break;
		}
	}

	uint64_t h = HashStart();
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		Assert(i < 16);

		uint64_t value = 0;
			switch (col.kind)
			{
				case TdcColumnKind::INT32:
			{
				int32_t v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				value = static_cast<uint32_t>(v);
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				value = static_cast<uint64_t>(v);
				break;
			}
				case TdcColumnKind::DOUBLE:
					std::memcpy(&value, row_ptr + col.offset, sizeof(value));
					break;
				case TdcColumnKind::STRING_REF:
				{
					VecStringRef ref;
					std::memcpy(&ref, row_ptr + col.offset, sizeof(ref));
					const char *ptr = VecStringRefDataPtr(ref,
						tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc)) : nullptr);
					if (ptr == nullptr && ref.len != 0)
						elog(ERROR, "pg_yaap: STRING_REF row-store hashing missing heap backing");
					value = Mix64(ref.prefix ^ static_cast<uint64_t>(ref.len));
					for (uint32_t byte_idx = 0; byte_idx < ref.len; ++byte_idx)
						value = HashCombine(value, static_cast<unsigned char>(ptr[byte_idx]), static_cast<uint16_t>(i + byte_idx));
					break;
				}
			}
		h = HashCombine(h, value, i);
	}
	return Mix64(h ^ static_cast<uint64_t>(layout->column_count));
}

bool
MatchGroup(const TupleDataLayout *layout,
           const TupleDataCollection *tdc,
           const uint8_t *row_ptr,
           const PipelineChunk &chunk,
           uint16_t row_idx)
{
	Assert(layout != nullptr && row_ptr != nullptr);
	Assert(row_idx < chunk.count);

	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		Assert(col.src_col_idx < 16);

			switch (col.kind)
			{
				case TdcColumnKind::INT32:
			{
				int32_t row_v;
				std::memcpy(&row_v, row_ptr + col.offset, sizeof(row_v));
				if (row_v != chunk.get_int32(col.src_col_idx, row_idx))
					return false;
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t row_v;
				std::memcpy(&row_v, row_ptr + col.offset, sizeof(row_v));
				if (row_v != chunk.get_int64(col.src_col_idx, row_idx))
					return false;
				break;
			}
				case TdcColumnKind::DOUBLE:
				{
					uint64_t row_v;
					uint64_t chunk_v;
					std::memcpy(&row_v, row_ptr + col.offset, sizeof(row_v));
					double chunk_value = chunk.get_double(col.src_col_idx, row_idx);
					std::memcpy(&chunk_v, &chunk_value, sizeof(chunk_v));
					if (row_v != chunk_v)
						return false;
				break;
			}
				case TdcColumnKind::STRING_REF:
				{
					VecStringRef row_ref = *reinterpret_cast<const VecStringRef *>(row_ptr + col.offset);
					const VecStringRef chunk_ref = chunk.get_string_ref(col.src_col_idx, row_idx);
					if (row_ref.len != chunk_ref.len || row_ref.prefix != chunk_ref.prefix)
						return false;
					const char *row_ptr_data = VecStringRefDataPtr(row_ref,
						tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc)) : nullptr);
					const char *chunk_ptr_data = chunk.get_string_ptr(col.src_col_idx, row_idx);
					if (row_ref.len != 0 && (row_ptr_data == nullptr || chunk_ptr_data == nullptr))
						elog(ERROR, "pg_yaap: STRING_REF row-store match missing backing storage");
					if (row_ref.len > 8 && std::memcmp(row_ptr_data, chunk_ptr_data, row_ref.len) != 0)
						return false;
					break;
				}
			}
	}
	return true;
}

bool
MatchGroupLayouts(const TupleDataLayout *row_layout,
                  const TupleDataCollection *tdc,
                  const uint8_t *row_ptr,
                  const TupleDataLayout *chunk_layout,
                  const PipelineChunk &chunk,
                  uint16_t row_idx)
{
	Assert(row_layout != nullptr && chunk_layout != nullptr && row_ptr != nullptr);
	Assert(row_idx < chunk.count);
	if (row_layout->column_count != chunk_layout->column_count)
		return false;

	if (!LayoutHasStringColumns(row_layout) && row_layout->column_count == 1)
	{
		const TdcColumnDesc &row_col = row_layout->columns[0];
		const TdcColumnDesc &chunk_col = chunk_layout->columns[0];
		if (row_col.kind != chunk_col.kind || row_col.width != chunk_col.width)
			return false;
		Assert(chunk_col.src_col_idx < 16);
			switch (row_col.kind)
			{
				case TdcColumnKind::INT32:
			{
				int32_t row_v;
				std::memcpy(&row_v, row_ptr + row_col.offset, sizeof(row_v));
				return row_v == chunk.get_int32(chunk_col.src_col_idx, row_idx);
			}
			case TdcColumnKind::INT64:
			{
				int64_t row_v;
				std::memcpy(&row_v, row_ptr + row_col.offset, sizeof(row_v));
				return row_v == chunk.get_int64(chunk_col.src_col_idx, row_idx);
			}
			case TdcColumnKind::DOUBLE:
			{
				uint64_t row_v;
				uint64_t chunk_v;
				std::memcpy(&row_v, row_ptr + row_col.offset, sizeof(row_v));
				double chunk_value = chunk.get_double(chunk_col.src_col_idx, row_idx);
				std::memcpy(&chunk_v, &chunk_value, sizeof(chunk_v));
				return row_v == chunk_v;
			}
			case TdcColumnKind::STRING_REF:
				break;
		}
	}

	if (!LayoutHasStringColumns(row_layout) && row_layout->column_count == 2 &&
		row_layout->columns[0].kind == TdcColumnKind::INT32 &&
		row_layout->columns[1].kind == TdcColumnKind::INT32 &&
		chunk_layout->columns[0].kind == TdcColumnKind::INT32 &&
		chunk_layout->columns[1].kind == TdcColumnKind::INT32)
	{
		const TdcColumnDesc &row_col0 = row_layout->columns[0];
		const TdcColumnDesc &row_col1 = row_layout->columns[1];
		const TdcColumnDesc &chunk_col0 = chunk_layout->columns[0];
		const TdcColumnDesc &chunk_col1 = chunk_layout->columns[1];
		int32_t row_v0;
		int32_t row_v1;
		std::memcpy(&row_v0, row_ptr + row_col0.offset, sizeof(row_v0));
		std::memcpy(&row_v1, row_ptr + row_col1.offset, sizeof(row_v1));
		return row_v0 == chunk.get_int32(chunk_col0.src_col_idx, row_idx) &&
			row_v1 == chunk.get_int32(chunk_col1.src_col_idx, row_idx);
	}

	for (uint16_t i = 0; i < row_layout->column_count; ++i)
	{
		const TdcColumnDesc &row_col = row_layout->columns[i];
		const TdcColumnDesc &chunk_col = chunk_layout->columns[i];
		if (row_col.kind != chunk_col.kind || row_col.width != chunk_col.width)
			return false;
		Assert(chunk_col.src_col_idx < 16);

		switch (row_col.kind)
		{
			case TdcColumnKind::INT32:
			{
				int32_t row_v;
				std::memcpy(&row_v, row_ptr + row_col.offset, sizeof(row_v));
				if (row_v != chunk.get_int32(chunk_col.src_col_idx, row_idx))
					return false;
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t row_v;
				std::memcpy(&row_v, row_ptr + row_col.offset, sizeof(row_v));
				if (row_v != chunk.get_int64(chunk_col.src_col_idx, row_idx))
					return false;
				break;
			}
			case TdcColumnKind::DOUBLE:
			{
				uint64_t row_v;
				uint64_t chunk_v;
				std::memcpy(&row_v, row_ptr + row_col.offset, sizeof(row_v));
				double chunk_value = chunk.get_double(chunk_col.src_col_idx, row_idx);
				std::memcpy(&chunk_v, &chunk_value, sizeof(chunk_v));
				if (row_v != chunk_v)
					return false;
				break;
			}
			case TdcColumnKind::STRING_REF:
			{
				VecStringRef row_ref;
				std::memcpy(&row_ref, row_ptr + row_col.offset, sizeof(row_ref));
				const VecStringRef chunk_ref = chunk.get_string_ref(chunk_col.src_col_idx, row_idx);
				if (row_ref.len != chunk_ref.len || row_ref.prefix != chunk_ref.prefix)
					return false;
				const char *row_ptr_data = VecStringRefDataPtr(row_ref,
					tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc)) : nullptr);
				const char *chunk_ptr_data = chunk.get_string_ptr(chunk_col.src_col_idx, row_idx);
				if (row_ref.len != 0 && (row_ptr_data == nullptr || chunk_ptr_data == nullptr))
					elog(ERROR, "pg_yaap: STRING_REF cross-layout match missing backing storage");
				if (row_ref.len > 8 && std::memcmp(row_ptr_data, chunk_ptr_data, row_ref.len) != 0)
					return false;
				break;
			}
		}
	}
	return true;
}

void
MatchGroupBatch(const TupleDataLayout *layout,
                const TupleDataCollection *tdc,
                const uint8_t *const *row_ptrs,
                const PipelineChunk &chunk,
                const uint16_t *row_indices,
                uint16_t count,
                bool *matches)
{
	Assert(layout != nullptr && row_ptrs != nullptr && row_indices != nullptr && matches != nullptr);
	Assert(count <= PIPELINE_DEFAULT_CHUNK_SIZE);

#if defined(__aarch64__)
	if (!LayoutHasStringColumns(layout) && layout->column_count == 1)
	{
		const TdcColumnDesc &col = layout->columns[0];
		if (col.kind == TdcColumnKind::INT32)
		{
			uint16_t i = 0;
			for (; i + 4 <= count; i += 4)
			{
				uint32_t row_vals[4] = {};
				uint32_t chunk_vals[4] = {};
				uint8_t valid_mask = 0;
				for (uint16_t lane = 0; lane < 4; ++lane)
				{
					const uint16_t idx = i + lane;
					if (row_ptrs[idx] == nullptr)
						continue;
					int32_t row_v;
					std::memcpy(&row_v, row_ptrs[idx] + col.offset, sizeof(row_v));
					row_vals[lane] = static_cast<uint32_t>(row_v);
					chunk_vals[lane] = static_cast<uint32_t>(chunk.get_int32(col.src_col_idx, row_indices[idx]));
					valid_mask |= static_cast<uint8_t>(1u << lane);
				}
				const uint8_t match_mask = NeonLaneMaskEqU32(
					vceqq_u32(vld1q_u32(row_vals), vld1q_u32(chunk_vals))) & valid_mask;
				for (uint16_t lane = 0; lane < 4; ++lane)
					matches[i + lane] = (match_mask & (1u << lane)) != 0;
			}
			for (; i < count; ++i)
				matches[i] = row_ptrs[i] != nullptr && MatchGroup(layout, tdc, row_ptrs[i], chunk, row_indices[i]);
			return;
		}
		if (col.kind == TdcColumnKind::INT64 || col.kind == TdcColumnKind::DOUBLE)
		{
			uint16_t i = 0;
			for (; i + 2 <= count; i += 2)
			{
				uint64_t row_vals[2] = {};
				uint64_t chunk_vals[2] = {};
				uint8_t valid_mask = 0;
				for (uint16_t lane = 0; lane < 2; ++lane)
				{
					const uint16_t idx = i + lane;
					if (row_ptrs[idx] == nullptr)
						continue;
					std::memcpy(&row_vals[lane], row_ptrs[idx] + col.offset, sizeof(uint64_t));
					if (col.kind == TdcColumnKind::INT64)
						chunk_vals[lane] = static_cast<uint64_t>(chunk.get_int64(col.src_col_idx, row_indices[idx]));
					else
					{
						double chunk_value = chunk.get_double(col.src_col_idx, row_indices[idx]);
						std::memcpy(&chunk_vals[lane], &chunk_value, sizeof(uint64_t));
					}
					valid_mask |= static_cast<uint8_t>(1u << lane);
				}
				const uint8_t match_mask = NeonLaneMaskEqU64(
					vceqq_u64(vld1q_u64(row_vals), vld1q_u64(chunk_vals))) & valid_mask;
				for (uint16_t lane = 0; lane < 2; ++lane)
					matches[i + lane] = (match_mask & (1u << lane)) != 0;
			}
			for (; i < count; ++i)
				matches[i] = row_ptrs[i] != nullptr && MatchGroup(layout, tdc, row_ptrs[i], chunk, row_indices[i]);
			return;
		}
	}

	if (!LayoutHasStringColumns(layout) && layout->column_count == 2 &&
		layout->columns[0].kind == TdcColumnKind::INT32 &&
		layout->columns[1].kind == TdcColumnKind::INT32)
	{
		const TdcColumnDesc &col0 = layout->columns[0];
		const TdcColumnDesc &col1 = layout->columns[1];
		uint16_t i = 0;
		for (; i + 4 <= count; i += 4)
		{
			uint32_t row0[4] = {};
			uint32_t row1[4] = {};
			uint32_t chunk0[4] = {};
			uint32_t chunk1[4] = {};
			uint8_t valid_mask = 0;
			for (uint16_t lane = 0; lane < 4; ++lane)
			{
				const uint16_t idx = i + lane;
				if (row_ptrs[idx] == nullptr)
					continue;
				int32_t row_v0;
				int32_t row_v1;
				std::memcpy(&row_v0, row_ptrs[idx] + col0.offset, sizeof(row_v0));
				std::memcpy(&row_v1, row_ptrs[idx] + col1.offset, sizeof(row_v1));
				row0[lane] = static_cast<uint32_t>(row_v0);
				row1[lane] = static_cast<uint32_t>(row_v1);
				chunk0[lane] = static_cast<uint32_t>(chunk.get_int32(col0.src_col_idx, row_indices[idx]));
				chunk1[lane] = static_cast<uint32_t>(chunk.get_int32(col1.src_col_idx, row_indices[idx]));
				valid_mask |= static_cast<uint8_t>(1u << lane);
			}
			const uint32x4_t cmp0 = vceqq_u32(vld1q_u32(row0), vld1q_u32(chunk0));
			const uint32x4_t cmp1 = vceqq_u32(vld1q_u32(row1), vld1q_u32(chunk1));
			const uint8_t match_mask = NeonLaneMaskEqU32(vandq_u32(cmp0, cmp1)) & valid_mask;
			for (uint16_t lane = 0; lane < 4; ++lane)
				matches[i + lane] = (match_mask & (1u << lane)) != 0;
		}
		for (; i < count; ++i)
			matches[i] = row_ptrs[i] != nullptr && MatchGroup(layout, tdc, row_ptrs[i], chunk, row_indices[i]);
		return;
	}
#endif

	for (uint16_t i = 0; i < count; ++i)
		matches[i] = row_ptrs[i] != nullptr && MatchGroup(layout, tdc, row_ptrs[i], chunk, row_indices[i]);
}

bool
MatchGroupRow(const TupleDataLayout *layout,
              const TupleDataCollection *tdc_a,
              const uint8_t *row_a,
              const TupleDataCollection *tdc_b,
              const uint8_t *row_b)
{
	Assert(layout != nullptr && row_a != nullptr && row_b != nullptr);

	if (!LayoutHasStringColumns(layout))
		return std::memcmp(row_a, row_b, LayoutGroupRegionWidth(layout)) == 0;

	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		Assert(i < 16);

		switch (col.kind)
		{
			case TdcColumnKind::STRING_REF:
			{
				VecStringRef ref_a;
				VecStringRef ref_b;
				std::memcpy(&ref_a, row_a + col.offset, sizeof(ref_a));
				std::memcpy(&ref_b, row_b + col.offset, sizeof(ref_b));
				if (ref_a.len != ref_b.len || ref_a.prefix != ref_b.prefix)
					return false;
				const char *ptr_a = VecStringRefDataPtr(ref_a,
					tdc_a != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc_a)) : nullptr);
				const char *ptr_b = VecStringRefDataPtr(ref_b,
					tdc_b != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc_b)) : nullptr);
				if (ref_a.len != 0 && (ptr_a == nullptr || ptr_b == nullptr))
					elog(ERROR, "pg_yaap: STRING_REF row-store match missing heap backing");
				if (ref_a.len > 8 && std::memcmp(ptr_a, ptr_b, ref_a.len) != 0)
					return false;
				break;
			}
			default:
				if (std::memcmp(row_a + col.offset,
				                row_b + col.offset,
				                col.width) != 0)
					return false;
				break;
		}
	}
	return true;
}

void
UpdateAggregates(const TupleDataLayout *layout,
                 uint8_t *row_ptr,
                 const PipelineChunk &chunk,
                 uint16_t row_idx)
{
	Assert(layout != nullptr && row_ptr != nullptr);
	Assert(row_idx < chunk.count);

	for (uint16_t a = 0; a < layout->aggregate_count; ++a)
	{
		const TdcAggregateDesc &agg = layout->aggregates[a];

		switch (agg.kind)
		{
		case TdcAggKind::SUM_INT64:
		case TdcAggKind::SUM_NUMERIC:
			Assert(agg.src_col_idx < 16);
			AddInt64At(row_ptr,
			           agg.offset,
			           chunk.get_int64(agg.src_col_idx, row_idx));
			break;
			case TdcAggKind::COUNT_STAR:
				AddInt64At(row_ptr, agg.offset, 1);
				break;
			case TdcAggKind::COUNT_NONNULL:
			case TdcAggKind::COUNT_DISTINCT_NONNULL:
				Assert(agg.src_col_idx < 16);
				if (chunk.nulls[agg.src_col_idx][row_idx] == 0)
					AddInt64At(row_ptr, agg.offset, 1);
				break;
		case TdcAggKind::AVG_NUMERIC:
			Assert(agg.src_col_idx < 16);
			AddInt64At(row_ptr,
			           agg.offset,
			           chunk.get_int64(agg.src_col_idx, row_idx));
			AddInt64At(row_ptr, agg.offset + 8, 1);
			break;
		case TdcAggKind::MIN_INT64:
		case TdcAggKind::MIN_NUMERIC:
		{
			Assert(agg.src_col_idx < 16);
			const int64_t value = chunk.get_int64(agg.src_col_idx, row_idx);
			const int64_t current = ReadInt64At(row_ptr, agg.offset);
			if (current == INT64_MAX || value < current)
				std::memcpy(row_ptr + agg.offset, &value, sizeof(int64_t));
			break;
		}
		case TdcAggKind::MAX_INT64:
		case TdcAggKind::MAX_NUMERIC:
		{
			Assert(agg.src_col_idx < 16);
			const int64_t value = chunk.get_int64(agg.src_col_idx, row_idx);
			const int64_t current = ReadInt64At(row_ptr, agg.offset);
			if (current == INT64_MIN || value > current)
				std::memcpy(row_ptr + agg.offset, &value, sizeof(int64_t));
			break;
		}
	}
}
}

void
UpdateAggregatesBatch(const TupleDataLayout *layout,
                      uint8_t **row_ptrs,
                      const PipelineChunk &chunk,
                      const uint16_t *row_indices,
                      uint16_t count)
{
	Assert(layout != nullptr && row_ptrs != nullptr);
	Assert(row_indices != nullptr);
	Assert(count <= chunk.count);

	UpdateAggregatesBatchGroupedGeneric(layout, row_ptrs, chunk, row_indices, count);
}

void
UpdateAggregatesGather(const TupleDataLayout *layout,
                       uint8_t *tdc_base,
                       uint32_t row_width,
                       const uint32_t *canonical_row_indices,
                       const PipelineChunk &chunk,
                       const uint16_t *row_indices,
                       uint16_t count)
{
	Assert(layout != nullptr && tdc_base != nullptr);
	Assert(canonical_row_indices != nullptr && row_indices != nullptr);
	Assert(row_width == layout->row_width);
	Assert(count <= PIPELINE_DEFAULT_CHUNK_SIZE);
	UpdateAggregatesGatherGroupedGeneric(layout,
		tdc_base,
		row_width,
		canonical_row_indices,
		chunk,
		row_indices,
		count);
}

void
CombineAggregates(const TupleDataLayout *layout,
                  uint8_t *dst_row,
                  const uint8_t *src_row)
{
	Assert(layout != nullptr && dst_row != nullptr && src_row != nullptr);

	for (uint16_t a = 0; a < layout->aggregate_count; ++a)
	{
		const TdcAggregateDesc &agg = layout->aggregates[a];
		switch (agg.kind)
		{
			case TdcAggKind::SUM_INT64:
			case TdcAggKind::COUNT_STAR:
			case TdcAggKind::COUNT_NONNULL:
			case TdcAggKind::COUNT_DISTINCT_NONNULL:
			case TdcAggKind::SUM_NUMERIC:
				AddInt64At(dst_row, agg.offset, ReadInt64At(src_row, agg.offset));
				break;
			case TdcAggKind::AVG_NUMERIC:
				AddInt64At(dst_row, agg.offset, ReadInt64At(src_row, agg.offset));
				AddInt64At(dst_row, agg.offset + 8, ReadInt64At(src_row, agg.offset + 8));
				break;
			case TdcAggKind::MIN_INT64:
			case TdcAggKind::MIN_NUMERIC:
			{
				const int64_t src = ReadInt64At(src_row, agg.offset);
				const int64_t dst = ReadInt64At(dst_row, agg.offset);
				if (dst == INT64_MAX || src < dst)
					std::memcpy(dst_row + agg.offset, &src, sizeof(int64_t));
				break;
			}
			case TdcAggKind::MAX_INT64:
			case TdcAggKind::MAX_NUMERIC:
			{
				const int64_t src = ReadInt64At(src_row, agg.offset);
				const int64_t dst = ReadInt64At(dst_row, agg.offset);
				if (dst == INT64_MIN || src > dst)
					std::memcpy(dst_row + agg.offset, &src, sizeof(int64_t));
				break;
			}
		}
	}

	/* Suppress -Wunused-but-set-variable if asserts compiled out. */
	(void) layout;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
