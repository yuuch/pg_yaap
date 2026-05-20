/*
 * pipeline/tuple_data_collection.cpp
 *
 * Implementation of the DSA-resident append-only row buffer. See header
 * for the design contract. Spec: §3.3, §10 step 3 of
 * .sisyphus/plans/3g2-tuple-data-collection-design.md.
 *
 * Concurrency model:
 *   - AppendRow grabs the slock_t, bumps an internal index, releases.
 *     Atomic store on row_count is published AFTER the row is materialized
 *     so concurrent scanners observing row_count see fully-written rows.
 *     (The mutex serializes appenders; the atomic publish is for readers.)
 *   - ClaimScanRow uses fetch_add on scan_cursor and bounds against an
 *     atomic-load snapshot of row_count. No lock on the read path.
 *   - GetRow has no synchronization; caller bounds by row_count snapshot.
 *
 * The slock_t covers the bump (we need atomicity between "decide my index"
 * and "publish row_count >= my_index+1" only insofar as readers need a
 * monotonic row_count; pg_atomic_fetch_add would suffice for the count
 * itself, but the spinlock is cheap at v1 contention levels and gives a
 * single obvious append-side serialization point. Q3+ moves to per-thread
 * local TDCs where this lock disappears entirely.).
 */

#include "tuple_data_collection.hpp"

#include "tuple_data_layout.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <algorithm>
#include <cstring>

namespace pg_yaap {
namespace pipeline {

namespace {

/*
 * Keep the single-allocation TDC safely below the 32-bit string offset space
 * used by VecStringRef while allowing string-heavy 10G queries like Q10 to
 * materialize large group/output buffers without tripping an overly small cap.
 */
static constexpr uint64_t kTupleDataCollectionMaxFlatAllocBytes = 3ull * 1024ull * 1024ull * 1024ull;

static inline uint32_t
HeapBytesNeededForStringLength(uint32_t len)
{
	return len > 8 ? len : 0;
}

}  /* namespace */

void
TupleDataCollectionInit(TupleDataCollection *tdc,
                        uint32_t row_capacity,
						uint32_t row_width,
						dsa_pointer layout_dp,
						uint32_t heap_capacity)
{
	Assert(tdc != nullptr);
	Assert(row_capacity > 0);
	Assert(row_width > 0);
	Assert(row_width % 8 == 0);  /* TupleDataLayoutSeal post-condition */

	tdc->row_width    = row_width;
	tdc->row_capacity = row_capacity;
	tdc->layout_dp    = layout_dp;
	SpinLockInit(&tdc->mutex);
	pg_atomic_init_u32(&tdc->row_count, 0);
	pg_atomic_init_u32(&tdc->scan_cursor, 0);
	tdc->heap_capacity = heap_capacity;
	pg_atomic_init_u32(&tdc->heap_used, 0);
	tdc->finalized = false;
	tdc->_pad[0] = 0;
	tdc->_pad[1] = 0;
	tdc->_pad[2] = 0;
	/* tdc->rows is already zero from dsa_allocate0; do not memset
	 * row_capacity*row_width bytes here (could be many MB). */
}

uint32_t
TupleDataCollectionDefaultHeapCapacity(const TupleDataLayout *layout,
	                                   uint32_t row_capacity)
{
	if (layout == nullptr)
		return 0;

	uint32_t string_columns = 0;
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		if (layout->columns[i].kind == TdcColumnKind::STRING_REF)
			string_columns++;
	}
	if (string_columns == 0)
		return 0;

	const uint64_t bytes = static_cast<uint64_t>(row_capacity) *
		static_cast<uint64_t>(string_columns) * 96u;
	return bytes > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(bytes);
}

void
TupleDataCollectionCheckFlatAllocSize(uint32_t row_capacity,
	                                    uint32_t row_width,
	                                    uint32_t heap_capacity)
{
	const uint64_t alloc_size = static_cast<uint64_t>(offsetof(TupleDataCollection, rows)) +
		static_cast<uint64_t>(row_capacity) * static_cast<uint64_t>(row_width) +
		static_cast<uint64_t>(heap_capacity);
	if (alloc_size >= kTupleDataCollectionMaxFlatAllocBytes)
		elog(ERROR,
			 "pg_yaap: TDC flat allocation exceeds limit (rows=%u row_width=%u heap=%u size=%llu limit=%llu)",
			 row_capacity,
			 row_width,
			 heap_capacity,
			 static_cast<unsigned long long>(alloc_size),
			 static_cast<unsigned long long>(kTupleDataCollectionMaxFlatAllocBytes));
}

size_t
TupleDataCollectionCheckedAllocSize(uint32_t row_capacity,
	                                  uint32_t row_width,
	                                  uint32_t heap_capacity)
{
	TupleDataCollectionCheckFlatAllocSize(row_capacity, row_width, heap_capacity);
	return TupleDataCollectionAllocSize(row_capacity, row_width, heap_capacity);
}

dsa_pointer
TupleDataCollectionAllocate(dsa_area *dsa,
	                       uint32_t row_capacity,
	                       uint32_t row_width,
	                       uint32_t heap_capacity)
{
	const size_t alloc_size = TupleDataCollectionCheckedAllocSize(row_capacity,
		row_width,
		heap_capacity);
	int flags = DSA_ALLOC_ZERO;
	if (!AllocSizeIsValid(alloc_size))
		flags |= DSA_ALLOC_HUGE;
	return dsa_allocate_extended(dsa, alloc_size, flags);
}

uint32_t
TupleDataCollectionGrowHeapCapacity(const TupleDataLayout *layout,
	                                  const TupleDataCollection *old_tdc,
	                                  uint32_t new_row_capacity,
	                                  uint32_t required_heap_bytes)
{
	Assert(old_tdc != nullptr);

	if (layout == nullptr || old_tdc->heap_capacity == 0)
	{
		const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(layout, new_row_capacity);
		TupleDataCollectionCheckFlatAllocSize(new_row_capacity, old_tdc->row_width, heap_capacity);
		return heap_capacity;
	}

	const uint32_t heap_used = pg_atomic_read_u32(
		const_cast<pg_atomic_uint32 *>(&old_tdc->heap_used));
	const uint64_t min_required = static_cast<uint64_t>(heap_used) +
		static_cast<uint64_t>(required_heap_bytes);
	uint64_t next_capacity = static_cast<uint64_t>(old_tdc->heap_capacity) +
		std::max<uint64_t>(static_cast<uint64_t>(old_tdc->heap_capacity) / 2u, 1024u * 1024u);
	if (next_capacity < min_required)
		next_capacity = min_required;

	const uint64_t row_bytes = TupleDataCollectionRowBytes(new_row_capacity,
		old_tdc->row_width);
	if (row_bytes + offsetof(TupleDataCollection, rows) >= kTupleDataCollectionMaxFlatAllocBytes)
		elog(ERROR, "pg_yaap: TDC row buffer exceeds flat allocation limit");
	const uint64_t max_heap_capacity = kTupleDataCollectionMaxFlatAllocBytes -
		row_bytes - offsetof(TupleDataCollection, rows);
	if (min_required > max_heap_capacity)
		elog(ERROR, "pg_yaap: TDC string heap exceeds flat allocation limit");
	if (next_capacity > max_heap_capacity)
		next_capacity = max_heap_capacity;
	return next_capacity > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(next_capacity);
}

uint32_t
TupleDataCollectionClampRowCapacity(uint32_t proposed_row_capacity,
	                                uint32_t row_width,
	                                uint32_t required_heap_bytes,
	                                uint32_t minimum_row_capacity)
{
	if (row_width == 0)
		elog(ERROR, "pg_yaap: TDC clamp received zero row width");
	if (required_heap_bytes >= kTupleDataCollectionMaxFlatAllocBytes)
		elog(ERROR, "pg_yaap: TDC required heap exceeds flat allocation limit");
	const uint64_t max_row_bytes = kTupleDataCollectionMaxFlatAllocBytes -
		offsetof(TupleDataCollection, rows) -
		static_cast<uint64_t>(required_heap_bytes);
	const uint64_t max_rows = max_row_bytes / static_cast<uint64_t>(row_width);
	if (max_rows < minimum_row_capacity)
		elog(ERROR,
			 "pg_yaap: TDC row buffer exceeds flat allocation limit (min_rows=%u row_width=%u required_heap=%u max_rows=%llu)",
			 minimum_row_capacity,
			 row_width,
			 required_heap_bytes,
			 static_cast<unsigned long long>(max_rows));
	return proposed_row_capacity > max_rows ? static_cast<uint32_t>(max_rows) : proposed_row_capacity;
}

bool
TupleDataCollectionStoreStringBytes(TupleDataCollection *tdc,
	                                const char *data,
	                                uint32_t len,
	                                VecStringRef *out_ref)
{
	Assert(tdc != nullptr && out_ref != nullptr);

	VecStringRef ref{len, 0, 0};
	if (len == 0 || data == nullptr)
	{
		*out_ref = ref;
		return true;
	}

	std::memcpy(&ref.prefix, data, len > 8 ? 8 : len);
	if (len <= 8)
	{
		ref.offset = kVecStringInlineOffset;
		*out_ref = ref;
		return true;
	}

	uint32_t heap_offset = 0;
	SpinLockAcquire(&tdc->mutex);
	{
		const uint32_t heap_used = pg_atomic_read_u32(&tdc->heap_used);
		if (heap_used > tdc->heap_capacity || len > tdc->heap_capacity - heap_used)
		{
			SpinLockRelease(&tdc->mutex);
			return false;
		}
		heap_offset = heap_used;
		pg_atomic_write_u32(&tdc->heap_used, heap_used + len);
	}
	SpinLockRelease(&tdc->mutex);

	uint8_t *heap = TupleDataCollectionHeap(tdc);
	std::memcpy(heap + heap_offset, data, len);
	ref.offset = heap_offset;
	*out_ref = ref;
	return true;
}

uint32_t
TupleDataCollectionRequiredHeapBytesForChunkRow(const TupleDataLayout *layout,
	                                           const PipelineChunk &chunk,
	                                           uint16_t row_idx)
{
	Assert(layout != nullptr);
	Assert(row_idx < chunk.count);

	uint64_t required = 0;
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		if (col.kind != TdcColumnKind::STRING_REF)
			continue;
		Assert(col.src_col_idx < 16);
		required += HeapBytesNeededForStringLength(
			chunk.get_string_ref(col.src_col_idx, row_idx).len);
	}

	return required > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(required);
}

uint32_t
TupleDataCollectionRequiredHeapBytesForRow(const TupleDataLayout *layout,
	                                      const TupleDataCollection *tdc,
	                                      const uint8_t *row_ptr)
{
	Assert(layout != nullptr);
	Assert(row_ptr != nullptr);

	uint64_t required = 0;
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		if (col.kind != TdcColumnKind::STRING_REF)
			continue;

		VecStringRef ref;
		std::memcpy(&ref, row_ptr + col.offset, sizeof(ref));
		const char *ptr = VecStringRefDataPtr(ref,
			tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc)) : nullptr);
		if (ref.len != 0 && ptr == nullptr)
			elog(ERROR, "pg_yaap: STRING_REF row-store copy missing heap backing");
		required += HeapBytesNeededForStringLength(ref.len);
	}

	return required > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(required);
}

bool
TupleDataCollectionHasSpaceForAppend(const TupleDataCollection *tdc,
	                                uint32_t required_heap_bytes)
{
	Assert(tdc != nullptr);

	const uint32_t row_count = pg_atomic_read_u32(
		const_cast<pg_atomic_uint32 *>(&tdc->row_count));
	if (row_count >= tdc->row_capacity)
		return false;

	if (required_heap_bytes == 0)
		return true;

	const uint32_t heap_used = pg_atomic_read_u32(
		const_cast<pg_atomic_uint32 *>(&tdc->heap_used));
	return heap_used <= tdc->heap_capacity &&
		required_heap_bytes <= tdc->heap_capacity - heap_used;
}

uint32_t
TupleDataCollectionAppendRow(TupleDataCollection *tdc, uint8_t **out_row)
{
	Assert(tdc != nullptr && out_row != nullptr);

	uint32_t my_idx;
	SpinLockAcquire(&tdc->mutex);
	{
		const uint32_t cur = pg_atomic_read_u32(&tdc->row_count);
		if (cur >= tdc->row_capacity)
		{
			SpinLockRelease(&tdc->mutex);
			*out_row = nullptr;
			return TDC_INVALID_ROW_INDEX;
		}
		my_idx = cur;
		/*
		 * Publish row_count BEFORE releasing the lock. Readers that observe
		 * row_count >= my_idx+1 are guaranteed to see a row buffer slot
		 * that the caller is about to write into. The slot itself is
		 * already zeroed by dsa_allocate0, so a reader racing the caller's
		 * Scatter sees zeros (which would be a logic bug — readers MUST
		 * bound by their own snapshot of row_count taken AFTER the
		 * appender finished, not during).
		 *
		 * In practice TDC has a strict sink-then-source phase boundary
		 * (HashAggregate sinks all rows before any source-side scan
		 * starts — enforced by the MetaPipeline DAG), so the race window
		 * is closed at the architectural level. The atomic publish is
		 * defense in depth.
		 */
		pg_atomic_write_u32(&tdc->row_count, my_idx + 1);
	}
	SpinLockRelease(&tdc->mutex);

	*out_row = TupleDataCollectionGetRow(tdc, my_idx);
	return my_idx;
}

uint32_t
TupleDataCollectionClaimScanRow(TupleDataCollection *tdc)
{
	Assert(tdc != nullptr);

	const uint32_t total = pg_atomic_read_u32(&tdc->row_count);
	uint32_t claimed = pg_atomic_fetch_add_u32(&tdc->scan_cursor, 1);
	if (claimed >= total)
	{
		/* Over-claim: pull the cursor back so subsequent claims still see
		 * the same total bound. Without this, scan_cursor can race past
		 * UINT32_MAX in pathological cases. CAS is best-effort: if another
		 * thread also over-claimed and rewound, we leave it. Note CAS
		 * writes back to `claimed` on failure — must be non-const. */
		uint32_t expected = claimed + 1;
		pg_atomic_compare_exchange_u32(&tdc->scan_cursor, &expected, total);
		return TDC_INVALID_ROW_INDEX;
	}
	return claimed;
}

void
TupleDataCollectionResetScan(TupleDataCollection *tdc)
{
	Assert(tdc != nullptr);
	pg_atomic_write_u32(&tdc->scan_cursor, 0);
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
