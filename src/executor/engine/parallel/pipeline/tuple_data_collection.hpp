#pragma once

/*
 * pipeline/tuple_data_collection.hpp
 *
 * 3g.2-final — DSA-resident, append-only row buffer used by HashAggregate
 * (sink) and Order (sink). Row format is described by a separately-allocated
 * TupleDataLayout (see tuple_data_layout.hpp); TDC itself is layout-agnostic
 * and just hands out byte ranges.
 *
 * v1 scope (per design §3.3):
 *   - Single segment, single chunk: one flat row buffer with a fixed cap
 *     (`row_capacity`) chosen at allocation time; overflow is elog(ERROR).
 *   - Coarse spinlock around the bump allocator (slock_t) — workers contend
 *     directly on the global TDC. Per-thread locals land at Q3+.
 *   - No heap area (fixed-width rows only — see TupleDataLayout v1 scope).
 *   - Atomic scan cursor for cooperative parallel scan in the source phase
 *     of the next pipeline (HashAgg as source).
 *
 * Memory layout (single dsa_allocate0):
 *
 *   | TupleDataCollection header | row 0 | row 1 | ... | row K-1 |
 *
 * `rows` is a flexible array member-style trailing buffer; total alloc =
 * `sizeof(TupleDataCollection) + row_capacity * row_width`. 3g.2 step 5/6
 * extends the header with `layout_dp` and `finalized` so source-side scans can
 * reconstruct the row codec and observe the sink->source hand-off without any
 * Q1-specific row POD.
 *
 * Spec: .sisyphus/plans/3g2-tuple-data-collection-design.md §3.3, §10 step 3.
 */

extern "C" {
#include "postgres.h"
#include "port/atomics.h"
#include "storage/spin.h"
#include "utils/dsa.h"
}

#include <cstdint>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

struct TupleDataLayout;

/*
 * Sentinel returned by AppendRow when the caller-side allocation policy
 * (bounded row_capacity) is exhausted. v1 treats this as a hard error in
 * the caller (HashAggregate sink elog(ERROR)s); Q3+ will spill.
 */
static constexpr uint32_t TDC_INVALID_ROW_INDEX = UINT32_MAX;

struct TupleDataCollection
{
	/* immutable after Init */
	uint32_t         row_width;       /* matches TupleDataLayout::row_width */
	uint32_t         row_capacity;    /* hard cap; overflow = elog(ERROR) in v1 */
	dsa_pointer      layout_dp;       /* -> TupleDataLayout driving Scatter/Gather */

	/* append-side state: spinlock + count */
	slock_t          mutex;           /* guards the bump allocator */
	pg_atomic_uint32 row_count;       /* current row count; readers use atomic load */
	uint32_t         heap_capacity;    /* trailing heap bytes for STRING_REF payloads */
	pg_atomic_uint32 heap_used;        /* bytes consumed in trailing heap */

	/* scan-side state: cooperative parallel scan cursor */
	pg_atomic_uint32 scan_cursor;     /* next row index to hand out to a scanner */

	bool             finalized;       /* set by sink Finalize before source GetData */
	uint8_t          _pad[3];

	/*
	 * Trailing flat row buffer (row_capacity * row_width bytes). C++ has no
	 * portable FAM syntax; we declare a 1-byte stub and index past it. The
	 * total allocation size is computed by TupleDataCollectionAllocSize().
	 */
	uint8_t          rows[1];
};

/*
 * Allocation size for `dsa_allocate0(... TupleDataCollectionAllocSize(...))`.
 * Caller MUST use dsa_allocate0 (not dsa_allocate) — agg state slots rely on
 * zero init for SUM=0 / COUNT=0.
 */
inline size_t
TupleDataCollectionRowBytes(uint32_t row_capacity, uint32_t row_width)
{
	return static_cast<size_t>(row_capacity) * row_width;
}

inline size_t
TupleDataCollectionAllocSize(uint32_t row_capacity,
	                         uint32_t row_width,
	                         uint32_t heap_capacity = 0)
{
	/* offsetof-style: header up to (but not including) the rows[1] stub +
	 * the actual row buffer. Equivalent to:
	 *   sizeof(TupleDataCollection) - 1 + row_capacity * row_width
	 * but we use offsetof to be explicit. */
	return offsetof(TupleDataCollection, rows) +
	       TupleDataCollectionRowBytes(row_capacity, row_width) +
	       heap_capacity;
}

void TupleDataCollectionCheckFlatAllocSize(uint32_t row_capacity,
	                                         uint32_t row_width,
	                                         uint32_t heap_capacity = 0);

size_t TupleDataCollectionCheckedAllocSize(uint32_t row_capacity,
	                                         uint32_t row_width,
	                                         uint32_t heap_capacity = 0);

dsa_pointer TupleDataCollectionAllocate(dsa_area *dsa,
	                                   uint32_t row_capacity,
	                                   uint32_t row_width,
	                                   uint32_t heap_capacity = 0);

inline uint8_t *
TupleDataCollectionHeap(TupleDataCollection *tdc)
{
	return &tdc->rows[0] + TupleDataCollectionRowBytes(tdc->row_capacity, tdc->row_width);
}

inline const uint8_t *
TupleDataCollectionHeapConst(const TupleDataCollection *tdc)
{
	return &tdc->rows[0] + TupleDataCollectionRowBytes(tdc->row_capacity, tdc->row_width);
}

/*
 * Initialize an already-zeroed TupleDataCollection in place. Caller has
 * dsa_allocate0'd a region of TupleDataCollectionAllocSize() bytes and
 * passes the resulting host pointer here. Workers attaching later
 * dsa_get_address the same dsa_pointer and use the TDC directly without
 * re-init.
 *
 * Contract delta (step 5/6): callers now pass `layout_dp` so the TDC header
 * can publish the row codec to downstream source-side readers.
 */
void TupleDataCollectionInit(TupleDataCollection *tdc,
                             uint32_t row_capacity,
							 uint32_t row_width,
							 dsa_pointer layout_dp,
							 uint32_t heap_capacity = 0);

uint32_t TupleDataCollectionDefaultHeapCapacity(const TupleDataLayout *layout,
	                                            uint32_t row_capacity);

uint32_t TupleDataCollectionGrowHeapCapacity(const TupleDataLayout *layout,
	                                          const TupleDataCollection *old_tdc,
	                                          uint32_t new_row_capacity,
	                                          uint32_t required_heap_bytes);
uint32_t TupleDataCollectionClampRowCapacity(uint32_t proposed_row_capacity,
	                                        uint32_t row_width,
	                                        uint32_t required_heap_bytes,
	                                        uint32_t minimum_row_capacity);

bool TupleDataCollectionStoreStringBytes(TupleDataCollection *tdc,
	                                     const char *data,
	                                     uint32_t len,
	                                     VecStringRef *out_ref);

uint32_t TupleDataCollectionRequiredHeapBytesForChunkRow(const TupleDataLayout *layout,
	                                                      const PipelineChunk &chunk,
	                                                      uint16_t row_idx);

uint32_t TupleDataCollectionRequiredHeapBytesForRow(const TupleDataLayout *layout,
	                                                 const TupleDataCollection *tdc,
	                                                 const uint8_t *row_ptr);

bool TupleDataCollectionHasSpaceForAppend(const TupleDataCollection *tdc,
	                                      uint32_t required_heap_bytes);

/*
 * Append a row. Returns the row index (0-based) and writes the row pointer
 * into *out_row. Caller fills *out_row in-place (Scatter + zero-init agg
 * slots). Returns TDC_INVALID_ROW_INDEX if the buffer is full — v1 callers
 * elog(ERROR) on this.
 *
 * Thread-safe under the spinlock. Returned pointer is stable for the
 * lifetime of the DSA segment (no rehashing / no reallocation in v1).
 */
uint32_t TupleDataCollectionAppendRow(TupleDataCollection *tdc,
                                      uint8_t **out_row);

/*
 * Read access by row index. No bounds check beyond an Assert; caller is
 * expected to bound by `row_count` snapshot. Returned pointer is stable.
 */
inline uint8_t *
TupleDataCollectionGetRow(TupleDataCollection *tdc, uint32_t row_idx)
{
	Assert(row_idx < tdc->row_capacity);
	return &tdc->rows[0] + static_cast<size_t>(row_idx) * tdc->row_width;
}

inline const uint8_t *
TupleDataCollectionGetRowConst(const TupleDataCollection *tdc, uint32_t row_idx)
{
	Assert(row_idx < tdc->row_capacity);
	return &tdc->rows[0] + static_cast<size_t>(row_idx) * tdc->row_width;
}

/*
 * Cooperative parallel scan: atomically claim the next row to read.
 * Returns TDC_INVALID_ROW_INDEX once scan_cursor reaches the snapshot of
 * row_count taken at call time.
 *
 * v1 hands out one row at a time. Q3+ may switch to morsel-style stride
 * for cache friendliness.
 */
uint32_t TupleDataCollectionClaimScanRow(TupleDataCollection *tdc);

/*
 * Reset scan_cursor back to 0. Called by the leader between pipelines if a
 * TDC is scanned more than once (not used by Q1 — kept for §10 step 7
 * Output sink completeness).
 */
void TupleDataCollectionResetScan(TupleDataCollection *tdc);

}  /* namespace pipeline */
}  /* namespace pg_yaap */
