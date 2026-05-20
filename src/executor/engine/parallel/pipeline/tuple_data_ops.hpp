#pragma once

/*
 * pipeline/tuple_data_ops.hpp
 *
 * 3g.2-final — stateless row<->chunk codec + hash/match/agg-update primitives
 * over TupleDataLayout-described row buffers. No DSA awareness, no allocation,
 * no PhysicalOperator coupling: every function takes a layout + raw row bytes
 * + a PipelineChunk. Reusable by HashAggregate sink (TDC + AHT), Sort sink
 * (TDC), and the Output sink's gather-then-materialize path.
 *
 * Modeled on duckdb::TupleDataCollection::{Scatter,Gather} +
 * GroupedAggregateHashTable::{HashGroup,MatchGroup,Update,Combine}, collapsed
 * for v1 (fixed-width-only, NOT NULL only, no validity bitmap, no heap).
 *
 * Chunk layout convention (set by translator + SeqScan, see
 * .sisyphus/plans/3g2-tuple-data-collection-design.md §6):
 *
 *   Input chunk for HashAggregate sink =
 *     [ group_col_0, ..., group_col_{N-1}, agg_input_0, ..., agg_input_{M-1} ]
 *
 *   - layout->columns[i] (i = 0..N-1) sources from chunk col
 *     `columns[i].src_col_idx`.
 *   - layout->aggregates[j] (j = 0..M-1) sources its update value from
 *     chunk col `aggregates[j].src_col_idx` (typically N+j, but the
	 *     translator may share one input col across multiple aggs, e.g. when
	 *     separate SUM/AVG aggregates read the same projected value).
 *
 * All functions are header-stable across operators. v1 dispatch is on
 * TdcColumnKind / TdcAggKind (no fn-ptrs, DSA-portable).
 *
 * Spec: .sisyphus/plans/3g2-tuple-data-collection-design.md §3.2, §4, §10
 *       step 2.
 */

#include "parallel/pipeline/tuple_data_layout.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/types.hpp"

#include <cstdint>

namespace pg_yaap {
namespace pipeline {

/*
 * Scatter:
 *   Write row[row_idx] of `chunk` into the row buffer at `row_ptr`, using
 *   `layout->columns` AND `layout->aggregates`. Group cols copy from
 *   `chunk.{int32,int64,double}_columns[i]`; aggregate state copies from
 *   `chunk.int64_columns[layout->column_count + a]` (matching Gather's
 *   output convention). For AVG_NUMERIC, the count tail at offset+8 is
 *   stamped to 1 so chained Gather→Scatter→Gather over Order/Output TDCs
 *   stays idempotent (count=1 means downstream Gather's sum/count divide
 *   is a no-op on an already-finalized scaled value).
 *
 * USE FOR: Order.SinkChunk, OutputSink staging — chunks produced by an
 *          upstream Gather that carry both group cols AND agg state.
 *
 * DO NOT USE FOR: HashAgg.SinkChunk — the input chunk carries only source
 *          columns (no agg slots); the upstream chunk's int64 slots in the
 *          aggregate-state index range hold src column data and would
 *          corrupt the freshly-zeroed agg state. Use ScatterGroupOnly.
 *
 * Preconditions:
 *   - row_ptr points to layout->row_width zero-filled bytes.
 *   - chunk.count > row_idx.
 *   - For each col i in 0..layout->column_count-1: columns[i].src_col_idx
 *     in chunk is < 16 (DataChunk fixed cap).
 *   - column_count + aggregate_count <= 16.
 */
void Scatter(const TupleDataLayout *layout,
             TupleDataCollection *tdc,
             uint8_t *row_ptr,
             const PipelineChunk &chunk,
             uint16_t row_idx);

/*
 * ScatterGroupOnly:
 *   Same as Scatter but writes ONLY group columns; aggregate state slots
 *   are left at their dsa_allocate0 zero values for UpdateAggregates to
 *   accumulate into. Used by HashAgg.SinkChunk where the input chunk is
 *   raw source data (qty/extprice/discount/...) and the chunk's int64
 *   slots in the [column_count, column_count + aggregate_count) range
 *   hold src column values, NOT agg state — copying them would seed the
 *   aggregate accumulator with unrelated row data.
 */
void ScatterGroupOnly(const TupleDataLayout *layout,
                      TupleDataCollection *tdc,
                      uint8_t *row_ptr,
                      const PipelineChunk &chunk,
                      uint16_t row_idx);

/*
 * Gather:
 *   Read row at `row_ptr` and write into `chunk` slot `row_idx`. Writes
 *   ALL group columns + ALL aggregate state columns into the chunk in
 *   order: chunk col `c` (c = 0..N-1) gets columns[c]; chunk col `N+a`
 *   (a = 0..M-1) gets aggregates[a].
 *
 * Preconditions:
 *   - chunk has capacity for row_idx.
 *   - layout->column_count + layout->aggregate_count <= 16.
 *   - Caller bumps chunk.count itself (Gather does NOT touch count).
 */
void Gather(const TupleDataLayout *layout,
            const TupleDataCollection *tdc,
            const uint8_t *row_ptr,
            PipelineChunk &chunk,
            uint16_t row_idx);

/*
 * HashGroup:
 *   Typed 64-bit hash over group columns (layout->columns[0..column_count-1])
 *   of chunk[row_idx]. Aggregate state columns are NOT hashed. INT32/INT64
 *   avoid byte-at-a-time hashing; DOUBLE hashes its bit representation.
 */
uint64_t HashGroup(const TupleDataLayout *layout,
                   const PipelineChunk &chunk,
                   uint16_t row_idx);

uint64_t HashSingleGroupInt32Value(int32_t value);
uint64_t HashSingleGroupInt64Value(int64_t value);

/* Row-buffer variant used by HashAgg combine and AHT rehash. Must stay
 * bit-identical to HashGroup for rows produced by ScatterGroupOnly. */
uint64_t HashGroupRow(const TupleDataLayout *layout,
                      const TupleDataCollection *tdc,
                      const uint8_t *row_ptr);

/*
 * MatchGroup:
 *   Returns true iff the group columns of chunk[row_idx] equal the
 *   group columns at row_ptr. Aggregate state columns are NOT compared.
 *   Uses memcmp on the column byte ranges — equivalent to per-kind
 *   compare since v1 layouts are fixed-width and host endianness
 *   matches on Scatter.
 */
bool MatchGroup(const TupleDataLayout *layout,
                const TupleDataCollection *tdc,
                const uint8_t *row_ptr,
                const PipelineChunk &chunk,
                uint16_t row_idx);

/*
 * MatchGroupLayouts:
 *   Row-vs-chunk variant for operators whose build/probe sides may store the
 *   same logical key in different chunk slots. row_layout describes row_ptr;
 *   chunk_layout describes chunk[row_idx]. Column positions are paired by
 *   ordinal, but each side uses its own src_col_idx/offset.
 */
bool MatchGroupLayouts(const TupleDataLayout *row_layout,
                       const TupleDataCollection *tdc,
                       const uint8_t *row_ptr,
                       const TupleDataLayout *chunk_layout,
                       const PipelineChunk &chunk,
                       uint16_t row_idx);

void MatchGroupBatch(const TupleDataLayout *layout,
                     const TupleDataCollection *tdc,
                     const uint8_t *const *row_ptrs,
                     const PipelineChunk &chunk,
                     const uint16_t *row_indices,
                     uint16_t count,
                     bool *matches);

/*
 * MatchGroupRow:
 *   Row-vs-row variant of MatchGroup. Returns true iff the group columns
 *   in row_a equal the group columns in row_b. Aggregate state columns
 *   are NOT compared. Per-column typed compare on col.width bytes (same
 *   semantics as MatchGroup, just both sides are row buffers). Used by
 *   AggregateHashTable probe to compare a candidate just-Scattered row
 *   against an existing canonical row.
 */
bool MatchGroupRow(const TupleDataLayout *layout,
                   const TupleDataCollection *tdc_a,
                   const uint8_t *row_a,
                   const TupleDataCollection *tdc_b,
                   const uint8_t *row_b);

/*
 * UpdateAggregates:
 *   For each aggregate a in layout->aggregates[0..aggregate_count-1]:
 *     - SUM_INT64    : *(int64*)&row[a.offset] += chunk[a.src_col_idx][row_idx]
 *                      (input read as int64; NUMERIC carried as scaled int64
 *                      with caller-managed scale).
 *     - COUNT_STAR   : *(int64*)&row[a.offset] += 1.
 *     - COUNT_NONNULL / COUNT_DISTINCT_NONNULL:
 *                      *(int64*)&row[a.offset] += 1 for non-null input rows.
 *
 * Aggregate state is plain int64 in v1 (no Wide128 in DSA — out of scope
	 * per design §3.1). Overflow is undefined for v1; supported queries are
	 * expected to fit int64 accumulators until Wide128 widening lands at Q3+.
 *
 * Preconditions: chunk.count > row_idx; src_col_idx < 16.
 */
void UpdateAggregates(const TupleDataLayout *layout,
                      uint8_t *row_ptr,
                      const PipelineChunk &chunk,
                      uint16_t row_idx);

void UpdateAggregatesBatch(const TupleDataLayout *layout,
                           uint8_t **row_ptrs,
                           const PipelineChunk &chunk,
                           const uint16_t *row_indices,
                           uint16_t count);

void UpdateAggregatesGather(const TupleDataLayout *layout,
                            uint8_t *tdc_base,
                            uint32_t row_width,
                            const uint32_t *canonical_row_indices,
                            const PipelineChunk &chunk,
                            const uint16_t *row_indices,
                            uint16_t count);

/*
 * CombineAggregates:
 *   For each aggregate a:
 *     - SUM_INT64 / COUNT_STAR / COUNT_NONNULL / COUNT_DISTINCT_NONNULL:
 *                                *(int64*)&dst_row[a.offset] +=
 *                                *(int64*)&src_row[a.offset].
 *
 * Used by HashAgg.Combine when merging per-thread local AHTs into the
 * global AHT. v1 single-mutex AHT means SinkChunk merges directly and
 * Combine is a no-op at the operator level — but this primitive lives
 * here for Q3+ when per-thread local AHTs land.
 *
 * Group columns at dst_row are assumed already equal to src_row (caller
 * verified via MatchGroup against src's group payload).
 */
void CombineAggregates(const TupleDataLayout *layout,
                       uint8_t *dst_row,
                       const uint8_t *src_row);

}  /* namespace pipeline */
}  /* namespace pg_yaap */
