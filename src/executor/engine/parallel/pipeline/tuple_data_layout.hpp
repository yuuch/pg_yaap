#pragma once

/*
 * pipeline/tuple_data_layout.hpp
 *
 * 3g.2-final — DSA-resident, fixed-width-only POD describing the row format
 * used by TupleDataCollection (HashAggregate sink, Order sink) and by the
 * scatter/gather routines in tuple_data_ops.cpp.
 *
 * Modeled on duckdb::TupleDataLayout (commit 617dfd0,
 * src/include/duckdb/common/types/row/tuple_data_layout.hpp), collapsed for
 * v1 scope:
 *   - Fixed-width columns only (int32 / int64 / double; NUMERIC → scaled int64).
 *   - NOT NULL only (no validity bitmap).
 *   - Single segment / single chunk (no spilling, no segment merging).
 *   - At most TUPLE_DATA_MAX_COLUMNS group cols + TUPLE_DATA_MAX_COLUMNS aggs.
 *
 * Out of scope (TODO(Q3+)):
 *   - VARCHAR/blob (would need a heap area + per-row heap_size_offset).
 *   - NULL bitmap (validity_width).
 *   - INT128 / Wide128 (interpreter-only types per project convention).
 *
 * Sizing: this struct is fixed-size (no FAM). One layout per operator;
 * leader allocates with `dsa_allocate0(sizeof(TupleDataLayout))` at
 * Translator time. Workers `dsa_get_address` to map.
 *
 * Spec: .sisyphus/plans/3g2-tuple-data-collection-design.md §3.1.
 */

extern "C" {
#include "postgres.h"
}

#include <cstdint>

namespace pg_yaap {
namespace pipeline {

/*
 * Compile-time cap on per-operator column counts. 32 covers all TPC-H
 * shapes (max ~12 cols including aggs at Q9). Layout struct size ≈ 1 KB.
 */
static constexpr uint16_t TUPLE_DATA_MAX_COLUMNS = 32;

/*
 * Column kind for fixed-width data columns. Scatter/gather dispatch on this
 * tag (no function pointers in DSA — see tuple_data_ops.cpp).
 *
 * INT32  is also used for char/bool/int16 (sign-extended to 4 bytes).
 * INT64  is also used for NUMERIC carried as scaled int64
 *        (see TdcColumnDesc::numeric_scale).
 *
 * STRING_REF stores a VecStringRef inline in the row. The pointed-to bytes
 * must remain owned by the surrounding pipeline object; current Milestone A
 * usage is limited to descriptor/output plumbing, not HashAgg/Order row-store
 * ownership.
 * TODO(Q3+): dedicated row-store string heap/ownership, INT128.
 */
enum class TdcColumnKind : uint8_t {
	INT32  = 0,
	INT64  = 1,
	DOUBLE = 2,
	STRING_REF = 3,
};

/*
 * Aggregate kind. Drives UpdateAggregates / CombineAggregates dispatch in
 * tuple_data_ops.cpp.
 *
 * SUM_INT64    : accumulator is int64; update reads input col as scaled
 *                int64; combine is dst += src.
 * COUNT_STAR   : accumulator is int64; update is += 1; combine is dst += src.
 * COUNT_NONNULL: accumulator is int64; update is += 1 for non-null input rows;
 *                combine is dst += src.
 * COUNT_DISTINCT_NONNULL:
 *                accumulator is int64; PhysicalHashAggregate first deduplicates
 *                distinct keys, then update/combine are the same as COUNT_NONNULL.
 *
 * TODO(Q3+): AVG (sum+count pair), STRING_AGG.
 */
enum class TdcAggKind : uint8_t {
	SUM_INT64    = 0,
	COUNT_STAR   = 1,
	COUNT_NONNULL = 2,
	COUNT_DISTINCT_NONNULL = 3,
	SUM_NUMERIC  = 4,
	AVG_NUMERIC  = 5,
	MIN_INT64    = 6,
	MIN_NUMERIC  = 7,
	MAX_INT64    = 8,
	MAX_NUMERIC  = 9,
};

/*
 * Per-column descriptor.
 *
 *   kind          - drives scatter/gather byte width and read/write ops.
 *   offset        - byte offset within the row.
 *   width         - bytes occupied by the column (4 or 8 in v1).
 *   src_col_idx   - source/output chunk column index used by Scatter/Gather.
 *   pg_type_oid   - PG type Oid (kept for diagnostics; v1 dispatch is on
 *                   TdcColumnKind, not pg_type_oid).
 *   numeric_scale - non-zero only for NUMERIC carried as scaled int64.
 *                   POW10[scale] is the multiplicative factor between the
 *                   PG NUMERIC value and the in-row int64. Valid scales for
 *                   Q1 are 0/2/4/6.
 */
struct TdcColumnDesc {
	TdcColumnKind kind;
	uint8_t       _pad0;
	uint16_t      offset;
	uint16_t      width;
	uint16_t      src_col_idx;
	int16_t       numeric_scale;
	Oid           pg_type_oid;
};

/*
 * Per-aggregate descriptor.
 *
 *   kind         - drives update/combine dispatch.
 *   offset       - byte offset within the row (after group cols).
 *   width        - 8 bytes for v1.
 *   src_col_idx  - input chunk column supplying values to update().
 *                  Ignored for COUNT_STAR.
 *   pg_agg_oid   - PG aggregate function Oid (diagnostics only in v1).
 */
struct TdcAggregateDesc {
	TdcAggKind kind;
	uint8_t    _pad0;
	uint16_t   offset;
	uint16_t   width;
	uint16_t   src_col_idx;
	int16_t    numeric_scale;
	uint16_t   _pad1;
	Oid        pg_agg_oid;
};

/*
 * Row layout (v1):
 *
 *   | col_0 | col_1 | ... | col_N | agg_0 | ... | agg_M | padding-to-8 |
 *   ^                                                                 ^
 *   0                                                          row_width
 *
 * Field ordering:
 *   - Group/payload columns first (column_count entries).
 *   - Aggregate state columns next (aggregate_count entries).
 *   - Trailing pad to align row_width up to 8 bytes.
 *
 *   column_count    + aggregate_count ≤ TUPLE_DATA_MAX_COLUMNS each.
 *   validity_width  must be 0 in v1 (NOT NULL only).
 *   row_width       always 8-byte aligned.
 */
struct TupleDataLayout {
	uint16_t         column_count;
	uint16_t         aggregate_count;
	uint16_t         row_width;
	uint16_t         validity_width;     /* 0 in v1 */
	uint32_t         _pad;
	TdcColumnDesc    columns[TUPLE_DATA_MAX_COLUMNS];
	TdcAggregateDesc aggregates[TUPLE_DATA_MAX_COLUMNS];
};

/*
 * Layout builder helpers. Implemented in tuple_data_layout.cpp.
 *
 * BuildLayoutInPlace:
 *   Reset the layout to empty (column_count=aggregate_count=0,
 *   row_width=0). Caller then calls AppendColumn / AppendAggregate / Seal
 *   in order. Must be called on a zero-initialized struct or one returned
 *   from a previous Seal().
 *
 * AppendColumn / AppendAggregate:
 *   Append a column/aggregate at the next 8-byte-aligned offset, bump
 *   row_width by `width` (which v1 forces to 8 for both — see body).
 *   Returns the column / aggregate index just appended.
 *   Errors via elog(ERROR) on overflow past TUPLE_DATA_MAX_COLUMNS.
 *
 * Seal:
 *   Round row_width up to the next multiple of 8. Idempotent.
 */
void   TupleDataLayoutInit(TupleDataLayout *layout);
uint16_t TupleDataLayoutAppendColumn(TupleDataLayout *layout,
                                      TdcColumnKind kind,
                                      uint16_t src_col_idx,
                                      Oid pg_type_oid,
                                      int16_t numeric_scale);
uint16_t TupleDataLayoutAppendAggregate(TupleDataLayout *layout,
                                         TdcAggKind kind,
                                         uint16_t src_col_idx,
                                         Oid pg_agg_oid,
                                         int16_t numeric_scale);
uint16_t TupleDataLayoutAggregateWidth(TdcAggKind kind);
void   TupleDataLayoutSeal(TupleDataLayout *layout);

/*
 * Width helper — returns the in-row byte width for a column kind.
 * v1: INT32=4, INT64=8, DOUBLE=8, STRING_REF=sizeof(VecStringRef).
 */
uint16_t TupleDataLayoutColumnWidth(TdcColumnKind kind);

}  /* namespace pipeline */
}  /* namespace pg_yaap */
