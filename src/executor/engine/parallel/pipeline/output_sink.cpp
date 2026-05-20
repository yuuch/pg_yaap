#include "parallel/pipeline/output_sink.hpp"

#include <algorithm>
#include <cstring>

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "port/atomics.h"
#include "utils/date.h"
#include "utils/dsa.h"
#include "utils/elog.h"
#include "utils/numeric.h"
#include "utils/builtins.h"
#include "catalog/pg_type_d.h"
}

#include "core/data_chunk.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_layout.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"

namespace pg_yaap {
namespace pipeline {

extern "C" bool pg_yaap_trace_hooks;

namespace {

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

static inline bool
ResolveRowValueLocation(const TupleDataLayout *layout,
	                    uint16_t chunk_slot,
	                    TdcColumnKind expected_kind,
	                    const TdcColumnDesc *&column,
	                    const TdcAggregateDesc *&aggregate)
{
	column = nullptr;
	aggregate = nullptr;
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		if (layout->columns[i].src_col_idx == chunk_slot &&
			layout->columns[i].kind == expected_kind)
		{
			column = &layout->columns[i];
			return true;
		}
	}
	if (chunk_slot >= layout->column_count)
	{
		const uint16_t agg_idx = chunk_slot - layout->column_count;
		if (agg_idx < layout->aggregate_count)
		{
			aggregate = &layout->aggregates[agg_idx];
			return true;
		}
	}
	return false;
}

static inline bool
ColumnDecodeKindToTdcKind(ColumnDecodeKind decode_kind, TdcColumnKind &out)
{
	switch (decode_kind)
	{
		case ColumnDecodeKind::INT32_CHAR:
		case ColumnDecodeKind::INT32_INT4:
		case ColumnDecodeKind::INT32_DATE:
			out = TdcColumnKind::INT32;
			return true;
		case ColumnDecodeKind::INT64_INT8:
		case ColumnDecodeKind::INT64_NUMERIC_SCALED:
			out = TdcColumnKind::INT64;
			return true;
		case ColumnDecodeKind::DOUBLE_FLOAT8:
			out = TdcColumnKind::DOUBLE;
			return true;
		case ColumnDecodeKind::STRING_REF:
			out = TdcColumnKind::STRING_REF;
			return true;
		case ColumnDecodeKind::NONE:
			return false;
	}
	return false;
}

static inline Datum
EncodeStringDatum(const ColumnSchema &col, const char *ptr, uint32_t len)
{
	if (ptr == nullptr)
		elog(ERROR, "pg_yaap: string output missing arena backing");
	if (col.type_oid == TEXTOID)
		return PointerGetDatum(cstring_to_text_with_len(ptr, len));
	if (col.type_oid == VARCHAROID)
		return DirectFunctionCall3(varcharin,
		                           CStringGetDatum(pnstrdup(ptr, len)),
		                           ObjectIdGetDatum(InvalidOid),
		                           Int32GetDatum(col.typmod));
	if (col.type_oid == BPCHAROID)
		return DirectFunctionCall3(bpcharin,
		                           CStringGetDatum(pnstrdup(ptr, len)),
		                           ObjectIdGetDatum(InvalidOid),
		                           Int32GetDatum(col.typmod));
	elog(ERROR, "pg_yaap: string decode_kind unsupported for output type %u",
	     col.type_oid);
	return (Datum) 0;
}

static Datum
EncodeColumnFromRow(const ColumnSchema &col,
	                const TupleDataLayout *layout,
	                const TupleDataCollection *tdc,
	                const uint8_t *row_ptr)
{
	const TdcColumnDesc *layout_col = nullptr;
	const TdcAggregateDesc *layout_agg = nullptr;
	TdcColumnKind expected_kind;
	if (!ColumnDecodeKindToTdcKind(col.decode_kind, expected_kind) ||
		!ResolveRowValueLocation(layout, col.chunk_slot, expected_kind, layout_col, layout_agg))
		elog(ERROR, "pg_yaap: output chunk slot %u not present in row layout",
		     static_cast<unsigned>(col.chunk_slot));

	switch (col.decode_kind)
	{
		case ColumnDecodeKind::INT32_CHAR:
		case ColumnDecodeKind::INT32_INT4:
		case ColumnDecodeKind::INT32_DATE:
		{
			if (layout_col == nullptr)
				elog(ERROR, "pg_yaap: int32 output slot %u missing row column",
				     static_cast<unsigned>(col.chunk_slot));
			int32_t value;
			std::memcpy(&value, row_ptr + layout_col->offset, sizeof(value));
			if (col.decode_kind == ColumnDecodeKind::INT32_CHAR)
				return CharGetDatum(static_cast<char>(value));
			if (col.decode_kind == ColumnDecodeKind::INT32_DATE)
				return DateADTGetDatum(static_cast<DateADT>(value));
			return Int32GetDatum(value);
		}
		case ColumnDecodeKind::INT64_INT8:
		{
			const uint16_t offset = layout_col != nullptr ? layout_col->offset : layout_agg->offset;
			int64_t value;
			std::memcpy(&value, row_ptr + offset, sizeof(value));
			if (col.type_oid == BOOLOID)
				return BoolGetDatum(value != 0);
			if (col.type_oid == INT4OID)
				return Int32GetDatum(static_cast<int32_t>(value));
			return Int64GetDatum(value);
		}
		case ColumnDecodeKind::DOUBLE_FLOAT8:
		{
			if (layout_col == nullptr)
				elog(ERROR, "pg_yaap: float8 output slot %u missing row column",
				     static_cast<unsigned>(col.chunk_slot));
			double value;
			std::memcpy(&value, row_ptr + layout_col->offset, sizeof(value));
			return Float8GetDatum(value);
		}
		case ColumnDecodeKind::STRING_REF:
		{
			if (layout_col == nullptr)
				elog(ERROR, "pg_yaap: string output slot %u missing row column",
				     static_cast<unsigned>(col.chunk_slot));
			VecStringRef ref;
			std::memcpy(&ref, row_ptr + layout_col->offset, sizeof(ref));
			const char *ptr = VecStringRefDataPtr(ref,
				tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc)) : nullptr);
			return EncodeStringDatum(col, ptr, ref.len);
		}
		case ColumnDecodeKind::INT64_NUMERIC_SCALED:
		{
			const uint16_t offset = layout_col != nullptr ? layout_col->offset : layout_agg->offset;
			const int16_t scale = layout_col != nullptr ? layout_col->numeric_scale : layout_agg->numeric_scale;
			int64_t value;
			std::memcpy(&value, row_ptr + offset, sizeof(value));
			return NumericGetDatum(int64_div_fast_to_numeric(value, scale));
		}
		case ColumnDecodeKind::NONE:
			elog(ERROR, "pg_yaap: output column decode_kind=NONE invalid for sink");
			return (Datum) 0;
	}
	elog(ERROR, "pg_yaap: unknown decode_kind %u", static_cast<unsigned>(col.decode_kind));
	return (Datum) 0;
}

static const TupleDataLayout *
ResolveLayout(dsa_area *dsa, dsa_pointer layout_dp)
{
	if (!DsaPointerIsValid(layout_dp))
		return nullptr;
	return static_cast<const TupleDataLayout *>(dsa_get_address(dsa, layout_dp));
}

static TupleDataCollection *
ResolveTdc(dsa_area *dsa, dsa_pointer tdc_dp)
{
	if (!DsaPointerIsValid(tdc_dp))
		return nullptr;
	return static_cast<TupleDataCollection *>(dsa_get_address(dsa, tdc_dp));
}

static int
CompareOutputKeyColumn(const TupleDataCollection *tdc,
	                   const TdcColumnDesc &col,
	                   const uint8_t *row_a,
	                   const uint8_t *row_b)
{
	const uint8_t *a = row_a + col.offset;
	const uint8_t *b = row_b + col.offset;

	switch (col.kind)
	{
		case TdcColumnKind::INT32:
		{
			int32_t va, vb;
			std::memcpy(&va, a, sizeof(int32_t));
			std::memcpy(&vb, b, sizeof(int32_t));
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
		case TdcColumnKind::INT64:
		{
			int64_t va, vb;
			std::memcpy(&va, a, sizeof(int64_t));
			std::memcpy(&vb, b, sizeof(int64_t));
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
		case TdcColumnKind::DOUBLE:
		{
			double va, vb;
			std::memcpy(&va, a, sizeof(double));
			std::memcpy(&vb, b, sizeof(double));
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
		case TdcColumnKind::STRING_REF:
		{
			VecStringRef ref_a;
			VecStringRef ref_b;
			std::memcpy(&ref_a, a, sizeof(ref_a));
			std::memcpy(&ref_b, b, sizeof(ref_b));
			const char *base = tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc)) : nullptr;
			const char *ptr_a = VecStringRefDataPtr(ref_a, base);
			const char *ptr_b = VecStringRefDataPtr(ref_b, base);
			const uint32_t cmp_len = ref_a.len < ref_b.len ? ref_a.len : ref_b.len;
			const int cmp = cmp_len > 0 ? std::memcmp(ptr_a, ptr_b, cmp_len) : 0;
			if (cmp != 0)
				return cmp < 0 ? -1 : 1;
			return (ref_a.len < ref_b.len) ? -1 : (ref_a.len > ref_b.len) ? 1 : 0;
		}
	}

	return 0;
}

static int
CompareStoredOutputRows(const OutputGlobalState &global,
	                    const uint8_t *row_a,
	                    const uint8_t *row_b)
{
	for (uint16_t i = 0; i < global.n_sort_keys; ++i)
	{
		const SortKeyDesc &key = global.sort_keys[i];
		if (key.col_idx >= global.layout->column_count)
			continue;
		const int cmp = CompareOutputKeyColumn(global.global_tdc,
			global.layout->columns[key.col_idx],
			row_a,
			row_b);
		if (cmp != 0)
			return key.asc ? cmp : -cmp;
	}
	return 0;
}

static int
CompareChunkRowToStoredRow(const OutputGlobalState &global,
	                       const PipelineChunk &chunk,
	                       uint16_t row_idx,
	                       const uint8_t *stored_row)
{
	for (uint16_t i = 0; i < global.n_sort_keys; ++i)
	{
		const SortKeyDesc &key = global.sort_keys[i];
		if (key.col_idx >= global.layout->column_count ||
			key.col_idx >= global.input_schema->n_columns)
			continue;

		const ColumnSchema &schema_col = global.input_schema->columns[key.col_idx];
		const TdcColumnDesc &layout_col = global.layout->columns[key.col_idx];
		int cmp = 0;
		switch (layout_col.kind)
		{
			case TdcColumnKind::INT32:
			{
				const int32_t lhs = chunk.get_int32(schema_col.chunk_slot, row_idx);
				int32_t rhs = 0;
				std::memcpy(&rhs, stored_row + layout_col.offset, sizeof(rhs));
				cmp = (lhs < rhs) ? -1 : (lhs > rhs) ? 1 : 0;
				break;
			}
			case TdcColumnKind::INT64:
			{
				const int64_t lhs = chunk.get_int64(schema_col.chunk_slot, row_idx);
				int64_t rhs = 0;
				std::memcpy(&rhs, stored_row + layout_col.offset, sizeof(rhs));
				cmp = (lhs < rhs) ? -1 : (lhs > rhs) ? 1 : 0;
				break;
			}
			case TdcColumnKind::DOUBLE:
			{
				const double lhs = chunk.get_double(schema_col.chunk_slot, row_idx);
				double rhs = 0;
				std::memcpy(&rhs, stored_row + layout_col.offset, sizeof(rhs));
				cmp = (lhs < rhs) ? -1 : (lhs > rhs) ? 1 : 0;
				break;
			}
			case TdcColumnKind::STRING_REF:
			{
				const VecStringRef lhs_ref = chunk.get_string_ref(schema_col.chunk_slot, row_idx);
				const char *lhs_ptr = chunk.get_string_ptr(schema_col.chunk_slot, row_idx);
				VecStringRef rhs_ref;
				std::memcpy(&rhs_ref, stored_row + layout_col.offset, sizeof(rhs_ref));
				const char *rhs_ptr = VecStringRefDataPtr(rhs_ref,
					reinterpret_cast<const char *>(TupleDataCollectionHeapConst(global.global_tdc)));
				const uint32_t cmp_len = lhs_ref.len < rhs_ref.len ? lhs_ref.len : rhs_ref.len;
				cmp = cmp_len > 0 ? std::memcmp(lhs_ptr, rhs_ptr, cmp_len) : 0;
				if (cmp != 0)
					cmp = cmp < 0 ? -1 : 1;
				else
					cmp = (lhs_ref.len < rhs_ref.len) ? -1 : (lhs_ref.len > rhs_ref.len) ? 1 : 0;
				break;
			}
		}
		if (cmp != 0)
			return key.asc ? cmp : -cmp;
	}
	return 0;
}

static uint32_t
FindWorstTopNRowIndex(const OutputGlobalState &global)
{
	const uint32_t row_count = pg_atomic_read_u32(
		const_cast<pg_atomic_uint32 *>(&global.global_tdc->row_count));
	uint32_t worst_idx = 0;
	for (uint32_t row_idx = 1; row_idx < row_count; ++row_idx)
	{
		const uint8_t *candidate = TupleDataCollectionGetRowConst(global.global_tdc, row_idx);
		const uint8_t *worst = TupleDataCollectionGetRowConst(global.global_tdc, worst_idx);
		if (CompareStoredOutputRows(global, candidate, worst) > 0)
			worst_idx = row_idx;
	}
	return worst_idx;
}

static void
CopyTdcRow(const TupleDataLayout *layout,
	       const TupleDataCollection *src_tdc,
	       const uint8_t *src_row,
	       TupleDataCollection *dst_tdc,
	       uint8_t *dst_row)
{
	if (!LayoutHasStringColumns(layout))
	{
		std::memcpy(dst_row, src_row, layout->row_width);
		return;
	}

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
		if (!TupleDataCollectionStoreStringBytes(dst_tdc, src_ptr, src_ref.len, &dst_ref))
			elog(ERROR, "pg_yaap: output TDC grow ran out of heap");
		std::memcpy(dst_row + col.offset, &dst_ref, sizeof(dst_ref));
	}

	for (uint16_t agg_idx = 0; agg_idx < layout->aggregate_count; ++agg_idx)
	{
		const TdcAggregateDesc &agg = layout->aggregates[agg_idx];
		const uint16_t width = agg.kind == TdcAggKind::AVG_NUMERIC ? 16 : 8;
		std::memcpy(dst_row + agg.offset, src_row + agg.offset, width);
	}
}

static inline void
AppendChunkRowToTdc(const OutputGlobalState &global,
	                TupleDataCollection *tdc,
	                uint8_t *row_ptr,
	                const PipelineChunk &chunk,
	                uint16_t row_idx)
{
	Scatter(global.layout, tdc, row_ptr, chunk, row_idx);
}

static dsa_pointer
GrowOutputTdc(ExecCtx &ctx, OutputGlobalState &global, uint32_t required_heap_bytes)
{
	dsa_pointer old_tdc_dp = global.shared_payload_dp;
	TupleDataCollection *old_tdc = global.global_tdc;
	if (old_tdc == nullptr || global.layout == nullptr)
		elog(ERROR, "pg_yaap: output TDC missing during grow");
	const uint32_t old_count = pg_atomic_read_u32(&old_tdc->row_count);
	uint32_t new_capacity = std::max(old_tdc->row_capacity * 2u, old_count + 1u);
	new_capacity = TupleDataCollectionClampRowCapacity(new_capacity,
		old_tdc->row_width,
		required_heap_bytes,
		old_count + 1u);
	const uint32_t heap_capacity = TupleDataCollectionGrowHeapCapacity(global.layout,
		old_tdc,
		new_capacity,
		required_heap_bytes);
	dsa_pointer new_tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
		new_capacity,
		old_tdc->row_width,
		heap_capacity);
	TupleDataCollection *new_tdc = ResolveTdc(ctx.dsa, new_tdc_dp);
	TupleDataCollectionInit(new_tdc,
		new_capacity,
		old_tdc->row_width,
		old_tdc->layout_dp,
		heap_capacity);
	for (uint32_t row_idx = 0; row_idx < old_count; ++row_idx)
	{
		uint8_t *dst = nullptr;
		const uint32_t copied_idx = TupleDataCollectionAppendRow(new_tdc, &dst);
		if (copied_idx == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: output TDC grow copy overflow");
		CopyTdcRow(global.layout,
			old_tdc,
			TupleDataCollectionGetRowConst(old_tdc, row_idx),
			new_tdc,
			dst);
	}
	global.global_tdc = new_tdc;
	global.shared_payload_dp = new_tdc_dp;
	if (DsaPointerIsValid(old_tdc_dp))
		dsa_free(ctx.dsa, old_tdc_dp);
	return new_tdc_dp;
}

static dsa_pointer
ReplaceWorstTopNRow(ExecCtx &ctx,
	                OutputGlobalState &global,
	                const PipelineChunk &chunk,
	                uint16_t row_idx,
	                uint32_t worst_idx)
{
	dsa_pointer old_tdc_dp = global.shared_payload_dp;
	TupleDataCollection *old_tdc = global.global_tdc;
	if (old_tdc == nullptr || global.layout == nullptr)
		elog(ERROR, "pg_yaap: top-N output TDC missing during replacement");

	const uint32_t row_capacity = old_tdc->row_capacity;
	const uint32_t row_count = pg_atomic_read_u32(&old_tdc->row_count);
	uint64_t heap_needed = TupleDataCollectionRequiredHeapBytesForChunkRow(global.layout, chunk, row_idx);
	for (uint32_t existing_idx = 0; existing_idx < row_count; ++existing_idx)
	{
		if (existing_idx == worst_idx)
			continue;
		heap_needed += TupleDataCollectionRequiredHeapBytesForRow(global.layout,
			old_tdc,
			TupleDataCollectionGetRowConst(old_tdc, existing_idx));
	}
	uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(global.layout, row_capacity);
	if (heap_needed > heap_capacity)
		heap_capacity = static_cast<uint32_t>(heap_needed);
	TupleDataCollectionCheckFlatAllocSize(row_capacity, old_tdc->row_width, heap_capacity);

	dsa_pointer new_tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
		row_capacity,
		old_tdc->row_width,
		heap_capacity);
	TupleDataCollection *new_tdc = ResolveTdc(ctx.dsa, new_tdc_dp);
	TupleDataCollectionInit(new_tdc,
		row_capacity,
		old_tdc->row_width,
		old_tdc->layout_dp,
		heap_capacity);
	for (uint32_t existing_idx = 0; existing_idx < row_count; ++existing_idx)
	{
		if (existing_idx == worst_idx)
			continue;
		uint8_t *dst = nullptr;
		if (TupleDataCollectionAppendRow(new_tdc, &dst) == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: top-N output replacement overflow while copying survivors");
		CopyTdcRow(global.layout,
			old_tdc,
			TupleDataCollectionGetRowConst(old_tdc, existing_idx),
			new_tdc,
			dst);
	}
	uint8_t *dst = nullptr;
	if (TupleDataCollectionAppendRow(new_tdc, &dst) == TDC_INVALID_ROW_INDEX)
		elog(ERROR, "pg_yaap: top-N output replacement overflow while appending new row");
	AppendChunkRowToTdc(global, new_tdc, dst, chunk, row_idx);
	global.global_tdc = new_tdc;
	global.shared_payload_dp = new_tdc_dp;
	if (DsaPointerIsValid(old_tdc_dp))
		dsa_free(ctx.dsa, old_tdc_dp);
	return new_tdc_dp;
}

}

std::unique_ptr<GlobalSinkState>
OutputSink::GetGlobalSinkState(ExecCtx &ctx)
{
	auto state = std::make_unique<OutputGlobalState>();

	/* Resolve descriptor-resident DSA references shared across workers + leader.
	 * Prefer translator-set member dps; fall back to the descriptor (workers
	 * reconstruct OutputSink with the same dps but the descriptor is the
	 * canonical publish point — see pipeline_descriptor.cpp:240-244).
	 *
	 * desc_ may legitimately be nullptr on the leader path (translator passes
	 * desc=nullptr in the 6-arg leader ctor; the descriptor is built later and
	 * never re-attached to the leader's OutputSink). When member dps are all
	 * valid the descriptor fallback is never consulted, so a null desc_ is
	 * fine. We only fault if a member dp is missing AND desc_ is also null. */
	const dsa_pointer schema_dp = DsaPointerIsValid(input_schema_dp_) ?
		input_schema_dp_ : (desc_ ? desc_->body.output.input_schema : InvalidDsaPointer);
	const dsa_pointer layout_dp = DsaPointerIsValid(layout_dp_) ?
		layout_dp_ : (desc_ ? desc_->body.output.layout : InvalidDsaPointer);
	const dsa_pointer sort_keys_dp = DsaPointerIsValid(final_sort_keys_dp_) ?
		final_sort_keys_dp_ : (desc_ ? desc_->body.output.sort_keys : InvalidDsaPointer);
	dsa_pointer payload_dp = (desc_ && DsaPointerIsValid(desc_->body.output.shared_payload)) ?
		desc_->body.output.shared_payload : shared_payload_dp_;

	if (!DsaPointerIsValid(schema_dp))
		elog(ERROR, "pg_yaap: output sink missing input_schema");
	if (!DsaPointerIsValid(layout_dp))
		elog(ERROR, "pg_yaap: output sink missing TupleDataLayout");

	state->input_schema = static_cast<const SchemaDescriptor *>(
		dsa_get_address(ctx.dsa, schema_dp));
	state->layout = ResolveLayout(ctx.dsa, layout_dp);
	state->n_sort_keys = n_final_sort_keys_ > 0 ? n_final_sort_keys_ :
		(desc_ ? desc_->body.output.n_sort_keys : 0);
	if (DsaPointerIsValid(sort_keys_dp) && state->n_sort_keys > 0)
		state->sort_keys = static_cast<const SortKeyDesc *>(dsa_get_address(ctx.dsa, sort_keys_dp));
	if (state->layout == nullptr)
		elog(ERROR, "pg_yaap: output sink layout resolve failed");

	/* Global TDC is pre-allocated + Init'd by Translator at descriptor-build
	 * time and either threaded directly through shared_payload_dp_ (leader
	 * ctor path) or republished via desc_->body.output.shared_payload (worker
	 * ctor path); both leader and workers only attach here. See translator.cpp
	 * around the OutputSink ctor for the alloc site and rationale (OUTPUT
	 * pipeline RUN tasks fan out across all workers, so no leader-first gate
	 * is available at attach time). */
	if (!DsaPointerIsValid(payload_dp))
		elog(ERROR, "pg_yaap: output sink shared_payload not published by translator");
	state->global_tdc = ResolveTdc(ctx.dsa, payload_dp);

	if (state->global_tdc == nullptr)
		elog(ERROR, "pg_yaap: output sink global TDC resolve failed");

	state->shared_payload_dp = payload_dp;
	state->max_emit_rows = max_emit_rows_ > 0 ? max_emit_rows_ :
		(desc_ ? desc_->body.output.max_emit_rows : 0);

	/* Leader-only artifacts: dest/tupdesc/slot are owned in the leader process
	 * and consumed by EmitGlobalTdcToDest after FINALIZE; workers leave these
	 * null and only stage rows into the shared TDC. */
	if (ctx.worker_index == LEADER_WORKER_INDEX && dest_ != nullptr)
	{
		state->dest = dest_;
		state->tupdesc = tupdesc_;
		state->slot = MakeSingleTupleTableSlot(tupdesc_, &TTSOpsVirtual);
	}

	return state;
}

std::unique_ptr<LocalSinkState>
OutputSink::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate)
{
	(void) ctx;
	(void) gstate;
	return std::make_unique<OutputLocalState>();
}

SinkResultType
OutputSink::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input)
{
	auto &global = static_cast<OutputGlobalState &>(input.global_state);
	auto &local = static_cast<OutputLocalState &>(input.local_state);

	if (global.global_tdc == nullptr || global.layout == nullptr)
		elog(ERROR, "pg_yaap: output sink not initialized");

	for (uint16_t row = 0; row < in.count; ++row)
	{
		if (global.max_emit_rows > 0 && global.n_sort_keys > 0)
		{
			const uint32_t row_count = pg_atomic_read_u32(&global.global_tdc->row_count);
			if (row_count >= global.max_emit_rows)
			{
				const uint32_t worst_idx = FindWorstTopNRowIndex(global);
				const uint8_t *worst_row = TupleDataCollectionGetRowConst(global.global_tdc, worst_idx);
				if (CompareChunkRowToStoredRow(global, in, row, worst_row) < 0)
				{
					shared_payload_dp_ = ReplaceWorstTopNRow(ctx, global, in, row, worst_idx);
					StoreSharedPayloadOnDescriptor(this, shared_payload_dp_);
					++local.emitted_rows;
				}
				continue;
			}
		}
		while (!TupleDataCollectionHasSpaceForAppend(global.global_tdc,
			TupleDataCollectionRequiredHeapBytesForChunkRow(global.layout, in, row)))
		{
			shared_payload_dp_ = GrowOutputTdc(ctx,
				global,
				TupleDataCollectionRequiredHeapBytesForChunkRow(global.layout, in, row));
			StoreSharedPayloadOnDescriptor(this, shared_payload_dp_);
		}
		uint8_t *row_ptr = nullptr;
		const uint32_t row_idx = TupleDataCollectionAppendRow(global.global_tdc, &row_ptr);
		if (row_idx == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_yaap: output sink row capacity %u exceeded",
			     global.global_tdc->row_capacity);

		/* OutputSink layout has columns only (no aggregates); Scatter writes
		 * exactly layout->columns[0..N-1] from the input chunk slots, matching
		 * the column-decode metadata in the parallel SchemaDescriptor. */
		AppendChunkRowToTdc(global, global.global_tdc, row_ptr, in, row);
		++local.emitted_rows;
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
OutputSink::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input)
{
	/* Single shared TDC: workers append directly under the TDC spinlock, so
	 * there is no per-thread local payload to merge. Combine is a no-op. */
	(void) ctx;
	(void) input;
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType
OutputSink::Finalize(ExecCtx &ctx, GlobalSinkState &gstate)
{
	(void) ctx;
	auto &global = static_cast<OutputGlobalState &>(gstate);

	/* Publish sink->source handoff: Output has no downstream operator, but
	 * EmitGlobalTdcToDest gates on this flag so the leader cannot drain a
	 * partially-populated TDC if Finalize is somehow re-entered. */
	if (global.global_tdc != nullptr)
		global.global_tdc->finalized = true;
	global.finalized = true;

	return SinkFinalizeType::READY;
}

void
OutputSink::EmitGlobalTdcToDest(ExecCtx &ctx)
{
	if (dest_ == nullptr)
		return;  /* Worker-side reconstruct never has a dest; leader-only path. */

	const dsa_pointer schema_dp = DsaPointerIsValid(input_schema_dp_) ?
		input_schema_dp_ : (desc_ ? desc_->body.output.input_schema : InvalidDsaPointer);
	const dsa_pointer layout_dp = DsaPointerIsValid(layout_dp_) ?
		layout_dp_ : (desc_ ? desc_->body.output.layout : InvalidDsaPointer);
	const dsa_pointer payload_dp = (desc_ && DsaPointerIsValid(desc_->body.output.shared_payload)) ?
		desc_->body.output.shared_payload : shared_payload_dp_;

	if (!DsaPointerIsValid(schema_dp) || !DsaPointerIsValid(layout_dp) ||
	    !DsaPointerIsValid(payload_dp))
		elog(ERROR, "pg_yaap: EmitGlobalTdcToDest missing DSA references");

	const auto *schema = static_cast<const SchemaDescriptor *>(
		dsa_get_address(ctx.dsa, schema_dp));
	const auto *layout = ResolveLayout(ctx.dsa, layout_dp);
	auto *tdc = ResolveTdc(ctx.dsa, payload_dp);
	if (schema == nullptr || layout == nullptr || tdc == nullptr)
		elog(ERROR, "pg_yaap: EmitGlobalTdcToDest resolve failed");
	if (!tdc->finalized)
		elog(ERROR, "pg_yaap: EmitGlobalTdcToDest invoked before TDC finalized");

	const uint16_t natts = schema->n_columns;
	if (tupdesc_ == nullptr || natts > tupdesc_->natts)
		elog(ERROR, "pg_yaap: output schema has %u columns but tupdesc has %d",
		     natts, tupdesc_ != nullptr ? tupdesc_->natts : 0);

	TupleTableSlot *slot = MakeSingleTupleTableSlot(tupdesc_, &TTSOpsVirtual);

	/* DestReceiver lifecycle: ExecutorRun normally calls rStartup before the
	 * first row and rShutdown after the last. We bypass standard_ExecutorRun,
	 * so we MUST drive it here, otherwise printtup never sends RowDescription
	 * ('T') and libpq drops every data row. */
	dest_->rStartup(dest_, operation_, tupdesc_);

	const uint32_t row_count = pg_atomic_read_u32(&tdc->row_count);
	std::vector<uint32_t> row_order;
	if (pg_yaap_trace_hooks)
		elog(LOG,
			 "pg_yaap: OutputSink.Emit rows=%u final_sort_keys=%zu max_emit_rows=%llu",
			 row_count,
			 final_sort_keys_.size(),
			 (unsigned long long) max_emit_rows_);
	if (!final_sort_keys_.empty())
	{
		const std::vector<SortKeyDesc> sort_keys = final_sort_keys_;
		if (pg_yaap_trace_hooks)
		{
			for (size_t i = 0; i < sort_keys.size(); ++i)
				elog(LOG,
					 "pg_yaap: OutputSink.SortKey idx=%zu col_idx=%u asc=%d nulls_first=%d",
					 i,
					 sort_keys[i].col_idx,
					 sort_keys[i].asc ? 1 : 0,
					 sort_keys[i].nulls_first ? 1 : 0);
		}
		row_order.resize(row_count);
		for (uint32_t i = 0; i < row_count; ++i)
			row_order[i] = i;
		std::sort(row_order.begin(), row_order.end(),
			[&sort_keys, layout, tdc](uint32_t lhs, uint32_t rhs)
		{
			const uint8_t *row_a = TupleDataCollectionGetRowConst(tdc, lhs);
			const uint8_t *row_b = TupleDataCollectionGetRowConst(tdc, rhs);
			for (const SortKeyDesc &key : sort_keys)
			{
				if (key.col_idx >= layout->column_count)
					continue;
				const TdcColumnDesc &col = layout->columns[key.col_idx];
				const int cmp = CompareOutputKeyColumn(tdc, col, row_a, row_b);
				if (cmp != 0)
					return key.asc ? (cmp < 0) : (cmp > 0);
			}
			return false;
		});
	}
	const uint32_t emit_count =
		(max_emit_rows_ > 0 && max_emit_rows_ < row_count)
			? static_cast<uint32_t>(max_emit_rows_)
			: row_count;
	for (uint32_t i = 0; i < emit_count; ++i)
	{
		const uint32_t row_idx = row_order.empty() ? i : row_order[i];
		const uint8_t *row_ptr = TupleDataCollectionGetRowConst(tdc, row_idx);

		ExecClearTuple(slot);
		for (uint16_t c = 0; c < natts; ++c)
		{
			const ColumnSchema &col = schema->columns[c];

			slot->tts_values[c] = EncodeColumnFromRow(col, layout, tdc, row_ptr);
			slot->tts_isnull[c] = false;

			/* Q1 stores l_returnflag/l_linestatus as INT32_CHAR (single byte)
			 * but the SQL column type is bpchar(1) (varlena). printtup calls
			 * bpcharsend which dereferences the Datum as a varlena pointer;
			 * passing the raw byte segfaults. Wrap in a 1-char bpchar here. */
			Oid atttypid = TupleDescAttr(tupdesc_, c)->atttypid;
			if (atttypid == BPCHAROID && col.decode_kind == ColumnDecodeKind::INT32_CHAR)
			{
				char ch = (char) DatumGetChar(slot->tts_values[c]);
				BpChar *bp = (BpChar *) palloc(VARHDRSZ + 1);
				SET_VARSIZE(bp, VARHDRSZ + 1);
				VARDATA(bp)[0] = ch;
				slot->tts_values[c] = PointerGetDatum(bp);
			}
		}
		ExecStoreVirtualTuple(slot);
		dest_->receiveSlot(slot, dest_);
	}

	dest_->rShutdown(dest_);

	ExecDropSingleTupleTableSlot(slot);
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
