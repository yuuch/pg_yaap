#include "parallel/pipeline/translator_internal.hpp"

#include <limits>

extern "C" {
#include "access/relation.h"
#include "access/tupdesc.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "storage/lockdefs.h"
#include "utils/rel.h"
#include "utils/fmgroids.h"
#include "utils/numeric.h"

extern bool pg_yaap_trace_hooks;
extern Datum numeric_mul(PG_FUNCTION_ARGS);
extern Datum numeric_int8(PG_FUNCTION_ARGS);
}

namespace pg_yaap {
namespace pipeline {
namespace translator_detail {

static constexpr int32 kNumericTypmodVarHdrSz = 4;

static int32
MakeNumericTypmod(int precision, int scale)
{
	return ((precision << 16) | (scale & 0x7ff)) + kNumericTypmodVarHdrSz;
}

Expr *
StripRelabels(Expr *expr)
{
	while (expr != nullptr && nodeTag(expr) == T_RelabelType)
		expr = ((RelabelType *) expr)->arg;
	return expr;
}

bool
Pow10Int64(int exp, int64_t &out)
{
	if (exp < 0 || exp > 18)
		return false;
	out = 1;
	for (int i = 0; i < exp; ++i)
		out *= 10;
	return true;
}

bool
RescaleInt64Constant(int64_t value, int8_t from_scale, int8_t to_scale, int64_t &out)
{
	if (to_scale < 0 || from_scale < 0)
		return false;
	if (to_scale < from_scale)
	{
		int64_t factor = 1;
		if (!Pow10Int64(static_cast<int>(from_scale - to_scale), factor) || factor == 0)
			return false;
		out = value / factor;
		return true;
	}
	int64_t factor = 1;
	if (!Pow10Int64(static_cast<int>(to_scale - from_scale), factor))
		return false;
	out = value * factor;
	return true;
}

bool
LookupRawColumn(const ColumnRef &ref,
		        const std::vector<ColumnRef> &raw_cols_ref,
		        const std::vector<ColumnSchema> &raw_cols,
		        const ColumnSchema *&out_col)
{
	if (raw_cols_ref.size() != raw_cols.size())
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: LookupRawColumn mismatched vectors refs=%zu schema=%zu target=(%u,%d)",
				 raw_cols_ref.size(),
				 raw_cols.size(),
				 ref.varno,
				 ref.attno);
		return false;
	}
	for (size_t i = 0; i < raw_cols_ref.size(); ++i)
	{
		if (raw_cols_ref[i] == ref)
		{
			out_col = &raw_cols[i];
			return true;
		}
	}
	return false;
}

int16_t
ExtractNumericTypmodScale(int32 typmod)
{
	if (typmod < VARHDRSZ)
		return 0;
	return static_cast<int16_t>((((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024);
}

bool
ColumnNumericScale(const ColumnSchema &col, int8_t &out)
{
	switch (col.decode_kind)
	{
		case ColumnDecodeKind::INT64_NUMERIC_SCALED:
			out = col.typmod >= VARHDRSZ ? static_cast<int8_t>(ExtractNumericTypmodScale(col.typmod)) : 2;
			return true;
		case ColumnDecodeKind::INT64_INT8:
		case ColumnDecodeKind::INT32_INT4:
		case ColumnDecodeKind::INT32_CHAR:
		case ColumnDecodeKind::INT32_DATE:
		case ColumnDecodeKind::DOUBLE_FLOAT8:
		case ColumnDecodeKind::STRING_REF:
			out = 0;
			return true;
		case ColumnDecodeKind::NONE:
			return false;
	}
	return false;
}

bool
ScaleNumericConstDatumToInt64(Const *c, int8_t &out_scale, int64_t &out_value)
{
	if (c == nullptr || c->constisnull || c->consttype != NUMERICOID)
		return false;
	out_scale = static_cast<int8_t>(ExtractNumericTypmodScale(c->consttypmod));
	int64_t factor = 1;
	if (!Pow10Int64(out_scale, factor))
		return false;
	Datum factor_numeric = NumericGetDatum(int64_to_numeric(factor));
	Datum scaled = DirectFunctionCall2(numeric_mul, c->constvalue, factor_numeric);
	out_value = DatumGetInt64(DirectFunctionCall1(numeric_int8, scaled));
	return true;
}

bool
ScaleNumericConstDatumToTargetScale(Const *c, int8_t target_scale, int64_t &out_value)
{
	if (c == nullptr || c->constisnull || c->consttype != NUMERICOID || target_scale < 0)
		return false;
	if (TryFastNumericToScaledInt64(c->constvalue, target_scale, &out_value))
		return true;
	int64_t factor = 1;
	if (!Pow10Int64(target_scale, factor))
		return false;
	Datum factor_numeric = NumericGetDatum(int64_to_numeric(factor));
	Datum scaled = DirectFunctionCall2(numeric_mul, c->constvalue, factor_numeric);
	out_value = DatumGetInt64(DirectFunctionCall1(numeric_int8, scaled));
	return true;
}

bool
MapProjectedExprSchema(Oid type_oid,
		       int32 typmod,
		       int8_t numeric_scale,
		       uint8_t slot,
		       ColumnSchema &out)
{
	ColumnSchema cs{};
	cs.type_oid = type_oid;
	cs.typmod = typmod;
	cs.chunk_slot = slot;
	cs.src_attno = 0;
	cs._pad0 = 0;
	switch (type_oid)
	{
		case BOOLOID:
			cs.typmod = -1;
			cs.typlen = 1;
			cs.typbyval = true;
			cs.decode_kind = ColumnDecodeKind::INT64_INT8;
			break;
		case INT8OID:
			cs.typmod = -1;
			cs.typlen = 8;
			cs.typbyval = true;
			cs.decode_kind = ColumnDecodeKind::INT64_INT8;
			break;
		case FLOAT8OID:
			cs.typmod = -1;
			cs.typlen = 8;
			cs.typbyval = true;
			cs.decode_kind = ColumnDecodeKind::DOUBLE_FLOAT8;
			break;
		case DATEOID:
			cs.typmod = -1;
			cs.typlen = 4;
			cs.typbyval = true;
			cs.decode_kind = ColumnDecodeKind::INT32_DATE;
			break;
		case INT4OID:
			cs.typmod = -1;
			cs.typlen = 4;
			cs.typbyval = true;
			cs.decode_kind = ColumnDecodeKind::INT64_INT8;
			break;
		case NUMERICOID:
			cs.typmod = MakeNumericTypmod(18, numeric_scale);
			cs.typlen = -1;
			cs.typbyval = false;
			cs.decode_kind = ColumnDecodeKind::INT64_NUMERIC_SCALED;
			break;
		case BPCHAROID:
		case TEXTOID:
		case VARCHAROID:
			cs.typlen = -1;
			cs.typbyval = false;
			cs.decode_kind = ColumnDecodeKind::STRING_REF;
			break;
		default:
			return false;
	}
	out = cs;
	return true;
}

static bool
UseInt32CharDecodeForColumn(Oid type_oid, int32 typmod)
{
	if (type_oid == CHAROID)
		return true;
	if (type_oid != BPCHAROID)
		return false;
	if (typmod < VARHDRSZ)
		return false;
	return (typmod - VARHDRSZ) == 1;
}

bool
TryBuildPerfectHashSpec(const std::vector<ColumnRef> &group_cols,
			     const std::vector<ColumnRef> &input_cols,
			     const std::vector<ColumnSchema> &input_columns,
			     uint32_t &out_capacity)
{
	if (group_cols.empty())
		return false;
	uint32_t capacity = 1;
	for (const ColumnRef &group_col : group_cols)
	{
		const ColumnSchema *col = nullptr;
		if (!LookupRawColumn(group_col, input_cols, input_columns, col))
			return false;
		if (col->decode_kind != ColumnDecodeKind::INT32_CHAR ||
			(col->type_oid != BPCHAROID && col->type_oid != CHAROID) ||
			capacity > 1024u / 256u)
			return false;
		capacity *= 256u;
	}
	out_capacity = capacity;
	return capacity > 0 && capacity <= 1024u;
}

bool
BuildOrderedSeqScanColumns(Oid relid,
			       const std::vector<ColumnRef> &cols,
			       Index expected_varno,
			       std::vector<ColumnSchema> &out)
{
	uint8_t next_int32_slot = 0;
	uint8_t next_int64_slot = 0;
	uint8_t next_double_slot = 0;
	return BuildSeqScanColumns(relid,
		cols,
		expected_varno,
		out,
		next_int32_slot,
		next_int64_slot,
		next_double_slot);
}

static bool
ColumnDecodeKindToTdc(ColumnDecodeKind dk, TdcColumnKind &out)
{
	switch (dk)
	{
		case ColumnDecodeKind::INT32_CHAR:
		case ColumnDecodeKind::INT32_DATE:
		case ColumnDecodeKind::INT32_INT4:
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

dsa_pointer
BuildSchemaDescriptorFromColumns(const std::vector<ColumnSchema> &columns, dsa_area *dsa)
{
	if (columns.size() > std::numeric_limits<uint16_t>::max())
		return InvalidDsaPointer;
	const uint16_t n_cols = static_cast<uint16_t>(columns.size());
	const Size sz = offsetof(SchemaDescriptor, columns) +
		static_cast<Size>(n_cols) * sizeof(ColumnSchema);
	dsa_pointer dp = dsa_allocate0(dsa, sz);
	if (!DsaPointerIsValid(dp))
		return InvalidDsaPointer;
	auto *schema = static_cast<SchemaDescriptor *>(dsa_get_address(dsa, dp));
	schema->n_columns = n_cols;
	schema->_pad0 = 0;
	schema->_pad1 = 0;
	for (uint16_t i = 0; i < n_cols; ++i)
		schema->columns[i] = columns[i];
	return dp;
}

bool
BuildColumnOnlyLayout(const std::vector<ColumnSchema> &columns, TupleDataLayout &out)
{
	TupleDataLayoutInit(&out);
	for (const ColumnSchema &cs : columns)
	{
		TdcColumnKind tdc_kind;
		int8_t numeric_scale = 0;
		if (!ColumnDecodeKindToTdc(cs.decode_kind, tdc_kind) || !ColumnNumericScale(cs, numeric_scale))
			return false;
		(void) TupleDataLayoutAppendColumn(&out, tdc_kind, cs.chunk_slot, cs.type_oid, numeric_scale);
	}
	TupleDataLayoutSeal(&out);
	return true;
}

bool
BuildColumnOnlyLayoutForRefs(const std::vector<ColumnRef> &refs,
				const std::vector<ColumnRef> &available_cols,
				const std::vector<ColumnSchema> &available_schema,
				TupleDataLayout &out)
{
	std::vector<ColumnSchema> ordered;
	ordered.reserve(refs.size());
	for (const ColumnRef &ref : refs)
	{
		const ColumnSchema *col = nullptr;
		if (!LookupRawColumn(ref, available_cols, available_schema, col) || col == nullptr)
			return false;
		ordered.push_back(*col);
	}
	return BuildColumnOnlyLayout(ordered, out);
}

bool
BuildHashJoinOutputMappings(const std::vector<ColumnRef> &output_cols,
				 const std::vector<ColumnRef> &left_cols,
				 const std::vector<ColumnSchema> &left_schema,
				 const std::vector<ColumnRef> &right_cols,
				 const std::vector<ColumnSchema> &right_schema,
				 std::vector<HashJoinOutputColumnDesc> &out_mappings,
				 std::vector<ColumnSchema> &out_schema)
{
	uint8_t next_int32_slot = 0;
	uint8_t next_int64_slot = 0;
	uint8_t next_double_slot = 0;
	uint8_t next_string_slot = 0;
	out_mappings.clear();
	out_schema.clear();
	out_mappings.reserve(output_cols.size());
	out_schema.reserve(output_cols.size());

	for (const ColumnRef &ref : output_cols)
	{
		const ColumnSchema *src = nullptr;
		HashJoinOutputSide side;
		if (LookupRawColumn(ref, left_cols, left_schema, src))
			side = HashJoinOutputSide::LEFT;
		else if (LookupRawColumn(ref, right_cols, right_schema, src))
			side = HashJoinOutputSide::RIGHT;
		else
		{
			if (pg_yaap_trace_hooks)
				elog(LOG,
					 "pg_yaap: hash join output mapping missing ref=(%u,%d) left_cols=%zu right_cols=%zu",
					 ref.varno,
					 ref.attno,
					 left_cols.size(),
					 right_cols.size());
			return false;
		}

		ColumnSchema out_col = *src;
		switch (src->decode_kind)
		{
			case ColumnDecodeKind::INT32_CHAR:
			case ColumnDecodeKind::INT32_DATE:
			case ColumnDecodeKind::INT32_INT4:
				out_col.chunk_slot = next_int32_slot++;
				break;
			case ColumnDecodeKind::INT64_INT8:
			case ColumnDecodeKind::INT64_NUMERIC_SCALED:
				out_col.chunk_slot = next_int64_slot++;
				break;
			case ColumnDecodeKind::DOUBLE_FLOAT8:
				out_col.chunk_slot = next_double_slot++;
				break;
			case ColumnDecodeKind::STRING_REF:
				out_col.chunk_slot = next_string_slot++;
				break;
			case ColumnDecodeKind::NONE:
				if (pg_yaap_trace_hooks)
					elog(LOG,
						 "pg_yaap: hash join output mapping unsupported decode NONE ref=(%u,%d)",
						 ref.varno,
						 ref.attno);
				return false;
		}
		if (out_col.chunk_slot >= 16)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG,
					 "pg_yaap: hash join output mapping slot overflow ref=(%u,%d) slot=%u decode=%d",
					 ref.varno,
					 ref.attno,
					 out_col.chunk_slot,
					 static_cast<int>(src->decode_kind));
			return false;
		}
		out_col.src_attno = 0;
		out_schema.push_back(out_col);

		HashJoinOutputColumnDesc mapping{};
		mapping.side = side;
		mapping.input_chunk_slot = src->chunk_slot;
		mapping.decode_kind = src->decode_kind;
		mapping.output_chunk_slot = out_col.chunk_slot;
		out_mappings.push_back(mapping);
	}

	return !out_mappings.empty() && out_mappings.size() == out_schema.size();
}

bool
BuildHashGroupLayout(const std::vector<ColumnRef> &group_cols,
			     const std::vector<ColumnRef> &input_cols,
			     const std::vector<ColumnSchema> &input_columns,
			     const std::vector<AggFuncDesc> &agg_funcs,
			     const std::vector<TdcAggKind> &agg_kinds,
			     const std::vector<int16_t> &agg_numeric_scales,
			     TupleDataLayout &out)
{
	TupleDataLayoutInit(&out);
	for (const ColumnRef &group_col : group_cols)
	{
		uint16_t src_idx = 0;
		bool found = false;
		for (uint16_t i = 0; i < input_cols.size(); ++i)
		{
			if (input_cols[i] == group_col)
			{
				src_idx = i;
				found = true;
				break;
			}
		}
		if (!found &&
			group_col.attno != InvalidAttrNumber &&
			group_col.attno > 0 &&
			static_cast<size_t>(group_col.attno) <= input_columns.size())
		{
			src_idx = static_cast<uint16_t>(group_col.attno - 1);
			found = true;
		}
		if (!found)
			return false;
		const ColumnSchema &cs = input_columns[src_idx];
		TdcColumnKind tdc_kind;
		int8_t numeric_scale = 0;
		if (!ColumnDecodeKindToTdc(cs.decode_kind, tdc_kind) || !ColumnNumericScale(cs, numeric_scale))
			return false;
		(void) TupleDataLayoutAppendColumn(&out, tdc_kind, cs.chunk_slot, cs.type_oid, numeric_scale);
	}
	if (agg_funcs.size() != agg_kinds.size() || agg_funcs.size() != agg_numeric_scales.size())
		return false;
	for (size_t i = 0; i < agg_funcs.size(); ++i)
	{
		(void) TupleDataLayoutAppendAggregate(&out,
			agg_kinds[i],
			agg_funcs[i].input_col_idx,
			agg_funcs[i].agg_oid,
			agg_numeric_scales[i]);
	}
	TupleDataLayoutSeal(&out);
	return true;
}

static bool
AppendAggOutputColumn(TdcAggKind kind,
			    int16_t numeric_scale,
			    uint16_t payload_slot,
			    ColumnSchema &out_schema_col,
			    TupleDataLayout &out_layout)
{
	ColumnSchema cs{};
	cs.chunk_slot = static_cast<uint8_t>(payload_slot);
	cs.src_attno = 0;
	cs._pad0 = 0;
	switch (kind)
	{
		case TdcAggKind::COUNT_STAR:
		case TdcAggKind::COUNT_NONNULL:
		case TdcAggKind::COUNT_DISTINCT_NONNULL:
		case TdcAggKind::SUM_INT64:
			cs.type_oid = INT8OID;
			cs.typmod = -1;
			cs.typlen = 8;
			cs.typbyval = true;
			cs.decode_kind = ColumnDecodeKind::INT64_INT8;
			break;
		case TdcAggKind::SUM_NUMERIC:
		case TdcAggKind::AVG_NUMERIC:
			cs.type_oid = NUMERICOID;
			cs.typmod = -1;
			cs.typlen = -1;
			cs.typbyval = false;
			cs.decode_kind = ColumnDecodeKind::INT64_NUMERIC_SCALED;
			break;
		default:
			return false;
	}
	out_schema_col = cs;
	(void) TupleDataLayoutAppendColumn(&out_layout,
		TdcColumnKind::INT64,
		payload_slot,
		cs.type_oid,
		numeric_scale);
	return true;
}

bool
BuildSeqScanColumns(Oid relid,
			 const std::vector<ColumnRef> &cols,
			 Index expected_varno,
			 std::vector<ColumnSchema> &out,
			 uint8_t &next_int32_slot,
			 uint8_t &next_int64_slot,
			 uint8_t &next_double_slot)
{
	Relation rel = relation_open(relid, AccessShareLock);
	TupleDesc td = RelationGetDescr(rel);
	uint8_t i32 = 0;
	uint8_t i64 = 0;
	uint8_t dbl = 0;
	out.clear();
	out.reserve(cols.size());
	for (const ColumnRef &ref : cols)
	{
		const AttrNumber attno = ref.attno;
		if ((expected_varno != 0 && ref.varno != expected_varno) || attno <= 0 || attno > td->natts)
		{
			relation_close(rel, AccessShareLock);
			return false;
		}
		Form_pg_attribute attr = TupleDescAttr(td, attno - 1);
		ColumnSchema cs{};
		cs.type_oid = attr->atttypid;
		cs.typmod = attr->atttypmod;
		cs.typlen = attr->attlen;
		cs.typbyval = attr->attbyval;
		cs.src_attno = attno;
		cs._pad0 = 0;
		switch (attr->atttypid)
		{
			case BPCHAROID:
				cs.decode_kind = UseInt32CharDecodeForColumn(attr->atttypid, attr->atttypmod) ?
					ColumnDecodeKind::INT32_CHAR :
					ColumnDecodeKind::STRING_REF;
				cs.chunk_slot = (cs.decode_kind == ColumnDecodeKind::INT32_CHAR) ? i32++ : i32++;
				break;
			case CHAROID:
				cs.decode_kind = ColumnDecodeKind::INT32_CHAR;
				cs.chunk_slot = i32++;
				break;
			case DATEOID:
				cs.decode_kind = ColumnDecodeKind::INT32_DATE;
				cs.chunk_slot = i32++;
				break;
			case INT4OID:
				cs.decode_kind = ColumnDecodeKind::INT32_INT4;
				cs.chunk_slot = i32++;
				break;
			case INT8OID:
				cs.decode_kind = ColumnDecodeKind::INT64_INT8;
				cs.chunk_slot = i64++;
				break;
			case NUMERICOID:
				cs.decode_kind = ColumnDecodeKind::INT64_NUMERIC_SCALED;
				cs.chunk_slot = i64++;
				break;
			case FLOAT8OID:
				cs.decode_kind = ColumnDecodeKind::DOUBLE_FLOAT8;
				cs.chunk_slot = dbl++;
				break;
			case TEXTOID:
			case VARCHAROID:
				cs.decode_kind = ColumnDecodeKind::STRING_REF;
				cs.chunk_slot = i32++;
				break;
			default:
				relation_close(rel, AccessShareLock);
				return false;
		}
		if (cs.chunk_slot >= 16)
		{
			relation_close(rel, AccessShareLock);
			return false;
		}
		out.push_back(cs);
	}
	relation_close(rel, AccessShareLock);
	next_int32_slot = i32;
	next_int64_slot = i64;
	next_double_slot = dbl;
	return true;
}

dsa_pointer
BuildAggOutputSchemaDescriptor(const std::vector<ColumnRef> &group_cols,
				       const std::vector<ColumnRef> &available_cols,
				       const std::vector<ColumnSchema> &available_schema,
				       const std::vector<TdcAggKind> &agg_kinds,
				       dsa_area *dsa)
{
	const uint16_t n_groups = static_cast<uint16_t>(group_cols.size());
	const uint16_t n_aggs = static_cast<uint16_t>(agg_kinds.size());
	const uint16_t n_cols = n_groups + n_aggs;
	if (n_cols == 0 || n_cols > 16)
		return InvalidDsaPointer;
	const Size sz = offsetof(SchemaDescriptor, columns) + static_cast<Size>(n_cols) * sizeof(ColumnSchema);
	dsa_pointer dp = dsa_allocate0(dsa, sz);
	if (!DsaPointerIsValid(dp))
		return InvalidDsaPointer;
	auto *schema = static_cast<SchemaDescriptor *>(dsa_get_address(dsa, dp));
	schema->n_columns = n_cols;
	schema->_pad0 = 0;
	schema->_pad1 = 0;
	for (uint16_t i = 0; i < n_groups; ++i)
	{
		const ColumnRef &ref = group_cols[i];
		const ColumnSchema *src = nullptr;
		if (!LookupRawColumn(ref, available_cols, available_schema, src))
			return InvalidDsaPointer;
		if (src == nullptr)
			return InvalidDsaPointer;
		ColumnSchema cs{};
		cs.type_oid = src->type_oid;
		cs.typmod = src->typmod;
		cs.typlen = src->typlen;
		cs.typbyval = src->typbyval;
		cs.chunk_slot = static_cast<uint8_t>(i);
		cs.src_attno = 0;
		cs.decode_kind = src->decode_kind;
		cs._pad0 = 0;
		schema->columns[i] = cs;
	}
	for (uint16_t a = 0; a < n_aggs; ++a)
	{
		ColumnSchema cs{};
		TupleDataLayout ignored{};
		TupleDataLayoutInit(&ignored);
		if (!AppendAggOutputColumn(agg_kinds[a], 0, static_cast<uint16_t>(n_groups + a), cs, ignored))
			return InvalidDsaPointer;
		schema->columns[n_groups + a] = cs;
	}
	return dp;
}

}  /* namespace translator_detail */
}  /* namespace pipeline */
}  /* namespace pg_yaap */
