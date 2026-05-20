// Reserved for M-Q1-PERF B.2 (batched columnar predicate path); not wired in B.1.
// B.1 evaluates the qual inline per-tuple inside PhysicalSeqScan; this batched
// API is kept so B.2 can flip on bitmap/dense selvec without re-introducing the
// type-dispatch table.

#include "parallel/pipeline/columnar_filter.hpp"

extern "C" {
#include "postgres.h"
#include "utils/date.h"
#include "utils/elog.h"
}

namespace pg_yaap {
namespace pipeline {

static inline bool
EvalClauseVector(const QualDescriptor::Clause &clause,
                 const DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE> &qual_chunk,
                 uint16_t dst_col,
                 uint16_t row_idx)
{
	if (qual_chunk.nulls[dst_col][row_idx])
		return false;

	switch (clause.const_typoid)
		{
			case DATEOID:
			{
				const int32_t l = qual_chunk.get_int32(dst_col, row_idx);
				const int32_t r = (int32_t) DatumGetDateADT((Datum) clause.const_value);
			switch (clause.op)
			{
				case QualOp::LE: return l <= r;
				case QualOp::LT: return l <  r;
				case QualOp::EQ: return l == r;
				case QualOp::GE: return l >= r;
				case QualOp::GT: return l >  r;
				case QualOp::NE: return l != r;
			}
			break;
		}
			case INT4OID:
			{
				const int32_t l = qual_chunk.get_int32(dst_col, row_idx);
				const int32_t r = DatumGetInt32((Datum) clause.const_value);
			switch (clause.op)
			{
				case QualOp::LE: return l <= r;
				case QualOp::LT: return l <  r;
				case QualOp::EQ: return l == r;
				case QualOp::GE: return l >= r;
				case QualOp::GT: return l >  r;
				case QualOp::NE: return l != r;
			}
			break;
		}
			case INT8OID:
			{
				const int64_t l = qual_chunk.get_int64(dst_col, row_idx);
				const int64_t r = DatumGetInt64((Datum) clause.const_value);
			switch (clause.op)
			{
				case QualOp::LE: return l <= r;
				case QualOp::LT: return l <  r;
				case QualOp::EQ: return l == r;
				case QualOp::GE: return l >= r;
				case QualOp::GT: return l >  r;
				case QualOp::NE: return l != r;
			}
			break;
		}
		default:
			elog(ERROR, "pg_yaap: EvalColumnarPredicate const_typoid=%u unsupported (by-value only)",
			     clause.const_typoid);
	}
	return false;
}

uint16_t
EvalColumnarPredicate(const QualDescriptor                            *qual,
                      const DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>    &qual_chunk,
                      const uint16_t                                  *qual_dst_cols,
                      uint16_t                                         count,
                      uint16_t                                        *selvec_out)
{
	if (qual == nullptr || qual->kind == QualKind::NONE)
	{
		for (uint16_t i = 0; i < count; ++i)
			selvec_out[i] = i;
		return count;
	}

	uint16_t s = 0;
	for (uint16_t row_idx = 0; row_idx < count; ++row_idx)
	{
		bool pass = true;
		for (uint8_t clause_idx = 0; clause_idx < qual->n_clauses; ++clause_idx)
		{
			if (!EvalClauseVector(qual->clauses[clause_idx],
					qual_chunk,
					qual_dst_cols[clause_idx],
					row_idx))
			{
				pass = false;
				break;
			}
		}
		if (pass)
		{
			selvec_out[s++] = row_idx;
		}
	}
	return s;
}

}
}
