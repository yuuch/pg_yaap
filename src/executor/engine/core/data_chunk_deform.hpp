#pragma once

#include "core/data_chunk.hpp"

namespace pg_yaap
{

class DataChunkDeformer {
public:
	DataChunkDeformer(TupleDesc desc, const DeformProgram *program) : desc_(desc), program_(*program) {}
	void set_jit_func(JitDeformFunc f) { jit_func_ = f; }
	void deform_tuple_header(HeapTupleHeader tuphdr, uint32 row_idx, const DeformBindings &bindings);
private:
	TupleDesc desc_; DeformProgram program_; JitDeformFunc jit_func_ = nullptr; bool jit_path_logged_ = false;
};

enum class VecOpCode {
	EEOP_VAR,
	EEOP_CONST,
	EEOP_FLOAT8_ADD,
	EEOP_FLOAT8_SUB,
	EEOP_FLOAT8_MUL,
	EEOP_INT64_ADD,
	EEOP_INT64_SUB,
	EEOP_INT64_MUL,
	EEOP_INT64_DIV_FLOAT8,
	EEOP_FLOAT8_LT,
	EEOP_FLOAT8_GT,
	EEOP_FLOAT8_LE,
	EEOP_FLOAT8_GE,
	EEOP_INT64_LT,
	EEOP_INT64_GT,
	EEOP_INT64_LE,
	EEOP_INT64_GE,
	EEOP_INT64_EQ,
	EEOP_INT64_NE,
	EEOP_DATE_LT,
	EEOP_DATE_LE,
	EEOP_DATE_GT,
	EEOP_DATE_GE,
	EEOP_DATE_PART_YEAR,
	EEOP_INT32_EQ,
	EEOP_AND,
	EEOP_OR,
	EEOP_NOT,
	EEOP_INT64_CASE,
	EEOP_FLOAT8_CASE,
	EEOP_STR_EQ,
	EEOP_STR_NE,
	EEOP_STR_PREFIX_LIKE,
	EEOP_STR_CONTAINS_LIKE,
	EEOP_STR_LIKE_PATTERN,
	EEOP_QUAL
};

} // namespace pg_yaap
