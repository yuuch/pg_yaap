#pragma once

#include "core/data_chunk.hpp"

namespace pg_yaap
{

struct VecExprStep {
	VecOpCode opcode;
	int res_idx;
	union {
		struct {
			int att_idx;
			Oid type;
			VecOutputStorageKind storage_kind;
			int storage_scale;
		} var;
		struct {
			double fval;
			int64_t i64val;
			int32_t ival;
			bool isnull;
			bool has_wide_i128;
			uint64_t wide_lo;
			int64_t wide_hi;
		} constant;
		struct { int left; int right; } op;
		struct { int cond; int if_true; int if_false; } ternary;
		struct { int att_idx; uint32_t len; uint32_t offset; uint64_t prefix; Oid type; } str_prefix;
	} d;
	};
typedef void (*VecExprJitFunc)(uint32_t count, double** col_f8, int64_t** col_i64, int32_t** col_i32, VecStringRef** col_str, uint8_t** col_nulls, const char *string_arena_base, double* res_f8, int64_t* res_i64, int32_t* res_i32, uint8_t* res_nulls, uint16_t* sel, bool has_sel);

class VecExprProgram;
bool pg_yaap_try_compile_jit_expr(const VecExprProgram *program, VecExprJitFunc *out_func, JitContext **out_context, const char **failure_reason);
void pg_yaap_register_llvm_jit_context(JitContext *context);
void pg_yaap_release_llvm_jit_context(JitContext *context);

class VecExprProgram : public PgMemoryContextObject {
public:
	VecExprProgram(); ~VecExprProgram();
	void evaluate(DataChunk<DEFAULT_CHUNK_SIZE> &chunk);
	void try_compile_jit();
	void release_jit_resources_for_proc_exit()
	{
#ifdef USE_LLVM
		if (jit_context != nullptr)
		{
			pg_yaap_release_llvm_jit_context((JitContext *) jit_context);
			jit_context = nullptr;
			jit_func = nullptr;
		}
#endif
	}
	void clear_string_consts() { string_constants.clear(); }
	uint32_t store_string_const(const char *data, uint32_t len)
	{
		uint32_t offset;

		if (data == nullptr || len == 0)
			return UINT32_MAX;
		offset = (uint32_t) string_constants.size();
		string_constants.insert(string_constants.end(), data, data + len);
		return offset;
	}
	const char *get_string_const_ptr(uint32_t offset) const
	{
		if (offset == UINT32_MAX)
			return nullptr;
		return string_constants.data() + offset;
	}
	const double* get_float8_reg(int i) const { return &registers_f8[i * DEFAULT_CHUNK_SIZE]; }
	const int64_t* get_int64_reg(int i) const { return &registers_i64[i * DEFAULT_CHUNK_SIZE]; }
	const int64_t* get_int64_hi_reg(int i) const { return &registers_i64_hi[i * DEFAULT_CHUNK_SIZE]; }
	const int32_t* get_int32_reg(int i) const { return &registers_i32[i * DEFAULT_CHUNK_SIZE]; }
	const uint8_t* get_nulls_reg(int i) const { return &registers_nulls[i * DEFAULT_CHUNK_SIZE]; }
	NumericWideInt get_wide_int_reg_value(int reg_idx, int row_idx) const
	{
		size_t slot;

		if (reg_idx < 0 || reg_idx >= MAX_REGISTERS ||
			row_idx < 0 || row_idx >= DEFAULT_CHUNK_SIZE)
			return 0;
		slot = ((size_t) reg_idx * DEFAULT_CHUNK_SIZE) + (size_t) row_idx;
		return MakeWideIntBits((uint64_t) registers_i64[slot],
							   (uint64_t) registers_i64_hi[slot]);
	}
	int get_register_scale(int i) const { return (i >= 0 && i < MAX_REGISTERS) ? register_scales[i] : 0; }
	void set_register_scale(int i, int scale) { if (i >= 0 && i < MAX_REGISTERS) register_scales[i] = scale; }
	int get_register_precision(int i) const { return (i >= 0 && i < MAX_REGISTERS) ? register_precisions[i] : 0; }
	void set_register_precision(int i, int precision) { if (i >= 0 && i < MAX_REGISTERS) register_precisions[i] = precision; }
	VecNumericWidth get_register_numeric_width(int i) const { return (i >= 0 && i < MAX_REGISTERS) ? register_numeric_widths[i] : VecNumericWidth::None; }
	void set_register_numeric_width(int i, VecNumericWidth width) { if (i >= 0 && i < MAX_REGISTERS) register_numeric_widths[i] = width; }
	void reset_register_scales() { memset(register_scales, 0, sizeof(register_scales)); }
	void reset_register_precisions() { memset(register_precisions, 0, sizeof(register_precisions)); }
	void reset_register_numeric_widths() { memset(register_numeric_widths, 0, sizeof(register_numeric_widths)); }
	int get_final_res_idx() const { return final_res_idx; }
	VolVecVector<VecExprStep> steps; int max_reg_idx; int final_res_idx;
	VecExprJitFunc jit_func = nullptr; void* jit_context = nullptr;
private:
	int32_t* registers_i32; int64_t* registers_i64; int64_t* registers_i64_hi; double* registers_f8; uint8_t* registers_nulls;
	VolVecVector<char> string_constants;
	int register_scales[MAX_REGISTERS];
	int register_precisions[MAX_REGISTERS];
	VecNumericWidth register_numeric_widths[MAX_REGISTERS];
};

} // namespace pg_yaap

