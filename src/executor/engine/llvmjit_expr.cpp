extern "C" {
#include "postgres.h"

#ifdef USE_LLVM

#include <llvm-c/Core.h>

#include "fmgr.h"
#include "jit/jit.h"
#include "jit/llvmjit.h"
#include "jit/llvmjit_emit.h"
#include "miscadmin.h"
#include "storage/fd.h"
#endif
}

#include "yaap_engine.hpp"
#include "llvmjit_deform_datachunk.h"

#ifdef USE_LLVM

#include <array>
#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <algorithm>
#include <vector>

namespace pg_yaap {

typedef void *(*llvm_create_context_type)(int);
typedef char *(*llvm_expand_funcname_type)(void *, const char *);
typedef void *(*llvm_get_function_type)(void *, const char *);
typedef LLVMModuleRef (*llvm_mutable_module_type)(LLVMJitContext *);
typedef LLVMTypeRef (*llvm_pg_var_type_type)(const char *);
typedef void (*llvm_release_context_direct_type)(LLVMJitContext *);

static llvm_create_context_type pg_llvm_create_context = nullptr;
static llvm_expand_funcname_type pg_llvm_expand_funcname = nullptr;
static llvm_get_function_type pg_llvm_get_function = nullptr;
static llvm_mutable_module_type pg_llvm_mutable_module = nullptr;
static llvm_pg_var_type_type pg_llvm_pg_var_type = nullptr;
static llvm_release_context_direct_type pg_llvm_release_context_direct = nullptr;
static std::vector<JitContext *> pg_yaap_live_jit_contexts;

static void
unregister_jit_context(JitContext *context)
{
	auto it = std::find(pg_yaap_live_jit_contexts.begin(),
						pg_yaap_live_jit_contexts.end(),
						context);

	if (it != pg_yaap_live_jit_contexts.end())
		pg_yaap_live_jit_contexts.erase(it);
}

static bool
jit_symbols_resolved()
{
	return pg_llvm_create_context != nullptr &&
		pg_llvm_expand_funcname != nullptr &&
		pg_llvm_get_function != nullptr &&
		pg_llvm_mutable_module != nullptr &&
		pg_llvm_pg_var_type != nullptr &&
		pg_llvm_release_context_direct != nullptr;
}

static bool
resolve_jit_symbols_from_handle(void *h)
{
	llvm_create_context_type create_context;
	llvm_expand_funcname_type expand_funcname;
	llvm_get_function_type get_function;
	llvm_mutable_module_type mutable_module;
	llvm_pg_var_type_type pg_var_type;
	llvm_release_context_direct_type release_context_direct;

	if (!h)
		return false;

	create_context = (llvm_create_context_type) dlsym(h, "llvm_create_context");
	expand_funcname = (llvm_expand_funcname_type) dlsym(h, "llvm_expand_funcname");
	get_function = (llvm_get_function_type) dlsym(h, "llvm_get_function");
	mutable_module = (llvm_mutable_module_type) dlsym(h, "llvm_mutable_module");
	pg_var_type = (llvm_pg_var_type_type) dlsym(h, "llvm_pg_var_type");
	release_context_direct = (llvm_release_context_direct_type) dlsym(h, "llvm_release_context_direct");
	if (create_context == nullptr ||
		expand_funcname == nullptr ||
		get_function == nullptr ||
		mutable_module == nullptr ||
		pg_var_type == nullptr ||
		release_context_direct == nullptr)
		return false;

	pg_llvm_create_context = create_context;
	pg_llvm_expand_funcname = expand_funcname;
	pg_llvm_get_function = get_function;
	pg_llvm_mutable_module = mutable_module;
	pg_llvm_pg_var_type = pg_var_type;
	pg_llvm_release_context_direct = release_context_direct;
	return true;
}

static bool
resolve_jit_symbols_from_process()
{
	void *h = dlopen(NULL, RTLD_NOW | RTLD_GLOBAL);

	return resolve_jit_symbols_from_handle(h);
}

static bool
resolve_jit_symbols_from_provider(const char *provider_path)
{
	void *h;
	bool resolved;

	h = dlopen(provider_path, RTLD_NOW | RTLD_GLOBAL);
	if (!h)
		return false;

	resolved = resolve_jit_symbols_from_handle(h);
	dlclose(h);
	return resolved;
}

static bool
load_jit_symbols(const char **failure_reason)
{
	char provider_path[MAXPGPATH];
	const char *provider_name;

	if (jit_symbols_resolved())
		return true;
	if (resolve_jit_symbols_from_process())
		return true;

	provider_name = (jit_provider != nullptr && jit_provider[0] != '\0') ?
		jit_provider : "llvmjit";
	snprintf(provider_path, sizeof(provider_path), "%s/%s%s",
			 pkglib_path, provider_name, DLSUFFIX);

	if (!pg_file_exists(provider_path))
	{
		if (failure_reason != nullptr)
			*failure_reason = "configured JIT provider library is not present";
		return false;
	}

	load_file(provider_path, false);
	if (resolve_jit_symbols_from_process())
		return true;
	if (resolve_jit_symbols_from_provider(provider_path))
		return true;

	if (failure_reason != nullptr)
		*failure_reason = "loaded JIT provider but LLVM entry points are unavailable";
	return false;
}

void
pg_yaap_register_llvm_jit_context(JitContext *context)
{
	if (context == nullptr)
		return;
	if (std::find(pg_yaap_live_jit_contexts.begin(),
				  pg_yaap_live_jit_contexts.end(),
				  context) == pg_yaap_live_jit_contexts.end())
		pg_yaap_live_jit_contexts.push_back(context);
}

void
pg_yaap_release_llvm_jit_context(JitContext *context)
{
	if (context == nullptr)
		return;
	unregister_jit_context(context);
	if (!load_jit_symbols(nullptr))
		return;
	pg_llvm_release_context_direct((LLVMJitContext *) context);
}

size_t
pg_yaap_release_all_registered_llvm_jit_contexts_for_proc_exit()
{
	size_t released = 0;

	if (!load_jit_symbols(nullptr))
		return 0;
	while (!pg_yaap_live_jit_contexts.empty())
	{
		JitContext *context = pg_yaap_live_jit_contexts.back();

		pg_yaap_live_jit_contexts.pop_back();
		if (context == nullptr)
			continue;
		pg_llvm_release_context_direct((LLVMJitContext *) context);
		released++;
	}
	return released;
}

}  /* namespace pg_yaap */

/* before_shmem_exit shim: drains live JIT contexts before llvm_shutdown's
 * on_proc_exit refcount PANIC fires (workers can skip ~SeqScanLocalState on
 * signal-driven proc_exit mid-compile; before_shmem_exit precedes on_proc_exit). */
extern "C" void pg_yaap_proc_exit_release_jit_contexts(int code, Datum arg);

extern "C" void
pg_yaap_proc_exit_release_jit_contexts(int /*code*/, Datum /*arg*/)
{
	(void) pg_yaap::pg_yaap_release_all_registered_llvm_jit_contexts_for_proc_exit();
}

namespace pg_yaap {

static inline int64_t
pg_yaap_jit_pow10_int64(int scale)
{
	static const int64_t kPowers[] = {
		INT64CONST(1),
		INT64CONST(10),
		INT64CONST(100),
		INT64CONST(1000),
		INT64CONST(10000),
		INT64CONST(100000),
		INT64CONST(1000000),
		INT64CONST(10000000),
		INT64CONST(100000000),
		INT64CONST(1000000000),
		INT64CONST(10000000000),
		INT64CONST(100000000000),
		INT64CONST(1000000000000),
		INT64CONST(10000000000000),
		INT64CONST(100000000000000),
		INT64CONST(1000000000000000),
		INT64CONST(10000000000000000),
		INT64CONST(100000000000000000),
		INT64CONST(1000000000000000000)
	};

	if (scale < 0)
		scale = 0;
	else if (scale >= (int) lengthof(kPowers))
		scale = lengthof(kPowers) - 1;

	return kPowers[scale];
}

static inline LLVMValueRef
pg_yaap_i32_const(LLVMTypeRef type_i32, int32_t value)
{
	return LLVMConstInt(type_i32, (uint64_t) (uint32_t) value, true);
}

static inline LLVMValueRef
pg_yaap_i64_const(LLVMTypeRef type_i64, int64_t value)
{
	return LLVMConstInt(type_i64, (uint64_t) value, true);
}

static inline LLVMValueRef
pg_yaap_i128_const(LLVMTypeRef type_i128, int64_t value)
{
	return LLVMConstInt(type_i128, (uint64_t) value, true);
}

static inline LLVMValueRef
pg_yaap_i128_const_bits(LLVMTypeRef type_i128, uint64_t lo, uint64_t hi)
{
	uint64_t words[2];

	words[0] = lo;
	words[1] = hi;
	return LLVMConstIntOfArbitraryPrecision(type_i128, 2, words);
}

static inline LLVMValueRef
pg_yaap_l_ptr_const(LLVMTypeRef type_sizet, void *ptr, LLVMTypeRef type)
{
	LLVMValueRef c = LLVMConstInt(type_sizet, (uintptr_t) ptr, false);

	return LLVMConstIntToPtr(c, type);
}

static uint32_t
pg_yaap_jit_trim_bpchar_length(const char *data, uint32_t len)
{
	while (len > 0 && data[len - 1] == ' ')
		len--;
	return len;
}

extern "C" bool pg_yaap_jit_string_const_match_ref(const VecStringRef *ref,
														const char *arena_base,
														const char *match,
														uint32_t match_len,
														Oid string_type,
														bool prefix_like);

extern "C" bool
pg_yaap_jit_string_const_match_ref(const VecStringRef *ref,
									 const char *arena_base,
									 const char *match,
									 uint32_t match_len,
									 Oid string_type,
									 bool prefix_like)
{
	const char *lhs;
	uint64_t mask = 0;

	if (ref == nullptr)
		return false;
	if (match_len > 0)
		mask = (match_len >= 8) ? UINT64_MAX : ((UINT64CONST(1) << (match_len * 8)) - 1);
	if (match_len > 0)
	{
		uint64_t rhs_prefix = 0;

		memcpy(&rhs_prefix, match, match_len > 8 ? 8 : match_len);
		if ((ref->prefix & mask) != (rhs_prefix & mask))
			return false;
	}

	if (prefix_like)
	{
		if (ref->len < match_len)
			return false;
		if (match_len <= 8)
			return true;
		lhs = VecStringRefDataPtr(*ref, arena_base);
		return lhs != nullptr && memcmp(lhs, match, match_len) == 0;
	}

	lhs = VecStringRefDataPtr(*ref, arena_base);
	if (lhs == nullptr)
		return false;

	if (string_type == BPCHAROID)
	{
		uint32_t lhs_len = pg_yaap_jit_trim_bpchar_length(lhs, ref->len);
		uint32_t rhs_len = pg_yaap_jit_trim_bpchar_length(match, match_len);

		if (lhs_len != rhs_len)
			return false;
		return lhs_len == 0 || memcmp(lhs, match, lhs_len) == 0;
	}

	if (ref->len != match_len)
		return false;
	if (match_len <= 8)
		return true;
	return memcmp(lhs, match, match_len) == 0;
}

static bool
pg_yaap_const_step_requires_wide_i128(const VecExprStep &step)
{
	int64_t expected_hi;

	if (step.opcode != VecOpCode::EEOP_CONST || !step.d.constant.has_wide_i128)
		return false;

	expected_hi = (step.d.constant.i64val < 0) ? -1 : 0;
	return step.d.constant.wide_lo != (uint64_t) step.d.constant.i64val ||
		(int64_t) step.d.constant.wide_hi != expected_hi;
}

struct ExprJitColumnBases
{
	std::array<LLVMValueRef, kMaxDeformTargets> f8;
	std::array<LLVMValueRef, kMaxDeformTargets> i64;
	std::array<LLVMValueRef, kMaxDeformTargets> i32;
	std::array<LLVMValueRef, kMaxDeformTargets> str;
	std::array<LLVMValueRef, kMaxDeformTargets> nulls;

	ExprJitColumnBases()
	{
		f8.fill(nullptr);
		i64.fill(nullptr);
		i32.fill(nullptr);
		str.fill(nullptr);
		nulls.fill(nullptr);
	}
};

static bool
pg_yaap_expr_opcode_supported(VecOpCode opcode)
{
	switch (opcode)
	{
		case VecOpCode::EEOP_VAR:
		case VecOpCode::EEOP_CONST:
		case VecOpCode::EEOP_FLOAT8_ADD:
		case VecOpCode::EEOP_FLOAT8_SUB:
		case VecOpCode::EEOP_FLOAT8_MUL:
		case VecOpCode::EEOP_INT64_ADD:
		case VecOpCode::EEOP_INT64_SUB:
		case VecOpCode::EEOP_INT64_MUL:
		case VecOpCode::EEOP_INT64_DIV_FLOAT8:
		case VecOpCode::EEOP_FLOAT8_LT:
		case VecOpCode::EEOP_FLOAT8_GT:
		case VecOpCode::EEOP_FLOAT8_LE:
		case VecOpCode::EEOP_FLOAT8_GE:
		case VecOpCode::EEOP_INT64_LT:
		case VecOpCode::EEOP_INT64_GT:
		case VecOpCode::EEOP_INT64_LE:
		case VecOpCode::EEOP_INT64_GE:
		case VecOpCode::EEOP_INT64_EQ:
		case VecOpCode::EEOP_INT64_NE:
		case VecOpCode::EEOP_DATE_LT:
		case VecOpCode::EEOP_DATE_LE:
		case VecOpCode::EEOP_DATE_GT:
		case VecOpCode::EEOP_DATE_GE:
		case VecOpCode::EEOP_AND:
		case VecOpCode::EEOP_OR:
		case VecOpCode::EEOP_NOT:
		case VecOpCode::EEOP_INT64_CASE:
		case VecOpCode::EEOP_FLOAT8_CASE:
		case VecOpCode::EEOP_STR_EQ:
		case VecOpCode::EEOP_STR_NE:
		case VecOpCode::EEOP_STR_PREFIX_LIKE:
		case VecOpCode::EEOP_QUAL:
			return true;
		default:
			return false;
	}
}

static ExprJitColumnBases
pg_yaap_preload_expr_column_bases(LLVMBuilderRef b,
								   LLVMTypeRef type_i8,
								   LLVMTypeRef type_i64,
								   LLVMTypeRef type_i32,
								   LLVMTypeRef type_stringref,
								   LLVMTypeRef type_double,
								   LLVMValueRef v_col_f8,
								   LLVMValueRef v_col_i64,
								   LLVMValueRef v_col_i32,
								   LLVMValueRef v_col_str,
								   LLVMValueRef v_col_nulls,
								   const VecExprProgram *program)
{
	ExprJitColumnBases bases;

	for (const auto &step : program->steps)
	{
		int att = -1;
		LLVMValueRef v_att;

		if (step.opcode == VecOpCode::EEOP_VAR)
			att = step.d.var.att_idx;
		else if (step.opcode == VecOpCode::EEOP_STR_EQ ||
				 step.opcode == VecOpCode::EEOP_STR_NE ||
				 step.opcode == VecOpCode::EEOP_STR_PREFIX_LIKE)
			att = step.d.str_prefix.att_idx;
		else
			continue;

		if (att < 0 || att >= kMaxDeformTargets)
			continue;

		v_att = LLVMConstInt(type_i64, att, false);
		if (bases.nulls[att] == nullptr)
			bases.nulls[att] = LLVMBuildLoad2(
				b,
				l_ptr(type_i8),
				LLVMBuildGEP2(b, l_ptr(type_i8), v_col_nulls, &v_att, 1, ""),
				"");

		if (step.opcode != VecOpCode::EEOP_VAR)
		{
			if (bases.str[att] == nullptr)
				bases.str[att] = LLVMBuildLoad2(
					b,
					l_ptr(type_stringref),
					LLVMBuildGEP2(b, l_ptr(type_stringref), v_col_str, &v_att, 1, ""),
					"");
			continue;
		}

		if (step.d.var.type == FLOAT8OID)
		{
			if (bases.f8[att] == nullptr)
				bases.f8[att] = LLVMBuildLoad2(
					b,
					l_ptr(type_double),
					LLVMBuildGEP2(b, l_ptr(type_double), v_col_f8, &v_att, 1, ""),
					"");
		}
		else if (step.d.var.type == NUMERICOID || step.d.var.type == INT8OID)
		{
			if (bases.i64[att] == nullptr)
				bases.i64[att] = LLVMBuildLoad2(
					b,
					l_ptr(type_i64),
					LLVMBuildGEP2(b, l_ptr(type_i64), v_col_i64, &v_att, 1, ""),
					"");
		}
		else
		{
			if (step.d.var.type == BPCHAROID ||
				step.d.var.type == TEXTOID ||
				step.d.var.type == VARCHAROID)
			{
				if (bases.str[att] == nullptr)
					bases.str[att] = LLVMBuildLoad2(
						b,
						l_ptr(type_stringref),
						LLVMBuildGEP2(b, l_ptr(type_stringref), v_col_str, &v_att, 1, ""),
						"");
			}
			else if (bases.i32[att] == nullptr)
				bases.i32[att] = LLVMBuildLoad2(
					b,
					l_ptr(type_i32),
					LLVMBuildGEP2(b, l_ptr(type_i32), v_col_i32, &v_att, 1, ""),
					"");
		}
	}

	return bases;
}

static LLVMValueRef
pg_yaap_build_rescale_int64(LLVMBuilderRef b,
							  LLVMTypeRef type_i64,
							  LLVMTypeRef type_i128,
							  LLVMValueRef value,
							  int from_scale,
							  int to_scale)
{
	if (value == nullptr || from_scale == to_scale)
		return value;

	if (from_scale < to_scale)
	{
		int64_t factor = pg_yaap_jit_pow10_int64(to_scale - from_scale);
		LLVMValueRef widened = LLVMBuildSExt(b, value, type_i128, "");
		LLVMValueRef scaled = LLVMBuildMul(b, widened,
										   pg_yaap_i128_const(type_i128, factor), "");
		return LLVMBuildTrunc(b, scaled, type_i64, "");
	}

	int64_t divisor = pg_yaap_jit_pow10_int64(from_scale - to_scale);
	int64_t halfway = divisor / 2;
	LLVMValueRef v_divisor = pg_yaap_i64_const(type_i64, divisor);
	LLVMValueRef v_halfway = pg_yaap_i64_const(type_i64, halfway);
	LLVMValueRef v_neg_halfway = pg_yaap_i64_const(type_i64, -halfway);
	LLVMValueRef quotient = LLVMBuildSDiv(b, value, v_divisor, "");
	LLVMValueRef remainder = LLVMBuildSRem(b, value, v_divisor, "");
	LLVMValueRef round_up = LLVMBuildICmp(b, LLVMIntSGE, remainder, v_halfway, "");
	LLVMValueRef round_down = LLVMBuildICmp(b, LLVMIntSLE, remainder, v_neg_halfway, "");
	LLVMValueRef plus_one = LLVMBuildAdd(b, quotient, pg_yaap_i64_const(type_i64, 1), "");
	LLVMValueRef minus_one = LLVMBuildSub(b, quotient, pg_yaap_i64_const(type_i64, 1), "");
	LLVMValueRef rounded = LLVMBuildSelect(b, round_up, plus_one, quotient, "");

	return LLVMBuildSelect(b, round_down, minus_one, rounded, "");
}

static LLVMValueRef
pg_yaap_build_rescale_compare_i128(LLVMBuilderRef b,
									 LLVMTypeRef type_i128,
									 LLVMValueRef value,
									 int from_scale,
									 int to_scale)
{
	LLVMValueRef widened = LLVMBuildSExt(b, value, type_i128, "");

	if (from_scale >= to_scale)
		return widened;

	return LLVMBuildMul(b,
						widened,
						pg_yaap_i128_const(type_i128,
											 pg_yaap_jit_pow10_int64(to_scale - from_scale)),
						"");
}

static LLVMValueRef
pg_yaap_build_rescale_compare_operand_i128(LLVMBuilderRef b,
											 LLVMTypeRef type_i128,
											 const VecExprStep *step,
											 LLVMValueRef value,
											 int from_scale,
											 int to_scale)
{
	LLVMValueRef widened;

	if (step != nullptr &&
		step->opcode == VecOpCode::EEOP_CONST &&
		step->d.constant.has_wide_i128)
	{
		widened = pg_yaap_i128_const_bits(type_i128,
											 step->d.constant.wide_lo,
											 (uint64_t) step->d.constant.wide_hi);
	}
	else
	{
		return pg_yaap_build_rescale_compare_i128(b, type_i128, value,
													 from_scale, to_scale);
	}
	if (from_scale >= to_scale)
		return widened;
	return LLVMBuildMul(b,
						widened,
						pg_yaap_i128_const(type_i128,
											 pg_yaap_jit_pow10_int64(to_scale - from_scale)),
						"");
}

static LLVMValueRef
pg_yaap_build_i32_truthy(LLVMBuilderRef b,
						   LLVMTypeRef type_i32,
						   LLVMValueRef is_null,
						   LLVMValueRef value)
{
	LLVMValueRef not_null = LLVMBuildNot(b, is_null, "");
	LLVMValueRef nonzero =
		LLVMBuildICmp(b, LLVMIntNE, value, pg_yaap_i32_const(type_i32, 0), "");

	return LLVMBuildAnd(b, not_null, nonzero, "");
}

static LLVMValueRef
pg_yaap_build_i32_falsey(LLVMBuilderRef b,
						   LLVMTypeRef type_i32,
						   LLVMValueRef is_null,
						   LLVMValueRef value)
{
	LLVMValueRef not_null = LLVMBuildNot(b, is_null, "");
	LLVMValueRef zero =
		LLVMBuildICmp(b, LLVMIntEQ, value, pg_yaap_i32_const(type_i32, 0), "");

	return LLVMBuildAnd(b, not_null, zero, "");
}

static void
pg_yaap_emit_bool_and(LLVMBuilderRef b,
						  LLVMTypeRef type_i32,
						  LLVMValueRef left_null,
						  LLVMValueRef left_val,
						  LLVMValueRef right_null,
						  LLVMValueRef right_val,
						  LLVMValueRef *out_null,
						  LLVMValueRef *out_val)
{
	LLVMValueRef left_true =
		pg_yaap_build_i32_truthy(b, type_i32, left_null, left_val);
	LLVMValueRef right_true =
		pg_yaap_build_i32_truthy(b, type_i32, right_null, right_val);
	LLVMValueRef left_false =
		pg_yaap_build_i32_falsey(b, type_i32, left_null, left_val);
	LLVMValueRef right_false =
		pg_yaap_build_i32_falsey(b, type_i32, right_null, right_val);
	LLVMValueRef result_false = LLVMBuildOr(b, left_false, right_false, "");
	LLVMValueRef result_true = LLVMBuildAnd(b, left_true, right_true, "");
	LLVMValueRef known = LLVMBuildOr(b, result_false, result_true, "");

	*out_null = LLVMBuildNot(b, known, "");
	*out_val = LLVMBuildZExt(b, result_true, type_i32, "");
}

static void
pg_yaap_emit_bool_or(LLVMBuilderRef b,
						 LLVMTypeRef type_i32,
						 LLVMValueRef left_null,
						 LLVMValueRef left_val,
						 LLVMValueRef right_null,
						 LLVMValueRef right_val,
						 LLVMValueRef *out_null,
						 LLVMValueRef *out_val)
{
	LLVMValueRef left_true =
		pg_yaap_build_i32_truthy(b, type_i32, left_null, left_val);
	LLVMValueRef right_true =
		pg_yaap_build_i32_truthy(b, type_i32, right_null, right_val);
	LLVMValueRef left_false =
		pg_yaap_build_i32_falsey(b, type_i32, left_null, left_val);
	LLVMValueRef right_false =
		pg_yaap_build_i32_falsey(b, type_i32, right_null, right_val);
	LLVMValueRef result_true = LLVMBuildOr(b, left_true, right_true, "");
	LLVMValueRef result_false = LLVMBuildAnd(b, left_false, right_false, "");
	LLVMValueRef known = LLVMBuildOr(b, result_false, result_true, "");

	*out_null = LLVMBuildNot(b, known, "");
	*out_val = LLVMBuildZExt(b, result_true, type_i32, "");
}

static LLVMValueRef
pg_yaap_emit_string_const_match(LLVMBuilderRef b,
									LLVMTypeRef type_i1,
									LLVMTypeRef type_i8,
									LLVMTypeRef type_i32,
									LLVMTypeRef type_i64,
									LLVMTypeRef type_sizet,
									LLVMTypeRef type_stringref,
									LLVMValueRef string_base,
									LLVMValueRef string_arena_base,
									LLVMValueRef row_idx_ext,
									const VecExprProgram *program,
									const VecExprStep &step)
{
	LLVMValueRef ref_ptr;
	LLVMValueRef ref_val;
	LLVMValueRef ref_len;
	LLVMValueRef ref_prefix;
	LLVMValueRef prefix_match;
	uint32_t match_len = step.d.str_prefix.len;
	uint64_t mask = 0;

	if (string_base == nullptr)
		return nullptr;

	ref_ptr = LLVMBuildGEP2(b, type_stringref, string_base, &row_idx_ext, 1, "");
	ref_val = LLVMBuildLoad2(b, type_stringref, ref_ptr, "");
	ref_len = LLVMBuildExtractValue(b, ref_val, 0, "str_len");
	ref_prefix = LLVMBuildExtractValue(b, ref_val, 2, "str_prefix");

	if (step.d.str_prefix.offset != UINT32_MAX)
	{
		LLVMTypeRef helper_arg_types[6] = {
			l_ptr(type_stringref),
			l_ptr(type_i8),
			l_ptr(type_i8),
			type_i32,
			type_i32,
			type_i1
		};
		LLVMTypeRef helper_ty =
			LLVMFunctionType(type_i1, helper_arg_types, lengthof(helper_arg_types), 0);
		const char *match_ptr = program->get_string_const_ptr(step.d.str_prefix.offset);
		LLVMValueRef v_helper =
			pg_yaap_l_ptr_const(type_sizet,
								  reinterpret_cast<void *>(&pg_yaap_jit_string_const_match_ref),
								  l_ptr(helper_ty));
		LLVMValueRef args[6];

		if (match_ptr == nullptr)
			return nullptr;

		args[0] = ref_ptr;
		args[1] = string_arena_base;
		args[2] = pg_yaap_l_ptr_const(type_sizet, const_cast<char *>(match_ptr), l_ptr(type_i8));
		args[3] = pg_yaap_i32_const(type_i32, match_len);
		args[4] = pg_yaap_i32_const(type_i32, (int32_t) step.d.str_prefix.type);
		args[5] = LLVMConstInt(type_i1, step.opcode == VecOpCode::EEOP_STR_PREFIX_LIKE, false);
		return LLVMBuildCall2(b, helper_ty, v_helper, args, lengthof(args), "str_match");
	}

	if (match_len > 0)
		mask = (match_len >= 8) ? UINT64_MAX : ((UINT64CONST(1) << (match_len * 8)) - 1);
	if (match_len == 0)
		prefix_match = LLVMConstInt(type_i1, 1, false);
	else
	{
		LLVMValueRef v_mask = LLVMConstInt(type_i64, mask, false);
		LLVMValueRef v_prefix =
			LLVMConstInt(type_i64, step.d.str_prefix.prefix & mask, false);
		LLVMValueRef lhs = LLVMBuildAnd(b, ref_prefix, v_mask, "");
		LLVMValueRef rhs = LLVMBuildAnd(b, v_prefix, v_mask, "");

		prefix_match = LLVMBuildICmp(b, LLVMIntEQ, lhs, rhs, "");
	}

	if (step.opcode == VecOpCode::EEOP_STR_PREFIX_LIKE)
	{
		LLVMValueRef len_ok =
			LLVMBuildICmp(b, LLVMIntUGE, ref_len, pg_yaap_i32_const(type_i32, match_len), "");

		return LLVMBuildAnd(b, len_ok, prefix_match, "");
	}

	if (step.d.str_prefix.type == BPCHAROID)
	{
		LLVMValueRef len_eq =
			LLVMBuildICmp(b, LLVMIntEQ, ref_len, pg_yaap_i32_const(type_i32, match_len), "");
		LLVMValueRef len_gt =
			LLVMBuildICmp(b, LLVMIntUGT, ref_len, pg_yaap_i32_const(type_i32, match_len), "");
		LLVMValueRef trailing_ok = LLVMConstInt(type_i1, 1, false);

		if (match_len < 8)
		{
			for (uint32_t j = match_len; j < 8; j++)
			{
				LLVMValueRef need_check =
					LLVMBuildICmp(b, LLVMIntUGT, ref_len, pg_yaap_i32_const(type_i32, j), "");
				LLVMValueRef shifted =
					LLVMBuildLShr(b, ref_prefix, LLVMConstInt(type_i64, j * 8, false), "");
				LLVMValueRef byte_val =
					LLVMBuildTrunc(b,
								   LLVMBuildAnd(b, shifted, LLVMConstInt(type_i64, 0xff, false), ""),
								   type_i8,
								   "");
				LLVMValueRef is_space =
					LLVMBuildICmp(b, LLVMIntEQ, byte_val,
								  LLVMConstInt(type_i8, ' ', false), "");
				LLVMValueRef ok =
					LLVMBuildOr(b, LLVMBuildNot(b, need_check, ""), is_space, "");

				trailing_ok = LLVMBuildAnd(b, trailing_ok, ok, "");
			}
		}

		return LLVMBuildAnd(
			b,
			prefix_match,
			LLVMBuildOr(b, len_eq, LLVMBuildAnd(b, len_gt, trailing_ok, ""), ""),
			"");
	}

	return LLVMBuildAnd(
		b,
		prefix_match,
		LLVMBuildICmp(b, LLVMIntEQ, ref_len, pg_yaap_i32_const(type_i32, match_len), ""),
		"");
}

static bool
pg_yaap_emit_expr_row(LLVMBuilderRef b,
						LLVMTypeRef type_i1,
						LLVMTypeRef type_i8,
						LLVMTypeRef type_i32,
						LLVMTypeRef type_i64,
						LLVMTypeRef type_i128,
						LLVMTypeRef type_sizet,
						LLVMTypeRef type_stringref,
						LLVMTypeRef type_double,
						const VecExprProgram *program,
						const ExprJitColumnBases &bases,
						LLVMValueRef v_string_arena_base,
						LLVMValueRef v_row_idx_ext,
						LLVMValueRef v_store_idx_ext,
						LLVMValueRef v_res_f8,
						LLVMValueRef v_res_i64,
						LLVMValueRef v_res_i32,
						LLVMValueRef v_res_nulls)
{
	std::vector<LLVMValueRef> reg_f8(program->max_reg_idx, nullptr);
	std::vector<LLVMValueRef> reg_i64(program->max_reg_idx, nullptr);
	std::vector<LLVMValueRef> reg_i32(program->max_reg_idx, nullptr);
	std::vector<LLVMValueRef> reg_null(program->max_reg_idx, nullptr);
	std::vector<const VecExprStep *> reg_defs(program->max_reg_idx, nullptr);

	for (const auto &step : program->steps)
	{
		if (step.res_idx >= 0 && step.res_idx < program->max_reg_idx)
			reg_defs[step.res_idx] = &step;
	}

	for (const auto &step : program->steps)
	{
		int res = step.res_idx;
		int l = step.d.op.left;
		int r = step.d.op.right;
		int left_scale = program->get_register_scale(l);
		int right_scale = program->get_register_scale(r);
		int res_scale = program->get_register_scale(res);

		switch (step.opcode)
		{
			case VecOpCode::EEOP_VAR:
			{
				int att = step.d.var.att_idx;

				if (att < 0 || att >= kMaxDeformTargets || bases.nulls[att] == nullptr)
					return false;

				reg_null[res] = LLVMBuildTrunc(
					b,
					LLVMBuildLoad2(b, type_i8,
								   LLVMBuildGEP2(b, type_i8, bases.nulls[att], &v_row_idx_ext, 1, ""),
								   ""),
					type_i1,
					"");

				if (step.d.var.type == FLOAT8OID)
				{
					if (bases.f8[att] == nullptr)
						return false;
					reg_f8[res] = LLVMBuildLoad2(
						b, type_double,
						LLVMBuildGEP2(b, type_double, bases.f8[att], &v_row_idx_ext, 1, ""),
						"");
				}
				else if (step.d.var.type == NUMERICOID || step.d.var.type == INT8OID)
				{
					if (bases.i64[att] == nullptr)
						return false;
					reg_i64[res] = LLVMBuildLoad2(
						b, type_i64,
						LLVMBuildGEP2(b, type_i64, bases.i64[att], &v_row_idx_ext, 1, ""),
						"");
				}
				else
				{
					LLVMValueRef value_i32;

					if (bases.i32[att] == nullptr)
						return false;
					value_i32 = LLVMBuildLoad2(
						b, type_i32,
						LLVMBuildGEP2(b, type_i32, bases.i32[att], &v_row_idx_ext, 1, ""),
						"");
					reg_i32[res] = value_i32;
					if (step.d.var.type == INT2OID || step.d.var.type == INT4OID)
						reg_i64[res] = LLVMBuildSExt(b, value_i32, type_i64, "");
				}
				break;
			}
			case VecOpCode::EEOP_CONST:
				reg_null[res] = LLVMConstInt(type_i1, step.d.constant.isnull, false);
				reg_f8[res] = LLVMConstReal(type_double, step.d.constant.fval);
				reg_i64[res] = pg_yaap_i64_const(type_i64, step.d.constant.i64val);
				reg_i32[res] = pg_yaap_i32_const(type_i32, step.d.constant.ival);
				break;
			case VecOpCode::EEOP_FLOAT8_ADD:
				if (!reg_null[l] || !reg_null[r] || !reg_f8[l] || !reg_f8[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				reg_f8[res] = LLVMBuildFAdd(b, reg_f8[l], reg_f8[r], "");
				break;
			case VecOpCode::EEOP_FLOAT8_SUB:
				if (!reg_null[l] || !reg_null[r] || !reg_f8[l] || !reg_f8[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				reg_f8[res] = LLVMBuildFSub(b, reg_f8[l], reg_f8[r], "");
				break;
			case VecOpCode::EEOP_FLOAT8_MUL:
				if (!reg_null[l] || !reg_null[r] || !reg_f8[l] || !reg_f8[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				reg_f8[res] = LLVMBuildFMul(b, reg_f8[l], reg_f8[r], "");
				break;
			case VecOpCode::EEOP_FLOAT8_LT:
				if (!reg_null[l] || !reg_null[r] || !reg_f8[l] || !reg_f8[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				reg_i32[res] = LLVMBuildZExt(
					b, LLVMBuildFCmp(b, LLVMRealULT, reg_f8[l], reg_f8[r], ""), type_i32, "");
				break;
			case VecOpCode::EEOP_FLOAT8_LE:
				if (!reg_null[l] || !reg_null[r] || !reg_f8[l] || !reg_f8[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				reg_i32[res] = LLVMBuildZExt(
					b, LLVMBuildFCmp(b, LLVMRealULE, reg_f8[l], reg_f8[r], ""), type_i32, "");
				break;
			case VecOpCode::EEOP_FLOAT8_GT:
				if (!reg_null[l] || !reg_null[r] || !reg_f8[l] || !reg_f8[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				reg_i32[res] = LLVMBuildZExt(
					b, LLVMBuildFCmp(b, LLVMRealUGT, reg_f8[l], reg_f8[r], ""), type_i32, "");
				break;
			case VecOpCode::EEOP_FLOAT8_GE:
				if (!reg_null[l] || !reg_null[r] || !reg_f8[l] || !reg_f8[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				reg_i32[res] = LLVMBuildZExt(
					b, LLVMBuildFCmp(b, LLVMRealUGE, reg_f8[l], reg_f8[r], ""), type_i32, "");
				break;
			case VecOpCode::EEOP_INT64_ADD:
			{
				LLVMValueRef left_val;
				LLVMValueRef right_val;

				if (!reg_null[l] || !reg_null[r] || !reg_i64[l] || !reg_i64[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				left_val = pg_yaap_build_rescale_int64(b, type_i64, type_i128, reg_i64[l],
														 left_scale, res_scale);
				right_val = pg_yaap_build_rescale_int64(b, type_i64, type_i128, reg_i64[r],
														  right_scale, res_scale);
				reg_i64[res] = LLVMBuildAdd(b, left_val, right_val, "");
				break;
			}
			case VecOpCode::EEOP_INT64_SUB:
			{
				LLVMValueRef left_val;
				LLVMValueRef right_val;

				if (!reg_null[l] || !reg_null[r] || !reg_i64[l] || !reg_i64[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				left_val = pg_yaap_build_rescale_int64(b, type_i64, type_i128, reg_i64[l],
														 left_scale, res_scale);
				right_val = pg_yaap_build_rescale_int64(b, type_i64, type_i128, reg_i64[r],
														  right_scale, res_scale);
				reg_i64[res] = LLVMBuildSub(b, left_val, right_val, "");
				break;
			}
			case VecOpCode::EEOP_INT64_MUL:
			{
				LLVMValueRef wide_left;
				LLVMValueRef wide_right;

				if (!reg_null[l] || !reg_null[r] || !reg_i64[l] || !reg_i64[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				wide_left = LLVMBuildSExt(b, reg_i64[l], type_i128, "");
				wide_right = LLVMBuildSExt(b, reg_i64[r], type_i128, "");
				reg_i64[res] = LLVMBuildTrunc(
					b, LLVMBuildMul(b, wide_left, wide_right, ""), type_i64, "");
				break;
			}
			case VecOpCode::EEOP_INT64_DIV_FLOAT8:
			{
				LLVMValueRef left_val;
				LLVMValueRef right_val;

				if (!reg_null[l] || !reg_null[r] || !reg_i64[l] || !reg_i64[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				left_val = LLVMBuildSIToFP(b, reg_i64[l], type_double, "");
				right_val = LLVMBuildSIToFP(b, reg_i64[r], type_double, "");
				if (left_scale > 0)
					left_val = LLVMBuildFDiv(
						b, left_val,
						LLVMConstReal(type_double, (double) pg_yaap_jit_pow10_int64(left_scale)),
						"");
				if (right_scale > 0)
					right_val = LLVMBuildFDiv(
						b, right_val,
						LLVMConstReal(type_double, (double) pg_yaap_jit_pow10_int64(right_scale)),
						"");
				reg_f8[res] = LLVMBuildFDiv(b, left_val, right_val, "");
				break;
			}
			case VecOpCode::EEOP_INT64_LT:
			case VecOpCode::EEOP_INT64_LE:
			case VecOpCode::EEOP_INT64_GT:
			case VecOpCode::EEOP_INT64_GE:
			case VecOpCode::EEOP_INT64_EQ:
			case VecOpCode::EEOP_INT64_NE:
			{
				LLVMIntPredicate pred;
				LLVMValueRef left_val;
				LLVMValueRef right_val;
				int cmp_scale;

				if (!reg_null[l] || !reg_null[r] || !reg_i64[l] || !reg_i64[r])
					return false;

				cmp_scale = Max(left_scale, right_scale);
				left_val = pg_yaap_build_rescale_compare_operand_i128(
					b, type_i128, reg_defs[l], reg_i64[l], left_scale, cmp_scale);
				right_val = pg_yaap_build_rescale_compare_operand_i128(
					b, type_i128, reg_defs[r], reg_i64[r], right_scale, cmp_scale);
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				pred = LLVMIntSLT;
				if (step.opcode == VecOpCode::EEOP_INT64_LE)
					pred = LLVMIntSLE;
				else if (step.opcode == VecOpCode::EEOP_INT64_GT)
					pred = LLVMIntSGT;
				else if (step.opcode == VecOpCode::EEOP_INT64_GE)
					pred = LLVMIntSGE;
				else if (step.opcode == VecOpCode::EEOP_INT64_EQ)
					pred = LLVMIntEQ;
				else if (step.opcode == VecOpCode::EEOP_INT64_NE)
					pred = LLVMIntNE;
				reg_i32[res] = LLVMBuildZExt(
					b, LLVMBuildICmp(b, pred, left_val, right_val, ""), type_i32, "");
				break;
			}
			case VecOpCode::EEOP_DATE_LT:
			case VecOpCode::EEOP_DATE_LE:
			case VecOpCode::EEOP_DATE_GT:
			case VecOpCode::EEOP_DATE_GE:
			{
				LLVMIntPredicate pred = LLVMIntSLT;

				if (!reg_null[l] || !reg_null[r] || !reg_i32[l] || !reg_i32[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				if (step.opcode == VecOpCode::EEOP_DATE_LE)
					pred = LLVMIntSLE;
				else if (step.opcode == VecOpCode::EEOP_DATE_GT)
					pred = LLVMIntSGT;
				else if (step.opcode == VecOpCode::EEOP_DATE_GE)
					pred = LLVMIntSGE;
				reg_i32[res] = LLVMBuildZExt(
					b, LLVMBuildICmp(b, pred, reg_i32[l], reg_i32[r], ""), type_i32, "");
				break;
			}
			case VecOpCode::EEOP_AND:
				if (!reg_null[l] || !reg_null[r] || !reg_i32[l] || !reg_i32[r])
					return false;
				pg_yaap_emit_bool_and(b, type_i32,
										 reg_null[l], reg_i32[l],
										 reg_null[r], reg_i32[r],
										 &reg_null[res], &reg_i32[res]);
				break;
			case VecOpCode::EEOP_OR:
				if (!reg_null[l] || !reg_null[r] || !reg_i32[l] || !reg_i32[r])
					return false;
				pg_yaap_emit_bool_or(b, type_i32,
										reg_null[l], reg_i32[l],
										reg_null[r], reg_i32[r],
										&reg_null[res], &reg_i32[res]);
				break;
			case VecOpCode::EEOP_NOT:
			{
				if (!reg_null[l] || !reg_i32[l])
					return false;
				reg_null[res] = reg_null[l];
				LLVMValueRef val_i1 = LLVMBuildTrunc(b, reg_i32[l], type_i1, "");
				LLVMValueRef negated = LLVMBuildNot(b, val_i1, "");
				reg_i32[res] = LLVMBuildZExt(b, negated, type_i32, "");
				break;
			}
			case VecOpCode::EEOP_INT64_CASE:
			{
				int c = step.d.ternary.cond;
				int t = step.d.ternary.if_true;
				int f = step.d.ternary.if_false;
				int true_scale = program->get_register_scale(t);
				int false_scale = program->get_register_scale(f);
				LLVMValueRef cond_true;
				LLVMValueRef true_val;
				LLVMValueRef false_val;

				if (!reg_null[c] || !reg_i32[c] || !reg_null[t] || !reg_null[f] ||
					!reg_i64[t] || !reg_i64[f])
					return false;
				cond_true = pg_yaap_build_i32_truthy(b, type_i32,
														 reg_null[c], reg_i32[c]);
				true_val = pg_yaap_build_rescale_int64(b, type_i64, type_i128,
														 reg_i64[t], true_scale, res_scale);
				false_val = pg_yaap_build_rescale_int64(b, type_i64, type_i128,
														  reg_i64[f], false_scale, res_scale);
				reg_null[res] = LLVMBuildSelect(b, cond_true, reg_null[t], reg_null[f], "");
				reg_i64[res] = LLVMBuildSelect(b, cond_true, true_val, false_val, "");
				break;
			}
			case VecOpCode::EEOP_FLOAT8_CASE:
			{
				int c = step.d.ternary.cond;
				int t = step.d.ternary.if_true;
				int f = step.d.ternary.if_false;
				LLVMValueRef cond_true;

				if (!reg_null[c] || !reg_i32[c] || !reg_null[t] || !reg_null[f] ||
					!reg_f8[t] || !reg_f8[f])
					return false;
				cond_true = pg_yaap_build_i32_truthy(b, type_i32,
														 reg_null[c], reg_i32[c]);
				reg_null[res] = LLVMBuildSelect(b, cond_true, reg_null[t], reg_null[f], "");
				reg_f8[res] = LLVMBuildSelect(b, cond_true, reg_f8[t], reg_f8[f], "");
				break;
			}
			case VecOpCode::EEOP_STR_PREFIX_LIKE:
			case VecOpCode::EEOP_STR_EQ:
			case VecOpCode::EEOP_STR_NE:
			{
				int att = step.d.str_prefix.att_idx;
				LLVMValueRef matches;

				if (att < 0 || att >= kMaxDeformTargets ||
					bases.str[att] == nullptr || bases.nulls[att] == nullptr)
					return false;
				reg_null[res] = LLVMBuildTrunc(
					b,
					LLVMBuildLoad2(b, type_i8,
								   LLVMBuildGEP2(b, type_i8, bases.nulls[att], &v_row_idx_ext, 1, ""),
								   ""),
					type_i1,
					"");
				matches = pg_yaap_emit_string_const_match(
					b, type_i1, type_i8, type_i32, type_i64, type_sizet, type_stringref,
					bases.str[att], v_string_arena_base, v_row_idx_ext, program, step);
				if (matches == nullptr)
					return false;
				if (step.opcode == VecOpCode::EEOP_STR_NE)
					matches = LLVMBuildNot(b, matches, "");
				reg_i32[res] = LLVMBuildZExt(b, matches, type_i32, "");
				break;
			}
			case VecOpCode::EEOP_QUAL:
				break;
			default:
				return false;
		}
	}

	if (program->final_res_idx < 0 || program->final_res_idx >= program->max_reg_idx)
		return false;

	int fres = program->final_res_idx;
	if (reg_null[fres] == nullptr)
		return false;

	LLVMBuildStore(
		b,
		LLVMBuildZExt(b, reg_null[fres], type_i8, ""),
		LLVMBuildGEP2(b, type_i8, v_res_nulls, &v_store_idx_ext, 1, ""));
	if (reg_f8[fres] != nullptr)
		LLVMBuildStore(
			b, reg_f8[fres],
			LLVMBuildGEP2(b, type_double, v_res_f8, &v_store_idx_ext, 1, ""));
	if (reg_i64[fres] != nullptr)
		LLVMBuildStore(
			b, reg_i64[fres],
			LLVMBuildGEP2(b, type_i64, v_res_i64, &v_store_idx_ext, 1, ""));
	if (reg_i32[fres] != nullptr)
		LLVMBuildStore(
			b, reg_i32[fres],
			LLVMBuildGEP2(b, type_i32, v_res_i32, &v_store_idx_ext, 1, ""));

	return true;
}

static LLVMValueRef
compile_expr_to_jit(LLVMJitContext *context,
				   const VecExprProgram *program,
				   const char *funcname)
{
	LLVMModuleRef mod = pg_llvm_mutable_module(context);
	LLVMContextRef lc = LLVMGetModuleContext(mod);
	LLVMBuilderRef b = LLVMCreateBuilderInContext(lc);

	LLVMTypeRef type_i1 = LLVMInt1TypeInContext(lc);
	LLVMTypeRef type_i8 = LLVMInt8TypeInContext(lc);
	LLVMTypeRef type_i16 = LLVMInt16TypeInContext(lc);
	LLVMTypeRef type_i32 = LLVMInt32TypeInContext(lc);
	LLVMTypeRef type_i64 = LLVMInt64TypeInContext(lc);
	LLVMTypeRef type_i128 = LLVMIntTypeInContext(lc, 128);
	LLVMTypeRef type_sizet = LLVMIntTypeInContext(lc, sizeof(size_t) * 8);
	LLVMTypeRef type_double = LLVMDoubleTypeInContext(lc);
	LLVMTypeRef type_stringref_members[3] = {type_i32, type_i32, type_i64};
	LLVMTypeRef type_stringref = LLVMStructTypeInContext(lc, type_stringref_members, 3, false);
	LLVMTypeRef param_types[13];
	param_types[0] = type_i32;
	param_types[1] = l_ptr(l_ptr(type_double));
	param_types[2] = l_ptr(l_ptr(type_i64));
	param_types[3] = l_ptr(l_ptr(type_i32));
	param_types[4] = l_ptr(l_ptr(type_stringref));
	param_types[5] = l_ptr(l_ptr(type_i8));
	param_types[6] = l_ptr(type_i8);
	param_types[7] = l_ptr(type_double);
	param_types[8] = l_ptr(type_i64);
	param_types[9] = l_ptr(type_i32);
	param_types[10] = l_ptr(type_i8);
	param_types[11] = l_ptr(type_i16);
	param_types[12] = type_i1;

	LLVMTypeRef func_sig = LLVMFunctionType(LLVMVoidTypeInContext(lc), param_types, 13, 0);
	LLVMValueRef v_func = LLVMAddFunction(mod, funcname, func_sig);
	LLVMValueRef v_count = LLVMGetParam(v_func, 0);
	LLVMValueRef v_col_f8 = LLVMGetParam(v_func, 1);
	LLVMValueRef v_col_i64 = LLVMGetParam(v_func, 2);
	LLVMValueRef v_col_i32 = LLVMGetParam(v_func, 3);
	LLVMValueRef v_col_str = LLVMGetParam(v_func, 4);
	LLVMValueRef v_col_nulls = LLVMGetParam(v_func, 5);
	LLVMValueRef v_string_arena_base = LLVMGetParam(v_func, 6);
	LLVMValueRef v_res_f8 = LLVMGetParam(v_func, 7);
	LLVMValueRef v_res_i64 = LLVMGetParam(v_func, 8);
	LLVMValueRef v_res_i32 = LLVMGetParam(v_func, 9);
	LLVMValueRef v_res_nulls = LLVMGetParam(v_func, 10);
	LLVMValueRef v_sel = LLVMGetParam(v_func, 11);
	LLVMValueRef v_has_sel = LLVMGetParam(v_func, 12);
	ExprJitColumnBases bases;
	LLVMBasicBlockRef b_entry;
	LLVMBasicBlockRef b_dense_cond;
	LLVMBasicBlockRef b_dense_body;
	LLVMBasicBlockRef b_selected_cond;
	LLVMBasicBlockRef b_selected_body;
	LLVMBasicBlockRef b_exit;
	LLVMValueRef v_dense_idx;
	LLVMValueRef v_selected_idx;
	LLVMValueRef v_zero = pg_yaap_i32_const(type_i32, 0);

	for (const auto &step : program->steps)
	{
		if (pg_yaap_const_step_requires_wide_i128(step))
		{
			LLVMDisposeBuilder(b);
			return nullptr;
		}
		if (!pg_yaap_expr_opcode_supported(step.opcode))
		{
			LLVMDisposeBuilder(b);
			return nullptr;
		}
	}

	b_entry = LLVMAppendBasicBlockInContext(lc, v_func, "entry");
	b_dense_cond = LLVMAppendBasicBlockInContext(lc, v_func, "dense_cond");
	b_dense_body = LLVMAppendBasicBlockInContext(lc, v_func, "dense_body");
	b_selected_cond = LLVMAppendBasicBlockInContext(lc, v_func, "selected_cond");
	b_selected_body = LLVMAppendBasicBlockInContext(lc, v_func, "selected_body");
	b_exit = LLVMAppendBasicBlockInContext(lc, v_func, "exit");
	LLVMPositionBuilderAtEnd(b, b_entry);
	bases = pg_yaap_preload_expr_column_bases(b, type_i8, type_i64, type_i32,
												type_stringref, type_double, v_col_f8, v_col_i64,
												v_col_i32, v_col_str, v_col_nulls, program);
	LLVMBuildCondBr(b, v_has_sel, b_selected_cond, b_dense_cond);

	LLVMPositionBuilderAtEnd(b, b_dense_cond);
	v_dense_idx = LLVMBuildPhi(b, type_i32, "dense_idx");
	LLVMAddIncoming(v_dense_idx, &v_zero, &b_entry, 1);
	LLVMBuildCondBr(
		b,
		LLVMBuildICmp(b, LLVMIntULT, v_dense_idx, v_count, ""),
		b_dense_body,
		b_exit);

	LLVMPositionBuilderAtEnd(b, b_dense_body);
	{
		LLVMValueRef v_dense_idx_ext = LLVMBuildZExt(b, v_dense_idx, type_i64, "");
		LLVMValueRef v_dense_next;

		if (!pg_yaap_emit_expr_row(b, type_i1, type_i8, type_i32, type_i64, type_i128,
									 type_sizet,
									 type_stringref,
									 type_double, program, bases, v_string_arena_base, v_dense_idx_ext,
									 v_dense_idx_ext, v_res_f8, v_res_i64, v_res_i32,
									 v_res_nulls))
		{
			LLVMDisposeBuilder(b);
			return nullptr;
		}

		v_dense_next = LLVMBuildAdd(b, v_dense_idx, pg_yaap_i32_const(type_i32, 1), "");
		LLVMBuildBr(b, b_dense_cond);
		LLVMAddIncoming(v_dense_idx, &v_dense_next, &b_dense_body, 1);
	}

	LLVMPositionBuilderAtEnd(b, b_selected_cond);
	v_selected_idx = LLVMBuildPhi(b, type_i32, "selected_idx");
	LLVMAddIncoming(v_selected_idx, &v_zero, &b_entry, 1);
	LLVMBuildCondBr(
		b,
		LLVMBuildICmp(b, LLVMIntULT, v_selected_idx, v_count, ""),
		b_selected_body,
		b_exit);

	LLVMPositionBuilderAtEnd(b, b_selected_body);
	{
		LLVMValueRef v_selected_idx_ext = LLVMBuildZExt(b, v_selected_idx, type_i64, "");
		LLVMValueRef v_sel_val = LLVMBuildLoad2(
			b, type_i16, LLVMBuildGEP2(b, type_i16, v_sel, &v_selected_idx_ext, 1, ""), "");
		LLVMValueRef v_row_idx = LLVMBuildZExt(b, v_sel_val, type_i32, "");
		LLVMValueRef v_row_idx_ext = LLVMBuildZExt(b, v_row_idx, type_i64, "");
		LLVMValueRef v_selected_next;

		if (!pg_yaap_emit_expr_row(b, type_i1, type_i8, type_i32, type_i64, type_i128,
									 type_sizet,
									 type_stringref,
									 type_double, program, bases, v_string_arena_base, v_row_idx_ext,
									 v_row_idx_ext, v_res_f8, v_res_i64, v_res_i32,
									 v_res_nulls))
		{
			LLVMDisposeBuilder(b);
			return nullptr;
		}

		v_selected_next = LLVMBuildAdd(
			b, v_selected_idx, pg_yaap_i32_const(type_i32, 1), "");
		LLVMBuildBr(b, b_selected_cond);
		LLVMAddIncoming(v_selected_idx, &v_selected_next, &b_selected_body, 1);
	}

	LLVMPositionBuilderAtEnd(b, b_exit);
	LLVMBuildRetVoid(b);
	LLVMDisposeBuilder(b);
	return v_func;
}

bool pg_yaap_try_compile_jit_expr(const VecExprProgram *program, VecExprJitFunc *out_func, JitContext **out_context, const char **failure_reason)
{
	if (!load_jit_symbols(failure_reason))
		return false;
	LLVMJitContext *ctx = (LLVMJitContext *) pg_llvm_create_context(PGJIT_PERFORM | PGJIT_OPT3);
	char base_name[96];
	char *funcname;
	LLVMValueRef fn = nullptr;
	bool success = false;

	snprintf(base_name, sizeof(base_name), "pg_yaap_jit_expr_%p", (const void *) program);
	funcname = pg_llvm_expand_funcname(ctx, base_name);
	PG_TRY();
	{
		fn = compile_expr_to_jit(ctx, program, funcname);
		if (!fn)
		{
			if (failure_reason != nullptr && *failure_reason == nullptr)
				*failure_reason = "expression JIT lowering rejected the program";
		}
		else
		{
			*out_func = (VecExprJitFunc) pg_llvm_get_function(ctx, funcname);
			*out_context = &ctx->base;
			pg_yaap_register_llvm_jit_context(&ctx->base);
			success = true;
		}
	}
	PG_CATCH();
	{
		pg_llvm_release_context_direct(ctx);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!success)
		pg_llvm_release_context_direct(ctx);
	return success;
}

} /* namespace pg_yaap */
#endif
