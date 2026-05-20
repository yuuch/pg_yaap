extern "C" {
#include "postgres.h"

#ifdef USE_LLVM

#include <llvm-c/Core.h>

#include "access/htup_details.h"
#include "access/tupmacs.h"
#include "access/tupdesc_details.h"
#include "fmgr.h"
#include "jit/jit.h"
#include "jit/llvmjit.h"
#include "jit/llvmjit_emit.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "varatt.h"
#endif
}

#include "yaap_engine.hpp"
#include "llvmjit_deform_datachunk.h"

#ifdef USE_LLVM

#include <dlfcn.h>
#include <cstdio>
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
static uint64_t pg_yaap_deform_jit_serial = 0;

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
	{
		elog(LOG, "pg_yaap: deform JIT pid=%d reusing already-resolved LLVM entry points",
			 MyProcPid);
		return true;
	}
	if (resolve_jit_symbols_from_process())
	{
		elog(LOG, "pg_yaap: deform JIT pid=%d resolved LLVM entry points from process",
			 MyProcPid);
		return true;
	}

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
	{
		elog(LOG, "pg_yaap: deform JIT pid=%d loaded provider %s and resolved LLVM entry points",
			 MyProcPid, provider_name);
		return true;
	}
	if (resolve_jit_symbols_from_provider(provider_path))
	{
		elog(LOG, "pg_yaap: deform JIT pid=%d resolved LLVM entry points from provider handle %s",
			 MyProcPid, provider_name);
		return true;
	}

	if (failure_reason != nullptr)
		*failure_reason = "loaded JIT provider but LLVM entry points are unavailable";
	return false;
}

static inline LLVMValueRef
pg_yaap_l_sizet_const(LLVMTypeRef type_sizet, size_t i)
{
	return LLVMConstInt(type_sizet, i, false);
}

static inline LLVMValueRef
pg_yaap_l_ptr_const(LLVMTypeRef type_sizet, void *ptr, LLVMTypeRef type)
{
	LLVMValueRef c = LLVMConstInt(type_sizet, (uintptr_t) ptr, false);
	return LLVMConstIntToPtr(c, type);
}

static bool
pg_yaap_jit_deform_supported(TupleDesc desc,
							  const DeformProgram *program,
							  const char **failure_reason)
{
	if (desc == nullptr || program == nullptr) {
		if (failure_reason != nullptr)
			*failure_reason = "missing tuple descriptor or deform program";
		return false;
	}
	if (program->ntargets <= 0)
		return false;
	if (program->last_att_index < 0 || program->last_att_index >= desc->natts) {
		if (failure_reason != nullptr)
			*failure_reason = "deform program references attribute outside tuple descriptor";
		return false;
	}
	for (int i = 0; i < program->ntargets; i++) {
		const DeformTarget &target = program->targets[i];
		CompactAttribute *att = TupleDescCompactAttr(desc, target.att_index);

		if (i > 0 && program->targets[i - 1].att_index > target.att_index) {
			if (failure_reason != nullptr)
				*failure_reason = "deform targets must be sorted by attribute number";
			return false;
		}
		if (target.att_index >= kMaxDeformTargets || target.dst_col >= kMaxDeformTargets) {
			if (failure_reason != nullptr)
				*failure_reason = "deform target exceeds fixed column limit";
			return false;
		}
		if (att->attisdropped) {
			if (failure_reason != nullptr)
				*failure_reason = "dropped attributes are not supported by deform JIT";
			return false;
		}
		/* atthasmissing is intentionally not rejected here. The JIT-generated code
		 * has attunavail blocks that null out targets for short tuples. */
			if (att->attlen == -2) {
				if (failure_reason != nullptr)
					*failure_reason = "cstring attributes are not supported by deform JIT";
				return false;
			}
		}
		return true;
	}

static int64_t fast_numeric_to_int64_v8(Numeric num) {
	int64_t scaled = 0;

	if (!TryFastNumericToScaledInt64(PointerGetDatum(num), DEFAULT_NUMERIC_SCALE, &scaled))
		elog(ERROR, "pg_yaap fast numeric decode failed inside deform JIT helper");
	return scaled;
}

static void pg_yaap_jit_store_numeric_int64_fast(const char *ptr, int64_t *dst) {
	Numeric num = (Numeric) ptr;
	*dst = fast_numeric_to_int64_v8(num);
}

static void pg_yaap_jit_store_stringref(const char *ptr, void *dst) {
	struct varlena *v = (struct varlena *) ptr;
	char *vptr = VARDATA_ANY(v);
	int len = VARSIZE_ANY_EXHDR(v);
	uint64_t pref = 0;
	if (len > 0)
		memcpy(&pref, vptr, len > 8 ? 8 : len);
	((uint32_t*)dst)[0] = len;
	((uint32_t*)dst)[1] = 0;
	((uint64_t*)((char*)dst + 8))[0] = pref;
}

static void
pg_yaap_jit_store_stringref_owned(const char *ptr, void *dst, void *chunk_ptr)
{
	DataChunk<DEFAULT_CHUNK_SIZE> *chunk =
		reinterpret_cast<DataChunk<DEFAULT_CHUNK_SIZE> *>(chunk_ptr);
	VecStringRef ref;
	struct varlena *v;
	char *vptr;
	int len;

	if (chunk == nullptr)
	{
		pg_yaap_jit_store_stringref(ptr, dst);
		return;
	}

	v = (struct varlena *) ptr;
	vptr = VARDATA_ANY(v);
	len = VARSIZE_ANY_EXHDR(v);
	ref = chunk->store_string_bytes(vptr, (uint32_t) len);
	memcpy(dst, &ref, sizeof(ref));
}

/*
 * Bug G: BPCHAR(1) payload lives behind the varlena header; emitting the raw
 * datum byte yields garbage and breaks HashAgg dedupe. This helper performs
 * the varlena header skip that the JIT switch case for kBpchar1 calls into,
 * so that l_returnflag / l_linestatus reach the int32 column as the actual
 * character byte.
 */
static void pg_yaap_jit_store_bpchar1_int32(const char *ptr, void *dst)
{
	struct varlena *v = (struct varlena *) ptr;
	const char *vptr = VARDATA_ANY(v);
	int len = VARSIZE_ANY_EXHDR(v);
	int32_t ch = (len > 0) ? (int32_t) (uint8_t) vptr[0] : 0;
	*((int32_t *) dst) = ch;
}

static LLVMValueRef
build_numeric_scale2_fast_store_helper(LLVMModuleRef mod,
										 LLVMContextRef lc,
										 LLVMTypeRef num_fn_ty,
										 LLVMValueRef v_num_fallback_fn,
										 const char *parent_funcname)
{
	LLVMTypeRef type_i1 = LLVMInt1TypeInContext(lc);
	LLVMTypeRef type_i8 = LLVMInt8TypeInContext(lc);
	LLVMTypeRef type_i16 = LLVMInt16TypeInContext(lc);
	LLVMTypeRef type_i32 = LLVMInt32TypeInContext(lc);
	LLVMTypeRef type_i64 = LLVMInt64TypeInContext(lc);
	LLVMBuilderRef b = LLVMCreateBuilderInContext(lc);
	LLVMValueRef v_func;
	LLVMValueRef v_ptr;
	LLVMValueRef v_dst;
	LLVMValueRef payload_ptr_p;
	LLVMValueRef payload_size_p;
	LLVMValueRef digits_ptr_p;
	LLVMValueRef ndigits_p;
	LLVMValueRef weight_p;
	LLVMValueRef negative_p;
	LLVMValueRef accum_p;
	LLVMValueRef idx_p;
	LLVMValueRef frac_digit_p;
	LLVMBasicBlockRef b_entry;
	LLVMBasicBlockRef b_oneb;
	LLVMBasicBlockRef b_fourb;
	LLVMBasicBlockRef b_parse;
	LLVMBasicBlockRef b_parse_dispatch;
	LLVMBasicBlockRef b_short;
	LLVMBasicBlockRef b_long;
	LLVMBasicBlockRef b_decode_init;
	LLVMBasicBlockRef b_int_cond;
	LLVMBasicBlockRef b_int_body;
	LLVMBasicBlockRef b_int_load_digit;
	LLVMBasicBlockRef b_int_latch;
	LLVMBasicBlockRef b_frac_prep;
	LLVMBasicBlockRef b_frac_load;
	LLVMBasicBlockRef b_frac_check;
	LLVMBasicBlockRef b_trailing_cond;
	LLVMBasicBlockRef b_trailing_body;
	LLVMBasicBlockRef b_store;
	LLVMBasicBlockRef b_fallback;
	char helper_name[128];

	auto load_i8 = [&](LLVMValueRef ptr, const char *name) {
		LLVMValueRef v = LLVMBuildLoad2(b, type_i8, ptr, name);
		LLVMSetAlignment(v, 1);
		return v;
	};
	auto load_i16_from_i8 = [&](LLVMValueRef ptr, const char *name) {
		LLVMValueRef p = LLVMBuildBitCast(b, ptr, l_ptr(type_i16), "");
		LLVMValueRef v = LLVMBuildLoad2(b, type_i16, p, name);
		LLVMSetAlignment(v, 1);
		return v;
	};
	auto load_i32_from_i8 = [&](LLVMValueRef ptr, const char *name) {
		LLVMValueRef p = LLVMBuildBitCast(b, ptr, l_ptr(type_i32), "");
		LLVMValueRef v = LLVMBuildLoad2(b, type_i32, p, name);
		LLVMSetAlignment(v, 1);
		return v;
	};
	auto gep_i8 = [&](LLVMValueRef base, LLVMValueRef idx, const char *name) {
		LLVMValueRef idxs[1] = {idx};
		return LLVMBuildGEP2(b, type_i8, base, idxs, 1, name);
	};
	auto gep_const_i8 = [&](LLVMValueRef base, int offset, const char *name) {
		LLVMValueRef idx = LLVMConstInt(type_i32, offset, false);
		return gep_i8(base, idx, name);
	};

	snprintf(helper_name, sizeof(helper_name), "%s_numeric_s2_fast", parent_funcname);
	v_func = LLVMAddFunction(mod, helper_name, num_fn_ty);
	LLVMSetLinkage(v_func, LLVMInternalLinkage);
	v_ptr = LLVMGetParam(v_func, 0);
	v_dst = LLVMGetParam(v_func, 1);

	b_entry = LLVMAppendBasicBlockInContext(lc, v_func, "entry");
	b_oneb = LLVMAppendBasicBlockInContext(lc, v_func, "oneb");
	b_fourb = LLVMAppendBasicBlockInContext(lc, v_func, "fourb");
	b_parse = LLVMAppendBasicBlockInContext(lc, v_func, "parse");
	b_parse_dispatch = LLVMAppendBasicBlockInContext(lc, v_func, "parse_dispatch");
	b_short = LLVMAppendBasicBlockInContext(lc, v_func, "short");
	b_long = LLVMAppendBasicBlockInContext(lc, v_func, "long");
	b_decode_init = LLVMAppendBasicBlockInContext(lc, v_func, "decode_init");
	b_int_cond = LLVMAppendBasicBlockInContext(lc, v_func, "int_cond");
	b_int_body = LLVMAppendBasicBlockInContext(lc, v_func, "int_body");
	b_int_load_digit = LLVMAppendBasicBlockInContext(lc, v_func, "int_load_digit");
	b_int_latch = LLVMAppendBasicBlockInContext(lc, v_func, "int_latch");
	b_frac_prep = LLVMAppendBasicBlockInContext(lc, v_func, "frac_prep");
	b_frac_load = LLVMAppendBasicBlockInContext(lc, v_func, "frac_load");
	b_frac_check = LLVMAppendBasicBlockInContext(lc, v_func, "frac_check");
	b_trailing_cond = LLVMAppendBasicBlockInContext(lc, v_func, "trailing_cond");
	b_trailing_body = LLVMAppendBasicBlockInContext(lc, v_func, "trailing_body");
	b_store = LLVMAppendBasicBlockInContext(lc, v_func, "store");
	b_fallback = LLVMAppendBasicBlockInContext(lc, v_func, "fallback");

	LLVMPositionBuilderAtEnd(b, b_entry);
	payload_ptr_p = LLVMBuildAlloca(b, l_ptr(type_i8), "payload_ptr");
	payload_size_p = LLVMBuildAlloca(b, type_i32, "payload_size");
	digits_ptr_p = LLVMBuildAlloca(b, l_ptr(type_i8), "digits_ptr");
	ndigits_p = LLVMBuildAlloca(b, type_i32, "ndigits");
	weight_p = LLVMBuildAlloca(b, type_i32, "weight");
	negative_p = LLVMBuildAlloca(b, type_i1, "negative");
	accum_p = LLVMBuildAlloca(b, type_i64, "accum");
	idx_p = LLVMBuildAlloca(b, type_i32, "idx");
	frac_digit_p = LLVMBuildAlloca(b, type_i32, "frac_digit");
	{
		LLVMValueRef v_first_byte = load_i8(v_ptr, "varhdr1");
#ifdef WORDS_BIGENDIAN
		LLVMValueRef v_is_1b =
			LLVMBuildICmp(b, LLVMIntEQ,
						  LLVMBuildAnd(b, v_first_byte, LLVMConstInt(type_i8, 0x80, false), ""),
						  LLVMConstInt(type_i8, 0x80, false),
						  "is_1b");
#else
		LLVMValueRef v_is_1b =
			LLVMBuildICmp(b, LLVMIntEQ,
						  LLVMBuildAnd(b, v_first_byte, LLVMConstInt(type_i8, 0x01, false), ""),
						  LLVMConstInt(type_i8, 0x01, false),
						  "is_1b");
#endif
		LLVMBuildCondBr(b, v_is_1b, b_oneb, b_fourb);
	}

	LLVMPositionBuilderAtEnd(b, b_oneb);
	{
		LLVMValueRef v_first_byte = load_i8(v_ptr, "oneb_hdr");
#ifdef WORDS_BIGENDIAN
		LLVMValueRef v_is_external =
			LLVMBuildICmp(b, LLVMIntEQ, v_first_byte,
						  LLVMConstInt(type_i8, 0x80, false), "is_external");
		LLVMValueRef v_total_size =
			LLVMBuildZExt(b,
						  LLVMBuildAnd(b, v_first_byte, LLVMConstInt(type_i8, 0x7F, false), ""),
						  type_i32,
						  "oneb_size");
#else
		LLVMValueRef v_is_external =
			LLVMBuildICmp(b, LLVMIntEQ, v_first_byte,
						  LLVMConstInt(type_i8, 0x01, false), "is_external");
		LLVMValueRef v_total_size =
			LLVMBuildZExt(b,
						  LLVMBuildLShr(b, v_first_byte, LLVMConstInt(type_i8, 1, false), ""),
						  type_i32,
						  "oneb_size");
#endif
		LLVMValueRef v_valid_size =
			LLVMBuildICmp(b, LLVMIntUGE, v_total_size, LLVMConstInt(type_i32, 1, false), "oneb_valid");
		LLVMValueRef v_ok = LLVMBuildAnd(b,
										 LLVMBuildNot(b, v_is_external, ""),
										 v_valid_size,
										 "oneb_ok");
		LLVMBuildStore(b, gep_const_i8(v_ptr, 1, ""), payload_ptr_p);
		LLVMBuildStore(b,
					   LLVMBuildSub(b, v_total_size, LLVMConstInt(type_i32, 1, false), ""),
					   payload_size_p);
		LLVMBuildCondBr(b, v_ok, b_parse, b_fallback);
	}

	LLVMPositionBuilderAtEnd(b, b_fourb);
	{
		LLVMValueRef v_first_byte = load_i8(v_ptr, "fourb_hdr1");
		LLVMValueRef v_header32 = load_i32_from_i8(v_ptr, "fourb_hdr");
#ifdef WORDS_BIGENDIAN
		LLVMValueRef v_is_compressed =
			LLVMBuildICmp(b, LLVMIntEQ,
						  LLVMBuildAnd(b, v_first_byte, LLVMConstInt(type_i8, 0xC0, false), ""),
						  LLVMConstInt(type_i8, 0x40, false),
						  "is_compressed");
		LLVMValueRef v_total_size =
			LLVMBuildAnd(b, v_header32, LLVMConstInt(type_i32, 0x3FFFFFFF, false), "fourb_size");
#else
		LLVMValueRef v_is_compressed =
			LLVMBuildICmp(b, LLVMIntEQ,
						  LLVMBuildAnd(b, v_first_byte, LLVMConstInt(type_i8, 0x03, false), ""),
						  LLVMConstInt(type_i8, 0x02, false),
						  "is_compressed");
		LLVMValueRef v_total_size =
			LLVMBuildAnd(b,
						 LLVMBuildLShr(b, v_header32, LLVMConstInt(type_i32, 2, false), ""),
						 LLVMConstInt(type_i32, 0x3FFFFFFF, false),
						 "fourb_size");
#endif
		LLVMValueRef v_valid_size =
			LLVMBuildICmp(b, LLVMIntUGE, v_total_size, LLVMConstInt(type_i32, 4, false), "fourb_valid");
		LLVMValueRef v_ok = LLVMBuildAnd(b,
										 LLVMBuildNot(b, v_is_compressed, ""),
										 v_valid_size,
										 "fourb_ok");
		LLVMBuildStore(b, gep_const_i8(v_ptr, 4, ""), payload_ptr_p);
		LLVMBuildStore(b,
					   LLVMBuildSub(b, v_total_size, LLVMConstInt(type_i32, 4, false), ""),
					   payload_size_p);
		LLVMBuildCondBr(b, v_ok, b_parse, b_fallback);
	}

	LLVMPositionBuilderAtEnd(b, b_parse);
	{
		LLVMValueRef v_payload_ptr = LLVMBuildLoad2(b, l_ptr(type_i8), payload_ptr_p, "payload_ptr");
		LLVMValueRef v_payload_size = LLVMBuildLoad2(b, type_i32, payload_size_p, "payload_size");
		LLVMValueRef v_header16;
		LLVMValueRef v_signmask;
		LLVMValueRef v_is_special;

		(void) v_payload_size;
		v_header16 = load_i16_from_i8(v_payload_ptr, "numeric_hdr");
		v_signmask = LLVMBuildAnd(b, v_header16, LLVMConstInt(type_i16, 0xC000, false), "signmask");
		v_is_special =
			LLVMBuildICmp(b, LLVMIntEQ, v_signmask,
						  LLVMConstInt(type_i16, 0xC000, false), "is_special");
		LLVMBuildCondBr(b, v_is_special, b_fallback, b_parse_dispatch);
	}

	LLVMPositionBuilderAtEnd(b, b_parse_dispatch);
	{
		LLVMValueRef v_payload_ptr = LLVMBuildLoad2(b, l_ptr(type_i8), payload_ptr_p, "payload_ptr");
		LLVMValueRef v_header16 = load_i16_from_i8(v_payload_ptr, "numeric_hdr_dispatch");
		LLVMValueRef v_signmask = LLVMBuildAnd(b, v_header16, LLVMConstInt(type_i16, 0xC000, false), "signmask_dispatch");
		LLVMValueRef v_is_short =
			LLVMBuildICmp(b, LLVMIntEQ, v_signmask,
						  LLVMConstInt(type_i16, 0x8000, false), "is_short_dispatch");
		LLVMBuildCondBr(b, v_is_short, b_short, b_long);
	}

	LLVMPositionBuilderAtEnd(b, b_short);
	{
		LLVMValueRef v_payload_ptr = LLVMBuildLoad2(b, l_ptr(type_i8), payload_ptr_p, "payload_ptr");
		LLVMValueRef v_payload_size = LLVMBuildLoad2(b, type_i32, payload_size_p, "payload_size");
		LLVMValueRef v_header16 = load_i16_from_i8(v_payload_ptr, "short_hdr");
		LLVMValueRef v_body_bytes;
		LLVMValueRef v_weight_bits;
		LLVMValueRef v_weight_signed;
		LLVMValueRef v_dscale;
		LLVMValueRef v_negative;
		LLVMValueRef v_valid;

		v_valid = LLVMBuildICmp(b, LLVMIntUGE, v_payload_size, LLVMConstInt(type_i32, 2, false), "short_has_hdr");
		v_body_bytes = LLVMBuildSub(b, v_payload_size, LLVMConstInt(type_i32, 2, false), "short_body");
		v_valid = LLVMBuildAnd(b, v_valid,
							   LLVMBuildICmp(b, LLVMIntEQ,
											 LLVMBuildAnd(b, v_body_bytes, LLVMConstInt(type_i32, 1, false), ""),
											 LLVMConstInt(type_i32, 0, false),
											 "short_even"),
							   "short_valid");
		v_dscale = LLVMBuildAnd(b,
								LLVMBuildLShr(b,
											  LLVMBuildZExt(b,
															LLVMBuildAnd(b, v_header16, LLVMConstInt(type_i16, 0x1F80, false), ""),
															type_i32, ""),
											  LLVMConstInt(type_i32, 7, false), ""),
								LLVMConstInt(type_i32, 0xFF, false),
								"short_dscale");
		v_valid = LLVMBuildAnd(b, v_valid,
							   LLVMBuildICmp(b, LLVMIntULE, v_dscale, LLVMConstInt(type_i32, DEFAULT_NUMERIC_SCALE, false), ""),
							   "short_scale_ok");
		v_weight_bits =
			LLVMBuildZExt(b, LLVMBuildAnd(b, v_header16, LLVMConstInt(type_i16, 0x003F, false), ""), type_i32, "short_weight_bits");
		v_weight_signed =
			LLVMBuildSelect(b,
							LLVMBuildICmp(b, LLVMIntNE,
										  LLVMBuildAnd(b, v_header16, LLVMConstInt(type_i16, 0x0040, false), ""),
										  LLVMConstInt(type_i16, 0, false),
										  "short_weight_neg"),
							LLVMBuildOr(b, v_weight_bits, LLVMConstInt(type_i32, ~0x003F, true), ""),
							v_weight_bits,
							"short_weight");
		v_negative =
			LLVMBuildICmp(b, LLVMIntNE,
						  LLVMBuildAnd(b, v_header16, LLVMConstInt(type_i16, 0x2000, false), ""),
						  LLVMConstInt(type_i16, 0, false),
						  "short_negative");
		LLVMBuildStore(b, gep_const_i8(v_payload_ptr, 2, ""), digits_ptr_p);
		LLVMBuildStore(b, LLVMBuildLShr(b, v_body_bytes, LLVMConstInt(type_i32, 1, false), ""), ndigits_p);
		LLVMBuildStore(b, v_weight_signed, weight_p);
		LLVMBuildStore(b, v_negative, negative_p);
		LLVMBuildCondBr(b, v_valid, b_decode_init, b_fallback);
	}

	LLVMPositionBuilderAtEnd(b, b_long);
	{
		LLVMValueRef v_payload_ptr = LLVMBuildLoad2(b, l_ptr(type_i8), payload_ptr_p, "payload_ptr");
		LLVMValueRef v_payload_size = LLVMBuildLoad2(b, type_i32, payload_size_p, "payload_size");
		LLVMValueRef v_header16 = load_i16_from_i8(v_payload_ptr, "long_hdr");
		LLVMValueRef v_body_bytes;
		LLVMValueRef v_dscale;
		LLVMValueRef v_negative;
		LLVMValueRef v_valid;
		LLVMValueRef v_weight =
			LLVMBuildSExt(b,
						  load_i16_from_i8(gep_const_i8(v_payload_ptr, 2, ""),
										 "long_weight"),
						  type_i32,
						  "long_weight32");

		v_valid = LLVMBuildICmp(b, LLVMIntUGE, v_payload_size, LLVMConstInt(type_i32, 4, false), "long_has_hdr");
		v_body_bytes = LLVMBuildSub(b, v_payload_size, LLVMConstInt(type_i32, 4, false), "long_body");
		v_valid = LLVMBuildAnd(b, v_valid,
							   LLVMBuildICmp(b, LLVMIntEQ,
											 LLVMBuildAnd(b, v_body_bytes, LLVMConstInt(type_i32, 1, false), ""),
											 LLVMConstInt(type_i32, 0, false),
											 "long_even"),
							   "long_valid");
		v_dscale =
			LLVMBuildAnd(b,
						 LLVMBuildZExt(b,
									   LLVMBuildAnd(b, v_header16, LLVMConstInt(type_i16, 0x3FFF, false), ""),
									   type_i32,
									   ""),
						 LLVMConstInt(type_i32, 0x3FFF, false),
						 "long_dscale");
		v_valid = LLVMBuildAnd(b, v_valid,
							   LLVMBuildICmp(b, LLVMIntULE, v_dscale, LLVMConstInt(type_i32, DEFAULT_NUMERIC_SCALE, false), ""),
							   "long_scale_ok");
		v_negative =
			LLVMBuildICmp(b, LLVMIntEQ,
						  LLVMBuildAnd(b, v_header16, LLVMConstInt(type_i16, 0xC000, false), ""),
						  LLVMConstInt(type_i16, 0x4000, false),
						  "long_negative");
		LLVMBuildStore(b, gep_const_i8(v_payload_ptr, 4, ""), digits_ptr_p);
		LLVMBuildStore(b, LLVMBuildLShr(b, v_body_bytes, LLVMConstInt(type_i32, 1, false), ""), ndigits_p);
		LLVMBuildStore(b, v_weight, weight_p);
		LLVMBuildStore(b, v_negative, negative_p);
		LLVMBuildCondBr(b, v_valid, b_decode_init, b_fallback);
	}

	LLVMPositionBuilderAtEnd(b, b_decode_init);
	LLVMBuildStore(b, LLVMConstInt(type_i64, 0, false), accum_p);
	LLVMBuildStore(b, LLVMConstInt(type_i32, 0, false), idx_p);
	LLVMBuildBr(b, b_int_cond);

	LLVMPositionBuilderAtEnd(b, b_int_cond);
	{
		LLVMValueRef v_idx = LLVMBuildLoad2(b, type_i32, idx_p, "idx");
		LLVMValueRef v_weight = LLVMBuildLoad2(b, type_i32, weight_p, "weight");
		LLVMValueRef v_continue =
			LLVMBuildICmp(b, LLVMIntSLE, v_idx, v_weight, "int_continue");
		LLVMBuildCondBr(b, v_continue, b_int_body, b_frac_prep);
	}

	LLVMPositionBuilderAtEnd(b, b_int_body);
	{
		LLVMValueRef v_idx = LLVMBuildLoad2(b, type_i32, idx_p, "idx");
		LLVMValueRef v_ndigits = LLVMBuildLoad2(b, type_i32, ndigits_p, "ndigits");
		LLVMValueRef v_has_digit =
			LLVMBuildICmp(b, LLVMIntSLT, v_idx, v_ndigits, "has_digit");
		LLVMValueRef v_accum =
			LLVMBuildMul(b, LLVMBuildLoad2(b, type_i64, accum_p, "accum"),
						 LLVMConstInt(type_i64, VOLVEC_NBASE, false),
						 "accum_mul");
		LLVMBuildStore(b, v_accum, accum_p);
		LLVMBuildCondBr(b, v_has_digit, b_int_load_digit, b_int_latch);
	}

	LLVMPositionBuilderAtEnd(b, b_int_load_digit);
	{
		LLVMValueRef v_idx = LLVMBuildLoad2(b, type_i32, idx_p, "idx");
		LLVMValueRef v_digits_ptr = LLVMBuildLoad2(b, l_ptr(type_i8), digits_ptr_p, "digits_ptr");
		LLVMValueRef v_digit_ptr =
			gep_i8(v_digits_ptr,
				   LLVMBuildMul(b, v_idx, LLVMConstInt(type_i32, 2, false), ""),
				   "");
		LLVMValueRef v_digit =
			LLVMBuildSExt(b, load_i16_from_i8(v_digit_ptr, "digit"), type_i64, "digit64");
		LLVMBuildStore(b,
					   LLVMBuildAdd(b, LLVMBuildLoad2(b, type_i64, accum_p, "accum"), v_digit, "accum_add"),
					   accum_p);
		LLVMBuildBr(b, b_int_latch);
	}

	LLVMPositionBuilderAtEnd(b, b_int_latch);
	LLVMBuildStore(b,
				   LLVMBuildAdd(b, LLVMBuildLoad2(b, type_i32, idx_p, "idx"),
								LLVMConstInt(type_i32, 1, false), "idx_next"),
				   idx_p);
	LLVMBuildBr(b, b_int_cond);

	LLVMPositionBuilderAtEnd(b, b_frac_prep);
	{
		LLVMValueRef v_weight = LLVMBuildLoad2(b, type_i32, weight_p, "weight");
		LLVMValueRef v_frac_index =
			LLVMBuildAdd(b, v_weight, LLVMConstInt(type_i32, 1, false), "frac_index");
		LLVMValueRef v_ndigits = LLVMBuildLoad2(b, type_i32, ndigits_p, "ndigits");
		LLVMValueRef v_in_range =
			LLVMBuildAnd(b,
						 LLVMBuildICmp(b, LLVMIntSGE, v_frac_index, LLVMConstInt(type_i32, 0, false), ""),
						 LLVMBuildICmp(b, LLVMIntSLT, v_frac_index, v_ndigits, ""),
						 "frac_in_range");
		LLVMBuildStore(b, LLVMConstInt(type_i32, 0, false), frac_digit_p);
		LLVMBuildStore(b, v_frac_index, idx_p);
		LLVMBuildCondBr(b, v_in_range, b_frac_load, b_frac_check);
	}

	LLVMPositionBuilderAtEnd(b, b_frac_load);
	{
		LLVMValueRef v_frac_index = LLVMBuildLoad2(b, type_i32, idx_p, "frac_index");
		LLVMValueRef v_digits_ptr = LLVMBuildLoad2(b, l_ptr(type_i8), digits_ptr_p, "digits_ptr");
		LLVMValueRef v_digit_ptr =
			gep_i8(v_digits_ptr,
				   LLVMBuildMul(b, v_frac_index, LLVMConstInt(type_i32, 2, false), ""),
				   "");
		LLVMBuildStore(b,
					   LLVMBuildSExt(b, load_i16_from_i8(v_digit_ptr, "frac_digit16"), type_i32, "frac_digit32"),
					   frac_digit_p);
		LLVMBuildBr(b, b_frac_check);
	}

	LLVMPositionBuilderAtEnd(b, b_frac_check);
	{
		LLVMValueRef v_frac_digit = LLVMBuildLoad2(b, type_i32, frac_digit_p, "frac_digit");
		LLVMValueRef v_is_exact =
			LLVMBuildICmp(b, LLVMIntEQ,
						  LLVMBuildSRem(b, v_frac_digit, LLVMConstInt(type_i32, 100, false), ""),
						  LLVMConstInt(type_i32, 0, false),
						  "frac_exact");
		LLVMValueRef v_start =
			LLVMBuildAdd(b, LLVMBuildLoad2(b, type_i32, idx_p, "frac_index"),
						 LLVMConstInt(type_i32, 1, false), "trail_start");
		LLVMValueRef v_nonneg_start =
			LLVMBuildSelect(b,
							LLVMBuildICmp(b, LLVMIntSGE, v_start, LLVMConstInt(type_i32, 0, false), ""),
							v_start,
							LLVMConstInt(type_i32, 0, false),
							"trail_start_nonneg");

		LLVMBuildStore(b,
					   LLVMBuildAdd(b,
									LLVMBuildMul(b, LLVMBuildLoad2(b, type_i64, accum_p, "accum"),
												 LLVMConstInt(type_i64, 100, false), ""),
									LLVMBuildSExt(b,
												  LLVMBuildSDiv(b, v_frac_digit, LLVMConstInt(type_i32, 100, false), ""),
												  type_i64,
												  ""),
									"scaled_accum"),
					   accum_p);
		LLVMBuildStore(b, v_nonneg_start, idx_p);
		LLVMBuildCondBr(b, v_is_exact, b_trailing_cond, b_fallback);
	}

	LLVMPositionBuilderAtEnd(b, b_trailing_cond);
	{
		LLVMValueRef v_idx = LLVMBuildLoad2(b, type_i32, idx_p, "trail_idx");
		LLVMValueRef v_ndigits = LLVMBuildLoad2(b, type_i32, ndigits_p, "ndigits");
		LLVMValueRef v_continue =
			LLVMBuildICmp(b, LLVMIntSLT, v_idx, v_ndigits, "trail_continue");
		LLVMBuildCondBr(b, v_continue, b_trailing_body, b_store);
	}

	LLVMPositionBuilderAtEnd(b, b_trailing_body);
	{
		LLVMValueRef v_idx = LLVMBuildLoad2(b, type_i32, idx_p, "trail_idx");
		LLVMValueRef v_digits_ptr = LLVMBuildLoad2(b, l_ptr(type_i8), digits_ptr_p, "digits_ptr");
		LLVMValueRef v_digit_ptr =
			gep_i8(v_digits_ptr,
				   LLVMBuildMul(b, v_idx, LLVMConstInt(type_i32, 2, false), ""),
				   "");
		LLVMValueRef v_digit = load_i16_from_i8(v_digit_ptr, "trail_digit");
		LLVMBuildStore(b,
					   LLVMBuildAdd(b, v_idx, LLVMConstInt(type_i32, 1, false), "trail_next"),
					   idx_p);
		LLVMBuildCondBr(b,
						LLVMBuildICmp(b, LLVMIntEQ, v_digit, LLVMConstInt(type_i16, 0, false), "trail_zero"),
						b_trailing_cond,
						b_fallback);
	}

	LLVMPositionBuilderAtEnd(b, b_store);
	{
		LLVMValueRef v_accum = LLVMBuildLoad2(b, type_i64, accum_p, "accum");
		LLVMValueRef v_negative = LLVMBuildLoad2(b, type_i1, negative_p, "negative");
		LLVMValueRef v_final =
			LLVMBuildSelect(b, v_negative,
							LLVMBuildSub(b, LLVMConstInt(type_i64, 0, false), v_accum, "neg_accum"),
							v_accum,
							"final");
		LLVMBuildStore(b, v_final, v_dst);
		LLVMBuildRetVoid(b);
	}

	LLVMPositionBuilderAtEnd(b, b_fallback);
	{
		LLVMValueRef args[] = {v_ptr, v_dst};
		l_call(b, num_fn_ty, v_num_fallback_fn, args, 2, "");
		LLVMBuildRetVoid(b);
	}

	LLVMDisposeBuilder(b);
	return v_func;
}

static LLVMValueRef
compile_deform_to_datachunk(LLVMJitContext *context,
							   TupleDesc desc,
							   const DeformProgram *program,
							   const char *funcname)
{
	LLVMModuleRef mod = pg_llvm_mutable_module(context);
	LLVMContextRef lc = LLVMGetModuleContext(mod);
	LLVMBuilderRef b = LLVMCreateBuilderInContext(lc);
	
	LLVMTypeRef type_i8 = LLVMInt8TypeInContext(lc);
	LLVMTypeRef type_i16 = LLVMInt16TypeInContext(lc);
	LLVMTypeRef type_i32 = LLVMInt32TypeInContext(lc);
	LLVMTypeRef type_i64 = LLVMInt64TypeInContext(lc);
	LLVMTypeRef type_f64 = LLVMDoubleTypeInContext(lc);
	LLVMTypeRef type_sizet = pg_llvm_pg_var_type("TypeSizeT");
	LLVMTypeRef type_heap_tuple_header = pg_llvm_pg_var_type("StructHeapTupleHeaderData");

	if (type_sizet == nullptr || type_heap_tuple_header == nullptr) {
		LLVMDisposeBuilder(b); return nullptr;
	}

	/* SIGNATURE: (HeapTupleHeader tuphdr, void** col_data, uint8_t** col_nulls, uint32 row_idx, void *owner_chunk) */
	LLVMTypeRef param_types[5];
	param_types[0] = l_ptr(type_heap_tuple_header);
	param_types[1] = l_ptr(l_ptr(type_i8));
	param_types[2] = l_ptr(l_ptr(type_i8));
	param_types[3] = type_i32;
	param_types[4] = l_ptr(type_i8);

	LLVMTypeRef func_sig = LLVMFunctionType(LLVMVoidTypeInContext(lc), param_types, 5, 0);
	LLVMValueRef v_func = LLVMAddFunction(mod, funcname, func_sig);
	LLVMValueRef v_tuphdr = LLVMGetParam(v_func, 0);
	LLVMValueRef v_col_data = LLVMGetParam(v_func, 1);
	LLVMValueRef v_col_nulls = LLVMGetParam(v_func, 2);
	LLVMValueRef v_row_idx = LLVMGetParam(v_func, 3);
	LLVMValueRef v_owner_chunk = LLVMGetParam(v_func, 4);

	LLVMTypeRef num_fn_args[2] = { l_ptr(type_i8), l_ptr(type_i64) };
	LLVMTypeRef str_fn_args[3] = { l_ptr(type_i8), l_ptr(type_i8), l_ptr(type_i8) };
	LLVMTypeRef bpchar1_fn_args[2] = { l_ptr(type_i8), l_ptr(type_i8) };
	LLVMTypeRef num_fn_ty = LLVMFunctionType(LLVMVoidTypeInContext(lc), num_fn_args, 2, 0);
	LLVMTypeRef str_fn_ty = LLVMFunctionType(LLVMVoidTypeInContext(lc), str_fn_args, 3, 0);
	LLVMTypeRef bpchar1_fn_ty = LLVMFunctionType(LLVMVoidTypeInContext(lc), bpchar1_fn_args, 2, 0);

	LLVMTypeRef varsize_any_args[1] = { l_ptr(type_i8) };
	LLVMTypeRef varsize_any_ty = LLVMFunctionType(type_sizet, varsize_any_args, 1, 0);
	LLVMValueRef v_num_fn = pg_yaap_l_ptr_const(type_sizet, reinterpret_cast<void*>(&pg_yaap_jit_store_numeric_int64_fast), l_ptr(num_fn_ty));
	LLVMValueRef v_str_fn = pg_yaap_l_ptr_const(type_sizet, reinterpret_cast<void*>(&pg_yaap_jit_store_stringref_owned), l_ptr(str_fn_ty));
	LLVMValueRef v_bpchar1_fn = pg_yaap_l_ptr_const(type_sizet, reinterpret_cast<void*>(&pg_yaap_jit_store_bpchar1_int32), l_ptr(bpchar1_fn_ty));
	LLVMValueRef v_varsize_any_fn = pg_yaap_l_ptr_const(type_sizet, reinterpret_cast<void*>(&varsize_any), l_ptr(varsize_any_ty));
	LLVMValueRef v_num_fast_s2_fn = build_numeric_scale2_fast_store_helper(mod, lc, num_fn_ty, v_num_fn, funcname);

	LLVMBasicBlockRef b_entry = LLVMAppendBasicBlockInContext(lc, v_func, "entry");
	LLVMBasicBlockRef b_out = LLVMAppendBasicBlockInContext(lc, v_func, "out");
	LLVMPositionBuilderAtEnd(b, b_entry);

	LLVMValueRef v_offp = LLVMBuildAlloca(b, type_sizet, "offp");
	LLVMBuildStore(b, pg_yaap_l_sizet_const(type_sizet, 0), v_offp);
	LLVMValueRef v_infomask1 =
		l_load_struct_gep(b,
						  type_heap_tuple_header,
						  v_tuphdr,
						  FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK,
						  "infomask1");
	LLVMValueRef v_infomask2 =
		l_load_struct_gep(b,
						  type_heap_tuple_header,
						  v_tuphdr,
						  FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK2,
						  "infomask2");
	LLVMValueRef v_bits =
		LLVMBuildBitCast(b,
						 l_struct_gep(b,
									  type_heap_tuple_header,
									  v_tuphdr,
									  FIELDNO_HEAPTUPLEHEADERDATA_BITS,
									  ""),
						 l_ptr(type_i8),
						 "t_bits");
	LLVMValueRef v_hasnulls =
		LLVMBuildICmp(b, LLVMIntNE,
					  LLVMBuildAnd(b,
								   LLVMConstInt(type_i16, HEAP_HASNULL, false),
								   v_infomask1, ""),
					  LLVMConstInt(type_i16, 0, false),
					  "hasnulls");
	LLVMValueRef v_maxatt =
		LLVMBuildAnd(b,
					 LLVMConstInt(type_i16, HEAP_NATTS_MASK, false),
					 v_infomask2,
					 "maxatt");
	LLVMValueRef v_hoff =
		LLVMBuildZExt(b,
					  l_load_struct_gep(b,
										type_heap_tuple_header,
										v_tuphdr,
										FIELDNO_HEAPTUPLEHEADERDATA_HOFF,
										""),
					  type_i32,
					  "t_hoff");
	LLVMValueRef v_tupdata_base =
		l_gep(b,
			  type_i8,
			  LLVMBuildBitCast(b, v_tuphdr, l_ptr(type_i8), ""),
			  &v_hoff, 1,
			  "tupdata_base");

	if (program->last_att_index < 0) {
		LLVMBuildBr(b, b_out);
	} else {
		std::vector<LLVMBasicBlockRef> attcheck(program->last_att_index + 1);
		std::vector<LLVMBasicBlockRef> attstart(program->last_att_index + 1);
		std::vector<LLVMBasicBlockRef> attnonnull(program->last_att_index + 1);
		std::vector<LLVMBasicBlockRef> attnull(program->last_att_index + 1);
		std::vector<LLVMBasicBlockRef> attalign(program->last_att_index + 1);
		std::vector<LLVMBasicBlockRef> attstore(program->last_att_index + 1);
		std::vector<LLVMBasicBlockRef> attunavail(program->last_att_index + 1);

		for (int att_index = 0; att_index <= program->last_att_index; att_index++) {
			attcheck[att_index] = l_bb_append_v(v_func, "att.%d.check", att_index);
			attstart[att_index] = l_bb_append_v(v_func, "att.%d.start", att_index);
			attnonnull[att_index] = l_bb_append_v(v_func, "att.%d.nonnull", att_index);
			attnull[att_index] = l_bb_append_v(v_func, "att.%d.null", att_index);
			attalign[att_index] = l_bb_append_v(v_func, "att.%d.align", att_index);
			attstore[att_index] = l_bb_append_v(v_func, "att.%d.store", att_index);
			attunavail[att_index] = l_bb_append_v(v_func, "att.%d.unavail", att_index);
		}

		auto store_target_null = [&](const DeformTarget &target) {
			LLVMValueRef v_dst_idx = LLVMConstInt(type_i32, target.dst_col, false);
			LLVMValueRef v_null_arr_ptr = LLVMBuildGEP2(b, l_ptr(type_i8), v_col_nulls, &v_dst_idx, 1, "");
			LLVMValueRef v_null_arr = LLVMBuildLoad2(b, l_ptr(type_i8), v_null_arr_ptr, "");
			LLVMValueRef v_row_null = LLVMBuildGEP2(b, type_i8, v_null_arr, &v_row_idx, 1, "");
			LLVMBuildStore(b, LLVMConstInt(type_i8, 1, false), v_row_null);
		};

		auto store_target_notnull = [&](const DeformTarget &target, LLVMValueRef v_attdatap) {
			LLVMValueRef v_dst_idx = LLVMConstInt(type_i32, target.dst_col, false);
			LLVMValueRef v_null_arr_ptr = LLVMBuildGEP2(b, l_ptr(type_i8), v_col_nulls, &v_dst_idx, 1, "");
			LLVMValueRef v_null_arr = LLVMBuildLoad2(b, l_ptr(type_i8), v_null_arr_ptr, "");
			LLVMValueRef v_row_null = LLVMBuildGEP2(b, type_i8, v_null_arr, &v_row_idx, 1, "");
			LLVMValueRef v_data_ptr_ptr = LLVMBuildGEP2(b, l_ptr(type_i8), v_col_data, &v_dst_idx, 1, "");
			LLVMValueRef v_data_array = LLVMBuildLoad2(b, l_ptr(type_i8), v_data_ptr_ptr, "");

			LLVMBuildStore(b, LLVMConstInt(type_i8, 0, false), v_row_null);

			switch (target.decode_kind) {
				case DeformDecodeKind::kInt32:
				case DeformDecodeKind::kDate32:
				{
					LLVMValueRef v_val = LLVMBuildLoad2(b, type_i32,
						LLVMBuildBitCast(b, v_attdatap, l_ptr(type_i32), ""), "val_i32");
					LLVMValueRef v_dest = LLVMBuildGEP2(b, type_i32,
						LLVMBuildBitCast(b, v_data_array, l_ptr(type_i32), ""),
						&v_row_idx, 1, "dest_i32");
					LLVMBuildStore(b, v_val, v_dest);
					break;
				}
				case DeformDecodeKind::kInt64:
				{
					LLVMValueRef v_val = LLVMBuildLoad2(b, type_i64,
						LLVMBuildBitCast(b, v_attdatap, l_ptr(type_i64), ""), "val_i64");
					LLVMValueRef v_dest = LLVMBuildGEP2(b, type_i64,
						LLVMBuildBitCast(b, v_data_array, l_ptr(type_i64), ""),
						&v_row_idx, 1, "dest_i64");
					LLVMBuildStore(b, v_val, v_dest);
					break;
				}
				case DeformDecodeKind::kFloat8:
				{
					LLVMValueRef v_val = LLVMBuildLoad2(b, type_f64,
						LLVMBuildBitCast(b, v_attdatap, l_ptr(type_f64), ""), "val_f64");
					LLVMValueRef v_dest = LLVMBuildGEP2(b, type_f64,
						LLVMBuildBitCast(b, v_data_array, l_ptr(type_f64), ""),
						&v_row_idx, 1, "dest_f64");
					LLVMBuildStore(b, v_val, v_dest);
					break;
				}
				case DeformDecodeKind::kBpchar1:
				{
					LLVMValueRef v_dest = LLVMBuildGEP2(b, type_i32,
						LLVMBuildBitCast(b, v_data_array, l_ptr(type_i32), ""),
						&v_row_idx, 1, "dest_bpchar1");
					LLVMValueRef v_dest_i8 = LLVMBuildBitCast(b, v_dest, l_ptr(type_i8), "");
					LLVMValueRef args[] = {v_attdatap, v_dest_i8};
					l_call(b, bpchar1_fn_ty, v_bpchar1_fn, args, 2, "");
					break;
				}
				case DeformDecodeKind::kNumeric:
				{
					int target_scale = GetNumericScaleFromTypmod(TupleDescAttr(desc, target.att_index)->atttypmod);
					LLVMValueRef v_dest = LLVMBuildGEP2(b, type_i64,
						LLVMBuildBitCast(b, v_data_array, l_ptr(type_i64), ""),
						&v_row_idx, 1, "dest_num");
					LLVMValueRef args[] = {v_attdatap, v_dest};
					LLVMValueRef v_call = l_call(b, num_fn_ty,
										 target_scale == DEFAULT_NUMERIC_SCALE ? v_num_fast_s2_fn : v_num_fn,
										 args, 2, "");
					l_callsite_alwaysinline(v_call);
					break;
				}
				case DeformDecodeKind::kStringRef:
				{
					LLVMTypeRef type_arr16 = LLVMArrayType(type_i8, 16);
					LLVMValueRef v_dest = LLVMBuildGEP2(b, type_arr16,
						LLVMBuildBitCast(b, v_data_array, l_ptr(type_arr16), ""),
						&v_row_idx, 1, "dest_str");
					LLVMValueRef v_dest_i8 = LLVMBuildBitCast(b, v_dest, l_ptr(type_i8), "");
					LLVMValueRef v_owner_chunk_i8 =
						LLVMBuildBitCast(b, v_owner_chunk, l_ptr(type_i8), "");
					LLVMValueRef args[] = {v_attdatap, v_dest_i8, v_owner_chunk_i8};
					l_call(b, str_fn_ty, v_str_fn, args, 3, "");
					break;
				}
			}
		};

		LLVMBuildBr(b, attcheck[0]);

		int target_idx = 0;
		for (int att_index = 0; att_index <= program->last_att_index; att_index++) {
			CompactAttribute *att = TupleDescCompactAttr(desc, att_index);
			LLVMBasicBlockRef b_next = (att_index == program->last_att_index) ? b_out : attcheck[att_index + 1];
			bool is_target = target_idx < program->ntargets && program->targets[target_idx].att_index == att_index;
			int current_target_idx = target_idx;

			LLVMPositionBuilderAtEnd(b, attcheck[att_index]);
			LLVMBuildCondBr(b,
							LLVMBuildICmp(b, LLVMIntULE, v_maxatt,
										  LLVMConstInt(type_i16, att_index, false), ""),
							attunavail[att_index],
							attstart[att_index]);

			LLVMPositionBuilderAtEnd(b, attstart[att_index]);
			if (att->attnullability != ATTNULLABLE_VALID) {
				LLVMValueRef v_nullbyteno = LLVMConstInt(type_i32, att_index >> 3, false);
				LLVMValueRef v_nullbytemask = LLVMConstInt(type_i8, 1 << (att_index & 0x07), false);
				LLVMValueRef v_nullbyte = l_load_gep1(b, type_i8, v_bits, v_nullbyteno, "attnullbyte");
				LLVMValueRef v_nullbit =
					LLVMBuildICmp(b, LLVMIntEQ,
								  LLVMBuildAnd(b, v_nullbyte, v_nullbytemask, ""),
								  LLVMConstInt(type_i8, 0, false),
								  "attisnull");
				LLVMValueRef v_attisnull = LLVMBuildAnd(b, v_hasnulls, v_nullbit, "");
				LLVMBuildCondBr(b, v_attisnull, attnull[att_index], attnonnull[att_index]);
			} else {
				LLVMBuildBr(b, attnonnull[att_index]);
			}

			LLVMPositionBuilderAtEnd(b, attnull[att_index]);
			if (is_target)
				store_target_null(program->targets[current_target_idx]);
			LLVMBuildBr(b, b_next);

			LLVMPositionBuilderAtEnd(b, attnonnull[att_index]);
			if (att->attalignby > 1) {
				if (att->attlen == -1) {
					LLVMValueRef v_off = LLVMBuildLoad2(b, type_sizet, v_offp, "off");
					LLVMValueRef v_possible_padbyte = l_load_gep1(b, type_i8, v_tupdata_base, v_off, "padbyte");
					LLVMValueRef v_ispad = LLVMBuildICmp(b, LLVMIntEQ,
														 v_possible_padbyte,
														 LLVMConstInt(type_i8, 0, false),
														 "ispadbyte");
					LLVMBuildCondBr(b, v_ispad, attalign[att_index], attstore[att_index]);
				} else {
					LLVMBuildBr(b, attalign[att_index]);
				}
			} else {
				LLVMBuildBr(b, attstore[att_index]);
			}

			LLVMPositionBuilderAtEnd(b, attalign[att_index]);
			{
				int align_bytes = att->attalignby;
				LLVMValueRef v_off = LLVMBuildLoad2(b, type_sizet, v_offp, "off");
				LLVMValueRef v_aligned_off =
					LLVMBuildAnd(b,
								 LLVMBuildAdd(b, v_off, pg_yaap_l_sizet_const(type_sizet, align_bytes - 1), ""),
								 pg_yaap_l_sizet_const(type_sizet, ~((Size) align_bytes - 1)),
								 "aligned_off");
				LLVMBuildStore(b, v_aligned_off, v_offp);
			}
			LLVMBuildBr(b, attstore[att_index]);

			LLVMPositionBuilderAtEnd(b, attstore[att_index]);
			{
				LLVMValueRef v_off = LLVMBuildLoad2(b, type_sizet, v_offp, "off");
				LLVMValueRef v_attdatap = LLVMBuildGEP2(b, type_i8, v_tupdata_base, &v_off, 1, "attdatap");

				if (is_target)
					store_target_notnull(program->targets[current_target_idx], v_attdatap);

				LLVMValueRef v_new_off;
				if (att->attlen > 0) {
					v_new_off = LLVMBuildAdd(b, v_off, pg_yaap_l_sizet_const(type_sizet, att->attlen), "next_off");
					LLVMBuildStore(b, v_new_off, v_offp);
				} else {
					/* Task A: inline VARATT_IS_1B fast path (byte0&1 ⇒ len=(byte0>>1)&0x7F);
					 * Q1 NUMERIC(15,2) is always short-header (was 57 samples in varsize_any).
					 * Long/external/compressed fall through to varsize_any for correctness. */
					LLVMBasicBlockRef bb_short = l_bb_append_v(v_func, "att.%d.varshort", att_index);
					LLVMBasicBlockRef bb_long = l_bb_append_v(v_func, "att.%d.varlong", att_index);
					LLVMBasicBlockRef bb_merge = l_bb_append_v(v_func, "att.%d.varmerge", att_index);

					LLVMValueRef v_byte0 = LLVMBuildLoad2(b, type_i8, v_attdatap, "varhdr_byte0");
					LLVMValueRef v_is_short = LLVMBuildICmp(b, LLVMIntEQ,
															 LLVMBuildAnd(b, v_byte0, LLVMConstInt(type_i8, 0x01, false), ""),
															 LLVMConstInt(type_i8, 0x01, false),
															 "is_1b_hdr");
					LLVMBuildCondBr(b, v_is_short, bb_short, bb_long);

					LLVMPositionBuilderAtEnd(b, bb_short);
					LLVMValueRef v_short_len = LLVMBuildAnd(b,
															 LLVMBuildLShr(b, v_byte0, LLVMConstInt(type_i8, 1, false), ""),
															 LLVMConstInt(type_i8, 0x7F, false),
															 "short_len");
					LLVMValueRef v_short_incby = LLVMBuildZExt(b, v_short_len, type_sizet, "short_incby");
					LLVMBuildBr(b, bb_merge);

					LLVMPositionBuilderAtEnd(b, bb_long);
					/* Task A: no callsite attrs on the long path. The pre-B.2 straight-line
					 * call had alwaysinline+ro, but adding a phi-merge after the call invalidated
					 * the inliner's predecessor map intermittently (cold-cache 1/10 PANIC repro).
					 * varsize_any is an external function pointer constant; alwaysinline cannot
					 * actually inline it, so dropping the attr is functionally equivalent. */
					LLVMValueRef v_long_incby = l_call(b,
													   varsize_any_ty,
													   v_varsize_any_fn,
													   &v_attdatap, 1,
													   "varsize_any");
					LLVMBuildBr(b, bb_merge);

					LLVMPositionBuilderAtEnd(b, bb_merge);
					LLVMValueRef v_incby_phi = LLVMBuildPhi(b, type_sizet, "incby_phi");
					LLVMValueRef incby_vals[2] = { v_short_incby, v_long_incby };
					LLVMBasicBlockRef incby_bbs[2] = { bb_short, bb_long };
					LLVMAddIncoming(v_incby_phi, incby_vals, incby_bbs, 2);
					v_new_off = LLVMBuildAdd(b, v_off, v_incby_phi, "next_off");
					LLVMBuildStore(b, v_new_off, v_offp);
				}
			}
			LLVMBuildBr(b, b_next);

			LLVMPositionBuilderAtEnd(b, attunavail[att_index]);
			for (int i = current_target_idx; i < program->ntargets; i++)
				store_target_null(program->targets[i]);
			LLVMBuildBr(b, b_out);

			if (is_target)
				target_idx++;
		}
	}

	LLVMPositionBuilderAtEnd(b, b_out);
	LLVMBuildRetVoid(b);
	LLVMDisposeBuilder(b);
	return v_func;
}

bool
pg_yaap_try_compile_jit_deform_to_datachunk(TupleDesc desc,
											 const DeformProgram *program,
											 JitDeformFunc *out_func,
											 JitContext **out_context,
											 const char **failure_reason)
{
	LLVMJitContext *context;
	LLVMValueRef fn;
	char *funcname;
	bool created_context = false;
	bool success = false;
	uint64_t serial;

	if (out_func != nullptr) *out_func = nullptr;
	elog(LOG, "pg_yaap: deform JIT compile begin pid=%d targets=%d last_att=%d",
		 MyProcPid,
		 program != nullptr ? program->ntargets : -1,
		 program != nullptr ? program->last_att_index : -1);

	if (!load_jit_symbols(failure_reason)) {
		if (failure_reason != nullptr && *failure_reason == nullptr)
			*failure_reason = "failed to load PostgreSQL LLVM entry points";
		elog(LOG, "pg_yaap: deform JIT compile pid=%d failed during symbol load reason=%s",
			 MyProcPid,
			 (failure_reason != nullptr && *failure_reason != nullptr) ? *failure_reason : "unknown");
		return false;
	}
	if (!pg_yaap_jit_deform_supported(desc, program, failure_reason))
	{
		elog(LOG, "pg_yaap: deform JIT compile pid=%d rejected by support check reason=%s",
			 MyProcPid,
			 (failure_reason != nullptr && *failure_reason != nullptr) ? *failure_reason : "unknown");
		return false;
	}

	if (out_context != nullptr && *out_context != nullptr)
		context = (LLVMJitContext *) *out_context;
	else {
		context = (LLVMJitContext *) pg_llvm_create_context(PGJIT_PERFORM | PGJIT_DEFORM | PGJIT_OPT3);
		if (context == nullptr)
		{
			if (failure_reason != nullptr)
				*failure_reason = "llvm_create_context returned null";
			elog(LOG, "pg_yaap: deform JIT compile pid=%d failed to create LLVM context",
				 MyProcPid);
			return false;
		}
		created_context = true;
	}
	char base_name[96];
	serial = ++pg_yaap_deform_jit_serial;
	snprintf(base_name, sizeof(base_name), "pg_yaap_deform_to_chunk_%p_%llu",
			 (const void *) context,
			 (unsigned long long) serial);
	funcname = pg_llvm_expand_funcname(context, base_name);
	if (funcname == nullptr)
	{
		if (created_context)
			pg_llvm_release_context_direct(context);
		if (failure_reason != nullptr)
			*failure_reason = "llvm_expand_funcname returned null";
		elog(LOG, "pg_yaap: deform JIT compile pid=%d failed to allocate function name",
			 MyProcPid);
		return false;
	}
	PG_TRY();
	{
		fn = compile_deform_to_datachunk(context, desc, program, funcname);
		if (fn == nullptr) {
			if (failure_reason != nullptr) *failure_reason = "failed to build LLVM deform function";
			success = false;
		} else {
			if (out_func != nullptr)
				*out_func = reinterpret_cast<JitDeformFunc>(pg_llvm_get_function(context, funcname));
			if (out_context != nullptr && *out_context == nullptr)
				*out_context = &context->base;
			success = (out_func == nullptr || *out_func != nullptr);
			if (!success && failure_reason != nullptr)
				*failure_reason = "llvm_get_function returned null";
			if (success)
			{
				pg_yaap_register_llvm_jit_context(&context->base);
				elog(LOG, "pg_yaap: deform JIT compile pid=%d succeeded func=%s",
					 MyProcPid, funcname);
			}
			else
				elog(LOG, "pg_yaap: deform JIT compile pid=%d failed to resolve callable symbol func=%s",
					 MyProcPid, funcname);
		}
	}
	PG_CATCH();
	{
		if (created_context)
			pg_llvm_release_context_direct(context);
		if (out_context != nullptr && created_context)
			*out_context = nullptr;
		if (funcname != nullptr)
			pfree(funcname);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!success && created_context)
		pg_llvm_release_context_direct(context);
	if (out_context != nullptr && created_context && !success)
		*out_context = nullptr;
	pfree(funcname);
	return success;
}

} /* namespace pg_yaap */

#else

namespace pg_yaap {
}

#endif
