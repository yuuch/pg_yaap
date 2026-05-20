#ifndef PG_YAAP_LLVMJIT_DEFORM_DATACHUNK_H
#define PG_YAAP_LLVMJIT_DEFORM_DATACHUNK_H

#include "yaap_engine.hpp"

namespace pg_yaap {

#ifdef USE_LLVM
bool pg_yaap_try_compile_jit_deform_to_datachunk(TupleDesc desc,
											 const DeformProgram *program,
											 JitDeformFunc *out_func,
											 JitContext **out_context,
											 const char **failure_reason);

bool pg_yaap_try_compile_jit_expr(const VecExprProgram *program,
								 VecExprJitFunc *out_func,
								 JitContext **out_context,
								 const char **failure_reason);
#endif

} /* namespace pg_yaap */

#endif
