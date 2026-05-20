#include "yaap_engine.hpp"
#include "expr/vector_operations.hpp"

#include <cmath>
#include <cstring>

extern "C" {
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "utils/array.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"

extern bool pg_yaap_trace_hooks;
}

namespace pg_yaap
{

static DateADT
TimestampToDateValue(Timestamp ts)
{
	struct pg_tm tm;
	fsec_t		fsec;

	if (timestamp2tm(ts, NULL, &tm, &fsec, NULL, NULL) != 0)
		elog(ERROR, "pg_yaap: timestamp out of range");
	return date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;
}

static DateADT
TimestampTzToDateValue(TimestampTz ts)
{
	struct pg_tm tm;
	fsec_t		fsec;
	int			tz;

	if (timestamp2tm(ts, &tz, &tm, &fsec, NULL, NULL) != 0)
		elog(ERROR, "pg_yaap: timestamptz out of range");
	return date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;
}

static const char *
VecOpCodeName(VecOpCode opcode)
{
	switch (opcode)
	{
		case VecOpCode::EEOP_VAR: return "VAR";
		case VecOpCode::EEOP_CONST: return "CONST";
		case VecOpCode::EEOP_FLOAT8_ADD: return "FLOAT8_ADD";
		case VecOpCode::EEOP_FLOAT8_SUB: return "FLOAT8_SUB";
		case VecOpCode::EEOP_FLOAT8_MUL: return "FLOAT8_MUL";
		case VecOpCode::EEOP_INT64_ADD: return "INT64_ADD";
		case VecOpCode::EEOP_INT64_SUB: return "INT64_SUB";
		case VecOpCode::EEOP_INT64_MUL: return "INT64_MUL";
		case VecOpCode::EEOP_INT64_DIV_FLOAT8: return "INT64_DIV_FLOAT8";
		case VecOpCode::EEOP_FLOAT8_LT: return "FLOAT8_LT";
		case VecOpCode::EEOP_FLOAT8_GT: return "FLOAT8_GT";
		case VecOpCode::EEOP_FLOAT8_LE: return "FLOAT8_LE";
		case VecOpCode::EEOP_FLOAT8_GE: return "FLOAT8_GE";
		case VecOpCode::EEOP_INT64_LT: return "INT64_LT";
		case VecOpCode::EEOP_INT64_GT: return "INT64_GT";
		case VecOpCode::EEOP_INT64_LE: return "INT64_LE";
		case VecOpCode::EEOP_INT64_GE: return "INT64_GE";
		case VecOpCode::EEOP_INT64_EQ: return "INT64_EQ";
		case VecOpCode::EEOP_INT64_NE: return "INT64_NE";
		case VecOpCode::EEOP_DATE_LT: return "DATE_LT";
		case VecOpCode::EEOP_DATE_LE: return "DATE_LE";
		case VecOpCode::EEOP_DATE_GT: return "DATE_GT";
		case VecOpCode::EEOP_DATE_GE: return "DATE_GE";
		case VecOpCode::EEOP_DATE_PART_YEAR: return "DATE_PART_YEAR";
		case VecOpCode::EEOP_AND: return "AND";
		case VecOpCode::EEOP_OR: return "OR";
		case VecOpCode::EEOP_NOT: return "NOT";
		case VecOpCode::EEOP_INT64_CASE: return "INT64_CASE";
		case VecOpCode::EEOP_FLOAT8_CASE: return "FLOAT8_CASE";
		case VecOpCode::EEOP_STR_EQ: return "STR_EQ";
		case VecOpCode::EEOP_STR_NE: return "STR_NE";
		case VecOpCode::EEOP_STR_PREFIX_LIKE: return "STR_PREFIX_LIKE";
		case VecOpCode::EEOP_QUAL: return "QUAL";
		default: return "UNKNOWN";
	}
}

static Expr *
StripImplicitNodes(Expr *expr)
{
	while (expr != nullptr)
	{
		if (IsA(expr, RelabelType))
			expr = ((RelabelType *) expr)->arg;
		else if (IsA(expr, CoerceToDomain))
			expr = ((CoerceToDomain *) expr)->arg;
		else if (IsA(expr, CoerceViaIO))
			expr = ((CoerceViaIO *) expr)->arg;
		else
			break;
	}

	return expr;
}

static bool
IsReadyParamExecDatum(Oid type, const ParamExecData *prm)
{
	int16 typlen = 0;
	bool typbyval = false;

	if (prm == nullptr || prm->execPlan != nullptr || !OidIsValid(type))
		return false;
	if (prm->isnull)
		return true;

	get_typlenbyval(type, &typlen, &typbyval);
	if (!typbyval && DatumGetPointer(prm->value) == nullptr)
		return false;
	return true;
}

static bool
TryFoldDateLikeConstCompare(Const *c,
							const char *opname,
							int32_t *out_date,
							VecOpCode *out_opcode)
{
	if (c == nullptr || c->constisnull || opname == nullptr ||
		out_date == nullptr || out_opcode == nullptr)
		return false;

	if (c->consttype == DATEOID)
	{
		*out_date = DatumGetDateADT(c->constvalue);
		if (strcmp(opname, "<") == 0)
			*out_opcode = VecOpCode::EEOP_DATE_LT;
		else if (strcmp(opname, "<=") == 0)
			*out_opcode = VecOpCode::EEOP_DATE_LE;
		else if (strcmp(opname, ">") == 0)
			*out_opcode = VecOpCode::EEOP_DATE_GT;
		else if (strcmp(opname, ">=") == 0)
			*out_opcode = VecOpCode::EEOP_DATE_GE;
		else
			return false;
		return true;
	}

	if (c->consttype == TIMESTAMPOID)
	{
		Timestamp ts = DatumGetTimestamp(c->constvalue);
		DateADT date_val;
		bool exact_midnight;

		if (TIMESTAMP_NOT_FINITE(ts))
			return false;
		date_val = TimestampToDateValue(ts);
		exact_midnight = (ts == date2timestamp_opt_overflow(date_val, nullptr));
		*out_date = date_val;
		if (strcmp(opname, "<") == 0)
			*out_opcode = exact_midnight ? VecOpCode::EEOP_DATE_LT : VecOpCode::EEOP_DATE_LE;
		else if (strcmp(opname, "<=") == 0)
			*out_opcode = VecOpCode::EEOP_DATE_LE;
		else if (strcmp(opname, ">") == 0)
			*out_opcode = VecOpCode::EEOP_DATE_GT;
		else if (strcmp(opname, ">=") == 0)
			*out_opcode = exact_midnight ? VecOpCode::EEOP_DATE_GE : VecOpCode::EEOP_DATE_GT;
		else
			return false;
		return true;
	}

	if (c->consttype == TIMESTAMPTZOID)
	{
		TimestampTz ts = DatumGetTimestampTz(c->constvalue);
		DateADT date_val;
		bool exact_midnight;

		if (TIMESTAMP_NOT_FINITE(ts))
			return false;
		date_val = TimestampTzToDateValue(ts);
		exact_midnight = (ts == date2timestamptz_opt_overflow(date_val, nullptr));
		*out_date = date_val;
		if (strcmp(opname, "<") == 0)
			*out_opcode = exact_midnight ? VecOpCode::EEOP_DATE_LT : VecOpCode::EEOP_DATE_LE;
		else if (strcmp(opname, "<=") == 0)
			*out_opcode = VecOpCode::EEOP_DATE_LE;
		else if (strcmp(opname, ">") == 0)
			*out_opcode = VecOpCode::EEOP_DATE_GT;
		else if (strcmp(opname, ">=") == 0)
			*out_opcode = exact_midnight ? VecOpCode::EEOP_DATE_GE : VecOpCode::EEOP_DATE_GT;
		else
			return false;
		return true;
	}

	return false;
}

static bool
TryExtractYearFieldConst(Const *c)
{
	char *field;
	bool matches;

	if (c == nullptr || c->constisnull)
		return false;
	if (c->consttype != TEXTOID &&
		c->consttype != VARCHAROID &&
		c->consttype != BPCHAROID)
		return false;

	field = TextDatumGetCString(c->constvalue);
	matches = (pg_strcasecmp(field, "year") == 0);
	pfree(field);
	return matches;
}

static bool
TryExtractConstInt32(Const *c, int32_t *out)
{
	if (c == nullptr || c->constisnull || out == nullptr)
		return false;

	if (c->consttype == INT4OID)
	{
		*out = DatumGetInt32(c->constvalue);
		return true;
	}
	if (c->consttype == INT8OID)
	{
		int64_t value = DatumGetInt64(c->constvalue);

		if (value < PG_INT32_MIN || value > PG_INT32_MAX)
			return false;
		*out = (int32_t) value;
		return true;
	}
	return false;
}

static inline bool
RegisterStepIsConst(const VecExprStep *const *reg_defs, int reg_idx)
{
	return reg_idx >= 0 && reg_idx < MAX_REGISTERS &&
		reg_defs[reg_idx] != nullptr && reg_defs[reg_idx]->opcode == VecOpCode::EEOP_CONST;
}

static bool
ExtractStringSourceVar(Expr *expr, Var **var_out, Oid *type_out)
{
	expr = StripImplicitNodes(expr);
	if (var_out != nullptr)
		*var_out = nullptr;
	if (type_out != nullptr)
		*type_out = InvalidOid;
	if (expr == nullptr)
		return false;
	if (IsA(expr, Var))
	{
		Var *var = (Var *) expr;
		Oid type = exprType((Node *) expr);

		if (type != BPCHAROID && type != TEXTOID && type != VARCHAROID)
			return false;
		if (var_out != nullptr)
			*var_out = var;
		if (type_out != nullptr)
			*type_out = type;
		return true;
	}
	if (IsA(expr, FuncExpr))
	{
		FuncExpr *func = (FuncExpr *) expr;
		char *funcname = get_func_name(func->funcid);
		Oid rettype = exprType((Node *) expr);

		if (list_length(func->args) == 1 &&
			(rettype == BPCHAROID || rettype == TEXTOID || rettype == VARCHAROID))
			return ExtractStringSourceVar(StripImplicitNodes((Expr *) linitial(func->args)),
										var_out,
										type_out);
	}
	return false;
}

static bool
MatchStringPrefixFuncExpr(Expr *expr, int *att_idx, Oid *source_type, uint32_t *prefix_len)
{
	FuncExpr *func;
	char *funcname;
	Expr *arg_expr;
	Expr *start_expr;
	Expr *len_expr;
	Var *var = nullptr;
	int32_t start_val;
	int32_t len_val;
	Oid arg_type;

	expr = StripImplicitNodes(expr);
	if (att_idx != nullptr)
		*att_idx = -1;
	if (source_type != nullptr)
		*source_type = InvalidOid;
	if (prefix_len != nullptr)
		*prefix_len = 0;
	if (expr == nullptr || !IsA(expr, FuncExpr))
		return false;

	func = (FuncExpr *) expr;
	funcname = get_func_name(func->funcid);
	if (list_length(func->args) == 1 &&
		(exprType((Node *) expr) == BPCHAROID ||
		 exprType((Node *) expr) == TEXTOID ||
		 exprType((Node *) expr) == VARCHAROID))
		return MatchStringPrefixFuncExpr((Expr *) linitial(func->args),
										 att_idx,
										 source_type,
										 prefix_len);
	if (funcname == nullptr ||
		(strcmp(funcname, "substring") != 0 && strcmp(funcname, "substr") != 0) ||
		(list_length(func->args) != 2 && list_length(func->args) != 3))
		return false;

	arg_expr = StripImplicitNodes((Expr *) linitial(func->args));
	start_expr = StripImplicitNodes((Expr *) lsecond(func->args));
	len_expr = list_length(func->args) == 3 ?
		StripImplicitNodes((Expr *) lthird(func->args)) : nullptr;
	if (arg_expr == nullptr || start_expr == nullptr || !IsA(start_expr, Const))
		return false;
	if (!ExtractStringSourceVar(arg_expr, &var, &arg_type) ||
		var == nullptr ||
		var->varattno <= 0 || var->varattno > 16 ||
		!TryExtractConstInt32((Const *) start_expr, &start_val) ||
		start_val != 1)
		return false;
	if (len_expr != nullptr)
	{
		if (!IsA(len_expr, Const) || !TryExtractConstInt32((Const *) len_expr, &len_val))
			return false;
	}
	else
	{
		return false;
	}
	if (len_val < 0)
		return false;

	if (att_idx != nullptr)
		*att_idx = var->varattno - 1;
	if (source_type != nullptr)
		*source_type = arg_type;
	if (prefix_len != nullptr)
		*prefix_len = (uint32_t) len_val;
	return true;
}

static int64_t
ExtractYearFromDate32(int32_t date_val)
{
	int year;
	int month;
	int day;

	j2date(date_val + POSTGRES_EPOCH_JDATE, &year, &month, &day);
	return (int64_t) year;
}

static bool
IsInt64LikeType(Oid type)
{
	return type == NUMERICOID || type == INT8OID || type == INT4OID || type == INT2OID;
}

static bool
IsIntegerType(Oid type)
{
	return type == INT2OID || type == INT4OID || type == INT8OID;
}

static bool
IsDateLikeType(Oid type)
{
	return type == DATEOID || type == TIMESTAMPOID || type == TIMESTAMPTZOID;
}

static VecOutputStorageKind
DefaultExprVarStorageKind(Oid type)
{
	if (type == FLOAT8OID)
		return VecOutputStorageKind::Double;
	if (type == NUMERICOID)
		return VecOutputStorageKind::NumericScaledInt64;
	if (type == BPCHAROID || type == TEXTOID || type == VARCHAROID)
		return VecOutputStorageKind::StringRef;
	if (type == INT8OID)
		return VecOutputStorageKind::Int64;
	return VecOutputStorageKind::Int32;
}

static bool
ExprProducesFloat8Result(Expr *expr)
{
	OpExpr *op;
	Expr *left_expr;
	Expr *right_expr;
	Oid left_type;
	Oid right_type;
	char *opname;

	expr = StripImplicitNodes(expr);
	if (expr == nullptr)
		return false;
	if (exprType((Node *) expr) == FLOAT8OID)
		return true;
	if (!IsA(expr, OpExpr))
		return false;

	op = (OpExpr *) expr;
	if (list_length(op->args) != 2)
		return false;

	left_expr = StripImplicitNodes((Expr *) linitial(op->args));
	right_expr = StripImplicitNodes((Expr *) lsecond(op->args));
	if (left_expr == nullptr || right_expr == nullptr)
		return false;

	left_type = exprType((Node *) left_expr);
	right_type = exprType((Node *) right_expr);
	opname = get_opname(op->opno);
	if (opname == nullptr)
		return false;

	if (strcmp(opname, "/") == 0 &&
		IsInt64LikeType(left_type) && IsInt64LikeType(right_type))
		return true;

	if ((strcmp(opname, "+") == 0 ||
		 strcmp(opname, "-") == 0 ||
		 strcmp(opname, "*") == 0) &&
		(ExprProducesFloat8Result(left_expr) || ExprProducesFloat8Result(right_expr)))
		return true;

	return false;
}

static bool
IsValidNumericTypmod(int32 typmod)
{
	return typmod >= (int32) VARHDRSZ;
}

static int
ClampTrackedScale(int scale)
{
	if (scale < 0)
		return 0;
	if (scale > 18)
		return 18;
	return scale;
}

static int64_t
Pow10Int64(int scale)
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

	scale = ClampTrackedScale(scale);
	return kPowers[scale];
}

static int64_t
RescaleInt64Value(int64_t value, int from_scale, int to_scale)
{
	if (from_scale == to_scale)
		return value;

	if (from_scale < to_scale)
	{
		NumericWideInt widened = WideIntFromInt64(value) * Pow10Int64(to_scale - from_scale);
		return WideIntToInt64Checked(widened, "rescaled numeric register");
	}

	int delta = from_scale - to_scale;
	int64_t divisor = Pow10Int64(delta);
	int64_t quotient = value / divisor;
	int64_t remainder = value % divisor;
	int64_t halfway = divisor / 2;

	if (remainder >= halfway)
		quotient++;
	else if (remainder <= -halfway)
		quotient--;

	return quotient;
}

static NumericWideInt
RescaleInt64ValueWide(int64_t value, int from_scale, int to_scale)
{
	if (from_scale == to_scale)
		return WideIntFromInt64(value);

	if (from_scale < to_scale)
	{
		NumericWideInt widened = WideIntFromInt64(value);
		NumericWideInt factor = WideIntFromInt64(Pow10Int64(to_scale - from_scale));

		return WideIntMul(widened, factor);
	}

	return WideIntFromInt64(RescaleInt64Value(value, from_scale, to_scale));
}

static bool
StepHasWideConst(const VecExprStep *step)
{
	return step != nullptr &&
		step->opcode == VecOpCode::EEOP_CONST &&
		step->d.constant.has_wide_i128;
}

static NumericWideInt
StepWideConstValue(const VecExprStep *step)
{
	if (!StepHasWideConst(step))
		return WideIntFromInt64(step != nullptr ? step->d.constant.i64val : 0);
	return MakeWideIntBits(step->d.constant.wide_lo,
						   (uint64_t) step->d.constant.wide_hi);
}

static NumericWideInt
ReadRegisterWideValue(const VecExprProgram &program,
					  const VecExprStep *step,
					  int reg_idx,
					  int slot_idx,
					  const int64_t *registers_i64,
					  const int64_t *registers_i64_hi)
{
	if (StepHasWideConst(step))
		return StepWideConstValue(step);
	if (program.get_register_numeric_width(reg_idx) == VecNumericWidth::Wide128)
	{
		return MakeWideIntBits((uint64_t) registers_i64[slot_idx],
							   (uint64_t) registers_i64_hi[slot_idx]);
	}
	return WideIntFromInt64(registers_i64[slot_idx]);
}

static void
StoreRegisterWideValue(int slot_idx,
					   NumericWideInt value,
					   int64_t *registers_i64,
					   int64_t *registers_i64_hi)
{
	registers_i64[slot_idx] = (int64_t) WideIntLow64(value);
	registers_i64_hi[slot_idx] = (int64_t) WideIntHigh64(value);
}

static NumericWideInt
RescaleWideValueForRegister(NumericWideInt value, int from_scale, int to_scale)
{
	if (from_scale >= to_scale)
		return value;
	return RescaleWideIntUp(value, to_scale - from_scale);
}

static NumericWideInt
RescaleOperandForCompare(const VecExprStep *step,
						 NumericWideInt reg_value,
						 int from_scale,
						 int to_scale)
{
	if (from_scale >= to_scale)
		return reg_value;
	return RescaleWideIntUp(reg_value, to_scale - from_scale);
}

static int64_t
ScaleFloatToInt64(double value, int scale)
{
	return (int64_t) std::llround(value * (double) Pow10Int64(scale));
}

static int
GetNumericScaleForVar(const Var *var)
{
	if (var == nullptr)
		return 0;
	if (var->vartype == INT8OID)
		return 0;
	if (var->vartype != NUMERICOID)
		return 0;
	if (IsValidNumericTypmod(var->vartypmod))
		return ClampTrackedScale(GetNumericScaleFromTypmod(var->vartypmod));
	return DEFAULT_NUMERIC_SCALE;
}

static int
GetTrackedPrecisionForVar(const Var *var)
{
	if (var == nullptr)
		return 0;
	switch (var->vartype)
	{
		case INT2OID:
			return 5;
		case INT4OID:
			return 10;
		case INT8OID:
			return 19;
		case NUMERICOID:
			if (IsValidNumericTypmod(var->vartypmod))
				return GetNumericPrecisionFromTypmod(var->vartypmod);
			return 0;
		default:
			return 0;
	}
}

static int
GetNumericScaleForConst(const Const *c)
{
	if (c == nullptr)
		return 0;
	if (c->consttype == INT8OID)
		return 0;
	if (c->consttype != NUMERICOID)
		return 0;
	if (IsValidNumericTypmod(c->consttypmod))
		return ClampTrackedScale(GetNumericScaleFromTypmod(c->consttypmod));
	return DEFAULT_NUMERIC_SCALE;
}

static int
GetTrackedPrecisionForConst(const Const *c)
{
	if (c == nullptr)
		return 0;
	switch (c->consttype)
	{
		case INT2OID:
			return 5;
		case INT4OID:
			return 10;
		case INT8OID:
			return 19;
		case NUMERICOID:
			if (IsValidNumericTypmod(c->consttypmod))
				return GetNumericPrecisionFromTypmod(c->consttypmod);
			return 0;
		default:
			return 0;
	}
}

static int
ResolveResultScale(VecOpCode opcode, int left_scale, int right_scale)
{
	switch (opcode)
	{
		case VecOpCode::EEOP_INT64_ADD:
		case VecOpCode::EEOP_INT64_SUB:
		case VecOpCode::EEOP_INT64_LT:
		case VecOpCode::EEOP_INT64_LE:
		case VecOpCode::EEOP_INT64_GT:
		case VecOpCode::EEOP_INT64_GE:
		case VecOpCode::EEOP_INT64_EQ:
			return Max(left_scale, right_scale);
		case VecOpCode::EEOP_INT64_MUL:
			return ClampTrackedScale(left_scale + right_scale);
		default:
			return 0;
	}
}

static int
ResolveResultPrecision(VecOpCode opcode,
					   int left_precision,
					   int left_scale,
					   int right_precision,
					   int right_scale,
					   int res_scale)
{
	int left_integral;
	int right_integral;

	if (left_precision <= 0 || right_precision <= 0)
		return 0;

	switch (opcode)
	{
		case VecOpCode::EEOP_INT64_ADD:
		case VecOpCode::EEOP_INT64_SUB:
			left_integral = Max(left_precision - left_scale, 0);
			right_integral = Max(right_precision - right_scale, 0);
			return Max(left_integral, right_integral) + 1 + res_scale;
		case VecOpCode::EEOP_INT64_MUL:
			return left_precision + right_precision;
		case VecOpCode::EEOP_INT64_CASE:
			return Max(left_precision, right_precision);
		default:
			return 0;
	}
}

static VecNumericWidth
ResolveResultNumericWidth(VecOpCode opcode,
						  VecNumericWidth left_width,
						  VecNumericWidth right_width,
						  int left_precision,
						  int left_scale,
						  int right_precision,
						  int right_scale,
						  int res_scale)
{
	int precision;

	if (opcode != VecOpCode::EEOP_INT64_ADD &&
		opcode != VecOpCode::EEOP_INT64_SUB &&
		opcode != VecOpCode::EEOP_INT64_MUL &&
		opcode != VecOpCode::EEOP_INT64_CASE)
		return VecNumericWidth::None;

	if (left_width == VecNumericWidth::Wide128 ||
		right_width == VecNumericWidth::Wide128)
		return VecNumericWidth::Wide128;

	precision = ResolveResultPrecision(opcode,
									   left_precision, left_scale,
									   right_precision, right_scale,
									   res_scale);
	if (precision > 0)
		return WidthForNumericPrecision(precision);
	if (left_width == VecNumericWidth::Int64 &&
		right_width == VecNumericWidth::Int64)
		return VecNumericWidth::Int64;
	return VecNumericWidth::None;
}

static int
AppendDateConstStep(VecExprProgram &program, int32_t date_val)
{
	int res_idx = program.max_reg_idx++;

	if (res_idx >= MAX_REGISTERS)
		return -1;

	VecExprStep step;
	step.opcode = VecOpCode::EEOP_CONST;
	step.res_idx = res_idx;
	step.d.constant.isnull = false;
	step.d.constant.has_wide_i128 = false;
	step.d.constant.wide_lo = 0;
	step.d.constant.wide_hi = 0;
	step.d.constant.ival = date_val;
	step.d.constant.i64val = (int64_t) date_val;
	step.d.constant.fval = (double) date_val;
	program.set_register_scale(res_idx, 0);
	program.steps.push_back(step);
	return res_idx;
}

static int
AllocateResultRegister(VecExprProgram &program)
{
	int res_idx = program.max_reg_idx++;

	if (res_idx >= MAX_REGISTERS)
		return -1;
	program.set_register_scale(res_idx, 0);
	program.set_register_precision(res_idx, 0);
	program.set_register_numeric_width(res_idx, VecNumericWidth::None);
	return res_idx;
}

static bool TryExtractStringConstPrefix(Const *c, uint64_t *prefix_out, uint32_t *len_out);
static bool TryExtractLikePrefix(Const *c, uint64_t *prefix_out, uint32_t *len_out);
static bool TryExtractLikeContains(Const *c, char **str_out, uint32_t *len_out, uint64_t *prefix_out);
static bool TryExtractStringConst(Const *c, char **str_out, uint32_t *len_out, uint64_t *prefix_out);
static int CompileExprRecursive(Expr *expr, VecExprProgram &program, EState *estate);

static bool
AppendStringCompareStep(VecExprProgram &program,
						  int res_idx,
						  Expr *left_expr,
						  Expr *right_expr,
						  const char *opname)
{
	VecExprStep special_step;
	uint64_t prefix = 0;
	uint32_t len = 0;
	bool ok;
	Oid left_type;
	int left_att_idx = -1;

	if (opname == nullptr ||
		left_expr == nullptr ||
		right_expr == nullptr ||
		!IsA(right_expr, Const))
		return false;

	if (IsA(left_expr, Var))
	{
		left_att_idx = ((Var *) left_expr)->varattno - 1;
		left_type = exprType((Node *) left_expr);
	}
	else if (!MatchStringPrefixFuncExpr(left_expr, &left_att_idx, &left_type, &len))
		return false;

	if (left_type != BPCHAROID &&
		left_type != TEXTOID &&
		left_type != VARCHAROID)
		return false;

	special_step.res_idx = res_idx;
	special_step.d.str_prefix.att_idx = left_att_idx;
	special_step.d.str_prefix.prefix = 0;
	special_step.d.str_prefix.len = len;
	special_step.d.str_prefix.offset = UINT32_MAX;
	special_step.d.str_prefix.type = left_type;

	if (strcmp(opname, "~~") == 0)
	{
		char *match = nullptr;

		ok = TryExtractLikePrefix((Const *) right_expr, &prefix, &len);
		special_step.opcode = VecOpCode::EEOP_STR_PREFIX_LIKE;
		if (!ok)
		{
			ok = TryExtractLikeContains((Const *) right_expr, &match, &len, &prefix);
			special_step.opcode = VecOpCode::EEOP_STR_CONTAINS_LIKE;
			if (ok && len > 8)
				special_step.d.str_prefix.offset = program.store_string_const(match, len);
		}
		if (match != nullptr)
		{
			pfree(match);
			match = nullptr;
		}
		if (!ok)
		{
			ok = TryExtractStringConst((Const *) right_expr, &match, &len, &prefix);
			special_step.opcode = VecOpCode::EEOP_STR_LIKE_PATTERN;
			if (ok && len > 8)
				special_step.d.str_prefix.offset = program.store_string_const(match, len);
		}
		if (match != nullptr)
			pfree(match);
	}
	else if (strcmp(opname, "=") == 0)
	{
		char *match = nullptr;

		ok = TryExtractStringConst((Const *) right_expr, &match, &len, &prefix);
		if (ok && len > 8)
			special_step.d.str_prefix.offset = program.store_string_const(match, len);
		if (match != nullptr)
			pfree(match);
		special_step.opcode = IsA(StripImplicitNodes(left_expr), FuncExpr) ?
			VecOpCode::EEOP_STR_PREFIX_LIKE : VecOpCode::EEOP_STR_EQ;
	}
	else if (strcmp(opname, "<>") == 0)
	{
		char *match = nullptr;

		ok = TryExtractStringConst((Const *) right_expr, &match, &len, &prefix);
		if (ok && len > 8)
			special_step.d.str_prefix.offset = program.store_string_const(match, len);
		if (match != nullptr)
			pfree(match);
		special_step.opcode = VecOpCode::EEOP_STR_NE;
	}
	else
		return false;

	if (!ok)
		return false;

	special_step.d.str_prefix.prefix = prefix;
	special_step.d.str_prefix.len = len;
	program.steps.push_back(special_step);
	return true;
}

static void
EvalBoolAnd(uint8_t left_null, int32_t left_val,
			uint8_t right_null, int32_t right_val,
			uint8_t *out_null, int32_t *out_val)
{
	bool left_true = !left_null && left_val != 0;
	bool right_true = !right_null && right_val != 0;
	bool left_false = !left_null && left_val == 0;
	bool right_false = !right_null && right_val == 0;

	if (left_false || right_false)
	{
		*out_null = 0;
		*out_val = 0;
		return;
	}
	if (left_true && right_true)
	{
		*out_null = 0;
		*out_val = 1;
		return;
	}
	*out_null = 1;
	*out_val = 0;
}

static void
EvalBoolOr(uint8_t left_null, int32_t left_val,
		   uint8_t right_null, int32_t right_val,
		   uint8_t *out_null, int32_t *out_val)
{
	bool left_true = !left_null && left_val != 0;
	bool right_true = !right_null && right_val != 0;
	bool left_false = !left_null && left_val == 0;
	bool right_false = !right_null && right_val == 0;

	if (left_true || right_true)
	{
		*out_null = 0;
		*out_val = 1;
		return;
	}
	if (left_false && right_false)
	{
		*out_null = 0;
		*out_val = 0;
		return;
	}
	*out_null = 1;
	*out_val = 0;
}

static bool
AppendBoolCombineStep(VecExprProgram &program,
					  VecOpCode opcode,
					  int left,
					  int right,
					  int res_idx)
{
	VecExprStep step;

	if (left < 0 || right < 0 || res_idx < 0)
		return false;

	step.opcode = opcode;
	step.res_idx = res_idx;
	step.d.op.left = left;
	step.d.op.right = right;
	program.steps.push_back(step);
	return true;
}

static bool
AppendBoolNotStep(VecExprProgram &program, int arg, int res_idx)
{
	VecExprStep step;

	if (arg < 0 || res_idx < 0)
		return false;
	step.opcode = VecOpCode::EEOP_NOT;
	step.res_idx = res_idx;
	step.d.op.left = arg;
	step.d.op.right = 0;
	program.steps.push_back(step);
	return true;
}

static int
CompileScalarArrayExpr(ScalarArrayOpExpr *array_expr,
					   VecExprProgram &program,
					   int res_idx,
					   EState *estate)
{
	Expr *left_expr;
	Expr *right_expr;
	Const *array_const;
	ArrayType *array_value;
	Oid elem_type;
	char *opname;
	int16 typlen;
	bool typbyval;
	char typalign;
	Datum *elem_values;
	bool *elem_nulls;
	int nelems;
	int left = -1;

	if (array_expr == nullptr || list_length(array_expr->args) != 2)
		return -1;

	left_expr = StripImplicitNodes((Expr *) linitial(array_expr->args));
	right_expr = StripImplicitNodes((Expr *) lsecond(array_expr->args));
	if (left_expr == nullptr || right_expr == nullptr || !IsA(right_expr, Const))
		return -1;

	array_const = (Const *) right_expr;
	if (array_const->constisnull)
		return -1;

	opname = get_opname(array_expr->opno);
	if (opname == nullptr)
		return -1;

	array_value = DatumGetArrayTypeP(array_const->constvalue);
	elem_type = ARR_ELEMTYPE(array_value);
	get_typlenbyvalalign(elem_type, &typlen, &typbyval, &typalign);
	deconstruct_array(array_value,
					  elem_type,
					  typlen,
					  typbyval,
					  typalign,
					  &elem_values,
					  &elem_nulls,
					  &nelems);

	if (nelems == 0)
	{
		VecExprStep step;

		step.opcode = VecOpCode::EEOP_CONST;
		step.res_idx = res_idx;
		step.d.constant.isnull = false;
		step.d.constant.fval = 0.0;
		step.d.constant.i64val = array_expr->useOr ? 0 : 1;
		step.d.constant.ival = array_expr->useOr ? 0 : 1;
		step.d.constant.has_wide_i128 = true;
		step.d.constant.wide_lo = WideIntLow64(WideIntFromInt64(step.d.constant.i64val));
		step.d.constant.wide_hi = WideIntHigh64(WideIntFromInt64(step.d.constant.i64val));
		program.steps.push_back(step);
		return res_idx;
	}

	for (int i = 0; i < nelems; i++)
	{
		Const elem_const;
		int cmp_reg;
		OpExpr cmp_expr;

		if (elem_nulls[i])
			return -1;

		memset(&elem_const, 0, sizeof(elem_const));
		elem_const.xpr.type = T_Const;
		elem_const.consttype = elem_type;
		elem_const.consttypmod = -1;
		elem_const.constcollid = array_expr->inputcollid;
		elem_const.constlen = typlen;
		elem_const.constbyval = typbyval;
		elem_const.constisnull = false;
		elem_const.location = -1;
		elem_const.constvalue = elem_values[i];

		memset(&cmp_expr, 0, sizeof(cmp_expr));
		cmp_expr.xpr.type = T_OpExpr;
		cmp_expr.opno = array_expr->opno;
		cmp_expr.opfuncid = array_expr->opfuncid;
		cmp_expr.opresulttype = BOOLOID;
		cmp_expr.opretset = false;
		cmp_expr.opcollid = InvalidOid;
		cmp_expr.inputcollid = array_expr->inputcollid;
		cmp_expr.args = list_make2(left_expr, &elem_const);
		cmp_expr.location = -1;

		cmp_reg = CompileExprRecursive((Expr *) &cmp_expr, program, estate);
		if (cmp_reg < 0)
			return -1;

		if (left < 0)
			left = cmp_reg;
		else if (!AppendBoolCombineStep(program,
										array_expr->useOr ? VecOpCode::EEOP_OR : VecOpCode::EEOP_AND,
										left,
										cmp_reg,
										res_idx))
			return -1;
		else
			left = res_idx;
	}

	return left;
}

static bool
ResolveBinaryOpcode(const char *opname, Oid left_type, Oid right_type, VecOpCode *opcode)
{
	if (opcode == nullptr || opname == nullptr)
		return false;

	if (left_type == FLOAT8OID && right_type == FLOAT8OID)
	{
		if (strcmp(opname, "+") == 0) *opcode = VecOpCode::EEOP_FLOAT8_ADD;
		else if (strcmp(opname, "-") == 0) *opcode = VecOpCode::EEOP_FLOAT8_SUB;
		else if (strcmp(opname, "*") == 0) *opcode = VecOpCode::EEOP_FLOAT8_MUL;
		else if (strcmp(opname, "<") == 0) *opcode = VecOpCode::EEOP_FLOAT8_LT;
		else if (strcmp(opname, "<=") == 0) *opcode = VecOpCode::EEOP_FLOAT8_LE;
		else if (strcmp(opname, ">") == 0) *opcode = VecOpCode::EEOP_FLOAT8_GT;
		else if (strcmp(opname, ">=") == 0) *opcode = VecOpCode::EEOP_FLOAT8_GE;
		else return false;
		return true;
	}

	if (IsInt64LikeType(left_type) && IsInt64LikeType(right_type))
	{
		if (strcmp(opname, "+") == 0) *opcode = VecOpCode::EEOP_INT64_ADD;
		else if (strcmp(opname, "-") == 0) *opcode = VecOpCode::EEOP_INT64_SUB;
		else if (strcmp(opname, "*") == 0) *opcode = VecOpCode::EEOP_INT64_MUL;
		else if (strcmp(opname, "/") == 0) *opcode = VecOpCode::EEOP_INT64_DIV_FLOAT8;
		else if (strcmp(opname, "<") == 0) *opcode = VecOpCode::EEOP_INT64_LT;
		else if (strcmp(opname, "<=") == 0) *opcode = VecOpCode::EEOP_INT64_LE;
		else if (strcmp(opname, ">") == 0) *opcode = VecOpCode::EEOP_INT64_GT;
		else if (strcmp(opname, ">=") == 0) *opcode = VecOpCode::EEOP_INT64_GE;
		else if (strcmp(opname, "=") == 0) *opcode = VecOpCode::EEOP_INT64_EQ;
		else if (strcmp(opname, "<>") == 0) *opcode = VecOpCode::EEOP_INT64_NE;
		else return false;
		return true;
	}

	if (left_type == DATEOID && right_type == DATEOID)
	{
		if (strcmp(opname, "<") == 0) *opcode = VecOpCode::EEOP_DATE_LT;
		else if (strcmp(opname, "<=") == 0) *opcode = VecOpCode::EEOP_DATE_LE;
		else if (strcmp(opname, ">") == 0) *opcode = VecOpCode::EEOP_DATE_GT;
		else if (strcmp(opname, ">=") == 0) *opcode = VecOpCode::EEOP_DATE_GE;
		else return false;
		return true;
	}

	return false;
}

static bool
TryExtractStringConstPrefix(Const *c, uint64_t *prefix_out, uint32_t *len_out)
{
	char *str;
	uint32_t len;
	uint64_t prefix = 0;

	if (c == nullptr || c->constisnull)
		return false;
	if (c->consttype != TEXTOID &&
		c->consttype != VARCHAROID &&
		c->consttype != BPCHAROID)
		return false;

	str = TextDatumGetCString(c->constvalue);
	len = (uint32_t) strlen(str);
	if (len > 8)
	{
		pfree(str);
		return false;
	}
	memcpy(&prefix, str, len);
	pfree(str);
	if (prefix_out != nullptr)
		*prefix_out = prefix;
	if (len_out != nullptr)
		*len_out = len;
	return true;
}

static bool
TryExtractStringConst(Const *c, char **str_out, uint32_t *len_out, uint64_t *prefix_out)
{
	char *str;
	uint32_t len;
	uint64_t prefix = 0;

	if (str_out != nullptr)
		*str_out = nullptr;
	if (c == nullptr || c->constisnull)
		return false;
	if (c->consttype != TEXTOID &&
		c->consttype != VARCHAROID &&
		c->consttype != BPCHAROID)
		return false;

	str = TextDatumGetCString(c->constvalue);
	len = (uint32_t) strlen(str);
	memcpy(&prefix, str, len > 8 ? 8 : len);
	if (str_out != nullptr)
		*str_out = str;
	else
		pfree(str);
	if (len_out != nullptr)
		*len_out = len;
	if (prefix_out != nullptr)
		*prefix_out = prefix;
	return true;
}

static bool
TryExtractLikePrefix(Const *c, uint64_t *prefix_out, uint32_t *len_out)
{
	char *pattern;
	size_t len;
	uint64_t prefix = 0;

	if (c == nullptr || c->constisnull)
		return false;
	if (c->consttype != TEXTOID &&
		c->consttype != VARCHAROID &&
		c->consttype != BPCHAROID)
		return false;

	pattern = TextDatumGetCString(c->constvalue);
	len = strlen(pattern);
	if (len == 0 || pattern[len - 1] != '%')
	{
		pfree(pattern);
		return false;
	}
	for (size_t i = 0; i + 1 < len; i++)
	{
		if (pattern[i] == '%' || pattern[i] == '_')
		{
			pfree(pattern);
			return false;
		}
	}
	if (len - 1 > 8)
	{
		pfree(pattern);
		return false;
	}
	memcpy(&prefix, pattern, len - 1);
	if (prefix_out != nullptr)
		*prefix_out = prefix;
	if (len_out != nullptr)
		*len_out = (uint32_t) (len - 1);
	pfree(pattern);
	return true;
}

static bool
TryExtractLikeContains(Const *c, char **str_out, uint32_t *len_out, uint64_t *prefix_out)
{
	char *pattern;
	size_t len;
	size_t inner_len;
	char *match;
	uint64_t prefix = 0;

	if (c == nullptr || c->constisnull)
		return false;
	if (c->consttype != TEXTOID &&
		c->consttype != VARCHAROID &&
		c->consttype != BPCHAROID)
		return false;

	pattern = TextDatumGetCString(c->constvalue);
	len = strlen(pattern);
	if (len < 2 || pattern[0] != '%' || pattern[len - 1] != '%')
	{
		pfree(pattern);
		return false;
	}
	for (size_t i = 1; i + 1 < len; i++)
	{
		if (pattern[i] == '%' || pattern[i] == '_')
		{
			pfree(pattern);
			return false;
		}
	}

	inner_len = len - 2;
	match = (char *) palloc(inner_len + 1);
	memcpy(match, pattern + 1, inner_len);
	match[inner_len] = '\0';
	if (inner_len > 0)
		memcpy(&prefix, match, inner_len > 8 ? 8 : inner_len);

	if (str_out != nullptr)
		*str_out = match;
	else
		pfree(match);
	if (len_out != nullptr)
		*len_out = (uint32_t) inner_len;
	if (prefix_out != nullptr)
		*prefix_out = prefix;
	pfree(pattern);
	return true;
}

VecExprProgram::VecExprProgram()
	: steps(PgMemoryContextAllocator<VecExprStep>(CurrentMemoryContext)),
	  max_reg_idx(0), final_res_idx(-1), jit_func(nullptr), jit_context(nullptr),
	  string_constants(PgMemoryContextAllocator<char>(CurrentMemoryContext))
{
	registers_i32 = (int32_t *) palloc(sizeof(int32_t) * MAX_REGISTERS * DEFAULT_CHUNK_SIZE);
	registers_i64 = (int64_t *) palloc(sizeof(int64_t) * MAX_REGISTERS * DEFAULT_CHUNK_SIZE);
	registers_i64_hi = (int64_t *) palloc(sizeof(int64_t) * MAX_REGISTERS * DEFAULT_CHUNK_SIZE);
	registers_f8 = (double *) palloc(sizeof(double) * MAX_REGISTERS * DEFAULT_CHUNK_SIZE);
	registers_nulls = (uint8_t *) palloc(sizeof(uint8_t) * MAX_REGISTERS * DEFAULT_CHUNK_SIZE);
	reset_register_scales();
}

VecExprProgram::~VecExprProgram()
{
	pfree(registers_i32);
	pfree(registers_i64);
	pfree(registers_i64_hi);
	pfree(registers_f8);
	pfree(registers_nulls);
#ifdef USE_LLVM
	if (jit_context)
		pg_yaap_release_llvm_jit_context((JitContext *) jit_context);
#endif
}

void
VecExprProgram::try_compile_jit()
{
#ifdef USE_LLVM
	for (int reg_idx = 0; reg_idx < max_reg_idx; reg_idx++)
	{
		if (get_register_numeric_width(reg_idx) == VecNumericWidth::Wide128)
			return;
	}
	for (const auto &step : steps)
	{
		if (step.opcode == VecOpCode::EEOP_VAR &&
			step.d.var.storage_kind == VecOutputStorageKind::NumericAvgPair)
			return;
		if (step.opcode == VecOpCode::EEOP_INT64_DIV_FLOAT8)
			return;
	}
	{
		bool has_complex = false;

		for (const auto &step : steps)
		{
			switch (step.opcode)
			{
				case VecOpCode::EEOP_STR_EQ:
				case VecOpCode::EEOP_STR_NE:
				case VecOpCode::EEOP_STR_PREFIX_LIKE:
				case VecOpCode::EEOP_STR_CONTAINS_LIKE:
				case VecOpCode::EEOP_STR_LIKE_PATTERN:
				case VecOpCode::EEOP_AND:
				case VecOpCode::EEOP_OR:
				case VecOpCode::EEOP_NOT:
				case VecOpCode::EEOP_INT64_CASE:
				case VecOpCode::EEOP_FLOAT8_CASE:
					has_complex = true;
					break;
				default:
					break;
			}
		}
		if (!has_complex)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: expr JIT skipped (pure arithmetic, interpreter SIMD)");
			return;
		}
	}
	if (jit_func != nullptr || jit_context != nullptr)
		return;
	if (final_res_idx < 0)
		return;
	const char *fr = nullptr;
	if (!pg_yaap_try_compile_jit_expr(this, &jit_func, (JitContext **) &jit_context, &fr))
	{
		if (pg_yaap_trace_hooks)
		{
			elog(LOG, "pg_yaap: expr JIT compile skipped or failed (steps=%zu, reason=%s)",
				 steps.size(), fr != nullptr ? fr : "unknown");
			for (size_t i = 0; i < steps.size(); i++)
			{
				const auto &step = steps[i];

				elog(LOG,
					 "pg_yaap: expr JIT step[%zu] opcode=%s res=%d left=%d right=%d scale=%d",
					 i,
					 VecOpCodeName(step.opcode),
					 step.res_idx,
					 step.d.op.left,
					 step.d.op.right,
					 get_register_scale(step.res_idx));
			}
		}
		jit_func = nullptr;
	}
	else if (pg_yaap_trace_hooks)
		elog(LOG, "pg_yaap: expr JIT compiled successfully (steps=%zu, func=%p)",
			 steps.size(), (void *) jit_func);
#endif
}

static void
ApplyQualSelection(DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
				   const uint8_t *nulls,
				   const int32_t *values)
{
	int count = 0;
	int n = chunk.has_selection ? chunk.sel.count : chunk.count;

	for (int s = 0; s < n; s++)
	{
		int row_idx = chunk.has_selection ? chunk.sel.row_ids[s] : s;

		if (!nulls[row_idx] && values[row_idx])
			chunk.sel.row_ids[count++] = row_idx;
	}

	chunk.sel.count = count;
	chunk.has_selection = (count < chunk.count);
}

static uint32_t
TrimBpcharLength(const char *data, uint32_t len)
{
	while (len > 0 && data[len - 1] == ' ')
		len--;
	return len;
}

static bool
StringConstMatches(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
					const VecExprProgram &program,
					const VecStringRef &ref,
					const VecExprStep &step)
{
	uint32_t match_len = step.d.str_prefix.len;
	Oid string_type = step.d.str_prefix.type;
	uint64_t mask = 0;

	if (match_len > 0)
		mask = (match_len >= 8) ? UINT64_MAX : ((UINT64CONST(1) << (match_len * 8)) - 1);
	if (match_len > 0 && ((ref.prefix & mask) != (step.d.str_prefix.prefix & mask)))
		return false;

	if (string_type == BPCHAROID)
	{
		if (match_len <= 8 && step.d.str_prefix.offset == UINT32_MAX)
		{
			const char *prefix_bytes = reinterpret_cast<const char *>(&ref.prefix);
			uint32_t prefix_limit = Min(ref.len, (uint32_t) 8);

			if (ref.len < match_len)
				return false;
			for (uint32_t pos = match_len; pos < prefix_limit; pos++)
			{
				if (prefix_bytes[pos] != ' ')
					return false;
			}
			if (ref.len <= 8)
				return true;
			const char *lhs = chunk.get_string_ptr(ref);

			if (lhs == nullptr)
				return false;
			for (uint32_t pos = 8; pos < ref.len; pos++)
			{
				if (lhs[pos] != ' ')
					return false;
			}
			return true;
		}
		const char *lhs = chunk.get_string_ptr(ref);
		const char *rhs = (step.d.str_prefix.offset != UINT32_MAX) ?
			program.get_string_const_ptr(step.d.str_prefix.offset) : nullptr;
		uint32_t lhs_len = TrimBpcharLength(lhs, ref.len);
		uint32_t rhs_len;

		if (rhs == nullptr)
		{
			rhs_len = match_len;
			if (lhs_len != rhs_len)
				return false;
			return lhs_len == 0 ||
				memcmp(lhs, &step.d.str_prefix.prefix, lhs_len) == 0;
		}

		rhs_len = TrimBpcharLength(rhs, match_len);
		if (lhs_len != rhs_len)
			return false;
		return lhs_len == 0 || memcmp(lhs, rhs, lhs_len) == 0;
	}

	if (ref.len != match_len)
		return false;
	if (match_len <= 8 && step.d.str_prefix.offset == UINT32_MAX)
		return match_len == 0 || memcmp(chunk.get_string_ptr(ref), &step.d.str_prefix.prefix, match_len) == 0;
	return match_len == 0 ||
		memcmp(chunk.get_string_ptr(ref), program.get_string_const_ptr(step.d.str_prefix.offset), match_len) == 0;
}

static bool
StringConstContains(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
					  const VecExprProgram &program,
					  const VecStringRef &ref,
					  const VecExprStep &step)
{
	uint32_t match_len = step.d.str_prefix.len;
	const char *haystack;
	const char *needle;

	if (ref.len < match_len)
		return false;
	if (match_len == 0)
		return true;

	haystack = chunk.get_string_ptr(ref);
	needle = (match_len <= 8 && step.d.str_prefix.offset == UINT32_MAX) ?
		(const char *) &step.d.str_prefix.prefix :
		program.get_string_const_ptr(step.d.str_prefix.offset);

	for (uint32_t pos = 0; pos + match_len <= ref.len; pos++)
	{
		if (memcmp(haystack + pos, needle, match_len) == 0)
			return true;
	}

	return false;
}

static bool
StringLikePatternMatches(const char *text, uint32_t text_len,
						 const char *pattern, uint32_t pattern_len)
{
	uint32_t text_pos = 0;
	uint32_t pattern_pos = 0;
	uint32_t star_pattern = UINT32_MAX;
	uint32_t star_text = 0;

	while (text_pos < text_len)
	{
		if (pattern_pos < pattern_len &&
			(pattern[pattern_pos] == '_' || pattern[pattern_pos] == text[text_pos]))
		{
			pattern_pos++;
			text_pos++;
			continue;
		}
		if (pattern_pos < pattern_len && pattern[pattern_pos] == '%')
		{
			star_pattern = pattern_pos++;
			star_text = text_pos;
			continue;
		}
		if (star_pattern != UINT32_MAX)
		{
			pattern_pos = star_pattern + 1;
			text_pos = ++star_text;
			continue;
		}
		return false;
	}

	while (pattern_pos < pattern_len && pattern[pattern_pos] == '%')
		pattern_pos++;
	return pattern_pos == pattern_len;
}

static bool
StringConstLikePattern(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
					   const VecExprProgram &program,
					   const VecStringRef &ref,
					   const VecExprStep &step)
{
	const char *text = chunk.get_string_ptr(ref);
	uint32_t text_len = ref.len;
	const char *pattern;

	if (step.d.str_prefix.len == 0)
		return true;
	if (step.d.str_prefix.type == BPCHAROID)
		text_len = TrimBpcharLength(text, text_len);
	pattern = (step.d.str_prefix.len <= 8 && step.d.str_prefix.offset == UINT32_MAX) ?
		(const char *) &step.d.str_prefix.prefix :
		program.get_string_const_ptr(step.d.str_prefix.offset);
	return StringLikePatternMatches(text, text_len, pattern, step.d.str_prefix.len);
}

static int
PopulateConstStep(VecExprProgram &program,
				 VecExprStep *step,
				 Oid consttype,
				 int32 consttypmod,
				 Datum constvalue,
				 bool constisnull)
{
	if (step == nullptr)
		return -1;

	step->opcode = VecOpCode::EEOP_CONST;
	step->d.constant.isnull = constisnull;
	step->d.constant.fval = 0.0;
	step->d.constant.i64val = 0;
	step->d.constant.ival = 0;
	step->d.constant.has_wide_i128 = false;
	step->d.constant.wide_lo = 0;
	step->d.constant.wide_hi = 0;

	if (!constisnull)
	{
		int16 typlen = 0;
		bool typbyval = false;

		if (!OidIsValid(consttype))
			return -1;
		get_typlenbyval(consttype, &typlen, &typbyval);
		if (!typbyval && DatumGetPointer(constvalue) == nullptr)
			return -1;

		if (consttype == FLOAT8OID)
		{
			step->d.constant.fval = DatumGetFloat8(constvalue);
		}
		else if (consttype == NUMERICOID)
		{
			int scale = DEFAULT_NUMERIC_SCALE;
			int exact_scale;
			double fval = 0.0;
			bool decoded = false;
			bool wide_decoded = false;
			NumericWideInt wide_value = 0;

			if (IsValidNumericTypmod(consttypmod))
				scale = ClampTrackedScale(GetNumericScaleFromTypmod(consttypmod));
			else if (DatumGetPointer(constvalue) != nullptr)
				scale = ClampTrackedScale(VolVecNumericDscale(DatumGetPointer(constvalue)));
			exact_scale = scale;
			if (TryFastNumericToScaledWideInt(constvalue, exact_scale, &wide_value))
			{
				wide_decoded = true;
				step->d.constant.has_wide_i128 = true;
				step->d.constant.wide_lo = WideIntLow64(wide_value);
				step->d.constant.wide_hi = WideIntHigh64(wide_value);
				if (WideIntFitsInt64(wide_value))
				{
					step->d.constant.i64val = (int64_t) wide_value;
					decoded = true;
				}
			}
			for (int candidate_scale = Min(exact_scale, 18); !wide_decoded && !decoded && candidate_scale >= 0; candidate_scale--)
			{
				if (!TryFastNumericToScaledInt64(constvalue, candidate_scale, &step->d.constant.i64val))
					continue;
				scale = candidate_scale;
				decoded = true;
				step->d.constant.has_wide_i128 = true;
				step->d.constant.wide_lo = WideIntLow64(WideIntFromInt64(step->d.constant.i64val));
				step->d.constant.wide_hi = WideIntHigh64(WideIntFromInt64(step->d.constant.i64val));
				break;
			}
			fval = DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow, constvalue));
			if (!decoded && !wide_decoded)
			{
				scale = 0;
				step->d.constant.i64val = ScaleFloatToInt64(fval, scale);
				if (!step->d.constant.has_wide_i128)
				{
					step->d.constant.has_wide_i128 = true;
					step->d.constant.wide_lo = WideIntLow64(WideIntFromInt64(step->d.constant.i64val));
					step->d.constant.wide_hi = WideIntHigh64(WideIntFromInt64(step->d.constant.i64val));
				}
			}
			if (wide_decoded)
				scale = exact_scale;
			step->d.constant.fval = fval;
			program.set_register_scale(step->res_idx, scale);
			if (IsValidNumericTypmod(consttypmod))
				program.set_register_precision(step->res_idx,
											 GetNumericPrecisionFromTypmod(consttypmod));
			else if (step->d.constant.has_wide_i128 &&
					 !WideIntFitsInt64(MakeWideIntBits(step->d.constant.wide_lo,
													  (uint64_t) step->d.constant.wide_hi)))
				program.set_register_precision(step->res_idx, 19);
			else
				program.set_register_precision(step->res_idx,
											 CountDecimalDigitsInt64(step->d.constant.i64val));
			program.set_register_numeric_width(
				step->res_idx,
				step->d.constant.has_wide_i128 &&
				!WideIntFitsInt64(MakeWideIntBits(step->d.constant.wide_lo,
												  (uint64_t) step->d.constant.wide_hi)) ?
				VecNumericWidth::Wide128 :
				VecNumericWidth::Int64);
		}
		else if (consttype == INT8OID)
		{
			step->d.constant.i64val = DatumGetInt64(constvalue);
			step->d.constant.fval = (double) step->d.constant.i64val;
			step->d.constant.has_wide_i128 = true;
			step->d.constant.wide_lo = WideIntLow64(WideIntFromInt64(step->d.constant.i64val));
			step->d.constant.wide_hi = WideIntHigh64(WideIntFromInt64(step->d.constant.i64val));
			program.set_register_precision(step->res_idx, 19);
			program.set_register_numeric_width(step->res_idx, VecNumericWidth::Int64);
		}
		else if (consttype == DATEOID)
		{
			step->d.constant.ival = DatumGetDateADT(constvalue);
		}
		else if (consttype == TIMESTAMPOID || consttype == TIMESTAMPTZOID)
		{
			step->d.constant.i64val = DatumGetInt64(constvalue);
		}
		else
		{
			step->d.constant.ival = DatumGetInt32(constvalue);
			step->d.constant.i64val = step->d.constant.ival;
			step->d.constant.fval = (double) step->d.constant.ival;
		}
	}

	return step->res_idx;
}

static int
CompileExprRecursive(Expr *expr, VecExprProgram &program, EState *estate)
{
	expr = StripImplicitNodes(expr);
	if (expr == nullptr)
		return -1;

	int res_idx = AllocateResultRegister(program);

	if (res_idx < 0)
		return -1;

	if (IsA(expr, Var))
	{
		Var *var = (Var *) expr;
		VecExprStep step;

		step.opcode = VecOpCode::EEOP_VAR;
		step.res_idx = res_idx;
		step.d.var.att_idx = var->varattno - 1;
		step.d.var.type = var->vartype;
		step.d.var.storage_kind = DefaultExprVarStorageKind(var->vartype);
		step.d.var.storage_scale = 0;
		if (IsInt64LikeType(var->vartype))
		{
			program.set_register_scale(res_idx, GetNumericScaleForVar(var));
			program.set_register_precision(res_idx, GetTrackedPrecisionForVar(var));
			program.set_register_numeric_width(
				res_idx,
				WidthForNumericPrecision(program.get_register_precision(res_idx)));
		}
		program.steps.push_back(step);
		return res_idx;
	}

	if (IsA(expr, Param))
	{
		Param *param = (Param *) expr;
		VecExprStep step;
		ParamExecData *prm;

		if (param->paramkind != PARAM_EXEC || estate == nullptr ||
			estate->es_param_exec_vals == nullptr || param->paramid < 0)
			return -1;
		prm = &estate->es_param_exec_vals[param->paramid];
		if (!IsReadyParamExecDatum(param->paramtype, prm))
			return -1;

		step.res_idx = res_idx;
		if (PopulateConstStep(program, &step,
							  param->paramtype,
							  param->paramtypmod,
							  prm->value,
							  prm->isnull) < 0)
			return -1;
		program.steps.push_back(step);
		return res_idx;
	}

	if (IsA(expr, Const))
	{
		Const *c = (Const *) expr;
		VecExprStep step;

		step.res_idx = res_idx;
		if (PopulateConstStep(program, &step,
							  c->consttype,
							  c->consttypmod,
							  c->constvalue,
							  c->constisnull) < 0)
			return -1;
		program.steps.push_back(step);
		return res_idx;
	}

	if (IsA(expr, BoolExpr))
	{
		BoolExpr *bool_expr = (BoolExpr *) expr;
		ListCell *lc;
		int left;
		VecOpCode combine_opcode;

		if (bool_expr->args == NIL)
			return -1;
		if (bool_expr->boolop == NOT_EXPR)
		{
			int arg_idx;

			if (list_length(bool_expr->args) != 1)
				return -1;
			arg_idx = CompileExprRecursive((Expr *) linitial(bool_expr->args), program, estate);
			if (arg_idx < 0 || !AppendBoolNotStep(program, arg_idx, res_idx))
				return -1;
			return res_idx;
		}
		if (bool_expr->boolop == AND_EXPR)
			combine_opcode = VecOpCode::EEOP_AND;
		else if (bool_expr->boolop == OR_EXPR)
			combine_opcode = VecOpCode::EEOP_OR;
		else
			return -1;

		lc = list_head(bool_expr->args);
		left = CompileExprRecursive((Expr *) lfirst(lc), program, estate);
		if (left < 0)
			return -1;
		if (lnext(bool_expr->args, lc) == nullptr)
			return left;

		for_each_from(lc, bool_expr->args, 1)
		{
			int right = CompileExprRecursive((Expr *) lfirst(lc), program, estate);

			if (right < 0)
				return -1;
			if (!AppendBoolCombineStep(program, combine_opcode, left, right, res_idx))
				return -1;
			left = res_idx;
		}

		return res_idx;
	}

	if (IsA(expr, CaseExpr))
	{
		CaseExpr *case_expr = (CaseExpr *) expr;
		CaseWhen *when_clause;
		int cond_idx;
		int true_idx;
		int false_idx;
		VecExprStep step;

		if (case_expr->arg != nullptr || list_length(case_expr->args) != 1 || case_expr->defresult == nullptr)
			return -1;

		when_clause = (CaseWhen *) linitial(case_expr->args);
		cond_idx = CompileExprRecursive((Expr *) when_clause->expr, program, estate);
		true_idx = CompileExprRecursive((Expr *) when_clause->result, program, estate);
		false_idx = CompileExprRecursive((Expr *) case_expr->defresult, program, estate);
		if (cond_idx < 0 || true_idx < 0 || false_idx < 0)
			return -1;

		step.res_idx = res_idx;
		step.d.ternary.cond = cond_idx;
		step.d.ternary.if_true = true_idx;
		step.d.ternary.if_false = false_idx;
		if (IsInt64LikeType(exprType((Node *) when_clause->result)) &&
			IsInt64LikeType(exprType((Node *) case_expr->defresult)))
		{
			step.opcode = VecOpCode::EEOP_INT64_CASE;
			program.set_register_scale(
				res_idx,
				Max(program.get_register_scale(true_idx),
					program.get_register_scale(false_idx)));
			program.set_register_precision(
				res_idx,
				ResolveResultPrecision(step.opcode,
									   program.get_register_precision(true_idx),
									   program.get_register_scale(true_idx),
									   program.get_register_precision(false_idx),
									   program.get_register_scale(false_idx),
									   program.get_register_scale(res_idx)));
			program.set_register_numeric_width(
				res_idx,
				ResolveResultNumericWidth(step.opcode,
										 program.get_register_numeric_width(true_idx),
										 program.get_register_numeric_width(false_idx),
										 program.get_register_precision(true_idx),
										 program.get_register_scale(true_idx),
										 program.get_register_precision(false_idx),
										 program.get_register_scale(false_idx),
										 program.get_register_scale(res_idx)));
		}
		else if (IsIntegerType(exprType((Node *) when_clause->result)) &&
				 IsIntegerType(exprType((Node *) case_expr->defresult)))
		{
			step.opcode = VecOpCode::EEOP_INT64_CASE;
			program.set_register_scale(res_idx, 0);
		}
		else if (exprType((Node *) when_clause->result) == FLOAT8OID &&
				 exprType((Node *) case_expr->defresult) == FLOAT8OID)
		{
			step.opcode = VecOpCode::EEOP_FLOAT8_CASE;
		}
		else
			return -1;
		program.steps.push_back(step);
		return res_idx;
	}

	if (IsA(expr, ScalarArrayOpExpr))
		return CompileScalarArrayExpr((ScalarArrayOpExpr *) expr, program, res_idx, estate);

	if (IsA(expr, FuncExpr))
	{
		FuncExpr *func = (FuncExpr *) expr;
		char *funcname = get_func_name(func->funcid);
		Expr *field_expr;
		Expr *value_expr;
		Expr *cast_arg_expr;
		Oid cast_arg_type;
		int arg;
		VecExprStep step;

		if (funcname == nullptr)
			return -1;
		if (func->funcresulttype == NUMERICOID &&
			list_length(func->args) == 1)
		{
			cast_arg_expr = StripImplicitNodes((Expr *) linitial(func->args));
			cast_arg_type = exprType((Node *) cast_arg_expr);

			if (strcmp(funcname, "numeric") == 0 &&
				IsIntegerType(cast_arg_type))
				return CompileExprRecursive(cast_arg_expr, program, estate);
		}
		if (list_length(func->args) != 2)
			return -1;
		field_expr = StripImplicitNodes((Expr *) linitial(func->args));
		value_expr = StripImplicitNodes((Expr *) lsecond(func->args));
		if ((strcmp(funcname, "date_part") != 0 && strcmp(funcname, "extract") != 0) ||
			field_expr == nullptr || value_expr == nullptr ||
			!IsA(field_expr, Const) ||
			!TryExtractYearFieldConst((Const *) field_expr) ||
			exprType((Node *) value_expr) != DATEOID)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG,
					 "pg_yaap: unsupported FuncExpr lowering (func=%s nargs=%d field_node=%d value_type=%u result_type=%u)",
					 funcname,
					 list_length(func->args),
					 field_expr != nullptr ? (int) nodeTag(field_expr) : -1,
					 value_expr != nullptr ? exprType((Node *) value_expr) : InvalidOid,
					 exprType((Node *) expr));
			return -1;
		}

		arg = CompileExprRecursive(value_expr, program, estate);
		if (arg < 0)
			return -1;
		step.opcode = VecOpCode::EEOP_DATE_PART_YEAR;
		step.res_idx = res_idx;
		step.d.op.left = arg;
		step.d.op.right = 0;
		program.set_register_scale(res_idx, 0);
		program.steps.push_back(step);
		return res_idx;
	}

	if (IsA(expr, OpExpr))
	{
		OpExpr *op = (OpExpr *) expr;
		Expr *left_expr;
		Expr *right_expr;
		Oid left_type;
		Oid right_type;
		char *opname;
		int left;
		int right;
		VecExprStep step;

		if (list_length(op->args) != 2)
			return -1;

		left_expr = StripImplicitNodes((Expr *) linitial(op->args));
		right_expr = StripImplicitNodes((Expr *) lsecond(op->args));
		left_type = exprType((Node *) left_expr);
		right_type = exprType((Node *) right_expr);
		opname = get_opname(op->opno);

		if (AppendStringCompareStep(program, res_idx, left_expr, right_expr, opname))
			return res_idx;
		if (opname != nullptr &&
			strcmp(opname, "!~~") == 0)
		{
			int like_reg = AllocateResultRegister(program);

			if (like_reg < 0)
				return -1;
			if (!AppendStringCompareStep(program, like_reg, left_expr, right_expr, "~~") ||
				!AppendBoolNotStep(program, like_reg, res_idx))
				return -1;
			return res_idx;
		}

		if (left_type == DATEOID &&
			IsDateLikeType(right_type) &&
			IsA(right_expr, Const))
		{
			int32_t right_date = 0;
			VecOpCode date_opcode;

			left = CompileExprRecursive(left_expr, program, estate);
			if (left < 0 ||
				!TryFoldDateLikeConstCompare((Const *) right_expr,
											 opname,
											 &right_date,
											 &date_opcode))
				return -1;

			right = AppendDateConstStep(program, right_date);
			if (right < 0)
				return -1;

			step.res_idx = res_idx;
			step.d.op.left = left;
			step.d.op.right = right;
			step.opcode = date_opcode;

			program.steps.push_back(step);
			return res_idx;
		}

		left = CompileExprRecursive(left_expr, program, estate);
		right = CompileExprRecursive(right_expr, program, estate);
		if (left < 0 || right < 0)
			return -1;

		step.res_idx = res_idx;
		step.d.op.left = left;
		step.d.op.right = right;
		if ((strcmp(opname, "+") == 0 ||
			 strcmp(opname, "-") == 0 ||
			 strcmp(opname, "*") == 0) &&
			(ExprProducesFloat8Result(left_expr) || ExprProducesFloat8Result(right_expr)) &&
			((left_type == FLOAT8OID || IsInt64LikeType(left_type)) &&
			 (right_type == FLOAT8OID || IsInt64LikeType(right_type))))
		{
			if (strcmp(opname, "+") == 0)
				step.opcode = VecOpCode::EEOP_FLOAT8_ADD;
			else if (strcmp(opname, "-") == 0)
				step.opcode = VecOpCode::EEOP_FLOAT8_SUB;
			else
				step.opcode = VecOpCode::EEOP_FLOAT8_MUL;
		}
		else if (!ResolveBinaryOpcode(opname, left_type, right_type, &step.opcode))
			return -1;

		program.set_register_scale(
			res_idx,
			ResolveResultScale(step.opcode,
							   program.get_register_scale(left),
							   program.get_register_scale(right)));
		program.set_register_precision(
			res_idx,
			ResolveResultPrecision(step.opcode,
								   program.get_register_precision(left),
								   program.get_register_scale(left),
								   program.get_register_precision(right),
								   program.get_register_scale(right),
								   program.get_register_scale(res_idx)));
		program.set_register_numeric_width(
			res_idx,
			ResolveResultNumericWidth(step.opcode,
									 program.get_register_numeric_width(left),
									 program.get_register_numeric_width(right),
									 program.get_register_precision(left),
									 program.get_register_scale(left),
									 program.get_register_precision(right),
									 program.get_register_scale(right),
									 program.get_register_scale(res_idx)));
		program.steps.push_back(step);
		return res_idx;
	}

	return -1;
}

void
CompileExpr(Expr *expr, VecExprProgram &program, bool is_filter, EState *estate)
{
	program.steps.clear();
	program.max_reg_idx = 0;
	program.reset_register_scales();
	program.reset_register_precisions();
	program.reset_register_numeric_widths();
	program.clear_string_consts();

	int final_res = CompileExprRecursive(expr, program, estate);
	if (final_res < 0)
	{
		program.steps.clear();
		program.max_reg_idx = 0;
		program.final_res_idx = -1;
		program.clear_string_consts();
		return;
	}

	program.final_res_idx = final_res;
	if (is_filter)
	{
		VecExprStep step;

		step.opcode = VecOpCode::EEOP_QUAL;
		step.res_idx = final_res;
		program.steps.push_back(step);
	}

	program.try_compile_jit();
}

void
VecExprProgram::evaluate(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	const VecExprStep *reg_defs[MAX_REGISTERS] = {0};

	for (const auto &step : steps)
	{
		if (step.res_idx >= 0 && step.res_idx < MAX_REGISTERS)
			reg_defs[step.res_idx] = &step;
	}

	if (jit_func)
	{
		uint32_t active_count = chunk.has_selection ? chunk.sel.count : chunk.count;
		double *col_f8[16];
		int64_t *col_i64[16];
		int32_t *col_i32[16];
		VecStringRef *col_str[16];
		uint8_t *col_nulls[16];

		chunk.get_double_ptrs(col_f8);
		chunk.get_int64_ptrs(col_i64);
		chunk.get_int32_ptrs(col_i32);
		chunk.get_string_ptrs(col_str);
		chunk.get_null_ptrs(col_nulls);
		jit_func(active_count, col_f8, col_i64, col_i32, col_str, col_nulls,
				 chunk.string_arena.data(),
				 &registers_f8[final_res_idx * DEFAULT_CHUNK_SIZE],
				 &registers_i64[final_res_idx * DEFAULT_CHUNK_SIZE],
				 &registers_i32[final_res_idx * DEFAULT_CHUNK_SIZE],
				 &registers_nulls[final_res_idx * DEFAULT_CHUNK_SIZE],
				 chunk.sel.row_ids, chunk.has_selection);

		if (!steps.empty() && steps.back().opcode == VecOpCode::EEOP_QUAL)
		{
			uint8_t *rn = &registers_nulls[final_res_idx * DEFAULT_CHUNK_SIZE];
			int32_t *r32 = &registers_i32[final_res_idx * DEFAULT_CHUNK_SIZE];

			ApplyQualSelection(chunk, rn, r32);
		}
		return;
	}

	for (const auto &step : steps)
	{
		int res = step.res_idx * DEFAULT_CHUNK_SIZE;
		int l = step.d.op.left * DEFAULT_CHUNK_SIZE;
		int r = step.d.op.right * DEFAULT_CHUNK_SIZE;
		int left_scale = get_register_scale(step.d.op.left);
		int right_scale = get_register_scale(step.d.op.right);
		int res_scale = get_register_scale(step.res_idx);
		bool left_const = RegisterStepIsConst(reg_defs, step.d.op.left);
		bool right_const = RegisterStepIsConst(reg_defs, step.d.op.right);

		switch (step.opcode)
		{
			case VecOpCode::EEOP_VAR:
			{
				int att = step.d.var.att_idx;
				Oid typ = step.d.var.type;

				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = chunk.nulls[att][i];
					if (step.d.var.storage_kind == VecOutputStorageKind::NumericAvgPair)
					{
						double count = chunk.get_double(att, i);
						int scale_delta = Max(res_scale - step.d.var.storage_scale, 0);
						long double scaled_avg = 0.0L;

						if (!registers_nulls[res + i] && count <= 0.0)
							registers_nulls[res + i] = 1;
						if (!registers_nulls[res + i])
						{
							const int64_t sum = chunk.get_int64(att, i);
							scaled_avg = ((long double) sum *
										  (long double) Pow10Int64(scale_delta)) /
								(long double) count;
							registers_i64[res + i] = (int64_t) std::llround(scaled_avg);
							registers_i64_hi[res + i] =
								(registers_i64[res + i] < 0) ? -1 : 0;
							registers_f8[res + i] =
								((double) sum /
								 (double) Pow10Int64(step.d.var.storage_scale)) / count;
						}
					}
					else if (typ == FLOAT8OID)
						registers_f8[res + i] = chunk.get_double(att, i);
					else if (typ == NUMERICOID || typ == INT8OID)
					{
						const int64_t value = chunk.get_int64(att, i);
						registers_i64[res + i] = value;
						registers_i64_hi[res + i] =
							(value < 0) ? -1 : 0;
					}
					else
					{
						const int32_t value = chunk.get_int32(att, i);
						registers_i32[res + i] = value;
						if (IsIntegerType(typ))
						{
							registers_i64[res + i] = (int64_t) value;
							registers_i64_hi[res + i] =
								(value < 0) ? -1 : 0;
						}
					}
				}
				break;
			}
			case VecOpCode::EEOP_CONST:
				for (int i = 0; i < chunk.count; i++)
				{
					NumericWideInt wide_value;

					registers_nulls[res + i] = (uint8_t) step.d.constant.isnull;
					registers_f8[res + i] = step.d.constant.fval;
					wide_value = StepWideConstValue(&step);
					StoreRegisterWideValue(res + i, wide_value, registers_i64, registers_i64_hi);
					registers_i32[res + i] = step.d.constant.ival;
				}
				break;
			case VecOpCode::EEOP_FLOAT8_ADD:
			case VecOpCode::EEOP_FLOAT8_SUB:
			case VecOpCode::EEOP_FLOAT8_MUL:
				DispatchFloat8Arithmetic(step.opcode,
					registers_f8, registers_nulls,
					step.d.op.left * DEFAULT_CHUNK_SIZE,
					step.d.op.right * DEFAULT_CHUNK_SIZE,
					res,
					chunk.count,
					left_const,
					right_const);
				break;
			case VecOpCode::EEOP_INT64_ADD:
				for (int i = 0; i < chunk.count; i++)
				{
					NumericWideInt left_val;
					NumericWideInt right_val;
					NumericWideInt result;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					left_val = RescaleWideValueForRegister(
						ReadRegisterWideValue(*this, reg_defs[step.d.op.left], step.d.op.left,
											 l + i, registers_i64, registers_i64_hi),
						left_scale, res_scale);
					right_val = RescaleWideValueForRegister(
						ReadRegisterWideValue(*this, reg_defs[step.d.op.right], step.d.op.right,
											 r + i, registers_i64, registers_i64_hi),
						right_scale, res_scale);
					result = left_val + right_val;
					StoreRegisterWideValue(res + i, result, registers_i64, registers_i64_hi);
				}
				break;
			case VecOpCode::EEOP_INT64_SUB:
				for (int i = 0; i < chunk.count; i++)
				{
					NumericWideInt left_val;
					NumericWideInt right_val;
					NumericWideInt result;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					left_val = RescaleWideValueForRegister(
						ReadRegisterWideValue(*this, reg_defs[step.d.op.left], step.d.op.left,
											 l + i, registers_i64, registers_i64_hi),
						left_scale, res_scale);
					right_val = RescaleWideValueForRegister(
						ReadRegisterWideValue(*this, reg_defs[step.d.op.right], step.d.op.right,
											 r + i, registers_i64, registers_i64_hi),
						right_scale, res_scale);
					result = left_val - right_val;
					StoreRegisterWideValue(res + i, result, registers_i64, registers_i64_hi);
				}
				break;
			case VecOpCode::EEOP_INT64_MUL:
				for (int i = 0; i < chunk.count; i++)
				{
					NumericWideInt product;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					product = WideIntMul(
						ReadRegisterWideValue(*this, reg_defs[step.d.op.left], step.d.op.left,
											 l + i, registers_i64, registers_i64_hi),
						ReadRegisterWideValue(*this, reg_defs[step.d.op.right], step.d.op.right,
											 r + i, registers_i64, registers_i64_hi));
					StoreRegisterWideValue(res + i, product, registers_i64, registers_i64_hi);
				}
				break;
			case VecOpCode::EEOP_INT64_DIV_FLOAT8:
				for (int i = 0; i < chunk.count; i++)
				{
					double left_val;
					double right_val;
					NumericWideInt left_wide;
					NumericWideInt right_wide;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					if (registers_nulls[res + i])
						continue;
					left_wide = ReadRegisterWideValue(*this, reg_defs[step.d.op.left], step.d.op.left,
													 l + i, registers_i64, registers_i64_hi);
					right_wide = ReadRegisterWideValue(*this, reg_defs[step.d.op.right], step.d.op.right,
													  r + i, registers_i64, registers_i64_hi);
					left_val = (double) left_wide / (double) Pow10Int64(left_scale);
					right_val = (double) right_wide / (double) Pow10Int64(right_scale);
					if (right_val == 0.0)
						elog(ERROR, "pg_yaap numeric division by zero");
					registers_f8[res + i] = left_val / right_val;
				}
				break;
			case VecOpCode::EEOP_INT64_LT:
				for (int i = 0; i < chunk.count; i++)
				{
					int scale = Max(left_scale, right_scale);
					NumericWideInt left_val;
					NumericWideInt right_val;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					left_val = RescaleOperandForCompare(
						reg_defs[step.d.op.left],
						ReadRegisterWideValue(*this, reg_defs[step.d.op.left], step.d.op.left,
											 l + i, registers_i64, registers_i64_hi),
						left_scale, scale);
					right_val = RescaleOperandForCompare(
						reg_defs[step.d.op.right],
						ReadRegisterWideValue(*this, reg_defs[step.d.op.right], step.d.op.right,
											 r + i, registers_i64, registers_i64_hi),
						right_scale, scale);
					registers_i32[res + i] = left_val < right_val;
				}
				break;
			case VecOpCode::EEOP_INT64_LE:
				for (int i = 0; i < chunk.count; i++)
				{
					int scale = Max(left_scale, right_scale);
					NumericWideInt left_val;
					NumericWideInt right_val;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					left_val = RescaleOperandForCompare(
						reg_defs[step.d.op.left],
						ReadRegisterWideValue(*this, reg_defs[step.d.op.left], step.d.op.left,
											 l + i, registers_i64, registers_i64_hi),
						left_scale, scale);
					right_val = RescaleOperandForCompare(
						reg_defs[step.d.op.right],
						ReadRegisterWideValue(*this, reg_defs[step.d.op.right], step.d.op.right,
											 r + i, registers_i64, registers_i64_hi),
						right_scale, scale);
					registers_i32[res + i] = left_val <= right_val;
				}
				break;
			case VecOpCode::EEOP_FLOAT8_LT:
			case VecOpCode::EEOP_FLOAT8_LE:
			case VecOpCode::EEOP_FLOAT8_GT:
			case VecOpCode::EEOP_FLOAT8_GE:
				DispatchFloat8Compare(step.opcode,
					registers_f8, registers_i32, registers_nulls,
					step.d.op.left * DEFAULT_CHUNK_SIZE,
					step.d.op.right * DEFAULT_CHUNK_SIZE,
					res,
					chunk.count,
					left_const,
					right_const);
				break;
			case VecOpCode::EEOP_DATE_LE:
			case VecOpCode::EEOP_DATE_LT:
			case VecOpCode::EEOP_DATE_GT:
			case VecOpCode::EEOP_DATE_GE:
				DispatchDateCompare(step.opcode,
					registers_i32, registers_nulls,
					step.d.op.left * DEFAULT_CHUNK_SIZE,
					step.d.op.right * DEFAULT_CHUNK_SIZE,
					res,
					chunk.count,
					left_const,
					right_const);
				break;
			case VecOpCode::EEOP_DATE_PART_YEAR:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i];
					if (!registers_nulls[res + i])
					{
						registers_i64[res + i] = ExtractYearFromDate32(registers_i32[l + i]);
						registers_i64_hi[res + i] = (registers_i64[res + i] < 0) ? -1 : 0;
					}
				}
				break;
			case VecOpCode::EEOP_INT64_GT:
				for (int i = 0; i < chunk.count; i++)
				{
					int scale = Max(left_scale, right_scale);
					NumericWideInt left_val;
					NumericWideInt right_val;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					left_val = RescaleOperandForCompare(
						reg_defs[step.d.op.left],
						ReadRegisterWideValue(*this, reg_defs[step.d.op.left], step.d.op.left,
											 l + i, registers_i64, registers_i64_hi),
						left_scale, scale);
					right_val = RescaleOperandForCompare(
						reg_defs[step.d.op.right],
						ReadRegisterWideValue(*this, reg_defs[step.d.op.right], step.d.op.right,
											 r + i, registers_i64, registers_i64_hi),
						right_scale, scale);
					registers_i32[res + i] = left_val > right_val;
				}
				break;
			case VecOpCode::EEOP_INT64_GE:
				for (int i = 0; i < chunk.count; i++)
				{
					int scale = Max(left_scale, right_scale);
					NumericWideInt left_val;
					NumericWideInt right_val;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					left_val = RescaleOperandForCompare(
						reg_defs[step.d.op.left],
						ReadRegisterWideValue(*this, reg_defs[step.d.op.left], step.d.op.left,
											 l + i, registers_i64, registers_i64_hi),
						left_scale, scale);
					right_val = RescaleOperandForCompare(
						reg_defs[step.d.op.right],
						ReadRegisterWideValue(*this, reg_defs[step.d.op.right], step.d.op.right,
											 r + i, registers_i64, registers_i64_hi),
						right_scale, scale);
					registers_i32[res + i] = left_val >= right_val;
				}
				break;
			case VecOpCode::EEOP_INT64_EQ:
			case VecOpCode::EEOP_INT64_NE:
				for (int i = 0; i < chunk.count; i++)
				{
					int scale = Max(left_scale, right_scale);
					NumericWideInt left_val;
					NumericWideInt right_val;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					left_val = RescaleOperandForCompare(
						reg_defs[step.d.op.left],
						ReadRegisterWideValue(*this, reg_defs[step.d.op.left], step.d.op.left,
											 l + i, registers_i64, registers_i64_hi),
						left_scale, scale);
					right_val = RescaleOperandForCompare(
						reg_defs[step.d.op.right],
						ReadRegisterWideValue(*this, reg_defs[step.d.op.right], step.d.op.right,
											 r + i, registers_i64, registers_i64_hi),
						right_scale, scale);
					registers_i32[res + i] =
						(step.opcode == VecOpCode::EEOP_INT64_EQ) ?
						(left_val == right_val) :
						(left_val != right_val);
				}
				break;
			case VecOpCode::EEOP_AND:
				for (int i = 0; i < chunk.count; i++)
				{
					EvalBoolAnd(registers_nulls[l + i], registers_i32[l + i],
								registers_nulls[r + i], registers_i32[r + i],
								&registers_nulls[res + i], &registers_i32[res + i]);
				}
				break;
			case VecOpCode::EEOP_OR:
				for (int i = 0; i < chunk.count; i++)
				{
					EvalBoolOr(registers_nulls[l + i], registers_i32[l + i],
							   registers_nulls[r + i], registers_i32[r + i],
							   &registers_nulls[res + i], &registers_i32[res + i]);
				}
				break;
			case VecOpCode::EEOP_NOT:
				DispatchBoolNot(registers_i32, registers_nulls,
					step.d.op.left * DEFAULT_CHUNK_SIZE,
					res,
					chunk.count);
				break;
			case VecOpCode::EEOP_INT64_CASE:
			{
				int c = step.d.ternary.cond * DEFAULT_CHUNK_SIZE;
				int t = step.d.ternary.if_true * DEFAULT_CHUNK_SIZE;
				int f = step.d.ternary.if_false * DEFAULT_CHUNK_SIZE;
				int true_scale = get_register_scale(step.d.ternary.if_true);
				int false_scale = get_register_scale(step.d.ternary.if_false);

				for (int i = 0; i < chunk.count; i++)
				{
					bool cond_null = registers_nulls[c + i] != 0;
					bool take_true = (!cond_null && registers_i32[c + i] != 0);
					int src = take_true ? t : f;
					int src_reg = take_true ? step.d.ternary.if_true : step.d.ternary.if_false;
					int src_scale = take_true ? true_scale : false_scale;
					NumericWideInt value;

					registers_nulls[res + i] = registers_nulls[src + i];
					value = RescaleWideValueForRegister(
						ReadRegisterWideValue(*this, reg_defs[src_reg], src_reg,
											 src + i, registers_i64, registers_i64_hi),
						src_scale, res_scale);
					StoreRegisterWideValue(res + i, value, registers_i64, registers_i64_hi);
				}
				break;
			}
			case VecOpCode::EEOP_FLOAT8_CASE:
			{
				int c = step.d.ternary.cond * DEFAULT_CHUNK_SIZE;
				int t = step.d.ternary.if_true * DEFAULT_CHUNK_SIZE;
				int f = step.d.ternary.if_false * DEFAULT_CHUNK_SIZE;

				DispatchFloat8Case(registers_i32, registers_f8, registers_nulls,
					c, t, f, res, chunk.count);
				break;
			}
				case VecOpCode::EEOP_STR_PREFIX_LIKE:
				{
					int att = step.d.str_prefix.att_idx;
					uint32_t prefix_len = step.d.str_prefix.len;
					uint64_t mask = 0;

				if (prefix_len > 0)
					mask = (prefix_len >= 8) ? UINT64_MAX : ((UINT64CONST(1) << (prefix_len * 8)) - 1);
				for (int i = 0; i < chunk.count; i++)
				{
					VecStringRef ref = chunk.get_string_ref(att, i);

					registers_nulls[res + i] = chunk.nulls[att][i];
					registers_i32[res + i] =
						(!registers_nulls[res + i] &&
							 ref.len >= prefix_len &&
							 (prefix_len == 0 || ((ref.prefix & mask) == (step.d.str_prefix.prefix & mask))));
					}
					break;
				}
			case VecOpCode::EEOP_STR_CONTAINS_LIKE:
				{
					int att = step.d.str_prefix.att_idx;

					for (int i = 0; i < chunk.count; i++)
					{
						VecStringRef ref = chunk.get_string_ref(att, i);

						registers_nulls[res + i] = chunk.nulls[att][i];
						registers_i32[res + i] = !registers_nulls[res + i] &&
							StringConstContains(chunk, *this, ref, step);
					}
					break;
				}
			case VecOpCode::EEOP_STR_LIKE_PATTERN:
				{
					int att = step.d.str_prefix.att_idx;

					for (int i = 0; i < chunk.count; i++)
					{
						VecStringRef ref = chunk.get_string_ref(att, i);

						registers_nulls[res + i] = chunk.nulls[att][i];
						registers_i32[res + i] = !registers_nulls[res + i] &&
							StringConstLikePattern(chunk, *this, ref, step);
					}
					break;
				}
			case VecOpCode::EEOP_STR_EQ:
				{
					int att = step.d.str_prefix.att_idx;

					for (int i = 0; i < chunk.count; i++)
					{
						VecStringRef ref = chunk.get_string_ref(att, i);

						registers_nulls[res + i] = chunk.nulls[att][i];
						registers_i32[res + i] = !registers_nulls[res + i] &&
							StringConstMatches(chunk, *this, ref, step);
					}
					break;
				}
				case VecOpCode::EEOP_STR_NE:
				{
					int att = step.d.str_prefix.att_idx;

					for (int i = 0; i < chunk.count; i++)
					{
						VecStringRef ref = chunk.get_string_ref(att, i);

						registers_nulls[res + i] = chunk.nulls[att][i];
						registers_i32[res + i] = registers_nulls[res + i] ? 0 :
							!StringConstMatches(chunk, *this, ref, step);
					}
					break;
				}
				case VecOpCode::EEOP_QUAL:
					ApplyQualSelection(chunk, &registers_nulls[res], &registers_i32[res]);
					break;
			default:
				break;
		}
	}
}

} /* namespace pg_yaap */
