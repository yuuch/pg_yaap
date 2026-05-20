#pragma once

#include "core/data_chunk_deform.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace pg_yaap
{

struct OpAdd {
	template <typename T>
	static T Op(T a, T b) { return a + b; }
};

struct OpSub {
	template <typename T>
	static T Op(T a, T b) { return a - b; }
};

struct OpMul {
	template <typename T>
	static T Op(T a, T b) { return a * b; }
};

struct OpLT {
	template <typename T>
	static bool Op(T a, T b) { return a < b; }
};

struct OpLE {
	template <typename T>
	static bool Op(T a, T b) { return a <= b; }
};

struct OpGT {
	template <typename T>
	static bool Op(T a, T b) { return a > b; }
};

struct OpGE {
	template <typename T>
	static bool Op(T a, T b) { return a >= b; }
};

struct OpNot {
	template <typename T>
	static T Op(T a) { return a == 0; }
};

template <typename LEFT, typename RIGHT, typename OUT, class OP,
		  bool LEFT_CONSTANT = false, bool RIGHT_CONSTANT = false>
struct BinaryExecutor
{
	static OUT Compute(const LEFT *__restrict ldata,
					 const RIGHT *__restrict rdata,
					 uint16_t idx)
	{
		const LEFT lval = LEFT_CONSTANT ? ldata[0] : ldata[idx];
		const RIGHT rval = RIGHT_CONSTANT ? rdata[0] : rdata[idx];

		return OP::template Op<LEFT>(lval, static_cast<LEFT>(rval));
	}

	static void ExecuteNoNulls(const LEFT *__restrict ldata,
							 const RIGHT *__restrict rdata,
							 OUT *__restrict result_data,
							 uint8_t *__restrict result_nulls,
							 uint16_t count)
	{
		for (uint16_t i = 0; i < count; i++)
		{
			result_nulls[i] = 0;
			result_data[i] = Compute(ldata, rdata, i);
		}
	}

	static void Execute(const LEFT *__restrict ldata,
						 const RIGHT *__restrict rdata,
						 OUT *__restrict result_data,
						 const uint8_t *__restrict lnulls,
						 const uint8_t *__restrict rnulls,
						 uint8_t *__restrict result_nulls,
						 uint16_t count)
	{
		bool any_nulls = false;

		if (lnulls != nullptr && std::memchr(lnulls, 1, count) != nullptr)
			any_nulls = true;
		if (!any_nulls && rnulls != nullptr && std::memchr(rnulls, 1, count) != nullptr)
			any_nulls = true;

		if (!any_nulls)
		{
			ExecuteNoNulls(ldata, rdata, result_data, result_nulls, count);
			return;
		}

		for (uint16_t i = 0; i < count; i++)
		{
			const bool left_null = (lnulls != nullptr) ? lnulls[i] != 0 : false;
			const bool right_null = (rnulls != nullptr) ? rnulls[i] != 0 : false;

			result_nulls[i] = left_null || right_null;
			result_data[i] = Compute(ldata, rdata, i);
		}
	}
};

template <typename IN, typename OUT, class OP>
struct UnaryExecutor
{
	static void ExecuteNoNulls(const IN *__restrict input_data,
							 OUT *__restrict result_data,
							 uint8_t *__restrict result_nulls,
							 uint16_t count)
	{
		for (uint16_t i = 0; i < count; i++)
		{
			result_nulls[i] = 0;
			result_data[i] = OP::template Op<IN>(input_data[i]);
		}
	}

	static void Execute(const IN *__restrict input_data,
					 OUT *__restrict result_data,
					 const uint8_t *__restrict input_nulls,
					 uint8_t *__restrict result_nulls,
					 uint16_t count)
	{
		const bool any_nulls = input_nulls != nullptr && std::memchr(input_nulls, 1, count) != nullptr;

		if (!any_nulls)
		{
			ExecuteNoNulls(input_data, result_data, result_nulls, count);
			return;
		}

		for (uint16_t i = 0; i < count; i++)
		{
			result_nulls[i] = input_nulls[i];
			result_data[i] = OP::template Op<IN>(input_data[i]);
		}
	}
};

template <typename T>
struct TernaryExecutor
{
	static void Execute(const int32_t *__restrict cond_data,
					 const uint8_t *__restrict cond_nulls,
					 const T *__restrict true_data,
					 const uint8_t *__restrict true_nulls,
					 const T *__restrict false_data,
					 const uint8_t *__restrict false_nulls,
					 T *__restrict result_data,
					 uint8_t *__restrict result_nulls,
					 uint16_t count)
	{
		for (uint16_t i = 0; i < count; i++)
		{
			const bool take_true = !(cond_nulls != nullptr && cond_nulls[i] != 0) && cond_data[i] != 0;

			result_nulls[i] = take_true ? (true_nulls != nullptr ? true_nulls[i] : 0) :
				(false_nulls != nullptr ? false_nulls[i] : 0);
			result_data[i] = take_true ? true_data[i] : false_data[i];
		}
	}
};

template <typename LEFT, typename RIGHT, typename OUT, class OP>
inline void
ExecuteBinaryDispatch(const LEFT *__restrict lhs,
					 const RIGHT *__restrict rhs,
					 OUT *__restrict out,
					 const uint8_t *__restrict lhs_n,
					 const uint8_t *__restrict rhs_n,
					 uint8_t *__restrict out_n,
					 uint16_t count,
					 bool left_constant,
					 bool right_constant)
{
	if (left_constant)
	{
		if (right_constant)
			BinaryExecutor<LEFT, RIGHT, OUT, OP, true, true>::Execute(lhs, rhs, out, lhs_n, rhs_n, out_n, count);
		else
			BinaryExecutor<LEFT, RIGHT, OUT, OP, true, false>::Execute(lhs, rhs, out, lhs_n, rhs_n, out_n, count);
	}
	else if (right_constant)
		BinaryExecutor<LEFT, RIGHT, OUT, OP, false, true>::Execute(lhs, rhs, out, lhs_n, rhs_n, out_n, count);
	else
		BinaryExecutor<LEFT, RIGHT, OUT, OP, false, false>::Execute(lhs, rhs, out, lhs_n, rhs_n, out_n, count);
}

inline void
DispatchFloat8Arithmetic(VecOpCode opcode,
						 double *registers_f8,
						 uint8_t *registers_nulls,
						 int l,
						 int r,
						 int res,
						 uint16_t count,
						 bool left_constant,
						 bool right_constant)
{
	double *__restrict out = &registers_f8[res];
	const double *__restrict lhs = &registers_f8[l];
	const double *__restrict rhs = &registers_f8[r];
	uint8_t *__restrict out_n = &registers_nulls[res];
	const uint8_t *__restrict lhs_n = &registers_nulls[l];
	const uint8_t *__restrict rhs_n = &registers_nulls[r];

	switch (opcode)
	{
		case VecOpCode::EEOP_FLOAT8_ADD:
			ExecuteBinaryDispatch<double, double, double, OpAdd>(
				lhs, rhs, out, lhs_n, rhs_n, out_n, count, left_constant, right_constant);
			break;
		case VecOpCode::EEOP_FLOAT8_SUB:
			ExecuteBinaryDispatch<double, double, double, OpSub>(
				lhs, rhs, out, lhs_n, rhs_n, out_n, count, left_constant, right_constant);
			break;
		case VecOpCode::EEOP_FLOAT8_MUL:
			ExecuteBinaryDispatch<double, double, double, OpMul>(
				lhs, rhs, out, lhs_n, rhs_n, out_n, count, left_constant, right_constant);
			break;
		default:
			break;
	}
}

inline void
DispatchFloat8Compare(VecOpCode opcode,
					 double *registers_f8,
					 int32_t *registers_i32,
					 uint8_t *registers_nulls,
					 int l,
					 int r,
					 int res,
					 uint16_t count,
					 bool left_constant,
					 bool right_constant)
{
	int32_t *__restrict out = &registers_i32[res];
	const double *__restrict lhs = &registers_f8[l];
	const double *__restrict rhs = &registers_f8[r];
	uint8_t *__restrict out_n = &registers_nulls[res];
	const uint8_t *__restrict lhs_n = &registers_nulls[l];
	const uint8_t *__restrict rhs_n = &registers_nulls[r];

	switch (opcode)
	{
		case VecOpCode::EEOP_FLOAT8_LT:
			ExecuteBinaryDispatch<double, double, int32_t, OpLT>(
				lhs, rhs, out, lhs_n, rhs_n, out_n, count, left_constant, right_constant);
			break;
		case VecOpCode::EEOP_FLOAT8_LE:
			ExecuteBinaryDispatch<double, double, int32_t, OpLE>(
				lhs, rhs, out, lhs_n, rhs_n, out_n, count, left_constant, right_constant);
			break;
		case VecOpCode::EEOP_FLOAT8_GT:
			ExecuteBinaryDispatch<double, double, int32_t, OpGT>(
				lhs, rhs, out, lhs_n, rhs_n, out_n, count, left_constant, right_constant);
			break;
		case VecOpCode::EEOP_FLOAT8_GE:
			ExecuteBinaryDispatch<double, double, int32_t, OpGE>(
				lhs, rhs, out, lhs_n, rhs_n, out_n, count, left_constant, right_constant);
			break;
		default:
			break;
	}
}

inline void
DispatchDateCompare(VecOpCode opcode,
					 int32_t *registers_i32,
					 uint8_t *registers_nulls,
					 int l,
					 int r,
					 int res,
					 uint16_t count,
					 bool left_constant,
					 bool right_constant)
{
	int32_t *__restrict out = &registers_i32[res];
	const int32_t *__restrict lhs = &registers_i32[l];
	const int32_t *__restrict rhs = &registers_i32[r];
	uint8_t *__restrict out_n = &registers_nulls[res];
	const uint8_t *__restrict lhs_n = &registers_nulls[l];
	const uint8_t *__restrict rhs_n = &registers_nulls[r];

	switch (opcode)
	{
		case VecOpCode::EEOP_DATE_LT:
			ExecuteBinaryDispatch<int32_t, int32_t, int32_t, OpLT>(
				lhs, rhs, out, lhs_n, rhs_n, out_n, count, left_constant, right_constant);
			break;
		case VecOpCode::EEOP_DATE_LE:
			ExecuteBinaryDispatch<int32_t, int32_t, int32_t, OpLE>(
				lhs, rhs, out, lhs_n, rhs_n, out_n, count, left_constant, right_constant);
			break;
		case VecOpCode::EEOP_DATE_GT:
			ExecuteBinaryDispatch<int32_t, int32_t, int32_t, OpGT>(
				lhs, rhs, out, lhs_n, rhs_n, out_n, count, left_constant, right_constant);
			break;
		case VecOpCode::EEOP_DATE_GE:
			ExecuteBinaryDispatch<int32_t, int32_t, int32_t, OpGE>(
				lhs, rhs, out, lhs_n, rhs_n, out_n, count, left_constant, right_constant);
			break;
		default:
			break;
	}
}

inline void
DispatchBoolNot(int32_t *registers_i32,
				uint8_t *registers_nulls,
				int l,
				int res,
				uint16_t count)
{
	int32_t *__restrict out = &registers_i32[res];
	const int32_t *__restrict input = &registers_i32[l];
	uint8_t *__restrict out_n = &registers_nulls[res];
	const uint8_t *__restrict input_n = &registers_nulls[l];

	UnaryExecutor<int32_t, int32_t, OpNot>::Execute(input, out, input_n, out_n, count);
}

inline void
DispatchFloat8Case(int32_t *registers_i32,
				   double *registers_f8,
				   uint8_t *registers_nulls,
				   int c,
				   int t,
				   int f,
				   int res,
				   uint16_t count)
{
	const int32_t *__restrict cond_data = &registers_i32[c];
	const uint8_t *__restrict cond_nulls = &registers_nulls[c];
	const double *__restrict true_data = &registers_f8[t];
	const uint8_t *__restrict true_nulls = &registers_nulls[t];
	const double *__restrict false_data = &registers_f8[f];
	const uint8_t *__restrict false_nulls = &registers_nulls[f];
	double *__restrict result_data = &registers_f8[res];
	uint8_t *__restrict result_nulls = &registers_nulls[res];

	TernaryExecutor<double>::Execute(cond_data,
		cond_nulls,
		true_data,
		true_nulls,
		false_data,
		false_nulls,
		result_data,
		result_nulls,
		count);
}

} // namespace pg_yaap
