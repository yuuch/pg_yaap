#pragma once

#include <memory>
#include <limits>
#include <new>
#include <utility>
#include <vector>
#include <unordered_map>
#include <queue>
#include <cstdint>
#include <cstring>
#include <string>

extern "C" {
#include "postgres.h"
#include "utils/rel.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/tupdesc_details.h"
#include "nodes/plannodes.h"
#include "executor/executor.h"
#include "jit/jit.h"
#include "storage/bufmgr.h"
#include "storage/buffile.h"
#include "storage/read_stream.h"
#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "access/visibilitymap.h"
#include "optimizer/optimizer.h"
}

namespace pg_yaap
{

static constexpr uint16_t DEFAULT_CHUNK_SIZE = 2048;
static constexpr int MAX_REGISTERS = 64;
static constexpr int DEFAULT_NUMERIC_SCALE = 2;
static constexpr int VOLVEC_DEC_DIGITS = 4;
static constexpr int VOLVEC_NBASE = 10000;

enum class VecNumericWidth : uint8_t {
	None = 0,
	Int64 = 1,
	Wide128 = 2
};

#if defined(__SIZEOF_INT128__)
using NumericWideInt = __int128_t;

static inline NumericWideInt
MakeWideIntBits(uint64_t lo, uint64_t hi)
{
	return (((NumericWideInt) ((__int128_t) ((int64_t) hi))) << 64) |
		(NumericWideInt) lo;
}

static inline uint64_t
WideIntLow64(NumericWideInt value)
{
	return (uint64_t) value;
}

static inline int64_t
WideIntHigh64(NumericWideInt value)
{
	return (int64_t) (((__int128_t) value) >> 64);
}
#else
struct NumericWideInt
{
	uint64_t	lo;
	int64_t		hi;

	constexpr NumericWideInt() : lo(0), hi(0) {}
	constexpr NumericWideInt(int64_t value)
		: lo((uint64_t) value), hi(value < 0 ? -1 : 0) {}
	constexpr NumericWideInt(uint64_t lo_bits, int64_t hi_bits)
		: lo(lo_bits), hi(hi_bits) {}
};

static inline NumericWideInt
MakeWideIntBits(uint64_t lo, uint64_t hi)
{
	return NumericWideInt(lo, (int64_t) hi);
}

static inline uint64_t
WideIntLow64(NumericWideInt value)
{
	return value.lo;
}

static inline int64_t
WideIntHigh64(NumericWideInt value)
{
	return value.hi;
}

static inline void
Mul64Wide(uint64_t left, uint64_t right, uint64_t *hi, uint64_t *lo)
{
	uint64_t left_lo = (uint32_t) left;
	uint64_t left_hi = left >> 32;
	uint64_t right_lo = (uint32_t) right;
	uint64_t right_hi = right >> 32;
	uint64_t prod_ll = left_lo * right_lo;
	uint64_t prod_lh = left_lo * right_hi;
	uint64_t prod_hl = left_hi * right_lo;
	uint64_t prod_hh = left_hi * right_hi;
	uint64_t middle = (prod_ll >> 32) + (uint32_t) prod_lh + (uint32_t) prod_hl;

	*lo = (middle << 32) | (uint32_t) prod_ll;
	*hi = prod_hh + (prod_lh >> 32) + (prod_hl >> 32) + (middle >> 32);
}

static inline bool
operator==(const NumericWideInt &left, const NumericWideInt &right)
{
	return left.hi == right.hi && left.lo == right.lo;
}

static inline bool
operator!=(const NumericWideInt &left, const NumericWideInt &right)
{
	return !(left == right);
}

static inline bool
operator<(const NumericWideInt &left, const NumericWideInt &right)
{
	if (left.hi != right.hi)
		return left.hi < right.hi;
	return left.lo < right.lo;
}

static inline bool
operator<=(const NumericWideInt &left, const NumericWideInt &right)
{
	return !(right < left);
}

static inline bool
operator>(const NumericWideInt &left, const NumericWideInt &right)
{
	return right < left;
}

static inline bool
operator>=(const NumericWideInt &left, const NumericWideInt &right)
{
	return !(left < right);
}

static inline NumericWideInt
operator-(const NumericWideInt &value)
{
	uint64_t lo = ~value.lo + 1;
	uint64_t hi = ~((uint64_t) value.hi) + (lo == 0 ? 1 : 0);

	return MakeWideIntBits(lo, hi);
}

static inline NumericWideInt
operator+(const NumericWideInt &left, const NumericWideInt &right)
{
	uint64_t lo = left.lo + right.lo;
	uint64_t carry = (lo < left.lo) ? 1 : 0;
	uint64_t hi = (uint64_t) left.hi + (uint64_t) right.hi + carry;

	return MakeWideIntBits(lo, hi);
}

static inline NumericWideInt
operator-(const NumericWideInt &left, const NumericWideInt &right)
{
	uint64_t lo = left.lo - right.lo;
	uint64_t borrow = (left.lo < right.lo) ? 1 : 0;
	uint64_t hi = (uint64_t) left.hi - (uint64_t) right.hi - borrow;

	return MakeWideIntBits(lo, hi);
}

static inline NumericWideInt
operator*(const NumericWideInt &left, const NumericWideInt &right)
{
	uint64_t prod_hi = 0;
	uint64_t prod_lo = 0;
	uint64_t cross1_hi = 0;
	uint64_t cross1_lo = 0;
	uint64_t cross2_hi = 0;
	uint64_t cross2_lo = 0;
	uint64_t hi;

	Mul64Wide(left.lo, right.lo, &prod_hi, &prod_lo);
	Mul64Wide(left.lo, (uint64_t) right.hi, &cross1_hi, &cross1_lo);
	Mul64Wide((uint64_t) left.hi, right.lo, &cross2_hi, &cross2_lo);
	hi = prod_hi + cross1_lo + cross2_lo;
	return MakeWideIntBits(prod_lo, hi);
}

static inline NumericWideInt &
operator+=(NumericWideInt &left, const NumericWideInt &right)
{
	left = left + right;
	return left;
}

static inline NumericWideInt &
operator-=(NumericWideInt &left, const NumericWideInt &right)
{
	left = left - right;
	return left;
}

static inline NumericWideInt &
operator*=(NumericWideInt &left, const NumericWideInt &right)
{
	left = left * right;
	return left;
}
#endif

static inline NumericWideInt
WideIntFromInt64(int64_t value)
{
	return NumericWideInt(value);
}

static inline bool
WideIntFitsInt64(NumericWideInt value)
{
	return value >= WideIntFromInt64(PG_INT64_MIN) &&
		   value <= WideIntFromInt64(PG_INT64_MAX);
}

static inline int64_t
WideIntToInt64Checked(NumericWideInt value, const char *what)
{
	if (!WideIntFitsInt64(value))
		elog(ERROR, "pg_yaap %s exceeds int64 range", what);
#if defined(__SIZEOF_INT128__)
	return (int64_t) value;
#else
	return (int64_t) value.lo;
#endif
}

static inline NumericWideInt
WideIntMul(NumericWideInt left, NumericWideInt right)
{
	return left * right;
}

static inline NumericWideInt
RescaleWideIntUp(NumericWideInt value, int delta_scale)
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

	if (delta_scale <= 0)
		return value;
	if (delta_scale >= (int) lengthof(kPowers))
		elog(ERROR, "pg_yaap rescale delta %d exceeds supported range", delta_scale);
	return WideIntMul(value, WideIntFromInt64(kPowers[delta_scale]));
}

static inline int
GetNumericScaleFromTypmod(int32 typmod)
{
	if (typmod < (int32) VARHDRSZ)
		return DEFAULT_NUMERIC_SCALE;
	return ((((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024);
}

static inline int
GetNumericPrecisionFromTypmod(int32 typmod)
{
	if (typmod < (int32) VARHDRSZ)
		return -1;
	return ((typmod - VARHDRSZ) >> 16) & 0xffff;
}

static inline int
CountDecimalDigitsInt64(int64_t value)
{
	uint64_t magnitude;
	int digits = 1;

	if (value == PG_INT64_MIN)
		return 19;
	magnitude = (uint64_t) (value < 0 ? -value : value);
	while (magnitude >= 10)
	{
		magnitude /= 10;
		digits++;
	}
	return digits;
}

static inline VecNumericWidth
WidthForNumericPrecision(int precision)
{
	if (precision <= 0)
		return VecNumericWidth::None;
	return precision <= 18 ? VecNumericWidth::Int64 : VecNumericWidth::Wide128;
}

struct VolVecNumericShort
{
	uint16		n_header;
	int16		n_data[FLEXIBLE_ARRAY_MEMBER];
};

struct VolVecNumericLong
{
	uint16		n_sign_dscale;
	int16		n_weight;
	int16		n_data[FLEXIBLE_ARRAY_MEMBER];
};

static inline bool
VolVecNumericHeaderIsShort(const void *datum_ptr)
{
	const VolVecNumericShort *num =
		reinterpret_cast<const VolVecNumericShort *>(VARDATA_ANY((const struct varlena *) datum_ptr));
	return (num->n_header & 0x8000) != 0;
}

static inline bool
VolVecNumericIsSpecial(const void *datum_ptr)
{
	const VolVecNumericShort *num =
		reinterpret_cast<const VolVecNumericShort *>(VARDATA_ANY((const struct varlena *) datum_ptr));
	return (num->n_header & 0xC000) == 0xC000;
}

static inline int
VolVecNumericDscale(const void *datum_ptr)
{
	if (VolVecNumericHeaderIsShort(datum_ptr))
	{
		const VolVecNumericShort *num =
			reinterpret_cast<const VolVecNumericShort *>(VARDATA_ANY((const struct varlena *) datum_ptr));
		return (num->n_header & 0x1F80) >> 7;
	}

	const VolVecNumericLong *num =
		reinterpret_cast<const VolVecNumericLong *>(VARDATA_ANY((const struct varlena *) datum_ptr));
	return (num->n_sign_dscale & 0x3FFF);
}

static inline int
VolVecNumericWeight(const void *datum_ptr)
{
	if (VolVecNumericHeaderIsShort(datum_ptr))
	{
		const VolVecNumericShort *num =
			reinterpret_cast<const VolVecNumericShort *>(VARDATA_ANY((const struct varlena *) datum_ptr));
		int weight = num->n_header & 0x003F;
		if ((num->n_header & 0x0040) != 0)
			weight |= ~0x003F;
		return weight;
	}

	const VolVecNumericLong *num =
		reinterpret_cast<const VolVecNumericLong *>(VARDATA_ANY((const struct varlena *) datum_ptr));
	return num->n_weight;
}

static inline bool
VolVecNumericNegative(const void *datum_ptr)
{
	if (VolVecNumericHeaderIsShort(datum_ptr))
	{
		const VolVecNumericShort *num =
			reinterpret_cast<const VolVecNumericShort *>(VARDATA_ANY((const struct varlena *) datum_ptr));
		return (num->n_header & 0x2000) != 0;
	}

	const VolVecNumericLong *num =
		reinterpret_cast<const VolVecNumericLong *>(VARDATA_ANY((const struct varlena *) datum_ptr));
	return (num->n_sign_dscale & 0xC000) == 0x4000;
}

static inline int
VolVecNumericHeaderSize(const void *datum_ptr)
{
	return sizeof(uint16) + (VolVecNumericHeaderIsShort(datum_ptr) ? 0 : sizeof(int16));
}

static inline const int16 *
VolVecNumericDigits(const void *datum_ptr)
{
	if (VolVecNumericHeaderIsShort(datum_ptr))
	{
		const VolVecNumericShort *num =
			reinterpret_cast<const VolVecNumericShort *>(VARDATA_ANY((const struct varlena *) datum_ptr));
		return num->n_data;
	}

	const VolVecNumericLong *num =
		reinterpret_cast<const VolVecNumericLong *>(VARDATA_ANY((const struct varlena *) datum_ptr));
	return num->n_data;
}

static inline int
VolVecNumericNDigits(const void *datum_ptr)
{
	return (VARSIZE_ANY_EXHDR((const struct varlena *) datum_ptr) - VolVecNumericHeaderSize(datum_ptr)) / (int) sizeof(int16);
}

static inline bool
TryFastNumericToScaledInt64(Datum value, int target_scale, int64_t *out)
{
	const void *datum_ptr = DatumGetPointer(value);
	const int16 *digits;
	NumericWideInt accum = 0;
	int ndigits;
	int weight;
	int dscale;
	int frac_index;
	int i;

	if (out == nullptr || datum_ptr == nullptr)
		return false;
	if (VolVecNumericIsSpecial(datum_ptr))
		return false;
	if (target_scale < 0 || target_scale > VOLVEC_DEC_DIGITS)
		return false;

	dscale = VolVecNumericDscale(datum_ptr);
	if (dscale < 0 || dscale > VOLVEC_DEC_DIGITS || dscale > target_scale)
		return false;

	digits = VolVecNumericDigits(datum_ptr);
	ndigits = VolVecNumericNDigits(datum_ptr);
	weight = VolVecNumericWeight(datum_ptr);

	for (i = 0; i <= weight; i++)
	{
		accum *= VOLVEC_NBASE;
		if (i >= 0 && i < ndigits)
			accum += digits[i];
	}

	for (i = 0; i < target_scale; i++)
		accum *= 10;

	frac_index = weight + 1;
	if (target_scale > 0)
	{
		int16 frac_digit = (frac_index >= 0 && frac_index < ndigits) ? digits[frac_index] : 0;
		int divisor = 1;

		for (i = 0; i < VOLVEC_DEC_DIGITS - target_scale; i++)
			divisor *= 10;
		if (frac_digit % divisor != 0)
			return false;
		accum += frac_digit / divisor;
	}

	for (i = Max(frac_index + 1, 0); i < ndigits; i++)
	{
		if (digits[i] != 0)
			return false;
	}

	if (VolVecNumericNegative(datum_ptr))
		accum = -accum;

	if (!WideIntFitsInt64(accum))
		return false;

	*out = (int64_t) accum;
	return true;
}

static inline bool
TryFastNumericToScaledWideInt(Datum value, int target_scale, NumericWideInt *out)
{
	const void *datum_ptr = DatumGetPointer(value);
	const int16 *digits;
	NumericWideInt accum = 0;
	int ndigits;
	int weight;
	int remaining_scale;
	int processed_groups;
	int i;
	NumericWideInt scale_factor = 1;

	if (out == nullptr || datum_ptr == nullptr)
		return false;
	if (VolVecNumericIsSpecial(datum_ptr))
		return false;
	if (target_scale < 0 || target_scale > 18)
		return false;

	digits = VolVecNumericDigits(datum_ptr);
	ndigits = VolVecNumericNDigits(datum_ptr);
	weight = VolVecNumericWeight(datum_ptr);

	for (i = 0; i < target_scale; i++)
		scale_factor *= WideIntFromInt64(10);

	for (i = 0; i <= weight; i++)
	{
		int16 digit = 0;

		if (i >= 0 && i < ndigits)
			digit = digits[i];
		accum *= WideIntFromInt64(VOLVEC_NBASE);
		accum += WideIntFromInt64(digit);
	}

	accum *= scale_factor;
	remaining_scale = target_scale;
	processed_groups = (target_scale + VOLVEC_DEC_DIGITS - 1) / VOLVEC_DEC_DIGITS;

	for (i = 0; i < processed_groups; i++)
	{
		int group_exp = -1 - i;
		int digit_index = weight - group_exp;
		int16 digit = (digit_index >= 0 && digit_index < ndigits) ? digits[digit_index] : 0;
		int take = Min(remaining_scale, VOLVEC_DEC_DIGITS);
		int drop = VOLVEC_DEC_DIGITS - take;
		int64_t divisor = 1;
		int64_t contrib;

		for (int j = 0; j < drop; j++)
			divisor *= 10;
		if (digit % divisor != 0)
			return false;
		contrib = digit / divisor;
		for (int j = 0; j < remaining_scale - take; j++)
			contrib *= 10;
		accum += WideIntFromInt64(contrib);
		remaining_scale -= take;
	}

	for (i = Max(weight + 1 + processed_groups, 0); i < ndigits; i++)
	{
		if (digits[i] != 0)
			return false;
	}

	if (VolVecNumericNegative(datum_ptr))
		accum = -accum;

	*out = accum;
	return true;
}

} // namespace pg_yaap
