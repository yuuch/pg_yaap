#pragma once

extern "C" {
#include "postgres.h"
#include "utils/dsa.h"
#include "utils/memutils.h"
}

namespace pg_yaap {

/* PG's ordinary DSA API rejects requests above MaxAllocSize before checking
 * area capacity. Large executor arrays are valid huge allocations; enabling
 * that flag does not bypass the DSA area's total size limit or layout bounds. */
inline dsa_pointer YaapDsaAllocate(dsa_area *area, size_t bytes, int flags = 0)
{
	if (!AllocSizeIsValid(bytes))
		flags |= DSA_ALLOC_HUGE;
	return dsa_allocate_extended(area, bytes, flags);
}

inline dsa_pointer YaapDsaAllocateZero(dsa_area *area, size_t bytes)
{
	return YaapDsaAllocate(area, bytes, DSA_ALLOC_ZERO);
}

} /* namespace pg_yaap */
