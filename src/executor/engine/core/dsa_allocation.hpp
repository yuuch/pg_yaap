#pragma once

extern "C" {
#include "postgres.h"
#include "utils/dsa.h"
#include "utils/memutils.h"
}

namespace pg_yaap {

/* PostgreSQL's dsa.c::make_new_segment() odd-sized segment branch allocates
 * pagemap space for `usable_pages` entries instead of `metadata_pages + usable_pages`
 * entries. Whenever `metadata_pages * sizeof(dsa_pointer)` exceeds the 4 KiB
 * page-rounding slack of `metadata_bytes`, the tail of pagemap[] overlaps the
 * first usable data page, corrupting pagemap entries and crashing dsa_free().
 *
 * Keeping single DSA allocations <= 512 MiB and rounding unsafe page counts up
 * to a multiple of (FPM_PAGE_SIZE / sizeof(dsa_pointer)) (2 MiB on 64-bit)
 * guarantees `H + r + metadata_pages * sizeof(dsa_pointer) <= FPM_PAGE_SIZE`
 * for all segment header + FreePageManager sizes H <= 1280 bytes. */
inline constexpr size_t kYaapPgFpmPageSize = 4096u;
inline constexpr size_t kYaapPgDsaHeaderReserve = 1280u;
inline constexpr size_t kYaapMaxSafeDsaSingleAllocBytes = 512ull * 1024ull * 1024ull;

constexpr size_t YaapDsaSafeAllocBytes(size_t bytes)
{
	if (bytes <= 8192u || bytes > kYaapMaxSafeDsaSingleAllocBytes)
		return bytes;
	const size_t ptr_size = sizeof(dsa_pointer);
	const size_t pages_per_pagemap_page = kYaapPgFpmPageSize / ptr_size;
	size_t npages = (bytes + kYaapPgFpmPageSize - 1u) / kYaapPgFpmPageSize;
	const size_t rem = (npages * ptr_size) % kYaapPgFpmPageSize;
	const size_t max_metadata_pages =
		(kYaapPgDsaHeaderReserve + npages * ptr_size + kYaapPgFpmPageSize - 1u) /
		kYaapPgFpmPageSize;
	if (kYaapPgDsaHeaderReserve + rem + max_metadata_pages * ptr_size <= kYaapPgFpmPageSize)
		return bytes;
	npages = ((npages + pages_per_pagemap_page - 1u) / pages_per_pagemap_page) *
		pages_per_pagemap_page;
	return npages * kYaapPgFpmPageSize;
}

/* PG's ordinary DSA API rejects requests above MaxAllocSize before checking
 * area capacity. Large executor arrays are valid huge allocations; enabling
 * that flag does not bypass the DSA area's total size limit or layout bounds. */
inline dsa_pointer YaapDsaAllocate(dsa_area *area, size_t bytes, int flags = 0)
{
	const size_t alloc_bytes = YaapDsaSafeAllocBytes(bytes);
	if (!AllocSizeIsValid(alloc_bytes))
		flags |= DSA_ALLOC_HUGE;
	return dsa_allocate_extended(area, alloc_bytes, flags);
}

inline dsa_pointer YaapDsaAllocateZero(dsa_area *area, size_t bytes)
{
	return YaapDsaAllocate(area, bytes, DSA_ALLOC_ZERO);
}

} /* namespace pg_yaap */
