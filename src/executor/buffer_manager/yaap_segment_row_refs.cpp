extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include "buffer_manager/yaap_segment_row_refs.hpp"

#include <algorithm>
#include <cstring>

namespace pg_yaap {
namespace buffer {

namespace {

static constexpr uint32_t kRowsPerPage = 4096;

static YaapSegmentRowRef *
ResolvePage(dsa_area *dsa, const YaapSegmentRowRefTable *table, uint32_t page)
{
	if (page >= table->page_count || !DsaPointerIsValid(table->refs_dp))
		return nullptr;
	auto *directory = static_cast<const dsa_pointer *>(dsa_get_address(dsa, table->refs_dp));
	if (directory == nullptr || !DsaPointerIsValid(directory[page]))
		return nullptr;
	return static_cast<YaapSegmentRowRef *>(dsa_get_address(dsa, directory[page]));
}

}  /* namespace */

dsa_pointer
YaapSegmentRowRefTableCreate(dsa_area *dsa, uint32_t capacity)
{
	if (dsa == nullptr || capacity == 0)
		ereport(ERROR, (errmsg("pg_yaap: invalid segment row reference capacity")));
	/* Estimates often exceed the actual row count by orders of magnitude.
	 * Reserve one small directory page up front and grow in fixed chunks. */
	(void) capacity;
	volatile dsa_pointer table_dp = dsa_allocate0(dsa, sizeof(YaapSegmentRowRefTable));
	volatile dsa_pointer refs_dp = InvalidDsaPointer;
	PG_TRY();
	{
			refs_dp = dsa_allocate0(dsa, sizeof(dsa_pointer));
		auto *table = static_cast<YaapSegmentRowRefTable *>(dsa_get_address(dsa, table_dp));
			table->capacity = kRowsPerPage;
		pg_atomic_init_u32(&table->count, 0);
		table->refs_dp = refs_dp;
			table->page_count = 1;
			table->_pad = 0;
	}
	PG_CATCH();
	{
		if (DsaPointerIsValid(refs_dp))
			dsa_free(dsa, refs_dp);
		dsa_free(dsa, table_dp);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return table_dp;
}

void
YaapSegmentRowRefTableDestroy(dsa_area *dsa, dsa_pointer table_dp)
{
	if (dsa == nullptr || !DsaPointerIsValid(table_dp))
		return;
	auto *table = static_cast<YaapSegmentRowRefTable *>(dsa_get_address(dsa, table_dp));
	if (table == nullptr)
		return;
	if (DsaPointerIsValid(table->refs_dp))
	{
		auto *directory = static_cast<dsa_pointer *>(dsa_get_address(dsa, table->refs_dp));
		for (uint32_t i = 0; i < table->page_count; ++i)
			if (DsaPointerIsValid(directory[i]))
				dsa_free(dsa, directory[i]);
		dsa_free(dsa, table->refs_dp);
	}
	dsa_free(dsa, table_dp);
}

YaapSegmentRowRefId
YaapSegmentRowRefAppend(dsa_area *dsa, YaapSegmentRowRefTable *table,
							const YaapSegmentRowRef &ref)
{
	if (dsa == nullptr || table == nullptr || !ref.segment.IsValid() ||
		ref.row_index == UINT32_MAX)
		return YAAP_INVALID_SEGMENT_ROW_REF;
	const uint32_t id = pg_atomic_read_u32(&table->count);
	if (id >= table->capacity)
	{
		if (id == UINT32_MAX)
			return YAAP_INVALID_SEGMENT_ROW_REF;
		const uint32_t needed_pages = id / kRowsPerPage + 1;
		if (needed_pages > table->page_count)
		{
			uint32_t new_page_count = std::max(needed_pages,
				table->page_count > UINT32_MAX / 2 ? UINT32_MAX : table->page_count * 2);
			dsa_pointer new_directory_dp = dsa_allocate0(dsa,
				static_cast<size_t>(new_page_count) * sizeof(dsa_pointer));
			auto *new_directory = static_cast<dsa_pointer *>(dsa_get_address(dsa, new_directory_dp));
			auto *old_directory = static_cast<const dsa_pointer *>(dsa_get_address(dsa, table->refs_dp));
			if (new_directory == nullptr || old_directory == nullptr)
				ereport(ERROR, (errmsg("pg_yaap: segment row reference directory growth failed")));
			std::memcpy(new_directory, old_directory,
				static_cast<size_t>(table->page_count) * sizeof(dsa_pointer));
			dsa_free(dsa, table->refs_dp);
			table->refs_dp = new_directory_dp;
			table->page_count = new_page_count;
			table->capacity = new_page_count > UINT32_MAX / kRowsPerPage
				? UINT32_MAX : new_page_count * kRowsPerPage;
		}
	}
	auto *directory = static_cast<dsa_pointer *>(dsa_get_address(dsa, table->refs_dp));
	const uint32_t page = id / kRowsPerPage;
	if (page >= table->page_count)
		ereport(ERROR, (errmsg("pg_yaap: segment row reference page index overflow")));
	if (!DsaPointerIsValid(directory[page]))
		directory[page] = dsa_allocate0(dsa,
			static_cast<size_t>(kRowsPerPage) * sizeof(YaapSegmentRowRef));
	auto *refs = ResolvePage(dsa, table, id / kRowsPerPage);
	if (refs == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: segment row reference page is missing")));
	refs[id % kRowsPerPage] = ref;
	pg_atomic_write_u32(&table->count, id + 1);
	return id;
}

const YaapSegmentRowRef *
YaapSegmentRowRefGet(dsa_area *dsa, const YaapSegmentRowRefTable *table,
						 YaapSegmentRowRefId ref_id)
{
	if (dsa == nullptr || table == nullptr || ref_id == YAAP_INVALID_SEGMENT_ROW_REF ||
		ref_id >= pg_atomic_read_u32(const_cast<pg_atomic_uint32 *>(&table->count)) ||
		ref_id >= table->capacity)
		return nullptr;
	const auto *page = ResolvePage(dsa, table, ref_id / kRowsPerPage);
	return page != nullptr ? &page[ref_id % kRowsPerPage] : nullptr;
}

}  /* namespace buffer */
}  /* namespace pg_yaap */
