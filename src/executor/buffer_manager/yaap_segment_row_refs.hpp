#pragma once

#include "buffer_manager/yaap_shared_segment_arena.hpp"

namespace pg_yaap {
namespace buffer {

using YaapSegmentRowRefId = uint32_t;
static constexpr YaapSegmentRowRefId YAAP_INVALID_SEGMENT_ROW_REF = UINT32_MAX;

struct YaapSegmentRowRef
{
	YaapSegmentHandle segment;
	uint32_t row_index = UINT32_MAX;
};

struct YaapSegmentRowRefTable
{
	uint32_t capacity;
	pg_atomic_uint32 count;
	dsa_pointer refs_dp; /* dsa_pointer page directory */
	uint32_t page_count;
	uint32_t _pad;
};

dsa_pointer YaapSegmentRowRefTableCreate(dsa_area *dsa, uint32_t capacity);
void YaapSegmentRowRefTableDestroy(dsa_area *dsa, dsa_pointer table_dp);

YaapSegmentRowRefId YaapSegmentRowRefAppend(dsa_area *dsa,
										YaapSegmentRowRefTable *table,
										const YaapSegmentRowRef &ref);

const YaapSegmentRowRef *YaapSegmentRowRefGet(dsa_area *dsa,
										  const YaapSegmentRowRefTable *table,
										  YaapSegmentRowRefId ref_id);

}  /* namespace buffer */
}  /* namespace pg_yaap */
