#pragma once

#include "buffer_manager/yaap_shared_segment_arena.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"

namespace pg_yaap {
namespace buffer {

using namespace pipeline;

struct YaapTdcSpillImageHeader
{
	uint32_t magic;
	uint16_t version;
	uint16_t reserved;
	uint32_t row_width;
	uint32_t row_capacity;
	uint32_t heap_capacity;
	uint32_t row_count;
	uint32_t heap_used;
	uint64_t checksum;
};

bool YaapSegmentedTdcSpill(const TupleDataCollection *tdc,
							 YaapSpillStore *store, YaapSpillExtent *extent);

TupleDataCollection *YaapSegmentedTdcReload(const YaapSpillExtent &extent,
									 YaapSpillStore *store, MemoryContext context,
									 dsa_pointer layout_dp);

struct YaapSegmentedRowHandle
{
	YaapSegmentHandle segment;
	uint32_t row_index = UINT32_MAX;

	bool IsValid() const { return segment.IsValid() && row_index != UINT32_MAX; }
};

/* Process-local writer state; every referenced payload is in DSA. */
struct YaapSegmentedTdc
{
	dsa_pointer arena_dp = InvalidDsaPointer;
	dsa_pointer layout_dp = InvalidDsaPointer;
	uint32_t owner_worker = UINT32_MAX;
	uint64_t owner_tag = 0;
	uint32_t row_width = 0;
	uint32_t row_capacity = 0;
	uint32_t heap_capacity = 0;
	YaapSegmentHandle first;
	YaapSegmentHandle active;
	uint32_t segment_count = 0;
	uint64_t row_count = 0;
};

struct YaapSegmentedTdcReader
{
	dsa_pointer arena_dp = InvalidDsaPointer;
	dsa_pointer current_desc_dp = InvalidDsaPointer;
	YaapSegmentHandle current_segment;
	uint32_t row_index = 0;
	YaapSharedSegmentReadLease lease;
};

struct YaapSegmentedTdcSharedDesc
{
	dsa_pointer arena_dp;
	dsa_pointer first_desc_dp;
	dsa_pointer layout_dp;
	uint32_t row_width;
	uint32_t row_capacity;
	uint32_t heap_capacity;
	uint64_t row_count;
};

void YaapSegmentedTdcInit(YaapSegmentedTdc *tdc, dsa_pointer arena_dp,
							 dsa_pointer layout_dp, uint32_t owner_worker,
								 uint32_t row_width, uint32_t row_capacity,
								 uint32_t heap_capacity, uint64_t owner_tag = 0,
								 const char *purpose = "unspecified");

bool YaapSegmentedTdcAppend(dsa_area *dsa, YaapBufferSharedState *state,
								 YaapSegmentedTdc *tdc, YaapSegmentedRowHandle *handle,
								 TupleDataCollection **row_store, uint8_t **row);

/* Append with an operator-computed heap requirement.  A sealed successor is
 * allocated when either the row area or the string heap cannot fit the row. */
bool YaapSegmentedTdcAppendWithHeap(dsa_area *dsa, YaapBufferSharedState *state,
									YaapSegmentedTdc *tdc, uint32_t required_heap_bytes,
									YaapSegmentedRowHandle *handle,
									TupleDataCollection **row_store, uint8_t **row);

TupleDataCollection *YaapSegmentedTdcResolveStore(dsa_area *dsa,
											 const YaapSegmentedRowHandle &handle,
											 dsa_pointer arena_dp);

const uint8_t *YaapSegmentedTdcResolveRow(dsa_area *dsa,
										  const YaapSegmentedRowHandle &handle,
										  dsa_pointer arena_dp);

void YaapSegmentedTdcSealPublish(dsa_area *dsa, YaapSegmentedTdc *tdc);

void YaapSegmentedTdcDestroy(dsa_area *dsa, YaapBufferSharedState *state,
							 YaapSegmentedTdc *tdc);

dsa_pointer YaapSegmentedTdcPublishDesc(dsa_area *dsa,
									const YaapSegmentedTdc &tdc);

const YaapSegmentedTdcSharedDesc *YaapSegmentedTdcResolveDesc(
	dsa_area *dsa, dsa_pointer desc_dp);

void YaapSegmentedTdcReaderInitShared(YaapSegmentedTdcReader *reader,
									 dsa_area *dsa,
									 const YaapSegmentedTdcSharedDesc *desc);

void YaapSegmentedTdcReaderInit(YaapSegmentedTdcReader *reader,
								dsa_area *dsa, dsa_pointer arena_dp,
								const YaapSegmentedTdc &tdc);

bool YaapSegmentedTdcReaderNext(dsa_area *dsa, YaapSegmentedTdcReader *reader,
								YaapSegmentedRowHandle *handle,
								TupleDataCollection **store, const uint8_t **row);

void YaapSegmentedTdcReaderClose(dsa_area *dsa, YaapSegmentedTdcReader *reader);

}  /* namespace buffer */
}  /* namespace pg_yaap */
