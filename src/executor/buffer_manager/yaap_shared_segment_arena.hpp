#pragma once

extern "C" {
#include "postgres.h"
#include "port/atomics.h"
#include "storage/spin.h"
#include "utils/dsa.h"
}

#include <cstdint>

#include "buffer_manager/yaap_buffer_shared.hpp"
#include "buffer_manager/yaap_spill_store.hpp"

namespace pg_yaap {
namespace buffer {

struct YaapSegmentHandle
{
	uint32_t arena_id = 0;
	uint32_t segment_id = UINT32_MAX;
	uint32_t generation = 0;

	bool IsValid() const { return segment_id != UINT32_MAX && generation != 0; }
};

enum class YaapSharedSegmentState : uint32_t
{
	FREE = 0,
	WRITABLE_LOCAL = 1,
	SEALED_LOCAL = 2,
	PUBLISHED = 3,
	CONSUMING = 4,
	RETIRED = 5,
	SPILLING = 6,
	SPILLED = 7,
	RELOADING = 8,
};

/* Runtime-only descriptor; payload contains only caller-defined bytes. */
struct YaapSharedSegmentDesc
{
	pg_atomic_uint32 state;
	pg_atomic_uint32 readers;
	uint32_t arena_id;
	uint32_t segment_id;
	uint32_t generation;
	uint32_t owner_worker;
	uint64_t owner_tag;
	uint32_t capacity;
	uint32_t used;
	dsa_pointer payload_dp;
	dsa_pointer self_dp;
	dsa_pointer next_dp;
	dsa_pointer chain_next_dp;
	YaapSpillExtent spill_extent;
	uint32_t spill_sequence;
	dsa_pointer free_next_dp;
};

struct YaapSharedSegmentArena
{
	slock_t mutex;
	uint32_t arena_id;
	uint32_t segment_size;
	uint32_t segment_count;
	uint32_t free_segment_count;
	uint32_t next_segment_id;
	uint64_t segment_index_bytes;
	dsa_pointer self_dp;
	dsa_pointer segment_index_root_dp;
	dsa_pointer first_segment_dp;
	dsa_pointer last_segment_dp;
	dsa_pointer eviction_cursor_dp;
	dsa_pointer free_segment_dp;
};

struct YaapSharedSegmentChain
{
	dsa_pointer arena_dp = InvalidDsaPointer;
	uint32_t owner_worker = UINT32_MAX;
	YaapSegmentHandle first;
	YaapSegmentHandle active;
	uint32_t segment_count = 0;
};

void YaapSharedSegmentChainInit(YaapSharedSegmentChain *chain,
								dsa_pointer arena_dp, uint32_t owner_worker);

/* Returns a writable window, growing the chain only when needed. */
void *YaapSharedSegmentChainAcquire(YaapSharedSegmentChain *chain,
								dsa_area *dsa,
								YaapBufferSharedState *buffer_state,
								uint32_t required_bytes,
								uint32_t *available_bytes);

void YaapSharedSegmentChainCommit(YaapSharedSegmentChain *chain,
								  dsa_area *dsa, uint32_t used_bytes);
void YaapSharedSegmentChainSealPublish(YaapSharedSegmentChain *chain,
										dsa_area *dsa);

YaapSharedSegmentDesc *YaapSharedSegmentChainFirst(dsa_area *dsa,
											 const YaapSharedSegmentChain *chain);

struct YaapSharedSegmentReadLease
{
	YaapSegmentHandle handle;
	const void *data = nullptr;
	uint32_t used = 0;

	bool IsValid() const { return handle.IsValid() && data != nullptr; }
};

struct YaapSegmentSpillImageHeader
{
	uint32_t magic;
	uint16_t version;
	uint16_t reserved;
	uint32_t arena_id;
	uint32_t segment_id;
	uint32_t generation;
	uint32_t payload_bytes;
	uint64_t checksum;
};

/* Release a published segment's resident payload.  When a shared spill image
 * already exists, preserve and reuse it without requiring a spill store. */
bool YaapSharedSegmentSpill(dsa_area *dsa,
							 YaapBufferSharedState *buffer_state,
							 const YaapSegmentHandle &handle,
							 dsa_pointer arena_dp,
							 YaapSpillStore *store,
							 YaapSpillExtent *extent);

bool YaapSharedSegmentReload(dsa_area *dsa,
							  YaapBufferSharedState *buffer_state,
							  const YaapSegmentHandle &handle,
							  dsa_pointer arena_dp,
							  YaapSpillStore *store,
							  const YaapSpillExtent &extent,
							  YaapSharedSegmentReadLease *lease = nullptr);

bool YaapSharedSegmentGetSpillExtent(dsa_area *dsa,
									 const YaapSegmentHandle &handle,
									 dsa_pointer arena_dp,
									 YaapSpillExtent *extent);

enum class YaapSegmentPublicationState : uint32_t
{
	EMPTY = 0,
	WRITING = 1,
	PUBLISHED = 2,
	CONSUMED = 3,
};

struct YaapSegmentPublicationSlot
{
	pg_atomic_uint32 state;
	uint32_t owner_worker;
	uint32_t partition_id;
	uint32_t row_count;
	YaapSegmentHandle handle;
};

struct YaapSegmentPublicationDirectory
{
	uint32_t operator_id;
	uint32_t partition_count;
	uint32_t producer_count;
	pg_atomic_uint32 producers_remaining;
	dsa_pointer slots_dp;
};

dsa_pointer YaapSegmentPublicationDirectoryCreate(dsa_area *dsa,
												YaapBufferSharedState *buffer_state,
												uint32_t operator_id,
												uint32_t partition_count,
												uint32_t producer_count);

YaapSegmentPublicationDirectory *YaapSegmentPublicationDirectoryResolve(
		dsa_area *dsa, dsa_pointer directory_dp);

bool YaapSegmentPublicationPublish(dsa_area *dsa,
									   dsa_pointer directory_dp,
									   uint32_t producer_id,
									   uint32_t partition_id,
									   const YaapSegmentHandle &handle,
									   uint32_t row_count);

/* Called exactly once after one producer has published all its partitions. */
bool YaapSegmentPublicationProducerDone(dsa_area *dsa,
										 dsa_pointer directory_dp);

const YaapSegmentPublicationSlot *YaapSegmentPublicationGet(
		dsa_area *dsa, dsa_pointer directory_dp,
		uint32_t producer_id, uint32_t partition_id);

bool YaapSegmentPublicationConsume(dsa_area *dsa,
								   dsa_pointer directory_dp,
								   uint32_t producer_id,
								   uint32_t partition_id);

void YaapSegmentPublicationDirectoryDestroy(dsa_area *dsa,
												YaapBufferSharedState *buffer_state,
												dsa_pointer directory_dp);

dsa_pointer YaapSharedSegmentArenaCreate(dsa_area *dsa,
										 YaapBufferSharedState *buffer_state,
										 uint32_t arena_id,
										 uint32_t segment_size);

YaapSharedSegmentArena *YaapSharedSegmentArenaResolve(dsa_area *dsa,
												   dsa_pointer arena_dp);

YaapSharedSegmentDesc *YaapSharedSegmentResolve(dsa_area *dsa,
												 dsa_pointer arena_dp,
												 const YaapSegmentHandle &handle);

YaapSegmentHandle YaapSharedSegmentArenaAllocate(dsa_area *dsa,
														 YaapBufferSharedState *buffer_state,
														 dsa_pointer arena_dp,
														 uint32_t owner_worker,
														 uint64_t owner_tag = 0);

void *YaapSharedSegmentWritableAddress(dsa_area *dsa,
												 YaapSharedSegmentDesc *segment);

void YaapSharedSegmentSetUsed(YaapSharedSegmentDesc *segment, uint32_t used);
void YaapSharedSegmentSeal(YaapSharedSegmentDesc *segment);
void YaapSharedSegmentPublish(YaapSharedSegmentDesc *segment);

bool YaapSharedSegmentAcquireRead(dsa_area *dsa,
									 dsa_pointer arena_dp,
									 const YaapSegmentHandle &handle,
									 YaapSharedSegmentReadLease *lease);
void YaapSharedSegmentReleaseRead(dsa_area *dsa,
									 dsa_pointer arena_dp,
									 YaapSharedSegmentReadLease *lease);

bool YaapSharedSegmentRetire(dsa_area *dsa,
								 YaapBufferSharedState *buffer_state,
								 dsa_pointer arena_dp,
									 const YaapSegmentHandle &handle,
									 bool discard_payload = false);

void YaapSharedSegmentArenaDestroy(dsa_area *dsa,
										YaapBufferSharedState *buffer_state,
										dsa_pointer arena_dp);

}  /* namespace buffer */
}  /* namespace pg_yaap */
