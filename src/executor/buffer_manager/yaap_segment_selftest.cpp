extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/dsa.h"
}

#include <cstring>

#include "buffer_manager/yaap_buffer_shared.hpp"
#include "buffer_manager/yaap_segmented_tdc.hpp"

extern "C" {
PGDLLEXPORT Datum pg_yaap_segment_selftest(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_yaap_segment_selftest);
}

Datum
pg_yaap_segment_selftest(PG_FUNCTION_ARGS)
{
	(void) fcinfo;
	MemoryContext context = CurrentMemoryContext;
	auto *buffer_state = static_cast<pg_yaap::buffer::YaapBufferSharedState *>(
		MemoryContextAllocZero(context, sizeof(pg_yaap::buffer::YaapBufferSharedState)));
	pg_yaap::buffer::YaapBufferSharedStateInit(buffer_state,
		64u * 1024u * 1024u, 48u * 1024u * 1024u);
	dsa_area *dsa = dsa_create(0);
	dsa_set_size_limit(dsa, 64u * 1024u * 1024u);
	dsa_pointer arena_dp = pg_yaap::buffer::YaapSharedSegmentArenaCreate(
		dsa, buffer_state, 99, 256u * 1024u);
	pg_yaap::buffer::YaapSegmentedTdc tdc;
	pg_yaap::buffer::YaapSegmentedTdcInit(&tdc, arena_dp, InvalidDsaPointer,
		0, 8, 2, 0);
	for (uint32_t i = 0; i < 5; ++i)
	{
		pg_yaap::buffer::YaapSegmentedRowHandle handle;
		pg_yaap::pipeline::TupleDataCollection *store = nullptr;
		uint8_t *row = nullptr;
		if (!pg_yaap::buffer::YaapSegmentedTdcAppend(dsa, buffer_state, &tdc,
			&handle, &store, &row))
			ereport(ERROR, (errmsg("pg_yaap segment self-test append failed")));
		std::memcpy(row, &i, sizeof(i));
	}
	pg_yaap::buffer::YaapSegmentedTdcSealPublish(dsa, &tdc);
	dsa_pointer shared_desc_dp = pg_yaap::buffer::YaapSegmentedTdcPublishDesc(dsa, tdc);
	const auto *shared_desc = pg_yaap::buffer::YaapSegmentedTdcResolveDesc(dsa, shared_desc_dp);
	if (shared_desc == nullptr)
		ereport(ERROR, (errmsg("pg_yaap segment self-test shared descriptor missing")));
	pg_yaap::buffer::YaapSegmentedTdcReader reader;
	pg_yaap::buffer::YaapSegmentedTdcReaderInitShared(&reader, dsa, shared_desc);
	uint32_t seen = 0;
	for (;;)
	{
		pg_yaap::buffer::YaapSegmentedRowHandle handle;
		pg_yaap::pipeline::TupleDataCollection *store = nullptr;
		const uint8_t *row = nullptr;
		if (!pg_yaap::buffer::YaapSegmentedTdcReaderNext(dsa, &reader,
			&handle, &store, &row))
			break;
		if (row == nullptr || store == nullptr)
			ereport(ERROR, (errmsg("pg_yaap segment self-test row resolve failed")));
		++seen;
	}
	if (seen != 5 || tdc.segment_count != 3)
		ereport(ERROR, (errmsg("pg_yaap segment self-test saw %u rows across %u segments",
		 seen, tdc.segment_count)));
	pg_yaap::buffer::YaapSegmentedTdcReaderClose(dsa, &reader);
	auto *first_segment = pg_yaap::buffer::YaapSharedSegmentResolve(dsa, arena_dp, tdc.first);
	auto *first_store = first_segment != nullptr
		? static_cast<pg_yaap::pipeline::TupleDataCollection *>(
			dsa_get_address(dsa, first_segment->payload_dp)) : nullptr;
	if (first_store == nullptr)
		ereport(ERROR, (errmsg("pg_yaap segment self-test first store missing")));
	pg_yaap::buffer::YaapSpillStore spill(context);
	pg_yaap::buffer::YaapSpillExtent extent{};
	if (!pg_yaap::buffer::YaapSegmentedTdcSpill(first_store, &spill, &extent))
		ereport(ERROR, (errmsg("pg_yaap segment self-test spill failed")));
	auto *reloaded = pg_yaap::buffer::YaapSegmentedTdcReload(extent, &spill,
		context, InvalidDsaPointer);
	if (reloaded == nullptr || pg_atomic_read_u32(&reloaded->row_count) !=
		pg_atomic_read_u32(&first_store->row_count))
		ereport(ERROR, (errmsg("pg_yaap segment self-test reload row count mismatch")));
	pg_yaap::buffer::YaapSpillExtent segment_extent{};
	if (!pg_yaap::buffer::YaapSharedSegmentSpill(dsa, buffer_state, tdc.first,
		arena_dp, &spill, &segment_extent))
		ereport(ERROR, (errmsg("pg_yaap segment self-test shared spill failed")));
	if (!pg_yaap::buffer::YaapSharedSegmentReload(dsa, buffer_state, tdc.first,
		arena_dp, &spill, segment_extent))
		ereport(ERROR, (errmsg("pg_yaap segment self-test shared reload failed")));
	auto *reloaded_segment = pg_yaap::buffer::YaapSharedSegmentResolve(dsa,
		arena_dp, tdc.first);
	if (reloaded_segment == nullptr || pg_atomic_read_u32(&reloaded_segment->state) !=
		static_cast<uint32_t>(pg_yaap::buffer::YaapSharedSegmentState::PUBLISHED))
		ereport(ERROR, (errmsg("pg_yaap segment self-test shared reload state mismatch")));
	/* A live reader prevents spill/retirement; reload retains immutable state. */
	pg_yaap::buffer::YaapSharedSegmentReadLease lease;
	if (!pg_yaap::buffer::YaapSharedSegmentAcquireRead(dsa, arena_dp, tdc.first, &lease) ||
		pg_yaap::buffer::YaapSharedSegmentRetire(dsa, buffer_state, arena_dp, tdc.first) ||
		pg_yaap::buffer::YaapSharedSegmentSpill(dsa, buffer_state, tdc.first,
			arena_dp, &spill, &segment_extent))
		ereport(ERROR, (errmsg("pg_yaap segment self-test live lease protection failed")));
	pg_yaap::buffer::YaapSharedSegmentReleaseRead(dsa, arena_dp, &lease);
	const uint64_t before_directory = pg_yaap::buffer::YaapBufferResidentBytes(buffer_state);
	const dsa_pointer directory = pg_yaap::buffer::YaapSegmentPublicationDirectoryCreate(
		dsa, buffer_state, 1, 3, 2);
	pg_yaap::buffer::YaapSegmentPublicationDirectoryDestroy(dsa, buffer_state, directory);
	if (pg_yaap::buffer::YaapBufferResidentBytes(buffer_state) != before_directory)
		ereport(ERROR, (errmsg("pg_yaap segment self-test publication accounting mismatch")));
	const auto old_handle = pg_yaap::buffer::YaapSharedSegmentArenaAllocate(
		dsa, buffer_state, arena_dp, 0, 0);
	auto *reusable = pg_yaap::buffer::YaapSharedSegmentResolve(dsa, arena_dp, old_handle);
	pg_yaap::buffer::YaapSharedSegmentSeal(reusable);
	pg_yaap::buffer::YaapSharedSegmentPublish(reusable);
	if (!pg_yaap::buffer::YaapSharedSegmentRetire(dsa, buffer_state, arena_dp, old_handle))
		ereport(ERROR, (errmsg("pg_yaap segment self-test retirement failed")));
	const auto new_handle = pg_yaap::buffer::YaapSharedSegmentArenaAllocate(
		dsa, buffer_state, arena_dp, 0, 0);
	if (new_handle.segment_id != old_handle.segment_id ||
		new_handle.generation == old_handle.generation ||
		pg_yaap::buffer::YaapSharedSegmentAcquireRead(dsa, arena_dp, old_handle, &lease))
		ereport(ERROR, (errmsg("pg_yaap segment self-test stale generation accepted")));
	reusable = pg_yaap::buffer::YaapSharedSegmentResolve(dsa, arena_dp, new_handle);
	pg_yaap::buffer::YaapSharedSegmentSeal(reusable);
	pg_yaap::buffer::YaapSharedSegmentPublish(reusable);
	if (!pg_yaap::buffer::YaapSharedSegmentRetire(dsa, buffer_state, arena_dp, new_handle))
		ereport(ERROR, (errmsg("pg_yaap segment self-test reused retirement failed")));
	const auto discard_handle = pg_yaap::buffer::YaapSharedSegmentArenaAllocate(
		dsa, buffer_state, arena_dp, 0, 0);
	auto *discard = pg_yaap::buffer::YaapSharedSegmentResolve(dsa, arena_dp, discard_handle);
	pg_yaap::buffer::YaapSharedSegmentSeal(discard);
	pg_yaap::buffer::YaapSharedSegmentPublish(discard);
	const uint64_t before_discard = pg_yaap::buffer::YaapBufferResidentBytes(buffer_state);
	const uint64_t discard_bytes = discard->capacity;
	if (!pg_yaap::buffer::YaapSharedSegmentRetire(dsa, buffer_state, arena_dp, discard_handle, true) ||
		DsaPointerIsValid(discard->payload_dp) ||
		pg_yaap::buffer::YaapBufferResidentBytes(buffer_state) != before_discard - discard_bytes)
		ereport(ERROR, (errmsg("pg_yaap segment self-test discard retirement retained payload budget")));
	pg_yaap::buffer::YaapSegmentedTdc string_tdc;
	pg_yaap::buffer::YaapSegmentedTdcInit(&string_tdc, arena_dp, InvalidDsaPointer,
		0, sizeof(pg_yaap::VecStringRef), 2, 32);
	pg_yaap::buffer::YaapSegmentedRowHandle string_handle;
	pg_yaap::pipeline::TupleDataCollection *string_store = nullptr;
	uint8_t *string_row = nullptr;
	if (!pg_yaap::buffer::YaapSegmentedTdcAppendWithHeap(dsa, buffer_state, &string_tdc,
		20, &string_handle, &string_store, &string_row))
		ereport(ERROR, (errmsg("pg_yaap segment self-test string append failed")));
	const char string_value[] = "segment-string";
	pg_yaap::VecStringRef string_ref{};
	if (!pg_yaap::pipeline::TupleDataCollectionStoreStringBytes(string_store,
		string_value, sizeof(string_value) - 1, &string_ref))
			ereport(ERROR, (errmsg("pg_yaap segment self-test string heap append failed")));
	std::memcpy(string_row, &string_ref, sizeof(string_ref));
	/* The second append must roll to a successor because the first string
	 * consumed part of the fixed heap budget. */
	if (!pg_yaap::buffer::YaapSegmentedTdcAppendWithHeap(dsa, buffer_state, &string_tdc,
		20, &string_handle, &string_store, &string_row))
		ereport(ERROR, (errmsg("pg_yaap segment self-test heap rollover failed")));
	if (!pg_yaap::pipeline::TupleDataCollectionStoreStringBytes(string_store,
		string_value, sizeof(string_value) - 1, &string_ref))
		ereport(ERROR, (errmsg("pg_yaap segment self-test second string heap append failed")));
	std::memcpy(string_row, &string_ref, sizeof(string_ref));
	if (string_tdc.segment_count != 2)
		ereport(ERROR, (errmsg("pg_yaap segment self-test expected heap rollover")));
	pg_yaap::buffer::YaapSegmentedTdcSealPublish(dsa, &string_tdc);
	auto *string_segment = pg_yaap::buffer::YaapSharedSegmentResolve(dsa, arena_dp,
		string_tdc.first);
	auto *string_tdc_store = string_segment != nullptr
		? static_cast<pg_yaap::pipeline::TupleDataCollection *>(
			dsa_get_address(dsa, string_segment->payload_dp)) : nullptr;
	pg_yaap::buffer::YaapSpillExtent string_extent{};
	if (string_tdc_store == nullptr || !pg_yaap::buffer::YaapSegmentedTdcSpill(
		string_tdc_store, &spill, &string_extent))
		ereport(ERROR, (errmsg("pg_yaap segment self-test string spill failed")));
	auto *string_reload = pg_yaap::buffer::YaapSegmentedTdcReload(string_extent,
		&spill, context, InvalidDsaPointer);
	if (string_reload == nullptr || pg_atomic_read_u32(&string_reload->heap_used) == 0)
		ereport(ERROR, (errmsg("pg_yaap segment self-test string reload heap missing")));
	const auto *reload_ref = reinterpret_cast<const pg_yaap::VecStringRef *>(string_reload->rows);
	const char *reload_string = pg_yaap::VecStringRefDataPtr(*reload_ref,
		reinterpret_cast<const char *>(pg_yaap::pipeline::TupleDataCollectionHeapConst(string_reload)));
	if (reload_string == nullptr || std::memcmp(reload_string, string_value,
		sizeof(string_value) - 1) != 0)
		ereport(ERROR, (errmsg("pg_yaap segment self-test string reload mismatch")));
	/* Raw shared-segment images must preserve the unused row-capacity gap
	 * before the heap, unlike the compact local TDC spill format above. */
	if (!pg_yaap::buffer::YaapSharedSegmentSpill(dsa, buffer_state, string_tdc.first,
		arena_dp, &spill, &string_extent) ||
		!pg_yaap::buffer::YaapSharedSegmentReload(dsa, buffer_state, string_tdc.first,
			arena_dp, &spill, string_extent))
		ereport(ERROR, (errmsg("pg_yaap segment self-test shared string roundtrip failed")));
	string_segment = pg_yaap::buffer::YaapSharedSegmentResolve(dsa, arena_dp, string_tdc.first);
	string_reload = static_cast<pg_yaap::pipeline::TupleDataCollection *>(
		dsa_get_address(dsa, string_segment->payload_dp));
	reload_ref = reinterpret_cast<const pg_yaap::VecStringRef *>(string_reload->rows);
	reload_string = pg_yaap::VecStringRefDataPtr(*reload_ref,
		reinterpret_cast<const char *>(pg_yaap::pipeline::TupleDataCollectionHeapConst(string_reload)));
	if (reload_string == nullptr || std::memcmp(reload_string, string_value,
		sizeof(string_value) - 1) != 0)
		ereport(ERROR, (errmsg("pg_yaap segment self-test shared string heap was truncated")));
	pg_yaap::buffer::YaapSegmentedTdcDestroy(dsa, buffer_state, &string_tdc);
	pg_yaap::buffer::YaapSegmentedTdcDestroy(dsa, buffer_state, &tdc);
	dsa_free(dsa, shared_desc_dp);
	pg_yaap::buffer::YaapSharedSegmentArenaDestroy(dsa, buffer_state, arena_dp);
	if (pg_yaap::buffer::YaapBufferResidentBytes(buffer_state) != 0)
		ereport(ERROR, (errmsg("pg_yaap segment self-test leaked resident budget")));
	dsa_detach(dsa);
	PG_RETURN_BOOL(true);
}
