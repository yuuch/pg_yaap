#include "parallel/pipeline/physical_hash_join_combine.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
}

#include <cstring>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_layout.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

static HashJoinLocalBuildRegistryEntry *
ResolveLocalBuildRegistry(dsa_area *dsa, HashJoinSharedPayload *payload)
{
	if (payload == nullptr || !DsaPointerIsValid(payload->local_build_registry_dp))
		return nullptr;
	return static_cast<HashJoinLocalBuildRegistryEntry *>(
		dsa_get_address(dsa, payload->local_build_registry_dp));
}

static TupleDataCollection *
ResolveTdc(dsa_area *dsa, dsa_pointer tdc_dp)
{
	if (!DsaPointerIsValid(tdc_dp))
		return nullptr;
	return static_cast<TupleDataCollection *>(dsa_get_address(dsa, tdc_dp));
}

static uint32_t
ReadTdcRowCount(const TupleDataCollection *tdc)
{
	return tdc == nullptr ? 0u :
		pg_atomic_read_u32(const_cast<pg_atomic_uint32 *>(&tdc->row_count));
}

static uint32_t
ReadTdcHeapUsed(const TupleDataCollection *tdc)
{
	return tdc == nullptr ? 0u :
		pg_atomic_read_u32(const_cast<pg_atomic_uint32 *>(&tdc->heap_used));
}

static uint16_t
HashJoinSalt(uint64_t hash)
{
	return static_cast<uint16_t>((hash >> 48) & 0xFFFFu);
}

static uint32_t
ComputeHashTableCapacity(uint32_t row_count)
{
	uint32_t hash_capacity = 1;
	while (hash_capacity < row_count * 2u)
		hash_capacity <<= 1;
	return hash_capacity;
}

static bool
TryHashSingleJoinKeyRow(const TupleDataLayout *layout,
						const TupleDataCollection *tdc,
						const uint8_t *row_ptr,
						uint64_t &out_hash)
{
	if (layout == nullptr || tdc == nullptr || row_ptr == nullptr || layout->column_count != 1)
		return false;

	const TdcColumnDesc &col = layout->columns[0];
	switch (col.kind)
	{
		case TdcColumnKind::INT32:
		{
			int32_t v;
			std::memcpy(&v, row_ptr + col.offset, sizeof(v));
			out_hash = HashSingleGroupInt32Value(v);
			return true;
		}
		case TdcColumnKind::INT64:
		{
			int64_t v;
			std::memcpy(&v, row_ptr + col.offset, sizeof(v));
			out_hash = HashSingleGroupInt64Value(v);
			return true;
		}
		default:
			return false;
	}
}

static void
PrecomputeJoinHashes(const TupleDataLayout *layout,
					 const TupleDataCollection *src,
					 HashJoinSharedPayload *payload,
					 dsa_area *dsa,
					 uint32_t global_row_offset,
					 uint32_t row_count)
{
	if (layout == nullptr || src == nullptr || payload == nullptr || row_count == 0)
		return;
	if (!DsaPointerIsValid(payload->hash_buckets_dp) || !DsaPointerIsValid(payload->hash_salts_dp))
		elog(ERROR, "pg_yaap: hash join precomputed hash arrays missing");

	auto *buckets = static_cast<uint32_t *>(dsa_get_address(dsa, payload->hash_buckets_dp));
	auto *salts = static_cast<uint16_t *>(dsa_get_address(dsa, payload->hash_salts_dp));
	if (buckets == nullptr || salts == nullptr)
		elog(ERROR, "pg_yaap: hash join precomputed hash arrays unresolved");

	const uint32_t hash_capacity = payload->hash_table_capacity;
	if (hash_capacity == 0 || (hash_capacity & (hash_capacity - 1u)) != 0)
		elog(ERROR, "pg_yaap: invalid precomputed hash table capacity %u", hash_capacity);

	for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
	{
		const uint8_t *row_ptr = TupleDataCollectionGetRowConst(src, row_idx);
		uint64_t hash = 0;
		if (!TryHashSingleJoinKeyRow(layout, src, row_ptr, hash))
			hash = HashGroupRow(layout, src, row_ptr);
		buckets[global_row_offset + row_idx] = static_cast<uint32_t>(hash) & (hash_capacity - 1u);
		salts[global_row_offset + row_idx] = HashJoinSalt(hash);
	}
}

static void
LinkPrecomputedJoinHashes(HashJoinSharedPayload *payload,
						  dsa_area *dsa,
						  uint32_t global_row_offset,
						  uint32_t row_count)
{
	if (payload == nullptr || dsa == nullptr || row_count == 0)
		return;
	if (!DsaPointerIsValid(payload->hash_table_dp) ||
		!DsaPointerIsValid(payload->hash_buckets_dp) ||
		!DsaPointerIsValid(payload->hash_links_dp))
		elog(ERROR, "pg_yaap: hash join prelinked arrays missing");

	auto *bucket_heads = static_cast<pg_atomic_uint32 *>(dsa_get_address(dsa, payload->hash_table_dp));
	auto *buckets = static_cast<const uint32_t *>(dsa_get_address(dsa, payload->hash_buckets_dp));
	auto *links = static_cast<uint32_t *>(dsa_get_address(dsa, payload->hash_links_dp));
	if (bucket_heads == nullptr || buckets == nullptr || links == nullptr)
		elog(ERROR, "pg_yaap: hash join prelinked arrays unresolved");

	for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
	{
		const uint32_t global_row_idx = global_row_offset + row_idx;
		const uint32_t bucket = buckets[global_row_idx];
		links[global_row_idx] = pg_atomic_exchange_u32(&bucket_heads[bucket], global_row_idx);
	}
}

static void
EnsureBuildRowMappingCapacity(ExecCtx &ctx,
                              HashJoinSharedPayload *payload,
                              uint32_t target_rows)
{
	size_t bytes = static_cast<size_t>(std::max(1u, target_rows)) * sizeof(uint32_t);
	if (!DsaPointerIsValid(payload->build_row_slots_dp))
		payload->build_row_slots_dp = dsa_allocate(ctx.dsa, bytes);
	if (!DsaPointerIsValid(payload->build_row_local_idxs_dp))
		payload->build_row_local_idxs_dp = dsa_allocate(ctx.dsa, bytes);
}

static void
EnsureEmptyJoinGlobalCapacity(ExecCtx &ctx,
                              const TupleDataLayout *layout,
                              dsa_pointer layout_dp,
                              dsa_pointer *tdc_dp,
                              uint32_t target_rows,
                              uint32_t target_heap_used)
{
	TupleDataCollection *tdc = ResolveTdc(ctx.dsa, *tdc_dp);
	if (tdc == nullptr)
		elog(ERROR, "pg_yaap: hash join global TDC missing during combine prepare");
	if (tdc->row_capacity >= target_rows && tdc->heap_capacity >= target_heap_used)
		return;
	if (ReadTdcRowCount(tdc) != 0 || ReadTdcHeapUsed(tdc) != 0)
		elog(ERROR, "pg_yaap: hash join combine prepare expected empty global TDC");

	uint32_t new_row_capacity = std::max(tdc->row_capacity, std::max(1u, target_rows));
	new_row_capacity = TupleDataCollectionClampRowCapacity(new_row_capacity,
		layout->row_width,
		target_heap_used,
		std::max(1u, target_rows));

	uint32_t new_heap_capacity = tdc->heap_capacity;
	if (new_heap_capacity < target_heap_used)
	{
		new_heap_capacity = TupleDataCollectionGrowHeapCapacity(layout,
			tdc,
			new_row_capacity,
			target_heap_used);
	}
	if (new_heap_capacity < target_heap_used)
		elog(ERROR, "pg_yaap: hash join combine prepare failed to size string heap");

	dsa_pointer new_tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
		new_row_capacity,
		layout->row_width,
		new_heap_capacity);
	TupleDataCollection *new_tdc = ResolveTdc(ctx.dsa, new_tdc_dp);
	TupleDataCollectionInit(new_tdc,
		new_row_capacity,
		layout->row_width,
		layout_dp,
		new_heap_capacity);
	dsa_free(ctx.dsa, *tdc_dp);
	*tdc_dp = new_tdc_dp;
}

static void
PrepareHashJoinCombine(ExecCtx &ctx, HashJoinGlobalSinkState &global)
{
	HashJoinSharedPayload *payload = global.payload;
	if (payload == nullptr)
		elog(ERROR, "pg_yaap: hash join combine missing shared payload");

	uint32 expected = 0;
	if (pg_atomic_compare_exchange_u32(&payload->combine_prepare_state, &expected, 1))
	{
		HashJoinLocalBuildRegistryEntry *registry = ResolveLocalBuildRegistry(ctx.dsa, payload);
		if (registry == nullptr)
			elog(ERROR, "pg_yaap: hash join combine registry missing");

		uint64_t total_rows = 0;
		uint64_t total_key_heap = 0;
		uint64_t total_row_heap = 0;
		for (uint32_t slot = 0; slot < payload->local_state_slot_count; ++slot)
		{
			TupleDataCollection *key_tdc = ResolveTdc(ctx.dsa, registry[slot].build_keys_dp);
			TupleDataCollection *row_tdc = ResolveTdc(ctx.dsa, registry[slot].build_rows_dp);
			registry[slot].row_count = ReadTdcRowCount(key_tdc);
			registry[slot].key_heap_used = ReadTdcHeapUsed(key_tdc);
			registry[slot].row_heap_used = ReadTdcHeapUsed(row_tdc);
			registry[slot].global_row_offset = static_cast<uint32_t>(total_rows);
			registry[slot].global_key_heap_offset = static_cast<uint32_t>(total_key_heap);
			registry[slot].global_row_heap_offset = static_cast<uint32_t>(total_row_heap);
			total_rows += registry[slot].row_count;
			total_key_heap += registry[slot].key_heap_used;
			total_row_heap += registry[slot].row_heap_used;
		}
		if (total_rows > UINT32_MAX || total_key_heap > UINT32_MAX || total_row_heap > UINT32_MAX)
			elog(ERROR, "pg_yaap: hash join combine totals exceed uint32 range");
		payload->hash_table_capacity = static_cast<uint32_t>(total_rows) > 0
			? ComputeHashTableCapacity(static_cast<uint32_t>(total_rows))
			: 0u;

		SpinLockAcquire(&payload->mutex);
		EnsureEmptyJoinGlobalCapacity(ctx,
			global.build_key_layout,
			global.build_key_layout_dp,
			&payload->build_keys_dp,
			static_cast<uint32_t>(total_rows),
			static_cast<uint32_t>(total_key_heap));
		if (payload->build_rows_use_keys)
			payload->build_rows_dp = payload->build_keys_dp;
		if (!payload->build_rows_shared_local && !payload->build_rows_use_keys)
		{
			EnsureEmptyJoinGlobalCapacity(ctx,
				global.build_layout,
				global.build_layout_dp,
				&payload->build_rows_dp,
				static_cast<uint32_t>(total_rows),
				static_cast<uint32_t>(total_row_heap));
		}
		else if (payload->build_rows_shared_local)
		{
			EnsureBuildRowMappingCapacity(ctx, payload, static_cast<uint32_t>(total_rows));
		}
		if (static_cast<uint32_t>(total_rows) > 0)
		{
			payload->hash_table_dp = dsa_allocate(ctx.dsa,
				static_cast<size_t>(payload->hash_table_capacity) * sizeof(uint32_t));
			payload->hash_buckets_dp = dsa_allocate(ctx.dsa,
				static_cast<size_t>(total_rows) * sizeof(uint32_t));
			payload->hash_links_dp = dsa_allocate(ctx.dsa,
				static_cast<size_t>(total_rows) * sizeof(uint32_t));
			payload->hash_salts_dp = dsa_allocate(ctx.dsa,
				static_cast<size_t>(total_rows) * sizeof(uint16_t));
			std::memset(dsa_get_address(ctx.dsa, payload->hash_table_dp),
				0xFF,
				static_cast<size_t>(payload->hash_table_capacity) * sizeof(uint32_t));
		}
		SpinLockRelease(&payload->mutex);

		payload->combined_row_count = static_cast<uint32_t>(total_rows);
		payload->combined_key_heap_used = static_cast<uint32_t>(total_key_heap);
		payload->combined_row_heap_used = payload->build_rows_use_keys
			? static_cast<uint32_t>(total_key_heap)
			: static_cast<uint32_t>(total_row_heap);
		pg_atomic_write_u32(&payload->combine_prepare_state, 2u);
	}
	else
	{
		while (pg_atomic_read_u32(&payload->combine_prepare_state) != 2u)
		{
			CHECK_FOR_INTERRUPTS();
			pg_usleep(1000L);
		}
	}

	global.build_keys = ResolveTdc(ctx.dsa, payload->build_keys_dp);
	global.build_rows = ResolveTdc(ctx.dsa, payload->build_rows_dp);
	if (global.build_keys == nullptr || (!payload->build_rows_shared_local && global.build_rows == nullptr))
		elog(ERROR, "pg_yaap: hash join combine global TDC missing after prepare");
}

static void
CopyStringAwareRowsIntoGlobal(const TupleDataLayout *layout,
                              const TupleDataCollection *src,
                              TupleDataCollection *dst,
                              uint32_t row_offset,
                              uint32_t row_count,
                              uint32_t heap_offset)
{
	if (layout == nullptr || row_count == 0)
		return;

	for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
	{
		const uint8_t *src_row = TupleDataCollectionGetRowConst(src, row_idx);
		uint8_t *dst_row = TupleDataCollectionGetRow(dst, row_offset + row_idx);
		std::memcpy(dst_row, src_row, layout->row_width);
		for (uint16_t col_idx = 0; col_idx < layout->column_count; ++col_idx)
		{
			const TdcColumnDesc &col = layout->columns[col_idx];
			if (col.kind != TdcColumnKind::STRING_REF)
				continue;

			VecStringRef ref;
			std::memcpy(&ref, src_row + col.offset, sizeof(ref));
			if (ref.len == 0 || VecStringRefIsInline(ref))
				continue;

			ref.offset += heap_offset;
			std::memcpy(dst_row + col.offset, &ref, sizeof(ref));
		}
	}
}

static void
CopyLocalTdcIntoGlobal(const TupleDataLayout *layout,
                       const TupleDataCollection *src,
                       TupleDataCollection *dst,
                       uint32_t row_offset,
                       uint32_t heap_offset)
{
	const uint32_t row_count = ReadTdcRowCount(src);
	const uint32_t heap_used = ReadTdcHeapUsed(src);
	if (heap_used > 0)
	{
		std::memcpy(TupleDataCollectionHeap(dst) + heap_offset,
			TupleDataCollectionHeapConst(src),
			heap_used);
	}

	bool has_string_ref = false;
	for (uint16_t col_idx = 0; col_idx < layout->column_count; ++col_idx)
	{
		if (layout->columns[col_idx].kind == TdcColumnKind::STRING_REF)
		{
			has_string_ref = true;
			break;
		}
	}

	if (row_count == 0)
		return;
	if (!has_string_ref)
	{
		std::memcpy(TupleDataCollectionGetRow(dst, row_offset),
			TupleDataCollectionGetRowConst(src, 0),
			static_cast<size_t>(row_count) * layout->row_width);
		return;
	}
	CopyStringAwareRowsIntoGlobal(layout, src, dst, row_offset, row_count, heap_offset);
}

}  // namespace

SinkCombineResultType
ExecuteHashJoinCombine(ExecCtx &ctx,
                       HashJoinGlobalSinkState &global,
                       HashJoinLocalSinkState &local)
{
	if (local.build_keys == nullptr || local.build_rows == nullptr)
		elog(ERROR, "pg_yaap: hash join combine local build rows missing");
	if (ctx.worker_index < 0)
		elog(ERROR, "pg_yaap: hash join combine requires worker-local slot");

	PrepareHashJoinCombine(ctx, global);

	HashJoinLocalBuildRegistryEntry *registry = ResolveLocalBuildRegistry(ctx.dsa, global.payload);
	if (registry == nullptr || static_cast<uint32_t>(ctx.worker_index) >= global.payload->local_state_slot_count)
		elog(ERROR, "pg_yaap: hash join combine registry slot missing");
	HashJoinLocalBuildRegistryEntry &entry = registry[ctx.worker_index];

	CopyLocalTdcIntoGlobal(global.build_key_layout,
		local.build_keys,
		global.build_keys,
		entry.global_row_offset,
		entry.global_key_heap_offset);
	PrecomputeJoinHashes(global.build_key_layout,
		local.build_keys,
		global.payload,
		ctx.dsa,
		entry.global_row_offset,
		entry.row_count);
	LinkPrecomputedJoinHashes(global.payload,
		ctx.dsa,
		entry.global_row_offset,
		entry.row_count);
	if (!global.payload->build_rows_use_keys)
		CopyLocalTdcIntoGlobal(global.build_layout,
			local.build_rows,
			global.build_rows,
			entry.global_row_offset,
			entry.global_row_heap_offset);
	return SinkCombineResultType::FINISHED;
}

SinkCombineResultType
ExecuteHashJoinSharedPayloadCombine(ExecCtx &ctx,
                                    HashJoinGlobalSinkState &global,
                                    HashJoinLocalSinkState &local)
{
	if (local.build_keys == nullptr || local.build_rows == nullptr)
		elog(ERROR, "pg_yaap: hash join shared-payload combine local build rows missing");
	if (ctx.worker_index < 0)
		elog(ERROR, "pg_yaap: hash join shared-payload combine requires worker-local slot");

	PrepareHashJoinCombine(ctx, global);

	HashJoinLocalBuildRegistryEntry *registry = ResolveLocalBuildRegistry(ctx.dsa, global.payload);
	if (registry == nullptr || static_cast<uint32_t>(ctx.worker_index) >= global.payload->local_state_slot_count)
		elog(ERROR, "pg_yaap: hash join shared-payload combine registry slot missing");
	HashJoinLocalBuildRegistryEntry &entry = registry[ctx.worker_index];

	CopyLocalTdcIntoGlobal(global.build_key_layout,
		local.build_keys,
		global.build_keys,
		entry.global_row_offset,
		entry.global_key_heap_offset);
	PrecomputeJoinHashes(global.build_key_layout,
		local.build_keys,
		global.payload,
		ctx.dsa,
		entry.global_row_offset,
		entry.row_count);
	LinkPrecomputedJoinHashes(global.payload,
		ctx.dsa,
		entry.global_row_offset,
		entry.row_count);

	auto *slots = DsaPointerIsValid(global.payload->build_row_slots_dp)
		? static_cast<uint32_t *>(dsa_get_address(ctx.dsa, global.payload->build_row_slots_dp))
		: nullptr;
	auto *local_idxs = DsaPointerIsValid(global.payload->build_row_local_idxs_dp)
		? static_cast<uint32_t *>(dsa_get_address(ctx.dsa, global.payload->build_row_local_idxs_dp))
		: nullptr;
	if (slots == nullptr || local_idxs == nullptr)
		elog(ERROR, "pg_yaap: hash join shared-payload combine mapping arrays missing");
	for (uint32_t row_idx = 0; row_idx < entry.row_count; ++row_idx)
	{
		const uint32_t global_row_idx = entry.global_row_offset + row_idx;
		slots[global_row_idx] = static_cast<uint32_t>(ctx.worker_index);
		local_idxs[global_row_idx] = row_idx;
	}
	return SinkCombineResultType::FINISHED;
}

void
PublishHashJoinCombinedRows(HashJoinGlobalSinkState &global)
{
	if (global.payload == nullptr ||
		pg_atomic_read_u32(&global.payload->combine_prepare_state) != 2u)
		return;

	global.build_keys = ResolveTdc(global.dsa, global.payload->build_keys_dp);
	global.build_rows = global.payload->build_rows_use_keys
		? global.build_keys
		: ResolveTdc(global.dsa, global.payload->build_rows_dp);
	if (global.build_keys == nullptr || (!global.payload->build_rows_shared_local && global.build_rows == nullptr))
		elog(ERROR, "pg_yaap: hash join finalize missing combined global TDC");

	pg_atomic_write_u32(&global.build_keys->row_count, global.payload->combined_row_count);
	pg_atomic_write_u32(&global.build_keys->heap_used, global.payload->combined_key_heap_used);
	if (!global.payload->build_rows_shared_local && !global.payload->build_rows_use_keys)
	{
		pg_atomic_write_u32(&global.build_rows->row_count, global.payload->combined_row_count);
		pg_atomic_write_u32(&global.build_rows->heap_used, global.payload->combined_row_heap_used);
	}
	global.payload->combined = true;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
