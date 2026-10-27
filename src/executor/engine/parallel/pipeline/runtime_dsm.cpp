extern "C" {
#include "postgres.h"

#include "miscadmin.h"
#include "storage/dsm.h"
#include "storage/lwlock.h"
#include "storage/shm_toc.h"
#include "utils/dsa.h"
}

#include <cstring>
#include <new>
#include <algorithm>

#include "parallel/pipeline/runtime_dsm.hpp"
#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/dsm_task_queue.hpp"
#include "buffer_manager/yaap_buffer_shared.hpp"
#include "buffer_manager/yaap_shared_segment_arena.hpp"

extern "C" {
extern int pg_yaap_parallel_max_workers;
extern int pg_yaap_memory_limit_mb;
extern int pg_yaap_memory_soft_limit_mb;
extern int pg_yaap_dsa_memory_limit_mb;
extern bool pg_yaap_jit_deform;
extern int pg_yaap_scan_read_stream;
extern int pg_yaap_scan_prefetch_buffers;
extern int pg_yaap_scan_prefetch_requests;
extern int pg_yaap_scan_prefetch_batch_pages;
extern int pg_yaap_profile_scan_blocks;
extern bool pg_yaap_trace_execution_path;
extern int pg_yaap_runtime_dsa_tranche_id(void);
}

namespace pg_yaap {
namespace pipeline {

static constexpr uint32_t kRuntimeTaskQueueCapacity = 64;
static constexpr uint32_t kSharedSegmentSizeBytes    = 256u * 1024u;
/* Keep wide-row spill granularity small enough to leave headroom under the
 * query DSA limit; rows still remain append-only and segment-backed. */
static constexpr uint32_t kLargeSharedSegmentSizeBytes = 1u * 1024u * 1024u;
static constexpr int      kRuntimeTocNumKeys        = 4;

bool
CreateRuntimeDsm(PgYaapQueryState *state, const char **error_out)
{
	Assert(state != nullptr);
	Assert(state->runtime_dsm == nullptr);
	Assert(state->runtime_dsa == nullptr);
	const uint64_t configured_bytes = static_cast<uint64_t>(pg_yaap_memory_limit_mb) *
		1024u * 1024u;
	if (configured_bytes > static_cast<uint64_t>(SIZE_MAX))
	{
		if (error_out != nullptr)
			*error_out = "pg_yaap: memory_limit exceeds addressable size";
		return false;
	}
	const size_t runtime_dsa_limit = static_cast<size_t>(configured_bytes);
	/* A budget is a ceiling, not a reservation. In particular mmap DSM
	 * creation writes every byte of its backing file before we can run. Let
	 * DSA add segments on demand instead of zero-filling the query limit. */
	const size_t runtime_dsa_size = std::min(runtime_dsa_limit,
		std::max<size_t>(dsa_minimum_size(), 4u * 1024u * 1024u));
	const uint64_t configured_soft_bytes = pg_yaap_memory_soft_limit_mb >= 0
		? static_cast<uint64_t>(pg_yaap_memory_soft_limit_mb) * 1024u * 1024u : 0;
	const uint64_t soft_limit = pg_yaap_memory_soft_limit_mb >= 0 &&
		configured_soft_bytes <= runtime_dsa_limit
		? configured_soft_bytes
		: runtime_dsa_limit * 3 / 4;

	shm_toc_estimator estimator;
	shm_toc_initialize_estimator(&estimator);
	shm_toc_estimate_chunk(&estimator, sizeof(PipelineSharedControl));
	shm_toc_estimate_chunk(&estimator, runtime_dsa_size);
	shm_toc_estimate_chunk(&estimator,
						   DsmTaskQueue::EstimateSize(kRuntimeTaskQueueCapacity));
	const int num_workers = std::max(1, pg_yaap_parallel_max_workers);
	const Size worker_ready_bytes =
		static_cast<Size>(num_workers) * sizeof(pg_atomic_uint32);
	shm_toc_estimate_chunk(&estimator, worker_ready_bytes);
	shm_toc_estimate_keys(&estimator, kRuntimeTocNumKeys);
	const Size segsize = shm_toc_estimate(&estimator);

	/* The payload budget and total DSA segment ceiling are separate. Keep
	 * the historical bounded metadata allowance unless explicitly set. */
	const size_t control_bytes = segsize - runtime_dsa_size;
	const uint64_t dsa_headroom = std::min<uint64_t>(
		runtime_dsa_limit / 2, UINT64CONST(4) * 1024u * 1024u * 1024u);
	const uint64_t dsa_limit = pg_yaap_dsa_memory_limit_mb >= 0
		? static_cast<uint64_t>(pg_yaap_dsa_memory_limit_mb) * 1024u * 1024u
		: runtime_dsa_limit + dsa_headroom - control_bytes;
	if (dsa_limit > static_cast<uint64_t>(SIZE_MAX) ||
		(pg_yaap_dsa_memory_limit_mb >= 0 && dsa_limit < runtime_dsa_limit))
	{
		if (error_out != nullptr)
			*error_out = "pg_yaap: dsa_memory_limit must be addressable and at least memory_limit";
		return false;
	}

	dsm_segment *seg = dsm_create(segsize, DSM_CREATE_NULL_IF_MAXSEGMENTS);
	if (seg == nullptr)
	{
		if (error_out != nullptr)
			*error_out = "pg_yaap: dsm_create returned NULL (max DSM segments exhausted)";
		return false;
	}

	shm_toc *toc = shm_toc_create(PIPELINE_DSM_MAGIC,
								  dsm_segment_address(seg),
								  segsize);

	void *control_mem = shm_toc_allocate(toc, sizeof(PipelineSharedControl));
	auto *control = new (control_mem) PipelineSharedControl();
	control->magic = PIPELINE_DSM_MAGIC;
	control->num_pipelines = 0;
	pg_atomic_init_u32(&control->worker_error, 0);
	control->pipelines_root = InvalidDsaPointer;
	control->leader_pid = MyProcPid;
	pg_atomic_init_u32(&control->shutdown_requested, 0);
	std::memset(control->worker_error_msg, 0, sizeof(control->worker_error_msg));
	control->event_states_root = InvalidDsaPointer;
	control->event_count = 0;
	control->db_oid = MyDatabaseId;
	control->num_workers = num_workers;
	control->jit_deform = pg_yaap_jit_deform ? 1 : 0;
	control->scan_read_stream = pg_yaap_scan_read_stream;
	control->scan_prefetch_buffers = pg_yaap_scan_prefetch_buffers;
	control->scan_prefetch_requests = pg_yaap_scan_prefetch_requests;
	control->scan_prefetch_batch_pages = pg_yaap_scan_prefetch_batch_pages;
	control->profile_scan_blocks = pg_yaap_profile_scan_blocks;
	pg_atomic_init_u32(&control->profile_enabled, 0);
	pg_atomic_init_u32(&control->trace_execution_path, 0);
	control->profile_records_root = InvalidDsaPointer;
	control->profile_event_count = 0;
	control->profile_worker_slots = 0;
	control->timeline_mode = 0;
	control->timeline_root = InvalidDsaPointer;
	control->buffer_state_root = InvalidDsaPointer;
	control->shared_segment_arena_root = InvalidDsaPointer;
	control->large_shared_segment_arena_root = InvalidDsaPointer;
	pg_atomic_init_u32(&control->next_partition_probe_arena_id, 3);
	pg_atomic_init_u64(&control->hybrid_resident_bytes, 0);
	shm_toc_insert(toc, PIPELINE_DSM_KEY_CONTROL, control);

	void *ready_mem = shm_toc_allocate(toc, worker_ready_bytes);
	auto *ready_array = static_cast<pg_atomic_uint32 *>(ready_mem);
	for (int i = 0; i < num_workers; ++i)
		pg_atomic_init_u32(&ready_array[i], 0);
	shm_toc_insert(toc, PIPELINE_DSM_KEY_WORKER_READY, ready_mem);

	void *dsa_place = shm_toc_allocate(toc, runtime_dsa_size);
	int tranche_id = pg_yaap_runtime_dsa_tranche_id();
	dsa_area *area = dsa_create_in_place(dsa_place,
										 runtime_dsa_size,
										 tranche_id,
										 seg);
	dsa_pin_mapping(area);
	control->dsa_limit_bytes = dsa_limit;
	dsa_set_size_limit(area, static_cast<size_t>(dsa_limit));
	shm_toc_insert(toc, PIPELINE_DSM_KEY_DSA, dsa_place);

	dsa_pointer buffer_state_dp = dsa_allocate0(area, sizeof(buffer::YaapBufferSharedState));
	auto *buffer_state = static_cast<buffer::YaapBufferSharedState *>(
		dsa_get_address(area, buffer_state_dp));
	buffer::YaapBufferSharedStateInit(buffer_state,
								  runtime_dsa_limit - control_bytes,
								  soft_limit);
	control->buffer_state_root = buffer_state_dp;
	control->shared_segment_arena_root = buffer::YaapSharedSegmentArenaCreate(
		area, buffer_state, 1, kSharedSegmentSizeBytes);
	control->large_shared_segment_arena_root = buffer::YaapSharedSegmentArenaCreate(
		area, buffer_state, 2, kLargeSharedSegmentSizeBytes);
	SharedFileSetInit(&control->spill_fileset, seg);

	void *queue_mem = shm_toc_allocate(toc,
									   DsmTaskQueue::EstimateSize(kRuntimeTaskQueueCapacity));
	(void) DsmTaskQueue::InitInPlace(queue_mem, kRuntimeTaskQueueCapacity);
	shm_toc_insert(toc, PIPELINE_DSM_KEY_TASK_QUEUE, queue_mem);

	state->runtime_dsm = seg;
	state->runtime_dsa = area;
	return true;
}

void
DestroyRuntimeDsm(PgYaapQueryState *state)
{
	if (state == nullptr)
		return;
	if (state->runtime_dsm != nullptr && state->runtime_dsa != nullptr)
	{
		shm_toc *toc = shm_toc_attach(PIPELINE_DSM_MAGIC,
			dsm_segment_address(state->runtime_dsm));
		auto *control = static_cast<PipelineSharedControl *>(
			shm_toc_lookup(toc, PIPELINE_DSM_KEY_CONTROL, true));
		if (control != nullptr)
		{
			auto *buffer_state = DsaPointerIsValid(control->buffer_state_root)
				? static_cast<buffer::YaapBufferSharedState *>(
					dsa_get_address(state->runtime_dsa, control->buffer_state_root))
				: nullptr;
			if (buffer_state != nullptr && pg_yaap_trace_execution_path)
				ereport(LOG, (errmsg("pg_yaap memory resident=%llu peak=%llu committed=%llu spill=%llu allocations=%llu pressure_epoch=%llu dsa_total=%zu payload_limit=%llu dsa_limit=%llu",
					(unsigned long long) buffer::YaapBufferResidentBytes(buffer_state),
					(unsigned long long) pg_atomic_read_u64(&buffer_state->peak_resident_bytes),
					(unsigned long long) pg_atomic_read_u64(&buffer_state->committed_bytes),
					(unsigned long long) pg_atomic_read_u64(&buffer_state->spill_bytes),
					(unsigned long long) pg_atomic_read_u64(&buffer_state->allocation_count),
					(unsigned long long) pg_atomic_read_u64(&buffer_state->pressure_epoch),
					dsa_get_total_size(state->runtime_dsa),
					(unsigned long long) buffer_state->hard_limit,
					(unsigned long long) control->dsa_limit_bytes)));
			if (buffer_state != nullptr)
			{
				if (DsaPointerIsValid(control->shared_segment_arena_root))
					buffer::YaapSharedSegmentArenaDestroy(state->runtime_dsa, buffer_state,
						control->shared_segment_arena_root);
				if (DsaPointerIsValid(control->large_shared_segment_arena_root))
					buffer::YaapSharedSegmentArenaDestroy(state->runtime_dsa, buffer_state,
						control->large_shared_segment_arena_root);
			}
			SharedFileSetDeleteAll(&control->spill_fileset);
		}
	}
	if (state->runtime_dsa != nullptr)
	{
		dsa_detach(state->runtime_dsa);
		state->runtime_dsa = nullptr;
	}
	if (state->runtime_dsm != nullptr)
	{
		dsm_detach(state->runtime_dsm);
		state->runtime_dsm = nullptr;
	}
}

}
}
