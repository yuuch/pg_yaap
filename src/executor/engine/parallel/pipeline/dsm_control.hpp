#pragma once

extern "C" {
#include "postgres.h"
#include "port/atomics.h"
#include "utils/dsa.h"
#include "storage/sharedfileset.h"
}

#include <sys/types.h>  /* pid_t */

#include "buffer_manager/yaap_buffer_shared.hpp"

namespace pg_yaap {
namespace pipeline {

/*
 * 3g.2-final extension (Oracle C7 design): worker-error message buffer length.
 * worker_error.hpp claims the diagnostic buffer, writes it, then publishes
 * READY with a memory barrier. Readers acquire READY before reading text.
 */
static constexpr int PIPELINE_WORKER_ERROR_MSG_LEN = 512;

/*
 * shm_toc keys for the MetaPipeline runtime DSM segment.
 *
 * Per docs/GLOBAL_LOCAL_STATE_DESIGN.md §8.5.2 / §8.6 (HEAD eb7901b022a),
 * the segment publishes EXACTLY THREE keys -- no PlannedStmt, no query text,
 * no partial fileset, no param-exec, no per-source ParallelTableScanDesc.
 * Workers reconstruct PhysicalOperator instances from the DSA-resident
 * OpDescriptor[] reachable via PipelineSharedControl::pipelines_root.
 *
 * Keys remain in the 0xD800000000000000 high-bit range to make accidental
 * cross-attach to a stale (pre-greenfield) DSM segment impossible. Old key
 * IDs 0x...0002..0007 are intentionally retired and MUST NOT be re-used.
 */
static constexpr uint64 PIPELINE_DSM_KEY_CONTROL      = UINT64CONST(0xD800000000000001);
static constexpr uint64 PIPELINE_DSM_KEY_DSA          = UINT64CONST(0xD800000000000008);
static constexpr uint64 PIPELINE_DSM_KEY_TASK_QUEUE   = UINT64CONST(0xD800000000000009);
/*
 * Per-worker startup-ready bit array (Oracle race-fix design):
 * pg_atomic_uint32[control->num_workers] in DSM. Workers set their slot to 1
 * AFTER BackgroundWorkerInitializeConnectionByOid() returns (which internally
 * runs InitPostgres -> InitProcessPhase2, making PGPROC discoverable via
 * BackendPidGetProc). The leader polls this array before calling
 * BackendPidGetProc(worker_pid), closing the BGWH_STARTED-vs-ProcArray race.
 */
static constexpr uint64 PIPELINE_DSM_KEY_WORKER_READY = UINT64CONST(0xD80000000000000A);

static constexpr uint32 PIPELINE_DSM_MAGIC = 0x56505043;

/*
 * Per-query control block published at PIPELINE_DSM_KEY_CONTROL.
 *
 * Plan-shape-agnostic. Workers attach, validate magic, then walk the
 * DSA-resident PipelineDescriptor[] rooted at pipelines_root to reconstruct
 * the operator graph. See §8.5.4.2 (POD layout) and §8.5.4.4 (worker
 * reconstruction) for the IR contract.
 */
struct PipelineSharedControl
{
	uint32           magic;             /* == PIPELINE_DSM_MAGIC */
	int32            num_pipelines;     /* length of PipelineDescriptor[] at pipelines_root */
	pg_atomic_uint32 worker_error;      /* WorkerErrorState: NONE / WRITING / READY */
	dsa_pointer      pipelines_root;    /* DSA pointer to PipelineDescriptor[num_pipelines] */

	pid_t            leader_pid;
	pg_atomic_uint32 shutdown_requested;
	char             worker_error_msg[PIPELINE_WORKER_ERROR_MSG_LEN];

	dsa_pointer      event_states_root;
	dsa_pointer      event_successors_root;
	uint32           event_count;
	pg_atomic_uint32 events_remaining;
	pg_atomic_uint32 scheduler_started;
	pg_atomic_uint32 scheduler_busy;
	uint32           active_probe_run;

	/*
	 * 3g.2-final bgworker handoff: bgworker entry points have signature
	 *   void(Datum main_arg)
	 * (see bgworker.h:79 bgworker_main_type). We therefore cannot pass both
	 * the dsm_handle and MyDatabaseId through bgw_main_arg. Convention:
	 *   bgw_main_arg = UInt32GetDatum(dsm_segment_handle(runtime_dsm))
	 * and the worker reads db_oid from this control block after attaching
	 * the DSM segment. Set by leader in CreateRuntimeDsm.
	 */
	Oid              db_oid;
	int32            num_workers;
	/* Backend-local execution settings captured before bgworker launch. */
	uint8            jit_deform;
	/* Serialized leader active snapshot in the query DSA. */
	dsa_pointer      snapshot_root;

	/*
	 * Optional per-query profiling storage. The leader allocates the records
	 * after TaskScheduler has built the Event DAG, because event_count is not
	 * known when the runtime DSM is first created. Workers only write their own
	 * worker slot; the leader aggregates after all workers have stopped.
	 */
	pg_atomic_uint32 profile_enabled;
	pg_atomic_uint32 trace_execution_path;
	dsa_pointer      profile_records_root;
	dsa_pointer      profile_slot_pids_root;
	uint32           profile_event_count;
	uint32           profile_worker_slots;
	/* Immutable after publication; worker-exclusive Task timeline storage. */
	uint32           scan_io_profile_mode;
    dsa_pointer      scan_io_profile_root;
    uint32           scan_io_reported;
    uint32           scan_io_min_pages;
    uint32           scan_io_wait_pct;
    uint32           scan_profile_mode;
	dsa_pointer      scan_profile_root;
	uint32           scan_profile_reported;
	uint32           timeline_mode;
	dsa_pointer      timeline_root;

	/* Query-scoped accounting for DSA-backed breaker storage. */
	uint64_t dsa_limit_bytes = 0;
	dsa_pointer      buffer_state_root;
	/* Stable root for lazy shared breaker segments. */
	dsa_pointer      shared_segment_arena_root;
	dsa_pointer      large_shared_segment_arena_root;
	pg_atomic_uint32 next_partition_probe_arena_id;
	/* Logical reservations for simultaneously resident hybrid initial batches. */
	pg_atomic_uint64 hybrid_resident_bytes;
	/* Query-scoped spill namespace; initialized by the leader in DSM. */
	SharedFileSet    spill_fileset;
};

/* Scheduler-owned event state. The elected worker is the only writer of the
 * graph, phase progress and backlog. Execution workers release atomic task
 * counts after publishing all task output. */
enum class SharedEventPhase : uint8_t { WAITING, RUNNING, FINISHED };

struct EventShmState
{
	uint32_t successor_offset;
	uint32_t successor_count;
	uint32_t dependencies_remaining;
	SharedEventPhase phase;
	bool partitioned;
	uint32_t task_count;
	uint32_t next_task;
	uint32_t max_concurrency;
	pg_atomic_uint32 tasks_remaining;
	pg_atomic_uint32 tasks_in_flight;
	int32_t activated_by;
	int32_t completed_by;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
