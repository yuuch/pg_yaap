#pragma once

extern "C" {
#include "postgres.h"
#include "port/atomics.h"
#include "utils/dsa.h"
}

#include <sys/types.h>  /* pid_t */

namespace pg_yaap {
namespace pipeline {

/*
 * 3g.2-final extension (Oracle C7 design): worker-error message buffer length.
 * Workers PG_CATCH then snprintf the error text into worker_error_msg[] under
 * a CAS guard on worker_error before re-raising. Leader picks up the message
 * verbatim in its event loop (vs. a plain "worker died" placeholder).
 */
static constexpr int PIPELINE_WORKER_ERROR_MSG_LEN = 256;

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
	pg_atomic_uint32 worker_error;      /* set by any worker on ERROR (§8.5.2 worker contract) */
	dsa_pointer      pipelines_root;    /* DSA pointer to PipelineDescriptor[num_pipelines] */

	/*
	 * 3g.2-final additions (Oracle C7 design — see C7 leader event loop and
	 * C8 worker pump):
	 *
	 *   leader_pid          - set by leader pre-launch; workers SetLatch the
	 *                         corresponding PGPROC after task completion to
	 *                         wake the leader's WaitLatch loop.
	 *   shutdown_requested  - leader sets to 1 on error/cancel; workers poll
	 *                         between tasks and exit cleanly. Workers also
	 *                         set this on PG_CATCH so siblings drain.
	 *   worker_error_msg    - first-writer-wins (gated by worker_error CAS):
	 *                         the worker that flips worker_error from 0->1
	 *                         is the sole owner of this buffer.
	 *   event_states_root   - DSA pointer to EventShmState[event_count],
	 *                         one slot per scheduled Event. Workers
	 *                         pg_atomic_sub_fetch_u32 on tasks_remaining
	 *                         after Task::Execute; the worker that brings
	 *                         it to 0 SetLatch's the leader so the leader
	 *                         calls FinishEvent() (Event objects are
	 *                         leader-process-local; workers MUST NOT touch
	 *                         them directly).
	 *   event_count         - length of EventShmState[] at event_states_root.
	 */
	pid_t            leader_pid;
	pg_atomic_uint32 shutdown_requested;
	char             worker_error_msg[PIPELINE_WORKER_ERROR_MSG_LEN];
	dsa_pointer      event_states_root;
	uint32           event_count;

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
};

/*
 * Per-Event shared completion counter (Oracle C7 design). One slot per
 * scheduled Event lives in DSA at PipelineSharedControl::event_states_root.
 *
 * Protocol:
 *   - Leader initializes tasks_remaining to N (number of Tasks the Event
 *     enqueued) BEFORE pushing any of those Tasks to the queue.
 *   - Workers pg_atomic_sub_fetch_u32(&slot.tasks_remaining, 1) after each
 *     Task::Execute completes. The worker observing the result == 0 is the
 *     sole completer; it SetLatch's leader_pid's PGPROC. Only the leader
 *     then calls Event::FinishEvent() / Event::Abort() on its in-process
 *     std::shared_ptr<Event> instance.
 *
 * Workers MUST NOT touch Event objects directly: those are
 * leader-process-local std::shared_ptr instances and crossing the process
 * boundary on them is undefined behavior.
 */
struct EventShmState
{
	pg_atomic_uint32 tasks_remaining;
	pg_atomic_uint32 saw_error;     /* set by any failed worker so the leader knows to Abort */
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
