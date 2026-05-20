#pragma once

/*
 * pipeline/dsm_task_queue.hpp  (M-FRAME-MIN step 3e)
 *
 * DSM-resident MPMC bounded task queue. Locked by:
 *   docs/PIPELINE_PORT_PLAN.md §15.5
 *   docs/GLOBAL_LOCAL_STATE_DESIGN.md §8.5
 *
 * Cross-process design constraints:
 *   - Lives inline in the DSM segment under PIPELINE_DSM_KEY_TASK_QUEUE.
 *     Sized at attach time via EstimateSize(capacity).
 *   - Stores POD TaskDescriptors (NOT C++ Task*). Workers reconstruct the
 *     concrete Task subclass locally from the descriptor + per-process
 *     PipelineDsmLookup (3f).
 *   - MPMC via Vyukov bounded queue: per-slot pg_atomic_uint32 sequence
 *     counter + atomic head/tail cursors. Wait-free TryPush/TryPop.
 *   - Wakeup: every successful Push calls SetLatch on every registered
 *     worker latch. Workers block in WaitLatch(MyLatch, WL_LATCH_SET |
 *     WL_EXIT_ON_PM_DEATH, 0, WAIT_EVENT_EXTENSION) when TryPop returns
 *     false. NO ConditionVariable (forbidden by §8.5 / Oracle §14.3 B4).
 *   - Capacity must be a power of two (mask-based slot index).
 *
 * BLOCKED is intentionally absent from the task path; see task.hpp.
 */

#include <cstddef>
#include <cstdint>

extern "C" {
#include "postgres.h"
#include "port/atomics.h"
#include "storage/latch.h"
#include <sys/types.h>            /* pid_t */
}

namespace pg_yaap {
namespace pipeline {

enum class TaskKind : uint8_t
{
	RUN      = 1,
	COMBINE  = 2,
	FINALIZE = 3,
};

/*
 * POD descriptor written into DSM. Layout-stable across processes; do NOT add
 * non-trivial members. event_id / pipeline_id are scheduler-assigned ids that
 * workers resolve via PipelineDsmLookup (3f) into the local Task / Event
 * objects.
 */
struct TaskDescriptor
{
	uint32_t pipeline_id;
	uint32_t event_id;
	uint32_t partition_id;
	int32_t  worker_index;
	uint8_t  kind;             /* TaskKind */
	uint8_t  pad_[3];
};

static_assert(sizeof(TaskDescriptor) == 20, "TaskDescriptor must stay POD/20B");

/*
 * Per-slot cell. sequence is a Vyukov ticket: producer waits for sequence ==
 * pos, consumer waits for sequence == pos + 1, both advance by capacity on
 * success. This gives correct MPMC ordering with no spinlock.
 */
struct DsmTaskQueueCell
{
	pg_atomic_uint32 sequence;
	uint32_t         pad_;
	TaskDescriptor   desc;
};

static_assert(sizeof(DsmTaskQueueCell) == 28, "DsmTaskQueueCell layout");

/*
 * Inline header in DSM. Followed by `capacity` cells. Worker latch table is
 * registered out-of-band by TaskScheduler (3g) and stored in
 * PipelineSharedControl, NOT here, because Latch* lifetime is per-process.
 *
 * Use EstimateSize(capacity) to compute the DSM allocation; never sizeof().
 */
class DsmTaskQueue
{
public:
	/* Power-of-two capacity required. */
	static size_t EstimateSize(uint32_t capacity);

	/* Construct in-place into a DSM-resident buffer of EstimateSize(capacity)
	 * bytes. capacity MUST be a power of two and >= 2. */
	static DsmTaskQueue *InitInPlace(void *buffer, uint32_t capacity);

	/* Re-bind a DSM segment that was already initialized by another process.
	 * No state mutation; just a typed cast + capacity sanity check. */
	static DsmTaskQueue *AttachInPlace(void *buffer);

	/* Wait-free MPMC. Returns true on success, false if the queue is full
	 * (caller decides whether to retry / yield / abort). On successful Push
	 * the queue wakes every registered worker via PID lookup. */
	bool TryPush(const TaskDescriptor &desc);

	/* Wait-free MPMC. Returns true on success, false if empty. */
	bool TryPop(TaskDescriptor *out);
	bool TryPopForWorker(int32_t worker_index, TaskDescriptor *out);

	/* Register PIDs the queue should wake on every successful Push and on
	 * every successful affine Pop. PIDs (vs Latch*) are DSM-safe across
	 * processes: workers resolve them via BackendPidGetProc each wake.
	 * Caller (leader) must call this after all worker PIDs are known and
	 * before EnqueueTasks runs. Workers do NOT call this. */
	void RegisterWorkerPids(const pid_t *pids, uint32 count);
	void WakeRegisteredWorkers();

	uint32_t Capacity() const { return capacity_; }

private:
	DsmTaskQueue() = default;

	/* Cap matches max bgworker fan-out we ever schedule for a single
	 * pipeline; well above pg_yaap.parallel_max_workers default. Inline
	 * array keeps the registry DSM-resident with no extra allocation. */
	static constexpr uint32_t kMaxWorkerPids = 64;

	uint32_t         capacity_;
	uint32_t         mask_;
	pid_t            worker_pids_[kMaxWorkerPids];
	uint32           worker_pid_count_;
	pg_atomic_uint64 enqueue_pos_;
	pg_atomic_uint64 dequeue_pos_;
	/* DsmTaskQueueCell cells_[capacity_] follows immediately after this
	 * struct in DSM. Access via Cells(). */

	DsmTaskQueueCell *Cells();
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
