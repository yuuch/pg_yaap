/*
 * pipeline/dsm_task_queue.cpp  (M-FRAME-MIN step 3e)
 *
 * Vyukov bounded MPMC queue. See dsm_task_queue.hpp for the full contract.
 */

#include "parallel/pipeline/dsm_task_queue.hpp"

#include <cstring>

extern "C" {
#include "storage/proc.h"
#include "storage/procarray.h"
}

namespace pg_yaap {
namespace pipeline {

#define PG_YAAP_IS_POW2(v) ((v) >= 2 && ((v) & ((v) - 1)) == 0)

size_t
DsmTaskQueue::EstimateSize(uint32_t capacity)
{
	Assert(PG_YAAP_IS_POW2(capacity));
	return sizeof(DsmTaskQueue) + (size_t) capacity * sizeof(DsmTaskQueueCell);
}

DsmTaskQueueCell *
DsmTaskQueue::Cells()
{
	return reinterpret_cast<DsmTaskQueueCell *>(this + 1);
}

DsmTaskQueue *
DsmTaskQueue::InitInPlace(void *buffer, uint32_t capacity)
{
	Assert(buffer != nullptr);
	Assert(PG_YAAP_IS_POW2(capacity));

	DsmTaskQueue *q = reinterpret_cast<DsmTaskQueue *>(buffer);
	q->capacity_ = capacity;
	q->mask_     = capacity - 1;
	q->worker_pid_count_ = 0;
	for (uint32_t i = 0; i < DsmTaskQueue::kMaxWorkerPids; i++)
		q->worker_pids_[i] = 0;
	pg_atomic_init_u64(&q->enqueue_pos_, 0);
	pg_atomic_init_u64(&q->dequeue_pos_, 0);

	DsmTaskQueueCell *cells = q->Cells();
	for (uint32_t i = 0; i < capacity; i++)
	{
		pg_atomic_init_u32(&cells[i].sequence, i);
		std::memset(&cells[i].desc, 0, sizeof(TaskDescriptor));
	}
	return q;
}

DsmTaskQueue *
DsmTaskQueue::AttachInPlace(void *buffer)
{
	Assert(buffer != nullptr);
	DsmTaskQueue *q = reinterpret_cast<DsmTaskQueue *>(buffer);
	Assert(PG_YAAP_IS_POW2(q->capacity_));
	Assert(q->mask_ == q->capacity_ - 1);
	return q;
}

void
DsmTaskQueue::RegisterWorkerPids(const pid_t *pids, uint32 count)
{
	Assert(count <= kMaxWorkerPids);
	worker_pid_count_ = count;
	for (uint32 i = 0; i < count; ++i)
		worker_pids_[i] = pids[i];
}

void
DsmTaskQueue::WakeRegisteredWorkers()
{
	for (uint32 i = 0; i < worker_pid_count_; ++i)
	{
		pid_t pid = worker_pids_[i];
		if (pid == 0)
			continue;
		/*
		 * BackendPidGetProc resolves the live PGPROC for any backend in the
		 * cluster (including bgworkers). Cross-process safe by construction:
		 * the lookup walks ProcArray by PID, so the queue does not have to
		 * cache a leader-private Latch* (which DSM cannot validly publish).
		 * Returns NULL if the worker has exited; skip silently in that case
		 * (the pop loop guarantees forward progress without that wake).
		 */
		PGPROC *p = BackendPidGetProc(pid);
		if (p != NULL)
			SetLatch(&p->procLatch);
	}
}

bool
DsmTaskQueue::TryPush(const TaskDescriptor &desc)
{
	DsmTaskQueueCell *cells = Cells();
	uint64_t          pos   = pg_atomic_read_u64(&enqueue_pos_);

	for (;;)
	{
		DsmTaskQueueCell *cell = &cells[pos & mask_];
		uint32_t          seq  = pg_atomic_read_u32(&cell->sequence);
		int64_t           diff = (int64_t) seq - (int64_t) (uint32_t) pos;

		if (diff == 0)
		{
			/*
			 * Slot is the next one we may claim. CAS the producer cursor;
			 * losers re-read and retry. Winner owns the cell exclusively
			 * until the sequence store below.
			 */
			if (pg_atomic_compare_exchange_u64(&enqueue_pos_, &pos, pos + 1))
			{
				cell->desc = desc;
				/*
				 * Release: publishes desc to consumer. Consumer waits for
				 * sequence == pos + 1.
				 */
				pg_atomic_write_u32(&cell->sequence, (uint32_t) (pos + 1));
				WakeRegisteredWorkers();
				return true;
			}
			/* CAS failure; pos has been refreshed by pg_atomic API. */
		}
		else if (diff < 0)
		{
			return false;            /* Queue full at this slot. */
		}
		else
		{
			pos = pg_atomic_read_u64(&enqueue_pos_);
		}
	}
}

bool
DsmTaskQueue::TryPop(TaskDescriptor *out)
{
	Assert(out != nullptr);

	DsmTaskQueueCell *cells = Cells();
	uint64_t          pos   = pg_atomic_read_u64(&dequeue_pos_);

	for (;;)
	{
		DsmTaskQueueCell *cell = &cells[pos & mask_];
		uint32_t          seq  = pg_atomic_read_u32(&cell->sequence);
		int64_t           diff = (int64_t) seq - (int64_t) (uint32_t) (pos + 1);

		if (diff == 0)
		{
			if (pg_atomic_compare_exchange_u64(&dequeue_pos_, &pos, pos + 1))
			{
				*out = cell->desc;
				/*
				 * Release the slot for reuse capacity_ generations later.
				 * Producer at the same wrapped index waits for sequence ==
				 * pos + capacity_.
				 */
				pg_atomic_write_u32(&cell->sequence,
				                    (uint32_t) (pos + capacity_));
				return true;
			}
		}
		else if (diff < 0)
		{
			return false;            /* Queue empty at this slot. */
		}
		else
		{
			pos = pg_atomic_read_u64(&dequeue_pos_);
		}
	}
}

bool
DsmTaskQueue::TryPopForWorker(int32_t worker_index, TaskDescriptor *out)
{
	Assert(out != nullptr);

	DsmTaskQueueCell *cells = Cells();
	uint64_t          pos   = pg_atomic_read_u64(&dequeue_pos_);

	for (;;)
	{
		DsmTaskQueueCell *cell = &cells[pos & mask_];
		uint32_t          seq  = pg_atomic_read_u32(&cell->sequence);
		int64_t           diff = (int64_t) seq - (int64_t) (uint32_t) (pos + 1);

		if (diff == 0)
		{
			/*
			 * Strict worker affinity for RUN/COMBINE: the COMBINE task at
			 * worker W consumes ProcessPipelineExecState (local TDC, local
			 * AHT) that the matching RUN at the same W produced in
			 * process-private memory. Stealing across workers crashes the
			 * backend (verified empirically — Bug O attempt #1). HOL is the
			 * only safe option here; we mitigate the WaitLatch staircase by
			 * waking all registered workers on every successful pop below
			 * (see Wake call after the cursor advance).
			 */
			if (cell->desc.worker_index != worker_index)
				return false;

			if (pg_atomic_compare_exchange_u64(&dequeue_pos_, &pos, pos + 1))
			{
				*out = cell->desc;
				pg_atomic_write_u32(&cell->sequence,
				                    (uint32_t) (pos + capacity_));
				/*
				 * KEY Bug O fix: advancing the dequeue cursor exposes the
				 * NEXT cell at the head, which may be affine to a different
				 * worker that is currently sleeping in WaitLatch. Without
				 * this wake, that worker only learns about its task when
				 * its 1000ms timeout fires — producing the 1s/2s/5s tail
				 * staircase Bug N exposed. The pop side wake is symmetric
				 * with TryPush's WakeRegisteredWorkers and closes the
				 * lost-wakeup window.
				 */
				WakeRegisteredWorkers();
				return true;
			}
		}
		else if (diff < 0)
		{
			return false;
		}
		else
		{
			pos = pg_atomic_read_u64(&dequeue_pos_);
		}
	}
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
