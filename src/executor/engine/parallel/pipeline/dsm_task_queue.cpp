#include "parallel/pipeline/dsm_task_queue.hpp"

extern "C" {
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procarray.h"
}

namespace pg_yaap {
namespace pipeline {

static void WakeProcess(pid_t pid)
{
    if (pid == 0) return;
    PGPROC *proc = BackendPidGetProc(pid);
    if (proc != nullptr) SetLatch(&proc->procLatch);
}

size_t DsmTaskQueue::EstimateSize(uint32_t capacity, uint32_t workers)
{
    Assert(capacity >= 2 && (capacity & (capacity - 1)) == 0);
    return MAXALIGN(sizeof(DsmTaskQueue)) +
        MAXALIGN(sizeof(pid_t) * static_cast<size_t>(workers)) +
        sizeof(DsmTaskQueueCell) * capacity;
}

pid_t *DsmTaskQueue::WorkerPids()
{
    return reinterpret_cast<pid_t *>(
        reinterpret_cast<char *>(this) + MAXALIGN(sizeof(DsmTaskQueue)));
}

DsmTaskQueueCell *DsmTaskQueue::Cells()
{
    return reinterpret_cast<DsmTaskQueueCell *>(
        reinterpret_cast<char *>(WorkerPids()) +
        MAXALIGN(sizeof(pid_t) * static_cast<size_t>(workers_)));
}

DsmTaskQueue *DsmTaskQueue::InitInPlace(void *buffer, uint32_t capacity, uint32_t workers)
{
    Assert(buffer != nullptr);
    Assert(capacity >= 2 && (capacity & (capacity - 1)) == 0);
    auto *q = static_cast<DsmTaskQueue *>(buffer);
    q->capacity_ = capacity;
    q->workers_ = workers;
    q->worker_pid_count_ = 0;
    pg_atomic_init_u64(&q->enqueue_pos_, 0);
    pg_atomic_init_u64(&q->dequeue_pos_, 0);
    for (uint32_t i = 0; i < capacity; ++i)
        pg_atomic_init_u64(&q->Cells()[i].sequence, i);
    for (uint32_t i = 0; i < workers; ++i)
        q->WorkerPids()[i] = 0;
    return q;
}

DsmTaskQueue *DsmTaskQueue::AttachInPlace(void *buffer)
{
    Assert(buffer != nullptr);
    auto *q = static_cast<DsmTaskQueue *>(buffer);
    Assert(q->capacity_ >= 2 && (q->capacity_ & (q->capacity_ - 1)) == 0);
    return q;
}

void DsmTaskQueue::RegisterWorkerPids(const pid_t *pids, uint32 count)
{
    Assert(count > 0 && count <= workers_);
    worker_pid_count_ = count;
    for (uint32_t i = 0; i < count; ++i)
        WorkerPids()[i] = pids[i];
}

void DsmTaskQueue::WakeRegisteredWorkers()
{
    for (uint32_t i = 0; i < worker_pid_count_; ++i)
        WakeProcess(WorkerPids()[i]);
}

bool DsmTaskQueue::TryPush(const TaskDescriptor &desc)
{
    uint64_t pos = pg_atomic_read_u64(&enqueue_pos_);
    for (;;) {
        DsmTaskQueueCell &cell = Cells()[pos & (capacity_ - 1)];
        const uint64_t sequence = pg_atomic_read_membarrier_u64(&cell.sequence);
        const int64_t difference = static_cast<int64_t>(sequence - pos);
        if (difference == 0) {
            if (pg_atomic_compare_exchange_u64(&enqueue_pos_, &pos, pos + 1)) {
                cell.desc = desc;
                /* Publish only after the complete non-atomic descriptor.
                 * Ordinary pg_atomic_write_u64 has no release semantics. */
                pg_atomic_write_membarrier_u64(&cell.sequence, pos + 1);
                // Every idle executor is eligible; task_index is not affinity.
                WakeRegisteredWorkers();
                return true;
            }
        } else if (difference < 0) {
            return false;
        } else {
            pos = pg_atomic_read_u64(&enqueue_pos_);
        }
    }
}

bool DsmTaskQueue::TryPop(TaskDescriptor *out)
{
    Assert(out != nullptr);
    uint64_t pos = pg_atomic_read_u64(&dequeue_pos_);
    for (;;) {
        DsmTaskQueueCell &cell = Cells()[pos & (capacity_ - 1)];
        const uint64_t sequence = pg_atomic_read_membarrier_u64(&cell.sequence);
        const int64_t difference = static_cast<int64_t>(sequence - (pos + 1));
        if (difference == 0) {
            if (pg_atomic_compare_exchange_u64(&dequeue_pos_, &pos, pos + 1)) {
                *out = cell.desc;
                /* Reclaim after every descriptor read, so a producer cannot
                 * overwrite a slot while its consumer still copies it. */
                pg_atomic_write_membarrier_u64(&cell.sequence, pos + capacity_);
                return true;
            }
        } else if (difference < 0) {
            return false;
        } else {
            pos = pg_atomic_read_u64(&dequeue_pos_);
        }
    }
}

} // namespace pipeline
} // namespace pg_yaap
