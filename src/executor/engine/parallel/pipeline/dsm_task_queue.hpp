#pragma once

#include <cstddef>
#include <cstdint>

extern "C" {
#include "postgres.h"
#include "port/atomics.h"
#include <sys/types.h>
}

namespace pg_yaap {
namespace pipeline {

enum class TaskKind : uint8_t { RUN = 1, COMBINE = 2, FINALIZE = 3 };

/* A published task belongs to the shared ready queue, not to an executor.
 * task_index identifies its logical partial/result slot; any idle process may
 * claim the descriptor and execute the complete task. No C++ pointer is shared. */
struct TaskDescriptor {
    uint32_t pipeline_id;
    uint32_t event_id;
    uint32_t partition_id;
    uint32_t task_index;
    uint8_t kind;
    uint8_t pad_[3];
};
static_assert(sizeof(TaskDescriptor) == 20, "TaskDescriptor must stay POD/20B");

struct DsmTaskQueueCell {
    pg_atomic_uint64 sequence;
    TaskDescriptor desc;
};

/* One global bounded ready queue. Workers publish and claim tasks. Per-cell sequence publication/reclamation
 * and cursor CAS make a claim exclusive without binding tasks to process IDs. */
class DsmTaskQueue {
public:
    /* workers only sizes the wake registry.
     * Capacity applies to the entire queue, independently of executor count. */
    static size_t EstimateSize(uint32_t capacity, uint32_t workers = 1);
    static DsmTaskQueue *InitInPlace(void *buffer, uint32_t capacity, uint32_t workers = 1);
    static DsmTaskQueue *AttachInPlace(void *buffer);

    bool TryPush(const TaskDescriptor &desc);
    bool TryPop(TaskDescriptor *out);

    /* Register ready worker PIDs before publishing tasks. There is no task-index-to-PID mapping. */
    void RegisterWorkerPids(const pid_t *pids, uint32 count);
    void WakeRegisteredWorkers();
    uint32_t Capacity() const { return capacity_; }
    uint32_t WorkerCount() const { return workers_; }

private:
    uint32_t capacity_;
    uint32_t workers_;
    uint32_t worker_pid_count_;
    pg_atomic_uint64 enqueue_pos_;
    pg_atomic_uint64 dequeue_pos_;

    pid_t *WorkerPids();
    DsmTaskQueueCell *Cells();
};

} // namespace pipeline
} // namespace pg_yaap
