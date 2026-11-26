/* Exercise the production global DSM queue with actual PG atomics and forked
 * consumers. Only backend latch/error entry points are stubbed. */
extern "C" {
#include "postgres.h"
#include "storage/proc.h"
#include "storage/procarray.h"
}
#include "parallel/pipeline/dsm_task_queue.hpp"
#include <cassert>
#include <cstdlib>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sched.h>

static PGPROC processes[3];
static int wake_count[3];
extern "C" PGPROC *BackendPidGetProc(int pid) {
    return pid >= 11 && pid <= 13 ? &processes[pid - 11] : nullptr;
}
extern "C" void SetLatch(Latch *latch) {
    for (int i = 0; i < 3; ++i)
        if (latch == &processes[i].procLatch) ++wake_count[i];
}
extern "C" void ExceptionalCondition(const char *, const char *, int) { abort(); }
extern "C" bool errstart_cold(int, const char *) { abort(); }
extern "C" int errmsg_internal(const char *, ...) { abort(); }
extern "C" void errfinish(const char *, int, const char *) { abort(); }

using namespace pg_yaap::pipeline;

static TaskDescriptor descriptor(uint32_t slot, uint32_t n)
{
    TaskDescriptor d{};
    d.task_index = slot;
    d.pipeline_id = n;
    d.event_id = n ^ 0xa5a5a5a5u;
    d.partition_id = ~n;
    d.kind = static_cast<uint8_t>(TaskKind::RUN);
    return d;
}

static void wait_ok(pid_t child)
{
    int status;
    assert(waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

int main()
{
    alarm(30);
    const size_t bytes = DsmTaskQueue::EstimateSize(2, 2);
    void *mem = mmap(nullptr, bytes, PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_ANON, -1, 0);
    assert(mem != MAP_FAILED);
    auto *q = DsmTaskQueue::InitInPlace(mem, 2, 2);
    TaskDescriptor out{};
    const pid_t pids[] = {11, 12};
    q->RegisterWorkerPids(pids, 2);
    assert(q->TryPush(descriptor(9000, 9)));
    // A published task may be claimed by any idle executor, not its slot's PID.
    assert(wake_count[0] == 1 && wake_count[1] == 1 && wake_count[2] == 0);
    assert(q->TryPop(&out) && out.pipeline_id == 9 && out.task_index == 9000);
    assert(wake_count[0] == 1 && wake_count[1] == 1 && wake_count[2] == 0);
    const pid_t no_pids[] = {0, 0};
    q->RegisterWorkerPids(no_pids, 2);

    assert(!q->TryPop(&out));
    assert(q->TryPush(descriptor(0, 1)));
    assert(q->TryPush(descriptor(127, 2)));
    assert(!q->TryPush(descriptor(9, 3))); // Capacity is global, not per worker.
    // Only one consumer is available. It can claim both differently indexed
    // tasks while every other worker is absent/busy; no owner can block it.
    pid_t idle_consumer = fork();
    assert(idle_consumer >= 0);
    if (idle_consumer == 0) {
        auto *attached = DsmTaskQueue::AttachInPlace(mem);
        assert(attached->TryPop(&out) && out.pipeline_id == 1 && out.task_index == 0);
        assert(attached->TryPop(&out) && out.pipeline_id == 2 && out.task_index == 127);
        assert(!attached->TryPop(&out));
        _exit(0);
    }
    wait_ok(idle_consumer);
    assert(q->TryPush(descriptor(99, 3))); // Released cells can be reused.
    assert(q->TryPop(&out) && out.pipeline_id == 3);

    constexpr uint32_t count = 600000;
    struct Stats {
        pg_atomic_uint32 consumed;
        pg_atomic_uint64 checksum;
    };
    const size_t stats_bytes = MAXALIGN(sizeof(Stats)) + sizeof(pg_atomic_uint32) * count;
    auto *stats = static_cast<Stats *>(mmap(nullptr, stats_bytes,
        PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    assert(stats != MAP_FAILED);
    auto *seen = reinterpret_cast<pg_atomic_uint32 *>(
        reinterpret_cast<char *>(stats) + MAXALIGN(sizeof(Stats)));
    pg_atomic_init_u32(&stats->consumed, 0);
    pg_atomic_init_u64(&stats->checksum, 0);
    for (uint32_t i = 0; i < count; ++i) pg_atomic_init_u32(&seen[i], 0);

    pid_t children[3];
    for (int consumer = 0; consumer < 3; ++consumer) {
        children[consumer] = fork();
        assert(children[consumer] >= 0);
        if (children[consumer] == 0) {
            auto *attached = DsmTaskQueue::AttachInPlace(mem);
            for (;;) {
                while (!attached->TryPop(&out)) sched_yield();
                if (out.pipeline_id == UINT32_MAX) break;
                const uint32_t i = out.pipeline_id;
                assert(i < count && out.task_index == i % 257);
                assert(out.event_id == (i ^ 0xa5a5a5a5u));
                assert(out.partition_id == ~i && out.kind == uint8_t(TaskKind::RUN));
                uint32_t expected = 0;
                assert(pg_atomic_compare_exchange_u32(&seen[i], &expected, 1));
                pg_atomic_fetch_add_u32(&stats->consumed, 1);
                pg_atomic_fetch_add_u64(&stats->checksum, i);
            }
            _exit(0);
        }
    }
    for (uint32_t i = 0; i < count; ++i)
        while (!q->TryPush(descriptor(i % 257, i))) sched_yield();
    for (int i = 0; i < 3; ++i)
        while (!q->TryPush(descriptor(0, UINT32_MAX))) sched_yield();
    for (pid_t child : children) wait_ok(child);
    assert(pg_atomic_read_u32(&stats->consumed) == count);
    assert(pg_atomic_read_u64(&stats->checksum) == static_cast<uint64_t>(count) * (count - 1) / 2);
    for (uint32_t i = 0; i < count; ++i) assert(pg_atomic_read_u32(&seen[i]) == 1);
    assert(munmap(stats, stats_bytes) == 0);
    assert(munmap(mem, bytes) == 0);

    // Dynamic notification storage supports more than 64 executor PIDs.
    const size_t many_bytes = DsmTaskQueue::EstimateSize(2, 128);
    mem = mmap(nullptr, many_bytes, PROT_READ | PROT_WRITE,
               MAP_SHARED | MAP_ANON, -1, 0);
    assert(mem != MAP_FAILED);
    q = DsmTaskQueue::InitInPlace(mem, 2, 128);
    pid_t many_pids[128]{};
    many_pids[127] = 13;
    const int before_wake = wake_count[2];
    q->RegisterWorkerPids(many_pids, 128);
    assert(q->TryPush(descriptor(1000000, 1)));
    assert(wake_count[2] == before_wake + 1);
    assert(q->TryPop(&out) && out.pipeline_id == 1 && out.task_index == 1000000);
    assert(wake_count[2] == before_wake + 1);
    assert(munmap(mem, many_bytes) == 0);
    return 0;
}
