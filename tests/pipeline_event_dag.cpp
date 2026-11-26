/* Exercise the production TaskScheduler + DsmTaskQueue Event DAG protocol
 * with real PG atomics across forked processes in shared memory. */
extern "C" {
#include "postgres.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/dsa.h"
#include "utils/memutils.h"
}

#include <atomic>
#include <cassert>
#include <csetjmp>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sched.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/dsm_task_queue.hpp"
#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/physical_cross_product.hpp"
#include "parallel/pipeline/physical_delim_scan.hpp"
#include "parallel/pipeline/physical_hash_join.hpp"
#include "parallel/pipeline/physical_hash_join_probe.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/pipeline_dsm_lookup.hpp"
#include "parallel/pipeline/pipeline_timeline.hpp"
#include "parallel/pipeline/task_scheduler.hpp"
#include "parallel/pipeline/worker_error.hpp"

/* ---- Minimal PostgreSQL backend stubs ---- */

sigjmp_buf *PG_exception_stack = nullptr;
ErrorContextCallback *error_context_stack = nullptr;
MemoryContext CurrentMemoryContext = nullptr;

static PGPROC g_procs[8];
static pg_atomic_uint32 *g_leader_wakes = nullptr;
static pg_atomic_uint32 *g_worker_wakes = nullptr;
static char g_last_error[256];

struct FakeDsaHeader {
    pg_atomic_uint64 offset;
    size_t capacity;
};

static char *g_dsa_base = nullptr;

extern "C" {

void *MemoryContextAlloc(MemoryContext, Size size) { return std::malloc(size ? size : 1); }
void *MemoryContextAllocZero(MemoryContext, Size size) { return std::calloc(1, size ? size : 1); }
void pfree(void *ptr) { std::free(ptr); }

dsa_pointer dsa_allocate_extended(dsa_area *, size_t size, int flags) {
    assert(g_dsa_base != nullptr);
    auto *hdr = reinterpret_cast<FakeDsaHeader *>(g_dsa_base);
    const size_t aligned = MAXALIGN(size ? size : 8);
    const uint64_t off = pg_atomic_fetch_add_u64(&hdr->offset, aligned);
    assert(off + aligned <= hdr->capacity);
    if (flags & DSA_ALLOC_ZERO)
        std::memset(g_dsa_base + off, 0, aligned);
    return static_cast<dsa_pointer>(off);
}

void *dsa_get_address(dsa_area *, dsa_pointer dp) {
    if (!DsaPointerIsValid(dp))
        return nullptr;
    return g_dsa_base + dp;
}

void dsa_trim(dsa_area *) {}

PGPROC *BackendPidGetProc(int pid) {
    if (pid == 100)
        return &g_procs[0];
    if (pid >= 101 && pid <= 107)
        return &g_procs[pid - 100];
    return nullptr;
}

void SetLatch(Latch *latch) {
    if (latch == &g_procs[0].procLatch && g_leader_wakes != nullptr)
        pg_atomic_fetch_add_u32(g_leader_wakes, 1);
    else if (g_worker_wakes != nullptr)
        pg_atomic_fetch_add_u32(g_worker_wakes, 1);
}

#undef fprintf
#undef vfprintf
#undef vsnprintf

void ExceptionalCondition(const char *cond, const char *file, int line) {
    ::fprintf(stderr, "ASSERT FAILED: %s at %s:%d\n", cond, file, line);
    std::abort();
}

int pg_vsnprintf(char *str, size_t count, const char *fmt, va_list args) {
    return ::vsnprintf(str, count, fmt, args);
}

int pg_fprintf(FILE *stream, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int r = ::vfprintf(stream, fmt, ap);
    va_end(ap);
    return r;
}

void pg_re_throw(void) {
    if (PG_exception_stack != nullptr)
        siglongjmp(*PG_exception_stack, 1);
    std::abort();
}

bool errstart_cold(int elevel, const char *) {
    g_last_error[0] = '\0';
    return elevel >= ERROR;
}

int errmsg_internal(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    ::vsnprintf(g_last_error, sizeof(g_last_error), fmt, ap);
    va_end(ap);
    return 0;
}

void errfinish(const char *, int, const char *) {
    if (PG_exception_stack != nullptr)
        siglongjmp(*PG_exception_stack, 1);
    ::fprintf(stderr, "UNCAUGHT PG ERROR: %s\n", g_last_error);
    std::abort();
}

} /* extern "C" */

namespace pg_yaap::pipeline {

void PipelineTimelineReady(PipelineSharedControl *, dsa_area *, uint32) {}
void PipelineTimelineFinished(PipelineSharedControl *, dsa_area *, uint32, bool) {}
void PhysicalHashJoinProbe::InitializeHybrid(ExecCtx &, uint32_t) {}
void PhysicalHashJoinProbe::ReleaseAfterConsumerRun(ExecCtx &) {}
void PhysicalHashJoin::ReleaseBuildPayloadAfterConsumerRun(ExecCtx &) {}
void PhysicalCrossProduct::ReleaseBuildPayloadAfterConsumerRun(ExecCtx &) {}
void PhysicalMaterialize::ReleaseAfterConsumerRun(ExecCtx &) {}

} /* namespace pg_yaap::pipeline */

using namespace pg_yaap::pipeline;

namespace {

struct SharedMockState {
    pg_atomic_uint32 leader_wakes;
    pg_atomic_uint32 worker_wakes;
    pg_atomic_uint32 run_done[64];
    pg_atomic_uint32 combine_done[64];
    pg_atomic_uint32 finalize_done[64];
    pg_atomic_uint32 task_seen[64 * 32];
    pg_atomic_uint32 total_tasks_executed;
};

struct MockGlobalSource : GlobalSourceState {};
struct MockGlobalSink : GlobalSinkState {};

class MockSourceOp final : public PhysicalOperator {
public:
    MockSourceOp(int max_threads, bool parallel)
        : PhysicalOperator(PhysicalOperatorType::SEQ_SCAN),
          max_threads_(max_threads), parallel_(parallel) {}

    bool IsSource() const override { return true; }
    bool ParallelSource() const override { return parallel_; }
    int MaxThreads(ExecCtx &) const override { return max_threads_; }
    std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &) override {
        return std::make_unique<MockGlobalSource>();
    }

private:
    int max_threads_;
    bool parallel_;
};

class MockSinkOp final : public PhysicalOperator {
public:
    MockSinkOp(int max_threads, SinkTaskPlan combine_plan, SinkTaskPlan finalize_plan,
               bool combine_trivial = false)
        : PhysicalOperator(PhysicalOperatorType::OUTPUT),
          max_threads_(max_threads),
          combine_plan_(combine_plan),
          finalize_plan_(finalize_plan),
          combine_trivial_(combine_trivial) {}

    bool IsSink() const override { return true; }
    int MaxThreads(ExecCtx &) const override { return max_threads_; }
    bool CombineIsTrivial() const override { return combine_trivial_; }
    SinkTaskPlan GetCombineTaskPlan(ExecCtx &, uint32_t) const override { return combine_plan_; }
    SinkTaskPlan GetFinalizeTaskPlan(ExecCtx &, uint32_t) const override { return finalize_plan_; }
    std::unique_ptr<GlobalSinkState> GetGlobalSinkState(ExecCtx &) override {
        return std::make_unique<MockGlobalSink>();
    }

private:
    int max_threads_;
    SinkTaskPlan combine_plan_;
    SinkTaskPlan finalize_plan_;
    bool combine_trivial_;
};

struct TestArena {
    void *mem = nullptr;
    size_t total_bytes = 0;
    PipelineSharedControl *control = nullptr;
    DsmTaskQueue *queue = nullptr;
    SharedMockState *mock = nullptr;

    TestArena(uint32_t queue_cap, int num_workers, size_t dsa_bytes = 2 * 1024 * 1024) {
        const size_t qbytes = DsmTaskQueue::EstimateSize(queue_cap, static_cast<uint32_t>(num_workers));
        total_bytes = MAXALIGN(sizeof(PipelineSharedControl)) +
                      MAXALIGN(qbytes) +
                      MAXALIGN(sizeof(SharedMockState)) +
                      dsa_bytes;
        mem = mmap(nullptr, total_bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
        assert(mem != MAP_FAILED);
        char *ptr = static_cast<char *>(mem);
        control = new (ptr) PipelineSharedControl();
        ptr += MAXALIGN(sizeof(PipelineSharedControl));
        queue = DsmTaskQueue::InitInPlace(ptr, queue_cap, static_cast<uint32_t>(num_workers));
        ptr += MAXALIGN(qbytes);
        mock = reinterpret_cast<SharedMockState *>(ptr);
        std::memset(mock, 0, sizeof(SharedMockState));
        ptr += MAXALIGN(sizeof(SharedMockState));
        g_dsa_base = ptr;
        auto *hdr = reinterpret_cast<FakeDsaHeader *>(g_dsa_base);
        pg_atomic_init_u64(&hdr->offset, MAXALIGN(sizeof(FakeDsaHeader)));
        hdr->capacity = dsa_bytes;

        g_leader_wakes = &mock->leader_wakes;
        g_worker_wakes = &mock->worker_wakes;
        control->leader_pid = 100;
        control->num_workers = num_workers;
        pg_atomic_init_u32(&control->shutdown_requested, 0);
        pg_atomic_init_u32(&control->worker_error, 0);
        pid_t pids[7] = {101, 102, 103, 104, 105, 106, 107};
        queue->RegisterWorkerPids(pids, static_cast<uint32_t>(num_workers));
    }

    ~TestArena() {
        g_dsa_base = nullptr;
        g_leader_wakes = nullptr;
        g_worker_wakes = nullptr;
        munmap(mem, total_bytes);
    }
};

static bool ExpectBuildError(const MetaPipelineBundle &bundle, ExecCtx &ctx, const char *needle) {
    bool caught = false;
    PG_TRY();
    {
        TaskScheduler::BuildEvents(bundle, ctx);
    }
    PG_CATCH();
    {
        caught = true;
    }
    PG_END_TRY();
    return caught && std::strstr(g_last_error, needle) != nullptr;
}

static void WaitChildOk(pid_t child) {
    int status = 0;
    assert(waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

static void TestValidationErrors() {
    TestArena arena(4, 2);
    ExecCtx ctx{nullptr, nullptr, LEADER_WORKER_INDEX, arena.control, INVALID_EVENT_ID};

    MockSourceOp src(2, true);
    MockSinkOp sink0(2, {0, 0, false}, {1, 1, false}, true);
    MockSinkOp sink1(2, {0, 0, false}, {1, 1, false}, true);

    /* 1. Cycle detection (0 -> 1 -> 0). */
    {
        MetaPipelineBundle b;
        auto p0 = std::make_unique<Pipeline>();
        p0->id = 0; p0->source = &src; p0->sink = &sink0; p0->depends_on.push_back(1);
        auto p1 = std::make_unique<Pipeline>();
        p1->id = 1; p1->source = &src; p1->sink = &sink1; p1->depends_on.push_back(0);
        b.pipelines.push_back(std::move(p0));
        b.pipelines.push_back(std::move(p1));
        assert(ExpectBuildError(b, ctx, "cycle"));
    }

    /* 2. Out-of-range dependency. */
    {
        MetaPipelineBundle b;
        auto p0 = std::make_unique<Pipeline>();
        p0->id = 0; p0->source = &src; p0->sink = &sink0; p0->depends_on.push_back(9);
        b.pipelines.push_back(std::move(p0));
        assert(ExpectBuildError(b, ctx, "invalid pipeline dependency"));
    }

    /* 3. Duplicate sink operator shared by two pipelines. */
    {
        MetaPipelineBundle b;
        auto p0 = std::make_unique<Pipeline>();
        p0->id = 0; p0->source = &src; p0->sink = &sink0;
        auto p1 = std::make_unique<Pipeline>();
        p1->id = 1; p1->source = &src; p1->sink = &sink0;
        b.pipelines.push_back(std::move(p0));
        b.pipelines.push_back(std::move(p1));
        assert(ExpectBuildError(b, ctx, "shared by pipelines"));
    }
}

static void TestDeepZeroTaskChain() {
    constexpr uint32_t kDepth = 20;
    TestArena arena(4, 1);
    ExecCtx leader_ctx{nullptr, nullptr, LEADER_WORKER_INDEX, arena.control, INVALID_EVENT_ID};
    MetaPipelineBundle b;
    std::vector<std::unique_ptr<MockSourceOp>> srcs;
    std::vector<std::unique_ptr<MockSinkOp>> sinks;
    PipelineDsmLookup<Pipeline> lookup(nullptr);
    for (uint32_t i = 0; i < kDepth; ++i) {
        srcs.push_back(std::make_unique<MockSourceOp>(1, false));
        /* Zero combine and zero finalize tasks -> tests iterative zero-task completion. */
        sinks.push_back(std::make_unique<MockSinkOp>(1, SinkTaskPlan{0, 0, false}, SinkTaskPlan{0, 0, false}, true));
        auto p = std::make_unique<Pipeline>();
        p->id = i;
        p->source = srcs.back().get();
        p->sink = sinks.back().get();
        if (i > 0) {
            /* Duplicate dependency edge must be coalesced to indegree 1. */
            p->depends_on.push_back(i - 1);
            p->depends_on.push_back(i - 1);
        }
        lookup.Register(i, p.get());
        b.pipelines.push_back(std::move(p));
    }
    TaskScheduler::BuildEvents(b, leader_ctx);
    auto *events = static_cast<EventShmState *>(dsa_get_address(nullptr, arena.control->event_states_root));
    assert(events[3].dependencies_remaining == 1); /* Coalesced duplicate edge. */

    pg_atomic_write_membarrier_u32(&arena.control->scheduler_started, 1);
    ExecCtx worker_ctx{nullptr, nullptr, 0, arena.control, INVALID_EVENT_ID};
    TaskScheduler scheduler(worker_ctx, *arena.queue, lookup);

    uint32_t completed_runs = 0;
    while (pg_atomic_read_membarrier_u32(&arena.control->events_remaining) > 0) {
        scheduler.Advance();
        TaskDescriptor desc{};
        if (arena.queue->TryPop(&desc)) {
            assert(static_cast<TaskKind>(desc.kind) == TaskKind::RUN);
            assert(desc.pipeline_id == completed_runs);
            ++completed_runs;
            scheduler.CompleteTask(desc.event_id);
        }
    }
    assert(completed_runs == kDepth);
    assert(pg_atomic_read_u32(&arena.mock->leader_wakes) == 1);
}

static void TestDiamondAndSmallQueueMultiprocess() {
    /* Diamond + fan-in/fan-out with 4 forked workers and a tiny 2-slot queue!
     *   P0 (root, 4 RUN, 8 partitioned COMBINE, 4 partitioned FINALIZE)
     *   P1 (depends on P0, 4 RUN, 0 COMBINE, 1 FINALIZE)
     *   P2 (depends on P0, 4 RUN, 4 COMBINE, 1 FINALIZE)
     *   P3 (fan-in depends on P1 and P2, 4 RUN, 0 COMBINE, 1 FINALIZE)
     */
    constexpr int kWorkers = 4;
    TestArena arena(2 /* tiny queue capacity < task batch! */, kWorkers);
    ExecCtx leader_ctx{nullptr, nullptr, LEADER_WORKER_INDEX, arena.control, INVALID_EVENT_ID};

    MetaPipelineBundle b;
    MockSourceOp src(kWorkers, true);
    MockSinkOp sink0(kWorkers, {8, 4, true}, {4, 4, true}, false);
    MockSinkOp sink1(kWorkers, {0, 0, false}, {1, 1, false}, true);
    MockSinkOp sink2(kWorkers, {4, 4, true}, {1, 1, false}, false);
    MockSinkOp sink3(kWorkers, {0, 0, false}, {1, 1, false}, true);

    auto make_pipe = [&](PipelineId id, MockSinkOp &sink, std::initializer_list<PipelineId> deps) {
        auto p = std::make_unique<Pipeline>();
        p->id = id;
        p->source = &src;
        p->sink = &sink;
        for (PipelineId d : deps) p->depends_on.push_back(d);
        return p;
    };
    b.pipelines.push_back(make_pipe(0, sink0, {}));
    b.pipelines.push_back(make_pipe(1, sink1, {0}));
    b.pipelines.push_back(make_pipe(2, sink2, {0}));
    b.pipelines.push_back(make_pipe(3, sink3, {1, 2}));

    PipelineDsmLookup<Pipeline> lookup(nullptr);
    for (auto &p : b.pipelines)
        lookup.Register(p->id, p.get());

    TaskScheduler::BuildEvents(b, leader_ctx);
    pg_atomic_write_membarrier_u32(&arena.control->scheduler_started, 1);

    pid_t kids[kWorkers];
    for (int w = 0; w < kWorkers; ++w) {
        kids[w] = fork();
        assert(kids[w] >= 0);
        if (kids[w] == 0) {
            ExecCtx wctx{nullptr, nullptr, w, arena.control, INVALID_EVENT_ID};
            TaskScheduler sched(wctx, *arena.queue, lookup);
            while (pg_atomic_read_membarrier_u32(&arena.control->events_remaining) > 0) {
                const bool adv = sched.Advance();
                TaskDescriptor desc{};
                if (!arena.queue->TryPop(&desc)) {
                    if (adv || sched.Advance())
                        continue;
                    sched_yield();
                    continue;
                }
                const uint32_t pid = desc.pipeline_id;
                const uint32_t slot = desc.event_id * 32 + desc.task_index;
                assert(slot < 64 * 32);
                uint32_t exp = 0;
                assert(pg_atomic_compare_exchange_u32(&arena.mock->task_seen[slot], &exp, 1));

                /* Verify upstream dependency ordering strictly held. */
                if (static_cast<TaskKind>(desc.kind) == TaskKind::RUN) {
                    if (pid == 1 || pid == 2)
                        assert(pg_atomic_read_membarrier_u32(&arena.mock->finalize_done[0]) == 4);
                    if (pid == 3) {
                        assert(pg_atomic_read_membarrier_u32(&arena.mock->finalize_done[1]) == 1);
                        assert(pg_atomic_read_membarrier_u32(&arena.mock->finalize_done[2]) == 1);
                    }
                    pg_atomic_fetch_add_u32(&arena.mock->run_done[pid], 1);
                } else if (static_cast<TaskKind>(desc.kind) == TaskKind::COMBINE) {
                    assert(pg_atomic_read_membarrier_u32(&arena.mock->run_done[pid]) == kWorkers);
                    pg_atomic_fetch_add_u32(&arena.mock->combine_done[pid], 1);
                } else if (static_cast<TaskKind>(desc.kind) == TaskKind::FINALIZE) {
                    assert(pg_atomic_read_membarrier_u32(&arena.mock->run_done[pid]) == kWorkers);
                    if (pid == 0)
                        assert(pg_atomic_read_membarrier_u32(&arena.mock->combine_done[0]) == 8);
                    if (pid == 2)
                        assert(pg_atomic_read_membarrier_u32(&arena.mock->combine_done[2]) == 4);
                    pg_atomic_fetch_add_u32(&arena.mock->finalize_done[pid], 1);
                }
                pg_atomic_fetch_add_u32(&arena.mock->total_tasks_executed, 1);
                sched.CompleteTask(desc.event_id);
            }
            _exit(0);
        }
    }
    for (pid_t kid : kids)
        WaitChildOk(kid);

    /* Expected total tasks:
     * P0: 4 RUN + 8 COMBINE + 4 FINALIZE = 16
     * P1: 4 RUN + 0 COMBINE + 1 FINALIZE = 5
     * P2: 4 RUN + 4 COMBINE + 1 FINALIZE = 9
     * P3: 4 RUN + 0 COMBINE + 1 FINALIZE = 5
     * Total = 35. */
    assert(pg_atomic_read_u32(&arena.mock->total_tasks_executed) == 35);
    assert(pg_atomic_read_u32(&arena.control->events_remaining) == 0);
    assert(pg_atomic_read_u32(&arena.mock->leader_wakes) == 1);

    auto *events = static_cast<EventShmState *>(dsa_get_address(nullptr, arena.control->event_states_root));
    for (EventId id = 0; id < arena.control->event_count; ++id) {
        assert(events[id].phase == SharedEventPhase::FINISHED);
        assert(events[id].activated_by >= 0 && events[id].completed_by >= 0);
    }
}

static void TestAbortBlocksSuccessorRelease() {
    TestArena arena(4, 2);
    ExecCtx leader_ctx{nullptr, nullptr, LEADER_WORKER_INDEX, arena.control, INVALID_EVENT_ID};
    MetaPipelineBundle b;
    MockSourceOp src(1, false);
    MockSinkOp sink0(1, {0, 0, false}, {1, 1, false}, true);
    MockSinkOp sink1(1, {0, 0, false}, {1, 1, false}, true);
    auto p0 = std::make_unique<Pipeline>();
    p0->id = 0; p0->source = &src; p0->sink = &sink0;
    auto p1 = std::make_unique<Pipeline>();
    p1->id = 1; p1->source = &src; p1->sink = &sink1; p1->depends_on.push_back(0);
    PipelineDsmLookup<Pipeline> lookup(nullptr);
    lookup.Register(0, p0.get());
    lookup.Register(1, p1.get());
    b.pipelines.push_back(std::move(p0));
    b.pipelines.push_back(std::move(p1));

    TaskScheduler::BuildEvents(b, leader_ctx);
    pg_atomic_write_membarrier_u32(&arena.control->scheduler_started, 1);

    ExecCtx wctx{nullptr, nullptr, 0, arena.control, INVALID_EVENT_ID};
    TaskScheduler sched(wctx, *arena.queue, lookup);
    assert(sched.Advance());
    TaskDescriptor desc{};
    assert(arena.queue->TryPop(&desc) && desc.pipeline_id == 0);

    /* Abort before completing P0's RUN; successor P1 must never become ready. */
    ReportWorkerError(arena.control, "injected abort");
    assert(!sched.Advance());
    auto *events = static_cast<EventShmState *>(dsa_get_address(nullptr, arena.control->event_states_root));
    assert(events[3].phase == SharedEventPhase::WAITING);
    assert(events[3].dependencies_remaining == 1);
    assert(pg_atomic_read_u32(&arena.control->events_remaining) == 6);
}

} /* namespace */

int main() {
    alarm(30);
    TestValidationErrors();
    TestDeepZeroTaskChain();
    TestDiamondAndSmallQueueMultiprocess();
    TestAbortBlocksSuccessorRelease();
    return 0;
}
