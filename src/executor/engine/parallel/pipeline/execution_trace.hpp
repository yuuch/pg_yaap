#pragma once

/*
 * parallel/pipeline/execution_trace.hpp
 *
 * Layer 1 Execution Trace infrastructure for pg_yaap.
 *
 * Design principles:
 *   - Per-process, single-writer, lock-free append-only buffers in query DSA.
 *   - Zero dynamic allocation, zero locks, zero string formatting, zero I/O
 *     on the event recording hot path.
 *   - Disabled fast path is a single process-local pointer check.
 *   - Shared monotonic clock origin initialized once by the leader.
 *   - Offline collection by the leader after all workers have stopped,
 *     streamed directly to Chrome Trace Event JSON (Perfetto UI compatible).
 *   - Scope-based Begin/End API designed to extend cleanly for Layer 2
 *     Resource Accounting (CPU time, off-CPU time, I/O bytes, etc.).
 */

#include <cstdint>
#include <ctime>

extern "C" {
#include "postgres.h"
#include "port/atomics.h"
#include "utils/dsa.h"
}

#include "parallel/pipeline/types.hpp"

namespace pg_yaap {
namespace pipeline {

struct PipelineSharedControl;

static constexpr uint32_t YAAP_TRACE_FORMAT_VERSION = 1u;
static constexpr uint32_t YAAP_TRACE_INVALID_ID     = UINT32_MAX;

/* Event categories (extensible for Layer 2 Resource Accounting). */
enum class YaapTraceCategory : uint32_t {
	EXECUTION = 0,
	SCHEDULER = 1,
	/* Reserved for future Layer 2 categories:
	 * RESOURCE = 2,
	 * IO       = 3,
	 * LOCK     = 4,
	 */
};

/* Layer 1 execution trace event types grounded in the Event DAG runtime. */
enum class YaapTraceEventType : uint32_t {
	QUERY_EXECUTE     = 1,   /* Leader: overall query execution interval (ph: X) */
	OUTPUT_EMIT       = 2,   /* Leader: OutputSink emit to client (ph: X) */
	LEADER_WAIT       = 3,   /* Leader: WaitLatch for worker ready or DAG completion (ph: X) */
	WORKER_START      = 4,   /* Worker: attached and ready to run tasks (ph: i) */
	WORKER_EXIT       = 5,   /* Worker: leaving main execution loop (ph: i) */
	WORKER_IDLE       = 6,   /* Worker: WaitLatch waiting for ready tasks (ph: X) */
	PIPELINE_READY    = 7,   /* Scheduler: pipeline RUN dependencies satisfied (ph: i) */
	PIPELINE_SCHEDULE = 8,   /* Scheduler: pipeline RUN tasks scheduled (ph: i) */
	PIPELINE_FINISH   = 9,   /* Scheduler: pipeline FINALIZE completed (ph: i) */
	EVENT_SCHEDULE    = 10,  /* Scheduler: Event DAG node activated (ph: i) */
	EVENT_FINISH      = 11,  /* Scheduler: Event DAG node finished (ph: i) */
	TASK_ENQUEUE      = 12,  /* Scheduler: TaskDescriptor pushed to DsmTaskQueue (ph: i) */
	TASK_DEQUEUE      = 13,  /* Worker: TaskDescriptor popped from DsmTaskQueue (ph: i) */
	TASK_EXECUTE      = 14,  /* Worker: actual execution interval of one task (ph: X) */
};

/* Task execution outcome packed into YaapTraceEvent::arg. */
enum class YaapTraceTaskOutcome : uint8_t {
	OK        = 0,
	FAILED    = 1,
	CANCELLED = 2,
};

/* Leader wait subtype packed into LEADER_WAIT arg. */
enum class YaapTraceLeaderWaitKind : uint64_t {
	WORKER_READY = 0,
	EVENT_DAG    = 1,
};

/* Compact helpers for packing/unpacking YaapTraceEvent::arg without strings. */
inline constexpr uint64_t
YaapTracePackTaskArg(uint32_t event_id,
                     uint8_t task_kind,
                     uint32_t partition_id = YAAP_TRACE_INVALID_ID,
                     YaapTraceTaskOutcome outcome = YaapTraceTaskOutcome::OK) noexcept
{
	return (static_cast<uint64_t>(partition_id) << 32) |
		(static_cast<uint64_t>(static_cast<uint8_t>(outcome) & 0x0Fu) << 28) |
		(static_cast<uint64_t>(task_kind & 0x0Fu) << 24) |
		static_cast<uint64_t>(event_id & 0x00FFFFFFu);
}

inline constexpr uint32_t
YaapTraceUnpackTaskEventId(uint64_t arg) noexcept
{
	return static_cast<uint32_t>(arg & 0x00FFFFFFu);
}

inline constexpr uint8_t
YaapTraceUnpackTaskKind(uint64_t arg) noexcept
{
	return static_cast<uint8_t>((arg >> 24) & 0x0Fu);
}

inline constexpr YaapTraceTaskOutcome
YaapTraceUnpackTaskOutcome(uint64_t arg) noexcept
{
	return static_cast<YaapTraceTaskOutcome>((arg >> 28) & 0x0Fu);
}

inline constexpr uint32_t
YaapTraceUnpackTaskPartitionId(uint64_t arg) noexcept
{
	return static_cast<uint32_t>(arg >> 32);
}

inline constexpr uint64_t
YaapTracePackEventArg(uint8_t task_kind, uint32_t task_count) noexcept
{
	return (static_cast<uint64_t>(task_count) << 32) |
		static_cast<uint64_t>(task_kind & 0xFFu);
}

inline constexpr uint8_t
YaapTraceUnpackEventKind(uint64_t arg) noexcept
{
	return static_cast<uint8_t>(arg & 0xFFu);
}

inline constexpr uint32_t
YaapTraceUnpackEventTaskCount(uint64_t arg) noexcept
{
	return static_cast<uint32_t>(arg >> 32);
}

/*
 * Compact shared-memory Trace Event record (POD, no strings, no pointers).
 * All timestamps are in nanoseconds relative to the leader's query origin.
 */
struct YaapTraceEvent {
	uint64_t timestamp_ns;
	uint64_t duration_ns;
	uint32_t event_type;
	uint32_t category;
	uint32_t pipeline_id;
	uint32_t task_id;
	uint64_t arg;
};
static_assert(sizeof(YaapTraceEvent) == 40, "YaapTraceEvent must remain 40 bytes");
static_assert(alignof(YaapTraceEvent) == 8, "YaapTraceEvent must be 8-byte aligned");

/* Lifecycle state of a per-process Trace Buffer. */
enum class YaapTraceBufferState : uint32_t {
	UNUSED    = 0,
	ACTIVE    = 1,
	FINALIZED = 2,
	ABORTED   = 3,
};

/*
 * Per-process Trace Buffer header stored in query DSA.
 * Cache-line aligned so each process writes exclusively to its own cache line.
 */
struct alignas(PG_CACHE_LINE_SIZE) YaapTraceBufferHeader {
	uint32_t         format_version;
	uint32_t         state;          /* YaapTraceBufferState */
	uint64_t         execution_id;
	int32_t          worker_id;      /* LEADER_WORKER_INDEX (-1) or 0..N-1 */
	int32_t          pid;            /* OS process ID */
	uint32_t         capacity;       /* Maximum YaapTraceEvent entries */
	uint32_t         _pad0;
	pg_atomic_uint64 event_count;    /* Published event count [0..capacity] */
	pg_atomic_uint64 dropped_count;  /* Events dropped when buffer full */
};
static_assert(sizeof(YaapTraceBufferHeader) % PG_CACHE_LINE_SIZE == 0,
              "YaapTraceBufferHeader must be a multiple of PG_CACHE_LINE_SIZE");

/*
 * Monotonic time capture interface.
 * Encapsulated here so Layer 2 can extend YaapTraceTimePoint with CPU time
 * without altering call sites.
 */
struct YaapTraceTimePoint {
	uint64_t monotonic_ns;
	/* Layer 2 extension point: uint64_t cpu_ns; */
};

inline uint64_t
YaapTraceNow() noexcept
{
	struct timespec ts;
	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
		return 0;
	return static_cast<uint64_t>(ts.tv_sec) * UINT64_C(1000000000) +
		static_cast<uint64_t>(ts.tv_nsec);
}

inline YaapTraceTimePoint
YaapTraceCaptureTime() noexcept
{
	return YaapTraceTimePoint{YaapTraceNow()};
}

/*
 * Process-local handle to the currently attached per-process Trace Buffer.
 * Null whenever tracing is disabled or detached, making YaapTraceIsEnabled()
 * a single null-pointer check on the hot path.
 */
struct YaapTraceLocalState {
	YaapTraceBufferHeader *header = nullptr;
	YaapTraceEvent        *events = nullptr;
	uint64_t               origin_ns = 0;
	uint64_t               event_count = 0;
	uint64_t               dropped_count = 0;
	uint32_t               capacity = 0;
};

extern YaapTraceLocalState *g_yaap_trace_local;

inline bool
YaapTraceIsEnabled() noexcept
{
	return g_yaap_trace_local != nullptr;
}

/*
 * Explicit scope context for Begin/End tracing.
 * Supports arbitrary nesting on the stack and future Layer 2 resource fields
 * (wall_time_ns, cpu_time_ns, off_cpu_time_ns) without rewriting call sites.
 */
struct YaapTraceScope {
	YaapTraceTimePoint begin_time{0};
	uint32_t           event_type = 0;
	uint32_t           category = 0;
	uint32_t           pipeline_id = YAAP_TRACE_INVALID_ID;
	uint32_t           task_id = YAAP_TRACE_INVALID_ID;
	uint64_t           arg = 0;
	bool               active = false;
};

/* Core recording primitives (called only when YaapTraceIsEnabled() is true). */
void YaapTraceRecordInstantImpl(YaapTraceEventType event_type,
                                YaapTraceCategory category,
                                uint32_t pipeline_id,
                                uint32_t task_id,
                                uint64_t arg) noexcept;

void YaapTraceRecordDurationImpl(YaapTraceEventType event_type,
                                 YaapTraceCategory category,
                                 uint32_t pipeline_id,
                                 uint32_t task_id,
                                 uint64_t begin_ns,
                                 uint64_t end_ns,
                                 uint64_t arg) noexcept;

inline void
YaapTraceInstant(YaapTraceEventType event_type,
                 YaapTraceCategory category = YaapTraceCategory::EXECUTION,
                 uint32_t pipeline_id = YAAP_TRACE_INVALID_ID,
                 uint32_t task_id = YAAP_TRACE_INVALID_ID,
                 uint64_t arg = 0) noexcept
{
	if (unlikely(YaapTraceIsEnabled()))
		YaapTraceRecordInstantImpl(event_type, category, pipeline_id, task_id, arg);
}

inline void
YaapTraceDuration(YaapTraceEventType event_type,
                  YaapTraceCategory category,
                  uint32_t pipeline_id,
                  uint32_t task_id,
                  uint64_t begin_ns,
                  uint64_t end_ns,
                  uint64_t arg = 0) noexcept
{
	if (unlikely(YaapTraceIsEnabled()))
		YaapTraceRecordDurationImpl(event_type, category, pipeline_id, task_id,
		                            begin_ns, end_ns, arg);
}

inline YaapTraceScope
YaapTraceBegin(YaapTraceEventType event_type,
               YaapTraceCategory category = YaapTraceCategory::EXECUTION,
               uint32_t pipeline_id = YAAP_TRACE_INVALID_ID,
               uint32_t task_id = YAAP_TRACE_INVALID_ID,
               uint64_t arg = 0) noexcept
{
	YaapTraceScope scope{};
	if (unlikely(YaapTraceIsEnabled()))
	{
		scope.begin_time = YaapTraceCaptureTime();
		scope.event_type = static_cast<uint32_t>(event_type);
		scope.category = static_cast<uint32_t>(category);
		scope.pipeline_id = pipeline_id;
		scope.task_id = task_id;
		scope.arg = arg;
		scope.active = true;
	}
	return scope;
}

inline void
YaapTraceEnd(YaapTraceScope *scope) noexcept
{
	if (unlikely(scope != nullptr && scope->active))
	{
		scope->active = false;
		if (YaapTraceIsEnabled())
		{
			const YaapTraceTimePoint end_time = YaapTraceCaptureTime();
			YaapTraceRecordDurationImpl(static_cast<YaapTraceEventType>(scope->event_type),
			                            static_cast<YaapTraceCategory>(scope->category),
			                            scope->pipeline_id,
			                            scope->task_id,
			                            scope->begin_time.monotonic_ns,
			                            end_time.monotonic_ns,
			                            scope->arg);
		}
	}
}

inline void
YaapTraceEnd(YaapTraceScope *scope, uint64_t override_arg) noexcept
{
	if (scope != nullptr)
		scope->arg = override_arg;
	YaapTraceEnd(scope);
}

/* Lightweight RAII guard for nested duration scopes. */
class YaapTraceScoped {
public:
	YaapTraceScoped(YaapTraceEventType event_type,
	                YaapTraceCategory category = YaapTraceCategory::EXECUTION,
	                uint32_t pipeline_id = YAAP_TRACE_INVALID_ID,
	                uint32_t task_id = YAAP_TRACE_INVALID_ID,
	                uint64_t arg = 0) noexcept
		: scope_(YaapTraceBegin(event_type, category, pipeline_id, task_id, arg))
	{
	}

	~YaapTraceScoped() noexcept
	{
		YaapTraceEnd(&scope_);
	}

	void SetArg(uint64_t arg) noexcept { scope_.arg = arg; }
	void Finish() noexcept { YaapTraceEnd(&scope_); }
	void Finish(uint64_t arg) noexcept { YaapTraceEnd(&scope_, arg); }
	void Cancel() noexcept { scope_.active = false; }

	YaapTraceScoped(const YaapTraceScoped &) = delete;
	YaapTraceScoped &operator=(const YaapTraceScoped &) = delete;

private:
	YaapTraceScope scope_;
};

/*
 * Fast-path macros that skip argument evaluation completely when trace is off.
 */
#define YAAP_TRACE_INSTANT(type, cat, pid, tid, arg) \
	do { \
		if (unlikely(::pg_yaap::pipeline::YaapTraceIsEnabled())) \
			::pg_yaap::pipeline::YaapTraceRecordInstantImpl( \
				(type), (cat), (pid), (tid), (arg)); \
	} while (0)

/* ---- Query lifecycle & offline collector/exporter API ---- */

struct YaapTraceExportSummary {
	uint64_t total_events = 0;
	uint64_t dropped_events = 0;
	uint64_t query_duration_ns = 0;
	uint64_t export_duration_ns = 0;
	uint32_t abnormal_processes = 0;
	bool     complete = false;
	bool     exported = false;
};

/*
 * Leader: allocate shared trace header and per-process buffers in query DSA,
 * then attach the leader slot (slot 0). Returns true if tracing is active.
 * Allocation failures degrade gracefully (WARNING + disabled, never throws).
 */
bool YaapTraceInitQuery(PipelineSharedControl *control,
                        dsa_area *dsa,
                        uint32_t num_workers);

/*
 * Explicit-config entry point used by unit tests to control capacity directly
 * without PostgreSQL GUC globals.
 */
bool YaapTraceInitQueryWithConfig(PipelineSharedControl *control,
                                  dsa_area *dsa,
                                  uint32_t num_workers,
                                  bool enabled,
                                  uint32_t capacity_per_slot,
                                  uint64_t max_total_bytes);

/*
 * Worker (or leader): attach process-local state to its exclusive DSA buffer.
 * Must be called once per process before recording any events.
 */
bool YaapTraceAttachProcess(PipelineSharedControl *control,
                            dsa_area *dsa,
                            int worker_index,
                            int pid) noexcept;

/*
 * Process teardown: publish final state (FINALIZED or ABORTED) with a write
 * barrier and clear g_yaap_trace_local. Safe to call multiple times.
 */
void YaapTraceDetachProcess(bool aborted = false) noexcept;

/*
 * Leader: collect all per-process buffers after all workers have stopped,
 * stream Chrome Trace Event JSON to a unique file under pg_yaap.trace_output_dir,
 * and emit summary NOTICE/WARNING. Must be called BEFORE DestroyRuntimeDsm().
 * Never throws; export errors are logged as WARNING.
 */
YaapTraceExportSummary YaapTraceCollectAndExport(PipelineSharedControl *control,
                                                 dsa_area *dsa,
                                                 bool query_succeeded);

/*
 * Direct file export helper (used by both production collector and unit tests).
 * Writes Chrome Trace Event JSON to an already-opened fd using a bounded buffer.
 */
bool YaapTraceWriteChromeJsonToFd(PipelineSharedControl *control,
                                  dsa_area *dsa,
                                  bool query_succeeded,
                                  int fd,
                                  YaapTraceExportSummary *summary_out);

}  /* namespace pipeline */
}  /* namespace pg_yaap */
