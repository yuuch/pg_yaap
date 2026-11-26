#include "parallel/pipeline/execution_trace.hpp"

extern "C" {
#include "catalog/pg_authid_d.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/elog.h"
#include "utils/memutils.h"
extern bool  pg_yaap_trace_enabled;
extern int   pg_yaap_trace_buffer_mb;
extern char *pg_yaap_trace_output_dir;
}

#include <algorithm>
#include <cerrno>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/dsm_task_queue.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"

namespace pg_yaap {
namespace pipeline {

YaapTraceLocalState *g_yaap_trace_local = nullptr;

namespace {

/* Hard safety ceiling across all processes in one query (64 MB). */
static constexpr uint64_t kTraceHardMaxTotalBytes = UINT64CONST(64) * 1024u * 1024u;
/* Stream buffer size for JSON file export (avoids large memory allocations). */
static constexpr size_t   kJsonWriteBufferBytes   = 64u * 1024u;
/* Dedicated synthetic PID for the Pipeline Scheduler timeline track in Perfetto. */
static constexpr int32_t  kSchedulerTrackPid      = 1;

/* Monotonically increasing per-backend sequence for unique execution IDs. */
static uint64_t g_local_execution_seq = 0;

/* Static process-local state storage (no heap allocation on attach/detach). */
static YaapTraceLocalState g_trace_local_storage{};

struct YaapTraceSharedHeader {
	uint32_t    format_version;
	uint32_t    num_slots;
	uint32_t    capacity_per_slot;
	uint32_t    reported;
	uint64_t    execution_id;
	uint64_t    origin_ns;
	int32_t     leader_pid;
	uint32_t    header_stride;
	uint64_t    events_stride;
	dsa_pointer headers_dp;
	dsa_pointer events_dp;
};

static inline uint32_t
SlotForWorker(int worker_index) noexcept
{
	return worker_index == LEADER_WORKER_INDEX
		? 0u
		: static_cast<uint32_t>(worker_index + 1);
}

static inline YaapTraceSharedHeader *
ResolveSharedHeader(PipelineSharedControl *control, dsa_area *dsa) noexcept
{
	if (control == nullptr || dsa == nullptr ||
		pg_atomic_read_u32(&control->trace_enabled) == 0 ||
		!DsaPointerIsValid(control->trace_root))
		return nullptr;
	return static_cast<YaapTraceSharedHeader *>(
		dsa_get_address(dsa, control->trace_root));
}

static inline YaapTraceBufferHeader *
ResolveBufferHeader(YaapTraceSharedHeader *shared, dsa_area *dsa, uint32_t slot) noexcept
{
	char *base = static_cast<char *>(dsa_get_address(dsa, shared->headers_dp));
	return reinterpret_cast<YaapTraceBufferHeader *>(
		base + static_cast<size_t>(slot) * shared->header_stride);
}

static inline YaapTraceEvent *
ResolveBufferEvents(YaapTraceSharedHeader *shared, dsa_area *dsa, uint32_t slot) noexcept
{
	char *base = static_cast<char *>(dsa_get_address(dsa, shared->events_dp));
	return reinterpret_cast<YaapTraceEvent *>(
		base + static_cast<uint64_t>(slot) * shared->events_stride);
}

/*
 * Allocate a cache-line-aligned region inside DSA without raising ERROR on OOM.
 * Returns InvalidDsaPointer if DSA cannot satisfy the allocation.
 */
static dsa_pointer
AlignedTraceDsaAllocNoOom(dsa_area *dsa, size_t bytes)
{
	const size_t padded = bytes + PG_CACHE_LINE_SIZE;
	dsa_pointer raw = dsa_allocate_extended(dsa, padded, DSA_ALLOC_ZERO | DSA_ALLOC_NO_OOM);
	if (!DsaPointerIsValid(raw))
		return InvalidDsaPointer;
	uintptr_t addr = reinterpret_cast<uintptr_t>(dsa_get_address(dsa, raw));
	uintptr_t delta = (-addr) & (PG_CACHE_LINE_SIZE - 1u);
	return raw + static_cast<dsa_pointer>(delta);
}

static inline uint64_t
RelativeNs(uint64_t now_ns, uint64_t origin_ns) noexcept
{
	return now_ns >= origin_ns ? (now_ns - origin_ns) : 0u;
}

static inline void
AppendEventLocal(YaapTraceLocalState *local, const YaapTraceEvent &ev) noexcept
{
	if (local->event_count < local->capacity)
	{
		local->events[local->event_count] = ev;
		/*
		 * Release barrier ensures the full YaapTraceEvent payload is visible
		 * in shared memory BEFORE event_count increments. Even if a worker
		 * terminates abruptly, every event in [0, header->event_count) is intact.
		 */
		pg_write_barrier();
		++local->event_count;
		pg_atomic_write_u64(&local->header->event_count, local->event_count);
	}
	else
	{
		++local->dropped_count;
		pg_atomic_write_u64(&local->header->dropped_count, local->dropped_count);
	}
}

/* ---- Bounded buffered file writer for O(1)-memory Chrome JSON export ---- */

class BufferedFdWriter {
public:
	explicit BufferedFdWriter(int fd) : fd_(fd) {}

	bool Ok() const noexcept { return ok_; }
	int SavedErrno() const noexcept { return saved_errno_; }

	void WriteBytes(const char *data, size_t len) noexcept
	{
		if (!ok_ || len == 0)
			return;
		if (len >= sizeof(buf_) - used_)
		{
			if (!Flush())
				return;
			if (len >= sizeof(buf_))
			{
				WriteAll(data, len);
				return;
			}
		}
		std::memcpy(buf_ + used_, data, len);
		used_ += len;
	}

	void WriteStr(const char *s) noexcept
	{
		WriteBytes(s, std::strlen(s));
	}

	void WriteChar(char c) noexcept
	{
		if (!ok_)
			return;
		if (used_ == sizeof(buf_) && !Flush())
			return;
		buf_[used_++] = c;
	}

	void Printf(const char *fmt, ...) noexcept
	{
		if (!ok_)
			return;
		char tmp[1024];
		va_list ap;
		va_start(ap, fmt);
		int n = std::vsnprintf(tmp, sizeof(tmp), fmt, ap);
		va_end(ap);
		if (n <= 0)
			return;
		const size_t len = static_cast<size_t>(n) < sizeof(tmp)
			? static_cast<size_t>(n)
			: (sizeof(tmp) - 1u);
		WriteBytes(tmp, len);
	}

	bool Flush() noexcept
	{
		if (!ok_)
			return false;
		if (used_ == 0)
			return true;
		const bool written = WriteAll(buf_, used_);
		used_ = 0;
		return written;
	}

private:
	bool WriteAll(const char *data, size_t len) noexcept
	{
		size_t off = 0;
		while (off < len)
		{
			ssize_t n = ::write(fd_, data + off, len - off);
			if (n < 0)
			{
				if (errno == EINTR)
					continue;
				ok_ = false;
				saved_errno_ = errno;
				return false;
			}
			if (n == 0)
			{
				ok_ = false;
				saved_errno_ = EIO;
				return false;
			}
			off += static_cast<size_t>(n);
		}
		return true;
	}

	int    fd_;
	size_t used_ = 0;
	bool   ok_ = true;
	int    saved_errno_ = 0;
	char   buf_[kJsonWriteBufferBytes];
};

static const char *
CategoryName(uint32_t cat) noexcept
{
	switch (static_cast<YaapTraceCategory>(cat))
	{
		case YaapTraceCategory::EXECUTION: return "execution";
		case YaapTraceCategory::SCHEDULER: return "scheduler";
	}
	return "other";
}

static const char *
TaskKindName(uint8_t kind) noexcept
{
	switch (static_cast<TaskKind>(kind))
	{
		case TaskKind::RUN:      return "RUN";
		case TaskKind::COMBINE:  return "COMBINE";
		case TaskKind::FINALIZE: return "FINALIZE";
	}
	return "TASK";
}

static const char *
TaskOutcomeName(YaapTraceTaskOutcome outcome) noexcept
{
	switch (outcome)
	{
		case YaapTraceTaskOutcome::OK:        return "ok";
		case YaapTraceTaskOutcome::FAILED:    return "error";
		case YaapTraceTaskOutcome::CANCELLED: return "cancelled";
	}
	return "unknown";
}

static const char *
OpKindShortName(OpKind kind) noexcept
{
	switch (kind)
	{
		case OpKind::SEQ_SCAN:               return "SEQ_SCAN";
		case OpKind::DELIM_SCAN:             return "DELIM_SCAN";
		case OpKind::MATERIALIZE:            return "MATERIALIZE";
		case OpKind::HASH_AGGREGATE:         return "HASH_AGGREGATE";
		case OpKind::PERFECT_HASH_AGGREGATE: return "PERFECT_HASH_AGGREGATE";
		case OpKind::HASH_JOIN:              return "HASH_JOIN";
		case OpKind::HASH_JOIN_PROBE:        return "HASH_JOIN_PROBE";
		case OpKind::CROSS_PRODUCT:          return "CROSS_PRODUCT";
		case OpKind::ORDER:                  return "ORDER";
		case OpKind::TOP_N:                  return "TOP_N";
		case OpKind::OUTPUT:                 return "OUTPUT";
		case OpKind::FILTER:                 return "FILTER";
		case OpKind::PROJECTION:             return "PROJECTION";
	}
	return "OP";
}

/*
 * Build a safe, schema-free pipeline description (operator kinds only: no SQL
 * text, no relation names, no constants, no syscache lookups). Safe to call
 * even during transaction abort / PG_CATCH cleanup.
 */
static void
FormatPipelineDesc(PipelineSharedControl *control,
                   dsa_area *dsa,
                   uint32_t pipeline_id,
                   char *out,
                   size_t out_size) noexcept
{
	if (control == nullptr || dsa == nullptr ||
		!DsaPointerIsValid(control->pipelines_root) ||
		pipeline_id >= static_cast<uint32_t>(control->num_pipelines))
	{
		std::snprintf(out, out_size, "Pipeline %u", pipeline_id);
		return;
	}
	auto *pipelines = static_cast<const PipelineDescriptor *>(
		dsa_get_address(dsa, control->pipelines_root));
	if (pipelines == nullptr)
	{
		std::snprintf(out, out_size, "Pipeline %u", pipeline_id);
		return;
	}
	const PipelineDescriptor &pd = pipelines[pipeline_id];
	if (!DsaPointerIsValid(pd.ops) || pd.op_count <= 0)
	{
		std::snprintf(out, out_size, "Pipeline %u", pipeline_id);
		return;
	}
	auto *ops = static_cast<const OpDescriptor *>(dsa_get_address(dsa, pd.ops));
	if (ops == nullptr)
	{
		std::snprintf(out, out_size, "Pipeline %u", pipeline_id);
		return;
	}
	std::snprintf(out, out_size, "%s -> %s",
	              OpKindShortName(ops[0].kind),
	              OpKindShortName(ops[pd.op_count - 1].kind));
}

static void
EmitCommaIfNeeded(BufferedFdWriter &w, bool &first) noexcept
{
	if (!first)
		w.WriteChar(',');
	first = false;
}

static int32_t
DisplayPidForSlot(const YaapTraceBufferHeader *hdr, uint32_t slot, int32_t leader_pid) noexcept
{
	if (hdr->pid > 0)
		return hdr->pid;
	if (slot == 0 && leader_pid > 0)
		return leader_pid;
	return 1000000 + static_cast<int32_t>(slot);
}

static void
EmitChromeEvent(BufferedFdWriter &w,
                bool &first,
                const YaapTraceEvent &ev,
                int32_t slot_pid,
                int32_t worker_id) noexcept
{
	const double ts_us  = static_cast<double>(ev.timestamp_ns) / 1000.0;
	const double dur_us = static_cast<double>(ev.duration_ns) / 1000.0;
	const char  *cat    = CategoryName(ev.category);

	EmitCommaIfNeeded(w, first);

	switch (static_cast<YaapTraceEventType>(ev.event_type))
	{
		case YaapTraceEventType::QUERY_EXECUTE:
			w.Printf("{\"name\":\"Query Execute\",\"cat\":\"%s\",\"ph\":\"X\","
			         "\"pid\":%d,\"tid\":0,\"ts\":%.3f,\"dur\":%.3f,"
			         "\"args\":{\"worker_id\":%d,\"os_pid\":%d,\"wall_time_ns\":%llu}}",
			         cat, slot_pid, ts_us, dur_us, worker_id, slot_pid,
			         static_cast<unsigned long long>(ev.duration_ns));
			break;

		case YaapTraceEventType::OUTPUT_EMIT:
			w.Printf("{\"name\":\"Output Emit\",\"cat\":\"%s\",\"ph\":\"X\","
			         "\"pid\":%d,\"tid\":0,\"ts\":%.3f,\"dur\":%.3f,"
			         "\"args\":{\"worker_id\":%d,\"os_pid\":%d,\"wall_time_ns\":%llu}}",
			         cat, slot_pid, ts_us, dur_us, worker_id, slot_pid,
			         static_cast<unsigned long long>(ev.duration_ns));
			break;

		case YaapTraceEventType::LEADER_WAIT:
		{
			const char *wait_name =
				(ev.arg == static_cast<uint64_t>(YaapTraceLeaderWaitKind::WORKER_READY))
				? "Leader Wait Workers Ready"
				: "Leader Wait Event DAG";
			w.Printf("{\"name\":\"%s\",\"cat\":\"%s\",\"ph\":\"X\","
			         "\"pid\":%d,\"tid\":1,\"ts\":%.3f,\"dur\":%.3f,"
			         "\"args\":{\"worker_id\":%d,\"os_pid\":%d,\"wall_time_ns\":%llu}}",
			         wait_name, cat, slot_pid, ts_us, dur_us, worker_id, slot_pid,
			         static_cast<unsigned long long>(ev.duration_ns));
			break;
		}

		case YaapTraceEventType::WORKER_START:
			w.Printf("{\"name\":\"Worker Start\",\"cat\":\"%s\",\"ph\":\"i\",\"s\":\"t\","
			         "\"pid\":%d,\"tid\":0,\"ts\":%.3f,"
			         "\"args\":{\"worker_id\":%d,\"os_pid\":%d}}",
			         cat, slot_pid, ts_us, worker_id, slot_pid);
			break;

		case YaapTraceEventType::WORKER_EXIT:
			w.Printf("{\"name\":\"Worker Exit\",\"cat\":\"%s\",\"ph\":\"i\",\"s\":\"t\","
			         "\"pid\":%d,\"tid\":0,\"ts\":%.3f,"
			         "\"args\":{\"worker_id\":%d,\"os_pid\":%d,\"abnormal\":%s}}",
			         cat, slot_pid, ts_us, worker_id, slot_pid,
			         ev.arg != 0 ? "true" : "false");
			break;

		case YaapTraceEventType::WORKER_IDLE:
			w.Printf("{\"name\":\"Worker Idle\",\"cat\":\"%s\",\"ph\":\"X\","
			         "\"pid\":%d,\"tid\":1,\"ts\":%.3f,\"dur\":%.3f,"
			         "\"args\":{\"worker_id\":%d,\"os_pid\":%d,\"wall_time_ns\":%llu}}",
			         cat, slot_pid, ts_us, dur_us, worker_id, slot_pid,
			         static_cast<unsigned long long>(ev.duration_ns));
			break;

		case YaapTraceEventType::PIPELINE_READY:
			w.Printf("{\"name\":\"Pipeline %u Ready\",\"cat\":\"%s\",\"ph\":\"i\",\"s\":\"t\","
			         "\"pid\":%d,\"tid\":%u,\"ts\":%.3f,"
			         "\"args\":{\"pipeline_id\":%u,\"emitter_worker_id\":%d,\"emitter_os_pid\":%d}}",
			         ev.pipeline_id, cat, kSchedulerTrackPid, ev.pipeline_id, ts_us,
			         ev.pipeline_id, worker_id, slot_pid);
			break;

		case YaapTraceEventType::PIPELINE_SCHEDULE:
			w.Printf("{\"name\":\"Pipeline %u Schedule\",\"cat\":\"%s\",\"ph\":\"i\",\"s\":\"t\","
			         "\"pid\":%d,\"tid\":%u,\"ts\":%.3f,"
			         "\"args\":{\"pipeline_id\":%u,\"task_count\":%llu,\"emitter_worker_id\":%d,\"emitter_os_pid\":%d}}",
			         ev.pipeline_id, cat, kSchedulerTrackPid, ev.pipeline_id, ts_us,
			         ev.pipeline_id, static_cast<unsigned long long>(ev.arg), worker_id, slot_pid);
			break;

		case YaapTraceEventType::PIPELINE_FINISH:
			w.Printf("{\"name\":\"Pipeline %u Finish\",\"cat\":\"%s\",\"ph\":\"i\",\"s\":\"t\","
			         "\"pid\":%d,\"tid\":%u,\"ts\":%.3f,"
			         "\"args\":{\"pipeline_id\":%u,\"emitter_worker_id\":%d,\"emitter_os_pid\":%d}}",
			         ev.pipeline_id, cat, kSchedulerTrackPid, ev.pipeline_id, ts_us,
			         ev.pipeline_id, worker_id, slot_pid);
			break;

		case YaapTraceEventType::EVENT_SCHEDULE:
		{
			const uint8_t kind = YaapTraceUnpackEventKind(ev.arg);
			const uint32_t task_count = YaapTraceUnpackEventTaskCount(ev.arg);
			w.Printf("{\"name\":\"Event %u Schedule (%s)\",\"cat\":\"%s\",\"ph\":\"i\",\"s\":\"t\","
			         "\"pid\":%d,\"tid\":%u,\"ts\":%.3f,"
			         "\"args\":{\"pipeline_id\":%u,\"event_id\":%u,\"task_kind\":\"%s\","
			         "\"task_count\":%u,\"emitter_worker_id\":%d,\"emitter_os_pid\":%d}}",
			         ev.task_id, TaskKindName(kind), cat, kSchedulerTrackPid, ev.pipeline_id, ts_us,
			         ev.pipeline_id, ev.task_id, TaskKindName(kind),
			         task_count, worker_id, slot_pid);
			break;
		}

		case YaapTraceEventType::EVENT_FINISH:
		{
			const uint8_t kind = YaapTraceUnpackEventKind(ev.arg);
			w.Printf("{\"name\":\"Event %u Finish (%s)\",\"cat\":\"%s\",\"ph\":\"i\",\"s\":\"t\","
			         "\"pid\":%d,\"tid\":%u,\"ts\":%.3f,"
			         "\"args\":{\"pipeline_id\":%u,\"event_id\":%u,\"task_kind\":\"%s\","
			         "\"emitter_worker_id\":%d,\"emitter_os_pid\":%d}}",
			         ev.task_id, TaskKindName(kind), cat, kSchedulerTrackPid, ev.pipeline_id, ts_us,
			         ev.pipeline_id, ev.task_id, TaskKindName(kind),
			         worker_id, slot_pid);
			break;
		}

		case YaapTraceEventType::TASK_ENQUEUE:
		{
			const uint32_t event_id = YaapTraceUnpackTaskEventId(ev.arg);
			const uint8_t kind = YaapTraceUnpackTaskKind(ev.arg);
			const uint32_t part = YaapTraceUnpackTaskPartitionId(ev.arg);
			w.Printf("{\"name\":\"Enqueue P%u %s T%u\",\"cat\":\"%s\",\"ph\":\"i\",\"s\":\"t\","
			         "\"pid\":%d,\"tid\":2,\"ts\":%.3f,"
			         "\"args\":{\"worker_id\":%d,\"os_pid\":%d,\"pipeline_id\":%u,"
			         "\"task_id\":%u,\"event_id\":%u,\"task_kind\":\"%s\"",
			         ev.pipeline_id, TaskKindName(kind), ev.task_id, cat, slot_pid, ts_us,
			         worker_id, slot_pid, ev.pipeline_id, ev.task_id, event_id, TaskKindName(kind));
			if (part != YAAP_TRACE_INVALID_ID)
				w.Printf(",\"partition_id\":%u", part);
			w.WriteStr("}}");
			break;
		}

		case YaapTraceEventType::TASK_DEQUEUE:
		{
			const uint32_t event_id = YaapTraceUnpackTaskEventId(ev.arg);
			const uint8_t kind = YaapTraceUnpackTaskKind(ev.arg);
			const uint32_t part = YaapTraceUnpackTaskPartitionId(ev.arg);
			w.Printf("{\"name\":\"Dequeue P%u %s T%u\",\"cat\":\"%s\",\"ph\":\"i\",\"s\":\"t\","
			         "\"pid\":%d,\"tid\":2,\"ts\":%.3f,"
			         "\"args\":{\"worker_id\":%d,\"os_pid\":%d,\"pipeline_id\":%u,"
			         "\"task_id\":%u,\"event_id\":%u,\"task_kind\":\"%s\"",
			         ev.pipeline_id, TaskKindName(kind), ev.task_id, cat, slot_pid, ts_us,
			         worker_id, slot_pid, ev.pipeline_id, ev.task_id, event_id, TaskKindName(kind));
			if (part != YAAP_TRACE_INVALID_ID)
				w.Printf(",\"partition_id\":%u", part);
			w.WriteStr("}}");
			break;
		}

		case YaapTraceEventType::TASK_EXECUTE:
		{
			const uint32_t event_id = YaapTraceUnpackTaskEventId(ev.arg);
			const uint8_t kind = YaapTraceUnpackTaskKind(ev.arg);
			const YaapTraceTaskOutcome outcome = YaapTraceUnpackTaskOutcome(ev.arg);
			const uint32_t part = YaapTraceUnpackTaskPartitionId(ev.arg);
			w.Printf("{\"name\":\"Pipeline %u / %s / Task %u\",\"cat\":\"%s\",\"ph\":\"X\","
			         "\"pid\":%d,\"tid\":0,\"ts\":%.3f,\"dur\":%.3f,"
			         "\"args\":{\"worker_id\":%d,\"os_pid\":%d,\"pipeline_id\":%u,"
			         "\"task_id\":%u,\"event_id\":%u,\"task_kind\":\"%s\","
			         "\"outcome\":\"%s\",\"wall_time_ns\":%llu",
			         ev.pipeline_id, TaskKindName(kind), ev.task_id, cat,
			         slot_pid, ts_us, dur_us,
			         worker_id, slot_pid, ev.pipeline_id, ev.task_id, event_id,
			         TaskKindName(kind), TaskOutcomeName(outcome),
			         static_cast<unsigned long long>(ev.duration_ns));
			if (part != YAAP_TRACE_INVALID_ID)
				w.Printf(",\"partition_id\":%u", part);
			w.WriteStr("}}");
			break;
		}

		default:
			w.Printf("{\"name\":\"Unknown(%u)\",\"cat\":\"%s\",\"ph\":\"i\",\"s\":\"t\","
			         "\"pid\":%d,\"tid\":0,\"ts\":%.3f}",
			         ev.event_type, cat, slot_pid, ts_us);
			break;
	}
}

}  /* namespace */

void
YaapTraceRecordInstantImpl(YaapTraceEventType event_type,
                           YaapTraceCategory category,
                           uint32_t pipeline_id,
                           uint32_t task_id,
                           uint64_t arg) noexcept
{
	YaapTraceLocalState *local = g_yaap_trace_local;
	if (unlikely(local == nullptr))
		return;
	const uint64_t now_ns = YaapTraceNow();
	YaapTraceEvent ev{};
	ev.timestamp_ns = RelativeNs(now_ns, local->origin_ns);
	ev.duration_ns  = 0;
	ev.event_type   = static_cast<uint32_t>(event_type);
	ev.category     = static_cast<uint32_t>(category);
	ev.pipeline_id  = pipeline_id;
	ev.task_id      = task_id;
	ev.arg          = arg;
	AppendEventLocal(local, ev);
}

void
YaapTraceRecordDurationImpl(YaapTraceEventType event_type,
                            YaapTraceCategory category,
                            uint32_t pipeline_id,
                            uint32_t task_id,
                            uint64_t begin_ns,
                            uint64_t end_ns,
                            uint64_t arg) noexcept
{
	YaapTraceLocalState *local = g_yaap_trace_local;
	if (unlikely(local == nullptr))
		return;
	const uint64_t rel_begin = RelativeNs(begin_ns, local->origin_ns);
	const uint64_t rel_end   = RelativeNs(end_ns, local->origin_ns);
	YaapTraceEvent ev{};
	ev.timestamp_ns = rel_begin;
	ev.duration_ns  = rel_end >= rel_begin ? (rel_end - rel_begin) : 0u;
	ev.event_type   = static_cast<uint32_t>(event_type);
	ev.category     = static_cast<uint32_t>(category);
	ev.pipeline_id  = pipeline_id;
	ev.task_id      = task_id;
	ev.arg          = arg;
	AppendEventLocal(local, ev);
}

bool
YaapTraceInitQueryWithConfig(PipelineSharedControl *control,
                             dsa_area *dsa,
                             uint32_t num_workers,
                             bool enabled,
                             uint32_t capacity_per_slot,
                             uint64_t max_total_bytes)
{
	g_yaap_trace_local = nullptr;
	if (control == nullptr || dsa == nullptr)
		return false;

	control->trace_root = InvalidDsaPointer;
	pg_atomic_write_u32(&control->trace_enabled, 0u);

	if (!enabled || capacity_per_slot == 0)
		return false;

	const uint32_t num_slots = num_workers + 1u;
	const uint32_t header_stride = static_cast<uint32_t>(CACHELINEALIGN(sizeof(YaapTraceBufferHeader)));
	const uint64_t header_total_bytes = static_cast<uint64_t>(num_slots) * header_stride;
	const uint64_t overhead_bytes = sizeof(YaapTraceSharedHeader) + header_total_bytes +
		static_cast<uint64_t>(num_slots) * PG_CACHE_LINE_SIZE + 2u * PG_CACHE_LINE_SIZE;

	uint64_t effective_limit = std::min(max_total_bytes, kTraceHardMaxTotalBytes);
	if (control->dsa_limit_bytes > 0)
		effective_limit = std::min(effective_limit, control->dsa_limit_bytes / 2u);

	if (effective_limit <= overhead_bytes)
	{
		elog(WARNING, "pg_yaap[trace] query DSA budget too small for trace buffers; trace disabled");
		return false;
	}

	const uint64_t avail_for_events = effective_limit - overhead_bytes;
	const uint64_t per_slot_budget = avail_for_events / num_slots;
	const uint32_t max_capacity = static_cast<uint32_t>(
		std::min<uint64_t>(per_slot_budget / sizeof(YaapTraceEvent), UINT32_MAX));
	if (max_capacity == 0)
	{
		elog(WARNING, "pg_yaap[trace] insufficient memory for trace events; trace disabled");
		return false;
	}

	uint32_t actual_capacity = std::min(capacity_per_slot, max_capacity);
	if (actual_capacity < capacity_per_slot)
	{
		elog(WARNING,
		     "pg_yaap[trace] clamped per-process trace capacity from %u to %u events to respect memory cap",
		     capacity_per_slot, actual_capacity);
	}

	const uint64_t events_stride = CACHELINEALIGN(
		static_cast<uint64_t>(actual_capacity) * sizeof(YaapTraceEvent));
	const uint64_t events_total_bytes = static_cast<uint64_t>(num_slots) * events_stride;

	dsa_pointer shared_dp = dsa_allocate_extended(dsa, sizeof(YaapTraceSharedHeader),
	                                              DSA_ALLOC_ZERO | DSA_ALLOC_NO_OOM);
	if (!DsaPointerIsValid(shared_dp))
	{
		elog(WARNING, "pg_yaap[trace] failed to allocate trace header in DSA; trace disabled");
		return false;
	}

	dsa_pointer headers_dp = AlignedTraceDsaAllocNoOom(dsa, static_cast<size_t>(header_total_bytes));
	if (!DsaPointerIsValid(headers_dp))
	{
		elog(WARNING, "pg_yaap[trace] failed to allocate trace buffer headers in DSA; trace disabled");
		return false;
	}

	dsa_pointer events_dp = AlignedTraceDsaAllocNoOom(dsa, static_cast<size_t>(events_total_bytes));
	if (!DsaPointerIsValid(events_dp))
	{
		elog(WARNING, "pg_yaap[trace] failed to allocate trace event buffers (%llu bytes) in DSA; trace disabled",
		     static_cast<unsigned long long>(events_total_bytes));
		return false;
	}

	auto *shared = static_cast<YaapTraceSharedHeader *>(dsa_get_address(dsa, shared_dp));
	shared->format_version    = YAAP_TRACE_FORMAT_VERSION;
	shared->num_slots         = num_slots;
	shared->capacity_per_slot = actual_capacity;
	shared->reported          = 0;
	shared->execution_id      = (static_cast<uint64_t>(static_cast<uint32_t>(control->leader_pid)) << 32) |
		(++g_local_execution_seq & 0xFFFFFFFFu);
	shared->origin_ns         = YaapTraceNow();
	shared->leader_pid        = control->leader_pid;
	shared->header_stride     = header_stride;
	shared->events_stride     = events_stride;
	shared->headers_dp        = headers_dp;
	shared->events_dp         = events_dp;

	for (uint32_t slot = 0; slot < num_slots; ++slot)
	{
		YaapTraceBufferHeader *hdr = ResolveBufferHeader(shared, dsa, slot);
		hdr->format_version = YAAP_TRACE_FORMAT_VERSION;
		hdr->state          = static_cast<uint32_t>(YaapTraceBufferState::UNUSED);
		hdr->execution_id   = shared->execution_id;
		hdr->worker_id      = (slot == 0) ? LEADER_WORKER_INDEX : static_cast<int32_t>(slot - 1u);
		hdr->pid            = 0;
		hdr->capacity       = actual_capacity;
		pg_atomic_init_u64(&hdr->event_count, 0);
		pg_atomic_init_u64(&hdr->dropped_count, 0);
	}

	control->trace_root = shared_dp;
	pg_write_barrier();
	pg_atomic_write_u32(&control->trace_enabled, 1u);

	return YaapTraceAttachProcess(control, dsa, LEADER_WORKER_INDEX, control->leader_pid);
}

bool
YaapTraceInitQuery(PipelineSharedControl *control,
                   dsa_area *dsa,
                   uint32_t num_workers)
{
	if (!pg_yaap_trace_enabled)
	{
		g_yaap_trace_local = nullptr;
		if (control != nullptr)
		{
			control->trace_root = InvalidDsaPointer;
			pg_atomic_write_u32(&control->trace_enabled, 0u);
		}
		return false;
	}

	const uint64_t per_slot_bytes =
		static_cast<uint64_t>(std::max(1, pg_yaap_trace_buffer_mb)) * 1024u * 1024u;
	const uint32_t capacity_per_slot =
		static_cast<uint32_t>(per_slot_bytes / sizeof(YaapTraceEvent));

	return YaapTraceInitQueryWithConfig(control, dsa, num_workers,
	                                    true, capacity_per_slot,
	                                    kTraceHardMaxTotalBytes);
}

bool
YaapTraceAttachProcess(PipelineSharedControl *control,
                       dsa_area *dsa,
                       int worker_index,
                       int pid) noexcept
{
	g_yaap_trace_local = nullptr;
	if (control == nullptr || dsa == nullptr)
		return false;
	if (pg_atomic_read_u32(&control->trace_enabled) == 0)
		return false;
	pg_read_barrier();

	YaapTraceSharedHeader *shared = ResolveSharedHeader(control, dsa);
	if (shared == nullptr || shared->format_version != YAAP_TRACE_FORMAT_VERSION)
		return false;

	const uint32_t slot = SlotForWorker(worker_index);
	if (slot >= shared->num_slots)
		return false;

	YaapTraceBufferHeader *hdr = ResolveBufferHeader(shared, dsa, slot);
	YaapTraceEvent *events = ResolveBufferEvents(shared, dsa, slot);
	if (hdr == nullptr || events == nullptr)
		return false;

	hdr->pid = pid;
	hdr->worker_id = worker_index;
	hdr->state = static_cast<uint32_t>(YaapTraceBufferState::ACTIVE);
	pg_write_barrier();

	g_trace_local_storage.header        = hdr;
	g_trace_local_storage.events        = events;
	g_trace_local_storage.origin_ns     = shared->origin_ns;
	g_trace_local_storage.event_count   = 0;
	g_trace_local_storage.dropped_count = 0;
	g_trace_local_storage.capacity      = hdr->capacity;
	g_yaap_trace_local = &g_trace_local_storage;
	return true;
}

void
YaapTraceDetachProcess(bool aborted) noexcept
{
	YaapTraceLocalState *local = g_yaap_trace_local;
	if (local == nullptr)
		return;
	if (local->header != nullptr)
	{
		pg_write_barrier();
		local->header->state = static_cast<uint32_t>(
			aborted ? YaapTraceBufferState::ABORTED : YaapTraceBufferState::FINALIZED);
		pg_write_barrier();
	}
	g_yaap_trace_local = nullptr;
	g_trace_local_storage = YaapTraceLocalState{};
}

bool
YaapTraceWriteChromeJsonToFd(PipelineSharedControl *control,
                             dsa_area *dsa,
                             bool query_succeeded,
                             int fd,
                             YaapTraceExportSummary *summary_out)
{
	YaapTraceExportSummary summary{};
	if (summary_out != nullptr)
		*summary_out = summary;

	YaapTraceSharedHeader *shared = ResolveSharedHeader(control, dsa);
	if (shared == nullptr || fd < 0)
		return false;

	/* Acquire all prior worker writes after OS worker-exit synchronization. */
	pg_read_barrier();

	const uint64_t export_begin_ns = YaapTraceNow();
	const uint64_t query_duration_ns = RelativeNs(export_begin_ns, shared->origin_ns);

	uint64_t total_events = 0;
	uint64_t dropped_events = 0;
	uint32_t abnormal_processes = 0;
	for (uint32_t slot = 0; slot < shared->num_slots; ++slot)
	{
		YaapTraceBufferHeader *hdr = ResolveBufferHeader(shared, dsa, slot);
		const uint64_t count = std::min<uint64_t>(
			pg_atomic_read_u64(&hdr->event_count), hdr->capacity);
		const uint64_t dropped = pg_atomic_read_u64(&hdr->dropped_count);
		total_events += count;
		dropped_events += dropped;
		const auto st = static_cast<YaapTraceBufferState>(hdr->state);
		if (st == YaapTraceBufferState::ACTIVE || st == YaapTraceBufferState::ABORTED)
			++abnormal_processes;
	}

	const bool complete = query_succeeded && dropped_events == 0 && abnormal_processes == 0;

	BufferedFdWriter w(fd);
	w.Printf("{\"displayTimeUnit\":\"ms\",\"yaap_trace\":{"
	         "\"format_version\":%u,"
	         "\"execution_id\":%llu,"
	         "\"complete\":%s,"
	         "\"leader_pid\":%d,"
	         "\"worker_count\":%u,"
	         "\"query_duration_ns\":%llu,"
	         "\"buffer_capacity_per_process\":%u,"
	         "\"total_events\":%llu,"
	         "\"dropped_events\":%llu,"
	         "\"abnormal_processes\":%u,"
	         "\"pipelines\":[",
	         shared->format_version,
	         static_cast<unsigned long long>(shared->execution_id),
	         complete ? "true" : "false",
	         shared->leader_pid,
	         shared->num_slots > 0 ? (shared->num_slots - 1u) : 0u,
	         static_cast<unsigned long long>(query_duration_ns),
	         shared->capacity_per_slot,
	         static_cast<unsigned long long>(total_events),
	         static_cast<unsigned long long>(dropped_events),
	         abnormal_processes);

	const uint32_t num_pipelines = control->num_pipelines > 0
		? static_cast<uint32_t>(control->num_pipelines) : 0u;
	for (uint32_t p = 0; p < num_pipelines; ++p)
	{
		char desc[128];
		FormatPipelineDesc(control, dsa, p, desc, sizeof(desc));
		if (p > 0)
			w.WriteChar(',');
		w.Printf("{\"pipeline_id\":%u,\"description\":\"%s\"}", p, desc);
	}
	w.WriteStr("]},\"traceEvents\":[");

	bool first_event = true;

	/* 1. Metadata events for Pipeline Scheduler virtual process and tracks. */
	EmitCommaIfNeeded(w, first_event);
	w.Printf("{\"name\":\"process_name\",\"ph\":\"M\",\"pid\":%d,"
	         "\"args\":{\"name\":\"YAAP Pipeline Scheduler\"}}",
	         kSchedulerTrackPid);
	EmitCommaIfNeeded(w, first_event);
	w.Printf("{\"name\":\"process_sort_index\",\"ph\":\"M\",\"pid\":%d,"
	         "\"args\":{\"sort_index\":0}}",
	         kSchedulerTrackPid);
	for (uint32_t p = 0; p < num_pipelines; ++p)
	{
		char desc[128];
		FormatPipelineDesc(control, dsa, p, desc, sizeof(desc));
		EmitCommaIfNeeded(w, first_event);
		w.Printf("{\"name\":\"thread_name\",\"ph\":\"M\",\"pid\":%d,\"tid\":%u,"
		         "\"args\":{\"name\":\"Pipeline %u: %s\"}}",
		         kSchedulerTrackPid, p, p, desc);
	}

	/* 2. Metadata events for Leader and every Worker process/tracks. */
	for (uint32_t slot = 0; slot < shared->num_slots; ++slot)
	{
		YaapTraceBufferHeader *hdr = ResolveBufferHeader(shared, dsa, slot);
		const uint64_t count = std::min<uint64_t>(
			pg_atomic_read_u64(&hdr->event_count), hdr->capacity);
		if (slot > 0 && hdr->pid == 0 && count == 0)
			continue;

		const int32_t display_pid = DisplayPidForSlot(hdr, slot, shared->leader_pid);
		EmitCommaIfNeeded(w, first_event);
		if (slot == 0)
		{
			w.Printf("{\"name\":\"process_name\",\"ph\":\"M\",\"pid\":%d,"
			         "\"args\":{\"name\":\"YAAP Leader (pid %d)\"}}",
			         display_pid, display_pid);
			EmitCommaIfNeeded(w, first_event);
			w.Printf("{\"name\":\"process_sort_index\",\"ph\":\"M\",\"pid\":%d,"
			         "\"args\":{\"sort_index\":1}}",
			         display_pid);
			EmitCommaIfNeeded(w, first_event);
			w.Printf("{\"name\":\"thread_name\",\"ph\":\"M\",\"pid\":%d,\"tid\":0,"
			         "\"args\":{\"name\":\"Query\"}}",
			         display_pid);
			EmitCommaIfNeeded(w, first_event);
			w.Printf("{\"name\":\"thread_name\",\"ph\":\"M\",\"pid\":%d,\"tid\":1,"
			         "\"args\":{\"name\":\"Leader Wait\"}}",
			         display_pid);
		}
		else
		{
			const int32_t wid = hdr->worker_id;
			w.Printf("{\"name\":\"process_name\",\"ph\":\"M\",\"pid\":%d,"
			         "\"args\":{\"name\":\"YAAP Worker %d (pid %d)\"}}",
			         display_pid, wid, display_pid);
			EmitCommaIfNeeded(w, first_event);
			w.Printf("{\"name\":\"process_sort_index\",\"ph\":\"M\",\"pid\":%d,"
			         "\"args\":{\"sort_index\":%u}}",
			         display_pid, slot + 1u);
			EmitCommaIfNeeded(w, first_event);
			w.Printf("{\"name\":\"thread_name\",\"ph\":\"M\",\"pid\":%d,\"tid\":0,"
			         "\"args\":{\"name\":\"Execution\"}}",
			         display_pid);
			EmitCommaIfNeeded(w, first_event);
			w.Printf("{\"name\":\"thread_name\",\"ph\":\"M\",\"pid\":%d,\"tid\":1,"
			         "\"args\":{\"name\":\"Idle\"}}",
			         display_pid);
			EmitCommaIfNeeded(w, first_event);
			w.Printf("{\"name\":\"thread_name\",\"ph\":\"M\",\"pid\":%d,\"tid\":2,"
			         "\"args\":{\"name\":\"Scheduler\"}}",
			         display_pid);
		}
	}

	/* 3. Stream all recorded events directly from each process's DSA buffer. */
	for (uint32_t slot = 0; slot < shared->num_slots && w.Ok(); ++slot)
	{
		YaapTraceBufferHeader *hdr = ResolveBufferHeader(shared, dsa, slot);
		const uint64_t count = std::min<uint64_t>(
			pg_atomic_read_u64(&hdr->event_count), hdr->capacity);
		if (count == 0)
			continue;

		const int32_t display_pid = DisplayPidForSlot(hdr, slot, shared->leader_pid);
		const YaapTraceEvent *events = ResolveBufferEvents(shared, dsa, slot);
		for (uint64_t i = 0; i < count && w.Ok(); ++i)
			EmitChromeEvent(w, first_event, events[i], display_pid, hdr->worker_id);
	}

	w.WriteStr("]}\n");
	const bool flushed = w.Flush();
	const uint64_t export_end_ns = YaapTraceNow();

	summary.total_events       = total_events;
	summary.dropped_events     = dropped_events;
	summary.query_duration_ns  = query_duration_ns;
	summary.export_duration_ns = export_end_ns >= export_begin_ns
		? (export_end_ns - export_begin_ns) : 0u;
	summary.abnormal_processes = abnormal_processes;
	summary.complete           = complete;
	summary.exported           = flushed;

	if (summary_out != nullptr)
		*summary_out = summary;
	return flushed;
}

YaapTraceExportSummary
YaapTraceCollectAndExport(PipelineSharedControl *control,
                          dsa_area *dsa,
                          bool query_succeeded)
{
	YaapTraceExportSummary summary{};

	/* Always detach leader before collecting so leader header state is finalized. */
	YaapTraceDetachProcess(!query_succeeded);

	YaapTraceSharedHeader *shared = ResolveSharedHeader(control, dsa);
	if (shared == nullptr || shared->reported != 0)
		return summary;
	shared->reported = 1;

	MemoryContext old_mcxt = CurrentMemoryContext;
	PG_TRY();
	{
		if (pg_yaap_trace_output_dir == nullptr || pg_yaap_trace_output_dir[0] == '\0')
		{
			elog(NOTICE,
			     "pg_yaap[trace] trace collected but not written: set pg_yaap.trace_output_dir to an absolute directory");
		}
		else if (!superuser() &&
		         !has_privs_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES))
		{
			elog(WARNING,
			     "pg_yaap[trace] trace file export requires superuser or pg_write_server_files");
		}
		else
		{
			char dir_clean[MAXPGPATH];
			strlcpy(dir_clean, pg_yaap_trace_output_dir, sizeof(dir_clean));
			size_t dir_len = std::strlen(dir_clean);
			while (dir_len > 1 && dir_clean[dir_len - 1] == '/')
				dir_clean[--dir_len] = '\0';

			char path[MAXPGPATH];
			std::snprintf(path, sizeof(path),
			              "%s/yaap_trace_%d_%llu_%llu.json",
			              dir_clean,
			              shared->leader_pid,
			              static_cast<unsigned long long>(shared->execution_id),
			              static_cast<unsigned long long>(shared->origin_ns));

			int fd = OpenTransientFilePerm(path,
			                               O_WRONLY | O_CREAT | O_EXCL | PG_BINARY,
			                               S_IRUSR | S_IWUSR);
			if (fd < 0)
			{
				elog(WARNING, "pg_yaap[trace] could not create trace file \"%s\": %m", path);
			}
			else
			{
				const bool write_ok = YaapTraceWriteChromeJsonToFd(
					control, dsa, query_succeeded, fd, &summary);
				int saved_errno = errno;
				int close_rc = CloseTransientFile(fd);
				if (!write_ok || close_rc != 0)
				{
					::unlink(path);
					errno = saved_errno;
					summary.exported = false;
					elog(WARNING, "pg_yaap[trace] failed to write trace file \"%s\": %m", path);
				}
				else
				{
					if (summary.dropped_events > 0)
					{
						elog(WARNING,
						     "pg_yaap[trace] buffer full: dropped %llu events; increase pg_yaap.trace_buffer_mb",
						     static_cast<unsigned long long>(summary.dropped_events));
					}
					elog(NOTICE,
					     "pg_yaap[trace] file=%s status=%s events=%llu dropped=%llu query_ms=%.3f export_ms=%.3f",
					     path,
					     summary.complete ? "complete" : "partial",
					     static_cast<unsigned long long>(summary.total_events),
					     static_cast<unsigned long long>(summary.dropped_events),
					     static_cast<double>(summary.query_duration_ns) / 1e6,
					     static_cast<double>(summary.export_duration_ns) / 1e6);
				}
			}
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(old_mcxt);
		FlushErrorState();
		elog(WARNING, "pg_yaap[trace] trace export aborted due to internal error");
	}
	PG_END_TRY();

	return summary;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
