#include "parallel/pipeline/pipeline_timeline.hpp"

extern "C" {
#include "catalog/pg_authid_d.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/elog.h"
#include "utils/json.h"
#include "utils/memutils.h"
extern int pg_yaap_profile_timeline;
extern int pg_yaap_profile_trace_capacity;
extern char *pg_yaap_profile_trace_file;
}

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/pipeline_profile.hpp"

namespace pg_yaap {
namespace pipeline {
namespace {

constexpr uint64 kMemoryLimit = UINT64CONST(16) * 1024 * 1024;
constexpr uint32 kPhases = 3;

// All clocks use PG's system-wide monotonic clock (MONOTONIC_RAW on macOS).
uint64 ClockNs() {
	instr_time now;
	INSTR_TIME_SET_CURRENT(now);
	return static_cast<uint64>(INSTR_TIME_GET_NANOSEC(now));
}

struct TimelineStats {
	uint64 first_start_ns, last_end_ns, active_ns, task_count;
	uint64 yielded, errors, cancelled, reserved;
};
// Records share lines only within the same writer's region. Region starts and
// strides below are aligned to PG_CACHE_LINE_SIZE (128 bytes on this build).
static_assert(sizeof(TimelineStats) == 64, "Unexpected timeline statistics layout");

struct TaskTraceEvent {
	uint64 start_ns, end_ns, task_id;
	uint32 event_id, partition_id;
	uint32 result, reserved;
};

struct TimelineSlot {
	uint64 start_ns, task_id, fragments, trace_count, dropped_events;
	uint32 event_id, partition_id;
	int32 pid, worker_id, conflicting_pid;
	pg_atomic_uint32 active;
	pg_atomic_uint32 updating;
	uint32 nested_starts;
};

struct TimelinePhase {
	uint64 ready_ns, finished_ns;
	uint32 aborted, reserved;
};

struct TimelineShared {
	uint64 origin_ns;
	uint32 slots, pipelines, capacity, mode;
	uint64 stats_stride, slot_stride, trace_stride;
	dsa_pointer stats_dp, slots_dp, trace_dp, phases_dp;
	bool reported;
};

uint32 WorkerSlot(int worker) { return worker < 0 ? 0u : static_cast<uint32>(worker + 1); }

TimelineShared *Shared(PipelineSharedControl *control, dsa_area *dsa) {
	if (!control || !dsa || !control->timeline_mode || !DsaPointerIsValid(control->timeline_root)) return nullptr;
	return static_cast<TimelineShared *>(dsa_get_address(dsa, control->timeline_root));
}

// DSA guarantees MAXALIGN, not cache-line alignment. Align the payload within
// a padded allocation; DSM mappings are page aligned in every process.
dsa_pointer AlignedAllocate(dsa_area *dsa, uint64 bytes) {
	dsa_pointer raw = dsa_allocate0(dsa, static_cast<Size>(bytes + PG_CACHE_LINE_SIZE));
	uintptr_t address = reinterpret_cast<uintptr_t>(dsa_get_address(dsa, raw));
	return raw + ((-address) & (PG_CACHE_LINE_SIZE - 1));
}

TimelineSlot *Slot(TimelineShared *shared, dsa_area *dsa, uint32 slot) {
	return reinterpret_cast<TimelineSlot *>(static_cast<char *>(dsa_get_address(dsa, shared->slots_dp)) +
		shared->slot_stride * slot);
}

TimelineStats *Stats(TimelineShared *shared, dsa_area *dsa, uint32 slot, uint32 event) {
	return reinterpret_cast<TimelineStats *>(static_cast<char *>(dsa_get_address(dsa, shared->stats_dp)) +
		shared->stats_stride * slot) + event;
}

TaskTraceEvent *Trace(TimelineShared *shared, dsa_area *dsa, uint32 slot) {
	return reinterpret_cast<TaskTraceEvent *>(static_cast<char *>(dsa_get_address(dsa, shared->trace_dp)) +
		shared->trace_stride * slot);
}

uint64 Relative(TimelineShared *shared) { return std::max(ClockNs(), shared->origin_ns) - shared->origin_ns + 1; }
const char *Kind(uint32 event) { return event % 3 == 0 ? "run" : event % 3 == 1 ? "combine" : "finalize"; }
const char *Outcome(uint32 result) {
	switch (static_cast<TimelineTaskResult>(result)) {
		case TimelineTaskResult::FINISHED: return "finished";
		case TimelineTaskResult::YIELDED: return "yielded";
		case TimelineTaskResult::FAILED: return "error";
		case TimelineTaskResult::CANCELLED: return "cancelled";
	}
	return "incomplete";
}

bool Begin(const ExecCtx &ctx, EventId event, uint32 partition) {
	auto *shared = Shared(ctx.control, ctx.dsa);
	if (!shared || event >= shared->pipelines * kPhases) return false;
	uint32 slot_id = WorkerSlot(ctx.worker_index);
	if (slot_id >= shared->slots) return false;
	auto *slot = Slot(shared, ctx.dsa, slot_id);
	if (slot->pid != MyProcPid || slot->conflicting_pid) return false;
	if (pg_atomic_read_u32(&slot->active)) { ++slot->nested_starts; return false; }
	slot->event_id = event;
	slot->partition_id = partition;
	slot->task_id = (static_cast<uint64>(event) << 32) | (partition == UINT32_MAX ? slot_id : partition);
	slot->start_ns = Relative(shared);
	pg_write_barrier();
	pg_atomic_write_u32(&slot->active, 1);
	return true;
}

void End(PipelineSharedControl *control, dsa_area *dsa, int worker, TimelineTaskResult result) {
	auto *shared = Shared(control, dsa);
	if (!shared || WorkerSlot(worker) >= shared->slots) return;
	uint32 slot_id = WorkerSlot(worker);
	auto *slot = Slot(shared, dsa, slot_id);
	if (slot->pid != MyProcPid || !pg_atomic_read_u32(&slot->active)) return;
	uint64 end = std::max(Relative(shared), slot->start_ns);
	pg_atomic_write_u32(&slot->updating, 1);
	pg_write_barrier();
	auto *stats = Stats(shared, dsa, slot_id, slot->event_id);
	if (!stats->task_count) stats->first_start_ns = slot->start_ns;
	stats->last_end_ns = end;
	stats->active_ns += end - slot->start_ns;
	++stats->task_count;
	stats->yielded += result == TimelineTaskResult::YIELDED;
	stats->errors += result == TimelineTaskResult::FAILED;
	stats->cancelled += result == TimelineTaskResult::CANCELLED;
	if (shared->mode == 2) {
		if (slot->trace_count < shared->capacity) {
			auto &trace = Trace(shared, dsa, slot_id)[slot->trace_count];
			trace = {slot->start_ns, end, slot->task_id, slot->event_id, slot->partition_id,
				static_cast<uint32>(result), 0};
			pg_write_barrier();
			++slot->trace_count;
		} else ++slot->dropped_events;
	}
	++slot->fragments;
	pg_write_barrier();
	pg_atomic_write_u32(&slot->active, 0);
	pg_atomic_write_u32(&slot->updating, 0);
}

struct WorkerTotals {
	uint64 first = 0, last = 0, active = 0, tasks = 0, run = 0, combine = 0;
	uint64 finalize_first = 0, finalize_last = 0, finalize_active = 0, finalize_tasks = 0;
};

uint64 MinStart(uint64 a, uint64 b) { return a == 0 ? b : b == 0 ? a : std::min(a, b); }

WorkerTotals Totals(TimelineShared *shared, dsa_area *dsa, uint32 slot_id, uint32 pipeline) {
	WorkerTotals out;
	// A killed process may have died halfway through publishing one update.
	// Do not report a torn record as trustworthy timing data.
	auto *slot = Slot(shared, dsa, slot_id);
	if (pg_atomic_read_u32(&slot->updating)) return out;
	for (uint32 phase = 0; phase < kPhases; ++phase) {
		auto *s = Stats(shared, dsa, slot_id, pipeline * kPhases + phase);
		if (phase == 2) {
			out.finalize_first = s->first_start_ns; out.finalize_last = s->last_end_ns;
			out.finalize_active = s->active_ns; out.finalize_tasks = s->task_count;
		} else {
			out.first = MinStart(out.first, s->first_start_ns); out.last = std::max(out.last, s->last_end_ns);
			out.active += s->active_ns; out.tasks += s->task_count;
			(phase == 0 ? out.run : out.combine) = s->active_ns;
		}
	}
	return out;
}

struct ExportEvent { uint64 start; uint32 slot, type; uint64 index; };
int CompareEvents(const void *a, const void *b) {
	auto &x = *static_cast<const ExportEvent *>(a), &y = *static_cast<const ExportEvent *>(b);
	if (x.start != y.start) return x.start < y.start ? -1 : 1;
	if (x.slot != y.slot) return x.slot < y.slot ? -1 : 1;
	return x.type < y.type ? -1 : x.type > y.type;
}

void ExportFile(StringInfo json, PipelineSharedControl *control, TimelineShared *shared) {
	if (!pg_yaap_profile_trace_file || !pg_yaap_profile_trace_file[0]) {
		elog(NOTICE, "pg_yaap[timeline] JSON export disabled: set pg_yaap.profile_trace_file to an absolute prefix");
		return;
	}
	if (!superuser() && !has_privs_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES)) {
		elog(WARNING, "pg_yaap: timeline JSON export requires superuser or pg_write_server_files");
		return;
	}
	char path[MAXPGPATH];
	snprintf(path, sizeof(path), "%s.%d." UINT64_FORMAT ".json",
		pg_yaap_profile_trace_file, control->leader_pid, shared->origin_ns);
	int fd = OpenTransientFilePerm(path, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, S_IRUSR | S_IWUSR);
	if (fd < 0) { elog(WARNING, "pg_yaap: could not create timeline file \"%s\": %m", path); return; }
	int written = 0;
	while (written < json->len) {
		ssize_t n = write(fd, json->data + written, json->len - written);
		if (n < 0 && errno == EINTR) continue;
		if (n <= 0) break;
		written += n;
	}
	int saved_errno = errno;
	int close_result = CloseTransientFile(fd);
	if (written != json->len || close_result) {
		unlink(path);
		errno = saved_errno;
		elog(WARNING, "pg_yaap: could not finish timeline file \"%s\": %m", path);
	} else elog(NOTICE, "pg_yaap[timeline] trace_file=%s", path);
}

void Report(PipelineSharedControl *control, dsa_area *dsa, TimelineShared *shared, bool success) {
	uint64 end = Relative(shared), dropped = 0, incomplete = 0;
	for (uint32 i = 0; i < shared->slots; ++i) {
		auto *slot = Slot(shared, dsa, i);
		dropped += slot->dropped_events;
		incomplete += pg_atomic_read_u32(&slot->active) != 0 || pg_atomic_read_u32(&slot->updating) != 0;
		if (slot->conflicting_pid || slot->nested_starts) success = false;
	}
	if (incomplete) success = false;
	elog(NOTICE, "pg_yaap[timeline] query status=%s total_ms=%.3f slots=%u trace_capacity=%u dropped_events=" UINT64_FORMAT " incomplete_tasks=" UINT64_FORMAT,
		success ? "complete" : "partial", end / 1e6, shared->slots, shared->capacity, dropped, incomplete);
	StringInfoData json;
	initStringInfo(&json);
	appendStringInfo(&json, "{\"displayTimeUnit\":\"ms\",\"yaap\":{\"schema_version\":1,\"complete\":%s,\"query_origin_ns\":" UINT64_FORMAT ",\"query_end_ns\":" UINT64_FORMAT ",\"dropped_events\":" UINT64_FORMAT ",\"incomplete_tasks\":" UINT64_FORMAT ",\"trace_capacity_per_worker\":%u,\"pipelines\":[",
		success ? "true" : "false", shared->origin_ns, end, dropped, incomplete, shared->capacity);
	auto *phases = static_cast<TimelinePhase *>(dsa_get_address(dsa, shared->phases_dp));
	auto *descriptors = static_cast<PipelineDescriptor *>(dsa_get_address(dsa, control->pipelines_root));
	for (uint32 p = 0; p < shared->pipelines; ++p) {
		WorkerTotals total;
		uint32 workers = 0;
		uint64 min_active = UINT64_MAX, max_active = 0;
		for (uint32 slot = 0; slot < shared->slots; ++slot) {
			auto t = Totals(shared, dsa, slot, p);
			if (t.tasks) {
				++workers; min_active = std::min(min_active, t.active); max_active = std::max(max_active, t.active);
			}
			total.first = MinStart(total.first, t.first); total.last = std::max(total.last, t.last);
			total.active += t.active; total.tasks += t.tasks;
			total.finalize_first = MinStart(total.finalize_first, t.finalize_first);
			total.finalize_last = std::max(total.finalize_last, t.finalize_last);
			total.finalize_active += t.finalize_active; total.finalize_tasks += t.finalize_tasks;
		}
		uint64 span = total.last >= total.first ? total.last - total.first : 0;
		double avg = workers ? static_cast<double>(total.active) / workers : 0;
		char *label = PipelineProfileEventLabel(control, dsa, p * kPhases);
		elog(NOTICE, "pg_yaap[timeline] %s start_ms=%.3f end_ms=%.3f wall_ms=%.3f active_ms=%.3f tasks=" UINT64_FORMAT " workers=%u finalize_start_ms=%.3f finalize_end_ms=%.3f finalize_active_ms=%.3f min_ms=%.3f max_ms=%.3f avg_ms=%.3f max_avg=%.3f",
			label, total.first / 1e6, total.last / 1e6, span / 1e6, total.active / 1e6, total.tasks, workers,
			total.finalize_first / 1e6, total.finalize_last / 1e6, total.finalize_active / 1e6,
			workers ? min_active / 1e6 : 0, max_active / 1e6, avg / 1e6, avg ? max_active / avg : 0);
		if (p) appendStringInfoChar(&json, ',');
		appendStringInfo(&json, "{\"pipeline_id\":%u,\"label\":", p); escape_json(&json, label); pfree(label);
		appendStringInfo(&json, ",\"first_start_ns\":" UINT64_FORMAT ",\"last_end_ns\":" UINT64_FORMAT ",\"wall_span_ns\":" UINT64_FORMAT ",\"worker_count\":%u,\"total_worker_active_ns\":" UINT64_FORMAT ",\"task_count\":" UINT64_FORMAT ",\"finalize_start_ns\":" UINT64_FORMAT ",\"finalize_end_ns\":" UINT64_FORMAT ",\"finalize_active_ns\":" UINT64_FORMAT ",\"ready_ns\":" UINT64_FORMAT ",\"finished_ns\":" UINT64_FORMAT ",\"aborted\":%s,\"dependencies\":[",
			total.first, total.last, span, workers, total.active, total.tasks, total.finalize_first, total.finalize_last,
			total.finalize_active, phases[p].ready_ns, phases[p].finished_ns, phases[p].aborted ? "true" : "false");
		const auto &pd = descriptors[p];
		bool first_dependency = true;
		for (uint32 j = 0; j < std::min<uint32>(64, shared->pipelines); ++j) {
			if (!(pd.dependency_mask & (UINT64CONST(1) << j))) continue;
			appendStringInfo(&json, "%s%u", first_dependency ? "" : ",", j);
			first_dependency = false;
		}
		appendStringInfoString(&json, "],\"workers\":[");
		bool first_worker = true;
		for (uint32 slot_id = 0; slot_id < shared->slots; ++slot_id) {
			auto t = Totals(shared, dsa, slot_id, p);
			auto *slot = Slot(shared, dsa, slot_id);
			if (!t.tasks && !t.finalize_tasks) continue;
			elog(NOTICE, "pg_yaap[timeline] pipeline=%u slot=%u worker_id=%d pid=%d start_ms=%.3f end_ms=%.3f active_ms=%.3f tasks=" UINT64_FORMAT " run_ms=%.3f combine_ms=%.3f finalize_ms=%.3f finalize_tasks=" UINT64_FORMAT,
				p, slot_id, slot->worker_id, slot->pid, t.first / 1e6, t.last / 1e6, t.active / 1e6,
				t.tasks, t.run / 1e6, t.combine / 1e6, t.finalize_active / 1e6, t.finalize_tasks);
			if (!first_worker) appendStringInfoChar(&json, ','); first_worker = false;
			appendStringInfo(&json, "{\"slot_id\":%u,\"worker_id\":%d,\"pid\":%d,\"first_start_ns\":" UINT64_FORMAT ",\"last_end_ns\":" UINT64_FORMAT ",\"active_ns\":" UINT64_FORMAT ",\"task_count\":" UINT64_FORMAT ",\"run_active_ns\":" UINT64_FORMAT ",\"combine_active_ns\":" UINT64_FORMAT ",\"finalize_start_ns\":" UINT64_FORMAT ",\"finalize_end_ns\":" UINT64_FORMAT ",\"finalize_active_ns\":" UINT64_FORMAT ",\"finalize_task_count\":" UINT64_FORMAT "}",
				slot_id, slot->worker_id, slot->pid, t.first, t.last, t.active, t.tasks, t.run, t.combine,
				t.finalize_first, t.finalize_last, t.finalize_active, t.finalize_tasks);
		}
		appendStringInfoString(&json, "]}");
	}
	appendStringInfoString(&json, "]},\"traceEvents\":[");
	if (shared->mode == 2) {
		for (uint32 slot_id = 0; slot_id < shared->slots; ++slot_id) {
			auto *slot = Slot(shared, dsa, slot_id);
			if (slot_id) appendStringInfoChar(&json, ',');
			appendStringInfo(&json, "{\"name\":\"thread_name\",\"ph\":\"M\",\"pid\":%d,\"tid\":%u,\"args\":{\"name\":\"%s %d (pid %d)\"}}",
				control->leader_pid, slot_id, slot_id ? "Worker" : "Leader", slot->worker_id, slot->pid);
		}
		uint64 count = 0;
		for (uint32 s = 0; s < shared->slots; ++s) count += Slot(shared, dsa, s)->trace_count + 1;
		count += 2u * shared->pipelines;
		auto *events = static_cast<ExportEvent *>(palloc(sizeof(ExportEvent) * count));
		uint64 n = 0;
		for (uint32 s = 0; s < shared->slots; ++s) {
			auto *slot = Slot(shared, dsa, s);
			for (uint64 i = 0; i < std::min<uint64>(slot->trace_count, shared->capacity); ++i)
				events[n++] = {Trace(shared, dsa, s)[i].start_ns, s, 0, i};
			if (pg_atomic_read_u32(&slot->active)) events[n++] = {slot->start_ns, s, 3, 0};
		}
		for (uint32 p = 0; p < shared->pipelines; ++p) {
			if (phases[p].ready_ns) events[n++] = {phases[p].ready_ns, 0, 1, p};
			if (phases[p].finished_ns) events[n++] = {phases[p].finished_ns, 0, 2, p};
		}
		qsort(events, n, sizeof(ExportEvent), CompareEvents);
		for (uint64 i = 0; i < n; ++i) {
			auto &e = events[i]; auto *slot = Slot(shared, dsa, e.slot);
			appendStringInfoChar(&json, ',');
			if (e.type == 0) {
				auto &t = Trace(shared, dsa, e.slot)[e.index];
				appendStringInfo(&json, "{\"name\":\"Pipeline %u / %s\",\"cat\":\"pipeline\",\"ph\":\"X\",\"pid\":%d,\"tid\":%u,\"ts\":%.3f,\"dur\":%.3f,\"args\":{\"pipeline_id\":%u,\"worker_id\":%d,\"os_pid\":%d,\"event_id\":%u,\"partition_id\":%u,\"task_id\":\"" UINT64_FORMAT "\",\"result\":\"%s\"}}",
					t.event_id / kPhases, Kind(t.event_id), control->leader_pid, e.slot, t.start_ns / 1000.0,
					(t.end_ns - t.start_ns) / 1000.0, t.event_id / kPhases, slot->worker_id, slot->pid,
					t.event_id, t.partition_id, t.task_id, Outcome(t.result));
			} else {
				uint32 p = e.type == 3 ? slot->event_id / kPhases : e.index;
				appendStringInfo(&json, "{\"name\":\"Pipeline %u / %s\",\"cat\":\"pipeline\",\"ph\":\"i\",\"s\":\"t\",\"pid\":%d,\"tid\":%u,\"ts\":%.3f,\"args\":{\"pipeline_id\":%u,\"os_pid\":%d}}",
					p, e.type == 1 ? "ready" : e.type == 2 ? "finished" : "incomplete", control->leader_pid, e.slot, e.start / 1000.0, p, slot->pid);
			}
		}
		pfree(events);
	}
	appendStringInfoString(&json, "]}");
	if (shared->mode == 2) ExportFile(&json, control, shared);
	pfree(json.data);
}

} // namespace

void PipelineTimelineAllocate(PipelineSharedControl *control, dsa_area *dsa, uint32 events, uint32 workers) {
	control->timeline_mode = 0; control->timeline_root = InvalidDsaPointer;
	if (!pg_yaap_profile_timeline || !events) return;
	uint32 slots = workers + 1, pipelines = events / kPhases;
	uint64 stats_stride = CACHELINEALIGN(static_cast<uint64>(events) * sizeof(TimelineStats));
	uint64 slot_stride = CACHELINEALIGN(sizeof(TimelineSlot));
	uint64 base_bytes = slots * (stats_stride + slot_stride) + pipelines * sizeof(TimelinePhase);
	if (base_bytes > kMemoryLimit) { elog(WARNING, "pg_yaap: timeline metadata exceeds 16MB; timeline disabled"); return; }
	uint32 capacity = pg_yaap_profile_timeline == 2 ? std::min<uint64>(pg_yaap_profile_trace_capacity,
		(kMemoryLimit - base_bytes) / (slots * sizeof(TaskTraceEvent) + slots * PG_CACHE_LINE_SIZE)) : 0;
	auto root = dsa_allocate0(dsa, sizeof(TimelineShared));
	auto *shared = static_cast<TimelineShared *>(dsa_get_address(dsa, root));
	shared->origin_ns = ClockNs(); shared->slots = slots; shared->pipelines = pipelines;
	shared->mode = pg_yaap_profile_timeline; shared->capacity = capacity;
	shared->stats_stride = stats_stride; shared->slot_stride = slot_stride;
	shared->trace_stride = CACHELINEALIGN(static_cast<uint64>(capacity) * sizeof(TaskTraceEvent));
	shared->stats_dp = AlignedAllocate(dsa, slots * stats_stride);
	shared->slots_dp = AlignedAllocate(dsa, slots * slot_stride);
	shared->trace_dp = capacity ? AlignedAllocate(dsa, slots * shared->trace_stride) : InvalidDsaPointer;
	shared->phases_dp = dsa_allocate0(dsa, std::max<Size>(1, pipelines * sizeof(TimelinePhase)));
	for (uint32 i = 0; i < slots; ++i) {
		auto *slot = Slot(shared, dsa, i); slot->worker_id = static_cast<int>(i) - 1;
		pg_atomic_init_u32(&slot->active, 0); pg_atomic_init_u32(&slot->updating, 0);
	}
	control->timeline_root = root; control->timeline_mode = shared->mode;
}

void PipelineTimelineRegisterProcess(PipelineSharedControl *control, dsa_area *dsa, int worker, int pid) {
	auto *shared = Shared(control, dsa);
	if (!shared || WorkerSlot(worker) >= shared->slots) return;
	auto *slot = Slot(shared, dsa, WorkerSlot(worker));
	if (slot->pid && slot->pid != pid) { slot->conflicting_pid = pid; return; }
	slot->pid = pid; slot->worker_id = worker;
	// Resolve these DSM mappings during registration rather than a timed Task
	// or a PG_CATCH closing its first fragment.
	(void) dsa_get_address(dsa, shared->stats_dp);
	if (shared->capacity) (void) dsa_get_address(dsa, shared->trace_dp);
}

PipelineTimelineTaskScope::PipelineTimelineTaskScope(const ExecCtx &ctx, EventId event, uint32 partition)
	: ctx_(ctx), active_(Begin(ctx, event, partition)) {}
PipelineTimelineTaskScope::~PipelineTimelineTaskScope() { if (active_) End(ctx_.control, ctx_.dsa, ctx_.worker_index, result_); }

void PipelineTimelineAbortProcess(PipelineSharedControl *control, dsa_area *dsa, int worker, bool cancelled) {
	End(control, dsa, worker, cancelled ? TimelineTaskResult::CANCELLED : TimelineTaskResult::FAILED);
}

void PipelineTimelineReady(PipelineSharedControl *control, dsa_area *dsa, uint32 pipeline) {
	auto *shared = Shared(control, dsa); if (!shared || pipeline >= shared->pipelines) return;
	auto *phase = static_cast<TimelinePhase *>(dsa_get_address(dsa, shared->phases_dp)) + pipeline;
	if (!phase->ready_ns) phase->ready_ns = Relative(shared);
}

void PipelineTimelineFinished(PipelineSharedControl *control, dsa_area *dsa, uint32 pipeline, bool aborted) {
	auto *shared = Shared(control, dsa); if (!shared || pipeline >= shared->pipelines) return;
	auto *phase = static_cast<TimelinePhase *>(dsa_get_address(dsa, shared->phases_dp)) + pipeline;
	if (!phase->finished_ns) phase->finished_ns = Relative(shared);
	phase->aborted |= aborted;
}

void PipelineTimelineReport(PipelineSharedControl *control, dsa_area *dsa, bool success) {
	auto *shared = Shared(control, dsa); if (!shared || shared->reported) return;
	shared->reported = true;
	// Profiling must not replace a query error (or fail an otherwise successful
// query) because a diagnostic allocation or export failed.
	MemoryContext old = CurrentMemoryContext;
	PG_TRY();
	{
		Report(control, dsa, shared, success);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(old);
		FlushErrorState();
		elog(WARNING, "pg_yaap: timeline report could not be completed");
	}
	PG_END_TRY();
}

} // namespace pipeline
} // namespace pg_yaap
