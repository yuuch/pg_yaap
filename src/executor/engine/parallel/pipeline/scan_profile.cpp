#include "parallel/pipeline/scan_profile.hpp"
#include "core/dsa_allocation.hpp"
#include <algorithm>
#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/pipeline_profile.hpp"
extern "C"
{
#include "miscadmin.h"
#include "utils/elog.h"
	extern int pg_yaap_profile_scan;
}
namespace pg_yaap {
namespace pipeline {
namespace {
ScanProfileRecord*
Record(PipelineSharedControl* c, dsa_area* d, int w, EventId e)
{
	if (!c || !d || !c->scan_profile_mode || !DsaPointerIsValid(c->scan_profile_root) ||
		e >= c->profile_event_count)
		return nullptr;
	uint32 slot = w == LEADER_WORKER_INDEX ? 0 : static_cast<uint32>(w + 1);
	if (slot >= c->profile_worker_slots)
		return nullptr;
	auto* base = static_cast<char*>(dsa_get_address(d, c->scan_profile_root));
	auto address = reinterpret_cast<uintptr_t>(base);
	auto* records = reinterpret_cast<ScanProfileRecord*>(
	  (address + PG_CACHE_LINE_SIZE - 1) & ~static_cast<uintptr_t>(PG_CACHE_LINE_SIZE - 1));
	const Size index = static_cast<Size>(slot) * c->profile_event_count + e;
	return reinterpret_cast<ScanProfileRecord *>(reinterpret_cast<char *>(records) +
		index * CACHELINEALIGN(sizeof(ScanProfileRecord)));
}
void
Add(ScanProfileRecord& a, const ScanProfileRecord& b)
{
#define ADD(f) a.f += b.f
	ADD(source_ns);
	ADD(pg_access_ns);
	ADD(row_to_column_ns);
	ADD(predicate_ns);
	ADD(setup_ns);
	ADD(visible_rows);
	ADD(output_rows);
	ADD(samples);
	ADD(load_ns);
	ADD(prepare_ns);
	ADD(tuple_ns);
	ADD(qual_ns);
	ADD(proj_ns);
	ADD(calls);
	ADD(pages);
	ADD(qual_rows);
	ADD(proj_rows);
	ADD(qual_samples);
	ADD(proj_samples);
	ADD(read_submit_ns); ADD(read_wait_ns); ADD(read_refill_ns);
	ADD(read_requests); ADD(read_need_wait); ADD(read_requested_pages); ADD(read_accepted_pages);
	ADD(read_submit_samples);
	ADD(read_submit_sample_wall_ns);
	ADD(read_submit_sample_cpu_ns);
	ADD(read_wait_calls);
	ADD(read_wait_samples);
	ADD(read_wait_sample_wall_ns);
	ADD(read_wait_sample_cpu_ns);
	ADD(read_cpu_clock_failures);
	ADD(read_refill_calls);
	ADD(read_pin_limit_stops);
	ADD(read_slot_limit_stops);
	ADD(read_split_requests);
	ADD(read_accepted_single);
	ADD(read_accepted_multi);
	ADD(read_request_limit_stops); ADD(read_new_io_burst_stops);
	ADD(read_ready_pages); ADD(shared_blks_hit); ADD(shared_blks_read);
	a.read_peak_inflight = std::max(a.read_peak_inflight, b.read_peak_inflight);
#undef ADD
}
void
Print(const char* level, uint32 pipeline, int worker, const ScanProfileRecord& r, bool success)
{
	const double ms = 1.0 / 1000000.0;
	const uint64 accounted = r.pg_access_ns + r.row_to_column_ns + r.predicate_ns + r.setup_ns;
	elog(
	  NOTICE,
	  "pg_yaap[scan] %s pipeline=%u worker=%d pid=%u operator=%u relation=%u mode=%s complete=%s "
	  "calls=%llu source_ms=%.3f pg_access_ms=%.3f row_to_column_ms=%.3f predicate_ms=%.3f "
	  "setup_ms=%.3f pg_input_ms=%.3f other_ms=%.3f overestimate_ms=%.3f pages=%llu visible_rows=%llu "
	  "output_rows=%llu samples=%llu qual_rows=%llu proj_rows=%llu qual_samples=%llu "
	  "proj_samples=%llu sample_clock_overhead_ns=%llu load_ms=%.3f prepare_ms=%.3f tuple_ms=%.3f qual_ms=%.3f proj_ms=%.3f",
	  level,
	  pipeline,
	  worker,
	  r.pid,
	  r.operator_id,
	  r.relation_oid,
	  r.mode == 1 ? "sample-estimated" : "exact",
	  success && !r.pid_conflict ? "yes" : "no",
	  (unsigned long long)r.calls,
	  r.source_ns * ms,
	  r.pg_access_ns * ms,
	  r.row_to_column_ns * ms,
	  r.predicate_ns * ms,
	  r.setup_ns * ms,
	  (r.pg_access_ns + r.row_to_column_ns + r.setup_ns) * ms,
	  (r.source_ns > accounted ? r.source_ns - accounted : 0) * ms,
	  (accounted > r.source_ns ? accounted - r.source_ns : 0) * ms,
	  (unsigned long long)r.pages,
	  (unsigned long long)r.visible_rows,
	  (unsigned long long)r.output_rows,
	  (unsigned long long)r.samples,
	  (unsigned long long)r.qual_rows,
	  (unsigned long long)r.proj_rows,
	  (unsigned long long)r.qual_samples,
	  (unsigned long long)r.proj_samples,
	  (unsigned long long)r.sample_clock_overhead_ns,
	  r.load_ns * ms,
	  r.prepare_ns * ms,
	  r.tuple_ns * ms,
	  r.qual_ns * ms,
	  r.proj_ns * ms);
	if (r.read_stream_mode)
		elog(NOTICE, "pg_yaap[read-stream] %s pipeline=%u worker=%d mode=yaap window=%u batch=%u submit_ms=%.3f wait_api_ms=%.3f refill_exclusive_ms=%.3f requests=%llu need_wait=%llu requested_pages=%llu accepted_pages=%llu",
			level, pipeline, worker, r.read_window_pages, r.read_batch_pages,
			r.read_submit_ns * ms, r.read_wait_ns * ms, r.read_refill_ns * ms,
			(unsigned long long) r.read_requests, (unsigned long long) r.read_need_wait,
			(unsigned long long) r.read_requested_pages, (unsigned long long) r.read_accepted_pages);
    if (r.read_stream_mode) {
        elog(NOTICE, "pg_yaap[read-stream-cpu] %s pipeline=%u worker=%d submit_samples=%llu submit_sample_wall_ms=%.3f submit_sample_cpu_ms=%.3f wait_calls=%llu wait_samples=%llu wait_sample_wall_ms=%.3f wait_sample_cpu_ms=%.3f cpu_clock_failures=%llu",
            level, pipeline, worker,
            (unsigned long long)r.read_submit_samples, r.read_submit_sample_wall_ns*ms, r.read_submit_sample_cpu_ns*ms,
            (unsigned long long)r.read_wait_calls, (unsigned long long)r.read_wait_samples,
            r.read_wait_sample_wall_ns*ms, r.read_wait_sample_cpu_ns*ms, (unsigned long long)r.read_cpu_clock_failures);
        elog(NOTICE, "pg_yaap[read-stream-queue] %s pipeline=%u worker=%d refills=%llu pin_limit_stops=%llu slot_limit_stops=%llu split_requests=%llu accepted_single=%llu accepted_multi=%llu max_queue_inflight=%llu ready_pages=%llu request_limit=%u request_limit_stops=%llu burst_stops=%llu",
            level,pipeline,worker,(unsigned long long)r.read_refill_calls,
            (unsigned long long)r.read_pin_limit_stops,(unsigned long long)r.read_slot_limit_stops,
            (unsigned long long)r.read_split_requests,(unsigned long long)r.read_accepted_single,
            (unsigned long long)r.read_accepted_multi,(unsigned long long)r.read_peak_inflight,
            (unsigned long long)r.read_ready_pages, r.read_request_limit,
            (unsigned long long)r.read_request_limit_stops,(unsigned long long)r.read_new_io_burst_stops);
    }
    const uint64 buffer_refs = r.shared_blks_hit + r.shared_blks_read;
    elog(NOTICE, "pg_yaap[scan-buffer] %s pipeline=%u worker=%d shared_hits=%llu shared_reads=%llu hit_percent=%.3f",
        level,pipeline,worker,(unsigned long long)r.shared_blks_hit,(unsigned long long)r.shared_blks_read,
        buffer_refs ? 100.0*r.shared_blks_hit/buffer_refs : -1.0);

}
}
bool
ScanProfileAllocate(PipelineSharedControl* c, dsa_area* d, uint32 events, uint32 workers)
{
	c->scan_profile_mode = pg_yaap_profile_scan;
	c->scan_profile_root = InvalidDsaPointer;
	c->scan_profile_reported = 0;
	if (!c->scan_profile_mode || !events)
		return true;
	Size n = static_cast<Size>(workers + 1) * events;
	constexpr Size max_bytes = 16 * 1024 * 1024;
	if (n > (max_bytes - PG_CACHE_LINE_SIZE + 1) / CACHELINEALIGN(sizeof(ScanProfileRecord))) {
		c->scan_profile_mode = 0;
		ereport(WARNING,
				(errmsg("pg_yaap: scan profile disabled: exceeds 16 MiB bounded allocation")));
		return true;
	}
	c->scan_profile_root = YaapDsaAllocate(
	  d, n * CACHELINEALIGN(sizeof(ScanProfileRecord)) + PG_CACHE_LINE_SIZE - 1, DSA_ALLOC_ZERO | DSA_ALLOC_NO_OOM);
	if (!DsaPointerIsValid(c->scan_profile_root)) {
		c->scan_profile_mode = 0;
		ereport(WARNING, (errmsg("pg_yaap: scan profile disabled: insufficient DSA capacity")));
	}
	return true;
}
int
ScanProfileMode(const ExecCtx& c)
{
	return c.control ? c.control->scan_profile_mode : 0;
}
uint64
ScanProfileElapsed(instr_time e, instr_time s)
{
	INSTR_TIME_SUBTRACT(e, s);
	return static_cast<uint64>(INSTR_TIME_GET_NANOSEC(e));
}
ScanProfileRecord
ScanProfileSnapshot(PipelineSharedControl* c, dsa_area* d, int w, EventId e)
{
	auto* r = Record(c, d, w, e);
	return r ? *r : ScanProfileRecord{};
}
void
ScanProfilePublish(const ExecCtx& c, const ScanProfileRecord& v)
{
	auto* r = Record(c.control, c.dsa, c.worker_index, c.profile_event_id);
	if (!r)
		return;
	if (r->pid && r->pid != static_cast<uint32>(MyProcPid)) {
		r->pid_conflict = 1;
		return;
	}
	Add(*r, v);
	r->operator_id = v.operator_id;
	r->relation_oid = v.relation_oid;
	r->mode = v.mode;
	r->pid = MyProcPid;
	r->sample_clock_overhead_ns = v.sample_clock_overhead_ns;
	r->read_window_pages = v.read_window_pages;
	r->read_request_limit = v.read_request_limit;
	r->read_batch_pages = v.read_batch_pages;
	r->read_stream_mode = v.read_stream_mode;
}
static void
ReportInternal(PipelineSharedControl* c, dsa_area* d, bool success)
{
	if (!c || !d || !c->scan_profile_mode || c->scan_profile_reported)
		return;
	c->scan_profile_reported = 1;
	elog(NOTICE,
		 "pg_yaap[scan] complete=%s elapsed worker sums, not query wall/CPU time; load includes buffer API "
		 "waits, not physical IO; sample tuple times estimated at 1/64 after empty-clock calibration, other is residual "
		 "estimate; zero samples means unknown, not zero cost; canceled/error GetData may be unpublished",
		 success ? "yes" : "no");
	ScanProfileRecord query{};
	query.mode = c->scan_profile_mode;
	for (uint32 e = 0; e < c->profile_event_count; e++) {
		ScanProfileRecord total{};
		total.mode = c->scan_profile_mode;
		for (uint32 s = 0; s < c->profile_worker_slots; s++) {
			int w = s ? static_cast<int>(s - 1) : LEADER_WORKER_INDEX;
			auto r = ScanProfileSnapshot(c, d, w, e);
			if (!r.calls)
				continue;
			Print("worker", e / 3, w, r, success);
			Add(total, r);
			total.pid_conflict |= r.pid_conflict;
			total.operator_id = r.operator_id;
			total.relation_oid = r.relation_oid;
			total.read_stream_mode = r.read_stream_mode;
		}
		if (total.calls) {
			char *label = PipelineProfileEventLabel(c, d, e);
			elog(NOTICE, "pg_yaap[scan] label pipeline=%u %s", e / 3, label);
			pfree(label);
			Print("pipeline", e / 3, -2, total, success);
			Add(query, total);
			query.pid_conflict |= total.pid_conflict;
			query.read_stream_mode = total.read_stream_mode;
		}
	}
	if (query.calls)
		Print("query", UINT32_MAX, -2, query, success);
}
void
ScanProfileReport(PipelineSharedControl* c, dsa_area* d, bool success)
{
	MemoryContext saved_context = CurrentMemoryContext;
	PG_TRY();
	{
		ReportInternal(c, d, success);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(saved_context);
		FlushErrorState();
	}
	PG_END_TRY();
}
}
}
