#include "parallel/pipeline/scan_io_profile.hpp"
#include "core/dsa_allocation.hpp"
#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/pipeline_profile.hpp"
#include "parallel/pipeline/scan_io_diagnosis.hpp"
#include "parallel/pipeline/scan_profile.hpp"
#include <algorithm>
#include <new>
extern "C" {
#include "miscadmin.h"
#include "utils/elog.h"
#include "utils/json.h"
extern int pg_yaap_profile_scan_io;
extern int pg_yaap_profile_scan_io_wait_pct;
extern int pg_yaap_profile_scan_io_min_pages;
}
namespace pg_yaap {
namespace pipeline {
namespace {
constexpr uint64 kMaxBytes = 16 * 1024 * 1024;
ScanIORecord *Record(PipelineSharedControl *c, dsa_area *d, int worker, uint32 event) {
	if (!c || !d || !c->scan_io_profile_mode || !DsaPointerIsValid(c->scan_io_profile_root) ||
		event >= c->profile_event_count)
		return nullptr;
	const uint32 slot = worker < 0 ? 0 : static_cast<uint32>(worker + 1);
	if (slot >= c->profile_worker_slots)
		return nullptr;
	char *base = static_cast<char *>(dsa_get_address(d, c->scan_io_profile_root));
	uintptr_t addr = (reinterpret_cast<uintptr_t>(base) + PG_CACHE_LINE_SIZE - 1) &
					 ~static_cast<uintptr_t>(PG_CACHE_LINE_SIZE - 1);
	return reinterpret_cast<ScanIORecord *>(
		addr + (static_cast<uint64>(slot) * c->profile_event_count + event) *
				   CACHELINEALIGN(sizeof(ScanIORecord)));
}
void Metadata(ScanIOStats &a, const ScanIOStats &b) {
	if (!b.enabled)
		return;
	if (a.enabled &&
		(a.requested_pages != b.requested_pages || a.granted_pages != b.granted_pages ||
		 a.ring_capacity != b.ring_capacity || a.request_cap != b.request_cap ||
		 a.io_method != b.io_method || a.batch_pages != b.batch_pages ||
         a.kernel_feature_supported != b.kernel_feature_supported ||
         a.buffer_feature_supported != b.buffer_feature_supported ||
		 a.io_workers != b.io_workers || a.effective_io_concurrency != b.effective_io_concurrency ||
		 a.io_combine_limit != b.io_combine_limit || a.io_max_concurrency != b.io_max_concurrency ||
		 (a.allocation_chunk_pages && b.allocation_chunk_pages &&
		  a.allocation_chunk_pages != b.allocation_chunk_pages)))
		a.mixed_config = true;
	const bool mixed = a.mixed_config || b.mixed_config;
#define COPY(n) a.n = b.n
	COPY(enabled);
	COPY(detail);
	COPY(requested_pages);
	COPY(granted_pages);
	COPY(query_share_cap);
	COPY(backend_share_cap);
	COPY(pg_pin_cap);
	COPY(ring_capacity);
	COPY(request_cap);
	COPY(batch_pages);
	COPY(block_size);
	COPY(io_method);
	COPY(io_workers);
	COPY(effective_io_concurrency);
	COPY(io_combine_limit);
	COPY(io_max_concurrency);
	COPY(shared_buffers_pages);
    COPY(kernel_feature_supported);
    COPY(buffer_feature_supported);
	if (b.allocation_chunk_pages)
		a.allocation_chunk_pages = b.allocation_chunk_pages;
	if (mixed)
		a.allocation_chunk_pages = 0;
#undef COPY
	a.mixed_config = mixed;
}
void AddStats(ScanIOStats &a, const ScanIOStats &b, const ScanIOStats *before = nullptr) {
	const ScanIOStats zero{};
	const auto &p = before ? *before : zero;
#define ADD(n) a.n += b.n - p.n;
	YAAP_SCAN_IO_COUNTERS(ADD)
#undef ADD
#define HIST(n)                                                                                    \
	if (b.detail) {                                                                                \
		if (before)                                                                                \
			a.n.MergeDelta(b.n, p.n);                                                              \
		else                                                                                       \
			a.n.Merge(b.n);                                                                        \
	}
	YAAP_SCAN_IO_HISTOGRAMS(HIST)
#undef HIST
	a.peak_submitted_ahead = std::max(a.peak_submitted_ahead, b.peak_submitted_ahead);
	a.peak_reserved = std::max(a.peak_reserved, b.peak_reserved);
	a.peak_unwaited = std::max(a.peak_unwaited, b.peak_unwaited);
	Metadata(a, b);
}
void AddRecord(ScanIORecord &a, const ScanIORecord &b) {
	AddStats(a.stats, b.stats);
	a.source_ns += b.source_ns;
	a.setup_ns += b.setup_ns;
	a.source_calls += b.source_calls;
	a.submit_api_ns += b.submit_api_ns;
	a.wait_api_ns += b.wait_api_ns;
	a.refill_exclusive_ns += b.refill_exclusive_ns;
	a.shared_hits += b.shared_hits;
	a.shared_reads += b.shared_reads;
	a.submit_sample_cpu_ns += b.submit_sample_cpu_ns;
	a.submit_sample_wall_ns += b.submit_sample_wall_ns;
	a.conflict = a.conflict || b.conflict;
}
void HistogramText(const char *scope, uint32 pipeline, int worker, const char *name,
				   const ScanIOHistogram &h, const char *unit) {
	uint64 q[4];
	bool ok = true;
	const unsigned p[4] = {50, 90, 95, 99};
	for (unsigned i = 0; i < 4; i++)
		ok = ok && h.QuantileUpper(p[i], q[i]);
	if (!ok) {
		elog(NOTICE,
			 "pg_yaap[scan-io-hist] %s pipeline=%u worker=%d metric=%s availability=UNAVAILABLE "
			 "observations=%llu",
			 scope, pipeline, worker, name, (unsigned long long)h.count);
		return;
	}
	elog(NOTICE,
		 "pg_yaap[scan-io-hist] %s pipeline=%u worker=%d metric=%s availability=ESTIMATED "
		 "semantics=BUCKET_UPPER_BOUND_CLIPPED_TO_OBSERVED_MAX unit=%s observations=%llu "
		 "p50_le=%llu p90_le=%llu p95_le=%llu p99_le=%llu",
		 scope, pipeline, worker, name, unit, (unsigned long long)h.count, (unsigned long long)q[0],
		 (unsigned long long)q[1], (unsigned long long)q[2], (unsigned long long)q[3]);
}
bool KernelTimingAvailable(const ScanIOStats &s, bool complete) {
    return complete && !s.mixed_config && s.detail && s.kernel_feature_supported &&
        s.kernel_profiled_operations == s.wait_validation_calls &&
        s.kernel_timed_read_requests == s.kernel_requests &&
        !s.kernel_failed_requests && !s.kernel_unclassified_requests;
}
bool BufferProfileAvailable(const ScanIOStats &s, bool complete) {
    return complete && !s.mixed_config && s.detail && s.buffer_feature_supported &&
        s.buffer_profiled_start_calls == s.api_start_calls;
}
void PrintBufferProfile(const char *scope, uint32 pipeline, int worker,
                        const ScanIOStats &s, bool complete) {
    if (!BufferProfileAvailable(s, complete)) {
        elog(NOTICE, "pg_yaap[scan-buffer-alloc] %s pipeline=%u worker=%d availability=UNAVAILABLE "
             "feature_version=%u profiled_start_calls=%llu start_calls=%llu",
             scope, pipeline, worker, s.buffer_feature_supported,
             (unsigned long long)s.buffer_profiled_start_calls, (unsigned long long)s.api_start_calls);
        return;
    }
    elog(NOTICE, "pg_yaap[scan-buffer-alloc] %s pipeline=%u worker=%d availability=EXACT "
         "alloc_calls=%llu lookup_tag_hits=%llu lookup_tag_misses=%llu "
         "ring_attempts=%llu ring_hits=%llu freelist_candidates=%llu freelist_hits=%llu "
         "clock_candidates=%llu clock_usage_decrements=%llu clock_pinned_skips=%llu "
         "victim_retries=%llu dirty_writebacks=%llu",
         scope, pipeline, worker,
         (unsigned long long)s.buffer_alloc_calls,
         (unsigned long long)s.buffer_lookup_hits,
         (unsigned long long)s.buffer_lookup_misses,
         (unsigned long long)s.buffer_ring_attempts,
         (unsigned long long)s.buffer_ring_hits,
         (unsigned long long)s.buffer_freelist_candidates,
         (unsigned long long)s.buffer_freelist_hits,
         (unsigned long long)s.buffer_clock_candidates,
         (unsigned long long)s.buffer_clock_usage_decrements,
         (unsigned long long)s.buffer_clock_pinned_skips,
         (unsigned long long)s.buffer_victim_retries,
         (unsigned long long)s.buffer_dirty_writebacks);

    const bool sampled = s.buffer_sampled_start_calls > 0;
    elog(NOTICE, "pg_yaap[scan-buffer-sample] %s pipeline=%u worker=%d availability=%s "
         "sample_rate=64 sampled_start_calls=%llu sampled_pages=%llu "
         "alloc_total_ms=%.6f "
         "lookup_ms=%.6f "
         "mapping_lock_ms=%.6f "
         "victim_ms=%.6f "
         "strategy_ms=%.6f "
         "ring_ms=%.6f "
         "freelist_ms=%.6f "
         "clock_ms=%.6f "
         "dirty_write_ms=%.6f "
         "invalidate_ms=%.6f "
         "pin_ms=%.6f "
         "semantics=SAMPLED_COHORT_SUM_NOT_TOTAL_INCLUSIVE_NESTED_PHASES_CLOCK_OVERHEAD_INCLUDED",
         scope, pipeline, worker, sampled ? "ESTIMATED" : "UNAVAILABLE",
         (unsigned long long)s.buffer_sampled_start_calls, (unsigned long long)s.buffer_sampled_pages,
         s.buffer_alloc_total_ns/1e6,
         s.buffer_lookup_ns/1e6,
         s.buffer_mapping_lock_ns/1e6,
         s.buffer_victim_ns/1e6,
         s.buffer_strategy_ns/1e6,
         s.buffer_ring_ns/1e6,
         s.buffer_freelist_ns/1e6,
         s.buffer_clock_ns/1e6,
         s.buffer_dirty_write_ns/1e6,
         s.buffer_invalidate_ns/1e6,
         s.buffer_pin_ns/1e6);
}
void Print(const char *scope, uint32 pipeline, int worker, const ScanIORecord &r, bool success,
		   bool detail, PipelineSharedControl *c) {
	const auto &s = r.stats;
	const bool complete = success && !r.conflict;
	if (!s.enabled) {
		elog(NOTICE,
			 "pg_yaap[scan-io] %s pipeline=%u worker=%d read_stream=pg complete=%s "
			 "custom_stream_observations=UNAVAILABLE source_elapsed_ms=%.3f pg_accounted_hits=%llu "
			 "pg_accounted_reads=%llu",
			 scope, pipeline, worker, complete ? "yes" : "no", r.source_ns / 1e6,
			 (unsigned long long)r.shared_hits, (unsigned long long)r.shared_reads);
		if (worker == -2)
			elog(NOTICE,
				 "pg_yaap[scan-io-diagnosis] scope=%s code=INSUFFICIENT_EVIDENCE "
				 "status=INSUFFICIENT_DATA evidence=native_ReadStream_internal_metrics_unavailable",
				 scope);
		return;
	}
	const double ready_lb =
		s.consumed_pages ? 100.0 * s.known_ready_at_need_pages / s.consumed_pages : -1;
	const double pg_rate = r.shared_hits + r.shared_reads
							   ? 100.0 * r.shared_hits / (r.shared_hits + r.shared_reads)
							   : -1;
	const double api_ratio = r.source_ns ? 100.0 * r.wait_api_ns / r.source_ns : -1;
	elog(NOTICE,
		 "pg_yaap[scan-io] %s pipeline=%u worker=%d pid=%d relation=%u operator=%u complete=%s "
		 "consumed_pages=%llu known_ready=%llu readiness_unknown=%llu ready_lower_bound_pct=%.3f "
		 "pg_accounted_hit_pct=%.3f source_elapsed_ms=%.3f next_ms=%.3f wait_api_ms=%.3f "
		 "wait_api_envelope_pct=%.3f unsubmitted_at_need=%llu",
		 scope, pipeline, worker, r.pid, r.relation_oid, r.operator_id, complete ? "yes" : "no",
		 (unsigned long long)s.consumed_pages, (unsigned long long)s.known_ready_at_need_pages,
		 (unsigned long long)s.readiness_unknown_pages, ready_lb, pg_rate, r.source_ns / 1e6,
		 s.next_total_ns / 1e6, r.wait_api_ns / 1e6, api_ratio,
		 (unsigned long long)s.request_not_submitted_at_need_pages);
    const bool pg_start_available = complete && s.api_start_calls &&
        s.pg_start_timed_calls == s.api_start_calls;
    const bool pg_wait_available = complete && s.wait_validation_calls &&
        s.pg_wait_timed_calls == s.wait_validation_calls;
    char pg_start_ms[48], pg_wait_ms[48];
    if (pg_start_available) snprintf(pg_start_ms, sizeof(pg_start_ms), "%.3f", s.pg_start_read_ns/1e6);
    else strlcpy(pg_start_ms, "UNAVAILABLE", sizeof(pg_start_ms));
    if (pg_wait_available) snprintf(pg_wait_ms, sizeof(pg_wait_ms), "%.3f", s.pg_wait_read_ns/1e6);
    else strlcpy(pg_wait_ms, "UNAVAILABLE", sizeof(pg_wait_ms));
    elog(NOTICE,
         "pg_yaap[scan-io-pg-time] %s pipeline=%u worker=%d "
         "start_read_ms=%s wait_read_ms=%s start_timed_calls=%llu wait_timed_calls=%llu "
         "wait_new_read_pages=%llu counter=shared_blk_read_time "
         "semantics=PG_ACCOUNTED_READ_TIME_WITHIN_API_NOT_PURE_STALL",
         scope, pipeline, worker, pg_start_ms, pg_wait_ms,
         (unsigned long long)s.pg_start_timed_calls, (unsigned long long)s.pg_wait_timed_calls,
         (unsigned long long)s.pg_wait_new_read_pages);
    const bool kernel_available = KernelTimingAvailable(s, complete);
    if (kernel_available)
            elog(NOTICE,
                 "pg_yaap[scan-io-kernel] %s pipeline=%u worker=%d availability=EXACT "
                 "profiled_operations=%llu requests=%llu worker_requests=%llu queued_requests=%llu "
                 "requested_bytes=%llu completed_bytes=%llu queue_ms=%.3f reopen_ms=%.3f "
                 "read_syscall_ms=%.3f shared_completion_ms=%.3f enqueue_to_shared_ms=%.3f "
                 "queue_full_sync_requests=%llu forced_sync_requests=%llu "
                 "semantics=CORE_MONOTONIC_REQUEST_TIMING_NOT_PHYSICAL_DEVICE_LATENCY",
                 scope, pipeline, worker, (unsigned long long)s.kernel_profiled_operations,
                 (unsigned long long)s.kernel_requests, (unsigned long long)s.kernel_worker_requests,
                 (unsigned long long)s.kernel_queued_requests,
                 (unsigned long long)s.kernel_requested_bytes, (unsigned long long)s.kernel_completed_bytes,
                 s.kernel_queue_ns/1e6, s.kernel_reopen_ns/1e6, s.kernel_read_ns/1e6,
                 s.kernel_shared_complete_ns/1e6, s.kernel_enqueue_to_shared_ns/1e6,
                 (unsigned long long)s.kernel_queue_full_sync_requests,
                 (unsigned long long)s.kernel_forced_sync_requests);
        else
            elog(NOTICE,
                 "pg_yaap[scan-io-kernel] %s pipeline=%u worker=%d availability=UNAVAILABLE "
                 "feature_version=%u profiled_operations=%llu wait_operations=%llu "
                 "failed_requests=%llu unclassified_requests=%llu",
                 scope, pipeline, worker, s.kernel_feature_supported,
                 (unsigned long long)s.kernel_profiled_operations,
                 (unsigned long long)s.wait_validation_calls,
                 (unsigned long long)s.kernel_failed_requests,
                 (unsigned long long)s.kernel_unclassified_requests);
    if (detail) {
        PrintBufferProfile(scope, pipeline, worker, s, complete);
        if (s.page_consumer_gap_count)
            elog(NOTICE,
                 "pg_yaap[scan-io-consumer] %s pipeline=%u worker=%d intervals=%llu "
                 "elapsed_ms=%.3f average_us_per_page_interval=%.3f "
                 "semantics=CALLER_ELAPSED_BETWEEN_NEXT_RETURN_AND_NEXT_ENTRY_NOT_PURE_CPU_OR_COLUMNAR_TIME",
                 scope, pipeline, worker, (unsigned long long)s.page_consumer_gap_count,
                 s.page_consumer_gap_ns/1e6,
                 s.page_consumer_gap_ns/1000.0/s.page_consumer_gap_count);
        else
            elog(NOTICE, "pg_yaap[scan-io-consumer] %s pipeline=%u worker=%d availability=UNAVAILABLE "
                 "intervals=0", scope, pipeline, worker);
        HistogramText(scope, pipeline, worker, "kernel_logical_operation_queue_sum",
                      s.kernel_operation_queue_ns, "ns");
        HistogramText(scope, pipeline, worker, "kernel_logical_operation_read_sum",
                      s.kernel_operation_read_ns, "ns");
        HistogramText(scope, pipeline, worker, "page_consumer_gap",
                      s.page_consumer_gap_distribution_ns, "ns");
		elog(NOTICE,
			 "pg_yaap[scan-io-window] %s pipeline=%u worker=%d mixed=%s requested=%u granted=%u "
			 "query_cap=%u backend_share=%u pg_pin_cap=%u ring=%u requests=%u peak_submitted=%llu "
			 "peak_reserved=%llu peak_unwaited=%llu pin_binding=%llu request_binding=%llu "
			 "window_full=%llu slots=%llu created=%llu reset=0 reset_semantics=NO_RESET_API "
			 "shared_buffers_pages=%llu block_size=%u io_method=%u io_workers=%u "
			 "effective_io_concurrency=%u io_combine_limit=%u io_max_concurrency=%u",
			 scope, pipeline, worker, s.mixed_config ? "yes" : "no", s.requested_pages,
			 s.granted_pages, s.query_share_cap, s.backend_share_cap, s.pg_pin_cap, s.ring_capacity,
			 s.request_cap, (unsigned long long)s.peak_submitted_ahead,
			 (unsigned long long)s.peak_reserved, (unsigned long long)s.peak_unwaited,
			 (unsigned long long)s.pin_binding_events, (unsigned long long)s.request_binding_events,
			 (unsigned long long)s.window_full_events, (unsigned long long)s.slot_binding_events,
			 (unsigned long long)s.streams_created, (unsigned long long)s.shared_buffers_pages,
			 s.block_size, s.io_method, s.io_workers, s.effective_io_concurrency,
			 s.io_combine_limit, s.io_max_concurrency);
		if (s.allocation_chunk_pages)
			elog(NOTICE,
				 "pg_yaap[scan-io-allocation] %s pipeline=%u worker=%d "
				 "initial_ownership_chunk_pages=%u actual_logical_batch_pages=%u "
				 "ownership_is_not_io_request=yes",
				 scope, pipeline, worker, s.allocation_chunk_pages, s.batch_pages);
		else
			elog(NOTICE,
				 "pg_yaap[scan-io-allocation] %s pipeline=%u worker=%d "
				 "initial_ownership_chunk_pages=UNAVAILABLE",
				 scope, pipeline, worker);
		const uint64 classified = r.submit_api_ns + r.wait_api_ns + r.refill_exclusive_ns;
		elog(NOTICE,
			 "pg_yaap[scan-io-time] %s pipeline=%u worker=%d next_ms=%.3f "
			 "start_buffers_api_ms=%.3f wait_buffers_api_ms=%.3f lookahead_exclusive_ms=%.3f "
			 "other_exclusive_ms=%.3f timing_excess_ms=%.3f start_calls=%llu "
			 "accepted_logical_pages=%llu need_wait_logical_pages=%llu",
			 scope, pipeline, worker, s.next_total_ns / 1e6, r.submit_api_ns / 1e6,
			 r.wait_api_ns / 1e6, r.refill_exclusive_ns / 1e6,
			 (s.next_total_ns > classified ? s.next_total_ns - classified : 0) / 1e6,
			 (classified > s.next_total_ns ? classified - s.next_total_ns : 0) / 1e6,
			 (unsigned long long)s.api_start_calls, (unsigned long long)s.api_accepted_pages,
			 (unsigned long long)s.start_need_wait_pages);
		if (s.consumed_pages)
			elog(
				NOTICE,
				"pg_yaap[scan-io-average] %s pipeline=%u worker=%d source_elapsed_per_page_us=%.3f "
				"next_per_page_us=%.3f semantics=AMORTIZED_ELAPSED_NOT_CPU_OR_DEVICE_LATENCY",
				scope, pipeline, worker, r.source_ns / 1000.0 / s.consumed_pages,
				s.next_total_ns / 1000.0 / s.consumed_pages);
		else
			elog(NOTICE,
				 "pg_yaap[scan-io-average] %s pipeline=%u worker=%d per_page=UNAVAILABLE "
				 "reason=zero_consumed_pages",
				 scope, pipeline, worker);
		if (s.api_need_wait_calls)
			elog(NOTICE,
				 "pg_yaap[scan-io-request] %s pipeline=%u worker=%d need_wait_operations=%llu "
				 "average_logical_pages=%.3f average_logical_bytes=%.3f "
				 "physical_io_size=UNAVAILABLE",
				 scope, pipeline, worker, (unsigned long long)s.api_need_wait_calls,
				 static_cast<double>(s.start_need_wait_pages) / s.api_need_wait_calls,
				 static_cast<double>(s.start_need_wait_pages) * s.block_size /
					 s.api_need_wait_calls);
        elog(NOTICE,
             "pg_yaap[scan-io-cpu] %s pipeline=%u worker=%d sampling=1_in_64 "
             "submit_samples=%llu submit_sample_wall_ms=%.3f submit_sample_cpu_ms=%.3f "
             "wait_samples=%llu wait_sample_wall_ms=%.3f wait_sample_cpu_ms=%.3f "
             "clock_failures=%llu semantics=SAMPLED_CPU_AND_ELAPSED_NOT_SYNC_IO_OR_PURE_STALL",
             scope, pipeline, worker, (unsigned long long)s.submit_cpu_samples,
             s.submit_sample_wall_ns/1e6, s.submit_sample_cpu_ns/1e6,
             (unsigned long long)s.wait_cpu_samples, s.wait_sample_wall_ns/1e6,
             s.wait_sample_cpu_ns/1e6, (unsigned long long)s.cpu_clock_failures);
        elog(NOTICE,
             "pg_yaap[scan-io-refill-detail] %s pipeline=%u worker=%d "
             "block_assignment_ms=%.3f pin_budget_check_ms=%.3f "
             "semantics=SUBSETS_OF_LOOKAHEAD_EXCLUSIVE_NOT_ADDITIONAL_TIME "
             "buffer_lookup_inside_start=UNAVAILABLE ring_replacement_inside_start=UNAVAILABLE "
             "submitter_sync_read=UNAVAILABLE queue_full_reason=UNAVAILABLE",
             scope, pipeline, worker, s.block_assignment_ns/1e6, s.pin_budget_check_ns/1e6);
        HistogramText(scope, pipeline, worker, "start_buffers_api", s.start_api_ns, "ns");
        HistogramText(scope, pipeline, worker, "wait_buffers_api", s.wait_api_ns, "ns");
		HistogramText(scope, pipeline, worker, "api_return_to_need", s.api_return_to_need_ns, "ns");
		HistogramText(scope, pipeline, worker, "need_to_submit_return", s.need_to_submit_return_ns,
					  "ns");
		HistogramText(scope, pipeline, worker, "api_return_to_validated_ready_observation",
					  s.validated_ready_upper_ns, "ns");
		HistogramText(scope, pipeline, worker, "accepted_pages_per_start_call",
					  s.accepted_pages_per_call, "pages");
		HistogramText(scope, pipeline, worker, "need_wait_accepted_pages_per_call",
					  s.need_wait_accepted_pages, "pages");
		HistogramText(scope, pipeline, worker, "submitted_lookahead_at_need",
					  s.submitted_ahead_at_need, "pages");
	}
	if (worker == -2) {
		ScanIODiagnosisInput in{};
		in.stats = &s;
		in.source_ns = r.source_ns;
		in.next_ns = s.next_total_ns;
		in.submit_ns = r.submit_api_ns;
		in.wait_api_ns = r.wait_api_ns;
		in.submit_sample_cpu_ns = r.submit_sample_cpu_ns;
		in.submit_sample_wall_ns = r.submit_sample_wall_ns;
		in.complete = complete;
		ScanIODiagnosisConfig cfg{};
		cfg.min_pages = c->scan_io_min_pages;
		cfg.wait_api_threshold_pct = c->scan_io_wait_pct;
		const auto report = ScanIODiagnose(in, cfg);
		for (uint32 i = 0; i < report.count; i++) {
			const auto &f = report.findings[i];
			StringInfoData values;
			initStringInfo(&values);
			if (f.value_a_name[0])
				appendStringInfo(&values, " %s=%llu", f.value_a_name,
								 (unsigned long long)f.value_a);
			if (f.value_b_name[0])
				appendStringInfo(&values, " %s=%llu", f.value_b_name,
								 (unsigned long long)f.value_b);
			elog(NOTICE,
				 "pg_yaap[scan-io-diagnosis] scope=%s pipeline=%u code=%s status=%s "
				 "priority_ms=%.3f priority_estimated=%s evidence=%s%s next_experiment=%s",
				 scope, pipeline, ScanIODiagnosisCodeName(f.code),
				 ScanIODiagnosisStatusName(f.status), f.priority_ns / 1e6,
				 f.priority_is_estimate ? "yes" : "no", f.evidence, values.data, f.suggestion);
			pfree(values.data);
		}
	}
}
void UnavailableJson(StringInfo out, const char *key, const char *reason) {
	appendStringInfo(out,
					 "\"%s\":{\"availability\":\"UNAVAILABLE\",\"value\":null,\"source\":null,"
					 "\"reason\":\"%s\"},",
					 key, reason);
}
void HistogramJson(StringInfo out, const char *name, const ScanIOHistogram &h) {
	appendStringInfo(
		out,
		"\"%s\":{\"availability\":\"%s\",\"count\":%llu,\"sum\":%llu,\"quantile_semantics\":"
		"\"BUCKET_UPPER_BOUND_CLIPPED_TO_OBSERVED_MAX\",\"timing_note\":\"API_boundary_observation_"
		"not_device_completion\",\"saturated\":%s,\"observed_min\":",
		name, h.count && !h.saturated ? "ESTIMATED" : "UNAVAILABLE", (unsigned long long)h.count,
		(unsigned long long)h.sum, h.saturated ? "true" : "false");
	if (h.count)
		appendStringInfo(out, "%llu", (unsigned long long)h.minimum);
	else
		appendStringInfoString(out, "null");
	appendStringInfoString(out, ",\"observed_max\":");
	if (h.count)
		appendStringInfo(out, "%llu", (unsigned long long)h.maximum);
	else
		appendStringInfoString(out, "null");
	appendStringInfoString(out, ",\"bins\":[");
	for (unsigned i = 0; i < 64; i++)
		appendStringInfo(out, "%s%llu", i ? "," : "", (unsigned long long)h.bins[i]);
	appendStringInfoString(out, "],\"quantiles\":{");
	const unsigned ps[4] = {50, 90, 95, 99};
	for (unsigned i = 0; i < 4; i++) {
		uint64 q = 0;
		bool ok = h.QuantileUpper(ps[i], q);
		appendStringInfo(out, "%s\"p%u_le\":", i ? "," : "", ps[i]);
		if (ok)
			appendStringInfo(out, "%llu", (unsigned long long)q);
		else
			appendStringInfoString(out, "null");
	}
	appendStringInfoString(out, "}},");
}
void ReportInternal(PipelineSharedControl *c, dsa_area *d, bool success) {
	if (!c || !d || !c->scan_io_profile_mode || c->scan_io_reported)
		return;
	c->scan_io_reported = 1;
	elog(
		NOTICE,
		"pg_yaap[scan-io] schema=1 complete=%s strict_buffer_hit=UNAVAILABLE "
		"prefetch_ready_rate=UNAVAILABLE cold_ready_rate=UNAVAILABLE actual_io_stall=UNAVAILABLE "
		"true_io_completion=UNAVAILABLE pg_queue_full_fallback=UNAVAILABLE "
		"pg_forced_sync=UNAVAILABLE physical_io_request_sizes=UNAVAILABLE "
		"known_ready_lower_bound_is_not_exact_ready_rate=yes wait_api_time_is_not_actual_stall=yes",
		success ? "yes" : "no");
	ScanIORecord query{};
	const bool detail = c->scan_io_profile_mode == 2;
	uint64 lost = 0;
	for (uint32 e = 0; e < c->profile_event_count; e++) {
		ScanIORecord total{};
		for (uint32 slot = 0; slot < c->profile_worker_slots; slot++) {
			int worker = slot ? static_cast<int>(slot - 1) : -1;
			auto *r = Record(c, d, worker, e);
			if (!r)
				continue;
			if (pg_atomic_read_u32(&r->publishing)) {
				++lost;
				query.conflict = true;
				continue;
			}
			if (!r->source_calls)
				continue;
			if (detail)
				Print("worker", e / 3, worker, *r, success, true, c);
			AddRecord(total, *r);
			total.relation_oid = r->relation_oid;
			total.operator_id = r->operator_id;
		}
		if (total.source_calls) {
			if (detail)
				Print("pipeline", e / 3, -2, total, success, true, c);
			AddRecord(query, total);
		}
	}
	if (query.source_calls)
		Print("query", UINT32_MAX, -2, query, success, detail, c);
	else
		elog(NOTICE, "pg_yaap[scan-io] no_complete_source_records=yes lost_publications=%llu",
			 (unsigned long long)lost);
}
} // namespace
void ScanIOProfileAllocate(PipelineSharedControl *c, dsa_area *d, uint32 events, uint32 workers) {
	c->scan_io_profile_mode = pg_yaap_profile_scan_io;
	c->scan_io_profile_root = InvalidDsaPointer;
	c->scan_io_reported = 0;
	c->scan_io_min_pages = pg_yaap_profile_scan_io_min_pages;
	c->scan_io_wait_pct = pg_yaap_profile_scan_io_wait_pct;
	if (!c->scan_io_profile_mode || !events)
		return;
	const uint64 n = static_cast<uint64>(workers + 1) * events;
	if (n > (kMaxBytes - PG_CACHE_LINE_SIZE) / CACHELINEALIGN(sizeof(ScanIORecord))) {
		c->scan_io_profile_mode = 0;
		elog(WARNING, "pg_yaap: scan IO profile exceeds 16MiB; disabled");
		return;
	}
	c->scan_io_profile_root =
		YaapDsaAllocate(d, n * CACHELINEALIGN(sizeof(ScanIORecord)) + PG_CACHE_LINE_SIZE,
						DSA_ALLOC_ZERO | DSA_ALLOC_NO_OOM);
	if (!DsaPointerIsValid(c->scan_io_profile_root)) {
		c->scan_io_profile_mode = 0;
		elog(WARNING, "pg_yaap: insufficient DSA for scan IO profiler; disabled");
		return;
	}
	for (uint32 s = 0; s <= workers; s++)
		for (uint32 e = 0; e < events; e++) {
			auto *r = Record(c, d, s ? static_cast<int>(s - 1) : -1, e);
			new (r) ScanIORecord{};
			pg_atomic_init_u32(&r->publishing, 0);
		}
}
int ScanIOProfileMode(const ExecCtx &ctx) {
	return ctx.control ? ctx.control->scan_io_profile_mode : 0;
}
void ScanIOProfilePublish(const ExecCtx &ctx, const ScanProfileRecord &cost, const ScanIOStats *now,
						  const ScanIOStats *previous) {
	auto *r = Record(ctx.control, ctx.dsa, ctx.worker_index, ctx.profile_event_id);
	if (!r)
		return;
	if (r->pid && r->pid != MyProcPid) {
		r->conflict = true;
		return;
	}
	pg_atomic_write_u32(&r->publishing, 1);
	pg_write_barrier();
	r->pid = MyProcPid;
	r->worker_id = ctx.worker_index;
	r->relation_oid = cost.relation_oid;
	r->operator_id = cost.operator_id;
	r->source_ns += cost.source_ns;
	r->setup_ns += cost.setup_ns;
	++r->source_calls;
	r->submit_api_ns += cost.read_submit_ns;
	r->wait_api_ns += cost.read_wait_ns;
	r->refill_exclusive_ns += cost.read_refill_ns;
	r->shared_hits += cost.shared_blks_hit;
	r->shared_reads += cost.shared_blks_read;
	r->submit_sample_cpu_ns += cost.read_submit_sample_cpu_ns;
	r->submit_sample_wall_ns += cost.read_submit_sample_wall_ns;
	if (now)
		AddStats(r->stats, *now, previous);
	pg_write_barrier();
	pg_atomic_write_u32(&r->publishing, 0);
}
void ScanIOProfileReport(PipelineSharedControl *c, dsa_area *d, bool success) {
	MemoryContext old = CurrentMemoryContext;
	PG_TRY();
	{
		ReportInternal(c, d, success);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(old);
		FlushErrorState();
		elog(WARNING, "pg_yaap: scan IO report incomplete");
	}
	PG_END_TRY();
}
static void AppendRecordJson(PipelineSharedControl *c, const ScanIORecord *r, StringInfo out,
							 bool complete) {
	const auto &s = r->stats;
	if (!s.enabled) {
		appendStringInfo(
			out,
			",\"scan_io\":{\"complete\":%s,\"read_stream\":\"pg\",\"custom_stream_observations\":{"
			"\"availability\":\"UNAVAILABLE\",\"value\":null},\"source_ns\":%llu,\"pg_accounted_"
			"hits\":%llu,\"pg_accounted_reads\":%llu}",
			complete && !r->conflict ? "true" : "false", (unsigned long long)r->source_ns,
			(unsigned long long)r->shared_hits, (unsigned long long)r->shared_reads);
		return;
	}
	appendStringInfo(out,
					 ",\"scan_io\":{\"schema_version\":1,\"complete\":%s,\"origin\":\"YAAP_"
					 "INSTRUMENTATION\",\"metrics\":{",
					 complete && !r->conflict ? "true" : "false");
	const char *unavailable[] = {"buffer_hit_rate",
								 "ready_pages_at_consume",
								 "not_ready_pages_at_consume",
								 "prefetch_lead_ns",
								 "aio_submitted_requests",
								 "aio_async_requests",
								 "buffer_lookup_ns",
								 "self_sync_read_ns",
								 "buffer_hit_pages",
								 "buffer_miss_pages",
								 "buffer_lookup_pages",
								 "buffer_already_loading_pages",
								 "prefetch_ready_rate",
								 "cold_prefetch_ready_rate",
								 "actual_io_wait_ns",
								 "actual_io_wait_count",
								 "io_stall_ratio",
								 "io_completion_ns",
								 "aio_queue_full_fallback_count",
								 "aio_queue_full_fallback_ns",
								 "aio_forced_sync_count",
								 "sync_read_count",
								 "sync_read_ns",
								 "current_pinned_buffers",
								 "actual_ios_in_progress",
								 "physical_io_request_bytes",
								 "repeated_page_load_count"};
	for (const char *key : unavailable)
		UnavailableJson(out, key,
						"PG public buffer contract lacks the required observation boundary");
    const bool kernel_available = KernelTimingAvailable(s, complete && !r->conflict);
    appendStringInfo(out, "\"kernel_timing\":{\"availability\":\"%s\",\"value\":%s,"
                     "\"source\":\"POSTGRES_CORE_INSTRUMENTATION\","
                     "\"semantic\":\"REQUEST_LIFECYCLE_NOT_PHYSICAL_DEVICE_LATENCY\","
                     "\"feature_version\":%u,\"profiled_operations\":%llu,\"wait_api_calls\":%llu},",
                     kernel_available ? "EXACT" : "UNAVAILABLE", kernel_available ? "true" : "null",
                     s.kernel_feature_supported, (unsigned long long)s.kernel_profiled_operations,
                     (unsigned long long)s.wait_validation_calls);
    const bool buffer_available = BufferProfileAvailable(s, complete && !r->conflict);
    appendStringInfo(out, "\"buffer_profile\":{\"availability\":\"%s\",\"value\":%s,"
                     "\"source\":\"POSTGRES_CORE_INSTRUMENTATION\","
                     "\"semantic\":\"SHARED_BUFFER_ALLOCATION_COUNTS_NOT_READY_CLASSIFICATION\","
                     "\"feature_version\":%u,\"profiled_start_calls\":%llu,\"start_calls\":%llu},",
                     buffer_available ? "EXACT" : "UNAVAILABLE", buffer_available ? "true" : "null",
                     s.buffer_feature_supported, (unsigned long long)s.buffer_profiled_start_calls,
                     (unsigned long long)s.api_start_calls);
    const bool buffer_sample_available = buffer_available && s.buffer_sampled_start_calls;
    appendStringInfo(out, "\"buffer_alloc_sampled_ns\":{\"availability\":\"%s\",\"value\":",
                     buffer_sample_available ? "ESTIMATED" : "UNAVAILABLE");
    if (buffer_sample_available) appendStringInfo(out, "%llu", (unsigned long long)s.buffer_alloc_total_ns);
    else appendStringInfoString(out, "null");
    appendStringInfo(out, ",\"source\":\"POSTGRES_CORE_INSTRUMENTATION\","
                     "\"semantic\":\"SAMPLED_COHORT_SUM_NOT_FULL_QUERY_TOTAL\",\"sample_rate\":64,"
                     "\"sampled_start_calls\":%llu,\"sampled_pages\":%llu,"
                     "\"nested_phase_times_are_inclusive\":true,\"clock_overhead_included\":true},",
                     (unsigned long long)s.buffer_sampled_start_calls,
                     (unsigned long long)s.buffer_sampled_pages);
    const bool consumer_available = complete && !r->conflict && s.detail && s.page_consumer_gap_count;
    appendStringInfo(out, "\"page_consumer_gap_ns\":{\"availability\":\"%s\",\"value\":",
                     consumer_available ? "EXACT" : "UNAVAILABLE");
    if (consumer_available) appendStringInfo(out, "%llu", (unsigned long long)s.page_consumer_gap_ns);
    else appendStringInfoString(out, "null");
    appendStringInfo(out, ",\"source\":\"YAAP_INSTRUMENTATION\","
                     "\"semantic\":\"CALLER_ELAPSED_NOT_PURE_CPU_OR_COLUMNAR_TIME\",\"intervals\":%llu},",
                     (unsigned long long)s.page_consumer_gap_count);
    const char *pg_names[] = {"pg_start_read_ns", "pg_wait_read_ns"};
    const uint64 pg_values[] = {s.pg_start_read_ns, s.pg_wait_read_ns};
    const uint64 pg_calls[] = {s.api_start_calls, s.wait_validation_calls};
    const uint64 pg_timed[] = {s.pg_start_timed_calls, s.pg_wait_timed_calls};
    for (unsigned i=0; i<2; ++i) {
        const bool available = complete && !r->conflict && pg_calls[i] && pg_timed[i]==pg_calls[i];
        appendStringInfo(out, "\"%s\":{\"availability\":\"%s\",\"value\":", pg_names[i],
                         available ? "EXACT" : "UNAVAILABLE");
        if (available) appendStringInfo(out, "%llu", (unsigned long long)pg_values[i]);
        else appendStringInfoString(out, "null");
        appendStringInfo(out,
            ",\"source\":\"POSTGRES_STATISTICS\",\"semantic\":\"PG_ACCOUNTED_READ_TIME_WITHIN_API\","
            "\"counter\":\"shared_blk_read_time\",\"timed_calls\":%llu,\"api_calls\":%llu},",
            (unsigned long long)pg_timed[i], (unsigned long long)pg_calls[i]);
    }
	appendStringInfo(out, "\"known_ready_rate_lower_bound\":{\"availability\":\"%s\",\"value\":",
					 s.consumed_pages && complete && !r->conflict ? "EXACT" : "UNAVAILABLE");
	if (s.consumed_pages && complete && !r->conflict)
		appendStringInfo(out, "%.12f",
						 static_cast<double>(s.known_ready_at_need_pages) / s.consumed_pages);
	else
		appendStringInfoString(out, "null");
	appendStringInfoString(
		out, ",\"semantic\":\"READY_RATE_LOWER_BOUND\",\"source\":\"YAAP_INSTRUMENTATION\"},");
	appendStringInfo(
		out, "\"pg_accounted_hit_rate\":{\"availability\":\"%s\",\"value\":",
		(r->shared_hits + r->shared_reads) && complete && !r->conflict ? "EXACT" : "UNAVAILABLE");
	if ((r->shared_hits + r->shared_reads) && complete && !r->conflict)
		appendStringInfo(out, "%.12f",
						 static_cast<double>(r->shared_hits) / (r->shared_hits + r->shared_reads));
	else
		appendStringInfoString(out, "null");
	appendStringInfoString(
		out, ",\"semantic\":\"PG_COUNTER_DELTA_NOT_INITIAL_LOOKUP_CLASSIFICATION\",\"source\":"
			 "\"POSTGRES_STATISTICS\"}},\"observed\":{\"counter_source\":\"YAAP_INSTRUMENTATION\","
			 "\"pg_counter_source\":\"POSTGRES_STATISTICS\",");
#define JSON(n) appendStringInfo(out, "\"" #n "\":%llu,", (unsigned long long)s.n);
	YAAP_SCAN_IO_COUNTERS(JSON)
#undef JSON
	appendStringInfo(
		out,
		"\"source_ns\":%llu,\"submit_api_ns\":%llu,\"wait_api_ns\":%llu,\"refill_exclusive_ns\":%"
		"llu,\"shared_hits\":%llu,\"shared_reads\":%llu},\"config\":{\"mixed\":%s,\"requested_"
		"pages\":%u,\"granted_pages\":%u,\"query_share_cap\":%u,\"backend_share_cap\":%u,\"pg_pin_"
		"cap\":%u,\"ring_capacity\":%u,\"request_cap\":%u,\"batch_pages\":%u,\"block_size\":%u,"
		"\"io_method\":%u,\"io_workers\":%u,\"shared_buffers_pages\":%llu,\"effective_io_"
		"concurrency\":%u,\"io_combine_limit\":%u,\"io_max_concurrency\":%u,\"kernel_feature_supported\":%u,\"buffer_feature_supported\":%u},\"histograms\":{",
		(unsigned long long)r->source_ns, (unsigned long long)r->submit_api_ns,
		(unsigned long long)r->wait_api_ns, (unsigned long long)r->refill_exclusive_ns,
		(unsigned long long)r->shared_hits, (unsigned long long)r->shared_reads,
		s.mixed_config ? "true" : "false", s.requested_pages, s.granted_pages, s.query_share_cap,
		s.backend_share_cap, s.pg_pin_cap, s.ring_capacity, s.request_cap, s.batch_pages,
		s.block_size, s.io_method, s.io_workers, (unsigned long long)s.shared_buffers_pages,
		s.effective_io_concurrency, s.io_combine_limit, s.io_max_concurrency, s.kernel_feature_supported, s.buffer_feature_supported);
#define HIST(n) HistogramJson(out, #n, s.n);
	YAAP_SCAN_IO_HISTOGRAMS(HIST)
#undef HIST
	appendStringInfoString(out, "\"true_completion_latency\":{\"availability\":\"UNAVAILABLE\","
								"\"value\":null}},\"allocation_chunk_pages\":");
	if (s.allocation_chunk_pages)
		appendStringInfo(out, "%u", s.allocation_chunk_pages);
	else
		appendStringInfoString(out, "null");
	appendStringInfo(
		out,
		",\"diagnosis_config\":{\"min_pages\":%u,\"wait_api_threshold_pct\":%u},\"diagnoses\":[",
		c->scan_io_min_pages, c->scan_io_wait_pct);
	ScanIODiagnosisInput input{};
	input.stats = &s;
	input.source_ns = r->source_ns;
	input.next_ns = s.next_total_ns;
	input.submit_ns = r->submit_api_ns;
	input.wait_api_ns = r->wait_api_ns;
	input.complete = complete && !r->conflict;
	input.submit_sample_cpu_ns = r->submit_sample_cpu_ns;
	input.submit_sample_wall_ns = r->submit_sample_wall_ns;
	ScanIODiagnosisConfig cfg{};
	cfg.min_pages = c->scan_io_min_pages;
	cfg.wait_api_threshold_pct = c->scan_io_wait_pct;
	const auto findings = ScanIODiagnose(input, cfg);
	for (size_t i = 0; i < findings.count; i++) {
		const auto &f = findings.findings[i];
		if (i)
			appendStringInfoChar(out, ',');
		appendStringInfo(out,
						 "{\"code\":\"%s\",\"status\":\"%s\",\"priority_ns\":%llu,\"priority_is_"
						 "estimate\":%s,\"evidence\":",
						 ScanIODiagnosisCodeName(f.code), ScanIODiagnosisStatusName(f.status),
						 (unsigned long long)f.priority_ns,
						 f.priority_is_estimate ? "true" : "false");
		escape_json(out, f.evidence);
		appendStringInfoString(out, ",\"suggested_experiment\":");
		escape_json(out, f.suggestion);
		if (f.value_a_name[0]) {
			appendStringInfoString(out, ",\"value_a_name\":");
			escape_json(out, f.value_a_name);
			appendStringInfo(out, ",\"value_a\":%llu", (unsigned long long)f.value_a);
		}
		if (f.value_b_name[0]) {
			appendStringInfoString(out, ",\"value_b_name\":");
			escape_json(out, f.value_b_name);
			appendStringInfo(out, ",\"value_b\":%llu", (unsigned long long)f.value_b);
		}
		appendStringInfoChar(out, '}');
	}
	appendStringInfoString(out, "]}");
}

void ScanIOProfileAppendWorkerJson(PipelineSharedControl *c, dsa_area *d, int worker, uint32 event,
								   StringInfo out, bool complete) {
	auto *r = Record(c, d, worker, event);
	if (!r)
		return;
	if (pg_atomic_read_u32(&r->publishing)) {
		appendStringInfoString(out, ",\"scan_io\":{\"complete\":false,\"availability\":"
									"\"UNAVAILABLE\",\"reason\":\"partial publication\"}");
		return;
	}
	if (!r->source_calls)
		return;
	AppendRecordJson(c, r, out, complete);
}
void ScanIOProfileAppendAggregateJson(PipelineSharedControl *c, dsa_area *d, int pipeline,
									  StringInfo out, bool complete) {
	if (!c || !c->scan_io_profile_mode)
		return;
	ScanIORecord total{};
	total.worker_id = -2;
	for (uint32 e = 0; e < c->profile_event_count; e++) {
		if (pipeline >= 0 && e / 3 != static_cast<uint32>(pipeline))
			continue;
		for (uint32 slot = 0; slot < c->profile_worker_slots; slot++) {
			auto *r = Record(c, d, slot ? static_cast<int>(slot - 1) : -1, e);
			if (!r)
				continue;
			if (pg_atomic_read_u32(&r->publishing)) {
				total.conflict = true;
				continue;
			}
			if (!r->source_calls)
				continue;
			if (!total.source_calls) {
				total.relation_oid = r->relation_oid;
				total.operator_id = r->operator_id;
			} else if (total.relation_oid != r->relation_oid ||
					   total.operator_id != r->operator_id) {
				total.relation_oid = 0;
				total.operator_id = UINT32_MAX;
			}
			AddRecord(total, *r);
		}
	}
	if (total.source_calls)
		AppendRecordJson(c, &total, out, complete);
}
} // namespace pipeline
} // namespace pg_yaap
