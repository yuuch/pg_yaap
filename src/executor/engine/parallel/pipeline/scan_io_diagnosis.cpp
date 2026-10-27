#include "parallel/pipeline/scan_io_diagnosis.hpp"
#include <algorithm>

namespace pg_yaap {
namespace pipeline {
namespace {
bool ExactValue(const ScanIOMetric &m) {
	return m.availability == MetricAvailability::EXACT && m.semantic == MetricSemantic::VALUE;
}
bool ObservedValue(const ScanIOMetric &m) {
	return m.availability != MetricAvailability::UNAVAILABLE && m.semantic == MetricSemantic::VALUE;
}
bool ObservedCounter(const ScanIOMetric &m) {
	return m.availability != MetricAvailability::UNAVAILABLE &&
		   (m.semantic == MetricSemantic::VALUE || m.semantic == MetricSemantic::SAMPLED ||
			m.semantic == MetricSemantic::PG_COUNTER_DELTA);
}
bool AtLeastPercent(uint64_t numerator, uint64_t denominator, unsigned percent) {
	return denominator && static_cast<long double>(numerator) * 100 >=
							  static_cast<long double>(denominator) * std::min(percent, 100u);
}
bool ExceedsWithTolerance(uint64_t value, uint64_t limit, uint64_t tolerance) {
	return value > limit && value - limit > tolerance;
}
bool HistogramValid(const ScanIOHistogram &h) {
	if (h.saturated)
		return true; // Explicitly unusable, not fabricated corruption.
	uint64_t total = 0;
	for (const auto n : h.bins) {
		if (UINT64_MAX - total < n)
			return false;
		total += n;
	}
	return total == h.count && (!h.count || h.minimum <= h.maximum);
}
void Add(ScanIODiagnosisReport &r, ScanIODiagnosisCode code, ScanIODiagnosisStatus status,
		 const char *evidence, const char *suggestion, const char *a_name = "", uint64_t a = 0,
		 const char *b_name = "", uint64_t b = 0, uint64_t priority = 0,
		 bool estimated_priority = false) {
	if (r.count == r.kCapacity)
		return;
	r.findings[r.count++] = {
		code, status, priority, estimated_priority, evidence, suggestion, a_name, a, b_name, b};
}
void Inconsistent(ScanIODiagnosisReport &r, const char *evidence, const char *a_name, uint64_t a,
				  const char *b_name, uint64_t b) {
	r.inconsistent = true;
	Add(r, ScanIODiagnosisCode::DATA_INCONSISTENT, ScanIODiagnosisStatus::INSUFFICIENT_DATA,
		evidence, "Verify accounting scope and publication before interpreting bottlenecks.",
		a_name, a, b_name, b);
}
void Sort(ScanIODiagnosisReport &r) {
	// Measured/estimated impacts are explicitly tagged; event count is not time.
	std::sort(r.findings.begin(), r.findings.begin() + r.count,
			  [](const ScanIODiagnosisFinding &a, const ScanIODiagnosisFinding &b) {
				  if (a.priority_ns != b.priority_ns)
					  return a.priority_ns > b.priority_ns;
				  if (a.status != b.status)
					  return a.status < b.status;
				  return a.code < b.code;
			  });
}
} // namespace

ScanIODiagnosisReport ScanIODiagnose(const ScanIODiagnosisInput &i,
									 const ScanIODiagnosisConfig &c) {
	ScanIODiagnosisReport r;
	const auto *s = i.stats;
	const uint64_t next = i.next_ns ? i.next_ns : (s ? s->next_total_ns : 0);
	r.partial = !i.complete;
	if (s && s->enabled) {
		if (s->known_ready_at_need_pages > s->consumed_pages ||
			s->readiness_unknown_pages > s->consumed_pages ||
			s->known_ready_at_need_pages > s->consumed_pages - s->readiness_unknown_pages ||
			(i.complete &&
			 s->known_ready_at_need_pages != s->consumed_pages - s->readiness_unknown_pages)) {
			Inconsistent(r, "Consumption readiness counters do not partition consumed pages.",
						 "known_ready", s->known_ready_at_need_pages, "consumed",
						 s->consumed_pages);
		}
		if (s->known_ready_pending_cohort_pages > s->pending_cohort_consumed_pages ||
			s->pending_cohort_consumed_pages > s->consumed_pages ||
			s->consumed_pages > s->api_accepted_pages ||
			s->start_ready_pages > s->api_accepted_pages ||
			s->start_need_wait_pages > s->api_accepted_pages ||
			s->start_ready_pages > s->api_accepted_pages - s->start_need_wait_pages ||
			(i.complete &&
			 s->start_ready_pages != s->api_accepted_pages - s->start_need_wait_pages)) {
			Inconsistent(r, "Submission/cohort page counters have incompatible populations.",
						 "accepted_pages", s->api_accepted_pages, "consumed_pages",
						 s->consumed_pages);
		}
		if (!s->mixed_config && s->request_cap && s->peak_unwaited > s->request_cap) {
			Inconsistent(r, "Observed unWaited request peak exceeds the declared own-source quota.",
						 "peak_unwaited", s->peak_unwaited, "request_cap", s->request_cap);
		}
		bool hist_valid = true;
#define CHECK_HIST(n) hist_valid = hist_valid && HistogramValid(s->n);
		YAAP_SCAN_IO_HISTOGRAMS(CHECK_HIST)
#undef CHECK_HIST
		if (!hist_valid)
			Inconsistent(r, "Histogram bucket counts do not match their population.",
						 "consumed_pages", s->consumed_pages, "detail", s->detail);
	}
	if ((i.source_ns &&
		 (ExceedsWithTolerance(next, i.source_ns, c.timing_tolerance_ns) ||
		  ExceedsWithTolerance(i.submit_ns, i.source_ns, c.timing_tolerance_ns) ||
		  ExceedsWithTolerance(i.wait_api_ns, i.source_ns, c.timing_tolerance_ns))) ||
		(next && (i.submit_ns > next || i.wait_api_ns > next ||
				  (i.submit_ns <= next && ExceedsWithTolerance(i.wait_api_ns, next - i.submit_ns,
															   c.timing_tolerance_ns)))) ||
		ExceedsWithTolerance(i.submit_sample_cpu_ns, i.submit_sample_wall_ns,
							 c.timing_tolerance_ns)) {
		Inconsistent(r, "Elapsed envelopes or sampled CPU exceed their containing interval.",
					 "next_ns", next, "source_ns", i.source_ns);
	}
	if (r.inconsistent)
		return r;

	const auto confirmed = ScanIODiagnosisStatus::CONFIRMED;
	const auto suspected = ScanIODiagnosisStatus::SUSPECTED;
	if (ExactValue(i.queue_full_count) && i.queue_full_count.value) {
		Add(r, ScanIODiagnosisCode::QUEUE_FULL_FALLBACK, confirmed,
			"Directly attributed queue-full synchronous execution was supplied for this scope; API "
			"latency alone was not used.",
			"Repeat with less bursty submission and compare end-to-end elapsed time.",
			"queue_full_count", i.queue_full_count.value,
			ExactValue(i.queue_full_time_ns) ? "attributed_ns" : "",
			ExactValue(i.queue_full_time_ns) ? i.queue_full_time_ns.value : 0,
			ExactValue(i.queue_full_time_ns) ? i.queue_full_time_ns.value : 0);
	} else if (!ExactValue(i.queue_full_count))
		r.missing_evidence_mask |= SCAN_IO_MISSING_QUEUE_REASON;
	if (ExactValue(i.sync_count) && i.sync_count.value) {
		Add(r, ScanIODiagnosisCode::SYNC_IO_FALLBACK, confirmed,
			"Actual synchronous execution was directly attributed; its cause is not inferred from "
			"a sync-method or f_sync flag.",
			"Separate forced-sync conditions from submission-capacity pressure.", "sync_count",
			i.sync_count.value, ExactValue(i.sync_time_ns) ? "attributed_ns" : "",
			ExactValue(i.sync_time_ns) ? i.sync_time_ns.value : 0,
			ExactValue(i.sync_time_ns) ? i.sync_time_ns.value : 0);
	} else if (!ExactValue(i.sync_count))
		r.missing_evidence_mask |= SCAN_IO_MISSING_SYNC_REASON;

	if (!ExactValue(i.actual_stall_ns))
		r.missing_evidence_mask |= SCAN_IO_MISSING_STALL;
	if (!ExactValue(i.actual_completion_ns))
		r.missing_evidence_mask |= SCAN_IO_MISSING_COMPLETION;
	if (!i.source_ns)
		r.missing_evidence_mask |= SCAN_IO_MISSING_SCOPE_TIME;
	if (!s || !s->enabled)
		r.missing_evidence_mask |= SCAN_IO_MISSING_STATS;
	if (!i.complete)
		r.missing_evidence_mask |= SCAN_IO_PARTIAL_SCOPE;

	if (i.complete && s && s->enabled) {
		const bool pages_enough = s->consumed_pages >= c.min_pages;
		const bool calls_enough = s->api_start_calls >= c.min_requests;
		const bool demand_not_proven_ready = s->known_ready_at_need_pages < s->consumed_pages;
		const bool api_cost_significant =
			i.wait_api_ns > 0 &&
			AtLeastPercent(i.wait_api_ns, i.source_ns, c.wait_api_threshold_pct);
		if (pages_enough && demand_not_proven_ready && api_cost_significant &&
			s->start_need_wait_pages && s->pending_cohort_consumed_pages &&
			s->request_not_submitted_at_need_pages &&
			AtLeastPercent(s->request_not_submitted_at_need_pages, s->consumed_pages,
						   c.late_page_pct)) {
			Add(r, ScanIODiagnosisCode::PREFETCH_TOO_LATE, suspected,
				"Demand reached pages before their Start API submission, a consumed need-wait "
				"cohort "
				"exists, and the Wait API envelope is significant; initial cache state and actual "
				"I/O stall remain unknown.",
				"Compare earlier submission at the same request cap; retain paired "
				"demand/submission observations.",
				"not_submitted_at_need_pages", s->request_not_submitted_at_need_pages,
				"consumed_pages", s->consumed_pages, i.wait_api_ns, true);
		}
		if (pages_enough && calls_enough && demand_not_proven_ready && api_cost_significant &&
			s->request_binding_events &&
			AtLeastPercent(s->request_binding_events, s->api_start_calls, c.limit_event_pct)) {
			Add(r, ScanIODiagnosisCode::LOOKAHEAD_IO_LIMITED, suspected,
				"Own request quota repeatedly stopped eligible submission and Wait API envelope is "
				"significant; this is not PG queue depth or measured actual stall.",
				"Vary the own request cap while holding pages, io_workers and query fixed.",
				"request_binding_events", s->request_binding_events, "api_start_calls",
				s->api_start_calls, i.wait_api_ns, true);
		}
		const bool near_granted =
			!s->mixed_config && s->granted_pages &&
			AtLeastPercent(s->peak_submitted_ahead, s->granted_pages, c.near_capacity_pct);
		const bool runtime_pin_evidence =
			s->pin_binding_events &&
			AtLeastPercent(s->pin_binding_events, s->api_start_calls, c.limit_event_pct);
		const bool backend_share_evidence =
			!s->mixed_config && s->backend_share_cap && s->granted_pages == s->backend_share_cap &&
			s->requested_pages > s->granted_pages && s->window_full_events &&
			AtLeastPercent(s->window_full_events, s->api_start_calls, c.limit_event_pct);
		if (pages_enough && calls_enough && demand_not_proven_ready && api_cost_significant &&
			near_granted && (runtime_pin_evidence || backend_share_evidence)) {
			if (runtime_pin_evidence) {
				Add(r, ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED, suspected,
					"A runtime pin-binding branch repeated, inclusive queued submitted pages "
					"reached near the grant, and Wait API envelope is significant; startup clamp "
					"alone was not used.",
					"Compare a larger safe pin budget without changing request quota or I/O "
					"workers.",
					"pin_binding_events", s->pin_binding_events, "peak_submitted_ahead_pages",
					s->peak_submitted_ahead, i.wait_api_ns, true);
			} else {
				Add(r, ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED, suspected,
					"YAAP BACKEND FAIR-SHARE policy derived from the PG pin cap matches the grant "
					"below the request; frequent full-window observations and near-grant submitted "
					"depth coexist with significant Wait API time. This does not prove actual PG "
					"pins were exhausted.",
					"Compare alternative safe fair-share allocation at the same global pin budget "
					"and request cap; verify query wall time rather than only the grant.",
					"window_full_events", s->window_full_events, "backend_share_cap_pages",
					s->backend_share_cap, i.wait_api_ns, true);
			}
		}
		if (ExactValue(i.reset_count) && ExactValue(i.reset_time_ns) &&
			i.reset_count.value >= c.min_resets &&
			AtLeastPercent(i.reset_time_ns.value, i.source_ns, c.reset_time_pct)) {
			Add(r, ScanIODiagnosisCode::READSTREAM_RESET_OVERHEAD, confirmed,
				"Repeated resets and their initialization elapsed time are directly measured "
				"within the same logical scan.",
				"Check unnecessary source reconstruction across task fragments; do not change "
				"lifecycle based solely on stream counts.",
				"reset_count", i.reset_count.value, "reset_time_ns", i.reset_time_ns.value,
				i.reset_time_ns.value);
		}
		if (ObservedCounter(i.repeat_loads) && ObservedCounter(i.evictions) &&
			i.repeat_loads_unexpected && i.repeat_loads.value && i.evictions.value) {
			Add(r, ScanIODiagnosisCode::BUFFER_PRESSURE_SUSPECTED, suspected,
				"Unexpected repeated loads and replacement evidence coexist; low cache hits or "
				"normal ring reuse alone were not used.",
				"Correlate replacement/reuse and repeated-load samples with other backends and "
				"expected rescans.",
				"unexpected_repeat_loads", i.repeat_loads.value, "evictions", i.evictions.value);
		}
		if (ExactValue(i.actual_completion_ns) && ExactValue(i.completion_late_pairs) &&
			i.completion_late_pairs.value >= c.min_requests && ExactValue(i.actual_stall_ns) &&
			AtLeastPercent(i.actual_stall_ns.value, i.source_ns, c.wait_api_threshold_pct)) {
			Add(r, ScanIODiagnosisCode::IO_LATENCY_HIGH, suspected,
				"Direct completion/need pairs were late with measured actual stall; observed-ready "
				"upper bounds and independent percentiles were not substituted.",
				"Separate queue and service latency with supported external observation, holding "
				"request shape constant.",
				"late_completion_pairs", i.completion_late_pairs.value, "actual_stall_ns",
				i.actual_stall_ns.value, i.actual_stall_ns.value, true);
		}
		if (ExactValue(i.storage_io_requests) && ExactValue(i.storage_io_bytes) &&
			i.storage_io_requests.value >= c.min_requests && s->block_size &&
			i.storage_io_bytes.value &&
			static_cast<long double>(i.storage_io_bytes.value) <=
				static_cast<long double>(i.storage_io_requests.value) * s->block_size *
					c.small_request_pages &&
			(api_cost_significant ||
			 AtLeastPercent(i.submit_ns, i.source_ns, c.wait_api_threshold_pct))) {
			Add(r, ScanIODiagnosisCode::IO_REQUEST_TOO_SMALL_SUSPECTED, suspected,
				"Storage-request evidence excluding cache-ready API calls shows small transfers "
				"together with significant API cost.",
				"Compare combining sizes while preserving outstanding-byte and request budgets.",
				"storage_io_requests", i.storage_io_requests.value, "storage_io_bytes",
				i.storage_io_bytes.value, std::max(i.submit_ns, i.wait_api_ns), true);
		}
		if (ExactValue(i.buffer_manager_exclusive_ns) &&
			AtLeastPercent(i.buffer_manager_exclusive_ns.value, i.source_ns,
						   c.buffer_overhead_pct)) {
			Add(r, ScanIODiagnosisCode::BUFFER_MANAGER_OVERHEAD, confirmed,
				"Exclusive non-I/O Buffer Manager cost was directly attributed; broad Submit CPU "
				"samples were not treated as exclusive Buffer Manager time.",
				"Profile lookup/pin/replacement CPU separately before changing buffer strategy.",
				"buffer_manager_exclusive_ns", i.buffer_manager_exclusive_ns.value, "source_ns",
				i.source_ns, i.buffer_manager_exclusive_ns.value);
		}
	}
	const bool device_known =
		ObservedValue(i.device_bytes) && ObservedValue(i.device_time_ns) &&
		ObservedValue(i.sustained_bandwidth_bytes_per_second) && i.device_time_ns.value &&
		i.sustained_bandwidth_bytes_per_second.value &&
		i.device_bytes.source == MetricSource::OS_OBSERVATION &&
		i.device_time_ns.source == MetricSource::OS_OBSERVATION &&
		i.sustained_bandwidth_bytes_per_second.source == MetricSource::OS_OBSERVATION;
	if (i.complete && device_known) {
		const long double rate =
			static_cast<long double>(i.device_bytes.value) * 1000000000.0L / i.device_time_ns.value;
		if (rate * 100 >= static_cast<long double>(i.sustained_bandwidth_bytes_per_second.value) *
							  std::min(c.bandwidth_reference_pct, 100u)) {
			Add(r, ScanIODiagnosisCode::STORAGE_BANDWIDTH_LIMITED, suspected,
				"Time-aligned OS device throughput is near an independently measured sustained "
				"reference; PG logical read bytes were not used as device traffic.",
				"Repeat with exclusive device workload and vary reader concurrency before "
				"asserting the device is the limiting cause.",
				"device_bytes", i.device_bytes.value, "device_window_ns", i.device_time_ns.value);
		}
	} else
		r.missing_evidence_mask |= SCAN_IO_MISSING_DEVICE;

	if (r.missing_evidence_mask || !r.count) {
		Add(r, ScanIODiagnosisCode::INSUFFICIENT_EVIDENCE, ScanIODiagnosisStatus::INSUFFICIENT_DATA,
			"Some causes cannot be attributed from this scope: actual stall/completion, PG "
			"queue/sync reason or device evidence may be unavailable. API envelopes and flags are "
			"not substitutes.",
			"Inspect availability and missing_evidence_mask; collect only the missing supported "
			"counters or scoped external evidence.",
			"missing_evidence_mask", r.missing_evidence_mask);
	}
	Sort(r);
	return r;
}

const char *ScanIODiagnosisCodeName(ScanIODiagnosisCode code) {
#define CASE(n)                                                                                    \
	case ScanIODiagnosisCode::n:                                                                   \
		return #n
	switch (code) {
		CASE(PREFETCH_TOO_LATE);
		CASE(LOOKAHEAD_PIN_LIMITED);
		CASE(LOOKAHEAD_IO_LIMITED);
		CASE(READSTREAM_RESET_OVERHEAD);
		CASE(QUEUE_FULL_FALLBACK);
		CASE(SYNC_IO_FALLBACK);
		CASE(BUFFER_PRESSURE_SUSPECTED);
		CASE(IO_LATENCY_HIGH);
		CASE(IO_REQUEST_TOO_SMALL_SUSPECTED);
		CASE(STORAGE_BANDWIDTH_LIMITED);
		CASE(BUFFER_MANAGER_OVERHEAD);
		CASE(INSUFFICIENT_EVIDENCE);
		CASE(DATA_INCONSISTENT);
	}
#undef CASE
	return "UNKNOWN";
}
const char *ScanIODiagnosisStatusName(ScanIODiagnosisStatus status) {
	switch (status) {
	case ScanIODiagnosisStatus::CONFIRMED:
		return "CONFIRMED";
	case ScanIODiagnosisStatus::SUSPECTED:
		return "SUSPECTED";
	case ScanIODiagnosisStatus::INSUFFICIENT_DATA:
		return "INSUFFICIENT_DATA";
	}
	return "UNKNOWN";
}
} // namespace pipeline
} // namespace pg_yaap
