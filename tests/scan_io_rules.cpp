// Portable deterministic tests: no server, PG headers, filesystem or sleeps.
#include "parallel/pipeline/scan_io_diagnosis.hpp"
#include <cassert>
#include <cstring>
#include <iostream>
using namespace pg_yaap::pipeline;

namespace {
ScanIOStats Stats() {
	ScanIOStats s;
	s.enabled = 1;
	s.consumed_pages = s.api_accepted_pages = 256;
	s.known_ready_at_need_pages = 128;
	s.readiness_unknown_pages = 128;
	s.start_ready_pages = s.start_need_wait_pages = 128;
	s.pending_cohort_consumed_pages = 128;
	s.api_start_calls = 64;
	s.request_cap = 4;
	s.granted_pages = 128;
	s.requested_pages = 8192;
	s.peak_submitted_ahead = 128;
	s.peak_unwaited = 4;
	s.block_size = 8192;
	return s;
}
ScanIODiagnosisInput Input(const ScanIOStats &s) {
	ScanIODiagnosisInput i;
	i.stats = &s;
	i.source_ns = 1000000;
	i.next_ns = 800000;
	i.submit_ns = i.wait_api_ns = 200000;
	return i;
}
ScanIOMetric Exact(uint64_t v, MetricSource src = MetricSource::YAAP_INSTRUMENTATION) {
	return {v, MetricAvailability::EXACT, src, MetricSemantic::VALUE};
}
const ScanIODiagnosisFinding *Find(const ScanIODiagnosisReport &r, ScanIODiagnosisCode code) {
	for (size_t n = 0; n < r.count; ++n)
		if (r.findings[n].code == code)
			return &r.findings[n];
	return nullptr;
}
void Not(const ScanIODiagnosisReport &r, ScanIODiagnosisCode code) { assert(!Find(r, code)); }
void Has(const ScanIODiagnosisReport &r, ScanIODiagnosisCode code, ScanIODiagnosisStatus status) {
	const auto *f = Find(r, code);
	assert(f && f->status == status);
}
} // namespace
int main() {
	unsigned checks = 0;
	{
		auto r = ScanIODiagnose({});
		Has(r, ScanIODiagnosisCode::INSUFFICIENT_EVIDENCE,
			ScanIODiagnosisStatus::INSUFFICIENT_DATA);
		assert(r.missing_evidence_mask & SCAN_IO_MISSING_STATS);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		auto r = ScanIODiagnose(i);
		assert(!r.inconsistent);
		assert(r.count == 1);
		Not(r, ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED); // Static clamp alone.
		Not(r, ScanIODiagnosisCode::IO_LATENCY_HIGH);
		Not(r, ScanIODiagnosisCode::QUEUE_FULL_FALLBACK);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		s.request_binding_events = 4;
		auto r = ScanIODiagnose(i);
		Has(r, ScanIODiagnosisCode::LOOKAHEAD_IO_LIMITED, ScanIODiagnosisStatus::SUSPECTED);
		assert(Find(r, ScanIODiagnosisCode::LOOKAHEAD_IO_LIMITED)->priority_is_estimate);
		s.request_binding_events = 3;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::LOOKAHEAD_IO_LIMITED);
		s.request_binding_events = 4;
		i.wait_api_ns = 199999;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::LOOKAHEAD_IO_LIMITED);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		s.request_binding_events = 4;
		s.api_start_calls = 63;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::LOOKAHEAD_IO_LIMITED);
		s.api_start_calls = 64;
		s.consumed_pages = 127;
		s.known_ready_at_need_pages = 0;
		s.readiness_unknown_pages = 127;
		s.pending_cohort_consumed_pages = 127;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::LOOKAHEAD_IO_LIMITED);
		s.consumed_pages = s.readiness_unknown_pages = 128;
		Has(ScanIODiagnose(i), ScanIODiagnosisCode::LOOKAHEAD_IO_LIMITED,
			ScanIODiagnosisStatus::SUSPECTED);
		ScanIODiagnosisConfig c;
		c.min_pages = 129;
		Not(ScanIODiagnose(i, c), ScanIODiagnosisCode::LOOKAHEAD_IO_LIMITED);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		s.pin_binding_events = 4;
		s.peak_submitted_ahead = 102;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED);
		s.peak_submitted_ahead = 103;
		Has(ScanIODiagnose(i), ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED,
			ScanIODiagnosisStatus::SUSPECTED);
		s.mixed_config = true;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		s.backend_share_cap = s.granted_pages;
		s.pg_pin_cap = 151;
		s.window_full_events = 4;
		auto r = ScanIODiagnose(i);
		Has(r, ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED, ScanIODiagnosisStatus::SUSPECTED);
		assert(std::strstr(Find(r, ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED)->evidence,
						   "BACKEND FAIR-SHARE"));
		s.peak_submitted_ahead = 64; // Request quota far below the page grant.
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED);
		s.peak_submitted_ahead = 128;
		s.window_full_events = 0;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED);
		s.window_full_events = 4;
		s.backend_share_cap = 256;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED);
		s.backend_share_cap = s.granted_pages = 1;
		s.peak_submitted_ahead = 1;
		s.api_start_calls = 256;
		s.window_full_events = 13;
		s.peak_unwaited = 1;
		Has(ScanIODiagnose(i), ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED,
			ScanIODiagnosisStatus::SUSPECTED);
		s.mixed_config = true;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		s.request_not_submitted_at_need_pages = 12;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::PREFETCH_TOO_LATE);
		s.request_not_submitted_at_need_pages = 13;
		Has(ScanIODiagnose(i), ScanIODiagnosisCode::PREFETCH_TOO_LATE,
			ScanIODiagnosisStatus::SUSPECTED);
		s.known_ready_at_need_pages = s.consumed_pages;
		s.readiness_unknown_pages = 0;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::PREFETCH_TOO_LATE);
		++checks;
	}
	{
		// Cached pages first submitted on demand have unknown pre-Start
		// readiness, but no need-wait cohort and no Wait API cost.
		auto s = Stats();
		auto i = Input(s);
		s.known_ready_at_need_pages = 0;
		s.readiness_unknown_pages = s.consumed_pages;
		s.start_ready_pages = s.api_accepted_pages;
		s.start_need_wait_pages = 0;
		s.pending_cohort_consumed_pages = 0;
		s.request_not_submitted_at_need_pages = s.consumed_pages;
		s.api_start_calls = s.consumed_pages;
		s.backend_share_cap = s.granted_pages = s.peak_submitted_ahead = 1;
		s.peak_unwaited = 0;
		s.window_full_events = s.consumed_pages;
		i.wait_api_ns = 0;
		auto r = ScanIODiagnose(i);
		assert(!r.inconsistent);
		Not(r, ScanIODiagnosisCode::PREFETCH_TOO_LATE);
		Not(r, ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED);
		// Even positive API-envelope overhead without a need-wait cohort
		// cannot turn cache-ready demand into evidence of late prefetch.
		i.wait_api_ns = 200000;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::PREFETCH_TOO_LATE);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		s.request_not_submitted_at_need_pages = s.consumed_pages;
		s.known_ready_at_need_pages = 0;
		s.readiness_unknown_pages = s.consumed_pages;
		s.start_ready_pages = 0;
		s.start_need_wait_pages = s.api_accepted_pages;
		s.pending_cohort_consumed_pages = s.consumed_pages;
		i.wait_api_ns = 0;
		ScanIODiagnosisConfig c;
		c.wait_api_threshold_pct = 0;
		Not(ScanIODiagnose(i, c), ScanIODiagnosisCode::PREFETCH_TOO_LATE);
		s.request_binding_events = s.pin_binding_events = 100;
		Not(ScanIODiagnose(i, c), ScanIODiagnosisCode::LOOKAHEAD_IO_LIMITED);
		Not(ScanIODiagnose(i, c), ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED);
		// Case C: positive, significant API cost plus all-need-wait demand.
		i.wait_api_ns = 200000;
		Has(ScanIODiagnose(i), ScanIODiagnosisCode::PREFETCH_TOO_LATE,
			ScanIODiagnosisStatus::SUSPECTED);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		s.known_ready_at_need_pages = s.consumed_pages;
		s.readiness_unknown_pages = 0;
		s.start_ready_pages = s.api_accepted_pages;
		s.start_need_wait_pages = 0;
		s.api_start_calls = 256;
		s.request_binding_events = s.pin_binding_events = 100;
		for (unsigned n = 0; n < 256; ++n)
			s.accepted_pages_per_call.Observe(1);
		auto r = ScanIODiagnose(i);
		Not(r, ScanIODiagnosisCode::LOOKAHEAD_IO_LIMITED);
		Not(r, ScanIODiagnosisCode::LOOKAHEAD_PIN_LIMITED);
		Not(r, ScanIODiagnosisCode::IO_REQUEST_TOO_SMALL_SUSPECTED);
		Not(r, ScanIODiagnosisCode::BUFFER_PRESSURE_SUSPECTED);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		s.configured_sync_need_wait_calls = 20;
		auto r = ScanIODiagnose(i);
		Not(r, ScanIODiagnosisCode::SYNC_IO_FALLBACK);
		Not(r, ScanIODiagnosisCode::QUEUE_FULL_FALLBACK);
		i.sync_count = Exact(3);
		i.sync_time_ns = Exact(50000);
		Has(ScanIODiagnose(i), ScanIODiagnosisCode::SYNC_IO_FALLBACK,
			ScanIODiagnosisStatus::CONFIRMED);
		i.queue_full_count = Exact(2);
		i.queue_full_time_ns = Exact(100000);
		r = ScanIODiagnose(i);
		Has(r, ScanIODiagnosisCode::QUEUE_FULL_FALLBACK, ScanIODiagnosisStatus::CONFIRMED);
		assert(r.findings[0].code == ScanIODiagnosisCode::QUEUE_FULL_FALLBACK);
		i.queue_full_count.semantic = MetricSemantic::SAMPLED;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::QUEUE_FULL_FALLBACK);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		s.api_return_to_need_ns.Observe(1);
		s.validated_ready_upper_ns.Observe(1000000);
		auto r = ScanIODiagnose(i);
		Not(r, ScanIODiagnosisCode::IO_LATENCY_HIGH);
		Not(r, ScanIODiagnosisCode::PREFETCH_TOO_LATE);
		i.actual_completion_ns = {1000000, MetricAvailability::EXACT,
								  MetricSource::YAAP_INSTRUMENTATION,
								  MetricSemantic::OBSERVED_UPPER_BOUND};
		i.actual_stall_ns = Exact(200000);
		i.completion_late_pairs = Exact(64);
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::IO_LATENCY_HIGH);
		i.actual_completion_ns = Exact(1000000);
		Has(ScanIODiagnose(i), ScanIODiagnosisCode::IO_LATENCY_HIGH,
			ScanIODiagnosisStatus::SUSPECTED);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		i.repeat_loads = Exact(20);
		i.evictions = Exact(30);
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::BUFFER_PRESSURE_SUSPECTED);
		i.repeat_loads_unexpected = true;
		Has(ScanIODiagnose(i), ScanIODiagnosisCode::BUFFER_PRESSURE_SUSPECTED,
			ScanIODiagnosisStatus::SUSPECTED);
		i.evictions.availability = MetricAvailability::UNAVAILABLE;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::BUFFER_PRESSURE_SUSPECTED);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		i.reset_count = Exact(2);
		i.reset_time_ns = Exact(50000);
		Has(ScanIODiagnose(i), ScanIODiagnosisCode::READSTREAM_RESET_OVERHEAD,
			ScanIODiagnosisStatus::CONFIRMED);
		i.reset_count.value = 1;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::READSTREAM_RESET_OVERHEAD);
		i.buffer_manager_exclusive_ns = Exact(200000);
		Has(ScanIODiagnose(i), ScanIODiagnosisCode::BUFFER_MANAGER_OVERHEAD,
			ScanIODiagnosisStatus::CONFIRMED);
		i.buffer_manager_exclusive_ns.semantic = MetricSemantic::API_ENVELOPE;
		i.submit_sample_cpu_ns = 199000;
		i.submit_sample_wall_ns = 200000;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::BUFFER_MANAGER_OVERHEAD);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		i.storage_io_requests = Exact(64);
		i.storage_io_bytes = Exact(64 * 8192 * 4);
		Has(ScanIODiagnose(i), ScanIODiagnosisCode::IO_REQUEST_TOO_SMALL_SUSPECTED,
			ScanIODiagnosisStatus::SUSPECTED);
		++i.storage_io_bytes.value;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::IO_REQUEST_TOO_SMALL_SUSPECTED);
		i.storage_io_bytes.value = 8192;
		i.storage_io_requests.semantic = MetricSemantic::PG_COUNTER_DELTA;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::IO_REQUEST_TOO_SMALL_SUSPECTED);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		i.device_bytes = Exact(900, MetricSource::OS_OBSERVATION);
		i.device_time_ns = Exact(1000000000, MetricSource::OS_OBSERVATION);
		i.sustained_bandwidth_bytes_per_second = Exact(1000, MetricSource::OS_OBSERVATION);
		Has(ScanIODiagnose(i), ScanIODiagnosisCode::STORAGE_BANDWIDTH_LIMITED,
			ScanIODiagnosisStatus::SUSPECTED);
		i.device_bytes.source = MetricSource::POSTGRES_STATISTICS;
		Not(ScanIODiagnose(i), ScanIODiagnosisCode::STORAGE_BANDWIDTH_LIMITED);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		s.known_ready_at_need_pages = 200;
		s.readiness_unknown_pages = 100;
		assert(ScanIODiagnose(i).inconsistent);
		s = Stats();
		s.known_ready_at_need_pages = 0;
		assert(ScanIODiagnose(i).inconsistent); // Complete unclassified population.
		i.complete = false;
		assert(!ScanIODiagnose(i).inconsistent);
		s = Stats();
		s.peak_unwaited = 5;
		i.complete = true;
		assert(ScanIODiagnose(i).inconsistent);
		s.mixed_config = true;
		assert(!ScanIODiagnose(i).inconsistent);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		i.complete = false;
		s.request_binding_events = s.pin_binding_events = 100;
		auto r = ScanIODiagnose(i);
		assert(r.partial);
		Not(r, ScanIODiagnosisCode::LOOKAHEAD_IO_LIMITED);
		i.queue_full_count = Exact(1);
		Has(ScanIODiagnose(i), ScanIODiagnosisCode::QUEUE_FULL_FALLBACK,
			ScanIODiagnosisStatus::CONFIRMED);
		++checks;
	}
	{
		auto s = Stats();
		auto i = Input(s);
		i.next_ns = i.source_ns + 1001;
		assert(ScanIODiagnose(i).inconsistent);
		i = Input(s);
		i.submit_ns = 600000;
		i.wait_api_ns = 300000;
		assert(ScanIODiagnose(i).inconsistent);
		i = Input(s);
		i.submit_sample_cpu_ns = 1002;
		i.submit_sample_wall_ns = 1;
		assert(ScanIODiagnose(i).inconsistent);
		i = Input(s);
		s.api_return_to_need_ns.bins[3] = 1;
		assert(ScanIODiagnose(i).inconsistent);
		++checks;
	}
	{
		ScanIOHistogram whole;
		for (unsigned n = 0; n < 1024; ++n)
			whole.Observe(n);
		for (unsigned workers : {1u, 4u, 8u, 12u}) {
			std::array<ScanIOHistogram, 12> parts{};
			for (unsigned n = 0; n < 1024; ++n)
				parts[n % workers].Observe(n);
			ScanIOHistogram merged;
			for (unsigned w = 0; w < workers; ++w)
				merged.Merge(parts[w]);
			assert(merged.count == whole.count && merged.sum == whole.sum);
			for (unsigned b = 0; b < 64; ++b)
				assert(merged.bins[b] == whole.bins[b]);
			for (unsigned p : {50u, 90u, 95u, 99u}) {
				uint64_t a = 0, b = 0;
				assert(merged.QuantileUpper(p, a) && whole.QuantileUpper(p, b) && a == b);
			}
		}
		ScanIOHistogram constant;
		constant.Observe(128);
		uint64_t exact_upper = 0;
		assert(constant.QuantileUpper(95, exact_upper) && exact_upper == 128);
		ScanIOHistogram empty;
		uint64_t upper = 123;
		assert(!empty.QuantileUpper(95, upper));
		ScanIOHistogram huge;
		huge.Observe(UINT64_MAX);
		assert(huge.QuantileUpper(100, upper) && upper == UINT64_MAX);
		whole.saturated = true;
		assert(!whole.QuantileUpper(50, upper));
		++checks;
	}
	assert(std::strcmp(ScanIODiagnosisCodeName(ScanIODiagnosisCode::QUEUE_FULL_FALLBACK),
					   "QUEUE_FULL_FALLBACK") == 0);
	assert(std::strcmp(ScanIODiagnosisStatusName(ScanIODiagnosisStatus::SUSPECTED), "SUSPECTED") ==
		   0);
	std::cout << "scan_io_rules: " << checks << " test groups passed\n";
}
