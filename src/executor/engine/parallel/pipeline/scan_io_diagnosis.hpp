#pragma once

#include "parallel/pipeline/scan_io_metrics.hpp"
#include <array>
#include <cstddef>

namespace pg_yaap {
namespace pipeline {

enum class ScanIODiagnosisCode : uint8_t {
	PREFETCH_TOO_LATE,
	LOOKAHEAD_PIN_LIMITED,
	LOOKAHEAD_IO_LIMITED,
	READSTREAM_RESET_OVERHEAD,
	QUEUE_FULL_FALLBACK,
	SYNC_IO_FALLBACK,
	BUFFER_PRESSURE_SUSPECTED,
	IO_LATENCY_HIGH,
	IO_REQUEST_TOO_SMALL_SUSPECTED,
	STORAGE_BANDWIDTH_LIMITED,
	BUFFER_MANAGER_OVERHEAD,
	INSUFFICIENT_EVIDENCE,
	DATA_INCONSISTENT
};
enum class ScanIODiagnosisStatus : uint8_t { CONFIRMED, SUSPECTED, INSUFFICIENT_DATA };

// Future evidence defaults to unavailable. VALUE/EXACT evidence must describe
// this input's scope, not a global counter or an arbitrary sampled occurrence.
struct ScanIODiagnosisInput {
	const ScanIOStats *stats = nullptr;
	uint64_t source_ns = 0, next_ns = 0, submit_ns = 0, wait_api_ns = 0;
	uint64_t submit_sample_cpu_ns = 0, submit_sample_wall_ns = 0;
	bool complete = true;
	ScanIOMetric actual_stall_ns, actual_completion_ns, completion_late_pairs;
	ScanIOMetric sync_count, sync_time_ns, queue_full_count, queue_full_time_ns;
	ScanIOMetric repeat_loads, evictions;
	bool repeat_loads_unexpected = false;
	ScanIOMetric reset_count, reset_time_ns;
	ScanIOMetric storage_io_requests, storage_io_bytes;
	ScanIOMetric buffer_manager_exclusive_ns;
	ScanIOMetric device_bytes, device_time_ns, sustained_bandwidth_bytes_per_second;
};
struct ScanIODiagnosisConfig {
	uint64_t min_pages = 128, min_requests = 64;
	unsigned wait_api_threshold_pct = 20, limit_event_pct = 5;
	unsigned late_page_pct = 5, near_capacity_pct = 80;
	unsigned reset_time_pct = 5, buffer_overhead_pct = 20;
	unsigned bandwidth_reference_pct = 90, small_request_pages = 4;
	uint64_t min_resets = 2, timing_tolerance_ns = 1000;
};
struct ScanIODiagnosisFinding {
	ScanIODiagnosisCode code = ScanIODiagnosisCode::INSUFFICIENT_EVIDENCE;
	ScanIODiagnosisStatus status = ScanIODiagnosisStatus::INSUFFICIENT_DATA;
	uint64_t priority_ns = 0;
	bool priority_is_estimate = false;
	const char *evidence = "";
	const char *suggestion = "";
	const char *value_a_name = "";
	uint64_t value_a = 0;
	const char *value_b_name = "";
	uint64_t value_b = 0;
};
struct ScanIODiagnosisReport {
	static constexpr size_t kCapacity = 16;
	std::array<ScanIODiagnosisFinding, kCapacity> findings{};
	size_t count = 0;
	bool inconsistent = false, partial = false;
	// Missing evidence is summarized once, not twelve default findings.
	uint64_t missing_evidence_mask = 0;
};
enum ScanIOMissingEvidence : uint64_t {
	SCAN_IO_MISSING_STATS = UINT64_C(1) << 0,
	SCAN_IO_MISSING_STALL = UINT64_C(1) << 1,
	SCAN_IO_MISSING_COMPLETION = UINT64_C(1) << 2,
	SCAN_IO_MISSING_SYNC_REASON = UINT64_C(1) << 3,
	SCAN_IO_MISSING_QUEUE_REASON = UINT64_C(1) << 4,
	SCAN_IO_MISSING_DEVICE = UINT64_C(1) << 5,
	SCAN_IO_MISSING_SCOPE_TIME = UINT64_C(1) << 6,
	SCAN_IO_PARTIAL_SCOPE = UINT64_C(1) << 7
};

ScanIODiagnosisReport ScanIODiagnose(const ScanIODiagnosisInput &input,
									 const ScanIODiagnosisConfig &config = {});
const char *ScanIODiagnosisCodeName(ScanIODiagnosisCode code);
const char *ScanIODiagnosisStatusName(ScanIODiagnosisStatus status);

} // namespace pipeline
} // namespace pg_yaap
