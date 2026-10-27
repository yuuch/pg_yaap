#pragma once
#include <cstdint>
#include <limits>

namespace pg_yaap {
namespace pipeline {
enum class MetricAvailability : uint8_t { EXACT, ESTIMATED, UNAVAILABLE };
enum class MetricSource : uint8_t { YAAP_INSTRUMENTATION, POSTGRES_STATISTICS, OS_OBSERVATION };
enum class MetricSemantic : uint8_t {
	VALUE,
	API_ENVELOPE,
	OBSERVED_UPPER_BOUND,
	PG_COUNTER_DELTA,
	SAMPLED
};
struct ScanIOMetric {
	uint64_t value = 0;
	MetricAvailability availability = MetricAvailability::UNAVAILABLE;
	MetricSource source = MetricSource::YAAP_INSTRUMENTATION;
	MetricSemantic semantic = MetricSemantic::VALUE;
};
struct ScanIOHistogram {
	static constexpr unsigned kBuckets = 64;
	uint64_t bins[kBuckets]{};
	uint64_t count = 0, sum = 0, minimum = UINT64_MAX, maximum = 0;
	bool saturated = false;
	static uint64_t AddSaturated(uint64_t a, uint64_t b, bool &overflow) {
		if (UINT64_MAX - a < b) {
			overflow = true;
			return UINT64_MAX;
		}
		return a + b;
	}
	static unsigned Bucket(uint64_t value) {
		return value ? 64u - static_cast<unsigned>(__builtin_clzll(value)) : 0;
	}
	void Observe(uint64_t value) {
		unsigned b = Bucket(value);
		if (b >= kBuckets)
			b = kBuckets - 1;
		bins[b] = AddSaturated(bins[b], 1, saturated);
		count = AddSaturated(count, 1, saturated);
		sum = AddSaturated(sum, value, saturated);
		if (value < minimum)
			minimum = value;
		if (value > maximum)
			maximum = value;
	}
	void Merge(const ScanIOHistogram &other) {
		saturated = saturated || other.saturated;
		for (unsigned i = 0; i < kBuckets; i++)
			bins[i] = AddSaturated(bins[i], other.bins[i], saturated);
		count = AddSaturated(count, other.count, saturated);
		sum = AddSaturated(sum, other.sum, saturated);
		if (other.count && other.minimum < minimum)
			minimum = other.minimum;
		if (other.maximum > maximum)
			maximum = other.maximum;
	}
	void MergeDelta(const ScanIOHistogram &now, const ScanIOHistogram &before) {
		saturated = saturated || now.saturated || before.saturated;
		for (unsigned i = 0; i < kBuckets; i++)
			bins[i] = AddSaturated(bins[i], now.bins[i] - before.bins[i], saturated);
		const uint64_t n = now.count - before.count;
		count = AddSaturated(count, n, saturated);
		sum = AddSaturated(sum, now.sum - before.sum, saturated);
		if (n && now.minimum < minimum)
			minimum = now.minimum;
		if (n && now.maximum > maximum)
			maximum = now.maximum;
	}
	// Quantiles are bucket upper bounds, never exact observed percentiles.
	bool QuantileUpper(unsigned percent, uint64_t &upper) const {
		if (!count || saturated || percent > 100)
			return false;
		uint64_t target = (count / 100) * percent + ((count % 100) * percent + 99) / 100;
		if (!target)
			target = 1;
		uint64_t seen = 0;
		for (unsigned i = 0; i < kBuckets; i++) {
			seen += bins[i];
			if (seen >= target) {
				upper = i == 0 ? 0 : (i == 63 ? UINT64_MAX : (UINT64_C(1) << i) - 1);
				if (upper > maximum)
					upper = maximum;
				return true;
			}
		}
		return false;
	}
};
#define YAAP_SCAN_BUFFER_COUNTERS(X) \
    X(buffer_alloc_calls) \
    X(buffer_lookup_hits) \
    X(buffer_lookup_misses) \
    X(buffer_ring_attempts) \
    X(buffer_ring_hits) \
    X(buffer_freelist_candidates) \
    X(buffer_freelist_hits) \
    X(buffer_clock_candidates) \
    X(buffer_clock_usage_decrements) \
    X(buffer_clock_pinned_skips) \
    X(buffer_victim_retries) \
    X(buffer_dirty_writebacks) \
    X(buffer_profiled_start_calls) \
    X(buffer_sampled_start_calls) \
    X(buffer_sampled_pages) \
    X(buffer_alloc_total_ns) \
    X(buffer_lookup_ns) \
    X(buffer_mapping_lock_ns) \
    X(buffer_victim_ns) \
    X(buffer_strategy_ns) \
    X(buffer_ring_ns) \
    X(buffer_freelist_ns) \
    X(buffer_clock_ns) \
    X(buffer_dirty_write_ns) \
    X(buffer_invalidate_ns) \
    X(buffer_pin_ns)
#define YAAP_SCAN_IO_COUNTERS(X)                                                                   \
	X(streams_created)                                                                             \
	X(consumed_pages) X(known_ready_at_need_pages) X(readiness_unknown_pages)                      \
		X(request_not_submitted_at_need_pages) X(submit_after_need_pages) X(api_lead_pages)        \
			X(next_total_ns) X(next_calls) X(wait_validation_ns) X(wait_validation_calls)          \
				X(api_start_calls) X(api_need_wait_calls) X(api_accepted_pages)                    \
					X(start_ready_pages) X(start_need_wait_pages) X(pin_binding_events)            \
						X(request_binding_events) X(window_full_events) X(slot_binding_events)     \
							X(burst_binding_events) X(validation_observations)                     \
								X(known_ready_pending_cohort_pages)                                \
									X(pending_cohort_consumed_pages)                               \
										X(configured_sync_need_wait_calls) X(api_lead_sum_ns)      \
											X(need_to_submit_return_sum_ns)                        \
												X(validated_ready_upper_sum_ns) X(block_assignment_ns) X(pin_budget_check_ns) \
    X(submit_cpu_samples) X(submit_sample_wall_ns) X(submit_sample_cpu_ns) \
    X(wait_cpu_samples) X(wait_sample_wall_ns) X(wait_sample_cpu_ns) X(cpu_clock_failures) \
    X(pg_start_read_ns) X(pg_wait_read_ns) X(pg_wait_new_read_pages) \
    X(pg_start_timed_calls) X(pg_wait_timed_calls) \
    X(kernel_profiled_operations) X(kernel_requests) X(kernel_requested_bytes) X(kernel_completed_bytes) \
    X(kernel_queued_requests) X(kernel_worker_requests) X(kernel_queue_full_sync_requests) \
    X(kernel_forced_sync_requests) X(kernel_queue_ns) X(kernel_reopen_ns) X(kernel_read_ns) \
    X(kernel_shared_complete_ns) X(kernel_enqueue_to_shared_ns) X(kernel_timed_read_requests) \
    X(kernel_failed_requests) X(kernel_unclassified_requests) \
    X(page_consumer_gap_ns) X(page_consumer_gap_count) \
    YAAP_SCAN_BUFFER_COUNTERS(X)
#define YAAP_SCAN_IO_HISTOGRAMS(X)                                                                 \
	X(api_return_to_need_ns)                                                                       \
	X(need_to_submit_return_ns) X(validated_ready_upper_ns) X(accepted_pages_per_call)             \
		X(need_wait_accepted_pages) X(submitted_ahead_at_need) X(reserved_at_need)                 \
			X(unwaited_at_need) X(wait_api_ns) X(start_api_ns) \
    X(kernel_operation_queue_ns) X(kernel_operation_read_ns) X(page_consumer_gap_distribution_ns)
struct ScanIOStats {
#define FIELD(n) uint64_t n = 0;
	YAAP_SCAN_IO_COUNTERS(FIELD)
#undef FIELD
#define HIST(n) ScanIOHistogram n;
	YAAP_SCAN_IO_HISTOGRAMS(HIST)
#undef HIST
	uint64_t peak_submitted_ahead = 0, peak_reserved = 0, peak_unwaited = 0;
	uint32_t enabled = 0, detail = 0, requested_pages = 0, granted_pages = 0;
	uint32_t query_share_cap = 0, backend_share_cap = 0, pg_pin_cap = 0, ring_capacity = 0;
	uint32_t request_cap = 0, batch_pages = 0, block_size = 0, io_method = 0,
			 allocation_chunk_pages = 0;
	uint32_t io_workers = 0, effective_io_concurrency = 0, io_combine_limit = 0,
			 io_max_concurrency = 0;
	uint64_t shared_buffers_pages = 0;
    uint32_t kernel_feature_supported = 0;
    uint32_t buffer_feature_supported = 0;
	bool mixed_config = false;
};
} // namespace pipeline
} // namespace pg_yaap
