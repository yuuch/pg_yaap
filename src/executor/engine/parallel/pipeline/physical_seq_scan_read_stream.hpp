#pragma once

extern "C" {
#include "postgres.h"
#include "access/heapam.h"
#include "storage/read_stream.h"
}

namespace pg_yaap {
namespace pipeline {

struct ScanBlockTrace;
struct SeqScanReadStreamState {
    ScanBlockTrace *block_trace = nullptr;
	HeapScanDesc scan = nullptr;
	int worker_index = -1;
	uint32_t worker_count = 1;
	bool trace_enabled = false;
	uint64_t chunk_sequence = 0;
	bool yaap_queue = false;
	uint32_t actual_window_pages = 0;
    uint32_t initial_chunk_pages = 0;
};

struct YaapReadStream;
void YaapReadStreamEnableBlockTrace(YaapReadStream *, uint32_t capacity);
struct ScanIOStats;
void YaapReadStreamEnableIOProfile(YaapReadStream *, bool detail, uint32_t requested,
 uint32_t query_cap, uint32_t backend_cap);
const ScanIOStats *YaapReadStreamGetIOStats(const YaapReadStream *);
struct YaapReadStreamStats {
 uint64_t submit_ns = 0, wait_ns = 0, refill_ns = 0;
 uint64_t calls = 0, requests = 0, need_wait = 0, ready_pages = 0;
 uint64_t requested_pages = 0, accepted_pages = 0, peak_pins = 0;
 uint64_t submit_samples = 0, submit_sample_wall_ns = 0, submit_sample_cpu_ns = 0;
 uint64_t wait_calls = 0, wait_samples = 0, wait_sample_wall_ns = 0, wait_sample_cpu_ns = 0;
 uint64_t cpu_clock_failures = 0, refill_calls = 0;
 uint64_t pin_limit_stops = 0, slot_limit_stops = 0, split_requests = 0;
 uint64_t accepted_single = 0, accepted_multi = 0, peak_inflight = 0;
 uint64_t request_limit_stops = 0, new_io_burst_stops = 0;
 uint32_t window_pages = 0, batch_pages = 0, max_requests = 0;
};
YaapReadStream *YaapReadStreamBegin(HeapScanDesc scan, SeqScanReadStreamState &state,
									int worker_index, uint32_t worker_count, bool trace_enabled,
									uint32_t window_pages, bool profile_enabled, uint32_t max_requests, uint32_t batch_pages);
Buffer YaapReadStreamNext(YaapReadStream *stream);
void YaapReadStreamEnd(YaapReadStream *stream);
const YaapReadStreamStats &YaapReadStreamGetStats(const YaapReadStream *stream);

void InstallAggressiveParallelSeqScanReadStream(HeapScanDesc scan, Relation rel,
												SeqScanReadStreamState &state, int worker_index,
												uint32_t worker_count, bool trace_enabled);

} /* namespace pipeline */
} /* namespace pg_yaap */
