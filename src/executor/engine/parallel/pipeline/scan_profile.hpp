#pragma once
#include "parallel/pipeline/types.hpp"
extern "C"
{
#include "pg_config_manual.h"
#include "portability/instr_time.h"
}
namespace pg_yaap {
namespace pipeline {
/* Cumulative worker-exclusive values. Sample mode tuple times are estimates;
 * Primary stage times are elapsed (including descheduling). Explicit
 * read_*_sample_cpu_ns fields alone contain sampled thread CPU time. */
struct ScanProfileRecord
{
	uint64 source_ns = 0, pg_access_ns = 0, row_to_column_ns = 0, predicate_ns = 0, setup_ns = 0;
	uint64 visible_rows = 0, output_rows = 0, samples = 0;
	uint64 load_ns = 0, prepare_ns = 0, tuple_ns = 0, qual_ns = 0, proj_ns = 0;
	uint64 calls = 0, pages = 0, qual_rows = 0, proj_rows = 0, qual_samples = 0, proj_samples = 0;
	uint32 operator_id = 0, relation_oid = 0, mode = 0, pid = 0;
	uint32 pid_conflict = 0;
	uint64 sample_clock_overhead_ns = 0;
	uint64 read_submit_ns = 0, read_wait_ns = 0, read_refill_ns = 0;
	uint64 read_requests = 0, read_need_wait = 0;
	uint64 read_requested_pages = 0, read_accepted_pages = 0;
	uint32 read_window_pages = 0, read_batch_pages = 0, read_stream_mode = 0;
	uint32 read_request_limit = 0;
	uint64 read_request_limit_stops = 0, read_new_io_burst_stops = 0;
	uint64 read_submit_samples = 0;
	uint64 read_submit_sample_wall_ns = 0;
	uint64 read_submit_sample_cpu_ns = 0;
	uint64 read_wait_calls = 0;
	uint64 read_wait_samples = 0;
	uint64 read_wait_sample_wall_ns = 0;
	uint64 read_wait_sample_cpu_ns = 0;
	uint64 read_cpu_clock_failures = 0;
	uint64 read_refill_calls = 0;
	uint64 read_pin_limit_stops = 0;
	uint64 read_slot_limit_stops = 0;
	uint64 read_split_requests = 0;
	uint64 read_accepted_single = 0;
	uint64 read_accepted_multi = 0;
	uint64 read_peak_inflight = 0, read_ready_pages = 0;
	uint64 shared_blks_hit = 0, shared_blks_read = 0;
};
bool
ScanProfileAllocate(PipelineSharedControl*, dsa_area*, uint32, uint32);
ScanProfileRecord
ScanProfileSnapshot(PipelineSharedControl*, dsa_area*, int, EventId);
void
ScanProfileReport(PipelineSharedControl*, dsa_area*, bool success = true);
int
ScanProfileMode(const ExecCtx&);
uint64
ScanProfileElapsed(instr_time end, instr_time start);
void
ScanProfilePublish(const ExecCtx&, const ScanProfileRecord&);
}
}
