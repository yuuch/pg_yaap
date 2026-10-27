#pragma once
#include "parallel/pipeline/scan_io_metrics.hpp"
#include "parallel/pipeline/types.hpp"
extern "C" {
#include "lib/stringinfo.h"
#include "port/atomics.h"
}
namespace pg_yaap {
namespace pipeline {
struct ScanProfileRecord;
struct ScanIORecord {
	ScanIOStats stats;
	uint64 source_ns = 0, setup_ns = 0, source_calls = 0;
	uint64 submit_api_ns = 0, wait_api_ns = 0, refill_exclusive_ns = 0;
	uint64 shared_hits = 0, shared_reads = 0;
	uint64 submit_sample_cpu_ns = 0, submit_sample_wall_ns = 0;
	uint32 relation_oid = 0, operator_id = UINT32_MAX;
	int32 pid = 0, worker_id = 0;
	bool conflict = false;
	pg_atomic_uint32 publishing;
};
void ScanIOProfileAllocate(PipelineSharedControl *, dsa_area *, uint32 events, uint32 workers);
int ScanIOProfileMode(const ExecCtx &);
void ScanIOProfilePublish(const ExecCtx &, const ScanProfileRecord &, const ScanIOStats *now,
						  const ScanIOStats *previous);
void ScanIOProfileReport(PipelineSharedControl *, dsa_area *, bool success);
void ScanIOProfileAppendAggregateJson(PipelineSharedControl *, dsa_area *, int pipeline, StringInfo,
									  bool complete);
void ScanIOProfileAppendWorkerJson(PipelineSharedControl *, dsa_area *, int worker, uint32 event,
								   StringInfo, bool complete);
} // namespace pipeline
} // namespace pg_yaap
