#pragma once

/*
 * pipeline/query_state.hpp — opaque per-query handle.
 *
 * M-FRAME-MIN step 2: intentionally minimal. The outer bridge layer
 * (src/bridge/state.c, src/bridge/execute.cpp, src/bridge/pg_yaap.c) holds
 * and threads PgYaapQueryState*
 * through the executor hook lifecycle but treats the parallel_* fields as
 * opaque void*. Real types are owned by the pipeline runtime once it is
 * written (M-FRAME-MIN step 3+).
 *
 * Layout MUST match struct PgYaapQueryState in src/bridge/state.c.
 *
 * Spec: PIPELINE_PORT_PLAN.md §15.3.2.
 */

extern "C" {
#include "postgres.h"
#include "utils/dsa.h"
#include "utils/memutils.h"

struct dsm_segment;
}

namespace pg_yaap {

struct PgYaapQueryState {
	MemoryContext context;
	void *parallel_plan;       /* owned by pipeline runtime (PhysicalOperator tree) */
	void *parallel_scheduler;  /* owned by pipeline runtime (TaskScheduler) */

	struct dsm_segment *runtime_dsm;
	dsa_area *runtime_dsa;
};

}  /* namespace pg_yaap */
