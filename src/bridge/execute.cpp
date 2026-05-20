/*
 * bridge/execute.cpp — thin C++ shim between PostgreSQL executor hooks and
 * the pg_yaap pipeline runtime.
 *
 * The bridge only accepts optimizer-owned physical plans. PostgreSQL's plan
 * tree is used to carry the registry key through ExecutorStart, not as an
 * execution input.
 *
 *   1. initialize_plan: look up the optimizer bundle for the PlannedStmt and
 *      lower bundle->physical_plan into a pipeline PhysicalOperator tree.
 *
 *   2. delete_plan: tear down the PhysicalOperator tree owned by
 *      state->parallel_plan.
 *
 *   3. execute_query: when called by ExecutorRun_hook (which only happens if
 *      a state was registered), dispatch to pipeline::PgYaapPipelineRun.
 *      Execution failures are raised as ERRORs; a false return is reserved
 *      for the no-state/no-admission case.
 *
 * Result materialization (DataChunk -> TupleTableSlot -> DestReceiver) lives
 * inside the pipeline runtime's OutputSink; the bridge no longer owns it.
 *
 * Spec: PIPELINE_PORT_PLAN.md §15.3.2, §15.4 (P3X-Q1 step 2).
 */

extern "C" {
#include "postgres.h"
#include "executor/executor.h"
#include "nodes/plannodes.h"
#include "utils/elog.h"
#include "utils/memutils.h"
}

#include "execute.h"

#include "optimizer_registry.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline_leader.hpp"
#include "parallel/pipeline/query_state.hpp"
#include "parallel/pipeline/runtime_dsm.hpp"
#include "parallel/pipeline/yaap_pipeline_init.hpp"

extern "C" {
extern bool pg_yaap_trace_hooks;
extern bool pg_yaap_trace_execution_path;
extern bool pg_yaap_parallel;
}

/*
 * Opaque non-null sentinel that satisfies the parallel-enabled gate in
 * pg_yaap_execute_query(). The real TaskScheduler ownership moves into
 * pipeline_leader once M-FRAME-MIN lands its scheduler implementation.
 */
static char pgyaap_parallel_scheduler_sentinel;
extern "C" {

bool
pg_yaap_initialize_plan(QueryDesc *queryDesc, PgYaapQueryState *state_ptr)
{
	auto *state = reinterpret_cast<pg_yaap::PgYaapQueryState *>(state_ptr);

	if (state == nullptr || queryDesc == nullptr || queryDesc->plannedstmt == nullptr)
		return false;

	MemoryContext old_context = MemoryContextSwitchTo(state->context);

	if (pg_yaap_trace_hooks)
		elog(LOG,
			 "pg_yaap: initialize_plan root_nodeTag=%d parallelModeNeeded=%s operation=%d",
			 queryDesc->plannedstmt->planTree != nullptr
				 ? (int) nodeTag(queryDesc->plannedstmt->planTree) : -1,
			 queryDesc->plannedstmt->parallelModeNeeded ? "on" : "off",
			 (int) queryDesc->operation);

	state->parallel_plan = nullptr;
	state->parallel_scheduler = nullptr;

	if (queryDesc->plannedstmt != nullptr)
	{
		if (pg_yaap::OptimizerPlanBundle *bundle =
				pg_yaap::LookupOptimizerPlanBundle(queryDesc->plannedstmt))
		{
			{
				pg_yaap::OptimizerPlanSupportStatus support =
					pg_yaap::AnalyzeOptimizerPlanSupport(*bundle);
				if (!support.supported)
				{
					const char *errmsg_cstr = pstrdup(psprintf(
						"pg_yaap: unsupported optimizer node at %s: %s",
						support.path.c_str(),
						support.detail.c_str()));
					MemoryContextSwitchTo(old_context);
					ereport(ERROR, (errmsg("%s", errmsg_cstr)));
				}
			}
		}
	}

	{
		const char *dsm_err = nullptr;
		if (!pg_yaap::pipeline::CreateRuntimeDsm(state, &dsm_err))
		{
			MemoryContextSwitchTo(old_context);
			ereport(ERROR,
					(errmsg("pg_yaap: %s",
							dsm_err != nullptr ? dsm_err : "runtime DSM/DSA allocation failed")));
		}
	}

	std::unique_ptr<pg_yaap::pipeline::PhysicalOperator> root;
	pg_yaap::OptimizerPlanBundle *optimizer_bundle = nullptr;
	if (queryDesc->plannedstmt != nullptr)
		optimizer_bundle = pg_yaap::LookupOptimizerPlanBundle(queryDesc->plannedstmt);
	if (optimizer_bundle != nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: lowering optimizer physical plan into executor pipeline optimizer_plan=%s",
				 pg_yaap::DescribeOptimizerPlan(*optimizer_bundle).c_str());
		root = pg_yaap::BuildPipelineFromOptimizerPlan(queryDesc, state, *optimizer_bundle);
		if (root == nullptr)
		{
			MemoryContextSwitchTo(old_context);
			pg_yaap::pipeline::DestroyRuntimeDsm(state);
			ereport(ERROR,
					(errmsg("pg_yaap: optimizer physical plan could not be lowered; inspect optimizer-provided bindings and output dictionaries")));
		}
	}

	MemoryContextSwitchTo(old_context);

	if (root == nullptr)
	{
		pg_yaap::pipeline::DestroyRuntimeDsm(state);
		ereport(ERROR,
				(errmsg("pg_yaap: no optimizer physical plan bundle registered for execution")));
	}

	if (!pg_yaap_parallel)
	{
		pg_yaap::pipeline::DestroyRuntimeDsm(state);
		ereport(ERROR,
				(errmsg("pg_yaap: pipeline executor requires pg_yaap.parallel=on")));
	}

	state->parallel_scheduler = &pgyaap_parallel_scheduler_sentinel;
	state->parallel_plan = static_cast<void *>(root.release());
	return true;
}

void
pg_yaap_delete_plan(PgYaapQueryState *state_ptr)
{
	auto *state = reinterpret_cast<pg_yaap::PgYaapQueryState *>(state_ptr);

	if (state == nullptr)
		return;

	state->parallel_scheduler = nullptr;
	if (state->parallel_plan != nullptr)
	{
		auto *root = static_cast<pg_yaap::pipeline::PhysicalOperator *>(state->parallel_plan);
		state->parallel_plan = nullptr;

		PG_TRY();
		{
			delete root;
		}
		PG_CATCH();
		{
			FlushErrorState();
			elog(WARNING, "pg_yaap: error during PhysicalOperator teardown, suppressed");
		}
		PG_END_TRY();
	}

	pg_yaap::pipeline::DestroyRuntimeDsm(state);
}

bool
pg_yaap_execute_query(QueryDesc *queryDesc, PgYaapQueryState *state_ptr,
						ScanDirection direction, uint64 count)
{
	auto *state = reinterpret_cast<pg_yaap::PgYaapQueryState *>(state_ptr);
	const char *failure_reason = nullptr;

	(void) direction;
	(void) count;

	if (state == nullptr)
		return false;
	if (state->parallel_scheduler == nullptr)
		ereport(ERROR,
				(errmsg("pg_yaap: executor state is registered without a scheduler")));

	if (state->parallel_plan == nullptr)
		ereport(ERROR,
				(errmsg("pg_yaap: executor state is registered without a physical plan")));

	bool ok = pg_yaap::pipeline::PgYaapPipelineRun(queryDesc, state, &failure_reason);

	if (!ok)
	{
		ereport(ERROR,
				(errmsg("pg_yaap: optimizer pipeline execution failed: %s",
						failure_reason != nullptr ? failure_reason : "no reason recorded")));
	}

	if (pg_yaap_trace_execution_path)
		elog(LOG, "pg_yaap_path: path=pipeline detail=yaap_pipeline");

	return true;
}

}  /* extern "C" */
