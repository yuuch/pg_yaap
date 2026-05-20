extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <memory>
#include <string>
#include <unordered_map>

#include "optimizer/optimizer_core.hpp"
#include "physical/physical_planner.hpp"
#include "planner/logical_planner.hpp"
#include "parallel/pipeline/yaap_pipeline_init.hpp"

#include "optimizer_registry.hpp"

extern "C" bool pg_yaap_trace_hooks;

namespace pg_yaap {

using BundleMap = std::unordered_map<const PlannedStmt *, std::unique_ptr<OptimizerPlanBundle>>;

static BundleMap &
OptimizerPlans()
{
	static BundleMap plans;
	return plans;
}

OptimizerPlanBundle *
LookupOptimizerPlanBundle(PlannedStmt *plannedstmt)
{
	if (plannedstmt == nullptr)
		return nullptr;

	auto &plans = OptimizerPlans();
	auto it = plans.find(plannedstmt);
	return it == plans.end() ? nullptr : it->second.get();
}

}  // namespace pg_yaap

extern "C" {

bool
pg_yaap_try_build_optimizer_plan(Query *parse, void **out_bundle)
{
	if (out_bundle == nullptr)
		return false;

	*out_bundle = nullptr;
	if (parse == nullptr || parse->commandType != CMD_SELECT)
		return false;

	try
	{
		yaap::LogicalPlanner logical_planner;
		auto logical_plan = logical_planner.Plan(parse);
		if (!logical_plan)
			ereport(ERROR,
					(errmsg("pg_yaap: optimizer logical planning returned null")));

		yaap::LogicalOptimizer logical_optimizer;
		auto optimized_plan = logical_optimizer.Optimize(std::move(logical_plan));
		if (!optimized_plan)
			ereport(ERROR,
					(errmsg("pg_yaap: optimizer logical optimization returned null")));

		yaap::PhysicalPlanner physical_planner;
		auto physical_plan = physical_planner.CreatePlan(*optimized_plan);
		if (!physical_plan)
			ereport(ERROR,
					(errmsg("pg_yaap: optimizer physical planning returned null")));

		auto bundle = std::make_unique<pg_yaap::OptimizerPlanBundle>();
		bundle->logical_plan = std::move(optimized_plan);
		bundle->physical_plan = std::move(physical_plan);
		bundle->output_targetlist = static_cast<List *>(copyObjectImpl(parse->targetList));
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: built optimizer physical plan %s",
				 pg_yaap::DescribeOptimizerPlan(*bundle).c_str());
		*out_bundle = bundle.release();
		return true;
	}
	catch (const std::exception &e)
	{
		const std::string message = e.what() != nullptr ? e.what() : "";
		if (message.rfind("Unsupported query shape:", 0) == 0 ||
			message.rfind("Unsupported ", 0) == 0)
			return false;
		ereport(ERROR,
				(errmsg("pg_yaap: optimizer failed: %s", e.what())));
	}
	catch (...)
	{
		ereport(ERROR,
				(errmsg("pg_yaap: optimizer failed with unknown exception")));
	}

	return false;
}

void
pg_yaap_register_optimizer_plan(PlannedStmt *plannedstmt, void *bundle)
{
	if (plannedstmt == nullptr || bundle == nullptr)
		return;

	auto &plans = pg_yaap::OptimizerPlans();
	plans[plannedstmt] = std::unique_ptr<pg_yaap::OptimizerPlanBundle>(
		static_cast<pg_yaap::OptimizerPlanBundle *>(bundle));
}

void *
pg_yaap_lookup_optimizer_plan(PlannedStmt *plannedstmt)
{
	return pg_yaap::LookupOptimizerPlanBundle(plannedstmt);
}

void
pg_yaap_unregister_optimizer_plan(PlannedStmt *plannedstmt)
{
	if (plannedstmt == nullptr)
		return;

	auto &plans = pg_yaap::OptimizerPlans();
	plans.erase(plannedstmt);
}

void
pg_yaap_discard_optimizer_plan(void *bundle)
{
	delete static_cast<pg_yaap::OptimizerPlanBundle *>(bundle);
}

TupleDesc
pg_yaap_build_optimizer_tupdesc(void *bundle)
{
	if (bundle == nullptr)
		return nullptr;
	return pg_yaap::BuildOptimizerOutputTupleDesc(
		*static_cast<pg_yaap::OptimizerPlanBundle *>(bundle));
}

char *
pg_yaap_describe_optimizer_plan(void *bundle)
{
	if (bundle == nullptr)
		return nullptr;
	return pstrdup(
		pg_yaap::DescribeOptimizerPlan(
			*static_cast<pg_yaap::OptimizerPlanBundle *>(bundle)).c_str());
}

}  // extern "C"
