#include "postgres.h"

#include "access/parallel.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#include "executor/executor.h"
#include "jit/jit.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "parser/scansup.h"
#include "storage/dsm_registry.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "tcop/utility.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"

#include "execute.h"
#include "optimizer_registry.h"
#include "state.h"

#if defined(__APPLE__) || defined(__linux__) || defined(HAVE_DLOPEN)
#include <dlfcn.h>
#endif

PG_MODULE_MAGIC;

static planner_hook_type prev_planner_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static ExplainOneQuery_hook_type prev_ExplainOneQuery = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static int
pg_yaap_forced_jit_flags(void)
{
	return PGJIT_PERFORM | PGJIT_EXPR | PGJIT_DEFORM | PGJIT_OPT3 | PGJIT_INLINE;
}

static bool pg_yaap_enabled = true;
bool pg_yaap_trace_hooks = false;
bool pg_yaap_trace_execution_path = false;
bool pg_yaap_jit_deform = true;
bool pg_yaap_parallel = true;
int pg_yaap_parallel_max_workers = 4;
int pg_yaap_parallel_min_relation_blocks = 1024;
bool pg_yaap_parallel_leader_participation = true;
bool pg_yaap_parallel_experimental_hash_pipeline = false;
bool pg_yaap_disable_jit_for_parallel_worker = false;
bool pg_yaap_profile = false;
char pg_yaap_bgworker_library_path[MAXPGPATH] = "pg_yaap";
static int pg_yaap_utility_nesting = 0;

#if PG_VERSION_NUM >= 190000
static PlannedStmt *pg_yaap_planner_hook(Query *parse, const char *query_string,
										 int cursorOptions, ParamListInfo boundParams,
										 ExplainState *es);
#else
static PlannedStmt *pg_yaap_planner_hook(Query *parse, const char *query_string,
										 int cursorOptions, ParamListInfo boundParams);
#endif
static void pg_yaap_ExplainOneQuery(Query *query,
									int cursorOptions,
									IntoClause *into,
									ExplainState *es,
									const char *queryString,
									ParamListInfo params,
									QueryEnvironment *queryEnv);
static void pg_yaap_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pg_yaap_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count);
static void pg_yaap_ExecutorFinish(QueryDesc *queryDesc);
static void pg_yaap_ExecutorEnd(QueryDesc *queryDesc);
static PlannedStmt *pg_yaap_build_plannedstmt(Query *parse);
static void pg_yaap_start_optimizer_executor(QueryDesc *queryDesc, int eflags);
static void pg_yaap_ProcessUtility(PlannedStmt *pstmt,
									  const char *queryString,
									  bool readOnlyTree,
									  ProcessUtilityContext context,
									  ParamListInfo params,
									  QueryEnvironment *queryEnv,
									  DestReceiver *dest,
									  QueryCompletion *qc);

static bool
pg_yaap_optimizer_bundle_has_tupdesc(void *optimizer_bundle)
{
	TupleDesc	tupdesc;

	if (optimizer_bundle == NULL)
		return false;
	tupdesc = pg_yaap_build_optimizer_tupdesc(optimizer_bundle);
	if (tupdesc == NULL)
		return false;
	FreeTupleDesc(tupdesc);
	return true;
}

extern int pg_yaap_runtime_dsa_tranche_id(void);
extern void pg_yaap_proc_exit_release_jit_contexts(int code, Datum arg);

#ifndef USE_LLVM
void
pg_yaap_proc_exit_release_jit_contexts(int code, Datum arg)
{
	(void) code;
	(void) arg;
}
#endif

void _PG_init(void);
void _PG_fini(void);

static const char *
pg_yaap_plan_node_name(Plan *plan)
{
	if (plan == NULL)
		return "NULL";
	if (IsA(plan, Gather))
		return "Gather";
	if (IsA(plan, GatherMerge))
		return "GatherMerge";
	if (IsA(plan, Agg))
		return "Agg";
	if (IsA(plan, Sort))
		return "Sort";
	if (IsA(plan, Limit))
		return "Limit";
	if (IsA(plan, SeqScan))
		return "SeqScan";
	if (IsA(plan, HashJoin))
		return "HashJoin";
	if (IsA(plan, MergeJoin))
		return "MergeJoin";
	if (IsA(plan, NestLoop))
		return "NestLoop";
	if (IsA(plan, SubqueryScan))
		return "SubqueryScan";
	if (IsA(plan, Material))
		return "Material";
	if (IsA(plan, Hash))
		return "Hash";
	return "Other";
}

static bool
pg_yaap_query_string_starts_with(const char *query_string, const char *keyword)
{
	size_t		i;

	if (query_string == NULL || keyword == NULL)
		return false;

	while (*query_string != '\0' && scanner_isspace((unsigned char) *query_string))
		query_string++;

	for (i = 0; keyword[i] != '\0'; ++i)
	{
		if (query_string[i] == '\0')
			return false;
		if (pg_ascii_tolower((unsigned char) query_string[i]) !=
			pg_ascii_tolower((unsigned char) keyword[i]))
			return false;
	}

	return true;
}

static bool
pg_yaap_skip_optimizer_for_query_string(const char *query_string)
{
	return pg_yaap_query_string_starts_with(query_string, "create") ||
		pg_yaap_query_string_starts_with(query_string, "drop") ||
		pg_yaap_query_string_starts_with(query_string, "alter");
}

#if PG_VERSION_NUM >= 190000
static void
pg_yaap_init_runtime_dsa_tranche(void *ptr, void *arg)
{
	int *tranche_id = (int *) ptr;

	(void) arg;
	*tranche_id = LWLockNewTrancheId("pg_yaap_runtime_dsa");
}
#else
static void
pg_yaap_init_runtime_dsa_tranche(void *ptr)
{
	int *tranche_id = (int *) ptr;

	*tranche_id = LWLockNewTrancheId();
}
#endif

int
pg_yaap_runtime_dsa_tranche_id(void)
{
	bool found;
	int *tranche_id;

	tranche_id = (int *) GetNamedDSMSegment("pg_yaap_runtime_dsa_tranche_id",
											sizeof(int),
											pg_yaap_init_runtime_dsa_tranche,
											&found);
#if PG_VERSION_NUM < 190000
	LWLockRegisterTranche(*tranche_id, "pg_yaap_runtime_dsa");
#endif
	return *tranche_id;
}

static void
pg_yaap_capture_bgworker_library_path(void)
{
#if defined(__APPLE__) || defined(__linux__) || defined(HAVE_DLOPEN)
	Dl_info info;

	if (dladdr((void *) _PG_init, &info) != 0 &&
		info.dli_fname != NULL &&
		info.dli_fname[0] != '\0')
	{
		strlcpy(pg_yaap_bgworker_library_path, info.dli_fname, MAXPGPATH);
		return;
	}
#endif

	strlcpy(pg_yaap_bgworker_library_path, "pg_yaap", MAXPGPATH);
}

void
_PG_init(void)
{
	pg_yaap_capture_bgworker_library_path();

	DefineCustomBoolVariable("pg_yaap.enabled",
							 "Enable the pg_yaap planner/executor hooks.",
							 NULL,
							 &pg_yaap_enabled,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_yaap.trace_hooks",
							 "Emit log messages when pg_yaap planner/executor hooks run.",
							 NULL,
							 &pg_yaap_trace_hooks,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_yaap.trace_execution_path",
							 "Emit one low-noise execution path log line per query.",
							 NULL,
							 &pg_yaap_trace_execution_path,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_yaap.jit_deform",
							 "Enable LLVM JIT deform in the YAAP executor path.",
							 NULL,
							 &pg_yaap_jit_deform,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_yaap.parallel",
							 "Enable experimental parallel lowering in the YAAP executor path.",
							 NULL,
							 &pg_yaap_parallel,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("pg_yaap.parallel_max_workers",
							"Maximum number of experimental YAAP parallel workers.",
							NULL,
							&pg_yaap_parallel_max_workers,
							4,
							0,
							1024,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_yaap.parallel_min_relation_blocks",
							"Minimum relation size in blocks before experimental YAAP parallel lowering is considered.",
							NULL,
							&pg_yaap_parallel_min_relation_blocks,
							1024,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("pg_yaap.parallel_leader_participation",
							 "Allow the leader to participate in experimental YAAP block-pool execution.",
							 NULL,
							 &pg_yaap_parallel_leader_participation,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_yaap.parallel_experimental_hash_pipeline",
							 "Enable the experimental leader-only HashBuild->Finalize->Probe pipeline DAG path.",
							 NULL,
							 &pg_yaap_parallel_experimental_hash_pipeline,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_yaap.profile",
							 "Emit YAAP per-stage pipeline timing after each offloaded query.",
							 NULL,
							 &pg_yaap_profile,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	pg_yaap_init_state_table();
	before_shmem_exit(pg_yaap_proc_exit_release_jit_contexts, (Datum) 0);

	prev_planner_hook = planner_hook;
	planner_hook = pg_yaap_planner_hook;

	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = pg_yaap_ProcessUtility;

	prev_ExplainOneQuery = ExplainOneQuery_hook;
	ExplainOneQuery_hook = pg_yaap_ExplainOneQuery;

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pg_yaap_ExecutorStart;

	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pg_yaap_ExecutorRun;

	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pg_yaap_ExecutorFinish;

	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pg_yaap_ExecutorEnd;
}

void
_PG_fini(void)
{
	planner_hook = prev_planner_hook;
	ProcessUtility_hook = prev_ProcessUtility;
	ExplainOneQuery_hook = prev_ExplainOneQuery;
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
}

static PlannedStmt *
pg_yaap_planner_hook(Query *parse, const char *query_string,
#if PG_VERSION_NUM >= 190000
					 int cursorOptions, ParamListInfo boundParams,
					 ExplainState *es)
#else
					 int cursorOptions, ParamListInfo boundParams)
#endif
{
	PlannedStmt *result = NULL;
	void *optimizer_bundle = NULL;
#if PG_VERSION_NUM >= 190000
	(void) es;
#endif

	if (IsParallelWorker())
	{
		if (prev_planner_hook)
#if PG_VERSION_NUM >= 190000
			return prev_planner_hook(parse, query_string, cursorOptions, boundParams, es);
#else
			return prev_planner_hook(parse, query_string, cursorOptions, boundParams);
#endif
#if PG_VERSION_NUM >= 190000
		return standard_planner(parse, query_string, cursorOptions, boundParams, es);
#else
		return standard_planner(parse, query_string, cursorOptions, boundParams);
#endif
	}

	if (pg_yaap_utility_nesting > 0)
	{
		if (prev_planner_hook)
#if PG_VERSION_NUM >= 190000
			return prev_planner_hook(parse, query_string, cursorOptions, boundParams, es);
#else
			return prev_planner_hook(parse, query_string, cursorOptions, boundParams);
#endif
#if PG_VERSION_NUM >= 190000
		return standard_planner(parse, query_string, cursorOptions, boundParams, es);
#else
		return standard_planner(parse, query_string, cursorOptions, boundParams);
#endif
	}

	if (pg_yaap_skip_optimizer_for_query_string(query_string))
	{
		if (prev_planner_hook)
#if PG_VERSION_NUM >= 190000
			return prev_planner_hook(parse, query_string, cursorOptions, boundParams, es);
#else
			return prev_planner_hook(parse, query_string, cursorOptions, boundParams);
#endif
#if PG_VERSION_NUM >= 190000
		return standard_planner(parse, query_string, cursorOptions, boundParams, es);
#else
		return standard_planner(parse, query_string, cursorOptions, boundParams);
#endif
	}

	if (pg_yaap_enabled && parse != NULL && parse->commandType == CMD_SELECT)
		(void) pg_yaap_try_build_optimizer_plan(parse, &optimizer_bundle);

	if (pg_yaap_enabled && optimizer_bundle != NULL)
	{
		result = pg_yaap_build_plannedstmt(parse);
		pg_yaap_register_optimizer_plan(result, optimizer_bundle);
		return result;
	}

	if (prev_planner_hook)
#if PG_VERSION_NUM >= 190000
		return prev_planner_hook(parse, query_string, cursorOptions, boundParams, es);
#else
		return prev_planner_hook(parse, query_string, cursorOptions, boundParams);
#endif
#if PG_VERSION_NUM >= 190000
	result = standard_planner(parse, query_string, cursorOptions, boundParams, es);
#else
	result = standard_planner(parse, query_string, cursorOptions, boundParams);
#endif
	return result;
}

static void
pg_yaap_ProcessUtility(PlannedStmt *pstmt,
						 const char *queryString,
						 bool readOnlyTree,
						 ProcessUtilityContext context,
						 ParamListInfo params,
						 QueryEnvironment *queryEnv,
						 DestReceiver *dest,
						 QueryCompletion *qc)
{
	pg_yaap_utility_nesting++;
	PG_TRY();
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
		pg_yaap_utility_nesting--;
	}
	PG_CATCH();
	{
		pg_yaap_utility_nesting--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static void
pg_yaap_ExplainOneQuery(Query *query,
						  int cursorOptions,
						  IntoClause *into,
						  ExplainState *es,
						  const char *queryString,
						  ParamListInfo params,
						  QueryEnvironment *queryEnv)
{
	void *optimizer_bundle = NULL;

	if (IsParallelWorker())
	{
		if (prev_ExplainOneQuery)
			prev_ExplainOneQuery(query, cursorOptions, into, es, queryString, params, queryEnv);
		else
			standard_ExplainOneQuery(query, cursorOptions, into, es, queryString, params, queryEnv);
		return;
	}

	if (pg_yaap_enabled && query != NULL && query->commandType == CMD_SELECT)
		(void) pg_yaap_try_build_optimizer_plan(query, &optimizer_bundle);

	if (pg_yaap_enabled && optimizer_bundle != NULL)
	{
		char *plan_text;

		if (es != NULL && es->analyze)
		{
			pg_yaap_discard_optimizer_plan(optimizer_bundle);
			ereport(ERROR,
					(errmsg("pg_yaap: EXPLAIN ANALYZE is not supported in optimizer-only mode")));
		}

		plan_text = pg_yaap_describe_optimizer_plan(optimizer_bundle);
		ExplainOpenGroup("Query", NULL, true, es);
		ExplainPropertyText("Planner", "YAAP optimizer", es);
		ExplainPropertyText("YAAP Physical Plan",
							plan_text != NULL ? plan_text : "<null>",
							es);
		ExplainCloseGroup("Query", NULL, true, es);
		if (plan_text != NULL)
			pfree(plan_text);
		pg_yaap_discard_optimizer_plan(optimizer_bundle);
		return;
	}

	if (prev_ExplainOneQuery)
		prev_ExplainOneQuery(query, cursorOptions, into, es, queryString, params, queryEnv);
	else
		standard_ExplainOneQuery(query, cursorOptions, into, es, queryString, params, queryEnv);
}

static PlannedStmt *
pg_yaap_build_plannedstmt(Query *parse)
{
	PlannedStmt *result = makeNode(PlannedStmt);
	Result *plan = makeNode(Result);

	result->commandType = parse->commandType;
	result->queryId = parse->queryId;
#if PG_VERSION_NUM >= 190000
	result->planOrigin = PLAN_STMT_INTERNAL;
#endif
	result->hasReturning = (parse->returningList != NIL);
	result->hasModifyingCTE = parse->hasModifyingCTE;
	result->canSetTag = parse->canSetTag;
	result->transientPlan = false;
	result->dependsOnRole = false;
	result->parallelModeNeeded = false;
	result->jitFlags = pg_yaap_forced_jit_flags();
	plan->plan.targetlist = copyObject(parse->targetList);
	plan->plan.qual = NIL;
	plan->plan.lefttree = NULL;
	plan->plan.righttree = NULL;
	plan->plan.initPlan = NIL;
	plan->plan.extParam = NULL;
	plan->plan.allParam = NULL;
	plan->plan.parallel_aware = false;
	plan->plan.parallel_safe = false;
	plan->plan.async_capable = false;
	plan->plan.plan_rows = 1.0;
	plan->plan.plan_width = list_length(parse->targetList);
	result->planTree = &plan->plan;
	result->partPruneInfos = NIL;
	result->rtable = copyObject(parse->rtable);
	result->unprunableRelids = NULL;
	result->permInfos = copyObject(parse->rteperminfos);
	result->resultRelations = NIL;
	result->appendRelations = NIL;
	result->subplans = NIL;
	result->rewindPlanIDs = NULL;
	result->rowMarks = copyObject(parse->rowMarks);
	result->relationOids = NIL;
	result->invalItems = NIL;
	result->paramExecTypes = NIL;
	result->utilityStmt = NULL;
#if PG_VERSION_NUM >= 190000
	result->extension_state = NIL;
#endif
	result->stmt_location = parse->stmt_location;
	result->stmt_len = parse->stmt_len;

	return result;
}

static void
pg_yaap_start_optimizer_executor(QueryDesc *queryDesc, int eflags)
{
	void *bundle_ptr;
	EState *estate;
	MemoryContext oldcontext;

	if (queryDesc == NULL || queryDesc->plannedstmt == NULL)
		ereport(ERROR, (errmsg("pg_yaap: optimizer-only executor start missing PlannedStmt")));

	bundle_ptr = pg_yaap_lookup_optimizer_plan(queryDesc->plannedstmt);
	if (bundle_ptr == NULL)
		ereport(ERROR, (errmsg("pg_yaap: optimizer-only executor start missing optimizer bundle")));

	Assert(queryDesc->estate == NULL);
	estate = CreateExecutorState();
	queryDesc->estate = estate;

	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	estate->es_param_list_info = queryDesc->params;
	estate->es_sourceText = queryDesc->sourceText;
	estate->es_queryEnv = queryDesc->queryEnv;
	estate->es_snapshot = RegisterSnapshot(queryDesc->snapshot);
	estate->es_crosscheck_snapshot = RegisterSnapshot(queryDesc->crosscheck_snapshot);
	estate->es_top_eflags = eflags | EXEC_FLAG_SKIP_TRIGGERS;
	estate->es_instrument = queryDesc->instrument_options;
	estate->es_jit_flags = queryDesc->plannedstmt->jitFlags != 0 ?
		queryDesc->plannedstmt->jitFlags :
		pg_yaap_forced_jit_flags();

	if (!ExecCheckPermissions(queryDesc->plannedstmt->rtable,
							  queryDesc->plannedstmt->permInfos,
							  true))
		ereport(ERROR, (errmsg("pg_yaap: permission check failed")));

	queryDesc->tupDesc = pg_yaap_build_optimizer_tupdesc(bundle_ptr);
	if (queryDesc->tupDesc == NULL)
		ereport(ERROR, (errmsg("pg_yaap: optimizer output tuple descriptor is null")));
	queryDesc->planstate = NULL;

	MemoryContextSwitchTo(oldcontext);
}

static void
pg_yaap_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	PgYaapQueryState *state;
	void *optimizer_bundle = queryDesc != NULL && queryDesc->plannedstmt != NULL ?
		pg_yaap_lookup_optimizer_plan(queryDesc->plannedstmt) : NULL;
	bool optimizer_only = queryDesc != NULL &&
		queryDesc->plannedstmt != NULL &&
		optimizer_bundle != NULL &&
		pg_yaap_optimizer_bundle_has_tupdesc(optimizer_bundle);
	Plan *plan = queryDesc != NULL && queryDesc->plannedstmt != NULL ?
		queryDesc->plannedstmt->planTree : NULL;

	if (!optimizer_only)
	{
		if (prev_ExecutorStart)
			prev_ExecutorStart(queryDesc, eflags);
		else
			standard_ExecutorStart(queryDesc, eflags);
	}
	else
		pg_yaap_start_optimizer_executor(queryDesc, eflags);

	if (!pg_yaap_enabled)
		return;

	if (pg_yaap_trace_hooks)
		elog(LOG,
			 "pg_yaap: ExecutorStart pid=%d parallel_worker=%s root=%s nodeTag=%d",
			 MyProcPid,
			 IsParallelWorker() ? "on" : "off",
			 pg_yaap_plan_node_name(plan),
			 plan != NULL ? (int) nodeTag(plan) : -1);

	if (IsParallelWorker())
		return;

	state = pg_yaap_try_build_query_state(queryDesc, eflags);
	if (state == NULL)
	{
		if (!optimizer_only)
			return;
		ereport(ERROR, (errmsg("pg_yaap: optimizer-only query state admission failed")));
	}
	if (pg_yaap_initialize_plan(queryDesc, state))
		pg_yaap_register_state(queryDesc, state);
	else
	{
		pg_yaap_close_query_state(state);
		if (!optimizer_only)
			return;
		ereport(ERROR, (errmsg("pg_yaap: optimizer-only plan initialization returned false")));
	}
}

static void
pg_yaap_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
{
	PgYaapQueryState *state = pg_yaap_lookup_state(queryDesc);

	if (IsParallelWorker())
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
		return;
	}

	if (state != NULL)
	{
		if (queryDesc->estate->es_snapshot == NULL)
			queryDesc->estate->es_snapshot = GetActiveSnapshot();

		if (pg_yaap_execute_query(queryDesc, state, direction, count))
			return;
	}

	if (prev_ExecutorRun)
		prev_ExecutorRun(queryDesc, direction, count);
	else
		standard_ExecutorRun(queryDesc, direction, count);
}

static void
pg_yaap_ExecutorFinish(QueryDesc *queryDesc)
{
	if (IsParallelWorker())
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		return;
	}

	if (queryDesc != NULL && queryDesc->plannedstmt != NULL &&
		pg_yaap_lookup_optimizer_plan(queryDesc->plannedstmt) != NULL)
	{
		standard_ExecutorFinish(queryDesc);
		return;
	}
	if (prev_ExecutorFinish)
		prev_ExecutorFinish(queryDesc);
	else
		standard_ExecutorFinish(queryDesc);
}

static void
pg_yaap_ExecutorEnd(QueryDesc *queryDesc)
{
	PgYaapQueryState *state = pg_yaap_lookup_state(queryDesc);
	bool optimizer_only = queryDesc != NULL && queryDesc->plannedstmt != NULL &&
		pg_yaap_lookup_optimizer_plan(queryDesc->plannedstmt) != NULL;

	if (IsParallelWorker())
	{
		if (prev_ExecutorEnd)
			prev_ExecutorEnd(queryDesc);
		else
			standard_ExecutorEnd(queryDesc);
		return;
	}

	if (state != NULL)
		pg_yaap_close_query_state(state);
	pg_yaap_unregister_state(queryDesc);

	if (queryDesc != NULL && queryDesc->plannedstmt != NULL)
		pg_yaap_unregister_optimizer_plan(queryDesc->plannedstmt);

	if (optimizer_only)
		standard_ExecutorEnd(queryDesc);
	else if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}
