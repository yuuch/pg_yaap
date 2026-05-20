#include "postgres.h"
#include "utils/hsearch.h"
#include "state.h"
#include "execute.h"
#include "optimizer_registry.h"
#include "nodes/plannodes.h"
#include "parser/parsetree.h"
#include "utils/dsa.h"
#include "utils/memutils.h"

struct dsm_segment;

extern bool pg_yaap_trace_hooks;

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

/* Layout MUST match query_state.hpp PgYaapQueryState (C++ mirror). */
struct PgYaapQueryState
{
	MemoryContext context;
	void *parallel_plan;
	void *parallel_scheduler;
	struct dsm_segment *runtime_dsm;
	dsa_area *runtime_dsa;
};

typedef struct StateEntry
{
	QueryDesc  *queryDesc;
	PgYaapQueryState *state;
} StateEntry;

static HTAB *state_table = NULL;

void
pg_yaap_init_state_table(void)
{
	HASHCTL         ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(QueryDesc *);
	ctl.entrysize = sizeof(StateEntry);
	state_table = hash_create("pg_yaap query state table",
							  128,
							  &ctl,
							  HASH_ELEM | HASH_BLOBS);
}

void
pg_yaap_register_state(QueryDesc *queryDesc, PgYaapQueryState *state)
{
	StateEntry *entry;
	bool            found;

	entry = (StateEntry *) hash_search(state_table, &queryDesc, HASH_ENTER, &found);
	entry->state = state;
}

PgYaapQueryState *
pg_yaap_lookup_state(QueryDesc *queryDesc)
{
	StateEntry *entry;

	if (state_table == NULL)
		return NULL;

	entry = (StateEntry *) hash_search(state_table, &queryDesc, HASH_FIND, NULL);
	return entry ? entry->state : NULL;
}

void
pg_yaap_unregister_state(QueryDesc *queryDesc)
{
	if (state_table == NULL)
		return;

	hash_search(state_table, &queryDesc, HASH_REMOVE, NULL);
}

void
pg_yaap_close_query_state(PgYaapQueryState *state)
{
	pg_yaap_delete_plan(state);
	if (state->context)
		MemoryContextDelete(state->context);
	pfree(state);
}

PgYaapQueryState *
pg_yaap_try_build_query_state(QueryDesc *queryDesc, int eflags)
{
	PgYaapQueryState *state;
	Plan *plan = queryDesc->plannedstmt->planTree;
	void *optimizer_bundle = pg_yaap_lookup_optimizer_plan(queryDesc->plannedstmt);
	(void) eflags;

	if (optimizer_bundle != NULL)
	{
		if (!pg_yaap_optimizer_bundle_has_tupdesc(optimizer_bundle))
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: query state admission rejected because optimizer bundle has no output tuple descriptor");
			return NULL;
		}
		state = (PgYaapQueryState *) palloc0(sizeof(PgYaapQueryState));
		state->context = AllocSetContextCreate(CurrentMemoryContext,
											   "pg_yaap query context",
											   ALLOCSET_DEFAULT_SIZES);
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: query state admitted via optimizer bundle (context=%p)",
				 (void *) state->context);
		return state;
	}

	if (pg_yaap_trace_hooks)
		elog(LOG,
			 "pg_yaap: query state admission rejected because query has no optimizer bundle (nodeTag=%d)",
			 plan != NULL ? (int) nodeTag(plan) : -1);
	return NULL;
}
