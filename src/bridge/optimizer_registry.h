#ifndef PG_YAAP_OPTIMIZER_REGISTRY_H
#define PG_YAAP_OPTIMIZER_REGISTRY_H

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"

#ifdef __cplusplus
extern "C" {
#endif

bool pg_yaap_try_build_optimizer_plan(Query *parse, void **out_bundle);
void pg_yaap_register_optimizer_plan(PlannedStmt *plannedstmt, void *bundle);
void *pg_yaap_lookup_optimizer_plan(PlannedStmt *plannedstmt);
void pg_yaap_unregister_optimizer_plan(PlannedStmt *plannedstmt);
void pg_yaap_discard_optimizer_plan(void *bundle);
TupleDesc pg_yaap_build_optimizer_tupdesc(void *bundle);
char *pg_yaap_describe_optimizer_plan(void *bundle);

#ifdef __cplusplus
}
#endif

#endif
