#ifndef PG_YAAP_EXECUTE_H
#define PG_YAAP_EXECUTE_H

#include "postgres.h"
#include "executor/executor.h"
#include "state.h"

#ifdef __cplusplus
extern "C" {
#endif

bool pg_yaap_initialize_plan(QueryDesc *queryDesc, PgYaapQueryState *state);
void pg_yaap_delete_plan(PgYaapQueryState *state);
bool pg_yaap_execute_query(QueryDesc *queryDesc, PgYaapQueryState *state,
							ScanDirection direction, uint64 count);

#ifdef __cplusplus
}
#endif

#endif
