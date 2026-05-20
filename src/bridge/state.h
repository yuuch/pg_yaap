#ifndef PG_YAAP_STATE_H
#define PG_YAAP_STATE_H

#include "postgres.h"
#include "executor/executor.h"

/* Opaque pointer for both C and C++ */
typedef struct PgYaapQueryState PgYaapQueryState;

#ifdef __cplusplus
extern "C" {
#endif

void pg_yaap_init_state_table(void);
void pg_yaap_register_state(QueryDesc *queryDesc, PgYaapQueryState *state);
PgYaapQueryState *pg_yaap_lookup_state(QueryDesc *queryDesc);
void pg_yaap_unregister_state(QueryDesc *queryDesc);

PgYaapQueryState *pg_yaap_try_build_query_state(QueryDesc *queryDesc, int eflags);
void pg_yaap_close_query_state(PgYaapQueryState *state);

#ifdef __cplusplus
}
#endif

#endif
