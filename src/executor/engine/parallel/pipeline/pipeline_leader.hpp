#pragma once

extern "C" {
#include "postgres.h"
#include "executor/execdesc.h"
}

namespace pg_yaap {

struct PgYaapQueryState;

namespace pipeline {

bool PgYaapPipelineRun(QueryDesc *queryDesc,
                         PgYaapQueryState *state,
                         const char **failure_reason);

}
}
