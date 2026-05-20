#pragma once

extern "C" {
#include "postgres.h"
}

#include "parallel/pipeline/query_state.hpp"

namespace pg_yaap {
namespace pipeline {

bool CreateRuntimeDsm(PgYaapQueryState *state, const char **error_out);
void DestroyRuntimeDsm(PgYaapQueryState *state);

}
}
