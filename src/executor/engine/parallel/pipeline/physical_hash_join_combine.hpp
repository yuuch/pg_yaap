#pragma once

#include "parallel/pipeline/physical_hash_join.hpp"

namespace pg_yaap {
namespace pipeline {

SinkCombineResultType ExecuteHashJoinCombine(ExecCtx &ctx,
                                             HashJoinGlobalSinkState &global,
                                             HashJoinLocalSinkState &local);
SinkCombineResultType ExecuteHashJoinSharedPayloadCombine(ExecCtx &ctx,
                                                          HashJoinGlobalSinkState &global,
                                                          HashJoinLocalSinkState &local);
void PublishHashJoinCombinedRows(HashJoinGlobalSinkState &global);

}  /* namespace pipeline */
}  /* namespace pg_yaap */
