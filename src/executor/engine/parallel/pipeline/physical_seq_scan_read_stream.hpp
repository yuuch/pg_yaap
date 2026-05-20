#pragma once

extern "C" {
#include "postgres.h"
#include "access/heapam.h"
#include "storage/read_stream.h"
}

namespace pg_yaap {
namespace pipeline {

struct SeqScanReadStreamState {
	HeapScanDesc scan = nullptr;
	int worker_index = -1;
	bool trace_enabled = false;
	uint64_t chunk_sequence = 0;
};

void InstallAggressiveParallelSeqScanReadStream(HeapScanDesc scan,
                                                Relation rel,
                                                SeqScanReadStreamState &state,
                                                int worker_index,
                                                bool trace_enabled);

}  /* namespace pipeline */
}  /* namespace pg_yaap */
