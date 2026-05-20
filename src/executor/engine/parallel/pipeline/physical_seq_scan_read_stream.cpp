#include "parallel/pipeline/physical_seq_scan_read_stream.hpp"

extern "C" {
#include "access/tableam.h"
#include "port/pg_bitutils.h"
#include "storage/bufmgr.h"
#include "storage/io_worker.h"
#include "utils/elog.h"
}

#include <algorithm>

namespace pg_yaap {
namespace pipeline {

namespace {

static constexpr uint32_t kYaapParallelSeqScanChunkDivisor = 512;
static constexpr uint32_t kYaapParallelSeqScanMaxChunkSize = 16384;
static constexpr uint32_t kYaapParallelSeqScanMinChunkSize = 1024;

static inline BlockNumber
ParallelScanBlockCount(const ParallelBlockTableScanDesc pbscan)
{
	return pbscan->phs_nblocks;
}

static uint32_t
ComputeAggressiveChunkSize(BlockNumber scan_nblocks, uint32_t default_chunk_size)
{
	uint32_t aggressive = pg_nextpower2_32(std::max<uint32_t>(scan_nblocks / kYaapParallelSeqScanChunkDivisor, 1u));
	aggressive = std::max(aggressive, default_chunk_size);
	aggressive = std::max(aggressive, static_cast<uint32_t>(io_combine_limit) * 64u);
	aggressive = std::max(aggressive, kYaapParallelSeqScanMinChunkSize);
	aggressive = std::min(aggressive, kYaapParallelSeqScanMaxChunkSize);
	aggressive = std::min(aggressive, static_cast<uint32_t>(std::max<BlockNumber>(scan_nblocks, 1)));
	return aggressive;
}

static void
LogAssignedChunk(const SeqScanReadStreamState &state,
                 const ParallelBlockTableScanDesc pbscan,
                 const ParallelBlockTableScanWorker pbscanwork,
                 BlockNumber page)
{
	if (!state.trace_enabled || page == InvalidBlockNumber)
		return;

	const BlockNumber scan_nblocks = ParallelScanBlockCount(pbscan);
	const uint64_t logical_offset = pbscanwork->phsw_nallocated;
	const uint32_t chunk_len = std::min<uint64_t>(pbscanwork->phsw_chunk_size,
	                                              scan_nblocks > logical_offset
	                                                ? scan_nblocks - logical_offset
	                                                : 0);
	const bool wrapped = chunk_len > 0 &&
		page + chunk_len - 1 >= pbscan->phs_nblocks;
	const BlockNumber last_block = chunk_len == 0
		? page
		: (page + chunk_len - 1) % pbscan->phs_nblocks;

	elog(LOG,
	     "pg_yaap: seqscan chunk_assign worker=%d rel=%s chunk=%llu logical_offset=%llu first_block=%u last_block=%u nblocks=%u wrapped=%d",
	     state.worker_index,
	     RelationGetRelationName(state.scan->rs_base.rs_rd),
	     (unsigned long long) state.chunk_sequence,
	     (unsigned long long) logical_offset,
	     page,
	     last_block,
	     chunk_len,
	     wrapped ? 1 : 0);
}

static BlockNumber
PgYaapSeqScanReadNextParallel(ReadStream *stream,
                              void *callback_private_data,
                              void *per_buffer_data)
{
	SeqScanReadStreamState *state = static_cast<SeqScanReadStreamState *>(callback_private_data);
	HeapScanDesc scan = state->scan;
	ParallelBlockTableScanDesc pbscan = (ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
	ParallelBlockTableScanWorker pbscanwork = scan->rs_parallelworkerdata;
	const BlockNumber scan_nblocks = ParallelScanBlockCount(pbscan);

	(void) stream;
	(void) per_buffer_data;

	Assert(ScanDirectionIsForward(scan->rs_dir));
	Assert(scan->rs_base.rs_parallel != nullptr);
	Assert(pbscanwork != nullptr);

	if (unlikely(!scan->rs_inited))
	{
		table_block_parallelscan_startblock_init(scan->rs_base.rs_rd,
		                                         pbscanwork,
		                                         pbscan);
		pbscanwork->phsw_chunk_size = ComputeAggressiveChunkSize(scan_nblocks,
		                                                         pbscanwork->phsw_chunk_size);
		scan->rs_inited = true;
		if (state->trace_enabled)
		{
			elog(LOG,
			     "pg_yaap: seqscan read_stream_init worker=%d rel=%s startblock=%u total_blocks=%u chunk_size=%u ring_buffers=%d io_combine_limit=%d effective_io_concurrency=%d io_workers=%d flags=SEQUENTIAL|USE_BATCHING",
			     state->worker_index,
			     RelationGetRelationName(scan->rs_base.rs_rd),
			     pbscan->phs_startblock,
			     scan_nblocks,
			     pbscanwork->phsw_chunk_size,
			     GetAccessStrategyBufferCount(scan->rs_strategy),
			     io_combine_limit,
			     effective_io_concurrency,
			     io_workers);
		}
	}

	const bool starting_new_chunk = (pbscanwork->phsw_chunk_remaining == 0);
	scan->rs_prefetch_block = table_block_parallelscan_nextpage(scan->rs_base.rs_rd,
	                                                            pbscanwork,
	                                                            pbscan);
	if (starting_new_chunk && scan->rs_prefetch_block != InvalidBlockNumber)
	{
		++state->chunk_sequence;
		LogAssignedChunk(*state, pbscan, pbscanwork, scan->rs_prefetch_block);
	}

	return scan->rs_prefetch_block;
}

}  // namespace

void
InstallAggressiveParallelSeqScanReadStream(HeapScanDesc scan,
                                           Relation rel,
                                           SeqScanReadStreamState &state,
                                           int worker_index,
                                           bool trace_enabled)
{
	Assert(scan != nullptr);
	Assert(scan->rs_base.rs_parallel != nullptr);

	if (scan->rs_read_stream != nullptr)
	{
		read_stream_end(scan->rs_read_stream);
		scan->rs_read_stream = nullptr;
	}

	scan->rs_inited = false;
	scan->rs_prefetch_block = InvalidBlockNumber;

	state.scan = scan;
	state.worker_index = worker_index;
	state.trace_enabled = trace_enabled;
	state.chunk_sequence = 0;

	scan->rs_read_stream = read_stream_begin_relation(READ_STREAM_SEQUENTIAL |
	                                                  READ_STREAM_USE_BATCHING,
	                                                  scan->rs_strategy,
	                                                  rel,
	                                                  MAIN_FORKNUM,
	                                                  PgYaapSeqScanReadNextParallel,
	                                                  &state,
	                                                  0);
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
