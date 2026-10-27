#include "parallel/pipeline/physical_seq_scan_read_stream.hpp"
#include "parallel/pipeline/scan_io_metrics.hpp"

extern "C" {
#include "access/tableam.h"
#include "miscadmin.h"
#include "access/syncscan.h"
#include "executor/instrument.h"
#include "portability/instr_time.h"
#include "utils/memutils.h"
#include "port/pg_bitutils.h"
#include "storage/bufmgr.h"
#include "storage/io_worker.h"
#include "storage/aio.h"
#include "utils/elog.h"
#include "utils/rel.h"
}

#include <algorithm>
#include <new>
#include <time.h>

namespace pg_yaap {
namespace pipeline {

// Backend-private, bounded diagnostic storage. Never shared through DSM.
struct ScanBlockEvent {
    const char *kind;
    uint64_t time_ns, end_ns, request;
    BlockNumber block;
    uint32_t pages, requested;
    int need_wait;
    Buffer buffer;
    bool kernel_available = false, kernel_mixed_executors = false;
    int32_t kernel_executor_pid = 0;
    uint64_t kernel_requests = 0, kernel_queue_ns = 0, kernel_read_ns = 0;
    uint64_t kernel_completed_bytes = 0, kernel_worker_requests = 0;
};
struct ScanBlockTrace {
    ScanBlockEvent *events;
    uint32_t capacity, count;
    uint64_t dropped, stream;
    SeqScanReadStreamState *state;
};
static uint64_t BlockTraceClock() {
    instr_time now;
    INSTR_TIME_SET_CURRENT(now);
    return INSTR_TIME_GET_NANOSEC(now);
}
static void BlockTraceRecord(ScanBlockTrace *t, const char *kind, uint64_t begin,
                            uint64_t end, uint64_t request, BlockNumber block,
                            uint32_t pages, uint32_t requested, int need_wait, Buffer buffer) {
    if (!t) return;
    if (t->count == t->capacity) { ++t->dropped; return; }
    t->events[t->count++] = {kind, begin, end, request, block, pages, requested, need_wait, buffer};
}
static void BlockTraceDump(ScanBlockTrace *t) {
    if (!t) return;
    const auto &state = *t->state;
    const Oid relid = RelationGetRelid(state.scan->rs_base.rs_rd);
    for (uint32_t i=0; i<t->count; ++i) {
        const auto &e=t->events[i];
        const BlockNumber last = e.pages && state.scan->rs_nblocks
            ? static_cast<BlockNumber>((static_cast<uint64_t>(e.block)+e.pages-1)%state.scan->rs_nblocks)
            : e.block;
        elog(LOG, "pg_yaap[scan-block] pid=%d worker=%d rel=%u stream=%llu "
             "event=%s seq=%u time_ns=%llu end_ns=%llu request=%llu block=%u last_block=%u "
             "pages=%u requested=%u need_wait=%d buffer=%d "
             "kernel_available=%d kernel_executor_pid=%d kernel_mixed_executors=%d "
             "kernel_requests=%llu kernel_worker_requests=%llu kernel_completed_bytes=%llu "
             "kernel_queue_ns=%llu kernel_read_ns=%llu",
             MyProcPid, state.worker_index, relid, (unsigned long long)t->stream,
             e.kind, i, (unsigned long long)e.time_ns, (unsigned long long)e.end_ns,
             (unsigned long long)e.request, e.block, last, e.pages, e.requested, e.need_wait, e.buffer,
             e.kernel_available, e.kernel_executor_pid, e.kernel_mixed_executors,
             (unsigned long long)e.kernel_requests, (unsigned long long)e.kernel_worker_requests,
             (unsigned long long)e.kernel_completed_bytes, (unsigned long long)e.kernel_queue_ns,
             (unsigned long long)e.kernel_read_ns);
    }
    elog(LOG, "pg_yaap[scan-block-summary] pid=%d worker=%d rel=%u stream=%llu "
         "recorded=%u dropped=%llu capacity=%u complete=yes",
         MyProcPid, state.worker_index, relid, (unsigned long long)t->stream,
         t->count, (unsigned long long)t->dropped, t->capacity);
}

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
ComputeAggressiveChunkSize(BlockNumber scan_nblocks, uint32_t default_chunk_size, uint32_t workers)
{
	uint32_t aggressive = pg_nextpower2_32(std::max<uint32_t>(scan_nblocks / kYaapParallelSeqScanChunkDivisor, 1u));
	aggressive = std::max(aggressive, default_chunk_size);
	aggressive = std::max(aggressive, static_cast<uint32_t>(io_combine_limit) * 64u);
	aggressive = std::max(aggressive, kYaapParallelSeqScanMinChunkSize);
	aggressive = std::min(aggressive, kYaapParallelSeqScanMaxChunkSize);
	/* The allocation chunk is worker-owned work, not the ReadStream I/O
	 * quota. Keep at least four chunks per worker on smaller relations so
	 * early scanners cannot reserve the entire probe side (Q13 customer).
	 * Retain combine-sized sequential runs and PG's tail ramp-down. */
	const uint32_t balanced = pg_prevpower2_32(static_cast<uint32_t>(
		std::max<uint64_t>(1, scan_nblocks / (4ull * std::max(1u, workers)))));
	const uint32_t balance_limit = std::max(balanced, static_cast<uint32_t>(io_combine_limit));
	/* Tiny relations cannot provide four combine-sized runs per worker.
	 * Keep their previous sequential allocation rather than multiplying
	 * downstream per-producer partition tails for a few hundred pages. */
	if (scan_nblocks / (4ull * std::max(1u, workers)) >= static_cast<uint32_t>(io_combine_limit))
		aggressive = std::min(aggressive, balance_limit);
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
		                                                         pbscanwork->phsw_chunk_size, state->worker_count);
		scan->rs_inited = true;
        state->initial_chunk_pages = pbscanwork->phsw_chunk_size;
		if (state->trace_enabled)
		{
			elog(LOG,
			     "pg_yaap: seqscan read_stream_init worker=%d rel=%s startblock=%u total_blocks=%u chunk_size=%u ring_buffers=%d io_combine_limit=%d effective_io_concurrency=%d io_workers=%d mode=%s flags=%s",
			     state->worker_index,
			     RelationGetRelationName(scan->rs_base.rs_rd),
			     pbscan->phs_startblock,
			     scan_nblocks,
			     pbscanwork->phsw_chunk_size,
			     state->yaap_queue ? static_cast<int>(state->actual_window_pages) : GetAccessStrategyBufferCount(scan->rs_strategy),
			     io_combine_limit,
			     effective_io_concurrency,
			     io_workers,
			     state->yaap_queue ? "yaap" : "pg",
			     state->yaap_queue ? "BOUNDED_BATCH_QUEUE" : "SEQUENTIAL|USE_BATCHING");
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
        if (state->block_trace) {
            const uint32_t pages = std::min<uint64_t>(pbscanwork->phsw_chunk_size,
                scan_nblocks - pbscanwork->phsw_nallocated);
            const uint64_t now = BlockTraceClock();
            BlockTraceRecord(state->block_trace, "ASSIGN", now, now, 0,
                             scan->rs_prefetch_block, pages, pages, -1, InvalidBuffer);
        }
	}

	return scan->rs_prefetch_block;
}

}  // namespace

void
InstallAggressiveParallelSeqScanReadStream(HeapScanDesc scan,
                                           Relation rel,
                                           SeqScanReadStreamState &state,
                                           int worker_index,
                                           uint32_t worker_count,
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
	state.worker_count = std::max(1u, worker_count);
	state.trace_enabled = trace_enabled;
	state.chunk_sequence = 0;
	state.yaap_queue = false;
	state.actual_window_pages = 0;

	scan->rs_read_stream = read_stream_begin_relation(
		READ_STREAM_SEQUENTIAL | READ_STREAM_USE_BATCHING, scan->rs_strategy, rel, MAIN_FORKNUM,
		PgYaapSeqScanReadNextParallel, &state, 0);
}

/* Each slot owns stable storage for both request descriptors and forwarded
 * pins. A split read is continued in-place, never moved across queue wrap. */
struct BlockTraceRequest { uint64_t id; uint32_t pages; };
struct YaapReadBatch {
    BlockTraceRequest *trace_requests;
	Buffer *buffers;
	ReadBuffersOperation *ops;
	bool *pending;
	int count, consumed, submitted;
	BlockNumber first_block;
	uint16_t *owner_request_index;
	uint64_t *request_submit_return_ns;
	bool *request_initial_need_wait;
};
struct YaapReadStream {
	HeapScanDesc scan;
	SeqScanReadStreamState *state;
	BufferAccessStrategy strategy;
	YaapReadBatch *slots;
	uint32_t slot_count, head, used, pinned;
	BlockNumber saved_block;
	bool eof, profile;
	uint64_t sample_rng, inflight;
	YaapReadStreamStats stats;
	ScanIOStats *io_stats;
	uint64_t submitted_ahead;
    uint64_t trace_request_sequence;
    uint64_t last_buffer_return_ns;
};
static uint64_t QueueClock(bool enabled) {
	if (!enabled)
		return 0;
	instr_time now;
	INSTR_TIME_SET_CURRENT(now);
	return INSTR_TIME_GET_NANOSEC(now);
}
/* Sampling decisions advance independently for submit and wait API calls.
 * Thread CPU clocks measure on-CPU cost, not physical I/O service time. */
static bool QueueSample(YaapReadStream *s) {
	if (!s->profile && !(s->io_stats && s->io_stats->detail))
		return false;
	uint64_t x = s->sample_rng;
	x ^= x >> 12;
	x ^= x << 25;
	x ^= x >> 27;
	s->sample_rng = x;
	return ((x * UINT64_C(2685821657736338717)) >> 58) == 0;
}
static bool QueueCpuClock(YaapReadStream *s, uint64_t &ns) {
#ifdef CLOCK_THREAD_CPUTIME_ID
	struct timespec ts;
	if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0) {
		ns = static_cast<uint64_t>(ts.tv_sec) * UINT64_C(1000000000) + ts.tv_nsec;
		return true;
	}
#endif
	++s->stats.cpu_clock_failures;
	if (s->io_stats) ++s->io_stats->cpu_clock_failures;
	return false;
}
static void QueueRecordSample(YaapReadStream *s, bool submit, uint64_t wall, bool cpu_ok,
							  uint64_t cpu_start, uint64_t cpu_end) {
	if (!cpu_ok || cpu_end < cpu_start)
		return;
	/* Nested clock brackets should enclose CPU time. Clamp tiny cross-clock
	 * quantization differences instead of exporting negative off-CPU time. */
	uint64_t cpu = std::min(wall, cpu_end - cpu_start);
    if (s->io_stats) {
        auto &io = *s->io_stats;
        if (submit) {
            ++io.submit_cpu_samples; io.submit_sample_wall_ns += wall;
            io.submit_sample_cpu_ns += cpu;
        } else {
            ++io.wait_cpu_samples; io.wait_sample_wall_ns += wall;
            io.wait_sample_cpu_ns += cpu;
        }
    }
	if (submit) {
		++s->stats.submit_samples;
		s->stats.submit_sample_wall_ns += wall;
		s->stats.submit_sample_cpu_ns += cpu;
	} else {
		++s->stats.wait_samples;
		s->stats.wait_sample_wall_ns += wall;
		s->stats.wait_sample_cpu_ns += cpu;
	}
}
static BlockNumber QueueNextBlock(YaapReadStream *s) {
	if (s->saved_block != InvalidBlockNumber) {
		BlockNumber b = s->saved_block;
		s->saved_block = InvalidBlockNumber;
		return b;
	}
	if (s->eof)
		return InvalidBlockNumber;
	if (s->scan->rs_base.rs_parallel)
		return PgYaapSeqScanReadNextParallel(nullptr, s->state, nullptr);
	HeapScanDesc scan = s->scan;
	if (!scan->rs_inited) {
		scan->rs_inited = true;
		if (!scan->rs_nblocks || scan->rs_numblocks == 0)
			return InvalidBlockNumber;
		return scan->rs_prefetch_block = scan->rs_startblock;
	}
	BlockNumber b = (scan->rs_prefetch_block + 1) % scan->rs_nblocks;
	if (scan->rs_base.rs_flags & SO_ALLOW_SYNC)
		ss_report_location(scan->rs_base.rs_rd, b);
	if (b == scan->rs_startblock ||
		(scan->rs_numblocks != InvalidBlockNumber && --scan->rs_numblocks == 0))
		return InvalidBlockNumber;
	return scan->rs_prefetch_block = b;
}
static void QueueRefill(YaapReadStream *s) {
	++s->stats.refill_calls;
	const uint64_t begin = QueueClock(s->profile);
	const uint64_t submit_before = s->stats.submit_ns;
	uint32_t new_io = 0;
	while (true) {
		if (s->stats.max_requests && s->inflight >= s->stats.max_requests) {
			++s->stats.request_limit_stops;
            if (s->io_stats) ++s->io_stats->request_binding_events;
			break;
		}
		if (s->stats.max_requests && new_io >= 2) {
			++s->stats.new_io_burst_stops;
            if (s->io_stats) ++s->io_stats->burst_binding_events;
			break;
		}
		YaapReadBatch *tail =
			s->used ? &s->slots[(s->head + s->used - 1) % s->slot_count] : nullptr;
		if (!tail || tail->submitted == tail->count) {
            if (s->eof || s->used == s->slot_count) {
                if (s->io_stats && !s->eof) ++s->io_stats->slot_binding_events;
                break;
            }
			uint32_t allowance =
				std::min<uint32_t>(s->stats.batch_pages, s->stats.window_pages - s->pinned);
			const uint64_t pin_begin = QueueClock(s->io_stats && s->io_stats->detail);
			const uint32_t additional =
				s->scan->rs_base.rs_rd->rd_rel->relpersistence == RELPERSISTENCE_TEMP
					? GetAdditionalLocalPinLimit()
					: GetAdditionalPinLimit();
			if (s->io_stats && s->io_stats->detail)
                s->io_stats->pin_budget_check_ns += QueueClock(true) - pin_begin;
			if (additional == 0)
				++s->stats.pin_limit_stops;
            if (s->io_stats && allowance > 0 && additional < allowance)
                ++s->io_stats->pin_binding_events;
            allowance = std::min(allowance, additional);
			if (!allowance) {
                if (s->used) {
                    break;
                }
				/* PG's additional-pin allowance is conservative. As in ReadStream,
				 * acquire one page when no queued pin can make progress. Never turn a
				 * pin-budget shortage into a successful, truncated scan. */
				allowance = 1;
			}
			const uint64_t assign_begin = QueueClock(s->io_stats && s->io_stats->detail);
			BlockNumber first = QueueNextBlock(s);
			if (first == InvalidBlockNumber) {
                if (s->io_stats && s->io_stats->detail)
                    s->io_stats->block_assignment_ns += QueueClock(true) - assign_begin;
				s->eof = true;
				break;
			}
			YaapReadBatch &b = s->slots[(s->head + s->used) % s->slot_count];
			b.first_block = first;
			b.submitted = 0;
			b.count = 1;
			b.consumed = 0;
			while (b.count < static_cast<int>(allowance)) {
				BlockNumber next = QueueNextBlock(s);
				if (next == InvalidBlockNumber) {
					s->eof = true;
					break;
				}
				if (next != first + b.count) {
					s->saved_block = next;
					break;
				}
				++b.count;
			}
			if (s->io_stats && s->io_stats->detail)
                s->io_stats->block_assignment_ns += QueueClock(true) - assign_begin;
            /* Publish ownership before calls which can ERROR. Request storage lives
			 * until transaction cleanup even if the query's C++ destructors are skipped. */
			++s->used;
			s->pinned += b.count;
			s->stats.peak_pins = std::max<uint64_t>(s->stats.peak_pins, s->pinned);
			if (s->io_stats) s->io_stats->peak_reserved = std::max<uint64_t>(s->io_stats->peak_reserved, s->pinned);
			tail = &b;
		}
		YaapReadBatch &b = *tail;
		while (b.submitted < b.count) {
			if (s->stats.max_requests && s->inflight >= s->stats.max_requests) {
				++s->stats.request_limit_stops;
            if (s->io_stats) ++s->io_stats->request_binding_events;
				goto refill_done;
			}
			if (s->stats.max_requests && new_io >= 2) {
				++s->stats.new_io_burst_stops;
            if (s->io_stats) ++s->io_stats->burst_binding_events;
				goto refill_done;
			}
			const int offset = b.submitted;
			ReadBuffersOperation &op = b.ops[offset];
			op.rel = s->scan->rs_base.rs_rd;
			op.smgr = RelationGetSmgr(op.rel);
			op.persistence = op.rel->rd_rel->relpersistence;
			op.forknum = MAIN_FORKNUM;
			op.strategy = s->strategy;
			const int requested = b.count - offset;
			int accepted = requested;
			s->stats.requested_pages += accepted;
			const bool sample = QueueSample(s);
			uint64_t cpu_start = 0, cpu_end = 0;
			uint64_t start = QueueClock(s->profile || s->io_stats || s->state->block_trace);
			bool cpu_ok = sample && QueueCpuClock(s, cpu_start);
            const bool pg_timed = s->io_stats && track_io_timing &&
                op.persistence != RELPERSISTENCE_TEMP;
            instr_time pg_read_before;
            if (pg_timed) pg_read_before = pgBufferUsage.shared_blk_read_time;
            int read_flags = READ_BUFFERS_ISSUE_ADVICE;
#ifdef YAAP_PG_AIO_TIMING_VERSION
            if (s->io_stats && s->io_stats->detail)
                read_flags |= READ_BUFFERS_PROFILE_TIMING;
#endif
#ifdef YAAP_PG_BUFFER_PROFILE_VERSION
            if (s->io_stats && s->io_stats->detail && op.persistence != RELPERSISTENCE_TEMP)
                read_flags |= READ_BUFFERS_PROFILE_BUFFER;
#endif
			b.pending[offset] = StartReadBuffers(&op, b.buffers + offset, b.first_block + offset,
                                                 &accepted, read_flags);
#ifdef YAAP_PG_BUFFER_PROFILE_VERSION
            if (s->io_stats && s->io_stats->detail) {
                const PgReadBufferProfile *buffer = ReadBuffersGetBufferProfile(&op);
                if (buffer && buffer->enabled) {
                    auto &io = *s->io_stats;
#define COPY_BUFFER(n) io.buffer_##n += buffer->n
                    COPY_BUFFER(alloc_calls);
                    COPY_BUFFER(lookup_hits);
                    COPY_BUFFER(lookup_misses);
                    COPY_BUFFER(ring_attempts);
                    COPY_BUFFER(ring_hits);
                    COPY_BUFFER(freelist_candidates);
                    COPY_BUFFER(freelist_hits);
                    COPY_BUFFER(clock_candidates);
                    COPY_BUFFER(clock_usage_decrements);
                    COPY_BUFFER(clock_pinned_skips);
                    COPY_BUFFER(victim_retries);
                    COPY_BUFFER(dirty_writebacks);
                    COPY_BUFFER(profiled_start_calls);
                    COPY_BUFFER(sampled_start_calls);
                    COPY_BUFFER(sampled_pages);
                    COPY_BUFFER(alloc_total_ns);
                    COPY_BUFFER(lookup_ns);
                    COPY_BUFFER(mapping_lock_ns);
                    COPY_BUFFER(victim_ns);
                    COPY_BUFFER(strategy_ns);
                    COPY_BUFFER(ring_ns);
                    COPY_BUFFER(freelist_ns);
                    COPY_BUFFER(clock_ns);
                    COPY_BUFFER(dirty_write_ns);
                    COPY_BUFFER(invalidate_ns);
                    COPY_BUFFER(pin_ns);
#undef COPY_BUFFER
                }
            }
#endif
            if (pg_timed) {
                instr_time delta = pgBufferUsage.shared_blk_read_time;
                INSTR_TIME_SUBTRACT(delta, pg_read_before);
                ++s->io_stats->pg_start_timed_calls;
                s->io_stats->pg_start_read_ns += INSTR_TIME_GET_NANOSEC(delta);
            }
			if (sample)
				cpu_ok = QueueCpuClock(s, cpu_end) && cpu_ok;
			const uint64_t api_end = QueueClock(s->profile || s->io_stats || s->state->block_trace);
			const uint64_t elapsed = api_end - start;
			s->stats.submit_ns += s->profile ? elapsed : 0;
			if (sample)
				QueueRecordSample(s, true, elapsed, cpu_ok, cpu_start, cpu_end);
			if (accepted <= 0 || accepted > requested)
				ereport(
					ERROR,
					(errmsg("pg_yaap read stream accepted invalid block count %d (requested %d)",
							accepted, requested)));
            if (s->io_stats) {
                auto &io = *s->io_stats;
                ++io.api_start_calls; io.api_accepted_pages += accepted;
                if(b.pending[offset]) ++io.api_need_wait_calls;
                if (b.pending[offset]) {
                    io.start_need_wait_pages += accepted;
                    if (io.io_method == IOMETHOD_SYNC) ++io.configured_sync_need_wait_calls;
                } else io.start_ready_pages += accepted;
                if (io.detail) {
                    io.start_api_ns.Observe(elapsed);
                    io.accepted_pages_per_call.Observe(accepted);
                    if (b.pending[offset]) io.need_wait_accepted_pages.Observe(accepted);
                }
                b.request_submit_return_ns[offset] = api_end;
                b.request_initial_need_wait[offset] = b.pending[offset];
                for (int j=offset;j<offset+accepted;++j) b.owner_request_index[j]=offset;
                s->submitted_ahead += accepted;
                io.peak_submitted_ahead = std::max(io.peak_submitted_ahead,s->submitted_ahead);
            }
            if (s->state->block_trace) {
                auto &r = b.trace_requests[offset];
                r.id = ++s->trace_request_sequence; r.pages = accepted;
                BlockTraceRecord(s->state->block_trace, "START", start, api_end, r.id,
                                 b.first_block+offset, accepted, requested, b.pending[offset],
                                 b.buffers[offset]);
                for (int j=offset; j<offset+accepted; ++j) b.owner_request_index[j]=offset;
            }
			++s->stats.requests;
			if (accepted < requested)
				++s->stats.split_requests;
			if (accepted == 1)
				++s->stats.accepted_single;
			else
				++s->stats.accepted_multi;
			s->stats.accepted_pages += accepted;
			if (b.pending[offset]) {
				++s->stats.need_wait;
				++s->inflight;
				++new_io;
				s->stats.peak_inflight = std::max(s->stats.peak_inflight, s->inflight);
				if(s->io_stats) s->io_stats->peak_unwaited=std::max(s->io_stats->peak_unwaited,s->inflight);
			} else
				s->stats.ready_pages += accepted;
			/* Pending bits only at request starts; other entries were zeroed. */
			b.submitted += accepted;
		}
	}
refill_done:
    // Co-binding capacity observation, not an exclusive loss-time cause.
    // Slot/request checks may stop first while the page window is also full.
    if(s->io_stats && !s->eof && s->pinned==s->stats.window_pages)
        ++s->io_stats->window_full_events;
	if (!s->eof && s->used == s->slot_count && s->pinned < s->stats.window_pages)
		++s->stats.slot_limit_stops;
	s->stats.refill_ns += QueueClock(s->profile) - begin - (s->stats.submit_ns - submit_before);
}
YaapReadStream *YaapReadStreamBegin(HeapScanDesc scan, SeqScanReadStreamState &state,
									int worker_index, uint32_t worker_count, bool trace_enabled,
									uint32_t window_pages, bool profile_enabled,
									uint32_t max_requests, uint32_t batch_pages) {
	if (scan->rs_read_stream) {
		read_stream_end(scan->rs_read_stream);
		scan->rs_read_stream = nullptr;
	}
	/* ResourceOwner owns in-flight requests. Keep their addresses valid through
	 * ERROR unwinding even when the query MemoryContext is reset first. */
	MemoryContext old = MemoryContextSwitchTo(TopTransactionContext);
	auto *s = static_cast<YaapReadStream *>(palloc0(sizeof(YaapReadStream)));
	new (s) YaapReadStream{};
	s->scan = scan;
	s->state = &state;
	s->profile = profile_enabled;
	s->stats.max_requests = max_requests;
	s->sample_rng = UINT64_C(0x9e3779b97f4a7c15) ^
					(static_cast<uint64_t>(worker_index + 2) * UINT64_C(0xd1b54a32d192ed03));
	s->saved_block = InvalidBlockNumber;
	state.scan = scan;
	state.worker_index = worker_index;
	state.worker_count = std::max(1u, worker_count);
	state.trace_enabled = trace_enabled;
	state.chunk_sequence = 0;
	scan->rs_inited = false;
	scan->rs_prefetch_block = InvalidBlockNumber;
	uint32_t limit = std::max<uint32_t>(1, NBuffers / (4ull * state.worker_count));
	limit = std::min(limit, std::max(1u, window_pages));
	limit = std::min(limit,
					 std::max(1u, scan->rs_base.rs_rd->rd_rel->relpersistence == RELPERSISTENCE_TEMP
									  ? GetLocalPinLimit()
									  : GetPinLimit()));
	s->strategy =
		GetAccessStrategyWithSize(BAS_BULKREAD, std::max<uint32_t>(1, limit * (BLCKSZ / 1024)));
	limit =
		std::min(limit, static_cast<uint32_t>(std::max(1, GetAccessStrategyPinLimit(s->strategy))));
	s->stats.window_pages = limit;
	state.yaap_queue = true;
	state.actual_window_pages = limit;
	// Leave room for the next full batch while consuming the validated head.
    // Genuine tiny pin/window budgets still get a one-page progress batch.
    const uint32_t batch_window_limit = std::max(1u, limit / 2);
    s->stats.batch_pages = std::min<uint32_t>(std::max(1, io_combine_limit), batch_window_limit);
	if (batch_pages > 0)
		s->stats.batch_pages = std::min(s->stats.batch_pages, batch_pages);
	s->slot_count = (limit + s->stats.batch_pages - 1) / s->stats.batch_pages + 1;
	s->slots = static_cast<YaapReadBatch *>(palloc0(sizeof(YaapReadBatch) * s->slot_count));
	/* Preallocate all request storage: no allocations in Next. Short runs
	 * may fill the request queue before the page window, which is intentional. */
	for (uint32_t i = 0; i < s->slot_count; ++i) {
		auto &b = s->slots[i];
		b.buffers = static_cast<Buffer *>(palloc0(sizeof(Buffer) * s->stats.batch_pages));
		b.ops = static_cast<ReadBuffersOperation *>(
			palloc0(sizeof(ReadBuffersOperation) * s->stats.batch_pages));
		b.pending = static_cast<bool *>(palloc0(sizeof(bool) * s->stats.batch_pages));
	}
	MemoryContextSwitchTo(old);
	return s;
}
static void QueueWait(YaapReadStream *s, YaapReadBatch &b, int index) {
	if (!b.pending[index])
		return;
	++s->stats.wait_calls;
	const bool sample = QueueSample(s);
	uint64_t cpu_start = 0, cpu_end = 0;
	uint64_t start = QueueClock(s->profile || s->io_stats || s->state->block_trace);
	bool cpu_ok = sample && QueueCpuClock(s, cpu_start);
    const bool pg_timed = s->io_stats && track_io_timing &&
        b.ops[index].persistence != RELPERSISTENCE_TEMP;
    instr_time pg_read_before;
    if (pg_timed) pg_read_before = pgBufferUsage.shared_blk_read_time;
    const int64 pg_reads_before = s->io_stats ? pgBufferUsage.shared_blks_read : 0;
	WaitReadBuffers(&b.ops[index]);
#ifdef YAAP_PG_AIO_TIMING_VERSION
    if (s->io_stats && s->io_stats->detail) {
        const PgAioReadTiming *timing = ReadBuffersGetTiming(&b.ops[index]);
        if (timing && timing->enabled) {
            auto &io = *s->io_stats;
            ++io.kernel_profiled_operations;
#define COPY_KERNEL(n) io.kernel_##n += timing->n
            COPY_KERNEL(requests);
            COPY_KERNEL(requested_bytes);
            COPY_KERNEL(completed_bytes);
            COPY_KERNEL(queued_requests);
            COPY_KERNEL(worker_requests);
            COPY_KERNEL(queue_full_sync_requests);
            COPY_KERNEL(forced_sync_requests);
            COPY_KERNEL(queue_ns);
            COPY_KERNEL(reopen_ns);
            COPY_KERNEL(read_ns);
            COPY_KERNEL(shared_complete_ns);
            COPY_KERNEL(enqueue_to_shared_ns);
            COPY_KERNEL(timed_read_requests);
            COPY_KERNEL(failed_requests);
            COPY_KERNEL(unclassified_requests);
#undef COPY_KERNEL
            if (timing->requests) {
                // One observation per logical operation; retries contribute a sum.
                io.kernel_operation_queue_ns.Observe(timing->queue_ns);
                io.kernel_operation_read_ns.Observe(timing->read_ns);
            }
        }
    }
#endif
    if (s->io_stats) {
        s->io_stats->pg_wait_new_read_pages += pgBufferUsage.shared_blks_read - pg_reads_before;
        if (pg_timed) {
            instr_time delta = pgBufferUsage.shared_blk_read_time;
            INSTR_TIME_SUBTRACT(delta, pg_read_before);
            ++s->io_stats->pg_wait_timed_calls;
            s->io_stats->pg_wait_read_ns += INSTR_TIME_GET_NANOSEC(delta);
        }
    }
	if (sample)
		cpu_ok = QueueCpuClock(s, cpu_end) && cpu_ok;
    const uint64_t api_end = QueueClock(s->profile || s->io_stats || s->state->block_trace);
    const uint64_t elapsed = api_end - start;
    if (s->io_stats) {
        auto &io=*s->io_stats;
        ++io.wait_validation_calls; io.wait_validation_ns+=elapsed;
        ++io.validation_observations;
        io.validated_ready_upper_sum_ns += api_end-b.request_submit_return_ns[index];
        if(io.detail) {
            io.wait_api_ns.Observe(elapsed);
            io.validated_ready_upper_ns.Observe(api_end-b.request_submit_return_ns[index]);
        }
    }
    if (s->state->block_trace) {
        const auto &r=b.trace_requests[index];
        auto *trace = s->state->block_trace;
        const uint32_t before = trace->count;
        BlockTraceRecord(s->state->block_trace, "WAIT", start, api_end, r.id,
                         b.first_block+index, r.pages, r.pages, 1, b.buffers[index]);
#ifdef YAAP_PG_AIO_TIMING_VERSION
        const auto *timing = ReadBuffersGetTiming(&b.ops[index]);
        if (trace->count > before && timing && timing->enabled) {
            auto &event = trace->events[before];
            event.kernel_available = timing->requests &&
                timing->timed_read_requests == timing->requests && !timing->failed_requests &&
                !timing->unclassified_requests;
            event.kernel_executor_pid = timing->executor_pid;
            event.kernel_mixed_executors = timing->mixed_executors;
            event.kernel_requests = timing->requests;
            event.kernel_worker_requests = timing->worker_requests;
            event.kernel_completed_bytes = timing->completed_bytes;
            event.kernel_queue_ns = timing->queue_ns;
            event.kernel_read_ns = timing->read_ns;
        }
#else
        (void) before;
#endif
    }
	s->stats.wait_ns += s->profile ? elapsed : 0;
	if (sample)
		QueueRecordSample(s, false, elapsed, cpu_ok, cpu_start, cpu_end);
	b.pending[index] = false;
	--s->inflight;
}
static bool QueueWaitOldest(YaapReadStream *s) {
	for (uint32_t n = 0; n < s->used; ++n) {
		auto &b = s->slots[(s->head + n) % s->slot_count];
		for (int i = b.consumed; i < b.submitted; ++i)
			if (b.pending[i]) {
				QueueWait(s, b, i);
				return true;
			}
	}
	return false;
}
Buffer YaapReadStreamNext(YaapReadStream *s) {
    const uint64_t need = QueueClock(s->io_stats || s->state->block_trace);
    if (s->io_stats && s->io_stats->detail && s->last_buffer_return_ns) {
        // Caller elapsed between pages, including downstream work/descheduling.
        const uint64_t gap = need - s->last_buffer_return_ns;
        ++s->io_stats->page_consumer_gap_count;
        s->io_stats->page_consumer_gap_ns += gap;
        s->io_stats->page_consumer_gap_distribution_ns.Observe(gap);
        s->last_buffer_return_ns = 0;
    }
    const uint64_t ahead_at_need=s->submitted_ahead, reserved_at_need=s->pinned,
                   unwaited_at_need=s->inflight;
    bool submitted_at_need=false, known_ready=false;
    int trace_need_wait = -1;
    if((s->io_stats || s->state->block_trace) && s->used) {
        const auto &head=s->slots[s->head];
        submitted_at_need=head.consumed<head.submitted;
        if(submitted_at_need)
            known_ready=!head.pending[head.owner_request_index[head.consumed]];
        if (submitted_at_need) trace_need_wait = known_ready ? 0 : 1;
    }
    ++s->stats.calls;
	if (s->pinned <= s->stats.window_pages / 2 &&
		(!s->stats.max_requests || s->inflight < s->stats.max_requests))
		QueueRefill(s);
    if (!s->used) {
        if(s->io_stats) { ++s->io_stats->next_calls; s->io_stats->next_total_ns+=QueueClock(true)-need; }
        return InvalidBuffer;
    }
	YaapReadBatch &b = s->slots[s->head];
	int index = b.consumed;
	while (index >= b.submitted) {
		/* An unsubmitted suffix may contain forwarded pins, but none may
		 * be returned until PG accepts and completes its own operation. */
		const int before = b.submitted;
		QueueRefill(s);
		if (b.submitted > before)
			continue;
		if (!QueueWaitOldest(s))
			ereport(ERROR, (errmsg("pg_yaap read queue cannot submit its next page")));
	}
    if (s->state->block_trace) {
        const auto &r=b.trace_requests[b.owner_request_index[index]];
        BlockTraceRecord(s->state->block_trace, "NEED", need, need, r.id,
                         b.first_block+index, 1, 1, trace_need_wait, b.buffers[index]);
    }
    QueueWait(s, b, index);
    if(s->io_stats) {
        auto &io=*s->io_stats;
        ++io.consumed_pages;
        io.allocation_chunk_pages = s->state->initial_chunk_pages;
        if(known_ready) ++io.known_ready_at_need_pages;
        else ++io.readiness_unknown_pages;
        if(!submitted_at_need) ++io.request_not_submitted_at_need_pages;
        const int owner=b.owner_request_index[index];
        if(b.request_initial_need_wait[owner]) {
            ++io.pending_cohort_consumed_pages;
            if(known_ready) ++io.known_ready_pending_cohort_pages;
        }
        const uint64_t submit=b.request_submit_return_ns[owner];
        if(submit<=need) {
            ++io.api_lead_pages;
            io.api_lead_sum_ns += need-submit;
            if(io.detail) io.api_return_to_need_ns.Observe(need-submit);
        } else {
            ++io.submit_after_need_pages;
            io.need_to_submit_return_sum_ns += submit-need;
            if(io.detail) io.need_to_submit_return_ns.Observe(submit-need);
        }
        if(io.detail) {
            io.submitted_ahead_at_need.Observe(ahead_at_need);
            io.reserved_at_need.Observe(reserved_at_need);
            io.unwaited_at_need.Observe(unwaited_at_need);
        }
        --s->submitted_ahead;
    }
	Buffer result = b.buffers[index];
	b.buffers[index] = InvalidBuffer;
	--s->pinned;
	if (++b.consumed == b.count) {
		b.count = b.consumed = b.submitted = 0;
		s->head = (s->head + 1) % s->slot_count;
		--s->used;
	}
    if (s->io_stats) {
        const uint64_t end = QueueClock(true);
        ++s->io_stats->next_calls;
        s->io_stats->next_total_ns += end-need;
        if (s->io_stats->detail) s->last_buffer_return_ns = end;
    }
	return result; /* Transfer exactly one pin to the scan. */
}
void YaapReadStreamEnd(YaapReadStream *s) {
	if (!s)
		return;
	for (uint32_t i = 0; i < s->slot_count; ++i) {
		auto &b = s->slots[i];
		for (int j = 0; j < b.submitted; ++j) {
			if (b.pending[j]) {
                const uint64_t begin = s->state->block_trace ? BlockTraceClock() : 0;
				WaitReadBuffers(&b.ops[j]);
                if (s->state->block_trace) {
                    const auto &r=b.trace_requests[j];
                    BlockTraceRecord(s->state->block_trace, "CLEANUP_WAIT", begin, BlockTraceClock(),
                                     r.id, b.first_block+j, r.pages, r.pages, 1, b.buffers[j]);
                }
				b.pending[j] = false;
				--s->inflight;
			}
		}
	}
	BlockTraceDump(s->state->block_trace);
	/* Keep every request and its buffer pins alive until all outstanding
	 * requests have completed, including when LIMIT abandons most of a queue. */
	for (uint32_t i = 0; i < s->slot_count; ++i) {
		auto &b = s->slots[i];
		for (uint32_t j = 0; j < s->stats.batch_pages; ++j)
			if (BufferIsValid(b.buffers[j]))
				ReleaseBuffer(b.buffers[j]);
		if(b.trace_requests) pfree(b.trace_requests);
		if(b.owner_request_index) pfree(b.owner_request_index);
		if(b.request_submit_return_ns) pfree(b.request_submit_return_ns);
		if(b.request_initial_need_wait) pfree(b.request_initial_need_wait);
		pfree(b.pending);
		pfree(b.ops);
		pfree(b.buffers);
	}
	FreeAccessStrategy(s->strategy);
    if (s->state->block_trace) {
        pfree(s->state->block_trace->events);
        pfree(s->state->block_trace);
        s->state->block_trace=nullptr;
    }
	pfree(s->slots);
	if(s->io_stats) pfree(s->io_stats);
	pfree(s);
}
void YaapReadStreamEnableIOProfile(YaapReadStream *s, bool detail, uint32_t requested,
                                  uint32_t query_cap, uint32_t backend_cap) {
    if(s->io_stats) return;
    Assert(s->used==0 && s->stats.calls==0);
    MemoryContext old=MemoryContextSwitchTo(TopTransactionContext);
    s->io_stats=static_cast<ScanIOStats *>(palloc(sizeof(ScanIOStats)));
    new(s->io_stats) ScanIOStats{};
    auto &io=*s->io_stats;
    io.enabled=1; io.detail=detail; io.streams_created=1;
#ifdef YAAP_PG_AIO_TIMING_VERSION
    io.kernel_feature_supported = YAAP_PG_AIO_TIMING_VERSION;
#endif
#ifdef YAAP_PG_BUFFER_PROFILE_VERSION
    io.buffer_feature_supported = YAAP_PG_BUFFER_PROFILE_VERSION;
#endif
    io.requested_pages=requested; io.granted_pages=s->stats.window_pages;
    io.query_share_cap=query_cap; io.backend_share_cap=backend_cap;
    io.pg_pin_cap=s->scan->rs_base.rs_rd->rd_rel->relpersistence==RELPERSISTENCE_TEMP ? GetLocalPinLimit():GetPinLimit();
    io.ring_capacity=GetAccessStrategyBufferCount(s->strategy);
    io.request_cap=s->stats.max_requests; io.batch_pages=s->stats.batch_pages;
    io.block_size=BLCKSZ; io.io_method=io_method; io.io_workers=io_workers;
    io.effective_io_concurrency=effective_io_concurrency;
    io.io_combine_limit=io_combine_limit; io.io_max_concurrency=io_max_concurrency;
    io.shared_buffers_pages=NBuffers;
    for(uint32_t i=0;i<s->slot_count;++i) {
        auto &b=s->slots[i];
        if (!b.owner_request_index)
            b.owner_request_index=static_cast<uint16_t *>(palloc0(sizeof(uint16_t)*s->stats.batch_pages));
        b.request_submit_return_ns=static_cast<uint64_t *>(palloc0(sizeof(uint64_t)*s->stats.batch_pages));
        b.request_initial_need_wait=static_cast<bool *>(palloc0(sizeof(bool)*s->stats.batch_pages));
    }
    MemoryContextSwitchTo(old);
}
const ScanIOStats *YaapReadStreamGetIOStats(const YaapReadStream *s) { return s->io_stats; }
const YaapReadStreamStats &YaapReadStreamGetStats(const YaapReadStream *s) { return s->stats; }

void YaapReadStreamEnableBlockTrace(YaapReadStream *s, uint32_t capacity) {
    if (!capacity || s->state->block_trace) return;
    Assert(!s->stats.calls && capacity<=65536);
    MemoryContext old=MemoryContextSwitchTo(TopTransactionContext);
    auto *t=static_cast<ScanBlockTrace *>(palloc0(sizeof(ScanBlockTrace)));
    t->capacity=capacity; t->state=s->state; t->stream=BlockTraceClock();
    t->events=static_cast<ScanBlockEvent *>(palloc0(sizeof(ScanBlockEvent)*capacity));
    s->state->block_trace=t;
    for (uint32_t i=0; i<s->slot_count; ++i) {
        auto &b=s->slots[i];
        b.trace_requests=static_cast<BlockTraceRequest *>(palloc0(sizeof(BlockTraceRequest)*s->stats.batch_pages));
        if (!b.owner_request_index)
            b.owner_request_index=static_cast<uint16_t *>(palloc0(sizeof(uint16_t)*s->stats.batch_pages));
    }
    MemoryContextSwitchTo(old);
}
} /* namespace pipeline */
} /* namespace pg_yaap */
