extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shm_toc.h"
#include "utils/dsa.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"
}

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <memory>
#include <vector>

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/dsm_task_queue.hpp"
#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/output_sink.hpp"
#include "parallel/pipeline/physical_cross_product.hpp"
#include "parallel/pipeline/physical_hash_join.hpp"
#include "parallel/pipeline/physical_hash_join_probe.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/pipeline_leader.hpp"
#include "parallel/pipeline/pipeline_profile.hpp"
#include "parallel/pipeline/pipeline_timeline.hpp"
#include "parallel/pipeline/query_state.hpp"
#include "parallel/pipeline/runtime_dsm.hpp"
#include "parallel/pipeline/task_scheduler.hpp"
#include "parallel/pipeline/types.hpp"
#include "parallel/pipeline/worker_error.hpp"

extern "C" {
extern int pg_yaap_parallel_max_workers;
extern bool pg_yaap_trace_hooks;
extern char pg_yaap_bgworker_library_path[];
extern bool pg_yaap_trace_execution_path;
extern void pg_yaap_proc_exit_release_jit_contexts(int code, Datum arg);
}

namespace pg_yaap {
namespace pipeline {

namespace {


/* B.2 startup-tax probe: one-shot phase timer to localize the ~2700 ms cold-
 * cache fixed cost in PgYaapPipelineRun. Toggle via env PG_YAAP_PHASE=1.
 * Compiled in always (cheap: 2 instr_time reads + an fprintf per phase, only
 * when env set). Remove once B.2 lever is identified and committed. */
struct PhaseTimer {
	instr_time t0;
	instr_time prev;
	bool       on;
	PhaseTimer() {
		const char *e = getenv("PG_YAAP_PHASE");
		on = (e != nullptr && e[0] == '1');
		if (on) { INSTR_TIME_SET_CURRENT(t0); prev = t0; }
	}
	void mark(const char *tag) {
		if (!on) return;
		instr_time now; INSTR_TIME_SET_CURRENT(now);
		const TimestampTz abs_now = GetCurrentTimestamp();
		instr_time d_total = now; INSTR_TIME_SUBTRACT(d_total, t0);
		instr_time d_step  = now; INSTR_TIME_SUBTRACT(d_step,  prev);
		fprintf(stderr, "PG_YAAP_PHASE abs=%s pid=%d tag=%-22s step=%6.1f ms total=%7.1f ms\n",
		        timestamptz_to_str(abs_now),
		        MyProcPid, tag,
		        INSTR_TIME_GET_MILLISEC(d_step),
		        INSTR_TIME_GET_MILLISEC(d_total));
		fflush(stderr);
		prev = now;
	}
};

struct LeaderCleanupState {
	PipelineSharedControl *control = nullptr;
	PgVector<BackgroundWorkerHandle *> *handles = nullptr;
	PgYaapQueryState *state = nullptr;
};

static void
WakeStartedWorkers(const PgVector<BackgroundWorkerHandle *> &handles)
{
	for (BackgroundWorkerHandle *handle : handles)
	{
		if (handle == nullptr)
			continue;

		pid_t pid = 0;
		if (GetBackgroundWorkerPid(handle, &pid) != BGWH_STARTED)
			continue;

		PGPROC *proc = BackendPidGetProc(pid);
		if (proc != nullptr)
			SetLatch(&proc->procLatch);
	}
}

static bool
WaitForBackgroundWorkerShutdownTimed(BackgroundWorkerHandle *handle, long timeout_ms)
{
	long waited_ms = 0;
	while (waited_ms < timeout_ms)
	{
		pid_t pid = 0;
		BgwHandleStatus status = GetBackgroundWorkerPid(handle, &pid);
		if (status == BGWH_STOPPED || status == BGWH_POSTMASTER_DIED)
			return true;

		const long step_ms = 10;
		int rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
					   step_ms,
					   WAIT_EVENT_BGWORKER_SHUTDOWN);
		ResetLatch(MyLatch);
		if (rc & WL_POSTMASTER_DEATH)
			return true;
		waited_ms += step_ms;
	}
	return false;
}

static void
DetectUnexpectedWorkerExit(PipelineSharedControl *control,
							 const PgVector<BackgroundWorkerHandle *> &handles)
{
	if (control == nullptr || WorkerErrorPending(control))
		return;
	for (size_t i = 0; i < handles.size(); ++i)
	{
		BackgroundWorkerHandle *handle = handles[i];
		if (handle == nullptr)
			continue;
		pid_t pid = 0;
		const BgwHandleStatus status = GetBackgroundWorkerPid(handle, &pid);
		if (status != BGWH_STOPPED && status != BGWH_POSTMASTER_DIED)
			continue;
		ReportWorkerError(control,
			"pg_yaap worker %zu (pid %d) exited unexpectedly",
			i, static_cast<int>(pid));
		return;
	}
}

static void
SignalShutdownAndWait(const LeaderCleanupState &cleanup,
					  bool terminate_workers = false)
{
	if (cleanup.control != nullptr)
		pg_atomic_write_u32(&cleanup.control->shutdown_requested, 1u);

	if (cleanup.handles != nullptr)
	{
		WakeStartedWorkers(*cleanup.handles);
		if (terminate_workers)
		{
			for (BackgroundWorkerHandle *handle : *cleanup.handles)
			{
				if (handle != nullptr)
					(void) WaitForBackgroundWorkerShutdownTimed(handle, 250);
			}
			for (BackgroundWorkerHandle *handle : *cleanup.handles)
			{
				if (handle != nullptr)
				{
					pid_t pid = 0;
					BgwHandleStatus status = GetBackgroundWorkerPid(handle, &pid);
					if (status == BGWH_STARTED)
						TerminateBackgroundWorker(handle);
				}
			}
		}
		for (BackgroundWorkerHandle *handle : *cleanup.handles)
		{
			if (handle == nullptr)
				continue;
			WaitForBackgroundWorkerShutdown(handle);
		}
	}

	/* Invariant: DSA stays mapped after this returns. The success path drains
	 * the global TDC via OutputSink::EmitGlobalTdcToDest after worker join,
	 * which dereferences DSA-resident schema/layout/payload. Destruction is
	 * the caller's responsibility (see ShutdownAndDestroy below). */
}

static void
ShutdownAndDestroy(const LeaderCleanupState &cleanup,
				   bool terminate_workers = false)
{
	SignalShutdownAndWait(cleanup, terminate_workers);
	PipelineTimelineReport(cleanup.control,
		cleanup.state ? cleanup.state->runtime_dsa : nullptr, false);
	if (cleanup.state != nullptr)
		DestroyRuntimeDsm(cleanup.state);
}

static void
DestroyLeaderMemoryContext(MemoryContext old_mcxt, MemoryContext leader_mcxt)
{
	MemoryContextSwitchTo(old_mcxt);
	if (leader_mcxt != nullptr)
		MemoryContextDelete(leader_mcxt);
}

/*
 * Bug L: stack-local C++ containers in PgYaapPipelineRun hold storage
 * palloc'd in leader_mcxt; their destructors call pfree(), which reads the
 * owning context from the chunk header (mcxt.c:1619). If leader_mcxt has
 * already been MemoryContextDelete'd, that lookup hits freed memory and
 * corrupts the parent context's freelist → next AllocSetAlloc SEGVs at
 * aset.c:1060. The guard ties teardown to scope-exit so it runs AFTER
 * containers declared later in the same scope are destroyed.
 *
 * PG_CATCH path: PG_RE_THROW longjmps over destructors, so PG_CATCH must
 * call DestroyLeaderMemoryContext explicitly and Disarm() the guard.
 */
class LeaderMemoryContextGuard
{
public:
	LeaderMemoryContextGuard(MemoryContext old_mcxt, MemoryContext leader_mcxt)
		: old_mcxt_(old_mcxt), leader_mcxt_(leader_mcxt) {}

	~LeaderMemoryContextGuard()
	{
		if (armed_)
			DestroyLeaderMemoryContext(old_mcxt_, leader_mcxt_);
	}

	void Disarm() noexcept { armed_ = false; }

	LeaderMemoryContextGuard(const LeaderMemoryContextGuard &) = delete;
	LeaderMemoryContextGuard &operator=(const LeaderMemoryContextGuard &) = delete;

private:
	MemoryContext old_mcxt_;
	MemoryContext leader_mcxt_;
	bool armed_ = true;
};

static bool
FailEarly(const char **failure_reason,
		  const char *reason,
		  const LeaderCleanupState &cleanup,
		  PgYaapQueryState *state)
{
	if (failure_reason != nullptr)
	{
		/* A worker's diagnostic may live in the DSM we are about to unmap.
		 * Return a copy that also outlives the leader's local context. */
		*failure_reason = MemoryContextStrdup(state->context, reason);
	}
	if (state != nullptr)
	{
		state->parallel_plan = nullptr;
		state->parallel_scheduler = nullptr;
	}
	ShutdownAndDestroy(cleanup);
	return false;
}

static void
ReleaseHashJoinPayloadsAtQueryEnd(ExecCtx &ctx, MetaPipelineBundle &bundle)
{
	std::vector<PhysicalHashJoin *> seen;
	for (auto &pipeline_uptr : bundle.pipelines)
	{
		std::vector<PhysicalOperator *> candidates;
		candidates.reserve(pipeline_uptr->ops.size() + 2);
		for (PhysicalOperator *op : pipeline_uptr->ops)
			candidates.push_back(op);
		if (pipeline_uptr->source != nullptr)
			candidates.push_back(pipeline_uptr->source);
		if (pipeline_uptr->sink != nullptr)
			candidates.push_back(pipeline_uptr->sink);
		for (PhysicalOperator *op : candidates)
		{
			if (op == nullptr || op->type() != PhysicalOperatorType::HASH_JOIN)
				continue;
			auto *join = static_cast<PhysicalHashJoin *>(op);
			if (std::find(seen.begin(), seen.end(), join) == seen.end())
			{
				seen.push_back(join);
				join->ReleaseBuildPayloadAtQueryEnd(ctx);
			}
		}
	}
}

static void
RaiseWorkerFailure(PipelineSharedControl *control,
			   const PgVector<BackgroundWorkerHandle *> &handles,
			   PgYaapQueryState *state)
{
	/*
	 * Snapshot the worker error message into stack memory before throwing.
	 * After ereport(ERROR), the surrounding PG_CATCH (in PgYaapPipelineRun)
	 * will run SignalShutdownAndWait → DestroyRuntimeDsm, which detaches the
	 * DSM segment that backs `control->worker_error_msg`. We must NOT do
	 * cleanup here ourselves: doing so would leave the PG_CATCH path with
	 * a stale `cleanup.control` pointing into an unmapped DSM segment and
	 * SIGSEGV on the second `pg_atomic_write_u32(&control->shutdown_requested,...)`.
	 *
	 * Sole responsibility for cleanup belongs to PG_CATCH. We only ereport.
	 *
	 * Unused parameters (handles, state) are retained for caller-site
	 * symmetry and to make the cleanup-ownership contract obvious at the
	 * call site (line ~608).
	 */
	(void) handles;
	(void) state;

	char errmsg_buf[PIPELINE_WORKER_ERROR_MSG_LEN];
	const char *message = WorkerErrorMessage(control);
	/* A reporter may have died while owning WRITING. Never read its partial
	 * buffer, and do not wait forever for a READY it can no longer publish. */
	std::snprintf(errmsg_buf, sizeof(errmsg_buf), "%s",
		message != nullptr ? message : "worker failed while publishing its diagnostic");

	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("pg_yaap worker reported failure: %s",
					errmsg_buf[0] != '\0' ? errmsg_buf : "(no message)")));
}

}  /* namespace */

bool
PgYaapPipelineRun(QueryDesc *queryDesc,
					PgYaapQueryState *state,
					const char **failure_reason)
{
	static uint32 wait_event_id = 0;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;

	if (queryDesc == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "pg_yaap: QueryDesc is null";
		return false;
	}
	if (state == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "pg_yaap: query state is null";
		return false;
	}
	if (state->parallel_plan == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "pg_yaap: parallel_plan is null";
		return false;
	}

	MemoryContext old_mcxt = CurrentMemoryContext;
	MemoryContext leader_mcxt = AllocSetContextCreate(CurrentMemoryContext,
									 "pg_yaap leader",
									 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(leader_mcxt);

	LeaderMemoryContextGuard mcxt_guard{old_mcxt, leader_mcxt};

	LeaderCleanupState cleanup{};
	cleanup.state = state;

	PhaseTimer phase{};
	phase.mark("T0_entry");

	auto *root = static_cast<PhysicalOperator *>(state->parallel_plan);
	state->parallel_plan = nullptr;

	std::unique_ptr<MetaPipelineBundle> bundle;
	PipelineSharedControl *control = nullptr;
	DsmTaskQueue *queue = nullptr;
	dsa_area *dsa = nullptr;
	shm_toc *toc = nullptr;
	PgVector<BackgroundWorkerHandle *> handles{
		PgMemoryContextAllocator<BackgroundWorkerHandle *>(leader_mcxt)};
	PgVector<pid_t> registered_pids{
		PgMemoryContextAllocator<pid_t>(leader_mcxt)};

	cleanup.handles = &handles;

	bool success = false;
	PG_TRY();
	{
		/* Return from the lambda, then restore PG exception stacks at END_TRY. */
		success = [&]() -> bool {
		if (wait_event_id == 0)
			wait_event_id = WaitEventExtensionNew("pg_yaap leader");

		if (pg_yaap_trace_execution_path)
			elog(LOG, "pg_yaap pipeline leader: before MetaPipeline::Build");
		bundle = MetaPipeline::Build(std::unique_ptr<PhysicalOperator>(root));
		root = nullptr;
		if (bundle == nullptr)
		{
			return FailEarly(failure_reason, "pg_yaap: MetaPipeline::Build returned null", cleanup, state);
		}
		if (bundle->pipelines.empty())
		{
			return FailEarly(failure_reason, "pg_yaap: BuildPipelines produced no pipelines", cleanup, state);
		}

		for (size_t i = 0; i < bundle->pipelines.size(); ++i)
			Assert(bundle->pipelines[i]->id == static_cast<PipelineId>(i));
		if (pg_yaap_trace_execution_path)
			elog(LOG, "pg_yaap pipeline leader: built pipelines=%zu", bundle->pipelines.size());

		phase.mark("T1_build_done");

		const int bgworker_count = pg_yaap_parallel_max_workers;
		if (bgworker_count <= 0)
		{
			return FailEarly(failure_reason, "pg_yaap: pg_yaap.parallel_max_workers must be >= 1", cleanup, state);
		}
		if (state->runtime_dsm == nullptr || state->runtime_dsa == nullptr)
		{
			const char *err = nullptr;
			if (!CreateRuntimeDsm(state, &err))
			{
				return FailEarly(failure_reason, err != nullptr ? err : "pg_yaap: runtime DSM creation failed", cleanup, state);
			}
		}

		phase.mark("T2_dsm_dsa_ready");

		toc = shm_toc_attach(PIPELINE_DSM_MAGIC,
						  dsm_segment_address(state->runtime_dsm));
		if (toc == nullptr)
		{
			return FailEarly(failure_reason, "pg_yaap: failed to attach runtime shm_toc", cleanup, state);
		}

		control = static_cast<PipelineSharedControl *>(
			shm_toc_lookup(toc, PIPELINE_DSM_KEY_CONTROL, false));
		void *queue_buf = shm_toc_lookup(toc, PIPELINE_DSM_KEY_TASK_QUEUE, false);
		dsa = state->runtime_dsa;
		queue = DsmTaskQueue::AttachInPlace(queue_buf);

		cleanup.control = control;

		Assert(control != nullptr);
		Assert(dsa != nullptr);
		Assert(queue != nullptr);
		Assert(control->magic == PIPELINE_DSM_MAGIC);
		Assert(control->leader_pid == MyProcPid);
		control->num_workers = bgworker_count;

		Snapshot leader_snapshot = queryDesc != nullptr ? queryDesc->snapshot : nullptr;
		if (leader_snapshot == nullptr)
			return FailEarly(failure_reason, "pg_yaap: query has no snapshot to share with workers", cleanup, state);
		Size snapshot_size = EstimateSnapshotSpace(leader_snapshot);
		control->snapshot_root = dsa_allocate(dsa, snapshot_size);
		SerializeSnapshot(leader_snapshot,
			static_cast<char *>(dsa_get_address(dsa, control->snapshot_root)));

		control->pipelines_root = LeaderSerializePipelines(*bundle, dsa);
		control->num_pipelines = static_cast<int32>(bundle->pipelines.size());
		if (pg_yaap_trace_execution_path)
			elog(LOG, "pg_yaap pipeline leader: serialized pipelines root=%llu count=%d",
				 static_cast<unsigned long long>(control->pipelines_root),
				 control->num_pipelines);

		phase.mark("T3_descriptor_pub");

		ExecCtx leader_ctx{leader_mcxt, dsa, LEADER_WORKER_INDEX, control, INVALID_EVENT_ID};
		TaskScheduler::BuildEvents(*bundle, leader_ctx);
		PipelineProfileAllocate(control, dsa, control->event_count,
			static_cast<uint32_t>(bgworker_count));
		pg_atomic_write_u32(&control->trace_execution_path,
							 pg_yaap_trace_execution_path ? 1u : 0u);
		PipelineProfileRegisterProcess(control,
							  dsa,
							  LEADER_WORKER_INDEX,
							  MyProcPid);
		instr_time profile_query_start;
		if (pg_atomic_read_u32(&control->profile_enabled) != 0)
			INSTR_TIME_SET_CURRENT(profile_query_start);

		/* Shared payload publication happens during descriptor serialization;
		 * there is no separate leader pre-bind virtual in the current operator
		 * base. */

		handles.reserve(static_cast<size_t>(bgworker_count));
		for (int worker_index = 0; worker_index < bgworker_count; ++worker_index)
		{
			BackgroundWorker bgw{};
			BackgroundWorkerHandle *handle = nullptr;

			bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
						BGWORKER_BACKEND_DATABASE_CONNECTION;
			bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
			bgw.bgw_restart_time = BGW_NEVER_RESTART;
			std::snprintf(bgw.bgw_library_name,
					  MAXPGPATH,
					  "%s",
					  (pg_yaap_bgworker_library_path[0] != '\0')
					  	? pg_yaap_bgworker_library_path
					  	: "pg_yaap");
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: launching worker with library=%s", bgw.bgw_library_name);
			std::snprintf(bgw.bgw_function_name, BGW_MAXLEN,
					  "pg_yaap_pipeline_worker_main");
			std::snprintf(bgw.bgw_name, BGW_MAXLEN,
					  "pg_yaap worker %d", worker_index);
			std::snprintf(bgw.bgw_type, BGW_MAXLEN, "pg_yaap worker");
			bgw.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(state->runtime_dsm));
			bgw.bgw_notify_pid = MyProcPid;
			std::memcpy(bgw.bgw_extra, &worker_index, sizeof(int32_t));

			if (!RegisterDynamicBackgroundWorker(&bgw, &handle))
				break;

			handles.push_back(handle);
		}

		if (handles.empty())
			return FailEarly(failure_reason, "pg_yaap: no pipeline worker slots available", cleanup, state);

		phase.mark("T4_bgw_registered");

		registered_pids.reserve(handles.size());

		auto *ready_array = static_cast<pg_atomic_uint32 *>(
			shm_toc_lookup(toc, PIPELINE_DSM_KEY_WORKER_READY, false));
		Assert(ready_array != nullptr);

		for (size_t worker_idx = 0; worker_idx < handles.size(); ++worker_idx)
		{
			BackgroundWorkerHandle *handle = handles[worker_idx];
			pid_t worker_pid = 0;
			BgwHandleStatus status = WaitForBackgroundWorkerStartup(handle, &worker_pid);

			if (status != BGWH_STARTED)
			{
				const char *reason = (status == BGWH_POSTMASTER_DIED)
					? "pg_yaap: postmaster died while starting pipeline worker"
					: "pg_yaap: pipeline worker exited before startup completed";
				return FailEarly(failure_reason, reason, cleanup, state);
			}

			/*
			 * BGWH_STARTED means the postmaster has assigned slot->pid only;
			 * the worker may not yet have completed InitProcessPhase2, so its
			 * PGPROC may not yet be visible to BackendPidGetProc. Wait for the
			 * worker to publish readiness after setting up its PG connection
			 * and reconstructing the pipelines. Detect early worker death
			 * via BGWH_STOPPED.
			 */
			while (pg_atomic_read_membarrier_u32(&ready_array[worker_idx]) == 0)
			{
				ResetLatch(MyLatch);
				CHECK_FOR_INTERRUPTS();

				pid_t poll_pid = 0;
				BgwHandleStatus poll_status = GetBackgroundWorkerPid(handle, &poll_pid);
				if (poll_status == BGWH_STOPPED)
				{
					const char *message = WorkerErrorMessage(control);
					if (message != nullptr && message[0] != '\0')
						return FailEarly(failure_reason, message, cleanup, state);
					return FailEarly(failure_reason, "pg_yaap: pipeline worker exited before reporting ready", cleanup, state);
				}
				if (poll_status == BGWH_POSTMASTER_DIED)
				{
					return FailEarly(failure_reason, "pg_yaap: postmaster died while waiting for worker ready", cleanup, state);
				}

				if (pg_atomic_read_membarrier_u32(&ready_array[worker_idx]) != 0)
					break;

				instr_time wait_start;
				if (pg_atomic_read_u32(&control->profile_enabled) != 0)
					INSTR_TIME_SET_CURRENT(wait_start);
				int rc = WaitLatch(MyLatch,
								   WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
								   1000,
								   wait_event_id);
				if (pg_atomic_read_u32(&control->profile_enabled) != 0)
				{
					instr_time wait_end;
					INSTR_TIME_SET_CURRENT(wait_end);
					instr_time elapsed = wait_end;
					INSTR_TIME_SUBTRACT(elapsed, wait_start);
					PipelineProfileAddElapsed(control,
										  dsa,
										  LEADER_WORKER_INDEX,
										  0,
										  PipelineProfileStage::LEADER_WAIT_READY,
										  elapsed);
				}
				if (rc & WL_POSTMASTER_DEATH)
				{
					return FailEarly(failure_reason, "pg_yaap: postmaster died while waiting for worker ready", cleanup, state);
				}
			}

			PGPROC *proc = BackendPidGetProc(worker_pid);
			if (proc == nullptr)
			{
				return FailEarly(failure_reason, "pg_yaap: BackendPidGetProc returned null after worker ready", cleanup, state);
			}

			registered_pids.push_back(worker_pid);
			PipelineProfileRegisterProcess(control,
							  dsa,
							  static_cast<int>(worker_idx),
							  worker_pid);
		}

		/* The successful registrations form a dense prefix. Size the task
		 * cohorts from that ready set before publishing any work. */
		control->num_workers = static_cast<int>(registered_pids.size());
		if (pg_yaap_trace_execution_path)
			elog(LOG, "pg_yaap pipeline leader: workers requested=%d ready=%d",
				bgworker_count, control->num_workers);

		/* Register worker latches before publishing the graph. */
		queue->RegisterWorkerPids(registered_pids.data(),
						 static_cast<uint32>(registered_pids.size()));

		phase.mark("T5_workers_ready");

		OutputSink *final_output = nullptr;
		for (const auto &pipeline : bundle->pipelines)
		{
			if (pipeline->sink != nullptr && pipeline->sink->type() == PhysicalOperatorType::OUTPUT)
				final_output = static_cast<OutputSink *>(pipeline->sink);
		}

		pg_atomic_write_membarrier_u32(&control->scheduler_started, 1);
		queue->WakeRegisteredWorkers();
		phase.mark("T6_release_event_graph");

		for (;;)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();

			DetectUnexpectedWorkerExit(control, handles);
			if (WorkerErrorPending(control))
				RaiseWorkerFailure(control, handles, state);
			if (pg_atomic_read_membarrier_u32(&control->events_remaining) == 0)
				break;

			instr_time wait_start;
			if (pg_atomic_read_u32(&control->profile_enabled) != 0)
				INSTR_TIME_SET_CURRENT(wait_start);
			int rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
						   1000,
						   wait_event_id);
			if (pg_atomic_read_u32(&control->profile_enabled) != 0)
			{
				instr_time wait_end;
				INSTR_TIME_SET_CURRENT(wait_end);
				instr_time elapsed = wait_end;
				INSTR_TIME_SUBTRACT(elapsed, wait_start);
				PipelineProfileAddElapsed(control,
									  dsa,
									  LEADER_WORKER_INDEX,
									  0,
									  PipelineProfileStage::LEADER_WAIT_EVENT,
									  elapsed);
			}
			if (rc & WL_POSTMASTER_DEATH)
			{
				cleanup.control = control;
				ShutdownAndDestroy(cleanup);
				proc_exit(1);
			}
		}

		/*
		 * Successful completion path: all events FINISHED. Workers are still
		 * blocked in their main-loop WaitLatch (TryPop returned empty
		 * + nobody SetLatch'd them since the queue is permanently drained).
		 * We must signal shutdown and wake them, otherwise
		 * WaitForBackgroundWorkerShutdown below blocks forever.
		 *
		 * SignalShutdownAndWait does exactly this: writes shutdown_requested=1,
		 * wakes every started worker latch, then waits for BGWH_STOPPED on each
		 * handle. Mirrors the error/PG_CATCH paths (lines 606, 630) which
		 * already use it.
		 */
		SignalShutdownAndWait(cleanup);
		ReleaseHashJoinPayloadsAtQueryEnd(leader_ctx, *bundle);

		phase.mark("T7_events_done");

		if (final_output != nullptr)
		{
			final_output->RefreshDestFromQueryDesc(
				queryDesc->dest,
				queryDesc->tupDesc,
				static_cast<int>(queryDesc->operation));
			EventId previous_profile_event = leader_ctx.profile_event_id;
			leader_ctx.profile_event_id = 0;
			{
				PipelineProfileScope emit_scope(leader_ctx,
					PipelineProfileStage::OUTPUT_EMIT);
				final_output->EmitGlobalTdcToDest(leader_ctx);
			}
			leader_ctx.profile_event_id = previous_profile_event;
		}

		phase.mark("T8_emit_done");

		if (WorkerErrorPending(control))
			RaiseWorkerFailure(control, handles, state);

		if (pg_atomic_read_u32(&control->profile_enabled) != 0)
		{
			instr_time profile_query_end;
			INSTR_TIME_SET_CURRENT(profile_query_end);
			instr_time elapsed = profile_query_end;
			INSTR_TIME_SUBTRACT(elapsed, profile_query_start);
			PipelineProfileAddElapsed(control,
								  dsa,
								  LEADER_WORKER_INDEX,
								  0,
								  PipelineProfileStage::TOTAL,
								  elapsed);
		}
		PipelineProfileReport(control, dsa);
		PipelineTimelineReport(control, dsa, true);

		DestroyRuntimeDsm(state);
		state->parallel_scheduler = nullptr;
		return true;
		}();
	}
	PG_CATCH();
	{
		/* Preserve the query error while best-effort profile export handles
		 * and flushes any error of its own. Keep the saved ErrorData in a
		 * query context that survives FlushErrorState and DSM teardown. */
		MemoryContextSwitchTo(leader_mcxt);
		ErrorData *query_error = CopyErrorData();
		FlushErrorState();
		state->parallel_plan = nullptr;
		state->parallel_scheduler = nullptr;
		/* These handles live in escaped cleanup/query state; stack locals
		 * assigned inside PG_TRY are indeterminate after longjmp. */
		PipelineSharedControl *error_control = cleanup.control;
		dsa_area *error_dsa = cleanup.state ? cleanup.state->runtime_dsa : nullptr;
		if (state->runtime_dsm != nullptr) {
			shm_toc *error_toc = shm_toc_attach(PIPELINE_DSM_MAGIC, dsm_segment_address(state->runtime_dsm));
			if (error_toc != nullptr)
				error_control = static_cast<PipelineSharedControl *>(shm_toc_lookup(error_toc, PIPELINE_DSM_KEY_CONTROL, false));
			cleanup.control = error_control;
		}
		PipelineTimelineAbortProcess(error_control, error_dsa, LEADER_WORKER_INDEX,
			query_error->sqlerrcode == ERRCODE_QUERY_CANCELED);
		SignalShutdownAndWait(cleanup, true);
		PipelineTimelineReport(error_control, error_dsa, false);
		if (cleanup.state != nullptr)
			DestroyRuntimeDsm(cleanup.state);
		/* PG_RE_THROW skips operator destructors. Drain the raw-pointer
		 * registry before transaction abort releases its ResourceOwner. */
		pg_yaap_proc_exit_release_jit_contexts(1, (Datum) 0);
		/*
		 * Bug L: do NOT DestroyLeaderMemoryContext here. PG_RE_THROW
		 * longjmps over C++ destructors, so PgVector containers above
		 * never pfree(). leader_mcxt is a child of the caller's context
		 * and dies on AbortCurrentTransaction / ExecutorEnd cleanup.
		 */
		ReThrowError(query_error);
	}
	PG_END_TRY();
	return success;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
