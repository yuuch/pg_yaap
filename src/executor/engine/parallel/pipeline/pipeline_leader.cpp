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
#include "utils/timestamp.h"
#include "utils/wait_event.h"
}

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <memory>

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/dsm_task_queue.hpp"
#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/output_sink.hpp"
#include "parallel/pipeline/physical_cross_product.hpp"
#include "parallel/pipeline/physical_hash_join.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/pipeline_dsm_lookup.hpp"
#include "parallel/pipeline/pipeline_leader.hpp"
#include "parallel/pipeline/pipeline_profile.hpp"
#include "parallel/pipeline/query_state.hpp"
#include "parallel/pipeline/runtime_dsm.hpp"
#include "parallel/pipeline/task.hpp"
#include "parallel/pipeline/task_scheduler.hpp"
#include "parallel/pipeline/types.hpp"

extern "C" {
extern int pg_yaap_parallel_max_workers;
extern bool pg_yaap_parallel_leader_participation;
extern bool pg_yaap_trace_hooks;
extern char pg_yaap_bgworker_library_path[];
extern bool pg_yaap_trace_execution_path;
}

namespace pg_yaap {
namespace pipeline {

namespace {

static constexpr uint32_t kTaskQueueCapacity = 64;

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
		*failure_reason = reason;
	if (state != nullptr)
	{
		state->parallel_plan = nullptr;
		state->parallel_scheduler = nullptr;
	}
	ShutdownAndDestroy(cleanup);
	return false;
}

static bool
AllEventsFinished(TaskScheduler &scheduler)
{
	for (uint32_t id = 0; id < scheduler.event_count(); ++id)
	{
		Event *event = scheduler.event_lookup().Resolve(id);
		Assert(event != nullptr);
		EventState state = event->state();
		if (state != EventState::FINISHED && state != EventState::ABORTED)
			return false;
	}
	return true;
}

enum class PipelineRunCleanupRole {
	SOURCE,
	OPERATOR,
	SINK,
};

static void
DispatchConsumerResourceCleanup(ExecCtx &ctx,
						 PhysicalOperator *op,
						 PipelineRunCleanupRole role)
{
	if (op == nullptr)
		return;

	switch (op->type())
	{
		case PhysicalOperatorType::HASH_JOIN:
			if (role == PipelineRunCleanupRole::OPERATOR)
				static_cast<PhysicalHashJoin *>(op)->ReleaseBuildPayloadAfterConsumerRun(ctx);
			return;
		case PhysicalOperatorType::CROSS_PRODUCT:
			if (role == PipelineRunCleanupRole::OPERATOR)
				static_cast<PhysicalCrossProduct *>(op)->ReleaseBuildPayloadAfterConsumerRun(ctx);
			return;
		default:
			return;
	}
}

static void
PipelinePostConsumerRunCleanup(ExecCtx &ctx, Pipeline &consumer)
{
	DispatchConsumerResourceCleanup(ctx, consumer.source, PipelineRunCleanupRole::SOURCE);
	for (PhysicalOperator *op : consumer.ops)
		DispatchConsumerResourceCleanup(ctx, op, PipelineRunCleanupRole::OPERATOR);
	DispatchConsumerResourceCleanup(ctx, consumer.sink, PipelineRunCleanupRole::SINK);
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
	std::memcpy(errmsg_buf, control->worker_error_msg, sizeof(errmsg_buf));
	errmsg_buf[sizeof(errmsg_buf) - 1] = '\0';

	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("pg_yaap worker reported failure: %s",
					errmsg_buf[0] != '\0' ? errmsg_buf : "(no message)")));
}

static TaskExecutionResult
ExecuteLeaderTask(TaskDescriptor desc,
			 Pipeline *pipeline,
			 WorkerTaskRuntime *leader_rt)
{
	std::unique_ptr<Task> task;

	switch (static_cast<TaskKind>(desc.kind))
	{
		case TaskKind::RUN:
			task = std::make_unique<PipelineRunTask>(desc.event_id,
								pipeline,
								leader_rt,
								desc.worker_index);
			break;
		case TaskKind::COMBINE:
			task = std::make_unique<PipelineCombineTask>(desc.event_id,
								pipeline,
								leader_rt,
								desc.worker_index,
								desc.partition_id);
			break;
		case TaskKind::FINALIZE:
			task = std::make_unique<PipelineFinalizeTask>(desc.event_id,
								pipeline,
								leader_rt,
								desc.worker_index);
			break;
	}

	Assert(task != nullptr);
	return task->Execute();
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
	WorkerTaskRuntime leader_rt{};
	PipelineDsmLookup<Pipeline> leader_lookup(leader_mcxt);
	PgVector<BackgroundWorkerHandle *> handles{
		PgMemoryContextAllocator<BackgroundWorkerHandle *>(leader_mcxt)};
	PgVector<pid_t> registered_pids{
		PgMemoryContextAllocator<pid_t>(leader_mcxt)};
	PgVector<char> event_scheduled{PgMemoryContextAllocator<char>(leader_mcxt)};
	PgVector<char> event_finished{PgMemoryContextAllocator<char>(leader_mcxt)};
	PgVector<char> run_cleanup_done{PgMemoryContextAllocator<char>(leader_mcxt)};

	cleanup.handles = &handles;

	PG_TRY();
	{
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

		control->pipelines_root = LeaderSerializePipelines(*bundle, dsa);
		control->num_pipelines = static_cast<int32>(bundle->pipelines.size());
		if (pg_yaap_trace_execution_path)
			elog(LOG, "pg_yaap pipeline leader: serialized pipelines root=%llu count=%d",
				 static_cast<unsigned long long>(control->pipelines_root),
				 control->num_pipelines);

		phase.mark("T3_descriptor_pub");

		const bool leader_participate_requested =
			pg_yaap_parallel_leader_participation;
		const bool leader_participate = false;
		(void) leader_participate_requested;
		/* Per-current scheduler/task-descriptor contract, leader participation
		 * remains disabled until a dedicated LEADER_WORKER_INDEX task slot is
		 * published. */

		TaskScheduler scheduler(leader_mcxt,
						std::move(bundle),
						TaskSchedulerSizing{static_cast<uint32_t>(bgworker_count),
										kTaskQueueCapacity});
		scheduler.BuildEvents();
		scheduler.BindRuntime(control, queue, dsa);
		scheduler.AllocateEventShmStates();
		if (pg_yaap_trace_execution_path)
			elog(LOG, "pg_yaap pipeline leader: scheduler initialized events=%u",
				 scheduler.event_count());
		PipelineProfileAllocate(control,
							 dsa,
							 scheduler.event_count(),
							 static_cast<uint32>(bgworker_count));
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
			{
				return FailEarly(failure_reason, "pg_yaap: RegisterDynamicBackgroundWorker failed (worker slots exhausted?)", cleanup, state);
			}

			handles.push_back(handle);
		}

		phase.mark("T4_bgw_registered");

		registered_pids.reserve(handles.size() + (leader_participate ? 1 : 0));

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
			 * worker to publish its ready bit (set after
			 * BackgroundWorkerInitializeConnectionByOid returns). Detect early
			 * worker death via BGWH_STOPPED.
			 */
			while (pg_atomic_read_u32(&ready_array[worker_idx]) == 0)
			{
				CHECK_FOR_INTERRUPTS();

				pid_t poll_pid = 0;
				BgwHandleStatus poll_status = GetBackgroundWorkerPid(handle, &poll_pid);
				if (poll_status == BGWH_STOPPED)
				{
					if (control->worker_error_msg[0] != '\0')
						return FailEarly(failure_reason, control->worker_error_msg, cleanup, state);
					return FailEarly(failure_reason, "pg_yaap: pipeline worker exited before reporting ready", cleanup, state);
				}
				if (poll_status == BGWH_POSTMASTER_DIED)
				{
					return FailEarly(failure_reason, "pg_yaap: postmaster died while waiting for worker ready", cleanup, state);
				}

				if (pg_atomic_read_u32(&ready_array[worker_idx]) != 0)
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
				ResetLatch(MyLatch);
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

		if (leader_participate)
			registered_pids.push_back(MyProcPid);

		queue->RegisterWorkerPids(registered_pids.data(),
						 static_cast<uint32>(registered_pids.size()));

		phase.mark("T5_workers_ready");

		leader_rt.exec_ctx = ExecCtx{leader_mcxt, dsa, LEADER_WORKER_INDEX, control, INVALID_EVENT_ID};
		leader_rt.control = control;
		leader_rt.event_shm = static_cast<EventShmState *>(
			dsa_get_address(dsa, control->event_states_root));
		leader_rt.pipelines = &leader_lookup;
		leader_rt.leader_qd = queryDesc;
		leader_rt.final_output = nullptr;

		for (auto &pipeline_uptr : scheduler.bundle().pipelines)
		{
			leader_lookup.Register(static_cast<uint32_t>(pipeline_uptr->id),
						      pipeline_uptr.get());
			if (pipeline_uptr->sink != nullptr &&
				pipeline_uptr->sink->type() == PhysicalOperatorType::OUTPUT)
			{
				leader_rt.final_output = static_cast<OutputSink *>(pipeline_uptr->sink);
			}
		}

		event_scheduled.assign(scheduler.event_count(), 0);
		event_finished.assign(scheduler.event_count(), 0);
		run_cleanup_done.assign(scheduler.event_count(), 0);

		for (auto &pipeline_uptr : scheduler.bundle().pipelines)
		{
			if (!pipeline_uptr->depends_on.empty())
				continue;

			EventId event_id = static_cast<EventId>(pipeline_uptr->id) * 3u;
			Event *event = scheduler.event_lookup().Resolve(event_id);
			Assert(event != nullptr);
			if (event->TrySchedule())
				event_scheduled[event_id] = 1;
		}

		phase.mark("T6_first_enqueue");

		for (;;)
		{
			CHECK_FOR_INTERRUPTS();

			if (pg_atomic_read_u32(&control->worker_error) != 0)
				RaiseWorkerFailure(control, handles, state);

			bool progress = false;

			for (uint32_t event_id = 0; event_id < scheduler.event_count(); ++event_id)
			{
				Event *event = scheduler.event_lookup().Resolve(event_id);
				Assert(event != nullptr);

				if (!event_scheduled[event_id] && event->state() == EventState::SCHEDULED)
					event_scheduled[event_id] = 1;

				if (!event_finished[event_id] && event->state() == EventState::ABORTED)
				{
					event_finished[event_id] = 1;
					progress = true;
					continue;
				}

				if ((event_id % 3u) == 2u &&
					event_scheduled[event_id] &&
					!event_finished[event_id] &&
					event->state() == EventState::SCHEDULED &&
					pg_atomic_read_u32(&leader_rt.event_shm[event_id].tasks_remaining) > 0)
				{
					Pipeline *pipeline = leader_lookup.Resolve(event->pipeline_id());
					Assert(pipeline != nullptr);

					TaskDescriptor desc{};
					desc.pipeline_id = static_cast<uint32_t>(pipeline->id);
					desc.event_id = event_id;
					desc.partition_id = UINT32_MAX;
					desc.worker_index = LEADER_WORKER_INDEX;
					desc.kind = static_cast<uint8_t>(TaskKind::FINALIZE);

					TaskExecutionResult exec_result = ExecuteLeaderTask(desc,
										   pipeline,
										   &leader_rt);
					if (exec_result != TaskExecutionResult::TASK_FINISHED)
					{
						ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("pg_yaap: leader finalize task did not finish")));
					}

					pg_atomic_write_u32(&leader_rt.event_shm[event_id].tasks_remaining, 0);
					event->FinishEvent();
					event_finished[event_id] = 1;
					progress = true;
					continue;
				}

				if (event_scheduled[event_id] &&
					!event_finished[event_id] &&
					pg_atomic_read_u32(&leader_rt.event_shm[event_id].tasks_remaining) == 0)
				{
					if ((event_id % 3u) == 0u && !run_cleanup_done[event_id])
					{
						Pipeline *pipeline = leader_lookup.Resolve(event->pipeline_id());
						Assert(pipeline != nullptr);
						PipelinePostConsumerRunCleanup(leader_rt.exec_ctx, *pipeline);
						run_cleanup_done[event_id] = 1;
					}
					event->FinishEvent();
					event_finished[event_id] = 1;
					progress = true;
				}
			}

			if (progress)
				continue;

			if (AllEventsFinished(scheduler))
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
			ResetLatch(MyLatch);
			if (rc & WL_POSTMASTER_DEATH)
			{
				cleanup.control = control;
				ShutdownAndDestroy(cleanup);
				proc_exit(1);
			}
		}

		/*
		 * Successful completion path: all events FINISHED. Workers are still
		 * blocked in their main-loop WaitLatch (TryPopForWorker returned empty
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

		phase.mark("T7_events_done");

		if (leader_rt.final_output != nullptr)
		{
			leader_rt.final_output->RefreshDestFromQueryDesc(
				queryDesc->dest,
				queryDesc->tupDesc,
				static_cast<int>(queryDesc->operation));
			EventId previous_profile_event = leader_rt.exec_ctx.profile_event_id;
			leader_rt.exec_ctx.profile_event_id = 0;
			{
				PipelineProfileScope emit_scope(leader_rt.exec_ctx,
					PipelineProfileStage::OUTPUT_EMIT);
				leader_rt.final_output->EmitGlobalTdcToDest(leader_rt.exec_ctx);
			}
			leader_rt.exec_ctx.profile_event_id = previous_profile_event;
		}

		phase.mark("T8_emit_done");

		if (pg_atomic_read_u32(&control->worker_error) != 0)
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

		DestroyRuntimeDsm(state);
		state->parallel_scheduler = nullptr;
		return true;
	}
	PG_CATCH();
	{
		state->parallel_plan = nullptr;
		state->parallel_scheduler = nullptr;
		ShutdownAndDestroy(cleanup, true);
		/*
		 * Bug L: do NOT DestroyLeaderMemoryContext here. PG_RE_THROW
		 * longjmps over C++ destructors, so PgVector containers above
		 * never pfree(). leader_mcxt is a child of the caller's context
		 * and dies on AbortCurrentTransaction / ExecutorEnd cleanup.
		 */
		PG_RE_THROW();
	}
	PG_END_TRY();
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
