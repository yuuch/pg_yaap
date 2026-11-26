/*
 * pipeline_worker_main.cpp
 */

extern "C" {
#include "postgres.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "libpq/pqsignal.h"
#include "tcop/tcopprot.h"
#include "pgstat.h"  // IWYU pragma: keep
#include "portability/instr_time.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"  // IWYU pragma: keep
#include "storage/latch.h"
#include "storage/proc.h"  // IWYU pragma: keep
#include "storage/procarray.h"
#include "storage/shm_toc.h"
#include "utils/dsa.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/inval.h"
#include "storage/sharedfileset.h"
}

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>

#include "core/memory.hpp"  // IWYU pragma: keep
#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/dsm_task_queue.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/pipeline_dsm_lookup.hpp"
#include "parallel/pipeline/pipeline_profile.hpp"
#include "parallel/pipeline/pipeline_timeline.hpp"
#include "parallel/pipeline/task.hpp"
#include "parallel/pipeline/task_scheduler.hpp"
#include "parallel/pipeline/types.hpp"
#include "parallel/pipeline/worker_error.hpp"

namespace pg_yaap {
namespace pipeline {

extern "C" PGDLLEXPORT void
pg_yaap_pipeline_worker_main(Datum main_arg);

extern "C" void
pg_yaap_proc_exit_release_jit_contexts(int code, Datum arg);

static inline bool
WorkerShutdownRequested(PipelineSharedControl *ctl)
{
	if (pg_atomic_read_u32(&ctl->shutdown_requested) != 0)
		return true;
	if (WorkerErrorPending(ctl))
		return true;
	return ctl->leader_pid != 0 && BackendPidGetProc(ctl->leader_pid) == nullptr;
}

static void
ReportWorkerFailure(PipelineSharedControl *ctl, const char *message)
{
	if (ctl == nullptr)
		return;

	ReportWorkerError(ctl, "%s", message != nullptr ? message : "(no message)");
	pg_atomic_write_u32(&ctl->shutdown_requested, 1u);
	PGPROC *leader = BackendPidGetProc(ctl->leader_pid);
	if (leader != nullptr)
		SetLatch(&leader->procLatch);
}

extern "C" PGDLLEXPORT void
pg_yaap_pipeline_worker_main(Datum main_arg)
{
	static uint32 wait_event_extension = 0;

	/* Match PostgreSQL parallel workers. Defer termination to an interrupt
	 * check so DSM teardown cannot start inside an LWLock wait. */
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();
	before_shmem_exit(pg_yaap_proc_exit_release_jit_contexts, (Datum) 0);
	dsm_handle handle = DatumGetUInt32(main_arg);
	dsm_segment *seg = dsm_attach(handle);
	if (seg == nullptr)
		ereport(ERROR,
		        (errmsg("pg_yaap worker could not attach DSM segment 0x%08x",
		                handle)));
	dsm_pin_mapping(seg);

	shm_toc *toc = shm_toc_attach(PIPELINE_DSM_MAGIC, dsm_segment_address(seg));
	if (toc == nullptr)
		ereport(ERROR,
		        (errmsg("pg_yaap worker shm_toc_attach failed (bad magic in DSM 0x%08x)",
		                handle)));

	PipelineSharedControl *ctl = static_cast<PipelineSharedControl *>(
		shm_toc_lookup(toc, PIPELINE_DSM_KEY_CONTROL, false));
	void *dsa_buf = shm_toc_lookup(toc, PIPELINE_DSM_KEY_DSA, false);
	void *queue_buf = shm_toc_lookup(toc, PIPELINE_DSM_KEY_TASK_QUEUE, false);

	if (ctl->magic != PIPELINE_DSM_MAGIC)
		ereport(ERROR,
		        (errmsg("pg_yaap worker attached to DSM with bad magic 0x%08x",
		                ctl->magic)));

	PG_TRY();
	{
		BackgroundWorkerInitializeConnectionByOid(ctl->db_oid, InvalidOid, 0);
		StartTransactionCommand();
		dsa_area *dsa = dsa_attach_in_place(dsa_buf, seg);
		if (!DsaPointerIsValid(ctl->snapshot_root))
			ereport(ERROR, (errmsg("pg_yaap worker has no serialized leader snapshot")));
		Snapshot worker_snapshot = RestoreSnapshot(
			static_cast<char *>(dsa_get_address(dsa, ctl->snapshot_root)));
		PGPROC *leader_proc = BackendPidGetProc(ctl->leader_pid);
		if (leader_proc == nullptr)
			ereport(ERROR, (errmsg("pg_yaap worker could not find leader process for snapshot restore")));
		RestoreTransactionSnapshot(worker_snapshot, leader_proc);
		PushActiveSnapshot(worker_snapshot);
		InvalidateSystemCaches();

		DsmTaskQueue *queue = DsmTaskQueue::AttachInPlace(queue_buf);
		SharedFileSetAttach(&ctl->spill_fileset, seg);

		int32_t worker_index = -1;
		/* TODO(Step 11c): finalize worker_index handoff via bgw_extra layout. */
		std::memcpy(&worker_index, MyBgworkerEntry->bgw_extra, sizeof(int32_t));
		Assert(worker_index >= 0);

		MemoryContext worker_mcxt = AllocSetContextCreate(TopMemoryContext,
		                                                 "pg_yaap worker",
		                                                 ALLOCSET_DEFAULT_SIZES);
		MemoryContextSwitchTo(worker_mcxt);
		if (wait_event_extension == 0)
			wait_event_extension = WaitEventExtensionNew("pg_yaap worker");

		WorkerTaskRuntime rt;
		rt.exec_ctx = ExecCtx{worker_mcxt, dsa, worker_index, ctl, INVALID_EVENT_ID};
		rt.control = ctl;
		PipelineProfileRegisterProcess(ctl, dsa, worker_index, MyProcPid);
		PipelineDsmLookup<Pipeline> lookup(worker_mcxt);

		PgVector<std::unique_ptr<Pipeline>> owned_pipelines{
			PgMemoryContextAllocator<std::unique_ptr<Pipeline>>(worker_mcxt)};
		WorkerReconstructPipelines(ctl, rt.exec_ctx, owned_pipelines);
		for (auto &pipeline : owned_pipelines)
			lookup.Register(pipeline->id, pipeline.get());

		TaskScheduler scheduler(rt.exec_ctx, *queue, lookup);

		/* A ready worker has a PGPROC and can resolve every pipeline. */
		auto *ready_array = static_cast<pg_atomic_uint32 *>(
			shm_toc_lookup(toc, PIPELINE_DSM_KEY_WORKER_READY, false));
		Assert(worker_index < ctl->num_workers);
		pg_atomic_write_membarrier_u32(&ready_array[worker_index], 1);
		SetLatch(&leader_proc->procLatch);

		for (;;)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
			if (WorkerShutdownRequested(ctl))
				break;

			const bool advanced = scheduler.Advance();
			TaskDescriptor desc;
			if (!queue->TryPop(&desc))
			{
				/* If the bounded queue was full during the first Advance() and
				 * just drained before TryPop(), publish pending cursor tasks
				 * immediately instead of waiting for the Latch timeout. */
				if (advanced || scheduler.Advance())
					continue;
				instr_time wait_start;
				if (pg_atomic_read_u32(&ctl->profile_enabled) != 0)
					INSTR_TIME_SET_CURRENT(wait_start);
				int rc = WaitLatch(MyLatch,
				                   WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
				                   1000,
				                   wait_event_extension);
				if (pg_atomic_read_u32(&ctl->profile_enabled) != 0)
				{
					instr_time wait_end;
					INSTR_TIME_SET_CURRENT(wait_end);
					instr_time elapsed = wait_end;
					INSTR_TIME_SUBTRACT(elapsed, wait_start);
					PipelineProfileAddElapsed(ctl,
									  dsa,
									  worker_index,
									  0,
									  PipelineProfileStage::WORKER_WAIT_TASK,
									  elapsed);
				}
				if (rc & WL_POSTMASTER_DEATH)
					break;
				if (WorkerShutdownRequested(ctl))
					break;
				continue;
			}
			if (WorkerShutdownRequested(ctl))
				break;

			Pipeline *pipeline = lookup.Resolve(desc.pipeline_id);
			if (pipeline == nullptr)
			{
				ReportWorkerError(ctl, "pg_yaap worker %d: unknown pipeline_id %u",
					worker_index, desc.pipeline_id);
				break;
			}

			std::unique_ptr<Task> task;
			switch (static_cast<TaskKind>(desc.kind))
			{
				case TaskKind::RUN:
					task = std::make_unique<PipelineRunTask>(desc.event_id,
					                                        pipeline,
					                                        &rt,
					                                        desc.task_index);
					break;
				case TaskKind::COMBINE:
					task = std::make_unique<PipelineCombineTask>(desc.event_id,
					                                            pipeline,
					                                            &rt,
					                                            desc.task_index,
					                                            desc.partition_id);
					break;
				case TaskKind::FINALIZE:
					task = std::make_unique<PipelineFinalizeTask>(desc.event_id,
						pipeline, &rt, desc.task_index, desc.partition_id);
					break;
			}

			PG_TRY();
			{
				if (task->Execute() != TaskExecutionResult::TASK_FINISHED)
					ereport(ERROR, (errmsg("pg_yaap: whole pipeline task did not finish")));
				CHECK_FOR_INTERRUPTS();
				if (!WorkerShutdownRequested(ctl))
					scheduler.CompleteTask(desc.event_id);
			}
			PG_CATCH();
			{
				ErrorData *edata = CopyErrorData();
				PipelineTimelineAbortProcess(ctl, dsa, worker_index,
					edata->sqlerrcode == ERRCODE_QUERY_CANCELED);
				ReportWorkerError(ctl,
					"worker=%d kind=%u pipeline=%u partition=%u: %s%s%s",
					worker_index, desc.kind, desc.pipeline_id, desc.partition_id,
					edata->message ? edata->message : "(no message)",
					edata->detail ? " | " : "",
					edata->detail ? edata->detail : "");
				FreeErrorData(edata);
				pg_atomic_write_u32(&ctl->shutdown_requested, 1u);
				PGPROC *leader = BackendPidGetProc(ctl->leader_pid);
				if (leader != nullptr)
					SetLatch(&leader->procLatch);
				PG_RE_THROW();
			}
			PG_END_TRY();
		}

		rt.per_pipeline.clear();
		owned_pipelines.clear();
		dsa_detach(dsa);
		dsm_detach(seg);
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		ErrorData *edata = CopyErrorData();
		ReportWorkerFailure(ctl, edata->message);
		FreeErrorData(edata);
		FlushErrorState();
		/* Drain the YAAP registry while its ResourceOwner is still alive.
		 * Abort frees registered contexts without removing YAAP pointers;
		 * letting the exit callback see them would double-release LLVM. */
		pg_yaap_proc_exit_release_jit_contexts(1, (Datum) 0);
		AbortCurrentTransaction();
		proc_exit(1);
	}
	PG_END_TRY();
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
