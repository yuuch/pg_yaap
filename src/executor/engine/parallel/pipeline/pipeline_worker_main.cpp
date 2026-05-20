/*
 * pipeline_worker_main.cpp
 */

extern "C" {
#include "postgres.h"
#include "access/xact.h"
#include "miscadmin.h"
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
#include "parallel/pipeline/task.hpp"
#include "parallel/pipeline/types.hpp"

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
	if (pg_atomic_read_u32(&ctl->worker_error) != 0)
		return true;
	return ctl->leader_pid != 0 && BackendPidGetProc(ctl->leader_pid) == nullptr;
}

static void
ReportWorkerStartupError(PipelineSharedControl *ctl, const char *message)
{
	if (ctl == nullptr)
		return;

	uint32 expected = 0;
	if (pg_atomic_compare_exchange_u32(&ctl->worker_error, &expected, 1u))
	{
		snprintf(ctl->worker_error_msg,
		         PIPELINE_WORKER_ERROR_MSG_LEN,
		         "%s",
		         message != nullptr ? message : "(no message)");
	}
	pg_atomic_write_u32(&ctl->shutdown_requested, 1u);
	PGPROC *leader = BackendPidGetProc(ctl->leader_pid);
	if (leader != nullptr)
		SetLatch(&leader->procLatch);
}

extern "C" PGDLLEXPORT void
pg_yaap_pipeline_worker_main(Datum main_arg)
{
	static uint32 wait_event_extension = 0;

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
		PushActiveSnapshot(GetTransactionSnapshot());

		dsa_area *dsa = dsa_attach_in_place(dsa_buf, seg);
		DsmTaskQueue *queue = DsmTaskQueue::AttachInPlace(queue_buf);

		int32_t worker_index = -1;
		/* TODO(Step 11c): finalize worker_index handoff via bgw_extra layout. */
		std::memcpy(&worker_index, MyBgworkerEntry->bgw_extra, sizeof(int32_t));
		Assert(worker_index >= 0);

		/*
		 * Publish startup-ready signal for the leader (Oracle race-fix). PGPROC is
		 * now in ProcArray because BackgroundWorkerInitializeConnectionByOid has
		 * returned, which internally drove InitPostgres -> InitProcessPhase2.
		 * BackendPidGetProc(MyProcPid) is therefore safe from this point forward.
		 */
		{
			auto *ready_array = static_cast<pg_atomic_uint32 *>(
				shm_toc_lookup(toc, PIPELINE_DSM_KEY_WORKER_READY, false));
			Assert(worker_index < ctl->num_workers);
			pg_atomic_write_u32(&ready_array[worker_index], 1);
			PGPROC *leader_proc = BackendPidGetProc(ctl->leader_pid);
			if (leader_proc != nullptr)
				SetLatch(&leader_proc->procLatch);
		}

		MemoryContext worker_mcxt = AllocSetContextCreate(TopMemoryContext,
		                                                 "pg_yaap worker",
		                                                 ALLOCSET_DEFAULT_SIZES);
		MemoryContextSwitchTo(worker_mcxt);
		if (wait_event_extension == 0)
			wait_event_extension = WaitEventExtensionNew("pg_yaap worker");

		WorkerTaskRuntime rt;
		rt.exec_ctx = ExecCtx{worker_mcxt, dsa, worker_index, ctl, INVALID_EVENT_ID};
		rt.control = ctl;
		rt.event_shm = static_cast<EventShmState *>(dsa_get_address(dsa, ctl->event_states_root));
		PipelineProfileRegisterProcess(ctl, dsa, worker_index, MyProcPid);
		PipelineDsmLookup<Pipeline> lookup(worker_mcxt);
		rt.pipelines = &lookup;
		rt.leader_qd = nullptr;
		rt.final_output = nullptr;

		PgVector<std::unique_ptr<Pipeline>> owned_pipelines{
			PgMemoryContextAllocator<std::unique_ptr<Pipeline>>(worker_mcxt)};
		WorkerReconstructPipelines(ctl, rt.exec_ctx, owned_pipelines);
		for (auto &pipeline : owned_pipelines)
			lookup.Register(pipeline->id, pipeline.get());

		for (;;)
		{
			if (WorkerShutdownRequested(ctl))
				break;

			TaskDescriptor desc;
			if (!queue->TryPopForWorker(worker_index, &desc))
			{
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
				ResetLatch(MyLatch);
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
				uint32 expected = 0;
				if (pg_atomic_compare_exchange_u32(&ctl->worker_error, &expected, 1u))
				{
					snprintf(ctl->worker_error_msg, PIPELINE_WORKER_ERROR_MSG_LEN,
					         "pg_yaap worker %d: unknown pipeline_id %u",
					         worker_index, desc.pipeline_id);
				}
				break;
			}

			EventShmState *event_shm = &rt.event_shm[desc.event_id];

			std::unique_ptr<Task> task;
			switch (static_cast<TaskKind>(desc.kind))
			{
				case TaskKind::RUN:
					task = std::make_unique<PipelineRunTask>(desc.event_id,
					                                        pipeline,
					                                        &rt,
					                                        desc.worker_index);
					break;
				case TaskKind::COMBINE:
					task = std::make_unique<PipelineCombineTask>(desc.event_id,
					                                            pipeline,
					                                            &rt,
					                                            desc.worker_index,
					                                            desc.partition_id);
					break;
				case TaskKind::FINALIZE:
				{
					uint32 expected = 0;
					if (pg_atomic_compare_exchange_u32(&ctl->worker_error, &expected, 1u))
					{
						snprintf(ctl->worker_error_msg, PIPELINE_WORKER_ERROR_MSG_LEN,
						         "pg_yaap worker %d: received FINALIZE descriptor (forbidden)",
						         worker_index);
					}
					goto cleanup_and_exit;
				}
			}

			PG_TRY();
			{
				TaskExecutionResult tres = task->Execute();
				if (tres == TaskExecutionResult::TASK_NOT_FINISHED)
				{
					bool re_pushed = queue->TryPush(desc);
					if (!re_pushed)
						ereport(ERROR,
						        (errmsg("pg_yaap worker %d: TryPush re-enqueue failed",
						                worker_index)));
					continue;
				}
				uint32 remaining = pg_atomic_sub_fetch_u32(&event_shm->tasks_remaining, 1);
				if (remaining == 0)
				{
					PGPROC *leader = BackendPidGetProc(ctl->leader_pid);
					if (leader != nullptr)
						SetLatch(&leader->procLatch);
				}
			}
			PG_CATCH();
			{
				ErrorData *edata = CopyErrorData();
				uint32 expected = 0;
				if (pg_atomic_compare_exchange_u32(&ctl->worker_error, &expected, 1u))
				{
					snprintf(ctl->worker_error_msg, PIPELINE_WORKER_ERROR_MSG_LEN,
					         "%s", edata->message ? edata->message : "(no message)");
				}
				FreeErrorData(edata);
				pg_atomic_write_u32(&ctl->shutdown_requested, 1u);
				PGPROC *leader = BackendPidGetProc(ctl->leader_pid);
				if (leader != nullptr)
					SetLatch(&leader->procLatch);
				PG_RE_THROW();
			}
			PG_END_TRY();
		}

cleanup_and_exit:
		rt.per_pipeline.clear();
		owned_pipelines.clear();
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		ErrorData *edata = CopyErrorData();
		ReportWorkerStartupError(ctl, edata->message);
		FreeErrorData(edata);
		FlushErrorState();
		proc_exit(1);
	}
	PG_END_TRY();
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
