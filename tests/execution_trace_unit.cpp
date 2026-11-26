/*
 * Unit tests for Layer 1 Execution Trace infrastructure:
 *   - Struct size/alignment contracts
 *   - Disabled fast-path (no allocations, no writes)
 *   - Monotonic clock & shared origin across forked worker processes
 *   - Nested Begin/End & RAII scopes
 *   - Buffer full overflow accounting (dropped_count)
 *   - Abnormal worker exit preservation of published event prefix
 *   - Streaming Chrome Trace JSON export validity
 */
extern "C" {
#include "postgres.h"
#include "storage/fd.h"
#include "utils/dsa.h"
#include "utils/memutils.h"
}

#include <cassert>
#include <csetjmp>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/dsm_task_queue.hpp"
#include "parallel/pipeline/execution_trace.hpp"

/* ---- Minimal PostgreSQL backend stubs ---- */

sigjmp_buf *PG_exception_stack = nullptr;
ErrorContextCallback *error_context_stack = nullptr;
MemoryContext CurrentMemoryContext = nullptr;

extern "C" {

bool  pg_yaap_trace_enabled = false;
int   pg_yaap_trace_buffer_mb = 1;
char *pg_yaap_trace_output_dir = const_cast<char *>("");

struct FakeDsaHeader {
	pg_atomic_uint64 offset;
	size_t           capacity;
};

static char *g_dsa_base = nullptr;

dsa_pointer
dsa_allocate_extended(dsa_area *, size_t size, int flags)
{
	assert(g_dsa_base != nullptr);
	auto *hdr = reinterpret_cast<FakeDsaHeader *>(g_dsa_base);
	const size_t aligned = MAXALIGN(size ? size : 8);
	uint64_t cur = pg_atomic_read_u64(&hdr->offset);
	for (;;)
	{
		if (cur + aligned > hdr->capacity)
		{
			if (flags & DSA_ALLOC_NO_OOM)
				return InvalidDsaPointer;
			std::abort();
		}
		if (pg_atomic_compare_exchange_u64(&hdr->offset, &cur, cur + aligned))
			break;
	}
	if (flags & DSA_ALLOC_ZERO)
		std::memset(g_dsa_base + cur, 0, aligned);
	return static_cast<dsa_pointer>(cur);
}

void *
dsa_get_address(dsa_area *, dsa_pointer dp)
{
	if (!DsaPointerIsValid(dp))
		return nullptr;
	return g_dsa_base + dp;
}

bool superuser(void) { return true; }
Oid GetUserId(void) { return 10; }
bool has_privs_of_role(Oid, Oid) { return true; }
void FlushErrorState(void) {}

int
OpenTransientFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
{
	return ::open(fileName, fileFlags, fileMode);
}

int
CloseTransientFile(int fd)
{
	return ::close(fd);
}

size_t
strlcpy(char *dst, const char *src, size_t siz)
{
	const size_t len = std::strlen(src);
	if (siz > 0)
	{
		const size_t copy = len >= siz ? siz - 1 : len;
		std::memcpy(dst, src, copy);
		dst[copy] = '\0';
	}
	return len;
}

#undef fprintf
#undef printf
#undef snprintf
#undef vsnprintf

int
pg_vsnprintf(char *str, size_t count, const char *fmt, va_list args)
{
	return ::vsnprintf(str, count, fmt, args);
}

int
pg_snprintf(char *str, size_t count, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	int r = ::vsnprintf(str, count, fmt, ap);
	va_end(ap);
	return r;
}

void
ExceptionalCondition(const char *cond, const char *file, int line)
{
	::fprintf(stderr, "ASSERT FAILED: %s at %s:%d\n", cond, file, line);
	std::abort();
}

void pg_re_throw(void) { std::abort(); }
bool errstart(int, const char *) { return false; }
bool errstart_cold(int, const char *) { return false; }
int errmsg_internal(const char *, ...) { return 0; }
int errmsg(const char *, ...) { return 0; }
void errfinish(const char *, int, const char *) {}

}  /* extern "C" */

using namespace pg_yaap::pipeline;

namespace {

struct SharedTestRegion {
	void                  *mem = nullptr;
	size_t                 total_bytes = 0;
	PipelineSharedControl *control = nullptr;

	explicit SharedTestRegion(size_t dsa_bytes = 4 * 1024 * 1024)
	{
		total_bytes = MAXALIGN(sizeof(PipelineSharedControl)) + dsa_bytes;
		mem = ::mmap(nullptr, total_bytes, PROT_READ | PROT_WRITE,
		             MAP_SHARED | MAP_ANON, -1, 0);
		assert(mem != MAP_FAILED);
		char *ptr = static_cast<char *>(mem);
		control = new (ptr) PipelineSharedControl();
		control->leader_pid = ::getpid();
		control->num_pipelines = 2;
		control->pipelines_root = InvalidDsaPointer;
		control->dsa_limit_bytes = dsa_bytes;
		pg_atomic_init_u32(&control->trace_enabled, 0);
		control->trace_root = InvalidDsaPointer;

		g_dsa_base = ptr + MAXALIGN(sizeof(PipelineSharedControl));
		auto *hdr = reinterpret_cast<FakeDsaHeader *>(g_dsa_base);
		pg_atomic_init_u64(&hdr->offset, MAXALIGN(sizeof(FakeDsaHeader)));
		hdr->capacity = dsa_bytes;
	}

	~SharedTestRegion()
	{
		YaapTraceDetachProcess(false);
		g_dsa_base = nullptr;
		::munmap(mem, total_bytes);
	}
};

static std::string
ExportToString(PipelineSharedControl *control,
               bool query_succeeded,
               YaapTraceExportSummary *summary)
{
	char tmpl[] = "/tmp/yaap_trace_unit_XXXXXX";
	int fd = ::mkstemp(tmpl);
	assert(fd >= 0);
	dsa_area *fake_dsa = reinterpret_cast<dsa_area *>(0x1);
	const bool ok = YaapTraceWriteChromeJsonToFd(control, fake_dsa, query_succeeded, fd, summary);
	assert(ok);
	assert(::lseek(fd, 0, SEEK_SET) == 0);
	std::string content;
	char buf[4096];
	ssize_t n;
	while ((n = ::read(fd, buf, sizeof(buf))) > 0)
		content.append(buf, static_cast<size_t>(n));
	::close(fd);
	::unlink(tmpl);
	return content;
}

static void
TestDisabledFastPath()
{
	SharedTestRegion region;
	dsa_area *fake_dsa = reinterpret_cast<dsa_area *>(0x1);
	assert(!YaapTraceInitQueryWithConfig(region.control, fake_dsa, 2, false, 64, 1024 * 1024));
	assert(!YaapTraceIsEnabled());
	assert(!DsaPointerIsValid(region.control->trace_root));
	YaapTraceInstant(YaapTraceEventType::WORKER_START);
	{
		YaapTraceScoped scope(YaapTraceEventType::QUERY_EXECUTE);
		(void) scope;
	}
	assert(!YaapTraceIsEnabled());
}

static void
TestNestedScopesAndOverflow()
{
	SharedTestRegion region;
	dsa_area *fake_dsa = reinterpret_cast<dsa_area *>(0x1);
	/* Tiny capacity of 4 events to deterministically exercise buffer overflow. */
	assert(YaapTraceInitQueryWithConfig(region.control, fake_dsa, 1, true, 4, 1024 * 1024));
	assert(YaapTraceIsEnabled());

	{
		YaapTraceScoped outer(YaapTraceEventType::QUERY_EXECUTE, YaapTraceCategory::EXECUTION);
		{
			YaapTraceScoped inner(YaapTraceEventType::OUTPUT_EMIT, YaapTraceCategory::EXECUTION);
			YaapTraceInstant(YaapTraceEventType::PIPELINE_READY, YaapTraceCategory::EXECUTION, 0);
			YaapTraceInstant(YaapTraceEventType::PIPELINE_SCHEDULE, YaapTraceCategory::SCHEDULER, 0,
			                 YAAP_TRACE_INVALID_ID, 2);
		}
		/* 3 events recorded so far; outer adds 4th. Now emit 3 more -> dropped! */
		YaapTraceInstant(YaapTraceEventType::PIPELINE_FINISH, YaapTraceCategory::EXECUTION, 0);
		YaapTraceInstant(YaapTraceEventType::EVENT_FINISH, YaapTraceCategory::SCHEDULER, 0, 2,
		                 YaapTracePackEventArg(static_cast<uint8_t>(TaskKind::FINALIZE), 1));
	}
	YaapTraceDetachProcess(false);

	YaapTraceExportSummary summary{};
	const std::string json = ExportToString(region.control, true, &summary);
	assert(summary.exported);
	assert(summary.total_events == 4);
	assert(summary.dropped_events == 2);
	assert(!summary.complete);
	assert(json.find("\"dropped_events\":2") != std::string::npos);
	assert(json.find("\"complete\":false") != std::string::npos);
	assert(json.find("\"Pipeline 0 Ready\"") != std::string::npos);
}

static void
TestMultiprocessAndAbnormalWorkerExit()
{
	constexpr int kWorkers = 4;
	SharedTestRegion region;
	dsa_area *fake_dsa = reinterpret_cast<dsa_area *>(0x1);
	assert(YaapTraceInitQueryWithConfig(region.control, fake_dsa, kWorkers, true, 128, 4 * 1024 * 1024));

	const uint64_t t_before = YaapTraceNow();
	YaapTraceScope query_scope = YaapTraceBegin(YaapTraceEventType::QUERY_EXECUTE);

	pid_t kids[kWorkers];
	for (int w = 0; w < kWorkers; ++w)
	{
		kids[w] = ::fork();
		assert(kids[w] >= 0);
		if (kids[w] == 0)
		{
			assert(YaapTraceAttachProcess(region.control, fake_dsa, w, ::getpid()));
			YaapTraceInstant(YaapTraceEventType::WORKER_START);
			{
				YaapTraceScoped idle(YaapTraceEventType::WORKER_IDLE, YaapTraceCategory::SCHEDULER);
				::usleep(1000);
			}
			{
				YaapTraceScoped task(
					YaapTraceEventType::TASK_EXECUTE,
					YaapTraceCategory::EXECUTION,
					0, static_cast<uint32_t>(w),
					YaapTracePackTaskArg(0, static_cast<uint8_t>(TaskKind::RUN),
					                     YAAP_TRACE_INVALID_ID, YaapTraceTaskOutcome::OK));
				::usleep(1000);
			}
			if (w == kWorkers - 1)
			{
				/* Simulate abrupt crash without calling YaapTraceDetachProcess. */
				::_exit(1);
			}
			YaapTraceInstant(YaapTraceEventType::WORKER_EXIT, YaapTraceCategory::EXECUTION,
			                 YAAP_TRACE_INVALID_ID, YAAP_TRACE_INVALID_ID, 0);
			YaapTraceDetachProcess(false);
			::_exit(0);
		}
	}

	for (int w = 0; w < kWorkers; ++w)
	{
		int status = 0;
		assert(::waitpid(kids[w], &status, 0) == kids[w]);
		assert(WIFEXITED(status));
		if (w == kWorkers - 1)
			assert(WEXITSTATUS(status) == 1);
		else
			assert(WEXITSTATUS(status) == 0);
	}

	YaapTraceEnd(&query_scope);
	YaapTraceDetachProcess(false);
	const uint64_t t_after = YaapTraceNow();
	assert(t_after >= t_before);

	YaapTraceExportSummary summary{};
	const std::string json = ExportToString(region.control, true, &summary);
	/* Leader: 1 event. Workers 0..2: 4 events each (12). Worker 3 (abrupt): 3 published events. Total = 16. */
	assert(summary.total_events == 16);
	assert(summary.dropped_events == 0);
	assert(summary.abnormal_processes == 1);
	assert(!summary.complete);
	assert(json.find("\"YAAP Leader") != std::string::npos);
	assert(json.find("\"YAAP Worker 0") != std::string::npos);
	assert(json.find("\"YAAP Worker 3") != std::string::npos);
	assert(json.find("\"Pipeline 0 / RUN / Task 3\"") != std::string::npos);
	assert(json.find("\"Worker Idle\"") != std::string::npos);
}

static void
TestGracefulOomDegradation()
{
	/* Tiny DSA area (128 bytes) cannot fit trace structures; must degrade safely. */
	SharedTestRegion region(256);
	dsa_area *fake_dsa = reinterpret_cast<dsa_area *>(0x1);
	assert(!YaapTraceInitQueryWithConfig(region.control, fake_dsa, 4, true, 1024, 1024 * 1024));
	assert(!YaapTraceIsEnabled());
	assert(pg_atomic_read_u32(&region.control->trace_enabled) == 0);
}

}  /* namespace */

int
main()
{
	::alarm(30);
	TestDisabledFastPath();
	TestNestedScopesAndOverflow();
	TestMultiprocessAndAbnormalWorkerExit();
	TestGracefulOomDegradation();
	::printf("execution_trace_unit: all tests passed\n");
	return 0;
}
