// Exercise the real shared-memory error publication protocol across fork().
#include <cassert>
#include <cstring>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#include "parallel/pipeline/worker_error.hpp"

using namespace pg_yaap::pipeline;

int main()
{
    auto *control = static_cast<PipelineSharedControl *>(mmap(nullptr,
        sizeof(PipelineSharedControl), PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANON, -1, 0));
    assert(control != MAP_FAILED);
    std::memset(control, 0, sizeof(*control));
    pg_atomic_init_u32(&control->worker_error, 0);
    int ready[2], resume[2];
    assert(pipe(ready) == 0 && pipe(resume) == 0);
    pid_t writer = fork();
    assert(writer >= 0);
    if (writer == 0)
    {
        assert(TryBeginWorkerError(control));
        std::strcpy(control->worker_error_msg, "partial");
        char token = 'x';
        assert(write(ready[1], &token, 1) == 1);
        assert(read(resume[0], &token, 1) == 1);
        std::strcpy(control->worker_error_msg, "complete diagnostic");
        FinishWorkerError(control);
        _exit(0);
    }
    char token;
    assert(read(ready[0], &token, 1) == 1);
    assert(WorkerErrorPending(control));
    assert(!WorkerErrorReady(control));
    assert(WorkerErrorMessage(control) == nullptr);
    // A competing reporter must not overwrite even an unpublished first error.
    assert(!TryBeginWorkerError(control));
    assert(write(resume[1], &token, 1) == 1);
    int status;
    assert(waitpid(writer, &status, 0) == writer);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    assert(WorkerErrorReady(control));
    assert(std::strcmp(WorkerErrorMessage(control), "complete diagnostic") == 0);
    assert(!TryBeginWorkerError(control));
    assert(std::strcmp(WorkerErrorMessage(control), "complete diagnostic") == 0);
    close(ready[0]); close(ready[1]); close(resume[0]); close(resume[1]);
    assert(munmap(control, sizeof(*control)) == 0);
}
