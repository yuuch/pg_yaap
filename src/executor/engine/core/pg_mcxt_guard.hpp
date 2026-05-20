#pragma once

/*
 * core/pg_mcxt_guard.hpp — RAII guards backed by MemoryContext reset
 * callbacks (B2.2).
 *
 * Why both dtor AND callback: a C function reachable from inside the C++
 * kernel (via Layer C) can ereport(ERROR), which longjmp's past every
 * C++ destructor on the stack. To survive that, every owning resource
 * (mutex, fd, refcount) must ALSO be released by a
 * MemoryContextRegisterResetCallback fired during context teardown.
 *
 * Symmetric release contract: the dtor MUST
 * MemoryContextUnregisterResetCallback before destruction, otherwise the
 * later context reset will call OnContextReset(this) on freed memory
 * (use-after-free). Release itself is guarded by `released_` so the dtor
 * path and callback path are individually idempotent on the resource.
 */

#include "core/types.hpp"

#include <pthread.h>
#include <utility>

namespace pg_yaap {

class PgMcxtMutexGuard {
public:
	PgMcxtMutexGuard(pthread_mutex_t *mu, MemoryContext ctx)
	    : mu_(mu), ctx_(ctx), released_(false)
	{
		pthread_mutex_lock(mu_);

		cb_.func = &OnContextReset;
		cb_.arg  = this;
		MemoryContextRegisterResetCallback(ctx_, &cb_);
	}

	~PgMcxtMutexGuard()
	{
		MemoryContextUnregisterResetCallback(ctx_, &cb_);
		Release();
	}

	PgMcxtMutexGuard(const PgMcxtMutexGuard &)            = delete;
	PgMcxtMutexGuard &operator=(const PgMcxtMutexGuard &) = delete;
	PgMcxtMutexGuard(PgMcxtMutexGuard &&)                 = delete;
	PgMcxtMutexGuard &operator=(PgMcxtMutexGuard &&)      = delete;

private:
	void Release() noexcept
	{
		if (!released_)
		{
			released_ = true;
			pthread_mutex_unlock(mu_);
		}
	}

	static void OnContextReset(void *arg) noexcept
	{
		static_cast<PgMcxtMutexGuard *>(arg)->Release();
	}

	pthread_mutex_t      *mu_;
	MemoryContext         ctx_;
	bool                  released_;
	MemoryContextCallback cb_{};
};

/*
 * Generic RAII guard that runs a user-supplied lambda on dtor OR on
 * MemoryContext reset (whichever fires first). Use for: dsa_detach,
 * fd close, refcount decrement — anything other than a mutex (mutex
 * has its own typed guard above).
 */
template <typename Releaser>
class PgMcxtCallbackGuard {
public:
	PgMcxtCallbackGuard(Releaser releaser, MemoryContext ctx)
	    : releaser_(std::move(releaser)),
	      ctx_(ctx),
	      released_(false)
	{
		cb_.func = &OnContextReset;
		cb_.arg  = this;
		MemoryContextRegisterResetCallback(ctx_, &cb_);
	}

	~PgMcxtCallbackGuard()
	{
		MemoryContextUnregisterResetCallback(ctx_, &cb_);
		Fire();
	}

	PgMcxtCallbackGuard(const PgMcxtCallbackGuard &)            = delete;
	PgMcxtCallbackGuard &operator=(const PgMcxtCallbackGuard &) = delete;
	PgMcxtCallbackGuard(PgMcxtCallbackGuard &&)                 = delete;
	PgMcxtCallbackGuard &operator=(PgMcxtCallbackGuard &&)      = delete;

private:
	void Fire() noexcept
	{
		if (!released_)
		{
			released_ = true;
			releaser_();
		}
	}

	static void OnContextReset(void *arg) noexcept
	{
		static_cast<PgMcxtCallbackGuard *>(arg)->Fire();
	}

	Releaser              releaser_;
	MemoryContext         ctx_;
	bool                  released_;
	MemoryContextCallback cb_{};
};

template <typename Releaser>
inline PgMcxtCallbackGuard<Releaser>
MakePgMcxtCallbackGuard(Releaser r, MemoryContext ctx)
{
	return PgMcxtCallbackGuard<Releaser>(std::move(r), ctx);
}

}
