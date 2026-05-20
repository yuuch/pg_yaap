#pragma once

#include "core/types.hpp"
#include "core/robin_hood_pg_adapter.hpp"

namespace pg_yaap
{

class PgMemoryContextObject
{
public:
	static void *operator new(std::size_t size)
	{
		return MemoryContextAlloc(CurrentMemoryContext, size);
	}

	static void operator delete(void *ptr) noexcept
	{
		if (ptr != nullptr)
			pfree(ptr);
	}

	static void operator delete(void *ptr, std::size_t) noexcept
	{
		if (ptr != nullptr)
			pfree(ptr);
	}
};

template <typename T>
class PgMemoryContextAllocator
{
public:
	using value_type = T;

	PgMemoryContextAllocator() noexcept : context_(CurrentMemoryContext) {}
	explicit PgMemoryContextAllocator(MemoryContext context) noexcept
		: context_(context != nullptr ? context : CurrentMemoryContext) {}

	template <typename U>
	PgMemoryContextAllocator(const PgMemoryContextAllocator<U> &other) noexcept
		: context_(other.context()) {}

	T *allocate(std::size_t n)
	{
		if (n > (std::numeric_limits<std::size_t>::max() / sizeof(T)))
			elog(ERROR, "pg_yaap allocator size overflow");
		return static_cast<T *>(MemoryContextAlloc(context_, n * sizeof(T)));
	}

	void deallocate(T *ptr, std::size_t) noexcept
	{
		if (ptr != nullptr)
			pfree(ptr);
	}

	MemoryContext context() const noexcept { return context_; }

	template <typename U>
	bool operator==(const PgMemoryContextAllocator<U> &other) const noexcept
	{
		return context_ == other.context();
	}

	template <typename U>
	bool operator!=(const PgMemoryContextAllocator<U> &other) const noexcept
	{
		return !(*this == other);
	}

private:
	template <typename>
	friend class PgMemoryContextAllocator;

	MemoryContext context_;
};

template <typename T>
using VolVecVector = std::vector<T, PgMemoryContextAllocator<T>>;

/*
 * MemoryContext-aware STL aliases (M-FRAME-MIN A1).
 *
 * Use these in place of bare std::vector / std::unordered_map / std::make_shared
 * anywhere the container's lifetime is bounded by a per-query MemoryContext.
 *
 * Rationale: ereport(ERROR) longjmps over C++ destructors. Bare std::vector
 * holds its element storage in glibc/libc malloc heap, which the MemoryContext
 * teardown does NOT reclaim — that is a true leak across every fallback path.
 * PgMemoryContextAllocator routes allocate()/deallocate() through palloc/pfree
 * tied to CurrentMemoryContext (or an explicit ctx), so the element storage
 * dies with the context.
 *
 * shared_ptr requires a single combined allocation for control block + T to
 * fully participate in the context — use AllocatePgShared, not make_shared.
 *
 * VolVecVector remains as a compatibility alias for older call sites.
 */
template <typename T>
using PgVector = std::vector<T, PgMemoryContextAllocator<T>>;

template <typename Key,
          typename Value,
          typename Hash = std::hash<Key>,
          typename Eq   = std::equal_to<Key>>
using PgUnorderedMap = std::unordered_map<
    Key, Value, Hash, Eq,
    PgMemoryContextAllocator<std::pair<const Key, Value>>>;

template <typename T, typename... Args>
inline std::shared_ptr<T> AllocatePgShared(Args &&...args)
{
	return std::allocate_shared<T>(PgMemoryContextAllocator<T>(),
	                               std::forward<Args>(args)...);
}

template <typename Key, typename Value, typename Hash = std::hash<Key>>
using VolVecHashMap = RobinHoodPgMap<Key, Value, Hash>;

/* === Parallel Hash Join: Radix Partition + Linear Probe + Bloom Filter === */
static constexpr int VOLVEC_RADIX_BITS = 8;
static constexpr int VOLVEC_RADIX_FANOUT = (1 << VOLVEC_RADIX_BITS);  /* 256 partitions */
static constexpr double VOLVEC_HT_LOAD_FACTOR = 0.75;

} // namespace pg_yaap

