#pragma once

/*
 * core/status.hpp — return-code error model for pg_yaap C++ kernel (B2.1).
 *
 * pg_yaap is built with -fno-exceptions. The C++ kernel under
 * parallel/pipeline/ propagates failures by value via Status / expected<T>.
 * The ONLY PG_TRY/PG_CATCH boundary lives at the outer C/C++ edge in
 * src/bridge/execute.cpp; PG C calls that may ereport() must be wrapped at
 * Layer C (see bridge/AGENTS.md) so longjmp never crosses C++ frames.
 *
 * Status::message_ is palloc'd in CurrentMemoryContext at construction.
 * Lifetime is bounded by the per-query MemoryContext (state->context).
 * Callers MUST NOT carry a Status across query teardown; copy the message
 * into a longer-lived context first if needed.
 */

#include "core/types.hpp"

#include <utility>
#include <variant>

namespace pg_yaap {

enum class StatusCode : uint8_t {
	OK = 0,
	UNSUPPORTED_PLAN_SHAPE,
	PG_ERROR_CAUGHT,
	INVARIANT_VIOLATED,
	OUT_OF_MEMORY,
	INTERNAL,
};

class Status {
public:
	static Status OK() noexcept { return Status(); }

	Status(StatusCode code, const char *msg, int pg_errcode = 0)
	    : code_(code),
	      pg_errcode_(pg_errcode),
	      message_(msg != nullptr ? pstrdup(msg) : nullptr)
	{
	}

	Status(const Status &) = default;
	Status(Status &&)      = default;
	Status &operator=(const Status &) = default;
	Status &operator=(Status &&)      = default;

	bool        ok()         const noexcept { return code_ == StatusCode::OK; }
	StatusCode  code()       const noexcept { return code_; }
	int         pg_errcode() const noexcept { return pg_errcode_; }
	const char *message()    const noexcept { return message_ != nullptr ? message_ : ""; }

private:
	Status() noexcept : code_(StatusCode::OK), pg_errcode_(0), message_(nullptr) {}

	StatusCode  code_;
	int         pg_errcode_;
	const char *message_;
};

/*
 * expected<T>: variant<Status, T>. Reading value() on a failed expected is
 * UB — callers MUST check ok() first.
 */
template <typename T>
class expected {
public:
	expected(T value)        : v_(std::in_place_index<1>, std::move(value)) {}
	expected(Status status)  : v_(std::in_place_index<0>, std::move(status)) {}

	bool          ok()      const noexcept { return v_.index() == 1; }
	const Status &status()  const          { return std::get<0>(v_); }
	const T      &value()   const          { return std::get<1>(v_); }
	T            &value()                  { return std::get<1>(v_); }
	T             release()                { return std::move(std::get<1>(v_)); }

private:
	std::variant<Status, T> v_;
};

/*
 * Monadic early-return for chained Status-returning calls:
 *   Status DoStuff() {
 *       PG_YAAP_TRY(StepA());
 *       PG_YAAP_TRY(StepB());
 *       return Status::OK();
 *   }
 */
#define PG_YAAP_TRY(expr)                                       \
	do {                                                          \
		::pg_yaap::Status _pgv_st = (expr);                     \
		if (!_pgv_st.ok()) return _pgv_st;                        \
	} while (0)

}
