extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include "buffer_manager/yaap_buffer_shared.hpp"

namespace pg_yaap {
namespace buffer {

namespace {

static void
UpdatePeak(YaapBufferSharedState *state, uint64_t resident)
{
	uint64_t peak = pg_atomic_read_u64(&state->peak_resident_bytes);
	while (resident > peak &&
		   !pg_atomic_compare_exchange_u64(&state->peak_resident_bytes, &peak, resident))
		;
}

}  /* namespace */

void
YaapBufferSharedStateInit(YaapBufferSharedState *state,
                          uint64_t hard_limit,
                          uint64_t soft_limit)
{
	if (state == nullptr || hard_limit == 0 || soft_limit > hard_limit)
		ereport(ERROR, (errmsg("pg_yaap: invalid shared buffer limits")));
	state->hard_limit = hard_limit;
	state->soft_limit = soft_limit;
	pg_atomic_init_u64(&state->resident_bytes, 0);
	pg_atomic_init_u64(&state->committed_bytes, 0);
	pg_atomic_init_u64(&state->spill_bytes, 0);
	pg_atomic_init_u64(&state->peak_resident_bytes, 0);
	pg_atomic_init_u64(&state->allocation_count, 0);
	pg_atomic_init_u64(&state->pressure_epoch, 0);
}

bool
YaapBufferTryChargeExternalBytes(YaapBufferSharedState *state, uint64_t bytes)
{
	if (state == nullptr)
		return false;
	if (bytes > state->hard_limit)
		return false;

	uint64_t resident = pg_atomic_read_u64(&state->resident_bytes);
	for (;;)
	{
		if (resident > state->hard_limit - bytes)
			return false;
		uint64_t expected = resident;
		if (pg_atomic_compare_exchange_u64(&state->resident_bytes,
											   &expected,
											   resident + bytes))
		{
			UpdatePeak(state, resident + bytes);
			if (resident < state->soft_limit && resident + bytes >= state->soft_limit)
				pg_atomic_fetch_add_u64(&state->pressure_epoch, 1);
			pg_atomic_fetch_add_u64(&state->allocation_count, 1);
			return true;
		}
		resident = expected;
	}
}

void
YaapBufferChargeExternalBytes(YaapBufferSharedState *state,
                              uint64_t bytes,
                              const char *what)
{
	if (!YaapBufferTryChargeExternalBytes(state, bytes))
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("pg_yaap: shared buffer limit exceeded while allocating %s",
						 what != nullptr ? what : "storage"),
				 errdetail("requested %llu bytes with %llu bytes resident and a %llu-byte limit",
						   (unsigned long long) bytes,
						   (unsigned long long) YaapBufferResidentBytes(state),
						   (unsigned long long) (state != nullptr ? state->hard_limit : 0))));
}

void
YaapBufferReleaseExternalBytes(YaapBufferSharedState *state, uint64_t bytes)
{
	if (state == nullptr || bytes == 0)
		return;
	uint64_t resident = pg_atomic_read_u64(&state->resident_bytes);
	for (;;)
	{
		if (resident < bytes)
			ereport(ERROR, (errmsg("pg_yaap: shared buffer accounting underflow")));
		uint64_t expected = resident;
		if (pg_atomic_compare_exchange_u64(&state->resident_bytes,
											   &expected,
											   resident - bytes))
			return;
		resident = expected;
	}
}

void
YaapBufferRecordSpillBytes(YaapBufferSharedState *state, uint64_t bytes)
{
	if (state != nullptr && bytes > 0)
		pg_atomic_fetch_add_u64(&state->spill_bytes, bytes);
}

uint64_t
YaapBufferResidentBytes(const YaapBufferSharedState *state)
{
	return state == nullptr ? 0 :
		pg_atomic_read_u64(const_cast<pg_atomic_uint64 *>(&state->resident_bytes));
}

bool
YaapBufferOverSoftLimit(const YaapBufferSharedState *state)
{
	return state != nullptr && YaapBufferResidentBytes(state) >= state->soft_limit;
}

uint64_t
YaapBufferPressureEpoch(const YaapBufferSharedState *state)
{
	return state == nullptr ? 0 :
		pg_atomic_read_u64(const_cast<pg_atomic_uint64 *>(&state->pressure_epoch));
}

}  /* namespace buffer */
}  /* namespace pg_yaap */
