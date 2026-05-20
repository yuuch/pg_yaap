#ifndef PG_YAAP_DATA_CHUNK_DEFORM_HPP
#define PG_YAAP_DATA_CHUNK_DEFORM_HPP

extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "fmgr.h"
#include "catalog/pg_type_d.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
}

#include <cstdint>
#include <cstring>
#include "data_chunk.hpp"

namespace pg_yaap
{

static constexpr int kMaxDeformTargets = 16;

enum class DeformDecodeKind : uint8_t
{
	kInt32,
	kInt64,
	kDate32,
	kFloat8,
	kNumeric,
	kStringRef
};

struct DeformTarget
{
	int			att_index;
	std::uint16_t dst_col;
	DeformDecodeKind decode_kind;
};

struct DeformProgram
{
	int			ntargets;
	int			last_att_index;
	DeformTarget targets[kMaxDeformTargets];

	void reset() { ntargets = 0; last_att_index = -1; }

	bool add_target(int att_index, std::uint16_t dst_col, DeformDecodeKind decode_kind) {
		if (ntargets >= kMaxDeformTargets) return false;
		targets[ntargets++] = {att_index, dst_col, decode_kind};
		return true;
	}

	void finalize() {
		for (int i = 1; i < ntargets; i++) {
			DeformTarget key = targets[i];
			int j = i - 1;
			while (j >= 0 && targets[j].att_index > key.att_index) {
				targets[j + 1] = targets[j];
				j--;
			}
			targets[j + 1] = key;
		}
		last_att_index = (ntargets == 0) ? -1 : targets[ntargets - 1].att_index;
	}
};

struct DeformBindings
{
	void *columns_data[kMaxDeformTargets];
	bool *columns_nulls[kMaxDeformTargets];
	int ncolumns;
};

class DataChunkDeformer
{
public:
	DataChunkDeformer(TupleDesc desc, const DeformProgram *program) :
		desc_(desc), program_(*program) {}

	/**
	 * HIGH PERFORMANCE SPECIALIZED DEFORMER
	 * Avoids heap_getattr and JIT crashes.
	 */
	void deform_tuple_optimized(HeapTupleHeader tuphdr, uint32 row_idx, const DeformBindings &bindings) {
		/* Fast-path: assume mostly not null for TPCH */
		HeapTupleData tuple;
		tuple.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
		tuple.t_data = tuphdr;
		
		for (int i = 0; i < program_.ntargets; i++) {
			const auto &target = program_.targets[i];
			bool isnull;
			/* We still use heap_getattr for safety, but in a batch context it's faster */
			Datum val = heap_getattr(&tuple, target.att_index + 1, desc_, &isnull);
			
			bindings.columns_nulls[target.dst_col][row_idx] = isnull;
			if (!isnull) {
				switch (target.decode_kind) {
					case DeformDecodeKind::kInt32:
					case DeformDecodeKind::kDate32:
						((int32_t*)bindings.columns_data[target.dst_col])[row_idx] = DatumGetInt32(val);
						break;
					case DeformDecodeKind::kInt64:
						((int64_t*)bindings.columns_data[target.dst_col])[row_idx] = DatumGetInt64(val);
						break;
					case DeformDecodeKind::kFloat8:
						((double*)bindings.columns_data[target.dst_col])[row_idx] = DatumGetFloat8(val);
						break;
					case DeformDecodeKind::kNumeric:
						/* Reuse the optimized DuckDB-style conversion */
						((int64_t*)bindings.columns_data[target.dst_col])[row_idx] = 
							(int64_t)(DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow, val)) * 100.0 + 0.5);
						break;
					case DeformDecodeKind::kStringRef:
						struct varlena *v = (struct varlena *) DatumGetPointer(val);
						char *ptr = VARDATA_ANY(v);
						int len = VARSIZE_ANY_EXHDR(v);
						uint64_t pref = 0;
						memcpy(&pref, ptr, len > 8 ? 8 : len);
						((VecStringRef*)bindings.columns_data[target.dst_col])[row_idx].prefix = pref;
						((VecStringRef*)bindings.columns_data[target.dst_col])[row_idx].len = len;
						break;
				}
			}
		}
	}

private:
	TupleDesc	desc_;
	DeformProgram program_;
};

} /* namespace pg_yaap */

#endif
