#include "core/data_chunk_deform.hpp"

extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "access/tupmacs.h"
#include "fmgr.h"
#include "utils/numeric.h"
#include "varatt.h"

extern bool pg_yaap_trace_hooks;

extern Datum int8_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_mul(PG_FUNCTION_ARGS);
extern Datum numeric_int8(PG_FUNCTION_ARGS);
}

#include "core/types.hpp"

namespace pg_yaap
{

/*
 * Native offset-walking deformer. Mirrors slot_deform_heap_tuple_internal
 * (src/backend/executor/execTuples.c) but writes directly into columnar
 * destination slots instead of a TupleTableSlot, skipping every attribute
 * that is not a deform target. Targets are pre-sorted by att_index in
 * DeformProgram::finalize(), so a single forward walk over [0, last_att_index]
 * decodes every needed column without re-walking the tuple per column
 * (which is what `nocachegetattr` does, and what dominates Q1's profile).
 */
void
DataChunkDeformer::deform_tuple_header(HeapTupleHeader tuphdr,
									   uint32 row_idx,
									   const DeformBindings &bindings)
{
	if (!jit_path_logged_ && pg_yaap_trace_hooks)
	{
		jit_path_logged_ = true;
		elog(LOG,
			 "pg_yaap: deformer dispatch targets=%d last_att=%d path=%s",
			 program_.ntargets,
			 program_.last_att_index,
			 jit_func_ != nullptr ? "jit" : "interpreter");
	}

	if (jit_func_ != nullptr)
	{
		jit_func_(tuphdr,
				  const_cast<void **>(bindings.columns_data),
				  const_cast<uint8_t **>(bindings.columns_nulls),
				  row_idx, bindings.owner_chunk);
		return;
	}

	if (program_.ntargets == 0)
		return;

	const bool		hasnulls = (tuphdr->t_infomask & HEAP_HASNULL) != 0;
	const bits8	   *bp = tuphdr->t_bits;
	char		   *tp = (char *) tuphdr + tuphdr->t_hoff;
	uint32			off = 0;
	int				next_target = 0;
	const int		natts = desc_->natts;
	const int		walk_to = program_.last_att_index;

	for (int attnum = 0; attnum <= walk_to && attnum < natts; attnum++)
	{
		CompactAttribute *att = TupleDescCompactAttr(desc_, attnum);
		bool		isnull = hasnulls && att_isnull(attnum, bp);

		if (isnull)
		{
			while (next_target < program_.ntargets &&
				   program_.targets[next_target].att_index == attnum)
			{
				const DeformTarget &t = program_.targets[next_target];
				bindings.columns_nulls[t.dst_col][row_idx] = 1;
				next_target++;
			}
			continue;
		}

		if (att->attlen == -1)
			off = att_pointer_alignby(off, att->attalignby, -1, tp + off);
		else if (att->attlen != 0)
			off = att_nominal_alignby(off, att->attalignby);

		const char *attptr = tp + off;

		while (next_target < program_.ntargets &&
			   program_.targets[next_target].att_index == attnum)
		{
			const DeformTarget &t = program_.targets[next_target];
			bindings.columns_nulls[t.dst_col][row_idx] = 0;

			switch (t.decode_kind)
			{
				case DeformDecodeKind::kInt32:
				case DeformDecodeKind::kDate32:
					((int32_t *) bindings.columns_data[t.dst_col])[row_idx] =
						*reinterpret_cast<const int32_t *>(attptr);
					break;

				case DeformDecodeKind::kInt64:
					((int64_t *) bindings.columns_data[t.dst_col])[row_idx] =
						*reinterpret_cast<const int64_t *>(attptr);
					break;

				case DeformDecodeKind::kFloat8:
					((double *) bindings.columns_data[t.dst_col])[row_idx] =
						*reinterpret_cast<const double *>(attptr);
					break;

				case DeformDecodeKind::kBpchar1:
				{
					/*
					 * Bug G invariant: BPCHAR(1) payload is a varlena whose
					 * data byte IS the character. Reading past the varlena
					 * header (1- or 4-byte) is required for correctness;
					 * a raw int32 load reads header bytes as data.
					 */
					((int32_t *) bindings.columns_data[t.dst_col])[row_idx] =
						(int32_t)(unsigned char) *VARDATA_ANY((const struct varlena *) attptr);
					break;
				}

				case DeformDecodeKind::kNumeric:
				{
					int target_scale =
						GetNumericScaleFromTypmod(TupleDescAttr(desc_, attnum)->atttypmod);
					int64_t scaled;

					if (TryFastNumericToScaledInt64(PointerGetDatum(attptr),
													target_scale, &scaled))
					{
						((int64_t *) bindings.columns_data[t.dst_col])[row_idx] = scaled;
					}
					else
					{
						/*
						 * Slow-path fallback for numerics outside the fast
						 * decoder's supported shape (e.g. dscale > target,
						 * trailing non-zero digits). Mirrors the existing
						 * scaling helper in physical_seq_scan.cpp.
						 */
						Datum hundred = DirectFunctionCall1(int8_numeric, Int64GetDatum(100));
						Datum prod = DirectFunctionCall2(numeric_mul,
														 PointerGetDatum(attptr), hundred);
						((int64_t *) bindings.columns_data[t.dst_col])[row_idx] =
							DatumGetInt64(DirectFunctionCall1(numeric_int8, prod));
					}
					break;
				}

				case DeformDecodeKind::kStringRef:
				{
					const struct varlena *vl = (const struct varlena *) attptr;
					const char *bytes = VARDATA_ANY(vl);
					uint32_t len = (uint32_t) VARSIZE_ANY_EXHDR(vl);
					VecStringRef ref = bindings.owner_chunk->store_string_bytes(bytes, len);
					((VecStringRef *) bindings.columns_data[t.dst_col])[row_idx] = ref;
					break;
				}
			}
			next_target++;
		}

		if (att->attlen > 0)
			off += att->attlen;
		else if (att->attlen == -1)
			off += VARSIZE_ANY(tp + off);
		else
			off += strlen(tp + off) + 1;
	}
}

}
