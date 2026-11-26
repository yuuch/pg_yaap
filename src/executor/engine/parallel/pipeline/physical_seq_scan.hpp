#pragma once

extern "C" {
#include "postgres.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "catalog/pg_type_d.h"
#include "executor/execdesc.h"
#include "storage/block.h"
#include "storage/read_stream.h"
#include "utils/dsa.h"
#include "utils/rel.h"
}

#include <memory>

#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/physical_seq_scan_read_stream.hpp"
#include "core/data_chunk.hpp"
#include "core/data_chunk_deform.hpp"
#include "expr/expr.hpp"  /* pg_yaap_release_llvm_jit_context */

extern "C" {
#include "jit/jit.h"  /* JitContext */
}

namespace pg_yaap {

namespace pipeline {

struct OpDescriptor;
struct SchemaDescriptor;
struct SeqScanSharedPayload;  /* canonical definition in pipeline_descriptor.hpp */

class SeqScanGlobalState final : public GlobalSourceState {
public:
	dsa_area             *dsa = nullptr;
	OpDescriptor         *desc = nullptr;
	SeqScanSharedPayload *shared = nullptr;
	dsa_pointer           shared_payload_dp = InvalidDsaPointer;
	uint32                max_threads = 1;
};

class SeqScanLocalState final : public LocalSourceState {
public:
	Relation        rel = nullptr;
	HeapScanDesc    scan_desc = nullptr;
	ReadStream     *read_stream = nullptr;
	TupleDesc       scan_tupdesc = nullptr;
	uint32          page_visible_index = 0;
	bool            check_serializable = false;
	bool            exhausted = false;
	bool            diag_first_call_logged = false;
	bool            descriptor_cache_ready = false;
	SchemaDescriptor *out_schema_cache = nullptr;
	FilterInputDesc *filter_inputs_cache = nullptr;
	FilterExprDesc  *filter_exprs_cache = nullptr;
	FilterStep      *filter_steps_cache = nullptr;
	const char      *filter_string_consts_cache = nullptr;
	uint16_t         required_bool_regs_cache = 0;
	const FilterStep *simple_filter_step_cache = nullptr;
	SeqScanReadStreamState read_stream_state{};

	/* M-Q1-PERF B.1: split deform into qual-side (1-row scratch chunk, written
	 * at row 0 every tuple, evaluated inline) + projection-side (written at
	 * out.count and only advanced on qual survival). Kills nocachegetattr
	 * (15.4% of worker time) on rejected rows by short-circuiting projection
	 * deform after the qual fails. Both deformers JIT-compile via the same
	 * factory; gated by pg_yaap.jit_deform. */
	DeformProgram                          proj_deform_program{};
	DeformProgram                          filter_deform_program{};
	bool                                   deform_programs_built = false;
	std::unique_ptr<DataChunkDeformer>     proj_deformer;
	std::unique_ptr<DataChunkDeformer>     filter_deformer;
	/* qual_chunk: heap-allocated 1024-row scratch (uses DataChunk's
	 * MemoryContext-backed operator new, ~600KB). Always written at row 0;
	 * size matches PIPELINE_DEFAULT_CHUNK_SIZE so it shares the existing
	 * deformer/JIT instantiation (no new template). The widened descriptor keeps
	 * clauses in a tiny fixed array, so we cache the per-clause dst columns once
	 * during build and then reuse them for every tuple. */
	std::unique_ptr<DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>> filter_chunk;
	uint8_t                                filter_bool_values[FILTER_MAX_BOOL_REGS]{};
	bool                                   filter_uses_string_arena = false;
	JitContext                            *proj_jit_context = nullptr;
	JitDeformFunc                          proj_jit_func = nullptr;
	JitContext                            *filter_jit_context = nullptr;
	JitDeformFunc                          filter_jit_func = nullptr;

	~SeqScanLocalState()
	{
#ifdef USE_LLVM
		if (proj_jit_context != nullptr)
		{
			pg_yaap_release_llvm_jit_context(proj_jit_context);
			proj_jit_context = nullptr;
			proj_jit_func = nullptr;
		}
		if (filter_jit_context != nullptr)
		{
			pg_yaap_release_llvm_jit_context(filter_jit_context);
			filter_jit_context = nullptr;
			filter_jit_func = nullptr;
		}
#endif
		if (scan_desc != nullptr)
		{
			if (read_stream != nullptr)
			{
				read_stream_end(read_stream);
				read_stream = nullptr;
				scan_desc->rs_read_stream = nullptr;
			}
			heap_endscan((TableScanDesc) scan_desc);
		}
		if (rel != nullptr)
			relation_close(rel, AccessShareLock);
	}
};

class PhysicalSeqScan final : public PhysicalOperator {
public:
	PhysicalSeqScan(Oid relid,
	                dsa_pointer input_schema_dp,
	                dsa_pointer output_schema_dp,
	                dsa_pointer filter_inputs_dp,
	                dsa_pointer filter_exprs_dp,
	                dsa_pointer filter_steps_dp,
	                dsa_pointer filter_string_consts_dp,
	                uint16_t n_filter_inputs,
	                uint16_t n_filter_exprs,
	                uint16_t n_filter_steps,
	                uint16_t filter_bool_regs,
	                uint32_t filter_string_const_bytes,
	                dsa_pointer shared_payload_dp,
	                OpDescriptor *desc = nullptr)
		: PhysicalOperator(PhysicalOperatorType::SEQ_SCAN)
		, relid_(relid)
		, input_schema_dp_(input_schema_dp)
		, output_schema_dp_(output_schema_dp)
		, filter_inputs_dp_(filter_inputs_dp)
		, filter_exprs_dp_(filter_exprs_dp)
		, filter_steps_dp_(filter_steps_dp)
		, filter_string_consts_dp_(filter_string_consts_dp)
		, n_filter_inputs_(n_filter_inputs)
		, n_filter_exprs_(n_filter_exprs)
		, n_filter_steps_(n_filter_steps)
		, filter_bool_regs_(filter_bool_regs)
		, filter_string_const_bytes_(filter_string_const_bytes)
		, shared_payload_dp_(shared_payload_dp)
		, desc_(desc)
	{}

	bool IsSource() const override { return true; }
	bool IsSink() const override { return false; }
	bool IsPipelineBreaker() const override { return false; }
	bool ParallelSource() const override { return true; }

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSourceState>  GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate) override;
	SourceResultType                   GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) override;
	int                                MaxThreads(ExecCtx &ctx) const override;

	Oid            relid() const { return relid_; }
	dsa_pointer    input_schema_dp() const { return input_schema_dp_; }
	dsa_pointer    output_schema_dp() const { return output_schema_dp_; }
	dsa_pointer    filter_inputs_dp() const { return filter_inputs_dp_; }
	dsa_pointer    filter_exprs_dp() const { return filter_exprs_dp_; }
	dsa_pointer    filter_steps_dp() const { return filter_steps_dp_; }
	dsa_pointer    filter_string_consts_dp() const { return filter_string_consts_dp_; }
	uint16_t       n_filter_inputs() const { return n_filter_inputs_; }
	uint16_t       n_filter_exprs() const { return n_filter_exprs_; }
	uint16_t       n_filter_steps() const { return n_filter_steps_; }
	uint16_t       filter_bool_regs() const { return filter_bool_regs_; }
	uint32_t       filter_string_const_bytes() const { return filter_string_const_bytes_; }
	dsa_pointer    shared_payload_dp() const { return shared_payload_dp_; }
	OpDescriptor  *desc() const { return desc_; }
	const PgVector<OpDescriptor *> &descs() const { return desc_list_; }
	void           AttachDescriptor(OpDescriptor *desc) { desc_ = desc; desc_list_.push_back(desc); }  /* see physical_hash_aggregate.hpp Fix A2 */

private:
	Oid          relid_;
	dsa_pointer  input_schema_dp_;
	dsa_pointer  output_schema_dp_;
	dsa_pointer  filter_inputs_dp_;
	dsa_pointer  filter_exprs_dp_;
	dsa_pointer  filter_steps_dp_;
	dsa_pointer  filter_string_consts_dp_;
	uint16_t     n_filter_inputs_;
	uint16_t     n_filter_exprs_;
	uint16_t     n_filter_steps_;
	uint16_t     filter_bool_regs_;
	uint32_t     filter_string_const_bytes_;
	dsa_pointer  shared_payload_dp_;
	OpDescriptor *desc_;
	PgVector<OpDescriptor *> desc_list_;
};

}  /* namespace pipeline */
}  /* namespace pg_yaap */
