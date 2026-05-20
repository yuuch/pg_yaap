#pragma once

/*
 * pipeline/pipeline_descriptor.hpp
 *
 * Cross-process IR descriptors for a single MetaPipeline bundle, plus the
 * leader-side serialize / worker-side reconstruct entry points.
 *
 * This header ships the POD layout that §8.5.4.2 of
 * `docs/GLOBAL_LOCAL_STATE_DESIGN.md` (HEAD eb7901b022a) locks down, plus the
 * leader-side serialize / worker-side reconstruct entry points used by the
 * MetaPipeline runtime cut-over.
 *
 * Anti-pattern compliance (pipeline/AGENTS.md + core/AGENTS.md):
 *   - No palloc'd pointer is ever stored in any *Body field. Only POD
 *     primitives, Oid, and dsa_pointer. Any pointer that crosses the
 *     leader/worker boundary is a DSA offset.
 *   - All `dsa_allocate` happens leader-side under
 *     `MemoryContextSwitchTo(per-query-mcxt)`.
 *
 * Spec: docs/GLOBAL_LOCAL_STATE_DESIGN.md §8.5.4.2 (POD layout, L1079-1162),
 * §8.5.4.3 (leader serialize, L1163-1250), §8.5.4.4 (worker reconstruct,
 * L1251-1299), §8.5.4.5 (lazy GlobalSinkState, L1300-1345),
 * §8.5.4.7 (ExprBytecode constraints, L1346-1357).
 */

extern "C" {
#include "postgres.h"
#include "access/relscan.h"
#include "storage/block.h"
#include "port/atomics.h"
#include "utils/dsa.h"
}

#include <cstdint>
#include <memory>  // IWYU pragma: keep

#include "core/memory.hpp"  // IWYU pragma: keep
#include "parallel/pipeline/tuple_data_layout.hpp"

namespace pg_yaap {

/*
 * Forward declarations only -- pipeline_descriptor.hpp must not pull in the
 * full operator/pipeline headers. That keeps the descriptor IR a leaf of the
 * include graph and avoids circular dependencies once 3g.2-final wires
 * physical_*.hpp to call StoreSharedPayloadOnDescriptor() on themselves.
 */
namespace pipeline {

class  PhysicalOperator;
struct Pipeline;
struct ExecCtx;
struct MetaPipelineBundle;
struct PipelineSharedControl;        /* defined in dsm_control.hpp */

/* -------------------------------------------------------------------------
 * §8.5.4.2 Operator kind tag (POD).
 * ------------------------------------------------------------------------- */
enum class OpKind : uint8_t {
	SEQ_SCAN       = 0,
	DELIM_SCAN     = 1,
	HASH_AGGREGATE = 2,
	PERFECT_HASH_AGGREGATE = 3,
	HASH_JOIN      = 4,
	CROSS_PRODUCT  = 5,
	ORDER          = 6,
	TOP_N          = 7,
	OUTPUT         = 8,
	FILTER         = 9,
	PROJECTION     = 10,
};

/* -------------------------------------------------------------------------
 * §8.5.4.2 Schema descriptor (POD with FAM).
 *
 * `columns` is a flexible-array trailer; total dsa_allocate size =
 *   offsetof(SchemaDescriptor, columns) + n_columns * sizeof(ColumnSchema).
 *
 * 3g.2-final Step 7 contract additions (descriptor-driven SeqScan projection):
 *   - `src_attno` is the 1-based heap attno that PhysicalSeqScan reads via
 *     heap_getattr(tuple, src_attno, tupdesc, &isnull). It is meaningful
 *     ONLY for SeqScan output_schema. For HashAgg / Order / Output schemas
 *     the translator MUST set src_attno = 0 and consumers MUST NOT read it.
 *   - `chunk_slot` is the per-storage-type DataChunk column index in
 *     [0, 16). DataChunk has int32_columns[16], int64_columns[16],
 *     double_columns[16], string_columns[16], nulls[16] (see
 *     core/data_chunk.hpp lines 66-69). Two columns of different storage
 *     kinds may legitimately share the same chunk_slot (one in int32,
 *     another in int64). Translator owns the assignment.
 *   - `decode_kind` selects how SeqScan converts a heap Datum to the
 *     scaled DataChunk slot. CHAR -> int32; NUMERIC(15,2) -> scaled int64
 *     via DirectFunctionCall; DATE -> int32 (DateADT is int32); etc.
 * ------------------------------------------------------------------------- */
enum class ColumnDecodeKind : uint8_t {
	NONE                 = 0,   /* not produced by SeqScan; consumer-defined */
	INT32_CHAR           = 1,   /* DatumGetChar -> int32_columns */
	INT32_DATE           = 2,   /* DatumGetDateADT -> int32_columns */
	INT32_INT4           = 3,   /* DatumGetInt32 -> int32_columns */
	INT64_INT8           = 4,   /* DatumGetInt64 -> int64_columns */
	INT64_NUMERIC_SCALED = 5,   /* numeric * 100 -> int64_columns */
	DOUBLE_FLOAT8        = 6,   /* DatumGetFloat8 -> double_columns */
	STRING_REF           = 7,   /* varlena/text-like -> string_columns */
};

struct ColumnSchema {
	Oid              type_oid;
	int32            typmod;
	int16_t          typlen;
	bool             typbyval;
	uint8_t          chunk_slot;     /* per-storage-type DataChunk slot [0, 16) */
	int16_t          src_attno;      /* 1-based heap attno; 0 if not from SeqScan */
	ColumnDecodeKind decode_kind;    /* how SeqScan converts Datum -> chunk slot */
	uint8_t          _pad0;
};

struct SchemaDescriptor {
	uint16_t     n_columns;
	uint16_t     _pad0;
	uint32_t     _pad1;
	ColumnSchema columns[FLEXIBLE_ARRAY_MEMBER];
};

/* -------------------------------------------------------------------------
 * Generic conjunctive qual descriptor for SeqScan.
 *
 * The translator publishes a compact AND-of-simple-clauses form so the scan
 * runtime can stay detached from PostgreSQL's planner node tree. Each clause
 * is still a single column-op-const comparison with a by-value payload; the
 * descriptor now just holds several of them so Q6-style filters avoid a
 * separate expression bytecode dependency.
 * ------------------------------------------------------------------------- */
enum class QualKind : uint8_t {
	NONE         = 0,   /* identically true */
	COL_OP_CONST = 1,
};

enum class QualOp : uint8_t {
	LE = 0,
	LT = 1,
	EQ = 2,
	GE = 3,
	GT = 4,
	NE = 5,
};

struct QualDescriptor {
	static constexpr uint16_t MAX_CLAUSES = 8;

	struct Clause {
		QualOp           op;
		ColumnDecodeKind decode_kind;     /* drives qual deform kind */
		uint16_t         col_attno;       /* 1-based heap attno (not chunk_slot) */
		uint16_t         _pad0;
		Oid              const_typoid;    /* runtime compare dispatch type */
		uint64_t         const_value;     /* by-value payload only */
	};

	QualKind kind;
	uint8_t  n_clauses;
	uint16_t _pad0;
	uint32_t _pad1;
	Clause   clauses[MAX_CLAUSES];
};

static constexpr uint16_t FILTER_MAX_INPUTS = 16;
static constexpr uint16_t FILTER_MAX_STEPS = 64;
static constexpr uint16_t FILTER_MAX_BOOL_REGS = 64;

enum class FilterStepOp : uint8_t {
	INT32_CMP_CONST    = 0,
	INT64_CMP_CONST    = 1,
	STRING_EQ_CONST    = 2,
	STRING_NE_CONST    = 3,
	STRING_PREFIX_LIKE = 4,
	STRING_CONTAINS_LIKE = 5,
	STRING_SQL_LIKE    = 6,
	BOOL_AND           = 7,
	BOOL_OR            = 8,
	BOOL_NOT           = 9,
	INT32_CMP_VAR      = 10,
	INT64_CMP_VAR      = 11,
};

struct FilterInputDesc {
	uint16_t         attno;
	uint8_t          dst_col;
	ColumnDecodeKind decode_kind;
	ColumnDecodeKind source_decode_kind;
	uint8_t          numeric_scale;
	uint8_t          _pad0;
};

struct FilterExprDesc {
	uint16_t first_step_idx;
	uint16_t n_steps;
	uint16_t output_bool_reg;
	uint16_t _pad0;
};

struct FilterOpBody {
	dsa_pointer input_schema;
	dsa_pointer filter_inputs;
	dsa_pointer filter_exprs;
	dsa_pointer filter_steps;
	dsa_pointer filter_string_consts;
	uint16_t    n_filter_inputs;
	uint16_t    n_filter_exprs;
	uint16_t    n_filter_steps;
	uint16_t    filter_bool_regs;
	uint32_t    filter_string_const_bytes;
};

struct FilterStep {
	FilterStepOp op;
	QualOp       cmp_op;
	uint16_t     left_idx;
	uint16_t     right_idx;
	uint16_t     out_bool_reg;
	uint16_t     _pad0;
	uint32_t     const_offset;
	uint32_t     const_len;
	uint64_t     const_value;
};

/* -------------------------------------------------------------------------
 * §8.5.4.7 Expression bytecode (POD; integer-index operands; by-value or
 * by-bytes-copied constants; Oid function dispatch via fmgr_info_cxt;
 * host byte order). 3g.2-prep ships the layout only; SerializeExprProgram
 * returns InvalidDsaPointer for nullptr quals and ereport(ERROR) for any
 * non-null qual until 3g.2-final/M-Q1-PERF lowers real expressions.
 * ------------------------------------------------------------------------- */
struct ExprBytecode {
	uint32_t n_insns;
	uint32_t n_consts;
	uint32_t const_pool_bytes;
	uint32_t _pad0;
	/* trailing layout (deferred to 3g.2-final implementation):
	 *   Insn      insns[n_insns];
	 *   ConstSlot consts[n_consts];
	 *   uint8_t   const_pool[const_pool_bytes];
	 */
};

/* -------------------------------------------------------------------------
 * Projection expression descriptor (POD).
 *
 * PhysicalProjection evaluates a small int64 opcode tape over DataChunk
 * chunk_slot inputs. Constants are pre-scaled integer payloads. The runtime
 * stays interpreter-only for now.
 * ------------------------------------------------------------------------- */
enum class ProjectOp : uint8_t {
	NUMERIC_SCALE_VAR_CONST = 0,
	NUMERIC_MUL_VAR_VAR   = 1,
	NUMERIC_MUL_VAR_CONST = 2,
	NUMERIC_SUB_CONST_VAR = 3,
	NUMERIC_ADD_CONST_VAR = 4,
	NUMERIC_ADD_VAR_VAR   = 5,
	NUMERIC_SUB_VAR_VAR   = 6,
	NUMERIC_ADD_VAR_CONST = 7,
	NUMERIC_SUB_VAR_CONST = 8,
	COPY_VAR              = 9,
	NUMERIC_DIV_VAR_VAR   = 10,
	STRING_PREFIX_LIKE    = 11,
	NUMERIC_CASE_VAR_CONST = 12,
	EXTRACT_YEAR_FROM_DATE = 13,
	STRING_EQ_VAR_CONST = 14,
	STRING_NE_VAR_CONST = 15,
	NUMERIC_CASE_ELSE_VAR = 16,
	BOOL_AND_VAR_VAR = 17,
	BOOL_OR_VAR_VAR = 18,
	BOOL_NOT_VAR = 19,
	CONST_INT64 = 20,
	INT32_TO_INT64_VAR = 21,
	STRING_PREFIX_SLICE = 22,
	INT64_LT_VAR_CONST = 23,
	INT64_LE_VAR_CONST = 24,
	INT64_EQ_VAR_CONST = 25,
	INT64_GE_VAR_CONST = 26,
	INT64_GT_VAR_CONST = 27,
	INT64_NE_VAR_CONST = 28,
};

struct ProjectStep {
	ProjectOp op;
	uint8_t   in_a_chunk_slot;
	uint8_t   in_b_chunk_slot;
	uint8_t   out_chunk_slot;
	int64_t   const_value;
};

struct ProjectExprDesc {
	uint16_t first_step_idx;
	uint16_t n_steps;
	uint8_t  output_chunk_slot;
	int8_t   output_scale;
	uint16_t _pad0;
};

struct ProjectOpBody {
	dsa_pointer input_schema;
	dsa_pointer output_schema;
	dsa_pointer expr_descs;
	dsa_pointer steps;
	uint16_t    n_exprs;
	uint16_t    n_steps_total;
	uint32_t    _pad0;
};

enum class HashJoinOutputSide : uint8_t {
	LEFT = 0,
	RIGHT = 1,
};

struct HashJoinFilterInputDesc {
	HashJoinOutputSide side;
	uint8_t            input_chunk_slot;
	ColumnDecodeKind   decode_kind;
	ColumnDecodeKind   source_decode_kind;
	uint8_t            numeric_scale;
};

struct HashJoinOutputColumnDesc {
	HashJoinOutputSide side;
	uint8_t            input_chunk_slot;
	ColumnDecodeKind   decode_kind;
	uint8_t            output_chunk_slot;
};

enum class HashJoinMatchMode : uint8_t {
	INNER = 0,
	SEMI = 1,
	ANTI = 2,
	LEFT = 3,
};

struct HashJoinOpBody {
	dsa_pointer left_input_schema;
	dsa_pointer right_input_schema;
	dsa_pointer output_schema;
	dsa_pointer left_key_layout;
	dsa_pointer right_key_layout;
	dsa_pointer left_payload_layout;
	dsa_pointer right_payload_layout;
	dsa_pointer output_columns; /* HashJoinOutputColumnDesc[output_column_count] */
	dsa_pointer filter_inputs; /* HashJoinFilterInputDesc[n_filter_inputs] */
	dsa_pointer filter_exprs;  /* FilterExprDesc[n_filter_exprs] */
	dsa_pointer filter_steps;  /* FilterStep[n_filter_steps] */
	dsa_pointer filter_string_consts; /* char[filter_string_const_bytes] */
	dsa_pointer shared_payload;
	uint16_t    n_left_keys;
	uint16_t    n_right_keys;
	uint16_t    output_column_count;
	uint16_t    n_filter_inputs;
	uint16_t    n_filter_exprs;
	uint16_t    n_filter_steps;
	uint16_t    filter_bool_regs;
	HashJoinMatchMode join_mode;
	uint8_t     _pad0[1];
	uint32_t    filter_string_const_bytes;
	uint32_t    max_rows;
};

struct CrossProductOpBody {
	dsa_pointer left_input_schema;
	dsa_pointer right_input_schema;
	dsa_pointer output_schema;
	dsa_pointer right_payload_layout;
	dsa_pointer output_columns; /* HashJoinOutputColumnDesc[output_column_count] */
	dsa_pointer shared_payload;
	uint16_t    output_column_count;
	uint16_t    _pad0;
	uint32_t    max_rows;
};

/* -------------------------------------------------------------------------
 * §6.3 + §8.5.4.5 Per-operator shared payload PODs.
 *
 * These structures are dsa_allocated by the leader and addressed via
 * dsa_pointer fields on the corresponding *OpBody. They are NOT owned by
 * any C++ class; the operator's GlobalSinkState/GlobalSourceState C++ object
 * holds a *view* via dsa_get_address() on every method call.
 * ------------------------------------------------------------------------- */
struct SeqScanSharedPayload {
	ParallelBlockTableScanDescData pbscan;
	BlockNumber                    total_blocks;
};

struct HashJoinLocalBuildRegistryEntry {
	dsa_pointer build_keys_dp;
	dsa_pointer build_rows_dp;
	uint32_t    row_count;
	uint32_t    key_heap_used;
	uint32_t    row_heap_used;
	uint32_t    global_row_offset;
	uint32_t    global_key_heap_offset;
	uint32_t    global_row_heap_offset;
};

struct HashJoinSharedPayload {
	slock_t     mutex;
	uint32_t    local_state_slot_count;
	uint32_t    build_partition_count;
	uint32_t    hash_table_capacity;
	uint8_t     radix_bits;
	bool        combined;
	bool        build_rows_shared_local;
	bool        build_rows_use_keys;
	bool        finalized;
	uint8_t     _pad0[2];
	pg_atomic_uint32 release_state;       /* 0=live, 1=releasing, 2=released */
	pg_atomic_uint32 combine_prepare_state; /* 0=pending, 1=preparing, 2=ready */
	uint32_t    combined_row_count;
	uint32_t    combined_key_heap_used;
	uint32_t    combined_row_heap_used;
	dsa_pointer local_build_registry_dp; /* HashJoinLocalBuildRegistryEntry[local_state_slot_count] */
	dsa_pointer build_keys_dp;           /* global build-side key row store */
	dsa_pointer build_rows_dp;           /* future global build-side row store */
	dsa_pointer build_row_slots_dp;      /* uint32_t[combined_row_count] -> registry slot */
	dsa_pointer build_row_local_idxs_dp; /* uint32_t[combined_row_count] -> row index within slot-local payload TDC */
	dsa_pointer hash_table_dp;           /* uint32_t[hash_table_capacity] bucket heads */
	dsa_pointer hash_buckets_dp;         /* uint32_t[build_rows.row_count] cached bucket indices */
	dsa_pointer hash_links_dp;           /* uint32_t[build_rows.row_count] next indices */
	dsa_pointer hash_salts_dp;           /* uint16_t[build_rows.row_count] high-hash salts */
};

struct CrossProductLocalBuildRegistryEntry {
	dsa_pointer build_rows_dp;
	uint32_t    row_count;
};

struct CrossProductSharedPayload {
	slock_t     mutex;
	uint32_t    local_state_slot_count;
	bool        finalized;
	uint8_t     _pad0[3];
	pg_atomic_uint32 release_state;
	uint32_t    combined_row_count;
	dsa_pointer local_build_registry_dp; /* CrossProductLocalBuildRegistryEntry[local_state_slot_count] */
	dsa_pointer build_rows_dp;           /* global build-side row store */
};

/*
 * 3g.2-final step 5 removes the Q1-specific payload aliases entirely. Shared
 * row storage is now described by TupleDataLayout and materialized either as a
 * TupleDataCollection (Order) or an AggregateHashTable over a TDC (HashAgg).
 */

/* -------------------------------------------------------------------------
 * §8.5.4.2 L1133 Aggregate function descriptor (POD per-agg).
 * ------------------------------------------------------------------------- */
struct AggFuncDesc {
	Oid      agg_oid;
	Oid      transtype;
	Oid      finaltype;
	uint16_t input_col_idx;
	uint16_t _pad;
};

/* -------------------------------------------------------------------------
 * §8.5.4.2 L1141 Sort key descriptor (POD per-key).
 * ------------------------------------------------------------------------- */
struct SortKeyDesc {
	Oid      collation_oid;
	uint16_t col_idx;
	bool     asc;
	bool     nulls_first;
	uint32_t _pad;
};

/* -------------------------------------------------------------------------
 * §8.5.4.2 L1090-1162 Per-OpKind body PODs. Each is the payload of the
 * tagged-union slot inside OpDescriptor.body.
 *
 * Sizing rule: every *OpBody is fixed-size (no FAM). Variable-length data
 * (input/output schemas, group key indices, agg/sort descriptors) is held
 * via *separate* dsa_pointer allocations or by inlining in a sibling FAM
 * struct (HashAgg/Order use the latter, see comments at struct site).
 * ------------------------------------------------------------------------- */
struct SeqScanOpBody {
	Oid         relid;
	dsa_pointer input_schema;        /* SchemaDescriptor */
	dsa_pointer output_schema;       /* SchemaDescriptor (may equal input_schema) */
	dsa_pointer filter_inputs;       /* FilterInputDesc[n_filter_inputs] */
	dsa_pointer filter_exprs;        /* FilterExprDesc[n_filter_exprs] */
	dsa_pointer filter_steps;        /* FilterStep[n_filter_steps] */
	dsa_pointer filter_string_consts; /* char[filter_string_const_bytes] */
	dsa_pointer shared_payload;      /* SeqScanSharedPayload */
	uint16_t    n_filter_inputs;
	uint16_t    n_filter_exprs;
	uint16_t    n_filter_steps;
	uint16_t    filter_bool_regs;
	uint32_t    filter_string_const_bytes;
};

struct HashAggOpBody {
	dsa_pointer input_schema;        /* SchemaDescriptor */
	dsa_pointer output_schema;       /* SchemaDescriptor */
	dsa_pointer group_keys;          /* uint16_t[n_group_keys] */
	dsa_pointer agg_funcs;           /* AggFuncDesc[n_agg_funcs] */
	dsa_pointer layout;              /* TupleDataLayout serialized from PG Agg plan */
	dsa_pointer shared_payload;      /* HashAggSharedPayload wrapper */
	uint16_t    n_group_keys;
	uint16_t    n_agg_funcs;
	uint32_t    max_groups;
	uint32_t    perfect_hash_capacity;
};

struct DelimScanOpBody {
	dsa_pointer input_schema;        /* SchemaDescriptor */
	dsa_pointer shared_payload;      /* HashAggSharedPayload wrapper */
};

struct OrderOpBody {
	dsa_pointer input_schema;        /* SchemaDescriptor */
	dsa_pointer sort_keys;           /* SortKeyDesc[n_sort_keys] */
	dsa_pointer key_layout;          /* TupleDataLayout for sort-key row storage */
	dsa_pointer payload_layout;      /* TupleDataLayout for full payload row storage */
	dsa_pointer shared_payload;      /* TupleDataCollection; may be Invalid until leader attach */
	dsa_pointer sort_indices;        /* OrderSortIndices; populated in Finalize, consumed in GetData */
	uint16_t    n_sort_keys;
	uint16_t    _pad0;
	uint32_t    max_rows;
};

struct TopNOpBody {
	dsa_pointer input_schema;        /* SchemaDescriptor */
	dsa_pointer layout;              /* TupleDataLayout for payload storage */
	dsa_pointer sort_keys;           /* SortKeyDesc[n_sort_keys] */
	dsa_pointer shared_payload;      /* TopNSharedPayload */
	dsa_pointer sort_indices;        /* OrderSortIndices; populated in Finalize */
	uint16_t    n_sort_keys;
	uint16_t    _pad0;
	uint32_t    max_rows;
};

struct OutputOpBody {
	dsa_pointer input_schema;        /* SchemaDescriptor */
	dsa_pointer layout;              /* TupleDataLayout serialized from input_schema */
	dsa_pointer sort_keys;           /* SortKeyDesc[n_sort_keys] for final top-N pruning */
	dsa_pointer shared_payload;      /* TupleDataCollection; lazy-init on first GetGlobalSinkState */
	uint16_t    n_sort_keys;
	uint16_t    _pad0;
	uint32_t    tdc_max_rows;        /* row capacity bound (clamp(plan_rows*1.5, 1024, 1<<20)) */
	uint64_t    max_emit_rows;
};

/* -------------------------------------------------------------------------
 * §8.5.4.2 L1145-1162 OpDescriptor: tagged union of operator bodies plus
 * fixed-fanout child indexing (OpDescriptor[]-relative inside the parent
 * PipelineDescriptor).
 *
 * `n_children` is the populated count; only `child_indices[0..n_children)`
	 * is meaningful. 4-slot inline cap matches the maximum fanout of the five
	 * locked operator types (HashAgg/Order are dual Sink+Source, so they
	 * appear in two pipelines but each pipeline references the operator via
	 * a single child index).
	 * ------------------------------------------------------------------------- */
struct OpDescriptor {
	OpKind   kind;
	uint8_t  n_children;
	uint16_t _pad0;
	uint32_t child_indices[4];

	union OpBodyUnion {
		SeqScanOpBody seq_scan;
		DelimScanOpBody delim_scan;
		HashAggOpBody hash_agg;
		HashAggOpBody perfect_hash_agg;
		HashJoinOpBody hash_join;
		CrossProductOpBody cross_product;
		OrderOpBody   order;
		TopNOpBody    top_n;
		OutputOpBody  output;
		FilterOpBody  filter;
		ProjectOpBody project;
	} body;
};

/* -------------------------------------------------------------------------
 * §8.5.4.2 PipelineDescriptor: one entry per pipeline inside a MetaPipeline
 * bundle. `ops` is a dsa_pointer to OpDescriptor[op_count].
 *
 * `global_source_state` / `global_sink_state` start at InvalidDsaPointer at
 * serialize time; they are lazily populated by the leader's first
 * `Sink::GetGlobalSinkState(ExecCtx&)` / `Source::GetGlobalSourceState(...)`
 * call (the one branch where `worker_index == LEADER_WORKER_INDEX`) via
 * StoreSharedPayloadOnDescriptor(). Workers attach via
 * LoadSharedPayloadFromDescriptor().
 *
 * `task_slot_next` is a per-pipeline atomic morsel cursor used by the
 * scheduler to hand out PipelineTask slots round-robin.
 * ------------------------------------------------------------------------- */
struct PipelineDescriptor {
	int32              pipeline_id;
	int32              op_count;
	uint64             dependency_mask;     /* bit i set => depends on pipeline_id i */
	dsa_pointer        ops;                  /* OpDescriptor[op_count] */
	dsa_pointer        global_source_state;  /* lazy; set by leader on first Get */
	dsa_pointer        global_sink_state;    /* lazy; set by leader on first Get */
	pg_atomic_uint32   task_slot_next;
	uint32_t           _pad;
};

/* -------------------------------------------------------------------------
 * §8.5.4.5 Helper API for lazy GlobalSinkState publication.
 *
 * Each PhysicalOperator instance carries a raw `OpDescriptor *desc_` field
 * (added in 3g.2-final when ctors are wired), populated when the worker
 * placement-news the operator in §8.5.4.4 L1264. These two helpers translate
 * between that pointer and the descriptor's `global_sink_state` slot.
 *
 * 3g.2-prep ships declarations only; bodies land in 3g.2-final next to the
 * GetGlobalSinkState/GetGlobalSourceState factories that call them.
 * ------------------------------------------------------------------------- */
void        StoreSharedPayloadOnDescriptor(const PhysicalOperator *op, dsa_pointer dp);
dsa_pointer LoadSharedPayloadFromDescriptor(const PhysicalOperator *op);
void        ClearSharedPayloadOnDescriptor(const PhysicalOperator *op);

/*
 * Step 5 contract delta: HashAggregate / Order descriptor bodies now publish
 * serialized TupleDataLayout PODs so workers reconstruct the exact row codec
 * the leader derived from the real PostgreSQL plan tree.
 */
dsa_pointer     SerializeTupleDataLayout(const TupleDataLayout &layout, dsa_area *dsa);

/* -------------------------------------------------------------------------
 * §8.5.4.3 Leader entry point.
 *
 * Walks `bundle` (one MetaPipeline tree, post-Translate) and emits a single
 * dsa-rooted PipelineDescriptor[] block. Returns the dsa_pointer that the
 * leader installs into PipelineSharedControl.pipelines_root.
 *
 * Serializes one MetaPipeline bundle into a DSA-rooted PipelineDescriptor[]
 * block for publication through PipelineSharedControl.pipelines_root.
 * ------------------------------------------------------------------------- */
dsa_pointer LeaderSerializePipelines(MetaPipelineBundle &bundle, dsa_area *dsa);

/* -------------------------------------------------------------------------
 * §8.5.4.4 Worker entry point.
 *
 * Reads `ctl->pipelines_root` from the attached DSA, placement-news every
 * PhysicalOperator into `worker_ctx.mcxt`, builds the C++ Pipeline objects,
 * and appends them to `out` in pipeline_id order.
 *
 * Reconstructs per-process Pipeline / PhysicalOperator objects from the
 * attached descriptor IR and appends them to `out` in pipeline_id order.
 * ------------------------------------------------------------------------- */
void WorkerReconstructPipelines(PipelineSharedControl                 *ctl,
                                ExecCtx                               &worker_ctx,
                                PgVector<std::unique_ptr<Pipeline>>   &out);

}  /* namespace pipeline */
}  /* namespace pg_yaap */
