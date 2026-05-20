/*-------------------------------------------------------------------------
 *
 * pipeline_descriptor.cpp
 *	  Cross-process IR helpers (3g.2-final step 5/6).
 *
 * Q1 runtime model after the Step 6 refactor:
 *   - Exactly two pipelines are serialized.
 *       P0: SeqScan(lineitem)+qual -> HashAggregate(sink)
 *       P1: HashAggregate(source) -> Order(sink+source) -> OutputSink
 *   - HashAggregate is one operator instance; the PARTIAL/FINAL split is gone.
 *   - A COMBINE event runs between Sink Finalize and Source GetData on that
 *     same PhysicalHashAggregate, DuckDB-faithful and leader-driven.
 *
 * Step 5 scope here is descriptor-only: we serialize already-built operator
 * metadata (schemas, group keys, aggregate descriptors, TupleDataLayout DSA
 * pointers) and reconstruct process-local operator objects on workers. Layout
 * construction itself moves to Step 10 translator work, where Plan* is in
 * scope; descriptor code remains plan-agnostic.
 *
 *-------------------------------------------------------------------------
 */

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <algorithm>
#include <cstring>

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/output_sink.hpp"
#include "parallel/pipeline/physical_cross_product.hpp"
#include "parallel/pipeline/physical_delim_scan.hpp"
#include "parallel/pipeline/physical_filter.hpp"
#include "parallel/pipeline/physical_hash_aggregate.hpp"
#include "parallel/pipeline/physical_hash_join.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/physical_order.hpp"
#include "parallel/pipeline/physical_perfect_hash_aggregate.hpp"
#include "parallel/pipeline/physical_projection.hpp"
#include "parallel/pipeline/physical_seq_scan.hpp"
#include "parallel/pipeline/physical_top_n.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

static uint64
DependencyMask(const Pipeline &pipeline)
{
	uint64 mask = 0;
	for (PipelineId dep : pipeline.depends_on)
	{
		Assert(dep < 64);
		mask |= (UINT64_C(1) << dep);
	}
	return mask;
}

static void
SerializeUInt16Vector(const PgVector<uint16_t> &values,
					   dsa_area *dsa,
					   dsa_pointer *out_dp,
					   uint16_t *out_count)
{
	*out_count = static_cast<uint16_t>(values.size());
	if (values.empty())
	{
		*out_dp = InvalidDsaPointer;
		return;
	}

	*out_dp = dsa_allocate0(dsa, sizeof(uint16_t) * values.size());
	std::memcpy(dsa_get_address(dsa, *out_dp), values.data(), sizeof(uint16_t) * values.size());
}

static void
SerializeAggFuncVector(const PgVector<AggFuncDesc> &values,
					 dsa_area *dsa,
					 dsa_pointer *out_dp,
					 uint16_t *out_count)
{
	*out_count = static_cast<uint16_t>(values.size());
	if (values.empty())
	{
		*out_dp = InvalidDsaPointer;
		return;
	}

	*out_dp = dsa_allocate0(dsa, sizeof(AggFuncDesc) * values.size());
	std::memcpy(dsa_get_address(dsa, *out_dp), values.data(), sizeof(AggFuncDesc) * values.size());
}

static void
SerializeProjectExprVector(const PgVector<ProjectExprDesc> &values,
					   dsa_area *dsa,
					   dsa_pointer *out_dp,
					   uint16_t *out_count)
{
	*out_count = static_cast<uint16_t>(values.size());
	if (values.empty())
	{
		*out_dp = InvalidDsaPointer;
		return;
	}

	*out_dp = dsa_allocate0(dsa, sizeof(ProjectExprDesc) * values.size());
	std::memcpy(dsa_get_address(dsa, *out_dp), values.data(), sizeof(ProjectExprDesc) * values.size());
}

static void
SerializeProjectStepVector(const PgVector<ProjectStep> &values,
					   dsa_area *dsa,
					   dsa_pointer *out_dp,
					   uint16_t *out_count)
{
	*out_count = static_cast<uint16_t>(values.size());
	if (values.empty())
	{
		*out_dp = InvalidDsaPointer;
		return;
	}

	*out_dp = dsa_allocate0(dsa, sizeof(ProjectStep) * values.size());
	std::memcpy(dsa_get_address(dsa, *out_dp), values.data(), sizeof(ProjectStep) * values.size());
}

static void
SerializeFilterInputVector(const PgVector<FilterInputDesc> &values,
					   dsa_area *dsa,
					   dsa_pointer *out_dp,
					   uint16_t *out_count)
{
	*out_count = static_cast<uint16_t>(values.size());
	if (values.empty())
	{
		*out_dp = InvalidDsaPointer;
		return;
	}

	*out_dp = dsa_allocate0(dsa, sizeof(FilterInputDesc) * values.size());
	std::memcpy(dsa_get_address(dsa, *out_dp), values.data(), sizeof(FilterInputDesc) * values.size());
}

static void
SerializeFilterExprVector(const PgVector<FilterExprDesc> &values,
					  dsa_area *dsa,
					  dsa_pointer *out_dp,
					  uint16_t *out_count)
{
	*out_count = static_cast<uint16_t>(values.size());
	if (values.empty())
	{
		*out_dp = InvalidDsaPointer;
		return;
	}

	*out_dp = dsa_allocate0(dsa, sizeof(FilterExprDesc) * values.size());
	std::memcpy(dsa_get_address(dsa, *out_dp), values.data(), sizeof(FilterExprDesc) * values.size());
}

static void
SerializeFilterStepVector(const PgVector<FilterStep> &values,
					  dsa_area *dsa,
					  dsa_pointer *out_dp,
					  uint16_t *out_count)
{
	*out_count = static_cast<uint16_t>(values.size());
	if (values.empty())
	{
		*out_dp = InvalidDsaPointer;
		return;
	}

	*out_dp = dsa_allocate0(dsa, sizeof(FilterStep) * values.size());
	std::memcpy(dsa_get_address(dsa, *out_dp), values.data(), sizeof(FilterStep) * values.size());
}

static void
SerializeCharVector(const PgVector<char> &values,
				dsa_area *dsa,
				dsa_pointer *out_dp,
				uint32_t *out_bytes)
{
	*out_bytes = static_cast<uint32_t>(values.size());
	if (values.empty())
	{
		*out_dp = InvalidDsaPointer;
		return;
	}

	*out_dp = dsa_allocate0(dsa, values.size());
	std::memcpy(dsa_get_address(dsa, *out_dp), values.data(), values.size());
}

static uint16_t
RequiredFilterBoolRegs(const PgVector<FilterExprDesc> &exprs)
{
	uint16_t max_reg = 0;
	for (const FilterExprDesc &expr : exprs)
		max_reg = std::max<uint16_t>(max_reg, static_cast<uint16_t>(expr.output_bool_reg + 1));
	return max_reg;
}

static void
EmitSeqScan(const PhysicalSeqScan &op, OpDescriptor &out)
{
	out.kind = OpKind::SEQ_SCAN;
	out.n_children = 0;
	out.body.seq_scan.relid = op.relid();
	out.body.seq_scan.input_schema = op.input_schema_dp();
	out.body.seq_scan.output_schema = op.output_schema_dp();
	out.body.seq_scan.filter_inputs = op.filter_inputs_dp();
	out.body.seq_scan.filter_exprs = op.filter_exprs_dp();
	out.body.seq_scan.filter_steps = op.filter_steps_dp();
	out.body.seq_scan.filter_string_consts = op.filter_string_consts_dp();
	out.body.seq_scan.shared_payload = op.shared_payload_dp();
	out.body.seq_scan.n_filter_inputs = op.n_filter_inputs();
	out.body.seq_scan.n_filter_exprs = op.n_filter_exprs();
	out.body.seq_scan.n_filter_steps = op.n_filter_steps();
	out.body.seq_scan.filter_bool_regs = op.filter_bool_regs();
	out.body.seq_scan.filter_string_const_bytes = op.filter_string_const_bytes();
}

static void
EmitDelimScan(const PhysicalDelimScan &op, OpDescriptor &out)
{
	out.kind = OpKind::DELIM_SCAN;
	out.n_children = 0;
	out.body.delim_scan.input_schema = op.input_schema_dp();
	out.body.delim_scan.shared_payload = op.shared_payload_dp();
}

static void
EmitHashAgg(const PhysicalHashAggregate &op, OpDescriptor &out, dsa_area *dsa)
{
	out.kind = op.type() == PhysicalOperatorType::PERFECT_HASH_AGGREGATE
		? OpKind::PERFECT_HASH_AGGREGATE
		: OpKind::HASH_AGGREGATE;
	out.n_children = 0;
	HashAggOpBody &body = out.kind == OpKind::PERFECT_HASH_AGGREGATE
		? out.body.perfect_hash_agg
		: out.body.hash_agg;
	body.input_schema = InvalidDsaPointer;
	body.output_schema = InvalidDsaPointer;
	body.layout = op.layout_dp();
	body.shared_payload = op.shared_payload_dp();
	body.max_groups = op.max_groups();
	body.perfect_hash_capacity = op.perfect_hash_capacity();
	SerializeUInt16Vector(op.group_keys(), dsa, &body.group_keys, &body.n_group_keys);
	SerializeAggFuncVector(op.agg_funcs(), dsa, &body.agg_funcs, &body.n_agg_funcs);
}

static void
EmitOrder(const PhysicalOrder &op, OpDescriptor &out, dsa_area *dsa)
{
	(void) dsa;
	out.kind = OpKind::ORDER;
	out.n_children = 0;
	out.body.order.input_schema = InvalidDsaPointer;
	out.body.order.sort_keys = op.sort_keys_dp();
	out.body.order.n_sort_keys = op.n_sort_keys();
	out.body.order.key_layout = op.key_layout_dp();
	out.body.order.payload_layout = op.payload_layout_dp();
	out.body.order.shared_payload = op.shared_payload_dp();
	out.body.order._pad0 = 0;
	out.body.order.max_rows = op.max_rows();
}

static void
EmitTopN(const PhysicalTopN &op, OpDescriptor &out)
{
	out.kind = OpKind::TOP_N;
	out.n_children = 0;
	out.body.top_n.input_schema = op.input_schema_dp();
	out.body.top_n.layout = op.layout_dp();
	out.body.top_n.sort_keys = op.sort_keys_dp();
	out.body.top_n.shared_payload = op.shared_payload_dp();
	out.body.top_n.sort_indices = InvalidDsaPointer;
	out.body.top_n.n_sort_keys = op.n_sort_keys();
	out.body.top_n._pad0 = 0;
	out.body.top_n.max_rows = op.max_rows();
}

static void
EmitOutput(const OutputSink &op, OpDescriptor &out)
{
	out.kind = OpKind::OUTPUT;
	out.n_children = 0;
	out.body.output.input_schema = op.input_schema_dp();
	out.body.output.layout = op.layout_dp();
	out.body.output.sort_keys = op.final_sort_keys_dp();
	out.body.output.n_sort_keys = op.n_final_sort_keys();
	out.body.output.shared_payload = op.shared_payload_dp();
	out.body.output.tdc_max_rows = op.tdc_max_rows();
	out.body.output._pad0 = 0;
	out.body.output.max_emit_rows = op.max_emit_rows();
}

static void
EmitFilter(const PhysicalFilter &op, OpDescriptor &out, dsa_area *dsa)
{
	out.kind = OpKind::FILTER;
	out.n_children = 0;
	out.body.filter.input_schema = op.input_schema_dp();
	SerializeFilterInputVector(op.filter_inputs(), dsa,
		&out.body.filter.filter_inputs, &out.body.filter.n_filter_inputs);
	SerializeFilterExprVector(op.filter_exprs(), dsa,
		&out.body.filter.filter_exprs, &out.body.filter.n_filter_exprs);
	SerializeFilterStepVector(op.filter_steps(), dsa,
		&out.body.filter.filter_steps, &out.body.filter.n_filter_steps);
	SerializeCharVector(op.filter_string_consts(), dsa,
		&out.body.filter.filter_string_consts,
		&out.body.filter.filter_string_const_bytes);
	out.body.filter.filter_bool_regs = RequiredFilterBoolRegs(op.filter_exprs());
}

static void
EmitProjection(const PhysicalProjection &op, OpDescriptor &out, dsa_area *dsa)
{
	out.kind = OpKind::PROJECTION;
	out.n_children = 0;
	out.body.project.input_schema = op.input_schema_dp();
	out.body.project.output_schema = op.output_schema_dp();
	SerializeProjectExprVector(op.expr_descs(), dsa,
		&out.body.project.expr_descs, &out.body.project.n_exprs);
	SerializeProjectStepVector(op.steps(), dsa,
		&out.body.project.steps, &out.body.project.n_steps_total);
	out.body.project._pad0 = 0;
}

static void
EmitHashJoin(const PhysicalHashJoin &op, OpDescriptor &out)
{
	out.kind = OpKind::HASH_JOIN;
	out.n_children = 0;
	out.body.hash_join.left_input_schema = op.left_input_schema_dp();
	out.body.hash_join.right_input_schema = op.right_input_schema_dp();
	out.body.hash_join.output_schema = op.output_schema_dp();
	out.body.hash_join.left_key_layout = op.left_key_layout_dp();
	out.body.hash_join.right_key_layout = op.right_key_layout_dp();
	out.body.hash_join.left_payload_layout = op.left_payload_layout_dp();
	out.body.hash_join.right_payload_layout = op.right_payload_layout_dp();
	out.body.hash_join.output_columns = op.output_columns_dp();
	out.body.hash_join.filter_inputs = op.filter_inputs_dp();
	out.body.hash_join.filter_exprs = op.filter_exprs_dp();
	out.body.hash_join.filter_steps = op.filter_steps_dp();
	out.body.hash_join.filter_string_consts = op.filter_string_consts_dp();
	out.body.hash_join.shared_payload = op.shared_payload_dp();
	out.body.hash_join.n_left_keys = op.n_left_keys();
	out.body.hash_join.n_right_keys = op.n_right_keys();
	out.body.hash_join.output_column_count = op.output_column_count();
	out.body.hash_join.n_filter_inputs = op.n_filter_inputs();
	out.body.hash_join.n_filter_exprs = op.n_filter_exprs();
	out.body.hash_join.n_filter_steps = op.n_filter_steps();
	out.body.hash_join.filter_bool_regs = op.filter_bool_regs();
	out.body.hash_join.join_mode = op.join_mode();
	out.body.hash_join._pad0[0] = 0;
	out.body.hash_join.filter_string_const_bytes = op.filter_string_const_bytes();
	out.body.hash_join.max_rows = op.max_rows();
}

static void
EmitCrossProduct(const PhysicalCrossProduct &op, OpDescriptor &out)
{
	out.kind = OpKind::CROSS_PRODUCT;
	out.n_children = 0;
	out.body.cross_product.left_input_schema = op.left_input_schema_dp();
	out.body.cross_product.right_input_schema = op.right_input_schema_dp();
	out.body.cross_product.output_schema = op.output_schema_dp();
	out.body.cross_product.right_payload_layout = op.right_payload_layout_dp();
	out.body.cross_product.output_columns = op.output_columns_dp();
	out.body.cross_product.shared_payload = op.shared_payload_dp();
	out.body.cross_product.output_column_count = op.output_column_count();
	out.body.cross_product._pad0 = 0;
	out.body.cross_product.max_rows = op.max_rows();
}

static std::unique_ptr<PhysicalOperator>
ReconstructOp(const OpDescriptor &op, ExecCtx &ctx)
{
	(void) ctx;
	switch (op.kind)
	{
		case OpKind::SEQ_SCAN:
			return std::make_unique<PhysicalSeqScan>(
				op.body.seq_scan.relid,
				op.body.seq_scan.input_schema,
				op.body.seq_scan.output_schema,
				op.body.seq_scan.filter_inputs,
				op.body.seq_scan.filter_exprs,
				op.body.seq_scan.filter_steps,
				op.body.seq_scan.filter_string_consts,
				op.body.seq_scan.n_filter_inputs,
				op.body.seq_scan.n_filter_exprs,
				op.body.seq_scan.n_filter_steps,
				op.body.seq_scan.filter_bool_regs,
				op.body.seq_scan.filter_string_const_bytes,
				op.body.seq_scan.shared_payload,
				const_cast<OpDescriptor *>(&op));

		case OpKind::DELIM_SCAN:
			return std::make_unique<PhysicalDelimScan>(
				op.body.delim_scan.input_schema,
				op.body.delim_scan.shared_payload,
				nullptr,
				const_cast<OpDescriptor *>(&op));

		case OpKind::HASH_AGGREGATE:
		case OpKind::PERFECT_HASH_AGGREGATE:
		{
			const HashAggOpBody &body = op.kind == OpKind::PERFECT_HASH_AGGREGATE
				? op.body.perfect_hash_agg
				: op.body.hash_agg;
			PgVector<uint16_t> group_keys;
			if (body.n_group_keys > 0 && DsaPointerIsValid(body.group_keys))
			{
				auto *keys = static_cast<uint16_t *>(dsa_get_address(ctx.dsa, body.group_keys));
				group_keys.assign(keys, keys + body.n_group_keys);
			}

			PgVector<AggFuncDesc> agg_funcs;
			if (body.n_agg_funcs > 0 && DsaPointerIsValid(body.agg_funcs))
			{
				auto *aggs = static_cast<AggFuncDesc *>(dsa_get_address(ctx.dsa, body.agg_funcs));
				agg_funcs.assign(aggs, aggs + body.n_agg_funcs);
			}

			if (op.kind == OpKind::PERFECT_HASH_AGGREGATE)
				return std::make_unique<PhysicalPerfectHashAggregate>(
					body.layout,
					std::move(group_keys),
					std::move(agg_funcs),
					body.shared_payload,
					body.max_groups,
					body.perfect_hash_capacity,
					const_cast<OpDescriptor *>(&op));

			return std::make_unique<PhysicalHashAggregate>(
				body.layout,
				std::move(group_keys),
				std::move(agg_funcs),
				body.shared_payload,
				body.max_groups,
				body.perfect_hash_capacity,
				const_cast<OpDescriptor *>(&op));
		}

			case OpKind::ORDER:
				return std::make_unique<PhysicalOrder>(
					op.body.order.sort_keys,
					op.body.order.n_sort_keys,
					op.body.order.key_layout,
					op.body.order.payload_layout,
					op.body.order.shared_payload,
					op.body.order.max_rows,
					const_cast<OpDescriptor *>(&op));

			case OpKind::TOP_N:
				return std::make_unique<PhysicalTopN>(
					op.body.top_n.input_schema,
					op.body.top_n.layout,
					op.body.top_n.sort_keys,
					op.body.top_n.n_sort_keys,
					op.body.top_n.shared_payload,
					op.body.top_n.max_rows,
					const_cast<OpDescriptor *>(&op));

			case OpKind::OUTPUT:
				return std::make_unique<OutputSink>(
				op.body.output.input_schema,
				op.body.output.layout,
				op.body.output.sort_keys,
				op.body.output.n_sort_keys,
				op.body.output.shared_payload,
				op.body.output.tdc_max_rows,
				op.body.output.max_emit_rows,
				const_cast<OpDescriptor *>(&op));

		case OpKind::FILTER:
		{
			PgVector<FilterInputDesc> filter_inputs;
			if (op.body.filter.n_filter_inputs > 0 && DsaPointerIsValid(op.body.filter.filter_inputs))
			{
				auto *inputs = static_cast<FilterInputDesc *>(dsa_get_address(ctx.dsa, op.body.filter.filter_inputs));
				filter_inputs.assign(inputs, inputs + op.body.filter.n_filter_inputs);
			}

			PgVector<FilterExprDesc> filter_exprs;
			if (op.body.filter.n_filter_exprs > 0 && DsaPointerIsValid(op.body.filter.filter_exprs))
			{
				auto *exprs = static_cast<FilterExprDesc *>(dsa_get_address(ctx.dsa, op.body.filter.filter_exprs));
				filter_exprs.assign(exprs, exprs + op.body.filter.n_filter_exprs);
			}

			PgVector<FilterStep> filter_steps;
			if (op.body.filter.n_filter_steps > 0 && DsaPointerIsValid(op.body.filter.filter_steps))
			{
				auto *steps = static_cast<FilterStep *>(dsa_get_address(ctx.dsa, op.body.filter.filter_steps));
				filter_steps.assign(steps, steps + op.body.filter.n_filter_steps);
			}

			PgVector<char> filter_string_consts;
			if (op.body.filter.filter_string_const_bytes > 0 && DsaPointerIsValid(op.body.filter.filter_string_consts))
			{
				auto *bytes = static_cast<char *>(dsa_get_address(ctx.dsa, op.body.filter.filter_string_consts));
				filter_string_consts.assign(bytes, bytes + op.body.filter.filter_string_const_bytes);
			}

			return std::make_unique<PhysicalFilter>(
				op.body.filter.input_schema,
				std::move(filter_inputs),
				std::move(filter_exprs),
				std::move(filter_steps),
				std::move(filter_string_consts),
				op.body.filter.filter_inputs,
				op.body.filter.filter_exprs,
				op.body.filter.filter_steps,
				op.body.filter.filter_string_consts,
				const_cast<OpDescriptor *>(&op));
		}

		case OpKind::PROJECTION:
		{
			PgVector<ProjectExprDesc> expr_descs;
			if (op.body.project.n_exprs > 0 && DsaPointerIsValid(op.body.project.expr_descs))
			{
				auto *exprs = static_cast<ProjectExprDesc *>(dsa_get_address(ctx.dsa, op.body.project.expr_descs));
				expr_descs.assign(exprs, exprs + op.body.project.n_exprs);
			}

			PgVector<ProjectStep> steps;
			if (op.body.project.n_steps_total > 0 && DsaPointerIsValid(op.body.project.steps))
			{
				auto *step_ptr = static_cast<ProjectStep *>(dsa_get_address(ctx.dsa, op.body.project.steps));
				steps.assign(step_ptr, step_ptr + op.body.project.n_steps_total);
			}

			return std::make_unique<PhysicalProjection>(
				op.body.project.input_schema,
				op.body.project.output_schema,
				std::move(expr_descs),
				std::move(steps),
				op.body.project.expr_descs,
				op.body.project.steps,
				const_cast<OpDescriptor *>(&op));
		}

		case OpKind::HASH_JOIN:
			return std::make_unique<PhysicalHashJoin>(
				op.body.hash_join.left_input_schema,
				op.body.hash_join.right_input_schema,
				op.body.hash_join.output_schema,
				op.body.hash_join.left_key_layout,
				op.body.hash_join.right_key_layout,
				op.body.hash_join.left_payload_layout,
				op.body.hash_join.right_payload_layout,
				op.body.hash_join.output_columns,
				op.body.hash_join.output_column_count,
				op.body.hash_join.filter_inputs,
				op.body.hash_join.filter_exprs,
				op.body.hash_join.filter_steps,
				op.body.hash_join.filter_string_consts,
				op.body.hash_join.n_filter_inputs,
				op.body.hash_join.n_filter_exprs,
				op.body.hash_join.n_filter_steps,
				op.body.hash_join.filter_bool_regs,
				op.body.hash_join.join_mode,
				op.body.hash_join.filter_string_const_bytes,
				op.body.hash_join.shared_payload,
				op.body.hash_join.n_left_keys,
				op.body.hash_join.n_right_keys,
				op.body.hash_join.max_rows,
				const_cast<OpDescriptor *>(&op));

		case OpKind::CROSS_PRODUCT:
			return std::make_unique<PhysicalCrossProduct>(
				op.body.cross_product.left_input_schema,
				op.body.cross_product.right_input_schema,
				op.body.cross_product.output_schema,
				op.body.cross_product.right_payload_layout,
				op.body.cross_product.output_columns,
				op.body.cross_product.output_column_count,
				op.body.cross_product.shared_payload,
				op.body.cross_product.max_rows,
				const_cast<OpDescriptor *>(&op));
	}

	elog(ERROR, "pg_yaap: unknown OpKind %u during reconstruction", (unsigned) op.kind);
	return nullptr;
}

}  /* namespace */

dsa_pointer
SerializeTupleDataLayout(const TupleDataLayout &layout, dsa_area *dsa)
{
	dsa_pointer layout_dp = dsa_allocate0(dsa, sizeof(TupleDataLayout));
	std::memcpy(dsa_get_address(dsa, layout_dp), &layout, sizeof(TupleDataLayout));
	return layout_dp;
}

dsa_pointer
LeaderSerializePipelines(MetaPipelineBundle &bundle, dsa_area *dsa)
{
	if (bundle.pipelines.empty())
		return InvalidDsaPointer;

	const size_t pipeline_count = bundle.pipelines.size();
	dsa_pointer root_dp = dsa_allocate0(dsa, sizeof(PipelineDescriptor) * pipeline_count);
	auto *root = static_cast<PipelineDescriptor *>(dsa_get_address(dsa, root_dp));

	for (const auto &pipeline_uptr : bundle.pipelines)
	{
		const Pipeline &pipeline = *pipeline_uptr;
		PipelineDescriptor &pd = root[pipeline.id];
		pd.pipeline_id = pipeline.id;
		pd.op_count = 2 + static_cast<int32>(pipeline.ops.size());
		pd.dependency_mask = DependencyMask(pipeline);
		pd.global_source_state = InvalidDsaPointer;
		pd.global_sink_state = InvalidDsaPointer;
		pg_atomic_init_u32(&pd.task_slot_next, 0);

		pd.ops = dsa_allocate0(dsa, sizeof(OpDescriptor) * pd.op_count);
		auto *ops = static_cast<OpDescriptor *>(dsa_get_address(dsa, pd.ops));

		int32 idx = 0;
		switch (pipeline.source->type())
		{
			case PhysicalOperatorType::SEQ_SCAN:
				EmitSeqScan(static_cast<const PhysicalSeqScan &>(*pipeline.source), ops[idx]);
				static_cast<PhysicalSeqScan &>(*pipeline.source).AttachDescriptor(&ops[idx]);
				idx++;
				break;
			case PhysicalOperatorType::DELIM_SCAN:
				EmitDelimScan(static_cast<const PhysicalDelimScan &>(*pipeline.source), ops[idx]);
				static_cast<PhysicalDelimScan &>(*pipeline.source).AttachDescriptor(&ops[idx]);
				idx++;
				break;
			case PhysicalOperatorType::HASH_AGGREGATE:
			case PhysicalOperatorType::PERFECT_HASH_AGGREGATE:
				EmitHashAgg(static_cast<const PhysicalHashAggregate &>(*pipeline.source), ops[idx], dsa);
				static_cast<PhysicalHashAggregate &>(*pipeline.source).AttachDescriptor(&ops[idx]);
				idx++;
				break;
			case PhysicalOperatorType::ORDER:
				EmitOrder(static_cast<const PhysicalOrder &>(*pipeline.source), ops[idx], dsa);
				static_cast<PhysicalOrder &>(*pipeline.source).AttachDescriptor(&ops[idx]);
				idx++;
				break;
			case PhysicalOperatorType::TOP_N:
				EmitTopN(static_cast<const PhysicalTopN &>(*pipeline.source), ops[idx]);
				static_cast<PhysicalTopN &>(*pipeline.source).AttachDescriptor(&ops[idx]);
				idx++;
				break;
			case PhysicalOperatorType::FILTER:
				EmitFilter(static_cast<const PhysicalFilter &>(*pipeline.source), ops[idx++], dsa);
				break;
			case PhysicalOperatorType::PROJECTION:
				EmitProjection(static_cast<const PhysicalProjection &>(*pipeline.source), ops[idx++], dsa);
				break;
			case PhysicalOperatorType::HASH_JOIN:
				EmitHashJoin(static_cast<const PhysicalHashJoin &>(*pipeline.source), ops[idx]);
				static_cast<PhysicalHashJoin &>(*pipeline.source).AttachDescriptor(&ops[idx]);
				idx++;
				break;
			case PhysicalOperatorType::CROSS_PRODUCT:
				EmitCrossProduct(static_cast<const PhysicalCrossProduct &>(*pipeline.source), ops[idx]);
				static_cast<PhysicalCrossProduct &>(*pipeline.source).AttachDescriptor(&ops[idx]);
				idx++;
				break;
			default:
				elog(ERROR, "pg_yaap: unsupported source operator type %u", (unsigned) pipeline.source->type());
		}

		for (PhysicalOperator *mid : pipeline.ops)
		{
			switch (mid->type())
			{
				case PhysicalOperatorType::HASH_AGGREGATE:
				case PhysicalOperatorType::PERFECT_HASH_AGGREGATE:
					EmitHashAgg(static_cast<const PhysicalHashAggregate &>(*mid), ops[idx], dsa);
					static_cast<PhysicalHashAggregate *>(mid)->AttachDescriptor(&ops[idx]);
					idx++;
					break;
				case PhysicalOperatorType::FILTER:
					EmitFilter(static_cast<const PhysicalFilter &>(*mid), ops[idx++], dsa);
					break;
				case PhysicalOperatorType::PROJECTION:
					EmitProjection(static_cast<const PhysicalProjection &>(*mid), ops[idx++], dsa);
					break;
				case PhysicalOperatorType::HASH_JOIN:
					EmitHashJoin(static_cast<const PhysicalHashJoin &>(*mid), ops[idx]);
					static_cast<PhysicalHashJoin *>(mid)->AttachDescriptor(&ops[idx]);
					idx++;
					break;
				case PhysicalOperatorType::CROSS_PRODUCT:
					EmitCrossProduct(static_cast<const PhysicalCrossProduct &>(*mid), ops[idx]);
					static_cast<PhysicalCrossProduct *>(mid)->AttachDescriptor(&ops[idx]);
					idx++;
					break;
				default:
					elog(ERROR, "pg_yaap: unsupported mid-pipeline operator type %u", (unsigned) mid->type());
			}
		}

		switch (pipeline.sink->type())
		{
			case PhysicalOperatorType::HASH_AGGREGATE:
			case PhysicalOperatorType::PERFECT_HASH_AGGREGATE:
				EmitHashAgg(static_cast<const PhysicalHashAggregate &>(*pipeline.sink), ops[idx], dsa);
				static_cast<PhysicalHashAggregate &>(*pipeline.sink).AttachDescriptor(&ops[idx]);
				break;
			case PhysicalOperatorType::ORDER:
				EmitOrder(static_cast<const PhysicalOrder &>(*pipeline.sink), ops[idx], dsa);
				static_cast<PhysicalOrder &>(*pipeline.sink).AttachDescriptor(&ops[idx]);
				break;
			case PhysicalOperatorType::TOP_N:
				EmitTopN(static_cast<const PhysicalTopN &>(*pipeline.sink), ops[idx]);
				static_cast<PhysicalTopN &>(*pipeline.sink).AttachDescriptor(&ops[idx]);
				break;
			case PhysicalOperatorType::OUTPUT:
				EmitOutput(static_cast<const OutputSink &>(*pipeline.sink), ops[idx]);
				static_cast<OutputSink &>(*pipeline.sink).AttachDescriptor(&ops[idx]);
				break;
			case PhysicalOperatorType::HASH_JOIN:
				EmitHashJoin(static_cast<const PhysicalHashJoin &>(*pipeline.sink), ops[idx]);
				static_cast<PhysicalHashJoin &>(*pipeline.sink).AttachDescriptor(&ops[idx]);
				break;
			case PhysicalOperatorType::CROSS_PRODUCT:
				EmitCrossProduct(static_cast<const PhysicalCrossProduct &>(*pipeline.sink), ops[idx]);
				static_cast<PhysicalCrossProduct &>(*pipeline.sink).AttachDescriptor(&ops[idx]);
				break;
			default:
				elog(ERROR, "pg_yaap: unsupported sink operator type %u", (unsigned) pipeline.sink->type());
		}
	}

	return root_dp;
}

void
WorkerReconstructPipelines(PipelineSharedControl *ctl,
				   ExecCtx &worker_ctx,
				   PgVector<std::unique_ptr<Pipeline>> &out)
{
	if (ctl == nullptr || ctl->pipelines_root == InvalidDsaPointer || ctl->num_pipelines <= 0)
		return;

	auto *root = static_cast<PipelineDescriptor *>(dsa_get_address(worker_ctx.dsa, ctl->pipelines_root));
	for (int32 idx = 0; idx < ctl->num_pipelines; ++idx)
	{
		PipelineDescriptor &pd = root[idx];
		auto pipeline = std::make_unique<Pipeline>();
		pipeline->id = static_cast<PipelineId>(pd.pipeline_id);

		auto *ops = static_cast<OpDescriptor *>(dsa_get_address(worker_ctx.dsa, pd.ops));
		pipeline->source = ReconstructOp(ops[0], worker_ctx).release();
		for (int32 op_idx = 1; op_idx < pd.op_count - 1; ++op_idx)
			pipeline->ops.push_back(ReconstructOp(ops[op_idx], worker_ctx).release());
		pipeline->sink = ReconstructOp(ops[pd.op_count - 1], worker_ctx).release();

		for (PipelineId dep = 0; dep < 64; ++dep)
			if (pd.dependency_mask & (UINT64_C(1) << dep))
				pipeline->depends_on.push_back(dep);

		out.push_back(std::move(pipeline));
	}
}

void
StoreSharedPayloadOnDescriptor(const PhysicalOperator *op, dsa_pointer payload_dp)
{
	if (op == nullptr)
		return;

	/*
	 * Fix A2: fan out the payload pointer to EVERY DSA OpDescriptor slot this
	 * operator was attached to. The same C++ instance can appear in multiple
	 * pipelines (e.g. HashAgg as P_producer.sink and P_consumer.source), and
	 * LeaderSerializePipelines allocated an independent slot per pipeline.
	 * Workers reconstruct from per-pipeline slots, so a single-slot Store
	 * leaves the other pipelines' workers reading InvalidDsaPointer.
	 * See physical_hash_aggregate.hpp AttachDescriptor for the contract.
	 */
	switch (op->type())
	{
		case PhysicalOperatorType::SEQ_SCAN:
		{
			for (OpDescriptor *desc : static_cast<const PhysicalSeqScan *>(op)->descs())
				if (desc != nullptr)
					desc->body.seq_scan.shared_payload = payload_dp;
			break;
		}
		case PhysicalOperatorType::DELIM_SCAN:
		{
			for (OpDescriptor *desc : static_cast<const PhysicalDelimScan *>(op)->descs())
				if (desc != nullptr)
					desc->body.delim_scan.shared_payload = payload_dp;
			break;
		}
		case PhysicalOperatorType::HASH_AGGREGATE:
		case PhysicalOperatorType::PERFECT_HASH_AGGREGATE:
		{
			const auto &dl = static_cast<const PhysicalHashAggregate *>(op)->descs();
			for (OpDescriptor *desc : dl)
			{
				if (desc != nullptr)
				{
					if (desc->kind == OpKind::PERFECT_HASH_AGGREGATE)
						desc->body.perfect_hash_agg.shared_payload = payload_dp;
					else
						desc->body.hash_agg.shared_payload = payload_dp;
				}
			}
			break;
		}
		case PhysicalOperatorType::ORDER:
		{
			const auto *order = static_cast<const PhysicalOrder *>(op);
			bool stored = false;
			for (OpDescriptor *desc : order->descs())
				if (desc != nullptr)
				{
					desc->body.order.shared_payload = payload_dp;
					stored = true;
				}
			if (!stored && order->desc() != nullptr)
				order->desc()->body.order.shared_payload = payload_dp;
			break;
		}
		case PhysicalOperatorType::TOP_N:
		{
			const auto *topn = static_cast<const PhysicalTopN *>(op);
			bool stored = false;
			for (OpDescriptor *desc : topn->descs())
				if (desc != nullptr)
				{
					desc->body.top_n.shared_payload = payload_dp;
					stored = true;
				}
			if (!stored && topn->desc() != nullptr)
				topn->desc()->body.top_n.shared_payload = payload_dp;
			break;
		}
		case PhysicalOperatorType::OUTPUT:
		{
			const auto *output = static_cast<const OutputSink *>(op);
			bool stored = false;
			for (OpDescriptor *desc : output->descs())
				if (desc != nullptr)
				{
					desc->body.output.shared_payload = payload_dp;
					stored = true;
				}
			if (!stored && output->desc() != nullptr)
				output->desc()->body.output.shared_payload = payload_dp;
			break;
		}
		case PhysicalOperatorType::HASH_JOIN:
		{
			const auto *join = static_cast<const PhysicalHashJoin *>(op);
			bool stored = false;
			for (OpDescriptor *desc : join->descs())
				if (desc != nullptr)
				{
					desc->body.hash_join.shared_payload = payload_dp;
					stored = true;
				}
			if (!stored && join->desc() != nullptr)
				join->desc()->body.hash_join.shared_payload = payload_dp;
			break;
		}
		case PhysicalOperatorType::CROSS_PRODUCT:
		{
			const auto *join = static_cast<const PhysicalCrossProduct *>(op);
			bool stored = false;
			for (OpDescriptor *desc : join->descs())
				if (desc != nullptr)
				{
					desc->body.cross_product.shared_payload = payload_dp;
					stored = true;
				}
			if (!stored && join->desc() != nullptr)
				join->desc()->body.cross_product.shared_payload = payload_dp;
			break;
		}
		case PhysicalOperatorType::FILTER:
		case PhysicalOperatorType::PROJECTION:
			break;
	}
}

void
ClearSharedPayloadOnDescriptor(const PhysicalOperator *op)
{
	StoreSharedPayloadOnDescriptor(op, InvalidDsaPointer);
}

dsa_pointer
LoadSharedPayloadFromDescriptor(const PhysicalOperator *op)
{
	if (op == nullptr)
		return InvalidDsaPointer;

	switch (op->type())
	{
		case PhysicalOperatorType::SEQ_SCAN:
		{
			OpDescriptor *desc = static_cast<const PhysicalSeqScan *>(op)->desc();
			return desc != nullptr ? desc->body.seq_scan.shared_payload : InvalidDsaPointer;
		}
		case PhysicalOperatorType::DELIM_SCAN:
		{
			OpDescriptor *desc = static_cast<const PhysicalDelimScan *>(op)->desc();
			return desc != nullptr ? desc->body.delim_scan.shared_payload : InvalidDsaPointer;
		}
		case PhysicalOperatorType::HASH_AGGREGATE:
		case PhysicalOperatorType::PERFECT_HASH_AGGREGATE:
		{
			OpDescriptor *desc = static_cast<const PhysicalHashAggregate *>(op)->desc();
			dsa_pointer ret = InvalidDsaPointer;
			if (desc != nullptr)
				ret = desc->kind == OpKind::PERFECT_HASH_AGGREGATE
					? desc->body.perfect_hash_agg.shared_payload
					: desc->body.hash_agg.shared_payload;
			return ret;
		}
		case PhysicalOperatorType::ORDER:
		{
			OpDescriptor *desc = static_cast<const PhysicalOrder *>(op)->desc();
			return desc != nullptr ? desc->body.order.shared_payload : InvalidDsaPointer;
		}
		case PhysicalOperatorType::TOP_N:
		{
			OpDescriptor *desc = static_cast<const PhysicalTopN *>(op)->desc();
			return desc != nullptr ? desc->body.top_n.shared_payload : InvalidDsaPointer;
		}
		case PhysicalOperatorType::OUTPUT:
		{
			OpDescriptor *desc = static_cast<const OutputSink *>(op)->desc();
			return desc != nullptr ? desc->body.output.shared_payload : InvalidDsaPointer;
		}
		case PhysicalOperatorType::HASH_JOIN:
		{
			OpDescriptor *desc = static_cast<const PhysicalHashJoin *>(op)->desc();
			return desc != nullptr ? desc->body.hash_join.shared_payload : InvalidDsaPointer;
		}
		case PhysicalOperatorType::CROSS_PRODUCT:
		{
			OpDescriptor *desc = static_cast<const PhysicalCrossProduct *>(op)->desc();
			return desc != nullptr ? desc->body.cross_product.shared_payload : InvalidDsaPointer;
		}
		case PhysicalOperatorType::FILTER:
		case PhysicalOperatorType::PROJECTION:
			return InvalidDsaPointer;
	}

	return InvalidDsaPointer;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */
