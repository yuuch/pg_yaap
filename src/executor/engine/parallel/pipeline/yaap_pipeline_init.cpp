#include "parallel/pipeline/yaap_pipeline_init_internal.hpp"

namespace pg_yaap {

using namespace optimizer_translator_detail;

namespace {

static bool
BuildFinalSortKeys(const PhysicalOrderBy &order,
				   QueryDesc *queryDesc,
				   const std::vector<yaap::PhysicalOperator::OutputColumn> *outputs,
				   const std::vector<ColumnRef> &cols,
				   const std::vector<ColumnSchema> &schema,
				   std::vector<SortKeyDesc> &out)
{
	out.clear();
	const std::vector<bool> directions = ParseOrderDirections(queryDesc != nullptr ? queryDesc->sourceText : nullptr, order.orders.size());
	for (size_t i = 0; i < order.orders.size(); ++i)
	{
		const auto *col_expr = dynamic_cast<const BoundColumnRefExpression *>(order.orders[i]);
		uint16_t output_col_idx = UINT16_MAX;
		if (col_expr == nullptr)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer order rejected: order expr %zu is not a column ref", i);
			return false;
		}
		const ColumnSchema *col = nullptr;
		ColumnRef ref{};
		if (LookupNamedExprInputColumn(col_expr, outputs, cols, schema, ref, col) && col != nullptr)
		{
			for (size_t col_idx = 0; col_idx < cols.size() && col_idx < schema.size(); ++col_idx)
			{
				if (cols[col_idx] == ref)
				{
					output_col_idx = static_cast<uint16_t>(col_idx);
					break;
				}
			}
		}
		if (col == nullptr || output_col_idx == UINT16_MAX)
		{
			if (pg_yaap_trace_hooks)
			{
				elog(LOG,
					 "pg_yaap: optimizer order rejected: order expr %zu binding=(%zu,%zu) not found in output cols=%zu schema=%zu",
					 i,
					 col_expr->binding.table_index.index,
					 col_expr->binding.column_index.index,
					 cols.size(),
					 schema.size());
				if (outputs != nullptr)
				{
					for (size_t out_idx = 0; out_idx < outputs->size(); ++out_idx)
						elog(LOG,
							 "pg_yaap: optimizer order output[%zu]=%s.%s binding=(%zu,%zu)",
							 out_idx,
							 (*outputs)[out_idx].table_name.c_str(),
							 (*outputs)[out_idx].column_name.c_str(),
							 (*outputs)[out_idx].binding.table_index.index,
							 (*outputs)[out_idx].binding.column_index.index);
				}
			}
			return false;
		}
		out.push_back(SortKeyDesc{InvalidOid, output_col_idx, directions[i], false, 0});
	}
	return true;
}

}  // namespace

OptimizerPlanSupportStatus
AnalyzeOptimizerPlanSupport(const OptimizerPlanBundle &bundle)
{
	if (bundle.physical_plan == nullptr)
		return OptimizerPlanSupportStatus{false, "root", "optimizer physical plan is null"};

	SupportContext ctx;
	ctx.stack.push_back(std::string("root(") + OptimizerOpTypeName(*bundle.physical_plan) + ")");
	return AnalyzeOptimizerPlanNode(*bundle.physical_plan, ctx);
}

std::string
DescribeOptimizerPlan(const OptimizerPlanBundle &bundle)
{
	if (bundle.physical_plan == nullptr)
		return "NULL";
	std::string out;
	out += "\n";
	AppendOptimizerPlanNodeTree(*bundle.physical_plan, "", true, true, out);
	if (!out.empty() && out.back() == '\n')
		out.pop_back();
	return out;
}

std::unique_ptr<PipelineOperator>
BuildPipelineFromOptimizerPlan(QueryDesc *queryDesc,
							   PgYaapQueryState *state,
							   const OptimizerPlanBundle &bundle)
{
	if (queryDesc == nullptr || state == nullptr || bundle.physical_plan == nullptr || state->runtime_dsa == nullptr)
		return nullptr;

	const OptimizerPlanSupportStatus support = AnalyzeOptimizerPlanSupport(bundle);
	if (!support.supported)
		return nullptr;

	if (pg_yaap_trace_hooks)
		elog(LOG, "pg_yaap: optimizer plan matched pipeline builder path");

	OptimizerNodeTranslation node;
	if (pg_yaap_trace_hooks)
		elog(LOG, "pg_yaap: optimizer pipeline builder entering node lowering");
	if (!TranslateOptimizerNode(*bundle.physical_plan, queryDesc, state, nullptr, node) || node.op == nullptr)
		return nullptr;
	if (pg_yaap_trace_hooks)
		elog(LOG, "pg_yaap: optimizer pipeline builder built node cols=%zu schema=%zu", node.cols.size(), node.schema.size());

	auto root = BuildOutputContract(node, queryDesc, state);
	if (pg_yaap_trace_hooks)
		elog(LOG, "pg_yaap: optimizer pipeline builder built output contract root=%p", static_cast<void *>(root.get()));
	return root;
}

TupleDesc
BuildOptimizerOutputTupleDesc(const OptimizerPlanBundle &bundle)
{
	if (bundle.output_targetlist == NIL)
		return nullptr;
	return ExecCleanTypeFromTL(bundle.output_targetlist);
}

namespace optimizer_translator_detail {

bool
TranslateOptimizerNode(const PhysicalOperator &op,
					   QueryDesc *queryDesc,
					   PgYaapQueryState *state,
					   const std::vector<ColumnRef> *required_output_cols,
					   OptimizerNodeTranslation &out)
{
	if (pg_yaap_trace_hooks)
		elog(LOG,
			 "pg_yaap: TranslateOptimizerNode op_type=%d required_output_cols=%zu",
			 static_cast<int>(op.type),
			 required_output_cols != nullptr ? required_output_cols->size() : 0);
	switch (op.type)
	{
		case PhysicalOperatorType::TABLE_SCAN:
			return TranslateTableScanNode(static_cast<const PhysicalTableScan &>(op), state, required_output_cols, out);

		case PhysicalOperatorType::FILTER:
		{
			const auto &filter = static_cast<const PhysicalFilter &>(op);
			if (filter.children.size() != 1 || filter.children[0] == nullptr)
				return false;
			std::vector<ColumnRef> child_required;
			if (required_output_cols != nullptr)
			{
				for (const ColumnRef &ref : *required_output_cols)
					AppendUniqueColumnRef(ref, child_required);
			}
			for (Expression *expr : filter.expressions)
				CollectReferencedColumns(expr, child_required);

			OptimizerNodeTranslation child;
			const std::vector<ColumnRef> *child_required_ptr =
				child_required.empty() ? required_output_cols : &child_required;
			if (!TranslateOptimizerNode(*filter.children[0], queryDesc, state, child_required_ptr, child))
				return false;
			if (filter.children[0]->type == PhysicalOperatorType::HASH_GROUP_BY)
				return ApplyPostAggregateFilters(std::move(child), static_cast<const PhysicalHashAggregate &>(*filter.children[0]), filter.expressions, state, out);
			return ApplyPipelineFilters(std::move(child), filter.children[0].get(), filter.expressions, state, out);
		}

		case PhysicalOperatorType::PROJECTION:
			return TranslateProjectionNode(static_cast<const PhysicalProjection &>(op), queryDesc, state, required_output_cols, out);

		case PhysicalOperatorType::HASH_GROUP_BY:
			return TranslateHashAggregateNode(static_cast<const PhysicalHashAggregate &>(op), queryDesc, state, out);

		case PhysicalOperatorType::HASH_JOIN:
			return TranslateHashJoinNode(static_cast<const PhysicalHashJoin &>(op), queryDesc, state, required_output_cols, out);

		case PhysicalOperatorType::CROSS_PRODUCT:
			return TranslateCrossProductNode(static_cast<const PhysicalCrossProduct &>(op), queryDesc, state, required_output_cols, out);

		case PhysicalOperatorType::DELIM_SCAN:
			return TranslateDelimScanNode(static_cast<const PhysicalDelimScan &>(op), queryDesc, state, out);

		case PhysicalOperatorType::ORDER_BY:
		{
			const auto &order = static_cast<const PhysicalOrderBy &>(op);
			if (order.children.size() != 1 || order.children[0] == nullptr)
				return false;
			if (!TranslateOptimizerNode(*order.children[0], queryDesc, state, required_output_cols, out))
			{
				if (pg_yaap_trace_hooks)
					elog(LOG, "pg_yaap: optimizer order rejected: child translation failed");
				return false;
			}
			return BuildFinalSortKeys(order, queryDesc, &out.outputs, out.cols, out.schema, out.final_sort_keys);
		}

		case PhysicalOperatorType::LIMIT:
		{
			const auto &limit = static_cast<const PhysicalLimit &>(op);
			if (limit.children.size() != 1 || limit.children[0] == nullptr)
				return false;
			uint64_t limit_count = 0;
			if (!TryParseLimitExpression(limit.limit_count, limit_count))
				return false;
			if (limit_count == 0)
				return false;
			if (IsTopNNode(op))
			{
				const auto &order = static_cast<const PhysicalOrderBy &>(*limit.children[0]);
				OptimizerNodeTranslation child;
				if (!TranslateOptimizerNode(*order.children[0],
						queryDesc,
						state,
						required_output_cols,
						child))
					return false;
				if (!BuildFinalSortKeys(order,
						queryDesc,
						&child.outputs,
						child.cols,
						child.schema,
						child.final_sort_keys))
					return false;
				TupleDataLayout payload_layout;
				if (!BuildColumnOnlyLayout(child.schema, payload_layout))
					return false;
				const dsa_pointer input_schema_dp = BuildSchemaDescriptorFromColumns(child.schema, state->runtime_dsa);
				const dsa_pointer layout_dp = SerializeTupleDataLayout(payload_layout, state->runtime_dsa);
				const dsa_pointer sort_keys_dp = BuildFilterArray(state->runtime_dsa,
					child.final_sort_keys.data(),
					sizeof(SortKeyDesc),
					child.final_sort_keys.size());
				const dsa_pointer shared_payload_dp = dsa_allocate0(state->runtime_dsa,
					sizeof(pipeline::TopNSharedPayload));
				if (!DsaPointerIsValid(input_schema_dp) ||
					!DsaPointerIsValid(layout_dp) ||
					!DsaPointerIsValid(sort_keys_dp) ||
					!DsaPointerIsValid(shared_payload_dp))
					return false;
				auto *shared_payload = static_cast<pipeline::TopNSharedPayload *>(
					dsa_get_address(state->runtime_dsa, shared_payload_dp));
				SpinLockInit(&shared_payload->mutex);
				shared_payload->sort_indices_dp = InvalidDsaPointer;
				shared_payload->finalized = false;
				shared_payload->tdc_dp = pipeline::TupleDataCollectionAllocate(state->runtime_dsa,
					static_cast<uint32_t>(limit_count),
					payload_layout.row_width,
					TupleDataCollectionDefaultHeapCapacity(&payload_layout, static_cast<uint32_t>(limit_count)));
				auto *global_tdc = static_cast<TupleDataCollection *>(
					dsa_get_address(state->runtime_dsa, shared_payload->tdc_dp));
				TupleDataCollectionInit(global_tdc,
					static_cast<uint32_t>(limit_count),
					payload_layout.row_width,
					layout_dp,
					TupleDataCollectionDefaultHeapCapacity(&payload_layout, static_cast<uint32_t>(limit_count)));
				auto topn = std::make_unique<PipelineTopN>(
					input_schema_dp,
					layout_dp,
					sort_keys_dp,
					static_cast<uint16_t>(child.final_sort_keys.size()),
					shared_payload_dp,
					static_cast<uint32_t>(limit_count));
				topn->AddChild(std::move(child.op));
				child.op = std::move(topn);
				child.final_sort_keys.clear();
				child.limit_count = 0;
				out = std::move(child);
				return true;
			}
			OptimizerNodeTranslation child;
			if (!TranslateOptimizerNode(*limit.children[0], queryDesc, state, required_output_cols, child))
				return false;
			out = std::move(child);
			out.limit_count = limit_count;
			return true;
		}

		default:
			return false;
	}
}

}  // namespace optimizer_translator_detail

}  // namespace pg_yaap
