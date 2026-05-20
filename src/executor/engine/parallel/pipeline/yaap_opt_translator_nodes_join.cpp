#include "parallel/pipeline/yaap_pipeline_init_internal.hpp"

namespace pg_yaap::optimizer_translator_detail {

static bool
BuildOrderedSchemaForRefs(const std::vector<ColumnRef> &refs,
						  const std::vector<ColumnRef> &available_cols,
						  const std::vector<ColumnSchema> &available_schema,
						  std::vector<ColumnSchema> &out_schema)
{
	out_schema.clear();
	out_schema.reserve(refs.size());
	for (const ColumnRef &ref : refs)
	{
		const ColumnSchema *col = nullptr;
		if (!LookupRawColumn(ref, available_cols, available_schema, col) || col == nullptr)
			return false;
		out_schema.push_back(*col);
	}
	return true;
}

static bool
AddRightFilterPayloadRefs(const std::vector<HashJoinFilterInputDesc> &filter_inputs,
						  const std::vector<ColumnRef> &build_cols,
						  const std::vector<ColumnSchema> &build_schema,
						  std::vector<ColumnRef> &build_payload_refs)
{
	for (const HashJoinFilterInputDesc &input : filter_inputs)
	{
		if (input.side != HashJoinOutputSide::RIGHT)
			continue;
		bool found = false;
		for (size_t i = 0; i < build_cols.size() && i < build_schema.size(); ++i)
		{
			if (build_schema[i].chunk_slot != input.input_chunk_slot ||
				build_schema[i].decode_kind != input.source_decode_kind)
				continue;
			found = true;
			bool exists = false;
			for (const ColumnRef &existing : build_payload_refs)
			{
				if (SameColumnRef(existing, build_cols[i]))
				{
					exists = true;
					break;
				}
			}
			if (!exists)
				build_payload_refs.push_back(build_cols[i]);
			break;
		}
		if (!found)
			return false;
	}
	return true;
}

static bool
OutputBindingsMatch(const std::vector<yaap::PhysicalOperator::OutputColumn> &lhs,
					  const std::vector<yaap::PhysicalOperator::OutputColumn> &rhs)
{
	if (lhs.size() != rhs.size())
		return false;
	for (size_t i = 0; i < lhs.size(); ++i)
	{
		if (lhs[i].binding.table_index.index != rhs[i].binding.table_index.index ||
			lhs[i].binding.column_index.index != rhs[i].binding.column_index.index)
			return false;
	}
	return true;
}

static size_t
CountSourceOutputMatches(const PhysicalOperator *source_op,
						 const std::vector<ColumnRef> *required_refs)
{
	if (source_op == nullptr || required_refs == nullptr || required_refs->empty())
		return 0;
	size_t matched_count = 0;
	for (const ColumnRef &required_ref : *required_refs)
	{
		for (const auto &output : source_op->outputs)
		{
			if (SameColumnRef(required_ref, BindingToColumnRef(output.binding)))
			{
				++matched_count;
				break;
			}
		}
	}
	return matched_count;
}

static bool IsScalarPhysicalNode(const PhysicalOperator *op);

static bool
IsScalarPhysicalNode(const PhysicalOperator *op)
{
	if (op == nullptr)
		return false;
	if (op->type == PhysicalOperatorType::HASH_GROUP_BY)
	{
		const auto *agg = static_cast<const PhysicalHashAggregate *>(op);
		return agg->groups.empty();
	}
	if ((op->type == PhysicalOperatorType::FILTER ||
		 op->type == PhysicalOperatorType::PROJECTION ||
		 op->type == PhysicalOperatorType::ORDER_BY ||
		 op->type == PhysicalOperatorType::LIMIT) &&
		op->children.size() == 1 && op->children[0] != nullptr)
	{
		return IsScalarPhysicalNode(op->children[0].get());
	}
	return false;
}

bool
TranslateHashJoinNode(const PhysicalHashJoin &join,
					  QueryDesc *queryDesc,
					  PgYaapQueryState *state,
					  const std::vector<ColumnRef> *required_output_cols,
					  OptimizerNodeTranslation &out)
{
	if (join.children.size() != 2 || join.children[0] == nullptr || join.children[1] == nullptr || state == nullptr || state->runtime_dsa == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: invalid children/state");
		return false;
	}
	const bool scalar_delim_join = join.delim_join && join.join_type == yaap::JOIN_SINGLE;
	const bool semi_or_anti_join = join.join_type == yaap::JOIN_SEMI || join.join_type == yaap::JOIN_ANTI;
	const bool left_outer_join = join.join_type == yaap::JOIN_LEFT;
	if (join.join_type != yaap::JOIN_INNER && !left_outer_join && !scalar_delim_join && !semi_or_anti_join)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: join_type=%d", join.join_type);
		return false;
	}
	const pg_yaap::pipeline::HashJoinMatchMode join_mode =
		join.join_type == yaap::JOIN_SEMI ? pg_yaap::pipeline::HashJoinMatchMode::SEMI :
		join.join_type == yaap::JOIN_ANTI ? pg_yaap::pipeline::HashJoinMatchMode::ANTI :
		join.join_type == yaap::JOIN_LEFT ? pg_yaap::pipeline::HashJoinMatchMode::LEFT :
		pg_yaap::pipeline::HashJoinMatchMode::INNER;
	const bool left_join_output_matches =
		semi_or_anti_join &&
		join.children[0] != nullptr &&
		OutputBindingsMatch(join.outputs, join.children[0]->outputs);
	const bool right_join_output_matches =
		semi_or_anti_join &&
		join.children[1] != nullptr &&
		OutputBindingsMatch(join.outputs, join.children[1]->outputs);
	const size_t left_required_match_count =
		semi_or_anti_join &&
		CountSourceOutputMatches(join.children[0].get(), required_output_cols);
	const size_t right_required_match_count =
		semi_or_anti_join &&
		CountSourceOutputMatches(join.children[1].get(), required_output_cols);
	const bool use_required_output_side =
		semi_or_anti_join &&
		(left_required_match_count != right_required_match_count) &&
		(left_required_match_count > 0 || right_required_match_count > 0);
	const bool use_join_output_side =
		semi_or_anti_join &&
		join.join_type == yaap::JOIN_SEMI &&
		(left_join_output_matches != right_join_output_matches);
	const bool right_output_semi_or_anti =
		semi_or_anti_join &&
		(use_required_output_side
			 ? (right_required_match_count > left_required_match_count)
			 : (use_join_output_side
					? right_join_output_matches
					: false));
	if (pg_yaap_trace_hooks && semi_or_anti_join)
		elog(LOG,
			 "pg_yaap: semi/anti routing join_type=%d left_req=%zu right_req=%zu left_join=%d right_join=%d use_required=%d use_join=%d right_output=%d delim=%d corr=%zu",
			 join.join_type,
			 left_required_match_count,
			 right_required_match_count,
			 left_join_output_matches ? 1 : 0,
			 right_join_output_matches ? 1 : 0,
			 use_required_output_side ? 1 : 0,
			 use_join_output_side ? 1 : 0,
			 right_output_semi_or_anti ? 1 : 0,
			 join.delim_join ? 1 : 0,
			 join.correlated_columns.size());

	std::vector<ColumnRef> join_condition_required_cols;
	for (Expression *expr : join.conditions)
		CollectReferencedColumns(expr, join_condition_required_cols);

	std::vector<ColumnRef> left_required_cols = join_condition_required_cols;
	std::vector<ColumnRef> right_required_cols = join_condition_required_cols;
	if (!semi_or_anti_join && required_output_cols != nullptr)
	{
		for (const ColumnRef &ref : *required_output_cols)
		{
			AppendUniqueColumnRef(ref, left_required_cols);
			AppendUniqueColumnRef(ref, right_required_cols);
		}
	}
	if (semi_or_anti_join && required_output_cols != nullptr)
	{
		auto &output_required_cols = right_output_semi_or_anti ? right_required_cols : left_required_cols;
		for (const ColumnRef &ref : *required_output_cols)
			AppendUniqueColumnRef(ref, output_required_cols);
	}
	if (!semi_or_anti_join && required_output_cols == nullptr)
	{
		for (const auto &output : join.children[0]->outputs)
			AppendUniqueColumnRef(BindingToColumnRef(output.binding), left_required_cols);
		for (const auto &output : join.children[1]->outputs)
			AppendUniqueColumnRef(BindingToColumnRef(output.binding), right_required_cols);
	}
	const bool left_child_scalar = join.children[0] != nullptr && IsScalarPhysicalNode(join.children[0].get());
	const bool right_child_scalar = join.children[1] != nullptr && IsScalarPhysicalNode(join.children[1].get());
	if (left_child_scalar || right_child_scalar)
	{
		left_required_cols.clear();
		right_required_cols.clear();
	}

	auto translate_children = [&](OptimizerNodeTranslation &left_out,
								  OptimizerNodeTranslation &right_out) -> bool
	{
		if (!TranslateOptimizerNode(*join.children[0],
				queryDesc,
				state,
				left_required_cols.empty() ? nullptr : &left_required_cols,
				left_out) ||
			!TranslateOptimizerNode(*join.children[1],
				queryDesc,
				state,
				right_required_cols.empty() ? nullptr : &right_required_cols,
				right_out) ||
			left_out.op == nullptr || right_out.op == nullptr)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer hash join rejected: child translation failed");
			return false;
		}
		return true;
	};

	std::vector<Expression *> rewritten_join_conditions;
	rewritten_join_conditions.reserve(join.conditions.size());
	for (Expression *expr : join.conditions)
		rewritten_join_conditions.push_back(expr);

	auto collect_join_state =
		[&](const OptimizerNodeTranslation &left_input,
			const OptimizerNodeTranslation &right_input,
			std::vector<ColumnRef> &out_left_keys,
			std::vector<ColumnRef> &out_right_keys,
			std::vector<Expression *> &out_residuals) -> bool
	{
		out_left_keys.clear();
		out_right_keys.clear();
		out_residuals.clear();

		std::vector<Expression *> join_condition_residuals;
		for (Expression *expr : rewritten_join_conditions)
		{
			if (!CollectJoinKeys(expr,
								 &left_input.outputs,
								 left_input.cols, left_input.schema,
								 &right_input.outputs,
								 right_input.cols, right_input.schema,
								 out_left_keys, out_right_keys,
								 join_condition_residuals))
			{
				if (pg_yaap_trace_hooks)
					elog(LOG, "pg_yaap: optimizer hash join rejected: join key extraction failed");
				return false;
			}
		}
		for (Expression *expr : join_condition_residuals)
			out_residuals.push_back(expr);
		return true;
	};

	OptimizerNodeTranslation left_child;
	OptimizerNodeTranslation right_child;
	if (!translate_children(left_child, right_child))
		return false;

	std::vector<ColumnRef> left_keys;
	std::vector<ColumnRef> right_keys;
	std::vector<Expression *> residuals;
	if (!collect_join_state(left_child,
							right_child,
							left_keys,
							right_keys,
							residuals))
	{
		return false;
	}
	const size_t left_rows = join.children[0] != nullptr ? join.children[0]->estimated_cardinality : 0;
	const size_t right_rows = join.children[1] != nullptr ? join.children[1]->estimated_cardinality : 0;
	const bool left_scalar = IsScalarPhysicalNode(join.children[0].get());
	const bool right_scalar = IsScalarPhysicalNode(join.children[1].get());
	if (pg_yaap_trace_hooks)
		elog(LOG,
			 "pg_yaap: optimizer hash join scalar-shape left_rows=%zu right_rows=%zu left_scalar=%d right_scalar=%d join_type=%d conditions=%zu",
			 left_rows,
			 right_rows,
			 left_scalar ? 1 : 0,
			 right_scalar ? 1 : 0,
			 join.join_type,
			 join.conditions.size());
	const bool scalar_residual_join =
		(join.join_type == yaap::JOIN_INNER || scalar_delim_join) &&
		left_keys.empty() &&
		right_keys.empty() &&
		!residuals.empty() &&
		(left_rows == 1 || right_rows == 1 || left_scalar || right_scalar);
	const bool scalar_zero_key_join =
		(join.join_type == yaap::JOIN_INNER || scalar_delim_join) &&
		left_keys.empty() &&
		right_keys.empty() &&
		residuals.empty() &&
		(left_rows == 1 || right_rows == 1 || left_scalar || right_scalar);
	if ((!scalar_residual_join && !scalar_zero_key_join && left_keys.empty()) || left_keys.size() != right_keys.size())
	{
		if (pg_yaap_trace_hooks)
		{
			for (size_t i = 0; i < left_child.cols.size(); ++i)
				elog(LOG,
					 "pg_yaap: optimizer hash join left_col[%zu]=(%u,%d)",
					 i,
					 left_child.cols[i].varno,
					 left_child.cols[i].attno);
			for (size_t i = 0; i < right_child.cols.size(); ++i)
				elog(LOG,
					 "pg_yaap: optimizer hash join right_col[%zu]=(%u,%d)",
					 i,
					 right_child.cols[i].varno,
					 right_child.cols[i].attno);
			for (size_t i = 0; i < join.conditions.size(); ++i)
			{
				const Expression *expr = join.conditions[i];
				if (expr != nullptr && expr->type == ExpressionType::BOUND_FUNCTION)
				{
					const auto *func = static_cast<const BoundFunctionExpression *>(expr);
					elog(LOG,
						 "pg_yaap: optimizer hash join cond[%zu] fn=%s children=%zu",
						 i,
						 func->function_name.c_str(),
						 func->children.size());
				}
				else
				{
					elog(LOG,
						 "pg_yaap: optimizer hash join cond[%zu] type=%d",
						 i,
						 expr != nullptr ? static_cast<int>(expr->type) : -1);
				}
			}
			elog(LOG,
				 "pg_yaap: optimizer hash join rejected: no usable equi-join keys (left=%zu right=%zu residuals=%zu)",
				 left_keys.size(),
				 right_keys.size(),
				 residuals.size());
		}
		return false;
	}
	if (pg_yaap_trace_hooks && scalar_residual_join)
		elog(LOG,
			 "pg_yaap: optimizer hash join using zero-key scalar residual fallback left_rows=%zu right_rows=%zu left_scalar=%d right_scalar=%d residuals=%zu",
			 left_rows,
			 right_rows,
			 left_scalar ? 1 : 0,
			 right_scalar ? 1 : 0,
			 residuals.size());
	if (pg_yaap_trace_hooks && scalar_zero_key_join)
		elog(LOG,
			 "pg_yaap: optimizer hash join using zero-key scalar join fallback left_rows=%zu right_rows=%zu left_scalar=%d right_scalar=%d",
			 left_rows,
			 right_rows,
			 left_scalar ? 1 : 0,
			 right_scalar ? 1 : 0);

	const bool swap_sides = semi_or_anti_join ? right_output_semi_or_anti :
		(left_outer_join ? false :
		((left_rows > 0 && right_rows > 0)
			? (left_rows < right_rows)
			: (left_child.schema.size() < right_child.schema.size())));
	const auto &probe_cols = swap_sides ? right_child.cols : left_child.cols;
	const auto &probe_schema = swap_sides ? right_child.schema : left_child.schema;
	const auto &probe_outputs = swap_sides ? right_child.outputs : left_child.outputs;
	const auto &build_cols = swap_sides ? left_child.cols : right_child.cols;
	const auto &build_schema = swap_sides ? left_child.schema : right_child.schema;
	const auto &build_outputs = swap_sides ? left_child.outputs : right_child.outputs;
	const PhysicalOperator *probe_source_op = swap_sides ? join.children[1].get() : join.children[0].get();
	const PhysicalOperator *build_source_op = swap_sides ? join.children[0].get() : join.children[1].get();
	const auto &probe_keys = swap_sides ? right_keys : left_keys;
	const auto &build_keys = swap_sides ? left_keys : right_keys;

	std::vector<ColumnRef> raw_output_cols = probe_cols;
	if (!semi_or_anti_join)
		raw_output_cols.insert(raw_output_cols.end(), build_cols.begin(), build_cols.end());
	if (pg_yaap_trace_hooks && scalar_delim_join)
	{
		elog(LOG,
			 "pg_yaap: scalar delim join probe_cols=%zu build_cols=%zu raw_output_cols=%zu required_output_cols=%zu",
			 probe_cols.size(),
			 build_cols.size(),
			 raw_output_cols.size(),
			 required_output_cols != nullptr ? required_output_cols->size() : 0);
		for (size_t i = 0; i < raw_output_cols.size(); ++i)
			elog(LOG,
				 "pg_yaap: scalar delim join raw_output_col[%zu]=(%u,%d)",
				 i,
				 raw_output_cols[i].varno,
				 raw_output_cols[i].attno);
		if (required_output_cols != nullptr)
		{
			for (size_t i = 0; i < required_output_cols->size(); ++i)
				elog(LOG,
					 "pg_yaap: scalar delim join required_output_col[%zu]=(%u,%d)",
					 i,
					 (*required_output_cols)[i].varno,
					 (*required_output_cols)[i].attno);
		}
	}
	std::vector<ColumnRef> requested_output_cols;
	if (scalar_delim_join || semi_or_anti_join)
		requested_output_cols = raw_output_cols;
	else
	{
		FilterRequestedColumns(raw_output_cols, required_output_cols, requested_output_cols);
		if (requested_output_cols.empty())
			requested_output_cols = raw_output_cols;
	}
	if (semi_or_anti_join)
	{
		for (const ColumnRef &ref : requested_output_cols)
		{
			const ColumnSchema *probe_col = nullptr;
			if (!LookupRawColumn(ref, probe_cols, probe_schema, probe_col) || probe_col == nullptr)
			{
				if (pg_yaap_trace_hooks)
					elog(LOG, "pg_yaap: optimizer hash join rejected: semi/anti output references build side");
				return false;
			}
		}
	}
	std::vector<HashJoinOutputColumnDesc> output_mappings;
	std::vector<ColumnSchema> output_schema;
	std::vector<yaap::PhysicalOperator::OutputColumn> output_bindings;
	std::vector<yaap::PhysicalOperator::OutputColumn> raw_output_bindings = probe_outputs;
	if (!semi_or_anti_join)
		raw_output_bindings.insert(raw_output_bindings.end(), build_outputs.begin(), build_outputs.end());
	TupleDataLayout probe_key_layout;
	TupleDataLayout build_key_layout;
	TupleDataLayout probe_payload_layout;
	TupleDataLayout build_payload_layout;
	std::vector<ColumnRef> build_payload_refs;
	std::vector<ColumnSchema> build_payload_schema_storage;
	const std::vector<ColumnSchema> *build_payload_schema = &build_schema;
	if (!BuildColumnOnlyLayoutForRefs(probe_keys, probe_cols, probe_schema, probe_key_layout) ||
		!BuildColumnOnlyLayoutForRefs(build_keys, build_cols, build_schema, build_key_layout) ||
		!BuildColumnOnlyLayout(probe_schema, probe_payload_layout) ||
		!BuildHashJoinOutputMappings(requested_output_cols, probe_cols, probe_schema, build_cols, build_schema, output_mappings, output_schema))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: join layout/output mapping build failed");
		return false;
	}
	if (!BuildOrderedOutputBindingsForRefs(requested_output_cols, raw_output_cols, raw_output_bindings, output_bindings))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: optimizer hash join rejected: output binding alignment failed probe_cols=%zu probe_outputs=%zu build_cols=%zu build_outputs=%zu raw_output_cols=%zu requested=%zu semi_or_anti=%d swapped=%d probe_child_type=%d build_child_type=%d",
				 probe_cols.size(),
				 probe_outputs.size(),
				 build_cols.size(),
				 build_outputs.size(),
				 raw_output_cols.size(),
				 requested_output_cols.size(),
				 semi_or_anti_join ? 1 : 0,
				 swap_sides ? 1 : 0,
				 static_cast<int>(swap_sides ? join.children[1]->type : join.children[0]->type),
				 static_cast<int>(swap_sides ? join.children[0]->type : join.children[1]->type));
		return false;
	}

	std::vector<HashJoinFilterInputDesc> filter_inputs;
	std::vector<FilterExprDesc> filter_exprs;
	std::vector<FilterStep> filter_steps;
	std::vector<char> filter_string_consts;
	uint16_t filter_bool_regs = 0;
	if (pg_yaap_trace_hooks && scalar_residual_join)
	{
		for (size_t i = 0; i < residuals.size(); ++i)
		{
			const auto *func = dynamic_cast<const BoundFunctionExpression *>(residuals[i]);
			if (func == nullptr || func->children.size() != 2)
				continue;
			const auto *lhs = dynamic_cast<const BoundColumnRefExpression *>(func->children[0].get());
			const auto *rhs = dynamic_cast<const BoundColumnRefExpression *>(func->children[1].get());
			if (lhs == nullptr || rhs == nullptr)
				continue;
			elog(LOG,
				 "pg_yaap: scalar residual[%zu] fn=%s lhs=(%zu,%zu %s.%s) rhs=(%zu,%zu %s.%s)",
				 i,
				 func->function_name.c_str(),
				 lhs->binding.table_index.index,
				 lhs->binding.column_index.index,
				 lhs->table_name.c_str(),
				 lhs->column_name.c_str(),
				 rhs->binding.table_index.index,
				 rhs->binding.column_index.index,
				 rhs->table_name.c_str(),
				 rhs->column_name.c_str());
		}
	}
	if (!LowerJoinFilters(residuals,
						  probe_source_op,
						  build_source_op,
						  &probe_outputs,
						  probe_cols, probe_schema,
						  &build_outputs,
						  build_cols, build_schema,
						  filter_inputs, filter_exprs, filter_steps, filter_string_consts, filter_bool_regs))
	{
		if (pg_yaap_trace_hooks)
		{
			for (size_t i = 0; i < residuals.size(); ++i)
			{
				const Expression *expr = residuals[i];
				if (const auto *func = dynamic_cast<const BoundFunctionExpression *>(expr))
					elog(LOG,
						 "pg_yaap: residual[%zu] fn=%s children=%zu",
						 i,
						 func->function_name.c_str(),
						 func->children.size());
				else if (const auto *conj = dynamic_cast<const BoundConjunctionExpression *>(expr))
					elog(LOG,
						 "pg_yaap: residual[%zu] conjunction type=%d children=%zu",
						 i,
						 conj->bool_expr_type,
						 conj->children.size());
				else
					elog(LOG,
						 "pg_yaap: residual[%zu] type=%d",
						 i,
						 expr != nullptr ? static_cast<int>(expr->type) : -1);
			}
			elog(LOG,
				 "pg_yaap: optimizer hash join rejected: residual join filter lowering failed (%zu residuals)",
				 residuals.size());
		}
		return false;
	}
	if (pg_yaap_trace_hooks && scalar_residual_join)
		elog(LOG,
			 "pg_yaap: scalar residual filter counts inputs=%zu exprs=%zu steps=%zu bool_regs=%u residuals=%zu",
			 filter_inputs.size(),
			 filter_exprs.size(),
			 filter_steps.size(),
			 filter_bool_regs,
			 residuals.size());

	for (const ColumnRef &candidate : build_cols)
	{
		for (const ColumnRef &requested_ref : requested_output_cols)
		{
			if (SameColumnRef(candidate, requested_ref))
			{
				build_payload_refs.push_back(candidate);
				break;
			}
		}
	}
	if (!AddRightFilterPayloadRefs(filter_inputs, build_cols, build_schema, build_payload_refs))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: residual payload column derivation failed");
		return false;
	}
	if (build_payload_refs.empty())
	{
		if (!build_keys.empty())
			build_payload_refs.push_back(build_keys.front());
		else if (!build_cols.empty())
			build_payload_refs.push_back(build_cols.front());
	}
	if (!BuildOrderedSchemaForRefs(build_payload_refs, build_cols, build_schema, build_payload_schema_storage) ||
		!BuildColumnOnlyLayout(build_payload_schema_storage, build_payload_layout))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: build payload schema derivation failed");
		return false;
	}
	build_payload_schema = &build_payload_schema_storage;

	dsa_pointer left_schema_dp = BuildSchemaDescriptorFromColumns(probe_schema, state->runtime_dsa);
	dsa_pointer right_schema_dp = BuildSchemaDescriptorFromColumns(*build_payload_schema, state->runtime_dsa);
	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(output_schema, state->runtime_dsa);
	dsa_pointer left_key_layout_dp = SerializeTupleDataLayout(probe_key_layout, state->runtime_dsa);
	dsa_pointer right_key_layout_dp = SerializeTupleDataLayout(build_key_layout, state->runtime_dsa);
	dsa_pointer left_payload_layout_dp = SerializeTupleDataLayout(probe_payload_layout, state->runtime_dsa);
	dsa_pointer right_payload_layout_dp = SerializeTupleDataLayout(build_payload_layout, state->runtime_dsa);
	dsa_pointer output_columns_dp = BuildFilterArray(state->runtime_dsa, output_mappings.data(), sizeof(HashJoinOutputColumnDesc), output_mappings.size());
	dsa_pointer filter_inputs_dp = BuildFilterArray(state->runtime_dsa, filter_inputs.data(), sizeof(HashJoinFilterInputDesc), filter_inputs.size());
	dsa_pointer filter_exprs_dp = BuildFilterArray(state->runtime_dsa, filter_exprs.data(), sizeof(FilterExprDesc), filter_exprs.size());
	dsa_pointer filter_steps_dp = BuildFilterArray(state->runtime_dsa, filter_steps.data(), sizeof(FilterStep), filter_steps.size());
	dsa_pointer filter_string_consts_dp = BuildCharArray(state->runtime_dsa, filter_string_consts);
	if (!DsaPointerIsValid(left_schema_dp) || !DsaPointerIsValid(right_schema_dp) ||
		!DsaPointerIsValid(output_schema_dp) || !DsaPointerIsValid(left_key_layout_dp) ||
		!DsaPointerIsValid(right_key_layout_dp) || !DsaPointerIsValid(left_payload_layout_dp) ||
		!DsaPointerIsValid(right_payload_layout_dp) || !DsaPointerIsValid(output_columns_dp))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: optimizer hash join rejected: DSA publish failed left_schema=%d right_schema=%d output_schema=%d left_key=%d right_key=%d left_payload=%d right_payload=%d output_cols=%d output_mappings=%zu output_schema_cols=%zu",
				 DsaPointerIsValid(left_schema_dp) ? 1 : 0,
				 DsaPointerIsValid(right_schema_dp) ? 1 : 0,
				 DsaPointerIsValid(output_schema_dp) ? 1 : 0,
				 DsaPointerIsValid(left_key_layout_dp) ? 1 : 0,
				 DsaPointerIsValid(right_key_layout_dp) ? 1 : 0,
				 DsaPointerIsValid(left_payload_layout_dp) ? 1 : 0,
				 DsaPointerIsValid(right_payload_layout_dp) ? 1 : 0,
				 DsaPointerIsValid(output_columns_dp) ? 1 : 0,
				 output_mappings.size(),
				 output_schema.size());
		return false;
	}

	auto join_op = std::make_unique<pg_yaap::pipeline::PhysicalHashJoin>(
		left_schema_dp,
		right_schema_dp,
		output_schema_dp,
		left_key_layout_dp,
		right_key_layout_dp,
		left_payload_layout_dp,
		right_payload_layout_dp,
		output_columns_dp,
		static_cast<uint16_t>(output_mappings.size()),
		filter_inputs_dp,
		filter_exprs_dp,
		filter_steps_dp,
		filter_string_consts_dp,
		static_cast<uint16_t>(filter_inputs.size()),
		static_cast<uint16_t>(filter_exprs.size()),
		static_cast<uint16_t>(filter_steps.size()),
		filter_bool_regs,
		join_mode,
		static_cast<uint32_t>(filter_string_consts.size()),
		InvalidDsaPointer,
		static_cast<uint16_t>(probe_keys.size()),
		static_cast<uint16_t>(build_keys.size()),
		EstimateHashJoinBuildRows(swap_sides ? left_child.schema.size() : right_child.schema.size()));
	if (swap_sides)
	{
		join_op->AddChild(std::move(right_child.op));
		join_op->AddChild(std::move(left_child.op));
	}
	else
	{
		join_op->AddChild(std::move(left_child.op));
		join_op->AddChild(std::move(right_child.op));
	}

	std::vector<ColumnRef> parent_facing_cols =
		BuildParentFacingOutputCols(requested_output_cols, required_output_cols);
	out.op = std::move(join_op);
	out.cols = std::move(parent_facing_cols);
	out.schema = std::move(output_schema);
	out.outputs = std::move(output_bindings);
	out.final_sort_keys.clear();
	out.limit_count = 0;
	out.estimated_groups = 0;
	return true;
}

bool
TranslateCrossProductNode(const PhysicalCrossProduct &join,
						  QueryDesc *queryDesc,
						  PgYaapQueryState *state,
						  const std::vector<ColumnRef> *required_output_cols,
						  OptimizerNodeTranslation &out)
{
	(void) queryDesc;
	if (join.children.size() != 2 || join.children[0] == nullptr || join.children[1] == nullptr ||
		state == nullptr || state->runtime_dsa == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer cross product rejected: invalid children/state");
		return false;
	}

	std::vector<ColumnRef> left_required_cols;
	std::vector<ColumnRef> right_required_cols;
	std::vector<ColumnRef> output_cols;
	if (required_output_cols != nullptr)
	{
		output_cols = *required_output_cols;
		for (const ColumnRef &ref : *required_output_cols)
		{
			AppendUniqueColumnRef(ref, left_required_cols);
			AppendUniqueColumnRef(ref, right_required_cols);
		}
	}
	else
	{
		for (const auto &output : join.outputs)
		{
			ColumnRef ref = BindingToColumnRef(output.binding);
			output_cols.push_back(ref);
			AppendUniqueColumnRef(ref, left_required_cols);
			AppendUniqueColumnRef(ref, right_required_cols);
		}
	}

	OptimizerNodeTranslation left_child;
	OptimizerNodeTranslation right_child;
	if (!TranslateOptimizerNode(*join.children[0],
			queryDesc,
			state,
			left_required_cols.empty() ? nullptr : &left_required_cols,
			left_child) ||
		!TranslateOptimizerNode(*join.children[1],
			queryDesc,
			state,
			right_required_cols.empty() ? nullptr : &right_required_cols,
			right_child) ||
		left_child.op == nullptr || right_child.op == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer cross product rejected: child translation failed");
		return false;
	}

	TupleDataLayout right_layout;
	if (!BuildColumnOnlyLayout(right_child.schema, right_layout))
		return false;

	std::vector<HashJoinOutputColumnDesc> output_mappings;
	std::vector<ColumnSchema> output_schema;
	if (!BuildHashJoinOutputMappings(output_cols,
			left_child.cols,
			left_child.schema,
			right_child.cols,
			right_child.schema,
			output_mappings,
			output_schema))
		return false;
	if (output_mappings.empty())
		return false;

	dsa_pointer left_input_schema_dp = BuildSchemaDescriptorFromColumns(left_child.schema, state->runtime_dsa);
	dsa_pointer right_input_schema_dp = BuildSchemaDescriptorFromColumns(right_child.schema, state->runtime_dsa);
	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(output_schema, state->runtime_dsa);
	dsa_pointer right_layout_dp = SerializeTupleDataLayout(right_layout, state->runtime_dsa);
	dsa_pointer output_columns_dp = BuildFilterArray(state->runtime_dsa,
		output_mappings.data(),
		sizeof(HashJoinOutputColumnDesc),
		output_mappings.size());
	auto cross_product_op = std::make_unique<PipelineCrossProduct>(
		left_input_schema_dp,
		right_input_schema_dp,
		output_schema_dp,
		right_layout_dp,
		output_columns_dp,
		static_cast<uint16_t>(output_mappings.size()),
		InvalidDsaPointer,
		EstimateHashJoinBuildRows(join.children[1]->estimated_cardinality));
	cross_product_op->AddChild(std::move(left_child.op));
	cross_product_op->AddChild(std::move(right_child.op));

	out.op = std::move(cross_product_op);
	out.cols = std::move(output_cols);
	out.schema = std::move(output_schema);
	out.outputs = join.outputs;
	out.final_sort_keys.clear();
	out.limit_count = 0;
	out.estimated_groups = 0;
	return true;
}

bool
BuildAllProjectionColumnRefs(const PhysicalProjection &projection,
							 const PhysicalTableScan &scan,
							 std::vector<ColumnRef> &out_cols)
{
	out_cols.clear();
	for (Expression *expr : projection.select_list)
	{
		const auto *colref = dynamic_cast<const BoundColumnRefExpression *>(expr);
		if (colref == nullptr || colref->binding.table_index.index != scan.table_index.index)
			return false;
		out_cols.push_back(BindingToColumnRef(colref->binding));
	}
	return !out_cols.empty();
}

bool
ExtractScanShape(const PhysicalOperator &op,
				 const PhysicalTableScan *&out_scan,
				 std::vector<ColumnRef> &out_cols)
{
	out_scan = nullptr;
	switch (op.type)
	{
		case PhysicalOperatorType::TABLE_SCAN:
			out_scan = static_cast<const PhysicalTableScan *>(&op);
			return BuildProjectedTableColumnRefs(out_scan->relid,
												 static_cast<Index>(out_scan->table_index.index + 1),
												 out_scan->projected_columns,
												 out_cols);
		case PhysicalOperatorType::PROJECTION:
		{
			const auto &projection = static_cast<const PhysicalProjection &>(op);
			if (projection.children.size() != 1 || projection.children[0] == nullptr ||
				projection.children[0]->type != PhysicalOperatorType::TABLE_SCAN)
				return false;
			out_scan = static_cast<const PhysicalTableScan *>(projection.children[0].get());
			return BuildAllProjectionColumnRefs(projection, *out_scan, out_cols);
		}
		default:
			return false;
	}
}

}  // namespace pg_yaap::optimizer_translator_detail
