#pragma once

extern "C" {
#include "postgres.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "catalog/pg_type_d.h"
#include "datatype/timestamp.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/dsa.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

extern Datum numeric_mul(PG_FUNCTION_ARGS);
extern Datum numeric_int8(PG_FUNCTION_ARGS);
}

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstring>
#include <limits>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "yaap_pipeline_init.hpp"
#include "adapter/yaap_adapter.hpp"
#include "optimizer_registry.hpp"
#include "parallel/pipeline/output_sink.hpp"
#include "parallel/pipeline/physical_delim_scan.hpp"
#include "parallel/pipeline/physical_cross_product.hpp"
#include "parallel/pipeline/physical_filter.hpp"
#include "parallel/pipeline/physical_hash_aggregate.hpp"
#include "parallel/pipeline/physical_hash_join.hpp"
#include "parallel/pipeline/physical_perfect_hash_aggregate.hpp"
#include "parallel/pipeline/physical_projection.hpp"
#include "parallel/pipeline/physical_seq_scan.hpp"
#include "parallel/pipeline/physical_top_n.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/translator_internal.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"

namespace pg_yaap {

extern "C" bool pg_yaap_trace_hooks;

namespace optimizer_translator_detail {

constexpr int32 kNumericTypmodVarHdrSz = 4;

inline int32
MakeNumericTypmod(int precision, int scale)
{
return ((precision << 16) | (scale & 0x7ff)) + kNumericTypmodVarHdrSz;
}

using yaap::BoundAggregateExpression;
using yaap::BoundColumnRefExpression;
using yaap::BoundConjunctionExpression;
using yaap::BoundConstantExpression;
using yaap::BoundFunctionExpression;
using yaap::Expression;
using yaap::ExpressionType;
using yaap::PhysicalCrossProduct;
using yaap::PhysicalDelimScan;
using yaap::PhysicalDistinct;
using yaap::PhysicalFilter;
using yaap::PhysicalHashAggregate;
using yaap::PhysicalHashJoin;
using yaap::PhysicalLimit;
using yaap::PhysicalOperator;
using yaap::PhysicalOperatorType;
using yaap::PhysicalOrderBy;
using yaap::PhysicalProjection;
using yaap::PhysicalSetOperation;
using yaap::PhysicalTableScan;
using yaap::PhysicalWindow;
using yaap::SetOperationType;

using pg_yaap::pipeline::AggFuncDesc;
using pg_yaap::pipeline::ColumnDecodeKind;
using pg_yaap::pipeline::ColumnSchema;
using pg_yaap::pipeline::FilterExprDesc;
using pg_yaap::pipeline::FilterInputDesc;
using pg_yaap::pipeline::FilterStep;
using pg_yaap::pipeline::FilterStepOp;
using pg_yaap::pipeline::HashJoinFilterInputDesc;
using pg_yaap::pipeline::HashJoinOutputColumnDesc;
using pg_yaap::pipeline::HashJoinOutputSide;
using OutputSink = pg_yaap::pipeline::OutputSink;
template <typename T>
using PgVector = pg_yaap::PgVector<T>;
using PipelineOperator = pg_yaap::pipeline::PhysicalOperator;
using PipelineDelimScan = pg_yaap::pipeline::PhysicalDelimScan;
using PipelineCrossProduct = pg_yaap::pipeline::PhysicalCrossProduct;
using PipelineFilter = pg_yaap::pipeline::PhysicalFilter;
using PipelineHashAggregate = pg_yaap::pipeline::PhysicalHashAggregate;
using PipelineHashJoin = pg_yaap::pipeline::PhysicalHashJoin;
using PipelinePerfectHashAggregate = pg_yaap::pipeline::PhysicalPerfectHashAggregate;
using PipelineProjection = pg_yaap::pipeline::PhysicalProjection;
using PipelineSeqScan = pg_yaap::pipeline::PhysicalSeqScan;
using PipelineTopN = pg_yaap::pipeline::PhysicalTopN;
using pg_yaap::pipeline::ProjectExprDesc;
using pg_yaap::pipeline::ProjectOp;
using pg_yaap::pipeline::ProjectStep;
using pg_yaap::pipeline::QualOp;
using pg_yaap::pipeline::SortKeyDesc;
using pg_yaap::pipeline::TdcAggKind;
using pg_yaap::pipeline::TupleDataCollection;
using pg_yaap::pipeline::TupleDataLayout;
using pg_yaap::pipeline::translator_detail::BuildAggOutputSchemaDescriptor;
using pg_yaap::pipeline::translator_detail::BuildColumnOnlyLayout;
using pg_yaap::pipeline::translator_detail::BuildColumnOnlyLayoutForRefs;
using pg_yaap::pipeline::translator_detail::BuildHashGroupLayout;
using pg_yaap::pipeline::translator_detail::BuildHashJoinOutputMappings;
using pg_yaap::pipeline::translator_detail::BuildOrderedSeqScanColumns;
using pg_yaap::pipeline::translator_detail::BuildSchemaDescriptorFromColumns;
using pg_yaap::pipeline::translator_detail::BuildSeqScanColumns;
using pg_yaap::pipeline::translator_detail::ColumnNumericScale;
using pg_yaap::pipeline::translator_detail::ColumnRef;
using pg_yaap::pipeline::translator_detail::ExtractNumericTypmodScale;
using pg_yaap::pipeline::translator_detail::MapProjectedExprSchema;
using pg_yaap::pipeline::translator_detail::LookupRawColumn;
using pg_yaap::pipeline::translator_detail::Pow10Int64;
using pg_yaap::pipeline::translator_detail::RescaleInt64Constant;

constexpr int8_t kProjectionDivisionScale = 16;
constexpr int8_t kProjectionConstDivisionScale = 6;
constexpr int16_t kAvgNumericExtraScale = 12;

inline bool
IsTransparentCastFunctionName(const std::string &function_name)
{
	return function_name == "text" ||
	       function_name == "varchar" ||
	       function_name == "bpchar" ||
	       function_name == "char" ||
	       function_name == "int8" ||
	       function_name == "int4" ||
	       function_name == "int2" ||
	       function_name == "numeric" ||
	       function_name == "float8" ||
	       function_name == "float4";
}

inline const Expression *
UnwrapTransparentCastExpr(const Expression *expr)
{
	while (true)
	{
		const auto *func = dynamic_cast<const BoundFunctionExpression *>(expr);
		if (func == nullptr || func->children.size() != 1 || !IsTransparentCastFunctionName(func->function_name))
			return expr;
		expr = func->children[0].get();
	}
}

inline const BoundColumnRefExpression *
UnwrapTransparentCastColumn(const Expression *expr)
{
	return dynamic_cast<const BoundColumnRefExpression *>(UnwrapTransparentCastExpr(expr));
}

struct SupportContext {
std::vector<std::string> stack;
};

struct MaterializedOptExpr {
const Expression *expr = nullptr;
int8_t scale = 0;
uint8_t slot = 0;
};

struct AggBuildState {
std::vector<AggFuncDesc> agg_funcs;
std::vector<TdcAggKind> agg_kinds;
std::vector<int16_t> agg_numeric_scales;
std::vector<ProjectStep> project_steps;
std::vector<ProjectExprDesc> project_exprs;
std::vector<MaterializedOptExpr> materialized_exprs;
TupleDataLayout hash_layout{};
uint32_t perfect_hash_capacity = 0;
};

struct OptimizerNodeTranslation {
std::unique_ptr<PipelineOperator> op;
std::vector<ColumnRef> cols;
std::vector<ColumnSchema> schema;
std::vector<yaap::PhysicalOperator::OutputColumn> outputs;
std::vector<SortKeyDesc> final_sort_keys;
uint64_t limit_count = 0;
uint32_t estimated_groups = 0;
};

std::string
CurrentSupportPath(const SupportContext &ctx);

OptimizerPlanSupportStatus
MakeSupportOk();

OptimizerPlanSupportStatus
MakeSupportError(const SupportContext &ctx, const char *detail);

OptimizerPlanSupportStatus
AnalyzeChildren(const PhysicalOperator &op, SupportContext &ctx, size_t expected_children);

OptimizerPlanSupportStatus
AnalyzeTableScanNode(const PhysicalTableScan &scan, SupportContext &ctx);

OptimizerPlanSupportStatus
AnalyzeFilterNode(const PhysicalFilter &filter, SupportContext &ctx);

OptimizerPlanSupportStatus
AnalyzeLimitNode(const PhysicalLimit &limit, SupportContext &ctx);

OptimizerPlanSupportStatus
AnalyzeProjectionNode(const PhysicalProjection &projection, SupportContext &ctx);

OptimizerPlanSupportStatus
AnalyzeHashJoinNode(const PhysicalHashJoin &join, SupportContext &ctx);

OptimizerPlanSupportStatus
AnalyzeCrossProductNode(const PhysicalCrossProduct &join, SupportContext &ctx);

OptimizerPlanSupportStatus
AnalyzeHashAggregateNode(const PhysicalHashAggregate &agg, SupportContext &ctx);

OptimizerPlanSupportStatus
AnalyzeDelimScanNode(const PhysicalDelimScan &scan, SupportContext &ctx);

OptimizerPlanSupportStatus
AnalyzeOrderByNode(const PhysicalOrderBy &order, SupportContext &ctx);

OptimizerPlanSupportStatus
AnalyzeOptimizerPlanNode(const PhysicalOperator &op, SupportContext &ctx);

const char *
OptimizerJoinOpTypeName(const PhysicalHashJoin &join);

bool
IsTopNNode(const PhysicalOperator &op);

const char *
OptimizerOpTypeName(const PhysicalOperator &op);

void
AppendPlanDetailList(std::string &out, const std::vector<std::string> &details);

const char *
OptimizerJoinTypeName(const PhysicalHashJoin &join);

const char *
OptimizerSetOperationName(SetOperationType setop_type);

void
CollectReferencedBindings(Expression *expr, std::set<std::pair<size_t, size_t>> &out);

bool
HasComplexAggregateInputs(const PhysicalHashAggregate &agg);

bool
HasSimpleAggregateInputs(const PhysicalHashAggregate &agg);

std::vector<std::string>
SyntheticProjectionDetails(size_t rows, size_t exprs);

bool
IsSimplePassThroughProjection(const PhysicalOperator &op);

const PhysicalOperator *
StripSimplePassThroughProjections(const PhysicalOperator *op);

const PhysicalOperator *
StripSingleExprProjectionOnAggregate(const PhysicalOperator *op);

const PhysicalOperator *
UnwrapScalarSubqueryPayload(const PhysicalOperator *op);

void
AppendSyntheticPlanNode(const std::string &name,
						   const std::vector<std::string> &details,
						   const std::string &prefix,
						   bool is_last,
						   std::string &out);

std::vector<std::string>
OptimizerPlanNodeDetails(const PhysicalOperator &op);

void
AppendOptimizerPlanNodeTree(const PhysicalOperator &op,
							 const std::string &prefix,
							 bool is_last,
							 bool is_root,
							 std::string &out,
							 const PhysicalOperator *parent = nullptr);

bool
UseInt32CharDecodeForType(Oid type_oid, int32 typmod);

dsa_pointer
BuildFilterArray(dsa_area *dsa, const void *data, size_t elem_size, size_t count);

dsa_pointer
BuildCharArray(dsa_area *dsa, const std::vector<char> &bytes);

dsa_pointer
BuildOutputTdc(dsa_area *dsa,
			   dsa_pointer layout_dp,
			   const TupleDataLayout &layout,
			   uint32_t row_capacity);

uint32_t
EstimateOutputRows(QueryDesc *qd);

uint32_t
EstimateResultRows(QueryDesc *qd, uint32_t estimated_groups);

uint32_t
EstimateInitialResultRows(QueryDesc *qd, uint32_t estimated_groups);

uint32_t
EstimateHashJoinBuildRows(size_t estimated_rows);

ColumnRef
BindingToColumnRef(const yaap::ColumnBinding &binding);

bool
SameColumnRef(const ColumnRef &lhs, const ColumnRef &rhs);

void
AppendUniqueColumnRef(const ColumnRef &ref, std::vector<ColumnRef> &out);

void
CollectReferencedColumns(const Expression *expr, std::vector<ColumnRef> &out);

void
CollectReferencedSourceColumns(const Expression *expr,
							   const PhysicalOperator *source_op,
							   std::vector<ColumnRef> &out);

void
FilterRequestedColumns(const std::vector<ColumnRef> &available,
					   const std::vector<ColumnRef> *required,
					   std::vector<ColumnRef> &out);

bool
InferSyntheticParentVarno(const std::vector<ColumnRef> *required,
						  Index &out_varno);

std::vector<ColumnRef>
BuildParentFacingOutputCols(const std::vector<ColumnRef> &selected_raw_output_cols,
							const std::vector<ColumnRef> *required_output_cols);

bool
LookupBindingColumn(const yaap::ColumnBinding &binding,
					const std::vector<ColumnRef> &cols,
					const std::vector<ColumnSchema> &schema,
					ColumnRef &out_ref,
					const ColumnSchema *&out_col);

bool
LookupNamedExprInputColumn(const BoundColumnRefExpression *expr,
						   const std::vector<yaap::PhysicalOperator::OutputColumn> *outputs,
						   const std::vector<ColumnRef> &cols,
						   const std::vector<ColumnSchema> &schema,
						   ColumnRef &out_ref,
						   const ColumnSchema *&out_col);

bool
LookupPassthroughColumn(const ColumnRef &ref,
						const std::vector<ColumnRef> &cols,
						const std::vector<ColumnSchema> &schema,
						const ColumnSchema *&out_col);

bool
IsComparisonName(const std::string &name);

bool
MapComparisonNameToQualOp(const std::string &name, QualOp &out);

bool
TryParseUInt64(const std::string &text, uint64_t &out);

bool
IsLimitCoercionFunction(const std::string &name);

bool
TryParseLimitExpression(const Expression *expr, uint64_t &out);

bool
ConvertConstantToDatum(const BoundConstantExpression *constant, Oid target_type, int32 typmod, Datum &out);

bool
ScaleNumericDatumToTargetScale(Datum numeric_datum, int8_t target_scale, int64_t &out_value);

bool
ScaleNumericConstantToInt64(const BoundConstantExpression *constant, int8_t &out_scale, int64_t &out_value);

bool
ScaleNumericConstantToTargetScale(const BoundConstantExpression *constant, int8_t target_scale, int64_t &out_value);

bool
ScaleIntegralConstantToTargetScale(const BoundConstantExpression *constant, int8_t target_scale, int64_t &out_value);

bool
ScaleConstantToTargetScale(const BoundConstantExpression *constant, int8_t target_scale, int64_t &out_value);

bool
TryExtractShortStringConst(const BoundConstantExpression *constant, uint8_t &out_len, int64_t &out_value);

bool
StoreStringConstBytes(const BoundConstantExpression *constant,
					  Oid type_oid,
					  int32 typmod,
					  std::vector<char> &pool,
					  uint32_t &out_offset,
					  uint32_t &out_len,
					  uint64_t &out_inline_value);

bool
ExtractCharFilterConst(const BoundConstantExpression *constant, int32_t &out_value);

bool
TryExtractLikePattern(const BoundConstantExpression *constant,
					  bool &out_prefix,
					  std::string &out_match);

bool
EvaluateDateExpression(const Expression *expr, DateADT &out);

std::vector<bool>
ParseOrderDirections(const char *source_text, size_t nkeys);

bool
BuildAllTableColumnRefs(Oid relid, Index varno, std::vector<ColumnRef> &out_cols);

bool
BuildProjectedTableColumnRefs(Oid relid,
							  Index varno,
							  const std::vector<yaap::ProjectionIndex> &projected_columns,
							  std::vector<ColumnRef> &out_cols);

bool
LookupOrAddScanFilterInput(const ColumnSchema &col,
						   std::vector<FilterInputDesc> &inputs,
						   uint16_t &out_idx);

bool
LookupOrAddScanFilterInputAs(const ColumnSchema &col,
							 ColumnDecodeKind target_decode_kind,
							 uint8_t target_numeric_scale,
							 std::vector<FilterInputDesc> &inputs,
							 uint16_t &out_idx);

bool
LookupOrAddJoinFilterInput(const ColumnRef &ref,
						  const std::vector<ColumnRef> &left_cols,
						  const std::vector<ColumnSchema> &left_schema,
						  const std::vector<ColumnRef> &right_cols,
						  const std::vector<ColumnSchema> &right_schema,
						  std::vector<HashJoinFilterInputDesc> &inputs,
						  uint16_t &out_idx,
						  const ColumnSchema *&out_col);

bool
LookupOrAddJoinFilterInputAs(const ColumnRef &ref,
							 const std::vector<ColumnRef> &left_cols,
							 const std::vector<ColumnSchema> &left_schema,
							 const std::vector<ColumnRef> &right_cols,
							 const std::vector<ColumnSchema> &right_schema,
							 ColumnDecodeKind target_decode_kind,
							 uint8_t target_numeric_scale,
							 std::vector<HashJoinFilterInputDesc> &inputs,
							 uint16_t &out_idx,
							 const ColumnSchema *&out_col);

bool
AppendFilterExpr(std::vector<FilterExprDesc> &exprs,
				 const std::vector<FilterStep> &steps,
				 size_t first_step_idx,
				 uint16_t out_bool_reg);

bool
LowerScanFilterBoolExpr(const Expression *expr,
						const std::vector<ColumnRef> &cols,
						const std::vector<ColumnSchema> &schema,
						std::vector<FilterInputDesc> &inputs,
						std::vector<FilterStep> &steps,
						std::vector<char> &string_consts,
						uint16_t &next_bool_reg,
						uint16_t &out_bool_reg);

bool
LowerJoinFilterBoolExpr(const Expression *expr,
						const PhysicalOperator *left_source_op,
						const PhysicalOperator *right_source_op,
						const std::vector<yaap::PhysicalOperator::OutputColumn> *left_outputs,
						const std::vector<ColumnRef> &left_cols,
						const std::vector<ColumnSchema> &left_schema,
						const std::vector<yaap::PhysicalOperator::OutputColumn> *right_outputs,
						const std::vector<ColumnRef> &right_cols,
						const std::vector<ColumnSchema> &right_schema,
						std::vector<HashJoinFilterInputDesc> &inputs,
						std::vector<FilterStep> &steps,
						std::vector<char> &string_consts,
						uint16_t &next_bool_reg,
						uint16_t &out_bool_reg);

bool
LowerScanFilterCompare(const BoundFunctionExpression *func,
					   const std::vector<yaap::PhysicalOperator::OutputColumn> *outputs,
					   const std::vector<ColumnRef> &cols,
					   const std::vector<ColumnSchema> &schema,
					   std::vector<FilterInputDesc> &inputs,
					   std::vector<FilterStep> &steps,
					   std::vector<char> &string_consts,
					   uint16_t &next_bool_reg,
					   uint16_t &out_bool_reg);

bool
LowerScanFilterBoolExpr(const Expression *expr,
						const std::vector<ColumnRef> &cols,
						const std::vector<ColumnSchema> &schema,
						std::vector<FilterInputDesc> &inputs,
						std::vector<FilterStep> &steps,
						std::vector<char> &string_consts,
						uint16_t &next_bool_reg,
						uint16_t &out_bool_reg);

bool
LowerJoinFilterCompare(const BoundFunctionExpression *func,
					   const PhysicalOperator *left_source_op,
					   const PhysicalOperator *right_source_op,
					   const std::vector<yaap::PhysicalOperator::OutputColumn> *left_outputs,
					   const std::vector<ColumnRef> &left_cols,
					   const std::vector<ColumnSchema> &left_schema,
					   const std::vector<yaap::PhysicalOperator::OutputColumn> *right_outputs,
					   const std::vector<ColumnRef> &right_cols,
					   const std::vector<ColumnSchema> &right_schema,
					   std::vector<HashJoinFilterInputDesc> &inputs,
					   std::vector<FilterStep> &steps,
					   std::vector<char> &string_consts,
					   uint16_t &next_bool_reg,
					   uint16_t &out_bool_reg);

bool
LowerJoinFilterBoolExpr(const Expression *expr,
						const PhysicalOperator *left_source_op,
						const PhysicalOperator *right_source_op,
						const std::vector<yaap::PhysicalOperator::OutputColumn> *left_outputs,
						const std::vector<ColumnRef> &left_cols,
						const std::vector<ColumnSchema> &left_schema,
						const std::vector<yaap::PhysicalOperator::OutputColumn> *right_outputs,
						const std::vector<ColumnRef> &right_cols,
						const std::vector<ColumnSchema> &right_schema,
						std::vector<HashJoinFilterInputDesc> &inputs,
						std::vector<FilterStep> &steps,
						std::vector<char> &string_consts,
						uint16_t &next_bool_reg,
						uint16_t &out_bool_reg);

bool
LowerScanFilters(const std::vector<Expression *> &filters,
				 const std::vector<yaap::PhysicalOperator::OutputColumn> *outputs,
				 const std::vector<ColumnRef> &cols,
				 const std::vector<ColumnSchema> &schema,
				 std::vector<FilterInputDesc> &inputs,
				 std::vector<FilterExprDesc> &exprs,
				 std::vector<FilterStep> &steps,
				 std::vector<char> &string_consts);

bool
LowerJoinFilters(const std::vector<Expression *> &filters,
				 const PhysicalOperator *left_source_op,
				 const PhysicalOperator *right_source_op,
				 const std::vector<yaap::PhysicalOperator::OutputColumn> *left_outputs,
				 const std::vector<ColumnRef> &left_cols,
				 const std::vector<ColumnSchema> &left_schema,
				 const std::vector<yaap::PhysicalOperator::OutputColumn> *right_outputs,
				 const std::vector<ColumnRef> &right_cols,
				 const std::vector<ColumnSchema> &right_schema,
				 std::vector<HashJoinFilterInputDesc> &inputs,
				 std::vector<FilterExprDesc> &exprs,
				 std::vector<FilterStep> &steps,
				 std::vector<char> &string_consts,
				 uint16_t &out_bool_regs);

bool
LookupCachedOptimizerExpr(const Expression *expr,
						  const std::vector<MaterializedOptExpr> *cache,
						  int8_t &out_scale,
						  uint8_t &out_slot);

bool
AppendScaleProjectStep(uint8_t input_slot,
					   int8_t input_scale,
					   int8_t target_scale,
					   std::vector<ProjectStep> &steps,
					   uint8_t &next_int64_slot,
					   uint8_t &out_slot);

bool
LowerOptimizerBoolExpr(const Expression *expr,
					   std::vector<ProjectStep> &steps,
					   uint8_t &next_int64_slot,
					   const std::vector<ColumnRef> &cols,
					   const std::vector<ColumnSchema> &schema,
					   uint8_t &out_slot);

bool
LowerOptimizerExpr(const Expression *expr,
				   std::vector<ProjectStep> &steps,
				   uint8_t &next_int64_slot,
				   const std::vector<ColumnRef> &cols,
				   const std::vector<ColumnSchema> &schema,
				   const std::vector<MaterializedOptExpr> *cache,
				   int8_t &out_scale,
				   uint8_t &out_slot);

bool
LowerOptimizerBoolExpr(const Expression *expr,
					   std::vector<ProjectStep> &steps,
					   uint8_t &next_int64_slot,
					   const std::vector<ColumnRef> &cols,
					   const std::vector<ColumnSchema> &schema,
					   uint8_t &out_slot);

bool
LowerProjectionConstant(const BoundConstantExpression *constant,
						std::vector<ProjectStep> &steps,
						uint8_t &next_int64_slot,
						int8_t &out_scale,
						uint8_t &out_slot);

bool
LowerProjectionStringCompare(const BoundFunctionExpression *func,
							 std::vector<ProjectStep> &steps,
							 uint8_t &next_int64_slot,
							 const std::vector<ColumnRef> &cols,
							 const std::vector<ColumnSchema> &schema,
							 uint8_t &out_slot);

bool
LowerProjectionStringLike(const BoundFunctionExpression *func,
						  std::vector<ProjectStep> &steps,
						  uint8_t &next_int64_slot,
						  const std::vector<ColumnRef> &cols,
						  const std::vector<ColumnSchema> &schema,
						  uint8_t &out_slot);

bool
LowerProjectionStringPrefixSlice(const BoundFunctionExpression *func,
								 std::vector<ProjectStep> &steps,
								 uint8_t &next_string_slot,
								 const std::vector<ColumnRef> &cols,
								 const std::vector<ColumnSchema> &schema,
								 uint8_t &out_slot);

bool
LowerOptimizerBoolExpr(const Expression *expr,
					   std::vector<ProjectStep> &steps,
					   uint8_t &next_int64_slot,
					   const std::vector<ColumnRef> &cols,
					   const std::vector<ColumnSchema> &schema,
					   uint8_t &out_slot);

bool
LowerExtractYearExpr(const BoundFunctionExpression *func,
					 std::vector<ProjectStep> &steps,
					 uint8_t &next_int64_slot,
					 const std::vector<ColumnRef> &cols,
					 const std::vector<ColumnSchema> &schema,
					 int8_t &out_scale,
					 uint8_t &out_slot);

bool
LowerNumericBinaryExpr(const BoundFunctionExpression *func,
					   std::vector<ProjectStep> &steps,
					   uint8_t &next_int64_slot,
					   const std::vector<ColumnRef> &cols,
					   const std::vector<ColumnSchema> &schema,
					   const std::vector<MaterializedOptExpr> *cache,
					   int8_t &out_scale,
					   uint8_t &out_slot);

bool
LowerCaseExpr(const BoundFunctionExpression *func,
			  std::vector<ProjectStep> &steps,
			  uint8_t &next_int64_slot,
			  const std::vector<ColumnRef> &cols,
			  const std::vector<ColumnSchema> &schema,
			  const std::vector<MaterializedOptExpr> *cache,
			  int8_t &out_scale,
			  uint8_t &out_slot);

bool
LowerOptimizerExpr(const Expression *expr,
				   std::vector<ProjectStep> &steps,
				   uint8_t &next_int64_slot,
				   const std::vector<ColumnRef> &cols,
				   const std::vector<ColumnSchema> &schema,
				   const std::vector<MaterializedOptExpr> *cache,
				   int8_t &out_scale,
				   uint8_t &out_slot);

bool
InferProjectionExprSchema(const Expression *expr,
						  const std::vector<ColumnRef> &cols,
						  const std::vector<ColumnSchema> &schema,
						  Oid &out_type_oid,
						  int32 &out_typmod,
						  int8_t &out_scale);

uint8_t
NextFreeInt64Slot(const std::vector<ColumnSchema> &schema);

uint8_t
NextFreeStringSlot(const std::vector<ColumnSchema> &schema);

bool
ClassifyOptimizerAggregate(const BoundAggregateExpression *agg,
						   const std::vector<yaap::PhysicalOperator::OutputColumn> *input_outputs,
						   const std::vector<ColumnRef> &cols,
						   const std::vector<ColumnSchema> &schema,
						   std::vector<ProjectStep> &project_steps,
						   std::vector<ProjectExprDesc> &project_exprs,
						   std::vector<MaterializedOptExpr> &materialized_exprs,
						   uint8_t &next_int64_slot,
						   AggFuncDesc &out_desc,
						   TdcAggKind &out_kind,
						   int16_t &out_numeric_scale);

bool
BuildOptimizerAggOutput(const PhysicalHashAggregate &agg,
						const std::vector<yaap::PhysicalOperator::OutputColumn> *input_outputs,
						const std::vector<ColumnRef> &input_cols,
						const std::vector<ColumnSchema> &input_schema,
						const AggBuildState &agg_state,
						std::vector<ColumnRef> &out_cols,
						std::vector<ColumnSchema> &out_schema);

bool
BuildOrderedOutputBindingsForRefs(const std::vector<ColumnRef> &requested_refs,
								  const std::vector<ColumnRef> &raw_refs,
								  const std::vector<yaap::PhysicalOperator::OutputColumn> &raw_outputs,
								  std::vector<yaap::PhysicalOperator::OutputColumn> &out_bindings);

bool
ApplyPostAggregateFilters(OptimizerNodeTranslation node,
						  const PhysicalHashAggregate &source_agg,
						  const std::vector<Expression *> &filters,
						  PgYaapQueryState *state,
						  OptimizerNodeTranslation &out);

bool
ApplyPipelineFilters(OptimizerNodeTranslation node,
					const PhysicalOperator *source_op,
					const std::vector<Expression *> &filters,
					PgYaapQueryState *state,
					OptimizerNodeTranslation &out);

bool
CollectJoinKeys(const Expression *expr,
				const std::vector<yaap::PhysicalOperator::OutputColumn> *left_outputs,
				const std::vector<ColumnRef> &left_cols,
				const std::vector<ColumnSchema> &left_schema,
				const std::vector<yaap::PhysicalOperator::OutputColumn> *right_outputs,
				const std::vector<ColumnRef> &right_cols,
				const std::vector<ColumnSchema> &right_schema,
				std::vector<ColumnRef> &left_keys,
				std::vector<ColumnRef> &right_keys,
				std::vector<Expression *> &residuals);

std::unique_ptr<PipelineOperator>
BuildOutputContract(OptimizerNodeTranslation &node,
					QueryDesc *queryDesc,
					PgYaapQueryState *state);

bool
TryBuildPureProjection(const PhysicalProjection &projection,
					   OptimizerNodeTranslation &child,
					   OptimizerNodeTranslation &out);

bool
TranslateProjectionNode(const PhysicalProjection &projection,
						QueryDesc *queryDesc,
						PgYaapQueryState *state,
						const std::vector<ColumnRef> *required_output_cols,
						OptimizerNodeTranslation &out);

bool
TranslateTableScanNode(const PhysicalTableScan &scan,
					   PgYaapQueryState *state,
					   const std::vector<ColumnRef> *required_output_cols,
					   OptimizerNodeTranslation &out);

bool
TranslateHashAggregateNode(const PhysicalHashAggregate &agg,
						   QueryDesc *queryDesc,
						   PgYaapQueryState *state,
						   OptimizerNodeTranslation &out);

bool
TranslateDelimScanNode(const PhysicalDelimScan &scan,
					   QueryDesc *queryDesc,
					   PgYaapQueryState *state,
					   OptimizerNodeTranslation &out);

bool
TranslateHashJoinNode(const PhysicalHashJoin &join,
					  QueryDesc *queryDesc,
					  PgYaapQueryState *state,
					  const std::vector<ColumnRef> *required_output_cols,
					  OptimizerNodeTranslation &out);

bool
TranslateCrossProductNode(const PhysicalCrossProduct &join,
						  QueryDesc *queryDesc,
						  PgYaapQueryState *state,
						  const std::vector<ColumnRef> *required_output_cols,
						  OptimizerNodeTranslation &out);

bool
TranslateOptimizerNode(const PhysicalOperator &op,
					   QueryDesc *queryDesc,
					   PgYaapQueryState *state,
					   const std::vector<ColumnRef> *required_output_cols,
					   OptimizerNodeTranslation &out);

bool
BuildAllProjectionColumnRefs(const PhysicalProjection &projection,
							 const PhysicalTableScan &scan,
							 std::vector<ColumnRef> &out_cols);

bool
ExtractScanShape(const PhysicalOperator &op,
				 const PhysicalTableScan *&out_scan,
				 std::vector<ColumnRef> &out_cols);

}  // namespace optimizer_translator_detail
}  // namespace pg_yaap
