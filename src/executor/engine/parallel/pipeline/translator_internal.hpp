#pragma once

extern "C" {
#include "postgres.h"
#include "executor/execdesc.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/dsa.h"
}

#include <vector>

#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/tuple_data_layout.hpp"

namespace pg_yaap {
namespace pipeline {
namespace translator_detail {

struct MaterializedProjectExpr {
	Expr   *expr;
	int8_t  scale;
	uint8_t slot;
};

struct ColumnRef {
	Index      varno = 0;
	AttrNumber attno = InvalidAttrNumber;

	bool operator==(const ColumnRef &other) const
	{
		return varno == other.varno && attno == other.attno;
	}
};

Expr *StripRelabels(Expr *expr);
bool Pow10Int64(int exp, int64_t &out);
bool RescaleInt64Constant(int64_t value, int8_t from_scale, int8_t to_scale, int64_t &out);
bool LookupRawColumn(const ColumnRef &ref,
			     const std::vector<ColumnRef> &raw_cols_ref,
			     const std::vector<ColumnSchema> &raw_cols,
			     const ColumnSchema *&out_col);
bool ColumnNumericScale(const ColumnSchema &col, int8_t &out);
int16_t ExtractNumericTypmodScale(int32 typmod);
bool ScaleNumericConstDatumToInt64(Const *c, int8_t &out_scale, int64_t &out_value);
bool ScaleNumericConstDatumToTargetScale(Const *c, int8_t target_scale, int64_t &out_value);
bool ExtractStringLikePrefix(Const *c,
				    std::vector<char> &pool,
				    uint32_t &out_offset,
				    uint32_t &out_len,
				    uint64_t &out_value);
bool ResolvePlanVarToColumnRef(Var *var, Plan *context_plan, ColumnRef &out_ref);
bool ResolvePlanExprToColumnRef(Expr *expr, Plan *context_plan, ColumnRef &out_ref);
bool IsBareVarArg(Expr *arg, Plan *context_plan, ColumnRef &out_ref);
bool CollectAggrefArgCols(const std::vector<Aggref *> &aggrefs,
			  Plan *context_plan,
			  std::vector<ColumnRef> &out);
bool CollectExprVarCols(Expr *expr,
			 Plan *context_plan,
			 std::vector<ColumnRef> &out);
bool ClassifyAggref(Aggref *ag,
		    const std::vector<ColumnRef> &raw_cols_ref,
		    const std::vector<ColumnSchema> &raw_cols,
		    Plan *context_plan,
		    std::vector<ProjectStep> &project_steps,
		    std::vector<ProjectExprDesc> &project_exprs,
		    std::vector<MaterializedProjectExpr> &materialized_exprs,
		    uint8_t &next_int64_slot,
		    AggFuncDesc &out_desc,
		    TdcAggKind &out_kind,
		    int16_t &out_numeric_scale);
bool LowerProjectionExpr(Expr *expr,
				std::vector<ProjectStep> &steps,
				uint8_t &next_int64_slot,
				const std::vector<ColumnRef> &raw_cols_ref,
				const std::vector<ColumnSchema> &raw_cols,
				Plan *context_plan,
				const std::vector<MaterializedProjectExpr> *cache,
				int8_t &out_result_scale,
				uint8_t &out_result_slot);

bool TryBuildPerfectHashSpec(const std::vector<ColumnRef> &group_cols,
			     const std::vector<ColumnRef> &input_cols,
			     const std::vector<ColumnSchema> &input_columns,
			     uint32_t &out_capacity);
bool BuildOrderedSeqScanColumns(Oid relid,
				 const std::vector<ColumnRef> &cols,
				 Index expected_varno,
				 std::vector<ColumnSchema> &out);
bool BuildSeqScanColumns(Oid relid,
			 const std::vector<ColumnRef> &cols,
			 Index expected_varno,
			 std::vector<ColumnSchema> &out,
			 uint8_t &next_int32_slot,
			 uint8_t &next_int64_slot,
			 uint8_t &next_double_slot);
dsa_pointer BuildSchemaDescriptorFromColumns(const std::vector<ColumnSchema> &columns, dsa_area *dsa);
dsa_pointer BuildAggOutputSchemaDescriptor(const std::vector<ColumnRef> &group_cols,
					 const std::vector<ColumnRef> &available_cols,
					 const std::vector<ColumnSchema> &available_schema,
					 const std::vector<TdcAggKind> &agg_kinds,
					 dsa_area *dsa);
bool BuildColumnOnlyLayout(const std::vector<ColumnSchema> &columns, TupleDataLayout &out);
bool BuildColumnOnlyLayoutForRefs(const std::vector<ColumnRef> &refs,
				  const std::vector<ColumnRef> &available_cols,
				  const std::vector<ColumnSchema> &available_schema,
				  TupleDataLayout &out);
bool BuildHashJoinOutputMappings(const std::vector<ColumnRef> &output_cols,
				 const std::vector<ColumnRef> &left_cols,
				 const std::vector<ColumnSchema> &left_schema,
				 const std::vector<ColumnRef> &right_cols,
				 const std::vector<ColumnSchema> &right_schema,
				 std::vector<HashJoinOutputColumnDesc> &out_mappings,
				 std::vector<ColumnSchema> &out_schema);
bool BuildHashGroupLayout(const std::vector<ColumnRef> &group_cols,
			  const std::vector<ColumnRef> &input_cols,
			  const std::vector<ColumnSchema> &input_columns,
			  const std::vector<AggFuncDesc> &agg_funcs,
			  const std::vector<TdcAggKind> &agg_kinds,
			  const std::vector<int16_t> &agg_numeric_scales,
			  TupleDataLayout &out);
bool MapProjectedExprSchema(Oid type_oid,
			 int32 typmod,
			 int8_t numeric_scale,
			 uint8_t slot,
			 ColumnSchema &out);

bool ExtractFilterQual(List *qual,
		       Oid relid,
		       std::vector<FilterInputDesc> &inputs,
		       std::vector<FilterExprDesc> &exprs,
		       std::vector<FilterStep> &steps,
		       std::vector<char> &string_consts,
		       uint16_t &next_bool_reg);
bool ExtractHashJoinFilterQual(List *qual,
			      Plan *context_plan,
			      const std::vector<ColumnRef> &left_cols,
			      const std::vector<ColumnSchema> &left_schema,
			      const std::vector<ColumnRef> &right_cols,
			      const std::vector<ColumnSchema> &right_schema,
			      std::vector<HashJoinFilterInputDesc> &inputs,
			      std::vector<FilterExprDesc> &exprs,
			      std::vector<FilterStep> &steps,
			      std::vector<char> &string_consts,
			      uint16_t &next_bool_reg);

}  /* namespace translator_detail */
}  /* namespace pipeline */
}  /* namespace pg_yaap */
