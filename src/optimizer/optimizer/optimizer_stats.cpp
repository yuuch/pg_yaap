extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "catalog/pg_statistic.h"
#include "fmgr.h"
#include "optimizer/plancat.h"
#include "utils/rel.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
}

#include "optimizer_stats.hpp"

#include <algorithm>
#include <cstdlib>
#include <cmath>
#include <sstream>

namespace yaap {

namespace {

struct ExtractedStatsSlot {
    Oid valuetype = InvalidOid;
    Oid stacoll = InvalidOid;
    Oid staop = InvalidOid;
    int16 typlen = 0;
    bool typbyval = false;
    std::vector<Datum> values;
    std::vector<double> numbers;
};

struct ColumnFilterConstraint {
    BoundConstantExpression* constant = nullptr;
    std::string op_name;
    Expression* expression = nullptr;
};

size_t ClampCardinality(double value, size_t upper_bound) {
    if (value <= 0.0) {
        return 0;
    }
    auto rounded = static_cast<size_t>(std::ceil(value));
    if (upper_bound != 0) {
        rounded = std::min(rounded, upper_bound);
    }
    return std::max<size_t>(rounded, 1);
}

size_t DistinctFromPG(double stadistinct, size_t cardinality) {
    if (cardinality == 0) {
        return 0;
    }
    if (stadistinct > 0.0) {
        return ClampCardinality(stadistinct, cardinality);
    }
    if (stadistinct < 0.0) {
        return ClampCardinality(std::abs(stadistinct) * static_cast<double>(cardinality), cardinality);
    }
    return cardinality;
}

size_t EstimateBaseRelationCardinality(Relation rel, size_t fallback) {
    if (rel == nullptr) {
        return fallback;
    }

    BlockNumber pages = 0;
    double reltuples = 0;
    double allvisfrac = 0;
    estimate_rel_size(rel, nullptr, &pages, &reltuples, &allvisfrac);
    if (reltuples <= 0) {
        return fallback;
    }
    return std::max<size_t>(1, static_cast<size_t>(std::ceil(reltuples)));
}

void MergeColumnStats(RelationStats& target, const RelationStats& source) {
    target.column_distinct_count.insert(target.column_distinct_count.end(),
                                        source.column_distinct_count.begin(),
                                        source.column_distinct_count.end());
    target.column_names.insert(target.column_names.end(), source.column_names.begin(), source.column_names.end());
    for (const auto& entry : source.column_stats) {
        target.column_stats[entry.first] = entry.second;
    }
    for (const auto& entry : source.aggregate_output_stats) {
        target.aggregate_output_stats[entry.first] = entry.second;
    }
}

bool IsComparisonOperator(const std::string& op_name) {
    return op_name == "=" || op_name == ">" || op_name == ">=" || op_name == "<" || op_name == "<=";
}

bool IsDynamicComparison(Expression* expression) {
    if (!expression || expression->type != ExpressionType::BOUND_FUNCTION) {
        return false;
    }

    auto* function = static_cast<BoundFunctionExpression*>(expression);
    if (!IsComparisonOperator(function->function_name) || function->children.size() != 2) {
        return false;
    }

    return function->children[0] &&
           function->children[1] &&
           function->children[0]->type != ExpressionType::BOUND_CONSTANT &&
           function->children[1]->type != ExpressionType::BOUND_CONSTANT;
}

bool IsArrayMembershipPredicate(Expression* expression) {
    if (!expression || expression->type != ExpressionType::BOUND_FUNCTION) {
        return false;
    }

    auto* function = static_cast<BoundFunctionExpression*>(expression);
    return (function->function_name == "any" || function->function_name == "all") &&
           !function->children.empty();
}

bool ParseNumericConstant(const BoundConstantExpression* constant, double& value) {
    if (!constant || constant->is_null) {
        return false;
    }

    char* end = nullptr;
    const char* text = constant->value.c_str();
    value = std::strtod(text, &end);
    if (end == text || (end && *end != '\0')) {
        return false;
    }
    return true;
}

bool ParseLimitValue(const BoundConstantExpression* constant, size_t& value) {
    double parsed = 0.0;
    if (!ParseNumericConstant(constant, parsed)) {
        return false;
    }
    if (parsed < 0.0) {
        value = 0;
        return true;
    }
    value = static_cast<size_t>(std::floor(parsed));
    return true;
}

bool IsLimitCoercionFunction(const std::string& function_name) {
    return function_name == "int8" || function_name == "int4" || function_name == "int2" ||
           function_name == "numeric" || function_name == "float8" || function_name == "float4";
}

bool TryParseLimitExpression(Expression* expression, size_t& value) {
    if (!expression) {
        return false;
    }

    if (expression->type == ExpressionType::BOUND_CONSTANT) {
        return ParseLimitValue(static_cast<BoundConstantExpression*>(expression), value);
    }

    if (expression->type == ExpressionType::BOUND_FUNCTION) {
        auto* function = static_cast<BoundFunctionExpression*>(expression);
        if (function->children.size() == 1 && IsLimitCoercionFunction(function->function_name)) {
            return TryParseLimitExpression(function->children[0].get(), value);
        }
    }

    return false;
}

bool ConvertConstantToDatum(const BoundConstantExpression* constant, Oid target_type, Datum& value) {
    if (!constant || constant->is_null || !OidIsValid(target_type)) {
        return false;
    }

    Oid input_func = InvalidOid;
    Oid ioparam = InvalidOid;
    getTypeInputInfo(target_type, &input_func, &ioparam);
    value = OidInputFunctionCall(input_func, const_cast<char*>(constant->value.c_str()), ioparam, -1);
    return true;
}

double SumFrequencies(const std::vector<double>& numbers) {
    double total = 0.0;
    for (double number : numbers) {
        total += number;
    }
    return total;
}

bool CompareDatums(Datum left, Datum right, Oid opno, Oid collation) {
    if (!OidIsValid(opno)) {
        return false;
    }

    FmgrInfo cmp;
    fmgr_info(get_opcode(opno), &cmp);
    return DatumGetBool(FunctionCall2Coll(&cmp, collation, left, right));
}

bool DatumEquals(Datum left, Datum right, bool typbyval, int16 typlen) {
    return datumIsEqual(left, right, typbyval, typlen);
}

bool DatumSatisfiesComparison(Datum value,
                              Datum constant,
                              const std::string& op_name,
                              const ColumnStats& column) {
    bool equal = DatumEquals(value, constant, column.type_by_val, column.type_len);
    if (op_name == "=") {
        return equal;
    }
    if (!OidIsValid(column.sort_op)) {
        return false;
    }
    bool value_lt_const = CompareDatums(value, constant, column.sort_op, column.collation_oid);
    bool const_lt_value = CompareDatums(constant, value, column.sort_op, column.collation_oid);
    if (op_name == "<") {
        return value_lt_const;
    }
    if (op_name == "<=") {
        return value_lt_const || equal;
    }
    if (op_name == ">") {
        return const_lt_value;
    }
    if (op_name == ">=") {
        return const_lt_value || equal;
    }
    return false;
}

std::string DistinctExpressionKey(Expression* expression) {
    if (!expression) {
        return "<null>";
    }

    std::stringstream ss;
    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* column = static_cast<BoundColumnRefExpression*>(expression);
            ss << "col:" << column->binding.table_index.index << "."
               << column->binding.column_index.index;
            break;
        }
        case ExpressionType::BOUND_CONSTANT: {
            auto* constant = static_cast<BoundConstantExpression*>(expression);
            ss << "const:" << (constant->is_null ? "NULL" : constant->value);
            break;
        }
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            ss << "fn:" << function->function_name << "(";
            for (size_t i = 0; i < function->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << DistinctExpressionKey(function->children[i].get());
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression);
            ss << "agg:" << aggregate->function_name << "(";
            for (size_t i = 0; i < aggregate->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << DistinctExpressionKey(aggregate->children[i].get());
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            ss << "conj:" << conjunction->bool_expr_type << "(";
            for (size_t i = 0; i < conjunction->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << DistinctExpressionKey(conjunction->children[i].get());
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subquery = static_cast<BoundSubqueryExpression*>(expression);
            ss << "subquery:" << subquery->sublink_name << "(";
            for (size_t i = 0; i < subquery->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << DistinctExpressionKey(subquery->children[i].get());
            }
            ss << ")";
            break;
        }
        default:
            ss << "opaque";
            break;
    }
    return ss.str();
}

bool LoadStatsSlot(HeapTuple statstuple, int reqkind, Oid reqop, int flags, ExtractedStatsSlot& out) {
    AttStatsSlot slot;
    if (!get_attstatsslot(&slot, statstuple, reqkind, reqop, flags)) {
        return false;
    }

    out.valuetype = slot.valuetype;
    out.stacoll = slot.stacoll;
    out.staop = slot.staop;
    get_typlenbyval(slot.valuetype, &out.typlen, &out.typbyval);

    if (flags & ATTSTATSSLOT_VALUES) {
        out.values.reserve(slot.nvalues);
        for (int i = 0; i < slot.nvalues; ++i) {
            out.values.push_back(datumCopy(slot.values[i], out.typbyval, out.typlen));
        }
    }

    if (flags & ATTSTATSSLOT_NUMBERS) {
        out.numbers.reserve(slot.nnumbers);
        for (int i = 0; i < slot.nnumbers; ++i) {
            out.numbers.push_back(slot.numbers[i]);
        }
    }

    free_attstatsslot(&slot);
    return true;
}

bool IsEqualityComparison(const std::string& op_name) {
    return op_name == "=";
}

bool IsRangeComparison(const std::string& op_name) {
    return op_name == "<" || op_name == "<=" || op_name == ">" || op_name == ">=";
}

double EstimateEqualitySelectivity(const ColumnStats& column, BoundConstantExpression* constant) {
    Datum const_datum = 0;
    if (!ConvertConstantToDatum(constant, column.type_oid, const_datum)) {
        return -1.0;
    }

    for (size_t i = 0; i < column.mcv_values.size() && i < column.mcv_frequencies.size(); ++i) {
        if (DatumEquals(column.mcv_values[i], const_datum, column.type_by_val, column.type_len)) {
            return column.mcv_frequencies[i];
        }
    }

    double non_null = std::max(0.0, 1.0 - column.null_fraction);
    double non_mcv = std::max(0.0, non_null - column.mcv_total_frequency);
    size_t other_distinct = 0;
    if (column.distinct.distinct_count > column.mcv_values.size()) {
        other_distinct = column.distinct.distinct_count - column.mcv_values.size();
    }
    if (other_distinct == 0) {
        other_distinct = 1;
    }
    return non_mcv / static_cast<double>(other_distinct);
}

double EstimateRangeSelectivity(const ColumnStats& column,
                                BoundConstantExpression* constant,
                                const std::string& op_name) {
    if (column.histogram_bounds.empty()) {
        return -1.0;
    }

    Datum const_datum = 0;
    if (!ConvertConstantToDatum(constant, column.type_oid, const_datum)) {
        return -1.0;
    }

    if (!OidIsValid(column.sort_op)) {
        return -1.0;
    }

    double histogram_fraction = 0.0;
    for (const auto& bound : column.histogram_bounds) {
        bool satisfies = false;
        bool equal = DatumEquals(bound, const_datum, column.type_by_val, column.type_len);
        bool bound_lt_const = CompareDatums(bound, const_datum, column.sort_op, column.collation_oid);
        bool const_lt_bound = CompareDatums(const_datum, bound, column.sort_op, column.collation_oid);

        if (op_name == "<") {
            satisfies = bound_lt_const;
        } else if (op_name == "<=") {
            satisfies = bound_lt_const || equal;
        } else if (op_name == ">") {
            satisfies = const_lt_bound;
        } else if (op_name == ">=") {
            satisfies = const_lt_bound || equal;
        }

        if (satisfies) {
            histogram_fraction += 1.0;
        }
    }

    histogram_fraction /= static_cast<double>(column.histogram_bounds.size());

    double non_null = std::max(0.0, 1.0 - column.null_fraction);
    double non_mcv = std::max(0.0, non_null - column.mcv_total_frequency);

    double mcv_fraction = 0.0;
    for (size_t i = 0; i < column.mcv_values.size() && i < column.mcv_frequencies.size(); ++i) {
        Datum mcv = column.mcv_values[i];
        bool equal = DatumEquals(mcv, const_datum, column.type_by_val, column.type_len);
        bool satisfies = false;
        bool mcv_lt_const = CompareDatums(mcv, const_datum, column.sort_op, column.collation_oid);
        bool const_lt_mcv = CompareDatums(const_datum, mcv, column.sort_op, column.collation_oid);

        if (op_name == "<") {
            satisfies = mcv_lt_const;
        } else if (op_name == "<=") {
            satisfies = mcv_lt_const || equal;
        } else if (op_name == ">") {
            satisfies = const_lt_mcv;
        } else if (op_name == ">=") {
            satisfies = const_lt_mcv || equal;
        }

        if (satisfies) {
            mcv_fraction += column.mcv_frequencies[i];
        }
    }

    double selectivity = mcv_fraction + histogram_fraction * non_mcv;
    return std::clamp(selectivity, 0.0, 1.0);
}

double EstimateColumnComparisonSelectivity(const ColumnStats& column,
                                           BoundConstantExpression* constant,
                                           const std::string& op_name) {
    if (IsEqualityComparison(op_name)) {
        return EstimateEqualitySelectivity(column, constant);
    }
    if (IsRangeComparison(op_name)) {
        return EstimateRangeSelectivity(column, constant, op_name);
    }
    return -1.0;
}

double EstimateCombinedColumnSelectivity(const ColumnStats& column,
                                         const std::vector<ColumnFilterConstraint>& constraints) {
    if (constraints.empty()) {
        return -1.0;
    }

    Datum equality_value = 0;
    BoundConstantExpression* equality_constant = nullptr;
    for (const auto& constraint : constraints) {
        if (constraint.op_name != "=") {
            continue;
        }
        Datum value = 0;
        if (!ConvertConstantToDatum(constraint.constant, column.type_oid, value)) {
            return -1.0;
        }
        if (equality_constant != nullptr &&
            !DatumEquals(value, equality_value, column.type_by_val, column.type_len)) {
            return 0.0;
        }
        equality_constant = constraint.constant;
        equality_value = value;
    }

    if (equality_constant != nullptr) {
        for (const auto& constraint : constraints) {
            if (!DatumSatisfiesComparison(equality_value, equality_value, constraint.op_name, column) &&
                constraint.op_name == "=") {
                return 0.0;
            }
            if (constraint.op_name != "=" &&
                !DatumSatisfiesComparison(equality_value,
                                          [&]() {
                                              Datum value = 0;
                                              ConvertConstantToDatum(constraint.constant, column.type_oid, value);
                                              return value;
                                          }(),
                                          constraint.op_name,
                                          column)) {
                return 0.0;
            }
        }
        return EstimateEqualitySelectivity(column, equality_constant);
    }

    if (column.histogram_bounds.empty() || !OidIsValid(column.sort_op)) {
        return -1.0;
    }

    std::vector<Datum> constraint_values;
    constraint_values.reserve(constraints.size());
    for (const auto& constraint : constraints) {
        Datum value = 0;
        if (!ConvertConstantToDatum(constraint.constant, column.type_oid, value)) {
            return -1.0;
        }
        constraint_values.push_back(value);
    }

    double histogram_fraction = 0.0;
    for (const auto& bound : column.histogram_bounds) {
        bool satisfies_all = true;
        for (size_t i = 0; i < constraints.size(); ++i) {
            if (!DatumSatisfiesComparison(bound, constraint_values[i], constraints[i].op_name, column)) {
                satisfies_all = false;
                break;
            }
        }
        if (satisfies_all) {
            histogram_fraction += 1.0;
        }
    }
    histogram_fraction /= static_cast<double>(column.histogram_bounds.size());

    double non_null = std::max(0.0, 1.0 - column.null_fraction);
    double non_mcv = std::max(0.0, non_null - column.mcv_total_frequency);

    double mcv_fraction = 0.0;
    for (size_t i = 0; i < column.mcv_values.size() && i < column.mcv_frequencies.size(); ++i) {
        bool satisfies_all = true;
        for (size_t j = 0; j < constraints.size(); ++j) {
            if (!DatumSatisfiesComparison(column.mcv_values[i], constraint_values[j], constraints[j].op_name, column)) {
                satisfies_all = false;
                break;
            }
        }
        if (satisfies_all) {
            mcv_fraction += column.mcv_frequencies[i];
        }
    }

    return std::clamp(mcv_fraction + histogram_fraction * non_mcv, 0.0, 1.0);
}

} // namespace

ColumnBindingKey MakeColumnBindingKey(ColumnBinding binding) {
    return ColumnBindingKey{binding.table_index.index, binding.column_index.index};
}

RelationStats RelationStatisticsHelper::Extract(LogicalOperator& op) const {
    switch (op.type) {
        case LogicalOperatorType::LOGICAL_GET:
            return ExtractGetStats(static_cast<LogicalGet&>(op));
        case LogicalOperatorType::LOGICAL_DELIM_GET:
            return ExtractDelimGetStats(static_cast<LogicalDelimGet&>(op));
        case LogicalOperatorType::LOGICAL_PROJECTION:
            return ExtractProjectionStats(static_cast<LogicalProjection&>(op));
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
            return ExtractAggregateStats(static_cast<LogicalAggregate&>(op));
        case LogicalOperatorType::LOGICAL_FILTER:
            return ExtractFilterStats(static_cast<LogicalFilter&>(op));
        case LogicalOperatorType::LOGICAL_DISTINCT:
            return ExtractDistinctStats(static_cast<LogicalDistinct&>(op));
        case LogicalOperatorType::LOGICAL_SET_OPERATION: {
            RelationStats stats = ExtractUnaryStats(op);
            stats.cardinality = op.estimated_cardinality;
            return stats;
        }
        case LogicalOperatorType::LOGICAL_LIMIT:
            return ExtractLimitStats(static_cast<LogicalLimit&>(op));
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
            return ExtractJoinStats(op);
        default:
            return ExtractUnaryStats(op);
    }
}

RelationStats RelationStatisticsHelper::ExtractGetStats(LogicalGet& get) const {
    RelationStats stats;
    stats.stats_initialized = true;
    stats.table_name = get.table_name;

    if (get.relid == 0) {
        stats.cardinality = get.estimated_cardinality;
        return stats;
    }

    Relation rel = RelationIdGetRelation(static_cast<Oid>(get.relid));
    if (rel == nullptr) {
        stats.cardinality = get.estimated_cardinality;
        return stats;
    }
    const size_t base_cardinality = EstimateBaseRelationCardinality(rel, get.estimated_cardinality);
    stats.cardinality = base_cardinality;

    TupleDesc tupdesc = RelationGetDescr(rel);
    for (int attno = 1; attno <= tupdesc->natts; ++attno) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);
        if (attr->attisdropped) {
            continue;
        }

        HeapTuple tuple = SearchSysCache3(STATRELATTINH,
                                          ObjectIdGetDatum(static_cast<Oid>(get.relid)),
                                          Int16GetDatum(attno),
                                          BoolGetDatum(false));
        ColumnBinding binding{get.table_index, ProjectionIndex{static_cast<size_t>(attno - 1)}};
        Oid atttype = InvalidOid;
        int32 atttypmod = -1;
        Oid attcollation = InvalidOid;
        get_atttypetypmodcoll(get.relid, attno, &atttype, &atttypmod, &attcollation);
        int16 typLen = 0;
        bool typByVal = false;
        char typAlign = 0;
        get_typlenbyvalalign(atttype, &typLen, &typByVal, &typAlign);

        ColumnStats column;
        column.binding = binding;
        column.has_stats = true;
        column.table_name = get.table_name;
        column.column_name = NameStr(attr->attname);
        column.type_oid = atttype;
        column.collation_oid = attcollation;
        column.type_len = typLen;
        column.type_by_val = typByVal;

        if (HeapTupleIsValid(tuple)) {
            auto* pg_stats = reinterpret_cast<Form_pg_statistic>(GETSTRUCT(tuple));
            column.has_catalog_stats = true;
            column.distinct.distinct_count = DistinctFromPG(pg_stats->stadistinct, base_cardinality);
            column.null_fraction = pg_stats->stanullfrac;

            ExtractedStatsSlot mcv_slot;
            if (LoadStatsSlot(tuple, STATISTIC_KIND_MCV, InvalidOid, ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS, mcv_slot)) {
                column.mcv_total_frequency = SumFrequencies(mcv_slot.numbers);
                column.mcv_values = std::move(mcv_slot.values);
                column.mcv_frequencies = std::move(mcv_slot.numbers);
            }

            ExtractedStatsSlot histogram_slot;
            if (LoadStatsSlot(tuple, STATISTIC_KIND_HISTOGRAM, InvalidOid, ATTSTATSSLOT_VALUES, histogram_slot)) {
                column.sort_op = histogram_slot.staop;
                column.histogram_bounds = std::move(histogram_slot.values);
            }
            ReleaseSysCache(tuple);
        } else {
            column.distinct.distinct_count = std::max<size_t>(1, base_cardinality == 0 ? 1 : base_cardinality);
        }

        stats.column_distinct_count.push_back(column.distinct);
        stats.column_names.push_back(column.column_name);
        stats.column_stats[MakeColumnBindingKey(binding)] = column;
    }

    RelationClose(rel);

    if (!get.filters.empty()) {
        stats.cardinality = ClampCardinality(
            static_cast<double>(base_cardinality) * DEFAULT_SELECTIVITY,
            base_cardinality);
    }

    return stats;
}

RelationStats RelationStatisticsHelper::ExtractProjectionStats(LogicalProjection& projection) const {
    RelationStats input_stats = Extract(*projection.children[0]);
    RelationStats stats;
    stats.cardinality = projection.estimated_cardinality;
    stats.stats_initialized = input_stats.stats_initialized;
    stats.table_name = input_stats.table_name;

    for (size_t idx = 0; idx < projection.expressions.size(); ++idx) {
        const auto& expression = projection.expressions[idx];
        std::vector<ColumnBinding> bindings;
        CollectColumnRefs(expression.get(), bindings);
        if (bindings.size() == 1) {
            auto column = LookupColumnStats(input_stats, bindings[0]);
            if (column.has_stats) {
                ColumnBinding output_binding{projection.table_index, ProjectionIndex{idx}};
                column.binding = output_binding;
                column.column_name = idx < projection.output_names.size()
                    ? projection.output_names[idx]
                    : column.column_name;
                stats.column_distinct_count.push_back(column.distinct);
                stats.column_names.push_back(column.column_name);
                stats.column_stats[MakeColumnBindingKey(output_binding)] = column;
            }
        }
    }

    return stats;
}

RelationStats RelationStatisticsHelper::ExtractAggregateStats(LogicalAggregate& aggregate) const {
    RelationStats input_stats = Extract(*aggregate.children[0]);
    RelationStats stats;
    stats.cardinality = aggregate.estimated_cardinality;
    stats.stats_initialized = input_stats.stats_initialized;
    stats.table_name = input_stats.table_name;
    const size_t input_cardinality = input_stats.cardinality;

    for (size_t i = 0; i < aggregate.groups.size(); ++i) {
        const auto& group = aggregate.groups[i];
        std::vector<ColumnBinding> bindings;
        CollectColumnRefs(group.get(), bindings);
        if (bindings.size() == 1) {
            auto column = LookupColumnStats(input_stats, bindings[0]);
            if (column.has_stats) {
                ColumnBinding output_binding{aggregate.group_index, ProjectionIndex{i}};
                column.binding = output_binding;
                stats.column_distinct_count.push_back(column.distinct);
                stats.column_names.push_back(column.column_name);
                stats.column_stats[MakeColumnBindingKey(output_binding)] = column;
            }
        }
    }

    for (size_t i = 0; i < aggregate.expressions.size(); ++i) {
        auto* expression = aggregate.expressions[i].get();
        if (!expression || expression->type != ExpressionType::BOUND_AGGREGATE) {
            continue;
        }

        auto* agg_expr = static_cast<BoundAggregateExpression*>(expression);
        ColumnBinding binding{aggregate.aggregate_index, ProjectionIndex{i}};
        ColumnStats column;
        column.binding = binding;
        column.has_stats = true;
        column.table_name = "agg";
        column.column_name = i < aggregate.aggregate_names.size()
            ? aggregate.aggregate_names[i]
            : agg_expr->function_name;
        column.distinct.distinct_count = std::max<size_t>(1, stats.cardinality == 0 ? 1 : stats.cardinality);
        stats.column_distinct_count.push_back(column.distinct);
        stats.column_names.push_back(column.column_name);
        stats.column_stats[MakeColumnBindingKey(binding)] = column;

        AggregateOutputStats output_stats;
        output_stats.has_stats = true;
        output_stats.is_count_like = agg_expr->function_name == "count";
        output_stats.is_count_star = output_stats.is_count_like && agg_expr->children.empty();
        output_stats.is_count_distinct = output_stats.is_count_like && agg_expr->is_distinct;
        output_stats.input_cardinality = input_cardinality;
        output_stats.group_cardinality = stats.cardinality;
        output_stats.effective_input_cardinality = input_cardinality;
        if (output_stats.is_count_distinct) {
            output_stats.effective_input_cardinality = EstimateDistinctCardinality(input_stats, agg_expr->children);
        } else if (output_stats.is_count_like && !agg_expr->children.empty()) {
            size_t effective = input_cardinality;
            for (const auto& child : agg_expr->children) {
                std::vector<ColumnBinding> bindings;
                CollectColumnRefs(child.get(), bindings);
                if (bindings.size() == 1) {
                    auto child_column = LookupColumnStats(input_stats, bindings[0]);
                    if (child_column.has_stats) {
                        double non_null = static_cast<double>(effective) * (1.0 - child_column.null_fraction);
                        effective = ClampCardinality(non_null, input_cardinality);
                    }
                }
            }
            output_stats.effective_input_cardinality = effective;
        }
        stats.aggregate_output_stats[MakeColumnBindingKey(binding)] = output_stats;
    }

    return stats;
}

RelationStats RelationStatisticsHelper::ExtractDistinctStats(LogicalDistinct& distinct) const {
    RelationStats input_stats = Extract(*distinct.children[0]);
    RelationStats stats = input_stats;
    stats.cardinality = EstimateDistinctCardinality(input_stats, distinct.expressions);
    return stats;
}

RelationStats RelationStatisticsHelper::ExtractFilterStats(LogicalFilter& filter) const {
    RelationStats input_stats = Extract(*filter.children[0]);
    RelationStats stats = input_stats;
    stats.cardinality = EstimateFilterCardinality(input_stats, filter.expressions);
    return stats;
}

RelationStats RelationStatisticsHelper::ExtractLimitStats(LogicalLimit& limit) const {
    RelationStats input_stats = Extract(*limit.children[0]);
    RelationStats stats = input_stats;
    stats.cardinality = EstimateLimitCardinality(input_stats, limit.limit_count.get(), limit.limit_offset.get());
    return stats;
}

RelationStats RelationStatisticsHelper::ExtractDelimGetStats(LogicalDelimGet& delim_get) const {
    RelationStats stats;
    stats.cardinality = std::max<size_t>(1, delim_get.estimated_cardinality);
    stats.stats_initialized = true;
    stats.table_name = "delim_get";

    for (size_t i = 0; i < delim_get.correlated_columns.size(); ++i) {
        ColumnBinding binding{delim_get.table_index, ProjectionIndex{i}};
        DistinctCount distinct{stats.cardinality, false};
        stats.column_distinct_count.push_back(distinct);
        std::string column_name = i < delim_get.output_names.size()
            ? delim_get.output_names[i]
            : "delim" + std::to_string(i + 1);
        stats.column_names.push_back(column_name);
        ColumnStats column;
        column.binding = binding;
        column.distinct = distinct;
        column.has_stats = true;
        column.table_name = "delim_get";
        column.column_name = column_name;
        stats.column_stats[MakeColumnBindingKey(binding)] = column;
    }

    return stats;
}

RelationStats RelationStatisticsHelper::ExtractUnaryStats(LogicalOperator& op) const {
    if (op.children.empty()) {
        RelationStats stats;
        stats.cardinality = op.estimated_cardinality;
        return stats;
    }

    RelationStats stats = Extract(*op.children[0]);
    stats.cardinality = op.estimated_cardinality;
    return stats;
}

RelationStats RelationStatisticsHelper::ExtractJoinStats(LogicalOperator& op) const {
    RelationStats stats;
    if (op.children.size() != 2) {
        stats.cardinality = op.estimated_cardinality;
        return stats;
    }

    auto left_stats = Extract(*op.children[0]);
    auto right_stats = Extract(*op.children[1]);
    stats = CombineReorderableStats(left_stats, right_stats, op.estimated_cardinality);

    if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
        op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
        op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        auto& join = static_cast<LogicalComparisonJoin&>(op);
        for (const auto& condition : join.conditions) {
            ColumnBinding left_binding;
            ColumnBinding right_binding;
            if (!TryExtractEqualityColumns(condition.get(), left_binding, right_binding)) {
                continue;
            }

            auto left_column = LookupColumnStats(stats, left_binding);
            auto right_column = LookupColumnStats(stats, right_binding);
            if (!left_column.has_stats || !right_column.has_stats ||
                left_column.distinct.distinct_count == 0 || right_column.distinct.distinct_count == 0) {
                continue;
            }

            auto unified_distinct = std::max<size_t>(
                1,
                std::min({left_column.distinct.distinct_count,
                          right_column.distinct.distinct_count,
                          std::max<size_t>(1, stats.cardinality)}));
            left_column.distinct.distinct_count = unified_distinct;
            right_column.distinct.distinct_count = unified_distinct;
            stats.column_stats[MakeColumnBindingKey(left_binding)] = left_column;
            stats.column_stats[MakeColumnBindingKey(right_binding)] = right_column;
        }
    }

    return stats;
}

size_t RelationStatisticsHelper::EstimateFilterCardinality(
    const RelationStats& input_stats,
    const std::vector<std::unique_ptr<Expression>>& filters) const {
    size_t cardinality = input_stats.cardinality;
    std::map<ColumnBindingKey, std::vector<ColumnFilterConstraint>> grouped_constraints;
    for (const auto& filter : filters) {
        ColumnBinding binding;
        BoundConstantExpression* constant = nullptr;
        std::string op_name;
        if (TryExtractColumnConstantComparison(filter.get(), binding, constant, op_name) &&
            constant != nullptr && !constant->is_null) {
            grouped_constraints[MakeColumnBindingKey(binding)].push_back({constant, op_name, filter.get()});
        }
    }

    std::set<Expression*> handled_filters;
    for (const auto& entry : grouped_constraints) {
        if (entry.second.size() < 2) {
            continue;
        }
        ColumnBinding binding{TableIndex{entry.first.table_index}, ProjectionIndex{entry.first.column_index}};
        auto column_stats = LookupColumnStats(input_stats, binding);
        if (!column_stats.has_stats) {
            continue;
        }
        double selectivity = EstimateCombinedColumnSelectivity(column_stats, entry.second);
        if (selectivity < 0.0) {
            continue;
        }
        cardinality = ClampCardinality(static_cast<double>(cardinality) * selectivity, input_stats.cardinality);
        for (const auto& constraint : entry.second) {
            handled_filters.insert(constraint.expression);
        }
    }

    for (const auto& filter : filters) {
        if (handled_filters.find(filter.get()) != handled_filters.end()) {
            continue;
        }
        cardinality = EstimateSingleFilter(cardinality, input_stats, filter.get());
    }
    return cardinality;
}

size_t RelationStatisticsHelper::EstimateDistinctCardinality(
    const RelationStats& input_stats,
    const std::vector<std::unique_ptr<Expression>>& distincts) const {
    if (input_stats.cardinality == 0) {
        return 0;
    }
    if (distincts.empty()) {
        return input_stats.cardinality;
    }

    double cardinality = 1.0;
    bool used_stats = false;
    std::set<std::string> seen_expressions;
    for (const auto& distinct : distincts) {
        auto key = DistinctExpressionKey(distinct.get());
        if (!seen_expressions.insert(key).second) {
            continue;
        }

        if (distinct && distinct->type == ExpressionType::BOUND_CONSTANT) {
            cardinality *= 1.0;
            used_stats = true;
            continue;
        }

        std::vector<ColumnBinding> bindings;
        CollectColumnRefs(distinct.get(), bindings);
        std::set<ColumnBindingKey> unique_bindings;
        for (const auto& binding : bindings) {
            unique_bindings.insert(MakeColumnBindingKey(binding));
        }

        if (!unique_bindings.empty()) {
            double expr_cardinality = 1.0;
            bool expr_used_stats = false;
            for (const auto& binding_key : unique_bindings) {
                ColumnBinding binding{TableIndex{binding_key.table_index}, ProjectionIndex{binding_key.column_index}};
                auto column = LookupColumnStats(input_stats, binding);
                if (column.has_stats && column.distinct.distinct_count > 0) {
                    expr_cardinality *= static_cast<double>(column.distinct.distinct_count);
                    expr_used_stats = true;
                } else {
                    expr_cardinality *= std::max<double>(1.0, static_cast<double>(input_stats.cardinality) * 0.5);
                }
            }
            cardinality *= expr_cardinality;
            used_stats = used_stats || expr_used_stats;
            continue;
        }

        cardinality *= std::max<double>(1.0, static_cast<double>(input_stats.cardinality) * 0.5);
    }

    if (!used_stats) {
        cardinality = static_cast<double>(input_stats.cardinality) * 0.5;
    }

    return ClampCardinality(cardinality, input_stats.cardinality);
}

size_t RelationStatisticsHelper::EstimateLimitCardinality(const RelationStats& input_stats,
                                                          Expression* limit_count,
                                                          Expression* limit_offset) const {
    size_t count = input_stats.cardinality;
    size_t offset = 0;

    if (TryParseLimitExpression(limit_count, count)) {
        count = std::min(count, input_stats.cardinality);
    }

    if (TryParseLimitExpression(limit_offset, offset)) {
        // Parsed successfully.
    }

    if (offset >= input_stats.cardinality) {
        return 0;
    }

    size_t remaining = input_stats.cardinality - offset;
    return std::min(count, remaining);
}

size_t RelationStatisticsHelper::EstimateSingleFilter(size_t input_cardinality,
                                                      const RelationStats& input_stats,
                                                      Expression* filter) const {
    if (input_cardinality == 0) {
        return 0;
    }

    ColumnBinding binding;
    BoundConstantExpression* constant = nullptr;
    std::string op_name;
    if (TryExtractColumnConstantComparison(filter, binding, constant, op_name)) {
        auto aggregate_output = input_stats.aggregate_output_stats.find(MakeColumnBindingKey(binding));
        if (aggregate_output != input_stats.aggregate_output_stats.end() &&
            aggregate_output->second.has_stats &&
            aggregate_output->second.is_count_like) {
            double constant_value = 0.0;
            if (ParseNumericConstant(constant, constant_value)) {
                size_t estimated_cardinality = input_cardinality;
                if (EstimateCountFilter(aggregate_output->second, op_name, constant_value, estimated_cardinality)) {
                    return estimated_cardinality;
                }
            }
        }

        auto column_stats = LookupColumnStats(input_stats, binding);
        if (column_stats.has_stats && column_stats.has_catalog_stats) {
            double selectivity = EstimateColumnComparisonSelectivity(column_stats, constant, op_name);
            if (selectivity >= 0.0) {
                return ClampCardinality(static_cast<double>(input_cardinality) * selectivity, input_cardinality);
            }
        }
    }

    if (IsDynamicComparison(filter) || IsArrayMembershipPredicate(filter)) {
        return input_cardinality;
    }

    return ClampCardinality(static_cast<double>(input_cardinality) * DEFAULT_SELECTIVITY, input_cardinality);
}

size_t RelationStatisticsHelper::EstimateJoinCardinality(
    const RelationStats& left_stats,
    const RelationStats& right_stats,
    const std::vector<Expression*>& join_conditions) const {
    if (left_stats.cardinality == 0 || right_stats.cardinality == 0) {
        return 0;
    }

    double cardinality = static_cast<double>(left_stats.cardinality) * static_cast<double>(right_stats.cardinality);
    bool used_stats = false;

    for (auto* condition : join_conditions) {
        ColumnBinding left_binding;
        ColumnBinding right_binding;
        if (!TryExtractEqualityColumns(condition, left_binding, right_binding)) {
            continue;
        }

        auto left_column = LookupColumnStats(left_stats, left_binding);
        auto right_column = LookupColumnStats(right_stats, right_binding);
        if (!left_column.has_stats || !right_column.has_stats) {
            left_column = LookupColumnStats(left_stats, right_binding);
            right_column = LookupColumnStats(right_stats, left_binding);
        }

        if (left_column.has_stats && right_column.has_stats &&
            left_column.distinct.distinct_count > 0 && right_column.distinct.distinct_count > 0) {
            auto divisor = std::max(left_column.distinct.distinct_count, right_column.distinct.distinct_count);
            cardinality /= static_cast<double>(divisor);
            used_stats = true;
        }
    }

    if (!used_stats && !join_conditions.empty()) {
        cardinality *= DEFAULT_SELECTIVITY;
    }

    return ClampCardinality(cardinality, 0);
}

size_t RelationStatisticsHelper::EstimateSemiOrAntiJoinCardinality(
    const RelationStats& left_stats,
    const RelationStats& right_stats,
    const std::vector<Expression*>& join_conditions,
    bool anti) const {
    if (left_stats.cardinality == 0) {
        return 0;
    }

    double matched_fraction = -1.0;
    bool used_stats = false;
    for (auto* condition : join_conditions) {
        ColumnBinding left_binding;
        ColumnBinding right_binding;
        if (!TryExtractEqualityColumns(condition, left_binding, right_binding)) {
            continue;
        }

        auto left_column = LookupColumnStats(left_stats, left_binding);
        auto right_column = LookupColumnStats(right_stats, right_binding);
        if (!left_column.has_stats || !right_column.has_stats) {
            left_column = LookupColumnStats(left_stats, right_binding);
            right_column = LookupColumnStats(right_stats, left_binding);
        }
        if (!left_column.has_stats || !right_column.has_stats ||
            left_column.distinct.distinct_count == 0 || right_column.distinct.distinct_count == 0) {
            continue;
        }

        double coverage = std::min(
            1.0,
            static_cast<double>(right_column.distinct.distinct_count) /
                static_cast<double>(std::max<size_t>(1, left_column.distinct.distinct_count)));
        matched_fraction = used_stats ? matched_fraction * coverage : coverage;
        used_stats = true;
    }

    if (!used_stats) {
        if (join_conditions.empty()) {
            matched_fraction = DEFAULT_SELECTIVITY;
        } else {
            auto joined = EstimateJoinCardinality(left_stats, right_stats, join_conditions);
            matched_fraction = std::min(
                1.0,
                static_cast<double>(joined) / static_cast<double>(std::max<size_t>(1, left_stats.cardinality)));
        }
    }

    matched_fraction = std::max(0.0, std::min(1.0, matched_fraction));
    size_t matched_rows = ClampCardinality(
        matched_fraction * static_cast<double>(left_stats.cardinality),
        left_stats.cardinality);
    if (!anti) {
        return matched_rows;
    }
    return left_stats.cardinality > matched_rows ? left_stats.cardinality - matched_rows : 0;
}

RelationStats RelationStatisticsHelper::CombineReorderableStats(
    const RelationStats& left_stats,
    const RelationStats& right_stats,
    size_t cardinality) const {
    RelationStats stats;
    stats.cardinality = cardinality;
    stats.stats_initialized = left_stats.stats_initialized || right_stats.stats_initialized;
    MergeColumnStats(stats, left_stats);
    MergeColumnStats(stats, right_stats);
    return stats;
}

ColumnStats RelationStatisticsHelper::LookupColumnStats(const RelationStats& stats, ColumnBinding binding) const {
    auto entry = stats.column_stats.find(MakeColumnBindingKey(binding));
    if (entry == stats.column_stats.end()) {
        return ColumnStats{};
    }
    return entry->second;
}

bool RelationStatisticsHelper::TryExtractEqualityColumns(Expression* expression,
                                                        ColumnBinding& left,
                                                        ColumnBinding& right) const {
    if (!expression || expression->type != ExpressionType::BOUND_FUNCTION) {
        return false;
    }
    auto* function = static_cast<BoundFunctionExpression*>(expression);
    if (function->function_name != "=" || function->children.size() != 2) {
        return false;
    }
    if (function->children[0]->type != ExpressionType::BOUND_COLUMN_REF ||
        function->children[1]->type != ExpressionType::BOUND_COLUMN_REF) {
        return false;
    }
    left = static_cast<BoundColumnRefExpression*>(function->children[0].get())->binding;
    right = static_cast<BoundColumnRefExpression*>(function->children[1].get())->binding;
    return true;
}

bool RelationStatisticsHelper::TryExtractColumnConstantComparison(Expression* expression,
                                                                  ColumnBinding& binding,
                                                                  BoundConstantExpression*& constant,
                                                                  std::string& op_name) const {
    if (!expression || expression->type != ExpressionType::BOUND_FUNCTION) {
        return false;
    }

    auto* function = static_cast<BoundFunctionExpression*>(expression);
    if (!IsComparisonOperator(function->function_name) || function->children.size() != 2) {
        return false;
    }

    auto* left = function->children[0].get();
    auto* right = function->children[1].get();
    if (left->type == ExpressionType::BOUND_COLUMN_REF && right->type == ExpressionType::BOUND_CONSTANT) {
        binding = static_cast<BoundColumnRefExpression*>(left)->binding;
        constant = static_cast<BoundConstantExpression*>(right);
        op_name = function->function_name;
        return true;
    }
    if (left->type == ExpressionType::BOUND_CONSTANT && right->type == ExpressionType::BOUND_COLUMN_REF) {
        binding = static_cast<BoundColumnRefExpression*>(right)->binding;
        constant = static_cast<BoundConstantExpression*>(left);
        op_name = function->function_name;
        return true;
    }
    return false;
}

bool RelationStatisticsHelper::EstimateCountFilter(const AggregateOutputStats& aggregate_stats,
                                                   const std::string& op_name,
                                                   double constant_value,
                                                   size_t& estimated_cardinality) const {
    if (aggregate_stats.effective_input_cardinality == 0 || aggregate_stats.group_cardinality == 0) {
        estimated_cardinality = 0;
        return true;
    }

    const double input = static_cast<double>(aggregate_stats.effective_input_cardinality);
    const double groups = static_cast<double>(aggregate_stats.group_cardinality);

    auto clamp_selectivity = [&](double selectivity) {
        selectivity = std::max(0.0, std::min(1.0, selectivity));
        estimated_cardinality = ClampCardinality(selectivity * groups, aggregate_stats.group_cardinality);
    };

    if (op_name == "=") {
        if (aggregate_stats.is_count_star && constant_value < 1.0) {
            estimated_cardinality = 0;
            return true;
        }
        clamp_selectivity(groups / input);
        return true;
    }

    if (op_name == ">" || op_name == ">=") {
        double minimum_count = (op_name == ">")
            ? std::floor(constant_value) + 1.0
            : std::ceil(constant_value);
        if (aggregate_stats.is_count_star && minimum_count <= 1.0) {
            estimated_cardinality = aggregate_stats.group_cardinality;
            return true;
        }
        double selectivity = 1.0 - ((minimum_count - 1.0) * groups / input);
        clamp_selectivity(selectivity);
        return true;
    }

    if (op_name == "<" || op_name == "<=") {
        double maximum_count = (op_name == "<")
            ? std::ceil(constant_value) - 1.0
            : std::floor(constant_value);
        if (maximum_count < 1.0 && aggregate_stats.is_count_star) {
            estimated_cardinality = 0;
            return true;
        }
        double selectivity = maximum_count * groups / input;
        clamp_selectivity(selectivity);
        return true;
    }

    return false;
}

void RelationStatisticsHelper::CollectColumnRefs(Expression* expression, std::vector<ColumnBinding>& bindings) const {
    if (!expression) {
        return;
    }
    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF:
            bindings.push_back(static_cast<BoundColumnRefExpression*>(expression)->binding);
            break;
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            for (auto& child : function->children) {
                CollectColumnRefs(child.get(), bindings);
            }
            break;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression);
            for (auto& child : aggregate->children) {
                CollectColumnRefs(child.get(), bindings);
            }
            break;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            for (auto& child : conjunction->children) {
                CollectColumnRefs(child.get(), bindings);
            }
            break;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subquery = static_cast<BoundSubqueryExpression*>(expression);
            for (auto& child : subquery->children) {
                CollectColumnRefs(child.get(), bindings);
            }
            break;
        }
        default:
            break;
    }
}

} // namespace yaap
