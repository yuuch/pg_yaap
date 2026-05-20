#pragma once

extern "C" {
#include "postgres.h"
}

#include "../adapter/yaap_adapter.hpp"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace yaap {

struct DistinctCount {
    size_t distinct_count = 0;
    bool from_hll = false;
};

struct ColumnBindingKey {
    size_t table_index = 0;
    size_t column_index = 0;

    bool operator<(const ColumnBindingKey& other) const {
        if (table_index != other.table_index) {
            return table_index < other.table_index;
        }
        return column_index < other.column_index;
    }
};

struct ColumnStats {
    ColumnBinding binding;
    DistinctCount distinct;
    double null_fraction = 0.0;
    bool has_stats = false;
    bool has_catalog_stats = false;
    std::string table_name;
    std::string column_name;
    Oid type_oid = InvalidOid;
    Oid collation_oid = InvalidOid;
    Oid equality_op = InvalidOid;
    Oid sort_op = InvalidOid;
    int16 type_len = 0;
    bool type_by_val = false;
    double mcv_total_frequency = 0.0;
    std::vector<Datum> mcv_values;
    std::vector<double> mcv_frequencies;
    std::vector<Datum> histogram_bounds;
};

struct AggregateOutputStats {
    bool has_stats = false;
    bool is_count_like = false;
    bool is_count_star = false;
    bool is_count_distinct = false;
    size_t input_cardinality = 0;
    size_t effective_input_cardinality = 0;
    size_t group_cardinality = 0;
};

struct RelationStats {
    std::vector<DistinctCount> column_distinct_count;
    size_t cardinality = 0;
    double filter_strength = 1.0;
    bool stats_initialized = false;
    std::vector<std::string> column_names;
    std::string table_name;
    std::map<ColumnBindingKey, ColumnStats> column_stats;
    std::map<ColumnBindingKey, AggregateOutputStats> aggregate_output_stats;
};

class RelationStatisticsHelper {
public:
    static constexpr double DEFAULT_SELECTIVITY = 0.2;

    RelationStats Extract(LogicalOperator& op) const;
    size_t EstimateFilterCardinality(const RelationStats& input_stats,
                                     const std::vector<std::unique_ptr<Expression>>& filters) const;
    size_t EstimateDistinctCardinality(const RelationStats& input_stats,
                                       const std::vector<std::unique_ptr<Expression>>& distincts) const;
    size_t EstimateLimitCardinality(const RelationStats& input_stats,
                                    Expression* limit_count,
                                    Expression* limit_offset) const;
    size_t EstimateJoinCardinality(const RelationStats& left_stats,
                                   const RelationStats& right_stats,
                                   const std::vector<Expression*>& join_conditions) const;
    size_t EstimateSemiOrAntiJoinCardinality(const RelationStats& left_stats,
                                             const RelationStats& right_stats,
                                             const std::vector<Expression*>& join_conditions,
                                             bool anti) const;
    RelationStats CombineReorderableStats(const RelationStats& left_stats,
                                          const RelationStats& right_stats,
                                          size_t cardinality) const;
    ColumnStats LookupColumnStats(const RelationStats& stats, ColumnBinding binding) const;

private:
    RelationStats ExtractGetStats(LogicalGet& get) const;
    RelationStats ExtractProjectionStats(LogicalProjection& projection) const;
    RelationStats ExtractAggregateStats(LogicalAggregate& aggregate) const;
    RelationStats ExtractFilterStats(LogicalFilter& filter) const;
    RelationStats ExtractDistinctStats(LogicalDistinct& distinct) const;
    RelationStats ExtractLimitStats(LogicalLimit& limit) const;
    RelationStats ExtractDelimGetStats(LogicalDelimGet& delim_get) const;
    RelationStats ExtractUnaryStats(LogicalOperator& op) const;
    RelationStats ExtractJoinStats(LogicalOperator& op) const;

    size_t EstimateSingleFilter(size_t input_cardinality,
                                const RelationStats& input_stats,
                                Expression* filter) const;
    bool TryExtractColumnConstantComparison(Expression* expression,
                                            ColumnBinding& binding,
                                            BoundConstantExpression*& constant,
                                            std::string& op_name) const;
    bool EstimateCountFilter(const AggregateOutputStats& aggregate_stats,
                             const std::string& op_name,
                             double constant_value,
                             size_t& estimated_cardinality) const;
    bool TryExtractEqualityColumns(Expression* expression,
                                   ColumnBinding& left,
                                   ColumnBinding& right) const;
    void CollectColumnRefs(Expression* expression, std::vector<ColumnBinding>& bindings) const;
};

ColumnBindingKey MakeColumnBindingKey(ColumnBinding binding);

} // namespace yaap
