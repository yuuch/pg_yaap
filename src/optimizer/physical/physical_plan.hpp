#pragma once

#include <memory>
#include <vector>

#include "../adapter/yaap_adapter.hpp"

namespace yaap {

enum class PhysicalOperatorType {
    TABLE_SCAN,
    PROJECTION,
    FILTER,
    DISTINCT,
    SET_OPERATION,
    LIMIT,
    WINDOW,
    HASH_JOIN,
    DELIM_SCAN,
    CROSS_PRODUCT,
    HASH_GROUP_BY,
    ORDER_BY
};

class PhysicalOperator {
public:
    struct OutputColumn {
        ColumnBinding binding;
        std::string table_name;
        std::string column_name;
        uint32_t type_oid = 0;
        int32_t typmod = -1;
    };

    PhysicalOperatorType type;
    size_t estimated_cardinality;
    std::vector<OutputColumn> outputs;
    std::vector<std::unique_ptr<PhysicalOperator>> children;
    // Physical planner rewrites some consumer expressions (e.g. aggregate refs)
    // into physical output bindings; these clones live with the owning node.
    std::vector<std::unique_ptr<Expression>> expression_storage;

    virtual ~PhysicalOperator() = default;

protected:
    PhysicalOperator(PhysicalOperatorType type, size_t estimated_cardinality)
        : type(type), estimated_cardinality(estimated_cardinality) {}
};

class PhysicalTableScan : public PhysicalOperator {
public:
    TableIndex table_index;
    int pg_rtindex;
    unsigned int relid;
    std::string table_name;
    std::vector<ColumnTypeInfo> column_types;
    std::vector<ProjectionIndex> projected_columns;
    std::vector<Expression*> filters;

    PhysicalTableScan(TableIndex table_index, int pg_rtindex, unsigned int relid, std::string table_name,
                      std::vector<ColumnTypeInfo> column_types,
                      std::vector<ProjectionIndex> projected_columns,
                      std::vector<Expression*> filters, size_t estimated_cardinality)
        : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, estimated_cardinality),
          table_index(table_index), pg_rtindex(pg_rtindex), relid(relid), table_name(std::move(table_name)),
          column_types(std::move(column_types)), projected_columns(std::move(projected_columns)),
          filters(std::move(filters)) {}
};

class PhysicalProjection : public PhysicalOperator {
public:
    TableIndex table_index;
    std::vector<Expression*> select_list;
    std::vector<std::string> output_names;

    PhysicalProjection(TableIndex table_index,
                       std::vector<Expression*> select_list,
                       std::vector<std::string> output_names,
                       size_t estimated_cardinality)
        : PhysicalOperator(PhysicalOperatorType::PROJECTION, estimated_cardinality),
          table_index(table_index),
          select_list(std::move(select_list)), output_names(std::move(output_names)) {}
};

class PhysicalFilter : public PhysicalOperator {
public:
    std::vector<Expression*> expressions;

    PhysicalFilter(std::vector<Expression*> expressions, size_t estimated_cardinality)
        : PhysicalOperator(PhysicalOperatorType::FILTER, estimated_cardinality),
          expressions(std::move(expressions)) {}
};

class PhysicalDistinct : public PhysicalOperator {
public:
    std::vector<Expression*> expressions;

    PhysicalDistinct(std::vector<Expression*> expressions, size_t estimated_cardinality)
        : PhysicalOperator(PhysicalOperatorType::DISTINCT, estimated_cardinality),
          expressions(std::move(expressions)) {}
};

class PhysicalSetOperation : public PhysicalOperator {
public:
    TableIndex table_index;
    SetOperationType setop_type;
    bool all = false;
    std::vector<std::string> output_names;

    PhysicalSetOperation(TableIndex table_index,
                         SetOperationType setop_type,
                         bool all,
                         std::vector<std::string> output_names,
                         size_t estimated_cardinality)
        : PhysicalOperator(PhysicalOperatorType::SET_OPERATION, estimated_cardinality),
          table_index(table_index),
          setop_type(setop_type),
          all(all),
          output_names(std::move(output_names)) {}
};

class PhysicalLimit : public PhysicalOperator {
public:
    Expression* limit_count;
    Expression* limit_offset;

    PhysicalLimit(Expression* limit_count, Expression* limit_offset, size_t estimated_cardinality)
        : PhysicalOperator(PhysicalOperatorType::LIMIT, estimated_cardinality),
          limit_count(limit_count), limit_offset(limit_offset) {}
};

class PhysicalWindow : public PhysicalOperator {
public:
    TableIndex table_index;
    std::vector<std::string> function_names;
    std::vector<std::string> output_names;
    std::vector<Expression*> partitions;
    std::vector<Expression*> orders;

    PhysicalWindow(TableIndex table_index,
                   std::vector<std::string> function_names,
                   std::vector<std::string> output_names,
                   std::vector<Expression*> partitions,
                   std::vector<Expression*> orders,
                   size_t estimated_cardinality)
        : PhysicalOperator(PhysicalOperatorType::WINDOW, estimated_cardinality),
          table_index(table_index),
          function_names(std::move(function_names)),
          output_names(std::move(output_names)),
          partitions(std::move(partitions)),
          orders(std::move(orders)) {}
};

class PhysicalCrossProduct : public PhysicalOperator {
public:
    explicit PhysicalCrossProduct(size_t estimated_cardinality)
        : PhysicalOperator(PhysicalOperatorType::CROSS_PRODUCT, estimated_cardinality) {}
};

class PhysicalHashJoin : public PhysicalOperator {
public:
    int join_type;
    bool dependent = false;
    bool delim_join = false;
    TableIndex mark_index{static_cast<size_t>(-1)};
    bool has_mark_index = false;
    bool invert_result = false;
    std::vector<ColumnBinding> correlated_columns;
    std::vector<Expression*> conditions;

    PhysicalHashJoin(int join_type, std::vector<Expression*> conditions, size_t estimated_cardinality)
        : PhysicalOperator(PhysicalOperatorType::HASH_JOIN, estimated_cardinality),
          join_type(join_type), conditions(std::move(conditions)) {}
};

class PhysicalDelimScan : public PhysicalOperator {
public:
    TableIndex table_index;
    std::vector<ColumnBinding> correlated_columns;
    const PhysicalOperator* delim_outer_child = nullptr;
    std::vector<ColumnBinding> delim_outer_bindings;
    std::vector<std::string> output_names;

    PhysicalDelimScan(TableIndex table_index, size_t estimated_cardinality)
        : PhysicalOperator(PhysicalOperatorType::DELIM_SCAN, estimated_cardinality), table_index(table_index) {}
};

class PhysicalHashAggregate : public PhysicalOperator {
public:
    TableIndex group_index;
    TableIndex aggregate_index;
    std::vector<Expression*> groups;
    std::vector<Expression*> expressions;
    std::vector<std::string> group_names;
    std::vector<std::string> aggregate_names;

    PhysicalHashAggregate(TableIndex group_index, TableIndex aggregate_index,
                          std::vector<Expression*> groups, std::vector<Expression*> expressions,
                          std::vector<std::string> group_names, std::vector<std::string> aggregate_names,
                          size_t estimated_cardinality)
        : PhysicalOperator(PhysicalOperatorType::HASH_GROUP_BY, estimated_cardinality),
          group_index(group_index), aggregate_index(aggregate_index),
          groups(std::move(groups)), expressions(std::move(expressions)),
          group_names(std::move(group_names)), aggregate_names(std::move(aggregate_names)) {}
};

class PhysicalOrder {
public:
    std::vector<Expression*> orders;

    explicit PhysicalOrder(std::vector<Expression*> orders)
        : orders(std::move(orders)) {}
};

class PhysicalOrderBy : public PhysicalOperator {
public:
    std::vector<Expression*> orders;

    PhysicalOrderBy(std::vector<Expression*> orders, size_t estimated_cardinality)
        : PhysicalOperator(PhysicalOperatorType::ORDER_BY, estimated_cardinality),
          orders(std::move(orders)) {}
};

} // namespace yaap
