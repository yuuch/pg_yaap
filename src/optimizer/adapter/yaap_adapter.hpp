#pragma once

#include <memory>
#include <cstdint>
#include <string>
#include <vector>

extern "C" {
// PostgreSQL structures
struct Query;
struct List;
struct Node;
struct RangeTblEntry;
struct CommonTableExpr;
struct Var;
struct SubLink;
struct ScalarArrayOpExpr;
struct NullTest;
struct ArrayExpr;
struct RowCompareExpr;
struct BooleanTest;
}

namespace yaap {

// Logical Operator Types supported
enum class LogicalOperatorType {
    LOGICAL_GET,
    LOGICAL_FILTER,
    LOGICAL_PROJECTION,
    LOGICAL_AGGREGATE_AND_GROUP_BY,
    LOGICAL_DISTINCT,
    LOGICAL_SET_OPERATION,
    LOGICAL_CROSS_PRODUCT,
    LOGICAL_COMPARISON_JOIN,
    LOGICAL_DEPENDENT_JOIN,
    LOGICAL_DELIM_JOIN,
    LOGICAL_DELIM_GET,
    LOGICAL_ORDER,
    LOGICAL_WINDOW,
    LOGICAL_LIMIT,
    LOGICAL_DUMMY
};

// Expression Types Supported
enum class ExpressionType {
    BOUND_COLUMN_REF,
    BOUND_CONSTANT,
    BOUND_AGGREGATE,
    BOUND_COMPARISON,
    BOUND_FUNCTION,
    BOUND_CONJUNCTION,
    BOUND_SUBQUERY,
    OPAQUE // Used internally if we wanted to support black-box exprs
};

class Expression {
public:
    ExpressionType type;
    virtual ~Expression() = default;
protected:
    Expression(ExpressionType type) : type(type) {}
};

struct TableIndex {
    size_t index;
};

struct ProjectionIndex {
    size_t index;
};

struct ColumnBinding {
    TableIndex table_index;
    ProjectionIndex column_index;
};

struct ColumnTypeInfo {
    uint32_t type_oid = 0;
    int32_t typmod = -1;
};

enum class JoinType : int {
    INNER = 0,
    LEFT = 1,
    FULL = 2,
    RIGHT = 3,
    SEMI = 4,
    ANTI = 5,
    MARK = 6,
    SINGLE = 7,
};

inline constexpr int JOIN_INNER = static_cast<int>(JoinType::INNER);
inline constexpr int JOIN_LEFT = static_cast<int>(JoinType::LEFT);
inline constexpr int JOIN_FULL = static_cast<int>(JoinType::FULL);
inline constexpr int JOIN_RIGHT = static_cast<int>(JoinType::RIGHT);
inline constexpr int JOIN_SEMI = static_cast<int>(JoinType::SEMI);
inline constexpr int JOIN_ANTI = static_cast<int>(JoinType::ANTI);
inline constexpr int JOIN_MARK = static_cast<int>(JoinType::MARK);
inline constexpr int JOIN_SINGLE = static_cast<int>(JoinType::SINGLE);

inline bool IsOuterJoinType(int join_type) {
    return join_type == JOIN_LEFT || join_type == JOIN_RIGHT || join_type == JOIN_FULL;
}

inline bool IsSemiOrAntiJoinType(int join_type) {
    return join_type == JOIN_SEMI || join_type == JOIN_ANTI;
}

enum class SetOperationType : int {
    UNION = 0,
};

struct PGBindingKey {
    int varlevelsup;
    int varno;
    int varattno;
};

class BoundColumnRefExpression : public Expression {
public:
    ColumnBinding binding;
    std::string table_name;
    std::string column_name;
    BoundColumnRefExpression(ColumnBinding binding, std::string table_name, std::string column_name)
        : Expression(ExpressionType::BOUND_COLUMN_REF), binding(binding),
          table_name(std::move(table_name)), column_name(std::move(column_name)) {}
};

class BoundConstantExpression : public Expression {
public:
    std::string value;
    bool is_null;
    BoundConstantExpression(std::string value, bool is_null)
        : Expression(ExpressionType::BOUND_CONSTANT), value(std::move(value)), is_null(is_null) {}
};

class BoundFunctionExpression : public Expression {
public:
    std::string function_name;
    int op_oid; // Using PG's OID to map the function/operator
    std::vector<std::unique_ptr<Expression>> children;
    BoundFunctionExpression(std::string function_name, int op_oid)
        : Expression(ExpressionType::BOUND_FUNCTION), function_name(std::move(function_name)), op_oid(op_oid) {}
};

class BoundAggregateExpression : public Expression {
public:
    std::string function_name;
    int agg_oid;
    bool is_distinct;
    std::vector<std::unique_ptr<Expression>> children;
    BoundAggregateExpression(std::string function_name, int agg_oid, bool is_distinct)
        : Expression(ExpressionType::BOUND_AGGREGATE), function_name(std::move(function_name)),
          agg_oid(agg_oid), is_distinct(is_distinct) {}
};

class BoundConjunctionExpression : public Expression {
public:
    int bool_expr_type; // AND, OR, NOT (maps to PG BoolExprType)
    std::vector<std::unique_ptr<Expression>> children;
    BoundConjunctionExpression(int type) : Expression(ExpressionType::BOUND_CONJUNCTION), bool_expr_type(type) {}
};

// Base class for DuckDB's LogicalOperator
class LogicalOperator {
public:
    LogicalOperatorType type;
    size_t estimated_cardinality = 0;
    std::vector<std::unique_ptr<LogicalOperator>> children;
    virtual ~LogicalOperator() = default;
protected:
    LogicalOperator(LogicalOperatorType type) : type(type) {}
};
// LogicalFilter represents a WHERE / HAVING condition
class LogicalFilter : public LogicalOperator {
public:
    std::vector<std::unique_ptr<Expression>> expressions;
    LogicalFilter() : LogicalOperator(LogicalOperatorType::LOGICAL_FILTER) {}
};

// LogicalProjection represents the SELECT target list
class LogicalProjection : public LogicalOperator {
public:
    TableIndex table_index;
    std::vector<std::unique_ptr<Expression>> expressions;
    std::vector<std::string> output_names;
    explicit LogicalProjection(TableIndex table_index)
        : LogicalOperator(LogicalOperatorType::LOGICAL_PROJECTION), table_index(table_index) {}
};

class LogicalAggregate : public LogicalOperator {
public:
    TableIndex group_index;
    TableIndex aggregate_index;
    std::vector<std::unique_ptr<Expression>> groups;
    std::vector<std::unique_ptr<Expression>> expressions;
    std::vector<std::string> group_names;
    std::vector<std::string> aggregate_names;
    LogicalAggregate(TableIndex group_index, TableIndex aggregate_index)
        : LogicalOperator(LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY),
          group_index(group_index), aggregate_index(aggregate_index) {}
};

class LogicalDistinct : public LogicalOperator {
public:
    std::vector<std::unique_ptr<Expression>> expressions;
    LogicalDistinct() : LogicalOperator(LogicalOperatorType::LOGICAL_DISTINCT) {}
};

class LogicalSetOperation : public LogicalOperator {
public:
    TableIndex table_index;
    SetOperationType setop_type;
    bool all = false;
    std::vector<std::string> output_names;

    LogicalSetOperation(TableIndex table_index, SetOperationType setop_type, bool all)
        : LogicalOperator(LogicalOperatorType::LOGICAL_SET_OPERATION),
          table_index(table_index), setop_type(setop_type), all(all) {}
};

// LogicalComparisonJoin represents an explicit JOIN ON
class LogicalComparisonJoin : public LogicalOperator {
public:
    int join_type; // e.g. mapping PG JoinType
    bool dependent = false;
    TableIndex mark_index{static_cast<size_t>(-1)};
    bool has_mark_index = false;
    bool convert_mark_to_semi = true;
    bool invert_result = false;
    std::vector<std::unique_ptr<Expression>> conditions;
    LogicalComparisonJoin(int type) : LogicalOperator(LogicalOperatorType::LOGICAL_COMPARISON_JOIN), join_type(type) {}
};

class LogicalDependentJoin : public LogicalComparisonJoin {
public:
    std::vector<ColumnBinding> correlated_columns;
    bool perform_delim = true;
    bool any_join = false;
    bool propagate_null_values = true;
    bool is_lateral_join = false;

    explicit LogicalDependentJoin(int type)
        : LogicalComparisonJoin(type) {
        this->type = LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
    }
};

class OrderByNode {
public:
    std::unique_ptr<Expression> expression;
};

// LogicalOrder represents ORDER BY clause
class LogicalOrder : public LogicalOperator {
public:
    std::vector<OrderByNode> orders;
    LogicalOrder() : LogicalOperator(LogicalOperatorType::LOGICAL_ORDER) {}
};

class LogicalWindow : public LogicalOperator {
public:
    TableIndex table_index;
    std::vector<std::string> function_names;
    std::vector<std::string> output_names;
    std::vector<std::unique_ptr<Expression>> partitions;
    std::vector<OrderByNode> orders;

    explicit LogicalWindow(TableIndex table_index)
        : LogicalOperator(LogicalOperatorType::LOGICAL_WINDOW), table_index(table_index) {}
};

class LogicalLimit : public LogicalOperator {
public:
    std::unique_ptr<Expression> limit_count;
    std::unique_ptr<Expression> limit_offset;

    LogicalLimit() : LogicalOperator(LogicalOperatorType::LOGICAL_LIMIT) {}
};

// LogicalCrossProduct represents an implicit JOIN (e.g. FROM a, b)
class LogicalCrossProduct : public LogicalOperator {
public:
    LogicalCrossProduct() : LogicalOperator(LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {}
};

class LogicalDelimGet : public LogicalOperator {
public:
    TableIndex table_index;
    std::vector<ColumnBinding> correlated_columns;
    std::vector<std::string> output_names;

    explicit LogicalDelimGet(TableIndex table_index)
        : LogicalOperator(LogicalOperatorType::LOGICAL_DELIM_GET), table_index(table_index) {}
};
// LogicalGet represents a table scan
class LogicalGet : public LogicalOperator {
public:
    TableIndex table_index;
    int pg_rtindex;
    unsigned int relid;
    std::string table_name;
    std::vector<std::string> output_names;
    std::vector<ColumnTypeInfo> output_types;
    std::vector<ProjectionIndex> projected_columns;
    std::vector<std::unique_ptr<Expression>> filters;
    LogicalGet(TableIndex table_index, int pg_rtindex, unsigned int relid, std::string table_name)
        : LogicalOperator(LogicalOperatorType::LOGICAL_GET),
          table_index(table_index), pg_rtindex(pg_rtindex), relid(relid), table_name(std::move(table_name)) {}
};

class BoundSubqueryExpression : public Expression {
public:
    int sublink_type;
    std::string sublink_name;
    std::unique_ptr<LogicalOperator> subquery_plan;
    std::vector<std::unique_ptr<Expression>> children;
    BoundSubqueryExpression(int sublink_type, std::string sublink_name)
        : Expression(ExpressionType::BOUND_SUBQUERY),
          sublink_type(sublink_type),
          sublink_name(std::move(sublink_name)) {}
};

// Adapts PostgreSQL's Query tree to yaap LogicalOperator tree
class YaapAdapter {
public:
    explicit YaapAdapter(YaapAdapter* parent = nullptr);
    ~YaapAdapter() = default;

    std::unique_ptr<LogicalOperator> TranslatePGQuery(::Query* pg_query);
    TableIndex GenerateTableIndex();

private:
    std::unique_ptr<LogicalOperator> TranslateFromExprList(::List* fromlist);
    std::unique_ptr<LogicalOperator> TranslateFromNode(::Node* pg_node);
    std::unique_ptr<LogicalOperator> TranslateSetOperation(::Query* pg_query, ::Node* setop_node);
    std::unique_ptr<Expression> TranslateExpression(::Node* pg_expr);
    std::unique_ptr<LogicalOperator> TranslateAggregate(::Query* pg_query, std::unique_ptr<LogicalOperator> input);
    std::unique_ptr<LogicalOperator> TranslateHaving(::Query* pg_query, std::unique_ptr<LogicalOperator> input);
    std::unique_ptr<LogicalOperator> TranslateWindow(::Query* pg_query, std::unique_ptr<LogicalOperator> input);
    std::unique_ptr<LogicalOperator> TranslateProjection(::Query* pg_query, std::unique_ptr<LogicalOperator> input);
    std::unique_ptr<LogicalOperator> TranslateDistinct(::Query* pg_query, std::unique_ptr<LogicalOperator> input);
    std::unique_ptr<LogicalOperator> TranslateOrder(::Query* pg_query, std::unique_ptr<LogicalOperator> input);
    std::unique_ptr<LogicalOperator> TranslateLimit(::Query* pg_query, std::unique_ptr<LogicalOperator> input);

    ::RangeTblEntry* GetRte(int rtindex);
    ::RangeTblEntry* GetRte(::Var* var);
    std::string GetRteName(int rtindex);
    std::string GetRteName(::Var* var);
    std::string GetColumnName(::Var* var);
    ColumnBinding BindBaseColumn(::Var* var);
    ColumnBinding GetBaseColumnBinding(::Var* var);
    size_t EstimateBaseCardinality(::RangeTblEntry* rte);
    void PropagateUnaryCardinality(LogicalOperator& op);
    void PropagateCrossProductCardinality(LogicalCrossProduct& op);
    void RegisterOutputBindings(int rtindex, ::RangeTblEntry* rte, LogicalOperator* plan);

    ::List* rtable_ = nullptr;
    YaapAdapter* parent_ = nullptr;
    size_t next_table_index_ = 0;
    std::vector<std::pair<PGBindingKey, ColumnBinding>> pg_bindings_;
    std::vector<::CommonTableExpr*> cte_list_;
};

} // namespace yaap
