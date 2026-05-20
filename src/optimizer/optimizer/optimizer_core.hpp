#pragma once

#include <memory>
#include <map>
#include <set>
#include <vector>

#include "optimizer_stats.hpp"

namespace yaap {

// Forward declaration of the logical operator boundary
struct TableIndex;
class Expression;
class LogicalOperator;
class JoinOrderPlanEnumerator;
class JoinOrderCostModel;
class JoinOrderQueryGraphManager;
class JoinOrderDPJoinNode;

struct JoinOrderJoinRelation {
    std::unique_ptr<LogicalOperator> plan;
    std::set<size_t> output_tables;
    size_t estimated_cardinality = 0;
    RelationStats stats;
};

struct JoinOrderJoinCondition {
    std::unique_ptr<Expression> expression;
    uint64_t relation_mask = 0;
    uint64_t left_table_mask = 0;
    uint64_t right_table_mask = 0;
    uint64_t left_relation_mask = 0;
    uint64_t right_relation_mask = 0;
    int join_type = 0;
    bool invert_result = false;
    bool from_residual_predicate = false;
};

enum class OptimizerPass {
    FILTER_PUSHDOWN,
    JOIN_PREDICATE_EXTRACTION,
    PREDICATE_PROPAGATION,
    SCAN_FILTER_FOLDING,
    REMOVE_UNUSED_COLUMNS,
    CARDINALITY_ESTIMATOR,
    JOIN_ORDER
};

class OptimizerRule {
public:
    virtual ~OptimizerRule() = default;
    virtual OptimizerPass Pass() const = 0;
    virtual std::unique_ptr<LogicalOperator> Optimize(std::unique_ptr<LogicalOperator> plan) = 0;
};

class FilterPushdown : public OptimizerRule {
public:
    OptimizerPass Pass() const override;
    std::unique_ptr<LogicalOperator> Optimize(std::unique_ptr<LogicalOperator> plan) override;

private:
    std::unique_ptr<LogicalOperator> Rewrite(std::unique_ptr<LogicalOperator> plan);
    std::unique_ptr<LogicalOperator> PushIntoCrossProduct(std::unique_ptr<LogicalOperator> filter_plan);
    std::unique_ptr<LogicalOperator> PushIntoProjection(std::unique_ptr<LogicalOperator> filter_plan);
    std::unique_ptr<LogicalOperator> PushIntoAggregate(std::unique_ptr<LogicalOperator> filter_plan);
    void CheckMarkToSemi(LogicalOperator* plan, std::set<size_t> table_bindings);

    void SplitConjunctions(std::vector<std::unique_ptr<Expression>>& expressions);
};

class JoinPredicateExtraction : public OptimizerRule {
public:
    OptimizerPass Pass() const override;
    std::unique_ptr<LogicalOperator> Optimize(std::unique_ptr<LogicalOperator> plan) override;

private:
    std::unique_ptr<LogicalOperator> Rewrite(std::unique_ptr<LogicalOperator> plan);
    std::unique_ptr<LogicalOperator> ExtractFromFilter(std::unique_ptr<LogicalOperator> filter_plan);

    bool IsEquiJoinPredicate(Expression* expression, const std::set<size_t>& left_tables,
                             const std::set<size_t>& right_tables);
};

class PredicatePropagation : public OptimizerRule {
public:
    OptimizerPass Pass() const override;
    std::unique_ptr<LogicalOperator> Optimize(std::unique_ptr<LogicalOperator> plan) override;

private:
    struct EqualityClass {
        std::set<ColumnBindingKey> columns;
        bool has_constant = false;
        std::unique_ptr<BoundConstantExpression> constant;
        std::map<ColumnBindingKey, std::pair<std::string, std::string>> labels;
    };

    std::unique_ptr<LogicalOperator> Rewrite(std::unique_ptr<LogicalOperator> plan);
    void CollectEqualities(LogicalOperator* op, std::vector<Expression*>& equalities);
    void CollectConjuncts(Expression* expression, std::vector<Expression*>& conjuncts) const;
    bool IsEqualityPredicate(Expression* expression) const;
    bool IsColumnConstantEquality(Expression* expression, ColumnBinding& binding, BoundConstantExpression*& constant) const;
    bool IsColumnColumnEquality(Expression* expression, ColumnBinding& left, ColumnBinding& right) const;
    std::string SerializeExpression(Expression* expression) const;
    std::unique_ptr<Expression> CloneExpression(Expression* expression) const;
    std::unique_ptr<LogicalOperator> InjectFilters(
        std::unique_ptr<LogicalOperator> plan,
        std::map<size_t, std::vector<std::unique_ptr<Expression>>>& filters);
    std::map<ColumnBindingKey, EqualityClass> BuildEqualityClasses(const std::vector<Expression*>& equalities) const;
    std::map<size_t, std::vector<std::unique_ptr<Expression>>> BuildDisjunctiveConstantFilters(LogicalOperator* op) const;
    std::map<size_t, std::vector<std::unique_ptr<Expression>>> BuildPropagatedFilters(
        const std::map<ColumnBindingKey, EqualityClass>& classes,
        const std::set<ColumnBindingKey>& direct_constant_bindings) const;
    std::vector<std::unique_ptr<Expression>> BuildPropagatedEqualities(
        const std::map<ColumnBindingKey, EqualityClass>& classes,
        const std::set<std::string>& direct_column_equalities) const;
};

class ScanFilterFolding : public OptimizerRule {
public:
    OptimizerPass Pass() const override;
    std::unique_ptr<LogicalOperator> Optimize(std::unique_ptr<LogicalOperator> plan) override;

private:
    std::unique_ptr<LogicalOperator> Rewrite(std::unique_ptr<LogicalOperator> plan);
    std::unique_ptr<LogicalOperator> FoldIntoGet(std::unique_ptr<LogicalOperator> filter_plan);
};

class RemoveUnusedColumns : public OptimizerRule {
public:
    OptimizerPass Pass() const override;
    std::unique_ptr<LogicalOperator> Optimize(std::unique_ptr<LogicalOperator> plan) override;
};

class CardinalityEstimator : public OptimizerRule {
public:
    OptimizerPass Pass() const override;
    std::unique_ptr<LogicalOperator> Optimize(std::unique_ptr<LogicalOperator> plan) override;

private:
    RelationStats Rewrite(LogicalOperator& plan);
    RelationStatisticsHelper statistics_helper_;
};

class JoinOrderOptimizer : public OptimizerRule {
public:
    OptimizerPass Pass() const override;
    std::unique_ptr<LogicalOperator> Optimize(std::unique_ptr<LogicalOperator> plan) override;

private:
	friend class JoinOrderPlanEnumerator;
	friend class JoinOrderCostModel;
	friend class JoinOrderQueryGraphManager;

    using JoinRelation = JoinOrderJoinRelation;
    using JoinCondition = JoinOrderJoinCondition;

    std::unique_ptr<LogicalOperator> Rewrite(std::unique_ptr<LogicalOperator> plan);
    bool IsReorderableJoinTree(LogicalOperator* op);
    bool IsSafeJoinOrderTree(LogicalOperator* op);
    bool IsJoinComponent(LogicalOperator* op);
    void ExtractJoinGraph(std::unique_ptr<LogicalOperator> plan,
                          std::vector<JoinRelation>& relations,
                          std::vector<JoinCondition>& conditions);
    std::vector<uint64_t> FindJoinComponents(const std::vector<JoinRelation>& relations,
                                             const std::vector<JoinCondition>& conditions);
    std::unique_ptr<LogicalOperator> OptimizeJoinSubset(uint64_t subset_mask,
                                                        std::vector<JoinRelation>& relations,
                                                        const std::vector<JoinCondition>& conditions);
    uint64_t ReferencedRelationMask(Expression* expression, const std::vector<JoinRelation>& relations);
    RelationStats GetDPStats(uint64_t mask,
                             const std::map<uint64_t, std::unique_ptr<JoinOrderDPJoinNode>>& plans,
                             const std::vector<JoinRelation>& relations) const;

    std::map<uint64_t, RelationStats> dp_stats_;
};

// Runs the logical optimizer stage after planner normalization.
class LogicalOptimizer {
public:
    LogicalOptimizer() = default;
    ~LogicalOptimizer() = default;

    // Run the pipeline on the input LogicalOperator tree, return optimized tree
    std::unique_ptr<LogicalOperator> Optimize(std::unique_ptr<LogicalOperator> plan);

private:
    void RunOptimizer(OptimizerPass pass, OptimizerRule& rule, std::unique_ptr<LogicalOperator>& plan);
};

using OptimizerCore = LogicalOptimizer;

} // namespace yaap
