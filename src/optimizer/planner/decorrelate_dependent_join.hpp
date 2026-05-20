#pragma once

#include <memory>
#include <set>
#include <vector>

namespace yaap {

struct TableIndex;
class Expression;
class LogicalOperator;

class DecorrelateDependentJoin {
public:
    std::unique_ptr<LogicalOperator> Optimize(std::unique_ptr<LogicalOperator> plan,
                                              bool* changed = nullptr);

private:
    std::unique_ptr<LogicalOperator> Rewrite(std::unique_ptr<LogicalOperator> plan,
                                             bool* changed);
    bool DecorrelateScalarAggregateJoin(std::unique_ptr<LogicalOperator>& plan);
    bool ContainsBlockingOperator(LogicalOperator* op) const;
    bool PushDownCorrelatedNode(std::unique_ptr<LogicalOperator>& plan,
                                const std::set<size_t>& allowed_tables,
                                std::vector<std::unique_ptr<Expression>>& lifted_filters);
    bool PushDownFilter(std::unique_ptr<LogicalOperator>& plan,
                        const std::set<size_t>& allowed_tables,
                        std::vector<std::unique_ptr<Expression>>& lifted_filters);
    bool PushDownProjection(std::unique_ptr<LogicalOperator>& plan,
                            const std::set<size_t>& allowed_tables,
                            std::vector<std::unique_ptr<Expression>>& lifted_filters);
    bool PushDownAggregate(std::unique_ptr<LogicalOperator>& plan,
                           const std::set<size_t>& allowed_tables,
                           std::vector<std::unique_ptr<Expression>>& lifted_filters);
    bool PushDownDistinct(std::unique_ptr<LogicalOperator>& plan,
                          const std::set<size_t>& allowed_tables,
                          std::vector<std::unique_ptr<Expression>>& lifted_filters);
    bool PushDownSetOperation(std::unique_ptr<LogicalOperator>& plan,
                              const std::set<size_t>& allowed_tables,
                              std::vector<std::unique_ptr<Expression>>& lifted_filters);
    bool PushDownOrder(std::unique_ptr<LogicalOperator>& plan,
                       const std::set<size_t>& allowed_tables,
                       std::vector<std::unique_ptr<Expression>>& lifted_filters);
    bool PushDownWindow(std::unique_ptr<LogicalOperator>& plan,
                        const std::set<size_t>& allowed_tables,
                        std::vector<std::unique_ptr<Expression>>& lifted_filters);
    bool PushDownLimit(std::unique_ptr<LogicalOperator>& plan,
                       const std::set<size_t>& allowed_tables,
                       std::vector<std::unique_ptr<Expression>>& lifted_filters);
    bool PushDownJoin(std::unique_ptr<LogicalOperator>& plan,
                      const std::set<size_t>& allowed_tables,
                      std::vector<std::unique_ptr<Expression>>& lifted_filters);
    bool PushDownCrossProduct(std::unique_ptr<LogicalOperator>& plan,
                              const std::set<size_t>& allowed_tables,
                              std::vector<std::unique_ptr<Expression>>& lifted_filters);
    bool PushDownGet(std::unique_ptr<LogicalOperator>& plan,
                     const std::set<size_t>& allowed_tables,
                     std::vector<std::unique_ptr<Expression>>& lifted_filters);
    void ExtractCorrelatedExpressions(std::vector<std::unique_ptr<Expression>>& expressions,
                                      const std::set<size_t>& allowed_tables,
                                      std::vector<std::unique_ptr<Expression>>& lifted_filters);
    void SplitConjunctions(std::vector<std::unique_ptr<Expression>>& expressions);
    bool ExpressionReferencesAllowedTables(Expression* expression,
                                           const std::set<size_t>& allowed_tables) const;
    bool SubtreeHasCorrelatedReferences(LogicalOperator* op,
                                        const std::set<size_t>& allowed_tables) const;
    void InitializeTableIndexAllocator(LogicalOperator* plan);
    TableIndex AllocateTableIndex();

    size_t next_table_index_ = 0;
};

} // namespace yaap
