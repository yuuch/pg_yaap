#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

namespace yaap {

struct TableIndex;
class Expression;
class LogicalAggregate;
class LogicalProjection;
class LogicalDistinct;
class OrderByNode;

bool ExpressionReferencesOnlyAllowedTables(Expression* expression, const std::set<size_t>& allowed_tables);

std::string DecorrelatedExpressionFingerprint(Expression* expression);

std::string DerivedDecorrelatedOutputName(Expression* expression, size_t index);

bool ExtractDecorrelatedEquality(Expression* expression,
                                 const std::set<size_t>& allowed_tables,
                                 std::unique_ptr<Expression>& inner_expr,
                                 std::unique_ptr<Expression>& outer_expr);

std::vector<OrderByNode> CloneDecorrelatedOrders(const std::vector<OrderByNode>& orders);

std::unique_ptr<Expression> BuildDecorrelatedLimitRowNumberFilter(Expression* limit_count,
                                                                  Expression* limit_offset,
                                                                  TableIndex window_table_index);

bool RewriteLiftedFiltersThroughProjection(LogicalProjection& projection,
                                           const std::set<size_t>& allowed_tables,
                                           std::vector<std::unique_ptr<Expression>>& lifted_filters);

bool RewriteLiftedFiltersThroughAggregate(LogicalAggregate& aggregate,
                                          const std::set<size_t>& allowed_tables,
                                          std::vector<std::unique_ptr<Expression>>& lifted_filters);

bool AddCorrelatedKeysToDistinct(LogicalDistinct& distinct,
                                 const std::set<size_t>& allowed_tables,
                                 const std::vector<std::unique_ptr<Expression>>& lifted_filters);

} // namespace yaap
