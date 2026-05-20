#pragma once

#include <memory>
#include <set>
#include <vector>

namespace yaap {

class Expression;
class LogicalOperator;

void CollectReferencedTables(Expression* expression, std::set<size_t>& table_indexes);
void CollectOutputTables(LogicalOperator* op, std::set<size_t>& table_indexes);
bool IsSubset(const std::set<size_t>& candidate, const std::set<size_t>& container);
std::unique_ptr<Expression> CloneExpressionTree(Expression* expression);
void AppendUniqueFilter(std::vector<std::unique_ptr<Expression>>& filters, std::unique_ptr<Expression> expression);

} // namespace yaap
