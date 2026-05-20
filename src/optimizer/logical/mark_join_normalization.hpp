#pragma once

#include <memory>
#include <set>
#include <vector>

namespace yaap {

struct TableIndex;
class Expression;
class LogicalOperator;

void DisableMarkJoinConversionForConsumedMarkers(LogicalOperator* plan, std::set<size_t> table_bindings = {});

bool IsExistsStyleMarkJoinRHS(LogicalOperator* rhs);

bool ExtractMarkerFilterPolarity(Expression* expression, TableIndex mark_index, bool& positive);

void SimplifyExistsStyleMarkJoinFilter(std::unique_ptr<LogicalOperator>& join_plan,
                                       std::vector<std::unique_ptr<Expression>>& filters);

void SimplifyExistsStyleMarkJoin(std::unique_ptr<LogicalOperator>& plan);

} // namespace yaap
