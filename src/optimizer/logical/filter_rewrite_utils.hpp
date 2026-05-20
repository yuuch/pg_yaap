#pragma once

#include <map>
#include <memory>
#include <utility>
#include <vector>

namespace yaap {

class Expression;
class LogicalOperator;

void SplitConjunctionList(std::vector<std::unique_ptr<Expression>>& expressions);

std::unique_ptr<LogicalOperator> MakeFilterNode(
    std::unique_ptr<LogicalOperator> child,
    std::vector<std::unique_ptr<Expression>> expressions);

bool CanRewriteProjectionFilter(
    Expression* expression,
    const std::map<std::pair<size_t, size_t>, Expression*>& replacements);

} // namespace yaap
