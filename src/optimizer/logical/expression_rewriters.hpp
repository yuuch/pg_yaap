#pragma once

#include <map>
#include <memory>
#include <utility>

namespace yaap {

class Expression;

std::unique_ptr<Expression> CloneExpressionWithExpressionReplacements(
    Expression* expression,
    const std::map<std::pair<size_t, size_t>, Expression*>& replacements);

} // namespace yaap
