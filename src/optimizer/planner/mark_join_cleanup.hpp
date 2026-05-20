#pragma once

#include <memory>

namespace yaap {

class LogicalOperator;

std::unique_ptr<LogicalOperator> NormalizeExistsStyleMarkJoins(std::unique_ptr<LogicalOperator> plan,
                                                               bool* changed = nullptr);

} // namespace yaap
