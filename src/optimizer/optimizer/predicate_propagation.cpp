#include "optimizer_core.hpp"

#include "../logical/filter_rewrite_utils.hpp"
#include "../logical/logical_utils.hpp"

#include <sstream>

namespace yaap {

OptimizerPass PredicatePropagation::Pass() const {
    return OptimizerPass::PREDICATE_PROPAGATION;
}

void PredicatePropagation::CollectConjuncts(Expression* expression, std::vector<Expression*>& conjuncts) const {
    if (!expression) {
        return;
    }
    if (expression->type == ExpressionType::BOUND_CONJUNCTION) {
        auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
        if (conjunction->bool_expr_type == 0) {
            for (auto& child : conjunction->children) {
                CollectConjuncts(child.get(), conjuncts);
            }
            return;
        }
    }
    if (expression->type == ExpressionType::BOUND_SUBQUERY) {
        conjuncts.push_back(expression);
        return;
    }
    conjuncts.push_back(expression);
}

bool PredicatePropagation::IsEqualityPredicate(Expression* expression) const {
    if (!expression || expression->type != ExpressionType::BOUND_FUNCTION) {
        return false;
    }
    auto* function = static_cast<BoundFunctionExpression*>(expression);
    return function->function_name == "=" && function->children.size() == 2;
}

bool PredicatePropagation::IsColumnConstantEquality(Expression* expression,
                                                    ColumnBinding& binding,
                                                    BoundConstantExpression*& constant) const {
    if (!IsEqualityPredicate(expression)) {
        return false;
    }
    auto* function = static_cast<BoundFunctionExpression*>(expression);
    auto* left = function->children[0].get();
    auto* right = function->children[1].get();
    if (left->type == ExpressionType::BOUND_COLUMN_REF && right->type == ExpressionType::BOUND_CONSTANT) {
        binding = static_cast<BoundColumnRefExpression*>(left)->binding;
        constant = static_cast<BoundConstantExpression*>(right);
        return true;
    }
    if (left->type == ExpressionType::BOUND_CONSTANT && right->type == ExpressionType::BOUND_COLUMN_REF) {
        binding = static_cast<BoundColumnRefExpression*>(right)->binding;
        constant = static_cast<BoundConstantExpression*>(left);
        return true;
    }
    return false;
}

bool PredicatePropagation::IsColumnColumnEquality(Expression* expression,
                                                  ColumnBinding& left,
                                                  ColumnBinding& right) const {
    if (!IsEqualityPredicate(expression)) {
        return false;
    }
    auto* function = static_cast<BoundFunctionExpression*>(expression);
    if (function->children[0]->type != ExpressionType::BOUND_COLUMN_REF ||
        function->children[1]->type != ExpressionType::BOUND_COLUMN_REF) {
        return false;
    }
    left = static_cast<BoundColumnRefExpression*>(function->children[0].get())->binding;
    right = static_cast<BoundColumnRefExpression*>(function->children[1].get())->binding;
    return true;
}

void PredicatePropagation::CollectEqualities(LogicalOperator* op, std::vector<Expression*>& equalities) {
    if (!op) {
        return;
    }

    switch (op->type) {
        case LogicalOperatorType::LOGICAL_FILTER: {
            auto* filter = static_cast<LogicalFilter*>(op);
            for (auto& expression : filter->expressions) {
                std::vector<Expression*> conjuncts;
                CollectConjuncts(expression.get(), conjuncts);
                for (auto* conjunct : conjuncts) {
                    if (IsEqualityPredicate(conjunct)) {
                        equalities.push_back(conjunct);
                    }
                }
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
            auto* join = static_cast<LogicalComparisonJoin*>(op);
            for (auto& expression : join->conditions) {
                std::vector<Expression*> conjuncts;
                CollectConjuncts(expression.get(), conjuncts);
                for (auto* conjunct : conjuncts) {
                    if (IsEqualityPredicate(conjunct)) {
                        equalities.push_back(conjunct);
                    }
                }
            }
            break;
        }
        default:
            break;
    }

    for (auto& child : op->children) {
        CollectEqualities(child.get(), equalities);
    }
}

std::string PredicatePropagation::SerializeExpression(Expression* expression) const {
    if (!expression) {
        return "<null>";
    }

    std::stringstream ss;
    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* column = static_cast<BoundColumnRefExpression*>(expression);
            ss << "col(" << column->binding.table_index.index << "."
               << column->binding.column_index.index << ")";
            break;
        }
        case ExpressionType::BOUND_CONSTANT: {
            auto* constant = static_cast<BoundConstantExpression*>(expression);
            ss << "const(" << (constant->is_null ? "NULL" : constant->value) << ")";
            break;
        }
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            ss << "fn(" << function->function_name << ":";
            for (size_t i = 0; i < function->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << SerializeExpression(function->children[i].get());
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression);
            ss << "agg(" << aggregate->function_name << ":";
            for (size_t i = 0; i < aggregate->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << SerializeExpression(aggregate->children[i].get());
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            ss << "conj(" << conjunction->bool_expr_type << ":";
            for (size_t i = 0; i < conjunction->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << SerializeExpression(conjunction->children[i].get());
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subquery = static_cast<BoundSubqueryExpression*>(expression);
            ss << "subquery(" << subquery->sublink_name << ":";
            for (size_t i = 0; i < subquery->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << SerializeExpression(subquery->children[i].get());
            }
            ss << ")";
            break;
        }
        default:
            ss << "opaque";
            break;
    }
    return ss.str();
}

std::unique_ptr<Expression> PredicatePropagation::CloneExpression(Expression* expression) const {
    if (!expression) {
        return nullptr;
    }

    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* column = static_cast<BoundColumnRefExpression*>(expression);
            return std::make_unique<BoundColumnRefExpression>(column->binding, column->table_name, column->column_name);
        }
        case ExpressionType::BOUND_CONSTANT: {
            auto* constant = static_cast<BoundConstantExpression*>(expression);
            return std::make_unique<BoundConstantExpression>(constant->value, constant->is_null);
        }
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            auto clone = std::make_unique<BoundFunctionExpression>(function->function_name, function->op_oid);
            for (auto& child : function->children) {
                clone->children.push_back(CloneExpression(child.get()));
            }
            return clone;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression);
            auto clone = std::make_unique<BoundAggregateExpression>(aggregate->function_name, aggregate->agg_oid, aggregate->is_distinct);
            for (auto& child : aggregate->children) {
                clone->children.push_back(CloneExpression(child.get()));
            }
            return clone;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            auto clone = std::make_unique<BoundConjunctionExpression>(conjunction->bool_expr_type);
            for (auto& child : conjunction->children) {
                clone->children.push_back(CloneExpression(child.get()));
            }
            return clone;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subquery = static_cast<BoundSubqueryExpression*>(expression);
            auto clone = std::make_unique<BoundSubqueryExpression>(subquery->sublink_type, subquery->sublink_name);
            for (auto& child : subquery->children) {
                clone->children.push_back(CloneExpression(child.get()));
            }
            return clone;
        }
        default:
            return nullptr;
    }
}

std::map<ColumnBindingKey, PredicatePropagation::EqualityClass>
PredicatePropagation::BuildEqualityClasses(const std::vector<Expression*>& equalities) const {
    std::map<ColumnBindingKey, ColumnBindingKey> parent;
    std::map<ColumnBindingKey, std::pair<std::string, std::string>> labels;

    auto ensure = [&](ColumnBindingKey key) {
        if (parent.find(key) == parent.end()) {
            parent[key] = key;
        }
    };

    auto find_root = [&](auto&& self, ColumnBindingKey key) -> ColumnBindingKey {
        auto it = parent.find(key);
        if (it == parent.end()) {
            return key;
        }
        if (it->second.table_index == key.table_index && it->second.column_index == key.column_index) {
            return key;
        }
        it->second = self(self, it->second);
        return it->second;
    };

    auto unite = [&](ColumnBindingKey left, ColumnBindingKey right) {
        ensure(left);
        ensure(right);
        auto left_root = find_root(find_root, left);
        auto right_root = find_root(find_root, right);
        if (left_root.table_index == right_root.table_index &&
            left_root.column_index == right_root.column_index) {
            return;
        }
        parent[right_root] = left_root;
    };

    for (auto* expression : equalities) {
        ColumnBinding left;
        ColumnBinding right;
        BoundConstantExpression* constant = nullptr;
        if (IsColumnColumnEquality(expression, left, right)) {
            auto* left_ref = static_cast<BoundColumnRefExpression*>(
                static_cast<BoundFunctionExpression*>(expression)->children[0].get());
            auto* right_ref = static_cast<BoundColumnRefExpression*>(
                static_cast<BoundFunctionExpression*>(expression)->children[1].get());
            auto left_key = MakeColumnBindingKey(left);
            auto right_key = MakeColumnBindingKey(right);
            ensure(left_key);
            ensure(right_key);
            labels[left_key] = {left_ref->table_name, left_ref->column_name};
            labels[right_key] = {right_ref->table_name, right_ref->column_name};
            unite(left_key, right_key);
            continue;
        }

        if (IsColumnConstantEquality(expression, left, constant)) {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            auto* column_ref = static_cast<BoundColumnRefExpression*>(
                function->children[0]->type == ExpressionType::BOUND_COLUMN_REF
                    ? function->children[0].get()
                    : function->children[1].get());
            auto key = MakeColumnBindingKey(left);
            ensure(key);
            labels[key] = {column_ref->table_name, column_ref->column_name};
        }
    }

    std::map<ColumnBindingKey, EqualityClass> classes;
    for (const auto& entry : parent) {
        auto root = find_root(find_root, entry.first);
        auto& cls = classes[root];
        cls.columns.insert(entry.first);
        auto label_it = labels.find(entry.first);
        if (label_it != labels.end()) {
            cls.labels[entry.first] = label_it->second;
        }
    }

    for (auto* expression : equalities) {
        ColumnBinding binding;
        BoundConstantExpression* constant = nullptr;
        if (!IsColumnConstantEquality(expression, binding, constant) || constant == nullptr || constant->is_null) {
            continue;
        }

        auto root = find_root(find_root, MakeColumnBindingKey(binding));
        auto& cls = classes[root];
        if (!cls.has_constant) {
            cls.constant = std::make_unique<BoundConstantExpression>(constant->value, constant->is_null);
            cls.has_constant = true;
        }
    }

    return classes;
}

std::map<size_t, std::vector<std::unique_ptr<Expression>>>
PredicatePropagation::BuildDisjunctiveConstantFilters(LogicalOperator* op) const {
    std::map<size_t, std::vector<std::unique_ptr<Expression>>> filters;
    if (!op) {
        return filters;
    }

    auto process_expression = [&](Expression* expression) {
        if (!expression || expression->type != ExpressionType::BOUND_CONJUNCTION) {
            return;
        }
        auto* disjunction = static_cast<BoundConjunctionExpression*>(expression);
        if (disjunction->bool_expr_type == 0 || disjunction->children.size() < 2) {
            return;
        }

        using DisjunctEntry = std::tuple<BoundConstantExpression*, std::string, std::string>;
        std::vector<std::map<ColumnBindingKey, DisjunctEntry>> disjunct_maps;
        disjunct_maps.reserve(disjunction->children.size());

        for (auto& child : disjunction->children) {
            std::vector<Expression*> conjuncts;
            CollectConjuncts(child.get(), conjuncts);

            std::map<ColumnBindingKey, DisjunctEntry> local;
            for (auto* conjunct : conjuncts) {
                ColumnBinding binding;
                BoundConstantExpression* constant = nullptr;
                if (!IsColumnConstantEquality(conjunct, binding, constant) || constant == nullptr || constant->is_null) {
                    continue;
                }

                auto* function = static_cast<BoundFunctionExpression*>(conjunct);
                auto* column_ref = static_cast<BoundColumnRefExpression*>(
                    function->children[0]->type == ExpressionType::BOUND_COLUMN_REF
                        ? function->children[0].get()
                        : function->children[1].get());
                local.emplace(
                    MakeColumnBindingKey(binding),
                    std::make_tuple(constant, column_ref->table_name, column_ref->column_name));
            }
            if (local.empty()) {
                return;
            }
            disjunct_maps.push_back(std::move(local));
        }

        if (disjunct_maps.size() < 2) {
            return;
        }

        for (const auto& entry : disjunct_maps.front()) {
            const auto& binding_key = entry.first;
            bool present_in_all = true;
            for (size_t i = 1; i < disjunct_maps.size(); ++i) {
                if (disjunct_maps[i].find(binding_key) == disjunct_maps[i].end()) {
                    present_in_all = false;
                    break;
                }
            }
            if (!present_in_all) {
                continue;
            }

            auto derived = std::make_unique<BoundConjunctionExpression>(1);
            for (const auto& disjunct_map : disjunct_maps) {
                const auto& disjunct_entry = disjunct_map.find(binding_key)->second;
                auto column = std::make_unique<BoundColumnRefExpression>(
                    ColumnBinding{TableIndex{binding_key.table_index}, ProjectionIndex{binding_key.column_index}},
                    std::get<1>(disjunct_entry),
                    std::get<2>(disjunct_entry));
                auto constant = std::make_unique<BoundConstantExpression>(
                    std::get<0>(disjunct_entry)->value,
                    std::get<0>(disjunct_entry)->is_null);
                auto equal = std::make_unique<BoundFunctionExpression>("=", 96);
                equal->children.push_back(std::move(column));
                equal->children.push_back(std::move(constant));
                derived->children.push_back(std::move(equal));
            }
            filters[binding_key.table_index].push_back(std::move(derived));
        }
    };

    switch (op->type) {
        case LogicalOperatorType::LOGICAL_FILTER: {
            auto* filter = static_cast<LogicalFilter*>(op);
            for (auto& expression : filter->expressions) {
                process_expression(expression.get());
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
            auto* join = static_cast<LogicalComparisonJoin*>(op);
            for (auto& expression : join->conditions) {
                process_expression(expression.get());
            }
            break;
        }
        default:
            break;
    }

    return filters;
}

std::map<size_t, std::vector<std::unique_ptr<Expression>>>
PredicatePropagation::BuildPropagatedFilters(const std::map<ColumnBindingKey, EqualityClass>& classes,
                                             const std::set<ColumnBindingKey>& direct_constant_bindings) const {
    std::map<size_t, std::vector<std::unique_ptr<Expression>>> filters;

    for (const auto& entry : classes) {
        const auto& cls = entry.second;
        if (!cls.has_constant || !cls.constant || cls.constant->is_null) {
            continue;
        }

        for (const auto& binding_key : cls.columns) {
            if (direct_constant_bindings.find(binding_key) != direct_constant_bindings.end()) {
                continue;
            }

            auto label_it = cls.labels.find(binding_key);
            std::string table_name = label_it != cls.labels.end()
                ? label_it->second.first
                : "t" + std::to_string(binding_key.table_index);
            std::string column_name = label_it != cls.labels.end()
                ? label_it->second.second
                : "col" + std::to_string(binding_key.column_index + 1);

            auto column = std::make_unique<BoundColumnRefExpression>(
                ColumnBinding{TableIndex{binding_key.table_index}, ProjectionIndex{binding_key.column_index}},
                table_name,
                column_name);
            auto constant = std::make_unique<BoundConstantExpression>(cls.constant->value, cls.constant->is_null);
            auto equal = std::make_unique<BoundFunctionExpression>("=", 96);
            equal->children.push_back(std::move(column));
            equal->children.push_back(std::move(constant));
            filters[binding_key.table_index].push_back(std::move(equal));
        }
    }

    return filters;
}

std::vector<std::unique_ptr<Expression>> PredicatePropagation::BuildPropagatedEqualities(
    const std::map<ColumnBindingKey, EqualityClass>& classes,
    const std::set<std::string>& direct_column_equalities) const {
    std::vector<std::unique_ptr<Expression>> equalities;

    auto make_pair_key = [](const ColumnBindingKey& left, const ColumnBindingKey& right) {
        const auto& first = (left.table_index < right.table_index ||
                             (left.table_index == right.table_index && left.column_index <= right.column_index))
            ? left
            : right;
        const auto& second = (&first == &left) ? right : left;
        return std::to_string(first.table_index) + "." + std::to_string(first.column_index) + "=" +
               std::to_string(second.table_index) + "." + std::to_string(second.column_index);
    };

    for (const auto& entry : classes) {
        const auto& cls = entry.second;
        if (cls.columns.size() < 3) {
            continue;
        }

        std::vector<ColumnBindingKey> columns(cls.columns.begin(), cls.columns.end());
        for (size_t i = 0; i < columns.size(); ++i) {
            for (size_t j = i + 1; j < columns.size(); ++j) {
                if (columns[i].table_index == columns[j].table_index) {
                    continue;
                }

                auto pair_key = make_pair_key(columns[i], columns[j]);
                if (direct_column_equalities.find(pair_key) != direct_column_equalities.end()) {
                    continue;
                }

                auto left_label = cls.labels.find(columns[i]);
                auto right_label = cls.labels.find(columns[j]);
                auto left = std::make_unique<BoundColumnRefExpression>(
                    ColumnBinding{TableIndex{columns[i].table_index}, ProjectionIndex{columns[i].column_index}},
                    left_label != cls.labels.end() ? left_label->second.first : "t" + std::to_string(columns[i].table_index),
                    left_label != cls.labels.end() ? left_label->second.second : "col" + std::to_string(columns[i].column_index + 1));
                auto right = std::make_unique<BoundColumnRefExpression>(
                    ColumnBinding{TableIndex{columns[j].table_index}, ProjectionIndex{columns[j].column_index}},
                    right_label != cls.labels.end() ? right_label->second.first : "t" + std::to_string(columns[j].table_index),
                    right_label != cls.labels.end() ? right_label->second.second : "col" + std::to_string(columns[j].column_index + 1));
                auto equal = std::make_unique<BoundFunctionExpression>("=", 96);
                equal->children.push_back(std::move(left));
                equal->children.push_back(std::move(right));
                equalities.push_back(std::move(equal));
            }
        }
    }

    return equalities;
}

std::unique_ptr<LogicalOperator> PredicatePropagation::InjectFilters(
    std::unique_ptr<LogicalOperator> plan,
    std::map<size_t, std::vector<std::unique_ptr<Expression>>>& filters) {
    if (!plan) {
        return nullptr;
    }

    for (auto& child : plan->children) {
        child = InjectFilters(std::move(child), filters);
    }

    if (plan->type == LogicalOperatorType::LOGICAL_GET) {
        auto* get = static_cast<LogicalGet*>(plan.get());
        auto it = filters.find(get->table_index.index);
        if (it != filters.end() && !it->second.empty()) {
            for (auto& expression : it->second) {
                AppendUniqueFilter(get->filters, std::move(expression));
            }
            filters.erase(it);
        }
    }

    return plan;
}

std::unique_ptr<LogicalOperator> PredicatePropagation::Rewrite(std::unique_ptr<LogicalOperator> plan) {
    for (auto& child : plan->children) {
        child = Rewrite(std::move(child));
    }

    if (plan->type != LogicalOperatorType::LOGICAL_FILTER &&
        plan->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT &&
        plan->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        return plan;
    }

    std::vector<Expression*> equalities;
    CollectEqualities(plan.get(), equalities);

    std::set<ColumnBindingKey> direct_constant_bindings;
    std::set<std::string> direct_column_equalities;
    auto make_pair_key = [](const ColumnBindingKey& left, const ColumnBindingKey& right) {
        const auto& first = (left.table_index < right.table_index ||
                             (left.table_index == right.table_index && left.column_index <= right.column_index))
            ? left
            : right;
        const auto& second = (&first == &left) ? right : left;
        return std::to_string(first.table_index) + "." + std::to_string(first.column_index) + "=" +
               std::to_string(second.table_index) + "." + std::to_string(second.column_index);
    };
    for (auto* expression : equalities) {
        ColumnBinding binding;
        BoundConstantExpression* constant = nullptr;
        if (IsColumnConstantEquality(expression, binding, constant) && constant != nullptr && !constant->is_null) {
            direct_constant_bindings.insert(MakeColumnBindingKey(binding));
            continue;
        }

        ColumnBinding left;
        ColumnBinding right;
        if (IsColumnColumnEquality(expression, left, right)) {
            direct_column_equalities.insert(
                make_pair_key(MakeColumnBindingKey(left), MakeColumnBindingKey(right)));
        }
    }

    auto classes = BuildEqualityClasses(equalities);
    auto propagated_filters = BuildPropagatedFilters(classes, direct_constant_bindings);
    auto disjunctive_filters = BuildDisjunctiveConstantFilters(plan.get());
    for (auto& entry : disjunctive_filters) {
        auto& target = propagated_filters[entry.first];
        for (auto& expression : entry.second) {
            target.push_back(std::move(expression));
        }
    }
    plan = InjectFilters(std::move(plan), propagated_filters);

    auto propagated_equalities = BuildPropagatedEqualities(classes, direct_column_equalities);
    if (!propagated_equalities.empty()) {
        plan = MakeFilterNode(std::move(plan), std::move(propagated_equalities));
    }
    return plan;
}

std::unique_ptr<LogicalOperator> PredicatePropagation::Optimize(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) {
        return nullptr;
    }
    return Rewrite(std::move(plan));
}

} // namespace yaap
