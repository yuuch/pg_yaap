#include "planner_normalizer.hpp"

#include "../adapter/yaap_adapter.hpp"
#include "decorrelate_dependent_join.hpp"
#include "delim_join_cleanup.hpp"
#include "mark_join_cleanup.hpp"

namespace yaap {

namespace {

constexpr size_t MAX_PLANNER_NORMALIZATION_ROUNDS = 8;
constexpr size_t MAX_SUBQUERY_CLEANUP_ITERATIONS = 8;

} // namespace

void PlannerNormalizer::RunPass(PlannerPass pass, std::unique_ptr<LogicalOperator>& plan, bool& changed) {
    if (!plan) {
        return;
    }

    switch (pass) {
        case PlannerPass::DECORRELATE_DEPENDENT_JOIN: {
            DecorrelateDependentJoin decorrelate_dependent_join;
            plan = decorrelate_dependent_join.Optimize(std::move(plan), &changed);
            return;
        }
        case PlannerPass::EXISTS_STYLE_MARK_CLEANUP:
            plan = NormalizeExistsStyleMarkJoins(std::move(plan), &changed);
            return;
        case PlannerPass::REDUNDANT_DELIM_CLEANUP:
            plan = CleanupRedundantDelimJoins(std::move(plan), &changed);
            return;
    }
}

void PlannerNormalizer::RunSubqueryCleanupLoop(std::unique_ptr<LogicalOperator>& plan, bool& changed) {
    for (size_t iteration = 0; iteration < MAX_SUBQUERY_CLEANUP_ITERATIONS; ++iteration) {
        bool iteration_changed = false;
        RunPass(PlannerPass::EXISTS_STYLE_MARK_CLEANUP, plan, iteration_changed);
        RunPass(PlannerPass::REDUNDANT_DELIM_CLEANUP, plan, iteration_changed);
        if (!iteration_changed) {
            return;
        }
        changed = true;
    }
}

std::unique_ptr<LogicalOperator> PlannerNormalizer::Normalize(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) {
        return nullptr;
    }

    for (size_t iteration = 0; iteration < MAX_PLANNER_NORMALIZATION_ROUNDS; ++iteration) {
        bool iteration_changed = false;
        RunPass(PlannerPass::DECORRELATE_DEPENDENT_JOIN, plan, iteration_changed);
        RunSubqueryCleanupLoop(plan, iteration_changed);
        if (!iteration_changed) {
            break;
        }
    }
    return plan;
}

} // namespace yaap
