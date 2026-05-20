#include "pg_external_catalog.hpp"

namespace yaap {

void PGExternalCatalog::LoadStats(PlannerInfo* root) {
    // TODO: Connect PG pg_statistic to inner stat structs
    // Extract histograms, MCV, NDV, null_count
}

} // namespace yaap
