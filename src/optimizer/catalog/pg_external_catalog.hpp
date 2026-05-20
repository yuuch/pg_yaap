#pragma once

extern "C" {
// PostgreSQL includes for metadata
struct PlannerInfo;
}

namespace yaap {

// Bridge between DuckDB's need for Catalog data and PG's relcache/statistics
class PGExternalCatalog {
public:
    PGExternalCatalog() = default;
    ~PGExternalCatalog() = default;

    // Load statistics into our catalog interface
    void LoadStats(PlannerInfo* root);
    
    // Example access patterns for optimizer
    // double GetTableCardinality(int relid); 
};

} // namespace yaap
