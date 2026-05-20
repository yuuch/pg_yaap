#ifndef VOLVEC_HASH_TABLE_HPP
#define VOLVEC_HASH_TABLE_HPP

/*
 * Linear probe hash table and bloom filter for parallel hash join.
 *
 * Design principles:
 * - 64-byte aligned allocation for cache-line friendly access
 * - Open addressing with linear probing (better prefetcher behavior than chaining)
 * - Bloom filter for pre-filtering non-matching probe keys
 * - Compact storage: keys/values/states in separate arrays
 */

#include <cstdint>
#include <cstring>
#include <algorithm>

extern "C" {
#include "postgres.h"
#include "utils/memutils.h"
}

#include "yaap_engine.hpp"

namespace pg_yaap
{

/* --- MurmurHash3 finalizer for 64-bit keys --- */
static inline uint32_t
volvec_hash_key(uint64_t key)
{
	key ^= key >> 33;
	key *= UINT64CONST(0xff51afd7ed558ccd);
	key ^= key >> 33;
	key *= UINT64CONST(0xc4ceb9fe1a85ec53);
	key ^= key >> 33;
	return (uint32_t) key;
}

/* --- Bloom filter --- */
static inline uint32_t
bloom_filter_bit_count(uint32_t n)
{
	/* ~10 bits per element gives ~1% false positive rate with 7 hash funcs */
	return std::max<uint32_t>(n * 10, 64);
}

static inline uint32_t
bloom_filter_hash_funcs(uint32_t bit_count, uint32_t n)
{
	/* k = (m/n) * ln(2), clamped to [1, 4] */
	double k = ((double) bit_count / n) * 0.693;
	uint32_t result = std::max<uint32_t>(1u, std::min<uint32_t>(4u, (uint32_t) k));
	return result;
}

static inline void
volvec_bloom_init(VecBloomFilter *bloom, uint32_t n, MemoryContext context)
{
	uint32_t bit_count = bloom_filter_bit_count(n);
	uint32_t hash_funcs = bloom_filter_hash_funcs(bit_count, n);
	uint32_t byte_count = (bit_count + 7) / 8;

	MemoryContext old = MemoryContextSwitchTo(context);
	bloom->bits = (uint64_t *) palloc0(byte_count);
	MemoryContextSwitchTo(old);

	bloom->bit_count = bit_count;
	bloom->hash_funcs = hash_funcs;
	bloom->seeds[0] = UINT64CONST(0x9e3779b97f4a7c15);
	bloom->seeds[1] = UINT64CONST(0x5a5e3a6be5e7c9d3);
	bloom->seeds[2] = UINT64CONST(0x1b1c2d3e4f5a6b7c);
	bloom->seeds[3] = UINT64CONST(0xd7e8f9a0b1c2d3e4);
}

static inline void
volvec_bloom_insert(VecBloomFilter *bloom, uint64_t key)
{
	for (uint32_t h = 0; h < bloom->hash_funcs; h++) {
		uint64_t hash = volvec_hash_key(key ^ bloom->seeds[h]);
		uint32_t bit = hash % bloom->bit_count;
		bloom->bits[bit / 64] |= ((uint64_t) 1 << (bit % 64));
	}
}

static inline bool
volvec_bloom_might_contain(const VecBloomFilter *bloom, uint64_t key)
{
	for (uint32_t h = 0; h < bloom->hash_funcs; h++) {
		uint64_t hash = volvec_hash_key(key ^ bloom->seeds[h]);
		uint32_t bit = hash % bloom->bit_count;
		if (!(bloom->bits[bit / 64] & ((uint64_t) 1 << (bit % 64))))
			return false;
	}
	return true;
}

static inline uint32_t
volvec_bloom_bytes(const VecBloomFilter *bloom)
{
	return (bloom->bit_count + 7) / 8;
}

/* --- Linear probe hash table --- */
static inline uint32_t
volvec_ht_capacity(uint32_t n)
{
	/* smallest power of 2 >= n / load_factor */
	uint32_t cap = 1;
	uint32_t needed = (uint32_t) (n / VOLVEC_HT_LOAD_FACTOR) + 1;
	while (cap < needed)
		cap <<= 1;
	return cap;
}

static inline void
volvec_ht_init(VecLinearProbeHT *ht, uint32_t n, MemoryContext context)
{
	uint32_t capacity = volvec_ht_capacity(n);

	MemoryContext old = MemoryContextSwitchTo(context);
	ht->keys = (uint64_t *) palloc0(capacity * sizeof(uint64_t));
	ht->values = (uint32_t *) palloc0(capacity * sizeof(uint32_t));
	ht->states = (uint8_t *) palloc0(capacity);
	ht->payloads = (uint64_t *) palloc0(n * sizeof(uint64_t));
	MemoryContextSwitchTo(old);

	ht->capacity = capacity;
	ht->count = 0;
}

static inline void
volvec_ht_insert(VecLinearProbeHT *ht, uint64_t key, uint64_t payload)
{
	uint32_t hash = volvec_hash_key(key);
	uint32_t pos = hash & (ht->capacity - 1);
	uint32_t slot = ht->count;

	/* Store payload first */
	ht->payloads[slot] = payload;

	/* Linear probe for the key slot */
	while (ht->states[pos] != 0) {
		pos = (pos + 1) & (ht->capacity - 1);
	}
	ht->keys[pos] = key;
	ht->values[pos] = slot;
	ht->states[pos] = 1;
	ht->count++;
}

/*
 * Probe the hash table for a key.
 * Returns true if found, sets *payload to the associated payload value.
 * For keys with multiple matches, caller must call volvec_ht_probe_next()
 * to find additional matches.
 */
static inline bool
volvec_ht_probe(const VecLinearProbeHT *ht, uint64_t key, uint32_t hash, uint64_t *payload)
{
	uint32_t pos = hash & (ht->capacity - 1);
	uint32_t probes = 0;

	while (ht->states[pos] != 0 && probes < ht->capacity) {
		if (ht->keys[pos] == key) {
			if (payload)
				*payload = ht->payloads[ht->values[pos]];
			return true;
		}
		pos = (pos + 1) & (ht->capacity - 1);
		probes++;
	}
	return false;
}

/*
 * Find all matching positions for a key (handles multi-match).
 * Caller provides an array to collect payload indices and max capacity.
 * Returns number of matches found.
 */
static inline uint32_t
volvec_ht_probe_all(const VecLinearProbeHT *ht, uint64_t key, uint32_t hash,
					uint32_t *match_slots, uint32_t max_matches)
{
	uint32_t pos = hash & (ht->capacity - 1);
	uint32_t probes = 0;
	uint32_t nmatches = 0;

	while (ht->states[pos] != 0 && probes < ht->capacity) {
		if (ht->keys[pos] == key) {
			if (nmatches < max_matches)
				match_slots[nmatches++] = ht->values[pos];
		}
		pos = (pos + 1) & (ht->capacity - 1);
		probes++;
	}
	return nmatches;
}

static inline void
volvec_ht_destroy(VecLinearProbeHT *ht)
{
	if (ht->keys) pfree(ht->keys);
	if (ht->values) pfree(ht->values);
	if (ht->states) pfree(ht->states);
	if (ht->payloads) pfree(ht->payloads);
	ht->keys = nullptr;
	ht->values = nullptr;
	ht->states = nullptr;
	ht->payloads = nullptr;
	ht->capacity = 0;
	ht->count = 0;
}

static inline void
volvec_bloom_destroy(VecBloomFilter *bloom)
{
	if (bloom->bits) pfree(bloom->bits);
	bloom->bits = nullptr;
	bloom->bit_count = 0;
	bloom->hash_funcs = 0;
}

/* --- Radix partitioning utilities --- */

/*
 * Extract partition index from hash: uses low RADIX_BITS bits.
 * With RADIX_BITS=8, gives 256 partitions.
 */
static inline uint32_t
volvec_radix_partition_idx(uint32_t hash)
{
	return hash & ((1 << VOLVEC_RADIX_BITS) - 1);
}

/*
 * Compute histogram of partition distribution for a batch of keys.
 */
static inline void
volvec_compute_histogram(const uint32_t *hashes, int n, uint32_t *histogram)
{
	memset(histogram, 0, VOLVEC_RADIX_FANOUT * sizeof(uint32_t));
	for (int i = 0; i < n; i++) {
		uint32_t pidx = volvec_radix_partition_idx(hashes[i]);
		histogram[pidx]++;
	}
}

/*
 * Compute prefix sums in-place. After this, offsets[i] contains the
 * starting position for partition i.
 */
static inline void
volvec_prefix_sum(uint32_t *offsets)
{
	uint32_t sum = 0;
	for (int i = 0; i < VOLVEC_RADIX_FANOUT; i++) {
		uint32_t count = offsets[i];
		offsets[i] = sum;
		sum += count;
	}
}

/*
 * Allocate partition storage based on histogram counts.
 * Each partition gets a contiguous buffer for keys and payloads.
 */
static inline void
volvec_alloc_partitions(VecHashPartitionTable *pt, MemoryContext context)
{
	MemoryContext old = MemoryContextSwitchTo(context);
	for (int i = 0; i < VOLVEC_RADIX_FANOUT; i++) {
		if (pt->partition_counts[i] == 0)
			continue;
		pt->partitions[i].capacity = pt->partition_counts[i];
		pt->partitions[i].count = 0;
		pt->partitions[i].keys = (uint64_t *) palloc(pt->partitions[i].capacity * sizeof(uint64_t));
		pt->partitions[i].payloads = (uint64_t *) palloc(pt->partitions[i].capacity * sizeof(uint64_t));
		pt->partitions[i].is_external = false;
	}
	MemoryContextSwitchTo(old);
}

/*
 * Distribute keys and payloads into their target partitions.
 * Uses running offsets (prefix sums) to write directly to the right position.
 */
static inline void
volvec_partition_batch(const uint64_t *keys, const uint64_t *payloads,
					   const uint32_t *hashes, int n,
					   VecHashPartitionTable *pt, uint32_t *running_offsets)
{
	for (int i = 0; i < n; i++) {
		uint32_t pidx = volvec_radix_partition_idx(hashes[i]);
		uint32_t pos = running_offsets[pidx]++;
		pt->partitions[pidx].keys[pos] = keys[i];
		pt->partitions[pidx].payloads[pos] = payloads[i];
		pt->partitions[pidx].count++;
	}
}

} /* namespace pg_yaap */

#endif /* VOLVEC_HASH_TABLE_HPP */
