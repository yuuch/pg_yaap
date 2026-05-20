#pragma once

#include "core/types.hpp"

namespace pg_yaap
{

/*
 * Robin-Hood hash table with PG MemoryContext allocation.
 * Minimal interface - callers adapt to this, not vice versa.
 */
template <typename Key, typename Value, typename Hash = std::hash<Key>>
class RobinHoodPgMap {
public:
	struct Slot {
		Key key;
		Value val;
		uint32_t psl;      // probe sequence length
		bool occupied;

		Slot() : psl(0), occupied(false) {}
	};

private:
	Slot *slots_;
	size_t capacity_;
	size_t size_;
	size_t mask_;
	Hash hasher_;
	MemoryContext ctx_;

	static constexpr double MAX_LOAD = 0.9;
	static constexpr size_t INITIAL_CAP = 16;

	size_t next_pow2(size_t n) {
		if (n <= 1) return 2;
		n--;
		n |= n >> 1; n |= n >> 2; n |= n >> 4;
		n |= n >> 8; n |= n >> 16; n |= n >> 32;
		return n + 1;
	}

	void rehash(size_t new_cap) {
		Slot *old_slots = slots_;
		size_t old_cap = capacity_;
		size_t alloc_size = new_cap * sizeof(Slot);

		if (new_cap != 0 && alloc_size / new_cap != sizeof(Slot))
			elog(ERROR, "pg_yaap robin hood hash table allocation overflow (new_cap=%zu slot_size=%zu)",
				 new_cap, sizeof(Slot));
		if (alloc_size > ((size_t) 512 * 1024 * 1024))
			elog(ERROR,
				 "pg_yaap refusing huge robin hood hash table allocation (new_cap=%zu slot_size=%zu bytes=%zu old_cap=%zu size=%zu)",
				 new_cap,
				 sizeof(Slot),
				 alloc_size,
				 old_cap,
				 size_);

		capacity_ = new_cap;
		mask_ = capacity_ - 1;
		slots_ = static_cast<Slot*>(MemoryContextAllocZero(ctx_, alloc_size));

		for (size_t i = 0; i < capacity_; ++i)
			new (&slots_[i]) Slot();

		if (old_slots) {
			for (size_t i = 0; i < old_cap; ++i) {
				if (old_slots[i].occupied) {
					insert_internal(std::move(old_slots[i].key), std::move(old_slots[i].val));
					old_slots[i].~Slot();
				}
			}
			pfree(old_slots);
		}
	}

	void insert_internal(Key k, Value v) {
		size_t idx = hasher_(k) & mask_;
		uint32_t psl = 0;

		while (true) {
			Slot &s = slots_[idx];

			if (!s.occupied) {
				new (&s.key) Key(std::move(k));
				new (&s.val) Value(std::move(v));
				s.psl = psl;
				s.occupied = true;
				return;
			}

			if (psl > s.psl) {
				std::swap(k, s.key);
				std::swap(v, s.val);
				std::swap(psl, s.psl);
			}

			idx = (idx + 1) & mask_;
			psl++;
		}
	}

public:
	RobinHoodPgMap(MemoryContext ctx = CurrentMemoryContext)
		: slots_(nullptr), capacity_(0), size_(0), mask_(0), hasher_(), ctx_(ctx) {
		rehash(INITIAL_CAP);
	}

	RobinHoodPgMap(size_t hint, Hash h, MemoryContext ctx)
		: slots_(nullptr), capacity_(0), size_(0), mask_(0), hasher_(h), ctx_(ctx) {
		rehash(next_pow2(hint));
	}

	~RobinHoodPgMap() {
		if (slots_) {
			for (size_t i = 0; i < capacity_; ++i)
				if (slots_[i].occupied)
					slots_[i].~Slot();
			pfree(slots_);
		}
	}

	RobinHoodPgMap(const RobinHoodPgMap&) = delete;
	RobinHoodPgMap& operator=(const RobinHoodPgMap&) = delete;

	// Core API: insert returns (slot*, inserted)
	std::pair<Slot*, bool> insert(const Key& k) {
		if (size_ >= capacity_ * MAX_LOAD)
			rehash(capacity_ * 2);

		size_t idx = hasher_(k) & mask_;
		uint32_t psl = 0;
		Slot *original_slot = nullptr;
		Key inserting_key = k;
		Value inserting_val{};
		bool is_new = false;

		while (true) {
			Slot &s = slots_[idx];

			if (!s.occupied) {
				// Found empty slot - insert here
				new (&s.key) Key(std::move(inserting_key));
				new (&s.val) Value(std::move(inserting_val));
				s.psl = psl;
				s.occupied = true;
				if (!original_slot) {
					// First insertion
					++size_;
					return {&s, true};
				} else {
					// Displaced insertion complete
					return {original_slot, is_new};
				}
			}

			if (s.key == inserting_key) {
				// Found existing key
				if (!original_slot) {
					return {&s, false};
				}
				// This shouldn't happen (displacing a duplicate)
				return {&s, false};
			}

			if (psl > s.psl) {
				// Displace this entry (Robin Hood: steal from the rich)
				if (!original_slot) {
					// Save the slot where the original key landed
					original_slot = &s;
					is_new = true;
					++size_;
				}
				
				// Swap current entry with what we're inserting
				std::swap(inserting_key, s.key);
				std::swap(inserting_val, s.val);
				std::swap(psl, s.psl);
			}

			idx = (idx + 1) & mask_;
			psl++;
		}
	}

	// Core API: find returns slot* or nullptr
	Slot* find(const Key& k) {
		size_t idx = hasher_(k) & mask_;
		uint32_t psl = 0;

		while (true) {
			Slot &s = slots_[idx];
			if (!s.occupied || s.psl < psl)
				return nullptr;
			if (s.key == k)
				return &s;
			idx = (idx + 1) & mask_;
			psl++;
		}
	}

	const Slot* find(const Key& k) const {
		return const_cast<RobinHoodPgMap*>(this)->find(k);
	}

	void reserve(size_t n) {
		size_t need = static_cast<size_t>(n / MAX_LOAD) + 1;
		if (need > capacity_)
			rehash(next_pow2(need));
	}

	size_t size() const { return size_; }
	bool empty() const { return size_ == 0; }

	// Iterator: simple forward scan
	struct Iterator {
		RobinHoodPgMap *map;
		size_t idx;

		Iterator() : map(nullptr), idx(0) {}

		Iterator(RobinHoodPgMap *m, size_t i) : map(m), idx(i) {
			skip_empty();
		}

		void skip_empty() {
			while (idx < map->capacity_ && !map->slots_[idx].occupied)
				++idx;
		}

		Slot& operator*() { return map->slots_[idx]; }
		Slot* operator->() { return &map->slots_[idx]; }

		Iterator& operator++() {
			++idx;
			skip_empty();
			return *this;
		}

		bool operator==(const Iterator& o) const {
			return map == o.map && idx == o.idx;
		}
		bool operator!=(const Iterator& o) const { return !(*this == o); }
	};

	Iterator begin() { return Iterator(this, 0); }
	Iterator end() { return Iterator(this, capacity_); }
	
	Iterator begin() const { return Iterator(const_cast<RobinHoodPgMap*>(this), 0); }
	Iterator end() const { return Iterator(const_cast<RobinHoodPgMap*>(this), capacity_); }
};

} // namespace pg_yaap
