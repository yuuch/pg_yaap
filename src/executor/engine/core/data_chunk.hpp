#pragma once

#include "core/types.hpp"
#include "core/memory.hpp"

namespace pg_yaap
{

struct SelectionVector { uint16_t row_ids[DEFAULT_CHUNK_SIZE]; uint16_t count; void clear() { count = 0; } };
struct VecStringRef { uint32_t len; uint32_t offset; uint64_t prefix; };
struct VecDictionaryDesc { bool active; uint8_t source_slot; uint16_t _pad0; };
static constexpr uint32_t kVecStringInlineOffset = UINT32_MAX;

static inline bool
VecStringRefIsInline(const VecStringRef &ref)
{
	return ref.len > 0 && ref.len <= 8 && ref.offset == kVecStringInlineOffset;
}

static inline const char *
VecStringRefDataPtr(const VecStringRef &ref, const char *arena_base)
{
	if (ref.len == 0)
		return "";
	if (VecStringRefIsInline(ref))
		return reinterpret_cast<const char *>(&ref.prefix);
	if (arena_base == nullptr)
		return nullptr;
	return arena_base + ref.offset;
}

enum class VecOutputStorageKind : uint8_t {
	Int32,
	Int64,
	Double,
	StringRef,
	NumericScaledInt64,
	NumericAvgPair
};

struct VecOutputColMeta {
	Oid sql_type = InvalidOid;
	VecOutputStorageKind storage_kind = VecOutputStorageKind::Int32;
	int scale = 0;
};

template <uint16_t Capacity>
struct alignas(16) DataChunk {
	static void *operator new(std::size_t size)
	{
		return MemoryContextAllocAligned(CurrentMemoryContext, size, alignof(DataChunk), 0);
	}

	static void operator delete(void *ptr) noexcept
	{
		if (ptr != nullptr)
			pfree(ptr);
	}

	static void operator delete(void *ptr, std::size_t) noexcept
	{
		if (ptr != nullptr)
			pfree(ptr);
	}

	uint16_t count;
	alignas(16) double double_columns[16][Capacity];
	alignas(16) int64_t int64_columns[16][Capacity];
	alignas(16) int32_t int32_columns[16][Capacity];
	alignas(16) VecStringRef string_columns[16][Capacity];
	alignas(16) uint8_t nulls[16][Capacity]; /* Use uint8_t for reliability */
	alignas(16) uint16_t double_dict_indices[16][Capacity];
	alignas(16) uint16_t int64_dict_indices[16][Capacity];
	alignas(16) uint16_t int32_dict_indices[16][Capacity];
	alignas(16) uint16_t string_dict_indices[16][Capacity];
	VecDictionaryDesc double_dict[16];
	VecDictionaryDesc int64_dict[16];
	VecDictionaryDesc int32_dict[16];
	VecDictionaryDesc string_dict[16];
	SelectionVector sel;
	bool has_selection;
	VolVecVector<char> string_arena;

	DataChunk()
		: count(0),
		  has_selection(false),
		  string_arena(PgMemoryContextAllocator<char>(CurrentMemoryContext))
	{
		memset(nulls, 0, sizeof(nulls));
		clear_dictionaries();
	}

	void clear_dictionaries()
	{
		for (uint8_t slot = 0; slot < 16; ++slot)
		{
			double_dict[slot] = VecDictionaryDesc{false, 0, 0};
			int64_dict[slot] = VecDictionaryDesc{false, 0, 0};
			int32_dict[slot] = VecDictionaryDesc{false, 0, 0};
			string_dict[slot] = VecDictionaryDesc{false, 0, 0};
		}
	}

	void reset_lightweight() {
		count = 0;
		sel.clear();
		has_selection = false;
		clear_dictionaries();
		string_arena.clear();
	}

	void reset() {
		reset_lightweight();
		memset(nulls, 0, sizeof(nulls));
	}

	bool has_any_dictionary() const
	{
		for (int i = 0; i < 16; ++i)
		{
			if (double_dict[i].active || int64_dict[i].active ||
				int32_dict[i].active || string_dict[i].active)
				return true;
		}
		return false;
	}

	bool has_int32_dictionary(uint8_t slot) const { return int32_dict[slot].active; }
	bool has_int64_dictionary(uint8_t slot) const { return int64_dict[slot].active; }
	bool has_double_dictionary(uint8_t slot) const { return double_dict[slot].active; }
	bool has_string_dictionary(uint8_t slot) const { return string_dict[slot].active; }

	void clear_int32_dictionary(uint8_t slot) { int32_dict[slot] = VecDictionaryDesc{false, 0, 0}; }
	void clear_int64_dictionary(uint8_t slot) { int64_dict[slot] = VecDictionaryDesc{false, 0, 0}; }
	void clear_double_dictionary(uint8_t slot) { double_dict[slot] = VecDictionaryDesc{false, 0, 0}; }
	void clear_string_dictionary(uint8_t slot) { string_dict[slot] = VecDictionaryDesc{false, 0, 0}; }

	void set_int32_dictionary(uint8_t slot, uint8_t source_slot, const uint16_t *indices, uint16_t logical_count)
	{
		int32_dict[slot] = VecDictionaryDesc{true, source_slot, 0};
		memcpy(int32_dict_indices[slot], indices, logical_count * sizeof(uint16_t));
	}
	void set_int64_dictionary(uint8_t slot, uint8_t source_slot, const uint16_t *indices, uint16_t logical_count)
	{
		int64_dict[slot] = VecDictionaryDesc{true, source_slot, 0};
		memcpy(int64_dict_indices[slot], indices, logical_count * sizeof(uint16_t));
	}
	void set_double_dictionary(uint8_t slot, uint8_t source_slot, const uint16_t *indices, uint16_t logical_count)
	{
		double_dict[slot] = VecDictionaryDesc{true, source_slot, 0};
		memcpy(double_dict_indices[slot], indices, logical_count * sizeof(uint16_t));
	}
	void set_string_dictionary(uint8_t slot, uint8_t source_slot, const uint16_t *indices, uint16_t logical_count)
	{
		string_dict[slot] = VecDictionaryDesc{true, source_slot, 0};
		memcpy(string_dict_indices[slot], indices, logical_count * sizeof(uint16_t));
	}

	bool column_has_nulls(uint8_t slot, uint16_t row_count) const
	{
		return row_count != 0 && std::memchr(nulls[slot], 1, row_count) != nullptr;
	}

	int32_t get_int32(uint8_t slot, uint16_t row_idx) const
	{
		if (int32_dict[slot].active)
			return int32_columns[int32_dict[slot].source_slot][int32_dict_indices[slot][row_idx]];
		return int32_columns[slot][row_idx];
	}

	int64_t get_int64(uint8_t slot, uint16_t row_idx) const
	{
		if (int64_dict[slot].active)
			return int64_columns[int64_dict[slot].source_slot][int64_dict_indices[slot][row_idx]];
		return int64_columns[slot][row_idx];
	}

	double get_double(uint8_t slot, uint16_t row_idx) const
	{
		if (double_dict[slot].active)
			return double_columns[double_dict[slot].source_slot][double_dict_indices[slot][row_idx]];
		return double_columns[slot][row_idx];
	}

	VecStringRef get_string_ref(uint8_t slot, uint16_t row_idx) const
	{
		if (string_dict[slot].active)
			return string_columns[string_dict[slot].source_slot][string_dict_indices[slot][row_idx]];
		return string_columns[slot][row_idx];
	}

	const char *get_string_ptr(uint8_t slot, uint16_t row_idx) const
	{
		const VecStringRef *ref = nullptr;
		if (string_dict[slot].active)
			ref = &string_columns[string_dict[slot].source_slot][string_dict_indices[slot][row_idx]];
		else
			ref = &string_columns[slot][row_idx];
		return VecStringRefDataPtr(*ref, string_arena.data());
	}

	void flatten()
	{
		for (uint8_t slot = 0; slot < 16; ++slot)
		{
			if (int32_dict[slot].active)
			{
				int32_t tmp[Capacity];
				for (uint16_t row = 0; row < count; ++row)
					tmp[row] = get_int32(slot, row);
				memcpy(int32_columns[slot], tmp, count * sizeof(int32_t));
				clear_int32_dictionary(slot);
			}
			if (int64_dict[slot].active)
			{
				int64_t tmp[Capacity];
				for (uint16_t row = 0; row < count; ++row)
					tmp[row] = get_int64(slot, row);
				memcpy(int64_columns[slot], tmp, count * sizeof(int64_t));
				clear_int64_dictionary(slot);
			}
			if (double_dict[slot].active)
			{
				double tmp[Capacity];
				for (uint16_t row = 0; row < count; ++row)
					tmp[row] = get_double(slot, row);
				memcpy(double_columns[slot], tmp, count * sizeof(double));
				clear_double_dictionary(slot);
			}
			if (string_dict[slot].active)
			{
				VecStringRef tmp[Capacity];
				for (uint16_t row = 0; row < count; ++row)
					tmp[row] = get_string_ref(slot, row);
				memcpy(string_columns[slot], tmp, count * sizeof(VecStringRef));
				clear_string_dictionary(slot);
			}
		}
	}

	VecStringRef store_string_bytes(const char *data, uint32_t len)
	{
		VecStringRef ref{len, 0, 0};

		if (len == 0 || data == nullptr)
			return ref;
		if (len > 65536)
			elog(ERROR, "pg_yaap suspicious string length %u while materializing DataChunk", len);
		memcpy(&ref.prefix, data, len > 8 ? 8 : len);
		if (len <= 8)
		{
			ref.offset = kVecStringInlineOffset;
			return ref;
		}
		ref.offset = (uint32_t) string_arena.size();
		string_arena.insert(string_arena.end(), data, data + len);
		return ref;
	}
	const char *get_string_ptr(const VecStringRef &ref) const
	{
		return VecStringRefDataPtr(ref, string_arena.data());
	}
	void get_double_ptrs(double** out) { for(int i=0; i<16; i++) out[i] = double_columns[i]; }
	void get_int64_ptrs(int64_t** out) { for(int i=0; i<16; i++) out[i] = int64_columns[i]; }
	void get_int32_ptrs(int32_t** out) { for(int i=0; i<16; i++) out[i] = int32_columns[i]; }
	void get_string_ptrs(VecStringRef** out) { for(int i=0; i<16; i++) out[i] = string_columns[i]; }
	void get_null_ptrs(uint8_t** out) { for(int i=0; i<16; i++) out[i] = nulls[i]; }
};

static constexpr int kMaxDeformTargets = 16;
/*
 * kBpchar1 — BPCHAR(1) varlena payload decoded as a single int32 character
 * value. Required because Q1's `l_returnflag`/`l_linestatus` (BPCHAR(1)) are
 * stored as varlenas whose payload byte IS the character (Bug G fix); a plain
 * 4-byte load past the varlena header reads garbage. Decoded into the int32
 * column slot, identical destination layout to kInt32.
 */
enum class DeformDecodeKind : uint8_t { kInt32, kInt64, kDate32, kFloat8, kNumeric, kStringRef, kBpchar1 };
struct DeformTarget { int att_index; uint16_t dst_col; DeformDecodeKind decode_kind; };
struct DeformProgram {
	int ntargets; int last_att_index; DeformTarget targets[kMaxDeformTargets];
	void reset() { ntargets = 0; last_att_index = -1; }
	void add_target(int att, int dst, DeformDecodeKind k) { if(ntargets<kMaxDeformTargets) targets[ntargets++] = {att, (uint16_t)dst, k}; }
	void finalize() {
		for (int i = 1; i < ntargets; i++) {
			DeformTarget key = targets[i];
			int j = i - 1;
			while (j >= 0 && targets[j].att_index > key.att_index) {
				targets[j + 1] = targets[j];
				j--;
			}
			targets[j + 1] = key;
		}
		last_att_index = (ntargets > 0) ? targets[ntargets - 1].att_index : -1;
	}
};
struct DeformBindings { void *columns_data[kMaxDeformTargets]; uint8_t *columns_nulls[kMaxDeformTargets]; int ncolumns; DataChunk<DEFAULT_CHUNK_SIZE> *owner_chunk; };
typedef void (*JitDeformFunc)(HeapTupleHeader tuphdr, void **col_data_ptrs, uint8_t **col_null_ptrs, uint32 row_idx, DataChunk<DEFAULT_CHUNK_SIZE> *owner_chunk);

} // namespace pg_yaap
