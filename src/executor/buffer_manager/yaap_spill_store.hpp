#pragma once

extern "C" {
#include "postgres.h"
#include "storage/buffile.h"
#include "storage/fd.h"
#include "storage/sharedfileset.h"
#include "utils/memutils.h"
}

#include <cstdint>

namespace pg_yaap {
namespace buffer {

struct YaapSpillExtent {
	int32_t fileno = 0;
	uint64_t offset = 0;
	uint64_t bytes = 0;
	char file_name[64] = {0};
};

class YaapSpillStore {
public:
	explicit YaapSpillStore(MemoryContext context);
	~YaapSpillStore();

	YaapSpillStore(const YaapSpillStore &) = delete;
	YaapSpillStore &operator=(const YaapSpillStore &) = delete;

	void AttachSharedFileSet(SharedFileSet *fileset, const char *file_prefix);
	void SetSharedFileName(const char *file_name);
	void DeleteSharedFile(const char *file_name);
	bool HasOpenStorage() const;
	void Close();

	void WriteBlock(const void *header,
	                size_t header_size,
	                const void *payload,
	                uint64_t payload_size,
	                YaapSpillExtent *extent);
	/* Advisory OS-cache prefetch; does not allocate/reload a DSM segment. */
	bool PrefetchBlock(const YaapSpillExtent &extent);
	void ReadBlock(const YaapSpillExtent &extent,
	               void *header,
	               size_t header_size,
	               void *payload,
	               uint64_t payload_size);

private:
	static constexpr uint32_t kReadFileCacheEntries = 8;
	struct ReadFileCacheEntry {
		File file = -1;
		uint64_t last_used = 0;
		char file_name[64] = {0};
	};
	File OpenSharedReadFile(const char *file_name);
	void EnsureLocalFile();
	void EnsureSharedFile();
	static void WriteFileExact(File file, const void *data, size_t size, off_t offset);
	static void ReadFileExact(File file, void *data, size_t size, off_t offset);

	SharedFileSet *shared_fileset_ = nullptr;
	BufFile *local_file_ = nullptr;
	File shared_file_ = -1;
	uint64_t shared_write_offset_ = 0;
	uint64_t read_cache_clock_ = 0;
	ReadFileCacheEntry read_file_cache_[kReadFileCacheEntries]{};
	char shared_file_name_[64] = {0};
};

}  /* namespace buffer */
}  /* namespace pg_yaap */
