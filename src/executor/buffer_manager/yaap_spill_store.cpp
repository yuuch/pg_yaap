extern "C" {
#include "postgres.h"
#include "storage/buffile.h"
#include "storage/fd.h"
#include "storage/fileset.h"
#include "utils/elog.h"
#include "utils/wait_event.h"
}

#include <cstdio>
#include <cstring>
#include <fcntl.h>

#include "buffer_manager/yaap_spill_store.hpp"

namespace pg_yaap {
namespace buffer {

YaapSpillStore::YaapSpillStore(MemoryContext context)
{
	(void) context;
}

YaapSpillStore::~YaapSpillStore()
{
	Close();
}

void
YaapSpillStore::AttachSharedFileSet(SharedFileSet *fileset, const char *file_prefix)
{
	if (HasOpenStorage())
		ereport(ERROR, (errmsg("pg_yaap: cannot attach SharedFileSet after spill storage creation")));
	shared_fileset_ = fileset;
	if (file_prefix != nullptr && file_prefix[0] != '\0')
		std::snprintf(shared_file_name_, sizeof(shared_file_name_), "%s", file_prefix);
}

void
YaapSpillStore::SetSharedFileName(const char *file_name)
{
	if (shared_fileset_ == nullptr)
		return;
	if (local_file_ != nullptr || file_name == nullptr || file_name[0] == '\0')
		ereport(ERROR, (errmsg("pg_yaap: invalid shared spill file name update")));
	if (shared_file_ >= 0)
	{
		FileClose(shared_file_);
		shared_file_ = -1;
	}
	std::snprintf(shared_file_name_, sizeof(shared_file_name_), "%s", file_name);
	shared_write_offset_ = 0;
}

void
YaapSpillStore::DeleteSharedFile(const char *file_name)
{
	if (shared_fileset_ != nullptr && file_name != nullptr && file_name[0] != '\0')
		(void) FileSetDelete(&shared_fileset_->fs, file_name, false);
}

bool
YaapSpillStore::HasOpenStorage() const
{
	if (local_file_ != nullptr || shared_file_ >= 0)
		return true;
	for (const auto &entry : read_file_cache_)
		if (entry.file >= 0)
			return true;
	return false;
}

void
YaapSpillStore::Close()
{
	if (local_file_ != nullptr)
	{
		BufFileClose(local_file_);
		local_file_ = nullptr;
	}
	if (shared_file_ >= 0)
	{
		FileClose(shared_file_);
		shared_file_ = -1;
	}
	for (auto &entry : read_file_cache_)
	{
		if (entry.file >= 0)
			FileClose(entry.file);
		entry = ReadFileCacheEntry{};
	}
	read_cache_clock_ = 0;
}

void
YaapSpillStore::EnsureLocalFile()
{
	if (local_file_ != nullptr)
		return;
	local_file_ = BufFileCreateTemp(true);
}

void
YaapSpillStore::EnsureSharedFile()
{
	if (shared_file_ >= 0)
		return;
	if (shared_fileset_ == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: shared spill store missing SharedFileSet")));
	if (shared_file_name_[0] == '\0')
		std::snprintf(shared_file_name_, sizeof(shared_file_name_), "pg_yaap_spill_%p", this);
	shared_file_ = FileSetCreate(&shared_fileset_->fs, shared_file_name_);
	if (shared_file_ <= 0)
		ereport(ERROR, (errmsg("pg_yaap: failed to create shared spill file")));
}

File
YaapSpillStore::OpenSharedReadFile(const char *file_name)
{
	if (shared_fileset_ == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: shared spill read missing SharedFileSet")));
	for (auto &entry : read_file_cache_)
	{
		if (entry.file >= 0 && std::strncmp(entry.file_name, file_name,
												 sizeof(entry.file_name)) == 0)
		{
			entry.last_used = ++read_cache_clock_;
			return entry.file;
		}
	}

	ReadFileCacheEntry *victim = &read_file_cache_[0];
	for (auto &entry : read_file_cache_)
	{
		if (entry.file < 0)
		{
			victim = &entry;
			break;
		}
		if (entry.last_used < victim->last_used)
			victim = &entry;
	}
	if (victim->file >= 0)
	{
		FileClose(victim->file);
		*victim = ReadFileCacheEntry{};
	}
	File file = FileSetOpen(&shared_fileset_->fs, file_name, O_RDONLY | PG_BINARY);
	if (file <= 0)
		ereport(ERROR, (errmsg("pg_yaap: failed to open shared spill file")));
	victim->file = file;
	victim->last_used = ++read_cache_clock_;
	std::snprintf(victim->file_name, sizeof(victim->file_name), "%s", file_name);
	return file;
}

void
YaapSpillStore::WriteFileExact(File file, const void *data, size_t size, off_t offset)
{
	const char *cursor = static_cast<const char *>(data);
	size_t remaining = size;
	off_t write_offset = offset;
	while (remaining > 0)
	{
		const ssize_t written = FileWrite(file,
			cursor,
			remaining,
			write_offset,
			WAIT_EVENT_BUFFILE_WRITE);
		if (written <= 0)
			ereport(ERROR, (errmsg("pg_yaap: failed to write shared spill file")));
		cursor += written;
		write_offset += written;
		remaining -= static_cast<size_t>(written);
	}
}

void
YaapSpillStore::ReadFileExact(File file, void *data, size_t size, off_t offset)
{
	char *cursor = static_cast<char *>(data);
	size_t remaining = size;
	off_t read_offset = offset;
	while (remaining > 0)
	{
		const ssize_t read = FileRead(file,
			cursor,
			remaining,
			read_offset,
			WAIT_EVENT_BUFFILE_READ);
		if (read <= 0)
			ereport(ERROR, (errmsg("pg_yaap: failed to read shared spill file")));
		cursor += read;
		read_offset += read;
		remaining -= static_cast<size_t>(read);
	}
}

void
YaapSpillStore::WriteBlock(const void *header,
                           size_t header_size,
                           const void *payload,
                           uint64_t payload_size,
                           YaapSpillExtent *extent)
{
	if (extent == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: spill write requires an output extent")));
	if (shared_fileset_ != nullptr)
	{
		EnsureSharedFile();
		const uint64_t start_offset = shared_write_offset_;
		WriteFileExact(shared_file_, header, header_size, static_cast<off_t>(start_offset));
		if (payload_size > 0)
			WriteFileExact(shared_file_,
				payload,
				static_cast<size_t>(payload_size),
				static_cast<off_t>(start_offset + header_size));
		extent->fileno = 0;
		extent->offset = start_offset;
		extent->bytes = header_size + payload_size;
		std::memcpy(extent->file_name, shared_file_name_, sizeof(extent->file_name));
		shared_write_offset_ += extent->bytes;
		return;
	}

	EnsureLocalFile();
	int fileno = 0;
	off_t offset = 0;
	BufFileTell(local_file_, &fileno, &offset);
	BufFileWrite(local_file_, header, header_size);
	if (payload_size > 0)
		BufFileWrite(local_file_, payload, static_cast<size_t>(payload_size));
	extent->fileno = fileno;
	extent->offset = static_cast<uint64_t>(offset);
	extent->bytes = header_size + payload_size;
	std::memset(extent->file_name, 0, sizeof(extent->file_name));
}

bool
YaapSpillStore::PrefetchBlock(const YaapSpillExtent &extent)
{
	/* FilePrefetch uses F_RDADVISE on macOS and POSIX_FADV_WILLNEED on
	 * supported Unix builds. Failure of this optional hint leaves the normal
	 * checked ReadBlock path responsible for reading and validating data. */
	if (shared_fileset_ == nullptr || extent.file_name[0] == '\0' || extent.bytes == 0 ||
		extent.bytes > INT_MAX || extent.offset > static_cast<uint64_t>(INT64_MAX) - extent.bytes)
		return false;
	const File file = OpenSharedReadFile(extent.file_name);
	return FilePrefetch(file, static_cast<off_t>(extent.offset),
		static_cast<off_t>(extent.bytes), WAIT_EVENT_BUFFILE_READ) == 0;
}

void
YaapSpillStore::ReadBlock(const YaapSpillExtent &extent,
                          void *header,
                          size_t header_size,
                          void *payload,
                          uint64_t payload_size)
{
	if (extent.file_name[0] != '\0')
	{
		if (shared_fileset_ == nullptr)
			ereport(ERROR, (errmsg("pg_yaap: shared spill read missing SharedFileSet")));
		File file = -1;
		if (shared_file_ >= 0 &&
			std::strncmp(extent.file_name, shared_file_name_, sizeof(extent.file_name)) == 0)
			file = shared_file_;
		else
			file = OpenSharedReadFile(extent.file_name);
		ReadFileExact(file, header, header_size, static_cast<off_t>(extent.offset));
		if (payload_size > 0)
			ReadFileExact(file,
				payload,
				static_cast<size_t>(payload_size),
				static_cast<off_t>(extent.offset + header_size));
		return;
	}

	if (local_file_ == nullptr)
		ereport(ERROR, (errmsg("pg_yaap: attempted to read local spill without an open file")));
	if (BufFileSeek(local_file_, extent.fileno, static_cast<off_t>(extent.offset), SEEK_SET) != 0)
		ereport(ERROR, (errmsg("pg_yaap: failed to seek local spill file")));
	BufFileReadExact(local_file_, header, header_size);
	if (payload_size > 0)
		BufFileReadExact(local_file_, payload, static_cast<size_t>(payload_size));
}

}  /* namespace buffer */
}  /* namespace pg_yaap */
