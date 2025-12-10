#include "noop_cache_reader.hpp"

#include "cache_filesystem.hpp"
#include "cache_httpfs_instance_state.hpp"

namespace duckdb {

void NoopCacheReader::ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset,
                                   idx_t requested_bytes_to_read, idx_t file_size) {
	auto &cache_handle = handle.Cast<CacheFileSystemHandle>();
	auto *internal_filesystem = cache_handle.GetInternalFileSystem();

	// Get profile collector from handle's connection_id via instance_state
	auto state = instance_state.lock();
	auto *profile_collector =
	    state ? state->profile_collector_manager.GetProfileCollector(cache_handle.GetConnectionId()) : nullptr;

	if (profile_collector) {
		const auto latency_guard = profile_collector->RecordOperationStart(IoOperation::kRead);
		internal_filesystem->Read(*cache_handle.internal_file_handle, buffer, requested_bytes_to_read,
		                          requested_start_offset);
	} else {
		internal_filesystem->Read(*cache_handle.internal_file_handle, buffer, requested_bytes_to_read,
		                          requested_start_offset);
	}
}

} // namespace duckdb
