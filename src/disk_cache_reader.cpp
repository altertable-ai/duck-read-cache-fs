// Design notes on concurrent access for local cache files:
// - There could be multiple threads accessing one local cache file, some of them try to open and read, while others
// trying to delete if the file is stale;
// - To avoid data race (open the file after deletion), read threads should open the file directly, instead of check
// existence and open, which guarantees even the file get deleted due to staleness, read threads still get a snapshot.

#include "cache_filesystem.hpp"
#include "cache_filesystem_logger.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "cache_read_chunk.hpp"
#include "disk_cache_reader.hpp"
#include "disk_cache_util.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "in_mem_cache_remap.hpp"
#include "in_memory_data_cache_storage.hpp"
#include "utils/include/chunk_utils.hpp"
#include "utils/include/filesystem_utils.hpp"
#include "utils/include/page_aligned_data_chunk.hpp"
#include "utils/include/parallel_executor.hpp"
#include "utils/include/thread_utils.hpp"

#include <cstdint>
#include <utility>

namespace duckdb {

DiskCacheReader::DiskCacheReader(weak_ptr<CacheHttpfsInstanceState> instance_state_p)
    : BaseCacheReader(std::move(instance_state_p)), local_filesystem(LocalFileSystem::CreateLocal()) {
}

string DiskCacheReader::EvictCacheBlockLru() {
	const concurrency::lock_guard<concurrency::mutex> lck(cache_file_creation_timestamp_map_mutex);
	// Initialize file creation timestamp map, which should be called only once.
	// IO operation is performed inside of critical section intentionally, since it's required for all threads.
	if (cache_file_creation_timestamp_map.empty()) {
		auto instance_state_locked = GetInstanceConfigOrThrow(instance_state);
		const auto &cache_directories = instance_state_locked->config.on_disk_cache_directories;
		cache_file_creation_timestamp_map = GetOnDiskFilesUnder(cache_directories);
	}
	ALWAYS_ASSERT(!cache_file_creation_timestamp_map.empty());

	auto filepath = std::move(cache_file_creation_timestamp_map.begin()->second);
	cache_file_creation_timestamp_map.erase(cache_file_creation_timestamp_map.begin());
	return filepath;
}

// TODO(hjiang): For oversized filepath, both in-memory cache and on-disk cache stores resolved path, which uses SHA-256
// instead of original filename, likely we should do a translation here. On-disk cache stores original filepath in file
// attributes, in-memory cache should do the same thing.
vector<DataCacheEntryInfo> DiskCacheReader::GetCacheEntriesInfo() const {
	vector<DataCacheEntryInfo> cache_entries_info;

	// Fill in in-memory cache blocks for on disk cache reader.
	if (in_mem_storage != nullptr) {
		auto keys = in_mem_storage->Keys();
		cache_entries_info.reserve(keys.size());
		for (auto &cur_key : keys) {
			cache_entries_info.emplace_back(DataCacheEntryInfo {
			    .cache_filepath = "(no disk cache)",
			    .original_remote_path = std::move(cur_key.fname),
			    .start_offset = cur_key.start_off,
			    .end_offset = cur_key.start_off + cur_key.blk_size,
			    .cache_type = "in-mem-disk-cache",
			});
		}
	}

	// Fill in on disk cache entries.
	auto instance_state_locked = GetInstanceConfigOrThrow(instance_state);
	const auto &cache_directories = instance_state_locked->config.on_disk_cache_directories;
	for (const auto &cur_cache_dir : cache_directories) {
		local_filesystem->ListFiles(
		    cur_cache_dir, [&cache_entries_info, cur_cache_dir](const string &fname, bool /*unused*/) {
			    // Skip in-flight temporary cache files. Their transient names don't follow the cache filename
			    // format, so parsing offsets/sizes out of them would throw; they appear as regular cache entries
			    // once the write renames them into place.
			    if (DiskCacheUtil::IsTempCacheFile(fname)) {
				    return;
			    }
			    auto cache_filepath = StringUtil::Format("%s/%s", cur_cache_dir, fname);
			    auto remote_file_info = DiskCacheUtil::GetRemoteFileInfo(cache_filepath);
			    auto original_remote_path = DiskCacheUtil::TryGetOriginalRemotePath(cache_filepath);
			    cache_entries_info.emplace_back(DataCacheEntryInfo {
			        .cache_filepath = std::move(cache_filepath),
			        .original_remote_path = std::move(original_remote_path),
			        .start_offset = remote_file_info.start_offset,
			        .end_offset = remote_file_info.end_offset,
			        .cache_type = "on-disk",
			    });
		    });
	}

	return cache_entries_info;
}

void DiskCacheReader::ProcessCacheReadChunk(FileHandle &handle, const InstanceConfig &config, const string &version_tag,
                                            CacheReadChunk cache_read_chunk) {
	SetThreadName("RdCachRdThd");

	auto &cache_handle = handle.Cast<CacheFileSystemHandle>();
	auto state = instance_state.lock();
	auto &collector = GetProfileCollectorOrThrow(state, cache_handle.GetConnectionId());

	// Resolve the on-disk cache path once up-front; use the resolved filepath as the unified key for both in-memory and
	// on-disk cache.
	auto cache_file =
	    DiskCacheUtil::GetLocalCacheFile(config.on_disk_cache_directories, handle.GetPath(),
	                                     cache_read_chunk.aligned_start_offset, cache_read_chunk.chunk_size);
	const auto &cache_directory = config.on_disk_cache_directories[cache_file.cache_directory_idx];
	auto cache_dest =
	    DiskCacheUtil::ResolveLocalCacheDestination(cache_directory, cache_file.cache_filepath, handle.GetPath());

	const InMemCacheBlock block_key {handle.GetPath(), cache_read_chunk.aligned_start_offset,
	                                 cache_read_chunk.chunk_size};

	// Attempt in-memory cache first, so potentially we don't need to access disk storage.
	if (in_mem_storage != nullptr) {
		auto pinned = in_mem_storage->Get(block_key, version_tag);
		if (pinned) {
			collector.RecordCacheAccess(CacheEntity::kData, CacheAccess::kCacheHit, cache_read_chunk.bytes_to_copy);
			DUCKDB_LOG_READ_CACHE_HIT((handle));
			cache_read_chunk.CopyBufferToRequestedMemory(pinned->Data());
			return;
		}
	}

	// Attempt to open and read local cache file directly, so a successfully opened file handle won't be
	// deleted by cleanup thread and lead to data race.
	//
	// TODO(hjiang): With in-memory cache block involved, we could place disk write to background thread.
	// Check local disk access before serving from cache.
	const bool can_access_cache_file = state->CanAccessFile(cache_dest.dest_local_filepath);
	if (can_access_cache_file) {
		const auto latency_guard = collector.RecordOperationStart(IoOperation::kDiskCacheRead);
		const DiskCacheUtil::ReadOption read_options {
		    // If on-disk in-memory cache is enabled, use direct IO to avoid double buffering.
		    // Otherwise, rely on page cache for repeated access.
		    .attempt_direct_io = config.enable_disk_reader_mem_cache,
		};
		auto read_result = DiskCacheUtil::ReadLocalCacheFile(cache_dest.dest_local_filepath,
		                                                     cache_read_chunk.chunk_size, version_tag, read_options);
		if (read_result.cache_hit) {
			collector.RecordCacheAccess(CacheEntity::kData, CacheAccess::kCacheHit, cache_read_chunk.bytes_to_copy);
			DUCKDB_LOG_READ_CACHE_HIT((handle));
			cache_read_chunk.CopyBufferToRequestedMemory(read_result.content);

			// Update in-memory cache if applicable.
			if (in_mem_storage != nullptr) {
				in_mem_storage->Put(block_key, std::move(read_result.content), version_tag);
			}
			return;
		}
	}

	// We suffer a cache loss, fallback to remote access then local filesystem write.
	collector.RecordCacheAccess(CacheEntity::kData, CacheAccess::kCacheMiss, cache_read_chunk.bytes_to_copy);
	DUCKDB_LOG_READ_CACHE_MISS((handle));
	auto content = AllocatePageAlignedChunk(cache_read_chunk.chunk_size);
	auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
	auto *internal_filesystem = disk_cache_handle.GetInternalFileSystem();

	{
		const auto latency_guard = collector.RecordOperationStart(IoOperation::kRead);
		internal_filesystem->Read(*disk_cache_handle.internal_file_handle, content.data(), cache_read_chunk.chunk_size,
		                          cache_read_chunk.aligned_start_offset);
		content.length = cache_read_chunk.chunk_size;
	}

	// Copy to destination buffer, if bytes are read into [content] buffer rather than user-provided buffer.
	cache_read_chunk.CopyBufferToRequestedMemory(content);

	// Attempt to cache file locally.
	// We're tolerate of local cache file write failure, which doesn't affect returned content correctness.
	if (!can_access_cache_file) {
		return;
	}
	try {
		DiskCacheUtil::StoreLocalCacheFile(cache_directory, cache_dest, content, version_tag, config,
		                                   [this]() { return EvictCacheBlockLru(); });

		// Update in-memory cache if applicable.
		if (in_mem_storage != nullptr) {
			in_mem_storage->Put(block_key, std::move(content), version_tag);
		}
	} catch (...) {
	}
}

idx_t DiskCacheReader::ScheduleChunks(FileHandle &handle, char *buffer, idx_t requested_start_offset,
                                      idx_t requested_bytes_to_read, idx_t file_size, BaseParallelExecutor &executor) {
	auto instance_state_locked = GetInstanceConfigOrThrow(instance_state);
	const auto &config = instance_state_locked->config;
	std::call_once(cache_init_flag, [this, &config, &instance_state_locked]() {
		if (config.enable_disk_reader_mem_cache) {
			in_mem_storage = BuildInMemoryDataCacheStorage(
			    config.in_mem_cache_storage, instance_state_locked->db_instance,
			    config.disk_reader_max_mem_cache_block_count, config.disk_reader_max_mem_cache_timeout_millisec);
		}
	});

	const ReadRequestParams read_params {
	    .requested_start_offset = requested_start_offset,
	    .requested_bytes_to_read = requested_bytes_to_read,
	    .block_size = config.cache_block_size,
	};

	// Get file-level metadata once before processing chunks.
	const bool enable_cache_validation =
	    instance_state_locked->ResolveSettingsForPath(handle.GetPath()).enable_cache_validation;
	auto version_tag =
	    make_shared_ptr<string>(enable_cache_validation ? handle.Cast<CacheFileSystemHandle>().GetVersionTag() : "");

	// To improve IO performance, we split requested bytes (after alignment) into multiple chunks and fetch them in
	// parallel. The instance state is captured into each task so it stays alive until the executor drains, which for a
	// warm request outlives this call.
	return ForEachCacheReadChunk(
	    read_params, file_size, buffer,
	    [this, &handle, &executor, instance_state_locked, version_tag](const CacheReadChunk &cache_read_chunk) {
		    executor.Schedule([this, &handle, instance_state_locked, version_tag, cache_read_chunk]() {
			    ProcessCacheReadChunk(handle, instance_state_locked->config, *version_tag, cache_read_chunk);
		    });
	    });
}

void DiskCacheReader::ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset,
                                   idx_t requested_bytes_to_read, idx_t file_size) {
	if (requested_bytes_to_read == 0) {
		return;
	}

	auto instance_state_locked = GetInstanceConfigOrThrow(instance_state);
	const auto &config = instance_state_locked->config;
	const ReadRequestParams read_params {
	    .requested_start_offset = requested_start_offset,
	    .requested_bytes_to_read = requested_bytes_to_read,
	    .block_size = config.cache_block_size,
	};
	const ChunkAlignmentInfo alignment_info = CalculateChunkAlignment(read_params);

	// Threads to parallelly perform IO.
	const auto task_count = GetThreadCountForSubrequests(alignment_info.subrequest_count, config.max_subrequest_count);
	auto parallel_executor =
	    CreateParallelExecutor(instance_state_locked->db_instance, config.parallel_read_mode, task_count);

	const idx_t total_bytes_to_cache =
	    ScheduleChunks(handle, buffer, requested_start_offset, requested_bytes_to_read, file_size, *parallel_executor);

	// Block wait for all IO operations to complete.
	parallel_executor->WaitAll();

	// Record "bytes to read" and "bytes to cache".
	auto &cache_handle = handle.Cast<CacheFileSystemHandle>();
	auto state_for_profile = instance_state.lock();
	auto &collector = GetProfileCollectorOrThrow(state_for_profile, cache_handle.GetConnectionId());
	collector.RecordActualCacheRead(/*cache_size=*/total_bytes_to_cache,
	                                /*actual_bytes=*/requested_bytes_to_read);
}

void DiskCacheReader::ScheduleWarm(FileHandle &handle, idx_t requested_start_offset, idx_t requested_bytes_to_read,
                                   idx_t file_size, BaseParallelExecutor &executor) {
	if (requested_bytes_to_read == 0) {
		return;
	}

	const idx_t total_bytes_to_cache = ScheduleChunks(handle, /*buffer=*/nullptr, requested_start_offset,
	                                                  requested_bytes_to_read, file_size, executor);

	auto &cache_handle = handle.Cast<CacheFileSystemHandle>();
	auto state_for_profile = instance_state.lock();
	auto &collector = GetProfileCollectorOrThrow(state_for_profile, cache_handle.GetConnectionId());
	collector.RecordActualCacheRead(/*cache_size=*/total_bytes_to_cache,
	                                /*actual_bytes=*/requested_bytes_to_read);
}

void DiskCacheReader::ClearCache() {
	auto instance_state_locked = GetInstanceConfigOrThrow(instance_state);
	const auto &config = instance_state_locked->config;
	for (const auto &cur_cache_dir : config.on_disk_cache_directories) {
		local_filesystem->RemoveDirectory(cur_cache_dir);
		// Create an empty directory, otherwise later read access errors.
		local_filesystem->CreateDirectory(cur_cache_dir);
	}
	if (in_mem_storage != nullptr) {
		in_mem_storage->Clear();
	}
}

void DiskCacheReader::ClearCache(const string &fname) {
	// Delete on-disk files.
	vector<string> cache_files_to_remove;
	const string cache_file_prefix = DiskCacheUtil::GetLocalCacheFilePrefix(fname);
	auto instance_state_locked = GetInstanceConfigOrThrow(instance_state);
	const auto &config = instance_state_locked->config;
	for (const auto &cur_cache_dir : config.on_disk_cache_directories) {
		local_filesystem->ListFiles(cur_cache_dir, [&](const string &cur_file, bool /*unused*/) {
			if (StringUtil::StartsWith(cur_file, cache_file_prefix)) {
				string filepath = StringUtil::Format("%s/%s", cur_cache_dir, cur_file);
				cache_files_to_remove.emplace_back(std::move(filepath));
			}
		});
	}

	const auto thread_num = std::min<size_t>(GetCpuCoreCount(), cache_files_to_remove.size());
	auto executor = CreateParallelExecutor(instance_state_locked->db_instance, config.parallel_read_mode, thread_num);
	for (auto cur_cache_file : cache_files_to_remove) {
		executor->Schedule([this, cur = std::move(cur_cache_file)]() { local_filesystem->TryRemoveFile(cur); });
	}
	executor->WaitAll();

	// Delete in-memory cache for on-disk cache files.
	if (in_mem_storage != nullptr) {
		// Start from the first block key for this file (ordered by fname, start_off, blk_size).
		const InMemCacheBlock start_key {fname, /*start_off=*/0, /*blk_size=*/0};
		in_mem_storage->Clear(start_key, [&fname](const InMemCacheBlock &block) { return block.fname == fname; });
	}
}

void DiskCacheReader::RemapInMemoryDataBlocksForNewBlockSize(idx_t new_block_size) {
	if (in_mem_storage == nullptr) {
		return;
	}
	auto taken = in_mem_storage->Take();
	// TODO: pass known remote file sizes (e.g. from metadata cache) so remap matches EOF behavior of real reads.
	auto rebuilt = RemapInMemCacheEntries(std::move(taken), new_block_size, /*file_size_by_path=*/ {});
	for (auto &kv : rebuilt) {
		auto &entry = kv.second;
		in_mem_storage->Put(std::move(kv.first), std::move(entry->data), std::move(entry->version_tag));
	}
}

} // namespace duckdb
