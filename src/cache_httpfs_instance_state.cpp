#include "cache_httpfs_instance_state.hpp"

#include <algorithm>

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/main/database.hpp"
#include "in_memory_cache_reader.hpp"
#include "noop_cache_reader.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// InstanceCacheFsRegistry implementation
//===--------------------------------------------------------------------===//

void InstanceCacheFsRegistry::Register(CacheFileSystem *fs) {
	std::lock_guard<std::mutex> lock(mutex);
	cache_filesystems.emplace_back(fs);
}

void InstanceCacheFsRegistry::Unregister(CacheFileSystem *fs) {
	std::lock_guard<std::mutex> lock(mutex);
	auto it = std::find(cache_filesystems.begin(), cache_filesystems.end(), fs);
	if (it != cache_filesystems.end()) {
		cache_filesystems.erase(it);
	}
}

vector<CacheFileSystem *> InstanceCacheFsRegistry::GetAllCacheFs() const {
	std::lock_guard<std::mutex> lock(mutex);
	return cache_filesystems;
}

void InstanceCacheFsRegistry::Reset() {
	std::lock_guard<std::mutex> lock(mutex);
	cache_filesystems.clear();
}

//===--------------------------------------------------------------------===//
// InstanceCacheReaderManager implementation
//===--------------------------------------------------------------------===//

void InstanceCacheReaderManager::SetCacheReader(const InstanceConfig &config, optional_ptr<DatabaseInstance> instance) {
	std::lock_guard<std::mutex> lock(mutex);

	if (config.cache_type == *NOOP_CACHE_TYPE) {
		if (noop_cache_reader == nullptr) {
			noop_cache_reader = make_uniq<NoopCacheReader>();
		}
		internal_cache_reader = noop_cache_reader.get();
		return;
	}

	if (config.cache_type == *ON_DISK_CACHE_TYPE) {
		if (on_disk_cache_reader == nullptr) {
			on_disk_cache_reader = make_uniq<DiskCacheReader>(config.on_disk_cache_directories, instance);
		}
		internal_cache_reader = on_disk_cache_reader.get();
		return;
	}

	if (config.cache_type == *IN_MEM_CACHE_TYPE) {
		if (in_mem_cache_reader == nullptr) {
			in_mem_cache_reader = make_uniq<InMemoryCacheReader>(instance);
		}
		internal_cache_reader = in_mem_cache_reader.get();
		return;
	}
}

BaseCacheReader *InstanceCacheReaderManager::GetCacheReader() const {
	std::lock_guard<std::mutex> lock(mutex);
	return internal_cache_reader;
}

vector<BaseCacheReader *> InstanceCacheReaderManager::GetCacheReaders() const {
	std::lock_guard<std::mutex> lock(mutex);
	vector<BaseCacheReader *> result;
	if (in_mem_cache_reader != nullptr) {
		result.emplace_back(in_mem_cache_reader.get());
	}
	if (on_disk_cache_reader != nullptr) {
		result.emplace_back(on_disk_cache_reader.get());
	}
	return result;
}

void InstanceCacheReaderManager::InitializeDiskCacheReader(const vector<string> &cache_directories) {
	std::lock_guard<std::mutex> lock(mutex);
	if (on_disk_cache_reader == nullptr) {
		on_disk_cache_reader = make_uniq<DiskCacheReader>(cache_directories, nullptr);
	}
}

void InstanceCacheReaderManager::ClearCache() {
	std::lock_guard<std::mutex> lock(mutex);
	if (noop_cache_reader != nullptr) {
		noop_cache_reader->ClearCache();
	}
	if (in_mem_cache_reader != nullptr) {
		in_mem_cache_reader->ClearCache();
	}
	if (on_disk_cache_reader != nullptr) {
		on_disk_cache_reader->ClearCache();
	}
}

void InstanceCacheReaderManager::ClearCache(const string &fname) {
	std::lock_guard<std::mutex> lock(mutex);
	if (noop_cache_reader != nullptr) {
		noop_cache_reader->ClearCache(fname);
	}
	if (in_mem_cache_reader != nullptr) {
		in_mem_cache_reader->ClearCache(fname);
	}
	if (on_disk_cache_reader != nullptr) {
		on_disk_cache_reader->ClearCache(fname);
	}
}

void InstanceCacheReaderManager::Reset() {
	std::lock_guard<std::mutex> lock(mutex);
	noop_cache_reader.reset();
	in_mem_cache_reader.reset();
	on_disk_cache_reader.reset();
	internal_cache_reader = nullptr;
}

//===--------------------------------------------------------------------===//
// InstanceConfig implementation
//===--------------------------------------------------------------------===//

void InstanceConfig::SetDefaults() {
	cache_block_size = DEFAULT_CACHE_BLOCK_SIZE;
	cache_type = *DEFAULT_CACHE_TYPE;
	profile_type = *DEFAULT_PROFILE_TYPE;
	max_subrequest_count = DEFAULT_MAX_SUBREQUEST_COUNT;
	ignore_sigpipe = DEFAULT_IGNORE_SIGPIPE;

	on_disk_cache_directories = {*DEFAULT_ON_DISK_CACHE_DIRECTORY};
	min_disk_bytes_for_cache = DEFAULT_MIN_DISK_BYTES_FOR_CACHE;
	on_disk_eviction_policy = *DEFAULT_ON_DISK_EVICTION_POLICY;

	enable_disk_reader_mem_cache = DEFAULT_ENABLE_DISK_READER_MEM_CACHE;
	disk_reader_max_mem_cache_block_count = DEFAULT_MAX_DISK_READER_MEM_CACHE_BLOCK_COUNT;
	disk_reader_max_mem_cache_timeout_millisec = DEFAULT_DISK_READER_MEM_CACHE_TIMEOUT_MILLISEC;

	max_in_mem_cache_block_count = DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT;
	in_mem_cache_block_timeout_millisec = DEFAULT_IN_MEM_BLOCK_CACHE_TIMEOUT_MILLISEC;

	enable_metadata_cache = DEFAULT_ENABLE_METADATA_CACHE;
	max_metadata_cache_entry = DEFAULT_MAX_METADATA_CACHE_ENTRY;
	metadata_cache_entry_timeout_millisec = DEFAULT_METADATA_CACHE_ENTRY_TIMEOUT_MILLISEC;

	enable_file_handle_cache = DEFAULT_ENABLE_FILE_HANDLE_CACHE;
	max_file_handle_cache_entry = DEFAULT_MAX_FILE_HANDLE_CACHE_ENTRY;
	file_handle_cache_entry_timeout_millisec = DEFAULT_FILE_HANDLE_CACHE_ENTRY_TIMEOUT_MILLISEC;

	enable_glob_cache = DEFAULT_ENABLE_GLOB_CACHE;
	max_glob_cache_entry = DEFAULT_MAX_GLOB_CACHE_ENTRY;
	glob_cache_entry_timeout_millisec = DEFAULT_GLOB_CACHE_ENTRY_TIMEOUT_MILLISEC;

	test_cache_type = "";
	test_insufficient_disk_space = false;
}

void InstanceConfig::UpdateFromOpener(optional_ptr<FileOpener> opener) {
	if (opener == nullptr) {
		// Apply test_cache_type override if set
		if (!test_cache_type.empty()) {
			cache_type = test_cache_type;
		}
		// Ensure cache directories exist
		auto local_fs = LocalFileSystem::CreateLocal();
		for (const auto &dir : on_disk_cache_directories) {
			local_fs->CreateDirectory(dir);
		}
		return;
	}

	Value val;

	// Cache type
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_type", val);
	auto cache_type_str = val.ToString();
	if (ALL_CACHE_TYPES.find(cache_type_str) != ALL_CACHE_TYPES.end()) {
		cache_type = std::move(cache_type_str);
	}

	// Test cache type override
	if (!test_cache_type.empty()) {
		cache_type = test_cache_type;
	}

	// Cache directories
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_cache_directory", val);
	auto cache_dir = val.ToString();
	if (!cache_dir.empty()) {
		on_disk_cache_directories = {cache_dir};
	}

	// Ensure directories exist
	auto local_fs = LocalFileSystem::CreateLocal();
	for (const auto &dir : on_disk_cache_directories) {
		local_fs->CreateDirectory(dir);
	}

	// Block size
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_cache_block_size", val);
	auto block_size_str = val.ToString();
	if (!block_size_str.empty()) {
		cache_block_size = std::stoull(block_size_str);
	}

	// Profile type
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_profile_type", val);
	auto profile_type_str = val.ToString();
	if (ALL_PROFILE_TYPES->find(profile_type_str) != ALL_PROFILE_TYPES->end()) {
		profile_type = std::move(profile_type_str);
	}

	// Additional settings can be added here following the same pattern
}

//===--------------------------------------------------------------------===//
// Instance state storage/retrieval
//===--------------------------------------------------------------------===//

// Global map to store instance states - using a map from DatabaseInstance* to state
static std::mutex &GetStateMapMutex() {
	static std::mutex mutex;
	return mutex;
}

static std::unordered_map<DatabaseInstance *, shared_ptr<CacheHttpfsInstanceState>> &GetStateMap() {
	static std::unordered_map<DatabaseInstance *, shared_ptr<CacheHttpfsInstanceState>> state_map;
	return state_map;
}

void SetInstanceState(DatabaseInstance &instance, shared_ptr<CacheHttpfsInstanceState> state) {
	std::lock_guard<std::mutex> lock(GetStateMapMutex());
	GetStateMap()[&instance] = std::move(state);
}

CacheHttpfsInstanceState *GetInstanceState(DatabaseInstance &instance) {
	std::lock_guard<std::mutex> lock(GetStateMapMutex());
	auto &state_map = GetStateMap();
	auto it = state_map.find(&instance);
	if (it == state_map.end()) {
		return nullptr;
	}
	return it->second.get();
}

CacheHttpfsInstanceState &GetInstanceStateOrThrow(DatabaseInstance &instance) {
	auto *state = GetInstanceState(instance);
	if (state == nullptr) {
		throw InternalException("cache_httpfs instance state not found - extension not properly loaded");
	}
	return *state;
}

} // namespace duckdb
