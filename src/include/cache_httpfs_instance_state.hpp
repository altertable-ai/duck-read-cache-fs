// Per-instance state for cache_httpfs extension.
// State is stored in DuckDB's ObjectCache for automatic cleanup when DatabaseInstance is destroyed.

#pragma once

#include <mutex>

#include "base_cache_reader.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

// Forward declarations
class CacheFileSystem;
class DatabaseInstance;
class FileOpener;

//===--------------------------------------------------------------------===//
// Per-instance filesystem registry (replaces global CacheFsRefRegistry)
//===--------------------------------------------------------------------===//
class InstanceCacheFsRegistry {
public:
	void Register(CacheFileSystem *fs);
	void Unregister(CacheFileSystem *fs);
	vector<CacheFileSystem *> GetAllCacheFs() const;
	void Reset();

private:
	mutable std::mutex mutex;
	vector<CacheFileSystem *> cache_filesystems;
};

//===--------------------------------------------------------------------===//
// Per-instance cache reader manager (replaces global CacheReaderManager)
//===--------------------------------------------------------------------===//

// Forward declaration
struct InstanceConfig;

class InstanceCacheReaderManager {
public:
	void SetCacheReader(const InstanceConfig &config, optional_ptr<DatabaseInstance> instance);
	BaseCacheReader *GetCacheReader() const;
	vector<BaseCacheReader *> GetCacheReaders() const;
	void InitializeDiskCacheReader(const vector<string> &cache_directories);
	void ClearCache();
	void ClearCache(const string &fname);
	void Reset();

private:
	mutable std::mutex mutex;
	unique_ptr<BaseCacheReader> noop_cache_reader;
	unique_ptr<BaseCacheReader> in_mem_cache_reader;
	unique_ptr<BaseCacheReader> on_disk_cache_reader;
	BaseCacheReader *internal_cache_reader = nullptr;
};

//===--------------------------------------------------------------------===//
// Per-instance configuration (replaces global g_* variables)
//===--------------------------------------------------------------------===//
struct InstanceConfig {
	// General config
	idx_t cache_block_size;
	string cache_type;
	string profile_type;
	uint64_t max_subrequest_count;
	bool ignore_sigpipe;

	// On-disk cache config
	vector<string> on_disk_cache_directories;
	idx_t min_disk_bytes_for_cache;
	string on_disk_eviction_policy;

	// Disk reader in-memory cache config
	bool enable_disk_reader_mem_cache;
	idx_t disk_reader_max_mem_cache_block_count;
	idx_t disk_reader_max_mem_cache_timeout_millisec;

	// In-memory cache config
	idx_t max_in_mem_cache_block_count;
	idx_t in_mem_cache_block_timeout_millisec;

	// Metadata cache config
	bool enable_metadata_cache;
	idx_t max_metadata_cache_entry;
	idx_t metadata_cache_entry_timeout_millisec;

	// File handle cache config
	bool enable_file_handle_cache;
	idx_t max_file_handle_cache_entry;
	idx_t file_handle_cache_entry_timeout_millisec;

	// Glob cache config
	bool enable_glob_cache;
	idx_t max_glob_cache_entry;
	idx_t glob_cache_entry_timeout_millisec;

	// Testing config
	string test_cache_type;
	bool test_insufficient_disk_space;

	// Initialize with defaults
	void SetDefaults();

	// Update from FileOpener settings
	void UpdateFromOpener(optional_ptr<FileOpener> opener);
};

//===--------------------------------------------------------------------===//
// Main per-instance state container
// Inherits from ObjectCacheEntry for automatic cleanup when DatabaseInstance is destroyed
//===--------------------------------------------------------------------===//
struct CacheHttpfsInstanceState : public ObjectCacheEntry {
	static constexpr const char *OBJECT_TYPE = "CacheHttpfsInstanceState";
	static constexpr const char *CACHE_KEY = "cache_httpfs_instance_state";

	InstanceCacheFsRegistry registry;
	InstanceCacheReaderManager cache_reader_manager;
	InstanceConfig config;

	// Initialize with defaults
	CacheHttpfsInstanceState() {
		config.SetDefaults();
	}

	// ObjectCacheEntry interface
	string GetObjectType() override {
		return OBJECT_TYPE;
	}

	static string ObjectType() {
		return OBJECT_TYPE;
	}
};

//===--------------------------------------------------------------------===//
// Helper functions to access instance state
//===--------------------------------------------------------------------===//

// Store instance state in DatabaseInstance
void SetInstanceState(DatabaseInstance &instance, shared_ptr<CacheHttpfsInstanceState> state);

// Get instance state from DatabaseInstance (returns nullptr if not set)
CacheHttpfsInstanceState *GetInstanceState(DatabaseInstance &instance);

// Get instance state, throwing if not found
CacheHttpfsInstanceState &GetInstanceStateOrThrow(DatabaseInstance &instance);

} // namespace duckdb
