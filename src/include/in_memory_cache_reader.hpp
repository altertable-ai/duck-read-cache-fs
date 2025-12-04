// A filesystem wrapper, which performs in-memory cache for read operations.

#pragma once

#include "base_cache_reader.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "in_mem_cache_block.hpp"
#include "shared_lru_cache.hpp"

namespace duckdb {

// Forward declaration.
class DatabaseInstance;

class InMemoryCacheReader final : public BaseCacheReader {
public:
	// Constructor: config values are read from instance state at runtime (with defaults as fallback).
	explicit InMemoryCacheReader(optional_ptr<DatabaseInstance> duckdb_instance_p = nullptr)
	    : duckdb_instance(duckdb_instance_p) {
	}
	~InMemoryCacheReader() override = default;

	std::string GetName() const override {
		return "in_mem_cache_reader";
	}

	void ClearCache() override;
	void ClearCache(const string &fname) override;
	void ReadAndCache(FileHandle &handle, char *buffer, uint64_t requested_start_offset,
	                  uint64_t requested_bytes_to_read, uint64_t file_size) override;
	vector<DataCacheEntryInfo> GetCacheEntriesInfo() const override;

private:
	using InMemCache = ThreadSafeSharedLruCache<InMemCacheBlock, string, InMemCacheBlockHash, InMemCacheBlockEqual>;

	// Duckdb instance for config lookup.
	optional_ptr<DatabaseInstance> duckdb_instance;

	// Once flag to guard against cache's initialization.
	std::once_flag cache_init_flag;
	// LRU cache to store blocks; late initialized after first access.
	unique_ptr<InMemCache> cache;
};

} // namespace duckdb
