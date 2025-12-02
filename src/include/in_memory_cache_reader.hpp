// A filesystem wrapper, which performs in-memory cache for read operations.

#pragma once

#include "base_cache_reader.hpp"
#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "copiable_value_lru_cache.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "in_mem_cache_block.hpp"
#include "shared_lru_cache.hpp"

namespace duckdb {

class InMemoryCacheReader final : public BaseCacheReader {
public:
	// Constructor accepting cache configuration parameters
	explicit InMemoryCacheReader(idx_t max_cache_block_count = DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT,
	                             idx_t cache_block_timeout_millisec = DEFAULT_IN_MEM_BLOCK_CACHE_TIMEOUT_MILLISEC,
	                             idx_t cache_block_size = DEFAULT_CACHE_BLOCK_SIZE)
	    : max_cache_block_count(max_cache_block_count), cache_block_timeout_millisec(cache_block_timeout_millisec),
	      cache_block_size(cache_block_size) {
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

	// Cache configuration (captured at construction)
	idx_t max_cache_block_count;
	idx_t cache_block_timeout_millisec;
	idx_t cache_block_size;

	// Once flag to guard against cache's initialization.
	std::once_flag cache_init_flag;
	// LRU cache to store blocks; late initialized after first access.
	unique_ptr<InMemCache> cache;
};

} // namespace duckdb
