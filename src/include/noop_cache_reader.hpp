// A noop cache reader, which simply delegates IO request to filesystem API calls.
// - It provides an option for users to disable caching and parallel reads;
// - It eases performance comparison benchmarks.

#pragma once

#include "base_cache_reader.hpp"
#include "base_profile_collector.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

// Forward declaration.
struct CacheHttpfsInstanceState;

class NoopCacheReader : public BaseCacheReader {
public:
	explicit NoopCacheReader(weak_ptr<CacheHttpfsInstanceState> instance_state_p)
	    : BaseCacheReader(std::move(instance_state_p)) {
	}
	virtual ~NoopCacheReader() = default;

	void ClearCache() override {
	}
	void ClearCache(const string &fname) override {
	}
	void ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset, idx_t requested_bytes_to_read,
	                  idx_t file_size) override;

	vector<DataCacheEntryInfo> GetCacheEntriesInfo() const override {
		return {};
	}

	string GetName() const override {
		return "noop_cache_reader";
	}
};

} // namespace duckdb
