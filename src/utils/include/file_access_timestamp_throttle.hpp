// Bounded per-path throttle for cache-file recency (mtime) updates.
// Limits filesystem metadata writes without affecting read correctness.

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "mutex.hpp"
#include "thread_annotation.hpp"

#include <functional>

namespace duckdb {

class FileAccessTimestampThrottle {
public:
	static constexpr idx_t DEFAULT_INTERVAL_MS = 60'000;
	static constexpr idx_t DEFAULT_MAX_TRACKED_PATHS = 65'536;

	using ClockFn = std::function<int64_t()>;

	explicit FileAccessTimestampThrottle(idx_t interval_ms = DEFAULT_INTERVAL_MS,
	                                     idx_t max_tracked_paths = DEFAULT_MAX_TRACKED_PATHS,
	                                     ClockFn clock_fn = nullptr);

	// Returns true if the caller should update timestamps for [filepath].
	bool ShouldTouch(const string &filepath);

	// Test helpers.
	idx_t TrackedPathCount() const;
	void Clear();

private:
	const idx_t interval_ms;
	const idx_t max_tracked_paths;
	ClockFn clock_fn;
	mutable concurrency::mutex mutex;
	unordered_map<string, int64_t> last_touch_ms DUCKDB_GUARDED_BY(mutex);
};

} // namespace duckdb
