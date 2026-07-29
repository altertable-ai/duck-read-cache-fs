#include "file_access_timestamp_throttle.hpp"

#include "time_utils.hpp"

namespace duckdb {

FileAccessTimestampThrottle::FileAccessTimestampThrottle(idx_t interval_ms_p, idx_t max_tracked_paths_p,
                                                         ClockFn clock_fn_p)
    : interval_ms(interval_ms_p), max_tracked_paths(max_tracked_paths_p),
      clock_fn(clock_fn_p ? std::move(clock_fn_p) : ClockFn {[]() {
	      return GetSteadyNowMilliSecSinceEpoch();
      }}) {
}

bool FileAccessTimestampThrottle::ShouldTouch(const string &filepath) {
	const int64_t now_ms = clock_fn();
	const concurrency::lock_guard<concurrency::mutex> lck(mutex);
	auto iter = last_touch_ms.find(filepath);
	if (iter != last_touch_ms.end() && now_ms - iter->second < static_cast<int64_t>(interval_ms)) {
		return false;
	}

	if (iter == last_touch_ms.end() && last_touch_ms.size() >= max_tracked_paths) {
		// Overflow may permit extra touches, but never grows without bound.
		last_touch_ms.clear();
	}
	last_touch_ms[filepath] = now_ms;
	return true;
}

idx_t FileAccessTimestampThrottle::TrackedPathCount() const {
	const concurrency::lock_guard<concurrency::mutex> lck(mutex);
	return last_touch_ms.size();
}

void FileAccessTimestampThrottle::Clear() {
	const concurrency::lock_guard<concurrency::mutex> lck(mutex);
	last_touch_ms.clear();
}

} // namespace duckdb
