// Unit tests for FileAccessTimestampThrottle.

#include "catch/catch.hpp"

#include "file_access_timestamp_throttle.hpp"

#include <atomic>
#include <thread>
#include <vector>

using namespace duckdb; // NOLINT

TEST_CASE("FileAccessTimestampThrottle allows first touch and suppresses within interval",
          "[file_access_timestamp_throttle]") {
	int64_t now_ms = 1'000;
	FileAccessTimestampThrottle throttle(/*interval_ms=*/60'000, /*max_tracked_paths=*/8,
	                                     /*clock_fn=*/[&now_ms]() { return now_ms; });

	REQUIRE(throttle.ShouldTouch("/tmp/a"));
	REQUIRE_FALSE(throttle.ShouldTouch("/tmp/a"));
	REQUIRE(throttle.TrackedPathCount() == 1);

	now_ms += 59'999;
	REQUIRE_FALSE(throttle.ShouldTouch("/tmp/a"));

	now_ms += 1;
	REQUIRE(throttle.ShouldTouch("/tmp/a"));
	REQUIRE_FALSE(throttle.ShouldTouch("/tmp/a"));
}

TEST_CASE("FileAccessTimestampThrottle tracks paths independently", "[file_access_timestamp_throttle]") {
	int64_t now_ms = 0;
	FileAccessTimestampThrottle throttle(/*interval_ms=*/1'000, /*max_tracked_paths=*/8,
	                                     /*clock_fn=*/[&now_ms]() { return now_ms; });

	REQUIRE(throttle.ShouldTouch("/tmp/a"));
	REQUIRE(throttle.ShouldTouch("/tmp/b"));
	REQUIRE_FALSE(throttle.ShouldTouch("/tmp/a"));
	REQUIRE_FALSE(throttle.ShouldTouch("/tmp/b"));
	REQUIRE(throttle.TrackedPathCount() == 2);
}

TEST_CASE("FileAccessTimestampThrottle clears on overflow and permits extra touches",
          "[file_access_timestamp_throttle]") {
	int64_t now_ms = 0;
	FileAccessTimestampThrottle throttle(/*interval_ms=*/60'000, /*max_tracked_paths=*/2,
	                                     /*clock_fn=*/[&now_ms]() { return now_ms; });

	REQUIRE(throttle.ShouldTouch("/tmp/a"));
	REQUIRE(throttle.ShouldTouch("/tmp/b"));
	REQUIRE(throttle.TrackedPathCount() == 2);

	// Overflow clears the map, so a new path may touch and previously tracked paths may touch again.
	REQUIRE(throttle.ShouldTouch("/tmp/c"));
	REQUIRE(throttle.TrackedPathCount() == 1);
	REQUIRE(throttle.ShouldTouch("/tmp/a"));
	REQUIRE(throttle.TrackedPathCount() == 2);
}

TEST_CASE("FileAccessTimestampThrottle is safe under concurrent access", "[file_access_timestamp_throttle]") {
	std::atomic<int64_t> now_ms {0};
	FileAccessTimestampThrottle throttle(/*interval_ms=*/60'000, /*max_tracked_paths=*/65'536,
	                                     /*clock_fn=*/[&now_ms]() { return now_ms.load(); });

	constexpr int THREAD_COUNT = 8;
	constexpr int ITERATIONS = 200;
	std::vector<std::thread> threads;
	std::atomic<int> touch_count {0};

	for (int t = 0; t < THREAD_COUNT; ++t) {
		threads.emplace_back([&]() {
			for (int i = 0; i < ITERATIONS; ++i) {
				if (throttle.ShouldTouch("/tmp/shared")) {
					touch_count.fetch_add(1);
				}
			}
		});
	}
	for (auto &thread : threads) {
		thread.join();
	}

	// First concurrent wave should permit exactly one touch while within the interval.
	REQUIRE(touch_count.load() == 1);
	REQUIRE(throttle.TrackedPathCount() == 1);

	now_ms.store(60'000);
	REQUIRE(throttle.ShouldTouch("/tmp/shared"));
}
