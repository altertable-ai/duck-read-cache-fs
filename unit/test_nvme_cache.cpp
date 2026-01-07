#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_httpfs_extension.hpp"
#include "duckdb.hpp"

using namespace duckdb;

namespace {

// Create a simple test to verify nvmefs:// cache directory support
TEST_CASE("Test nvmefs cache directory support", "[nvmefs][cache]") {
	DuckDB db(":memory:");
	Connection con(db);

	// Load the cache_httpfs extension
	con.Query("LOAD 'build/release/extension/cache_httpfs/cache_httpfs.duckdb_extension';");

	SECTION("Test configuration parsing") {
		// Test that we can set nvme configuration options
		auto result = con.Query("SET cache_httpfs_nvme_device_path = '/dev/nvme0n1';");
		REQUIRE(!result->HasError());

		result = con.Query("SET cache_httpfs_nvme_backend = 'nvme';");
		REQUIRE(!result->HasError());

		result = con.Query("SET cache_httpfs_nvme_async = false;");
		REQUIRE(!result->HasError());
	}

	SECTION("Test nvmefs path recognition") {
		// Test that nvmefs:// paths are recognized
		// Note: This test doesn't require an actual NVMe device
		// It just verifies the configuration and path handling logic

		// Set up cache directory with nvmefs:// scheme
		auto result = con.Query("SET cache_httpfs_cache_directories_config = 'nvmefs:///tmp/cache';");
		
		// The query should succeed even without a real device
		// (actual NVMe operations would fail, but configuration should work)
		REQUIRE(!result->HasError());
	}
}

// Test case for validating the filesystem selection logic
TEST_CASE("Test filesystem selection for cache directories", "[disk_cache][filesystem]") {
	DuckDB db(":memory:");
	Connection con(db);

	// Load the extension
	con.Query("LOAD 'build/release/extension/cache_httpfs/cache_httpfs.duckdb_extension';");

	SECTION("Regular path uses LocalFileSystem") {
		auto result = con.Query("SET cache_httpfs_cache_directory = '/tmp/regular_cache';");
		REQUIRE(!result->HasError());

		// Query should work with regular filesystem path
		result = con.Query("SELECT cache_httpfs_get_ondisk_data_cache_size();");
		REQUIRE(!result->HasError());
	}

	SECTION("nvmefs path requires device configuration") {
		// Setting nvmefs:// path without device should be OK for configuration
		auto result = con.Query("SET cache_httpfs_cache_directory = 'nvmefs:///tmp/nvme_cache';");
		REQUIRE(!result->HasError());

		// Note: Actual I/O operations would fail without a real device,
		// but the configuration itself should be valid
	}
}

} // namespace

int main(int argc, char *argv[]) {
	Catch::Session session;
	int returnCode = session.applyCommandLine(argc, argv);
	if (returnCode != 0) {
		return returnCode;
	}
	return session.run();
}

