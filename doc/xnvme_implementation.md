# xNVMe-backed On-Disk Cache Implementation

This implementation adds xNVMe support to the duck-read-cache-fs extension, allowing cache files to be stored directly on NVMe devices using the xNVMe library, bypassing the POSIX file system layer.

## Implementation Summary

### 1. Vendored nvmefs Backend

Created a minimal nvmefs subsystem under `src/nvmefs/` containing:

- **`device.hpp/cpp`**: Base `Device` class with read/write interface
- **`nvme_device.hpp/cpp`**: `NvmeDevice` implementation using xNVMe library
  - Synchronous `xnvme_nvm_read` and `xnvme_nvm_write` operations
  - Asynchronous I/O support (compiled but not enabled by default)
  - Device geometry and LBA management
  - FDP (Flexible Data Placement) support
- **`nvmefs.hpp/cpp`**: `NvmeFileSystem` implementing DuckDB's `FileSystem` interface
  - Routes DuckDB file operations to xNVMe device operations
  - **`MoveFile` support** for atomic cache file writes (renames metadata entry)
- **`nvmefs_temporary_block_manager.hpp/cpp`**: LBA block allocation
- **`temporary_file_metadata_manager.hpp/cpp`**: Maps filenames to LBA ranges

### 2. Extension Configuration

Added four new extension options in `cache_httpfs_extension.cpp`:

```sql
SET cache_httpfs_nvme_device_path = '/dev/nvme0n1';  -- Required to enable
SET cache_httpfs_nvme_backend = 'nvme';              -- xNVMe backend
SET cache_httpfs_nvme_async = false;                 -- Sync (default) or async I/O
SET cache_httpfs_nvme_max_threads = 8;               -- Threads for async I/O
```

When `cache_httpfs_nvme_device_path` is set, the extension:
1. Creates an `NvmeFileSystem` instance
2. Registers it with the DuckDB `VirtualFileSystem`
3. Makes `nvmefs://` URIs available throughout DuckDB

### 3. Cache Directory Scheme Selection

Updated `DiskCacheReader` to support both regular and nvmefs paths:

**New Method**: `GetFileSystemForCacheDirectory(const string &cache_directory)`
- Returns `LocalFileSystem` for regular paths
- Returns `VirtualFileSystem` (with nvmefs support) for `nvmefs://` paths

**Modified Methods**:
- `CacheLocal()`: Uses appropriate filesystem based on cache directory scheme
- `ReadAndCache()`: Detects scheme and routes to correct filesystem
- `ClearCache()`: Works with both regular and nvmefs directories
- `GetCacheEntriesInfo()`: Lists files from both filesystem types

**Key Design**: 
- Cache files can use mixed schemes (e.g., `/tmp/cache1;nvmefs:///tmp/cache2`)
- Each cache directory is evaluated independently
- Atomic write safety preserved via `MoveFile` for both backends

### 4. Cache File Safety

The `write-temp-then-move` pattern is preserved for both backends:

**POSIX (LocalFileSystem)**:
- Writes to temp file with UUID name
- Atomically renames via `rename(2)` syscall

**nvmefs:// (NvmeFileSystem)**:
- Writes to temp metadata entry
- `MoveFile` atomically renames metadata entry under lock
- No filesystem rename needed - only in-memory metadata update

### 5. Cache Validation

- **POSIX**: Supports extended attributes for version tags
- **nvmefs://**: Extended attributes not supported; validation disabled by default
- Future: Could add version header to cache blocks

## Architecture

```
CacheFileSystem::ReadImpl
    ↓
DiskCacheReader::ReadAndCache
    ↓
DiskCacheReader::GetFileSystemForCacheDirectory(cache_dir)
    ↓
    ├─ "nvmefs://" → VirtualFileSystem → NvmeFileSystem → NvmeDevice → xnvme_nvm_read
    └─ regular path → LocalFileSystem → open/read syscalls
```

## Build Integration

Updated `CMakeLists.txt` to:
1. Include `src/nvmefs/include` in include paths
2. Add all nvmefs source files to `EXTENSION_SOURCES`
3. Find and link `libxnvme` via pkg-config or direct link

## Testing

Created tests in:
- `unit/test_nvme_cache.cpp`: C++ unit tests (configuration and integration)
- `test/sql/test_nvme_cache_config.test`: SQL test script
- `test/sql/README_NVME_TESTS.md`: Testing guide

Tests verify:
- Configuration options are accepted
- `nvmefs://` paths are recognized
- Filesystem selection works correctly
- Cache operations succeed (with device configured)

## Usage Example

```sql
-- Load extension
LOAD 'cache_httpfs.duckdb_extension';

-- Configure NVMe device
SET cache_httpfs_nvme_device_path = '/dev/nvme0n1';
SET cache_httpfs_nvme_backend = 'nvme';

-- Set cache directory to use NVMe
SET cache_httpfs_cache_directory = 'nvmefs:///tmp/cache';

-- Or use multiple directories with mixed schemes
SET cache_httpfs_cache_directories_config = '/tmp/posix_cache;nvmefs:///tmp/nvme_cache';

-- Now queries will cache to NVMe device
SELECT * FROM read_parquet('s3://bucket/data.parquet');
```

## Performance Characteristics

**Expected benefits** (based on the paper):
- Lower latency for cache hits (direct device access)
- Higher throughput with async I/O enabled
- Better thread scalability with thread-owned queues
- Reduced CPU overhead (no kernel context switches)

**Trade-offs**:
- Requires xNVMe library and permissions
- No filesystem metadata (size, timestamps, etc.)
- Cache validation via extended attributes not available

## Future Work

1. **Enable async I/O**: Set `cache_httpfs_nvme_async=true` to use thread-owned queues
2. **Add version headers**: Store cache validity info in block headers
3. **Benchmark suite**: Compare POSIX vs xNVMe cache performance
4. **Multi-device support**: Distribute cache across multiple NVMe devices
5. **WAL support**: Extend nvmefs to support DuckDB's WAL on NVMe

## Files Modified

### Core Implementation
- `src/cache_httpfs_extension.cpp`: Added nvme configuration and filesystem registration
- `src/include/cache_httpfs_instance_state.hpp`: Added nvme config fields and db_instance pointer
- `src/include/disk_cache_reader.hpp`: Added `GetFileSystemForCacheDirectory` method
- `src/disk_cache_reader.cpp`: Updated all filesystem operations to support both backends
- `CMakeLists.txt`: Added nvmefs sources and xnvme linking

### New Files (nvmefs subsystem)
- `src/nvmefs/include/device.hpp`
- `src/nvmefs/device.cpp`
- `src/nvmefs/include/nvme_device.hpp`
- `src/nvmefs/nvme_device.cpp`
- `src/nvmefs/include/nvmefs.hpp`
- `src/nvmefs/nvmefs.cpp`
- `src/nvmefs/include/nvmefs_temporary_block_manager.hpp`
- `src/nvmefs/nvmefs_temporary_block_manager.cpp`
- `src/nvmefs/include/temporary_file_metadata_manager.hpp`
- `src/nvmefs/temporary_file_metadata_manager.cpp`

### Tests
- `unit/test_nvme_cache.cpp`
- `test/sql/test_nvme_cache_config.test`
- `test/sql/README_NVME_TESTS.md`

## References

- Paper: "DuckDB on xNVMe" (file://2512.01490v1.pdf)
- xNVMe Library: https://xnvme.io/
- Original nvmefs implementation: `/Users/redox/dev/itu-rad/nvmefs/`

