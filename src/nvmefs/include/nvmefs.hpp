#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"

#include "device.hpp"
#include "nvme_device.hpp"
#include "temporary_file_metadata_manager.hpp"

namespace duckdb {

constexpr const char *NVMEFS_PATH_PREFIX = "nvmefs://";
constexpr const char *NVMEFS_TMP_DIR_PATH = "nvmefs:///tmp";

class NvmeFileHandle : public FileHandle {
	friend class NvmeFileSystem;

public:
	NvmeFileHandle(FileSystem &file_system, string path, FileOpenFlags flags);
	~NvmeFileHandle() = default;

	void Read(void *buffer, idx_t nr_bytes, idx_t location);
	void Write(void *buffer, idx_t nr_bytes, idx_t location);

	idx_t GetFileSize();
	void Sync();

	void Close() override;

private:
	unique_ptr<CmdContext> PrepareWriteCommand(idx_t nr_bytes, idx_t start_lba, idx_t offset);
	unique_ptr<CmdContext> PrepareReadCommand(idx_t nr_bytes, idx_t start_lba, idx_t offset);

	/// @brief Calculates the amount of LBAs required to store the given number of bytes
	/// @param nr_bytes The number of bytes to store
	/// @return The number of LBAs required to store the given number of bytes
	idx_t CalculateRequiredLBACount(idx_t nr_bytes);

	void SetFilePointer(idx_t location);
	idx_t GetFilePointer();

private:
	idx_t cursor_offset;
};

class NvmeFileSystem : public FileSystem {
public:
	NvmeFileSystem(const string &device_path, const string &backend, bool async, idx_t max_threads);
	~NvmeFileSystem();

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	bool CanHandleFile(const string &fpath) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	int64_t GetFileSize(FileHandle &handle) override;
	void FileSync(FileHandle &handle) override;
	bool OnDiskFile(FileHandle &handle) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void Seek(FileHandle &handle, idx_t location) override;
	void Reset(FileHandle &handle) override;
	idx_t SeekPosition(FileHandle &handle) override;
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override;
	bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) override;

	// Add MoveFile support for cache file atomic writes
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override;

	string GetName() const override {
		return "NvmeFileSystem";
	}

	Device &GetDevice();

private:
	idx_t GetLBA(const string &filename, idx_t nr_bytes, idx_t location, idx_t nr_lbas);
	bool IsLBAInRange(const string &filename, idx_t start_lba, idx_t lba_count);

private:
	Allocator &allocator;
	unique_ptr<Device> device;
	unique_ptr<TemporaryFileMetadataManager> temp_meta_manager;
	mutable std::mutex temp_lock;
};
} // namespace duckdb
