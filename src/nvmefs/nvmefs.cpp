#include "include/nvmefs.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

NvmeFileHandle::NvmeFileHandle(FileSystem &file_system, string path, FileOpenFlags flags)
    : FileHandle(file_system, path, flags), cursor_offset(0) {
}

void NvmeFileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Read(*this, buffer, nr_bytes, location);
}

void NvmeFileHandle::Write(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Write(*this, buffer, nr_bytes, location);
}

idx_t NvmeFileHandle::GetFileSize() {
	return file_system.GetFileSize(*this);
}

void NvmeFileHandle::Sync() {
	file_system.FileSync(*this);
}

void NvmeFileHandle::Close() {
}

unique_ptr<CmdContext> NvmeFileHandle::PrepareWriteCommand(idx_t nr_bytes, idx_t start_lba, idx_t offset) {
	unique_ptr<NvmeCmdContext> nvme_cmd_ctx = make_uniq<NvmeCmdContext>();
	nvme_cmd_ctx->nr_bytes = nr_bytes;
	nvme_cmd_ctx->filepath = path;
	nvme_cmd_ctx->offset = offset;
	nvme_cmd_ctx->start_lba = start_lba;
	nvme_cmd_ctx->nr_lbas = CalculateRequiredLBACount(nr_bytes);

	return std::move(nvme_cmd_ctx);
}

unique_ptr<CmdContext> NvmeFileHandle::PrepareReadCommand(idx_t nr_bytes, idx_t start_lba, idx_t offset) {
	unique_ptr<NvmeCmdContext> nvme_cmd_ctx = make_uniq<NvmeCmdContext>();
	nvme_cmd_ctx->nr_bytes = nr_bytes;
	nvme_cmd_ctx->filepath = path;
	nvme_cmd_ctx->offset = offset;
	nvme_cmd_ctx->start_lba = start_lba;
	nvme_cmd_ctx->nr_lbas = CalculateRequiredLBACount(nr_bytes);

	return std::move(nvme_cmd_ctx);
}

idx_t NvmeFileHandle::CalculateRequiredLBACount(idx_t nr_bytes) {
	NvmeFileSystem &nvmefs = file_system.Cast<NvmeFileSystem>();
	DeviceGeometry geo = nvmefs.GetDevice().GetDeviceGeometry();
	idx_t lba_size = geo.lba_size;
	return (nr_bytes + lba_size - 1) / lba_size;
}

void NvmeFileHandle::SetFilePointer(idx_t location) {
	cursor_offset = location;
}

idx_t NvmeFileHandle::GetFilePointer() {
	return cursor_offset;
}

////////////////////////////////////////

NvmeFileSystem::NvmeFileSystem(const string &device_path, const string &backend, bool async, idx_t max_threads)
    : allocator(Allocator::DefaultAllocator()),
      device(make_uniq<NvmeDevice>(device_path, backend, async, max_threads)) {
	DeviceGeometry geo = device->GetDeviceGeometry();
	// Use the entire device for temporary files (cache files)
	temp_meta_manager = make_uniq<TemporaryFileMetadataManager>(0, geo.lba_count, geo.lba_size);
}

NvmeFileSystem::~NvmeFileSystem() {
}

unique_ptr<FileHandle> NvmeFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                optional_ptr<FileOpener> opener) {
	// Ensure temp file metadata exists if creating
	if (flags.CreateFileIfNotExists()) {
		temp_meta_manager->CreateFile(path);
	}

	unique_ptr<FileHandle> handle = make_uniq<NvmeFileHandle>(*this, path, flags);
	return std::move(handle);
}

void NvmeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	NvmeFileHandle &fh = handle.Cast<NvmeFileHandle>();
	DeviceGeometry geo = device->GetDeviceGeometry();

	idx_t cursor_offset = SeekPosition(handle);
	location += cursor_offset;
	idx_t nr_lbas = fh.CalculateRequiredLBACount(nr_bytes);
	idx_t start_lba = GetLBA(handle.path, nr_bytes, location, nr_lbas);
	idx_t in_block_offset = location % geo.lba_size;
	unique_ptr<CmdContext> cmd_ctx = fh.PrepareReadCommand(nr_bytes, start_lba, in_block_offset);

	if (!IsLBAInRange(handle.path, start_lba, cmd_ctx->nr_lbas)) {
		throw IOException("Read out of range");
	}

	device->Read(buffer, *cmd_ctx);
}

void NvmeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	NvmeFileHandle &fh = handle.Cast<NvmeFileHandle>();
	DeviceGeometry geo = device->GetDeviceGeometry();

	idx_t cursor_offset = SeekPosition(handle);
	location += cursor_offset;
	idx_t nr_lbas = fh.CalculateRequiredLBACount(nr_bytes);
	idx_t start_lba = GetLBA(fh.path, nr_bytes, location, nr_lbas);
	idx_t in_block_offset = location % geo.lba_size;
	unique_ptr<CmdContext> cmd_ctx = fh.PrepareWriteCommand(nr_bytes, start_lba, in_block_offset);

	if (!IsLBAInRange(handle.path, start_lba, cmd_ctx->nr_lbas)) {
		throw IOException("Write out of range");
	}

	device->Write(buffer, *cmd_ctx);
}

int64_t NvmeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	Read(handle, buffer, nr_bytes, 0);
	return nr_bytes;
}

int64_t NvmeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	Write(handle, buffer, nr_bytes, 0);
	return nr_bytes;
}

bool NvmeFileSystem::CanHandleFile(const string &fpath) {
	return StringUtil::StartsWith(fpath, NVMEFS_PATH_PREFIX);
}

bool NvmeFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	return temp_meta_manager->FileExists(filename);
}

int64_t NvmeFileSystem::GetFileSize(FileHandle &handle) {
	DeviceGeometry geo = device->GetDeviceGeometry();
	NvmeFileHandle &fh = handle.Cast<NvmeFileHandle>();

	idx_t nr_lbas = temp_meta_manager->GetFileSizeLBA(fh.path);
	return nr_lbas * geo.lba_size;
}

void NvmeFileSystem::FileSync(FileHandle &handle) {
	// No need for sync. All writes are directly to disk.
}

bool NvmeFileSystem::OnDiskFile(FileHandle &handle) {
	// No remote accesses to disks. We only interact with physical device, i.e. always disk "files".
	return true;
}

void NvmeFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	NvmeFileHandle &nvme_handle = handle.Cast<NvmeFileHandle>();
	int64_t current_size = GetFileSize(nvme_handle);

	if (new_size <= current_size) {
		temp_meta_manager->TruncateFile(nvme_handle.path, new_size);
	} else {
		throw InvalidInputException("new_size is bigger than the current file size.");
	}
}

bool NvmeFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	return StringUtil::StartsWith(directory, NVMEFS_PATH_PREFIX);
}

void NvmeFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	// Only support removing tmp directory
	if (StringUtil::StartsWith(directory, NVMEFS_TMP_DIR_PATH)) {
		temp_meta_manager->Clear();
	}
}

void NvmeFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	// Directories implicitly exist
}

void NvmeFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	temp_meta_manager->DeleteFile(filename);
}

void NvmeFileSystem::Seek(FileHandle &handle, idx_t location) {
	NvmeFileHandle &nvme_handle = handle.Cast<NvmeFileHandle>();
	nvme_handle.SetFilePointer(location);
}

void NvmeFileSystem::Reset(FileHandle &handle) {
	NvmeFileHandle &fh = handle.Cast<NvmeFileHandle>();
	fh.SetFilePointer(0);
}

idx_t NvmeFileSystem::SeekPosition(FileHandle &handle) {
	return handle.Cast<NvmeFileHandle>().GetFilePointer();
}

bool NvmeFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                               FileOpener *opener) {
	if (StringUtil::StartsWith(directory, NVMEFS_TMP_DIR_PATH) || StringUtil::Equals(directory.c_str(), NVMEFS_PATH_PREFIX)) {
		temp_meta_manager->ListFiles(directory, callback);
		return true;
	}
	return false;
}

bool NvmeFileSystem::Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) {
	data_ptr_t data = allocator.AllocateData(length_bytes);

	memset(data, 0, length_bytes);
	Write(handle, data, length_bytes, offset_bytes);

	allocator.FreeData(data, length_bytes);
	return true;
}

void NvmeFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	// Atomic rename for cache files - just rename the metadata entry
	temp_meta_manager->RenameFile(source, target);
}

Device &NvmeFileSystem::GetDevice() {
	return *device;
}

idx_t NvmeFileSystem::GetLBA(const string &filename, idx_t nr_bytes, idx_t location, idx_t nr_lbas) {
	return temp_meta_manager->GetLBA(filename, location, nr_lbas);
}

bool NvmeFileSystem::IsLBAInRange(const string &filename, idx_t start_lba, idx_t lba_count) {
	DeviceGeometry geo = device->GetDeviceGeometry();
	// Check if LBA range is within device bounds
	return (start_lba + lba_count) <= geo.lba_count;
}

} // namespace duckdb

