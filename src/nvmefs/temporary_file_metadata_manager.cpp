#include "include/temporary_file_metadata_manager.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// Simplified version for cache files - just allocate blocks as needed
unique_ptr<TempFileMetadata> CreateTempFileMetadata(const string &filename, idx_t lba_size) {
	unique_ptr<TempFileMetadata> tfmeta = make_uniq<TempFileMetadata>();
	tfmeta->is_active.store(true);
	// For cache files, use a standard block size (256KB)
	tfmeta->block_size = 262144;
	tfmeta->file_index = 0;
	tfmeta->nr_blocks = 0;
	tfmeta->lba_location.store(0);
	return std::move(tfmeta);
}

const TempFileMetadata *TemporaryFileMetadataManager::GetOrCreateFile(const string &filename) {
	std::lock_guard<std::mutex> lock(temp_mutex);

	// Check if the file already exists
	if (file_to_temp_meta.count(filename)) {
		return file_to_temp_meta[filename].get();
	}

	// Create a new TempFileMetadata object
	unique_ptr<TempFileMetadata> tfmeta = CreateTempFileMetadata(filename, lba_size);
	tfmeta->is_active.store(true);
	auto [entry, is_new] = file_to_temp_meta.emplace(filename, std::move(tfmeta));

	return file_to_temp_meta[filename].get();
}

void TemporaryFileMetadataManager::CreateFile(const string &filename) {
	GetOrCreateFile(filename);
}

idx_t TemporaryFileMetadataManager::GetLBA(const string &filename, idx_t location, idx_t nr_lbas) {
	std::lock_guard<std::mutex> lock(temp_mutex);

	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();
	std::lock_guard<std::mutex> file_lock(tfmeta->file_mutex);

	idx_t block_index = location / tfmeta->block_size;

	if (nr_lbas != (tfmeta->block_size / lba_size)) {
		throw IOException("Temporary file block size mismatch");
	}

	if (tfmeta->block_map.count(block_index)) {
		return tfmeta->block_map[block_index]->GetStartLBA();
	}

	// Allocate a new block
	TemporaryBlock *block = block_manager->AllocateBlock(nr_lbas);
	tfmeta->block_map[block_index] = block;
	idx_t lba = tfmeta->block_map[block_index]->GetStartLBA();

	return lba;
}

void TemporaryFileMetadataManager::MoveLBALocation(const string &filename, idx_t lba_location) {
	// Not used for cache files currently
}

void TemporaryFileMetadataManager::TruncateFile(const string &filename, idx_t new_size) {
	std::lock_guard<std::mutex> lock(temp_mutex);

	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();
	std::lock_guard<std::mutex> file_lock(tfmeta->file_mutex);

	idx_t to_block_index = new_size / tfmeta->block_size;
	idx_t from_block_index = tfmeta->block_map.size();

	for (idx_t i = from_block_index; i > to_block_index; i--) {
		idx_t block_index = i - 1;
		TemporaryBlock *block = tfmeta->block_map[block_index];
		block_manager->FreeBlock(block);
		tfmeta->block_map.erase(block_index);
	}
}

void TemporaryFileMetadataManager::DeleteFile(const string &filename) {
	std::lock_guard<std::mutex> lock(temp_mutex);

	if (!file_to_temp_meta.count(filename)) {
		return;
	}

	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();
	{
		std::lock_guard<std::mutex> file_lock(tfmeta->file_mutex);
		for (const auto &kv : tfmeta->block_map) {
			block_manager->FreeBlock(kv.second);
		}
	}

	file_to_temp_meta.erase(filename);
}

bool TemporaryFileMetadataManager::FileExists(const string &filename) {
	std::lock_guard<std::mutex> lock(temp_mutex);
	return file_to_temp_meta.count(filename) > 0;
}

idx_t TemporaryFileMetadataManager::GetFileSizeLBA(const string &filename) {
	std::lock_guard<std::mutex> lock(temp_mutex);

	if (!file_to_temp_meta.count(filename)) {
		return 0;
	}

	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();
	std::lock_guard<std::mutex> file_lock(tfmeta->file_mutex);

	idx_t nr_lbas = (tfmeta->block_size * tfmeta->block_map.size()) / lba_size;

	return nr_lbas;
}

void TemporaryFileMetadataManager::Clear() {
	std::lock_guard<std::mutex> lock(temp_mutex);

	for (const auto &kv : file_to_temp_meta) {
		TempFileMetadata *tfmeta = kv.second.get();
		std::lock_guard<std::mutex> file_lock(tfmeta->file_mutex);

		for (const auto &block : tfmeta->block_map) {
			block_manager->FreeBlock(block.second);
		}
	}

	file_to_temp_meta.clear();
}

idx_t TemporaryFileMetadataManager::GetSeekBound(const string &filename) {
	std::lock_guard<std::mutex> lock(temp_mutex);

	if (!file_to_temp_meta.count(filename)) {
		return 0;
	}

	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();
	std::lock_guard<std::mutex> file_lock(tfmeta->file_mutex);

	return tfmeta->block_size * tfmeta->block_map.size();
}

idx_t TemporaryFileMetadataManager::GetAvailableSpace(idx_t lba_count, idx_t lba_start) {
	std::lock_guard<std::mutex> lock(temp_mutex);
	idx_t temp_max_bytes = ((lba_count - 1) - lba_start) * lba_size;
	idx_t temp_used_bytes = 0;

	for (const auto &kv : file_to_temp_meta) {
		TempFileMetadata *tfmeta = kv.second.get();
		std::lock_guard<std::mutex> file_lock(tfmeta->file_mutex);

		temp_used_bytes += kv.second->block_size * kv.second->block_map.size();
	}

	return (temp_max_bytes - temp_used_bytes);
}

void TemporaryFileMetadataManager::ListFiles(const string &directory,
                                             const std::function<void(const string &, bool)> &callback) {
	std::lock_guard<std::mutex> lock(temp_mutex);

	for (const auto &kv : file_to_temp_meta) {
		callback(StringUtil::GetFileName(kv.first), false);
	}
}

void TemporaryFileMetadataManager::RenameFile(const string &old_name, const string &new_name) {
	std::lock_guard<std::mutex> lock(temp_mutex);

	if (!file_to_temp_meta.count(old_name)) {
		throw IOException("Cannot rename file that doesn't exist: %s", old_name);
	}

	if (file_to_temp_meta.count(new_name)) {
		throw IOException("Target file already exists: %s", new_name);
	}

	// Move the metadata entry (C++14 compatible)
	file_to_temp_meta[new_name] = std::move(file_to_temp_meta[old_name]);
	file_to_temp_meta.erase(old_name);
}

} // namespace duckdb
