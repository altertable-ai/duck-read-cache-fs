#include "chunk_utils.hpp"

#include "duckdb/common/helper.hpp"

namespace duckdb {

ChunkAlignmentInfo CalculateChunkAlignment(const ReadRequestParams &params) {
	const idx_t aligned_start_offset = params.requested_start_offset / params.block_size * params.block_size;
	const idx_t aligned_last_chunk_offset =
	    (params.requested_start_offset + params.requested_bytes_to_read - 1) / params.block_size * params.block_size;
	const idx_t subrequest_count = (aligned_last_chunk_offset - aligned_start_offset) / params.block_size + 1;

	return ChunkAlignmentInfo {
	    .aligned_start_offset = aligned_start_offset,
	    .aligned_last_chunk_offset = aligned_last_chunk_offset,
	    .subrequest_count = subrequest_count,
	};
}

idx_t ForEachCacheReadChunk(const ReadRequestParams &params, idx_t file_size, char *buffer,
                            const std::function<void(const CacheReadChunk &)> &chunk_callback) {
	const idx_t block_size = params.block_size;
	const ChunkAlignmentInfo alignment_info = CalculateChunkAlignment(params);

	idx_t write_offset = 0;

	idx_t already_read_bytes = 0;
	idx_t requested_start_offset = params.requested_start_offset;
	idx_t total_bytes_to_cache = 0;

	for (idx_t io_start_offset = alignment_info.aligned_start_offset;
	     io_start_offset <= alignment_info.aligned_last_chunk_offset; io_start_offset += block_size) {
		if (io_start_offset == file_size) {
			continue;
		}

		CacheReadChunk cache_read_chunk;
		cache_read_chunk.requested_start_addr = buffer == nullptr ? nullptr : buffer + write_offset;
		cache_read_chunk.aligned_start_offset = io_start_offset;
		cache_read_chunk.requested_start_offset = requested_start_offset;

		// Case-1: If there's only one chunk, which serves as both the first chunk and the last one.
		if (io_start_offset == alignment_info.aligned_start_offset &&
		    io_start_offset == alignment_info.aligned_last_chunk_offset) {
			cache_read_chunk.chunk_size = MinValue<idx_t>(block_size, file_size - io_start_offset);
			cache_read_chunk.bytes_to_copy = params.requested_bytes_to_read;
		}
		// Case-2: First chunk.
		else if (io_start_offset == alignment_info.aligned_start_offset) {
			const idx_t delta_offset = requested_start_offset - alignment_info.aligned_start_offset;
			write_offset += block_size - delta_offset;
			already_read_bytes += block_size - delta_offset;

			cache_read_chunk.chunk_size = block_size;
			cache_read_chunk.bytes_to_copy = block_size - delta_offset;
		}
		// Case-3: Last chunk.
		else if (io_start_offset == alignment_info.aligned_last_chunk_offset) {
			cache_read_chunk.chunk_size = MinValue<idx_t>(block_size, file_size - io_start_offset);
			cache_read_chunk.bytes_to_copy = params.requested_bytes_to_read - already_read_bytes;
		}
		// Case-4: Middle chunks.
		else {
			write_offset += block_size;
			already_read_bytes += block_size;

			cache_read_chunk.bytes_to_copy = block_size;
			cache_read_chunk.chunk_size = block_size;
		}
		total_bytes_to_cache += cache_read_chunk.chunk_size;

		// Update read offset for next chunk read.
		requested_start_offset = io_start_offset + block_size;

		chunk_callback(cache_read_chunk);
	}

	return total_bytes_to_cache;
}

} // namespace duckdb
