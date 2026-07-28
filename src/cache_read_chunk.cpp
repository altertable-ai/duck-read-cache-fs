#include "cache_read_chunk.hpp"

#include "page_aligned_data_chunk.hpp"

namespace duckdb {

void CacheReadChunk::CopyBufferToRequestedMemory(const PageAlignedDataChunk &buffer) {
	buffer.CopyTo(requested_start_addr, GetDeltaOffset(), bytes_to_copy);
}

} // namespace duckdb
