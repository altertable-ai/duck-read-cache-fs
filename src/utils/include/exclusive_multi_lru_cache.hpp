// ExclusiveMultiLruCache is a LRU cache with all entries exclusive, which
// - pops out at `GetAndPop` operation;
// - allows multiple values for one single key.
//
// It's made for values which indicate exclusive resource, for example, file handle.
//
// Example usage:
// ExclusiveMultiLruCache<string, FileHandle> cache{/*max_entries_p=*/1, /*timeout_millisec_p=*/1000};
// cache.Put("hello", make_unique<FileHandle>(handle));
// auto cached_handle = cache.GetAndPop("hello");

#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <string>
#include <utility>
#include <type_traits>
#include <iostream>

#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector.hpp"
#include "time_utils.hpp"

namespace duckdb {

// TODO(hjiang): The most ideal return type is `std::optional<Value>` for `GetAndPop`, but we're still at C++14, so have
// to use `unique_ptr`.
template <typename Key, typename Val, typename KeyHash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
class ExclusiveMultiLruCache {
public:
	using key_type = Key;
	using mapped_type = vector<unique_ptr<Val>>;
	using hasher = KeyHash;
	using key_equal = KeyEqual;

	struct GetAndPopResult {
		// Entries evicted due to staleness.
		vector<unique_ptr<Val>> evicted_items;
		// The real entry caller could use.
		unique_ptr<Val> target_item;
	};

	// @param max_entries_p: A `max_entries` of 0 means that there is no limit on the number of entries in the cache.
	// @param timeout_millisec_p: Timeout in milliseconds for entries, exceeding which invalidates the cache entries; 0
	// means no timeout.
	ExclusiveMultiLruCache(size_t max_entries_p, uint64_t timeout_millisec_p)
	    : max_entries(max_entries_p), timeout_millisec(timeout_millisec_p) {
	}

	// Disable copy and move.
	ExclusiveMultiLruCache(const ExclusiveMultiLruCache &) = delete;
	ExclusiveMultiLruCache &operator=(const ExclusiveMultiLruCache &) = delete;

	~ExclusiveMultiLruCache() = default;

	// Insert `value` with key `key`, the values with the same key will be kept and evicted first.
	// Return evicted value if any.
	//
	// Reasoning for returning the value back to caller:
	// 1. Caller is able to do processing for the value.
	// 2. For thread-safe lru cache, processing could be moved out of critical section.
	unique_ptr<Val> Put(Key key, unique_ptr<Val> value) {
		std::cout << "[PUT] START: cur_entries_num=" << cur_entries_num << " max_entries=" << max_entries << std::endl;
		
		// Add to front of LRU list (most recent)
		lru_list.emplace_front(key);
		std::cout << "[PUT] Added key to LRU front, list size=" << lru_list.size() << std::endl;
		
		Entry new_entry {
		    .value = std::move(value),
		    .timestamp = static_cast<uint64_t>(GetSteadyNowMilliSecSinceEpoch()),
		    .lru_iterator = lru_list.begin(),
		};
		
		auto key_cref = std::cref(lru_list.front());
		auto& entries = entry_map[key_cref];
		entries.emplace_back(std::move(new_entry));
		++cur_entries_num;
		
		std::cout << "[PUT] Added entry to map, entries_for_key=" << entries.size() << " total_entries=" << cur_entries_num << std::endl;

		unique_ptr<Val> evicted_val = nullptr;
		if (max_entries > 0 && cur_entries_num > max_entries) {
			std::cout << "[PUT] EVICTION NEEDED: cur_entries_num=" << cur_entries_num << " > max_entries=" << max_entries << std::endl;
			
			const auto &stale_key = lru_list.back();
			std::cout << "[PUT] Found LRU (stale) key at back of LRU list" << std::endl;
			
			auto iter = entry_map.find(stale_key);
			D_ASSERT(iter != entry_map.end());
			
			std::cout << "[PUT] Found stale key in entry_map, entries_for_stale_key=" << iter->second.size() << std::endl;
			
			// CRITICAL BUG CHECK: Are we evicting the correct entry?
			auto lru_back_iter = std::prev(lru_list.end());
			auto front_iter = iter->second.front().lru_iterator;
			bool is_correct_eviction = (lru_back_iter == front_iter);
			
			std::cout << "[PUT] *** BUG CHECK *** entries.size()=" << iter->second.size() 
			          << " LRU_back==front_iter=" << is_correct_eviction << std::endl;
			
			if (!is_correct_eviction && iter->second.size() > 1) {
				std::cout << "[PUT] *** WRONG EVICTION DETECTED! *** We should evict LRU entry but will evict front entry instead!" << std::endl;
				std::cout << "[PUT] This is the root cause of the segfault bug!" << std::endl;
			}
			
			evicted_val = DeleteFirstEntry(iter);
		}
		
		std::cout << "[PUT] END: evicted=" << (evicted_val ? "yes" : "no") << std::endl;
		return evicted_val;
	}

	// Look up the entry with key `key` and remove from cache.
	// If there're multiple values corresponds to the given [key]. the oldest value will be returned.
	GetAndPopResult GetAndPop(const Key &key) {
		std::cout << "[GET_AND_POP] START" << std::endl;
		GetAndPopResult result;

		const auto entry_map_iter = entry_map.find(key);
		if (entry_map_iter == entry_map.end()) {
			std::cout << "[GET_AND_POP] Key not found" << std::endl;
			return result;
		}

		// There're multiple entries correspond to the given [key], check whether they're stale one by one.
		auto &entries = entry_map_iter->second;
		std::cout << "[GET_AND_POP] Found key with " << entries.size() << " entries" << std::endl;
		if (timeout_millisec > 0) {
			const auto now = GetSteadyNowMilliSecSinceEpoch();
			size_t cur_entries_size = entries.size();
			std::cout << "[GET_AND_POP] Checking for stale entries, timeout=" << timeout_millisec << "ms" << std::endl;
			
			// BUG: This loop condition is wrong! Should check entries.empty(), not cur_entries_num > 0
			while (cur_entries_num > 0) {
				if (entries.empty()) {
					std::cout << "[GET_AND_POP] *** BUG TRIGGERED *** entries is empty but cur_entries_num=" << cur_entries_num << std::endl;
					std::cout << "[GET_AND_POP] This can cause undefined behavior accessing entries.front()!" << std::endl;
					break;
				}
				
				auto &cur_entry = entries.front();
				auto age_ms = now - cur_entry.timestamp;
				std::cout << "[GET_AND_POP] Checking entry age=" << age_ms << "ms vs timeout=" << timeout_millisec << "ms" << std::endl;
				
				if (age_ms > timeout_millisec) {
					std::cout << "[GET_AND_POP] Entry is stale, removing via DeleteFirstEntry" << std::endl;
					result.evicted_items.emplace_back(DeleteFirstEntry(entry_map_iter));
					--cur_entries_size;
					continue;
				}
				break;
			}

			// If there're no left entries correspond to the given [key], we directly return.
			if (cur_entries_size == 0) {
				std::cout << "[GET_AND_POP] All entries were stale, returning empty result" << std::endl;
				return result;
			}
		}

		// There're still fresh entry for the given [key].
		D_ASSERT(!entries.empty());
		std::cout << "[GET_AND_POP] Getting target item and removing entry" << std::endl;
		result.target_item = std::move(entries.front().value);
		DeleteFirstEntry(entry_map_iter);
		std::cout << "[GET_AND_POP] END: found_item=yes" << std::endl;
		return result;
	}

	// Clear the cache and get all values, application could perform their processing logic upon these values.
	vector<unique_ptr<Val>> ClearAndGetValues() {
		vector<unique_ptr<Val>> values;
		values.reserve(cur_entries_num);
		for (auto &[_, entries] : entry_map) {
			for (auto &cur_entry : entries) {
				values.emplace_back(std::move(cur_entry.value));
			}
		}
		entry_map.clear();
		lru_list.clear();
		cur_entries_num = 0;
		return values;
	}

	// Clear the cache entries which matches the given [key_pred] and return all the deleted values.
	template <typename KeyPred>
	vector<unique_ptr<Val>> ClearAndGetValues(KeyPred &&key_pred) {
		vector<Key> keys_to_delete;
		for (const auto &[key, _] : entry_map) {
			if (key_pred(key)) {
				keys_to_delete.emplace_back(key);
			}
		}

		vector<unique_ptr<Val>> values;
		for (const auto &key : keys_to_delete) {
			auto entry_map_iter = entry_map.find(key);
			D_ASSERT(entry_map_iter != entry_map.end());
			auto &entries = entry_map_iter->second;
			for (auto &cur_entry : entries) {
				values.emplace_back(std::move(cur_entry.value));
				lru_list.erase(cur_entry.lru_iterator);
				--cur_entries_num;
			}
			entry_map.erase(entry_map_iter);
		}

		return values;
	}

	// Check invariant:
	// - the number of entries in the LRU cache (1) = the number of entries in the entry map (2)
	// - the number of entries in the LRU cache (1) = the number of keys in lru list (3)
	//
	// This method iterates all elements in the LRU cache, which is time-consuming; it's supposed to be used in the unit
	// test and debug assertion.
	bool Verify() {
		// Count 2.
		int entry_map_count = 0;
		for (const auto &[_, entries] : entry_map) {
			if (entries.empty()) {
				return false;
			}
			entry_map_count += entries.size();
		}

		if (entry_map_count != cur_entries_num) {
			return false;
		}

		// Count 3.
		return lru_list.size() == cur_entries_num;
	}

private:
	struct Entry {
		// The entry's value.
		unique_ptr<Val> value;

		// Steady clock timestamp when current entry was inserted into cache.
		// 1. It's not updated at later accesses.
		// 2. It's updated at replace update operations.
		uint64_t timestamp;

		// A list iterator pointing to the entry's position in the LRU list.
		typename std::list<Key>::iterator lru_iterator;
	};

	using EntryMap = std::unordered_map<Key, std::deque<Entry>, KeyHash, KeyEqual>;

	// Delete the first entry from the given [iter], return the deleted entry.
	unique_ptr<Val> DeleteFirstEntry(typename EntryMap::iterator iter) {
		auto &entries = iter->second;
		D_ASSERT(!entries.empty());
		
		std::cout << "[DELETE_FIRST] START: entries.size()=" << entries.size() << " cur_entries_num=" << cur_entries_num << std::endl;

		auto value = std::move(entries.front().value);
		auto lru_iterator_to_erase = entries.front().lru_iterator;
		
		std::cout << "[DELETE_FIRST] About to erase from lru_list, list_size=" << lru_list.size() << std::endl;
		lru_list.erase(lru_iterator_to_erase);
		std::cout << "[DELETE_FIRST] Erased from lru_list, new_list_size=" << lru_list.size() << std::endl;
		
		if (entries.size() == 1) {
			std::cout << "[DELETE_FIRST] Last entry for this key, removing from entry_map" << std::endl;
			entry_map.erase(iter);
		} else {
			std::cout << "[DELETE_FIRST] Multiple entries for this key, removing front entry only" << std::endl;
			entries.pop_front();
		}
		--cur_entries_num;
		
		std::cout << "[DELETE_FIRST] END: cur_entries_num=" << cur_entries_num << std::endl;
		return value;
	}

	// Current number of entries in the cache.
	size_t cur_entries_num = 0;

	// The maximum number of entries in the cache. A value of 0 means there is no limit on entry count.
	const size_t max_entries;

	// The timeout in seconds for cache entries; entries with exceeding timeout would be invalidated.
	const uint64_t timeout_millisec;

	// All keys are stored as refernce (`std::reference_wrapper`), and the ownership lies in `lru_list`.
	EntryMap entry_map;

	// The LRU list of entries. The front of the list identifies the most recently accessed entry.
	std::list<Key> lru_list;
};

// Same interfaces as `ExclusiveMultiLruCache`, but all cached values are `const` specified to avoid concurrent updates.
template <typename K, typename V, typename KeyHash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using ExclusiveLruConstCache = ExclusiveMultiLruCache<K, const V, KeyHash, KeyEqual>;

// Thread-safe implementation.
template <typename Key, typename Val, typename KeyHash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
class ThreadSafeExclusiveMultiLruCache {
public:
	using lru_impl = ExclusiveMultiLruCache<Key, Val, KeyHash, KeyEqual>;
	using key_type = typename lru_impl::key_type;
	using mapped_type = typename lru_impl::mapped_type;
	using hasher = typename lru_impl::hasher;
	using key_equal = typename lru_impl::key_equal;
	using GetAndPopResult = typename lru_impl::GetAndPopResult;

	// @param max_entries_p: A `max_entries` of 0 means that there is no limit on the number of entries in the cache.
	// @param timeout_millisec_p: Timeout in milliseconds for entries, exceeding which invalidates the cache entries; 0
	// means no timeout.
	ThreadSafeExclusiveMultiLruCache(size_t max_entries, uint64_t timeout_millisec)
	    : internal_cache(max_entries, timeout_millisec) {
	}

	// Disable copy and move.
	ThreadSafeExclusiveMultiLruCache(const ThreadSafeExclusiveMultiLruCache &) = delete;
	ThreadSafeExclusiveMultiLruCache &operator=(const ThreadSafeExclusiveMultiLruCache &) = delete;

	~ThreadSafeExclusiveMultiLruCache() = default;

	// Insert `value` with key `key`, the values with the same key will be kept and evicted first.
	// Return evicted value if any.
	unique_ptr<Val> Put(Key key, unique_ptr<Val> value) {
		std::lock_guard<std::mutex> lock(mu);
		return internal_cache.Put(std::move(key), std::move(value));
	}

	// Look up the entry with key `key` and remove from cache.
	// If there're multiple values corresponds to the given [key]. the oldest value will be returned.
	GetAndPopResult GetAndPop(const Key &key) {
		std::unique_lock<std::mutex> lock(mu);
		return internal_cache.GetAndPop(key);
	}

	// Clear the cache and get all values, application could perform their processing logic upon these values.
	vector<unique_ptr<Val>> ClearAndGetValues() {
		std::unique_lock<std::mutex> lock(mu);
		return internal_cache.ClearAndGetValues();
	}

	// Clear the cache entries which matches the given [key_pred] and return all the deleted values.
	template <typename KeyPred>
	vector<unique_ptr<Val>> ClearAndGetValues(KeyPred &&key_pred) {
		std::unique_lock<std::mutex> lock(mu);
		return internal_cache.ClearAndGetValues(std::forward<KeyPred>(key_pred));
	}

	// Check invariant.
	bool Verify() {
		std::unique_lock<std::mutex> lock(mu);
		return internal_cache.Verify();
	}

private:
	std::mutex mu;
	ExclusiveMultiLruCache<Key, Val, KeyHash, KeyEqual> internal_cache;
};

// Same interfaces as `ExclusiveMultiLruCache`, but all cached values are `const` specified to avoid concurrent updates.
template <typename K, typename V, typename KeyHash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using ThreadSafeExclusiveLruConstCache = ThreadSafeExclusiveMultiLruCache<K, const V, KeyHash, KeyEqual>;

} // namespace duckdb
