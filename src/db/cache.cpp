#include <algorithm>

#include "cache.h"
#include "util.h"
#include "../cpp_util/log.hpp"

using namespace cpp_util;

namespace sdb {

// ========== public function ==========
Bytes BlockCache::get(BlockNum key) {
    std::lock_guard<std::mutex> lg(mutex);
    auto it = key_map.find(key);
    if (it == key_map.end()) {
        Bytes block = io.read_block(io.block_path(), key);
        if (value_list.size() >= max_block_count) {
            pop();
        }
        value_list.push_front(CacheValue(key, block));
        key_map[key] = value_list.begin();
        mutex.unlock();
        return block;
    }
    Bytes block = it->second->data;
    value_list.erase(it->second);
    value_list.push_front(CacheValue(key, block));
    it->second = value_list.begin();
    return block;
}

void BlockCache::put(BlockNum key, const Bytes &data) {
    std::lock_guard<std::mutex> lg(mutex);
    auto it = key_map.find(key);
    if (it == key_map.end()) {
        value_list.push_front(CacheValue(key, data));
        key_map[key] = value_list.begin();
        mutex.unlock();
        return;
    }
    value_list.erase(it->second);
    value_list.push_front(CacheValue(key, data));
    it->second = value_list.begin();
}

// ========== private function ==========
void BlockCache::sync() {
    std::lock_guard<std::mutex> lg(mutex);
    for (auto &&[key, vl_it] : key_map) {
        IO::get().write_block(io.block_path(), key, vl_it->data);
    }
}

void BlockCache::sync(BlockNum block_num) {
    std::lock_guard<std::mutex> lg(mutex);
    auto it = key_map.find(block_num);
    IO::get().write_block(io.block_path(), block_num, it->second->data);
}

void BlockCache::pop() {
    auto &&[key, data] = value_list.back();
    IO::get().write_block(io.block_path(), key, data);
    key_map.erase(key);
    value_list.pop_back();
}

} // namespace sdb
