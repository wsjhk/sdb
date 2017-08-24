#include <algorithm>

#include "cache.h"
#include "util.h"
#include "io.h"
#include "../cpp_util/log.hpp"

using namespace cpp_util;

namespace sdb {

// ========== public function ==========
Bytes BlockCache::get(const std::string &path, size_t block_num) {
    mutex.lock();
    CacheKey key = encode_key(path, block_num);
    auto it = key_map.find(key);
    if (it == key_map.end()) {
        Bytes bytes = read_block(path, block_num);
        if (value_list.size() >= max_block_count) {
            pop();
        }
        value_list.push_front(CacheValue(key, std::make_shared<Bytes>(bytes)));
        key_map[key] = value_list.begin();
        mutex.unlock();
        return bytes;
    }
    BytesPtr ptr = it->second->ptr;
    Bytes bytes = *ptr;
    value_list.erase(it->second);
    value_list.push_front(CacheValue(key, ptr));
    it->second = value_list.begin();
    mutex.unlock();
    return bytes;
}

void BlockCache::put(const std::string &path, size_t block_num, const Bytes &bytes) {
    mutex.lock();
    CacheKey key = encode_key(path, block_num);
    BytesPtr ptr = std::make_shared<Bytes>(bytes);
    auto it = key_map.find(key);
    if (it == key_map.end()) {
        value_list.push_front(CacheValue(key, ptr));
        key_map[key] = value_list.begin();
        mutex.unlock();
        return;
    }
    value_list.erase(it->second);
    value_list.push_front(CacheValue(key, ptr));
    it->second = value_list.begin();
    mutex.unlock();
}

Bytes BlockCache::read_block(const std::string &path, size_t block_num) {
    // read cache
    IO &io = IO::get();
    Bytes cache_bytes = io.read_block(path, block_num);
    return cache_bytes;
}

// ========== private function ==========
BlockCache::CacheKey BlockCache::encode_key(const std::string &path, size_t block_num) {
    return path+"+"+std::to_string(block_num);
}

BlockCache::KeyPair BlockCache::decode_key(const std::string &key) {
    size_t iter = key.find('+');
    std::string filename(key, 0, iter);
    size_t block_num = std::stoul(std::string(key, iter+1, key.size()));
    return std::make_pair(filename, block_num);
}

void BlockCache::sync() {
    mutex.lock();
    for (auto &&[key, vl_it] : key_map) {
        auto &&[path, block_num] = decode_key(key);
        IO::get().write_block(path, block_num, *vl_it->ptr);
    }
    mutex.unlock();
}

auto BlockCache::sync(const std::string &path, size_t block_num) {
    mutex.lock();
    auto it = key_map.find(encode_key(path, block_num));
    IO::get().write_block(path, block_num, *it->second->ptr);
    mutex.unlock();
}

void BlockCache::pop() {
    auto &&[key, ptr] = value_list.back();
    auto &&[path, block_num] = decode_key(key);
    auto it = key_map.find(key);
    IO::get().write_block(path, block_num, *it->second->ptr);
    key_map.erase(key);
    value_list.pop_back();
}

} // namespace sdb
