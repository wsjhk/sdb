//
// Created by sven on 17-3-18.
//

#ifndef MAIN_CACHE_H
#define MAIN_CACHE_H

#include <memory>
#include <list>
#include <unordered_map>
#include <mutex>

#include "util.h"

namespace sdb {

// for log cache
// class LogCache {
// public:
//     void push();
// 
// private:
//     std::list<Log>
// };
// 

// LRU
class BlockCache {
public:
    // alias
    using BlockPtr = std::shared_ptr<Bytes>;

    using CacheKey = std::string;
    using KeyPair = std::pair<std::string, BlockNum>;

    struct CacheValue {
        CacheKey key;
        BlockPtr ptr;
        CacheValue()=delete;
        CacheValue(const std::string &key, BlockPtr ptr):key(key), ptr(ptr){}
    };

    using ValueList = std::list<CacheValue>;

    explicit BlockCache(size_t max_block_count):max_block_count(max_block_count){}
    BlockCache(const BlockCache &)=delete;
    BlockCache(BlockCache &&)=delete;
    BlockCache &operator=(const BlockCache &)=delete;
    BlockCache &operator=(BlockCache &&)=delete;

    // get and put
    Bytes get(const std::string &path, size_t block_num);
    void put(const std::string &path, size_t block_num, const Bytes &data);

    // sync all cache
    void sync();
    // sync file
    // auto sync(const std::string &path);
    // sync block
    auto sync(const std::string &path, size_t block_num);

    // for test
    ValueList _value_list()const {
        return value_list;
    }

private:
    CacheKey encode_key(const std::string &path, size_t block_num);
    KeyPair decode_key(const std::string &key);

    // block io
    Bytes read_block(const std::string &path, size_t block_num);

    // pop
    void pop();

private:
    // max cache block
    size_t max_block_count;
    // full mutex
    std::mutex mutex;
    // block cache data list
    ValueList value_list;
    std::unordered_map<CacheKey, ValueList::iterator> key_map;
};

class CacheMaster {
public:
    static BlockCache &get_block_cache() {
        static BlockCache cache(100);
        return cache;
    }
};

} // namespace sdb

#endif //CACHE_H
