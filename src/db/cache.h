//
// Created by sven on 17-3-18.
//

#ifndef DB_CACHE_H
#define DB_CACHE_H

#include <memory>
#include <list>
#include <unordered_map>
#include <mutex>

#include "util.h"
#include "io.h"

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

class BlockCache {
public:
    struct CacheValue {
        BlockNum key;
        Bytes data;
        CacheValue()=delete;
        CacheValue(BlockNum key, Bytes data):key(key), data(data){}
    };
    using ValueList = std::list<CacheValue>;

    explicit BlockCache(size_t max_block_count):max_block_count(max_block_count){}
    BlockCache(const BlockCache &)=delete;
    BlockCache(BlockCache &&)=delete;
    BlockCache &operator=(const BlockCache &)=delete;
    BlockCache &operator=(BlockCache &&)=delete;

    // get and put
    Bytes get(BlockNum block_num);
    void put(BlockNum block_num, const Bytes &data);

    // sync all cache
    void sync();
    // sync file
    // sync block
    void sync(BlockNum block_num);

    // for test
    ValueList _value_list()const {
        return value_list;
    }

private:
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
    std::unordered_map<BlockNum, ValueList::iterator> key_map;
    // io
    IO &io = IO::get();
};

class CacheMaster {
public:
    static BlockCache &get_block_cache() {
        static BlockCache cache(100);
        return cache;
    }
};

} // namespace sdb

#endif // DB_CACHE_H
