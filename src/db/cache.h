#ifndef DB_CACHE_H
#define DB_CACHE_H

#include <memory>
#include <list>
#include <unordered_map>
#include <mutex>

#include "util.h"
#include "io.h"
#include "block_alloc.h"
#include "../cpp_util/lib/cache.hpp"

namespace sdb {

template <size_t _max_size>
class BlockCache : public cpp_util::ConcurrentLruCache<BlockNum, Bytes, _max_size> {
public:
    std::optional<Bytes> load(const BlockNum &key)override;
    void unload(const BlockNum &block_num, const Bytes &data)override;

private:
    // io
    IO &io = IO::get();
    BlockAlloc &balloc = BlockAlloc::get();
};

// ========== public function ==========
template <size_t _max_size>
std::optional<Bytes> BlockCache<_max_size>::load(const BlockNum &key) {
    Bytes bytes = io.read_block(io.block_path(), key);
    return bytes;
}

template <size_t _max_size>
void BlockCache<_max_size>::unload(const BlockNum &block_num, const Bytes &data) {
    balloc.sync_block(block_num);
    io.write_block(io.block_path(), block_num, data);
}

class CacheMaster {
public:
    static BlockCache<100> &get_block_cache() {
        static BlockCache<100> cache;
        return cache;
    }
};

} // namespace sdb

#endif // DB_CACHE_H
