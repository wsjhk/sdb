#include <gtest/gtest.h>

#include "../../src/db/cache.h"
#include "../../src/db/io.h"
#include "../../src/db/util.h"

using namespace sdb;

TEST(db_cache_test, BlockCache) {
    IO &io = IO::get();
    std::string file_path = "_cache_test.tmp";
    if (io.has_file(file_path)) {
        io.delete_file(file_path);
    }
    io.create_file(file_path);
    Bytes b0(BLOCK_SIZE, 'a');
    Bytes b1(BLOCK_SIZE, 'b');
    Bytes b2(BLOCK_SIZE, 'c');
    Bytes b3(BLOCK_SIZE, 'd');
    if (true) {
        BlockCache cache(3);
        cache.put(file_path, 0, b0);
        cache.put(file_path, 1, b1);
        cache.put(file_path, 2, b2);
        cache.put(file_path, 0, b0);
        cache.put(file_path, 3, b3);
        BlockCache::ValueList lst = cache._value_list();
        // put
        auto it = lst.begin();
        ASSERT_TRUE(*it->ptr == b3);
        it = std::next(it);
        ASSERT_TRUE(*it->ptr == b0);
        it = std::next(it);
        ASSERT_TRUE(*it->ptr == b2);
        // get and sync
        Bytes read_bytes = cache.get(file_path, 1);
        ASSERT_TRUE(read_bytes == b1);

        // sync all cache
        cache.sync();
    }
    // check get sync
    BlockCache cache(3);
    cache.get(file_path, 0);
    cache.get(file_path, 1);
    cache.get(file_path, 2);
    cache.get(file_path, 0);
    cache.get(file_path, 3);
    BlockCache::ValueList lst = cache._value_list();
    // put
    auto it = lst.begin();
    ASSERT_TRUE(*it->ptr == b3);
    it = std::next(it);
    ASSERT_TRUE(*it->ptr == b0);
    it = std::next(it);
    ASSERT_TRUE(*it->ptr == b2);

    io.delete_file(file_path);

    // === todo
    // + multithreading check
}
