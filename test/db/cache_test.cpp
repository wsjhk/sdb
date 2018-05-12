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
        BlockCache<3> cache;
        cache.put(0, b0);
        cache.put(1, b1);
        cache.put(2, b2);
        cache.put(0, b0);
        cache.put(3, b3);

        // sync all cache
        cache.sync();
    }
    // check get sync
    BlockCache<3> cache;
    cache.get(0);
    cache.get(1);
    cache.get(2);
    cache.get(0);
    cache.get(3);
    io.delete_file(file_path);

    // === todo
    // + multithreading check
}
