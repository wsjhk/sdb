#include <gtest/gtest.h>

#include "../../src/db/block_alloc.h"
#include "../../src/db/io.h"

using namespace sdb;

TEST(db_block_alloc_test, block_alloc) {
    IO &io = IO::get();
    io.create_dir("_block_alloc_test");
    io.create_file("_block_alloc_test/alloc.sdb");
    std::vector<size_t> v{0};
    io.full_write_file("_block_alloc_test/alloc.sdb", en_bytes(v));
    BlockAlloc &block_alloc = BlockAlloc::get();
    size_t bn = block_alloc.new_block("_block_alloc_test");
    ASSERT_TRUE(bn == 0);
    bn = block_alloc.new_block("_block_alloc_test");
    ASSERT_TRUE(bn == 1);
    bn = block_alloc.new_block("_block_alloc_test");
    ASSERT_TRUE(bn == 2);
    block_alloc.free_block("_block_alloc_test", 1);
    bn = block_alloc.new_block("_block_alloc_test");
    ASSERT_TRUE(bn == 1);
}

