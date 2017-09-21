#include "block_alloc.h"
#include "io.h"
#include "../cpp_util/error.hpp"

namespace sdb {

BlockNum BlockAlloc::new_block() {
    std::lock_guard<std::mutex> gt(mutex);
    // read free list
    IO &io = IO::get();
    Bytes bytes = io.read_file(io.alloc_path());
    int offset = 0;
    std::vector<int> free_lst;
    de_bytes(free_lst, bytes, offset);
    assert_msg(!free_lst.empty(), "block alloc error");

    // get return block number
    size_t ret;
    if (free_lst.size() == 1) {
        ret = free_lst[0];
        free_lst[0]++;
    } else  {
        ret = free_lst.back();
        free_lst.pop_back();
    }

    // write back
    io.full_write_file(io.alloc_path(), en_bytes(free_lst));
    return ret;
}

void BlockAlloc::free_block(BlockNum block_num) {
    std::lock_guard<std::mutex> gt(mutex);

    IO &io = IO::get();
    Bytes bytes = io.read_file(io.alloc_path());
    int offset = 0;
    std::vector<Size> free_lst;
    de_bytes(free_lst, bytes, offset);

    free_lst.push_back(block_num);

    io.full_write_file(io.alloc_path(), en_bytes(free_lst));
}

} // namespace sdb
