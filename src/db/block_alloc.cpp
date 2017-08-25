#include "block_alloc.h"
#include "io.h"
#include "../cpp_util/error.hpp"

namespace sdb {

size_t BlockAlloc::new_block(const std::string &db_name) {
    mutex.lock();

    // read free list
    std::string file_path = db_name+"/alloc.sdb";
    IO &io = IO::get();
    Bytes bytes = io.read_file(file_path);
    size_t offset = 0;
    std::vector<size_t> free_lst;
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
    io.full_write_file(file_path, en_bytes(free_lst));

    mutex.unlock();
    return ret;
}

void BlockAlloc::free_block(const std::string &db_name, size_t block_num) {
    mutex.lock();

    std::string file_path = db_name+"/alloc.sdb";
    IO &io = IO::get();
    Bytes bytes = io.read_file(file_path);
    size_t offset = 0;
    std::vector<size_t> free_lst;
    de_bytes(free_lst, bytes, offset);

    free_lst.push_back(block_num);

    io.full_write_file(file_path, en_bytes(free_lst));

    mutex.unlock();
}

} // namespace sdb
