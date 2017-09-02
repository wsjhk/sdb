#include "block_alloc.h"
#include "io.h"
#include "../cpp_util/error.hpp"

namespace sdb {

BlockNum BlockAlloc::new_block(const std::string &db_name) {
    auto mutex_it = mutex_map.find(db_name);
    assert_msg(mutex_it != mutex_map.end(), cpp_util::format("has not db : [%s]", db_name));
    mutex_it->second.lock();

    // read free list
    std::string file_path = db_name+"/alloc.sdb";
    IO &io = IO::get();
    Bytes bytes = io.read_file(file_path);
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
    io.full_write_file(file_path, en_bytes(free_lst));

    mutex_it->second.unlock();
    return ret;
}

void BlockAlloc::free_block(const std::string &db_name, BlockNum block_num) {
     auto mutex_it = mutex_map.find(db_name);
    assert_msg(mutex_it != mutex_map.end(), cpp_util::format("not has db : [%s]", db_name));
    mutex_it->second.lock();

    std::string file_path = db_name+"/alloc.sdb";
    IO &io = IO::get();
    Bytes bytes = io.read_file(file_path);
    int offset = 0;
    std::vector<Size> free_lst;
    de_bytes(free_lst, bytes, offset);

    free_lst.push_back(block_num);

    io.full_write_file(file_path, en_bytes(free_lst));

    mutex_it->second.unlock();
}

} // namespace sdb
