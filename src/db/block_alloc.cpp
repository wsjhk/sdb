#include "block_alloc.h"
#include "io.h"
#include "tlog.h"
#include "../cpp_util/lib/error.hpp"

namespace sdb {

enum BlockLogType : char { 
    LAST_NUM_UPDATE, 
    FREE_SET_INSERT, 
    FREE_SET_REMOVE, 
    TEMP_SET_INSERT, 
    TEMP_SET_REMOVE, 
};

// ========== public ==========
BlockNum BlockAlloc::new_block() {
    // get num and log
    BlockNum num;
    auto num_opt = free_set.pop_min();
    if (num_opt.has_value()) {
        num = num_opt.value();
        log.log(sdb::en_bytes(FREE_SET_REMOVE, num));
    } else {
        num = atomic_increment_integer(last_num);
        log.log(sdb::en_bytes(LAST_NUM_UPDATE, num));
    }
    assert(temp_set.insert(num));
    log.log(sdb::en_bytes(TEMP_SET_INSERT, num));
    return num;
}

void BlockAlloc::free_block(BlockNum block_num) {
    assert(free_set.insert(block_num));
    log.log(sdb::en_bytes(FREE_SET_INSERT, block_num));
}

void BlockAlloc::free_temp_block(BlockNum block_num) {
    assert(temp_set.remove(block_num));
    log.log(sdb::en_bytes(TEMP_SET_REMOVE, block_num));
}

void BlockAlloc::sync_block(BlockNum block_num) {
    assert(temp_set.remove(block_num));
    log.log(sdb::en_bytes(FREE_SET_REMOVE, block_num));
}

// ========== private ==========
BlockAlloc::BlockAlloc():log(io.balloc_log_path()) {
}

void BlockAlloc::load_backup() {
    // bytes: |free_set, temp_set|
    Bytes bytes = io.read_file(io.balloc_backup_path());
    Size offset = 0;
    Size len = 0;
    // free set
    sdb::de_bytes(len, bytes, offset);
    for (int i = 0; i < len; ++i) {
        BlockNum num;
        sdb::de_bytes(num, bytes, offset);
        free_set.insert(num);
    }
    // temp set
    sdb::de_bytes(len, bytes, offset);
    for (int i = 0; i < len; ++i) {
        BlockNum num;
        sdb::de_bytes(num, bytes, offset);
        temp_set.insert(num);
    }
}

void BlockAlloc::write_backup() {
    auto f = [this](Bytes &&bytes){
        Size offset = 0;
        char type;
        sdb::de_bytes(type, bytes, offset);
        BlockNum num;
        sdb::de_bytes(num, bytes, offset);
        switch (type) {
            case LAST_NUM_UPDATE:
                atomic_increment_integer(last_num);
                break;
            case FREE_SET_INSERT:
                free_set.insert(num);
                break;
            case FREE_SET_REMOVE:
                free_set.remove(num);
                break;
            case TEMP_SET_INSERT:
                temp_set.remove(num);
                break;
            case TEMP_SET_REMOVE:
                temp_set.remove(num);
                break;
            default:
                assert(false);
        }
    };
    // range op
    log.range(f);
}

} // namespace sdb
