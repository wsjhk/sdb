#ifndef DB_BLOCK_ALLOC_H
#define DB_BLOCK_ALLOC_H

#include "util.h"
#include "io.h"
#include "tlog.h"
#include "../cpp_util/lib/skip_list_set.hpp"

namespace sdb {

class BlockAlloc {
public:
    static BlockAlloc &get() {
        static BlockAlloc block_alloc;
        return block_alloc;
    }

    BlockNum new_block();
    void sync_block(BlockNum block_num);
    void free_block(BlockNum block_num);
    void free_temp_block(BlockNum block_num);

private:
    BlockAlloc();
    BlockAlloc(const BlockAlloc &)=delete;
    BlockAlloc(BlockAlloc &&)=delete;
    BlockAlloc &operator=(const BlockAlloc &)=delete;
    BlockAlloc &operator=(BlockAlloc &&)=delete;

    void load_backup();
    void write_backup();
    void load_log();
    
private:
    std::atomic_int64_t last_num;
    cpp_util::SkipListSet<BlockNum, 12> free_set;
    cpp_util::SkipListSet<BlockNum, 12> temp_set;

    IO &io = IO::get();
    BaseLog log;
};

} // namespace sdb


#endif /* ifndef DB_BLOCK_ALLOC_H */
