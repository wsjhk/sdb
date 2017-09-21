#ifndef DB_BLOCK_ALLOC_H
#define DB_BLOCK_ALLOC_H

#include <mutex>

#include "util.h"

namespace sdb {

class BlockAlloc {
public:
    static BlockAlloc &get() {
        static BlockAlloc block_alloc;
        return block_alloc;
    }

    BlockNum new_block();
    BlockNum new_temp_block();
    void free_block(BlockNum block_num);

private:
    BlockAlloc(){}
    BlockAlloc(const BlockAlloc &)=delete;
    BlockAlloc(BlockAlloc &&)=delete;
    BlockAlloc &operator=(const BlockAlloc &)=delete;
    BlockAlloc &operator=(BlockAlloc &&)=delete;

private:
    std::mutex mutex;
};

} // namespace sdb


#endif /* ifndef DB_BLOCK_ALLOC_H */
