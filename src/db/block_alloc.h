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

    size_t new_block(const std::string &db_name);
    void free_block(const std::string &db_name, size_t block_num);

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
