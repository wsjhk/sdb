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

    BlockNum new_block(const std::string &db_name);
    BlockNum new_temp_block(const std::string &db_name);
    void free_block(const std::string &db_name, BlockNum block_num);

private:
    BlockAlloc(){}
    BlockAlloc(const BlockAlloc &)=delete;
    BlockAlloc(BlockAlloc &&)=delete;
    BlockAlloc &operator=(const BlockAlloc &)=delete;
    BlockAlloc &operator=(BlockAlloc &&)=delete;

private:
    std::unordered_map<std::string, std::mutex> mutex_map;

public: //for test
    std::unordered_map<std::string, std::mutex> &_get_map() {
        return mutex_map;
    }
};

} // namespace sdb


#endif /* ifndef DB_BLOCK_ALLOC_H */
