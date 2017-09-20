#ifndef DB_SNAPSHOT_H
#define DB_SNAPSHOT_H 

#include <map>

#include "util.h"
#include "cache.h"
#include "io.h"
#include "block_alloc.h"

namespace sdb {

class Snapshot {
public:
    Snapshot()=delete;
    Snapshot(const std::string &db_name):db_name(db_name){}
    Bytes read_block(BlockNum block_num, bool is_record);
    void write_block(BlockNum block_num, const Bytes &bytes, bool is_record);
    void rollback();
    void commit();

    // setter
    void set_isolation_level(TransInfo::Level level) {
        this->level = level;
    }

private:
    std::string block_path()const {
        return IO::get_block_path(db_name);
    }

public:
    // <old_block_num, <is_record_block, new_block_num>>
    std::map<BlockNum, std::pair<bool, BlockNum>> block_map;
private:
    std::string db_name;
    TransInfo::Level level = TransInfo::READ;
    BlockCache &block_cache = CacheMaster::get_block_cache();
    BlockAlloc &block_alloc = BlockAlloc::get();
};

} // namespace sdb

#endif /* ifndef DB_SNAPSHOT_H */
