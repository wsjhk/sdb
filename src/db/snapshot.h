#ifndef DB_SNAPSHOT_H
#define DB_SNAPSHOT_H 

#include <map>

#include "util.h"
#include "cache.h"
#include "io.h"
#include "block_alloc.h"

namespace sdb {

// record snapshot
class Snapshot {
public:
    Snapshot(){}
    Bytes read_block(BlockNum block_num);
    void write_block(BlockNum block_num, const Bytes &bytes);
    void rollback();
    void commit();

    // setter
    void set_isolation_level(TransInfo::Level level) {
        this->level = level;
    }

public:
    // <old_block_num, new_block_num>>
    std::map<BlockNum, BlockNum> block_map;

private:
    TransInfo::Level level = TransInfo::READ;
    BlockCache &block_cache = CacheMaster::get_block_cache();
    BlockAlloc &block_alloc = BlockAlloc::get();
    IO &io = IO::get();
};

} // namespace sdb

#endif /* ifndef DB_SNAPSHOT_H */
