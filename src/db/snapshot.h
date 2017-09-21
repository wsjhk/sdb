#ifndef DB_SNAPSHOT_H
#define DB_SNAPSHOT_H 

#include <map>

#include "util.h"
#include "cache.h"
#include "io.h"
#include "block_alloc.h"
#include "tuple.h"

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

private:
    BlockCache &block_cache = CacheMaster::get_block_cache();
    BlockAlloc &block_alloc = BlockAlloc::get();
    IO &io = IO::get();

private:
    TransInfo::Level level = TransInfo::READ;
    // <old_block_num, new_block_num>>
    
    // modified tuple key set
    using KeySet = std::set<Tuple>;
    std::map<BlockNum, std::pair<BlockNum, KeySet>> block_map;

    // block lock()
    // TODO concurrent map
    static std::map<BlockNum, std::mutex> block_lock_map;
};

} // namespace sdb

#endif /* ifndef DB_SNAPSHOT_H */
