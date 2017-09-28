#include "snapshot.h"

namespace sdb {

// static init
static std::map<BlockNum, std::mutex> block_lock_map;

// ========== public function =========
Bytes Snapshot::read_block(BlockNum block_num) {
    auto it = block_map.find(block_num);
    if (it != block_map.end()) {
        return block_cache.get(it->second);
    }
    Bytes bytes = block_cache.get(block_num);
    if (level == TransInfo::READ) {
        return bytes;
    }
    BlockNum new_block_num = block_alloc.new_temp_block();
    block_cache.put(new_block_num, bytes);
    block_map[block_num] = new_block_num;
    return bytes;
}

void Snapshot::write_block(BlockNum block_num, const Bytes &bytes) {
    if (level == TransInfo::READ) {
        block_lock_map[block_num].lock();
    }
    auto it = block_map.find(block_num);
    BlockNum num;
    if (it == block_map.end()) {
        num = block_alloc.new_temp_block();
        block_map[block_num] = num;
    } else {
        num = it->first;
    }
    block_cache.put(num, bytes);
}

void Snapshot::rollback(){
    for (auto &&[old_num, new_num] : block_map) {
        if (level == TransInfo::READ) {
            block_lock_map[old_num].unlock();
        }
        block_alloc.free_block(new_num);
    }
}

bool Snapshot::commit() {
    if (level == TransInfo::READ) {
        for (auto &&[old_num, new_num] : block_map) {
            Bytes new_bytes = block_cache.get(new_num);
            block_cache.put(old_num, new_bytes);
            block_lock_map[old_num].unlock();
        }
    } else if (level == TransInfo::R_READ) {
        // check version
        for (auto &&[old_num, new_num] : block_map) {
            Bytes old_bytes = block_cache.get(old_num);
            Bytes new_bytes = block_cache.get(new_num);
            Size offset = 0;
            Vid old_v_id, new_v_id;
            sdb::de_bytes(old_v_id, old_bytes, offset);
            sdb::de_bytes(new_v_id, old_bytes, (offset = 0));
            if (new_v_id - old_v_id != 1) {
                return false;
            }
        }
        // sync
        for (auto &&[old_num, new_num] : block_map) {
            Bytes new_bytes = block_cache.get(new_num);
            block_cache.put(old_num, new_bytes);
        }
    }
    return true;
}


} // namespace sdb
